// 文件导读：Task 完成、过期恢复和 committed outputs 都通过 permit/attempt 校验；
// 只有成功 Attempt 的正式输出进入 rebuild_attempt_outputs，普通事件不会自动成为输出。
// finish/recover 负责状态机写入，committed_* 负责从成功索引只读重建；两类入口共享同一 Store 表，
// 但读取结果不会重新激活或改写 Attempt。
impl Store {
    // 只接受 terminal TaskStatus；在 Immediate 事务内复核 permit、读取冻结 on_failure 并收束 Task/Attempt。
    pub fn finish_task(
        &self,
        permit: &TaskWritePermit,
        status: TaskStatus,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        if !status.is_terminal() {
            return Err(StoreError::TaskNotRunnable(permit.task_id.clone()));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
        let (_, on_failure) = task_retry_policy(&transaction, &permit.task_id)?;
        finish_permitted_task(&transaction, permit, status, on_failure, None, now)?;
        transaction.commit()?;
        Ok(())
    }

    // 用 now 选 lease_until<now 的 running Task；单个 Immediate 事务内逐项检查 workflow 可执行、
    // Run cancellation 和 durable retry budget，再恢复 queued 或终止失败。返回数是扫描出的过期行数。
    pub fn recover_expired_tasks(&self, now: DateTime<Utc>) -> StoreResult<u64> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let expired = {
            let mut statement = transaction.prepare(
                r#"SELECT task_id, run_id, active_attempt_id, lease_id, lease_epoch, contract_hash
                   FROM rebuild_tasks
                   WHERE status = 'running' AND lease_until < ?1
                   ORDER BY task_id"#,
            )?;
            let rows = statement
                .query_map(params![now.to_rfc3339()], |row| {
                    Ok((
                        TaskId(row.get::<_, String>(0)?),
                        RunId(row.get::<_, String>(1)?),
                        akzio_domain::AttemptId(row.get::<_, String>(2)?),
                        akzio_domain::LeaseId(row.get::<_, String>(3)?),
                        row.get::<_, u64>(4)?,
                        row.get::<_, Option<String>>(5)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            rows
        };
        for (_, run_id, _, _, _, _) in &expired { assert_workflow_executable(&transaction, run_id)?; }
        for (task_id, run_id, attempt_id, lease_id, epoch, contract_hash) in &expired {
            // 从 SQL 行重建旧 permit；取消 Run 时直接收束 Cancelled，否则依 Task policy 决定 abandon/retry 或 fail。
            let permit = TaskWritePermit {
                run_id: run_id.clone(),
                task_id: task_id.clone(),
                attempt_id: attempt_id.clone(),
                lease_id: lease_id.clone(),
                epoch: *epoch,
                contract_hash: contract_hash.as_deref().map(ContentHash::new).transpose()?,
            };
            let cancelled = transaction
                .query_row(
                    "SELECT 1 FROM rebuild_run_cancellations WHERE run_id = ?1",
                    params![run_id.0],
                    |_| Ok(()),
                )
                .optional()?
                .is_some();
            let (retry, on_failure) = task_retry_policy(&transaction, task_id)?;
            if cancelled {
                finish_permitted_task(
                    &transaction,
                    &permit,
                    TaskStatus::Cancelled,
                    on_failure,
                    None,
                    now,
                )?;
                continue;
            }
            let attempts = task_failure_attempt_count(&transaction, task_id)?;
            if attempts < u64::from(retry.max_attempts) {
                // 未耗尽时释放当前 owner/lease，标记 Attempt abandoned 并记录 TaskRecovered。
                transaction.execute(
                    r#"UPDATE rebuild_tasks
                       SET status = 'queued', lease_id = NULL, active_attempt_id = NULL,
                           worker_id = NULL, lease_until = NULL, ready_at = ?1
                       WHERE task_id = ?2"#,
                    params![now.to_rfc3339(), task_id.0],
                )?;
                transaction.execute(
                    "UPDATE rebuild_attempts SET status = 'abandoned', finished_at = ?1 WHERE attempt_id = ?2",
                    params![now.to_rfc3339(), attempt_id.0],
                )?;
                append_event(
                    &transaction,
                    run_id,
                    Some(task_id),
                    Some(attempt_id),
                    LifecycleEventType::TaskRecovered,
                    None,
                    now,
                )?;
                debug::settle_attempt(&transaction, &permit, "abandoned_for_recovery", now)?;
            } else {
                // 耗尽时写 recovery_exhausted event，再按冻结 on_failure 收束终态并传播影响。
                append_event(
                    &transaction,
                    run_id,
                    Some(task_id),
                    Some(attempt_id),
                    LifecycleEventType::TaskRecoveryExhausted,
                    None,
                    now,
                )?;
                finish_permitted_task(
                    &transaction,
                    &permit,
                    TaskStatus::Failed,
                    on_failure,
                    None,
                    now,
                )?;
            }
        }
        transaction.commit()?;
        Ok(expired.len() as u64)
    }

    /// 返回精确 Run/Task 下按完成时间倒序选中的成功 Attempt 正式产物；
    /// 中途的 Agent/Tool Artifact 不在成功输出索引中，不能仅凭事件引用当作任务结果。
    // 按 run+task 选择最新 succeeded Attempt，再由 helper 校验 event/index/Artifact 三者对应。
    pub fn committed_task_outputs(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
    ) -> StoreResult<Vec<Artifact>> {
        let connection = self.connection()?;
        let attempt_id = connection
            .query_row(
                r#"SELECT a.attempt_id
                   FROM rebuild_tasks AS t
                   JOIN rebuild_attempts AS a ON a.task_id = t.task_id
                  WHERE t.run_id = ?1
                    AND t.task_id = ?2
                    AND t.status = 'succeeded'
                    AND a.status = 'succeeded'
                  ORDER BY a.finished_at DESC, a.attempt_id DESC
                  LIMIT 1"#,
                params![run_id.0, task_id.0],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::CommittedOutputTask {
                run_id: run_id.clone(),
                task_id: task_id.clone(),
            })?;
        read_committed_attempt_outputs(&connection, Some(run_id), task_id, &AttemptId(attempt_id))
    }

    /// As [`Self::committed_task_outputs`], but permits an explicitly
    /// successful no-output gate. The task/attempt still had to reach durable
    /// `succeeded`; callers must never use this for arbitrary running work.
    // 仅把 CommittedOutputAttempt 这个“成功 gate 无 output”形状转换为空 Vec；其它存储/完整性错误继续传播。
    pub fn succeeded_task_outputs_or_empty(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
    ) -> StoreResult<Vec<Artifact>> {
        match self.committed_task_outputs(run_id, task_id) {
            Ok(artifacts) => Ok(artifacts),
            Err(StoreError::CommittedOutputAttempt { .. }) => Ok(Vec::new()),
            Err(error) => Err(error),
        }
    }

    pub fn committed_attempt_outputs(
        &self,
        task_id: &TaskId,
        attempt_id: &AttemptId,
    ) -> StoreResult<Vec<Artifact>> {
        // 输入 task_id/attempt_id 必须精确命中成功关系；expected_run_id=None 只省略额外 Run 对照。
        let connection = self.connection()?;
        read_committed_attempt_outputs(&connection, None, task_id, attempt_id)
    }
}
