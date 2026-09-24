// 文件导读：本文件处理取消、defer/retry 和 claim。Task 状态、Attempt lease、Run 状态、
// 生命周期事件与 Debug control 必须在同一事务内收束，读取到的 retry 次数来自 durable rows。
// 写路径由 Scheduler/Worker 调用；先看 cancel/defer/retry 的终态边界，再读 claim_next_task... 的
// SQL 选择条件、epoch permit 构造和 Attempt/Task/event 一次提交。
impl Store {
    // 从 Task 的持久事件统计当前 Outcome stage 的失败尝试数；handler 不传自报计数。
    pub fn failure_attempts_for_current_stage(&self, task_id: &TaskId) -> StoreResult<u64> {
        let connection = self.connection()?;
        task_failure_attempt_count(&connection, task_id)
    }

    /// Request cancellation once. Queued tasks are durably cancelled in the
    /// same transaction; running attempts observe this request through
    /// [`Self::run_cancel_requested`] and finish through their permit.
    pub fn request_run_cancel(
        &self,
        run_id: &RunId,
        reason: &str,
        now: DateTime<Utc>,
    ) -> StoreResult<bool> {
        // reason 必须非空；Immediate 事务确认 Run 存在后 INSERT OR IGNORE cancellation。
        // 首次插入会同事务追加 RunCancelRequested、取消 queued Task 并刷新 Run/control；重复请求返回 false。
        if reason.trim().is_empty() {
            return Err(StoreError::Domain(DomainError::EmptyField {
                field: "run_cancel.reason",
            }));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let exists = transaction
            .query_row(
                "SELECT 1 FROM rebuild_runs WHERE run_id = ?1",
                params![run_id.0],
                |_| Ok(()),
            )
            .optional()?;
        if exists.is_none() {
            return Err(StoreError::MissingRun(run_id.clone()));
        }
        let inserted = transaction.execute(
            r#"INSERT OR IGNORE INTO rebuild_run_cancellations (run_id, reason, requested_at)
               VALUES (?1, ?2, ?3)"#,
            params![run_id.0, reason, now.to_rfc3339()],
        )?;
        if inserted == 0 {
            transaction.commit()?;
            return Ok(false);
        }
        append_event(
            &transaction,
            run_id,
            None,
            None,
            LifecycleEventType::RunCancelRequested,
            None,
            now,
        )?;
        cancel_queued_tasks(&transaction, run_id, now)?;
        refresh_run_status(&transaction, run_id, now)?;
        super::run_control::settle_continuous(&transaction, run_id, now)?;
        transaction.commit()?;
        Ok(true)
    }

    pub fn run_cancel_requested(&self, run_id: &RunId) -> StoreResult<bool> {
        // 按 run_id 查询取消 marker；存在为 true，不存在为 false，不触碰 Task 或 Run 状态。
        let connection = self.connection()?;
        Ok(connection
            .query_row(
                "SELECT 1 FROM rebuild_run_cancellations WHERE run_id = ?1",
                params![run_id.0],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
    }

    /// 将持有 permit 的 Task 延期到未来 `ready_at`；Attempt 记为 deferred，
    /// 不读取也不消费失败重试次数。它既不是 retry，也不是任务终态。
    pub fn defer_task(
        &self,
        permit: &TaskWritePermit,
        ready_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        // defer 的 ready_at 必须晚于 now；事务内断言 permit 后把 Task 重新排队、关闭当前 Attempt 为 deferred，
        // 追加 event 并结算 Debug control。此路径不读取 retry budget，也不消费失败重试次数。
        if ready_at <= now {
            return Err(StoreError::InvalidTaskDeferral(permit.task_id.clone()));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
        transaction.execute(
            r#"UPDATE rebuild_tasks
               SET status = 'queued', lease_id = NULL, active_attempt_id = NULL,
                   worker_id = NULL, lease_until = NULL, ready_at = ?1
               WHERE task_id = ?2"#,
            params![ready_at.to_rfc3339(), permit.task_id.0],
        )?;
        transaction.execute(
            "UPDATE rebuild_attempts SET status = 'deferred', finished_at = ?1 WHERE attempt_id = ?2",
            params![now.to_rfc3339(), permit.attempt_id.0],
        )?;
        append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::TaskDeferred,
            None,
            now,
        )?;
        debug::settle_attempt(&transaction, permit, "deferred", now)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn retry_task(
        &self,
        permit: &TaskWritePermit,
        retry_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> StoreResult<RetryTaskResult> {
        // retry_at 由调用者给定；真正的可重试额度从 Task policy 与 durable Attempt rows 重算。
        // 仍有额度时 requeue 并返回 Requeued；耗尽时写 exhausted event、按 on_failure 收束并返回 Terminal。
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_workflow_executable(&transaction, &permit.run_id)?;
        assert_permit(&transaction, permit)?;
        let (retry, on_failure) = task_retry_policy(&transaction, &permit.task_id)?;
        let attempt_count = task_failure_attempt_count(&transaction, &permit.task_id)?;
        if attempt_count < u64::from(retry.max_attempts) {
            transaction.execute(
                r#"UPDATE rebuild_tasks
                   SET status = 'queued', lease_id = NULL, active_attempt_id = NULL,
                       worker_id = NULL, lease_until = NULL, ready_at = ?1
                   WHERE task_id = ?2"#,
                params![retry_at.to_rfc3339(), permit.task_id.0],
            )?;
            transaction.execute(
                "UPDATE rebuild_attempts SET status = 'retried', finished_at = ?1 WHERE attempt_id = ?2",
                params![now.to_rfc3339(), permit.attempt_id.0],
            )?;
            append_event(
                &transaction,
                &permit.run_id,
                Some(&permit.task_id),
                Some(&permit.attempt_id),
                LifecycleEventType::TaskRetryScheduled,
                None,
                now,
            )?;
            debug::settle_attempt(&transaction, permit, "retry_scheduled", now)?;
            transaction.commit()?;
            return Ok(RetryTaskResult::Requeued);
        }

        append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::TaskRetryExhausted,
            None,
            now,
        )?;
        let status = finish_permitted_task(
            &transaction,
            permit,
            TaskStatus::Failed,
            on_failure,
            None,
            now,
        )?;
        transaction.commit()?;
        Ok(RetryTaskResult::Terminal(status))
    }

    pub fn claim_next_task_for_workload(
        &self,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_for: Duration,
        workload: TaskWorkload,
    ) -> StoreResult<Option<ClaimedAttempt>> {
        // 普通入口不提供 runtime_identity，统一委派到更完整入口；Debug task 会因 identity 不匹配而不可领取。
        self.claim_next_task_for_workload_with_identity(worker_id,now,lease_for,workload,None)
    }

    pub fn claim_next_task_for_workload_with_identity(
        &self, worker_id:&str, now:DateTime<Utc>, lease_for:Duration,
        workload:TaskWorkload, runtime_identity:Option<&ContentHash>,
    ) -> StoreResult<Option<ClaimedAttempt>> {
        // worker_id 必须非空；IMMEDIATE 事务按 workload、ready_at、Debug identity/status、Run 状态、
        // cancellation 和依赖未完成条件选择一条 queued Task：先按 ready_at 升序，
        // 同时刻再按 priority 降序及 task_id 排序，并非跨不同 ready_at 的全局最高优先级。
        // 无候选时提交只读事务并返回 None；命中时以 lease_epoch+1 和新 Attempt/Lease ID 构造 permit，
        // 条件 UPDATE、Attempt insert、Run/event/AttemptRelation、Debug permit consumption 共同提交。
        if worker_id.trim().is_empty() {
            return Err(StoreError::Domain(DomainError::EmptyField {
                field: "worker_id",
            }));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let selected = transaction
            .query_row(
        r#"SELECT t.task_id, t.run_id, t.recipe_id, t.objective, t.contract_hash, t.priority,
        t.budget_json, t.retry_json, t.on_failure, t.parent_task_id, t.input_artifacts_json, t.node_spec_json
                    FROM rebuild_tasks AS t
                    JOIN rebuild_runs AS r ON r.run_id = t.run_id
                    LEFT JOIN rebuild_run_controls AS debug ON debug.run_id = t.run_id
               WHERE t.status = 'queued' AND t.ready_at <= ?1
                 AND (debug.identity_artifact_id IS NOT NULL OR NOT EXISTS (SELECT 1 FROM rebuild_metadata WHERE key='debug_environment'))
                 AND (debug.identity_artifact_id IS NULL OR debug.runtime_identity = ?4)
                 AND (debug.status = 'running'
                      OR (debug.status = 'stepping' AND debug.permitted_task_id = t.task_id
                          AND debug.active_attempt_id IS NULL))
                 AND (?3 = 0 OR (?3 = 1 AND t.recipe_id != ?2) OR (?3 = 2 AND t.recipe_id = ?2))
                 AND (r.status IN ('queued', 'running')
                      OR (r.status = 'completed' AND t.recipe_id = ?2))
              AND NOT EXISTS (
                  SELECT 1 FROM rebuild_run_cancellations AS c WHERE c.run_id = t.run_id
              )
              AND NOT EXISTS (
                        SELECT 1 FROM rebuild_task_dependencies AS d
                        JOIN rebuild_tasks AS p ON p.task_id = d.depends_on_task_id
                        WHERE d.task_id = t.task_id AND p.status NOT IN ('succeeded', 'skipped')
                      )
                    ORDER BY t.ready_at ASC, t.priority DESC, t.task_id ASC LIMIT 1"#,
                params![now.to_rfc3339(), POST_TERMINAL_WORKER_RECIPE_ID, workload.query_code(),runtime_identity.map(ContentHash::as_str)],
            row_to_node,
            )
            .optional()?;
        let Some((run_id, mut node)) = selected else {
            transaction.commit()?;
            return Ok(None);
        };
        assert_workflow_executable(&transaction, &run_id)?;
        node.dependencies = task_dependencies(&transaction, &node.task_id)?;
        let permit = TaskWritePermit {
            run_id: run_id.clone(),
            task_id: node.task_id.clone(),
            attempt_id: akzio_domain::AttemptId::new(),
            lease_id: akzio_domain::LeaseId::new(),
            epoch: transaction.query_row(
                "SELECT lease_epoch + 1 FROM rebuild_tasks WHERE task_id = ?1",
                params![node.task_id.0],
                |row| row.get(0),
            )?,
            contract_hash: node.contract_hash.clone(),
        };
        let previous_attempt = transaction
            .query_row(
                "SELECT attempt_id, status FROM rebuild_attempts WHERE task_id = ?1 ORDER BY started_at DESC, attempt_id DESC LIMIT 1",
                params![node.task_id.0],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        // 只在仍为 queued 的 WHERE 下领取；并发条件不成立即 TaskNotRunnable，不返回半成品 permit。
        let updated = transaction.execute(
            r#"UPDATE rebuild_tasks
               SET status = 'running', lease_id = ?1, lease_epoch = ?2, active_attempt_id = ?3,
                   lease_until = ?4, worker_id = ?5
               WHERE task_id = ?6 AND status = 'queued'"#,
            params![
                permit.lease_id.0,
                permit.epoch,
                permit.attempt_id.0,
                (now + lease_for).to_rfc3339(),
                worker_id,
                permit.task_id.0,
            ],
        )?;
        if updated != 1 {
            return Err(StoreError::TaskNotRunnable(permit.task_id));
        }
        transaction.execute(
            r#"INSERT INTO rebuild_attempts
               (attempt_id, task_id, run_id, lease_id, epoch, worker_id, status, started_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'running', ?7)"#,
            params![
                permit.attempt_id.0,
                permit.task_id.0,
                permit.run_id.0,
                permit.lease_id.0,
                permit.epoch,
                worker_id,
                now.to_rfc3339(),
            ],
        )?;
        transaction.execute(
            "UPDATE rebuild_runs SET status = 'running' WHERE run_id = ?1 AND status = 'queued'",
            params![permit.run_id.0],
        )?;
        append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::TaskStarted,
            None,
            now,
        )?;
        if let Some((parent_attempt_id, parent_status)) = previous_attempt {
            // 上一 Attempt 为 abandoned 记 Recovery，其它终态记 Retry；关系 Artifact/event 仍在 claim 事务内。
            let relation = if parent_status == "abandoned" {
                AttemptRelationKind::Recovery
            } else {
                AttemptRelationKind::Retry
            };
            self.record_attempt_relation_in_transaction(
                &transaction,
                &permit,
                &AttemptId(parent_attempt_id),
                relation,
                now,
            )?;
        }
        debug::consume_claim(&transaction, &permit, now)?;
        // permit 只有在事务成功 commit 后才交给 worker；失败时 Task 与 Attempt 行一并回滚。
        transaction.commit()?;
        Ok(Some(ClaimedAttempt {
            run_id,
            node,
            permit,
        }))
    }
}
