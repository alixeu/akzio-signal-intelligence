// 文件导读：本文件处理 Task permit 的 heartbeat、事件、Artifact 写入和 Attempt 提交；
// 每个公开写入口都在最终事务内再次核验 permit，外部副作用不能仅凭预检查获得授权。
// 先读 write_task_artifact 与 commit_attempt 区分单项 Artifact 和成功 Attempt outputs，
// 再看 heartbeat/verify_attempt_terminal 的 lease/终态检查；所有持久写操作都复用 Immediate 事务。
impl Store {
    // 先确认 Task/Attempt 仍处于 permit 指定状态，再读取持久 lease_until；
    // 只把 expiry 向后延长，不改 owner、epoch 或 active Attempt，且条件 UPDATE 失败报 StalePermit。
    pub fn heartbeat_task(
        &self,
        permit: &TaskWritePermit,
        expires_at: DateTime<Utc>,
    ) -> StoreResult<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
        let current: String = transaction.query_row(
            "SELECT lease_until FROM rebuild_tasks WHERE task_id = ?1",
            params![permit.task_id.0],
            |row| row.get(0),
        )?;
        // A heartbeat queued before maintenance must not shorten the lease
        // extended by maintenance, nor revive an expired lease.
        let expires_at = expires_at.max(parse_time(&current)?);
        let updated = transaction.execute(
            r#"UPDATE rebuild_tasks SET lease_until = ?1
               WHERE task_id = ?2 AND status = 'running' AND lease_id = ?3 AND lease_epoch = ?4
                 AND active_attempt_id = ?5"#,
            params![
                expires_at.to_rfc3339(),
                permit.task_id.0,
                permit.lease_id.0,
                permit.epoch,
                permit.attempt_id.0,
            ],
        )?;
        if updated != 1 {
            return Err(StoreError::StalePermit(permit.task_id.clone()));
        }
        transaction.commit()?;
        Ok(())
    }

    /// Verifies that a handler still owns the active task attempt without
    /// creating an artifact or changing task state. External adapters use
    /// this immediately before side effects; final persistence rechecks the
    /// same permit in its own transaction.
    // 只执行一次 Immediate 事务内 assert_permit；该预检之后的外部动作仍必须由最终提交再次核验。
    pub fn validate_task_permit(&self, permit: &TaskWritePermit) -> StoreResult<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
        transaction.commit()?;
        Ok(())
    }

    /// Append a task-scoped lifecycle fact without creating an artifact.
    /// The permit check and event insert share one transaction so a stale
    /// attempt cannot publish an AgentTurnStarted fact after takeover.
    // 事件形状校验和 AgentTurn 全 Run 生命周期校验都在插入事务内；只有全部通过才 commit。
    pub fn append_task_event(
        &self,
        permit: &TaskWritePermit,
        event_type: LifecycleEventType,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
        append_task_event(&transaction, permit, event_type, now)?;
        validate_agent_turn_lifecycle_events(&transaction, Some(&permit.run_id))?;
        transaction.commit()?;
        Ok(())
    }

    /// Verify a handler-owned transaction already closed this exact attempt.
    /// A merely stale permit is insufficient: task and attempt terminal state,
    /// run, lease, epoch, and contract must all still identify the caller.
    // 用 Deferred 快照查询指定 Attempt，要求 Task 与 Attempt 的 status 都等于传入 terminal status，
    // 同时 Task active_attempt_id 已清空；始终校验 Tool 生命周期，succeeded 时额外要求无未结 ToolCall。
    pub fn verify_attempt_terminal(
        &self,
        permit: &TaskWritePermit,
        status: TaskStatus,
    ) -> StoreResult<()> {
        if !status.is_terminal() {
            return Err(StoreError::TaskNotRunnable(permit.task_id.clone()));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let current = transaction
            .query_row(
                r#"SELECT t.run_id, t.status, t.active_attempt_id, t.contract_hash,
                          a.task_id, a.run_id, a.lease_id, a.epoch, a.status
                   FROM rebuild_attempts AS a
                   JOIN rebuild_tasks AS t ON t.task_id = a.task_id
                   WHERE a.attempt_id = ?1"#,
                params![permit.attempt_id.0],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, u64>(7)?,
                        row.get::<_, String>(8)?,
                    ))
                },
            )
            .optional()?;
        let Some(current) = current else {
            return Err(StoreError::StalePermit(permit.task_id.clone()));
        };
        let expected_contract = permit.contract_hash.as_ref().map(ContentHash::as_str);
        if current.0 != permit.run_id.0
            || current.1 != enum_name(status)
            || current.2.is_some()
            || current.3.as_deref() != expected_contract
            || current.4 != permit.task_id.0
            || current.5 != permit.run_id.0
            || current.6 != permit.lease_id.0
            || current.7 != permit.epoch
            || current.8 != enum_name(status)
        {
            return Err(StoreError::StalePermit(permit.task_id.clone()));
        }
        validate_tool_lifecycle_events(&transaction, Some(&permit.run_id))?;
        if status == TaskStatus::Succeeded {
            ensure_no_pending_tool_calls(
                &transaction,
                &permit.run_id,
                &permit.task_id,
                &permit.attempt_id,
            )?;
        }
        Ok(())
    }

    pub fn write_task_artifact(
        &self,
        permit: &TaskWritePermit,
        artifact: &Artifact,
        event_type: LifecycleEventType,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        // 不带 daemon lease 的薄 wrapper；调用方给的 permit 与 Artifact 仍由 fenced 入口核验。
        self.write_task_artifact_fenced(None, permit, artifact, event_type, now)
    }

    /// Persist a task artifact while optionally fencing a daemon-owned worker.
    /// The lease check is in the same transaction as the artifact/event write,
    /// so a takeover cannot leave a stale worker's output committed.
    // Artifact/domain/BLOB 专项预检先发生；Immediate 事务内若提供 daemon lease 则重验 lease，
    // 再核验 Task permit/lifecycle/origin，插入 Artifact、event 并重跑生命周期校验后提交。
    // 此单 Artifact API 不把 Artifact 自动登记成 succeeded Attempt output，也不终结 Task。
    pub fn write_task_artifact_fenced(
        &self,
        lease: Option<&DaemonLease>,
        permit: &TaskWritePermit,
        artifact: &Artifact,
        event_type: LifecycleEventType,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        artifact.validate()?;
        reject_generic_learning_artifact(artifact)?;
        self.read_blob(&artifact.blob)?;
        self.validate_specialized_artifact(artifact)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(lease) = lease {
            assert_daemon_lease(&transaction, lease, Utc::now())?;
        }
        assert_permit(&transaction, permit)?;
        assert_task_artifact_lifecycle(&transaction, &permit.run_id, artifact)?;
        assert_origin_matches(artifact.origin.as_ref(), permit)?;
        insert_artifact(&transaction, artifact)?;
        append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            event_type,
            Some(&artifact.artifact_id),
            now,
        )?;
        validate_tool_lifecycle_events(&transaction, Some(&permit.run_id))?;
        validate_agent_turn_lifecycle_events(&transaction, Some(&permit.run_id))?;
        validate_context_lifecycle_events(&transaction, Some(&permit.run_id))?;
        validate_gate_lifecycle_events(&transaction, Some(&permit.run_id))?;
        transaction.commit()?;
        Ok(())
    }

    /// Commit the final artifacts and terminal task state together. A reader
    /// cannot observe a completed attempt without every committed output and
    /// its corresponding durable events.
    // 先验证非空成功输出、BLOB 和专项 payload；在一笔 Immediate 事务插入整个 Artifact 闭包、
    // committed events/output index 并终结 Task/Attempt。
    pub fn commit_attempt(
        &self,
        permit: &TaskWritePermit,
        artifacts: &[Artifact],
        status: TaskStatus,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        self.validate_attempt_commit(permit, artifacts, status)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        commit_attempt_transaction(&transaction, permit, artifacts, status, now)?;
        transaction.commit()?;
        Ok(())
    }

    /// Atomically persist broker-visible task outputs only while both the
    /// daemon epoch and task attempt permit remain current.
    // 与 commit_attempt 相同，但 daemon lease、Attempt permit 和所有 outputs 共用最终事务提交点。
    pub fn commit_fenced_attempt(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        artifacts: &[Artifact],
        status: TaskStatus,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        self.validate_attempt_commit(permit, artifacts, status)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;
        commit_attempt_transaction(&transaction, permit, artifacts, status, now)?;
        transaction.commit()?;
        Ok(())
    }
}
