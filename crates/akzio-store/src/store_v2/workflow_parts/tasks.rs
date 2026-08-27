impl V2Store {
    pub fn heartbeat_task(
        &self,
        permit: &TaskWritePermit,
        expires_at: DateTime<Utc>,
    ) -> StoreResult<()> {
        let connection = self.connection()?;
        let updated = connection.execute(
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
        Ok(())
    }

    /// Verifies that a handler still owns the active task attempt without
    /// creating an artifact or changing task state. External adapters use
    /// this immediately before side effects; final persistence rechecks the
    /// same permit in its own transaction.
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
        self.write_task_artifact_fenced(None, permit, artifact, event_type, now)
    }

    /// Persist a task artifact while optionally fencing a daemon-owned worker.
    /// The lease check is in the same transaction as the artifact/event write,
    /// so a takeover cannot leave a stale worker's output committed.
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
