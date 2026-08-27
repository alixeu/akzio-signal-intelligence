use super::*;

impl V2Store {
    pub fn commit_execution(
        &self,
        lease: &DaemonLease,
        commit: &ExecutionCommit,
    ) -> StoreResult<ExecutionCommitResult> {
        if commit.session_key.trim().is_empty()
            || commit.commitment.kind != ArtifactKind::ExecutionCommitment
        {
            return Err(StoreError::InvalidSessionSlot(commit.session_key.clone()));
        }
        commit.commitment.validate()?;
        let payload: PaperCommitment =
            serde_json::from_slice(&self.read_blob(&commit.commitment.blob)?)?;
        payload.validate()?;
        if payload.broker_session != commit.session_key
            || !commit
                .commitment
                .source_refs
                .iter()
                .any(|source| source == &payload.execution_context)
        {
            return Err(StoreError::InvalidSessionSlot(commit.session_key.clone()));
        }

        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, commit.committed_at)?;
        assert_permit(&transaction, &commit.permit)?;
        assert_paper_run(&transaction, &commit.permit.run_id)?;
        assert_origin_matches(commit.commitment.origin.as_ref(), &commit.permit)?;
        let plan = self.validate_execution_commitment_lineage(
            &transaction,
            &commit.commitment,
            &payload,
            &commit.permit.run_id,
            &commit.session_key,
        )?;
        self.validate_consumed_paper_approval(
            &transaction,
            &commit.session_key,
            &plan,
            commit.committed_at,
        )?;
        let (_, on_failure) = task_retry_policy(&transaction, &commit.permit.task_id)?;
        let slot = transaction
            .query_row(
                "SELECT run_id, commitment_artifact_id FROM rebuild_session_slots WHERE session_key = ?1",
                params![commit.session_key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()?;
        let Some((run_id, existing_commitment)) = slot else {
            return Err(StoreError::InvalidSessionSlot(commit.session_key.clone()));
        };
        if run_id != commit.permit.run_id.0 {
            return Err(StoreError::InvalidSessionSlot(commit.session_key.clone()));
        }
        if let Some(existing_commitment) = existing_commitment {
            if existing_commitment == commit.commitment.artifact_id.0.as_str() {
                let existing_artifact = read_artifact(
                    &transaction,
                    &ArtifactId(ContentHash::new(existing_commitment)?),
                )?;
                let existing_payload: PaperCommitment =
                    serde_json::from_slice(&self.read_blob(&existing_artifact.blob)?)?;
                self.validate_execution_commitment_lineage(
                    &transaction,
                    &existing_artifact,
                    &existing_payload,
                    &commit.permit.run_id,
                    &commit.session_key,
                )?;
                let event_id = append_event(
                    &transaction,
                    &commit.permit.run_id,
                    Some(&commit.permit.task_id),
                    Some(&commit.permit.attempt_id),
                    LifecycleEventType::ArtifactCommitted,
                    Some(&commit.commitment.artifact_id),
                    commit.committed_at,
                )?;
                record_attempt_output(
                    &transaction,
                    &commit.permit,
                    &commit.commitment.artifact_id,
                    event_id,
                )?;
                append_event(
                    &transaction,
                    &commit.permit.run_id,
                    Some(&commit.permit.task_id),
                    Some(&commit.permit.attempt_id),
                    LifecycleEventType::ExecutionCommitmentRecovered,
                    Some(&commit.commitment.artifact_id),
                    commit.committed_at,
                )?;
                finish_permitted_task(
                    &transaction,
                    &commit.permit,
                    TaskStatus::Succeeded,
                    on_failure,
                    Some(&commit.commitment.artifact_id),
                    commit.committed_at,
                )?;
                transaction.commit()?;
                return Ok(ExecutionCommitResult {
                    commitment_artifact_id: commit.commitment.artifact_id.clone(),
                    newly_committed: false,
                });
            }
            let existing_artifact_id = ArtifactId(ContentHash::new(existing_commitment)?);
            let existing_artifact = read_artifact(&transaction, &existing_artifact_id)?;
            if existing_artifact.kind == ArtifactKind::ExecutionCommitment {
                let existing_payload: PaperCommitment =
                    serde_json::from_slice(&self.read_blob(&existing_artifact.blob)?)?;
                self.validate_execution_commitment_lineage(
                    &transaction,
                    &existing_artifact,
                    &existing_payload,
                    &commit.permit.run_id,
                    &commit.session_key,
                )?;
                if same_paper_commitment(&existing_payload, &payload) {
                    let event_id = append_event(
                        &transaction,
                        &commit.permit.run_id,
                        Some(&commit.permit.task_id),
                        Some(&commit.permit.attempt_id),
                        LifecycleEventType::ArtifactCommitted,
                        Some(&existing_artifact_id),
                        commit.committed_at,
                    )?;
                    record_attempt_output(
                        &transaction,
                        &commit.permit,
                        &existing_artifact_id,
                        event_id,
                    )?;
                    append_event(
                        &transaction,
                        &commit.permit.run_id,
                        Some(&commit.permit.task_id),
                        Some(&commit.permit.attempt_id),
                        LifecycleEventType::ExecutionCommitmentRecovered,
                        Some(&existing_artifact_id),
                        commit.committed_at,
                    )?;
                    finish_permitted_task(
                        &transaction,
                        &commit.permit,
                        TaskStatus::Succeeded,
                        on_failure,
                        Some(&existing_artifact_id),
                        commit.committed_at,
                    )?;
                    transaction.commit()?;
                    return Ok(ExecutionCommitResult {
                        commitment_artifact_id: existing_artifact_id,
                        newly_committed: false,
                    });
                }
            }
            return Err(StoreError::DuplicateExecutionCommitment(
                commit.session_key.clone(),
            ));
        }
        insert_artifact(&transaction, &commit.commitment)?;
        transaction.execute(
            "UPDATE rebuild_session_slots SET commitment_artifact_id = ?1, committed_at = ?2 WHERE session_key = ?3 AND commitment_artifact_id IS NULL",
            params![
                commit.commitment.artifact_id.0.as_str(),
                commit.committed_at.to_rfc3339(),
                commit.session_key,
            ],
        )?;
        let event_id = append_event(
            &transaction,
            &commit.permit.run_id,
            Some(&commit.permit.task_id),
            Some(&commit.permit.attempt_id),
            LifecycleEventType::ArtifactCommitted,
            Some(&commit.commitment.artifact_id),
            commit.committed_at,
        )?;
        record_attempt_output(
            &transaction,
            &commit.permit,
            &commit.commitment.artifact_id,
            event_id,
        )?;
        append_event(
            &transaction,
            &commit.permit.run_id,
            Some(&commit.permit.task_id),
            Some(&commit.permit.attempt_id),
            LifecycleEventType::ExecutionCommitted,
            Some(&commit.commitment.artifact_id),
            commit.committed_at,
        )?;
        finish_permitted_task(
            &transaction,
            &commit.permit,
            TaskStatus::Succeeded,
            on_failure,
            Some(&commit.commitment.artifact_id),
            commit.committed_at,
        )?;
        transaction.commit()?;
        Ok(ExecutionCommitResult {
            commitment_artifact_id: commit.commitment.artifact_id.clone(),
            newly_committed: true,
        })
    }

    /// Return the one durable r0 -> r1 intent for an order in a committed
    /// Paper session. The table is only an immutable-history index; callers
    /// still consume the returned artifact and its provenance.
    pub fn reprice_for(
        &self,
        commitment: &ArtifactRef,
        asset: Asset,
    ) -> StoreResult<Option<Artifact>> {
        if commitment.kind != ArtifactKind::ExecutionCommitment {
            return Err(StoreError::InvalidExecutionReprice);
        }
        let connection = self.connection()?;
        let artifact_id = connection
            .query_row(
                "SELECT reprice_artifact_id FROM rebuild_execution_reprices \
                 WHERE commitment_artifact_id = ?1 AND asset = ?2",
                params![commitment.artifact_id.0.as_str(), asset.symbol()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        artifact_id
            .map(ContentHash::new)
            .transpose()?
            .map(ArtifactId)
            .map(|artifact_id| read_artifact(&connection, &artifact_id))
            .transpose()
    }

    /// Atomically records the single Rust-owned Paper effect intent and
    /// terminally completes its task. The broker
    /// adapter may receive only the returned immutable intent afterwards.
    pub fn record_paper_effect_intent(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        effect: &ArtifactRef,
        now: DateTime<Utc>,
    ) -> StoreResult<bool> {
        self.validate_paper_effect_artifact(effect, &permit.run_id)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;
        assert_permit(&transaction, permit)?;
        assert_paper_run(&transaction, &permit.run_id)?;
        assert_paper_effect_artifact(&transaction, effect, &permit.run_id)?;
        validate_paper_effect_events(&transaction, Some(&permit.run_id))?;
        if paper_effect_terminal_exists(&transaction, &permit.run_id, &effect.artifact_id)? {
            return Err(StoreError::PaperEffectAlreadySettled(
                effect.artifact_id.clone(),
            ));
        }
        let already_recorded =
            paper_effect_intent_exists(&transaction, &permit.run_id, &effect.artifact_id)?;
        if already_recorded {
            transaction.commit()?;
            return Ok(true);
        }
        append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::ExecutionEffectIntent,
            Some(&effect.artifact_id),
            now,
        )?;
        transaction.commit()?;
        Ok(false)
    }

    /// Commit Paper reconciliation artifacts and the effect settlement marker
    /// under the same daemon lease/attempt fence and SQLite transaction.
    pub fn commit_fenced_attempt_with_effect(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        artifacts: &[Artifact],
        effect: &ArtifactRef,
        recovered: bool,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        self.validate_attempt_commit(permit, artifacts, TaskStatus::Succeeded)?;
        self.validate_paper_effect_artifact(effect, &permit.run_id)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;
        assert_permit(&transaction, permit)?;
        assert_paper_run(&transaction, &permit.run_id)?;
        assert_paper_effect_artifact(&transaction, effect, &permit.run_id)?;
        validate_paper_effect_events(&transaction, Some(&permit.run_id))?;
        if paper_effect_terminal_exists(&transaction, &permit.run_id, &effect.artifact_id)? {
            return Err(StoreError::PaperEffectAlreadySettled(
                effect.artifact_id.clone(),
            ));
        }
        if !paper_effect_intent_exists(&transaction, &permit.run_id, &effect.artifact_id)? {
            return Err(StoreError::MissingPaperEffectIntent(
                effect.artifact_id.clone(),
            ));
        }
        commit_attempt_transaction_with_effect(
            &transaction,
            permit,
            artifacts,
            TaskStatus::Succeeded,
            Some((
                effect,
                if recovered {
                    LifecycleEventType::ExecutionEffectRecovered
                } else {
                    LifecycleEventType::ExecutionEffectSettled
                },
            )),
            now,
        )?;
        transaction.commit()?;
        Ok(())
    }
}
