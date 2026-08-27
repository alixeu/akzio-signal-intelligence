impl V2Store {
    /// Records retry/recovery/replay/shadow lineage as an immutable
    /// RunScoped artifact. Parent attempts must already exist and the
    /// resulting graph is checked for cycles inside the same transaction.
    /// Atomically records one RunScoped partial Outcome snapshot and its
    /// T+1/T+3 retrospective. The snapshot is not indexed as a committed
    /// task output because this worker remains retryable until T+5.
    pub fn record_partial_outcome_retrospective_fenced(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        outcome_artifact: &Artifact,
        retrospective_artifact: &Artifact,
        now: DateTime<Utc>,
    ) -> StoreResult<bool> {
        if outcome_artifact.kind != ArtifactKind::Outcome
            || outcome_artifact.lifecycle != ArtifactLifecycle::RunScoped
            || retrospective_artifact.kind != ArtifactKind::Retrospective
            || retrospective_artifact.lifecycle != ArtifactLifecycle::RunScoped
        {
            return Err(StoreError::InvalidLearningCommit(
                "partial_retrospective.kind_or_lifecycle",
            ));
        }
        outcome_artifact.validate()?;
        retrospective_artifact.validate()?;
        self.read_blob(&outcome_artifact.blob)?;
        self.read_blob(&retrospective_artifact.blob)?;
        let outcome: Outcome = self.read_artifact_payload(outcome_artifact)?;
        outcome.validate()?;
        if outcome.sealed_at.is_some() || outcome.windows.is_empty() {
            return Err(StoreError::InvalidLearningCommit(
                "partial_retrospective.outcome_must_be_unsealed",
            ));
        }
        let retrospective: Retrospective = self.read_artifact_payload(retrospective_artifact)?;
        retrospective.validate()?;
        if retrospective.horizon == OutcomeHorizon::T5
            || retrospective.outcome.artifact_id != outcome_artifact.artifact_id
            || retrospective.outcome.kind != ArtifactKind::Outcome
        {
            return Err(StoreError::InvalidLearningCommit(
                "partial_retrospective.links",
            ));
        }
        if !outcome
            .windows
            .iter()
            .any(|window| window.horizon == retrospective.horizon)
        {
            return Err(StoreError::InvalidLearningCommit(
                "partial_retrospective.horizon_window",
            ));
        }
        let expected_windows = match retrospective.horizon {
            OutcomeHorizon::T1 => 1,
            OutcomeHorizon::T3 => 2,
            OutcomeHorizon::T5 => 0,
        };
        if outcome.windows.len() != expected_windows
            || (retrospective.horizon == OutcomeHorizon::T3
                && !outcome
                    .windows
                    .iter()
                    .any(|window| window.horizon == OutcomeHorizon::T1))
        {
            return Err(StoreError::InvalidLearningCommit(
                "partial_retrospective.prefix_windows",
            ));
        }
        assert_origin_matches(outcome_artifact.origin.as_ref(), permit)?;
        assert_origin_matches(retrospective_artifact.origin.as_ref(), permit)?;

        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;
        assert_permit(&transaction, permit)?;
        assert_paper_run(&transaction, &permit.run_id)?;

        let schedule = read_artifact(&transaction, &outcome.schedule.artifact_id)?;
        if schedule.kind != ArtifactKind::OutcomeSchedule {
            return Err(StoreError::InvalidLearningCommit(
                "partial_retrospective.schedule_kind",
            ));
        }
        for source in &outcome.market_evidence {
            let evidence = read_artifact(&transaction, &source.artifact_id)?;
            if evidence.kind != source.kind {
                return Err(StoreError::InvalidLearningCommit(
                    "partial_retrospective.market_evidence_kind",
                ));
            }
        }
        if !has_exact_source_refs(
            outcome_artifact,
            &std::iter::once(outcome.schedule.clone())
                .chain(outcome.market_evidence.iter().cloned())
                .collect::<Vec<_>>(),
        ) {
            return Err(StoreError::InvalidLearningCommit(
                "partial_retrospective.outcome_source_refs",
            ));
        }
        let outcome_ref = ArtifactRef {
            artifact_id: outcome_artifact.artifact_id.clone(),
            kind: ArtifactKind::Outcome,
        };
        if !retrospective_artifact.source_refs.contains(&outcome_ref) {
            return Err(StoreError::InvalidLearningCommit(
                "partial_retrospective.source_refs",
            ));
        }
        for source in &retrospective_artifact.source_refs {
            if source.artifact_id == outcome_artifact.artifact_id {
                continue;
            }
            read_artifact(&transaction, &source.artifact_id)?;
        }

        for existing in read_kind_artifacts(&transaction, ArtifactKind::Retrospective)? {
            let Some(origin) = existing.origin.as_ref() else {
                continue;
            };
            if origin.run_id.as_ref() != Some(&permit.run_id) {
                continue;
            }
            let existing_payload: Retrospective = self.read_artifact_payload(&existing)?;
            if existing_payload.outcome_id == retrospective.outcome_id
                && existing_payload.horizon == retrospective.horizon
            {
                if existing == *retrospective_artifact {
                    transaction.commit()?;
                    return Ok(false);
                }
                return Err(StoreError::Integrity(
                    "duplicate retrospective identity different payload".to_owned(),
                ));
            }
        }

        insert_artifact(&transaction, outcome_artifact)?;
        append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::ArtifactCommitted,
            Some(&outcome_artifact.artifact_id),
            now,
        )?;
        insert_artifact(&transaction, retrospective_artifact)?;
        append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::RetrospectiveCreated,
            Some(&retrospective_artifact.artifact_id),
            now,
        )?;
        transaction.commit()?;
        Ok(true)
    }


    /// Read only accepted retrospective artifacts for a run. Drafts and
    /// AgentTurn/provider payloads never cross this query boundary.
    pub fn retrospectives(&self, run_id: &RunId) -> StoreResult<Vec<Artifact>> {
        let connection = self.connection()?;
        let mut artifacts = read_kind_artifacts(&connection, ArtifactKind::Retrospective)?
            .into_iter()
            .filter(|artifact| {
                artifact
                    .origin
                    .as_ref()
                    .and_then(|origin| origin.run_id.as_ref())
                    == Some(run_id)
            })
            .collect::<Vec<_>>();
        artifacts.sort_by_key(|artifact| artifact.created_at);
        Ok(artifacts)
    }
}
