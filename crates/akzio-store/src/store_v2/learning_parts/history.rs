impl V2Store {
    /// Returns the accepted retrospective for one run/outcome/horizon identity.
    /// The integrity gate guarantees that at most one artifact can match.
    pub fn retrospective_for(
        &self,
        run_id: &RunId,
        outcome_id: &OutcomeId,
        horizon: OutcomeHorizon,
    ) -> StoreResult<Option<Artifact>> {
        let connection = self.connection()?;
        let mut matching = Vec::new();
        for artifact in read_kind_artifacts(&connection, ArtifactKind::Retrospective)? {
            let Some(origin) = artifact.origin.as_ref() else {
                continue;
            };
            if origin.run_id.as_ref() != Some(run_id) {
                continue;
            }
            let payload: Retrospective = self.read_artifact_payload(&artifact)?;
            if payload.outcome_id == *outcome_id && payload.horizon == horizon {
                matching.push(artifact);
            }
        }
        match matching.len() {
            0 => Ok(None),
            1 => Ok(matching.pop()),
            _ => Err(StoreError::Integrity(
                "duplicate retrospective identity".to_owned(),
            )),
        }
    }

    /// Returns the accepted outcome for one run/outcome identity.
    /// Repeated evaluation attempts reuse this immutable materialization.
    pub fn outcome_for(
        &self,
        run_id: &RunId,
        outcome_id: &OutcomeId,
    ) -> StoreResult<Option<Artifact>> {
        let connection = self.connection()?;
        let mut matching = Vec::new();
        for artifact in read_kind_artifacts(&connection, ArtifactKind::Outcome)? {
            let Some(origin) = artifact.origin.as_ref() else {
                continue;
            };
            if origin.run_id.as_ref() != Some(run_id) {
                continue;
            }
            let payload: Outcome = self.read_artifact_payload(&artifact)?;
            if artifact.lifecycle == ArtifactLifecycle::Canonical
                && payload.is_sealed()
                && payload.outcome_id == *outcome_id
            {
                matching.push(artifact);
            }
        }
        match matching.len() {
            0 => Ok(None),
            1 => Ok(matching.pop()),
            _ => Err(StoreError::Integrity(
                "duplicate outcome identity".to_owned(),
            )),
        }
    }

    /// Reads the current policy head without exposing mutable storage to
    /// callers. Previous policy versions remain in `rebuild_policy_transitions`.
    pub fn outcome_schedule_for_run(&self, run_id: &RunId) -> StoreResult<Option<Artifact>> {
        let connection = self.connection()?;
        let mut matching = read_kind_artifacts(&connection, ArtifactKind::OutcomeSchedule)?
            .into_iter()
            .filter(|artifact| {
                artifact
                    .origin
                    .as_ref()
                    .and_then(|origin| origin.run_id.as_ref())
                    == Some(run_id)
                    && artifact.lifecycle == ArtifactLifecycle::Canonical
            })
            .collect::<Vec<_>>();
        for artifact in &matching {
            let schedule: OutcomeSchedule = self.read_artifact_payload(artifact)?;
            schedule.validate()?;
        }
        matching.sort_by_key(|artifact| artifact.created_at);
        match matching.len() {
            0 => Ok(None),
            1 => Ok(matching.pop()),
            _ => Err(StoreError::Integrity(format!(
                "run {run_id} has multiple OutcomeSchedule artifacts"
            ))),
        }
    }

    pub fn outcome_for_run(&self, run_id: &RunId) -> StoreResult<Option<Artifact>> {
        let connection = self.connection()?;
        let mut matching = Vec::new();
        for artifact in read_kind_artifacts(&connection, ArtifactKind::Outcome)? {
            if artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run_id)
            {
                continue;
            }
            let outcome: Outcome = self.read_artifact_payload(&artifact)?;
            if outcome.is_sealed()
                && matches!(
                    artifact.lifecycle,
                    ArtifactLifecycle::Canonical | ArtifactLifecycle::RunScoped
                )
            {
                matching.push(artifact);
            }
        }
        matching.sort_by_key(|artifact| artifact.created_at);
        match matching.len() {
            0 => Ok(None),
            1 => Ok(matching.pop()),
            _ => Err(StoreError::Integrity(format!(
                "run {run_id} has multiple sealed Outcome artifacts"
            ))),
        }
    }

    pub fn policy_head(&self, subject: &PolicySubject) -> StoreResult<Option<PolicyHead>> {
        subject.validate()?;
        let connection = self.connection()?;
        read_policy_head(&connection, subject)
    }

    /// Captures one durable freshness window for all horizons. The returned
    /// cutoff is later committed verbatim; pairs completed after it remain
    /// fresh even if they arrive before evaluation persistence.
    pub fn policy_shadow_pair_snapshot(
        &self,
        subject: &PolicySubject,
    ) -> StoreResult<PolicyShadowPairSnapshot> {
        subject.validate()?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let after_cursor = read_policy_consumption_head(&transaction, subject)?
            .map_or(0, |head| head.consumed_pair_cursor);
        let through_cursor = max_shadow_pair_cursor(&transaction, subject)?;
        let counts_by_horizon =
            shadow_pair_counts_between(&transaction, subject, after_cursor, through_cursor)?;
        transaction.commit()?;
        Ok(PolicyShadowPairSnapshot {
            after_cursor,
            through_cursor,
            counts_by_horizon,
        })
    }

    /// Resolves only policy influences that were durably committed by a
    /// canonical evaluation. Arbitrary Experience/CandidatePolicy artifacts
    /// therefore cannot enter Context or Execution provenance.
    pub fn recorded_policy_influence_subject(
        &self,
        artifact_id: &ArtifactId,
    ) -> StoreResult<Option<PolicySubject>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"SELECT subject_id, subject_json, 'experience'
               FROM rebuild_policy_evaluations WHERE experience_artifact_id = ?1
               UNION ALL
               SELECT subject_id, subject_json, 'candidate_policy'
               FROM rebuild_policy_evaluations WHERE candidate_policy_artifact_id = ?1"#,
        )?;
        let rows = statement
            .query_map(params![artifact_id.0.as_str()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        if rows.is_empty() {
            return Ok(None);
        }
        let artifact = read_artifact(&connection, artifact_id)?;
        let mut resolved = None;
        for (subject_id, subject_json, influence_kind) in rows {
            let expected_kind = match influence_kind.as_str() {
                "experience" => ArtifactKind::Experience,
                "candidate_policy" => ArtifactKind::CandidatePolicy,
                _ => unreachable!("query emits fixed influence kinds"),
            };
            if artifact.kind != expected_kind {
                return Err(StoreError::Integrity(format!(
                    "policy influence {artifact_id} has invalid kind"
                )));
            }
            let subject = parse_persisted_subject(&subject_id, &subject_json)?;
            if resolved.as_ref().is_some_and(|current| current != &subject) {
                return Err(StoreError::Integrity(format!(
                    "policy influence {artifact_id} has conflicting subjects"
                )));
            }
            resolved = Some(subject);
        }
        Ok(resolved)
    }

    /// Replays immutable policy transitions in revision order. Consumers use
    /// this for audit/replay; mutations remain limited to
    /// `record_policy_evaluation`.
    pub fn policy_transitions(
        &self,
        subject: &PolicySubject,
    ) -> StoreResult<Vec<PolicyTransitionRecord>> {
        subject.validate()?;
        let connection = self.connection()?;
        read_policy_transitions(&connection, subject)
    }
}
