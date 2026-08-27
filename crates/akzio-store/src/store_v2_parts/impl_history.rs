impl V2Store {
    fn verify_contract_catalogue_history(&self, connection: &Connection) -> StoreResult<()> {
        let installations = connection
            .prepare(
                "SELECT contract_hash, contract_artifact_id, contract_id, contract_version, purpose, baseline_contract_hash FROM rebuild_contract_installations ORDER BY installed_at, contract_hash",
            )?
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut contracts = BTreeMap::new();
        for (hash, artifact_id, contract_id, version, purpose, baseline) in installations {
            let contract_hash = ContentHash::new(hash)?;
            let stored = self
                .stored_contract_with_connection(connection, &contract_hash)?
                .ok_or_else(|| {
                    StoreError::Integrity(format!(
                        "contract installation {contract_hash} disappeared"
                    ))
                })?;
            if stored.artifact.artifact_id.0.as_str() != artifact_id
                || stored.contract.contract_id.0 != contract_id
                || i64::from(stored.contract.version) != version
                || stored.contract.purpose.as_str() != purpose
                || stored
                    .baseline_contract_hash
                    .as_ref()
                    .map(ContentHash::as_str)
                    != baseline.as_deref()
            {
                return Err(StoreError::Integrity(format!(
                    "contract installation {contract_hash} metadata disagrees with payload"
                )));
            }
            if let Some(baseline_hash) = &stored.baseline_contract_hash {
                let baseline_contract = self
                    .stored_contract_with_connection(connection, baseline_hash)?
                    .ok_or_else(|| {
                        StoreError::Integrity(format!(
                            "candidate contract {contract_hash} has missing baseline {baseline_hash}"
                        ))
                    })?;
                if !candidate_is_bounded(&baseline_contract.contract, &stored.contract) {
                    return Err(StoreError::Integrity(format!(
                        "candidate contract {contract_hash} exceeds its installed baseline"
                    )));
                }
            }
            contracts.insert(contract_hash, stored);
        }

        let activations = connection
            .prepare(
                "SELECT activation_id, purpose, previous_contract_hash, contract_hash, policy_transition_id FROM rebuild_contract_activations ORDER BY activation_id",
            )?
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut latest = BTreeMap::<String, (i64, ContentHash)>::new();
        for (activation_id, purpose, previous, hash, transition_id) in activations {
            let contract_hash = ContentHash::new(hash)?;
            let previous = previous.map(ContentHash::new).transpose()?;
            let expected_previous = latest.get(&purpose).map(|(_, hash)| hash.clone());
            if previous != expected_previous {
                return Err(StoreError::Integrity(format!(
                    "contract activation {activation_id} is not the next history entry for {purpose}"
                )));
            }
            let contract = contracts.get(&contract_hash).ok_or_else(|| {
                StoreError::Integrity(format!(
                    "contract activation {activation_id} references unknown contract {contract_hash}"
                ))
            })?;
            if contract.contract.purpose.as_str() != purpose {
                return Err(StoreError::Integrity(format!(
                    "contract activation {activation_id} purpose disagrees with its contract"
                )));
            }
            match (previous.as_ref(), transition_id) {
                (None, None) if contract.baseline_contract_hash.is_none() => {}
                (Some(previous_hash), None) => {
                    let previous_contract = contracts.get(previous_hash).ok_or_else(|| {
                        StoreError::Integrity(format!(
                            "contract activation {activation_id} canonical upgrade has no previous contract"
                        ))
                    })?;
                    if contract.baseline_contract_hash.as_ref() != Some(previous_hash)
                        || contract.contract.contract_id != previous_contract.contract.contract_id
                        || contract.contract.version <= previous_contract.contract.version
                        || !candidate_is_bounded(&previous_contract.contract, &contract.contract)
                    {
                        return Err(StoreError::Integrity(format!(
                            "contract activation {activation_id} is not a valid canonical upgrade"
                        )));
                    }
                }
                (Some(previous_hash), Some(transition_id)) => {
                    let transition =
                        read_policy_transition(connection, &PolicyTransitionId(transition_id))?
                            .ok_or_else(|| {
                                StoreError::Integrity(format!(
                                    "contract activation {activation_id} has no policy transition"
                                ))
                            })?;
                    let promoted = transition.transition.subject
                        == PolicySubject::Contract(contract_hash.clone())
                        && transition.transition.to
                            == PolicyState::Contract(CandidatePolicyState::Active)
                        && contract.baseline_contract_hash.as_ref() == Some(previous_hash);
                    let rolled_back = transition.transition.subject
                        == PolicySubject::Contract(previous_hash.clone())
                        && transition.transition.from
                            == PolicyState::Contract(CandidatePolicyState::Active)
                        && contract_hash
                            == contracts
                                .get(previous_hash)
                                .and_then(|candidate| candidate.baseline_contract_hash.as_ref())
                                .cloned()
                                .ok_or_else(|| {
                                    StoreError::Integrity(format!(
                                        "contract activation {activation_id} rollback has no baseline"
                                    ))
                                })?;
                    if !promoted && !rolled_back {
                        return Err(StoreError::Integrity(format!(
                            "contract activation {activation_id} is not a valid promotion or rollback"
                        )));
                    }
                }
                _ => {
                    return Err(StoreError::Integrity(format!(
                        "contract activation {activation_id} has an invalid history binding"
                    )));
                }
            }
            latest.insert(purpose, (activation_id, contract_hash));
        }

        let heads = connection
            .prepare(
                "SELECT purpose, contract_hash, activation_id FROM rebuild_contract_catalogue_heads ORDER BY purpose",
            )?
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        if heads.len() != latest.len() {
            return Err(StoreError::Integrity(
                "contract catalogue head count disagrees with activation history".to_owned(),
            ));
        }
        for (purpose, contract_hash, activation_id) in heads {
            let contract_hash = ContentHash::new(contract_hash)?;
            if latest.get(&purpose) != Some(&(activation_id, contract_hash)) {
                return Err(StoreError::Integrity(format!(
                    "contract catalogue head for {purpose} is stale"
                )));
            }
        }
        Ok(())
    }

    fn verify_policy_evaluation_history(&self, connection: &Connection) -> StoreResult<()> {
        let evaluation_ids = connection
            .prepare(
                "SELECT evaluation_artifact_id FROM rebuild_policy_evaluations \
                 ORDER BY event_cursor",
            )?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        let mut subject_history: BTreeMap<String, (i64, PolicyState)> = BTreeMap::new();

        for value in evaluation_ids {
            let evaluation_artifact_id = ArtifactId(ContentHash::new(value)?);
            let stored =
                read_policy_evaluation(connection, &evaluation_artifact_id)?.ok_or_else(|| {
                    StoreError::Integrity(format!(
                        "policy evaluation {evaluation_artifact_id} disappeared"
                    ))
                })?;
            stored.subject.validate()?;
            if !stored.subject.accepts_state(stored.from)
                || !stored.subject.accepts_state(stored.to)
            {
                return Err(StoreError::Integrity(format!(
                    "policy evaluation {evaluation_artifact_id} has incompatible subject state"
                )));
            }
            let subject_id = stored.subject.subject_id();
            let (previous_consumed_cursor, expected_from) = subject_history
                .get(&subject_id)
                .copied()
                .unwrap_or((0, stored.subject.initial_state()));
            if stored.from != expected_from
                || stored.consumed_pair_cursor < previous_consumed_cursor
            {
                return Err(StoreError::Integrity(format!(
                    "policy evaluation {evaluation_artifact_id} breaks subject history"
                )));
            }

            let outcome_artifact = read_artifact(connection, &stored.outcome_artifact_id)?;
            let experience_artifact = read_artifact(connection, &stored.experience_artifact_id)?;
            let evaluation_artifact = read_artifact(connection, &stored.evaluation_artifact_id)?;
            for (artifact, expected_kind) in [
                (&outcome_artifact, ArtifactKind::Outcome),
                (&experience_artifact, ArtifactKind::Experience),
                (&evaluation_artifact, ArtifactKind::Evaluation),
            ] {
                if artifact.kind != expected_kind
                    || artifact.lifecycle != ArtifactLifecycle::Canonical
                    || artifact_run_purpose(connection, artifact)? != RunPurpose::Paper
                {
                    return Err(StoreError::Integrity(format!(
                        "policy evaluation {evaluation_artifact_id} has invalid canonical artifact"
                    )));
                }
            }

            let outcome: Outcome =
                serde_json::from_slice(&self.read_blob(&outcome_artifact.blob)?)?;
            outcome.validate_sealed()?;
            let schedule = self.read_outcome_schedule_with_connection(
                connection,
                &outcome,
                &[RunPurpose::Paper],
            )?;
            let experience: Experience =
                serde_json::from_slice(&self.read_blob(&experience_artifact.blob)?)?;
            experience.validate()?;
            let evaluation: Evaluation =
                serde_json::from_slice(&self.read_blob(&evaluation_artifact.blob)?)?;
            evaluation.validate()?;

            let outcome_ref = ArtifactRef {
                artifact_id: outcome_artifact.artifact_id.clone(),
                kind: ArtifactKind::Outcome,
            };
            let experience_ref = ArtifactRef {
                artifact_id: experience_artifact.artifact_id.clone(),
                kind: ArtifactKind::Experience,
            };
            if experience.subject != stored.subject
                || experience.policy_state != stored.from
                || experience.outcome != outcome_ref
                || experience.decision != schedule.decision
                || experience.decision_context != schedule.decision_context
                || experience.execution_context != schedule.execution_context
                || evaluation.outcome != outcome_ref
                || evaluation.experience != experience_ref
            {
                return Err(StoreError::Integrity(format!(
                    "policy evaluation {evaluation_artifact_id} lineage is invalid"
                )));
            }

            match (&stored.subject, &stored.candidate_policy_artifact_id) {
                (PolicySubject::Memory(_), None) => {}
                (PolicySubject::Memory(_), Some(_)) => {
                    return Err(StoreError::Integrity(format!(
                        "policy evaluation {evaluation_artifact_id} binds a memory candidate"
                    )));
                }
                (PolicySubject::Contract(_) | PolicySubject::Topology(_), None) => {
                    return Err(StoreError::Integrity(format!(
                        "policy evaluation {evaluation_artifact_id} has no candidate policy"
                    )));
                }
                (_, Some(candidate_policy_artifact_id)) => {
                    let candidate = read_artifact(connection, candidate_policy_artifact_id)?;
                    if candidate.kind != ArtifactKind::CandidatePolicy
                        || candidate.lifecycle != ArtifactLifecycle::Canonical
                        || artifact_run_purpose(connection, &candidate)? != RunPurpose::Paper
                    {
                        return Err(StoreError::Integrity(format!(
                            "policy evaluation {evaluation_artifact_id} has invalid candidate policy"
                        )));
                    }
                }
            }

            match &stored.transition_id {
                Some(transition_id) => {
                    let transition = read_policy_transition(connection, transition_id)?
                        .ok_or_else(|| {
                            StoreError::Integrity(format!(
                                "policy evaluation {evaluation_artifact_id} references missing transition {transition_id}"
                            ))
                        })?;
                    if transition.transition.subject != stored.subject
                        || transition.transition.from != stored.from
                        || transition.transition.to != stored.to
                        || transition.transition.evaluation.artifact_id
                            != stored.evaluation_artifact_id
                        || transition.run_id != stored.run_id
                    {
                        return Err(StoreError::Integrity(format!(
                            "policy evaluation {evaluation_artifact_id} disagrees with transition {transition_id}"
                        )));
                    }
                }
                None if stored.from != stored.to => {
                    return Err(StoreError::Integrity(format!(
                        "policy evaluation {evaluation_artifact_id} changed state without transition"
                    )));
                }
                None => {}
            }

            let event = connection
                .query_row(
                    "SELECT run_id, event_type, artifact_id, created_at \
                     FROM rebuild_events WHERE event_id = ?1",
                    params![stored.event_cursor],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, String>(3)?,
                        ))
                    },
                )
                .optional()?
                .ok_or_else(|| {
                    StoreError::Integrity(format!(
                        "policy evaluation {evaluation_artifact_id} has no durable event"
                    ))
                })?;
            if event.0 != stored.run_id.0
                || event.1 != "policy.evaluated"
                || event.2.as_deref() != Some(stored.evaluation_artifact_id.0.as_str())
                || parse_time(&event.3)? != stored.completed_at
            {
                return Err(StoreError::Integrity(format!(
                    "policy evaluation {evaluation_artifact_id} event is invalid"
                )));
            }

            if stored.consumed_pair_cursor < 0
                || (stored.consumed_pair_cursor != 0
                    && stored.consumed_pair_cursor >= stored.event_cursor)
            {
                return Err(StoreError::Integrity(format!(
                    "policy evaluation {evaluation_artifact_id} consumed invalid shadow cursor"
                )));
            }
            if stored.consumed_pair_cursor > previous_consumed_cursor {
                let boundary_exists = connection
                    .query_row(
                        "SELECT 1 FROM rebuild_shadow_pairs \
                         WHERE subject_id = ?1 AND pair_event_cursor = ?2",
                        params![subject_id, stored.consumed_pair_cursor],
                        |_| Ok(()),
                    )
                    .optional()?
                    .is_some();
                if !boundary_exists {
                    return Err(StoreError::Integrity(format!(
                        "policy evaluation {evaluation_artifact_id} consumed non-pair cursor"
                    )));
                }
            }
            subject_history.insert(subject_id, (stored.consumed_pair_cursor, stored.to));
        }

        let head_subjects = connection
            .prepare(
                "SELECT subject_json FROM rebuild_policy_consumption_heads ORDER BY subject_id",
            )?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        for subject_json in head_subjects {
            let subject: PolicySubject = serde_json::from_str(&subject_json)?;
            subject.validate()?;
            let head = read_policy_consumption_head(connection, &subject)?.ok_or_else(|| {
                StoreError::Integrity(format!(
                    "policy consumption head {} disappeared",
                    subject.subject_id()
                ))
            })?;
            let latest_id = connection.query_row(
                "SELECT evaluation_artifact_id FROM rebuild_policy_evaluations \
                 WHERE subject_id = ?1 ORDER BY event_cursor DESC LIMIT 1",
                params![subject.subject_id()],
                |row| row.get::<_, String>(0),
            )?;
            let latest =
                read_policy_evaluation(connection, &ArtifactId(ContentHash::new(latest_id)?))?
                    .ok_or_else(|| {
                        StoreError::Integrity(format!(
                            "policy consumption head {} has no evaluation",
                            subject.subject_id()
                        ))
                    })?;
            if head.subject != latest.subject
                || head.consumed_pair_cursor != latest.consumed_pair_cursor
                || head.evaluation_artifact_id != latest.evaluation_artifact_id
                || head.evaluation_cursor != latest.event_cursor
                || head.updated_at != latest.completed_at
            {
                return Err(StoreError::Integrity(format!(
                    "policy consumption head {} does not match latest evaluation",
                    subject.subject_id()
                )));
            }
        }

        let orphan_evaluation = connection
            .query_row(
                r#"SELECT e.evaluation_artifact_id
                   FROM rebuild_policy_evaluations AS e
                   LEFT JOIN rebuild_policy_consumption_heads AS h
                     ON h.subject_id = e.subject_id
                   WHERE h.subject_id IS NULL LIMIT 1"#,
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if let Some(evaluation_id) = orphan_evaluation {
            return Err(StoreError::Integrity(format!(
                "policy evaluation {evaluation_id} has no consumption head"
            )));
        }

        Ok(())
    }
}
