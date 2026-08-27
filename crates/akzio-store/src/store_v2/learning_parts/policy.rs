impl V2Store {
    /// Commit canonical learning while fencing an optional daemon worker in
    /// the same SQLite transaction as the policy/evaluation writes.
    pub fn record_policy_evaluation_fenced(
        &self,
        lease: Option<&DaemonLease>,
        commit: &PolicyEvaluationCommit,
    ) -> StoreResult<PolicyEvaluationResult> {
        commit.subject.validate()?;
        if !commit.subject.accepts_state(commit.from) || !commit.subject.accepts_state(commit.to) {
            return Err(StoreError::InvalidLearningCommit(
                "policy_evaluation.subject_state",
            ));
        }
        let subject_id = commit.subject.subject_id();
        let subject_json = serde_json::to_string(&commit.subject)?;

        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(lease) = lease {
            assert_daemon_lease(&transaction, lease, Utc::now())?;
        }
        self.validate_policy_evaluation_commit_with_connection(&transaction, commit)?;

        if let Some(existing) =
            read_policy_evaluation(&transaction, &commit.evaluation.artifact_id)?
        {
            if !same_policy_evaluation(&existing, commit) {
                return Err(StoreError::PolicyEvaluationConflict(
                    commit.evaluation.artifact_id.to_string(),
                ));
            }
            if let Some(candidate_policy) = &commit.candidate_policy {
                let stored = read_artifact(&transaction, &candidate_policy.artifact_id)?;
                if stored != *candidate_policy {
                    return Err(StoreError::PolicyEvaluationConflict(
                        commit.evaluation.artifact_id.to_string(),
                    ));
                }
            }
            let consumption = read_policy_consumption_head(&transaction, &commit.subject)?
                .ok_or_else(|| {
                    StoreError::Integrity(format!(
                        "policy evaluation {} has no consumption head",
                        commit.evaluation.artifact_id
                    ))
                })?;
            if consumption.consumed_pair_cursor < existing.consumed_pair_cursor {
                return Err(StoreError::Integrity(format!(
                    "policy evaluation {} consumption cursor regressed",
                    commit.evaluation.artifact_id
                )));
            }
            let policy_head = read_policy_head(&transaction, &commit.subject)?;
            transaction.commit()?;
            return Ok(PolicyEvaluationResult {
                policy_head,
                consumed_pair_cursor: existing.consumed_pair_cursor,
                evaluation_cursor: existing.event_cursor,
                newly_recorded: false,
            });
        }

        assert_permit(&transaction, &commit.permit)?;
        assert_paper_run(&transaction, &commit.permit.run_id)?;
        let previous = read_policy_head(&transaction, &commit.subject)?;
        match &previous {
            Some(head) if head.state != commit.from => {
                return Err(StoreError::PolicyHeadMismatch(subject_id));
            }
            None if commit.subject.initial_state() != commit.from => {
                return Err(StoreError::PolicyHeadMismatch(subject_id));
            }
            _ => {}
        }
        match &commit.transition {
            Some(transition) => {
                if commit.from == commit.to || !is_allowed_policy_transition(commit.from, commit.to)
                {
                    return Err(StoreError::InvalidLearningCommit("policy_transition.path"));
                }
                if read_policy_transition(&transaction, &transition.transition_id)?.is_some() {
                    return Err(StoreError::PolicyTransitionConflict(
                        transition.transition_id.to_string(),
                    ));
                }
            }
            None if commit.from != commit.to => {
                return Err(StoreError::InvalidLearningCommit(
                    "policy_evaluation.noop_state",
                ));
            }
            None => {}
        }
        validate_policy_shadow_pair_snapshot(&transaction, &commit.subject, commit.pair_snapshot)?;

        let (_, on_failure) = task_retry_policy(&transaction, &commit.permit.task_id)?;
        for artifact in [
            &commit.outcome,
            &commit.final_retrospective,
            &commit.experience,
            &commit.evaluation,
        ]
        .into_iter()
        .chain(commit.candidate_policy.iter())
        {
            let existing = match read_artifact(&transaction, &artifact.artifact_id) {
                Ok(existing) => Some(existing),
                Err(StoreError::MissingArtifact(_)) => None,
                Err(error) => return Err(error),
            };
            if let Some(existing) = &existing {
                if *existing != *artifact {
                    return Err(StoreError::Integrity(format!(
                        "conflicting learning artifact {}",
                        artifact.artifact_id
                    )));
                }
            } else {
                assert_origin_matches(artifact.origin.as_ref(), &commit.permit)?;
                insert_artifact(&transaction, artifact)?;
            }
            let event_id = append_event(
                &transaction,
                &commit.permit.run_id,
                Some(&commit.permit.task_id),
                Some(&commit.permit.attempt_id),
                LifecycleEventType::ArtifactCommitted,
                Some(&artifact.artifact_id),
                commit.completed_at,
            )?;
            record_attempt_output(
                &transaction,
                &commit.permit,
                &artifact.artifact_id,
                event_id,
            )?;
            if artifact.kind == ArtifactKind::Retrospective {
                append_event(
                    &transaction,
                    &commit.permit.run_id,
                    Some(&commit.permit.task_id),
                    Some(&commit.permit.attempt_id),
                    LifecycleEventType::RetrospectiveCreated,
                    Some(&artifact.artifact_id),
                    commit.completed_at,
                )?;
            }
        }

        let consumed_pair_cursor = commit.pair_snapshot.through_cursor;
        let evaluation_cursor = append_event(
            &transaction,
            &commit.permit.run_id,
            Some(&commit.permit.task_id),
            Some(&commit.permit.attempt_id),
            LifecycleEventType::PolicyEvaluated,
            Some(&commit.evaluation.artifact_id),
            commit.completed_at,
        )?;

        let policy_head = if let Some(transition) = &commit.transition {
            let revision = previous
                .as_ref()
                .map_or(1, |head| head.revision.saturating_add(1));
            let transition_cursor = append_event(
                &transaction,
                &commit.permit.run_id,
                Some(&commit.permit.task_id),
                Some(&commit.permit.attempt_id),
                LifecycleEventType::PolicyTransitioned,
                Some(&commit.evaluation.artifact_id),
                commit.completed_at,
            )?;
            transaction.execute(
                r#"INSERT INTO rebuild_policy_transitions
                   (transition_id, subject_id, subject_json, from_state_json, to_state_json,
                    evaluation_artifact_id, run_id, revision, created_at, event_cursor)
                   VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"#,
                params![
                    transition.transition_id.0,
                    subject_id,
                    subject_json,
                    serde_json::to_string(&commit.from)?,
                    serde_json::to_string(&commit.to)?,
                    commit.evaluation.artifact_id.0.as_str(),
                    commit.permit.run_id.0,
                    revision,
                    transition.created_at.to_rfc3339(),
                    transition_cursor,
                ],
            )?;
            match previous {
                Some(_) => {
                    transaction.execute(
                        "UPDATE rebuild_policy_heads SET subject_json = ?1, state_json = ?2, revision = ?3, transition_id = ?4, transition_event_cursor = ?5, updated_at = ?6 WHERE subject_id = ?7",
                        params![
                            serde_json::to_string(&commit.subject)?,
                            serde_json::to_string(&commit.to)?,
                            revision,
                            transition.transition_id.0,
                            transition_cursor,
                            transition.created_at.to_rfc3339(),
                            commit.subject.subject_id(),
                        ],
                    )?;
                }
                None => {
                    transaction.execute(
                        r#"INSERT INTO rebuild_policy_heads
                           (subject_id, subject_json, state_json, revision, transition_id,
                            transition_event_cursor, updated_at)
                           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"#,
                        params![
                            commit.subject.subject_id(),
                            serde_json::to_string(&commit.subject)?,
                            serde_json::to_string(&commit.to)?,
                            revision,
                            transition.transition_id.0,
                            transition_cursor,
                            transition.created_at.to_rfc3339(),
                        ],
                    )?;
                }
            }
            Some(PolicyHead {
                subject: commit.subject.clone(),
                state: commit.to,
                revision,
                transition_id: transition.transition_id.clone(),
                transition_cursor,
                updated_at: transition.created_at,
            })
        } else {
            previous
        };

        if let Some(transition) = &commit.transition {
            self.apply_contract_catalogue_transition(&transaction, commit, transition)?;
        }

        transaction.execute(
            r#"INSERT INTO rebuild_policy_evaluations
                (evaluation_artifact_id, subject_id, subject_json, outcome_artifact_id,
                 experience_artifact_id, candidate_policy_artifact_id, from_state_json,
                 to_state_json, transition_id, run_id, consumed_pair_cursor, event_cursor,
                 completed_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)"#,
            params![
                commit.evaluation.artifact_id.0.as_str(),
                commit.subject.subject_id(),
                serde_json::to_string(&commit.subject)?,
                commit.outcome.artifact_id.0.as_str(),
                commit.experience.artifact_id.0.as_str(),
                commit
                    .candidate_policy
                    .as_ref()
                    .map(|artifact| artifact.artifact_id.0.as_str()),
                serde_json::to_string(&commit.from)?,
                serde_json::to_string(&commit.to)?,
                commit
                    .transition
                    .as_ref()
                    .map(|transition| transition.transition_id.0.as_str()),
                commit.permit.run_id.0,
                consumed_pair_cursor,
                evaluation_cursor,
                commit.completed_at.to_rfc3339(),
            ],
        )?;
        transaction.execute(
            r#"INSERT INTO rebuild_policy_consumption_heads
               (subject_id, subject_json, consumed_pair_cursor, evaluation_artifact_id,
                evaluation_event_cursor, updated_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6)
               ON CONFLICT(subject_id) DO UPDATE SET
                   subject_json = excluded.subject_json,
                   consumed_pair_cursor = excluded.consumed_pair_cursor,
                   evaluation_artifact_id = excluded.evaluation_artifact_id,
                   evaluation_event_cursor = excluded.evaluation_event_cursor,
                   updated_at = excluded.updated_at"#,
            params![
                commit.subject.subject_id(),
                serde_json::to_string(&commit.subject)?,
                consumed_pair_cursor,
                commit.evaluation.artifact_id.0.as_str(),
                evaluation_cursor,
                commit.completed_at.to_rfc3339(),
            ],
        )?;
        finish_permitted_task(
            &transaction,
            &commit.permit,
            TaskStatus::Succeeded,
            on_failure,
            Some(&commit.evaluation.artifact_id),
            commit.completed_at,
        )?;
        transaction.commit()?;
        Ok(PolicyEvaluationResult {
            policy_head,
            consumed_pair_cursor,
            evaluation_cursor,
            newly_recorded: true,
        })
    }
}
