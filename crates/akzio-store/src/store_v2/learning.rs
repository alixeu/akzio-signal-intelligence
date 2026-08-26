use super::*;

impl V2Store {
    /// Commits the terminal `OutcomeSchedule` and installs the scheduler-owned
    /// learning task in the same SQLite transaction. The learning task is not
    /// part of the frozen research graph; it is a post-terminal durable worker
    /// attached to the Paper run and cannot be created by a planner or agent.
    pub fn commit_outcome_schedule_with_worker(
        &self,
        permit: &TaskWritePermit,
        schedule: &Artifact,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        if schedule.kind != ArtifactKind::OutcomeSchedule {
            return Err(StoreError::InvalidLearningCommit(
                "outcome_schedule.worker_kind",
            ));
        }
        schedule.validate()?;
        self.read_blob(&schedule.blob)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let schedule_ref = ArtifactRef {
            artifact_id: schedule.artifact_id.clone(),
            kind: ArtifactKind::OutcomeSchedule,
        };
        self.validate_attempt_commit(
            permit,
            std::slice::from_ref(schedule),
            TaskStatus::Succeeded,
        )?;
        let existing_worker = transaction
            .prepare(
                "SELECT task_id, input_artifacts_json FROM rebuild_tasks WHERE run_id = ?1 AND recipe_id = ?2",
            )?
            .query_map(
                params![permit.run_id.0, POST_TERMINAL_WORKER_RECIPE_ID],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )?
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .find_map(|(task_id, input_json)| {
                let inputs = serde_json::from_str::<Vec<ArtifactRef>>(&input_json).ok()?;
                inputs
                    .iter()
                    .any(|reference| reference == &schedule_ref)
                    .then_some(task_id)
            });
        if existing_worker.is_some() {
            assert_idempotent_outcome_schedule_commit(&transaction, permit, schedule)?;
            transaction.commit()?;
            return Ok(());
        }
        let worker_contract_hash = contract_catalogue_head(
            &transaction,
            &ContractPurpose::new(POST_TERMINAL_WORKER_RECIPE_ID)?,
        )?
        .map(|(hash, _)| hash)
        .or_else(|| schedule.provenance.producer_contract_hash.clone());
        // AgentRuntime rejects a task whose durable policy diverges from its contract.
        let worker_policy = worker_contract_hash
            .as_ref()
            .map(|contract_hash| {
                self.stored_contract_with_connection(&transaction, contract_hash)?
                    .ok_or_else(|| StoreError::MissingContractInstallation(contract_hash.clone()))
            })
            .transpose()?;
        let (worker_budget, worker_retry, worker_on_failure) = worker_policy
            .map(|stored| {
                (
                    stored.contract.budget,
                    stored.contract.retry,
                    stored.contract.on_failure,
                )
            })
            .unwrap_or_else(|| {
                (
                    TaskBudget {
                        max_input_tokens: 1_024,
                        max_output_tokens: 1_024,
                        max_wall_time_secs: 120,
                        max_tool_calls: 0,
                    },
                    RetryPolicy {
                        max_attempts: u8::MAX,
                        initial_backoff_ms: 3_600_000,
                        retry_transport: true,
                        retry_rate_limited: true,
                        retry_invalid_output: false,
                    },
                    FailureDisposition::FailRun,
                )
            });
        let mut worker_inputs = vec![schedule_ref];
        worker_inputs.extend(schedule.source_refs.clone());
        let deliberation_note_ids = transaction
            .prepare(
                r#"
                SELECT DISTINCT e.artifact_id
                FROM rebuild_events AS e
                JOIN rebuild_tasks AS t
                  ON t.run_id = e.run_id
                 AND t.task_id = e.task_id
                JOIN rebuild_attempts AS a
                  ON a.run_id = e.run_id
                 AND a.task_id = e.task_id
                 AND a.attempt_id = e.attempt_id
                JOIN rebuild_artifacts AS artifact
                  ON artifact.artifact_id = e.artifact_id
                WHERE e.run_id = ?1
                  AND e.event_type = ?2
                  AND e.artifact_id IS NOT NULL
                  AND t.status = 'succeeded'
                  AND a.status = 'succeeded'
                  AND artifact.kind = ?3
                ORDER BY e.artifact_id ASC
                "#,
            )?
            .query_map(
                params![
                    permit.run_id.0,
                    LifecycleEventType::DeliberationNoteCreated.as_str(),
                    enum_name(ArtifactKind::DeliberationNote),
                ],
                |row| row.get::<_, String>(0),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        worker_inputs.extend(
            deliberation_note_ids
                .into_iter()
                .map(|artifact_id| {
                    Ok(ArtifactRef {
                        artifact_id: ArtifactId(ContentHash::new(artifact_id)?),
                        kind: ArtifactKind::DeliberationNote,
                    })
                })
                .collect::<StoreResult<Vec<_>>>()?,
        );
        worker_inputs.sort();
        worker_inputs.dedup();
        let worker = WorkflowNode {
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new("learning.outcome_worker")?,
            contract_hash: worker_contract_hash,
            objective: "Seal governed T+1/T+3/T+5 Paper outcome and record evaluation.".to_owned(),
            dependencies: Vec::new(),
            input_artifacts: worker_inputs,
            priority: 100,
            budget: worker_budget,
            retry: worker_retry,
            on_failure: worker_on_failure,
            parent_task_id: None,
        };
        commit_attempt_transaction(
            &transaction,
            permit,
            std::slice::from_ref(schedule),
            TaskStatus::Succeeded,
            now,
        )?;
        insert_task_node(&transaction, &permit.run_id, &worker, now)?;
        append_event(
            &transaction,
            &permit.run_id,
            Some(&worker.task_id),
            None,
            LifecycleEventType::OutcomeWorkerEnqueued,
            Some(&schedule.artifact_id),
            now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Commits sealed Paper or Shadow outcomes through a purpose-aware path.
    /// Generic task artifact APIs reject Outcome so learning lineage cannot be
    /// created without these checks.
    pub fn commit_outcomes(
        &self,
        permit: &TaskWritePermit,
        outcomes: &[Artifact],
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        if outcomes.is_empty() {
            return Err(StoreError::Domain(DomainError::EmptyField {
                field: "commit_outcomes.outcomes",
            }));
        }
        let payloads = outcomes
            .iter()
            .map(|artifact| {
                artifact.validate()?;
                self.read_blob(&artifact.blob)?;
                if artifact.kind != ArtifactKind::Outcome {
                    return Err(StoreError::InvalidLearningCommit("commit_outcomes.kind"));
                }
                let outcome: Outcome = self.read_artifact_payload(artifact)?;
                outcome.validate_sealed()?;
                Ok(outcome)
            })
            .collect::<StoreResult<Vec<_>>>()?;

        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
        let purpose = run_purpose_from_connection(&transaction, &permit.run_id)?;
        let expected_lifecycle = match purpose {
            RunPurpose::Paper => ArtifactLifecycle::Canonical,
            RunPurpose::Shadow => ArtifactLifecycle::RunScoped,
            _ => return Err(StoreError::NonCanonicalLearningPurpose(purpose)),
        };
        let allowed_schedule_purposes: &[RunPurpose] = match purpose {
            RunPurpose::Paper => &[RunPurpose::Paper],
            RunPurpose::Shadow => &[RunPurpose::Paper, RunPurpose::Shadow],
            _ => unreachable!("non-learning purpose rejected above"),
        };
        for (artifact, outcome) in outcomes.iter().zip(&payloads) {
            if artifact.lifecycle != expected_lifecycle {
                return Err(StoreError::InvalidLearningCommit(
                    "commit_outcomes.lifecycle",
                ));
            }
            let schedule_artifact = read_artifact(&transaction, &outcome.schedule.artifact_id)?;
            assert_artifact_from_allowed_purposes(&transaction, &schedule_artifact, &[purpose])?;
            self.read_outcome_schedule_with_connection(
                &transaction,
                outcome,
                allowed_schedule_purposes,
            )?;
            if !has_exact_source_refs(
                artifact,
                &std::iter::once(outcome.schedule.clone())
                    .chain(outcome.market_evidence.iter().cloned())
                    .collect::<Vec<_>>(),
            ) {
                return Err(StoreError::InvalidLearningCommit(
                    "commit_outcomes.source_refs",
                ));
            }
        }

        let (_, on_failure) = task_retry_policy(&transaction, &permit.task_id)?;
        insert_artifact_batch(&transaction, outcomes)?;
        for artifact in outcomes {
            assert_origin_matches(artifact.origin.as_ref(), permit)?;
            let event_id = append_event(
                &transaction,
                &permit.run_id,
                Some(&permit.task_id),
                Some(&permit.attempt_id),
                LifecycleEventType::ArtifactCommitted,
                Some(&artifact.artifact_id),
                now,
            )?;
            record_attempt_output(&transaction, permit, &artifact.artifact_id, event_id)?;
        }
        finish_permitted_task(
            &transaction,
            permit,
            TaskStatus::Succeeded,
            on_failure,
            outcomes.last().map(|artifact| &artifact.artifact_id),
            now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Atomically seals one Paper outcome and its final retrospective while
    /// leaving no window where a worker can finish with only one of them.
    pub fn commit_outcome_retrospective_fenced(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        outcome_artifact: &Artifact,
        retrospective_artifact: &Artifact,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        outcome_artifact.validate()?;
        retrospective_artifact.validate()?;
        self.read_blob(&outcome_artifact.blob)?;
        self.read_blob(&retrospective_artifact.blob)?;
        if outcome_artifact.kind != ArtifactKind::Outcome
            || outcome_artifact.lifecycle != ArtifactLifecycle::Canonical
            || retrospective_artifact.kind != ArtifactKind::Retrospective
            || retrospective_artifact.lifecycle != ArtifactLifecycle::Canonical
        {
            return Err(StoreError::InvalidLearningCommit(
                "outcome_retrospective.kind_or_lifecycle",
            ));
        }
        let outcome: Outcome = self.read_artifact_payload(outcome_artifact)?;
        outcome.validate_sealed()?;
        let retrospective: Retrospective = self.read_artifact_payload(retrospective_artifact)?;
        retrospective.validate()?;
        if retrospective.horizon != OutcomeHorizon::T5
            || retrospective.outcome.artifact_id != outcome_artifact.artifact_id
            || retrospective.outcome.kind != ArtifactKind::Outcome
        {
            return Err(StoreError::InvalidLearningCommit(
                "outcome_retrospective.links",
            ));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;
        assert_permit(&transaction, permit)?;
        for artifact in [outcome_artifact, retrospective_artifact] {
            match read_artifact(&transaction, &artifact.artifact_id) {
                Ok(existing) if existing != *artifact => {
                    return Err(StoreError::Integrity(format!(
                        "conflicting learning artifact {}",
                        artifact.artifact_id
                    )));
                }
                Ok(_) => {}
                Err(StoreError::MissingArtifact(_)) => {
                    assert_origin_matches(artifact.origin.as_ref(), permit)?;
                }
                Err(error) => return Err(error),
            }
        }
        if run_purpose_from_connection(&transaction, &permit.run_id)? != RunPurpose::Paper {
            return Err(StoreError::NonCanonicalLearningPurpose(
                run_purpose_from_connection(&transaction, &permit.run_id)?,
            ));
        }
        let schedule = self.read_outcome_schedule_with_connection(
            &transaction,
            &outcome,
            &[RunPurpose::Paper],
        )?;
        let expected_sources = std::iter::once(outcome.schedule.clone())
            .chain(outcome.market_evidence.iter().cloned())
            .collect::<Vec<_>>();
        if !has_exact_source_refs(outcome_artifact, &expected_sources) {
            return Err(StoreError::InvalidLearningCommit(
                "outcome_retrospective.outcome_source_refs",
            ));
        }
        let outcome_ref = ArtifactRef {
            artifact_id: outcome_artifact.artifact_id.clone(),
            kind: ArtifactKind::Outcome,
        };
        if !retrospective_artifact.source_refs.contains(&outcome_ref) {
            return Err(StoreError::InvalidLearningCommit(
                "outcome_retrospective.source_refs",
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
                && existing != *retrospective_artifact
            {
                return Err(StoreError::Integrity(
                    "duplicate retrospective identity different payload".to_owned(),
                ));
            }
        }
        let _ = schedule;
        for artifact in [outcome_artifact, retrospective_artifact] {
            let already_exists = read_artifact(&transaction, &artifact.artifact_id).is_ok();
            if already_exists {
                continue;
            }
            insert_artifact(&transaction, artifact)?;
            let event_type = if artifact.kind == ArtifactKind::Retrospective {
                LifecycleEventType::RetrospectiveCreated
            } else {
                LifecycleEventType::ArtifactCommitted
            };
            let event_id = append_event(
                &transaction,
                &permit.run_id,
                Some(&permit.task_id),
                Some(&permit.attempt_id),
                event_type,
                Some(&artifact.artifact_id),
                now,
            )?;
            // `rebuild_attempt_outputs` is the task-output index used by
            // replay and Doctor.  A retrospective has its own typed
            // lifecycle event and is supporting material, not a second task
            // output.  Recording only the Outcome keeps that index's
            // artifact.committed invariant intact.
            if event_type == LifecycleEventType::ArtifactCommitted {
                record_attempt_output(&transaction, permit, &artifact.artifact_id, event_id)?;
            }
        }
        let (_, on_failure) = task_retry_policy(&transaction, &permit.task_id)?;
        finish_permitted_task(
            &transaction,
            permit,
            TaskStatus::Succeeded,
            on_failure,
            Some(&outcome_artifact.artifact_id),
            now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Records an immutable outcome-backed comparison. Completion is keyed by
    /// the compared decisions/context/candidate/horizon, never by wall-clock
    /// time, so a recovered attempt cannot create a second pair.
    pub fn complete_shadow_pair(
        &self,
        permit: &TaskWritePermit,
        completion: &ShadowPairCompletion,
    ) -> StoreResult<ShadowPairWriteResult> {
        completion.validate()?;
        let pair_key = completion.pair_key()?;
        let subject_id = completion.subject.subject_id();
        let subject_json = serde_json::to_string(&completion.subject)?;

        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;

        assert_permit(&transaction, permit)?;
        assert_paper_run(&transaction, &permit.run_id)?;
        self.assert_shadow_pair_sources_with_connection(&transaction, completion)?;

        if let Some(existing) = read_shadow_pair(&transaction, &pair_key)? {
            if same_shadow_pair(&existing.completion, completion) {
                transaction.commit()?;
                return Ok(ShadowPairWriteResult::Existing(existing));
            }
            return Err(StoreError::ShadowPairConflict(pair_key.to_string()));
        }

        let completion_cursor = append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::ShadowPairCompleted,
            Some(&completion.candidate_outcome.artifact_id),
            completion.completed_at,
        )?;
        transaction.execute(
            r#"INSERT INTO rebuild_shadow_pairs
            (pair_key, subject_id, subject_json, parent_decision_artifact_id, execution_context_artifact_id,
             candidate_decision_artifact_id, candidate_contract_hash, candidate_topology_id,
             horizon, parent_outcome_artifact_id, candidate_outcome_artifact_id, completed_at,
             pair_event_cursor)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)"#,
            params![
                pair_key.as_str(),
                subject_id,
                subject_json,
                completion.parent_decision.artifact_id.0.as_str(),
                completion.execution_context.artifact_id.0.as_str(),
                completion.candidate_decision.artifact_id.0.as_str(),
                completion.candidate_contract_hash.as_str(),
                completion.candidate_topology_id,
                enum_name(completion.horizon),
                completion.parent_outcome.artifact_id.0.as_str(),
                completion.candidate_outcome.artifact_id.0.as_str(),
                completion.completed_at.to_rfc3339(),
                completion_cursor,
            ],
        )?;
        transaction.commit()?;
        Ok(ShadowPairWriteResult::Inserted(StoredShadowPair {
            pair_key,
            completion: completion.clone(),
            completion_cursor,
        }))
    }

    /// Commits every canonical outcome-backed evaluation. A no-op still closes
    /// the subject's durable pair-consumption cursor, so one completed shadow
    /// pair cannot be used by more than one canonical evaluation.
    pub fn record_policy_evaluation(
        &self,
        commit: &PolicyEvaluationCommit,
    ) -> StoreResult<PolicyEvaluationResult> {
        self.record_policy_evaluation_fenced(None, commit)
    }

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

    /// Atomically records a governed retrospective diagnostic. The
    /// `(run_id, outcome_id, horizon)` identity is reconstructed from the
    /// immutable artifact history, so retries are idempotent without another
    /// table or index.
    pub fn record_retrospective_diagnostic_fenced(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        artifact: &Artifact,
        now: DateTime<Utc>,
    ) -> StoreResult<bool> {
        if artifact.kind != ArtifactKind::Retrospective {
            return Err(StoreError::InvalidLearningCommit("retrospective.kind"));
        }
        artifact.validate()?;
        self.read_blob(&artifact.blob)?;
        let payload: Retrospective = self.read_artifact_payload(artifact)?;
        payload.validate()?;
        if payload.horizon == OutcomeHorizon::T5
            && payload.status == RetrospectiveStatus::Complete
            && artifact.lifecycle != ArtifactLifecycle::Canonical
        {
            return Err(StoreError::InvalidLearningCommit(
                "retrospective.t5_lifecycle",
            ));
        }
        if payload.horizon != OutcomeHorizon::T5
            && artifact.lifecycle != ArtifactLifecycle::RunScoped
        {
            return Err(StoreError::InvalidLearningCommit(
                "retrospective.intermediate_lifecycle",
            ));
        }
        assert_origin_matches(artifact.origin.as_ref(), permit)?;
        if payload.outcome.kind != ArtifactKind::Outcome {
            return Err(StoreError::InvalidLearningCommit(
                "retrospective.outcome_kind",
            ));
        }

        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;
        assert_permit(&transaction, permit)?;
        let outcome = read_artifact(&transaction, &payload.outcome.artifact_id)?;
        if outcome.kind != ArtifactKind::Outcome {
            return Err(StoreError::InvalidLearningCommit(
                "retrospective.outcome_kind",
            ));
        }
        let outcome_payload: Outcome = self.read_artifact_payload(&outcome)?;
        if payload.horizon == OutcomeHorizon::T5 {
            outcome_payload.validate_sealed()?;
            if artifact.lifecycle != ArtifactLifecycle::Canonical
                || outcome.lifecycle != ArtifactLifecycle::Canonical
            {
                return Err(StoreError::InvalidLearningCommit(
                    "retrospective.t5_lifecycle",
                ));
            }
        } else {
            outcome_payload.validate()?;
            if artifact.lifecycle != ArtifactLifecycle::RunScoped
                || outcome.lifecycle != ArtifactLifecycle::RunScoped
                || outcome_payload.sealed_at.is_some()
            {
                return Err(StoreError::InvalidLearningCommit(
                    "retrospective.intermediate_lifecycle",
                ));
            }
        }
        assert_artifact_from_paper_with_connection(&transaction, &outcome)?;

        for existing in read_kind_artifacts(&transaction, ArtifactKind::Retrospective)? {
            if existing
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(&permit.run_id)
            {
                continue;
            }
            let existing_payload: Retrospective = self.read_artifact_payload(&existing)?;
            if existing_payload.outcome_id == payload.outcome_id
                && existing_payload.horizon == payload.horizon
            {
                if existing == *artifact {
                    transaction.commit()?;
                    return Ok(false);
                }
                return Err(StoreError::Integrity(
                    "duplicate retrospective identity has different payload".to_owned(),
                ));
            }
        }

        insert_artifact(&transaction, artifact)?;
        append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::RetrospectiveCreated,
            Some(&artifact.artifact_id),
            now,
        )?;
        transaction.commit()?;
        Ok(true)
    }

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

    pub fn record_attempt_relation_fenced(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        artifact: &Artifact,
        now: DateTime<Utc>,
    ) -> StoreResult<bool> {
        if artifact.kind != ArtifactKind::AttemptRelation
            || artifact.lifecycle != ArtifactLifecycle::RunScoped
        {
            return Err(StoreError::InvalidLearningCommit(
                "attempt_relation.kind_or_lifecycle",
            ));
        }
        artifact.validate()?;
        self.read_blob(&artifact.blob)?;
        let payload: AttemptRelation = self.read_artifact_payload(artifact)?;
        payload.validate()?;
        if payload.run_id != permit.run_id
            || payload.task_id != permit.task_id
            || payload.child_attempt_id != permit.attempt_id
        {
            return Err(StoreError::PermitOriginMismatch);
        }
        assert_origin_matches(artifact.origin.as_ref(), permit)?;

        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;
        assert_permit(&transaction, permit)?;
        let parent_exists = transaction
            .query_row(
                "SELECT 1 FROM rebuild_attempts WHERE run_id = ?1 AND task_id = ?2 AND attempt_id = ?3",
                params![payload.run_id.0, payload.task_id.0, payload.parent_attempt_id.0],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !parent_exists {
            return Err(StoreError::InvalidLearningCommit(
                "attempt_relation.parent_missing",
            ));
        }
        let child_exists = transaction
            .query_row(
                "SELECT 1 FROM rebuild_attempts WHERE run_id = ?1 AND task_id = ?2 AND attempt_id = ?3",
                params![payload.run_id.0, payload.task_id.0, payload.child_attempt_id.0],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !child_exists {
            return Err(StoreError::InvalidLearningCommit(
                "attempt_relation.child_missing",
            ));
        }

        let mut parent_by_child = BTreeMap::<AttemptId, AttemptId>::new();
        for existing in read_kind_artifacts(&transaction, ArtifactKind::AttemptRelation)? {
            let existing_payload: AttemptRelation = self.read_artifact_payload(&existing)?;
            if existing_payload.run_id == payload.run_id
                && existing_payload.task_id == payload.task_id
            {
                if existing_payload.child_attempt_id == payload.child_attempt_id {
                    if existing_payload.parent_attempt_id == payload.parent_attempt_id
                        && existing_payload.relation == payload.relation
                        && existing == *artifact
                    {
                        transaction.commit()?;
                        return Ok(false);
                    }
                    return Err(StoreError::Integrity(
                        "attempt_relation.child_has_multiple_parents".to_owned(),
                    ));
                }
                parent_by_child.insert(
                    existing_payload.child_attempt_id,
                    existing_payload.parent_attempt_id,
                );
            }
        }
        let mut cursor = payload.parent_attempt_id.clone();
        let mut hops = 0_u16;
        while let Some(parent) = parent_by_child.get(&cursor) {
            if *parent == payload.child_attempt_id {
                return Err(StoreError::Integrity("attempt_relation.cycle".to_owned()));
            }
            cursor = parent.clone();
            hops = hops.saturating_add(1);
            if hops > 1_024 {
                return Err(StoreError::Integrity("attempt_relation.cycle".to_owned()));
            }
        }

        insert_artifact(&transaction, artifact)?;
        append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::AttemptRelationCreated,
            Some(&artifact.artifact_id),
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
