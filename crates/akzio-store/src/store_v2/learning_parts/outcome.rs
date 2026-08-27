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
}
