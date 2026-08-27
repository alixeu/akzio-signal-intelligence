impl Daemon {
    pub(super) async fn execute_shadow_evaluate(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        if self.store.outcome_for_run(&task.run_id)?.is_some() {
            return Ok(TaskCompletion::Committed);
        }
        let Some(session) = self.store.canary_session_for_run(&task.run_id)? else {
            return Ok(TaskCompletion::NoOutput);
        };
        let Some(parent_schedule_artifact) = self
            .store
            .outcome_schedule_for_run(&session.reservation.parent_run_id)?
        else {
            return Ok(TaskCompletion::DeferredUntil(next_outcome_check_at(now)?));
        };
        let parent_schedule_ref = ArtifactRef {
            artifact_id: parent_schedule_artifact.artifact_id.clone(),
            kind: ArtifactKind::OutcomeSchedule,
        };
        let parent_schedule: OutcomeSchedule = self.read_artifact_payload(&parent_schedule_ref)?;
        let candidate_decision_ref = self.terminal_input(task, ArtifactKind::Decision)?;
        let candidate_decision: Decision = self.read_artifact_payload(&candidate_decision_ref)?;
        candidate_decision.validate()?;
        let candidate_context_ref = candidate_decision.decision_context.clone();
        let candidate_context: DecisionContext =
            self.read_artifact_payload(&candidate_context_ref)?;
        candidate_context.validate()?;
        let Some(outcome_lease) = self.store.acquire_daemon_lease(
            OUTCOME_WORKER_LEASE_NAME,
            self.paper.scheduler.owner_id(),
            now,
            now + Duration::minutes(5),
        )?
        else {
            return Ok(TaskCompletion::Retry(RetryCause::Transport));
        };
        let Some(mut collected) = self
            .collect_outcome_materialization(
                &outcome_lease,
                task,
                &parent_schedule_ref,
                &parent_schedule,
                now,
            )
            .await?
        else {
            return Ok(TaskCompletion::DeferredUntil(next_outcome_check_at(now)?));
        };
        self.store
            .validate_daemon_lease(&outcome_lease, Utc::now())?;
        for artifact in &collected.evidence_artifacts {
            self.store.write_task_artifact_fenced(
                Some(&outcome_lease),
                &task.permit,
                artifact,
                LifecycleEventType::OutcomeEvidence,
                now,
            )?;
        }

        let mut schedule_source_refs = vec![
            candidate_decision_ref.clone(),
            candidate_context_ref.clone(),
            parent_schedule.execution_context.clone(),
        ];
        match &parent_schedule.execution {
            OutcomeExecutionLineage::NoOrder { execution_verdict } => {
                schedule_source_refs.push(execution_verdict.clone());
            }
            OutcomeExecutionLineage::ReconciledPaper {
                execution_verdict,
                commitment,
                reconciliation,
            } => {
                schedule_source_refs.extend([
                    execution_verdict.clone(),
                    commitment.clone(),
                    reconciliation.clone(),
                ]);
            }
        }
        schedule_source_refs.sort();
        schedule_source_refs.dedup();
        let candidate_schedule = OutcomeSchedule {
            schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
            outcome_id: OutcomeId(
                ContentHash::of_bytes(
                    format!(
                        "shadow-outcome:{}:{}",
                        task.run_id.0, parent_schedule.outcome_id.0
                    )
                    .as_bytes(),
                )
                .as_str()
                .to_owned(),
            ),
            decision: candidate_decision_ref.clone(),
            decision_context: candidate_context_ref.clone(),
            execution_context: parent_schedule.execution_context.clone(),
            execution: parent_schedule.execution.clone(),
            baseline_trading_day: parent_schedule.baseline_trading_day,
            created_at: now,
        };
        candidate_schedule.validate()?;
        let schedule_artifact = Artifact::new(
            ArtifactKind::OutcomeSchedule,
            self.store.put_json(&candidate_schedule)?,
            "learning.shadow_outcome_schedule",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio-learning".to_owned(),
                observed_at: Some(now),
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: task.permit.contract_hash.clone(),
            },
            Some(task.permit.artifact_origin()),
            schedule_source_refs,
            now,
        )?;
        self.store.write_task_artifact_fenced(
            Some(&outcome_lease),
            &task.permit,
            &schedule_artifact,
            LifecycleEventType::ShadowOutcomeScheduleCreated,
            now,
        )?;

        collected.materialization.schedule = candidate_schedule;
        collected.materialization.schedule_artifact = ArtifactRef {
            artifact_id: schedule_artifact.artifact_id.clone(),
            kind: ArtifactKind::OutcomeSchedule,
        };
        collected.materialization.target = candidate_decision.targets.clone();
        collected.materialization.forecasts = candidate_decision.forecasts.clone();
        let expected_risk_count = (candidate_context.hard_blockers.len()
            + candidate_context.material_conflicts.len()) as u64;
        for observation in &mut collected.materialization.observations {
            observation.expected_risk_count = expected_risk_count;
        }
        let candidate_outcome_payload =
            akzio_learning::materialize_outcome(&collected.materialization)?;
        let candidate_outcome = Artifact::new(
            ArtifactKind::Outcome,
            self.store.put_json(&candidate_outcome_payload)?,
            "learning.shadow_outcome",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio-learning".to_owned(),
                observed_at: Some(now),
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: task.permit.contract_hash.clone(),
            },
            Some(task.permit.artifact_origin()),
            std::iter::once(ArtifactRef {
                artifact_id: schedule_artifact.artifact_id.clone(),
                kind: ArtifactKind::OutcomeSchedule,
            })
            .chain(collected.materialization.market_evidence.iter().cloned())
            .collect(),
            now,
        )?;
        self.store
            .commit_outcomes(&task.permit, std::slice::from_ref(&candidate_outcome), now)?;
        Ok(TaskCompletion::Committed)
    }
}
