use super::*;

impl Daemon {
    pub(super) async fn execute_outcome_narrative_repair(
        &self,
        task: &ClaimedAttempt,
        schedule: &OutcomeSchedule,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        let outcome_artifact = self
            .store
            .outcome_for(&task.run_id, &schedule.outcome_id)?
            .ok_or_else(|| {
                DaemonError::InvalidInput("narrative repair requires sealed outcome".to_owned())
            })?;
        let outcome: Outcome =
            serde_json::from_slice(&self.store.read_blob(&outcome_artifact.blob)?)?;
        outcome.validate_sealed()?;
        let previous = self
            .store
            .retrospective_for(&task.run_id, &schedule.outcome_id, OutcomeHorizon::T5)?
            .ok_or_else(|| {
                DaemonError::InvalidInput(
                    "narrative repair requires prior retrospective".to_owned(),
                )
            })?;
        let old: Retrospective = serde_json::from_slice(&self.store.read_blob(&previous.blob)?)?;
        old.validate()?;
        let subject = PolicySubject::Memory(MemoryId("paper:default".to_owned()));
        let canary_session = self.store.canary_session_for_run(&task.run_id)?;
        if canary_session.is_none()
            && self
                .store
                .policy_evaluation_for_outcome(
                    &subject,
                    &ArtifactRef {
                        artifact_id: outcome_artifact.artifact_id.clone(),
                        kind: ArtifactKind::Outcome,
                    },
                )?
                .is_some()
        {
            return Ok(TaskCompletion::NoOutput);
        }
        let Some(lease) = self.store.acquire_daemon_lease(
            &format!(
                "{OUTCOME_WORKER_LEASE_NAME}:{}:{}",
                task.run_id, schedule.outcome_id.0
            ),
            self.paper.scheduler.owner_id(),
            now,
            now + Duration::minutes(5),
        )?
        else {
            return Ok(TaskCompletion::DeferredUntil(now + Duration::seconds(30)));
        };
        let _lease_guard = OutcomeLeaseGuard {
            store: self.store.clone(),
            lease: lease.clone(),
        };
        let outcome_ref = ArtifactRef {
            artifact_id: outcome_artifact.artifact_id.clone(),
            kind: ArtifactKind::Outcome,
        };
        let previous_ref = ArtifactRef {
            artifact_id: previous.artifact_id.clone(),
            kind: ArtifactKind::Retrospective,
        };
        let context: DecisionContext = self.read_artifact_payload(&schedule.decision_context)?;
        let mut candidates = vec![
            outcome_ref.clone(),
            previous_ref.clone(),
            outcome.schedule.clone(),
            schedule.decision.clone(),
            schedule.decision_context.clone(),
            schedule.execution_context.clone(),
        ];
        candidates.extend(context.claims);
        candidates.extend(context.critiques);
        for horizon in [OutcomeHorizon::T1, OutcomeHorizon::T3] {
            if let Some(prior) =
                self.store
                    .retrospective_for(&task.run_id, &schedule.outcome_id, horizon)?
            {
                candidates.push(ArtifactRef {
                    artifact_id: prior.artifact_id,
                    kind: ArtifactKind::Retrospective,
                });
            }
        }
        candidates.sort();
        candidates.dedup();
        let mut node = task.node.clone();
        node.objective = format!(
            "[outcome_horizon=t5] Repair only the narrative for outcome {}. Rust numeric facts, metric basis, original research and cutoff are immutable. Do not fetch new prices. Cite the sealed Outcome. The model supplies narrative only; Rust separately rechecks learning eligibility.",
            schedule.outcome_id.0
        );
        let draft = if old.status == akzio_domain::RetrospectiveStatus::Complete {
            let source = old
                .source_refs
                .iter()
                .find(|r| r.kind == ArtifactKind::RetrospectiveDraft)
                .ok_or_else(|| {
                    DaemonError::InvalidInput(
                        "complete retrospective has no governed draft".to_owned(),
                    )
                })?;
            self.read_artifact_payload::<RetrospectiveDraft>(source)?
        } else {
            let artifact = self
                .agents
                .run(
                    &task.permit,
                    &node,
                    candidates,
                    self.model_for(task.node.recipe_id.as_str()),
                    now,
                )
                .await?;
            let draft: RetrospectiveDraft =
                serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
            draft.validate()?;
            if draft.outcome_id != outcome.outcome_id || draft.horizon != OutcomeHorizon::T5 {
                return Err(DaemonError::InvalidInput(
                    "repaired retrospective identity mismatch".to_owned(),
                ));
            }
            self.store.write_task_artifact_fenced(
                Some(&lease),
                &task.permit,
                &artifact,
                LifecycleEventType::RetrospectiveDraftCreated,
                Utc::now(),
            )?;
            draft
        };
        if let Some(session) = canary_session.as_ref() {
            return Ok(
                if self.complete_canary_session(
                    &lease,
                    task,
                    session,
                    &outcome_artifact,
                    None,
                    Some(&draft),
                )? {
                    TaskCompletion::Committed
                } else {
                    TaskCompletion::DeferredUntil(next_outcome_check_at(now)?)
                },
            );
        }
        let producer_usage = self
            .store
            .model_usage_for_producing_run(&schedule.decision)?;
        let snapshot = self.store.workflow_snapshot(&task.run_id)?;
        let contract_hash = snapshot
            .tasks
            .iter()
            .find(|t| t.node.recipe_id.as_str() == akzio_domain::RESEARCH_SYNTHESIZER_RECIPE_ID)
            .and_then(|t| t.node.contract_hash.clone())
            .ok_or_else(|| {
                DaemonError::InvalidInput("decision producer contract missing".to_owned())
            })?;
        EvaluationRuntime::new(self.store.clone(), EvaluationPolicy::default())?
            .evaluate_sealed_with_retrospective(
                Some(&lease),
                akzio_learning::SealedEvaluationInput {
                    complete_task: true,
                    permit: task.permit.clone(),
                    subject: PolicySubject::Memory(MemoryId("paper:default".to_owned())),
                    hypothesis_id: format!("paper-outcome:{}", schedule.outcome_id.0),
                    outcome: outcome_ref,
                    contract_hash,
                    topology_id: TopologyId(snapshot.run.topology_id),
                    candidate_policy: None,
                    token_cost: producer_usage.billable_tokens_if_complete(),
                    latency_millis: producer_usage.latency_millis_if_complete(),
                },
                &draft,
                None,
            )?;
        Ok(TaskCompletion::Committed)
    }
}
