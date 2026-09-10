use super::*;

impl Daemon {
    pub(crate) async fn execute_outcome_worker(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        if self.store.run_purpose(&task.run_id)? != RunPurpose::Paper {
            return Ok(TaskCompletion::NoOutput);
        }
        let schedule_reference = task
            .node
            .input_artifacts
            .iter()
            .find(|reference| reference.kind == ArtifactKind::OutcomeSchedule)
            .cloned()
            .ok_or_else(|| {
                DaemonError::InvalidInput("outcome worker schedule input missing".to_owned())
            })?;
        let schedule: OutcomeSchedule = self.read_artifact_payload(&schedule_reference)?;
        if task.node.objective.starts_with("[narrative_repair]") {
            return self
                .execute_outcome_narrative_repair(task, &schedule, now)
                .await;
        }
        // Canonical T5 already owns completion; polling it never invokes a model.
        let sealed_retrospective =
            self.store
                .retrospective_for(&task.run_id, &schedule.outcome_id, OutcomeHorizon::T5)?;
        let canary_session = self.store.canary_session_for_run(&task.run_id)?;
        if sealed_retrospective.is_some() && canary_session.is_none() {
            return Ok(TaskCompletion::NoOutput);
        }
        let lease_name = format!(
            "{OUTCOME_WORKER_LEASE_NAME}:{}:{}",
            task.run_id, schedule.outcome_id.0
        );
        let Some(outcome_lease) = self.store.acquire_daemon_lease(
            &lease_name,
            self.paper.scheduler.owner_id(),
            now,
            now + Duration::minutes(5),
        )?
        else {
            return Ok(TaskCompletion::DeferredUntil(now + Duration::seconds(30)));
        };
        let _lease_guard = OutcomeLeaseGuard {
            store: self.store.clone(),
            lease: outcome_lease.clone(),
        };
        if let (Some(retrospective_artifact), Some(session)) =
            (sealed_retrospective, canary_session.as_ref())
        {
            let previous: Retrospective =
                serde_json::from_slice(&self.store.read_blob(&retrospective_artifact.blob)?)?;
            previous.validate()?;
            let draft = if previous.status == akzio_domain::RetrospectiveStatus::Complete {
                previous
                    .source_refs
                    .iter()
                    .find(|r| r.kind == ArtifactKind::RetrospectiveDraft)
                    .map(|r| self.read_artifact_payload::<RetrospectiveDraft>(r))
                    .transpose()?
            } else {
                None
            };
            let outcome = self.store.artifact(&previous.outcome.artifact_id)?;
            return Ok(
                if self.complete_canary_session(
                    &outcome_lease,
                    task,
                    session,
                    &outcome,
                    None,
                    draft.as_ref(),
                )? {
                    TaskCompletion::Committed
                } else {
                    TaskCompletion::DeferredUntil(next_outcome_check_at(now)?)
                },
            );
        }
        let Some(mut collected) = self
            .collect_outcome_materialization(
                &outcome_lease,
                task,
                &schedule_reference,
                &schedule,
                now,
            )
            .await?
        else {
            return Ok(TaskCompletion::DeferredUntil(next_outcome_check_at(now)?));
        };
        let mut pending = Vec::new();
        for observation in &collected.materialization.observations {
            if self
                .store
                .retrospective_for(&task.run_id, &schedule.outcome_id, observation.horizon)?
                .is_none()
            {
                pending.push(observation.horizon);
            }
        }
        pending.sort();
        pending.dedup();
        let Some(horizon) = pending.first().copied() else {
            return Ok(TaskCompletion::DeferredUntil(next_outcome_check_at(now)?));
        };
        // One bounded model invocation per logical stage, earliest first. Late
        // catch-up cannot let T1 inspect T3/T5 prices, NAV paths or narratives.
        let stage_day = collected
            .materialization
            .observations
            .iter()
            .find(|o| o.horizon == horizon)
            .expect("pending horizon exists")
            .observed_trading_day;
        collected
            .materialization
            .observations
            .retain(|o| o.horizon <= horizon);
        for observation in &mut collected.materialization.observations {
            observation.completed_trading_sessions = horizon.trading_days();
        }
        collected
            .materialization
            .daily_observations
            .retain(|o| o.observed_trading_day <= stage_day);
        for artifact in &collected.evidence_artifacts {
            self.store.write_task_artifact_fenced(
                Some(&outcome_lease),
                &task.permit,
                artifact,
                LifecycleEventType::OutcomeEvidence,
                Utc::now(),
            )?;
        }
        // Persist provider evidence before the stage gate so a refusal is auditable.
        for artifact in collected
            .evidence_artifacts
            .iter()
            .filter(|a| a.kind == ArtifactKind::NormalizedEvidence)
        {
            let payload: akzio_ingest::NormalizedEvidencePayload =
                serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
            akzio_ingest::validate_outcome_price_window(
                &payload.value,
                schedule.baseline_trading_day,
                stage_day,
            )
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        }
        let mut prior_retrospectives = Vec::new();
        for artifact in self.store.retrospectives(&task.run_id)? {
            let payload: Retrospective =
                serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
            payload.validate()?;
            if payload.outcome_id == schedule.outcome_id && payload.horizon < horizon {
                prior_retrospectives.push(ArtifactRef {
                    artifact_id: artifact.artifact_id,
                    kind: ArtifactKind::Retrospective,
                });
            }
        }
        let stage_facts = akzio_learning::materialize_partial_outcome(&collected.materialization)?;
        let decision_context: DecisionContext =
            self.read_artifact_payload(&schedule.decision_context)?;
        let mut candidates = vec![
            schedule_reference.clone(),
            schedule.decision.clone(),
            schedule.decision_context.clone(),
            schedule.execution_context.clone(),
        ];
        candidates.extend(decision_context.claims.iter().cloned());
        candidates.extend(decision_context.critiques.iter().cloned());
        candidates.extend(prior_retrospectives.iter().cloned());
        // Only this derived immutable projection is model-readable. Its source
        // closure retains complete provider evidence without granting that later
        // evidence as a top-level Context document.
        let mut sources = candidates.clone();
        sources.extend(collected.materialization.market_evidence.iter().cloned());
        sources.sort();
        sources.dedup();
        let packet = serde_json::json!({
            "type": "outcome_stage_context", "version": 1,
            "outcome_id": schedule.outcome_id, "horizon": horizon,
            "baseline_session": schedule.baseline_trading_day,
            "market_cutoff_session": stage_day, "facts_authority": "rust",
            "metric_basis": stage_facts.metric_basis,
            "actual_account_nav": {"status": "unavailable", "reason": "no subsequent fill and cash flow ledger"},
            "numeric_outcome": stage_facts,
            "prior_retrospectives": prior_retrospectives,
            "original_decision": schedule.decision,
            "research_sufficiency": {"hard_blockers": decision_context.hard_blockers,
                "soft_warnings": decision_context.soft_warnings},
            "instruction": "Review only this horizon. Earlier-stage unavailable narratives are unknown, never successful learning. Do not recompute or replace Rust numeric facts."
        });
        let stage_artifact = Artifact::new(
            ArtifactKind::SemanticDetail,
            self.store.stage_json(&packet)?,
            "learning.outcome_stage",
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
            sources,
            now,
        )?;
        self.store.write_task_artifact_fenced(
            Some(&outcome_lease),
            &task.permit,
            &stage_artifact,
            LifecycleEventType::OutcomeEvidence,
            Utc::now(),
        )?;
        candidates.push(ArtifactRef {
            artifact_id: stage_artifact.artifact_id.clone(),
            kind: stage_artifact.kind,
        });
        candidates.sort();
        candidates.dedup();
        let mut stage_node = task.node.clone();
        stage_node.objective = format!(
            "[outcome_horizon={}] Review outcome {} using only the Rust stage packet; market cutoff session {}.",
            serde_json::to_value(horizon)?.as_str().unwrap_or_default(),
            schedule.outcome_id.0,
            stage_day
        );
        let (retrospective_draft, diagnostic) = if task.node.contract_hash.is_some() {
            match self
                .agents
                .run(
                    &task.permit,
                    &stage_node,
                    candidates,
                    self.model_for(task.node.recipe_id.as_str()),
                    now,
                )
                .await
            {
                Ok(artifact) => {
                    // AgentRuntime returns a staged blob, not a committed ID.
                    let draft: RetrospectiveDraft =
                        serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                    draft.validate()?;
                    if draft.horizon != horizon || draft.outcome_id != schedule.outcome_id {
                        (None, "stage_identity_mismatch".to_owned())
                    } else {
                        self.store.write_task_artifact_fenced(
                            Some(&outcome_lease),
                            &task.permit,
                            &artifact,
                            LifecycleEventType::RetrospectiveDraftCreated,
                            Utc::now(),
                        )?;
                        (Some(draft), String::new())
                    }
                }
                Err(akzio_research::ResearchError::Store(error))
                | Err(akzio_research::ResearchError::Context(
                    akzio_context::ContextError::Store(error),
                )) => return Err(error.into()),
                Err(error) => {
                    // Preserve the typed error category without provider content.
                    tracing::warn!(run_id=%task.run_id, category=outcome_failure_category(&error), "outcome narrative unavailable");
                    (
                        None,
                        format!("narrative_failure:{}", outcome_failure_category(&error)),
                    )
                }
            }
        } else {
            (None, "contract_unavailable".to_owned())
        };
        let evaluation = EvaluationRuntime::new(self.store.clone(), EvaluationPolicy::default())?;
        if horizon != OutcomeHorizon::T5 {
            evaluation.record_partial_retrospective_with_diagnostic_fenced(
                &outcome_lease,
                &task.permit,
                collected.materialization,
                horizon,
                retrospective_draft.as_ref(),
                &prior_retrospectives,
                &diagnostic,
                Utc::now(),
            )?;
            return Ok(TaskCompletion::DeferredUntil(if pending.len() > 1 {
                Utc::now() + Duration::seconds(1)
            } else {
                next_outcome_check_at(now)?
            }));
        }
        if self.store.debug_learning_isolated(&task.run_id)? {
            evaluation.seal_outcome_with_retrospective_fenced(
                &outcome_lease,
                &task.permit,
                collected.materialization,
                retrospective_draft.as_ref(),
                &diagnostic,
                Utc::now(),
            )?;
            return Ok(TaskCompletion::Committed);
        }
        if let Some(session) = self.store.canary_session_for_run(&task.run_id)? {
            let materialization = collected.materialization;
            let (parent_outcome, _) = evaluation.seal_outcome_for_evaluation_fenced(
                &outcome_lease,
                &task.permit,
                materialization.clone(),
                retrospective_draft.as_ref(),
                &diagnostic,
                Utc::now(),
                false,
            )?;
            if !self.complete_canary_session(
                &outcome_lease,
                task,
                &session,
                &parent_outcome,
                Some(&materialization),
                retrospective_draft.as_ref(),
            )? {
                return Ok(TaskCompletion::DeferredUntil(next_outcome_check_at(now)?));
            }
            return Ok(TaskCompletion::Committed);
        }
        let decision_usage = self
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
        let input = EvaluationInput {
            permit: task.permit.clone(),
            subject: PolicySubject::Memory(MemoryId("paper:default".to_owned())),
            hypothesis_id: format!("paper-outcome:{}", schedule.outcome_id.0),
            materialization: collected.materialization,
            contract_hash,
            topology_id: TopologyId(snapshot.run.topology_id),
            candidate_policy: None,
            token_cost: decision_usage.billable_tokens_if_complete(),
            latency_millis: decision_usage.latency_millis_if_complete(),
        };
        if let Some(draft) = retrospective_draft.as_ref() {
            evaluation.evaluate_with_lease_and_retrospective(Some(&outcome_lease), input, draft)?;
        } else {
            evaluation.seal_outcome_with_rust_retrospective_fenced(
                &outcome_lease,
                &task.permit,
                input.materialization,
                &diagnostic,
                Utc::now(),
            )?;
        }
        Ok(TaskCompletion::Committed)
    }
}

fn outcome_failure_category(error: &akzio_research::ResearchError) -> &'static str {
    use akzio_research::ResearchError;
    match error {
        ResearchError::Context(_) => "context_rejected",
        ResearchError::InvalidOutput(_)
        | ResearchError::AmbiguousSubmission
        | ResearchError::MissingFinalOutput => "invalid_submission",
        ResearchError::WallTimeExceeded { .. } => "model_timeout",
        ResearchError::InputBudgetExceeded { .. }
        | ResearchError::OutputBudgetExceeded { .. }
        | ResearchError::ModelCallBudgetExceeded => "budget_exhausted",
        ResearchError::Store(_) => "store_error",
        _ => "model_or_contract_error",
    }
}
