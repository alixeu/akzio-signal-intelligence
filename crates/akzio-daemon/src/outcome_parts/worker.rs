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
        let Some(outcome_lease) = self.store.acquire_daemon_lease(
            OUTCOME_WORKER_LEASE_NAME,
            self.paper.scheduler.owner_id(),
            now,
            now + Duration::minutes(5),
        )?
        else {
            return Ok(TaskCompletion::Retry(RetryCause::Transport));
        };
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
        let Some(collected) = self
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

        self.store
            .validate_daemon_lease(&outcome_lease, Utc::now())?;
        for artifact in collected.evidence_artifacts {
            self.store.write_task_artifact_fenced(
                Some(&outcome_lease),
                &task.permit,
                &artifact,
                LifecycleEventType::OutcomeEvidence,
                now,
            )?;
        }
        let mut due_horizons = collected
            .materialization
            .observations
            .iter()
            .map(|observation| observation.horizon)
            .collect::<Vec<_>>();
        due_horizons.sort();
        due_horizons.dedup();
        let highest_due = due_horizons
            .last()
            .copied()
            .ok_or_else(|| DaemonError::Unavailable("no due outcome horizon".to_owned()))?;
        let prior_retrospectives = self
            .store
            .retrospectives(&task.run_id)?
            .into_iter()
            .map(|artifact| ArtifactRef {
                artifact_id: artifact.artifact_id,
                kind: ArtifactKind::Retrospective,
            })
            .collect::<Vec<_>>();
        let market_evidence = collected.materialization.market_evidence.clone();
        let retrospective_draft = if task.node.contract_hash.is_some() {
            match self.agent_session().candidates(task) {
                Ok(mut candidates) => {
                    candidates.extend(market_evidence.iter().cloned());
                    candidates.extend(prior_retrospectives.iter().cloned());
                    candidates.sort();
                    candidates.dedup();
                    match self
                        .agents
                        .run(
                            &task.permit,
                            &task.node,
                            candidates,
                            self.model_for(task.node.recipe_id.as_str()),
                            now,
                        )
                        .await
                    {
                        Ok(draft_artifact) => {
                            let draft_ref = ArtifactRef {
                                artifact_id: draft_artifact.artifact_id,
                                kind: draft_artifact.kind,
                            };
                            let draft =
                                self.read_artifact_payload::<RetrospectiveDraft>(&draft_ref)?;
                            if draft.horizon == highest_due {
                                Some(draft)
                            } else {
                                tracing::warn!(
                                    run_id = %task.run_id,
                                    expected = ?highest_due,
                                    actual = ?draft.horizon,
                                    "governed retrospective draft horizon did not match due horizon"
                                );
                                None
                            }
                        }
                        Err(error) => {
                            tracing::warn!(
                                run_id = %task.run_id,
                                error = %error,
                                "governed retrospective model unavailable; sealing Rust-only diagnostic"
                            );
                            None
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        run_id = %task.run_id,
                        error = %error,
                        "retrospective context unavailable; sealing Rust-only diagnostic"
                    );
                    None
                }
            }
        } else {
            None
        };
        let contract_hash = task.permit.contract_hash.clone().unwrap_or_else(|| {
            ContentHash::of_bytes(akzio_domain::LEARNING_OUTCOME_WORKER_RECIPE_ID.as_bytes())
        });
        let evaluation = EvaluationRuntime::new(self.store.clone(), EvaluationPolicy::default())?;
        for horizon in due_horizons
            .iter()
            .copied()
            .filter(|horizon| *horizon != OutcomeHorizon::T5)
        {
            if self
                .store
                .retrospective_for(&task.run_id, &schedule.outcome_id, horizon)?
                .is_some()
            {
                continue;
            }
            let mut partial = collected.materialization.clone();
            partial
                .observations
                .retain(|observation| observation.horizon <= horizon);
            let prior = if horizon == OutcomeHorizon::T3 {
                self.store
                    .retrospective_for(&task.run_id, &schedule.outcome_id, OutcomeHorizon::T1)?
                    .map(|artifact| {
                        vec![ArtifactRef {
                            artifact_id: artifact.artifact_id,
                            kind: ArtifactKind::Retrospective,
                        }]
                    })
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            let draft = (horizon == highest_due)
                .then_some(retrospective_draft.as_ref())
                .flatten();
            evaluation.record_partial_retrospective_fenced(
                &outcome_lease,
                &task.permit,
                partial,
                horizon,
                draft,
                &prior,
                now,
            )?;
        }
        if highest_due != OutcomeHorizon::T5 {
            return Ok(TaskCompletion::DeferredUntil(next_outcome_check_at(now)?));
        }
        if let Some(session) = self.store.canary_session_for_run(&task.run_id)? {
            let materialization = collected.materialization;
            let (parent_outcome, _) = evaluation.seal_outcome_with_retrospective_fenced(
                &outcome_lease,
                &task.permit,
                materialization.clone(),
                retrospective_draft.as_ref(),
                "canary parent outcome sealed",
                now,
            )?;
            if !self.complete_canary_session(
                &outcome_lease,
                task,
                &session,
                &parent_outcome,
                materialization,
                retrospective_draft.as_ref(),
            )? {
                return Ok(TaskCompletion::DeferredUntil(next_outcome_check_at(now)?));
            }
            return Ok(TaskCompletion::Committed);
        }

        let input = EvaluationInput {
            permit: task.permit.clone(),
            subject: PolicySubject::Memory(MemoryId("paper:default".to_owned())),
            hypothesis_id: format!("paper-outcome:{}", schedule.outcome_id.0),
            materialization: collected.materialization,
            contract_hash,
            topology_id: TopologyId("paper-outcome".to_owned()),
            candidate_policy: None,
            token_cost: None,
            latency_millis: None,
        };
        if let Some(draft) = retrospective_draft.as_ref() {
            let result = evaluation.evaluate_with_lease_and_retrospective(
                Some(&outcome_lease),
                input,
                draft,
            )?;
            let _ = result;
        } else {
            let _ = evaluation.seal_outcome_with_rust_retrospective_fenced(
                &outcome_lease,
                &task.permit,
                input.materialization,
                "governed retrospective model unavailable",
                now,
            )?;
        }
        Ok(TaskCompletion::Committed)
    }
}
