//! Outcome collection and evaluation dispatch.

use super::*;

impl Daemon {
    pub(super) async fn execute_outcome_worker(
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
            match self.context_candidates(task) {
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

    pub(super) fn realized_execution_target(
        &self,
        schedule: &OutcomeSchedule,
        execution_context: &ExecutionContext,
    ) -> Result<TargetPortfolio> {
        let account_reference = execution_context.account_snapshot.as_ref().ok_or_else(|| {
            DaemonError::InvalidInput(
                "Outcome execution context has no account snapshot".to_owned(),
            )
        })?;
        let account: AccountSnapshot = self.read_artifact_payload(account_reference)?;
        account
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        let mut values = Asset::EXECUTABLE
            .into_iter()
            .map(|asset| {
                let value = account
                    .positions
                    .get(&asset)
                    .map_or(0_i128, |position| i128::from(position.market_value.0));
                (asset, value)
            })
            .collect::<BTreeMap<_, _>>();

        if let OutcomeExecutionLineage::ReconciledPaper { reconciliation, .. } = &schedule.execution
        {
            let reconciliation: Reconciliation = self.read_artifact_payload(reconciliation)?;
            if reconciliation.state != ReconciliationState::Complete {
                return Err(DaemonError::InvalidInput(
                    "Outcome requires complete reconciliation".to_owned(),
                ));
            }
            let plan_reference = execution_context.execution_plan.as_ref().ok_or_else(|| {
                DaemonError::InvalidInput("Outcome execution context has no plan".to_owned())
            })?;
            let plan: ExecutionPlan = self.read_artifact_payload(plan_reference)?;
            for receipt_reference in &reconciliation.broker_receipts {
                let receipt: OrderReceipt = self.read_artifact_payload(receipt_reference)?;
                if receipt.state != OrderReceiptState::Filled {
                    return Err(DaemonError::InvalidInput(
                        "Outcome reconciliation contains non-filled receipt".to_owned(),
                    ));
                }
                let fill_price = receipt.average_fill_price.ok_or_else(|| {
                    DaemonError::InvalidInput("Filled receipt has no average price".to_owned())
                })?;
                let order = plan
                    .orders
                    .iter()
                    .find(|order| order.asset == receipt.asset)
                    .ok_or_else(|| {
                        DaemonError::InvalidInput(
                            "Filled receipt is not in execution plan".to_owned(),
                        )
                    })?;
                let fill_value = i128::from(receipt.filled_quantity_micros)
                    .saturating_mul(i128::from(fill_price.0))
                    .saturating_div(1_000_000);
                let signed = match order.side {
                    OrderSide::Buy => fill_value,
                    OrderSide::Sell => -fill_value,
                };
                let value = values.get_mut(&receipt.asset).expect("v2 asset is indexed");
                *value = value.saturating_add(signed);
                if *value < 0 {
                    return Err(DaemonError::InvalidInput(
                        "Execution fills produce a short realized position".to_owned(),
                    ));
                }
            }
        }

        let equity = i128::from(account.equity.0);
        let weights = values
            .into_iter()
            .map(|(asset, value)| {
                let ppm = value
                    .saturating_mul(1_000_000)
                    .checked_div(equity)
                    .and_then(|value| u32::try_from(value).ok())
                    .ok_or_else(|| {
                        DaemonError::InvalidInput("Realized position weight invalid".to_owned())
                    })?;
                Ok((asset, WeightPpm(ppm)))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let target = TargetPortfolio { weights };
        target
            .validate_universe()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        Ok(target)
    }

    pub(super) async fn collect_outcome_materialization(
        &self,
        outcome_lease: &DaemonLease,
        task: &ClaimedAttempt,
        schedule_reference: &ArtifactRef,
        schedule: &OutcomeSchedule,
        now: DateTime<Utc>,
    ) -> Result<Option<CollectedOutcome>> {
        let adapter = self
            .production_evidence
            .get(&EvidenceSource::Alpaca)
            .ok_or_else(|| {
                DaemonError::Unavailable(
                    "Paper outcome worker requires Alpaca Paper evidence adapter".to_owned(),
                )
            })?;
        let decision: Decision = self.read_artifact_payload(&schedule.decision)?;
        let decision_context: DecisionContext =
            self.read_artifact_payload(&schedule.decision_context)?;
        let execution_context: ExecutionContext =
            self.read_artifact_payload(&schedule.execution_context)?;
        let realized_target = self.realized_execution_target(schedule, &execution_context)?;
        let quote_reference = execution_context.quote_snapshot.clone().ok_or_else(|| {
            DaemonError::Unavailable("Paper outcome baseline quote snapshot missing".to_owned())
        })?;
        let quote_artifact = self.store.artifact(&quote_reference.artifact_id)?;
        let quotes: QuoteSnapshot =
            serde_json::from_slice(&self.store.read_blob(&quote_artifact.blob)?)?;
        quotes.validate()?;
        let baseline_prices = quotes
            .quotes
            .into_iter()
            .map(|(asset, quote)| {
                let midpoint = quote
                    .bid
                    .0
                    .checked_add(quote.ask.0)
                    .and_then(|value| value.checked_div(2))
                    .unwrap_or_default();
                (asset, MoneyMicros(midpoint))
            })
            .collect::<BTreeMap<_, _>>();
        if Asset::EXECUTABLE
            .into_iter()
            .any(|asset| baseline_prices.get(&asset).is_none_or(|price| price.0 <= 0))
        {
            return Err(DaemonError::Unavailable(
                "Paper outcome baseline quotes are incomplete".to_owned(),
            ));
        }

        let mut bars_by_asset = BTreeMap::<Asset, BTreeMap<NaiveDate, MoneyMicros>>::new();
        let mut evidence_artifacts = Vec::new();
        let runtime = EvidenceRuntime::new(self.store.clone(), [EvidenceSource::Alpaca]);
        for asset in Asset::EXECUTABLE {
            let resource = format!(
                "bars:{}:1d:{}:6",
                asset.symbol(),
                schedule.baseline_trading_day
            );
            let need = EvidenceNeed {
                schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
                source_family: EvidenceSource::Alpaca.as_str().to_owned(),
                resource: resource.clone(),
                max_age_secs: 604_800,
            };
            let need_artifact = Artifact::new(
                ArtifactKind::EvidenceNeed,
                self.store.put_json(&need)?,
                "learning.outcome_worker.need",
                ArtifactLifecycle::RunScoped,
                ArtifactProvenance {
                    source_family: "akzio-learning".to_owned(),
                    observed_at: Some(now),
                    retrieved_at: now,
                    source_uri: None,
                    confidence_ppm: 1_000_000,
                    producer_contract_hash: task.permit.contract_hash.clone(),
                },
                Some(ArtifactOrigin {
                    run_id: Some(task.run_id.clone()),
                    task_id: Some(task.node.task_id.clone()),
                    attempt_id: Some(task.permit.attempt_id.clone()),
                    contract_hash: task.permit.contract_hash.clone(),
                }),
                vec![schedule_reference.clone()],
                now,
            )?;
            self.store.write_task_artifact_fenced(
                Some(outcome_lease),
                &task.permit,
                &need_artifact,
                LifecycleEventType::OutcomeNeed,
                now,
            )?;
            let bundle = runtime
                .acquire_and_normalize_async(
                    &task.permit,
                    &ArtifactRef {
                        artifact_id: need_artifact.artifact_id.clone(),
                        kind: ArtifactKind::EvidenceNeed,
                    },
                    &EvidenceRequest {
                        source: EvidenceSource::Alpaca,
                        resource,
                        max_age: Duration::days(7),
                    },
                    adapter.as_ref(),
                    now,
                )
                .await?;
            let envelope: NormalizedEvidencePayload =
                serde_json::from_slice(&self.store.read_blob(&bundle.normalized.blob)?)?;
            let bars = parse_daily_bars(&envelope.value, envelope.observed_at)?;
            if bars.is_empty() {
                return Ok(None);
            }
            bars_by_asset.insert(asset, bars);
            evidence_artifacts.extend([bundle.raw, bundle.normalized]);
        }

        let common_dates = common_bar_dates(&bars_by_asset, schedule.baseline_trading_day);
        if common_dates.is_empty() {
            return Ok(None);
        }
        let observations = horizon_observations(
            &bars_by_asset,
            &common_dates,
            (decision_context.hard_blockers.len() + decision_context.material_conflicts.len())
                as u64,
        )?;
        Ok(Some(CollectedOutcome {
            materialization: OutcomeMaterializationInput {
                schedule: schedule.clone(),
                schedule_artifact: schedule_reference.clone(),
                target: realized_target,
                forecasts: decision.forecasts,
                baseline_prices,
                observations,
                market_evidence: evidence_artifacts
                    .iter()
                    .filter(|artifact| artifact.kind == ArtifactKind::NormalizedEvidence)
                    .map(|artifact| ArtifactRef {
                        artifact_id: artifact.artifact_id.clone(),
                        kind: artifact.kind,
                    })
                    .collect(),
                cost_model: self.paper.outcome_cost_model,
                sealed_at: now,
            },
            evidence_artifacts,
        }))
    }

    pub(super) fn paper_baseline_day(&self, run_id: &RunId) -> Result<NaiveDate> {
        let slot = self.store.session_slot_for_run(run_id)?.ok_or_else(|| {
            DaemonError::InvalidInput(format!("Paper run {run_id} has no session slot"))
        })?;
        NaiveDate::parse_from_str(&slot.session_key, "%Y-%m-%d").map_err(|_| {
            DaemonError::InvalidInput(format!(
                "Paper session key {} is not a broker trading date",
                slot.session_key
            ))
        })
    }
}

fn next_outcome_check_at(now: DateTime<Utc>) -> Result<DateTime<Utc>> {
    let today = DateTime::from_naive_utc_and_offset(
        now.date_naive()
            .and_hms_opt(22, 0, 0)
            .expect("22:00 UTC is a valid time"),
        Utc,
    );
    if today > now {
        return Ok(today);
    }
    let tomorrow = now
        .date_naive()
        .succ_opt()
        .ok_or_else(|| DaemonError::Unavailable("outcome check date overflow".to_owned()))?;
    Ok(DateTime::from_naive_utc_and_offset(
        tomorrow
            .and_hms_opt(22, 0, 0)
            .expect("22:00 UTC is a valid time"),
        Utc,
    ))
}
