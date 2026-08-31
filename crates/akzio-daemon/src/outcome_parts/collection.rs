use super::*;

impl Daemon {
    pub(crate) async fn collect_outcome_materialization(
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
        let mut acquisitions = Vec::new();
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
            let need_artifact =
                self.outcome_evidence_need(outcome_lease, task, schedule_reference, &need, now)?;
            let need_reference = ArtifactRef {
                artifact_id: need_artifact.artifact_id,
                kind: ArtifactKind::EvidenceNeed,
            };
            let request = EvidenceRequest {
                source: EvidenceSource::Alpaca,
                resource,
                max_age: Duration::days(7),
            };
            let acquired = runtime
                .acquire_validated_async(
                    &task.permit,
                    &need_reference,
                    &request,
                    adapter.as_ref(),
                    now,
                )
                .await?;
            let bars = parse_daily_bars(&acquired.normalized, acquired.observed_at)?;
            if bars.is_empty() {
                return Ok(None);
            }
            bars_by_asset.insert(asset, bars);
            acquisitions.push((need_reference, request, acquired));
        }

        let common_dates = common_bar_dates(&bars_by_asset, schedule.baseline_trading_day);
        if common_dates.is_empty() {
            return Ok(None);
        }
        let mut evidence_artifacts = Vec::with_capacity(acquisitions.len() * 2);
        for (need, request, acquired) in acquisitions {
            let bundle =
                runtime.materialize_validated(&task.permit, &need, &request, acquired, now)?;
            evidence_artifacts.extend([bundle.raw, bundle.normalized]);
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

    fn outcome_evidence_need(
        &self,
        outcome_lease: &DaemonLease,
        task: &ClaimedAttempt,
        schedule_reference: &ArtifactRef,
        need: &EvidenceNeed,
        now: DateTime<Utc>,
    ) -> Result<Artifact> {
        for artifact in self.store.artifacts_referencing(
            &schedule_reference.artifact_id,
            Some(ArtifactKind::EvidenceNeed),
        )? {
            if artifact.producer != "learning.outcome_worker.need" {
                continue;
            }
            let payload: EvidenceNeed =
                serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
            if payload != *need {
                continue;
            }
            if artifact.lifecycle != ArtifactLifecycle::RunScoped
                || artifact.source_refs != [schedule_reference.clone()]
                || artifact
                    .origin
                    .as_ref()
                    .and_then(|origin| origin.run_id.as_ref())
                    != Some(&task.run_id)
                || artifact
                    .origin
                    .as_ref()
                    .and_then(|origin| origin.task_id.as_ref())
                    != Some(&task.node.task_id)
                || artifact
                    .origin
                    .as_ref()
                    .and_then(|origin| origin.contract_hash.as_ref())
                    != task.permit.contract_hash.as_ref()
                || artifact.provenance.producer_contract_hash != task.permit.contract_hash
            {
                return Err(DaemonError::InvalidInput(
                    "outcome EvidenceNeed provenance is invalid".to_owned(),
                ));
            }
            return Ok(artifact);
        }

        let artifact = Artifact::new(
            ArtifactKind::EvidenceNeed,
            self.store.put_json(need)?,
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
            &artifact,
            LifecycleEventType::OutcomeNeed,
            now,
        )?;
        Ok(artifact)
    }

    pub(crate) fn paper_baseline_day(&self, run_id: &RunId) -> Result<NaiveDate> {
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
