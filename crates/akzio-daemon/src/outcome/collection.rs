// 文件导读：Outcome collection 在独立 outcome lease 下读取 baseline Decision/ExecutionContext
// 和 Alpaca raw daily bars，寻找四资产共同的 T+1/T+3/T+5 session，构造可封存的
// OutcomeMaterializationInput。窗口不足只 Deferred；真实 bars/风险评估和后续 seal 分开，
// 不把采集完成写成 fill、NAV 或 learning 资格。
// Rust 机制：`BTreeMap<Asset, BTreeMap<Date, MoneyMicros>>` 对齐共同日期；`join_all`/迭代器
// 组合异步采集；fenced Store write 依赖 lease/permit，`Option<CollectedOutcome>` 表示尚未成熟。

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
        // market_day <= baseline 时先 Deferred，避免 mint 未来/倒置的 bars Need；之后每个
        // asset 独立采集、按共同 session 对齐，并把 risk-ground-truth 作为独立受审计输入。
        let adapter = self
            .production_evidence
            .get(&EvidenceSource::Alpaca)
            .ok_or_else(|| {
                DaemonError::Unavailable(
                    "Paper outcome worker requires Alpaca Paper evidence adapter".to_owned(),
                )
            })?;
        let market_day = akzio_ingest::market_session_day(now);
        // Overnight T0 belongs to the following trading date. Before that
        // date (and on T0 itself), no post-baseline daily session can exist.
        // Defer before minting a need or requesting an inverted/future range.
        if market_day <= schedule.baseline_trading_day {
            return Ok(None);
        }
        let decision_artifact = self.store.artifact(&schedule.decision.artifact_id)?;
        let decision: Decision =
            serde_json::from_slice(&self.store.read_blob(&decision_artifact.blob)?)?;
        let execution_context: ExecutionContext =
            self.read_artifact_payload(&schedule.execution_context)?;
        let realized_execution = self.realized_execution(schedule, &execution_context)?;
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
                "bars:{}:1d:{}:252:raw:{}",
                asset.symbol(),
                schedule.baseline_trading_day,
                market_day.min(schedule.baseline_trading_day + Duration::days(366))
            );
            let need = EvidenceNeed {
                schema_version: akzio_domain::DOMAIN_SCHEMA_VERSION,
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
                acquisition_mode: EvidenceAcquisitionMode::VerifiedSource,
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

        let common_dates = common_bar_dates(&bars_by_asset, schedule.baseline_trading_day)
            .into_iter()
            .take(5)
            .collect::<Vec<_>>();
        if common_dates.len() < 5
            && (bars_by_asset.values().any(|bars| bars.len() >= 252)
                || market_day >= schedule.baseline_trading_day + Duration::days(366))
        {
            return Err(DaemonError::Unavailable(
                "common-session search exhausted the bounded 252-bar/366-day window".to_owned(),
            ));
        }
        if common_dates.is_empty() {
            return Ok(None);
        }
        let expected_evidence_count = acquisitions.len() as u64;
        let mut evidence_artifacts = Vec::with_capacity(acquisitions.len() * 2);
        for (need, request, acquired) in acquisitions {
            let bundle =
                runtime.materialize_validated(&task.permit, &need, &request, acquired, now)?;
            evidence_artifacts.extend([bundle.raw, bundle.normalized]);
        }
        let observed_evidence_count = evidence_artifacts
            .iter()
            .filter(|artifact| artifact.kind == ArtifactKind::NormalizedEvidence)
            .count() as u64;
        let mut observations = horizon_observations(
            &bars_by_asset,
            &common_dates,
            expected_evidence_count,
            observed_evidence_count,
        )?;
        let assessments = self.outcome_risk_ground_truth_assessments(
            task,
            schedule_reference,
            schedule,
            &decision_artifact.producer,
            now,
        )?;
        apply_risk_ground_truth_assessments(
            &mut observations,
            schedule,
            schedule_reference,
            &decision_artifact.producer,
            &assessments,
            now,
        )?;
        let daily_observations = daily_observations(&bars_by_asset, &common_dates)?;
        Ok(Some(CollectedOutcome {
            materialization: OutcomeMaterializationInput {
                schedule: schedule.clone(),
                schedule_artifact: schedule_reference.clone(),
                target: realized_execution.target,
                forecasts: decision.forecasts,
                baseline_prices,
                observations,
                daily_observations,
                market_evidence: evidence_artifacts
                    .iter()
                    .filter(|artifact| artifact.kind == ArtifactKind::NormalizedEvidence)
                    .map(|artifact| ArtifactRef {
                        artifact_id: artifact.artifact_id.clone(),
                        kind: artifact.kind,
                    })
                    .collect(),
                cost_model: self.paper.outcome_cost_model,
                observed_execution: Some(realized_execution.metrics),
                sealed_at: now,
            },
            evidence_artifacts,
        }))
    }

    fn outcome_risk_ground_truth_assessments(
        &self,
        task: &ClaimedAttempt,
        schedule_reference: &ArtifactRef,
        schedule: &OutcomeSchedule,
        decision_producer_identity: &str,
        now: DateTime<Utc>,
    ) -> Result<Vec<(ArtifactRef, RiskGroundTruthAssessment)>> {
        // 只接受当前 Run、当前 schedule、canonical producer 的 assessment，并检查 source
        // closure；缺 assessment 不被默认填成满分。
        let mut assessments = Vec::new();
        for artifact in self.store.artifacts_referencing(
            &schedule_reference.artifact_id,
            Some(ArtifactKind::RiskGroundTruthAssessment),
        )? {
            if artifact.lifecycle != ArtifactLifecycle::Canonical
                || artifact.producer != "akzio-learning.risk_ground_truth"
                || artifact
                    .origin
                    .as_ref()
                    .and_then(|origin| origin.run_id.as_ref())
                    != Some(&task.run_id)
            {
                return Err(DaemonError::InvalidInput(
                    "Risk ground-truth artifact has invalid lifecycle or origin".to_owned(),
                ));
            }
            let assessment: RiskGroundTruthAssessment =
                serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
            assessment.validate_sealed_at(now)?;
            if assessment.schedule != *schedule_reference
                || assessment.decision != schedule.decision
                || assessment.decision_producer_identity != decision_producer_identity
            {
                return Err(DaemonError::InvalidInput(
                    "Risk ground-truth artifact identity mismatch".to_owned(),
                ));
            }
            let mut expected_sources = vec![schedule_reference.clone(), schedule.decision.clone()];
            expected_sources.extend(assessment.basis_refs.iter().cloned());
            expected_sources.sort();
            expected_sources.dedup();
            if artifact.source_refs != expected_sources {
                return Err(DaemonError::InvalidInput(
                    "Risk ground-truth artifact source closure mismatch".to_owned(),
                ));
            }
            assessments.push((
                ArtifactRef {
                    artifact_id: artifact.artifact_id,
                    kind: ArtifactKind::RiskGroundTruthAssessment,
                },
                assessment,
            ));
        }
        Ok(assessments)
    }

    fn outcome_evidence_need(
        &self,
        outcome_lease: &DaemonLease,
        task: &ClaimedAttempt,
        schedule_reference: &ArtifactRef,
        need: &EvidenceNeed,
        now: DateTime<Utc>,
    ) -> Result<Artifact> {
        // 先复用同一 task 已创建且 provenance 完整的 Need；否则在 outcome lease fencing 下
        // 新建一份，保证 crash/retry 不重写旧 CAS。
        for artifact in self
            .store
            .run_artifacts_by_kind(&task.run_id, ArtifactKind::EvidenceNeed)?
        {
            if artifact.producer != "learning.outcome_worker.need"
                || !artifact.source_refs.contains(schedule_reference)
                || artifact
                    .origin
                    .as_ref()
                    .and_then(|origin| origin.task_id.as_ref())
                    != Some(&task.node.task_id)
            {
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
            self.store.stage_json(need)?,
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
        // Baseline 来自 scheduler session slot，而不是当前自然日/UTC 日期；没有 slot 的
        // Paper Run 不能被 Outcome worker 猜测继续。
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
