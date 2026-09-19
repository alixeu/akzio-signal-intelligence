use akzio_execution::{
    build_offline_decision_policy, HistoricalForecastProvenance, HistoricalForecastSample,
    HistoricalPricePoint, HistoricalPriceSeries, OfflineCalibrationInput, OfflineRiskLimits,
};
use akzio_domain::{
    ArtifactKind, ArtifactLifecycle, ArtifactRef, Decision, DecisionContext, DecisionHorizon,
    MoneyMicros, Outcome, OutcomeHorizon, OutcomeSchedule,
};
use akzio_ingest::{parse_daily_bars, EvidenceAdapterError, NormalizedEvidencePayload};
use chrono::{DateTime, Duration as ChronoDuration};

fn handle_calibration(command: &CalibrationCommand, config_path: &Path) -> Result<()> {
    match command {
        CalibrationCommand::Build { input, output } => {
            if output.exists() {
                bail!("refusing to overwrite existing policy {}", output.display());
            }
            let input_bytes = fs::read(input)
                .with_context(|| format!("read calibration dataset {}", input.display()))?;
            let dataset: OfflineCalibrationInput = serde_json::from_slice(&input_bytes)
                .with_context(|| format!("decode calibration dataset {}", input.display()))?;
            let artifact = build_offline_decision_policy(&dataset, Utc::now())
                .context("fit offline historical decision policy")?;
            let bytes = serde_json::to_vec_pretty(&artifact).context("encode policy artifact")?;
            let parent = output
                .parent()
                .context("policy output must have a parent directory")?;
            fs::create_dir_all(parent)
                .with_context(|| format!("create policy output directory {}", parent.display()))?;
            fs::write(output, bytes)
                .with_context(|| format!("write policy artifact {}", output.display()))?;
            print_json(&serde_json::json!({
                "status": "built",
                "output": output,
                "policy_version": artifact.provenance.policy_version,
                "algorithm_version": artifact.provenance.algorithm_version,
                "training_start": artifact.provenance.training_start,
                "training_end": artifact.provenance.training_end,
                "training_samples": artifact.provenance.sample_count,
                "source_runs": artifact.provenance.source_runs,
                "provider_id": artifact.provenance.provider_id,
                "model_route": artifact.provenance.model_route,
                "contract_hash": artifact.provenance.contract_hash,
                "calibrated_assets": artifact.policy.asset_calibrations.keys().collect::<Vec<_>>(),
                "decision_capable": artifact.policy.decision_capable(),
                "decision_policy_status": decision_policy_status(&artifact.policy),
                "risk_model_hash": artifact.provenance.risk_model_hash,
                "policy_hash": artifact.provenance.output_hash,
            }))
        }
        CalibrationCommand::Export {
            store,
            risk_limits,
            output,
            min_samples,
            training_start,
            training_end,
        } => export_calibration_dataset(
            store,
            risk_limits,
            output,
            *min_samples,
            training_start.as_deref(),
            training_end.as_deref(),
            config_path,
        ),
        CalibrationCommand::Validate { input } => {
            let bytes = fs::read(input)
                .with_context(|| format!("read policy artifact {}", input.display()))?;
            let artifact = DecisionPolicyArtifact::decode_strict(&bytes)
                .context("validate frozen decision policy artifact")?;
            print_json(&serde_json::json!({
                "status": "valid",
                "input": input,
                "policy_version": artifact.provenance.policy_version,
                "training_samples": artifact.provenance.sample_count,
                "provider_id": artifact.provenance.provider_id,
                "model_route": artifact.provenance.model_route,
                "contract_hash": artifact.provenance.contract_hash,
                "calibrated_assets": artifact.policy.asset_calibrations.keys().collect::<Vec<_>>(),
                "decision_capable": artifact.policy.decision_capable(),
                "decision_policy_status": decision_policy_status(&artifact.policy),
                "risk_model_hash": artifact.provenance.risk_model_hash,
                "policy_hash": artifact.provenance.output_hash,
            }))
        }
        CalibrationCommand::Inspect { input } => {
            let bytes = fs::read(input)
                .with_context(|| format!("read policy artifact {}", input.display()))?;
            let artifact = DecisionPolicyArtifact::decode_strict(&bytes)
                .context("decode frozen decision policy artifact")?;
            print_json(&serde_json::json!({
                "input": input,
                "policy_version": artifact.provenance.policy_version,
                "algorithm_version": artifact.provenance.algorithm_version,
                "created_at": artifact.provenance.created_at,
                "training_window": {
                    "start": artifact.provenance.training_start,
                    "end": artifact.provenance.training_end,
                },
                "training_samples": artifact.provenance.sample_count,
                "source_runs": artifact.provenance.source_runs,
                "provider_id": artifact.provenance.provider_id,
                "model_route": artifact.provenance.model_route,
                "contract_hash": artifact.provenance.contract_hash,
                "assets": artifact.provenance.assets,
                "horizons": artifact.provenance.horizons,
                "calibrated_assets": artifact.policy.asset_calibrations.keys().collect::<Vec<_>>(),
                "decision_policy_path_ready": true,
                "decision_capable": artifact.policy.decision_capable(),
                "decision_policy_status": decision_policy_status(&artifact.policy),
                "risk_model_hash": artifact.provenance.risk_model_hash,
                "policy_hash": artifact.provenance.output_hash,
            }))
        }
    }
}

fn decision_policy_status(policy: &DecisionPolicy) -> &'static str {
    if policy.decision_capable() {
        "ready_for_current_decision"
    } else if policy.asset_calibrations.is_empty() {
        "validated_but_no_asset_calibration"
    } else {
        "validated_but_insufficient_samples"
    }
}

struct CalibrationExportCandidate {
    run_id: RunId,
    decision_artifact: akzio_domain::Artifact,
    decision: Decision,
    outcome: Outcome,
    schedule: OutcomeSchedule,
    bars: BTreeMap<Asset, BTreeMap<NaiveDate, MoneyMicros>>,
    provider_id: String,
    model_id: String,
    contract_hash: ContentHash,
}

fn export_calibration_dataset(
    store_path: &Path,
    risk_limits_path: &Path,
    output: &Path,
    min_samples: u32,
    training_start: Option<&str>,
    training_end: Option<&str>,
    config_path: &Path,
) -> Result<()> {
    if output.exists() {
        bail!("refusing to overwrite calibration dataset {}", output.display());
    }
    if min_samples == 0 {
        bail!("calibration export requires min_samples > 0");
    }
    let report_path = output.with_extension("report.json");
    if report_path.exists() {
        bail!("refusing to overwrite calibration report {}", report_path.display());
    }
    if path_is_within(output, store_path) || path_is_within(&report_path, store_path) {
        bail!(
            "calibration dataset and report must be outside the source Store Root"
        );
    }
    let output_parent = output
        .parent()
        .context("calibration export output must have a parent directory")?;
    fs::create_dir_all(output_parent)
        .with_context(|| format!("create calibration export directory {}", output_parent.display()))?;
    let config = load_config(config_path).context("load model identity for calibration export")?;
    let model_config = config
        .model
        .as_ref()
        .context("calibration export requires [model] configuration")?;
    let synthesizer_route = model_config.routes.get("research.synthesizer");
    if synthesizer_route
        .and_then(|route| route.release_date.as_ref())
        .or(model_config.release_date.as_ref())
        .is_none()
        || synthesizer_route
            .and_then(|route| route.knowledge_cutoff.as_ref())
            .or(model_config.knowledge_cutoff.as_ref())
            .is_none()
    {
        bail!(
            "calibration export requires release_date and knowledge_cutoff for the effective research.synthesizer route"
        );
    }
    let (model_id, model_version_hash) = configured_synthesizer_identity(model_config)?;
    let provider_id = model_config.provider_identity().as_str().to_owned();
    let risk_limits: OfflineRiskLimits = serde_json::from_slice(
        &fs::read(risk_limits_path)
            .with_context(|| format!("read approved calibration risk limits {}", risk_limits_path.display()))?,
    )
    .with_context(|| format!("decode approved calibration risk limits {}", risk_limits_path.display()))?;
    let requested_start = training_start
        .map(|value| parse_calibration_time(value, "training_start"))
        .transpose()?;
    let requested_end = training_end
        .map(|value| parse_calibration_time(value, "training_end"))
        .transpose()?;
    if requested_start.zip(requested_end).is_some_and(|(start, end)| start > end) {
        bail!("calibration training_start must not be after training_end");
    }

    let store = Store::open_existing(store_path)
        .with_context(|| format!("open calibration source Store {} read-only", store_path.display()))?;
    let decision_artifacts = store
        .recent_artifacts_by_kind(ArtifactKind::Decision, 500)
        .context("scan recent Decision artifacts for calibration export")?;
    let mut skipped = Vec::new();
    let mut candidates = Vec::new();
    for decision_artifact in decision_artifacts.iter().cloned() {
        let Some(run_id) = decision_artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.clone())
        else {
            continue;
        };
        if store.run_purpose(&run_id)? != RunPurpose::Paper {
            continue;
        }
        if decision_artifact.lifecycle != ArtifactLifecycle::Canonical {
            skipped.push(serde_json::json!({
                "run_id": run_id,
                "reason": "decision_not_canonical",
            }));
            continue;
        }
        let decision: Decision = match serde_json::from_slice(&store.read_blob(&decision_artifact.blob)?) {
            Ok(value) => value,
            Err(error) => {
                skipped.push(serde_json::json!({"run_id":run_id,"reason":"decision_decode","detail":error.to_string()}));
                continue;
            }
        };
        if let Err(error) = decision.validate() {
            skipped.push(serde_json::json!({"run_id":run_id,"reason":"decision_invalid","detail":error.to_string()}));
            continue;
        }
        let context_artifact = match store.artifact(&decision.decision_context.artifact_id) {
            Ok(value) => value,
            Err(error) => {
                skipped.push(serde_json::json!({"run_id":run_id,"reason":"decision_context_missing","detail":error.to_string()}));
                continue;
            }
        };
        let context: DecisionContext = match serde_json::from_slice(&store.read_blob(&context_artifact.blob)?) {
            Ok(value) => value,
            Err(error) => {
                skipped.push(serde_json::json!({"run_id":run_id,"reason":"decision_context_decode","detail":error.to_string()}));
                continue;
            }
        };
        if let Err(error) = context.validate() {
            skipped.push(serde_json::json!({"run_id":run_id,"reason":"decision_context_invalid","detail":error.to_string()}));
            continue;
        }
        let Some(outcome_artifact) = store.outcome_for_run(&run_id)? else {
            skipped.push(serde_json::json!({"run_id":run_id,"reason":"outcome_not_mature"}));
            continue;
        };
        if outcome_artifact.lifecycle != ArtifactLifecycle::Canonical {
            skipped.push(serde_json::json!({"run_id":run_id,"reason":"outcome_not_canonical"}));
            continue;
        }
        let outcome: Outcome = match serde_json::from_slice(&store.read_blob(&outcome_artifact.blob)?) {
            Ok(value) => value,
            Err(error) => {
                skipped.push(serde_json::json!({"run_id":run_id,"reason":"outcome_decode","detail":error.to_string()}));
                continue;
            }
        };
        if let Err(error) = outcome.validate_sealed() {
            skipped.push(serde_json::json!({"run_id":run_id,"reason":"outcome_not_sealed","detail":error.to_string()}));
            continue;
        }
        let schedule_artifact = match store.artifact(&outcome.schedule.artifact_id) {
            Ok(value) => value,
            Err(error) => {
                skipped.push(serde_json::json!({"run_id":run_id,"reason":"schedule_missing","detail":error.to_string()}));
                continue;
            }
        };
        let schedule: OutcomeSchedule = match serde_json::from_slice(&store.read_blob(&schedule_artifact.blob)?) {
            Ok(value) => value,
            Err(error) => {
                skipped.push(serde_json::json!({"run_id":run_id,"reason":"schedule_decode","detail":error.to_string()}));
                continue;
            }
        };
        if schedule.decision != (ArtifactRef {
            artifact_id: decision_artifact.artifact_id.clone(),
            kind: ArtifactKind::Decision,
        }) {
            skipped.push(serde_json::json!({"run_id":run_id,"reason":"schedule_decision_mismatch"}));
            continue;
        }
        let Some(identity) = synthesizer_identity_for_decision(
            &store,
            &decision_artifact,
            &decision,
            &run_id,
        )? else {
            skipped.push(serde_json::json!({"run_id":run_id,"reason":"synthesizer_identity_missing"}));
            continue;
        };
        if identity.provider_id != provider_id || identity.model_id != model_id {
            skipped.push(serde_json::json!({
                "run_id":run_id,
                "reason":"model_identity_mismatch",
                "expected_model":model_id,
                "actual_model":identity.model_id,
                "expected_provider":provider_id,
                "actual_provider":identity.provider_id,
            }));
            continue;
        }
        let bars = match outcome_bars(&store, &outcome) {
            Ok(value) => value,
            Err(error) => {
                skipped.push(serde_json::json!({"run_id":run_id,"reason":"outcome_bars_unusable","detail":error.to_string()}));
                continue;
            }
        };
        if let Err(reason) = candidate_has_all_labels(&decision, &schedule, &outcome, &bars) {
            skipped.push(serde_json::json!({"run_id":run_id,"reason":"maturity_label_missing","detail":reason}));
            continue;
        }
        candidates.push(CalibrationExportCandidate {
            run_id,
            decision_artifact,
            decision,
            outcome,
            schedule,
            bars,
            provider_id: identity.provider_id,
            model_id: identity.model_id,
            contract_hash: identity.contract_hash,
        });
    }
    candidates.sort_by(|left, right| left.run_id.cmp(&right.run_id));

    let contract_hash = candidates.first().map(|candidate| candidate.contract_hash.clone());
    let mut selected = Vec::new();
    for candidate in candidates {
        if contract_hash.as_ref() == Some(&candidate.contract_hash) {
            selected.push(candidate);
        } else {
            skipped.push(serde_json::json!({
                "run_id": candidate.run_id,
                "reason": "contract_identity_mismatch",
            }));
        }
    }
    let actual_start = requested_start.or_else(|| {
        selected
            .iter()
            .map(|candidate| candidate.decision.created_at)
            .min()
    });
    let actual_end = requested_end.or_else(|| {
        selected
            .iter()
            .flat_map(|candidate| candidate.outcome.windows.iter())
            .map(|window| session_close_time(window.observed_trading_day))
            .max()
    });
    let mut samples = Vec::new();
    let mut panel = BTreeMap::<Asset, BTreeMap<NaiveDate, MoneyMicros>>::new();
    let mut exported_runs = Vec::new();
    let mut sample_counts = BTreeMap::<String, usize>::new();
    let mut price_conflicts = Vec::new();
    for candidate in &selected {
        let Some(start) = actual_start else { continue };
        let Some(end) = actual_end else { continue };
        let mut candidate_samples = Vec::new();
        for forecast in &candidate.decision.forecasts {
            let window_horizon = match forecast.horizon {
                DecisionHorizon::T1 => OutcomeHorizon::T1,
                DecisionHorizon::T3 => OutcomeHorizon::T3,
                DecisionHorizon::T5 => OutcomeHorizon::T5,
            };
            let Some(window) = candidate
                .outcome
                .windows
                .iter()
                .find(|window| window.horizon == window_horizon)
            else {
                continue;
            };
            let Some(base) = candidate.bars.get(&forecast.asset).and_then(|bars| {
                bars.get(&candidate.schedule.baseline_trading_day)
            }) else {
                continue;
            };
            let Some(future) = candidate
                .bars
                .get(&forecast.asset)
                .and_then(|bars| bars.get(&window.observed_trading_day))
            else {
                continue;
            };
            let realized_at = session_close_time(window.observed_trading_day);
            if candidate.decision.created_at < start
                || candidate.decision.created_at > end
                || realized_at > end
            {
                continue;
            }
            candidate_samples.push(HistoricalForecastSample {
                source_run_id: candidate.run_id.0.clone(),
                asset: forecast.asset,
                horizon: forecast.horizon,
                forecast_at: candidate.decision.created_at,
                realized_at,
                raw_probability_ppm: forecast.positive_return_probability_ppm,
                raw_expected_return_ppm: forecast.expected_return_ppm,
                realized_return_ppm: relative_return_ppm(*base, *future)?,
                provenance: HistoricalForecastProvenance {
                    provider_id: candidate.provider_id.clone(),
                    model_id: candidate.model_id.clone(),
                    model_version_hash: model_version_hash.clone(),
                    route: "research.synthesizer".to_owned(),
                    contract_hash: candidate.contract_hash.clone(),
                    source_decision: ArtifactRef {
                        artifact_id: candidate.decision_artifact.artifact_id.clone(),
                        kind: ArtifactKind::Decision,
                    },
                    source_decision_context: candidate.decision.decision_context.clone(),
                    forecast_cutoff: candidate.decision.created_at,
                },
            });
        }
        if candidate_samples.len() != Asset::EXECUTABLE.len() * DecisionHorizon::ALL.len() {
            skipped.push(serde_json::json!({
                "run_id": candidate.run_id,
                "reason": "training_window_excludes_complete_forecast_set",
                "sample_count": candidate_samples.len(),
            }));
            continue;
        }
        for (key, count) in candidate_samples.iter().fold(BTreeMap::<String, usize>::new(), |mut counts, sample| {
            *counts.entry(format!("{}:{:?}", sample.asset, sample.horizon)).or_default() += 1;
            counts
        }) {
            *sample_counts.entry(key).or_default() += count;
        }
        for (asset, bars) in &candidate.bars {
            for (date, price) in bars {
                if actual_start.is_some_and(|start| *date < start.date_naive())
                    || actual_end.is_some_and(|end| *date > end.date_naive())
                {
                    continue;
                }
                match panel.entry(*asset).or_default().entry(*date) {
                    std::collections::btree_map::Entry::Occupied(existing) => {
                        if *existing.get() != *price {
                        price_conflicts.push(serde_json::json!({
                            "run_id": candidate.run_id,
                            "asset": asset,
                            "date": date,
                            "existing_close_micros": existing.get(),
                            "incoming_close_micros": price,
                        }));
                        }
                    }
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(*price);
                    }
                }
            }
        }
        samples.extend(candidate_samples);
        exported_runs.push(candidate.run_id.0.clone());
    }
    samples.sort_by(|left, right| {
        left.source_run_id
            .cmp(&right.source_run_id)
            .then_with(|| left.asset.cmp(&right.asset))
            .then_with(|| left.horizon.cmp(&right.horizon))
    });
    exported_runs.sort();
    exported_runs.dedup();
    let common_dates = panel
        .values()
        .map(|bars| bars.keys().copied().collect::<BTreeSet<_>>())
        .reduce(|left, right| left.intersection(&right).copied().collect())
        .unwrap_or_default();
    let price_series = Asset::EXECUTABLE
        .into_iter()
        .map(|asset| HistoricalPriceSeries {
            asset,
            points: common_dates
                .iter()
                .filter_map(|date| {
                    panel.get(&asset)?.get(date).map(|price| HistoricalPricePoint {
                        observed_at: *date,
                        close_micros: price.0,
                    })
                })
                .collect(),
        })
        .collect::<Vec<_>>();
    let required_forecast_samples = usize::try_from(min_samples)
        .unwrap_or(usize::MAX)
        .saturating_mul(Asset::EXECUTABLE.len())
        .saturating_mul(DecisionHorizon::ALL.len());
    let enough_forecasts = sample_counts.values().all(|count| *count >= usize::try_from(min_samples).unwrap_or(usize::MAX))
        && sample_counts.len() == Asset::EXECUTABLE.len() * DecisionHorizon::ALL.len();
    let enough_prices = price_series
        .iter()
        .all(|series| series.points.len() >= usize::try_from(min_samples).unwrap_or(usize::MAX).saturating_add(1));
    let status = if contract_hash.is_some()
        && actual_start.is_some()
        && actual_end.is_some()
        && !samples.is_empty()
        && samples.len() >= required_forecast_samples
        && enough_forecasts
        && enough_prices
        && price_conflicts.is_empty()
    {
        "ready_for_build"
    } else {
        "BLOCKED"
    };
    let mut dataset_path = None;
    let mut input_hash = None;
    if !samples.is_empty() && price_conflicts.is_empty() {
        let input = OfflineCalibrationInput {
            schema_version: 1,
            policy_version: "offline_historical_v2".to_owned(),
            algorithm_version: "offline_historical_risk_v1".to_owned(),
            provider_id: provider_id.clone(),
            model_id: model_id.clone(),
            model_version_hash,
            model_route: "research.synthesizer".to_owned(),
            contract_hash: contract_hash.clone().expect("samples have contract identity"),
            regime: "all".to_owned(),
            training_start: actual_start.unwrap_or_else(Utc::now),
            training_end: actual_end.unwrap_or_else(Utc::now),
            min_samples,
            source_runs: exported_runs.clone(),
            forecasts: samples,
            price_series,
            risk_limits,
        };
        let canonical_input_hash = content_hash_json(&serde_json::to_value(&input)?)?;
        let bytes = serde_json::to_vec_pretty(&input).context("encode calibration dataset")?;
        input_hash = Some(canonical_input_hash);
        fs::write(output, &bytes)
            .with_context(|| format!("write calibration dataset {}", output.display()))?;
        dataset_path = Some(output);
    }
    let report = serde_json::json!({
        "status": status,
        "source_store": store_path,
        "dataset": dataset_path,
        "report": report_path,
        "input_hash": input_hash,
        "scan": {
            "decision_artifacts_considered": decision_artifacts.len(),
            "scan_limit": 500,
            "may_be_truncated": decision_artifacts.len() == 500,
            "matured_runs": exported_runs.len(),
            "exported_forecast_samples": sample_counts.values().sum::<usize>(),
            "sample_counts": sample_counts,
            "common_price_points": common_dates.len(),
        },
        "identity": {
            "provider_id": provider_id,
            "model_id": model_id,
            "model_route": "research.synthesizer",
            "contract_hash": contract_hash,
            "training_start": actual_start,
            "training_end": actual_end,
        },
        "gaps": {
            "min_samples": min_samples,
            "enough_forecasts": enough_forecasts,
            "enough_prices": enough_prices,
            "price_conflicts": price_conflicts,
            "skipped_runs": skipped,
        },
        "next_step": if status == "ready_for_build" {
            "akzio calibration build --input <dataset> --output <frozen-policy>"
        } else {
            "collect additional canonical Paper outcomes; no policy was built"
        },
    });
    fs::write(&report_path, serde_json::to_vec_pretty(&report)?).with_context(|| {
        format!("write calibration quality report {}", report_path.display())
    })?;
    print_json(&report)
}

fn path_is_within(path: &Path, parent: &Path) -> bool {
    let absolute = |value: &Path| {
        if value.is_absolute() {
            value.to_path_buf()
        } else {
            std::env::current_dir()
                .map(|current| current.join(value))
                .unwrap_or_else(|_| value.to_path_buf())
        }
    };
    absolute(path).starts_with(absolute(parent))
}

fn parse_calibration_time(value: &str, field: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .with_context(|| format!("{field} must be RFC3339"))
}

fn session_close_time(date: NaiveDate) -> DateTime<Utc> {
    date.and_hms_opt(23, 59, 59)
        .expect("23:59:59 is a valid time")
        .and_utc()
}

fn relative_return_ppm(base: MoneyMicros, future: MoneyMicros) -> Result<i64> {
    if base.0 <= 0 || future.0 <= 0 {
        bail!("calibration label requires positive baseline and future prices");
    }
    i64::try_from(
        (i128::from(future.0) - i128::from(base.0))
            .saturating_mul(1_000_000)
            .checked_div(i128::from(base.0))
            .context("calibration return arithmetic overflow")?,
    )
    .context("calibration return does not fit i64")
}

struct SynthesizerIdentity {
    provider_id: String,
    model_id: String,
    contract_hash: ContentHash,
}

fn synthesizer_identity_for_decision(
    store: &Store,
    decision_artifact: &akzio_domain::Artifact,
    decision: &Decision,
    run_id: &RunId,
) -> Result<Option<SynthesizerIdentity>> {
    let mut queue = vec![ArtifactRef {
        artifact_id: decision_artifact.artifact_id.clone(),
        kind: ArtifactKind::Decision,
    }];
    let mut seen = BTreeSet::new();
    let mut found = None;
    let mut found_at = None;
    while let Some(reference) = queue.pop() {
        if !seen.insert(reference.artifact_id.clone()) || seen.len() > 256 {
            continue;
        }
        let Ok(artifact) = store.artifact(&reference.artifact_id) else {
            continue;
        };
        if artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.as_ref())
            != Some(run_id)
            || artifact.created_at > decision.created_at
        {
            continue;
        }
        if artifact.kind == ArtifactKind::AgentTurn {
            let payload: serde_json::Value = serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
            let request = payload.get("request").or_else(|| payload.get("domain_request"));
            if request
                .and_then(|request| request.get("purpose"))
                .and_then(serde_json::Value::as_str)
                == Some("research.synthesizer")
            {
                let capability = payload.get("capability_snapshot");
                let provider_id = capability
                    .and_then(|value| value.get("provider_id"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);
                let model_id = payload
                    .pointer("/response/telemetry/actual_model")
                    .and_then(serde_json::Value::as_str)
                    .or_else(|| {
                        capability
                            .and_then(|value| value.get("model_id"))
                            .and_then(serde_json::Value::as_str)
                    })
                    .map(str::to_owned);
                let contract_hash = payload
                    .get("contract_hash")
                    .or_else(|| request.and_then(|request| request.get("contract_hash")))
                    .cloned()
                    .and_then(|value| serde_json::from_value::<ContentHash>(value).ok());
                if let (Some(provider_id), Some(model_id), Some(contract_hash)) =
                    (provider_id, model_id, contract_hash)
                {
                    if found_at.is_none_or(|current| artifact.created_at > current) {
                        found = Some(SynthesizerIdentity {
                            provider_id,
                            model_id,
                            contract_hash,
                        });
                        found_at = Some(artifact.created_at);
                    }
                }
            }
        }
        queue.extend(artifact.source_refs);
    }
    Ok(found)
}

fn outcome_bars(
    store: &Store,
    outcome: &Outcome,
) -> Result<BTreeMap<Asset, BTreeMap<NaiveDate, MoneyMicros>>> {
    let mut bars_by_asset = BTreeMap::new();
    for reference in &outcome.market_evidence {
        if reference.kind != ArtifactKind::NormalizedEvidence {
            continue;
        }
        let artifact = store.artifact(&reference.artifact_id)?;
        let payload: NormalizedEvidencePayload =
            serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
        if payload.source != EvidenceSource::Alpaca || !payload.resource.starts_with("bars:") {
            continue;
        }
        let asset = payload
            .resource
            .split(':')
            .nth(1)
            .and_then(|value| Asset::try_from(value).ok())
            .context("outcome bar resource has no executable asset")?;
        let bars = parse_daily_bars(&payload.value, payload.observed_at)
            .with_context(|| format!("parse outcome bars for {asset}"))?;
        let destination = bars_by_asset.entry(asset).or_insert_with(BTreeMap::new);
        for (date, price) in bars {
            if let Some(existing) = destination.insert(date, price) {
                if existing != price {
                    bail!("outcome bars contain conflicting {} price on {}", asset, date);
                }
            }
        }
    }
    if bars_by_asset.len() != Asset::EXECUTABLE.len() {
        bail!("outcome market evidence lacks all four executable asset bar series");
    }
    Ok(bars_by_asset)
}

fn candidate_has_all_labels(
    decision: &Decision,
    schedule: &OutcomeSchedule,
    outcome: &Outcome,
    bars: &BTreeMap<Asset, BTreeMap<NaiveDate, MoneyMicros>>,
) -> std::result::Result<(), String> {
    for forecast in &decision.forecasts {
        let horizon = match forecast.horizon {
            DecisionHorizon::T1 => OutcomeHorizon::T1,
            DecisionHorizon::T3 => OutcomeHorizon::T3,
            DecisionHorizon::T5 => OutcomeHorizon::T5,
        };
        let Some(window) = outcome.windows.iter().find(|window| window.horizon == horizon) else {
            return Err(format!("missing {:?} Outcome window", horizon));
        };
        if session_close_time(window.observed_trading_day) <= decision.created_at {
            return Err(format!(
                "{:?} realized session is not after the frozen decision cutoff",
                horizon
            ));
        }
        let Some(asset_bars) = bars.get(&forecast.asset) else {
            return Err(format!("missing {} bar series", forecast.asset));
        };
        if !asset_bars.contains_key(&schedule.baseline_trading_day)
            || !asset_bars.contains_key(&window.observed_trading_day)
        {
            return Err(format!(
                "missing {} baseline or {:?} realized session bar",
                forecast.asset, horizon
            ));
        }
    }
    Ok(())
}

async fn run_evidence_command(command: &EvidenceCommand, config: &Config) -> Result<()> {
    match command {
        EvidenceCommand::Preflight { resource } => {
            let model_config = config
                .model
                .as_ref()
                .context("NewsWeb preflight requires [model] configuration")?;
            let route = if model_config.routes.contains_key("evidence.news_web") { "evidence.news_web" } else { "default" };
            let adapter = akzio_ingest::configured_news_evidence_transport(model_config)
                .context("construct governed NewsWeb router")?;
            let request = EvidenceRequest {
                source: EvidenceSource::NewsWeb,
                resource: resource.clone(),
                max_age: ChronoDuration::days(7),
                acquisition_mode: akzio_domain::evidence_acquisition_mode(RunPurpose::PositionPlan, &akzio_domain::EvidenceNeed {
                    schema_version: akzio_domain::DOMAIN_SCHEMA_VERSION,
                    source_family: "news_web".into(), resource: resource.clone(), max_age_secs: 604800,
                }),
            };
            let acquired = match adapter.acquire(&request).await {
                Ok(acquired) => acquired,
                Err(error) => {
                    let (diagnostic, detail) = evidence_preflight_error(&error);
                    return print_json(&serde_json::json!({
                        "status": "BLOCKED",
                        "source_family": "news_web",
                        "resource": resource,
                        "route": route,
                        "diagnostic": diagnostic,
                        "detail": detail,
                        "artifact_id": serde_json::Value::Null,
                        "artifact_note": "CLI preflight is read-only and does not write a Store artifact",
                    }));
                }
            };
            let raw_hash = akzio_domain::ContentHash::of_bytes(&acquired.raw);
            let normalized_bytes = serde_json::to_vec(&acquired.normalized)
                .context("encode normalized NewsWeb preflight payload")?;
            let normalized_hash = akzio_domain::ContentHash::of_bytes(&normalized_bytes);
            let asset = Asset::EXECUTABLE
                .into_iter()
                .find(|asset| resource.contains(asset.symbol()));
            let usable_at = acquired
                .provenance
                .published_at
                .unwrap_or(acquired.observed_at);
            let status = acquired.quality.citations_complete;
            print_json(&serde_json::json!({
                "status": if status { "available" } else { "BLOCKED" },
                "source_family": "news_web",
                "resource": resource,
                "asset": asset,
                "route": route,
                "evidence_domain": if resource.starts_with("news:") { Some("news_event") } else { None },
                "source_review": acquired.normalized.get("source_review"),
                "reviewed_facts": acquired.normalized.get("reviewed_facts"),
                "execution_authorization": "not_granted",
                "published_at": acquired.provenance.published_at,
                "retrieved_at": acquired.observed_at,
                "usable_at": usable_at,
                "source_uri": acquired.source_uri,
                "citations": acquired.provenance.citations,
                "quality": acquired.quality,
                "raw_hash": raw_hash,
                "normalized_hash": normalized_hash,
                "diagnostic": if status { "none" } else { "source_verification_incomplete" },
                "artifact_id": serde_json::Value::Null,
                "artifact_note": "CLI preflight is read-only and does not write a Store artifact",
            }))
        }
    }
}

fn evidence_preflight_error(error: &EvidenceAdapterError) -> (&'static str, &'static str) {
    match error {
        EvidenceAdapterError::NativeWeb { kind, .. } => (kind.as_str(), "native web contract failure"),
        EvidenceAdapterError::Unauthorized(_) => ("authorization", "provider authorization or entitlement denied"),
        EvidenceAdapterError::RateLimited { .. } => ("rate_limited", "provider rate limited the request"),
        EvidenceAdapterError::Pending(_) => ("pending", "provider data is pending"),
        EvidenceAdapterError::NotConfigured(_) => (
            "adapter_unavailable",
            "no authorized provider is configured",
        ),
        EvidenceAdapterError::Permanent(_) => ("permanent_provider_error", "provider rejected the request"),
        EvidenceAdapterError::Transport(_) => ("transport", "provider route or network transport failed"),
        EvidenceAdapterError::Policy { .. } => ("policy_rejected", "Rust evidence policy rejected the response"),
        EvidenceAdapterError::DataQuality(_) => ("data_quality", "provider payload failed data quality checks"),
        EvidenceAdapterError::MissingFixture(_) => ("fixture_missing", "fixture evidence is unavailable"),
        EvidenceAdapterError::SourceMismatch => ("source_mismatch", "adapter source does not match the request"),
    }
}
