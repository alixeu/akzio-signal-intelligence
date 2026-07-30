use anyhow::{bail, Context, Result};
use chrono::{Local, NaiveDate, Utc};
use orchestrator_core::{
    config_get, config_int, config_str, config_strings, display_ticker, load_config, parse_tickers,
    project_path, BenchmarkBindingV1, BenchmarkSelectionV1, DecisionSection,
    DecisionSectionUnavailableReason, DecisionSnapshotV2, EvaluationSpec, MemoryPolicyV1,
    MemoryUsageReferenceStatus, PersistenceContextV1, PersistenceNamespace, PolicyRef,
    ReflectionTaskStatus, RunPurpose, DECISION_SNAPSHOT_SCHEMA_VERSION,
};
use orchestrator_ingest::{jin10, technical};
use orchestrator_store::{
    append_index_detail, content_hash, create_index, finalize_index, read_indexes,
    read_run_manifest, write_run_manifest, AppendIndexDetailInput, CreateIndexInput, DetailSection,
    EvaluationStore, FileStore, FileStoreOptions, IndexKind, IndexQuery, IndexScope, ManifestError,
    RunCompactionMode, RunLocation, RunManifest, RunManifestInit, RunStatus, RunStore,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::Duration,
};

use crate::evaluation::{materialize_pending, MarketInputConfigV1, MaterializerPolicyV1};
use crate::orchestration::{
    allocation::{
        compute_allocation_context, derive_guarded_allocation, market_snapshot_from_technical,
    },
    config::RuntimeConfig,
    input_snapshot_runtime::{capture_phase1_file_store_inputs, phase1_input_sources},
    lifecycle::{
        investable_assets_from_state, run_id_for, run_id_for_seed, set_phase_status,
        tickers_from_state, validate_asset_scope,
    },
    role_jobs::{
        commit_historical_reflection, prepare_role_job, record_role_job_metrics, run_role_jobs,
        RoleRun,
    },
    summary_store::{
        parse_phase_index_candidate, write_compiled_phase_index, PhaseIndexCandidate,
        PhaseIndexCandidateDetail,
    },
    summary_units::derive_summary_index_id,
};

mod args;
pub use args::*;

const STATE_SCHEMA_VERSION: u32 = 1;

pub async fn run(args: ExecArgs) -> Result<Value> {
    validate_args(&args)?;
    let current_date = args
        .date
        .clone()
        .unwrap_or_else(|| Local::now().date_naive().to_string());
    NaiveDate::parse_from_str(&current_date, "%Y-%m-%d")
        .with_context(|| format!("invalid --date value {current_date:?}"))?;
    let config_path = args
        .config
        .clone()
        .unwrap_or_else(|| project_path("config/config.yaml"));
    let config = if args.config.is_some() {
        load_config(Some(&config_path))
            .with_context(|| format!("failed to load config from {}", config_path.display()))?
    } else {
        load_config(Some(&config_path)).unwrap_or_else(|_| json!({}))
    };
    let tickers =
        parse_tickers(config_strings(&config, "orchestrator.analysis_universe", &[]).join(","));
    if tickers.is_empty() {
        bail!("orchestrator.analysis_universe is required in config")
    }
    let runtime = RuntimeConfig::from_value(&config)?;
    validate_asset_scope(
        &tickers,
        &runtime.allocation.investable_assets,
        &runtime.allocation.regime_signal,
    )?;
    let store_root = runtime.store.resolve_root(args.store_root.as_deref())?;
    let canonical_run_id = run_id_for(&tickers, &current_date);
    // Paper/Live retain their stable identity so a configuration drift cannot
    // accidentally resume or overwrite an investment run. Mock and Debug are
    // diagnostic namespaces: include the config fingerprint in their run ID
    // so an old local fixture never blocks an isolated verification run.
    let run_id = if args.mock || args.debug {
        let mode = if args.mock { "mock" } else { "debug" };
        let config_hash = content_hash(&config)?;
        run_id_for_seed(&tickers, &current_date, &format!("{mode}\x1f{config_hash}"))
    } else {
        canonical_run_id
    };
    let location = RunLocation::new(current_date.clone(), run_id.clone())?;
    let store = FileStore::open(
        &store_root,
        FileStoreOptions {
            atomic_fsync: runtime.store.atomic_fsync,
            stale_temp_age: Some(Duration::from_secs(runtime.store.stale_temp_age_sec)),
        },
    )?;
    let run_store = RunStore::new(store.clone(), location.clone());
    let mut manifest = prepare_manifest(&store, &location, &runtime, &config)?;

    let initial_state = json!({
        "schema_version": STATE_SCHEMA_VERSION,
        "run_id": run_id,
        "current_date": current_date,
        "ticker": display_ticker(&tickers),
        "tickers": tickers,
        "analysis_universe": tickers,
        "investable_assets": runtime.allocation.investable_assets,
        "store_root": store.root(),
        "config": config,
        "mode": args.mode.as_str(),
        "lang": if args.lang == "zh" { config_str(&config, "orchestrator.runtime.lang", "zh") } else { args.lang.clone() },
        "window_days": args.window_days.unwrap_or_else(|| config_int(&config, "orchestrator.runtime.window_days", 150)),
        "mock": args.mock,
        "debug": args.debug,
        "phase_status": {},
        "degraded": false,
    });
    let mut state = load_or_initialize_state(&store, &location, initial_state)?;
    state["max_debate_rounds"] = json!(args.max_debate_rounds.unwrap_or_else(|| config_int(
        &config,
        "orchestrator.runtime.max_debate_rounds",
        3
    )));

    // Historical evaluation is strictly non-blocking for the current market
    // workflow. Ordinary gaps become ledger records; an integrity failure is
    // retained in run state and the investment phases still proceed.
    if runtime.evaluation.enabled && !args.mock && !args.debug {
        let context = evaluation_persistence_context(&runtime, &config, &args, &location)?;
        if matches!(context.namespace, PersistenceNamespace::Canonical)
            && context.canonical_memory_writes_enabled
        {
            let evaluation = EvaluationStore::open(store.clone(), context.clone())?;
            let policy = MaterializerPolicyV1 {
                materialization_policy_ref: context.config_ref,
            };
            let market = MarketInputConfigV1 {
                interval: "daily".to_owned(),
                provider: runtime.evaluation.market_data_provider.clone(),
                price_basis: runtime.evaluation.price_basis,
                adjustment_policy: runtime.evaluation.market_data_adjustment_policy,
                corporate_action_capability: runtime.evaluation.corporate_action_capability.clone(),
            };
            match materialize_pending(&store, &evaluation, &location, &policy, &market) {
                Ok(report) => state["evaluation_materialization"] = serde_json::to_value(report)?,
                Err(error) => record_nonblocking_evaluation_failure(&mut state, &error),
            }
        }
    }

    if args.from_phase <= 0 && args.to_phase >= 0 && !phase_completed(&manifest, 0) {
        run_phase0(
            &store,
            &location,
            &mut state,
            &runtime,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        finish_phase(&store, &location, &mut manifest, &mut state, 0, "done")?;
    }
    let phase1_needs_run = args.from_phase <= 1
        && args.to_phase >= 1
        && (!phase_completed(&manifest, 1)
            || !has_required_phase_summaries(&store, &location, &state, &runtime, 1)?);
    if phase1_needs_run {
        if !args.mock {
            refresh_market_inputs_if_needed(&args, &mut state, &tickers).await?;
        }
        run_phase1(
            &mut state,
            &runtime,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        let summary_units = summarize(
            &store_root,
            &mut state,
            &runtime,
            1,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        manifest.summary_units.extend(summary_units);
        finish_phase(&store, &location, &mut manifest, &mut state, 1, "done")?;
    }
    if args.from_phase <= 2
        && args.to_phase >= 2
        && (!phase_completed(&manifest, 2)
            || !has_required_phase_summaries(&store, &location, &state, &runtime, 2)?)
    {
        run_phase2(
            &store,
            &location,
            &mut state,
            &runtime,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        let summary_units = summarize(
            &store_root,
            &mut state,
            &runtime,
            2,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        manifest.summary_units.extend(summary_units);
        finish_phase(&store, &location, &mut manifest, &mut state, 2, "done")?;
    }
    if args.from_phase <= 3
        && args.to_phase >= 3
        && (!phase_completed(&manifest, 3)
            || !has_required_phase_summaries(&store, &location, &state, &runtime, 3)?)
    {
        run_phase3(
            &mut state,
            &runtime,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        let summary_units = summarize(
            &store_root,
            &mut state,
            &runtime,
            3,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        manifest.summary_units.extend(summary_units);
        finish_phase(&store, &location, &mut manifest, &mut state, 3, "done")?;
    }
    if args.from_phase <= 4
        && args.to_phase >= 4
        && (!phase_completed(&manifest, 4)
            || !has_required_phase_summaries(&store, &location, &state, &runtime, 4)?)
    {
        run_phase4(
            &mut state,
            &runtime,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        let summary_units = summarize(
            &store_root,
            &mut state,
            &runtime,
            4,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        manifest.summary_units.extend(summary_units);
        finish_phase(&store, &location, &mut manifest, &mut state, 4, "done")?;
    }
    if args.from_phase <= 5
        && args.to_phase >= 5
        && (!phase_completed(&manifest, 5)
            || !has_required_phase_summaries(&store, &location, &state, &runtime, 5)?)
    {
        run_phase5(
            &mut state,
            &runtime,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        let summary_units = summarize(
            &store_root,
            &mut state,
            &runtime,
            5,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        manifest.summary_units.extend(summary_units);
        finish_phase(&store, &location, &mut manifest, &mut state, 5, "done")?;
    }
    if args.from_phase <= 6
        && args.to_phase >= 6
        && (!phase_completed(&manifest, 6)
            || !has_required_phase_summaries(&store, &location, &state, &runtime, 6)?)
    {
        run_phase6(
            &mut state,
            &runtime,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        let summary_units = summarize(
            &store_root,
            &mut state,
            &runtime,
            6,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        manifest.summary_units.extend(summary_units);
        finish_phase(&store, &location, &mut manifest, &mut state, 6, "done")?;
    }
    if args.from_phase <= 7
        && args.to_phase >= 7
        && (!phase_completed(&manifest, 7)
            || !has_required_phase_summaries(&store, &location, &state, &runtime, 7)?)
    {
        run_phase7(&store, &location, &mut state, &runtime)?;
        let summary_units = summarize(
            &store_root,
            &mut state,
            &runtime,
            7,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        manifest.summary_units.extend(summary_units);
        finish_phase(&store, &location, &mut manifest, &mut state, 7, "done")?;
    }
    if args.from_phase <= 8 && args.to_phase >= 8 && !phase_completed(&manifest, 8) {
        manifest.summary_units.extend(run_phase8(
            &store, &location, &mut state, &runtime, &config, &args,
        )?);
        finish_phase(&store, &location, &mut manifest, &mut state, 8, "done")?;
    }

    // Completed Indexes are the run's semantic authority. The manifest only
    // carries lifecycle and Index references.
    manifest.artifacts.clear();
    manifest.status = RunStatus::Completed;
    sync_manifest_health(&mut manifest, &state);
    if let Some(phase) = highest_completed_phase(&manifest) {
        manifest.current_phase = phase;
    }
    manifest.completed_at = Some(Utc::now().to_rfc3339());
    write_run_manifest(&store, &location, manifest)?;
    seal_state(&mut state)?;
    store.write_json_value(&location.child_relative(Path::new("state.json"))?, &state)?;
    let store_compaction = match run_store.compact_completed_run(RunCompactionMode::Apply) {
        Ok(report) => serde_json::to_value(report)?,
        Err(error) => {
            tracing::warn!(error = %error, "completed-run store compaction failed");
            json!({
                "eligible": true,
                "applied": false,
                "failure": error.to_string(),
            })
        }
    };

    Ok(json!({
        "run_id": state["run_id"],
        "date": state["current_date"],
        "store_root": store.root(),
        "debate_mode": "file_store",
        "degraded": state["degraded"],
        "rating": investable_assets_from_state(&state)
            .first()
            .and_then(|ticker| state.pointer(&format!("/research_plan/per_ticker/{ticker}/rating")))
            .cloned()
            .unwrap_or(Value::Null),
        "action": investable_assets_from_state(&state)
            .first()
            .and_then(|ticker| state.pointer(&format!("/trader_investment_plan/per_ticker/{ticker}/action")))
            .cloned()
            .unwrap_or(Value::Null),
        "portfolio_allocation": state.get("portfolio_allocation").cloned().unwrap_or(Value::Null),
        "store_compaction": store_compaction,
        "run_state": state,
    }))
}

async fn refresh_market_inputs_if_needed(
    args: &ExecArgs,
    state: &mut Value,
    tickers: &[String],
) -> Result<()> {
    if args.mock || !args.tech_refresh_enabled {
        return Ok(());
    }
    state["input_refresh"] = json!({"status":"started"});
    technical::run(technical::TechnicalArgs {
        source: None,
        symbols: Some(tickers.join(",")),
        start: None,
        end: None,
        days: None,
        intervals: String::new(),
        timeout: None,
        sleep: None,
        parallelism: None,
    })
    .await
    .context("phase 1 input refresh failed for technical source")?;

    jin10::run(jin10::Jin10Args {
        channel: None,
        vip: None,
        classify: None,
        lookback_hours: Some(args.jin10_refresh_lookback_hours),
        pages: None,
        sleep: None,
        timeout: None,
        output: String::new(),
        jsonl: String::new(),
        pretty: false,
    })
    .await
    .context("phase 1 input refresh failed for jin10 source")?;

    state["input_refresh"] = json!({"status": "completed"});
    Ok(())
}

fn record_nonblocking_evaluation_failure(state: &mut Value, error: &anyhow::Error) {
    if !state["errors"].is_array() {
        state["errors"] = json!([]);
    }
    state["errors"]
        .as_array_mut()
        .expect("errors set to array")
        .push(json!({
            "phase": "evaluation",
            "kind": "non_blocking_materialization_failure",
            "failure": error.to_string(),
        }));
    state["evaluation_materialization"] = json!({
        "status": "failed_non_blocking",
        "failure": error.to_string(),
    });
}

fn validate_args(args: &ExecArgs) -> Result<()> {
    if args.from_phase > args.to_phase || args.to_phase > 8 {
        bail!("phase range must satisfy 0 <= from_phase <= to_phase <= 8")
    }
    if args
        .max_debate_rounds
        .is_some_and(|rounds| !(0..=10).contains(&rounds))
    {
        bail!("max_debate_rounds must be in 0..=10")
    }
    Ok(())
}

fn prepare_manifest(
    store: &FileStore,
    location: &RunLocation,
    runtime: &RuntimeConfig,
    config: &Value,
) -> Result<RunManifest> {
    if store.exists(&location.manifest_relative())? {
        let manifest = read_run_manifest(store, location)?;
        let current_config_hash = content_hash(config)?;
        if manifest.config_hash != current_config_hash {
            bail!(
                "run {} was created with config hash {}; current config hash is {}; start a new run instead of silently reusing artifacts",
                manifest.run_id,
                manifest.config_hash,
                current_config_hash
            );
        }
        return Ok(manifest);
    }
    let snapshot = runtime.role_profile_registry.snapshot();
    write_run_manifest(
        store,
        location,
        RunManifest::new(RunManifestInit {
            location: location.clone(),
            workflow_version: format!("orchestrator-workflow-v{}", env!("CARGO_PKG_VERSION")),
            prompt_versions: runtime.prompts.versions.clone(),
            git_sha: option_env!("GIT_SHA").unwrap_or("unavailable").to_owned(),
            config_hash: content_hash(config)?,
            role_profile_registry_hash: snapshot.content_hash,
            created_at: Utc::now().to_rfc3339(),
        })?,
    )
    .map_err(Into::into)
}

fn load_or_initialize_state(
    store: &FileStore,
    location: &RunLocation,
    initial_state: Value,
) -> Result<Value> {
    let relative = location.child_relative(Path::new("state.json"))?;
    if !store.exists(&relative)? {
        return Ok(initial_state);
    }
    let mut state = store.read_json_value(&relative)?;
    let version = state
        .get("schema_version")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .context("run state schema_version is required")?;
    if version > STATE_SCHEMA_VERSION {
        bail!("run state schema version {version} is newer than supported {STATE_SCHEMA_VERSION}");
    }
    if version < STATE_SCHEMA_VERSION {
        bail!(
            "run state schema version {version} requires an explicit migration to {STATE_SCHEMA_VERSION}"
        );
    }
    let stored_hash = state
        .get("content_hash")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .context("run state content_hash is required")?
        .to_owned();
    state["content_hash"] = Value::String(String::new());
    let expected_hash = content_hash(&state)?;
    if stored_hash != expected_hash {
        bail!(
            "run state content_hash mismatch at {}: expected {expected_hash}, found {stored_hash}",
            store.root().join(relative).display()
        );
    }
    state["content_hash"] = Value::String(stored_hash);
    for field in ["run_id", "current_date", "ticker", "tickers", "config"] {
        if state.get(field) != initial_state.get(field) {
            bail!("existing run state field {field} differs from requested run; start a new run");
        }
    }
    Ok(state)
}

fn finish_phase(
    store: &FileStore,
    location: &RunLocation,
    manifest: &mut RunManifest,
    state: &mut Value,
    phase: u8,
    status: &str,
) -> Result<()> {
    set_phase_status(state, i64::from(phase), status);
    manifest.current_phase = phase;
    sync_manifest_health(manifest, state);
    manifest.phase_status.insert(
        phase.to_string(),
        if status == "done" {
            orchestrator_store::PhaseStatus::Completed
        } else {
            orchestrator_store::PhaseStatus::Failed
        },
    );
    write_run_manifest(store, location, manifest.clone())?;
    Ok(())
}

fn sync_manifest_health(manifest: &mut RunManifest, state: &Value) {
    manifest.degraded = state["degraded"].as_bool().unwrap_or(false);
    manifest.errors = state["errors"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|entry| ManifestError {
            phase: entry
                .get("phase")
                .and_then(Value::as_u64)
                .and_then(|value| u8::try_from(value).ok()),
            code: entry
                .get("kind")
                .and_then(Value::as_str)
                .unwrap_or("workflow_error")
                .to_owned(),
            message: entry.to_string(),
            created_at: Utc::now().to_rfc3339(),
        })
        .collect();
}

fn highest_completed_phase(manifest: &RunManifest) -> Option<u8> {
    manifest
        .phase_status
        .iter()
        .filter(|(_, status)| {
            matches!(
                status,
                orchestrator_store::PhaseStatus::Completed
                    | orchestrator_store::PhaseStatus::Degraded
            )
        })
        .filter_map(|(phase, _)| phase.parse::<u8>().ok())
        .max()
}

fn phase_completed(manifest: &RunManifest, phase: u8) -> bool {
    manifest.phase_status.get(&phase.to_string())
        == Some(&orchestrator_store::PhaseStatus::Completed)
}

fn has_required_phase_summaries(
    store: &FileStore,
    location: &RunLocation,
    state: &Value,
    runtime: &RuntimeConfig,
    phase: u8,
) -> Result<bool> {
    let completed = read_indexes(
        store,
        Some(location),
        &IndexQuery {
            kind: Some(IndexKind::PhaseSummary),
            source_phase: Some(phase),
            limit: runtime.tool_managed.max_summary_units_per_phase,
            ..Default::default()
        },
    )?
    .indexes;
    Ok(completed.len() >= required_phase_index_count(state, runtime, phase))
}

async fn summarize(
    store_root: &Path,
    state: &mut Value,
    runtime: &RuntimeConfig,
    phase: i64,
    _model: Option<&str>,
    _reasoning: Option<&str>,
) -> Result<BTreeMap<String, String>> {
    let store = FileStore::open(store_root, FileStoreOptions::default())?;
    let location = RunLocation::new(
        state["current_date"]
            .as_str()
            .context("summary recovery requires current_date")?,
        state["run_id"]
            .as_str()
            .context("summary recovery requires run_id")?,
    )?;
    let completed_ids = read_indexes(
        &store,
        Some(&location),
        &IndexQuery {
            kind: Some(IndexKind::PhaseSummary),
            source_phase: Some(u8::try_from(phase).context("summary phase must fit u8")?),
            limit: runtime.tool_managed.max_summary_units_per_phase,
            ..Default::default()
        },
    )?
    .indexes;
    let phase = u8::try_from(phase).context("summary phase must fit u8")?;
    let required = required_phase_index_count(state, runtime, phase);
    if completed_ids.len() < required {
        bail!(
            "Phase {phase} produced {} completed Indexes; expected at least {required}",
            completed_ids.len()
        )
    }
    Ok(completed_ids
        .into_iter()
        .map(|index| {
            let unit_key = index
                .authoritative_fields
                .get("unit_key")
                .and_then(Value::as_str)
                .unwrap_or(&index.index_id)
                .to_owned();
            (unit_key, index.index_id)
        })
        .collect())
}

fn required_phase_index_count(state: &Value, _runtime: &RuntimeConfig, phase: u8) -> usize {
    match phase {
        1 => 2,
        2 => {
            let topics = state
                .pointer("/topic_generation_artifact/topics")
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            // Warm-up, topic generation, and final reduction are always
            // present. Each topic always has two seeds and one controller
            // decision; later interaction/controller rounds are optional and
            // stop as soon as the controller says the debate is complete.
            3 + topics * 3
        }
        3 | 4 | 6 => 1,
        5 => 3,
        7 | 8 => 1,
        _ => 0,
    }
}

async fn run_phase0(
    store: &FileStore,
    location: &RunLocation,
    state: &mut Value,
    runtime: &RuntimeConfig,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<()> {
    let tasks = reflection_tasks(store, location, state, &runtime.reflection)?;
    let planned_tasks = tasks.clone();
    let mut completed = Vec::new();
    let task_ledger = orchestrator_store::ReflectionTaskLedger::new(store.clone());
    for task in tasks {
        if state["mock"].as_bool().unwrap_or(false) {
            completed.push(json!({
                "status": "skipped_mock",
                "ticker": task.get("ticker"),
                "source_run_id": task.get("source_run_id"),
            }));
            continue;
        }
        state["reflection_task"] = task.clone();
        state["phase0"]["tasks"] = json!([task]);
        let task_id = task
            .get("task_id")
            .and_then(Value::as_str)
            .context("reflection scheduler returned a task without task_id")?
            .to_owned();
        let artifact = run_unit(
            state,
            runtime,
            "reflector.historical",
            0,
            "historical_reflection",
            None,
            Some(&task_id),
            task.get("ticker").and_then(Value::as_str),
            model,
            reasoning,
        )
        .await;
        // The dedicated terminal has already atomically written the immutable
        // HistoricalReflectionArtifact and task receipt. Do not mirror it to
        // a second run-local record: that would create two authorities.
        match artifact {
            Ok(artifact) => completed.push(artifact),
            Err(error) => {
                let detail = error.to_string();
                match task_ledger.mark_failed(
                    &task_id,
                    &location.run_id,
                    detail.clone(),
                    runtime.reflection.max_attempts,
                    &Utc::now().to_rfc3339(),
                ) {
                    Ok(task) => completed.push(json!({
                        "task_id": task.task_id,
                        "status": task.status,
                        "error": detail,
                    })),
                    Err(ledger_error) => {
                        // The investment workflow remains non-blocking even
                        // if persistence of diagnostic state itself fails;
                        // retain both failures in its run state for Doctor.
                        state["degraded"] = Value::Bool(true);
                        if let Some(errors) = state["errors"].as_array_mut() {
                            errors.push(json!({
                                "phase": 0,
                                "role": "reflector.historical",
                                "task_id": task_id,
                                "error": detail,
                                "ledger_error": ledger_error.to_string(),
                            }));
                        }
                        completed.push(json!({
                            "task_id": task_id,
                            "status": "failure_unrecorded",
                            "error": detail,
                            "ledger_error": ledger_error.to_string(),
                        }));
                    }
                }
            }
        }
    }
    state["phase0"] = json!({
        "status": "completed",
        "tasks": planned_tasks,
        "reflections": completed,
    });
    state
        .as_object_mut()
        .map(|object| object.remove("reflection_task"));
    Ok(())
}

fn reflection_tasks(
    store: &FileStore,
    current: &RunLocation,
    state: &Value,
    config: &crate::orchestration::config::ReflectionConfig,
) -> Result<Vec<Value>> {
    canonical_reflection_tasks(store, current, state, config)
}

fn canonical_reflection_tasks(
    store: &FileStore,
    current: &RunLocation,
    state: &Value,
    config: &crate::orchestration::config::ReflectionConfig,
) -> Result<Vec<Value>> {
    let full_config = state.get("config").cloned().unwrap_or_else(|| json!({}));
    let evaluation_config = config_get(&full_config, "orchestrator.evaluation")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let evaluation_policy = PolicyRef {
        policy_id: "orchestrator.evaluation".to_owned(),
        version: evaluation_config
            .get("policy_version")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .unwrap_or(1),
        content_hash: content_hash(&evaluation_config)?,
    };
    let reflection_config = config_get(&full_config, "orchestrator.reflection")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let memory_policy = MemoryPolicyV1 {
        policy_ref: PolicyRef {
            policy_id: "memory.policy".to_owned(),
            version: config.policy_version,
            content_hash: content_hash(&reflection_config)?,
        },
        reflection_total_quota: config.task_limit as u32,
        reflection_new_outcome_quota: config.new_outcome_quota as u32,
        reflection_retry_quota: config.retry_quota as u32,
        reflection_backlog_quota: config.backlog_quota as u32,
        reflection_max_attempts: config.max_attempts,
    };
    if !memory_policy.is_valid() {
        bail!("configured MemoryPolicyV1 is invalid");
    }
    let context = PersistenceContextV1 {
        run_purpose: RunPurpose::Paper,
        namespace: PersistenceNamespace::Canonical,
        canonical_memory_writes_enabled: false,
        invocation_id: current.run_id.clone(),
        config_ref: evaluation_policy.clone(),
        source_store_fingerprint: evaluation_policy.content_hash.clone(),
    };
    let evaluation = EvaluationStore::open(store.clone(), context)?;
    let ledger = orchestrator_store::ReflectionTaskLedger::new(store.clone());
    let current_outcomes = evaluation.list_current_outcomes()?;
    let current_outcome_ids = current_outcomes
        .iter()
        .map(|outcome| outcome.outcome_id.clone())
        .collect::<BTreeSet<_>>();
    let now = Utc::now().to_rfc3339();
    ledger.supersede_non_current_outcomes(&current_outcome_ids, &current.run_id, &now)?;
    let mut render_by_task_id = BTreeMap::new();
    for outcome in current_outcomes {
        let decision: DecisionSnapshotV2 = store.read_versioned_json(
            Path::new(&outcome.decision_ref.relative_path),
            orchestrator_store::FileSchemaKind::DecisionSnapshot,
        )?;
        if decision.source_run_id == current.run_id {
            continue;
        }
        let Some(source_location) =
            orchestrator_store::find_run_location(store, &decision.source_run_id)?
        else {
            continue;
        };
        if read_indexes(
            store,
            Some(&source_location),
            &IndexQuery {
                kind: Some(IndexKind::PhaseSummary),
                ticker: Some(decision.ticker.clone()),
                limit: 1,
                ..Default::default()
            },
        )?
        .indexes
        .is_empty()
        {
            continue;
        }
        let key = orchestrator_core::ReflectionTaskKeyV1 {
            source_run_id: decision.source_run_id.clone(),
            ticker: decision.ticker.clone(),
            outcome_id: outcome.outcome_id.clone(),
            outcome_content_hash: outcome.content_hash.clone(),
            policy_ref: memory_policy.policy_ref.clone(),
            profile_version: 3,
            builder_version: 1,
        };
        let task = ledger.create_or_read(
            key,
            evaluation.outcome_reference(&outcome.outcome_id)?,
            &now,
        )?;
        render_by_task_id.insert(
            task.task_id.clone(),
            json!({
                "task_id": task.task_id,
                "ticker": decision.ticker,
                "source_run_id": decision.source_run_id,
                "outcome": outcome,
                "decision": decision,
            }),
        );
    }
    let mut retries = Vec::new();
    let mut fresh = Vec::new();
    let mut backlog = Vec::new();
    for task in ledger.list_tasks()? {
        let Some(rendered) = render_by_task_id.get(&task.task_id).cloned() else {
            continue;
        };
        match task.status {
            ReflectionTaskStatus::FailedRetryable => retries.push((task, rendered)),
            ReflectionTaskStatus::Pending if task.updated_at == now => fresh.push((task, rendered)),
            ReflectionTaskStatus::Pending => backlog.push((task, rendered)),
            _ => {}
        }
    }
    let mut selected =
        select_reflection_task_budget(&mut fresh, &mut retries, &mut backlog, &memory_policy);
    let mut tasks = Vec::new();
    for (task, rendered) in selected.drain(..) {
        if ledger
            .claim(&task.task_id, &current.run_id, &now)?
            .is_some()
        {
            tasks.push(rendered);
        }
    }
    Ok(tasks)
}

fn select_reflection_task_budget<T>(
    fresh: &mut Vec<T>,
    retries: &mut Vec<T>,
    backlog: &mut Vec<T>,
    policy: &MemoryPolicyV1,
) -> Vec<T> {
    let limit = policy.reflection_total_quota as usize;
    let mut selected = Vec::with_capacity(limit);
    let retry_count = (policy.reflection_retry_quota as usize)
        .min(limit.saturating_sub(selected.len()))
        .min(retries.len());
    selected.extend(retries.drain(..retry_count));
    let fresh_count = (policy.reflection_new_outcome_quota as usize)
        .min(limit.saturating_sub(selected.len()))
        .min(fresh.len());
    selected.extend(fresh.drain(..fresh_count));
    let backlog_count = (policy.reflection_backlog_quota as usize)
        .min(limit.saturating_sub(selected.len()))
        .min(backlog.len());
    selected.extend(backlog.drain(..backlog_count));
    // A quota reserves capacity for a class but does not waste the total
    // budget when that class is empty. Round-robin the remaining oldest tasks
    // across classes, so retries cannot indefinitely starve fresh Outcomes
    // and a sustained fresh stream cannot starve the backlog.
    while selected.len() < limit {
        let mut made_progress = false;
        for bucket in [&mut *retries, &mut *fresh, &mut *backlog] {
            if selected.len() == limit {
                break;
            }
            if !bucket.is_empty() {
                selected.push(bucket.remove(0));
                made_progress = true;
            }
        }
        if !made_progress {
            break;
        }
    }
    selected
}

async fn run_phase1(
    state: &mut Value,
    runtime: &RuntimeConfig,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<()> {
    if !state["mock"].as_bool().unwrap_or(false) {
        let tickers = tickers_from_state(state);
        let sources = phase1_input_sources(
            state["current_date"]
                .as_str()
                .context("missing current_date")?,
            true,
            true,
            &tickers,
        )?;
        let input = capture_phase1_file_store_inputs(state, runtime, &sources)?;
        let technical_snapshot = orchestrator_llm::tools::read_technical_snapshot::execute(
            json!({"tickers": tickers, "intervals": ["daily"]}),
            &orchestrator_llm::tools::ExternalToolConfig {
                file_store_input: Some(input.clone()),
                ..Default::default()
            },
        )?;
        state["market_snapshot"] =
            market_snapshot_from_technical(&technical_snapshot, &runtime.allocation)?;
        state["file_store_input"] = json!({
            "store_root": input.store_root,
            "run_id": input.run_id,
            "current_date": input.current_date,
        });
    }
    let roles = ["analyst.technical", "analyst.news_macro"];
    let mut reports = serde_json::Map::new();
    for role in roles {
        let artifact = run_unit(
            state, runtime, role, 1, "artifact", None, None, None, model, reasoning,
        )
        .await?;
        reports.insert(
            role.to_owned(),
            artifact.get("payload").cloned().unwrap_or(artifact),
        );
    }
    state["analyst_reports"] = Value::Object(reports);
    state["phase1_index"] = json!({"roles": state["analyst_reports"], "authority": "file_store"});
    state["weighted_probability_base"] = weighted_probability_base(state);
    Ok(())
}

async fn run_phase2(
    store: &FileStore,
    _location: &RunLocation,
    state: &mut Value,
    runtime: &RuntimeConfig,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<()> {
    let warmup = run_unit(
        state,
        runtime,
        "mediator.topic",
        2,
        "warmup",
        Some(0),
        None,
        None,
        model,
        reasoning,
    )
    .await?;
    state["phase2_warmup"] = warmup;
    // Preserve the completed Warmup artifact first, then attach its Rust-owned
    // session identity. Reversing this order silently erased the fork source
    // and let Bull/Bear seeds start without their required parent evidence.
    record_phase2_session(state, "mediator.topic", "warmup", None, None, Some(0));
    let generated = run_unit(
        state,
        runtime,
        "mediator.topic",
        2,
        "topic_generation",
        None,
        None,
        None,
        model,
        reasoning,
    )
    .await?;
    record_phase2_session(
        state,
        "mediator.topic",
        "topic_generation",
        None,
        None,
        None,
    );
    let topics = generated
        .pointer("/payload/topics")
        .or_else(|| generated.get("topics"))
        .cloned()
        .unwrap_or_else(|| json!([]));
    let topic_generation_session =
        runtime_session_for(state, "mediator.topic", "topic_generation", None, None);
    state["topic_generation_session_id"] = topic_generation_session["session_id"].clone();
    state["topic_generation_turn_id"] = topic_generation_session["turn_id"].clone();
    let actionable = topics.as_array().is_some_and(|items| !items.is_empty());
    state["topic_generation_artifact"] =
        json!({"artifact": generated, "topics": topics, "actionable": actionable});

    let mut controllers = serde_json::Map::new();
    for topic in topics.as_array().into_iter().flatten() {
        let topic_id = topic
            .get("topic_id")
            .and_then(Value::as_str)
            .context("Phase 2 topic generation returned a topic without topic_id")?
            .to_owned();
        state["topic_debate_states"][&topic_id] = json!({"topic": topic, "turns": []});
        for (role, kind, side) in [
            ("researcher.bull.initial", "bull_seed", "bull"),
            ("researcher.bear.initial", "bear_seed", "bear"),
        ] {
            let seed = run_unit(
                state,
                runtime,
                role,
                2,
                kind,
                Some(0),
                Some(&topic_id),
                None,
                model,
                reasoning,
            )
            .await?;
            record_phase2_session(state, role, kind, Some(&topic_id), Some(side), Some(0));
            state["topic_debate_states"][&topic_id]["turns"]
                .as_array_mut()
                .expect("topic turns initialized")
                .push(json!({"role":role, "artifact": seed}));
        }
        let mut controller = run_unit(
            state,
            runtime,
            "mediator.topic_controller",
            2,
            "topic_control",
            Some(0),
            Some(&topic_id),
            None,
            model,
            reasoning,
        )
        .await?;
        record_phase2_session(
            state,
            "mediator.topic_controller",
            "topic_control",
            Some(&topic_id),
            Some("controller"),
            Some(0),
        );
        state["topic_debate_states"][&topic_id]["controller_artifact"] = controller.clone();
        let max_rounds = state
            .get("max_debate_rounds")
            .and_then(Value::as_i64)
            .unwrap_or(1)
            .max(0);
        for round in 1..=max_rounds {
            if !controller_should_continue(&controller)? {
                break;
            }
            for (role, side) in [
                ("researcher.bull.interaction", "bull"),
                ("researcher.bear.interaction", "bear"),
            ] {
                let response = run_unit(
                    state,
                    runtime,
                    role,
                    2,
                    "interaction",
                    Some(round),
                    Some(&topic_id),
                    None,
                    model,
                    reasoning,
                )
                .await?;
                record_phase2_session(
                    state,
                    role,
                    "interaction",
                    Some(&topic_id),
                    Some(side),
                    Some(round),
                );
                state["topic_debate_states"][&topic_id]["turns"]
                    .as_array_mut()
                    .expect("topic turns initialized")
                    .push(json!({"role":role, "round":round, "artifact": response}));
            }
            controller = run_unit(
                state,
                runtime,
                "mediator.topic_controller",
                2,
                "topic_control",
                Some(round),
                Some(&topic_id),
                None,
                model,
                reasoning,
            )
            .await?;
            record_phase2_session(
                state,
                "mediator.topic_controller",
                "topic_control",
                Some(&topic_id),
                Some("controller"),
                Some(round),
            );
            state["topic_debate_states"][&topic_id]["controller_artifact"] = controller.clone();
        }
        controllers.insert(topic_id, controller);
    }
    let reducer_text = serde_json::to_string(&controllers)?;
    let reducer = write_compiled_phase_index(
        store.root(),
        state,
        2,
        "rust.phase2_final_reducer",
        "final_reducer",
        None,
        None,
        None,
        &reducer_text,
        PhaseIndexCandidate {
            summary: "Phase 2 议题辩论已完成归并。".to_owned(),
            confidence: 1.0,
            authoritative_fields: serde_json::from_value(json!({
                "controllers": controllers,
                "reducer": "deterministic",
            }))?,
            details: Vec::new(),
            missing_fields: Vec::new(),
            ambiguities: Vec::new(),
        },
    )?;
    let reducer = serde_json::to_value(&reducer)?;
    state["debate_state_artifact"] = json!({
        "status": "completed",
        "topic_briefs": state["topic_generation_artifact"]["topics"],
        "final_reducer": reducer,
        "authority": "file_store"
    });
    Ok(())
}

async fn run_phase3(
    state: &mut Value,
    runtime: &RuntimeConfig,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<()> {
    let artifact = run_unit(
        state,
        runtime,
        "manager.research",
        3,
        "artifact",
        None,
        None,
        None,
        model,
        reasoning,
    )
    .await?;
    let decisions = artifact
        .pointer("/payload/decisions")
        .cloned()
        .context("finalized research artifact is missing decisions")?;
    state["research_plan"] = json!({
        "per_ticker": decisions,
        "regime_context": artifact.pointer("/payload/regime_context").cloned().unwrap_or(Value::Null),
        "authority": "file_store"
    });
    Ok(())
}

fn controller_should_continue(controller: &Value) -> Result<bool> {
    let Some(should_continue) = controller
        .pointer("/payload/soft_control/should_continue")
        .or_else(|| controller.pointer("/soft_control/should_continue"))
        .and_then(Value::as_bool)
    else {
        tracing::warn!(
            "Topic Controller Summary omitted soft_control.should_continue; stopping debate safely"
        );
        return Ok(false);
    };
    if should_continue {
        let next_steers = controller
            .pointer("/payload/next_steers")
            .or_else(|| controller.pointer("/next_steers"))
            .and_then(Value::as_array)
            .context("continuing Topic Controller Summary requires next_steers")?;
        if next_steers.is_empty() {
            bail!("continuing Topic Controller Summary requires non-empty next_steers")
        }
    }
    Ok(should_continue)
}

async fn run_phase4(
    state: &mut Value,
    runtime: &RuntimeConfig,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<()> {
    let artifact = run_unit(
        state, runtime, "trader", 4, "artifact", None, None, None, model, reasoning,
    )
    .await?;
    let plans = artifact
        .pointer("/payload/plans")
        .cloned()
        .context("finalized trader artifact is missing plans")?;
    state["trader_investment_plan"] = json!({"per_ticker": plans, "authority": "file_store"});
    Ok(())
}

async fn run_phase5(
    state: &mut Value,
    runtime: &RuntimeConfig,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<()> {
    let mut history = Vec::new();
    for role in ["risk.aggressive", "risk.neutral", "risk.conservative"] {
        let artifact = run_unit(
            state, runtime, role, 5, "artifact", None, None, None, model, reasoning,
        )
        .await?;
        history.push(artifact);
    }
    state["risk_debate_state"] = json!({"history": history, "authority": "file_store"});
    Ok(())
}

async fn run_phase6(
    state: &mut Value,
    runtime: &RuntimeConfig,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<()> {
    let artifact = run_unit(
        state,
        runtime,
        "portfolio.manager",
        6,
        "artifact",
        None,
        None,
        None,
        model,
        reasoning,
    )
    .await?;
    let per_asset = artifact
        .pointer("/payload/per_asset")
        .cloned()
        .context("finalized portfolio artifact is missing per_asset")?;
    state["final_trade_decision"] = json!({"per_asset": per_asset, "authority": "file_store"});
    Ok(())
}

fn run_phase7(
    store: &FileStore,
    _location: &RunLocation,
    state: &mut Value,
    runtime: &RuntimeConfig,
) -> Result<Value> {
    let context = compute_allocation_context(state, &runtime.allocation)?;
    let allocation = derive_guarded_allocation(state, &context, &runtime.allocation)
        .unwrap_or_else(|error| {
            json!({
                "weights": {"cash_hedge": {"weight": 1.0, "rationale": error.to_string()}},
                "total_equity_exposure": 0.0,
                "allocation_method": "fallback_cash",
            })
        });
    state["allocation_context"] = context;
    state["portfolio_allocation"] = allocation.clone();
    let response_text = serde_json::to_string(&json!({
        "allocation_context": state["allocation_context"],
        "allocation": allocation,
    }))?;
    let artifact = write_compiled_phase_index(
        store.root(),
        state,
        7,
        "rust.allocation",
        "allocation",
        None,
        None,
        None,
        &response_text,
        PhaseIndexCandidate {
            summary: "Rust 已完成受约束的组合分配。".to_owned(),
            confidence: 1.0,
            authoritative_fields: serde_json::from_value(json!({
                "allocation_context": state["allocation_context"],
                "allocation": allocation,
            }))?,
            details: Vec::new(),
            missing_fields: Vec::new(),
            ambiguities: Vec::new(),
        },
    )?;
    let artifact = serde_json::to_value(artifact)?;
    state["allocation_artifact"] = artifact.clone();
    state["allocation_result"] = artifact.clone();
    Ok(artifact)
}

fn run_phase8(
    store: &FileStore,
    location: &RunLocation,
    state: &mut Value,
    runtime: &RuntimeConfig,
    config: &Value,
    args: &ExecArgs,
) -> Result<BTreeMap<String, String>> {
    let mut decision_snapshots = BTreeMap::new();
    if runtime.evaluation.enabled && !args.mock {
        let context = evaluation_persistence_context(runtime, config, args, location)?;
        if !matches!(context.namespace, PersistenceNamespace::Canonical)
            || context.canonical_memory_writes_enabled
        {
            let evaluation = EvaluationStore::open(store.clone(), context.clone())?;
            let usage_ledger =
                orchestrator_store::MemoryUsageLedger::new(store.clone(), location.clone());
            let memory_usage_ref = if usage_ledger.read_all()?.is_empty() {
                MemoryUsageReferenceStatus::NotCaptured
            } else {
                MemoryUsageReferenceStatus::Available {
                    document_ref: usage_ledger.publish_report(&Utc::now().to_rfc3339())?,
                }
            };
            for ticker in investable_assets_from_state(state) {
                let decision = decision_snapshot(
                    runtime,
                    location,
                    &ticker,
                    &context,
                    memory_usage_ref.clone(),
                )?;
                let decision = evaluation.write_decision(location, decision)?;
                decision_snapshots.insert(ticker, serde_json::to_value(decision)?);
            }
        }
    }
    state["phase8"] = json!({"status": "completed", "archive": "file_store"});
    write_final_decision_indexes(store, location, state, &decision_snapshots)
}

fn write_final_decision_indexes(
    store: &FileStore,
    location: &RunLocation,
    state: &Value,
    decision_snapshots: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, String>> {
    let mut summary_units = BTreeMap::new();
    let created_at = Utc::now().to_rfc3339();
    let unit_key = "phase8:final-decision:aggregate".to_owned();
    let payload = json!({
        "final_trade_decision": state["final_trade_decision"],
        "allocation_context": state["allocation_context"],
        "portfolio_allocation": state["portfolio_allocation"],
        "allocation_result": state["allocation_result"],
        "decision_snapshots": decision_snapshots,
    });
    let source_payload_hash = content_hash(&payload)?;
    let index_id = derive_summary_index_id(
        &location.run_id,
        8,
        "rust.final_decision",
        None,
        None,
        &unit_key,
        &source_payload_hash,
    );
    let scope = IndexScope {
        kind: IndexKind::PhaseSummary,
        location: Some(location.clone()),
        index_id: index_id.clone(),
        run_id: location.run_id.clone(),
        source_run_id: None,
        source_phase: 8,
        role: "rust.final_decision".to_owned(),
        ticker: None,
        topic_id: None,
        source_payload_hash,
        authoritative_fields: payload
            .as_object()
            .expect("final decision payload is an object")
            .clone(),
        created_at,
    };
    create_index(
        store,
        CreateIndexInput {
            scope: scope.clone(),
            summary: "Final decision".to_owned(),
            confidence: 1.0,
            pattern_key: None,
            applies_to_phases: Vec::new(),
        },
    )?;
    append_index_detail(
        store,
        AppendIndexDetailInput {
            scope: scope.clone(),
            section: DetailSection::Execution,
            detail: serde_json::to_string(&payload)?,
            source_refs: Vec::new(),
        },
    )?;
    finalize_index(store, &scope)?;
    summary_units.insert(unit_key, index_id);
    Ok(summary_units)
}

fn evaluation_persistence_context(
    runtime: &RuntimeConfig,
    config: &Value,
    args: &ExecArgs,
    location: &RunLocation,
) -> Result<PersistenceContextV1> {
    let run_purpose = if args.mock {
        RunPurpose::Mock
    } else if args.debug {
        RunPurpose::Debug
    } else {
        args.run_purpose
            .map(Into::into)
            .unwrap_or(runtime.evaluation.default_run_purpose)
    };
    let namespace = match run_purpose {
        RunPurpose::Live | RunPurpose::Paper => PersistenceNamespace::Canonical,
        RunPurpose::Debug => PersistenceNamespace::Debug {
            invocation_id: location.run_id.clone(),
        },
        RunPurpose::Mock => PersistenceNamespace::Disabled,
        RunPurpose::Replay => PersistenceNamespace::Replay {
            replay_id: location.run_id.clone(),
        },
        RunPurpose::MigrationFixture => PersistenceNamespace::MigrationFixture {
            fixture_id: location.run_id.clone(),
        },
    };
    let evaluation_config = config_get(config, "orchestrator.evaluation")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let config_hash = content_hash(&evaluation_config)?;
    Ok(PersistenceContextV1 {
        run_purpose,
        namespace,
        canonical_memory_writes_enabled: runtime.evaluation.canonical_memory_writes_enabled,
        invocation_id: location.run_id.clone(),
        config_ref: PolicyRef {
            policy_id: "orchestrator.evaluation".to_owned(),
            version: runtime.evaluation.policy_version,
            content_hash: config_hash.clone(),
        },
        source_store_fingerprint: config_hash,
    })
}

fn decision_snapshot(
    runtime: &RuntimeConfig,
    location: &RunLocation,
    ticker: &str,
    context: &PersistenceContextV1,
    memory_usage_ref: MemoryUsageReferenceStatus,
) -> Result<DecisionSnapshotV2> {
    let policy = context.config_ref.clone();
    let benchmark_selection = runtime
        .evaluation
        .benchmarks
        .get(&ticker.to_ascii_uppercase())
        .map(|binding| BenchmarkSelectionV1::Configured {
            binding: BenchmarkBindingV1 {
                benchmark_id: binding.ticker.clone(),
                provider: binding.provider.clone(),
                price_basis: binding.price_basis,
                policy_ref: policy.clone(),
            },
        })
        .unwrap_or_else(|| BenchmarkSelectionV1::Missing {
            policy_ref: policy.clone(),
        });
    let decision_id = content_hash(&json!({
        "source_run_id": location.run_id,
        "ticker": ticker,
        "evaluation_contract_id": runtime.evaluation.evaluation_contract_id,
    }))?;
    Ok(DecisionSnapshotV2 {
        schema_version: DECISION_SNAPSHOT_SCHEMA_VERSION,
        decision_id,
        source_run_id: location.run_id.clone(),
        ticker: ticker.to_owned(),
        thesis: unavailable_decision_section(),
        trade: unavailable_decision_section(),
        risk: unavailable_decision_section(),
        allocation: unavailable_decision_section(),
        execution_plan: unavailable_decision_section(),
        evaluation_spec: EvaluationSpec {
            evaluation_contract_id: runtime.evaluation.evaluation_contract_id.clone(),
            horizon_trading_days: runtime.evaluation.prediction_horizon_trading_days,
            benchmark_policy_ref: policy.clone(),
            benchmark_selection,
            price_basis: runtime.evaluation.price_basis,
            materialization_policy_ref: policy,
        },
        source_artifact_refs: Vec::new(),
        source_input_refs: Vec::new(),
        memory_usage_ref,
        run_purpose: context.run_purpose,
        decided_at: format!("{}T00:00:00Z", location.current_date),
        content_hash: String::new(),
    })
}

fn unavailable_decision_section<T>() -> DecisionSection<T> {
    DecisionSection::Unavailable {
        reason: DecisionSectionUnavailableReason::ArtifactMissing,
        source_refs: Vec::new(),
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_unit(
    state: &mut Value,
    runtime: &RuntimeConfig,
    role: &str,
    phase: i64,
    kind: &str,
    round: Option<i64>,
    topic_id: Option<&str>,
    ticker: Option<&str>,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<Value> {
    let completed_key = completed_unit_key(role, phase, kind, round, topic_id, ticker);
    if let Some(artifact) = state
        .get("_completed_units")
        .and_then(|units| units.get(&completed_key))
        .cloned()
    {
        return Ok(artifact);
    }
    let scoped = scoped_state_for_unit(state, ticker);
    let prompt_path = runtime
        .prompts
        .path_for(prompt_owner_for_unit(role, kind))
        .cloned();
    let job = prepare_role_job(RoleRun {
        state: scoped,
        role,
        phase,
        kind,
        round,
        topic_id,
        mock: state["mock"].as_bool().unwrap_or(false),
        model_override: model,
        reasoning_effort_override: reasoning,
        config: runtime,
        prompt_path: prompt_path.as_deref(),
    })?;
    let result = run_role_jobs(vec![job], 1, runtime.workflow.agent_timeout_sec)
        .await
        .into_iter()
        .next()
        .context("ToolManaged role produced no result")?;
    record_role_job_metrics(state, &result);
    let raw = result.artifact.clone().with_context(|| {
        format!(
            "{} phase {phase} produced no final Assistant text: {}",
            role,
            result.error.as_deref().unwrap_or("unknown role failure")
        )
    })?;
    let response_text = raw
        .get("response_text")
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .with_context(|| format!("{role} phase {phase} returned empty response_text"))?
        .to_owned();
    let artifact = compile_unit_response(
        state,
        runtime,
        role,
        phase,
        kind,
        round,
        topic_id,
        ticker,
        &response_text,
        model,
        reasoning,
    )
    .await?;
    let session_id = if result.session_id.is_empty() {
        format!(
            "{}:p{}:{}:{}:{}:{}",
            state["run_id"].as_str().unwrap_or_default(),
            phase,
            role,
            phase2_profile_name(role, kind),
            topic_id.unwrap_or("aggregate"),
            round.unwrap_or(0)
        )
    } else {
        result.session_id
    };
    let turn_id = if result.turn_id.is_empty() {
        "mock-response".to_owned()
    } else {
        result.turn_id
    };
    state["_runtime_sessions"][runtime_session_key(role, kind, topic_id, round)] =
        json!({"session_id": session_id, "turn_id": turn_id});
    state["_completed_units"][completed_key] = artifact.clone();
    checkpoint_state(state)?;
    Ok(artifact)
}

#[allow(clippy::too_many_arguments)]
async fn compile_unit_response(
    state: &mut Value,
    runtime: &RuntimeConfig,
    role: &str,
    phase: i64,
    kind: &str,
    round: Option<i64>,
    topic_id: Option<&str>,
    ticker: Option<&str>,
    response_text: &str,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<Value> {
    let phase_u8 = u8::try_from(phase).context("compiled phase must fit u8")?;
    let mut candidate = if state["mock"].as_bool().unwrap_or(false) {
        mock_phase_index_candidate(state, phase_u8, role, kind, response_text)
    } else {
        let mut scoped = scoped_state_for_unit(state, ticker);
        scoped["_summary_source_payload"] = json!({
            "phase": phase,
            "role": role,
            "kind": kind,
            "round": round,
            "ticker": ticker,
            "topic_id": topic_id,
            "response_text": response_text,
        });
        let summary_role = format!("compressor.phase{phase}");
        let prompt_path = runtime
            .prompts
            .path_for(&summary_role)
            .with_context(|| format!("missing dedicated Phase {phase} Summary prompt"))?
            .clone();
        let job = prepare_role_job(RoleRun {
            state: scoped,
            role: "compressor.phase_summary",
            phase,
            kind: "phase_summary",
            round,
            topic_id,
            mock: false,
            model_override: model,
            reasoning_effort_override: reasoning,
            config: runtime,
            prompt_path: Some(&prompt_path),
        })?;
        let result = run_role_jobs(vec![job], 1, runtime.workflow.agent_timeout_sec)
            .await
            .into_iter()
            .next()
            .context("Phase Summary compiler produced no result")?;
        record_role_job_metrics(state, &result);
        let text = result
            .artifact
            .as_ref()
            .and_then(|value| value.get("response_text"))
            .and_then(Value::as_str)
            .with_context(|| {
                format!(
                    "Phase {phase} Summary compiler failed: {}",
                    result.error.as_deref().unwrap_or("empty response")
                )
            })?;
        let mut candidate = parse_phase_index_candidate(text)?;
        // The Summary model compresses only the Index fields.  Preserve the
        // original free-text response exactly once in the Rust-owned Detail;
        // asking the model to copy it again wastes output budget and can cause
        // long Phase 2 reports to terminate with finish_reason=Length.
        candidate.details = vec![
            crate::orchestration::summary_store::PhaseIndexCandidateDetail {
                section: "analysis".to_owned(),
                detail: response_text.to_owned(),
                source_refs: Vec::new(),
            },
        ];
        candidate
    };
    enrich_compiled_fields(
        role,
        kind,
        topic_id,
        response_text,
        &mut candidate.authoritative_fields,
    )?;
    validate_compiled_asset_scope(state, phase_u8, &candidate.authoritative_fields)?;
    if phase_u8 == 0 {
        let submission = phase0_submission(&candidate, response_text)?;
        commit_historical_reflection(
            Path::new(
                state["store_root"]
                    .as_str()
                    .context("store_root is required for Experience commit")?,
            ),
            state,
            submission,
        )?;
    }
    let index = write_compiled_phase_index(
        Path::new(
            state["store_root"]
                .as_str()
                .context("store_root is required for compiled Index")?,
        ),
        state,
        phase_u8,
        role,
        kind,
        round,
        ticker,
        topic_id,
        response_text,
        candidate,
    )?;
    Ok(json!({
        "phase": phase,
        "role": role,
        "profile": phase2_profile_name(role, kind),
        "unit_key": index.authoritative_fields.get("unit_key"),
        "ticker": ticker,
        "topic_id": topic_id,
        "payload": index.authoritative_fields,
        "response_text": response_text,
        "index_id": index.index_id,
        "authority": "index",
    }))
}

fn phase0_submission(
    candidate: &PhaseIndexCandidate,
    response_text: &str,
) -> Result<orchestrator_llm::tools::historical_reflection::HistoricalReflectionSubmission> {
    let fields = &candidate.authoritative_fields;
    let disposition = fields
        .get("disposition")
        .cloned()
        .context("Phase 0 Summary requires disposition")?;
    let learned = disposition.as_str() == Some("learned");
    let experience = fields
        .get("experience_candidate")
        .filter(|value| !value.is_null());
    let submission = serde_json::from_value(json!({
        "disposition": disposition,
        "summary": candidate.summary,
        "detail": candidate
            .details
            .first()
            .map(|detail| detail.detail.as_str())
            .unwrap_or(response_text),
        "confidence": learned.then_some(candidate.confidence),
        "root_cause_phase": fields.get("root_cause_phase"),
        "propagation_phases": fields.get("propagation_phases").cloned().unwrap_or_else(|| json!([])),
        "source_refs": fields.get("source_index_ids").cloned().unwrap_or_else(|| json!([])),
        "pattern_identity": experience.and_then(|value| value.get("pattern_identity")),
        "learned_rule": experience.and_then(|value| value.get("learned_rule")),
    }))?;
    Ok(submission)
}

fn enrich_compiled_fields(
    role: &str,
    kind: &str,
    topic_id: Option<&str>,
    response_text: &str,
    fields: &mut serde_json::Map<String, Value>,
) -> Result<()> {
    attach_verified_web_evidence(response_text, fields)?;
    if kind == "topic_control" {
        let has_valid_soft_control = fields
            .get("soft_control")
            .and_then(Value::as_object)
            .and_then(|value| value.get("should_continue"))
            .and_then(Value::as_bool)
            .is_some();
        if !has_valid_soft_control {
            fields.insert(
                "soft_control".to_owned(),
                json!({
                    "should_continue": false,
                    "info_gain_score": 0.0,
                    "stop_reason": "Summary omitted a valid soft_control decision; debate stopped safely.",
                }),
            );
        }
    }
    if kind == "topic_generation" {
        for (offset, topic) in fields
            .get_mut("topics")
            .and_then(Value::as_array_mut)
            .into_iter()
            .flatten()
            .enumerate()
        {
            let topic_object = topic
                .as_object_mut()
                .context("Phase 2 topic must be an object")?;
            if !topic_object.contains_key("topic_id") {
                let seed = format!(
                    "{}:{}",
                    topic_object
                        .get("topic")
                        .and_then(Value::as_str)
                        .unwrap_or("topic"),
                    offset
                );
                topic_object.insert(
                    "topic_id".to_owned(),
                    Value::String(format!("topic-{}", orchestrator_core::md5_3(seed))),
                );
            }
        }
    }
    if matches!(kind, "bull_seed" | "bear_seed") {
        let side = if role.contains("bull") {
            "bull"
        } else {
            "bear"
        };
        for (offset, claim) in fields
            .get_mut("claims")
            .and_then(Value::as_array_mut)
            .into_iter()
            .flatten()
            .enumerate()
        {
            let claim = claim
                .as_object_mut()
                .context("Phase 2 claim must be an object")?;
            claim.entry("claim_id".to_owned()).or_insert_with(|| {
                Value::String(format!(
                    "{}:{side}:{}",
                    topic_id.unwrap_or("topic"),
                    offset + 1
                ))
            });
        }
    }
    Ok(())
}

fn attach_verified_web_evidence(
    response_text: &str,
    fields: &mut serde_json::Map<String, Value>,
) -> Result<()> {
    let marker = orchestrator_llm::tools::research_evidence_gap::VERIFIED_PACKET_MARKER;
    let Some((_, packet_json)) = response_text.rsplit_once(marker) else {
        return Ok(());
    };
    let packets: Vec<Value> = serde_json::from_str(packet_json.trim())
        .context("Rust-verified Web evidence packet attachment is malformed")?;
    let mut seen = BTreeSet::new();
    let mut evidence = Vec::new();
    let mut requests = Vec::new();
    for packet in packets {
        for item in ["evidence", "counterevidence"].into_iter().flat_map(|key| {
            packet
                .get(key)
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        }) {
            let Some(evidence_id) = item.get("evidence_id").and_then(Value::as_str) else {
                continue;
            };
            if seen.insert(evidence_id.to_owned()) {
                evidence.push(item.clone());
            }
        }
        requests.push(json!({
            "status": packet.get("status").cloned().unwrap_or(Value::Null),
            "request_id": packet.get("request_id").cloned().unwrap_or(Value::Null),
            "scope": packet.get("scope").cloned().unwrap_or(Value::Null),
            "cached": packet.get("cached").cloned().unwrap_or(Value::Null),
            "unresolved_gaps": packet.get("unresolved_gaps").cloned().unwrap_or_else(|| json!([])),
            "search_queries": packet.get("search_queries").cloned().unwrap_or_else(|| json!([])),
            "source_count": packet.get("source_count").cloned().unwrap_or(Value::Null),
        }));
    }
    fields.insert("web_evidence".to_owned(), Value::Array(evidence));
    fields.insert("web_evidence_requests".to_owned(), Value::Array(requests));
    Ok(())
}

fn mock_phase_index_candidate(
    state: &Value,
    phase: u8,
    role: &str,
    kind: &str,
    response_text: &str,
) -> PhaseIndexCandidate {
    let analysis = tickers_from_state(state);
    let investable = investable_assets_from_state(state);
    let authoritative_fields = match phase {
        0 => json!({"disposition":"no_reusable_memory","source_index_ids":[]}),
        1 => json!({"per_ticker": analysis.into_iter().map(|ticker| (
            ticker,
            json!({
                "direction":"neutral","confidence":0.5,"priced_in":"unclear",
                "report":response_text,"key_evidence":[],"validation_triggers":[],
                "data_gaps":[],"echo_chamber_risk":"low","crowded_consensus_risk":"low",
                "jin10_attention":[]
            })
        )).collect::<serde_json::Map<_, _>>()}),
        2 if kind == "topic_generation" => {
            json!({"common_ground":{},"topics":[],"summary":"No mock debate topic."})
        }
        2 => json!({"status":"prepared","claims":[],"replies":[]}),
        3 => json!({
            "decisions": investable.iter().map(|ticker| (
                ticker.clone(),
                json!({
                    "rating":"Hold","long_probability":0.5,"short_probability":0.5,
                    "base_probability":0.5,"debate_adjustment":0.0,
                    "confidence_basis":"evidence_balanced","hold_reason":"evidence_balanced",
                    "plan":response_text,"probability_rationale":"Mock evidence is balanced.",
                    "scenarios":{},"decision_hinges":[],"validation_plan":[]
                })
            )).collect::<serde_json::Map<_, _>>(),
            "regime_context": {"signal": "VIX", "status": "mock"}
        }),
        4 => json!({"plans": investable.iter().map(|ticker| (
            ticker.clone(),
            json!({
                "action":"Hold","execution_decision":"hold",
                "position_size_pct_max":0.0,"entry_price":null,"stop_loss":null,
                "blockers":[],"execution_conditions":[],"downgrade_reason":"mock","rationale":response_text
            })
        )).collect::<serde_json::Map<_, _>>()}),
        5 => json!({
            "stance":role.strip_prefix("risk.").unwrap_or("neutral"),
            "unique_risk_contribution":"","disagreement_with_prior":"",
            "no_new_information":true,"recommended_adjustment":"",
            "per_asset": investable.iter().map(|ticker| (
                ticker.clone(),
                json!({
                    "position_cap_pct":0.0,"max_drawdown_pct":0.0,"stop_type":"",
                    "risk_off_trigger":"","rebalance_trigger":"","review_window":"",
                    "constraint_confidence":0.0
                })
            )).collect::<serde_json::Map<_, _>>(),
            "cash_hedge_recommendation":""
        }),
        6 => json!({"per_asset": investable.iter().map(|ticker| (
            ticker.clone(),
            json!({
                "direction_constraint":"unchanged","execution_status":"wait",
                "max_target_weight":0.0,"max_weight_delta":0.0,
                "binding_risk_controls":[],"rating":"Hold",
                "inherited_probability":0.5,"execution_rationale":response_text,
                "unresolved_blockers":[]
            })
        )).collect::<serde_json::Map<_, _>>()}),
        _ => json!({}),
    };
    PhaseIndexCandidate {
        summary: format!("Mock Phase {phase} {kind} summary"),
        confidence: if phase == 0 { 0.0 } else { 0.5 },
        authoritative_fields: authoritative_fields
            .as_object()
            .cloned()
            .unwrap_or_default(),
        details: vec![PhaseIndexCandidateDetail {
            section: "analysis".to_owned(),
            detail: response_text.to_owned(),
            source_refs: Vec::new(),
        }],
        missing_fields: Vec::new(),
        ambiguities: Vec::new(),
    }
}

fn prompt_owner_for_unit<'a>(role: &'a str, kind: &str) -> &'a str {
    if role == "mediator.topic" && kind == "warmup" {
        "researcher.warmup"
    } else {
        role
    }
}

fn scoped_state_for_unit(state: &Value, ticker: Option<&str>) -> Value {
    let mut scoped = state.clone();
    if let Some(ticker) = ticker {
        scoped["ticker"] = json!(ticker);
        scoped["tickers"] = json!([ticker]);
        scoped["analysis_universe"] = json!([ticker]);
    }
    scoped
}

fn completed_unit_key(
    role: &str,
    phase: i64,
    kind: &str,
    round: Option<i64>,
    topic_id: Option<&str>,
    ticker: Option<&str>,
) -> String {
    format!(
        "p{phase}:{role}:{kind}:{}:{}:{}",
        ticker.unwrap_or("aggregate"),
        topic_id.unwrap_or("none"),
        round.unwrap_or(0)
    )
}

fn checkpoint_state(state: &mut Value) -> Result<()> {
    let store_root = state
        .get("store_root")
        .and_then(Value::as_str)
        .context("store_root is required for FileStore state checkpoint")?
        .to_owned();
    let current_date = state
        .get("current_date")
        .and_then(Value::as_str)
        .context("current_date is required for FileStore state checkpoint")?
        .to_owned();
    let run_id = state
        .get("run_id")
        .and_then(Value::as_str)
        .context("run_id is required for FileStore state checkpoint")?
        .to_owned();
    let location = RunLocation::new(current_date, run_id)?;
    seal_state(state)?;
    FileStore::open(store_root, FileStoreOptions::default())?
        .write_json_value(&location.child_relative(Path::new("state.json"))?, state)?;
    Ok(())
}

fn runtime_session_key(
    role: &str,
    kind: &str,
    topic_id: Option<&str>,
    round: Option<i64>,
) -> String {
    format!(
        "{role}:{kind}:{}:{}",
        topic_id.unwrap_or("aggregate"),
        round.unwrap_or(0)
    )
}

fn runtime_session_for(
    state: &Value,
    role: &str,
    kind: &str,
    topic_id: Option<&str>,
    round: Option<i64>,
) -> Value {
    state["_runtime_sessions"]
        .get(runtime_session_key(role, kind, topic_id, round))
        .cloned()
        .unwrap_or(Value::Null)
}

fn phase2_profile_name(role: &str, kind: &str) -> &'static str {
    match (role, kind) {
        ("mediator.topic", "warmup") => "researcher_warmup",
        ("mediator.topic", "topic_generation") => "topic_generation",
        ("researcher.bull.initial" | "researcher.bear.initial", _) => "debate_seed",
        ("researcher.bull.interaction" | "researcher.bear.interaction", _) => "debate_response",
        ("mediator.topic_controller", _) => "topic_control",
        ("manager.research", _) => "research_decision",
        ("trader", _) => "trade_intent",
        ("risk.aggressive" | "risk.neutral" | "risk.conservative", _) => "risk_review",
        ("portfolio.manager", _) => "portfolio_decision",
        _ => "analyst_report",
    }
}

fn record_phase2_session(
    state: &mut Value,
    role: &str,
    kind: &str,
    topic_id: Option<&str>,
    side: Option<&str>,
    round: Option<i64>,
) {
    let session = runtime_session_for(state, role, kind, topic_id, round);
    if kind == "warmup" {
        state["phase2_warmup"]["session_id"] = session["session_id"].clone();
        state["phase2_warmup"]["turn_id"] = session["turn_id"].clone();
        return;
    }
    let (Some(topic_id), Some(side)) = (topic_id, side) else {
        return;
    };
    state["phase2_file_store_sessions"][topic_id][side] = session;
}

fn weighted_probability_base(state: &Value) -> Value {
    let values = investable_assets_from_state(state)
        .into_iter()
        .map(|ticker| (ticker, json!({"long_probability": 0.5, "short_probability": 0.5, "source": "phase1_tool_managed"})))
        .collect::<serde_json::Map<_, _>>();
    Value::Object(values)
}

fn validate_asset_keys(state: &Value, value: &Value, label: &str) -> Result<()> {
    let actual = value
        .as_object()
        .with_context(|| format!("{label} must be an object"))?
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    let expected = investable_assets_from_state(state)
        .into_iter()
        .collect::<BTreeSet<_>>();
    if actual != expected {
        bail!("{label} must contain exactly {expected:?}; got {actual:?}")
    }
    Ok(())
}

fn validate_compiled_asset_scope(
    state: &Value,
    phase: u8,
    fields: &serde_json::Map<String, Value>,
) -> Result<()> {
    match phase {
        1 => validate_analysis_keys(
            state,
            fields.get("per_ticker").unwrap_or(&Value::Null),
            "Phase 1 per_ticker",
        ),
        3 => validate_asset_keys(
            state,
            fields.get("decisions").unwrap_or(&Value::Null),
            "Phase 3 decisions",
        ),
        4 => validate_asset_keys(
            state,
            fields.get("plans").unwrap_or(&Value::Null),
            "Phase 4 plans",
        ),
        5 | 6 => validate_asset_keys(
            state,
            fields.get("per_asset").unwrap_or(&Value::Null),
            &format!("Phase {phase} per_asset"),
        ),
        _ => Ok(()),
    }
}

fn validate_analysis_keys(state: &Value, value: &Value, label: &str) -> Result<()> {
    let actual = value
        .as_object()
        .with_context(|| format!("{label} must be an object"))?
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    let expected = tickers_from_state(state)
        .into_iter()
        .collect::<BTreeSet<_>>();
    if actual != expected {
        bail!("{label} must contain exactly {expected:?}; got {actual:?}")
    }
    Ok(())
}

fn seal_state(state: &mut Value) -> Result<()> {
    state["content_hash"] = Value::String(String::new());
    state["content_hash"] = Value::String(content_hash(state)?);
    Ok(())
}

#[cfg(test)]
mod phase2_session_tests {
    use orchestrator_core::{MemoryPolicyV1, PolicyRef};
    use orchestrator_store::{PhaseStatus, RunLocation, RunManifest, RunManifestInit};
    use serde_json::json;

    use super::{
        attach_verified_web_evidence, controller_should_continue, enrich_compiled_fields,
        highest_completed_phase, prompt_owner_for_unit, record_phase2_session, runtime_session_key,
        scoped_state_for_unit, select_reflection_task_budget, sync_manifest_health,
    };

    #[test]
    fn warmup_uses_its_own_prompt_owner() {
        assert_eq!(
            prompt_owner_for_unit("mediator.topic", "warmup"),
            "researcher.warmup"
        );
        assert_eq!(
            prompt_owner_for_unit("mediator.topic", "topic_generation"),
            "mediator.topic"
        );
    }

    #[test]
    fn controller_soft_control_owns_whether_another_round_runs() {
        assert!(controller_should_continue(&json!({
            "payload": {
                "soft_control": {"should_continue": true},
                "next_steers": [{"steer_id": "steer-1"}]
            }
        }))
        .unwrap());
        assert!(!controller_should_continue(&json!({
            "payload": {"soft_control": {"should_continue": false}}
        }))
        .unwrap());
        assert!(!controller_should_continue(&json!({})).unwrap());
        assert!(controller_should_continue(&json!({
            "payload": {"soft_control": {"should_continue": true}}
        }))
        .is_err());
    }

    #[test]
    fn missing_controller_soft_control_is_normalized_to_safe_stop() {
        let mut fields = serde_json::Map::from_iter([("soft_control".to_owned(), json!(""))]);
        enrich_compiled_fields(
            "mediator.topic_controller",
            "topic_control",
            Some("topic-a"),
            "controller report",
            &mut fields,
        )
        .unwrap();
        assert_eq!(fields["soft_control"]["should_continue"], false);
        assert_eq!(fields["soft_control"]["info_gain_score"], 0.0);
    }

    #[test]
    fn verified_web_evidence_overrides_summary_omission() {
        let response = format!(
            "议题报告\n\n{}\n{}",
            orchestrator_llm::tools::research_evidence_gap::VERIFIED_PACKET_MARKER,
            json!([{
                "status": "supported",
                "request_id": "web-abcdef",
                "scope": "run:phase2:topic-generation",
                "cached": false,
                "evidence": [{
                    "evidence_id": "web-123456",
                    "claim": "official fact",
                    "relation": "supports",
                    "source_url": "https://example.com/fact"
                }],
                "counterevidence": [],
                "unresolved_gaps": [],
                "search_queries": ["official fact"],
                "source_count": 1
            }])
        );
        let mut fields = serde_json::Map::new();
        attach_verified_web_evidence(&response, &mut fields).unwrap();

        assert_eq!(fields["web_evidence"][0]["evidence_id"], "web-123456");
        assert_eq!(
            fields["web_evidence_requests"][0]["request_id"],
            "web-abcdef"
        );
    }

    #[test]
    fn historical_ticker_scope_does_not_narrow_investable_assets() {
        let scoped = scoped_state_for_unit(
            &json!({
                "tickers": ["QQQ", "SOXX"],
                "analysis_universe": ["QQQ", "SOXX"],
                "investable_assets": ["QQQ", "SOXX"]
            }),
            Some("SOXX"),
        );
        assert_eq!(scoped["tickers"], json!(["SOXX"]));
        assert_eq!(scoped["analysis_universe"], json!(["SOXX"]));
        assert_eq!(scoped["investable_assets"], json!(["QQQ", "SOXX"]));
    }

    #[test]
    fn warmup_artifact_retains_the_fork_identity_after_assignment() {
        let mut state = json!({
            "phase2_warmup": {"status": "completed"},
            "_runtime_sessions": {
                runtime_session_key("mediator.topic", "warmup", None, Some(0)): {
                    "session_id": "warmup-session",
                    "turn_id": "warmup-turn"
                }
            }
        });

        record_phase2_session(&mut state, "mediator.topic", "warmup", None, None, Some(0));

        assert_eq!(state["phase2_warmup"]["session_id"], "warmup-session");
        assert_eq!(state["phase2_warmup"]["turn_id"], "warmup-turn");
    }

    #[test]
    fn manifest_projects_degraded_state_on_each_completed_phase() {
        let mut manifest = RunManifest::new(RunManifestInit {
            location: RunLocation::new("2026-07-27", "run-health-test").unwrap(),
            workflow_version: "test".to_owned(),
            prompt_versions: Default::default(),
            git_sha: "test".to_owned(),
            config_hash: "test".to_owned(),
            role_profile_registry_hash: "test".to_owned(),
            created_at: "2026-07-27T00:00:00Z".to_owned(),
        })
        .unwrap();
        sync_manifest_health(
            &mut manifest,
            &json!({
                "degraded": true,
                "errors": [{"phase": 3, "kind": "artifact", "failure": "terminal missing"}]
            }),
        );

        assert!(manifest.degraded);
        assert_eq!(manifest.errors.len(), 1);
        assert_eq!(manifest.errors[0].phase, Some(3));
        assert_eq!(manifest.errors[0].code, "artifact");
    }

    #[test]
    fn completed_run_projection_uses_the_highest_completed_phase() {
        let mut manifest = RunManifest::new(RunManifestInit {
            location: RunLocation::new("2026-07-27", "run-phase-test").unwrap(),
            workflow_version: "test".to_owned(),
            prompt_versions: Default::default(),
            git_sha: "test".to_owned(),
            config_hash: "test".to_owned(),
            role_profile_registry_hash: "test".to_owned(),
            created_at: "2026-07-27T00:00:00Z".to_owned(),
        })
        .unwrap();
        manifest
            .phase_status
            .insert("2".to_owned(), PhaseStatus::Completed);
        manifest
            .phase_status
            .insert("7".to_owned(), PhaseStatus::Degraded);
        manifest
            .phase_status
            .insert("8".to_owned(), PhaseStatus::Completed);

        assert_eq!(highest_completed_phase(&manifest), Some(8));
    }

    #[test]
    fn reflection_scheduler_uses_versioned_quotas_and_fills_without_starvation() {
        let policy = MemoryPolicyV1 {
            policy_ref: PolicyRef {
                policy_id: "test-memory-policy".into(),
                version: 7,
                content_hash: "sha256:test".into(),
            },
            reflection_total_quota: 4,
            reflection_new_outcome_quota: 1,
            reflection_retry_quota: 1,
            reflection_backlog_quota: 1,
            reflection_max_attempts: 5,
        };
        let mut fresh = vec!["fresh-1", "fresh-2"];
        let mut retries = vec!["retry-1", "retry-2"];
        let mut backlog = vec!["backlog-1", "backlog-2"];
        let selected =
            select_reflection_task_budget(&mut fresh, &mut retries, &mut backlog, &policy);
        assert_eq!(selected.len(), 4);
        assert_eq!(&selected[..3], ["retry-1", "fresh-1", "backlog-1"]);
        // The final spare slot is filled in round-robin order instead of
        // repeatedly favoring a permanent retry stream.
        assert_eq!(selected[3], "retry-2");
    }
}
