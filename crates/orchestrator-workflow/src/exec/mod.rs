use anyhow::{bail, Context, Result};
use chrono::{Local, NaiveDate, Utc};
use orchestrator_core::{
    config_int, config_str, config_strings, display_ticker, load_config, parse_tickers,
    project_path, ToolManagedProfile,
};
use orchestrator_llm::agent_loop::{
    FileStoreSessionRuntime, SessionRuntimeSpec, ToolResultItem, Turn,
};
use orchestrator_store::{
    content_hash, list_run_locations, read_indexes, read_learning_record, read_run_manifest,
    rebuild_run_manifest, write_learning_record, write_run_manifest, FileStore, FileStoreOptions,
    FinalizedArtifactRef, IndexKind, IndexQuery, LearningKind, LearningRecord, ManifestError,
    RunLocation, RunManifest, RunManifestInit, RunStatus,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::Duration,
};

use crate::orchestration::{
    allocation::{compute_allocation_context, derive_guarded_allocation},
    config::RuntimeConfig,
    domain_runtime::{
        finalize_degraded_analyst_report, finalize_degraded_phase2,
        finalize_degraded_portfolio_decision, finalize_degraded_research_decision,
        finalize_degraded_risk_review, finalize_degraded_trade_intent, FileStoreDomainRuntimePlan,
    },
    input_snapshot_runtime::{capture_phase1_file_store_inputs, phase1_input_sources},
    lifecycle::{run_id_for, set_phase_status, tickers_from_state},
    role_jobs::{prepare_role_job, record_role_job_metrics, run_role_jobs, RoleRun},
    summary_store::{
        phase_summary_source_payload, planned_summary_units, write_deterministic_phase_summary,
    },
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
    let store_root = runtime.store.resolve_root(args.store_root.as_deref())?;
    let run_id = run_id_for(&tickers, &current_date);
    let location = RunLocation::new(current_date.clone(), run_id.clone())?;
    let store = FileStore::open(
        &store_root,
        FileStoreOptions {
            atomic_fsync: runtime.store.atomic_fsync,
            stale_temp_age: Some(Duration::from_secs(runtime.store.stale_temp_age_sec)),
        },
    )?;
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
    if args.from_phase <= 1
        && args.to_phase >= 1
        && (!phase_completed(&manifest, 1)
            || !has_required_phase_summaries(&store, &location, &state, &runtime, 1)?)
    {
        run_phase1(
            &mut state,
            &runtime,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        refresh_finalized_artifacts(&store, &location, &mut manifest)?;
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
        refresh_finalized_artifacts(&store, &location, &mut manifest)?;
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
        refresh_finalized_artifacts(&store, &location, &mut manifest)?;
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
        refresh_finalized_artifacts(&store, &location, &mut manifest)?;
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
        refresh_finalized_artifacts(&store, &location, &mut manifest)?;
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
        refresh_finalized_artifacts(&store, &location, &mut manifest)?;
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
        let allocation_artifact = run_phase7(&store, &location, &mut state, &runtime)?;
        record_manifest_artifact(
            &mut manifest,
            &allocation_artifact,
            Path::new("artifacts/phase7/allocation.json"),
        )?;
        refresh_finalized_artifacts(&store, &location, &mut manifest)?;
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
        run_phase8(&store, &location, &mut state)?;
        finish_phase(&store, &location, &mut manifest, &mut state, 8, "done")?;
    }

    // Artifact files are the recovery authority. At successful run completion
    // project every finalized file back into the lightweight manifest without
    // treating state or sessions as proof of completion.
    let rebuilt = rebuild_run_manifest(
        &store,
        RunManifestInit {
            location: location.clone(),
            workflow_version: manifest.workflow_version.clone(),
            prompt_versions: manifest.prompt_versions.clone(),
            git_sha: manifest.git_sha.clone(),
            config_hash: manifest.config_hash.clone(),
            role_profile_registry_hash: manifest.role_profile_registry_hash.clone(),
            created_at: manifest.created_at.clone(),
        },
    )?;
    manifest.artifacts = rebuilt.artifacts;
    manifest.status = RunStatus::Completed;
    sync_manifest_health(&mut manifest, &state);
    if let Some(phase) = highest_completed_phase(&manifest) {
        manifest.current_phase = phase;
    }
    manifest.completed_at = Some(Utc::now().to_rfc3339());
    write_run_manifest(&store, &location, manifest)?;
    seal_state(&mut state)?;
    store.write_json_value(&location.child_relative(Path::new("state.json"))?, &state)?;

    Ok(json!({
        "run_id": state["run_id"],
        "date": state["current_date"],
        "store_root": store.root(),
        "debate_mode": "file_store",
        "degraded": state["degraded"],
        "rating": state.pointer("/research_plan/rating").cloned().unwrap_or(Value::Null),
        "action": state.pointer("/trader_investment_plan/intent/action").cloned().unwrap_or(Value::Null),
        "portfolio_allocation": state.get("portfolio_allocation").cloned().unwrap_or(Value::Null),
        "run_state": state,
    }))
}

fn validate_args(args: &ExecArgs) -> Result<()> {
    if args.from_phase > args.to_phase || args.to_phase > 8 {
        bail!("phase range must satisfy 0 <= from_phase <= to_phase <= 8")
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

/// Refresh only the manifest's reference catalog before a Phase Summary is
/// planned. The rebuild scans finalized Artifact files and their completed
/// Draft references; it never reads the mutable workflow state.
fn refresh_finalized_artifacts(
    store: &FileStore,
    location: &RunLocation,
    manifest: &mut RunManifest,
) -> Result<()> {
    let rebuilt = rebuild_run_manifest(
        store,
        RunManifestInit {
            location: location.clone(),
            workflow_version: manifest.workflow_version.clone(),
            prompt_versions: manifest.prompt_versions.clone(),
            git_sha: manifest.git_sha.clone(),
            config_hash: manifest.config_hash.clone(),
            role_profile_registry_hash: manifest.role_profile_registry_hash.clone(),
            created_at: manifest.created_at.clone(),
        },
    )?;
    manifest.artifacts = rebuilt.artifacts;
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
    // A process may have committed one or more canonical Artifacts and
    // checkpointed their completed units before it had rebuilt the mutable
    // phase projection in state.json.  Treat that projection gap as an
    // incomplete phase so the normal phase runner rehydrates from completed
    // Artifacts; `run_unit` returns those files without another LLM call.
    // A finalized Index is still the only proof that the summary itself is
    // complete.
    if phase_summary_source_payload(store.root(), state, i64::from(phase)).is_err() {
        return Ok(false);
    }
    let (_, units) = planned_summary_units(
        store.root(),
        state,
        i64::from(phase),
        runtime.tool_managed.max_summary_units_per_phase,
    )?;
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
    .indexes
    .into_iter()
    .map(|index| index.index_id)
    .collect::<std::collections::BTreeSet<_>>();
    Ok(units.iter().all(|unit| completed.contains(&unit.index_id)))
}

async fn summarize(
    store_root: &Path,
    state: &mut Value,
    runtime: &RuntimeConfig,
    phase: i64,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<BTreeMap<String, String>> {
    let (_, units) = planned_summary_units(
        store_root,
        state,
        phase,
        runtime.tool_managed.max_summary_units_per_phase,
    )?;
    let summary_units = units
        .iter()
        .map(|unit| (unit.unit_key.clone(), unit.index_id.clone()))
        .collect::<BTreeMap<_, _>>();
    // `run_phase*` may have just rehydrated its state projection from
    // canonical Artifacts after a process crash.  Do not re-invoke a Summary
    // Agent merely because that mutable projection was absent when the outer
    // phase gate ran: completed Indexes are authoritative and already cover
    // these fixed Rust-planned units.
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
    .indexes
    .into_iter()
    .map(|index| index.index_id)
    .collect::<std::collections::BTreeSet<_>>();
    if units
        .iter()
        .all(|unit| completed_ids.contains(&unit.index_id))
    {
        return Ok(summary_units);
    }
    if state["mock"].as_bool().unwrap_or(false) {
        write_deterministic_phase_summary(
            store_root,
            state,
            phase,
            runtime.tool_managed.max_summary_units_per_phase,
        )?;
        return Ok(summary_units);
    }
    let (source_payload, units) = planned_summary_units(
        store_root,
        state,
        phase,
        runtime.tool_managed.max_summary_units_per_phase,
    )?;
    let mut completed = Vec::with_capacity(units.len());
    for unit in units {
        state["_summary_unit"] = serde_json::to_value(&unit)?;
        state["_summary_source_payload"] = source_payload.clone();
        let artifact = match run_unit(
            state,
            runtime,
            "compressor.phase_summary",
            phase,
            "phase_summary",
            None,
            unit.topic_id.as_deref(),
            unit.ticker.as_deref(),
            model,
            reasoning,
        )
        .await
        {
            Ok(artifact) => artifact,
            Err(error) => {
                // A summary cannot block the workflow merely because its
                // compressor did not reach terminal finalize.  The fixed
                // Rust plan writes the same Index/Detail schema through the
                // same create/append/finalize service; it never revives a
                // legacy summary bundle or a second persistence path.
                state["degraded"] = Value::Bool(true);
                if let Some(errors) = state["errors"].as_array_mut() {
                    errors.push(json!({
                        "phase": phase,
                        "role": "compressor.phase_summary",
                        "unit_key": unit.unit_key,
                        "error": error.to_string(),
                        "fallback": "deterministic_index_finalize",
                    }));
                }
                write_deterministic_phase_summary(
                    store_root,
                    state,
                    phase,
                    runtime.tool_managed.max_summary_units_per_phase,
                )?;
                if let Some(object) = state.as_object_mut() {
                    object.remove("_summary_unit");
                    object.remove("_summary_source_payload");
                }
                return Ok(summary_units);
            }
        };
        completed.push(artifact);
    }
    state["phase_summary_live"][phase.to_string()] = Value::Array(completed);
    if let Some(object) = state.as_object_mut() {
        object.remove("_summary_unit");
        object.remove("_summary_source_payload");
    }
    Ok(summary_units)
}

async fn run_phase0(
    store: &FileStore,
    location: &RunLocation,
    state: &mut Value,
    runtime: &RuntimeConfig,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<()> {
    let tasks = reflection_tasks(store, location, state, runtime.reflection.task_limit)?;
    let planned_tasks = tasks.clone();
    let mut completed = Vec::new();
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
        let artifact = run_unit(
            state,
            runtime,
            "reflector.historical",
            0,
            "historical_reflection",
            None,
            None,
            task.get("ticker").and_then(Value::as_str),
            model,
            reasoning,
        )
        .await?;
        let ticker = task["ticker"]
            .as_str()
            .expect("planned reflection ticker")
            .to_owned();
        let source_run_id = task["source_run_id"]
            .as_str()
            .expect("planned reflection source run")
            .to_owned();
        write_learning_record(
            store,
            location,
            LearningKind::Reflection,
            LearningRecord {
                schema_version: orchestrator_store::LEARNING_RECORD_SCHEMA_VERSION,
                kind: LearningKind::Reflection,
                run_id: location.run_id.clone(),
                ticker,
                source_run_id: Some(source_run_id),
                payload: json!({"experience_index": artifact}),
                created_at: Utc::now().to_rfc3339(),
                content_hash: String::new(),
            },
        )?;
        completed.push(artifact);
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
    limit: usize,
) -> Result<Vec<Value>> {
    let mut tasks = Vec::new();
    for location in list_run_locations(store)? {
        if location == *current {
            continue;
        }
        for ticker in tickers_from_state(state) {
            let Ok(outcome) =
                read_learning_record(store, &location, LearningKind::Outcome, &ticker)
            else {
                continue;
            };
            let source_run_id = outcome
                .source_run_id
                .clone()
                .unwrap_or_else(|| location.run_id.clone());
            if source_run_id == current.run_id {
                continue;
            }
            let source = orchestrator_store::find_run_location(store, &source_run_id)?;
            if source.is_none() {
                continue;
            }
            let source_location = source.expect("checked Some");
            if read_indexes(
                store,
                Some(&source_location),
                &IndexQuery {
                    kind: Some(IndexKind::PhaseSummary),
                    ticker: Some(ticker.clone()),
                    limit: 1,
                    ..Default::default()
                },
            )?
            .indexes
            .is_empty()
            {
                continue;
            }
            let decision =
                read_learning_record(store, &source_location, LearningKind::Decision, &ticker)
                    .ok()
                    .map(|record| record.payload)
                    .unwrap_or_else(|| json!({"status":"unavailable"}));
            tasks.push(json!({
                "task_id": tasks.len() as i64 + 1,
                "ticker": ticker,
                "source_run_id": source_run_id,
                "outcome": outcome.payload,
                "decision": decision,
            }));
            if tasks.len() >= limit.max(1) {
                return Ok(tasks);
            }
        }
    }
    Ok(tasks)
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
        state["file_store_input"] = json!({
            "store_root": input.store_root,
            "run_id": input.run_id,
            "current_date": input.current_date,
        });
    }
    let roles = ["analyst.technical", "analyst.news_macro"];
    let mut reports = serde_json::Map::new();
    for role in roles {
        for ticker in tickers_from_state(state) {
            let artifact = run_unit(
                state,
                runtime,
                role,
                1,
                "artifact",
                None,
                None,
                Some(&ticker),
                model,
                reasoning,
            )
            .await?;
            let entry = reports
                .entry(role.to_owned())
                .or_insert_with(|| json!({"role": role, "per_ticker": {}}));
            entry["per_ticker"][ticker] = artifact
                .get("per_ticker")
                .and_then(|items| items.get(&ticker))
                .cloned()
                .unwrap_or(artifact.clone());
        }
    }
    state["analyst_reports"] = Value::Object(reports);
    state["phase1_index"] = json!({"per_ticker": phase1_index(state), "authority": "file_store"});
    state["weighted_probability_base"] = weighted_probability_base(state);
    Ok(())
}

async fn run_phase2(
    store: &FileStore,
    location: &RunLocation,
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
                Some(1),
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
                Some(1),
            );
            state["topic_debate_states"][&topic_id]["turns"]
                .as_array_mut()
                .expect("topic turns initialized")
                .push(json!({"role":role, "artifact": response}));
        }
        let controller = run_unit(
            state,
            runtime,
            "mediator.topic_controller",
            2,
            "topic_control",
            Some(1),
            Some(&topic_id),
            None,
            model,
            reasoning,
        )
        .await?;
        state["topic_debate_states"][&topic_id]["controller_artifact"] = controller.clone();
        controllers.insert(topic_id, controller);
    }
    let source_payload_hash = content_hash(&Value::Object(controllers.clone()))?;
    let reducer = json!({
        "schema_version": 1,
        "artifact_id": format!("artifact-sha256:{}", &source_payload_hash[7..31]),
        "run_id": state["run_id"],
        "phase": 2,
        "role": "rust.phase2_final_reducer",
        "profile": "rust_phase2_final_reducer",
        "unit_key": "phase2:final-reducer:aggregate",
        "source_payload_hash": source_payload_hash,
        "evidence_refs": [],
        "controllers": controllers,
        "created_at": Utc::now().to_rfc3339(),
        "content_hash": "",
    });
    let mut reducer = reducer;
    reducer["content_hash"] = json!(content_hash(&reducer)?);
    store.write_json_value(
        &location.child_relative(Path::new("artifacts/phase2/final-reducer.json"))?,
        &reducer,
    )?;
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
    state["research_plan"] = artifact;
    Ok(())
}

async fn run_phase4(
    state: &mut Value,
    runtime: &RuntimeConfig,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<()> {
    let mut per_ticker = serde_json::Map::new();
    for ticker in tickers_from_state(state) {
        let artifact = run_unit(
            state,
            runtime,
            "trader",
            4,
            "artifact",
            None,
            None,
            Some(&ticker),
            model,
            reasoning,
        )
        .await?;
        per_ticker.insert(ticker, artifact);
    }
    state["trader_investment_plan"] = json!({"per_ticker": per_ticker});
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
        for ticker in tickers_from_state(state) {
            history.push(
                run_unit(
                    state,
                    runtime,
                    role,
                    5,
                    "artifact",
                    None,
                    None,
                    Some(&ticker),
                    model,
                    reasoning,
                )
                .await?,
            );
        }
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
    let assets = if runtime.allocation.investable_assets.is_empty() {
        tickers_from_state(state)
    } else {
        runtime.allocation.investable_assets.clone()
    };
    let mut per_asset = serde_json::Map::new();
    for ticker in assets {
        let artifact = run_unit(
            state,
            runtime,
            "portfolio.manager",
            6,
            "artifact",
            None,
            None,
            Some(&ticker),
            model,
            reasoning,
        )
        .await?;
        per_asset.insert(ticker, artifact);
    }
    state["final_trade_decision"] = json!({"per_asset": per_asset, "authority": "file_store"});
    Ok(())
}

fn run_phase7(
    store: &FileStore,
    location: &RunLocation,
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
    let source_payload_hash = content_hash(&json!({
        "allocation_context": state["allocation_context"],
        "portfolio_allocation": allocation,
    }))?;
    let artifact_id = format!(
        "artifact-sha256:{}",
        source_payload_hash
            .strip_prefix("sha256:")
            .unwrap_or(&source_payload_hash)
            .chars()
            .take(24)
            .collect::<String>()
    );
    let mut artifact = json!({
        "schema_version": 1,
        "artifact_id": artifact_id,
        "run_id": state["run_id"],
        "phase": 7,
        "role": "rust.allocation",
        "profile": "rust_allocation",
        "unit_key": "phase7:allocation:aggregate",
        "source_payload_hash": source_payload_hash,
        "evidence_refs": [],
        "allocation": allocation,
        "created_at": Utc::now().to_rfc3339(),
        "content_hash": "",
    });
    let hash = content_hash(&artifact)?;
    artifact["content_hash"] = Value::String(hash);
    store.write_json_value(
        &location.child_relative(Path::new("artifacts/phase7/allocation.json"))?,
        &artifact,
    )?;
    state["allocation_artifact"] = artifact.clone();
    state["allocation_result"] = artifact.clone();
    Ok(artifact)
}

fn record_manifest_artifact(
    manifest: &mut RunManifest,
    artifact: &Value,
    relative_path: &Path,
) -> Result<()> {
    let required = |field: &str| {
        artifact
            .get(field)
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .with_context(|| format!("canonical artifact {field} is required"))
    };
    let phase = artifact
        .get("phase")
        .and_then(Value::as_u64)
        .and_then(|value| u8::try_from(value).ok())
        .context("canonical artifact phase is required")?;
    manifest.record_finalized_artifact(FinalizedArtifactRef::new(
        required("artifact_id")?,
        relative_path,
        phase,
        required("role")?,
        required("profile")?,
        required("unit_key")?,
        required("source_payload_hash")?,
        required("created_at")?,
    )?)?;
    Ok(())
}

fn run_phase8(store: &FileStore, location: &RunLocation, state: &mut Value) -> Result<()> {
    for ticker in tickers_from_state(state) {
        let record = LearningRecord {
            schema_version: orchestrator_store::LEARNING_RECORD_SCHEMA_VERSION,
            kind: LearningKind::Decision,
            run_id: state["run_id"].as_str().unwrap_or_default().to_owned(),
            ticker,
            source_run_id: None,
            payload: json!({"portfolio_allocation": state["portfolio_allocation"], "phase": 8}),
            created_at: Utc::now().to_rfc3339(),
            content_hash: String::new(),
        };
        write_learning_record(store, location, LearningKind::Decision, record)?;
    }
    state["phase8"] = json!({"status": "completed", "archive": "file_store"});
    Ok(())
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
    let mut scoped = state.clone();
    if let Some(ticker) = ticker {
        scoped["ticker"] = json!(ticker);
        scoped["tickers"] = json!([ticker]);
    }
    let prompt_path = runtime.prompts.path_for(role).cloned();
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
    let (artifact, degraded_terminal) = match result.artifact.clone() {
        Some(artifact) => (artifact, None),
        None => {
            let (artifact, session_id, turn_id) = finalize_degraded_tool_managed_unit(
                state, runtime, role, phase, kind, round, topic_id, ticker, &result,
            )?;
            (artifact, Some((session_id, turn_id)))
        }
    };
    let (session_id, turn_id) = if let Some(identity) = degraded_terminal {
        identity
    } else {
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
            "mock-finalize".to_owned()
        } else {
            result.turn_id
        };
        (session_id, turn_id)
    };
    state["_runtime_sessions"][runtime_session_key(role, kind, topic_id, round)] =
        json!({"session_id": session_id, "turn_id": turn_id});
    state["_completed_units"][completed_key] = artifact.clone();
    checkpoint_state(state)?;
    Ok(artifact)
}

/// A terminal ToolManaged failure is not permission to revive an old storage
/// path.  It is finalized through the same FileStore Draft/Builder service as
/// an ordinary role, marked degraded, and recorded as a terminal session
/// event so recovery and Store Doctor see one authority.
#[allow(clippy::too_many_arguments)]
fn finalize_degraded_tool_managed_unit(
    state: &mut Value,
    runtime: &RuntimeConfig,
    role: &str,
    phase: i64,
    kind: &str,
    round: Option<i64>,
    topic_id: Option<&str>,
    ticker: Option<&str>,
    result: &crate::orchestration::role_jobs::RoleJobResult,
) -> Result<(Value, String, String)> {
    let failure = result
        .error
        .as_deref()
        .unwrap_or("ToolManaged role ended without terminal finalize");
    let profile = match (phase, role) {
        (1, "analyst.technical" | "analyst.news_macro") => ToolManagedProfile::AnalystReport,
        (2, "mediator.topic") if kind == "warmup" => ToolManagedProfile::ResearcherWarmup,
        (2, "mediator.topic") if kind == "topic_generation" => ToolManagedProfile::TopicGeneration,
        (2, "researcher.bull.initial" | "researcher.bear.initial") => {
            ToolManagedProfile::DebateSeed
        }
        (2, "researcher.bull.interaction" | "researcher.bear.interaction") => {
            ToolManagedProfile::DebateResponse
        }
        (2, "mediator.topic_controller") => ToolManagedProfile::TopicControl,
        (3, "manager.research") => ToolManagedProfile::ResearchDecision,
        (4, "trader") => ToolManagedProfile::TradeIntent,
        (5, "risk.aggressive" | "risk.neutral" | "risk.conservative") => {
            ToolManagedProfile::RiskReview
        }
        (6, "portfolio.manager") => ToolManagedProfile::PortfolioDecision,
        _ => {
            bail!(
                "ToolManaged role {role} ended without terminal finalize: {failure}; no Rust degraded policy is registered for phase={phase} kind={kind}"
            )
        }
    };
    let registration = runtime.role_profile_registry.registration(role, profile)?;
    let tickers = ticker
        .map(|ticker| vec![ticker.to_owned()])
        .unwrap_or_else(|| tickers_from_state(state));
    let trade_candidate_action = tickers.first().and_then(|ticker| {
        state
            .pointer(&format!("/research_plan/per_ticker/{ticker}/rating"))
            .or_else(|| state.pointer("/research_plan/rating"))
            .and_then(Value::as_str)
            .map(|rating| match rating {
                "Buy" | "Overweight" => "Buy",
                "Sell" | "Underweight" => "Sell",
                _ => "Hold",
            })
            .map(ToOwned::to_owned)
    });
    let portfolio_rating = tickers.first().and_then(|ticker| {
        state
            .pointer(&format!("/research_plan/per_ticker/{ticker}/rating"))
            .or_else(|| state.pointer("/research_plan/rating"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    });
    let portfolio_current_weight = tickers
        .first()
        .and_then(|ticker| {
            state
                .pointer(&format!("/account/positions/{ticker}/weight"))
                .or_else(|| state.pointer(&format!("/current_portfolio_weights/{ticker}")))
                .and_then(Value::as_f64)
        })
        .or(Some(0.0));
    let plan = FileStoreDomainRuntimePlan {
        role: role.to_owned(),
        phase,
        profile,
        profile_version: registration.profile_version,
        builder_version: registration.builder_version,
        tickers,
        visible_evidence_refs: Default::default(),
        topic_id: topic_id.map(ToOwned::to_owned),
        side: phase2_side_for_role(role).map(ToOwned::to_owned),
        round: round.and_then(|round| u32::try_from(round).ok()),
        visible_claims: visible_phase2_claims(state, topic_id),
        fork: phase2_fork_reference(state, role, topic_id),
        trade_candidate_action,
        portfolio_rating,
        portfolio_current_weight,
    };
    let store_root = state
        .get("store_root")
        .and_then(Value::as_str)
        .context("degraded ToolManaged fallback requires store_root")?;
    let fallback_fork = plan.fork.clone();
    let artifact = match profile {
        ToolManagedProfile::ResearcherWarmup
        | ToolManagedProfile::TopicGeneration
        | ToolManagedProfile::DebateSeed
        | ToolManagedProfile::DebateResponse
        | ToolManagedProfile::TopicControl => {
            finalize_degraded_phase2(Path::new(store_root), state, plan, failure)?
        }
        ToolManagedProfile::AnalystReport => {
            finalize_degraded_analyst_report(Path::new(store_root), state, plan, failure)?
        }
        ToolManagedProfile::ResearchDecision => {
            finalize_degraded_research_decision(Path::new(store_root), state, plan, failure)?
        }
        ToolManagedProfile::TradeIntent => {
            finalize_degraded_trade_intent(Path::new(store_root), state, plan, failure)?
        }
        ToolManagedProfile::RiskReview => {
            finalize_degraded_risk_review(Path::new(store_root), state, plan, failure)?
        }
        ToolManagedProfile::PortfolioDecision => {
            finalize_degraded_portfolio_decision(Path::new(store_root), state, plan, failure)?
        }
        _ => unreachable!("only profiles with a Rust degraded policy reach this branch"),
    };
    let (terminal_session_id, terminal_turn_id) = persist_degraded_terminal(
        state,
        role,
        phase,
        profile,
        &result.session_id,
        &result.turn_id,
        fallback_fork,
        &artifact,
    )?;
    state["degraded"] = Value::Bool(true);
    if !state["errors"].is_array() {
        state["errors"] = json!([]);
    }
    state["errors"]
        .as_array_mut()
        .expect("errors is an array after initialization")
        .push(json!({"role": role, "phase": phase, "kind": kind, "failure": failure}));
    Ok((artifact, terminal_session_id, terminal_turn_id))
}

#[allow(clippy::too_many_arguments)]
fn persist_degraded_terminal(
    state: &Value,
    role: &str,
    phase: i64,
    profile: ToolManagedProfile,
    session_id: &str,
    turn_id: &str,
    fork: Option<orchestrator_store::ForkReference>,
    artifact: &Value,
) -> Result<(String, String)> {
    let run_id = state["run_id"]
        .as_str()
        .context("degraded terminal requires run_id")?;
    let date = state["current_date"]
        .as_str()
        .context("degraded terminal requires current_date")?;
    let store_root = state["store_root"]
        .as_str()
        .context("degraded terminal requires store_root")?;
    let session_id = if session_id.is_empty() {
        format!("{run_id}:p{phase}:{role}:{}:degraded", profile.as_str())
    } else {
        session_id.to_owned()
    };
    let turn_id = if turn_id.is_empty() {
        "rust-degraded-finalize".to_owned()
    } else {
        format!("{turn_id}:rust-degraded-finalize")
    };
    let session = FileStoreSessionRuntime::create_or_load(
        FileStore::open(store_root, FileStoreOptions::default())?,
        SessionRuntimeSpec {
            run: RunLocation::new(date, run_id)?,
            session_id: session_id.clone(),
            role: role.to_owned(),
            phase: u8::try_from(phase).context("degraded terminal phase must fit u8")?,
            profile: profile.as_str().to_owned(),
            fork,
            created_at: Utc::now().to_rfc3339(),
        },
    )?;
    let mut turn = Turn::new(&turn_id, &session_id, run_id, role, "");
    turn.phase = Some(phase);
    let terminal = ToolResultItem {
        call_id: "rust-degraded-finalize".to_owned(),
        name: format!("finalize_degraded_{}", profile.as_str()),
        status: "completed".to_owned(),
        output: json!({"artifact": artifact, "status": "completed", "terminal": true, "degraded": true}),
        error: None,
    };
    session.append_terminal(&turn, &terminal, Utc::now().to_rfc3339())?;
    Ok((session_id, turn_id))
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

fn phase2_side_for_role(role: &str) -> Option<&'static str> {
    if role.contains(".bull.") {
        Some("bull")
    } else if role.contains(".bear.") {
        Some("bear")
    } else {
        None
    }
}

fn visible_phase2_claims(state: &Value, topic_id: Option<&str>) -> BTreeSet<String> {
    let Some(topic_id) = topic_id else {
        return BTreeSet::new();
    };
    state
        .pointer(&format!("/topic_debate_states/{topic_id}/turns"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(|turn| {
            turn.pointer("/artifact/payload/claims")
                .or_else(|| turn.pointer("/artifact/claims"))
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
        .filter_map(|claim| claim.get("claim_id").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect()
}

fn phase2_fork_reference(
    state: &Value,
    role: &str,
    topic_id: Option<&str>,
) -> Option<orchestrator_store::ForkReference> {
    let (fork_from_session_id, fork_from_turn_id) = if role == "mediator.topic_controller" {
        let _ = topic_id?;
        (
            state
                .get("topic_generation_session_id")?
                .as_str()?
                .to_owned(),
            state.get("topic_generation_turn_id")?.as_str()?.to_owned(),
        )
    } else if matches!(role, "researcher.bull.initial" | "researcher.bear.initial") {
        let _ = topic_id?;
        let warmup = state.get("phase2_warmup")?;
        (
            warmup.get("session_id")?.as_str()?.to_owned(),
            warmup.get("turn_id")?.as_str()?.to_owned(),
        )
    } else if matches!(
        role,
        "researcher.bull.interaction" | "researcher.bear.interaction"
    ) {
        let topic_id = topic_id?;
        let side = phase2_side_for_role(role)?;
        let source = state.pointer(&format!("/phase2_file_store_sessions/{topic_id}/{side}"))?;
        (
            source.get("session_id")?.as_str()?.to_owned(),
            source.get("turn_id")?.as_str()?.to_owned(),
        )
    } else {
        return None;
    };
    Some(orchestrator_store::ForkReference {
        fork_from_session_id,
        fork_from_turn_id,
    })
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

fn phase1_index(state: &Value) -> serde_json::Map<String, Value> {
    tickers_from_state(state)
        .into_iter()
        .map(|ticker| {
            let roles = state["analyst_reports"]
                .as_object()
                .map(|reports| {
                    reports
                        .iter()
                        .filter_map(|(role, report)| {
                            report
                                .pointer(&format!("/per_ticker/{ticker}"))
                                .map(|value| json!({"role": role, "artifact": value}))
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            (
                ticker,
                json!({"role_summaries": roles, "evidence_quality": "tool_managed"}),
            )
        })
        .collect()
}

fn weighted_probability_base(state: &Value) -> Value {
    let values = tickers_from_state(state)
        .into_iter()
        .map(|ticker| (ticker, json!({"long_probability": 0.5, "short_probability": 0.5, "source": "phase1_tool_managed"})))
        .collect::<serde_json::Map<_, _>>();
    Value::Object(values)
}

fn seal_state(state: &mut Value) -> Result<()> {
    state["content_hash"] = Value::String(String::new());
    state["content_hash"] = Value::String(content_hash(state)?);
    Ok(())
}

#[cfg(test)]
mod phase2_session_tests {
    use orchestrator_store::{PhaseStatus, RunLocation, RunManifest, RunManifestInit};
    use serde_json::json;

    use super::{
        highest_completed_phase, phase2_fork_reference, record_phase2_session, runtime_session_key,
        sync_manifest_health,
    };

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
    fn degraded_phase2_seed_uses_the_same_immutable_warmup_fork() {
        let state = json!({
            "phase2_warmup": {
                "session_id": "warmup-session",
                "turn_id": "warmup-turn"
            }
        });

        let fork = phase2_fork_reference(&state, "researcher.bull.initial", Some("topic-1"))
            .expect("seed requires the completed warmup fork");

        assert_eq!(fork.fork_from_session_id, "warmup-session");
        assert_eq!(fork.fork_from_turn_id, "warmup-turn");
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
}
