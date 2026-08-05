use anyhow::{bail, Context, Error, Result};
use chrono::{Local, NaiveDate, Utc};
use futures::{stream, StreamExt};
use orchestrator_core::evaluation::RiskDecision;
use orchestrator_core::{
    config_get, config_int, config_str, config_strings, default_project_root, display_ticker,
    load_config, parse_tickers, project_path, research_rating_for_probability,
    validate_analyst_ticker_artifact, validate_asset_execution_constraint,
    validate_research_decision, validate_risk_constraints, validate_trade_intent,
    AllocationDecision, AnalystTickerArtifact, AssetExecutionConstraint, BenchmarkBindingV1,
    BenchmarkSelectionV1, DecisionSection, DecisionSectionUnavailableReason, DecisionSnapshotV2,
    DocumentRef, EvaluationSpec, ExecutionOutcome, ExecutionPlan, ExecutionPlanStatus,
    ForecastDirection, MemoryPolicyV1, MemoryUsageReferenceStatus, OutcomeRecordV1, OutcomeSection,
    PersistenceContextV1, PersistenceNamespace, PolicyRef, ReflectionTaskStatus, ResearchDecision,
    RiskConstraints, RunPurpose, StopType, ThesisDecision, TradeAction, TradeDecision, TradeIntent,
    DECISION_SNAPSHOT_SCHEMA_VERSION,
};
use orchestrator_ingest::{jin10, technical};
use orchestrator_store::{
    append_index_detail, canonical_json_bytes, content_hash, content_hash_bytes, create_index,
    finalize_index, read_all_indexes, read_index_details, read_indexes,
    read_input_snapshot_manifest, read_run_manifest, write_run_manifest, AppendIndexDetailInput,
    CreateIndexInput, DetailQuery, DetailSection, EvaluationStore, FileSchemaKind, FileStore,
    FileStoreOptions, Index, IndexArchive, IndexKind, IndexQuery, IndexScope, ManifestError,
    RunCompactionMode, RunLocation, RunManifest, RunManifestInit, RunStatus, RunStore,
};
use serde_json::{json, Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use crate::evaluation::{materialize_pending, MarketInputConfigV1, MaterializerPolicyV1};
use crate::orchestration::{
    allocation::{
        cash_only_allocation, compute_allocation_context, derive_guarded_allocation,
        market_snapshot_from_technical,
    },
    config::RuntimeConfig,
    execution::{
        build_order_plan, credentials as alpaca_credentials, debug_account_snapshot,
        load_alpaca_account_snapshot, submit_order_plan, AccountSnapshot, ExecutionReport,
    },
    input_snapshot_runtime::{capture_phase1_file_store_inputs, phase1_input_sources},
    lifecycle::{
        debug_run_id_for, investable_assets_from_state, research_plan_to_trade_intent, run_id_for,
        run_id_for_seed, run_location_from_state, set_phase_status, tickers_from_state,
        validate_asset_scope,
    },
    role_jobs::{
        commit_historical_reflection, prepare_role_job, record_role_job_metrics,
        refresh_role_job_metrics, run_role_jobs, RoleRun,
    },
    summary_store::{
        parse_phase_index_candidate, write_compiled_phase_index, PhaseIndexCandidate,
        PhaseIndexCandidateDetail,
    },
    summary_units::derive_summary_index_id,
    topic_debate_tree::{DebateActor, TopicDebateTree},
};

mod args;
mod finalization;
mod gates;
pub use args::*;

#[cfg(test)]
use finalization::{
    decision_snapshot, final_decision_payload, finalized_phase_index, risk_decision_from_index,
};
use finalization::{evaluation_persistence_context, run_phase8};
use gates::{
    has_phase3_retrieval_audit, has_required_phase_summaries, highest_completed_phase,
    phase1_summaries_visible_to_phase3, phase_completed, phase_summary_visible_to_phase,
    required_phase_index_count, retrieval_audit_covers_required_source_phases,
    validate_phase_range,
};

const STATE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone)]
struct RunFailureContext {
    store: FileStore,
    location: RunLocation,
}

#[derive(Debug, Clone)]
struct UnitSpec {
    role: String,
    phase: i64,
    kind: String,
    round: Option<i64>,
    topic_id: Option<String>,
    ticker: Option<String>,
}

pub async fn run(args: ExecArgs) -> Result<Value> {
    let mut failure_context = None;
    let result = run_inner(args, &mut failure_context).await;
    if let Err(error) = &result {
        record_run_failure(failure_context.as_ref(), error);
    }
    result
}

async fn run_inner(
    args: ExecArgs,
    failure_context: &mut Option<RunFailureContext>,
) -> Result<Value> {
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
    // Debug is one explicit, reusable workspace per ticker universe. Unlike
    // Mock/Paper/Live it must not vary with calendar date or config hash:
    // `orchestrator-exec --debug --from-phase X` must reopen its exact Index.
    let run_id = if args.debug {
        debug_run_id_for(&tickers)
    } else if args.mock {
        let config_hash = content_hash(&config)?;
        run_id_for_seed(&tickers, &current_date, &format!("mock\x1f{config_hash}"))
    } else {
        canonical_run_id
    };
    let location = if args.debug {
        RunLocation::debug(current_date.clone(), run_id.clone())?
    } else {
        RunLocation::new(current_date.clone(), run_id.clone())?
    };
    let store = FileStore::open(
        &store_root,
        FileStoreOptions {
            atomic_fsync: runtime.store.atomic_fsync,
            stale_temp_age: Some(Duration::from_secs(runtime.store.stale_temp_age_sec)),
        },
    )?;
    let run_store = RunStore::new(store.clone(), location.clone());
    let mut manifest = prepare_manifest(&store, &location, &runtime, &config)?;
    *failure_context = Some(RunFailureContext {
        store: store.clone(),
        location: location.clone(),
    });

    let initial_state = json!({
        "schema_version": STATE_SCHEMA_VERSION,
        "run_id": run_id,
        "current_date": current_date,
        "ticker": display_ticker(&tickers),
        "tickers": tickers,
        "analysis_universe": tickers,
        "investable_assets": runtime.allocation.investable_assets,
        "store_root": store.root(),
        "config": redacted_config_for_state(&config),
        "mode": args.mode.as_str(),
        "lang": if args.lang == "zh" { config_str(&config, "orchestrator.runtime.lang", "zh") } else { args.lang.clone() },
        "window_days": args.window_days.unwrap_or_else(|| config_int(&config, "orchestrator.runtime.window_days", 150)),
        "mock": args.mock,
        "debug": args.debug,
        "storage_namespace": if args.debug { Value::String("debug".to_owned()) } else { Value::Null },
        "phase_status": {},
        "degraded": false,
    });
    let state_was_missing = !store.exists(&location.state_relative())?;
    let mut state = load_or_initialize_state(&store, &location, initial_state)?;
    let mut state_was_rehydrated = false;
    if args.debug
        && manifest.status == RunStatus::Completed
        && state
            .get("phase_status")
            .and_then(Value::as_object)
            .is_some_and(Map::is_empty)
    {
        state_was_rehydrated =
            rehydrate_completed_debug_state(&store, &location, &manifest, &mut state)?;
    }
    if rehydrate_completed_phase_projections(&store, &location, &manifest, &mut state)? {
        state_was_rehydrated = true;
        persist_state(&mut state)?;
    }
    state["max_debate_rounds"] = json!(args.max_debate_rounds.unwrap_or_else(|| config_int(
        &config,
        "orchestrator.runtime.max_debate_rounds",
        3
    )));
    state["max_topics_per_side"] = json!(args.max_topics_per_side.unwrap_or_else(|| config_int(
        &config,
        "orchestrator.runtime.max_topics_per_side",
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
    let phase1_visibility_missing =
        !args.mock && !phase1_summaries_visible_to_phase3(&store, &location, &runtime)?;
    let phase1_needs_run = args.from_phase <= 1
        && args.to_phase >= 1
        && (!phase_completed(&manifest, 1)
            || !has_required_phase_summaries(&store, &location, &state, &runtime, 1)?
            || phase1_visibility_missing);
    if phase1_needs_run {
        if phase1_visibility_missing {
            // Existing debug Stores may contain cached Phase 1 artifacts
            // produced before Phase 3 was an allowed consumer. Recompute the
            // two role summaries so the corrected visibility contract is
            // persisted through the normal Index writer.
            state["_force_phase1_recompute"] = Value::Bool(true);
        }
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
        state
            .as_object_mut()
            .map(|object| object.remove("_force_phase1_recompute"));
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
    // Completed Debug runs may intentionally retain only the manifest and
    // finalized Index archives after an older compaction. The Indexes are the
    // semantic authority in that state; do not reopen Phase 3 merely because
    // its transient retrieval metrics were compacted with state.json.
    let state_is_completed_projection = phase_completed(&manifest, 8)
        && state.get("final_trade_decision").is_some()
        && state.get("role_job_metrics").is_none();
    let state_was_compacted = ((state_was_missing || state_was_rehydrated)
        && phase_completed(&manifest, 8))
        || state_is_completed_projection;
    let phase3_retrieval_audit_missing =
        !args.mock && !state_was_compacted && !has_phase3_retrieval_audit(&state);
    let phase3_visibility_missing =
        !args.mock && !phase_summary_visible_to_phase(&store, &location, &runtime, 3, 6, 1)?;
    if args.from_phase <= 3
        && args.to_phase >= 3
        && (!phase_completed(&manifest, 3)
            || !has_required_phase_summaries(&store, &location, &state, &runtime, 3)?
            || phase3_retrieval_audit_missing
            || phase3_visibility_missing)
    {
        if phase3_visibility_missing {
            state["_force_phase3_recompute"] = Value::Bool(true);
        }
        run_phase3(
            &mut state,
            &runtime,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        state
            .as_object_mut()
            .map(|object| object.remove("_force_phase3_recompute"));
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
    let phase4_visibility_missing =
        !args.mock && !phase_summary_visible_to_phase(&store, &location, &runtime, 4, 6, 1)?;
    if args.from_phase <= 4
        && args.to_phase >= 4
        && (!phase_completed(&manifest, 4)
            || !has_required_phase_summaries(&store, &location, &state, &runtime, 4)?
            || phase4_visibility_missing)
    {
        if phase4_visibility_missing {
            state["_force_phase4_recompute"] = Value::Bool(true);
        }
        run_phase4(
            &mut state,
            &runtime,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        state
            .as_object_mut()
            .map(|object| object.remove("_force_phase4_recompute"));
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
    let phase5_visibility_missing =
        !args.mock && !phase_summary_visible_to_phase(&store, &location, &runtime, 5, 6, 3)?;
    if args.from_phase <= 5
        && args.to_phase >= 5
        && (!phase_completed(&manifest, 5)
            || !has_required_phase_summaries(&store, &location, &state, &runtime, 5)?
            || phase5_visibility_missing)
    {
        if phase5_visibility_missing {
            state["_force_phase5_recompute"] = Value::Bool(true);
        }
        run_phase5(
            &mut state,
            &runtime,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        state
            .as_object_mut()
            .map(|object| object.remove("_force_phase5_recompute"));
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
        if !args.mock {
            ensure_execution_account_snapshot(&mut state, &runtime, &args).await?;
        }
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
        if !args.mock {
            ensure_execution_account_snapshot(&mut state, &runtime, &args).await?;
        }
        run_phase7(&store, &location, &mut state, &runtime, &args).await?;
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
    let run_completed = phase_completed(&manifest, 8);
    manifest.artifacts.clear();
    manifest.status = if run_completed {
        RunStatus::Completed
    } else {
        RunStatus::Running
    };
    sync_manifest_health(&mut manifest, &state);
    if let Some(phase) = highest_completed_phase(&manifest) {
        manifest.current_phase = phase;
    }
    manifest.completed_at = run_completed.then(|| Utc::now().to_rfc3339());
    write_run_manifest(&store, &location, manifest)?;
    persist_state(&mut state)?;
    let store_compaction = if run_completed {
        match run_store.compact_completed_run(RunCompactionMode::Apply) {
            Ok(report) => serde_json::to_value(report)?,
            Err(error) => {
                tracing::warn!(error = %error, "completed-run store compaction failed");
                json!({
                    "eligible": true,
                    "applied": false,
                    "failure": error.to_string(),
                })
            }
        }
    } else {
        json!({
            "eligible": false,
            "applied": false,
        })
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
        "order_plan": state.get("order_plan").cloned().unwrap_or(Value::Null),
        "execution_report": state.get("execution_report").cloned().unwrap_or(Value::Null),
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
    validate_phase_range(args)?;
    if args
        .max_debate_rounds
        .is_some_and(|rounds| !(0..=10).contains(&rounds))
    {
        bail!("max_debate_rounds must be in 0..=10")
    }
    if args
        .max_topics_per_side
        .is_some_and(|topics| !(1..=20).contains(&topics))
    {
        bail!("max_topics_per_side must be in 1..=20")
    }
    if args.submit_orders && (args.mock || args.debug) {
        bail!("--submit-orders is only valid for a non-mock, non-debug Paper run")
    }
    if args.submit_orders
        && args
            .run_purpose
            .is_some_and(|purpose| !matches!(purpose, crate::exec::args::RunPurposeArg::Paper))
    {
        bail!("--submit-orders requires --run-purpose paper when --run-purpose is set")
    }
    Ok(())
}

fn prepare_manifest(
    store: &FileStore,
    location: &RunLocation,
    runtime: &RuntimeConfig,
    config: &Value,
) -> Result<RunManifest> {
    let workflow_version = format!("orchestrator-workflow-v{}", env!("CARGO_PKG_VERSION"));
    let prompt_content_hash = prompt_surface_hash(runtime)?;
    let project_root = default_project_root();
    let git_sha = resolve_git_sha(&project_root)?;
    let source_surface_hash = workflow_source_surface_hash(&project_root)?;
    let config_hash = content_hash(config)?;
    let role_profile_registry_hash = runtime.role_profile_registry.snapshot().content_hash;
    if store.exists(&location.manifest_relative())? {
        let manifest = read_run_manifest(store, location)?;
        let mut mismatches = Vec::new();
        for (field, expected, actual) in [
            (
                "workflow_version",
                workflow_version.as_str(),
                manifest.workflow_version.as_str(),
            ),
            (
                "config_hash",
                config_hash.as_str(),
                manifest.config_hash.as_str(),
            ),
            ("git_sha", git_sha.as_str(), manifest.git_sha.as_str()),
            (
                "source_surface_hash",
                source_surface_hash.as_str(),
                manifest.source_surface_hash.as_str(),
            ),
            (
                "role_profile_registry_hash",
                role_profile_registry_hash.as_str(),
                manifest.role_profile_registry_hash.as_str(),
            ),
            (
                "prompt_content_hash",
                prompt_content_hash.as_str(),
                manifest.prompt_content_hash.as_str(),
            ),
        ] {
            if expected != actual {
                mismatches.push(format!("{field}: stored={actual}, current={expected}"));
            }
        }
        if manifest.prompt_versions != runtime.prompts.versions {
            mismatches.push("prompt_versions differ".to_owned());
        }
        if !mismatches.is_empty() {
            bail!(
                "run {} identity differs from the current workflow ({}); refusing to reuse sealed artifacts. Use a distinct --store-root or intentionally recreate the isolated run instead of mixing evidence, prompts, configuration, or code",
                manifest.run_id,
                mismatches.join("; ")
            );
        }
        return Ok(manifest);
    }
    write_run_manifest(
        store,
        location,
        RunManifest::new(RunManifestInit {
            location: location.clone(),
            workflow_version,
            prompt_versions: runtime.prompts.versions.clone(),
            prompt_content_hash,
            source_surface_hash,
            git_sha,
            config_hash,
            role_profile_registry_hash,
            created_at: Utc::now().to_rfc3339(),
        })?,
    )
    .map_err(Into::into)
}

/// Return a content hash for every template file that can influence the
/// runtime renderer.  Version labels alone are deliberately insufficient:
/// an in-place prompt edit must invalidate a resumable run just as a config
/// or source-code revision does.  Hashing the non-archived prompt tree is
/// conservative, but it prevents a changed shared fragment from slipping
/// through merely because no role path changed.
fn prompt_surface_hash(runtime: &RuntimeConfig) -> Result<String> {
    let mut role_templates = BTreeMap::new();
    let mut prompt_roots = BTreeSet::new();
    for (role, path) in &runtime.prompts.prompts {
        let bytes = std::fs::read(path)
            .with_context(|| format!("failed to hash prompt template {}", path.display()))?;
        role_templates.insert(
            role.clone(),
            json!({
                "path": path.to_string_lossy(),
                "content_hash": content_hash_bytes(&bytes),
            }),
        );
        if let Some(root) = prompts_root_for_path(path) {
            prompt_roots.insert(root);
        }
    }
    let mut prompt_files = BTreeMap::new();
    for root in prompt_roots {
        collect_prompt_surface_files(&root, &root, &mut prompt_files)?;
    }
    let components = runtime
        .component_plugins
        .components
        .iter()
        .map(|(name, plugin)| {
            Ok((
                name.clone(),
                json!({
                    "path": plugin.path.to_string_lossy(),
                    "content_hash": content_hash(&json!({
                        "manifest": &plugin.manifest,
                        "template": &plugin.template,
                    }))?,
                }),
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    Ok(content_hash(&json!({
        "role_templates": role_templates,
        "prompt_files": prompt_files,
        "components": components,
    }))?)
}

/// Pin the actual executable Rust/Cargo surface, including local dirty edits.
/// `git_sha` identifies the committed base only; Debug recovery must not reuse
/// a partially completed run after a different uncommitted binary is built.
/// Prompt and runtime config have separate hashes, so this deliberately covers
/// only the workspace code/dependency manifests.
fn workflow_source_surface_hash(project_root: &Path) -> Result<String> {
    let worktree_root = git_worktree_root(project_root)?;
    workflow_source_surface_hash_at(&worktree_root)
}

fn git_worktree_root(project_root: &Path) -> Result<PathBuf> {
    let output = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(project_root)
        .output()
        .with_context(|| {
            format!(
                "failed to resolve git worktree in {}",
                project_root.display()
            )
        })?;
    if !output.status.success() {
        bail!(
            "git rev-parse --show-toplevel failed in {}: {}",
            project_root.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        )
    }
    let root = PathBuf::from(
        String::from_utf8(output.stdout)
            .context("git rev-parse --show-toplevel returned non-UTF-8 output")?
            .trim(),
    );
    if !root.join("Cargo.toml").is_file() {
        bail!(
            "git worktree {} does not contain the Rust workspace Cargo.toml",
            root.display()
        )
    }
    Ok(root)
}

fn workflow_source_surface_hash_at(worktree_root: &Path) -> Result<String> {
    let mut files = BTreeMap::new();
    for relative in [
        "Cargo.toml",
        "Cargo.lock",
        "build.rs",
        "rust-toolchain.toml",
    ] {
        let path = worktree_root.join(relative);
        if path.is_file() {
            files.insert(
                relative.to_owned(),
                content_hash_bytes(
                    &std::fs::read(&path).with_context(|| {
                        format!("failed to hash source file {}", path.display())
                    })?,
                ),
            );
        }
    }
    let crates_root = worktree_root.join("crates");
    if !crates_root.is_dir() {
        bail!(
            "Rust workspace source surface is missing crates directory at {}",
            crates_root.display()
        )
    }
    collect_workflow_source_surface_files(worktree_root, &crates_root, &mut files)?;
    if files.is_empty() {
        bail!("Rust workspace source surface is empty")
    }
    Ok(content_hash(&json!({
        "authority": "rust_workspace_source_surface_v1",
        "files": files,
    }))?)
}

fn collect_workflow_source_surface_files(
    worktree_root: &Path,
    directory: &Path,
    output: &mut BTreeMap<String, String>,
) -> Result<()> {
    let mut entries = std::fs::read_dir(directory)
        .with_context(|| {
            format!(
                "failed to enumerate source directory {}",
                directory.display()
            )
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            bail!(
                "source surface refuses symlinked workspace file {}",
                path.display()
            )
        }
        if file_type.is_dir() {
            collect_workflow_source_surface_files(worktree_root, &path, output)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        let relative = path
            .strip_prefix(worktree_root)
            .expect("source file remains beneath resolved worktree")
            .to_string_lossy()
            .to_string();
        let bytes = std::fs::read(&path)
            .with_context(|| format!("failed to hash source file {}", path.display()))?;
        output.insert(relative, content_hash_bytes(&bytes));
    }
    Ok(())
}

fn prompts_root_for_path(path: &Path) -> Option<PathBuf> {
    path.ancestors().find_map(|ancestor| {
        (ancestor.file_name().and_then(|name| name.to_str()) == Some("prompts"))
            .then(|| ancestor.to_path_buf())
    })
}

fn collect_prompt_surface_files(
    root: &Path,
    directory: &Path,
    output: &mut BTreeMap<String, String>,
) -> Result<()> {
    for entry in std::fs::read_dir(directory).with_context(|| {
        format!(
            "failed to enumerate prompt directory {}",
            directory.display()
        )
    })? {
        let entry = entry?;
        let name = entry.file_name();
        if name == "_archive" {
            continue;
        }
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_prompt_surface_files(root, &path, output)?;
        } else if file_type.is_file() {
            let relative = path
                .strip_prefix(root)
                .expect("prompt file remains beneath its root")
                .to_string_lossy()
                .to_string();
            let bytes = std::fs::read(&path)
                .with_context(|| format!("failed to hash prompt file {}", path.display()))?;
            output.insert(relative, content_hash_bytes(&bytes));
        }
    }
    Ok(())
}

fn resolve_git_sha(project_root: &Path) -> Result<String> {
    if let Some(sha) = option_env!("GIT_SHA")
        .map(str::trim)
        .filter(|sha| is_git_sha(sha))
    {
        return Ok(sha.to_owned());
    }
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(project_root)
        .output()
        .with_context(|| format!("failed to resolve git SHA in {}", project_root.display()))?;
    if !output.status.success() {
        bail!(
            "git rev-parse HEAD failed in {}: {}",
            project_root.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        )
    }
    let sha = String::from_utf8(output.stdout)
        .context("git rev-parse HEAD returned non-UTF-8 output")?
        .trim()
        .to_owned();
    if !is_git_sha(&sha) {
        bail!("git rev-parse HEAD returned invalid SHA {sha:?}")
    }
    Ok(sha)
}

fn is_git_sha(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.chars().all(|character| character.is_ascii_hexdigit())
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
    let is_debug = state.get("storage_namespace").and_then(Value::as_str) == Some("debug")
        && initial_state
            .get("storage_namespace")
            .and_then(Value::as_str)
            == Some("debug");
    let identity_fields: &[&str] = if is_debug {
        // A Debug workspace is deliberately date-free, but `prepare_manifest`
        // has already required an exact code/config/prompt identity before we
        // reach this state reader. Its persisted market date/config therefore
        // remain the auditable input context for a safe replay.
        &["run_id", "ticker", "tickers", "storage_namespace"]
    } else {
        &[
            "run_id",
            "current_date",
            "ticker",
            "tickers",
            "config",
            "storage_namespace",
        ]
    };
    for field in identity_fields {
        if state.get(field) != initial_state.get(field) {
            bail!("existing run state field {field} differs from requested run; start a new run");
        }
    }
    Ok(state)
}

fn rehydrate_completed_debug_state(
    store: &FileStore,
    location: &RunLocation,
    manifest: &RunManifest,
    state: &mut Value,
) -> Result<bool> {
    let final_index = read_indexes(
        store,
        Some(location),
        &IndexQuery {
            kind: Some(IndexKind::PhaseSummary),
            source_phase: Some(8),
            role: Some("rust.final_decision".to_owned()),
            limit: 1,
            ..Default::default()
        },
    )?
    .indexes
    .into_iter()
    .next()
    .context("completed Debug run is missing its Phase 8 final-decision Index")?;

    for key in [
        "account_snapshot",
        "allocation_context",
        "allocation_result",
        "execution_report",
        "final_trade_decision",
        "order_plan",
        "portfolio_allocation",
    ] {
        if let Some(value) = final_index.authoritative_fields.get(key) {
            state[key] = value.clone();
        }
    }
    state["phase_status"] = Value::Object(
        manifest
            .phase_status
            .iter()
            .map(|(phase, status)| {
                let value = if matches!(
                    status,
                    orchestrator_store::PhaseStatus::Completed
                        | orchestrator_store::PhaseStatus::Degraded
                ) {
                    "done"
                } else {
                    "failed"
                };
                (phase.clone(), Value::String(value.to_owned()))
            })
            .collect(),
    );
    state["degraded"] = Value::Bool(manifest.degraded);
    state["errors"] = serde_json::to_value(&manifest.errors)?;
    Ok(true)
}

fn rehydrate_completed_phase_projections(
    store: &FileStore,
    location: &RunLocation,
    manifest: &RunManifest,
    state: &mut Value,
) -> Result<bool> {
    let mut changed = false;
    for (phase, status) in &manifest.phase_status {
        if matches!(
            status,
            orchestrator_store::PhaseStatus::Completed | orchestrator_store::PhaseStatus::Degraded
        ) && state
            .pointer(&format!("/phase_status/{phase}"))
            .and_then(Value::as_str)
            != Some("done")
        {
            state["phase_status"][phase.as_str()] = json!("done");
            changed = true;
        }
    }
    if phase_completed(manifest, 3) && state.get("research_plan").is_none() {
        let index = read_indexes(
            store,
            Some(location),
            &IndexQuery {
                kind: Some(IndexKind::PhaseSummary),
                source_phase: Some(3),
                role: Some("manager.research".to_owned()),
                limit: 1,
                ..Default::default()
            },
        )?
        .indexes
        .into_iter()
        .next()
        .context("manifest completed Phase 3 but its ResearchDecision Index is missing")?;
        let decisions = index
            .authoritative_fields
            .get("decisions")
            .cloned()
            .context("Phase 3 ResearchDecision Index is missing decisions")?;
        state["research_plan"] = json!({
            "per_ticker": decisions,
            "regime_context": index
                .authoritative_fields
                .get("regime_context")
                .cloned()
                .unwrap_or(Value::Null),
            "authority": "rehydrated_phase3_index",
            "index_id": index.index_id,
        });
        state
            .as_object_mut()
            .map(|object| object.remove("_force_phase3_recompute"));
        changed = true;
    }
    Ok(changed)
}

fn record_run_failure(context: Option<&RunFailureContext>, error: &anyhow::Error) {
    let Some(context) = context else {
        return;
    };
    let message = format!("{error:#}");
    let mut manifest = match read_run_manifest(&context.store, &context.location) {
        Ok(manifest) => manifest,
        Err(read_error) => {
            tracing::warn!(error = %read_error, failure = %message, "failed run could not reload its manifest");
            return;
        }
    };
    if manifest.status == RunStatus::Completed {
        tracing::warn!(failure = %message, "refusing to overwrite a completed run while recording a later error");
        return;
    }

    let phase = phase_from_failure_message(&message);
    if let Some(phase) = phase {
        manifest.current_phase = phase;
        manifest
            .phase_status
            .insert(phase.to_string(), orchestrator_store::PhaseStatus::Failed);
    }
    manifest.status = RunStatus::Failed;
    manifest.completed_at = None;
    if !manifest
        .errors
        .iter()
        .any(|entry| entry.code == "run_failed" && entry.message == message)
    {
        manifest.errors.push(ManifestError {
            phase,
            code: "run_failed".to_owned(),
            message: message.clone(),
            created_at: Utc::now().to_rfc3339(),
        });
    }

    if let Err(state_error) = persist_run_failure_state(context, &message, phase) {
        tracing::warn!(error = %state_error, failure = %message, "failed run state checkpoint failed");
    }
    if let Err(write_error) = write_run_manifest(&context.store, &context.location, manifest) {
        tracing::warn!(error = %write_error, failure = %message, "failed run manifest update failed");
    }
}

fn persist_run_failure_state(
    context: &RunFailureContext,
    message: &str,
    phase: Option<u8>,
) -> Result<()> {
    let relative = context.location.state_relative();
    if !context.store.exists(&relative)? {
        return Ok(());
    }
    let mut state = context.store.read_json_value(&relative)?;
    if let Some(phase) = phase {
        set_phase_status(&mut state, i64::from(phase), "failed");
    }
    if !state.get("errors").is_some_and(Value::is_array) {
        state["errors"] = json!([]);
    }
    state["errors"]
        .as_array_mut()
        .expect("errors set to array")
        .push(json!({
            "phase": phase,
            "kind": "run_failed",
            "failure": message,
            "recovered": false,
        }));
    persist_state(&mut state)
}

fn phase_from_failure_message(message: &str) -> Option<u8> {
    let lowercase = message.to_ascii_lowercase();
    let marker = "phase ";
    let start = lowercase.find(marker)? + marker.len();
    let digits = lowercase[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    let phase = digits.parse::<u8>().ok()?;
    (phase <= 8).then_some(phase)
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
        if status == "done" && manifest.degraded {
            orchestrator_store::PhaseStatus::Degraded
        } else if status == "done" {
            orchestrator_store::PhaseStatus::Completed
        } else {
            orchestrator_store::PhaseStatus::Failed
        },
    );
    // Persist the state projection before the manifest advertises completion.
    // A later phase may fail, and recovery must never observe a manifest that
    // is ahead of the state it authorizes.
    persist_state(state)?;
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

async fn summarize(
    store_root: &Path,
    state: &mut Value,
    runtime: &RuntimeConfig,
    phase: i64,
    _model: Option<&str>,
    _reasoning: Option<&str>,
) -> Result<BTreeMap<String, String>> {
    let store = FileStore::open(store_root, FileStoreOptions::default())?;
    let location = run_location_from_state(state)?;
    let completed_ids = read_all_indexes(
        &store,
        Some(&location),
        &IndexQuery {
            kind: Some(IndexKind::PhaseSummary),
            source_phase: Some(u8::try_from(phase).context("summary phase must fit u8")?),
            ..Default::default()
        },
    )?;
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

async fn run_phase0(
    store: &FileStore,
    location: &RunLocation,
    state: &mut Value,
    runtime: &RuntimeConfig,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<()> {
    refresh_analyst_calibration(store, location, state);
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
    state: &mut Value,
    config: &crate::orchestration::config::ReflectionConfig,
) -> Result<Vec<Value>> {
    canonical_reflection_tasks(store, current, state, config)
}

fn canonical_reflection_tasks(
    store: &FileStore,
    current: &RunLocation,
    state: &mut Value,
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
    let mut eligibility = Vec::new();
    for outcome in current_outcomes {
        let decision: DecisionSnapshotV2 = store.read_versioned_json(
            Path::new(&outcome.decision_ref.relative_path),
            orchestrator_store::FileSchemaKind::DecisionSnapshot,
        )?;
        let gap_reasons = reflection_learning_gap_reasons(&decision, &outcome);
        let eligible = gap_reasons.is_empty();
        eligibility.push(json!({
            "outcome_id": outcome.outcome_id.clone(),
            "decision_id": decision.decision_id.clone(),
            "ticker": decision.ticker.clone(),
            "eligible": eligible,
            "gap_reasons": gap_reasons.clone(),
            "authority": "rust_reflection_learning_eligibility_v1",
        }));
        if !eligible {
            continue;
        }
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
    eligibility.sort_by(|left, right| {
        left.get("outcome_id")
            .and_then(Value::as_str)
            .cmp(&right.get("outcome_id").and_then(Value::as_str))
    });
    state["reflection_eligibility"] = json!({
        "authority": "rust_reflection_learning_eligibility_v1",
        "candidates": eligibility,
    });
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

/// A historical reflection is allowed to create reusable Experience only
/// when both the original plan and its scored Outcome expose the fields
/// required to distinguish research quality from market luck or execution
/// effects.  This is deliberately stricter than Outcome persistence: an
/// incomplete Outcome may remain a valid audit record, but it is not valid
/// training data.
fn reflection_learning_gap_reasons(
    decision: &DecisionSnapshotV2,
    outcome: &OutcomeRecordV1,
) -> Vec<&'static str> {
    let mut gaps = Vec::new();
    if outcome.ticker != decision.ticker {
        gaps.push("outcome_ticker_does_not_match_decision");
    }
    if !matches!(&decision.thesis, DecisionSection::Available { .. }) {
        gaps.push("decision_thesis_unavailable");
    }
    if !matches!(&decision.trade, DecisionSection::Available { .. }) {
        gaps.push("decision_trade_unavailable");
    }
    if !matches!(&decision.risk, DecisionSection::Available { .. }) {
        gaps.push("decision_risk_unavailable");
    }
    if !matches!(&decision.allocation, DecisionSection::Available { .. }) {
        gaps.push("decision_allocation_unavailable");
    }
    let execution_plan = match &decision.execution_plan {
        DecisionSection::Available { value } => Some(value),
        _ => {
            gaps.push("decision_execution_plan_unavailable");
            None
        }
    };

    match &outcome.market {
        OutcomeSection::Available { value }
            if value.asset_return.is_finite()
                && value.max_adverse_excursion.is_finite()
                && value.corporate_action_resolved => {}
        OutcomeSection::Available { value } if !value.corporate_action_resolved => {
            gaps.push("market_corporate_action_unresolved");
        }
        OutcomeSection::Available { .. } => gaps.push("market_outcome_non_finite"),
        _ => gaps.push("market_outcome_unavailable"),
    }
    match &outcome.benchmark {
        OutcomeSection::Available { value }
            if value.benchmark_return.is_finite() && value.excess_return.is_finite() => {}
        OutcomeSection::Available { .. } => gaps.push("benchmark_outcome_non_finite"),
        _ => gaps.push("benchmark_outcome_unavailable"),
    }
    match &outcome.allocation {
        OutcomeSection::Available { value }
            if value.target_weight.is_finite()
                && value.current_weight.is_finite()
                && value
                    .counterfactual_contribution
                    .is_none_or(|contribution| contribution.is_finite()) => {}
        OutcomeSection::Available { .. } => gaps.push("allocation_outcome_non_finite"),
        _ => gaps.push("allocation_outcome_unavailable"),
    }
    if execution_plan.is_some_and(|plan| plan.attributable_execution_expected) {
        match &outcome.execution {
            OutcomeSection::Available {
                value:
                    ExecutionOutcome::Attributed {
                        order_refs,
                        executed_price,
                        executed_quantity,
                        ..
                    },
            } if !order_refs.is_empty()
                && executed_price.is_finite()
                && executed_quantity.is_finite() => {}
            _ => gaps.push("attributable_execution_outcome_unavailable"),
        }
    }
    gaps
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

#[derive(Debug, Clone)]
struct AnalystCalibrationSample {
    probability: f64,
    long_outcome: bool,
    outcome_id: String,
    phase1_index_id: String,
    source_run_id: String,
}

/// Refresh the Phase 1 role calibration only from mature, canonical Paper or
/// Live Outcomes that were known on or before this run's frozen date. The
/// current run, Debug, Mock, Replay, and unmaterialized Outcomes never enter
/// the sample set. Calibration remains advisory: a store/read error yields a
/// recorded unavailable state and the Phase 1 base falls back to its explicit
/// bootstrap discount.
fn refresh_analyst_calibration(store: &FileStore, current: &RunLocation, state: &mut Value) {
    if state.get("mock").and_then(Value::as_bool) == Some(true) {
        state["analyst_calibration"] = json!({
            "_meta": {
                "authority": "rust_canonical_outcome_brier_v1",
                "status": "not_applicable_mock",
                "reason": "mock_runs_must_not_learn_or_consume_canonical_calibration",
            }
        });
        return;
    }
    state["analyst_calibration"] = match canonical_analyst_calibration(store, current, state) {
        Ok(calibration) => calibration,
        Err(error) => json!({
            "_meta": {
                "authority": "rust_canonical_outcome_brier_v1",
                "status": "unavailable",
                "reason": "canonical_outcome_calibration_read_failed",
                "error": error.to_string(),
            }
        }),
    };
}

fn canonical_analyst_calibration(
    store: &FileStore,
    current: &RunLocation,
    state: &Value,
) -> Result<Value> {
    let as_of_date = state
        .get("current_date")
        .and_then(Value::as_str)
        .and_then(parse_evidence_date)
        .context("analyst calibration requires current_date")?;
    let full_config = state.get("config").cloned().unwrap_or_else(|| json!({}));
    let evaluation_config = config_get(&full_config, "orchestrator.evaluation")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let policy_ref = PolicyRef {
        policy_id: "orchestrator.evaluation".to_owned(),
        version: evaluation_config
            .get("policy_version")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .unwrap_or(1),
        content_hash: content_hash(&evaluation_config)?,
    };
    let evaluation = EvaluationStore::open(
        store.clone(),
        PersistenceContextV1 {
            run_purpose: RunPurpose::Paper,
            namespace: PersistenceNamespace::Canonical,
            canonical_memory_writes_enabled: false,
            invocation_id: current.run_id.clone(),
            config_ref: policy_ref.clone(),
            source_store_fingerprint: policy_ref.content_hash.clone(),
        },
    )?;

    let mut samples = BTreeMap::<String, BTreeMap<String, Vec<AnalystCalibrationSample>>>::new();
    let mut excluded = BTreeMap::<String, u64>::new();
    let mut eligible_outcome_count = 0u64;
    for outcome in evaluation.list_current_outcomes()? {
        if parse_evidence_date(&outcome.created_at).is_none_or(|date| date > as_of_date) {
            increment_calibration_exclusion(&mut excluded, "outcome_not_available_at_as_of");
            continue;
        }
        let OutcomeSection::Available { value: market } = &outcome.market else {
            increment_calibration_exclusion(&mut excluded, "market_outcome_unavailable");
            continue;
        };
        if !market.asset_return.is_finite() {
            increment_calibration_exclusion(&mut excluded, "market_outcome_non_finite");
            continue;
        }
        let decision: DecisionSnapshotV2 = store.read_versioned_json(
            Path::new(&outcome.decision_ref.relative_path),
            FileSchemaKind::DecisionSnapshot,
        )?;
        if !matches!(decision.run_purpose, RunPurpose::Paper | RunPurpose::Live) {
            increment_calibration_exclusion(&mut excluded, "non_canonical_decision_purpose");
            continue;
        }
        if decision.source_run_id == current.run_id {
            increment_calibration_exclusion(&mut excluded, "current_run_is_not_out_of_sample");
            continue;
        }
        let Some(source_location) =
            orchestrator_store::find_run_location(store, &decision.source_run_id)?
        else {
            increment_calibration_exclusion(&mut excluded, "source_run_missing");
            continue;
        };
        let phase1_indexes = read_all_indexes(
            store,
            Some(&source_location),
            &IndexQuery {
                kind: Some(IndexKind::PhaseSummary),
                source_phase: Some(1),
                ..Default::default()
            },
        )?;
        let mut contributed = false;
        for role in ["analyst.technical", "analyst.news_macro"] {
            let role_indexes = phase1_indexes
                .iter()
                .filter(|index| index.role == role)
                .collect::<Vec<_>>();
            if role_indexes.len() != 1 {
                increment_calibration_exclusion(
                    &mut excluded,
                    "phase1_role_index_missing_or_ambiguous",
                );
                continue;
            }
            let index = role_indexes[0];
            let Some(probability) = index
                .authoritative_fields
                .get("per_ticker")
                .and_then(Value::as_object)
                .and_then(|per_ticker| per_ticker.get(&outcome.ticker))
                .and_then(|ticker| ticker.get("long_probability"))
                .and_then(Value::as_f64)
                .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
            else {
                increment_calibration_exclusion(&mut excluded, "phase1_probability_unavailable");
                continue;
            };
            samples
                .entry(role.to_owned())
                .or_default()
                .entry(outcome.ticker.clone())
                .or_default()
                .push(AnalystCalibrationSample {
                    probability,
                    long_outcome: market.asset_return > 0.0,
                    outcome_id: outcome.outcome_id.clone(),
                    phase1_index_id: index.index_id.clone(),
                    source_run_id: decision.source_run_id.clone(),
                });
            contributed = true;
        }
        if contributed {
            eligible_outcome_count += 1;
        }
    }

    let mut calibration = serde_json::Map::new();
    for (role, by_ticker) in samples {
        let mut tickers = serde_json::Map::new();
        for (ticker, mut ticker_samples) in by_ticker {
            ticker_samples.sort_by(|left, right| {
                (&left.outcome_id, &left.phase1_index_id)
                    .cmp(&(&right.outcome_id, &right.phase1_index_id))
            });
            let sample_size = ticker_samples.len() as u64;
            let brier_score = ticker_samples
                .iter()
                .map(|sample| {
                    let outcome = if sample.long_outcome { 1.0 } else { 0.0 };
                    (sample.probability - outcome).powi(2)
                })
                .sum::<f64>()
                / sample_size as f64;
            // A 50/50 forecast has Brier 0.25. The reliability scale only
            // rewards out-of-sample improvement over that uninformative
            // baseline and never turns a poor sample into a negative weight.
            let reliability = (1.0 - brier_score / 0.25).clamp(0.0, 1.0);
            let directional_accuracy = ticker_samples
                .iter()
                .filter(|sample| (sample.probability > 0.5) == sample.long_outcome)
                .count() as f64
                / sample_size as f64;
            let sample_set_hash = content_hash(&json!(ticker_samples
                .iter()
                .map(|sample| json!({
                    "outcome_id": sample.outcome_id,
                    "phase1_index_id": sample.phase1_index_id,
                    "source_run_id": sample.source_run_id,
                    "probability": round_probability(sample.probability),
                    "long_outcome": sample.long_outcome,
                }))
                .collect::<Vec<_>>()))?;
            tickers.insert(
                ticker,
                json!({
                    "authority": "rust_canonical_outcome_brier_v1",
                    "status": if sample_size >= 20 { "available" } else { "insufficient_samples" },
                    "sample_size": sample_size,
                    "minimum_sample_size": 20,
                    "brier_score": round_probability(brier_score),
                    "reliability": round_probability(reliability),
                    "directional_accuracy": round_probability(directional_accuracy),
                    "sample_set_hash": sample_set_hash,
                    "sample_outcome_ids": ticker_samples.iter().map(|sample| sample.outcome_id.clone()).take(20).collect::<Vec<_>>(),
                    "sample_phase1_index_ids": ticker_samples.iter().map(|sample| sample.phase1_index_id.clone()).take(20).collect::<Vec<_>>(),
                }),
            );
        }
        calibration.insert(role, Value::Object(tickers));
    }
    calibration.insert(
        "_meta".to_owned(),
        json!({
            "authority": "rust_canonical_outcome_brier_v1",
            "status": if eligible_outcome_count > 0 { "available" } else { "no_eligible_samples" },
            "as_of_date": as_of_date.to_string(),
            "source_namespace": "canonical",
            "eligible_outcome_count": eligible_outcome_count,
            "excluded_outcomes_by_reason": excluded,
            "policy_ref": policy_ref,
        }),
    );
    Ok(Value::Object(calibration))
}

fn increment_calibration_exclusion(excluded: &mut BTreeMap<String, u64>, reason: &str) {
    *excluded.entry(reason.to_owned()).or_default() += 1;
}

async fn run_phase1(
    state: &mut Value,
    runtime: &RuntimeConfig,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<()> {
    let store = FileStore::open(
        Path::new(
            state
                .get("store_root")
                .and_then(Value::as_str)
                .context("Phase 1 calibration requires store_root")?,
        ),
        FileStoreOptions::default(),
    )?;
    let location = run_location_from_state(state)?;
    refresh_analyst_calibration(&store, &location, state);
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
            "storage_namespace": input.storage_namespace,
        });
    }
    let roles = ["analyst.technical", "analyst.news_macro"];
    let artifacts = run_parallel_units(
        state,
        runtime,
        roles
            .iter()
            .map(|role| UnitSpec {
                role: (*role).to_owned(),
                phase: 1,
                kind: "artifact".to_owned(),
                round: None,
                topic_id: None,
                ticker: None,
            })
            .collect(),
        roles.len(),
        model,
        reasoning,
    )
    .await?;
    let reports = roles
        .into_iter()
        .zip(artifacts)
        .map(|(role, artifact)| {
            (
                role.to_owned(),
                artifact.get("payload").cloned().unwrap_or(artifact),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    state["analyst_reports"] = Value::Object(reports);
    state["phase1_index"] = json!({"roles": state["analyst_reports"], "authority": "file_store"});
    state["phase1_evidence_event_ledger"] = phase1_evidence_event_ledger(state)?;
    state["weighted_probability_base"] = weighted_probability_base(state)?;
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
    let phase1_evidence_clusters = phase2_initial_evidence_event_clusters(state, store, location)?;
    let mut initial_artifacts = run_parallel_units(
        state,
        runtime,
        vec![
            UnitSpec {
                role: "mediator.topic".to_owned(),
                phase: 2,
                kind: "warmup".to_owned(),
                round: Some(0),
                topic_id: None,
                ticker: None,
            },
            UnitSpec {
                role: "mediator.topic".to_owned(),
                phase: 2,
                kind: "topic_generation".to_owned(),
                round: None,
                topic_id: None,
                ticker: None,
            },
        ],
        2,
        model,
        reasoning,
    )
    .await?;
    let warmup = initial_artifacts.remove(0);
    state["phase2_warmup"] = warmup;
    // Preserve the completed Warmup artifact first, then attach its Rust-owned
    // session identity. Reversing this order silently erased the fork source
    // and let Bull/Bear seeds start without their required parent evidence.
    record_phase2_session(state, "mediator.topic", "warmup", None, None, Some(0));
    let mut generated = initial_artifacts
        .pop()
        .context("Phase 2 parallel initialization produced no Topic Generator artifact")?;
    record_phase2_session(
        state,
        "mediator.topic",
        "topic_generation",
        None,
        None,
        None,
    );
    // The Topic Generator receives long immutable IDs in its Phase 1 packet,
    // but a model may still render an unambiguous display abbreviation such as
    // `jin10-81e36c...`.  Resolve only abbreviations that map to exactly one
    // Rust-observed Phase 1 reference before topic IDs are content-hashed. An
    // unknown or ambiguous spelling is rejected here, rather than surviving
    // into a later Phase 2 summary where it would poison the Detail lineage.
    let generated_has_payload = generated.get("payload").is_some();
    let generated_payload = if generated_has_payload {
        generated.pointer_mut("/payload")
    } else {
        Some(&mut generated)
    };
    let generated_fields = generated_payload
        .and_then(Value::as_object_mut)
        .context("Phase 2 topic generation artifact requires object payload")?;
    normalize_phase2_topic_generation_evidence_refs(generated_fields, &phase1_evidence_clusters)?;
    project_topic_generation_selection(generated_fields)?;
    project_topic_generation_residual_coverage(generated_fields)?;
    validate_phase2_compiled_contract("topic_generation", generated_fields, &[])?;
    let generated_topics = generated
        .pointer("/payload/topics")
        .or_else(|| generated.get("topics"))
        .cloned()
        .unwrap_or_else(|| json!([]));
    let generated_candidate_topics = generated
        .pointer("/payload/candidate_topics")
        .or_else(|| generated.get("candidate_topics"))
        .cloned()
        .unwrap_or_else(|| json!([]));
    let generated_residual_risks = generated
        .pointer("/payload/residual_risks")
        .or_else(|| generated.get("residual_risks"))
        .cloned()
        .unwrap_or_else(|| json!([]));
    let generated_coverage = generated
        .pointer("/payload/coverage")
        .or_else(|| generated.get("coverage"))
        .cloned()
        .unwrap_or_else(|| json!([]));
    let max_topics_per_side = state
        .get("max_topics_per_side")
        .and_then(Value::as_i64)
        .unwrap_or(3)
        .clamp(1, 20) as usize;
    let (topics, topic_selection) = select_phase2_topics(generated_topics, max_topics_per_side)?;
    let unselected_candidates = unselected_phase2_candidates(&generated_candidate_topics, &topics)?;
    let topic_generation_session =
        runtime_session_for(state, "mediator.topic", "topic_generation", None, None);
    state["topic_generation_session_id"] = topic_generation_session["session_id"].clone();
    state["topic_generation_turn_id"] = topic_generation_session["turn_id"].clone();
    let actionable = topics.as_array().is_some_and(|items| !items.is_empty());
    state["topic_generation_artifact"] = json!({
        "artifact": generated,
        "candidate_topics": generated_candidate_topics,
        "topics": topics,
        "unselected_candidates": unselected_candidates,
        "residual_risks": generated_residual_risks,
        "coverage": generated_coverage,
        "actionable": actionable,
        "selection": topic_selection,
    });

    let topic_values = topics.as_array().cloned().unwrap_or_default();
    let base_state = state.clone();
    let base_metric_count = state
        .get("role_job_metrics")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    let base_error_count = state
        .get("errors")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    let topic_outcomes =
        stream::iter(topic_values.into_iter().enumerate().map(|(index, topic)| {
            let topic_id = topic
                .get("topic_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            let mut worker_state = base_state.clone();
            let evidence_clusters = phase1_evidence_clusters.clone();
            async move {
                let topic_id = topic_id
                    .context("Phase 2 topic generation returned a topic without topic_id")?;
                let result = run_phase2_topic(
                    &mut worker_state,
                    runtime,
                    topic,
                    &evidence_clusters,
                    model,
                    reasoning,
                )
                .await;
                Ok::<_, Error>((index, topic_id, result, worker_state))
            }
        }))
        .buffer_unordered(state["max_topics_per_side"].as_u64().unwrap_or(3).max(1) as usize)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    let mut topic_outcomes = topic_outcomes;
    topic_outcomes.sort_by_key(|(index, _, _, _)| *index);

    let mut controllers = serde_json::Map::new();
    let mut first_topic_error: Option<Error> = None;
    for (_, topic_id, result, worker_state) in topic_outcomes {
        match result {
            Ok(controller) => {
                merge_parallel_state_delta(
                    state,
                    &worker_state,
                    base_metric_count,
                    base_error_count,
                    Some(&topic_id),
                );
                controllers.insert(topic_id, controller);
            }
            Err(error) => {
                merge_parallel_state_delta(
                    state,
                    &worker_state,
                    base_metric_count,
                    base_error_count,
                    Some(&topic_id),
                );
                if first_topic_error.is_none() {
                    first_topic_error =
                        Some(error.context(format!("Phase 2 topic debate failed for {topic_id}")));
                }
            }
        }
    }
    refresh_role_job_metrics(state);
    if let Some(error) = first_topic_error {
        checkpoint_state(state).context("failed to persist Phase 2 parallel topic failure")?;
        return Err(error);
    }
    // The only Phase 2-wide Summary runs after every topic tree has reached a
    // terminal Controller closure. Individual stree turns deliberately stay
    // as raw session events so a partial debate cannot become cross-phase
    // truth merely because one participant replied.
    let reducer_text = serde_json::to_string_pretty(&controllers)?;
    let reducer = compile_unit_response(
        state,
        runtime,
        "mediator.topic_controller",
        2,
        "phase2_final",
        None,
        None,
        None,
        &reducer_text,
        model,
        reasoning,
        true,
    )
    .await?;
    state["debate_state_artifact"] = json!({
        "status": "completed",
        "topic_briefs": state["topic_generation_artifact"]["topics"],
        "final_reducer": {
            "authoritative_fields": {"controllers": controllers},
            "phase_summary": reducer.clone()
        },
        "phase_summary": reducer,
        "authority": "file_store"
    });
    checkpoint_state(state)?;
    write_phase2_debate_debug_summary(state)?;
    Ok(())
}

/// Execute one complete topic tree. The tree itself remains sequential because
/// each delivery depends on the previous mailbox state; separate topic trees
/// are independent and are therefore run by `run_phase2` concurrently.
async fn run_phase2_topic(
    state: &mut Value,
    runtime: &RuntimeConfig,
    topic: Value,
    phase1_evidence_clusters: &BTreeMap<String, String>,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<Value> {
    let topic_id = topic
        .get("topic_id")
        .and_then(Value::as_str)
        .context("Phase 2 topic generation returned a topic without topic_id")?
        .to_owned();
    let max_rounds = state
        .get("max_debate_rounds")
        .and_then(Value::as_i64)
        .unwrap_or(1)
        .max(0);
    let mut tree = TopicDebateTree::open(&topic_id, topic.clone(), max_rounds as u32)?;
    for (reference, event_cluster_id) in phase1_evidence_clusters {
        tree.register_evidence_ref_cluster(reference, event_cluster_id)?;
    }
    tree.set_independence_context(
        phase2_role_model(runtime, model, "researcher.bull"),
        phase2_role_model(runtime, model, "researcher.bear"),
    );
    tree.recover_inflight();
    state["topic_debate_states"][&topic_id] = json!({"topic": topic, "stree": tree});
    let mut final_controller = Value::Null;
    let max_dispatches = (max_rounds as u32)
        .saturating_mul(12)
        .saturating_add(12)
        .clamp(12, 64);
    let mut dispatch_count = 0u32;
    while !tree.is_closed() {
        if dispatch_count >= max_dispatches {
            tree.close_after_safety_limit()?;
            break;
        }
        let Some(dispatch) = tree.next_dispatch() else {
            tree.close_after_safety_limit()?;
            break;
        };
        dispatch_count = dispatch_count.saturating_add(1);
        let role = dispatch.actor.role();
        let round = i64::from(tree.round);
        let delivery_ids = dispatch
            .deliveries
            .iter()
            .map(|delivery| delivery.delivery_id.as_str())
            .collect::<Vec<_>>();
        state["_phase2_stree_dispatch_key"] = json!(phase2_stree_dispatch_key(
            &topic_id,
            dispatch.actor,
            &delivery_ids,
        ));
        state["_phase2_stree_injection"] = if dispatch.deliveries.is_empty() {
            Value::Null
        } else {
            Value::String(tree.injected_user_message(&dispatch.deliveries)?)
        };
        state["topic_debate_states"][&topic_id]["stree"] = serde_json::to_value(&tree)?;
        let mut artifact = match run_unit_with_checkpoint(
            state,
            runtime,
            role,
            2,
            "stree_turn",
            Some(round),
            Some(&topic_id),
            None,
            model,
            reasoning,
            false,
        )
        .await
        {
            Ok(artifact) => artifact,
            Err(error) => {
                record_phase2_runtime_failure(
                    state,
                    &topic_id,
                    dispatch.actor,
                    "role_job_failure",
                    &error.to_string(),
                );
                tree.record_failure(dispatch.actor, error.to_string(), 1)?;
                state["topic_debate_states"][&topic_id]["stree"] = serde_json::to_value(&tree)?;
                continue;
            }
        };
        state["_phase2_stree_injection"] = Value::Null;
        // A natural-language response is not a completed STree turn. Give the
        // same persisted conversation one Rust-owned correction before
        // recording a tree failure: the model retains its analysis while the
        // retry can only finish through the required terminal tool.
        if !state["mock"].as_bool().unwrap_or(false)
            && !phase2_stree_terminal_command_present(&artifact)
        {
            state["_phase2_stree_injection"] = Value::String(phase2_terminal_tool_retry_injection(
                &topic_id,
                dispatch.actor,
            ));
            artifact = match run_unit_with_checkpoint(
                state,
                runtime,
                role,
                2,
                "stree_turn",
                Some(round),
                Some(&topic_id),
                None,
                model,
                reasoning,
                false,
            )
            .await
            {
                Ok(retry) => retry,
                Err(error) => {
                    state["_phase2_stree_injection"] = Value::Null;
                    record_phase2_runtime_failure(
                        state,
                        &topic_id,
                        dispatch.actor,
                        "role_job_failure",
                        &error.to_string(),
                    );
                    tree.record_failure(dispatch.actor, error.to_string(), 1)?;
                    state["topic_debate_states"][&topic_id]["stree"] = serde_json::to_value(&tree)?;
                    continue;
                }
            };
            state["_phase2_stree_injection"] = Value::Null;
        }
        if state["mock"].as_bool().unwrap_or(false) {
            apply_mock_phase2_stree_command(&mut tree, dispatch.actor)?;
        } else if let Err(error) = apply_phase2_stree_command(&mut tree, dispatch.actor, &artifact)
        {
            let error_text = error.to_string();
            if dispatch.actor == DebateActor::Controller
                && is_phase2_controller_close_required_error(&error_text)
            {
                // The Controller has already received the complete final
                // collision wave. A rejected extra route must not turn that
                // evidence into an artificial Rust closure: give the same
                // persisted session one explicit terminal-close correction.
                state["_phase2_stree_injection"] =
                    Value::String(phase2_controller_close_retry_injection(&topic_id));
                artifact = match run_unit_with_checkpoint(
                    state,
                    runtime,
                    role,
                    2,
                    "stree_turn",
                    Some(round),
                    Some(&topic_id),
                    None,
                    model,
                    reasoning,
                    false,
                )
                .await
                {
                    Ok(retry) => retry,
                    Err(error) => {
                        state["_phase2_stree_injection"] = Value::Null;
                        record_phase2_runtime_failure(
                            state,
                            &topic_id,
                            dispatch.actor,
                            "role_job_failure",
                            &error.to_string(),
                        );
                        tree.record_failure(dispatch.actor, error.to_string(), 1)?;
                        state["topic_debate_states"][&topic_id]["stree"] =
                            serde_json::to_value(&tree)?;
                        continue;
                    }
                };
                state["_phase2_stree_injection"] = Value::Null;
                if let Err(retry_error) =
                    apply_phase2_stree_command(&mut tree, dispatch.actor, &artifact)
                {
                    let retry_error_text = retry_error.to_string();
                    record_phase2_runtime_failure(
                        state,
                        &topic_id,
                        dispatch.actor,
                        "stree_command_failure",
                        &retry_error_text,
                    );
                    tree.record_failure(dispatch.actor, retry_error_text, 1)?;
                }
            } else {
                record_phase2_runtime_failure(
                    state,
                    &topic_id,
                    dispatch.actor,
                    "stree_command_failure",
                    &error_text,
                );
                tree.record_failure(dispatch.actor, error_text, 1)?;
            }
        }
        if dispatch.actor == DebateActor::Controller {
            final_controller = artifact.clone();
        }
        state["topic_debate_states"][&topic_id]["stree"] = serde_json::to_value(&tree)?;
        state["topic_debate_states"][&topic_id]["latest_artifact"] = artifact;
    }
    state["_phase2_stree_dispatch_key"] = Value::Null;
    state["topic_debate_states"][&topic_id]["final_controller_artifact"] = final_controller;
    Ok(tree.process_summary())
}

fn select_phase2_topics(generated: Value, max_topics_per_side: usize) -> Result<(Value, Value)> {
    let mut topics = generated
        .as_array()
        .cloned()
        .context("Phase 2 topic generation topics must be an array")?;
    let generated_count = topics.len();
    let mut topic_ids = BTreeSet::new();
    for (index, topic) in topics.iter().enumerate() {
        let topic_id = topic
            .get("topic_id")
            .and_then(Value::as_str)
            .with_context(|| format!("Phase 2 topic {index} requires topic_id"))?;
        if topic_id.trim().is_empty() || topic_id != topic_id.trim() {
            bail!("Phase 2 topic {index} topic_id must be non-empty and trimmed")
        }
        if !topic_ids.insert(topic_id.to_owned()) {
            bail!("Phase 2 topic generation returned duplicate topic_id {topic_id:?}")
        }
    }
    topics.truncate(max_topics_per_side);
    let selected_count = topics.len();
    Ok((
        Value::Array(topics),
        json!({
            "authority": "rust",
            "policy": "each Bull/Bear side participates in at most max_topics_per_side topic lanes",
            "max_topics_per_side": max_topics_per_side,
            "generated_count": generated_count,
            "selected_count": selected_count,
            "truncated_count": generated_count.saturating_sub(selected_count),
        }),
    ))
}

/// Candidate selection is Rust-owned once the Summary has supplied canonical
/// candidate IDs. Preserve the full unselected records separately from the
/// model's generic residual-risk classes so Phase 3 can inspect their
/// evidence without forcing the model to duplicate them in two schemas.
fn unselected_phase2_candidates(
    candidate_topics: &Value,
    selected_topics: &Value,
) -> Result<Value> {
    let candidates = candidate_topics
        .as_array()
        .context("Phase 2 topic generation candidate_topics must be an array")?;
    let selected = selected_topics
        .as_array()
        .context("Phase 2 topic generation topics must be an array")?;
    let selected_ids = selected
        .iter()
        .map(|topic| {
            topic
                .get("topic_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .context("Phase 2 selected topic requires topic_id")
        })
        .collect::<Result<BTreeSet<_>>>()?;
    let mut unselected = Vec::new();
    for candidate in candidates {
        let topic_id = candidate
            .get("topic_id")
            .and_then(Value::as_str)
            .context("Phase 2 candidate topic requires topic_id")?;
        if !selected_ids.contains(topic_id) {
            unselected.push(candidate.clone());
        }
    }
    Ok(Value::Array(unselected))
}

fn phase2_initial_evidence_registry(
    store: &FileStore,
    location: &RunLocation,
) -> Result<BTreeMap<String, String>> {
    let indexes = read_all_indexes(
        store,
        Some(location),
        &IndexQuery {
            kind: Some(IndexKind::PhaseSummary),
            source_phase: Some(1),
            ..IndexQuery::default()
        },
    )?;
    let mut references = BTreeMap::new();
    for index in indexes {
        references.insert(
            index.index_id.clone(),
            format!("phase1-summary:{}", index.index_id),
        );
        let fields = Value::Object(index.authoritative_fields);
        let mut cited_references = BTreeSet::new();
        collect_reference_array_ids(&fields, &mut cited_references);
        references.extend(
            cited_references
                .into_iter()
                .map(|reference| (reference.clone(), format!("known-reference:{reference}"))),
        );
        // Phase 1 Details expose the complete, Rust-verified Web tool result
        // packet to the Topic Generator.  It is therefore legitimate for a
        // topic to cite a result that the analyst did not select as
        // `key_evidence`.  Preserve that runtime-owned registry separately
        // from the analyst's citations so the Topic Generator neither loses
        // visible evidence nor gets to introduce an arbitrary `web-*` ID.
        for record in phase1_verified_web_evidence_records(&fields) {
            let evidence_id = record
                .get("evidence_id")
                .and_then(Value::as_str)
                .context("Phase 1 verified Web registry record requires evidence_id")?;
            let source_url = record
                .get("source_url")
                .and_then(Value::as_str)
                .context("Phase 1 verified Web registry record requires source_url")?;
            references.insert(
                evidence_id.to_owned(),
                format!("url:{}", normalize_evidence_origin(source_url)),
            );
        }
        // The raw Phase 1 Detail similarly exposes every Rust-observed
        // technical/Jin10 result, not just the analyst's selected citations.
        // Keep those IDs in a separate runtime-owned registry.  This permits a
        // Phase 2 topic to identify an unselected data-quality or event-risk
        // issue without allowing a model-invented `technical-*`/`jin10-*` ID.
        for record in phase1_verified_input_evidence_records(&fields) {
            let evidence_id = record
                .get("evidence_id")
                .and_then(Value::as_str)
                .context("Phase 1 verified input registry record requires evidence_id")?;
            references
                .entry(evidence_id.to_owned())
                .or_insert_with(|| format!("known-reference:{evidence_id}"));
        }
    }
    if references.is_empty() {
        bail!("Phase 2 requires persisted Phase 1 evidence provenance")
    }
    Ok(references)
}

/// Build the event-level evidence map available before either side speaks.
/// Index IDs themselves are known context, but evidence IDs inherit the
/// Phase 1 event ledger so a new reference spelling cannot masquerade as a
/// new fact in the debate.
fn phase2_initial_evidence_event_clusters(
    state: &Value,
    store: &FileStore,
    location: &RunLocation,
) -> Result<BTreeMap<String, String>> {
    let mut clusters = phase2_initial_evidence_registry(store, location)?;
    if let Some(events) = state
        .pointer("/phase1_evidence_event_ledger/events")
        .and_then(Value::as_object)
    {
        for (event_cluster_id, event) in events {
            for reference in event
                .get("evidence_refs")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
            {
                if is_phase1_evidence_id(reference) {
                    clusters.insert(reference.to_owned(), event_cluster_id.clone());
                }
            }
        }
    }
    if clusters.is_empty() {
        bail!("Phase 2 requires a non-empty Phase 1 event evidence map")
    }
    Ok(clusters)
}

/// Normalize model-rendered Phase 1 references before Topic Generator output
/// becomes a canonical Phase 2 search space.  The set is intentionally the
/// Rust-built Phase 1 registry, not a fuzzy search over arbitrary historical
/// artifacts: an abbreviated ID is accepted only when its literal prefix has
/// exactly one current-run target.
fn normalize_phase2_topic_generation_evidence_refs(
    fields: &mut serde_json::Map<String, Value>,
    known_phase1_references: &BTreeMap<String, String>,
) -> Result<()> {
    let mut document = Value::Object(std::mem::take(fields));
    let mut projections = Vec::new();
    normalize_phase2_topic_generation_reference_arrays(
        &mut document,
        None,
        known_phase1_references,
        &mut projections,
    )?;
    let mut normalized = document
        .as_object()
        .cloned()
        .context("Phase 2 topic generation fields must remain an object")?;
    if !projections.is_empty() {
        normalized.insert(
            "topic_generation_reference_projection".to_owned(),
            json!({
                "authority": "rust_phase2_topic_generation_reference_projection_v1",
                "resolved_abbreviations": projections,
            }),
        );
    }
    *fields = normalized;
    Ok(())
}

fn normalize_phase2_topic_generation_reference_arrays(
    value: &mut Value,
    parent_key: Option<&str>,
    known_phase1_references: &BTreeMap<String, String>,
    projections: &mut Vec<Value>,
) -> Result<()> {
    match value {
        Value::Array(values)
            if matches!(
                parent_key,
                Some("evidence_refs" | "source_refs" | "source_index_ids")
            ) =>
        {
            for reference in values {
                let original = reference
                    .as_str()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .context("Phase 2 topic generation references must contain non-empty strings")?
                    .to_owned();
                let canonical = canonical_phase2_topic_generation_reference(
                    &original,
                    known_phase1_references,
                )?;
                if canonical != original {
                    projections.push(json!({
                        "model_reference": original,
                        "resolved_reference": canonical,
                    }));
                }
                *reference = Value::String(canonical);
            }
            Ok(())
        }
        Value::Array(values) => {
            for value in values {
                normalize_phase2_topic_generation_reference_arrays(
                    value,
                    parent_key,
                    known_phase1_references,
                    projections,
                )?;
            }
            Ok(())
        }
        Value::Object(values) => {
            for (key, value) in values {
                normalize_phase2_topic_generation_reference_arrays(
                    value,
                    Some(key),
                    known_phase1_references,
                    projections,
                )?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn canonical_phase2_topic_generation_reference(
    reference: &str,
    known_phase1_references: &BTreeMap<String, String>,
) -> Result<String> {
    if known_phase1_references.contains_key(reference) {
        return Ok(reference.to_owned());
    }
    let Some(prefix) = reference.strip_suffix("...") else {
        bail!(
            "Phase 2 topic generation reference {reference:?} is not a current-run Phase 1 stable ID"
        )
    };
    let mut candidates = known_phase1_references
        .keys()
        .filter(|candidate| candidate.starts_with(prefix))
        .cloned()
        .collect::<Vec<_>>();
    candidates.sort();
    match candidates.as_slice() {
        [candidate] => Ok(candidate.clone()),
        [] => bail!(
            "Phase 2 topic generation abbreviated reference {reference:?} has no current-run Phase 1 match"
        ),
        _ => bail!(
            "Phase 2 topic generation abbreviated reference {reference:?} is ambiguous across {} current-run Phase 1 IDs",
            candidates.len()
        ),
    }
}

/// Return the Rust-canonical compiler input for a Topic Generator response
/// plus a durable audit projection.  This intentionally leaves unknown and
/// ambiguous prose untouched: only a later structured `evidence_refs` field
/// is a declared citation and will be rejected if it remains invalid.
fn canonicalize_phase2_topic_generation_summary_source(
    response_text: &str,
    known_phase1_references: &BTreeMap<String, String>,
) -> Result<(String, Vec<Value>)> {
    const PREFIXES: [&str; 4] = ["technical-", "jin10-", "web-", "idx-"];
    let mut projected = String::with_capacity(response_text.len());
    let mut projections = Vec::new();
    let mut cursor = 0usize;

    while cursor < response_text.len() {
        let remainder = &response_text[cursor..];
        let next = PREFIXES
            .iter()
            .filter_map(|prefix| remainder.find(prefix).map(|offset| (offset, *prefix)))
            .min_by_key(|(offset, _)| *offset);
        let Some((offset, prefix)) = next else {
            projected.push_str(remainder);
            break;
        };
        let start = cursor + offset;
        projected.push_str(&response_text[cursor..start]);
        let suffix = &response_text[start..];
        let Some(ellipsis_offset) = suffix.find("...") else {
            projected.push_str(prefix);
            cursor = start + prefix.len();
            continue;
        };
        let candidate = &suffix[..ellipsis_offset + 3];
        let abbreviated_digest = candidate.strip_suffix("...").unwrap_or_default();
        // A prose occurrence such as `technical analysis ...` is not an ID.
        // Stable references contain the role prefix followed by at least one
        // hexadecimal digest character before the ellipsis.
        let digest = abbreviated_digest.strip_prefix(prefix).unwrap_or_default();
        if digest.is_empty()
            || !digest
                .chars()
                .all(|character| character.is_ascii_hexdigit())
        {
            projected.push_str(prefix);
            cursor = start + prefix.len();
            continue;
        }
        let matches = known_phase1_references
            .keys()
            .filter(|reference| reference.starts_with(abbreviated_digest))
            .cloned()
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [canonical] => {
                projected.push_str(canonical);
                projections.push(json!({
                    "model_reference": candidate,
                    "resolved_reference": canonical,
                    "stage": "before_phase2_summary_extraction",
                }));
                cursor = start + candidate.len();
            }
            // Do not decide a generic `idx-...` placeholder or a collision in
            // free text.  It remains visible in the original Detail and a
            // structured citation must later pass the strict validator.
            _ => {
                projected.push_str(prefix);
                cursor = start + prefix.len();
            }
        }
    }
    Ok((projected, projections))
}

fn phase2_role_model(runtime: &RuntimeConfig, model_override: Option<&str>, role: &str) -> String {
    model_override
        .filter(|model| !model.trim().is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            runtime
                .llm_roles
                .get(role)
                .map(|settings| settings.model.clone())
        })
        .unwrap_or_default()
}

fn record_phase2_runtime_failure(
    state: &mut Value,
    topic_id: &str,
    actor: DebateActor,
    kind: &str,
    failure: &str,
) {
    state["degraded"] = Value::Bool(true);
    if !state["errors"].is_array() {
        state["errors"] = json!([]);
    }
    state["errors"]
        .as_array_mut()
        .expect("errors initialized as array")
        .push(json!({
            "phase": 2,
            "kind": kind,
            "topic_id": topic_id,
            "role": actor.role(),
            "failure": failure,
            "recovered": true,
        }));
}

fn apply_phase2_stree_command(
    tree: &mut TopicDebateTree,
    actor: DebateActor,
    artifact: &Value,
) -> Result<()> {
    let verified_evidence_refs = stree_artifact_verified_evidence_refs(artifact)?;
    register_stree_artifact_evidence_refs(tree, artifact, &verified_evidence_refs)?;
    let command = artifact
        .pointer("/phase2_stree/command")
        .and_then(Value::as_str)
        .context("Phase 2 turn did not finish through a stree terminal tool")?;
    let payload = artifact
        .pointer("/phase2_stree/payload")
        .cloned()
        .context("Phase 2 stree terminal omitted payload")?;
    match (actor, command) {
        (DebateActor::Bull | DebateActor::Bear, "submit_debate_turn") => {
            tree.submit(actor, payload)?;
        }
        (DebateActor::Controller, "route_debate_turn") => {
            tree.controller_route(payload)?;
        }
        (DebateActor::Controller, "wait_for_debate_turn") => {
            tree.controller_wait(payload)?;
        }
        (DebateActor::Controller, "close_debate") => {
            tree.controller_close_with_verified_evidence(payload, &verified_evidence_refs)?;
        }
        _ => bail!(
            "{} is not allowed to issue stree command {command}",
            actor.role()
        ),
    }
    Ok(())
}

fn register_stree_artifact_evidence_refs(
    tree: &mut TopicDebateTree,
    artifact: &Value,
    references: &BTreeSet<String>,
) -> Result<()> {
    let event_clusters = artifact
        .get("verified_evidence_records")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|record| {
            let reference = record.get("evidence_id")?.as_str()?;
            Some((
                reference.to_owned(),
                phase2_external_event_cluster_id(record, reference),
            ))
        })
        .collect::<BTreeMap<_, _>>();
    for reference in references {
        let event_cluster_id = event_clusters
            .get(reference.as_str())
            .map(String::as_str)
            .unwrap_or(reference.as_str());
        tree.register_evidence_ref_cluster(reference, event_cluster_id)?;
    }
    Ok(())
}

fn stree_artifact_verified_evidence_refs(artifact: &Value) -> Result<BTreeSet<String>> {
    let Some(references) = artifact.get("verified_evidence_refs") else {
        return Ok(BTreeSet::new());
    };
    references
        .as_array()
        .context("Phase 2 terminal artifact verified_evidence_refs must be an array")?
        .iter()
        .map(|reference| {
            reference
                .as_str()
                .filter(|reference| !reference.trim().is_empty())
                .map(ToOwned::to_owned)
                .context("Phase 2 terminal artifact verified_evidence_refs must contain strings")
        })
        .collect()
}

/// Cross-phase event identity for Web evidence.  A stable source URL wins
/// over the evidence ID and its retrieval time, so the same event surfaced by
/// Phase 1 and Phase 2 is counted once even when each tool emitted a new ID.
fn phase2_external_event_cluster_id(record: &Value, fallback_reference: &str) -> String {
    let source_url = record
        .get("source_url")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| value.starts_with("https://") || value.starts_with("http://"));
    if let Some(source_url) = source_url {
        return format!("url:{}", normalize_evidence_origin(source_url));
    }
    let origin = record
        .get("publisher")
        .and_then(Value::as_str)
        .map(normalize_evidence_origin)
        .filter(|value| !value.is_empty());
    let clock = ["event_time", "published_at", "published_time"]
        .into_iter()
        .filter_map(|field| record.get(field).and_then(Value::as_str))
        .find(|value| !value.trim().is_empty())
        .map(normalize_evidence_clock);
    match (origin, clock) {
        (Some(origin), Some(clock)) => format!("origin:{origin}:{clock}"),
        (Some(origin), None) => format!("origin:{origin}:unknown-time"),
        _ => format!("reference:{fallback_reference}"),
    }
}

fn phase2_stree_terminal_command_present(artifact: &Value) -> bool {
    artifact
        .pointer("/phase2_stree/command")
        .and_then(Value::as_str)
        .is_some_and(|command| !command.trim().is_empty())
}

fn phase2_stree_dispatch_key(topic_id: &str, actor: DebateActor, delivery_ids: &[&str]) -> String {
    format!("{topic_id}:{}:{}", actor.role(), delivery_ids.join(","))
}

fn phase2_terminal_tool_retry_injection(topic_id: &str, actor: DebateActor) -> String {
    let terminal_tools = match actor {
        DebateActor::Bull | DebateActor::Bear => "submit_debate_turn",
        DebateActor::Controller => {
            "one of route_debate_turn, wait_for_debate_turn, or close_debate"
        }
    };
    format!(
        "stree: {}",
        json!({
            "trusted_protocol": "phase2_terminal_tool_retry",
            "topic_id": topic_id,
            "actor": actor.role(),
            "instruction": format!(
                "Your immediately preceding response did not invoke the required terminal tool. Do not emit prose or JSON text. Complete the already-delivered STree work item now by calling {terminal_tools} with a valid payload."
            ),
        })
    )
}

fn phase2_controller_close_retry_injection(topic_id: &str) -> String {
    format!(
        "stree: {}",
        json!({
            "trusted_protocol": "phase2_terminal_close_retry",
            "topic_id": topic_id,
            "actor": DebateActor::Controller.role(),
            "instruction": "Rust rejected the immediately preceding route because the debate must close: either the round cap is reached or the direct collision produced no newly observed evidence event. The complete final Controller delivery batch is already in this same session. Do not route or wait. Call close_debate now with a valid terminal reason and report that accounts for every delivered claim."
        })
    )
}

fn is_phase2_controller_close_required_error(error: &str) -> bool {
    error.contains("max_debate_rounds") || error.contains("no newly observed evidence event")
}

fn phase1_summary_validation_retry_instruction() -> String {
    "Rust rejected the previous Phase 1 Summary contract. Preserve every explicit probability from the Analyst; do not derive it from confidence. Enforce direction coherence: bullish must be >0.5, bearish <0.5, neutral and unobserved exactly 0.5, and mixed may be 0.4..=0.6. When the source report explicitly describes conflicting timeframes or mixed evidence while giving a probability in 0.4..=0.6, use mixed instead of neutral. For every ticker with observed evidence, retain at least one non-empty key_evidence item with its exact prior evidence_refs; never replace a previously non-empty key_evidence array with [] or drop a ticker while fixing another field. If context-only VIX has no cited evidence or every VIX evidence_refs array is empty, this is an unobserved data gap: set VIX direction to unobserved, keep long_probability at 0.5, set key_evidence to [], and record the absence in data_gaps or missing_fields. Never return neutral, bullish, bearish, or mixed for VIX with an empty key_evidence array. The JSON shape is strict: authoritative_fields.per_ticker may contain only ticker keys QQQ, SOXX, and VIX; cross_asset_findings is a sibling of per_ticker under authoritative_fields, never a per_ticker key. Every retained key_evidence.timestamp must be a non-empty ISO-8601 string, never null; optional event/publish/ingest/as_of fields may be null. If an item has no observed timestamp, remove it from key_evidence and record the gap instead of inventing a date. Return only the required JSON object and keep all evidence IDs unchanged.".to_owned()
}

/// Models occasionally place the shared cross-asset findings inside the
/// ticker map because the prompt shows both fields close together.  Keep the
/// canonical Index shape stable by moving only that known misplaced field;
/// every ticker artifact remains otherwise model-owned and fail-closed.
fn normalize_phase1_summary_layout(fields: &mut serde_json::Map<String, Value>) -> Result<()> {
    let nested_findings = fields
        .get_mut("per_ticker")
        .and_then(Value::as_object_mut)
        .and_then(|per_ticker| per_ticker.remove("cross_asset_findings"));
    let Some(nested_findings) = nested_findings else {
        return Ok(());
    };
    let nested_findings = nested_findings
        .as_array()
        .context("Phase 1 cross_asset_findings must be an array")?
        .clone();
    if let Some(existing) = fields.get_mut("cross_asset_findings") {
        let existing = existing
            .as_array_mut()
            .context("Phase 1 cross_asset_findings must be an array")?;
        existing.extend(nested_findings);
    } else {
        fields.insert(
            "cross_asset_findings".to_owned(),
            Value::Array(nested_findings),
        );
    }
    Ok(())
}

fn preserve_phase1_summary_key_evidence(
    fields: &mut serde_json::Map<String, Value>,
    previous: &Value,
) {
    let Some(current_reports) = fields.get_mut("per_ticker").and_then(Value::as_object_mut) else {
        return;
    };
    let Some(previous_reports) = previous.get("per_ticker").and_then(Value::as_object) else {
        return;
    };

    for (ticker, previous_report) in previous_reports {
        let Some(previous_evidence) = previous_report
            .get("key_evidence")
            .and_then(Value::as_array)
            .filter(|items| !items.is_empty())
        else {
            continue;
        };
        let Some(current_report) = current_reports
            .get_mut(ticker)
            .and_then(Value::as_object_mut)
        else {
            continue;
        };
        let current_is_empty = current_report
            .get("key_evidence")
            .and_then(Value::as_array)
            .is_none_or(|items| items.is_empty());
        if current_is_empty {
            current_report.insert(
                "key_evidence".to_owned(),
                Value::Array(previous_evidence.clone()),
            );
        }
    }
}

fn phase2_topic_generation_validation_retry_instruction() -> String {
    "Rust rejected the previous Phase 2 topic-generation Summary because its search-space audit was incomplete. Preserve the original topics and evidence IDs; do not invent new evidence. Return all five coverage categories exactly once: trend, valuation_expectations, macro, event_risk, data_quality. For each give status, non-empty reason, and evidence_refs. Keep every substantive candidate in candidate_topics and make every topics item reuse that candidate's exact decision_hinge. Put every candidate_only, residual_risk, or data_gap coverage status in residual_risks; its category may be the coverage category or that exact status label. Return only the required JSON object.".to_owned()
}

fn phase5_summary_validation_retry_instruction() -> String {
    "Rust rejected the previous Phase 5 Summary because it treated repeated prior risk as new information. Preserve only what the reviewer explicitly said. If no_new_information=true, set unique_risk_contribution and recommended_adjustment to empty strings and risk_dimension to null. Otherwise provide one non-empty contribution, one non-empty recommended adjustment, and exactly one risk_dimension from gap, liquidity, volatility, correlation, concentration, execution, data_quality, or other. Return only the required JSON object; do not invent evidence, thresholds, or a new market view.".to_owned()
}

fn phase6_summary_validation_retry_instruction() -> String {
    "Rust rejected the previous Phase 6 Summary because a purported binding risk control did not have a verified marginal Phase 5 contribution. Preserve Phase 3 probability/rating/thesis and Phase 4 direction. Do not cite repetitive no_new_information reviewers as binding controls. If no eligible Phase 5 reviewer changed a cap, emit no binding_risk_controls rather than inventing a source; otherwise cite only the one actual eligible Phase 5 Index for each control and keep max_target_weight at or below its recorded position cap. Return only the required JSON object.".to_owned()
}

fn apply_mock_phase2_stree_command(tree: &mut TopicDebateTree, actor: DebateActor) -> Result<()> {
    let initial = |side: DebateActor| {
        tree.nodes
            .iter()
            .find(|node| node.from == Some(side) && node.payload.get("reply_to_node_id").is_none())
            .map(|node| node.node_id.clone())
    };
    match actor {
        DebateActor::Bull | DebateActor::Bear => {
            let opponent = if actor == DebateActor::Bull {
                DebateActor::Bear
            } else {
                DebateActor::Bull
            };
            let mut payload = json!({
                "stance": "challenge", "message": "mock bounded debate position",
                "report": "Mock Phase 2 stree participant report", "evidence_refs": [],
                "evidence_links": []
            });
            if let Some(node_id) = initial(opponent) {
                payload["reply_to_node_id"] = json!(node_id);
            }
            tree.submit(actor, payload)?;
        }
        DebateActor::Controller => {
            let bull = initial(DebateActor::Bull);
            let bear = initial(DebateActor::Bear);
            let collision_complete =
                bull.as_ref()
                    .zip(bear.as_ref())
                    .is_some_and(|(bull, bear)| {
                        tree.nodes.iter().any(|node| {
                            node.from == Some(DebateActor::Bull)
                                && node.payload.get("reply_to_node_id").and_then(Value::as_str)
                                    == Some(bear)
                        }) && tree.nodes.iter().any(|node| {
                            node.from == Some(DebateActor::Bear)
                                && node.payload.get("reply_to_node_id").and_then(Value::as_str)
                                    == Some(bull)
                        })
                    });
            if collision_complete {
                tree.controller_close(json!({"reason":"unresolved_disagreement", "message":"mock Controller closed after direct collision", "report":"Mock close"}))?;
            } else if let Some(reply_to_node_id) = bear.or(bull) {
                tree.controller_route(json!({"targets":["bull","bear"], "reply_to_node_id":reply_to_node_id, "message":"respond to the opposing position", "report":"Mock route"}))?;
            } else {
                tree.controller_wait(
                    json!({"message":"wait for the other initial position", "report":"Mock wait"}),
                )?;
            }
        }
    }
    Ok(())
}

fn phase2_debate_debug_summary(state: &Value) -> Value {
    let mut topic_summaries = Vec::new();
    let mut final_controllers = serde_json::Map::new();
    let mut topic_ids = state
        .get("topic_debate_states")
        .and_then(Value::as_object)
        .map(|topics| topics.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    topic_ids.sort();

    for topic_id in topic_ids {
        let Some(topic_state) = state.pointer(&format!("/topic_debate_states/{topic_id}")) else {
            continue;
        };
        let stree = topic_state.get("stree").cloned().unwrap_or(Value::Null);
        let final_controller = topic_state
            .get("final_controller_artifact")
            .map(phase2_debug_artifact_summary)
            .unwrap_or(Value::Null);
        let stree_injections = phase2_stree_injection_views(&stree);
        final_controllers.insert(topic_id.clone(), final_controller.clone());
        topic_summaries.push(json!({
            "topic_id": topic_id,
            "topic": topic_state.get("topic").cloned().unwrap_or(Value::Null),
            "stree": stree,
            "stree_injections": stree_injections,
            "final_controller": final_controller,
            "source_session_turns": phase2_debug_source_turns(state, Some(&topic_id), None),
        }));
    }

    json!({
        "kind": "phase2_debate_process_summary",
        "phase": 2,
        "status": "completed",
        "run_id": state.get("run_id").cloned().unwrap_or(Value::Null),
        "identity_kind": "aggregate_runtime_view",
        "source_session_turns": phase2_debug_source_turns(state, None, None),
        "topic_generation": state
            .pointer("/topic_generation_artifact/topics")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "topic_count": topic_summaries.len(),
        "topics": topic_summaries,
        "final_controllers": final_controllers,
    })
}

/// A Phase 2 runtime view merges several independent agent sessions and can
/// never truthfully name one `session_id` / `turn_id`.  Carry the exact set of
/// source identities instead of assigning it the last writer's identity.
fn phase2_debug_source_turns(state: &Value, topic_id: Option<&str>, actor: Option<&str>) -> Value {
    let mut sources = state
        .get("role_job_metrics")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|metric| metric.get("phase").and_then(Value::as_i64) == Some(2))
        .filter(|metric| {
            topic_id.is_none_or(|topic| {
                metric.get("topic_id").and_then(Value::as_str) == Some(topic)
            })
        })
        .filter(|metric| {
            actor.is_none_or(|expected| {
                metric
                    .get("role")
                    .and_then(Value::as_str)
                    .is_some_and(|role| role == expected || role.starts_with(&format!("{expected}.")))
            })
        })
        .map(|metric| {
            json!({
                "run_id": metric.get("run_id").cloned().unwrap_or_else(|| state.get("run_id").cloned().unwrap_or(Value::Null)),
                "session_id": metric.get("session_id").cloned().unwrap_or(Value::Null),
                "turn_id": metric.get("turn_id").cloned().unwrap_or(Value::Null),
                "role": metric.get("role").cloned().unwrap_or(Value::Null),
                "topic_id": metric.get("topic_id").cloned().unwrap_or(Value::Null),
                "round": metric.get("round").cloned().unwrap_or(Value::Null),
            })
        })
        .collect::<Vec<_>>();
    sources.sort_by(|left, right| {
        serde_json::to_string(left)
            .unwrap_or_default()
            .cmp(&serde_json::to_string(right).unwrap_or_default())
    });
    sources.dedup();
    Value::Array(sources)
}

fn phase2_debug_artifact_summary(artifact: &Value) -> Value {
    json!({
        "profile": artifact.get("profile").cloned().unwrap_or(Value::Null),
        "unit_key": artifact.get("unit_key").cloned().unwrap_or(Value::Null),
        "payload": artifact
            .get("payload")
            .or_else(|| artifact.pointer("/phase2_stree/payload"))
            .cloned()
            .unwrap_or(Value::Null),
        "phase2_stree": artifact.get("phase2_stree").cloned().unwrap_or(Value::Null),
    })
}

fn phase2_stree_injection_views(stree: &Value) -> Value {
    let deliveries = stree
        .get("deliveries")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let nodes = stree
        .get("nodes")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    Value::Array(
        deliveries
            .into_iter()
            .filter_map(|delivery| {
                let node_id = delivery.get("node_id")?.as_str()?;
                let node = nodes
                    .iter()
                    .find(|node| node.get("node_id").and_then(Value::as_str) == Some(node_id))?;
                let message = json!({
                    "delivery_id": delivery.get("delivery_id").cloned().unwrap_or(Value::Null),
                    "node_id": node_id,
                    "sequence": node.get("sequence").cloned().unwrap_or(Value::Null),
                    "round": node.get("round").cloned().unwrap_or(Value::Null),
                    "from": node.get("from").cloned().unwrap_or(Value::Null),
                    "kind": node.get("kind").cloned().unwrap_or(Value::Null),
                    "payload": node.get("payload").cloned().unwrap_or(Value::Null),
                    "trusted_protocol": "phase2_topic_debate_tree"
                });
                Some(json!({
                    "target": delivery.get("target").cloned().unwrap_or(Value::Null),
                    "delivered": delivery.get("delivered").cloned().unwrap_or(Value::Null),
                    "user_message": format!("stree: {}", serde_json::to_string(&message).ok()?)
                }))
            })
            .collect(),
    )
}

fn write_phase2_debate_debug_summary(state: &Value) -> Result<()> {
    if state.get("debug").and_then(Value::as_bool) != Some(true) {
        return Ok(());
    }
    let project_root = default_project_root();
    let summary = phase2_debate_debug_summary(state);
    orchestrator_llm::append_debug_output_record(
        &project_root,
        Path::new("outputs/debug/phase2/summary/debate_process_summary.json"),
        "runtime:phase2_debate_summary",
        summary.clone(),
    )?;
    for topic in summary
        .get("topics")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(topic_id) = topic.get("topic_id").and_then(Value::as_str) else {
            continue;
        };
        let safe_topic_id: String = topic_id
            .chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                    ch
                } else {
                    '_'
                }
            })
            .collect();
        let topic_dir =
            if safe_topic_id.starts_with("topic-") || safe_topic_id.starts_with("topic_") {
                safe_topic_id
            } else {
                format!("topic-{safe_topic_id}")
            };
        for (actor, file) in [
            ("researcher.bull", "debate-bull.json"),
            ("researcher.bear", "debate-bear.json"),
            ("mediator.topic_controller", "topic-controller.json"),
        ] {
            orchestrator_llm::append_debug_output_record(
                &project_root,
                &Path::new("outputs/debug/phase2")
                    .join(&topic_dir)
                    .join(file),
                "runtime:phase2_stree",
                json!({
                    "kind": "phase2_stree_view", "phase": 2, "topic_id": topic_id,
                    "run_id": state.get("run_id").cloned().unwrap_or(Value::Null),
                    "identity_kind": "aggregate_runtime_view",
                    "actor": actor, "end_turn": true, "stree": topic.get("stree").cloned().unwrap_or(Value::Null),
                    "stree_injections": topic.get("stree_injections").cloned().unwrap_or(Value::Null),
                    "final_controller": topic.get("final_controller").cloned().unwrap_or(Value::Null),
                    "source_session_turns": phase2_debug_source_turns(state, Some(topic_id), Some(actor))
                }),
            )?;
        }
    }
    Ok(())
}

async fn run_phase3(
    state: &mut Value,
    runtime: &RuntimeConfig,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<()> {
    let first_attempt = run_unit(
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
    .await;
    let artifact = match first_attempt {
        Ok(artifact) => artifact,
        Err(error)
            if !state["mock"].as_bool().unwrap_or(false)
                && is_phase3_scenario_contract_error(&error) =>
        {
            // Scenario probability mass and long-outcome probability have
            // distinct semantics.  Do not silently rewrite either ledger in
            // Rust: return to the same Research Manager session once with the
            // rejected contract and require a new, internally consistent
            // decision.  The failed response remains in the session/debug
            // trace for audit.
            state["_phase3_research_validation_retry"] =
                Value::String(phase3_scenario_validation_retry_instruction(&error));
            let retry = run_unit(
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
            .await;
            state
                .as_object_mut()
                .map(|object| object.remove("_phase3_research_validation_retry"));
            retry.with_context(|| {
                format!(
                    "Phase 3 Research Manager correction failed after the first scenario contract error: {error}"
                )
            })?
        }
        Err(error) => return Err(error),
    };
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

fn is_phase3_scenario_contract_error(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("scenario probabilities imply long_probability")
        || message.contains("scenario probabilities must sum to 1")
        || message.contains("scenario conditional_long_probability")
        || message.contains("scenario conditional_long_probability must be ordered")
}

fn phase3_scenario_validation_retry_instruction(error: &anyhow::Error) -> String {
    format!(
        "## Rust scenario-contract correction required\n\nThe immediately preceding Decision was rejected: `{error}`. Rewrite the entire Decision using the same allowed evidence; do not invent a debate adjustment or change the Rust base. `probability` is the probability mass that a bull/base/bear regime occurs. `conditional_long_probability` is the chance that the long outcome occurs conditional on that regime. For every ticker, make scenario masses sum to 1 and make `long_probability` exactly equal to `Σ(probability × conditional_long_probability)` (six decimal places or fewer). Keep the semantic order `bull >= base >= bear` for conditional_long_probability. Do not use the invalid shortcut `bull + base = long` or assume the base conditional probability is always 0.5."
    )
}

#[cfg(test)]
fn controller_should_continue(
    controller: &mut Value,
    topic_state: &Value,
    topic_id: &str,
) -> Result<bool> {
    let Some(should_continue) = controller
        .pointer("/payload/soft_control/should_continue")
        .or_else(|| controller.pointer("/soft_control/should_continue"))
        .and_then(Value::as_bool)
    else {
        bail!("Topic Controller Summary omitted soft_control.should_continue");
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
    } else if requires_initial_collision(topic_state) {
        let fields = controller
            .pointer_mut("/payload")
            .and_then(Value::as_object_mut)
            .context("Topic Controller Summary payload must be an object")?;
        if ensure_initial_collision_route(fields, topic_state, topic_id)? {
            return Ok(true);
        }
        bail!(
            "Topic Controller cannot stop {topic_id} before Bull and Bear directly respond to routed opposing claims"
        );
    }
    Ok(should_continue)
}

fn ensure_initial_collision_route(
    fields: &mut serde_json::Map<String, Value>,
    topic_state: &Value,
    topic_id: &str,
) -> Result<bool> {
    if !requires_initial_collision(topic_state)
        || fields
            .get("soft_control")
            .and_then(|value| value.get("should_continue"))
            .and_then(Value::as_bool)
            != Some(false)
    {
        return Ok(false);
    }

    let bull_claim_id = first_initial_claim_id(topic_state, "researcher.bull.initial")
        .with_context(|| format!("initial Bull claim ID missing for {topic_id}"))?;
    let bear_claim_id = first_initial_claim_id(topic_state, "researcher.bear.initial")
        .with_context(|| format!("initial Bear claim ID missing for {topic_id}"))?;
    let hinge = fields
        .get("decision_hinges")
        .and_then(Value::as_array)
        .and_then(|hinges| hinges.first())
        .and_then(|hinge| hinge.get("hinge"))
        .and_then(Value::as_str)
        .or_else(|| {
            topic_state
                .pointer("/topic/decision_hinge")
                .and_then(Value::as_str)
        })
        .unwrap_or("the opposing seed claims' observable conditions");

    let steer = |target_side: &str, opponent_claim_id: &str| {
        json!({
            "steer_id": format!("{topic_id}:collision:{target_side}"),
            "target_side": target_side,
            "reply_to_claim_id": opponent_claim_id,
            "opponent_claim_id": opponent_claim_id,
            "hinge": hinge,
            "expected_stance": "rebut",
            "observable_boundary": "Directly respond to the opposing seed claim and state a falsifiable boundary."
        })
    };
    fields.insert(
        "next_steers".to_owned(),
        json!([steer("bull", &bear_claim_id), steer("bear", &bull_claim_id)]),
    );
    let soft_control = fields
        .get_mut("soft_control")
        .and_then(Value::as_object_mut)
        .context("Topic Controller Summary soft_control must be an object")?;
    soft_control.insert("should_continue".to_owned(), Value::Bool(true));
    soft_control.insert(
        "stop_reason".to_owned(),
        Value::String(
            "Rust enforced the mandatory first collision: Bull and Bear must directly respond to the opposing seed claims before the debate can stop."
                .to_owned(),
        ),
    );
    Ok(true)
}

fn first_initial_claim_id(topic_state: &Value, role: &str) -> Option<String> {
    topic_state
        .get("turns")
        .and_then(Value::as_array)
        .and_then(|turns| {
            turns.iter().find_map(|turn| {
                (turn.get("role").and_then(Value::as_str) == Some(role))
                    .then(|| {
                        turn.pointer("/artifact/payload/claims")
                            .and_then(Value::as_array)
                            .and_then(|claims| {
                                claims.iter().find_map(|claim| {
                                    claim
                                        .get("claim_id")
                                        .and_then(Value::as_str)
                                        .filter(|id| !id.trim().is_empty())
                                        .map(ToOwned::to_owned)
                                })
                            })
                    })
                    .flatten()
            })
        })
}

fn requires_initial_collision(topic_state: &Value) -> bool {
    let turns = topic_state
        .get("turns")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let has_initial_claims = ["researcher.bull.initial", "researcher.bear.initial"]
        .into_iter()
        .all(|role| {
            turns.iter().any(|turn| {
                turn.get("role").and_then(Value::as_str) == Some(role)
                    && turn
                        .pointer("/artifact/payload/claims")
                        .and_then(Value::as_array)
                        .is_some_and(|claims| !claims.is_empty())
            })
        });
    if !has_initial_claims {
        return false;
    }
    let bull_replied_to_bear = turns.iter().any(|turn| {
        turn.get("role").and_then(Value::as_str) == Some("researcher.bull.interaction")
            && turn
                .pointer("/artifact/payload/replies")
                .and_then(Value::as_array)
                .is_some_and(|replies| {
                    replies.iter().any(|reply| {
                        reply
                            .get("reply_to_claim_id")
                            .and_then(Value::as_str)
                            .is_some_and(|id| id.contains(":bear:"))
                    })
                })
    });
    let bear_replied_to_bull = turns.iter().any(|turn| {
        turn.get("role").and_then(Value::as_str) == Some("researcher.bear.interaction")
            && turn
                .pointer("/artifact/payload/replies")
                .and_then(Value::as_array)
                .is_some_and(|replies| {
                    replies.iter().any(|reply| {
                        reply
                            .get("reply_to_claim_id")
                            .and_then(Value::as_str)
                            .is_some_and(|id| id.contains(":bull:"))
                    })
                })
    });
    !(bull_replied_to_bear && bear_replied_to_bull)
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
    let roles = ["risk.aggressive", "risk.neutral", "risk.conservative"];
    let history = run_parallel_units(
        state,
        runtime,
        roles
            .iter()
            .map(|role| UnitSpec {
                role: (*role).to_owned(),
                phase: 5,
                kind: "artifact".to_owned(),
                round: None,
                topic_id: None,
                ticker: None,
            })
            .collect(),
        roles.len(),
        model,
        reasoning,
    )
    .await?;
    let reviewer_independence =
        phase5_reviewer_independence_ledger(&history, &investable_assets_from_state(state))?;
    state["risk_debate_state"] = json!({
        "history": history,
        "reviewer_independence": reviewer_independence,
        "authority": "file_store"
    });
    Ok(())
}

/// Phase 5 reviewers are different perspectives over the same frozen
/// Phase 3/4 inputs, not statistically independent market observations.  This
/// ledger proves whether a reviewer supplied a unique, numerically marginal
/// position cap by removing it from the cap envelope one reviewer at a time.
/// Phase 6 may bind only a source that survives both checks.
fn phase5_reviewer_independence_ledger(
    history: &[Value],
    investable_assets: &[String],
) -> Result<Value> {
    let mut reviewers = Vec::new();
    let mut dimension_counts = BTreeMap::<String, usize>::new();
    for artifact in history {
        let role = artifact
            .get("role")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .context("Phase 5 reviewer artifact is missing role")?
            .to_owned();
        let index_id = artifact
            .get("index_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .context("Phase 5 reviewer artifact is missing finalized index_id")?
            .to_owned();
        let payload = artifact
            .get("payload")
            .and_then(Value::as_object)
            .context("Phase 5 reviewer artifact is missing payload")?;
        let no_new_information = payload
            .get("no_new_information")
            .and_then(Value::as_bool)
            .context("Phase 5 reviewer payload is missing no_new_information")?;
        let risk_dimension = payload
            .get("risk_dimension")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        if !no_new_information {
            let dimension = risk_dimension.as_ref().context(
                "Phase 5 incremental reviewer is missing risk_dimension after validation",
            )?;
            *dimension_counts.entry(dimension.clone()).or_default() += 1;
        }
        reviewers.push(json!({
            "role": role,
            "index_id": index_id,
            "no_new_information": no_new_information,
            "risk_dimension": risk_dimension,
            "unique_risk_contribution": payload.get("unique_risk_contribution").cloned().unwrap_or(Value::Null),
            "per_asset": payload.get("per_asset").cloned().unwrap_or_else(|| json!({})),
        }));
    }

    for reviewer in &mut reviewers {
        let no_new_information = reviewer
            .get("no_new_information")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let risk_dimension = reviewer
            .get("risk_dimension")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let dimension_is_unique = risk_dimension
            .as_ref()
            .is_some_and(|dimension| dimension_counts.get(dimension) == Some(&1));
        let contribution_is_independent = !no_new_information && dimension_is_unique;
        reviewer["contribution_is_independent"] = json!(contribution_is_independent);
        reviewer["independence_reason"] = json!(if no_new_information {
            "reviewer_declared_no_incremental_constraint"
        } else if dimension_is_unique {
            "unique_declared_risk_dimension"
        } else {
            "risk_dimension_repeated_by_another_reviewer"
        });
    }

    let mut per_asset = serde_json::Map::new();
    for ticker in investable_assets {
        let candidates = reviewers
            .iter()
            .filter(|reviewer| {
                reviewer
                    .get("contribution_is_independent")
                    .and_then(Value::as_bool)
                    == Some(true)
            })
            .filter_map(|reviewer| {
                let position_cap_pct = reviewer
                    .pointer(&format!("/per_asset/{ticker}/position_cap_pct"))
                    .and_then(Value::as_f64)
                    .filter(|cap| cap.is_finite() && (0.0..=1.0).contains(cap))?;
                Some((
                    reviewer.get("index_id")?.as_str()?.to_owned(),
                    reviewer.get("role")?.as_str()?.to_owned(),
                    position_cap_pct,
                ))
            })
            .collect::<Vec<_>>();
        let full_effective_position_cap_pct = candidates
            .iter()
            .map(|(_, _, cap)| *cap)
            .min_by(|left, right| left.total_cmp(right));
        let leave_one_reviewer_out = candidates
            .iter()
            .map(|(index_id, role, cap)| {
                let cap_without = candidates
                    .iter()
                    .filter(|(other_index_id, _, _)| other_index_id != index_id)
                    .map(|(_, _, other_cap)| *other_cap)
                    .min_by(|left, right| left.total_cmp(right));
                let marginal = full_effective_position_cap_pct.is_some_and(|full| {
                    cap_without
                        .map(|without| full + 0.000_001 < without)
                        .unwrap_or(true)
                });
                json!({
                    "index_id": index_id,
                    "role": role,
                    "position_cap_pct": cap,
                    "full_effective_position_cap_pct": full_effective_position_cap_pct,
                    "position_cap_pct_without_reviewer": cap_without,
                    "marginal": marginal,
                    "reason": if marginal {
                        "removing_this_reviewer_relaxes_the_effective_position_cap"
                    } else {
                        "removing_this_reviewer_does_not_change_the_effective_position_cap"
                    }
                })
            })
            .collect::<Vec<_>>();
        let eligible_source_refs = leave_one_reviewer_out
            .iter()
            .filter(|entry| entry.get("marginal").and_then(Value::as_bool) == Some(true))
            .filter_map(|entry| entry.get("index_id").and_then(Value::as_str))
            .collect::<Vec<_>>();
        per_asset.insert(
            ticker.clone(),
            json!({
                "full_effective_position_cap_pct": full_effective_position_cap_pct,
                "leave_one_reviewer_out": leave_one_reviewer_out,
                "eligible_source_refs": eligible_source_refs,
            }),
        );
    }

    Ok(json!({
        "authority": "rust_phase5_leave_one_reviewer_out_v1",
        "shared_input_phases": [3, 4],
        "reviewers": reviewers,
        "per_asset": per_asset,
    }))
}

async fn ensure_execution_account_snapshot(
    state: &mut Value,
    runtime: &RuntimeConfig,
    args: &ExecArgs,
) -> Result<()> {
    // A persisted Paper snapshot is only a checkpoint, never an execution
    // authorization. Refresh it immediately before Phase 6/7 so a resumed
    // run cannot use stale positions or buying power to build an order plan.
    // Debug snapshots are deterministic and isolated, so retaining one makes
    // same-namespace recovery reproducible without contacting Alpaca.
    if args.debug {
        if let Some(existing) = state
            .get("account_snapshot")
            .filter(|value| !value.is_null())
        {
            let snapshot: AccountSnapshot = serde_json::from_value(existing.clone())
                .context("stored account_snapshot is invalid")?;
            state["current_portfolio_weights"] = serde_json::to_value(snapshot.current_weights)?;
            return Ok(());
        }
    }
    let investable = investable_assets_from_state(state);
    let snapshot = if args.debug {
        debug_account_snapshot(&investable, runtime.alpaca_debug_starting_cash)?
    } else {
        let purpose = args
            .run_purpose
            .map(Into::into)
            .unwrap_or(runtime.evaluation.default_run_purpose);
        if purpose != RunPurpose::Paper {
            bail!(
                "Alpaca execution supports Paper runs only; use --debug for simulation or --run-purpose paper"
            )
        }
        let credentials = alpaca_credentials(
            runtime.alpaca_api_key.as_deref(),
            runtime.alpaca_api_secret.as_deref(),
        )?;
        load_alpaca_account_snapshot(&credentials, &investable).await?
    };
    state["current_portfolio_weights"] = serde_json::to_value(snapshot.current_weights.clone())?;
    state["account_snapshot"] = serde_json::to_value(snapshot)?;
    checkpoint_state(state)
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
    let mut per_asset = per_asset;
    enrich_final_trade_decision_fields(state, &mut per_asset)?;
    state["final_trade_decision"] = json!({"per_asset": per_asset, "authority": "file_store"});
    Ok(())
}

fn enrich_final_trade_decision_fields(state: &Value, per_asset: &mut Value) -> Result<()> {
    let research_per_ticker = state
        .pointer("/research_plan/per_ticker")
        .and_then(Value::as_object)
        .context("final trade decision enrichment requires research_plan.per_ticker")?;
    let trader_per_ticker = state
        .pointer("/trader_investment_plan/per_ticker")
        .and_then(Value::as_object)
        .context("final trade decision enrichment requires trader_investment_plan.per_ticker")?;
    let decisions = per_asset
        .as_object_mut()
        .context("finalized portfolio per_asset must be an object")?;

    for (ticker, decision) in decisions {
        let research = research_per_ticker
            .get(ticker)
            .with_context(|| format!("research_plan missing ticker {ticker}"))?;
        let long_probability = research
            .get("long_probability")
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
            .with_context(|| format!("research_plan long_probability missing for {ticker}"))?;
        let short_probability = research
            .get("short_probability")
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
            .with_context(|| format!("research_plan short_probability missing for {ticker}"))?;
        let trader = trader_per_ticker
            .get(ticker)
            .with_context(|| format!("trader_investment_plan missing ticker {ticker}"))?;
        let entry_conditions = trader
            .get("execution_conditions")
            .cloned()
            .unwrap_or_else(|| json!([]));
        if !entry_conditions.is_array() {
            bail!("trader execution_conditions must be an array for {ticker}");
        }
        let blockers = trader.get("blockers").cloned().unwrap_or_else(|| json!([]));
        if !blockers.is_array() {
            bail!("trader blockers must be an array for {ticker}");
        }
        let decision = decision
            .as_object_mut()
            .with_context(|| format!("final portfolio decision must be an object for {ticker}"))?;
        decision.insert("long_probability".to_owned(), json!(long_probability));
        decision.insert("short_probability".to_owned(), json!(short_probability));
        decision.insert("inherited_probability".to_owned(), json!(long_probability));
        decision.insert(
            "direction".to_owned(),
            trader.get("action").cloned().unwrap_or(Value::Null),
        );
        decision.insert("entry_conditions".to_owned(), entry_conditions);
        decision.insert(
            "entry_price".to_owned(),
            trader.get("entry_price").cloned().unwrap_or(Value::Null),
        );
        decision.insert(
            "stop_loss".to_owned(),
            trader.get("stop_loss").cloned().unwrap_or(Value::Null),
        );
        decision.insert(
            "invalidation_conditions".to_owned(),
            json!({
                "downgrade_reason": trader
                    .get("downgrade_reason")
                    .cloned()
                    .unwrap_or(Value::Null),
                "blockers": blockers,
            }),
        );
    }
    Ok(())
}

fn inject_runtime_current_weights_into_final_decision(state: &mut Value) -> Result<()> {
    let investable = investable_assets_from_state(state);
    let weights = state
        .get("current_portfolio_weights")
        .and_then(Value::as_object)
        .context("runtime current_portfolio_weights are required before Phase 7")?
        .clone();
    let per_asset = state
        .pointer_mut("/final_trade_decision/per_asset")
        .and_then(Value::as_object_mut)
        .context("final_trade_decision.per_asset is required before Phase 7")?;
    for ticker in investable {
        let current_weight = weights
            .get(&ticker)
            .and_then(Value::as_f64)
            .with_context(|| format!("runtime current weight missing for {ticker}"))?;
        let decision = per_asset
            .get_mut(&ticker)
            .and_then(Value::as_object_mut)
            .with_context(|| format!("Phase 6 decision missing for {ticker}"))?;
        decision.insert("current_weight".to_owned(), json!(current_weight));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase7ExecutionMode {
    DisabledMock,
    BlockedAllocation,
    SimulatedDebug,
    SubmitPaper,
    PlannedConfigDisabled,
    PlannedExplicitAuthorizationRequired,
}

fn phase7_execution_mode(
    mock: bool,
    debug: bool,
    allocation_failed: bool,
    config_order_submission_enabled: bool,
    explicit_submit_orders: bool,
) -> Phase7ExecutionMode {
    if mock {
        Phase7ExecutionMode::DisabledMock
    } else if allocation_failed {
        Phase7ExecutionMode::BlockedAllocation
    } else if debug {
        Phase7ExecutionMode::SimulatedDebug
    } else if !config_order_submission_enabled {
        Phase7ExecutionMode::PlannedConfigDisabled
    } else if !explicit_submit_orders {
        Phase7ExecutionMode::PlannedExplicitAuthorizationRequired
    } else {
        Phase7ExecutionMode::SubmitPaper
    }
}

async fn run_phase7(
    store: &FileStore,
    location: &RunLocation,
    state: &mut Value,
    runtime: &RuntimeConfig,
    args: &ExecArgs,
) -> Result<Value> {
    if !args.mock {
        inject_runtime_current_weights_into_final_decision(state)?;
    }
    let context = compute_allocation_context(state, &runtime.allocation)?;
    let (allocation, allocation_failure) =
        match derive_guarded_allocation(state, &context, &runtime.allocation) {
            Ok(allocation) => (allocation, None),
            Err(error) => {
                let failure = error.to_string();
                state["degraded"] = Value::Bool(true);
                if !state["errors"].is_array() {
                    state["errors"] = json!([]);
                }
                state["errors"]
                    .as_array_mut()
                    .expect("errors initialized as array")
                    .push(json!({
                        "phase": 7,
                        "kind": "allocation_blocked",
                        "failure": failure.clone(),
                    }));
                (cash_only_allocation(&context, &failure), Some(failure))
            }
        };
    state["allocation_context"] = context;
    state["portfolio_allocation"] = allocation.clone();
    let execution_mode = phase7_execution_mode(
        args.mock,
        args.debug,
        allocation_failure.is_some(),
        runtime.alpaca_order_submission_enabled,
        args.submit_orders,
    );
    match execution_mode {
        Phase7ExecutionMode::DisabledMock => {
            state["order_plan"] = json!({
                "status": "disabled_mock",
                "account_equity": null,
                "orders": [],
                "skipped": [],
            });
            state["execution_report"] = json!({
                "status": "disabled_mock",
                "simulated": true,
                "receipts": [],
            });
        }
        Phase7ExecutionMode::BlockedAllocation => {
            let failure = allocation_failure
                .as_deref()
                .expect("blocked allocation mode requires an allocation error");
            state["order_plan"] = json!({
                "status": "blocked_allocation_invalid",
                "account_equity": null,
                "orders": [],
                "skipped": [{
                    "symbol": "*",
                    "reason": failure,
                    "estimated_notional": 0.0,
                }],
            });
            state["execution_report"] = json!({
                "status": "blocked_allocation_invalid",
                "simulated": args.debug,
                "receipts": [],
            });
        }
        _ => {
            let account: AccountSnapshot = serde_json::from_value(
                state
                    .get("account_snapshot")
                    .cloned()
                    .context("Phase 7 requires an account snapshot")?,
            )
            .context("stored account_snapshot is invalid")?;
            let plan = build_order_plan(
                state["run_id"].as_str().context("run_id is required")?,
                &allocation,
                state.get("market_snapshot"),
                &account,
            )?;
            state["order_plan"] = serde_json::to_value(&plan)?;
            // Persist the exact plan and deterministic client_order_ids before any
            // remote mutation. A retry queries Alpaca by client_order_id first.
            checkpoint_state(state)?;
            let report = match execution_mode {
                Phase7ExecutionMode::SimulatedDebug => {
                    submit_order_plan(&plan, &account, None, true).await?
                }
                Phase7ExecutionMode::SubmitPaper => {
                    let credentials = alpaca_credentials(
                        runtime.alpaca_api_key.as_deref(),
                        runtime.alpaca_api_secret.as_deref(),
                    )?;
                    submit_order_plan(&plan, &account, Some(&credentials), false).await?
                }
                Phase7ExecutionMode::PlannedConfigDisabled => ExecutionReport {
                    status: "planned_submission_disabled_by_config".to_owned(),
                    simulated: false,
                    receipts: Vec::new(),
                },
                Phase7ExecutionMode::PlannedExplicitAuthorizationRequired => ExecutionReport {
                    status: "planned_submission_not_authorized".to_owned(),
                    simulated: false,
                    receipts: Vec::new(),
                },
                Phase7ExecutionMode::DisabledMock | Phase7ExecutionMode::BlockedAllocation => {
                    unreachable!("handled before building a Phase 7 order plan")
                }
            };
            state["execution_report"] = serde_json::to_value(report)?;
            tracing::info!(
                order_plan = %serde_json::to_string(&state["order_plan"])?,
                "Phase 7 order plan"
            );
            tracing::info!(
                execution_report = %serde_json::to_string(&state["execution_report"])?,
                "Phase 7 execution result"
            );
        }
    }
    let response_text = serde_json::to_string(&json!({
        "account_snapshot": state.get("account_snapshot").cloned().unwrap_or(Value::Null),
        "allocation_context": state["allocation_context"],
        "allocation": allocation,
        "order_plan": state["order_plan"],
        "execution_report": state["execution_report"],
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
            summary: if allocation_failure.is_some() {
                "Rust 拒绝了无效的受约束组合分配，未生成或提交订单。".to_owned()
            } else {
                "Rust 已完成受约束的组合分配。".to_owned()
            },
            confidence: if allocation_failure.is_some() {
                0.0
            } else {
                1.0
            },
            authoritative_fields: serde_json::from_value(json!({
                "account_snapshot": state.get("account_snapshot").cloned().unwrap_or(Value::Null),
                "allocation_context": state["allocation_context"],
                "allocation": allocation,
                "order_plan": state["order_plan"],
                "execution_report": state["execution_report"],
            }))?,
            details: vec![PhaseIndexCandidateDetail {
                section: "execution".to_owned(),
                detail: String::new(),
                source_refs: read_all_indexes(
                    store,
                    Some(location),
                    &IndexQuery {
                        kind: Some(IndexKind::PhaseSummary),
                        source_phase: Some(6),
                        ..Default::default()
                    },
                )?
                .into_iter()
                .map(|index| index.index_id)
                .collect(),
            }],
            missing_fields: allocation_failure
                .as_ref()
                .map(|_| vec!["guarded_allocation".to_owned()])
                .unwrap_or_default(),
            ambiguities: allocation_failure.into_iter().collect(),
        },
    )?;
    let artifact = serde_json::to_value(artifact)?;
    state["allocation_artifact"] = artifact.clone();
    state["allocation_result"] = artifact.clone();
    Ok(artifact)
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
    run_unit_with_checkpoint(
        state, runtime, role, phase, kind, round, topic_id, ticker, model, reasoning, true,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_unit_with_checkpoint(
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
    checkpoint: bool,
) -> Result<Value> {
    let completed_key = completed_unit_key(role, phase, kind, round, topic_id, ticker);
    let cacheable_unit = is_cacheable_unit(phase, kind);
    if !cacheable_unit {
        // A stree dispatch is a mailbox event, not a repeatable unit.  Its
        // FileStore session identity is stable; each delivery gets its own stable
        // turn key so retries resume that delivery while later deliveries in the
        // same role/round execute in a fresh turn with the full prior history.
        if let Some(units) = state
            .get_mut("_completed_units")
            .and_then(Value::as_object_mut)
        {
            units.remove(&completed_key);
        }
    }
    let force_phase_recompute = state
        .get(format!("_force_phase{phase}_recompute"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if cacheable_unit {
        if let Some(artifact) = state
            .get("_completed_units")
            .and_then(|units| units.get(&completed_key))
            .cloned()
        {
            let needs_retrieval_audit = phase == 3
                && role == "manager.research"
                && !retrieval_audit_covers_required_source_phases(artifact.get("retrieval_audit"));
            if !needs_retrieval_audit && !force_phase_recompute {
                return Ok(artifact);
            }
        }
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
    if result.artifact.is_none() {
        // The normal success checkpoint happens after compilation below.  A
        // failed role has no later success path, so persist its metrics before
        // returning; otherwise the manifest/session show the failure while
        // state.json silently loses the terminal role attempt.
        if checkpoint {
            checkpoint_state(state).context("failed to persist terminal role failure metrics")?;
        }
    }
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
    let retrieval_audit = raw.get("retrieval_audit").cloned();
    let mut artifact = if defers_phase_summary(phase, kind) {
        json!({
            "phase": phase,
            "role": role,
            "profile": phase2_profile_name(role, kind),
            "ticker": ticker,
            "topic_id": topic_id,
            "response_text": response_text,
            "authority": "session"
        })
    } else {
        let compiled = match compile_unit_response(
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
            persists_phase_index(phase, kind),
        )
        .await
        {
            Ok(artifact) => Ok(artifact),
            Err(error)
                if phase == 1
                    && kind == "artifact"
                    && role.starts_with("analyst.")
                    && !state["mock"].as_bool().unwrap_or(false) =>
            {
                // A Summary compiler can faithfully copy a natural-language
                // direction while missing the machine contract's distinction
                // between `neutral=0.5` and `mixed=0.4..=0.6`.  Retry the same
                // persisted Summary session once with a Rust-owned correction;
                // never silently coerce the probability in the reducer.
                state["_phase1_summary_validation_retry"] =
                    Value::String(phase1_summary_validation_retry_instruction());
                let retry = compile_unit_response(
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
                    persists_phase_index(phase, kind),
                )
                .await;
                if let Some(object) = state.as_object_mut() {
                    object.remove("_phase1_summary_validation_retry");
                    object.remove("_phase1_summary_retry_candidate");
                }
                retry.with_context(|| {
                    format!(
                        "Phase 1 Summary correction failed after the first contract error: {error}"
                    )
                })
            }
            Err(error)
                if phase == 2
                    && kind == "topic_generation"
                    && !state["mock"].as_bool().unwrap_or(false) =>
            {
                // The topic queue is deliberately bounded, but the prior
                // search space must remain auditable.  Give the same Summary
                // session one correction rather than silently inventing or
                // dropping residual risks in Rust.
                state["_phase2_topic_generation_validation_retry"] =
                    Value::String(phase2_topic_generation_validation_retry_instruction());
                let retry = compile_unit_response(
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
                    persists_phase_index(phase, kind),
                )
                .await;
                state
                    .as_object_mut()
                    .map(|object| object.remove("_phase2_topic_generation_validation_retry"));
                retry.with_context(|| {
                    format!(
                        "Phase 2 topic-generation Summary correction failed after the first contract error: {error}"
                    )
                })
            }
            Err(error)
                if phase == 5
                    && kind == "artifact"
                    && !state["mock"].as_bool().unwrap_or(false) =>
            {
                // A reviewer may use natural language such as "no new
                // information" while still restating an old cap.  Keep the
                // raw risk view intact, but make the Summary state its
                // incremental contribution explicitly instead of silently
                // treating correlated prose as an independent control.
                state["_phase5_summary_validation_retry"] =
                    Value::String(phase5_summary_validation_retry_instruction());
                let retry = compile_unit_response(
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
                    persists_phase_index(phase, kind),
                )
                .await;
                state
                    .as_object_mut()
                    .map(|object| object.remove("_phase5_summary_validation_retry"));
                retry.with_context(|| {
                    format!(
                        "Phase 5 Summary correction failed after the first contract error: {error}"
                    )
                })
            }
            Err(error)
                if phase == 6
                    && kind == "artifact"
                    && !state["mock"].as_bool().unwrap_or(false) =>
            {
                state["_phase6_summary_validation_retry"] =
                    Value::String(phase6_summary_validation_retry_instruction());
                let retry = compile_unit_response(
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
                    persists_phase_index(phase, kind),
                )
                .await;
                state
                    .as_object_mut()
                    .map(|object| object.remove("_phase6_summary_validation_retry"));
                retry.with_context(|| {
                    format!(
                        "Phase 6 Summary correction failed after the first contract error: {error}"
                    )
                })
            }
            Err(error) => Err(error),
        }?;
        compiled
    };
    if let Some(retrieval_audit) = retrieval_audit {
        artifact["retrieval_audit"] = retrieval_audit;
    }
    if let Some(stree) = raw.get("phase2_stree") {
        artifact["phase2_stree"] = stree.clone();
    }
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
    if cacheable_unit {
        state["_completed_units"][completed_key] = artifact.clone();
    }
    if checkpoint {
        checkpoint_state(state)?;
    }
    Ok(artifact)
}

/// Run independent units concurrently while keeping Rust-owned compilation and
/// state publication deterministic. Each worker receives a private state
/// snapshot, so it may run the model and Summary compiler without racing on
/// `state.json`; the deltas are merged in the caller's input order afterwards.
async fn run_parallel_units(
    state: &mut Value,
    runtime: &RuntimeConfig,
    specs: Vec<UnitSpec>,
    parallelism: usize,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<Vec<Value>> {
    if specs.is_empty() {
        return Ok(Vec::new());
    }
    let base_state = state.clone();
    let base_metric_count = state
        .get("role_job_metrics")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    let base_error_count = state
        .get("errors")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    let outcomes = stream::iter(specs.into_iter().enumerate().map(|(index, spec)| {
        let mut worker_state = base_state.clone();
        async move {
            let result = run_unit_with_checkpoint(
                &mut worker_state,
                runtime,
                &spec.role,
                spec.phase,
                &spec.kind,
                spec.round,
                spec.topic_id.as_deref(),
                spec.ticker.as_deref(),
                model,
                reasoning,
                false,
            )
            .await;
            (index, spec, result, worker_state)
        }
    }))
    .buffer_unordered(parallelism.max(1))
    .collect::<Vec<_>>()
    .await;

    let mut outcomes = outcomes;
    outcomes.sort_by_key(|(index, _, _, _)| *index);
    let mut artifacts = Vec::with_capacity(outcomes.len());
    let mut first_error: Option<Error> = None;
    for (_, spec, result, worker_state) in outcomes {
        merge_parallel_state_delta(
            state,
            &worker_state,
            base_metric_count,
            base_error_count,
            spec.topic_id.as_deref(),
        );
        match result {
            Ok(artifact) => artifacts.push(artifact),
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error.context(format!(
                        "{} phase {} {} failed",
                        spec.role, spec.phase, spec.kind
                    )));
                }
            }
        }
    }
    refresh_role_job_metrics(state);
    if let Some(error) = first_error {
        // A failed worker did not checkpoint its private state. Publish the
        // merged metrics and session identities before surfacing the failure.
        checkpoint_state(state).context("failed to persist parallel unit failure metrics")?;
        return Err(error);
    }
    Ok(artifacts)
}

fn merge_parallel_state_delta(
    state: &mut Value,
    worker_state: &Value,
    base_metric_count: usize,
    base_error_count: usize,
    topic_id: Option<&str>,
) {
    append_array_delta(state, worker_state, "role_job_metrics", base_metric_count);
    append_array_delta(state, worker_state, "errors", base_error_count);
    merge_object_delta(state, worker_state, "_runtime_sessions");
    merge_object_delta(state, worker_state, "_completed_units");
    if worker_state
        .get("degraded")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        state["degraded"] = Value::Bool(true);
    }
    if let Some(topic_id) = topic_id {
        if let Some(topic_state) = worker_state
            .get("topic_debate_states")
            .and_then(Value::as_object)
            .and_then(|topics| topics.get(topic_id))
        {
            state["topic_debate_states"][topic_id] = topic_state.clone();
        }
    }
}

fn append_array_delta(state: &mut Value, worker_state: &Value, key: &str, base_len: usize) {
    let Some(worker_values) = worker_state.get(key).and_then(Value::as_array) else {
        return;
    };
    let delta = worker_values
        .iter()
        .skip(base_len)
        .cloned()
        .collect::<Vec<_>>();
    if delta.is_empty() {
        return;
    }
    if !state.get(key).is_some_and(Value::is_array) {
        state[key] = json!([]);
    }
    state[key]
        .as_array_mut()
        .expect("parallel state delta array initialized")
        .extend(delta);
}

fn merge_object_delta(state: &mut Value, worker_state: &Value, key: &str) {
    let Some(worker_object) = worker_state.get(key).and_then(Value::as_object) else {
        return;
    };
    if !state.get(key).is_some_and(Value::is_object) {
        state[key] = json!({});
    }
    let target = state[key]
        .as_object_mut()
        .expect("parallel state delta object initialized");
    for (key, value) in worker_object {
        target.insert(key.clone(), value.clone());
    }
}

fn defers_phase_summary(phase: i64, kind: &str) -> bool {
    phase == 2 && kind == "stree_turn"
}

fn is_cacheable_unit(phase: i64, kind: &str) -> bool {
    !defers_phase_summary(phase, kind)
}

fn persists_phase_index(phase: i64, kind: &str) -> bool {
    phase != 2 || kind == "phase2_final"
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
    persist_phase_index: bool,
) -> Result<Value> {
    let phase_u8 = u8::try_from(phase).context("compiled phase must fit u8")?;
    // A Phase 1 Detail is part of the cross-phase interface.  Keep the raw
    // terminal artifact in its session/debug record, but do not let a model-
    // invented stable-looking ID become visible to Phase 2 merely because it
    // appeared in an otherwise useful analyst report.  Only IDs emitted by
    // the Rust-owned Phase 1 tool markers survive this projection.
    let (phase1_detail_response_text, phase1_reference_projections) =
        if phase_u8 == 1 && !state["mock"].as_bool().unwrap_or(false) {
            canonicalize_phase1_cross_phase_source(response_text)?
        } else {
            (response_text.to_owned(), Vec::new())
        };
    // The Topic Generator is allowed to write a human-readable display
    // abbreviation such as `technical-abcd...` in its prose.  The temporary
    // Phase 2 extractor, however, must preserve full stable IDs.  Expand only
    // an unambiguous abbreviation *before* that extractor sees the source so
    // it does not discard a legitimate candidate simply because it cannot
    // infer the omitted digest.  The original Agent response remains the
    // Detail below; this string is an explicitly recorded compiler input
    // projection, not a rewrite of the terminal artifact.
    let (summary_response_text, phase2_reference_projections) = if phase_u8 == 2
        && kind == "topic_generation"
        && !state["mock"].as_bool().unwrap_or(false)
    {
        let store_root = state
            .get("store_root")
            .and_then(Value::as_str)
            .context("Phase 2 topic-generation Summary requires store_root")?;
        let store = FileStore::open(store_root, FileStoreOptions::default())?;
        let location = run_location_from_state(state)?;
        let known_references = phase2_initial_evidence_event_clusters(state, &store, &location)?;
        canonicalize_phase2_topic_generation_summary_source(response_text, &known_references)?
    } else {
        (phase1_detail_response_text.clone(), Vec::new())
    };
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
            "response_text": summary_response_text,
        });
        let summary_role = format!("compressor.phase{phase}");
        let prompt_path = runtime
            .prompts
            .path_for(&summary_role)
            .with_context(|| format!("missing dedicated Phase {phase} Summary prompt"))?
            .clone();
        let compiler_kind = if phase == 2 && !persist_phase_index {
            "phase2_extraction"
        } else {
            "phase_summary"
        };
        let job = prepare_role_job(RoleRun {
            state: scoped,
            role: "compressor.phase_summary",
            phase,
            kind: compiler_kind,
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
        if phase_u8 == 1 {
            normalize_phase1_summary_layout(&mut candidate.authoritative_fields)?;
        }
        // The Summary model compresses only the Index fields.  Preserve the
        // original free-text response exactly once in the Rust-owned Detail;
        // asking the model to copy it again wastes output budget and can cause
        // long Phase 2 reports to terminate with finish_reason=Length.
        candidate.details = vec![
            crate::orchestration::summary_store::PhaseIndexCandidateDetail {
                section: "analysis".to_owned(),
                detail: phase1_detail_response_text,
                source_refs: Vec::new(),
            },
        ];
        candidate
    };
    if !phase2_reference_projections.is_empty() {
        candidate.authoritative_fields.insert(
            "topic_generation_summary_input_reference_projection".to_owned(),
            json!({
                "authority": "rust_phase2_topic_generation_summary_input_v1",
                "mappings": phase2_reference_projections,
            }),
        );
    }
    if !phase1_reference_projections.is_empty() {
        candidate.authoritative_fields.insert(
            "phase1_cross_phase_reference_projection".to_owned(),
            json!({
                "authority": "rust_phase1_cross_phase_reference_sanitizer_v1",
                // The raw terminal artifact remains audit-visible in the
                // session/debug store.  Do not repeat a model-invented ID in
                // a cross-phase Index field where it could be mistaken for a
                // valid citation by another role.
                "removed_unverified_reference_count": phase1_reference_projections.len(),
            }),
        );
    }
    enrich_compiled_fields(
        role,
        kind,
        topic_id,
        response_text,
        &mut candidate.authoritative_fields,
    )?;
    if kind == "topic_control" {
        normalize_phase2_topic_control_fields(&mut candidate.authoritative_fields)?;
    }
    if kind == "topic_generation" {
        validate_phase2_topic_ttls(&candidate.authoritative_fields)?;
    }
    if kind == "phase2_final" {
        project_phase2_final_fields(state, &mut candidate.authoritative_fields)?;
    }
    if kind == "topic_control" {
        if let Some(topic_id) = topic_id {
            if let Some(topic_state) = state.pointer(&format!("/topic_debate_states/{topic_id}")) {
                ensure_initial_collision_route(
                    &mut candidate.authoritative_fields,
                    topic_state,
                    topic_id,
                )?;
            }
        }
    }
    if phase_u8 == 6 {
        enrich_and_validate_phase6_compiled_fields(state, &mut candidate.authoritative_fields)?;
    }
    if phase_u8 == 1 {
        if let Some(previous) = state.get("_phase1_summary_retry_candidate").cloned() {
            preserve_phase1_summary_key_evidence(&mut candidate.authoritative_fields, &previous);
        }
        let retry_candidate = candidate.authoritative_fields.clone();
        if let Err(error) =
            attach_verified_phase1_web_sources(response_text, &mut candidate.authoritative_fields)
        {
            state["_phase1_summary_retry_candidate"] = Value::Object(retry_candidate);
            return Err(error);
        }
        if let Err(error) = validate_phase1_compiled_fields(&candidate.authoritative_fields) {
            state["_phase1_summary_retry_candidate"] =
                Value::Object(candidate.authoritative_fields.clone());
            return Err(error);
        }
    }
    if phase_u8 == 5 {
        validate_phase5_compiled_fields(
            state,
            role,
            &candidate.summary,
            &candidate.missing_fields,
            &mut candidate.authoritative_fields,
        )?;
    }
    if phase_u8 == 4 {
        validate_phase4_compiled_fields(state, &mut candidate.authoritative_fields)?;
    }
    if phase_u8 == 3 {
        validate_phase3_compiled_fields(state, &mut candidate.authoritative_fields)?;
    }
    if phase_u8 >= 2 {
        project_detail_hash_source_refs(state, &mut candidate.authoritative_fields)?;
    }
    validate_phase2_compiled_contract(
        kind,
        &candidate.authoritative_fields,
        &candidate.missing_fields,
    )?;
    validate_compiled_asset_scope(state, phase_u8, &candidate.authoritative_fields)?;
    if !persist_phase_index {
        return Ok(json!({
            "phase": phase,
            "role": role,
            "profile": phase2_profile_name(role, kind),
            "ticker": ticker,
            "topic_id": topic_id,
            "payload": candidate.authoritative_fields,
            "response_text": response_text,
            "authority": "transient_phase2_extraction"
        }));
    }
    validate_declared_detail_source_refs(&candidate.details)?;
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

fn validate_declared_detail_source_refs(details: &[PhaseIndexCandidateDetail]) -> Result<()> {
    for detail in details {
        let mut seen = BTreeSet::new();
        for reference in &detail.source_refs {
            if !is_complete_declared_source_ref(reference) {
                bail!(
                    "Detail source reference {reference:?} must be an exact idx- or phase-1 evidence ID"
                )
            }
            if !seen.insert(reference) {
                bail!("Detail source references must not contain duplicates")
            }
        }
    }
    Ok(())
}

/// A model may echo the immutable content hash or `detail_id` returned by
/// `read_index_details`.  Those hashes are not independently readable
/// cross-phase references.  Resolve only aliases that map to exactly one
/// finalized current-run Detail, then replace them with that Detail's parent
/// Index. Unknown or ambiguous hashes remain invalid and are rejected by the
/// normal stable-reference validator.
fn project_detail_hash_source_refs(
    state: &Value,
    fields: &mut serde_json::Map<String, Value>,
) -> Result<()> {
    let aliases = finalized_detail_hash_aliases(state)?;
    if aliases.is_empty() {
        return Ok(());
    }
    let mut projections = Vec::new();
    let mut projected = Value::Object(fields.clone());
    project_detail_hash_source_refs_inner(&mut projected, None, &aliases, &mut projections);
    if projections.is_empty() {
        return Ok(());
    }
    let Value::Object(mut projected_fields) = projected else {
        unreachable!("a JSON object remains an object after reference projection")
    };
    projected_fields.insert(
        "detail_reference_projection".to_owned(),
        json!({
            "authority": "rust_finalized_detail_parent_index_v1",
            "mappings": projections,
        }),
    );
    *fields = projected_fields;
    Ok(())
}

fn finalized_detail_hash_aliases(state: &Value) -> Result<BTreeMap<String, String>> {
    let Some(store_root) = state.get("store_root").and_then(Value::as_str) else {
        return Ok(BTreeMap::new());
    };
    let store = FileStore::open(store_root, FileStoreOptions::default())?;
    let location = run_location_from_state(state)?;
    let indexes = read_all_indexes(
        &store,
        Some(&location),
        &IndexQuery {
            kind: Some(IndexKind::PhaseSummary),
            ..IndexQuery::default()
        },
    )?;
    let mut candidates = BTreeMap::<String, BTreeSet<String>>::new();
    for index in indexes {
        let scope = IndexScope {
            kind: index.kind,
            location: Some(location.clone()),
            index_id: index.index_id.clone(),
            run_id: index.run_id,
            source_run_id: index.source_run_id,
            source_phase: index.source_phase,
            role: index.role,
            ticker: index.ticker,
            topic_id: index.topic_id,
            source_payload_hash: index.source_payload_hash,
            authoritative_fields: index.authoritative_fields,
            created_at: index.created_at,
        };
        let mut cursor = 0usize;
        loop {
            let page = read_index_details(
                &store,
                &scope,
                &DetailQuery {
                    limit: 100,
                    cursor,
                    ..DetailQuery::default()
                },
            )?;
            for detail in page.details {
                for alias in [detail.detail_id, detail.content_hash] {
                    if is_detail_hash_alias(&alias) {
                        candidates
                            .entry(alias)
                            .or_default()
                            .insert(scope.index_id.clone());
                    }
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = next_cursor
                .parse::<usize>()
                .context("finalized Detail pagination cursor must be numeric")?;
        }
    }
    Ok(candidates
        .into_iter()
        .filter_map(|(alias, indexes)| {
            (indexes.len() == 1).then(|| (alias, indexes.into_iter().next().unwrap_or_default()))
        })
        .collect())
}

fn project_detail_hash_source_refs_inner(
    value: &mut Value,
    parent_key: Option<&str>,
    aliases: &BTreeMap<String, String>,
    projections: &mut Vec<Value>,
) {
    match value {
        Value::Array(values)
            if matches!(
                parent_key,
                Some("source_refs" | "evidence_refs" | "source_index_ids")
            ) =>
        {
            for reference in values {
                let Some(original) = reference.as_str().map(ToOwned::to_owned) else {
                    continue;
                };
                let Some(resolved_index_id) = aliases.get(&original) else {
                    continue;
                };
                *reference = Value::String(resolved_index_id.clone());
                projections.push(json!({
                    "model_reference": original,
                    "resolved_index_id": resolved_index_id,
                }));
            }
        }
        Value::Array(values) => {
            for value in values {
                project_detail_hash_source_refs_inner(value, parent_key, aliases, projections);
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                project_detail_hash_source_refs_inner(value, Some(key), aliases, projections);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
fn project_detail_hash_source_refs_object(
    fields: &mut serde_json::Map<String, Value>,
    aliases: &BTreeMap<String, String>,
    projections: &mut Vec<Value>,
) {
    for (key, value) in fields {
        project_detail_hash_source_refs_inner(value, Some(key), aliases, projections);
    }
}

fn is_detail_hash_alias(reference: &str) -> bool {
    reference.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .chars()
                .all(|character| character.is_ascii_hexdigit())
    })
}

fn is_complete_declared_source_ref(reference: &str) -> bool {
    let Some((prefix, digest)) = ["idx-", "technical-", "jin10-", "web-"]
        .into_iter()
        .find_map(|prefix| {
            reference
                .strip_prefix(prefix)
                .map(|digest| (prefix, digest))
        })
    else {
        return false;
    };
    let _ = prefix;
    digest.len() == 64
        && digest
            .chars()
            .all(|character| character.is_ascii_hexdigit())
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
    if kind == "topic_generation" {
        project_topic_generation_selection(fields)?;
        project_topic_generation_residual_coverage(fields)?;
    }
    if kind == "phase2_final" {
        let controllers = serde_json::from_str::<Value>(response_text)
            .context("Phase 2 final reducer source must be the complete controllers object")?;
        let controllers = controllers
            .as_object()
            .cloned()
            .context("Phase 2 final reducer source must be an object keyed by topic_id")?;
        fields.insert("controllers".to_owned(), Value::Object(controllers));
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

/// The Topic Generator owns the candidate search space and names its chosen
/// decision hinges.  It does not own two independently worded copies of a
/// selected topic: a paraphrase changes a content-hash-derived topic ID and
/// would otherwise make `topics` look like a new candidate.  Rust therefore
/// owns topic IDs and projects every selected hinge back onto the exact,
/// evidence-bearing candidate record.
fn project_topic_generation_selection(fields: &mut serde_json::Map<String, Value>) -> Result<()> {
    let candidate_topics = fields
        .remove("candidate_topics")
        .and_then(|value| value.as_array().cloned())
        .context("Phase 2 topic_generation requires candidate_topics array")?;
    let selected_topics = fields
        .remove("topics")
        .and_then(|value| value.as_array().cloned())
        .context("Phase 2 topic_generation requires topics array")?;

    let mut candidates_by_hinge = BTreeMap::new();
    let mut canonical_candidates = Vec::with_capacity(candidate_topics.len());
    for candidate in candidate_topics {
        let mut candidate = candidate
            .as_object()
            .cloned()
            .context("Phase 2 topic_generation candidate_topics entries must be objects")?;
        // Topic identity is Rust-owned. Ignore a stale or model-supplied ID
        // before hashing the fully evidence-bearing candidate record.
        candidate.remove("topic_id");
        let hinge = candidate
            .get("decision_hinge")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .context("Phase 2 topic_generation candidate_topics requires decision_hinge")?
            .to_owned();
        if candidates_by_hinge.contains_key(&hinge) {
            bail!(
                "Phase 2 topic_generation candidate_topics must have unique decision_hinge values for Rust selection projection"
            )
        }
        let hash = orchestrator_store::content_hash(&Value::Object(candidate.clone()))?;
        candidate.insert(
            "topic_id".to_owned(),
            Value::String(format!(
                "topic-{}",
                hash.strip_prefix("sha256:").unwrap_or(&hash)
            )),
        );
        let canonical = Value::Object(candidate);
        candidates_by_hinge.insert(hinge, canonical.clone());
        canonical_candidates.push(canonical);
    }

    let mut canonical_selected = Vec::with_capacity(selected_topics.len());
    for selected in selected_topics {
        let selected = selected
            .as_object()
            .context("Phase 2 topic_generation topics entries must be objects")?;
        let hinge = selected
            .get("decision_hinge")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .context("Phase 2 topic_generation topics requires decision_hinge")?;
        let candidate = candidates_by_hinge.get(hinge).with_context(|| {
            format!(
                "Phase 2 topic_generation selected decision_hinge {hinge:?} does not name a candidate topic"
            )
        })?;
        canonical_selected.push(candidate.clone());
    }

    fields.insert(
        "candidate_topics".to_owned(),
        Value::Array(canonical_candidates),
    );
    fields.insert("topics".to_owned(), Value::Array(canonical_selected));
    Ok(())
}

/// `coverage` is the Topic Generator's complete five-category search-space
/// audit. A bounded `residual_risks` list is only its downstream view, so an
/// omission there must not erase a coverage row already provided by the model.
/// The projection copies exact reason and references and labels its authority;
/// it does not manufacture a risk or evidence item.
fn project_topic_generation_residual_coverage(
    fields: &mut serde_json::Map<String, Value>,
) -> Result<()> {
    const RESIDUAL_STATUSES: [&str; 3] = ["candidate_only", "residual_risk", "data_gap"];
    let coverage = fields
        .get("coverage")
        .and_then(Value::as_array)
        .cloned()
        .context("Phase 2 topic_generation requires coverage array")?;
    let mut residuals = fields
        .get("residual_risks")
        .and_then(Value::as_array)
        .cloned()
        .context("Phase 2 topic_generation requires residual_risks array")?;
    for entry in coverage {
        let entry = entry
            .as_object()
            .context("Phase 2 topic_generation coverage entries must be objects")?;
        let Some(category) = entry.get("category").and_then(Value::as_str) else {
            continue;
        };
        let Some(status) = entry.get("status").and_then(Value::as_str) else {
            continue;
        };
        if !RESIDUAL_STATUSES.contains(&status) {
            continue;
        }
        let represented = residuals.iter().any(|residual| {
            residual
                .get("category")
                .and_then(Value::as_str)
                .is_some_and(|value| value == category || value == status)
        });
        if represented {
            continue;
        }
        let reason = entry
            .get("reason")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .context("Phase 2 topic_generation residual coverage requires a non-empty reason")?;
        residuals.push(json!({
            "category": category,
            "reason": reason,
            "evidence_refs": entry.get("evidence_refs").cloned().unwrap_or_else(|| json!([])),
            "coverage_projection": {
                "authority": "rust_phase2_coverage_projection_v1",
                "coverage_category": category,
                "coverage_status": status,
            }
        }));
    }
    fields.insert("residual_risks".to_owned(), Value::Array(residuals));
    Ok(())
}

fn inject_current_weights_into_phase6_fields(
    state: &Value,
    fields: &mut serde_json::Map<String, Value>,
) -> Result<()> {
    let weights = state
        .get("current_portfolio_weights")
        .and_then(Value::as_object);
    let per_asset = fields
        .get_mut("per_asset")
        .and_then(Value::as_object_mut)
        .context("Phase 6 Summary requires per_asset before runtime weight injection")?;
    for ticker in investable_assets_from_state(state) {
        let current_weight = weights
            .and_then(|items| items.get(&ticker))
            .and_then(Value::as_f64)
            .or_else(|| state["mock"].as_bool().unwrap_or(false).then_some(0.0))
            .with_context(|| format!("runtime current weight missing for {ticker}"))?;
        let decision = per_asset
            .get_mut(&ticker)
            .and_then(Value::as_object_mut)
            .with_context(|| format!("Phase 6 Summary decision missing for {ticker}"))?;
        decision.insert("current_weight".to_owned(), json!(current_weight));
    }
    Ok(())
}

/// Summary models sometimes serialize a single decision hinge as a map keyed
/// by hinge name. Convert that equivalent representation to the canonical
/// array before collision repair and contract validation consume it.
fn normalize_phase2_topic_control_fields(
    fields: &mut serde_json::Map<String, Value>,
) -> Result<()> {
    let Some(hinge_map) = fields
        .get("decision_hinges")
        .and_then(Value::as_object)
        .cloned()
    else {
        return Ok(());
    };

    let mut normalized = Vec::with_capacity(hinge_map.len());
    for (hinge, value) in hinge_map {
        let mut item = value.as_object().cloned().with_context(|| {
            format!("Phase 2 topic_control Summary decision_hinges.{hinge} requires object value")
        })?;
        item.insert("hinge".to_owned(), Value::String(hinge));
        normalized.push(Value::Object(item));
    }
    fields.insert("decision_hinges".to_owned(), Value::Array(normalized));
    Ok(())
}

fn validate_phase2_topic_ttls(fields: &serde_json::Map<String, Value>) -> Result<()> {
    for field in ["candidate_topics", "topics"] {
        let topics = fields
            .get(field)
            .and_then(Value::as_array)
            .with_context(|| format!("Phase 2 topic_generation requires {field}"))?;
        for (index, topic) in topics.iter().enumerate() {
            let ttl = topic
                .get("ttl")
                .and_then(Value::as_str)
                .with_context(|| format!("Phase 2 {field}[{index}] requires ttl"))?;
            if !matches!(ttl, "intraday" | "1-3d") {
                bail!(
                    "Phase 2 {field}[{index}] ttl {ttl:?} exceeds the supported 1-5 trading-day decision horizon"
                )
            }
        }
    }
    Ok(())
}

fn project_phase2_final_fields(
    state: &Value,
    fields: &mut serde_json::Map<String, Value>,
) -> Result<()> {
    let empty_topic_states = serde_json::Map::new();
    let topic_states = match state.get("topic_debate_states").and_then(Value::as_object) {
        Some(topic_states) => topic_states,
        None if state
            .pointer("/topic_generation_artifact/topics")
            .and_then(Value::as_array)
            .is_none_or(Vec::is_empty) =>
        {
            &empty_topic_states
        }
        None => bail!("Phase 2 final projection requires topic_debate_states"),
    };
    let mut topic_ids = topic_states.keys().cloned().collect::<Vec<_>>();
    topic_ids.sort();
    let mut topics = Vec::with_capacity(topic_ids.len());
    let mut consensus = Vec::new();
    let mut unresolved = Vec::new();
    let mut closure_reasons = Vec::with_capacity(topic_ids.len());
    for topic_id in topic_ids {
        let topic_state = &topic_states[&topic_id];
        let stree = topic_state
            .get("stree")
            .and_then(Value::as_object)
            .with_context(|| format!("Phase 2 topic {topic_id} has no stree"))?;
        if stree.get("status").and_then(Value::as_str) != Some("closed") {
            bail!("Phase 2 topic {topic_id} is not closed")
        }
        let closure = stree
            .get("closure")
            .and_then(Value::as_object)
            .with_context(|| format!("Phase 2 topic {topic_id} has no closure"))?;
        let reason = closure
            .get("reason")
            .and_then(Value::as_str)
            .with_context(|| format!("Phase 2 topic {topic_id} closure has no reason"))?;
        let ledger = closure
            .get("claim_ledger")
            .cloned()
            .unwrap_or_else(|| json!([]));
        let round = closure
            .get("round")
            .cloned()
            .or_else(|| stree.get("round").cloned())
            .unwrap_or_else(|| json!(0));
        topics.push(json!({
            "topic_id": topic_id,
            "topic": topic_state.get("topic").cloned().unwrap_or(Value::Null),
            "round": round,
            "closure": closure,
            "claim_ledger": ledger,
        }));
        closure_reasons.push(json!({"topic_id": topic_id, "reason": reason, "round": round}));
        if reason == "consensus" {
            consensus.push(json!({
                "topic_id": topic_id,
                "claim_ids": closure.get("consensus_claim_ids").cloned().unwrap_or_else(|| json!([])),
            }));
        } else {
            unresolved.push(json!({
                "topic_id": topic_id,
                "reason": reason,
                "claim_ids": closure.get("unresolved_claim_ids").cloned().unwrap_or_else(|| json!([])),
            }));
        }
    }
    fields.insert("topics".to_owned(), Value::Array(topics));
    fields.insert("consensus".to_owned(), Value::Array(consensus));
    fields.insert(
        "unresolved_disagreements".to_owned(),
        Value::Array(unresolved),
    );
    fields.insert("closure_reasons".to_owned(), Value::Array(closure_reasons));
    fields.insert(
        "topic_search_space".to_owned(),
        json!({
            "candidate_topics": state
                .pointer("/topic_generation_artifact/candidate_topics")
                .cloned()
                .unwrap_or_else(|| json!([])),
            "unselected_candidates": state
                .pointer("/topic_generation_artifact/unselected_candidates")
                .cloned()
                .unwrap_or_else(|| json!([])),
            "residual_risks": state
                .pointer("/topic_generation_artifact/residual_risks")
                .cloned()
                .unwrap_or_else(|| json!([])),
            "coverage": state
                .pointer("/topic_generation_artifact/coverage")
                .cloned()
                .unwrap_or_else(|| json!([])),
            "selection": state
                .pointer("/topic_generation_artifact/selection")
                .cloned()
                .unwrap_or(Value::Null),
            "authority": "rust_phase2_topic_search_space_v1",
        }),
    );
    Ok(())
}

fn validate_phase2_compiled_contract(
    kind: &str,
    fields: &serde_json::Map<String, Value>,
    missing_fields: &[String],
) -> Result<()> {
    // `missing_fields` can contain paths to nested values such as
    // `claims[0].confidence`. Those do not mean that the top-level `claims`
    // array was omitted; treating them as such hides the actual contract
    // failure and makes a valid array look absent.
    let missing = |field: &str| missing_fields.iter().any(|missing| missing == field);
    let required_array = |field: &str, min: usize, max: usize| -> Result<&Vec<Value>> {
        if missing(field) {
            bail!("Phase 2 {kind} Summary omitted required {field}")
        }
        let values = fields
            .get(field)
            .and_then(Value::as_array)
            .with_context(|| format!("Phase 2 {kind} Summary requires array {field}"))?;
        if values.len() < min || values.len() > max {
            bail!(
                "Phase 2 {kind} Summary requires {field} length in {min}..={max}, got {}",
                values.len()
            )
        }
        Ok(values)
    };
    match kind {
        "topic_generation" => {
            const CATEGORIES: [&str; 5] = [
                "trend",
                "valuation_expectations",
                "macro",
                "event_risk",
                "data_quality",
            ];
            let coverage = required_array("coverage", CATEGORIES.len(), CATEGORIES.len())?;
            let candidate_topics = required_array("candidate_topics", 0, 5)?;
            let selected_topics = required_array("topics", 0, 5)?;
            let residual_risks = required_array("residual_risks", 0, 10)?;
            let mut coverage_categories = BTreeSet::new();
            let mut coverage_needing_residual = BTreeMap::new();
            for entry in coverage {
                let entry = entry
                    .as_object()
                    .context("Phase 2 topic_generation coverage entries must be objects")?;
                let category = entry
                    .get("category")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .context("Phase 2 topic_generation coverage requires category")?;
                if !CATEGORIES.contains(&category) || !coverage_categories.insert(category) {
                    bail!("Phase 2 topic_generation coverage must contain each required category exactly once")
                }
                let status = entry
                    .get("status")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .context("Phase 2 topic_generation coverage requires status")?;
                if !matches!(
                    status,
                    "selected" | "candidate_only" | "residual_risk" | "not_present" | "data_gap"
                ) {
                    bail!("Phase 2 topic_generation coverage has invalid status {status:?}")
                }
                if entry
                    .get("reason")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .is_none_or(str::is_empty)
                {
                    bail!("Phase 2 topic_generation coverage requires a non-empty reason")
                }
                let evidence_refs = entry
                    .get("evidence_refs")
                    .and_then(Value::as_array)
                    .context("Phase 2 topic_generation coverage requires evidence_refs array")?;
                if evidence_refs.len() > 3
                    || evidence_refs.iter().any(|reference| {
                        reference
                            .as_str()
                            .is_none_or(|reference| reference.trim().is_empty())
                    })
                {
                    bail!("Phase 2 topic_generation coverage evidence_refs must contain 0..=3 non-empty strings")
                }
                if matches!(status, "candidate_only" | "residual_risk" | "data_gap") {
                    coverage_needing_residual.insert(category.to_owned(), status.to_owned());
                }
            }
            if coverage_categories.len() != CATEGORIES.len() {
                bail!("Phase 2 topic_generation coverage is missing a required category")
            }
            let topic_ids = |topics: &Vec<Value>, field: &str| -> Result<BTreeSet<String>> {
                let mut ids = BTreeSet::new();
                for topic in topics {
                    let topic = topic.as_object().with_context(|| {
                        format!("Phase 2 topic_generation {field} entries must be objects")
                    })?;
                    let topic_id = topic
                        .get("topic_id")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .with_context(|| {
                            format!("Phase 2 topic_generation {field} requires Rust topic_id")
                        })?;
                    if !ids.insert(topic_id.to_owned()) {
                        bail!("Phase 2 topic_generation {field} contains duplicate topic_id {topic_id:?}")
                    }
                    if topic
                        .get("decision_hinge")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .is_none_or(str::is_empty)
                    {
                        bail!("Phase 2 topic_generation {field} requires a falsifiable decision_hinge")
                    }
                    let evidence_refs = topic
                        .get("evidence_refs")
                        .and_then(Value::as_array)
                        .with_context(|| {
                            format!("Phase 2 topic_generation {field} requires evidence_refs")
                        })?;
                    if evidence_refs.is_empty()
                        || evidence_refs.len() > 5
                        || evidence_refs.iter().any(|reference| {
                            reference
                                .as_str()
                                .is_none_or(|reference| reference.trim().is_empty())
                        })
                    {
                        bail!(
                            "Phase 2 topic_generation {field} requires 1..=5 non-empty evidence_refs"
                        )
                    }
                }
                Ok(ids)
            };
            let candidate_ids = topic_ids(candidate_topics, "candidate_topics")?;
            let selected_ids = topic_ids(selected_topics, "topics")?;
            if !selected_ids.is_subset(&candidate_ids) {
                bail!("Phase 2 topic_generation topics must be a subset of candidate_topics")
            }
            const RESIDUAL_KINDS: [&str; 3] = ["candidate_only", "residual_risk", "data_gap"];
            let mut residual_categories = BTreeSet::new();
            let mut residual_kinds = BTreeSet::new();
            for residual in residual_risks {
                let residual = residual
                    .as_object()
                    .context("Phase 2 topic_generation residual_risks entries must be objects")?;
                let category = residual
                    .get("category")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .context("Phase 2 topic_generation residual_risks requires category")?;
                if CATEGORIES.contains(&category) {
                    residual_categories.insert(category.to_owned());
                } else if RESIDUAL_KINDS.contains(&category) {
                    residual_kinds.insert(category.to_owned());
                } else {
                    bail!(
                        "Phase 2 topic_generation residual_risks category {category:?} must be a coverage category or candidate_only, residual_risk, data_gap"
                    )
                }
                if residual
                    .get("reason")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .is_none_or(str::is_empty)
                {
                    bail!("Phase 2 topic_generation residual_risks requires a non-empty reason")
                }
            }
            if coverage_needing_residual.iter().any(|(category, status)| {
                !residual_categories.contains(category) && !residual_kinds.contains(status)
            }) {
                bail!(
                    "Phase 2 topic_generation residual_risks must preserve every candidate_only, residual_risk, and data_gap coverage status"
                )
            }
        }
        "bull_seed" | "bear_seed" => {
            let claims = required_array("claims", 1, 2)?;
            for (index, claim) in claims.iter().enumerate() {
                let claim_path = format!("claims[{index}]");
                let claim = claim
                    .as_object()
                    .with_context(|| format!("Phase 2 {kind} requires object {claim_path}"))?;
                if claim
                    .get("claim")
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty)
                {
                    bail!("Phase 2 {kind} Summary requires non-empty claim text")
                }
                let evidence_refs = claim
                    .get("evidence_refs")
                    .and_then(Value::as_array)
                    .with_context(|| {
                        format!("Phase 2 {kind} Summary requires array {claim_path}.evidence_refs")
                    })?;
                if evidence_refs.len() > 3
                    || evidence_refs.iter().any(|reference| {
                        reference
                            .as_str()
                            .is_none_or(|reference| reference.trim().is_empty())
                    })
                {
                    bail!(
                        "Phase 2 {kind} Summary requires 0..=3 non-empty string values in {claim_path}.evidence_refs"
                    )
                }
                let confidence = claim
                    .get("confidence")
                    .and_then(Value::as_f64)
                    .with_context(|| {
                        format!("Phase 2 {kind} Summary omitted required {claim_path}.confidence")
                    })?;
                if !(0.0..=1.0).contains(&confidence) || !confidence.is_finite() {
                    bail!("Phase 2 {kind} Summary requires {claim_path}.confidence in 0..=1")
                }
                if claim
                    .get("needs_mediator_check")
                    .and_then(Value::as_bool)
                    .is_none()
                {
                    bail!(
                        "Phase 2 {kind} Summary omitted required {claim_path}.needs_mediator_check"
                    )
                }
            }
        }
        "interaction" => {
            let replies = required_array("replies", 1, 2)?;
            for reply in replies {
                for field in ["reply_to_claim_id", "stance", "reason"] {
                    if reply
                        .get(field)
                        .and_then(Value::as_str)
                        .is_none_or(str::is_empty)
                    {
                        bail!("Phase 2 interaction Summary requires non-empty {field}")
                    }
                }
            }
        }
        "topic_control" => {
            required_array("claim_ledger", 1, 3)?;
            required_array("decision_hinges", 1, 3)?;
            if missing("next_steers") || !fields.get("next_steers").is_some_and(Value::is_array) {
                bail!("Phase 2 topic_control Summary requires next_steers array")
            }
            let soft_control = fields
                .get("soft_control")
                .and_then(Value::as_object)
                .context("Phase 2 topic_control Summary requires soft_control object")?;
            if soft_control
                .get("should_continue")
                .and_then(Value::as_bool)
                .is_none()
                || soft_control
                    .get("stop_reason")
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty)
            {
                bail!(
                    "Phase 2 topic_control Summary requires boolean soft_control.should_continue and non-empty stop_reason"
                )
            }
        }
        _ => {}
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
    let packet_json = packet_json
        .split(orchestrator_llm::tools::web_run::VERIFIED_RESULTS_MARKER)
        .next()
        .unwrap_or(packet_json);
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

fn verified_phase1_tool_ids_from_response(response_text: &str) -> Result<Option<BTreeSet<String>>> {
    response_text
        .rsplit_once(orchestrator_llm::VERIFIED_PHASE1_EVIDENCE_MARKER)
        .map(|(_, registry_json)| {
            let registry_json = registry_json
                .split(orchestrator_llm::tools::web_run::VERIFIED_RESULTS_MARKER)
                .next()
                .unwrap_or(registry_json);
            serde_json::from_str::<Vec<String>>(registry_json.trim())
                .context("Rust-verified Phase 1 evidence ID attachment is malformed")
                .map(|ids| ids.into_iter().collect::<BTreeSet<_>>())
        })
        .transpose()
}

fn verified_phase1_web_records_from_response(response_text: &str) -> Result<Vec<Value>> {
    let marker = orchestrator_llm::tools::web_run::VERIFIED_RESULTS_MARKER;
    let registry = response_text
        .rsplit_once(marker)
        .map(|(_, registry_json)| {
            serde_json::from_str::<Vec<Value>>(registry_json.trim())
                .context("Rust-verified Web search result attachment is malformed")
        })
        .transpose()?
        .unwrap_or_default();
    canonical_phase1_verified_web_evidence_records(registry)
}

/// Produce the only Phase 1 free-text Detail that later phases may read.
/// Raw model output remains in the terminal/session artifact, while this
/// projection removes stable-looking IDs that the runtime cannot trace to an
/// actual native-tool result.  Without this boundary, a Topic Generator can
/// faithfully repeat an invalid ID it saw in an analyst's prose and cause a
/// late Phase 2 failure.
fn canonicalize_phase1_cross_phase_source(response_text: &str) -> Result<(String, Vec<Value>)> {
    const PREFIXES: [&str; 3] = ["technical-", "jin10-", "web-"];
    const MIN_REFERENCE_SUFFIX_LEN: usize = 6;

    let mut verified = verified_phase1_tool_ids_from_response(response_text)?.unwrap_or_default();
    verified.extend(
        verified_phase1_web_records_from_response(response_text)?
            .into_iter()
            .filter_map(|record| {
                record
                    .get("evidence_id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            }),
    );
    if verified.is_empty() {
        return Ok((response_text.to_owned(), Vec::new()));
    }

    let mut projected = String::with_capacity(response_text.len());
    let mut removals = Vec::new();
    let mut cursor = 0usize;
    while cursor < response_text.len() {
        let remainder = &response_text[cursor..];
        let next = PREFIXES
            .iter()
            .filter_map(|prefix| remainder.find(prefix).map(|offset| (offset, *prefix)))
            .min_by_key(|(offset, _)| *offset);
        let Some((offset, prefix)) = next else {
            projected.push_str(remainder);
            break;
        };
        let start = cursor + offset;
        projected.push_str(&response_text[cursor..start]);
        let suffix = &response_text[start..];
        let token_len = suffix
            .char_indices()
            .find_map(|(index, character)| {
                (!(character.is_ascii_alphanumeric() || character == '-' || character == '_'))
                    .then_some(index)
            })
            .unwrap_or(suffix.len());
        let token = &suffix[..token_len];
        if token.len() <= prefix.len() + MIN_REFERENCE_SUFFIX_LEN || verified.contains(token) {
            projected.push_str(token);
        } else {
            projected.push_str("unverified_phase1_reference_removed");
            removals.push(json!({"prefix": prefix}));
        }
        cursor = start + token_len;
    }
    Ok((projected, removals))
}

fn attach_verified_phase1_web_sources(
    response_text: &str,
    fields: &mut serde_json::Map<String, Value>,
) -> Result<()> {
    let verified_tool_ids = verified_phase1_tool_ids_from_response(response_text)?;
    let verified_time_metadata = response_text
        .rsplit_once(orchestrator_llm::VERIFIED_PHASE1_EVIDENCE_RECORDS_MARKER)
        .map(|(_, registry_json)| {
            let registry_json = registry_json
                .split(orchestrator_llm::VERIFIED_PHASE1_EVIDENCE_MARKER)
                .next()
                .unwrap_or(registry_json)
                .split(orchestrator_llm::tools::web_run::VERIFIED_RESULTS_MARKER)
                .next()
                .unwrap_or(registry_json);
            serde_json::from_str::<Vec<Value>>(registry_json.trim())
                .context("Rust-verified Phase 1 evidence metadata attachment is malformed")
                .map(|records| {
                    records
                        .into_iter()
                        .filter_map(|record| {
                            Some((record.get("evidence_id")?.as_str()?.to_owned(), record))
                        })
                        .collect::<BTreeMap<_, _>>()
                })
        })
        .transpose()?
        .unwrap_or_default();
    let verified_web_records = verified_phase1_web_records_from_response(response_text)?;
    let verified_input_records = canonical_phase1_verified_input_evidence_records(
        verified_tool_ids.as_ref(),
        &verified_time_metadata,
    );
    let urls = verified_web_records
        .iter()
        .filter_map(|item| {
            Some((
                item.get("evidence_id")?.as_str()?.to_owned(),
                item.get("source_url")?.as_str()?.to_owned(),
            ))
        })
        .collect::<BTreeMap<_, _>>();
    let verified_web_ids = urls.keys().cloned().collect::<BTreeSet<_>>();
    let mut normalization = Phase1ReferenceNormalization::default();
    for (key, value) in fields.iter_mut() {
        normalize_phase1_reference_arrays(
            value,
            Some(key),
            verified_tool_ids.as_ref(),
            &verified_web_ids,
            &mut normalization,
        );
    }
    prune_unbacked_phase1_findings(fields, &mut normalization)?;
    for value in fields.values_mut() {
        attach_verified_web_urls_to_referencing_objects(value, &urls);
    }
    let time_projections = attach_verified_phase1_time_metadata(fields, &verified_time_metadata);
    if !time_projections.is_empty() {
        fields.insert(
            "evidence_time_projection".to_owned(),
            json!({
                "authority": "rust_verified_phase1_tool_metadata_v1",
                "projections": time_projections,
            }),
        );
    }
    if !verified_web_records.is_empty() {
        // This is intentionally broader than `key_evidence`: the raw Phase 1
        // Detail contains every result of a Rust-observed Web call, and a
        // later Phase 2 Topic Generator may legitimately identify a material
        // issue in one of those results.  Keeping it in an explicit
        // Rust-owned field supplies a stable cross-phase allow-list without
        // treating an unselected search result as an analyst endorsement.
        fields.insert(
            "phase1_verified_web_evidence".to_owned(),
            json!({
                "authority": "rust_verified_phase1_web_run_v1",
                "records": verified_web_records,
            }),
        );
    }
    if !verified_input_records.is_empty() {
        // This mirrors the Web registry above.  A Phase 2 role may refer to a
        // directly observed, unselected technical/Jin10 item, but the record
        // remains explicitly "visible input", not an analyst endorsement.
        fields.insert(
            "phase1_verified_input_evidence".to_owned(),
            json!({
                "authority": "rust_verified_phase1_input_tool_v1",
                "records": verified_input_records,
            }),
        );
    }
    fields.insert(
        "evidence_normalization".to_owned(),
        json!({
            "authority": "rust",
            "unverified_web_refs_removed": normalization.unverified_web_refs_removed,
            "unverified_technical_refs_removed": normalization.unverified_technical_refs_removed,
            "unverified_jin10_refs_removed": normalization.unverified_jin10_refs_removed,
            "unverified_malformed_refs_removed": normalization.unverified_malformed_refs_removed,
            "canonicalized_web_refs": normalization.canonicalized_web_refs,
            "canonicalized_technical_refs": normalization.canonicalized_technical_refs,
            "canonicalized_jin10_refs": normalization.canonicalized_jin10_refs,
            "unbacked_key_evidence_removed": normalization.unbacked_key_evidence_removed,
            "unbacked_cross_asset_findings_removed": normalization.unbacked_cross_asset_findings_removed,
        }),
    );
    Ok(())
}

/// Canonical, runtime-owned Phase 1 Web evidence registry.  The only source
/// is the marker appended by `orchestrator-llm` from actual Web tool results;
/// the model's prose is never parsed as evidence here.
fn canonical_phase1_verified_web_evidence_records(registry: Vec<Value>) -> Result<Vec<Value>> {
    let mut records = BTreeMap::new();
    for item in registry {
        let evidence_id = item
            .get("evidence_id")
            .and_then(Value::as_str)
            .filter(|id| id.starts_with("web-"))
            .context("Rust-verified Phase 1 Web registry record requires a web-* evidence_id")?;
        let source_url = item
            .get("source_url")
            .and_then(Value::as_str)
            .filter(|url| url.starts_with("https://") || url.starts_with("http://"))
            .context("Rust-verified Phase 1 Web registry record requires an HTTP(S) source_url")?;
        let record = json!({
            "evidence_id": evidence_id,
            "source_url": source_url,
            "published_at": item.get("published_at").cloned().unwrap_or(Value::Null),
            "title": item.get("title").cloned().unwrap_or(Value::Null),
        });
        if let Some(existing) = records.insert(evidence_id.to_owned(), record.clone()) {
            if existing != record {
                bail!(
                    "Rust-verified Phase 1 Web registry contains conflicting metadata for {evidence_id}"
                );
            }
        }
    }
    Ok(records.into_values().collect())
}

fn phase1_verified_web_evidence_records(fields: &Value) -> Vec<&Value> {
    fields
        .pointer("/phase1_verified_web_evidence/records")
        .and_then(Value::as_array)
        .map(|records| records.iter().collect())
        .unwrap_or_default()
}

/// Canonical, runtime-owned registry for every Phase 1 technical/Jin10 item
/// read by the active agent.  The ID marker is emitted from actual native-tool
/// results; time metadata is attached only when the source supplied it.  This
/// keeps "visible but unselected" distinct from both model prose and an
/// analyst's endorsed `key_evidence`.
fn canonical_phase1_verified_input_evidence_records(
    verified_tool_ids: Option<&BTreeSet<String>>,
    verified_time_metadata: &BTreeMap<String, Value>,
) -> Vec<Value> {
    let mut records = Vec::new();
    for evidence_id in verified_tool_ids
        .into_iter()
        .flatten()
        .filter(|id| id.starts_with("technical-") || id.starts_with("jin10-"))
    {
        let source = if evidence_id.starts_with("technical-") {
            "filestore.run_input.technical"
        } else {
            "filestore.run_input.jin10"
        };
        let metadata = verified_time_metadata.get(evidence_id);
        let source_metadata = |key: &str| {
            metadata
                .and_then(|record| record.get(key))
                .filter(|value| value.is_string())
                .cloned()
                .unwrap_or(Value::Null)
        };
        records.push(json!({
            "evidence_id": evidence_id,
            "source": source,
            "event_time": source_metadata("event_time"),
            "published_time": source_metadata("published_time"),
            "ingested_time": source_metadata("ingested_time"),
            "as_of": source_metadata("as_of"),
            "timezone": source_metadata("timezone"),
            "time_metadata_available": metadata.is_some(),
        }));
    }
    records
}

fn phase1_verified_input_evidence_records(fields: &Value) -> Vec<&Value> {
    fields
        .pointer("/phase1_verified_input_evidence/records")
        .and_then(Value::as_array)
        .map(|records| records.iter().collect())
        .unwrap_or_default()
}

/// Phase 1's mandatory `timestamp` is source metadata, not a Summary-model
/// inference.  Restore it only when every cited, Rust-verified record exposes
/// the same clock.  A multi-source claim with incompatible clocks remains
/// unfilled and therefore fails the normal artifact contract instead of
/// silently adopting an arbitrary one.
fn attach_verified_phase1_time_metadata(
    fields: &mut serde_json::Map<String, Value>,
    metadata: &BTreeMap<String, Value>,
) -> Vec<Value> {
    let mut projections = Vec::new();
    let Some(reports) = fields.get_mut("per_ticker").and_then(Value::as_object_mut) else {
        return projections;
    };
    for (ticker, report) in reports {
        let Some(evidence_items) = report.get_mut("key_evidence").and_then(Value::as_array_mut)
        else {
            continue;
        };
        for evidence in evidence_items {
            let Some(item) = evidence.as_object_mut() else {
                continue;
            };
            let references = item
                .get("evidence_refs")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>();
            let records = references
                .iter()
                .filter_map(|reference| metadata.get(reference))
                .collect::<Vec<_>>();
            if records.is_empty() || records.len() != references.len() {
                continue;
            }

            for optional_field in [
                "event_time",
                "published_time",
                "ingested_time",
                "as_of",
                "timezone",
            ] {
                if item.get(optional_field).is_some_and(Value::is_null) {
                    item.remove(optional_field);
                }
            }
            if item.get("timestamp").is_some_and(Value::is_null) {
                item.remove("timestamp");
            }

            let mut projected_fields = Vec::new();
            let mut timestamp_clock = None;
            if item.get("timestamp").is_none() {
                for field in ["event_time", "published_time", "ingested_time", "as_of"] {
                    if let Some(value) = exact_verified_metadata_value(&records, field) {
                        item.insert("timestamp".to_owned(), Value::String(value));
                        projected_fields.push("timestamp".to_owned());
                        timestamp_clock = Some(field.to_owned());
                        break;
                    }
                }
            }
            for field in [
                "event_time",
                "published_time",
                "ingested_time",
                "as_of",
                "timezone",
            ] {
                if item.get(field).is_none() {
                    if let Some(value) = exact_verified_metadata_value(&records, field) {
                        item.insert(field.to_owned(), Value::String(value));
                        projected_fields.push(field.to_owned());
                    }
                }
            }
            if !projected_fields.is_empty() {
                projections.push(json!({
                    "ticker": ticker,
                    "evidence_refs": references,
                    "projected_fields": projected_fields,
                    "timestamp_clock": timestamp_clock,
                }));
            }
        }
    }
    projections
}

fn exact_verified_metadata_value(records: &[&Value], field: &str) -> Option<String> {
    let values = records
        .iter()
        .map(|record| record.get(field).and_then(Value::as_str).map(str::trim))
        .collect::<Option<Vec<_>>>()?;
    let mut unique = values.into_iter().filter(|value| !value.is_empty());
    let value = unique.next()?;
    unique
        .all(|candidate| candidate == value)
        .then(|| value.to_owned())
}

#[derive(Default)]
struct Phase1ReferenceNormalization {
    unverified_web_refs_removed: usize,
    unverified_technical_refs_removed: usize,
    unverified_jin10_refs_removed: usize,
    unverified_malformed_refs_removed: usize,
    canonicalized_web_refs: usize,
    canonicalized_technical_refs: usize,
    canonicalized_jin10_refs: usize,
    unbacked_key_evidence_removed: usize,
    unbacked_cross_asset_findings_removed: usize,
}

fn prune_unbacked_phase1_findings(
    fields: &mut serde_json::Map<String, Value>,
    normalization: &mut Phase1ReferenceNormalization,
) -> Result<()> {
    if let Some(reports) = fields.get_mut("per_ticker").and_then(Value::as_object_mut) {
        for (ticker, report) in reports {
            let Some(key_evidence) = report.get_mut("key_evidence").and_then(Value::as_array_mut)
            else {
                continue;
            };
            let before = key_evidence.len();
            key_evidence.retain(|evidence| {
                evidence
                    .get("evidence_refs")
                    .and_then(Value::as_array)
                    .is_none_or(|references| !references.is_empty())
            });
            normalization.unbacked_key_evidence_removed += before - key_evidence.len();
            if key_evidence.is_empty() {
                let explicitly_unobserved = report.get("direction").and_then(Value::as_str)
                    == Some("unobserved")
                    && report
                        .get("long_probability")
                        .and_then(Value::as_f64)
                        .is_some_and(|probability| (probability - 0.5).abs() <= 0.000001)
                    && report
                        .get("data_gaps")
                        .and_then(Value::as_array)
                        .is_some_and(|gaps| !gaps.is_empty());
                if !explicitly_unobserved {
                    bail!("Phase 1 has no verified key evidence remaining for {ticker}");
                }
            }
        }
    }
    if let Some(findings) = fields
        .get_mut("cross_asset_findings")
        .and_then(Value::as_array_mut)
    {
        let before = findings.len();
        findings.retain(|finding| {
            finding
                .get("evidence_refs")
                .and_then(Value::as_array)
                .is_none_or(|references| !references.is_empty())
        });
        normalization.unbacked_cross_asset_findings_removed += before - findings.len();
    }
    Ok(())
}

fn normalize_phase1_reference_arrays(
    value: &mut Value,
    parent_key: Option<&str>,
    verified_tool_ids: Option<&BTreeSet<String>>,
    verified_web_ids: &BTreeSet<String>,
    normalization: &mut Phase1ReferenceNormalization,
) {
    match value {
        Value::Array(values) if matches!(parent_key, Some("evidence_refs" | "source_refs")) => {
            let original = std::mem::take(values);
            let mut normalized = Vec::with_capacity(original.len());
            let mut seen = BTreeSet::new();
            for reference in original {
                let Some(reference_text) = reference.as_str() else {
                    normalized.push(reference);
                    continue;
                };
                let Some(canonical) = canonical_phase1_evidence_reference(
                    reference_text,
                    verified_tool_ids,
                    verified_web_ids,
                ) else {
                    normalization.record_removed(reference_text);
                    continue;
                };
                normalization.record_canonicalization(reference_text, &canonical);
                if seen.insert(canonical.clone()) {
                    normalized.push(Value::String(canonical));
                }
            }
            *values = normalized;
        }
        Value::Array(values) => {
            for value in values {
                normalize_phase1_reference_arrays(
                    value,
                    parent_key,
                    verified_tool_ids,
                    verified_web_ids,
                    normalization,
                );
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                normalize_phase1_reference_arrays(
                    value,
                    Some(key),
                    verified_tool_ids,
                    verified_web_ids,
                    normalization,
                );
            }
        }
        _ => {}
    }
}

/// Source URLs are runtime-owned provenance.  Preserve them beside every
/// structured finding that carries a verified `web-*` reference, not only
/// inside `per_ticker.key_evidence`; cross-asset findings otherwise looked
/// source-backed in one field but lost the authoritative URL at validation.
fn attach_verified_web_urls_to_referencing_objects(
    value: &mut Value,
    urls: &BTreeMap<String, String>,
) {
    match value {
        Value::Array(values) => {
            for value in values {
                attach_verified_web_urls_to_referencing_objects(value, urls);
            }
        }
        Value::Object(values) => {
            let first_web_ref = values
                .get("evidence_refs")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .find(|reference| reference.starts_with("web-"));
            if let Some(source_url) = first_web_ref.and_then(|reference| urls.get(reference)) {
                values.insert("source".to_owned(), Value::String(source_url.clone()));
            }
            for value in values.values_mut() {
                attach_verified_web_urls_to_referencing_objects(value, urls);
            }
        }
        _ => {}
    }
}

fn canonical_phase1_evidence_reference(
    reference: &str,
    verified_tool_ids: Option<&BTreeSet<String>>,
    verified_web_ids: &BTreeSet<String>,
) -> Option<String> {
    if reference.starts_with("web-") {
        return canonicalize_verified_phase1_reference(reference, verified_web_ids);
    }
    if reference.starts_with("technical-") || reference.starts_with("jin10-") {
        return match verified_tool_ids {
            Some(ids) => canonicalize_verified_phase1_reference(reference, ids),
            // Without a Rust registry this normalizer cannot establish
            // authority. Preserve the legacy value so the canonical field
            // validator can report a precise format failure instead.
            None => Some(reference.to_owned()),
        };
    }
    // Bare provider IDs, event IDs, and hashes have no source authority even
    // when their characters resemble a digest. Do not preserve them merely
    // to fail later after they have kept a finding artificially backed.
    None
}

fn canonicalize_verified_phase1_reference(
    reference: &str,
    verified_ids: &BTreeSet<String>,
) -> Option<String> {
    verified_ids.get(reference).cloned()
}

impl Phase1ReferenceNormalization {
    fn record_removed(&mut self, reference: &str) {
        if reference.starts_with("web-") {
            self.unverified_web_refs_removed += 1;
        } else if reference.starts_with("technical-") {
            self.unverified_technical_refs_removed += 1;
        } else if reference.starts_with("jin10-") {
            self.unverified_jin10_refs_removed += 1;
        } else {
            self.unverified_malformed_refs_removed += 1;
        }
    }

    fn record_canonicalization(&mut self, reference: &str, canonical: &str) {
        if reference == canonical {
            return;
        }
        if reference.starts_with("web-") {
            self.canonicalized_web_refs += 1;
        } else if reference.starts_with("technical-") {
            self.canonicalized_technical_refs += 1;
        } else if reference.starts_with("jin10-") {
            self.canonicalized_jin10_refs += 1;
        }
    }
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
    // Mock is intentionally non-live, but it must still satisfy the current
    // Phase 1 evidence contract. A 1970 timestamp and zero source confidence
    // make every synthetic contribution stale/zero-weight, which turns the
    // mock lifecycle tests into an accidental test of an impossible base.
    let mock_timestamp = state
        .get("current_date")
        .and_then(Value::as_str)
        .map(|date| format!("{date}T00:00:00Z"))
        .unwrap_or_else(|| "2000-01-01T00:00:00Z".to_owned());
    let authoritative_fields = match phase {
        0 => json!({"disposition":"no_reusable_memory","source_index_ids":[]}),
        1 => json!({"per_ticker": analysis.into_iter().map(|ticker| (
            ticker,
            json!({
                "direction":"neutral","confidence":0.5,"long_probability":0.5,"priced_in":"unclear",
                "report":response_text,"key_evidence":[{
                    "claim":"mock evidence is explicitly non-live","evidence_type":"inference",
                    "source":"mock fixture","timestamp":mock_timestamp,
                    "source_tier":"unknown","first_source":"mock fixture",
                    "is_derivative_repost":false,"evidence_age":"0-2d","source_confidence":0.5,
                    "evidence_refs":["technical-0000000000000000000000000000000000000000000000000000000000000000"]
                }],"validation_triggers":[],
                "data_gaps":[],"echo_chamber_risk":"low","crowded_consensus_risk":"low",
                "jin10_attention":[]
            })
        )).collect::<serde_json::Map<_, _>>()}),
        2 if kind == "topic_generation" => {
            json!({
                "common_ground": {},
                "coverage": [
                    {"category":"trend","status":"not_present","reason":"mock has no live trend evidence","evidence_refs":[]},
                    {"category":"valuation_expectations","status":"not_present","reason":"mock has no valuation evidence","evidence_refs":[]},
                    {"category":"macro","status":"not_present","reason":"mock has no macro evidence","evidence_refs":[]},
                    {"category":"event_risk","status":"not_present","reason":"mock has no event-risk evidence","evidence_refs":[]},
                    {"category":"data_quality","status":"not_present","reason":"mock has no data-quality exception","evidence_refs":[]}
                ],
                "candidate_topics": [],
                "topics": [],
                "residual_risks": [],
                "summary": "No mock debate topic."
            })
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
                    "scenarios":{
                        "bull":{"probability":0.25,"conditional_long_probability":0.7,"drivers":["mock upside driver"],"triggers":["mock upside trigger"],"confirmation":"mock upside confirmation"},
                        "base":{"probability":0.50,"conditional_long_probability":0.5,"drivers":["mock base driver"],"triggers":["mock base trigger"],"confirmation":"mock base confirmation"},
                        "bear":{"probability":0.25,"conditional_long_probability":0.3,"drivers":["mock downside driver"],"triggers":["mock downside trigger"],"confirmation":"mock downside confirmation"}
                    },"decision_hinges":[],"validation_plan":[]
                })
            )).collect::<serde_json::Map<_, _>>(),
            "regime_context": {"signal": "VIX", "status": "mock"}
        }),
        4 => json!({"plans": investable.iter().map(|ticker| (
            ticker.clone(),
            json!({
                "action":"Hold","candidate_action":"Hold","execution_decision":"hold",
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
                    "position_cap_pct":0.0,"max_drawdown_pct":0.0,"stop_type":"none",
                    "risk_off_trigger":"no mock trigger","rebalance_trigger":"no mock trigger","review_window":"mock review",
                    "constraint_confidence":0.0
                })
            )).collect::<serde_json::Map<_, _>>(),
            "cash_hedge_recommendation":"no mock hedge"
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
    } else if role == "researcher.bull" {
        "researcher.bull.interaction"
    } else if role == "researcher.bear" {
        "researcher.bear.interaction"
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

fn persist_state(state: &mut Value) -> Result<()> {
    let store_root = state
        .get("store_root")
        .and_then(Value::as_str)
        .context("store_root is required for FileStore state persistence")?
        .to_owned();
    let location = run_location_from_state(state)?;
    let mut sealed = state.clone();
    if let Some(config) = sealed.get("config").cloned() {
        sealed["config"] = redacted_config_for_state(&config);
    }
    // `serde_json::Value` can retain an arbitrary-precision number's original
    // spelling while the canonical FileStore write emits its normalized
    // spelling (for example, `0.37939999999999996` becomes `0.3794`). Hash
    // the same representation that will be persisted so checkpoints remain
    // reloadable even when a phase computes floating-point metadata.
    let canonical = canonical_json_bytes(&sealed)?;
    sealed = serde_json::from_slice(&canonical)
        .context("canonicalized run state could not be decoded")?;
    seal_state(&mut sealed)?;
    let store = FileStore::open(store_root, FileStoreOptions::default())?;
    let relative = location.child_relative(Path::new("state.json"))?;
    store.write_json_value(&relative, &sealed)?;

    let persisted = store.read_json_value(&relative)?;
    let stored_hash = persisted
        .get("content_hash")
        .and_then(Value::as_str)
        .context("persisted run state content_hash is required")?;
    let mut without_hash = persisted.clone();
    without_hash["content_hash"] = Value::String(String::new());
    let expected_hash = content_hash(&without_hash)?;
    if stored_hash != expected_hash {
        bail!(
            "persisted run state content_hash mismatch at {}: expected {expected_hash}, found {stored_hash}",
            store.root().join(relative).display()
        );
    }
    *state = sealed;
    Ok(())
}

fn checkpoint_state(state: &mut Value) -> Result<()> {
    persist_state(state)
}

fn redacted_config_for_state(config: &Value) -> Value {
    match config {
        Value::Object(object) => Value::Object(
            object
                .iter()
                .map(|(key, value)| {
                    let lower = key.to_ascii_lowercase();
                    let sensitive = matches!(
                        lower.as_str(),
                        "api_key"
                            | "api_secret"
                            | "client_secret"
                            | "password"
                            | "secret"
                            | "token"
                    ) || lower.ends_with("_key")
                        || lower.ends_with("_secret")
                        || lower.ends_with("_password")
                        || lower.ends_with("_token");
                    (
                        key.clone(),
                        if sensitive {
                            Value::String("[redacted]".to_owned())
                        } else {
                            redacted_config_for_state(value)
                        },
                    )
                })
                .collect(),
        ),
        Value::Array(values) => {
            Value::Array(values.iter().map(redacted_config_for_state).collect())
        }
        _ => config.clone(),
    }
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
        ("researcher.bull" | "researcher.bear", _) => "debate_response",
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

fn weighted_probability_base(state: &Value) -> Result<Value> {
    let reports = state
        .get("analyst_reports")
        .and_then(Value::as_object)
        .context("weighted probability base requires analyst_reports")?;
    let event_ledger = phase1_event_ledger_entries(state)?;
    let event_occurrences = event_ledger
        .iter()
        .map(|(event_id, entry)| (event_id.clone(), entry.occurrences.len()))
        .collect::<BTreeMap<_, _>>();
    let as_of_date = state
        .get("current_date")
        .and_then(Value::as_str)
        .and_then(parse_evidence_date);
    let mut values = serde_json::Map::new();
    for ticker in investable_assets_from_state(state) {
        let mut contributions = Vec::new();
        let mut excluded_contributions = Vec::new();
        for (role, report) in reports {
            let per_ticker = report
                .get("per_ticker")
                .and_then(Value::as_object)
                .with_context(|| format!("{role} report requires per_ticker"))?;
            let ticker_report = per_ticker
                .get(&ticker)
                .with_context(|| format!("{role} report missing investable ticker {ticker}"))?;
            let direction = ticker_report
                .get("direction")
                .and_then(Value::as_str)
                .with_context(|| format!("{role} direction missing for {ticker}"))?;
            let confidence = ticker_report
                .get("confidence")
                .and_then(Value::as_f64)
                .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
                .with_context(|| format!("{role} confidence invalid for {ticker}"))?;
            let long_probability = ticker_report
                .get("long_probability")
                .and_then(Value::as_f64)
                .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
                .with_context(|| format!("{role} long_probability invalid for {ticker}"))?;
            if !matches!(
                direction,
                "bullish" | "bearish" | "neutral" | "mixed" | "unobserved"
            ) {
                bail!("{role} direction {direction:?} is invalid for {ticker}")
            }
            if direction == "unobserved" {
                excluded_contributions.push(json!({
                    "role": role,
                    "direction": direction,
                    "confidence": confidence,
                    "long_probability": round_probability(long_probability),
                    "reason": "unobserved_does_not_contribute_to_probability_base",
                }));
                continue;
            }
            let evidence_assessment = phase1_evidence_assessment(
                ticker_report,
                role,
                &ticker,
                as_of_date,
                &event_occurrences,
            )?;
            let evidence_quality = evidence_assessment
                .get("evidence_quality")
                .and_then(Value::as_f64)
                .context("Phase 1 evidence assessment is missing evidence_quality")?;
            let calibration = analyst_calibration_multiplier(state, role, &ticker);
            let calibration_multiplier = calibration
                .get("multiplier")
                .and_then(Value::as_f64)
                .context("Phase 1 calibration assessment is missing multiplier")?;
            let effective_weight = confidence * evidence_quality * calibration_multiplier;
            if effective_weight <= f64::EPSILON {
                excluded_contributions.push(json!({
                    "role": role,
                    "direction": direction,
                    "confidence": confidence,
                    "long_probability": round_probability(long_probability),
                    "reason": "no_currently_available_independent_evidence",
                    "evidence_assessment": evidence_assessment,
                    "calibration": calibration,
                }));
                continue;
            }
            contributions.push(json!({
                "role": role,
                "direction": direction,
                "analyst_confidence": confidence,
                "evidence_quality": evidence_quality,
                "analyst_long_probability": round_probability(long_probability),
                "quality_weight": round_probability(effective_weight),
                "evidence_assessment": evidence_assessment,
                "calibration": calibration,
            }));
        }
        if contributions.is_empty() {
            bail!("weighted probability base has no Phase 1 contributions for {ticker}")
        }
        let total_quality_weight = contributions
            .iter()
            .filter_map(|item| item.get("quality_weight").and_then(Value::as_f64))
            .sum::<f64>();
        if total_quality_weight <= f64::EPSILON {
            bail!("weighted probability base has no positive evidence quality weight for {ticker}")
        }
        let uncalibrated_long_probability = contributions
            .iter()
            .map(|item| {
                item.get("analyst_long_probability")
                    .and_then(Value::as_f64)
                    .unwrap_or_default()
                    * item
                        .get("quality_weight")
                        .and_then(Value::as_f64)
                        .unwrap_or_default()
            })
            .sum::<f64>()
            / total_quality_weight;
        let calibration_reliability = contributions
            .iter()
            .map(|item| {
                item.get("quality_weight")
                    .and_then(Value::as_f64)
                    .unwrap_or_default()
                    * item
                        .pointer("/calibration/reliability")
                        .and_then(Value::as_f64)
                        .unwrap_or(0.0)
            })
            .sum::<f64>()
            / total_quality_weight;
        // A probability with no out-of-sample calibration is not allowed to
        // look as sharp as the raw weighted mean.  Shrink it toward the
        // no-edge prior while retaining every contribution and its audit
        // record.  Once Phase 0 has enough scored cases, its calibrated
        // reliability replaces this bootstrap value.
        let long_probability = round_probability(
            0.5 + calibration_reliability * (uncalibrated_long_probability - 0.5),
        );
        values.insert(
            ticker,
            json!({
                "long_probability": long_probability,
                "short_probability": round_probability(1.0 - long_probability),
                "uncalibrated_long_probability": round_probability(uncalibrated_long_probability),
                "source": "phase1_explicit_long_probability_v3",
                "weighting": "evidence_lineage_freshness_independence_weighted_mean",
                "calibration_projection": {
                    "authority": "rust",
                    "reliability": round_probability(calibration_reliability),
                    "identity": "long = 0.5 + reliability * (uncalibrated_long - 0.5)",
                },
                "contributions": contributions,
                "excluded_contributions": excluded_contributions,
            }),
        );
    }
    Ok(Value::Object(values))
}

#[derive(Debug, Default)]
struct Phase1EventLedgerEntry {
    occurrences: BTreeSet<String>,
    roles: BTreeSet<String>,
    tickers: BTreeSet<String>,
    evidence_refs: BTreeSet<String>,
    sources: BTreeSet<String>,
    timestamps: BTreeSet<String>,
    origin_keys: BTreeSet<String>,
    event_clocks: BTreeSet<String>,
}

fn phase1_event_ledger_entries(state: &Value) -> Result<BTreeMap<String, Phase1EventLedgerEntry>> {
    let reports = state
        .get("analyst_reports")
        .and_then(Value::as_object)
        .context("Phase 1 evidence ledger requires analyst_reports")?;
    let mut entries = BTreeMap::<String, Phase1EventLedgerEntry>::new();
    for (role, report) in reports {
        let per_ticker = report
            .get("per_ticker")
            .and_then(Value::as_object)
            .with_context(|| format!("{role} report requires per_ticker"))?;
        for (ticker, ticker_report) in per_ticker {
            for evidence in ticker_report
                .get("key_evidence")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let event_id = phase1_event_cluster_id(role, ticker, evidence);
                let entry = entries.entry(event_id).or_default();
                entry.occurrences.insert(format!("{role}:{ticker}"));
                entry.roles.insert(role.clone());
                entry.tickers.insert(ticker.clone());
                entry.evidence_refs.extend(
                    evidence
                        .get("evidence_refs")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_str)
                        .map(ToOwned::to_owned),
                );
                if let Some(source) = evidence.get("first_source").and_then(Value::as_str) {
                    entry.sources.insert(source.to_owned());
                }
                if let Some(timestamp) = evidence.get("timestamp").and_then(Value::as_str) {
                    entry.timestamps.insert(timestamp.to_owned());
                }
                entry
                    .origin_keys
                    .insert(phase1_evidence_origin_key(evidence));
                entry.event_clocks.insert(phase1_evidence_clock(evidence));
            }
        }
    }
    Ok(entries)
}

fn phase1_evidence_event_ledger(state: &Value) -> Result<Value> {
    let entries = phase1_event_ledger_entries(state)?;
    let duplicate_event_count = entries
        .values()
        .filter(|entry| entry.occurrences.len() > 1)
        .count();
    Ok(json!({
        "authority": "rust_phase1_event_lineage_v1",
        "event_count": entries.len(),
        "duplicate_event_count": duplicate_event_count,
        "events": entries.into_iter().map(|(event_id, entry)| (event_id, json!({
            "occurrences": entry.occurrences.into_iter().collect::<Vec<_>>(),
            "roles": entry.roles.into_iter().collect::<Vec<_>>(),
            "tickers": entry.tickers.into_iter().collect::<Vec<_>>(),
            "evidence_refs": entry.evidence_refs.into_iter().collect::<Vec<_>>(),
            "first_sources": entry.sources.into_iter().collect::<Vec<_>>(),
            "timestamps": entry.timestamps.into_iter().collect::<Vec<_>>(),
            "origin_keys": entry.origin_keys.into_iter().collect::<Vec<_>>(),
            "event_clocks": entry.event_clocks.into_iter().collect::<Vec<_>>(),
        }))).collect::<serde_json::Map<_, _>>(),
    }))
}

fn phase1_event_cluster_id(role: &str, ticker: &str, evidence: &Value) -> String {
    let clock = phase1_evidence_clock(evidence);
    if role == "analyst.technical" {
        return format!("technical-price-series:{ticker}:{clock}");
    }
    let origin = phase1_evidence_origin_key(evidence);
    if origin.starts_with("url:") {
        // A canonical source URL identifies a single published event even
        // when a downstream source omitted a publish timestamp.  This catches
        // the common Phase 1 / Phase 2 duplicate where the same event arrives
        // through a new `web-*` evidence ID.
        return origin;
    }
    format!("origin:{origin}:{clock}")
}

fn phase1_evidence_clock(evidence: &Value) -> String {
    ["event_time", "published_time", "as_of", "timestamp"]
        .into_iter()
        .filter_map(|field| evidence.get(field).and_then(Value::as_str))
        .find(|value| !value.trim().is_empty())
        .map(normalize_evidence_clock)
        .unwrap_or_else(|| "unknown-time".to_owned())
}

fn phase1_evidence_origin_key(evidence: &Value) -> String {
    let source_url = evidence
        .get("source")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| value.starts_with("https://") || value.starts_with("http://"));
    if let Some(source_url) = source_url {
        return format!("url:{}", normalize_evidence_origin(source_url));
    }
    let origin = evidence
        .get("first_source")
        .or_else(|| evidence.get("source"))
        .and_then(Value::as_str)
        .map(normalize_evidence_origin)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown-origin".to_owned());
    format!("origin:{origin}")
}

fn normalize_evidence_clock(value: &str) -> String {
    value
        .trim()
        .replace(char::is_whitespace, "")
        .to_ascii_lowercase()
}

fn normalize_evidence_origin(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect()
}

fn phase1_source_confidence(item: &Value) -> (f64, &'static str) {
    let reported = item
        .get("source_confidence")
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
        .unwrap_or(0.0);
    let verified_technical_input = item
        .get("source")
        .and_then(Value::as_str)
        .is_some_and(|source| source == "filestore.run_input.technical")
        && item
            .get("evidence_refs")
            .and_then(Value::as_array)
            .is_some_and(|references| {
                references.iter().any(|reference| {
                    reference.as_str().is_some_and(|reference| {
                        reference.starts_with("technical-") && is_phase1_evidence_id(reference)
                    })
                })
            });

    if verified_technical_input && reported == 0.0 {
        // The Phase 1 Summary schema uses 0.0 as the placeholder for an
        // unavailable source-confidence value. A sealed technical input is
        // runtime-verified, so a conservative neutral floor keeps a valid
        // technical report from disappearing from the probability base.
        return (0.5, "rust_verified_technical_input_neutral_default");
    }
    (reported, "model_reported")
}

fn phase1_evidence_assessment(
    ticker_report: &Value,
    role: &str,
    ticker: &str,
    as_of_date: Option<NaiveDate>,
    event_occurrences: &BTreeMap<String, usize>,
) -> Result<Value> {
    let evidence = ticker_report
        .get("key_evidence")
        .and_then(Value::as_array)
        .with_context(|| format!("{role} report requires key_evidence for {ticker}"))?;
    if evidence.is_empty() {
        bail!("{role} report has no key_evidence for {ticker}")
    }
    let mut evidence_quality_total = 0.0;
    let mut freshness_total = 0.0;
    let mut type_total = 0.0;
    let mut tier_total = 0.0;
    let mut source_confidence_total = 0.0;
    let mut time_lineage_total = 0.0;
    let mut duplicate_total = 0.0;
    let mut correlation_groups = BTreeSet::new();
    let mut event_cluster_ids = BTreeSet::new();
    let mut records = Vec::new();
    for item in evidence {
        let evidence_type = item
            .get("evidence_type")
            .and_then(Value::as_str)
            .unwrap_or("inference");
        let type_weight = match evidence_type {
            "fact" => 1.0,
            "opinion" => 0.65,
            "inference" => 0.45,
            _ => 0.25,
        };
        let source_tier = item
            .get("source_tier")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let tier_weight = match source_tier {
            "official" => 1.0,
            "major_media" => 0.82,
            "professional_research" => 0.74,
            "longform_analysis" => 0.58,
            _ => 0.45,
        };
        let (source_confidence, source_confidence_basis) = phase1_source_confidence(item);
        let derivative_weight = if item
            .get("is_derivative_repost")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            0.40
        } else {
            1.0
        };
        let (freshness_weight, time_lineage_weight, time_audit) =
            phase1_time_assessment(item, as_of_date);
        let event_cluster_id = phase1_event_cluster_id(role, ticker, item);
        let occurrence_count = event_occurrences
            .get(&event_cluster_id)
            .copied()
            .unwrap_or(1);
        let duplicate_event_weight = 1.0 / occurrence_count as f64;
        let correlation_group = if role == "analyst.technical" {
            format!("technical-price-series:{ticker}")
        } else {
            phase1_evidence_origin_key(item)
        };
        correlation_groups.insert(correlation_group.clone());
        event_cluster_ids.insert(event_cluster_id.clone());
        let quality = type_weight
            * tier_weight
            * source_confidence
            * freshness_weight
            * time_lineage_weight
            * derivative_weight
            * duplicate_event_weight;
        evidence_quality_total += quality;
        freshness_total += freshness_weight;
        type_total += type_weight;
        tier_total += tier_weight;
        source_confidence_total += source_confidence;
        time_lineage_total += time_lineage_weight;
        duplicate_total += duplicate_event_weight;
        records.push(json!({
            "event_cluster_id": event_cluster_id,
            "evidence_refs": item.get("evidence_refs").cloned().unwrap_or_else(|| json!([])),
            "evidence_type": evidence_type,
            "source_tier": source_tier,
            "source_confidence": source_confidence,
            "source_confidence_basis": source_confidence_basis,
            "type_weight": type_weight,
            "tier_weight": tier_weight,
            "freshness_weight": freshness_weight,
            "time_lineage_weight": time_lineage_weight,
            "derivative_weight": derivative_weight,
            "duplicate_event_weight": duplicate_event_weight,
            "correlation_group": correlation_group,
            "time_audit": time_audit,
            "quality_weight": round_probability(quality),
        }));
    }
    let count = evidence.len() as f64;
    let correlation_discount = (correlation_groups.len() as f64 / count).sqrt();
    let echo_discount = match ticker_report
        .get("echo_chamber_risk")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
    {
        "low" => 1.0,
        "medium" => 0.85,
        "high" => 0.65,
        _ => 0.75,
    };
    let evidence_quality = (evidence_quality_total / count) * correlation_discount * echo_discount;
    Ok(json!({
        "authority": "rust_phase1_evidence_lineage_v1",
        "evidence_quality": round_probability(evidence_quality),
        "freshness_weight": round_probability(freshness_total / count),
        "evidence_type_weight": round_probability(type_total / count),
        "source_tier_weight": round_probability(tier_total / count),
        "source_confidence_weight": round_probability(source_confidence_total / count),
        "time_lineage_weight": round_probability(time_lineage_total / count),
        "duplicate_event_weight": round_probability(duplicate_total / count),
        "correlation_discount": round_probability(correlation_discount),
        "echo_chamber_discount": echo_discount,
        "event_cluster_ids": event_cluster_ids.into_iter().collect::<Vec<_>>(),
        "correlation_groups": correlation_groups.into_iter().collect::<Vec<_>>(),
        "records": records,
    }))
}

fn phase1_time_assessment(evidence: &Value, as_of_date: Option<NaiveDate>) -> (f64, f64, Value) {
    let availability_clock = ["ingested_time", "published_time", "timestamp"]
        .into_iter()
        .filter_map(|field| {
            evidence
                .get(field)
                .and_then(Value::as_str)
                .map(|value| (field, value))
        })
        .find(|(_, value)| !value.trim().is_empty());
    let observed_date = availability_clock.and_then(|(_, value)| parse_evidence_date(value));
    let (freshness_weight, availability_status, age_days) = match (as_of_date, observed_date) {
        (Some(as_of), Some(observed)) if observed > as_of => (
            0.0,
            "future_evidence_rejected",
            Some((observed - as_of).num_days()),
        ),
        (Some(as_of), Some(observed)) => {
            let age_days = (as_of - observed).num_days();
            let weight = match age_days {
                ..=2 => 1.0,
                3..=5 => 0.75,
                6..=10 => 0.40,
                _ => 0.10,
            };
            (weight, "available_at_as_of", Some(age_days))
        }
        _ => {
            let weight = match evidence.get("evidence_age").and_then(Value::as_str) {
                Some("0-2d") => 0.75,
                Some("3-5d") => 0.55,
                Some("6-10d") => 0.30,
                Some("10d+") => 0.10,
                _ => 0.25,
            };
            (weight, "availability_time_unverified", None)
        }
    };
    let time_fields_present = [
        "event_time",
        "published_time",
        "ingested_time",
        "as_of",
        "timezone",
    ]
    .into_iter()
    .filter(|field| {
        evidence
            .get(*field)
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty())
    })
    .count();
    let time_lineage_weight = match time_fields_present {
        0 => 0.65,
        1 => 0.75,
        2 => 0.85,
        _ => 1.0,
    };
    (
        freshness_weight,
        time_lineage_weight,
        json!({
            "availability_status": availability_status,
            "availability_clock": availability_clock.map(|(field, _)| field),
            "age_days": age_days,
            "time_fields_present": time_fields_present,
            "declared_evidence_age": evidence.get("evidence_age"),
        }),
    )
}

fn parse_evidence_date(value: &str) -> Option<NaiveDate> {
    let date = value.trim().get(..10)?;
    NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()
}

fn analyst_calibration_multiplier(state: &Value, role: &str, ticker: &str) -> Value {
    let record = state.pointer(&format!("/analyst_calibration/{role}/{ticker}"));
    let trusted_authority = record
        .and_then(|value| value.get("authority"))
        .and_then(Value::as_str)
        == Some("rust_canonical_outcome_brier_v1");
    let status_available = record
        .and_then(|value| value.get("status"))
        .and_then(Value::as_str)
        == Some("available");
    let sample_size = record
        .and_then(|value| value.get("sample_size"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let reliability = record
        .and_then(|value| value.get("reliability"))
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && (0.0..=1.0).contains(value));
    if trusted_authority && status_available && sample_size >= 20 {
        if let Some(reliability) = reliability {
            return json!({
                "authority": "phase0_out_of_sample_calibration",
                "status": "available",
                "sample_size": sample_size,
                "reliability": reliability,
                "multiplier": round_probability(0.5 + 0.5 * reliability),
            });
        }
    }
    json!({
        "authority": "bootstrap_uncalibrated_discount",
        "status": "unavailable",
        "rejected_calibration_authority": record
            .and_then(|value| value.get("authority"))
            .cloned()
            .unwrap_or(Value::Null),
        "rejected_calibration_status": record
            .and_then(|value| value.get("status"))
            .cloned()
            .unwrap_or(Value::Null),
        "sample_size": sample_size,
        "minimum_sample_size": 20,
        "reliability": 0.60,
        "multiplier": 0.80,
    })
}

fn round_probability(value: f64) -> f64 {
    (value * 1_000_000.0).round() / 1_000_000.0
}

fn validate_phase1_compiled_fields(fields: &serde_json::Map<String, Value>) -> Result<()> {
    let per_ticker = fields
        .get("per_ticker")
        .and_then(Value::as_object)
        .context("Phase 1 Summary requires per_ticker")?;
    for (ticker, report) in per_ticker {
        let canonical = serde_json::from_value::<AnalystTickerArtifact>(report.clone())
            .with_context(|| {
                format!("Phase 1 canonical AnalystTickerArtifact invalid for {ticker}")
            })?;
        validate_analyst_ticker_artifact(&canonical).map_err(|error| {
            anyhow::anyhow!("Phase 1 analyst artifact invalid for {ticker}: {error}")
        })?;
        if canonical.key_evidence.is_empty() && ticker != "VIX" {
            bail!("Phase 1 empty key_evidence is only allowed for context-only VIX, not {ticker}");
        }
        for evidence in &canonical.key_evidence {
            if evidence.evidence_refs.is_empty() {
                bail!("Phase 1 evidence for {ticker} requires at least one stable evidence_refs ID")
            }
        }
    }
    let fields_value = Value::Object(fields.clone());
    validate_phase1_reference_arrays(&fields_value, None)?;
    validate_phase1_web_source_urls(&fields_value)?;
    Ok(())
}

fn validate_phase1_web_source_urls(value: &Value) -> Result<()> {
    match value {
        Value::Array(values) => {
            for value in values {
                validate_phase1_web_source_urls(value)?;
            }
        }
        Value::Object(values) => {
            let has_web_ref = values
                .get("evidence_refs")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .any(|reference| reference.starts_with("web-"));
            if has_web_ref
                && !values
                    .get("source")
                    .and_then(Value::as_str)
                    .is_some_and(|source| {
                        source.starts_with("https://") || source.starts_with("http://")
                    })
            {
                bail!("Phase 1 web evidence requires an authoritative http(s) source URL")
            }
            for value in values.values() {
                validate_phase1_web_source_urls(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_phase1_reference_arrays(value: &Value, parent_key: Option<&str>) -> Result<()> {
    match value {
        Value::Array(values) if matches!(parent_key, Some("evidence_refs" | "source_refs")) => {
            for reference in values {
                let reference = reference
                    .as_str()
                    .context("Phase 1 evidence reference must be a string")?;
                if !is_phase1_evidence_id(reference) {
                    bail!(
                        "Phase 1 evidence reference {reference:?} must be a complete technical-, jin10-, or web-sha256 ID"
                    )
                }
            }
            Ok(())
        }
        Value::Array(values) => {
            for value in values {
                validate_phase1_reference_arrays(value, parent_key)?;
            }
            Ok(())
        }
        Value::Object(values) => {
            for (key, value) in values {
                validate_phase1_reference_arrays(value, Some(key))?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn is_phase1_evidence_id(reference: &str) -> bool {
    ["technical-", "jin10-", "web-"]
        .into_iter()
        .find_map(|prefix| reference.strip_prefix(prefix))
        .is_some_and(|digest| {
            digest.len() == 64
                && digest
                    .chars()
                    .all(|character| character.is_ascii_hexdigit())
        })
}

fn validate_phase3_compiled_fields(
    state: &Value,
    fields: &mut serde_json::Map<String, Value>,
) -> Result<()> {
    let verified_source_refs = verified_phase3_source_refs(state)?;
    let decisions = fields
        .get_mut("decisions")
        .and_then(Value::as_object_mut)
        .context("Phase 3 Summary requires decisions")?;
    for ticker in investable_assets_from_state(state) {
        let rust_base = state
            .pointer(&format!(
                "/weighted_probability_base/{ticker}/long_probability"
            ))
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
            .with_context(|| format!("Rust weighted probability base missing for {ticker}"))?;
        let decision = decisions
            .get_mut(&ticker)
            .and_then(Value::as_object_mut)
            .with_context(|| format!("Phase 3 Summary decision missing for {ticker}"))?;
        if let Some(verified_source_refs) = verified_source_refs.as_ref() {
            project_phase3_evidence_refs(decision, verified_source_refs);
        }
        // The Phase 1 weighted base is Rust-owned. Preserve any model echo for
        // audit, but always project the canonical value before checking the
        // model-owned debate adjustment and final probability ledger.
        let model_base = decision
            .get("base_probability")
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite() && (0.0..=1.0).contains(value));
        let base_overridden = model_base.is_none_or(|base| (base - rust_base).abs() > 0.000001);
        decision.insert("base_probability".to_owned(), json!(rust_base));
        decision.insert(
            "base_probability_projection".to_owned(),
            json!({
                "authority": "rust_weighted_probability_base",
                "model_base_probability": model_base,
                "projected_base_probability": rust_base,
                "overridden": base_overridden,
            }),
        );
        let long = required_probability(decision, "long_probability", &ticker)?;
        let short = required_probability(decision, "short_probability", &ticker)?;
        if (long + short - 1.0).abs() > 0.000001 {
            bail!("Phase 3 long_probability + short_probability must equal 1 for {ticker}")
        }
        let adjustment = decision
            .get("debate_adjustment")
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite())
            .with_context(|| format!("Phase 3 debate_adjustment missing for {ticker}"))?;
        let expected_long = round_probability(rust_base + adjustment);
        if !(0.0..=1.0).contains(&expected_long) || (long - expected_long).abs() > 0.000001 {
            bail!(
                "Phase 3 probability ledger mismatch for {ticker}: Rust base {rust_base} + debate_adjustment {adjustment} != long_probability {long}"
            )
        }
        if adjustment.abs() > 0.000001 {
            let accepted_claim_evidence = accepted_phase2_claim_evidence(state)?;
            let base_event_evidence = phase1_base_event_evidence(state, &ticker)?;
            validate_phase3_adjustment_provenance(
                decision,
                &ticker,
                adjustment,
                rust_base,
                &accepted_claim_evidence,
                &base_event_evidence,
            )?;
        } else {
            decision.insert("adjustment_reason".to_owned(), Value::Null);
            decision.insert("adjustment_scale".to_owned(), Value::Null);
        }
        if decision.get("scenarios").is_none_or(Value::is_null) {
            bail!("Phase 3 scenarios are required for {ticker}")
        }
        validate_phase3_scenario_probabilities(decision, long, &ticker)?;
        let model_rating = decision
            .get("rating")
            .and_then(Value::as_str)
            .unwrap_or("missing")
            .to_owned();
        let rust_rating = research_rating_for_probability(long);
        let rating_overridden = model_rating != rust_rating;
        decision.insert("rating".to_owned(), Value::String(rust_rating.to_owned()));
        decision.insert(
            "rating_projection".to_owned(),
            json!({
                "authority": "rust",
                "model_rating": model_rating,
                "projected_rating": rust_rating,
                "overridden": rating_overridden,
            }),
        );
        if rust_rating == "Hold" {
            let confidence_basis = decision
                .get("confidence_basis")
                .and_then(Value::as_str)
                .context("Phase 3 Hold requires confidence_basis")?;
            let hold_reason = match confidence_basis {
                "evidence_balanced" => "evidence_balanced",
                "data_insufficient" => "evidence_insufficient",
                "conflicting_evidence" => "conflicting_evidence",
                other => bail!("Phase 3 Hold cannot use confidence_basis={other}"),
            };
            decision.insert(
                "hold_reason".to_owned(),
                Value::String(hold_reason.to_owned()),
            );
        } else {
            decision.insert("hold_reason".to_owned(), Value::Null);
        }
        let canonical = serde_json::from_value::<ResearchDecision>(json!({
            "rating": decision.get("rating"),
            "long_probability": long,
            "short_probability": short,
            "confidence_basis": decision.get("confidence_basis"),
            "hold_reason": decision.get("hold_reason"),
            "plan": decision.get("plan"),
            "probability_rationale": decision.get("probability_rationale"),
            "scenarios": decision.get("scenarios"),
        }))
        .with_context(|| format!("Phase 3 canonical ResearchDecision invalid for {ticker}"))?;
        validate_research_decision(&canonical)
            .map_err(|error| anyhow::anyhow!("Phase 3 decision invalid for {ticker}: {error}"))?;
    }
    Ok(())
}

fn verified_phase3_source_refs(state: &Value) -> Result<Option<BTreeSet<String>>> {
    let Some(store_root) = state.get("store_root").and_then(Value::as_str) else {
        return Ok(None);
    };
    let store = FileStore::open(store_root, FileStoreOptions::default())?;
    let location = run_location_from_state(state)?;
    let mut verified = BTreeSet::new();
    for phase in [1_u8, 2_u8] {
        let indexes = read_all_indexes(
            &store,
            Some(&location),
            &IndexQuery {
                source_phase: Some(phase),
                ..IndexQuery::default()
            },
        )?;
        for index in indexes {
            verified.insert(index.index_id);
            if phase == 1 {
                let fields = Value::Object(index.authoritative_fields);
                collect_reference_array_ids(&fields, &mut verified);
                for record in phase1_verified_web_evidence_records(&fields) {
                    if let Some(evidence_id) = record.get("evidence_id").and_then(Value::as_str) {
                        verified.insert(evidence_id.to_owned());
                    }
                }
                for record in phase1_verified_input_evidence_records(&fields) {
                    if let Some(evidence_id) = record.get("evidence_id").and_then(Value::as_str) {
                        verified.insert(evidence_id.to_owned());
                    }
                }
            } else if let Some(web_evidence) = index.authoritative_fields.get("web_evidence") {
                collect_complete_phase1_ids(web_evidence, &mut verified);
            }
        }
    }
    if verified.is_empty() {
        bail!("Phase 3 requires persisted Phase 1/2 Index provenance")
    }
    Ok(Some(verified))
}

fn collect_reference_array_ids(value: &Value, verified: &mut BTreeSet<String>) {
    match value {
        Value::Object(values) => {
            for (key, value) in values {
                if matches!(key.as_str(), "evidence_refs" | "source_refs") {
                    collect_complete_phase1_ids(value, verified);
                } else {
                    collect_reference_array_ids(value, verified);
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_reference_array_ids(value, verified);
            }
        }
        _ => {}
    }
}

fn collect_complete_phase1_ids(value: &Value, verified: &mut BTreeSet<String>) {
    match value {
        Value::String(reference) if is_phase1_evidence_id(reference) => {
            verified.insert(reference.to_owned());
        }
        Value::Array(values) => {
            for value in values {
                collect_complete_phase1_ids(value, verified);
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                collect_complete_phase1_ids(value, verified);
            }
        }
        _ => {}
    }
}

fn project_phase3_evidence_refs(
    decision: &mut serde_json::Map<String, Value>,
    verified_source_refs: &BTreeSet<String>,
) {
    let mut model_refs = Vec::new();
    let mut projected_refs = BTreeSet::new();
    let mut removed = 0usize;
    for hinge in decision
        .get_mut("decision_hinges")
        .and_then(Value::as_array_mut)
        .into_iter()
        .flatten()
    {
        let Some(refs) = hinge.get_mut("evidence_refs").and_then(Value::as_array_mut) else {
            continue;
        };
        for reference in refs.iter().filter_map(Value::as_str) {
            model_refs.push(reference.to_owned());
        }
        refs.retain(|reference| {
            let keep = reference
                .as_str()
                .is_some_and(|reference| verified_source_refs.contains(reference));
            removed += usize::from(!keep);
            if let Some(reference) = reference.as_str().filter(|_| keep) {
                projected_refs.insert(reference.to_owned());
            }
            keep
        });
    }
    decision.insert(
        "evidence_reference_projection".to_owned(),
        json!({
            "authority": "rust_filestore_indexes",
            "model_refs": model_refs,
            "projected_refs": projected_refs.into_iter().collect::<Vec<_>>(),
            "unverified_refs_removed": removed,
        }),
    );
}

/// Return the evidence associated with claims that Rust itself recorded as a
/// completed Bull/Bear consensus.  A Phase 2 topic that ended unresolved can
/// still be useful context, but it is not an authority for a non-zero
/// probability adjustment.
fn accepted_phase2_claim_evidence(state: &Value) -> Result<BTreeMap<String, BTreeSet<String>>> {
    let topics = state
        .get("topic_debate_states")
        .and_then(Value::as_object)
        .context("non-zero Phase 3 debate adjustment requires Phase 2 topic state")?;
    let mut accepted = BTreeMap::new();
    for (topic_id, topic_state) in topics {
        let closure = topic_state
            .pointer("/stree/closure")
            .and_then(Value::as_object)
            .with_context(|| format!("Phase 2 topic {topic_id} has no closure"))?;
        if closure.get("reason").and_then(Value::as_str) != Some("consensus") {
            continue;
        }
        if closure.get("controller_decided").and_then(Value::as_bool) != Some(true) {
            bail!("Phase 2 topic {topic_id} consensus was not controller-decided")
        }
        let independence = closure
            .get("independence_assessment")
            .and_then(Value::as_object);
        if independence
            .and_then(|assessment| assessment.get("adjustment_eligible"))
            .and_then(Value::as_bool)
            != Some(true)
        {
            let reason = independence
                .and_then(|assessment| assessment.get("reason"))
                .and_then(Value::as_str)
                .unwrap_or("independence_not_proven");
            bail!(
                "Phase 2 topic {topic_id} consensus is correlated and cannot support a non-zero Phase 3 adjustment: {reason}"
            )
        }
        let claim_evidence_links = closure
            .get("claim_ledger")
            .and_then(Value::as_array)
            .with_context(|| format!("Phase 2 topic {topic_id} consensus has no claim_ledger"))?
            .iter()
            .filter_map(|claim| {
                let claim_id = claim.get("claim_id").and_then(Value::as_str)?;
                let evidence_refs = claim
                    .get("evidence_links")
                    .and_then(Value::as_array)?
                    .iter()
                    .filter_map(|link| link.get("evidence_ref").and_then(Value::as_str))
                    .map(ToOwned::to_owned)
                    .collect::<BTreeSet<_>>();
                Some((claim_id.to_owned(), evidence_refs))
            })
            .collect::<BTreeMap<_, _>>();
        let controller_accepted_evidence = closure
            .get("accepted_evidence")
            .and_then(Value::as_array)
            .with_context(|| {
                format!(
                    "Phase 2 topic {topic_id} consensus has no controller-attested accepted_evidence"
                )
            })?
            .iter()
            .filter_map(|accepted| {
                let claim_id = accepted.get("claim_id").and_then(Value::as_str)?;
                let evidence_refs = accepted
                    .get("evidence_refs")
                    .and_then(Value::as_array)?
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToOwned::to_owned)
                    .collect::<BTreeSet<_>>();
                Some((claim_id.to_owned(), evidence_refs))
            })
            .collect::<BTreeMap<_, _>>();
        let controller_verified_evidence = closure
            .get("controller_verified_evidence_refs")
            .and_then(Value::as_array)
            .with_context(|| {
                format!(
                    "Phase 2 topic {topic_id} consensus has no Controller evidence observation ledger"
                )
            })?
            .iter()
            .filter_map(Value::as_str)
            .map(ToOwned::to_owned)
            .collect::<BTreeSet<_>>();
        for claim_id in closure
            .get("consensus_claim_ids")
            .and_then(Value::as_array)
            .with_context(|| {
                format!("Phase 2 topic {topic_id} consensus has no consensus_claim_ids")
            })?
            .iter()
        {
            let claim_id = claim_id.as_str().with_context(|| {
                format!("Phase 2 topic {topic_id} consensus claim ID must be a string")
            })?;
            let declared_evidence_refs = claim_evidence_links.get(claim_id).with_context(|| {
                format!(
                    "Phase 2 topic {topic_id} consensus claim {claim_id} has no declared claim-evidence links"
                )
            })?;
            let evidence_refs = controller_accepted_evidence.get(claim_id).with_context(|| {
                format!(
                    "Phase 2 topic {topic_id} consensus claim {claim_id} was not explicitly accepted with evidence by the Controller"
                )
            })?;
            if evidence_refs.is_empty() {
                bail!(
                    "Phase 2 topic {topic_id} consensus claim {claim_id} has no controller-attested stable evidence refs"
                )
            }
            if !evidence_refs.is_subset(declared_evidence_refs) {
                bail!(
                    "Phase 2 topic {topic_id} consensus claim {claim_id} has accepted evidence without a participant-declared relation"
                )
            }
            if !evidence_refs.is_subset(&controller_verified_evidence) {
                bail!(
                    "Phase 2 topic {topic_id} consensus claim {claim_id} has accepted evidence that the Controller did not observe"
                )
            }
            accepted.insert(claim_id.to_owned(), evidence_refs.clone());
        }
    }
    if accepted.is_empty() {
        bail!(
            "non-zero Phase 3 debate adjustment requires at least one controller-accepted Phase 2 consensus claim"
        )
    }
    Ok(accepted)
}

fn validate_phase3_adjustment_provenance(
    decision: &mut serde_json::Map<String, Value>,
    ticker: &str,
    adjustment: f64,
    base_probability: f64,
    accepted_claim_evidence: &BTreeMap<String, BTreeSet<String>>,
    base_event_evidence: &BTreeMap<String, String>,
) -> Result<()> {
    let adjustment_reason = decision
        .get("adjustment_reason")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .with_context(|| {
            format!("non-zero Phase 3 debate adjustment requires adjustment_reason for {ticker}")
        })?;
    const ADJUSTMENT_REASONS: &[&str] = &[
        "new_information",
        "duplicate_evidence_discount",
        "direction_conflict_discount",
        "evidence_contradiction_discount",
        "missing_data_convergence",
        "track_record_convergence",
    ];
    if !ADJUSTMENT_REASONS.contains(&adjustment_reason) {
        bail!("Phase 3 adjustment_reason {adjustment_reason:?} is invalid for {ticker}")
    }
    let adjustment_scale = decision
        .get("adjustment_scale")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .with_context(|| {
            format!("non-zero Phase 3 debate adjustment requires adjustment_scale for {ticker}")
        })?;
    if adjustment_scale != "uncalibrated_conservative_v1" {
        bail!(
            "Phase 3 adjustment_scale must be uncalibrated_conservative_v1 until an explicit historical calibration scale is available for {ticker}"
        )
    }
    let absolute_adjustment = adjustment.abs();
    if ![0.01_f64, 0.03_f64]
        .iter()
        .any(|allowed| (absolute_adjustment - allowed).abs() <= 0.000001)
    {
        bail!(
            "Phase 3 uncalibrated debate_adjustment for {ticker} must have absolute value 0.01 or 0.03"
        )
    }
    let hinges = decision
        .get("decision_hinges")
        .and_then(Value::as_array)
        .with_context(|| {
            format!("non-zero Phase 3 debate adjustment requires decision_hinges for {ticker}")
        })?;
    if hinges.is_empty() {
        bail!("non-zero Phase 3 debate adjustment requires decision_hinges for {ticker}")
    }

    let mut referenced_claim_ids = BTreeSet::new();
    let mut matched_evidence_refs = BTreeSet::new();
    let mut all_hinge_evidence_refs = BTreeSet::new();
    for hinge in hinges {
        let evidence_refs = hinge
            .get("evidence_refs")
            .and_then(Value::as_array)
            .with_context(|| {
                format!(
                    "non-zero Phase 3 debate adjustment requires complete stable evidence_refs for {ticker}"
                )
            })?
            .iter()
            .map(|reference| {
                reference
                    .as_str()
                    .map(str::trim)
                    .filter(|reference| {
                        !reference.is_empty()
                            && !reference.contains("...")
                            && !reference.starts_with("web.run:search")
                    })
                    .map(ToOwned::to_owned)
                    .with_context(|| {
                        format!(
                            "non-zero Phase 3 debate adjustment requires complete stable evidence_refs for {ticker}"
                        )
                    })
            })
            .collect::<Result<BTreeSet<_>>>()?;
        if evidence_refs.is_empty() {
            bail!(
                "non-zero Phase 3 debate adjustment requires complete stable evidence_refs for {ticker}"
            )
        }
        all_hinge_evidence_refs.extend(evidence_refs.iter().cloned());
        let phase2_claim_ids = hinge
            .get("phase2_claim_ids")
            .and_then(Value::as_array)
            .with_context(|| {
                format!("non-zero Phase 3 debate adjustment requires phase2_claim_ids for {ticker}")
            })?;
        if phase2_claim_ids.is_empty() {
            bail!("non-zero Phase 3 debate adjustment requires phase2_claim_ids for {ticker}")
        }
        let mut hinge_matches_accepted_evidence = false;
        for claim_id in phase2_claim_ids {
            let claim_id = claim_id.as_str().map(str::trim).filter(|id| !id.is_empty()).with_context(|| {
                format!(
                    "non-zero Phase 3 debate adjustment requires complete phase2_claim_ids for {ticker}"
                )
            })?;
            let accepted_evidence = accepted_claim_evidence.get(claim_id).with_context(|| {
                format!(
                    "Phase 3 adjustment for {ticker} references Phase 2 claim {claim_id} that was not controller-accepted"
                )
            })?;
            let overlap = evidence_refs
                .intersection(accepted_evidence)
                .cloned()
                .collect::<BTreeSet<_>>();
            hinge_matches_accepted_evidence |= !overlap.is_empty();
            matched_evidence_refs.extend(overlap);
            referenced_claim_ids.insert(claim_id.to_owned());
        }
        if !hinge_matches_accepted_evidence {
            bail!(
                "Phase 3 adjustment for {ticker} must cite evidence attached to each controller-accepted Phase 2 claim"
            )
        }
    }
    let base_evidence_refs = base_event_evidence.keys().cloned().collect::<BTreeSet<_>>();
    let base_overlap_refs = all_hinge_evidence_refs
        .intersection(&base_evidence_refs)
        .cloned()
        .collect::<BTreeSet<_>>();
    let novel_evidence_refs = all_hinge_evidence_refs
        .difference(&base_overlap_refs)
        .cloned()
        .collect::<BTreeSet<_>>();
    let base_overlap_event_cluster_ids = base_overlap_refs
        .iter()
        .filter_map(|reference| base_event_evidence.get(reference))
        .cloned()
        .collect::<BTreeSet<_>>();
    if novel_evidence_refs.is_empty() {
        if !matches!(
            adjustment_reason,
            "duplicate_evidence_discount"
                | "direction_conflict_discount"
                | "evidence_contradiction_discount"
                | "missing_data_convergence"
                | "track_record_convergence"
        ) {
            bail!(
                "Phase 3 adjustment for {ticker} has no novel evidence and must be an explicit convergence/correction reason"
            )
        }
        let corrected_long = round_probability(base_probability + adjustment);
        if (corrected_long - 0.5).abs() >= (base_probability - 0.5).abs() - 0.000001 {
            bail!(
                "Phase 3 adjustment for {ticker} reuses base evidence and must converge toward 0.5 rather than reinforce it"
            )
        }
    }
    decision.insert(
        "debate_adjustment_provenance".to_owned(),
        json!({
            "authority": "rust_phase2_consensus_claims",
            "adjustment": adjustment,
            "adjustment_reason": adjustment_reason,
            "adjustment_scale": adjustment_scale,
            "accepted_phase2_claim_ids": referenced_claim_ids.into_iter().collect::<Vec<_>>(),
            "matched_evidence_refs": matched_evidence_refs.into_iter().collect::<Vec<_>>(),
            "base_overlap_evidence_refs": base_overlap_refs.into_iter().collect::<Vec<_>>(),
            "base_overlap_event_cluster_ids": base_overlap_event_cluster_ids.into_iter().collect::<Vec<_>>(),
            "novel_evidence_refs": novel_evidence_refs.into_iter().collect::<Vec<_>>(),
        }),
    );
    Ok(())
}

/// Maps every Phase 1 evidence reference affecting `ticker` to its Rust-owned
/// event cluster.  Phase 3 needs this ledger to distinguish new debate
/// evidence from a second presentation of the base signal.
fn phase1_base_event_evidence(state: &Value, ticker: &str) -> Result<BTreeMap<String, String>> {
    let events = state
        .pointer("/phase1_evidence_event_ledger/events")
        .and_then(Value::as_object)
        .context("Phase 3 adjustment provenance requires the Phase 1 event ledger")?;
    let mut references = BTreeMap::new();
    for (event_cluster_id, event) in events {
        let applies_to_ticker = event
            .get("tickers")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .any(|candidate| candidate == ticker);
        if !applies_to_ticker {
            continue;
        }
        for reference in event
            .get("evidence_refs")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
        {
            references.insert(reference.to_owned(), event_cluster_id.clone());
        }
    }
    if references.is_empty() {
        bail!("Phase 3 adjustment provenance found no Phase 1 evidence for {ticker}")
    }
    Ok(references)
}

fn validate_phase3_scenario_probabilities(
    decision: &mut serde_json::Map<String, Value>,
    long_probability: f64,
    ticker: &str,
) -> Result<()> {
    let scenarios = decision
        .get("scenarios")
        .and_then(Value::as_object)
        .with_context(|| format!("Phase 3 scenarios must be an object for {ticker}"))?;
    let probability = |scenario: &str| -> Result<f64> {
        scenarios
            .get(scenario)
            .and_then(Value::as_object)
            .and_then(|value| value.get("probability"))
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
            .with_context(|| format!("Phase 3 {ticker} scenario {scenario} probability is invalid"))
    };
    let conditional_long_probability = |scenario: &str| -> Result<f64> {
        scenarios
            .get(scenario)
            .and_then(Value::as_object)
            .and_then(|value| value.get("conditional_long_probability"))
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
            .with_context(|| {
                format!(
                    "Phase 3 {ticker} scenario {scenario} conditional_long_probability is invalid"
                )
            })
    };
    let bull = probability("bull")?;
    let base = probability("base")?;
    let bear = probability("bear")?;
    let bull_conditional = conditional_long_probability("bull")?;
    let base_conditional = conditional_long_probability("base")?;
    let bear_conditional = conditional_long_probability("bear")?;
    if (bull + base + bear - 1.0).abs() > 0.000001 {
        bail!("Phase 3 {ticker} scenario probabilities must sum to 1")
    }
    if bull_conditional < base_conditional || base_conditional < bear_conditional {
        bail!(
            "Phase 3 {ticker} scenario conditional_long_probability must be ordered bull >= base >= bear"
        )
    }
    let implied_long_probability = round_probability(
        bull * bull_conditional + base * base_conditional + bear * bear_conditional,
    );
    if (implied_long_probability - long_probability).abs() > 0.000001 {
        bail!(
            "Phase 3 {ticker} scenario probabilities imply long_probability {implied_long_probability}, expected {long_probability}"
        )
    }
    decision.insert(
        "scenario_probability_validation".to_owned(),
        json!({
            "authority": "rust_validation",
            "model": {
                "bull": {"probability": bull, "conditional_long_probability": bull_conditional},
                "base": {"probability": base, "conditional_long_probability": base_conditional},
                "bear": {"probability": bear, "conditional_long_probability": bear_conditional}
            },
            "identity": "long = Σ(scenario.probability * scenario.conditional_long_probability); scenario probabilities sum to 1",
        }),
    );
    Ok(())
}

fn required_probability(
    object: &serde_json::Map<String, Value>,
    field: &str,
    ticker: &str,
) -> Result<f64> {
    object
        .get(field)
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
        .with_context(|| format!("Phase 3 {field} missing or invalid for {ticker}"))
}

fn validate_phase4_compiled_fields(
    state: &Value,
    fields: &mut serde_json::Map<String, Value>,
) -> Result<()> {
    let plans = fields
        .get_mut("plans")
        .and_then(Value::as_object_mut)
        .context("Phase 4 Summary requires plans")?;
    for ticker in investable_assets_from_state(state) {
        let research = state
            .pointer(&format!("/research_plan/per_ticker/{ticker}"))
            .with_context(|| format!("Phase 3 research decision missing for {ticker}"))?;
        let research_projection = research_plan_to_trade_intent(research);
        let expected_candidate = research_projection["candidate_action"]
            .as_str()
            .context("Rust-owned candidate action missing")?
            .to_owned();
        let research_position_cap = research_projection["research_probability_position_cap"]
            .as_f64()
            .context("Rust-owned probability position cap missing")?;
        let plan = plans
            .get_mut(&ticker)
            .and_then(Value::as_object_mut)
            .with_context(|| format!("Phase 4 plan missing for {ticker}"))?;
        if let Some(reported_candidate) = plan.get("candidate_action").and_then(Value::as_str) {
            if reported_candidate != expected_candidate {
                bail!(
                    "Phase 4 candidate_action for {ticker} must remain {expected_candidate}; got {reported_candidate}"
                )
            }
        }
        plan.insert(
            "candidate_action".to_owned(),
            Value::String(expected_candidate.clone()),
        );
        let action = plan
            .get("action")
            .and_then(Value::as_str)
            .with_context(|| format!("Phase 4 action missing for {ticker}"))?;
        if action != expected_candidate && action != "Hold" {
            bail!(
                "Phase 4 action for {ticker} may execute {expected_candidate} or downgrade to Hold; got {action}"
            )
        }
        let execution_decision = plan
            .get("execution_decision")
            .and_then(Value::as_str)
            .with_context(|| format!("Phase 4 execution_decision missing for {ticker}"))?;
        if (action == "Hold" && execution_decision != "hold")
            || (action != "Hold" && execution_decision != "execute_candidate")
        {
            bail!("Phase 4 action and execution_decision disagree for {ticker}")
        }
        let blockers = plan
            .get("blockers")
            .and_then(Value::as_array)
            .with_context(|| format!("Phase 4 blockers must be an array for {ticker}"))?;
        let mut normalized_blockers = BTreeSet::new();
        for blocker in blockers {
            let blocker = blocker
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .with_context(|| {
                    format!("Phase 4 blockers must contain non-empty strings for {ticker}")
                })?;
            if !normalized_blockers.insert(blocker.to_owned()) {
                bail!("Phase 4 blockers must not contain duplicate values for {ticker}")
            }
        }
        let has_open_blocker = !normalized_blockers.is_empty();
        if has_open_blocker && action != "Hold" {
            bail!(
                "Phase 4 open execution blockers require action=Hold and execution_decision=hold for {ticker}"
            )
        }
        if expected_candidate != "Hold" && action == "Hold" && !has_open_blocker {
            bail!(
                "Phase 4 may downgrade candidate {expected_candidate} to Hold only with an explicit open blocker for {ticker}"
            )
        }
        let position_size_pct_max = plan
            .get("position_size_pct_max")
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
            .with_context(|| {
                format!("Phase 4 position_size_pct_max missing or invalid for {ticker}")
            })?;
        if position_size_pct_max > research_position_cap + 0.000001 {
            bail!(
                "Phase 4 position_size_pct_max {position_size_pct_max} exceeds Rust probability risk budget {research_position_cap} for {ticker}"
            )
        }
        plan.insert(
            "blocker_enforcement".to_owned(),
            json!({
                "authority": "rust_phase4_execution_gate_v1",
                "candidate_action": expected_candidate,
                "open_blockers": normalized_blockers.into_iter().collect::<Vec<_>>(),
                "execution_allowed": !has_open_blocker && action != "Hold",
                "research_probability_position_cap": research_position_cap,
                "trader_position_size_pct_max": position_size_pct_max,
            }),
        );
        let canonical = serde_json::from_value::<TradeIntent>(json!({
            "action": plan.get("action"),
            "candidate_action": plan.get("candidate_action"),
            "execution_decision": plan.get("execution_decision"),
            "entry_price": plan.get("entry_price"),
            "stop_loss": plan.get("stop_loss"),
            "position_size_pct_max": plan.get("position_size_pct_max"),
            "blockers": plan.get("blockers"),
            "rationale": plan.get("rationale"),
        }))
        .with_context(|| format!("Phase 4 canonical TradeIntent invalid for {ticker}"))?;
        validate_trade_intent(&canonical).map_err(|error| {
            anyhow::anyhow!("Phase 4 trade intent invalid for {ticker}: {error}")
        })?;
    }
    Ok(())
}

fn validate_phase5_compiled_fields(
    state: &Value,
    role: &str,
    summary: &str,
    missing_fields: &[String],
    fields: &mut serde_json::Map<String, Value>,
) -> Result<()> {
    let expected_stance = role
        .strip_prefix("risk.")
        .with_context(|| format!("Phase 5 role {role} has no risk stance"))?;
    if let Some(reported) = fields.get("stance").and_then(Value::as_str) {
        // `stance` is a closed, Rust-owned enum. The Summary compiler may use
        // display case (`AGGRESSIVE`) or echo the runtime role (`risk.neutral`).
        // Both spellings identify the same enum member, but no other stance is
        // accepted before Rust rewrites the stored field canonically.
        let normalized = reported.trim().to_ascii_lowercase();
        let normalized = normalized.strip_prefix("risk.").unwrap_or(&normalized);
        if normalized != expected_stance {
            bail!("Phase 5 stance must match runtime role {expected_stance}; got {reported}")
        }
    }
    fields.insert(
        "stance".to_owned(),
        Value::String(expected_stance.to_owned()),
    );
    let no_new_information = fields
        .get("no_new_information")
        .and_then(Value::as_bool)
        .context("Phase 5 no_new_information is required")?;
    let unique = fields
        .get("unique_risk_contribution")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    let adjustment = fields
        .get("recommended_adjustment")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    let risk_dimension = fields
        .get("risk_dimension")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    const RISK_DIMENSIONS: &[&str] = &[
        "gap",
        "liquidity",
        "volatility",
        "correlation",
        "concentration",
        "execution",
        "data_quality",
        "other",
    ];
    if no_new_information {
        if !unique.trim().is_empty() || !adjustment.trim().is_empty() || risk_dimension.is_some() {
            bail!(
                "Phase 5 no_new_information=true requires empty contribution/adjustment and null risk_dimension"
            )
        }
        fields.insert("risk_dimension".to_owned(), Value::Null);
    } else {
        if unique.trim().is_empty() || adjustment.trim().is_empty() {
            bail!("Phase 5 new information requires a contribution and recommended adjustment")
        }
        let risk_dimension = risk_dimension
            .context("Phase 5 new information requires one explicit risk_dimension")?;
        if !RISK_DIMENSIONS.contains(&risk_dimension.as_str()) {
            bail!("Phase 5 risk_dimension is invalid: {risk_dimension}")
        }
        fields.insert("risk_dimension".to_owned(), Value::String(risk_dimension));
    }
    let disagreement = fields
        .get("disagreement_with_prior")
        .and_then(Value::as_str)
        .context("Phase 5 disagreement_with_prior is required")?;
    let cash_hedge = phase5_string(
        fields.get("cash_hedge_recommendation"),
        "portfolio",
        "cash_hedge_recommendation",
        missing_fields,
    )?;
    let per_asset = fields
        .get("per_asset")
        .and_then(Value::as_object)
        .context("Phase 5 Summary requires per_asset")?;
    for ticker in investable_assets_from_state(state) {
        let constraints = per_asset
            .get(&ticker)
            .and_then(Value::as_object)
            .with_context(|| format!("Phase 5 constraints missing for {ticker}"))?;
        let stop_type = match constraints.get("stop_type").and_then(Value::as_str) {
            Some("hard") => StopType::Hard,
            Some("soft") => StopType::Soft,
            Some("none") => StopType::None,
            Some(other) if !other.trim().is_empty() => {
                bail!("Phase 5 stop_type invalid for {ticker}: {other}")
            }
            _ if phase5_field_is_missing(missing_fields, &ticker, "stop_type") => StopType::None,
            _ => bail!("Phase 5 stop_type missing without missing_fields entry for {ticker}"),
        };
        let canonical = RiskConstraints {
            stance: expected_stance.to_owned(),
            argument: summary.to_owned(),
            unique_risk_contribution: unique.clone(),
            disagreement_with_prior: disagreement.to_owned(),
            no_new_information,
            recommended_adjustment: adjustment.clone(),
            stop_type,
            max_drawdown_pct: phase5_number(
                constraints.get("max_drawdown_pct"),
                &ticker,
                "max_drawdown_pct",
                missing_fields,
            )?,
            position_cap_pct: phase5_number(
                constraints.get("position_cap_pct"),
                &ticker,
                "position_cap_pct",
                missing_fields,
            )?,
            rebalance_trigger: phase5_string(
                constraints.get("rebalance_trigger"),
                &ticker,
                "rebalance_trigger",
                missing_fields,
            )?,
            risk_off_trigger: phase5_string(
                constraints.get("risk_off_trigger"),
                &ticker,
                "risk_off_trigger",
                missing_fields,
            )?,
            review_window: phase5_string(
                constraints.get("review_window"),
                &ticker,
                "review_window",
                missing_fields,
            )?,
            cash_hedge_recommendation: cash_hedge.clone(),
            constraint_confidence: phase5_number(
                constraints.get("constraint_confidence"),
                &ticker,
                "constraint_confidence",
                missing_fields,
            )?,
        };
        validate_risk_constraints(&canonical).map_err(|error| {
            anyhow::anyhow!("Phase 5 constraints invalid for {ticker}: {error}")
        })?;
    }
    Ok(())
}

fn phase5_field_is_missing(missing_fields: &[String], scope: &str, field: &str) -> bool {
    let dotted = format!("{scope}.{field}");
    let nested = format!("per_asset.{scope}.{field}");
    let pointer = format!("/per_asset/{scope}/{field}");
    missing_fields
        .iter()
        .any(|missing| matches!(missing.as_str(), value if value == dotted || value == nested || value == pointer))
}

fn phase5_number(
    value: Option<&Value>,
    scope: &str,
    field: &str,
    missing_fields: &[String],
) -> Result<f64> {
    if let Some(value) = value.and_then(Value::as_f64) {
        return Ok(value);
    }
    if phase5_field_is_missing(missing_fields, scope, field) {
        return Ok(0.0);
    }
    bail!("Phase 5 {scope}.{field} missing without missing_fields entry")
}

fn phase5_string(
    value: Option<&Value>,
    scope: &str,
    field: &str,
    missing_fields: &[String],
) -> Result<String> {
    if let Some(value) = value
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        return Ok(value.to_owned());
    }
    if phase5_field_is_missing(missing_fields, scope, field) {
        return Ok(String::new());
    }
    bail!("Phase 5 {scope}.{field} missing without missing_fields entry")
}

fn phase5_marginal_position_caps(state: &Value, ticker: &str) -> Result<BTreeMap<String, f64>> {
    let entries = state
        .pointer(&format!(
            "/risk_debate_state/reviewer_independence/per_asset/{ticker}/leave_one_reviewer_out"
        ))
        .and_then(Value::as_array)
        .with_context(|| {
            format!("Phase 6 requires the Rust Phase 5 leave-one-reviewer-out ledger for {ticker}")
        })?;
    let mut caps = BTreeMap::new();
    for entry in entries {
        if entry.get("marginal").and_then(Value::as_bool) != Some(true) {
            continue;
        }
        let index_id = entry
            .get("index_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .with_context(|| {
                format!("Phase 5 leave-one-reviewer-out ledger has invalid index_id for {ticker}")
            })?;
        let cap = entry
            .get("position_cap_pct")
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
            .with_context(|| {
                format!("Phase 5 leave-one-reviewer-out ledger has invalid cap for {ticker}")
            })?;
        caps.insert(index_id.to_owned(), cap);
    }
    Ok(caps)
}

fn enrich_and_validate_phase6_compiled_fields(
    state: &Value,
    fields: &mut serde_json::Map<String, Value>,
) -> Result<()> {
    inject_current_weights_into_phase6_fields(state, fields)?;
    let per_asset = fields
        .get_mut("per_asset")
        .context("Phase 6 Summary requires per_asset")?;
    enrich_final_trade_decision_fields(state, per_asset)?;

    let phase5_refs = state
        .pointer("/risk_debate_state/history")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|artifact| artifact.get("index_id").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();
    let risk_missing_fields = state
        .pointer("/risk_debate_state/history")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(|artifact| {
            artifact
                .pointer("/payload/missing_fields")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
        .filter_map(Value::as_str)
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();
    let investable = investable_assets_from_state(state);
    let decisions = per_asset
        .as_object_mut()
        .context("Phase 6 per_asset must be an object")?;
    for ticker in &investable {
        let decision = decisions
            .get_mut(ticker)
            .and_then(Value::as_object_mut)
            .with_context(|| format!("Phase 6 decision missing for {ticker}"))?;
        let trader_action = state
            .pointer(&format!(
                "/trader_investment_plan/per_ticker/{ticker}/action"
            ))
            .and_then(Value::as_str)
            .with_context(|| format!("Trader action missing for {ticker}"))?;
        let model_direction = decision
            .get("direction_constraint")
            .and_then(Value::as_str)
            .with_context(|| format!("Phase 6 direction_constraint missing for {ticker}"))?
            .to_owned();
        let projected_direction = match trader_action {
            "Buy" => "increase_only",
            "Sell" => "decrease_only",
            "Hold" => "unchanged",
            _ => bail!("unsupported Trader action {trader_action:?} for {ticker}"),
        };
        let current_weight = decision
            .get("current_weight")
            .and_then(Value::as_f64)
            .with_context(|| format!("Phase 6 current_weight missing for {ticker}"))?;
        let model_max_target_weight = decision
            .get("max_target_weight")
            .and_then(Value::as_f64)
            .with_context(|| format!("Phase 6 max_target_weight missing for {ticker}"))?;
        let model_max_weight_delta = decision
            .get("max_weight_delta")
            .and_then(Value::as_f64)
            .with_context(|| format!("Phase 6 max_weight_delta missing for {ticker}"))?;
        let projected_max_target_weight = match projected_direction {
            "increase_only" => model_max_target_weight.max(current_weight),
            "decrease_only" => model_max_target_weight.min(current_weight),
            "unchanged" => current_weight,
            _ => unreachable!("projected direction is exhaustive"),
        };
        let projected_max_weight_delta = if projected_direction == "unchanged" {
            0.0
        } else {
            model_max_weight_delta
        };
        decision.insert(
            "direction_constraint".to_owned(),
            json!(projected_direction),
        );
        decision.insert(
            "max_target_weight".to_owned(),
            json!(projected_max_target_weight),
        );
        decision.insert(
            "max_weight_delta".to_owned(),
            json!(projected_max_weight_delta),
        );
        if trader_action == "Hold"
            && decision.get("execution_status").and_then(Value::as_str) == Some("execute")
        {
            decision.insert("execution_status".to_owned(), json!("wait"));
        }
        decision.insert(
            "constraint_projection".to_owned(),
            json!({
                "authority": "rust",
                "trader_action": trader_action,
                "model_direction_constraint": model_direction,
                "projected_direction_constraint": projected_direction,
                "model_max_target_weight": model_max_target_weight,
                "projected_max_target_weight": projected_max_target_weight,
                "model_max_weight_delta": model_max_weight_delta,
                "projected_max_weight_delta": projected_max_weight_delta,
                "overridden": model_direction != projected_direction
                    || (model_max_target_weight - projected_max_target_weight).abs() > f64::EPSILON
                    || (model_max_weight_delta - projected_max_weight_delta).abs() > f64::EPSILON,
            }),
        );

        let marginal_position_caps = phase5_marginal_position_caps(state, ticker)?;
        let controls = decision
            .get_mut("binding_risk_controls")
            .and_then(Value::as_array_mut)
            .with_context(|| format!("Phase 6 binding_risk_controls missing for {ticker}"))?;
        let mut control_source_projections = Vec::with_capacity(controls.len());
        let mut binding_position_caps = Vec::new();
        for control in controls.iter_mut() {
            let object = control.as_object_mut().with_context(|| {
                format!("Phase 6 binding risk control must be an object for {ticker}")
            })?;
            let control_text = object
                .get("control")
                .and_then(Value::as_str)
                .filter(|text| !text.trim().is_empty())
                .with_context(|| format!("Phase 6 binding risk control text missing for {ticker}"))?
                .to_owned();
            let model_refs = object
                .get("source_refs")
                .and_then(Value::as_array)
                .with_context(|| {
                    format!("Phase 6 binding risk control source_refs missing for {ticker}")
                })?
                .iter()
                .map(|reference| {
                    reference
                        .as_str()
                        .map(str::trim)
                        .filter(|reference| !reference.is_empty())
                        .map(ToOwned::to_owned)
                        .with_context(|| {
                            format!(
                                "Phase 6 binding risk control source_refs must contain non-empty strings for {ticker}"
                            )
                        })
                })
                .collect::<Result<BTreeSet<_>>>()?;
            let referenced_phase5_refs = model_refs
                .intersection(&phase5_refs)
                .cloned()
                .collect::<Vec<_>>();
            if referenced_phase5_refs.is_empty() {
                bail!(
                    "Phase 6 binding risk control for {ticker} must reference at least one actual Phase 5 summary"
                );
            }
            let accepted_phase5_refs = referenced_phase5_refs
                .iter()
                .filter(|reference| marginal_position_caps.contains_key(*reference))
                .cloned()
                .collect::<Vec<_>>();
            if accepted_phase5_refs.is_empty() {
                bail!(
                    "Phase 6 binding risk control for {ticker} cites no marginal Phase 5 reviewer; correlated or no-new-information reviewers are report-only"
                );
            }
            if accepted_phase5_refs.len() != 1 {
                bail!(
                    "Phase 6 binding risk control for {ticker} must cite exactly one marginal Phase 5 reviewer rather than treating correlated reviewers as independent confirmation"
                );
            }
            let source_ref = accepted_phase5_refs
                .first()
                .expect("one accepted Phase 5 reference")
                .to_owned();
            let position_cap_pct = *marginal_position_caps
                .get(&source_ref)
                .expect("accepted source has a marginal cap");
            binding_position_caps.push((source_ref.clone(), position_cap_pct));
            let rejected_non_phase5_refs = model_refs
                .difference(&phase5_refs)
                .cloned()
                .collect::<Vec<_>>();
            let rejected_non_marginal_phase5_refs = referenced_phase5_refs
                .iter()
                .filter(|reference| !marginal_position_caps.contains_key(*reference))
                .cloned()
                .collect::<Vec<_>>();
            object.insert("source_refs".to_owned(), json!(accepted_phase5_refs));
            control_source_projections.push(json!({
                "control": control_text,
                "model_source_refs": model_refs.into_iter().collect::<Vec<_>>(),
                "accepted_phase5_source_refs": accepted_phase5_refs,
                "rejected_non_phase5_source_refs": rejected_non_phase5_refs,
                "rejected_non_marginal_phase5_source_refs": rejected_non_marginal_phase5_refs,
                "leave_one_reviewer_out_position_cap_pct": position_cap_pct,
            }));
        }
        let effective_position_cap_pct = binding_position_caps
            .iter()
            .map(|(_, cap)| *cap)
            .min_by(|left, right| left.total_cmp(right));
        let mut final_max_target_weight = projected_max_target_weight;
        let mut final_max_weight_delta = projected_max_weight_delta;
        let current_exposure_exceeds_risk_cap =
            effective_position_cap_pct.is_some_and(|cap| current_weight > cap + 0.000_001);
        if let Some(position_cap_pct) = effective_position_cap_pct {
            if current_exposure_exceeds_risk_cap {
                if projected_direction == "decrease_only" {
                    // The Trader already authorized a reduction.  Preserve
                    // that semantic direction and make the risk cap the
                    // executable target instead of silently turning an
                    // explicit de-risking plan into `wait`.
                    final_max_target_weight = final_max_target_weight.min(position_cap_pct);
                    final_max_weight_delta =
                        final_max_weight_delta.max((current_weight - position_cap_pct).max(0.0));
                } else {
                    // A Buy/Hold direction cannot secretly reverse to force
                    // a sale.  Record the conflict and wait at the current
                    // exposure until a later explicit decrease-only decision
                    // exists.
                    decision.insert("execution_status".to_owned(), json!("wait"));
                    final_max_target_weight = current_weight;
                }
            } else {
                final_max_target_weight = final_max_target_weight.min(position_cap_pct);
                if projected_direction == "decrease_only" {
                    final_max_weight_delta =
                        final_max_weight_delta.max((current_weight - position_cap_pct).max(0.0));
                }
            }
        }
        decision.insert(
            "max_target_weight".to_owned(),
            json!(final_max_target_weight),
        );
        decision.insert("max_weight_delta".to_owned(), json!(final_max_weight_delta));
        decision.insert(
            "risk_control_source_projection".to_owned(),
            json!({
                "authority": "rust_phase5_leave_one_reviewer_out_v1",
                "controls": control_source_projections,
                "marginal_position_cap_sources": marginal_position_caps,
                "binding_position_caps": binding_position_caps,
                "effective_position_cap_pct": effective_position_cap_pct,
                "current_exposure_exceeds_risk_cap": current_exposure_exceeds_risk_cap,
                "pre_risk_max_target_weight": projected_max_target_weight,
                "projected_max_target_weight": final_max_target_weight,
                "pre_risk_max_weight_delta": projected_max_weight_delta,
                "projected_max_weight_delta": final_max_weight_delta,
            }),
        );

        let blockers = decision
            .entry("unresolved_blockers".to_owned())
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .with_context(|| {
                format!("Phase 6 unresolved_blockers must be an array for {ticker}")
            })?;
        for missing in &risk_missing_fields {
            let belongs_to_other_ticker = investable
                .iter()
                .any(|asset| asset != ticker && missing.contains(asset));
            if (missing.contains(ticker) || !belongs_to_other_ticker)
                && !blockers
                    .iter()
                    .any(|existing| existing.as_str() == Some(missing))
            {
                blockers.push(Value::String(missing.clone()));
            }
        }
        if !blockers.is_empty()
            && decision.get("execution_status").and_then(Value::as_str) == Some("execute")
        {
            decision.insert("execution_status".to_owned(), json!("downgrade"));
        }

        let canonical = serde_json::from_value::<AssetExecutionConstraint>(json!({
            "direction_constraint": decision.get("direction_constraint"),
            "execution_status": decision.get("execution_status"),
            "current_weight": decision.get("current_weight"),
            "max_target_weight": decision.get("max_target_weight"),
            "max_weight_delta": decision.get("max_weight_delta"),
            "binding_risk_controls": decision.get("binding_risk_controls"),
        }))
        .with_context(|| format!("Phase 6 canonical execution constraint invalid for {ticker}"))?;
        validate_asset_execution_constraint(&canonical)
            .map_err(|error| anyhow::anyhow!("Phase 6 constraint invalid for {ticker}: {error}"))?;
    }
    Ok(())
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
    use orchestrator_core::evaluation::RiskDecision;
    use orchestrator_core::{
        AdjustmentPolicy, AllocationDecision, AllocationOutcome, BenchmarkBindingV1,
        BenchmarkOutcome, BenchmarkSelectionV1, DecisionSection, DecisionSectionUnavailableReason,
        DecisionSnapshotV2, DocumentRef, EvaluationSpec, ExecutionOutcome, ExecutionPlan,
        ExecutionPlanStatus, ForecastDirection, MarketOutcome, MemoryPolicyV1,
        MemoryUsageReferenceStatus, OutcomeRecordV1, OutcomeSection,
        OutcomeSectionUnavailableReason, PersistenceContextV1, PersistenceNamespace, PolicyRef,
        PriceBasis, PricePoint, RunPurpose, ThesisDecision, TradeAction, TradeDecision,
    };
    use orchestrator_store::{
        append_index_detail, capture_run_inputs, content_hash, create_index, finalize_index,
        read_run_manifest, write_input_payload, write_run_manifest, AppendIndexDetailInput,
        CreateIndexInput, DetailSection, FileStore, FileStoreOptions, IndexKind, IndexScope,
        InputSource, PhaseStatus, RunLocation, RunManifest, RunManifestInit,
    };
    use serde_json::{json, Value};
    use std::collections::{BTreeMap, BTreeSet};
    use tempfile::tempdir;

    use crate::orchestration::config::RuntimeConfig;
    use crate::orchestration::summary_store::PhaseIndexCandidateDetail;

    use super::{
        analyst_calibration_multiplier, apply_phase2_stree_command,
        attach_verified_phase1_web_sources, attach_verified_web_evidence,
        canonicalize_phase1_cross_phase_source,
        canonicalize_phase2_topic_generation_summary_source, controller_should_continue,
        decision_snapshot, defers_phase_summary, enrich_and_validate_phase6_compiled_fields,
        enrich_compiled_fields, enrich_final_trade_decision_fields, ensure_initial_collision_route,
        final_decision_payload, finalized_phase_index, finish_phase, highest_completed_phase,
        is_cacheable_unit, load_or_initialize_state, merge_parallel_state_delta,
        normalize_phase1_summary_layout, normalize_phase2_topic_control_fields,
        normalize_phase2_topic_generation_evidence_refs, persist_state, persists_phase_index,
        phase1_evidence_event_ledger, phase2_controller_close_retry_injection,
        phase2_debate_debug_summary, phase2_initial_evidence_registry, phase2_stree_dispatch_key,
        phase2_stree_terminal_command_present, phase2_terminal_tool_retry_injection,
        phase3_scenario_validation_retry_instruction, phase5_reviewer_independence_ledger,
        phase7_execution_mode, phase_completed, phase_from_failure_message, prepare_manifest,
        preserve_phase1_summary_key_evidence, project_detail_hash_source_refs_object,
        project_phase2_final_fields, project_phase3_evidence_refs,
        project_topic_generation_selection, prompt_owner_for_unit, prune_unbacked_phase1_findings,
        record_phase2_runtime_failure, record_phase2_session, record_run_failure,
        redacted_config_for_state, reflection_learning_gap_reasons, resolve_git_sha,
        risk_decision_from_index, runtime_session_key, scoped_state_for_unit, select_phase2_topics,
        select_reflection_task_budget, sync_manifest_health, unselected_phase2_candidates,
        validate_declared_detail_source_refs, validate_phase1_compiled_fields,
        validate_phase1_web_source_urls, validate_phase2_compiled_contract,
        validate_phase2_topic_ttls, validate_phase3_compiled_fields,
        validate_phase4_compiled_fields, validate_phase5_compiled_fields,
        weighted_probability_base, workflow_source_surface_hash_at, DebateActor,
        Phase1ReferenceNormalization, Phase7ExecutionMode, RunFailureContext, TopicDebateTree,
    };

    #[test]
    fn persisted_state_is_sealed_and_round_trippable() {
        let directory = tempdir().unwrap();
        let mut state = json!({
            "schema_version": 1,
            "run_id": "run-a",
            "current_date": "2026-08-01",
            "ticker": "QQQ,SOXX,VIX",
            "tickers": ["QQQ", "SOXX", "VIX"],
            "analysis_universe": ["QQQ", "SOXX", "VIX"],
            "store_root": directory.path(),
            "storage_namespace": "debug",
            "computed_float": 0.37939999999999996,
            "phase_status": {}
        });

        persist_state(&mut state).unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = RunLocation::debug("2026-08-01", "run-a").unwrap();
        let persisted = store.read_json_value(&location.state_relative()).unwrap();
        let stored_hash = persisted["content_hash"].as_str().unwrap();
        let mut without_hash = persisted.clone();
        without_hash["content_hash"] = Value::String(String::new());
        assert_eq!(stored_hash, content_hash(&without_hash).unwrap());

        state["phase_status"]["0"] = json!("completed");
        persist_state(&mut state).unwrap();
        assert!(load_or_initialize_state(
            &store,
            &location,
            json!({
                "storage_namespace": "debug",
                "run_id": "run-a",
                "ticker": "QQQ,SOXX,VIX",
                "tickers": ["QQQ", "SOXX", "VIX"]
            })
        )
        .is_ok());
    }

    #[test]
    fn persisted_state_config_redacts_credentials_but_keeps_runtime_shape() {
        let config = json!({
            "orchestrator": {
                "llm": {"api_key": "secret-key", "max_completion_tokens": 8192},
                "alpaca": {"api_secret": "secret-value"}
            },
            "report": {"email": {"password": "smtp-secret"}}
        });
        let redacted = redacted_config_for_state(&config);
        assert_eq!(redacted["orchestrator"]["llm"]["api_key"], "[redacted]");
        assert_eq!(
            redacted["orchestrator"]["llm"]["max_completion_tokens"],
            8192
        );
        assert_eq!(
            redacted["orchestrator"]["alpaca"]["api_secret"],
            "[redacted]"
        );
        assert_eq!(redacted["report"]["email"]["password"], "[redacted]");
    }

    fn snapshot_test_index(
        store: &FileStore,
        location: &RunLocation,
        phase: u8,
        role: &str,
        index_id: &str,
        fields: Value,
    ) {
        let source_payload_hash = content_hash(&fields).unwrap();
        let scope = IndexScope {
            kind: IndexKind::PhaseSummary,
            location: Some(location.clone()),
            index_id: index_id.to_owned(),
            run_id: location.run_id.clone(),
            source_run_id: None,
            source_phase: phase,
            role: role.to_owned(),
            ticker: None,
            topic_id: None,
            source_payload_hash,
            authoritative_fields: fields.as_object().unwrap().clone(),
            created_at: "2026-08-03T00:00:00Z".to_owned(),
        };
        create_index(
            store,
            CreateIndexInput {
                scope: scope.clone(),
                summary: format!("Phase {phase} {role}"),
                confidence: 1.0,
                pattern_key: None,
                applies_to_phases: Vec::new(),
            },
        )
        .unwrap();
        append_index_detail(
            store,
            AppendIndexDetailInput {
                scope: scope.clone(),
                section: DetailSection::Other,
                detail: format!("sealed Phase {phase} fixture"),
                source_refs: Vec::new(),
            },
        )
        .unwrap();
        finalize_index(store, &scope).unwrap();
    }

    fn seed_snapshot_phase_indexes(store: &FileStore, location: &RunLocation) {
        snapshot_test_index(
            store,
            location,
            1,
            "analyst.technical",
            "idx-snapshot-phase1",
            json!({"per_ticker": {"QQQ": {"report": "sealed evidence"}}}),
        );
        snapshot_test_index(
            store,
            location,
            2,
            "mediator.topic_controller",
            "idx-snapshot-phase2",
            json!({"topics": []}),
        );
        snapshot_test_index(
            store,
            location,
            3,
            "manager.research",
            "idx-snapshot-phase3",
            json!({"decisions": {"QQQ": {
                "rating": "Buy",
                "long_probability": 0.65,
                "validation_plan": ["invalidate if the breakout fails"]
            }}}),
        );
        snapshot_test_index(
            store,
            location,
            4,
            "trader",
            "idx-snapshot-phase4",
            json!({"plans": {"QQQ": {
                "action": "Buy",
                "execution_conditions": ["daily breakout holds", "volume confirms"],
                "position_size_pct_max": 0.25,
                "blockers": ["await confirmation"]
            }}}),
        );
        snapshot_test_index(
            store,
            location,
            5,
            "risk.neutral",
            "idx-snapshot-phase5",
            json!({"per_asset": {"QQQ": {"stance": "neutral"}}}),
        );
        snapshot_test_index(
            store,
            location,
            6,
            "portfolio.manager",
            "idx-snapshot-phase6",
            json!({"per_asset": {"QQQ": {
                "direction_constraint": "increase_only",
                "max_target_weight": 0.30,
                "max_weight_delta": 0.20,
                "current_weight": 0.10,
                "execution_status": "execute",
                "binding_risk_controls": [{
                    "control": "do not exceed the portfolio cap",
                    "source_refs": ["idx-snapshot-phase5"]
                }]
            }}}),
        );
        snapshot_test_index(
            store,
            location,
            7,
            "rust.allocation",
            "idx-snapshot-phase7",
            json!({
                "allocation": {"weights": {
                    "QQQ": {"weight": 0.30},
                    "cash_hedge": {"weight": 0.70}
                }},
                "order_plan": {"orders": [{"symbol": "QQQ", "side": "buy"}]}
            }),
        );
    }

    fn snapshot_test_runtime() -> RuntimeConfig {
        RuntimeConfig::from_value(&json!({
            "orchestrator": {
                "llm": {
                    "defaults": {
                        "route": "chat_completions",
                        "model": "fixture-model",
                        "base_url": "https://fixture.invalid/v1",
                        "api_key": "fixture-key"
                    }
                },
                "evaluation": {
                    "enabled": true,
                    "canonical_memory_writes_enabled": true,
                    "policy_version": 1,
                    "evaluation_contract_id": "snapshot-test",
                    "prediction_horizon_trading_days": 3
                }
            }
        }))
        .unwrap()
    }

    fn snapshot_test_context(run_id: &str) -> PersistenceContextV1 {
        PersistenceContextV1 {
            run_purpose: RunPurpose::Paper,
            namespace: PersistenceNamespace::Canonical,
            canonical_memory_writes_enabled: true,
            invocation_id: run_id.to_owned(),
            config_ref: PolicyRef {
                policy_id: "snapshot-test".to_owned(),
                version: 1,
                content_hash: "sha256:snapshot-test".to_owned(),
            },
            source_store_fingerprint: "snapshot-test-store".to_owned(),
        }
    }

    #[test]
    fn decision_snapshot_preserves_sealed_phase1_to_phase7_lineage() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = RunLocation::new("2026-08-03", "snapshot-lineage").unwrap();
        seed_snapshot_phase_indexes(&store, &location);
        let source = InputSource::technical("QQQ", "daily").unwrap();
        write_input_payload(
            &store,
            source.clone(),
            b"date,close\n2026-08-03,100\n",
            "2026-08-03T00:00:00Z",
        )
        .unwrap();
        capture_run_inputs(&store, &location, &[source], "2026-08-03T00:00:00Z").unwrap();

        let decision = decision_snapshot(
            &store,
            &snapshot_test_runtime(),
            &location,
            "QQQ",
            &snapshot_test_context(&location.run_id),
            MemoryUsageReferenceStatus::NotCaptured,
        )
        .unwrap();

        let DecisionSection::Available { value: thesis } = decision.thesis else {
            panic!("Phase 3 thesis should be available from its finalized Index");
        };
        assert_eq!(thesis.probability, 0.65);
        assert_eq!(thesis.horizon, "3 trading days");
        assert_eq!(
            thesis.invalidation_conditions,
            ["invalidate if the breakout fails"]
        );
        let DecisionSection::Available { value: trade } = decision.trade else {
            panic!("Phase 4 trade should be available from its finalized Index");
        };
        assert_eq!(
            trade.entry_condition.as_deref(),
            Some("daily breakout holds; volume confirms")
        );
        assert_eq!(trade.position_size_ceiling, Some(0.25));
        let DecisionSection::Available { value: risk } = decision.risk else {
            panic!("Phase 5/6 risk should be available from their finalized Indexes");
        };
        assert!(risk
            .artifact_refs
            .iter()
            .any(|reference| reference.document_id == "idx-snapshot-phase5"));
        let DecisionSection::Available { value: allocation } = decision.allocation else {
            panic!("Phase 7 allocation should be available from its finalized Index");
        };
        assert_eq!(allocation.current_weight, Some(0.10));
        assert_eq!(allocation.target_weight, Some(0.30));
        let DecisionSection::Available { value: execution } = decision.execution_plan else {
            panic!("Phase 7 execution plan should be available from its finalized Index");
        };
        assert_eq!(execution.status, ExecutionPlanStatus::Execute);
        assert!(execution.attributable_execution_expected);
        assert_eq!(execution.order_intent_refs.len(), 1);
        assert_eq!(decision.source_artifact_refs.len(), 7);
        assert_eq!(decision.source_input_refs.len(), 2);
    }

    #[test]
    fn risk_snapshot_preserves_an_explicit_no_marginal_reviewer_outcome() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = RunLocation::new("2026-08-03", "snapshot-no-marginal-risk").unwrap();
        seed_snapshot_phase_indexes(&store, &location);
        let (mut phase6, phase6_ref) =
            finalized_phase_index(&store, &location, 6, "portfolio.manager")
                .unwrap()
                .unwrap();
        phase6.authoritative_fields["per_asset"]["QQQ"]["binding_risk_controls"] = json!([]);

        let risk =
            risk_decision_from_index(&phase6, phase6_ref.clone(), "QQQ", &BTreeMap::new()).unwrap();

        assert!(risk.binding_controls.is_empty());
        assert_eq!(risk.artifact_refs, vec![phase6_ref]);
    }

    #[test]
    fn decision_snapshot_marks_thesis_as_upstream_gap_when_input_manifest_is_missing() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = RunLocation::new("2026-08-03", "snapshot-input-gap").unwrap();
        seed_snapshot_phase_indexes(&store, &location);

        let decision = decision_snapshot(
            &store,
            &snapshot_test_runtime(),
            &location,
            "QQQ",
            &snapshot_test_context(&location.run_id),
            MemoryUsageReferenceStatus::NotCaptured,
        )
        .unwrap();

        assert!(matches!(
            decision.thesis,
            DecisionSection::Unavailable {
                reason: DecisionSectionUnavailableReason::UpstreamDataGap,
                ..
            }
        ));
        assert!(matches!(decision.trade, DecisionSection::Available { .. }));
        assert!(matches!(decision.risk, DecisionSection::Available { .. }));
        assert!(decision.source_input_refs.is_empty());
    }

    #[test]
    fn debug_manifest_refuses_config_or_prompt_identity_drift() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = RunLocation::debug("2026-08-03", "snapshot-debug-identity").unwrap();
        let runtime = snapshot_test_runtime();
        let config = json!({"orchestrator": {"identity_fixture": "first"}});
        let manifest = prepare_manifest(&store, &location, &runtime, &config).unwrap();
        assert!(!manifest.prompt_content_hash.is_empty());

        let config_error = prepare_manifest(
            &store,
            &location,
            &runtime,
            &json!({"orchestrator": {"identity_fixture": "changed"}}),
        )
        .unwrap_err();
        assert!(config_error.to_string().contains("config_hash"));

        let mut tampered = manifest;
        tampered.prompt_content_hash = "sha256:old-prompt-surface".to_owned();
        write_run_manifest(&store, &location, tampered).unwrap();
        let prompt_error = prepare_manifest(&store, &location, &runtime, &config).unwrap_err();
        assert!(prompt_error.to_string().contains("prompt_content_hash"));
    }

    #[test]
    fn source_surface_hash_pins_dirty_workspace_code_but_ignores_outputs() {
        let directory = tempdir().unwrap();
        std::fs::create_dir_all(directory.path().join("crates/example/src")).unwrap();
        std::fs::create_dir_all(directory.path().join("outputs/debug")).unwrap();
        std::fs::write(
            directory.path().join("Cargo.toml"),
            "[workspace]\nmembers = [\"crates/example\"]\n",
        )
        .unwrap();
        std::fs::write(
            directory.path().join("crates/example/Cargo.toml"),
            "[package]\nname = \"example\"\nversion = \"0.1.0\"\n",
        )
        .unwrap();
        let source = directory.path().join("crates/example/src/lib.rs");
        std::fs::write(&source, "pub fn value() -> u8 { 1 }\n").unwrap();

        let initial = workflow_source_surface_hash_at(directory.path()).unwrap();
        std::fs::write(&source, "pub fn value() -> u8 { 2 }\n").unwrap();
        let dirty_source = workflow_source_surface_hash_at(directory.path()).unwrap();
        std::fs::write(
            directory.path().join("outputs/debug/ignored.json"),
            "not executable source",
        )
        .unwrap();

        assert_ne!(initial, dirty_source);
        assert_eq!(
            dirty_source,
            workflow_source_surface_hash_at(directory.path()).unwrap()
        );
    }

    #[test]
    fn final_trade_decision_carries_probability_and_execution_conditions() {
        let state = json!({
            "research_plan": {"per_ticker": {
                "QQQ": {"long_probability": 0.46, "short_probability": 0.54}
            }},
            "trader_investment_plan": {"per_ticker": {
                "QQQ": {
                    "action": "Sell",
                    "execution_conditions": ["10Y falls"],
                    "entry_price": null,
                    "stop_loss": null,
                    "downgrade_reason": "invalidate on synchronized reversal",
                    "blockers": ["missing price anchor"]
                }
            }}
        });
        let mut per_asset = json!({"QQQ": {"rating": "Sell"}});

        enrich_final_trade_decision_fields(&state, &mut per_asset).unwrap();

        assert_eq!(per_asset["QQQ"]["long_probability"], 0.46);
        assert_eq!(per_asset["QQQ"]["short_probability"], 0.54);
        assert_eq!(per_asset["QQQ"]["inherited_probability"], 0.46);
        assert_eq!(per_asset["QQQ"]["direction"], "Sell");
        assert_eq!(per_asset["QQQ"]["entry_conditions"], json!(["10Y falls"]));
        assert_eq!(per_asset["QQQ"]["entry_price"], Value::Null);
        assert_eq!(per_asset["QQQ"]["stop_loss"], Value::Null);
        assert_eq!(
            per_asset["QQQ"]["invalidation_conditions"]["downgrade_reason"],
            "invalidate on synchronized reversal"
        );
        assert_eq!(
            per_asset["QQQ"]["invalidation_conditions"]["blockers"],
            json!(["missing price anchor"])
        );
    }

    #[test]
    fn final_decision_index_payload_keeps_the_report_projection() {
        let state = json!({
            "current_date": "2026-08-03",
            "ticker": "QQQ",
            "investable_assets": ["QQQ"],
            "research_plan": {"per_ticker": {"QQQ": {"long_probability": 0.7}}},
            "trader_investment_plan": {"per_ticker": {"QQQ": {"action": "Buy"}}},
            "risk_debate_state": {"history": [{"payload": {"argument": "risk"}}]},
            "final_trade_decision": {"per_asset": {"QQQ": {"direction": "Buy"}}},
            "portfolio_allocation": {"weights": {"QQQ": {"weight": 0.2}}},
            "debate_state_artifact": {"status": "completed"}
        });

        let payload = final_decision_payload(&state, &Default::default());

        assert_eq!(
            payload["report_projection"]["research_plan"]["per_ticker"]["QQQ"]["long_probability"],
            0.7
        );
        assert_eq!(
            payload["report_projection"]["final_trade_decision"]["per_asset"]["QQQ"]["direction"],
            "Buy"
        );
    }

    fn phase1_test_evidence(reference: &str, source: &str) -> Value {
        json!({
            "claim": "Fresh source-backed observation.",
            "evidence_type": "fact",
            "source": source,
            "timestamp": "2026-08-03T00:00:00Z",
            "source_tier": "unknown",
            "first_source": source,
            "is_derivative_repost": false,
            "evidence_age": "0-2d",
            "source_confidence": 0.9,
            "evidence_refs": [reference]
        })
    }

    #[test]
    fn weighted_probability_base_uses_explicit_probabilities_and_evidence_lineage_weights() {
        let technical_qqq = phase1_test_evidence("technical-qqq", "technical");
        let technical_soxx = phase1_test_evidence("technical-soxx", "technical");
        let news_qqq = phase1_test_evidence("jin10-qqq", "jin10-qqq");
        let news_soxx = phase1_test_evidence("jin10-soxx", "jin10-soxx");
        let state = json!({
            "current_date": "2026-08-03",
            "investable_assets": ["QQQ", "SOXX"],
            "analyst_reports": {
                "analyst.technical": {"per_ticker": {
                    "QQQ": {"direction": "bearish", "confidence": 0.90, "long_probability": 0.40, "key_evidence": [technical_qqq]},
                    "SOXX": {"direction": "bearish", "confidence": 0.20, "long_probability": 0.30, "key_evidence": [technical_soxx]}
                }},
                "analyst.news_macro": {"per_ticker": {
                    "QQQ": {"direction": "mixed", "confidence": 0.10, "long_probability": 0.60, "key_evidence": [news_qqq]},
                    "SOXX": {"direction": "mixed", "confidence": 0.80, "long_probability": 0.55, "key_evidence": [news_soxx]}
                }}
            }
        });

        let base = weighted_probability_base(&state).unwrap();

        assert_eq!(base["QQQ"]["uncalibrated_long_probability"], 0.42);
        assert_eq!(base["QQQ"]["long_probability"], 0.452);
        assert_eq!(base["QQQ"]["short_probability"], 0.548);
        assert_eq!(base["SOXX"]["long_probability"], 0.50);
        assert_eq!(base["SOXX"]["short_probability"], 0.50);
        assert_eq!(base["QQQ"]["source"], "phase1_explicit_long_probability_v3");
        assert_eq!(
            base["QQQ"]["weighting"],
            "evidence_lineage_freshness_independence_weighted_mean"
        );
        assert_eq!(
            base["QQQ"]["contributions"][0]["analyst_long_probability"],
            0.6
        );
        assert_eq!(
            base["QQQ"]["contributions"][0]["evidence_assessment"]["records"][0]
                ["freshness_weight"],
            1.0
        );
    }

    #[test]
    fn weighted_probability_base_keeps_verified_technical_input_when_source_confidence_is_zero() {
        let technical = json!({
            "claim": "The sealed technical snapshot contains a current signal.",
            "evidence_type": "fact",
            "source": "filestore.run_input.technical",
            "timestamp": "2026-08-03T00:00:00Z",
            "source_tier": "unknown",
            "first_source": "filestore.run_input.technical",
            "is_derivative_repost": false,
            "evidence_age": "0-2d",
            "source_confidence": 0.0,
            "evidence_refs": [format!("technical-{}", "a".repeat(64))]
        });
        let state = json!({
            "current_date": "2026-08-03",
            "investable_assets": ["QQQ"],
            "analyst_reports": {
                "analyst.technical": {"per_ticker": {
                    "QQQ": {
                        "direction": "mixed",
                        "confidence": 0.55,
                        "long_probability": 0.52,
                        "key_evidence": [technical]
                    }
                }}
            }
        });

        let base = weighted_probability_base(&state).unwrap();

        assert_eq!(base["QQQ"]["contributions"].as_array().unwrap().len(), 1);
        assert_eq!(
            base["QQQ"]["contributions"][0]["evidence_assessment"]["records"][0]
                ["source_confidence"],
            0.5
        );
    }

    #[test]
    fn phase1_summary_retry_preserves_prior_key_evidence_when_model_omits_it() {
        let evidence = json!({
            "claim": "The sealed VIX snapshot records a current observation.",
            "evidence_type": "fact",
            "source": "filestore.run_input.technical",
            "timestamp": "2026-08-03T07:00:00Z",
            "source_tier": "unknown",
            "first_source": "filestore.run_input.technical",
            "is_derivative_repost": false,
            "evidence_age": "0-2d",
            "source_confidence": 0.8,
            "evidence_refs": [format!("technical-{}", "a".repeat(64))]
        });
        let previous = json!({
            "per_ticker": {
                "VIX": {"key_evidence": [evidence]}
            }
        });
        let mut current = json!({
            "per_ticker": {
                "QQQ": {"key_evidence": [{"claim": "keep current QQQ evidence"}]},
                "VIX": {"key_evidence": []}
            }
        });

        preserve_phase1_summary_key_evidence(current.as_object_mut().unwrap(), &previous);

        assert_eq!(
            current["per_ticker"]["VIX"]["key_evidence"][0]["evidence_refs"][0],
            "technical-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(
            current["per_ticker"]["QQQ"]["key_evidence"][0]["claim"],
            "keep current QQQ evidence"
        );
    }

    #[test]
    fn analyst_calibration_only_accepts_canonical_outcome_brier_records() {
        let trusted = json!({
            "analyst_calibration": {
                "analyst.technical": {
                    "QQQ": {
                        "authority": "rust_canonical_outcome_brier_v1",
                        "status": "available",
                        "sample_size": 20,
                        "reliability": 0.9
                    }
                }
            }
        });
        assert_eq!(
            analyst_calibration_multiplier(&trusted, "analyst.technical", "QQQ")["multiplier"],
            0.95
        );

        let untrusted = json!({
            "analyst_calibration": {
                "analyst.technical": {
                    "QQQ": {
                        "authority": "model_claim",
                        "status": "available",
                        "sample_size": 100,
                        "reliability": 1.0
                    }
                }
            }
        });
        let bootstrap = analyst_calibration_multiplier(&untrusted, "analyst.technical", "QQQ");
        assert_eq!(bootstrap["authority"], "bootstrap_uncalibrated_discount");
        assert_eq!(bootstrap["multiplier"], 0.8);
        assert_eq!(bootstrap["rejected_calibration_authority"], "model_claim");
    }

    #[test]
    fn weighted_probability_base_excludes_unobserved_analysts() {
        let news_qqq = phase1_test_evidence("jin10-qqq", "jin10-qqq");
        let state = json!({
            "current_date": "2026-08-03",
            "investable_assets": ["QQQ"],
            "analyst_reports": {
                "analyst.technical": {"per_ticker": {
                    "QQQ": {"direction": "unobserved", "confidence": 0.9, "long_probability": 0.5}
                }},
                "analyst.news_macro": {"per_ticker": {
                    "QQQ": {"direction": "bullish", "confidence": 0.4, "long_probability": 0.7, "key_evidence": [news_qqq]}
                }}
            }
        });

        let base = weighted_probability_base(&state).unwrap();

        assert_eq!(base["QQQ"]["uncalibrated_long_probability"], 0.7);
        assert_eq!(base["QQQ"]["long_probability"], 0.62);
        assert_eq!(
            base["QQQ"]["excluded_contributions"][0]["reason"],
            "unobserved_does_not_contribute_to_probability_base"
        );
    }

    #[test]
    fn weighted_probability_base_records_freshness_correlation_and_cross_role_event_dedup() {
        let duplicate_event = |reference: &str| {
            json!({
                "claim": "A shared policy release.",
                "evidence_type": "fact",
                "source": "https://example.test/release",
                "timestamp": "2026-08-03T08:00:00Z",
                "event_time": "2026-08-03T08:00:00Z",
                "published_time": "2026-08-03T08:00:00Z",
                "ingested_time": "2026-08-03T08:05:00Z",
                "as_of": "2026-08-03T08:00:00Z",
                "timezone": "UTC",
                "source_tier": "official",
                "first_source": "Federal Reserve",
                "is_derivative_repost": false,
                "evidence_age": "0-2d",
                "source_confidence": 0.9,
                "evidence_refs": [reference]
            })
        };
        let technical_evidence = vec![
            json!({
                "claim": "A stale technical indicator.",
                "evidence_type": "inference",
                "source": "technical",
                "timestamp": "2026-07-01T00:00:00Z",
                "source_tier": "unknown",
                "first_source": "technical",
                "is_derivative_repost": false,
                "evidence_age": "0-2d",
                "source_confidence": 0.9,
                "evidence_refs": ["technical-stale"]
            }),
            json!({
                "claim": "A related technical indicator.",
                "evidence_type": "fact",
                "source": "technical",
                "timestamp": "2026-08-03T01:00:00Z",
                "source_tier": "unknown",
                "first_source": "technical",
                "is_derivative_repost": false,
                "evidence_age": "0-2d",
                "source_confidence": 0.9,
                "evidence_refs": ["technical-one"]
            }),
            json!({
                "claim": "Another related technical indicator.",
                "evidence_type": "fact",
                "source": "technical",
                "timestamp": "2026-08-03T02:00:00Z",
                "source_tier": "unknown",
                "first_source": "technical",
                "is_derivative_repost": false,
                "evidence_age": "0-2d",
                "source_confidence": 0.9,
                "evidence_refs": ["technical-two"]
            }),
        ];
        let state = json!({
            "current_date": "2026-08-03",
            "investable_assets": ["QQQ"],
            "analyst_reports": {
                "analyst.technical": {"per_ticker": {
                    "QQQ": {"direction": "bearish", "confidence": 0.8, "long_probability": 0.2, "key_evidence": technical_evidence}
                }},
                "analyst.news_macro": {"per_ticker": {
                    "QQQ": {"direction": "bullish", "confidence": 0.8, "long_probability": 0.8, "key_evidence": [duplicate_event("jin10-1")]}
                }},
                "analyst.alt_news": {"per_ticker": {
                    "QQQ": {"direction": "bullish", "confidence": 0.8, "long_probability": 0.8, "key_evidence": [duplicate_event("web-1")]}
                }}
            }
        });

        let base = weighted_probability_base(&state).unwrap();
        let contributions = base["QQQ"]["contributions"].as_array().unwrap();
        let technical = contributions
            .iter()
            .find(|item| item["role"] == "analyst.technical")
            .unwrap();
        let macro_news = contributions
            .iter()
            .find(|item| item["role"] == "analyst.news_macro")
            .unwrap();

        assert_eq!(
            technical["evidence_assessment"]["records"][0]["freshness_weight"],
            0.1
        );
        assert_eq!(
            technical["evidence_assessment"]["records"][0]["evidence_type"],
            "inference"
        );
        assert_eq!(
            technical["evidence_assessment"]["correlation_discount"],
            0.57735
        );
        assert_eq!(
            macro_news["evidence_assessment"]["duplicate_event_weight"],
            0.5
        );
        let ledger = phase1_evidence_event_ledger(&state).unwrap();
        assert_eq!(ledger["duplicate_event_count"], 1);
        assert_eq!(ledger["events"].as_object().unwrap().len(), 4);
    }

    #[test]
    fn phase2_topic_budget_is_deterministic_and_audited() {
        let generated = json!([
            {"topic_id": "topic-a"},
            {"topic_id": "topic-b"},
            {"topic_id": "topic-c"}
        ]);

        let (selected, audit) = select_phase2_topics(generated, 2).unwrap();

        assert_eq!(
            selected,
            json!([{"topic_id": "topic-a"}, {"topic_id": "topic-b"}])
        );
        assert_eq!(audit["authority"], "rust");
        assert_eq!(audit["generated_count"], 3);
        assert_eq!(audit["selected_count"], 2);
        assert_eq!(audit["truncated_count"], 1);
        assert_eq!(audit["max_topics_per_side"], 2);
    }

    #[test]
    fn phase2_topic_selection_rejects_duplicate_topic_ids() {
        let error = select_phase2_topics(
            json!([
                {"topic_id": "topic-a"},
                {"topic_id": "topic-a"}
            ]),
            2,
        )
        .unwrap_err();

        assert!(error.to_string().contains("duplicate topic_id"));
    }

    #[test]
    fn topic_generator_contract_preserves_unselected_candidates_as_residual_risk() {
        let selected = json!({
            "topic_id": "topic-trend",
            "topic": "Does the trend persist?",
            "tickers": ["QQQ"],
            "meta_factor": "trend",
            "decision_hinge": "breakout confirmation",
            "ttl": "1-3d",
            "why_debate": "trend and macro disagree",
            "evidence_refs": [
                "idx-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "idx-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "idx-cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "idx-dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
            ]
        });
        let unselected = json!({
            "topic_id": "topic-macro",
            "topic": "Does the macro event reprice duration?",
            "tickers": ["QQQ"],
            "meta_factor": "macro",
            "decision_hinge": "yield repricing",
            "ttl": "1-3d",
            "why_debate": "event timing is unresolved",
            "evidence_refs": ["idx-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]
        });
        let mut fields = json!({
            "coverage": [
                {"category":"trend","status":"selected","reason":"highest decision impact","evidence_refs":["idx-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],"topic_id":"topic-trend"},
                {"category":"valuation_expectations","status":"not_present","reason":"no valuation evidence in this run","evidence_refs":[]},
                {"category":"macro","status":"candidate_only","reason":"bounded topic queue deferred the macro hinge","evidence_refs":["idx-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"],"topic_id":"topic-macro"},
                {"category":"event_risk","status":"not_present","reason":"no distinct event risk","evidence_refs":[]},
                {"category":"data_quality","status":"not_present","reason":"no data quality gap","evidence_refs":[]}
            ],
            "candidate_topics": [selected.clone(), unselected],
            "topics": [selected],
            "residual_risks": [{
                "category":"macro",
                "topic_id":"topic-macro",
                "reason":"must remain visible to Research Manager despite the topic cap",
                "evidence_refs":["idx-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]
            }]
        })
        .as_object()
        .unwrap()
        .clone();

        validate_phase2_topic_ttls(&fields).unwrap();
        validate_phase2_compiled_contract("topic_generation", &fields, &[]).unwrap();

        fields.insert(
            "residual_risks".to_owned(),
            json!([{
                "category":"candidate_only",
                "reason":"the macro coverage status remains visible without duplicating its candidate record",
                "evidence_refs":["idx-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]
            }]),
        );
        validate_phase2_compiled_contract("topic_generation", &fields, &[]).unwrap();

        fields.insert("residual_risks".to_owned(), json!([]));
        let error =
            validate_phase2_compiled_contract("topic_generation", &fields, &[]).unwrap_err();
        assert!(error.to_string().contains("residual_risks"));
    }

    #[test]
    fn unselected_phase2_candidates_are_retained_as_rust_owned_search_space() {
        let candidates = json!([
            {"topic_id":"topic-selected", "decision_hinge":"selected"},
            {"topic_id":"topic-unselected", "decision_hinge":"unselected"}
        ]);
        let selected = json!([{"topic_id":"topic-selected", "decision_hinge":"selected"}]);

        assert_eq!(
            unselected_phase2_candidates(&candidates, &selected).unwrap(),
            json!([{"topic_id":"topic-unselected", "decision_hinge":"unselected"}])
        );
    }

    #[test]
    fn topic_generation_projects_an_omitted_residual_from_its_existing_coverage_row() {
        let event_ref = "idx-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut fields = json!({
            "coverage": [
                {"category":"trend","status":"selected","reason":"selected trend","evidence_refs":[event_ref]},
                {"category":"valuation_expectations","status":"selected","reason":"selected valuation","evidence_refs":[event_ref]},
                {"category":"macro","status":"selected","reason":"selected macro","evidence_refs":[event_ref]},
                {"category":"event_risk","status":"residual_risk","reason":"event sequence is unresolved","evidence_refs":[event_ref]},
                {"category":"data_quality","status":"not_present","reason":"no gap","evidence_refs":[]}
            ],
            "residual_risks": []
        })
        .as_object()
        .unwrap()
        .clone();

        super::project_topic_generation_residual_coverage(&mut fields).unwrap();

        assert_eq!(fields["residual_risks"].as_array().unwrap().len(), 1);
        assert_eq!(fields["residual_risks"][0]["category"], "event_risk");
        assert_eq!(
            fields["residual_risks"][0]["reason"],
            "event sequence is unresolved"
        );
        assert_eq!(
            fields["residual_risks"][0]["coverage_projection"]["authority"],
            "rust_phase2_coverage_projection_v1"
        );
    }

    #[test]
    fn finalized_detail_hashes_project_only_to_their_verified_parent_index() {
        let detail_hash = format!("sha256:{}", "a".repeat(64));
        let unknown_hash = format!("sha256:{}", "b".repeat(64));
        let index_id = format!("idx-{}", "c".repeat(64));
        let stable_evidence = format!("jin10-{}", "d".repeat(64));
        let mut fields = json!({
            "common_ground": {"evidence_refs": [detail_hash, stable_evidence]},
            "unresolved": {"evidence_refs": [unknown_hash]},
            "free_text": detail_hash
        })
        .as_object()
        .unwrap()
        .clone();
        let aliases = BTreeMap::from([(detail_hash.clone(), index_id.clone())]);
        let mut projections = Vec::new();

        project_detail_hash_source_refs_object(&mut fields, &aliases, &mut projections);

        assert_eq!(
            fields["common_ground"]["evidence_refs"],
            json!([index_id, stable_evidence])
        );
        assert_eq!(fields["unresolved"]["evidence_refs"], json!([unknown_hash]));
        assert_eq!(fields["free_text"], json!(detail_hash));
        assert_eq!(projections.len(), 1);
        assert_eq!(projections[0]["model_reference"], detail_hash);
    }

    #[test]
    fn topic_generation_selected_hinge_must_project_to_the_canonical_candidate() {
        let candidate = json!({
            "topic": "Does the trend persist?",
            "tickers": ["QQQ"],
            "meta_factor": "trend",
            "decision_hinge": "breakout_confirmation",
            "ttl": "1-3d",
            "why_debate": "trend and macro disagree",
            "evidence_refs": ["idx-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
        });
        // This is the real-world shape observed in Debug: the extractor
        // correctly selects the candidate's hinge but paraphrases its full
        // record. A content hash must not turn that selection into a distinct
        // topic.
        let selected_paraphrase = json!({
            "topic": "Should QQQ wait for a breakout confirmation?",
            "tickers": ["QQQ"],
            "meta_factor": "short-term trend",
            "decision_hinge": "breakout_confirmation",
            "ttl": "1-3d",
            "why_debate": "the confirmation threshold is unresolved",
            "evidence_refs": ["idx-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
        });
        let mut fields = json!({
            "candidate_topics": [candidate],
            "topics": [selected_paraphrase],
            "residual_risks": [],
            "coverage": [
                {"category":"trend","status":"selected","reason":"the selected hinge tests trend persistence","evidence_refs":["idx-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]},
                {"category":"valuation_expectations","status":"not_present","reason":"the fixture has no valuation evidence","evidence_refs":[]},
                {"category":"macro","status":"not_present","reason":"the fixture has no macro evidence","evidence_refs":[]},
                {"category":"event_risk","status":"not_present","reason":"the fixture has no event-risk evidence","evidence_refs":[]},
                {"category":"data_quality","status":"not_present","reason":"the fixture has no data-quality gap","evidence_refs":[]}
            ]
        })
        .as_object()
        .unwrap()
        .clone();

        enrich_compiled_fields("mediator.topic", "topic_generation", None, "", &mut fields)
            .unwrap();

        assert_eq!(fields["topics"][0], fields["candidate_topics"][0]);
    }

    #[test]
    fn topic_generation_resolves_a_unique_display_abbreviation_before_topic_id_hashing() {
        let evidence_id = format!("jin10-{}", "a".repeat(64));
        let index_id = format!("idx-{}", "b".repeat(64));
        let known = BTreeMap::from([
            (evidence_id.clone(), "event-a".to_owned()),
            (index_id.clone(), "phase1-summary".to_owned()),
        ]);
        let mut fields = json!({
            "candidate_topics": [{
                "topic": "Does the fact survive?",
                "tickers": ["QQQ"],
                "meta_factor": "macro",
                "decision_hinge": "fact_survival",
                "ttl": "1-3d",
                "why_debate": "a stable source is required",
                "evidence_refs": ["jin10-aaaa...", "idx-bbbb..."]
            }],
            "topics": [{"decision_hinge": "fact_survival"}],
            "coverage": [
                {"category":"trend","status":"not_present","reason":"not in fixture","evidence_refs":[]},
                {"category":"valuation_expectations","status":"not_present","reason":"not in fixture","evidence_refs":[]},
                {"category":"macro","status":"selected","reason":"the source is under review","evidence_refs":["jin10-aaaa..."]},
                {"category":"event_risk","status":"not_present","reason":"not in fixture","evidence_refs":[]},
                {"category":"data_quality","status":"not_present","reason":"not in fixture","evidence_refs":[]}
            ],
            "residual_risks": []
        })
        .as_object()
        .unwrap()
        .clone();

        normalize_phase2_topic_generation_evidence_refs(&mut fields, &known).unwrap();
        project_topic_generation_selection(&mut fields).unwrap();

        assert_eq!(
            fields["candidate_topics"][0]["evidence_refs"],
            json!([evidence_id, index_id])
        );
        assert_eq!(
            fields["topics"][0]["evidence_refs"],
            fields["candidate_topics"][0]["evidence_refs"]
        );
        assert_eq!(
            fields["topic_generation_reference_projection"]["resolved_abbreviations"]
                .as_array()
                .unwrap()
                .len(),
            3,
        );
    }

    #[test]
    fn topic_generation_summary_input_expands_only_unambiguous_display_ids() {
        let technical = format!("technical-{}", "a".repeat(64));
        let jin10 = format!("jin10-{}", "b".repeat(64));
        let known = BTreeMap::from([
            (technical.clone(), "technical-event".to_owned()),
            (jin10.clone(), "macro-event".to_owned()),
            (format!("idx-{}", "c".repeat(64)), "index-one".to_owned()),
            (format!("idx-{}", "d".repeat(64)), "index-two".to_owned()),
        ]);

        let (canonical, projection) = canonicalize_phase2_topic_generation_summary_source(
            "候选使用 technical-aaaa... 与 jin10-bbbb...；泛化占位 idx-... 保持待校验。",
            &known,
        )
        .unwrap();

        assert!(canonical.contains(&technical));
        assert!(canonical.contains(&jin10));
        assert!(canonical.contains("idx-..."));
        assert_eq!(projection.len(), 2);
        assert_eq!(projection[0]["stage"], "before_phase2_summary_extraction");
    }

    #[test]
    fn topic_generation_rejects_ambiguous_display_abbreviation() {
        let known = BTreeMap::from([
            (
                format!("jin10-deadbeef{}", "a".repeat(56)),
                "event-a".to_owned(),
            ),
            (
                format!("jin10-deadbeef{}", "b".repeat(56)),
                "event-b".to_owned(),
            ),
        ]);
        let mut fields = json!({
            "candidate_topics": [{"evidence_refs": ["jin10-deadbeef..."]}]
        })
        .as_object()
        .unwrap()
        .clone();

        let error =
            normalize_phase2_topic_generation_evidence_refs(&mut fields, &known).unwrap_err();

        assert!(error.to_string().contains("is ambiguous"));
    }

    #[test]
    fn phase1_rejects_raw_hashes_and_local_web_result_numbers() {
        let fields = json!({
            "per_ticker": {"QQQ": {
                "direction": "mixed", "confidence": 0.5, "long_probability": 0.5, "report": "mixed",
                "key_evidence": [{
                    "claim": "claim", "evidence_type": "fact", "source": "source",
                    "timestamp": "2026-08-03T00:00:00Z", "source_tier": "official",
                    "first_source": "source", "is_derivative_repost": false,
                    "evidence_age": "0-2d", "source_confidence": 0.8
                }],
                "priced_in": "unclear", "echo_chamber_risk": "low",
                "crowded_consensus_risk": "low", "validation_triggers": [], "data_gaps": [],
                "analysis_trace": {"source_refs": ["web.run:search0"]}
            }}
        })
        .as_object()
        .unwrap()
        .clone();

        assert!(validate_phase1_compiled_fields(&fields).is_err());
    }

    #[test]
    fn phase1_accepts_structured_evidence_with_a_stable_reference() {
        let evidence_id = format!("technical-{}", "a".repeat(64));
        let fields = json!({
            "per_ticker": {"QQQ": {
                "direction": "mixed", "confidence": 0.5, "long_probability": 0.5, "report": "mixed",
                "key_evidence": [{
                    "claim": "claim", "evidence_type": "fact", "source": "filestore.run_input.technical",
                    "timestamp": "2026-08-03T00:00:00Z", "source_tier": "unknown",
                    "first_source": "filestore.run_input.technical", "is_derivative_repost": false,
                    "evidence_age": "0-2d", "source_confidence": 0.8,
                    "evidence_refs": [evidence_id]
                }],
                "priced_in": "unclear", "echo_chamber_risk": "unknown",
                "crowded_consensus_risk": "unknown", "validation_triggers": [], "data_gaps": []
            }}
        })
        .as_object()
        .unwrap()
        .clone();

        validate_phase1_compiled_fields(&fields).unwrap();
    }

    #[test]
    fn phase1_only_allows_empty_evidence_for_context_only_vix() {
        let unobserved = |ticker: &str| {
            json!({
                "direction": "unobserved",
                "confidence": 0.0,
                "long_probability": 0.5,
                "report": format!("{ticker} is unobserved."),
                "key_evidence": [],
                "priced_in": "unclear",
                "echo_chamber_risk": "unknown",
                "crowded_consensus_risk": "unknown",
                "validation_triggers": ["Collect current evidence."],
                "data_gaps": ["Current evidence is unavailable."]
            })
        };
        let vix_only = json!({
            "per_ticker": {"VIX": unobserved("VIX")}
        });
        validate_phase1_compiled_fields(vix_only.as_object().unwrap()).unwrap();

        let investable = json!({
            "per_ticker": {"QQQ": unobserved("QQQ")}
        });
        let error = validate_phase1_compiled_fields(investable.as_object().unwrap()).unwrap_err();
        assert!(error
            .to_string()
            .contains("empty key_evidence is only allowed for context-only VIX"));
    }

    #[test]
    fn phase1_normalizes_nested_cross_asset_findings_before_ticker_validation() {
        let evidence_id = format!("technical-{}", "a".repeat(64));
        let mut fields = json!({
            "per_ticker": {
                "QQQ": {
                    "direction": "mixed", "confidence": 0.5, "long_probability": 0.5,
                    "report": "mixed", "key_evidence": [{
                        "claim": "claim", "evidence_type": "fact",
                        "source": "filestore.run_input.technical",
                        "timestamp": "2026-08-03T00:00:00Z", "source_tier": "unknown",
                        "first_source": "filestore.run_input.technical",
                        "is_derivative_repost": false, "evidence_age": "0-2d",
                        "source_confidence": 0.8, "evidence_refs": [evidence_id]
                    }],
                    "priced_in": "unclear", "echo_chamber_risk": "unknown",
                    "crowded_consensus_risk": "unknown", "validation_triggers": [],
                    "data_gaps": []
                },
                "cross_asset_findings": [{
                    "claim": "shared macro finding",
                    "evidence_refs": [evidence_id]
                }]
            }
        })
        .as_object_mut()
        .unwrap()
        .clone();

        normalize_phase1_summary_layout(&mut fields).unwrap();

        assert!(fields["per_ticker"].get("cross_asset_findings").is_none());
        assert_eq!(
            fields["cross_asset_findings"][0]["claim"],
            "shared macro finding"
        );
        validate_phase1_compiled_fields(&fields).unwrap();
    }

    fn canonical_phase3_fields() -> serde_json::Map<String, Value> {
        json!({
            "decisions": {
                "QQQ": {
                    "rating": "Underweight",
                    "long_probability": 0.44,
                    "short_probability": 0.56,
                    "base_probability": 0.45,
                    "debate_adjustment": -0.01,
                    "adjustment_reason": "new_information",
                    "adjustment_scale": "uncalibrated_conservative_v1",
                    "confidence_basis": "directional_evidence",
                    "hold_reason": null,
                    "plan": "Maintain the evidence-bounded downside plan.",
                    "probability_rationale": "The validated debate moved the Phase 1 base by one point.",
                    "scenarios": {
                        "bull": {"probability": 0.19, "conditional_long_probability": 0.75, "drivers": ["breadth recovery"], "triggers": ["3h breakout"], "confirmation": "price confirms"},
                        "base": {"probability": 0.50, "conditional_long_probability": 0.471, "drivers": ["range continuation"], "triggers": ["range persists"], "confirmation": "range holds"},
                        "bear": {"probability": 0.31, "conditional_long_probability": 0.20, "drivers": ["downtrend continuation"], "triggers": ["20m breakdown"], "confirmation": "lower low confirms"}
                    },
                    "decision_hinges": [{
                        "hinge": "validated collision",
                        "evidence_refs": ["web-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
                        "phase2_claim_ids": ["topic-accepted:stree:7"]
                    }],
                    "validation_plan": ["observe the cited hinge"]
                }
            }
        })
        .as_object()
        .unwrap()
        .clone()
    }

    fn phase3_state_with_accepted_debate_claim() -> Value {
        json!({
            "investable_assets": ["QQQ"],
            "weighted_probability_base": {"QQQ": {"long_probability": 0.45}},
            "phase1_evidence_event_ledger": {"events": {
                "technical-price-series:QQQ:2026-08-03": {
                    "tickers": ["QQQ"],
                    "evidence_refs": ["technical-base"]
                }
            }},
            "topic_debate_states": {
                "topic-accepted": {
                    "stree": {
                        "closure": {
                            "reason": "consensus",
                            "controller_decided": true,
                            "independence_assessment": {
                                "adjustment_eligible": true,
                                "reason": "distinct_models_and_new_event_after_direct_collision"
                            },
                            "consensus_claim_ids": ["topic-accepted:stree:7"],
                            "claim_ledger": [{
                                "claim_id": "topic-accepted:stree:7",
                                "evidence_refs": ["web-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
                                "evidence_links": [{
                                    "evidence_ref": "web-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                    "relation": "supports"
                                }]
                            }],
                            "accepted_evidence": [{
                                "claim_id": "topic-accepted:stree:7",
                                "evidence_refs": ["web-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
                            }],
                            "controller_verified_evidence_refs": ["web-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
                        }
                    }
                }
            }
        })
    }

    #[test]
    fn phase3_rejects_unaccounted_probability_shift() {
        let state = json!({
            "investable_assets": ["QQQ"],
            "weighted_probability_base": {"QQQ": {"long_probability": 0.45}}
        });
        let mut fields = canonical_phase3_fields();
        fields["decisions"]["QQQ"]["debate_adjustment"] = json!(0.0);

        assert!(validate_phase3_compiled_fields(&state, &mut fields).is_err());
    }

    #[test]
    fn phase3_accepts_only_canonical_accounted_decision() {
        let state = phase3_state_with_accepted_debate_claim();
        let mut fields = canonical_phase3_fields();

        validate_phase3_compiled_fields(&state, &mut fields).unwrap();
        assert_eq!(
            fields["decisions"]["QQQ"]["debate_adjustment_provenance"]["accepted_phase2_claim_ids"],
            json!(["topic-accepted:stree:7"])
        );
    }

    #[test]
    fn phase3_projects_the_rust_owned_base_probability() {
        let state = phase3_state_with_accepted_debate_claim();
        let mut fields = canonical_phase3_fields();
        fields["decisions"]["QQQ"]["base_probability"] = json!(0.28);

        validate_phase3_compiled_fields(&state, &mut fields).unwrap();

        assert_eq!(fields["decisions"]["QQQ"]["base_probability"], 0.45);
        assert_eq!(
            fields["decisions"]["QQQ"]["base_probability_projection"]["model_base_probability"],
            0.28
        );
        assert_eq!(
            fields["decisions"]["QQQ"]["base_probability_projection"]["overridden"],
            true
        );
    }

    #[test]
    fn phase3_rating_and_non_hold_reason_are_rust_projected() {
        let state = phase3_state_with_accepted_debate_claim();
        let mut fields = canonical_phase3_fields();
        fields["decisions"]["QQQ"]["rating"] = json!("Hold");
        fields["decisions"]["QQQ"]["hold_reason"] = json!("conflicting_evidence");

        validate_phase3_compiled_fields(&state, &mut fields).unwrap();

        assert_eq!(fields["decisions"]["QQQ"]["rating"], "Underweight");
        assert_eq!(fields["decisions"]["QQQ"]["hold_reason"], Value::Null);
        assert_eq!(
            fields["decisions"]["QQQ"]["rating_projection"]["overridden"],
            true
        );
        assert_eq!(
            fields["decisions"]["QQQ"]["scenarios"]["bull"]["probability"],
            0.19
        );
        assert_eq!(
            fields["decisions"]["QQQ"]["scenarios"]["bull"]["conditional_long_probability"],
            0.75
        );
        assert_eq!(
            fields["decisions"]["QQQ"]["scenarios"]["base"]["probability"],
            0.5
        );
        assert_eq!(
            fields["decisions"]["QQQ"]["scenarios"]["bear"]["probability"],
            0.31
        );
        assert_eq!(
            fields["decisions"]["QQQ"]["scenario_probability_validation"]["authority"],
            "rust_validation"
        );
    }

    #[test]
    fn phase3_rejects_scenarios_that_do_not_match_long_probability() {
        let state = phase3_state_with_accepted_debate_claim();
        let mut fields = canonical_phase3_fields();
        fields["decisions"]["QQQ"]["scenarios"]["bull"]["probability"] = json!(0.1);
        fields["decisions"]["QQQ"]["scenarios"]["base"]["probability"] = json!(0.2);
        fields["decisions"]["QQQ"]["scenarios"]["bear"]["probability"] = json!(0.7);

        let error = validate_phase3_compiled_fields(&state, &mut fields).unwrap_err();

        assert!(error
            .to_string()
            .contains("scenario probabilities imply long_probability"));
    }

    #[test]
    fn phase3_scenario_retry_instruction_preserves_the_probability_ledger() {
        let error = anyhow::anyhow!(
            "Phase 3 QQQ scenario probabilities imply long_probability 0.37, expected 0.53"
        );
        let instruction = phase3_scenario_validation_retry_instruction(&error);

        assert!(instruction.contains("do not invent a debate adjustment"));
        assert!(instruction.contains("conditional_long_probability"));
        assert!(instruction.contains("Σ(probability × conditional_long_probability)"));
    }

    #[test]
    fn phase3_rejects_nonzero_adjustment_without_controller_consensus() {
        let state = json!({
            "investable_assets": ["QQQ"],
            "weighted_probability_base": {"QQQ": {"long_probability": 0.45}},
            "topic_debate_states": {
                "topic-unresolved": {
                    "stree": {
                        "closure": {
                            "reason": "unresolved_disagreement",
                            "controller_decided": true,
                            "consensus_claim_ids": [],
                            "claim_ledger": [{
                                "claim_id": "topic-accepted:stree:7",
                                "evidence_refs": ["web-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
                            }]
                        }
                    }
                }
            }
        });
        let mut fields = canonical_phase3_fields();

        let error = validate_phase3_compiled_fields(&state, &mut fields).unwrap_err();

        assert!(error
            .to_string()
            .contains("controller-accepted Phase 2 consensus claim"));
    }

    #[test]
    fn phase3_rejects_a_correlated_same_model_consensus_as_probability_evidence() {
        let mut state = phase3_state_with_accepted_debate_claim();
        state["topic_debate_states"]["topic-accepted"]["stree"]["closure"]
            ["independence_assessment"] = json!({
            "adjustment_eligible": false,
            "reason": "same_model_shared_warmup_is_correlated_not_an_independent_vote"
        });
        let mut fields = canonical_phase3_fields();

        let error = validate_phase3_compiled_fields(&state, &mut fields).unwrap_err();

        assert!(error.to_string().contains("consensus is correlated"));
        assert!(error
            .to_string()
            .contains("same_model_shared_warmup_is_correlated_not_an_independent_vote"));
    }

    #[test]
    fn phase3_rejects_reinforcing_an_adjustment_with_base_evidence() {
        let mut state = phase3_state_with_accepted_debate_claim();
        state["topic_debate_states"]["topic-accepted"]["stree"]["closure"]["claim_ledger"] = json!([{
            "claim_id": "topic-accepted:stree:7",
            "evidence_refs": ["technical-base"],
            "evidence_links": [{"evidence_ref": "technical-base", "relation": "supports"}]
        }]);
        state["topic_debate_states"]["topic-accepted"]["stree"]["closure"]["accepted_evidence"] = json!([{
            "claim_id": "topic-accepted:stree:7",
            "evidence_refs": ["technical-base"]
        }]);
        state["topic_debate_states"]["topic-accepted"]["stree"]["closure"]
            ["controller_verified_evidence_refs"] = json!(["technical-base"]);
        let mut fields = canonical_phase3_fields();
        fields["decisions"]["QQQ"]["decision_hinges"][0]["evidence_refs"] =
            json!(["technical-base"]);

        let error = validate_phase3_compiled_fields(&state, &mut fields).unwrap_err();

        assert!(error.to_string().contains("no novel evidence"));
    }

    #[test]
    fn phase3_rejects_an_uncalibrated_adjustment_outside_the_rust_scale() {
        let state = phase3_state_with_accepted_debate_claim();
        let mut fields = canonical_phase3_fields();
        fields["decisions"]["QQQ"]["debate_adjustment"] = json!(-0.02);
        fields["decisions"]["QQQ"]["long_probability"] = json!(0.43);
        fields["decisions"]["QQQ"]["short_probability"] = json!(0.57);

        let error = validate_phase3_compiled_fields(&state, &mut fields).unwrap_err();

        assert!(error
            .to_string()
            .contains("must have absolute value 0.01 or 0.03"));
    }

    #[test]
    fn phase3_rejects_adjustment_when_hinge_evidence_does_not_match_claim() {
        let state = phase3_state_with_accepted_debate_claim();
        let mut fields = canonical_phase3_fields();
        fields["decisions"]["QQQ"]["decision_hinges"][0]["evidence_refs"] =
            json!(["idx-unrelated"]);

        let error = validate_phase3_compiled_fields(&state, &mut fields).unwrap_err();

        assert!(error
            .to_string()
            .contains("must cite evidence attached to each controller-accepted Phase 2 claim"));
    }

    #[test]
    fn phase4_rejects_reversing_the_rust_owned_candidate_action() {
        let state = json!({
            "investable_assets": ["QQQ"],
            "research_plan": {"per_ticker": {"QQQ": {
                "rating": "Underweight", "probability_rationale": "downside dominates"
            }}}
        });
        let mut fields = json!({"plans": {"QQQ": {
            "action": "Buy", "candidate_action": "Buy",
            "execution_decision": "execute_candidate", "position_size_pct_max": 0.2,
            "entry_price": null, "stop_loss": null, "blockers": [],
            "execution_conditions": [], "downgrade_reason": "", "rationale": "reverse it"
        }}})
        .as_object()
        .unwrap()
        .clone();

        assert!(validate_phase4_compiled_fields(&state, &mut fields).is_err());
    }

    #[test]
    fn phase4_open_blocker_forces_a_hold_instead_of_report_only_text() {
        let state = json!({
            "investable_assets": ["QQQ"],
            "research_plan": {"per_ticker": {"QQQ": {
                "rating": "Buy", "long_probability": 0.80, "probability_rationale": "upside dominates"
            }}}
        });
        let mut fields = json!({"plans": {"QQQ": {
            "action": "Buy", "candidate_action": "Buy",
            "execution_decision": "execute_candidate", "position_size_pct_max": 0.1,
            "entry_price": null, "stop_loss": null, "blockers": ["price anchor missing"],
            "execution_conditions": [], "downgrade_reason": "", "rationale": "would otherwise execute"
        }}})
        .as_object()
        .unwrap()
        .clone();

        let error = validate_phase4_compiled_fields(&state, &mut fields).unwrap_err();

        assert!(error
            .to_string()
            .contains("open execution blockers require action=Hold"));
    }

    #[test]
    fn phase4_probability_risk_budget_rejects_an_oversized_plan() {
        let state = json!({
            "investable_assets": ["QQQ"],
            "research_plan": {"per_ticker": {"QQQ": {
                "rating": "Overweight", "long_probability": 0.68, "probability_rationale": "modest upside"
            }}}
        });
        let mut fields = json!({"plans": {"QQQ": {
            "action": "Buy", "candidate_action": "Buy",
            "execution_decision": "execute_candidate", "position_size_pct_max": 0.30,
            "entry_price": null, "stop_loss": null, "blockers": [],
            "execution_conditions": [], "downgrade_reason": "", "rationale": "too large"
        }}})
        .as_object()
        .unwrap()
        .clone();

        let error = validate_phase4_compiled_fields(&state, &mut fields).unwrap_err();

        assert!(error
            .to_string()
            .contains("exceeds Rust probability risk budget"));
    }

    #[test]
    fn phase5_rejects_an_undeclared_missing_constraint() {
        let state = json!({"investable_assets": ["QQQ"]});
        let mut fields = json!({
            "stance": "neutral",
            "unique_risk_contribution": "gap risk",
            "risk_dimension": "gap",
            "disagreement_with_prior": "none",
            "no_new_information": false,
            "recommended_adjustment": "cap the position",
            "per_asset": {"QQQ": {
                "position_cap_pct": 0.2,
                "max_drawdown_pct": null,
                "stop_type": "soft",
                "risk_off_trigger": "breakdown",
                "rebalance_trigger": "volatility doubles",
                "review_window": "one day",
                "constraint_confidence": 0.7
            }},
            "cash_hedge_recommendation": "hold cash"
        })
        .as_object()
        .unwrap()
        .clone();

        assert!(validate_phase5_compiled_fields(
            &state,
            "risk.neutral",
            "risk summary",
            &[],
            &mut fields
        )
        .is_err());
    }

    #[test]
    fn phase5_accepts_declared_missing_constraint_confidence() {
        let state = json!({"investable_assets": ["QQQ"]});
        let mut fields = json!({
            "stance": "neutral",
            "unique_risk_contribution": "gap risk",
            "risk_dimension": "gap",
            "disagreement_with_prior": "none",
            "no_new_information": false,
            "recommended_adjustment": "cap the position",
            "per_asset": {"QQQ": {
                "position_cap_pct": 0.2,
                "max_drawdown_pct": 0.1,
                "stop_type": "soft",
                "risk_off_trigger": "breakdown",
                "rebalance_trigger": "volatility doubles",
                "review_window": "one day",
                "constraint_confidence": null
            }},
            "cash_hedge_recommendation": "hold cash"
        })
        .as_object()
        .unwrap()
        .clone();

        validate_phase5_compiled_fields(
            &state,
            "risk.neutral",
            "risk summary",
            &["QQQ.constraint_confidence".to_owned()],
            &mut fields,
        )
        .unwrap();
    }

    #[test]
    fn phase5_canonicalizes_display_case_and_role_prefix_before_role_validation() {
        let state = json!({"investable_assets": ["QQQ"]});
        let mut fields = json!({
            "stance": "RISK.AGGRESSIVE",
            "unique_risk_contribution": "",
            "disagreement_with_prior": "none",
            "no_new_information": true,
            "recommended_adjustment": "",
            "per_asset": {"QQQ": {
                "position_cap_pct": 0.2,
                "max_drawdown_pct": 0.1,
                "stop_type": "soft",
                "risk_off_trigger": "breakdown",
                "rebalance_trigger": "volatility doubles",
                "review_window": "one day",
                "constraint_confidence": 0.7
            }},
            "cash_hedge_recommendation": "hold cash"
        })
        .as_object()
        .unwrap()
        .clone();

        validate_phase5_compiled_fields(
            &state,
            "risk.aggressive",
            "risk summary",
            &[],
            &mut fields,
        )
        .unwrap();

        assert_eq!(fields["stance"], "aggressive");
    }

    #[test]
    fn phase5_rejects_a_repeated_constraint_labeled_as_no_new_information() {
        let state = json!({"investable_assets": ["QQQ"]});
        let mut fields = json!({
            "stance": "neutral",
            "unique_risk_contribution": "repeats the Phase 4 cap",
            "risk_dimension": "volatility",
            "disagreement_with_prior": "none",
            "no_new_information": true,
            "recommended_adjustment": "keep the same cap",
            "per_asset": {"QQQ": {
                "position_cap_pct": 0.2,
                "max_drawdown_pct": 0.1,
                "stop_type": "soft",
                "risk_off_trigger": "breakdown",
                "rebalance_trigger": "volatility doubles",
                "review_window": "one day",
                "constraint_confidence": 0.7
            }},
            "cash_hedge_recommendation": "hold cash"
        })
        .as_object()
        .unwrap()
        .clone();

        let error = validate_phase5_compiled_fields(
            &state,
            "risk.neutral",
            "risk summary",
            &[],
            &mut fields,
        )
        .unwrap_err();

        assert!(error.to_string().contains("no_new_information=true"));
    }

    #[test]
    fn phase5_leave_one_reviewer_out_excludes_repeated_dimensions_and_keeps_only_marginal_caps() {
        let history = vec![
            json!({
                "role": "risk.aggressive",
                "index_id": "idx-gap",
                "payload": {
                    "no_new_information": false,
                    "risk_dimension": "gap",
                    "unique_risk_contribution": "overnight-gap cap",
                    "per_asset": {"QQQ": {"position_cap_pct": 0.20}}
                }
            }),
            json!({
                "role": "risk.neutral",
                "index_id": "idx-liquidity",
                "payload": {
                    "no_new_information": false,
                    "risk_dimension": "liquidity",
                    "unique_risk_contribution": "liquidity cap",
                    "per_asset": {"QQQ": {"position_cap_pct": 0.40}}
                }
            }),
            json!({
                "role": "risk.conservative",
                "index_id": "idx-gap-duplicate",
                "payload": {
                    "no_new_information": false,
                    "risk_dimension": "gap",
                    "unique_risk_contribution": "same gap cap restated",
                    "per_asset": {"QQQ": {"position_cap_pct": 0.10}}
                }
            }),
        ];

        let ledger = phase5_reviewer_independence_ledger(&history, &["QQQ".to_owned()]).unwrap();

        assert_eq!(
            ledger["reviewers"]
                .as_array()
                .unwrap()
                .iter()
                .find(|entry| entry["index_id"] == "idx-gap")
                .unwrap()["contribution_is_independent"],
            false
        );
        assert_eq!(
            ledger["per_asset"]["QQQ"]["full_effective_position_cap_pct"],
            json!(0.40)
        );
        assert_eq!(
            ledger["per_asset"]["QQQ"]["eligible_source_refs"],
            json!(["idx-liquidity"])
        );
        assert_eq!(
            ledger["per_asset"]["QQQ"]["leave_one_reviewer_out"][0]
                ["position_cap_pct_without_reviewer"],
            Value::Null
        );
        assert_eq!(
            ledger["per_asset"]["QQQ"]["leave_one_reviewer_out"][0]["marginal"],
            true
        );
    }

    #[test]
    fn phase2_recovered_failure_degrades_the_run_health_projection() {
        let mut state = json!({"degraded": false, "errors": []});

        record_phase2_runtime_failure(
            &mut state,
            "topic-a",
            super::DebateActor::Bear,
            "stree_command_failure",
            "invalid claim id",
        );

        assert_eq!(state["degraded"], true);
        assert_eq!(state["errors"][0]["phase"], 2);
        assert_eq!(state["errors"][0]["recovered"], true);
    }

    #[test]
    fn phase2_registers_terminal_tool_evidence_before_accepting_a_submission() {
        let mut tree = TopicDebateTree::open("topic-a", json!({"topic": "rates"}), 1).unwrap();
        let actor = tree.next_dispatch().unwrap().actor;
        let evidence_id = "web-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let artifact = json!({
            "verified_evidence_refs": [evidence_id],
            "verified_evidence_records": [{
                "evidence_id": evidence_id,
                "source_url": "https://example.test/same-event",
                "published_at": "2026-08-03T00:00:00Z"
            }],
            "phase2_stree": {
                "command": "submit_debate_turn",
                "payload": {
                    "stance": "needs_evidence",
                    "message": "Use the verified source before concluding.",
                    "report": "Participant records an evidence gap.",
                    "evidence_refs": [evidence_id],
                    "evidence_links": [{
                        "evidence_ref": evidence_id,
                        "relation": "qualifies"
                    }]
                }
            }
        });

        apply_phase2_stree_command(&mut tree, actor, &artifact).unwrap();

        assert!(tree.evidence_registry.contains(evidence_id));
        assert_eq!(
            tree.evidence_event_clusters[evidence_id],
            "url:httpsexampletestsameevent"
        );
    }

    #[test]
    fn phase2_nonterminal_turn_gets_a_same_session_terminal_tool_retry() {
        assert!(!phase2_stree_terminal_command_present(&json!({
            "response_text": "analysis without a tool call"
        })));
        assert!(phase2_stree_terminal_command_present(&json!({
            "phase2_stree": {"command": "submit_debate_turn"}
        })));

        let participant = phase2_terminal_tool_retry_injection("topic-a", DebateActor::Bull);
        assert!(participant.contains("submit_debate_turn"));
        assert!(participant.contains("phase2_terminal_tool_retry"));

        let controller = phase2_terminal_tool_retry_injection("topic-a", DebateActor::Controller);
        assert!(controller.contains("route_debate_turn"));
        assert!(controller.contains("close_debate"));

        let close_retry = phase2_controller_close_retry_injection("topic-a");
        assert!(close_retry.contains("phase2_terminal_close_retry"));
        assert!(close_retry.contains("Do not route or wait"));
        assert!(close_retry.contains("close_debate"));
        assert!(super::is_phase2_controller_close_required_error(
            "controller must close because no newly observed evidence event was introduced"
        ));
        assert!(super::is_phase2_controller_close_required_error(
            "controller route exceeded max_debate_rounds"
        ));
    }

    #[test]
    fn phase2_dispatch_key_changes_when_delivery_changes() {
        let first = phase2_stree_dispatch_key("topic-a", DebateActor::Bull, &["delivery-a"]);
        let retry = phase2_stree_dispatch_key("topic-a", DebateActor::Bull, &["delivery-a"]);
        let next = phase2_stree_dispatch_key("topic-a", DebateActor::Bull, &["delivery-b"]);

        assert_eq!(first, retry);
        assert_ne!(first, next);
    }

    #[test]
    fn phase2_rejects_a_topic_ttl_outside_the_decision_window() {
        let fields = json!({"topics": [{"ttl": "1-2w"}]})
            .as_object()
            .unwrap()
            .clone();

        assert!(validate_phase2_topic_ttls(&fields).is_err());
    }

    #[test]
    fn phase2_final_projection_does_not_invent_consensus() {
        let state = json!({"topic_debate_states": {"topic-a": {
            "topic": {"topic": "rates"},
            "stree": {
                "status": "closed", "round": 2,
                "closure": {
                    "reason": "unresolved_disagreement", "round": 2,
                    "claim_ledger": [{"claim_id": "topic-a:stree:7"}],
                    "consensus_claim_ids": [],
                    "unresolved_claim_ids": ["topic-a:stree:7"]
                }
            }
        }}});
        let mut fields = json!({
            "topics": [], "consensus": [{"topic_id": "topic-a"}],
            "unresolved_disagreements": [], "closure_reasons": []
        })
        .as_object()
        .unwrap()
        .clone();

        project_phase2_final_fields(&state, &mut fields).unwrap();

        assert_eq!(fields["consensus"], json!([]));
        assert_eq!(fields["unresolved_disagreements"][0]["topic_id"], "topic-a");
        assert_eq!(fields["closure_reasons"][0]["round"], 2);
    }

    #[test]
    fn manifest_git_sha_resolves_to_a_full_commit_identity() {
        let sha = resolve_git_sha(&orchestrator_core::default_project_root()).unwrap();

        assert!(matches!(sha.len(), 40 | 64));
        assert!(sha.chars().all(|character| character.is_ascii_hexdigit()));
    }

    #[test]
    fn phase6_inherits_probability_and_projects_risk_gaps_with_sources() {
        let state = json!({
            "investable_assets": ["QQQ"],
            "current_portfolio_weights": {"QQQ": 0.2},
            "research_plan": {"per_ticker": {"QQQ": {
                "rating": "Underweight", "long_probability": 0.44, "short_probability": 0.56
            }}},
            "trader_investment_plan": {"per_ticker": {"QQQ": {
                "action": "Sell", "execution_conditions": [], "entry_price": null,
                "stop_loss": null, "downgrade_reason": "wait for reversal", "blockers": []
            }}},
            "risk_debate_state": {
                "history": [{
                    "index_id": "idx-risk01",
                    "payload": {"missing_fields": ["QQQ.max_drawdown_pct"]}
                }],
                "reviewer_independence": {"per_asset": {"QQQ": {
                    "leave_one_reviewer_out": [{
                        "index_id": "idx-risk01", "position_cap_pct": 0.15, "marginal": true
                    }]
                }}}
            }
        });
        let mut fields = json!({"per_asset": {"QQQ": {
            "direction_constraint": "decrease_only",
            "execution_status": "execute",
            "max_target_weight": 0.20,
            "max_weight_delta": 0.10,
            "binding_risk_controls": [{
                "control": "reduce on a confirmed breakdown",
                "source_refs": ["idx-risk01", "idx-phase3"]
            }],
            "rating": "",
            "inherited_probability": null,
            "execution_rationale": "de-risk only",
            "unresolved_blockers": []
        }}})
        .as_object()
        .unwrap()
        .clone();

        enrich_and_validate_phase6_compiled_fields(&state, &mut fields).unwrap();

        let decision = &fields["per_asset"]["QQQ"];
        assert_eq!(decision["inherited_probability"], 0.44);
        assert_eq!(decision["long_probability"], 0.44);
        assert_eq!(decision["short_probability"], 0.56);
        assert_eq!(
            decision["binding_risk_controls"][0]["source_refs"],
            json!(["idx-risk01"])
        );
        assert_eq!(
            decision["risk_control_source_projection"]["controls"][0]
                ["accepted_phase5_source_refs"],
            json!(["idx-risk01"])
        );
        assert_eq!(
            decision["risk_control_source_projection"]["controls"][0]
                ["rejected_non_phase5_source_refs"],
            json!(["idx-phase3"])
        );
        assert_eq!(decision["execution_status"], "downgrade");
        assert_eq!(decision["max_target_weight"], 0.15);
        assert_eq!(decision["max_weight_delta"], 0.1);
        assert_eq!(
            decision["risk_control_source_projection"]["current_exposure_exceeds_risk_cap"],
            true
        );
        assert!(decision["unresolved_blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "QQQ.max_drawdown_pct"));
    }

    #[test]
    fn phase6_projects_constraints_from_the_trader_final_hold_action() {
        let state = json!({
            "investable_assets": ["QQQ"],
            "current_portfolio_weights": {"QQQ": 0.1},
            "research_plan": {"per_ticker": {"QQQ": {
                "rating": "Hold", "long_probability": 0.51, "short_probability": 0.49
            }}},
            "trader_investment_plan": {"per_ticker": {"QQQ": {
                "action": "Hold", "execution_conditions": [], "entry_price": null,
                "stop_loss": null, "downgrade_reason": "insufficient confirmation", "blockers": []
            }}},
            "risk_debate_state": {
                "history": [{
                    "index_id": "idx-risk01", "payload": {"missing_fields": []}
                }],
                "reviewer_independence": {"per_asset": {"QQQ": {
                    "leave_one_reviewer_out": [{
                        "index_id": "idx-risk01", "position_cap_pct": 0.2, "marginal": true
                    }]
                }}}
            }
        });
        let mut fields = json!({"per_asset": {"QQQ": {
            "direction_constraint": "decrease_only",
            "execution_status": "execute",
            "max_target_weight": 0.0,
            "max_weight_delta": 0.1,
            "binding_risk_controls": [{
                "control": "do not add without confirmation", "source_refs": ["idx-risk01"]
            }],
            "rating": "Hold",
            "inherited_probability": null,
            "execution_rationale": "wait",
            "unresolved_blockers": []
        }}})
        .as_object()
        .unwrap()
        .clone();

        enrich_and_validate_phase6_compiled_fields(&state, &mut fields).unwrap();

        let decision = &fields["per_asset"]["QQQ"];
        assert_eq!(decision["direction_constraint"], "unchanged");
        assert_eq!(decision["execution_status"], "wait");
        assert_eq!(decision["current_weight"], 0.1);
        assert_eq!(decision["max_target_weight"], 0.1);
        assert_eq!(decision["max_weight_delta"], 0.0);
        assert_eq!(
            decision["constraint_projection"]["model_direction_constraint"],
            "decrease_only"
        );
        assert_eq!(decision["constraint_projection"]["overridden"], true);
    }

    #[test]
    fn stree_turns_defer_phase_summary_until_the_phase2_reducer() {
        assert!(defers_phase_summary(2, "stree_turn"));
        assert!(!is_cacheable_unit(2, "stree_turn"));
        assert!(is_cacheable_unit(2, "phase2_final"));
        assert!(!defers_phase_summary(2, "phase2_final"));
        assert!(!defers_phase_summary(3, "stree_turn"));
        assert!(!persists_phase_index(2, "warmup"));
        assert!(!persists_phase_index(2, "topic_generation"));
        assert!(!persists_phase_index(2, "stree_turn"));
        assert!(persists_phase_index(2, "phase2_final"));
        assert_eq!(
            prompt_owner_for_unit("researcher.bull", "stree_turn"),
            "researcher.bull.interaction"
        );
        assert_eq!(
            prompt_owner_for_unit("researcher.bear", "stree_turn"),
            "researcher.bear.interaction"
        );
    }

    #[test]
    fn phase2_debug_summary_contains_each_debate_turn_and_final_controller() {
        let state = json!({
            "run_id": "run-a",
            "role_job_metrics": [
                {"phase": 2, "role": "researcher.bull.initial", "topic_id": "topic-a", "round": 0, "run_id": "run-a", "session_id": "session-bull", "turn_id": "turn-bull"},
                {"phase": 2, "role": "researcher.bear.interaction", "topic_id": "topic-a", "round": 1, "run_id": "run-a", "session_id": "session-bear", "turn_id": "turn-bear"},
                {"phase": 2, "role": "mediator.topic_controller", "topic_id": "topic-a", "round": 1, "run_id": "run-a", "session_id": "session-controller", "turn_id": "turn-controller"}
            ],
            "topic_generation_artifact": {
                "topics": [{"topic_id": "topic-a", "topic": "rate versus momentum"}]
            },
            "topic_debate_states": {
                "topic-a": {
                    "topic": {"topic_id": "topic-a", "topic": "rate versus momentum"},
                    "turns": [
                        {
                            "role": "researcher.bull.initial",
                            "artifact": {
                                "profile": "debate_seed",
                                "unit_key": "seed-bull",
                                "payload": {"claims": [{"claim": "bull claim"}]}
                            }
                        },
                        {
                            "role": "researcher.bear.interaction",
                            "round": 1,
                            "artifact": {
                                "profile": "debate_response",
                                "unit_key": "reply-bear",
                                "payload": {"replies": [{"reason": "bear reply"}]}
                            }
                        }
                    ],
                    "final_controller_artifact": {
                        "profile": "topic_control",
                        "unit_key": "controller-1",
                        "payload": {
                            "claim_ledger": [{"status": "contested"}],
                            "decision_hinges": [{"hinge": "the hinge"}],
                            "soft_control": {
                                "should_continue": false,
                                "stop_reason": "completed"
                            }
                        }
                    }
                }
            }
        });

        let summary = phase2_debate_debug_summary(&state);

        assert_eq!(summary["kind"], "phase2_debate_process_summary");
        assert_eq!(summary["identity_kind"], "aggregate_runtime_view");
        assert_eq!(summary["source_session_turns"].as_array().unwrap().len(), 3);
        assert_eq!(
            summary["topics"][0]["source_session_turns"][0]["run_id"],
            "run-a"
        );
        assert_eq!(summary["topics"].as_array().unwrap().len(), 1);
        assert!(summary["topics"][0].get("turns").is_none());
        assert!(summary["topics"][0].get("stree").is_some());
        assert_eq!(
            summary["topics"][0]["final_controller"]["payload"]["soft_control"]["should_continue"],
            false
        );
    }

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
        let no_collision = json!({"turns": []});
        let mut continue_controller = json!({
            "payload": {
                "soft_control": {"should_continue": true},
                "next_steers": [{"steer_id": "steer-1"}]
            }
        });
        assert!(
            controller_should_continue(&mut continue_controller, &no_collision, "topic-a").unwrap()
        );
        let mut stop_controller = json!({
            "payload": {"soft_control": {"should_continue": false}}
        });
        assert!(
            !controller_should_continue(&mut stop_controller, &no_collision, "topic-a").unwrap()
        );
        let mut missing_fields = json!({});
        assert!(controller_should_continue(&mut missing_fields, &no_collision, "topic-a").is_err());
        let mut missing_steers = json!({
            "payload": {"soft_control": {"should_continue": true}}
        });
        assert!(controller_should_continue(&mut missing_steers, &no_collision, "topic-a").is_err());
    }

    #[test]
    fn controller_cannot_stop_before_direct_collision() {
        let topic_state = json!({"turns": [
            {"role": "researcher.bull.initial", "artifact": {"payload": {"claims": [{"claim_id": "topic-a:bull:1"}]}}},
            {"role": "researcher.bear.initial", "artifact": {"payload": {"claims": [{"claim_id": "topic-a:bear:1"}]}}}
        ]});
        let mut controller = json!({
            "payload": {
                "next_steers": [],
                "soft_control": {
                    "should_continue": false,
                    "stop_reason": "no new information"
                }
            }
        });
        assert!(controller_should_continue(&mut controller, &topic_state, "topic-a",).unwrap());
        assert_eq!(
            controller["payload"]["next_steers"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn controller_stop_is_repaired_into_two_initial_collision_routes() {
        let mut fields = serde_json::Map::from_iter([
            (
                "decision_hinges".to_owned(),
                json!([{"hinge": "20m versus 3h trend shift"}]),
            ),
            ("next_steers".to_owned(), json!([])),
            (
                "soft_control".to_owned(),
                json!({
                    "should_continue": false,
                    "stop_reason": "no new information"
                }),
            ),
        ]);
        let topic_state = json!({
            "topic": {"decision_hinge": "20m versus 3h trend shift"},
            "turns": [
                {"role": "researcher.bull.initial", "artifact": {"payload": {
                    "claims": [{"claim_id": "topic-a:bull:1"}]
                }}},
                {"role": "researcher.bear.initial", "artifact": {"payload": {
                    "claims": [{"claim_id": "topic-a:bear:1"}]
                }}}
            ]
        });

        assert!(ensure_initial_collision_route(&mut fields, &topic_state, "topic-a").unwrap());
        assert_eq!(fields["soft_control"]["should_continue"], true);
        assert_eq!(fields["next_steers"].as_array().unwrap().len(), 2);
        assert_eq!(
            fields["next_steers"][0]["reply_to_claim_id"],
            "topic-a:bear:1"
        );
        assert_eq!(
            fields["next_steers"][1]["reply_to_claim_id"],
            "topic-a:bull:1"
        );
    }

    #[test]
    fn phase2_seed_contract_rejects_claim_spam() {
        let fields = serde_json::Map::from_iter([(
            "claims".to_owned(),
            json!([
                {"claim": "one"}, {"claim": "two"}, {"claim": "three"}
            ]),
        )]);
        assert!(validate_phase2_compiled_contract("bull_seed", &fields, &[]).is_err());
    }

    #[test]
    fn phase2_seed_reports_nested_missing_fields_precisely() {
        let fields = serde_json::Map::from_iter([(
            "claims".to_owned(),
            json!([{"claim": "one", "evidence_refs": ["idx-123456"]}]),
        )]);
        let error = validate_phase2_compiled_contract(
            "bull_seed",
            &fields,
            &["claims[0].confidence".to_owned()],
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("claims[0].confidence"),
            "unexpected validation error: {error}"
        );
    }

    #[test]
    fn controller_contract_rejects_topic_generator_fallback_fields() {
        let fields = serde_json::Map::from_iter([("topics".to_owned(), json!([]))]);
        assert!(validate_phase2_compiled_contract("topic_control", &fields, &[]).is_err());
    }

    #[test]
    fn topic_control_normalizes_hinge_map_before_contract_validation() {
        let mut fields = serde_json::Map::from_iter([
            (
                "claim_ledger".to_owned(),
                json!([{
                    "claim_pair": "topic-a:bull:1 and topic-a:bear:1",
                    "status": "contested",
                    "evidence_refs": ["idx-123456"],
                    "reason": "direction remains disputed"
                }]),
            ),
            (
                "decision_hinges".to_owned(),
                json!({
                    "direction_conflict": {
                        "evidence_refs": ["idx-123456"],
                        "summary": "the observable direction boundary"
                    }
                }),
            ),
            ("next_steers".to_owned(), json!([])),
            (
                "soft_control".to_owned(),
                json!({
                    "should_continue": false,
                    "stop_reason": "the hinge is resolved"
                }),
            ),
        ]);

        normalize_phase2_topic_control_fields(&mut fields).unwrap();
        validate_phase2_compiled_contract("topic_control", &fields, &[]).unwrap();
        assert_eq!(
            fields["decision_hinges"][0],
            json!({
                "hinge": "direction_conflict",
                "evidence_refs": ["idx-123456"],
                "summary": "the observable direction boundary"
            })
        );
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
    fn phase1_cross_phase_detail_removes_model_invented_stable_ids() {
        let verified = "technical-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let invented = "technical-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let response = format!(
            "报告引用 {verified} 与 {invented}\n\n{}\n{}",
            orchestrator_llm::VERIFIED_PHASE1_EVIDENCE_MARKER,
            json!([verified])
        );

        let (projection, removals) = canonicalize_phase1_cross_phase_source(&response).unwrap();

        assert!(projection.contains(verified));
        assert!(!projection.contains(invented));
        assert!(projection.contains("unverified_phase1_reference_removed"));
        assert_eq!(removals, vec![json!({"prefix": "technical-"})]);
    }

    #[test]
    fn phase1_web_source_is_restored_from_the_verified_runtime_registry() {
        let response = format!(
            "报告\n\n{}\n{}",
            orchestrator_llm::tools::web_run::VERIFIED_RESULTS_MARKER,
            json!([{
                "evidence_id": "web-123456",
                "source_url": "https://example.com/fact"
            }])
        );
        let mut fields = json!({
            "per_ticker": {"QQQ": {"key_evidence": [{
                "source": "example.com",
                "evidence_refs": ["jin10-123456", "web-123456"]
            }]}}
        })
        .as_object()
        .unwrap()
        .clone();

        attach_verified_phase1_web_sources(&response, &mut fields).unwrap();

        assert_eq!(
            fields["per_ticker"]["QQQ"]["key_evidence"][0]["source"],
            "https://example.com/fact"
        );
    }

    #[test]
    fn phase1_verified_web_registry_keeps_runtime_results_not_selected_by_the_analyst() {
        let response = format!(
            "报告\n\n{}\n{}",
            orchestrator_llm::tools::web_run::VERIFIED_RESULTS_MARKER,
            json!([
                {
                    "evidence_id": "web-selected",
                    "source_url": "https://example.com/selected",
                    "published_at": "2026-08-03T00:00:00Z",
                    "title": "selected result"
                },
                {
                    "evidence_id": "web-visible-but-unselected",
                    "source_url": "https://example.com/unselected",
                    "published_at": null,
                    "title": "visible result"
                }
            ])
        );
        let mut fields = json!({
            "per_ticker": {"QQQ": {"key_evidence": [{
                "source": "example.com",
                "evidence_refs": ["web-selected"]
            }]}}
        })
        .as_object()
        .unwrap()
        .clone();

        attach_verified_phase1_web_sources(&response, &mut fields).unwrap();

        assert_eq!(
            fields["phase1_verified_web_evidence"]["authority"],
            "rust_verified_phase1_web_run_v1"
        );
        assert_eq!(
            fields["phase1_verified_web_evidence"]["records"],
            json!([
                {
                    "evidence_id": "web-selected",
                    "source_url": "https://example.com/selected",
                    "published_at": "2026-08-03T00:00:00Z",
                    "title": "selected result"
                },
                {
                    "evidence_id": "web-visible-but-unselected",
                    "source_url": "https://example.com/unselected",
                    "published_at": null,
                    "title": "visible result"
                }
            ])
        );
    }

    #[test]
    fn phase2_registry_allows_only_a_phase1_visible_verified_web_result() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = RunLocation::debug("2026-08-03", "phase2-visible-web").unwrap();
        let web_id = "web-visible-but-unselected";
        snapshot_test_index(
            &store,
            &location,
            1,
            "analyst.news_macro",
            "idx-phase1-visible-web",
            json!({
                "per_ticker": {"QQQ": {"key_evidence": []}},
                "phase1_verified_web_evidence": {
                    "authority": "rust_verified_phase1_web_run_v1",
                    "records": [{
                        "evidence_id": web_id,
                        "source_url": "https://example.com/unselected",
                        "published_at": null,
                        "title": "visible result"
                    }]
                }
            }),
        );

        let registry = phase2_initial_evidence_registry(&store, &location).unwrap();

        assert_eq!(
            registry.get(web_id),
            Some(&"url:httpsexamplecomunselected".to_owned())
        );
        assert!(!registry.contains_key("web-model-invented"));
    }

    #[test]
    fn phase1_verified_input_registry_keeps_runtime_results_not_selected_by_the_analyst() {
        let selected = "technical-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let unselected = "jin10-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let response = format!(
            "报告\n\n{}\n{}\n\n{}\n{}",
            orchestrator_llm::VERIFIED_PHASE1_EVIDENCE_RECORDS_MARKER,
            json!([
                {
                    "evidence_id": selected,
                    "event_time": "2026-08-03T15:00:00Z",
                    "as_of": "2026-08-03T15:00:00Z",
                    "timezone": "UTC"
                },
                {
                    "evidence_id": unselected,
                    "event_time": "2026-08-03T16:00:00Z",
                    "published_time": "2026-08-03T16:01:00Z",
                    "timezone": "Asia/Shanghai"
                }
            ]),
            orchestrator_llm::VERIFIED_PHASE1_EVIDENCE_MARKER,
            json!([selected, unselected])
        );
        let mut fields = json!({
            "per_ticker": {"QQQ": {"key_evidence": [{
                "source": "filestore.run_input.technical",
                "evidence_refs": [selected]
            }]}}
        })
        .as_object()
        .unwrap()
        .clone();

        attach_verified_phase1_web_sources(&response, &mut fields).unwrap();

        assert_eq!(
            fields["phase1_verified_input_evidence"]["authority"],
            "rust_verified_phase1_input_tool_v1"
        );
        assert_eq!(
            fields["phase1_verified_input_evidence"]["records"],
            json!([
                {
                    "evidence_id": unselected,
                    "source": "filestore.run_input.jin10",
                    "event_time": "2026-08-03T16:00:00Z",
                    "published_time": "2026-08-03T16:01:00Z",
                    "ingested_time": null,
                    "as_of": null,
                    "timezone": "Asia/Shanghai",
                    "time_metadata_available": true
                },
                {
                    "evidence_id": selected,
                    "source": "filestore.run_input.technical",
                    "event_time": "2026-08-03T15:00:00Z",
                    "published_time": null,
                    "ingested_time": null,
                    "as_of": "2026-08-03T15:00:00Z",
                    "timezone": "UTC",
                    "time_metadata_available": true
                }
            ])
        );
    }

    #[test]
    fn phase2_registry_allows_only_a_phase1_visible_verified_input_result() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = RunLocation::debug("2026-08-03", "phase2-visible-input").unwrap();
        let input_id = "technical-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        snapshot_test_index(
            &store,
            &location,
            1,
            "analyst.technical",
            "idx-phase1-visible-input",
            json!({
                "per_ticker": {"QQQ": {"key_evidence": []}},
                "phase1_verified_input_evidence": {
                    "authority": "rust_verified_phase1_input_tool_v1",
                    "records": [{
                        "evidence_id": input_id,
                        "source": "filestore.run_input.technical",
                        "event_time": "2026-08-03T15:00:00Z",
                        "published_time": null,
                        "ingested_time": null,
                        "as_of": "2026-08-03T15:00:00Z",
                        "timezone": "UTC",
                        "time_metadata_available": true
                    }]
                }
            }),
        );

        let registry = phase2_initial_evidence_registry(&store, &location).unwrap();

        assert_eq!(
            registry.get(input_id),
            Some(&format!("known-reference:{input_id}"))
        );
        assert!(!registry.contains_key("technical-model-invented"));
    }

    #[test]
    fn phase1_null_timestamp_is_restored_only_from_the_matching_verified_tool_record() {
        let technical =
            "technical-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let response = format!(
            "报告\n\n{}\n{}\n\n{}\n{}",
            orchestrator_llm::VERIFIED_PHASE1_EVIDENCE_RECORDS_MARKER,
            json!([{
                "evidence_id": technical,
                "event_time": "2026-08-03T15:00:00Z",
                "as_of": "2026-08-03T15:00:00Z",
                "timezone": "UTC"
            }]),
            orchestrator_llm::VERIFIED_PHASE1_EVIDENCE_MARKER,
            json!([technical])
        );
        let mut fields = json!({
            "per_ticker": {"QQQ": {"key_evidence": [{
                "claim": "verified technical claim",
                "source": "filestore.run_input.technical",
                "timestamp": null,
                "event_time": null,
                "published_time": null,
                "ingested_time": null,
                "as_of": null,
                "timezone": null,
                "evidence_refs": [technical]
            }]}}
        })
        .as_object()
        .unwrap()
        .clone();

        attach_verified_phase1_web_sources(&response, &mut fields).unwrap();

        let evidence = &fields["per_ticker"]["QQQ"]["key_evidence"][0];
        assert_eq!(evidence["timestamp"], "2026-08-03T15:00:00Z");
        assert_eq!(evidence["event_time"], "2026-08-03T15:00:00Z");
        assert_eq!(evidence["as_of"], "2026-08-03T15:00:00Z");
        assert_eq!(evidence["timezone"], "UTC");
        assert!(evidence.get("published_time").is_none());
        assert_eq!(
            fields["evidence_time_projection"]["projections"][0]["timestamp_clock"],
            "event_time"
        );
    }

    #[test]
    fn phase1_web_source_is_restored_for_cross_asset_findings_too() {
        let response = format!(
            "报告\n\n{}\n{}",
            orchestrator_llm::tools::web_run::VERIFIED_RESULTS_MARKER,
            json!([{
                "evidence_id": "web-123456",
                "source_url": "https://example.com/fact"
            }])
        );
        let mut fields = json!({
            "per_ticker": {"QQQ": {"key_evidence": [{
                "source": "jin10",
                "evidence_refs": ["jin10-123456"]
            }]}},
            "cross_asset_findings": [{"evidence_refs": ["web-123456"]}]
        })
        .as_object()
        .unwrap()
        .clone();

        attach_verified_phase1_web_sources(&response, &mut fields).unwrap();

        assert_eq!(
            fields["cross_asset_findings"][0]["source"],
            "https://example.com/fact"
        );
        validate_phase1_web_source_urls(&Value::Object(fields)).unwrap();
    }

    #[test]
    fn phase1_drops_bare_hashes_before_they_can_back_a_finding() {
        let valid = "jin10-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let bare_hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let response = format!(
            "报告\n\n{}\n{}",
            orchestrator_llm::VERIFIED_PHASE1_EVIDENCE_MARKER,
            json!([valid])
        );
        let mut fields = json!({
            "per_ticker": {"QQQ": {"key_evidence": [{
                "source": "FileStore",
                "evidence_refs": [valid, bare_hash]
            }]}}
        })
        .as_object()
        .unwrap()
        .clone();

        attach_verified_phase1_web_sources(&response, &mut fields).unwrap();

        assert_eq!(
            fields["per_ticker"]["QQQ"]["key_evidence"][0]["evidence_refs"],
            json!([valid])
        );
        assert_eq!(
            fields["evidence_normalization"]["unverified_malformed_refs_removed"],
            1
        );
    }

    #[test]
    fn phase1_drops_unverified_web_refs_without_guessing_an_id() {
        let response = format!(
            "报告\n\n{}\n[]",
            orchestrator_llm::tools::web_run::VERIFIED_RESULTS_MARKER
        );
        let mut fields = json!({
            "per_ticker": {"QQQ": {"key_evidence": [{
                "source": "jin10",
                "evidence_refs": [
                    "jin10-123456",
                    "web-ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                ]
            }]}}
        })
        .as_object()
        .unwrap()
        .clone();

        attach_verified_phase1_web_sources(&response, &mut fields).unwrap();

        assert_eq!(
            fields["per_ticker"]["QQQ"]["key_evidence"][0]["evidence_refs"],
            json!(["jin10-123456"])
        );
        assert_eq!(
            fields["evidence_normalization"]["unverified_web_refs_removed"],
            1
        );
    }

    #[test]
    fn phase1_drops_one_character_transcriptions_instead_of_repairing_them() {
        let response = format!(
            "报告\n\n{}\n{}",
            orchestrator_llm::VERIFIED_PHASE1_EVIDENCE_MARKER,
            json!([
                "technical-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "jin10-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ])
        );
        let mut fields = json!({
            "per_ticker": {"QQQ": {"key_evidence": [{
                "source": "FileStore",
                "evidence_refs": [
                    "technical-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab",
                    "jin10-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbc",
                    "technical-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "jin10-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                ]
            }]}}
        })
        .as_object()
        .unwrap()
        .clone();

        attach_verified_phase1_web_sources(&response, &mut fields).unwrap();

        assert_eq!(
            fields["per_ticker"]["QQQ"]["key_evidence"][0]["evidence_refs"],
            json!([
                "technical-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "jin10-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ])
        );
        assert_eq!(
            fields["evidence_normalization"]["unverified_technical_refs_removed"],
            1
        );
        assert_eq!(
            fields["evidence_normalization"]["unverified_jin10_refs_removed"],
            1
        );
        assert_eq!(
            fields["evidence_normalization"]["canonicalized_technical_refs"],
            0
        );
        assert_eq!(
            fields["evidence_normalization"]["canonicalized_jin10_refs"],
            0
        );
    }

    #[test]
    fn phase1_drops_extended_cross_asset_refs_instead_of_trimming_them() {
        let technical =
            "technical-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let response = format!(
            "报告\n\n{}\n{}",
            orchestrator_llm::VERIFIED_PHASE1_EVIDENCE_MARKER,
            json!([technical])
        );
        let mut fields = json!({
            "per_ticker": {"QQQ": {"key_evidence": [{
                "source": "FileStore",
                "evidence_refs": [technical]
            }]}},
            "cross_asset_findings": [{
                "evidence_refs": [format!("{technical}c")]
            }]
        })
        .as_object()
        .unwrap()
        .clone();

        attach_verified_phase1_web_sources(&response, &mut fields).unwrap();

        assert_eq!(fields["cross_asset_findings"], json!([]));
        assert_eq!(
            fields["evidence_normalization"]["canonicalized_technical_refs"],
            0
        );
    }

    #[test]
    fn phase1_does_not_guess_between_multiple_one_character_candidates() {
        let technical_a = format!("technical-a{}", "0".repeat(63));
        let technical_b = format!("technical-b{}", "0".repeat(63));
        let ambiguous = format!("technical-c{}", "0".repeat(63));
        let response = format!(
            "报告\n\n{}\n{}",
            orchestrator_llm::VERIFIED_PHASE1_EVIDENCE_MARKER,
            json!([technical_a.clone(), technical_b])
        );
        let mut fields = json!({
            "per_ticker": {"QQQ": {"key_evidence": [{
                "source": "FileStore",
                "evidence_refs": [ambiguous, technical_a.clone()]
            }]}}
        })
        .as_object()
        .unwrap()
        .clone();

        attach_verified_phase1_web_sources(&response, &mut fields).unwrap();

        assert_eq!(
            fields["per_ticker"]["QQQ"]["key_evidence"][0]["evidence_refs"],
            json!([technical_a])
        );
        assert_eq!(
            fields["evidence_normalization"]["unverified_technical_refs_removed"],
            1
        );
    }

    #[test]
    fn phase1_prunes_only_key_evidence_and_findings_without_verified_refs() {
        let valid = "jin10-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let invalid = "jin10-cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let response = format!(
            "报告\n\n{}\n{}",
            orchestrator_llm::VERIFIED_PHASE1_EVIDENCE_MARKER,
            json!([valid])
        );
        let mut fields = json!({
            "per_ticker": {"QQQ": {"key_evidence": [
                {"source": "FileStore", "evidence_refs": [invalid]},
                {"source": "FileStore", "evidence_refs": [valid]}
            ]}},
            "cross_asset_findings": [
                {"evidence_refs": [invalid]},
                {"evidence_refs": [valid]}
            ]
        })
        .as_object()
        .unwrap()
        .clone();

        attach_verified_phase1_web_sources(&response, &mut fields).unwrap();

        assert_eq!(
            fields["per_ticker"]["QQQ"]["key_evidence"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(fields["cross_asset_findings"].as_array().unwrap().len(), 1);
        assert_eq!(
            fields["evidence_normalization"]["unbacked_key_evidence_removed"],
            1
        );
        assert_eq!(
            fields["evidence_normalization"]["unbacked_cross_asset_findings_removed"],
            1
        );
    }

    #[test]
    fn phase1_allows_unobserved_ticker_without_verified_evidence_when_gap_is_declared() {
        let mut fields = json!({
            "per_ticker": {
                "VIX": {
                    "direction": "unobserved",
                    "long_probability": 0.5,
                    "data_gaps": ["没有可验证的 VIX 反应数据"],
                    "key_evidence": []
                }
            }
        });
        let mut normalization = Phase1ReferenceNormalization::default();

        prune_unbacked_phase1_findings(fields.as_object_mut().unwrap(), &mut normalization)
            .unwrap();
    }

    #[test]
    fn phase1_still_rejects_observed_ticker_without_verified_evidence() {
        let mut fields = json!({
            "per_ticker": {
                "QQQ": {
                    "direction": "mixed",
                    "long_probability": 0.5,
                    "data_gaps": ["缺少独立证据"],
                    "key_evidence": []
                }
            }
        });
        let mut normalization = Phase1ReferenceNormalization::default();

        let error =
            prune_unbacked_phase1_findings(fields.as_object_mut().unwrap(), &mut normalization)
                .unwrap_err();
        assert!(error
            .to_string()
            .contains("Phase 1 has no verified key evidence remaining for QQQ"));
    }

    #[test]
    fn phase3_drops_refs_that_do_not_resolve_to_persisted_source_indexes() {
        let valid = "idx-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let invalid = "idx-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab";
        let mut decision = json!({
            "decision_hinges": [{"hinge": "macro", "evidence_refs": [invalid, valid]}]
        })
        .as_object()
        .unwrap()
        .clone();

        project_phase3_evidence_refs(&mut decision, &BTreeSet::from([valid.to_owned()]));

        assert_eq!(
            decision["decision_hinges"][0]["evidence_refs"],
            json!([valid])
        );
        assert_eq!(
            decision["evidence_reference_projection"]["unverified_refs_removed"],
            1
        );
    }

    #[test]
    fn empty_compiled_details_remain_uncited_without_a_declared_source() {
        let index_ref = format!("idx-{}", "a".repeat(64));
        let details = vec![
            PhaseIndexCandidateDetail {
                section: "execution".to_owned(),
                detail: "phase detail".to_owned(),
                source_refs: Vec::new(),
            },
            PhaseIndexCandidateDetail {
                section: "execution".to_owned(),
                detail: "already cited".to_owned(),
                source_refs: vec![index_ref.clone()],
            },
        ];

        validate_declared_detail_source_refs(&details).unwrap();
        assert!(details[0].source_refs.is_empty());
        assert_eq!(details[1].source_refs, vec![index_ref]);
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
    fn parallel_state_deltas_preserve_order_and_only_merge_their_topic() {
        let mut state = json!({
            "role_job_metrics": [{"role": "prior"}],
            "errors": [{"kind": "prior"}],
            "_runtime_sessions": {"prior": {"turn_id": "prior"}},
            "_completed_units": {"prior": {"status": "ok"}},
            "degraded": false,
            "topic_debate_states": {
                "topic-a": {"status": "base"},
                "topic-b": {"status": "base"}
            }
        });
        let worker_a = json!({
            "role_job_metrics": [{"role": "prior"}, {"role": "a"}],
            "errors": [{"kind": "prior"}, {"kind": "a"}],
            "_runtime_sessions": {
                "prior": {"turn_id": "prior"},
                "a": {"turn_id": "a"}
            },
            "_completed_units": {
                "prior": {"status": "ok"},
                "a": {"status": "ok"}
            },
            "degraded": true,
            "topic_debate_states": {
                "topic-a": {"status": "updated-a"},
                "topic-b": {"status": "must-not-overwrite"}
            }
        });
        merge_parallel_state_delta(&mut state, &worker_a, 1, 1, Some("topic-a"));

        assert_eq!(state["role_job_metrics"].as_array().unwrap().len(), 2);
        assert_eq!(state["role_job_metrics"][1]["role"], "a");
        assert_eq!(state["errors"].as_array().unwrap().len(), 2);
        assert_eq!(state["_runtime_sessions"]["a"]["turn_id"], "a");
        assert_eq!(state["_completed_units"]["a"]["status"], "ok");
        assert_eq!(
            state["topic_debate_states"]["topic-a"]["status"],
            "updated-a"
        );
        assert_eq!(state["topic_debate_states"]["topic-b"]["status"], "base");
        assert!(state["degraded"].as_bool().unwrap());

        let worker_b = json!({
            "role_job_metrics": [{"role": "prior"}, {"role": "a"}, {"role": "b"}],
            "errors": [{"kind": "prior"}, {"kind": "a"}, {"kind": "b"}],
            "_runtime_sessions": {
                "prior": {"turn_id": "prior"},
                "a": {"turn_id": "a"},
                "b": {"turn_id": "b"}
            },
            "_completed_units": {
                "prior": {"status": "ok"},
                "a": {"status": "ok"},
                "b": {"status": "ok"}
            },
            "degraded": false,
            "topic_debate_states": {
                "topic-a": {"status": "must-not-overwrite"},
                "topic-b": {"status": "updated-b"}
            }
        });
        merge_parallel_state_delta(&mut state, &worker_b, 2, 2, Some("topic-b"));

        assert_eq!(
            state["role_job_metrics"]
                .as_array()
                .unwrap()
                .iter()
                .map(|metric| metric["role"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["prior", "a", "b"]
        );
        assert_eq!(state["errors"].as_array().unwrap().len(), 3);
        assert_eq!(
            state["topic_debate_states"]["topic-a"]["status"],
            "updated-a"
        );
        assert_eq!(
            state["topic_debate_states"]["topic-b"]["status"],
            "updated-b"
        );
    }

    #[test]
    fn manifest_projects_degraded_state_on_each_completed_phase() {
        let mut manifest = RunManifest::new(RunManifestInit {
            location: RunLocation::new("2026-07-27", "run-health-test").unwrap(),
            workflow_version: "test".to_owned(),
            prompt_versions: Default::default(),
            prompt_content_hash: "sha256:prompts".to_owned(),
            source_surface_hash: "sha256:source".to_owned(),
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
    fn failed_run_persists_failure_status_and_terminal_phase() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = RunLocation::new("2026-08-03", "run-failure-test").unwrap();
        let manifest = RunManifest::new(RunManifestInit {
            location: location.clone(),
            workflow_version: "test".to_owned(),
            prompt_versions: Default::default(),
            prompt_content_hash: "sha256:prompts".to_owned(),
            source_surface_hash: "sha256:source".to_owned(),
            git_sha: "a".repeat(40),
            config_hash: "sha256:test".to_owned(),
            role_profile_registry_hash: "sha256:test".to_owned(),
            created_at: "2026-08-03T00:00:00Z".to_owned(),
        })
        .unwrap();
        write_run_manifest(&store, &location, manifest).unwrap();
        let mut state = json!({
            "schema_version": 1,
            "run_id": "run-failure-test",
            "current_date": "2026-08-03",
            "ticker": "QQQ",
            "tickers": ["QQQ"],
            "analysis_universe": ["QQQ"],
            "store_root": directory.path(),
            "phase_status": {},
            "errors": [],
            "degraded": false
        });
        persist_state(&mut state).unwrap();

        let context = RunFailureContext {
            store,
            location: location.clone(),
        };
        let error = anyhow::anyhow!(
            "risk.conservative phase 5 produced no final Assistant text: upstream stream failed"
        );
        record_run_failure(Some(&context), &error);

        let manifest = read_run_manifest(&context.store, &location).unwrap();
        assert_eq!(manifest.status, orchestrator_store::RunStatus::Failed);
        assert_eq!(manifest.current_phase, 5);
        assert_eq!(manifest.phase_status["5"], PhaseStatus::Failed);
        assert_eq!(manifest.errors[0].phase, Some(5));
        assert_eq!(manifest.errors[0].code, "run_failed");
        let state = context
            .store
            .read_json_value(&location.state_relative())
            .unwrap();
        assert_eq!(state["phase_status"]["5"], "failed");
        assert_eq!(state["errors"][0]["kind"], "run_failed");
    }

    #[test]
    fn failure_phase_parser_ignores_unscoped_errors() {
        assert_eq!(
            phase_from_failure_message(
                "risk.conservative phase 5 produced no final Assistant text"
            ),
            Some(5)
        );
        assert_eq!(phase_from_failure_message("configuration is invalid"), None);
        assert_eq!(phase_from_failure_message("phase 9 is unsupported"), None);
    }

    #[test]
    fn finish_phase_persists_state_before_manifest_completion() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = RunLocation::new("2026-08-03", "run-finish-test").unwrap();
        let mut manifest = RunManifest::new(RunManifestInit {
            location: location.clone(),
            workflow_version: "test".to_owned(),
            prompt_versions: Default::default(),
            prompt_content_hash: "sha256:prompts".to_owned(),
            source_surface_hash: "sha256:source".to_owned(),
            git_sha: "a".repeat(40),
            config_hash: "sha256:test".to_owned(),
            role_profile_registry_hash: "sha256:test".to_owned(),
            created_at: "2026-08-03T00:00:00Z".to_owned(),
        })
        .unwrap();
        let mut state = json!({
            "schema_version": 1,
            "run_id": "run-finish-test",
            "current_date": "2026-08-03",
            "ticker": "QQQ",
            "tickers": ["QQQ"],
            "analysis_universe": ["QQQ"],
            "investable_assets": ["QQQ"],
            "store_root": directory.path(),
            "config": {},
            "storage_namespace": null,
            "phase_status": {},
            "degraded": false,
            "errors": []
        });

        finish_phase(&store, &location, &mut manifest, &mut state, 3, "done").unwrap();

        let persisted = store.read_json_value(&location.state_relative()).unwrap();
        assert_eq!(persisted["phase_status"]["3"], "done");
        assert_eq!(manifest.phase_status["3"], PhaseStatus::Completed);
    }

    #[test]
    fn completed_run_projection_uses_the_highest_completed_phase() {
        let mut manifest = RunManifest::new(RunManifestInit {
            location: RunLocation::new("2026-07-27", "run-phase-test").unwrap(),
            workflow_version: "test".to_owned(),
            prompt_versions: Default::default(),
            prompt_content_hash: "sha256:prompts".to_owned(),
            source_surface_hash: "sha256:source".to_owned(),
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
    fn degraded_phase_is_terminal_for_recovery() {
        let mut manifest = RunManifest::new(RunManifestInit {
            location: RunLocation::new("2026-08-03", "run-degraded-phase-test").unwrap(),
            workflow_version: "test".to_owned(),
            prompt_versions: Default::default(),
            prompt_content_hash: "sha256:prompts".to_owned(),
            source_surface_hash: "sha256:source".to_owned(),
            git_sha: "test".to_owned(),
            config_hash: "test".to_owned(),
            role_profile_registry_hash: "test".to_owned(),
            created_at: "2026-08-03T00:00:00Z".to_owned(),
        })
        .unwrap();
        manifest
            .phase_status
            .insert("7".to_owned(), PhaseStatus::Degraded);
        manifest
            .phase_status
            .insert("8".to_owned(), PhaseStatus::Degraded);

        assert!(phase_completed(&manifest, 7));
        assert!(phase_completed(&manifest, 8));
    }

    #[test]
    fn phase7_execution_requires_valid_allocation_and_two_explicit_paper_guards() {
        assert_eq!(
            phase7_execution_mode(false, false, true, true, true),
            Phase7ExecutionMode::BlockedAllocation
        );
        assert_eq!(
            phase7_execution_mode(false, false, false, false, true),
            Phase7ExecutionMode::PlannedConfigDisabled
        );
        assert_eq!(
            phase7_execution_mode(false, false, false, true, false),
            Phase7ExecutionMode::PlannedExplicitAuthorizationRequired
        );
        assert_eq!(
            phase7_execution_mode(false, false, false, true, true),
            Phase7ExecutionMode::SubmitPaper
        );
        assert_eq!(
            phase7_execution_mode(false, true, false, true, true),
            Phase7ExecutionMode::SimulatedDebug
        );
        assert_eq!(
            phase7_execution_mode(true, false, false, true, true),
            Phase7ExecutionMode::DisabledMock
        );
    }

    fn reflection_fixture_ref(document_id: &str) -> DocumentRef {
        DocumentRef {
            document_id: document_id.to_owned(),
            relative_path: format!("fixtures/{document_id}.json"),
            content_hash: format!("sha256:{document_id}"),
        }
    }

    fn eligible_reflection_fixture() -> (DecisionSnapshotV2, OutcomeRecordV1) {
        let policy = PolicyRef {
            policy_id: "fixture-policy".into(),
            version: 1,
            content_hash: "sha256:fixture-policy".into(),
        };
        let source = reflection_fixture_ref("source");
        let point = |session: &str, price: f64| PricePoint {
            session: session.to_owned(),
            price,
            source_ref: source.clone(),
        };
        let decision = DecisionSnapshotV2 {
            schema_version: orchestrator_core::DECISION_SNAPSHOT_SCHEMA_VERSION,
            decision_id: "decision-fixture".into(),
            source_run_id: "source-run".into(),
            ticker: "QQQ".into(),
            thesis: DecisionSection::Available {
                value: ThesisDecision {
                    artifact_ref: source.clone(),
                    direction: ForecastDirection::Up,
                    probability: 0.62,
                    horizon: "5 trading days".into(),
                    invalidation_conditions: vec!["loss of support".into()],
                },
            },
            trade: DecisionSection::Available {
                value: TradeDecision {
                    artifact_ref: source.clone(),
                    action: TradeAction::Buy,
                    entry_condition: Some("confirm breakout".into()),
                    position_size_ceiling: Some(0.1),
                    blockers: Vec::new(),
                },
            },
            risk: DecisionSection::Available {
                value: RiskDecision {
                    artifact_refs: vec![source.clone()],
                    direction_constraint: "long_only".into(),
                    max_target_weight: Some(0.1),
                    max_weight_delta: Some(0.1),
                    binding_controls: vec!["stop".into()],
                },
            },
            allocation: DecisionSection::Available {
                value: AllocationDecision {
                    artifact_ref: source.clone(),
                    current_weight: Some(0.0),
                    target_weight: Some(0.1),
                    cash_weight: Some(0.9),
                    allocation_policy_version: 1,
                },
            },
            execution_plan: DecisionSection::Available {
                value: ExecutionPlan {
                    status: ExecutionPlanStatus::Execute,
                    intended_action: TradeAction::Buy,
                    order_intent_refs: vec![source.clone()],
                    attributable_execution_expected: true,
                },
            },
            evaluation_spec: EvaluationSpec {
                evaluation_contract_id: "fixture-contract".into(),
                horizon_trading_days: 5,
                benchmark_policy_ref: policy.clone(),
                benchmark_selection: BenchmarkSelectionV1::Configured {
                    binding: BenchmarkBindingV1 {
                        benchmark_id: "SPY".into(),
                        provider: "fixture".into(),
                        price_basis: PriceBasis::AdjustedClose,
                        policy_ref: policy.clone(),
                    },
                },
                price_basis: PriceBasis::AdjustedClose,
                materialization_policy_ref: policy.clone(),
            },
            source_artifact_refs: vec![source.clone()],
            source_input_refs: vec![source.clone()],
            memory_usage_ref: MemoryUsageReferenceStatus::NotCaptured,
            run_purpose: RunPurpose::Paper,
            decided_at: "2026-08-01T00:00:00Z".into(),
            content_hash: "sha256:decision".into(),
        };
        let outcome = OutcomeRecordV1 {
            schema_version: orchestrator_core::OUTCOME_RECORD_SCHEMA_VERSION,
            outcome_id: "outcome-fixture".into(),
            evaluation_key: "fixture-key".into(),
            supersedes_outcome_id: None,
            decision_ref: source.clone(),
            ticker: "QQQ".into(),
            market: OutcomeSection::Available {
                value: MarketOutcome {
                    provider: "fixture".into(),
                    price_basis: PriceBasis::AdjustedClose,
                    adjustment_policy: AdjustmentPolicy::All,
                    anchor: point("2026-08-01", 100.0),
                    exit: point("2026-08-08", 105.0),
                    asset_return: 0.05,
                    max_adverse_excursion: -0.01,
                    corporate_action_resolved: true,
                },
            },
            benchmark: OutcomeSection::Available {
                value: BenchmarkOutcome {
                    benchmark_id: "SPY".into(),
                    benchmark_policy_ref: policy.clone(),
                    provider: "fixture".into(),
                    price_basis: PriceBasis::AdjustedClose,
                    anchor: point("2026-08-01", 200.0),
                    exit: point("2026-08-08", 202.0),
                    benchmark_return: 0.01,
                    excess_return: 0.04,
                },
            },
            allocation: OutcomeSection::Available {
                value: AllocationOutcome {
                    target_weight: 0.1,
                    current_weight: 0.0,
                    counterfactual_contribution: Some(0.004),
                    allocation_policy_ref: policy.clone(),
                },
            },
            execution: OutcomeSection::Available {
                value: ExecutionOutcome::Attributed {
                    order_refs: vec![source.clone()],
                    executed_price: 101.0,
                    executed_quantity: 1.0,
                    realized_pnl: Some(4.0),
                },
            },
            evaluation_input_manifest_ref: source.clone(),
            materialization_policy_ref: policy.clone(),
            benchmark_policy_ref: policy,
            materializer_version: 1,
            created_at: "2026-08-08T00:00:00Z".into(),
            content_hash: "sha256:outcome".into(),
        };
        (decision, outcome)
    }

    #[test]
    fn reflection_learning_requires_complete_plan_outcome_and_attribution() {
        let (mut decision, mut outcome) = eligible_reflection_fixture();
        assert!(reflection_learning_gap_reasons(&decision, &outcome).is_empty());

        outcome.benchmark = OutcomeSection::Unavailable {
            reason: OutcomeSectionUnavailableReason::DataIncomplete,
        };
        let benchmark_gaps = reflection_learning_gap_reasons(&decision, &outcome);
        assert!(benchmark_gaps.contains(&"benchmark_outcome_unavailable"));

        outcome.benchmark = eligible_reflection_fixture().1.benchmark;
        outcome.execution = OutcomeSection::NotApplicable;
        let execution_gaps = reflection_learning_gap_reasons(&decision, &outcome);
        assert!(execution_gaps.contains(&"attributable_execution_outcome_unavailable"));

        decision.risk = DecisionSection::NotApplicable;
        let decision_gaps = reflection_learning_gap_reasons(&decision, &outcome);
        assert!(decision_gaps.contains(&"decision_risk_unavailable"));
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
