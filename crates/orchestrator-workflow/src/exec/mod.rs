use anyhow::{bail, Context, Result};
use chrono::{Local, NaiveDate, Utc};
use orchestrator_core::{
    config_get, config_int, config_str, config_strings, default_project_root, display_ticker,
    load_config, parse_tickers, project_path, research_rating_for_probability,
    validate_analyst_ticker_artifact, validate_asset_execution_constraint,
    validate_research_decision, validate_risk_constraints, validate_trade_intent,
    AnalystTickerArtifact, AssetExecutionConstraint, BenchmarkBindingV1, BenchmarkSelectionV1,
    DecisionSection, DecisionSectionUnavailableReason, DecisionSnapshotV2, EvaluationSpec,
    MemoryPolicyV1, MemoryUsageReferenceStatus, PersistenceContextV1, PersistenceNamespace,
    PolicyRef, ReflectionTaskStatus, ResearchDecision, RiskConstraints, RunPurpose, StopType,
    TradeIntent, DECISION_SNAPSHOT_SCHEMA_VERSION,
};
use orchestrator_ingest::{jin10, technical};
use orchestrator_store::{
    append_index_detail, canonical_json_bytes, content_hash, create_index, finalize_index,
    read_all_indexes, read_indexes, read_run_manifest, write_run_manifest, AppendIndexDetailInput,
    CreateIndexInput, DetailSection, EvaluationStore, FileStore, FileStoreOptions, IndexKind,
    IndexQuery, IndexScope, ManifestError, RunCompactionMode, RunLocation, RunManifest,
    RunManifestInit, RunStatus, RunStore,
};
use serde_json::{json, Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
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
        commit_historical_reflection, prepare_role_job, record_role_job_metrics, run_role_jobs,
        RoleRun,
    },
    summary_store::{
        parse_phase_index_candidate, write_compiled_phase_index, PhaseIndexCandidate,
        PhaseIndexCandidateDetail,
    },
    summary_units::derive_summary_index_id,
    topic_debate_tree::{DebateActor, TopicDebateTree},
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
    if args.from_phase > args.to_phase || args.to_phase > 8 {
        bail!("phase range must satisfy 0 <= from_phase <= to_phase <= 8")
    }
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
    if store.exists(&location.manifest_relative())? {
        let manifest = read_run_manifest(store, location)?;
        let current_config_hash = content_hash(config)?;
        if manifest.config_hash != current_config_hash
            && location.storage_namespace() != Some("debug")
        {
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
            git_sha: resolve_git_sha(&default_project_root())?,
            config_hash: content_hash(config)?,
            role_profile_registry_hash: snapshot.content_hash,
            created_at: Utc::now().to_rfc3339(),
        })?,
    )
    .map_err(Into::into)
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
        // A Debug workspace is deliberately reusable across calendar days and
        // config edits. Its persisted market date/config remain its auditable
        // input context, but they are not part of its storage identity.
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
    matches!(
        manifest.phase_status.get(&phase.to_string()),
        Some(
            orchestrator_store::PhaseStatus::Completed | orchestrator_store::PhaseStatus::Degraded
        )
    )
}

fn has_phase3_retrieval_audit(state: &Value) -> bool {
    state
        .get("role_job_metrics")
        .and_then(Value::as_array)
        .and_then(|metrics| {
            metrics.iter().rev().find(|metric| {
                metric.get("phase").and_then(Value::as_i64) == Some(3)
                    && metric.get("role").and_then(Value::as_str) == Some("manager.research")
                    && metric.get("kind").and_then(Value::as_str) == Some("artifact")
            })
        })
        .and_then(|metric| metric.get("retrieval_audit"))
        .is_some_and(|audit| retrieval_audit_covers_required_source_phases(Some(audit)))
}

fn retrieval_audit_covers_required_source_phases(audit: Option<&Value>) -> bool {
    let Some(audit) = audit.and_then(Value::as_object) else {
        return false;
    };
    let visible_source_phases = audit
        .get("visible_source_phases")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    [1, 2].iter().all(|phase| {
        visible_source_phases
            .iter()
            .any(|value| value.as_i64() == Some(i64::from(*phase)))
    })
}

fn phase1_summaries_visible_to_phase3(
    store: &FileStore,
    location: &RunLocation,
    runtime: &RuntimeConfig,
) -> Result<bool> {
    phase_summary_visible_to_phase(store, location, runtime, 1, 3, 2)
}

fn phase_summary_visible_to_phase(
    store: &FileStore,
    location: &RunLocation,
    runtime: &RuntimeConfig,
    source_phase: u8,
    consumer_phase: u8,
    minimum_visible: usize,
) -> Result<bool> {
    let indexes = read_indexes(
        store,
        Some(location),
        &IndexQuery {
            kind: Some(IndexKind::PhaseSummary),
            source_phase: Some(source_phase),
            limit: runtime.tool_managed.max_summary_units_per_phase,
            ..Default::default()
        },
    )?
    .indexes;
    Ok(indexes
        .iter()
        .filter(|index| index.applies_to_phases.contains(&consumer_phase))
        .count()
        >= minimum_visible)
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

fn required_phase_index_count(_state: &Value, _runtime: &RuntimeConfig, phase: u8) -> usize {
    match phase {
        1 => 2,
        // Phase 2 has exactly one cross-phase Index: the reducer compiled
        // after every topic's Controller closure. Warmup, topic generation,
        // and individual stree turns are run-local transient state.
        2 => 1,
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
            "storage_namespace": input.storage_namespace,
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
    let phase1_evidence_registry = phase2_initial_evidence_registry(store, location)?;
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
    let generated_topics = generated
        .pointer("/payload/topics")
        .or_else(|| generated.get("topics"))
        .cloned()
        .unwrap_or_else(|| json!([]));
    let max_topics_per_side = state
        .get("max_topics_per_side")
        .and_then(Value::as_i64)
        .unwrap_or(3)
        .clamp(1, 20) as usize;
    let (topics, topic_selection) = select_phase2_topics(generated_topics, max_topics_per_side)?;
    let topic_generation_session =
        runtime_session_for(state, "mediator.topic", "topic_generation", None, None);
    state["topic_generation_session_id"] = topic_generation_session["session_id"].clone();
    state["topic_generation_turn_id"] = topic_generation_session["turn_id"].clone();
    let actionable = topics.as_array().is_some_and(|items| !items.is_empty());
    state["topic_generation_artifact"] = json!({"artifact": generated, "topics": topics, "actionable": actionable, "selection": topic_selection});

    let mut controllers = serde_json::Map::new();
    for topic in topics.as_array().into_iter().flatten() {
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
        tree.register_evidence_refs(phase1_evidence_registry.iter().map(String::as_str))?;
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
            state["_phase2_stree_injection"] = dispatch
                .delivery
                .as_ref()
                .map(|delivery| tree.injected_user_message(delivery))
                .transpose()?
                .map(Value::String)
                .unwrap_or(Value::Null);
            state["topic_debate_states"][&topic_id]["stree"] = serde_json::to_value(&tree)?;
            checkpoint_state(state)?;
            let mut artifact = match run_unit(
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
                    checkpoint_state(state)?;
                    continue;
                }
            };
            state["_phase2_stree_injection"] = Value::Null;
            // A natural-language response is not a completed STree turn. Give
            // the same persisted conversation one Rust-owned correction before
            // recording a tree failure: the model retains its analysis while
            // the retry can only finish through the required terminal tool.
            // This keeps a successful protocol repair out of the run's health
            // failure projection, while a second omission remains observable
            // and follows the bounded failure path below.
            if !state["mock"].as_bool().unwrap_or(false)
                && !phase2_stree_terminal_command_present(&artifact)
            {
                state["_phase2_stree_injection"] = Value::String(
                    phase2_terminal_tool_retry_injection(&topic_id, dispatch.actor),
                );
                artifact = match run_unit(
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
                        checkpoint_state(state)?;
                        continue;
                    }
                };
                state["_phase2_stree_injection"] = Value::Null;
            }
            if state["mock"].as_bool().unwrap_or(false) {
                apply_mock_phase2_stree_command(&mut tree, dispatch.actor)?;
            } else if let Err(error) =
                apply_phase2_stree_command(&mut tree, dispatch.actor, &artifact)
            {
                let error_text = error.to_string();
                if dispatch.actor == DebateActor::Controller
                    && error_text.contains("max_debate_rounds")
                {
                    tree.close_after_safety_limit()?;
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
            checkpoint_state(state)?;
        }
        controllers.insert(topic_id.clone(), tree.process_summary());
        state["topic_debate_states"][&topic_id]["final_controller_artifact"] = final_controller;
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

fn phase2_initial_evidence_registry(
    store: &FileStore,
    location: &RunLocation,
) -> Result<BTreeSet<String>> {
    let indexes = read_all_indexes(
        store,
        Some(location),
        &IndexQuery {
            kind: Some(IndexKind::PhaseSummary),
            source_phase: Some(1),
            ..IndexQuery::default()
        },
    )?;
    let mut references = BTreeSet::new();
    for index in indexes {
        references.insert(index.index_id);
        collect_reference_array_ids(&Value::Object(index.authoritative_fields), &mut references);
    }
    if references.is_empty() {
        bail!("Phase 2 requires persisted Phase 1 evidence provenance")
    }
    Ok(references)
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

#[allow(dead_code)] // Retained temporarily for the legacy Controller contract regression tests below.
fn record_phase2_controller_turn(state: &mut Value, topic_id: &str, round: i64, artifact: &Value) {
    let topic_state = &mut state["topic_debate_states"][topic_id];
    if topic_state
        .get("controller_turns")
        .and_then(Value::as_array)
        .is_none()
    {
        topic_state["controller_turns"] = json!([]);
    }
    let turns = topic_state["controller_turns"]
        .as_array_mut()
        .expect("controller turns initialized");
    let entry = json!({
        "role": "mediator.topic_controller",
        "round": round,
        "artifact": artifact,
    });
    if let Some(existing) = turns.iter_mut().find(|turn| {
        turn.get("round").and_then(Value::as_i64) == Some(round)
            && turn.get("role").and_then(Value::as_str) == Some("mediator.topic_controller")
    }) {
        *existing = entry;
    } else {
        turns.push(entry);
    }
}

fn apply_phase2_stree_command(
    tree: &mut TopicDebateTree,
    actor: DebateActor,
    artifact: &Value,
) -> Result<()> {
    register_stree_artifact_evidence_refs(tree, artifact)?;
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
            tree.controller_close(payload)?;
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
) -> Result<()> {
    let Some(references) = artifact.get("verified_evidence_refs") else {
        return Ok(());
    };
    let references = references
        .as_array()
        .context("Phase 2 terminal artifact verified_evidence_refs must be an array")?;
    let references = references
        .iter()
        .map(|reference| {
            reference
                .as_str()
                .context("Phase 2 terminal artifact verified_evidence_refs must contain strings")
        })
        .collect::<Result<Vec<_>>>()?;
    tree.register_evidence_refs(references)
}

fn phase2_stree_terminal_command_present(artifact: &Value) -> bool {
    artifact
        .pointer("/phase2_stree/command")
        .and_then(Value::as_str)
        .is_some_and(|command| !command.trim().is_empty())
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
                "report": "Mock Phase 2 stree participant report", "evidence_refs": []
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
        }));
    }

    json!({
        "kind": "phase2_debate_process_summary",
        "phase": 2,
        "status": "completed",
        "run_id": state.get("run_id").cloned().unwrap_or(Value::Null),
        "topic_generation": state
            .pointer("/topic_generation_artifact/topics")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "topic_count": topic_summaries.len(),
        "topics": topic_summaries,
        "final_controllers": final_controllers,
    })
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
                    "actor": actor, "end_turn": true, "stree": topic.get("stree").cloned().unwrap_or(Value::Null),
                    "stree_injections": topic.get("stree_injections").cloned().unwrap_or(Value::Null),
                    "final_controller": topic.get("final_controller").cloned().unwrap_or(Value::Null)
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

#[allow(dead_code)] // Legacy free-text Controller contract verifier; stree is now authoritative.
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
    let payload = final_decision_payload(state, decision_snapshots);
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
    let mut source_refs = state
        .get("allocation_artifact")
        .and_then(|artifact| artifact.get("index_id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .into_iter()
        .collect::<Vec<_>>();
    source_refs.extend(
        read_all_indexes(
            store,
            Some(location),
            &IndexQuery {
                kind: Some(IndexKind::PhaseSummary),
                source_phase: Some(6),
                ..Default::default()
            },
        )?
        .into_iter()
        .map(|index| index.index_id),
    );
    source_refs.sort();
    source_refs.dedup();
    append_index_detail(
        store,
        AppendIndexDetailInput {
            scope: scope.clone(),
            section: DetailSection::Execution,
            detail: serde_json::to_string(&payload)?,
            source_refs,
        },
    )?;
    finalize_index(store, &scope)?;
    summary_units.insert(unit_key, index_id);
    Ok(summary_units)
}

fn final_decision_payload(state: &Value, decision_snapshots: &BTreeMap<String, Value>) -> Value {
    json!({
        "final_trade_decision": state["final_trade_decision"],
        "allocation_context": state["allocation_context"],
        "portfolio_allocation": state["portfolio_allocation"],
        "allocation_result": state["allocation_result"],
        "account_snapshot": state.get("account_snapshot").cloned().unwrap_or(Value::Null),
        "order_plan": state.get("order_plan").cloned().unwrap_or(Value::Null),
        "execution_report": state.get("execution_report").cloned().unwrap_or(Value::Null),
        "decision_snapshots": decision_snapshots,
        "report_projection": crate::report::builder::report_projection(state),
    })
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
    let cacheable_unit = is_cacheable_unit(phase, kind);
    if !cacheable_unit {
        // A stree dispatch is a mailbox event, not a repeatable unit.  Its
        // session/turn identity is intentionally stable, but each delivery
        // must execute another loop against that existing history.
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
        compile_unit_response(
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
        .await?
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
    checkpoint_state(state)?;
    Ok(artifact)
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
        attach_verified_phase1_web_sources(response_text, &mut candidate.authoritative_fields)?;
        validate_phase1_compiled_fields(&candidate.authoritative_fields)?;
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
                let hash = orchestrator_store::content_hash_bytes(seed.as_bytes());
                topic_object.insert(
                    "topic_id".to_owned(),
                    Value::String(format!(
                        "topic-{}",
                        hash.strip_prefix("sha256:").unwrap_or(&hash)
                    )),
                );
            }
        }
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
    let topics = fields
        .get("topics")
        .and_then(Value::as_array)
        .context("Phase 2 topic_generation requires topics")?;
    for (index, topic) in topics.iter().enumerate() {
        let ttl = topic
            .get("ttl")
            .and_then(Value::as_str)
            .with_context(|| format!("Phase 2 topic {index} requires ttl"))?;
        if !matches!(ttl, "intraday" | "1-3d") {
            bail!(
                "Phase 2 topic {index} ttl {ttl:?} exceeds the supported 1-5 trading-day decision horizon"
            )
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

fn attach_verified_phase1_web_sources(
    response_text: &str,
    fields: &mut serde_json::Map<String, Value>,
) -> Result<()> {
    let verified_tool_ids = response_text
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
        .transpose()?;
    let marker = orchestrator_llm::tools::web_run::VERIFIED_RESULTS_MARKER;
    let registry = response_text
        .rsplit_once(marker)
        .map(|(_, registry_json)| {
            serde_json::from_str::<Vec<Value>>(registry_json.trim())
                .context("Rust-verified Web search result attachment is malformed")
        })
        .transpose()?
        .unwrap_or_default();
    let urls = registry
        .into_iter()
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
    for evidence in fields
        .get_mut("per_ticker")
        .and_then(Value::as_object_mut)
        .into_iter()
        .flat_map(|reports| reports.values_mut())
        .flat_map(|report| {
            report
                .get_mut("key_evidence")
                .and_then(Value::as_array_mut)
                .into_iter()
                .flatten()
        })
    {
        let Some(evidence) = evidence.as_object_mut() else {
            continue;
        };
        let first_web_ref = evidence
            .get("evidence_refs")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .find(|reference| reference.starts_with("web-"));
        let Some(web_ref) = first_web_ref else {
            continue;
        };
        let source_url = urls
            .get(web_ref)
            .expect("unverified Web references were removed above");
        evidence.insert("source".to_owned(), Value::String(source_url.clone()));
    }
    fields.insert(
        "evidence_normalization".to_owned(),
        json!({
            "authority": "rust",
            "unverified_web_refs_removed": normalization.unverified_web_refs_removed,
            "unverified_technical_refs_removed": normalization.unverified_technical_refs_removed,
            "unverified_jin10_refs_removed": normalization.unverified_jin10_refs_removed,
            "canonicalized_web_refs": normalization.canonicalized_web_refs,
            "canonicalized_technical_refs": normalization.canonicalized_technical_refs,
            "canonicalized_jin10_refs": normalization.canonicalized_jin10_refs,
            "unbacked_key_evidence_removed": normalization.unbacked_key_evidence_removed,
            "unbacked_cross_asset_findings_removed": normalization.unbacked_cross_asset_findings_removed,
        }),
    );
    Ok(())
}

#[derive(Default)]
struct Phase1ReferenceNormalization {
    unverified_web_refs_removed: usize,
    unverified_technical_refs_removed: usize,
    unverified_jin10_refs_removed: usize,
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
                bail!("Phase 1 has no verified key evidence remaining for {ticker}");
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
    Some(reference.to_owned())
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
    let authoritative_fields = match phase {
        0 => json!({"disposition":"no_reusable_memory","source_index_ids":[]}),
        1 => json!({"per_ticker": analysis.into_iter().map(|ticker| (
            ticker,
            json!({
                "direction":"neutral","confidence":0.5,"priced_in":"unclear",
                "report":response_text,"key_evidence":[{
                    "claim":"mock evidence is explicitly non-live","evidence_type":"inference",
                    "source":"mock fixture","timestamp":"1970-01-01T00:00:00Z",
                    "source_tier":"unknown","first_source":"mock fixture",
                    "is_derivative_repost":false,"evidence_age":"unknown","source_confidence":0.0,
                    "evidence_refs":["technical-0000000000000000000000000000000000000000000000000000000000000000"]
                }],"validation_triggers":[],
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
                    "scenarios":{
                        "bull":{"probability":0.25,"drivers":["mock upside driver"],"triggers":["mock upside trigger"],"confirmation":"mock upside confirmation"},
                        "base":{"probability":0.50,"drivers":["mock base driver"],"triggers":["mock base trigger"],"confirmation":"mock base confirmation"},
                        "bear":{"probability":0.25,"drivers":["mock downside driver"],"triggers":["mock downside trigger"],"confirmation":"mock downside confirmation"}
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
    let mut values = serde_json::Map::new();
    for ticker in investable_assets_from_state(state) {
        let mut contributions = Vec::new();
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
            let long_probability = match direction {
                "bullish" => confidence,
                "bearish" => 1.0 - confidence,
                "neutral" | "mixed" | "unobserved" => 0.5,
                other => bail!("{role} direction {other:?} is invalid for {ticker}"),
            };
            contributions.push(json!({
                "role": role,
                "direction": direction,
                "confidence": confidence,
                "long_probability": round_probability(long_probability),
            }));
        }
        if contributions.is_empty() {
            bail!("weighted probability base has no Phase 1 contributions for {ticker}")
        }
        let long_probability = contributions
            .iter()
            .filter_map(|item| item.get("long_probability").and_then(Value::as_f64))
            .sum::<f64>()
            / contributions.len() as f64;
        let long_probability = round_probability(long_probability);
        values.insert(
            ticker,
            json!({
                "long_probability": long_probability,
                "short_probability": round_probability(1.0 - long_probability),
                "source": "phase1_direction_confidence_v1",
                "weighting": "equal_role_mean",
                "contributions": contributions,
            }),
        );
    }
    Ok(Value::Object(values))
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
        for evidence in &canonical.key_evidence {
            if evidence.evidence_refs.is_empty() {
                bail!("Phase 1 evidence for {ticker} requires at least one stable evidence_refs ID")
            }
        }
    }
    let fields_value = Value::Object(fields.clone());
    validate_phase1_reference_arrays(&fields_value, None)?;
    if contains_phase1_web_ref(&fields_value)
        && !per_ticker.values().any(|report| {
            report
                .get("key_evidence")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|evidence| evidence.get("source").and_then(Value::as_str))
                .any(|source| source.starts_with("https://") || source.starts_with("http://"))
        })
    {
        bail!("Phase 1 web evidence requires an authoritative http(s) source URL")
    }
    Ok(())
}

fn contains_phase1_web_ref(value: &Value) -> bool {
    match value {
        Value::String(value) => value.starts_with("web-"),
        Value::Array(values) => values.iter().any(contains_phase1_web_ref),
        Value::Object(values) => values.values().any(contains_phase1_web_ref),
        _ => false,
    }
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
            let hinges = decision
                .get("decision_hinges")
                .and_then(Value::as_array)
                .with_context(|| {
                    format!(
                        "non-zero Phase 3 debate adjustment requires decision_hinges for {ticker}"
                    )
                })?;
            if hinges.is_empty()
                || hinges.iter().any(|hinge| {
                    hinge
                        .get("evidence_refs")
                        .and_then(Value::as_array)
                        .is_none_or(|refs| {
                            refs.is_empty()
                                || refs.iter().any(|reference| {
                                    reference.as_str().is_none_or(|reference| {
                                        reference.trim().is_empty()
                                            || reference.contains("...")
                                            || reference.starts_with("web.run:search")
                                    })
                                })
                        })
                })
            {
                bail!(
                    "non-zero Phase 3 debate adjustment requires complete stable evidence_refs for {ticker}"
                )
            }
        }
        if decision.get("scenarios").is_none_or(Value::is_null) {
            bail!("Phase 3 scenarios are required for {ticker}")
        }
        project_phase3_scenario_probabilities(decision, long, &ticker)?;
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
                collect_reference_array_ids(
                    &Value::Object(index.authoritative_fields),
                    &mut verified,
                );
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

fn project_phase3_scenario_probabilities(
    decision: &mut serde_json::Map<String, Value>,
    long_probability: f64,
    ticker: &str,
) -> Result<()> {
    let (model_bull, model_base, model_bear) = {
        let scenarios = decision
            .get_mut("scenarios")
            .and_then(Value::as_object_mut)
            .with_context(|| format!("Phase 3 scenarios must be an object for {ticker}"))?;
        let probability = |scenario: &str| -> Result<f64> {
            scenarios
                .get(scenario)
                .and_then(Value::as_object)
                .and_then(|value| value.get("probability"))
                .and_then(Value::as_f64)
                .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
                .with_context(|| {
                    format!("Phase 3 {ticker} scenario {scenario} probability is invalid")
                })
        };
        (
            probability("bull")?,
            probability("base")?,
            probability("bear")?,
        )
    };
    let max_base = 2.0 * long_probability.min(1.0 - long_probability);
    let projected_base = round_probability(model_base.min(max_base));
    let projected_bull = round_probability(long_probability - 0.5 * projected_base);
    let projected_bear = round_probability(1.0 - long_probability - 0.5 * projected_base);
    let scenarios = decision
        .get_mut("scenarios")
        .and_then(Value::as_object_mut)
        .expect("Phase 3 scenarios checked above");
    for (scenario, probability) in [
        ("bull", projected_bull),
        ("base", projected_base),
        ("bear", projected_bear),
    ] {
        scenarios
            .get_mut(scenario)
            .and_then(Value::as_object_mut)
            .with_context(|| format!("Phase 3 {ticker} scenario {scenario} must be an object"))?
            .insert("probability".to_owned(), json!(probability));
    }
    decision.insert(
        "scenario_probability_projection".to_owned(),
        json!({
            "authority": "rust",
            "model": {"bull": model_bull, "base": model_base, "bear": model_bear},
            "projected": {"bull": projected_bull, "base": projected_base, "bear": projected_bear},
            "base_probability_capped": model_base > max_base,
            "identity": "long = bull + 0.5 * base; bull + base + bear = 1",
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
        let expected_candidate = research_plan_to_trade_intent(research)["candidate_action"]
            .as_str()
            .context("Rust-owned candidate action missing")?
            .to_owned();
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
        .unwrap_or("");
    let adjustment = fields
        .get("recommended_adjustment")
        .and_then(Value::as_str)
        .unwrap_or("");
    if !no_new_information && (unique.trim().is_empty() || adjustment.trim().is_empty()) {
        bail!("Phase 5 new information requires a contribution and recommended adjustment")
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
            unique_risk_contribution: unique.to_owned(),
            disagreement_with_prior: disagreement.to_owned(),
            no_new_information,
            recommended_adjustment: adjustment.to_owned(),
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

        let controls = decision
            .get_mut("binding_risk_controls")
            .and_then(Value::as_array_mut)
            .with_context(|| format!("Phase 6 binding_risk_controls missing for {ticker}"))?;
        let mut control_source_projections = Vec::with_capacity(controls.len());
        for control in controls.iter_mut() {
            if let Some(text) = control.as_str() {
                *control = json!({
                    "control": text,
                    "source_refs": phase5_refs.iter().cloned().collect::<Vec<_>>()
                });
            }
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
                .cloned()
                .unwrap_or_default();
            let projected_refs = phase5_refs.iter().cloned().collect::<Vec<_>>();
            object.insert("source_refs".to_owned(), json!(projected_refs));
            control_source_projections.push(json!({
                "control": control_text,
                "model_source_refs": model_refs,
                "projected_source_refs": phase5_refs.iter().cloned().collect::<Vec<_>>(),
            }));
        }
        decision.insert(
            "risk_control_source_projection".to_owned(),
            json!({
                "authority": "rust",
                "controls": control_source_projections,
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
    use orchestrator_core::{MemoryPolicyV1, PolicyRef};
    use orchestrator_store::{
        content_hash, FileStore, FileStoreOptions, PhaseStatus, RunLocation, RunManifest,
        RunManifestInit,
    };
    use serde_json::{json, Value};
    use std::collections::BTreeSet;
    use tempfile::tempdir;

    use crate::orchestration::summary_store::PhaseIndexCandidateDetail;

    use super::{
        apply_phase2_stree_command, attach_verified_phase1_web_sources,
        attach_verified_web_evidence, controller_should_continue, defers_phase_summary,
        enrich_and_validate_phase6_compiled_fields, enrich_final_trade_decision_fields,
        ensure_initial_collision_route, final_decision_payload, finish_phase,
        highest_completed_phase, is_cacheable_unit, load_or_initialize_state,
        normalize_phase2_topic_control_fields, persist_state, persists_phase_index,
        phase2_debate_debug_summary, phase2_stree_terminal_command_present,
        phase2_terminal_tool_retry_injection, phase7_execution_mode, phase_completed,
        project_phase2_final_fields, project_phase3_evidence_refs, prompt_owner_for_unit,
        record_phase2_runtime_failure, record_phase2_session, redacted_config_for_state,
        resolve_git_sha, runtime_session_key, scoped_state_for_unit, select_phase2_topics,
        select_reflection_task_budget, sync_manifest_health, validate_declared_detail_source_refs,
        validate_phase1_compiled_fields, validate_phase2_compiled_contract,
        validate_phase2_topic_ttls, validate_phase3_compiled_fields,
        validate_phase4_compiled_fields, validate_phase5_compiled_fields,
        weighted_probability_base, DebateActor, Phase7ExecutionMode, TopicDebateTree,
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

    #[test]
    fn weighted_probability_base_uses_phase1_direction_and_confidence() {
        let state = json!({
            "investable_assets": ["QQQ", "SOXX"],
            "analyst_reports": {
                "analyst.technical": {"per_ticker": {
                    "QQQ": {"direction": "bearish", "confidence": 0.60},
                    "SOXX": {"direction": "bearish", "confidence": 0.78}
                }},
                "analyst.news_macro": {"per_ticker": {
                    "QQQ": {"direction": "mixed", "confidence": 0.54},
                    "SOXX": {"direction": "mixed", "confidence": 0.57}
                }}
            }
        });

        let base = weighted_probability_base(&state).unwrap();

        assert_eq!(base["QQQ"]["long_probability"], 0.45);
        assert_eq!(base["QQQ"]["short_probability"], 0.55);
        assert_eq!(base["SOXX"]["long_probability"], 0.36);
        assert_eq!(base["SOXX"]["short_probability"], 0.64);
        assert_eq!(base["QQQ"]["source"], "phase1_direction_confidence_v1");
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
    fn phase1_rejects_raw_hashes_and_local_web_result_numbers() {
        let fields = json!({
            "per_ticker": {"QQQ": {
                "direction": "mixed", "confidence": 0.5, "report": "mixed",
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
                "direction": "mixed", "confidence": 0.5, "report": "mixed",
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

    fn canonical_phase3_fields() -> serde_json::Map<String, Value> {
        json!({
            "decisions": {
                "QQQ": {
                    "rating": "Underweight",
                    "long_probability": 0.44,
                    "short_probability": 0.56,
                    "base_probability": 0.45,
                    "debate_adjustment": -0.01,
                    "confidence_basis": "directional_evidence",
                    "hold_reason": null,
                    "plan": "Maintain the evidence-bounded downside plan.",
                    "probability_rationale": "The validated debate moved the Phase 1 base by one point.",
                    "scenarios": {
                        "bull": {"probability": 0.19, "drivers": ["breadth recovery"], "triggers": ["3h breakout"], "confirmation": "price confirms"},
                        "base": {"probability": 0.50, "drivers": ["range continuation"], "triggers": ["range persists"], "confirmation": "range holds"},
                        "bear": {"probability": 0.31, "drivers": ["downtrend continuation"], "triggers": ["20m breakdown"], "confirmation": "lower low confirms"}
                    },
                    "decision_hinges": [{"hinge": "validated collision", "evidence_refs": ["idx-123456"]}],
                    "validation_plan": ["observe the cited hinge"]
                }
            }
        })
        .as_object()
        .unwrap()
        .clone()
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
        let state = json!({
            "investable_assets": ["QQQ"],
            "weighted_probability_base": {"QQQ": {"long_probability": 0.45}}
        });
        let mut fields = canonical_phase3_fields();

        validate_phase3_compiled_fields(&state, &mut fields).unwrap();
    }

    #[test]
    fn phase3_projects_the_rust_owned_base_probability() {
        let state = json!({
            "investable_assets": ["QQQ"],
            "weighted_probability_base": {"QQQ": {"long_probability": 0.45}}
        });
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
        let state = json!({
            "investable_assets": ["QQQ"],
            "weighted_probability_base": {"QQQ": {"long_probability": 0.45}}
        });
        let mut fields = canonical_phase3_fields();
        fields["decisions"]["QQQ"]["rating"] = json!("Hold");
        fields["decisions"]["QQQ"]["hold_reason"] = json!("conflicting_evidence");
        fields["decisions"]["QQQ"]["scenarios"]["bull"]["probability"] = json!(0.1);
        fields["decisions"]["QQQ"]["scenarios"]["base"]["probability"] = json!(0.2);
        fields["decisions"]["QQQ"]["scenarios"]["bear"]["probability"] = json!(0.7);

        validate_phase3_compiled_fields(&state, &mut fields).unwrap();

        assert_eq!(fields["decisions"]["QQQ"]["rating"], "Underweight");
        assert_eq!(fields["decisions"]["QQQ"]["hold_reason"], Value::Null);
        assert_eq!(
            fields["decisions"]["QQQ"]["rating_projection"]["overridden"],
            true
        );
        assert_eq!(
            fields["decisions"]["QQQ"]["scenarios"]["bull"]["probability"],
            0.34
        );
        assert_eq!(
            fields["decisions"]["QQQ"]["scenarios"]["base"]["probability"],
            0.2
        );
        assert_eq!(
            fields["decisions"]["QQQ"]["scenarios"]["bear"]["probability"],
            0.46
        );
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
    fn phase5_rejects_an_undeclared_missing_constraint() {
        let state = json!({"investable_assets": ["QQQ"]});
        let mut fields = json!({
            "stance": "neutral",
            "unique_risk_contribution": "gap risk",
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
            "phase2_stree": {
                "command": "submit_debate_turn",
                "payload": {
                    "stance": "needs_evidence",
                    "message": "Use the verified source before concluding.",
                    "report": "Participant records an evidence gap.",
                    "evidence_refs": [evidence_id]
                }
            }
        });

        apply_phase2_stree_command(&mut tree, actor, &artifact).unwrap();

        assert!(tree.evidence_registry.contains(evidence_id));
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
            "risk_debate_state": {"history": [{
                "index_id": "idx-risk01",
                "payload": {"missing_fields": ["QQQ.max_drawdown_pct"]}
            }]}
        });
        let mut fields = json!({"per_asset": {"QQQ": {
            "direction_constraint": "decrease_only",
            "execution_status": "execute",
            "max_target_weight": 0.20,
            "max_weight_delta": 0.10,
            "binding_risk_controls": ["reduce on a confirmed breakdown"],
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
        assert_eq!(decision["execution_status"], "downgrade");
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
            "risk_debate_state": {"history": [{
                "index_id": "idx-risk01", "payload": {"missing_fields": []}
            }]}
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
    fn finish_phase_persists_state_before_manifest_completion() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = RunLocation::new("2026-08-03", "run-finish-test").unwrap();
        let mut manifest = RunManifest::new(RunManifestInit {
            location: location.clone(),
            workflow_version: "test".to_owned(),
            prompt_versions: Default::default(),
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
