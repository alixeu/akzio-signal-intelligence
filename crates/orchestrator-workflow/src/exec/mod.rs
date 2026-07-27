use anyhow::{bail, Context, Result};
use chrono::{Local, NaiveDate, Utc};
use orchestrator_core::{
    config_int, config_str, config_strings, default_project_root, display_ticker, load_config,
    parse_tickers, project_path, research_rating_for_probability, ArtifactAuthority, MarketRegime,
    ToolManagedProfile,
};
use orchestrator_sql::{
    archive::{upsert_run_archive, RunArchiveInput},
    clear_agent_loop_history, connect, pending_reflection_tasks, persist_reflection_artifact,
    prediction::{upsert_prediction, PredictionInput},
    score_mature_predictions, set_reflection_task_status, set_run_current_phase, update_run_status,
    upsert_decision_snapshot, write_run_record, DecisionSnapshotInput, ReflectionThresholds,
    RunRecordInput, AGGREGATE_TICKER,
};
use orchestrator_store::{
    content_hash, read_learning_record, read_run_manifest, write_learning_record,
    write_run_manifest, FileStore, FileStoreOptions, LearningKind, LearningRecord, RunLocation,
    RunManifest, RunManifestInit, LEARNING_RECORD_SCHEMA_VERSION,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tracing::debug;

use crate::orchestration::allocation::{
    allocation_prompt_context, compute_allocation_context, derive_guarded_allocation,
};
use crate::orchestration::artifact::market_truth_violation_report;
use crate::orchestration::artifact::{
    build_debate_state_artifact, build_phase1_index, build_topic_generation_artifact,
    materialize_weighted_probability_base, persist_artifact, persist_artifact_with_last_md,
    persist_message, persist_message_with_topic, reducer_brief_md, topic_id_from_topic,
    topics_from_generation_artifact,
};
use crate::orchestration::config::{is_critical_role, validate_sqlite_context, RuntimeConfig};
use crate::orchestration::degraded::{record_degraded_role, role_artifact_or_degraded};
use crate::orchestration::domain_runtime::{
    finalize_degraded_analyst_report, finalize_degraded_portfolio_decision,
    finalize_degraded_research_decision, finalize_degraded_risk_review,
    finalize_degraded_trade_intent, FileStoreDomainRuntimePlan,
};
use crate::orchestration::input_snapshot_runtime::{
    capture_phase1_file_store_inputs, phase1_input_sources,
};
use crate::orchestration::lifecycle::{
    append_topic_controller_artifact, append_topic_turn, record_contracts,
    research_plan_to_trade_intent, run_id_for, set_phase_status, set_topic_controller_state,
    tickers_from_state, upsert_topic_debate_state,
};
use crate::orchestration::policy::{
    enforce_preflight_policy, run_file_store_phase1_preflight, run_phase1_preflight,
};
use crate::orchestration::policy::{
    evaluate_workflow_policy, record_workflow_policy, WorkflowPolicyDecision, WorkflowPolicyMode,
    WorkflowPolicySignals,
};
use crate::orchestration::render::mode_prompt_path;
use crate::orchestration::retrieval::inject_phase_summary_reflection;
use crate::orchestration::role_jobs::{
    merge_role_job_metrics, persist_prompt_metric, prepare_role_job, record_role_job_metrics,
    run_role_jobs, run_single_role_job, run_single_role_job_result, run_single_steer_role_job,
    RoleRun, SteerRoleRun,
};
use crate::orchestration::summary_store::write_deterministic_phase_summary;
use orchestrator_core::role_registry::DEFAULT_PHASE1_AGENTS;
use rusqlite::{params, OptionalExtension};

mod args;
pub use args::*;

type TopicDebateResult = (String, Vec<Value>, Value, Value);

const PHASE2_TOPIC_FORK_USER_PROMPT: &str =
    include_str!("../../../../prompts/phase2/messages/topic_fork_user.md");

struct PhaseTimer {
    phase: i64,
    label: &'static str,
    started_at: Instant,
}

fn is_mock(state: &Value) -> bool {
    state.get("mock").and_then(Value::as_bool).unwrap_or(false)
}

fn has_file_store_authority(runtime_config: &RuntimeConfig) -> bool {
    runtime_config
        .authority_registry
        .registrations()
        .any(|registration| registration.authority == ArtifactAuthority::FileStore)
}

/// The registry, rather than a best-effort write result, is the source of
/// truth for whether the legacy phase-summary database may be touched.
fn phase_summary_uses_file_store(runtime_config: &RuntimeConfig) -> Result<bool> {
    Ok(runtime_config
        .authority_registry
        .authority_for("compressor.phase_summary", ToolManagedProfile::PhaseSummary)?
        == ArtifactAuthority::FileStore)
}

/// Phase 1 has a per-role/per-ticker authority boundary.  This lookup is the
/// only place the workflow decides whether an Analyst unit may touch legacy
/// persistence; a failed FileStore unit must never query or reuse SQLite.
fn analyst_uses_file_store(runtime_config: &RuntimeConfig, role: &str) -> Result<bool> {
    Ok(runtime_config
        .authority_registry
        .authority_for(role, ToolManagedProfile::AnalystReport)?
        == ArtifactAuthority::FileStore)
}

fn phase1_has_file_store_analyst(runtime_config: &RuntimeConfig, roles: &[String]) -> Result<bool> {
    roles
        .iter()
        .map(|role| analyst_uses_file_store(runtime_config, role))
        .try_fold(false, |found, migrated| {
            migrated.map(|migrated| found || migrated)
        })
}

/// A migrated Phase 3 manager is FileStore-only.  Its failure path therefore
/// finalizes a typed degraded Draft rather than producing a legacy JSON
/// fallback or touching the SQLite role-artifact cache.
fn research_uses_file_store(runtime_config: &RuntimeConfig) -> Result<bool> {
    Ok(runtime_config
        .authority_registry
        .authority_for("manager.research", ToolManagedProfile::ResearchDecision)?
        == ArtifactAuthority::FileStore)
}

fn profile_uses_file_store(
    runtime_config: &RuntimeConfig,
    role: &str,
    profile: ToolManagedProfile,
) -> Result<bool> {
    Ok(runtime_config
        .authority_registry
        .authority_for(role, profile)?
        == ArtifactAuthority::FileStore)
}

/// Phase 2 is all-or-nothing.  Mixed authority would let a controller read a
/// SQLite packet emitted by a FileStore seed (or vice versa), so reject a
/// partial registry rather than falling back across stores.
fn phase2_uses_file_store(runtime_config: &RuntimeConfig) -> Result<bool> {
    let registrations = [
        ("mediator.topic", ToolManagedProfile::ResearcherWarmup),
        ("mediator.topic", ToolManagedProfile::TopicGeneration),
        ("researcher.bull.initial", ToolManagedProfile::DebateSeed),
        ("researcher.bear.initial", ToolManagedProfile::DebateSeed),
        (
            "researcher.bull.interaction",
            ToolManagedProfile::DebateResponse,
        ),
        (
            "researcher.bear.interaction",
            ToolManagedProfile::DebateResponse,
        ),
        (
            "mediator.topic_controller",
            ToolManagedProfile::TopicControl,
        ),
    ];
    let mut values = registrations
        .into_iter()
        .map(|(role, profile)| {
            runtime_config
                .authority_registry
                .authority_for(role, profile)
                .map_err(anyhow::Error::from)
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(|authority| authority == ArtifactAuthority::FileStore);
    let first = values.next().unwrap_or(false);
    if values.any(|value| value != first) {
        bail!("Phase 2 authority registry must migrate every Phase 2 profile together")
    }
    Ok(first)
}

/// The manifest is part of the FileStore authority, not a second run ledger
/// for legacy-only execution. Until a profile migrates, the legacy path stays
/// the sole persistence authority and this function must not touch the store
/// root at all.
fn prepare_file_store_run_manifest_if_migrated(
    store_root: &Path,
    runtime_config: &RuntimeConfig,
    config: &Value,
    current_date: &str,
    run_id: &str,
) -> Result<Option<RunManifest>> {
    if !has_file_store_authority(runtime_config) {
        return Ok(None);
    }
    prepare_file_store_run_manifest(store_root, runtime_config, config, current_date, run_id)
        .map(Some)
}

/// Prepare the one FileStore-owned run manifest before any legacy workflow
/// write, once at least one profile has explicitly migrated. It records no
/// Artifacts itself, so it never creates a second persistence path for a role.
fn prepare_file_store_run_manifest(
    store_root: &Path,
    runtime_config: &RuntimeConfig,
    config: &Value,
    current_date: &str,
    run_id: &str,
) -> Result<RunManifest> {
    let location = RunLocation::new(current_date, run_id)?;
    let authority_snapshot = runtime_config.authority_registry.snapshot();
    authority_snapshot.verify()?;
    let manifest_init = RunManifestInit {
        location: location.clone(),
        workflow_version: format!("orchestrator-workflow-v{}", env!("CARGO_PKG_VERSION")),
        prompt_versions: runtime_config.prompts.versions.clone(),
        git_sha: workflow_git_sha(),
        config_hash: content_hash(config)?,
        authority_registry_hash: authority_snapshot.content_hash,
        created_at: Utc::now().to_rfc3339(),
    };
    let store = FileStore::open(
        store_root,
        FileStoreOptions {
            atomic_fsync: runtime_config.store.atomic_fsync,
            stale_temp_age: Some(Duration::from_secs(runtime_config.store.stale_temp_age_sec)),
        },
    )?;

    // Re-check inside a lock so two invocations for the same deterministic run
    // ID cannot race and silently select different authority/config snapshots.
    let manifest =
        store.with_exclusive_lock(&location.relative_root().join(".manifest.lock"), || {
            if store.exists(&location.manifest_relative())? {
                return read_run_manifest(&store, &location);
            }
            write_run_manifest(&store, &location, RunManifest::new(manifest_init.clone())?)
        })?;
    validate_recovered_manifest(&manifest, &manifest_init)?;
    Ok(manifest)
}

fn validate_recovered_manifest(manifest: &RunManifest, expected: &RunManifestInit) -> Result<()> {
    // `read_run_manifest` has already rejected malformed JSON, unsupported or
    // old schemas, content-hash mismatches, and location mismatches. These
    // comparisons reject a resumed run whose process contract changed.
    for (field, found, current) in [
        (
            "workflow_version",
            manifest.workflow_version.as_str(),
            expected.workflow_version.as_str(),
        ),
        (
            "config_hash",
            manifest.config_hash.as_str(),
            expected.config_hash.as_str(),
        ),
        (
            "authority_registry_hash",
            manifest.authority_registry_hash.as_str(),
            expected.authority_registry_hash.as_str(),
        ),
        (
            "git_sha",
            manifest.git_sha.as_str(),
            expected.git_sha.as_str(),
        ),
    ] {
        if found != current {
            bail!(
                "FileStore run manifest recovery rejected: {field} differs (stored `{found}`, current `{current}`)"
            );
        }
    }
    if manifest.prompt_versions != expected.prompt_versions {
        bail!(
            "FileStore run manifest recovery rejected: prompt_versions differ; create a new run instead of reusing this manifest"
        );
    }
    Ok(())
}

fn workflow_git_sha() -> String {
    option_env!("AKZIO_GIT_SHA")
        .or(option_env!("GIT_SHA"))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("unavailable")
        .to_owned()
}

pub async fn run(args: ExecArgs) -> Result<Value> {
    validate_args(&args)?;
    debug!(
        mode = args.mode.as_str(),
        mock = args.mock,
        debug = args.debug,
        from_phase = args.from_phase,
        to_phase = args.to_phase,
        "orchestrator exec starting"
    );
    let date = args
        .date
        .clone()
        .unwrap_or_else(|| Local::now().date_naive().to_string());
    NaiveDate::parse_from_str(&date, "%Y-%m-%d")
        .with_context(|| format!("invalid --date value {date:?}"))?;
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
    // Run tickers come from config analysis_universe (includes VIX for research).
    // allocation.investable_assets is separate and only used later for sizing.
    let tickers =
        parse_tickers(config_strings(&config, "orchestrator.analysis_universe", &[]).join(","));
    if tickers.is_empty() {
        bail!("orchestrator.analysis_universe is required in config (e.g. [QQQ, SOXX, VIX])");
    }
    let ticker = display_ticker(&tickers);
    let analysis_universe = tickers.clone();
    let runtime_config = RuntimeConfig::from_value(&config)?;
    // Parse one canonical store root now so CLI/config errors fail before any
    // legacy side effect. The run manifest is always FileStore-owned; business
    // Artifacts remain exclusively owned by the authority registry.
    let store_root = runtime_config
        .store
        .resolve_root(args.store_root.as_deref())?;
    debug!(
        plugins_enabled = runtime_config.plugins.enabled,
        component_plugins = runtime_config.component_plugins.components.len(),
        role_plugins = runtime_config.role_plugins.roles.len(),
        store_root = %store_root.display(),
        "prompt plugin runtime config loaded"
    );
    let run_dir = resolve_run_dir(&args);
    let db_path = resolve_db_path(&args, &config);
    let run_id = run_id_for(&tickers, &date);
    if let Some(store_manifest) = prepare_file_store_run_manifest_if_migrated(
        &store_root,
        &runtime_config,
        &config,
        &date,
        &run_id,
    )? {
        debug!(
            store_manifest = %store_manifest.location()?.manifest_relative().display(),
            authority_registry_hash = %store_manifest.authority_registry_hash,
            "FileStore run manifest ready"
        );
    }
    let mut conn = connect(&db_path)?;
    let state_path = run_dir.as_ref().map(|path| path.join("state.json"));
    let phase1_agents = parse_phase1_agents_with_config(DEFAULT_PHASE1_AGENTS, &runtime_config)?;
    let model_override = args.model.clone().filter(|value| !value.is_empty());
    let reasoning_effort_override = args
        .reasoning_effort
        .clone()
        .filter(|value| !value.trim().is_empty());
    let window_days = args
        .window_days
        .unwrap_or_else(|| config_int(&config, "orchestrator.runtime.window_days", 150));
    debug!(
        run_id,
        ticker,
        date,
        run_dir = ?run_dir.as_ref().map(|path| path.display().to_string()),
        db_path = %db_path.display(),
        config_path = %config_path.display(),
        "orchestrator exec resolved runtime paths"
    );

    let analyst_weights = phase1_analyst_weights();
    let mut state = json!({
        "run_id": run_id,
        "ticker": ticker,
        "tickers": tickers,
        "analysis_universe": analysis_universe,
        "investable_assets": runtime_config.allocation.investable_assets,
        "current_date": date,
        "lang": if args.lang == "zh" { config_str(&config, "orchestrator.runtime.lang", "zh") } else { args.lang.clone() },
        "mode": args.mode.as_str(),
        "window_days": window_days,
        "run_dir": run_dir,
        "db_path": db_path,
        "store_root": store_root,
        "phase_status": {},
        "phase1_agents": phase1_agents,
        "tech_refresh_enabled": args.tech_refresh_enabled,
        "jin10_lookback_hours": args.jin10_refresh_lookback_hours,
        "analyst_weights": analyst_weights,
        "degraded": false
    });
    state["mock"] = Value::Bool(args.mock);
    state["debug"] = Value::Bool(args.debug);
    if args.debug {
        orchestrator_llm::reset_debug_output_dir(&default_project_root())?;
    }
    {
        let conn = connect(&db_path)?;
        clear_agent_loop_history(&conn, &run_id)?;
    }
    write_run_record(
        &mut conn,
        &RunRecordInput {
            run_id: state["run_id"].as_str().unwrap(),
            current_date: &date,
        },
    )?;
    if !args.mock && runtime_config.strict_sqlite && args.to_phase >= 1 {
        debug!(
            required_contexts = ?runtime_config.required_contexts,
            "validating strict sqlite contexts"
        );
        validate_sqlite_context(&conn, &runtime_config)?;
    }

    // Each completed business phase is synchronously summarized by phase_summary before
    // the next phase starts, so downstream roles always see a complete index.
    let mut compress_jobs: Vec<(i64, std::thread::JoinHandle<Result<CompressJobResult>>)> =
        Vec::new();
    let phase_summary_gate = std::sync::Arc::new(orchestrator_sql::PhaseSummaryGate::new(&run_id));
    orchestrator_sql::register_phase_summary_gate(&run_id, phase_summary_gate.clone());

    if args.from_phase <= 0 && args.to_phase >= 0 {
        debug!("phase 0 (historical reflection and experience retrieval) starting");
        let phase_timer = start_phase_timer(0, "phase0");
        set_run_current_phase(&mut conn, &run_id, 0)?;
        if args.mock || !runtime_config.reflection.enabled {
            state["phase0"] = json!({
                "status": "skipped",
                "reason": if args.mock { "mock_runs_never_learn" } else { "reflection_disabled" }
            });
            set_phase_status(&mut state, 0, "skipped");
        } else {
            let alpaca_history = if runtime_config
                .alpaca_api_key
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
                && runtime_config
                    .alpaca_api_secret
                    .as_ref()
                    .is_some_and(|value| !value.trim().is_empty())
            {
                let tool_config = orchestrator_llm::tools::ExternalToolConfig {
                    project_root: default_project_root(),
                    db_path: Some(db_path.clone()),
                    run_dir: run_dir.clone(),
                    run_id: Some(run_id.clone()),
                    phase: Some(0),
                    allowed_reflection_task_ids: Vec::new(),
                    phase_summary_page_limit: runtime_config.retrieval.summary_page_limit,
                    phase_summary_detail_page_limit: runtime_config.retrieval.detail_page_limit,
                    tickers: runtime_config.allocation.investable_assets.clone(),
                    alpaca_live: true,
                    alpaca_market_data: false,
                    alpaca_api_key: runtime_config.alpaca_api_key.clone(),
                    alpaca_api_secret: runtime_config.alpaca_api_secret.clone(),
                    phase_summary_index: None,
                    phase_summary_gate: None,
                    file_store_input: None,
                    file_store_reflection_source: None,
                };
                match orchestrator_llm::tools::alpaca::get_history(&tool_config).await {
                    Ok(history) => json!({
                        "status": "completed",
                        "portfolio": history.get("portfolio").cloned().unwrap_or(Value::Null),
                        "fill_count": history.get("fills").and_then(Value::as_array).map(Vec::len),
                        "locally_imported_execution_count": history.get("locally_imported_execution_count").cloned().unwrap_or(json!(0))
                    }),
                    Err(error) => {
                        tracing::warn!(run_id, error = %error, "Alpaca history unavailable in phase 0");
                        json!({"status": "non_blocking_failed", "message": error.to_string()})
                    }
                }
            } else {
                json!({
                    "status": "unconfigured",
                    "reason": "ALPACA_API_KEY and ALPACA_API_SECRET are required; no brokerage or alternate account operation was attempted."
                })
            };
            match score_mature_predictions(
                &conn,
                &date,
                "1d",
                500,
                ReflectionThresholds {
                    loss_return: runtime_config.reflection.loss_return,
                    excess_return: runtime_config.reflection.excess_return,
                    high_confidence: runtime_config.reflection.high_confidence,
                    calibration_error: runtime_config.reflection.calibration_error,
                    repeated_error_count: runtime_config.reflection.repeated_error_count,
                },
                Some(&run_id),
                &runtime_config.reflection.reflection_version,
                runtime_config.reflection.task_limit,
            ) {
                Ok(scoring) => {
                    let tasks = pending_reflection_tasks(
                        &conn,
                        &run_id,
                        runtime_config.reflection.task_limit,
                    )?;
                    state["phase0"] = json!({
                        "status": "completed",
                        "outcome_scoring": scoring,
                        "task_limit": runtime_config.reflection.task_limit,
                        "parallelism": runtime_config.reflection.parallelism,
                        "alpaca_history": alpaca_history,
                        "tasks": tasks,
                        "note": "All matured outcomes receive routine review; anomaly triggers upgrade a task to deep review."
                    });
                    match run_phase0_reflections(
                        &mut conn,
                        &mut state,
                        model_override.as_deref(),
                        reasoning_effort_override.as_deref(),
                        &runtime_config,
                    )
                    .await
                    {
                        Ok(result) => state["phase0"]["reflection_execution"] = result,
                        Err(error) => {
                            tracing::warn!(run_id, error = %error, "phase 0 reflection execution failed");
                            state["phase0"]["reflection_execution"] = json!({
                                "status": "non_blocking_failed",
                                "message": error.to_string()
                            });
                        }
                    }
                    set_phase_status(&mut state, 0, "done");
                }
                Err(error) => {
                    tracing::warn!(run_id, error = %error, "phase 0 scoring failed; continuing");
                    state["phase0"] = json!({
                        "status": "non_blocking_failed",
                        "message": error.to_string()
                    });
                    set_phase_status(&mut state, 0, "non_blocking_failed");
                }
            }
        }
        record_phase_elapsed(&mut state, phase_timer);
        let phase0 = state.get("phase0").cloned().unwrap_or(Value::Null);
        record_runtime_debug_artifact(&mut state, 0, &phase0)?;
        debug!(status = ?state["phase_status"]["0"], "phase 0 fact collection finished");
    }
    if let Err(error) = inject_phase_summary_reflection(&conn, &mut state, &runtime_config) {
        tracing::warn!(
            run_id,
            error = %error,
            "experience retrieval failed; continuing without optional reflection context"
        );
        state["prior_memory"] = json!({
            "enabled": false,
            "status": "non_blocking_failed",
            "message": error.to_string()
        });
    }

    if args.from_phase <= 1 && args.to_phase >= 1 {
        debug!(roles = ?phase1_agents, "phase 1 starting");
        let phase_timer = start_phase_timer(1, "phase1");
        set_run_current_phase(&mut conn, &run_id, 1)?;
        run_phase1(
            &mut conn,
            &mut state,
            &phase1_agents,
            model_override.as_deref(),
            reasoning_effort_override.as_deref(),
            &runtime_config,
        )
        .await?;
        set_phase_status(&mut state, 1, "done");
        record_phase_elapsed(&mut state, phase_timer);
        compress_jobs.push((
            1,
            spawn_compress_job(
                phase_summary_gate.clone(),
                &state,
                1,
                model_override.as_deref(),
                reasoning_effort_override.as_deref(),
                &runtime_config,
            ),
        ));
        await_all_compress_jobs(&mut compress_jobs, &mut state).await?;
        debug!("phase 1 completed; phase_summary compress(1) finished");
    }
    if args.from_phase <= 2 && args.to_phase >= 2 {
        // Weighting is phase 2/3 work, not phase1 organize.
        materialize_weighted_probability_base(&mut state);
        let max_debate_rounds = args
            .max_debate_rounds
            .unwrap_or_else(|| config_int(&config, "orchestrator.runtime.max_debate_rounds", 5));
        let max_topics_per_side = args
            .max_topics_per_side
            .unwrap_or_else(|| config_int(&config, "orchestrator.runtime.max_topics_per_side", 10));
        debug!(max_debate_rounds, "phase 2 starting");
        let phase_timer = start_phase_timer(2, "phase2");
        set_run_current_phase(&mut conn, &run_id, 2)?;
        conn = run_phase2(
            conn,
            &mut state,
            model_override.as_deref(),
            reasoning_effort_override.as_deref(),
            max_debate_rounds,
            max_topics_per_side,
            &runtime_config,
        )
        .await?;
        let phase2_actionable = state
            .get("topic_generation_artifact")
            .and_then(|artifact| artifact.get("actionable"))
            .and_then(Value::as_bool)
            != Some(false);
        let phase2_status = if phase2_actionable { "done" } else { "skipped" };
        set_phase_status(&mut state, 2, phase2_status);
        record_phase_elapsed(&mut state, phase_timer);
        compress_jobs.push((
            2,
            spawn_compress_job(
                phase_summary_gate.clone(),
                &state,
                2,
                model_override.as_deref(),
                reasoning_effort_override.as_deref(),
                &runtime_config,
            ),
        ));
        await_all_compress_jobs(&mut compress_jobs, &mut state).await?;
        debug!("phase 2 completed; phase_summary compress(2) finished");
    }
    if args.from_phase <= 3 && args.to_phase >= 3 {
        // PhaseSummary has already completed for every preceding selected phase.
        if let Some(g) = orchestrator_sql::phase_summary_gate(&run_id) {
            if !g.has_inflight() {
                state["phase_summary_memory"] = g.snapshot().to_state_value();
            }
        }
        materialize_weighted_probability_base(&mut state);
        debug!("phase 3 starting");
        let phase_timer = start_phase_timer(3, "phase3");
        set_run_current_phase(&mut conn, &run_id, 3)?;
        run_phase3(
            &mut conn,
            &mut state,
            model_override.as_deref(),
            reasoning_effort_override.as_deref(),
            &runtime_config,
        )
        .await?;
        set_phase_status(&mut state, 3, "done");
        record_phase_elapsed(&mut state, phase_timer);
        compress_jobs.push((
            3,
            spawn_compress_job(
                phase_summary_gate.clone(),
                &state,
                3,
                model_override.as_deref(),
                reasoning_effort_override.as_deref(),
                &runtime_config,
            ),
        ));
        await_all_compress_jobs(&mut compress_jobs, &mut state).await?;
        debug!("phase 3 completed; phase_summary compress(3) finished");
    }
    let policy = if state.get("research_plan").is_some() {
        Some(apply_workflow_policy(&mut state, &conn, &runtime_config)?)
    } else {
        None
    };
    if args.from_phase <= 4 && args.to_phase >= 4 {
        debug!("phase 4 (trader) starting");
        let phase_timer = start_phase_timer(4, "phase4");
        set_run_current_phase(&mut conn, &run_id, 4)?;
        let phase4_status = if should_run_llm_trader(policy.as_ref(), &runtime_config) {
            run_phase4(
                &mut conn,
                &mut state,
                model_override.as_deref(),
                reasoning_effort_override.as_deref(),
                &runtime_config,
            )
            .await?;
            "done"
        } else {
            run_phase4_rust_rule(&mut conn, &mut state, &runtime_config)?;
            "derived"
        };
        set_phase_status(&mut state, 4, phase4_status);
        record_phase_elapsed(&mut state, phase_timer);
        compress_jobs.push((
            4,
            spawn_compress_job(
                phase_summary_gate.clone(),
                &state,
                4,
                model_override.as_deref(),
                reasoning_effort_override.as_deref(),
                &runtime_config,
            ),
        ));
        await_all_compress_jobs(&mut compress_jobs, &mut state).await?;
        debug!("phase 4 (trader) completed; phase_summary compress(4) finished");
    }
    if args.from_phase <= 5 && args.to_phase >= 5 {
        debug!("phase 5 (risk debate) starting");
        let phase_timer = start_phase_timer(5, "phase5");
        set_run_current_phase(&mut conn, &run_id, 5)?;
        let phase5_status = if should_run_risk_review(policy.as_ref(), &runtime_config) {
            run_phase5(
                &mut conn,
                &mut state,
                model_override.as_deref(),
                reasoning_effort_override.as_deref(),
                &runtime_config,
            )
            .await?;
            "done"
        } else {
            run_phase5_skipped(&mut conn, &mut state, &runtime_config)?;
            "skipped"
        };
        set_phase_status(&mut state, 5, phase5_status);
        record_phase_elapsed(&mut state, phase_timer);
        compress_jobs.push((
            5,
            spawn_compress_job(
                phase_summary_gate.clone(),
                &state,
                5,
                model_override.as_deref(),
                reasoning_effort_override.as_deref(),
                &runtime_config,
            ),
        ));
        await_all_compress_jobs(&mut compress_jobs, &mut state).await?;
        debug!("phase 5 (risk debate) completed; phase_summary compress(5) finished");
    }
    if args.from_phase <= 6 && args.to_phase >= 6 {
        debug!("phase 6 (portfolio manager) starting");
        let phase_timer = start_phase_timer(6, "phase6");
        set_run_current_phase(&mut conn, &run_id, 6)?;
        let phase6_status = if should_run_portfolio_review(policy.as_ref(), &runtime_config) {
            run_phase6(
                &mut conn,
                &mut state,
                model_override.as_deref(),
                reasoning_effort_override.as_deref(),
                &runtime_config,
            )
            .await?;
            "done"
        } else {
            run_phase6_derived(&mut conn, &mut state, &runtime_config)?;
            "derived"
        };
        set_phase_status(&mut state, 6, phase6_status);
        record_phase_elapsed(&mut state, phase_timer);
        compress_jobs.push((
            6,
            spawn_compress_job(
                phase_summary_gate.clone(),
                &state,
                6,
                model_override.as_deref(),
                reasoning_effort_override.as_deref(),
                &runtime_config,
            ),
        ));
        await_all_compress_jobs(&mut compress_jobs, &mut state).await?;
        debug!("phase 6 (portfolio manager) completed; phase_summary compress(6) finished");
    }
    if args.from_phase <= 7 && args.to_phase >= 7 {
        debug!("phase 7 (allocation) starting");
        let phase_timer = start_phase_timer(7, "phase7");
        set_run_current_phase(&mut conn, &run_id, 7)?;
        run_phase7(
            &mut conn,
            &mut state,
            model_override.as_deref(),
            reasoning_effort_override.as_deref(),
            &runtime_config,
        )
        .await?;
        set_phase_status(&mut state, 7, "done");
        record_phase_elapsed(&mut state, phase_timer);
        let allocation = state
            .get("portfolio_allocation")
            .cloned()
            .unwrap_or(Value::Null);
        record_runtime_debug_artifact(&mut state, 7, &allocation)?;
        compress_jobs.push((
            7,
            spawn_compress_job(
                phase_summary_gate.clone(),
                &state,
                7,
                model_override.as_deref(),
                reasoning_effort_override.as_deref(),
                &runtime_config,
            ),
        ));
        await_all_compress_jobs(&mut compress_jobs, &mut state).await?;
        debug!("phase 7 (allocation) completed; phase_summary compress(7) finished");
    }
    if args.from_phase <= 8 && args.to_phase >= 8 {
        debug!("phase 8 (archive + predict) starting");
        let phase_timer = start_phase_timer(8, "phase8");
        set_run_current_phase(&mut conn, &run_id, 8)?;
        if let Err(error) = run_phase8(&mut conn, &mut state, &runtime_config) {
            tracing::warn!(
                run_id,
                error = %error,
                "phase 8 archive/prediction failed after allocation; returning the validated decision"
            );
            state["phase8_error"] = json!({
                "status": "non_blocking_failed",
                "message": error.to_string()
            });
            set_phase_status(&mut state, 8, "non_blocking_failed");
        } else {
            set_phase_status(&mut state, 8, "done");
        }
        record_phase_elapsed(&mut state, phase_timer);
        let archive = json!({
            "status": state["phase_status"]["8"],
            "error": state.get("phase8_error").cloned().unwrap_or(Value::Null)
        });
        record_runtime_debug_artifact(&mut state, 8, &archive)?;
        debug!(status = ?state["phase_status"]["8"], "phase 8 archive/prediction finished");
    }
    // Drain any compress still running when the pipeline ends early (e.g. to_phase < 8).
    await_all_compress_jobs(&mut compress_jobs, &mut state).await?;

    // Idempotent insurance for the legacy profile only.  Once Summary is
    // FileStore-authoritative, every unit was finalized atomically before its
    // gate completed; writing the compatibility projection below would create
    // a forbidden second authority.
    let phase_summary_flushed = if !phase_summary_uses_file_store(&runtime_config)? {
        crate::orchestration::compress::flush_phase_summary_to_sqlite(&conn, &mut state)?
    } else {
        0
    };
    orchestrator_sql::unregister_phase_summary_gate(&run_id);
    debug!(
        phase_summary_flushed,
        "phase_summary memory flushed to sqlite at run end"
    );

    update_run_status(&mut conn, &run_id, "completed", None)?;
    record_contracts(&mut state);
    let final_summary_path = if let (Some(run_dir), Some(state_path)) = (&run_dir, &state_path) {
        persist_run_outputs(run_dir, state_path, &state)?;
        Some(run_dir.join("final_summary.md"))
    } else {
        None
    };
    debug!(
        state_path = ?state_path.as_ref().map(|path| path.display().to_string()),
        final_summary = ?final_summary_path
            .as_ref()
            .map(|path| path.display().to_string()),
        degraded = state
            .get("degraded")
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
        "orchestrator exec finished"
    );

    let research = state
        .get("research_plan")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let allocation = state
        .get("portfolio_allocation")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let trader = state
        .get("trader_investment_plan")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let final_decision = state
        .get("final_trade_decision")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let result = json!({
        "ticker": ticker,
        "tickers": tickers_from_state(&state),
        "mode": args.mode.as_str(),
        "debate_mode": "sqlite",
        "phase1_agents": phase1_agents,
        "date": date,
        "run_dir": run_dir,
        "db_path": db_path,
        "state": state_path,
        "final_summary": final_summary_path,
        "degraded": state.get("degraded").and_then(Value::as_bool).unwrap_or(false),
        "rating": final_decision.get("rating").cloned().or_else(|| research.get("rating").cloned()).unwrap_or(Value::Null),
        "action": trader.get("action").cloned().unwrap_or(Value::Null),
        "research_rating": research.get("rating").cloned().unwrap_or(Value::Null),
        "long_probability": research.get("long_probability").cloned().unwrap_or(Value::Null),
        "short_probability": research.get("short_probability").cloned().unwrap_or(Value::Null),
        "trader_investment_plan": trader,
        "final_trade_decision": final_decision,
        "vix_regime": allocation.get("vix_regime").cloned().unwrap_or(Value::Null),
        "portfolio_allocation": allocation,
        "run_state": state.clone(),
    });
    Ok(result)
}

async fn run_phase0_reflections(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<Value> {
    let reflection_authority = config.authority_registry.authority_for(
        "reflector.historical",
        ToolManagedProfile::HistoricalReflection,
    )?;
    let tasks = state
        .pointer("/phase0/tasks")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if tasks.is_empty() {
        return Ok(json!({"status": "completed", "processed": 0, "failed": 0}));
    }
    let prompt_path = config
        .prompts
        .path_for("reflector.historical")
        .context("missing prompt path for reflector.historical")?;
    let mut jobs = Vec::new();
    for task in &tasks {
        let task_id = task
            .get("task_id")
            .and_then(Value::as_i64)
            .context("phase0 task_id is required")?;
        if reflection_authority == ArtifactAuthority::Legacy {
            set_reflection_task_status(conn, task_id, "running", None)?;
        } else if phase0_reflection_is_completed_in_file_store(state, task)? {
            continue;
        }
        let mut task_state = state.clone();
        task_state["reflection_task"] = task.clone();
        task_state["phase0"]["tasks"] = json!([task]);
        jobs.push(prepare_role_job(RoleRun {
            state: task_state,
            role: "reflector.historical",
            phase: 0,
            kind: "historical_reflection",
            round: None,
            topic_id: Some(&task_id.to_string()),
            mock: false,
            model_override,
            reasoning_effort_override,
            config,
            prompt_path: Some(prompt_path),
        })?);
    }
    let results = run_role_jobs(
        jobs,
        config.reflection.parallelism,
        config.workflow.agent_timeout_sec,
    )
    .await;
    let mut processed = 0;
    let mut failed = 0;
    let mut audit = Vec::new();
    for result in results {
        let task_id = result
            .topic_id
            .as_deref()
            .and_then(|value| value.parse::<i64>().ok())
            .context("reflector result is missing task id")?;
        if let Some(artifact) = result.artifact {
            let persisted = if reflection_authority == ArtifactAuthority::FileStore {
                persist_file_store_reflection_record(state, task_id, &artifact)
                    .map(|count| json!({"experience_count": count, "persistence": "file_store"}))
            } else {
                persist_reflection_artifact(
                    conn,
                    task_id,
                    &config.reflection.reflection_version,
                    &artifact,
                )
                .map(|count| json!({"experience_count": count, "persistence": "legacy"}))
            };
            match persisted {
                Ok(summary) => {
                    processed += 1;
                    audit.push(json!({
                        "task_id": task_id,
                        "status": "completed",
                        "experience_count": summary["experience_count"],
                        "persistence": summary["persistence"]
                    }));
                }
                Err(error) => {
                    failed += 1;
                    if reflection_authority == ArtifactAuthority::Legacy {
                        set_reflection_task_status(
                            conn,
                            task_id,
                            "failed",
                            Some(&error.to_string()),
                        )?;
                    }
                    audit.push(json!({
                        "task_id": task_id,
                        "status": "failed_validation",
                        "message": error.to_string()
                    }));
                }
            }
        } else {
            failed += 1;
            let message = result
                .error
                .unwrap_or_else(|| "reflector returned no artifact".to_string());
            if reflection_authority == ArtifactAuthority::Legacy {
                set_reflection_task_status(conn, task_id, "failed", Some(&message))?;
            }
            audit.push(json!({
                "task_id": task_id,
                "status": "failed",
                "message": message
            }));
        }
    }
    Ok(json!({
        "status": if failed == 0 { "completed" } else { "completed_with_failures" },
        "processed": processed,
        "failed": failed,
        "tasks": audit
    }))
}

fn phase0_reflection_is_completed_in_file_store(state: &Value, task: &Value) -> Result<bool> {
    let task_id = task
        .get("task_id")
        .and_then(Value::as_i64)
        .context("phase0 task_id is required")?;
    let ticker = task
        .get("ticker")
        .and_then(Value::as_str)
        .context("phase0 task ticker is required")?;
    let store_root = state
        .get("store_root")
        .and_then(Value::as_str)
        .context("store_root is required for FileStore reflection")?;
    let location = RunLocation::new(
        state
            .get("current_date")
            .and_then(Value::as_str)
            .context("current_date is required for FileStore reflection")?,
        state
            .get("run_id")
            .and_then(Value::as_str)
            .context("run_id is required for FileStore reflection")?,
    )?;
    let store = FileStore::open(store_root, FileStoreOptions::default())?;
    match read_learning_record(&store, &location, LearningKind::Reflection, ticker) {
        Ok(record) => Ok(record.payload.get("task_id").and_then(Value::as_i64) == Some(task_id)),
        Err(orchestrator_store::StoreError::Io { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok(false)
        }
        Err(error) => Err(error.into()),
    }
}

fn persist_file_store_reflection_record(
    state: &Value,
    task_id: i64,
    artifact: &Value,
) -> Result<usize> {
    let task = state
        .get("reflection_task")
        .and_then(Value::as_object)
        .context("FileStore reflection task missing from state")?;
    let source_run_id = task
        .get("source_run_id")
        .and_then(Value::as_str)
        .context("FileStore reflection source_run_id is required")?;
    let ticker = task
        .get("ticker")
        .and_then(Value::as_str)
        .context("FileStore reflection ticker is required")?;
    if artifact.get("kind").and_then(Value::as_str) != Some("experience")
        || artifact.get("role").and_then(Value::as_str) != Some("reflector.historical")
        || artifact.get("source_run_id").and_then(Value::as_str) != Some(source_run_id)
        || artifact.get("ticker").and_then(Value::as_str) != Some(ticker)
    {
        bail!("terminal HistoricalReflection artifact does not match Rust-owned task scope");
    }
    let store_root = state
        .get("store_root")
        .and_then(Value::as_str)
        .context("store_root is required for FileStore reflection")?;
    let location = RunLocation::new(
        state
            .get("current_date")
            .and_then(Value::as_str)
            .context("current_date is required for FileStore reflection")?,
        state
            .get("run_id")
            .and_then(Value::as_str)
            .context("run_id is required for FileStore reflection")?,
    )?;
    let store = FileStore::open(store_root, FileStoreOptions::default())?;
    let record = LearningRecord {
        schema_version: LEARNING_RECORD_SCHEMA_VERSION,
        kind: LearningKind::Reflection,
        run_id: location.run_id.clone(),
        ticker: ticker.to_owned(),
        source_run_id: Some(source_run_id.to_owned()),
        payload: json!({
            "task_id": task_id,
            "source_run_id": source_run_id,
            "reflection_task": task,
            "experience_index_id": artifact.get("index_id").cloned().unwrap_or(Value::Null),
            "experience_level": "derived_from_historical_case_count",
            "artifact": artifact,
        }),
        created_at: Utc::now().to_rfc3339(),
        content_hash: String::new(),
    };
    write_learning_record(&store, &location, LearningKind::Reflection, record)?;
    Ok(1)
}

fn persist_run_outputs(run_dir: &Path, state_path: &Path, state: &Value) -> Result<()> {
    fs::create_dir_all(run_dir)
        .with_context(|| format!("failed to create run dir {}", run_dir.display()))?;
    fs::write(
        state_path,
        serde_json::to_string_pretty(state).context("failed to serialize run state")?,
    )
    .with_context(|| format!("failed to write {}", state_path.display()))?;
    let summary = crate::report::builder::build_human_readable_report(state);
    let summary_path = run_dir.join("final_summary.md");
    fs::write(&summary_path, summary)
        .with_context(|| format!("failed to write {}", summary_path.display()))?;
    Ok(())
}

fn validate_args(args: &ExecArgs) -> Result<()> {
    if let Some(rounds) = args.max_debate_rounds {
        if rounds < 1 {
            bail!("--max-debate-rounds must be >= 1");
        }
    }
    if let Some(topics) = args.max_topics_per_side {
        if topics < 1 {
            bail!("--max-topics-per-side must be >= 1");
        }
    }
    if args.from_phase < 0 || args.from_phase > 8 {
        bail!("--from-phase must be 0-8");
    }
    if args.to_phase < args.from_phase || args.to_phase > 8 {
        bail!("--to-phase must be between --from-phase and 8");
    }
    Ok(())
}

#[cfg(test)]
fn parse_phase1_agents(raw: &str) -> Result<Vec<String>> {
    let registry = orchestrator_core::role_registry::AgentRegistry::builtin();
    registry
        .parse_role_list(raw)
        .map_err(|e| anyhow::anyhow!(e))
}

fn parse_phase1_agents_with_config(raw: &str, config: &RuntimeConfig) -> Result<Vec<String>> {
    config
        .agent_registry
        .parse_role_list(raw)
        .map_err(|e| anyhow::anyhow!(e))
}

fn apply_workflow_policy(
    state: &mut Value,
    conn: &rusqlite::Connection,
    config: &RuntimeConfig,
) -> Result<WorkflowPolicyDecision> {
    let allocation_context = compute_allocation_context(state, conn, &config.allocation)?;
    state["allocation_context"] = allocation_context.clone();
    let signals = workflow_policy_signals(state, &allocation_context, config);
    let decision = evaluate_workflow_policy(
        config.workflow.policy_mode,
        3,
        &signals,
        &config.workflow.policy_thresholds,
    );
    record_workflow_policy(state, &decision);
    Ok(decision)
}

fn workflow_policy_signals(
    state: &Value,
    allocation_context: &Value,
    config: &RuntimeConfig,
) -> WorkflowPolicySignals {
    let research = state.get("research_plan").unwrap_or(&Value::Null);
    WorkflowPolicySignals {
        confidence: research_confidence(research),
        long_probability: research.get("long_probability").and_then(Value::as_f64),
        volatility: max_allocation_volatility(allocation_context),
        correlation: allocation_context
            .get("correlation_60d")
            .and_then(Value::as_f64),
        proposed_position: proposed_position_signal(state, research),
        high_risk_flag: has_high_risk_flag(research),
        trade_research_conflict: compute_trade_research_conflict(state),
        force_portfolio_review: config.workflow.force_portfolio_review,
        research_degraded: research_is_degraded(research),
    }
}

fn research_is_degraded(research: &Value) -> bool {
    research
        .get("degraded")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || research.get("usable").and_then(Value::as_bool) == Some(false)
        || research.get("status").and_then(Value::as_str) == Some("degraded")
}

/// Estimate the largest single-name weight the run is heading toward.
/// Prefers an explicit numeric recommendation, then trader position_size,
/// then a conviction proxy from |long_probability - 0.5| * 2.
fn proposed_position_signal(state: &Value, research: &Value) -> Option<f64> {
    if let Some(value) = research
        .get("recommended_position")
        .or_else(|| research.get("position_pct"))
        .or_else(|| research.get("max_position"))
        .and_then(Value::as_f64)
    {
        return Some(value.clamp(0.0, 1.0));
    }

    if let Some(size) = state.get("trader_investment_plan").and_then(|plan| {
        plan.get("position_size_pct_max")
            .and_then(Value::as_f64)
            .or_else(|| {
                plan.get("position_size")
                    .and_then(Value::as_str)
                    .and_then(parse_position_size_pct)
            })
    }) {
        return Some(size.clamp(0.0, 1.0));
    }

    research
        .get("long_probability")
        .and_then(Value::as_f64)
        .map(|probability| ((probability - 0.5).abs() * 2.0).clamp(0.0, 1.0))
}

fn parse_position_size_pct(raw: &str) -> Option<f64> {
    let trimmed = raw.trim();
    if trimmed == "0%" {
        return Some(0.0);
    }
    // Prefer the upper bound of ranges like "0%-30%" or "30%-50%".
    let mut values = Vec::new();
    for part in trimmed.split(|c: char| c == '-' || c == '/' || c.is_whitespace()) {
        let part = part.trim().trim_end_matches('%');
        if part.is_empty() {
            continue;
        }
        if let Ok(value) = part.parse::<f64>() {
            values.push((value / 100.0).clamp(0.0, 1.0));
        }
    }
    values.into_iter().reduce(f64::max)
}

fn research_confidence(research: &Value) -> Option<f64> {
    research
        .get("confidence")
        .and_then(Value::as_f64)
        .or_else(|| {
            let values = research
                .get("per_ticker")
                .and_then(Value::as_object)?
                .values()
                .filter_map(|item| item.get("confidence").and_then(Value::as_f64))
                .collect::<Vec<_>>();
            if values.is_empty() {
                None
            } else {
                Some(values.iter().sum::<f64>() / values.len() as f64)
            }
        })
}

fn max_allocation_volatility(allocation_context: &Value) -> Option<f64> {
    allocation_context
        .get("per_ticker")
        .and_then(Value::as_object)
        .and_then(|items| {
            items
                .values()
                .filter_map(|item| item.get("vol_pct").and_then(Value::as_f64))
                .reduce(f64::max)
        })
}

fn has_high_risk_flag(research: &Value) -> bool {
    research
        .get("high_risk_flag")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || research
            .get("risk_flags")
            .and_then(Value::as_array)
            .is_some_and(|items| !items.is_empty())
}

/// Detect a conflict between the research manager's final probability and the
/// Phase 1 weighted analyst base. A large divergence (|delta| > 0.15) means the
/// research manager significantly departed from the analyst consensus, which
/// warrants running the LLM trader to carefully reconcile rather than using the
/// mechanical rust rule.
fn compute_trade_research_conflict(state: &Value) -> bool {
    const CONFLICT_THRESHOLD: f64 = 0.15;

    let research_long = state
        .get("research_plan")
        .and_then(|r| r.get("long_probability"))
        .and_then(Value::as_f64);
    let Some(research_long) = research_long else {
        return false;
    };

    let weighted_base = state
        .get("weighted_probability_base")
        .and_then(Value::as_object);

    let Some(weighted_base) = weighted_base else {
        return false;
    };

    let base_values: Vec<f64> = weighted_base
        .values()
        .filter_map(|item| item.get("long_probability").and_then(Value::as_f64))
        .collect();
    if base_values.is_empty() {
        return false;
    }

    let avg_base = base_values.iter().sum::<f64>() / base_values.len() as f64;
    (research_long - avg_base).abs() > CONFLICT_THRESHOLD
}

fn is_selective_policy(config: &RuntimeConfig) -> bool {
    config.workflow.policy_mode == WorkflowPolicyMode::Selective
}

fn should_run_llm_trader(policy: Option<&WorkflowPolicyDecision>, config: &RuntimeConfig) -> bool {
    policy
        .map(|decision| decision.need_trader)
        .unwrap_or_else(|| !is_selective_policy(config))
}

fn should_run_risk_review(policy: Option<&WorkflowPolicyDecision>, config: &RuntimeConfig) -> bool {
    policy
        .map(|decision| decision.need_risk_review)
        .unwrap_or_else(|| !is_selective_policy(config))
}

fn should_run_portfolio_review(
    policy: Option<&WorkflowPolicyDecision>,
    config: &RuntimeConfig,
) -> bool {
    policy
        .map(|decision| decision.need_portfolio_review)
        .unwrap_or_else(|| !is_selective_policy(config))
}

const PHASE3_PROBABILITY_DRIFT_CRITICAL: f64 = 0.15;

fn phase3_probability_drift_violations(state: &Value, artifact: &Value) -> Vec<Value> {
    let weighted_base = state
        .get("weighted_probability_base")
        .and_then(Value::as_object);
    let primary_ticker = tickers_from_state(state)
        .into_iter()
        .next()
        .or_else(|| weighted_base.and_then(|items| items.keys().next().cloned()));
    weighted_base
        .into_iter()
        .flatten()
        .filter_map(|(ticker, base)| {
            let base_long = base
                .get("long_probability")
                .or_else(|| base.get("weighted_long_probability"))
                .or_else(|| base.get("probability"))
                .and_then(Value::as_f64)?;
            let base_short = base
                .get("short_probability")
                .or_else(|| base.get("weighted_short_probability"))
                .and_then(Value::as_f64)
                .unwrap_or(1.0 - base_long);
            let is_primary = primary_ticker.as_deref() == Some(ticker.as_str());
            let proposed_long = research_decision_for_ticker(artifact, ticker)
                .and_then(|decision| {
                    decision
                .get("long_probability")
                        .and_then(Value::as_f64)
                })
                .or_else(|| {
                    is_primary
                        .then(|| artifact.get("long_probability").and_then(Value::as_f64))
                        .flatten()
                });
            let base_confidence_basis = state
                .get("phase1_index")
                .and_then(|value| value.get("per_ticker"))
                .and_then(Value::as_object)
                .and_then(|items| items.get(ticker))
                .and_then(|value| value.get("evidence_quality"))
                .and_then(|value| value.get("confidence_basis"))
                .cloned()
                .unwrap_or_else(|| json!("evidence_available"));
            let Some(proposed_long) = proposed_long else {
                return Some(json!({
                    "ticker": ticker,
                    "base_long_probability": base_long,
                    "base_short_probability": base_short,
                    "base_confidence_basis": base_confidence_basis,
                    "proposed_long_probability": Value::Null,
                    "delta": Value::Null,
                    "severity": "critical",
                    "is_primary": is_primary,
                    "reason": "manager.research omitted a numeric per-ticker long_probability"
                }));
            };
            let delta = (proposed_long - base_long).abs();
            let has_valid_debate_increment = debate_justifies_probability_drift(state, ticker);
            (delta > PHASE3_PROBABILITY_DRIFT_CRITICAL
                || (!has_valid_debate_increment && delta > f64::EPSILON))
            .then(|| {
                json!({
                    "ticker": ticker,
                    "base_long_probability": base_long,
                    "base_short_probability": base_short,
                    "base_confidence_basis": base_confidence_basis,
                    "proposed_long_probability": proposed_long,
                    "delta": delta,
                    "severity": if delta > PHASE3_PROBABILITY_DRIFT_CRITICAL { "critical" } else { "warning" },
                    "is_primary": is_primary,
                    "reason": if delta > PHASE3_PROBABILITY_DRIFT_CRITICAL {
                        "probability drift exceeds the absolute Rust limit of 0.15"
                    } else {
                        "non-zero debate adjustment requires a converged decision hinge with evidence references"
                    }
                })
            })
        })
        .collect()
}

fn debate_justifies_probability_drift(state: &Value, ticker: &str) -> bool {
    let Some(debate) = state.get("debate_state_artifact") else {
        return false;
    };
    let per_ticker_support = debate
        .get("per_ticker")
        .and_then(Value::as_object)
        .and_then(|items| items.get(ticker))
        .is_some_and(|item| is_explicitly_converged(item) && has_evidence_backed_hinge(item));
    per_ticker_support
        || debate
            .get("topic_briefs")
            .and_then(Value::as_array)
            .is_some_and(|briefs| {
                briefs.iter().any(|brief| {
                    topic_brief_targets_ticker(brief, ticker)
                        && is_explicitly_converged(brief)
                        && has_evidence_backed_hinge(brief)
                })
            })
}

fn is_explicitly_converged(value: &Value) -> bool {
    ["convergence_status", "status"]
        .iter()
        .any(|key| value.get(*key).and_then(Value::as_str) == Some("converged"))
        || value
            .get("controller_artifact")
            .is_some_and(is_explicitly_converged)
}

fn topic_brief_targets_ticker(brief: &Value, ticker: &str) -> bool {
    brief
        .get("tickers")
        .and_then(Value::as_array)
        .is_some_and(|items| items.iter().any(|item| item.as_str() == Some(ticker)))
        || brief.get("target_ticker").and_then(Value::as_str) == Some(ticker)
}

fn has_evidence_backed_hinge(value: &Value) -> bool {
    match value {
        Value::Array(items) => items.iter().any(has_evidence_backed_hinge),
        Value::Object(object) => {
            let direct_hinge = object
                .get("decision_hinge")
                .or_else(|| object.get("hinge"))
                .and_then(Value::as_str)
                .is_some_and(|value| !value.trim().is_empty());
            let hinge_list = object
                .get("decision_hinges")
                .and_then(Value::as_array)
                .is_some_and(|items| !items.is_empty());
            let direct_evidence = [
                "evidence_refs",
                "source_refs",
                "long_evidence_refs",
                "short_evidence_refs",
            ]
            .iter()
            .any(|key| {
                object
                    .get(*key)
                    .and_then(Value::as_array)
                    .is_some_and(|items| !items.is_empty())
            });
            ((direct_hinge || hinge_list) && direct_evidence)
                || object.values().any(has_evidence_backed_hinge)
        }
        _ => false,
    }
}

fn phase3_probability_retry_state(state: &Value, violations: &[Value]) -> Value {
    let mut retry_state = state.clone();
    for violation in violations {
        let Some(ticker) = violation.get("ticker").and_then(Value::as_str) else {
            continue;
        };
        retry_state["debate_state_artifact"]["per_ticker"][ticker]
            ["manager_probability_guard_retry"] = json!({
            "status": "previous_manager_probability_rejected",
            "base_long_probability": violation.get("base_long_probability").cloned().unwrap_or(Value::Null),
            "proposed_long_probability": violation.get("proposed_long_probability").cloned().unwrap_or(Value::Null),
            "delta": violation.get("delta").cloned().unwrap_or(Value::Null)
        });
    }
    retry_state
}

fn apply_phase3_probability_fallback(mut artifact: Value, violations: &[Value]) -> Value {
    for violation in violations {
        let Some(ticker) = violation.get("ticker").and_then(Value::as_str) else {
            continue;
        };
        let Some(base_long) = violation
            .get("base_long_probability")
            .and_then(Value::as_f64)
        else {
            continue;
        };
        let base_short = violation
            .get("base_short_probability")
            .and_then(Value::as_f64)
            .unwrap_or(1.0 - base_long);
        if !artifact.get("per_ticker").is_some_and(Value::is_object) {
            artifact["per_ticker"] = json!({});
        }
        let base_is_insufficient = violation
            .get("base_confidence_basis")
            .and_then(Value::as_str)
            == Some("data_insufficient");
        let rating = research_rating_for_probability(base_long);
        let confidence_basis = if base_is_insufficient {
            "data_insufficient"
        } else if rating == "Hold" {
            "evidence_balanced"
        } else {
            "directional_evidence"
        };
        let hold_reason = (rating == "Hold").then_some(if base_is_insufficient {
            "evidence_insufficient"
        } else {
            "evidence_balanced"
        });
        let fallback_rationale = format!(
            "Probability guard rejected the manager adjustment and restored the Phase 1 index base for {ticker}."
        );
        {
            let payload = artifact
                .get_mut("per_ticker")
                .and_then(Value::as_object_mut)
                .expect("per_ticker initialized above")
                .entry(ticker.to_string())
                .or_insert_with(|| json!({}));
            payload["rating"] = json!(rating);
            payload["long_probability"] = json!(base_long);
            payload["short_probability"] = json!(base_short);
            payload["confidence_basis"] = json!(confidence_basis);
            if let Some(hold_reason) = hold_reason {
                payload["hold_reason"] = json!(hold_reason);
            } else if let Some(object) = payload.as_object_mut() {
                object.remove("hold_reason");
            }
            if let Some(object) = payload.as_object_mut() {
                object.remove("scenarios");
            }
            payload["probability_rationale"] = json!(fallback_rationale.clone());
            payload["probability_guard"] = json!({
                "status": "clamped_to_phase1_base",
                "proposed_long_probability": violation.get("proposed_long_probability").cloned().unwrap_or(Value::Null),
                "delta": violation.get("delta").cloned().unwrap_or(Value::Null),
                "severity": violation.get("severity").cloned().unwrap_or(Value::Null)
            });
        }
        if violation.get("is_primary").and_then(Value::as_bool) == Some(true) {
            artifact["rating"] = json!(rating);
            artifact["long_probability"] = json!(base_long);
            artifact["short_probability"] = json!(base_short);
            artifact["confidence_basis"] = json!(confidence_basis);
            artifact["hold_reason"] = hold_reason.map(Value::from).unwrap_or(Value::Null);
            if let Some(object) = artifact.as_object_mut() {
                object.remove("scenarios");
            }
            artifact["probability_rationale"] = json!(fallback_rationale);
        }
    }
    artifact["probability_guard"] = json!({
        "status": "clamped_to_phase1_base",
        "violations": violations
    });
    artifact
}

fn start_phase_timer(phase: i64, label: &'static str) -> PhaseTimer {
    PhaseTimer {
        phase,
        label,
        started_at: Instant::now(),
    }
}

fn record_phase_elapsed(state: &mut Value, timer: PhaseTimer) {
    let elapsed_ms = timer.started_at.elapsed().as_millis() as u64;
    if !state.get("phase_metrics").is_some_and(Value::is_array) {
        state["phase_metrics"] = json!([]);
    }
    if let Some(items) = state["phase_metrics"].as_array_mut() {
        items.push(json!({
            "phase": timer.phase,
            "label": timer.label,
            "elapsed_ms": elapsed_ms,
        }));
    }
    let total = state
        .get("phase_metrics")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.get("elapsed_ms").and_then(Value::as_u64))
                .sum::<u64>()
        })
        .unwrap_or(0);
    if !state.get("workflow_metrics").is_some_and(Value::is_object) {
        state["workflow_metrics"] = json!({});
    }
    state["workflow_metrics"]["phase_count"] = state
        .get("phase_metrics")
        .and_then(Value::as_array)
        .map(|items| json!(items.len()))
        .unwrap_or_else(|| json!(0));
    state["workflow_metrics"]["total_phase_elapsed_ms"] = json!(total);
    if state.get("debug").and_then(Value::as_bool) == Some(true) {
        orchestrator_llm::debug_log_time(
            &default_project_root(),
            json!({
                "kind": "phase",
                "name": timer.label,
                "phase": timer.phase,
                "elapsed_ms": elapsed_ms,
            }),
        );
    }
}

fn record_market_truth_check(state: &mut Value, downstream_name: &str, downstream: &Value) {
    let Some(research_plan) = state.get("research_plan").cloned() else {
        return;
    };
    let report = market_truth_violation_report(&research_plan, downstream_name, downstream);
    if !state
        .get("market_truth_checks")
        .is_some_and(Value::is_array)
    {
        state["market_truth_checks"] = json!([]);
    }
    if let Some(items) = state["market_truth_checks"].as_array_mut() {
        items.push(report.clone());
    }

    if report
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| status == "violation")
    {
        if !state
            .get("market_truth_violations")
            .is_some_and(Value::is_array)
        {
            state["market_truth_violations"] = json!([]);
        }
        if let Some(items) = state["market_truth_violations"].as_array_mut() {
            items.push(report);
        }
    }

    let violation_count = state
        .get("market_truth_violations")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.get("violation_count").and_then(Value::as_u64))
                .sum::<u64>()
        })
        .unwrap_or(0);
    let check_count = state
        .get("market_truth_checks")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    if !state.get("workflow_metrics").is_some_and(Value::is_object) {
        state["workflow_metrics"] = json!({});
    }
    state["workflow_metrics"]["market_truth_check_count"] = json!(check_count);
    state["workflow_metrics"]["market_truth_violation_count"] = json!(violation_count);
}

fn enforce_phase3_market_truth(state: &Value, downstream: &mut Value) {
    let Some(research) = state.get("research_plan") else {
        return;
    };
    for (research_field, downstream_field, shadow_field) in [
        ("rating", "rating", "llm_rating"),
        ("plan", "investment_thesis", "llm_investment_thesis"),
    ] {
        let Some(authoritative) = research.get(research_field).cloned() else {
            continue;
        };
        if let Some(existing) = downstream.get(downstream_field).cloned() {
            if existing != authoritative {
                downstream[shadow_field] = existing;
            }
        }
        downstream[downstream_field] = authoritative;
    }
    strip_non_authoritative_market_truth_fields(downstream);
}

fn strip_downstream_market_truth_fields(downstream: &mut Value) {
    let Some(object) = downstream.as_object_mut() else {
        return;
    };
    for field in [
        "rating",
        "long_probability",
        "short_probability",
        "probability_rationale",
        "plan",
        "thesis",
        "investment_thesis",
        "market_thesis",
    ] {
        if let Some(value) = object.remove(field) {
            object.insert(format!("llm_{field}"), value);
        }
    }
}

fn strip_non_authoritative_market_truth_fields(downstream: &mut Value) {
    let Some(object) = downstream.as_object_mut() else {
        return;
    };
    for field in [
        "long_probability",
        "short_probability",
        "probability_rationale",
        "plan",
        "thesis",
        "market_thesis",
    ] {
        if let Some(value) = object.remove(field) {
            object.insert(format!("llm_{field}"), value);
        }
    }
}

fn sanitize_downstream_constraints(state: &mut Value, downstream_name: &str, artifact: &mut Value) {
    record_market_truth_check(state, downstream_name, artifact);
    strip_downstream_market_truth_fields(artifact);
}

fn resolve_run_dir(args: &ExecArgs) -> Option<PathBuf> {
    args.run_dir.as_ref().map(|path| {
        if path.is_absolute() {
            path.clone()
        } else {
            default_project_root().join(path)
        }
    })
}

fn resolve_db_path(args: &ExecArgs, config: &Value) -> PathBuf {
    if let Some(path) = &args.db_path {
        return project_path(path);
    }
    for key in ["orchestrator.db_path", "orchestrator.runtime.db_path"] {
        if let Some(value) = orchestrator_core::config_get(config, key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return project_path(value);
        }
    }
    project_path("outputs/orchestrator.sqlite")
}

fn phase1_analyst_weights() -> Value {
    json!({
        "analyst.technical": 50.0,
        "analyst.news_macro": 50.0
    })
}

async fn run_phase1(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    roles: &[String],
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<()> {
    let mock = is_mock(state);
    debug!(roles = ?roles, mock, "phase 1 preflight starting");
    for role in roles {
        // FileStore Analyst units obtain their evidence through the
        // ToolManaged runtime and snapshots.  Do not run the old SQLite import
        // preflight for a migrated authority.
        if !mock {
            if analyst_uses_file_store(config, role)? {
                run_file_store_phase1_preflight(state, role, config).await?;
            } else {
                run_phase1_preflight(conn, state, role, config).await?;
            }
            enforce_preflight_policy(state, role, config)?;
        }
    }

    // Capture every input required by a migrated role before any Phase 1
    // Agent Loop begins.  Role jobs receive only this context, never the
    // mutable CSV paths or SQLite fallback.  Recovery reuses the same sealed
    // manifest and skips this mutable-file read entirely.
    if !mock {
        let mut needs_technical = false;
        let mut needs_jin10 = false;
        for role in roles {
            if !analyst_uses_file_store(config, role)? {
                continue;
            }
            needs_technical |= role == "analyst.technical";
            needs_jin10 |= role == "analyst.news_macro";
        }
        if needs_technical || needs_jin10 {
            let current_date = state
                .get("current_date")
                .and_then(Value::as_str)
                .context("state.current_date missing for FileStore input capture")?;
            let sources = phase1_input_sources(
                current_date,
                needs_technical,
                needs_jin10,
                &tickers_from_state(state),
            )?;
            let binding = capture_phase1_file_store_inputs(state, config, &sources)?;
            state["file_store_input"] = json!({
                "store_root": binding.store_root,
                "run_id": binding.run_id,
                "current_date": binding.current_date,
            });
        }
    }

    let mut jobs = Vec::new();
    for role in roles {
        if analyst_uses_file_store(config, role)? {
            // One FileStore artifact and Draft lifecycle per analyst+ticker.
            // The role cannot write, read, or finalize another ticker's unit.
            for ticker in tickers_from_state(state) {
                let mut ticker_state = state.clone();
                ticker_state["ticker"] = Value::String(ticker.clone());
                ticker_state["tickers"] = json!([ticker]);
                jobs.push(prepare_role_job(RoleRun {
                    state: ticker_state,
                    role,
                    phase: 1,
                    kind: "artifact",
                    round: None,
                    topic_id: None,
                    mock,
                    model_override,
                    reasoning_effort_override,
                    config,
                    prompt_path: config.prompts.analyst_path(role),
                })?);
            }
        } else {
            jobs.push(prepare_role_job(RoleRun {
                state: state.clone(),
                role,
                phase: 1,
                kind: "artifact",
                round: None,
                topic_id: None,
                mock,
                model_override,
                reasoning_effort_override,
                config,
                prompt_path: config.prompts.analyst_path(role),
            })?);
        }
    }
    debug!(job_count = jobs.len(), "phase 1 jobs prepared");
    let results = run_role_jobs(
        jobs,
        config.workflow.phase1_parallelism,
        config.workflow.agent_timeout_sec,
    )
    .await;

    let mut reports = serde_json::Map::new();
    let current_run_id = state
        .get("run_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    for result in results {
        let role = result.role.clone();
        let mut result = result;
        let file_store_authoritative = analyst_uses_file_store(config, &role)?;
        if file_store_authoritative && result.artifact.is_none() {
            let failure = result
                .error
                .as_deref()
                .unwrap_or("role execution failed before terminal finalize")
                .to_owned();
            let registration = config
                .authority_registry
                .registration(&role, ToolManagedProfile::AnalystReport)?;
            let store_root = state
                .get("store_root")
                .and_then(Value::as_str)
                .context("store_root missing for migrated Phase 1 Analyst")?;
            let ticker = result
                .tickers
                .first()
                .cloned()
                .context("migrated Phase 1 Analyst result has no ticker")?;
            let mut ticker_state = state.clone();
            ticker_state["ticker"] = Value::String(ticker.clone());
            ticker_state["tickers"] = json!([ticker]);
            let fallback = finalize_degraded_analyst_report(
                Path::new(store_root),
                &ticker_state,
                FileStoreDomainRuntimePlan {
                    role: role.clone(),
                    phase: 1,
                    profile: ToolManagedProfile::AnalystReport,
                    profile_version: registration.profile_version,
                    builder_version: registration.builder_version,
                    tickers: result.tickers.clone(),
                    visible_evidence_refs: BTreeSet::new(),
                    topic_id: None,
                    side: None,
                    round: None,
                    visible_claims: BTreeSet::new(),
                    fork: None,
                    trade_candidate_action: None,
                    portfolio_rating: None,
                    portfolio_current_weight: None,
                },
                &failure,
            )?;
            record_degraded_role(state, &result, &failure);
            result.artifact = Some(fallback);
        } else if !file_store_authoritative
            && result.artifact.is_none()
            && is_critical_role(config, &result.role)
            && !current_run_id.is_empty()
        {
            if let Some(artifact) = phase1_cached_analyst_artifact_fallback(
                conn,
                &current_run_id,
                &result.role,
                &result.tickers,
            )? {
                debug!(role = result.role, "using cached phase1 artifact fallback");
                result.artifact = Some(artifact);
            }
        }
        debug!(
            role,
            elapsed_ms = result.elapsed_ms,
            timed_out = result.timed_out,
            ok = result.artifact.is_some(),
            "phase 1 role finished"
        );
        // Prompt metrics are currently a no-op, but keep this call exclusive
        // to legacy ownership so it cannot become an accidental FileStore
        // profile SQLite write in a future change.
        if !file_store_authoritative {
            persist_prompt_metric(conn, &result);
        }
        record_role_job_metrics(state, &result);
        if file_store_authoritative {
            let artifact = result
                .artifact
                .clone()
                .context("FileStore Analyst must return a terminal canonical artifact")?;
            merge_file_store_phase1_artifact(&mut reports, &role, artifact)?;
        } else {
            let artifact = role_artifact_or_degraded(state, config, result)?;
            persist_artifact(conn, state, 1, &role, artifact.clone())?;
            reports.insert(role.clone(), artifact);
        }
    }
    state["analyst_reports"] = Value::Object(reports);
    // Materialize phase1_index in-process (no separate phase 1.5 / phase 15).
    materialize_phase1_index(
        conn,
        state,
        config,
        !phase1_has_file_store_analyst(config, roles)?,
    )?;
    Ok(())
}

/// The in-memory Phase 1 reducer is a read model for immediate downstream
/// scheduling only.  It is assembled from finalized FileStore artifacts and
/// is never a second persisted source of truth.
fn merge_file_store_phase1_artifact(
    reports: &mut serde_json::Map<String, Value>,
    role: &str,
    artifact: Value,
) -> Result<()> {
    if artifact.get("role").and_then(Value::as_str) != Some(role) {
        bail!("FileStore analyst artifact role differs from its planned role")
    }
    let per_ticker = artifact
        .get("per_ticker")
        .and_then(Value::as_object)
        .context("FileStore analyst artifact is missing per_ticker")?;
    if per_ticker.len() != 1 {
        bail!("FileStore analyst ticker unit must contain exactly one per_ticker entry")
    }
    let entry = reports.entry(role.to_owned()).or_insert_with(|| {
        json!({
            "id": role,
            "role": role,
            "profile": "analyst_report",
            "authority": "file_store",
            "per_ticker": {},
            "artifact_refs": [],
        })
    });
    let target = entry
        .get_mut("per_ticker")
        .and_then(Value::as_object_mut)
        .context("FileStore analyst state projection is malformed")?;
    for (ticker, payload) in per_ticker {
        if target.insert(ticker.clone(), payload.clone()).is_some() {
            bail!("duplicate FileStore Phase 1 analyst ticker unit for {role}/{ticker}")
        }
    }
    let refs = entry
        .get_mut("artifact_refs")
        .and_then(Value::as_array_mut)
        .context("FileStore analyst state projection artifact_refs is malformed")?;
    refs.push(json!({
        "artifact_id": artifact.get("artifact_id"),
        "content_hash": artifact.get("content_hash"),
        "source_payload_hash": artifact.get("source_payload_hash"),
    }));
    Ok(())
}

fn phase1_cached_analyst_artifact_fallback(
    conn: &rusqlite::Connection,
    current_run_id: &str,
    role: &str,
    tickers: &[String],
) -> Result<Option<Value>> {
    if tickers.is_empty() {
        return Ok(None);
    }
    let mut per_ticker = serde_json::Map::new();
    let mut template = None;

    if let Some(aggregate) =
        query_latest_phase1_artifact(conn, current_run_id, role, AGGREGATE_TICKER)?
    {
        if let Some(values) = aggregate.get("per_ticker").and_then(Value::as_object) {
            for ticker in tickers {
                if let Some(payload) = values.get(ticker) {
                    per_ticker.insert(ticker.clone(), payload.clone());
                }
            }
            template = Some(aggregate);
        }
    }

    for ticker in tickers {
        if per_ticker.contains_key(ticker) {
            continue;
        }
        if let Some(artifact) =
            query_latest_phase1_artifact(conn, current_run_id, role, ticker.as_str())?
        {
            let payload = artifact
                .get("per_ticker")
                .and_then(Value::as_object)
                .and_then(|values| values.get(ticker).cloned())
                .unwrap_or_else(|| artifact.clone());
            per_ticker.insert(ticker.clone(), payload);
            if template.is_none() {
                template = Some(artifact);
            }
        }
    }

    if per_ticker.is_empty() {
        return Ok(None);
    }

    let mut artifact = template.unwrap_or_else(|| json!({ "id": role, "role": role }));
    if let Some(object) = artifact.as_object_mut() {
        object.insert("status".to_string(), Value::String("degraded".to_string()));
        object.insert("degraded".to_string(), Value::Bool(true));
        object.insert("usable".to_string(), Value::Bool(false));
        object.insert("fallback".to_string(), json!("cached_db_artifact"));
        object.insert(
            "degraded_reason".to_string(),
            json!("using cached phase1 artifact fallback"),
        );
        object.insert(
            "report".to_string(),
            json!("phase1 artifact reused from cache due to live LLM gateway failure"),
        );
        object.insert(
            "probability_rationale".to_string(),
            json!("phase1 artifact reused from cache due to live LLM gateway failure"),
        );
        object.insert("per_ticker".to_string(), Value::Object(per_ticker));
    } else {
        return Ok(None);
    }

    Ok(Some(artifact))
}

fn query_latest_phase1_artifact(
    conn: &rusqlite::Connection,
    _current_run_id: &str,
    role: &str,
    ticker: &str,
) -> Result<Option<Value>> {
    let raw_json: Option<String> = conn
        .query_row(
            "SELECT summary_json FROM role_turn_summaries \
             WHERE role = ?1 AND phase = 1 AND summary_type = 'artifact' \
             AND ticker = ?2 \
             ORDER BY created_at_ms DESC LIMIT 1",
            params![role, ticker],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    Ok(raw_json.and_then(|raw| serde_json::from_str(&raw).ok()))
}

async fn run_phase2(
    mut conn: rusqlite::Connection,
    state: &mut Value,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    max_debate_rounds: i64,
    max_topics: i64,
    config: &RuntimeConfig,
) -> Result<rusqlite::Connection> {
    if phase2_uses_file_store(config)? {
        run_phase2_file_store(
            &conn,
            state,
            model_override,
            reasoning_effort_override,
            max_debate_rounds,
            max_topics,
            config,
        )
        .await?;
        return Ok(conn);
    }
    let db_path = state
        .get("db_path")
        .and_then(Value::as_str)
        .map(|s| s.to_string())
        .context("db_path missing from state")?;

    // Build the shared Bull/Bear warmup checkpoint, then run Topic Generator
    // independently. Topic roles fork from the checkpoint appropriate to the role.
    let model_override_owned = model_override.map(|s| s.to_string());
    let reasoning_effort_override_owned = reasoning_effort_override.map(|s| s.to_string());
    let mut warmup_state = state.clone();
    warmup_state["role_job_metrics"] = json!([]);
    let (warmup, warmup_metrics) = run_phase2_shared_warmup(
        warmup_state,
        model_override,
        reasoning_effort_override,
        config,
    )
    .await?;
    let warmup_degraded = warmup
        .get("degraded")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    state["phase2_warmup"] = warmup;
    if warmup_degraded {
        state["degraded"] = json!(true);
    }
    merge_role_job_metrics(state, &warmup_metrics);
    let topics = run_phase2_topic_generation(
        &mut conn,
        state,
        model_override,
        reasoning_effort_override,
        config,
    )
    .await?
    .into_iter()
    .take(max_topics.max(1) as usize)
    .collect::<Vec<_>>();
    debug!(
        topic_count = topics.len(),
        "phase 2 shared warmup and topic generation ready"
    );
    state["debate_turns"] = json!([]);

    if topics.is_empty() {
        run_phase2_final_reducer(
            &mut conn,
            state,
            model_override,
            reasoning_effort_override,
            config,
        )
        .await?;
        return Ok(conn);
    }

    let common_ground = state
        .get("topic_generation_artifact")
        .and_then(|a| a.get("common_ground"))
        .cloned()
        .unwrap_or_else(|| json!({}));

    let readonly_state = json!({
        "run_id": state.get("run_id").cloned().unwrap_or(Value::Null),
        "ticker": state.get("ticker").cloned().unwrap_or(Value::Null),
        "tickers": state.get("tickers").cloned().unwrap_or_else(|| json!([])),
        "current_date": state.get("current_date").cloned().unwrap_or(Value::Null),
        "lang": state.get("lang").cloned().unwrap_or(Value::Null),
        "window_days": state.get("window_days").cloned().unwrap_or(Value::Null),
        "mode": state.get("mode").cloned().unwrap_or(Value::Null),
        "mock": state.get("mock").cloned().unwrap_or(Value::Null),
        "db_path": state.get("db_path").cloned().unwrap_or(Value::Null),
        "run_dir": state.get("run_dir").cloned().unwrap_or(Value::Null),
        "phase1_index": state.get("phase1_index").cloned().unwrap_or(Value::Null),
        "phase1_brief_md": state.get("phase1_brief_md").cloned().unwrap_or(Value::Null),
        "phase_summary_tables": state.get("phase_summary_tables").cloned().unwrap_or_else(|| json!({})),
        "phase_summary_memory": state.get("phase_summary_memory").cloned().unwrap_or_else(|| json!({})),
        "phase_compress": state.get("phase_compress").cloned().unwrap_or_else(|| json!({})),
        "phase2_warmup": state.get("phase2_warmup").cloned().unwrap_or(Value::Null),
        "topic_generation_turn_id": state.get("topic_generation_turn_id").cloned().unwrap_or(Value::Null),
        "common_ground": common_ground,
        "late_evidence": state.get("late_evidence").cloned().unwrap_or_else(|| json!([])),
        "degraded": state.get("degraded").cloned().unwrap_or(Value::Null),
        "debug": state.get("debug").cloned().unwrap_or(Value::Null),
    });

    let mut topic_futures = Vec::new();
    for topic in topics {
        let db_path = db_path.clone();
        let state_clone = readonly_state.clone();
        let model_ov = model_override_owned.clone();
        let reasoning_ov = reasoning_effort_override_owned.clone();
        let config_clone = config.clone();
        topic_futures.push(async move {
            let topic_conn = orchestrator_sql::connect(&db_path).with_context(|| {
                format!(
                    "failed to open topic connection for {}",
                    topic_id_from_topic(&topic)
                )
            })?;
            run_one_topic_debate(
                topic_conn,
                &state_clone,
                topic,
                model_ov,
                reasoning_ov,
                max_debate_rounds,
                &config_clone,
            )
            .await
        });
    }

    let results: Vec<Result<TopicDebateResult>> = futures::future::join_all(topic_futures).await;

    let mut failed_topics = Vec::new();
    let mut succeeded = 0usize;
    for result in results {
        match result {
            Ok((topic_id, turns, topic_state, role_metrics)) => {
                merge_role_job_metrics(state, &role_metrics);
                if let Some(turns_arr) = state["debate_turns"].as_array_mut() {
                    turns_arr.extend(turns);
                }
                upsert_topic_debate_state(state, &topic_id, topic_state);
                succeeded += 1;
            }
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "phase 2 topic debate failed, skipping topic"
                );
                failed_topics.push(error.to_string());
            }
        }
    }
    if succeeded == 0 && !failed_topics.is_empty() {
        bail!(
            "all phase 2 topic debates failed: {}",
            failed_topics.join("; ")
        );
    }
    if !failed_topics.is_empty() {
        state["degraded"] = json!(true);
        if !state.get("degraded_report").is_some_and(Value::is_object) {
            state["degraded_report"] = json!({"is_degraded": true, "roles": []});
        }
        state["phase2_failed_topics"] = json!(failed_topics);
    }

    run_phase2_final_reducer(
        &mut conn,
        state,
        model_override,
        reasoning_effort_override,
        config,
    )
    .await?;
    Ok(conn)
}

/// FileStore-authoritative Phase 2.  Every model role enters through the
/// typed domain binding; this function only creates an in-memory projection
/// for the existing Rust reducer.  It deliberately does not call any SQLite
/// message/session/artifact writer.
#[allow(clippy::too_many_arguments)]
async fn run_phase2_file_store(
    conn: &rusqlite::Connection,
    state: &mut Value,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    max_debate_rounds: i64,
    max_topics: i64,
    config: &RuntimeConfig,
) -> Result<()> {
    let run_id = state
        .get("run_id")
        .and_then(Value::as_str)
        .context("run_id missing for FileStore Phase 2")?
        .to_owned();
    let warmup_session = format!("{run_id}:phase2:warmup:shared");
    let warmup_turn = warmup_session.clone();
    let _warmup = run_file_store_phase2_role(
        conn,
        state,
        "mediator.topic",
        "warmup",
        Some(0),
        None,
        warmup_turn.clone(),
        warmup_session.clone(),
        Some(steer_payload("warmup", &json!({"allow_ready": true}))),
        model_override,
        reasoning_effort_override,
        config,
    )
    .await?;
    state["phase2_warmup"] = json!({
        "session_id": warmup_session,
        "turn_id": warmup_turn,
        "ready": true,
        "warmup_ready": true,
        "status": "ready",
        "response": "准备完毕",
        "authority": "file_store"
    });

    let baseline = build_topic_generation_artifact(state);
    let topic_session = format!("{run_id}:phase2:topic-generator");
    let topic_turn = topic_session.clone();
    let generated = run_file_store_phase2_role(
        conn,
        state,
        "mediator.topic",
        "topic_generation",
        None,
        None,
        topic_turn.clone(),
        topic_session.clone(),
        topic_generation_steer(),
        model_override,
        reasoning_effort_override,
        config,
    )
    .await?;
    state["topic_generation_session_id"] = json!(topic_session);
    state["topic_generation_turn_id"] = json!(topic_turn);
    let topic_generation = project_file_store_topic_generation(&baseline, &generated, state)?;
    state["topic_generation_artifact"] = topic_generation.clone();
    let topics = topics_from_generation_artifact(&topic_generation)
        .into_iter()
        .take(max_topics.max(1) as usize)
        .collect::<Vec<_>>();
    state["debate_topics"] = json!(topics);
    state["debate_turns"] = json!([]);
    state["topic_debate_states"] = json!({});
    state["phase2_file_store_sessions"] = json!({});

    for topic in topics {
        run_one_file_store_topic_debate(
            conn,
            state,
            topic,
            model_override,
            reasoning_effort_override,
            max_debate_rounds,
            config,
        )
        .await?;
    }
    let artifact = build_debate_state_artifact(state, config);
    state["debate_state_artifact"] = artifact.clone();
    state["debate_brief_md"] = Value::String(reducer_brief_md(&artifact));
    state["phase2_authority"] = json!("file_store");
    record_local_debug_artifact(
        state,
        2,
        "reducer.debate_final",
        PathBuf::from("outputs/debug/phase2/debate_final.json"),
        "runtime",
        &artifact,
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_file_store_phase2_role(
    conn: &rusqlite::Connection,
    state: &mut Value,
    role: &str,
    kind: &str,
    round: Option<i64>,
    topic_id: Option<&str>,
    session_id: String,
    turn_id: String,
    steer: Option<String>,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<Value> {
    let prompt_path = match role {
        "mediator.topic" if kind == "warmup" => config.prompts.path_for("researcher.warmup"),
        "mediator.topic" => config.prompts.path_for("mediator.topic"),
        _ => config.prompts.path_for(role),
    }
    .with_context(|| format!("missing FileStore Phase 2 prompt for {role}/{kind}"))?;
    run_single_steer_role_job(
        SteerRoleRun {
            state: state.clone(),
            role,
            phase: 2,
            kind,
            round,
            topic_id,
            mock: is_mock(state),
            model_override,
            reasoning_effort_override,
            config,
            prompt_path: Some(prompt_path.as_path()),
            session_id,
            turn_id,
            steer,
        },
        if role == "mediator.topic_controller" {
            config.workflow.reducer_timeout_sec
        } else {
            config.workflow.agent_timeout_sec
        },
        config,
        state,
        conn,
    )
    .await
}

fn project_file_store_topic_generation(
    baseline: &Value,
    canonical: &Value,
    state: &Value,
) -> Result<Value> {
    let payload = canonical
        .get("payload")
        .context("FileStore topic generation artifact missing payload")?;
    let topics = payload
        .get("topics")
        .and_then(Value::as_array)
        .context("FileStore topic generation artifact missing topics")?
        .iter()
        .map(|topic| {
            json!({
                "topic_id": topic.get("topic_id").cloned().unwrap_or(Value::Null),
                "topic": topic.get("topic").cloned().unwrap_or(Value::Null),
                "decision_hinge": topic.get("decision_hinge").cloned().unwrap_or(Value::Null),
                "evidence_refs": topic.get("evidence_refs").cloned().unwrap_or_else(|| json!([])),
                "tickers": tickers_from_state(state),
                "why_debate": "ToolManaged Topic Generator selected this Rust-scoped topic."
            })
        })
        .collect::<Vec<_>>();
    let mut projection = baseline.clone();
    projection["common_ground"] = payload
        .get("common_ground")
        .cloned()
        .unwrap_or_else(|| json!(""));
    // Rust owns the Phase 1 actionability gate.  A mock/live Topic Generator
    // cannot manufacture a debate unit when the upstream evidence boundary
    // is closed.
    projection["topics"] =
        if baseline.get("evidence_actionable").and_then(Value::as_bool) == Some(true) {
            json!(topics)
        } else {
            json!([])
        };
    let actionable = projection["topics"]
        .as_array()
        .is_some_and(|items| !items.is_empty());
    projection["actionable"] = json!(actionable);
    projection["debate_required"] = json!(actionable);
    projection["status"] = json!(if actionable { "ready" } else { "skipped" });
    projection["material_conflict_count"] =
        json!(projection["topics"].as_array().map(Vec::len).unwrap_or(0));
    Ok(projection)
}

#[allow(clippy::too_many_arguments)]
async fn run_one_file_store_topic_debate(
    conn: &rusqlite::Connection,
    state: &mut Value,
    topic: Value,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    max_debate_rounds: i64,
    config: &RuntimeConfig,
) -> Result<()> {
    let topic_id = topic_id_from_topic(&topic);
    let run_id = state
        .get("run_id")
        .and_then(Value::as_str)
        .unwrap_or("run")
        .to_owned();
    let topic_state = json!({"topic": topic.clone(), "turns": [], "controller_artifacts": []});
    upsert_topic_debate_state(state, &topic_id, topic_state);
    let common_ground = state
        .pointer("/topic_generation_artifact/common_ground")
        .cloned()
        .unwrap_or(Value::Null);
    let fork = Some(steer_payload(
        "topic_fork",
        &json!({"user_message": topic_fork_user_message(&topic, &common_ground), "topic": topic}),
    ));
    let mut seed_artifacts = Vec::new();
    for (side, role) in [
        ("bull", "researcher.bull.initial"),
        ("bear", "researcher.bear.initial"),
    ] {
        let session = format!("{run_id}:phase2:{topic_id}:{side}:seed");
        let turn = format!("{topic_id}:{side}:seed");
        let artifact = run_file_store_phase2_role(
            conn,
            state,
            role,
            "bull_seed",
            Some(1),
            Some(&topic_id),
            session.clone(),
            turn.clone(),
            fork.clone(),
            model_override,
            reasoning_effort_override,
            config,
        )
        .await?;
        remember_file_store_phase2_session(state, &topic_id, side, &session, &turn);
        append_file_store_phase2_turn(state, &topic_id, role, "seed", 1, artifact.clone());
        seed_artifacts.push((side, artifact));
    }
    let mut controller = run_file_store_phase2_role(
        conn,
        state,
        "mediator.topic_controller",
        "controller_packet",
        Some(1),
        Some(&topic_id),
        format!("{run_id}:phase2:{topic_id}:controller:1"),
        format!("{topic_id}:controller:1"),
        Some(steer_payload(
            "seed_claims",
            &json!({"bull_seed": seed_artifacts[0].1, "bear_seed": seed_artifacts[1].1}),
        )),
        model_override,
        reasoning_effort_override,
        config,
    )
    .await?;
    let mut controller_projection = project_file_store_controller(&controller, state, &topic_id);
    append_file_store_phase2_turn(
        state,
        &topic_id,
        "mediator.topic_controller",
        "controller",
        1,
        controller.clone(),
    );
    set_topic_controller_state(state, &topic_id, controller_projection.clone());
    append_topic_controller_artifact(state, &topic_id, controller_projection.clone());

    for round in 2..=max_debate_rounds.max(2) {
        let mut responses = Vec::new();
        for (side, role) in [
            ("bull", "researcher.bull.interaction"),
            ("bear", "researcher.bear.interaction"),
        ] {
            let session = format!("{run_id}:phase2:{topic_id}:{side}:response:{round}");
            let turn = format!("{topic_id}:{side}:response:{round}");
            let artifact = run_file_store_phase2_role(
                conn,
                state,
                role,
                "response",
                Some(round),
                Some(&topic_id),
                session.clone(),
                turn.clone(),
                Some(steer_payload(
                    "point_debate",
                    &json!({"controller": controller, "side": side}),
                )),
                model_override,
                reasoning_effort_override,
                config,
            )
            .await?;
            remember_file_store_phase2_session(state, &topic_id, side, &session, &turn);
            append_file_store_phase2_turn(
                state,
                &topic_id,
                role,
                "response",
                round,
                artifact.clone(),
            );
            responses.push((side, artifact));
        }
        controller = run_file_store_phase2_role(
            conn,
            state,
            "mediator.topic_controller",
            "controller_packet",
            Some(round),
            Some(&topic_id),
            format!("{run_id}:phase2:{topic_id}:controller:{round}"),
            format!("{topic_id}:controller:{round}"),
            Some(steer_payload(
                "debater_packets",
                &json!({"bull_packet": responses[0].1, "bear_packet": responses[1].1}),
            )),
            model_override,
            reasoning_effort_override,
            config,
        )
        .await?;
        controller_projection = project_file_store_controller(&controller, state, &topic_id);
        append_file_store_phase2_turn(
            state,
            &topic_id,
            "mediator.topic_controller",
            "controller",
            round,
            controller.clone(),
        );
        set_topic_controller_state(state, &topic_id, controller_projection.clone());
        append_topic_controller_artifact(state, &topic_id, controller_projection.clone());
        if controller_projection
            .pointer("/soft_control/should_continue")
            .and_then(Value::as_bool)
            == Some(false)
        {
            break;
        }
    }
    Ok(())
}

fn remember_file_store_phase2_session(
    state: &mut Value,
    topic_id: &str,
    side: &str,
    session_id: &str,
    turn_id: &str,
) {
    if !state
        .get("phase2_file_store_sessions")
        .is_some_and(Value::is_object)
    {
        state["phase2_file_store_sessions"] = json!({});
    }
    state["phase2_file_store_sessions"][topic_id][side] =
        json!({"session_id": session_id, "turn_id": turn_id});
}

fn append_file_store_phase2_turn(
    state: &mut Value,
    topic_id: &str,
    role: &str,
    kind: &str,
    round: i64,
    artifact: Value,
) {
    let turn = json!({"role": role, "kind": kind, "round": round, "topic_id": topic_id, "artifact": artifact});
    append_topic_turn(state, topic_id, turn.clone());
    if let Some(turns) = state.get_mut("debate_turns").and_then(Value::as_array_mut) {
        turns.push(turn);
    }
}

fn project_file_store_controller(canonical: &Value, state: &Value, topic_id: &str) -> Value {
    let payload = canonical.get("payload").unwrap_or(&Value::Null);
    let refs = state
        .pointer(&format!("/topic_debate_states/{topic_id}/turns"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(|turn| {
            turn.pointer("/artifact/evidence_refs")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
        .cloned()
        .collect::<Vec<_>>();
    let hinges = payload
        .get("decision_hinges")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|hinge| json!({"hinge": hinge, "evidence_refs": refs}))
        .collect::<Vec<_>>();
    json!({
        "role": "mediator.topic_controller",
        "artifact_type": "phase2_controller_artifact",
        "topic_id": topic_id,
        "agreed_facts": payload.get("agreed_facts").cloned().unwrap_or_else(|| json!([])),
        "decision_hinges": hinges,
        "claim_ledger": payload.get("claim_statuses").cloned().unwrap_or_else(|| json!([])),
        "next_steers": payload.get("routes").cloned().unwrap_or_else(|| json!([])),
        "soft_control": {"should_continue": payload.get("should_continue").and_then(Value::as_bool).unwrap_or(false)}
    })
}

fn topic_fork_user_message(topic: &Value, common_ground: &Value) -> String {
    let title = topic
        .get("topic")
        .and_then(Value::as_str)
        .or_else(|| topic.get("topic_id").and_then(Value::as_str))
        .unwrap_or("topic");
    let topic_id = topic
        .get("topic_id")
        .and_then(Value::as_str)
        .unwrap_or(title);
    let hinge = topic.get("decision_hinge").cloned().unwrap_or(Value::Null);
    let cg = serde_json::to_string(common_ground).unwrap_or_else(|_| "{}".into());
    PHASE2_TOPIC_FORK_USER_PROMPT
        .replace("{{title}}", title)
        .replace("{{topic_id}}", topic_id)
        .replace("{{decision_hinge}}", &hinge.to_string())
        .replace("{{common_ground}}", &cg)
        .trim()
        .to_string()
}

async fn run_phase2_shared_warmup(
    mut state: Value,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<(Value, Value)> {
    let run_id = state
        .get("run_id")
        .and_then(Value::as_str)
        .unwrap_or("run")
        .to_string();
    let metadata = shared_warmup_metadata(&run_id);
    if is_mock(&state) {
        let mut ready = metadata;
        ready["ready"] = json!(true);
        ready["warmup_ready"] = json!(true);
        ready["mode"] = json!("mock");
        ready["llm_calls"] = json!(0);
        ready["degraded"] = json!(false);
        return Ok((ready, json!([])));
    }

    let role = "mediator.topic";
    let session_id = metadata["session_id"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let turn_id = metadata["turn_id"].as_str().unwrap_or_default().to_string();
    let db_path = state
        .get("db_path")
        .and_then(Value::as_str)
        .context("db_path missing for phase 2 warmup")?;
    let conn = orchestrator_sql::connect(db_path)?;
    let prompt_path = config
        .prompts
        .path_for("researcher.warmup")
        .context("missing shared Phase 2 warmup prompt")?
        .clone();
    let mut last_error: Option<String> = None;
    let mut status = String::new();
    for attempt in 1..=2 {
        let artifact = match run_single_steer_role_job(
            SteerRoleRun {
                state: state.clone(),
                role,
                phase: 2,
                kind: "warmup",
                round: Some(0),
                topic_id: None,
                mock: false,
                model_override,
                reasoning_effort_override,
                config,
                prompt_path: Some(prompt_path.as_path()),
                session_id: session_id.clone(),
                turn_id: turn_id.clone(),
                steer: Some(steer_payload(
                    "warmup",
                    &json!({
                        "allow_ready": true
                    }),
                )),
            },
            config.workflow.agent_timeout_sec,
            config,
            &mut state,
            &conn,
        )
        .await
        {
            Ok(artifact) => artifact,
            Err(error) => {
                last_error = Some(error.to_string());
                if attempt == 1 {
                    tracing::warn!(role, attempt, "shared Phase 2 warmup failed; retrying once");
                }
                continue;
            }
        };

        status = artifact
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if artifact.get("status").and_then(Value::as_str) == Some("ready")
            && artifact.get("response").and_then(Value::as_str) == Some("准备完毕")
        {
            let mut ready_metadata = metadata.clone();
            ready_metadata["ready"] = json!(true);
            ready_metadata["mode"] = json!("live");
            ready_metadata["llm_calls"] = json!(1);
            ready_metadata["status"] = json!("ready");
            ready_metadata["response"] = json!("准备完毕");
            ready_metadata["warmup_ready"] = json!(true);
            ready_metadata["degraded"] = json!(false);
            ready_metadata["artifact_status"] =
                artifact.get("status").cloned().unwrap_or(Value::Null);
            ready_metadata["artifact_response"] =
                artifact.get("response").cloned().unwrap_or(Value::Null);
            return Ok((
                ready_metadata,
                state
                    .get("role_job_metrics")
                    .cloned()
                    .unwrap_or_else(|| json!([])),
            ));
        }
        last_error = Some(format!(
            "unexpected artifact response (status={:?}, response={:?})",
            artifact.get("status"),
            artifact.get("response")
        ));
        if attempt == 1 {
            tracing::warn!(
                role,
                "shared Phase 2 warmup handshake failed; retrying once"
            );
        }
    }
    tracing::warn!(
        ready = false,
        status = status,
        error = last_error
            .as_deref()
            .unwrap_or("shared Phase 2 warmup did not return the required 准备完毕 handshake"),
        "shared Phase 2 warmup handshake fallback applied"
    );
    state["degraded"] = json!(true);
    if !state.get("degraded_report").is_some_and(Value::is_object) {
        state["degraded_report"] = json!({"is_degraded": true, "roles": []});
    }
    if let Some(roles) = state
        .get_mut("degraded_report")
        .and_then(|report| report.get_mut("roles"))
        .and_then(Value::as_array_mut)
    {
        roles.push(json!({
            "role": role,
            "phase": 2,
            "kind": "warmup",
            "error": last_error.clone().unwrap_or_else(|| "warmup failure".to_string()),
            "message": "shared Phase 2 warmup did not return the required 准备完毕 handshake"
        }));
    }
    let mut fallback = metadata;
    fallback["ready"] = json!(false);
    fallback["mode"] = json!("live");
    fallback["llm_calls"] = json!(1);
    fallback["status"] = json!("degraded");
    fallback["response"] = json!("准备完毕");
    fallback["warmup_ready"] = json!(false);
    fallback["degraded"] = json!(true);
    fallback["degraded_reason"] =
        Value::String(last_error.unwrap_or_else(|| "phase2 warmup failed".to_string()));
    Ok((
        fallback,
        state
            .get("role_job_metrics")
            .cloned()
            .unwrap_or_else(|| json!([])),
    ))
}

fn shared_warmup_metadata(run_id: &str) -> Value {
    let id = format!("{run_id}:phase2:warmup:shared");
    json!({
        "session_id": id.clone(),
        "turn_id": id,
        "ack": "准备完毕",
        "response": "准备完毕",
        "warmup_ready": false,
        "status": "ready"
    })
}

async fn run_one_topic_debate(
    mut conn: rusqlite::Connection,
    state: &Value,
    topic: Value,
    model_override: Option<String>,
    reasoning_effort_override: Option<String>,
    max_debate_rounds: i64,
    config: &RuntimeConfig,
) -> Result<TopicDebateResult> {
    let topic_id = topic_id_from_topic(&topic);
    debug!(topic_id, "phase 2 steer-room topic debate starting");

    let model_override_ref = model_override.as_deref();
    let reasoning_effort_ref = reasoning_effort_override.as_deref();
    let mut local_state = state.clone();
    let sessions = steer_topic_sessions(&local_state, &topic_id);
    let common_ground = local_state
        .get("common_ground")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let fork_msg = topic_fork_user_message(&topic, &common_ground);
    let initial_topic_state = json!({
        "topic": topic.clone(),
        "mode": "steer_room_fork",
        "warmup_ready": local_state
            .get("phase2_warmup")
            .and_then(|warmup| warmup.get("ready"))
            .cloned()
            .unwrap_or(json!(false)),
        "fork_user_message": fork_msg,
        "turns": [],
        "controller_artifacts": [],
        "thread": sessions
    });
    upsert_topic_debate_state(&mut local_state, &topic_id, initial_topic_state);
    let mut turns = Vec::new();

    let topic_steer = Some(steer_payload(
        "topic_fork",
        &json!({
            "user_message": topic_fork_user_message(&topic, &common_ground),
            "common_ground": common_ground,
            "topic": topic
        }),
    ));

    let bull_seed = run_topic_steer_step(
        &mut conn,
        &mut local_state,
        "researcher.bull.initial",
        "bull_seed",
        1,
        &topic_id,
        &sessions,
        topic_steer.clone(),
        model_override_ref,
        reasoning_effort_ref,
        config,
        mode_prompt_path(
            config.prompts.path_for("researcher.bull.initial").unwrap(),
            state,
        ),
    )
    .await?;
    let bear_seed = run_topic_steer_step(
        &mut conn,
        &mut local_state,
        "researcher.bear.initial",
        "bear_seed",
        1,
        &topic_id,
        &sessions,
        topic_steer,
        model_override_ref,
        reasoning_effort_ref,
        config,
        mode_prompt_path(
            config.prompts.path_for("researcher.bear.initial").unwrap(),
            state,
        ),
    )
    .await?;
    turns.push(bull_seed.clone());
    turns.push(bear_seed.clone());
    let mut latest_bull = bull_seed;
    let mut latest_bear = bear_seed;

    let mut mediator_output = run_topic_steer_step(
        &mut conn,
        &mut local_state,
        "mediator.topic_controller",
        "controller_packet",
        1,
        &topic_id,
        &sessions,
        Some(steer_payload(
            "seed_claims",
            &json!({
                "bull_seed": compact_debate_turn(&latest_bull),
                "bear_seed": compact_debate_turn(&latest_bear)
            }),
        )),
        model_override_ref,
        reasoning_effort_ref,
        config,
        config
            .prompts
            .path_for("mediator.topic_controller")
            .unwrap()
            .clone(),
    )
    .await?;
    turns.push(mediator_output.clone());

    // Sequential point debate: bull rebuts latest bear claims, then bear rebuts
    // this-round bull claims, then mediator packages the next claim ledger.
    for round in 2..=max_debate_rounds.max(2) {
        let bull_steer =
            build_point_debate_steer(&mediator_output, "bull", &latest_bear, &latest_bull);
        let bull_rebuttal = run_topic_steer_step(
            &mut conn,
            &mut local_state,
            "researcher.bull.interaction",
            "bull_packet",
            round,
            &topic_id,
            &sessions,
            Some(bull_steer),
            model_override_ref,
            reasoning_effort_ref,
            config,
            config
                .prompts
                .path_for("researcher.bull.interaction")
                .unwrap()
                .clone(),
        )
        .await?;
        latest_bull = bull_rebuttal.clone();

        let bear_steer =
            build_point_debate_steer(&mediator_output, "bear", &latest_bull, &latest_bear);
        let bear_rebuttal = run_topic_steer_step(
            &mut conn,
            &mut local_state,
            "researcher.bear.interaction",
            "bear_packet",
            round,
            &topic_id,
            &sessions,
            Some(bear_steer),
            model_override_ref,
            reasoning_effort_ref,
            config,
            config
                .prompts
                .path_for("researcher.bear.interaction")
                .unwrap()
                .clone(),
        )
        .await?;
        latest_bear = bear_rebuttal.clone();

        mediator_output = run_topic_steer_step(
            &mut conn,
            &mut local_state,
            "mediator.topic_controller",
            if round == max_debate_rounds.max(2) {
                "topic_summary_final"
            } else {
                "controller_packet"
            },
            round,
            &topic_id,
            &sessions,
            Some(steer_payload(
                "debater_packets",
                &json!({
                    "bull_packet": compact_debate_turn(&latest_bull),
                    "bear_packet": compact_debate_turn(&latest_bear)
                }),
            )),
            model_override_ref,
            reasoning_effort_ref,
            config,
            config
                .prompts
                .path_for("mediator.topic_controller")
                .unwrap()
                .clone(),
        )
        .await?;
        turns.push(bull_rebuttal);
        turns.push(bear_rebuttal);
        turns.push(mediator_output.clone());

        let should_continue = mediator_output
            .get("artifact")
            .and_then(|a| a.get("soft_control"))
            .and_then(|sc| sc.get("should_continue"))
            .and_then(Value::as_bool);
        if should_continue == Some(false) {
            debug!(
                topic_id,
                round, "phase 2 mediator soft-stop; ending topic debate early"
            );
            break;
        }
    }

    let turn_count = turns.len();
    debug!(
        topic_id,
        turn_count, "phase 2 topic debate completed (parallel)"
    );

    let topic_state = local_state
        .get("topic_debate_states")
        .and_then(|s| s.get(&topic_id))
        .cloned()
        .unwrap_or_else(|| json!({}));

    Ok((
        topic_id,
        turns,
        topic_state,
        local_state
            .get("role_job_metrics")
            .cloned()
            .unwrap_or_else(|| json!([])),
    ))
}

fn steer_topic_sessions(state: &Value, topic_id: &str) -> Value {
    let run_id = state.get("run_id").and_then(Value::as_str).unwrap_or("run");
    json!({
        "bull": {
            "session_id": format!("{run_id}:phase2:{topic_id}:bull"),
            "turn_id": format!("turn-{topic_id}-bull-initial")
        },
        "bear": {
            "session_id": format!("{run_id}:phase2:{topic_id}:bear"),
            "turn_id": format!("turn-{topic_id}-bear-initial")
        },
        "mediator": {
            "session_id": format!("{run_id}:phase2:{topic_id}:mediator"),
            "turn_id": format!("turn-{topic_id}-mediator")
        }
    })
}

/// Initial and interaction roles must not share a turn_id — shared history drops the
/// interaction role prompt and burns max_model_calls on schema mismatch.
fn steer_turn_id_for_role(topic_id: &str, role: &str) -> String {
    if role.contains("bull.initial") {
        format!("turn-{topic_id}-bull-initial")
    } else if role.contains("bull.interaction") {
        format!("turn-{topic_id}-bull-interaction")
    } else if role.contains("bear.initial") {
        format!("turn-{topic_id}-bear-initial")
    } else if role.contains("bear.interaction") {
        format!("turn-{topic_id}-bear-interaction")
    } else {
        format!("turn-{topic_id}-mediator")
    }
}

fn fork_source_turn_id(state: &Value, topic_id: &str, role: &str) -> Option<String> {
    if role == "mediator.topic_controller" {
        return state
            .get("topic_generation_turn_id")?
            .as_str()
            .filter(|turn_id| !turn_id.is_empty())
            .map(ToString::to_string);
    }
    if role.contains("bull.initial") || role.contains("bear.initial") {
        return state
            .get("phase2_warmup")?
            .get("turn_id")?
            .as_str()
            .filter(|turn_id| !turn_id.is_empty())
            .map(ToString::to_string);
    }
    if role.contains("bull.interaction") {
        return Some(format!("turn-{topic_id}-bull-initial"));
    }
    if role.contains("bear.interaction") {
        return Some(format!("turn-{topic_id}-bear-initial"));
    }
    None
}

fn attach_fork_source(
    steer: Option<String>,
    source_turn_id: Option<String>,
    include_prompt_on_fork: bool,
) -> Option<String> {
    let steer = steer?;
    let Some(source_turn_id) = source_turn_id else {
        return Some(steer);
    };
    let Ok(mut value) = serde_json::from_str::<Value>(&steer) else {
        return Some(steer);
    };
    value["fork_from_turn_id"] = Value::String(source_turn_id);
    if include_prompt_on_fork {
        value["include_prompt_on_fork"] = Value::Bool(true);
    }
    Some(value.to_string())
}

fn topic_generation_steer() -> Option<String> {
    Some(steer_payload("topic_generation", &json!({})))
}

#[allow(clippy::too_many_arguments)]
async fn run_topic_steer_step(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    role: &str,
    kind: &str,
    round: i64,
    topic_id: &str,
    sessions: &Value,
    steer: Option<String>,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
    prompt_path: PathBuf,
) -> Result<Value> {
    let steer = attach_fork_source(
        steer,
        fork_source_turn_id(state, topic_id, role),
        // Forked turns inherit prior dynamic prompts.  Always inject the
        // current role prompt as well, otherwise an initial warm-up suffix
        // can make a seed or interaction repeat the ready handshake instead
        // of emitting its required packet.
        role == "mediator.topic_controller"
            || role == "researcher.bull.initial"
            || role == "researcher.bear.initial"
            || role == "researcher.bull.interaction"
            || role == "researcher.bear.interaction",
    );
    let session_key = if role.contains("bull") {
        "bull"
    } else if role.contains("bear") {
        "bear"
    } else {
        "mediator"
    };
    let session = sessions
        .get(session_key)
        .cloned()
        .unwrap_or_else(|| json!({}));
    let artifact = run_single_steer_role_job(
        SteerRoleRun {
            state: state.clone(),
            role,
            phase: 2,
            kind,
            round: Some(round),
            topic_id: Some(topic_id),
            mock: is_mock(state),
            model_override,
            reasoning_effort_override,
            config,
            prompt_path: Some(prompt_path.as_path()),
            session_id: session
                .get("session_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            turn_id: steer_turn_id_for_role(topic_id, role),
            steer,
        },
        if role == "mediator.topic_controller" {
            config.workflow.reducer_timeout_sec
        } else {
            config.workflow.agent_timeout_sec
        },
        config,
        state,
        conn,
    )
    .await?;
    persist_message_with_topic(
        conn,
        state,
        2,
        role,
        kind,
        Some(round),
        Some(topic_id),
        artifact.clone(),
    )?;
    let turn = json!({
        "role": role,
        "phase": 2,
        "kind": kind,
        "round": round,
        "topic_id": topic_id,
        "artifact": artifact,
        "session": session
    });
    append_topic_turn(state, topic_id, turn.clone());
    if role == "mediator.topic_controller" {
        set_topic_controller_state(state, topic_id, turn["artifact"].clone());
        append_topic_controller_artifact(state, topic_id, turn["artifact"].clone());
    }
    Ok(turn)
}

fn steer_payload(kind: &str, value: &Value) -> String {
    json!({"kind": kind, "payload": value}).to_string()
}

/// Build a claim-level debate steer so bull/bear must address the opponent's points.
fn build_point_debate_steer(
    controller_turn: &Value,
    side: &str,
    opponent_turn: &Value,
    own_previous_turn: &Value,
) -> String {
    let mediator_instruction = mediator_instruction_for_side(controller_turn, side);
    let opponent_packet = compact_debate_turn(opponent_turn);
    let opponent_claims = extract_addressable_claims(&opponent_packet);
    let accepted_for_you = accepted_claims_for_side(controller_turn, side);
    json!({
        "kind": "point_debate",
        "side": side,
        "mediator_instruction": mediator_instruction,
        "opponent_packet": opponent_packet,
        "opponent_claims_to_address": opponent_claims,
        "accepted_for_you": accepted_for_you,
        "own_previous_packet": compact_debate_turn(own_previous_turn),
        "reply_to_required": true
    })
    .to_string()
}

fn mediator_instruction_for_side(controller_turn: &Value, side: &str) -> Value {
    let artifact = controller_turn.get("artifact").unwrap_or(controller_turn);
    let keys = match side {
        "bull" => ["bull", "researcher.bull.interaction", "to_bull"],
        _ => ["bear", "researcher.bear.interaction", "to_bear"],
    };
    artifact
        .get("next_steers")
        .and_then(Value::as_object)
        .and_then(|object| keys.iter().find_map(|key| object.get(*key).cloned()))
        .unwrap_or_else(|| compact_debate_turn(controller_turn))
}

fn accepted_claims_for_side(controller_turn: &Value, side: &str) -> Value {
    let artifact = controller_turn.get("artifact").unwrap_or(controller_turn);
    let accepted = artifact
        .get("accepted_for_opponent")
        .cloned()
        .unwrap_or(Value::Null);
    // Controller may nest by side or return a flat claim list.
    if let Some(object) = accepted.as_object() {
        let keys = match side {
            "bull" => ["bull", "to_bull", "researcher.bull.interaction"],
            _ => ["bear", "to_bear", "researcher.bear.interaction"],
        };
        if let Some(value) = keys.iter().find_map(|key| object.get(*key).cloned()) {
            return value;
        }
    }
    accepted
}

fn extract_addressable_claims(packet: &Value) -> Value {
    let artifact = packet.get("artifact").unwrap_or(packet);
    if let Some(claims) = artifact.get("claims").and_then(Value::as_array) {
        let items: Vec<Value> = claims
            .iter()
            .map(|claim| {
                json!({
                    "claim_id": claim.get("claim_id").cloned().unwrap_or(Value::Null),
                    "claim": claim.get("claim").cloned().unwrap_or(Value::Null),
                    "decision_hinge": claim.get("decision_hinge").cloned().unwrap_or(Value::Null),
                    "confidence": claim.get("confidence").cloned().unwrap_or(Value::Null),
                    "evidence_refs": claim.get("evidence_refs").cloned().unwrap_or(Value::Null)
                })
            })
            .collect();
        if !items.is_empty() {
            return Value::Array(items);
        }
    }
    if artifact.get("claim").is_some() {
        return json!([{
            "claim_id": artifact.get("reply_to_claim_id").cloned()
                .or_else(|| artifact.get("claim_id").cloned())
                .unwrap_or(Value::Null),
            "claim": artifact.get("claim").cloned().unwrap_or(Value::Null),
            "decision_hinge": artifact.get("decision_hinge").cloned().unwrap_or(Value::Null),
            "confidence": artifact.get("confidence").cloned().unwrap_or(Value::Null),
            "evidence_refs": artifact.get("evidence_refs").cloned().unwrap_or(Value::Null),
            "stance": artifact.get("stance").cloned().unwrap_or(Value::Null)
        }]);
    }
    json!([])
}

fn compact_debate_turn(turn: &Value) -> Value {
    let artifact = turn.get("artifact").unwrap_or(turn);
    json!({
        "role": turn.get("role").or_else(|| artifact.get("role")).cloned().unwrap_or(Value::Null),
        "kind": turn.get("kind").or_else(|| artifact.get("kind")).cloned().unwrap_or(Value::Null),
        "round": turn.get("round").or_else(|| artifact.get("round")).cloned().unwrap_or(Value::Null),
        "topic_id": turn.get("topic_id").or_else(|| artifact.get("topic_id")).cloned().unwrap_or(Value::Null),
        "artifact": compact_debate_artifact(artifact)
    })
}

fn compact_debate_artifact(artifact: &Value) -> Value {
    const FIELDS: &[&str] = &[
        "id",
        "role",
        "artifact_type",
        "topic_id",
        "claims",
        "summary",
        "reducer_checks",
        "reply_to_claim_id",
        "steer_id",
        "stance",
        "claim",
        "evidence_refs",
        "confidence",
        "send_to_mediator",
        "blocked_ack",
        "steelman",
        "fatal_weakness",
        "invalidation_condition",
        "evidence_needed",
        "unresolved",
        "upside_asymmetry",
        "downside_asymmetry",
        "claim_ledger",
        "accepted_for_opponent",
        "rejected_to_origin",
        "blocked_claims",
        "next_steers",
        "topic_summary_delta",
        "soft_control",
        "info_gain_score",
        "agreed_facts",
        "decision_hinges",
        "missing_evidence",
        "highest_value_next_query",
    ];
    let Some(object) = artifact.as_object() else {
        return Value::Null;
    };
    Value::Object(
        FIELDS
            .iter()
            .filter_map(|field| {
                object
                    .get(*field)
                    .map(|value| ((*field).to_string(), value.clone()))
            })
            .collect(),
    )
}

async fn run_phase2_topic_generation(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<Vec<Value>> {
    let baseline = build_topic_generation_artifact(state);
    let mut artifact = baseline.clone();
    if !is_mock(state) {
        let run_id = state.get("run_id").and_then(Value::as_str).unwrap_or("run");
        let session_id = format!("{run_id}:phase2:topic-generator");
        let turn_id = session_id.clone();
        let prompt_path = config
            .prompts
            .path_for("mediator.topic")
            .context("missing mediator.topic prompt path")?
            .clone();
        let generated = run_single_steer_role_job(
            SteerRoleRun {
                state: state.clone(),
                role: "mediator.topic",
                phase: 2,
                kind: "topic_generation",
                round: None,
                topic_id: None,
                mock: false,
                model_override,
                reasoning_effort_override,
                config,
                prompt_path: Some(prompt_path.as_path()),
                session_id: session_id.clone(),
                turn_id: turn_id.clone(),
                steer: topic_generation_steer(),
            },
            config.workflow.reducer_timeout_sec,
            config,
            state,
            conn,
        )
        .await?;
        // The independent Topic Generator turn is the controller checkpoint,
        // even when its artifact falls back to Rust. Bull/Bear use the separate
        // shared warmup checkpoint.
        state["topic_generation_turn_id"] = Value::String(turn_id);
        state["topic_generation_session_id"] = Value::String(session_id);
        if generated.get("artifact_type").and_then(Value::as_str)
            == Some("phase2_topic_generation_artifact")
        {
            artifact = merge_topic_generation_output(&baseline, &generated);
        } else {
            tracing::warn!("mediator.topic degraded; using deterministic topic fallback");
        }
    }
    state["topic_generation_artifact"] = artifact.clone();
    let topics = topics_from_generation_artifact(&artifact);
    if topics.is_empty() {
        state["debate_topics"] = json!([]);
        persist_message(
            conn,
            state,
            2,
            "mediator.topic",
            "topic_final",
            None,
            artifact,
        )?;
        debug!("phase 2 debate skipped by topic-generation gate");
        return Ok(Vec::new());
    }
    state["debate_topics"] = Value::Array(topics.clone());
    persist_message(
        conn,
        state,
        2,
        "mediator.topic",
        "topic_final",
        None,
        artifact,
    )?;
    debug!(topic_count = topics.len(), "phase 2 topics generated");
    Ok(topics)
}

fn merge_topic_generation_output(baseline: &Value, generated: &Value) -> Value {
    let mut artifact = baseline.clone();
    for field in ["common_ground", "summary"] {
        if let Some(value) = generated.get(field) {
            artifact[field] = value.clone();
        }
    }
    if baseline.get("evidence_actionable").and_then(Value::as_bool) == Some(true) {
        artifact["topics"] = generated
            .get("topics")
            .cloned()
            .unwrap_or_else(|| json!([]));
    }
    let topic_count = artifact
        .get("topics")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or_default();
    let ticker_count = artifact
        .get("generated_from")
        .and_then(|value| value.get("tickers"))
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(1)
        .max(1);
    let evidence_actionable =
        baseline.get("evidence_actionable").and_then(Value::as_bool) == Some(true);
    let actionable = topic_count > 0;
    artifact["status"] = json!(if actionable { "ready" } else { "skipped" });
    artifact["actionable"] = json!(actionable);
    artifact["debate_required"] = json!(actionable);
    artifact["skip_reason"] = if actionable {
        Value::Null
    } else if !evidence_actionable {
        baseline
            .get("skip_reason")
            .cloned()
            .unwrap_or_else(|| json!("phase1_evidence_insufficient"))
    } else {
        json!("no_material_cross_analyst_conflict")
    };
    artifact["material_conflict_count"] = json!(topic_count);
    artifact["conflict_score"] = json!((topic_count as f64 / ticker_count as f64).clamp(0.0, 1.0));
    artifact
}

/// Deterministic Phase 1 index: weighted base, conflicts, evidence_quality.
/// End of phase 1 only — not a separate phase 1.5 / 15.
fn materialize_phase1_index(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    config: &RuntimeConfig,
    persist_legacy_projection: bool,
) -> Result<()> {
    let artifact = build_phase1_index(state, config);
    let brief = reducer_brief_md(&artifact);
    state["phase1_index"] = artifact.clone();
    state["phase1_brief_md"] = Value::String(brief.clone());
    if persist_legacy_projection {
        persist_artifact_with_last_md(conn, state, 1, "phase1.index", artifact, brief)?;
    } else {
        state["phase1_index_authority"] = json!("file_store_derived");
    }
    Ok(())
}

async fn run_phase2_final_reducer(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    _model_override: Option<&str>,
    _reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<()> {
    let artifact = build_debate_state_artifact(state, config);
    let brief = reducer_brief_md(&artifact);
    state["debate_state_artifact"] = artifact.clone();
    state["debate_brief_md"] = Value::String(brief.clone());
    persist_artifact_with_last_md(
        conn,
        state,
        2,
        "reducer.debate_final",
        artifact.clone(),
        brief,
    )?;
    record_local_debug_artifact(
        state,
        2,
        "reducer.debate_final",
        PathBuf::from("outputs/debug/phase2/debate_final.json"),
        "runtime",
        &artifact,
    )?;
    // Compression is scheduled by the main pipeline so it can overlap later work.
    Ok(())
}

/// Result of a background phase-00 LLM compress job, already persisted to SQLite.
struct CompressJobResult {
    source_phase: i64,
    written: usize,
    batch: orchestrator_sql::PhaseSummaryPhaseBatch,
    /// True only when the completed Index directories are the persistence
    /// authority. The batch then exists solely as an in-process compatibility
    /// projection for not-yet-migrated readers and is never flushed to SQLite.
    file_store_authoritative: bool,
    debug_enabled: bool,
    debug_output_path: PathBuf,
    debug_source_label: String,
    role_metrics: Value,
}

async fn compress_phase_job(
    mut state: Value,
    source_phase: i64,
    model_override: Option<String>,
    reasoning_effort_override: Option<String>,
    config: RuntimeConfig,
) -> Result<CompressJobResult> {
    let debug_enabled = state.get("debug").and_then(Value::as_bool) == Some(true);
    let prompt_path = config
        .prompts
        .path_for("compressor.phase_summary")
        .context("missing compressor.phase_summary prompt path")?
        .clone();
    let debug_source_label = debug_prompt_source_label(&prompt_path)?;
    let debug_output_path = PathBuf::from(format!(
        "outputs/debug/phase{source_phase}/summary/phase{source_phase}_summary.json"
    ));
    if phase_summary_uses_file_store(&config)? {
        let store_root = state
            .get("store_root")
            .and_then(Value::as_str)
            .context("store_root missing for FileStore phase_summary")?;
        let file_store = write_deterministic_phase_summary(
            Path::new(store_root),
            &state,
            source_phase,
            config.tool_managed.max_summary_units_per_phase,
        )?;
        let batch = crate::orchestration::compress::build_phase_compress(&state, source_phase)?;
        return Ok(CompressJobResult {
            source_phase,
            written: file_store.indexes.len(),
            batch,
            file_store_authoritative: true,
            debug_enabled,
            debug_output_path,
            debug_source_label,
            role_metrics: Value::Array(vec![]),
        });
    }
    let (batch, role_metrics) = if is_mock(&state) {
        (
            crate::orchestration::compress::build_phase_compress(&state, source_phase)?,
            Value::Array(vec![]),
        )
    } else if source_phase == 1 && phase1_cached_artifact_fallback_active(&state) {
        tracing::warn!(
            source_phase,
            "compressor.phase_summary falling back to deterministic in-memory summary due cached phase1 artifacts"
        );
        (
            crate::orchestration::compress::build_phase_compress(&state, source_phase)?,
            Value::Array(vec![]),
        )
    } else {
        let source_payload =
            crate::orchestration::compress::phase_summary_source_payload(&state, source_phase)?;
        let mut job = prepare_role_job(RoleRun {
            state: state.clone(),
            role: "compressor.phase_summary",
            phase: source_phase,
            kind: "phase_summary",
            round: Some(source_phase),
            topic_id: None,
            mock: false,
            model_override: model_override.as_deref(),
            reasoning_effort_override: reasoning_effort_override.as_deref(),
            config: &config,
            prompt_path: Some(prompt_path.as_path()),
        })?;
        job.debug_output_path = Some(debug_output_path.clone());
        if let Some(llm) = job.llm.as_mut() {
            llm.tools.clear();
        }
        job.prompt.push_str("\n\n");
        job.prompt.push_str(
            &include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../prompts/system/messages/source_payload.md"
            ))
            .replace(
                "{{source_payload}}",
                &serde_json::to_string(&source_payload)?,
            ),
        );
        let conn = orchestrator_sql::connect(
            state
                .get("db_path")
                .and_then(Value::as_str)
                .context("db_path missing for phase_summary compressor")?,
        )?;
        let result = run_role_jobs(vec![job], 1, config.workflow.agent_timeout_sec)
            .await
            .into_iter()
            .next()
            .context("phase_summary compressor returned no role result")?;
        persist_prompt_metric(&conn, &result);
        record_role_job_metrics(&mut state, &result);
        if let Some(error) = result.error.as_deref() {
            tracing::warn!(
                source_phase,
                error = %error,
                "phase_summary compressor role failed; falling back to deterministic in-memory summary"
            );
            state["degraded"] = json!(true);
            if !state.get("degraded_report").is_some_and(Value::is_object) {
                state["degraded_report"] = json!({"is_degraded": true, "roles": []});
            }
            if let Some(roles) = state
                .get_mut("degraded_report")
                .and_then(|report| report.get_mut("roles"))
                .and_then(Value::as_array_mut)
            {
                roles.push(json!({
                    "role": "compressor.phase_summary",
                    "phase": source_phase,
                    "kind": "phase_summary",
                    "error": error,
                    "message": format!("phase_summary compressor failed for phase {source_phase}")
                }));
            }
            let role_metrics = state
                .get("role_job_metrics")
                .and_then(Value::as_array)
                .and_then(|items| items.last())
                .cloned()
                .map(|item| json!([item]))
                .unwrap_or_else(|| json!([]));
            return Ok(CompressJobResult {
                source_phase,
                written: 0,
                batch: crate::orchestration::compress::build_phase_compress(&state, source_phase)?,
                file_store_authoritative: false,
                debug_enabled,
                debug_output_path,
                debug_source_label,
                role_metrics,
            });
        }
        let artifact = result
            .artifact
            .as_ref()
            .context("phase_summary compressor returned no artifact")?;
        let batch = crate::orchestration::compress::phase_summary_bundle_to_batch(
            &state,
            source_phase,
            artifact,
        )?;
        let current = state
            .get("role_job_metrics")
            .and_then(Value::as_array)
            .and_then(|items| items.last())
            .cloned()
            .map(|item| json!([item]))
            .unwrap_or_else(|| json!([]));
        (batch, current)
    };
    let written = batch.written();
    let conn = orchestrator_sql::connect(
        state
            .get("db_path")
            .and_then(Value::as_str)
            .context("db_path missing for phase_summary persistence")?,
    )?;
    let run_id = state
        .get("run_id")
        .and_then(Value::as_str)
        .context("run_id missing for phase_summary persistence")?;
    orchestrator_sql::persist_phase_summary_batch(&conn, run_id, &batch)?;
    Ok(CompressJobResult {
        source_phase,
        written,
        batch,
        file_store_authoritative: false,
        debug_enabled,
        debug_output_path,
        debug_source_label,
        role_metrics,
    })
}

fn apply_compress_result(state: &mut Value, result: CompressJobResult) -> Result<()> {
    let CompressJobResult {
        source_phase,
        batch,
        file_store_authoritative,
        debug_enabled,
        debug_output_path,
        debug_source_label,
        role_metrics,
        ..
    } = result;
    merge_role_job_metrics(state, &role_metrics);
    let snapshot = crate::orchestration::compress::apply_phase_summary_batch(state, batch)?;
    state["phase_compress"][source_phase.to_string()]["persisted"] = json!(true);
    state["phase_compress"][source_phase.to_string()]["authority"] =
        json!(if file_store_authoritative {
            "file_store"
        } else {
            "legacy"
        });
    state["phase_summary_tables"][source_phase.to_string()]["persisted"] = json!(true);
    if debug_enabled {
        let role = format!("compressor.after_phase_{source_phase}");
        record_local_debug_artifact(
            state,
            source_phase,
            &role,
            debug_output_path,
            &debug_source_label,
            &snapshot,
        )?;
    }
    debug!(
        source_phase,
        written = result.written,
        "phase_summary compress applied to memory state"
    );
    Ok(())
}

fn phase1_cached_artifact_fallback_active(state: &Value) -> bool {
    state
        .get("analyst_reports")
        .and_then(Value::as_object)
        .is_some_and(|reports| {
            reports.values().any(|artifact| {
                artifact.get("fallback").and_then(Value::as_str) == Some("cached_db_artifact")
            })
        })
}

/// Spawn phase-00 after a business phase. The caller awaits the result before
/// starting the next phase, while the gate remains available to role tools.
fn spawn_compress_job(
    gate: std::sync::Arc<orchestrator_sql::PhaseSummaryGate>,
    state: &Value,
    source_phase: i64,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> std::thread::JoinHandle<Result<CompressJobResult>> {
    gate.mark_inflight(source_phase);
    let state_snapshot = state.clone();
    let gate_job = gate.clone();
    let model_override = model_override.map(ToString::to_string);
    let reasoning_effort_override = reasoning_effort_override.map(ToString::to_string);
    let config = config.clone();
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async move {
                let result = compress_phase_job(
                    state_snapshot,
                    source_phase,
                    model_override,
                    reasoning_effort_override,
                    config,
                )
                .await;
                match &result {
                    Ok(ok) => gate_job.complete(source_phase, ok.batch.clone()),
                    Err(err) => gate_job.fail(source_phase, err.to_string()),
                }
                result
            })
    })
}

async fn await_compress_job(
    handle: std::thread::JoinHandle<Result<CompressJobResult>>,
    state: &mut Value,
) -> Result<()> {
    let result = handle
        .join()
        .map_err(|_| anyhow::anyhow!("compress task panicked"))?
        .context("compress task failed")?;
    apply_compress_result(state, result)
}

async fn await_all_compress_jobs(
    jobs: &mut Vec<(i64, std::thread::JoinHandle<Result<CompressJobResult>>)>,
    state: &mut Value,
) -> Result<()> {
    while let Some((_phase, handle)) = jobs.pop() {
        await_compress_job(handle, state).await?;
    }
    Ok(())
}

fn record_local_debug_artifact(
    state: &mut Value,
    phase: i64,
    role: &str,
    relative_path: PathBuf,
    source_label: &str,
    artifact: &Value,
) -> Result<()> {
    if state.get("debug").and_then(Value::as_bool) != Some(true) {
        return Ok(());
    }

    let started = Instant::now();
    let status = artifact
        .get("status")
        .cloned()
        .or_else(|| state.pointer(&format!("/phase_status/{phase}")).cloned())
        .unwrap_or_else(|| json!("derived"));
    let record = json!({
        "kind": "runtime",
        "phase": phase,
        "role": role,
        "req": {
            "phase_status": state.pointer(&format!("/phase_status/{phase}")).cloned().unwrap_or(Value::Null)
        },
        "resp": {
            "status": status,
            "artifact": artifact
        }
    });
    orchestrator_llm::append_debug_output_record(
        &default_project_root(),
        &relative_path,
        source_label,
        record,
    )?;

    if !state
        .get("debug_phase_records")
        .is_some_and(Value::is_array)
    {
        state["debug_phase_records"] = json!([]);
    }
    if let Some(records) = state["debug_phase_records"].as_array_mut() {
        records.push(json!({
            "kind": "local_reducer",
            "phase": phase,
            "role": role,
            "path": relative_path,
        }));
    }
    orchestrator_llm::debug_log_time(
        &default_project_root(),
        json!({
            "kind": "function",
            "name": format!("record_local_debug_artifact:{role}"),
            "phase": phase,
            "role": role,
            "elapsed_ms": started.elapsed().as_millis(),
        }),
    );
    Ok(())
}

fn record_runtime_debug_artifact(state: &mut Value, phase: i64, artifact: &Value) -> Result<()> {
    record_local_debug_artifact(
        state,
        phase,
        "runtime",
        PathBuf::from(format!("outputs/debug/phase{phase}/runtime.json")),
        "runtime",
        artifact,
    )
}

fn debug_prompt_source_label(prompt_path: &Path) -> Result<String> {
    let project_root = default_project_root();
    if let Ok(relative) = prompt_path.strip_prefix(&project_root) {
        return Ok(relative.display().to_string());
    }
    prompt_path
        .to_str()
        .and_then(|value| {
            value
                .find("prompts/")
                .map(|index| value[index..].to_string())
        })
        .with_context(|| {
            format!(
                "debug prompt path must contain prompts/: {}",
                prompt_path.display()
            )
        })
}

/// FileStore-only Phase 3 execution.  The generic role helper intentionally
/// remains legacy-compatible for unmigrated phases, so it cannot be used for
/// a migrated manager failure: it would synthesize the old JSON artifact.
async fn run_file_store_research_job(
    input: RoleRun<'_>,
    timeout_sec: u64,
    config: &RuntimeConfig,
    state: &mut Value,
    conn: &rusqlite::Connection,
) -> Result<Value> {
    let result = run_single_role_job_result(input, timeout_sec, state, conn).await?;
    if let Some(artifact) = result.artifact {
        return Ok(artifact);
    }

    let failure = result
        .error
        .as_deref()
        .unwrap_or("manager.research failed before terminal finalize")
        .to_owned();
    let registration = config
        .authority_registry
        .registration("manager.research", ToolManagedProfile::ResearchDecision)?;
    let store_root = state
        .get("store_root")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .context("store_root missing for migrated Phase 3 ResearchDecision")?;
    let tickers = tickers_from_state(state);
    record_degraded_role(state, &result, &failure);
    finalize_degraded_research_decision(
        &store_root,
        state,
        FileStoreDomainRuntimePlan {
            role: "manager.research".to_owned(),
            phase: 3,
            profile: ToolManagedProfile::ResearchDecision,
            profile_version: registration.profile_version,
            builder_version: registration.builder_version,
            tickers,
            visible_evidence_refs: BTreeSet::new(),
            topic_id: None,
            side: None,
            round: None,
            visible_claims: BTreeSet::new(),
            fork: None,
            trade_candidate_action: None,
            portfolio_rating: None,
            portfolio_current_weight: None,
        },
        &failure,
    )
}

fn record_prompt_runtime_debug_artifact(
    state: &mut Value,
    phase: i64,
    role: &str,
    prompt_path: &Path,
    artifact: &Value,
) -> Result<()> {
    let source_label = debug_prompt_source_label(prompt_path)?;
    let relative_path =
        orchestrator_llm::debug_record_relative_path_from_prompt(Path::new(&source_label))?;
    record_local_debug_artifact(state, phase, role, relative_path, &source_label, artifact)
}

async fn run_phase3(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<()> {
    let mock = is_mock(state);
    debug!("manager research role starting");
    let run = RoleRun {
        state: state.clone(),
        role: "manager.research",
        phase: 3,
        kind: "artifact",
        round: None,
        topic_id: None,
        mock,
        model_override,
        reasoning_effort_override,
        config,
        prompt_path: Some(
            config
                .prompts
                .path_for("manager.research")
                .context("missing prompt path for manager.research")?,
        ),
    };
    let file_store_authoritative = research_uses_file_store(config)?;
    let mut artifact = if file_store_authoritative {
        run_file_store_research_job(run, config.workflow.agent_timeout_sec, config, state, conn)
            .await?
    } else {
        run_single_role_job(run, config.workflow.agent_timeout_sec, config, state, conn).await?
    };
    enforce_phase3_deterministic_fields(state, &mut artifact);
    let artifact_is_degraded = artifact
        .get("degraded")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let initial_violations = phase3_probability_drift_violations(state, &artifact);
    let artifact = if artifact_is_degraded {
        state["degraded"] = Value::Bool(true);
        state["phase3_probability_guard"] = json!({
            "status": "clamped_to_phase1_base",
            "retry_attempted": false,
            "retry_error": artifact.get("error").cloned().unwrap_or_else(|| json!("manager.research degraded")),
            "violations": initial_violations
        });
        apply_phase3_probability_fallback(artifact, &initial_violations)
    } else if initial_violations.is_empty() {
        artifact
    } else if mock {
        state["degraded"] = Value::Bool(true);
        state["phase3_probability_guard"] = json!({
            "status": "clamped_to_phase1_base",
            "retry_attempted": false,
            "violations": initial_violations
        });
        apply_phase3_probability_fallback(artifact, &initial_violations)
    } else {
        let retry_state = phase3_probability_retry_state(state, &initial_violations);
        let retry_run = RoleRun {
            state: retry_state,
            role: "manager.research",
            phase: 3,
            kind: "artifact",
            round: None,
            topic_id: None,
            mock: false,
            model_override,
            reasoning_effort_override,
            config,
            prompt_path: Some(
                config
                    .prompts
                    .path_for("manager.research")
                    .context("missing prompt path for manager.research")?,
            ),
        };
        let retry_result = if file_store_authoritative {
            run_file_store_research_job(
                retry_run,
                config.workflow.agent_timeout_sec,
                config,
                state,
                conn,
            )
            .await
        } else {
            run_single_role_job(
                retry_run,
                config.workflow.agent_timeout_sec,
                config,
                state,
                conn,
            )
            .await
        };
        match retry_result {
            Ok(mut retry_artifact)
                if !retry_artifact
                    .get("degraded")
                    .and_then(Value::as_bool)
                    .unwrap_or(false) =>
            {
                enforce_phase3_deterministic_fields(state, &mut retry_artifact);
                let retry_violations = phase3_probability_drift_violations(state, &retry_artifact);
                if retry_violations.is_empty() {
                    state["phase3_probability_guard"] = json!({
                        "status": "retry_accepted",
                        "retry_attempted": true,
                        "initial_violations": initial_violations
                    });
                    retry_artifact
                } else {
                    state["degraded"] = Value::Bool(true);
                    state["phase3_probability_guard"] = json!({
                        "status": "clamped_to_phase1_base",
                        "retry_attempted": true,
                        "initial_violations": initial_violations,
                        "violations": retry_violations
                    });
                    apply_phase3_probability_fallback(retry_artifact, &retry_violations)
                }
            }
            Ok(retry_artifact) => {
                state["degraded"] = Value::Bool(true);
                state["phase3_probability_guard"] = json!({
                    "status": "clamped_to_phase1_base",
                    "retry_attempted": true,
                    "retry_error": retry_artifact.get("error").cloned().unwrap_or_else(|| json!("manager.research retry degraded")),
                    "violations": initial_violations
                });
                apply_phase3_probability_fallback(artifact, &initial_violations)
            }
            Err(error) => {
                state["degraded"] = Value::Bool(true);
                state["phase3_probability_guard"] = json!({
                    "status": "clamped_to_phase1_base",
                    "retry_attempted": true,
                    "retry_error": error.to_string(),
                    "violations": initial_violations
                });
                apply_phase3_probability_fallback(artifact, &initial_violations)
            }
        }
    };
    let mut artifact = artifact;
    apply_missing_data_convergence(state, &mut artifact);
    if !file_store_authoritative {
        persist_artifact(conn, state, 3, "manager.research", artifact.clone())?;
    } else {
        state["research_plan_authority"] = json!("file_store");
    }
    state["research_plan"] = artifact;
    debug!("manager research role completed");
    Ok(())
}

fn apply_missing_data_convergence(state: &Value, artifact: &mut Value) {
    let tickers = tickers_from_state(state);
    for (index, ticker) in tickers.iter().enumerate() {
        let missing_items = missing_high_impact_items(state, ticker);
        if missing_items.is_empty() {
            continue;
        }
        let (current, adjusted, requested, applied, premium) = {
            let Some(payload) = artifact
                .get_mut("per_ticker")
                .and_then(Value::as_object_mut)
                .and_then(|items| items.get_mut(ticker))
            else {
                continue;
            };
            let Some(current) = payload
                .get("final_probability")
                .or_else(|| payload.get("long_probability"))
                .and_then(Value::as_f64)
            else {
                continue;
            };
            let requested = (missing_items.len() as f64 * 0.025).min(0.08);
            let adjusted = converge_toward_neutral(current, requested);
            let applied = (adjusted - current).abs();
            set_research_probability(payload, adjusted);
            adjust_scenario_probabilities(payload, adjusted - current);
            let convergence = json!({
                "reason_code": "missing_data_convergence",
                "item_count": missing_items.len(),
                "items": missing_items,
                "requested_convergence": requested,
                "applied_convergence": applied,
                "from_probability": current,
                "to_probability": adjusted
            });
            payload["missing_data_convergence"] = convergence.clone();
            if payload.get("rating").and_then(Value::as_str) == Some("Hold") {
                payload["confidence_basis"] = json!("data_insufficient");
                payload["hold_reason"] = json!("evidence_insufficient");
            }
            append_adjustment_rationale(
                payload,
                &format!(
                    "missing_data_convergence: {} high-impact missing items; requested convergence {:.3}, applied {:.3}, final {:.3}.",
                    missing_items.len(), requested, applied, adjusted
                ),
            );
            (current, adjusted, requested, applied, convergence)
        };
        if index == 0 {
            set_research_probability(artifact, adjusted);
            adjust_scenario_probabilities(artifact, adjusted - current);
            artifact["missing_data_convergence"] = premium;
            if artifact.get("rating").and_then(Value::as_str) == Some("Hold") {
                artifact["confidence_basis"] = json!("data_insufficient");
                artifact["hold_reason"] = json!("evidence_insufficient");
            }
            append_adjustment_rationale(
                artifact,
                &format!(
                    "missing_data_convergence: {} high-impact missing items for {ticker}; requested convergence {:.3}, applied {:.3}, final {:.3}.",
                    missing_items.len(),
                    requested,
                    applied,
                    adjusted
                ),
            );
        }
    }
}

fn missing_high_impact_items(state: &Value, ticker: &str) -> Vec<String> {
    let mut items = std::collections::BTreeSet::new();
    let ticker_debate = state
        .get("debate_state_artifact")
        .and_then(|value| value.get("per_ticker"))
        .and_then(|value| value.get(ticker));

    if let Some(factors) = ticker_debate
        .and_then(|value| value.get("missing_high_impact_factors"))
        .and_then(Value::as_array)
    {
        for item in factors {
            if let Some(text) = item.as_str().map(str::trim).filter(|text| !text.is_empty()) {
                items.insert(text.to_string());
            } else if let Some(text) = item
                .get("factor")
                .or_else(|| item.get("claim"))
                .or_else(|| item.get("description"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|text| !text.is_empty())
            {
                items.insert(text.to_string());
            }
        }
    }

    if let Some(evidence) = ticker_debate
        .and_then(|value| value.get("missing_evidence"))
        .and_then(Value::as_array)
    {
        for item in evidence {
            let is_high_impact = item
                .get("impact")
                .or_else(|| item.get("severity"))
                .and_then(Value::as_str)
                .is_some_and(|value| value.eq_ignore_ascii_case("high"));
            if is_high_impact {
                if let Some(text) = item
                    .get("factor")
                    .or_else(|| item.get("claim"))
                    .or_else(|| item.get("description"))
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                {
                    items.insert(text.to_string());
                }
            }
        }
    }

    if let Some(roles) = state
        .get("phase1_index")
        .and_then(|value| value.get("evidence_quality"))
        .and_then(|value| value.get("missing_critical_roles"))
        .and_then(Value::as_array)
    {
        for role in roles.iter().filter_map(Value::as_str) {
            items.insert(format!("missing critical role: {role}"));
        }
    }

    // Phase 1 index "insufficient" is itself a high-impact evidence gap: no critical
    // role produced usable direction for this ticker, even when roles are ready
    // with direction=unobserved (not listed under missing_critical_roles).
    let evidence_quality = state
        .get("phase1_index")
        .and_then(|value| value.get("evidence_quality"));
    let phase1_insufficient = evidence_quality
        .and_then(|value| value.get("status"))
        .and_then(Value::as_str)
        == Some("insufficient");
    let ticker_marked_insufficient = evidence_quality
        .and_then(|value| value.get("insufficient_tickers"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .any(|value| value == ticker);
    if phase1_insufficient
        && (ticker_marked_insufficient
            || evidence_quality
                .and_then(|value| value.get("insufficient_tickers"))
                .and_then(Value::as_array)
                .map(|items| items.is_empty())
                .unwrap_or(true))
    {
        items.insert(format!("phase1 evidence insufficient for {ticker}"));
    }
    items.into_iter().collect()
}

fn converge_toward_neutral(probability: f64, amount: f64) -> f64 {
    if probability > 0.5 {
        (probability - amount).max(0.5)
    } else if probability < 0.5 {
        (probability + amount).min(0.5)
    } else {
        0.5
    }
}

fn enforce_phase3_deterministic_fields(state: &Value, artifact: &mut Value) {
    let tickers = tickers_from_state(state);
    let mut primary_payload = None;
    for ticker in &tickers {
        let Some(base_probability) = weighted_base_probability_for_ticker(state, ticker) else {
            continue;
        };
        let Some(payload) = artifact
            .get_mut("per_ticker")
            .and_then(Value::as_object_mut)
            .and_then(|items| items.get_mut(ticker))
        else {
            continue;
        };
        payload["base_probability"] = json!(base_probability);
        if let Some(final_probability) = payload
            .get("long_probability")
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
        {
            set_research_probability(payload, final_probability);
            payload["debate_adjustment"] = json!(final_probability - base_probability);
        }
        if primary_payload.is_none() {
            primary_payload = Some(payload.clone());
        }
    }

    if let Some(primary) = primary_payload {
        for field in [
            "rating",
            "long_probability",
            "short_probability",
            "base_probability",
            "debate_adjustment",
            "final_probability",
            "confidence_basis",
            "hold_reason",
        ] {
            if let Some(value) = primary.get(field).cloned() {
                artifact[field] = value;
            } else if field == "hold_reason" {
                if let Some(object) = artifact.as_object_mut() {
                    object.remove(field);
                }
            }
        }
    }
}

fn set_research_probability(value: &mut Value, probability: f64) {
    value["long_probability"] = json!(probability);
    value["short_probability"] = json!(((1.0 - probability) * 10_000.0).round() / 10_000.0);
    value["final_probability"] = json!(probability);
    let rating = research_rating_for_probability(probability);
    value["rating"] = json!(rating);
    if rating == "Hold" {
        let confidence_basis = value
            .get("confidence_basis")
            .and_then(Value::as_str)
            .filter(|basis| {
                matches!(
                    *basis,
                    "evidence_balanced" | "data_insufficient" | "conflicting_evidence"
                )
            })
            .unwrap_or("evidence_balanced");
        let hold_reason = match confidence_basis {
            "data_insufficient" => "evidence_insufficient",
            "conflicting_evidence" => "conflicting_evidence",
            _ => "evidence_balanced",
        };
        value["confidence_basis"] = json!(confidence_basis);
        value["hold_reason"] = json!(hold_reason);
    } else if let Some(object) = value.as_object_mut() {
        object.remove("hold_reason");
    }
}

fn adjust_scenario_probabilities(value: &mut Value, long_delta: f64) {
    let Some(scenarios) = value.get_mut("scenarios") else {
        return;
    };
    let Some(bull) = scenarios
        .get("bull")
        .and_then(|value| value.get("probability"))
        .and_then(Value::as_f64)
    else {
        return;
    };
    let Some(bear) = scenarios
        .get("bear")
        .and_then(|value| value.get("probability"))
        .and_then(Value::as_f64)
    else {
        return;
    };
    let bounded_delta = long_delta.max(-bull).min(bear);
    scenarios["bull"]["probability"] = json!(bull + bounded_delta);
    scenarios["bear"]["probability"] = json!(bear - bounded_delta);
}

fn append_adjustment_rationale(value: &mut Value, addition: &str) {
    let existing = value
        .get("adjustment_rationale")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim();
    value["adjustment_rationale"] = json!(if existing.is_empty() {
        addition.to_string()
    } else {
        format!("{existing} {addition}")
    });
}

async fn run_phase4(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<()> {
    if profile_uses_file_store(config, "trader", ToolManagedProfile::TradeIntent)? {
        return run_file_store_phase4(
            state,
            model_override,
            reasoning_effort_override,
            config,
            conn,
        )
        .await;
    }
    let prompt_path = config
        .prompts
        .path_for("trader")
        .context("missing prompt path for trader")?;
    let mut artifact = run_single_role_job(
        RoleRun {
            state: state.clone(),
            role: "trader",
            phase: 4,
            kind: "artifact",
            round: None,
            topic_id: None,
            mock: is_mock(state),
            model_override,
            reasoning_effort_override,
            config,
            prompt_path: Some(prompt_path),
        },
        config.workflow.agent_timeout_sec,
        config,
        state,
        conn,
    )
    .await?;
    sanitize_downstream_constraints(state, "trader_investment_plan", &mut artifact);
    enforce_trade_candidate(state, &mut artifact);
    persist_artifact(conn, state, 4, "trader", artifact.clone())?;
    record_prompt_runtime_debug_artifact(state, 4, "trader", prompt_path, &artifact)?;
    state["trader_investment_plan"] = artifact;
    Ok(())
}

/// FileStore Trader units are one per ticker. The legacy state projection is
/// strictly derived from finalized canonical artifacts for downstream Rust
/// allocation only; it is never written to SQLite.
async fn run_file_store_phase4(
    state: &mut Value,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
    conn: &rusqlite::Connection,
) -> Result<()> {
    let registration = config
        .authority_registry
        .registration("trader", ToolManagedProfile::TradeIntent)?;
    let store_root = state
        .get("store_root")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .context("store_root missing for migrated Phase 4 Trader")?;
    let mut projected = serde_json::Map::new();
    for ticker in tickers_from_state(state) {
        let mut ticker_state = state.clone();
        ticker_state["ticker"] = json!(ticker);
        ticker_state["tickers"] = json!([ticker]);
        let result = run_single_role_job_result(
            RoleRun {
                state: ticker_state.clone(),
                role: "trader",
                phase: 4,
                kind: "artifact",
                round: None,
                topic_id: None,
                mock: is_mock(state),
                model_override,
                reasoning_effort_override,
                config,
                prompt_path: Some(
                    config
                        .prompts
                        .path_for("trader")
                        .context("missing prompt path for trader")?,
                ),
            },
            config.workflow.agent_timeout_sec,
            state,
            conn,
        )
        .await?;
        let candidate_action = research_candidate_for_ticker(state, &ticker);
        let plan = FileStoreDomainRuntimePlan {
            role: "trader".to_owned(),
            phase: 4,
            profile: ToolManagedProfile::TradeIntent,
            profile_version: registration.profile_version,
            builder_version: registration.builder_version,
            tickers: vec![ticker.clone()],
            visible_evidence_refs: BTreeSet::new(),
            topic_id: None,
            side: None,
            round: None,
            visible_claims: BTreeSet::new(),
            fork: None,
            trade_candidate_action: Some(candidate_action),
            portfolio_rating: None,
            portfolio_current_weight: None,
        };
        let artifact = match result.artifact {
            Some(artifact) => artifact,
            None => {
                let failure = result
                    .error
                    .as_deref()
                    .unwrap_or("trader failed before terminal finalize");
                record_degraded_role(state, &result, failure);
                finalize_degraded_trade_intent(&store_root, &ticker_state, plan, failure)?
            }
        };
        let intent = artifact
            .get("intent")
            .cloned()
            .context("FileStore trade artifact missing intent")?;
        // Debug retention is a local append-only diagnostic artifact, not a
        // second business authority.  The canonical unit above is already
        // finalized atomically before this optional record is written.
        record_prompt_runtime_debug_artifact(
            state,
            4,
            "trader",
            config
                .prompts
                .path_for("trader")
                .context("missing prompt path for trader")?,
            &artifact,
        )?;
        projected.insert(ticker, intent);
    }
    let first = projected.values().next().cloned().unwrap_or_else(|| {
        json!({
            "action":"Hold", "candidate_action":"Hold", "execution_decision":"hold",
            "position_size_pct_max":0.0, "blockers":["no_ticker"]
        })
    });
    let mut state_projection = first;
    state_projection["per_ticker"] = Value::Object(projected);
    state["phase4_authority"] = json!("file_store");
    state["trader_investment_plan"] = state_projection;
    Ok(())
}

fn research_candidate_for_ticker(state: &Value, ticker: &str) -> String {
    let rating = state
        .get("research_plan")
        .and_then(|value| value.get("per_ticker"))
        .and_then(|items| items.get(ticker))
        .and_then(|item| item.get("rating"))
        .or_else(|| {
            state
                .get("research_plan")
                .and_then(|value| value.get("rating"))
        })
        .and_then(Value::as_str);
    match rating {
        Some("Buy" | "Overweight") => "Buy".to_owned(),
        Some("Sell" | "Underweight") => "Sell".to_owned(),
        _ => "Hold".to_owned(),
    }
}

fn enforce_trade_candidate(state: &Value, artifact: &mut Value) {
    let candidate = match state
        .get("research_plan")
        .and_then(|plan| plan.get("rating"))
        .and_then(Value::as_str)
    {
        Some("Buy" | "Overweight") => "Buy",
        Some("Sell" | "Underweight") => "Sell",
        _ => "Hold",
    };
    artifact["candidate_action"] = json!(candidate);
    let executes_candidate = artifact.get("execution_decision").and_then(Value::as_str)
        == Some("execute_candidate")
        && artifact.get("action").and_then(Value::as_str) == Some(candidate)
        && candidate != "Hold";
    if executes_candidate {
        return;
    }
    artifact["action"] = json!("Hold");
    artifact["execution_decision"] = json!("hold");
    artifact["position_size_pct_max"] = json!(0.0);
    if let Some(blockers) = artifact.get_mut("blockers").and_then(Value::as_array_mut) {
        if !blockers
            .iter()
            .any(|item| item == "runtime_candidate_mismatch")
        {
            blockers.push(json!("runtime_candidate_mismatch"));
        }
    }
}

fn run_phase4_rust_rule(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    config: &RuntimeConfig,
) -> Result<()> {
    if profile_uses_file_store(config, "trader", ToolManagedProfile::TradeIntent)? {
        return run_file_store_phase4_derived(state, config);
    }
    let mut artifact =
        research_plan_to_trade_intent(state.get("research_plan").unwrap_or(&Value::Null));
    artifact["id"] = json!("trader");
    artifact["role"] = json!("trader");
    artifact["phase"] = json!(4);
    artifact["kind"] = json!("artifact");
    artifact["status"] = json!("derived");
    artifact["derived_from"] = json!("research_plan");
    sanitize_downstream_constraints(state, "trader_investment_plan", &mut artifact);
    persist_artifact(conn, state, 4, "trader", artifact.clone())?;
    let prompt_path = config
        .prompts
        .path_for("trader")
        .context("missing prompt path for trader")?;
    record_prompt_runtime_debug_artifact(state, 4, "trader", prompt_path, &artifact)?;
    state["trader_investment_plan"] = artifact;
    Ok(())
}

fn run_file_store_phase4_derived(state: &mut Value, config: &RuntimeConfig) -> Result<()> {
    let registration = config
        .authority_registry
        .registration("trader", ToolManagedProfile::TradeIntent)?;
    let store_root = state
        .get("store_root")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .context("store_root missing for migrated Phase 4 Trader")?;
    let mut projected = serde_json::Map::new();
    for ticker in tickers_from_state(state) {
        let mut ticker_state = state.clone();
        ticker_state["ticker"] = json!(ticker);
        ticker_state["tickers"] = json!([ticker]);
        let artifact = finalize_degraded_trade_intent(
            &store_root,
            &ticker_state,
            FileStoreDomainRuntimePlan {
                role: "trader".to_owned(),
                phase: 4,
                profile: ToolManagedProfile::TradeIntent,
                profile_version: registration.profile_version,
                builder_version: registration.builder_version,
                tickers: vec![ticker.clone()],
                visible_evidence_refs: BTreeSet::new(),
                topic_id: None,
                side: None,
                round: None,
                visible_claims: BTreeSet::new(),
                fork: None,
                trade_candidate_action: Some("Hold".to_owned()),
                portfolio_rating: None,
                portfolio_current_weight: None,
            },
            "workflow_policy_not_triggered",
        )?;
        record_prompt_runtime_debug_artifact(
            state,
            4,
            "trader",
            config
                .prompts
                .path_for("trader")
                .context("missing prompt path for trader")?,
            &artifact,
        )?;
        projected.insert(ticker, artifact["intent"].clone());
    }
    let mut projection = projected
        .values()
        .next()
        .cloned()
        .unwrap_or_else(|| json!({"action":"Hold","position_size_pct_max":0.0}));
    projection["per_ticker"] = Value::Object(projected);
    state["phase4_authority"] = json!("file_store");
    state["trader_investment_plan"] = projection;
    Ok(())
}

async fn run_phase5(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<()> {
    if phase5_uses_file_store(config)? {
        return run_file_store_phase5(
            state,
            model_override,
            reasoning_effort_override,
            config,
            conn,
        )
        .await;
    }
    state["risk_debate_state"] = json!({"history": []});
    let roles = ["risk.aggressive", "risk.neutral", "risk.conservative"];
    let jobs = roles
        .into_iter()
        .enumerate()
        .map(|(index, risk_role)| {
            prepare_role_job(RoleRun {
                state: state.clone(),
                role: risk_role,
                phase: 5,
                kind: "risk_argument",
                round: Some((index + 1) as i64),
                topic_id: None,
                mock: is_mock(state),
                model_override,
                reasoning_effort_override,
                config,
                prompt_path: Some(
                    config
                        .prompts
                        .path_for(risk_role)
                        .with_context(|| format!("missing prompt path for {risk_role}"))?,
                ),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut results = run_role_jobs(jobs, roles.len(), config.workflow.agent_timeout_sec).await;
    results.sort_by_key(|result| result.round.unwrap_or_default());
    for result in results {
        let risk_role = result.role.clone();
        let round = result.round.unwrap_or_default();
        persist_prompt_metric(conn, &result);
        record_role_job_metrics(state, &result);
        let mut artifact = role_artifact_or_degraded(state, config, result)?;
        sanitize_downstream_constraints(state, &risk_role, &mut artifact);
        let turn = json!({
            "role": risk_role,
            "phase": 5,
            "kind": "risk_argument",
            "round": round,
            "artifact": artifact
        });
        if let Some(history) = state["risk_debate_state"]["history"].as_array_mut() {
            history.push(turn.clone());
        }
        persist_message(
            conn,
            state,
            5,
            &risk_role,
            "risk_argument",
            Some(round),
            turn,
        )?;
    }
    Ok(())
}

/// Phase 5 is one logical risk review.  A partial authority migration would
/// let a FileStore reviewer and a SQLite reviewer influence the same reducer,
/// which is a dual-authority fallback in disguise.  Reject it instead.
fn phase5_uses_file_store(config: &RuntimeConfig) -> Result<bool> {
    let roles = ["risk.aggressive", "risk.neutral", "risk.conservative"];
    let migrated = roles
        .into_iter()
        .map(|role| profile_uses_file_store(config, role, ToolManagedProfile::RiskReview))
        .collect::<Result<Vec<_>>>()?;
    if migrated.iter().all(|value| *value) {
        Ok(true)
    } else if migrated.iter().all(|value| !*value) {
        Ok(false)
    } else {
        bail!("Phase 5 RiskReview authority must be all FileStore or all Legacy")
    }
}

async fn run_file_store_phase5(
    state: &mut Value,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
    conn: &rusqlite::Connection,
) -> Result<()> {
    let store_root = state
        .get("store_root")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .context("store_root missing for migrated Phase 5 RiskReview")?;
    let mut history = Vec::new();
    for (round, role) in ["risk.aggressive", "risk.neutral", "risk.conservative"]
        .into_iter()
        .enumerate()
    {
        let registration = config
            .authority_registry
            .registration(role, ToolManagedProfile::RiskReview)?;
        for ticker in tickers_from_state(state) {
            let mut ticker_state = state.clone();
            ticker_state["ticker"] = json!(ticker);
            ticker_state["tickers"] = json!([ticker]);
            let result = run_single_role_job_result(
                RoleRun {
                    state: ticker_state.clone(),
                    role,
                    phase: 5,
                    kind: "risk_argument",
                    round: Some((round + 1) as i64),
                    topic_id: None,
                    mock: is_mock(state),
                    model_override,
                    reasoning_effort_override,
                    config,
                    prompt_path: Some(
                        config
                            .prompts
                            .path_for(role)
                            .with_context(|| format!("missing prompt path for {role}"))?,
                    ),
                },
                config.workflow.agent_timeout_sec,
                state,
                conn,
            )
            .await?;
            let plan = FileStoreDomainRuntimePlan {
                role: role.to_owned(),
                phase: 5,
                profile: ToolManagedProfile::RiskReview,
                profile_version: registration.profile_version,
                builder_version: registration.builder_version,
                tickers: vec![ticker.clone()],
                visible_evidence_refs: BTreeSet::new(),
                topic_id: None,
                side: None,
                round: Some((round + 1) as u32),
                visible_claims: BTreeSet::new(),
                fork: None,
                trade_candidate_action: None,
                portfolio_rating: None,
                portfolio_current_weight: None,
            };
            let artifact = match result.artifact {
                Some(artifact) => artifact,
                None => {
                    let failure = result
                        .error
                        .as_deref()
                        .unwrap_or("risk review failed before terminal finalize");
                    record_degraded_role(state, &result, failure);
                    finalize_degraded_risk_review(&store_root, &ticker_state, plan, failure)?
                }
            };
            let constraints = artifact
                .get("constraints")
                .cloned()
                .context("FileStore risk artifact missing constraints")?;
            history.push(json!({
                "role":role, "phase":5, "kind":"risk_argument", "round":round + 1,
                "ticker":ticker, "artifact":constraints, "artifact_ref":artifact.get("artifact_id")
            }));
        }
    }
    state["risk_debate_state"] = json!({"history":history, "authority":"file_store"});
    Ok(())
}

fn run_phase5_skipped(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    config: &RuntimeConfig,
) -> Result<()> {
    if phase5_uses_file_store(config)? {
        return run_file_store_phase5_skipped(state, config);
    }
    let mut artifact = json!({
        "id": "risk.review",
        "role": "risk.review",
        "phase": 5,
        "kind": "risk_review",
        "status": "skipped",
        "history": [],
        "reason": "workflow_policy_not_triggered",
        "constraints": [],
    });
    sanitize_downstream_constraints(state, "risk.review", &mut artifact);
    persist_message(
        conn,
        state,
        5,
        "risk.review",
        "skipped",
        None,
        artifact.clone(),
    )?;
    state["risk_debate_state"] = artifact;
    Ok(())
}

/// A policy-skipped FileStore phase is still represented by terminal typed
/// RiskReview artifacts.  This gives downstream Phase 6 an honest hard-zero
/// constraint and avoids reviving the former SQLite skipped-message path.
fn run_file_store_phase5_skipped(state: &mut Value, config: &RuntimeConfig) -> Result<()> {
    let store_root = state
        .get("store_root")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .context("store_root missing for migrated Phase 5 RiskReview")?;
    let mut history = Vec::new();
    for (round, role) in ["risk.aggressive", "risk.neutral", "risk.conservative"]
        .into_iter()
        .enumerate()
    {
        let registration = config
            .authority_registry
            .registration(role, ToolManagedProfile::RiskReview)?;
        for ticker in tickers_from_state(state) {
            let mut ticker_state = state.clone();
            ticker_state["ticker"] = json!(ticker);
            ticker_state["tickers"] = json!([ticker]);
            let artifact = finalize_degraded_risk_review(
                &store_root,
                &ticker_state,
                FileStoreDomainRuntimePlan {
                    role: role.to_owned(),
                    phase: 5,
                    profile: ToolManagedProfile::RiskReview,
                    profile_version: registration.profile_version,
                    builder_version: registration.builder_version,
                    tickers: vec![ticker.clone()],
                    visible_evidence_refs: BTreeSet::new(),
                    topic_id: None,
                    side: None,
                    round: Some((round + 1) as u32),
                    visible_claims: BTreeSet::new(),
                    fork: None,
                    trade_candidate_action: None,
                    portfolio_rating: None,
                    portfolio_current_weight: None,
                },
                "workflow_policy_not_triggered",
            )?;
            history.push(json!({
                "role":role, "phase":5, "kind":"risk_argument", "round":round + 1,
                "ticker":ticker, "artifact":artifact["constraints"],
                "artifact_ref":artifact.get("artifact_id"), "status":"skipped"
            }));
        }
    }
    state["risk_debate_state"] = json!({
        "history":history, "authority":"file_store", "status":"skipped",
        "reason":"workflow_policy_not_triggered"
    });
    Ok(())
}

async fn run_phase6(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<()> {
    if profile_uses_file_store(
        config,
        "portfolio.manager",
        ToolManagedProfile::PortfolioDecision,
    )? {
        return run_file_store_phase6(
            state,
            model_override,
            reasoning_effort_override,
            config,
            conn,
        )
        .await;
    }
    let mut artifact = run_single_role_job(
        RoleRun {
            state: state.clone(),
            role: "portfolio.manager",
            phase: 6,
            kind: "artifact",
            round: None,
            topic_id: None,
            mock: is_mock(state),
            model_override,
            reasoning_effort_override,
            config,
            prompt_path: Some(
                config
                    .prompts
                    .path_for("portfolio.manager")
                    .context("missing prompt path for portfolio.manager")?,
            ),
        },
        config.workflow.agent_timeout_sec,
        config,
        state,
        conn,
    )
    .await?;
    record_market_truth_check(state, "final_trade_decision", &artifact);
    enforce_phase3_market_truth(state, &mut artifact);
    stamp_phase6_execution_constraints(state, &mut artifact);
    persist_artifact(conn, state, 6, "portfolio.manager", artifact.clone())?;
    state["final_trade_decision"] = artifact;
    Ok(())
}

fn run_phase6_derived(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    config: &RuntimeConfig,
) -> Result<()> {
    if profile_uses_file_store(
        config,
        "portfolio.manager",
        ToolManagedProfile::PortfolioDecision,
    )? {
        return run_file_store_phase6_derived(state, config);
    }
    let research = state.get("research_plan").unwrap_or(&Value::Null);
    let trader = state.get("trader_investment_plan").unwrap_or(&Value::Null);
    let artifact = json!({
        "id": "portfolio.manager",
        "role": "portfolio.manager",
        "phase": 6,
        "kind": "artifact",
        "status": "derived",
        "derived_from": ["research_plan", "trader_investment_plan", "workflow_policy"],
        "rating": research.get("rating").cloned().unwrap_or_else(|| json!("Hold")),
        "execution_status": if trader.get("action").and_then(Value::as_str) == Some("Hold") { "wait" } else { "execute" },
        "execution_summary": "Portfolio review skipped by workflow policy; Phase 3 market view remains authoritative.",
        "investment_thesis": research.get("plan").cloned().unwrap_or_else(|| json!("")),
        "target_price": Value::Null,
        "horizon": "Use the Phase 3 research horizon.",
        "risk_controls": [],
        "rationale": format!(
            "Derived validation preserved Phase 3 rating and used trader action {} without recalculating probability or thesis.",
            trader.get("action").and_then(Value::as_str).unwrap_or("Hold")
        )
    });
    let mut artifact = artifact;
    record_market_truth_check(state, "final_trade_decision", &artifact);
    enforce_phase3_market_truth(state, &mut artifact);
    stamp_phase6_execution_constraints(state, &mut artifact);
    persist_artifact(conn, state, 6, "portfolio.manager", artifact.clone())?;
    state["final_trade_decision"] = artifact;
    Ok(())
}

/// Phase 6 has one strictly ticker-scoped FileStore unit per investable asset.
/// The aggregate below is only a transient Rust projection for allocation and
/// reports; canonical PortfolioDecision artifacts remain the authority.
async fn run_file_store_phase6(
    state: &mut Value,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
    conn: &rusqlite::Connection,
) -> Result<()> {
    let registration = config
        .authority_registry
        .registration("portfolio.manager", ToolManagedProfile::PortfolioDecision)?;
    let store_root = state
        .get("store_root")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .context("store_root missing for migrated Phase 6 PortfolioDecision")?;
    let mut artifacts = Vec::new();
    for ticker in portfolio_assets_from_state(state) {
        let mut ticker_state = state.clone();
        ticker_state["ticker"] = json!(ticker);
        ticker_state["tickers"] = json!([ticker]);
        let result = run_single_role_job_result(
            RoleRun {
                state: ticker_state.clone(),
                role: "portfolio.manager",
                phase: 6,
                kind: "artifact",
                round: None,
                topic_id: None,
                mock: is_mock(state),
                model_override,
                reasoning_effort_override,
                config,
                prompt_path: Some(
                    config
                        .prompts
                        .path_for("portfolio.manager")
                        .context("missing prompt path for portfolio.manager")?,
                ),
            },
            config.workflow.agent_timeout_sec,
            state,
            conn,
        )
        .await?;
        let plan = FileStoreDomainRuntimePlan {
            role: "portfolio.manager".to_owned(),
            phase: 6,
            profile: ToolManagedProfile::PortfolioDecision,
            profile_version: registration.profile_version,
            builder_version: registration.builder_version,
            tickers: vec![ticker.clone()],
            visible_evidence_refs: BTreeSet::new(),
            topic_id: None,
            side: None,
            round: None,
            visible_claims: BTreeSet::new(),
            fork: None,
            trade_candidate_action: None,
            portfolio_rating: portfolio_rating_for_ticker(state, &ticker),
            portfolio_current_weight: Some(runtime_current_weight(state, &ticker)),
        };
        let artifact = match result.artifact {
            Some(artifact) => artifact,
            None => {
                let failure = result
                    .error
                    .as_deref()
                    .unwrap_or("portfolio manager failed before terminal finalize");
                record_degraded_role(state, &result, failure);
                finalize_degraded_portfolio_decision(&store_root, &ticker_state, plan, failure)?
            }
        };
        artifacts.push(artifact);
    }
    let projection = project_file_store_portfolio_decisions(artifacts)?;
    record_market_truth_check(state, "final_trade_decision", &projection);
    state["final_trade_decision"] = projection;
    state["phase6_authority"] = json!("file_store");
    Ok(())
}

fn run_file_store_phase6_derived(state: &mut Value, config: &RuntimeConfig) -> Result<()> {
    let registration = config
        .authority_registry
        .registration("portfolio.manager", ToolManagedProfile::PortfolioDecision)?;
    let store_root = state
        .get("store_root")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .context("store_root missing for migrated Phase 6 PortfolioDecision")?;
    let mut artifacts = Vec::new();
    for ticker in portfolio_assets_from_state(state) {
        let mut ticker_state = state.clone();
        ticker_state["ticker"] = json!(ticker);
        ticker_state["tickers"] = json!([ticker]);
        artifacts.push(finalize_degraded_portfolio_decision(
            &store_root,
            &ticker_state,
            FileStoreDomainRuntimePlan {
                role: "portfolio.manager".to_owned(),
                phase: 6,
                profile: ToolManagedProfile::PortfolioDecision,
                profile_version: registration.profile_version,
                builder_version: registration.builder_version,
                tickers: vec![ticker.clone()],
                visible_evidence_refs: BTreeSet::new(),
                topic_id: None,
                side: None,
                round: None,
                visible_claims: BTreeSet::new(),
                fork: None,
                trade_candidate_action: None,
                portfolio_rating: portfolio_rating_for_ticker(state, &ticker),
                portfolio_current_weight: Some(runtime_current_weight(state, &ticker)),
            },
            "workflow_policy_not_triggered",
        )?);
    }
    let projection = project_file_store_portfolio_decisions(artifacts)?;
    record_market_truth_check(state, "final_trade_decision", &projection);
    state["final_trade_decision"] = projection;
    state["phase6_authority"] = json!("file_store");
    Ok(())
}

fn portfolio_assets_from_state(state: &Value) -> Vec<String> {
    let assets = state
        .get("investable_assets")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::trim)
        .filter(|ticker| !ticker.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if assets.is_empty() {
        tickers_from_state(state)
    } else {
        assets
    }
}

fn portfolio_rating_for_ticker(state: &Value, ticker: &str) -> Option<String> {
    state
        .get("research_plan")
        .and_then(|plan| plan.get("per_ticker"))
        .and_then(Value::as_object)
        .and_then(|items| items.get(ticker))
        .and_then(|item| item.get("rating"))
        .or_else(|| {
            state
                .get("research_plan")
                .and_then(|plan| plan.get("rating"))
        })
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn project_file_store_portfolio_decisions(artifacts: Vec<Value>) -> Result<Value> {
    let mut aggregate: Option<Value> = None;
    let mut per_asset = serde_json::Map::new();
    for artifact in artifacts {
        let ticker = artifact
            .get("ticker")
            .and_then(Value::as_str)
            .context("FileStore PortfolioDecision artifact missing ticker")?;
        let decision = artifact
            .get("decision")
            .cloned()
            .context("FileStore PortfolioDecision artifact missing decision")?;
        let constraint = decision
            .get("per_asset")
            .and_then(Value::as_object)
            .and_then(|assets| assets.get(ticker))
            .cloned()
            .context("FileStore PortfolioDecision missing its ticker constraint")?;
        if aggregate.is_none() {
            aggregate = Some(decision);
        }
        per_asset.insert(ticker.to_owned(), constraint);
    }
    let mut aggregate =
        aggregate.context("Phase 6 has no FileStore PortfolioDecision artifacts")?;
    aggregate["per_asset"] = Value::Object(per_asset);
    Ok(aggregate)
}

/// Phase 6 owns semantic limits, not account mechanics.  Runtime supplies the
/// current weight and fills any omitted asset with the least permissive
/// constraint compatible with the Trader direction.
fn stamp_phase6_execution_constraints(state: &Value, artifact: &mut Value) {
    let assets = state
        .get("investable_assets")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let assets = if assets.is_empty() {
        tickers_from_state(state)
    } else {
        assets
    };
    let top_status = artifact
        .get("execution_status")
        .and_then(Value::as_str)
        .filter(|status| matches!(*status, "execute" | "wait" | "downgrade"))
        .unwrap_or("wait");
    let top_controls = artifact
        .get("risk_controls")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    // Legacy portfolio artifacts expressed controls as bare strings.  Phase 6
    // is the last Rust-owned boundary before the canonical v2 artifact, so it
    // attaches the Phase 5 reviews that supplied those controls here instead
    // of weakening the canonical `BindingRiskControl` contract.
    let default_control_refs = phase5_control_source_refs(state);
    let supplied = artifact
        .get("per_asset")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let constraints = assets
        .iter()
        .map(|ticker| {
            let raw = supplied.get(ticker).unwrap_or(&Value::Null);
            let current_weight = runtime_current_weight(state, ticker);
            let trader_direction = trader_direction_constraint(state, ticker);
            let direction = raw
                .get("direction_constraint")
                .and_then(Value::as_str)
                .filter(|direction| {
                    matches!(*direction, "increase_only" | "decrease_only" | "unchanged")
                })
                .filter(|direction| {
                    trader_direction == "unchanged"
                        || *direction == trader_direction
                        || *direction == "unchanged"
                })
                .unwrap_or(trader_direction);
            let status = raw
                .get("execution_status")
                .and_then(Value::as_str)
                .filter(|status| matches!(*status, "execute" | "wait" | "downgrade"))
                .unwrap_or(top_status);
            let trader_cap = trader_position_cap(state, ticker);
            let mut max_target_weight = raw
                .get("max_target_weight")
                .and_then(Value::as_f64)
                .filter(|weight| weight.is_finite())
                .unwrap_or(trader_cap)
                .clamp(0.0, trader_cap);
            let mut max_weight_delta = raw
                .get("max_weight_delta")
                .and_then(Value::as_f64)
                .filter(|weight| weight.is_finite())
                .unwrap_or((max_target_weight - current_weight).abs())
                .clamp(0.0, 1.0);
            if status == "wait" || direction == "unchanged" {
                max_target_weight = current_weight;
                max_weight_delta = 0.0;
            } else if status == "downgrade" {
                max_target_weight = max_target_weight.min(current_weight);
                max_weight_delta = max_weight_delta.min(current_weight - max_target_weight);
            }
            let supplied_controls = raw
                .get("binding_risk_controls")
                .and_then(Value::as_array)
                .map(|controls| {
                    controls
                        .iter()
                        .filter_map(|control| {
                            if let Some(name) = control.as_str() {
                                Some(json!({
                                    "control": name,
                                    "source_refs": default_control_refs.clone(),
                                }))
                            } else {
                                let name = control.get("control")?.as_str()?.trim();
                                let refs = control
                                    .get("source_refs")?
                                    .as_array()?
                                    .iter()
                                    .filter_map(Value::as_str)
                                    .map(ToOwned::to_owned)
                                    .collect::<Vec<_>>();
                                (!name.is_empty() && !refs.is_empty())
                                    .then(|| json!({"control":name, "source_refs":refs}))
                            }
                        })
                        .collect::<Vec<_>>()
                })
                .filter(|controls| !controls.is_empty())
                .unwrap_or_else(|| {
                    top_controls
                        .iter()
                        .map(|control| {
                            json!({
                                "control": control,
                                "source_refs": default_control_refs.clone(),
                            })
                        })
                        .collect()
                });
            (
                ticker.clone(),
                json!({
                    "direction_constraint": direction,
                    "execution_status": status,
                    "current_weight": current_weight,
                    "max_target_weight": max_target_weight,
                    "max_weight_delta": max_weight_delta,
                    "binding_risk_controls": supplied_controls
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    artifact["per_asset"] = Value::Object(constraints);
}

fn phase5_control_source_refs(state: &Value) -> Vec<String> {
    let refs = state
        .get("risk_debate_state")
        .and_then(|value| value.get("history"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|turn| turn.get("role").and_then(Value::as_str))
        .map(|role| format!("phase5:{role}"))
        .collect::<Vec<_>>();
    if refs.is_empty() {
        vec!["phase5:workflow_policy".to_string()]
    } else {
        refs
    }
}

fn runtime_current_weight(state: &Value, ticker: &str) -> f64 {
    state
        .get("current_portfolio_weights")
        .and_then(Value::as_object)
        .and_then(|weights| weights.get(ticker))
        .and_then(Value::as_f64)
        .filter(|weight| weight.is_finite())
        .unwrap_or(0.0)
        .clamp(0.0, 1.0)
}

fn trader_direction_constraint(state: &Value, ticker: &str) -> &'static str {
    let plan = state
        .get("trader_investment_plan")
        .and_then(|plan| plan.get("per_ticker"))
        .and_then(Value::as_object)
        .and_then(|plans| plans.get(ticker))
        .or_else(|| state.get("trader_investment_plan"));
    match plan
        .and_then(|plan| plan.get("candidate_action").or_else(|| plan.get("action")))
        .and_then(Value::as_str)
    {
        Some("Buy") => "increase_only",
        Some("Sell") => "decrease_only",
        _ => "unchanged",
    }
}

fn trader_position_cap(state: &Value, ticker: &str) -> f64 {
    let plan = state
        .get("trader_investment_plan")
        .and_then(|plan| plan.get("per_ticker"))
        .and_then(Value::as_object)
        .and_then(|plans| plans.get(ticker))
        .or_else(|| state.get("trader_investment_plan"));
    plan.and_then(|plan| plan.get("position_size_pct_max"))
        .and_then(Value::as_f64)
        .filter(|weight| weight.is_finite())
        .unwrap_or(0.0)
        .clamp(0.0, 1.0)
}

#[allow(clippy::too_many_arguments)]
fn run_phase8(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    _config: &RuntimeConfig,
) -> Result<()> {
    let tx = conn.transaction()?;
    let run_id = state
        .get("run_id")
        .and_then(Value::as_str)
        .context("state.run_id is required for phase 8")?
        .to_string();
    let _tickers = tickers_from_state(state);
    let prediction_date = state
        .get("current_date")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let research_plan = state.get("research_plan").cloned().unwrap_or(Value::Null);
    let market_regime = market_regime_from_state(state);
    let market_regime_json = serde_json::to_value(&market_regime)?;
    let phase_count = state
        .get("workflow_metrics")
        .and_then(|value| value.get("phase_count"))
        .and_then(Value::as_i64)
        .unwrap_or_default();
    let total_elapsed_ms = state
        .get("workflow_metrics")
        .and_then(|value| value.get("total_phase_elapsed_ms"))
        .and_then(Value::as_i64)
        .unwrap_or_default();

    upsert_run_archive(
        &tx,
        &RunArchiveInput {
            run_id: run_id.clone(),
            workflow_version: "v1".to_string(),
            prompt_versions_json: json!({}),
            git_sha: String::new(),
            config_hash: String::new(),
            artifact_path: String::new(),
            degraded: state
                .get("degraded")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            phase_count,
            total_elapsed_ms,
        },
    )?;

    let learning_eligible = !is_mock(state);
    state["phase8_learning_eligible"] = Value::Bool(learning_eligible);
    let mut written_predictions = 0usize;
    // Prediction maturity is fixed in trading bars and is intentionally
    // independent from the market-data retrieval window.
    let window_days = 3;
    if learning_eligible {
        for item_ticker in tickers_from_state(state) {
            if let Some(decision) = research_decision_for_ticker(&research_plan, &item_ticker) {
                let long_probability = decision.get("long_probability").and_then(Value::as_f64);
                let short_probability = decision.get("short_probability").and_then(Value::as_f64);
                if let (Some(long_probability), Some(short_probability)) =
                    (long_probability, short_probability)
                {
                    upsert_prediction(
                        &tx,
                        &PredictionInput {
                            run_id: run_id.clone(),
                            ticker: item_ticker.clone(),
                            prediction_date: prediction_date.clone(),
                            long_probability,
                            short_probability,
                            rating: decision
                                .get("rating")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string(),
                            window_days,
                            market_regime_json: market_regime_json.clone(),
                            agent_probabilities_json: agent_probabilities_for_ticker(
                                state,
                                &item_ticker,
                            ),
                            weighted_base_probability: weighted_base_probability_for_ticker(
                                state,
                                &item_ticker,
                            ),
                        },
                    )?;
                    let final_decision = state
                        .get("final_trade_decision")
                        .cloned()
                        .unwrap_or(Value::Null);
                    let trader_action = state
                        .get("trader_investment_plan")
                        .and_then(|value| value.get("action"))
                        .and_then(Value::as_str)
                        .unwrap_or("Hold");
                    let linked_signal_id = final_decision
                        .get("signal_id")
                        .and_then(Value::as_i64)
                        .map(|value| value.to_string())
                        .or_else(|| {
                            final_decision
                                .get("signal_id")
                                .and_then(Value::as_str)
                                .map(str::to_string)
                        })
                        .or_else(|| {
                            tx.query_row(
                                r#"
                                SELECT signal_id FROM ai4trade_executions
                                WHERE run_id=?1 AND ticker=?2 AND signal_id IS NOT NULL
                                ORDER BY executed_at_ms DESC LIMIT 1
                                "#,
                                rusqlite::params![run_id, item_ticker],
                                |row| row.get::<_, String>(0),
                            )
                            .ok()
                        });
                    upsert_decision_snapshot(
                        &tx,
                        &DecisionSnapshotInput {
                            run_id: run_id.clone(),
                            ticker: item_ticker.clone(),
                            action: trader_action.to_string(),
                            decision_date: prediction_date.clone(),
                            position_id: linked_signal_id,
                            long_probability: Some(long_probability),
                            short_probability: Some(short_probability),
                            decision_json: json!({
                                "research_decision": decision,
                                "trader_action": trader_action,
                                "final_trade_decision": final_decision,
                                "counterfactual": trader_action.eq_ignore_ascii_case("hold"),
                                "note": "A three-trading-day decision snapshot; it does not force a trade or close an existing position."
                            }),
                        },
                    )?;
                    written_predictions += 1;
                }
            }
        }
    }
    if learning_eligible && written_predictions == 0 {
        state["degraded"] = Value::Bool(true);
        state["phase8_warning"] = json!("no complete ticker probabilities found in research_plan");
    }

    tx.commit()?;

    Ok(())
}

fn market_regime_from_state(state: &Value) -> MarketRegime {
    let volatility = state
        .get("allocation_context")
        .and_then(|value| value.get("vix"))
        .and_then(|value| value.get("regime"))
        .and_then(Value::as_str)
        .or_else(|| {
            state
                .get("portfolio_allocation")
                .and_then(|value| value.get("vix_regime"))
                .and_then(Value::as_str)
        })
        .unwrap_or_default()
        .to_string();
    MarketRegime {
        volatility,
        ..Default::default()
    }
}

fn research_decision_for_ticker(research_plan: &Value, ticker: &str) -> Option<Value> {
    if let Some(item) = research_plan
        .get("per_ticker")
        .and_then(Value::as_object)
        .and_then(|items| items.get(ticker))
    {
        return Some(item.clone());
    }
    if let Some(item) = research_plan
        .get("ticker_decisions")
        .and_then(Value::as_array)
        .and_then(|items| {
            items.iter().find(|item| {
                item.get("ticker")
                    .and_then(Value::as_str)
                    .is_some_and(|value| value == ticker)
            })
        })
    {
        return Some(item.clone());
    }
    research_plan
        .get("long_probability")
        .is_some()
        .then(|| research_plan.clone())
}

fn agent_probabilities_for_ticker(state: &Value, ticker: &str) -> Value {
    state
        .get("phase1_index")
        .and_then(|value| value.get("per_ticker"))
        .and_then(Value::as_object)
        .and_then(|items| items.get(ticker))
        .and_then(|value| value.get("role_summaries"))
        .cloned()
        .unwrap_or_else(|| json!({}))
}

fn weighted_base_probability_for_ticker(state: &Value, ticker: &str) -> Option<f64> {
    state
        .get("weighted_probability_base")
        .and_then(Value::as_object)
        .and_then(|items| items.get(ticker))
        .and_then(|value| {
            value
                .get("long_probability")
                .or_else(|| value.get("weighted_long_probability"))
                .or_else(|| value.get("probability"))
        })
        .and_then(Value::as_f64)
}

#[allow(clippy::too_many_arguments)]
async fn run_phase7(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    _model_override: Option<&str>,
    _reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<()> {
    debug!("allocation context computation starting");
    load_phase7_account_weights(state, config).await;
    if let Some(decision) = state.get("final_trade_decision").cloned() {
        let mut decision = decision;
        stamp_phase6_execution_constraints(state, &mut decision);
        state["final_trade_decision"] = decision;
    }
    let context = compute_allocation_context(state, conn, &config.allocation)?;
    state["allocation_context"] = allocation_prompt_context(&context);
    debug!(vix_regime = ?context.get("vix").and_then(|v| v.get("regime")), "allocation context ready");
    let mut allocation = derive_guarded_allocation(state, &context, &config.allocation)?;
    allocation["id"] = json!("allocator.rust");
    allocation["role"] = json!("allocator.rust");
    allocation["status"] = json!("usable");
    sanitize_downstream_constraints(state, "portfolio_allocation", &mut allocation);
    persist_artifact(conn, state, 7, "allocator.rust", allocation.clone())?;
    state["portfolio_allocation"] = allocation;
    debug!("Rust allocation guardrails completed");
    Ok(())
}

/// Account state is a runtime input, never an LLM assertion.  It is optional
/// for a plan-only run, but a successful read refreshes current weights before
/// Phase 7 projects Phase 6's semantic constraints.
async fn load_phase7_account_weights(state: &mut Value, config: &RuntimeConfig) {
    let disabled_reason = if is_mock(state) {
        Some("mock runs never read trading accounts")
    } else if state.get("debug").and_then(Value::as_bool) == Some(true) {
        Some("debug runs do not change execution semantics")
    } else if config
        .alpaca_api_key
        .as_deref()
        .is_none_or(|value| value.trim().is_empty())
        || config
            .alpaca_api_secret
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
    {
        Some("Paper Trading credentials are unavailable")
    } else {
        None
    };
    if let Some(reason) = disabled_reason {
        state["phase7_account_snapshot"] = json!({"status": "data_gap", "reason": reason});
        return;
    }
    let tickers = state
        .get("investable_assets")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let tool_config = orchestrator_llm::tools::ExternalToolConfig {
        project_root: orchestrator_core::default_project_root(),
        db_path: state
            .get("db_path")
            .and_then(Value::as_str)
            .filter(|path| !path.trim().is_empty())
            .map(std::path::PathBuf::from),
        run_id: state
            .get("run_id")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        phase: Some(7),
        tickers,
        alpaca_live: true,
        alpaca_api_key: config.alpaca_api_key.clone(),
        alpaca_api_secret: config.alpaca_api_secret.clone(),
        ..Default::default()
    };
    match orchestrator_llm::tools::alpaca::get_portfolio(&tool_config).await {
        Ok(snapshot) => {
            let equity = snapshot
                .get("equity")
                .and_then(Value::as_f64)
                .filter(|value| *value > 0.0 && value.is_finite());
            let weights = equity
                .map(|equity| {
                    snapshot
                        .get("positions")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(|position| {
                            let ticker = position.get("symbol")?.as_str()?.to_string();
                            let market_value = position.get("market_value")?.as_f64()?;
                            Some((ticker, json!((market_value / equity).clamp(0.0, 1.0))))
                        })
                        .collect::<serde_json::Map<_, _>>()
                })
                .unwrap_or_default();
            state["current_portfolio_weights"] = Value::Object(weights);
            state["phase7_account_snapshot"] = snapshot;
        }
        Err(error) => {
            state["phase7_account_snapshot"] = json!({
                "status": "data_gap",
                "reason": "account snapshot unavailable",
                "error": error.to_string()
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_llm::web_search::{
        WebSearchContextSize, WebSearchMode, WebSearchProviderKind,
    };
    use orchestrator_llm::LlmRoute;

    fn manifest_runtime_config(store_root: &Path) -> (Value, RuntimeConfig) {
        let config = json!({
            "orchestrator": {
                "plugins": {"enabled": false},
                "llm": {
                    "defaults": {
                        "route": "responses",
                        "model": "test-model",
                        "base_url": "https://llm.example.com/v1",
                        "api_key": "test-key",
                        "max_turns": null,
                        "reasoning_effort": null,
                        "native_web_search": false,
                        "think_tool": false,
                        "tools": "all"
                    }
                },
                "store": {
                    "root": store_root.to_string_lossy(),
                    "schema_version": 1,
                    "retain_turn_history": false,
                    "retain_debug_history": true,
                    "atomic_fsync": false,
                    "stale_temp_age_sec": 3600
                }
            }
        });
        let mut runtime = RuntimeConfig::from_value(&config).unwrap();
        runtime
            .authority_registry
            .migrate_to_file_store(
                "analyst.technical",
                orchestrator_core::ToolManagedProfile::AnalystReport,
            )
            .unwrap();
        (config, runtime)
    }

    #[test]
    fn phase_summary_authority_prepares_the_file_store_manifest() {
        let directory = tempfile::tempdir().unwrap();
        let config = json!({
            "orchestrator": {
                "plugins": {"enabled": false},
                "llm": {
                    "defaults": {
                        "route": "responses",
                        "model": "test-model",
                        "base_url": "https://llm.example.com/v1",
                        "api_key": "test-key",
                        "max_turns": null,
                        "reasoning_effort": null,
                        "native_web_search": false,
                        "think_tool": false,
                        "tools": "all"
                    }
                },
                "store": {
                    "root": directory.path().to_string_lossy(),
                    "atomic_fsync": false
                }
            }
        });
        let runtime = RuntimeConfig::from_value(&config).unwrap();
        assert!(has_file_store_authority(&runtime));

        let missing_root = directory.path().join("not-created");
        assert!(prepare_file_store_run_manifest_if_migrated(
            &missing_root,
            &runtime,
            &config,
            "2026-07-27",
            "phase-summary-file-store",
        )
        .unwrap()
        .is_some());
        assert!(missing_root.exists());
    }

    #[test]
    fn migrated_reflection_persists_only_file_store_completion_record() {
        let directory = tempfile::tempdir().unwrap();
        let state = json!({
            "store_root": directory.path(),
            "current_date": "2026-07-27",
            "run_id": "reflection-current",
            "reflection_task": {
                "task_id": 41,
                "source_run_id": "historical-run",
                "ticker": "QQQ",
                "decision": {"action":"Hold"},
                "outcome": {"actual_return": -0.03}
            }
        });
        let artifact = json!({
            "kind": "experience",
            "role": "reflector.historical",
            "source_run_id": "historical-run",
            "ticker": "QQQ",
            "index_id": "experience-index"
        });
        assert_eq!(
            persist_file_store_reflection_record(&state, 41, &artifact).unwrap(),
            1
        );
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = RunLocation::new("2026-07-27", "reflection-current").unwrap();
        let record = read_learning_record(&store, &location, LearningKind::Reflection, "QQQ").unwrap();
        assert_eq!(record.payload["task_id"], 41);
        assert_eq!(record.payload["experience_index_id"], "experience-index");
        assert!(phase0_reflection_is_completed_in_file_store(&state, &state["reflection_task"]).unwrap());
    }

    #[test]
    fn migrated_phase_summary_never_flushes_the_legacy_database_projection() {
        let directory = tempfile::tempdir().unwrap();
        let (_config, runtime) = manifest_runtime_config(directory.path());
        assert!(phase_summary_uses_file_store(&runtime).unwrap());
    }

    #[tokio::test]
    async fn injected_phase1_file_store_authority_writes_canonical_ticker_units_without_sqlite_projection(
    ) {
        let directory = tempfile::tempdir().unwrap();
        let (_config, mut runtime) = manifest_runtime_config(directory.path());
        runtime
            .authority_registry
            .migrate_to_file_store(
                "analyst.news_macro",
                orchestrator_core::ToolManagedProfile::AnalystReport,
            )
            .unwrap();
        let mut state = json!({
            "run_id": "phase1-file-store-test",
            "current_date": "2026-07-27",
            "ticker": "QQQ,SOXX",
            "tickers": ["QQQ", "SOXX"],
            "analysis_universe": ["QQQ", "SOXX"],
            "store_root": directory.path(),
            "mock": true,
            "debug": false,
            "phase1_agents": ["analyst.technical", "analyst.news_macro"],
            "phase_status": {},
        });
        let db = directory.path().join("legacy.sqlite");
        let mut conn = connect(&db).unwrap();
        let roles = vec![
            "analyst.technical".to_owned(),
            "analyst.news_macro".to_owned(),
        ];

        run_phase1(&mut conn, &mut state, &roles, None, None, &runtime)
            .await
            .unwrap();

        assert_eq!(state["phase1_index_authority"], "file_store_derived");
        for role in &roles {
            assert_eq!(state["analyst_reports"][role]["authority"], "file_store");
            assert_eq!(
                state["analyst_reports"][role]["per_ticker"]
                    .as_object()
                    .unwrap()
                    .len(),
                2
            );
        }
        let legacy_phase1_rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM role_turn_summaries \
                 WHERE phase = 1 AND (role LIKE 'analyst.%' OR role = 'phase1.index')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(legacy_phase1_rows, 0);

        let artifacts_root = directory.path().join("runs");
        let artifact_count = fs::read_dir(artifacts_root)
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|date| fs::read_dir(date.path()).ok())
            .flatten()
            .filter_map(Result::ok)
            .filter_map(|run| fs::read_dir(run.path().join("artifacts/phase1")).ok())
            .flatten()
            .filter_map(Result::ok)
            .filter_map(|role| fs::read_dir(role.path()).ok())
            .flatten()
            .filter_map(Result::ok)
            .filter(|artifact| artifact.path().extension().is_some_and(|ext| ext == "json"))
            .count();
        assert_eq!(artifact_count, 4);

        let summaries = write_deterministic_phase_summary(
            directory.path(),
            &state,
            1,
            runtime.tool_managed.max_summary_units_per_phase,
        )
        .unwrap();
        assert_eq!(summaries.indexes.len(), 4);
        assert!(summaries
            .indexes
            .iter()
            .all(|index| index.kind == orchestrator_store::IndexKind::PhaseSummary));
    }

    #[tokio::test]
    async fn default_phase3_file_store_authority_never_persists_a_sqlite_artifact() {
        let directory = tempfile::tempdir().unwrap();
        let (_config, runtime) = manifest_runtime_config(directory.path());
        assert!(research_uses_file_store(&runtime).unwrap());
        let mut state = json!({
            "run_id": "phase3-file-store-test",
            "current_date": "2026-07-27",
            "ticker": "QQQ",
            "tickers": ["QQQ"],
            "analysis_universe": ["QQQ"],
            "store_root": directory.path(),
            "mock": true,
            "debug": false,
            "phase_status": {},
            "phase1_index": {"per_ticker": {"QQQ": {"evidence_quality": {"confidence_basis": "evidence_available"}}}},
            "weighted_probability_base": {"QQQ": {"long_probability": 0.5, "short_probability": 0.5}},
        });
        let db = directory.path().join("legacy.sqlite");
        let mut conn = connect(&db).unwrap();

        run_phase3(&mut conn, &mut state, None, None, &runtime)
            .await
            .unwrap();

        assert_eq!(state["research_plan_authority"], "file_store");
        let legacy_rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM role_turn_summaries WHERE phase = 3 AND role = 'manager.research'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(legacy_rows, 0);
        let artifact = directory.path().join(
            RunLocation::new("2026-07-27", "phase3-file-store-test")
                .unwrap()
                .child_relative(Path::new("artifacts/phase3/research-decision.json"))
                .unwrap(),
        );
        assert!(artifact.exists());
    }

    #[test]
    fn file_store_manifest_is_created_then_recovered_without_legacy_artifacts() {
        let directory = tempfile::tempdir().unwrap();
        let (config, runtime) = manifest_runtime_config(directory.path());
        let location = RunLocation::new("2026-07-27", "manifest-recovery").unwrap();

        let created = prepare_file_store_run_manifest(
            directory.path(),
            &runtime,
            &config,
            "2026-07-27",
            "manifest-recovery",
        )
        .unwrap();
        assert!(directory.path().join(location.manifest_relative()).exists());
        assert!(created.artifacts.is_empty());

        let recovered = prepare_file_store_run_manifest(
            directory.path(),
            &runtime,
            &config,
            "2026-07-27",
            "manifest-recovery",
        )
        .unwrap();
        assert_eq!(recovered, created);
    }

    #[test]
    fn file_store_manifest_recovery_rejects_changed_authority_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let (config, runtime) = manifest_runtime_config(directory.path());
        prepare_file_store_run_manifest(
            directory.path(),
            &runtime,
            &config,
            "2026-07-27",
            "authority-mismatch",
        )
        .unwrap();

        let mut changed_runtime = runtime.clone();
        // All builtin profiles are FileStore-authoritative now; use the
        // explicit legacy registry to simulate opening a manifest with a
        // materially different authority snapshot.
        changed_runtime.authority_registry = orchestrator_core::AuthorityRegistry::builtin_legacy();
        let error = prepare_file_store_run_manifest(
            directory.path(),
            &changed_runtime,
            &config,
            "2026-07-27",
            "authority-mismatch",
        )
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("authority_registry_hash differs"));
    }

    #[test]
    fn file_store_manifest_recovery_rejects_a_tampered_content_hash() {
        let directory = tempfile::tempdir().unwrap();
        let (config, runtime) = manifest_runtime_config(directory.path());
        let location = RunLocation::new("2026-07-27", "tampered-manifest").unwrap();
        prepare_file_store_run_manifest(
            directory.path(),
            &runtime,
            &config,
            "2026-07-27",
            "tampered-manifest",
        )
        .unwrap();

        let path = directory.path().join(location.manifest_relative());
        let original = fs::read_to_string(&path).unwrap();
        assert!(original.contains("\"status\":\"running\""));
        fs::write(
            &path,
            original.replacen("\"status\":\"running\"", "\"status\":\"completed\"", 1),
        )
        .unwrap();

        let error = prepare_file_store_run_manifest(
            directory.path(),
            &runtime,
            &config,
            "2026-07-27",
            "tampered-manifest",
        )
        .unwrap_err();
        assert!(error.to_string().contains("content hash mismatch"));
    }

    #[test]
    fn file_store_manifest_recovery_rejects_a_valid_manifest_at_the_wrong_location() {
        let directory = tempfile::tempdir().unwrap();
        let (config, runtime) = manifest_runtime_config(directory.path());
        let source_location = RunLocation::new("2026-07-27", "source-run").unwrap();
        let target_location = RunLocation::new("2026-07-27", "other-run").unwrap();
        prepare_file_store_run_manifest(
            directory.path(),
            &runtime,
            &config,
            "2026-07-27",
            "source-run",
        )
        .unwrap();

        let source = directory.path().join(source_location.manifest_relative());
        let target = directory.path().join(target_location.manifest_relative());
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::copy(source, target).unwrap();

        let error = prepare_file_store_run_manifest(
            directory.path(),
            &runtime,
            &config,
            "2026-07-27",
            "other-run",
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("manifest run identity differs from requested store location"));
    }

    #[test]
    fn file_store_run_manifests_are_isolated_by_store_root() {
        let directory = tempfile::tempdir().unwrap();
        let first_root = directory.path().join("first");
        let second_root = directory.path().join("second");
        let (first_config, first_runtime) = manifest_runtime_config(&first_root);
        let (second_config, second_runtime) = manifest_runtime_config(&second_root);
        let location = RunLocation::new("2026-07-27", "isolated-run").unwrap();

        prepare_file_store_run_manifest(
            &first_root,
            &first_runtime,
            &first_config,
            "2026-07-27",
            "isolated-run",
        )
        .unwrap();
        assert!(first_root.join(location.manifest_relative()).exists());
        assert!(!second_root.join(location.manifest_relative()).exists());

        prepare_file_store_run_manifest(
            &second_root,
            &second_runtime,
            &second_config,
            "2026-07-27",
            "isolated-run",
        )
        .unwrap();
        assert!(second_root.join(location.manifest_relative()).exists());
    }

    fn test_llm_settings(native_web_search: bool) -> orchestrator_llm::RoleLlmSettings {
        orchestrator_llm::RoleLlmSettings {
            route: LlmRoute::Responses,
            model: "gpt-5.4".to_string(),
            preamble: None,
            max_turns: Some(4),
            max_completion_tokens: None,
            reasoning_effort: None,
            reasoning_summary: None,
            preserve_reasoning_state: false,
            text_verbosity: None,
            transport: Default::default(),
            base_url: Some("https://llm.example.com/v1".to_string()),
            api_key: Some("test-key".to_string()),
            think_tool: false,
            tools: Vec::new(),
            native_web_search,
            free_opencode: false,
        }
    }

    fn test_llm_roles<I>(
        roles: I,
    ) -> std::collections::BTreeMap<String, orchestrator_llm::RoleLlmSettings>
    where
        I: IntoIterator<Item = &'static str>,
    {
        roles
            .into_iter()
            .map(|role| (role.to_string(), test_llm_settings(false)))
            .collect()
    }

    #[test]
    fn llm_roles_inherit_global_defaults_and_builtin_role_values() {
        let roles = crate::orchestration::config::required_llm_roles()
            .iter()
            .map(|role| ((*role).to_string(), json!({})))
            .collect::<serde_json::Map<_, _>>();
        let config = json!({
            "orchestrator": {
                "llm": {
                    "defaults": {
                        "route": "responses",
                        "model": "gpt-5.4",
                        "base_url": "https://llm.example.com/v1",
                        "api_key": "test-key",
                        "max_turns": null,
                        "reasoning_effort": "medium",
                        "native_web_search": true,
                        "think_tool": false,
                        "tools": "all"
                    },
                    "roles": roles
                }
            }
        });

        let roles = crate::orchestration::config::llm_roles_from_config(&config).unwrap();
        let settings = &roles["analyst.technical"];
        assert_eq!(settings.model, "gpt-5.4");
        assert_eq!(settings.max_turns, Some(12));
        assert_eq!(settings.reasoning_effort.as_deref(), Some("medium"));
        assert!(settings.native_web_search);
        assert!(!settings.tools.contains(&"read_run_context".to_string()));
        assert!(settings
            .tools
            .contains(&"read_technical_snapshot".to_string()));
        assert!(settings.tools.contains(&"read_experience".to_string()));
        for role in ["trader", "risk.conservative", "portfolio.manager"] {
            assert_eq!(
                roles[role].tools,
                vec!["read_phase_summaries", "read_phase_summary_details"],
                "role={role}"
            );
        }
    }

    #[test]
    fn llm_role_config_overrides_defaults() {
        let mut roles = crate::orchestration::config::required_llm_roles()
            .iter()
            .map(|role| ((*role).to_string(), json!({})))
            .collect::<serde_json::Map<_, _>>();
        roles.insert(
            "manager.research".to_string(),
            json!({
                "model": "role-model",
                "max_turns": 4,
                "reasoning_effort": "low",
                "tools": ["read_phase_summaries", "read_phase_summary_details"]
            }),
        );
        let config = json!({
            "orchestrator": {
                "llm": {
                    "defaults": {
                        "route": "responses",
                        "model": "default-model",
                        "base_url": "https://llm.example.com/v1",
                        "api_key": "test-key",
                        "max_turns": null,
                        "reasoning_effort": "medium",
                        "native_web_search": true,
                        "think_tool": false,
                        "tools": "all"
                    },
                    "roles": roles
                }
            }
        });

        let roles = crate::orchestration::config::llm_roles_from_config(&config).unwrap();
        let settings = &roles["manager.research"];
        assert_eq!(settings.model, "role-model");
        assert_eq!(settings.max_turns, Some(4));
        assert_eq!(settings.reasoning_effort.as_deref(), Some("low"));
        assert_eq!(
            settings.tools,
            vec![
                "read_phase_summaries".to_string(),
                "read_phase_summary_details".to_string()
            ]
        );
    }

    #[test]
    fn llm_roles_reject_deepseek_route() {
        let roles = crate::orchestration::config::required_llm_roles()
            .iter()
            .map(|role| ((*role).to_string(), json!({})))
            .collect::<serde_json::Map<_, _>>();
        let config = json!({
            "orchestrator": {
                "llm": {
                    "defaults": {
                        "route": "deepseek",
                        "model": "gpt-5.4",
                        "base_url": "https://llm.example.com/v1",
                        "api_key": "test-key",
                        "max_turns": null,
                        "reasoning_effort": "medium",
                        "native_web_search": false,
                        "think_tool": false,
                        "transport": "ws",
                        "tools": "all"
                    },
                    "roles": roles
                }
            }
        });

        let err = crate::orchestration::config::llm_roles_from_config(&config).unwrap_err();

        assert!(format!("{err:#}").contains("invalid LLM config"));
    }

    #[test]
    fn web_search_applies_builtin_role_defaults() {
        let config = json!({
            "orchestrator": {
                "llm": {
                    "roles": {
                        "analyst.technical": {},
                        "analyst.news_macro": {}
                    }
                }
            }
        });
        let roles = test_llm_roles(["analyst.technical", "analyst.news_macro"]);

        let web_search =
            crate::orchestration::config::web_search_by_role_from_config(&config, roles.iter())
                .unwrap();

        let technical = &web_search["analyst.technical"];
        assert_eq!(
            technical,
            &orchestrator_llm::web_search::WebSearchConfig::default()
        );
        assert_eq!(technical.mode, WebSearchMode::Disabled);
        assert_eq!(technical.provider, WebSearchProviderKind::Mock);
        assert_eq!(technical.context_size, WebSearchContextSize::Medium);
        assert_eq!(technical.max_result_chars, 12_000);

        let news_macro = &web_search["analyst.news_macro"];
        assert_eq!(news_macro.mode, WebSearchMode::Live);
        assert_eq!(news_macro.provider, WebSearchProviderKind::Mock);
        assert_eq!(news_macro.context_size, WebSearchContextSize::Medium);
        assert_eq!(news_macro.max_result_chars, 12_000);
    }

    #[test]
    fn role_web_search_override_merges_with_global_config() {
        let config = json!({
            "orchestrator": {
                "web_search": {
                    "mode": "disabled",
                    "provider": "mock",
                    "context_size": "high",
                    "max_result_chars": 9000
                },
                "llm": {
                    "roles": {
                        "analyst.technical": {
                            "web_search": {
                                "mode": "live"
                            }
                        },
                        "analyst.news_macro": {}
                    }
                }
            }
        });
        let roles = test_llm_roles(["analyst.technical", "analyst.news_macro"]);

        let web_search =
            crate::orchestration::config::web_search_by_role_from_config(&config, roles.iter())
                .unwrap();

        assert_eq!(web_search["analyst.technical"].mode, WebSearchMode::Live);
        assert_eq!(
            web_search["analyst.technical"].provider,
            WebSearchProviderKind::Mock
        );
        assert_eq!(
            web_search["analyst.technical"].context_size,
            WebSearchContextSize::High
        );
        assert_eq!(web_search["analyst.technical"].max_result_chars, 9000);
        assert_eq!(web_search["analyst.news_macro"].mode, WebSearchMode::Live);
        assert_eq!(
            web_search["analyst.news_macro"].provider,
            WebSearchProviderKind::Mock
        );
        assert_eq!(
            web_search["analyst.news_macro"].context_size,
            WebSearchContextSize::High
        );
        assert_eq!(web_search["analyst.news_macro"].max_result_chars, 9000);
    }

    #[test]
    fn web_search_deserializes_camel_case_fields() {
        let config = json!({
            "orchestrator": {
                "web_search": {
                    "mode": "cached",
                    "provider": "exa",
                    "baseUrl": "https://mcp.exa.ai/mcp",
                    "contextSize": "low",
                    "allowedDomains": ["example.com"],
                    "blockedDomains": ["blocked.example"],
                    "maxResultChars": 4096
                },
                "llm": {
                    "roles": {
                        "analyst.technical": {
                            "web_search": {
                                "contextSize": "high"
                            }
                        }
                    }
                }
            }
        });
        let roles = test_llm_roles(["analyst.technical"]);

        let web_search =
            crate::orchestration::config::web_search_by_role_from_config(&config, roles.iter())
                .unwrap();
        let role_config = &web_search["analyst.technical"];

        assert_eq!(role_config.mode, WebSearchMode::Cached);
        assert_eq!(role_config.provider, WebSearchProviderKind::Exa);
        assert_eq!(
            role_config.base_url.as_deref(),
            Some("https://mcp.exa.ai/mcp")
        );
        assert_eq!(role_config.api_key, None);
        assert_eq!(role_config.context_size, WebSearchContextSize::High);
        assert_eq!(role_config.allowed_domains, vec!["example.com"]);
        assert_eq!(role_config.blocked_domains, vec!["blocked.example"]);
        assert_eq!(role_config.max_result_chars, 4096);
    }

    #[test]
    fn web_search_validation_reports_invalid_shared_field() {
        let config = json!({
            "orchestrator": {
                "web_search": {
                    "mode": "live",
                    "context_size": "huge"
                },
                "llm": {
                    "roles": {
                        "analyst.technical": {}
                    }
                }
            }
        });
        let roles = test_llm_roles(["analyst.technical"]);

        let err =
            crate::orchestration::config::web_search_by_role_from_config(&config, roles.iter())
                .unwrap_err();
        let message = format!("{err:#}");

        assert!(message.contains("context_size"));
    }

    #[test]
    fn web_search_accepts_live_exa_without_api_key() {
        let config = json!({
            "orchestrator": {
                "web_search": {
                    "mode": "live",
                    "provider": "exa"
                },
                "llm": {
                    "roles": {
                        "analyst.technical": {}
                    }
                }
            }
        });
        let roles = test_llm_roles(["analyst.technical"]);

        let web_search =
            crate::orchestration::config::web_search_by_role_from_config(&config, roles.iter())
                .unwrap();
        let role_config = &web_search["analyst.technical"];

        assert_eq!(role_config.mode, WebSearchMode::Live);
        assert_eq!(role_config.provider, WebSearchProviderKind::Exa);
        assert_eq!(role_config.api_key, None);
    }

    #[test]
    fn web_search_rejects_tavily_provider() {
        let config = json!({
            "orchestrator": {
                "web_search": {
                    "mode": "live",
                    "provider": "tavily"
                },
                "llm": {
                    "roles": {
                        "analyst.technical": {}
                    }
                }
            }
        });
        let roles = test_llm_roles(["analyst.technical"]);

        let err =
            crate::orchestration::config::web_search_by_role_from_config(&config, roles.iter())
                .unwrap_err();
        let message = format!("{err:#}");

        assert!(message.contains("provider"));
    }

    #[test]
    fn web_search_rejects_tavily_even_when_role_has_native_web_search() {
        let config = json!({
            "orchestrator": {
                "web_search": {
                    "mode": "live",
                    "provider": "tavily"
                },
                "llm": {
                    "roles": {
                        "analyst.technical": {
                            "native_web_search": true
                        }
                    }
                }
            }
        });
        let roles = std::collections::BTreeMap::from([(
            "analyst.technical".to_string(),
            test_llm_settings(true),
        )]);

        let err =
            crate::orchestration::config::web_search_by_role_from_config(&config, roles.iter())
                .unwrap_err();

        assert!(format!("{err:#}").contains("provider"));
    }

    #[test]
    fn web_search_preserves_direct_api_key_without_requiring_env() {
        let config = json!({
            "orchestrator": {
                "web_search": {
                    "mode": "live",
                    "provider": "exa",
                    "api_key": "sk-secret-do-not-leak"
                },
                "llm": {
                    "roles": {
                        "analyst.technical": {}
                    }
                }
            }
        });
        let roles = test_llm_roles(["analyst.technical"]);

        let web_search =
            crate::orchestration::config::web_search_by_role_from_config(&config, roles.iter())
                .unwrap();
        let role_config = &web_search["analyst.technical"];

        assert_eq!(
            role_config.api_key.as_deref(),
            Some("sk-secret-do-not-leak")
        );
    }

    #[test]
    fn parse_phase1_agents_rejects_standalone_fundamental() {
        let err = parse_phase1_agents("technical,news,fundamental").unwrap_err();

        assert!(err.to_string().contains("fundamental analyst was removed"));
    }

    #[test]
    fn parse_phase1_agents_normalizes_supported_roles() {
        let roles = parse_phase1_agents("technical,news").unwrap();

        assert_eq!(roles, vec!["analyst.technical", "analyst.news_macro"]);
    }

    #[test]
    fn phase3_market_truth_overrides_portfolio_market_fields() {
        let state = json!({
            "research_plan": {
                "rating": "Buy",
                "long_probability": 0.68,
                "short_probability": 0.32,
                "plan": "Phase 3 authoritative thesis."
            }
        });
        let mut downstream = json!({
            "rating": "Sell",
            "long_probability": 0.41,
            "investment_thesis": "Downstream rewritten thesis.",
            "execution_summary": "Reduce execution strength."
        });

        enforce_phase3_market_truth(&state, &mut downstream);

        assert_eq!(downstream["rating"], "Buy");
        assert_eq!(
            downstream["investment_thesis"],
            "Phase 3 authoritative thesis."
        );
        assert_eq!(downstream["llm_rating"], "Sell");
        assert_eq!(
            downstream["llm_investment_thesis"],
            "Downstream rewritten thesis."
        );
        assert!(downstream.get("long_probability").is_none());
        assert_eq!(downstream["llm_long_probability"], 0.41);
        assert_eq!(
            downstream["execution_summary"],
            "Reduce execution strength."
        );
    }

    #[test]
    fn phase3_probability_drift_without_converged_evidence_falls_back_to_base() {
        let state = json!({
            "tickers": ["QQQ"],
            "weighted_probability_base": {
                    "QQQ": {"long_probability": 0.50, "short_probability": 0.50}
                },
            "debate_state_artifact": {
                "convergence_status": "converged_or_pending_review",
                "per_ticker": {
                    "QQQ": {
                        "convergence_status": "converged_or_pending_review",
                        "decision_hinges": []
                    }
                },
                "topic_briefs": [{
                    "tickers": ["QQQ"],
                    "controller_artifact": {
                        "soft_control": {"should_continue": false, "stop_reason": "no_info_gain"}
                    }
                }]
            }
        });
        let artifact = json!({
            "rating": "Overweight",
            "long_probability": 0.59,
            "short_probability": 0.41,
            "plan": "Track confirmation.",
            "probability_rationale": "Manager adjustment.",
            "per_ticker": {
                "QQQ": {
                    "rating": "Overweight",
                    "long_probability": 0.59,
                    "short_probability": 0.41,
                    "plan": "Track confirmation.",
                    "probability_rationale": "Manager adjustment."
                }
            }
        });

        let violations = phase3_probability_drift_violations(&state, &artifact);
        let guarded = apply_phase3_probability_fallback(artifact, &violations);

        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0]["ticker"], "QQQ");
        assert_eq!(violations[0]["severity"], "warning");
        assert_eq!(guarded["long_probability"], 0.50);
        assert_eq!(guarded["short_probability"], 0.50);
        assert_eq!(guarded["per_ticker"]["QQQ"]["long_probability"], 0.50);
        assert_eq!(
            guarded["probability_guard"]["status"],
            "clamped_to_phase1_base"
        );
    }

    #[test]
    fn phase3_probability_drift_with_converged_evidence_is_accepted() {
        let state = json!({
            "tickers": ["QQQ"],
            "weighted_probability_base": {
                    "QQQ": {"long_probability": 0.50, "short_probability": 0.50}
                },
            "debate_state_artifact": {
                "per_ticker": {
                    "QQQ": {
                        "convergence_status": "converged",
                        "decision_hinges": [{
                            "hinge": "earnings revision breadth",
                            "evidence_refs": ["evidence:earnings-breadth"]
                        }]
                    }
                }
            }
        });
        let artifact = json!({
            "rating": "Overweight",
            "long_probability": 0.60,
            "short_probability": 0.40,
            "plan": "Track earnings revisions.",
            "probability_rationale": "Converged evidence supports the adjustment.",
            "per_ticker": {
                "QQQ": {
                    "rating": "Overweight",
                    "long_probability": 0.60,
                    "short_probability": 0.40,
                    "plan": "Track earnings revisions.",
                    "probability_rationale": "Converged evidence supports the adjustment."
                }
            }
        });

        assert!(phase3_probability_drift_violations(&state, &artifact).is_empty());
    }

    #[test]
    fn phase3_probability_adjustment_without_valid_debate_is_rejected() {
        let state = json!({
            "tickers": ["QQQ"],
            "weighted_probability_base": {
                    "QQQ": {"long_probability": 0.50, "short_probability": 0.50}
                }
        });
        let artifact = json!({
            "rating": "Overweight",
            "long_probability": 0.58,
            "short_probability": 0.42,
            "per_ticker": {
                "QQQ": {
                    "rating": "Overweight",
                    "long_probability": 0.58,
                    "short_probability": 0.42
                }
            }
        });

        let violations = phase3_probability_drift_violations(&state, &artifact);
        assert_eq!(violations.len(), 1);
        assert!(violations[0]["reason"]
            .as_str()
            .unwrap()
            .contains("requires a converged decision hinge"));
    }

    #[test]
    fn missing_data_convergence_is_enforced_from_itemized_and_critical_gaps() {
        let state = json!({
            "tickers": ["QQQ"],
            "phase1_index": {
                "evidence_quality": {"missing_critical_roles": ["analyst.technical"]},
                "per_ticker": {"QQQ": {"missing_evidence": ["current price confirmation"]}}
            },
            "debate_state_artifact": {
                "per_ticker": {"QQQ": {"missing_high_impact_factors": ["rate-path surprise"]}}
            }
        });
        let mut artifact = json!({
            "rating": "Overweight",
            "long_probability": 0.65,
            "short_probability": 0.35,
            "base_probability": 0.60,
            "debate_adjustment": 0.05,
            "scenarios": {
                "bull": {"probability": 0.50},
                "base": {"probability": 0.30},
                "bear": {"probability": 0.20}
            },
            "per_ticker": {"QQQ": {
                "rating": "Overweight",
                "long_probability": 0.65,
                "short_probability": 0.35,
                "base_probability": 0.60,
                "debate_adjustment": 0.05,
                "scenarios": {
                    "bull": {"probability": 0.50},
                    "base": {"probability": 0.30},
                    "bear": {"probability": 0.20}
                }
            }}
        });

        apply_missing_data_convergence(&state, &mut artifact);

        let premium = &artifact["per_ticker"]["QQQ"]["missing_data_convergence"];
        assert_eq!(premium["item_count"], 2);
        assert!((premium["requested_convergence"].as_f64().unwrap() - 0.05).abs() < 1e-9);
        assert!(
            (artifact["per_ticker"]["QQQ"]["long_probability"]
                .as_f64()
                .unwrap()
                - 0.60)
                .abs()
                < 1e-9
        );
        assert!((artifact["long_probability"].as_f64().unwrap() - 0.60).abs() < 1e-9);
        assert!(
            (artifact["per_ticker"]["QQQ"]["scenarios"]["bull"]["probability"]
                .as_f64()
                .unwrap()
                - 0.45)
                .abs()
                < 1e-9
        );
        assert!(
            (artifact["per_ticker"]["QQQ"]["scenarios"]["bear"]["probability"]
                .as_f64()
                .unwrap()
                - 0.25)
                .abs()
                < 1e-9
        );
    }

    #[test]
    fn phase3_critical_probability_drift_is_clamped_per_ticker() {
        let state = json!({
            "tickers": ["QQQ", "SOXX"],
            "weighted_probability_base": {
                    "QQQ": {"long_probability": 0.55, "short_probability": 0.45},
                    "SOXX": {"long_probability": 0.45, "short_probability": 0.55}
                }
        });
        let artifact = json!({
            "rating": "Overweight",
            "long_probability": 0.57,
            "short_probability": 0.43,
            "per_ticker": {
                "QQQ": {
                    "rating": "Overweight",
                    "long_probability": 0.57,
                    "short_probability": 0.43
                },
                "SOXX": {
                    "rating": "Overweight",
                    "long_probability": 0.66,
                    "short_probability": 0.34
                }
            }
        });

        let violations = phase3_probability_drift_violations(&state, &artifact);
        let guarded = apply_phase3_probability_fallback(artifact, &violations);

        assert_eq!(violations.len(), 2);
        assert_eq!(violations[0]["ticker"], "QQQ");
        assert_eq!(violations[0]["severity"], "warning");
        assert_eq!(violations[1]["ticker"], "SOXX");
        assert_eq!(violations[1]["severity"], "critical");
        assert_eq!(guarded["per_ticker"]["SOXX"]["long_probability"], 0.45);
        assert_eq!(guarded["per_ticker"]["SOXX"]["short_probability"], 0.55);
        assert_eq!(guarded["per_ticker"]["QQQ"]["long_probability"], 0.55);
        assert_eq!(guarded["long_probability"], 0.55);
    }

    #[test]
    fn phase3_missing_ticker_probability_is_clamped_to_base() {
        let state = json!({
            "tickers": ["QQQ"],
            "weighted_probability_base": {
                    "QQQ": {"long_probability": 0.50, "short_probability": 0.50}
                }
        });
        let artifact = json!({
            "rating": "Buy",
            "long_probability": 0.90,
            "short_probability": 0.10,
            "per_ticker": {"QQQ": {}}
        });

        let violations = phase3_probability_drift_violations(&state, &artifact);
        let guarded = apply_phase3_probability_fallback(artifact, &violations);

        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0]["severity"], "critical");
        assert_eq!(guarded["long_probability"], 0.50);
        assert_eq!(guarded["per_ticker"]["QQQ"]["long_probability"], 0.50);
        assert_eq!(guarded["per_ticker"]["QQQ"]["rating"], "Hold");
    }

    #[test]
    fn downstream_constraints_strip_market_truth_fields() {
        let mut downstream = json!({
            "rating": "Sell",
            "long_probability": 0.41,
            "short_probability": 0.59,
            "probability_rationale": "Downstream probability rewrite.",
            "investment_thesis": "Downstream thesis rewrite.",
            "action": "Hold",
            "position_size": "0%"
        });

        strip_downstream_market_truth_fields(&mut downstream);

        for field in [
            "rating",
            "long_probability",
            "short_probability",
            "probability_rationale",
            "investment_thesis",
        ] {
            assert!(
                downstream.get(field).is_none(),
                "{field} should be stripped"
            );
        }
        assert_eq!(downstream["llm_rating"], "Sell");
        assert_eq!(downstream["llm_long_probability"], 0.41);
        assert_eq!(
            downstream["llm_investment_thesis"],
            "Downstream thesis rewrite."
        );
        assert_eq!(downstream["action"], "Hold");
        assert_eq!(downstream["position_size"], "0%");
    }

    #[test]
    fn preflight_error_marks_state_degraded() {
        let mut state = json!({"degraded": false});
        crate::orchestration::degraded::record_preflight_result(
            &mut state,
            "read_technical_snapshot",
            Err(anyhow::anyhow!("missing technical data")),
        );

        assert_eq!(state["degraded"], true);
        assert_eq!(
            state["preflight"]["read_technical_snapshot"]["status"],
            "error"
        );
        assert!(state["preflight"]["read_technical_snapshot"]["message"]
            .as_str()
            .unwrap()
            .contains("missing technical data"));
    }

    #[test]
    fn phase6_constraints_use_runtime_weight_and_cannot_reverse_trader() {
        let state = json!({
            "investable_assets": ["QQQ"],
            "current_portfolio_weights": {"QQQ": 0.25},
            "trader_investment_plan": {
                "candidate_action": "Buy",
                "position_size_pct_max": 0.4
            }
        });
        let mut artifact = json!({
            "execution_status": "execute",
            "risk_controls": ["cap concentration"],
            "per_asset": {
                "QQQ": {
                    "direction_constraint": "decrease_only",
                    "execution_status": "execute",
                    "current_weight": 0.99,
                    "max_target_weight": 0.8,
                    "max_weight_delta": 0.8,
                    "binding_risk_controls": []
                }
            }
        });

        stamp_phase6_execution_constraints(&state, &mut artifact);

        assert_eq!(
            artifact["per_asset"]["QQQ"]["direction_constraint"],
            "increase_only"
        );
        assert_eq!(artifact["per_asset"]["QQQ"]["current_weight"], 0.25);
        assert_eq!(artifact["per_asset"]["QQQ"]["max_target_weight"], 0.4);
        assert_eq!(
            artifact["per_asset"]["QQQ"]["binding_risk_controls"],
            json!([{
                "control": "cap concentration",
                "source_refs": ["phase5:workflow_policy"]
            }])
        );
    }

    #[tokio::test]
    async fn technical_preflight_rejects_missing_sqlite_import_source() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        orchestrator_sql::ensure_schema(&conn).unwrap();
        let mut state = json!({
            "degraded": false,
            "tech_refresh_enabled": false,
            "analysis_universe": ["MISSING_STRICT_SQLITE_TEST"]
        });
        crate::orchestration::policy::run_technical_csv_preflight(&mut conn, &mut state)
            .await
            .unwrap();
        assert_eq!(
            state["preflight"]["read_technical_snapshot"]["status"],
            "error"
        );
    }
}
#[test]
fn steer_packets_exclude_recursive_transport_fields() {
    let turn = json!({
        "role": "researcher.bull.interaction",
        "kind": "bull_packet",
        "round": 2,
        "topic_id": "QQQ-aggregate",
        "session": {"session_id": "session", "turn_id": "turn"},
        "artifact": {
            "claims": [{"claim": "price confirmation", "evidence_ref": "tech-1"}],
            "summary": "one-line delta",
            "steer": "recursively nested prior artifact",
            "prompt_path": "/large/path",
            "session_id": "session",
            "turn_id": "turn"
        }
    });

    let compact = compact_debate_turn(&turn);

    assert_eq!(compact["artifact"]["claims"][0]["evidence_ref"], "tech-1");
    assert_eq!(compact["artifact"]["summary"], "one-line delta");
    assert!(compact.get("session").is_none());
    assert!(compact["artifact"].get("steer").is_none());
    assert!(compact["artifact"].get("prompt_path").is_none());
    assert!(compact["artifact"].get("session_id").is_none());
}

#[test]
fn point_debate_steer_embeds_opponent_claims() {
    let controller = json!({
        "role": "mediator.topic_controller",
        "artifact": {
            "next_steers": {
                "to_bull": {"must_address": ["bear-1"], "instruction": "rebut liquidity claim"}
            },
            "accepted_for_opponent": {
                "bull": [{"claim_id": "bear-1", "claim": "failed breakout"}]
            }
        }
    });
    let opponent = json!({
        "role": "researcher.bear.initial",
        "kind": "bear_seed",
        "artifact": {
            "claims": [{
                "claim_id": "bear-1",
                "claim": "failed breakout risk",
                "decision_hinge": "price reclaim",
                "confidence": 0.6,
                "evidence_refs": ["tech-1"]
            }]
        }
    });
    let own = json!({
        "role": "researcher.bull.initial",
        "kind": "bull_seed",
        "artifact": {
            "claims": [{"claim_id": "bull-1", "claim": "repair bounce"}]
        }
    });

    let steer: Value = serde_json::from_str(&build_point_debate_steer(
        &controller,
        "bull",
        &opponent,
        &own,
    ))
    .unwrap();
    assert_eq!(steer["kind"], "point_debate");
    assert_eq!(steer["side"], "bull");
    assert_eq!(steer["reply_to_required"], true);
    assert_eq!(steer["opponent_claims_to_address"][0]["claim_id"], "bear-1");
    assert_eq!(steer["accepted_for_you"][0]["claim_id"], "bear-1");
    assert!(steer["mediator_instruction"]
        .get("instruction")
        .and_then(Value::as_str)
        .unwrap_or("")
        .contains("rebut"));
}

#[test]
fn topic_controller_forks_from_topic_generation_with_its_own_prompt() {
    let state = json!({
        "phase2_warmup": {"turn_id": "warmup-shared-ready"},
        "topic_generation_turn_id": "turn-topic-root"
    });
    let source = fork_source_turn_id(&state, "QQQ-volatility", "mediator.topic_controller");
    let steer: Value = serde_json::from_str(
        &attach_fork_source(Some(steer_payload("seed_claims", &json!({}))), source, true).unwrap(),
    )
    .unwrap();

    assert_eq!(steer["fork_from_turn_id"], "turn-topic-root");
    assert_eq!(steer["include_prompt_on_fork"], true);
}

#[test]
fn topic_generator_starts_without_the_warmup_checkpoint() {
    let steer: Value = serde_json::from_str(&topic_generation_steer().unwrap()).unwrap();

    assert!(steer.get("fork_from_turn_id").is_none());
    assert!(steer.get("include_prompt_on_fork").is_none());
}

#[test]
fn phase2_initial_researchers_fork_from_the_shared_warmup_checkpoint() {
    let state = json!({
        "phase2_warmup": {"turn_id": "warmup-shared-ready"},
        "topic_generation_turn_id": "turn-topic-root"
    });

    assert_eq!(
        fork_source_turn_id(&state, "QQQ-volatility", "researcher.bull.initial"),
        Some("warmup-shared-ready".to_string())
    );
    assert_eq!(
        fork_source_turn_id(&state, "QQQ-volatility", "researcher.bear.initial"),
        Some("warmup-shared-ready".to_string())
    );
    assert_eq!(
        fork_source_turn_id(
            &json!({"topic_generation_turn_id": "turn-topic-root"}),
            "QQQ-volatility",
            "researcher.bull.initial"
        ),
        None
    );
}

#[test]
fn interaction_forks_include_the_current_role_prompt() {
    let state = json!({});
    for role in ["researcher.bull.interaction", "researcher.bear.interaction"] {
        let source = fork_source_turn_id(&state, "QQQ-volatility", role);
        let include_prompt = role == "mediator.topic_controller"
            || role == "researcher.bull.initial"
            || role == "researcher.bear.initial"
            || role == "researcher.bull.interaction"
            || role == "researcher.bear.interaction";
        let steer: Value = serde_json::from_str(
            &attach_fork_source(
                Some(steer_payload("point_debate", &json!({}))),
                source,
                include_prompt,
            )
            .unwrap(),
        )
        .unwrap();

        assert_eq!(steer["include_prompt_on_fork"], true, "{role}");
    }
}

#[test]
fn phase4_runtime_keeps_only_the_research_candidate_or_hold() {
    let state = json!({"research_plan": {"rating": "Buy"}});
    let mut reversed = json!({
        "action": "Sell",
        "candidate_action": "Sell",
        "execution_decision": "execute_candidate",
        "position_size_pct_max": 0.25,
        "blockers": []
    });
    enforce_trade_candidate(&state, &mut reversed);
    assert_eq!(reversed["candidate_action"], "Buy");
    assert_eq!(reversed["action"], "Hold");
    assert_eq!(reversed["execution_decision"], "hold");
    assert_eq!(reversed["position_size_pct_max"], 0.0);

    let mut valid = json!({
        "action": "Buy",
        "execution_decision": "execute_candidate",
        "position_size_pct_max": 0.15,
        "blockers": []
    });
    enforce_trade_candidate(&state, &mut valid);
    assert_eq!(valid["action"], "Buy");
    assert_eq!(valid["position_size_pct_max"], 0.15);
}
