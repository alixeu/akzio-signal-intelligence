use anyhow::{bail, Context, Result};
use chrono::{Local, NaiveDate};
use orchestrator_core::{
    config_int, config_str, config_strings, default_project_root, display_ticker, load_config,
    parse_tickers, project_path, research_rating_for_probability, MarketRegime,
};
use orchestrator_sql::{
    archive::{upsert_run_archive, RunArchiveInput},
    clear_agent_loop_history, connect, pending_reflection_tasks, persist_reflection_artifact,
    prediction::{upsert_prediction, PredictionInput},
    score_mature_predictions, set_reflection_task_status, set_run_current_phase, update_run_status,
    upsert_decision_snapshot, write_run_record, DecisionSnapshotInput, ReflectionThresholds,
    RunRecordInput,
};
use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
    time::Instant,
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
use crate::orchestration::config::{validate_sqlite_context, RuntimeConfig};
use crate::orchestration::degraded::role_artifact_or_degraded;
use crate::orchestration::lifecycle::{
    append_topic_controller_artifact, append_topic_turn, record_contracts,
    research_plan_to_trade_intent, run_id_for, set_phase_status, set_topic_controller_state,
    tickers_from_state, upsert_topic_debate_state,
};
use crate::orchestration::policy::{enforce_preflight_policy, run_phase1_preflight};
use crate::orchestration::policy::{
    evaluate_workflow_policy, record_workflow_policy, WorkflowPolicyDecision, WorkflowPolicyMode,
    WorkflowPolicySignals,
};
use crate::orchestration::render::mode_prompt_path;
use crate::orchestration::retrieval::inject_phase_summary_reflection;
use crate::orchestration::role_jobs::{
    merge_role_job_metrics, persist_prompt_metric, prepare_role_job, record_role_job_metrics,
    run_role_jobs, run_single_role_job, run_single_steer_role_job, RoleRun, SteerRoleRun,
};
use crate::orchestration::PHASE2_REDUCER;
use orchestrator_core::role_registry::DEFAULT_PHASE1_AGENTS;

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
    debug!(
        plugins_enabled = runtime_config.plugins.enabled,
        component_plugins = runtime_config.component_plugins.components.len(),
        role_plugins = runtime_config.role_plugins.roles.len(),
        "prompt plugin runtime config loaded"
    );
    let run_dir = resolve_run_dir(&args);
    let db_path = resolve_db_path(&args, &config);
    let mut conn = connect(&db_path)?;
    let run_id = run_id_for(&tickers, &date);
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
            let alpaca_history = if runtime_config.alpaca_api_key.is_some()
                && runtime_config.alpaca_api_secret.is_some()
            {
                let tool_config = orchestrator_llm::tools::ExternalToolConfig {
                    project_root: default_project_root(),
                    db_path: Some(db_path.clone()),
                    run_dir: run_dir.clone(),
                    run_id: Some(run_id.clone()),
                    phase: Some(0),
                    allowed_reflection_task_ids: Vec::new(),
                    tickers: runtime_config.allocation.investable_assets.clone(),
                    alpaca_live: true,
                    alpaca_api_key: runtime_config.alpaca_api_key.clone(),
                    alpaca_api_secret: runtime_config.alpaca_api_secret.clone(),
                    phase_summary_index: None,
                    phase_summary_gate: None,
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
        set_phase_status(&mut state, PHASE2_REDUCER, phase2_status);
        record_phase2_summary_debug_artifact(&mut state, phase2_status)?;
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
            run_phase4_rust_rule(&mut conn, &mut state)?;
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
            run_phase5_skipped(&mut conn, &mut state)?;
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
            run_phase6_derived(&mut conn, &mut state)?;
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
        debug!(status = ?state["phase_status"]["8"], "phase 8 archive/prediction finished");
    }
    // Drain any compress still running when the pipeline ends early (e.g. to_phase < 8).
    await_all_compress_jobs(&mut compress_jobs, &mut state).await?;

    // Idempotent insurance: each phase is already persisted before its gate completes.
    let phase_summary_flushed =
        crate::orchestration::compress::flush_phase_summary_to_sqlite(&conn, &mut state)?;
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
        set_reflection_task_status(conn, task_id, "running", None)?;
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
            match persist_reflection_artifact(
                conn,
                task_id,
                &config.reflection.reflection_version,
                &artifact,
            ) {
                Ok(experience_count) => {
                    processed += 1;
                    audit.push(json!({
                        "task_id": task_id,
                        "status": "completed",
                        "experience_count": experience_count
                    }));
                }
                Err(error) => {
                    failed += 1;
                    set_reflection_task_status(conn, task_id, "failed", Some(&error.to_string()))?;
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
            set_reflection_task_status(conn, task_id, "failed", Some(&message))?;
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
        if !mock {
            run_phase1_preflight(conn, state, role, config).await?;
            enforce_preflight_policy(state, role, config)?;
        }
    }

    let mut jobs = Vec::new();
    for role in roles {
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
    debug!(job_count = jobs.len(), "phase 1 jobs prepared");
    let results = run_role_jobs(
        jobs,
        config.workflow.phase1_parallelism,
        config.workflow.agent_timeout_sec,
    )
    .await;

    let mut reports = serde_json::Map::new();
    for result in results {
        let role = result.role.clone();
        debug!(
            role,
            elapsed_ms = result.elapsed_ms,
            timed_out = result.timed_out,
            ok = result.artifact.is_some(),
            "phase 1 role finished"
        );
        persist_prompt_metric(conn, &result);
        record_role_job_metrics(state, &result);
        let artifact = role_artifact_or_degraded(state, config, result)?;
        persist_artifact(conn, state, 1, &role, artifact.clone())?;
        reports.insert(role.clone(), artifact);
    }
    state["analyst_reports"] = Value::Object(reports);
    // Materialize phase1_index in-process (no separate phase 1.5 / phase 15).
    materialize_phase1_index(conn, state, config)?;
    Ok(())
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
    let db_path = state
        .get("db_path")
        .and_then(Value::as_str)
        .map(|s| s.to_string())
        .context("db_path missing from state")?;

    // Start topic generation and both side-specific warmups together. Each topic
    // later forks Bull/Bear from its side warmup and Controller from topic generation.
    let model_override_owned = model_override.map(|s| s.to_string());
    let reasoning_effort_override_owned = reasoning_effort_override.map(|s| s.to_string());
    let topic_db_path = db_path.clone();
    let mut topic_state = state.clone();
    topic_state["role_job_metrics"] = json!([]);
    let mut warmup_state = state.clone();
    warmup_state["role_job_metrics"] = json!([]);
    let topic_config = config.clone();
    let topic_model_override = model_override_owned.clone();
    let topic_reasoning_override = reasoning_effort_override_owned.clone();
    let (topics_result, warmup_result) = tokio::join!(
        async move {
            let mut topic_conn = orchestrator_sql::connect(&topic_db_path)
                .with_context(|| format!("topic-gen connect {topic_db_path}"))?;
            let topics = run_phase2_topic_generation(
                &mut topic_conn,
                &mut topic_state,
                topic_model_override.as_deref(),
                topic_reasoning_override.as_deref(),
                &topic_config,
            )
            .await?;
            Ok::<_, anyhow::Error>((topics, topic_state))
        },
        run_phase2_side_warmups(
            warmup_state,
            model_override,
            reasoning_effort_override,
            config,
        )
    );
    let (topics, topic_state) = topics_result?;
    let (warmup, warmup_metrics) = warmup_result?;
    for key in [
        "topic_generation_artifact",
        "topic_generation_turn_id",
        "debate_topics",
    ] {
        if let Some(value) = topic_state.get(key) {
            state[key] = value.clone();
        }
    }
    if let Some(metrics) = topic_state.get("role_job_metrics") {
        merge_role_job_metrics(state, metrics);
    }
    state["phase2_warmup"] = warmup;
    merge_role_job_metrics(state, &warmup_metrics);
    let topics = topics
        .into_iter()
        .take(max_topics.max(1) as usize)
        .collect::<Vec<_>>();
    debug!(
        topic_count = topics.len(),
        "phase 2 topic generation and side warmups ready"
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

async fn run_phase2_side_warmups(
    state: Value,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<(Value, Value)> {
    let run_id = state
        .get("run_id")
        .and_then(Value::as_str)
        .unwrap_or("run")
        .to_string();
    if is_mock(&state) {
        return Ok((
            json!({
                "ready": true,
                "mode": "mock",
                "llm_calls": 0,
                "bull": warmup_metadata(&run_id, "bull"),
                "bear": warmup_metadata(&run_id, "bear")
            }),
            json!([]),
        ));
    }

    let (bull_result, bear_result) = tokio::join!(
        run_phase2_side_warmup(
            state.clone(),
            "bull",
            model_override,
            reasoning_effort_override,
            config,
        ),
        run_phase2_side_warmup(
            state,
            "bear",
            model_override,
            reasoning_effort_override,
            config,
        )
    );
    let (bull, bull_metrics) = bull_result?;
    let (bear, bear_metrics) = bear_result?;
    let mut metrics = Vec::new();
    if let Some(items) = bull_metrics.as_array() {
        metrics.extend(items.iter().cloned());
    }
    if let Some(items) = bear_metrics.as_array() {
        metrics.extend(items.iter().cloned());
    }
    Ok((
        json!({
            "ready": true,
            "mode": "live",
            "llm_calls": 2,
            "bull": bull,
            "bear": bear
        }),
        Value::Array(metrics),
    ))
}

async fn run_phase2_side_warmup(
    mut state: Value,
    side: &str,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<(Value, Value)> {
    let run_id = state
        .get("run_id")
        .and_then(Value::as_str)
        .unwrap_or("run")
        .to_string();
    let role = format!("researcher.{side}.initial");
    let metadata = warmup_metadata(&run_id, side);
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
        .with_context(|| format!("missing warmup prompt for {role}"))?
        .clone();
    for attempt in 1..=2 {
        let artifact = run_single_steer_role_job(
            SteerRoleRun {
                state: state.clone(),
                role: &role,
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
        .await?;
        if artifact.get("status").and_then(Value::as_str) == Some("ready")
            && artifact.get("response").and_then(Value::as_str) == Some("准备完毕")
        {
            return Ok((
                metadata,
                state
                    .get("role_job_metrics")
                    .cloned()
                    .unwrap_or_else(|| json!([])),
            ));
        }
        if attempt == 1 {
            tracing::warn!(role, "phase 2 warmup handshake failed; retrying once");
        }
    }
    bail!("{side} warmup did not return the required 准备完毕 handshake")
}

fn warmup_metadata(run_id: &str, side: &str) -> Value {
    let id = format!("{run_id}:phase2:warmup:{side}");
    json!({
        "session_id": id.clone(),
        "turn_id": id,
        "ack": "准备完毕",
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
    debug!(
        topic_id,
        "phase 2 steer-room topic debate starting (forked after warm-up)"
    );

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
    if role.contains("bull.initial") || role.contains("bear.initial") {
        let side = if role.contains("bull") {
            "bull"
        } else {
            "bear"
        };
        return state
            .get("phase2_warmup")?
            .get(side)?
            .get("turn_id")?
            .as_str()
            .map(ToString::to_string);
    }
    if role.contains("bull.interaction") {
        return Some(format!("turn-{topic_id}-bull-initial"));
    }
    if role.contains("bear.interaction") {
        return Some(format!("turn-{topic_id}-bear-initial"));
    }
    if role == "mediator.topic_controller" {
        return state
            .get("topic_generation_turn_id")?
            .as_str()
            .filter(|turn_id| !turn_id.is_empty())
            .map(ToString::to_string);
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
            phase: if role == "mediator.topic_controller" {
                PHASE2_REDUCER
            } else {
                2
            },
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
        if role == "mediator.topic_controller" {
            PHASE2_REDUCER
        } else {
            2
        },
        role,
        kind,
        Some(round),
        Some(topic_id),
        artifact.clone(),
    )?;
    let turn = json!({
        "role": role,
        "phase": if role == "mediator.topic_controller" { PHASE2_REDUCER } else { 2 },
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
        let prompt_path = config
            .prompts
            .path_for("mediator.topic")
            .context("missing mediator.topic prompt path")?
            .clone();
        let job = prepare_role_job(RoleRun {
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
        })?;
        let result = run_role_jobs(vec![job], 1, config.workflow.reducer_timeout_sec)
            .await
            .into_iter()
            .next()
            .context("mediator.topic returned no role result")?;
        let turn_id = result.turn_id.clone();
        persist_prompt_metric(conn, &result);
        record_role_job_metrics(state, &result);
        let generated = role_artifact_or_degraded(state, config, result)?;
        if generated.get("artifact_type").and_then(Value::as_str)
            == Some("phase2_topic_generation_artifact")
        {
            artifact = merge_topic_generation_output(&baseline, &generated);
            if !turn_id.is_empty() {
                state["topic_generation_turn_id"] = Value::String(turn_id);
            }
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
    for field in ["common_ground", "summary", "reducer_checks"] {
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
) -> Result<()> {
    let artifact = build_phase1_index(state, config);
    let brief = reducer_brief_md(&artifact);
    state["phase1_index"] = artifact.clone();
    state["phase1_brief_md"] = Value::String(brief.clone());
    persist_artifact_with_last_md(conn, state, 1, "phase1.index", artifact, brief)?;
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
        PHASE2_REDUCER,
        "reducer.debate_final",
        artifact.clone(),
        brief,
    )?;
    record_local_debug_artifact(state, PHASE2_REDUCER, "reducer.debate_final", &artifact)?;
    set_phase_status(state, PHASE2_REDUCER, "done");
    // Compression is scheduled by the main pipeline so it can overlap later work.
    Ok(())
}

/// Result of a background phase-00 LLM compress job, already persisted to SQLite.
struct CompressJobResult {
    source_phase: i64,
    written: usize,
    batch: orchestrator_sql::PhaseSummaryPhaseBatch,
    debug_enabled: bool,
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
    let batch = if is_mock(&state) {
        crate::orchestration::compress::build_phase_compress(&state, source_phase)?
    } else {
        let source_payload =
            crate::orchestration::compress::phase_summary_source_payload(&state, source_phase)?;
        let prompt_path = config
            .prompts
            .path_for("compressor.phase_summary")
            .context("missing compressor.phase_summary prompt path")?
            .clone();
        let mut job = prepare_role_job(RoleRun {
            state: state.clone(),
            role: "compressor.phase_summary",
            phase: 0,
            kind: "phase_summary",
            round: Some(source_phase),
            topic_id: None,
            mock: false,
            model_override: model_override.as_deref(),
            reasoning_effort_override: reasoning_effort_override.as_deref(),
            config: &config,
            prompt_path: Some(prompt_path.as_path()),
        })?;
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
            bail!("phase_summary compressor failed for phase {source_phase}: {error}");
        }
        let artifact = result
            .artifact
            .as_ref()
            .context("phase_summary compressor returned no artifact")?;
        crate::orchestration::compress::phase_summary_bundle_to_batch(
            &state,
            source_phase,
            artifact,
        )?
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
    let role_metrics = state
        .get("role_job_metrics")
        .and_then(Value::as_array)
        .and_then(|items| items.last())
        .cloned()
        .map(|item| json!([item]))
        .unwrap_or_else(|| json!([]));
    Ok(CompressJobResult {
        source_phase,
        written,
        batch,
        debug_enabled,
        role_metrics,
    })
}

fn apply_compress_result(state: &mut Value, result: CompressJobResult) -> Result<()> {
    merge_role_job_metrics(state, &result.role_metrics);
    let snapshot = crate::orchestration::compress::apply_phase_summary_batch(state, result.batch)?;
    state["phase_compress"][result.source_phase.to_string()]["persisted"] = json!(true);
    state["phase_summary_tables"][result.source_phase.to_string()]["persisted"] = json!(true);
    if result.debug_enabled {
        let role = format!("compressor.after_phase_{}", result.source_phase);
        record_local_debug_artifact(state, 0, &role, &snapshot)?;
    }
    debug!(
        source_phase = result.source_phase,
        written = result.written,
        "phase_summary compress applied to memory state"
    );
    Ok(())
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
    artifact: &Value,
) -> Result<()> {
    if state.get("debug").and_then(Value::as_bool) != Some(true) {
        return Ok(());
    }

    let started = Instant::now();
    let relative_path = orchestrator_llm::debug_record_relative_path(phase, role);
    let path = default_project_root().join(&relative_path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create debug dir {}", parent.display()))?;
    }

    // Never clobber an existing LLM req/resp debug file with a local stub.
    // Instead attach the final artifact onto the last exchange when possible.
    if path.exists() {
        if let Ok(existing) = fs::read_to_string(&path) {
            if let Some(line) = existing.lines().find(|line| !line.trim().is_empty()) {
                if let Ok(mut value) = serde_json::from_str::<Value>(line) {
                    let has_req_resp = value.get("req").is_some() || value.get("resp").is_some();
                    if has_req_resp {
                        if let Some(object) = value.as_object_mut() {
                            object.insert("final_artifact".to_string(), artifact.clone());
                        }
                        let mut merged = serde_json::to_string(&value)?;
                        merged.push('\n');
                        fs::write(&path, merged.as_bytes()).with_context(|| {
                            format!("failed to merge debug workflow record {}", path.display())
                        })?;
                        if let Some(records) = state
                            .get_mut("debug_phase_records")
                            .and_then(Value::as_array_mut)
                        {
                            records.push(json!({
                                "kind": "llm_with_final_artifact",
                                "phase": phase,
                                "role": role,
                                "path": relative_path
                            }));
                        }
                        orchestrator_llm::debug_log_time(
                            &default_project_root(),
                            json!({
                                "kind": "function",
                                "name": format!("record_local_debug_artifact_merge:{role}"),
                                "phase": phase,
                                "role": role,
                                "elapsed_ms": started.elapsed().as_millis(),
                            }),
                        );
                        return Ok(());
                    }
                }
            }
        }
    }

    // Shared envelope with LLM debug records: req/resp present (null for local).
    let record = json!({
        "kind": "local_reducer",
        "phase": phase,
        "role": role,
        "req": Value::Null,
        "resp": Value::Null,
        "artifact": artifact
    });
    let mut line = serde_json::to_string(&record)?;
    line.push('\n');
    fs::write(&path, line.as_bytes())
        .with_context(|| format!("failed to write debug workflow record {}", path.display()))?;

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
            "path": relative_path
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

fn record_phase2_summary_debug_artifact(state: &mut Value, status: &str) -> Result<()> {
    let artifact = json!({
        "id": "phase2.summary",
        "role": "phase2.summary",
        "phase": 2,
        "status": status,
        "reason": state
            .get("topic_generation_artifact")
            .and_then(|artifact| artifact.get("reason"))
            .cloned()
            .unwrap_or(Value::Null),
        "topic_generation": state.get("topic_generation_artifact").cloned().unwrap_or(Value::Null),
        "debate_turn_count": state.get("debate_turns").and_then(Value::as_array).map(Vec::len).unwrap_or_default()
    });
    record_local_debug_artifact(state, 2, "phase2.summary", &artifact)
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
    let mut artifact = run_single_role_job(
        RoleRun {
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
        },
        config.workflow.agent_timeout_sec,
        config,
        state,
        conn,
    )
    .await?;
    enforce_phase3_deterministic_fields(state, &mut artifact);
    if artifact
        .get("degraded")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        anyhow::bail!(
            "manager.research returned a degraded artifact: {}",
            artifact
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("unknown validation failure")
        );
    }
    let initial_violations = phase3_probability_drift_violations(state, &artifact);
    let artifact = if initial_violations.is_empty() {
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
        let retry_result = run_single_role_job(
            RoleRun {
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
            },
            config.workflow.agent_timeout_sec,
            config,
            state,
            conn,
        )
        .await;
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
    persist_artifact(conn, state, 3, "manager.research", artifact.clone())?;
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
            prompt_path: Some(
                config
                    .prompts
                    .path_for("trader")
                    .context("missing prompt path for trader")?,
            ),
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
    // LLM path already wrote outputs/debug/phase04/trader.jsonl with req/resp.
    // Merge final artifact only if that file exists; never replace with a bare stub.
    record_local_debug_artifact(state, 4, "trader", &artifact)?;
    state["trader_investment_plan"] = artifact;
    Ok(())
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

fn run_phase4_rust_rule(conn: &mut rusqlite::Connection, state: &mut Value) -> Result<()> {
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
    record_local_debug_artifact(state, 4, "trader", &artifact)?;
    state["trader_investment_plan"] = artifact;
    Ok(())
}

async fn run_phase5(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<()> {
    state["risk_debate_state"] = json!({"history": []});
    // Sequential perspectives make each reviewer answer the prior argument.
    for (index, risk_role) in ["risk.aggressive", "risk.neutral", "risk.conservative"]
        .into_iter()
        .enumerate()
    {
        let round = (index + 1) as i64;
        let result = run_role_jobs(
            vec![prepare_role_job(RoleRun {
                state: state.clone(),
                role: risk_role,
                phase: 5,
                kind: "risk_argument",
                round: Some(round),
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
            })?],
            1,
            config.workflow.agent_timeout_sec,
        )
        .await
        .into_iter()
        .next()
        .context("risk committee member returned no result")?;
        persist_prompt_metric(conn, &result);
        record_role_job_metrics(state, &result);
        let mut artifact = role_artifact_or_degraded(state, config, result)?;
        sanitize_downstream_constraints(state, risk_role, &mut artifact);
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
            risk_role,
            "risk_argument",
            Some(round),
            turn,
        )?;
    }
    Ok(())
}

fn run_phase5_skipped(conn: &mut rusqlite::Connection, state: &mut Value) -> Result<()> {
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

async fn run_phase6(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<()> {
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
    persist_artifact(conn, state, 6, "portfolio.manager", artifact.clone())?;
    state["final_trade_decision"] = artifact;
    Ok(())
}

fn run_phase6_derived(conn: &mut rusqlite::Connection, state: &mut Value) -> Result<()> {
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
    persist_artifact(conn, state, 6, "portfolio.manager", artifact.clone())?;
    state["final_trade_decision"] = artifact;
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_llm::web_search::{
        WebSearchContextSize, WebSearchMode, WebSearchProviderKind,
    };
    use orchestrator_llm::LlmRoute;

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
            .contains(&"read_technical_context".to_string()));
        assert!(settings.tools.contains(&"read_experience".to_string()));
        for role in ["trader", "risk.conservative"] {
            assert!(roles[role].tools.is_empty(), "role={role}");
        }
        assert_eq!(
            roles["portfolio.manager"].tools,
            vec![
                "alpaca_get_portfolio".to_string(),
                "alpaca_get_price".to_string(),
                "alpaca_submit_trade".to_string(),
            ]
        );
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
            "read_technical_context",
            Err(anyhow::anyhow!("missing technical data")),
        );

        assert_eq!(state["degraded"], true);
        assert_eq!(
            state["preflight"]["read_technical_context"]["status"],
            "error"
        );
        assert!(state["preflight"]["read_technical_context"]["message"]
            .as_str()
            .unwrap()
            .contains("missing technical data"));
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
            state["preflight"]["read_technical_context"]["status"],
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
    let state = json!({"topic_generation_turn_id": "turn-topic-root"});
    let source = fork_source_turn_id(&state, "QQQ-volatility", "mediator.topic_controller");
    let steer: Value = serde_json::from_str(
        &attach_fork_source(Some(steer_payload("seed_claims", &json!({}))), source, true).unwrap(),
    )
    .unwrap();

    assert_eq!(steer["fork_from_turn_id"], "turn-topic-root");
    assert_eq!(steer["include_prompt_on_fork"], true);
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
