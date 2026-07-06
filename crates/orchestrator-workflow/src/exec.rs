use anyhow::{bail, Context, Result};
use chrono::{Local, NaiveDate};
use clap::{Args, ValueEnum};
use orchestrator_core::{
    config_int, config_str, default_project_root, display_ticker, load_config, parse_tickers,
    project_path, run_slug,
};
use orchestrator_sql;
use orchestrator_sql::{connect, write_run_record, RunRecordInput};
use serde_json::{json, Value};
use std::{fs, path::PathBuf, time::Instant};
use tracing::debug;

use crate::orchestration::allocation::{compute_allocation_context, normalize_allocation};
use crate::orchestration::artifact::{
    build_debate_state_artifact, build_phase1_state_artifact, build_topic_generation_artifact,
    merge_reducer_output, persist_artifact, persist_artifact_with_last_md, persist_message,
    persist_message_with_topic, reducer_brief_md, topic_id_from_topic,
    topics_from_generation_artifact,
};
use crate::orchestration::config::{config_weight, validate_sqlite_context, RuntimeConfig};
use crate::orchestration::contract::record_contracts;
use crate::orchestration::degraded::{manager_research_fallback, role_artifact_or_degraded};
use crate::orchestration::market_truth::market_truth_violation_report;
use crate::orchestration::policy::{
    evaluate_workflow_policy, record_workflow_policy, WorkflowPolicyDecision, WorkflowPolicyMode,
    WorkflowPolicySignals,
};
use crate::orchestration::preflight::{enforce_preflight_policy, run_phase1_preflight};
use crate::orchestration::render::mode_prompt_path;
use crate::orchestration::role_jobs::{
    merge_role_job_metrics, prepare_role_job, record_role_job_metrics, run_role_jobs,
    run_single_role_job, run_single_steer_role_job, RoleRun, SteerRoleRun,
};
use crate::orchestration::state::{
    append_topic_controller_artifact, append_topic_turn, run_id_for, set_phase_status,
    set_topic_controller_state, tickers_from_state, upsert_topic_debate_state, write_final_summary,
    write_json,
};
use crate::orchestration::trade_intent::research_plan_to_trade_intent;
use crate::orchestration::PHASE2_REDUCER;
use orchestrator_core::role_registry::DEFAULT_PHASE1_AGENTS;

type TopicDebateResult = (String, Vec<Value>, Value, Value);

struct PhaseTimer {
    phase: i64,
    label: &'static str,
    started_at: Instant,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum Mode {
    Probability,
    Monitor,
}

impl Mode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Mode::Probability => "probability",
            Mode::Monitor => "monitor",
        }
    }
}

#[derive(Debug, Clone, Args)]
pub struct ExecArgs {
    pub ticker: String,
    #[arg(long)]
    pub date: Option<String>,
    #[arg(long, default_value = "zh")]
    pub lang: String,
    #[arg(long, value_enum, default_value_t = Mode::Probability)]
    pub mode: Mode,
    #[arg(long, default_value_t = 150)]
    pub window_days: i64,
    #[arg(long, default_value = "technical,news,youtube,reddit,x")]
    pub phase1_agents: String,
    #[arg(long)]
    pub db_path: Option<PathBuf>,
    #[arg(long)]
    pub run_dir: Option<PathBuf>,
    #[arg(long)]
    pub config: Option<PathBuf>,
    #[arg(long)]
    pub model: Option<String>,
    #[arg(long)]
    pub reasoning_effort: Option<String>,
    #[arg(long, default_value_t = 5)]
    pub max_debate_rounds: i64,
    #[arg(long, default_value_t = 10)]
    pub max_topics_per_side: i64,
    #[arg(long, default_value_t = 40.0)]
    pub technical_weight: f64,
    #[arg(long, default_value_t = 35.0)]
    pub news_weight: f64,
    #[arg(long, default_value_t = 8.0)]
    pub youtube_weight: f64,
    #[arg(long, default_value_t = 9.0)]
    pub reddit_weight: f64,
    #[arg(long, default_value_t = 8.0)]
    pub x_weight: f64,
    #[arg(long, default_value_t = 1)]
    pub from_phase: i64,
    #[arg(long, default_value_t = 7)]
    pub to_phase: i64,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub tech_refresh_enabled: bool,
    #[arg(long, default_value = "1d,2h,30min")]
    pub tech_refresh_intervals: String,
    #[arg(long, default_value_t = 120)]
    pub tech_refresh_save_bars: i64,
    #[arg(long)]
    pub tech_refresh_script_path: Option<PathBuf>,
    #[arg(long, default_value_t = 900)]
    pub tech_refresh_timeout_sec: u64,
    #[arg(long)]
    pub tech_refresh_python_bin: Option<PathBuf>,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub jin10_refresh_enabled: bool,
    #[arg(long, default_value_t = 24.0)]
    pub jin10_refresh_lookback_hours: f64,
    #[arg(long)]
    pub jin10_refresh_script_path: Option<PathBuf>,
    #[arg(long, default_value_t = 120)]
    pub jin10_refresh_timeout_sec: u64,
    #[arg(long)]
    pub mock: bool,
}

fn is_mock(state: &Value) -> bool {
    state.get("mock").and_then(Value::as_bool).unwrap_or(false)
}

pub async fn run(args: ExecArgs) -> Result<Value> {
    validate_args(&args)?;
    debug!(
        ticker = %args.ticker,
        mode = args.mode.as_str(),
        mock = args.mock,
        from_phase = args.from_phase,
        to_phase = args.to_phase,
        "orchestrator exec starting"
    );
    let tickers = parse_tickers(&args.ticker);
    if tickers.is_empty() {
        bail!("ticker is required");
    }
    let ticker = display_ticker(&tickers);
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
    let runtime_config = RuntimeConfig::from_value(&config)?;
    let run_dir = resolve_run_dir(&args, &tickers, &date, &config);
    fs::create_dir_all(&run_dir)
        .with_context(|| format!("failed to create run dir {}", run_dir.display()))?;
    let db_path = resolve_db_path(&args, &config);
    let mut conn = connect(&db_path)?;
    let run_id = run_id_for(&tickers, &date, &run_dir);
    let state_path = run_dir.join("state.json");
    let phase1_agents_raw = if args.phase1_agents == DEFAULT_PHASE1_AGENTS {
        config_str(&config, "orchestrator.phase1_agents", DEFAULT_PHASE1_AGENTS)
    } else {
        args.phase1_agents.clone()
    };
    let phase1_agents = parse_phase1_agents(&phase1_agents_raw)?;
    let model_override = args.model.clone().filter(|value| !value.is_empty());
    let reasoning_effort_override = args
        .reasoning_effort
        .clone()
        .filter(|value| !value.trim().is_empty());
    debug!(
        run_id,
        ticker,
        date,
        run_dir = %run_dir.display(),
        db_path = %db_path.display(),
        config_path = %config_path.display(),
        "orchestrator exec resolved runtime paths"
    );

    let mut state = json!({
        "run_id": run_id,
        "ticker": ticker,
        "tickers": tickers,
        "current_date": date,
        "lang": if args.lang == "zh" { config_str(&config, "orchestrator.runtime.lang", "zh") } else { args.lang.clone() },
        "mode": args.mode.as_str(),
        "window_days": if args.window_days == 150 { config_int(&config, "orchestrator.runtime.window_days", 150) } else { args.window_days },
        "run_dir": run_dir,
        "db_path": db_path,
        "phase_status": {},
        "phase1_agents": phase1_agents,
        "tech_refresh_enabled": args.tech_refresh_enabled,
        "analyst_weights": {
            "analyst.technical": config_weight(&config, "technical", args.technical_weight),
            "analyst.news_macro": config_weight(&config, "news_macro", args.news_weight),
            "analyst.youtube": config_weight(&config, "youtube", args.youtube_weight),
            "analyst.reddit": config_weight(&config, "reddit", args.reddit_weight),
            "analyst.x": config_weight(&config, "x", args.x_weight)
        },
        "degraded": false
    });
    state["mock"] = Value::Bool(args.mock);
    write_run_record(
        &mut conn,
        &RunRecordInput {
            run_id: state["run_id"].as_str().unwrap(),
            current_date: &date,
        },
    )?;
    if !args.mock && runtime_config.strict_sqlite {
        debug!(
            required_contexts = ?runtime_config.required_contexts,
            "validating strict sqlite contexts"
        );
        validate_sqlite_context(&conn, &runtime_config)?;
    }

    if args.from_phase <= 1 && args.to_phase >= 1 {
        debug!(roles = ?phase1_agents, "phase 1 starting");
        let phase_timer = start_phase_timer(1, "phase1");
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
        debug!("phase 1 completed");
    }
    if args.from_phase <= 2 && args.to_phase >= 2 {
        debug!(
            max_debate_rounds = args.max_debate_rounds,
            "phase 2 starting"
        );
        let phase_timer = start_phase_timer(2, "phase2");
        conn = run_phase2(
            conn,
            &mut state,
            model_override.as_deref(),
            reasoning_effort_override.as_deref(),
            if args.max_debate_rounds == 5 {
                config_int(&config, "orchestrator.runtime.max_debate_rounds", 5)
            } else {
                args.max_debate_rounds
            },
            if args.max_topics_per_side == 10 {
                config_int(&config, "orchestrator.runtime.max_topics_per_side", 10)
            } else {
                args.max_topics_per_side
            },
            &runtime_config,
        )
        .await?;
        set_phase_status(&mut state, 2, "done");
        set_phase_status(&mut state, PHASE2_REDUCER, "done");
        record_phase_elapsed(&mut state, phase_timer);
        debug!("phase 2 completed");
    }
    if args.from_phase <= 3 && args.to_phase >= 3 {
        debug!("phase 3 starting");
        let phase_timer = start_phase_timer(3, "phase3");
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
        debug!("phase 3 completed");
    }
    let policy = if state.get("research_plan").is_some() {
        Some(apply_workflow_policy(&mut state, &conn, &runtime_config))
    } else {
        None
    };
    if args.from_phase <= 4 && args.to_phase >= 4 {
        debug!("phase 4 (trader) starting");
        let phase_timer = start_phase_timer(4, "phase4");
        if should_run_llm_trader(policy.as_ref(), &runtime_config) {
            run_phase4(
                &mut conn,
                &mut state,
                model_override.as_deref(),
                reasoning_effort_override.as_deref(),
                &runtime_config,
            )
            .await?;
        } else {
            run_phase4_rust_rule(&mut conn, &mut state)?;
        }
        set_phase_status(&mut state, 4, "done");
        record_phase_elapsed(&mut state, phase_timer);
        debug!("phase 4 (trader) completed");
    }
    if args.from_phase <= 5 && args.to_phase >= 5 {
        debug!("phase 5 (risk debate) starting");
        let phase_timer = start_phase_timer(5, "phase5");
        if should_run_risk_review(policy.as_ref(), &runtime_config) {
            run_phase5(
                &mut conn,
                &mut state,
                model_override.as_deref(),
                reasoning_effort_override.as_deref(),
                &runtime_config,
            )
            .await?;
        } else {
            run_phase5_skipped(&mut conn, &mut state)?;
        }
        set_phase_status(&mut state, 5, "done");
        record_phase_elapsed(&mut state, phase_timer);
        debug!("phase 5 (risk debate) completed");
    }
    if args.from_phase <= 6 && args.to_phase >= 6 {
        debug!("phase 6 (portfolio manager) starting");
        let phase_timer = start_phase_timer(6, "phase6");
        if should_run_portfolio_review(policy.as_ref(), &runtime_config) {
            run_phase6(
                &mut conn,
                &mut state,
                model_override.as_deref(),
                reasoning_effort_override.as_deref(),
                &runtime_config,
            )
            .await?;
        } else {
            run_phase6_derived(&mut conn, &mut state)?;
        }
        set_phase_status(&mut state, 6, "done");
        record_phase_elapsed(&mut state, phase_timer);
        debug!("phase 6 (portfolio manager) completed");
    }
    if args.from_phase <= 7 && args.to_phase >= 7 {
        debug!("phase 7 (allocation) starting");
        let phase_timer = start_phase_timer(7, "phase7");
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
        debug!("phase 7 (allocation) completed");
    }

    record_contracts(&mut state);
    write_json(&state_path, &state)?;
    write_final_summary(&run_dir, &state)?;
    debug!(
        state_path = %state_path.display(),
        final_summary = %run_dir.join("final_summary.md").display(),
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
        "final_summary": run_dir.join("final_summary.md"),
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
    });
    Ok(result)
}

fn validate_args(args: &ExecArgs) -> Result<()> {
    if args.max_debate_rounds < 1 {
        bail!("--max-debate-rounds must be >= 1");
    }
    if args.max_topics_per_side < 1 {
        bail!("--max-topics-per-side must be >= 1");
    }
    if args.from_phase < 1 || args.from_phase > 7 {
        bail!("--from-phase must be 1-7");
    }
    if args.to_phase < args.from_phase || args.to_phase > 7 {
        bail!("--to-phase must be between --from-phase and 7");
    }
    for (name, value) in [
        ("--technical-weight", args.technical_weight),
        ("--news-weight", args.news_weight),
        ("--youtube-weight", args.youtube_weight),
        ("--reddit-weight", args.reddit_weight),
        ("--x-weight", args.x_weight),
    ] {
        if value < 0.0 {
            bail!("{name} must be >= 0");
        }
    }
    Ok(())
}

fn parse_phase1_agents(raw: &str) -> Result<Vec<String>> {
    let registry = orchestrator_core::role_registry::AgentRegistry::builtin();
    registry
        .parse_role_list(raw)
        .map_err(|e| anyhow::anyhow!(e))
}

fn apply_workflow_policy(
    state: &mut Value,
    conn: &rusqlite::Connection,
    config: &RuntimeConfig,
) -> WorkflowPolicyDecision {
    let allocation_context = compute_allocation_context(state, conn, &config.allocation);
    state["allocation_context"] = allocation_context.clone();
    let signals = workflow_policy_signals(state, &allocation_context);
    let decision = evaluate_workflow_policy(
        config.workflow.policy_mode,
        3,
        &signals,
        &config.workflow.policy_thresholds,
    );
    record_workflow_policy(state, &decision);
    decision
}

fn workflow_policy_signals(state: &Value, allocation_context: &Value) -> WorkflowPolicySignals {
    let research = state.get("research_plan").unwrap_or(&Value::Null);
    WorkflowPolicySignals {
        confidence: research_confidence(research),
        long_probability: research.get("long_probability").and_then(Value::as_f64),
        volatility: max_allocation_volatility(allocation_context),
        correlation: allocation_context
            .get("correlation_60d")
            .and_then(Value::as_f64),
        position_size: None,
        high_risk_flag: has_high_risk_flag(research),
        trade_research_conflict: false,
        high_impact_risk_constraint: false,
        force_portfolio_review: false,
    }
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

fn is_selective_policy(config: &RuntimeConfig) -> bool {
    config.workflow.policy_mode == WorkflowPolicyMode::Selective
}

fn should_run_llm_trader(policy: Option<&WorkflowPolicyDecision>, config: &RuntimeConfig) -> bool {
    !is_selective_policy(config) || policy.is_none_or(|decision| decision.need_trader)
}

fn should_run_risk_review(policy: Option<&WorkflowPolicyDecision>, config: &RuntimeConfig) -> bool {
    !is_selective_policy(config) || policy.is_none_or(|decision| decision.need_risk_review)
}

fn should_run_portfolio_review(
    policy: Option<&WorkflowPolicyDecision>,
    config: &RuntimeConfig,
) -> bool {
    !is_selective_policy(config) || policy.is_none_or(|decision| decision.need_portfolio_review)
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

fn resolve_run_dir(args: &ExecArgs, tickers: &[String], date: &str, config: &Value) -> PathBuf {
    if let Some(path) = &args.run_dir {
        return if path.is_absolute() {
            path.clone()
        } else {
            default_project_root().join(path)
        };
    }
    let slug = run_slug(tickers);
    let pattern = config_str(
        config,
        "orchestrator.run_dir_pattern",
        "outputs/{dir_slug}/{date}_exec",
    );
    let path = pattern
        .replace("{dir_slug}", &slug)
        .replace("{dir_slug_lower}", &slug.to_ascii_lowercase())
        .replace("{date}", date);
    project_path(path)
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
        record_role_job_metrics(state, &result);
        let artifact = role_artifact_or_degraded(state, config, result)?;
        persist_artifact(conn, state, 1, &role, artifact.clone())?;
        reports.insert(role.clone(), artifact);
    }
    state["analyst_reports"] = Value::Object(reports);
    run_phase1_reducer(
        conn,
        state,
        model_override,
        reasoning_effort_override,
        config,
    )
    .await?;
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
    let _mock = is_mock(state);
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
    debug!(topic_count = topics.len(), "phase 2 topics generated");
    state["debate_turns"] = json!([]);

    let db_path = state
        .get("db_path")
        .and_then(Value::as_str)
        .map(|s| s.to_string())
        .context("db_path missing from state")?;

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
        "phase1_state_artifact": state.get("phase1_state_artifact").cloned().unwrap_or(Value::Null),
        "phase1_brief_md": state.get("phase1_brief_md").cloned().unwrap_or(Value::Null),
        "late_evidence": state.get("late_evidence").cloned().unwrap_or_else(|| json!([])),
        "degraded": state.get("degraded").cloned().unwrap_or(Value::Null),
    });
    let model_override_owned = model_override.map(|s| s.to_string());
    let reasoning_effort_override_owned = reasoning_effort_override.map(|s| s.to_string());

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

    for result in results {
        let (topic_id, turns, topic_state, role_metrics) = result?;
        merge_role_job_metrics(state, &role_metrics);
        // Merge turns into global state
        if let Some(turns_arr) = state["debate_turns"].as_array_mut() {
            turns_arr.extend(turns);
        }
        // Merge topic state into global state
        upsert_topic_debate_state(state, &topic_id, topic_state);
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
    let initial_topic_state = json!({
        "topic": topic.clone(),
        "mode": "steer_room",
        "turns": [],
        "controller_artifacts": [],
        "thread": sessions
    });
    upsert_topic_debate_state(&mut local_state, &topic_id, initial_topic_state);
    let mut turns = Vec::new();

    let bull_seed = run_topic_steer_step(
        &mut conn,
        &mut local_state,
        "researcher.bull.initial",
        "bull_seed",
        1,
        &topic_id,
        &sessions,
        None,
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
        None,
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
            &json!({"bull_seed": bull_seed, "bear_seed": bear_seed}),
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

    for round in 2..=max_debate_rounds.max(2) {
        let bull_steer = steer_for_role(&mediator_output, "bull")
            .unwrap_or_else(|| steer_payload("respond_to_mediator", &mediator_output));
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
        let bear_steer = steer_for_role(&mediator_output, "bear")
            .unwrap_or_else(|| steer_payload("respond_to_mediator", &mediator_output));
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
                &json!({"bull_packet": bull_rebuttal, "bear_packet": bear_rebuttal}),
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
            "turn_id": format!("turn-{topic_id}-bull")
        },
        "bear": {
            "session_id": format!("{run_id}:phase2:{topic_id}:bear"),
            "turn_id": format!("turn-{topic_id}-bear")
        },
        "mediator": {
            "session_id": format!("{run_id}:phase2:{topic_id}:mediator"),
            "turn_id": format!("turn-{topic_id}-mediator")
        }
    })
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
            turn_id: session
                .get("turn_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            steer,
        },
        if role == "mediator.topic_controller" {
            config.workflow.reducer_timeout_sec
        } else {
            config.workflow.agent_timeout_sec
        },
        config,
        state,
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

fn steer_for_role(controller_turn: &Value, side: &str) -> Option<String> {
    let artifact = controller_turn.get("artifact").unwrap_or(controller_turn);
    let keys = match side {
        "bull" => ["bull", "researcher.bull.interaction", "to_bull"],
        _ => ["bear", "researcher.bear.interaction", "to_bear"],
    };
    artifact
        .get("next_steers")
        .and_then(Value::as_object)
        .and_then(|object| keys.iter().find_map(|key| object.get(*key).cloned()))
        .map(|value| steer_payload("mediator_instruction", &value))
}

async fn run_phase2_topic_generation(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<Vec<Value>> {
    let mock = is_mock(state);
    let base = build_topic_generation_artifact(state);
    state["topic_generation_artifact"] = base.clone();
    debug!("phase 2 topic generation role starting");
    let output = run_single_role_job(
        RoleRun {
            state: state.clone(),
            role: "mediator.topic",
            phase: 2,
            kind: "topic_generation",
            round: None,
            topic_id: None,
            mock,
            model_override,
            reasoning_effort_override,
            config,
            prompt_path: config
                .prompts
                .path_for("mediator.topic")
                .map(|p| p.as_path()),
        },
        config.workflow.reducer_timeout_sec,
        config,
        state,
    )
    .await?;
    let artifact = merge_reducer_output(base, output);
    let topics = topics_from_generation_artifact(&artifact);
    debug!(
        topic_count = topics.len(),
        "phase 2 topic generation role completed"
    );
    state["topic_generation_artifact"] = artifact.clone();
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
    Ok(topics)
}

async fn run_phase1_reducer(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    _model_override: Option<&str>,
    _reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<()> {
    let artifact = build_phase1_state_artifact(state, config);
    let brief = reducer_brief_md(&artifact);
    state["phase1_state_artifact"] = artifact.clone();
    state["phase1_brief_md"] = Value::String(brief.clone());
    persist_artifact_with_last_md(conn, state, 15, "reducer.evidence", artifact, brief)?;
    set_phase_status(state, 15, "done");
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
        artifact,
        brief,
    )?;
    set_phase_status(state, PHASE2_REDUCER, "done");
    Ok(())
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
    let artifact = run_single_role_job(
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
            prompt_path: Some(config.prompts.manager_research.as_path()),
        },
        config.workflow.agent_timeout_sec,
        config,
        state,
    )
    .await
    .unwrap_or_else(|error| manager_research_fallback(state, error));
    let artifact = if artifact
        .get("degraded")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        manager_research_fallback(
            state,
            anyhow::anyhow!(
                "{}",
                artifact
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("manager.research degraded")
            ),
        )
    } else {
        artifact
    };
    persist_artifact(conn, state, 3, "manager.research", artifact.clone())?;
    state["research_plan"] = artifact;
    debug!("manager research role completed");
    Ok(())
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
            prompt_path: Some(config.prompts.trader.as_path()),
        },
        config.workflow.agent_timeout_sec,
        config,
        state,
    )
    .await?;
    sanitize_downstream_constraints(state, "trader_investment_plan", &mut artifact);
    persist_artifact(conn, state, 4, "trader", artifact.clone())?;
    state["trader_investment_plan"] = artifact;
    Ok(())
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
    for round in 1..=config.workflow.risk_rounds {
        for (role, prompt_path) in [
            ("risk.aggressive", config.prompts.risk_aggressive.as_path()),
            (
                "risk.conservative",
                config.prompts.risk_conservative.as_path(),
            ),
            ("risk.neutral", config.prompts.risk_neutral.as_path()),
        ] {
            let mut artifact = run_single_role_job(
                RoleRun {
                    state: state.clone(),
                    role,
                    phase: 5,
                    kind: "risk_argument",
                    round: Some(round),
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
            )
            .await?;
            sanitize_downstream_constraints(state, role, &mut artifact);
            let turn = json!({
                "role": role,
                "phase": 5,
                "kind": "risk_argument",
                "round": round,
                "artifact": artifact
            });
            if let Some(history) = state["risk_debate_state"]["history"].as_array_mut() {
                history.push(turn.clone());
            }
            persist_message(conn, state, 5, role, "risk_argument", Some(round), turn)?;
        }
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
            prompt_path: Some(config.prompts.portfolio_manager.as_path()),
        },
        config.workflow.agent_timeout_sec,
        config,
        state,
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
async fn run_phase7(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<()> {
    let mock = is_mock(state);
    debug!("allocation context computation starting");
    let context = compute_allocation_context(state, conn, &config.allocation);
    state["allocation_context"] = context.clone();
    debug!(vix_regime = ?context.get("vix").and_then(|v| v.get("regime")), "allocation context ready");

    let raw_artifact = run_single_role_job(
        RoleRun {
            state: state.clone(),
            role: "allocation.manager",
            phase: 7,
            kind: "artifact",
            round: None,
            topic_id: None,
            mock,
            model_override,
            reasoning_effort_override,
            config,
            prompt_path: Some(config.prompts.allocation_manager.as_path()),
        },
        config.workflow.agent_timeout_sec,
        config,
        state,
    )
    .await?;
    let mut allocation = normalize_allocation(&raw_artifact, &context, &config.allocation);
    sanitize_downstream_constraints(state, "portfolio_allocation", &mut allocation);
    persist_artifact(conn, state, 7, "allocation.manager", allocation.clone())?;
    state["portfolio_allocation"] = allocation;
    debug!("allocation manager role completed");
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
    fn llm_roles_inherit_global_defaults_and_expand_all_tools() {
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
        assert_eq!(settings.max_turns, None);
        assert_eq!(settings.reasoning_effort.as_deref(), Some("medium"));
        assert!(settings.native_web_search);
        assert!(settings.tools.contains(&"read_run_context".to_string()));
        assert!(settings
            .tools
            .contains(&"run_technical_indicators".to_string()));
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
                "tools": ["read_run_context"]
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
        assert_eq!(settings.tools, vec!["read_run_context".to_string()]);
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
    fn web_search_defaults_to_disabled_mock_medium_limit() {
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

        for config in web_search.values() {
            assert_eq!(
                config,
                &orchestrator_llm::web_search::WebSearchConfig::default()
            );
            assert_eq!(config.mode, WebSearchMode::Disabled);
            assert_eq!(config.provider, WebSearchProviderKind::Mock);
            assert_eq!(config.context_size, WebSearchContextSize::Medium);
            assert_eq!(config.max_result_chars, 12_000);
        }
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
        assert_eq!(
            web_search["analyst.news_macro"].mode,
            WebSearchMode::Disabled
        );
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
        let err = parse_phase1_agents("technical,news,fundamental,youtube,reddit,x").unwrap_err();

        assert!(err.to_string().contains("fundamental analyst was removed"));
    }

    #[test]
    fn parse_phase1_agents_normalizes_supported_roles() {
        let roles = parse_phase1_agents("technical,news,youtube,reddit,x").unwrap();

        assert_eq!(
            roles,
            vec![
                "analyst.technical",
                "analyst.news_macro",
                "analyst.youtube",
                "analyst.reddit",
                "analyst.x"
            ]
        );
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
            "run_technical_indicators",
            Err(anyhow::anyhow!("missing technical data")),
        );

        assert_eq!(state["degraded"], true);
        assert_eq!(
            state["preflight"]["run_technical_indicators"]["status"],
            "error"
        );
        assert!(state["preflight"]["run_technical_indicators"]["message"]
            .as_str()
            .unwrap()
            .contains("missing technical data"));
    }

    #[tokio::test]
    async fn technical_preflight_can_be_skipped() {
        let mut state = json!({"degraded": false, "tech_refresh_enabled": false});

        crate::orchestration::preflight::run_technical_preflight(&mut state)
            .await
            .unwrap();

        assert_eq!(state["degraded"], false);
        assert_eq!(
            state["preflight"]["run_technical_indicators"]["status"],
            "skipped"
        );
    }
}
