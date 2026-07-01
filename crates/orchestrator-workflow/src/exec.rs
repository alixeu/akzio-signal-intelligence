use anyhow::{bail, Context, Result};
use chrono::{Local, NaiveDate};
use clap::{Args, ValueEnum};
use orchestrator_core::{
    config_int, config_str, default_project_root, display_ticker, load_config, parse_tickers,
    project_path, run_slug,
};
use orchestrator_sql;
use orchestrator_sql::{apply_memory_update_proposal, connect, write_run_record, RunRecordInput};
use serde_json::{json, Value};
use std::{fs, path::PathBuf};
use tracing::{debug, warn};

use crate::orchestration::artifact::{
    build_debate_state_artifact, build_phase1_state_artifact, build_topic_controller_artifact,
    build_topic_generation_artifact, merge_reducer_output, persist_artifact,
    persist_artifact_with_last_md, persist_message, persist_message_with_topic,
    phase1_reducer_fallback, reducer_brief_md, topic_id_from_topic,
    topics_from_generation_artifact,
};
use crate::orchestration::config::{config_weight, validate_sqlite_context, RuntimeConfig};
use crate::orchestration::degraded::{manager_research_fallback, role_artifact_or_degraded};
use crate::orchestration::memory::{
    record_memory_reflector_status, validate_memory_update_proposal,
};
use crate::orchestration::preflight::{enforce_preflight_policy, run_phase1_preflight};
use crate::orchestration::render::mode_prompt_path;
use crate::orchestration::role_jobs::{
    prepare_role_job, run_role_job_with_timeout, run_role_jobs, run_single_role_job, RoleRun,
};
use crate::orchestration::state::{
    append_topic_controller_artifact, append_topic_turn, run_id_for, set_phase_status,
    set_topic_controller_state, tickers_from_state, upsert_topic_debate_state, write_final_summary,
    write_json,
};
use orchestrator_core::role_registry::{DEFAULT_PHASE1_AGENTS, MEMORY_REFLECTOR_ROLE};
use orchestrator_domain::Phase;

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
    #[arg(long, default_value_t = 3)]
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
        debug!("phase 1 completed");
    }
    if args.from_phase <= 2 && args.to_phase >= 2 {
        debug!(
            max_debate_rounds = args.max_debate_rounds,
            "phase 2 starting"
        );
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
        set_phase_status(&mut state, Phase::Phase2Reducer.as_i64(), "done");
        debug!("phase 2 completed");
    }
    if args.from_phase <= 3 && args.to_phase >= 3 {
        debug!("phase 3 starting");
        run_phase3(
            &mut conn,
            &mut state,
            model_override.as_deref(),
            reasoning_effort_override.as_deref(),
            &runtime_config,
        )
        .await?;
        set_phase_status(&mut state, 3, "done");
        debug!("phase 3 completed");
    }

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
        "rating": research.get("rating").cloned().unwrap_or(Value::Null),
        "action": Value::Null,
        "research_rating": research.get("rating").cloned().unwrap_or(Value::Null),
        "long_probability": research.get("long_probability").cloned().unwrap_or(Value::Null),
        "short_probability": research.get("short_probability").cloned().unwrap_or(Value::Null),
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
    if args.from_phase < 1 || args.from_phase > 3 {
        bail!("--from-phase must be 1, 2, or 3");
    }
    if args.to_phase < args.from_phase || args.to_phase > 3 {
        bail!("--to-phase must be between --from-phase and 3");
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

    let results: Vec<Result<(String, Vec<Value>, Value)>> =
        futures::future::join_all(topic_futures).await;

    for result in results {
        let (topic_id, turns, topic_state) = result?;
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
) -> Result<(String, Vec<Value>, Value)> {
    let topic_id = topic_id_from_topic(&topic);
    debug!(topic_id, "phase 2 topic debate starting (parallel)");

    let model_override_ref = model_override.as_deref();
    let reasoning_effort_ref = reasoning_effort_override.as_deref();

    // Initialize local mutable state for this topic
    let mut local_state = state.clone();
    local_state["debate_turns"] = json!([]);
    let initial_topic_state = json!({
        "topic": topic,
        "turns": [],
        "controller_artifacts": []
    });
    upsert_topic_debate_state(&mut local_state, &topic_id, initial_topic_state);

    let mut turns = Vec::new();

    // ── Round 1: bull_initial + bear_initial (sequential) ──
    {
        let mut bull_state = local_state.clone();
        let mut bear_state = local_state.clone();

        let bull_opening = run_topic_debate_step(
            &mut conn,
            &mut bull_state,
            "researcher.bull.initial",
            "analysis_initial",
            1,
            &topic_id,
            model_override_ref,
            reasoning_effort_ref,
            config,
            mode_prompt_path(
                config.prompts.path_for("researcher.bull.initial").unwrap(),
                state,
            ),
        )
        .await?;

        let bear_opening = run_topic_debate_step(
            &mut conn,
            &mut bear_state,
            "researcher.bear.initial",
            "analysis_initial",
            1,
            &topic_id,
            model_override_ref,
            reasoning_effort_ref,
            config,
            mode_prompt_path(
                config.prompts.path_for("researcher.bear.initial").unwrap(),
                state,
            ),
        )
        .await?;

        // Merge: bull's state is the base; add bear's turns and degradation
        if let Some(src_turns) = bear_state
            .get("topic_debate_states")
            .and_then(|s| s.get(&*topic_id))
            .and_then(|t| t.get("turns"))
            .and_then(|t| t.as_array())
        {
            if !bull_state
                .get("topic_debate_states")
                .is_some_and(Value::is_object)
            {
                bull_state["topic_debate_states"] = json!({});
            }
            let entry = bull_state["topic_debate_states"]
                .as_object_mut()
                .unwrap()
                .entry(topic_id.to_string())
                .or_insert_with(|| {
                    json!({
                        "topic": {"topic_id": topic_id.clone(), "topic": topic_id.clone(), "tickers": []},
                        "turns": [],
                        "controller_artifacts": []
                    })
                });
            if !entry.get("turns").is_some_and(Value::is_array) {
                entry["turns"] = json!([]);
            }
            if let Some(tgt_turns) = entry["turns"].as_array_mut() {
                tgt_turns.extend(src_turns.iter().cloned());
            }
        }
        if let Some(src_degraded) = bear_state.get("degraded").and_then(|v| v.as_object()) {
            if !bull_state.get("degraded").is_some_and(Value::is_object) {
                bull_state["degraded"] = json!({});
            }
            if let Some(tgt_degraded) = bull_state["degraded"].as_object_mut() {
                for (k, v) in src_degraded {
                    tgt_degraded.insert(k.clone(), v.clone());
                }
            }
        }
        local_state = bull_state;
        turns.push(bull_opening);
        turns.push(bear_opening);
    }

    // Controllers after openings (sequential)
    run_topic_controller(
        &mut conn,
        &mut local_state,
        &topic_id,
        "after_bull_opening",
        1,
        model_override_ref,
        reasoning_effort_ref,
        config,
    )
    .await?;
    run_topic_controller(
        &mut conn,
        &mut local_state,
        &topic_id,
        "after_bear_opening",
        1,
        model_override_ref,
        reasoning_effort_ref,
        config,
    )
    .await?;

    // ── Rounds 2+ (sequential) ──
    for round in 2..=max_debate_rounds {
        debug!(
            topic_id,
            round, "phase 2 debate round starting (parallel topic)"
        );
        let bull_rebuttal = run_topic_debate_step(
            &mut conn,
            &mut local_state,
            "researcher.bull.interaction",
            "interaction_research",
            round,
            &topic_id,
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
        turns.push(bull_rebuttal);
        run_topic_controller(
            &mut conn,
            &mut local_state,
            &topic_id,
            "after_bull_rebuttal",
            round,
            model_override_ref,
            reasoning_effort_ref,
            config,
        )
        .await?;

        let bear_rebuttal = run_topic_debate_step(
            &mut conn,
            &mut local_state,
            "researcher.bear.interaction",
            "interaction_research",
            round,
            &topic_id,
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
        turns.push(bear_rebuttal);
        run_topic_controller(
            &mut conn,
            &mut local_state,
            &topic_id,
            "after_bear_rebuttal",
            round,
            model_override_ref,
            reasoning_effort_ref,
            config,
        )
        .await?;
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

    Ok((topic_id, turns, topic_state))
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

#[allow(clippy::too_many_arguments)]
async fn run_topic_debate_step(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    role: &str,
    kind: &str,
    round: i64,
    topic_id: &str,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
    prompt_path: PathBuf,
) -> Result<Value> {
    let mock = is_mock(state);
    let artifact = run_single_role_job(
        RoleRun {
            state: state.clone(),
            role,
            phase: 2,
            kind,
            round: Some(round),
            topic_id: Some(topic_id),
            mock,
            model_override,
            reasoning_effort_override,
            config,
            prompt_path: Some(prompt_path.as_path()),
        },
        config.workflow.agent_timeout_sec,
        config,
        state,
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
        "artifact": artifact
    });
    append_topic_turn(state, topic_id, turn.clone());
    Ok(turn)
}

#[allow(clippy::too_many_arguments)]
async fn run_topic_controller(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    topic_id: &str,
    checkpoint: &str,
    round: i64,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<()> {
    let mock = is_mock(state);
    let base = build_topic_controller_artifact(state, topic_id, checkpoint, round, config);
    set_topic_controller_state(state, topic_id, base.clone());
    let output = run_single_role_job(
        RoleRun {
            state: state.clone(),
            role: "mediator.topic_controller",
            phase: Phase::Phase2Reducer.as_i64(),
            kind: checkpoint,
            round: Some(round),
            topic_id: Some(topic_id),
            mock,
            model_override,
            reasoning_effort_override,
            config,
            prompt_path: config
                .prompts
                .path_for("mediator.topic_controller")
                .map(|p| p.as_path()),
        },
        config.workflow.reducer_timeout_sec,
        config,
        state,
    )
    .await?;
    let artifact = merge_reducer_output(base, output);
    set_topic_controller_state(state, topic_id, artifact.clone());
    append_topic_controller_artifact(state, topic_id, artifact.clone());
    persist_message_with_topic(
        conn,
        state,
        25,
        "mediator.topic_controller",
        checkpoint,
        Some(round),
        Some(topic_id),
        artifact,
    )?;
    Ok(())
}

async fn run_phase1_reducer(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<()> {
    let mock = is_mock(state);
    let base = build_phase1_state_artifact(state, config);
    state["phase1_state_artifact"] = base.clone();
    let reducer_result = run_single_role_job(
        RoleRun {
            state: state.clone(),
            role: "reducer.evidence",
            phase: 15,
            kind: "artifact",
            round: None,
            topic_id: None,
            mock,
            model_override,
            reasoning_effort_override,
            config,
            prompt_path: config
                .prompts
                .path_for("reducer.evidence")
                .map(|p| p.as_path()),
        },
        config.workflow.reducer_timeout_sec,
        config,
        state,
    )
    .await;
    let artifact = match reducer_result {
        Ok(reducer_output) => merge_reducer_output(base, reducer_output),
        Err(error) => phase1_reducer_fallback(base, error),
    };
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
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<()> {
    let mock = is_mock(state);
    let base = build_debate_state_artifact(state, config);
    state["debate_state_artifact"] = base.clone();
    let reducer_output = run_single_role_job(
        RoleRun {
            state: state.clone(),
            role: "reducer.debate_final",
            phase: Phase::Phase2Reducer.as_i64(),
            kind: "final_artifact",
            round: None,
            topic_id: None,
            mock,
            model_override,
            reasoning_effort_override,
            config,
            prompt_path: config
                .prompts
                .path_for("reducer.debate_final")
                .map(|p| p.as_path()),
        },
        config.workflow.reducer_timeout_sec,
        config,
        state,
    )
    .await?;
    let artifact = merge_reducer_output(base, reducer_output);
    let brief = reducer_brief_md(&artifact);
    state["debate_state_artifact"] = artifact.clone();
    state["debate_brief_md"] = Value::String(brief.clone());
    persist_artifact_with_last_md(
        conn,
        state,
        Phase::Phase2Reducer.as_i64(),
        "reducer.debate_final",
        artifact,
        brief,
    )?;
    set_phase_status(state, Phase::Phase2Reducer.as_i64(), "done");
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
    run_memory_reflector_after_phase3(
        conn,
        state,
        model_override,
        reasoning_effort_override,
        config,
    )
    .await?;
    Ok(())
}

async fn run_memory_reflector_after_phase3(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<()> {
    let mock = is_mock(state);
    let Some(prompt_path) = config.prompts.memory_reflector.as_deref() else {
        debug!("memory reflector skipped: prompt not configured");
        record_memory_reflector_status(
            state,
            "skipped",
            "optional prompt orchestrator.prompts.meta.memory_reflector is not configured",
        );
        return Ok(());
    };
    if !config.llm_roles.contains_key(MEMORY_REFLECTOR_ROLE) {
        debug!("memory reflector skipped: role not configured");
        record_memory_reflector_status(
            state,
            "skipped",
            "optional role meta.memory_reflector is not configured",
        );
        return Ok(());
    }
    if mock {
        debug!("memory reflector skipped: mock run");
        record_memory_reflector_status(state, "skipped", "mock run");
        return Ok(());
    }

    debug!("memory reflector role starting");
    let job = match prepare_role_job(RoleRun {
        state: state.clone(),
        role: MEMORY_REFLECTOR_ROLE,
        phase: Phase::Phase3MemoryReflector.as_i64(),
        kind: "memory_update_proposal",
        round: None,
        topic_id: None,
        mock,
        model_override,
        reasoning_effort_override,
        config,
        prompt_path: Some(prompt_path),
    }) {
        Ok(job) => job,
        Err(error) => {
            record_memory_reflector_status(
                state,
                "failed",
                &format!("failed to prepare reflector role: {error}"),
            );
            return Ok(());
        }
    };

    let result = run_role_job_with_timeout(job, config.workflow.reducer_timeout_sec).await;
    let Some(artifact) = result.artifact else {
        let message = result
            .error
            .unwrap_or_else(|| "memory reflector role failed".to_string());
        warn!(message, "memory reflector failed");
        record_memory_reflector_status(state, "failed", &message);
        return Ok(());
    };

    if let Err(error) = validate_memory_update_proposal(&artifact, &tickers_from_state(state)) {
        state["memory_update_proposal_rejected"] = artifact;
        record_memory_reflector_status(
            state,
            "invalid",
            &format!("MemoryUpdateProposal validation failed: {error}"),
        );
        return Ok(());
    }

    match persist_artifact(
        conn,
        state,
        Phase::Phase3MemoryReflector.as_i64(),
        MEMORY_REFLECTOR_ROLE,
        artifact.clone(),
    ) {
        Ok(()) => {
            state["memory_update_proposal"] = artifact.clone();
            match apply_memory_update_proposal(conn, &artifact) {
                Ok(result) => {
                    debug!(
                        applied = result.applied,
                        reused = result.reused,
                        memory_ids = ?result.memory_ids,
                        "memory update proposal applied"
                    );
                    state["memory_reflector"] = json!({
                        "status": "applied",
                        "role": MEMORY_REFLECTOR_ROLE,
                        "phase": Phase::Phase3MemoryReflector.as_i64(),
                        "persisted_agent_message": true,
                        "applied": result.applied,
                        "reused": result.reused,
                        "memory_ids": result.memory_ids
                    });
                }
                Err(error) => {
                    warn!(%error, "memory update proposal apply failed");
                    record_memory_reflector_status(
                        state,
                        "apply_failed",
                        &format!("proposal persisted but memory apply failed: {error}"),
                    );
                }
            }
        }
        Err(error) => {
            state["memory_update_proposal"] = artifact;
            record_memory_reflector_status(
                state,
                "validated_not_persisted",
                &format!("proposal validated but agent_message persistence failed: {error}"),
            );
        }
    }

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

    #[test]
    fn memory_update_proposal_contract_accepts_valid_thesis_update() {
        let artifact = json!({
            "artifact_type": "MemoryUpdateProposal",
            "schema_version": 1,
            "source_role": "manager.research",
            "generated_at": "2026-06-18T09:30:00Z",
            "run_id": "run-1",
            "proposals": [{
                "update_type": "thesis",
                "ticker": "TQQQ",
                "scope": "ticker",
                "observed_at": "2026-06-18T09:00:00Z",
                "source_date": "2026-06-18",
                "expires_at": null,
                "confidence": 0.72,
                "summary": "Liquidity improved while breadth remained narrow.",
                "evidence_refs": [{
                    "source_type": "final_research",
                    "source_id": "manager.research",
                    "quote_or_fact": "Final research raised the liquidity caveat."
                }],
                "invalidation_conditions": ["Breadth weakens below the stated threshold."],
                "follow_up_checks": ["Check next session market breadth."],
                "thesis": {
                    "status": "update",
                    "prior_thesis_id": "thesis-previous"
                }
            }]
        });

        validate_memory_update_proposal(&artifact, &["TQQQ".to_string()]).unwrap();
    }

    #[test]
    fn memory_update_proposal_contract_requires_prior_thesis_for_updates() {
        let artifact = json!({
            "artifact_type": "MemoryUpdateProposal",
            "schema_version": 1,
            "source_role": "manager.research",
            "run_id": "run-1",
            "generated_at": "2026-06-18T09:30:00Z",
            "proposals": [{
                "update_type": "thesis",
                "ticker": "TQQQ",
                "scope": "ticker",
                "observed_at": "2026-06-18T09:00:00Z",
                "source_date": "2026-06-18",
                "expires_at": null,
                "confidence": 0.72,
                "summary": "Liquidity improved while breadth remained narrow.",
                "evidence_refs": [{
                    "source_type": "final_research",
                    "source_id": "manager.research",
                    "quote_or_fact": "Final research raised the liquidity caveat."
                }],
                "invalidation_conditions": ["Breadth weakens below the stated threshold."],
                "follow_up_checks": ["Check next session market breadth."],
                "thesis": {
                    "status": "update"
                }
            }]
        });

        let err = validate_memory_update_proposal(&artifact, &["TQQQ".to_string()]).unwrap_err();
        assert!(err.to_string().contains("prior_thesis_id"));
    }

    #[test]
    fn memory_update_proposal_apply_result_can_be_persisted_after_validation() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("memory.sqlite");
        let mut conn = connect(&db_path).unwrap();
        let artifact = json!({
            "artifact_type": "MemoryUpdateProposal",
            "schema_version": 1,
            "source_role": "manager.research",
            "run_id": "run-1",
            "generated_at": "2026-06-19T00:00:00Z",
            "proposals": [{
                "update_type": "observation",
                "ticker": "TQQQ",
                "scope": "ticker",
                "observed_at": "2026-06-19T00:00:00Z",
                "source_date": "2026-06-19",
                "expires_at": null,
                "confidence": 0.72,
                "summary": "Liquidity improved while breadth remained narrow.",
                "evidence_refs": [{
                    "source_type": "final_research",
                    "source_id": "manager.research",
                    "quote_or_fact": "Final research raised the liquidity caveat."
                }],
                "invalidation_conditions": ["Breadth weakens below the stated threshold."],
                "follow_up_checks": ["Check next session market breadth."]
            }]
        });
        validate_memory_update_proposal(&artifact, &["TQQQ".to_string()]).unwrap();

        let applied = apply_memory_update_proposal(&mut conn, &artifact).unwrap();

        assert_eq!(applied.applied, 1);
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM memory_items", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn runtime_config_accepts_optional_memory_reflector_role() {
        let temp = tempfile::tempdir().unwrap();
        let prompt_path = temp.path().join("memory_reflector.md");
        fs::write(&prompt_path, "Return JSON").unwrap();
        let prompt_value = prompt_path.display().to_string();
        let mut config = json!({
            "orchestrator": {
                "data_source": {
                    "strict_sqlite": true,
                    "required_contexts": ["technical"]
                },
                "prompts": {
                    "analyst": {
                        "technical": prompt_value.clone(),
                        "news_macro": prompt_value.clone(),
                        "youtube": prompt_value.clone(),
                        "reddit": prompt_value.clone(),
                        "x": prompt_value.clone()
                    },
                    "phase2": {
                        "topic_generation": prompt_value.clone(),
                        "topic_controller": prompt_value.clone(),
                        "bull_initial": prompt_value.clone(),
                        "bull_interaction": prompt_value.clone(),
                        "bear_initial": prompt_value.clone(),
                        "bear_interaction": prompt_value.clone()
                    },
                    "reducers": {
                        "evidence": prompt_value.clone(),
                        "debate_final": prompt_value.clone()
                    },
                    "manager": {
                        "research": prompt_value.clone()
                    },
                    "meta": {
                        "memory_reflector": prompt_value
                    }
                },
                "llm": {
                    "roles": test_complete_llm_roles_config()
                }
            }
        });
        config["orchestrator"]["llm"]["roles"][MEMORY_REFLECTOR_ROLE] = serde_json::json!({
            "route": "responses",
            "model": "gpt-5.4",
            "base_url": "https://llm.example.com/v1",
            "api_key": "test-key",
            "max_turns": 4,
            "reasoning_effort": "medium",
            "think_tool": false,
            "tools": ["read_run_context"]
        });

        let config = RuntimeConfig::from_value(&config).unwrap();
        assert_eq!(
            config.prompts.memory_reflector.as_deref(),
            Some(prompt_path.as_path())
        );
        assert!(config.llm_roles.contains_key(MEMORY_REFLECTOR_ROLE));
    }

    fn test_complete_llm_roles_config() -> Value {
        let mut roles = serde_json::Map::new();
        for role in crate::orchestration::config::required_llm_roles() {
            roles.insert(
                (*role).to_string(),
                serde_json::json!({
                    "route": "responses",
                    "model": "gpt-5.4",
                    "base_url": "https://llm.example.com/v1",
                    "api_key": "test-key",
                    "max_turns": 4,
                    "reasoning_effort": null,
                    "think_tool": false,
                    "tools": []
                }),
            );
        }
        Value::Object(roles)
    }
}
