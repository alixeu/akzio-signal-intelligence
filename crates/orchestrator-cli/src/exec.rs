use anyhow::{bail, Context, Result};
use chrono::{DateTime, Local, NaiveDate};
use clap::{Args, ValueEnum};
use futures::{stream, StreamExt};
use orchestrator_core::{
    config_bool, config_get, config_int, config_str, config_strings, default_project_root,
    display_ticker, load_config, parse_tickers, project_path, replace_placeholders, run_slug,
};
use orchestrator_llm::{
    mock_role_artifact, run_rig_agent_loop,
    tools::ExternalToolConfig,
    web_search::{validate_web_search_runtime_config, WebSearchConfig, WebSearchConfigOverride},
    OutputMode, RigSettings, RoleLlmSettings,
};
use orchestrator_sql::{
    connect, context_count, import_jin10_payload, write_agent_message_scoped, write_run_record,
    AgentMessageInput, RunRecordInput,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
    time::{Duration, Instant},
};
use tokio::time;
use tracing::{debug, warn};

const DEFAULT_PHASE1_AGENTS: &str = "technical,news,youtube,reddit,x";
const MEMORY_REFLECTOR_ROLE: &str = "meta.memory_reflector";
const MEMORY_REFLECTOR_PHASE: i64 = 35;

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
    #[arg(long, default_value_t = 0)]
    pub cleanup_days: i64,
    #[arg(long)]
    pub cleanup_old_runs: bool,
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
    pub wait_for_monitor_time: Option<Option<u64>>,
    #[arg(long)]
    pub monitor_probability_threshold: Option<f64>,
    #[arg(long)]
    pub monitor_reversal_threshold: Option<f64>,
    #[arg(long)]
    pub monitor_email_enabled: Option<bool>,
    #[arg(long)]
    pub mock: bool,
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
    let config = load_config(Some(&config_path)).unwrap_or_else(|_| json!({}));
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
    write_run_record(
        &mut conn,
        RunRecordInput {
            run_id: state["run_id"].as_str().unwrap(),
            ticker: &ticker,
            tickers: tickers_from_state(&state).as_slice(),
            current_date: &date,
            mode: args.mode.as_str(),
            run_dir: run_dir.to_str().unwrap_or_default(),
            db_path: db_path.to_str().unwrap_or_default(),
            config: &config,
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
            args.mock,
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
        run_phase2(
            &mut conn,
            &mut state,
            args.mock,
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
        set_phase_status(&mut state, 25, "done");
        debug!("phase 2 completed");
    }
    if args.from_phase <= 3 && args.to_phase >= 3 {
        debug!("phase 3 starting");
        run_phase3(
            &mut conn,
            &mut state,
            args.mock,
            model_override.as_deref(),
            reasoning_effort_override.as_deref(),
            &runtime_config,
        )
        .await?;
        set_phase_status(&mut state, 3, "done");
        debug!("phase 3 completed");
    }
    set_phase_status(&mut state, 4, "skipped");
    set_phase_status(&mut state, 5, "skipped");
    set_phase_status(&mut state, 6, "skipped");
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
        "monitor": if matches!(args.mode, Mode::Monitor) { Some(json!({})) } else { None },
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
        "cleanup": {"enabled": args.cleanup_days > 0, "deleted_runs": 0, "deleted_run_dirs": 0}
    });
    Ok(result)
}

#[derive(Debug, Clone)]
struct RuntimeConfig {
    llm_roles: BTreeMap<String, RoleLlmSettings>,
    web_search: BTreeMap<String, WebSearchConfig>,
    strict_sqlite: bool,
    required_contexts: Vec<String>,
    prompts: PromptConfig,
    workflow: WorkflowConfig,
}

#[derive(Debug, Clone)]
struct WorkflowConfig {
    phase1_parallelism: usize,
    agent_timeout_sec: u64,
    reducer_timeout_sec: u64,
    critical_roles: BTreeSet<String>,
    late_evidence_enabled: bool,
}

#[derive(Debug, Clone)]
struct PromptConfig {
    analyst_technical: PathBuf,
    analyst_news_macro: PathBuf,
    analyst_youtube: PathBuf,
    analyst_reddit: PathBuf,
    analyst_x: PathBuf,
    bull_initial: PathBuf,
    bull_interaction: PathBuf,
    bear_initial: PathBuf,
    bear_interaction: PathBuf,
    topic_generation: PathBuf,
    topic_controller: PathBuf,
    reducer_evidence: PathBuf,
    reducer_debate_final: PathBuf,
    manager_research: PathBuf,
    memory_reflector: Option<PathBuf>,
}

impl RuntimeConfig {
    fn from_value(config: &Value) -> Result<Self> {
        let prompts = PromptConfig {
            analyst_technical: prompt_path(
                config,
                "orchestrator.prompts.analyst.technical",
                "prompts/analysts/technical.md",
            )?,
            analyst_news_macro: prompt_path(
                config,
                "orchestrator.prompts.analyst.news_macro",
                "prompts/analysts/news_macro.md",
            )?,
            analyst_youtube: prompt_path(
                config,
                "orchestrator.prompts.analyst.youtube",
                "prompts/analysts/youtube.md",
            )?,
            analyst_reddit: prompt_path(
                config,
                "orchestrator.prompts.analyst.reddit",
                "prompts/analysts/reddit.md",
            )?,
            analyst_x: prompt_path(
                config,
                "orchestrator.prompts.analyst.x",
                "prompts/analysts/x.md",
            )?,
            bull_initial: prompt_path(
                config,
                "orchestrator.prompts.phase2.bull_initial",
                "prompts/researchers/bull_initial.md",
            )?,
            bull_interaction: prompt_path(
                config,
                "orchestrator.prompts.phase2.bull_interaction",
                "prompts/researchers/bull_interaction.md",
            )?,
            bear_initial: prompt_path(
                config,
                "orchestrator.prompts.phase2.bear_initial",
                "prompts/researchers/bear_initial.md",
            )?,
            bear_interaction: prompt_path(
                config,
                "orchestrator.prompts.phase2.bear_interaction",
                "prompts/researchers/bear_interaction.md",
            )?,
            topic_generation: prompt_path_any(
                config,
                &[
                    "orchestrator.prompts.phase2.topic_generation",
                    "orchestrator.prompts.mediator.topic",
                ],
                "prompts/mediators/topic_generation.md",
            )?,
            topic_controller: prompt_path_any(
                config,
                &[
                    "orchestrator.prompts.phase25.topic_controller",
                    "orchestrator.prompts.mediator.topic_controller",
                ],
                "prompts/mediators/topic_controller.md",
            )?,
            reducer_evidence: prompt_path_any(
                config,
                &[
                    "orchestrator.prompts.reducers.evidence",
                    "orchestrator.prompts.reducer.evidence",
                ],
                "prompts/reducers/evidence.md",
            )?,
            reducer_debate_final: prompt_path_any(
                config,
                &["orchestrator.prompts.reducers.debate_final"],
                "prompts/reducers/debate_final.md",
            )?,
            manager_research: prompt_path(
                config,
                "orchestrator.prompts.manager.research",
                "prompts/managers/research_manager.md",
            )?,
            memory_reflector: prompt_path_optional(
                config,
                "orchestrator.prompts.meta.memory_reflector",
            )?,
        };
        let llm_roles = llm_roles_from_config(config)?;
        let web_search = web_search_by_role_from_config(config, llm_roles.iter())?;
        let workflow = WorkflowConfig::from_value(config);
        Ok(Self {
            llm_roles,
            web_search,
            strict_sqlite: config_bool(config, "orchestrator.data_source.strict_sqlite", true),
            required_contexts: config_strings(
                config,
                "orchestrator.data_source.required_contexts",
                &["technical"],
            ),
            prompts,
            workflow,
        })
    }
}

fn llm_roles_from_config(config: &Value) -> Result<BTreeMap<String, RoleLlmSettings>> {
    let value = config_get(config, "orchestrator.llm.roles")
        .context("orchestrator.llm.roles is required")?;
    let object = value
        .as_object()
        .context("orchestrator.llm.roles must be a map")?;
    let defaults = config_get(config, "orchestrator.llm.defaults");
    let mut roles = BTreeMap::new();
    for (role, role_value) in object {
        let mut effective = defaults.cloned().unwrap_or_else(|| json!({}));
        orchestrator_core::deep_merge(&mut effective, role_value.clone());
        normalize_llm_role_tools(&mut effective, role)?;
        let settings: RoleLlmSettings = serde_json::from_value(effective)
            .with_context(|| format!("invalid LLM config for role {role:?}"))?;
        roles.insert(role.clone(), settings);
    }
    for role in required_llm_roles().iter().copied() {
        let settings = roles
            .get(role)
            .with_context(|| format!("missing LLM config for required role {role:?}"))?;
        settings.validate(role)?;
    }
    Ok(roles)
}

fn normalize_llm_role_tools(value: &mut Value, role: &str) -> Result<()> {
    let Some(object) = value.as_object_mut() else {
        return Ok(());
    };
    let Some(tools_value) = object.get_mut("tools") else {
        return Ok(());
    };
    match tools_value {
        Value::String(text) if text.trim().eq_ignore_ascii_case("all") => {
            *tools_value = Value::Array(
                orchestrator_llm::tools::tool_names()
                    .iter()
                    .map(|name| Value::String((*name).to_string()))
                    .collect(),
            );
            Ok(())
        }
        Value::String(text) => {
            let tools = text
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(|item| Value::String(item.to_string()))
                .collect::<Vec<_>>();
            *tools_value = Value::Array(tools);
            Ok(())
        }
        Value::Array(_) => Ok(()),
        Value::Null => {
            *tools_value = Value::Array(Vec::new());
            Ok(())
        }
        _ => bail!("orchestrator.llm.roles.{role}.tools must be a list, comma string, or all"),
    }
}

fn web_search_by_role_from_config<'a>(
    config: &Value,
    roles: impl Iterator<Item = (&'a String, &'a RoleLlmSettings)>,
) -> Result<BTreeMap<String, WebSearchConfig>> {
    let global = web_search_config_at_path(config, "orchestrator.web_search")?
        .unwrap_or_else(WebSearchConfig::default);
    let role_values = config_get(config, "orchestrator.llm.roles")
        .and_then(Value::as_object)
        .context("orchestrator.llm.roles must be a map")?;
    let mut web_search = BTreeMap::new();
    for (role, llm_settings) in roles {
        let role_path = format!("orchestrator.llm.roles.{role}.web_search");
        let role_override = if let Some(role_value) = role_values
            .get(role)
            .and_then(|value| value.get("web_search"))
        {
            Some(web_search_override_from_value(role_value, &role_path)?)
        } else {
            None
        };
        let effective = global.merge_override(role_override.as_ref());
        if !llm_settings.native_web_search {
            validate_web_search_runtime_config(&effective, role)?;
        }
        web_search.insert(role.clone(), effective);
    }
    Ok(web_search)
}

fn web_search_config_at_path(config: &Value, path: &str) -> Result<Option<WebSearchConfig>> {
    config_get(config, path)
        .map(|value| web_search_config_from_value(value, path))
        .transpose()
}

fn web_search_config_from_value(value: &Value, path: &str) -> Result<WebSearchConfig> {
    validate_web_search_config_value(value, path)?;
    serde_json::from_value(value.clone()).with_context(|| format!("invalid {path} config"))
}

fn web_search_override_from_value(value: &Value, path: &str) -> Result<WebSearchConfigOverride> {
    validate_web_search_config_value(value, path)?;
    serde_json::from_value(value.clone()).with_context(|| format!("invalid {path} config"))
}

fn validate_web_search_config_value(value: &Value, path: &str) -> Result<()> {
    let Some(object) = value.as_object() else {
        return Ok(());
    };
    validate_web_search_enum_field(object, path, "mode", &["disabled", "cached", "live"])?;
    validate_web_search_enum_field(
        object,
        path,
        "provider",
        &["tavily", "exa", "brave", "mock"],
    )?;
    validate_web_search_enum_field(object, path, "context_size", &["low", "medium", "high"])?;
    validate_web_search_enum_field(object, path, "contextSize", &["low", "medium", "high"])?;
    Ok(())
}

fn validate_web_search_enum_field(
    object: &serde_json::Map<String, Value>,
    path: &str,
    field: &str,
    allowed: &[&str],
) -> Result<()> {
    let Some(value) = object.get(field) else {
        return Ok(());
    };
    let Some(value) = value.as_str() else {
        return Ok(());
    };
    if allowed.contains(&value) {
        Ok(())
    } else {
        bail!("{path}.{field} must be one of {}", allowed.join(", "))
    }
}

fn required_llm_roles() -> &'static [&'static str] {
    &[
        "analyst.technical",
        "analyst.news_macro",
        "analyst.youtube",
        "analyst.reddit",
        "analyst.x",
        "researcher.bull.initial",
        "researcher.bear.initial",
        "researcher.bull.interaction",
        "researcher.bear.interaction",
        "mediator.topic",
        "mediator.topic_controller",
        "reducer.evidence",
        "reducer.debate_final",
        "manager.research",
    ]
}

impl WorkflowConfig {
    fn from_value(config: &Value) -> Self {
        let phase1_parallelism = config_int_any(
            config,
            &[
                "orchestrator.workflow.phase1.parallelism",
                "orchestrator.workflow.parallel.max_worker_concurrency",
            ],
            5,
        )
        .max(1) as usize;
        let agent_timeout_sec = config_int_any(
            config,
            &[
                "orchestrator.workflow.agent_timeout_sec",
                "orchestrator.workflow.timeouts.worker_sec",
            ],
            300,
        )
        .max(1) as u64;
        let reducer_timeout_sec = config_int_any(
            config,
            &[
                "orchestrator.workflow.reducer_timeout_sec",
                "orchestrator.workflow.timeouts.reducer_sec",
            ],
            300,
        )
        .max(1) as u64;
        let mut critical_roles = config_strings_any(
            config,
            &[
                "orchestrator.workflow.phase1.critical_roles",
                "orchestrator.workflow.critical_roles.phase1",
            ],
            &["analyst.technical", "analyst.news_macro"],
        )
        .into_iter()
        .map(|role| normalize_phase1_role_name(&role))
        .collect::<BTreeSet<_>>();
        critical_roles.extend(config_strings_any(
            config,
            &["orchestrator.workflow.critical_roles.reducers"],
            &["reducer.evidence", "reducer.debate_final"],
        ));
        let late_evidence_enabled =
            config_bool(config, "orchestrator.workflow.late_evidence.enabled", true);
        Self {
            phase1_parallelism,
            agent_timeout_sec,
            reducer_timeout_sec,
            critical_roles,
            late_evidence_enabled,
        }
    }
}

impl PromptConfig {
    fn analyst_path(&self, role: &str) -> Option<&std::path::Path> {
        match role {
            "analyst.technical" => Some(self.analyst_technical.as_path()),
            "analyst.news_macro" => Some(self.analyst_news_macro.as_path()),
            "analyst.youtube" => Some(self.analyst_youtube.as_path()),
            "analyst.reddit" => Some(self.analyst_reddit.as_path()),
            "analyst.x" => Some(self.analyst_x.as_path()),
            _ => None,
        }
    }
}

fn prompt_path(config: &Value, key: &str, default: &str) -> Result<PathBuf> {
    let path = project_path(config_str(config, key, default));
    if !path.exists() {
        bail!(
            "configured prompt path does not exist for {key}: {}",
            path.display()
        );
    }
    Ok(path)
}

fn prompt_path_optional(config: &Value, key: &str) -> Result<Option<PathBuf>> {
    let Some(value) = config_get(config, key) else {
        return Ok(None);
    };
    let Some(path_text) = value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        bail!("{key} must be a non-empty prompt path");
    };
    Ok(Some(project_path(path_text)))
}

fn prompt_path_any(config: &Value, keys: &[&str], default: &str) -> Result<PathBuf> {
    for key in keys {
        if let Some(value) = config_get(config, key).and_then(Value::as_str) {
            let path = project_path(value);
            if !path.exists() {
                bail!(
                    "configured prompt path does not exist for {key}: {}",
                    path.display()
                );
            }
            return Ok(path);
        }
    }
    let path = project_path(default);
    if !path.exists() {
        bail!(
            "configured prompt path does not exist for {}: {}",
            keys.first().copied().unwrap_or("prompt"),
            path.display()
        );
    }
    Ok(path)
}

fn mode_prompt_path(base: &std::path::Path, state: &Value) -> PathBuf {
    if state.get("mode").and_then(Value::as_str) != Some("monitor") {
        return base.to_path_buf();
    }
    let Some(stem) = base.file_stem().and_then(|value| value.to_str()) else {
        return base.to_path_buf();
    };
    let candidate = base.with_file_name(format!("{stem}_monitor.md"));
    if candidate.exists() {
        candidate
    } else {
        base.to_path_buf()
    }
}

fn config_int_any(config: &Value, keys: &[&str], default: i64) -> i64 {
    keys.iter()
        .find_map(|key| config_get(config, key).and_then(Value::as_i64))
        .unwrap_or(default)
}

fn config_strings_any(config: &Value, keys: &[&str], default: &[&str]) -> Vec<String> {
    keys.iter()
        .find_map(|key| {
            config_get(config, key).and_then(|value| {
                value.as_array().map(|items| {
                    items
                        .iter()
                        .filter_map(Value::as_str)
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                })
            })
        })
        .unwrap_or_else(|| default.iter().map(|value| (*value).to_string()).collect())
}

fn normalize_phase1_role_name(role: &str) -> String {
    match role.trim() {
        "technical" => "analyst.technical",
        "news" | "news_macro" => "analyst.news_macro",
        "youtube" => "analyst.youtube",
        "reddit" => "analyst.reddit",
        "x" => "analyst.x",
        other => other,
    }
    .to_string()
}

fn config_weight(config: &Value, name: &str, cli_value: f64) -> f64 {
    config_get(config, &format!("orchestrator.analyst_weights.{name}"))
        .and_then(|value| value.as_f64())
        .unwrap_or(cli_value)
}

fn validate_sqlite_context(conn: &rusqlite::Connection, config: &RuntimeConfig) -> Result<()> {
    for context in &config.required_contexts {
        if matches!(
            context.as_str(),
            "technical" | "technical-context" | "technical_context" | "jin10" | "jin10-context"
        ) {
            continue;
        }
        let count = context_count(conn, context)?;
        if count == 0 {
            bail!("strict SQLite data source requires context {context:?} before live run");
        }
    }
    Ok(())
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
    let mut roles = Vec::new();
    for item in raw.split(',') {
        let text = item.trim();
        if text.is_empty() {
            continue;
        }
        let role = match text {
            "technical" | "analyst.technical" => "analyst.technical",
            "news" | "news_macro" | "analyst.news_macro" => "analyst.news_macro",
            "youtube" | "analyst.youtube" => "analyst.youtube",
            "reddit" | "analyst.reddit" => "analyst.reddit",
            "x" | "analyst.x" => "analyst.x",
            "fundamental" | "analyst.fundamental" => {
                bail!("standalone fundamental analyst was removed; use news/news_macro")
            }
            other => bail!("unsupported phase1 agent {other:?}"),
        };
        roles.push(role.to_string());
    }
    Ok(roles)
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
    crate::cli_config::shared_db_path_from_config(config)
}

fn run_id_for(tickers: &[String], date: &str, run_dir: &std::path::Path) -> String {
    format!(
        "{}-{}-{}",
        run_slug(tickers).to_ascii_lowercase(),
        date,
        run_dir
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("run")
    )
}

async fn run_phase1(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    roles: &[String],
    mock: bool,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<()> {
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
            state,
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
        mock,
        model_override,
        reasoning_effort_override,
        config,
    )
    .await?;
    Ok(())
}

async fn run_phase2(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    mock: bool,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    max_debate_rounds: i64,
    max_topics: i64,
    config: &RuntimeConfig,
) -> Result<()> {
    let topics = run_phase2_topic_generation(
        conn,
        state,
        mock,
        model_override,
        reasoning_effort_override,
        config,
    )
    .await?
    .into_iter()
    .take(max_topics.max(1) as usize)
    .collect::<Vec<_>>();
    debug!(topic_count = topics.len(), "phase 2 topics generated");
    let mut turns = Vec::new();
    for topic in topics {
        let topic_id = topic_id_from_topic(&topic);
        debug!(topic_id, "phase 2 topic debate starting");
        let topic_state = json!({
            "topic": topic,
            "turns": [],
            "controller_artifacts": []
        });
        upsert_topic_debate_state(state, &topic_id, topic_state);

        let bull_opening = run_topic_debate_step(
            conn,
            state,
            "researcher.bull.initial",
            "analysis_initial",
            1,
            &topic_id,
            mock,
            model_override,
            reasoning_effort_override,
            config,
            mode_prompt_path(&config.prompts.bull_initial, state),
        )
        .await?;
        turns.push(bull_opening);
        state["debate_turns"] = Value::Array(turns.clone());
        run_topic_controller(
            conn,
            state,
            &topic_id,
            "after_bull_opening",
            1,
            mock,
            model_override,
            reasoning_effort_override,
            config,
        )
        .await?;

        let bear_opening = run_topic_debate_step(
            conn,
            state,
            "researcher.bear.initial",
            "analysis_initial",
            1,
            &topic_id,
            mock,
            model_override,
            reasoning_effort_override,
            config,
            mode_prompt_path(&config.prompts.bear_initial, state),
        )
        .await?;
        turns.push(bear_opening);
        state["debate_turns"] = Value::Array(turns.clone());
        run_topic_controller(
            conn,
            state,
            &topic_id,
            "after_bear_opening",
            1,
            mock,
            model_override,
            reasoning_effort_override,
            config,
        )
        .await?;

        for round in 2..=max_debate_rounds {
            debug!(topic_id, round, "phase 2 debate round starting");
            let bull_rebuttal = run_topic_debate_step(
                conn,
                state,
                "researcher.bull.interaction",
                "interaction_research",
                round,
                &topic_id,
                mock,
                model_override,
                reasoning_effort_override,
                config,
                config.prompts.bull_interaction.clone(),
            )
            .await?;
            turns.push(bull_rebuttal);
            state["debate_turns"] = Value::Array(turns.clone());
            run_topic_controller(
                conn,
                state,
                &topic_id,
                "after_bull_rebuttal",
                round,
                mock,
                model_override,
                reasoning_effort_override,
                config,
            )
            .await?;

            let bear_rebuttal = run_topic_debate_step(
                conn,
                state,
                "researcher.bear.interaction",
                "interaction_research",
                round,
                &topic_id,
                mock,
                model_override,
                reasoning_effort_override,
                config,
                config.prompts.bear_interaction.clone(),
            )
            .await?;
            turns.push(bear_rebuttal);
            state["debate_turns"] = Value::Array(turns.clone());
            run_topic_controller(
                conn,
                state,
                &topic_id,
                "after_bear_rebuttal",
                round,
                mock,
                model_override,
                reasoning_effort_override,
                config,
            )
            .await?;
        }
        debug!(
            topic_id,
            turn_count = turns.len(),
            "phase 2 topic debate completed"
        );
    }
    state["debate_turns"] = Value::Array(turns.clone());
    run_phase2_final_reducer(
        conn,
        state,
        mock,
        model_override,
        reasoning_effort_override,
        config,
    )
    .await?;
    Ok(())
}

async fn run_phase2_topic_generation(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    mock: bool,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<Vec<Value>> {
    let base = build_topic_generation_artifact(state);
    state["topic_generation_artifact"] = base.clone();
    debug!("phase 2 topic generation role starting");
    let output = run_single_role_job(
        RoleRun {
            state,
            role: "mediator.topic",
            phase: 2,
            kind: "topic_generation",
            round: None,
            topic_id: None,
            mock,
            model_override,
            reasoning_effort_override,
            config,
            prompt_path: Some(config.prompts.topic_generation.as_path()),
        },
        config.workflow.reducer_timeout_sec,
        config,
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
    mock: bool,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
    prompt_path: PathBuf,
) -> Result<Value> {
    let artifact = run_single_role_job(
        RoleRun {
            state,
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
    mock: bool,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<()> {
    let base = build_topic_controller_artifact(state, topic_id, checkpoint, round, config);
    set_topic_controller_state(state, topic_id, base.clone());
    let output = run_single_role_job(
        RoleRun {
            state,
            role: "mediator.topic_controller",
            phase: 25,
            kind: checkpoint,
            round: Some(round),
            topic_id: Some(topic_id),
            mock,
            model_override,
            reasoning_effort_override,
            config,
            prompt_path: Some(config.prompts.topic_controller.as_path()),
        },
        config.workflow.reducer_timeout_sec,
        config,
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
    mock: bool,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<()> {
    let base = build_phase1_state_artifact(state, config);
    state["phase1_state_artifact"] = base.clone();
    let reducer_result = run_single_role_job(
        RoleRun {
            state,
            role: "reducer.evidence",
            phase: 15,
            kind: "artifact",
            round: None,
            topic_id: None,
            mock,
            model_override,
            reasoning_effort_override,
            config,
            prompt_path: Some(config.prompts.reducer_evidence.as_path()),
        },
        config.workflow.reducer_timeout_sec,
        config,
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

fn phase1_reducer_fallback(mut base: Value, error: anyhow::Error) -> Value {
    if let Some(object) = base.as_object_mut() {
        object.insert("llm_reducer_status".to_string(), json!("fallback"));
        object.insert(
            "llm_reducer_error".to_string(),
            Value::String(error.to_string()),
        );
        object.insert(
            "llm_brief".to_string(),
            Value::String(
                "reducer.evidence used local phase1_state_artifact fallback.".to_string(),
            ),
        );
        object.insert(
            "reducer_checks".to_string(),
            json!({
                "json_valid": true,
                "no_new_external_facts": true,
                "all_claims_source_backed": true,
                "fallback": true
            }),
        );
    }
    base
}

async fn run_phase2_final_reducer(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    mock: bool,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<()> {
    let base = build_debate_state_artifact(state, config);
    state["debate_state_artifact"] = base.clone();
    let reducer_output = run_single_role_job(
        RoleRun {
            state,
            role: "reducer.debate_final",
            phase: 25,
            kind: "final_artifact",
            round: None,
            topic_id: None,
            mock,
            model_override,
            reasoning_effort_override,
            config,
            prompt_path: Some(config.prompts.reducer_debate_final.as_path()),
        },
        config.workflow.reducer_timeout_sec,
        config,
    )
    .await?;
    let artifact = merge_reducer_output(base, reducer_output);
    let brief = reducer_brief_md(&artifact);
    state["debate_state_artifact"] = artifact.clone();
    state["debate_brief_md"] = Value::String(brief.clone());
    persist_artifact_with_last_md(conn, state, 25, "reducer.debate_final", artifact, brief)?;
    set_phase_status(state, 25, "done");
    Ok(())
}

fn build_phase1_state_artifact(state: &Value, config: &RuntimeConfig) -> Value {
    let tickers = tickers_from_state(state);
    let reports = state
        .get("analyst_reports")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let roles = state
        .get("phase1_agents")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let missing_sources = state
        .get("missing_sources")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    let missing_critical_roles = roles
        .iter()
        .filter(|role| config.workflow.critical_roles.contains(*role))
        .filter(|role| !reports.contains_key(*role) || missing_sources.contains(*role))
        .cloned()
        .collect::<Vec<_>>();
    let degraded_noncritical_roles = missing_sources
        .iter()
        .filter(|role| !config.workflow.critical_roles.contains(*role))
        .cloned()
        .collect::<Vec<_>>();
    let status = if !missing_critical_roles.is_empty() {
        "blocked"
    } else if state
        .get("degraded")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        "partial"
    } else {
        "ready"
    };
    let weighted_probability_base = weighted_probability_base(state, &tickers, &reports);
    let per_ticker = tickers
        .iter()
        .map(|ticker| {
            let role_summaries = roles
                .iter()
                .map(|role| {
                    let payload = reports
                        .get(role)
                        .and_then(|artifact| artifact_for_ticker(artifact, ticker));
                    json!({
                        "role": role,
                        "status": if missing_sources.contains(role) { "missing" } else { "ready" },
                        "stance": payload.and_then(|value| value.get("direction")).and_then(Value::as_str).unwrap_or("neutral"),
                        "confidence": payload.and_then(|value| value.get("confidence")).cloned().unwrap_or(Value::Null),
                        "key_evidence": payload.and_then(|value| value.get("evidence")).cloned().unwrap_or_else(|| json!([])),
                        "weaknesses": payload.and_then(|value| value.get("weaknesses")).cloned().unwrap_or_else(|| json!([])),
                        "source_node_ids": payload.and_then(|value| value.get("source_node_ids")).cloned().unwrap_or_else(|| json!([])),
                        "summary": payload.and_then(|value| value.get("report")).and_then(Value::as_str).unwrap_or("")
                    })
                })
                .collect::<Vec<_>>();
            (
                ticker.clone(),
                json!({
                    "id": "reducer.evidence",
                    "role": "reducer.evidence",
                    "artifact_type": "phase1_state_artifact",
                    "weighted_probability_base": weighted_probability_base.get(ticker).cloned().unwrap_or(Value::Null),
                    "role_summaries": role_summaries,
                    "long_evidence": [],
                    "short_evidence": [],
                    "neutral_or_ambiguous_evidence": [],
                    "evidence_clusters": [],
                    "independent_signals": [],
                    "duplicate_signals": [],
                    "conflicts": [],
                    "missing_evidence": degraded_noncritical_roles,
                    "decision_hinges": [],
                    "topic_candidates": [
                        {
                            "topic_id": format!("{ticker}-aggregate"),
                            "topic": format!("Highest-impact unresolved long/short evidence for {ticker}"),
                            "tickers": [ticker],
                            "long_evidence_refs": [],
                            "short_evidence_refs": [],
                            "why_debate": "Fallback topic generated from Phase 1.5 state."
                        }
                    ],
                    "state_summary": format!("Phase 1 state for {ticker}: {status}.")
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    json!({
        "id": "reducer.evidence",
        "role": "reducer.evidence",
        "artifact_type": "phase1_state_artifact",
        "phase": "phase1.5",
        "status": status,
        "workflow_pattern": "Workflow -> Stage/Sub-workflow -> Agent workers -> Reducer -> state artifact",
        "generated_from": {
            "worker_roles": roles,
            "critical_roles": config.workflow.critical_roles.iter().cloned().collect::<Vec<_>>(),
            "missing_critical_roles": missing_critical_roles,
            "degraded_noncritical_roles": degraded_noncritical_roles
        },
        "late_evidence": state.get("late_evidence").cloned().unwrap_or_else(|| json!([])),
        "weighted_probability_base": weighted_probability_base,
        "per_ticker": per_ticker,
        "topic_candidates": fallback_topics_for_tickers(&tickers),
        "cross_ticker_notes": [],
        "reducer_checks": {
            "json_valid": true,
            "no_new_external_facts": true,
            "all_claims_source_backed": true
        }
    })
}

fn build_topic_generation_artifact(state: &Value) -> Value {
    let tickers = tickers_from_state(state);
    let topics = phase1_topic_candidates(state);
    json!({
        "id": "mediator.topic",
        "role": "mediator.topic",
        "artifact_type": "phase2_topic_generation_artifact",
        "phase": "phase2.topic_generation",
        "status": "ready",
        "generated_from": {
            "source_artifact": "phase1_state_artifact",
            "tickers": tickers
        },
        "topics": topics,
        "reducer_checks": {
            "json_valid": true,
            "from_phase1_5_only": true,
            "no_new_external_facts": true
        }
    })
}

fn build_debate_state_artifact(state: &Value, config: &RuntimeConfig) -> Value {
    let tickers = tickers_from_state(state);
    let turns = state
        .get("debate_turns")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let topic_states = state
        .get("topic_debate_states")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let late_evidence = state
        .get("late_evidence")
        .and_then(Value::as_array)
        .map(|items| !items.is_empty())
        .unwrap_or(false);
    let per_ticker = tickers
        .iter()
        .map(|ticker| {
            (
                ticker.clone(),
                json!({
                    "id": "reducer.debate_final",
                    "role": "reducer.debate_final",
                    "artifact_type": "phase2_5_debate_state_artifact",
                    "status": "ready",
                    "turn_count": turns.len(),
                    "decision_hinges": [],
                    "missing_evidence": [],
                    "manager_handoff": {
                        "directional_pressure": "mixed",
                        "confidence_modifier": "neutral",
                        "why": format!("Debate reducer compressed {} turns for {ticker}.", turns.len()),
                        "do_not_exceed": "Do not treat this as final probability or rating."
                    }
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    let topic_briefs = if topic_states.is_empty() {
        fallback_topics_for_tickers(&tickers)
            .into_iter()
            .map(|topic| debate_topic_brief(topic, turns.len(), late_evidence, config))
            .collect::<Vec<_>>()
    } else {
        topic_states
            .values()
            .map(|topic_state| {
                let topic = topic_state
                    .get("topic")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                let latest = topic_state
                    .get("controller_artifact")
                    .cloned()
                    .or_else(|| {
                        topic_state
                            .get("controller_artifacts")
                            .and_then(Value::as_array)
                            .and_then(|items| items.last())
                            .cloned()
                    })
                    .unwrap_or_else(|| json!({}));
                debate_topic_brief_from_state(topic, latest, late_evidence, config)
            })
            .collect::<Vec<_>>()
    };
    json!({
        "id": "reducer.debate_final",
        "role": "reducer.debate_final",
        "artifact_type": "phase2_5_debate_state_artifact",
        "phase": "phase2.5b",
        "status": "ready",
        "workflow_pattern": "Workflow -> Stage/Sub-workflow -> Agent workers -> Reducer -> state artifact",
        "generated_from": {
            "worker_roles": [
                "mediator.topic",
                "researcher.bull.initial",
                "researcher.bear.initial",
                "researcher.bull.interaction",
                "researcher.bear.interaction",
                "mediator.topic_controller"
            ],
            "turn_count": turns.len(),
            "topic_count": topic_briefs.len()
        },
        "topic_briefs": topic_briefs,
        "topic_debate_states": topic_states,
        "debate_turns": turns,
        "late_evidence": state.get("late_evidence").cloned().unwrap_or_else(|| json!([])),
        "per_ticker": per_ticker,
        "cross_topic_notes": [],
        "reducer_checks": {
            "json_valid": true,
            "no_final_probability": true,
            "no_winner_declared": true,
            "no_new_external_facts": true
        }
    })
}

fn debate_topic_brief(
    topic: Value,
    turn_count: usize,
    late_evidence: bool,
    config: &RuntimeConfig,
) -> Value {
    let topic_id = topic_id_from_topic(&topic);
    let topic_name = topic
        .get("topic")
        .and_then(Value::as_str)
        .unwrap_or("Aggregate debate state");
    let tickers = topic.get("tickers").cloned().unwrap_or_else(|| json!([]));
    json!({
        "topic_id": topic_id,
        "topic": topic_name,
        "tickers": tickers,
        "status": "ready",
        "is_repetitive": false,
        "needs_manager_attention": false,
        "adjudication": {
            "bull_argument_strength": null,
            "bear_argument_strength": null,
            "convergence_score": null,
            "unresolved_conflict": "",
            "no_winner_declared": true
        },
        "fact_check": {
            "supported_claims": [],
            "contested_claims": [],
            "unsupported_claims": [],
            "stale_or_late_evidence": []
        },
        "compressed_state": {
            "agreed_facts": [],
            "agreed_assumptions": [],
            "agreed_risks": [],
            "decision_hinges": [],
            "missing_high_impact_factors": [],
            "missing_evidence": [],
            "highest_value_next_query": "",
            "info_gain_score": null,
            "expected_info_gain_next_round": null,
            "should_continue": false,
            "stop_reason": format!("Final reducer compressed {turn_count} topic turns."),
            "question_grant": null
        },
        "late_evidence_effect": {
            "has_late_evidence": late_evidence,
            "used": late_evidence && config.workflow.late_evidence_enabled,
            "effect": if late_evidence { "pending" } else { "none" },
            "reason": if late_evidence { "Late evidence is appended and marked; stages are not replayed." } else { "" }
        },
        "manager_handoff": {
            "directional_pressure": "mixed",
            "confidence_modifier": "neutral",
            "why": "Review compressed per-topic bull/bear claims; reducer does not issue final probability.",
            "do_not_exceed": "Do not treat this as final probability or rating."
        }
    })
}

fn debate_topic_brief_from_state(
    topic: Value,
    controller_artifact: Value,
    late_evidence: bool,
    config: &RuntimeConfig,
) -> Value {
    let mut brief = debate_topic_brief(topic, 0, late_evidence, config);
    if let Some(object) = brief.as_object_mut() {
        if let Some(status) = controller_artifact.get("status").cloned() {
            object.insert("status".to_string(), status);
        }
        if let Some(claims) = controller_artifact.get("claim_ledger").cloned() {
            object.insert("claim_ledger".to_string(), claims);
        }
        if let Some(duplicates) = controller_artifact.get("duplicate_claims").cloned() {
            object.insert("duplicate_claims".to_string(), duplicates);
        }
        if let Some(unverifiable) = controller_artifact.get("unverifiable_claims").cloned() {
            object.insert("unverifiable_claims".to_string(), unverifiable);
        }
        if let Some(agenda) = controller_artifact.get("next_agenda").cloned() {
            object.insert("next_agenda".to_string(), agenda);
        }
        object.insert("controller_artifact".to_string(), controller_artifact);
    }
    brief
}

fn build_topic_controller_artifact(
    state: &Value,
    topic_id: &str,
    checkpoint: &str,
    round: i64,
    config: &RuntimeConfig,
) -> Value {
    let topic_state = topic_state(state, topic_id).unwrap_or_else(|| json!({}));
    let topic = topic_state
        .get("topic")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let turns = topic_state
        .get("turns")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    json!({
        "id": format!("mediator.topic_controller:{topic_id}:{checkpoint}:{round}"),
        "role": "mediator.topic_controller",
        "artifact_type": "topic_debate_state_artifact",
        "phase": "phase2.5a",
        "status": "ready",
        "topic_id": topic_id,
        "topic": topic,
        "checkpoint": checkpoint,
        "round": round,
        "turn_count": turns.len(),
        "claim_ledger": [],
        "duplicate_claims": [],
        "unverifiable_claims": [],
        "supported_claims": [],
        "contested_claims": [],
        "blocked_repeats": [],
        "next_agenda": [],
        "soft_control": {
            "should_continue": true,
            "stop_reason": "",
            "hard_stop_enforced": false,
            "max_rounds_remains_runtime_cap": true
        },
        "latest_turns": turns,
        "phase1_state_artifact": state.get("phase1_state_artifact").cloned().unwrap_or(Value::Null),
        "late_evidence_effect": {
            "has_late_evidence": state.get("late_evidence").and_then(Value::as_array).is_some_and(|items| !items.is_empty()),
            "used": config.workflow.late_evidence_enabled,
            "effect": "pending",
            "reason": ""
        },
        "reducer_checks": {
            "json_valid": true,
            "no_winner_declared": true,
            "no_new_external_facts": true
        }
    })
}

fn fallback_topics_for_tickers(tickers: &[String]) -> Vec<Value> {
    tickers
        .iter()
        .map(|ticker| {
            json!({
                "topic_id": format!("{ticker}-aggregate"),
                "topic": format!("Highest-impact unresolved long/short evidence for {ticker}"),
                "tickers": [ticker],
                "long_evidence_refs": [],
                "short_evidence_refs": [],
                "why_debate": "Fallback topic generated from Phase 1.5 state."
            })
        })
        .collect()
}

fn phase1_topic_candidates(state: &Value) -> Vec<Value> {
    state
        .get("phase1_state_artifact")
        .and_then(|artifact| artifact.get("topic_candidates"))
        .and_then(Value::as_array)
        .cloned()
        .filter(|items| !items.is_empty())
        .unwrap_or_else(|| fallback_topics_for_tickers(&tickers_from_state(state)))
}

fn topics_from_generation_artifact(artifact: &Value) -> Vec<Value> {
    artifact
        .get("reducer_output")
        .and_then(|output| {
            output
                .get("topics")
                .or_else(|| output.get("topic_candidates"))
        })
        .or_else(|| artifact.get("topics"))
        .or_else(|| artifact.get("topic_candidates"))
        .and_then(Value::as_array)
        .cloned()
        .filter(|items| !items.is_empty())
        .unwrap_or_else(|| fallback_topics_for_tickers(&tickers_from_state(artifact)))
}

fn topic_id_from_topic(topic: &Value) -> String {
    topic
        .get("topic_id")
        .or_else(|| topic.get("id"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToString::to_string)
        .unwrap_or_else(|| "topic-aggregate".to_string())
}

fn topic_state(state: &Value, topic_id: &str) -> Option<Value> {
    state
        .get("topic_debate_states")
        .and_then(Value::as_object)
        .and_then(|items| items.get(topic_id))
        .cloned()
}

fn upsert_topic_debate_state(state: &mut Value, topic_id: &str, topic_state: Value) {
    if !state
        .get("topic_debate_states")
        .is_some_and(Value::is_object)
    {
        state["topic_debate_states"] = json!({});
    }
    if let Some(items) = state["topic_debate_states"].as_object_mut() {
        items.insert(topic_id.to_string(), topic_state);
    }
}

fn append_topic_turn(state: &mut Value, topic_id: &str, turn: Value) {
    if !state
        .get("topic_debate_states")
        .is_some_and(Value::is_object)
    {
        state["topic_debate_states"] = json!({});
    }
    if let Some(items) = state["topic_debate_states"].as_object_mut() {
        let entry = items.entry(topic_id.to_string()).or_insert_with(|| {
            json!({
                "topic": {"topic_id": topic_id, "topic": topic_id, "tickers": []},
                "turns": [],
                "controller_artifacts": []
            })
        });
        if !entry.get("turns").is_some_and(Value::is_array) {
            entry["turns"] = json!([]);
        }
        if let Some(turns) = entry["turns"].as_array_mut() {
            turns.push(turn);
        }
    }
}

fn set_topic_controller_state(state: &mut Value, topic_id: &str, artifact: Value) {
    if !state
        .get("topic_debate_states")
        .is_some_and(Value::is_object)
    {
        state["topic_debate_states"] = json!({});
    }
    if let Some(items) = state["topic_debate_states"].as_object_mut() {
        let entry = items.entry(topic_id.to_string()).or_insert_with(|| {
            json!({
                "topic": {"topic_id": topic_id, "topic": topic_id, "tickers": []},
                "turns": [],
                "controller_artifacts": []
            })
        });
        entry["controller_artifact"] = artifact;
    }
}

fn append_topic_controller_artifact(state: &mut Value, topic_id: &str, artifact: Value) {
    if !state
        .get("topic_debate_states")
        .is_some_and(Value::is_object)
    {
        state["topic_debate_states"] = json!({});
    }
    if let Some(items) = state["topic_debate_states"].as_object_mut() {
        let entry = items.entry(topic_id.to_string()).or_insert_with(|| {
            json!({
                "topic": {"topic_id": topic_id, "topic": topic_id, "tickers": []},
                "turns": [],
                "controller_artifacts": []
            })
        });
        if !entry
            .get("controller_artifacts")
            .is_some_and(Value::is_array)
        {
            entry["controller_artifacts"] = json!([]);
        }
        if let Some(items) = entry["controller_artifacts"].as_array_mut() {
            items.push(artifact);
        }
    }
}

fn merge_reducer_output(mut base: Value, reducer_output: Value) -> Value {
    if let Some(object) = base.as_object_mut() {
        object.insert("reducer_output".to_string(), reducer_output.clone());
        if let Some(status) = reducer_output.get("status").cloned() {
            object.insert("llm_reducer_status".to_string(), status);
        }
        if let Some(checks) = reducer_output.get("reducer_checks").cloned() {
            object.insert("llm_reducer_checks".to_string(), checks);
        }
        if let Some(summary) = reducer_output
            .get("state_summary")
            .or_else(|| reducer_output.get("summary"))
            .or_else(|| reducer_output.get("brief_markdown"))
            .cloned()
        {
            object.insert("llm_brief".to_string(), summary);
        }
    }
    base
}

fn reducer_brief_md(artifact: &Value) -> String {
    if let Some(text) = artifact
        .get("llm_brief")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        return text.to_string();
    }
    let role = artifact.get("role").and_then(Value::as_str).unwrap_or("");
    let status = artifact
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let artifact_type = artifact
        .get("artifact_type")
        .and_then(Value::as_str)
        .unwrap_or("state_artifact");
    format!("{role} produced {artifact_type} with status {status}.")
}

fn weighted_probability_base(
    state: &Value,
    tickers: &[String],
    reports: &serde_json::Map<String, Value>,
) -> serde_json::Map<String, Value> {
    let weights = state
        .get("analyst_weights")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    tickers
        .iter()
        .map(|ticker| {
            let mut weighted_direction = 0.0;
            let mut confidence_total = 0.0;
            let mut weight_total = 0.0;
            let mut source_roles = Vec::new();
            for (role, report) in reports {
                let weight = weights.get(role).and_then(Value::as_f64).unwrap_or(0.0);
                if weight <= 0.0 {
                    continue;
                }
                let payload = artifact_for_ticker(report, ticker).unwrap_or(report);
                let confidence = payload
                    .get("confidence")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.5)
                    .clamp(0.0, 1.0);
                let direction = match payload
                    .get("direction")
                    .and_then(Value::as_str)
                    .unwrap_or("neutral")
                {
                    "bullish" | "long" | "positive" => 1.0,
                    "bearish" | "short" | "negative" => -1.0,
                    _ => 0.0,
                };
                weighted_direction += weight * confidence * direction;
                confidence_total += weight * confidence;
                weight_total += weight;
                source_roles.push(Value::String(role.clone()));
            }
            let net = if confidence_total > 0.0 {
                weighted_direction / confidence_total
            } else {
                0.0
            };
            let long_probability = ((net + 1.0) / 2.0).clamp(0.0, 1.0);
            let short_probability = 1.0 - long_probability;
            let confidence = if weight_total > 0.0 {
                (confidence_total / weight_total).clamp(0.0, 1.0)
            } else {
                0.0
            };
            (
                ticker.clone(),
                json!({
                    "long_probability": long_probability,
                    "short_probability": short_probability,
                    "confidence": confidence,
                    "source_roles": source_roles
                }),
            )
        })
        .collect()
}

fn artifact_for_ticker<'a>(artifact: &'a Value, ticker: &str) -> Option<&'a Value> {
    artifact
        .get("per_ticker")
        .and_then(Value::as_object)
        .and_then(|items| items.get(ticker))
}

async fn run_phase3(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    mock: bool,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<()> {
    debug!("manager research role starting");
    let artifact = run_single_role_job(
        RoleRun {
            state,
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
        mock,
        model_override,
        reasoning_effort_override,
        config,
    )
    .await?;
    Ok(())
}

fn manager_research_fallback(state: &mut Value, error: anyhow::Error) -> Value {
    let mut artifact = mock_role_artifact("manager.research", &tickers_from_state(state));
    artifact["status"] = json!("degraded");
    artifact["degraded"] = Value::Bool(true);
    artifact["error"] = Value::String(error.to_string());
    artifact["probability_rationale"] = Value::String(format!(
        "manager.research fallback used because live LLM failed: {error}"
    ));
    state["degraded"] = Value::Bool(true);
    artifact
}

async fn run_memory_reflector_after_phase3(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    mock: bool,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
) -> Result<()> {
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
        state,
        role: MEMORY_REFLECTOR_ROLE,
        phase: MEMORY_REFLECTOR_PHASE,
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
        MEMORY_REFLECTOR_PHASE,
        MEMORY_REFLECTOR_ROLE,
        artifact.clone(),
    ) {
        Ok(()) => {
            state["memory_update_proposal"] = artifact;
            state["memory_reflector"] = json!({
                "status": "validated_persisted",
                "role": MEMORY_REFLECTOR_ROLE,
                "phase": MEMORY_REFLECTOR_PHASE,
                "persisted_agent_message": true
            });
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

fn record_memory_reflector_status(state: &mut Value, status: &str, message: &str) {
    state["memory_reflector"] = json!({
        "status": status,
        "message": message,
        "role": MEMORY_REFLECTOR_ROLE,
        "phase": MEMORY_REFLECTOR_PHASE
    });
}

fn validate_memory_update_proposal(artifact: &Value, tickers: &[String]) -> Result<()> {
    if !artifact.is_object() {
        bail!("MemoryUpdateProposal must be a JSON object");
    }
    let artifact_type = required_non_empty_str(artifact, "artifact_type")?;
    if artifact_type != "MemoryUpdateProposal" {
        bail!("artifact_type must be MemoryUpdateProposal");
    }
    let schema_version = artifact
        .get("schema_version")
        .and_then(Value::as_i64)
        .context("schema_version must be integer 1")?;
    if schema_version != 1 {
        bail!("schema_version must be 1");
    }
    let source_role = required_non_empty_str(artifact, "source_role")?;
    if source_role != "manager.research" {
        bail!("source_role must be manager.research");
    }
    required_non_empty_str(artifact, "run_id")?;
    validate_rfc3339_field(artifact, "generated_at")?;
    let proposals = artifact
        .get("proposals")
        .and_then(Value::as_array)
        .context("proposals must be an array")?;
    if proposals.is_empty() {
        required_non_empty_str(artifact, "no_update_reason")?;
        return Ok(());
    }
    for (index, proposal) in proposals.iter().enumerate() {
        validate_memory_update_item(proposal, index, tickers)?;
    }
    Ok(())
}

fn validate_memory_update_item(proposal: &Value, index: usize, tickers: &[String]) -> Result<()> {
    if !proposal.is_object() {
        bail!("proposals[{index}] must be an object");
    }
    validate_enum_field(
        proposal,
        "update_type",
        &["thesis", "observation", "risk", "follow_up"],
    )
    .with_context(|| format!("proposals[{index}].update_type"))?;
    let ticker = required_non_empty_str(proposal, "ticker")?;
    if !tickers.is_empty() && !tickers.iter().any(|item| item == ticker) {
        bail!("proposals[{index}].ticker {ticker:?} is not in run tickers");
    }
    validate_enum_field(
        proposal,
        "scope",
        &["ticker", "sector", "macro", "market", "portfolio"],
    )
    .with_context(|| format!("proposals[{index}].scope"))?;
    validate_rfc3339_field(proposal, "observed_at")?;
    validate_date_field(proposal, "source_date")?;
    validate_nullable_rfc3339_field(proposal, "expires_at")?;
    validate_confidence(proposal, index)?;
    required_non_empty_str(proposal, "summary")?;
    validate_evidence_refs(proposal, index)?;
    validate_non_empty_string_array(proposal, "invalidation_conditions", index)?;
    validate_non_empty_string_array(proposal, "follow_up_checks", index)?;
    if proposal
        .get("update_type")
        .and_then(Value::as_str)
        .is_some_and(|value| value == "thesis")
    {
        validate_thesis_update(proposal, index)?;
    }
    Ok(())
}

fn validate_confidence(proposal: &Value, index: usize) -> Result<()> {
    let confidence = proposal
        .get("confidence")
        .and_then(Value::as_f64)
        .with_context(|| format!("proposals[{index}].confidence must be a number"))?;
    if !(0.0..=1.0).contains(&confidence) {
        bail!("proposals[{index}].confidence must be between 0 and 1");
    }
    Ok(())
}

fn validate_evidence_refs(proposal: &Value, index: usize) -> Result<()> {
    let refs = proposal
        .get("evidence_refs")
        .and_then(Value::as_array)
        .with_context(|| format!("proposals[{index}].evidence_refs must be an array"))?;
    if refs.is_empty() {
        bail!("proposals[{index}].evidence_refs must not be empty");
    }
    for (ref_index, item) in refs.iter().enumerate() {
        validate_enum_field(
            item,
            "source_type",
            &[
                "final_research",
                "debate_brief",
                "evidence_brief",
                "source_item",
                "prior_memory",
            ],
        )
        .with_context(|| format!("proposals[{index}].evidence_refs[{ref_index}]"))?;
        required_non_empty_str(item, "source_id")
            .with_context(|| format!("proposals[{index}].evidence_refs[{ref_index}]"))?;
        required_non_empty_str(item, "quote_or_fact")
            .with_context(|| format!("proposals[{index}].evidence_refs[{ref_index}]"))?;
    }
    Ok(())
}

fn validate_thesis_update(proposal: &Value, index: usize) -> Result<()> {
    let thesis = proposal
        .get("thesis")
        .with_context(|| format!("proposals[{index}].thesis is required for thesis updates"))?;
    let status = required_non_empty_str(thesis, "status")
        .with_context(|| format!("proposals[{index}].thesis.status"))?;
    match status {
        "new" => Ok(()),
        "update" => {
            required_non_empty_str(thesis, "prior_thesis_id")
                .with_context(|| format!("proposals[{index}].thesis.prior_thesis_id"))?;
            Ok(())
        }
        other => bail!("proposals[{index}].thesis.status {other:?} must be new or update"),
    }
}

fn required_non_empty_str<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .with_context(|| format!("{field} must be a non-empty string"))
}

fn validate_enum_field<'a>(value: &'a Value, field: &str, allowed: &[&str]) -> Result<&'a str> {
    let text = required_non_empty_str(value, field)?;
    if allowed.contains(&text) {
        Ok(text)
    } else {
        bail!("{field} must be one of {}", allowed.join(", "))
    }
}

fn validate_rfc3339_field(value: &Value, field: &str) -> Result<()> {
    let text = required_non_empty_str(value, field)?;
    DateTime::parse_from_rfc3339(text).with_context(|| format!("{field} must be RFC3339"))?;
    Ok(())
}

fn validate_nullable_rfc3339_field(value: &Value, field: &str) -> Result<()> {
    if matches!(value.get(field), Some(Value::Null)) {
        return Ok(());
    }
    validate_rfc3339_field(value, field)
}

fn validate_date_field(value: &Value, field: &str) -> Result<()> {
    let text = required_non_empty_str(value, field)?;
    NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .with_context(|| format!("{field} must use YYYY-MM-DD"))?;
    Ok(())
}

fn validate_non_empty_string_array(value: &Value, field: &str, index: usize) -> Result<()> {
    let items = value
        .get(field)
        .and_then(Value::as_array)
        .with_context(|| format!("proposals[{index}].{field} must be an array"))?;
    if items.is_empty() {
        bail!("proposals[{index}].{field} must not be empty");
    }
    for (item_index, item) in items.iter().enumerate() {
        if item
            .as_str()
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .is_none()
        {
            bail!("proposals[{index}].{field}[{item_index}] must be a non-empty string");
        }
    }
    Ok(())
}

struct RoleRun<'a> {
    state: &'a Value,
    role: &'a str,
    phase: i64,
    kind: &'a str,
    round: Option<i64>,
    topic_id: Option<&'a str>,
    mock: bool,
    model_override: Option<&'a str>,
    reasoning_effort_override: Option<&'a str>,
    config: &'a RuntimeConfig,
    prompt_path: Option<&'a std::path::Path>,
}

#[derive(Debug)]
struct RoleJob {
    role: String,
    phase: i64,
    kind: String,
    round: Option<i64>,
    topic_id: Option<String>,
    mock: bool,
    prompt: String,
    prompt_path: Option<String>,
    tickers: Vec<String>,
    output_mode: OutputMode,
    llm: Option<RoleLlmSettings>,
    reasoning_effort_override: Option<String>,
    tools: ExternalToolConfig,
    web_search: WebSearchConfig,
}

#[derive(Debug)]
struct RoleJobResult {
    role: String,
    phase: i64,
    kind: String,
    round: Option<i64>,
    topic_id: Option<String>,
    tickers: Vec<String>,
    artifact: Option<Value>,
    error: Option<String>,
    timed_out: bool,
    elapsed_ms: u128,
}

fn prepare_role_job(input: RoleRun<'_>) -> Result<RoleJob> {
    let RoleRun {
        state,
        role,
        phase,
        kind,
        round,
        topic_id,
        mock,
        model_override,
        reasoning_effort_override,
        config,
        prompt_path,
    } = input;
    let tickers = tickers_from_state(state);
    let prompt = if mock {
        String::new()
    } else {
        render_prompt(state, role, phase, kind, round, topic_id, prompt_path)?
    };
    let llm = if mock {
        None
    } else {
        let mut llm = config
            .llm_roles
            .get(role)
            .with_context(|| format!("missing LLM config for role {role:?}"))?
            .clone();
        if let Some(model) = model_override.filter(|value| !value.trim().is_empty()) {
            llm.model = model.to_string();
        }
        Some(llm)
    };
    debug!(
        role,
        phase,
        kind,
        round,
        topic_id,
        mock,
        prompt_path = prompt_path.map(|path| path.display().to_string()),
        prompt_chars = prompt.len(),
        "prepared role job"
    );
    Ok(RoleJob {
        role: role.to_string(),
        phase,
        kind: kind.to_string(),
        round,
        topic_id: topic_id.map(ToString::to_string),
        mock,
        prompt,
        prompt_path: prompt_path.map(|path| path.display().to_string()),
        tickers: tickers.clone(),
        output_mode: output_mode_for_role(role),
        llm,
        reasoning_effort_override: reasoning_effort_override.map(ToString::to_string),
        tools: ExternalToolConfig {
            project_root: default_project_root(),
            db_path: state
                .get("db_path")
                .and_then(Value::as_str)
                .map(PathBuf::from),
            run_dir: state
                .get("run_dir")
                .and_then(Value::as_str)
                .map(PathBuf::from),
            run_id: state
                .get("run_id")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            tickers,
        },
        web_search: config.web_search.get(role).cloned().unwrap_or_default(),
    })
}

async fn run_role_jobs(
    jobs: Vec<RoleJob>,
    parallelism: usize,
    timeout_sec: u64,
) -> Vec<RoleJobResult> {
    debug!(
        job_count = jobs.len(),
        parallelism = parallelism.max(1),
        timeout_sec,
        "running role jobs"
    );
    stream::iter(jobs)
        .map(|job| run_role_job_with_timeout(job, timeout_sec))
        .buffer_unordered(parallelism.max(1))
        .collect()
        .await
}

async fn run_single_role_job(
    input: RoleRun<'_>,
    timeout_sec: u64,
    config: &RuntimeConfig,
) -> Result<Value> {
    let job = prepare_role_job(input)?;
    let result = run_role_job_with_timeout(job, timeout_sec).await;
    let mut local_state = json!({});
    role_artifact_or_degraded(&mut local_state, config, result)
}

async fn run_role_job_with_timeout(job: RoleJob, timeout_sec: u64) -> RoleJobResult {
    let role = job.role.clone();
    let phase = job.phase;
    let kind = job.kind.clone();
    let round = job.round;
    let topic_id = job.topic_id.clone();
    let tickers = job.tickers.clone();
    let started_at = Instant::now();
    debug!(
        role,
        phase, kind, round, topic_id, timeout_sec, "role job starting"
    );
    match time::timeout(
        Duration::from_secs(timeout_sec.max(1)),
        execute_role_job(job),
    )
    .await
    {
        Ok(Ok(artifact)) => {
            let elapsed_ms = started_at.elapsed().as_millis();
            debug!(role, phase, kind, elapsed_ms, "role job completed");
            RoleJobResult {
                role,
                phase,
                kind,
                round,
                topic_id,
                tickers,
                artifact: Some(artifact),
                error: None,
                timed_out: false,
                elapsed_ms,
            }
        }
        Ok(Err(error)) => {
            let elapsed_ms = started_at.elapsed().as_millis();
            warn!(
                role,
                phase,
                kind,
                elapsed_ms,
                error = %error,
                "role job failed"
            );
            RoleJobResult {
                role,
                phase,
                kind,
                round,
                topic_id,
                tickers,
                artifact: None,
                error: Some(error.to_string()),
                timed_out: false,
                elapsed_ms,
            }
        }
        Err(_) => {
            let elapsed_ms = started_at.elapsed().as_millis();
            warn!(
                role,
                phase, kind, elapsed_ms, timeout_sec, "role job timed out"
            );
            RoleJobResult {
                role,
                phase,
                kind,
                round,
                topic_id,
                tickers,
                artifact: None,
                error: Some(format!("role execution timed out after {timeout_sec}s")),
                timed_out: true,
                elapsed_ms,
            }
        }
    }
}

async fn execute_role_job(job: RoleJob) -> Result<Value> {
    if job.mock {
        debug!(
            role = job.role,
            phase = job.phase,
            kind = job.kind,
            "using mock artifact"
        );
        let mut artifact = mock_role_artifact(&job.role, &job.tickers);
        artifact["phase"] = Value::Number(job.phase.into());
        artifact["kind"] = Value::String(job.kind);
        if let Some(round) = job.round {
            artifact["round"] = Value::Number(round.into());
        }
        if let Some(topic_id) = job.topic_id {
            artifact["topic_id"] = Value::String(topic_id);
        }
        if let Some(path) = job.prompt_path {
            artifact["prompt_path"] = Value::String(path);
        }
        return Ok(artifact);
    }
    let llm = job
        .llm
        .with_context(|| format!("missing prepared LLM config for role {:?}", job.role))?;
    let settings = RigSettings {
        role: job.role,
        phase: Some(job.phase),
        tickers: job.tickers,
        output_mode: job.output_mode,
        llm,
        reasoning_effort_override: job.reasoning_effort_override,
        tools: Some(job.tools),
        web_search: job.web_search,
    };
    debug!(
        role = settings.role,
        model = settings.llm.model,
        prompt_chars = job.prompt.len(),
        "calling rig agent loop"
    );
    run_rig_agent_loop(&settings, &job.prompt).await
}

fn role_artifact_or_degraded(
    state: &mut Value,
    config: &RuntimeConfig,
    result: RoleJobResult,
) -> Result<Value> {
    if let Some(artifact) = result.artifact {
        return Ok(artifact);
    }
    let message = result
        .error
        .clone()
        .unwrap_or_else(|| "role execution failed".to_string());
    if is_critical_role(config, &result.role) {
        bail!(
            "critical role {} failed in phase {} kind {}: {}",
            result.role,
            result.phase,
            result.kind,
            message
        );
    }
    // ponytail: keep failed non-critical roles visible in state instead of retrying whole phases.
    warn!(
        role = result.role,
        phase = result.phase,
        kind = result.kind,
        timed_out = result.timed_out,
        elapsed_ms = result.elapsed_ms,
        message,
        "role degraded"
    );
    record_degraded_role(state, &result, &message);
    Ok(degraded_role_artifact(&result, &message))
}

fn output_mode_for_role(role: &str) -> OutputMode {
    if role == "manager.research" {
        OutputMode::ResearchArtifact
    } else {
        OutputMode::JsonArtifact
    }
}

fn is_critical_role(config: &RuntimeConfig, role: &str) -> bool {
    config.workflow.critical_roles.contains(role)
}

fn record_degraded_role(state: &mut Value, result: &RoleJobResult, message: &str) {
    state["degraded"] = Value::Bool(true);
    if !state.get("degraded_roles").is_some_and(Value::is_array) {
        state["degraded_roles"] = json!([]);
    }
    if let Some(items) = state["degraded_roles"].as_array_mut() {
        items.push(json!({
            "role": result.role,
            "phase": result.phase,
            "kind": result.kind,
            "round": result.round,
            "topic_id": result.topic_id,
            "timed_out": result.timed_out,
            "elapsed_ms": result.elapsed_ms,
            "message": message
        }));
    }
    if !state.get("missing_sources").is_some_and(Value::is_array) {
        state["missing_sources"] = json!([]);
    }
    if let Some(items) = state["missing_sources"].as_array_mut() {
        items.push(Value::String(result.role.clone()));
    }
}

fn degraded_role_artifact(result: &RoleJobResult, message: &str) -> Value {
    let per_ticker = result
        .tickers
        .iter()
        .map(|ticker| {
            (
                ticker.clone(),
                json!({
                    "status": "missing",
                    "direction": "neutral",
                    "confidence": 0.0,
                    "report": format!("{} did not produce usable evidence: {message}", result.role),
                    "error": message
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    json!({
        "id": result.role,
        "role": result.role,
        "phase": result.phase,
        "kind": result.kind,
        "round": result.round,
        "topic_id": result.topic_id,
        "status": "degraded",
        "degraded": true,
        "timed_out": result.timed_out,
        "elapsed_ms": result.elapsed_ms,
        "error": message,
        "per_ticker": per_ticker
    })
}

fn enforce_preflight_policy(state: &mut Value, role: &str, config: &RuntimeConfig) -> Result<()> {
    let Some(tool) = preflight_tool_for_role(role) else {
        return Ok(());
    };
    let Some(status) = preflight_status(state, tool) else {
        return Ok(());
    };
    if status.get("status").and_then(Value::as_str) == Some("error") {
        let message = status
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("preflight failed")
            .to_string();
        if is_critical_role(config, role) {
            bail!("critical preflight {tool} for role {role} failed: {message}");
        }
        if !state.get("degraded_roles").is_some_and(Value::is_array) {
            state["degraded_roles"] = json!([]);
        }
        if let Some(items) = state["degraded_roles"].as_array_mut() {
            items.push(json!({
                "role": role,
                "phase": 1,
                "kind": "preflight",
                "tool": tool,
                "message": message
            }));
        }
    }
    Ok(())
}

fn preflight_tool_for_role(role: &str) -> Option<&'static str> {
    match role {
        "analyst.technical" => Some("run_technical_indicators"),
        "analyst.news_macro" => Some("fetch_jin10_flash"),
        _ => None,
    }
}

async fn run_phase1_preflight(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    role: &str,
    config: &RuntimeConfig,
) -> Result<()> {
    match role {
        "analyst.technical" => run_technical_preflight(state).await,
        "analyst.news_macro" => run_jin10_preflight(conn, state, config).await,
        _ => Ok(()),
    }
}

async fn run_technical_preflight(state: &mut Value) -> Result<()> {
    if preflight_status(state, "run_technical_indicators").is_some() {
        return Ok(());
    }
    if state
        .get("tech_refresh_enabled")
        .and_then(Value::as_bool)
        .is_some_and(|enabled| !enabled)
    {
        record_preflight_result(
            state,
            "run_technical_indicators",
            Ok(json!({"status": "skipped", "reason": "tech_refresh_enabled=false"})),
        );
        return Ok(());
    }
    let db_path = state
        .get("db_path")
        .and_then(Value::as_str)
        .map(PathBuf::from);
    let result = crate::technical::run(crate::technical::TechnicalArgs {
        symbols: Some(tickers_from_state(state).join(",")),
        start: None,
        end: None,
        days: state
            .get("window_days")
            .and_then(Value::as_i64)
            .or(Some(60)),
        intervals: "1d,3h,20min".to_string(),
        db_path,
        model: None,
        api_key: None,
        timeout: None,
        sleep: None,
    })
    .await;
    record_preflight_result(state, "run_technical_indicators", result);
    Ok(())
}

async fn run_jin10_preflight(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    _config: &RuntimeConfig,
) -> Result<()> {
    if preflight_status(state, "fetch_jin10_flash").is_some() {
        return Ok(());
    }
    let result = crate::jin10::run(crate::jin10::Jin10Args {
        channel: None,
        vip: None,
        classify: None,
        lookback_hours: Some(24.0),
        pages: None,
        sleep: None,
        timeout: None,
        output: String::new(),
        jsonl: String::new(),
        pretty: false,
    })
    .await
    .and_then(|payload| {
        let imported = import_jin10_payload(conn, &payload)?;
        Ok(json!({
            "status": "success",
            "imported_rows": imported,
            "payload": payload
        }))
    });
    record_preflight_result(state, "fetch_jin10_flash", result);
    Ok(())
}

fn record_preflight_result(state: &mut Value, name: &str, result: Result<Value>) {
    if !state.get("preflight").is_some_and(Value::is_object) {
        state["preflight"] = json!({});
    }
    match result {
        Ok(mut value) => {
            if value.get("status").is_none() {
                value["status"] = Value::String("success".to_string());
            }
            state["preflight"][name] = value;
        }
        Err(error) => {
            state["degraded"] = Value::Bool(true);
            state["preflight"][name] = json!({
                "status": "error",
                "message": error.to_string()
            });
        }
    }
}

fn preflight_status<'a>(state: &'a Value, name: &str) -> Option<&'a Value> {
    state.get("preflight").and_then(|items| items.get(name))
}

#[allow(clippy::too_many_arguments)]
fn render_prompt(
    state: &Value,
    role: &str,
    phase: i64,
    kind: &str,
    round: Option<i64>,
    topic_id: Option<&str>,
    prompt_path: Option<&std::path::Path>,
) -> Result<String> {
    let tickers = tickers_from_state(state);
    let template = if let Some(path) = prompt_path {
        fs::read_to_string(path)
            .with_context(|| format!("failed to read prompt template {}", path.display()))?
    } else {
        "Return only artifact JSON for role {role}, kind {kind}, phase {phase}, and tickers {tickers}. Include per_ticker for every ticker.".to_string()
    };
    let current_topic_state = topic_id
        .and_then(|id| topic_state(state, id))
        .unwrap_or(Value::Null);
    let current_topic = current_topic_state
        .get("topic")
        .cloned()
        .unwrap_or(Value::Null);
    let current_controller = current_topic_state
        .get("controller_artifact")
        .cloned()
        .unwrap_or(Value::Null);
    let blocked_repeats = current_controller
        .get("blocked_repeats")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let next_agenda = current_controller
        .get("next_agenda")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let values = json!({
        "run_id": state.get("run_id").and_then(Value::as_str).unwrap_or(""),
        "ticker": state.get("ticker").and_then(Value::as_str).unwrap_or(""),
        "tickers": tickers.join(","),
        "date": state.get("current_date").and_then(Value::as_str).unwrap_or(""),
        "lang": state.get("lang").and_then(Value::as_str).unwrap_or("zh"),
        "window_days": state.get("window_days").cloned().unwrap_or(Value::Null),
        "role": role,
        "phase": phase,
        "kind": kind,
        "round": round.unwrap_or_default(),
        "topic_id": topic_id.unwrap_or(""),
        "topic": serde_json::to_string_pretty(&current_topic)?,
        "blocked_repeats": serde_json::to_string_pretty(&blocked_repeats)?,
        "next_agenda": serde_json::to_string_pretty(&next_agenda)?,
        "workflow_pattern": "Workflow -> Stage/Sub-workflow -> Agent workers -> Reducer -> state artifact"
    });
    Ok(replace_placeholders(&template, &values))
}

fn persist_artifact(
    conn: &mut rusqlite::Connection,
    state: &Value,
    phase: i64,
    role: &str,
    artifact: Value,
) -> Result<()> {
    persist_artifact_with_last_md(conn, state, phase, role, artifact, String::new())
}

fn persist_artifact_with_last_md(
    conn: &mut rusqlite::Connection,
    state: &Value,
    phase: i64,
    role: &str,
    artifact: Value,
    last_md: String,
) -> Result<()> {
    persist_agent_content(
        conn,
        state,
        PersistContent {
            phase,
            role,
            kind: "artifact",
            round: None,
            topic_id: None,
            artifact,
            last_md,
        },
    )
}

fn persist_message(
    conn: &mut rusqlite::Connection,
    state: &Value,
    phase: i64,
    role: &str,
    kind: &str,
    round: Option<i64>,
    artifact: Value,
) -> Result<()> {
    persist_message_with_topic(conn, state, phase, role, kind, round, None, artifact)
}

#[allow(clippy::too_many_arguments)]
fn persist_message_with_topic(
    conn: &mut rusqlite::Connection,
    state: &Value,
    phase: i64,
    role: &str,
    kind: &str,
    round: Option<i64>,
    topic_id: Option<&str>,
    artifact: Value,
) -> Result<()> {
    persist_agent_content(
        conn,
        state,
        PersistContent {
            phase,
            role,
            kind,
            round,
            topic_id,
            artifact,
            last_md: String::new(),
        },
    )
}

struct PersistContent<'a> {
    phase: i64,
    role: &'a str,
    kind: &'a str,
    round: Option<i64>,
    topic_id: Option<&'a str>,
    artifact: Value,
    last_md: String,
}

fn persist_agent_content(
    conn: &mut rusqlite::Connection,
    state: &Value,
    input: PersistContent<'_>,
) -> Result<()> {
    let tickers = tickers_from_state(state);
    debug!(
        role = input.role,
        phase = input.phase,
        kind = input.kind,
        round = input.round,
        topic_id = input.topic_id,
        "persisting agent content"
    );
    write_agent_message_scoped(
        conn,
        &AgentMessageInput {
            run_id: state["run_id"].as_str().unwrap_or_default().to_string(),
            phase: input.phase,
            role: input.role.to_string(),
            ticker: state["ticker"].as_str().unwrap_or_default().to_string(),
            tickers,
            skill: input.role.to_string(),
            kind: input.kind.to_string(),
            topic_id: input.topic_id.map(ToString::to_string),
            round: input.round,
            message_group_id: None,
            valid: true,
            content: input.artifact,
            last_md: input.last_md,
        },
    )?;
    Ok(())
}

fn set_phase_status(state: &mut Value, phase: i64, status: &str) {
    if !state.get("phase_status").is_some_and(Value::is_object) {
        state["phase_status"] = json!({});
    }
    state["phase_status"][phase.to_string()] = Value::String(status.to_string());
}

fn tickers_from_state(state: &Value) -> Vec<String> {
    state
        .get("tickers")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn write_json(path: &std::path::Path, value: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_string_pretty(value)?)?;
    Ok(())
}

fn write_final_summary(run_dir: &std::path::Path, state: &Value) -> Result<()> {
    let research = state.get("research_plan").unwrap_or(&Value::Null);
    let summary = format!(
        "# Stock Probability Summary\n\n- ticker: {}\n- rating: {}\n- long_probability: {}\n- short_probability: {}\n\n{}\n",
        state.get("ticker").and_then(Value::as_str).unwrap_or(""),
        research.get("rating").and_then(Value::as_str).unwrap_or(""),
        research.get("long_probability").map(Value::to_string).unwrap_or_default(),
        research.get("short_probability").map(Value::to_string).unwrap_or_default(),
        research.get("probability_rationale").and_then(Value::as_str).unwrap_or("")
    );
    fs::write(run_dir.join("final_summary.md"), summary)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_llm::web_search::{
        WebSearchContextSize, WebSearchMode, WebSearchProviderKind,
    };
    use orchestrator_llm::LlmRoute;

    fn test_llm_settings(native_web_search: bool) -> RoleLlmSettings {
        RoleLlmSettings {
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

    fn test_llm_roles<I>(roles: I) -> BTreeMap<String, RoleLlmSettings>
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
        let roles = required_llm_roles()
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

        let roles = llm_roles_from_config(&config).unwrap();
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
        let mut roles = required_llm_roles()
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

        let roles = llm_roles_from_config(&config).unwrap();
        let settings = &roles["manager.research"];
        assert_eq!(settings.model, "role-model");
        assert_eq!(settings.max_turns, Some(4));
        assert_eq!(settings.reasoning_effort.as_deref(), Some("low"));
        assert_eq!(settings.tools, vec!["read_run_context".to_string()]);
    }

    #[test]
    fn llm_roles_accept_deepseek_chat_completions_api() {
        let roles = required_llm_roles()
            .iter()
            .map(|role| ((*role).to_string(), json!({})))
            .collect::<serde_json::Map<_, _>>();
        let config = json!({
            "orchestrator": {
                "llm": {
                    "defaults": {
                        "route": "deepseek",
                        "model": "deepseek-chat",
                        "base_url": "https://api.deepseek.com/v1",
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

        let roles = llm_roles_from_config(&config).unwrap();
        let settings = &roles["analyst.technical"];

        assert_eq!(settings.route, LlmRoute::Deepseek);
        assert_eq!(settings.transport, orchestrator_llm::LlmTransport::Ws);
        assert_eq!(settings.model, "deepseek-chat");
        assert_eq!(
            settings.base_url.as_deref(),
            Some("https://api.deepseek.com/v1")
        );
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

        let web_search = web_search_by_role_from_config(&config, roles.iter()).unwrap();

        for config in web_search.values() {
            assert_eq!(config, &WebSearchConfig::default());
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

        let web_search = web_search_by_role_from_config(&config, roles.iter()).unwrap();

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
                    "provider": "tavily",
                    "baseUrl": "https://gateway.example.com/tavily",
                    "apiKey": "test-tavily-key",
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

        let web_search = web_search_by_role_from_config(&config, roles.iter()).unwrap();
        let role_config = &web_search["analyst.technical"];

        assert_eq!(role_config.mode, WebSearchMode::Cached);
        assert_eq!(role_config.provider, WebSearchProviderKind::Tavily);
        assert_eq!(
            role_config.base_url.as_deref(),
            Some("https://gateway.example.com/tavily")
        );
        assert_eq!(role_config.api_key.as_deref(), Some("test-tavily-key"));
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

        let err = web_search_by_role_from_config(&config, roles.iter()).unwrap_err();
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

        let web_search = web_search_by_role_from_config(&config, roles.iter()).unwrap();
        let role_config = &web_search["analyst.technical"];

        assert_eq!(role_config.mode, WebSearchMode::Live);
        assert_eq!(role_config.provider, WebSearchProviderKind::Exa);
        assert_eq!(role_config.api_key, None);
    }

    #[test]
    fn web_search_rejects_live_tavily_without_api_key() {
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

        let err = web_search_by_role_from_config(&config, roles.iter()).unwrap_err();
        let message = format!("{err:#}");

        assert!(message.contains("requires api_key"));
    }

    #[test]
    fn web_search_skips_tavily_validation_when_role_has_native_web_search() {
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
        let roles = BTreeMap::from([("analyst.technical".to_string(), test_llm_settings(true))]);

        let web_search = web_search_by_role_from_config(&config, roles.iter()).unwrap();

        assert_eq!(web_search["analyst.technical"].mode, WebSearchMode::Live);
        assert_eq!(
            web_search["analyst.technical"].provider,
            WebSearchProviderKind::Tavily
        );
        assert_eq!(web_search["analyst.technical"].api_key, None);
    }

    #[test]
    fn web_search_accepts_direct_api_key_without_requiring_env() {
        let config = json!({
            "orchestrator": {
                "web_search": {
                    "mode": "live",
                    "provider": "tavily",
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

        let web_search = web_search_by_role_from_config(&config, roles.iter()).unwrap();
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
        record_preflight_result(
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

        run_technical_preflight(&mut state).await.unwrap();

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
        for role in required_llm_roles() {
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
