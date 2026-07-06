use anyhow::{bail, Context, Result};
use orchestrator_core::{
    config_bool, config_get, config_int, config_str, config_strings, project_path,
};
use orchestrator_llm::{
    web_search::{validate_web_search_runtime_config, WebSearchConfig, WebSearchConfigOverride},
    OutputMode, RoleLlmSettings,
};
use orchestrator_sql::context_count;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use super::policy::{WorkflowPolicyMode, WorkflowPolicyThresholds};

#[derive(Debug, Clone)]
pub(crate) struct RuntimeConfig {
    pub llm_roles: BTreeMap<String, RoleLlmSettings>,
    pub web_search: BTreeMap<String, WebSearchConfig>,
    pub strict_sqlite: bool,
    pub required_contexts: Vec<String>,
    pub prompts: PromptConfig,
    pub workflow: WorkflowConfig,
    pub allocation: AllocationConfig,
}

#[derive(Debug, Clone)]
pub(crate) struct AllocationConfig {
    pub investable_tickers: Vec<String>,
    pub regime_signal: String,
    pub regime_thresholds: Vec<f64>,
    pub regime_labels: Vec<String>,
    pub correlation_window_days: usize,
    pub max_single_position: f64,
    pub vol_indicator: String,
}

#[derive(Debug, Clone)]
pub(crate) struct WorkflowConfig {
    pub phase1_parallelism: usize,
    pub agent_timeout_sec: u64,
    pub reducer_timeout_sec: u64,
    pub risk_rounds: i64,
    pub critical_roles: BTreeSet<String>,
    pub late_evidence_enabled: bool,
    pub policy_mode: WorkflowPolicyMode,
    pub policy_thresholds: WorkflowPolicyThresholds,
}

#[derive(Debug, Clone)]
pub(crate) struct PromptConfig {
    pub prompts: BTreeMap<String, PathBuf>,
    pub manager_research: PathBuf,
    pub trader: PathBuf,
    pub risk_aggressive: PathBuf,
    pub risk_conservative: PathBuf,
    pub risk_neutral: PathBuf,
    pub portfolio_manager: PathBuf,
    pub allocation_manager: PathBuf,
}

impl RuntimeConfig {
    pub fn from_value(config: &Value) -> Result<Self> {
        let mut prompts = BTreeMap::new();
        prompts.insert(
            "analyst.technical".to_string(),
            prompt_path(
                config,
                "orchestrator.prompts.analyst.technical",
                "prompts/analysts/technical.md",
            )?,
        );
        prompts.insert(
            "analyst.news_macro".to_string(),
            prompt_path(
                config,
                "orchestrator.prompts.analyst.news_macro",
                "prompts/analysts/news_macro.md",
            )?,
        );
        prompts.insert(
            "analyst.youtube".to_string(),
            prompt_path(
                config,
                "orchestrator.prompts.analyst.youtube",
                "prompts/analysts/youtube.md",
            )?,
        );
        prompts.insert(
            "analyst.reddit".to_string(),
            prompt_path(
                config,
                "orchestrator.prompts.analyst.reddit",
                "prompts/analysts/reddit.md",
            )?,
        );
        prompts.insert(
            "analyst.x".to_string(),
            prompt_path(
                config,
                "orchestrator.prompts.analyst.x",
                "prompts/analysts/x.md",
            )?,
        );
        prompts.insert(
            "researcher.bull.initial".to_string(),
            prompt_path(
                config,
                "orchestrator.prompts.phase2.bull_initial",
                "prompts/researchers/bull_initial.md",
            )?,
        );
        prompts.insert(
            "researcher.bull.interaction".to_string(),
            prompt_path(
                config,
                "orchestrator.prompts.phase2.bull_interaction",
                "prompts/researchers/bull_interaction.md",
            )?,
        );
        prompts.insert(
            "researcher.bear.initial".to_string(),
            prompt_path(
                config,
                "orchestrator.prompts.phase2.bear_initial",
                "prompts/researchers/bear_initial.md",
            )?,
        );
        prompts.insert(
            "researcher.bear.interaction".to_string(),
            prompt_path(
                config,
                "orchestrator.prompts.phase2.bear_interaction",
                "prompts/researchers/bear_interaction.md",
            )?,
        );
        prompts.insert(
            "mediator.topic".to_string(),
            prompt_path(
                config,
                "orchestrator.prompts.mediator.topic",
                "prompts/mediators/topic_generation.md",
            )?,
        );
        prompts.insert(
            "mediator.topic_controller".to_string(),
            prompt_path(
                config,
                "orchestrator.prompts.mediator.topic_controller",
                "prompts/mediators/topic_controller.md",
            )?,
        );
        let prompts_config = PromptConfig {
            prompts,
            manager_research: prompt_path(
                config,
                "orchestrator.prompts.manager.research",
                "prompts/managers/research_manager.md",
            )?,
            trader: prompt_path(
                config,
                "orchestrator.prompts.trader",
                "prompts/traders/trader.md",
            )?,
            risk_aggressive: prompt_path(
                config,
                "orchestrator.prompts.risk.aggressive",
                "prompts/risk/aggressive.md",
            )?,
            risk_conservative: prompt_path(
                config,
                "orchestrator.prompts.risk.conservative",
                "prompts/risk/conservative.md",
            )?,
            risk_neutral: prompt_path(
                config,
                "orchestrator.prompts.risk.neutral",
                "prompts/risk/neutral.md",
            )?,
            portfolio_manager: prompt_path(
                config,
                "orchestrator.prompts.portfolio.manager",
                "prompts/managers/portfolio_manager.md",
            )?,
            allocation_manager: prompt_path(
                config,
                "orchestrator.prompts.allocation.manager",
                "prompts/allocation/manager.md",
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
            prompts: prompts_config,
            workflow,
            allocation: AllocationConfig::from_value(config),
        })
    }
}

pub(crate) fn llm_roles_from_config(config: &Value) -> Result<BTreeMap<String, RoleLlmSettings>> {
    let value = config_get(config, "orchestrator.llm.roles")
        .context("orchestrator.llm.roles is required")?;
    let object = value
        .as_object()
        .context("orchestrator.llm.roles must be a map")?;
    let defaults = config_get(config, "orchestrator.llm.defaults");
    let mut roles = BTreeMap::new();
    for (role, role_value) in object {
        let mut effective = defaults
            .cloned()
            .unwrap_or_else(|| Value::String(String::new()));
        orchestrator_core::deep_merge(&mut effective, role_value.clone());
        normalize_llm_role_tools(&mut effective, role)?;
        let settings: RoleLlmSettings = serde_json::from_value(effective)
            .with_context(|| format!("invalid LLM config for role {role:?}"))?;
        roles.insert(role.clone(), settings);
    }
    for role in required_llm_roles() {
        let settings = roles
            .get(&role)
            .with_context(|| format!("missing LLM config for required role {role:?}"))?;
        settings.validate(&role)?;
    }
    Ok(roles)
}

pub(crate) fn normalize_llm_role_tools(value: &mut Value, role: &str) -> Result<()> {
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

pub(crate) fn web_search_by_role_from_config<'a>(
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
    validate_web_search_enum_field(object, path, "provider", &["exa", "mock"])?;
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

pub(crate) fn required_llm_roles() -> Vec<String> {
    let registry = orchestrator_core::role_registry::AgentRegistry::builtin();
    let mut ids = registry.all_role_ids();
    for id in [
        "researcher.bull.initial",
        "researcher.bear.initial",
        "researcher.bull.interaction",
        "researcher.bear.interaction",
        "mediator.topic",
        "mediator.topic_controller",
        "manager.research",
    ] {
        if !ids.contains(&id.to_string()) {
            ids.push(id.to_string());
        }
    }
    ids
}

impl WorkflowConfig {
    pub fn from_value(config: &Value) -> Self {
        let phase1_parallelism =
            config_int(config, "orchestrator.workflow.phase1.parallelism", 5).max(1) as usize;
        let agent_timeout_sec =
            config_int(config, "orchestrator.workflow.agent_timeout_sec", 300).max(1) as u64;
        let reducer_timeout_sec =
            config_int(config, "orchestrator.workflow.reducer_timeout_sec", 300).max(1) as u64;
        let risk_rounds = config_int(config, "orchestrator.runtime.max_risk_rounds", 1).max(1);
        let critical_roles = config_strings(
            config,
            "orchestrator.workflow.phase1.critical_roles",
            &["analyst.technical", "analyst.news_macro"],
        )
        .into_iter()
        .map(|role| normalize_phase1_role_name(&role))
        .collect::<BTreeSet<_>>();
        let late_evidence_enabled =
            config_bool(config, "orchestrator.workflow.late_evidence.enabled", true);
        Self {
            phase1_parallelism,
            agent_timeout_sec,
            reducer_timeout_sec,
            risk_rounds,
            critical_roles,
            late_evidence_enabled,
            policy_mode: policy_mode_from_config(config),
            policy_thresholds: policy_thresholds_from_config(config),
        }
    }
}

fn policy_mode_from_config(config: &Value) -> WorkflowPolicyMode {
    match config_str(config, "orchestrator.workflow.policy.mode", "selective")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "selective" | "gated" => WorkflowPolicyMode::Selective,
        _ => WorkflowPolicyMode::Legacy,
    }
}

fn policy_thresholds_from_config(config: &Value) -> WorkflowPolicyThresholds {
    let defaults = WorkflowPolicyThresholds::default();
    WorkflowPolicyThresholds {
        min_confidence: config_f64(
            config,
            "orchestrator.workflow.policy.min_confidence",
            defaults.min_confidence,
        ),
        neutral_probability_band: config_f64(
            config,
            "orchestrator.workflow.policy.neutral_probability_band",
            defaults.neutral_probability_band,
        ),
        max_volatility: config_f64(
            config,
            "orchestrator.workflow.policy.max_volatility",
            defaults.max_volatility,
        ),
        max_correlation: config_f64(
            config,
            "orchestrator.workflow.policy.max_correlation",
            defaults.max_correlation,
        ),
        max_position: config_f64(
            config,
            "orchestrator.workflow.policy.max_position",
            defaults.max_position,
        ),
    }
}

fn config_f64(config: &Value, path: &str, default: f64) -> f64 {
    config_get(config, path)
        .and_then(|value| value.as_f64().or_else(|| value.as_str()?.parse().ok()))
        .unwrap_or(default)
}

impl PromptConfig {
    pub fn analyst_path(&self, role: &str) -> Option<&std::path::Path> {
        self.prompts.get(role).map(|p| p.as_path())
    }

    pub fn path_for(&self, role: &str) -> Option<&PathBuf> {
        self.prompts.get(role)
    }
}

pub(crate) fn prompt_path(config: &Value, key: &str, default: &str) -> Result<PathBuf> {
    let path = project_path(config_str(config, key, default));
    if !path.exists() {
        bail!(
            "configured prompt path does not exist for {key}: {}",
            path.display()
        );
    }
    Ok(path)
}

pub(crate) fn normalize_phase1_role_name(role: &str) -> String {
    let registry = orchestrator_core::role_registry::AgentRegistry::builtin();
    registry.normalize_role_name(role)
}

pub(crate) fn config_weight(config: &Value, name: &str, cli_value: f64) -> f64 {
    config_get(config, &format!("orchestrator.analyst_weights.{name}"))
        .and_then(|value| value.as_f64())
        .unwrap_or(cli_value)
}

pub(crate) fn validate_sqlite_context(
    conn: &rusqlite::Connection,
    config: &RuntimeConfig,
) -> Result<()> {
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

pub(crate) fn output_mode_for_role(role: &str) -> OutputMode {
    if role == "manager.research" {
        OutputMode::ResearchArtifact
    } else {
        OutputMode::JsonArtifact
    }
}

pub(crate) fn is_critical_role(config: &RuntimeConfig, role: &str) -> bool {
    config.workflow.critical_roles.contains(role)
}
