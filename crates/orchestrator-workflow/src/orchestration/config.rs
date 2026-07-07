use anyhow::{bail, Context, Result};
use orchestrator_core::{
    config_bool, config_get, config_int, config_str, config_strings, project_path, AgentRegistry,
};
use orchestrator_llm::{
    llm_judge::JudgeConfig,
    truncation::TruncationConfig,
    web_search::{validate_web_search_runtime_config, WebSearchConfig, WebSearchConfigOverride},
    OutputMode, RoleLlmSettings,
};
use orchestrator_sql::context_count;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use tracing::warn;

use super::plugin_loader::{validate_plugins, ComponentRegistry, RolePluginRegistry};
use super::policy::{WorkflowPolicyMode, WorkflowPolicyThresholds};

// Prompt versioning convention:
// - v1 is the current/base prompt path and keeps backward compatibility with flat string config.
// - v2+ resolves to `<stem>_vN.md` beside the configured base prompt when that file exists.
// - Missing v2+ files fall back to the base prompt with a warning so rollout is non-breaking.
// - Old prompt revisions may be archived under `prompts/_archive/`.
// - Absent `version` fields default to v1.

#[derive(Debug, Clone)]
pub(crate) struct RuntimeConfig {
    pub llm_roles: BTreeMap<String, RoleLlmSettings>,
    pub web_search: BTreeMap<String, WebSearchConfig>,
    pub truncation: TruncationConfig,
    pub judge: JudgeConfig,
    pub strict_sqlite: bool,
    pub required_contexts: Vec<String>,
    pub prompts: PromptConfig,
    pub workflow: WorkflowConfig,
    pub allocation: AllocationConfig,
    pub plugins: PluginConfig,
    pub component_plugins: ComponentRegistry,
    pub role_plugins: RolePluginRegistry,
    pub agent_registry: AgentRegistry,
}

#[derive(Debug, Clone)]
pub(crate) struct PluginConfig {
    pub enabled: bool,
    pub components_dir: PathBuf,
    pub roles_dir: PathBuf,
    pub disabled_components: Vec<String>,
    pub extra_component_dirs: Vec<PathBuf>,
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
    pub skip_zero_weight_analysts: bool,
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

impl PluginConfig {
    pub fn from_value(config: &Value) -> Self {
        let components_dir = project_path(config_str(
            config,
            "orchestrator.plugins.components_dir",
            "prompts/components",
        ));
        let roles_dir = project_path(config_str(
            config,
            "orchestrator.plugins.roles_dir",
            "prompts/roles",
        ));
        let extra_component_dirs =
            config_strings(config, "orchestrator.plugins.extra_component_dirs", &[])
                .into_iter()
                .map(project_path)
                .collect();
        Self {
            enabled: config_bool(config, "orchestrator.plugins.enabled", true),
            components_dir,
            roles_dir,
            disabled_components: config_strings(
                config,
                "orchestrator.plugins.disabled_components",
                &[],
            ),
            extra_component_dirs,
        }
    }
}

impl RuntimeConfig {
    pub fn from_value(config: &Value) -> Result<Self> {
        let mut prompts = BTreeMap::new();
        let mut versions = BTreeMap::new();
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "analyst.technical",
            "orchestrator.prompts.analyst.technical",
            "prompts/analysts/technical.md",
        )?;
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "analyst.news_macro",
            "orchestrator.prompts.analyst.news_macro",
            "prompts/analysts/news_macro.md",
        )?;
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "analyst.youtube",
            "orchestrator.prompts.analyst.youtube",
            "prompts/analysts/youtube.md",
        )?;
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "analyst.reddit",
            "orchestrator.prompts.analyst.reddit",
            "prompts/analysts/reddit.md",
        )?;
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "analyst.x",
            "orchestrator.prompts.analyst.x",
            "prompts/analysts/x.md",
        )?;
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "researcher.bull.initial",
            "orchestrator.prompts.phase2.bull_initial",
            "prompts/researchers/bull_initial.md",
        )?;
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "researcher.bull.interaction",
            "orchestrator.prompts.phase2.bull_interaction",
            "prompts/researchers/bull_interaction.md",
        )?;
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "researcher.bear.initial",
            "orchestrator.prompts.phase2.bear_initial",
            "prompts/researchers/bear_initial.md",
        )?;
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "researcher.bear.interaction",
            "orchestrator.prompts.phase2.bear_interaction",
            "prompts/researchers/bear_interaction.md",
        )?;
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "mediator.topic",
            "orchestrator.prompts.mediator.topic",
            "prompts/mediators/topic_generation.md",
        )?;
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "mediator.topic_controller",
            "orchestrator.prompts.mediator.topic_controller",
            "prompts/mediators/topic_controller.md",
        )?;
        let (manager_research, manager_research_version) = prompt_entry(
            config,
            "orchestrator.prompts.manager.research",
            "prompts/managers/research_manager.md",
        )?;
        versions.insert("manager.research".to_string(), manager_research_version);
        let (trader, trader_version) = prompt_entry(
            config,
            "orchestrator.prompts.trader",
            "prompts/traders/trader.md",
        )?;
        versions.insert("trader".to_string(), trader_version);
        let (risk_aggressive, risk_aggressive_version) = prompt_entry(
            config,
            "orchestrator.prompts.risk.aggressive",
            "prompts/risk/aggressive.md",
        )?;
        versions.insert("risk.aggressive".to_string(), risk_aggressive_version);
        let (risk_conservative, risk_conservative_version) = prompt_entry(
            config,
            "orchestrator.prompts.risk.conservative",
            "prompts/risk/conservative.md",
        )?;
        versions.insert("risk.conservative".to_string(), risk_conservative_version);
        let (risk_neutral, risk_neutral_version) = prompt_entry(
            config,
            "orchestrator.prompts.risk.neutral",
            "prompts/risk/neutral.md",
        )?;
        versions.insert("risk.neutral".to_string(), risk_neutral_version);
        let (portfolio_manager, portfolio_manager_version) = prompt_entry(
            config,
            "orchestrator.prompts.portfolio.manager",
            "prompts/managers/portfolio_manager.md",
        )?;
        versions.insert("portfolio.manager".to_string(), portfolio_manager_version);
        let (allocation_manager, allocation_manager_version) = prompt_entry(
            config,
            "orchestrator.prompts.allocation.manager",
            "prompts/allocation/manager.md",
        )?;
        versions.insert("allocation.manager".to_string(), allocation_manager_version);
        let plugin_config = PluginConfig::from_value(config);
        let (component_plugins, role_plugins) = if plugin_config.enabled {
            let mut component_dirs = vec![plugin_config.components_dir.clone()];
            component_dirs.extend(plugin_config.extra_component_dirs.clone());
            let mut components = ComponentRegistry::discover_all(&component_dirs)?;
            components.disable_components(&plugin_config.disabled_components);
            let roles = RolePluginRegistry::discover_dir(&plugin_config.roles_dir)?;
            validate_plugins(&components, &roles)?;
            tracing::info!(
                component_plugins = components.components.len(),
                role_plugins = roles.roles.len(),
                "discovered prompt plugins"
            );
            (components, roles)
        } else {
            (ComponentRegistry::default(), RolePluginRegistry::default())
        };
        for plugin in role_plugins.roles.values() {
            prompts.insert(plugin.manifest.role_id.clone(), plugin.role_path());
            versions
                .entry(plugin.manifest.role_id.clone())
                .or_insert_with(|| "v1".to_string());
        }
        let prompts_config = PromptConfig {
            prompts,
            manager_research,
            trader,
            risk_aggressive,
            risk_conservative,
            risk_neutral,
            portfolio_manager,
            allocation_manager,
        };
        let mut llm_roles = llm_roles_from_config(config)?;
        merge_plugin_llm_role_defaults(config, &mut llm_roles, &role_plugins)?;
        let truncation = truncation_config_from_value(config);
        let judge = judge_config_from_value(config);
        let mut web_search = web_search_by_role_from_config(config, llm_roles.iter())?;
        for config in web_search.values_mut() {
            config.truncation = truncation.clone();
        }
        let workflow = WorkflowConfig::from_value_with_registry(config, &role_plugins);
        let mut agent_registry = AgentRegistry::builtin();
        agent_registry.extend_role_manifests(
            role_plugins
                .roles
                .values()
                .map(|plugin| (&plugin.manifest, plugin.role_path())),
        );
        Ok(Self {
            llm_roles,
            web_search,
            truncation,
            judge,
            strict_sqlite: config_bool(config, "orchestrator.data_source.strict_sqlite", true),
            required_contexts: config_strings(
                config,
                "orchestrator.data_source.required_contexts",
                &["technical"],
            ),
            prompts: prompts_config,
            workflow,
            allocation: AllocationConfig::from_value(config),
            plugins: plugin_config,
            component_plugins,
            role_plugins,
            agent_registry,
        })
    }
}

fn truncation_config_from_value(config: &Value) -> TruncationConfig {
    config_get(config, "orchestrator.llm.truncation")
        .map_or_else(TruncationConfig::default, |value| {
            serde_json::from_value::<TruncationConfig>(value.clone()).unwrap_or_default()
        })
}

fn judge_config_from_value(config: &Value) -> JudgeConfig {
    JudgeConfig {
        enabled: config_bool(config, "orchestrator.llm.judge.enabled", true),
        model: config_str(
            config,
            "orchestrator.llm.judge.model",
            orchestrator_llm::llm_judge::DEFAULT_JUDGE_MODEL,
        ),
        max_messages_per_turn: config_int(config, "orchestrator.llm.judge.max_messages_per_turn", 3)
            .max(0) as usize,
    }
}

fn merge_plugin_llm_role_defaults(
    config: &Value,
    roles: &mut BTreeMap<String, RoleLlmSettings>,
    plugins: &RolePluginRegistry,
) -> Result<()> {
    let defaults = config_get(config, "orchestrator.llm.defaults")
        .cloned()
        .unwrap_or_else(|| Value::String(String::new()));
    for plugin in plugins.roles.values() {
        if let Some(settings) = roles.get_mut(&plugin.manifest.role_id) {
            if settings.tools.is_empty() && !plugin.manifest.tools.is_empty() {
                settings.tools = plugin.manifest.tools.clone();
            }
            continue;
        }
        if plugin.manifest.tools.is_empty() {
            continue;
        }
        let mut effective = defaults.clone();
        if let Some(object) = effective.as_object_mut() {
            object.insert(
                "tools".to_string(),
                Value::Array(
                    plugin
                        .manifest
                        .tools
                        .iter()
                        .map(|tool| Value::String(tool.clone()))
                        .collect(),
                ),
            );
        }
        normalize_llm_role_tools(&mut effective, &plugin.manifest.role_id)?;
        let settings: RoleLlmSettings = serde_json::from_value(effective).with_context(|| {
            format!(
                "invalid LLM defaults for plugin role {:?}",
                plugin.manifest.role_id
            )
        })?;
        settings.validate(&plugin.manifest.role_id)?;
        roles.insert(plugin.manifest.role_id.clone(), settings);
    }
    Ok(())
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
        Self::from_value_with_registry(config, &RolePluginRegistry::default())
    }

    pub fn from_value_with_registry(config: &Value, role_plugins: &RolePluginRegistry) -> Self {
        let phase1_parallelism =
            config_int(config, "orchestrator.workflow.phase1.parallelism", 5).max(1) as usize;
        let agent_timeout_sec =
            config_int(config, "orchestrator.workflow.agent_timeout_sec", 300).max(1) as u64;
        let reducer_timeout_sec =
            config_int(config, "orchestrator.workflow.reducer_timeout_sec", 300).max(1) as u64;
        let risk_rounds = config_int(config, "orchestrator.runtime.max_risk_rounds", 1).max(1);
        let mut registry = AgentRegistry::builtin();
        registry.extend_role_manifests(
            role_plugins
                .roles
                .values()
                .map(|plugin| (&plugin.manifest, plugin.role_path())),
        );
        let critical_roles = config_strings(
            config,
            "orchestrator.workflow.phase1.critical_roles",
            &["analyst.technical", "analyst.news_macro"],
        )
        .into_iter()
        .map(|role| registry.normalize_role_name(&role))
        .collect::<BTreeSet<_>>();
        let late_evidence_enabled =
            config_bool(config, "orchestrator.workflow.late_evidence.enabled", true);
        let skip_zero_weight_analysts = config_bool(
            config,
            "orchestrator.workflow.phase1.skip_zero_weight",
            true,
        );
        Self {
            phase1_parallelism,
            agent_timeout_sec,
            reducer_timeout_sec,
            risk_rounds,
            critical_roles,
            late_evidence_enabled,
            policy_mode: policy_mode_from_config(config),
            policy_thresholds: policy_thresholds_from_config(config),
            skip_zero_weight_analysts,
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

pub(crate) fn prompt_version(config: &Value, key: &str) -> String {
    config_prompt_version(config, key).unwrap_or_else(|| "v1".to_string())
}

fn insert_prompt_entry(
    config: &Value,
    prompts: &mut BTreeMap<String, PathBuf>,
    versions: &mut BTreeMap<String, String>,
    role: &str,
    key: &str,
    default: &str,
) -> Result<()> {
    let (path, version) = prompt_entry(config, key, default)?;
    prompts.insert(role.to_string(), path);
    versions.insert(role.to_string(), version);
    Ok(())
}

fn prompt_entry(config: &Value, key: &str, default: &str) -> Result<(PathBuf, String)> {
    let version = config_prompt_version(config, key).unwrap_or_else(|| "v1".to_string());
    let path = prompt_path(config, key, default)?;
    Ok((path, version))
}

/// Extract path string from either old flat string config or `{ path, version }` config.
fn config_prompt_path(config: &Value, key: &str, default: &str) -> String {
    match config_get(config, key) {
        Some(Value::String(value)) => value.clone(),
        Some(Value::Object(object)) => object
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or(default)
            .to_string(),
        _ => default.to_string(),
    }
}

fn config_prompt_version(config: &Value, key: &str) -> Option<String> {
    config_get(config, key)
        .and_then(Value::as_object)
        .and_then(|object| object.get("version"))
        .and_then(Value::as_str)
        .filter(|version| !version.trim().is_empty())
        .map(|version| version.trim().to_string())
}

/// Resolve a prompt path with optional version suffix.
/// v1 returns the base path; v2+ tries `<stem>_vN.<ext>` and falls back to base.
pub(crate) fn resolve_versioned_prompt_path(base: &Path, version: Option<&str>) -> Result<PathBuf> {
    let version = version.unwrap_or("v1").trim();
    if version.is_empty() || version == "v1" {
        if !base.exists() {
            bail!("prompt path does not exist: {}", base.display());
        }
        return Ok(base.to_path_buf());
    }

    let Some(stem) = base.file_stem().and_then(|value| value.to_str()) else {
        bail!("invalid prompt path: {}", base.display());
    };
    let extension = base
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("md");
    let versioned = base.with_file_name(format!("{stem}_{version}.{extension}"));
    if versioned.exists() {
        return Ok(versioned);
    }

    if !base.exists() {
        bail!("prompt path does not exist: {}", base.display());
    }
    warn!(
        version,
        path = %base.display(),
        versioned_path = %versioned.display(),
        "versioned prompt not found, falling back to base"
    );
    Ok(base.to_path_buf())
}

pub(crate) fn prompt_path(config: &Value, key: &str, default: &str) -> Result<PathBuf> {
    let base = project_path(config_prompt_path(config, key, default));
    let version = config_prompt_version(config, key);
    let path = resolve_versioned_prompt_path(&base, version.as_deref())?;
    if !path.exists() {
        bail!(
            "configured prompt path does not exist for {key}: {}",
            path.display()
        );
    }
    Ok(path)
}

pub(crate) fn normalize_phase1_role_name(role: &str) -> String {
    let registry = AgentRegistry::builtin();
    registry.normalize_role_name(role)
}

pub(crate) fn config_weight(config: &Value, name: &str, cli_value: f64) -> f64 {
    let cli_default = match name {
        "technical" => 40.0,
        "news_macro" => 35.0,
        "youtube" => 8.0,
        "reddit" => 9.0,
        "x" => 8.0,
        _ => cli_value,
    };
    if (cli_value - cli_default).abs() > f64::EPSILON {
        return cli_value;
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_llm::truncation::TruncationStrategy;
    use serde_json::json;

    #[test]
    fn judge_config_parses_from_runtime_config_value() {
        let value = json!({
            "orchestrator": {
                "llm": {
                    "judge": {
                        "enabled": false,
                        "model": "judge-model",
                        "max_messages_per_turn": 7
                    }
                }
            }
        });

        let config = judge_config_from_value(&value);

        assert!(!config.enabled);
        assert_eq!(config.model, "judge-model");
        assert_eq!(config.max_messages_per_turn, 7);
    }

    #[test]
    fn judge_config_defaults_when_missing() {
        assert_eq!(judge_config_from_value(&json!({})), JudgeConfig::default());
    }

    #[test]
    fn truncation_config_parses_from_runtime_config_value() {
        let value = json!({
            "orchestrator": {
                "llm": {
                    "truncation": {
                        "tool_result_chars": 1234,
                        "context_fragment_chars": 5678,
                        "strategy": "hard",
                        "json": {
                            "preserve_fields": ["status", "role"],
                            "max_array_elements": 7
                        },
                        "text": {
                            "head_ratio": 0.7,
                            "tail_ratio": 0.3
                        }
                    }
                }
            }
        });

        let config = truncation_config_from_value(&value);

        assert_eq!(config.tool_result_chars, 1234);
        assert_eq!(config.context_fragment_chars, 5678);
        assert_eq!(config.strategy, TruncationStrategy::Hard);
        assert_eq!(config.json.preserve_fields, vec!["status", "role"]);
        assert_eq!(config.json.max_array_elements, 7);
        assert_eq!(config.text.head_ratio, 0.7);
        assert_eq!(config.text.tail_ratio, 0.3);
    }

    #[test]
    fn truncation_config_defaults_when_missing_or_invalid() {
        let missing = truncation_config_from_value(&json!({}));
        assert_eq!(missing, TruncationConfig::default());

        let invalid = truncation_config_from_value(&json!({
            "orchestrator": {"llm": {"truncation": {"strategy": "not-valid"}}}
        }));
        assert_eq!(invalid, TruncationConfig::default());
    }
}
