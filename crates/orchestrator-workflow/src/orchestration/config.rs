use anyhow::{bail, Context, Result};
use orchestrator_core::{
    config_bool, config_get, config_int, config_str, config_strings, project_path,
    AuthorityRegistry,
};
use orchestrator_llm::{
    truncation::TruncationConfig,
    web_search::{validate_web_search_runtime_config, WebSearchConfig, WebSearchConfigOverride},
    RoleLlmSettings,
};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use tracing::warn;

use orchestrator_core::{validate_plugins, ComponentRegistry};

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
    pub prompts: PromptConfig,
    pub workflow: WorkflowConfig,
    pub allocation: AllocationConfig,
    pub alpaca_api_key: Option<String>,
    pub alpaca_api_secret: Option<String>,
    pub reflection: ReflectionConfig,
    pub retrieval: RetrievalConfig,
    pub store: StoreConfig,
    pub tool_managed: ToolManagedConfig,
    pub component_plugins: ComponentRegistry,
    /// Immutable FileStore ownership for every role/profile, captured in the
    /// run manifest before recovery is permitted.
    pub authority_registry: AuthorityRegistry,
}

#[derive(Debug, Clone)]
pub(crate) struct StoreConfig {
    /// Canonical absolute root derived from `orchestrator.store.root`.
    pub root: PathBuf,
    pub atomic_fsync: bool,
    pub stale_temp_age_sec: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ToolManagedConfig {
    pub max_write_calls_per_role: usize,
    pub max_summary_units_per_phase: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct RetrievalConfig {
    pub summary_page_limit: usize,
    pub detail_page_limit: usize,
    pub phase2_max_details: usize,
    pub phase3_max_details: usize,
    pub phase4_max_details: usize,
    pub phase5_max_details: usize,
    pub phase6_max_details: usize,
    pub reflection_max_details: usize,
}

impl RetrievalConfig {
    fn from_value(config: &Value) -> Self {
        let bounded = |key: &str, default| config_int(config, key, default).clamp(1, 100) as usize;
        Self {
            summary_page_limit: bounded("orchestrator.retrieval.summary_page_limit", 20),
            detail_page_limit: bounded("orchestrator.retrieval.detail_page_limit", 20),
            phase2_max_details: bounded("orchestrator.retrieval.phase2_max_details", 4),
            phase3_max_details: bounded("orchestrator.retrieval.phase3_max_details", 6),
            phase4_max_details: bounded("orchestrator.retrieval.phase4_max_details", 6),
            phase5_max_details: bounded("orchestrator.retrieval.phase5_max_details", 4),
            phase6_max_details: bounded("orchestrator.retrieval.phase6_max_details", 8),
            reflection_max_details: bounded("orchestrator.retrieval.reflection_max_details", 8),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PluginConfig {
    pub enabled: bool,
    pub components_dir: PathBuf,
    pub disabled_components: Vec<String>,
    pub extra_component_dirs: Vec<PathBuf>,
}

#[derive(Debug, Clone)]
pub(crate) struct AllocationConfig {
    pub investable_assets: Vec<String>,
    pub regime_signal: String,
    pub regime_thresholds: Vec<f64>,
    pub regime_labels: Vec<String>,
    pub max_single_position: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct ReflectionConfig {
    pub task_limit: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct WorkflowConfig {
    pub agent_timeout_sec: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct PromptConfig {
    pub prompts: BTreeMap<String, PathBuf>,
    /// The configured revision for each registered prompt. This is captured in
    /// FileStore run manifests without eagerly reading template bodies.
    pub versions: BTreeMap<String, String>,
}

impl PluginConfig {
    pub fn from_value(config: &Value) -> Self {
        let components_dir = project_path(config_str(
            config,
            "orchestrator.plugins.components_dir",
            "prompts/common/components",
        ));
        let extra_component_dirs =
            config_strings(config, "orchestrator.plugins.extra_component_dirs", &[])
                .into_iter()
                .map(project_path)
                .collect();
        Self {
            enabled: config_bool(config, "orchestrator.plugins.enabled", true),
            components_dir,
            disabled_components: config_strings(
                config,
                "orchestrator.plugins.disabled_components",
                &[],
            ),
            extra_component_dirs,
        }
    }
}

impl StoreConfig {
    pub fn from_value(config: &Value) -> Result<Self> {
        const PATH: &str = "orchestrator.store";
        let object = strict_optional_object(config, PATH)?;
        if let Some(object) = object {
            validate_known_fields(object, PATH, ["root", "atomic_fsync", "stale_temp_age_sec"])?;
        }
        let root = strict_string_or_default(object, PATH, "root", "outputs/store")?;
        Ok(Self {
            root: resolve_store_root_path(Path::new(&root))?,
            atomic_fsync: strict_bool_or_default(object, PATH, "atomic_fsync", true)?,
            stale_temp_age_sec: strict_u64_or_default(object, PATH, "stale_temp_age_sec", 3600)?,
        })
        .and_then(|config| {
            if config.stale_temp_age_sec == 0 {
                bail!("{PATH}.stale_temp_age_sec must be at least 1");
            }
            Ok(config)
        })
    }

    /// Resolve the one canonical store root for a run. This does not create or
    /// read the directory; PR1's FileStore runtime owns that side effect.
    pub fn resolve_root(&self, cli_override: Option<&Path>) -> Result<PathBuf> {
        match cli_override {
            Some(path) => resolve_store_root_path(path),
            None => Ok(self.root.clone()),
        }
    }
}

impl ToolManagedConfig {
    pub fn from_value(config: &Value) -> Result<Self> {
        const PATH: &str = "orchestrator.tool_managed";
        let object = strict_optional_object(config, PATH)?;
        if let Some(object) = object {
            validate_known_fields(
                object,
                PATH,
                ["max_write_calls_per_role", "max_summary_units_per_phase"],
            )?;
        }
        let max_write_calls_per_role =
            strict_bounded_usize(object, PATH, "max_write_calls_per_role", 20, 1, 1_000)?;
        let max_summary_units_per_phase =
            strict_bounded_usize(object, PATH, "max_summary_units_per_phase", 32, 1, 256)?;
        Ok(Self {
            max_write_calls_per_role,
            max_summary_units_per_phase,
        })
    }
}

fn strict_optional_object<'a>(
    config: &'a Value,
    path: &str,
) -> Result<Option<&'a Map<String, Value>>> {
    match config_get(config, path) {
        None => Ok(None),
        Some(Value::Object(object)) => Ok(Some(object)),
        Some(_) => bail!("{path} must be an object"),
    }
}

fn validate_known_fields(
    object: &Map<String, Value>,
    path: &str,
    allowed: impl IntoIterator<Item = &'static str>,
) -> Result<()> {
    let allowed = allowed.into_iter().collect::<BTreeSet<_>>();
    for key in object.keys() {
        if !allowed.contains(key.as_str()) {
            bail!("{path}.{key} is not a supported setting");
        }
    }
    Ok(())
}

fn strict_string_or_default(
    object: Option<&Map<String, Value>>,
    path: &str,
    field: &str,
    default: &str,
) -> Result<String> {
    let Some(value) = object.and_then(|object| object.get(field)) else {
        return Ok(default.to_string());
    };
    let value = value
        .as_str()
        .with_context(|| format!("{path}.{field} must be a string"))?;
    if value.is_empty() || value != value.trim() || value.contains('\0') {
        bail!("{path}.{field} must be a non-empty, trimmed path");
    }
    Ok(value.to_string())
}

fn strict_bool_or_default(
    object: Option<&Map<String, Value>>,
    path: &str,
    field: &str,
    default: bool,
) -> Result<bool> {
    let Some(value) = object.and_then(|object| object.get(field)) else {
        return Ok(default);
    };
    value
        .as_bool()
        .with_context(|| format!("{path}.{field} must be a boolean"))
}

fn strict_u64_or_default(
    object: Option<&Map<String, Value>>,
    path: &str,
    field: &str,
    default: u64,
) -> Result<u64> {
    let Some(value) = object.and_then(|object| object.get(field)) else {
        return Ok(default);
    };
    value
        .as_u64()
        .with_context(|| format!("{path}.{field} must be an unsigned integer"))
}

fn strict_bounded_usize(
    object: Option<&Map<String, Value>>,
    path: &str,
    field: &str,
    default: usize,
    min: usize,
    max: usize,
) -> Result<usize> {
    let value = strict_u64_or_default(object, path, field, default as u64)?;
    let value: usize = value
        .try_into()
        .map_err(|_| anyhow::anyhow!("{path}.{field} is too large"))?;
    if !(min..=max).contains(&value) {
        bail!("{path}.{field} must be between {min} and {max}; got {value}");
    }
    Ok(value)
}

fn resolve_store_root_path(path: &Path) -> Result<PathBuf> {
    if path.as_os_str().is_empty() {
        bail!("--store-root must be a non-empty path");
    }
    Ok(project_path(path))
}

impl RuntimeConfig {
    pub fn from_value(config: &Value) -> Result<Self> {
        let mut prompts = BTreeMap::new();
        let mut versions = BTreeMap::new();
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "reflector.historical",
            "orchestrator.prompts.reflection.historical",
            "prompts/phase0/historical_reflection.md",
        )?;
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "analyst.technical",
            "orchestrator.prompts.analyst.technical",
            "prompts/phase1/technical.md",
        )?;
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "analyst.news_macro",
            "orchestrator.prompts.analyst.news_macro",
            "prompts/phase1/news_macro.md",
        )?;
        // Phase 2 researchers share one debate template; runtime kind selects
        // the initial claim packet or a routed point-debate packet.
        const PHASE2_WARMUP_PROMPT: &str = "prompts/phase2/researcher/warmup.md";
        const PHASE2_DEBATE_PROMPT: &str = "prompts/phase2/researcher/debate.md";
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "mediator.topic",
            "orchestrator.prompts.phase2.topic_generator",
            "prompts/phase2/topic_generator.md",
        )?;
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "researcher.warmup",
            "orchestrator.prompts.phase2.warmup",
            PHASE2_WARMUP_PROMPT,
        )?;
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "researcher.bull.initial",
            "orchestrator.prompts.phase2.bull_initial",
            PHASE2_DEBATE_PROMPT,
        )?;
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "researcher.bull.interaction",
            "orchestrator.prompts.phase2.bull_interaction",
            PHASE2_DEBATE_PROMPT,
        )?;
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "researcher.bear.initial",
            "orchestrator.prompts.phase2.bear_initial",
            PHASE2_DEBATE_PROMPT,
        )?;
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "researcher.bear.interaction",
            "orchestrator.prompts.phase2.bear_interaction",
            PHASE2_DEBATE_PROMPT,
        )?;
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "mediator.topic_controller",
            "orchestrator.prompts.mediator.topic_controller",
            "prompts/phase2/topic_controller.md",
        )?;
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "compressor.phase_summary",
            "orchestrator.prompts.compressor.phase_summary",
            "prompts/phase_summary/phase_summary.md",
        )?;
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "manager.research",
            "orchestrator.prompts.manager.research",
            "prompts/phase3/research_manager.md",
        )?;
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "trader",
            "orchestrator.prompts.trader",
            "prompts/phase4/trader.md",
        )?;
        for stance in ["aggressive", "neutral", "conservative"] {
            insert_prompt_entry(
                config,
                &mut prompts,
                &mut versions,
                &format!("risk.{stance}"),
                &format!("orchestrator.prompts.risk.{stance}"),
                &format!("prompts/phase5/{stance}.md"),
            )?;
        }
        insert_prompt_entry(
            config,
            &mut prompts,
            &mut versions,
            "portfolio.manager",
            "orchestrator.prompts.portfolio.manager",
            "prompts/phase6/portfolio_manager.md",
        )?;
        let plugin_config = PluginConfig::from_value(config);
        let component_plugins = if plugin_config.enabled {
            let mut component_dirs = vec![plugin_config.components_dir.clone()];
            component_dirs.extend(plugin_config.extra_component_dirs.clone());
            let mut components = ComponentRegistry::discover_all(&component_dirs)?;
            components.disable_components(&plugin_config.disabled_components);
            validate_plugins(&components)?;
            tracing::info!(
                component_plugins = components.components.len(),
                "discovered prompt plugins"
            );
            components
        } else {
            ComponentRegistry::default()
        };
        let prompts_config = PromptConfig { prompts, versions };
        let llm_roles = llm_roles_from_config(config)?;
        let truncation = truncation_config_from_value(config);
        let mut web_search = web_search_by_role_from_config(config, llm_roles.iter())?;
        for config in web_search.values_mut() {
            config.truncation = truncation.clone();
        }
        let workflow = WorkflowConfig::from_value(config);
        let authority_registry = AuthorityRegistry::builtin();
        let alpaca_api_key = config_str(config, "orchestrator.alpaca.api_key", "")
            .trim()
            .to_string();
        let alpaca_api_key = (!alpaca_api_key.is_empty()).then_some(alpaca_api_key);
        let alpaca_api_secret = config_str(config, "orchestrator.alpaca.api_secret", "")
            .trim()
            .to_string();
        let alpaca_api_secret = (!alpaca_api_secret.is_empty()).then_some(alpaca_api_secret);
        Ok(Self {
            llm_roles,
            web_search,
            truncation,
            prompts: prompts_config,
            workflow,
            allocation: AllocationConfig::from_value(config),
            alpaca_api_key,
            alpaca_api_secret,
            reflection: ReflectionConfig::from_value(config),
            retrieval: RetrievalConfig::from_value(config),
            store: StoreConfig::from_value(config)?,
            tool_managed: ToolManagedConfig::from_value(config)?,
            component_plugins,
            authority_registry,
        })
    }
}

fn truncation_config_from_value(config: &Value) -> TruncationConfig {
    config_get(config, "orchestrator.llm.truncation")
        .map_or_else(TruncationConfig::default, |value| {
            serde_json::from_value::<TruncationConfig>(value.clone()).unwrap_or_default()
        })
}

pub(crate) fn llm_roles_from_config(config: &Value) -> Result<BTreeMap<String, RoleLlmSettings>> {
    let defaults = config_get(config, "orchestrator.llm.defaults");
    let role_values = effective_llm_role_values(config)?;
    let mut roles = BTreeMap::new();
    for (role, role_value) in role_values {
        let mut effective = defaults
            .cloned()
            .unwrap_or_else(|| Value::String(String::new()));
        orchestrator_core::deep_merge(&mut effective, role_value);
        normalize_llm_role_tools(&mut effective, &role)?;
        let settings: RoleLlmSettings = serde_json::from_value(effective)
            .with_context(|| format!("invalid LLM config for role {role:?}"))?;
        roles.insert(role, settings);
    }
    for role in required_llm_roles() {
        let settings = roles
            .get(&role)
            .with_context(|| format!("missing LLM config for required role {role:?}"))?;
        settings.validate(&role)?;
    }
    Ok(roles)
}

fn effective_llm_role_values(config: &Value) -> Result<BTreeMap<String, Value>> {
    let configured_roles = config_get(config, "orchestrator.llm.roles")
        .map(|value| {
            value
                .as_object()
                .context("orchestrator.llm.roles must be a map")
        })
        .transpose()?;
    let mut role_values = builtin_llm_role_values();
    if let Some(object) = configured_roles {
        for (role, role_value) in object {
            role_values
                .entry(role.clone())
                .and_modify(|effective| {
                    orchestrator_core::deep_merge(effective, role_value.clone())
                })
                .or_insert_with(|| role_value.clone());
        }
    }
    Ok(role_values)
}

fn builtin_llm_role_values() -> BTreeMap<String, Value> {
    let mut roles = BTreeMap::new();
    for (role, max_turns, reasoning_effort, tools, web_search_live) in [
        (
            "reflector.historical",
            6,
            Some("medium"),
            vec![
                "read_reflection_source",
                "read_indexes",
                "read_index_details",
            ],
            false,
        ),
        (
            "analyst.technical",
            12,
            None,
            vec!["read_technical_snapshot", "read_technical_detail"],
            false,
        ),
        (
            "analyst.news_macro",
            6,
            None,
            vec!["read_jin10_candidates", "verify_event", "alpaca_get_news"],
            true,
        ),
        // Phase-2 roles read the compact index, then expand selected summaries.
        (
            "mediator.topic",
            6,
            Some("medium"),
            vec!["read_indexes", "read_index_details"],
            false,
        ),
        (
            "researcher.bull.initial",
            10,
            None,
            vec!["read_indexes", "read_index_details"],
            false,
        ),
        (
            "researcher.bear.initial",
            10,
            None,
            vec!["read_indexes", "read_index_details"],
            false,
        ),
        (
            "researcher.bull.interaction",
            10,
            None,
            vec!["read_indexes", "read_index_details"],
            false,
        ),
        (
            "researcher.bear.interaction",
            10,
            None,
            vec!["read_indexes", "read_index_details"],
            false,
        ),
        (
            "mediator.topic_controller",
            10,
            Some("medium"),
            vec!["read_indexes", "read_index_details"],
            false,
        ),
        (
            "manager.research",
            6,
            Some("medium"),
            vec!["read_indexes", "read_index_details"],
            false,
        ),
        ("compressor.phase_summary", 4, None, vec![], false),
        (
            "trader",
            6,
            None,
            vec!["read_indexes", "read_index_details"],
            false,
        ),
        (
            "risk.aggressive",
            6,
            None,
            vec!["read_indexes", "read_index_details"],
            false,
        ),
        (
            "risk.neutral",
            6,
            None,
            vec!["read_indexes", "read_index_details"],
            false,
        ),
        (
            "risk.conservative",
            6,
            None,
            vec!["read_indexes", "read_index_details"],
            false,
        ),
        (
            "portfolio.manager",
            8,
            Some("medium"),
            vec!["read_indexes", "read_index_details"],
            false,
        ),
    ] {
        let mut object = serde_json::Map::new();
        object.insert("max_turns".to_string(), Value::from(max_turns));
        if role == "mediator.topic" {
            // The topic artifact carries the shared analysis trace after three
            // evidence lookups, which exceeds the gateway's short default cap.
            object.insert("max_completion_tokens".to_string(), Value::from(8_192));
        }
        object.insert(
            "tools".to_string(),
            Value::Array(
                tools
                    .into_iter()
                    .map(|tool| Value::String(tool.to_string()))
                    .collect(),
            ),
        );
        if let Some(reasoning_effort) = reasoning_effort {
            object.insert(
                "reasoning_effort".to_string(),
                Value::String(reasoning_effort.to_string()),
            );
        }
        if web_search_live {
            object.insert(
                "web_search".to_string(),
                serde_json::json!({ "mode": "live" }),
            );
        } else if matches!(
            role,
            "researcher.bull.initial"
                | "researcher.bear.initial"
                | "researcher.bull.interaction"
                | "researcher.bear.interaction"
                | "mediator.topic"
                | "mediator.topic_controller"
        ) {
            object.insert(
                "web_search".to_string(),
                serde_json::json!({ "mode": "disabled" }),
            );
        }
        roles.insert(role.to_string(), Value::Object(object));
    }
    roles
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
    let role_values = effective_llm_role_values(config)?;
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
    [
        "analyst.technical",
        "analyst.news_macro",
        "reflector.historical",
        "mediator.topic",
        "researcher.bull.initial",
        "researcher.bear.initial",
        "researcher.bull.interaction",
        "researcher.bear.interaction",
        "mediator.topic_controller",
        "manager.research",
        "compressor.phase_summary",
    ]
    .into_iter()
    .map(ToString::to_string)
    .collect()
}

impl ReflectionConfig {
    pub fn from_value(config: &Value) -> Self {
        Self {
            task_limit: config_int(config, "orchestrator.reflection.task_limit", 10).max(1)
                as usize,
        }
    }
}

impl WorkflowConfig {
    pub fn from_value(config: &Value) -> Self {
        let agent_timeout_sec =
            config_int(config, "orchestrator.workflow.agent_timeout_sec", 300).max(1) as u64;
        Self { agent_timeout_sec }
    }
}

impl PromptConfig {
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

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_llm::truncation::TruncationStrategy;
    use serde_json::json;

    #[test]
    fn store_config_defaults_to_the_canonical_root_and_safe_retention() {
        let store = StoreConfig::from_value(&json!({})).unwrap();
        assert_eq!(store.root, project_path("outputs/store"));
        assert!(store.atomic_fsync);
        assert_eq!(store.stale_temp_age_sec, 3600);
    }

    #[test]
    fn store_config_is_strict_and_cli_root_is_the_only_override() {
        let root = std::env::temp_dir().join("akzio-store-config-test");
        let store = StoreConfig::from_value(&json!({
            "orchestrator": {
                "store": {
                    "root": "outputs/isolated-store",
                    "atomic_fsync": false,
                    "stale_temp_age_sec": 42
                }
            }
        }))
        .unwrap();
        assert_eq!(store.root, project_path("outputs/isolated-store"));
        assert!(!store.atomic_fsync);
        assert_eq!(store.stale_temp_age_sec, 42);
        assert_eq!(store.resolve_root(Some(&root)).unwrap(), root);
    }

    #[test]
    fn store_config_rejects_unknown_invalid_and_future_fields() {
        for store in [
            json!({"root": "outputs/store", "unknown": true}),
            json!({"root": " outputs/store"}),
            json!({"stale_temp_age_sec": 0}),
        ] {
            let error =
                StoreConfig::from_value(&json!({"orchestrator": {"store": store}})).unwrap_err();
            assert!(error.to_string().contains("orchestrator.store"));
        }
    }

    #[test]
    fn tool_managed_config_defaults_and_enforces_bounded_repair_policy() {
        let defaults = ToolManagedConfig::from_value(&json!({})).unwrap();
        assert_eq!(defaults.max_write_calls_per_role, 20);
        assert_eq!(defaults.max_summary_units_per_phase, 32);

        let error = ToolManagedConfig::from_value(&json!({
            "orchestrator": {"tool_managed": {"unknown": 1}}
        }))
        .unwrap_err();
        assert!(error.to_string().contains("orchestrator.tool_managed"));
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

    #[test]
    fn role_tools_are_phase_scoped() {
        let roles = builtin_llm_role_values();
        assert_eq!(roles["compressor.phase_summary"]["tools"], json!([]));
        for role in [
            "trader",
            "risk.aggressive",
            "risk.neutral",
            "risk.conservative",
            "portfolio.manager",
        ] {
            assert_eq!(
                roles[role]["tools"],
                json!(["read_indexes", "read_index_details"]),
                "role={role}"
            );
        }
        assert_eq!(
            roles["analyst.news_macro"]["tools"],
            json!(["read_jin10_candidates", "verify_event", "alpaca_get_news"])
        );
        for role in ["researcher.bull.initial", "researcher.bear.initial"] {
            assert_eq!(
                roles[role]["tools"],
                json!(["read_indexes", "read_index_details"]),
                "role={role}"
            );
            assert_eq!(roles[role]["web_search"]["mode"], "disabled");
        }
        for role in ["researcher.bull.interaction", "researcher.bear.interaction"] {
            assert_eq!(
                roles[role]["tools"],
                json!(["read_indexes", "read_index_details"]),
                "role={role}"
            );
        }
        assert_eq!(
            roles["reflector.historical"]["tools"],
            json!([
                "read_reflection_source",
                "read_indexes",
                "read_index_details"
            ])
        );
        for role in ["mediator.topic", "mediator.topic_controller"] {
            assert_eq!(
                roles[role]["tools"],
                json!(["read_indexes", "read_index_details"]),
                "role={role}"
            );
        }
        assert_eq!(
            roles["manager.research"]["tools"],
            json!(["read_indexes", "read_index_details"])
        );
    }
}
