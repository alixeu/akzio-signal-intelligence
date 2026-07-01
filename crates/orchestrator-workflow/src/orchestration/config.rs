use anyhow::{bail, Context, Result};
use orchestrator_core::{config_bool, config_get, config_str, config_strings, project_path};
use orchestrator_llm::{
    web_search::{validate_web_search_runtime_config, WebSearchConfig, WebSearchConfigOverride},
    OutputMode, RoleLlmSettings,
};
use orchestrator_sql::context_count;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub(crate) struct RuntimeConfig {
    pub llm_roles: BTreeMap<String, RoleLlmSettings>,
    pub web_search: BTreeMap<String, WebSearchConfig>,
    pub strict_sqlite: bool,
    pub required_contexts: Vec<String>,
    pub prompts: PromptConfig,
    pub workflow: WorkflowConfig,
}

#[derive(Debug, Clone)]
pub(crate) struct WorkflowConfig {
    pub phase1_parallelism: usize,
    pub agent_timeout_sec: u64,
    pub reducer_timeout_sec: u64,
    pub critical_roles: BTreeSet<String>,
    pub late_evidence_enabled: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct PromptConfig {
    pub prompts: BTreeMap<String, PathBuf>,
    pub manager_research: PathBuf,
    pub memory_reflector: Option<PathBuf>,
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
            prompt_path_any(
                config,
                &[
                    "orchestrator.prompts.phase2.topic_generation",
                    "orchestrator.prompts.mediator.topic",
                ],
                "prompts/mediators/topic_generation.md",
            )?,
        );
        prompts.insert(
            "mediator.topic_controller".to_string(),
            prompt_path_any(
                config,
                &[
                    "orchestrator.prompts.phase25.topic_controller",
                    "orchestrator.prompts.mediator.topic_controller",
                ],
                "prompts/mediators/topic_controller.md",
            )?,
        );
        prompts.insert(
            "reducer.evidence".to_string(),
            prompt_path_any(
                config,
                &[
                    "orchestrator.prompts.reducers.evidence",
                    "orchestrator.prompts.reducer.evidence",
                ],
                "prompts/reducers/evidence.md",
            )?,
        );
        prompts.insert(
            "reducer.debate_final".to_string(),
            prompt_path_any(
                config,
                &["orchestrator.prompts.reducers.debate_final"],
                "prompts/reducers/debate_final.md",
            )?,
        );
        let prompts_config = PromptConfig {
            prompts,
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
            prompts: prompts_config,
            workflow,
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
        "reducer.evidence",
        "reducer.debate_final",
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

pub(crate) fn prompt_path_optional(config: &Value, key: &str) -> Result<Option<PathBuf>> {
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

pub(crate) fn prompt_path_any(config: &Value, keys: &[&str], default: &str) -> Result<PathBuf> {
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

pub(crate) fn config_int_any(config: &Value, keys: &[&str], default: i64) -> i64 {
    keys.iter()
        .find_map(|key| config_get(config, key).and_then(Value::as_i64))
        .unwrap_or(default)
}

pub(crate) fn config_strings_any(config: &Value, keys: &[&str], default: &[&str]) -> Vec<String> {
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
