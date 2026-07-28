use agent_loop::{
    AgentLoopConfig, AgentLoopModel, ModelEventHandler, ModelStreamEvent, ModelStreamResult,
    ProjectToolRuntime, RetrievalPolicy, ToolCallRequest, Turn,
};
use anyhow::{bail, Context, Result};
use async_openai::{config::OpenAIConfig, Client as OpenAIClient};
use futures::StreamExt;
use orchestrator_core::{default_project_root, ToolManagedProfile};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};
use tracing::debug;
use truncation::TruncationConfig;
use uuid::Uuid;
use web_search::{
    validate_web_search_runtime_config, ExaWebSearchProvider, WebSearchConfig, WebSearchMode,
};

pub mod agent_loop;
pub mod tools;
pub mod truncation;
pub mod web_search;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmRoute {
    Responses,
    ChatCompletions,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmTransport {
    #[default]
    Http,
    Ws,
}

/// Fixed endpoint, credentials, and model for the free opencode Zen gateway.
/// When a role sets `free_opencode`, the configured gateway base_url / api_key
/// and model are ignored in favor of these values (chat_completions only).
const FREE_OPENCODE_BASE_URL: &str = "https://opencode.ai/zen/v1";
const FREE_OPENCODE_API_KEY: &str = "public";
const FREE_OPENCODE_MODEL: &str = "deepseek-v4-flash-free";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleLlmSettings {
    pub route: LlmRoute,
    pub model: String,
    #[serde(default)]
    pub preamble: Option<String>,
    #[serde(default)]
    pub max_turns: Option<usize>,
    #[serde(default)]
    pub max_completion_tokens: Option<u32>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub reasoning_summary: Option<String>,
    #[serde(default)]
    pub preserve_reasoning_state: bool,
    #[serde(default)]
    pub text_verbosity: Option<String>,
    #[serde(default)]
    pub transport: LlmTransport,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub think_tool: bool,
    #[serde(default)]
    pub tools: Vec<String>,
    #[serde(default)]
    pub native_web_search: bool,
    /// When true, ignore the configured gateway base_url / api_key and model,
    /// and route every call through the free opencode Zen gateway using the
    /// chat_completions API with the model pinned to `deepseek-v4-flash-free`.
    #[serde(default, alias = "free-opencode")]
    pub free_opencode: bool,
}

impl RoleLlmSettings {
    pub fn validate(&self, role: &str) -> Result<()> {
        if !self.free_opencode && self.model.trim().is_empty() {
            bail!("LLM config for role {role:?} requires model");
        }
        if self.max_turns == Some(0) {
            bail!("LLM config for role {role:?} requires max_turns >= 1");
        }
        // free_opencode pins base_url / api_key to the opencode Zen gateway, so
        // the configured OpenAI-compatible endpoint credentials are not required.
        if !self.free_opencode
            && self
                .base_url
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
        {
            bail!("LLM config for role {role:?} requires base_url for openai_compatible");
        }
        let has_api_key = self
            .api_key
            .as_deref()
            .map(str::trim)
            .is_some_and(|value| !value.is_empty());
        if !self.free_opencode && !has_api_key {
            bail!("LLM config for role {role:?} requires api_key for openai_compatible");
        }
        for tool in &self.tools {
            validate_tool_name(tool)
                .with_context(|| format!("unknown tool name {tool:?} for role {role:?}"))?;
        }
        if let Some(effort) = &self.reasoning_effort {
            validate_reasoning_effort(effort)?;
        }
        if let Some(summary) = &self.reasoning_summary {
            validate_reasoning_summary(summary)?;
        }
        if let Some(verbosity) = &self.text_verbosity {
            validate_text_verbosity(verbosity)?;
            if self.route == LlmRoute::Responses {
                bail!("text_verbosity is only supported by the chat_completions route");
            }
        }
        Ok(())
    }

    pub fn effective_reasoning_effort<'a>(
        &'a self,
        override_effort: Option<&'a str>,
    ) -> Option<&'a str> {
        override_effort
            .filter(|value| !value.trim().is_empty())
            .or(self.reasoning_effort.as_deref())
    }

    pub fn effective_preamble(&self) -> Option<&str> {
        self.preamble
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }

    /// The route actually used for requests. free_opencode always calls the
    /// chat_completions API regardless of the configured route.
    pub fn effective_route(&self) -> LlmRoute {
        if self.free_opencode {
            LlmRoute::ChatCompletions
        } else {
            self.route
        }
    }

    /// The model actually sent to the provider. free_opencode pins the model to
    /// the free opencode model regardless of the configured value.
    pub fn effective_model(&self) -> &str {
        if self.free_opencode {
            FREE_OPENCODE_MODEL
        } else {
            self.model.as_str()
        }
    }
}

#[derive(Debug, Clone)]
pub struct AgentSettings {
    pub role: String,
    pub phase: Option<i64>,
    /// Optional topic identifier retained on every debug record.
    pub topic_id: Option<String>,
    /// Prompt source path relative to the project root, used to mirror debug output.
    pub debug_prompt_path: Option<PathBuf>,
    /// Optional debug output path relative to the project root for non-standard layouts.
    pub debug_output_path: Option<PathBuf>,
    /// Optional role-specific round number retained on every debug record.
    pub debug_round: Option<usize>,
    pub tickers: Vec<String>,
    /// Rust-owned draft/builder contract; assistant prose is never an artifact.
    pub tool_managed_profile: ToolManagedProfile,
    /// Concrete FileStore authority for this agent session.
    pub session_runtime: agent_loop::FileStoreSessionRuntime,
    /// Present only for an Index/Detail unit.
    pub index_tool_runtime: Option<tools::index_tools::IndexToolRuntimeBinding>,
    /// Present only for a business unit. It is a typed, scoped FileStore service.
    pub domain_tool_runtime: Option<tools::domain_tools::DomainToolRuntimeBinding>,
    /// Upper bound for typed Draft/Index write attempts in one agent turn.
    /// Reads and `think` do not consume this budget.
    pub max_write_calls: Option<usize>,
    pub llm: RoleLlmSettings,
    pub reasoning_effort_override: Option<String>,
    pub tools: Option<tools::ExternalToolConfig>,
    pub web_search: WebSearchConfig,
    pub truncation: TruncationConfig,
    pub debug: bool,
    pub retrieval_policy: RetrievalPolicy,
}

impl AgentSettings {
    fn validate_tool_managed(&self) -> Result<()> {
        if let Some(binding) = self.domain_tool_runtime.as_ref() {
            let profile = self.tool_managed_profile;
            if binding.scope().profile != profile {
                bail!(
                    "DomainToolRuntime profile {} differs from AgentSettings profile {}",
                    binding.scope().profile.as_str(),
                    profile.as_str()
                );
            }
        }
        Ok(())
    }

    fn reasoning_summary_as_enum(
        &self,
    ) -> Option<async_openai::types::responses::ReasoningSummary> {
        use async_openai::types::responses::ReasoningSummary;
        self.llm
            .reasoning_summary
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(|s| match s.to_ascii_lowercase().as_str() {
                "concise" => ReasoningSummary::Concise,
                "detailed" => ReasoningSummary::Detailed,
                _ => ReasoningSummary::Auto,
            })
    }
}

#[derive(Debug, Clone)]
pub struct SteerLoopInput<'a> {
    pub session_id: String,
    pub turn_id: String,
    pub prompt: &'a str,
    pub steer: Option<String>,
}

impl SteerLoopInput<'_> {
    fn steer_value(&self) -> Option<Value> {
        self.steer
            .as_deref()
            .and_then(|steer| serde_json::from_str(steer).ok())
    }

    fn fork_from_turn_id(&self) -> Option<String> {
        self.steer_value()?
            .get("fork_from_turn_id")?
            .as_str()
            .map(str::trim)
            .filter(|turn_id| !turn_id.is_empty())
            .map(ToString::to_string)
    }

    fn includes_prompt_on_fork(&self) -> bool {
        self.steer_value()
            .as_ref()
            .and_then(|value| value.get("include_prompt_on_fork"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone)]
pub struct AgentLoopOutput {
    pub artifact: Value,
    /// Full Rust-owned terminal result. Legacy roles leave it empty until
    /// migrated to ToolManaged.
    pub terminal_tool_result: Option<agent_loop::ToolResultItem>,
    pub metrics: ModelStreamResult,
    pub turn_id: String,
    pub session_id: String,
}

pub async fn run_agent_loop(settings: &AgentSettings, prompt: &str) -> Result<Value> {
    Ok(run_agent_loop_with_metrics(settings, prompt)
        .await?
        .artifact)
}

pub async fn run_agent_loop_with_metrics(
    settings: &AgentSettings,
    prompt: &str,
) -> Result<AgentLoopOutput> {
    settings.llm.validate(&settings.role)?;
    settings.validate_tool_managed()?;
    validate_fallback_web_search_runtime_config(settings)?;
    let session = &settings.session_runtime;
    let session_id = session.manifest().session_id.clone();
    let turn_id = format!("turn-{}", Uuid::new_v4());
    let mut turn = Turn::new(
        turn_id,
        session_id,
        session.manifest().run_id.clone(),
        settings.role.clone(),
        prompt.to_string(),
    );
    turn.phase = settings.phase;
    turn.tools_disabled = role_disables_tools(&settings.role);
    turn.model_context = format!(
        "role={}\nprofile={}\ntickers={}\navailable_tools={}",
        settings.role,
        settings.tool_managed_profile.as_str(),
        settings.tickers.join(","),
        serde_json::to_string(&configured_tool_names(settings))?
    );
    let tool_config = settings.tools.clone().unwrap_or_else(default_tool_config);
    let mut tools = ProjectToolRuntime::with_available_tools(
        tool_config,
        configured_tool_names(settings)
            .into_iter()
            .map(ToString::to_string)
            .collect(),
    );
    tools = tools.with_max_write_calls(settings.max_write_calls);
    if let Some(binding) = settings.index_tool_runtime.clone() {
        tools = tools.with_index_tool_runtime(binding);
    }
    if let Some(binding) = settings.domain_tool_runtime.clone() {
        tools = tools.with_domain_tool_runtime(binding);
    }
    if let Some(web_run) = web_run_runtime_for_settings(settings) {
        tools = tools.with_web_run_runtime(web_run);
    }
    let mut model = AgentLoopModel::new(settings.clone());
    let metrics = agent_loop::run_turn(
        session,
        &mut turn,
        &mut model,
        &mut tools,
        agent_loop_config_from_settings(settings),
    )
    .await?;
    let terminal_tool_result = turn.terminal_tool_result.clone();
    let artifact = completed_turn_artifact(&turn)?;
    Ok(AgentLoopOutput {
        artifact,
        terminal_tool_result,
        metrics,
        turn_id: turn.turn_id,
        session_id: turn.session_id,
    })
}

fn agent_loop_config_from_settings(settings: &AgentSettings) -> AgentLoopConfig {
    AgentLoopConfig {
        max_agent_loops: settings.llm.max_turns,
        truncation: settings.truncation.clone(),
        debug: settings.debug,
        project_root: Some(debug_project_root(settings)),
        role: settings.role.clone(),
        phase: settings.phase,
        model: settings.llm.effective_model().to_string(),
        topic_id: settings.topic_id.clone(),
        retrieval_policy: settings.retrieval_policy.clone(),
        require_terminal_tool: true,
        ..AgentLoopConfig::default()
    }
}

pub async fn run_agent_steer_loop(
    settings: &AgentSettings,
    input: SteerLoopInput<'_>,
) -> Result<Value> {
    Ok(run_agent_steer_loop_with_metrics(settings, input)
        .await?
        .artifact)
}

pub async fn run_agent_steer_loop_with_metrics(
    settings: &AgentSettings,
    input: SteerLoopInput<'_>,
) -> Result<AgentLoopOutput> {
    settings.llm.validate(&settings.role)?;
    settings.validate_tool_managed()?;
    validate_fallback_web_search_runtime_config(settings)?;
    let session = &settings.session_runtime;
    // Scope resume detection to this turn_id. Using run_id-latest history made
    // later phase-2 roles see sibling turns as "existing history" and drop their
    // own role prompt (live debate mass max_agent_loops / empty context).
    if session.manifest().session_id != input.session_id {
        bail!("steer input session does not match FileStore session authority");
    }
    let target_history = session_history_values(session.read_current_turn(&input.turn_id)?);
    let fork_from_turn_id = input.fork_from_turn_id();
    let fork_history = if target_history.is_empty() {
        if fork_from_turn_id.is_some() {
            let history = session_history_values(session.read_fork_turn()?);
            if history.is_empty() {
                bail!("FileStore fork source turn has no persisted history");
            }
            history
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };
    let is_new_fork = target_history.is_empty() && !fork_history.is_empty();
    let prior_history = select_fork_history(target_history, fork_history);
    let has_existing_history = !prior_history.is_empty();
    let include_prompt_on_fork = input.includes_prompt_on_fork();
    let (user_input, fork_input, pending_steer) = prepare_steer_turn_inputs(
        input.prompt,
        input.steer,
        has_existing_history,
        is_new_fork,
        include_prompt_on_fork,
    );
    let mut turn = Turn::new(
        input.turn_id.clone(),
        input.session_id.clone(),
        session.manifest().run_id.clone(),
        settings.role.clone(),
        user_input,
    );
    if has_existing_history {
        // Seed in-memory history so multi-round steer resumes do not wipe the
        // previous full_context snapshot on the next persist_turn.
        turn.emitted_items = prior_history
            .into_iter()
            .map(|value| {
                // Reuse agent-loop mapping via a thin JSON round-trip shape the
                // history loader already understands.
                agent_loop::turn_item_from_history_value(value)
            })
            .collect();
    }
    if let Some(fork_input) = fork_input {
        turn.emitted_items
            .push(agent_loop::TurnItem::user(fork_input));
    }
    turn.phase = settings.phase;
    turn.tools_disabled = role_disables_tools(&settings.role);
    turn.model_context = format!(
        "role={}\nprofile={}\ntickers={}\navailable_tools={}\nhistory_fork={}",
        settings.role,
        settings.tool_managed_profile.as_str(),
        settings.tickers.join(","),
        serde_json::to_string(&configured_tool_names(settings))?,
        fork_from_turn_id.is_some()
    );
    if let Some(steer) = pending_steer {
        turn.push_pending_input(steer);
    }
    let tool_config = settings.tools.clone().unwrap_or_else(default_tool_config);
    let mut tools = ProjectToolRuntime::with_available_tools(
        tool_config,
        configured_tool_names(settings)
            .into_iter()
            .map(ToString::to_string)
            .collect(),
    );
    tools = tools.with_max_write_calls(settings.max_write_calls);
    if let Some(binding) = settings.index_tool_runtime.clone() {
        tools = tools.with_index_tool_runtime(binding);
    }
    if let Some(binding) = settings.domain_tool_runtime.clone() {
        tools = tools.with_domain_tool_runtime(binding);
    }
    if let Some(web_run) = web_run_runtime_for_settings(settings) {
        tools = tools.with_web_run_runtime(web_run);
    }
    let mut model = AgentLoopModel::new(settings.clone());
    let metrics = agent_loop::run_turn(
        session,
        &mut turn,
        &mut model,
        &mut tools,
        agent_loop_config_from_settings(settings),
    )
    .await?;
    let terminal_tool_result = turn.terminal_tool_result.clone();
    let artifact = completed_turn_artifact(&turn)?;
    Ok(AgentLoopOutput {
        artifact,
        terminal_tool_result,
        metrics,
        turn_id: turn.turn_id,
        session_id: turn.session_id,
    })
}

/// Completion is terminal-tool-owned. Assistant prose is never an artifact.
fn completed_turn_artifact(turn: &Turn) -> Result<Value> {
    let terminal = turn
        .terminal_tool_result
        .as_ref()
        .context("tool-managed agent loop finished without terminal tool result")?;
    Ok(terminal
        .output
        .get("artifact")
        .cloned()
        .unwrap_or(Value::Null))
}

fn select_fork_history(target_history: Vec<Value>, fork_history: Vec<Value>) -> Vec<Value> {
    if target_history.is_empty() {
        fork_history
    } else {
        target_history
    }
}

fn session_history_values(events: Vec<orchestrator_store::SessionEvent>) -> Vec<Value> {
    events
        .into_iter()
        .filter_map(|event| {
            event
                .payload
                .get("item_type")
                .is_some()
                .then_some(event.payload)
        })
        .collect()
}

fn prepare_steer_turn_inputs(
    prompt: &str,
    steer: Option<String>,
    has_existing_history: bool,
    is_new_fork: bool,
    include_prompt_on_fork: bool,
) -> (String, Option<String>, Option<String>) {
    if is_new_fork && include_prompt_on_fork {
        let fork_input = match steer {
            Some(steer) => format!("{prompt}\n\nSteer: {steer}"),
            None => prompt.to_string(),
        };
        return (String::new(), Some(fork_input), None);
    }
    let user_input = if has_existing_history && !(is_new_fork && include_prompt_on_fork) {
        String::new()
    } else {
        prompt.to_string()
    };
    (user_input, None, steer)
}

pub fn append_debug_llm_record(settings: &AgentSettings, record: Value) -> Result<()> {
    if !settings.debug {
        return Ok(());
    }
    let root = settings
        .tools
        .as_ref()
        .map(|tools| tools.project_root.clone())
        .unwrap_or_else(default_project_root);
    let prompt_path = settings.debug_prompt_path.as_deref().ok_or_else(|| {
        anyhow::anyhow!(
            "debug LLM record for role {:?} requires debug_prompt_path",
            settings.role
        )
    })?;
    let output_path = settings
        .debug_output_path
        .as_deref()
        .map(validate_debug_output_relative_path)
        .transpose()?
        .unwrap_or(debug_record_relative_path_from_prompt(prompt_path)?);

    let mut record = record;
    if let Some(object) = record.as_object_mut() {
        object.entry("role").or_insert_with(|| json!(settings.role));
        object
            .entry("phase")
            .or_insert_with(|| json!(settings.phase));
        object
            .entry("topic_id")
            .or_insert_with(|| json!(settings.topic_id));
        object
            .entry("round")
            .or_insert_with(|| json!(settings.debug_round));
    }
    append_debug_output_record(&root, &output_path, &prompt_path.to_string_lossy(), record)
}

pub fn reset_debug_output_dir(project_root: &std::path::Path) -> Result<()> {
    let debug_dir = project_root.join("outputs/debug");
    if debug_dir.exists() {
        fs::remove_dir_all(&debug_dir)
            .with_context(|| format!("failed to clear debug dir {}", debug_dir.display()))?;
    }
    fs::create_dir_all(&debug_dir)
        .with_context(|| format!("failed to create debug dir {}", debug_dir.display()))?;
    Ok(())
}

/// Append one timing record to the formatted `outputs/debug/time.json` array.
pub fn append_debug_time_record(project_root: &std::path::Path, record: Value) -> Result<()> {
    append_debug_json_record(project_root, "outputs/debug/time.json", record)
}

/// Append one token-usage record to the formatted `outputs/debug/token.json` array.
pub fn append_debug_token_record(project_root: &std::path::Path, record: Value) -> Result<()> {
    append_debug_json_record(project_root, "outputs/debug/token.json", record)
}

fn append_debug_json_record(
    project_root: &std::path::Path,
    relative: &str,
    mut record: Value,
) -> Result<()> {
    let path = project_root.join(relative);
    with_debug_output_lock(|| {
        if let Some(object) = record.as_object_mut() {
            object
                .entry("ts_ms".to_string())
                .or_insert_with(|| json!(debug_now_ms()));
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create debug dir {}", parent.display()))?;
        }
        let mut records = if path.exists() {
            let contents = fs::read_to_string(&path)
                .with_context(|| format!("failed to read debug metrics {}", path.display()))?;
            serde_json::from_str::<Vec<Value>>(&contents)
                .with_context(|| format!("debug metrics {} must be a JSON array", path.display()))?
        } else {
            Vec::new()
        };
        records.push(record);
        fs::write(&path, serde_json::to_string_pretty(&records)?)
            .with_context(|| format!("failed to write debug metrics {}", path.display()))
    })
}

fn debug_now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

/// Resolve project root used for debug artifacts from settings.
pub fn debug_project_root(settings: &AgentSettings) -> PathBuf {
    settings
        .tools
        .as_ref()
        .map(|tools| tools.project_root.clone())
        .unwrap_or_else(default_project_root)
}

/// Best-effort time log; never fails the main workflow.
pub fn debug_log_time(project_root: &std::path::Path, record: Value) {
    if let Err(error) = append_debug_time_record(project_root, record) {
        tracing::warn!(error = %error, "failed to write debug time.json");
    }
}

/// Best-effort token log; never fails the main workflow.
pub fn debug_log_token(project_root: &std::path::Path, record: Value) {
    if let Err(error) = append_debug_token_record(project_root, record) {
        tracing::warn!(error = %error, "failed to write debug token.json");
    }
}

static DEBUG_OUTPUT_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn with_debug_output_lock<T>(write: impl FnOnce() -> Result<T>) -> Result<T> {
    let lock = DEBUG_OUTPUT_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock
        .lock()
        .map_err(|_| anyhow::anyhow!("debug output lock was poisoned"))?;
    write()
}

/// Resolve the prompt-mirrored JSON debug output path.
///
/// `prompts/phase1/news_macro.md` becomes `outputs/debug/phase1/news_macro.json`.
pub fn debug_record_relative_path_from_prompt(prompt_path: &Path) -> Result<PathBuf> {
    let mut components = prompt_path.components();
    if components.next() != Some(Component::Normal("prompts".as_ref()))
        || components.clone().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        bail!(
            "debug prompt path must be a relative path under prompts/: {}",
            prompt_path.display()
        );
    }
    if prompt_path
        .extension()
        .and_then(|extension| extension.to_str())
        != Some("md")
    {
        bail!(
            "debug prompt path must name a .md file: {}",
            prompt_path.display()
        );
    }
    let relative = prompt_path
        .strip_prefix("prompts")
        .expect("checked prompts path prefix");
    Ok(PathBuf::from("outputs/debug")
        .join(relative)
        .with_extension("json"))
}

fn validate_debug_output_relative_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        bail!(
            "debug output path must be relative to the project root: {}",
            path.display()
        );
    }
    Ok(path.to_path_buf())
}

/// Write the latest workflow-local or runtime debug request/response record.
///
/// `relative_output_path` is relative to the project root, typically below
/// `outputs/debug/`; `source_label` identifies a non-prompt producer such as `runtime`.
pub fn append_debug_output_record(
    project_root: &Path,
    relative_output_path: &Path,
    source_label: &str,
    record: Value,
) -> Result<()> {
    let relative_output_path = validate_debug_output_relative_path(relative_output_path)?;
    let source_label = source_label.trim();
    if source_label.is_empty() {
        bail!("debug source label must not be empty");
    }
    let path = project_root.join(relative_output_path);
    with_debug_output_lock(|| {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create debug dir {}", parent.display()))?;
        }
        let mut output = record;
        let object = output.as_object_mut().ok_or_else(|| {
            anyhow::anyhow!("debug record {} must be a JSON object", path.display())
        })?;
        if object
            .get("prompt_path")
            .is_some_and(|value| value.as_str() != Some(source_label))
        {
            bail!(
                "debug record {} has prompt_path {:?}, expected {:?}",
                path.display(),
                object.get("prompt_path"),
                source_label
            );
        }
        object.insert("prompt_path".to_string(), json!(source_label));
        fs::write(&path, serde_json::to_string_pretty(&output)?)
            .with_context(|| format!("failed to write debug record {}", path.display()))
    })
}

fn web_run_runtime(config: &WebSearchConfig) -> Option<tools::WebRunRuntime> {
    Some(
        tools::WebRunRuntime::new(config.clone())
            .with_truncation(TruncationConfig::default())
            .with_provider(Arc::new(ExaWebSearchProvider::from_config(config))),
    )
}

fn web_run_runtime_for_settings(settings: &AgentSettings) -> Option<tools::WebRunRuntime> {
    if uses_web_run_fallback(settings) {
        web_run_runtime(&settings.web_search)
            .map(|runtime| runtime.with_truncation(settings.truncation.clone()))
    } else {
        None
    }
}

fn uses_native_web_search(settings: &AgentSettings) -> bool {
    !role_disables_web_search(&settings.role) && settings.llm.native_web_search
}

fn uses_web_run_fallback(settings: &AgentSettings) -> bool {
    !role_disables_web_search(&settings.role)
        && !uses_native_web_search(settings)
        && settings.web_search.mode != WebSearchMode::Disabled
}

fn validate_fallback_web_search_runtime_config(settings: &AgentSettings) -> Result<()> {
    if uses_web_run_fallback(settings) {
        validate_web_search_runtime_config(&settings.web_search, &settings.role)
    } else {
        Ok(())
    }
}

async fn run_model_text_once(
    settings: &AgentSettings,
    input: &agent_loop::ModelInput,
    prompt: &str,
) -> Result<String> {
    match settings.llm.effective_route() {
        LlmRoute::Responses => run_responses_text_once(settings, input, prompt).await,
        LlmRoute::ChatCompletions => run_chat_completions_text_once(settings, input, prompt).await,
    }
}

pub async fn run_model_event_stream(
    settings: &AgentSettings,
    input: &agent_loop::ModelInput,
    prompt: &str,
    handler: &mut dyn ModelEventHandler,
) -> Result<()> {
    match settings.llm.effective_route() {
        LlmRoute::Responses => stream_responses_with_retry(settings, input, prompt, handler).await,
        LlmRoute::ChatCompletions => {
            stream_chat_completions_with_retry(settings, input, prompt, handler).await
        }
    }
}

async fn run_responses_text_once(
    settings: &AgentSettings,
    input: &agent_loop::ModelInput,
    prompt: &str,
) -> Result<String> {
    let client = openai_compatible_responses_client(&settings.llm)?;
    let request = build_responses_request(settings, input, prompt, false)?;
    let started = std::time::Instant::now();
    debug!(role = %settings.role, "sending non-streaming Responses API request");
    let response = client
        .responses()
        .create(request)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
        .context("OpenAI-compatible Responses prompt failed")?;
    let text = response.output_text().unwrap_or_default();
    debug!(
        role = %settings.role,
        elapsed_ms = started.elapsed().as_millis() as u64,
        response_len = text.len(),
        "non-streaming Responses API completed"
    );
    Ok(text)
}

async fn run_chat_completions_text_once(
    settings: &AgentSettings,
    input: &agent_loop::ModelInput,
    prompt: &str,
) -> Result<String> {
    let client = openai_compatible_responses_client(&settings.llm)?;
    let request = build_chat_completions_request(settings, input, prompt, false)?;
    let started = std::time::Instant::now();
    debug!(role = %settings.role, "sending non-streaming Chat Completions API request");
    let response = client
        .chat()
        .create(request)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
        .context("OpenAI-compatible Chat Completions prompt failed")?;
    let text = response
        .choices
        .first()
        .and_then(|c| c.message.content.clone())
        .unwrap_or_default();
    debug!(
        role = %settings.role,
        elapsed_ms = started.elapsed().as_millis() as u64,
        response_len = text.len(),
        "non-streaming Chat Completions API completed"
    );
    Ok(text)
}

fn build_responses_request(
    settings: &AgentSettings,
    input: &agent_loop::ModelInput,
    prompt: &str,
    with_tools: bool,
) -> Result<async_openai::types::responses::CreateResponse> {
    use async_openai::types::responses::*;

    let mut items: Vec<InputItem> = Vec::new();
    let mut seen_first_user = false;

    for item in &input.items {
        match item.item_type {
            agent_loop::TurnItemType::UserMessage => {
                if !seen_first_user {
                    seen_first_user = true;
                    continue;
                }
                let msg = EasyInputMessage {
                    role: Role::User,
                    content: EasyInputContent::Text(item.content_text.clone()),
                    ..Default::default()
                };
                items.push(InputItem::EasyMessage(msg));
            }
            agent_loop::TurnItemType::AssistantMessage => {
                if !item.content_text.is_empty() {
                    let msg = EasyInputMessage {
                        role: Role::Assistant,
                        content: EasyInputContent::Text(item.content_text.clone()),
                        ..Default::default()
                    };
                    items.push(InputItem::EasyMessage(msg));
                }
            }
            agent_loop::TurnItemType::ToolCall => {
                let call = item.content_json.get("call");
                let arguments = call
                    .and_then(|c| c.get("arguments"))
                    .cloned()
                    .unwrap_or(Value::Null);
                let name = call
                    .and_then(|c| c.get("name"))
                    .and_then(Value::as_str)
                    .unwrap_or(&item.tool_name)
                    .to_string();
                let call_id = item.tool_call_id.clone();
                if let Ok(tc) = serde_json::from_value::<InputItem>(json!({
                    "type": "function_call",
                    "call_id": call_id,
                    "name": name,
                    "arguments": arguments.to_string()
                })) {
                    items.push(tc);
                }
            }
            agent_loop::TurnItemType::ToolResult => {
                let content_text = truncation::truncate_semantic(
                    &item.content_text,
                    input.truncation.tool_result_chars,
                    &input.truncation,
                );
                if let Ok(tr) = serde_json::from_value::<InputItem>(json!({
                    "type": "function_call_output",
                    "call_id": item.tool_call_id,
                    "output": content_text
                })) {
                    items.push(tr);
                }
            }
            agent_loop::TurnItemType::ReasoningState => {
                if settings.llm.preserve_reasoning_state {
                    if let (Some(id), Some(encrypted)) = (
                        item.content_json
                            .get("output_item_id")
                            .and_then(Value::as_str),
                        item.content_json
                            .get("encrypted_content")
                            .and_then(Value::as_str),
                    ) {
                        if let Ok(ri) = serde_json::from_value::<InputItem>(json!({
                            "type": "reasoning",
                            "id": id,
                            "encrypted_content": encrypted,
                            "summary": []
                        })) {
                            items.push(ri);
                        }
                    }
                }
            }
            agent_loop::TurnItemType::CompactSummary => {
                let msg = EasyInputMessage {
                    role: Role::User,
                    content: EasyInputContent::Text(format!(
                        "[Compacted Context] {}",
                        item.content_text
                    )),
                    ..Default::default()
                };
                items.push(InputItem::EasyMessage(msg));
            }
            _ => {}
        }
    }

    let user_msg = EasyInputMessage {
        role: Role::User,
        content: EasyInputContent::Text(prompt.to_string()),
        ..Default::default()
    };
    items.push(InputItem::EasyMessage(user_msg));

    let item_count = items.len();
    let model = settings.llm.model.clone();
    let mut binding = CreateResponseArgs::default();
    let mut builder = binding.model(&model).input(InputParam::Items(items));

    let has_system = if let Some(system) = &input.system_instruction {
        builder = builder.instructions(system.clone());
        true
    } else if let Some(preamble) = settings.llm.effective_preamble() {
        builder = builder.instructions(preamble.to_string());
        true
    } else {
        false
    };

    let mut tool_count = 0;
    if with_tools {
        let tool_defs = tools::responses_tool_definitions(&input.available_tools);
        tool_count = tool_defs.len();
        if !tool_defs.is_empty() {
            builder = builder
                .tools(tool_defs)
                .tool_choice(ToolChoiceParam::Mode(ToolChoiceOptions::Auto));
        }
    }

    let has_reasoning = if let Some(reasoning) = build_reasoning_param(settings) {
        builder = builder.reasoning(reasoning);
        true
    } else {
        false
    };

    if settings.llm.preserve_reasoning_state {
        builder = builder.store(false);
        builder = builder.include(vec![IncludeEnum::ReasoningEncryptedContent]);
    }

    if uses_native_web_search(settings) {
        builder = builder.tools(vec![Tool::WebSearch(
            WebSearchToolArgs::default().build().unwrap(),
        )]);
        tool_count += 1;
    }

    debug!(
        model = %model,
        input_items = item_count,
        tools = tool_count,
        has_system,
        has_reasoning,
        preserve_reasoning_state = settings.llm.preserve_reasoning_state,
        streaming = with_tools,
        "built Responses API request"
    );

    builder
        .build()
        .map_err(|e| anyhow::anyhow!("{e}"))
        .context("failed to build response request")
}

fn build_chat_completions_request(
    settings: &AgentSettings,
    input: &agent_loop::ModelInput,
    prompt: &str,
    with_tools: bool,
) -> Result<async_openai::types::chat::CreateChatCompletionRequest> {
    use async_openai::types::chat::*;

    let mut messages: Vec<ChatCompletionRequestMessage> = Vec::new();

    if let Some(system) = &input.system_instruction {
        messages.push(ChatCompletionRequestMessage::System(
            ChatCompletionRequestSystemMessage {
                content: ChatCompletionRequestSystemMessageContent::Text(system.clone()),
                name: None,
            },
        ));
    } else if let Some(preamble) = settings.llm.effective_preamble() {
        messages.push(ChatCompletionRequestMessage::System(
            ChatCompletionRequestSystemMessage {
                content: ChatCompletionRequestSystemMessageContent::Text(preamble.to_string()),
                name: None,
            },
        ));
    }

    let mut seen_first_user = false;
    let mut pending_tool_calls = Vec::new();
    let flush_tool_calls =
        |messages: &mut Vec<ChatCompletionRequestMessage>,
         tool_calls: &mut Vec<ChatCompletionMessageToolCalls>| {
            if tool_calls.is_empty() {
                return;
            }
            #[allow(deprecated)]
            messages.push(ChatCompletionRequestMessage::Assistant(
                ChatCompletionRequestAssistantMessage {
                    content: None,
                    refusal: None,
                    name: None,
                    audio: None,
                    tool_calls: Some(std::mem::take(tool_calls)),
                    function_call: None,
                },
            ));
        };
    for item in &input.items {
        if item.item_type != agent_loop::TurnItemType::ToolCall {
            flush_tool_calls(&mut messages, &mut pending_tool_calls);
        }
        match item.item_type {
            agent_loop::TurnItemType::UserMessage => {
                if !seen_first_user {
                    seen_first_user = true;
                    continue;
                }
                messages.push(ChatCompletionRequestMessage::User(
                    ChatCompletionRequestUserMessage {
                        content: ChatCompletionRequestUserMessageContent::Text(
                            item.content_text.clone(),
                        ),
                        name: None,
                    },
                ));
            }
            agent_loop::TurnItemType::AssistantMessage => {
                if !item.content_text.is_empty() {
                    #[allow(deprecated)]
                    messages.push(ChatCompletionRequestMessage::Assistant(
                        ChatCompletionRequestAssistantMessage {
                            content: Some(ChatCompletionRequestAssistantMessageContent::Text(
                                item.content_text.clone(),
                            )),
                            refusal: None,
                            name: None,
                            audio: None,
                            tool_calls: None,
                            function_call: None,
                        },
                    ));
                }
            }
            agent_loop::TurnItemType::ToolCall => {
                let call = item.content_json.get("call");
                let arguments = call
                    .and_then(|c| c.get("arguments"))
                    .cloned()
                    .unwrap_or(Value::Null);
                let name = call
                    .and_then(|c| c.get("name"))
                    .and_then(Value::as_str)
                    .unwrap_or(&item.tool_name)
                    .to_string();
                let call_id = item.tool_call_id.clone();
                pending_tool_calls.push(ChatCompletionMessageToolCalls::Function(
                    ChatCompletionMessageToolCall {
                        id: call_id,
                        function: FunctionCall {
                            name,
                            arguments: arguments.to_string(),
                        },
                    },
                ));
            }
            agent_loop::TurnItemType::ToolResult => {
                let content_text = truncation::truncate_semantic(
                    &item.content_text,
                    input.truncation.tool_result_chars,
                    &input.truncation,
                );
                messages.push(ChatCompletionRequestMessage::Tool(
                    ChatCompletionRequestToolMessage {
                        content: ChatCompletionRequestToolMessageContent::Text(content_text),
                        tool_call_id: item.tool_call_id.clone(),
                    },
                ));
            }
            agent_loop::TurnItemType::CompactSummary => {
                messages.push(ChatCompletionRequestMessage::User(
                    ChatCompletionRequestUserMessage {
                        content: ChatCompletionRequestUserMessageContent::Text(format!(
                            "[Compacted Context] {}",
                            item.content_text
                        )),
                        name: None,
                    },
                ));
            }
            _ => {}
        }
    }
    flush_tool_calls(&mut messages, &mut pending_tool_calls);

    messages.push(ChatCompletionRequestMessage::User(
        ChatCompletionRequestUserMessage {
            content: ChatCompletionRequestUserMessageContent::Text(prompt.to_string()),
            name: None,
        },
    ));

    let msg_count = messages.len();
    let model = settings.llm.effective_model().to_string();
    let mut binding = CreateChatCompletionRequestArgs::default();
    let mut builder = binding.model(&model).messages(messages);

    if let Some(max_completion_tokens) = settings.llm.max_completion_tokens {
        // DeepSeek-compatible Chat Completions gateways use the legacy field.
        // The Responses route has its own request builder, so keep this
        // compatibility detail local to Chat Completions.
        #[allow(deprecated)]
        {
            builder = builder.max_tokens(max_completion_tokens);
        }
    }

    let mut tool_count = 0;
    if with_tools {
        let tool_defs = tools::chat_completions_tool_definitions(&input.available_tools);
        tool_count = tool_defs.len();
        if !tool_defs.is_empty() {
            builder = builder
                .tools(tool_defs)
                .tool_choice(ChatCompletionToolChoiceOption::Mode(
                    ToolChoiceOptions::Auto,
                ));
        }
    }

    let has_reasoning = if let Some(effort) = build_chat_reasoning_effort(settings) {
        builder = builder.reasoning_effort(effort);
        true
    } else {
        false
    };

    if let Some(verbosity) = &settings.llm.text_verbosity {
        let v = match verbosity.to_ascii_lowercase().as_str() {
            "low" => Verbosity::Low,
            "high" => Verbosity::High,
            _ => Verbosity::Medium,
        };
        builder = builder.verbosity(v);
    }

    builder = builder.stream_options(ChatCompletionStreamOptions {
        include_usage: Some(true),
        include_obfuscation: None,
    });

    debug!(
        model = %model,
        messages = msg_count,
        tools = tool_count,
        has_reasoning,
        streaming = with_tools,
        "built Chat Completions API request"
    );

    builder
        .build()
        .map_err(|e| anyhow::anyhow!("{e}"))
        .context("failed to build chat completion request")
}

fn build_chat_reasoning_effort(
    settings: &AgentSettings,
) -> Option<async_openai::types::chat::ReasoningEffort> {
    use async_openai::types::chat::ReasoningEffort;

    let effort = settings
        .llm
        .effective_reasoning_effort(settings.reasoning_effort_override.as_deref())?;
    let trimmed = effort.trim();
    if trimmed.is_empty() || is_zero_reasoning_effort(trimmed) {
        return None;
    }
    Some(match trimmed.to_ascii_lowercase().as_str() {
        "low" => ReasoningEffort::Low,
        "medium" => ReasoningEffort::Medium,
        "high" => ReasoningEffort::High,
        _ => ReasoningEffort::Medium,
    })
}

fn build_reasoning_param(
    settings: &AgentSettings,
) -> Option<async_openai::types::responses::Reasoning> {
    use async_openai::types::responses::{Reasoning, ReasoningEffort};

    let effort = settings
        .llm
        .effective_reasoning_effort(settings.reasoning_effort_override.as_deref());

    let summary = settings.reasoning_summary_as_enum();

    if effort.is_none() && summary.is_none() {
        return None;
    }

    let effort_enum = effort
        .map(str::trim)
        .filter(|v| !v.is_empty() && !is_zero_reasoning_effort(v))
        .map(|e| match e.to_ascii_lowercase().as_str() {
            "low" => ReasoningEffort::Low,
            "medium" => ReasoningEffort::Medium,
            "high" => ReasoningEffort::High,
            _ => ReasoningEffort::Medium,
        });

    Some(Reasoning {
        effort: effort_enum,
        summary,
    })
}

async fn stream_responses_with_retry(
    settings: &AgentSettings,
    input: &agent_loop::ModelInput,
    prompt: &str,
    handler: &mut dyn ModelEventHandler,
) -> Result<()> {
    const MAX_ATTEMPTS: usize = 5;
    let mut attempt = 0usize;
    loop {
        attempt += 1;
        match stream_responses_once(settings, input, prompt, handler).await {
            Ok(()) => return Ok(()),
            Err((error, made_progress))
                if attempt < MAX_ATTEMPTS && !made_progress && is_transient_llm_error(&error) =>
            {
                let backoff_ms = 1_000u64 * (1u64 << (attempt - 1)).min(8)
                    + retry_jitter_ms(&settings.role, attempt);
                tracing::warn!(
                    attempt,
                    backoff_ms,
                    error = %error,
                    error_chain = %format!("{error:#}"),
                    role = %settings.role,
                    "retrying transient LLM stream failure"
                );
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
            }
            Err((error, _)) => return Err(error),
        }
    }
}

fn is_transient_llm_error(error: &anyhow::Error) -> bool {
    let text = format!("{error:#}").to_ascii_lowercase();
    // Provider request IDs are numeric and can accidentally contain "502" or
    // "503". Quota exhaustion is unambiguously permanent, so reject it before
    // scanning the free-form error text for transient status fragments.
    if text.contains("insufficient_user_quota") || text.contains("额度已用完") {
        return false;
    }
    // Some gateways (e.g. the opencode free tier) wrap transient upstream
    // failures in a 400 invalid_request_error envelope. Evaluate explicit
    // transient signals first so they win over the permanent-error heuristic.
    let has_transient_signal = text.contains("503")
        || text.contains("502")
        || text.contains("429")
        || text.contains("bad_response_status_code")
        || text.contains("no healthy upstream")
        || text.contains("timeout")
        || text.contains("timed out")
        || text.contains("connection reset")
        || text.contains("transport error")
        || text.contains("error decoding response body")
        || text.contains("temporarily unavailable")
        || text.contains("upstream_error")
        || text.contains("upstream request failed");
    if has_transient_signal {
        return true;
    }
    // Permanent request/context errors must not burn stream retries.
    !is_permanent_llm_error_text(&text)
}

fn retry_jitter_ms(role: &str, attempt: usize) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    role.hash(&mut hasher);
    attempt.hash(&mut hasher);
    hasher.finish() % 251
}

fn is_permanent_llm_error_text(text: &str) -> bool {
    text.contains("insufficient_user_quota")
        || text.contains("额度已用完")
        || text.contains("context window is full")
        || text.contains("reduce conversation history")
        || text.contains("invalid_request_error")
        || text.contains("请精简对话历史")
        || text.contains("context window")
        || (text.contains("400")
            && (text.contains("invalid_request")
                || text.contains("context")
                || text.contains("too large")
                || text.contains("token")))
}

async fn stream_responses_once(
    settings: &AgentSettings,
    input: &agent_loop::ModelInput,
    prompt: &str,
    handler: &mut dyn ModelEventHandler,
) -> std::result::Result<(), (anyhow::Error, bool)> {
    use async_openai::types::responses::{OutputItem, ResponseStreamEvent};

    let started = std::time::Instant::now();
    let client = openai_compatible_responses_client(&settings.llm).map_err(|e| (e, false))?;
    let request = build_responses_request(settings, input, prompt, true).map_err(|e| (e, false))?;
    debug!(role = %settings.role, "opening streaming Responses API connection");
    let mut stream = client
        .responses()
        .create_stream(request)
        .await
        .map_err(|e| (anyhow::anyhow!("{e}").context("LLM stream failed"), false))?;
    debug!(
        role = %settings.role,
        connect_ms = started.elapsed().as_millis() as u64,
        "stream connected, reading events"
    );

    let mut text_item_id: Option<String> = None;
    let mut text_started = false;
    let mut saw_tool_call = false;
    let mut final_raw = Value::Null;
    let mut made_progress = false;
    let mut reasoning_item_id: Option<String> = None;
    let mut event_count: u64 = 0;
    let mut saw_response_completed = false;
    let mut function_call_meta: std::collections::HashMap<String, (String, String)> =
        std::collections::HashMap::new();
    let mut emitted_tool_calls: std::collections::HashSet<String> =
        std::collections::HashSet::new();

    while let Some(event) = stream.next().await {
        event_count += 1;
        let event = match event {
            Ok(ev) => ev,
            Err(e) => {
                return Err((
                    anyhow::anyhow!("{e}").context("LLM stream chunk failed"),
                    made_progress,
                ))
            }
        };
        match event {
            ResponseStreamEvent::ResponseOutputItemAdded(ev) => match &ev.item {
                OutputItem::Message(msg) => {
                    let item_id = msg.id.clone();
                    text_item_id = Some(item_id.clone());
                    if !text_started {
                        handler
                            .handle(ModelStreamEvent::AssistantMessageStarted {
                                item_id: item_id.clone(),
                            })
                            .await
                            .map_err(|e| (e, made_progress))?;
                        text_started = true;
                    }
                }
                OutputItem::FunctionCall(fc) => {
                    if let Some(id) = &fc.id {
                        function_call_meta
                            .insert(id.clone(), (fc.name.clone(), fc.call_id.clone()));
                    }
                }
                OutputItem::Reasoning(r) => {
                    reasoning_item_id =
                        r.id.clone()
                            .or_else(|| Some(format!("reasoning-{}", Uuid::new_v4())));
                }
                _ => {}
            },
            ResponseStreamEvent::ResponseOutputTextDelta(ev) => {
                made_progress = true;
                let item_id = text_item_id
                    .clone()
                    .unwrap_or_else(|| format!("msg-{}", Uuid::new_v4()));
                if !text_started {
                    text_item_id = Some(item_id.clone());
                    handler
                        .handle(ModelStreamEvent::AssistantMessageStarted {
                            item_id: item_id.clone(),
                        })
                        .await
                        .map_err(|e| (e, made_progress))?;
                    text_started = true;
                }
                handler
                    .handle(ModelStreamEvent::AssistantTextDelta {
                        item_id,
                        delta: ev.delta,
                    })
                    .await
                    .map_err(|e| (e, made_progress))?;
            }
            ResponseStreamEvent::ResponseOutputTextDone(_ev) => {
                if text_started {
                    let item_id = text_item_id
                        .clone()
                        .unwrap_or_else(|| format!("msg-{}", Uuid::new_v4()));
                    handler
                        .handle(ModelStreamEvent::AssistantMessageCompleted {
                            item_id,
                            turn_status: agent_loop::TurnStatus::Unknown,
                        })
                        .await
                        .map_err(|e| (e, made_progress))?;
                    text_started = false;
                    text_item_id = None;
                }
            }
            ResponseStreamEvent::ResponseReasoningSummaryTextDelta(ev) => {
                made_progress = true;
                let item_id = reasoning_item_id
                    .clone()
                    .unwrap_or_else(|| format!("reasoning-{}", Uuid::new_v4()));
                handler
                    .handle(ModelStreamEvent::ReasoningSummaryDelta {
                        item_id,
                        delta: ev.delta,
                    })
                    .await
                    .map_err(|e| (e, made_progress))?;
            }
            ResponseStreamEvent::ResponseReasoningSummaryTextDone(_ev) => {
                if let Some(item_id) = reasoning_item_id.clone() {
                    handler
                        .handle(ModelStreamEvent::ReasoningSummaryCompleted { item_id })
                        .await
                        .map_err(|e| (e, made_progress))?;
                }
            }
            ResponseStreamEvent::ResponseOutputItemDone(ev) => match &ev.item {
                OutputItem::Reasoning(r) => {
                    if let Ok(raw) = serde_json::to_value(r) {
                        if let Some(encrypted) = extract_encrypted_reasoning(&raw) {
                            let item_id =
                                r.id.clone()
                                    .unwrap_or_else(|| format!("reasoning-{}", Uuid::new_v4()));
                            made_progress = true;
                            handler
                                .handle(ModelStreamEvent::ReasoningStateCompleted {
                                    item_id,
                                    encrypted_content: encrypted,
                                })
                                .await
                                .map_err(|e| (e, made_progress))?;
                        }
                    }
                    reasoning_item_id = None;
                }
                OutputItem::FunctionCall(fc) => {
                    let item_id = fc.id.as_deref().unwrap_or(&fc.call_id);
                    if !emitted_tool_calls.contains(item_id) {
                        if fc.call_id.trim().is_empty() || fc.name.trim().is_empty() {
                            return Err((
                                    anyhow::anyhow!(
                                        "Responses tool call completed without call_id/name (item_id={item_id})"
                                    ),
                                    made_progress,
                                ));
                        }
                        made_progress = true;
                        saw_tool_call = true;
                        let arguments: Value = serde_json::from_str(&fc.arguments).map_err(|error| {
                                (
                                    anyhow::anyhow!(error).context(format!(
                                        "malformed Responses tool arguments for item_id={item_id}, call_id={}",
                                        fc.call_id
                                    )),
                                    made_progress,
                                )
                            })?;
                        let name = tools::resolve_tool_name(&fc.name);
                        emitted_tool_calls.insert(item_id.to_string());
                        handler
                            .handle(ModelStreamEvent::ToolCallCompleted {
                                tool_call: ToolCallRequest {
                                    call_id: fc.call_id.clone(),
                                    name,
                                    arguments,
                                },
                            })
                            .await
                            .map_err(|e| (e, made_progress))?;
                    }
                }
                _ => {}
            },
            ResponseStreamEvent::ResponseFunctionCallArgumentsDone(ev) => {
                if emitted_tool_calls.contains(&ev.item_id) {
                    continue;
                }
                made_progress = true;
                saw_tool_call = true;
                let arguments: Value = serde_json::from_str(&ev.arguments).map_err(|error| {
                    (
                        anyhow::anyhow!(error).context(format!(
                            "malformed Responses tool arguments for item_id={}",
                            ev.item_id
                        )),
                        made_progress,
                    )
                })?;
                let meta = function_call_meta.get(&ev.item_id);
                let name = ev
                    .name
                    .as_deref()
                    .filter(|n| !n.is_empty() && *n != "unknown")
                    .or_else(|| meta.map(|(n, _)| n.as_str()))
                    .ok_or_else(|| {
                        (
                            anyhow::anyhow!(
                                "Responses tool arguments completed without a tool name (item_id={})",
                                ev.item_id
                            ),
                            made_progress,
                        )
                    })?;
                let call_id = meta
                    .map(|(_, cid)| cid.clone())
                    .unwrap_or_else(|| ev.item_id.clone());
                if call_id.trim().is_empty() {
                    return Err((
                        anyhow::anyhow!(
                            "Responses tool arguments completed without call_id (item_id={})",
                            ev.item_id
                        ),
                        made_progress,
                    ));
                }
                emitted_tool_calls.insert(ev.item_id);
                handler
                    .handle(ModelStreamEvent::ToolCallCompleted {
                        tool_call: ToolCallRequest {
                            call_id,
                            name: tools::resolve_tool_name(name),
                            arguments,
                        },
                    })
                    .await
                    .map_err(|e| (e, made_progress))?;
            }
            ResponseStreamEvent::ResponseCompleted(ev) => {
                saw_response_completed = true;
                final_raw = response_usage_to_raw(&ev.response);
            }
            _ => {}
        }
    }

    debug!(
        role = %settings.role,
        elapsed_ms = started.elapsed().as_millis() as u64,
        event_count,
        has_text = text_started,
        has_tool_call = saw_tool_call,
        "stream completed"
    );

    if !saw_response_completed {
        return Err((
            anyhow::anyhow!(
                "Responses stream ended without response.completed after {event_count} events"
            ),
            made_progress,
        ));
    }

    if text_started {
        if let Some(item_id) = text_item_id.clone() {
            handler
                .handle(ModelStreamEvent::AssistantMessageCompleted {
                    item_id,
                    turn_status: agent_loop::TurnStatus::Unknown,
                })
                .await
                .map_err(|e| (e, made_progress))?;
        }
    }

    handler
        .handle(ModelStreamEvent::ResponseCompleted {
            end_turn: !saw_tool_call,
            raw: final_raw,
        })
        .await
        .map_err(|e| (e, made_progress))?;

    Ok(())
}

async fn stream_chat_completions_with_retry(
    settings: &AgentSettings,
    input: &agent_loop::ModelInput,
    prompt: &str,
    handler: &mut dyn ModelEventHandler,
) -> Result<()> {
    const MAX_ATTEMPTS: usize = 5;
    let mut attempt = 0usize;
    loop {
        attempt += 1;
        match stream_chat_completions_once(settings, input, prompt, handler).await {
            Ok(()) => return Ok(()),
            Err((error, made_progress))
                if attempt < MAX_ATTEMPTS && !made_progress && is_transient_llm_error(&error) =>
            {
                let backoff_ms = 1_000u64 * (1u64 << (attempt - 1)).min(8)
                    + retry_jitter_ms(&settings.role, attempt);
                tracing::warn!(
                    attempt,
                    backoff_ms,
                    error = %error,
                    error_chain = %format!("{error:#}"),
                    role = %settings.role,
                    "retrying transient Chat Completions stream failure"
                );
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
            }
            Err((error, _)) => return Err(error),
        }
    }
}

async fn stream_chat_completions_once(
    settings: &AgentSettings,
    input: &agent_loop::ModelInput,
    prompt: &str,
    handler: &mut dyn ModelEventHandler,
) -> std::result::Result<(), (anyhow::Error, bool)> {
    use async_openai::types::chat::FinishReason;

    let started = std::time::Instant::now();
    let client = openai_compatible_responses_client(&settings.llm).map_err(|e| (e, false))?;
    let request =
        build_chat_completions_request(settings, input, prompt, true).map_err(|e| (e, false))?;
    debug!(role = %settings.role, "opening streaming Chat Completions API connection");
    let mut stream = client.chat().create_stream(request).await.map_err(|e| {
        (
            anyhow::anyhow!("{e:?}").context("Chat Completions stream failed"),
            false,
        )
    })?;
    debug!(
        role = %settings.role,
        connect_ms = started.elapsed().as_millis() as u64,
        "chat completions stream connected, reading events"
    );

    let mut text_item_id: Option<String> = None;
    let mut text_started = false;
    let mut saw_tool_call = false;
    let mut final_raw = Value::Null;
    let mut made_progress = false;
    let mut event_count: u64 = 0;
    let mut saw_terminal_finish = false;

    struct PendingToolCall {
        id: String,
        name: String,
        arguments: String,
    }
    let mut pending_tool_calls: std::collections::HashMap<u32, PendingToolCall> =
        std::collections::HashMap::new();

    while let Some(event) = stream.next().await {
        event_count += 1;
        let chunk = match event {
            Ok(ev) => ev,
            Err(async_openai::error::OpenAIError::JSONDeserialize(err, _))
                if settings.llm.free_opencode =>
            {
                // The opencode Zen gateway interleaves non-standard SSE events
                // (e.g. `x-opencode-type: inference-cost`) that are not valid
                // OpenAI chunks. async-openai keeps the stream alive after a
                // per-event decode error, so skip what we cannot parse.
                debug!(
                    role = %settings.role,
                    error = %err,
                    "skipping unparsable free opencode stream event"
                );
                continue;
            }
            Err(e) => {
                return Err((
                    anyhow::anyhow!("{e}").context("Chat Completions stream chunk failed"),
                    made_progress,
                ))
            }
        };

        if let Some(usage) = &chunk.usage {
            let cached = usage
                .prompt_tokens_details
                .as_ref()
                .map(|d| d.cached_tokens.unwrap_or(0))
                .unwrap_or(0);
            let reasoning = usage
                .completion_tokens_details
                .as_ref()
                .and_then(|d| d.reasoning_tokens)
                .unwrap_or(0);
            final_raw = json!({
                "usage": {
                    "input_tokens": usage.prompt_tokens,
                    "output_tokens": usage.completion_tokens,
                    "total_tokens": usage.total_tokens,
                    "input_tokens_details": { "cached_tokens": cached },
                    "output_tokens_details": { "reasoning_tokens": reasoning }
                }
            });
        }

        for choice in &chunk.choices {
            let delta = &choice.delta;

            if let Some(content) = &delta.content {
                if !content.is_empty() {
                    made_progress = true;
                    let item_id = text_item_id
                        .clone()
                        .unwrap_or_else(|| format!("msg-{}", Uuid::new_v4()));
                    if !text_started {
                        text_item_id = Some(item_id.clone());
                        handler
                            .handle(ModelStreamEvent::AssistantMessageStarted {
                                item_id: item_id.clone(),
                            })
                            .await
                            .map_err(|e| (e, made_progress))?;
                        text_started = true;
                    }
                    handler
                        .handle(ModelStreamEvent::AssistantTextDelta {
                            item_id,
                            delta: content.clone(),
                        })
                        .await
                        .map_err(|e| (e, made_progress))?;
                }
            }

            if let Some(tool_calls) = &delta.tool_calls {
                for tc_chunk in tool_calls {
                    let idx = tc_chunk.index;
                    let pending =
                        pending_tool_calls
                            .entry(idx)
                            .or_insert_with(|| PendingToolCall {
                                id: String::new(),
                                name: String::new(),
                                arguments: String::new(),
                            });
                    if let Some(id) = &tc_chunk.id {
                        pending.id = id.clone();
                    }
                    if let Some(func) = &tc_chunk.function {
                        if let Some(name) = &func.name {
                            pending.name = name.clone();
                        }
                        if let Some(args) = &func.arguments {
                            pending.arguments.push_str(args);
                        }
                    }
                }
            }

            if let Some(finish_reason) = choice.finish_reason {
                let finish = format!("{finish_reason:?}");
                if finish.eq_ignore_ascii_case("length")
                    || finish.eq_ignore_ascii_case("contentfilter")
                    || finish.eq_ignore_ascii_case("content_filter")
                {
                    return Err((
                        anyhow::anyhow!(
                            "Chat Completions stream terminated with non-recoverable finish_reason={finish}"
                        ),
                        made_progress,
                    ));
                }
                if matches!(finish_reason, FinishReason::ToolCalls) {
                    saw_tool_call = true;
                }
                saw_terminal_finish = true;
            }
        }
    }

    if !(saw_terminal_finish || (settings.llm.free_opencode && made_progress)) {
        return Err((
            anyhow::anyhow!(
                "Chat Completions stream ended without a terminal finish_reason after {event_count} chunks"
            ),
            made_progress,
        ));
    }

    if text_started {
        if let Some(item_id) = text_item_id.clone() {
            handler
                .handle(ModelStreamEvent::AssistantMessageCompleted {
                    item_id,
                    turn_status: agent_loop::TurnStatus::Unknown,
                })
                .await
                .map_err(|e| (e, made_progress))?;
        }
    }

    let mut indices: Vec<u32> = pending_tool_calls.keys().copied().collect();
    indices.sort();
    for idx in indices {
        if let Some(tc) = pending_tool_calls.remove(&idx) {
            if tc.id.trim().is_empty() || tc.name.trim().is_empty() {
                return Err((
                    anyhow::anyhow!("Chat Completions tool call index {idx} ended without id/name"),
                    made_progress,
                ));
            }
            made_progress = true;
            saw_tool_call = true;
            let arguments: Value = serde_json::from_str(&tc.arguments).map_err(|error| {
                (
                    anyhow::anyhow!(error).context(format!(
                        "malformed Chat Completions tool arguments for index={idx}, call_id={}",
                        tc.id
                    )),
                    made_progress,
                )
            })?;
            let name = tools::resolve_tool_name(&tc.name);
            handler
                .handle(ModelStreamEvent::ToolCallCompleted {
                    tool_call: ToolCallRequest {
                        call_id: tc.id,
                        name,
                        arguments,
                    },
                })
                .await
                .map_err(|e| (e, made_progress))?;
        }
    }

    debug!(
        role = %settings.role,
        elapsed_ms = started.elapsed().as_millis() as u64,
        event_count,
        has_text = text_started,
        has_tool_call = saw_tool_call,
        "chat completions stream completed"
    );

    handler
        .handle(ModelStreamEvent::ResponseCompleted {
            end_turn: !saw_tool_call,
            raw: final_raw,
        })
        .await
        .map_err(|e| (e, made_progress))?;

    Ok(())
}

fn response_usage_to_raw(response: &async_openai::types::responses::Response) -> Value {
    match &response.usage {
        Some(usage) => {
            let cached = serde_json::to_value(&usage.input_tokens_details)
                .ok()
                .and_then(|v| v.get("cached_tokens").and_then(Value::as_u64))
                .unwrap_or(0);
            let reasoning = serde_json::to_value(&usage.output_tokens_details)
                .ok()
                .and_then(|v| v.get("reasoning_tokens").and_then(Value::as_u64))
                .unwrap_or(0);
            json!({
                "usage": {
                    "input_tokens": usage.input_tokens,
                    "output_tokens": usage.output_tokens,
                    "total_tokens": usage.total_tokens,
                    "input_tokens_details": { "cached_tokens": cached },
                    "output_tokens_details": { "reasoning_tokens": reasoning }
                }
            })
        }
        None => Value::Null,
    }
}

fn extract_encrypted_reasoning(raw: &Value) -> Option<String> {
    raw.get("content")
        .and_then(Value::as_array)
        .and_then(|arr| {
            arr.iter().find_map(|c| {
                c.get("encrypted_content")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .map(ToString::to_string)
            })
        })
}

fn openai_compatible_api_key(settings: &RoleLlmSettings) -> Result<String> {
    if let Some(api_key) = settings
        .api_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(api_key.to_string());
    }
    bail!("api_key is required for OpenAI-compatible provider")
}

fn openai_compatible_base_url(settings: &RoleLlmSettings) -> Result<&str> {
    settings
        .base_url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .context("base_url is required for OpenAI-compatible provider")
}

fn openai_compatible_responses_client(
    settings: &RoleLlmSettings,
) -> Result<OpenAIClient<OpenAIConfig>> {
    if settings.free_opencode {
        return free_opencode_client();
    }
    let api_key = openai_compatible_api_key(settings)?;
    let base_url = openai_compatible_base_url(settings)?;
    debug!(
        base_url = %base_url,
        model = %settings.model,
        "creating OpenAI-compatible responses client"
    );
    let config = OpenAIConfig::new()
        .with_api_key(api_key)
        .with_api_base(base_url);
    Ok(OpenAIClient::with_config(config))
}

/// Build a client for the free opencode Zen gateway. Every documented header
/// must be present or the gateway rejects the request: Authorization comes from
/// the fixed public api key, Content-Type is added per JSON request by the
/// client, and the remaining opencode headers are attached here.
fn free_opencode_client() -> Result<OpenAIClient<OpenAIConfig>> {
    let session = format!("sess_{}", Uuid::new_v4().simple());
    let request_id = format!("msg_{}", Uuid::new_v4().simple());
    debug!(
        base_url = %FREE_OPENCODE_BASE_URL,
        model = %FREE_OPENCODE_MODEL,
        "creating free opencode Zen chat completions client"
    );
    let config = OpenAIConfig::new()
        .with_api_base(FREE_OPENCODE_BASE_URL)
        .with_api_key(FREE_OPENCODE_API_KEY)
        .with_header("x-opencode-project", "proj_akzio_signal")
        .and_then(|config| config.with_header("x-opencode-session", session.as_str()))
        .and_then(|config| config.with_header("x-opencode-request", request_id.as_str()))
        .and_then(|config| config.with_header("x-opencode-client", "cli"))
        .and_then(|config| config.with_header("Accept", "text/event-stream"))
        .map_err(|e| anyhow::anyhow!("{e}"))
        .context("failed to set free opencode gateway headers")?;
    Ok(OpenAIClient::with_config(config))
}

pub fn additional_params(settings: &AgentSettings) -> Option<Value> {
    let mut params = openai_responses_reasoning_params(
        &settings.llm,
        settings
            .llm
            .effective_reasoning_effort(settings.reasoning_effort_override.as_deref()),
    );
    if uses_native_web_search(settings) {
        params = Some(add_openai_responses_native_web_search(params));
    }
    params
}

pub fn openai_responses_reasoning_params(
    settings: &RoleLlmSettings,
    effort: Option<&str>,
) -> Option<Value> {
    let mut params = serde_json::Map::new();
    let mut reasoning = serde_json::Map::new();
    if let Some(effort) = effort
        .map(str::trim)
        .filter(|value| !value.is_empty() && !is_zero_reasoning_effort(value))
    {
        reasoning.insert("effort".to_string(), json!(effort.to_ascii_lowercase()));
    }
    if let Some(summary) = settings
        .reasoning_summary
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        reasoning.insert("summary".to_string(), json!(summary.to_ascii_lowercase()));
    }
    if !reasoning.is_empty() {
        params.insert("reasoning".to_string(), Value::Object(reasoning));
    }
    if settings.preserve_reasoning_state {
        params.insert("store".to_string(), json!(false));
        params.insert(
            "include".to_string(),
            json!(["reasoning.encrypted_content"]),
        );
    }
    (!params.is_empty()).then_some(Value::Object(params))
}

fn add_openai_responses_native_web_search(params: Option<Value>) -> Value {
    let mut object = match params {
        Some(Value::Object(object)) => object,
        Some(other) => {
            let mut object = serde_json::Map::new();
            object.insert("value".to_string(), other);
            object
        }
        None => serde_json::Map::new(),
    };
    let mut tools = match object.remove("tools") {
        Some(Value::Array(tools)) => tools,
        _ => Vec::new(),
    };
    tools.push(json!({"type": "web_search"}));
    object.insert("tools".to_string(), Value::Array(tools));
    Value::Object(object)
}

fn configured_tool_names(settings: &AgentSettings) -> Vec<&str> {
    if role_disables_tools(&settings.role) {
        return Vec::new();
    }
    let mut names = Vec::new();
    if settings.llm.think_tool {
        names.push("think");
    }
    names.extend(
        settings
            .llm
            .tools
            .iter()
            .map(String::as_str)
            .filter(|name| {
                if *name == tools::ALPACA_GET_NEWS_TOOL_NAME {
                    settings
                        .tools
                        .as_ref()
                        .is_some_and(|config| config.alpaca_market_data)
                } else {
                    true
                }
            }),
    );
    if uses_web_run_fallback(settings) {
        names.push(tools::WEB_RUN_TOOL_NAME);
    }
    if let Some(binding) = &settings.index_tool_runtime {
        if binding.allows_write() {
            names.extend([
                tools::CREATE_INDEX_TOOL_NAME,
                tools::APPEND_INDEX_DETAIL_TOOL_NAME,
                tools::FINALIZE_INDEX_TOOL_NAME,
            ]);
        }
        names.extend([
            tools::READ_INDEXES_TOOL_NAME,
            tools::READ_INDEX_DETAILS_TOOL_NAME,
        ]);
        if !binding.allows_write() {
            names.retain(|name| {
                !matches!(
                    *name,
                    tools::CREATE_INDEX_TOOL_NAME
                        | tools::APPEND_INDEX_DETAIL_TOOL_NAME
                        | tools::FINALIZE_INDEX_TOOL_NAME
                )
            });
        }
    }
    // LLM role configuration can name the same read tool that a typed
    // runtime injects. The gateway rejects duplicate schemas, so normalize
    // the final Rust-owned allowlist before it is rendered or registered.
    let mut seen = BTreeSet::new();
    names.retain(|name| seen.insert(*name));
    names
}

fn role_disables_tools(role: &str) -> bool {
    let _ = role;
    false
}

fn role_disables_web_search(role: &str) -> bool {
    role == "trader" || role.starts_with("risk.") || role == "portfolio.manager"
}

fn validate_tool_name(name: &str) -> Result<()> {
    if tools::tool_definition(name).is_some() {
        Ok(())
    } else {
        bail!("unknown tool name: {name}")
    }
}

fn validate_reasoning_effort(value: &str) -> Result<()> {
    match value.trim().to_ascii_lowercase().as_str() {
        "0" | "none" | "minimal" | "low" | "medium" | "high" | "xhigh" => Ok(()),
        other => bail!("unsupported reasoning_effort {other:?}"),
    }
}

fn is_zero_reasoning_effort(value: &str) -> bool {
    matches!(value.trim().to_ascii_lowercase().as_str(), "0" | "none")
}

fn validate_reasoning_summary(value: &str) -> Result<()> {
    match value.trim().to_ascii_lowercase().as_str() {
        "auto" | "concise" | "detailed" => Ok(()),
        other => bail!("unsupported reasoning_summary {other:?}"),
    }
}

fn validate_text_verbosity(value: &str) -> Result<()> {
    match value.trim().to_ascii_lowercase().as_str() {
        "low" | "medium" | "high" => Ok(()),
        other => bail!("unsupported text_verbosity {other:?}"),
    }
}

fn default_tool_config() -> tools::ExternalToolConfig {
    tools::ExternalToolConfig {
        project_root: default_project_root(),
        run_id: None,
        phase: None,
        phase_summary_page_limit: 20,
        phase_summary_detail_page_limit: 20,
        tickers: std::env::var("ORCH_TICKERS")
            .ok()
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToString::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        alpaca_market_data: false,
        alpaca_api_key: None,
        alpaca_api_secret: None,
        file_store_input: None,
        file_store_reflection_source: None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        agent_loop, is_permanent_llm_error_text, is_transient_llm_error, tools, AgentSettings,
        LlmRoute, LlmTransport, RoleLlmSettings, ToolManagedProfile, TruncationConfig,
    };
    use crate::web_search::{WebSearchConfig, WebSearchMode};
    use anyhow::anyhow;
    use orchestrator_store::{FileStore, FileStoreOptions, RunLocation};
    use serde_json::{json, Value};
    use std::path::{Path, PathBuf};

    #[test]
    fn context_window_full_is_not_transient() {
        let err = anyhow!(
            "LLM stream chunk failed: InvalidStatusCodeWithMessage(400, \
             \"{{\\\"error\\\":{{\\\"message\\\":\\\"Context window is full — reduce conversation history\\\"\
             ,\\\"type\\\":\\\"invalid_request_error\\\"}}}}\")"
        );
        assert!(!is_transient_llm_error(&err));
        assert!(is_permanent_llm_error_text(
            &format!("{err:#}").to_ascii_lowercase()
        ));
    }

    #[test]
    fn gateway_502_upstream_is_transient() {
        let err = anyhow!(
            "LLM stream chunk failed: InvalidStatusCodeWithMessage(502, \
             \"{{\\\"error\\\":{{\\\"message\\\":\\\"Upstream request failed\\\",\\\"type\\\":\\\"upstream_error\\\"}}}}\")"
        );
        assert!(is_transient_llm_error(&err));
    }

    #[test]
    fn opencode_400_upstream_request_failed_is_transient() {
        // The opencode free tier wraps transient upstream failures in a 400
        // invalid_request_error envelope; it must still be retried.
        let err = anyhow!(
            "Chat Completions stream failed: ApiError(ApiErrorResponse {{ status_code: 400, \
             api_error: ApiError {{ message: \"Error from provider (Console): Upstream request failed\", \
             type: Some(\"invalid_request_error\"), param: None, code: Some(\"invalid_request_error\") }} }})"
        );
        assert!(is_transient_llm_error(&err));
    }

    #[test]
    fn insufficient_user_quota_is_not_transient() {
        let err = anyhow!(
            "Chat Completions stream failed: ApiError(ApiErrorResponse {{ status_code: 403, \
             api_error: ApiError {{ message: \"quota exhausted (request id: 2026072711094651059170850217812)\", type: Some(\"one_api_error\"), \
             param: Some(\"\"), code: Some(\"insufficient_user_quota\") }} }})"
        );
        assert!(!is_transient_llm_error(&err));
    }

    fn base_settings(route: LlmRoute) -> AgentSettings {
        AgentSettings {
            role: "manager.research".to_string(),
            phase: None,
            topic_id: None,
            debug_prompt_path: None,
            debug_output_path: None,
            debug_round: None,
            tickers: vec!["TQQQ".to_string()],
            tool_managed_profile: ToolManagedProfile::ResearchDecision,
            session_runtime: test_session_runtime(),
            index_tool_runtime: None,
            domain_tool_runtime: None,
            max_write_calls: None,
            llm: RoleLlmSettings {
                route,
                model: "gpt-5.4".to_string(),
                preamble: None,
                max_turns: Some(6),
                max_completion_tokens: None,
                reasoning_effort: Some("low".to_string()),
                reasoning_summary: None,
                preserve_reasoning_state: false,
                text_verbosity: None,
                transport: Default::default(),
                base_url: None,
                api_key: None,
                think_tool: true,
                tools: Vec::new(),
                native_web_search: false,
                free_opencode: false,
            },
            reasoning_effort_override: None,
            tools: None,
            web_search: WebSearchConfig::default(),
            truncation: TruncationConfig::default(),
            debug: false,
            retrieval_policy: agent_loop::RetrievalPolicy::default(),
        }
    }

    fn test_session_runtime() -> agent_loop::FileStoreSessionRuntime {
        let temp = tempfile::tempdir().unwrap();
        agent_loop::FileStoreSessionRuntime::create_or_load(
            FileStore::open(temp.path(), FileStoreOptions::default()).unwrap(),
            agent_loop::SessionRuntimeSpec {
                run: RunLocation::new("2026-07-27", "run-test").unwrap(),
                session_id: "session-test".to_owned(),
                role: "manager.research".to_owned(),
                phase: 3,
                profile: "research_decision".to_owned(),
                fork: None,
                created_at: "2026-07-27T00:00:00Z".to_owned(),
            },
        )
        .unwrap()
    }

    #[test]
    fn tool_managed_completion_needs_no_assistant_message() {
        let mut turn =
            agent_loop::Turn::new("turn-1", "session-1", "run-1", "manager.research", "");
        turn.terminal_tool_result = Some(agent_loop::ToolResultItem {
            call_id: "finalize-1".to_string(),
            name: "finalize_research_decision".to_string(),
            status: "completed".to_string(),
            output: json!({"terminal": true, "artifact": {"source": "terminal"}}),
            error: None,
        });

        assert_eq!(
            super::completed_turn_artifact(&turn).unwrap(),
            json!({"source": "terminal"})
        );
    }

    #[test]
    fn tool_managed_completion_ignores_assistant_text() {
        let mut turn =
            agent_loop::Turn::new("turn-1", "session-1", "run-1", "manager.research", "");
        turn.emitted_items.push(agent_loop::TurnItem::assistant(
            "this prose is deliberately not JSON",
            Value::Null,
        ));
        turn.terminal_tool_result = Some(agent_loop::ToolResultItem {
            call_id: "finalize-1".to_string(),
            name: "finalize_research_decision".to_string(),
            status: "completed".to_string(),
            output: json!({"terminal": true, "artifact": {"source": "terminal"}}),
            error: None,
        });

        assert_eq!(
            super::completed_turn_artifact(&turn).unwrap(),
            json!({"source": "terminal"})
        );
    }

    #[test]
    fn tool_managed_completion_rejects_missing_terminal() {
        let mut turn = agent_loop::Turn::new("turn-1", "session-1", "run-1", "custom.legacy", "");
        let prose = "Completed market assessment with no machine-readable artifact. ".repeat(4);
        turn.emitted_items
            .push(agent_loop::TurnItem::assistant(prose.clone(), Value::Null));

        assert!(super::completed_turn_artifact(&turn).is_err());
    }

    #[test]
    fn external_tool_names_are_registered() {
        assert_eq!(
            tools::tool_names(),
            vec![
                "read_reflection_source",
                "read_technical_snapshot",
                "read_technical_detail",
                "read_jin10_candidates",
                "verify_event",
                "alpaca_get_news",
            ]
        );
    }

    #[test]
    fn route_deserializes_supported_values() {
        assert_eq!(
            serde_json::from_value::<LlmRoute>(json!("responses")).unwrap(),
            LlmRoute::Responses
        );
        assert_eq!(
            serde_json::from_value::<LlmRoute>(json!("chat_completions")).unwrap(),
            LlmRoute::ChatCompletions
        );
        assert!(serde_json::from_value::<LlmRoute>(json!("deepseek")).is_err());
    }

    #[test]
    fn ws_transport_is_allowed_for_responses() {
        let responses = RoleLlmSettings {
            transport: LlmTransport::Ws,
            base_url: Some("https://llm.example.com/v1".to_string()),
            api_key: Some("test-key".to_string()),
            ..base_settings(LlmRoute::Responses).llm
        };
        responses.validate("manager.research").unwrap();
    }

    #[test]
    fn chat_completions_groups_consecutive_tool_calls_before_results() {
        let calls = [
            agent_loop::ToolCallRequest {
                call_id: "call-summary".to_string(),
                name: "read_indexes".to_string(),
                arguments: json!({"source_phase": 3}),
            },
            agent_loop::ToolCallRequest {
                call_id: "call-details".to_string(),
                name: "read_index_details".to_string(),
                arguments: json!({"index_id": "abc"}),
            },
        ];
        let results = [
            agent_loop::ToolResultItem {
                call_id: "call-summary".to_string(),
                name: "read_indexes".to_string(),
                status: "completed".to_string(),
                output: json!({"items": []}),
                error: None,
            },
            agent_loop::ToolResultItem {
                call_id: "call-details".to_string(),
                name: "read_index_details".to_string(),
                status: "completed".to_string(),
                output: json!({"items": []}),
                error: None,
            },
        ];
        let input = agent_loop::ModelInput {
            system_instruction: None,
            items: vec![
                agent_loop::TurnItem::user("portfolio role prompt"),
                agent_loop::TurnItem::tool_call(&calls[0]),
                agent_loop::TurnItem::tool_call(&calls[1]),
                agent_loop::TurnItem::tool_result(&results[0], &TruncationConfig::default()),
                agent_loop::TurnItem::tool_result(&results[1], &TruncationConfig::default()),
            ],
            available_tools: Vec::new(),
            truncation: TruncationConfig::default(),
        };

        let request = super::build_chat_completions_request(
            &base_settings(LlmRoute::ChatCompletions),
            &input,
            "produce final portfolio artifact",
            false,
        )
        .unwrap();
        let messages = serde_json::to_value(request).unwrap()["messages"].clone();

        assert_eq!(messages[0]["role"], "assistant");
        assert_eq!(messages[0]["tool_calls"].as_array().unwrap().len(), 2);
        assert_eq!(messages[1]["role"], "tool");
        assert_eq!(messages[1]["tool_call_id"], "call-summary");
        assert_eq!(messages[2]["role"], "tool");
        assert_eq!(messages[2]["tool_call_id"], "call-details");
    }

    #[test]
    fn append_debug_llm_record_keeps_only_the_latest_request_and_response() {
        let temp = tempfile::tempdir().unwrap();
        let mut settings = base_settings(LlmRoute::Responses);
        settings.debug = true;
        settings.phase = Some(1);
        settings.role = "analyst.technical".to_string();
        settings.debug_prompt_path = Some(PathBuf::from("prompts/phase1/technical.md"));
        settings.tools = Some(tools::ExternalToolConfig {
            project_root: temp.path().to_path_buf(),
            run_id: None,
            phase: None,
            phase_summary_page_limit: 20,
            phase_summary_detail_page_limit: 20,
            tickers: vec!["TQQQ".to_string()],
            alpaca_market_data: false,
            alpaca_api_key: None,
            alpaca_api_secret: None,
            file_store_input: None,
            file_store_reflection_source: None,
        });

        super::append_debug_llm_record(
            &settings,
            json!({
                "kind": "generate",
                "req": { "messages": [{"role": "user", "content": "hello"}] },
                "resp": { "status": "completed", "output": [{"type": "output_text", "text": "world"}] },
                "elapsed_ms": 50,
                "token": null,
            }),
        )
        .unwrap();
        super::append_debug_llm_record(
            &settings,
            json!({
                "kind": "stream",
                "req": { "messages": [{"role": "user", "content": "again"}] },
                "resp": { "id": "resp_1", "status": "completed" },
                "elapsed_ms": 120,
                "token": { "input_tokens": 10, "output_tokens": 5, "total_tokens": 15 },
            }),
        )
        .unwrap();

        let path = temp.path().join("outputs/debug/phase1/technical.json");
        let contents = std::fs::read_to_string(&path).unwrap();
        let output: Value = serde_json::from_str(&contents).unwrap();
        assert_eq!(output["prompt_path"], "prompts/phase1/technical.md");
        assert_eq!(output["kind"], "stream");
        assert_eq!(output["req"]["messages"][0]["content"], "again");
        assert_eq!(output["resp"]["id"], "resp_1");
        assert!(output.get("elapsed_ms").is_some());
        assert!(output.get("token").is_some());
        assert!(output.get("records").is_none());
        assert!(!temp
            .path()
            .join("outputs/debug/phase1/technical.jsonl")
            .exists());
    }

    #[test]
    fn append_debug_output_record_serializes_concurrent_latest_writes() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        let mut tasks = Vec::new();
        for id in 0..8 {
            let root = root.clone();
            tasks.push(std::thread::spawn(move || {
                super::append_debug_output_record(
                    &root,
                    Path::new("outputs/debug/phase0/runtime.json"),
                    "runtime",
                    json!({"req": {"id": id}, "resp": {"status": "derived"}}),
                )
            }));
        }
        for task in tasks {
            task.join().unwrap().unwrap();
        }

        let output: Value = serde_json::from_str(
            &std::fs::read_to_string(temp.path().join("outputs/debug/phase0/runtime.json"))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(output["prompt_path"], "runtime");
        assert!(output["req"]["id"].as_u64().is_some_and(|id| id < 8));
        assert_eq!(output["resp"]["status"], "derived");
        assert!(output.get("records").is_none());
    }

    #[test]
    fn append_debug_time_and_token_records_write_formatted_json_arrays() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        super::append_debug_time_record(
            root,
            json!({
                "kind": "role_job",
                "name": "analyst.technical",
                "elapsed_ms": 100,
                "llm_ms": 60,
                "tool_ms": 25,
                "wait_ms": 15
            }),
        )
        .unwrap();
        super::append_debug_time_record(
            root,
            json!({"kind": "phase", "name": "phase1", "elapsed_ms": 40}),
        )
        .unwrap();
        super::append_debug_token_record(
            root,
            json!({
                "kind": "role_job",
                "role": "analyst.technical",
                "input_tokens": 10,
                "output_tokens": 4,
                "total_tokens": 14
            }),
        )
        .unwrap();

        let time_path = root.join("outputs/debug/time.json");
        let token_path = root.join("outputs/debug/token.json");
        let time_records: Vec<Value> =
            serde_json::from_str(&std::fs::read_to_string(&time_path).unwrap()).unwrap();
        let token_records: Vec<Value> =
            serde_json::from_str(&std::fs::read_to_string(&token_path).unwrap()).unwrap();
        assert_eq!(time_records.len(), 2);
        assert_eq!(time_records[0]["kind"], "role_job");
        assert_eq!(time_records[0]["llm_ms"], 60);
        assert_eq!(time_records[0]["tool_ms"], 25);
        assert_eq!(time_records[0]["wait_ms"], 15);
        assert_eq!(time_records[1]["name"], "phase1");
        assert_eq!(token_records.len(), 1);
        assert_eq!(token_records[0]["total_tokens"], 14);
        assert!(token_records[0].get("ts_ms").is_some());
    }

    #[test]
    fn debug_record_path_mirrors_prompt_hierarchy() {
        let path = super::debug_record_relative_path_from_prompt(Path::new(
            "prompts/phase2/researcher/debate.md",
        ))
        .unwrap();
        assert_eq!(
            path,
            PathBuf::from("outputs/debug/phase2/researcher/debate.json")
        );
        assert_eq!(
            super::debug_record_relative_path_from_prompt(Path::new(
                "prompts/phase1/news_macro.md"
            ))
            .unwrap(),
            PathBuf::from("outputs/debug/phase1/news_macro.json")
        );
        assert!(
            super::debug_record_relative_path_from_prompt(Path::new("phase1/news_macro.md"))
                .is_err()
        );
        assert!(
            super::debug_record_relative_path_from_prompt(Path::new("prompts/../outside.md"))
                .is_err()
        );
    }

    #[test]
    fn role_llm_settings_rejects_unknown_tools() {
        let settings = RoleLlmSettings {
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
            tools: vec!["missing_tool".to_string()],
            native_web_search: false,
            free_opencode: false,
        };
        let err = settings.validate("analyst.technical").unwrap_err();
        assert!(err.to_string().contains("unknown tool name"));
    }

    #[test]
    fn role_llm_settings_allows_missing_or_blank_preamble() {
        let value = json!({
            "route": "responses",
            "model": "gpt-5.4",
            "base_url": "https://llm.example.com/v1",
            "api_key": "test-key",
            "max_turns": 4,
            "tools": []
        });
        let settings: RoleLlmSettings = serde_json::from_value(value).unwrap();
        assert!(settings.effective_preamble().is_none());
        settings.validate("manager.research").unwrap();

        let settings = RoleLlmSettings {
            preamble: Some("   ".to_string()),
            ..settings
        };
        assert!(settings.effective_preamble().is_none());
        settings.validate("manager.research").unwrap();
    }

    #[test]
    fn openai_compatible_requires_base_url_and_api_key_for_responses() {
        let settings = RoleLlmSettings {
            route: LlmRoute::Responses,
            model: "third-party-model".to_string(),
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
            native_web_search: false,
            free_opencode: false,
        };
        settings.validate("manager.research").unwrap();

        let settings = RoleLlmSettings {
            base_url: None,
            ..settings
        };
        assert!(settings
            .validate("manager.research")
            .unwrap_err()
            .to_string()
            .contains("requires base_url"));

        let settings = RoleLlmSettings {
            base_url: Some("https://llm.example.com/v1".to_string()),
            api_key: None,
            ..settings
        };
        assert!(settings
            .validate("manager.research")
            .unwrap_err()
            .to_string()
            .contains("requires api_key"));

        let settings = RoleLlmSettings {
            api_key: Some("config-key".to_string()),
            ..settings
        };
        settings.validate("manager.research").unwrap();
    }

    #[test]
    fn free_opencode_forces_chat_route_and_relaxes_validation() {
        let value = json!({
            "route": "responses",
            "model": "",
            "max_turns": 4,
            "tools": [],
            "free-opencode": true
        });
        let settings: RoleLlmSettings = serde_json::from_value(value).unwrap();
        assert!(settings.free_opencode);
        // Missing model / base_url / api_key are tolerated when free_opencode is on.
        settings.validate("manager.research").unwrap();
        assert_eq!(settings.effective_route(), LlmRoute::ChatCompletions);
        assert_eq!(settings.effective_model(), "deepseek-v4-flash-free");
    }

    #[test]
    fn responses_gets_reasoning_additional_params() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.llm.base_url = Some("https://llm.example.com/v1".to_string());
        settings.llm.api_key = Some("test-key".to_string());
        assert_eq!(
            super::additional_params(&settings),
            Some(json!({"reasoning": {"effort": "low"}}))
        );

        settings.reasoning_effort_override = Some("HIGH".to_string());
        assert_eq!(
            super::additional_params(&settings),
            Some(json!({"reasoning": {"effort": "high"}}))
        );

        settings.reasoning_effort_override = Some("0".to_string());
        assert_eq!(super::additional_params(&settings), None);

        settings.reasoning_effort_override = Some("HIGH".to_string());
        settings.llm.reasoning_summary = Some("auto".to_string());
        settings.llm.preserve_reasoning_state = true;
        assert_eq!(
            super::additional_params(&settings),
            Some(json!({
                "reasoning": {"effort": "high", "summary": "auto"},
                "store": false,
                "include": ["reasoning.encrypted_content"]
            }))
        );
    }

    #[test]
    fn native_web_search_adds_provider_tool_to_additional_params() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "analyst.news_macro".to_string();
        settings.llm.base_url = Some("https://llm.example.com/v1".to_string());
        settings.llm.api_key = Some("test-key".to_string());
        settings.llm.native_web_search = true;

        assert_eq!(
            super::additional_params(&settings),
            Some(json!({
                "reasoning": {"effort": "low"},
                "tools": [{"type": "web_search"}]
            }))
        );

        settings.llm.reasoning_effort = None;
        settings.reasoning_effort_override = None;
        assert_eq!(
            super::additional_params(&settings),
            Some(json!({"tools": [{"type": "web_search"}]}))
        );
    }

    #[test]
    fn openai_compatible_responses_uses_responses_reasoning_and_native_web_search() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "analyst.news_macro".to_string();
        settings.llm.base_url = Some("https://llm.example.com/v1".to_string());
        settings.llm.api_key = Some("test-key".to_string());
        settings.llm.native_web_search = true;
        settings.web_search.mode = WebSearchMode::Live;

        assert_eq!(
            super::additional_params(&settings),
            Some(json!({
                "reasoning": {"effort": "low"},
                "tools": [{"type": "web_search"}]
            }))
        );
        assert!(!super::configured_tool_names(&settings).contains(&tools::WEB_RUN_TOOL_NAME));
    }

    #[test]
    fn think_tool_registration_is_role_controlled() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "analyst.technical".to_string();
        settings.llm.base_url = Some("https://llm.example.com/v1".to_string());
        settings.llm.api_key = Some("test-key".to_string());
        settings.llm.tools = vec!["read_technical_snapshot".to_string()];
        settings.web_search.mode = WebSearchMode::Live;
        assert_eq!(
            super::configured_tool_names(&settings),
            vec!["think", "read_technical_snapshot", "web.run"]
        );

        settings.llm.think_tool = false;
        assert_eq!(
            super::configured_tool_names(&settings),
            vec!["read_technical_snapshot", "web.run"]
        );
    }

    #[test]
    fn execution_roles_keep_scoped_tools_but_disable_web_search() {
        for role in ["trader", "risk.neutral"] {
            let mut settings = base_settings(LlmRoute::Responses);
            settings.role = role.to_string();
            settings.llm.base_url = Some("https://llm.example.com/v1".to_string());
            settings.llm.api_key = Some("test-key".to_string());
            settings.llm.tools = vec![tools::READ_INDEXES_TOOL_NAME.to_string()];
            settings.llm.native_web_search = true;
            settings.web_search.mode = WebSearchMode::Live;

            assert_eq!(
                super::configured_tool_names(&settings),
                vec!["think", tools::READ_INDEXES_TOOL_NAME]
            );
            assert_eq!(
                super::additional_params(&settings),
                Some(json!({"reasoning": {"effort": "low"}}))
            );
            assert!(super::web_run_runtime_for_settings(&settings).is_none());
        }

        // manager.research is not tool-disabled, so live search enables web.run.
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "manager.research".to_string();
        settings.llm.base_url = Some("https://llm.example.com/v1".to_string());
        settings.llm.api_key = Some("test-key".to_string());
        settings.llm.tools = vec![tools::READ_INDEXES_TOOL_NAME.to_string()];
        settings.web_search.mode = WebSearchMode::Live;
        assert!(super::configured_tool_names(&settings).contains(&tools::WEB_RUN_TOOL_NAME));
    }

    #[test]
    fn news_analyst_only_gets_alpaca_news_when_market_data_gate_is_open() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "analyst.news_macro".to_string();
        settings.llm.think_tool = false;
        settings.llm.tools = vec![tools::ALPACA_GET_NEWS_TOOL_NAME.to_string()];
        settings.tools = Some(tools::ExternalToolConfig::default());

        assert!(super::configured_tool_names(&settings).is_empty());
        settings.tools.as_mut().unwrap().alpaca_market_data = true;
        assert_eq!(
            super::configured_tool_names(&settings),
            vec![tools::ALPACA_GET_NEWS_TOOL_NAME]
        );
    }

    #[test]
    fn web_run_tool_requires_enabled_search() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "analyst.news_macro".to_string();
        settings.llm.base_url = Some("https://llm.example.com/v1".to_string());
        settings.llm.api_key = Some("test-key".to_string());
        settings.llm.think_tool = false;

        assert!(!super::configured_tool_names(&settings).contains(&tools::WEB_RUN_TOOL_NAME));

        settings.web_search.mode = WebSearchMode::Live;
        assert!(super::configured_tool_names(&settings).contains(&tools::WEB_RUN_TOOL_NAME));

        settings.role = "trader".to_string();
        assert!(!super::configured_tool_names(&settings).contains(&tools::WEB_RUN_TOOL_NAME));
    }

    #[test]
    fn native_web_search_suppresses_web_run_fallback_tool() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "analyst.news_macro".to_string();
        settings.llm.base_url = Some("https://llm.example.com/v1".to_string());
        settings.llm.api_key = Some("test-key".to_string());
        settings.llm.think_tool = false;
        settings.llm.native_web_search = true;
        settings.web_search.mode = WebSearchMode::Live;

        assert!(!super::configured_tool_names(&settings).contains(&tools::WEB_RUN_TOOL_NAME));
        assert!(super::web_run_runtime_for_settings(&settings).is_none());
    }

    #[test]
    fn fork_history_is_used_only_for_a_new_target_turn() {
        let target = vec![json!({"id": "target"})];
        let source = vec![json!({"id": "source"})];
        assert_eq!(
            super::select_fork_history(target.clone(), source.clone()),
            target
        );
        assert_eq!(
            super::select_fork_history(Vec::new(), source.clone()),
            source
        );
    }

    #[test]
    fn new_fork_appends_one_topic_instruction_after_checkpoint_history() {
        let steer = r#"{"kind":"seed_claims","topic_id":"topic-a"}"#.to_string();
        let (user_input, fork_input, pending_steer) = super::prepare_steer_turn_inputs(
            "BULL ROLE PROMPT",
            Some(steer.clone()),
            true,
            true,
            true,
        );

        assert!(user_input.is_empty());
        assert_eq!(
            fork_input,
            Some(format!("BULL ROLE PROMPT\n\nSteer: {steer}"))
        );
        assert!(pending_steer.is_none());
    }
}
