use agent_loop::{
    AgentLoopConfig, AgentLoopModel, ModelEventHandler, ModelStreamEvent, ModelStreamResult,
    ProjectToolRuntime, RetrievalPolicy, ToolCallRequest, Turn,
};
use anyhow::{bail, Context, Result};
use futures::StreamExt;
use orchestrator_core::{default_project_root, ToolManagedProfile};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};
use tracing::{debug, enabled, Level};
use truncation::TruncationConfig;
use uuid::Uuid;
use web_search::{
    validate_web_search_runtime_config, ExaWebSearchProvider, WebSearchConfig,
    WebSearchContextSize, WebSearchMode,
};

pub mod agent_loop;
pub mod tools;
pub mod truncation;
pub mod web_search;

mod providers;

/// Appended to read-only Phase 1 output so reducers can distinguish exact
/// FileStore/tool evidence IDs from strings copied or mutated by the model.
pub const VERIFIED_PHASE1_EVIDENCE_MARKER: &str = "<!-- Rust-verified Phase 1 evidence IDs -->";

/// Appended before [`VERIFIED_PHASE1_EVIDENCE_MARKER`] when a Phase 1 tool
/// exposed an authoritative clock for an evidence ID.  Reducers may use these
/// records only to restore metadata for that exact ID; they are not model
/// generated evidence.
pub const VERIFIED_PHASE1_EVIDENCE_RECORDS_MARKER: &str =
    "<!-- Rust-verified Phase 1 evidence metadata -->";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmRoute {
    Responses,
    ChatCompletions,
}

/// Build the same typed async-openai transport used by the runtime for the
/// isolated provider contract probes.  The contract path still owns its own
/// requests and never creates a FileStore or workflow runtime.
pub fn build_provider_contract_client(
    base_url: &str,
    api_key: &str,
) -> Result<async_openai::Client<async_openai::config::OpenAIConfig>> {
    providers::openai_compatible_contract_client(base_url, api_key)
}

#[derive(Debug)]
enum ProviderFailure {
    Http(async_openai::error::OpenAIError),
    StreamDisconnected,
    ResponseFailed {
        code: Option<String>,
        message: Option<String>,
    },
    ResponseIncomplete {
        reason: Option<String>,
    },
    ProtocolViolation {
        detail: String,
    },
}

impl std::fmt::Display for ProviderFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Http(error) => write!(formatter, "provider HTTP request failed: {error:?}"),
            Self::StreamDisconnected => write!(formatter, "provider stream disconnected"),
            Self::ResponseFailed { code, message } => write!(
                formatter,
                "provider response failed{}{}",
                code.as_deref()
                    .map(|code| format!(" ({code})"))
                    .unwrap_or_default(),
                message
                    .as_deref()
                    .map(|message| format!(": {message}"))
                    .unwrap_or_default()
            ),
            Self::ResponseIncomplete { reason } => write!(
                formatter,
                "provider response incomplete{}",
                reason
                    .as_deref()
                    .map(|reason| format!(": {reason}"))
                    .unwrap_or_default()
            ),
            Self::ProtocolViolation { detail } => {
                write!(formatter, "provider protocol violation: {detail}")
            }
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplayState {
    NoProviderEvent,
    ProviderEventObserved,
    ApplicationEventEmitted,
    SideEffectCommitted,
}

#[derive(Debug)]
struct StreamAttemptError {
    error: anyhow::Error,
    replay_state: ReplayState,
}

impl StreamAttemptError {
    fn new(error: anyhow::Error, replay_state: ReplayState) -> Self {
        Self {
            error,
            replay_state,
        }
    }

    fn provider(failure: ProviderFailure, replay_state: ReplayState) -> Self {
        Self::new(anyhow::anyhow!(failure.to_string()), replay_state)
    }

    fn protocol(detail: impl Into<String>, replay_state: ReplayState) -> Self {
        Self::provider(
            ProviderFailure::ProtocolViolation {
                detail: detail.into(),
            },
            replay_state,
        )
    }

    fn handler(error: anyhow::Error, replay_state: ReplayState) -> Self {
        Self::new(error.context("model event handler failed"), replay_state)
    }

    fn with_context(self, context: String) -> Self {
        Self::new(self.error.context(context), self.replay_state)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmTransport {
    #[default]
    Http,
    Ws,
}

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
        if self.native_web_search && self.effective_route() != LlmRoute::Responses {
            bail!(
                "LLM config for role {role:?} enables native_web_search but its effective route is not responses"
            );
        }
        providers::validate_configuration(self, role)?;
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
        providers::effective_model(self)
    }
}

#[derive(Debug, Clone)]
pub struct AgentSettings {
    pub role: String,
    pub phase: Option<i64>,
    /// Optional topic identifier retained on timing and token metrics.
    pub topic_id: Option<String>,
    pub tickers: Vec<String>,
    /// Rust-owned role identity used to scope reads and Summary compilation.
    pub tool_managed_profile: ToolManagedProfile,
    /// Concrete FileStore authority for this agent session.
    pub session_runtime: agent_loop::FileStoreSessionRuntime,
    /// Present only for an Index/Detail unit.
    pub index_tool_runtime: Option<tools::index_tools::IndexToolRuntimeBinding>,
    /// Read-only, Rust-scoped retrieval of historical Experience.
    pub experience_retrieval: Option<tools::experience_tools::ExperienceRetrievalBinding>,
    /// Bounded Phase 2 delegation to the neutral Web evidence researcher.
    pub evidence_research: Option<tools::research_evidence_gap::EvidenceResearchBinding>,
    pub llm: RoleLlmSettings,
    pub reasoning_effort_override: Option<String>,
    pub tools: Option<tools::ExternalToolConfig>,
    pub web_search: WebSearchConfig,
    pub truncation: TruncationConfig,
    pub debug: bool,
    pub retrieval_policy: RetrievalPolicy,
}

impl AgentSettings {
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

/// Validate the provider-hosted Responses web-search configuration at the
/// boundary where both the role's LLM settings and its Rust-owned tool
/// authority are known.  The boolean is deliberately supplied by the caller:
/// configuration loading gets it from `RoleProfileRegistry`, while execution
/// gets it from the already-bound tool allowlist.
pub fn validate_native_web_search_configuration(
    llm: &RoleLlmSettings,
    web_search: &WebSearchConfig,
    role: &str,
    has_web_run_authority: bool,
) -> Result<()> {
    if !llm.native_web_search {
        return Ok(());
    }
    if llm.effective_route() != LlmRoute::Responses {
        bail!("native_web_search for role {role:?} requires the responses route");
    }
    if web_search.mode != WebSearchMode::Live {
        bail!("native_web_search for role {role:?} requires web_search.mode=live");
    }
    if !has_web_run_authority {
        bail!(
            "native_web_search for role {role:?} requires a profile that explicitly authorizes web.run"
        );
    }
    if !web_search.blocked_domains.is_empty() {
        bail!(
            "native_web_search for role {role:?} cannot honor blocked_domains; use allowed_domains or keep the Exa web.run path"
        );
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct ForkLoopInput<'a> {
    pub session_id: String,
    pub turn_id: String,
    pub prompt: &'a str,
    pub fork_from_turn_id: Option<String>,
    pub include_prompt_on_fork: bool,
    /// Rust-owned cross-agent delivery appended as a user message to the
    /// existing session before its next agent loop.
    pub injected_user_message: Option<String>,
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

pub async fn run_agent_loop_with_metrics(
    settings: &AgentSettings,
    prompt: &str,
) -> Result<AgentLoopOutput> {
    settings.llm.validate(&settings.role)?;
    validate_native_web_search_runtime_config(settings)?;
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
    turn.tools_disabled = false;
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
    if let Some(binding) = settings.index_tool_runtime.clone() {
        tools = tools.with_index_tool_runtime(binding);
    }
    if let Some(binding) = settings.experience_retrieval.clone() {
        tools = tools.with_experience_retrieval(binding);
    }
    if let Some(binding) = settings.evidence_research.clone() {
        tools = tools.with_evidence_research(binding);
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
        ..AgentLoopConfig::default()
    }
}

pub async fn run_agent_fork_loop_with_metrics(
    settings: &AgentSettings,
    input: ForkLoopInput<'_>,
) -> Result<AgentLoopOutput> {
    settings.llm.validate(&settings.role)?;
    validate_native_web_search_runtime_config(settings)?;
    validate_fallback_web_search_runtime_config(settings)?;
    let session = &settings.session_runtime;
    // Scope resume detection to this turn_id. Using run_id-latest history made
    // later phase-2 roles see sibling turns as "existing history" and drop their
    // own role prompt (live debate mass max_agent_loops / empty context).
    if session.manifest().session_id != input.session_id {
        bail!("fork input session does not match FileStore session authority");
    }
    let target_history = session_history_values(session.read_current_turn(&input.turn_id)?);
    let fork_from_turn_id = input.fork_from_turn_id;
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
    let include_prompt_on_fork = input.include_prompt_on_fork;
    let user_input = prepare_fork_turn_input(
        input.prompt,
        has_existing_history,
        is_new_fork,
        include_prompt_on_fork,
    );
    let mut turn = Turn::new(
        input.turn_id.clone(),
        input.session_id.clone(),
        session.manifest().run_id.clone(),
        settings.role.clone(),
        String::new(),
    );
    if has_existing_history {
        // Seed in-memory history so multi-round forks do not wipe the
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
    if !user_input.trim().is_empty() {
        // A forked debate is one continued conversation: preserve the
        // complete parent transcript, then append the child role task, then
        // append the Rust-owned stree user turn below.  Inserting the role
        // task at index zero made the debug history and the model request
        // start with a message that chronologically belongs after warmup.
        turn.emitted_items
            .push(agent_loop::TurnItem::user(user_input));
    }
    if let Some(message) = input
        .injected_user_message
        .filter(|value| !value.trim().is_empty())
    {
        turn.emitted_items.push(agent_loop::TurnItem {
            item_type: agent_loop::TurnItemType::InjectedContext,
            role: "user".to_owned(),
            content_text: message,
            content_json: json!({"source":"stree","delivery":"workflow"}),
            tool_call_id: String::new(),
            tool_name: String::new(),
            output_item_id: String::new(),
            phase: None,
            status: None,
            db_row_id: None,
        });
    }
    if is_new_fork {
        turn.retrieval_scope_start = turn.emitted_items.len();
    }
    turn.phase = settings.phase;
    turn.tools_disabled = false;
    turn.model_context = format!(
        "role={}\nprofile={}\ntickers={}\navailable_tools={}\nhistory_fork={}",
        settings.role,
        settings.tool_managed_profile.as_str(),
        settings.tickers.join(","),
        serde_json::to_string(&configured_tool_names(settings))?,
        fork_from_turn_id.is_some()
    );
    let tool_config = settings.tools.clone().unwrap_or_else(default_tool_config);
    let mut tools = ProjectToolRuntime::with_available_tools(
        tool_config,
        configured_tool_names(settings)
            .into_iter()
            .map(ToString::to_string)
            .collect(),
    );
    if let Some(binding) = settings.index_tool_runtime.clone() {
        tools = tools.with_index_tool_runtime(binding);
    }
    if let Some(binding) = settings.experience_retrieval.clone() {
        tools = tools.with_experience_retrieval(binding);
    }
    if let Some(binding) = settings.evidence_research.clone() {
        tools = tools.with_evidence_research(binding);
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

/// Write-managed roles complete through their terminal tool. Read-only roles
/// complete with their final Assistant text, which is kept in memory until a
/// phase-specific Summary commits the canonical Index.
fn completed_turn_artifact(turn: &Turn) -> Result<Value> {
    if let Some(terminal) = turn.terminal_tool_result.as_ref() {
        let mut artifact = terminal
            .output
            .get("artifact")
            .cloned()
            .unwrap_or(Value::Null);
        if let Some(object) = artifact.as_object_mut() {
            let evidence_refs = verified_terminal_evidence_refs(turn);
            if !evidence_refs.is_empty() {
                object.insert("verified_evidence_refs".to_owned(), json!(evidence_refs));
            }
            let evidence_records = verified_terminal_evidence_records(turn);
            if !evidence_records.is_empty() {
                object.insert(
                    "verified_evidence_records".to_owned(),
                    Value::Array(evidence_records),
                );
            }
        }
        return Ok(artifact);
    }
    let mut response_text = turn
        .emitted_items
        .iter()
        .rev()
        .find(|item| {
            item.item_type == agent_loop::TurnItemType::AssistantMessage
                && item.phase == Some(agent_loop::AgentItemPhase::Final)
                && !item.content_text.trim().is_empty()
        })
        .map(|item| item.content_text.trim().to_owned())
        .context("read-only agent loop finished without final Assistant text")?;
    let web_evidence = turn
        .emitted_items
        .iter()
        .filter(|item| {
            item.item_type == agent_loop::TurnItemType::ToolResult
                && item.tool_name == tools::research_evidence_gap::NAME
        })
        .filter_map(|item| item.content_json.pointer("/result/output").cloned())
        .collect::<Vec<_>>();
    if !web_evidence.is_empty() {
        response_text.push_str("\n\n");
        response_text.push_str(tools::research_evidence_gap::VERIFIED_PACKET_MARKER);
        response_text.push('\n');
        response_text.push_str(&serde_json::to_string_pretty(&web_evidence)?);
    }
    let verified_phase1_records = verified_phase1_evidence_records(turn);
    if !verified_phase1_records.is_empty() {
        response_text.push_str("\n\n");
        response_text.push_str(VERIFIED_PHASE1_EVIDENCE_RECORDS_MARKER);
        response_text.push('\n');
        response_text.push_str(&serde_json::to_string_pretty(&verified_phase1_records)?);
    }
    let verified_phase1_ids = verified_phase1_evidence_ids(turn);
    if !verified_phase1_ids.is_empty() {
        response_text.push_str("\n\n");
        response_text.push_str(VERIFIED_PHASE1_EVIDENCE_MARKER);
        response_text.push('\n');
        response_text.push_str(&serde_json::to_string_pretty(&verified_phase1_ids)?);
    }
    let verified_web_results = verified_web_search_results(turn);
    if !verified_web_results.is_empty() || has_verified_web_search_activity(turn) {
        response_text.push_str("\n\n");
        response_text.push_str(tools::web_run::VERIFIED_RESULTS_MARKER);
        response_text.push('\n');
        response_text.push_str(&serde_json::to_string_pretty(&verified_web_results)?);
    }
    Ok(json!({
        "phase": turn.phase,
        "role": turn.role,
        "response_text": response_text,
        "retrieval_audit": agent_loop::retrieval_audit(turn),
    }))
}

fn verified_phase1_evidence_ids(turn: &Turn) -> Vec<String> {
    let mut ids = BTreeSet::new();
    for item in turn.emitted_items.iter().filter(|item| {
        item.item_type == agent_loop::TurnItemType::ToolResult
            && matches!(
                item.tool_name.as_str(),
                tools::read_technical_snapshot::NAME | tools::read_jin10_candidates::NAME
            )
    }) {
        let Some(output) = item.content_json.pointer("/result/output") else {
            continue;
        };
        collect_verified_phase1_ids(output, &mut ids);
    }
    ids.into_iter().collect()
}

/// Preserve only source metadata that is emitted directly by the two Phase 1
/// read tools.  In particular, a technical signal's `as_of` clock is not an
/// inference by the analyst or Summary compiler.
fn verified_phase1_evidence_records(turn: &Turn) -> Vec<Value> {
    let mut records = BTreeMap::new();
    for item in turn.emitted_items.iter().filter(|item| {
        item.item_type == agent_loop::TurnItemType::ToolResult
            && matches!(
                item.tool_name.as_str(),
                tools::read_technical_snapshot::NAME | tools::read_jin10_candidates::NAME
            )
    }) {
        let Some(output) = item.content_json.pointer("/result/output") else {
            continue;
        };
        collect_verified_phase1_evidence_records(output, &mut records);
    }
    records.into_values().collect()
}

fn collect_verified_phase1_evidence_records(value: &Value, records: &mut BTreeMap<String, Value>) {
    match value {
        Value::Object(values) => {
            let evidence_id = values
                .get("signal_id")
                .or_else(|| values.get("evidence_id"))
                .and_then(Value::as_str)
                .filter(|id| id.starts_with("technical-") || id.starts_with("jin10-"));
            if let Some(evidence_id) = evidence_id {
                let mut record = serde_json::Map::new();
                record.insert(
                    "evidence_id".to_owned(),
                    Value::String(evidence_id.to_owned()),
                );
                if evidence_id.starts_with("technical-") {
                    record.insert(
                        "source".to_owned(),
                        Value::String("filestore.run_input.technical".to_owned()),
                    );
                    if let Some(as_of) = values.get("as_of").and_then(Value::as_str) {
                        record.insert("event_time".to_owned(), Value::String(as_of.to_owned()));
                        record.insert("as_of".to_owned(), Value::String(as_of.to_owned()));
                        record.insert("timezone".to_owned(), Value::String("UTC".to_owned()));
                    }
                } else {
                    record.insert(
                        "source".to_owned(),
                        Value::String("filestore.run_input.jin10".to_owned()),
                    );
                    for key in [
                        "event_time",
                        "published_time",
                        "ingested_time",
                        "as_of",
                        "timezone",
                    ] {
                        if let Some(value) = values.get(key).filter(|value| value.is_string()) {
                            record.insert(key.to_owned(), value.clone());
                        }
                    }
                }
                if ["event_time", "published_time", "ingested_time", "as_of"]
                    .into_iter()
                    .any(|key| {
                        record
                            .get(key)
                            .and_then(Value::as_str)
                            .is_some_and(|text| !text.trim().is_empty())
                    })
                {
                    records
                        .entry(evidence_id.to_owned())
                        .or_insert(Value::Object(record));
                }
            }
            for value in values.values() {
                collect_verified_phase1_evidence_records(value, records);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_verified_phase1_evidence_records(value, records);
            }
        }
        _ => {}
    }
}

fn collect_verified_phase1_ids(value: &Value, ids: &mut BTreeSet<String>) {
    match value {
        Value::Object(values) => {
            for (key, value) in values {
                if matches!(key.as_str(), "signal_id" | "evidence_id") {
                    if let Some(id) = value
                        .as_str()
                        .filter(|id| id.starts_with("technical-") || id.starts_with("jin10-"))
                    {
                        ids.insert(id.to_owned());
                    }
                }
                collect_verified_phase1_ids(value, ids);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_verified_phase1_ids(value, ids);
            }
        }
        _ => {}
    }
}

fn verified_web_search_results(turn: &Turn) -> Vec<Value> {
    let mut by_id = BTreeMap::new();
    for output in turn
        .emitted_items
        .iter()
        .filter(|item| {
            item.item_type == agent_loop::TurnItemType::ToolResult
                && matches!(
                    item.tool_name.as_str(),
                    tools::web_run::NAME | tools::verify_event::NAME
                )
        })
        .filter_map(|item| item.content_json.pointer("/result/output"))
    {
        for pointer in ["/search/results", "/results"] {
            for result in output
                .pointer(pointer)
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let Some(evidence_id) = result
                    .get("subject_id")
                    .or_else(|| result.get("ref_id"))
                    .and_then(Value::as_str)
                    .filter(|id| id.starts_with("web-"))
                else {
                    continue;
                };
                let Some(url) = result
                    .get("url")
                    .and_then(Value::as_str)
                    .filter(|url| url.starts_with("https://") || url.starts_with("http://"))
                else {
                    continue;
                };
                by_id.entry(evidence_id.to_owned()).or_insert_with(|| {
                    json!({
                        "evidence_id": evidence_id,
                        "source_url": url,
                        "title": result.get("title").cloned().unwrap_or(Value::Null),
                        "published_at": result
                            .get("published_at")
                            .or_else(|| result.get("published"))
                            .cloned()
                            .unwrap_or(Value::Null),
                    })
                });
            }
        }
    }
    for item in turn.emitted_items.iter().filter(|item| {
        item.item_type == agent_loop::TurnItemType::NativeWebSearch
            && item.tool_name == "native_web_search"
    }) {
        for result in item
            .content_json
            .get("results")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(evidence_id) = result
                .get("evidence_id")
                .and_then(Value::as_str)
                .filter(|id| id.starts_with("web-"))
            else {
                continue;
            };
            let Some(url) = result
                .get("source_url")
                .and_then(Value::as_str)
                .filter(|url| url.starts_with("https://") || url.starts_with("http://"))
            else {
                continue;
            };
            by_id.entry(evidence_id.to_owned()).or_insert_with(|| {
                json!({
                    "evidence_id": evidence_id,
                    "source_url": url,
                    "title": result.get("title").cloned().unwrap_or(Value::Null),
                    "published_at": result.get("published_at").cloned().unwrap_or(Value::Null),
                    "provider": result.get("provider").cloned().unwrap_or(Value::Null),
                    "citation": result.get("citation").cloned().unwrap_or(Value::Null),
                })
            });
        }
    }
    by_id.into_values().collect()
}

fn has_verified_web_search_activity(turn: &Turn) -> bool {
    turn.emitted_items.iter().any(|item| {
        (item.item_type == agent_loop::TurnItemType::ToolResult
            && matches!(
                item.tool_name.as_str(),
                tools::web_run::NAME | tools::verify_event::NAME
            ))
            || (item.item_type == agent_loop::TurnItemType::NativeWebSearch
                && item.tool_name == "native_web_search")
    })
}

/// Terminal tools own their artifact shape, but their output alone does not
/// preserve evidence fetched earlier in the same turn. This reconstructs only
/// IDs from actual tool results for the workflow's Rust-owned registry.
fn verified_terminal_evidence_refs(turn: &Turn) -> Vec<String> {
    let mut ids = verified_phase1_evidence_ids(turn)
        .into_iter()
        .filter(|id| is_complete_tool_evidence_id(id))
        .collect::<BTreeSet<_>>();
    // A Phase 2 role can legitimately cite evidence carried by an expanded
    // FileStore Detail.  Treat those Detail `source_refs` as visible only in
    // this turn: this is the Rust-observed boundary used later when the
    // Controller attests to a consensus claim.  An Index listing alone is not
    // sufficient because it contains no underlying evidence body.
    ids.extend(verified_index_detail_evidence_refs(turn));
    for result in verified_web_search_results(turn) {
        if let Some(id) = result
            .get("evidence_id")
            .and_then(Value::as_str)
            .filter(|id| is_complete_tool_evidence_id(id))
        {
            ids.insert(id.to_owned());
        }
    }
    for output in turn
        .emitted_items
        .iter()
        .filter(|item| {
            item.item_type == agent_loop::TurnItemType::ToolResult
                && item.tool_name == tools::research_evidence_gap::NAME
        })
        .filter_map(|item| item.content_json.pointer("/result/output"))
    {
        for item in ["evidence", "counterevidence"].into_iter().flat_map(|key| {
            output
                .get(key)
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        }) {
            if let Some(id) = item
                .get("evidence_id")
                .and_then(Value::as_str)
                .filter(|id| is_complete_tool_evidence_id(id))
            {
                ids.insert(id.to_owned());
            }
        }
    }
    ids.into_iter().collect()
}

fn verified_index_detail_evidence_refs(turn: &Turn) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    for output in turn
        .emitted_items
        .iter()
        .filter(|item| {
            item.item_type == agent_loop::TurnItemType::ToolResult
                && item.tool_name == tools::index_tools::READ_INDEX_DETAILS_NAME
        })
        .filter_map(|item| item.content_json.pointer("/result/output"))
    {
        for detail in output
            .get("details")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            for reference in detail
                .get("source_refs")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .filter(|reference| is_complete_tool_evidence_id(reference))
            {
                ids.insert(reference.to_owned());
            }
        }
    }
    ids
}

/// Retain the event-identity fields that Rust actually observed in a terminal
/// Phase 2 turn.  IDs alone only prove that a source existed; the workflow
/// also needs URL/time metadata to recognize the same event arriving under a
/// second `web-*` ID in another phase or fork.
fn verified_terminal_evidence_records(turn: &Turn) -> Vec<Value> {
    let mut by_id = BTreeMap::<String, Value>::new();
    for result in verified_web_search_results(turn) {
        let Some(evidence_id) = result
            .get("evidence_id")
            .and_then(Value::as_str)
            .filter(|id| is_complete_tool_evidence_id(id))
        else {
            continue;
        };
        by_id.insert(
            evidence_id.to_owned(),
            json!({
                "evidence_id": evidence_id,
                "source_url": result.get("source_url").cloned().unwrap_or(Value::Null),
                "published_at": result.get("published_at").cloned().unwrap_or(Value::Null),
                "retrieved_at": result.get("retrieved_at").cloned().unwrap_or(Value::Null),
                "publisher": result.get("publisher").cloned().unwrap_or(Value::Null),
                "source_tier": result.get("source_tier").cloned().unwrap_or(Value::Null),
                "event_identity_authority": "rust_verified_web_search_result",
            }),
        );
    }
    for output in turn
        .emitted_items
        .iter()
        .filter(|item| {
            item.item_type == agent_loop::TurnItemType::ToolResult
                && item.tool_name == tools::research_evidence_gap::NAME
        })
        .filter_map(|item| item.content_json.pointer("/result/output"))
    {
        for item in ["evidence", "counterevidence"].into_iter().flat_map(|key| {
            output
                .get(key)
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        }) {
            let Some(evidence_id) = item
                .get("evidence_id")
                .and_then(Value::as_str)
                .filter(|id| is_complete_tool_evidence_id(id))
            else {
                continue;
            };
            by_id.insert(
                evidence_id.to_owned(),
                json!({
                    "evidence_id": evidence_id,
                    "source_url": item.get("source_url").cloned().unwrap_or(Value::Null),
                    "published_at": item.get("published_at").cloned().unwrap_or(Value::Null),
                    "retrieved_at": item.get("retrieved_at").cloned().unwrap_or(Value::Null),
                    "publisher": item.get("publisher").cloned().unwrap_or(Value::Null),
                    "source_tier": item.get("source_tier").cloned().unwrap_or(Value::Null),
                    "claim": item.get("claim").cloned().unwrap_or(Value::Null),
                    "relation": item.get("relation").cloned().unwrap_or(Value::Null),
                    "event_identity_authority": "rust_verified_evidence_gap_result",
                }),
            );
        }
    }
    by_id.into_values().collect()
}

fn is_complete_tool_evidence_id(id: &str) -> bool {
    ["technical-", "jin10-", "web-"]
        .into_iter()
        .find_map(|prefix| id.strip_prefix(prefix))
        .is_some_and(|digest| {
            digest.len() == 64
                && digest
                    .chars()
                    .all(|character| character.is_ascii_hexdigit())
        })
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

fn prepare_fork_turn_input(
    prompt: &str,
    has_existing_history: bool,
    is_new_fork: bool,
    include_prompt_on_fork: bool,
) -> String {
    let context_instruction =
        "继续这个既有会话；Rust 会以 `stree: {...}` user message 注入本轮跨角色信息。依据该消息和已有会话上下文完成本轮终端工具动作。";
    if is_new_fork && include_prompt_on_fork {
        return format!(
            "这是一个新的子回合。上一条 assistant 输出只是恢复的 checkpoint 上下文，不是本轮答案。\
             请执行下面的新角色与任务，生成新的回复。\n\n{prompt}\n\n{context_instruction}"
        );
    }
    if has_existing_history {
        // The initial child role task is already present after the forked
        // history. Keep this turn empty so the only newly appended user data
        // is the Rust-owned InjectedContext/stree item.
        String::new()
    } else {
        prompt.to_string()
    }
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

/// Write the latest workflow-local or runtime debug record.
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
    let path = project_root.join(&relative_output_path);
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
    if uses_web_run_fallback(settings) || uses_bounded_event_verification(settings) {
        web_run_runtime(&settings.web_search)
            .map(|runtime| runtime.with_truncation(settings.truncation.clone()))
    } else {
        None
    }
}

fn uses_native_web_search(settings: &AgentSettings) -> bool {
    settings.llm.native_web_search
        && settings.web_search.mode == WebSearchMode::Live
        && settings
            .llm
            .tools
            .iter()
            .any(|name| name == tools::web_run::NAME)
}

fn uses_web_run_fallback(settings: &AgentSettings) -> bool {
    settings
        .llm
        .tools
        .iter()
        .any(|name| name == tools::web_run::NAME)
        && !uses_native_web_search(settings)
        && settings.web_search.mode != WebSearchMode::Disabled
}

fn uses_bounded_event_verification(settings: &AgentSettings) -> bool {
    settings
        .llm
        .tools
        .iter()
        .any(|name| name == tools::verify_event::NAME)
        && settings.web_search.mode != WebSearchMode::Disabled
}

fn validate_fallback_web_search_runtime_config(settings: &AgentSettings) -> Result<()> {
    if uses_web_run_fallback(settings) || uses_bounded_event_verification(settings) {
        validate_web_search_runtime_config(&settings.web_search, &settings.role)
    } else {
        Ok(())
    }
}

fn validate_native_web_search_runtime_config(settings: &AgentSettings) -> Result<()> {
    validate_native_web_search_configuration(
        &settings.llm,
        &settings.web_search,
        &settings.role,
        settings
            .llm
            .tools
            .iter()
            .any(|name| name == tools::web_run::NAME),
    )
}

fn native_web_search_tool(
    settings: &AgentSettings,
) -> Result<Option<async_openai::types::responses::Tool>> {
    use async_openai::types::responses::{
        Tool, WebSearchTool, WebSearchToolFilters, WebSearchToolSearchContextSize,
    };

    if !settings.llm.native_web_search {
        return Ok(None);
    }
    validate_native_web_search_runtime_config(settings)?;
    if !uses_native_web_search(settings) {
        return Ok(None);
    }
    let search_context_size = match settings.web_search.context_size {
        WebSearchContextSize::Low => WebSearchToolSearchContextSize::Low,
        WebSearchContextSize::Medium => WebSearchToolSearchContextSize::Medium,
        WebSearchContextSize::High => WebSearchToolSearchContextSize::High,
    };
    let filters = (!settings.web_search.allowed_domains.is_empty()).then(|| WebSearchToolFilters {
        allowed_domains: Some(settings.web_search.allowed_domains.clone()),
    });
    Ok(Some(Tool::WebSearch(WebSearchTool {
        filters,
        user_location: None,
        search_context_size: Some(search_context_size),
        search_content_types: None,
    })))
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

const HTTP_TRACE_TARGET: &str = "orchestrator_llm::http";

fn log_typed_provider_payload<T: Serialize>(role: &str, route: &str, direction: &str, payload: &T) {
    if !enabled!(target: HTTP_TRACE_TARGET, Level::DEBUG) {
        return;
    }
    debug!(
        target: HTTP_TRACE_TARGET,
        role,
        route,
        direction,
        payload = %providers::debug_typed_payload(payload),
        "async-openai typed provider payload"
    );
}

async fn run_responses_text_once(
    settings: &AgentSettings,
    input: &agent_loop::ModelInput,
    prompt: &str,
) -> Result<String> {
    let client = providers::openai_compatible_responses_client(&settings.llm)?;
    let request = build_responses_request(settings, input, prompt, false, true)?;
    let started = std::time::Instant::now();
    debug!(role = %settings.role, "sending non-streaming Responses API request");
    let response = client
        .responses()
        .create(request)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
        .context("OpenAI-compatible Responses prompt failed")?;
    log_typed_provider_payload(&settings.role, "responses", "response", &response);
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
    let client = providers::openai_compatible_responses_client(&settings.llm)?;
    let request = build_chat_completions_request(settings, input, prompt, false, true)?;
    let started = std::time::Instant::now();
    debug!(role = %settings.role, "sending non-streaming Chat Completions API request");
    let response = client
        .chat()
        .create(request)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
        .context("OpenAI-compatible Chat Completions prompt failed")?;
    log_typed_provider_payload(&settings.role, "chat_completions", "response", &response);
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
    append_prompt: bool,
) -> Result<async_openai::types::responses::CreateResponse> {
    use async_openai::types::responses::*;

    let mut items: Vec<InputItem> = Vec::new();
    let mut seen_first_user = false;

    for item in &input.items {
        match item.item_type {
            agent_loop::TurnItemType::UserMessage => {
                if append_prompt && !seen_first_user {
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
            // `InjectedContext` is a Rust-owned user turn such as a Phase 2
            // stree delivery.  It is intentionally distinct in the persisted
            // history/debug view, but the model must receive it exactly like
            // a follow-up user message in the existing conversation.
            agent_loop::TurnItemType::InjectedContext => {
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
                let call = item
                    .content_json
                    .get("call")
                    .context("Responses history tool call is missing its call payload")?;
                let arguments = call
                    .get("arguments")
                    .cloned()
                    .context("Responses history tool call is missing arguments")?;
                if !arguments.is_object() {
                    bail!("Responses history tool call arguments must be a JSON object");
                }
                let name = call
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|name| !name.trim().is_empty())
                    .or_else(|| {
                        (!item.tool_name.trim().is_empty()).then_some(item.tool_name.as_str())
                    })
                    .context("Responses history tool call is missing its name")?
                    .to_owned();
                let call_id = item.tool_call_id.clone();
                if call_id.trim().is_empty() {
                    bail!("Responses history tool call is missing call_id");
                }
                // Keep the persisted call id and provider item id distinct.
                // The previous JSON round-trip silently dropped the latter
                // and made malformed history look like a valid call.
                let item_id = (!item.output_item_id.trim().is_empty())
                    .then(|| item.output_item_id.clone())
                    .context("Responses history tool call is missing output item id")?;
                items.push(InputItem::Item(Item::FunctionCall(FunctionToolCall {
                    arguments: arguments.to_string(),
                    call_id,
                    namespace: None,
                    name,
                    id: Some(item_id),
                    status: Some(OutputStatus::Completed),
                })));
            }
            agent_loop::TurnItemType::ToolResult => {
                // TurnItem stores a bounded text projection for model input;
                // the full structured ToolResultItem remains in the durable
                // tool_execution record used for idempotent recovery.
                let content_text = truncation::truncate_semantic(
                    &item.content_text,
                    input.truncation.tool_result_chars,
                    &input.truncation,
                );
                items.push(InputItem::Item(Item::FunctionCallOutput(
                    FunctionCallOutputItemParam {
                        call_id: item.tool_call_id.clone(),
                        output: FunctionCallOutput::Text(content_text),
                        id: (!item.output_item_id.trim().is_empty())
                            .then(|| item.output_item_id.clone()),
                        status: Some(OutputStatus::Completed),
                    },
                )));
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
                        items.push(InputItem::Item(Item::Reasoning(ReasoningItem {
                            id: Some(id.to_owned()),
                            summary: Vec::new(),
                            content: None,
                            encrypted_content: Some(encrypted.to_owned()),
                            status: Some(OutputStatus::Completed),
                        })));
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

    if append_prompt {
        let user_msg = EasyInputMessage {
            role: Role::User,
            content: EasyInputContent::Text(prompt.to_string()),
            ..Default::default()
        };
        items.push(InputItem::EasyMessage(user_msg));
    }

    let item_count = items.len();
    let model = settings.llm.model.clone();
    let mut binding = CreateResponseArgs::default();
    let mut builder = binding
        .model(&model)
        .input(InputParam::Items(items))
        .prompt_cache_key(prompt_cache_key(settings));

    let has_system = if let Some(system) = &input.system_instruction {
        builder = builder.instructions(system.clone());
        true
    } else if let Some(preamble) = settings.llm.effective_preamble() {
        builder = builder.instructions(preamble.to_string());
        true
    } else {
        false
    };

    let mut tool_defs = if with_tools {
        tools::responses_tool_definitions(&input.available_tools)
    } else {
        Vec::new()
    };

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

    if with_tools {
        if let Some(native_web_search) = native_web_search_tool(settings)? {
            tool_defs.push(native_web_search);
        }
    }
    let tool_count = tool_defs.len();
    if !tool_defs.is_empty() {
        builder = builder
            .tools(tool_defs)
            .tool_choice(ToolChoiceParam::Mode(ToolChoiceOptions::Auto));
    }

    // JSON mode is only used for the tool-free phase-summary compressor. It
    // guarantees syntactically valid JSON; the Rust PhaseIndexCandidate
    // validator remains the business-schema authority.
    if settings.role == "compressor.phase_summary" && input.available_tools.is_empty() {
        builder = builder.text(ResponseTextParam {
            format: TextResponseFormatConfiguration::JsonObject,
            verbosity: None,
        });
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
    append_prompt: bool,
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
                if append_prompt && !seen_first_user {
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
            // Keep the persisted item type for traceability while delivering
            // a stree node to the target role as an ordinary user message.
            agent_loop::TurnItemType::InjectedContext => {
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

    if append_prompt {
        messages.push(ChatCompletionRequestMessage::User(
            ChatCompletionRequestUserMessage {
                content: ChatCompletionRequestUserMessageContent::Text(prompt.to_string()),
                name: None,
            },
        ));
    }

    let msg_count = messages.len();
    let model = settings.llm.effective_model().to_string();
    let mut binding = CreateChatCompletionRequestArgs::default();
    let mut builder = binding
        .model(&model)
        .messages(messages)
        .prompt_cache_key(prompt_cache_key(settings));

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

    if settings.role == "compressor.phase_summary" && input.available_tools.is_empty() {
        // Phase Summary has a strict JSON artifact contract and no tool-call
        // protocol to preserve. Ask Chat Completions to enforce valid JSON at
        // the transport boundary instead of relying on a corrective retry.
        builder = builder.response_format(ResponseFormat::JsonObject);
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

fn prompt_cache_key(settings: &AgentSettings) -> String {
    let phase = settings
        .phase
        .map(|phase| phase.to_string())
        .unwrap_or_else(|| "none".to_owned());
    format!(
        "akzio:p{phase}:{}:{}",
        settings.role,
        settings.tool_managed_profile.as_str()
    )
    .chars()
    .take(64)
    .collect()
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
    const MAX_SSE_OPENS: usize = 2;
    for attempt in 1..=MAX_SSE_OPENS {
        match stream_responses_once(settings, input, prompt, handler).await {
            Ok(()) => return Ok(()),
            Err(error)
                if attempt < MAX_SSE_OPENS
                    && error.replay_state == ReplayState::NoProviderEvent =>
            {
                tracing::warn!(
                    attempt,
                    role = %settings.role,
                    error = %error.error,
                    "reopening Responses stream before any provider event"
                );
            }
            Err(error) => return Err(error.error),
        }
    }
    unreachable!("the bounded Responses stream retry loop always returns")
}

fn is_recoverable_length_finish(text_started: bool, pending_tool_calls: bool) -> bool {
    text_started && !pending_tool_calls
}

async fn stream_responses_once(
    settings: &AgentSettings,
    input: &agent_loop::ModelInput,
    prompt: &str,
    handler: &mut dyn ModelEventHandler,
) -> std::result::Result<(), StreamAttemptError> {
    use async_openai::types::responses::{OutputItem, ResponseStreamEvent};

    let started = std::time::Instant::now();
    let client = providers::openai_compatible_responses_client(&settings.llm)
        .map_err(|error| StreamAttemptError::new(error, ReplayState::NoProviderEvent))?;
    let mut request = build_responses_request(settings, input, prompt, true, false)
        .map_err(|error| StreamAttemptError::new(error, ReplayState::NoProviderEvent))?;
    request.stream = Some(true);
    debug!(role = %settings.role, "opening streaming Responses API connection");
    let mut stream = client
        .responses()
        .create_stream(request)
        .await
        .map_err(|error| {
            StreamAttemptError::provider(ProviderFailure::Http(error), ReplayState::NoProviderEvent)
        })?;
    debug!(
        role = %settings.role,
        connect_ms = started.elapsed().as_millis() as u64,
        "stream connected, reading events"
    );

    let mut text_item_id: Option<String> = None;
    let mut text_started = false;
    let mut final_raw = Value::Null;
    let mut reasoning_item_id: Option<String> = None;
    let mut event_count: u64 = 0;
    let mut saw_response_created = false;
    let mut saw_terminal = false;
    let mut replay_state = ReplayState::NoProviderEvent;
    let mut emitted_tool_calls: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    let mut native_web_response = NativeWebSearchCollector::default();

    while let Some(event) = stream.next().await {
        event_count += 1;
        let event = match event {
            Ok(event) => {
                // Any successfully decoded provider event makes replay
                // unsafe, even if no text/tool side effect was emitted yet.
                replay_state = ReplayState::ProviderEventObserved;
                log_typed_provider_payload(&settings.role, "responses", "sse_event", &event);
                event
            }
            Err(error) => {
                let error_text = error.to_string();
                let failure = match &error {
                    async_openai::error::OpenAIError::JSONDeserialize(_, detail) => {
                        ProviderFailure::ProtocolViolation {
                            detail: detail.clone(),
                        }
                    }
                    _ => ProviderFailure::StreamDisconnected,
                };
                return Err(StreamAttemptError::provider(failure, replay_state)
                    .with_context(format!("Responses stream chunk failed: {error_text}")));
            }
        };
        if uses_native_web_search(settings) {
            native_web_response.observe(&event);
        }
        if saw_terminal {
            return Err(StreamAttemptError::protocol(
                "Responses stream emitted an event after a terminal response event",
                replay_state,
            ));
        }
        if !saw_response_created {
            if !matches!(&event, ResponseStreamEvent::ResponseCreated(_)) {
                return Err(StreamAttemptError::protocol(
                    "Responses stream did not begin with response.created",
                    replay_state,
                ));
            }
            saw_response_created = true;
        }
        match event {
            ResponseStreamEvent::ResponseCreated(_) => {}
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
                            .map_err(|e| StreamAttemptError::handler(e, replay_state))?;
                        text_started = true;
                        replay_state = ReplayState::ApplicationEventEmitted;
                    }
                }
                OutputItem::Reasoning(r) => {
                    reasoning_item_id = r.id.clone();
                }
                _ => {}
            },
            ResponseStreamEvent::ResponseOutputTextDelta(ev) => {
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
                        .map_err(|e| StreamAttemptError::handler(e, replay_state))?;
                    text_started = true;
                }
                handler
                    .handle(ModelStreamEvent::AssistantTextDelta {
                        item_id,
                        delta: ev.delta,
                    })
                    .await
                    .map_err(|e| StreamAttemptError::handler(e, replay_state))?;
                replay_state = ReplayState::ApplicationEventEmitted;
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
                        .map_err(|e| StreamAttemptError::handler(e, replay_state))?;
                    replay_state = ReplayState::ApplicationEventEmitted;
                    text_started = false;
                    text_item_id = None;
                }
            }
            ResponseStreamEvent::ResponseReasoningSummaryTextDelta(ev) => {
                let item_id = reasoning_item_id
                    .clone()
                    .unwrap_or_else(|| format!("reasoning-{}", Uuid::new_v4()));
                handler
                    .handle(ModelStreamEvent::ReasoningSummaryDelta {
                        item_id,
                        delta: ev.delta,
                    })
                    .await
                    .map_err(|e| StreamAttemptError::handler(e, replay_state))?;
                replay_state = ReplayState::ApplicationEventEmitted;
            }
            ResponseStreamEvent::ResponseReasoningSummaryTextDone(_ev) => {
                if let Some(item_id) = reasoning_item_id.clone() {
                    handler
                        .handle(ModelStreamEvent::ReasoningSummaryCompleted { item_id })
                        .await
                        .map_err(|e| StreamAttemptError::handler(e, replay_state))?;
                    replay_state = ReplayState::ApplicationEventEmitted;
                }
            }
            ResponseStreamEvent::ResponseOutputItemDone(ev) => match &ev.item {
                OutputItem::Reasoning(r) => {
                    if let Some(encrypted) = r.encrypted_content.clone() {
                        let item_id = r.id.clone().ok_or_else(|| {
                            StreamAttemptError::protocol(
                                "Responses reasoning item is missing id",
                                replay_state,
                            )
                        })?;
                        handler
                            .handle(ModelStreamEvent::ReasoningStateCompleted {
                                item_id,
                                encrypted_content: encrypted,
                            })
                            .await
                            .map_err(|e| StreamAttemptError::handler(e, replay_state))?;
                        replay_state = ReplayState::ApplicationEventEmitted;
                    }
                    reasoning_item_id = None;
                }
                OutputItem::FunctionCall(fc) => {
                    validate_responses_function_call(fc, replay_state)?;
                    if emitted_tool_calls.insert(fc.call_id.clone()) {
                        let arguments: Value =
                            serde_json::from_str(&fc.arguments).map_err(|error| {
                                StreamAttemptError::protocol(
                                    format!("malformed Responses tool arguments: {error}"),
                                    replay_state,
                                )
                            })?;
                        let Value::Object(_) = arguments else {
                            return Err(StreamAttemptError::protocol(
                                "Responses function call arguments must be a JSON object",
                                replay_state,
                            ));
                        };
                        handler
                            .handle(ModelStreamEvent::ToolCallCompletedWithOutputItem {
                                tool_call: ToolCallRequest {
                                    call_id: fc.call_id.clone(),
                                    name: tools::resolve_tool_name(&fc.name),
                                    arguments,
                                },
                                output_item_id: fc.id.clone().expect("validated output item id"),
                            })
                            .await
                            .map_err(|e| StreamAttemptError::handler(e, replay_state))?;
                        replay_state = ReplayState::ApplicationEventEmitted;
                    }
                }
                _ => {}
            },
            // Argument delta/done events are useful for UI progress but are
            // never sufficient to execute a tool. Only output_item.done is
            // authoritative for a complete FunctionToolCall.
            ResponseStreamEvent::ResponseFunctionCallArgumentsDelta(_)
            | ResponseStreamEvent::ResponseFunctionCallArgumentsDone(_) => {}
            ResponseStreamEvent::ResponseCompleted(ev) => {
                validate_response_output(&ev.response.output, replay_state)?;
                saw_terminal = true;
                if uses_native_web_search(settings) {
                    let record = native_web_response.record(&ev.response);
                    handler
                        .handle(ModelStreamEvent::NativeWebSearchCompleted {
                            item_id: format!("native-web-search-{}", ev.response.id),
                            record,
                        })
                        .await
                        .map_err(|error| StreamAttemptError::handler(error, replay_state))?;
                    replay_state = ReplayState::ApplicationEventEmitted;
                }
                final_raw = response_usage_to_raw(&ev.response);
            }
            ResponseStreamEvent::ResponseFailed(ev) => {
                return Err(StreamAttemptError::provider(
                    ProviderFailure::ResponseFailed {
                        code: ev.response.error.as_ref().map(|error| error.code.clone()),
                        message: ev
                            .response
                            .error
                            .as_ref()
                            .map(|error| error.message.clone()),
                    },
                    replay_state,
                ));
            }
            ResponseStreamEvent::ResponseIncomplete(ev) => {
                return Err(StreamAttemptError::provider(
                    ProviderFailure::ResponseIncomplete {
                        reason: ev
                            .response
                            .incomplete_details
                            .as_ref()
                            .map(|details| details.reason.clone()),
                    },
                    replay_state,
                ));
            }
            ResponseStreamEvent::ResponseError(ev) => {
                return Err(StreamAttemptError::provider(
                    ProviderFailure::ResponseFailed {
                        code: ev.code,
                        message: Some(ev.message),
                    },
                    replay_state,
                ));
            }
            _ => {}
        }
    }

    debug!(
        role = %settings.role,
        elapsed_ms = started.elapsed().as_millis() as u64,
        event_count,
        has_text = text_started,
        has_tool_call = !emitted_tool_calls.is_empty(),
        "stream completed"
    );

    if !saw_terminal {
        return Err(StreamAttemptError::protocol(
            format!("Responses stream ended without a terminal event after {event_count} events"),
            replay_state,
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
                .map_err(|e| StreamAttemptError::handler(e, replay_state))?;
            replay_state = ReplayState::ApplicationEventEmitted;
        }
    }

    handler
        .handle(ModelStreamEvent::ResponseCompleted {
            end_turn: emitted_tool_calls.is_empty(),
            raw: final_raw,
        })
        .await
        .map_err(|e| StreamAttemptError::handler(e, replay_state))?;

    Ok(())
}

fn validate_responses_function_call(
    call: &async_openai::types::responses::FunctionToolCall,
    replay_state: ReplayState,
) -> std::result::Result<(), StreamAttemptError> {
    if call.id.as_deref().is_none_or(|id| id.trim().is_empty())
        || call.call_id.trim().is_empty()
        || call.name.trim().is_empty()
    {
        return Err(StreamAttemptError::protocol(
            "Responses FunctionToolCall is missing id, call_id, or name",
            replay_state,
        ));
    }
    if call.status != Some(async_openai::types::responses::OutputStatus::Completed) {
        return Err(StreamAttemptError::protocol(
            "Responses FunctionToolCall output_item.done is not completed",
            replay_state,
        ));
    }
    let arguments: Value = serde_json::from_str(&call.arguments).map_err(|error| {
        StreamAttemptError::protocol(
            format!("malformed Responses tool arguments: {error}"),
            replay_state,
        )
    })?;
    if !arguments.is_object() {
        return Err(StreamAttemptError::protocol(
            "Responses function call arguments must be a JSON object",
            replay_state,
        ));
    }
    Ok(())
}

fn validate_response_output(
    output: &[async_openai::types::responses::OutputItem],
    replay_state: ReplayState,
) -> std::result::Result<(), StreamAttemptError> {
    for item in output {
        if let async_openai::types::responses::OutputItem::FunctionCall(call) = item {
            validate_responses_function_call(call, replay_state)?;
        }
    }
    Ok(())
}

async fn stream_chat_completions_with_retry(
    settings: &AgentSettings,
    input: &agent_loop::ModelInput,
    prompt: &str,
    handler: &mut dyn ModelEventHandler,
) -> Result<()> {
    const MAX_SSE_OPENS: usize = 2;
    for attempt in 1..=MAX_SSE_OPENS {
        match stream_chat_completions_once(settings, input, prompt, handler).await {
            Ok(()) => return Ok(()),
            Err((error, made_progress)) if attempt < MAX_SSE_OPENS && !made_progress => {
                tracing::warn!(
                    attempt,
                    error = %error,
                    error_chain = %format!("{error:#}"),
                    role = %settings.role,
                    "reopening Chat Completions stream before any provider event"
                );
            }
            Err((error, _)) => return Err(error),
        }
    }
    unreachable!("the bounded Chat Completions stream retry loop always returns")
}

struct PendingChatToolCall {
    id: String,
    name: String,
    arguments: String,
}

fn merge_chat_tool_call_delta(
    pending: &mut PendingChatToolCall,
    chunk: &async_openai::types::chat::ChatCompletionMessageToolCallChunk,
) {
    if let Some(id) = chunk.id.as_deref().filter(|id| !id.trim().is_empty()) {
        pending.id = id.to_owned();
    }
    if let Some(function) = chunk.function.as_ref() {
        if let Some(name) = function
            .name
            .as_deref()
            .filter(|name| !name.trim().is_empty())
        {
            pending.name = name.to_owned();
        }
        if let Some(arguments) = function.arguments.as_deref() {
            pending.arguments.push_str(arguments);
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
    let client =
        providers::openai_compatible_responses_client(&settings.llm).map_err(|e| (e, false))?;
    let request = build_chat_completions_request(settings, input, prompt, true, false)
        .map_err(|e| (e, false))?;
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
    let mut truncated_by_length = false;

    let mut pending_tool_calls: std::collections::HashMap<u32, PendingChatToolCall> =
        std::collections::HashMap::new();

    while let Some(event) = stream.next().await {
        event_count += 1;
        let chunk = match event {
            Ok(ev) => {
                // A successfully decoded provider chunk is enough to make
                // replay unsafe, even before text or a tool call is emitted.
                made_progress = true;
                log_typed_provider_payload(&settings.role, "chat_completions", "sse_chunk", &ev);
                ev
            }
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
                            .or_insert_with(|| PendingChatToolCall {
                                id: String::new(),
                                name: String::new(),
                                arguments: String::new(),
                            });
                    merge_chat_tool_call_delta(pending, tc_chunk);
                }
            }

            if let Some(finish_reason) = choice.finish_reason {
                let finish = format!("{finish_reason:?}");
                if finish.eq_ignore_ascii_case("length") {
                    if is_recoverable_length_finish(text_started, !pending_tool_calls.is_empty()) {
                        // Preserve the completed text item and let the Agent
                        // Loop request a continuation.  A length-terminated
                        // tool call is still fatal because its arguments are
                        // not safe to execute.
                        truncated_by_length = true;
                        saw_terminal_finish = true;
                        continue;
                    }
                    return Err((
                        anyhow::anyhow!(
                            "Chat Completions stream terminated with non-recoverable finish_reason={finish}"
                        ),
                        made_progress,
                    ));
                }
                if finish.eq_ignore_ascii_case("contentfilter")
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

    // A provider-specific non-JSON SSE event may be skipped above, but it is
    // never a completion signal. Treating any partial text from free_opencode
    // as a successful turn can publish an incomplete report or execute an
    // incomplete protocol transition downstream.
    if !saw_terminal_finish {
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
            end_turn: !saw_tool_call && !truncated_by_length,
            raw: final_raw,
        })
        .await
        .map_err(|e| (e, made_progress))?;

    Ok(())
}

fn response_usage_to_raw(response: &async_openai::types::responses::Response) -> Value {
    match &response.usage {
        Some(usage) => {
            json!({
                "usage": {
                    "input_tokens": usage.input_tokens,
                    "output_tokens": usage.output_tokens,
                    "total_tokens": usage.total_tokens,
                    "input_tokens_details": { "cached_tokens": usage.input_tokens_details.cached_tokens },
                    "output_tokens_details": { "reasoning_tokens": usage.output_tokens_details.reasoning_tokens }
                }
            })
        }
        None => Value::Null,
    }
}

#[derive(Default)]
struct NativeWebSearchCollector {
    response_id: Option<String>,
    created_at: Option<u64>,
    completed_at: Option<u64>,
    output: BTreeMap<u32, async_openai::types::responses::OutputItem>,
}

impl NativeWebSearchCollector {
    fn observe(&mut self, event: &async_openai::types::responses::ResponseStreamEvent) {
        use async_openai::types::responses::ResponseStreamEvent;

        match event {
            ResponseStreamEvent::ResponseCreated(event) => self.observe_response(&event.response),
            ResponseStreamEvent::ResponseInProgress(event) => {
                self.observe_response(&event.response)
            }
            ResponseStreamEvent::ResponseCompleted(event) => self.observe_response(&event.response),
            ResponseStreamEvent::ResponseFailed(event) => self.observe_response(&event.response),
            ResponseStreamEvent::ResponseIncomplete(event) => {
                self.observe_response(&event.response)
            }
            ResponseStreamEvent::ResponseOutputItemDone(event) => {
                self.output.insert(event.output_index, event.item.clone());
            }
            _ => {}
        }
    }

    fn observe_response(&mut self, response: &async_openai::types::responses::Response) {
        self.response_id = Some(response.id.clone());
        self.created_at = Some(response.created_at);
        self.completed_at = response.completed_at;
        for (index, item) in response.output.iter().enumerate() {
            self.output.insert(index as u32, item.clone());
        }
    }

    fn record(&self, response: &async_openai::types::responses::Response) -> Value {
        use async_openai::types::responses::{Annotation, OutputItem, WebSearchToolCallAction};

        let mut search_calls = Vec::new();
        let mut sources = BTreeSet::new();
        let mut citations = BTreeMap::<String, Value>::new();

        for item in self.output.values() {
            match item {
                OutputItem::WebSearchCall(call) => {
                    if let Some(WebSearchToolCallAction::Search(search)) = &call.action {
                        for source in search.sources.iter().flatten() {
                            if let Some(url) = normalize_native_web_url(&source.url) {
                                sources.insert(url);
                            }
                        }
                    }
                    search_calls.push(json!({
                        "id": call.id,
                        "status": call.status,
                        "action": call.action,
                    }));
                }
                OutputItem::Message(message) => {
                    for content in &message.content {
                        let async_openai::types::responses::OutputMessageContent::OutputText(text) =
                            content
                        else {
                            continue;
                        };
                        for annotation in &text.annotations {
                            let Annotation::UrlCitation(_) = annotation else {
                                continue;
                            };
                            let Ok(raw) = serde_json::to_value(annotation) else {
                                continue;
                            };
                            let Some(url) = raw.get("url").and_then(Value::as_str) else {
                                continue;
                            };
                            let Some(url) = normalize_native_web_url(url) else {
                                continue;
                            };
                            let title = raw.get("title").and_then(Value::as_str);
                            let authority = if sources.contains(&url) {
                                "citation_and_source"
                            } else {
                                "citation_only"
                            };
                            insert_native_web_citation_with_authority(
                                &mut citations,
                                &url,
                                title,
                                authority,
                            );
                        }
                    }
                }
                _ => {}
            }
        }

        // Sources without a matching citation remain diagnostic provenance;
        // they are never promoted to cited evidence.
        for url in sources {
            citations
                .entry(url.clone())
                .or_insert_with(|| native_web_diagnostic_citation(&url, "source_only"));
        }

        json!({
            "provider": "openai_responses_web_search",
            "response_id": self.response_id.as_deref().unwrap_or(&response.id),
            "created_at": self.created_at.or(Some(response.created_at)),
            "completed_at": self.completed_at.or(response.completed_at),
            "search_calls": search_calls,
            "results": citations.into_values().collect::<Vec<_>>(),
        })
    }
}

fn normalize_native_web_url(value: &str) -> Option<String> {
    let mut url = reqwest::Url::parse(value.trim()).ok()?;
    if !matches!(url.scheme(), "http" | "https") {
        return None;
    }
    url.set_fragment(None);
    let retained = url
        .query_pairs()
        .filter(|(key, _)| {
            let key = key.to_ascii_lowercase();
            !key.starts_with("utm_") && !matches!(key.as_str(), "gclid" | "fbclid")
        })
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>();
    if retained.is_empty() {
        url.set_query(None);
    } else {
        url.set_query(Some(&retained.join("&")));
    }
    if url.path() != "/" {
        let path = url.path().trim_end_matches('/').to_owned();
        url.set_path(if path.is_empty() { "/" } else { &path });
    }
    Some(url.to_string())
}

fn insert_native_web_citation_with_authority(
    citations: &mut BTreeMap<String, Value>,
    url: &str,
    title: Option<&str>,
    authority: &str,
) {
    let evidence_id = web_search::stable_search_ref_id(&web_search::SearchResult {
        ref_id: String::new(),
        title: title.unwrap_or_default().to_owned(),
        url: url.to_owned(),
        snippet: String::new(),
        published_at: None,
        source: None,
    });
    citations.entry(url.to_owned()).or_insert_with(|| {
        json!({
            "evidence_id": evidence_id,
            "source_url": url,
            "title": title,
            "published_at": Value::Null,
            "provider": "openai_responses_web_search",
            "citation": authority != "source_only",
            "authority": authority,
        })
    });
}

fn native_web_diagnostic_citation(url: &str, authority: &str) -> Value {
    let evidence_id = web_search::stable_search_ref_id(&web_search::SearchResult {
        ref_id: String::new(),
        title: String::new(),
        url: url.to_owned(),
        snippet: String::new(),
        published_at: None,
        source: None,
    });
    json!({
        "evidence_id": evidence_id,
        "source_url": url,
        "title": Value::Null,
        "published_at": Value::Null,
        "provider": "openai_responses_web_search",
        "citation": false,
        "authority": authority,
    })
}

fn configured_tool_names(settings: &AgentSettings) -> Vec<&str> {
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
                if *name == tools::alpaca::GET_NEWS_NAME {
                    settings
                        .tools
                        .as_ref()
                        .is_some_and(|config| config.alpaca_market_data)
                } else if *name == tools::web_run::NAME {
                    uses_web_run_fallback(settings)
                } else {
                    true
                }
            }),
    );
    // The RoleProfileRegistry allowlist copied into `llm.tools` is the sole
    // source of model-visible business authority. Typed runtime bindings
    // independently reject unavailable execution paths, but must never add
    // names that the profile did not authorize.
    let mut seen = BTreeSet::new();
    names.retain(|name| seen.insert(*name));
    names
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
        phase2_context: None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        agent_loop, is_recoverable_length_finish, merge_chat_tool_call_delta, tools, AgentSettings,
        LlmRoute, LlmTransport, PendingChatToolCall, RoleLlmSettings, ToolManagedProfile,
        TruncationConfig,
    };
    use crate::web_search::{WebSearchConfig, WebSearchMode};
    use orchestrator_store::{FileStore, FileStoreOptions, RunLocation};
    use serde_json::{json, Value};
    use std::path::Path;

    #[test]
    fn length_finish_is_recoverable_only_for_plain_text_without_pending_tools() {
        assert!(is_recoverable_length_finish(true, false));
        assert!(!is_recoverable_length_finish(false, false));
        assert!(!is_recoverable_length_finish(true, true));
    }

    #[test]
    fn chat_tool_call_merge_preserves_metadata_when_follow_up_delta_is_empty() {
        let first: async_openai::types::chat::ChatCompletionMessageToolCallChunk =
            serde_json::from_value(json!({
                "index": 0,
                "id": "call-123",
                "type": "function",
                "function": {
                    "name": "read_technical_detail",
                    "arguments": ""
                }
            }))
            .unwrap();
        let follow_up: async_openai::types::chat::ChatCompletionMessageToolCallChunk =
            serde_json::from_value(json!({
                "index": 0,
                "id": "",
                "function": {
                    "name": "",
                    "arguments": "{\"ticker\":\"QQQ\"}"
                }
            }))
            .unwrap();
        let mut pending = PendingChatToolCall {
            id: String::new(),
            name: String::new(),
            arguments: String::new(),
        };

        merge_chat_tool_call_delta(&mut pending, &first);
        merge_chat_tool_call_delta(&mut pending, &follow_up);

        assert_eq!(pending.id, "call-123");
        assert_eq!(pending.name, "read_technical_detail");
        assert_eq!(pending.arguments, r#"{"ticker":"QQQ"}"#);
    }

    #[test]
    fn phase_summary_chat_request_requires_json_object_without_tools() {
        let mut settings = base_settings(LlmRoute::ChatCompletions);
        settings.role = "compressor.phase_summary".to_owned();
        let input = agent_loop::ModelInput {
            system_instruction: None,
            items: vec![agent_loop::TurnItem::user("summary input")],
            available_tools: Vec::new(),
            truncation: TruncationConfig::default(),
        };

        let request = super::build_chat_completions_request(
            &settings,
            &input,
            "return the phase summary as JSON",
            true,
            true,
        )
        .unwrap();

        assert_eq!(
            serde_json::to_value(request).unwrap()["response_format"],
            json!({"type": "json_object"})
        );
    }

    #[test]
    fn phase_summary_responses_request_uses_json_object_with_rust_schema_validation() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "compressor.phase_summary".to_owned();
        let input = agent_loop::ModelInput {
            system_instruction: None,
            items: vec![agent_loop::TurnItem::user("summary input")],
            available_tools: Vec::new(),
            truncation: TruncationConfig::default(),
        };

        let request = super::build_responses_request(
            &settings,
            &input,
            "return the phase summary as JSON",
            false,
            true,
        )
        .unwrap();

        assert_eq!(
            serde_json::to_value(request).unwrap()["text"],
            json!({"format": {"type": "json_object"}})
        );
    }

    fn base_settings(route: LlmRoute) -> AgentSettings {
        AgentSettings {
            role: "manager.research".to_string(),
            phase: None,
            topic_id: None,
            tickers: vec!["TQQQ".to_string()],
            tool_managed_profile: ToolManagedProfile::ResearchDecision,
            session_runtime: test_session_runtime(),
            index_tool_runtime: None,
            experience_retrieval: None,
            evidence_research: None,
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
            name: "submit_terminal_result".to_string(),
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
    fn terminal_completion_exposes_only_rust_verified_evidence_refs() {
        let mut turn = agent_loop::Turn::new("turn-1", "session-1", "run-1", "researcher.bull", "");
        let evidence_id = "web-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let detail_evidence_id =
            "technical-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        turn.emitted_items.push(agent_loop::TurnItem::tool_result(
            &agent_loop::ToolResultItem {
                call_id: "research-1".to_owned(),
                name: tools::research_evidence_gap::NAME.to_owned(),
                status: "completed".to_owned(),
                output: json!({
                    "evidence": [
                        {
                            "evidence_id": evidence_id,
                            "source_url": "https://example.test/event",
                            "published_at": "2026-08-03T00:00:00Z",
                            "publisher": "Example"
                        },
                        {"evidence_id": "web-too-short"}
                    ],
                    "counterevidence": []
                }),
                error: None,
            },
            &TruncationConfig::default(),
        ));
        turn.emitted_items.push(agent_loop::TurnItem::tool_result(
            &agent_loop::ToolResultItem {
                call_id: "detail-1".to_owned(),
                name: tools::index_tools::READ_INDEX_DETAILS_NAME.to_owned(),
                status: "completed".to_owned(),
                output: json!({
                    "details": [{
                        "source_refs": [detail_evidence_id, "idx-not-a-source-evidence-id"]
                    }]
                }),
                error: None,
            },
            &TruncationConfig::default(),
        ));
        turn.terminal_tool_result = Some(agent_loop::ToolResultItem {
            call_id: "finalize-1".to_owned(),
            name: tools::phase2_stree::SUBMIT_DEBATE_TURN.to_owned(),
            status: "completed".to_owned(),
            output: json!({"terminal": true, "artifact": {"phase2_stree": {}}}),
            error: None,
        });

        let artifact = super::completed_turn_artifact(&turn).unwrap();
        assert_eq!(
            artifact["verified_evidence_refs"],
            json!([detail_evidence_id, evidence_id])
        );
        assert_eq!(
            artifact["verified_evidence_records"][0]["source_url"],
            "https://example.test/event"
        );
        assert_eq!(
            artifact["verified_evidence_records"][0]["event_identity_authority"],
            "rust_verified_evidence_gap_result"
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
            name: "submit_terminal_result".to_string(),
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
    fn read_only_completion_attaches_rust_verified_web_evidence() {
        let mut turn = agent_loop::Turn::new("turn-1", "session-1", "run-1", "mediator.topic", "");
        turn.emitted_items.push(agent_loop::TurnItem::tool_result(
            &agent_loop::ToolResultItem {
                call_id: "research-1".to_owned(),
                name: tools::research_evidence_gap::NAME.to_owned(),
                status: "completed".to_owned(),
                output: json!({
                    "status": "supported",
                    "request_id": "web-abcdef",
                    "evidence": [{"evidence_id":"web-123456"}]
                }),
                error: None,
            },
            &TruncationConfig::default(),
        ));
        turn.emitted_items.push(agent_loop::TurnItem::tool_result(
            &agent_loop::ToolResultItem {
                call_id: "verify-1".to_owned(),
                name: "verify_event".to_owned(),
                status: "completed".to_owned(),
                output: json!({
                    "search": {"results": [{
                        "subject_id": "web-123456",
                        "url": "https://example.com/fact",
                        "title": "Official fact"
                    }]}
                }),
                error: None,
            },
            &TruncationConfig::default(),
        ));
        let mut final_message = agent_loop::TurnItem::assistant("议题报告", Value::Null);
        final_message.phase = Some(agent_loop::AgentItemPhase::Final);
        turn.emitted_items.push(final_message);

        let artifact = super::completed_turn_artifact(&turn).unwrap();
        let response = artifact["response_text"].as_str().unwrap();
        assert!(response.contains("议题报告"));
        assert!(response.contains("Rust-verified Web evidence packets"));
        assert!(response.contains("web-123456"));
        assert!(response.contains("Rust-verified Web search results"));
        assert!(response.contains("https://example.com/fact"));
        assert!(artifact.get("retrieval_audit").is_some());
    }

    #[test]
    fn read_only_completion_ignores_web_shaped_output_from_unrelated_tools() {
        let mut turn = agent_loop::Turn::new("turn-1", "session-1", "run-1", "trader", "");
        turn.emitted_items.push(agent_loop::TurnItem::tool_result(
            &agent_loop::ToolResultItem {
                call_id: "indexes-1".to_owned(),
                name: tools::index_tools::READ_INDEXES_NAME.to_owned(),
                status: "completed".to_owned(),
                output: json!({
                    "search": {"results": [{
                        "subject_id": "web-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "url": "https://untrusted.example/result"
                    }]}
                }),
                error: None,
            },
            &TruncationConfig::default(),
        ));
        let mut final_message = agent_loop::TurnItem::assistant("交易报告", Value::Null);
        final_message.phase = Some(agent_loop::AgentItemPhase::Final);
        turn.emitted_items.push(final_message);

        let artifact = super::completed_turn_artifact(&turn).unwrap();
        assert!(!artifact["response_text"]
            .as_str()
            .unwrap()
            .contains(tools::web_run::VERIFIED_RESULTS_MARKER));
    }

    #[test]
    fn read_only_completion_attaches_exact_phase1_tool_evidence_ids() {
        let mut turn = agent_loop::Turn::new("turn-1", "session-1", "run-1", "analyst.news", "");
        for (name, output) in [
            (
                tools::read_technical_snapshot::NAME,
                json!({"snapshots": [{"intervals": [{"signals": [{
                    "signal_id": "technical-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "as_of": "2026-08-03T15:00:00Z"
                }]}]}]}),
            ),
            (
                tools::read_jin10_candidates::NAME,
                json!({"candidates": [{
                    "evidence_id": "jin10-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "event_time": "2026-08-03T12:00:00Z"
                }]}),
            ),
        ] {
            turn.emitted_items.push(agent_loop::TurnItem::tool_result(
                &agent_loop::ToolResultItem {
                    call_id: format!("{name}-1"),
                    name: name.to_owned(),
                    status: "completed".to_owned(),
                    output,
                    error: None,
                },
                &TruncationConfig::default(),
            ));
        }
        let mut final_message = agent_loop::TurnItem::assistant("分析报告", Value::Null);
        final_message.phase = Some(agent_loop::AgentItemPhase::Final);
        turn.emitted_items.push(final_message);

        let artifact = super::completed_turn_artifact(&turn).unwrap();
        let response = artifact["response_text"].as_str().unwrap();
        assert!(response.contains(super::VERIFIED_PHASE1_EVIDENCE_MARKER));
        assert!(response.contains(super::VERIFIED_PHASE1_EVIDENCE_RECORDS_MARKER));
        assert!(response.contains(
            "technical-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        assert!(response
            .contains("jin10-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
        assert!(response.contains("2026-08-03T15:00:00Z"));
        assert!(response.contains("2026-08-03T12:00:00Z"));
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
                "record_phase2_context",
                "submit_debate_turn",
                "route_debate_turn",
                "wait_for_debate_turn",
                "close_debate",
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
            true,
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
    fn stree_injected_context_is_sent_as_a_user_message_for_both_routes() {
        let stree_text = "stree: bear accepts Bull claim topic-a:bull:1; please close the debate.";
        let injected = agent_loop::TurnItem {
            item_type: agent_loop::TurnItemType::InjectedContext,
            role: "user".to_owned(),
            content_text: stree_text.to_owned(),
            content_json: json!({"source": "stree", "node_id": "node-a"}),
            tool_call_id: String::new(),
            tool_name: String::new(),
            output_item_id: String::new(),
            phase: None,
            status: None,
            db_row_id: None,
        };
        let input = agent_loop::ModelInput {
            system_instruction: None,
            items: vec![agent_loop::TurnItem::user("Bull role prompt"), injected],
            available_tools: Vec::new(),
            truncation: TruncationConfig::default(),
        };

        let responses = super::build_responses_request(
            &base_settings(LlmRoute::Responses),
            &input,
            "continue the existing conversation",
            false,
            false,
        )
        .unwrap();
        assert!(serde_json::to_string(&responses)
            .unwrap()
            .contains(stree_text));

        let chat = super::build_chat_completions_request(
            &base_settings(LlmRoute::ChatCompletions),
            &input,
            "continue the existing conversation",
            false,
            false,
        )
        .unwrap();
        let messages = serde_json::to_value(chat).unwrap()["messages"].clone();
        assert!(messages.as_array().is_some_and(|items| {
            items.iter().any(|message| {
                message["role"] == "user" && message["content"].as_str() == Some(stree_text)
            })
        }));
    }

    #[test]
    fn both_routes_emit_a_stable_role_scoped_prompt_cache_key() {
        let input = agent_loop::ModelInput {
            system_instruction: None,
            items: vec![agent_loop::TurnItem::user("static summary prefix")],
            available_tools: Vec::new(),
            truncation: TruncationConfig::default(),
        };
        let mut chat_settings = base_settings(LlmRoute::ChatCompletions);
        chat_settings.phase = Some(1);
        chat_settings.role = "compressor.phase_summary".to_owned();
        let expected = super::prompt_cache_key(&chat_settings);
        let chat = super::build_chat_completions_request(
            &chat_settings,
            &input,
            "dynamic payload",
            false,
            true,
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(chat).unwrap()["prompt_cache_key"],
            expected
        );

        let mut response_settings = base_settings(LlmRoute::Responses);
        response_settings.phase = Some(1);
        response_settings.role = "compressor.phase_summary".to_owned();
        let responses = super::build_responses_request(
            &response_settings,
            &input,
            "dynamic payload",
            false,
            true,
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(responses).unwrap()["prompt_cache_key"],
            super::prompt_cache_key(&response_settings)
        );
    }

    #[test]
    fn streaming_request_preserves_fork_history_role_task_then_stree() {
        let stree_text = "stree: opening for topic-a";
        let injected = agent_loop::TurnItem {
            item_type: agent_loop::TurnItemType::InjectedContext,
            role: "user".to_owned(),
            content_text: stree_text.to_owned(),
            content_json: json!({"source": "stree", "node_id": "topic-a:stree:1"}),
            tool_call_id: String::new(),
            tool_name: String::new(),
            output_item_id: String::new(),
            phase: None,
            status: None,
            db_row_id: None,
        };
        let input = agent_loop::ModelInput {
            system_instruction: None,
            items: vec![
                agent_loop::TurnItem::user("WARMUP PROMPT"),
                agent_loop::TurnItem::assistant("warmup ready", Value::Null),
                agent_loop::TurnItem::user("BEAR ROLE TASK"),
                injected,
            ],
            available_tools: Vec::new(),
            truncation: TruncationConfig::default(),
        };

        let chat = super::build_chat_completions_request(
            &base_settings(LlmRoute::ChatCompletions),
            &input,
            "must not be appended for a stream",
            true,
            false,
        )
        .unwrap();
        let messages = serde_json::to_value(chat).unwrap()["messages"].clone();
        assert_eq!(messages[0]["content"], "WARMUP PROMPT");
        assert_eq!(messages[1]["content"], "warmup ready");
        assert_eq!(messages[2]["content"], "BEAR ROLE TASK");
        assert_eq!(messages[3]["content"], stree_text);

        let responses = super::build_responses_request(
            &base_settings(LlmRoute::Responses),
            &input,
            "must not be appended for a stream",
            true,
            false,
        )
        .unwrap();
        let wire = serde_json::to_string(&responses).unwrap();
        let warmup = wire.find("WARMUP PROMPT").unwrap();
        let role = wire.find("BEAR ROLE TASK").unwrap();
        let stree = wire.find(stree_text).unwrap();
        assert!(warmup < role && role < stree);
        assert!(!wire.contains("must not be appended for a stream"));
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
                    json!({"kind": "runtime", "id": id, "status": "derived"}),
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
        assert!(output["id"].as_u64().is_some_and(|id| id < 8));
        assert_eq!(output["status"], "derived");
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
    fn think_tool_registration_is_role_controlled() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "analyst.technical".to_string();
        settings.llm.base_url = Some("https://llm.example.com/v1".to_string());
        settings.llm.api_key = Some("test-key".to_string());
        settings.llm.tools = vec!["read_technical_snapshot".to_string()];
        settings.web_search.mode = WebSearchMode::Live;
        assert_eq!(
            super::configured_tool_names(&settings),
            vec!["think", "read_technical_snapshot"]
        );

        settings.llm.think_tool = false;
        assert_eq!(
            super::configured_tool_names(&settings),
            vec!["read_technical_snapshot"]
        );
    }

    #[test]
    fn web_search_is_never_added_outside_the_profile_allowlist() {
        for role in ["trader", "risk.neutral"] {
            let mut settings = base_settings(LlmRoute::Responses);
            settings.role = role.to_string();
            settings.llm.base_url = Some("https://llm.example.com/v1".to_string());
            settings.llm.api_key = Some("test-key".to_string());
            settings.llm.tools = vec![tools::index_tools::READ_INDEXES_NAME.to_string()];
            settings.llm.native_web_search = true;
            settings.web_search.mode = WebSearchMode::Live;

            assert_eq!(
                super::configured_tool_names(&settings),
                vec!["think", tools::index_tools::READ_INDEXES_NAME]
            );
            assert!(super::web_run_runtime_for_settings(&settings).is_none());
        }

        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "manager.research".to_string();
        settings.llm.base_url = Some("https://llm.example.com/v1".to_string());
        settings.llm.api_key = Some("test-key".to_string());
        settings.llm.tools = vec![tools::index_tools::READ_INDEXES_NAME.to_string()];
        settings.web_search.mode = WebSearchMode::Live;
        assert!(!super::configured_tool_names(&settings).contains(&tools::web_run::NAME));
        assert!(super::web_run_runtime_for_settings(&settings).is_none());
    }

    #[test]
    fn news_analyst_only_gets_alpaca_news_when_market_data_gate_is_open() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "analyst.news_macro".to_string();
        settings.llm.think_tool = false;
        settings.llm.tools = vec![tools::alpaca::GET_NEWS_NAME.to_string()];
        settings.tools = Some(tools::ExternalToolConfig::default());

        assert!(super::configured_tool_names(&settings).is_empty());
        settings.tools.as_mut().unwrap().alpaca_market_data = true;
        assert_eq!(
            super::configured_tool_names(&settings),
            vec![tools::alpaca::GET_NEWS_NAME]
        );
    }

    #[test]
    fn web_run_tool_requires_enabled_search() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "analyst.news_macro".to_string();
        settings.llm.base_url = Some("https://llm.example.com/v1".to_string());
        settings.llm.api_key = Some("test-key".to_string());
        settings.llm.think_tool = false;
        settings.llm.tools = vec![tools::web_run::NAME.to_string()];

        assert!(!super::configured_tool_names(&settings).contains(&tools::web_run::NAME));

        settings.web_search.mode = WebSearchMode::Live;
        assert!(super::configured_tool_names(&settings).contains(&tools::web_run::NAME));

        settings.role = "trader".to_string();
        assert!(super::configured_tool_names(&settings).contains(&tools::web_run::NAME));
    }

    #[test]
    fn native_web_search_suppresses_web_run_fallback_tool() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "researcher.web_evidence".to_string();
        settings.llm.base_url = Some("https://llm.example.com/v1".to_string());
        settings.llm.api_key = Some("test-key".to_string());
        settings.llm.think_tool = false;
        settings.llm.native_web_search = true;
        settings.llm.tools = vec![tools::web_run::NAME.to_string()];
        settings.web_search.mode = WebSearchMode::Live;

        assert!(!super::configured_tool_names(&settings).contains(&tools::web_run::NAME));
        assert!(super::web_run_runtime_for_settings(&settings).is_none());
    }

    #[test]
    fn native_web_search_is_added_without_discarding_function_tools() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "researcher.web_evidence".to_string();
        settings.llm.native_web_search = true;
        settings.llm.tools = vec![tools::web_run::NAME.to_owned()];
        settings.web_search.mode = WebSearchMode::Live;
        settings.web_search.context_size = crate::web_search::WebSearchContextSize::High;
        settings.web_search.allowed_domains = vec!["sec.gov".to_string()];
        let input = agent_loop::ModelInput {
            system_instruction: None,
            items: vec![agent_loop::TurnItem::user("research")],
            available_tools: vec![tools::think::NAME.to_owned()],
            truncation: TruncationConfig::default(),
        };

        let request =
            super::build_responses_request(&settings, &input, "research", true, true).unwrap();
        let tools = serde_json::to_value(request).unwrap()["tools"]
            .as_array()
            .cloned()
            .unwrap();
        assert!(tools.iter().any(|tool| tool["type"] == "function"));
        let web_search = tools
            .iter()
            .find(|tool| tool["type"] == "web_search")
            .expect("Responses request must contain native web_search");
        assert_eq!(web_search["search_context_size"], "high");
        assert_eq!(web_search["filters"]["allowed_domains"], json!(["sec.gov"]));
    }

    #[test]
    fn native_web_search_requires_responses_live_and_role_authority() {
        let mut settings = base_settings(LlmRoute::ChatCompletions);
        settings.llm.native_web_search = true;
        settings.llm.base_url = Some("https://llm.example.com/v1".to_string());
        settings.llm.api_key = Some("test-key".to_string());
        assert!(settings
            .llm
            .validate("researcher.web_evidence")
            .unwrap_err()
            .to_string()
            .contains("effective route is not responses"));

        let mut settings = base_settings(LlmRoute::Responses);
        settings.llm.native_web_search = true;
        settings.web_search.mode = WebSearchMode::Live;
        super::validate_native_web_search_configuration(
            &settings.llm,
            &settings.web_search,
            "researcher.web_evidence",
            true,
        )
        .unwrap();

        assert!(super::validate_native_web_search_configuration(
            &settings.llm,
            &settings.web_search,
            "trader",
            false,
        )
        .unwrap_err()
        .to_string()
        .contains("explicitly authorizes web.run"));

        settings.web_search.blocked_domains = vec!["example.com".to_string()];
        assert!(super::validate_native_web_search_configuration(
            &settings.llm,
            &settings.web_search,
            "researcher.web_evidence",
            true,
        )
        .unwrap_err()
        .to_string()
        .contains("cannot honor blocked_domains"));
    }

    fn typed_native_web_record(response: &async_openai::types::responses::Response) -> Value {
        let mut collector = super::NativeWebSearchCollector::default();
        collector.observe(
            &async_openai::types::responses::ResponseStreamEvent::ResponseCompleted(
                async_openai::types::responses::ResponseCompletedEvent {
                    sequence_number: 0,
                    response: response.clone(),
                },
            ),
        );
        collector.record(response)
    }

    #[test]
    fn native_web_search_citations_become_stable_rust_owned_records() {
        let response: async_openai::types::responses::Response = serde_json::from_value(json!({
            "id": "resp-native-1",
            "created_at": 1_754_000_000,
            "completed_at": 1_754_000_005,
            "object": "response",
            "status": "completed",
            "model": "fixture",
            "output": [
                {
                    "type": "web_search_call",
                    "id": "ws-1",
                    "status": "completed",
                    "action": {
                        "type": "search",
                        "query": "SEC ETF filing",
                        "sources": [{"type": "url", "url": "https://www.sec.gov/example/?utm_source=test#fragment"}]
                    }
                },
                {
                    "type": "message",
                    "id": "msg-1",
                    "role": "assistant",
                    "status": "completed",
                    "content": [{
                        "type": "output_text",
                        "text": "Verified filing.",
                        "annotations": [{
                            "type": "url_citation",
                            "start_index": 0,
                            "end_index": 16,
                            "title": "SEC filing",
                            "url": "https://www.sec.gov/example"
                        }]
                    }]
                }
            ]
        }))
        .unwrap();
        let record = typed_native_web_record(&response);

        assert_eq!(record["provider"], "openai_responses_web_search");
        assert_eq!(
            record["search_calls"][0]["action"]["query"],
            "SEC ETF filing"
        );
        assert_eq!(
            record["results"][0]["source_url"],
            "https://www.sec.gov/example"
        );
        assert_eq!(record["results"][0]["authority"], "citation_and_source");
        assert!(record["results"][0]["evidence_id"]
            .as_str()
            .is_some_and(|id| id.starts_with("web-") && id.len() == 68));

        let mut turn = agent_loop::Turn::new(
            "turn-native",
            "session-native",
            "run-native",
            "researcher.web_evidence",
            "",
        );
        turn.emitted_items
            .push(agent_loop::TurnItem::native_web_search(
                "native-web-search-resp-native-1",
                record,
            ));
        let mut final_message = agent_loop::TurnItem::assistant("{}", Value::Null);
        final_message.phase = Some(agent_loop::AgentItemPhase::Final);
        turn.emitted_items.push(final_message);

        let artifact = super::completed_turn_artifact(&turn).unwrap();
        let response_text = artifact["response_text"].as_str().unwrap();
        assert!(response_text.contains(tools::web_run::VERIFIED_RESULTS_MARKER));
        assert!(response_text.contains("https://www.sec.gov/example"));
    }

    #[test]
    fn native_web_search_action_sources_are_verified_without_text_annotations() {
        let response: async_openai::types::responses::Response = serde_json::from_value(json!({
            "id": "resp-native-sources",
            "object": "response",
            "created_at": 1_754_000_000,
            "status": "completed",
            "model": "fixture",
            "output": [{
                "type": "web_search_call",
                "id": "ws-sources",
                "status": "completed",
                "action": {
                    "type": "search",
                    "query": "SEC ETF filing",
                    "sources": [{"type": "url", "url": "https://www.sec.gov/example"}]
                }
            }]
        }))
        .unwrap();
        let record = typed_native_web_record(&response);

        assert_eq!(
            record["results"][0]["source_url"],
            "https://www.sec.gov/example"
        );
    }

    #[test]
    fn native_web_search_records_an_empty_attempt_for_fail_closed_evidence() {
        let response: async_openai::types::responses::Response = serde_json::from_value(json!({
            "id": "resp-native-empty",
            "created_at": 1_754_000_000,
            "object": "response",
            "status": "completed",
            "model": "fixture",
            "output": [{
                "type": "message",
                "id": "msg-empty",
                "role": "assistant",
                "status": "completed",
                "content": [{
                    "type": "output_text",
                    "text": "No source was searched.",
                    "annotations": []
                }]
            }]
        }))
        .unwrap();
        let record = typed_native_web_record(&response);
        assert_eq!(record["search_calls"], json!([]));
        assert_eq!(record["results"], json!([]));

        let mut turn = agent_loop::Turn::new(
            "turn-empty",
            "session-empty",
            "run-empty",
            "researcher.web_evidence",
            "",
        );
        turn.emitted_items
            .push(agent_loop::TurnItem::native_web_search(
                "native-web-search-resp-native-empty",
                record,
            ));
        let mut final_message = agent_loop::TurnItem::assistant("{}", Value::Null);
        final_message.phase = Some(agent_loop::AgentItemPhase::Final);
        turn.emitted_items.push(final_message);

        let artifact = super::completed_turn_artifact(&turn).unwrap();
        assert!(artifact["response_text"].as_str().unwrap().ends_with("[]"));
    }

    #[test]
    fn verify_event_gets_an_internal_search_runtime_without_exposing_web_run() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "analyst.news_macro".to_string();
        settings.llm.think_tool = false;
        settings.llm.tools = vec![tools::verify_event::NAME.to_string()];
        settings.web_search.mode = WebSearchMode::Live;

        assert_eq!(
            super::configured_tool_names(&settings),
            vec![tools::verify_event::NAME]
        );
        assert!(super::web_run_runtime_for_settings(&settings).is_some());
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
    fn new_fork_pins_the_current_role_prompt() {
        let input = super::prepare_fork_turn_input("BULL ROLE PROMPT", true, true, true);

        assert_eq!(
            input,
            "这是一个新的子回合。上一条 assistant 输出只是恢复的 checkpoint 上下文，不是本轮答案。\
             请执行下面的新角色与任务，生成新的回复。\n\nBULL ROLE PROMPT\n\n\
             继续这个既有会话；Rust 会以 `stree: {...}` user message 注入本轮跨角色信息。依据该消息和已有会话上下文完成本轮终端工具动作。"
        );
    }

    #[test]
    fn resumed_topic_turn_keeps_the_existing_role_prompt() {
        let input = super::prepare_fork_turn_input("BULL ROLE PROMPT", true, false, false);

        assert!(input.is_empty());
    }
}
