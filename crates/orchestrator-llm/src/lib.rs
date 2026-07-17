use agent_loop::{
    AgentLoopConfig, ModelEventHandler, ModelStreamEvent, ModelStreamResult, ProjectToolRuntime,
    RigLoopModel, ToolCallRequest, Turn,
};
use anyhow::{bail, Context, Result};
use futures::StreamExt;
use llm_judge::JudgeConfig;
use orchestrator_core::{
    default_project_root, extract_json_artifact, normalize_research_artifact_value,
    validate_analyst_ticker_artifact, validate_research_artifact, AnalystTickerArtifact,
    ResearchArtifact,
};
use rig_core::{
    agent::AgentBuilder,
    client::CompletionClient,
    completion::{CompletionModel, GetTokenUsage, Prompt},
    message::{AssistantContent, Message, Reasoning, ToolChoice},
    providers::openai::{self, responses_api},
    streaming::StreamedAssistantContent,
    OneOrMany,
};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{fs, path::PathBuf, sync::Arc};
use truncation::TruncationConfig;
use uuid::Uuid;
use web_search::{
    validate_web_search_runtime_config, ExaWebSearchProvider, WebSearchConfig, WebSearchMode,
};

pub mod agent_loop;
pub mod llm_judge;
pub mod tools;
pub mod truncation;
pub mod web_search;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmRoute {
    Responses,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmTransport {
    #[default]
    Http,
    Ws,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    JsonArtifact,
    ResearchArtifact,
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
}

impl RoleLlmSettings {
    pub fn validate(&self, role: &str) -> Result<()> {
        if self.model.trim().is_empty() {
            bail!("LLM config for role {role:?} requires model");
        }
        if self.max_turns == Some(0) {
            bail!("LLM config for role {role:?} requires max_turns >= 1");
        }
        if self
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
        if !has_api_key {
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
            bail!("text_verbosity is not supported by the current Rig Responses transport");
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
}

#[derive(Debug, Clone)]
pub struct RigSettings {
    pub role: String,
    pub phase: Option<i64>,
    /// Optional debate topic id so phase-2 debug files do not clobber each other.
    pub topic_id: Option<String>,
    pub tickers: Vec<String>,
    pub output_mode: OutputMode,
    pub llm: RoleLlmSettings,
    pub reasoning_effort_override: Option<String>,
    pub tools: Option<tools::ExternalToolConfig>,
    pub web_search: WebSearchConfig,
    pub truncation: TruncationConfig,
    pub judge: JudgeConfig,
    pub debug: bool,
}

#[derive(Debug, Clone)]
pub struct SteerLoopInput<'a> {
    pub session_id: String,
    pub turn_id: String,
    pub prompt: &'a str,
    pub steer: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AgentLoopOutput {
    pub artifact: Value,
    pub metrics: ModelStreamResult,
    pub turn_id: String,
    pub session_id: String,
}

pub async fn run_rig_agent_loop(settings: &RigSettings, prompt: &str) -> Result<Value> {
    Ok(run_rig_agent_loop_with_metrics(settings, prompt)
        .await?
        .artifact)
}

pub async fn run_rig_agent_loop_with_metrics(
    settings: &RigSettings,
    prompt: &str,
) -> Result<AgentLoopOutput> {
    settings.llm.validate(&settings.role)?;
    validate_fallback_web_search_runtime_config(settings)?;
    let conn = open_loop_connection(settings)?;
    let session_id = loop_session_id(settings);
    let turn_id = format!("turn-{}", Uuid::new_v4());
    let mut turn = Turn::new(
        turn_id,
        session_id,
        loop_run_id(settings),
        settings.role.clone(),
        prompt.to_string(),
    );
    turn.phase = settings.phase;
    turn.tools_disabled = role_disables_tools(&settings.role);
    turn.model_context = format!(
        "role={}\noutput_mode={:?}\ntickers={}\navailable_tools={}",
        settings.role,
        settings.output_mode,
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
    if let Some(web_run) = web_run_runtime_for_settings(settings) {
        tools = tools.with_web_run_runtime(web_run);
    }
    let mut model = RigLoopModel::new(settings.clone());
    let metrics = agent_loop::run_turn(
        &conn,
        &mut turn,
        &mut model,
        &mut tools,
        agent_loop_config_from_settings(settings),
    )
    .await?;
    write_role_end_context(settings, &turn)?;
    let final_text = turn
        .emitted_items
        .iter()
        .rev()
        .find(|item| item.item_type == agent_loop::TurnItemType::AssistantMessage)
        .map(|item| {
            if !item.content_text.trim().is_empty() {
                item.content_text.clone()
            } else {
                item.content_json.to_string()
            }
        })
        .context("agent loop finished without assistant message")?;
    let artifact = parse_final_output(settings, &final_text)?;
    record_jin10_usage_from_artifact(settings, &conn, &turn.turn_id, &artifact)?;
    Ok(AgentLoopOutput {
        artifact,
        metrics,
        turn_id: turn.turn_id,
        session_id: turn.session_id,
    })
}

fn agent_loop_config_from_settings(settings: &RigSettings) -> AgentLoopConfig {
    AgentLoopConfig {
        max_agent_loops: settings.llm.max_turns,
        truncation: settings.truncation.clone(),
        judge: settings.judge.clone(),
        judge_endpoint: settings.llm.base_url.clone(),
        judge_api_key: settings.llm.api_key.clone(),
        debug: settings.debug,
        project_root: Some(debug_project_root(settings)),
        role: settings.role.clone(),
        phase: settings.phase,
        model: settings.llm.model.clone(),
        topic_id: settings.topic_id.clone(),
        ..AgentLoopConfig::default()
    }
}

pub async fn run_rig_agent_steer_loop(
    settings: &RigSettings,
    input: SteerLoopInput<'_>,
) -> Result<Value> {
    Ok(run_rig_agent_steer_loop_with_metrics(settings, input)
        .await?
        .artifact)
}

pub async fn run_rig_agent_steer_loop_with_metrics(
    settings: &RigSettings,
    input: SteerLoopInput<'_>,
) -> Result<AgentLoopOutput> {
    settings.llm.validate(&settings.role)?;
    validate_fallback_web_search_runtime_config(settings)?;
    let conn = open_loop_connection(settings)?;
    // Scope resume detection to this turn_id. Using run_id-latest history made
    // later phase-2 roles see sibling turns as "existing history" and drop their
    // own role prompt (live debate mass max_agent_loops / empty context).
    let prior_history = orchestrator_sql::turn_history_items(&conn, &input.turn_id)?;
    let has_existing_history = !prior_history.is_empty();
    let user_input = if has_existing_history {
        String::new()
    } else {
        input.prompt.to_string()
    };
    let mut turn = Turn::new(
        input.turn_id.clone(),
        input.session_id.clone(),
        loop_run_id(settings),
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
    turn.phase = settings.phase;
    turn.tools_disabled = role_disables_tools(&settings.role);
    turn.model_context = format!(
        "role={}\noutput_mode={:?}\ntickers={}\navailable_tools={}",
        settings.role,
        settings.output_mode,
        settings.tickers.join(","),
        serde_json::to_string(&configured_tool_names(settings))?
    );
    if let Some(steer) = input.steer {
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
    if let Some(web_run) = web_run_runtime_for_settings(settings) {
        tools = tools.with_web_run_runtime(web_run);
    }
    let mut model = RigLoopModel::new(settings.clone());
    let metrics = agent_loop::run_turn(
        &conn,
        &mut turn,
        &mut model,
        &mut tools,
        agent_loop_config_from_settings(settings),
    )
    .await?;
    write_role_end_context(settings, &turn)?;
    let final_text = turn
        .emitted_items
        .iter()
        .rev()
        .find(|item| item.item_type == agent_loop::TurnItemType::AssistantMessage)
        .map(|item| {
            if !item.content_text.trim().is_empty() {
                item.content_text.clone()
            } else {
                item.content_json.to_string()
            }
        })
        .context("agent loop finished without assistant message")?;
    let artifact = parse_final_output(settings, &final_text)?;
    record_jin10_usage_from_artifact(settings, &conn, &turn.turn_id, &artifact)?;
    Ok(AgentLoopOutput {
        artifact,
        metrics,
        turn_id: turn.turn_id,
        session_id: turn.session_id,
    })
}

fn record_jin10_usage_from_artifact(
    settings: &RigSettings,
    conn: &rusqlite::Connection,
    turn_id: &str,
    artifact: &Value,
) -> Result<()> {
    if settings.role != "analyst.news_macro" {
        return Ok(());
    }
    let attention = extract_jin10_attention(artifact);
    if attention.is_empty() {
        return Ok(());
    }
    let run_id = loop_run_id(settings);
    let updated = orchestrator_sql::record_jin10_attention_for_turn(
        conn,
        &run_id,
        turn_id,
        &settings.role,
        settings.phase,
        &attention,
    )?;
    tracing::debug!(
        role = %settings.role,
        turn_id,
        scored = attention.len(),
        updated,
        "recorded jin10 attention scores to ledger"
    );
    Ok(())
}

fn extract_jin10_attention(artifact: &Value) -> Vec<orchestrator_sql::Jin10Attention> {
    use orchestrator_sql::Jin10Attention;
    let mut out: Vec<Jin10Attention> = Vec::new();
    let mut push = |id: &str, score: f64| {
        let id = id.trim();
        if id.is_empty() {
            return;
        }
        if let Some(existing) = out.iter_mut().find(|item| item.id == id) {
            existing.score = existing.score.max(score.clamp(0.0, 1.0));
        } else {
            out.push(Jin10Attention {
                id: id.to_string(),
                score: score.clamp(0.0, 1.0),
            });
        }
    };

    // Preferred: jin10_attention: [{id, score}, ...] or {id: score}
    if let Some(items) = artifact.get("jin10_attention").and_then(Value::as_array) {
        for item in items {
            if let Some(id) = item.get("id").and_then(Value::as_str) {
                let score = item
                    .get("score")
                    .or_else(|| item.get("attention_score"))
                    .and_then(Value::as_f64)
                    .unwrap_or(0.5);
                push(id, score);
            }
        }
    } else if let Some(object) = artifact.get("jin10_attention").and_then(Value::as_object) {
        for (id, score) in object {
            if let Some(score) = score.as_f64() {
                push(id, score);
            }
        }
    }

    // Backward-compatible: bare id lists default to mid attention 0.5
    if let Some(items) = artifact
        .get("referenced_jin10_ids")
        .and_then(Value::as_array)
    {
        for item in items {
            if let Some(id) = item.as_str() {
                push(id, 0.5);
            }
        }
    }

    if let Some(per_ticker) = artifact.get("per_ticker").and_then(Value::as_object) {
        for payload in per_ticker.values() {
            if let Some(items) = payload.get("jin10_attention").and_then(Value::as_array) {
                for item in items {
                    if let Some(id) = item.get("id").and_then(Value::as_str) {
                        let score = item
                            .get("score")
                            .or_else(|| item.get("attention_score"))
                            .and_then(Value::as_f64)
                            .unwrap_or(0.5);
                        push(id, score);
                    }
                }
            }
            if let Some(items) = payload
                .get("referenced_jin10_ids")
                .and_then(Value::as_array)
            {
                for item in items {
                    if let Some(id) = item.as_str() {
                        push(id, 0.5);
                    }
                }
            }
            if let Some(items) = payload.get("key_evidence").and_then(Value::as_array) {
                for evidence in items {
                    if let Some(id) = evidence
                        .get("jin10_id")
                        .and_then(Value::as_str)
                        .or_else(|| evidence.get("id").and_then(Value::as_str))
                    {
                        let trimmed = id.trim();
                        if trimmed.len() == 32 && trimmed.chars().all(|ch| ch.is_ascii_hexdigit()) {
                            let score = evidence
                                .get("attention_score")
                                .or_else(|| evidence.get("score"))
                                .and_then(Value::as_f64)
                                .unwrap_or(0.55);
                            push(trimmed, score);
                        }
                    }
                }
            }
        }
    }
    out
}

fn write_role_end_context(settings: &RigSettings, turn: &Turn) -> Result<()> {
    let Some(path) = role_end_context_path(settings, turn) else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut lines = Vec::new();
    for item in &turn.emitted_items {
        if let Some(value) = end_context_item(item) {
            lines.push(serde_json::to_string(&value)?);
        }
    }
    fs::write(path, lines.join("\n"))?;
    Ok(())
}

fn role_end_context_path(settings: &RigSettings, turn: &Turn) -> Option<PathBuf> {
    let run_dir = settings
        .tools
        .as_ref()
        .and_then(|tools| tools.run_dir.as_ref())?;
    let phase = settings.phase.unwrap_or_default();
    Some(run_dir.join(format!("phase{phase:02}")).join(format!(
        "{}_{}_end_context.jsonl",
        safe_path_part(&settings.role),
        safe_path_part(&turn.turn_id)
    )))
}

pub fn append_debug_llm_record(settings: &RigSettings, record: Value) -> Result<()> {
    if !settings.debug {
        return Ok(());
    }
    let phase = settings.phase.unwrap_or_default();
    let root = settings
        .tools
        .as_ref()
        .map(|tools| tools.project_root.clone())
        .unwrap_or_else(default_project_root);
    let path = root.join(debug_record_relative_path_with_topic(
        phase,
        &settings.role,
        settings.topic_id.as_deref(),
    ));
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create debug dir {}", parent.display()))?;
    }
    // Keep only the latest LLM turn per role/topic (last prompt already includes prior tool context).
    let mut line = serde_json::to_string(&record)?;
    line.push('\n');
    fs::write(&path, line.as_bytes())
        .with_context(|| format!("failed to write debug llm record {}", path.display()))?;
    Ok(())
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

/// Append one timing record to `outputs/debug/time.jsonl` (debug mode only callers).
pub fn append_debug_time_record(project_root: &std::path::Path, record: Value) -> Result<()> {
    append_debug_jsonl_line(project_root, "outputs/debug/time.jsonl", record)
}

/// Append one token-usage record to `outputs/debug/token.jsonl` (debug mode only callers).
pub fn append_debug_token_record(project_root: &std::path::Path, record: Value) -> Result<()> {
    append_debug_jsonl_line(project_root, "outputs/debug/token.jsonl", record)
}

fn append_debug_jsonl_line(
    project_root: &std::path::Path,
    relative: &str,
    mut record: Value,
) -> Result<()> {
    if let Some(object) = record.as_object_mut() {
        object
            .entry("ts_ms".to_string())
            .or_insert_with(|| json!(debug_now_ms()));
    }
    let path = project_root.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create debug dir {}", parent.display()))?;
    }
    let mut line = serde_json::to_string(&record)?;
    line.push('\n');
    use std::io::Write;
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("failed to open debug metrics {}", path.display()))?
        .write_all(line.as_bytes())
        .with_context(|| format!("failed to append debug metrics {}", path.display()))?;
    Ok(())
}

fn debug_now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

/// Resolve project root used for debug artifacts from settings.
pub fn debug_project_root(settings: &RigSettings) -> PathBuf {
    settings
        .tools
        .as_ref()
        .map(|tools| tools.project_root.clone())
        .unwrap_or_else(default_project_root)
}

/// Best-effort time log; never fails the main workflow.
pub fn debug_log_time(project_root: &std::path::Path, record: Value) {
    if let Err(error) = append_debug_time_record(project_root, record) {
        tracing::warn!(error = %error, "failed to write debug time.jsonl");
    }
}

/// Best-effort token log; never fails the main workflow.
pub fn debug_log_token(project_root: &std::path::Path, record: Value) {
    if let Err(error) = append_debug_token_record(project_root, record) {
        tracing::warn!(error = %error, "failed to write debug token.jsonl");
    }
}

pub fn debug_record_relative_path(phase: i64, role: &str) -> PathBuf {
    debug_record_relative_path_with_topic(phase, role, None)
}

pub fn debug_record_relative_path_with_topic(
    phase: i64,
    role: &str,
    topic_id: Option<&str>,
) -> PathBuf {
    let role_part = safe_path_part(role);
    let file_stem = match topic_id.map(str::trim).filter(|value| !value.is_empty()) {
        Some(topic) => format!("{role_part}__{}", safe_path_part(topic)),
        None => role_part,
    };
    PathBuf::from(format!("outputs/debug/phase{phase:02}/{file_stem}.jsonl"))
}

fn end_context_item(item: &agent_loop::TurnItem) -> Option<Value> {
    match item.item_type {
        agent_loop::TurnItemType::UserMessage => Some(json!({
            "role": "user",
            "content": item.content_text
        })),
        agent_loop::TurnItemType::AssistantMessage => Some(json!({
            "role": "assistant",
            "content": item.content_text
        })),
        agent_loop::TurnItemType::ToolCall => {
            let call = item.content_json.get("call")?;
            let arguments = call.get("arguments").cloned().unwrap_or(Value::Null);
            Some(json!({
                "role": "assistant",
                "tool_calls": [{
                    "id": call.get("call_id").and_then(Value::as_str).unwrap_or(&item.tool_call_id),
                    "type": "function",
                    "function": {
                        "name": call.get("name").and_then(Value::as_str).unwrap_or(&item.tool_name),
                        "arguments": serde_json::to_string(&arguments).unwrap_or_else(|_| "null".to_string())
                    }
                }]
            }))
        }
        agent_loop::TurnItemType::ToolResult => Some(json!({
            "role": "tool",
            "tool_call_id": item.tool_call_id,
            "content": item.content_text
        })),
        _ => None,
    }
}

fn safe_path_part(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

fn web_run_runtime(config: &WebSearchConfig) -> Option<tools::WebRunRuntime> {
    match config.mode {
        WebSearchMode::Live => Some(
            tools::WebRunRuntime::new(config.clone())
                .with_truncation(TruncationConfig::default())
                .with_provider(Arc::new(ExaWebSearchProvider::from_config(config))),
        ),
        WebSearchMode::Disabled | WebSearchMode::Cached => None,
    }
}

fn web_run_runtime_for_settings(settings: &RigSettings) -> Option<tools::WebRunRuntime> {
    if uses_web_run_fallback(settings) {
        web_run_runtime(&settings.web_search)
            .map(|runtime| runtime.with_truncation(settings.truncation.clone()))
    } else {
        None
    }
}

fn uses_native_web_search(settings: &RigSettings) -> bool {
    !role_disables_tools(&settings.role)
        && settings.llm.native_web_search
        && settings.web_search.mode == WebSearchMode::Live
}

fn uses_web_run_fallback(settings: &RigSettings) -> bool {
    !role_disables_tools(&settings.role)
        && !uses_native_web_search(settings)
        && settings.web_search.mode == WebSearchMode::Live
}

fn validate_fallback_web_search_runtime_config(settings: &RigSettings) -> Result<()> {
    if uses_web_run_fallback(settings) {
        validate_web_search_runtime_config(&settings.web_search, &settings.role)
    } else {
        Ok(())
    }
}

async fn run_model_text_once(
    settings: &RigSettings,
    _input: &agent_loop::ModelInput,
    prompt: &str,
) -> Result<String> {
    let client = openai_compatible_responses_client(&settings.llm)?;
    let model = client.completion_model(&settings.llm.model);
    let builder = AgentBuilder::new(model).default_max_turns(1);
    let builder = apply_optional_preamble(builder, &settings.llm);
    let builder = if let Some(params) = additional_params(settings) {
        builder.additional_params(params)
    } else {
        builder
    };
    builder
        .build()
        .prompt(prompt)
        .await
        .context("OpenAI-compatible Responses prompt failed")
}

pub async fn run_model_event_stream(
    settings: &RigSettings,
    input: &agent_loop::ModelInput,
    prompt: &str,
    handler: &mut dyn ModelEventHandler,
) -> Result<()> {
    let client = openai_compatible_responses_client(&settings.llm)?;
    let model = client.completion_model(&settings.llm.model);
    match settings.llm.transport {
        LlmTransport::Http => {
            stream_completion_model(settings, input, model, prompt, handler).await
        }
        LlmTransport::Ws => {
            stream_openai_compatible_responses_websocket(settings, input, model, prompt, handler)
                .await
        }
    }
}

async fn stream_openai_compatible_responses_websocket(
    settings: &RigSettings,
    input: &agent_loop::ModelInput,
    model: rig_core::providers::openai::responses_api::ResponsesCompletionModel,
    prompt: &str,
    handler: &mut dyn ModelEventHandler,
) -> Result<()> {
    let builder = completion_request_builder(settings, input, model, prompt);
    let client = openai_compatible_responses_client(&settings.llm)?;
    let mut session = client
        .responses_websocket(settings.llm.model.clone())
        .await
        .context("OpenAI-compatible Responses websocket connection failed")?;
    let result =
        stream_openai_compatible_responses_websocket_events(&mut session, builder.build(), handler)
            .await
            .context("OpenAI-compatible Responses websocket completion failed");
    let close_result = session.close().await;
    if let Err(error) = close_result {
        return Err(anyhow::anyhow!(error))
            .context("OpenAI-compatible Responses websocket close failed");
    }
    result
}

async fn stream_openai_compatible_responses_websocket_events(
    session: &mut responses_api::websocket::ResponsesWebSocketSession,
    request: rig_core::completion::CompletionRequest,
    handler: &mut dyn ModelEventHandler,
) -> Result<()> {
    session.send(request).await?;
    let mut saw_tool_call = false;
    let mut text_message = AssistantTextAccumulator::new();
    loop {
        match session.next_event().await? {
            responses_api::websocket::ResponsesWebSocketEvent::Item(chunk) => match chunk.data {
                responses_api::streaming::ItemChunkKind::OutputTextDelta(delta)
                | responses_api::streaming::ItemChunkKind::RefusalDelta(delta) => {
                    if !delta.delta.is_empty() {
                        let fallback_id = chunk
                            .item_id
                            .clone()
                            .unwrap_or_else(|| format!("ws-text-{}", Uuid::new_v4()));
                        text_message
                            .push_delta(handler, fallback_id, delta.delta)
                            .await?;
                    }
                }
                responses_api::streaming::ItemChunkKind::ReasoningSummaryTextDelta(delta) => {
                    if !delta.delta.is_empty() {
                        handler
                            .handle(ModelStreamEvent::ReasoningSummaryDelta {
                                item_id: chunk
                                    .item_id
                                    .clone()
                                    .unwrap_or_else(|| "ws-reasoning".to_string()),
                                delta: delta.delta,
                            })
                            .await?;
                    }
                }
                responses_api::streaming::ItemChunkKind::OutputItemDone(output) => {
                    match output.item {
                        responses_api::Output::FunctionCall(function_call) => {
                            saw_tool_call = true;
                            handler
                                .handle(ModelStreamEvent::ToolCallCompleted {
                                    tool_call: ToolCallRequest {
                                        call_id: function_call.call_id,
                                        name: tools::resolve_tool_name(&function_call.name),
                                        arguments: function_call.arguments,
                                    },
                                })
                                .await?;
                        }
                        responses_api::Output::Message(message) => {
                            text_message.ensure_item_id(message.id);
                            text_message.complete(handler).await?;
                        }
                        responses_api::Output::Reasoning {
                            id,
                            summary,
                            encrypted_content,
                            ..
                        } => {
                            if let Some(encrypted_content) = encrypted_content {
                                handler
                                    .handle(ModelStreamEvent::ReasoningStateCompleted {
                                        item_id: id.clone(),
                                        encrypted_content,
                                    })
                                    .await?;
                            }
                            for item in summary {
                                let responses_api::ReasoningSummary::SummaryText { text } = item;
                                if !text.trim().is_empty() {
                                    handler
                                        .handle(ModelStreamEvent::ReasoningSummaryDelta {
                                            item_id: id.clone(),
                                            delta: text,
                                        })
                                        .await?;
                                }
                            }
                            handler
                                .handle(ModelStreamEvent::ReasoningSummaryCompleted { item_id: id })
                                .await?;
                        }
                        _ => {}
                    }
                }
                _ => {}
            },
            responses_api::websocket::ResponsesWebSocketEvent::Response(chunk) => {
                match chunk.kind {
                    responses_api::streaming::ResponseChunkKind::ResponseCompleted => {
                        text_message.complete(handler).await?;
                        handler
                            .handle(ModelStreamEvent::ResponseCompleted {
                                // Tools present => continue loop; do not force end_turn.
                                end_turn: !saw_tool_call,
                                raw: serde_json::to_value(chunk.response)
                                    .context("failed to serialize websocket response")?,
                            })
                            .await?;
                        return Ok(());
                    }
                    responses_api::streaming::ResponseChunkKind::ResponseFailed
                    | responses_api::streaming::ResponseChunkKind::ResponseIncomplete => {
                        response_status_result(chunk.response)?;
                    }
                    responses_api::streaming::ResponseChunkKind::ResponseCreated
                    | responses_api::streaming::ResponseChunkKind::ResponseInProgress => {}
                }
            }
            responses_api::websocket::ResponsesWebSocketEvent::Done(done) => {
                text_message.complete(handler).await?;
                handler
                    .handle(ModelStreamEvent::ResponseCompleted {
                        end_turn: !saw_tool_call,
                        raw: done.response,
                    })
                    .await?;
                return Ok(());
            }
            responses_api::websocket::ResponsesWebSocketEvent::Error(error) => {
                bail!("OpenAI-compatible Responses websocket error: {error}");
            }
        }
    }
}

/// Tracks assistant text lifecycle while mapping rig stream chunks to agent events.
struct AssistantTextAccumulator {
    item_id: Option<String>,
    started: bool,
    completed: bool,
}

impl AssistantTextAccumulator {
    fn new() -> Self {
        Self {
            item_id: None,
            started: false,
            completed: false,
        }
    }

    fn ensure_item_id(&mut self, item_id: String) {
        if self.item_id.is_none() {
            self.item_id = Some(item_id);
        }
    }

    async fn push_delta(
        &mut self,
        handler: &mut dyn ModelEventHandler,
        fallback_item_id: String,
        delta: String,
    ) -> Result<()> {
        self.ensure_item_id(fallback_item_id);
        let item_id = self
            .item_id
            .clone()
            .context("assistant text item id missing")?;
        if !self.started {
            handler
                .handle(ModelStreamEvent::AssistantMessageStarted {
                    item_id: item_id.clone(),
                })
                .await?;
            self.started = true;
        }
        handler
            .handle(ModelStreamEvent::AssistantTextDelta { item_id, delta })
            .await?;
        Ok(())
    }

    async fn complete(&mut self, handler: &mut dyn ModelEventHandler) -> Result<()> {
        if self.completed || !self.started {
            return Ok(());
        }
        let item_id = self
            .item_id
            .clone()
            .context("assistant text item id missing")?;
        handler
            .handle(ModelStreamEvent::AssistantMessageCompleted {
                item_id,
                turn_status: agent_loop::TurnStatus::Unknown,
            })
            .await?;
        self.completed = true;
        Ok(())
    }
}

fn token_usage_raw<R>(response: &R) -> Value
where
    R: GetTokenUsage,
{
    response
        .token_usage()
        .map(|usage| {
            json!({
                "usage": {
                    "input_tokens": usage.input_tokens,
                    "output_tokens": usage.output_tokens,
                    "total_tokens": usage.total_tokens,
                    "input_tokens_details": {
                        "cached_tokens": usage.cached_input_tokens
                    },
                    "output_tokens_details": {
                        "reasoning_tokens": usage.reasoning_tokens
                    }
                }
            })
        })
        .unwrap_or(Value::Null)
}

fn response_status_result(response: responses_api::CompletionResponse) -> Result<()> {
    match response.status {
        responses_api::ResponseStatus::Completed => Ok(()),
        responses_api::ResponseStatus::Failed => {
            let message = response
                .error
                .map(|error| {
                    if error.code.is_empty() {
                        error.message
                    } else {
                        format!("{}: {}", error.code, error.message)
                    }
                })
                .unwrap_or_else(|| "OpenAI-compatible Responses websocket failed".to_string());
            bail!("{message}")
        }
        responses_api::ResponseStatus::Incomplete => {
            let reason = response
                .incomplete_details
                .map(|details| details.reason)
                .unwrap_or_else(|| "unknown reason".to_string());
            bail!("OpenAI-compatible Responses websocket incomplete: {reason}")
        }
        status => bail!("OpenAI-compatible Responses websocket ended with status {status:?}"),
    }
}

fn completion_request_builder<M>(
    settings: &RigSettings,
    input: &agent_loop::ModelInput,
    model: M,
    prompt: &str,
) -> rig_core::completion::CompletionRequestBuilder<M>
where
    M: CompletionModel,
{
    let mut builder = model.completion_request(Message::user(prompt.to_string()));
    if let Some(preamble) = settings.llm.effective_preamble() {
        builder = builder.preamble(preamble.to_string());
    }
    if let Some(reasoning) = reasoning_history_message(&settings.llm, input) {
        builder = builder.message(reasoning);
    }
    // Native rig function-calling tools (not text-protocol tool calls).
    let tool_defs = tools::rig_tool_definitions(&input.available_tools);
    if !tool_defs.is_empty() {
        builder = builder.tools(tool_defs).tool_choice(ToolChoice::Auto);
    }
    if let Some(params) = additional_params(settings) {
        builder = builder.additional_params(params);
    }
    builder
}

fn reasoning_history_message(
    settings: &RoleLlmSettings,
    input: &agent_loop::ModelInput,
) -> Option<Message> {
    if !settings.preserve_reasoning_state {
        return None;
    }
    input
        .items
        .iter()
        .rev()
        .find(|item| item.item_type == agent_loop::TurnItemType::ReasoningState)
        .and_then(|item| {
            let id = item
                .content_json
                .get("output_item_id")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())?;
            let encrypted_content = item
                .content_json
                .get("encrypted_content")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())?;
            Some(Message::Assistant {
                id: None,
                content: OneOrMany::one(AssistantContent::Reasoning(
                    Reasoning::encrypted(encrypted_content.to_string()).with_id(id.to_string()),
                )),
            })
        })
}

async fn stream_completion_model<M>(
    settings: &RigSettings,
    input: &agent_loop::ModelInput,
    model: M,
    prompt: &str,
    handler: &mut dyn ModelEventHandler,
) -> Result<()>
where
    M: CompletionModel + Clone,
    M::StreamingResponse: Clone + Unpin + GetTokenUsage,
{
    // Live gateway blips (502/503/429) are common. Retry only when the attempt
    // made no observable progress, so we never double-emit partial stream events.
    const MAX_ATTEMPTS: usize = 5;
    let mut attempt = 0usize;
    loop {
        attempt += 1;
        match stream_completion_model_once(settings, input, model.clone(), prompt, handler).await {
            Ok(()) => return Ok(()),
            Err((error, made_progress))
                if attempt < MAX_ATTEMPTS && !made_progress && is_transient_llm_error(&error) =>
            {
                let backoff_ms = 1_000u64 * (1u64 << (attempt - 1)).min(8);
                tracing::warn!(
                    attempt,
                    backoff_ms,
                    error = %error,
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
    text.contains("503")
        || text.contains("502")
        || text.contains("429")
        || text.contains("bad_response_status_code")
        || text.contains("no healthy upstream")
        || text.contains("timeout")
        || text.contains("timed out")
        || text.contains("connection reset")
        || text.contains("temporarily unavailable")
}

async fn stream_completion_model_once<M>(
    settings: &RigSettings,
    input: &agent_loop::ModelInput,
    model: M,
    prompt: &str,
    handler: &mut dyn ModelEventHandler,
) -> std::result::Result<(), (anyhow::Error, bool)>
where
    M: CompletionModel,
    M::StreamingResponse: Clone + Unpin + GetTokenUsage,
{
    // Map rig's StreamedAssistantContent directly — no custom event-JSON reparse.
    let started = std::time::Instant::now();
    let builder = completion_request_builder(settings, input, model, prompt);
    let mut stream = builder
        .stream()
        .await
        .context("LLM stream failed")
        .map_err(|error| (error, false))?;
    let mut text_message = AssistantTextAccumulator::new();
    let mut saw_tool_call = false;
    let mut final_raw = Value::Null;
    let mut made_progress = false;
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk.context("LLM stream chunk failed") {
            Ok(chunk) => chunk,
            Err(error) => return Err((error, made_progress)),
        };
        match chunk {
            StreamedAssistantContent::Text(text) => {
                made_progress = true;
                text_message
                    .push_delta(
                        handler,
                        format!("msg-{}", Uuid::new_v4()),
                        text.text().to_string(),
                    )
                    .await
                    .map_err(|error| (error, made_progress))?;
            }
            StreamedAssistantContent::Reasoning(reasoning) => {
                if let (Some(id), Some(encrypted_content)) =
                    (reasoning.id.clone(), reasoning.encrypted_content())
                {
                    made_progress = true;
                    handler
                        .handle(ModelStreamEvent::ReasoningStateCompleted {
                            item_id: id,
                            encrypted_content: encrypted_content.to_string(),
                        })
                        .await
                        .map_err(|error| (error, made_progress))?;
                }
                let text = reasoning.display_text();
                if !text.trim().is_empty() {
                    made_progress = true;
                    let item_id = reasoning
                        .id
                        .clone()
                        .unwrap_or_else(|| format!("reasoning-{}", Uuid::new_v4()));
                    handler
                        .handle(ModelStreamEvent::ReasoningSummaryDelta {
                            item_id: item_id.clone(),
                            delta: text,
                        })
                        .await
                        .map_err(|error| (error, made_progress))?;
                    handler
                        .handle(ModelStreamEvent::ReasoningSummaryCompleted { item_id })
                        .await
                        .map_err(|error| (error, made_progress))?;
                }
            }
            StreamedAssistantContent::ReasoningDelta { id, reasoning } => {
                if !reasoning.trim().is_empty() {
                    made_progress = true;
                    handler
                        .handle(ModelStreamEvent::ReasoningSummaryDelta {
                            item_id: id.unwrap_or_else(|| "reasoning-stream".to_string()),
                            delta: reasoning,
                        })
                        .await
                        .map_err(|error| (error, made_progress))?;
                }
            }
            StreamedAssistantContent::ToolCall { tool_call, .. } => {
                made_progress = true;
                saw_tool_call = true;
                handler
                    .handle(ModelStreamEvent::ToolCallCompleted {
                        tool_call: ToolCallRequest {
                            call_id: tool_call.call_id.unwrap_or(tool_call.id),
                            name: tools::resolve_tool_name(&tool_call.function.name),
                            arguments: tool_call.function.arguments,
                        },
                    })
                    .await
                    .map_err(|error| (error, made_progress))?;
            }
            StreamedAssistantContent::ToolCallDelta { .. } => {}
            StreamedAssistantContent::Final(response) => {
                final_raw = token_usage_raw(&response);
            }
        }
    }
    text_message
        .complete(handler)
        .await
        .map_err(|error| (error, made_progress))?;
    handler
        .handle(ModelStreamEvent::ResponseCompleted {
            end_turn: !saw_tool_call,
            raw: final_raw.clone(),
        })
        .await
        .map_err(|error| (error, made_progress))?;
    if settings.debug {
        let elapsed_ms = started.elapsed().as_millis();
        let root = debug_project_root(settings);
        let usage = agent_loop::extract_token_usage(&final_raw);
        debug_log_time(
            &root,
            json!({
                "kind": "llm_stream",
                "name": settings.role,
                "role": settings.role,
                "phase": settings.phase,
                "topic_id": settings.topic_id,
                "model": settings.llm.model,
                "transport": "http",
                "elapsed_ms": elapsed_ms,
                "llm_ms": elapsed_ms,
                "tool_ms": 0,
                "wait_ms": 0,
                "saw_tool_call": saw_tool_call,
            }),
        );
        debug_log_token(
            &root,
            json!({
                "kind": "llm_stream",
                "role": settings.role,
                "phase": settings.phase,
                "topic_id": settings.topic_id,
                "model": settings.llm.model,
                "transport": "http",
                "input_tokens": usage.input_tokens,
                "output_tokens": usage.output_tokens,
                "cached_tokens": usage.cached_tokens,
                "reasoning_tokens": usage.reasoning_tokens,
                "total_tokens": usage.total_tokens,
                "elapsed_ms": elapsed_ms,
                "llm_ms": elapsed_ms,
                "tool_ms": 0,
                "wait_ms": 0,
                "saw_tool_call": saw_tool_call,
            }),
        );
    }
    Ok(())
}

fn apply_optional_preamble<M, P, ToolState>(
    builder: AgentBuilder<M, P, ToolState>,
    settings: &RoleLlmSettings,
) -> AgentBuilder<M, P, ToolState>
where
    M: CompletionModel,
    P: rig_core::agent::PromptHook<M>,
{
    if let Some(preamble) = settings.effective_preamble() {
        builder.preamble(preamble)
    } else {
        builder
    }
}

fn parse_final_output(settings: &RigSettings, text: &str) -> Result<Value> {
    match settings.output_mode {
        OutputMode::ResearchArtifact => {
            let value = extract_json_artifact(text)?;
            let value = normalize_research_artifact_value(value, &settings.tickers)
                .context("failed to normalize research artifact JSON")?;
            validate_optional_role(settings, &value)?;
            let artifact: ResearchArtifact =
                serde_json::from_value(value).context("failed to parse research artifact JSON")?;
            validate_research_artifact(&artifact, &settings.tickers)
                .map_err(|error| anyhow::anyhow!(error))?;
            serde_json::to_value(artifact).context("failed to serialize research artifact")
        }
        OutputMode::JsonArtifact => {
            if agent_loop::assistant_message_needs_follow_up(text) {
                bail!("agent ended with an action note instead of a JSON artifact");
            }
            match parse_json_object_artifact(text) {
                Ok(artifact) => {
                    validate_json_artifact_contract(settings, &artifact)?;
                    Ok(artifact)
                }
                Err(error) if requires_structured_final_artifact(&settings.role) => Err(error)
                    .with_context(|| {
                        format!(
                            "{role} requires a JSON artifact; refusing text fallback",
                            role = settings.role
                        )
                    }),
                Err(_) => Ok(text_fallback_artifact(settings, text)),
            }
        }
    }
}

fn text_fallback_artifact(settings: &RigSettings, text: &str) -> Value {
    let per_ticker = settings
        .tickers
        .iter()
        .map(|ticker| {
            (
                ticker.clone(),
                json!({
                    "direction": "unobserved",
                    "confidence": 0.0,
                    "report": text,
                    "data_gaps": ["model returned non-JSON artifact; raw text preserved"]
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    json!({
        "id": settings.role,
        "role": settings.role,
        "status": "degraded",
        "degraded": true,
        "report": text,
        "per_ticker": per_ticker
    })
}

fn requires_structured_final_artifact(role: &str) -> bool {
    role.starts_with("analyst.")
        || matches!(
            role,
            "researcher.bull.initial"
                | "researcher.bear.initial"
                | "researcher.bull.interaction"
                | "researcher.bear.interaction"
                | "mediator.topic"
                | "mediator.topic_controller"
                | "trader"
                | "risk.aggressive"
                | "risk.neutral"
                | "risk.conservative"
                | "allocation.manager"
        )
}

fn validate_json_artifact_contract(settings: &RigSettings, artifact: &Value) -> Result<()> {
    if settings.role.starts_with("analyst.") {
        return validate_analyst_artifact_contract(settings, artifact);
    }
    match settings.role.as_str() {
        "researcher.bull.initial" | "researcher.bear.initial" => {
            validate_seed_packet_contract(settings, artifact)
        }
        "researcher.bull.interaction" | "researcher.bear.interaction" => {
            validate_interaction_packet_contract(settings, artifact)
        }
        "mediator.topic_controller" => validate_controller_packet_contract(settings, artifact),
        "mediator.topic" => validate_topic_generation_contract(settings, artifact),
        "trader" => validate_trade_intent_contract(settings, artifact),
        "risk.aggressive" | "risk.neutral" | "risk.conservative" => {
            validate_risk_constraints_contract(settings, artifact)
        }
        "allocation.manager" => validate_allocation_artifact_contract(settings, artifact),
        _ => Ok(()),
    }
}

fn validate_analyst_artifact_contract(settings: &RigSettings, artifact: &Value) -> Result<()> {
    let actual_role = artifact
        .get("role")
        .and_then(Value::as_str)
        .context("analyst artifact requires role")?;
    if actual_role != settings.role {
        bail!(
            "analyst artifact role mismatch: expected {:?}, got {:?}",
            settings.role,
            actual_role
        );
    }
    if actual_role == "analyst.news_macro" {
        if let Some(attention) = artifact.get("jin10_attention") {
            if !attention.is_array() && !attention.is_object() {
                bail!(
                    "analyst.news_macro jin10_attention must be an array of {{id,score}} or a map id->score"
                );
            }
        }
        if let Some(ids) = artifact.get("referenced_jin10_ids") {
            if !ids.is_array() {
                bail!("analyst.news_macro referenced_jin10_ids must be an array of jin10 ids");
            }
        }
    }
    let Some(per_ticker) = artifact.get("per_ticker").and_then(Value::as_object) else {
        bail!("analyst artifact requires per_ticker object");
    };
    let expected = settings
        .tickers
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let actual = per_ticker
        .keys()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    if actual != expected {
        bail!(
            "analyst artifact per_ticker keys mismatch: expected {:?}, got {:?}",
            expected,
            actual
        );
    }
    for ticker in &settings.tickers {
        let Some(payload) = per_ticker.get(ticker) else {
            bail!("analyst artifact missing per_ticker.{ticker}");
        };
        let parsed: AnalystTickerArtifact = serde_json::from_value(payload.clone())
            .with_context(|| format!("invalid analyst per_ticker.{ticker} payload"))?;
        validate_analyst_ticker_artifact(&parsed)
            .map_err(|error| anyhow::anyhow!("analyst per_ticker.{ticker}: {error}"))?;
    }
    Ok(())
}

fn validate_seed_packet_contract(settings: &RigSettings, artifact: &Value) -> Result<()> {
    require_exact_role(settings, artifact)?;
    let expected_type = if settings.role.contains("bull") {
        "bull_seed_packet"
    } else {
        "bear_seed_packet"
    };
    require_exact_string(artifact, "artifact_type", expected_type)?;
    require_non_empty_string(artifact, "topic_id")?;
    let claims = require_array(artifact, "claims")?;
    if claims.is_empty() {
        bail!("initial seed packet claims must not be empty");
    }
    let constraint_field = if settings.role.contains("bull") {
        "known_bear_constraint"
    } else {
        "known_bull_constraint"
    };
    for (index, claim) in claims.iter().enumerate() {
        let claim = claim
            .as_object()
            .with_context(|| format!("initial seed claim {index} must be an object"))?;
        let claim = Value::Object(claim.clone());
        require_non_empty_string(&claim, "claim_id")?;
        require_non_empty_string(&claim, "decision_hinge")?;
        require_non_empty_string(&claim, "claim")?;
        require_array(&claim, "evidence_refs")?;
        require_number_in_range(&claim, "confidence", 0.0, 1.0)?;
        require_non_empty_string(&claim, constraint_field)?;
        if claim
            .get("needs_mediator_check")
            .and_then(Value::as_bool)
            .is_none()
        {
            bail!("initial seed claim requires needs_mediator_check boolean");
        }
    }
    require_non_empty_string(artifact, "summary")?;
    require_object(artifact, "reducer_checks")?;
    Ok(())
}

fn validate_interaction_packet_contract(settings: &RigSettings, artifact: &Value) -> Result<()> {
    require_exact_role(settings, artifact)?;
    let expected_type = if settings.role.contains("bull") {
        "bull_debate_packet"
    } else {
        "bear_debate_packet"
    };
    require_exact_string(artifact, "artifact_type", expected_type)?;
    require_non_empty_string(artifact, "topic_id")?;
    require_non_empty_string(artifact, "reply_to")?;
    let stance = require_non_empty_string(artifact, "stance")?;
    if !matches!(
        stance,
        "accept" | "rebut" | "downgrade" | "needs_evidence" | "no_new_info"
    ) {
        bail!("interaction packet has invalid stance {stance:?}");
    }
    require_non_empty_string(artifact, "claim")?;
    require_array(artifact, "evidence_refs")?;
    require_number_in_range(artifact, "confidence", 0.0, 1.0)?;
    require_non_empty_string(artifact, "send_to_mediator")?;
    require_array(artifact, "blocked_ack")?;
    if stance != "no_new_info" {
        require_object(artifact, "steelman")?;
    }
    Ok(())
}

fn validate_controller_packet_contract(settings: &RigSettings, artifact: &Value) -> Result<()> {
    require_exact_role(settings, artifact)?;
    require_exact_string(artifact, "artifact_type", "topic_controller_packet")?;
    require_non_empty_string(artifact, "topic_id")?;
    require_array(artifact, "claim_ledger")?;
    require_array(artifact, "accepted_for_opponent")?;
    require_array(artifact, "rejected_to_origin")?;
    require_array(artifact, "blocked_claims")?;
    require_array(artifact, "agreed_facts")?;
    let decision_hinges = require_array(artifact, "decision_hinges")?;
    for (index, hinge) in decision_hinges.iter().enumerate() {
        require_non_empty_string(hinge, "hinge")
            .with_context(|| format!("decision_hinges[{index}] is invalid"))?;
        let refs = require_array(hinge, "evidence_refs")?;
        if refs.is_empty() {
            bail!("decision_hinges[{index}].evidence_refs must not be empty");
        }
    }
    require_number_in_range(artifact, "info_gain_score", 0.0, 1.0)?;
    require_object(artifact, "next_steers")?;
    require_object(artifact, "topic_summary_delta")?;
    let soft_control = require_object(artifact, "soft_control")?;
    if soft_control
        .get("should_continue")
        .and_then(Value::as_bool)
        .is_none()
    {
        bail!("controller packet requires soft_control.should_continue boolean");
    }
    if soft_control
        .get("stop_reason")
        .and_then(Value::as_str)
        .is_none_or(|value| value.trim().is_empty())
    {
        bail!("controller packet requires soft_control.stop_reason string");
    }
    require_object(artifact, "reducer_checks")?;
    Ok(())
}

fn validate_topic_generation_contract(settings: &RigSettings, artifact: &Value) -> Result<()> {
    require_exact_role(settings, artifact)?;
    require_exact_string(
        artifact,
        "artifact_type",
        "phase2_topic_generation_artifact",
    )?;
    require_array(artifact, "topics")?;
    require_non_empty_string(artifact, "summary")?;
    require_object(artifact, "reducer_checks")?;
    Ok(())
}

fn validate_allocation_artifact_contract(settings: &RigSettings, artifact: &Value) -> Result<()> {
    validate_optional_role(settings, artifact)?;
    let weights = require_object(artifact, "weights")?;
    if weights.is_empty() {
        bail!("allocation artifact weights must not be empty");
    }
    let mut total_weight = 0.0;
    let mut equity_weight = 0.0;
    for (ticker, entry) in weights {
        if ticker.eq_ignore_ascii_case("VIX") {
            bail!("VIX is a regime signal and must not appear in allocation weights");
        }
        let weight = entry
            .as_f64()
            .or_else(|| entry.get("weight").and_then(Value::as_f64))
            .with_context(|| format!("allocation weight for {ticker} must be numeric"))?;
        if !(0.0..=1.0).contains(&weight) {
            bail!("allocation weight for {ticker} must be in 0..1");
        }
        total_weight += weight;
        if ticker != "cash_hedge" {
            equity_weight += weight;
        }
    }
    if artifact
        .get("per_ticker")
        .and_then(Value::as_object)
        .is_some_and(|items| {
            items
                .keys()
                .any(|ticker| ticker.eq_ignore_ascii_case("VIX"))
        })
    {
        bail!("VIX is a regime signal and must not appear in allocation per_ticker");
    }
    if (total_weight - 1.0).abs() > 0.03 {
        bail!("allocation weights must sum to approximately 1.0 (got {total_weight})");
    }
    if let Some(total_equity) = artifact
        .get("total_equity_exposure")
        .and_then(Value::as_f64)
    {
        if (total_equity - equity_weight).abs() > 0.03 {
            bail!(
                "total_equity_exposure {total_equity} does not match non-cash weights {equity_weight}"
            );
        }
    }
    require_non_empty_string(artifact, "correlation_note")?;
    Ok(())
}

fn validate_trade_intent_contract(settings: &RigSettings, artifact: &Value) -> Result<()> {
    validate_optional_role(settings, artifact)?;
    if artifact.get("action").and_then(Value::as_str).is_some() {
        return validate_trade_intent_entry(artifact);
    }
    if let Some(per_ticker) = artifact.get("per_ticker").and_then(Value::as_object) {
        for (ticker, entry) in per_ticker {
            validate_trade_intent_entry(entry)
                .with_context(|| format!("per_ticker.{ticker} trade intent invalid"))?;
        }
        return Ok(());
    }
    bail!("trade intent requires top-level action or per_ticker structure");
}

fn validate_trade_intent_entry(entry: &Value) -> Result<()> {
    let action = require_non_empty_string(entry, "action")?;
    if !matches!(action, "Buy" | "Sell" | "Hold") {
        bail!("trade intent action must be Buy, Sell, or Hold");
    }
    let position_size = require_non_empty_string(entry, "position_size")?;
    let position_cap = parse_position_upper_bound(position_size)
        .context("trade intent position_size must be a percentage or percentage range")?;
    if action == "Hold" && position_cap > f64::EPSILON {
        bail!("Hold trade intent must use position_size=0%");
    }
    require_non_empty_string(entry, "rationale")?;
    for field in ["entry_price", "stop_loss"] {
        if let Some(value) = entry.get(field) {
            if !value.is_null() && !value.is_string() {
                bail!("trade intent {field} must be a string or null");
            }
        }
    }
    Ok(())
}

fn parse_position_upper_bound(value: &str) -> Option<f64> {
    value
        .split(['-', '–', '—'])
        .filter_map(|part| {
            part.trim()
                .strip_suffix('%')
                .and_then(|percent| percent.trim().parse::<f64>().ok())
                .map(|percent| (percent / 100.0).clamp(0.0, 1.0))
        })
        .max_by(f64::total_cmp)
}

fn validate_risk_constraints_contract(settings: &RigSettings, artifact: &Value) -> Result<()> {
    validate_optional_role(settings, artifact)?;
    let stance = require_non_empty_string(artifact, "stance")?;
    let expected_stance = settings.role.strip_prefix("risk.").unwrap_or_default();
    if stance != expected_stance {
        bail!("risk stance mismatch: expected {expected_stance:?}, got {stance:?}");
    }
    require_non_empty_string(artifact, "argument")?;
    require_non_empty_string(artifact, "recommended_adjustment")?;
    let stop_type = require_non_empty_string(artifact, "stop_type")?;
    if !matches!(
        stop_type,
        "none" | "tight" | "trailing" | "event_based" | "time_based"
    ) {
        bail!("risk stop_type is invalid: {stop_type:?}");
    }
    require_number_in_range(artifact, "max_drawdown_pct", 0.0, 1.0)?;
    require_number_in_range(artifact, "position_cap_pct", 0.0, 1.0)?;
    require_number_in_range(artifact, "constraint_confidence", 0.0, 1.0)?;
    for field in [
        "rebalance_trigger",
        "risk_off_trigger",
        "review_window",
        "cash_hedge_recommendation",
    ] {
        require_non_empty_string(artifact, field)?;
    }
    Ok(())
}

fn validate_optional_role(settings: &RigSettings, artifact: &Value) -> Result<()> {
    if let Some(role) = artifact.get("role") {
        let role = role
            .as_str()
            .context("artifact role must be a string when provided")?;
        if role != settings.role && !settings.role.ends_with(&format!(".{role}")) {
            bail!(
                "artifact role mismatch: expected {:?}, got {:?}",
                settings.role,
                role
            );
        }
    }
    Ok(())
}

fn require_exact_role<'a>(settings: &RigSettings, artifact: &'a Value) -> Result<&'a str> {
    let role = artifact
        .get("role")
        .and_then(Value::as_str)
        .context("artifact requires role")?;
    if role != settings.role {
        bail!(
            "artifact role mismatch: expected {:?}, got {:?}",
            settings.role,
            role
        );
    }
    Ok(role)
}

fn require_exact_string<'a>(artifact: &'a Value, field: &str, expected: &str) -> Result<&'a str> {
    let actual = require_non_empty_string(artifact, field)?;
    if actual != expected {
        bail!(
            "artifact {field} mismatch: expected {:?}, got {:?}",
            expected,
            actual
        );
    }
    Ok(actual)
}

fn require_non_empty_string<'a>(artifact: &'a Value, field: &str) -> Result<&'a str> {
    artifact
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("artifact requires non-empty {field} string"))
}

fn require_array<'a>(artifact: &'a Value, field: &str) -> Result<&'a Vec<Value>> {
    artifact
        .get(field)
        .and_then(Value::as_array)
        .with_context(|| format!("artifact requires {field} array"))
}

fn require_object<'a>(
    artifact: &'a Value,
    field: &str,
) -> Result<&'a serde_json::Map<String, Value>> {
    artifact
        .get(field)
        .and_then(Value::as_object)
        .with_context(|| format!("artifact requires {field} object"))
}

fn require_number_in_range(artifact: &Value, field: &str, min: f64, max: f64) -> Result<f64> {
    let value = artifact
        .get(field)
        .and_then(Value::as_f64)
        .with_context(|| format!("artifact requires numeric {field}"))?;
    if !value.is_finite() || !(min..=max).contains(&value) {
        bail!("artifact {field} must be within {min}..={max}");
    }
    Ok(value)
}

fn parse_json_object_artifact(text: &str) -> Result<Value> {
    let value = extract_json_artifact(text)
        .or_else(|_| extract_artifact_from_event_text(text))
        .or_else(|_| extract_embedded_json_object(text))?;
    if !value.is_object() {
        bail!("JSON artifact must be an object");
    }
    Ok(value)
}

fn extract_artifact_from_event_text(text: &str) -> Result<Value> {
    let mut best = None;
    let stream = serde_json::Deserializer::from_str(text).into_iter::<Value>();
    for value in stream {
        let value = value?;
        if value.get("type").and_then(Value::as_str) != Some("assistant_text_delta") {
            continue;
        }
        let Some(delta) = value.get("delta").and_then(Value::as_str) else {
            continue;
        };
        if let Ok(value) =
            extract_json_artifact(delta).or_else(|_| extract_embedded_json_object(delta))
        {
            if value.is_object() {
                best = Some(value);
            }
        }
    }
    best.context("event text did not contain artifact delta")
}

fn extract_embedded_json_object(text: &str) -> Result<Value> {
    let mut depth = 0i64;
    let mut start = None;
    let mut in_string = false;
    let mut escape = false;
    let mut last = None;
    for (index, ch) in text.char_indices() {
        if in_string {
            if escape {
                escape = false;
            } else if ch == '\\' {
                escape = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '{' => {
                if depth == 0 {
                    start = Some(index);
                }
                depth += 1;
            }
            '}' => {
                depth -= 1;
                if depth == 0 {
                    if let Some(start) = start {
                        last = Some((start, index + ch.len_utf8()));
                    }
                }
            }
            _ => {}
        }
    }
    let Some((start, end)) = last else {
        bail!("model response did not contain embedded JSON object");
    };
    serde_json::from_str(&text[start..end]).context("failed to parse embedded JSON object")
}

fn open_loop_connection(settings: &RigSettings) -> Result<Connection> {
    let db_path = settings
        .tools
        .as_ref()
        .and_then(|tools| tools.db_path.clone())
        .unwrap_or_else(|| default_project_root().join("outputs/agent_loop.sqlite"));
    orchestrator_sql::connect(db_path)
}

fn loop_session_id(settings: &RigSettings) -> String {
    let run_id = loop_run_id(settings);
    format!("{run_id}:{}", settings.role)
}

fn loop_run_id(settings: &RigSettings) -> String {
    settings
        .tools
        .as_ref()
        .and_then(|tools| {
            tools
                .run_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
                .or_else(|| {
                    tools
                        .run_dir
                        .as_ref()
                        .and_then(|path| path.file_name())
                        .and_then(|name| name.to_str())
                        .map(ToString::to_string)
                })
        })
        .unwrap_or_else(|| "agent-loop".to_string())
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

fn openai_compatible_responses_client(settings: &RoleLlmSettings) -> Result<openai::Client> {
    let api_key = openai_compatible_api_key(settings)?;
    let base_url = openai_compatible_base_url(settings)?;
    openai::Client::builder()
        .api_key(&api_key)
        .base_url(base_url)
        .build()
        .context("failed to build OpenAI-compatible responses client")
}

pub fn additional_params(settings: &RigSettings) -> Option<Value> {
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

fn configured_tool_names(settings: &RigSettings) -> Vec<&str> {
    if role_disables_tools(&settings.role) {
        return Vec::new();
    }
    let mut names = Vec::new();
    if settings.llm.think_tool {
        names.push("think");
    }
    names.extend(settings.llm.tools.iter().map(String::as_str));
    if uses_web_run_fallback(settings) {
        names.push(tools::WEB_RUN_TOOL_NAME);
    }
    names
}

fn role_disables_tools(role: &str) -> bool {
    // manager.research may call read_run_context for phase_summaries / attention.
    // Trader / risk / PM stay tool-free.
    matches!(
        role,
        "trader" | "portfolio.manager" | "allocation.manager"
    ) || role.starts_with("risk.")
}

fn validate_tool_name(name: &str) -> Result<()> {
    if tools::tool_names().contains(&name) {
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
        db_path: std::env::var_os("ORCH_DB_PATH").map(PathBuf::from),
        run_dir: None,
        run_id: None,
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
        phase00_index: None,
    }
}

pub fn mock_role_artifact(role: &str, tickers: &[String]) -> Value {
    match role {
        "manager.research" => orchestrator_sql::write::mock_research_artifact(tickers),
        "trader" => mock_trader_artifact(),
        "risk.aggressive" | "risk.conservative" | "risk.neutral" => mock_risk_artifact(role),
        "portfolio.manager" => mock_portfolio_artifact(),
        "allocation.manager" => mock_allocation_artifact(tickers),
        _ => {
            let per_ticker = tickers
                .iter()
                .map(|ticker| {
                    (
                        ticker.clone(),
                        serde_json::json!({
                            "direction": "neutral",
                            "confidence": 0.5,
                            "report": format!("Mock report for {ticker} from {role}.")
                        }),
                    )
                })
                .collect::<serde_json::Map<_, _>>();
            serde_json::json!({
                "id": role,
                "role": role,
                "report": format!("Mock report from {role}."),
                "per_ticker": per_ticker
            })
        }
    }
}

fn mock_trader_artifact() -> Value {
    serde_json::json!({
        "id": "trader",
        "role": "trader",
        "action": "Hold",
        "entry_price": null,
        "stop_loss": null,
        "position_size": "0%",
        "rationale": "Mock trader plan based on neutral research."
    })
}

fn mock_risk_artifact(role: &str) -> Value {
    let stance = role.strip_prefix("risk.").unwrap_or("neutral");
    let position_cap_pct = match stance {
        "aggressive" => 0.5,
        "conservative" => 0.15,
        _ => 0.3,
    };
    serde_json::json!({
        "id": role,
        "role": role,
        "stance": stance,
        "argument": format!("Mock {stance} risk argument."),
        "unique_risk_contribution": format!("Mock {stance} stance-specific constraint."),
        "disagreement_with_prior": "Mock review records whether prior constraints require a stance-specific change.",
        "no_new_information": false,
        "recommended_adjustment": "No change in mock mode.",
        "stop_type": "event_based",
        "max_drawdown_pct": 0.1,
        "position_cap_pct": position_cap_pct,
        "rebalance_trigger": "Mock rebalance trigger.",
        "risk_off_trigger": "Mock risk-off trigger.",
        "review_window": "1d",
        "cash_hedge_recommendation": "Maintain cash reserve.",
        "constraint_confidence": 0.8
    })
}

fn mock_portfolio_artifact() -> Value {
    serde_json::json!({
        "id": "portfolio.manager",
        "role": "portfolio.manager",
        "rating": "Hold",
        "execution_summary": "Mock final portfolio decision.",
        "investment_thesis": "Mock probability analysis.",
        "target_price": null,
        "horizon": "1-5 trading days",
        "risk_controls": ["Keep allocation capped in mock mode"],
        "rationale": "Mock portfolio manager decision based on neutral research and risk debate."
    })
}

fn mock_allocation_artifact(tickers: &[String]) -> Value {
    let investable: Vec<&String> = tickers.iter().filter(|t| t.as_str() != "VIX").collect();
    if investable.is_empty() {
        return serde_json::json!({
            "id": "allocation.manager",
            "role": "allocation.manager",
            "weights": {
                "cash_hedge": {
                    "weight": 1.0,
                    "rationale": "Mock cash allocation; no investable tickers"
                }
            },
            "total_equity_exposure": 0.0,
            "vix_regime": "normal",
            "correlation_note": "Mock correlation note",
            "summary": "Mock allocation artifact.",
            "allocation_method": "mock"
        });
    }
    let count = investable.len().max(1);
    let equity = 0.6_f64;
    let per = (equity / count as f64 * 10_000.0).round() / 10_000.0;
    let cash = (1.0 - per * count as f64).max(0.0);
    let mut weights = serde_json::Map::new();
    for ticker in &investable {
        weights.insert(
            ticker.to_string(),
            serde_json::json!({
                "weight": per,
                "rationale": format!("Mock allocation for {}", ticker)
            }),
        );
    }
    weights.insert(
        "cash_hedge".to_string(),
        serde_json::json!({
            "weight": cash,
            "rationale": "Mock cash hedge"
        }),
    );
    serde_json::json!({
        "id": "allocation.manager",
        "role": "allocation.manager",
        "weights": weights,
        "total_equity_exposure": per * count as f64,
        "vix_regime": "normal",
        "correlation_note": "Mock correlation note",
        "summary": "Mock allocation artifact.",
        "allocation_method": "mock"
    })
}

#[cfg(test)]
mod tests {
    use super::{
        agent_loop, llm_judge::JudgeConfig, tools, LlmRoute, LlmTransport, OutputMode, RigSettings,
        RoleLlmSettings, TruncationConfig,
    };
    use crate::web_search::{WebSearchConfig, WebSearchMode};
    use crate::{AssistantTextAccumulator, ModelEventHandler, ModelStreamEvent};
    use anyhow::Result;
    use serde_json::json;
    use std::{future::Future, path::PathBuf, pin::Pin};

    fn base_settings(route: LlmRoute) -> RigSettings {
        RigSettings {
            role: "manager.research".to_string(),
            phase: None,
            topic_id: None,
            tickers: vec!["TQQQ".to_string()],
            output_mode: OutputMode::ResearchArtifact,
            llm: RoleLlmSettings {
                route,
                model: "gpt-5.4".to_string(),
                preamble: None,
                max_turns: Some(6),
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
            },
            reasoning_effort_override: None,
            tools: None,
            web_search: WebSearchConfig::default(),
            truncation: TruncationConfig::default(),
            judge: JudgeConfig::default(),
            debug: false,
        }
    }

    #[test]
    fn external_tool_names_are_registered() {
        assert_eq!(
            tools::tool_names(),
            vec![
                "read_run_context",
                "fetch_jin10_flash",
                "fetch_youtube_transcript",
                "fetch_wayinvideo_transcript",
                "run_technical_indicators",
                "fetch_last30days_context",
            ]
        );
    }

    #[test]
    fn route_deserializes_supported_values() {
        assert_eq!(
            serde_json::from_value::<LlmRoute>(json!("responses")).unwrap(),
            LlmRoute::Responses
        );
        assert!(serde_json::from_value::<LlmRoute>(json!("chat_completions")).is_err());
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
    fn end_context_item_matches_provider_message_shape() {
        let call = agent_loop::ToolCallRequest {
            call_id: "call_file_001".to_string(),
            name: "read_file".to_string(),
            arguments: json!({"path": "README.md"}),
        };
        let tool_result = agent_loop::ToolResultItem {
            call_id: "call_file_001".to_string(),
            name: "read_file".to_string(),
            status: "completed".to_string(),
            output: json!({"content": "# My Project"}),
            error: None,
        };

        assert_eq!(
            super::end_context_item(&agent_loop::TurnItem::user("帮我查看 README.md")).unwrap(),
            json!({"role": "user", "content": "帮我查看 README.md"})
        );
        assert_eq!(
            super::end_context_item(&agent_loop::TurnItem::tool_call(&call)).unwrap(),
            json!({
                "role": "assistant",
                "tool_calls": [{
                    "id": "call_file_001",
                    "type": "function",
                    "function": {
                        "name": "read_file",
                        "arguments": "{\"path\":\"README.md\"}"
                    }
                }]
            })
        );
        assert_eq!(
            super::end_context_item(&agent_loop::TurnItem::tool_result(
                &tool_result,
                &TruncationConfig::default(),
            ))
            .unwrap(),
            json!({"role": "tool", "tool_call_id": "call_file_001", "content": "# My Project"})
        );
        assert_eq!(
            super::safe_path_part("analyst.news_macro"),
            "analyst_news_macro"
        );
    }

    #[test]
    fn role_end_context_paths_do_not_collide_for_parallel_turns() {
        let temp = tempfile::tempdir().unwrap();
        let mut settings = base_settings(LlmRoute::Responses);
        settings.phase = Some(25);
        settings.role = "researcher.bull.interaction".to_string();
        settings.tools = Some(tools::ExternalToolConfig {
            project_root: temp.path().to_path_buf(),
            db_path: None,
            run_dir: Some(temp.path().to_path_buf()),
            run_id: None,
            tickers: vec!["QQQ".to_string()],
            phase00_index: None,
        });
        let first = agent_loop::Turn::new(
            "turn-topic-a",
            "run:topic-a",
            "run",
            "researcher.bull.interaction",
            "",
        );
        let second = agent_loop::Turn::new(
            "turn-topic-b",
            "run:topic-b",
            "run",
            "researcher.bull.interaction",
            "",
        );

        let first_path = super::role_end_context_path(&settings, &first).unwrap();
        let second_path = super::role_end_context_path(&settings, &second).unwrap();

        assert_ne!(first_path, second_path);
        assert!(first_path.ends_with("researcher_bull_interaction_turn_topic_a_end_context.jsonl"));
    }

    #[test]
    fn append_debug_llm_record_writes_jsonl_under_outputs_debug() {
        let temp = tempfile::tempdir().unwrap();
        let mut settings = base_settings(LlmRoute::Responses);
        settings.debug = true;
        settings.phase = Some(1);
        settings.role = "analyst.technical".to_string();
        settings.tools = Some(tools::ExternalToolConfig {
            project_root: temp.path().to_path_buf(),
            db_path: None,
            run_dir: None,
            run_id: None,
            tickers: vec!["TQQQ".to_string()],
            phase00_index: None,
        });

        super::append_debug_llm_record(
            &settings,
            json!({
                "kind": "generate",
                "prompt": "hello",
                "response_text": "world",
            }),
        )
        .unwrap();
        super::append_debug_llm_record(
            &settings,
            json!({
                "kind": "stream",
                "prompt": "again",
                "response_text": "ok",
            }),
        )
        .unwrap();

        let path = temp
            .path()
            .join("outputs/debug/phase01/analyst_technical.jsonl");
        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<_> = contents.lines().filter(|line| !line.is_empty()).collect();
        // Latest turn only — intermediate tool turns are overwritten.
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("\"kind\":\"stream\""));
        assert!(lines[0].contains("again"));
    }

    #[test]
    fn append_debug_time_and_token_records_append_jsonl() {
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

        let time_path = root.join("outputs/debug/time.jsonl");
        let token_path = root.join("outputs/debug/token.jsonl");
        let time_contents = std::fs::read_to_string(&time_path).unwrap();
        let time_lines: Vec<_> = time_contents
            .lines()
            .filter(|line| !line.is_empty())
            .collect();
        let token_contents = std::fs::read_to_string(&token_path).unwrap();
        let token_lines: Vec<_> = token_contents
            .lines()
            .filter(|line| !line.is_empty())
            .collect();
        assert_eq!(time_lines.len(), 2);
        assert!(time_lines[0].contains("role_job"));
        assert!(time_lines[0].contains("\"llm_ms\":60"));
        assert!(time_lines[0].contains("\"tool_ms\":25"));
        assert!(time_lines[0].contains("\"wait_ms\":15"));
        assert!(time_lines[1].contains("phase1"));
        assert_eq!(token_lines.len(), 1);
        assert!(token_lines[0].contains("\"total_tokens\":14"));
        assert!(token_lines[0].contains("ts_ms"));
    }

    #[test]
    fn debug_record_path_includes_topic_id_for_phase2_roles() {
        let path = super::debug_record_relative_path_with_topic(
            2,
            "researcher.bull.initial",
            Some("QQQ-rate-volatility-confirmation"),
        );
        assert_eq!(
            path,
            PathBuf::from(
                "outputs/debug/phase02/researcher_bull_initial__QQQ_rate_volatility_confirmation.jsonl"
            )
        );
        assert_eq!(
            super::debug_record_relative_path(1, "analyst.news_macro"),
            PathBuf::from("outputs/debug/phase01/analyst_news_macro.jsonl")
        );
    }

    #[test]
    fn json_artifact_parser_requires_object() {
        assert_eq!(
            super::parse_json_object_artifact("{\"ok\":true}").unwrap(),
            json!({"ok": true})
        );
        let err = super::parse_json_object_artifact("[{\"ok\":true}]").unwrap_err();
        assert!(err.to_string().contains("must be an object"));
    }

    #[test]
    fn parse_final_output_enforces_analyst_direction_and_confidence() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "analyst.technical".to_string();
        settings.tickers = vec!["QQQ".to_string()];
        settings.output_mode = OutputMode::JsonArtifact;

        let missing_direction = r#"{"id":"analyst.technical","role":"analyst.technical","per_ticker":{"QQQ":{"confidence":0.7,"report":"x"}}}"#;
        let err = super::parse_final_output(&settings, missing_direction).unwrap_err();
        assert!(
            err.to_string().contains("direction") || err.to_string().contains("invalid analyst"),
            "unexpected error: {err}"
        );

        let valid = r#"{"id":"analyst.technical","role":"analyst.technical","per_ticker":{"QQQ":{"direction":"bullish","confidence":0.7,"report":"ok"}}}"#;
        let artifact = super::parse_final_output(&settings, valid).unwrap();
        assert_eq!(artifact["per_ticker"]["QQQ"]["direction"], json!("bullish"));
    }

    #[test]
    fn parse_final_output_rejects_analyst_role_mismatch() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "analyst.technical".to_string();
        settings.tickers = vec!["QQQ".to_string()];
        settings.output_mode = OutputMode::JsonArtifact;

        let text = r#"{"id":"analyst.youtube","role":"analyst.youtube","per_ticker":{"QQQ":{"direction":"bullish","confidence":0.7,"report":"ok"}}}"#;
        let error = super::parse_final_output(&settings, text).unwrap_err();

        assert!(
            error.to_string().contains("role"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn parse_final_output_rejects_analyst_ticker_substitution_without_repairing_it() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "analyst.technical".to_string();
        settings.tickers = vec!["QQQ".to_string(), "SOXX".to_string()];
        settings.output_mode = OutputMode::JsonArtifact;

        let text = r#"{
            "id":"analyst.technical",
            "role":"analyst.technical",
            "per_ticker":{
                "QQQ":{"direction":"bullish","confidence":0.7,"report":"ok"},
                "TQQQ":{"direction":"bullish","confidence":0.7,"report":"wrong ticker"}
            }
        }"#;
        let error = super::parse_final_output(&settings, text).unwrap_err();

        assert!(
            error.to_string().contains("per_ticker keys mismatch"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn parse_final_output_rejects_non_json_allocation_artifact() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "allocation.manager".to_string();
        settings.tickers = vec!["QQQ".to_string()];
        settings.output_mode = OutputMode::JsonArtifact;

        let text = "The allocation review is complete, but this response deliberately contains no JSON artifact. It repeats enough prose to be treated as a terminal answer by the agent loop rather than a short action note. The runtime must reject this execution-critical response instead of converting it into a degraded text artifact that downstream allocation normalization could misinterpret.";
        let error = super::parse_final_output(&settings, text).unwrap_err();

        assert!(
            error.to_string().contains("allocation.manager")
                || error.to_string().contains("JSON artifact"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn parse_final_output_rejects_wrapped_allocation_artifact() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "allocation.manager".to_string();
        settings.tickers = vec!["QQQ".to_string()];
        settings.output_mode = OutputMode::JsonArtifact;

        let text = r#"{
            "id":"allocation.manager",
            "role":"allocation.manager",
            "report":{
                "weights":{"QQQ":{"weight":0.0},"cash_hedge":{"weight":1.0}},
                "total_equity_exposure":0.0,
                "vix_regime":"normal",
                "correlation_note":"none",
                "summary":"cash"
            }
        }"#;
        let error = super::parse_final_output(&settings, text).unwrap_err();

        assert!(
            error.to_string().contains("weights"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn parse_final_output_accepts_direct_allocation_without_runtime_identity() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "allocation.manager".to_string();
        settings.tickers = vec!["QQQ".to_string()];
        settings.output_mode = OutputMode::JsonArtifact;

        let text = r#"{
            "weights":{"QQQ":{"weight":0.0},"cash_hedge":{"weight":1.0}},
            "total_equity_exposure":0.0,
            "vix_regime":"normal",
            "correlation_note":"none",
            "summary":"cash"
        }"#;
        let artifact = super::parse_final_output(&settings, text).unwrap();

        assert_eq!(artifact["total_equity_exposure"], 0.0);
        assert!(artifact.get("role").is_none());
    }

    #[test]
    fn parse_final_output_rejects_allocation_weight_sum_mismatch() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "allocation.manager".to_string();
        settings.tickers = vec!["QQQ".to_string()];
        settings.output_mode = OutputMode::JsonArtifact;

        let text = r#"{
            "weights":{"QQQ":{"weight":0.10}},
            "total_equity_exposure":0.10,
            "vix_regime":"normal",
            "correlation_note":"none",
            "summary":"incomplete"
        }"#;
        let error = super::parse_final_output(&settings, text).unwrap_err();

        assert!(
            error.to_string().contains("sum"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn parse_final_output_rejects_hold_trade_intent_with_positive_position() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "trader".to_string();
        settings.output_mode = OutputMode::JsonArtifact;

        let text = r#"{
            "action":"Hold",
            "entry_price":null,
            "stop_loss":null,
            "position_size":"0%-30%",
            "rationale":"observe"
        }"#;
        let error = super::parse_final_output(&settings, text).unwrap_err();

        assert!(
            error.to_string().contains("Hold"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn parse_final_output_rejects_incomplete_risk_constraints() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "risk.conservative".to_string();
        settings.output_mode = OutputMode::JsonArtifact;

        let text = r#"{
            "stance":"conservative",
            "argument":"Protect capital.",
            "recommended_adjustment":"Reduce size."
        }"#;
        let error = super::parse_final_output(&settings, text).unwrap_err();

        assert!(
            error.to_string().contains("position_cap_pct")
                || error.to_string().contains("max_drawdown_pct")
                || error.to_string().contains("stop_type"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn parse_final_output_rejects_interaction_packet_from_another_role() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "researcher.bull.interaction".to_string();
        settings.output_mode = OutputMode::JsonArtifact;

        let text = r#"{
            "role":"mediator.topic_controller",
            "artifact_type":"topic_controller_packet",
            "topic_id":"qqq-trend",
            "claim_ledger":[],
            "accepted_for_opponent":[],
            "rejected_to_origin":[],
            "blocked_claims":[],
            "next_steers":{},
            "topic_summary_delta":{},
            "soft_control":{"should_continue":false,"stop_reason":"done"},
            "reducer_checks":{}
        }"#;
        let error = super::parse_final_output(&settings, text).unwrap_err();

        assert!(
            error.to_string().contains("role mismatch"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn parse_final_output_rejects_non_json_initial_debate_packet() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "researcher.bull.initial".to_string();
        settings.output_mode = OutputMode::JsonArtifact;

        let text = "The bullish seed analysis is complete, but this terminal response contains no JSON packet and therefore cannot be consumed by the debate reducer. The runtime must reject it instead of manufacturing a generic degraded artifact.";
        let error = super::parse_final_output(&settings, text).unwrap_err();

        assert!(
            error.to_string().contains("researcher.bull.initial")
                || error.to_string().contains("JSON artifact"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn parse_final_output_rejects_initial_packet_from_another_role() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "researcher.bear.initial".to_string();
        settings.output_mode = OutputMode::JsonArtifact;

        let text = r#"{
            "role":"researcher.bull.initial",
            "artifact_type":"bull_seed_packet",
            "topic_id":"qqq-trend",
            "claims":[],
            "summary":"wrong side",
            "reducer_checks":{}
        }"#;
        let error = super::parse_final_output(&settings, text).unwrap_err();

        assert!(
            error.to_string().contains("role mismatch"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn parse_final_output_rejects_incomplete_initial_claim() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "researcher.bull.initial".to_string();
        settings.output_mode = OutputMode::JsonArtifact;

        let text = r#"{
            "role":"researcher.bull.initial",
            "artifact_type":"bull_seed_packet",
            "topic_id":"qqq-trend",
            "claims":[{"claim":"upside"}],
            "summary":"one claim",
            "reducer_checks":{}
        }"#;
        let error = super::parse_final_output(&settings, text).unwrap_err();

        assert!(
            error.to_string().contains("claim_id")
                || error.to_string().contains("initial seed claim"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn parse_final_output_accepts_role_specific_initial_packets() {
        for (role, artifact_type, constraint_field) in [
            (
                "researcher.bull.initial",
                "bull_seed_packet",
                "known_bear_constraint",
            ),
            (
                "researcher.bear.initial",
                "bear_seed_packet",
                "known_bull_constraint",
            ),
        ] {
            let mut settings = base_settings(LlmRoute::Responses);
            settings.role = role.to_string();
            settings.output_mode = OutputMode::JsonArtifact;
            let packet = json!({
                "role": role,
                "artifact_type": artifact_type,
                "topic_id": "qqq-trend",
                "claims": [{
                    "claim_id": "claim-1",
                    "decision_hinge": "trend confirmation",
                    "claim": "directional claim",
                    "evidence_refs": ["phase1.5:qqq"],
                    "confidence": 0.65,
                    (constraint_field): "opposing constraint",
                    "needs_mediator_check": true
                }],
                "summary": "one evidence-backed seed claim",
                "reducer_checks": {}
            });

            let artifact = super::parse_final_output(&settings, &packet.to_string())
                .expect("valid seed packet");
            assert_eq!(artifact["role"], json!(role));
            assert_eq!(artifact["artifact_type"], json!(artifact_type));
        }
    }

    #[test]
    fn parse_final_output_accepts_per_ticker_only_research_envelope() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "manager.research".to_string();
        settings.tickers = vec!["QQQ".to_string(), "SOXX".to_string()];
        settings.output_mode = OutputMode::ResearchArtifact;

        let text = r#"{
            "per_ticker":{
                "QQQ":{
                    "rating":"Overweight",
                    "long_probability":0.57,
                    "short_probability":0.43,
                    "confidence_basis":"directional_evidence",
                    "plan":["Verify volume","Watch break"],
                    "probability_rationale":"Near base after discount."
                },
                "SOXX":{
                    "rating":"Hold",
                    "long_probability":0.51,
                    "short_probability":0.49,
                    "confidence_basis":"data_insufficient",
                    "hold_reason":"evidence_insufficient",
                    "plan":"Wait for confirmation",
                    "probability_rationale":"Insufficient SOXX-specific evidence."
                }
            }
        }"#;

        let artifact = super::parse_final_output(&settings, text).unwrap();
        assert_eq!(artifact["rating"], json!("Overweight"));
        assert_eq!(artifact["long_probability"], json!(0.57));
        assert_eq!(artifact["short_probability"], json!(0.43));
        assert!(artifact["plan"].as_str().unwrap().contains("Verify volume"));
        assert_eq!(artifact["per_ticker"]["SOXX"]["rating"], json!("Hold"));
    }

    #[test]
    fn parse_final_output_rejects_research_artifact_from_another_role() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "manager.research".to_string();
        settings.tickers = vec!["QQQ".to_string()];
        settings.output_mode = OutputMode::ResearchArtifact;

        let text = r#"{
            "role":"analyst.technical",
            "rating":"Hold",
            "long_probability":0.5,
            "short_probability":0.5,
            "confidence_basis":"evidence_balanced",
            "hold_reason":"evidence_balanced",
            "plan":"Wait.",
            "probability_rationale":"Balanced evidence.",
            "per_ticker":{"QQQ":{
                "rating":"Hold",
                "long_probability":0.5,
                "short_probability":0.5,
                "confidence_basis":"evidence_balanced",
                "hold_reason":"evidence_balanced"
            }}
        }"#;

        let error = super::parse_final_output(&settings, text).unwrap_err();
        assert!(error.to_string().contains("role mismatch"));
    }

    #[test]
    fn json_artifact_parser_reads_artifact_from_event_delta() {
        let text = "{\"type\":\"assistant_message_started\",\"item_id\":\"msg-1\"}\n{\"type\":\"assistant_text_delta\",\"item_id\":\"msg-1\",\"delta\":\"{\\\"id\\\":\\\"a\\\",\\\"status\\\":\\\"completed\\\"}\"}\n{\"type\":\"assistant_message_completed\",\"item_id\":\"msg-1\"}";

        assert_eq!(
            super::parse_json_object_artifact(text).unwrap(),
            json!({"id": "a", "status": "completed"})
        );
    }

    #[derive(Default)]
    struct CollectEvents {
        events: Vec<ModelStreamEvent>,
    }

    impl ModelEventHandler for CollectEvents {
        fn handle<'a>(
            &'a mut self,
            event: ModelStreamEvent,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
            self.events.push(event);
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn assistant_text_accumulator_emits_started_delta_completed() {
        let mut text = AssistantTextAccumulator::new();
        let mut handler = CollectEvents::default();

        text.push_delta(&mut handler, "msg-1".to_string(), "hello".to_string())
            .await
            .unwrap();
        text.push_delta(
            &mut handler,
            "msg-ignored".to_string(),
            " world".to_string(),
        )
        .await
        .unwrap();
        text.complete(&mut handler).await.unwrap();
        // complete is idempotent
        text.complete(&mut handler).await.unwrap();

        assert_eq!(handler.events.len(), 4);
        assert!(matches!(
            &handler.events[0],
            ModelStreamEvent::AssistantMessageStarted { item_id } if item_id == "msg-1"
        ));
        assert!(matches!(
            &handler.events[1],
            ModelStreamEvent::AssistantTextDelta { item_id, delta }
                if item_id == "msg-1" && delta == "hello"
        ));
        assert!(matches!(
            &handler.events[2],
            ModelStreamEvent::AssistantTextDelta { item_id, delta }
                if item_id == "msg-1" && delta == " world"
        ));
        assert!(matches!(
            &handler.events[3],
            ModelStreamEvent::AssistantMessageCompleted { item_id, .. } if item_id == "msg-1"
        ));
    }

    #[test]
    fn role_llm_settings_rejects_unknown_tools() {
        let settings = RoleLlmSettings {
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
            tools: vec!["missing_tool".to_string()],
            native_web_search: false,
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
            Some(json!({"reasoning": {"effort": "low"}}))
        );

        settings.web_search.mode = WebSearchMode::Live;
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
        settings.llm.tools = vec!["run_technical_indicators".to_string()];
        assert_eq!(
            super::configured_tool_names(&settings),
            vec!["think", "run_technical_indicators"]
        );

        settings.llm.think_tool = false;
        assert_eq!(
            super::configured_tool_names(&settings),
            vec!["run_technical_indicators"]
        );
    }

    #[test]
    fn execution_roles_disable_loop_and_native_tools() {
        for role in ["manager.research", "allocation.manager"] {
            let mut settings = base_settings(LlmRoute::Responses);
            settings.role = role.to_string();
            settings.llm.base_url = Some("https://llm.example.com/v1".to_string());
            settings.llm.api_key = Some("test-key".to_string());
            settings.llm.tools = vec!["read_run_context".to_string()];
            settings.llm.native_web_search = true;
            settings.web_search.mode = WebSearchMode::Live;

            assert!(super::configured_tool_names(&settings).is_empty());
            assert_eq!(
                super::additional_params(&settings),
                Some(json!({"reasoning": {"effort": "low"}}))
            );
            assert!(super::web_run_runtime_for_settings(&settings).is_none());
        }
    }

    #[test]
    fn web_run_tool_registration_follows_web_search_mode() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.role = "analyst.news_macro".to_string();
        settings.llm.base_url = Some("https://llm.example.com/v1".to_string());
        settings.llm.api_key = Some("test-key".to_string());
        settings.llm.think_tool = false;

        assert!(!super::configured_tool_names(&settings).contains(&tools::WEB_RUN_TOOL_NAME));

        settings.web_search.mode = WebSearchMode::Cached;
        assert!(!super::configured_tool_names(&settings).contains(&tools::WEB_RUN_TOOL_NAME));

        settings.web_search.mode = WebSearchMode::Live;
        assert!(super::configured_tool_names(&settings).contains(&tools::WEB_RUN_TOOL_NAME));
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
}
