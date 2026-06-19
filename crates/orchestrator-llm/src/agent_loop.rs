use anyhow::{bail, Context, Result};
use orchestrator_sql::{
    append_agent_turn_item, session_history_items, update_agent_turn_end,
    update_agent_turn_item_content, upsert_agent_turn, write_turn_tool_call, AgentTurnInput,
    AgentTurnItemInput, TurnToolCallInput,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::tools;
use crate::RigSettings;

const DEFAULT_MAX_AGENT_LOOPS: usize = 8;
const MAX_CONTEXT_FRAGMENT_CHARS: usize = 12_000;
const MAX_TOOL_RESULT_CHARS: usize = 8_000;
const FINALIZE_STEER: &str = "The previous assistant message was an action note, not final output. Do not ask for more input. Use the current prompt, context, and tool results. Either call the required next tool now, or return the complete final JSON artifact for this role. If data is missing, encode the gap inside the JSON artifact.";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnItemType {
    UserMessage,
    AssistantMessage,
    ReasoningSummary,
    PlanUpdate,
    ToolCall,
    ToolResult,
    SystemContext,
    DeveloperContext,
    CompactSummary,
    InjectedContext,
}

impl TurnItemType {
    fn as_str(&self) -> &'static str {
        match self {
            Self::UserMessage => "user_message",
            Self::AssistantMessage => "assistant_message",
            Self::ReasoningSummary => "reasoning_summary",
            Self::PlanUpdate => "plan_update",
            Self::ToolCall => "tool_call",
            Self::ToolResult => "tool_result",
            Self::SystemContext => "system_context",
            Self::DeveloperContext => "developer_context",
            Self::CompactSummary => "compact_summary",
            Self::InjectedContext => "injected_context",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolMode {
    Blocking,
    Background,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallRequest {
    pub call_id: String,
    pub name: String,
    #[serde(default)]
    pub arguments: Value,
    #[serde(default = "default_tool_mode")]
    pub mode: ToolMode,
}

fn default_tool_mode() -> ToolMode {
    ToolMode::Blocking
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolResultItem {
    pub call_id: String,
    pub name: String,
    pub status: String,
    #[serde(default)]
    pub output: Value,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub background_job_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentItemPhase {
    Commentary,
    Final,
}

impl AgentItemPhase {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Commentary => "commentary",
            Self::Final => "final",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentItemStatus {
    InProgress,
    Completed,
    Pending,
    Running,
    Failed,
    Interrupted,
}

impl AgentItemStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentOutputItem {
    AssistantMessage {
        id: String,
        phase: AgentItemPhase,
        content: String,
        status: AgentItemStatus,
    },
    ReasoningSummary {
        id: String,
        content: String,
        status: AgentItemStatus,
    },
    PlanUpdate {
        id: String,
        content: String,
        status: AgentItemStatus,
    },
    ToolCall {
        id: String,
        tool_name: String,
        arguments: Value,
        status: AgentItemStatus,
    },
    ToolResult {
        id: String,
        tool_call_id: String,
        content: String,
        status: AgentItemStatus,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentLoopEvent {
    TurnItemStarted {
        turn_id: String,
        item: AgentOutputItem,
    },
    TurnItemDelta {
        turn_id: String,
        item_id: String,
        delta: String,
    },
    TurnItemCompleted {
        turn_id: String,
        item: AgentOutputItem,
    },
}

pub trait AgentEventSink {
    fn emit<'a>(
        &'a mut self,
        event: AgentLoopEvent,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

pub trait ModelEventHandler {
    fn handle<'a>(
        &'a mut self,
        event: ModelStreamEvent,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>>;
}

#[derive(Debug, Default)]
pub struct NoopAgentEventSink;

impl AgentEventSink for NoopAgentEventSink {
    fn emit<'a>(
        &'a mut self,
        _event: AgentLoopEvent,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnItem {
    pub item_type: TurnItemType,
    #[serde(default)]
    pub role: String,
    #[serde(default)]
    pub content_text: String,
    #[serde(default)]
    pub content_json: Value,
    #[serde(default)]
    pub tool_call_id: String,
    #[serde(default)]
    pub tool_name: String,
    #[serde(default)]
    pub output_item_id: String,
    #[serde(default)]
    pub phase: Option<AgentItemPhase>,
    #[serde(default)]
    pub status: Option<AgentItemStatus>,
    #[serde(skip)]
    pub db_row_id: Option<i64>,
}

impl TurnItem {
    pub fn user(text: impl Into<String>) -> Self {
        Self {
            item_type: TurnItemType::UserMessage,
            role: "user".to_string(),
            content_text: text.into(),
            content_json: Value::Null,
            tool_call_id: String::new(),
            tool_name: String::new(),
            output_item_id: String::new(),
            phase: None,
            status: None,
            db_row_id: None,
        }
    }

    pub fn assistant(text: impl Into<String>, json_value: Value) -> Self {
        let text = text.into();
        Self {
            item_type: TurnItemType::AssistantMessage,
            role: "assistant".to_string(),
            content_text: text.clone(),
            content_json: merge_item_metadata(
                json_value,
                "",
                Some(AgentItemPhase::Commentary),
                AgentItemStatus::Completed,
            ),
            tool_call_id: String::new(),
            tool_name: String::new(),
            output_item_id: String::new(),
            phase: Some(AgentItemPhase::Commentary),
            status: Some(AgentItemStatus::Completed),
            db_row_id: None,
        }
    }

    pub fn tool_call(call: &ToolCallRequest) -> Self {
        let content_json = json!({
            "call": call,
            "output_item_id": call.call_id,
            "status": AgentItemStatus::Pending.as_str()
        });
        Self {
            item_type: TurnItemType::ToolCall,
            role: "assistant".to_string(),
            content_text: String::new(),
            content_json,
            tool_call_id: call.call_id.clone(),
            tool_name: call.name.clone(),
            output_item_id: call.call_id.clone(),
            phase: None,
            status: Some(AgentItemStatus::Pending),
            db_row_id: None,
        }
    }

    pub fn tool_result(result: &ToolResultItem) -> Self {
        let status = if result.status == "completed" || result.status == "started" {
            AgentItemStatus::Completed
        } else {
            AgentItemStatus::Failed
        };
        let content_text = result
            .output
            .get("content")
            .or_else(|| result.output.get("text"))
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .unwrap_or_else(|| result.output.to_string());
        Self {
            item_type: TurnItemType::ToolResult,
            role: "tool".to_string(),
            content_text: truncate_chars(&content_text, MAX_TOOL_RESULT_CHARS),
            content_json: json!({
                "result": result,
                "output_item_id": format!("result-{}", result.call_id),
                "status": status.as_str()
            }),
            tool_call_id: result.call_id.clone(),
            tool_name: result.name.clone(),
            output_item_id: format!("result-{}", result.call_id),
            phase: None,
            status: Some(status),
            db_row_id: None,
        }
    }
}

fn merge_item_metadata(
    value: Value,
    output_item_id: &str,
    phase: Option<AgentItemPhase>,
    status: AgentItemStatus,
) -> Value {
    let mut object = match value {
        Value::Object(map) => map,
        other if other.is_null() => serde_json::Map::new(),
        other => {
            let mut map = serde_json::Map::new();
            map.insert("raw".to_string(), other);
            map
        }
    };
    if !output_item_id.is_empty() {
        object.insert(
            "output_item_id".to_string(),
            Value::String(output_item_id.to_string()),
        );
    }
    if let Some(phase) = phase {
        object.insert(
            "phase".to_string(),
            Value::String(phase.as_str().to_string()),
        );
    }
    object.insert(
        "status".to_string(),
        Value::String(status.as_str().to_string()),
    );
    Value::Object(object)
}

#[derive(Debug, Clone)]
pub struct Turn {
    pub turn_id: String,
    pub session_id: String,
    pub run_id: String,
    pub phase: Option<i64>,
    pub role: String,
    pub user_input: String,
    pub model_context: String,
    pub pending_input: VecDeque<String>,
    pub emitted_items: Vec<TurnItem>,
    pub pending_tool_calls: Vec<ToolCallRequest>,
    pub cancellation_state: String,
    pub needs_follow_up: bool,
    pub end_reason: Option<String>,
}

impl Turn {
    pub fn new(
        turn_id: impl Into<String>,
        session_id: impl Into<String>,
        run_id: impl Into<String>,
        role: impl Into<String>,
        user_input: impl Into<String>,
    ) -> Self {
        Self {
            turn_id: turn_id.into(),
            session_id: session_id.into(),
            run_id: run_id.into(),
            phase: None,
            role: role.into(),
            user_input: user_input.into(),
            model_context: String::new(),
            pending_input: VecDeque::new(),
            emitted_items: Vec::new(),
            pending_tool_calls: Vec::new(),
            cancellation_state: "none".to_string(),
            needs_follow_up: false,
            end_reason: None,
        }
    }

    pub fn push_pending_input(&mut self, input: impl Into<String>) {
        self.pending_input.push_back(input.into());
    }
}

#[derive(Debug, Clone)]
pub struct ModelInput {
    pub items: Vec<TurnItem>,
    pub available_tools: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ModelResponse {
    pub assistant_message: Option<String>,
    pub reasoning_summary: Option<String>,
    pub tool_calls: Vec<ToolCallRequest>,
    pub end_turn: bool,
    pub raw: Value,
}

#[derive(Debug, Clone)]
pub enum ModelStreamEvent {
    AssistantMessageStarted { item_id: String },
    AssistantTextDelta { item_id: String, delta: String },
    AssistantMessageCompleted { item_id: String },
    ReasoningSummaryDelta { item_id: String, delta: String },
    ReasoningSummaryCompleted { item_id: String },
    PlanUpdateCompleted { item_id: String, content: String },
    ToolCallCompleted { tool_call: ToolCallRequest },
    ResponseCompleted { end_turn: bool, raw: Value },
    StreamInterrupted { error: String },
}

#[derive(Debug, Clone, Default)]
pub struct ModelStreamResult {
    pub needs_follow_up: bool,
    pub last_assistant_message_id: Option<String>,
    pub tool_calls: Vec<ToolCallRequest>,
}

pub trait LoopModel: Send {
    fn generate<'a>(
        &'a mut self,
        input: ModelInput,
    ) -> Pin<Box<dyn Future<Output = Result<ModelResponse>> + Send + 'a>>;

    fn stream_events<'a>(
        &'a mut self,
        input: ModelInput,
        handler: &'a mut dyn ModelEventHandler,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
        Box::pin(async move {
            let response = self.generate(input).await?;
            for event in response_to_stream_events(response)? {
                handler.handle(event).await?;
            }
            Ok(())
        })
    }
}

pub(crate) fn response_to_stream_events(response: ModelResponse) -> Result<Vec<ModelStreamEvent>> {
    let mut events = Vec::new();
    if let Some(summary) = response
        .reasoning_summary
        .clone()
        .filter(|value| !value.trim().is_empty())
    {
        let item_id = format!("reasoning-{}", Uuid::new_v4());
        events.push(ModelStreamEvent::ReasoningSummaryDelta {
            item_id: item_id.clone(),
            delta: summary,
        });
        events.push(ModelStreamEvent::ReasoningSummaryCompleted { item_id });
    }
    if let Some(message) = response
        .assistant_message
        .clone()
        .filter(|value| !value.trim().is_empty())
    {
        let item_id = format!("msg-{}", Uuid::new_v4());
        events.push(ModelStreamEvent::AssistantMessageStarted {
            item_id: item_id.clone(),
        });
        events.push(ModelStreamEvent::AssistantTextDelta {
            item_id: item_id.clone(),
            delta: message,
        });
        events.push(ModelStreamEvent::AssistantMessageCompleted { item_id });
    }
    for tool_call in response.tool_calls {
        events.push(ModelStreamEvent::ToolCallCompleted { tool_call });
    }
    events.push(ModelStreamEvent::ResponseCompleted {
        end_turn: response.end_turn,
        raw: response.raw,
    });
    Ok(events)
}

pub trait LoopToolRuntime {
    fn set_turn_context(&mut self, _context: ToolRuntimeTurnContext) {}

    fn execute<'a>(
        &'a mut self,
        call: ToolCallRequest,
    ) -> Pin<Box<dyn Future<Output = ToolResultItem> + Send + 'a>>;
}

#[derive(Debug, Clone)]
pub struct ToolRuntimeTurnContext {
    pub run_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub role: String,
}

pub struct JsonPromptModel<F>
where
    F: for<'a> FnMut(&'a str) -> Pin<Box<dyn Future<Output = Result<String>> + Send + 'a>> + Send,
{
    prompt_fn: F,
}

pub struct RigLoopModel {
    settings: RigSettings,
}

impl RigLoopModel {
    pub fn new(settings: RigSettings) -> Self {
        Self { settings }
    }
}

impl LoopModel for RigLoopModel {
    fn generate<'a>(
        &'a mut self,
        input: ModelInput,
    ) -> Pin<Box<dyn Future<Output = Result<ModelResponse>> + Send + 'a>> {
        Box::pin(async move {
            let prompt = react_prompt(&input)?;
            let text = crate::run_model_text_once(&self.settings, &prompt).await?;
            let value = extract_json_value(&text)?;
            parse_react_response(value)
        })
    }

    fn stream_events<'a>(
        &'a mut self,
        input: ModelInput,
        handler: &'a mut dyn ModelEventHandler,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
        Box::pin(async move {
            let prompt = react_prompt(&input)?;
            crate::run_model_event_stream(&self.settings, &prompt, handler).await
        })
    }
}

impl<F> JsonPromptModel<F>
where
    F: for<'a> FnMut(&'a str) -> Pin<Box<dyn Future<Output = Result<String>> + Send + 'a>> + Send,
{
    pub fn new(prompt_fn: F) -> Self {
        Self { prompt_fn }
    }
}

impl<F> LoopModel for JsonPromptModel<F>
where
    F: for<'a> FnMut(&'a str) -> Pin<Box<dyn Future<Output = Result<String>> + Send + 'a>> + Send,
{
    fn generate<'a>(
        &'a mut self,
        input: ModelInput,
    ) -> Pin<Box<dyn Future<Output = Result<ModelResponse>> + Send + 'a>> {
        Box::pin(async move {
            let prompt = react_prompt(&input)?;
            let text = (self.prompt_fn)(&prompt).await?;
            let value = extract_json_value(&text)?;
            parse_react_response(value)
        })
    }
}

#[derive(Debug, Clone)]
pub struct AgentLoopConfig {
    pub max_agent_loops: Option<usize>,
    pub history_limit: usize,
    pub compact_after_items: usize,
}

impl Default for AgentLoopConfig {
    fn default() -> Self {
        Self {
            max_agent_loops: Some(DEFAULT_MAX_AGENT_LOOPS),
            history_limit: 200,
            compact_after_items: 120,
        }
    }
}

pub async fn run_turn<M, T>(
    conn: &rusqlite::Connection,
    turn: &mut Turn,
    model: &mut M,
    tools: &mut T,
    config: AgentLoopConfig,
) -> Result<()>
where
    M: LoopModel,
    T: LoopToolRuntime,
{
    let mut sink = NoopAgentEventSink;
    run_turn_with_events(conn, turn, model, tools, config, &mut sink).await
}

pub async fn run_turn_with_events<M, T, S>(
    conn: &rusqlite::Connection,
    turn: &mut Turn,
    model: &mut M,
    tools: &mut T,
    config: AgentLoopConfig,
    sink: &mut S,
) -> Result<()>
where
    M: LoopModel,
    T: LoopToolRuntime,
    S: AgentEventSink,
{
    debug!(
        turn_id = turn.turn_id,
        session_id = turn.session_id,
        run_id = turn.run_id,
        role = turn.role,
        phase = turn.phase,
        max_agent_loops = config.max_agent_loops,
        history_limit = config.history_limit,
        compact_after_items = config.compact_after_items,
        "agent loop starting"
    );
    persist_turn(conn, turn)?;
    tools.set_turn_context(ToolRuntimeTurnContext {
        run_id: turn.run_id.clone(),
        session_id: turn.session_id.clone(),
        turn_id: turn.turn_id.clone(),
        role: turn.role.clone(),
    });
    append_turn_item(conn, turn, &TurnItem::user(turn.user_input.clone()))?;
    let mut first_iteration = true;
    let max_loops = config.max_agent_loops.map(|value| value.max(1));
    let mut loop_index = 0usize;
    loop {
        if let Some(max_loops) = max_loops {
            if loop_index >= max_loops {
                turn.end_reason = Some("max_loops".to_string());
                update_agent_turn_end(conn, &turn.turn_id, false, "max_loops")?;
                bail!("agent loop reached max_agent_loops={}", max_loops);
            }
        }
        loop_index += 1;
        let input = build_model_input(conn, turn, first_iteration, &config)?;
        debug!(
            turn_id = turn.turn_id,
            role = turn.role,
            loop_index,
            input_items = input.items.len(),
            available_tools = ?input.available_tools,
            pending_input = turn.pending_input.len(),
            pending_tool_calls = turn.pending_tool_calls.len(),
            "agent loop model iteration starting"
        );
        first_iteration = false;
        let mut stream_handler = ModelStreamHandler::new(conn, turn, sink);
        model.stream_events(input, &mut stream_handler).await?;
        let stream_result = stream_handler.finish().await?;
        debug!(
            turn_id = turn.turn_id,
            role = turn.role,
            loop_index,
            tool_calls = stream_result.tool_calls.len(),
            needs_follow_up = stream_result.needs_follow_up,
            last_assistant_message_id = stream_result.last_assistant_message_id,
            "agent loop model iteration completed"
        );

        if !turn.pending_tool_calls.is_empty() {
            let calls = std::mem::take(&mut turn.pending_tool_calls);
            for call in calls {
                let call_id = call.call_id.clone();
                let call_name = call.name.clone();
                let call_args = call.arguments.clone();
                debug!(
                    turn_id = turn.turn_id,
                    call_id = call.call_id,
                    tool = call.name,
                    mode = ?call.mode,
                    "agent loop tool call starting"
                );
                emit_tool_call_status(turn, sink, &call, AgentItemStatus::Running).await?;
                let result = tools.execute(call).await;
                write_turn_tool_call(
                    conn,
                    &TurnToolCallInput {
                        run_id: turn.run_id.clone(),
                        turn_id: turn.turn_id.clone(),
                        role: turn.role.clone(),
                        phase: turn.phase,
                        ticker: turn
                            .model_context
                            .lines()
                            .find_map(|line| line.strip_prefix("tickers="))
                            .and_then(|value| value.split(',').next())
                            .unwrap_or_default()
                            .to_string(),
                        item_time: String::new(),
                        topic_id: None,
                        debate_id: None,
                        tool_call_id: call_id,
                        tool_type: call_name.clone(),
                        tool_name: call_name,
                        args_json: call_args,
                        status: result.status.clone(),
                        error: result.error.clone().unwrap_or_default(),
                    },
                )?;
                debug!(
                    turn_id = turn.turn_id,
                    call_id = result.call_id,
                    tool = result.name,
                    status = result.status,
                    background_job_id = result.background_job_id,
                    error = result.error,
                    "agent loop tool call completed"
                );
                emit_tool_result(turn, sink, &result).await?;
                append_turn_item(conn, turn, &TurnItem::tool_result(&result))?;
            }
            turn.needs_follow_up = true;
            persist_turn(conn, turn)?;
            continue;
        }

        if !turn.pending_input.is_empty() {
            turn.needs_follow_up = true;
            persist_turn(conn, turn)?;
            continue;
        }

        if turn.needs_follow_up {
            turn.needs_follow_up = false;
            persist_turn(conn, turn)?;
            continue;
        }

        if let Some(item_id) = stream_result.last_assistant_message_id {
            let final_text = turn
                .emitted_items
                .iter()
                .rev()
                .find(|item| item.output_item_id == item_id)
                .map(|item| item.content_text.clone())
                .unwrap_or_default();
            if assistant_message_needs_follow_up(&final_text) {
                turn.push_pending_input(FINALIZE_STEER);
                turn.needs_follow_up = true;
                persist_turn(conn, turn)?;
                continue;
            }
            mark_last_assistant_message_as_final(conn, turn, &item_id, sink).await?;
        }
        turn.end_reason = Some("completed".to_string());
        update_agent_turn_end(conn, &turn.turn_id, false, "completed")?;
        debug!(
            turn_id = turn.turn_id,
            role = turn.role,
            loop_index,
            "agent loop completed"
        );
        return Ok(());
    }
}

fn persist_turn(conn: &rusqlite::Connection, turn: &Turn) -> Result<()> {
    upsert_agent_turn(
        conn,
        &AgentTurnInput {
            turn_id: turn.turn_id.clone(),
            session_id: turn.session_id.clone(),
            run_id: turn.run_id.clone(),
            phase: turn.phase,
            role: turn.role.clone(),
            user_input: turn.user_input.clone(),
            model_context: truncate_chars(&turn.model_context, MAX_CONTEXT_FRAGMENT_CHARS),
            cancellation_state: turn.cancellation_state.clone(),
            needs_follow_up: turn.needs_follow_up,
            end_reason: turn.end_reason.clone().unwrap_or_default(),
        },
    )
}

fn append_turn_item(conn: &rusqlite::Connection, turn: &mut Turn, item: &TurnItem) -> Result<i64> {
    let row_id = append_agent_turn_item(
        conn,
        &AgentTurnItemInput {
            turn_id: turn.turn_id.clone(),
            session_id: turn.session_id.clone(),
            run_id: turn.run_id.clone(),
            item_type: item.item_type.as_str().to_string(),
            role: item.role.clone(),
            tool_call_id: item.tool_call_id.clone(),
            tool_name: item.tool_name.clone(),
            content_json: item.content_json.clone(),
            content_text: truncate_chars(&item.content_text, MAX_TOOL_RESULT_CHARS),
        },
    )?;
    let mut stored = item.clone();
    stored.db_row_id = Some(row_id);
    turn.emitted_items.push(stored);
    Ok(row_id)
}

fn update_turn_item(
    conn: &rusqlite::Connection,
    turn: &mut Turn,
    output_item_id: &str,
    content_text: String,
    phase: Option<AgentItemPhase>,
    status: AgentItemStatus,
) -> Result<Option<TurnItem>> {
    let Some(index) = turn
        .emitted_items
        .iter()
        .rposition(|item| item.output_item_id == output_item_id)
    else {
        return Ok(None);
    };
    let mut item = turn.emitted_items[index].clone();
    item.content_text = content_text;
    item.phase = phase;
    item.status = Some(status);
    item.content_json = merge_item_metadata(
        item.content_json.clone(),
        &item.output_item_id,
        item.phase.clone(),
        item.status.clone().unwrap_or(AgentItemStatus::Completed),
    );
    if let Some(row_id) = item.db_row_id {
        update_agent_turn_item_content(
            conn,
            row_id,
            &item.content_json,
            &truncate_chars(&item.content_text, MAX_TOOL_RESULT_CHARS),
        )?;
    }
    turn.emitted_items[index] = item.clone();
    Ok(Some(item))
}

fn output_item_for(item: &TurnItem) -> Option<AgentOutputItem> {
    let id = if item.output_item_id.is_empty() {
        item.tool_call_id.clone()
    } else {
        item.output_item_id.clone()
    };
    match item.item_type {
        TurnItemType::AssistantMessage => Some(AgentOutputItem::AssistantMessage {
            id,
            phase: item.phase.clone().unwrap_or(AgentItemPhase::Commentary),
            content: item.content_text.clone(),
            status: item.status.clone().unwrap_or(AgentItemStatus::Completed),
        }),
        TurnItemType::ReasoningSummary => Some(AgentOutputItem::ReasoningSummary {
            id,
            content: item.content_text.clone(),
            status: item.status.clone().unwrap_or(AgentItemStatus::Completed),
        }),
        TurnItemType::PlanUpdate => Some(AgentOutputItem::PlanUpdate {
            id,
            content: item.content_text.clone(),
            status: item.status.clone().unwrap_or(AgentItemStatus::Completed),
        }),
        TurnItemType::ToolCall => Some(AgentOutputItem::ToolCall {
            id,
            tool_name: item.tool_name.clone(),
            arguments: item
                .content_json
                .get("call")
                .and_then(|value| value.get("arguments"))
                .cloned()
                .unwrap_or(Value::Null),
            status: item.status.clone().unwrap_or(AgentItemStatus::Pending),
        }),
        TurnItemType::ToolResult => Some(AgentOutputItem::ToolResult {
            id,
            tool_call_id: item.tool_call_id.clone(),
            content: item.content_text.clone(),
            status: item.status.clone().unwrap_or(AgentItemStatus::Completed),
        }),
        _ => None,
    }
}

async fn emit_started<S: AgentEventSink>(turn: &Turn, sink: &mut S, item: &TurnItem) -> Result<()> {
    if let Some(output_item) = output_item_for(item) {
        sink.emit(AgentLoopEvent::TurnItemStarted {
            turn_id: turn.turn_id.clone(),
            item: output_item,
        })
        .await?;
    }
    Ok(())
}

async fn emit_completed<S: AgentEventSink>(
    turn: &Turn,
    sink: &mut S,
    item: &TurnItem,
) -> Result<()> {
    if let Some(output_item) = output_item_for(item) {
        sink.emit(AgentLoopEvent::TurnItemCompleted {
            turn_id: turn.turn_id.clone(),
            item: output_item,
        })
        .await?;
    }
    Ok(())
}

async fn emit_delta<S: AgentEventSink>(
    turn: &Turn,
    sink: &mut S,
    item_id: &str,
    delta: &str,
) -> Result<()> {
    sink.emit(AgentLoopEvent::TurnItemDelta {
        turn_id: turn.turn_id.clone(),
        item_id: item_id.to_string(),
        delta: delta.to_string(),
    })
    .await
}

fn started_assistant_item(item_id: &str) -> TurnItem {
    TurnItem {
        item_type: TurnItemType::AssistantMessage,
        role: "assistant".to_string(),
        content_text: String::new(),
        content_json: merge_item_metadata(
            Value::Null,
            item_id,
            Some(AgentItemPhase::Commentary),
            AgentItemStatus::InProgress,
        ),
        tool_call_id: String::new(),
        tool_name: String::new(),
        output_item_id: item_id.to_string(),
        phase: Some(AgentItemPhase::Commentary),
        status: Some(AgentItemStatus::InProgress),
        db_row_id: None,
    }
}

fn started_reasoning_item(item_id: &str) -> TurnItem {
    TurnItem {
        item_type: TurnItemType::ReasoningSummary,
        role: "assistant".to_string(),
        content_text: String::new(),
        content_json: merge_item_metadata(Value::Null, item_id, None, AgentItemStatus::InProgress),
        tool_call_id: String::new(),
        tool_name: String::new(),
        output_item_id: item_id.to_string(),
        phase: None,
        status: Some(AgentItemStatus::InProgress),
        db_row_id: None,
    }
}

fn completed_plan_item(item_id: &str, content: String) -> TurnItem {
    TurnItem {
        item_type: TurnItemType::PlanUpdate,
        role: "assistant".to_string(),
        content_text: content,
        content_json: merge_item_metadata(Value::Null, item_id, None, AgentItemStatus::Completed),
        tool_call_id: String::new(),
        tool_name: String::new(),
        output_item_id: item_id.to_string(),
        phase: None,
        status: Some(AgentItemStatus::Completed),
        db_row_id: None,
    }
}

async fn emit_tool_call_status<S: AgentEventSink>(
    turn: &Turn,
    sink: &mut S,
    call: &ToolCallRequest,
    status: AgentItemStatus,
) -> Result<()> {
    sink.emit(AgentLoopEvent::TurnItemCompleted {
        turn_id: turn.turn_id.clone(),
        item: AgentOutputItem::ToolCall {
            id: call.call_id.clone(),
            tool_name: call.name.clone(),
            arguments: call.arguments.clone(),
            status,
        },
    })
    .await
}

async fn emit_tool_result<S: AgentEventSink>(
    turn: &Turn,
    sink: &mut S,
    result: &ToolResultItem,
) -> Result<()> {
    let status = if result.status == "completed" || result.status == "started" {
        AgentItemStatus::Completed
    } else {
        AgentItemStatus::Failed
    };
    sink.emit(AgentLoopEvent::TurnItemCompleted {
        turn_id: turn.turn_id.clone(),
        item: AgentOutputItem::ToolResult {
            id: format!("result-{}", result.call_id),
            tool_call_id: result.call_id.clone(),
            content: result.output.to_string(),
            status,
        },
    })
    .await
}

fn build_model_input(
    conn: &rusqlite::Connection,
    turn: &mut Turn,
    first_iteration: bool,
    config: &AgentLoopConfig,
) -> Result<ModelInput> {
    let mut items = history_items(conn, &turn.session_id, config.history_limit)?;
    if first_iteration && items.is_empty() {
        items.push(TurnItem::user(turn.user_input.clone()));
    }
    while let Some(input) = turn.pending_input.pop_front() {
        let item = TurnItem::user(format!("Steer: {input}"));
        append_turn_item(conn, turn, &item)?;
        items.push(item);
    }
    if items.len() > config.compact_after_items {
        let summary = compact_summary_card(&items);
        let item = TurnItem {
            item_type: TurnItemType::CompactSummary,
            role: "system".to_string(),
            content_text: summary.clone(),
            content_json: json!({"summary": summary}),
            tool_call_id: String::new(),
            tool_name: String::new(),
            output_item_id: String::new(),
            phase: None,
            status: None,
            db_row_id: None,
        };
        append_turn_item(conn, turn, &item)?;
        items = vec![item];
    }
    Ok(ModelInput {
        items,
        available_tools: turn_available_tools(turn),
    })
}

fn turn_available_tools(turn: &Turn) -> Vec<String> {
    turn.model_context
        .lines()
        .find_map(|line| line.strip_prefix("available_tools="))
        .and_then(|value| serde_json::from_str::<Vec<String>>(value).ok())
        .unwrap_or_default()
}

fn history_items(
    conn: &rusqlite::Connection,
    session_id: &str,
    limit: usize,
) -> Result<Vec<TurnItem>> {
    session_history_items(conn, session_id, limit)?
        .into_iter()
        .map(|value| {
            let item_type = match value.get("item_type").and_then(Value::as_str).unwrap_or("") {
                "user_message" => TurnItemType::UserMessage,
                "assistant_message" => TurnItemType::AssistantMessage,
                "reasoning_summary" => TurnItemType::ReasoningSummary,
                "plan_update" => TurnItemType::PlanUpdate,
                "tool_call" => TurnItemType::ToolCall,
                "tool_result" => TurnItemType::ToolResult,
                "system_context" => TurnItemType::SystemContext,
                "developer_context" => TurnItemType::DeveloperContext,
                "compact_summary" => TurnItemType::CompactSummary,
                _ => TurnItemType::InjectedContext,
            };
            Ok(TurnItem {
                item_type,
                role: value
                    .get("role")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                content_text: value
                    .get("content_text")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                content_json: value.get("content_json").cloned().unwrap_or(Value::Null),
                tool_call_id: value
                    .get("tool_call_id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                tool_name: value
                    .get("tool_name")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                output_item_id: value
                    .get("content_json")
                    .and_then(|value| value.get("output_item_id"))
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                phase: value
                    .get("content_json")
                    .and_then(|value| value.get("phase"))
                    .and_then(Value::as_str)
                    .and_then(parse_agent_item_phase),
                status: value
                    .get("content_json")
                    .and_then(|value| value.get("status"))
                    .and_then(Value::as_str)
                    .and_then(parse_agent_item_status),
                db_row_id: None,
            })
        })
        .collect()
}

fn parse_agent_item_phase(value: &str) -> Option<AgentItemPhase> {
    match value {
        "commentary" => Some(AgentItemPhase::Commentary),
        "final" => Some(AgentItemPhase::Final),
        _ => None,
    }
}

fn parse_agent_item_status(value: &str) -> Option<AgentItemStatus> {
    match value {
        "in_progress" => Some(AgentItemStatus::InProgress),
        "completed" => Some(AgentItemStatus::Completed),
        "pending" => Some(AgentItemStatus::Pending),
        "running" => Some(AgentItemStatus::Running),
        "failed" => Some(AgentItemStatus::Failed),
        "interrupted" => Some(AgentItemStatus::Interrupted),
        _ => None,
    }
}

struct ModelStreamHandler<'a, S: AgentEventSink> {
    conn: &'a rusqlite::Connection,
    turn: &'a mut Turn,
    sink: &'a mut S,
    result: ModelStreamResult,
    assistant_buffers: BTreeMap<String, String>,
    reasoning_buffers: BTreeMap<String, String>,
    in_progress: Vec<String>,
}

impl<'a, S: AgentEventSink> ModelStreamHandler<'a, S> {
    fn new(conn: &'a rusqlite::Connection, turn: &'a mut Turn, sink: &'a mut S) -> Self {
        Self {
            conn,
            turn,
            sink,
            result: ModelStreamResult::default(),
            assistant_buffers: BTreeMap::new(),
            reasoning_buffers: BTreeMap::new(),
            in_progress: Vec::new(),
        }
    }

    async fn handle_event(&mut self, event: ModelStreamEvent) -> Result<()> {
        match event {
            ModelStreamEvent::AssistantMessageStarted { item_id } => {
                debug!(
                    turn_id = self.turn.turn_id,
                    item_id, "model stream assistant message started"
                );
                let item = started_assistant_item(&item_id);
                append_turn_item(self.conn, self.turn, &item)?;
                emit_started(
                    self.turn,
                    self.sink,
                    self.turn.emitted_items.last().expect("just appended"),
                )
                .await?;
                self.assistant_buffers
                    .insert(item_id.clone(), String::new());
                self.in_progress.push(item_id);
            }
            ModelStreamEvent::AssistantTextDelta { item_id, delta } => {
                let buffer = self.assistant_buffers.entry(item_id.clone()).or_default();
                buffer.push_str(&delta);
                let _ = update_turn_item(
                    self.conn,
                    self.turn,
                    &item_id,
                    buffer.clone(),
                    Some(AgentItemPhase::Commentary),
                    AgentItemStatus::InProgress,
                )?;
                emit_delta(self.turn, self.sink, &item_id, &delta).await?;
            }
            ModelStreamEvent::AssistantMessageCompleted { item_id } => {
                let text = self.assistant_buffers.remove(&item_id).unwrap_or_default();
                debug!(
                    turn_id = self.turn.turn_id,
                    item_id,
                    text_chars = text.len(),
                    "model stream assistant message completed"
                );
                if let Some(item) = update_turn_item(
                    self.conn,
                    self.turn,
                    &item_id,
                    text,
                    Some(AgentItemPhase::Commentary),
                    AgentItemStatus::Completed,
                )? {
                    emit_completed(self.turn, self.sink, &item).await?;
                }
                self.in_progress.retain(|value| value != &item_id);
                self.result.last_assistant_message_id = Some(item_id);
            }
            ModelStreamEvent::ReasoningSummaryDelta { item_id, delta } => {
                if !self.reasoning_buffers.contains_key(&item_id) {
                    let item = started_reasoning_item(&item_id);
                    append_turn_item(self.conn, self.turn, &item)?;
                    emit_started(
                        self.turn,
                        self.sink,
                        self.turn.emitted_items.last().expect("just appended"),
                    )
                    .await?;
                    self.in_progress.push(item_id.clone());
                }
                let buffer = self.reasoning_buffers.entry(item_id.clone()).or_default();
                buffer.push_str(&delta);
                let _ = update_turn_item(
                    self.conn,
                    self.turn,
                    &item_id,
                    buffer.clone(),
                    None,
                    AgentItemStatus::InProgress,
                )?;
                emit_delta(self.turn, self.sink, &item_id, &delta).await?;
            }
            ModelStreamEvent::ReasoningSummaryCompleted { item_id } => {
                let text = self.reasoning_buffers.remove(&item_id).unwrap_or_default();
                debug!(
                    turn_id = self.turn.turn_id,
                    item_id,
                    text_chars = text.len(),
                    "model stream reasoning summary completed"
                );
                if let Some(item) = update_turn_item(
                    self.conn,
                    self.turn,
                    &item_id,
                    text,
                    None,
                    AgentItemStatus::Completed,
                )? {
                    emit_completed(self.turn, self.sink, &item).await?;
                }
                self.in_progress.retain(|value| value != &item_id);
            }
            ModelStreamEvent::PlanUpdateCompleted { item_id, content } => {
                debug!(
                    turn_id = self.turn.turn_id,
                    item_id,
                    content_chars = content.len(),
                    "model stream plan update completed"
                );
                let item = completed_plan_item(&item_id, content);
                append_turn_item(self.conn, self.turn, &item)?;
                emit_completed(
                    self.turn,
                    self.sink,
                    self.turn.emitted_items.last().expect("just appended"),
                )
                .await?;
            }
            ModelStreamEvent::ToolCallCompleted { tool_call } => {
                debug!(
                    turn_id = self.turn.turn_id,
                    call_id = tool_call.call_id,
                    tool = tool_call.name,
                    mode = ?tool_call.mode,
                    "model stream tool call requested"
                );
                let item = TurnItem::tool_call(&tool_call);
                append_turn_item(self.conn, self.turn, &item)?;
                emit_completed(
                    self.turn,
                    self.sink,
                    self.turn.emitted_items.last().expect("just appended"),
                )
                .await?;
                self.result.needs_follow_up = true;
                self.result.tool_calls.push(tool_call.clone());
                self.turn.pending_tool_calls.push(tool_call);
            }
            ModelStreamEvent::ResponseCompleted { end_turn, raw: _ } => {
                debug!(
                    turn_id = self.turn.turn_id,
                    end_turn, "model stream response completed"
                );
                if !end_turn {
                    self.result.needs_follow_up = true;
                }
            }
            ModelStreamEvent::StreamInterrupted { error } => {
                warn!(
                    turn_id = self.turn.turn_id,
                    error, "model stream interrupted"
                );
                self.mark_in_progress_interrupted().await?;
                bail!("model stream interrupted: {error}");
            }
        }
        Ok(())
    }

    async fn mark_in_progress_interrupted(&mut self) -> Result<()> {
        let ids = self.in_progress.clone();
        for item_id in ids {
            let text = self
                .assistant_buffers
                .remove(&item_id)
                .or_else(|| self.reasoning_buffers.remove(&item_id))
                .unwrap_or_default();
            let phase = self
                .turn
                .emitted_items
                .iter()
                .rev()
                .find(|item| item.output_item_id == item_id)
                .and_then(|item| item.phase.clone());
            if let Some(item) = update_turn_item(
                self.conn,
                self.turn,
                &item_id,
                text,
                phase,
                AgentItemStatus::Interrupted,
            )? {
                emit_completed(self.turn, self.sink, &item).await?;
            }
        }
        Ok(())
    }

    async fn finish(mut self) -> Result<ModelStreamResult> {
        self.result.needs_follow_up = self.result.needs_follow_up
            || !self.turn.pending_tool_calls.is_empty()
            || !self.turn.pending_input.is_empty();
        self.turn.needs_follow_up = self.result.needs_follow_up;
        persist_turn(self.conn, self.turn)?;
        Ok(self.result)
    }
}

impl<S: AgentEventSink> ModelEventHandler for ModelStreamHandler<'_, S> {
    fn handle<'a>(
        &'a mut self,
        event: ModelStreamEvent,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
        Box::pin(async move { self.handle_event(event).await })
    }
}

async fn mark_last_assistant_message_as_final<S: AgentEventSink>(
    conn: &rusqlite::Connection,
    turn: &mut Turn,
    item_id: &str,
    sink: &mut S,
) -> Result<()> {
    if let Some(item) = update_turn_item(
        conn,
        turn,
        item_id,
        turn.emitted_items
            .iter()
            .rev()
            .find(|item| item.output_item_id == item_id)
            .map(|item| item.content_text.clone())
            .unwrap_or_default(),
        Some(AgentItemPhase::Final),
        AgentItemStatus::Completed,
    )? {
        emit_completed(turn, sink, &item).await?;
    }
    Ok(())
}

pub fn parse_react_response(value: Value) -> Result<ModelResponse> {
    let has_react_shape = value.get("assistant_message").is_some()
        || value.get("message").is_some()
        || value.get("reasoning_summary").is_some()
        || value.get("tool_calls").is_some()
        || value.get("end_turn").is_some();
    if !has_react_shape {
        return Ok(ModelResponse {
            assistant_message: Some(value.to_string()),
            reasoning_summary: None,
            tool_calls: Vec::new(),
            end_turn: true,
            raw: value,
        });
    }
    let assistant_message = value
        .get("assistant_message")
        .or_else(|| value.get("message"))
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let reasoning_summary = value
        .get("reasoning_summary")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let end_turn = value
        .get("end_turn")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let tool_calls = value
        .get("tool_calls")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    serde_json::from_value::<ToolCallRequest>(item.clone())
                        .context("invalid tool call item")
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();
    Ok(ModelResponse {
        assistant_message,
        reasoning_summary,
        tool_calls,
        end_turn,
        raw: value,
    })
}

pub(crate) fn assistant_message_needs_follow_up(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return true;
    }
    if trimmed.chars().count() > 1_200 {
        return false;
    }
    if extract_json_value(trimmed).is_ok_and(|value| value.is_object()) {
        return false;
    }
    let lower = trimmed.to_ascii_lowercase();
    [
        "i need a few key inputs",
        "i need a ticker",
        "without a ticker",
        "once you provide",
        "ready to analyze",
        "i'll ask for it",
        "attempting to",
        "try one more",
        "retry",
        "无法给到相关内容",
        "开始执行",
        "正在读取",
        "接下来",
        "现在使用",
        "尝试最后一次",
        "若仍失败",
        "需要补上",
    ]
    .iter()
    .any(|pattern| lower.contains(pattern) || trimmed.contains(pattern))
}

pub fn react_prompt(input: &ModelInput) -> Result<String> {
    let items = input
        .items
        .iter()
        .map(|item| {
            json!({
                "type": item.item_type.as_str(),
                "role": item.role,
                "content_text": truncate_chars(&item.content_text, MAX_CONTEXT_FRAGMENT_CHARS),
                "content_json": item.content_json,
                "tool_call_id": item.tool_call_id,
                "tool_name": item.tool_name
            })
        })
        .collect::<Vec<_>>();
    Ok(format!(
        "You are running inside an agent loop runtime. Decide the next step from these ordered context items.\n\
Return newline-delimited JSON events only. Each line must be one complete JSON object, with no markdown fences.\n\
Use assistant message events for visible text. Intermediate explanations, plans, and current action notes should be emitted as assistant_message items; the runtime records them as commentary until the turn truly ends.\n\
Supported event shapes:\n\
{{\"type\":\"assistant_message_started\",\"item_id\":\"msg-1\"}}\n\
{{\"type\":\"assistant_text_delta\",\"item_id\":\"msg-1\",\"delta\":\"visible text chunk\"}}\n\
{{\"type\":\"assistant_message_completed\",\"item_id\":\"msg-1\"}}\n\
{{\"type\":\"reasoning_summary_delta\",\"item_id\":\"reasoning-1\",\"delta\":\"brief reasoning summary chunk\"}}\n\
{{\"type\":\"reasoning_summary_completed\",\"item_id\":\"reasoning-1\"}}\n\
{{\"type\":\"plan_update_completed\",\"item_id\":\"plan-1\",\"content\":\"plan or status update\"}}\n\
{{\"type\":\"tool_call_completed\",\"tool_call\":{{\"call_id\":\"call-1\",\"name\":\"tool_name\",\"arguments\":{{}},\"mode\":\"blocking\"}}}}\n\
{{\"type\":\"response_completed\",\"end_turn\":true}}\n\
If you need a tool, emit any visible commentary first, then tool_call_completed, then response_completed with end_turn=false. If tool results answer the task, emit the final assistant_message and response_completed with end_turn=true. A final assistant_message must be the complete role artifact, preferably one JSON object with id, role, status, report, and per_ticker. Do not end the turn with text saying you are about to retry, fetch, analyze, or wait for inputs; call the tool or produce the final artifact. For web.run use {{\"search_query\":[{{\"q\":\"TQQQ QQQ VIX site:reddit.com\",\"domains\":[\"reddit.com\"],\"numResults\":10}}],\"response_length\":\"medium\"}}. For fetch_last30days_context use {{\"source\":\"reddit\",\"tickers\":[\"TQQQ\"]}}. The runtime also accepts the older single-object response shape only as a compatibility fallback.\n\n\
Available tools:\n{}\n\n\
Context items:\n{}",
        serde_json::to_string_pretty(&input.available_tools)?,
        serde_json::to_string_pretty(&items)?
    ))
}

pub(crate) fn extract_json_value(text: &str) -> Result<Value> {
    if let Ok(value) = serde_json::from_str::<Value>(text) {
        return Ok(value);
    }
    let Some(start) = text.find('{') else {
        bail!("model response did not contain JSON object")
    };
    let Some(end) = text.rfind('}') else {
        bail!("model response did not contain complete JSON object")
    };
    serde_json::from_str(&text[start..=end]).context("failed to parse ReAct JSON response")
}

pub fn compact_summary_card(items: &[TurnItem]) -> String {
    let recent = items
        .iter()
        .rev()
        .take(8)
        .map(|item| {
            format!(
                "- {} {} {}",
                item.item_type.as_str(),
                item.tool_name,
                truncate_chars(&item.content_text, 240)
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "Conversation Summary Card\n\nGoal:\n- Continue the current agent turn.\n\nDecisions:\n- Preserve ReAct item order and only inject compact state into the next model request.\n\nCurrent State:\n- {} items were compacted.\n\nOpen Tasks:\n- Continue from the latest pending input, tool result, or assistant request.\n\nImportant Context:\n- Do not drop file paths, commands, errors, or user steering.\n\nRecent Tool Results:\n{}",
        items.len(),
        recent
    )
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    let mut out = value.chars().take(max_chars).collect::<String>();
    out.push_str("\n[truncated]");
    out
}

pub struct StaticToolRuntime {
    tools: BTreeMap<String, Box<dyn Fn(Value) -> ToolResultItem + Send>>,
}

pub struct ProjectToolRuntime {
    config: tools::ExternalToolConfig,
    web_run: Option<tools::WebRunRuntime>,
    turn_context: Option<ToolRuntimeTurnContext>,
}

impl ProjectToolRuntime {
    pub fn new(config: tools::ExternalToolConfig) -> Self {
        Self {
            config,
            web_run: None,
            turn_context: None,
        }
    }

    pub fn with_web_run_runtime(mut self, web_run: tools::WebRunRuntime) -> Self {
        self.web_run = Some(web_run);
        self
    }
}

impl LoopToolRuntime for ProjectToolRuntime {
    fn set_turn_context(&mut self, context: ToolRuntimeTurnContext) {
        debug!(
            run_id = context.run_id,
            session_id = context.session_id,
            turn_id = context.turn_id,
            role = context.role,
            "project tool runtime context set"
        );
        self.turn_context = Some(context);
    }

    fn execute<'a>(
        &'a mut self,
        call: ToolCallRequest,
    ) -> Pin<Box<dyn Future<Output = ToolResultItem> + Send + 'a>> {
        Box::pin(async move {
            debug!(
                call_id = call.call_id,
                tool = call.name,
                mode = ?call.mode,
                "project tool runtime dispatching tool"
            );
            if call.name == "think" {
                return ToolResultItem {
                    call_id: call.call_id,
                    name: call.name,
                    status: "completed".to_string(),
                    output: json!({
                        "status": "completed",
                        "summary": call.arguments
                    }),
                    error: None,
                    background_job_id: None,
                };
            }
            let web_run_config = self.web_run.as_ref().map(tools::WebRunRuntime::config);
            if call.name != tools::WEB_RUN_TOOL_NAME
                && !tools::enabled_tool_names(web_run_config).contains(&call.name.as_str())
            {
                warn!(
                    call_id = call.call_id,
                    tool = call.name,
                    "project tool runtime rejected unknown tool"
                );
                return ToolResultItem {
                    call_id: call.call_id,
                    name: call.name,
                    status: "error".to_string(),
                    output: Value::Null,
                    error: Some("unknown tool name".to_string()),
                    background_job_id: None,
                };
            }
            let call_id = call.call_id;
            let name = call.name;
            if name == tools::WEB_RUN_TOOL_NAME {
                let output = if let Some(web_run) = &self.web_run {
                    web_run.execute(call.arguments).await
                } else {
                    tools::execute_named_tool(
                        &name,
                        call.arguments,
                        &self.config,
                        self.turn_context.as_ref(),
                        None,
                    )
                    .await
                };
                return match output {
                    Ok(output) => {
                        debug!(call_id, tool = name, "web.run tool completed");
                        ToolResultItem {
                            call_id,
                            name,
                            status: "completed".to_string(),
                            output,
                            error: None,
                            background_job_id: None,
                        }
                    }
                    Err(error) => {
                        warn!(call_id, tool = name, error = %error, "web.run tool failed");
                        ToolResultItem {
                            call_id,
                            name,
                            status: "error".to_string(),
                            output: Value::Null,
                            error: Some(error.to_string()),
                            background_job_id: None,
                        }
                    }
                };
            }
            match tools::execute_named_tool(
                &name,
                call.arguments,
                &self.config,
                self.turn_context.as_ref(),
                self.web_run.as_ref(),
            )
            .await
            {
                Ok(output) => {
                    debug!(call_id, tool = name, "project tool completed");
                    ToolResultItem {
                        call_id,
                        name,
                        status: "completed".to_string(),
                        output,
                        error: None,
                        background_job_id: None,
                    }
                }
                Err(error) => {
                    warn!(call_id, tool = name, error = %error, "project tool failed");
                    ToolResultItem {
                        call_id,
                        name,
                        status: "error".to_string(),
                        output: Value::Null,
                        error: Some(error.to_string()),
                        background_job_id: None,
                    }
                }
            }
        })
    }
}

impl StaticToolRuntime {
    pub fn new() -> Self {
        Self {
            tools: BTreeMap::new(),
        }
    }

    pub fn add_tool<F>(&mut self, name: impl Into<String>, tool: F)
    where
        F: Fn(Value) -> ToolResultItem + Send + 'static,
    {
        self.tools.insert(name.into(), Box::new(tool));
    }
}

impl Default for StaticToolRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl LoopToolRuntime for StaticToolRuntime {
    fn execute<'a>(
        &'a mut self,
        call: ToolCallRequest,
    ) -> Pin<Box<dyn Future<Output = ToolResultItem> + Send + 'a>> {
        Box::pin(async move {
            let Some(tool) = self.tools.get(&call.name) else {
                return ToolResultItem {
                    call_id: call.call_id,
                    name: call.name,
                    status: "error".to_string(),
                    output: Value::Null,
                    error: Some("unknown tool name".to_string()),
                    background_job_id: None,
                };
            };
            tool(call.arguments)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::web_search::{MockWebPage, MockWebSearchProvider, WebSearchConfig, WebSearchMode};
    use orchestrator_sql::ensure_schema;
    use serde_json::json;
    use std::{path::PathBuf, sync::Arc};

    struct FakeModel {
        responses: VecDeque<ModelResponse>,
        seen_inputs: Vec<ModelInput>,
    }

    impl FakeModel {
        fn new(responses: Vec<ModelResponse>) -> Self {
            Self {
                responses: VecDeque::from(responses),
                seen_inputs: Vec::new(),
            }
        }
    }

    impl LoopModel for FakeModel {
        fn generate<'a>(
            &'a mut self,
            input: ModelInput,
        ) -> Pin<Box<dyn Future<Output = Result<ModelResponse>> + Send + 'a>> {
            Box::pin(async move {
                self.seen_inputs.push(input);
                self.responses
                    .pop_front()
                    .context("fake model has no response")
            })
        }
    }

    #[derive(Default)]
    struct RecordingSink {
        events: Vec<AgentLoopEvent>,
    }

    impl AgentEventSink for RecordingSink {
        fn emit<'a>(
            &'a mut self,
            event: AgentLoopEvent,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move {
                self.events.push(event);
                Ok(())
            })
        }
    }

    struct FakeStreamModel {
        event_batches: VecDeque<Vec<ModelStreamEvent>>,
        seen_inputs: Vec<ModelInput>,
    }

    impl FakeStreamModel {
        fn new(event_batches: Vec<Vec<ModelStreamEvent>>) -> Self {
            Self {
                event_batches: VecDeque::from(event_batches),
                seen_inputs: Vec::new(),
            }
        }
    }

    impl LoopModel for FakeStreamModel {
        fn generate<'a>(
            &'a mut self,
            _input: ModelInput,
        ) -> Pin<Box<dyn Future<Output = Result<ModelResponse>> + Send + 'a>> {
            Box::pin(async { bail!("fake stream model does not use generate") })
        }

        fn stream_events<'a>(
            &'a mut self,
            input: ModelInput,
            handler: &'a mut dyn ModelEventHandler,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
            Box::pin(async move {
                self.seen_inputs.push(input);
                for event in self
                    .event_batches
                    .pop_front()
                    .context("fake stream model has no event batch")?
                {
                    handler.handle(event).await?;
                }
                Ok(())
            })
        }
    }

    fn model_response(message: Option<&str>, end_turn: bool) -> ModelResponse {
        ModelResponse {
            assistant_message: message.map(ToString::to_string),
            reasoning_summary: None,
            tool_calls: vec![],
            end_turn,
            raw: json!({
                "assistant_message": message,
                "end_turn": end_turn
            }),
        }
    }

    fn assistant_texts(conn: &rusqlite::Connection) -> Vec<String> {
        let mut stmt = conn
            .prepare(
                "SELECT content_text FROM agent_turn_items \
                 WHERE item_type = 'assistant_message' ORDER BY item_index",
            )
            .unwrap();
        stmt.query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn item_count(conn: &rusqlite::Connection, item_type: &str) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM agent_turn_items WHERE item_type = ?",
            [item_type],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn turn_end_state(conn: &rusqlite::Connection, turn_id: &str) -> (bool, String) {
        conn.query_row(
            "SELECT needs_follow_up, end_reason FROM agent_turns WHERE turn_id = ?",
            [turn_id],
            |row| {
                let needs_follow_up: i64 = row.get(0)?;
                let end_reason: String = row.get(1)?;
                Ok((needs_follow_up != 0, end_reason))
            },
        )
        .unwrap()
    }

    #[tokio::test]
    async fn intermediate_assistant_text_before_tool_call_keeps_turn_open_for_follow_up() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        ensure_schema(&conn).unwrap();
        let mut first = model_response(Some("Checking context before I call a tool."), false);
        first.tool_calls = vec![ToolCallRequest {
            call_id: "call-1".to_string(),
            name: "echo".to_string(),
            arguments: json!({"text": "observed"}),
            mode: ToolMode::Blocking,
        }];
        let mut model = FakeModel::new(vec![
            first,
            model_response(Some("Final after tool result."), true),
        ]);
        let mut tools = StaticToolRuntime::new();
        tools.add_tool("echo", |args| ToolResultItem {
            call_id: "call-1".to_string(),
            name: "echo".to_string(),
            status: "completed".to_string(),
            output: args,
            error: None,
            background_job_id: None,
        });
        let mut turn = Turn::new("turn-1", "session-1", "run-1", "analyst.test", "start");

        run_turn(
            &conn,
            &mut turn,
            &mut model,
            &mut tools,
            AgentLoopConfig::default(),
        )
        .await
        .unwrap();

        assert_eq!(model.seen_inputs.len(), 2);
        assert_eq!(
            assistant_texts(&conn),
            vec![
                "Checking context before I call a tool.".to_string(),
                "Final after tool result.".to_string(),
            ]
        );
        assert_eq!(item_count(&conn, "tool_call"), 1);
        assert_eq!(item_count(&conn, "tool_result"), 1);
        assert_eq!(
            turn_end_state(&conn, "turn-1"),
            (false, "completed".to_string())
        );
    }

    #[tokio::test]
    async fn final_assistant_text_completes_turn_only_when_no_follow_up_work_exists() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        ensure_schema(&conn).unwrap();
        let mut model = FakeModel::new(vec![model_response(
            Some("Final answer without more work."),
            true,
        )]);
        let mut tools = StaticToolRuntime::new();
        let mut turn = Turn::new("turn-1", "session-1", "run-1", "analyst.test", "start");

        run_turn(
            &conn,
            &mut turn,
            &mut model,
            &mut tools,
            AgentLoopConfig::default(),
        )
        .await
        .unwrap();

        assert_eq!(model.seen_inputs.len(), 1);
        assert_eq!(
            assistant_texts(&conn),
            vec!["Final answer without more work.".to_string()]
        );
        assert_eq!(item_count(&conn, "tool_call"), 0);
        assert_eq!(
            turn_end_state(&conn, "turn-1"),
            (false, "completed".to_string())
        );
    }

    #[tokio::test]
    async fn end_turn_false_with_assistant_text_requests_another_model_iteration() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        ensure_schema(&conn).unwrap();
        let mut model = FakeModel::new(vec![
            model_response(
                Some("I have a partial answer and need another pass."),
                false,
            ),
            model_response(Some("Final answer after the follow-up pass."), true),
        ]);
        let mut tools = StaticToolRuntime::new();
        let mut turn = Turn::new("turn-1", "session-1", "run-1", "analyst.test", "start");

        run_turn(
            &conn,
            &mut turn,
            &mut model,
            &mut tools,
            AgentLoopConfig::default(),
        )
        .await
        .unwrap();

        assert_eq!(model.seen_inputs.len(), 2);
        assert!(model.seen_inputs[1].items.iter().any(|item| {
            item.item_type == TurnItemType::AssistantMessage
                && item.content_text == "I have a partial answer and need another pass."
        }));
        assert_eq!(
            assistant_texts(&conn),
            vec![
                "I have a partial answer and need another pass.".to_string(),
                "Final answer after the follow-up pass.".to_string(),
            ]
        );
        assert_eq!(
            turn_end_state(&conn, "turn-1"),
            (false, "completed".to_string())
        );
    }

    #[tokio::test]
    async fn streaming_assistant_text_deltas_merge_into_one_intermediate_item() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        ensure_schema(&conn).unwrap();
        let long_text = (0..300)
            .map(|index| format!("intermediate-delta-{index:03};"))
            .collect::<String>();
        let split_at = long_text.len() / 2;
        let mut model = FakeStreamModel::new(vec![
            vec![
                ModelStreamEvent::AssistantMessageStarted {
                    item_id: "msg-1".to_string(),
                },
                ModelStreamEvent::AssistantTextDelta {
                    item_id: "msg-1".to_string(),
                    delta: long_text[..split_at].to_string(),
                },
                ModelStreamEvent::AssistantTextDelta {
                    item_id: "msg-1".to_string(),
                    delta: long_text[split_at..].to_string(),
                },
                ModelStreamEvent::AssistantMessageCompleted {
                    item_id: "msg-1".to_string(),
                },
                ModelStreamEvent::ResponseCompleted {
                    end_turn: false,
                    raw: json!({"step": 1}),
                },
            ],
            vec![
                ModelStreamEvent::AssistantMessageStarted {
                    item_id: "msg-2".to_string(),
                },
                ModelStreamEvent::AssistantTextDelta {
                    item_id: "msg-2".to_string(),
                    delta: "Final after long intermediate text.".to_string(),
                },
                ModelStreamEvent::AssistantMessageCompleted {
                    item_id: "msg-2".to_string(),
                },
                ModelStreamEvent::ResponseCompleted {
                    end_turn: true,
                    raw: json!({"step": 2}),
                },
            ],
        ]);
        let mut tools = StaticToolRuntime::new();
        let mut sink = RecordingSink::default();
        let mut turn = Turn::new("turn-1", "session-1", "run-1", "analyst.test", "start");

        run_turn_with_events(
            &conn,
            &mut turn,
            &mut model,
            &mut tools,
            AgentLoopConfig::default(),
            &mut sink,
        )
        .await
        .unwrap();

        assert_eq!(assistant_texts(&conn)[0], long_text);
        assert!(model.seen_inputs[1].items.iter().any(|item| {
            item.item_type == TurnItemType::AssistantMessage && item.content_text == long_text
        }));
        let msg_1_deltas = sink
            .events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    AgentLoopEvent::TurnItemDelta { item_id, .. } if item_id == "msg-1"
                )
            })
            .count();
        assert_eq!(msg_1_deltas, 2);
    }

    #[tokio::test]
    async fn model_stream_handler_emits_and_persists_deltas_immediately() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        ensure_schema(&conn).unwrap();
        let mut sink = RecordingSink::default();
        let mut turn = Turn::new("turn-1", "session-1", "run-1", "analyst.test", "start");
        persist_turn(&conn, &turn).unwrap();

        {
            let mut handler = ModelStreamHandler::new(&conn, &mut turn, &mut sink);
            handler
                .handle(ModelStreamEvent::AssistantMessageStarted {
                    item_id: "msg-live".to_string(),
                })
                .await
                .unwrap();
            handler
                .handle(ModelStreamEvent::AssistantTextDelta {
                    item_id: "msg-live".to_string(),
                    delta: "live chunk".to_string(),
                })
                .await
                .unwrap();
        }

        assert!(sink.events.iter().any(|event| {
            matches!(
                event,
                AgentLoopEvent::TurnItemDelta { item_id, delta, .. }
                    if item_id == "msg-live" && delta == "live chunk"
            )
        }));
        assert_eq!(assistant_texts(&conn), vec!["live chunk".to_string()]);
        let content_json: String = conn
            .query_row(
                "SELECT content_json FROM agent_turn_items \
                 WHERE item_type = 'assistant_message'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let content_json: Value = serde_json::from_str(&content_json).unwrap();
        assert_eq!(content_json["status"], "in_progress");
        assert_eq!(content_json["phase"], "commentary");
    }

    #[tokio::test]
    async fn streaming_interruption_marks_in_progress_assistant_item_interrupted() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        ensure_schema(&conn).unwrap();
        let mut model = FakeStreamModel::new(vec![vec![
            ModelStreamEvent::AssistantMessageStarted {
                item_id: "msg-1".to_string(),
            },
            ModelStreamEvent::AssistantTextDelta {
                item_id: "msg-1".to_string(),
                delta: "Partial before interruption.".to_string(),
            },
            ModelStreamEvent::StreamInterrupted {
                error: "connection closed".to_string(),
            },
        ]]);
        let mut tools = StaticToolRuntime::new();
        let mut sink = RecordingSink::default();
        let mut turn = Turn::new("turn-1", "session-1", "run-1", "analyst.test", "start");

        let result = run_turn_with_events(
            &conn,
            &mut turn,
            &mut model,
            &mut tools,
            AgentLoopConfig {
                max_agent_loops: Some(1),
                ..AgentLoopConfig::default()
            },
            &mut sink,
        )
        .await;

        assert!(result.is_err());
        let content_json: String = conn
            .query_row(
                "SELECT content_json FROM agent_turn_items \
                 WHERE item_type = 'assistant_message'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let content_json: Value = serde_json::from_str(&content_json).unwrap();
        assert_eq!(content_json["status"], "interrupted");
        assert!(sink.events.iter().any(|event| {
            matches!(
                event,
                AgentLoopEvent::TurnItemCompleted {
                    item: AgentOutputItem::AssistantMessage {
                        id,
                        status: AgentItemStatus::Interrupted,
                        ..
                    },
                    ..
                } if id == "msg-1"
            )
        }));
    }

    #[tokio::test]
    async fn run_turn_executes_tool_and_feeds_result_back_to_model() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        ensure_schema(&conn).unwrap();
        let mut model = FakeModel::new(vec![
            ModelResponse {
                assistant_message: Some("I need the tool.".to_string()),
                reasoning_summary: Some("Need observation.".to_string()),
                tool_calls: vec![ToolCallRequest {
                    call_id: "call-1".to_string(),
                    name: "echo".to_string(),
                    arguments: json!({"text": "observed"}),
                    mode: ToolMode::Blocking,
                }],
                end_turn: false,
                raw: json!({"step": 1}),
            },
            ModelResponse {
                assistant_message: Some("Final after observed.".to_string()),
                reasoning_summary: None,
                tool_calls: vec![],
                end_turn: true,
                raw: json!({"step": 2}),
            },
        ]);
        let mut tools = StaticToolRuntime::new();
        tools.add_tool("echo", |args| ToolResultItem {
            call_id: "call-1".to_string(),
            name: "echo".to_string(),
            status: "completed".to_string(),
            output: args,
            error: None,
            background_job_id: None,
        });
        let mut turn = Turn::new("turn-1", "session-1", "run-1", "analyst.test", "start");

        run_turn(
            &conn,
            &mut turn,
            &mut model,
            &mut tools,
            AgentLoopConfig::default(),
        )
        .await
        .unwrap();

        assert_eq!(turn.end_reason.as_deref(), Some("completed"));
        assert_eq!(model.seen_inputs.len(), 2);
        assert!(model.seen_inputs[1]
            .items
            .iter()
            .any(|item| item.item_type == TurnItemType::ToolResult));

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM agent_turn_items", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 6);
        let user_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM agent_turn_items WHERE item_type = 'user_message'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(user_count, 1);
    }

    #[tokio::test]
    async fn web_run_tool_result_is_written_to_history_and_triggers_follow_up() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        ensure_schema(&conn).unwrap();
        let mut model = FakeModel::new(vec![
            ModelResponse {
                assistant_message: Some("Searching current context.".to_string()),
                reasoning_summary: None,
                tool_calls: vec![ToolCallRequest {
                    call_id: "call-web".to_string(),
                    name: tools::WEB_RUN_TOOL_NAME.to_string(),
                    arguments: json!({"search_query": [{"q": "TQQQ liquidity"}]}),
                    mode: ToolMode::Blocking,
                }],
                end_turn: false,
                raw: json!({"step": 1}),
            },
            ModelResponse {
                assistant_message: Some("Final after web result.".to_string()),
                reasoning_summary: None,
                tool_calls: vec![],
                end_turn: true,
                raw: json!({"step": 2}),
            },
        ]);
        let config = WebSearchConfig {
            mode: WebSearchMode::Cached,
            ..WebSearchConfig::default()
        };
        let provider = MockWebSearchProvider::new(vec![MockWebPage {
            title: "TQQQ liquidity update".to_string(),
            url: "https://research.example.com/tqqq-liquidity".to_string(),
            content: "TQQQ liquidity and volatility context for the current session.".to_string(),
        }]);
        let mut tools = ProjectToolRuntime::new(tools::ExternalToolConfig {
            project_root: PathBuf::from("."),
            db_path: None,
            run_dir: None,
            run_id: None,
            tickers: vec!["TQQQ".to_string()],
        })
        .with_web_run_runtime(tools::WebRunRuntime::new(config).with_provider(Arc::new(provider)));
        let mut turn = Turn::new("turn-web", "session-1", "run-1", "analyst.test", "start");

        run_turn(
            &conn,
            &mut turn,
            &mut model,
            &mut tools,
            AgentLoopConfig::default(),
        )
        .await
        .unwrap();

        assert_eq!(model.seen_inputs.len(), 2);
        assert_eq!(
            model.seen_inputs[1]
                .items
                .iter()
                .filter(|item| item.item_type == TurnItemType::ToolResult)
                .count(),
            1
        );
        let tool_result = model.seen_inputs[1]
            .items
            .iter()
            .find(|item| item.item_type == TurnItemType::ToolResult)
            .unwrap();
        assert_eq!(tool_result.role, "tool");
        assert_eq!(tool_result.tool_call_id, "call-web");
        assert_eq!(tool_result.tool_name, tools::WEB_RUN_TOOL_NAME);
        assert!(tool_result.content_text.contains("Search results:"));
        assert!(tool_result
            .content_text
            .contains("Title: TQQQ liquidity update"));
        assert!(!tool_result.content_text.starts_with('{'));
        assert_eq!(
            turn_end_state(&conn, "turn-web"),
            (false, "completed".to_string())
        );

        let stored_tool_result: String = conn
            .query_row(
                "SELECT content_text FROM agent_turn_items \
                 WHERE item_type = 'tool_result' AND tool_name = ?",
                [tools::WEB_RUN_TOOL_NAME],
                |row| row.get(0),
            )
            .unwrap();
        assert!(stored_tool_result.contains("URL: https://research.example.com/tqqq-liquidity"));
    }

    #[test]
    fn parse_react_response_reads_structured_tool_calls() {
        let response = parse_react_response(json!({
            "assistant_message": "checking",
            "reasoning_summary": "needs data",
            "end_turn": false,
            "tool_calls": [{
                "call_id": "call-1",
                "name": "read_context",
                "arguments": {"query": "latest"},
                "mode": "blocking"
            }]
        }))
        .unwrap();

        assert_eq!(response.assistant_message.as_deref(), Some("checking"));
        assert!(!response.end_turn);
        assert_eq!(response.tool_calls[0].name, "read_context");
    }

    #[test]
    fn agent_loop_event_serializes_output_item_snapshot() {
        let event = AgentLoopEvent::TurnItemStarted {
            turn_id: "turn-1".to_string(),
            item: AgentOutputItem::AssistantMessage {
                id: "msg-1".to_string(),
                phase: AgentItemPhase::Commentary,
                content: "partial answer".to_string(),
                status: AgentItemStatus::InProgress,
            },
        };

        let value = serde_json::to_value(event).unwrap();

        assert_eq!(value["type"], "turn_item_started");
        assert_eq!(value["turn_id"], "turn-1");
        assert_eq!(value["item"]["type"], "assistant_message");
        assert_eq!(value["item"]["phase"], "commentary");
        assert_eq!(value["item"]["content"], "partial answer");
        assert_eq!(value["item"]["status"], "in_progress");
    }
}
