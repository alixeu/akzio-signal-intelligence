use anyhow::{bail, Context, Result};
use orchestrator_core;
use orchestrator_sql::{turn_history_items, upsert_agent_turn, AgentTurnInput};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    path::PathBuf,
    pin::Pin,
    time::Instant,
};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::llm_judge::{judge_message_status, JudgeConfig};
use crate::tools::{self, truncate_chars};
use crate::truncation::{truncate_semantic, TruncationConfig};
use crate::RigSettings;

const DEFAULT_MAX_AGENT_LOOPS: usize = 8;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnItemType {
    UserMessage,
    AssistantMessage,
    ReasoningSummary,
    ReasoningState,
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
            Self::ReasoningState => "reasoning_state",
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallRequest {
    pub call_id: String,
    pub name: String,
    #[serde(default)]
    pub arguments: Value,
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

    pub fn tool_result(result: &ToolResultItem, truncation: &TruncationConfig) -> Self {
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
        let truncated_text = truncate_tool_result(&content_text, truncation);
        // Keep content_json lean. Storing the raw tool payload here previously
        // re-inflated truncated content_text when model_prompt serialized both.
        let compact_output =
            compact_tool_output_for_history(&result.output, &truncated_text, truncation);
        let compact_result = ToolResultItem {
            call_id: result.call_id.clone(),
            name: result.name.clone(),
            status: result.status.clone(),
            output: compact_output,
            error: result.error.clone(),
        };
        Self {
            item_type: TurnItemType::ToolResult,
            role: "tool".to_string(),
            content_text: truncated_text,
            content_json: json!({
                "result": compact_result,
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
    /// When true, subsequent model iterations get no tools and must emit the artifact.
    pub tools_disabled: bool,
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
            tools_disabled: false,
        }
    }

    pub fn push_pending_input(&mut self, input: impl Into<String>) {
        self.pending_input.push_back(input.into());
    }
}

#[derive(Debug, Clone)]
pub struct ModelInput {
    pub system_instruction: Option<String>,
    pub items: Vec<TurnItem>,
    pub available_tools: Vec<String>,
    pub truncation: TruncationConfig,
}

#[derive(Debug, Clone)]
pub struct ModelResponse {
    pub assistant_message: Option<String>,
    pub reasoning_summary: Option<String>,
    pub tool_calls: Vec<ToolCallRequest>,
    pub end_turn: bool,
    pub raw: Value,
    pub turn_status: TurnStatus,
}

#[derive(Debug, Clone)]
pub enum ModelStreamEvent {
    AssistantMessageStarted {
        item_id: String,
    },
    AssistantTextDelta {
        item_id: String,
        delta: String,
    },
    AssistantMessageCompleted {
        item_id: String,
        turn_status: TurnStatus,
    },
    ReasoningSummaryDelta {
        item_id: String,
        delta: String,
    },
    ReasoningSummaryCompleted {
        item_id: String,
    },
    ReasoningStateCompleted {
        item_id: String,
        encrypted_content: String,
    },
    ToolCallCompleted {
        tool_call: ToolCallRequest,
    },
    ResponseCompleted {
        end_turn: bool,
        raw: Value,
    },
}

#[derive(Debug, Clone, Default)]
pub struct ModelStreamResult {
    pub needs_follow_up: bool,
    pub last_assistant_message_id: Option<String>,
    pub tool_calls: Vec<ToolCallRequest>,
    pub usage: TokenUsage,
    pub turn_count: u64,
    pub tool_call_count: u64,
    /// Wall time spent inside model stream/generate calls (sum of iterations).
    pub llm_ms: u128,
    /// Wall time spent executing tools (sum of concurrent batch wall times).
    pub tool_ms: u128,
    pub(crate) assistant_message_decisions: Vec<AssistantMessageDecision>,
}

impl ModelStreamResult {
    /// Wait/overhead = total - llm - tool (clamped at 0).
    pub fn wait_ms(&self, total_elapsed_ms: u128) -> u128 {
        total_elapsed_ms.saturating_sub(self.llm_ms.saturating_add(self.tool_ms))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AssistantMessageDecision {
    pub item_id: String,
    pub text: String,
    pub decision: FollowUpDecision,
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
        events.push(ModelStreamEvent::AssistantMessageCompleted {
            item_id,
            turn_status: response.turn_status,
        });
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

pub trait LoopToolRuntime: Send + Sync {
    fn set_turn_context(&mut self, _context: ToolRuntimeTurnContext) {}

    fn execute<'a>(
        &'a self,
        call: ToolCallRequest,
    ) -> Pin<Box<dyn Future<Output = ToolResultItem> + Send + 'a>>;
}

#[derive(Debug, Clone)]
pub struct ToolRuntimeTurnContext {
    pub run_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub role: String,
    pub phase: Option<i64>,
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
            let started = Instant::now();
            let req_messages = input_to_debug_messages(&input);
            let prompt = model_prompt(&input)?;
            let text = crate::run_model_text_once(&self.settings, &input, &prompt).await?;
            if self.settings.debug {
                let elapsed_ms = started.elapsed().as_millis();
                crate::append_debug_llm_record(
                    &self.settings,
                    json!({
                        "kind": "generate",
                        "role": self.settings.role,
                        "phase": self.settings.phase,
                        "topic_id": self.settings.topic_id,
                        "model": self.settings.llm.model,
                        "req": { "messages": req_messages },
                        "resp": {
                            "status": "completed",
                            "output": [{"type": "output_text", "text": &text}],
                        },
                        "elapsed_ms": elapsed_ms,
                        "token": null,
                        "response_text": text,
                    }),
                )?;
            }
            Ok(model_response_from_assistant_text(&text))
        })
    }

    fn stream_events<'a>(
        &'a mut self,
        input: ModelInput,
        handler: &'a mut dyn ModelEventHandler,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
        Box::pin(async move {
            let prompt = model_prompt(&input)?;
            let mut capture = DebugLlmCapture::new(handler, &input);
            let result =
                crate::run_model_event_stream(&self.settings, &input, &prompt, &mut capture).await;
            if self.settings.debug {
                crate::append_debug_llm_record(
                    &self.settings,
                    capture.into_record(&self.settings),
                )?;
            }
            result
        })
    }
}

struct DebugLlmCapture<'a> {
    inner: &'a mut dyn ModelEventHandler,
    req_messages: Vec<Value>,
    assistant_text: String,
    tool_calls: Vec<Value>,
    raw: Value,
    end_turn: Option<bool>,
    started: Instant,
}

impl<'a> DebugLlmCapture<'a> {
    fn new(inner: &'a mut dyn ModelEventHandler, input: &ModelInput) -> Self {
        Self {
            inner,
            req_messages: input_to_debug_messages(input),
            assistant_text: String::new(),
            tool_calls: Vec::new(),
            raw: Value::Null,
            end_turn: None,
            started: Instant::now(),
        }
    }

    fn into_record(self, settings: &RigSettings) -> Value {
        let end_turn = if self.tool_calls.is_empty() {
            self.end_turn
        } else {
            Some(false)
        };
        let elapsed_ms = self.started.elapsed().as_millis();
        let usage = extract_token_usage(&self.raw);
        json!({
            "kind": "stream",
            "role": settings.role,
            "phase": settings.phase,
            "topic_id": settings.topic_id,
            "model": settings.llm.model,
            "req": { "messages": self.req_messages },
            "resp": self.raw,
            "elapsed_ms": elapsed_ms,
            "token": {
                "input_tokens": usage.input_tokens,
                "output_tokens": usage.output_tokens,
                "cached_tokens": usage.cached_tokens,
                "reasoning_tokens": usage.reasoning_tokens,
                "total_tokens": usage.total_tokens,
            },
            "response_text": self.assistant_text,
            "tool_calls": self.tool_calls,
            "end_turn": end_turn,
        })
    }
}

pub fn input_to_debug_messages(input: &ModelInput) -> Vec<Value> {
    let mut messages = Vec::new();
    if let Some(system) = &input.system_instruction {
        messages.push(json!({"role": "system", "content": system}));
    }
    for item in &input.items {
        if let Some(msg) = turn_item_to_debug_message(item) {
            messages.push(msg);
        }
    }
    messages
}

fn turn_item_to_debug_message(item: &TurnItem) -> Option<Value> {
    match item.item_type {
        TurnItemType::UserMessage | TurnItemType::CompactSummary => Some(json!({
            "role": if item.item_type == TurnItemType::CompactSummary { "system" } else { "user" },
            "content": item.content_text,
        })),
        TurnItemType::AssistantMessage => Some(json!({
            "role": "assistant",
            "content": item.content_text,
        })),
        TurnItemType::ToolCall => {
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
        TurnItemType::ToolResult => Some(json!({
            "role": "tool",
            "tool_call_id": item.tool_call_id,
            "content": item.content_text,
        })),
        _ => None,
    }
}

impl ModelEventHandler for DebugLlmCapture<'_> {
    fn handle<'a>(
        &'a mut self,
        event: ModelStreamEvent,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
        Box::pin(async move {
            match &event {
                ModelStreamEvent::AssistantTextDelta { delta, .. } => {
                    self.assistant_text.push_str(delta);
                }
                ModelStreamEvent::ToolCallCompleted { tool_call } => {
                    self.tool_calls.push(json!({
                        "call_id": tool_call.call_id,
                        "name": tool_call.name,
                        "arguments": tool_call.arguments,
                    }));
                }
                ModelStreamEvent::ResponseCompleted { end_turn, raw } => {
                    self.end_turn = Some(*end_turn);
                    self.raw = raw.clone();
                }
                _ => {}
            }
            self.inner.handle(event).await
        })
    }
}

#[derive(Debug, Clone)]
pub struct AgentLoopConfig {
    pub max_agent_loops: Option<usize>,
    pub history_limit: usize,
    pub compact_after_items: usize,
    pub max_context_tokens: Option<usize>,
    pub compact_at_token_ratio: f64,
    pub truncation: TruncationConfig,
    pub judge: JudgeConfig,
    pub judge_endpoint: Option<String>,
    pub judge_api_key: Option<String>,
    /// When true, write per-iteration timing/token rows under outputs/debug/.
    pub debug: bool,
    pub project_root: Option<PathBuf>,
    pub role: String,
    pub phase: Option<i64>,
    pub model: String,
    pub topic_id: Option<String>,
}

impl Default for AgentLoopConfig {
    fn default() -> Self {
        Self {
            max_agent_loops: Some(DEFAULT_MAX_AGENT_LOOPS),
            history_limit: 200,
            compact_after_items: 120,
            max_context_tokens: Some(orchestrator_core::token::MAX_PROMPT_TOKENS),
            compact_at_token_ratio: 0.8,
            truncation: TruncationConfig::default(),
            judge: JudgeConfig::default(),
            judge_endpoint: None,
            judge_api_key: None,
            debug: false,
            project_root: None,
            role: String::new(),
            phase: None,
            model: String::new(),
            topic_id: None,
        }
    }
}

pub async fn run_turn<M, T>(
    conn: &rusqlite::Connection,
    turn: &mut Turn,
    model: &mut M,
    tools: &mut T,
    config: AgentLoopConfig,
) -> Result<ModelStreamResult>
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
) -> Result<ModelStreamResult>
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
        max_context_tokens = config.max_context_tokens,
        compact_at_token_ratio = config.compact_at_token_ratio,
        truncation_strategy = ?config.truncation.strategy,
        "agent loop starting"
    );
    persist_turn(conn, turn, &config.truncation)?;
    tools.set_turn_context(ToolRuntimeTurnContext {
        run_id: turn.run_id.clone(),
        session_id: turn.session_id.clone(),
        turn_id: turn.turn_id.clone(),
        role: turn.role.clone(),
        phase: turn.phase,
    });
    if !turn.user_input.trim().is_empty() {
        turn.emitted_items
            .push(TurnItem::user(turn.user_input.clone()));
    }
    // Preload role default evidence before the first LLM hop (jin10/technical/compose).
    if !turn.tools_disabled {
        if let Some(kind) = preseed_context_kind(&turn.role) {
            let already = turn
                .emitted_items
                .iter()
                .any(|item| item.item_type == TurnItemType::ToolResult);
            if !already {
                let call = ToolCallRequest {
                    call_id: "preseed-read_run_context".to_string(),
                    name: "read_run_context".to_string(),
                    arguments: json!({ "kind": kind }),
                };
                turn.emitted_items.push(TurnItem::tool_call(&call));
                let result = tools.execute(call).await;
                turn.emitted_items
                    .push(TurnItem::tool_result(&result, &config.truncation));
                persist_turn(conn, turn, &config.truncation)?;
            }
        }
    }
    let mut first_iteration = true;
    let max_loops = config.max_agent_loops.map(|value| value.max(1));
    let mut loop_index = 0usize;
    let mut aggregate_result = ModelStreamResult::default();
    let mut judge_call_count = 0usize;
    loop {
        if let Some(max_loops) = max_loops {
            if loop_index >= max_loops {
                turn.end_reason = Some("max_loops".to_string());
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
        let llm_started = Instant::now();
        let mut stream_handler =
            ModelStreamHandler::new(conn, turn, sink, config.truncation.clone());
        model.stream_events(input, &mut stream_handler).await?;
        let mut stream_result = stream_handler.finish().await?;
        apply_judge_to_stream_result(turn, &config, &mut stream_result, &mut judge_call_count)
            .await?;
        let llm_elapsed_ms = llm_started.elapsed().as_millis();
        debug!(
            turn_id = turn.turn_id,
            role = turn.role,
            loop_index,
            tool_calls = stream_result.tool_calls.len(),
            needs_follow_up = stream_result.needs_follow_up,
            last_assistant_message_id = stream_result.last_assistant_message_id,
            input_tokens = stream_result.usage.input_tokens,
            output_tokens = stream_result.usage.output_tokens,
            cached_tokens = stream_result.usage.cached_tokens,
            reasoning_tokens = stream_result.usage.reasoning_tokens,
            total_tokens = stream_result.usage.total_tokens,
            elapsed_ms = llm_elapsed_ms,
            "agent loop model iteration completed"
        );
        if config.debug {
            log_debug_llm_iteration(&config, turn, loop_index, llm_elapsed_ms, &stream_result);
        }
        aggregate_result.usage += stream_result.usage;
        aggregate_result.turn_count += stream_result.turn_count;
        aggregate_result.tool_call_count += stream_result.tool_call_count;
        aggregate_result.llm_ms = aggregate_result.llm_ms.saturating_add(llm_elapsed_ms);
        aggregate_result
            .tool_calls
            .extend(stream_result.tool_calls.iter().cloned());
        aggregate_result.needs_follow_up = stream_result.needs_follow_up;

        if !turn.pending_tool_calls.is_empty() {
            let calls = std::mem::take(&mut turn.pending_tool_calls);

            // Emit "running" status for all tools (sequentially, fast)
            for call in &calls {
                emit_tool_call_status(turn, sink, call, AgentItemStatus::Running).await?;
            }

            // Execute all tools concurrently
            let debug_metrics = config.debug;
            let debug_root = config.project_root.clone();
            let debug_role = turn.role.clone();
            let debug_phase = turn.phase;
            let debug_topic = config.topic_id.clone();
            let debug_loop = loop_index;
            let tool_batch_started = Instant::now();
            let futures: Vec<_> = calls
                .into_iter()
                .map(|call| async {
                    let call_id = call.call_id.clone();
                    let name = call.name.clone();
                    debug!(
                        call_id = call_id,
                        tool = name,
                        "agent loop tool call starting"
                    );
                    let tool_started = Instant::now();
                    let result = tools.execute(call).await;
                    let tool_elapsed_ms = tool_started.elapsed().as_millis();
                    if debug_metrics {
                        if let Some(root) = debug_root.as_ref() {
                            crate::debug_log_time(
                                root,
                                json!({
                                    "kind": "tool",
                                    "name": result.name,
                                    "role": debug_role,
                                    "phase": debug_phase,
                                    "topic_id": debug_topic,
                                    "loop_index": debug_loop,
                                    "call_id": result.call_id,
                                    "status": result.status,
                                    "elapsed_ms": tool_elapsed_ms,
                                    "llm_ms": 0,
                                    "tool_ms": tool_elapsed_ms,
                                    "wait_ms": 0,
                                }),
                            );
                        }
                    }
                    debug!(
                        call_id = result.call_id,
                        tool = result.name,
                        status = result.status,
                        error = result.error,
                        elapsed_ms = tool_elapsed_ms,
                        "agent loop tool call completed"
                    );
                    (result, call_id, name)
                })
                .collect();

            let results = futures::future::join_all(futures).await;
            // Concurrent tools share wall time; charge the batch duration once.
            let tool_batch_ms = tool_batch_started.elapsed().as_millis();
            aggregate_result.tool_ms = aggregate_result.tool_ms.saturating_add(tool_batch_ms);
            let stop_rereading = results.iter().any(|(result, _, _)| {
                result.output.get("status").and_then(Value::as_str) == Some("stop_rereading")
            });
            let analyst_evidence_ready = turn.role.starts_with("analyst.")
                && results.iter().any(|(result, _, name)| {
                    name == "read_run_context"
                        && matches!(
                            result.output.get("status").and_then(Value::as_str),
                            Some("ok") | Some("stop_rereading")
                        )
                });

            // Emit results and append to DB (sequentially, in completion order)
            for (result, _call_id, _name) in results {
                emit_tool_result(turn, sink, &result).await?;
                turn.emitted_items
                    .push(TurnItem::tool_result(&result, &config.truncation));
            }
            persist_turn(conn, turn, &config.truncation)?;
            // Detect stop_rereading / enough evidence and force artifact emission.
            // Analysts only need one successful context read; waiting for loop_index>=3
            // left live runs hanging on long tool-enabled streams.
            if stop_rereading || analyst_evidence_ready || loop_index >= 3 {
                turn.tools_disabled = true;
                let hint = if turn.role.starts_with("analyst.") {
                    "Emit one JSON object with id/role for this analyst, status=completed, and per_ticker.<TICKER>.{direction,confidence,report}. direction must be bullish|bearish|neutral|mixed|unobserved; confidence must be a 0..1 number."
                } else if turn.role.contains("bear.initial") {
                    "Emit bear_seed_packet JSON now: role=researcher.bear.initial, artifact_type=bear_seed_packet, topic_id, claims[], summary, reducer_checks. Do not call tools again."
                } else if turn.role.contains("bull.initial") {
                    "Emit bull_seed_packet JSON now: role=researcher.bull.initial, artifact_type=bull_seed_packet, topic_id, claims[], summary, reducer_checks. Do not call tools again."
                } else if turn.role.contains("interaction") {
                    "Emit the debate packet JSON required by the role prompt now. Do not call tools again."
                } else if turn.role.starts_with("mediator.") {
                    "Emit the mediator JSON artifact required by the role prompt now. Do not call tools again."
                } else {
                    "Emit the final JSON artifact required by the role prompt now. Tools are disabled for this turn."
                };
                turn.push_pending_input(format!(
                    "Tool evidence is already available. Tools are now disabled. {hint}"
                ));
            }
            turn.needs_follow_up = true;
            persist_turn(conn, turn, &config.truncation)?;
            continue;
        }

        if !turn.pending_input.is_empty() {
            turn.needs_follow_up = true;
            persist_turn(conn, turn, &config.truncation)?;
            continue;
        }

        if turn.needs_follow_up {
            turn.needs_follow_up = false;
            persist_turn(conn, turn, &config.truncation)?;
            continue;
        }

        if let Some(text) = last_assistant_message_text(turn) {
            if turn.role.starts_with("analyst.")
                && !analyst_final_artifact_looks_valid(&turn.role, &turn_tickers(turn), &text)
            {
                turn.tools_disabled = true;
                turn.push_pending_input(
                    "Your last JSON is invalid for this analyst role. Emit one JSON object with id/role for this analyst, status=completed, and per_ticker.<TICKER>.{direction,confidence,report}. direction must be bullish|bearish|neutral|mixed|unobserved; confidence must be a 0..1 number. Do not invent alternate schemas (no stance-only payloads).",
                );
                turn.needs_follow_up = true;
                persist_turn(conn, turn, &config.truncation)?;
                continue;
            }
            if seed_packet_role(&turn.role) && !seed_packet_looks_valid(&turn.role, &text) {
                turn.tools_disabled = true;
                turn.push_pending_input(
                    "Your last JSON is invalid for this seed role. Emit the required seed packet JSON with artifact_type, topic_id, claims[], summary, and reducer_checks. Do not call tools again.",
                );
                turn.needs_follow_up = true;
                persist_turn(conn, turn, &config.truncation)?;
                continue;
            }
            if turn.role == "manager.research"
                && !research_artifact_looks_valid(&turn_tickers(turn), &text)
            {
                turn.tools_disabled = true;
                turn.push_pending_input(
                    "Your last output is invalid for manager.research. Emit the ResearchArtifact required by the current role schema. Use a valid confidence_basis and, for Hold, the matching hold_reason. If probabilities live under per_ticker, also copy the primary ticker's rating/probabilities to the top level. Do not call tools again.",
                );
                turn.needs_follow_up = true;
                persist_turn(conn, turn, &config.truncation)?;
                continue;
            }
            if turn.role == "trader" && !trade_intent_looks_valid(&text) {
                turn.tools_disabled = true;
                turn.push_pending_input(
                    "Your last output is invalid for trader. Emit the TradeIntent required by the current role schema. Hold must use position_size=0%. Do not call tools again.",
                );
                turn.needs_follow_up = true;
                persist_turn(conn, turn, &config.truncation)?;
                continue;
            }
            if turn.role.starts_with("risk.") && !risk_constraints_look_valid(&turn.role, &text) {
                turn.tools_disabled = true;
                turn.push_pending_input(
                    "Your last output is invalid for this risk role. Emit the RiskConstraints required by the current role schema, including stance, unique_risk_contribution (or no_new_information=true), disagreement_with_prior, numeric caps, stop policy, triggers, review window, hedge recommendation, and confidence. Do not call tools again.",
                );
                turn.needs_follow_up = true;
                persist_turn(conn, turn, &config.truncation)?;
                continue;
            }
            if interaction_packet_role(&turn.role)
                && !interaction_packet_looks_valid(&turn.role, &text)
            {
                turn.tools_disabled = true;
                turn.push_pending_input(
                    "Your last JSON is invalid for this interaction role. Emit the exact role-specific debate packet with role, artifact_type, topic_id, reply_to, stance, claim, evidence_refs, confidence, send_to_mediator, blocked_ack, and steelman unless stance=no_new_info. Do not call tools again.",
                );
                turn.needs_follow_up = true;
                persist_turn(conn, turn, &config.truncation)?;
                continue;
            }
            if turn.role == "mediator.topic_controller" && !controller_packet_looks_valid(&text) {
                turn.tools_disabled = true;
                turn.push_pending_input(
                    "Your last JSON is invalid for mediator.topic_controller. Emit the topic_controller_packet required by the current role schema, including evidence-backed decision_hinges, agreed_facts, info_gain_score, and soft_control. Do not call tools again.",
                );
                turn.needs_follow_up = true;
                persist_turn(conn, turn, &config.truncation)?;
                continue;
            }
            if turn.role == "allocation.manager" && !allocation_artifact_looks_valid(&text) {
                turn.tools_disabled = true;
                turn.push_pending_input(
                    "Your last JSON is invalid for allocation.manager. You MUST include ALL of these top-level fields: weights (object with numeric values summing to 1.0), total_equity_exposure (number 0-1 = sum of non-cash weights), vix_regime (string: risk_on/normal/elevated/defensive), correlation_note (string), summary (string). Do not wrap in id/role/status/report. Do not call tools again.",
                );
                turn.needs_follow_up = true;
                persist_turn(conn, turn, &config.truncation)?;
                continue;
            }
        }

        if let Some(item_id) = stream_result.last_assistant_message_id.clone() {
            aggregate_result.last_assistant_message_id = Some(item_id.clone());
            mark_last_assistant_message_as_final(conn, turn, &item_id, sink, &config.truncation)
                .await?;
        }
        turn.end_reason = Some("completed".to_string());
        debug!(
            turn_id = turn.turn_id,
            role = turn.role,
            loop_index,
            input_tokens = aggregate_result.usage.input_tokens,
            output_tokens = aggregate_result.usage.output_tokens,
            cached_tokens = aggregate_result.usage.cached_tokens,
            reasoning_tokens = aggregate_result.usage.reasoning_tokens,
            total_tokens = aggregate_result.usage.total_tokens,
            turn_count = aggregate_result.turn_count,
            tool_call_count = aggregate_result.tool_call_count,
            "agent loop completed"
        );
        persist_turn(conn, turn, &config.truncation)?;
        return Ok(aggregate_result);
    }
}

async fn apply_judge_to_stream_result(
    turn: &mut Turn,
    config: &AgentLoopConfig,
    stream_result: &mut ModelStreamResult,
    judge_call_count: &mut usize,
) -> Result<()> {
    for item in &mut stream_result.assistant_message_decisions {
        if !matches!(item.decision, FollowUpDecision::Ambiguous) {
            continue;
        }
        if !config.judge.enabled {
            debug!(
                turn_id = turn.turn_id,
                item_id = item.item_id,
                "LLM judge disabled, defaulting ambiguous assistant message to Final"
            );
            item.decision = FollowUpDecision::Final;
            continue;
        }
        if *judge_call_count >= config.judge.max_messages_per_turn {
            warn!(
                turn_id = turn.turn_id,
                item_id = item.item_id,
                count = *judge_call_count,
                max = config.judge.max_messages_per_turn,
                "LLM judge call limit reached, defaulting to Final"
            );
            item.decision = FollowUpDecision::Final;
            continue;
        }
        let Some(endpoint) = config
            .judge_endpoint
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            warn!(
                turn_id = turn.turn_id,
                item_id = item.item_id,
                "LLM judge endpoint missing, defaulting to Final"
            );
            item.decision = FollowUpDecision::Final;
            continue;
        };
        let Some(api_key) = config
            .judge_api_key
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            warn!(
                turn_id = turn.turn_id,
                item_id = item.item_id,
                "LLM judge API key missing, defaulting to Final"
            );
            item.decision = FollowUpDecision::Final;
            continue;
        };
        *judge_call_count += 1;
        match judge_message_status(&item.text, endpoint, api_key, &config.judge.model).await {
            Ok(true) => {
                debug!(
                    turn_id = turn.turn_id,
                    item_id = item.item_id,
                    "LLM judge classified assistant message as stall"
                );
                item.decision = FollowUpDecision::NeedsFollowUp;
            }
            Ok(false) => {
                debug!(
                    turn_id = turn.turn_id,
                    item_id = item.item_id,
                    "LLM judge classified assistant message as final"
                );
                item.decision = FollowUpDecision::Final;
            }
            Err(error) => {
                warn!(
                    turn_id = turn.turn_id,
                    item_id = item.item_id,
                    error = %error,
                    "LLM judge failed, defaulting to Final"
                );
                item.decision = FollowUpDecision::Final;
            }
        }
    }
    stream_result.needs_follow_up = stream_result.needs_follow_up
        || stream_result
            .assistant_message_decisions
            .iter()
            .any(|item| matches!(item.decision, FollowUpDecision::NeedsFollowUp));
    turn.needs_follow_up = stream_result.needs_follow_up;
    Ok(())
}

fn truncate_tool_result(content: &str, truncation: &TruncationConfig) -> String {
    truncate_semantic(content, truncation.tool_result_chars, truncation)
}

fn truncate_context_fragment(content: &str, truncation: &TruncationConfig) -> String {
    truncate_semantic(content, truncation.context_fragment_chars, truncation)
}

/// Shrink oversized tool outputs before they are stored in turn history / prompts.
fn compact_tool_output_for_history(
    output: &Value,
    truncated_text: &str,
    truncation: &TruncationConfig,
) -> Value {
    let encoded = output.to_string();
    if encoded.chars().count() <= truncation.tool_result_chars {
        return output.clone();
    }
    // Prefer structured preview when the original payload was JSON-like.
    if let Ok(parsed) = serde_json::from_str::<Value>(truncated_text) {
        return parsed;
    }
    json!({
        "truncated": true,
        "preview": truncated_text,
    })
}

fn persist_turn(
    conn: &rusqlite::Connection,
    turn: &Turn,
    truncation: &TruncationConfig,
) -> Result<()> {
    let turn_number: i64 = conn
        .query_row(
            "SELECT COALESCE(turn_number, 0) FROM agent_events WHERE turn_id = ?",
            rusqlite::params![turn.turn_id],
            |row| row.get(0),
        )
        .unwrap_or_else(|_| {
            conn.query_row(
                "SELECT COALESCE(MAX(turn_number), 0) + 1 FROM agent_events WHERE run_id = ?",
                rusqlite::params![turn.run_id],
                |row| row.get(0),
            )
            .unwrap_or(1)
        });
    let full_context: Vec<Value> = turn
        .emitted_items
        .iter()
        .map(|item| {
            json!({
                "event_type": item.item_type.as_str(),
                "role": item.role,
                "content_text": item.content_text,
                "content_json": item.content_json,
                "tool_call_id": item.tool_call_id,
                "tool_name": item.tool_name,
            })
        })
        .collect();
    let summary = if let Some(last) = turn.emitted_items.last() {
        truncate_context_fragment(&last.content_text, truncation)
    } else {
        truncate_context_fragment(&turn.user_input, truncation)
    };
    upsert_agent_turn(
        conn,
        &AgentTurnInput {
            turn_id: turn.turn_id.clone(),
            run_id: turn.run_id.clone(),
            phase: turn.phase,
            turn_number,
            role: turn.role.clone(),
            full_context_json: json!(full_context),
            summary,
        },
    )
}

fn update_turn_item(
    _conn: &rusqlite::Connection,
    turn: &mut Turn,
    output_item_id: &str,
    content_text: String,
    phase: Option<AgentItemPhase>,
    status: AgentItemStatus,
    _truncation: &TruncationConfig,
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
    _first_iteration: bool,
    config: &AgentLoopConfig,
) -> Result<ModelInput> {
    let mut items = history_items(conn, turn, config.history_limit)?;
    let role_prompt =
        (!turn.user_input.trim().is_empty()).then(|| TurnItem::user(turn.user_input.clone()));
    if let Some(role_prompt) = &role_prompt {
        items.retain(|item| {
            item.item_type != TurnItemType::UserMessage
                || item.content_text != role_prompt.content_text
        });
        items.insert(0, role_prompt.clone());
    }
    while let Some(input) = turn.pending_input.pop_front() {
        let item = TurnItem::user(format!("Steer: {input}"));
        turn.emitted_items.push(item.clone());
        items.push(item);
    }
    let latest_reasoning_state = items
        .iter()
        .rev()
        .find(|item| item.item_type == TurnItemType::ReasoningState)
        .cloned();
    // Budget the dynamic suffix independently so a large, cacheable role
    // prompt cannot evict fresh tool evidence on the next loop iteration.
    let total_tokens = estimate_items_tokens(&items[usize::from(role_prompt.is_some())..]);
    let token_threshold = config
        .max_context_tokens
        .map(|max_tokens| token_compaction_threshold(max_tokens, config.compact_at_token_ratio))
        .unwrap_or(usize::MAX);
    let needs_token_compaction = total_tokens > token_threshold;
    let needs_item_compaction = items.len() > config.compact_after_items;
    if needs_token_compaction || needs_item_compaction {
        let trigger = if needs_token_compaction {
            "token_threshold"
        } else {
            "item_count"
        };
        let items_before = items.len();
        debug!(
            turn_id = turn.turn_id,
            role = turn.role,
            items_count = items_before,
            estimated_tokens = total_tokens,
            token_threshold,
            compact_after_items = config.compact_after_items,
            trigger,
            "compaction triggered"
        );
        let summary = compact_summary_card(&items);
        let item = TurnItem {
            item_type: TurnItemType::CompactSummary,
            role: "system".to_string(),
            content_text: summary.clone(),
            content_json: json!({
                "summary": summary,
                "compaction_trigger": trigger,
                "items_compacted": items_before,
                "estimated_tokens_before": total_tokens,
                "token_threshold": token_threshold,
            }),
            tool_call_id: String::new(),
            tool_name: String::new(),
            output_item_id: String::new(),
            phase: None,
            status: None,
            db_row_id: None,
        };
        turn.emitted_items.push(item.clone());
        // Keep the original role prompt + a capped slice of latest tool evidence.
        // Previously we kept two full tool results, which often made tokens_after
        // larger than tokens_before and defeated token-threshold compaction.
        let evidence_char_cap = if needs_token_compaction {
            8_000
        } else {
            10_000
        };
        let recent_tool_results: Vec<TurnItem> = items
            .iter()
            .rev()
            .filter(|item| item.item_type == TurnItemType::ToolResult)
            .take(if needs_token_compaction { 1 } else { 2 })
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .map(|mut tool_item| {
                if tool_item.content_text.chars().count() > evidence_char_cap {
                    tool_item.content_text =
                        truncate_chars(&tool_item.content_text, evidence_char_cap);
                    tool_item.content_json = json!({
                        "truncated": true,
                        "preview": tool_item.content_text.clone(),
                        "tool_name": tool_item.tool_name.clone(),
                    });
                }
                tool_item
            })
            .collect();
        items = Vec::new();
        if let Some(role_prompt) = role_prompt.clone() {
            items.push(role_prompt);
        }
        items.push(item);
        items.extend(recent_tool_results);
        if let Some(reasoning_state) = latest_reasoning_state.clone() {
            items.push(reasoning_state);
        }
        debug!(
            turn_id = turn.turn_id,
            role = turn.role,
            trigger,
            items_before,
            items_after = items.len(),
            tokens_before = total_tokens,
            tokens_after = estimate_items_tokens(&items),
            "compaction completed"
        );
    }
    if let Some(max_tokens) = config.max_context_tokens {
        let pinned_role_prompt = role_prompt.clone();
        let mut kept: Vec<TurnItem> = Vec::new();
        let mut total_tokens = 0usize;
        for item in items
            .iter()
            .filter(|item| {
                pinned_role_prompt
                    .as_ref()
                    .is_none_or(|prompt| item.content_text != prompt.content_text)
            })
            .rev()
        {
            if item.item_type == TurnItemType::ReasoningState {
                kept.push(item.clone());
                continue;
            }
            let tokens = estimate_turn_item_tokens(item);
            if total_tokens + tokens <= max_tokens || kept.is_empty() {
                total_tokens += tokens;
                kept.push(item.clone());
            }
        }
        kept.reverse();
        if let Some(role_prompt) = pinned_role_prompt {
            kept.insert(0, role_prompt);
        }
        items = kept;
    }
    let tools = if turn.tools_disabled {
        Vec::new()
    } else {
        turn_available_tools(turn)
    };
    Ok(ModelInput {
        items,
        available_tools: tools.clone(),
        system_instruction: Some(model_system_instruction(
            &tools,
            &turn.role,
            &turn_tickers(turn),
        )),
        truncation: config.truncation.clone(),
    })
}

fn turn_tickers(turn: &Turn) -> Vec<String> {
    turn.model_context
        .lines()
        .find_map(|line| {
            line.split(", ")
                .find_map(|field| field.strip_prefix("tickers="))
        })
        .map(|tickers| {
            tickers
                .split(',')
                .map(str::trim)
                .filter(|ticker| !ticker.is_empty())
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn estimate_turn_item_tokens(item: &TurnItem) -> usize {
    orchestrator_core::token::estimate_turn_item_tokens(
        item.item_type.as_str(),
        &item.role,
        &item.content_text,
        &item.content_json,
    )
}

fn estimate_items_tokens(items: &[TurnItem]) -> usize {
    items.iter().map(estimate_turn_item_tokens).sum()
}

fn token_compaction_threshold(max_tokens: usize, ratio: f64) -> usize {
    if !ratio.is_finite() || ratio <= 0.0 {
        return max_tokens;
    }
    ((max_tokens as f64) * ratio).floor().max(1.0) as usize
}

fn turn_available_tools(turn: &Turn) -> Vec<String> {
    turn.model_context
        .lines()
        .find_map(|line| line.strip_prefix("available_tools="))
        .and_then(|value| serde_json::from_str::<Vec<String>>(value).ok())
        .unwrap_or_default()
}

fn history_items(conn: &rusqlite::Connection, turn: &Turn, limit: usize) -> Result<Vec<TurnItem>> {
    // Prefer in-memory emitted items for the active loop iteration.
    //
    // Loading "latest full_context_json for this run_id" is wrong when multiple
    // roles share a run: parallel phase-1 jobs each own a distinct turn_id, so a
    // later-persisted sibling role would replace this role's tool evidence and
    // cause analysts to claim "no technical/Jin10 data" despite successful
    // tool calls (live F1 regression).
    let items = if !turn.emitted_items.is_empty() {
        turn.emitted_items.clone()
    } else {
        // Resume path for multi-round steer sessions that recreate a Turn with
        // the same turn_id: reload only this turn's snapshot.
        turn_history_items(conn, &turn.turn_id)?
            .into_iter()
            .map(turn_item_from_history_value)
            .collect()
    };
    if limit == 0 || items.len() <= limit {
        return Ok(items);
    }
    Ok(items[items.len() - limit..].to_vec())
}

/// Convert a persisted agent-event history value into a runtime turn item.
pub fn turn_item_from_history_value(value: Value) -> TurnItem {
    let item_type = match value
        .get("event_type")
        .and_then(Value::as_str)
        .unwrap_or("")
    {
        "user_message" => TurnItemType::UserMessage,
        "assistant_message" => TurnItemType::AssistantMessage,
        "reasoning_summary" => TurnItemType::ReasoningSummary,
        "reasoning_state" => TurnItemType::ReasoningState,
        "plan_update" => TurnItemType::PlanUpdate,
        "tool_call" => TurnItemType::ToolCall,
        "tool_result" => TurnItemType::ToolResult,
        "system_context" => TurnItemType::SystemContext,
        "developer_context" => TurnItemType::DeveloperContext,
        "compact_summary" => TurnItemType::CompactSummary,
        _ => TurnItemType::InjectedContext,
    };
    let content_json = value.get("content_json").cloned().unwrap_or(Value::Null);
    TurnItem {
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
        content_json: content_json.clone(),
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
        output_item_id: content_json
            .get("output_item_id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        phase: content_json
            .get("phase")
            .and_then(Value::as_str)
            .and_then(|value| match value {
                "commentary" => Some(AgentItemPhase::Commentary),
                "final" => Some(AgentItemPhase::Final),
                _ => None,
            }),
        status: content_json
            .get("status")
            .and_then(Value::as_str)
            .and_then(|value| match value {
                "in_progress" => Some(AgentItemStatus::InProgress),
                "completed" => Some(AgentItemStatus::Completed),
                "pending" => Some(AgentItemStatus::Pending),
                "running" => Some(AgentItemStatus::Running),
                "failed" => Some(AgentItemStatus::Failed),
                "interrupted" => Some(AgentItemStatus::Interrupted),
                _ => None,
            }),
        db_row_id: None,
    }
}

struct ModelStreamHandler<'a, S: AgentEventSink> {
    conn: &'a rusqlite::Connection,
    turn: &'a mut Turn,
    sink: &'a mut S,
    result: ModelStreamResult,
    assistant_buffers: BTreeMap<String, String>,
    reasoning_buffers: BTreeMap<String, String>,
    truncation: TruncationConfig,
}

impl<'a, S: AgentEventSink> ModelStreamHandler<'a, S> {
    fn new(
        conn: &'a rusqlite::Connection,
        turn: &'a mut Turn,
        sink: &'a mut S,
        truncation: TruncationConfig,
    ) -> Self {
        Self {
            conn,
            turn,
            sink,
            result: ModelStreamResult::default(),
            assistant_buffers: BTreeMap::new(),
            reasoning_buffers: BTreeMap::new(),
            truncation,
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
                self.turn.emitted_items.push(item);
                emit_started(
                    self.turn,
                    self.sink,
                    self.turn.emitted_items.last().expect("just appended"),
                )
                .await?;
                self.assistant_buffers
                    .insert(item_id.clone(), String::new());
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
                    &self.truncation,
                )?;
                emit_delta(self.turn, self.sink, &item_id, &delta).await?;
            }
            ModelStreamEvent::AssistantMessageCompleted {
                item_id,
                turn_status,
            } => {
                let text = self.assistant_buffers.remove(&item_id).unwrap_or_default();
                debug!(
                    turn_id = self.turn.turn_id,
                    item_id,
                    text_chars = text.len(),
                    turn_status = ?turn_status,
                    "model stream assistant message completed"
                );
                let decision = classify_assistant_message(&text, turn_status);
                let needs_follow_up = matches!(decision, FollowUpDecision::NeedsFollowUp);
                if needs_follow_up {
                    self.result.needs_follow_up = true;
                }
                self.result
                    .assistant_message_decisions
                    .push(AssistantMessageDecision {
                        item_id: item_id.clone(),
                        text: text.clone(),
                        decision,
                    });
                if let Some(item) = update_turn_item(
                    self.conn,
                    self.turn,
                    &item_id,
                    text,
                    Some(AgentItemPhase::Commentary),
                    AgentItemStatus::Completed,
                    &self.truncation,
                )? {
                    emit_completed(self.turn, self.sink, &item).await?;
                }
                self.result.last_assistant_message_id = Some(item_id);
            }
            ModelStreamEvent::ReasoningSummaryDelta { item_id, delta } => {
                if !self.reasoning_buffers.contains_key(&item_id) {
                    let item = started_reasoning_item(&item_id);
                    self.turn.emitted_items.push(item);
                    emit_started(
                        self.turn,
                        self.sink,
                        self.turn.emitted_items.last().expect("just appended"),
                    )
                    .await?;
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
                    &self.truncation,
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
                    &self.truncation,
                )? {
                    emit_completed(self.turn, self.sink, &item).await?;
                }
            }
            ModelStreamEvent::ReasoningStateCompleted {
                item_id,
                encrypted_content,
            } => {
                debug!(
                    turn_id = self.turn.turn_id,
                    item_id,
                    state_chars = encrypted_content.len(),
                    "model stream reasoning state completed"
                );
                let item = TurnItem {
                    item_type: TurnItemType::ReasoningState,
                    role: "assistant".to_string(),
                    content_text: String::new(),
                    content_json: json!({
                        "output_item_id": item_id,
                        "encrypted_content": encrypted_content,
                        "summary": []
                    }),
                    tool_call_id: String::new(),
                    tool_name: String::new(),
                    output_item_id: item_id,
                    phase: None,
                    status: Some(AgentItemStatus::Completed),
                    db_row_id: None,
                };
                self.turn.emitted_items.push(item);
            }
            ModelStreamEvent::ToolCallCompleted { tool_call } => {
                debug!(
                    turn_id = self.turn.turn_id,
                    call_id = tool_call.call_id,
                    tool = tool_call.name,
                    "model stream tool call requested"
                );
                let item = TurnItem::tool_call(&tool_call);
                self.turn.emitted_items.push(item);
                emit_completed(
                    self.turn,
                    self.sink,
                    self.turn.emitted_items.last().expect("just appended"),
                )
                .await?;
                self.result.needs_follow_up = true;
                self.result.tool_call_count += 1;
                self.result.tool_calls.push(tool_call.clone());
                self.turn.pending_tool_calls.push(tool_call);
            }
            ModelStreamEvent::ResponseCompleted { end_turn, raw } => {
                let token_usage = extract_token_usage(&raw);
                debug!(
                    turn_id = self.turn.turn_id,
                    end_turn,
                    input_tokens = token_usage.input_tokens,
                    output_tokens = token_usage.output_tokens,
                    cached_tokens = token_usage.cached_tokens,
                    reasoning_tokens = token_usage.reasoning_tokens,
                    total_tokens = token_usage.total_tokens,
                    "model stream response completed"
                );
                self.result.usage += token_usage;
                self.result.turn_count += 1;
                if !end_turn {
                    self.result.needs_follow_up = true;
                }
            }
        }
        Ok(())
    }

    async fn finish(mut self) -> Result<ModelStreamResult> {
        self.result.needs_follow_up = self.result.needs_follow_up
            || self
                .result
                .assistant_message_decisions
                .iter()
                .any(|item| matches!(item.decision, FollowUpDecision::NeedsFollowUp))
            || !self.turn.pending_tool_calls.is_empty()
            || !self.turn.pending_input.is_empty();
        self.turn.needs_follow_up = self.result.needs_follow_up;
        persist_turn(self.conn, self.turn, &self.truncation)?;
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
    truncation: &TruncationConfig,
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
        truncation,
    )? {
        emit_completed(turn, sink, &item).await?;
    }
    Ok(())
}

fn preseed_context_kind(role: &str) -> Option<&'static str> {
    match role {
        "analyst.technical" => Some("technical"),
        "analyst.news_macro" => Some("jin10"),
        // Phase-2 topic/debate roles fork Phase 1.5 summary from the prompt
        // (`{phase15_fork}`); do not preseed raw compose_context/jin10/technical.
        _ => None,
    }
}

/// Map plain assistant text (rig stream/text output) into a ModelResponse.
/// Tool calls are expected via native function-calling on the stream path.
pub fn model_response_from_assistant_text(text: &str) -> ModelResponse {
    let trimmed = text.trim();
    // Compatibility: if a legacy ReAct wrapper still appears, unwrap it.
    if let Ok(value) = extract_json_value(trimmed) {
        if value.get("assistant_message").is_some()
            || value.get("tool_calls").is_some()
            || value.get("end_turn").is_some()
        {
            if let Ok(parsed) = parse_legacy_react_response(value) {
                return parsed;
            }
        }
    }
    ModelResponse {
        assistant_message: Some(text.to_string()),
        reasoning_summary: None,
        tool_calls: Vec::new(),
        end_turn: true,
        raw: json!({"source": "rig_text"}),
        turn_status: TurnStatus::Unknown,
    }
}

/// Legacy ReAct JSON wrapper (assistant_message/tool_calls/end_turn). Kept only for
/// compatibility with older fixtures and non-stream generate fallbacks.
pub fn parse_react_response(value: Value) -> Result<ModelResponse> {
    parse_legacy_react_response(value)
}

fn parse_legacy_react_response(value: Value) -> Result<ModelResponse> {
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
            turn_status: TurnStatus::Unknown,
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
    let turn_status = extract_turn_status(&value);
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
    // Tool calls always continue the loop; never honor end_turn=true alongside tools.
    let end_turn = if tool_calls.is_empty() {
        value
            .get("end_turn")
            .and_then(Value::as_bool)
            .unwrap_or(true)
    } else {
        false
    };
    Ok(ModelResponse {
        assistant_message,
        reasoning_summary,
        tool_calls,
        end_turn,
        raw: value,
        turn_status,
    })
}

/// Turn status as reported by the LLM in the assistant_message_completed event.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TurnStatus {
    /// LLM reports this is the final artifact.
    Final,
    /// LLM reports this is an intermediate/stall message.
    Intermediate,
    /// LLM did not report a status (legacy or omitted).
    #[default]
    Unknown,
}

/// Result of stall detection for an assistant message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FollowUpDecision {
    /// The message is a stall; the agent loop should continue.
    NeedsFollowUp,
    /// The message is final; the agent loop should end the turn.
    Final,
    /// The message is ambiguous; run the LLM judge to decide.
    Ambiguous,
}

/// Extract turn_status from the assistant_message_completed event metadata.
/// The event may carry {"turn_status": "final" | "intermediate"} as an extra field.
pub fn extract_turn_status(event: &Value) -> TurnStatus {
    match event
        .get("turn_status")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("final") => TurnStatus::Final,
        Some("intermediate") => TurnStatus::Intermediate,
        _ => TurnStatus::Unknown,
    }
}

/// Fast first-pass stall detection (no LLM call).
/// Uses self-reported status, phrase matching, length check, and JSON detection.
fn last_assistant_message_text(turn: &Turn) -> Option<String> {
    turn.emitted_items
        .iter()
        .rev()
        .find(|item| item.item_type == TurnItemType::AssistantMessage)
        .map(|item| {
            if !item.content_text.trim().is_empty() {
                item.content_text.clone()
            } else {
                item.content_json.to_string()
            }
        })
}

fn seed_packet_role(role: &str) -> bool {
    matches!(role, "researcher.bull.initial" | "researcher.bear.initial")
}

fn seed_packet_looks_valid(role: &str, text: &str) -> bool {
    let Ok(value) = extract_json_value(text) else {
        return false;
    };
    let expected = if role.contains("bear") {
        "bear_seed_packet"
    } else {
        "bull_seed_packet"
    };
    let constraint_field = if role.contains("bear") {
        "known_bull_constraint"
    } else {
        "known_bear_constraint"
    };
    value.get("role").and_then(Value::as_str) == Some(role)
        && value.get("artifact_type").and_then(Value::as_str) == Some(expected)
        && value
            .get("claims")
            .and_then(Value::as_array)
            .is_some_and(|claims| {
                !claims.is_empty()
                    && claims.iter().all(|claim| {
                        non_empty_string_field(claim, "claim_id")
                            && non_empty_string_field(claim, "decision_hinge")
                            && non_empty_string_field(claim, "claim")
                            && claim
                                .get("evidence_refs")
                                .and_then(Value::as_array)
                                .is_some()
                            && claim
                                .get("confidence")
                                .and_then(Value::as_f64)
                                .is_some_and(|confidence| (0.0..=1.0).contains(&confidence))
                            && non_empty_string_field(claim, constraint_field)
                            && claim
                                .get("needs_mediator_check")
                                .and_then(Value::as_bool)
                                .is_some()
                    })
            })
        && value
            .get("topic_id")
            .and_then(Value::as_str)
            .is_some_and(|topic| !topic.trim().is_empty())
        && non_empty_string_field(&value, "summary")
        && value
            .get("reducer_checks")
            .and_then(Value::as_object)
            .is_some()
}

fn interaction_packet_role(role: &str) -> bool {
    role.contains(".interaction")
}

fn interaction_packet_looks_valid(role: &str, text: &str) -> bool {
    let Ok(value) = extract_json_value(text) else {
        return false;
    };
    let expected_type = if role == "researcher.bull.interaction" {
        "bull_debate_packet"
    } else if role == "researcher.bear.interaction" {
        "bear_debate_packet"
    } else {
        return false;
    };
    !assistant_message_needs_follow_up(text)
        && value.get("role").and_then(Value::as_str) == Some(role)
        && value.get("artifact_type").and_then(Value::as_str) == Some(expected_type)
        && non_empty_string_field(&value, "topic_id")
        && non_empty_string_field(&value, "reply_to")
        && value
            .get("stance")
            .and_then(Value::as_str)
            .is_some_and(|stance| {
                matches!(
                    stance,
                    "accept" | "rebut" | "downgrade" | "needs_evidence" | "no_new_info"
                )
            })
        && non_empty_string_field(&value, "claim")
        && value
            .get("evidence_refs")
            .and_then(Value::as_array)
            .is_some()
        && value
            .get("confidence")
            .and_then(Value::as_f64)
            .is_some_and(|confidence| (0.0..=1.0).contains(&confidence))
        && non_empty_string_field(&value, "send_to_mediator")
        && value.get("blocked_ack").and_then(Value::as_array).is_some()
        && (value.get("stance").and_then(Value::as_str) == Some("no_new_info")
            || value.get("steelman").and_then(Value::as_object).is_some())
}

fn controller_packet_looks_valid(text: &str) -> bool {
    let Ok(value) = extract_json_value(text) else {
        return false;
    };
    !assistant_message_needs_follow_up(text)
        && value.get("role").and_then(Value::as_str) == Some("mediator.topic_controller")
        && value.get("artifact_type").and_then(Value::as_str) == Some("topic_controller_packet")
        && non_empty_string_field(&value, "topic_id")
        && [
            "claim_ledger",
            "accepted_for_opponent",
            "rejected_to_origin",
            "blocked_claims",
            "agreed_facts",
            "decision_hinges",
        ]
        .iter()
        .all(|field| value.get(*field).and_then(Value::as_array).is_some())
        && ["next_steers", "topic_summary_delta", "reducer_checks"]
            .iter()
            .all(|field| value.get(*field).and_then(Value::as_object).is_some())
        && value
            .get("decision_hinges")
            .and_then(Value::as_array)
            .is_some_and(|hinges| {
                hinges.iter().all(|hinge| {
                    non_empty_string_field(hinge, "hinge")
                        && hinge
                            .get("evidence_refs")
                            .and_then(Value::as_array)
                            .is_some_and(|refs| !refs.is_empty())
                })
            })
        && value
            .get("info_gain_score")
            .and_then(Value::as_f64)
            .is_some_and(|score| (0.0..=1.0).contains(&score))
        && value
            .get("soft_control")
            .and_then(Value::as_object)
            .is_some_and(|soft_control| {
                soft_control
                    .get("should_continue")
                    .and_then(Value::as_bool)
                    .is_some()
                    && soft_control
                        .get("stop_reason")
                        .and_then(Value::as_str)
                        .is_some_and(|reason| !reason.trim().is_empty())
            })
}

fn allocation_artifact_looks_valid(text: &str) -> bool {
    let Ok(value) = extract_json_value(text) else {
        return false;
    };
    let Some(weights) = value.get("weights").and_then(Value::as_object) else {
        return false;
    };
    if weights.contains_key("VIX")
        || value
            .get("per_ticker")
            .and_then(Value::as_object)
            .and_then(|items| items.get("VIX"))
            .is_some_and(|payload| payload.get("weight").is_some())
    {
        return false;
    }
    let parsed = weights
        .iter()
        .map(|(ticker, entry)| {
            entry
                .as_f64()
                .or_else(|| entry.get("weight").and_then(Value::as_f64))
                .filter(|weight| (0.0..=1.0).contains(weight))
                .map(|weight| (ticker, weight))
        })
        .collect::<Option<Vec<_>>>();
    let Some(parsed) = parsed.filter(|items| !items.is_empty()) else {
        return false;
    };
    let total_weight = parsed.iter().map(|(_, weight)| *weight).sum::<f64>();
    if assistant_message_needs_follow_up(text) || (total_weight - 1.0).abs() > 0.03 {
        return false;
    }
    non_empty_string_field(&value, "correlation_note")
}

fn non_empty_string_field(value: &Value, field: &str) -> bool {
    value
        .get(field)
        .and_then(Value::as_str)
        .is_some_and(|field| !field.trim().is_empty())
}

fn trade_intent_looks_valid(text: &str) -> bool {
    let Ok(value) = extract_json_value(text) else {
        return false;
    };
    if trade_intent_entry_valid(&value) {
        return true;
    }
    if let Some(per_ticker) = value.get("per_ticker").and_then(Value::as_object) {
        return per_ticker.values().any(trade_intent_entry_valid);
    }
    false
}

fn trade_intent_entry_valid(value: &Value) -> bool {
    let action = value.get("action").and_then(Value::as_str);
    let position_cap = value
        .get("position_size")
        .and_then(Value::as_str)
        .and_then(position_upper_bound);
    matches!(action, Some("Buy" | "Sell" | "Hold"))
        && position_cap.is_some()
        && !(action == Some("Hold") && position_cap.is_some_and(|cap| cap > f64::EPSILON))
        && non_empty_string_field(value, "rationale")
}

fn position_upper_bound(value: &str) -> Option<f64> {
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

fn risk_constraints_look_valid(role: &str, text: &str) -> bool {
    let Ok(value) = extract_json_value(text) else {
        return false;
    };
    let expected_stance = role.strip_prefix("risk.").unwrap_or_default();
    value.get("stance").and_then(Value::as_str) == Some(expected_stance)
        && non_empty_string_field(&value, "argument")
        && (non_empty_string_field(&value, "unique_risk_contribution")
            || value.get("no_new_information").and_then(Value::as_bool) == Some(true))
        && non_empty_string_field(&value, "disagreement_with_prior")
        && non_empty_string_field(&value, "recommended_adjustment")
        && value
            .get("stop_type")
            .and_then(Value::as_str)
            .is_some_and(|stop_type| {
                matches!(
                    stop_type,
                    "none" | "tight" | "trailing" | "event_based" | "time_based"
                )
            })
        && [
            "max_drawdown_pct",
            "position_cap_pct",
            "constraint_confidence",
        ]
        .iter()
        .all(|field| {
            value
                .get(*field)
                .and_then(Value::as_f64)
                .is_some_and(|number| (0.0..=1.0).contains(&number))
        })
        && [
            "rebalance_trigger",
            "risk_off_trigger",
            "review_window",
            "cash_hedge_recommendation",
        ]
        .iter()
        .all(|field| non_empty_string_field(&value, field))
}

fn research_artifact_looks_valid(tickers: &[String], text: &str) -> bool {
    let Ok(value) = extract_json_value(text) else {
        return false;
    };
    let Ok(normalized) = orchestrator_core::normalize_research_artifact_value(value, &[]) else {
        return false;
    };
    let Ok(artifact) = serde_json::from_value::<orchestrator_core::ResearchArtifact>(normalized)
    else {
        return false;
    };
    orchestrator_core::validate_research_artifact(&artifact, tickers).is_ok()
}

fn analyst_final_artifact_looks_valid(role: &str, expected_tickers: &[String], text: &str) -> bool {
    let Ok(value) = extract_json_value(text) else {
        return false;
    };
    if value.get("id").and_then(Value::as_str) != Some(role)
        || value.get("role").and_then(Value::as_str) != Some(role)
    {
        return false;
    }
    let Some(per_ticker) = value.get("per_ticker").and_then(Value::as_object) else {
        return false;
    };
    let expected = expected_tickers
        .iter()
        .collect::<std::collections::BTreeSet<_>>();
    let actual = per_ticker.keys().collect::<std::collections::BTreeSet<_>>();
    if actual != expected {
        return false;
    }
    per_ticker.values().all(|payload| {
        let direction = payload
            .get("direction")
            .and_then(Value::as_str)
            .unwrap_or_default();
        matches!(
            direction,
            "bullish" | "bearish" | "neutral" | "mixed" | "unobserved"
        ) && payload
            .get("confidence")
            .and_then(Value::as_f64)
            .is_some_and(|confidence| (0.0..=1.0).contains(&confidence))
    })
}

pub fn classify_assistant_message(text: &str, turn_status: TurnStatus) -> FollowUpDecision {
    let trimmed = text.trim();

    match turn_status {
        TurnStatus::Final => return FollowUpDecision::Final,
        TurnStatus::Intermediate => return FollowUpDecision::NeedsFollowUp,
        TurnStatus::Unknown => {}
    }

    if trimmed.is_empty() {
        return FollowUpDecision::NeedsFollowUp;
    }
    if trimmed.chars().count() > 1_200 {
        return FollowUpDecision::Final;
    }
    if extract_json_value(trimmed).is_ok_and(|value| value.is_object()) {
        return FollowUpDecision::Final;
    }
    let lower = trimmed.to_ascii_lowercase();
    let phrase_match = [
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
        "我将读取",
        "我会读取",
        "读取完整",
        "读取运行上下文",
        "接下来",
        "现在使用",
        "尝试最后一次",
        "若仍失败",
        "需要补上",
        "避免依据截断",
    ]
    .iter()
    .any(|pattern| lower.contains(pattern) || trimmed.contains(pattern));

    if phrase_match {
        return FollowUpDecision::NeedsFollowUp;
    }
    // Short non-JSON commentary is almost never a finished role artifact. Prefer
    // another loop iteration over ending the turn with degraded text fallback.
    if trimmed.chars().count() < 200 && !trimmed.contains('{') {
        return FollowUpDecision::NeedsFollowUp;
    }
    if trimmed.chars().count() < 200 {
        return FollowUpDecision::Ambiguous;
    }
    FollowUpDecision::Final
}

/// Synchronous wrapper for backward compatibility.
/// Does NOT run the LLM judge; ambiguous messages default to Final.
pub(crate) fn assistant_message_needs_follow_up(text: &str) -> bool {
    matches!(
        classify_assistant_message(text, TurnStatus::Unknown),
        FollowUpDecision::NeedsFollowUp
    )
}

/// System instruction for rig-native tool calling + plain-text final artifacts.
pub fn model_system_instruction(
    available_tools: &[String],
    executing_role: &str,
    tickers: &[String],
) -> String {
    let tools_line = if available_tools.is_empty() {
        "No tools are available for this turn. Emit the final JSON artifact required by the current role schema now.".to_string()
    } else {
        format!(
            "Native function-calling tools are available (use the API tool channel, not invented JSON tool events): {}.\n\
If evidence is missing, call the appropriate tool. When tool evidence is enough, stop calling tools and emit the final JSON artifact as plain assistant text (one JSON object, no markdown fences).",
            serde_json::to_string(available_tools).unwrap_or_default()
        )
    };
    format!(
        "You are running inside an agent loop powered by the rig completion API.\n\
You are executing role `{executing_role}` for exactly these tickers: {}.\n\
A final artifact's role must exactly equal `{executing_role}`. Analyst final artifacts must contain per_ticker entries for exactly this ticker set; do not substitute, rename, or omit tickers.\n\
Protocol:\n\
1) Use native function calls for tools (read_run_context, web.run, etc.). Do not invent custom event JSON lines.\n\
2) Intermediate commentary may be plain text, but the turn must end with the complete artifact required by the current role schema as a single JSON object in assistant text.\n\
3) Do not end with text saying you are about to retry, fetch, analyze, or wait for inputs; call the tool or produce the final artifact.\n\
4) Do not wrap the artifact in id/role/status/report envelopes unless that role schema defines them.\n\
{tools_line}",
        serde_json::to_string(tickers).unwrap_or_default(),
    )
}

/// Backward-compatible alias.
pub fn react_system_instruction(
    available_tools: &[String],
    executing_role: &str,
    tickers: &[String],
) -> String {
    model_system_instruction(available_tools, executing_role, tickers)
}

fn turn_item_prompt_json(
    item: &TurnItem,
    include_tool_metadata: bool,
    truncation: &TruncationConfig,
) -> Value {
    let content_text = truncate_context_fragment(&item.content_text, truncation);
    // Tool results already carry the truncated payload in content_text. Re-emitting
    // content_json would duplicate (and historically re-inflate) that evidence.
    let content_json = match item.item_type {
        TurnItemType::ToolResult => json!({
            "status": item
                .status
                .as_ref()
                .map(AgentItemStatus::as_str)
                .unwrap_or("completed"),
        }),
        _ => {
            let encoded = item.content_json.to_string();
            if encoded.chars().count() > truncation.context_fragment_chars {
                json!({
                    "truncated": true,
                    "preview": truncate_context_fragment(&encoded, truncation),
                })
            } else {
                item.content_json.clone()
            }
        }
    };
    let mut value = json!({
        "type": item.item_type.as_str(),
        "role": item.role,
        "content_text": content_text,
        "content_json": content_json,
    });
    if include_tool_metadata {
        if let Some(map) = value.as_object_mut() {
            map.insert("tool_call_id".to_string(), json!(item.tool_call_id));
            map.insert("tool_name".to_string(), json!(item.tool_name));
        }
    }
    value
}

/// Token usage from a single OpenAI Responses API call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_tokens: u64,
    pub reasoning_tokens: u64,
    pub total_tokens: u64,
}

impl TokenUsage {
    pub fn non_cached_input_tokens(&self) -> u64 {
        self.input_tokens.saturating_sub(self.cached_tokens)
    }

    pub fn visible_output_tokens(&self) -> u64 {
        self.output_tokens.saturating_sub(self.reasoning_tokens)
    }
}

impl std::ops::AddAssign for TokenUsage {
    fn add_assign(&mut self, rhs: Self) {
        self.input_tokens += rhs.input_tokens;
        self.output_tokens += rhs.output_tokens;
        self.cached_tokens += rhs.cached_tokens;
        self.reasoning_tokens += rhs.reasoning_tokens;
        self.total_tokens += rhs.total_tokens;
    }
}

fn log_debug_llm_iteration(
    config: &AgentLoopConfig,
    turn: &Turn,
    loop_index: usize,
    elapsed_ms: u128,
    stream_result: &ModelStreamResult,
) {
    let Some(root) = config.project_root.as_ref() else {
        return;
    };
    let role = if config.role.is_empty() {
        turn.role.as_str()
    } else {
        config.role.as_str()
    };
    let phase = config.phase.or(turn.phase);
    crate::debug_log_time(
        root,
        json!({
            "kind": "llm_iteration",
            "name": role,
            "role": role,
            "phase": phase,
            "topic_id": config.topic_id,
            "model": config.model,
            "loop_index": loop_index,
            "turn_id": turn.turn_id,
            "elapsed_ms": elapsed_ms,
            "llm_ms": elapsed_ms,
            "tool_ms": 0,
            "wait_ms": 0,
            "tool_calls": stream_result.tool_calls.len(),
        }),
    );
    crate::debug_log_token(
        root,
        json!({
            "kind": "llm_iteration",
            "role": role,
            "phase": phase,
            "topic_id": config.topic_id,
            "model": config.model,
            "loop_index": loop_index,
            "turn_id": turn.turn_id,
            "input_tokens": stream_result.usage.input_tokens,
            "output_tokens": stream_result.usage.output_tokens,
            "cached_tokens": stream_result.usage.cached_tokens,
            "reasoning_tokens": stream_result.usage.reasoning_tokens,
            "total_tokens": stream_result.usage.total_tokens,
            "non_cached_input_tokens": stream_result.usage.non_cached_input_tokens(),
            "visible_output_tokens": stream_result.usage.visible_output_tokens(),
            "elapsed_ms": elapsed_ms,
            "tool_calls": stream_result.tool_calls.len(),
        }),
    );
}

pub fn extract_token_usage(raw: &Value) -> TokenUsage {
    let usage = raw
        .get("usage")
        .or_else(|| raw.get("raw").and_then(|raw| raw.get("usage")));
    let input_tokens = usage
        .and_then(|usage| usage.get("input_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let output_tokens = usage
        .and_then(|usage| usage.get("output_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let cached_tokens = usage
        .and_then(|usage| usage.get("input_tokens_details"))
        .and_then(|details| details.get("cached_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let reasoning_tokens = usage
        .and_then(|usage| usage.get("output_tokens_details"))
        .and_then(|details| details.get("reasoning_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let total_tokens = usage
        .and_then(|usage| usage.get("total_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(input_tokens + output_tokens);
    TokenUsage {
        input_tokens,
        output_tokens,
        cached_tokens,
        reasoning_tokens,
        total_tokens,
    }
}

/// Build the user prompt for a rig completion request from turn items.
pub fn model_prompt(input: &ModelInput) -> Result<String> {
    let system = input
        .system_instruction
        .clone()
        .unwrap_or_else(|| model_system_instruction(&input.available_tools, "unknown", &[]));
    let mut static_items = Vec::new();
    let mut dynamic_items = Vec::new();
    let mut captured_role_prompt = false;

    for item in input
        .items
        .iter()
        .filter(|item| item.item_type != TurnItemType::ReasoningState)
    {
        if !captured_role_prompt && item.item_type == TurnItemType::UserMessage {
            static_items.push(turn_item_prompt_json(item, false, &input.truncation));
            captured_role_prompt = true;
        } else {
            dynamic_items.push(turn_item_prompt_json(item, true, &input.truncation));
        }
    }

    let static_context = serde_json::to_string_pretty(&static_items)?;
    let dynamic_context = serde_json::to_string_pretty(&dynamic_items)?;
    Ok(format!(
        "{system}\n\nStatic context:\n{static_context}\n\nDynamic context:\n{dynamic_context}\n\n\
Respond with either a native tool call or the final JSON artifact as plain assistant text."
    ))
}

/// Backward-compatible alias.
pub fn react_prompt(input: &ModelInput) -> Result<String> {
    model_prompt(input)
}

pub(crate) fn extract_json_value(text: &str) -> Result<Value> {
    let trimmed = strip_markdown_json_fences(text.trim());
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        return Ok(value);
    }
    if let Ok(value) = extract_balanced_json_object(trimmed) {
        return Ok(value);
    }
    // Last resort: outermost brace span (may fail on truncated / multi-object text).
    let Some(start) = trimmed.find('{') else {
        bail!("model response did not contain JSON object")
    };
    let Some(end) = trimmed.rfind('}') else {
        bail!("model response did not contain complete JSON object")
    };
    serde_json::from_str(&trimmed[start..=end]).context("failed to parse ReAct JSON response")
}

fn strip_markdown_json_fences(text: &str) -> &str {
    let trimmed = text.trim();
    let Some(rest) = trimmed.strip_prefix("```") else {
        return trimmed;
    };
    let rest = rest
        .strip_prefix("json")
        .or_else(|| rest.strip_prefix("JSON"))
        .unwrap_or(rest)
        .trim_start_matches(|ch: char| ch == '\r' || ch == '\n' || ch.is_whitespace());
    rest.strip_suffix("```").map(str::trim).unwrap_or(trimmed)
}

fn extract_balanced_json_object(text: &str) -> Result<Value> {
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
                    if let Some(start_idx) = start {
                        last = Some((start_idx, index + ch.len_utf8()));
                    }
                } else if depth < 0 {
                    bail!("unbalanced JSON braces in model response");
                }
            }
            _ => {}
        }
    }
    let Some((start, end)) = last else {
        bail!("model response did not contain balanced JSON object");
    };
    serde_json::from_str(&text[start..end]).context("failed to parse ReAct JSON response")
}

pub fn compact_summary_card(items: &[TurnItem]) -> String {
    let total_tokens = estimate_items_tokens(items);
    let chars_per_item = if total_tokens > 20_000 { 500 } else { 240 };
    let recent = items
        .iter()
        .rev()
        .filter(|item| item.item_type != TurnItemType::ReasoningState)
        .take(8)
        .map(|item| {
            format!(
                "- {} {} {}",
                item.item_type.as_str(),
                item.tool_name,
                truncate_chars(&item.content_text, chars_per_item)
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let critical_context = extract_critical_context(items);
    format!(
        "Conversation Summary Card\n\nGoal:\n- Continue the current agent turn.\n\nDecisions:\n- Preserve ReAct item order and only inject compact state into the next model request.\n\nCurrent State:\n- {} items were compacted (~{} tokens).\n\nOpen Tasks:\n- Continue from the latest pending input, tool result, or assistant request.\n\nImportant Context:\n- Do not drop file paths, commands, errors, or user steering.\n\nCritical Context Preserved:\n{}\n\nRecent Tool Results:\n{}",
        items.len(),
        total_tokens,
        critical_context,
        recent
    )
}

fn extract_critical_context(items: &[TurnItem]) -> String {
    let mut critical = Vec::new();
    for item in items {
        collect_paths(&item.content_text, &mut critical);
        collect_urls(&item.content_text, &mut critical);
        if item.item_type == TurnItemType::ToolResult && contains_error_signal(&item.content_text) {
            critical.push(format!(
                "error: {}",
                truncate_chars(&item.content_text, 200)
            ));
        }
        if critical.len() >= 20 {
            break;
        }
    }

    if critical.is_empty() {
        "None".to_string()
    } else {
        critical.into_iter().take(20).collect::<Vec<_>>().join("\n")
    }
}

fn collect_paths(text: &str, critical: &mut Vec<String>) {
    for token in text.split_whitespace() {
        if critical.len() >= 20 {
            break;
        }
        let candidate = trim_context_token(token);
        if candidate.starts_with('/') && has_important_path_extension(candidate) {
            critical.push(format!("path: {candidate}"));
        }
    }
}

fn collect_urls(text: &str, critical: &mut Vec<String>) {
    for token in text.split_whitespace() {
        if critical.len() >= 20 {
            break;
        }
        let candidate = trim_context_token(token);
        if candidate.starts_with("http://") || candidate.starts_with("https://") {
            critical.push(format!("url: {candidate}"));
        }
    }
}

fn trim_context_token(token: &str) -> &str {
    token.trim_matches(|ch: char| {
        matches!(
            ch,
            ',' | '.' | ':' | ';' | '"' | '\'' | ')' | ']' | '}' | '(' | '[' | '{' | '<' | '>'
        )
    })
}

fn has_important_path_extension(path: &str) -> bool {
    [
        ".rs", ".md", ".json", ".yaml", ".yml", ".sqlite", ".db", ".txt",
    ]
    .iter()
    .any(|extension| path.ends_with(extension))
}

fn contains_error_signal(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.contains("error")
        || lower.contains("failed")
        || lower.contains("panic")
        || lower.contains("bail!")
        || lower.contains("unwrap")
}

#[cfg(test)]
pub struct StaticToolRuntime {
    tools: BTreeMap<String, Box<dyn Fn(Value) -> ToolResultItem + Send + Sync>>,
}

pub struct ProjectToolRuntime {
    config: tools::ExternalToolConfig,
    available_tools: Vec<String>,
    web_run: Option<tools::WebRunRuntime>,
    turn_context: Option<ToolRuntimeTurnContext>,
}

impl ProjectToolRuntime {
    pub fn new(config: tools::ExternalToolConfig) -> Self {
        Self::with_available_tools(
            config,
            tools::tool_names()
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
        )
    }

    pub fn with_available_tools(
        config: tools::ExternalToolConfig,
        available_tools: Vec<String>,
    ) -> Self {
        Self {
            config,
            available_tools,
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
        &'a self,
        call: ToolCallRequest,
    ) -> Pin<Box<dyn Future<Output = ToolResultItem> + Send + 'a>> {
        let config = self.config.clone();
        let available_tools = self.available_tools.clone();
        let web_run = self.web_run.clone();
        let turn_context = self.turn_context.clone();
        Box::pin(async move {
            debug!(
                call_id = call.call_id,
                tool = call.name,
                "project tool runtime dispatching tool"
            );
            let web_run_config = web_run.as_ref().map(tools::WebRunRuntime::config);
            let configured = available_tools.iter().any(|name| name == &call.name);
            let enabled = call.name == "think"
                || tools::enabled_tool_names(web_run_config).contains(&call.name.as_str());
            if !configured || !enabled {
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
                };
            }
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
                };
            }
            let call_id = call.call_id;
            let name = call.name;
            if name == tools::WEB_RUN_TOOL_NAME {
                let output = if let Some(web_run) = &web_run {
                    web_run.execute(call.arguments).await
                } else {
                    tools::execute_named_tool(
                        &name,
                        call.arguments,
                        &config,
                        turn_context.as_ref(),
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
                        }
                    }
                };
            }
            match tools::execute_named_tool(
                &name,
                call.arguments,
                &config,
                turn_context.as_ref(),
                web_run.as_ref(),
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
                    }
                }
            }
        })
    }
}

#[cfg(test)]
impl StaticToolRuntime {
    pub fn new() -> Self {
        Self {
            tools: BTreeMap::new(),
        }
    }

    pub fn add_tool<F>(&mut self, name: impl Into<String>, tool: F)
    where
        F: Fn(Value) -> ToolResultItem + Send + Sync + 'static,
    {
        self.tools.insert(name.into(), Box::new(tool));
    }
}

#[cfg(test)]
impl Default for StaticToolRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl LoopToolRuntime for StaticToolRuntime {
    fn execute<'a>(
        &'a self,
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
                };
            };
            tool(call.arguments)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn turn_tickers_reads_generated_model_context() {
        let mut turn = Turn::new(
            "turn-1",
            "session-1",
            "run-1",
            "analyst.technical",
            "prompt",
        );
        turn.model_context =
            "role=analyst.technical, output_mode=JsonArtifact, tickers=QQQ,SOXX\navailable_tools=[]"
                .to_string();

        assert_eq!(turn_tickers(&turn), vec!["QQQ", "SOXX"]);
    }
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
            turn_status: TurnStatus::Unknown,
        }
    }

    fn agent_loop_config_with_judge(judge: JudgeConfig) -> AgentLoopConfig {
        AgentLoopConfig {
            judge,
            judge_endpoint: Some("http://127.0.0.1:9".to_string()),
            judge_api_key: Some("test-key".to_string()),
            ..AgentLoopConfig::default()
        }
    }

    #[test]
    fn empty_message_needs_follow_up() {
        assert!(assistant_message_needs_follow_up(""));
        assert!(assistant_message_needs_follow_up("   "));
    }

    #[test]
    fn long_message_is_final() {
        let long = "x".repeat(1201);
        assert!(!assistant_message_needs_follow_up(&long));
    }

    #[test]
    fn json_object_is_final() {
        let json = r#"{"id": "test", "role": "analyst.technical", "per_ticker": {}}"#;
        assert!(!assistant_message_needs_follow_up(json));
    }

    #[test]
    fn phrase_match_needs_follow_up() {
        assert!(assistant_message_needs_follow_up(
            "I need a few key inputs to proceed"
        ));
        assert!(assistant_message_needs_follow_up("接下来我会分析"));
        assert!(assistant_message_needs_follow_up(
            "我将读取完整运行上下文，提取 QQQ 的可核对技术指标"
        ));
        assert!(assistant_message_needs_follow_up("retry the request"));
    }

    #[test]
    fn turn_status_final_overrides_phrase_match() {
        let decision = classify_assistant_message("retry the request", TurnStatus::Final);
        assert_eq!(decision, FollowUpDecision::Final);
    }

    #[test]
    fn turn_status_intermediate_overrides_json() {
        let json = r#"{"id": "test", "role": "analyst", "per_ticker": {}}"#;
        let decision = classify_assistant_message(json, TurnStatus::Intermediate);
        assert_eq!(decision, FollowUpDecision::NeedsFollowUp);
    }

    #[test]
    fn short_non_json_no_phrase_needs_follow_up() {
        let decision = classify_assistant_message("Let me check the data.", TurnStatus::Unknown);
        assert_eq!(decision, FollowUpDecision::NeedsFollowUp);
        assert!(assistant_message_needs_follow_up("Let me check the data."));
    }

    #[test]
    fn research_artifact_looks_valid_accepts_per_ticker_envelope() {
        let text = r#"{
            "report":"ok",
            "per_ticker":{
                "QQQ":{
                    "rating":"Hold",
                    "long_probability":0.55,
                    "short_probability":0.45,
                    "confidence_basis":"evidence_balanced",
                    "hold_reason":"evidence_balanced",
                    "plan":["Watch confirmation"],
                    "probability_rationale":"Evidence remains balanced."
                }
            }
        }"#;
        assert!(research_artifact_looks_valid(&["QQQ".to_string()], text));
        assert!(!research_artifact_looks_valid(
            &["QQQ".to_string()],
            "正在读取完整上下文"
        ));
    }

    #[test]
    fn interaction_packet_looks_valid_rejects_action_notes() {
        assert!(interaction_packet_role("researcher.bull.interaction"));
        assert!(!interaction_packet_looks_valid(
            "researcher.bull.interaction",
            "接下来我会分析并给出 packet"
        ));
        assert!(!interaction_packet_looks_valid(
            "researcher.bull.interaction",
            r#"{"role":"mediator.topic_controller","artifact_type":"topic_controller_packet"}"#
        ));
    }

    #[test]
    fn seed_packet_looks_valid_rejects_another_role() {
        let text = r#"{
            "role":"researcher.bear.initial",
            "artifact_type":"bull_seed_packet",
            "topic_id":"qqq-trend",
            "claims":[{"claim":"upside"}]
        }"#;

        assert!(!seed_packet_looks_valid("researcher.bull.initial", text));
    }

    #[test]
    fn seed_packet_looks_valid_rejects_incomplete_claims() {
        let text = r#"{
            "role":"researcher.bull.initial",
            "artifact_type":"bull_seed_packet",
            "topic_id":"qqq-trend",
            "claims":[{"claim":"upside"}],
            "summary":"one claim",
            "reducer_checks":{}
        }"#;

        assert!(!seed_packet_looks_valid("researcher.bull.initial", text));
    }

    #[test]
    fn analyst_final_packet_requires_exact_role_and_ticker_set() {
        let expected_tickers = vec!["QQQ".to_string(), "SOXX".to_string()];
        let wrong_role = r#"{
            "role":"analyst.youtube",
            "per_ticker":{
                "QQQ":{"direction":"bullish","confidence":0.7},
                "SOXX":{"direction":"bearish","confidence":0.6}
            }
        }"#;
        let wrong_ticker = r#"{
            "role":"analyst.technical",
            "per_ticker":{
                "QQQ":{"direction":"bullish","confidence":0.7},
                "TQQQ":{"direction":"bearish","confidence":0.6}
            }
        }"#;

        assert!(!analyst_final_artifact_looks_valid(
            "analyst.technical",
            &expected_tickers,
            wrong_role
        ));
        assert!(!analyst_final_artifact_looks_valid(
            "analyst.technical",
            &expected_tickers,
            wrong_ticker
        ));

        let wrong_id = r#"{
            "id":"technical-analysis-2026-07-16",
            "role":"analyst.technical",
            "per_ticker":{
                "QQQ":{"direction":"bullish","confidence":0.7},
                "SOXX":{"direction":"bearish","confidence":0.6}
            }
        }"#;
        assert!(!analyst_final_artifact_looks_valid(
            "analyst.technical",
            &expected_tickers,
            wrong_id
        ));
    }

    #[test]
    fn controller_and_allocation_final_packets_reject_incomplete_or_wrapped_json() {
        assert!(!controller_packet_looks_valid(
            r#"{"role":"mediator.topic_controller","artifact_type":"topic_controller_packet","topic_id":"qqq"}"#
        ));
        assert!(!allocation_artifact_looks_valid(
            r#"{
                "role":"allocation.manager",
                "report":{
                    "weights":{"cash_hedge":{"weight":1.0}},
                    "total_equity_exposure":0.0,
                    "vix_regime":"normal",
                    "correlation_note":"none",
                    "summary":"cash"
                }
            }"#
        ));
        assert!(allocation_artifact_looks_valid(
            r#"{
                "weights":{"cash_hedge":{"weight":1.0}},
                "total_equity_exposure":0.0,
                "vix_regime":"normal",
                "correlation_note":"none",
                "summary":"cash"
            }"#
        ));
        assert!(!allocation_artifact_looks_valid(
            r#"{
                "weights":{"VIX":{"weight":0.0},"cash_hedge":{"weight":1.0}},
                "total_equity_exposure":0.0,
                "vix_regime":"normal",
                "correlation_note":"none",
                "summary":"cash"
            }"#
        ));
    }

    #[test]
    fn risk_packet_requires_explicit_information_gain() {
        let base = json!({
            "stance": "neutral",
            "argument": "Balanced review.",
            "recommended_adjustment": "Keep the existing cap.",
            "stop_type": "event_based",
            "max_drawdown_pct": 0.04,
            "position_cap_pct": 0.10,
            "rebalance_trigger": "Review on confirmation.",
            "risk_off_trigger": "Reduce on invalidation.",
            "review_window": "1d",
            "cash_hedge_recommendation": "Keep cash.",
            "constraint_confidence": 0.8
        });
        assert!(!risk_constraints_look_valid(
            "risk.neutral",
            &base.to_string()
        ));

        let mut valid = base;
        valid["unique_risk_contribution"] =
            json!("Correlation concentration requires a combined cap.");
        valid["disagreement_with_prior"] = json!(
            "Agree with the prior zero-position result, but use correlation as the binding reason."
        );
        valid["no_new_information"] = json!(false);
        assert!(risk_constraints_look_valid(
            "risk.neutral",
            &valid.to_string()
        ));
    }

    #[test]
    fn medium_non_json_no_phrase_is_final() {
        let msg = "This is a medium-length message that does not match any phrases ".to_string()
            + &"and is over 200 chars ".repeat(10)
            + "but under 1200.";
        let decision = classify_assistant_message(&msg, TurnStatus::Unknown);
        assert_eq!(decision, FollowUpDecision::Final);
    }

    #[test]
    fn extract_turn_status_reads_optional_metadata() {
        assert_eq!(
            extract_turn_status(&json!({"turn_status": "final"})),
            TurnStatus::Final
        );
        assert_eq!(
            extract_turn_status(&json!({"turn_status": "intermediate"})),
            TurnStatus::Intermediate
        );
        assert_eq!(extract_turn_status(&json!({})), TurnStatus::Unknown);
    }

    #[tokio::test]
    async fn ambiguous_message_defaults_final_when_judge_disabled() {
        let mut turn = Turn::new("turn-judge", "session-1", "run-1", "loop.test", "start");
        let mut result = ModelStreamResult {
            needs_follow_up: false,
            assistant_message_decisions: vec![AssistantMessageDecision {
                item_id: "msg-1".to_string(),
                text: "Let me check the data.".to_string(),
                decision: FollowUpDecision::Ambiguous,
            }],
            ..ModelStreamResult::default()
        };
        let config = agent_loop_config_with_judge(JudgeConfig {
            enabled: false,
            ..JudgeConfig::default()
        });
        let mut judge_calls = 0;

        apply_judge_to_stream_result(&mut turn, &config, &mut result, &mut judge_calls)
            .await
            .unwrap();

        assert_eq!(judge_calls, 0);
        assert!(!result.needs_follow_up);
        assert_eq!(
            result.assistant_message_decisions[0].decision,
            FollowUpDecision::Final
        );
    }

    #[tokio::test]
    async fn ambiguous_message_defaults_final_when_judge_cap_reached() {
        let mut turn = Turn::new("turn-judge", "session-1", "run-1", "loop.test", "start");
        let mut result = ModelStreamResult::default();
        result
            .assistant_message_decisions
            .push(AssistantMessageDecision {
                item_id: "msg-1".to_string(),
                text: "Let me check the data.".to_string(),
                decision: FollowUpDecision::Ambiguous,
            });
        let config = agent_loop_config_with_judge(JudgeConfig {
            max_messages_per_turn: 0,
            ..JudgeConfig::default()
        });
        let mut judge_calls = 0;

        apply_judge_to_stream_result(&mut turn, &config, &mut result, &mut judge_calls)
            .await
            .unwrap();

        assert_eq!(judge_calls, 0);
        assert!(!result.needs_follow_up);
        assert_eq!(
            result.assistant_message_decisions[0].decision,
            FollowUpDecision::Final
        );
    }

    #[tokio::test]
    async fn ambiguous_message_defaults_final_when_judge_fails() {
        let mut turn = Turn::new("turn-judge", "session-1", "run-1", "loop.test", "start");
        let mut result = ModelStreamResult::default();
        result
            .assistant_message_decisions
            .push(AssistantMessageDecision {
                item_id: "msg-1".to_string(),
                text: "Let me check the data.".to_string(),
                decision: FollowUpDecision::Ambiguous,
            });
        let config = agent_loop_config_with_judge(JudgeConfig::default());
        let mut judge_calls = 0;

        apply_judge_to_stream_result(&mut turn, &config, &mut result, &mut judge_calls)
            .await
            .unwrap();

        assert_eq!(judge_calls, 1);
        assert!(!result.needs_follow_up);
        assert_eq!(
            result.assistant_message_decisions[0].decision,
            FollowUpDecision::Final
        );
    }

    fn assistant_texts(conn: &rusqlite::Connection) -> Vec<String> {
        let rows = conn
            .prepare("SELECT full_context_json FROM agent_events ORDER BY turn_number")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let mut texts = Vec::new();
        for json_str in &rows {
            if let Ok(items) = serde_json::from_str::<Vec<Value>>(json_str) {
                for item in items {
                    if item.get("event_type").and_then(|v| v.as_str()) == Some("assistant_message")
                    {
                        if let Some(text) = item.get("content_text").and_then(|v| v.as_str()) {
                            texts.push(text.to_string());
                        }
                    }
                }
            }
        }
        texts
    }

    fn test_item(item_type: TurnItemType, role: &str, content_text: String) -> TurnItem {
        TurnItem {
            item_type,
            role: role.to_string(),
            content_text,
            content_json: Value::Null,
            tool_call_id: String::new(),
            tool_name: String::new(),
            output_item_id: String::new(),
            phase: None,
            status: None,
            db_row_id: None,
        }
    }

    fn append_history(conn: &rusqlite::Connection, turn: &mut Turn, items: Vec<TurnItem>) {
        for item in items {
            turn.emitted_items.push(item.clone());
        }
        persist_turn(conn, turn, &TruncationConfig::default()).unwrap();
    }

    #[allow(dead_code)]
    fn item_count(_conn: &rusqlite::Connection, _event_type: &str) -> i64 {
        0 // ponytail: no per-event rows in new schema, tests use this for compat
    }

    fn turn_end_state(conn: &rusqlite::Connection, turn_id: &str) -> (bool, String) {
        conn.query_row(
            "SELECT summary FROM agent_events WHERE turn_id = ?",
            [turn_id],
            |row| {
                let summary: String = row.get(0)?;
                Ok((!summary.is_empty(), summary))
            },
        )
        .unwrap_or((false, String::new()))
    }

    #[test]
    fn token_based_compaction_triggers_before_item_count() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        ensure_schema(&conn).unwrap();
        let mut turn = Turn::new("turn-compact", "session-compact", "run-1", "loop.test", "");
        let items = (0..10)
            .map(|index| {
                test_item(
                    TurnItemType::ToolResult,
                    "tool",
                    format!("/tmp/large-{index}.rs {}", "x".repeat(8_000)),
                )
            })
            .collect::<Vec<_>>();
        let total_tokens = estimate_items_tokens(&items);
        assert!(total_tokens > 9_600);
        assert!(items.len() < 120);
        append_history(&conn, &mut turn, items);

        let input =
            build_model_input(&conn, &mut turn, false, &AgentLoopConfig::default()).unwrap();

        assert_eq!(input.items[0].item_type, TurnItemType::CompactSummary);
        assert_eq!(
            input.items[0].content_json["compaction_trigger"],
            "token_threshold"
        );
        assert_eq!(input.items[0].content_json["items_compacted"], 10);
        assert!(
            input.items[0].content_json["estimated_tokens_before"]
                .as_u64()
                .unwrap()
                > 9_600
        );
        assert!(input.items[0].content_text.contains("~"));
        assert!(input.items[0].content_text.contains("path: /tmp/large-"));
    }

    #[test]
    fn dynamic_budget_keeps_role_prompt_and_latest_tool_evidence() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        ensure_schema(&conn).unwrap();
        let role_prompt = format!("TECHNICAL ROLE {}", "p".repeat(30_000));
        let mut turn = Turn::new(
            "turn-evidence",
            "session-evidence",
            "run-1",
            "analyst.technical",
            role_prompt.clone(),
        );
        turn.model_context = "tickers=QQQ,SOXX,VIX".to_string();
        let tool_result = ToolResultItem {
            call_id: "call-1".to_string(),
            name: "read_run_context".to_string(),
            status: "completed".to_string(),
            output: json!({
                "evidence": {
                    "daily": [
                        {"ticker": "QQQ", "Close": 717.73},
                        {"ticker": "SOXX", "Close": 555.27},
                        {"ticker": "VIX", "Close": 15.67}
                    ]
                }
            }),
            error: None,
        };
        append_history(
            &conn,
            &mut turn,
            vec![TurnItem::tool_result(
                &tool_result,
                &TruncationConfig::default(),
            )],
        );
        turn.push_pending_input("emit the final artifact");

        let input =
            build_model_input(&conn, &mut turn, false, &AgentLoopConfig::default()).unwrap();

        assert_eq!(input.items[0].content_text, role_prompt);
        assert!(input.items.iter().any(|item| {
            item.item_type == TurnItemType::ToolResult
                && item.content_text.contains("717.73")
                && item.content_text.contains("555.27")
                && item.content_text.contains("15.67")
        }));
        assert!(input.items.iter().any(|item| {
            item.item_type == TurnItemType::UserMessage
                && item.content_text.contains("emit the final artifact")
        }));
    }

    #[test]
    fn turn_history_resume_loads_only_matching_turn_id() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        ensure_schema(&conn).unwrap();

        let mut technical = Turn::new(
            "turn-tech",
            "session-tech",
            "run-shared",
            "analyst.technical",
            "TECH PROMPT",
        );
        technical.emitted_items.push(TurnItem::user("TECH PROMPT"));
        technical.emitted_items.push(TurnItem::tool_result(
            &ToolResultItem {
                call_id: "call-1".to_string(),
                name: "read_run_context".to_string(),
                status: "completed".to_string(),
                output: json!({"status": "ok", "evidence": {"daily": [{"ticker": "QQQ", "Close": 100.0}]}}),
                error: None,
            },
            &TruncationConfig::default(),
        ));
        persist_turn(&conn, &technical, &TruncationConfig::default()).unwrap();

        let mut news = Turn::new(
            "turn-news",
            "session-news",
            "run-shared",
            "analyst.news_macro",
            "NEWS PROMPT",
        );
        news.emitted_items.push(TurnItem::user("NEWS PROMPT ONLY"));
        persist_turn(&conn, &news, &TruncationConfig::default()).unwrap();

        // Simulate multi-round resume: empty in-memory turn with same turn_id.
        let resumed = Turn::new(
            "turn-tech",
            "session-tech",
            "run-shared",
            "analyst.technical",
            "",
        );
        let items = history_items(&conn, &resumed, 200).unwrap();
        assert!(
            items.iter().any(|item| {
                item.item_type == TurnItemType::ToolResult && item.content_text.contains("100.0")
            }),
            "resume must load technical tool evidence by turn_id"
        );
        assert!(!items
            .iter()
            .any(|item| item.content_text.contains("NEWS PROMPT ONLY")));
    }

    #[test]
    fn parallel_roles_do_not_steal_each_others_tool_evidence() {
        // Live F1: two phase-1 roles share run_id. session_history_items used to
        // load ORDER BY turn_number DESC for the whole run, so news_macro's later
        // turn replaced technical's tool evidence on the next model iteration.
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        ensure_schema(&conn).unwrap();

        let technical_prompt = "TECHNICAL ROLE PROMPT";
        let mut technical = Turn::new(
            "turn-technical",
            "session-technical",
            "run-shared",
            "analyst.technical",
            technical_prompt,
        );
        technical.model_context = "tickers=QQQ,SOXX,VIX".to_string();
        technical
            .emitted_items
            .push(TurnItem::user(technical_prompt));
        technical.emitted_items.push(TurnItem::tool_result(
            &ToolResultItem {
                call_id: "call-tech".to_string(),
                name: "read_run_context".to_string(),
                status: "completed".to_string(),
                output: json!({
                    "status": "ok",
                    "evidence": {
                        "daily": [
                            {"ticker": "QQQ", "Close": 717.73, "RSI14": 55.0},
                            {"ticker": "SOXX", "Close": 555.27},
                            {"ticker": "VIX", "Close": 15.67}
                        ]
                    }
                }),
                error: None,
            },
            &TruncationConfig::default(),
        ));
        persist_turn(&conn, &technical, &TruncationConfig::default()).unwrap();

        // Sibling role persists a higher turn_number without technical evidence.
        let mut news = Turn::new(
            "turn-news",
            "session-news",
            "run-shared",
            "analyst.news_macro",
            "NEWS ROLE PROMPT",
        );
        news.emitted_items.push(TurnItem::user(
            "NEWS ROLE PROMPT without technical snapshots",
        ));
        persist_turn(&conn, &news, &TruncationConfig::default()).unwrap();

        technical.push_pending_input(
            "Tool evidence is already available. Tools are now disabled. Emit final JSON.",
        );
        let input =
            build_model_input(&conn, &mut technical, false, &AgentLoopConfig::default()).unwrap();

        assert!(
            input.items.iter().any(|item| {
                item.item_type == TurnItemType::ToolResult
                    && item.content_text.contains("717.73")
                    && item.content_text.contains("RSI14")
            }),
            "technical tool evidence must survive a later sibling role persist; items={:?}",
            input
                .items
                .iter()
                .map(|item| (
                    item.item_type.as_str(),
                    item.tool_name.as_str(),
                    item.content_text.chars().take(80).collect::<String>()
                ))
                .collect::<Vec<_>>()
        );
        assert!(input.items.iter().any(|item| {
            item.item_type == TurnItemType::UserMessage
                && item
                    .content_text
                    .contains("Tool evidence is already available")
        }));
        assert!(!input
            .items
            .iter()
            .any(|item| item.content_text.contains("NEWS ROLE PROMPT")));
    }

    #[test]
    fn item_count_compaction_still_works_as_fallback() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        ensure_schema(&conn).unwrap();
        let mut turn = Turn::new("turn-items", "session-items", "run-1", "loop.test", "");
        let items = (0..121)
            .map(|index| {
                test_item(
                    TurnItemType::AssistantMessage,
                    "assistant",
                    format!("msg {index}"),
                )
            })
            .collect::<Vec<_>>();
        let total_tokens = estimate_items_tokens(&items);
        assert!(total_tokens < 9_600);
        assert!(items.len() > 120);
        append_history(&conn, &mut turn, items);

        let input =
            build_model_input(&conn, &mut turn, false, &AgentLoopConfig::default()).unwrap();

        assert_eq!(input.items[0].item_type, TurnItemType::CompactSummary);
        assert_eq!(
            input.items[0].content_json["compaction_trigger"],
            "item_count"
        );
        assert_eq!(input.items[0].content_json["items_compacted"], 121);
    }

    #[test]
    fn compaction_does_not_trigger_under_item_and_token_thresholds() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        ensure_schema(&conn).unwrap();
        let mut turn = Turn::new("turn-small", "session-small", "run-1", "loop.test", "");
        let items = (0..50)
            .map(|index| {
                test_item(
                    TurnItemType::AssistantMessage,
                    "assistant",
                    format!("msg {index} {}", "x".repeat(100)),
                )
            })
            .collect::<Vec<_>>();
        let total_tokens = estimate_items_tokens(&items);
        assert!(total_tokens < 9_600);
        append_history(&conn, &mut turn, items);

        let input =
            build_model_input(&conn, &mut turn, false, &AgentLoopConfig::default()).unwrap();

        assert_eq!(input.items.len(), 50);
        assert!(!input
            .items
            .iter()
            .any(|item| item.item_type == TurnItemType::CompactSummary));
    }

    #[test]
    fn compact_summary_card_preserves_critical_context() {
        let items = vec![
            test_item(
                TurnItemType::UserMessage,
                "user",
                "Please edit /Users/alixeu/project/akzio-signal-intelligence/crates/orchestrator-llm/src/agent_loop.rs".to_string(),
            ),
            test_item(
                TurnItemType::ToolResult,
                "tool",
                "Command failed with error: panic while reading https://example.com/context".to_string(),
            ),
        ];

        let summary = compact_summary_card(&items);

        assert!(summary.contains("items were compacted (~"));
        assert!(summary.contains("path: /Users/alixeu/project/akzio-signal-intelligence/crates/orchestrator-llm/src/agent_loop.rs"));
        assert!(summary.contains("url: https://example.com/context"));
        assert!(summary.contains("error: Command failed with error"));
    }

    #[test]
    fn token_compaction_threshold_defaults_to_max_tokens_for_invalid_ratio() {
        assert_eq!(token_compaction_threshold(12_000, 0.8), 9_600);
        assert_eq!(token_compaction_threshold(12_000, 0.0), 12_000);
        assert_eq!(token_compaction_threshold(12_000, f64::NAN), 12_000);
    }

    #[tokio::test]
    async fn project_runtime_rejects_unconfigured_tool() {
        let runtime = ProjectToolRuntime::with_available_tools(
            tools::ExternalToolConfig {
                project_root: PathBuf::from("."),
                db_path: None,
                run_dir: None,
                run_id: None,
                tickers: Vec::new(),
                phase00_index: None,
            phase00_gate: None,
        },
            vec![tools::READ_RUN_CONTEXT_TOOL_NAME.to_string()],
        );

        let result = runtime
            .execute(ToolCallRequest {
                call_id: "call-unknown".to_string(),
                name: "unknown_tool".to_string(),
                arguments: json!({"command": "printf no"}),
            })
            .await;

        assert_eq!(result.status, "error");
        assert_eq!(result.error.as_deref(), Some("unknown tool name"));
    }

    #[tokio::test]
    async fn project_runtime_rejects_unconfigured_think_tool() {
        let runtime = ProjectToolRuntime::with_available_tools(
            tools::ExternalToolConfig {
                project_root: PathBuf::from("."),
                db_path: None,
                run_dir: None,
                run_id: None,
                tickers: Vec::new(),
                phase00_index: None,
            phase00_gate: None,
        },
            Vec::new(),
        );

        let result = runtime
            .execute(ToolCallRequest {
                call_id: "call-think".to_string(),
                name: "think".to_string(),
                arguments: json!({"summary": "should not run"}),
            })
            .await;

        assert_eq!(result.status, "error");
        assert_eq!(result.error.as_deref(), Some("unknown tool name"));
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
        }];
        let mut model = FakeModel::new(vec![
            first,
            model_response(Some("Final answer ready for downstream consumers after completing the requested analysis steps without further tool calls. detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail "), true),
        ]);
        let mut tools = StaticToolRuntime::new();
        tools.add_tool("echo", |args| ToolResultItem {
            call_id: "call-1".to_string(),
            name: "echo".to_string(),
            status: "completed".to_string(),
            output: args,
            error: None,
        });
        let mut turn = Turn::new("turn-1", "session-1", "run-1", "loop.test", "start");

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
                "Final answer ready for downstream consumers after completing the requested analysis steps without further tool calls. detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail ".to_string(),
            ]
        );
        let (has_summary, _summary) = turn_end_state(&conn, "turn-1");
        assert!(has_summary);
    }

    #[tokio::test]
    async fn final_assistant_text_completes_turn_only_when_no_follow_up_work_exists() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        ensure_schema(&conn).unwrap();
        let mut model = FakeModel::new(vec![model_response(
            Some("Final answer ready for downstream consumers after completing the requested analysis steps without further tool calls. detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail "),
            true,
        )]);
        let mut tools = StaticToolRuntime::new();
        let mut turn = Turn::new("turn-1", "session-1", "run-1", "loop.test", "start");

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
            vec!["Final answer ready for downstream consumers after completing the requested analysis steps without further tool calls. detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail ".to_string()]
        );
        let (has_summary, _summary) = turn_end_state(&conn, "turn-1");
        assert!(has_summary);
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
            model_response(Some("Final answer ready for downstream consumers after completing the requested analysis steps without further tool calls. detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail "), true),
        ]);
        let mut tools = StaticToolRuntime::new();
        let mut turn = Turn::new("turn-1", "session-1", "run-1", "loop.test", "start");

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
                "Final answer ready for downstream consumers after completing the requested analysis steps without further tool calls. detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail ".to_string(),
            ]
        );
        let (has_summary, _summary) = turn_end_state(&conn, "turn-1");
        assert!(has_summary);
    }

    #[test]
    fn react_prompt_splits_static_role_prompt_from_dynamic_turn_items() {
        let tool_result = ToolResultItem {
            call_id: "call-1".to_string(),
            name: "read_run_context".to_string(),
            status: "ok".to_string(),
            output: json!({"dynamic": true}),
            error: None,
        };
        let input = ModelInput {
            system_instruction: None,
            items: vec![
                TurnItem::user("STATIC ROLE PROMPT"),
                TurnItem::assistant("dynamic assistant note", Value::Null),
                TurnItem::tool_result(&tool_result, &TruncationConfig::default()),
            ],
            available_tools: vec!["read_run_context".to_string()],
            truncation: TruncationConfig::default(),
        };

        let prompt = react_prompt(&input).unwrap();
        let static_index = prompt.find("Static context:").unwrap();
        let dynamic_index = prompt.find("Dynamic context:").unwrap();
        assert!(static_index < dynamic_index);
        assert!(prompt[static_index..dynamic_index].contains("STATIC ROLE PROMPT"));
        assert!(!prompt[static_index..dynamic_index].contains("dynamic assistant note"));
        assert!(prompt[dynamic_index..].contains("dynamic assistant note"));
        assert!(prompt[dynamic_index..].contains("read_run_context"));
    }

    #[test]
    fn tool_result_history_does_not_store_full_mega_payload() {
        let huge = "x".repeat(50_000);
        let tool_result = ToolResultItem {
            call_id: "call-huge".to_string(),
            name: "read_run_context".to_string(),
            status: "completed".to_string(),
            output: json!({
                "status": "ok",
                "evidence": { "csv": huge.clone() },
            }),
            error: None,
        };
        let item = TurnItem::tool_result(&tool_result, &TruncationConfig::default());
        assert!(item.content_text.chars().count() <= TruncationConfig::default().tool_result_chars);
        let stored = item.content_json.to_string();
        assert!(
            stored.chars().count() < 20_000,
            "content_json must not re-embed the raw mega payload, got {} chars",
            stored.chars().count()
        );
        assert!(!stored.contains(&huge));

        let prompt = react_prompt(&ModelInput {
            system_instruction: Some("sys".to_string()),
            items: vec![item],
            available_tools: vec![],
            truncation: TruncationConfig::default(),
        })
        .unwrap();
        assert!(!prompt.contains(&huge));
        assert!(prompt.chars().count() < 30_000);
    }

    #[test]
    fn react_system_instruction_keeps_executing_role_and_tickers_visible() {
        let instruction = react_system_instruction(
            &[],
            "analyst.technical",
            &["QQQ".to_string(), "SOXX".to_string()],
        );

        assert!(instruction.contains("analyst.technical"));
        assert!(instruction.contains("QQQ"));
        assert!(instruction.contains("SOXX"));
        assert!(instruction.contains("artifact required by the current role schema"));
        assert!(!instruction.contains("JSON with id, role, status, per_ticker"));
    }

    #[test]
    fn extract_token_usage_reads_cached_tokens_from_response_usage() {
        let raw = json!({
            "usage": {
                "input_tokens": 1200,
                "output_tokens": 80,
                "total_tokens": 1280,
                "input_tokens_details": {"cached_tokens": 384}
            }
        });

        assert_eq!(
            extract_token_usage(&raw),
            TokenUsage {
                input_tokens: 1200,
                output_tokens: 80,
                cached_tokens: 384,
                reasoning_tokens: 0,
                total_tokens: 1280,
            }
        );
        assert_eq!(
            extract_token_usage(&json!({"usage": {}})),
            TokenUsage {
                input_tokens: 0,
                output_tokens: 0,
                cached_tokens: 0,
                reasoning_tokens: 0,
                total_tokens: 0,
            }
        );
        assert_eq!(
            extract_token_usage(
                &json!({"raw": {"usage": {"input_tokens": 7, "output_tokens": 5}}})
            ),
            TokenUsage {
                input_tokens: 7,
                output_tokens: 5,
                cached_tokens: 0,
                reasoning_tokens: 0,
                total_tokens: 12,
            }
        );

        let with_reasoning = json!({
            "usage": {
                "input_tokens": 5000,
                "output_tokens": 2000,
                "total_tokens": 7000,
                "input_tokens_details": {"cached_tokens": 3000},
                "output_tokens_details": {"reasoning_tokens": 800}
            }
        });
        let usage = extract_token_usage(&with_reasoning);
        assert_eq!(usage.reasoning_tokens, 800);
        assert_eq!(usage.cached_tokens, 3000);
        assert_eq!(usage.non_cached_input_tokens(), 2000);
        assert_eq!(usage.visible_output_tokens(), 1200);
    }

    #[test]
    fn token_usage_add_assign_accumulates() {
        let mut a = TokenUsage {
            input_tokens: 100,
            output_tokens: 50,
            cached_tokens: 40,
            reasoning_tokens: 10,
            total_tokens: 150,
        };
        let b = TokenUsage {
            input_tokens: 200,
            output_tokens: 80,
            cached_tokens: 60,
            reasoning_tokens: 30,
            total_tokens: 280,
        };
        a += b;
        assert_eq!(a.input_tokens, 300);
        assert_eq!(a.output_tokens, 130);
        assert_eq!(a.cached_tokens, 100);
        assert_eq!(a.reasoning_tokens, 40);
        assert_eq!(a.total_tokens, 430);
        assert_eq!(a.non_cached_input_tokens(), 200);
        assert_eq!(a.visible_output_tokens(), 90);
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
                    turn_status: TurnStatus::Unknown,
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
                    delta: "Final answer ready for downstream consumers after completing the requested analysis steps without further tool calls. detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail ".to_string(),
                },
                ModelStreamEvent::AssistantMessageCompleted {
                    item_id: "msg-2".to_string(),
                    turn_status: TurnStatus::Unknown,
                },
                ModelStreamEvent::ResponseCompleted {
                    end_turn: true,
                    raw: json!({"step": 2}),
                },
            ],
        ]);
        let mut tools = StaticToolRuntime::new();
        let mut sink = RecordingSink::default();
        let mut turn = Turn::new("turn-1", "session-1", "run-1", "loop.test", "start");

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
        let mut turn = Turn::new("turn-1", "session-1", "run-1", "loop.test", "start");
        persist_turn(&conn, &turn, &TruncationConfig::default()).unwrap();

        {
            let mut handler =
                ModelStreamHandler::new(&conn, &mut turn, &mut sink, TruncationConfig::default());
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
        persist_turn(&conn, &turn, &TruncationConfig::default()).unwrap();

        assert!(sink.events.iter().any(|event| {
            matches!(
                event,
                AgentLoopEvent::TurnItemDelta { item_id, delta, .. }
                    if item_id == "msg-live" && delta == "live chunk"
            )
        }));
        assert_eq!(assistant_texts(&conn), vec!["live chunk".to_string()]);
        let full_json: String = conn
            .query_row(
                "SELECT full_context_json FROM agent_events WHERE turn_id = ?",
                ["turn-1"],
                |row| row.get(0),
            )
            .unwrap();
        let events: Vec<Value> = serde_json::from_str(&full_json).unwrap_or_default();
        let content_json = events
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("assistant_message"))
            .and_then(|e| e.get("content_json"))
            .cloned()
            .unwrap_or(Value::Null);
        assert_eq!(content_json["status"], "in_progress");
        assert_eq!(content_json["phase"], "commentary");
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
                }],
                end_turn: false,
                raw: json!({"step": 1}),
                turn_status: TurnStatus::Unknown,
            },
            ModelResponse {
                assistant_message: Some("Final answer ready for downstream consumers after completing the requested analysis steps without further tool calls. detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail ".to_string()),
                reasoning_summary: None,
                tool_calls: vec![],
                end_turn: true,
                raw: json!({"step": 2}),
                turn_status: TurnStatus::Unknown,
            },
        ]);
        let mut tools = StaticToolRuntime::new();
        tools.add_tool("echo", |args| ToolResultItem {
            call_id: "call-1".to_string(),
            name: "echo".to_string(),
            status: "completed".to_string(),
            output: args,
            error: None,
        });
        let mut turn = Turn::new("turn-1", "session-1", "run-1", "loop.test", "start");

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
            .query_row("SELECT COUNT(*) FROM agent_events", [], |row| row.get(0))
            .unwrap();
        assert!(count >= 1);
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
                }],
                end_turn: false,
                raw: json!({"step": 1}),
                turn_status: TurnStatus::Unknown,
            },
            ModelResponse {
                assistant_message: Some("Final answer ready for downstream consumers after completing the requested analysis steps without further tool calls. detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail ".to_string()),
                reasoning_summary: None,
                tool_calls: vec![],
                end_turn: true,
                raw: json!({"step": 2}),
                turn_status: TurnStatus::Unknown,
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
        let mut tools = ProjectToolRuntime::with_available_tools(
            tools::ExternalToolConfig {
                project_root: PathBuf::from("."),
                db_path: None,
                run_dir: None,
                run_id: None,
                tickers: vec!["TQQQ".to_string()],
                phase00_index: None,
            phase00_gate: None,
        },
            vec![tools::WEB_RUN_TOOL_NAME.to_string()],
        )
        .with_web_run_runtime(tools::WebRunRuntime::new(config).with_provider(Arc::new(provider)));
        let mut turn = Turn::new("turn-web", "session-1", "run-1", "loop.test", "start");

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
        let (has_summary, _summary) = turn_end_state(&conn, "turn-web");
        assert!(has_summary, "turn should have a non-empty summary");

        let stored_json: String = conn
            .query_row(
                "SELECT full_context_json FROM agent_events WHERE turn_id = ?",
                ["turn-web"],
                |row| row.get(0),
            )
            .unwrap();
        let events: Vec<Value> = serde_json::from_str(&stored_json).unwrap_or_default();
        let stored_content = events
            .iter()
            .find(|item| {
                item.get("event_type").and_then(|v| v.as_str()) == Some("tool_result")
                    && item.get("tool_name").and_then(|v| v.as_str())
                        == Some(tools::WEB_RUN_TOOL_NAME)
            })
            .and_then(|item| item.get("content_text"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        assert!(stored_content.contains("URL: https://research.example.com/tqqq-liquidity"));
    }

    #[test]
    fn extract_json_value_strips_markdown_fences() {
        let value =
            extract_json_value("```json\n{\"end_turn\": true, \"assistant_message\": \"{}\"}\n```")
                .unwrap();
        assert_eq!(value["end_turn"], true);
    }

    #[test]
    fn extract_json_value_prefers_balanced_object_over_outer_span() {
        let text = "prefix {\"noise\": \"{\"} {\"end_turn\": true, \"assistant_message\": \"ok\"} trailing";
        let value = extract_json_value(text).unwrap();
        assert_eq!(value["end_turn"], true);
        assert_eq!(value["assistant_message"], "ok");
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
    fn parse_react_response_forces_end_turn_false_when_tools_present() {
        let response = parse_react_response(json!({
            "assistant_message": "fetching",
            "end_turn": true,
            "tool_calls": [{
                "call_id": "call-1",
                "name": "read_run_context",
                "arguments": {"kind": "jin10"}
            }]
        }))
        .unwrap();
        assert!(!response.end_turn);
        assert_eq!(response.tool_calls.len(), 1);
    }

    #[test]
    fn react_prompt_hides_encrypted_reasoning_state() {
        let input = ModelInput {
            system_instruction: None,
            items: vec![
                TurnItem::user("visible request"),
                TurnItem {
                    item_type: TurnItemType::ReasoningState,
                    role: "assistant".to_string(),
                    content_text: String::new(),
                    content_json: json!({
                        "output_item_id": "rs_1",
                        "encrypted_content": "secret-state"
                    }),
                    tool_call_id: String::new(),
                    tool_name: String::new(),
                    output_item_id: "rs_1".to_string(),
                    phase: None,
                    status: Some(AgentItemStatus::Completed),
                    db_row_id: None,
                },
            ],
            available_tools: Vec::new(),
            truncation: TruncationConfig::default(),
        };

        let prompt = react_prompt(&input).unwrap();

        assert!(prompt.contains("visible request"));
        assert!(!prompt.contains("secret-state"));
        assert!(!prompt.contains("reasoning_state"));
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
