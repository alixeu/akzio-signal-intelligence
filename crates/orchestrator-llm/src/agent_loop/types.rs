use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;

use crate::truncation::TruncationConfig;

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
    /// Rust-observed output from the provider-hosted Responses `web_search`
    /// tool. It is persisted for provenance but never replayed as a function
    /// call or a user message on a later model iteration.
    NativeWebSearch,
    SystemContext,
    DeveloperContext,
    CompactSummary,
    InjectedContext,
}

impl TurnItemType {
    pub(super) fn as_str(&self) -> &'static str {
        match self {
            Self::UserMessage => "user_message",
            Self::AssistantMessage => "assistant_message",
            Self::ReasoningSummary => "reasoning_summary",
            Self::ReasoningState => "reasoning_state",
            Self::PlanUpdate => "plan_update",
            Self::ToolCall => "tool_call",
            Self::ToolResult => "tool_result",
            Self::NativeWebSearch => "native_web_search",
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolResultItem {
    pub call_id: String,
    pub name: String,
    pub status: String,
    #[serde(default)]
    pub output: Value,
    #[serde(default)]
    pub error: Option<String>,
}

/// Stable identity for one externally visible tool execution.
///
/// Provider output-item IDs are not execution identities: a provider may
/// re-emit an item while the same FileStore turn is being resumed.  The
/// session/turn/call tuple is owned by Rust and is therefore the only key used
/// for execution recovery and idempotency.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ToolExecutionKey {
    pub session_id: String,
    pub turn_id: String,
    pub call_id: String,
}

impl ToolExecutionKey {
    pub fn new(
        session_id: impl Into<String>,
        turn_id: impl Into<String>,
        call_id: impl Into<String>,
    ) -> Result<Self> {
        let key = Self {
            session_id: session_id.into(),
            turn_id: turn_id.into(),
            call_id: call_id.into(),
        };
        if key.session_id.trim().is_empty()
            || key.turn_id.trim().is_empty()
            || key.call_id.trim().is_empty()
        {
            bail!("tool execution key requires non-empty session, turn, and call IDs")
        }
        Ok(key)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolExecutionStatus {
    Pending,
    Running,
    Completed,
    Interrupted,
}

/// FileStore record for a tool side effect.  A `Completed` record contains
/// the exact ToolResultItem that can be replayed; `Running` and `Interrupted`
/// are deliberately fail-closed on recovery.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolExecutionRecord {
    pub key: ToolExecutionKey,
    pub tool_name: String,
    pub status: ToolExecutionStatus,
    #[serde(default)]
    pub result: Option<ToolResultItem>,
    #[serde(default)]
    pub reason: Option<String>,
}

impl ToolExecutionRecord {
    pub fn pending(key: ToolExecutionKey, tool_name: impl Into<String>) -> Self {
        Self {
            key,
            tool_name: tool_name.into(),
            status: ToolExecutionStatus::Pending,
            result: None,
            reason: None,
        }
    }

    pub fn running(key: ToolExecutionKey, tool_name: impl Into<String>) -> Self {
        Self {
            key,
            tool_name: tool_name.into(),
            status: ToolExecutionStatus::Running,
            result: None,
            reason: None,
        }
    }

    pub fn completed(result: ToolResultItem, key: ToolExecutionKey) -> Self {
        Self {
            key,
            tool_name: result.name.clone(),
            status: ToolExecutionStatus::Completed,
            result: Some(result),
            reason: None,
        }
    }

    pub fn interrupted(
        key: ToolExecutionKey,
        tool_name: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            key,
            tool_name: tool_name.into(),
            status: ToolExecutionStatus::Interrupted,
            result: None,
            reason: Some(reason.into()),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RetrievalPolicy {
    pub mandatory_summary_query: bool,
    #[serde(default)]
    pub required_source_phases: Vec<i64>,
    #[serde(default)]
    pub required_detail_source_phases: Vec<i64>,
    #[serde(default)]
    pub minimum_detail_expansions: usize,
    #[serde(default)]
    pub maximum_detail_expansions: usize,
    #[serde(default = "default_retrieval_page_limit")]
    pub summary_page_limit: usize,
    #[serde(default = "default_retrieval_page_limit")]
    pub detail_page_limit: usize,
    #[serde(default)]
    pub allow_empty_when_no_visible_summary: bool,
    #[serde(default)]
    pub allowed_direct_contexts: Vec<String>,
}

fn default_retrieval_page_limit() -> usize {
    20
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentItemPhase {
    Commentary,
    Final,
}

impl AgentItemPhase {
    pub(super) fn as_str(&self) -> &'static str {
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
    pub(super) fn as_str(&self) -> &'static str {
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

pub trait AgentEventSink: Send {
    fn emit<'a>(
        &'a mut self,
        event: AgentLoopEvent,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

pub trait ModelEventHandler: Send {
    fn handle<'a>(
        &'a mut self,
        event: ModelStreamEvent,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
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

    pub fn tool_call_with_output_item_id(
        call: &ToolCallRequest,
        output_item_id: impl Into<String>,
    ) -> Self {
        let output_item_id = output_item_id.into();
        let mut item = Self::tool_call(call);
        item.output_item_id = output_item_id.clone();
        if let Some(object) = item.content_json.as_object_mut() {
            object.insert("output_item_id".to_owned(), Value::String(output_item_id));
        }
        item
    }

    pub fn tool_result(result: &ToolResultItem, truncation: &TruncationConfig) -> Self {
        let status = if result.status == "completed" || result.status == "started" {
            AgentItemStatus::Completed
        } else {
            AgentItemStatus::Failed
        };
        // A failed tool commonly has `output: null` and puts the actionable
        // validation error in `error`.  Keep that error in the next model
        // input: otherwise the model sees only `null`, cannot repair its
        // Draft, and eventually exhausts its turn budget.
        let content_text = result.error.as_ref().map_or_else(
            || {
                result
                    .output
                    .get("content")
                    .or_else(|| result.output.get("text"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(|| result.output.to_string())
            },
            |error| json!({"error": error}).to_string(),
        );
        let truncated_text = super::truncate_tool_result(&content_text, truncation);
        // Keep content_json lean. Storing the raw tool payload here previously
        // re-inflated truncated content_text when model_prompt serialized both.
        let compact_output =
            super::compact_tool_output_for_history(&result.output, &truncated_text, truncation);
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

    /// Persist provider-hosted web-search provenance without fabricating a
    /// function-call/tool-result pair. Responses manages this tool internally,
    /// so replaying it as a function output would corrupt the next request.
    pub fn native_web_search(output_item_id: impl Into<String>, record: Value) -> Self {
        let output_item_id = output_item_id.into();
        Self {
            item_type: TurnItemType::NativeWebSearch,
            role: "tool".to_string(),
            content_text: "OpenAI Responses native web_search completed".to_string(),
            content_json: merge_item_metadata(
                record,
                &output_item_id,
                None,
                AgentItemStatus::Completed,
            ),
            tool_call_id: String::new(),
            tool_name: "native_web_search".to_string(),
            output_item_id,
            phase: None,
            status: Some(AgentItemStatus::Completed),
            db_row_id: None,
        }
    }
}

pub(super) fn merge_item_metadata(
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
    /// First item belonging to this turn's retrieval budget. Forked turns
    /// retain parent items for model context, but parent retrieval calls must
    /// not consume the child turn's duplicate or detail budget.
    pub retrieval_scope_start: usize,
    pub pending_tool_calls: Vec<ToolCallRequest>,
    pub cancellation_state: String,
    pub needs_follow_up: bool,
    pub end_reason: Option<String>,
    /// Successful terminal tool result for ToolManaged roles. This is kept
    /// outside assistant text so a finalized artifact never depends on a
    /// natural-language response.
    pub terminal_tool_result: Option<ToolResultItem>,
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
            retrieval_scope_start: 0,
            pending_tool_calls: Vec::new(),
            cancellation_state: "none".to_string(),
            needs_follow_up: false,
            end_reason: None,
            terminal_tool_result: None,
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
    /// A Responses FunctionToolCall carries both the execution `call_id` and
    /// a distinct provider output-item `id`. Keep both when persisting the
    /// transcript; the call_id remains the tool-execution idempotency key.
    ToolCallCompletedWithOutputItem {
        tool_call: ToolCallRequest,
        output_item_id: String,
    },
    /// Completed evidence/provenance from a provider-hosted Responses
    /// `web_search` tool. Unlike `ToolCallCompleted`, this does not require a
    /// local function response or a follow-up model turn.
    NativeWebSearchCompleted {
        item_id: String,
        record: Value,
    },
    ResponseCompleted {
        end_turn: bool,
        raw: Value,
    },
}

#[derive(Debug, Clone, Default)]
pub struct ModelStreamResult {
    /// Whether this model response explicitly ended the turn.
    pub end_turn: bool,
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
}

impl ModelStreamResult {
    /// Wait/overhead = total - llm - tool (clamped at 0).
    pub fn wait_ms(&self, total_elapsed_ms: u128) -> u128 {
        total_elapsed_ms.saturating_sub(self.llm_ms.saturating_add(self.tool_ms))
    }
}

pub trait LoopModel: Send {
    fn generate<'a>(
        &'a mut self,
        input: ModelInput,
    ) -> Pin<Box<dyn Future<Output = Result<ModelResponse>> + Send + 'a>>;

    fn stream_events<'a>(
        &'a mut self,
        input: ModelInput,
        handler: &'a mut (dyn ModelEventHandler + Send),
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
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

    /// A runtime may stop the current loop after returning an audited tool
    /// result, for example when a Rust-owned write budget is exhausted.
    fn abort_reason(&self) -> Option<String> {
        None
    }

    fn execute<'a>(
        &'a self,
        call: ToolCallRequest,
    ) -> Pin<Box<dyn Future<Output = ToolResultItem> + Send + 'a>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_tool_result_keeps_the_actionable_error_in_history() {
        let item = TurnItem::tool_result(
            &ToolResultItem {
                call_id: "call-1".to_owned(),
                name: "submit_terminal_result".to_owned(),
                status: "error".to_owned(),
                output: Value::Null,
                error: Some("key_evidence must contain at least one source-backed item".to_owned()),
            },
            &TruncationConfig::default(),
        );

        assert!(item.content_text.contains("key_evidence"));
        assert_eq!(
            item.content_json
                .pointer("/result/error")
                .and_then(Value::as_str),
            Some("key_evidence must contain at least one source-backed item")
        );
    }
}

#[derive(Debug, Clone)]
pub struct ToolRuntimeTurnContext {
    pub run_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub role: String,
    pub phase: Option<i64>,
}
