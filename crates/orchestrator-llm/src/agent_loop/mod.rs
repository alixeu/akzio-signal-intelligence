mod debug_capture;
mod session_store;
mod streaming;
mod types;

pub use debug_capture::input_to_debug_messages;
pub use session_store::{FileStoreSessionRuntime, SessionRuntimeSpec, TurnCheckpoint};
pub use types::*;

use streaming::ModelStreamHandler;

use anyhow::{bail, Result};
use orchestrator_core;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    path::PathBuf,
    pin::Pin,
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tracing::{debug, warn};

use crate::llm_judge::{judge_message_status, JudgeConfig};
use crate::tools::{self, truncate_chars};
use crate::truncation::{truncate_semantic, TruncationConfig};
use crate::AgentSettings;

const DEFAULT_MAX_AGENT_LOOPS: usize = 8;
const SYSTEM_PROMPT_TEMPLATE: &str = include_str!("../../../../prompts/system/agent_loop.md");
const REQUEST_WRAPPER_TEMPLATE: &str =
    include_str!("../../../../prompts/system/messages/request_wrapper.md");
const FINALIZE_INSTRUCTION: &str = include_str!("../../../../prompts/system/messages/finalize.md");

pub struct AgentLoopModel {
    settings: AgentSettings,
}

impl AgentLoopModel {
    pub fn new(settings: AgentSettings) -> Self {
        Self { settings }
    }
}

impl LoopModel for AgentLoopModel {
    fn generate<'a>(
        &'a mut self,
        input: ModelInput,
    ) -> Pin<Box<dyn Future<Output = Result<ModelResponse>> + Send + 'a>> {
        Box::pin(async move {
            let started = Instant::now();
            let req_messages = input_to_debug_messages(&input);
            let prompt = model_prompt(&input)?;
            let result = crate::run_model_text_once(&self.settings, &input, &prompt).await;
            if self.settings.debug {
                let elapsed_ms = started.elapsed().as_millis();
                crate::append_debug_llm_record(
                    &self.settings,
                    json!({
                        "kind": "generate",
                        "role": self.settings.role,
                        "phase": self.settings.phase,
                        "topic_id": self.settings.topic_id,
                        "round": self.settings.debug_round,
                        "model": self.settings.llm.model,
                        "req": { "messages": req_messages },
                        "resp": {
                            "status": if result.is_ok() { "completed" } else { "error" },
                            "output": result.as_ref().ok().map(|text| json!([{"type": "output_text", "text": text}])).unwrap_or_else(|| json!([])),
                            "error": result.as_ref().err().map(ToString::to_string),
                        },
                        "elapsed_ms": elapsed_ms,
                        "token": null,
                        "response_text": result.as_ref().ok(),
                    }),
                )?;
            }
            let text = result?;
            Ok(model_response_from_assistant_text(&text))
        })
    }

    fn stream_events<'a>(
        &'a mut self,
        input: ModelInput,
        handler: &'a mut dyn ModelEventHandler,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
        Box::pin(async move {
            let prompt = model_role_prompt(&input)?;
            let mut capture =
                debug_capture::DebugLlmCapture::new(handler, &input, &self.settings.llm.tools);
            let result =
                crate::run_model_event_stream(&self.settings, &input, &prompt, &mut capture).await;
            if self.settings.debug {
                crate::append_debug_llm_record(
                    &self.settings,
                    capture.into_record(&self.settings, result.as_ref().err()),
                )?;
            }
            result
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
    pub retrieval_policy: RetrievalPolicy,
    /// Tool-managed roles must finish through a successful terminal tool,
    /// rather than an assistant-message artifact.
    pub require_terminal_tool: bool,
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
            retrieval_policy: RetrievalPolicy::default(),
            require_terminal_tool: false,
        }
    }
}

pub async fn run_turn<M, T>(
    session: &FileStoreSessionRuntime,
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
    run_turn_with_events(session, turn, model, tools, config, &mut sink).await
}

pub async fn run_turn_with_events<M, T, S>(
    session: &FileStoreSessionRuntime,
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
    persist_turn(session, turn, &config.truncation)?;
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
        let already = turn
            .emitted_items
            .iter()
            .any(|item| item.item_type == TurnItemType::ToolResult);
        if !already {
            let available_tools = turn_available_tools(turn);
            let preseed_calls = preseed_tool_calls(turn, &turn_tickers(turn), &available_tools);
            for call in preseed_calls {
                turn.emitted_items.push(TurnItem::tool_call(&call));
                let result = tools.execute(call).await;
                turn.emitted_items
                    .push(TurnItem::tool_result(&result, &config.truncation));
            }
            if turn
                .emitted_items
                .iter()
                .any(|item| item.item_type == TurnItemType::ToolResult)
            {
                persist_turn(session, turn, &config.truncation)?;
            }
        }
    }
    let mut first_iteration = true;
    let max_loops = config.max_agent_loops.map(|value| value.max(1));
    let mut loop_index = 0usize;
    let mut end_turn_count = 0usize;
    let mut aggregate_result = ModelStreamResult::default();
    let mut judge_call_count = 0usize;
    let mut retrieval_retry_queued = false;
    loop {
        if let Some(max_loops) = max_loops {
            if end_turn_count >= max_loops {
                turn.end_reason = Some("max_loops".to_string());
                warn!(
                    turn_id = turn.turn_id,
                    role = turn.role,
                    phase = turn.phase,
                    model_iterations = loop_index,
                    completed_end_turns = end_turn_count,
                    max_end_turns = max_loops,
                    pending_input = turn.pending_input.len(),
                    pending_tool_calls = turn.pending_tool_calls.len(),
                    "agent loop exhausted its end-turn budget"
                );
                bail!(
                    "agent loop reached max_agent_loops={max_loops} after end_turns={end_turn_count}"
                );
            }
        }
        loop_index += 1;
        let input = build_model_input(session, turn, first_iteration, &config)?;
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
            ModelStreamHandler::new(session, turn, sink, config.truncation.clone());
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
        if stream_result.end_turn {
            end_turn_count += 1;
            debug!(
                turn_id = turn.turn_id,
                role = turn.role,
                loop_index,
                end_turn_count,
                max_end_turns = ?max_loops,
                "agent loop recorded end_turn"
            );
        }

        if !turn.pending_tool_calls.is_empty() {
            let calls = std::mem::take(&mut turn.pending_tool_calls);

            // Tool calls are intentionally sequential. A later write in the
            // same model response may cite evidence returned by an earlier
            // read, and a terminal tool must stop subsequent calls.
            let debug_metrics = config.debug;
            let debug_root = config.project_root.clone();
            let debug_role = turn.role.clone();
            let debug_phase = turn.phase;
            let debug_topic = config.topic_id.clone();
            let debug_loop = loop_index;
            let tool_batch_started = Instant::now();
            let mut terminal_completed = false;
            let mut calls = calls.into_iter();
            while let Some(call) = calls.next() {
                emit_tool_call_status(turn, sink, &call, AgentItemStatus::Running).await?;
                let call_id = call.call_id.clone();
                let name = call.name.clone();
                let tool_started = Instant::now();
                // The typed FileStore Index runtime owns list-before-detail
                // visibility.  It tracks IDs returned by read_indexes and
                // rejects any unlisted read_index_details request.
                debug!(
                    call_id = call_id,
                    tool = name,
                    "agent loop tool call starting"
                );
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
                emit_tool_result(turn, sink, &result).await?;
                turn.emitted_items
                    .push(TurnItem::tool_result(&result, &config.truncation));
                if is_terminal_tool_result(&result) {
                    turn.terminal_tool_result = Some(result);
                    terminal_completed = true;
                    for ignored in calls {
                        let ignored_result = ToolResultItem {
                            call_id: ignored.call_id,
                            name: ignored.name,
                            status: "ignored".to_string(),
                            output: json!({
                                "status": "ignored_after_terminal",
                                "item_count": 0,
                                "items": []
                            }),
                            error: Some(
                                "tool call was ignored because a prior terminal finalize succeeded"
                                    .to_string(),
                            ),
                        };
                        emit_tool_result(turn, sink, &ignored_result).await?;
                        turn.emitted_items
                            .push(TurnItem::tool_result(&ignored_result, &config.truncation));
                    }
                    break;
                }
            }
            let tool_batch_ms = tool_batch_started.elapsed().as_millis();
            aggregate_result.tool_ms = aggregate_result.tool_ms.saturating_add(tool_batch_ms);
            persist_turn(session, turn, &config.truncation)?;
            if terminal_completed {
                turn.end_reason = Some("terminal_tool".to_string());
                persist_turn(session, turn, &config.truncation)?;
                return Ok(aggregate_result);
            }
            if loop_index >= 3 && !config.require_terminal_tool {
                turn.tools_disabled = true;
                turn.push_pending_input(FINALIZE_INSTRUCTION);
            }
            turn.needs_follow_up = true;
            persist_turn(session, turn, &config.truncation)?;
            continue;
        }

        if !turn.pending_input.is_empty() {
            turn.needs_follow_up = true;
            persist_turn(session, turn, &config.truncation)?;
            continue;
        }

        if config.require_terminal_tool {
            if !retrieval_retry_queued {
                retrieval_retry_queued = true;
                turn.push_pending_input(
                    "No terminal finalize tool succeeded. Use the assigned finalize tool now; do not provide a prose answer.",
                );
                turn.needs_follow_up = true;
                persist_turn(session, turn, &config.truncation)?;
                continue;
            }
            bail!("tool-managed agent ended without a successful terminal finalize tool");
        }

        if turn.needs_follow_up {
            turn.needs_follow_up = false;
            persist_turn(session, turn, &config.truncation)?;
            continue;
        }

        if let Some(item_id) = stream_result.last_assistant_message_id.clone() {
            aggregate_result.last_assistant_message_id = Some(item_id.clone());
            mark_last_assistant_message_as_final(turn, &item_id, sink, &config.truncation).await?;
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
        persist_turn(session, turn, &config.truncation)?;
        return Ok(aggregate_result);
    }
}

pub fn retrieval_audit(turn: &Turn) -> Value {
    let mut calls = Vec::new();
    let mut call_args = BTreeMap::<String, Value>::new();
    let mut summary_ids = BTreeSet::new();
    let mut detail_ids = BTreeSet::new();
    let mut expanded_summary_ids = Vec::new();
    let mut summary_source_phases = BTreeMap::<String, i64>::new();
    let mut visible_source_phases = BTreeSet::new();
    let mut expanded_source_phases = BTreeSet::new();
    let mut listed_before_detail = BTreeSet::new();
    let mut detail_before_list = BTreeSet::new();
    let mut signatures = BTreeMap::<String, usize>::new();
    let mut summary_query_count = 0usize;
    let mut successful_summary_query_count = 0usize;
    let mut detail_call_count = 0usize;
    let mut visible_summary_count = 0usize;
    let mut any_truncated = false;
    let mut summary_filters = Vec::new();
    let mut successful_summary_filters = Vec::new();

    for item in &turn.emitted_items {
        match item.item_type {
            TurnItemType::ToolCall
                if matches!(
                    item.tool_name.as_str(),
                    tools::READ_INDEXES_TOOL_NAME
                        | tools::READ_INDEX_DETAILS_TOOL_NAME
                        | tools::READ_REFLECTION_SOURCE_TOOL_NAME
                ) =>
            {
                let arguments = item
                    .content_json
                    .pointer("/call/arguments")
                    .cloned()
                    .unwrap_or(Value::Null);
                let signature = format!("{}:{}", item.tool_name, canonical_value(&arguments));
                *signatures.entry(signature).or_default() += 1;
                if item.tool_name == tools::READ_INDEXES_TOOL_NAME {
                    summary_query_count += 1;
                    summary_filters.push(arguments.clone());
                } else if item.tool_name == tools::READ_INDEX_DETAILS_TOOL_NAME {
                    detail_call_count += 1;
                    if let Some(summary_id) = arguments.get("index_id").and_then(Value::as_str) {
                        expanded_summary_ids.push(summary_id.to_string());
                        if let Some(source_phase) = summary_source_phases.get(summary_id) {
                            expanded_source_phases.insert(*source_phase);
                        }
                        if !listed_before_detail.contains(summary_id) {
                            detail_before_list.insert(summary_id.to_string());
                        }
                    }
                }
                call_args.insert(item.tool_call_id.clone(), arguments.clone());
                calls.push(json!({
                    "call_id": item.tool_call_id,
                    "tool": item.tool_name,
                    "arguments": arguments
                }));
            }
            TurnItemType::ToolResult => {
                let Some(output) = tool_result_output(item) else {
                    continue;
                };
                if item.tool_name == tools::READ_INDEXES_TOOL_NAME {
                    if item.status == Some(AgentItemStatus::Completed) {
                        successful_summary_query_count += 1;
                        if let Some(arguments) = call_args.get(&item.tool_call_id) {
                            successful_summary_filters.push(arguments.clone());
                        }
                    }
                    visible_summary_count = visible_summary_count.saturating_add(
                        output
                            .get("item_count")
                            .and_then(Value::as_u64)
                            .unwrap_or(0) as usize,
                    );
                    any_truncated |= output
                        .get("truncated")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    if let Some(items) = output.get("items").and_then(Value::as_array) {
                        for summary in items {
                            if let Some(id) = summary.get("index_id").and_then(Value::as_str) {
                                summary_ids.insert(id.to_string());
                                listed_before_detail.insert(id.to_string());
                                if let Some(source_phase) =
                                    summary.get("source_phase").and_then(Value::as_i64)
                                {
                                    summary_source_phases.insert(id.to_string(), source_phase);
                                    visible_source_phases.insert(source_phase);
                                }
                            }
                        }
                    }
                } else if item.tool_name == tools::READ_INDEX_DETAILS_TOOL_NAME {
                    any_truncated |= output
                        .get("truncated")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    if let Some(items) = output.get("items").and_then(Value::as_array) {
                        for detail in items {
                            if let Some(id) = detail.get("detail_id").and_then(Value::as_str) {
                                detail_ids.insert(id.to_string());
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    let duplicate_retrievals = signatures
        .values()
        .map(|count| count.saturating_sub(1))
        .sum::<usize>();
    let unique_expanded = expanded_summary_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    json!({
        "source": "rust_turn_history",
        "status": "available",
        "summary_query_count": summary_query_count,
        "successful_summary_query_count": successful_summary_query_count,
        "detail_call_count": detail_call_count,
        "visible_summary_count": visible_summary_count,
        "summary_filters": summary_filters,
        "successful_summary_filters": successful_summary_filters,
        "returned_summary_ids": summary_ids,
        "visible_source_phases": visible_source_phases,
        "expanded_summary_ids": unique_expanded,
        "expanded_source_phases": expanded_source_phases,
        "read_detail_ids": detail_ids,
        "duplicate_retrieval_count": duplicate_retrievals,
        "detail_requested_before_visible_index": detail_before_list,
        "tool_result_truncated": any_truncated,
        "calls": calls,
        "call_argument_count": call_args.len()
    })
}

fn tool_result_output(item: &TurnItem) -> Option<Value> {
    item.content_json
        .pointer("/result/output")
        .cloned()
        .or_else(|| serde_json::from_str(&item.content_text).ok())
}

fn canonical_value(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
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

pub(super) fn truncate_tool_result(content: &str, truncation: &TruncationConfig) -> String {
    truncate_semantic(content, truncation.tool_result_chars, truncation)
}

fn truncate_context_fragment(content: &str, truncation: &TruncationConfig) -> String {
    truncate_semantic(content, truncation.context_fragment_chars, truncation)
}

/// Shrink oversized tool outputs before they are stored in turn history / prompts.
pub(super) fn compact_tool_output_for_history(
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

pub(super) fn persist_turn(
    session: &FileStoreSessionRuntime,
    turn: &Turn,
    _truncation: &TruncationConfig,
) -> Result<()> {
    let persisted = session.read_current_turn(&turn.turn_id)?;
    let next_item = persisted
        .iter()
        .filter_map(|event| event.payload.get("item_index").and_then(Value::as_u64))
        .max()
        .map_or(0, |index| index as usize + 1);
    for (index, item) in turn.emitted_items.iter().enumerate().skip(next_item) {
        session.append_turn_item_at(turn, item, Some(index), session_timestamp())?;
    }
    session.append_checkpoint(
        turn,
        TurnCheckpoint {
            item_count: turn.emitted_items.len(),
            end_reason: turn.end_reason.clone(),
            needs_follow_up: turn.needs_follow_up,
        },
        session_timestamp(),
    )?;
    if let Some(terminal) = &turn.terminal_tool_result {
        if !persisted
            .iter()
            .any(|event| event.event_type == orchestrator_store::SessionEventType::Terminal)
        {
            session.append_terminal(turn, terminal, session_timestamp())?;
        }
    }
    Ok(())
}

pub(super) fn update_turn_item(
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

pub(super) async fn emit_started<S: AgentEventSink>(
    turn: &Turn,
    sink: &mut S,
    item: &TurnItem,
) -> Result<()> {
    if let Some(output_item) = output_item_for(item) {
        sink.emit(AgentLoopEvent::TurnItemStarted {
            turn_id: turn.turn_id.clone(),
            item: output_item,
        })
        .await?;
    }
    Ok(())
}

pub(super) async fn emit_completed<S: AgentEventSink>(
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

pub(super) async fn emit_delta<S: AgentEventSink>(
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

pub(super) fn started_assistant_item(item_id: &str) -> TurnItem {
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

pub(super) fn started_reasoning_item(item_id: &str) -> TurnItem {
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
    session: &FileStoreSessionRuntime,
    turn: &mut Turn,
    _first_iteration: bool,
    config: &AgentLoopConfig,
) -> Result<ModelInput> {
    let mut items = history_items(session, turn, config.history_limit)?;
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
        let retained_call_ids: std::collections::HashSet<&str> = recent_tool_results
            .iter()
            .filter(|tr| !tr.tool_call_id.is_empty())
            .map(|tr| tr.tool_call_id.as_str())
            .collect();
        let matching_tool_calls: std::collections::HashMap<String, TurnItem> = items
            .iter()
            .filter(|item| {
                item.item_type == TurnItemType::ToolCall
                    && retained_call_ids.contains(item.tool_call_id.as_str())
            })
            .map(|item| (item.tool_call_id.clone(), item.clone()))
            .collect();
        items = Vec::new();
        if let Some(role_prompt) = role_prompt.clone() {
            items.push(role_prompt);
        }
        items.push(item);
        for tr in &recent_tool_results {
            if let Some(tc) = matching_tool_calls.get(&tr.tool_call_id) {
                items.push(tc.clone());
            }
            items.push(tr.clone());
        }
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

fn history_items(
    session: &FileStoreSessionRuntime,
    turn: &Turn,
    limit: usize,
) -> Result<Vec<TurnItem>> {
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
        session
            .read_current_turn(&turn.turn_id)?
            .into_iter()
            .filter_map(|event| {
                event
                    .payload
                    .get("item_type")
                    .is_some()
                    .then_some(event.payload)
            })
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
        .or_else(|| value.get("item_type"))
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

async fn mark_last_assistant_message_as_final<S: AgentEventSink>(
    turn: &mut Turn,
    item_id: &str,
    sink: &mut S,
    truncation: &TruncationConfig,
) -> Result<()> {
    if let Some(item) = update_turn_item(
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

fn session_timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs().to_string())
        .unwrap_or_else(|_| "0".to_owned())
}

fn preseed_tool_calls(
    turn: &Turn,
    tickers: &[String],
    available_tools: &[String],
) -> Vec<ToolCallRequest> {
    let mut calls = Vec::new();
    let tool_enabled = |name: &str| available_tools.iter().any(|tool| tool == name);
    match turn.role.as_str() {
        "analyst.technical" if tool_enabled("read_technical_snapshot") => {
            calls.push(ToolCallRequest {
                call_id: "preseed-technical-snapshot".to_string(),
                name: "read_technical_snapshot".to_string(),
                arguments: json!({ "tickers": tickers, "intervals": ["daily", "3h", "20min"] }),
            });
        }
        "analyst.news_macro" if tool_enabled("read_jin10_candidates") => {
            calls.push(ToolCallRequest {
                call_id: "preseed-jin10-candidates".to_string(),
                name: "read_jin10_candidates".to_string(),
                arguments: json!({ "tickers": tickers }),
            });
        }
        _ => {}
    }
    calls
}

/// Map assistant prose into a non-artifact response. Native function calls arrive on the stream.
pub fn model_response_from_assistant_text(text: &str) -> ModelResponse {
    ModelResponse {
        assistant_message: Some(text.to_string()),
        reasoning_summary: None,
        tool_calls: Vec::new(),
        end_turn: true,
        raw: json!({"source": "plain_text"}),
        turn_status: TurnStatus::Unknown,
    }
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

/// A terminal tool result is a Rust-owned completion signal. ToolManaged
/// profiles never infer completion from assistant prose.
pub fn is_terminal_tool_result(result: &ToolResultItem) -> bool {
    result.status == "completed"
        && result
            .output
            .get("terminal")
            .and_then(Value::as_bool)
            .unwrap_or(false)
}

pub fn classify_assistant_message(_text: &str, turn_status: TurnStatus) -> FollowUpDecision {
    match turn_status {
        TurnStatus::Intermediate => FollowUpDecision::NeedsFollowUp,
        TurnStatus::Final | TurnStatus::Unknown => FollowUpDecision::Final,
    }
}

/// System instruction for native tool calling + plain-text final artifacts.
pub fn model_system_instruction(
    available_tools: &[String],
    executing_role: &str,
    tickers: &[String],
) -> String {
    SYSTEM_PROMPT_TEMPLATE
        .replace("{executing_role}", executing_role)
        .replace(
            "{tickers}",
            &serde_json::to_string(tickers).unwrap_or_default(),
        )
        .replace(
            "{available_tools}",
            &serde_json::to_string(available_tools).unwrap_or_default(),
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

/// Extract the role prompt (first user message) for use as the main prompt
/// in the Responses API request. History items (tool calls, tool results,
/// steer messages) are handled separately via native multi-turn messages.
pub fn model_role_prompt(input: &ModelInput) -> Result<String> {
    let role_prompt = input
        .items
        .iter()
        .find(|item| item.item_type == TurnItemType::UserMessage)
        .map(|item| item.content_text.clone())
        .unwrap_or_default();
    if role_prompt.trim().is_empty() {
        bail!("no role prompt found in model input items");
    }
    Ok(role_prompt)
}

/// Build the user prompt for a Responses API request from turn items.
/// Used by the generate (non-streaming) path which sends a single text blob.
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
    Ok(REQUEST_WRAPPER_TEMPLATE
        .replace("{system}", &system)
        .replace("{static_context}", &static_context)
        .replace("{dynamic_context}", &dynamic_context))
}

/// Backward-compatible alias.
pub fn react_prompt(input: &ModelInput) -> Result<String> {
    model_prompt(input)
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
    index_binding: Option<tools::index_tools::IndexToolRuntimeBinding>,
    index_runtime: Option<
        tools::index_tools::IndexToolRuntime<
            std::sync::Arc<dyn tools::index_tools::IndexToolService>,
        >,
    >,
    index_runtime_error: Option<String>,
    domain_binding: Option<tools::domain_tools::DomainToolRuntimeBinding>,
    domain_runtime_error: Option<String>,
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
            index_binding: None,
            index_runtime: None,
            index_runtime_error: None,
            domain_binding: None,
            domain_runtime_error: None,
        }
    }

    pub fn with_web_run_runtime(mut self, web_run: tools::WebRunRuntime) -> Self {
        self.web_run = Some(web_run);
        self
    }

    /// Attach the one typed FileStore Index domain runtime for a migrated
    /// unit. Absence is intentional: legacy roles never gain a fallback path
    /// to Index persistence.
    pub fn with_index_tool_runtime(
        mut self,
        binding: tools::index_tools::IndexToolRuntimeBinding,
    ) -> Self {
        self.index_binding = Some(binding);
        self
    }

    /// Attach the one typed FileStore business-domain runtime for a migrated
    /// unit.  Absence is intentional: legacy jobs can never invoke a domain
    /// writer as an implicit fallback.
    pub fn with_domain_tool_runtime(
        mut self,
        binding: tools::domain_tools::DomainToolRuntimeBinding,
    ) -> Self {
        self.domain_binding = Some(binding);
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
        self.index_runtime = None;
        self.index_runtime_error = None;
        self.domain_runtime_error = None;
        if let Some(binding) = &self.index_binding {
            match binding.build(context.clone()) {
                Ok(runtime) => self.index_runtime = Some(runtime),
                Err(error) => self.index_runtime_error = Some(error.to_string()),
            }
        }
        if let Some(binding) = &self.domain_binding {
            if let Err(error) = binding.set_turn_context(&context) {
                self.domain_runtime_error = Some(error.to_string());
            }
        }
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
        let index_runtime = self.index_runtime.as_ref();
        let index_runtime_error = self.index_runtime_error.as_deref();
        let domain_binding = self.domain_binding.as_ref();
        let domain_runtime_error = self.domain_runtime_error.as_deref();
        Box::pin(async move {
            debug!(
                call_id = call.call_id,
                tool = call.name,
                "project tool runtime dispatching tool"
            );
            let web_run_config = web_run.as_ref().map(tools::WebRunRuntime::config);
            let configured = available_tools.iter().any(|name| name == &call.name);
            let is_index_tool = matches!(
                call.name.as_str(),
                tools::CREATE_INDEX_TOOL_NAME
                    | tools::APPEND_INDEX_DETAIL_TOOL_NAME
                    | tools::FINALIZE_INDEX_TOOL_NAME
                    | tools::READ_INDEXES_TOOL_NAME
                    | tools::READ_INDEX_DETAILS_TOOL_NAME
            );
            let is_domain_tool = tools::domain_tools::is_domain_tool(&call.name);
            let enabled = call.name == "think"
                || tools::enabled_tool_names(
                    web_run_config,
                    config.alpaca_live,
                    config.alpaca_market_data,
                )
                .contains(&call.name.as_str())
                || (is_index_tool && index_runtime.is_some())
                || (is_domain_tool && domain_binding.is_some());
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
            if is_index_tool {
                let output = match (index_runtime, index_runtime_error) {
                    (Some(runtime), _) => runtime.execute(&name, call.arguments),
                    (None, Some(error)) => Err(anyhow::anyhow!(error.to_owned())),
                    (None, None) => Err(anyhow::anyhow!(
                        "Index tools require a migrated FileStore runtime binding"
                    )),
                };
                return match output {
                    Ok(output) => ToolResultItem {
                        call_id,
                        name,
                        status: "completed".to_owned(),
                        output,
                        error: None,
                    },
                    Err(error) => ToolResultItem {
                        call_id,
                        name,
                        status: "error".to_owned(),
                        output: Value::Null,
                        error: Some(error.to_string()),
                    },
                };
            }
            if is_domain_tool {
                let output = match (domain_binding, domain_runtime_error) {
                    (_, Some(error)) => Err(anyhow::anyhow!(error.to_owned())),
                    (Some(binding), None) => binding.execute(&name, call.arguments),
                    (None, None) => Err(anyhow::anyhow!(
                        "domain tools require a migrated FileStore runtime binding"
                    )),
                };
                return match output {
                    Ok(output) => ToolResultItem {
                        call_id,
                        name,
                        status: "completed".to_owned(),
                        output,
                        error: None,
                    },
                    Err(error) => ToolResultItem {
                        call_id,
                        name,
                        status: "error".to_owned(),
                        output: Value::Null,
                        error: Some(error.to_string()),
                    },
                };
            }
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
                    if let (Some(binding), Some(context)) = (domain_binding, turn_context.as_ref())
                    {
                        if let Err(error) =
                            tools::evidence_reads_from_tool_output(&name, &output, context)
                                .and_then(|reads| {
                                    for read in reads {
                                        binding.record_evidence_read(read)?;
                                    }
                                    Ok(())
                                })
                        {
                            warn!(call_id, tool = name, error = %error, "project evidence read persistence failed");
                            return ToolResultItem {
                                call_id,
                                name,
                                status: "error".to_string(),
                                output: Value::Null,
                                error: Some(error.to_string()),
                            };
                        }
                    }
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
