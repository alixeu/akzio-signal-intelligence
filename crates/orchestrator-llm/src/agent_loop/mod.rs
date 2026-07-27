mod debug_capture;
mod streaming;
mod types;

pub use debug_capture::input_to_debug_messages;
pub use types::*;

use streaming::ModelStreamHandler;

use anyhow::{bail, Context, Result};
use orchestrator_core;
use orchestrator_sql::{turn_history_items, upsert_agent_turn, AgentTurnInput};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    path::PathBuf,
    pin::Pin,
    time::Instant,
};
use tracing::{debug, warn};

#[cfg(test)]
use std::collections::VecDeque;

use crate::llm_judge::{judge_message_status, JudgeConfig};
use crate::tools::{self, truncate_chars};
use crate::truncation::{truncate_semantic, TruncationConfig};
use crate::AgentSettings;

const DEFAULT_MAX_AGENT_LOOPS: usize = 8;
const SYSTEM_PROMPT_TEMPLATE: &str = include_str!("../../../../prompts/system/agent_loop.md");
const REQUEST_WRAPPER_TEMPLATE: &str =
    include_str!("../../../../prompts/system/messages/request_wrapper.md");
const ARTIFACT_RETRY_INSTRUCTION: &str =
    include_str!("../../../../prompts/system/messages/artifact_retry.md");
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
                persist_turn(conn, turn, &config.truncation)?;
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
            let visible_summary_ids = visible_summary_ids_from_history(turn);
            let tool_batch_started = Instant::now();
            let mut terminal_completed = false;
            let mut calls = calls.into_iter();
            while let Some(call) = calls.next() {
                emit_tool_call_status(turn, sink, &call, AgentItemStatus::Running).await?;
                let call_id = call.call_id.clone();
                let name = call.name.clone();
                let tool_started = Instant::now();
                let result = if name == tools::READ_PHASE_SUMMARY_DETAILS_TOOL_NAME {
                    let summary_id = call
                        .arguments
                        .get("summary_id")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    if !visible_summary_ids.contains(summary_id) {
                        ToolResultItem {
                            call_id: call.call_id,
                            name,
                            status: "error".to_string(),
                            output: json!({
                                "status": "not_visible",
                                "item_count": 0,
                                "items": [],
                                "source_policy": "list_before_detail_required"
                            }),
                            error: Some(
                                "summary is not visible in a prior read_phase_summaries result"
                                    .to_string(),
                            ),
                        }
                    } else {
                        debug!(
                            call_id = call_id,
                            tool = name,
                            "agent loop tool call starting"
                        );
                        tools.execute(call).await
                    }
                } else {
                    debug!(
                        call_id = call_id,
                        tool = name,
                        "agent loop tool call starting"
                    );
                    tools.execute(call).await
                };
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
            persist_turn(conn, turn, &config.truncation)?;
            if terminal_completed {
                turn.end_reason = Some("terminal_tool".to_string());
                persist_turn(conn, turn, &config.truncation)?;
                return Ok(aggregate_result);
            }
            if loop_index >= 3 && !config.require_terminal_tool {
                turn.tools_disabled = true;
                turn.push_pending_input(FINALIZE_INSTRUCTION);
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

        if config.require_terminal_tool {
            if !retrieval_retry_queued {
                retrieval_retry_queued = true;
                turn.push_pending_input(
                    "No terminal finalize tool succeeded. Use the assigned finalize tool now; do not provide a prose answer.",
                );
                turn.needs_follow_up = true;
                persist_turn(conn, turn, &config.truncation)?;
                continue;
            }
            bail!("tool-managed agent ended without a successful terminal finalize tool");
        }

        if turn.needs_follow_up {
            turn.needs_follow_up = false;
            persist_turn(conn, turn, &config.truncation)?;
            continue;
        }

        if let Some(text) = last_assistant_message_text(turn) {
            if let Err(error) = validate_retrieval_policy(turn, &config.retrieval_policy, &text) {
                if !retrieval_retry_queued {
                    retrieval_retry_queued = true;
                    queue_artifact_retry(turn, &format!("retrieval policy: {error}"));
                    persist_turn(conn, turn, &config.truncation)?;
                    continue;
                }
                bail!("retrieval policy remained invalid after one repair retry: {error}");
            }
            if turn.role.starts_with("analyst.") {
                if let Err(error) =
                    analyst_final_artifact_validation_error(&turn.role, &turn_tickers(turn), &text)
                {
                    warn!(role = turn.role, error = %error, "analyst final artifact rejected");
                    queue_artifact_retry(turn, &error);
                    persist_turn(conn, turn, &config.truncation)?;
                    continue;
                }
            }
            if seed_packet_role(&turn.role) && text.trim() != "准备完毕" {
                if let Err(error) = seed_packet_validation_error(&turn.role, &text) {
                    queue_artifact_retry(turn, &error);
                    persist_turn(conn, turn, &config.truncation)?;
                    continue;
                }
            }
            if turn.role == "manager.research" {
                if let Err(error) = research_artifact_validation_error(&turn_tickers(turn), &text) {
                    queue_artifact_retry(turn, &error);
                    persist_turn(conn, turn, &config.truncation)?;
                    continue;
                }
            }
            if turn.role == "trader" {
                if let Err(error) = trade_intent_validation_error(&text) {
                    queue_artifact_retry(turn, &error);
                    persist_turn(conn, turn, &config.truncation)?;
                    continue;
                }
            }
            if turn.role.starts_with("risk.") {
                if let Err(error) = risk_constraints_validation_error(&turn.role, &text) {
                    queue_artifact_retry(turn, &error);
                    persist_turn(conn, turn, &config.truncation)?;
                    continue;
                }
            }
            if interaction_packet_role(&turn.role) {
                if let Err(error) = interaction_packet_validation_error(&turn.role, &text) {
                    queue_artifact_retry(turn, &error);
                    persist_turn(conn, turn, &config.truncation)?;
                    continue;
                }
            }
            if turn.role == "mediator.topic_controller" {
                if let Err(error) = controller_packet_validation_error(&text) {
                    queue_artifact_retry(turn, &error);
                    persist_turn(conn, turn, &config.truncation)?;
                    continue;
                }
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

fn visible_summary_ids_from_history(turn: &Turn) -> BTreeSet<String> {
    turn.emitted_items
        .iter()
        .filter(|item| {
            item.item_type == TurnItemType::ToolResult
                && item.tool_name == tools::READ_PHASE_SUMMARIES_TOOL_NAME
        })
        .filter_map(tool_result_output)
        .filter_map(|output| output.get("items").and_then(Value::as_array).cloned())
        .flatten()
        .filter_map(|summary| {
            summary
                .get("id")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .collect()
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
                    tools::READ_PHASE_SUMMARIES_TOOL_NAME
                        | tools::READ_PHASE_SUMMARY_DETAILS_TOOL_NAME
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
                if item.tool_name == tools::READ_PHASE_SUMMARIES_TOOL_NAME {
                    summary_query_count += 1;
                    summary_filters.push(arguments.clone());
                } else if item.tool_name == tools::READ_PHASE_SUMMARY_DETAILS_TOOL_NAME {
                    detail_call_count += 1;
                    if let Some(summary_id) = arguments.get("summary_id").and_then(Value::as_str) {
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
                if item.tool_name == tools::READ_PHASE_SUMMARIES_TOOL_NAME {
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
                            if let Some(id) = summary.get("id").and_then(Value::as_str) {
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
                } else if item.tool_name == tools::READ_PHASE_SUMMARY_DETAILS_TOOL_NAME {
                    any_truncated |= output
                        .get("truncated")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    if let Some(items) = output.get("items").and_then(Value::as_array) {
                        for detail in items {
                            if let Some(id) = detail.get("id").and_then(Value::as_str) {
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

fn validate_retrieval_policy(
    turn: &Turn,
    policy: &RetrievalPolicy,
    final_text: &str,
) -> std::result::Result<(), String> {
    if !policy.mandatory_summary_query
        && policy.required_source_phases.is_empty()
        && policy.maximum_detail_expansions == 0
    {
        return Ok(());
    }
    let audit = retrieval_audit(turn);
    let summary_query_count = audit["successful_summary_query_count"]
        .as_u64()
        .unwrap_or(0) as usize;
    let visible_summary_count = audit["visible_summary_count"].as_u64().unwrap_or(0) as usize;
    if policy.mandatory_summary_query && summary_query_count == 0 {
        return Err("read_phase_summaries was not called".to_string());
    }
    let filters = audit["successful_summary_filters"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    for phase in &policy.required_source_phases {
        let queried = filters
            .iter()
            .any(|args| args.get("source_phase").and_then(Value::as_i64) == Some(*phase));
        if !queried {
            return Err(format!(
                "read_phase_summaries(source_phase={phase}) is required"
            ));
        }
    }
    let expanded_source_phases = audit["expanded_source_phases"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_i64)
        .collect::<BTreeSet<_>>();
    let visible_source_phases = audit["visible_source_phases"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_i64)
        .collect::<BTreeSet<_>>();
    for phase in &policy.required_detail_source_phases {
        if visible_source_phases.contains(phase) && !expanded_source_phases.contains(phase) {
            return Err(format!(
                "at least one detail from source_phase={phase} is required when summaries are visible"
            ));
        }
    }
    for args in &filters {
        let limit = args.get("limit").and_then(Value::as_u64).unwrap_or(20) as usize;
        if policy.summary_page_limit > 0 && limit > policy.summary_page_limit {
            return Err(format!(
                "summary page limit exceeded: maximum {}, got {limit}",
                policy.summary_page_limit
            ));
        }
    }
    let largest_detail_limit = audit["calls"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|call| {
            call.get("tool").and_then(Value::as_str)
                == Some(tools::READ_PHASE_SUMMARY_DETAILS_TOOL_NAME)
        })
        .filter_map(|call| call.pointer("/arguments/limit").and_then(Value::as_u64))
        .max();
    if let Some(limit) = largest_detail_limit {
        if policy.detail_page_limit > 0 && limit as usize > policy.detail_page_limit {
            return Err(format!(
                "detail page limit exceeded: maximum {}, got {limit}",
                policy.detail_page_limit
            ));
        }
    }
    let expanded = audit["expanded_summary_ids"]
        .as_array()
        .map(Vec::len)
        .unwrap_or(0);
    if visible_summary_count > 0 && expanded < policy.minimum_detail_expansions {
        return Err(format!(
            "at least {} relevant summary detail expansions are required when summaries are visible; got {expanded}",
            policy.minimum_detail_expansions
        ));
    }
    if policy.maximum_detail_expansions > 0 && expanded > policy.maximum_detail_expansions {
        return Err(format!(
            "detail expansion budget exceeded: maximum {}, got {expanded}",
            policy.maximum_detail_expansions
        ));
    }
    if let Some(ids) = audit["detail_requested_before_visible_index"].as_array() {
        if !ids.is_empty() {
            return Err(format!(
                "summary details were requested before their ids appeared in a visible summary index: {}",
                Value::Array(ids.clone())
            ));
        }
    }
    if final_text.trim() == "准备完毕" {
        return Ok(());
    }
    let artifact = extract_json_value(final_text).map_err(|error| error.to_string())?;
    let mut referenced = BTreeSet::new();
    collect_evidence_ids(&artifact, &mut referenced);
    let readable = audit["returned_summary_ids"]
        .as_array()
        .into_iter()
        .flatten()
        .chain(audit["read_detail_ids"].as_array().into_iter().flatten())
        .filter_map(Value::as_str)
        .collect::<BTreeSet<_>>();
    let unread = referenced
        .iter()
        .filter(|id| !readable.contains(id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !unread.is_empty() {
        return Err(format!(
            "artifact references summary/detail ids not read in this session: {}",
            unread.join(", ")
        ));
    }
    if turn.role == "mediator.topic"
        && visible_summary_count > 0
        && artifact
            .get("topics")
            .and_then(Value::as_array)
            .is_some_and(|topics| !topics.is_empty())
        && expanded == 0
    {
        return Err(
            "topic generation with visible summaries requires one relevant detail expansion"
                .to_string(),
        );
    }
    Ok(())
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

fn collect_evidence_ids(value: &Value, ids: &mut BTreeSet<String>) {
    match value {
        Value::String(text)
            if text.len() == 32 && text.bytes().all(|byte| byte.is_ascii_hexdigit()) =>
        {
            ids.insert(text.to_ascii_lowercase());
        }
        Value::Array(items) => {
            for item in items {
                collect_evidence_ids(item, ids);
            }
        }
        Value::Object(map) => {
            for value in map.values() {
                collect_evidence_ids(value, ids);
            }
        }
        _ => {}
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
    conn: &rusqlite::Connection,
    turn: &Turn,
    truncation: &TruncationConfig,
) -> Result<()> {
    let force_history_checkpoint = turn
        .model_context
        .lines()
        .any(|line| line == "history_fork=true");
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
            full_context_json: json!(full_context.clone()),
            summary,
        },
    )?;
    if force_history_checkpoint {
        conn.execute(
            "UPDATE agent_events SET full_context_json = ?1, context_delta_json = '[]' WHERE turn_id = ?2",
            rusqlite::params![serde_json::to_string(&full_context)?, turn.turn_id],
        )?;
    }
    Ok(())
}

pub(super) fn update_turn_item(
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

fn preseed_tool_calls(
    turn: &Turn,
    tickers: &[String],
    available_tools: &[String],
) -> Vec<ToolCallRequest> {
    let mut calls = Vec::new();
    let tool_enabled = |name: &str| available_tools.iter().any(|tool| tool == name);
    if tool_enabled(tools::READ_EXPERIENCE_TOOL_NAME)
        && turn.phase.is_some_and(|phase| (1..=6).contains(&phase))
        && turn.role != "compressor.phase_summary"
    {
        for ticker in tickers {
            calls.push(ToolCallRequest {
                call_id: format!("preseed-experience-{}", ticker.to_lowercase()),
                name: tools::READ_EXPERIENCE_TOOL_NAME.to_string(),
                arguments: json!({ "ticker": ticker }),
            });
        }
    }
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

/// Map plain assistant text (stream/text output) into a ModelResponse.
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
        raw: json!({"source": "plain_text"}),
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

fn seed_packet_role(role: &str) -> bool {
    matches!(role, "researcher.bull.initial" | "researcher.bear.initial")
}

fn queue_artifact_retry(turn: &mut Turn, validation_error: &str) {
    let tickers = turn_tickers(turn);
    let expected_tickers = if tickers.is_empty() {
        "[]".to_string()
    } else {
        tickers.join(", ")
    };
    turn.tools_disabled = true;
    turn.push_pending_input(
        ARTIFACT_RETRY_INSTRUCTION
            .replace("{expected_role}", &turn.role)
            .replace("{expected_tickers}", &expected_tickers)
            .replace(
                "{validation_errors}",
                &truncate_chars(validation_error, 2_000),
            ),
    );
    turn.needs_follow_up = true;
}

#[cfg(test)]
fn seed_packet_looks_valid(role: &str, text: &str) -> bool {
    seed_packet_validation_error(role, text).is_ok()
}

fn seed_packet_validation_error(role: &str, text: &str) -> std::result::Result<(), String> {
    let value = artifact_json(text)?;
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
    require_equal_string(&value, "role", role)?;
    require_equal_string(&value, "artifact_type", expected)?;
    require_non_empty_string(&value, "topic_id")?;
    let claims = require_array(&value, "claims")?;
    if !(1..=2).contains(&claims.len()) {
        return Err("claims must contain 1-2 items".to_string());
    }
    for (index, claim) in claims.iter().enumerate() {
        let path = format!("claims[{index}]");
        require_non_empty_string_at(claim, &path, "claim_id")?;
        require_non_empty_string_at(claim, &path, "decision_hinge")?;
        require_non_empty_string_at(claim, &path, "claim")?;
        require_array_at(claim, &path, "evidence_refs")?;
        require_probability_at(claim, &path, "confidence")?;
        require_non_empty_string_at(claim, &path, constraint_field)?;
        if claim
            .get("needs_mediator_check")
            .and_then(Value::as_bool)
            .is_none()
        {
            return Err(format!("{path}.needs_mediator_check must be boolean"));
        }
    }
    require_non_empty_string(&value, "summary")?;
    require_object(&value, "reducer_checks")?;
    Ok(())
}

fn interaction_packet_role(role: &str) -> bool {
    role.contains(".interaction")
}

#[cfg(test)]
fn interaction_packet_looks_valid(role: &str, text: &str) -> bool {
    interaction_packet_validation_error(role, text).is_ok()
}

fn interaction_packet_validation_error(role: &str, text: &str) -> std::result::Result<(), String> {
    let value = artifact_json(text)?;
    let expected_type = if role == "researcher.bull.interaction" {
        "bull_debate_packet"
    } else if role == "researcher.bear.interaction" {
        "bear_debate_packet"
    } else {
        return Err(format!("unsupported interaction role {role}"));
    };
    if assistant_message_needs_follow_up(text) {
        return Err("response is still a planning or waiting message".to_string());
    }
    require_equal_string(&value, "role", role)?;
    require_equal_string(&value, "artifact_type", expected_type)?;
    for field in [
        "topic_id",
        "reply_to_claim_id",
        "steer_id",
        "claim",
        "send_to_mediator",
    ] {
        require_non_empty_string(&value, field)?;
    }
    let stance = value
        .get("stance")
        .and_then(Value::as_str)
        .ok_or_else(|| "stance must be a string".to_string())?;
    if !matches!(
        stance,
        "accept" | "rebut" | "downgrade" | "needs_evidence" | "no_new_info"
    ) {
        return Err(
            "stance must be accept, rebut, downgrade, needs_evidence, or no_new_info".to_string(),
        );
    }
    require_array(&value, "evidence_refs")?;
    require_probability(&value, "confidence")?;
    require_array(&value, "blocked_ack")?;
    if stance != "no_new_info" {
        require_object(&value, "steelman")?;
    }
    Ok(())
}

#[cfg(test)]
fn controller_packet_looks_valid(text: &str) -> bool {
    controller_packet_validation_error(text).is_ok()
}

fn controller_packet_validation_error(text: &str) -> std::result::Result<(), String> {
    let value = artifact_json(text)?;
    if assistant_message_needs_follow_up(text) {
        return Err("response is still a planning or waiting message".to_string());
    }
    require_equal_string(&value, "role", "mediator.topic_controller")?;
    require_equal_string(&value, "artifact_type", "topic_controller_packet")?;
    require_non_empty_string(&value, "topic_id")?;
    for field in [
        "claim_ledger",
        "accepted_for_opponent",
        "rejected_to_origin",
        "blocked_claims",
        "agreed_facts",
        "decision_hinges",
    ] {
        require_array(&value, field)?;
    }
    for field in ["next_steers", "topic_summary_delta", "reducer_checks"] {
        require_object(&value, field)?;
    }
    for (index, hinge) in require_array(&value, "decision_hinges")?.iter().enumerate() {
        let path = format!("decision_hinges[{index}]");
        require_non_empty_string_at(hinge, &path, "hinge")?;
        if require_array_at(hinge, &path, "evidence_refs")?.is_empty() {
            return Err(format!("{path}.evidence_refs must not be empty"));
        }
    }
    require_probability(&value, "info_gain_score")?;
    let soft_control = value
        .get("soft_control")
        .ok_or_else(|| "soft_control must be an object".to_string())?;
    if soft_control
        .get("should_continue")
        .and_then(Value::as_bool)
        .is_none()
    {
        return Err("soft_control.should_continue must be boolean".to_string());
    }
    require_non_empty_string_at(soft_control, "soft_control", "stop_reason")?;
    Ok(())
}

fn artifact_json(text: &str) -> std::result::Result<Value, String> {
    extract_json_value(text).map_err(|error| format!("JSON artifact: {error:#}"))
}

fn require_equal_string(
    value: &Value,
    field: &str,
    expected: &str,
) -> std::result::Result<(), String> {
    if value.get(field).and_then(Value::as_str) == Some(expected) {
        Ok(())
    } else {
        Err(format!("{field} must equal {expected}"))
    }
}

fn require_non_empty_string(value: &Value, field: &str) -> std::result::Result<(), String> {
    require_non_empty_string_at(value, "", field)
}

fn require_non_empty_string_at(
    value: &Value,
    path: &str,
    field: &str,
) -> std::result::Result<(), String> {
    if non_empty_string_field(value, field) {
        Ok(())
    } else if path.is_empty() {
        Err(format!("{field} must be a non-empty string"))
    } else {
        Err(format!("{path}.{field} must be a non-empty string"))
    }
}

fn require_array<'a>(value: &'a Value, field: &str) -> std::result::Result<&'a Vec<Value>, String> {
    require_array_at(value, "", field)
}

fn require_array_at<'a>(
    value: &'a Value,
    path: &str,
    field: &str,
) -> std::result::Result<&'a Vec<Value>, String> {
    value.get(field).and_then(Value::as_array).ok_or_else(|| {
        if path.is_empty() {
            format!("{field} must be an array")
        } else {
            format!("{path}.{field} must be an array")
        }
    })
}

fn require_object(value: &Value, field: &str) -> std::result::Result<(), String> {
    value
        .get(field)
        .and_then(Value::as_object)
        .map(|_| ())
        .ok_or_else(|| format!("{field} must be an object"))
}

fn require_probability(value: &Value, field: &str) -> std::result::Result<(), String> {
    require_probability_at(value, "", field)
}

fn require_probability_at(
    value: &Value,
    path: &str,
    field: &str,
) -> std::result::Result<(), String> {
    if value
        .get(field)
        .and_then(Value::as_f64)
        .is_some_and(|number| (0.0..=1.0).contains(&number))
    {
        Ok(())
    } else if path.is_empty() {
        Err(format!("{field} must be a number in [0, 1]"))
    } else {
        Err(format!("{path}.{field} must be a number in [0, 1]"))
    }
}

fn non_empty_string_field(value: &Value, field: &str) -> bool {
    value
        .get(field)
        .and_then(Value::as_str)
        .is_some_and(|field| !field.trim().is_empty())
}

fn trade_intent_validation_error(text: &str) -> std::result::Result<(), String> {
    let value = artifact_json(text)?;
    if value.get("per_ticker").and_then(Value::as_object).is_some() {
        let per_ticker = value["per_ticker"].as_object().expect("checked object");
        let mut errors = Vec::new();
        for (ticker, entry) in per_ticker {
            match trade_intent_entry_validation_error(entry) {
                Ok(()) => return Ok(()),
                Err(error) => errors.push(format!("per_ticker.{ticker}: {error}")),
            }
        }
        return Err(errors
            .into_iter()
            .next()
            .unwrap_or_else(|| "per_ticker must contain a trade intent".to_string()));
    }
    trade_intent_entry_validation_error(&value)
}

fn trade_intent_entry_validation_error(value: &Value) -> std::result::Result<(), String> {
    let action = value.get("action").and_then(Value::as_str);
    let position_cap = value
        .get("position_size")
        .and_then(Value::as_str)
        .and_then(position_upper_bound);
    if !matches!(action, Some("Buy" | "Sell" | "Hold")) {
        return Err("action must be Buy, Sell, or Hold".to_string());
    }
    if position_cap.is_none() {
        return Err("position_size must contain a percentage range".to_string());
    }
    if action == Some("Hold") && position_cap.is_some_and(|cap| cap > f64::EPSILON) {
        return Err("Hold requires a zero position_size".to_string());
    }
    require_non_empty_string(value, "rationale")
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

#[cfg(test)]
fn risk_constraints_look_valid(role: &str, text: &str) -> bool {
    risk_constraints_validation_error(role, text).is_ok()
}

fn risk_constraints_validation_error(role: &str, text: &str) -> std::result::Result<(), String> {
    let value = artifact_json(text)?;
    let expected_stance = role.strip_prefix("risk.").unwrap_or_default();
    require_equal_string(&value, "stance", expected_stance)?;
    require_non_empty_string(&value, "argument")?;
    if !non_empty_string_field(&value, "unique_risk_contribution")
        && value.get("no_new_information").and_then(Value::as_bool) != Some(true)
    {
        return Err(
            "unique_risk_contribution must be non-empty unless no_new_information is true"
                .to_string(),
        );
    }
    for field in ["disagreement_with_prior", "recommended_adjustment"] {
        require_non_empty_string(&value, field)?;
    }
    let stop_type = value
        .get("stop_type")
        .and_then(Value::as_str)
        .ok_or_else(|| "stop_type must be a string".to_string())?;
    if !matches!(stop_type, "none" | "soft" | "hard") {
        return Err("stop_type is not a supported value".to_string());
    }
    for field in [
        "max_drawdown_pct",
        "position_cap_pct",
        "constraint_confidence",
    ] {
        require_probability(&value, field)?;
    }
    for field in [
        "rebalance_trigger",
        "risk_off_trigger",
        "review_window",
        "cash_hedge_recommendation",
    ] {
        require_non_empty_string(&value, field)?;
    }
    Ok(())
}

#[cfg(test)]
fn research_artifact_looks_valid(tickers: &[String], text: &str) -> bool {
    research_artifact_validation_error(tickers, text).is_ok()
}

fn research_artifact_validation_error(
    tickers: &[String],
    text: &str,
) -> std::result::Result<(), String> {
    let value = artifact_json(text)?;
    let normalized = orchestrator_core::normalize_research_artifact_value(value, &[])
        .map_err(|error| format!("research artifact normalization: {error:#}"))?;
    let artifact = serde_json::from_value::<orchestrator_core::ResearchArtifact>(normalized)
        .map_err(|error| format!("research artifact schema: {error}"))?;
    orchestrator_core::validate_research_artifact(&artifact, tickers)
        .map_err(|error| format!("research artifact validation: {error:#}"))
}

#[cfg(test)]
fn analyst_final_artifact_looks_valid(role: &str, expected_tickers: &[String], text: &str) -> bool {
    analyst_final_artifact_validation_error(role, expected_tickers, text).is_ok()
}

fn analyst_final_artifact_validation_error(
    role: &str,
    expected_tickers: &[String],
    text: &str,
) -> std::result::Result<(), String> {
    let value = extract_json_value(text).map_err(|error| error.to_string())?;
    let value =
        crate::normalize_analyst_artifact_value(value).map_err(|error| format!("{error:#}"))?;
    if value.get("id").and_then(Value::as_str) != Some(role)
        || value.get("role").and_then(Value::as_str) != Some(role)
    {
        return Err(format!("id and role must both equal {role}"));
    }
    let per_ticker = value
        .get("per_ticker")
        .and_then(Value::as_object)
        .ok_or_else(|| "per_ticker must be an object".to_string())?;
    let expected = expected_tickers
        .iter()
        .collect::<std::collections::BTreeSet<_>>();
    let actual = per_ticker.keys().collect::<std::collections::BTreeSet<_>>();
    if actual != expected {
        return Err(format!(
            "per_ticker keys must equal {expected:?}, got {actual:?}"
        ));
    }
    for (ticker, payload) in per_ticker {
        let payload =
            serde_json::from_value::<orchestrator_core::AnalystTickerArtifact>(payload.clone())
                .map_err(|error| format!("per_ticker.{ticker}: {error}"))?;
        orchestrator_core::validate_analyst_ticker_artifact(&payload)
            .map_err(|error| format!("per_ticker.{ticker}: {error}"))?;
    }
    Ok(())
}

pub fn classify_assistant_message(text: &str, turn_status: TurnStatus) -> FollowUpDecision {
    if text.trim() == "准备完毕" {
        return FollowUpDecision::Final;
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retrieval_audit_tracks_real_ids_filters_duplicates_and_truncation() {
        let summary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let detail_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let mut turn = Turn::new("turn", "session", "run", "trader", "");
        let list = ToolCallRequest {
            call_id: "list-1".to_string(),
            name: tools::READ_PHASE_SUMMARIES_TOOL_NAME.to_string(),
            arguments: json!({"source_phase": 3, "ticker": "QQQ"}),
        };
        turn.emitted_items.push(TurnItem::tool_call(&list));
        turn.emitted_items.push(TurnItem::tool_result(
            &ToolResultItem {
                call_id: list.call_id,
                name: list.name,
                status: "completed".to_string(),
                output: json!({
                    "item_count": 1,
                    "total_count": 2,
                    "truncated": true,
                    "items": [{"id": summary_id, "source_phase": 3}]
                }),
                error: None,
            },
            &TruncationConfig::default(),
        ));
        for call_id in ["detail-1", "detail-2"] {
            let call = ToolCallRequest {
                call_id: call_id.to_string(),
                name: tools::READ_PHASE_SUMMARY_DETAILS_TOOL_NAME.to_string(),
                arguments: json!({"summary_id": summary_id}),
            };
            turn.emitted_items.push(TurnItem::tool_call(&call));
            turn.emitted_items.push(TurnItem::tool_result(
                &ToolResultItem {
                    call_id: call.call_id,
                    name: call.name,
                    status: "completed".to_string(),
                    output: json!({
                        "item_count": 1,
                        "items": [{"id": detail_id, "summary_id": summary_id}]
                    }),
                    error: None,
                },
                &TruncationConfig::default(),
            ));
        }
        let audit = retrieval_audit(&turn);
        assert_eq!(audit["summary_query_count"], 1);
        assert_eq!(audit["detail_call_count"], 2);
        assert_eq!(audit["duplicate_retrieval_count"], 1);
        assert_eq!(audit["tool_result_truncated"], true);
        assert_eq!(audit["returned_summary_ids"][0], summary_id);
        assert_eq!(audit["read_detail_ids"][0], detail_id);
        assert_eq!(audit["expanded_source_phases"][0], 3);
    }

    #[test]
    fn retrieval_policy_rejects_unread_artifact_ids() {
        let mut turn = Turn::new("turn", "session", "run", "trader", "");
        let list = ToolCallRequest {
            call_id: "list".to_string(),
            name: tools::READ_PHASE_SUMMARIES_TOOL_NAME.to_string(),
            arguments: json!({"source_phase": 3}),
        };
        turn.emitted_items.push(TurnItem::tool_call(&list));
        turn.emitted_items.push(TurnItem::tool_result(
            &ToolResultItem {
                call_id: list.call_id,
                name: list.name,
                status: "completed".to_string(),
                output: json!({"item_count": 0, "items": []}),
                error: None,
            },
            &TruncationConfig::default(),
        ));
        let policy = RetrievalPolicy {
            mandatory_summary_query: true,
            required_source_phases: vec![3],
            allow_empty_when_no_visible_summary: true,
            ..Default::default()
        };
        let error = validate_retrieval_policy(
            &turn,
            &policy,
            r#"{"evidence_refs":["cccccccccccccccccccccccccccccccc"]}"#,
        )
        .unwrap_err();
        assert!(error.contains("not read"));
    }

    #[test]
    fn retrieval_policy_accepts_warmup_ready_after_required_query() {
        let mut turn = Turn::new("turn", "session", "run", "mediator.topic", "");
        let list = ToolCallRequest {
            call_id: "list".to_string(),
            name: tools::READ_PHASE_SUMMARIES_TOOL_NAME.to_string(),
            arguments: json!({"source_phase": 1}),
        };
        turn.emitted_items.push(TurnItem::tool_call(&list));
        turn.emitted_items.push(TurnItem::tool_result(
            &ToolResultItem {
                call_id: list.call_id,
                name: list.name,
                status: "completed".to_string(),
                output: json!({"item_count": 0, "items": []}),
                error: None,
            },
            &TruncationConfig::default(),
        ));
        let policy = RetrievalPolicy {
            mandatory_summary_query: true,
            required_source_phases: vec![1],
            allow_empty_when_no_visible_summary: true,
            ..Default::default()
        };

        assert!(validate_retrieval_policy(&turn, &policy, "准备完毕").is_ok());
    }

    #[test]
    fn retrieval_policy_requires_details_from_each_visible_required_phase() {
        let summary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut turn = Turn::new("turn", "session", "run", "risk.neutral", "");
        for (call_id, source_phase) in [("list-3", 3), ("list-4", 4)] {
            let call = ToolCallRequest {
                call_id: call_id.to_string(),
                name: tools::READ_PHASE_SUMMARIES_TOOL_NAME.to_string(),
                arguments: json!({"source_phase": source_phase}),
            };
            turn.emitted_items.push(TurnItem::tool_call(&call));
            turn.emitted_items.push(TurnItem::tool_result(
                &ToolResultItem {
                    call_id: call.call_id,
                    name: call.name,
                    status: "completed".to_string(),
                    output: json!({
                        "item_count": 1,
                        "items": [{"id": format!("{summary_id}{source_phase}"), "source_phase": source_phase}]
                    }),
                    error: None,
                },
                &TruncationConfig::default(),
            ));
        }
        let phase3_id = format!("{summary_id}3");
        let detail = ToolCallRequest {
            call_id: "detail-3".to_string(),
            name: tools::READ_PHASE_SUMMARY_DETAILS_TOOL_NAME.to_string(),
            arguments: json!({"summary_id": phase3_id}),
        };
        turn.emitted_items.push(TurnItem::tool_call(&detail));
        let policy = RetrievalPolicy {
            mandatory_summary_query: true,
            required_source_phases: vec![3, 4],
            required_detail_source_phases: vec![3, 4],
            minimum_detail_expansions: 2,
            maximum_detail_expansions: 4,
            ..Default::default()
        };

        let error = validate_retrieval_policy(&turn, &policy, "{}").unwrap_err();
        assert!(error.contains("source_phase=4"));
    }

    #[test]
    fn phase_summary_retrieval_is_never_preseeded() {
        let mut warmup = Turn::new(
            "turn-warmup",
            "session",
            "run",
            "mediator.topic",
            "role prompt",
        );
        warmup.push_pending_input(r#"Steer: {"kind":"warmup"}"#);
        let calls = preseed_tool_calls(
            &warmup,
            &["QQQ".to_string()],
            &[tools::READ_PHASE_SUMMARIES_TOOL_NAME.to_string()],
        );
        assert!(calls.is_empty());

        let mut topic = Turn::new(
            "turn-topic",
            "session",
            "run",
            "mediator.topic",
            "role prompt",
        );
        topic.push_pending_input(r#"Steer: {"kind":"topic_generation"}"#);
        assert!(preseed_tool_calls(
            &topic,
            &["QQQ".to_string()],
            &[tools::READ_PHASE_SUMMARIES_TOOL_NAME.to_string()],
        )
        .is_empty());
    }

    #[test]
    fn preseed_only_uses_tools_enabled_for_the_role() {
        let mut analyst = Turn::new(
            "turn-1",
            "session",
            "run",
            "analyst.technical",
            "role prompt",
        );
        analyst.phase = Some(1);
        assert!(preseed_tool_calls(&analyst, &["QQQ".to_string()], &[]).is_empty());

        let calls = preseed_tool_calls(
            &analyst,
            &["QQQ".to_string()],
            &[
                tools::READ_EXPERIENCE_TOOL_NAME.to_string(),
                "read_technical_snapshot".to_string(),
            ],
        );
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].name, tools::READ_EXPERIENCE_TOOL_NAME);
        assert!(calls
            .iter()
            .skip(1)
            .all(|call| call.name == "read_technical_snapshot"));
    }

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
    use orchestrator_sql::{ensure_schema, turn_history_items};
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
    fn seed_packet_rejects_more_than_two_claims() {
        let claim = json!({
            "claim_id": "qqq-trend:bull:1",
            "decision_hinge": "earnings",
            "claim": "upside",
            "evidence_refs": [],
            "confidence": 0.6,
            "known_bear_constraint": "valuation",
            "needs_mediator_check": false
        });
        let text = json!({
            "role": "researcher.bull.initial",
            "artifact_type": "bull_seed_packet",
            "topic_id": "qqq-trend",
            "claims": [claim.clone(), claim.clone(), claim],
            "summary": "three claims",
            "reducer_checks": {}
        })
        .to_string();

        assert_eq!(
            seed_packet_validation_error("researcher.bull.initial", &text),
            Err("claims must contain 1-2 items".to_string())
        );
    }

    #[test]
    fn analyst_final_packet_requires_exact_role_and_ticker_set() {
        let expected_tickers = vec!["QQQ".to_string(), "SOXX".to_string()];
        let wrong_role = r#"{
            "role":"analyst.news_macro",
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

        let canonical_source_tier = r#"{
            "id":"analyst.technical",
            "role":"analyst.technical",
            "per_ticker":{
                "QQQ":{
                    "direction":"bullish",
                    "confidence":0.7,
                    "report":"QQQ remains above its 20-day average.",
                    "key_evidence":[{
                        "claim":"QQQ closed above its 20-day average.",
                        "evidence_type":"fact",
                        "source":"Yahoo Finance daily OHLCV",
                        "timestamp":"2026-07-22",
                        "source_tier":"official",
                        "source_confidence":0.9
                    }]
                },
                "SOXX":{
                    "direction":"bearish",
                    "confidence":0.6,
                    "report":"SOXX remains below its 20-day average.",
                    "key_evidence":[{
                        "claim":"SOXX closed below its 20-day average.",
                        "evidence_type":"fact",
                        "source":"Yahoo Finance daily OHLCV",
                        "timestamp":"2026-07-22",
                        "source_tier":"official",
                        "source_confidence":0.9
                    }]
                }
            }
        }"#;
        assert!(analyst_final_artifact_looks_valid(
            "analyst.technical",
            &expected_tickers,
            canonical_source_tier
        ));
    }

    #[tokio::test]
    async fn truncated_analyst_artifact_gets_compact_retry_instruction() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        ensure_schema(&conn).unwrap();
        let valid_artifact = json!({
            "id": "analyst.news_macro",
            "role": "analyst.news_macro",
            "per_ticker": {
                "QQQ": {
                    "direction": "neutral",
                    "confidence": 0.4,
                    "report": "Evidence is limited.",
                    "key_evidence": [{
                        "claim": "No decisive new macro catalyst was supplied.",
                        "evidence_type": "fact",
                        "source": "read_jin10_candidates",
                        "timestamp": "2026-07-22",
                        "source_confidence": 0.8
                    }]
                }
            }
        })
        .to_string();
        let mut model = FakeStreamModel::new(vec![
            vec![
                ModelStreamEvent::AssistantMessageStarted {
                    item_id: "msg-truncated".to_string(),
                },
                ModelStreamEvent::AssistantTextDelta {
                    item_id: "msg-truncated".to_string(),
                    delta: format!("{{{}", "x".repeat(1_500)),
                },
                ModelStreamEvent::AssistantMessageCompleted {
                    item_id: "msg-truncated".to_string(),
                    turn_status: TurnStatus::Unknown,
                },
                ModelStreamEvent::ResponseCompleted {
                    end_turn: true,
                    raw: Value::Null,
                },
            ],
            vec![
                ModelStreamEvent::AssistantMessageStarted {
                    item_id: "msg-valid".to_string(),
                },
                ModelStreamEvent::AssistantTextDelta {
                    item_id: "msg-valid".to_string(),
                    delta: valid_artifact,
                },
                ModelStreamEvent::AssistantMessageCompleted {
                    item_id: "msg-valid".to_string(),
                    turn_status: TurnStatus::Unknown,
                },
                ModelStreamEvent::ResponseCompleted {
                    end_turn: true,
                    raw: Value::Null,
                },
            ],
        ]);
        let mut tools = StaticToolRuntime::new();
        let mut turn = Turn::new(
            "turn-truncated",
            "session-truncated",
            "run-truncated",
            "analyst.news_macro",
            "prompt",
        );
        turn.model_context = "tickers=QQQ".to_string();

        run_turn(
            &conn,
            &mut turn,
            &mut model,
            &mut tools,
            AgentLoopConfig {
                max_agent_loops: Some(2),
                ..AgentLoopConfig::default()
            },
        )
        .await
        .unwrap();

        assert!(model.seen_inputs[1].items.iter().any(|item| {
            item.item_type == TurnItemType::UserMessage
                && item
                    .content_text
                    .contains("Previous output failed validation or was incomplete")
                && item
                    .content_text
                    .contains("Expected role: analyst.news_macro")
                && item.content_text.contains("Expected tickers: QQQ")
                && item.content_text.contains("Validation errors:")
        }));
        assert!(model.seen_inputs[1].available_tools.is_empty());
    }

    #[test]
    fn analyst_system_instruction_requests_the_role_contract() {
        let instruction = model_system_instruction(&[], "analyst.news_macro", &["QQQ".to_string()]);

        assert!(instruction.contains("Follow the active role prompt and its output contract"));
    }

    #[test]
    fn controller_final_packet_rejects_incomplete_json() {
        assert!(!controller_packet_looks_valid(
            r#"{"role":"mediator.topic_controller","artifact_type":"topic_controller_packet","topic_id":"qqq"}"#
        ));
    }

    #[test]
    fn risk_committee_packet_requires_debate_fields() {
        let base = json!({
            "stance": "conservative",
            "argument": "Balanced review.",
            "recommended_adjustment": "Keep the existing cap.",
            "stop_type": "soft",
            "max_drawdown_pct": 0.04,
            "position_cap_pct": 0.10,
            "rebalance_trigger": "Review on confirmation.",
            "risk_off_trigger": "Reduce on invalidation.",
            "review_window": "1d",
            "cash_hedge_recommendation": "Keep cash.",
            "constraint_confidence": 0.8
        });
        assert!(!risk_constraints_look_valid(
            "risk.conservative",
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
            "risk.conservative",
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
        let turn_id: String = conn
            .query_row(
                "SELECT turn_id FROM agent_events ORDER BY turn_number DESC, id DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let rows = turn_history_items(conn, &turn_id).unwrap();
        let mut texts = Vec::new();
        for item in rows {
            if item.get("event_type").and_then(|v| v.as_str()) == Some("assistant_message") {
                if let Some(text) = item.get("content_text").and_then(|v| v.as_str()) {
                    texts.push(text.to_string());
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
                    format!("/tmp/large-{index}.rs {}", "x".repeat(50_000)),
                )
            })
            .collect::<Vec<_>>();
        let total_tokens = estimate_items_tokens(&items);
        assert!(total_tokens > 96_000);
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
                > 96_000
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
                phase: None,
                allowed_reflection_task_ids: Vec::new(),
                phase_summary_page_limit: 20,
                phase_summary_detail_page_limit: 20,
                tickers: Vec::new(),
                alpaca_live: false,
                alpaca_market_data: false,
                alpaca_api_key: None,
                alpaca_api_secret: None,
                phase_summary_index: None,
                phase_summary_gate: None,
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

    struct TerminalIndexService;

    impl tools::index_tools::IndexToolService for TerminalIndexService {
        fn create_index(&self, _: tools::index_tools::CreateIndexCommand) -> anyhow::Result<Value> {
            anyhow::bail!("not used")
        }

        fn append_index_detail(
            &self,
            _: tools::index_tools::AppendIndexDetailCommand,
        ) -> anyhow::Result<Value> {
            anyhow::bail!("not used")
        }

        fn finalize_index(
            &self,
            command: tools::index_tools::FinalizeIndexCommand,
        ) -> anyhow::Result<Value> {
            Ok(json!({"index_id": command.scope.index_id}))
        }

        fn read_indexes(
            &self,
            _: tools::index_tools::ReadIndexesCommand,
        ) -> anyhow::Result<tools::index_tools::IndexReadPage> {
            anyhow::bail!("not used")
        }

        fn read_index_details(
            &self,
            _: tools::index_tools::ReadIndexDetailsCommand,
        ) -> anyhow::Result<Value> {
            anyhow::bail!("not used")
        }
    }

    #[tokio::test]
    async fn project_runtime_dispatches_terminal_index_finalize_only_with_binding() {
        let binding = tools::index_tools::IndexToolRuntimeBinding::new(
            tools::index_tools::IndexOwnedScope {
                run_id: "run-1".to_owned(),
                source_run_id: None,
                source_phase: 1,
                role: "compressor.phase_summary".to_owned(),
                kind: tools::index_tools::IndexKind::PhaseSummary,
                ticker: None,
                topic_id: None,
                unit_key: "phase1:aggregate".to_owned(),
                source_payload_hash: "hash".to_owned(),
                index_id: "index-1".to_owned(),
            },
            Default::default(),
            std::sync::Arc::new(TerminalIndexService),
        )
        .unwrap();
        let mut runtime = ProjectToolRuntime::with_available_tools(
            tools::ExternalToolConfig::default(),
            vec![tools::FINALIZE_INDEX_TOOL_NAME.to_owned()],
        )
        .with_index_tool_runtime(binding);
        runtime.set_turn_context(ToolRuntimeTurnContext {
            run_id: "run-1".to_owned(),
            session_id: "session-1".to_owned(),
            turn_id: "turn-1".to_owned(),
            role: "compressor.phase_summary".to_owned(),
            phase: Some(1),
        });
        let result = runtime
            .execute(ToolCallRequest {
                call_id: "call-finalize".to_owned(),
                name: tools::FINALIZE_INDEX_TOOL_NAME.to_owned(),
                arguments: json!({}),
            })
            .await;
        assert_eq!(result.status, "completed");
        assert_eq!(result.output["terminal"], true);
        assert_eq!(result.output["artifact"]["index_id"], "index-1");
    }

    struct TerminalDomainService;

    impl tools::domain_tools::DomainToolService for TerminalDomainService {
        fn set_analyst_assessment(
            &self,
            _: tools::domain_tools::AnalystAssessmentCommand,
        ) -> anyhow::Result<Value> {
            anyhow::bail!("not used")
        }
        fn append_analyst_evidence(
            &self,
            _: tools::domain_tools::AnalystEvidenceCommand,
        ) -> anyhow::Result<Value> {
            Ok(json!({"status":"draft"}))
        }
        fn append_analyst_data_gap(
            &self,
            _: tools::domain_tools::AnalystDataGapCommand,
        ) -> anyhow::Result<Value> {
            anyhow::bail!("not used")
        }
        fn set_analyst_invalidation(
            &self,
            _: tools::domain_tools::AnalystInvalidationCommand,
        ) -> anyhow::Result<Value> {
            anyhow::bail!("not used")
        }
        fn finalize_analyst_report(&self) -> anyhow::Result<Value> {
            Ok(json!({"artifact_id":"analyst-1"}))
        }
        fn set_research_decision(
            &self,
            _: tools::domain_tools::ResearchDecisionCommand,
        ) -> anyhow::Result<Value> {
            anyhow::bail!("not used")
        }
        fn set_research_scenarios(
            &self,
            _: tools::domain_tools::ResearchScenariosCommand,
        ) -> anyhow::Result<Value> {
            anyhow::bail!("not used")
        }
        fn append_research_hinge(
            &self,
            _: tools::domain_tools::ResearchHingeCommand,
        ) -> anyhow::Result<Value> {
            anyhow::bail!("not used")
        }
        fn finalize_research_decision(&self) -> anyhow::Result<Value> {
            anyhow::bail!("not used")
        }
        fn set_trade_intent(
            &self,
            _: tools::domain_tools::TradeIntentCommand,
        ) -> anyhow::Result<Value> {
            anyhow::bail!("not used")
        }
        fn append_trade_blocker(
            &self,
            _: tools::domain_tools::TradeBlockerCommand,
        ) -> anyhow::Result<Value> {
            anyhow::bail!("not used")
        }
        fn finalize_trade_intent(&self) -> anyhow::Result<Value> {
            anyhow::bail!("not used")
        }
        fn set_risk_assessment(
            &self,
            _: tools::domain_tools::RiskAssessmentCommand,
        ) -> anyhow::Result<Value> {
            anyhow::bail!("not used")
        }
        fn set_risk_constraints(
            &self,
            _: tools::domain_tools::RiskConstraintsCommand,
        ) -> anyhow::Result<Value> {
            anyhow::bail!("not used")
        }
        fn finalize_risk_review(&self) -> anyhow::Result<Value> {
            anyhow::bail!("not used")
        }
        fn set_portfolio_asset_decision(
            &self,
            _: tools::domain_tools::PortfolioAssetDecisionCommand,
        ) -> anyhow::Result<Value> {
            anyhow::bail!("not used")
        }
        fn append_binding_risk_control(
            &self,
            _: tools::domain_tools::BindingRiskControlCommand,
        ) -> anyhow::Result<Value> {
            anyhow::bail!("not used")
        }
        fn finalize_portfolio_decision(&self) -> anyhow::Result<Value> {
            anyhow::bail!("not used")
        }
    }

    #[tokio::test]
    async fn project_runtime_dispatches_terminal_domain_finalize_only_with_binding() {
        let binding = tools::domain_tools::DomainToolRuntimeBinding::new(
            tools::domain_tools::DomainToolScope {
                profile: orchestrator_core::ToolManagedProfile::AnalystReport,
                tickers: ["QQQ".to_owned()].into_iter().collect(),
                visible_evidence_refs: Default::default(),
            },
            std::sync::Arc::new(TerminalDomainService),
        )
        .unwrap();
        let runtime = ProjectToolRuntime::with_available_tools(
            tools::ExternalToolConfig::default(),
            vec![tools::FINALIZE_ANALYST_REPORT_TOOL_NAME.to_owned()],
        )
        .with_domain_tool_runtime(binding);
        let result = runtime
            .execute(ToolCallRequest {
                call_id: "call-domain-finalize".to_owned(),
                name: tools::FINALIZE_ANALYST_REPORT_TOOL_NAME.to_owned(),
                arguments: json!({}),
            })
            .await;
        assert_eq!(result.status, "completed");
        assert_eq!(result.output["terminal"], true);
        assert_eq!(result.output["artifact"]["artifact_id"], "analyst-1");
    }

    #[tokio::test]
    async fn project_runtime_records_structured_reads_before_domain_evidence_writes() {
        let temp = tempfile::tempdir().unwrap();
        let csv_dir = temp.path().join(orchestrator_core::DEFAULT_JIN10_CSV_DIR);
        let path = orchestrator_core::jin10_csv_path(&csv_dir, "2026-07-27");
        orchestrator_core::write_jin10_csv(
            &path,
            &[orchestrator_core::Jin10CsvRow {
                id: "event-1".to_owned(),
                time: "2026-07-27 09:00:00".to_owned(),
                content: "Fed CPI update for QQQ".to_owned(),
            }],
        )
        .unwrap();
        let binding = tools::domain_tools::DomainToolRuntimeBinding::new(
            tools::domain_tools::DomainToolScope {
                profile: orchestrator_core::ToolManagedProfile::AnalystReport,
                tickers: ["QQQ".to_owned()].into_iter().collect(),
                visible_evidence_refs: Default::default(),
            },
            std::sync::Arc::new(TerminalDomainService),
        )
        .unwrap();
        let mut runtime = ProjectToolRuntime::with_available_tools(
            tools::ExternalToolConfig {
                project_root: temp.path().to_path_buf(),
                tickers: vec!["QQQ".to_owned()],
                ..Default::default()
            },
            vec![
                tools::read_jin10_candidates::NAME.to_owned(),
                tools::APPEND_ANALYST_EVIDENCE_TOOL_NAME.to_owned(),
            ],
        )
        .with_domain_tool_runtime(binding);
        runtime.set_turn_context(ToolRuntimeTurnContext {
            run_id: "run-1".to_owned(),
            session_id: "session-1".to_owned(),
            turn_id: "turn-1".to_owned(),
            role: "analyst.news_macro".to_owned(),
            phase: Some(1),
        });
        let evidence_write = || ToolCallRequest {
            call_id: "write-evidence".to_owned(),
            name: tools::APPEND_ANALYST_EVIDENCE_TOOL_NAME.to_owned(),
            arguments: json!({
                "ticker":"QQQ", "evidence_ref":"event-1",
                "evidence": {
                    "claim":"CPI update", "evidence_type":"fact", "source":"Jin10",
                    "timestamp":"2026-07-27", "source_tier":"official", "first_source":"Jin10",
                    "is_derivative_repost":false, "evidence_age":"0-2d", "source_confidence":0.9
                }
            }),
        };
        let before = runtime.execute(evidence_write()).await;
        assert_eq!(before.status, "error");
        assert!(before
            .error
            .as_deref()
            .is_some_and(|message| message.contains("not visible")));

        let read = runtime
            .execute(ToolCallRequest {
                call_id: "read-jin10".to_owned(),
                name: tools::read_jin10_candidates::NAME.to_owned(),
                arguments: json!({"tickers":["QQQ"]}),
            })
            .await;
        assert_eq!(read.status, "completed");
        assert_eq!(read.output["candidates"][0]["event_id"], "event-1");

        let after = runtime.execute(evidence_write()).await;
        assert_eq!(after.status, "completed");
    }

    #[tokio::test]
    async fn project_runtime_rejects_unconfigured_think_tool() {
        let runtime = ProjectToolRuntime::with_available_tools(
            tools::ExternalToolConfig {
                project_root: PathBuf::from("."),
                db_path: None,
                run_dir: None,
                run_id: None,
                phase: None,
                allowed_reflection_task_ids: Vec::new(),
                phase_summary_page_limit: 20,
                phase_summary_detail_page_limit: 20,
                tickers: Vec::new(),
                alpaca_live: false,
                alpaca_market_data: false,
                alpaca_api_key: None,
                alpaca_api_secret: None,
                phase_summary_index: None,
                phase_summary_gate: None,
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

    #[tokio::test]
    async fn max_agent_loops_counts_end_turns_not_model_iterations() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        ensure_schema(&conn).unwrap();
        let mut responses = vec![
            model_response(Some("Still gathering evidence."), false),
            model_response(Some("Still gathering evidence."), false),
            model_response(Some("Still gathering evidence."), false),
            model_response(Some("Still gathering evidence."), false),
            model_response(Some("Still gathering evidence."), false),
            model_response(Some("Still gathering evidence."), false),
        ];
        responses.push(model_response(
            Some("Final answer ready for downstream consumers after completing the requested analysis steps without further tool calls. detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail detail "),
            true,
        ));
        let mut model = FakeModel::new(responses);
        let mut tools = StaticToolRuntime::new();
        let mut turn = Turn::new(
            "turn-end-budget",
            "session-1",
            "run-1",
            "loop.test",
            "start",
        );

        run_turn(
            &conn,
            &mut turn,
            &mut model,
            &mut tools,
            AgentLoopConfig {
                max_agent_loops: Some(1),
                ..AgentLoopConfig::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(model.seen_inputs.len(), 7);
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
        assert!(
            instruction.contains("Artifact role and ticker coverage must match the active role")
        );
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
        let events = turn_history_items(&conn, "turn-1").unwrap();
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
    async fn tool_managed_terminal_stops_later_calls_in_the_same_response() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        ensure_schema(&conn).unwrap();
        let mut model = FakeModel::new(vec![ModelResponse {
            assistant_message: Some("I will finalize through tools.".to_string()),
            reasoning_summary: None,
            tool_calls: vec![
                ToolCallRequest {
                    call_id: "read-1".to_string(),
                    name: "read".to_string(),
                    arguments: Value::Null,
                },
                ToolCallRequest {
                    call_id: "finalize-1".to_string(),
                    name: "finalize".to_string(),
                    arguments: Value::Null,
                },
                ToolCallRequest {
                    call_id: "after-1".to_string(),
                    name: "after".to_string(),
                    arguments: Value::Null,
                },
            ],
            end_turn: false,
            raw: json!({"step": 1}),
            turn_status: TurnStatus::Unknown,
        }]);
        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut tools = StaticToolRuntime::new();
        for (name, terminal) in [("read", false), ("finalize", true), ("after", false)] {
            let calls = std::sync::Arc::clone(&calls);
            tools.add_tool(name, move |_| {
                calls.lock().unwrap().push(name.to_string());
                ToolResultItem {
                    call_id: format!("{name}-result"),
                    name: name.to_string(),
                    status: "completed".to_string(),
                    output: json!({"terminal": terminal, "artifact": {"id": "artifact-1"}}),
                    error: None,
                }
            });
        }
        let mut turn = Turn::new("turn-1", "session-1", "run-1", "loop.test", "start");
        let config = AgentLoopConfig {
            require_terminal_tool: true,
            ..AgentLoopConfig::default()
        };

        run_turn(&conn, &mut turn, &mut model, &mut tools, config)
            .await
            .unwrap();

        assert_eq!(*calls.lock().unwrap(), vec!["read", "finalize"]);
        assert_eq!(turn.end_reason.as_deref(), Some("terminal_tool"));
        assert_eq!(
            turn.terminal_tool_result
                .as_ref()
                .and_then(|result| result.output.get("artifact"))
                .and_then(|artifact| artifact.get("id"))
                .and_then(Value::as_str),
            Some("artifact-1")
        );
        assert!(turn.emitted_items.iter().any(|item| {
            item.tool_call_id == "after-1"
                && item.item_type == TurnItemType::ToolResult
                && item
                    .content_json
                    .pointer("/result/output/status")
                    .and_then(Value::as_str)
                    == Some("ignored_after_terminal")
        }));
    }

    #[tokio::test]
    async fn tool_managed_prose_end_gets_one_repair_then_terminal_succeeds() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        ensure_schema(&conn).unwrap();
        let mut model = FakeModel::new(vec![
            model_response(Some("This prose does not finalize the draft."), true),
            ModelResponse {
                assistant_message: None,
                reasoning_summary: None,
                tool_calls: vec![ToolCallRequest {
                    call_id: "finalize-1".to_string(),
                    name: "finalize".to_string(),
                    arguments: Value::Null,
                }],
                end_turn: false,
                raw: json!({"step": 2}),
                turn_status: TurnStatus::Unknown,
            },
        ]);
        let mut tools = StaticToolRuntime::new();
        tools.add_tool("finalize", |_| ToolResultItem {
            call_id: "finalize-1".to_string(),
            name: "finalize".to_string(),
            status: "completed".to_string(),
            output: json!({"terminal": true, "artifact": {"id": "artifact-1"}}),
            error: None,
        });
        let mut turn = Turn::new("turn-1", "session-1", "run-1", "loop.test", "start");

        run_turn(
            &conn,
            &mut turn,
            &mut model,
            &mut tools,
            AgentLoopConfig {
                require_terminal_tool: true,
                ..AgentLoopConfig::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(model.seen_inputs.len(), 2);
        assert_eq!(
            turn.emitted_items
                .iter()
                .filter(|item| {
                    item.item_type == TurnItemType::UserMessage
                        && item
                            .content_text
                            .contains("No terminal finalize tool succeeded")
                })
                .count(),
            1
        );
        assert_eq!(turn.end_reason.as_deref(), Some("terminal_tool"));
        assert_eq!(
            turn.terminal_tool_result
                .as_ref()
                .and_then(|result| result.output.get("artifact"))
                .and_then(|artifact| artifact.get("id"))
                .and_then(Value::as_str),
            Some("artifact-1")
        );
    }

    #[tokio::test]
    async fn tool_managed_second_prose_end_fails_after_one_repair() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        ensure_schema(&conn).unwrap();
        let mut model = FakeModel::new(vec![
            model_response(Some("This prose does not finalize the draft."), true),
            model_response(
                Some("This second prose response still does not finalize."),
                true,
            ),
        ]);
        let mut tools = StaticToolRuntime::new();
        let mut turn = Turn::new("turn-1", "session-1", "run-1", "loop.test", "start");

        let error = run_turn(
            &conn,
            &mut turn,
            &mut model,
            &mut tools,
            AgentLoopConfig {
                require_terminal_tool: true,
                ..AgentLoopConfig::default()
            },
        )
        .await
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("tool-managed agent ended without a successful terminal finalize tool"));
        assert_eq!(model.seen_inputs.len(), 2);
        assert_eq!(
            turn.emitted_items
                .iter()
                .filter(|item| {
                    item.item_type == TurnItemType::UserMessage
                        && item
                            .content_text
                            .contains("No terminal finalize tool succeeded")
                })
                .count(),
            1
        );
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
                phase: None,
                allowed_reflection_task_ids: Vec::new(),
                phase_summary_page_limit: 20,
                phase_summary_detail_page_limit: 20,
                tickers: vec!["TQQQ".to_string()],
                alpaca_live: false,
                alpaca_market_data: false,
                alpaca_api_key: None,
                alpaca_api_secret: None,
                phase_summary_index: None,
                phase_summary_gate: None,
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

        let events = turn_history_items(&conn, "turn-web").unwrap();
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
