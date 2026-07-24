use anyhow::Result;
use serde_json::{json, Value};
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;

use crate::tools;
use crate::AgentSettings;

use super::*;

pub(super) struct DebugLlmCapture<'a> {
    inner: &'a mut dyn ModelEventHandler,
    req_messages: Vec<Value>,
    req_tools: Vec<Value>,
    assistant_text: String,
    tool_calls: Vec<Value>,
    raw: Value,
    end_turn: Option<bool>,
    started: Instant,
}

impl<'a> DebugLlmCapture<'a> {
    pub(super) fn new(
        inner: &'a mut dyn ModelEventHandler,
        input: &ModelInput,
        configured_tools: &[String],
    ) -> Self {
        let tools = if input.available_tools.is_empty() && !configured_tools.is_empty() {
            tools::tool_definitions_json(configured_tools)
        } else {
            tools::tool_definitions_json(&input.available_tools)
        };
        Self {
            inner,
            req_messages: input_to_debug_messages(input),
            req_tools: tools,
            assistant_text: String::new(),
            tool_calls: Vec::new(),
            raw: Value::Null,
            end_turn: None,
            started: Instant::now(),
        }
    }

    pub(super) fn into_record(
        self,
        settings: &AgentSettings,
        error: Option<&anyhow::Error>,
    ) -> Value {
        let end_turn = if self.tool_calls.is_empty() {
            self.end_turn
        } else {
            Some(false)
        };
        let elapsed_ms = self.started.elapsed().as_millis();
        let usage = extract_token_usage(&self.raw);

        let mut output = Vec::new();
        if !self.assistant_text.is_empty() {
            output.push(json!({"type": "text", "text": self.assistant_text}));
        }
        for tc in &self.tool_calls {
            output.push(json!({
                "type": "function_call",
                "call_id": tc.get("call_id"),
                "name": tc.get("name"),
                "arguments": tc.get("arguments"),
            }));
        }

        let resp = json!({
            "status": if error.is_some() {
                "error"
            } else if end_turn == Some(true) {
                "completed"
            } else {
                "in_progress"
            },
            "output": output,
            "error": error.map(ToString::to_string),
            "usage": {
                "input_tokens": usage.input_tokens,
                "output_tokens": usage.output_tokens,
                "cached_tokens": usage.cached_tokens,
                "reasoning_tokens": usage.reasoning_tokens,
                "total_tokens": usage.total_tokens,
            },
        });

        json!({
            "kind": "stream",
            "role": settings.role,
            "phase": settings.phase,
            "topic_id": settings.topic_id,
            "round": settings.debug_round,
            "model": settings.llm.model,
            "req": {
                "messages": self.req_messages,
                "tools": self.req_tools,
            },
            "resp": resp,
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
