use anyhow::Result;
use serde_json::json;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use crate::truncation::TruncationConfig;

use super::*;

pub(super) struct ModelStreamHandler<'a, S: AgentEventSink> {
    conn: &'a rusqlite::Connection,
    turn: &'a mut Turn,
    sink: &'a mut S,
    result: ModelStreamResult,
    assistant_buffers: BTreeMap<String, String>,
    reasoning_buffers: BTreeMap<String, String>,
    truncation: TruncationConfig,
}

impl<'a, S: AgentEventSink> ModelStreamHandler<'a, S> {
    pub(super) fn new(
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

    pub(super) async fn finish(mut self) -> Result<ModelStreamResult> {
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
