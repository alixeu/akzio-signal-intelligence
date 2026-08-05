//! Pure adapters between persisted turn-item data and the agent-loop runtime.
//!
//! Persistence uses stringly typed event/item and metadata fields for
//! serde/FileStore compatibility. Keep their tolerant decoding and the
//! runtime output projection together here so the loop orchestration does not
//! own compatibility details.

use super::{AgentItemPhase, AgentItemStatus, AgentOutputItem, TurnItem, TurnItemType};
use serde_json::Value;

/// Convert a persisted agent-event history value into a runtime turn item.
///
/// `event_type` is preferred because older session events used it as the
/// persisted item discriminator; newer item payloads may provide
/// `item_type`. Unknown discriminators intentionally retain the historical
/// `InjectedContext` fallback.
pub(crate) fn turn_item_from_history_value(value: Value) -> TurnItem {
    let item_type = persisted_item_type(
        value
            .get("event_type")
            .or_else(|| value.get("item_type"))
            .and_then(Value::as_str),
    );
    let content_json = value.get("content_json").cloned().unwrap_or(Value::Null);
    TurnItem {
        item_type,
        role: string_field(&value, "role"),
        content_text: string_field(&value, "content_text"),
        content_json: content_json.clone(),
        tool_call_id: string_field(&value, "tool_call_id"),
        tool_name: string_field(&value, "tool_name"),
        output_item_id: string_field(&content_json, "output_item_id"),
        phase: phase_from_persisted(&content_json),
        status: status_from_persisted(&content_json),
        db_row_id: None,
    }
}

fn string_field(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned()
}

fn persisted_item_type(value: Option<&str>) -> TurnItemType {
    match value.unwrap_or("") {
        "user_message" => TurnItemType::UserMessage,
        "assistant_message" => TurnItemType::AssistantMessage,
        "reasoning_summary" => TurnItemType::ReasoningSummary,
        "reasoning_state" => TurnItemType::ReasoningState,
        "plan_update" => TurnItemType::PlanUpdate,
        "tool_call" => TurnItemType::ToolCall,
        "tool_result" => TurnItemType::ToolResult,
        "native_web_search" => TurnItemType::NativeWebSearch,
        "system_context" => TurnItemType::SystemContext,
        "developer_context" => TurnItemType::DeveloperContext,
        "compact_summary" => TurnItemType::CompactSummary,
        _ => TurnItemType::InjectedContext,
    }
}

fn phase_from_persisted(value: &Value) -> Option<AgentItemPhase> {
    value
        .get("phase")
        .and_then(Value::as_str)
        .and_then(|phase| match phase {
            "commentary" => Some(AgentItemPhase::Commentary),
            "final" => Some(AgentItemPhase::Final),
            _ => None,
        })
}

fn status_from_persisted(value: &Value) -> Option<AgentItemStatus> {
    value
        .get("status")
        .and_then(Value::as_str)
        .and_then(|status| match status {
            "in_progress" => Some(AgentItemStatus::InProgress),
            "completed" => Some(AgentItemStatus::Completed),
            "pending" => Some(AgentItemStatus::Pending),
            "running" => Some(AgentItemStatus::Running),
            "failed" => Some(AgentItemStatus::Failed),
            "interrupted" => Some(AgentItemStatus::Interrupted),
            _ => None,
        })
}

/// Project a persisted/runtime turn item into the public streaming item
/// envelope. Items with no public event representation remain silent.
pub(super) fn output_item_for(item: &TurnItem) -> Option<AgentOutputItem> {
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

/// Preserve the existing runtime projection of stringly typed tool results.
pub(super) fn runtime_status_for_tool_result(status: &str) -> AgentItemStatus {
    if status == "completed" || status == "started" {
        AgentItemStatus::Completed
    } else {
        AgentItemStatus::Failed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn persisted_event_type_and_metadata_decode_with_unknown_fallbacks() {
        let item = turn_item_from_history_value(json!({
            "event_type": "tool_result",
            "item_type": "assistant_message",
            "role": "tool",
            "content_text": "result",
            "content_json": {
                "output_item_id": "result-call-1",
                "phase": "not-a-phase",
                "status": "not-a-status"
            },
            "tool_call_id": "call-1",
            "tool_name": "read_indexes"
        }));

        assert_eq!(item.item_type, TurnItemType::ToolResult);
        assert_eq!(item.output_item_id, "result-call-1");
        assert_eq!(item.phase, None);
        assert_eq!(item.status, None);

        let unknown = turn_item_from_history_value(json!({
            "event_type": "future_item_type"
        }));
        assert_eq!(unknown.item_type, TurnItemType::InjectedContext);
    }

    #[test]
    fn runtime_output_projection_keeps_default_status_and_silent_items() {
        let item = TurnItem {
            item_type: TurnItemType::ToolCall,
            role: "assistant".to_owned(),
            content_text: String::new(),
            content_json: json!({"call": {"arguments": {"source_phase": 1}}}),
            tool_call_id: "call-1".to_owned(),
            tool_name: "read_indexes".to_owned(),
            output_item_id: String::new(),
            phase: None,
            status: None,
            db_row_id: None,
        };

        assert!(matches!(
            output_item_for(&item),
            Some(AgentOutputItem::ToolCall {
                status: AgentItemStatus::Pending,
                ..
            })
        ));
        assert!(output_item_for(&TurnItem::user("context")).is_none());
        assert_eq!(
            runtime_status_for_tool_result("started"),
            AgentItemStatus::Completed
        );
        assert_eq!(
            runtime_status_for_tool_result("error"),
            AgentItemStatus::Failed
        );
    }
}
