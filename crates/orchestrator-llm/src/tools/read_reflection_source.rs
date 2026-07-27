use anyhow::{bail, Result};
use serde_json::{json, Value};

use super::{api_tool_name, tool_connection, ExternalToolConfig, ToolDefinition};
use crate::agent_loop::ToolRuntimeTurnContext;

pub const NAME: &str = "read_reflection_source";

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "Phase 0 only: read bootstrap metadata, decision, outcome, and summary availability for one allowlisted reflection task. Use read_phase_summaries and read_phase_summary_details for evidence.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "task_id": {"type": "integer", "minimum": 1}
            },
            "required": ["task_id"],
            "additionalProperties": false
        }),
    }
}

pub fn execute(
    args: Value,
    config: &ExternalToolConfig,
    turn_context: Option<&ToolRuntimeTurnContext>,
) -> Result<Value> {
    if turn_context.and_then(|context| context.phase) != Some(0) {
        bail!("read_reflection_source is only available in phase 0");
    }
    let task_id = args
        .get("task_id")
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow::anyhow!("read_reflection_source.task_id is required"))?;
    if !config.allowed_reflection_task_ids.contains(&task_id) {
        bail!("reflection task {task_id} is not allowlisted for this turn");
    }
    let conn = tool_connection(config)?;
    orchestrator_sql::reflection_source_context(&conn, task_id)
}
