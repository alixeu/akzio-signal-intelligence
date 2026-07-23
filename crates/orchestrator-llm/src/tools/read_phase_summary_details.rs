use anyhow::Result;
use serde_json::{json, Value};

use super::{api_tool_name, tool_connection, ExternalToolConfig, ToolDefinition};
use crate::agent_loop::ToolRuntimeTurnContext;

pub const NAME: &str = "read_phase_summary_details";

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "Expand one phase summary by id. The summary must belong to the current run and an earlier phase; inaccessible ids are reported as not visible.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "summary_id": {
                    "type": "string",
                    "description": "A summary id returned by read_phase_summaries."
                }
            },
            "required": ["summary_id"],
            "additionalProperties": false
        }),
    }
}

pub fn execute(
    args: Value,
    config: &ExternalToolConfig,
    turn_context: Option<&ToolRuntimeTurnContext>,
) -> Result<Value> {
    let (run_id, current_phase) = super::read_run_context::visible_scope(turn_context)?;
    let summary_id = args
        .get("summary_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("read_phase_summary_details.summary_id is required"))?;
    let max_source_phase = current_phase - 1;

    if let Some(index) =
        super::read_run_context::wait_for_phase_summary(config, run_id, max_source_phase)?
    {
        return index.list_visible_details(run_id, current_phase, summary_id);
    }
    if let Some(index) = config
        .phase_summary_index
        .as_ref()
        .filter(|index| index.run_id == run_id)
    {
        return index.list_visible_details(run_id, current_phase, summary_id);
    }
    let conn = tool_connection(config)?;
    orchestrator_sql::list_phase_summary_details(&conn, run_id, current_phase, summary_id)
}
