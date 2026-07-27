use anyhow::{bail, Context, Result};
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
                },
                "task_id": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Required only in Phase 0; must match the allowlisted reflection task."
                },
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20},
                "cursor": {"type": ["string", "null"]}
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
    let context = turn_context.context("read_phase_summary_details requires turn context")?;
    let summary_id = args
        .get("summary_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("read_phase_summary_details.summary_id is required"))?;
    let (limit, offset) = super::pagination_args(&args, config.phase_summary_detail_page_limit)?;
    let query = orchestrator_sql::PhaseSummaryDetailQuery { limit, offset };

    if context.phase == Some(0) {
        let task_id = args
            .get("task_id")
            .and_then(Value::as_i64)
            .context("read_phase_summary_details.task_id is required in phase 0")?;
        if !config.allowed_reflection_task_ids.contains(&task_id) {
            bail!("reflection task {task_id} is not allowlisted for this turn");
        }
        let conn = tool_connection(config)?;
        let source_run_id = orchestrator_sql::reflection_task_source_run(&conn, task_id)?;
        let mut result = orchestrator_sql::query_phase_summary_details(
            &conn,
            &source_run_id,
            8,
            summary_id,
            query,
        )?;
        result["source_policy"] = json!("task_allowlisted_historical_run_only");
        result["task_id"] = json!(task_id);
        return Ok(result);
    }

    let (run_id, current_phase) = super::read_run_context::visible_scope(turn_context)?;
    let max_source_phase = current_phase - 1;

    if let Some(index) =
        super::read_run_context::wait_for_phase_summary(config, run_id, max_source_phase)?
    {
        return index.query_visible_details(run_id, current_phase, summary_id, query);
    }
    if let Some(index) = config
        .phase_summary_index
        .as_ref()
        .filter(|index| index.run_id == run_id)
    {
        return index.query_visible_details(run_id, current_phase, summary_id, query);
    }
    let conn = tool_connection(config)?;
    orchestrator_sql::query_phase_summary_details(&conn, run_id, current_phase, summary_id, query)
}
