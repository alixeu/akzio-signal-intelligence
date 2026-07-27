use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use super::{api_tool_name, tool_connection, ExternalToolConfig, ToolDefinition};
use crate::agent_loop::ToolRuntimeTurnContext;

pub const NAME: &str = "read_phase_summaries";

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "List compact phase-summary indexes from earlier phases in the current run. Use the returned summary id with read_phase_summary_details when evidence must be expanded.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "ticker": {
                    "type": "string",
                    "description": "Optional ticker filter. Run and phase visibility are fixed by the current turn."
                },
                "source_phase": {"type": "integer", "minimum": 1, "maximum": 7},
                "role": {"type": "string"},
                "topic_id": {"type": "string"},
                "task_id": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Required only in Phase 0; resolves an allowlisted historical run."
                },
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20},
                "cursor": {"type": ["string", "null"]}
            },
            "required": [],
            "additionalProperties": false
        }),
    }
}

pub fn execute(
    args: Value,
    config: &ExternalToolConfig,
    turn_context: Option<&ToolRuntimeTurnContext>,
) -> Result<Value> {
    let context = turn_context.context("read_phase_summaries requires turn context")?;
    let ticker = super::optional_string_arg(&args, "ticker")?;
    let role = super::optional_string_arg(&args, "role")?;
    let topic_id = super::optional_string_arg(&args, "topic_id")?;
    let source_phase = match args.get("source_phase") {
        None | Some(Value::Null) => None,
        Some(value) => Some(
            value
                .as_i64()
                .filter(|phase| (1..=7).contains(phase))
                .context("read_phase_summaries.source_phase must be between 1 and 7")?,
        ),
    };
    let (limit, offset) = super::pagination_args(&args, config.phase_summary_page_limit)?;
    let query = orchestrator_sql::PhaseSummaryQuery {
        ticker,
        source_phase,
        role,
        topic_id,
        limit,
        offset,
    };

    if context.phase == Some(0) {
        let task_id = args
            .get("task_id")
            .and_then(Value::as_i64)
            .context("read_phase_summaries.task_id is required in phase 0")?;
        if !config.allowed_reflection_task_ids.contains(&task_id) {
            bail!("reflection task {task_id} is not allowlisted for this turn");
        }
        let conn = tool_connection(config)?;
        let source_run_id = orchestrator_sql::reflection_task_source_run(&conn, task_id)?;
        let mut result = orchestrator_sql::query_phase_summaries(&conn, &source_run_id, 8, &query)?;
        result["source_policy"] = json!("task_allowlisted_historical_run_only");
        result["task_id"] = json!(task_id);
        return Ok(result);
    }

    let (run_id, current_phase) = super::read_run_context::visible_scope(turn_context)?;
    let max_source_phase = current_phase - 1;

    if let Some(index) =
        super::read_run_context::wait_for_phase_summary(config, run_id, max_source_phase)?
    {
        return index.query_visible_summaries(run_id, current_phase, &query);
    }
    if let Some(index) = config
        .phase_summary_index
        .as_ref()
        .filter(|index| index.run_id == run_id)
    {
        return index.query_visible_summaries(run_id, current_phase, &query);
    }
    let conn = tool_connection(config)?;
    orchestrator_sql::query_phase_summaries(&conn, run_id, current_phase, &query)
}
