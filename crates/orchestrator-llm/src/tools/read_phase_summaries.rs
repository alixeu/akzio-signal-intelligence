use anyhow::{bail, Result};
use serde_json::{json, Value};

use super::{api_tool_name, tool_connection, ExternalToolConfig, ToolDefinition};
use crate::agent_loop::ToolRuntimeTurnContext;

pub const NAME: &str = "read_phase_summaries";

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "List compact Phase 00 summary indexes from earlier phases in the current run. Use the returned summary id with read_phase_summary_details when evidence must be expanded.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "ticker": {
                    "type": "string",
                    "description": "Optional ticker filter. Run and phase visibility are fixed by the current turn."
                }
            },
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
    let max_source_phase = current_phase - 1;
    let ticker = match args.get("ticker") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) if !value.trim().is_empty() => Some(value.trim()),
        Some(Value::String(_)) => None,
        Some(_) => bail!("read_phase_summaries.ticker must be a string"),
    };

    if let Some(index) =
        super::read_run_context::wait_for_phase00(config, run_id, max_source_phase)?
    {
        return index.list_visible_summaries(run_id, current_phase, ticker);
    }
    if let Some(index) = config
        .phase00_index
        .as_ref()
        .filter(|index| index.run_id == run_id)
    {
        return index.list_visible_summaries(run_id, current_phase, ticker);
    }
    let conn = tool_connection(config)?;
    orchestrator_sql::list_phase_summaries(&conn, run_id, current_phase, ticker)
}
