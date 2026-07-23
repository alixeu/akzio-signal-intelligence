use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use super::{api_tool_name, tool_connection, ExternalToolConfig, ToolDefinition};
use crate::agent_loop::ToolRuntimeTurnContext;

pub const NAME: &str = "read_experience";

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "Retrieve phase-scoped, ticker-scoped historical experience before analysis. Items are advisory and rebuttable; cite the experience id when used or rejected.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 50}
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
    let context = turn_context.context("read_experience requires a live turn context")?;
    let phase = context
        .phase
        .context("read_experience requires the current phase")?;
    if !(1..=6).contains(&phase) {
        bail!("read_experience is only available in phases 1-6");
    }
    let ticker = args
        .get("ticker")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty());
    let limit = args.get("limit").and_then(Value::as_u64).unwrap_or(20) as usize;
    let conn = tool_connection(config)?;
    orchestrator_sql::read_experience(&conn, phase, ticker, limit)
}
