use anyhow::{bail, Result};
use serde_json::{json, Value};

use super::{api_tool_name, ExternalToolConfig, ToolDefinition};
use crate::agent_loop::ToolRuntimeTurnContext;

pub const NAME: &str = "read_reflection_source";

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "Phase 0 only: read the Rust-owned bootstrap metadata, decision, outcome, and FileStore Index availability for this reflection unit. Use read_indexes and read_index_details for evidence.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": false
        }),
    }
}

pub fn execute(
    _args: Value,
    config: &ExternalToolConfig,
    turn_context: Option<&ToolRuntimeTurnContext>,
) -> Result<Value> {
    if turn_context.and_then(|context| context.phase) != Some(0) {
        bail!("read_reflection_source is only available in phase 0");
    }
    if let Some(source) = &config.file_store_reflection_source {
        return Ok(source.clone());
    }
    bail!("read_reflection_source requires a Rust-owned FileStore reflection source")
}
