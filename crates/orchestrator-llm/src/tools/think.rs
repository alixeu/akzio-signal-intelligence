use rig_core::completion::ToolDefinition;
use serde_json::{json, Value};

use super::api_tool_name;

pub const NAME: &str = "think";

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "When you need a short private planning note before another tool call or the final artifact. Do not use this as a substitute for reading evidence.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "note": {"type": "string", "description": "Brief planning note for this turn only"}
            },
            "additionalProperties": true
        }),
    }
}

pub fn execute(args: Value) -> Value {
    json!({
        "status": "completed",
        "summary": args
    })
}
