use super::ToolDefinition;
use anyhow::{bail, Result};
use serde_json::{json, Value};

use super::{api_tool_name, ExternalToolConfig};
use crate::agent_loop::ToolRuntimeTurnContext;

pub const NAME: &str = "read_run_context";

/// Compatibility definition for internal callers. This tool is not model-registered.
pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "Deprecated compatibility wrapper for phase summary reads.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "kind": {"type": "string", "enum": ["phase_summaries", "phase_summary_details"]},
                "ticker": {"type": "string"},
                "topic_id": {"type": "string"}
            },
            "required": ["kind"],
            "additionalProperties": false
        }),
    }
}

pub fn execute(
    args: Value,
    config: &ExternalToolConfig,
    turn_context: Option<&ToolRuntimeTurnContext>,
) -> Result<Value> {
    match args.get("kind").and_then(Value::as_str) {
        Some("phase_summaries") => super::read_phase_summaries::execute(
            json!({"ticker": args.get("ticker")}),
            config,
            turn_context,
        ),
        Some("phase_summary_details") => {
            let summary_id = args
                .get("topic_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            super::read_phase_summary_details::execute(
                json!({"summary_id": summary_id}),
                config,
                turn_context,
            )
        }
        other => bail!(
            "read_run_context only supports kinds phase_summaries|phase_summary_details; got {:?}",
            other
        ),
    }
}

pub(super) fn visible_scope(turn_context: Option<&ToolRuntimeTurnContext>) -> Result<(&str, i64)> {
    let context =
        turn_context.ok_or_else(|| anyhow::anyhow!("phase summary tool requires turn context"))?;
    if context.run_id.trim().is_empty() {
        bail!("phase summary tool requires a non-empty run_id from turn context");
    }
    let current_phase = context.phase.filter(|phase| *phase > 0).ok_or_else(|| {
        anyhow::anyhow!("phase summary tool requires phase > 0 from turn context")
    })?;
    Ok((context.run_id.as_str(), current_phase))
}

pub(super) fn wait_for_phase00(
    config: &ExternalToolConfig,
    run_id: &str,
    max_source_phase: i64,
) -> Result<Option<orchestrator_sql::Phase00MemoryIndex>> {
    let configured_gate = config
        .phase00_gate
        .as_ref()
        .filter(|gate| gate.run_id() == run_id)
        .cloned();
    let gate = configured_gate.or_else(|| orchestrator_sql::phase00_gate(run_id));
    let Some(gate) = gate else {
        return Ok(None);
    };
    gate.wait_until_ready_checked(Some(max_source_phase), std::time::Duration::from_secs(600))
        .map_err(|error| anyhow::anyhow!("phase00 summaries unavailable: {error}"))?;
    let snapshot = gate.snapshot();
    if snapshot.run_id != run_id {
        bail!("phase00 memory belongs to a different run");
    }
    Ok(Some(snapshot))
}
