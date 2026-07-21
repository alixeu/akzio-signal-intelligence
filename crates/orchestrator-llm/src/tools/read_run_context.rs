use anyhow::{bail, Result};
use rig_core::completion::ToolDefinition;
use serde_json::{json, Value};

use super::{api_tool_name, tool_connection, ExternalToolConfig};
use crate::agent_loop::ToolRuntimeTurnContext;

pub const NAME: &str = "read_run_context";

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "Read phase00 organized summaries. Use kind=phase_summaries to list the summary index (optionally filtered by ticker); use kind=phase_summary_details with topic_id to retrieve one summary's full body.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "kind": {
                    "type": "string",
                    "enum": ["phase_summaries", "phase_summary_details"],
                    "description": "phase_summaries = list index; phase_summary_details = one summary body (requires topic_id)."
                },
                "ticker": {
                    "type": "string",
                    "description": "Optional ticker filter for phase_summaries."
                },
                "topic_id": {
                    "type": "string",
                    "description": "Required for phase_summary_details: the summary id to expand."
                }
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
    let kind = args
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if !matches!(kind.as_str(), "phase_summaries" | "phase_summary_details") {
        bail!(
            "read_run_context only supports kinds phase_summaries|phase_summary_details; got {:?}",
            kind
        );
    }

    let ticker = args
        .get("ticker")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let topic_id = args
        .get("topic_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let phase = turn_context.and_then(|ctx| ctx.phase);
    let run_id = turn_context.map(|ctx| ctx.run_id.clone());

    let request = orchestrator_sql::RunContextReadRequest {
        kind: kind.clone(),
        ticker: ticker.clone(),
        topic_id: topic_id.clone(),
        run_id,
        role: turn_context.map(|ctx| ctx.role.clone()),
        phase,
        tickers: config.tickers.clone(),
        turn_id: turn_context.map(|ctx| ctx.turn_id.clone()),
        persist_context: false,
        token_budget: None,
    };

    maybe_wait_phase00_gate(config, &request);

    if let Some(from_mem) = try_read_phase00_from_memory(config, &request) {
        return Ok(from_mem);
    }
    let mut conn = tool_connection(config)?;
    orchestrator_sql::read_run_context(&mut conn, &request)
}

fn maybe_wait_phase00_gate(
    config: &ExternalToolConfig,
    request: &orchestrator_sql::RunContextReadRequest,
) {
    let gate = config.phase00_gate.clone().or_else(|| {
        config
            .run_id
            .as_deref()
            .and_then(orchestrator_sql::phase00_gate)
    });
    let Some(gate) = gate else {
        return;
    };
    let max_prior = request.phase.filter(|p| *p > 0).map(|p| p - 1);
    let _ = gate.wait_until_ready(max_prior, std::time::Duration::from_secs(600));
}


fn try_read_phase00_from_memory(
    config: &ExternalToolConfig,
    request: &orchestrator_sql::RunContextReadRequest,
) -> Option<Value> {
    let owned;
    let index: &orchestrator_sql::Phase00MemoryIndex = if let Some(gate) =
        config.phase00_gate.as_ref().cloned().or_else(|| {
            config
                .run_id
                .as_deref()
                .and_then(orchestrator_sql::phase00_gate)
        }) {
        owned = gate.snapshot();
        &owned
    } else if let Some(idx) = config.phase00_index.as_ref() {
        idx.as_ref()
    } else {
        return None;
    };

    match request.kind.as_str() {
        "phase_summaries" => {
            let max_source_phase = request.phase.filter(|p| *p > 0).map(|p| p - 1);
            Some(index.list_summaries(
                max_source_phase,
                request.ticker.as_deref().filter(|t| !t.is_empty()),
            ))
        }
        "phase_summary_details" => {
            let summary_id = request
                .topic_id
                .as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or_default();
            if summary_id.is_empty() {
                return None;
            }
            Some(index.list_details(summary_id))
        }
        _ => None,
    }
}

