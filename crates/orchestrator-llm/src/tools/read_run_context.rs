use anyhow::{bail, Context, Result};
use rig_core::completion::ToolDefinition;
use serde_json::{json, Value};

use super::{api_tool_name, tool_connection, ExternalToolConfig};
use crate::agent_loop::ToolRuntimeTurnContext;

pub const NAME: &str = "read_run_context";

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "When the dynamic prompt fork is insufficient and you need structured run evidence already stored for this run. Choose kind by situation: research_inputs or compose_context for imported analyst/social bundles; technical or jin10 only for Phase-1 analyst re-reads of those stores; phase_summaries or prior_phase_summaries for the phase00 summary index; phase_summary_details when one summary body is required (topic_id = summary id); attention for ranking; attention_expand to open a subject (pass kind:id in tickers). Prefer the dynamic fork when it already answers the need. Phase-2+ roles must not use raw jin10/technical/compose_context/research_inputs.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "kind": {
                    "type": "string",
                    "description": "Situation selector: research_inputs | compose_context (imported analyst/social context); technical | jin10 (Phase-1 stores only); phase_summaries | prior_phase_summaries (summary index); phase_summary_details (one summary body, topic_id=summary id); attention | attention_expand (ranking / expand kind:id subjects)."
                },
                "ticker": {
                    "type": "string",
                    "description": "Optional single ticker filter when the situation is ticker-scoped."
                },
                "tickers": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional multi-ticker filter, or for attention_expand a list of kind:id subjects."
                },
                "token_budget": {
                    "type": "integer",
                    "minimum": 256,
                    "description": "Optional max token budget for compose_context-style responses."
                }
            },
            "additionalProperties": true
        }),
    }
}

pub fn execute(
    args: Value,
    config: &ExternalToolConfig,
    turn_context: Option<&ToolRuntimeTurnContext>,
) -> Result<Value> {
    let mut request = serde_json::from_value::<orchestrator_sql::RunContextReadRequest>(args)
        .context("invalid read_run_context arguments")?;
    if request.run_id.is_none() {
        request.run_id = turn_context.map(|context| context.run_id.clone());
    }
    if request.role.is_none() {
        request.role = turn_context.map(|context| context.role.clone());
    }
    if request.phase.is_none() {
        request.phase = turn_context.and_then(|context| context.phase);
    }
    if request.tickers.is_empty() {
        request.tickers = config.tickers.clone();
    }
    let mut conn = tool_connection(config)?;
    let prior_reads = turn_context
        .map(|context| count_turn_tool_results(&conn, &context.turn_id, NAME))
        .transpose()?
        .unwrap_or(0);
    if request.kind.trim().is_empty() {
        request.kind = match request.role.as_deref() {
            Some("analyst.technical") => "technical".to_string(),
            Some("analyst.news_macro") => "jin10".to_string(),
            Some(role)
                if role.starts_with("researcher.")
                    || role.starts_with("mediator.")
                    || role.starts_with("manager.")
                    || role.starts_with("risk.")
                    || matches!(role, "trader" | "portfolio.manager" | "allocation.manager") =>
            {
                "phase_summaries".to_string()
            }
            _ => String::new(),
        };
    }
    if request.role.as_deref().is_some_and(is_phase2_plus_role) {
        let allowed = matches!(
            request.kind.as_str(),
            "phase_summaries"
                | "prior_phase_summaries"
                | "phase_summary_details"
                | "attention"
                | "attention_expand"
        );
        if !allowed {
            bail!(
                "role {:?} may only call read_run_context kinds \
                 phase_summaries|phase_summary_details|attention|attention_expand; got {:?}",
                request.role,
                request.kind
            );
        }
    }
    if request.kind == "compose_context" && request.token_budget.is_none() {
        request.token_budget = Some(4096);
    }

    maybe_wait_phase00_gate(config, &request);

    let evidence = if let Some(from_mem) = try_read_phase00_from_memory(config, &request) {
        from_mem
    } else {
        orchestrator_sql::read_run_context(&mut conn, &request)?
    };
    wrap_read_run_context_evidence(config, &request, prior_reads, evidence)
}

fn maybe_wait_phase00_gate(
    config: &ExternalToolConfig,
    request: &orchestrator_sql::RunContextReadRequest,
) {
    let needs_index = matches!(
        request.kind.as_str(),
        "phase_summaries" | "prior_phase_summaries" | "phase_summary_details" | "attention_expand"
    );
    if !needs_index {
        return;
    }
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
    let ok = gate.wait_until_ready(max_prior, std::time::Duration::from_secs(600));
    let _ = ok;
}

fn is_phase2_plus_role(role: &str) -> bool {
    role.starts_with("researcher.")
        || role.starts_with("mediator.")
        || role.starts_with("manager.")
        || role.starts_with("risk.")
        || matches!(role, "trader" | "portfolio.manager" | "allocation.manager")
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
        "phase_summaries" | "prior_phase_summaries" => {
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
                .or_else(|| request.turn_id.as_deref().filter(|s| !s.is_empty()))
                .unwrap_or_default();
            if summary_id.is_empty() {
                return None;
            }
            Some(index.list_details(summary_id))
        }
        "attention_expand" => {
            let mut items = Vec::new();
            let mut any = false;
            for entry in &request.tickers {
                if let Some((kind, id)) = entry.split_once(':') {
                    let kind = kind.trim();
                    let id = id.trim();
                    if kind.is_empty() || id.is_empty() {
                        continue;
                    }
                    match kind {
                        "summary" => {
                            if let Some(v) = index.expand_summary(id) {
                                items.push(v);
                                any = true;
                            }
                        }
                        "detail" => {
                            if let Some(v) = index.expand_detail(id) {
                                items.push(v);
                                any = true;
                            }
                        }
                        _ => {}
                    }
                }
            }
            if any {
                Some(json!({
                    "query": "attention_expand",
                    "item_count": items.len(),
                    "items": items,
                    "source": "phase00_memory"
                }))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn wrap_read_run_context_evidence(
    config: &ExternalToolConfig,
    request: &orchestrator_sql::RunContextReadRequest,
    prior_reads: i64,
    evidence: Value,
) -> Result<Value> {
    if request.role.as_deref().is_some_and(|role| {
        role.starts_with("analyst.")
            || role.starts_with("researcher.")
            || role.starts_with("mediator.")
            || role.starts_with("manager.")
            || role.starts_with("risk.")
            || matches!(role, "trader" | "portfolio.manager" | "allocation.manager")
    }) {
        let tickers = if request.tickers.is_empty() {
            config.tickers.clone()
        } else {
            request.tickers.clone()
        };
        let same_default_reread = prior_reads >= 1
            && matches!(
                request.kind.as_str(),
                "technical"
                    | "jin10"
                    | "compose_context"
                    | "research_inputs"
                    | "phase_summaries"
                    | "prior_phase_summaries"
            );
        let artifact_hint = if request
            .role
            .as_deref()
            .is_some_and(|role| role.starts_with("analyst."))
        {
            "Emit one JSON object with id/role for this analyst, status=completed, and per_ticker.<TICKER>.{direction,confidence,report}. direction must be bullish|bearish|neutral|mixed|unobserved; confidence must be a 0..1 number."
        } else {
            "Emit the final JSON artifact required by the role prompt now."
        };
        if same_default_reread {
            return Ok(json!({
                "status": "stop_rereading",
                "role": request.role,
                "tickers": tickers,
                "kind": request.kind,
                "message": format!(
                    "read_run_context already returned evidence in this turn. Do not call it again unless requesting a different kind. {artifact_hint}"
                ),
                "evidence": evidence,
            }));
        }
        return Ok(json!({
            "status": "ok",
            "role": request.role,
            "tickers": tickers,
            "kind": request.kind,
            "message": format!(
                "Evidence payload only. Tickers are listed in this object and the role prompt. {artifact_hint}"
            ),
            "evidence": evidence,
        }));
    }
    Ok(evidence)
}

fn count_turn_tool_results(
    conn: &rusqlite::Connection,
    turn_id: &str,
    tool_name: &str,
) -> Result<i64> {
    let full_context_json: String = match conn.query_row(
        "SELECT full_context_json FROM agent_events WHERE turn_id = ?",
        rusqlite::params![turn_id],
        |row| row.get(0),
    ) {
        Ok(json) => json,
        Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(0),
        Err(e) => return Err(e.into()),
    };
    let items: Vec<serde_json::Value> =
        serde_json::from_str(&full_context_json).unwrap_or_default();
    let count = items
        .iter()
        .filter(|item| {
            item.get("event_type").and_then(|v| v.as_str()) == Some("tool_result")
                && item.get("tool_name").and_then(|v| v.as_str()) == Some(tool_name)
        })
        .count() as i64;
    Ok(count)
}
