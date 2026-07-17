//! Post-phase compressor: materialize summary → detail index after each phase.

use anyhow::Result;
use orchestrator_sql::{
    clear_phase_compress, upsert_phase_summary, upsert_phase_summary_detail,
    PhaseSummaryDetailInput, PhaseSummaryInput, AGGREGATE_TICKER,
};
use rusqlite::Connection;
use serde_json::{json, Value};
use tracing::debug;

use super::state::tickers_from_state;

/// Deterministic post-phase compression into `phase_summaries` + `phase_summary_details`.
/// Returns number of summary/detail rows written.
pub(crate) fn compress_phase(
    conn: &Connection,
    state: &mut Value,
    source_phase: i64,
) -> Result<usize> {
    let run_id = state
        .get("run_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if run_id.is_empty() {
        return Ok(0);
    }
    clear_phase_compress(conn, &run_id, source_phase)?;
    let written = match source_phase {
        1 => compress_phase1(conn, &run_id, state)?,
        2 => compress_phase2(conn, &run_id, state)?,
        3 => compress_generic(
            conn,
            &run_id,
            3,
            "manager.research",
            state.get("research_plan"),
        )?,
        4 => compress_generic(
            conn,
            &run_id,
            4,
            "trader",
            state.get("trader_investment_plan"),
        )?,
        5 => compress_generic(conn, &run_id, 5, "risk", state.get("risk_debate_state"))?,
        6 => compress_generic(
            conn,
            &run_id,
            6,
            "portfolio.manager",
            state.get("final_trade_decision"),
        )?,
        7 => compress_generic(
            conn,
            &run_id,
            7,
            "allocation.manager",
            state
                .get("allocation_result")
                .or_else(|| state.get("portfolio_allocation"))
                .or_else(|| state.get("allocation")),
        )?,
        _ => 0,
    };
    if !state.get("phase_compress").is_some_and(Value::is_object) {
        state["phase_compress"] = json!({});
    }
    if let Some(map) = state["phase_compress"].as_object_mut() {
        map.insert(
            source_phase.to_string(),
            json!({ "written": written, "status": "done" }),
        );
    }
    debug!(source_phase, written, "post-phase compression complete");
    Ok(written)
}

fn compress_phase1(conn: &Connection, run_id: &str, state: &Value) -> Result<usize> {
    let mut written = 0usize;
    let phase1 = state.get("phase1_state_artifact").unwrap_or(&Value::Null);
    let brief = state
        .get("phase1_brief_md")
        .and_then(Value::as_str)
        .unwrap_or("");
    if !brief.is_empty() {
        let sid = upsert_phase_summary(
            conn,
            &PhaseSummaryInput {
                run_id: run_id.to_string(),
                source_phase: 1,
                role: "compressor".to_string(),
                ticker: AGGREGATE_TICKER.to_string(),
                topic_id: None,
                summary: truncate(brief, 500),
                summary_json: json!({
                    "status": phase1.get("status"),
                    "evidence_quality": phase1.get("evidence_quality"),
                }),
                confidence: phase1
                    .get("evidence_quality")
                    .and_then(|q| q.get("status"))
                    .and_then(Value::as_str)
                    .map(|s| match s {
                        "actionable" => 0.8,
                        "partial" => 0.55,
                        _ => 0.35,
                    })
                    .unwrap_or(0.5),
            },
        )?;
        written += 1;
        if let Some(eq) = phase1.get("evidence_quality") {
            upsert_phase_summary_detail(
                conn,
                &PhaseSummaryDetailInput {
                    summary_id: sid,
                    run_id: run_id.to_string(),
                    source_phase: 1,
                    detail: format!(
                        "evidence_quality={}",
                        eq.get("status")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown")
                    ),
                    detail_json: eq.clone(),
                    source_ref: "reducer.evidence".to_string(),
                    sort_order: 0,
                },
            )?;
            written += 1;
        }
    }

    let tickers = tickers_from_state(state);
    let per_ticker = phase1
        .get("per_ticker")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    for ticker in tickers {
        let Some(payload) = per_ticker.get(&ticker) else {
            continue;
        };
        let summary_text = payload
            .get("state_summary")
            .and_then(Value::as_str)
            .unwrap_or("phase1 ticker summary")
            .to_string();
        let sid = upsert_phase_summary(
            conn,
            &PhaseSummaryInput {
                run_id: run_id.to_string(),
                source_phase: 1,
                role: "compressor".to_string(),
                ticker: ticker.clone(),
                topic_id: None,
                summary: summary_text,
                summary_json: json!({
                    "weighted_probability_base": payload.get("weighted_probability_base"),
                    "evidence_quality": payload.get("evidence_quality"),
                }),
                confidence: payload
                    .get("weighted_probability_base")
                    .and_then(|w| w.get("confidence"))
                    .and_then(Value::as_f64)
                    .unwrap_or(0.5),
            },
        )?;
        written += 1;
        let mut order = 0i64;
        if let Some(roles) = payload.get("role_summaries").and_then(Value::as_array) {
            for role_sum in roles {
                let role = role_sum
                    .get("role")
                    .and_then(Value::as_str)
                    .unwrap_or("analyst");
                let stance = role_sum
                    .get("stance")
                    .and_then(Value::as_str)
                    .unwrap_or("neutral");
                let text = role_sum
                    .get("summary")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let detail = if text.is_empty() {
                    format!("{role}: stance={stance}")
                } else {
                    format!("{role} [{stance}]: {}", truncate(text, 400))
                };
                upsert_phase_summary_detail(
                    conn,
                    &PhaseSummaryDetailInput {
                        summary_id: sid.clone(),
                        run_id: run_id.to_string(),
                        source_phase: 1,
                        detail,
                        detail_json: role_sum.clone(),
                        source_ref: role.to_string(),
                        sort_order: order,
                    },
                )?;
                order += 1;
                written += 1;
            }
        }
        if let Some(conflicts) = payload
            .get("cross_analyst_conflicts")
            .and_then(Value::as_array)
        {
            for conflict in conflicts {
                upsert_phase_summary_detail(
                    conn,
                    &PhaseSummaryDetailInput {
                        summary_id: sid.clone(),
                        run_id: run_id.to_string(),
                        source_phase: 1,
                        detail: format!(
                            "conflict: {}",
                            conflict
                                .get("conflict_type")
                                .or_else(|| conflict.get("type"))
                                .and_then(Value::as_str)
                                .unwrap_or("unknown")
                        ),
                        detail_json: conflict.clone(),
                        source_ref: "cross_analyst_conflicts".to_string(),
                        sort_order: order,
                    },
                )?;
                order += 1;
                written += 1;
            }
        }
    }
    Ok(written)
}

fn compress_phase2(conn: &Connection, run_id: &str, state: &Value) -> Result<usize> {
    let mut written = 0usize;
    let debate = state.get("debate_state_artifact").unwrap_or(&Value::Null);
    let brief = state
        .get("debate_brief_md")
        .and_then(Value::as_str)
        .unwrap_or("");
    if !brief.is_empty() || !debate.is_null() {
        let sid = upsert_phase_summary(
            conn,
            &PhaseSummaryInput {
                run_id: run_id.to_string(),
                source_phase: 2,
                role: "compressor".to_string(),
                ticker: AGGREGATE_TICKER.to_string(),
                topic_id: None,
                summary: if brief.is_empty() {
                    format!(
                        "phase2 debate status={}",
                        debate
                            .get("status")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown")
                    )
                } else {
                    truncate(brief, 500)
                },
                summary_json: json!({
                    "status": debate.get("status"),
                    "convergence_status": debate.get("convergence_status"),
                }),
                confidence: 0.55,
            },
        )?;
        written += 1;
        if let Some(briefs) = debate.get("topic_briefs").and_then(Value::as_array) {
            for (i, tb) in briefs.iter().enumerate() {
                upsert_phase_summary_detail(
                    conn,
                    &PhaseSummaryDetailInput {
                        summary_id: sid.clone(),
                        run_id: run_id.to_string(),
                        source_phase: 2,
                        detail: format!(
                            "topic {}",
                            tb.get("topic_id")
                                .or_else(|| tb.get("topic"))
                                .and_then(Value::as_str)
                                .unwrap_or("unknown")
                        ),
                        detail_json: tb.clone(),
                        source_ref: "topic_briefs".to_string(),
                        sort_order: i as i64,
                    },
                )?;
                written += 1;
            }
        }
    }

    // Per-topic generation topics
    if let Some(topics) = state
        .get("topic_generation_artifact")
        .and_then(|a| a.get("topics"))
        .and_then(Value::as_array)
    {
        for topic in topics {
            let topic_id = topic
                .get("topic_id")
                .and_then(Value::as_str)
                .unwrap_or("topic")
                .to_string();
            let text = topic
                .get("topic")
                .and_then(Value::as_str)
                .unwrap_or(topic_id.as_str())
                .to_string();
            let sid = upsert_phase_summary(
                conn,
                &PhaseSummaryInput {
                    run_id: run_id.to_string(),
                    source_phase: 2,
                    role: "mediator.topic".to_string(),
                    ticker: topic
                        .get("tickers")
                        .and_then(Value::as_array)
                        .and_then(|a| a.first())
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                    topic_id: Some(topic_id.clone()),
                    summary: text,
                    summary_json: topic.clone(),
                    confidence: 0.6,
                },
            )?;
            written += 1;
            if let Some(hinge) = topic.get("decision_hinge").and_then(Value::as_str) {
                upsert_phase_summary_detail(
                    conn,
                    &PhaseSummaryDetailInput {
                        summary_id: sid,
                        run_id: run_id.to_string(),
                        source_phase: 2,
                        detail: format!("decision_hinge: {hinge}"),
                        detail_json: json!({"decision_hinge": hinge}),
                        source_ref: "topic_generation".to_string(),
                        sort_order: 0,
                    },
                )?;
                written += 1;
            }
        }
    }
    Ok(written)
}

fn compress_generic(
    conn: &Connection,
    run_id: &str,
    source_phase: i64,
    role: &str,
    artifact: Option<&Value>,
) -> Result<usize> {
    let Some(artifact) = artifact else {
        return Ok(0);
    };
    if artifact.is_null() {
        return Ok(0);
    }
    let summary = artifact
        .get("summary")
        .or_else(|| artifact.get("execution_summary"))
        .or_else(|| artifact.get("rationale"))
        .or_else(|| artifact.get("argument"))
        .and_then(Value::as_str)
        .map(|s| truncate(s, 500))
        .unwrap_or_else(|| format!("{role} phase {source_phase} artifact"));
    let conf = artifact
        .get("confidence")
        .and_then(Value::as_f64)
        .unwrap_or(0.5);
    let sid = upsert_phase_summary(
        conn,
        &PhaseSummaryInput {
            run_id: run_id.to_string(),
            source_phase,
            role: role.to_string(),
            ticker: AGGREGATE_TICKER.to_string(),
            topic_id: None,
            summary: summary.clone(),
            summary_json: artifact.clone(),
            confidence: conf,
        },
    )?;
    let mut written = 1usize;
    // Flatten a few top-level fields as details
    if let Some(obj) = artifact.as_object() {
        let mut order = 0i64;
        for key in [
            "rating",
            "status",
            "action",
            "stance",
            "convergence_status",
            "hold_reason",
        ] {
            if let Some(v) = obj.get(key) {
                upsert_phase_summary_detail(
                    conn,
                    &PhaseSummaryDetailInput {
                        summary_id: sid.clone(),
                        run_id: run_id.to_string(),
                        source_phase,
                        detail: format!("{key}={v}"),
                        detail_json: json!({ key: v }),
                        source_ref: role.to_string(),
                        sort_order: order,
                    },
                )?;
                order += 1;
                written += 1;
            }
        }
    }
    Ok(written)
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let clipped: String = s.chars().take(max.saturating_sub(1)).collect();
    format!("{clipped}…")
}
