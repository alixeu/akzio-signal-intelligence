//! Post-phase compressor: build summary → detail index in memory; SQLite flush at run end.

use anyhow::Result;
use orchestrator_sql::{
    Phase00MemoryIndex, Phase00PhaseBatch, PhaseSummaryDetailInput, PhaseSummaryInput,
    AGGREGATE_TICKER,
};
use rusqlite::Connection;
use serde_json::{json, Value};
use tracing::debug;

use super::lifecycle::tickers_from_state;

/// Build a deterministic phase00 batch from in-memory phase artifacts (no DB I/O).
pub(crate) fn build_phase_compress(state: &Value, source_phase: i64) -> Result<Phase00PhaseBatch> {
    let run_id = state
        .get("run_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if run_id.is_empty() {
        return Ok(Phase00PhaseBatch {
            source_phase,
            ..Default::default()
        });
    }
    let batch = match source_phase {
        1 => build_phase1(&run_id, state),
        2 => build_phase2(&run_id, state),
        3 => build_generic(
            &run_id,
            3,
            "manager.research",
            state.get("research_plan"),
            RESEARCH_FIELDS,
        ),
        4 => build_generic(
            &run_id,
            4,
            "trader",
            state.get("trader_investment_plan"),
            TRADER_FIELDS,
        ),
        5 => build_generic(
            &run_id,
            5,
            "risk",
            state.get("risk_debate_state"),
            RISK_FIELDS,
        ),
        6 => build_generic(
            &run_id,
            6,
            "portfolio.manager",
            state.get("final_trade_decision"),
            PORTFOLIO_FIELDS,
        ),
        7 => build_generic(
            &run_id,
            7,
            "allocation.manager",
            state
                .get("allocation_result")
                .or_else(|| state.get("portfolio_allocation"))
                .or_else(|| state.get("allocation")),
            ALLOCATION_FIELDS,
        ),
        _ => Phase00PhaseBatch {
            source_phase,
            ..Default::default()
        },
    };
    debug!(
        source_phase,
        written = batch.written(),
        "phase00 compress built in memory"
    );
    Ok(batch)
}

/// Merge a built batch into `state.phase00_memory` / `phase00_tables` / `phase_compress`.
pub(crate) fn apply_phase00_batch(state: &mut Value, batch: Phase00PhaseBatch) -> Result<Value> {
    let source_phase = batch.source_phase;
    let written = batch.written();
    let snapshot = batch.debug_snapshot();

    let mut index = state
        .get("phase00_memory")
        .map(Phase00MemoryIndex::from_state_value)
        .unwrap_or_else(|| {
            Phase00MemoryIndex::new(
                state
                    .get("run_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            )
        });
    index.merge(batch);
    state["phase00_memory"] = index.to_state_value();

    if !state.get("phase_compress").is_some_and(Value::is_object) {
        state["phase_compress"] = json!({});
    }
    if let Some(map) = state["phase_compress"].as_object_mut() {
        map.insert(
            source_phase.to_string(),
            json!({ "written": written, "status": "done", "persisted": false }),
        );
    }
    if !state.get("phase00_tables").is_some_and(Value::is_object) {
        state["phase00_tables"] = json!({});
    }
    if let Some(map) = state["phase00_tables"].as_object_mut() {
        map.insert(source_phase.to_string(), snapshot.clone());
    }
    Ok(snapshot)
}

/// Flush the full in-memory phase00 index to SQLite (run end).
pub(crate) fn flush_phase00_to_sqlite(conn: &Connection, state: &mut Value) -> Result<usize> {
    let Some(raw) = state.get("phase00_memory") else {
        return Ok(0);
    };
    let index = Phase00MemoryIndex::from_state_value(raw);
    if index.phases.is_empty() {
        return Ok(0);
    }
    let written = index.flush(conn)?;
    if let Some(map) = state
        .get_mut("phase_compress")
        .and_then(Value::as_object_mut)
    {
        for (phase, status) in map.iter_mut() {
            if let Some(obj) = status.as_object_mut() {
                obj.insert("persisted".into(), json!(true));
            }
            let _ = phase;
        }
    }
    // Mark snapshots as persisted.
    if let Some(tables) = state
        .get_mut("phase00_tables")
        .and_then(Value::as_object_mut)
    {
        for (_k, snap) in tables.iter_mut() {
            if let Some(obj) = snap.as_object_mut() {
                obj.insert("persisted".into(), json!(true));
            }
        }
    }
    debug!(written, "phase00 memory flushed to sqlite");
    Ok(written)
}

/// Legacy helper: build + apply only (no DB). Prefer `build_phase_compress` + `apply_phase00_batch`.
#[allow(dead_code)]
pub(crate) fn compress_phase_in_memory(
    state: &mut Value,
    source_phase: i64,
) -> Result<(usize, Value)> {
    let batch = build_phase_compress(state, source_phase)?;
    let written = batch.written();
    let snapshot = apply_phase00_batch(state, batch)?;
    Ok((written, snapshot))
}

const RESEARCH_FIELDS: &[&str] = &[
    "rating",
    "long_probability",
    "short_probability",
    "confidence",
    "confidence_basis",
    "hold_reason",
    "base_probability",
    "debate_adjustment",
    "final_probability",
    "dominant_driver",
    "why_now",
    "why_not_already_priced",
    "probability_rationale",
    "adjustment_rationale",
    "scenarios",
    "plan",
    "data_gaps",
    "risk_flags",
    "tail_risk_flag",
    "missing_data_premium",
    "per_ticker",
    "summary",
];

const TRADER_FIELDS: &[&str] = &[
    "action",
    "position_size",
    "entry_price",
    "stop_loss",
    "rationale",
    "status",
    "summary",
];

const RISK_FIELDS: &[&str] = &[
    "history",
    "status",
    "summary",
    "convergence_status",
    "recommended_adjustment",
];

const PORTFOLIO_FIELDS: &[&str] = &[
    "rating",
    "execution_summary",
    "investment_thesis",
    "risk_controls",
    "rationale",
    "action",
    "summary",
];

const ALLOCATION_FIELDS: &[&str] = &[
    "weights",
    "total_equity_exposure",
    "vix_regime",
    "summary",
    "allocation_method",
    "correlation_note",
];

fn build_phase1(run_id: &str, state: &Value) -> Phase00PhaseBatch {
    let mut batch = Phase00PhaseBatch {
        source_phase: 1,
        ..Default::default()
    };
    let phase1 = state.get("phase1_index").unwrap_or(&Value::Null);
    let brief = state
        .get("phase1_brief_md")
        .and_then(Value::as_str)
        .unwrap_or("");
    if !brief.is_empty() {
        let sid = batch.push_summary(&PhaseSummaryInput {
            run_id: run_id.to_string(),
            source_phase: 1,
            role: "compressor".to_string(),
            ticker: AGGREGATE_TICKER.to_string(),
            topic_id: None,
            summary: truncate(brief, 500),
            summary_json: json!({
                "status": phase1.get("status"),
                "evidence_quality": phase1.get("evidence_quality"),
                "weighted_probability_base": phase1.get("weighted_probability_base"),
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
        });
        if let Some(eq) = phase1.get("evidence_quality") {
            batch.push_detail(&PhaseSummaryDetailInput {
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
                source_ref: "phase1.index".to_string(),
                sort_order: 0,
            });
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
        let sid = batch.push_summary(&PhaseSummaryInput {
            run_id: run_id.to_string(),
            source_phase: 1,
            role: "compressor".to_string(),
            ticker: ticker.clone(),
            topic_id: None,
            summary: summary_text,
            summary_json: json!({
                "evidence_quality": payload.get("evidence_quality"),
                "weighted_probability_base": payload.get("weighted_probability_base"),
                "usable_source_roles": payload
                    .get("evidence_quality")
                    .and_then(|q| q.get("usable_source_roles")),
                "role_summaries": payload.get("role_summaries"),
                "cross_analyst_conflicts": payload.get("cross_analyst_conflicts"),
            }),
            confidence: payload
                .get("role_summaries")
                .and_then(Value::as_array)
                .map(|roles| {
                    let confs: Vec<f64> = roles
                        .iter()
                        .filter_map(|r| r.get("confidence").and_then(Value::as_f64))
                        .collect();
                    if confs.is_empty() {
                        0.5
                    } else {
                        confs.iter().sum::<f64>() / confs.len() as f64
                    }
                })
                .unwrap_or(0.5),
        });
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
                batch.push_detail(&PhaseSummaryDetailInput {
                    summary_id: sid.clone(),
                    run_id: run_id.to_string(),
                    source_phase: 1,
                    detail,
                    detail_json: role_sum.clone(),
                    source_ref: role.to_string(),
                    sort_order: order,
                });
                order += 1;
            }
        }
        if let Some(conflicts) = payload
            .get("cross_analyst_conflicts")
            .and_then(Value::as_array)
        {
            for conflict in conflicts {
                batch.push_detail(&PhaseSummaryDetailInput {
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
                });
                order += 1;
            }
        }
    }
    batch
}

fn build_phase2(run_id: &str, state: &Value) -> Phase00PhaseBatch {
    let mut batch = Phase00PhaseBatch {
        source_phase: 2,
        ..Default::default()
    };
    let debate = state.get("debate_state_artifact").unwrap_or(&Value::Null);
    let brief = state
        .get("debate_brief_md")
        .and_then(Value::as_str)
        .unwrap_or("");
    if !brief.is_empty() || !debate.is_null() {
        let sid = batch.push_summary(&PhaseSummaryInput {
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
                "topic_briefs": debate.get("topic_briefs"),
                "per_ticker": debate.get("per_ticker"),
            }),
            confidence: 0.55,
        });
        if let Some(briefs) = debate.get("topic_briefs").and_then(Value::as_array) {
            for (i, tb) in briefs.iter().enumerate() {
                batch.push_detail(&PhaseSummaryDetailInput {
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
                });
            }
        }
    }

    if let Some(common_ground) = state
        .get("topic_generation_artifact")
        .and_then(|a| a.get("common_ground"))
    {
        batch.push_summary(&PhaseSummaryInput {
            run_id: run_id.to_string(),
            source_phase: 2,
            role: "mediator.topic".to_string(),
            ticker: AGGREGATE_TICKER.to_string(),
            topic_id: Some("common_ground".to_string()),
            summary: "phase2 common_ground".to_string(),
            summary_json: common_ground.clone(),
            confidence: 0.7,
        });
    }

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
            let sid = batch.push_summary(&PhaseSummaryInput {
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
            });
            if let Some(hinge) = topic.get("decision_hinge").and_then(Value::as_str) {
                batch.push_detail(&PhaseSummaryDetailInput {
                    summary_id: sid,
                    run_id: run_id.to_string(),
                    source_phase: 2,
                    detail: format!("decision_hinge: {hinge}"),
                    detail_json: json!({"decision_hinge": hinge}),
                    source_ref: "topic_generation".to_string(),
                    sort_order: 0,
                });
            }
        }
    }
    batch
}

fn build_generic(
    run_id: &str,
    source_phase: i64,
    role: &str,
    artifact: Option<&Value>,
    keep_fields: &[&str],
) -> Phase00PhaseBatch {
    let mut batch = Phase00PhaseBatch {
        source_phase,
        ..Default::default()
    };
    let Some(artifact) = artifact else {
        return batch;
    };
    if artifact.is_null() {
        return batch;
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
    let compact = compact_fields(artifact, keep_fields);
    let sid = batch.push_summary(&PhaseSummaryInput {
        run_id: run_id.to_string(),
        source_phase,
        role: role.to_string(),
        ticker: AGGREGATE_TICKER.to_string(),
        topic_id: None,
        summary: summary.clone(),
        summary_json: compact,
        confidence: conf,
    });
    if let Some(obj) = artifact.as_object() {
        let mut order = 0i64;
        for key in [
            "rating",
            "status",
            "action",
            "stance",
            "convergence_status",
            "hold_reason",
            "long_probability",
            "short_probability",
            "position_size",
        ] {
            if let Some(v) = obj.get(key) {
                batch.push_detail(&PhaseSummaryDetailInput {
                    summary_id: sid.clone(),
                    run_id: run_id.to_string(),
                    source_phase,
                    detail: format!("{key}={v}"),
                    detail_json: json!({ key: v }),
                    source_ref: role.to_string(),
                    sort_order: order,
                });
                order += 1;
            }
        }
    }
    batch
}

fn compact_fields(value: &Value, fields: &[&str]) -> Value {
    let Some(obj) = value.as_object() else {
        return value.clone();
    };
    let mut out = serde_json::Map::new();
    for field in fields {
        if let Some(v) = obj.get(*field) {
            out.insert((*field).to_string(), v.clone());
        }
    }
    // Always keep per_ticker if present and requested via wildcard-ish: already in fields.
    if out.is_empty() {
        value.clone()
    } else {
        Value::Object(out)
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let clipped: String = s.chars().take(max.saturating_sub(1)).collect();
    format!("{clipped}…")
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_sql::{connect, ensure_schema};
    use serde_json::json;

    #[test]
    fn build_phase1_does_not_write_sqlite() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("t.sqlite");
        let conn = connect(&path).unwrap();
        ensure_schema(&conn).unwrap();

        let state = json!({
            "run_id": "run-mem",
            "tickers": ["QQQ"],
            "phase1_brief_md": "brief about QQQ",
            "phase1_index": {
                "status": "done",
                "evidence_quality": {"status": "actionable"},
                "per_ticker": {
                    "QQQ": {
                        "state_summary": "QQQ mixed",
                        "role_summaries": [
                            {"role": "analyst.technical", "stance": "bullish", "summary": "up", "confidence": 0.7}
                        ]
                    }
                }
            }
        });
        let batch = build_phase_compress(&state, 1).unwrap();
        assert!(batch.written() >= 1);

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM phase_summaries", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);

        let mut state = state;
        apply_phase00_batch(&mut state, batch).unwrap();
        assert!(state.get("phase00_memory").is_some());
        assert_eq!(state["phase_compress"]["1"]["persisted"], false);

        let flushed = flush_phase00_to_sqlite(&conn, &mut state).unwrap();
        assert!(flushed >= 1);
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM phase_summaries", [], |r| r.get(0))
            .unwrap();
        assert!(count >= 1);
        assert_eq!(state["phase_compress"]["1"]["persisted"], true);
    }
}
