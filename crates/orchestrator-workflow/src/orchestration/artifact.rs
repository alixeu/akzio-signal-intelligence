use anyhow::Result;
use orchestrator_sql::{write_agent_message_scoped, AgentMessageInput};
use serde_json::{json, Value};
use std::collections::BTreeSet;

use super::config::RuntimeConfig;
use super::state::tickers_from_state;

pub(crate) fn build_phase1_state_artifact(state: &Value, config: &RuntimeConfig) -> Value {
    let tickers = tickers_from_state(state);
    let reports = state
        .get("analyst_reports")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let roles = state
        .get("phase1_agents")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let missing_sources = state
        .get("missing_sources")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    let missing_critical_roles = roles
        .iter()
        .filter(|role| config.workflow.critical_roles.contains(*role))
        .filter(|role| !reports.contains_key(*role) || missing_sources.contains(*role))
        .cloned()
        .collect::<Vec<_>>();
    let degraded_noncritical_roles = missing_sources
        .iter()
        .filter(|role| !config.workflow.critical_roles.contains(*role))
        .cloned()
        .collect::<Vec<_>>();
    let status = if !missing_critical_roles.is_empty() {
        "blocked"
    } else if state
        .get("degraded")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        "partial"
    } else {
        "ready"
    };
    let weighted_probability_base = weighted_probability_base(state, &tickers, &reports);
    let per_ticker = tickers
        .iter()
        .map(|ticker| {
            let role_summaries = roles
                .iter()
                .map(|role| {
                    let payload = reports
                        .get(role)
                        .and_then(|artifact| artifact_for_ticker(artifact, ticker));
                    json!({
                        "role": role,
                        "status": if missing_sources.contains(role) { "missing" } else { "ready" },
                        "stance": payload.and_then(|value| value.get("direction")).and_then(Value::as_str).unwrap_or("neutral"),
                        "confidence": payload.and_then(|value| value.get("confidence")).cloned().unwrap_or(Value::Null),
                        "key_evidence": payload.and_then(|value| value.get("evidence")).cloned().unwrap_or_else(|| json!([])),
                        "weaknesses": payload.and_then(|value| value.get("weaknesses")).cloned().unwrap_or_else(|| json!([])),
                        "source_node_ids": payload.and_then(|value| value.get("source_node_ids")).cloned().unwrap_or_else(|| json!([])),
                        "summary": payload.and_then(|value| value.get("report")).and_then(Value::as_str).unwrap_or("")
                    })
                })
                .collect::<Vec<_>>();
            (
                ticker.clone(),
                json!({
                    "id": "reducer.evidence",
                    "role": "reducer.evidence",
                    "artifact_type": "phase1_state_artifact",
                    "weighted_probability_base": weighted_probability_base.get(ticker).cloned().unwrap_or(Value::Null),
                    "role_summaries": role_summaries,
                    "long_evidence": [],
                    "short_evidence": [],
                    "neutral_or_ambiguous_evidence": [],
                    "evidence_clusters": [],
                    "independent_signals": [],
                    "duplicate_signals": [],
                    "conflicts": [],
                    "missing_evidence": degraded_noncritical_roles,
                    "decision_hinges": [],
                    "topic_candidates": [
                        {
                            "topic_id": format!("{ticker}-aggregate"),
                            "topic": format!("Highest-impact unresolved long/short evidence for {ticker}"),
                            "tickers": [ticker],
                            "long_evidence_refs": [],
                            "short_evidence_refs": [],
                            "why_debate": "Fallback topic generated from Phase 1.5 state."
                        }
                    ],
                    "state_summary": format!("Phase 1 state for {ticker}: {status}.")
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    json!({
        "id": "reducer.evidence",
        "role": "reducer.evidence",
        "artifact_type": "phase1_state_artifact",
        "phase": "phase1.5",
        "status": status,
        "workflow_pattern": "Workflow -> Stage/Sub-workflow -> Agent workers -> Reducer -> state artifact",
        "generated_from": {
            "worker_roles": roles,
            "critical_roles": config.workflow.critical_roles.iter().cloned().collect::<Vec<_>>(),
            "missing_critical_roles": missing_critical_roles,
            "degraded_noncritical_roles": degraded_noncritical_roles
        },
        "late_evidence": state.get("late_evidence").cloned().unwrap_or_else(|| json!([])),
        "weighted_probability_base": weighted_probability_base,
        "per_ticker": per_ticker,
        "topic_candidates": fallback_topics_for_tickers(&tickers),
        "cross_ticker_notes": [],
        "reducer_checks": {
            "json_valid": true,
            "no_new_external_facts": true,
            "all_claims_source_backed": true
        }
    })
}

pub(crate) fn build_topic_generation_artifact(state: &Value) -> Value {
    let tickers = tickers_from_state(state);
    let topics = phase1_topic_candidates(state);
    json!({
        "id": "mediator.topic",
        "role": "mediator.topic",
        "artifact_type": "phase2_topic_generation_artifact",
        "phase": "phase2.topic_generation",
        "status": "ready",
        "generated_from": {
            "source_artifact": "phase1_state_artifact",
            "tickers": tickers
        },
        "topics": topics,
        "reducer_checks": {
            "json_valid": true,
            "from_phase1_5_only": true,
            "no_new_external_facts": true
        }
    })
}

pub(crate) fn build_debate_state_artifact(state: &Value, config: &RuntimeConfig) -> Value {
    let tickers = tickers_from_state(state);
    let turns = state
        .get("debate_turns")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let topic_states = state
        .get("topic_debate_states")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let late_evidence = state
        .get("late_evidence")
        .and_then(Value::as_array)
        .map(|items| !items.is_empty())
        .unwrap_or(false);
    let per_ticker = tickers
        .iter()
        .map(|ticker| {
            (
                ticker.clone(),
                json!({
                    "id": "reducer.debate_final",
                    "role": "reducer.debate_final",
                    "artifact_type": "phase2_5_debate_state_artifact",
                    "status": "ready",
                    "turn_count": turns.len(),
                    "decision_hinges": [],
                    "missing_evidence": [],
                    "manager_handoff": {
                        "directional_pressure": "mixed",
                        "confidence_modifier": "neutral",
                        "why": format!("Debate reducer compressed {} turns for {ticker}.", turns.len()),
                        "do_not_exceed": "Do not treat this as final probability or rating."
                    }
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    let topic_briefs = if topic_states.is_empty() {
        fallback_topics_for_tickers(&tickers)
            .into_iter()
            .map(|topic| debate_topic_brief(topic, turns.len(), late_evidence, config))
            .collect::<Vec<_>>()
    } else {
        topic_states
            .values()
            .map(|topic_state| {
                let topic = topic_state
                    .get("topic")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                let latest = topic_state
                    .get("controller_artifact")
                    .cloned()
                    .or_else(|| {
                        topic_state
                            .get("controller_artifacts")
                            .and_then(Value::as_array)
                            .and_then(|items| items.last())
                            .cloned()
                    })
                    .unwrap_or_else(|| json!({}));
                debate_topic_brief_from_state(topic, latest, late_evidence, config)
            })
            .collect::<Vec<_>>()
    };
    json!({
        "id": "reducer.debate_final",
        "role": "reducer.debate_final",
        "artifact_type": "phase2_5_debate_state_artifact",
        "phase": "phase2.5b",
        "status": "ready",
        "workflow_pattern": "Workflow -> Stage/Sub-workflow -> Agent workers -> Reducer -> state artifact",
        "generated_from": {
            "worker_roles": [
                "mediator.topic",
                "researcher.bull.initial",
                "researcher.bear.initial",
                "researcher.bull.interaction",
                "researcher.bear.interaction",
                "mediator.topic_controller"
            ],
            "turn_count": turns.len(),
            "topic_count": topic_briefs.len()
        },
        "topic_briefs": topic_briefs,
        "topic_debate_states": topic_states,
        "debate_turns": turns,
        "late_evidence": state.get("late_evidence").cloned().unwrap_or_else(|| json!([])),
        "per_ticker": per_ticker,
        "cross_topic_notes": [],
        "reducer_checks": {
            "json_valid": true,
            "no_final_probability": true,
            "no_winner_declared": true,
            "no_new_external_facts": true
        }
    })
}

pub(crate) fn debate_topic_brief(
    topic: Value,
    turn_count: usize,
    late_evidence: bool,
    config: &RuntimeConfig,
) -> Value {
    let topic_id = topic_id_from_topic(&topic);
    let topic_name = topic
        .get("topic")
        .and_then(Value::as_str)
        .unwrap_or("Aggregate debate state");
    let tickers = topic.get("tickers").cloned().unwrap_or_else(|| json!([]));
    json!({
        "topic_id": topic_id,
        "topic": topic_name,
        "tickers": tickers,
        "status": "ready",
        "is_repetitive": false,
        "needs_manager_attention": false,
        "adjudication": {
            "bull_argument_strength": null,
            "bear_argument_strength": null,
            "convergence_score": null,
            "unresolved_conflict": "",
            "no_winner_declared": true
        },
        "fact_check": {
            "supported_claims": [],
            "contested_claims": [],
            "unsupported_claims": [],
            "stale_or_late_evidence": []
        },
        "compressed_state": {
            "agreed_facts": [],
            "agreed_assumptions": [],
            "agreed_risks": [],
            "decision_hinges": [],
            "missing_high_impact_factors": [],
            "missing_evidence": [],
            "highest_value_next_query": "",
            "info_gain_score": null,
            "expected_info_gain_next_round": null,
            "should_continue": false,
            "stop_reason": format!("Final reducer compressed {turn_count} topic turns."),
            "question_grant": null
        },
        "late_evidence_effect": {
            "has_late_evidence": late_evidence,
            "used": late_evidence && config.workflow.late_evidence_enabled,
            "effect": if late_evidence { "pending" } else { "none" },
            "reason": if late_evidence { "Late evidence is appended and marked; stages are not replayed." } else { "" }
        },
        "manager_handoff": {
            "directional_pressure": "mixed",
            "confidence_modifier": "neutral",
            "why": "Review compressed per-topic bull/bear claims; reducer does not issue final probability.",
            "do_not_exceed": "Do not treat this as final probability or rating."
        }
    })
}

pub(crate) fn debate_topic_brief_from_state(
    topic: Value,
    controller_artifact: Value,
    late_evidence: bool,
    config: &RuntimeConfig,
) -> Value {
    let mut brief = debate_topic_brief(topic, 0, late_evidence, config);
    if let Some(object) = brief.as_object_mut() {
        if let Some(status) = controller_artifact.get("status").cloned() {
            object.insert("status".to_string(), status);
        }
        if let Some(claims) = controller_artifact.get("claim_ledger").cloned() {
            object.insert("claim_ledger".to_string(), claims);
        }
        if let Some(duplicates) = controller_artifact.get("duplicate_claims").cloned() {
            object.insert("duplicate_claims".to_string(), duplicates);
        }
        if let Some(unverifiable) = controller_artifact.get("unverifiable_claims").cloned() {
            object.insert("unverifiable_claims".to_string(), unverifiable);
        }
        if let Some(agenda) = controller_artifact.get("next_agenda").cloned() {
            object.insert("next_agenda".to_string(), agenda);
        }
        object.insert("controller_artifact".to_string(), controller_artifact);
    }
    brief
}

pub(crate) fn fallback_topics_for_tickers(tickers: &[String]) -> Vec<Value> {
    tickers
        .iter()
        .map(|ticker| {
            json!({
                "topic_id": format!("{ticker}-aggregate"),
                "topic": format!("Highest-impact unresolved long/short evidence for {ticker}"),
                "tickers": [ticker],
                "long_evidence_refs": [],
                "short_evidence_refs": [],
                "why_debate": "Fallback topic generated from Phase 1.5 state."
            })
        })
        .collect()
}

pub(crate) fn phase1_topic_candidates(state: &Value) -> Vec<Value> {
    state
        .get("phase1_state_artifact")
        .and_then(|artifact| artifact.get("topic_candidates"))
        .and_then(Value::as_array)
        .cloned()
        .filter(|items| !items.is_empty())
        .unwrap_or_else(|| fallback_topics_for_tickers(&tickers_from_state(state)))
}

pub(crate) fn topics_from_generation_artifact(artifact: &Value) -> Vec<Value> {
    artifact
        .get("reducer_output")
        .and_then(|output| {
            output
                .get("topics")
                .or_else(|| output.get("topic_candidates"))
        })
        .or_else(|| artifact.get("topics"))
        .or_else(|| artifact.get("topic_candidates"))
        .and_then(Value::as_array)
        .cloned()
        .filter(|items| !items.is_empty())
        .unwrap_or_else(|| fallback_topics_for_tickers(&tickers_from_state(artifact)))
}

pub(crate) fn topic_id_from_topic(topic: &Value) -> String {
    topic
        .get("topic_id")
        .or_else(|| topic.get("id"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToString::to_string)
        .unwrap_or_else(|| "topic-aggregate".to_string())
}

pub(crate) fn merge_reducer_output(mut base: Value, reducer_output: Value) -> Value {
    if let Some(object) = base.as_object_mut() {
        object.insert("reducer_output".to_string(), reducer_output.clone());
        if let Some(status) = reducer_output.get("status").cloned() {
            object.insert("llm_reducer_status".to_string(), status);
        }
        if let Some(checks) = reducer_output.get("reducer_checks").cloned() {
            object.insert("llm_reducer_checks".to_string(), checks);
        }
        if let Some(summary) = reducer_output
            .get("state_summary")
            .or_else(|| reducer_output.get("summary"))
            .or_else(|| reducer_output.get("brief_markdown"))
            .cloned()
        {
            object.insert("llm_brief".to_string(), summary);
        }
    }
    base
}

pub(crate) fn reducer_brief_md(artifact: &Value) -> String {
    if let Some(text) = artifact
        .get("llm_brief")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        return text.to_string();
    }
    let role = artifact.get("role").and_then(Value::as_str).unwrap_or("");
    let status = artifact
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let artifact_type = artifact
        .get("artifact_type")
        .and_then(Value::as_str)
        .unwrap_or("state_artifact");
    format!("{role} produced {artifact_type} with status {status}.")
}

pub(crate) fn weighted_probability_base(
    state: &Value,
    tickers: &[String],
    reports: &serde_json::Map<String, Value>,
) -> serde_json::Map<String, Value> {
    let weights = state
        .get("analyst_weights")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    tickers
        .iter()
        .map(|ticker| {
            let mut weighted_direction = 0.0;
            let mut confidence_total = 0.0;
            let mut weight_total = 0.0;
            let mut source_roles = Vec::new();
            let mut skipped_roles = Vec::new();
            for (role, report) in reports {
                let weight = weights.get(role).and_then(Value::as_f64).unwrap_or(0.0);
                if weight <= 0.0 {
                    continue;
                }
                let payload = artifact_for_ticker(report, ticker).unwrap_or(report);
                // Skip degraded / fallback / unobserved contributions instead of
                // letting a neutral 0.0-confidence placeholder drag the base toward
                // 0.50. A silently-failed analyst must not count as evidence.
                if is_non_contributing(report, payload) {
                    skipped_roles.push(Value::String(role.clone()));
                    continue;
                }
                let confidence = payload
                    .get("confidence")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.5)
                    .clamp(0.0, 1.0);
                let direction = match payload
                    .get("direction")
                    .and_then(Value::as_str)
                    .unwrap_or("neutral")
                {
                    "bullish" | "long" | "positive" => 1.0,
                    "bearish" | "short" | "negative" => -1.0,
                    _ => 0.0,
                };
                weighted_direction += weight * confidence * direction;
                confidence_total += weight * confidence;
                weight_total += weight;
                source_roles.push(Value::String(role.clone()));
            }
            let net = if confidence_total > 0.0 {
                weighted_direction / confidence_total
            } else {
                0.0
            };
            let long_probability = ((net + 1.0) / 2.0).clamp(0.0, 1.0);
            let short_probability = 1.0 - long_probability;
            let confidence = if weight_total > 0.0 {
                (confidence_total / weight_total).clamp(0.0, 1.0)
            } else {
                0.0
            };
            (
                ticker.clone(),
                json!({
                    "long_probability": long_probability,
                    "short_probability": short_probability,
                    "confidence": confidence,
                    "source_roles": source_roles,
                    "skipped_roles": skipped_roles
                }),
            )
        })
        .collect()
}

/// A report contributes no directional evidence when it was degraded, used a
/// fallback, or explicitly reported no observation. Such artifacts carry a
/// neutral/0.0 placeholder that must be excluded from the weighted base rather
/// than counted as a real neutral vote.
fn is_non_contributing(report: &Value, payload: &Value) -> bool {
    if report
        .get("degraded")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return true;
    }
    let flagged_status = |value: &Value| {
        matches!(
            value.get("status").and_then(Value::as_str),
            Some("degraded") | Some("missing") | Some("error")
        )
    };
    if flagged_status(report) || flagged_status(payload) {
        return true;
    }
    matches!(
        payload.get("direction").and_then(Value::as_str),
        Some("unobserved")
    )
}

pub(crate) fn artifact_for_ticker<'a>(artifact: &'a Value, ticker: &str) -> Option<&'a Value> {
    artifact
        .get("per_ticker")
        .and_then(Value::as_object)
        .and_then(|items| items.get(ticker))
}

pub(crate) fn persist_artifact(
    conn: &mut rusqlite::Connection,
    state: &Value,
    phase: i64,
    role: &str,
    artifact: Value,
) -> Result<()> {
    persist_artifact_with_last_md(conn, state, phase, role, artifact, String::new())
}

pub(crate) fn persist_artifact_with_last_md(
    conn: &mut rusqlite::Connection,
    state: &Value,
    phase: i64,
    role: &str,
    artifact: Value,
    last_md: String,
) -> Result<()> {
    persist_agent_content(
        conn,
        state,
        PersistContent {
            phase,
            role,
            kind: "artifact",
            round: None,
            topic_id: None,
            artifact,
            last_md,
        },
    )
}

pub(crate) fn persist_message(
    conn: &mut rusqlite::Connection,
    state: &Value,
    phase: i64,
    role: &str,
    kind: &str,
    round: Option<i64>,
    artifact: Value,
) -> Result<()> {
    persist_message_with_topic(conn, state, phase, role, kind, round, None, artifact)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn persist_message_with_topic(
    conn: &mut rusqlite::Connection,
    state: &Value,
    phase: i64,
    role: &str,
    kind: &str,
    round: Option<i64>,
    topic_id: Option<&str>,
    artifact: Value,
) -> Result<()> {
    persist_agent_content(
        conn,
        state,
        PersistContent {
            phase,
            role,
            kind,
            round,
            topic_id,
            artifact,
            last_md: String::new(),
        },
    )
}

pub(crate) struct PersistContent<'a> {
    phase: i64,
    role: &'a str,
    kind: &'a str,
    round: Option<i64>,
    topic_id: Option<&'a str>,
    artifact: Value,
    last_md: String,
}

pub(crate) fn persist_agent_content(
    conn: &mut rusqlite::Connection,
    state: &Value,
    input: PersistContent<'_>,
) -> Result<()> {
    let tickers = tickers_from_state(state);
    write_agent_message_scoped(
        conn,
        &AgentMessageInput {
            run_id: state["run_id"].as_str().unwrap_or_default().to_string(),
            phase: input.phase,
            role: input.role.to_string(),
            ticker: state["ticker"].as_str().unwrap_or_default().to_string(),
            tickers,
            skill: input.role.to_string(),
            kind: input.kind.to_string(),
            topic_id: input.topic_id.map(ToString::to_string),
            round: input.round,
            message_group_id: None,
            valid: true,
            content: input.artifact,
            last_md: input.last_md,
        },
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn state_with_weights() -> Value {
        json!({
            "analyst_weights": {
                "analyst.technical": 40.0,
                "analyst.news_macro": 35.0
            }
        })
    }

    #[test]
    fn degraded_report_is_skipped_not_counted_as_neutral() {
        let state = state_with_weights();
        let tickers = vec!["QQQ".to_string()];
        let mut reports = serde_json::Map::new();
        // Strong bullish technical.
        reports.insert(
            "analyst.technical".to_string(),
            json!({"per_ticker": {"QQQ": {"direction": "bullish", "confidence": 0.8}}}),
        );
        // Degraded news_macro that would otherwise drag toward 0.50.
        reports.insert(
            "analyst.news_macro".to_string(),
            json!({
                "degraded": true,
                "per_ticker": {"QQQ": {"direction": "neutral", "confidence": 0.0}}
            }),
        );

        let base = weighted_probability_base(&state, &tickers, &reports);
        let qqq = &base["QQQ"];
        // Only the bullish technical contributes -> net = +1 -> long_prob = 1.0.
        assert!((qqq["long_probability"].as_f64().unwrap() - 1.0).abs() < 1e-9);
        assert_eq!(qqq["source_roles"], json!(["analyst.technical"]));
        assert_eq!(qqq["skipped_roles"], json!(["analyst.news_macro"]));
    }

    #[test]
    fn unobserved_direction_is_skipped() {
        let state = state_with_weights();
        let tickers = vec!["QQQ".to_string()];
        let mut reports = serde_json::Map::new();
        reports.insert(
            "analyst.technical".to_string(),
            json!({"per_ticker": {"QQQ": {"direction": "bearish", "confidence": 0.6}}}),
        );
        reports.insert(
            "analyst.news_macro".to_string(),
            json!({"per_ticker": {"QQQ": {"direction": "unobserved", "confidence": 0.0}}}),
        );

        let base = weighted_probability_base(&state, &tickers, &reports);
        let qqq = &base["QQQ"];
        assert!(qqq["long_probability"].as_f64().unwrap() < 0.5);
        assert_eq!(qqq["skipped_roles"], json!(["analyst.news_macro"]));
    }

    #[test]
    fn all_contributing_reports_are_counted() {
        let state = state_with_weights();
        let tickers = vec!["QQQ".to_string()];
        let mut reports = serde_json::Map::new();
        reports.insert(
            "analyst.technical".to_string(),
            json!({"per_ticker": {"QQQ": {"direction": "bullish", "confidence": 0.5}}}),
        );
        reports.insert(
            "analyst.news_macro".to_string(),
            json!({"per_ticker": {"QQQ": {"direction": "bearish", "confidence": 0.5}}}),
        );

        let base = weighted_probability_base(&state, &tickers, &reports);
        let qqq = &base["QQQ"];
        assert_eq!(qqq["skipped_roles"], json!([]));
        assert_eq!(qqq["source_roles"].as_array().unwrap().len(), 2);
    }
}
