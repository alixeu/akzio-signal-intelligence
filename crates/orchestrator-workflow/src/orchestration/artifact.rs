use anyhow::{bail, Result};
use orchestrator_sql::{write_agent_message_scoped, AgentMessageInput};
use serde_json::{json, Value};
use std::collections::BTreeSet;

use super::config::RuntimeConfig;
use super::conflict_detection::detect_all_conflicts;
use super::lifecycle::tickers_from_state;

pub(crate) fn build_phase1_index(state: &Value, config: &RuntimeConfig) -> Value {
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
    // Phase 1 index only organizes evidence (role summaries, conflicts, quality).
    // Weighted probability is computed in Phase 2 / Phase 3 (see materialize_weighted_probability_base).
    let required_critical_roles = roles
        .iter()
        .filter(|role| config.workflow.critical_roles.contains(*role))
        .cloned()
        .collect::<Vec<_>>();
    let per_ticker = tickers
        .iter()
        .map(|ticker| {
            let role_summaries = roles
                .iter()
                .map(|role| {
                    let payload = reports
                        .get(role)
                        .and_then(|artifact| artifact_for_ticker(artifact, ticker));
                    let key_evidence = payload
                        .and_then(|value| value.get("key_evidence").or_else(|| value.get("evidence")))
                        .cloned()
                        .unwrap_or_else(|| json!([]));
                    json!({
                        "role": role,
                        "status": if missing_sources.contains(role) { "missing" } else { "ready" },
                        "stance": payload.and_then(|value| value.get("direction")).and_then(Value::as_str).unwrap_or("unobserved"),
                        "confidence": payload.and_then(|value| value.get("confidence")).cloned().unwrap_or(Value::Null),
                        "key_evidence": key_evidence,
                        "evidence_type_summary": payload
                            .map(summarize_evidence_types)
                            .unwrap_or_else(empty_evidence_type_summary),
                        "weaknesses": payload.and_then(|value| value.get("weaknesses")).cloned().unwrap_or_else(|| json!([])),
                        "source_node_ids": payload.and_then(|value| value.get("source_node_ids")).cloned().unwrap_or_else(|| json!([])),
                        "summary": payload.and_then(|value| value.get("report")).and_then(Value::as_str).unwrap_or("")
                    })
                })
                .collect::<Vec<_>>();
            let ticker_evidence_quality = evidence_quality_for_ticker(
                &reports,
                ticker,
                &required_critical_roles,
                &missing_sources,
            );
            let ticker_is_actionable = ticker_evidence_quality["status"] == "actionable";
            let conflicts = detect_all_conflicts(ticker, &role_summaries);
            let conflict_values = conflicts
                .iter()
                .map(|conflict| conflict.to_json())
                .collect::<Vec<_>>();
            let material_conflicts = conflict_values
                .iter()
                .filter(|conflict| conflict_requires_semantic_debate(conflict))
                .cloned()
                .collect::<Vec<_>>();
            let topic_candidates = if ticker_is_actionable {
                material_conflicts
                    .iter()
                    .enumerate()
                    .map(|(index, conflict)| topic_from_conflict(ticker, index, conflict))
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            (
                ticker.clone(),
                json!({
                    "id": "phase1.index",
                    "role": "phase1.index",
                    "artifact_type": "phase1_index",
                    "role_summaries": role_summaries,
                    "long_evidence": [],
                    "short_evidence": [],
                    "neutral_or_ambiguous_evidence": [],
                    "evidence_clusters": [],
                    "independent_signals": [],
                    "duplicate_signals": [],
                    "cross_analyst_conflicts": conflict_values.clone(),
                    "conflicts": conflict_values,
                    "missing_evidence": degraded_noncritical_roles,
                    "decision_hinges": [],
                    "evidence_quality": ticker_evidence_quality,
                    "material_conflicts": material_conflicts,
                    "topic_candidates": topic_candidates,
                    "state_summary": format!("Phase 1 state for {ticker}: {}.", ticker_evidence_quality["status"].as_str().unwrap_or("unknown"))
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    let evidence_quality = aggregate_evidence_quality(
        &per_ticker,
        &missing_critical_roles,
        state
            .get("degraded")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    );
    let status = match evidence_quality["status"].as_str() {
        Some("actionable") => "ready",
        Some("partial") => "partial",
        Some("blocked") => "blocked",
        _ => "insufficient",
    };
    let topic_candidates = per_ticker
        .values()
        .filter(|artifact| artifact["evidence_quality"]["status"] == "actionable")
        .filter_map(|artifact| artifact.get("topic_candidates").and_then(Value::as_array))
        .flatten()
        .cloned()
        .collect::<Vec<_>>();
    let cross_analyst_conflicts_summary = per_ticker
        .values()
        .filter_map(|ticker_artifact| ticker_artifact.get("cross_analyst_conflicts"))
        .filter_map(Value::as_array)
        .flatten()
        .cloned()
        .collect::<Vec<_>>();
    json!({
        "id": "phase1.index",
        "role": "phase1.index",
        "artifact_type": "phase1_index",
        "phase": "phase1",
        "status": status,
        "evidence_quality": evidence_quality,
        "workflow_pattern": "Workflow -> Stage/Sub-workflow -> Agent workers -> Phase1 index (organize only) -> state",
        "generated_from": {
            "worker_roles": roles,
            "critical_roles": config.workflow.critical_roles.iter().cloned().collect::<Vec<_>>(),
            "missing_critical_roles": missing_critical_roles,
            "degraded_noncritical_roles": degraded_noncritical_roles
        },
        "late_evidence": state.get("late_evidence").cloned().unwrap_or_else(|| json!([])),
        "per_ticker": per_ticker,
        "topic_candidates": topic_candidates,
        "cross_analyst_conflicts_summary": cross_analyst_conflicts_summary,
        "cross_ticker_notes": [],
        "index_checks": {
            "json_valid": true,
            "no_new_external_facts": true,
            "all_claims_source_backed": true,
            "weighting_deferred_to_phase2_and_3": true
        }
    })
}

/// Whether a report payload has usable directional fields for gating (not weighting).
fn role_has_usable_direction(report: &Value, payload: &Value) -> bool {
    if is_non_contributing(report, payload) {
        return false;
    }
    if payload.get("confidence").and_then(Value::as_f64).is_none() {
        return false;
    }
    matches!(
        payload.get("direction").and_then(Value::as_str),
        Some(
            "bullish"
                | "long"
                | "positive"
                | "bearish"
                | "short"
                | "negative"
                | "neutral"
                | "mixed"
        )
    )
}

fn evidence_quality_for_ticker(
    reports: &serde_json::Map<String, Value>,
    ticker: &str,
    required_critical_roles: &[String],
    missing_sources: &BTreeSet<String>,
) -> Value {
    let mut source_roles = BTreeSet::new();
    for (role, report) in reports {
        let payload = artifact_for_ticker(report, ticker).unwrap_or(report);
        if role_has_usable_direction(report, payload) {
            source_roles.insert(role.clone());
        }
    }
    let unavailable_critical_roles = required_critical_roles
        .iter()
        .filter(|role| missing_sources.contains(*role) || !source_roles.contains(*role))
        .cloned()
        .collect::<Vec<_>>();
    let actionable = !source_roles.is_empty() && unavailable_critical_roles.is_empty();
    json!({
        "status": if actionable { "actionable" } else { "insufficient" },
        "confidence_basis": if actionable { "evidence_available" } else { "data_insufficient" },
        "usable_source_roles": source_roles.into_iter().collect::<Vec<_>>(),
        "unusable_critical_roles": unavailable_critical_roles,
        "reason": if actionable {
            "All critical roles supplied usable directional evidence."
        } else {
            "One or more critical roles supplied no usable directional evidence."
        }
    })
}

/// Phase 2 / Phase 3 entry: compute analyst-weighted base probability (Rust).
///
/// Phase 1 index only organizes evidence; weighting is owned by debate (phase 2)
/// and Research Manager (phase 3) inputs, not by the phase-1 organize step.
pub(crate) fn materialize_weighted_probability_base(state: &mut Value) {
    let tickers = tickers_from_state(state);
    let reports = state
        .get("analyst_reports")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let base = weighted_probability_base(state, &tickers, &reports);
    state["weighted_probability_base"] = Value::Object(base);
}

fn aggregate_evidence_quality(
    per_ticker: &serde_json::Map<String, Value>,
    missing_critical_roles: &[String],
    workflow_degraded: bool,
) -> Value {
    let actionable_tickers = per_ticker
        .iter()
        .filter(|(_, artifact)| artifact["evidence_quality"]["status"] == "actionable")
        .map(|(ticker, _)| ticker.clone())
        .collect::<Vec<_>>();
    let insufficient_tickers = per_ticker
        .iter()
        .filter(|(_, artifact)| artifact["evidence_quality"]["status"] != "actionable")
        .map(|(ticker, _)| ticker.clone())
        .collect::<Vec<_>>();
    let status = if !missing_critical_roles.is_empty() {
        "blocked"
    } else if actionable_tickers.is_empty() {
        "insufficient"
    } else if !insufficient_tickers.is_empty() || workflow_degraded {
        "partial"
    } else {
        "actionable"
    };
    json!({
        "status": status,
        "confidence_basis": if status == "actionable" { "evidence_available" } else { "data_insufficient" },
        "actionable_tickers": actionable_tickers,
        "insufficient_tickers": insufficient_tickers,
        "missing_critical_roles": missing_critical_roles,
        "reason": match status {
            "actionable" => "All analyzed tickers have usable critical-role evidence.",
            "partial" => "Only a subset of analyzed tickers has usable critical-role evidence.",
            "blocked" => "One or more critical roles did not produce an artifact.",
            _ => "No analyzed ticker has usable critical-role evidence."
        }
    })
}

fn empty_evidence_type_summary() -> Value {
    json!({
        "fact_count": 0,
        "opinion_count": 0,
        "speculation_count": 0,
        "unclassified_count": 0,
        "speculation_ratio": 0.0
    })
}

fn summarize_evidence_types(payload: &Value) -> Value {
    let evidence = payload
        .get("key_evidence")
        .or_else(|| payload.get("evidence"))
        .and_then(Value::as_array);
    let Some(evidence) = evidence else {
        return empty_evidence_type_summary();
    };

    let mut fact_count = 0;
    let mut opinion_count = 0;
    let mut speculation_count = 0;
    let mut unclassified_count = 0;

    for item in evidence {
        match item {
            Value::String(_) => unclassified_count += 1,
            Value::Object(object) => match object.get("evidence_type").and_then(Value::as_str) {
                Some("fact") => fact_count += 1,
                Some("opinion") => opinion_count += 1,
                Some("speculation") => speculation_count += 1,
                _ => unclassified_count += 1,
            },
            _ => unclassified_count += 1,
        }
    }

    let total = evidence.len().max(1);
    json!({
        "fact_count": fact_count,
        "opinion_count": opinion_count,
        "speculation_count": speculation_count,
        "unclassified_count": unclassified_count,
        "speculation_ratio": speculation_count as f64 / total as f64
    })
}

pub(crate) fn build_topic_generation_artifact(state: &Value) -> Value {
    let tickers = tickers_from_state(state);
    let evidence_actionable = phase1_evidence_is_actionable(state);
    let topics = if evidence_actionable {
        phase1_topic_candidates(state)
    } else {
        Vec::new()
    };
    let debate_required = !topics.is_empty();
    let material_conflict_count = topics.len();
    let conflict_score =
        (material_conflict_count as f64 / tickers.len().max(1) as f64).clamp(0.0, 1.0);
    let skip_reason = if !evidence_actionable {
        Some("phase1_evidence_insufficient")
    } else if !debate_required {
        Some("no_material_cross_analyst_conflict")
    } else {
        None
    };
    let common_ground = derive_common_ground_from_phase1(state);
    json!({
        "id": "mediator.topic",
        "role": "mediator.topic",
        "artifact_type": "phase2_topic_generation_artifact",
        "phase": "phase2.topic_generation",
        "status": if debate_required { "ready" } else { "skipped" },
        "actionable": debate_required,
        "debate_required": debate_required,
        "evidence_actionable": evidence_actionable,
        "skip_reason": skip_reason,
        "conflict_score": conflict_score,
        "material_conflict_count": material_conflict_count,
        "evidence_quality": state.get("phase1_index")
            .and_then(|value| value.get("evidence_quality"))
            .cloned()
            .unwrap_or(Value::Null),
        "generated_from": {
            "source_artifact": "phase1_index",
            "tickers": tickers
        },
        "common_ground": common_ground,
        "topics": topics,
        "reducer_checks": {
            "json_valid": true,
            "from_phase1_index_only": true,
            "no_new_external_facts": true
        }
    })
}

/// Neutral facts/constraints shared across analysts — seed for bull/bear warm-up and topics.
fn derive_common_ground_from_phase1(state: &Value) -> Value {
    let phase1 = state.get("phase1_index").unwrap_or(&Value::Null);
    let mut agreed_facts = Vec::new();
    let mut shared_constraints = Vec::new();
    let mut evidence_refs = Vec::new();

    if let Some(eq) = phase1.get("evidence_quality") {
        if let Some(status) = eq.get("status").and_then(Value::as_str) {
            agreed_facts.push(json!(format!("phase1 evidence_quality.status={status}")));
        }
        if let Some(roles) = eq
            .get("usable_source_roles")
            .and_then(Value::as_array)
            .cloned()
        {
            for role in roles {
                if let Some(r) = role.as_str() {
                    evidence_refs.push(json!(format!("role:{r}")));
                }
            }
        }
    }
    if let Some(brief) = state.get("phase1_brief_md").and_then(Value::as_str) {
        if !brief.trim().is_empty() {
            agreed_facts.push(json!(truncate_str(brief, 240)));
        }
    }
    // Conflicts become shared constraints (what both sides must acknowledge).
    if let Some(conflicts) = phase1
        .get("cross_analyst_conflicts_summary")
        .and_then(Value::as_array)
    {
        for c in conflicts {
            let ctype = c
                .get("conflict_type")
                .or_else(|| c.get("type"))
                .and_then(Value::as_str)
                .unwrap_or("conflict");
            shared_constraints.push(json!(format!("acknowledge_{ctype}")));
        }
    }
    if agreed_facts.is_empty() {
        agreed_facts.push(json!(
            "Only phase00 / phase1 index summaries are admissible; no raw market re-fetch."
        ));
    }
    json!({
        "agreed_facts": agreed_facts,
        "shared_constraints": shared_constraints,
        "non_debated_assumptions": [
            "Do not invent external facts beyond phase00 index."
        ],
        "evidence_refs": evidence_refs
    })
}

fn truncate_str(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let clipped: String = s.chars().take(max.saturating_sub(1)).collect();
        format!("{clipped}…")
    }
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
    let debate_skipped = state
        .get("topic_generation_artifact")
        .and_then(|artifact| artifact.get("actionable"))
        .and_then(Value::as_bool)
        == Some(false)
        || !phase1_evidence_is_actionable(state);
    let has_controller_artifact = topic_states
        .values()
        .any(topic_state_has_controller_artifact);
    let topic_briefs = if debate_skipped || topic_states.is_empty() {
        Vec::new()
    } else {
        topic_states
            .values()
            .map(|topic_state| {
                let topic = topic_state
                    .get("topic")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                match latest_controller_artifact(topic_state) {
                    Some(latest) => {
                        debate_topic_brief_from_state(topic, latest, late_evidence, config)
                    }
                    None => not_converged_debate_topic_brief(topic, late_evidence, config),
                }
            })
            .collect::<Vec<_>>()
    };
    let all_topics_converged = !topic_briefs.is_empty()
        && topic_briefs
            .iter()
            .all(|brief| brief.get("status").and_then(Value::as_str) == Some("converged"));
    let generation_skip_reason = state
        .get("topic_generation_artifact")
        .and_then(|artifact| artifact.get("skip_reason"))
        .and_then(Value::as_str);
    let (status, convergence_status, reason) = if debate_skipped {
        match generation_skip_reason {
            Some("no_material_cross_analyst_conflict") => (
                "skipped_no_material_conflict",
                "skipped",
                "Rust found no material cross-analyst conflict; semantic debate added no value.",
            ),
            _ => (
                "skipped_no_actionable_evidence",
                "skipped",
                "Phase 1 evidence was insufficient for an actionable debate.",
            ),
        }
    } else if all_topics_converged {
        (
            "ready",
            "converged",
            "All topic controllers resolved an evidence-backed decision hinge.",
        )
    } else if has_controller_artifact {
        (
            "ready",
            "converged_or_pending_review",
            "Controller artifacts were recorded but not every topic produced an evidence-backed convergence proof.",
        )
    } else {
        (
            "not_converged",
            "not_converged",
            "No topic-controller artifact was recorded.",
        )
    };
    let per_ticker = tickers
        .iter()
        .map(|ticker| {
            let ticker_briefs = topic_briefs
                .iter()
                .filter(|brief| debate_brief_targets_ticker(brief, ticker))
                .collect::<Vec<_>>();
            let ticker_converged = !ticker_briefs.is_empty()
                && ticker_briefs.iter().all(|brief| {
                    brief.get("status").and_then(Value::as_str) == Some("converged")
                });
            let decision_hinges = ticker_briefs
                .iter()
                .flat_map(|brief| collect_decision_hinges(brief))
                .collect::<Vec<_>>();
            (
                ticker.clone(),
                json!({
                    "id": "reducer.debate_final",
                    "role": "reducer.debate_final",
                    "artifact_type": "phase2_5_debate_state_artifact",
                    "status": status,
                    "convergence_status": if ticker_converged { "converged" } else { convergence_status },
                    "turn_count": turns.len(),
                    "decision_hinges": decision_hinges,
                    "missing_evidence": if ticker_converged { json!([]) } else { json!([reason]) },
                    "manager_handoff": {
                        "directional_pressure": if status == "ready" { "mixed" } else { "unavailable" },
                        "confidence_modifier": if status == "ready" { "neutral" } else { "data_insufficient" },
                        "why": format!("{reason} Debate reducer inspected {} turns for {ticker}.", turns.len()),
                        "do_not_exceed": "Do not treat this as final probability or rating."
                    }
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    json!({
        "id": "reducer.debate_final",
        "role": "reducer.debate_final",
        "artifact_type": "phase2_5_debate_state_artifact",
        "phase": "phase2.5b",
        "status": status,
        "convergence_status": convergence_status,
        "convergence_reason": reason,
        "debate_skipped": debate_skipped,
        "skip_reason": generation_skip_reason,
        "workflow_pattern": "Workflow -> Stage/Sub-workflow -> Agent workers -> Reducer -> state artifact",
        "generated_from": {
            "worker_roles": executed_phase2_worker_roles(state),
            "planned_worker_roles": [
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

fn executed_phase2_worker_roles(state: &Value) -> Vec<String> {
    let mut roles = BTreeSet::new();
    if state
        .get("topic_generation_artifact")
        .and_then(|artifact| artifact.get("status"))
        .and_then(Value::as_str)
        .is_some_and(|status| !status.starts_with("skipped"))
    {
        roles.insert("mediator.topic".to_string());
    }
    for turn in state
        .get("debate_turns")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Some(role) = turn.get("role").and_then(Value::as_str) {
            roles.insert(role.to_string());
        }
    }
    for topic in state
        .get("topic_debate_states")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|topics| topics.values())
    {
        if topic
            .get("controller_artifacts")
            .and_then(Value::as_array)
            .is_some_and(|items| !items.is_empty())
        {
            roles.insert("mediator.topic_controller".to_string());
        }
    }
    roles.into_iter().collect()
}

fn topic_state_has_controller_artifact(topic_state: &Value) -> bool {
    latest_controller_artifact(topic_state).is_some()
}

fn debate_brief_targets_ticker(brief: &Value, ticker: &str) -> bool {
    brief
        .get("tickers")
        .and_then(Value::as_array)
        .is_some_and(|items| items.iter().any(|item| item.as_str() == Some(ticker)))
}

fn collect_decision_hinges(value: &Value) -> Vec<Value> {
    match value {
        Value::Array(items) => items.iter().flat_map(collect_decision_hinges).collect(),
        Value::Object(object) => {
            let mut hinges = Vec::new();
            if object
                .get("hinge")
                .or_else(|| object.get("decision_hinge"))
                .and_then(Value::as_str)
                .is_some_and(|value| !value.trim().is_empty())
                && object
                    .get("evidence_refs")
                    .and_then(Value::as_array)
                    .is_some_and(|refs| !refs.is_empty())
            {
                hinges.push(Value::Object(object.clone()));
            }
            hinges.extend(
                object
                    .get("decision_hinges")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default(),
            );
            hinges.extend(
                object
                    .iter()
                    .filter(|(key, _)| key.as_str() != "decision_hinges")
                    .flat_map(|(_, child)| collect_decision_hinges(child)),
            );
            hinges
        }
        _ => Vec::new(),
    }
}

fn controller_artifact_is_converged(artifact: &Value) -> bool {
    let stop_advised = artifact
        .get("soft_control")
        .and_then(|control| control.get("should_continue"))
        .and_then(Value::as_bool)
        == Some(false);
    stop_advised
        && collect_decision_hinges(artifact).iter().any(|hinge| {
            let has_hinge = hinge
                .get("hinge")
                .or_else(|| hinge.get("decision_hinge"))
                .and_then(Value::as_str)
                .is_some_and(|value| !value.trim().is_empty());
            let has_refs = hinge
                .get("evidence_refs")
                .and_then(Value::as_array)
                .is_some_and(|refs| !refs.is_empty());
            has_hinge && has_refs
        })
}

fn latest_controller_artifact(topic_state: &Value) -> Option<Value> {
    topic_state
        .get("controller_artifact")
        .filter(|artifact| artifact.is_object())
        .cloned()
        .or_else(|| {
            topic_state
                .get("controller_artifacts")
                .and_then(Value::as_array)
                .and_then(|items| items.last())
                .filter(|artifact| artifact.is_object())
                .cloned()
        })
}

fn not_converged_debate_topic_brief(
    topic: Value,
    late_evidence: bool,
    config: &RuntimeConfig,
) -> Value {
    let mut brief = debate_topic_brief(topic, 0, late_evidence, config);
    if let Some(object) = brief.as_object_mut() {
        object.insert(
            "status".to_string(),
            Value::String("not_converged".to_string()),
        );
        object.insert("needs_manager_attention".to_string(), Value::Bool(true));
        if let Some(compressed_state) = object
            .get_mut("compressed_state")
            .and_then(Value::as_object_mut)
        {
            compressed_state.insert(
                "missing_evidence".to_string(),
                json!(["No topic-controller artifact was recorded."]),
            );
            compressed_state.insert(
                "stop_reason".to_string(),
                Value::String("Topic debate did not reach controller review.".to_string()),
            );
        }
    }
    brief
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
        object.insert(
            "status".to_string(),
            Value::String(
                if controller_artifact_is_converged(&controller_artifact) {
                    "converged"
                } else {
                    "not_converged"
                }
                .to_string(),
            ),
        );
        if let Some(claims) = controller_artifact.get("claim_ledger").cloned() {
            object.insert("claim_ledger".to_string(), claims);
        }
        if let Some(duplicates) = controller_artifact.get("duplicate_claims").cloned() {
            object.insert("duplicate_claims".to_string(), duplicates);
        }
        if let Some(unverifiable) = controller_artifact.get("unverifiable_claims").cloned() {
            object.insert("unverifiable_claims".to_string(), unverifiable);
        }
        if let Some(steers) = controller_artifact.get("next_steers").cloned() {
            object.insert("next_steers".to_string(), steers);
        }
        object.insert("controller_artifact".to_string(), controller_artifact);
    }
    brief
}

fn topic_from_conflict(ticker: &str, index: usize, conflict: &Value) -> Value {
    let conflict_type = conflict
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("material_conflict");
    let description = conflict
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or("Resolve the material evidence conflict.");
    json!({
        "topic_id": format!("{ticker}-{conflict_type}-{}", index + 1),
        "topic": description,
        "tickers": [ticker],
        "long_evidence_refs": [],
        "short_evidence_refs": [],
        "why_debate": format!("Rust detected a material {conflict_type}."),
        "source_conflict": conflict
    })
}

fn conflict_requires_semantic_debate(conflict: &Value) -> bool {
    let conflict_type = conflict.get("type").and_then(Value::as_str);
    let severity = conflict.get("severity").and_then(Value::as_str);
    matches!(
        (conflict_type, severity),
        (
            Some("direction_conflict" | "confidence_divergence" | "evidence_contradiction"),
            Some("medium" | "high")
        )
    )
}

pub(crate) fn phase1_topic_candidates(state: &Value) -> Vec<Value> {
    state
        .get("phase1_index")
        .and_then(|artifact| artifact.get("topic_candidates"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

pub(crate) fn phase1_evidence_is_actionable(state: &Value) -> bool {
    state
        .get("phase1_index")
        .and_then(|artifact| artifact.get("evidence_quality"))
        .and_then(|quality| quality.get("status"))
        .and_then(Value::as_str)
        .map(|status| !matches!(status, "insufficient" | "blocked"))
        // Preserve existing workflow behavior for states produced before the
        // evidence-quality contract existed.
        .unwrap_or(true)
}

pub(crate) fn topics_from_generation_artifact(artifact: &Value) -> Vec<Value> {
    if artifact
        .get("actionable")
        .or_else(|| {
            artifact
                .get("reducer_output")
                .and_then(|output| output.get("actionable"))
        })
        .and_then(Value::as_bool)
        == Some(false)
    {
        return Vec::new();
    }

    // Prefer non-empty arrays; empty `topics: []` from a partial coerce must not
    // hide alternate shapes that still carry real debate hinges.
    for source in [
        artifact
            .get("reducer_output")
            .and_then(|output| output.get("topics")),
        artifact
            .get("reducer_output")
            .and_then(|output| output.get("topic_candidates")),
        artifact.get("topics"),
        artifact.get("topic_candidates"),
    ] {
        if let Some(items) = source.and_then(Value::as_array) {
            if !items.is_empty() {
                return items.clone();
            }
        }
    }

    let from_per_ticker = topics_from_per_ticker_shape(
        artifact
            .get("reducer_output")
            .and_then(|output| output.get("per_ticker"))
            .or_else(|| artifact.get("per_ticker")),
    );
    if !from_per_ticker.is_empty() {
        return from_per_ticker;
    }

    Vec::new()
}

/// Recover topics when the model nested them under `per_ticker` instead of `topics[]`.
fn topics_from_per_ticker_shape(per_ticker: Option<&Value>) -> Vec<Value> {
    let Some(object) = per_ticker.and_then(Value::as_object) else {
        return Vec::new();
    };
    let mut topics = Vec::new();
    let mut seen_ids = std::collections::BTreeSet::new();
    for (ticker, body) in object {
        if let Some(candidates) = body
            .get("topic_candidates")
            .or_else(|| body.get("topics"))
            .and_then(Value::as_array)
        {
            for candidate in candidates {
                push_topic_with_default_ticker(&mut topics, &mut seen_ids, candidate, ticker);
            }
            continue;
        }
        // Live shape: per_ticker.T is itself one topic object.
        if body
            .get("topic_id")
            .or_else(|| body.get("id"))
            .or_else(|| body.get("topic"))
            .and_then(Value::as_str)
            .map(str::trim)
            .is_some_and(|value| !value.is_empty())
        {
            push_topic_with_default_ticker(&mut topics, &mut seen_ids, body, ticker);
        }
    }
    topics
}

fn push_topic_with_default_ticker(
    topics: &mut Vec<Value>,
    seen_ids: &mut std::collections::BTreeSet<String>,
    candidate: &Value,
    ticker: &str,
) {
    let mut topic = candidate.clone();
    if let Some(topic_obj) = topic.as_object_mut() {
        if topic_obj.get("tickers").and_then(Value::as_array).is_none() {
            topic_obj.insert("tickers".to_string(), json!([ticker]));
        }
        if let Some(topic_id) = topic_obj
            .get("topic_id")
            .or_else(|| topic_obj.get("id"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if !seen_ids.insert(topic_id.to_string()) {
                return;
            }
        }
    }
    topics.push(topic);
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
            let mut unobserved_roles = Vec::new();
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
                    if payload.get("direction").and_then(Value::as_str) == Some("unobserved")
                        && !report
                            .get("degraded")
                            .and_then(Value::as_bool)
                            .unwrap_or(false)
                        && !matches!(
                            report.get("status").and_then(Value::as_str),
                            Some("degraded" | "missing" | "error")
                        )
                    {
                        unobserved_roles.push(Value::String(role.clone()));
                        continue;
                    }
                    skipped_roles.push(Value::String(role.clone()));
                    continue;
                }
                // Missing confidence/direction is a contract violation, not a
                // silent 0.5/neutral vote. Skip so the weighted base stays honest.
                let Some(confidence) = payload.get("confidence").and_then(Value::as_f64) else {
                    skipped_roles.push(Value::String(role.clone()));
                    continue;
                };
                let confidence = confidence.clamp(0.0, 1.0);
                let Some(direction_raw) = payload.get("direction").and_then(Value::as_str) else {
                    skipped_roles.push(Value::String(role.clone()));
                    continue;
                };
                let direction = match direction_raw {
                    "bullish" | "long" | "positive" => 1.0,
                    "bearish" | "short" | "negative" => -1.0,
                    "neutral" | "mixed" => 0.0,
                    "unobserved" => {
                        unobserved_roles.push(Value::String(role.clone()));
                        continue;
                    }
                    _ => {
                        skipped_roles.push(Value::String(role.clone()));
                        continue;
                    }
                };
                // Apply research_calibration speculation_discount in Rust so
                // prompt promises are enforced rather than left to the LLM.
                let confidence = apply_speculation_discount(payload, confidence);
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
                    "skipped_roles": skipped_roles,
                    "unobserved_roles": unobserved_roles
                }),
            )
        })
        .collect()
}

/// Apply `speculation_discount` from research_calibration.md:
/// opinion ×0.7, speculation ×0.3 (fact ×1.0, unclassified ×0.5), and if
/// speculation ratio > 50% apply an additional ×0.7 overall haircut.
fn apply_speculation_discount(payload: &Value, confidence: f64) -> f64 {
    let Some(items) = payload
        .get("key_evidence")
        .or_else(|| payload.get("evidence"))
        .and_then(Value::as_array)
    else {
        return confidence;
    };
    if items.is_empty() {
        return confidence;
    }

    let mut type_weight_sum = 0.0;
    let mut speculation_count = 0usize;
    for item in items {
        let evidence_type = match item {
            Value::Object(object) => object
                .get("evidence_type")
                .and_then(Value::as_str)
                .unwrap_or("unclassified"),
            Value::String(_) => "unclassified",
            _ => "unclassified",
        };
        let type_weight = match evidence_type {
            "fact" => 1.0,
            "opinion" => 0.7,
            "speculation" => {
                speculation_count += 1;
                0.3
            }
            _ => 0.5,
        };
        type_weight_sum += type_weight;
    }
    let avg_type_weight = type_weight_sum / items.len() as f64;
    let speculation_ratio = speculation_count as f64 / items.len() as f64;
    let overall = if speculation_ratio > 0.5 {
        avg_type_weight * 0.7
    } else {
        avg_type_weight
    };
    (confidence * overall).clamp(0.0, 1.0)
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

/// Verify runtime identity without rewriting the model's content. Callers must
/// retry or degrade an invalid artifact; relabeling it would make a foreign
/// role's evidence appear trustworthy to downstream reducers.
pub(crate) fn validate_artifact_identity(artifact: &Value, executing_role: &str) -> Result<()> {
    let artifact_id = artifact
        .get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| anyhow::anyhow!("artifact id is missing or empty"))?;
    let artifact_role = artifact
        .get("role")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|role| !role.is_empty())
        .ok_or_else(|| anyhow::anyhow!("artifact role is missing or empty"))?;
    if artifact_role != executing_role {
        bail!(
            "artifact role mismatch: expected {executing_role}, received {artifact_role} (id={artifact_id})"
        );
    }
    Ok(())
}

pub(crate) fn persist_artifact(
    conn: &mut rusqlite::Connection,
    state: &Value,
    phase: i64,
    role: &str,
    artifact: Value,
) -> Result<()> {
    validate_artifact_identity(&artifact, role)?;
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
    validate_artifact_identity(&artifact, role)?;
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

    fn test_runtime_config() -> RuntimeConfig {
        RuntimeConfig {
            llm_roles: std::collections::BTreeMap::new(),
            web_search: std::collections::BTreeMap::new(),
            truncation: orchestrator_llm::truncation::TruncationConfig::default(),
            judge: orchestrator_llm::llm_judge::JudgeConfig::default(),
            strict_sqlite: true,
            required_contexts: Vec::new(),
            prompts: crate::orchestration::config::PromptConfig {
                prompts: std::collections::BTreeMap::new(),
                manager_research: std::path::PathBuf::new(),

                trader: std::path::PathBuf::new(),
                risk_conservative: std::path::PathBuf::new(),
                portfolio_manager: std::path::PathBuf::new(),
            },
            workflow: crate::orchestration::config::WorkflowConfig {
                phase1_parallelism: 5,
                agent_timeout_sec: 300,
                reducer_timeout_sec: 300,
                critical_roles: ["analyst.technical", "analyst.news_macro"]
                    .into_iter()
                    .map(String::from)
                    .collect(),
                late_evidence_enabled: true,
                policy_mode: crate::orchestration::policy::WorkflowPolicyMode::Selective,
                policy_thresholds: Default::default(),
                force_portfolio_review: false,
            },
            allocation: crate::orchestration::config::AllocationConfig {
                investable_assets: vec!["QQQ".to_string(), "SOXX".to_string()],
                regime_signal: "VIX".to_string(),
                regime_thresholds: vec![15.0, 20.0, 30.0],
                regime_labels: vec![
                    "risk_on".to_string(),
                    "normal".to_string(),
                    "elevated".to_string(),
                    "defensive".to_string(),
                ],
                correlation_window_days: 60,
                max_single_position: 0.70,
                vol_indicator: "STD20".to_string(),
            },
            reflection: crate::orchestration::config::ReflectionConfig {
                enabled: true,
                reflection_version: "v1".to_string(),
                _promote_mode: "auto".to_string(),
                retrieval: orchestrator_core::RetrievalBudget::default(),
            },
            plugins: crate::orchestration::config::PluginConfig {
                enabled: false,
                components_dir: std::path::PathBuf::new(),
                roles_dir: std::path::PathBuf::new(),
                disabled_components: Vec::new(),
                extra_component_dirs: Vec::new(),
            },
            component_plugins: orchestrator_core::ComponentRegistry::default(),
            role_plugins: orchestrator_core::RolePluginRegistry::default(),
            agent_registry: orchestrator_core::AgentRegistry::builtin(),
        }
    }

    #[test]
    fn phase1_index_populates_cross_analyst_conflicts() {
        let config = test_runtime_config();
        let state = json!({
            "tickers": ["TQQQ"],
            "phase1_agents": ["analyst.technical", "analyst.news_macro"],
            "analyst_weights": {
                "analyst.technical": 40.0,
                "analyst.news_macro": 35.0
            },
            "analyst_reports": {
                "analyst.technical": {
                    "per_ticker": {
                        "TQQQ": {
                            "direction": "bullish",
                            "confidence": 0.7,
                            "key_evidence": ["breakout above 50MA"]
                        }
                    }
                },
                "analyst.news_macro": {
                    "per_ticker": {
                        "TQQQ": {
                            "direction": "bearish",
                            "confidence": 0.8,
                            "key_evidence": ["Fed hawkish surprise"]
                        }
                    }
                }
            }
        });

        let artifact = build_phase1_index(&state, &config);
        let ticker_artifact = &artifact["per_ticker"]["TQQQ"];
        let conflicts = ticker_artifact["cross_analyst_conflicts"]
            .as_array()
            .expect("cross_analyst_conflicts should be an array");

        assert!(conflicts
            .iter()
            .any(|conflict| conflict["type"] == "direction_conflict"));
        assert_eq!(
            ticker_artifact["conflicts"],
            ticker_artifact["cross_analyst_conflicts"]
        );
        assert_eq!(
            artifact["cross_analyst_conflicts_summary"],
            ticker_artifact["cross_analyst_conflicts"]
        );
    }

    #[test]
    fn phase1_index_summarizes_typed_evidence() {
        let config = test_runtime_config();
        let state = json!({
            "tickers": ["TQQQ"],
            "phase1_agents": ["analyst.technical"],
            "analyst_reports": {
                "analyst.technical": {
                    "per_ticker": {
                        "TQQQ": {
                            "direction": "bullish",
                            "confidence": 0.7,
                            "key_evidence": [
                                {"claim": "CPI 3.2%", "evidence_type": "fact", "source": "BLS"},
                                {"claim": "Fed may cut", "evidence_type": "opinion", "source": "Fed funds futures"},
                                {"claim": "Options whale rumored", "evidence_type": "speculation", "source": "unverified market report"}
                            ]
                        }
                    }
                }
            }
        });

        let artifact = build_phase1_index(&state, &config);
        let summary = &artifact["per_ticker"]["TQQQ"]["role_summaries"][0]["evidence_type_summary"];

        assert_eq!(summary["fact_count"], json!(1));
        assert_eq!(summary["opinion_count"], json!(1));
        assert_eq!(summary["speculation_count"], json!(1));
        assert_eq!(summary["unclassified_count"], json!(0));
        assert!((summary["speculation_ratio"].as_f64().unwrap() - (1.0 / 3.0)).abs() < 1e-9);
    }

    #[test]
    fn phase1_index_summarizes_legacy_evidence_as_unclassified() {
        let config = test_runtime_config();
        let state = json!({
            "tickers": ["TQQQ"],
            "phase1_agents": ["analyst.technical"],
            "analyst_reports": {
                "analyst.technical": {
                    "per_ticker": {
                        "TQQQ": {
                            "direction": "bullish",
                            "confidence": 0.7,
                            "evidence": ["breakout above 50MA", "volume confirmation"]
                        }
                    }
                }
            }
        });

        let artifact = build_phase1_index(&state, &config);
        let role_summary = &artifact["per_ticker"]["TQQQ"]["role_summaries"][0];

        assert_eq!(
            role_summary["key_evidence"],
            json!(["breakout above 50MA", "volume confirmation"])
        );
        assert_eq!(
            role_summary["evidence_type_summary"]["unclassified_count"],
            json!(2)
        );
        assert_eq!(
            role_summary["evidence_type_summary"]["speculation_ratio"],
            json!(0.0)
        );
    }

    #[test]
    fn phase1_index_includes_empty_conflicts_when_analysts_agree() {
        let config = test_runtime_config();
        let state = json!({
            "tickers": ["TQQQ"],
            "phase1_agents": ["analyst.technical", "analyst.news_macro"],
            "analyst_reports": {
                "analyst.technical": {
                    "per_ticker": {
                        "TQQQ": {"direction": "bullish", "confidence": 0.7}
                    }
                },
                "analyst.news_macro": {
                    "per_ticker": {
                        "TQQQ": {"direction": "bullish", "confidence": 0.6}
                    }
                }
            }
        });

        let artifact = build_phase1_index(&state, &config);
        let ticker_artifact = &artifact["per_ticker"]["TQQQ"];

        assert_eq!(ticker_artifact["cross_analyst_conflicts"], json!([]));
        assert_eq!(ticker_artifact["conflicts"], json!([]));
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
    fn unobserved_direction_is_recorded_separately_from_skipped() {
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
        assert_eq!(qqq["skipped_roles"], json!([]));
        assert_eq!(qqq["unobserved_roles"], json!(["analyst.news_macro"]));
    }

    #[test]
    fn debate_generated_from_lists_only_executed_workers() {
        let config = test_runtime_config();
        let skipped = json!({
            "tickers": ["QQQ"],
            "topic_generation_artifact": {
                "status": "skipped_no_actionable_evidence",
                "actionable": false
            }
        });
        let skipped_artifact = build_debate_state_artifact(&skipped, &config);
        assert_eq!(
            skipped_artifact["generated_from"]["worker_roles"],
            json!([])
        );

        let executed = json!({
            "tickers": ["QQQ"],
            "topic_generation_artifact": {"status": "ready", "actionable": true},
            "debate_turns": [
                {"role": "researcher.bull.initial"},
                {"role": "researcher.bear.initial"}
            ]
        });
        let executed_artifact = build_debate_state_artifact(&executed, &config);
        assert_eq!(
            executed_artifact["generated_from"]["worker_roles"],
            json!([
                "mediator.topic",
                "researcher.bear.initial",
                "researcher.bull.initial"
            ])
        );
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

    #[test]
    fn skips_reports_missing_confidence_or_direction() {
        let state = json!({
            "analyst_weights": {
                "analyst.technical": 1.0,
                "analyst.news_macro": 1.0
            }
        });
        let tickers = vec!["QQQ".to_string()];
        let mut reports = serde_json::Map::new();
        reports.insert(
            "analyst.technical".to_string(),
            json!({"per_ticker": {"QQQ": {"direction": "bullish"}}}),
        );
        reports.insert(
            "analyst.news_macro".to_string(),
            json!({"per_ticker": {"QQQ": {"confidence": 0.8}}}),
        );
        let base = weighted_probability_base(&state, &tickers, &reports);
        let qqq = base.get("QQQ").unwrap();
        assert_eq!(qqq["confidence"], json!(0.0));
        assert_eq!(qqq["long_probability"], json!(0.5));
        let skipped = qqq["skipped_roles"].as_array().unwrap();
        assert_eq!(skipped.len(), 2);
        assert!(skipped.iter().any(|v| v == "analyst.technical"));
        assert!(skipped.iter().any(|v| v == "analyst.news_macro"));
    }

    #[test]
    fn applies_speculation_discount_to_confidence() {
        let state = json!({
            "analyst_weights": { "analyst.news_macro": 1.0 }
        });
        let tickers = vec!["QQQ".to_string()];
        let mut reports = serde_json::Map::new();
        reports.insert(
            "analyst.news_macro".to_string(),
            json!({
                "per_ticker": {
                    "QQQ": {
                        "direction": "bullish",
                        "confidence": 1.0,
                        "key_evidence": [
                            {"claim": "rumor A", "evidence_type": "speculation"},
                            {"claim": "rumor B", "evidence_type": "speculation"}
                        ]
                    }
                }
            }),
        );
        let base = weighted_probability_base(&state, &tickers, &reports);
        let confidence = base["QQQ"]["confidence"].as_f64().unwrap();
        // avg type weight = 0.3, speculation_ratio=1.0 => *0.7 => 0.21
        assert!((confidence - 0.21).abs() < 1e-9, "got {confidence}");
    }

    #[test]
    fn phase1_marks_unusable_critical_evidence_as_insufficient_not_neutral() {
        let config = test_runtime_config();
        let state = json!({
            "tickers": ["QQQ"],
            "phase1_agents": ["analyst.technical", "analyst.news_macro"],
            "analyst_weights": {
                "analyst.technical": 1.0,
                "analyst.news_macro": 1.0
            },
            "analyst_reports": {
                "analyst.technical": {
                    "per_ticker": {"QQQ": {"direction": "unobserved", "confidence": 0.0}}
                },
                "analyst.news_macro": {
                    "per_ticker": {"QQQ": {"direction": "unobserved", "confidence": 0.0}}
                }
            }
        });

        let artifact = build_phase1_index(&state, &config);

        assert_eq!(artifact["status"], "insufficient");
        assert_eq!(artifact["evidence_quality"]["status"], "insufficient");
        assert_eq!(
            artifact["per_ticker"]["QQQ"]["evidence_quality"]["confidence_basis"],
            "data_insufficient"
        );
        // Weighting is not part of phase1 index.
        assert!(artifact.get("weighted_probability_base").is_none());
    }

    #[test]
    fn materialize_weighted_probability_base_is_phase23_state_field() {
        let mut state = json!({
            "tickers": ["QQQ"],
            "analyst_weights": {
                "analyst.technical": 1.0,
                "analyst.news_macro": 1.0
            },
            "analyst_reports": {
                "analyst.technical": {
                    "per_ticker": {"QQQ": {"direction": "bullish", "confidence": 0.8}}
                },
                "analyst.news_macro": {
                    "per_ticker": {"QQQ": {"direction": "bullish", "confidence": 0.6}}
                }
            }
        });
        materialize_weighted_probability_base(&mut state);
        let long = state["weighted_probability_base"]["QQQ"]["long_probability"]
            .as_f64()
            .unwrap();
        assert!(
            long > 0.9,
            "both bullish should yield high long_prob, got {long}"
        );
        assert!(state
            .get("phase1_index")
            .and_then(|v| v.get("weighted_probability_base"))
            .is_none());
    }

    #[test]
    fn topic_generation_skips_debate_when_phase1_evidence_is_insufficient() {
        let state = json!({
            "tickers": ["QQQ"],
            "phase1_index": {
                "evidence_quality": {"status": "insufficient"},
                "per_ticker": {
                    "QQQ": {"evidence_quality": {"status": "insufficient"}}
                }
            }
        });

        let artifact = build_topic_generation_artifact(&state);

        assert_eq!(artifact["actionable"], false);
        assert_eq!(artifact["status"], "skipped");
        assert_eq!(artifact["skip_reason"], "phase1_evidence_insufficient");
        assert_eq!(artifact["topics"], json!([]));
        assert_eq!(
            topics_from_generation_artifact(&artifact),
            Vec::<Value>::new()
        );
    }

    #[test]
    fn topic_generation_skips_when_actionable_evidence_has_no_material_conflict() {
        let state = json!({
            "tickers": ["QQQ"],
            "phase1_index": {
                "evidence_quality": {"status": "actionable"},
                "topic_candidates": [],
                "per_ticker": {
                    "QQQ": {"evidence_quality": {"status": "actionable"}}
                }
            }
        });

        let artifact = build_topic_generation_artifact(&state);

        assert_eq!(artifact["evidence_actionable"], true);
        assert_eq!(artifact["debate_required"], false);
        assert_eq!(
            artifact["skip_reason"],
            "no_material_cross_analyst_conflict"
        );
        assert_eq!(artifact["conflict_score"], 0.0);
    }

    #[test]
    fn topics_from_generation_recovers_per_ticker_direct_topic_objects() {
        // Live shape that previously produced topic_count=0 and skipped all researchers:
        // coerce wrote topics:[], while real hinges lived under reducer_output.per_ticker.
        let artifact = json!({
            "actionable": true,
            "topics": [],
            "summary": "no actionable debate topics",
            "reducer_output": {
                "role": "mediator.topic",
                "artifact_type": "phase2_topic_generation_artifact",
                "topics": [],
                "per_ticker": {
                    "QQQ": {
                        "topic_id": "QQQ-valuation-risk-vs-ai-support",
                        "topic": "Will valuation risk overpower AI support?",
                        "why_debate": "tech weak vs AI demand"
                    },
                    "SOXX": {
                        "topic_id": "SOXX-ai-capex-return-vs-overinvestment",
                        "topic": "Is SOXX pricing AI capex returns correctly?"
                    },
                    "VIX": {
                        "topic_id": "VIX-risk-regime-persistence",
                        "topic": "Is VIX a persistent regime shift?"
                    }
                }
            }
        });

        let topics = topics_from_generation_artifact(&artifact);
        assert_eq!(topics.len(), 3, "expected recovered topics: {topics:?}");
        assert!(topics.iter().any(|topic| {
            topic.get("topic_id").and_then(Value::as_str)
                == Some("QQQ-valuation-risk-vs-ai-support")
                && topic
                    .get("tickers")
                    .and_then(Value::as_array)
                    .is_some_and(|tickers| tickers.iter().any(|value| value == "QQQ"))
        }));
    }

    #[test]
    fn debate_state_without_controller_is_not_converged() {
        let config = test_runtime_config();
        let state = json!({
            "tickers": ["QQQ"],
            "topic_generation_artifact": {
                "actionable": true,
                "topics": [{
                    "topic_id": "QQQ-trend",
                    "topic": "QQQ trend",
                    "tickers": ["QQQ"]
                }]
            },
            "topic_debate_states": {},
            "debate_turns": []
        });

        let artifact = build_debate_state_artifact(&state, &config);

        assert_eq!(artifact["status"], "not_converged");
        assert_eq!(artifact["convergence_status"], "not_converged");
        assert_eq!(artifact["per_ticker"]["QQQ"]["status"], "not_converged");
        assert_eq!(artifact["topic_briefs"], json!([]));
    }

    #[test]
    fn debate_state_marks_evidence_backed_controller_resolution_converged() {
        let config = test_runtime_config();
        let state = json!({
            "tickers": ["QQQ"],
            "phase1_index": {
                "evidence_quality": {"status": "actionable"},
                "per_ticker": {"QQQ": {"evidence_quality": {"status": "actionable"}}}
            },
            "topic_generation_artifact": {"actionable": true},
            "topic_debate_states": {
                "QQQ-trend": {
                    "topic": {"topic_id": "QQQ-trend", "topic": "QQQ trend", "tickers": ["QQQ"]},
                    "controller_artifact": {
                        "soft_control": {"should_continue": false, "stop_reason": "resolved"},
                        "decision_hinges": [{
                            "hinge": "price confirmation",
                            "evidence_refs": ["technical:QQQ:breakout"]
                        }]
                    }
                }
            },
            "debate_turns": []
        });

        let artifact = build_debate_state_artifact(&state, &config);

        assert_eq!(artifact["convergence_status"], "converged");
        assert_eq!(
            artifact["per_ticker"]["QQQ"]["convergence_status"],
            "converged"
        );
        assert_eq!(
            artifact["per_ticker"]["QQQ"]["decision_hinges"][0]["evidence_refs"][0],
            "technical:QQQ:breakout"
        );
    }

    #[test]
    fn debate_state_skips_when_topic_generation_is_not_actionable() {
        let config = test_runtime_config();
        let state = json!({
            "tickers": ["QQQ"],
            "topic_generation_artifact": {"actionable": false},
            "topic_debate_states": {},
            "debate_turns": []
        });

        let artifact = build_debate_state_artifact(&state, &config);

        assert_eq!(artifact["status"], "skipped_no_actionable_evidence");
        assert_eq!(artifact["convergence_status"], "skipped");
        assert_eq!(
            artifact["per_ticker"]["QQQ"]["manager_handoff"]["confidence_modifier"],
            "data_insufficient"
        );
        assert_eq!(artifact["generated_from"]["worker_roles"], json!([]));
        assert_eq!(
            artifact["generated_from"]["planned_worker_roles"]
                .as_array()
                .unwrap()
                .len(),
            6
        );
    }

    #[test]
    fn artifact_identity_validation_does_not_mutate_mismatched_payload() {
        let artifact = json!({
            "id": "analyst.news_macro",
            "role": "analyst.news_macro",
            "per_ticker": {"QQQ": {}}
        });
        let original = artifact.clone();

        let error = validate_artifact_identity(&artifact, "analyst.technical").unwrap_err();

        assert!(error.to_string().contains("role mismatch"));
        assert_eq!(artifact, original);
    }
}

// --- market truth validation (merged from market_truth.rs) ---

pub(crate) fn market_truth_violation_report(
    research_plan: &Value,
    downstream_name: &str,
    downstream: &Value,
) -> Value {
    let mut violations = Vec::new();
    for field in [
        "rating",
        "long_probability",
        "short_probability",
        "probability_rationale",
    ] {
        push_market_truth_conflict(&mut violations, field, field, research_plan, downstream);
    }
    for downstream_field in ["plan", "thesis", "investment_thesis", "market_thesis"] {
        push_market_truth_conflict(
            &mut violations,
            "plan",
            downstream_field,
            research_plan,
            downstream,
        );
    }

    json!({
        "status": if violations.is_empty() { "ok" } else { "violation" },
        "downstream_artifact": downstream_name,
        "violation_count": violations.len(),
        "violations": violations,
    })
}

fn push_market_truth_conflict(
    violations: &mut Vec<Value>,
    research_field: &str,
    downstream_field: &str,
    research_plan: &Value,
    downstream: &Value,
) {
    let Some(research_value) = research_plan.get(research_field) else {
        return;
    };
    let Some(downstream_value) = downstream.get(downstream_field) else {
        return;
    };
    if !same_market_value(research_value, downstream_value) {
        violations.push(json!({
            "field": downstream_field,
            "source_field": research_field,
            "phase3_value": research_value,
            "downstream_value": downstream_value,
        }));
    }
}

fn same_market_value(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::String(left), Value::String(right)) => left.trim() == right.trim(),
        _ => left == right,
    }
}

#[cfg(test)]
mod market_truth_tests {
    use super::*;

    fn research_plan() -> Value {
        json!({
            "rating": "Buy",
            "long_probability": 0.68,
            "short_probability": 0.32,
            "probability_rationale": "Bull evidence outweighs downside.",
            "plan": "Stay long while breadth confirms."
        })
    }

    #[test]
    fn reports_no_conflict_when_market_fields_match_or_are_absent() {
        let report = market_truth_violation_report(
            &research_plan(),
            "final_trade_decision",
            &json!({
                "rating": "Buy",
                "long_probability": 0.68,
                "notes": "Execution detail only."
            }),
        );

        assert_eq!(report["status"], "ok");
        assert_eq!(report["violation_count"], 0);
        assert_eq!(report["violations"], json!([]));
    }

    #[test]
    fn reports_rating_conflict() {
        let report = market_truth_violation_report(
            &research_plan(),
            "final_trade_decision",
            &json!({"rating": "Sell"}),
        );

        assert_eq!(report["status"], "violation");
        assert_eq!(report["violations"][0]["field"], "rating");
        assert_eq!(report["violations"][0]["phase3_value"], "Buy");
        assert_eq!(report["violations"][0]["downstream_value"], "Sell");
    }

    #[test]
    fn reports_probability_conflict() {
        let report = market_truth_violation_report(
            &research_plan(),
            "portfolio_allocation",
            &json!({"long_probability": 0.41}),
        );

        assert_eq!(report["status"], "violation");
        assert_eq!(report["violations"][0]["field"], "long_probability");
        assert_eq!(report["violations"][0]["phase3_value"], 0.68);
        assert_eq!(report["violations"][0]["downstream_value"], 0.41);
    }

    #[test]
    fn reports_thesis_like_conflict_against_phase3_plan() {
        let report = market_truth_violation_report(
            &research_plan(),
            "portfolio_allocation",
            &json!({"investment_thesis": "Flip short into failed breakout."}),
        );

        assert_eq!(report["status"], "violation");
        assert_eq!(report["violations"][0]["field"], "investment_thesis");
        assert_eq!(report["violations"][0]["source_field"], "plan");
    }
}
