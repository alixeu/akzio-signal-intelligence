use anyhow::Result;
use serde_json::{json, Value};
use std::collections::BTreeSet;

use super::super::lifecycle::{research_plan_to_trade_intent, tickers_from_state};

/// Return lower-snake-case placeholders from the original template only.
///
/// This intentionally mirrors `render_template`: JSON braces and replacement
/// text are not placeholders. The renderer uses this before materialising
/// values so unused context and components are never loaded or serialised.
pub(crate) fn raw_template_placeholders(template: &str) -> BTreeSet<String> {
    let bytes = template.as_bytes();
    let mut placeholders = BTreeSet::new();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        if bytes[cursor] != b'{' {
            cursor += 1;
            continue;
        }
        let start = cursor + 1;
        let Some(end_offset) = bytes[start..].iter().position(|byte| *byte == b'}') else {
            break;
        };
        let end = start + end_offset;
        let name = &template[start..end];
        if !name.is_empty()
            && name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        {
            placeholders.insert(name.to_string());
        }
        cursor = end + 1;
    }
    placeholders
}

pub(super) fn add_template_placeholders(placeholders: &mut BTreeSet<String>, template: &str) {
    placeholders.extend(raw_template_placeholders(template));
}

fn insert_if_referenced(
    values: &mut serde_json::Map<String, Value>,
    placeholders: &BTreeSet<String>,
    key: &str,
    value: impl FnOnce() -> Result<Value>,
) -> Result<()> {
    if placeholders.contains(key) {
        values.insert(key.to_string(), value()?);
    }
    Ok(())
}

pub(super) fn phase3_context(state: &Value) -> Value {
    let input_tickers = tickers_from_state(state);
    let primary_ticker = input_tickers.first().cloned();
    let probability_base = state
        .get("weighted_probability_base")
        .filter(|value| !value.is_null())
        .cloned();

    json!({
        "status": if probability_base.is_some() { "available" } else { "not_loaded" },
        "item_count": probability_base.as_ref().and_then(Value::as_object).map(|items| items.len()).unwrap_or(0),
        "source": "rust_deterministic_probability_baseline",
        "input_tickers": input_tickers,
        "primary_ticker": primary_ticker,
        "weighted_probability_base": probability_base.unwrap_or(Value::Null),
        "analyst_weights": state.get("analyst_weights").cloned().unwrap_or(Value::Null)
    })
}

pub(super) fn retrieval_bootstrap(state: &Value, current_phase: i64) -> Value {
    let mut counts = serde_json::Map::new();
    let mut roles = BTreeSet::new();
    let mut total = 0usize;
    if let Some(completed) = state.get("phase_summary_live").and_then(Value::as_object) {
        for (phase, summaries) in completed {
            let Ok(phase) = phase.parse::<i64>() else {
                continue;
            };
            if phase >= current_phase {
                continue;
            }
            let count = summaries.as_array().map(Vec::len).unwrap_or_default();
            if let Some(summaries) = summaries.as_array() {
                roles.extend(
                    summaries
                        .iter()
                        .filter_map(|summary| summary.get("role").and_then(Value::as_str))
                        .map(ToOwned::to_owned),
                );
            }
            counts.insert(phase.to_string(), json!(count));
            total += count;
        }
    }
    let conflict_count = state
        .get("phase1_index")
        .and_then(|value| value.get("per_ticker"))
        .and_then(Value::as_object)
        .map(|items| {
            items
                .values()
                .filter_map(|item| {
                    item.get("cross_analyst_conflicts")
                        .and_then(Value::as_array)
                })
                .map(Vec::len)
                .sum::<usize>()
        })
        .unwrap_or(0);
    json!({
        "status": if total == 0 { "empty" } else { "available" },
        "item_count": total,
        "source_phase_counts": counts,
        "source_roles_present": roles,
        "phase1_completed": state.get("phase1_index").is_some_and(|value| !value.is_null()),
        "direction_or_evidence_conflict_count": conflict_count,
        "source": "file_store_index_metadata",
        "directly_injected": false,
        "semantic_content_included": false,
        "retrievable_via_tools": true
    })
}

pub(super) fn phase4_control_context(state: &Value) -> Value {
    let research_plan = state.get("research_plan").filter(|value| !value.is_null());
    let candidates = research_plan
        .and_then(|plan| plan.get("per_ticker"))
        .and_then(Value::as_object)
        .map(|decisions| {
            decisions
                .iter()
                .map(|(ticker, decision)| (ticker.clone(), research_plan_to_trade_intent(decision)))
                .collect::<serde_json::Map<_, _>>()
        })
        .map(Value::Object)
        .unwrap_or(Value::Null);
    json!({
        "status": if research_plan.is_some() { "available" } else { "not_loaded" },
        "item_count": candidates.as_object().map_or(0, serde_json::Map::len),
        "per_asset": candidates,
        "semantic_source": "rust_deterministic_rating_mapping",
        "phase3_semantics_included": false
    })
}

pub(super) fn phase5_control_context(state: &Value) -> Value {
    let scenario = state.get("overnight_gap_scenario");
    json!({
        "status": "available",
        "overnight_gap_scenario": {
            "status": if scenario.is_some() { "available" } else { "not_loaded" },
            "item_count": usize::from(scenario.is_some()),
            "data": scenario.cloned().unwrap_or(Value::Null),
            "source": if scenario.is_some() { "runtime" } else { "none" }
        },
        "hard_position_cap": state.pointer("/allocation_context/max_single_position")
            .cloned().unwrap_or(Value::Null),
        "prior_phase_semantics_included": false
    })
}

pub(super) fn phase6_control_context(state: &Value) -> Value {
    let weights = state
        .get("current_portfolio_weights")
        .cloned()
        .unwrap_or(Value::Null);
    let marginal_risk_controls = state
        .pointer("/risk_debate_state/reviewer_independence/per_asset")
        .and_then(Value::as_object)
        .map(|per_asset| {
            per_asset
                .iter()
                .map(|(ticker, entry)| {
                    (
                        ticker.clone(),
                        json!({
                            "eligible_source_refs": entry.get("eligible_source_refs").cloned().unwrap_or_else(|| json!([])),
                            "effective_position_cap_pct": entry.get("full_effective_position_cap_pct").cloned().unwrap_or(Value::Null),
                        }),
                    )
                })
                .collect::<serde_json::Map<_, _>>()
        })
        .map(Value::Object)
        .unwrap_or_else(|| json!({}));
    json!({
        "status": "available",
        "investable_assets": state.get("investable_assets").cloned().unwrap_or_else(|| json!([])),
        "current_weights": {
            "status": if weights.is_null() { "not_loaded" } else { "available" },
            "item_count": weights.as_object().map(|items| items.len()).unwrap_or(0),
            "data": weights,
            "source": "rust_runtime"
        },
        "hard_position_cap": state.pointer("/allocation_context/max_single_position")
            .cloned().unwrap_or(Value::Null),
        "phase5_marginal_control_context": {
            "status": if marginal_risk_controls.as_object().is_some_and(|items| !items.is_empty()) { "available" } else { "not_loaded" },
            "per_asset": marginal_risk_controls,
            "authority": "rust_phase5_leave_one_reviewer_out_v1"
        },
        "allowed_execution_status": ["execute", "wait", "downgrade"],
        "prior_phase_semantics_included": false
    })
}

fn context_manifest_item(name: &str, value: Value, source: &str, retrievable: bool) -> Value {
    let item_count = value
        .get("item_count")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| {
            value
                .as_object()
                .map(|items| items.len() as u64)
                .unwrap_or(u64::from(!value.is_null()))
        });
    json!({
        "name": name,
        "status": value.get("status").and_then(Value::as_str).unwrap_or(if value.is_null() { "not_loaded" } else { "available" }),
        "item_count": item_count,
        "character_count": serde_json::to_string(&value).map(|text| text.chars().count()).unwrap_or(0),
        "source": source,
        "directly_injected": true,
        "retrievable_via_tools": retrievable
    })
}

pub(crate) fn direct_context_manifest(state: &Value, phase: i64) -> Value {
    let mut contexts = Vec::new();
    if phase == 0 || (2..=6).contains(&phase) {
        contexts.push(context_manifest_item(
            "retrieval_bootstrap",
            retrieval_bootstrap(state, phase),
            "phase_summary_index_metadata",
            true,
        ));
    }
    match phase {
        3 => contexts.push(context_manifest_item(
            "phase3_context",
            phase3_context(state),
            "rust_deterministic_probability_baseline",
            false,
        )),
        4 => contexts.push(context_manifest_item(
            "phase4_control_context",
            phase4_control_context(state),
            "rust_deterministic_rating_mapping",
            false,
        )),
        5 => contexts.push(context_manifest_item(
            "phase5_control_context",
            phase5_control_context(state),
            "rust_runtime_constraints",
            false,
        )),
        6 => contexts.push(context_manifest_item(
            "phase6_control_context",
            phase6_control_context(state),
            "rust_runtime_constraints",
            false,
        )),
        _ => {}
    }
    json!({
        "status": if contexts.is_empty() { "not_applicable" } else { "available" },
        "context_count": contexts.len(),
        "contexts": contexts
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn project_values(
    state: &Value,
    role: &str,
    phase: i64,
    kind: &str,
    ticker: &str,
    tickers: &[String],
    investable_assets: &[String],
    context_only_assets: &[String],
    side: &str,
    side_label: &str,
    opponent: &str,
    opponent_label: &str,
    stance_role_label: &str,
    placeholders: &BTreeSet<String>,
) -> Result<Value> {
    let mut values = serde_json::Map::new();
    insert_if_referenced(&mut values, placeholders, "ticker", || {
        Ok(Value::String(ticker.to_string()))
    })?;
    insert_if_referenced(&mut values, placeholders, "tickers", || {
        Ok(Value::String(tickers.join(",")))
    })?;
    insert_if_referenced(&mut values, placeholders, "analysis_universe", || {
        Ok(Value::String(tickers.join(",")))
    })?;
    insert_if_referenced(&mut values, placeholders, "investable_assets", || {
        Ok(Value::String(investable_assets.join(",")))
    })?;
    insert_if_referenced(&mut values, placeholders, "context_only_assets", || {
        Ok(Value::String(context_only_assets.join(",")))
    })?;
    insert_if_referenced(&mut values, placeholders, "role", || {
        Ok(Value::String(role.to_string()))
    })?;
    insert_if_referenced(&mut values, placeholders, "phase", || Ok(json!(phase)))?;
    insert_if_referenced(&mut values, placeholders, "kind", || {
        Ok(Value::String(kind.to_string()))
    })?;
    insert_if_referenced(&mut values, placeholders, "lang", || {
        Ok(Value::String(
            state
                .get("lang")
                .and_then(Value::as_str)
                .unwrap_or("zh")
                .to_string(),
        ))
    })?;
    insert_if_referenced(&mut values, placeholders, "side", || {
        Ok(Value::String(side.to_string()))
    })?;
    insert_if_referenced(&mut values, placeholders, "side_label", || {
        Ok(Value::String(side_label.to_string()))
    })?;
    insert_if_referenced(&mut values, placeholders, "opponent", || {
        Ok(Value::String(opponent.to_string()))
    })?;
    insert_if_referenced(&mut values, placeholders, "opponent_label", || {
        Ok(Value::String(opponent_label.to_string()))
    })?;
    insert_if_referenced(&mut values, placeholders, "stance", || {
        Ok(Value::String(stance_role_label.to_string()))
    })?;
    insert_if_referenced(&mut values, placeholders, "stance_label", || {
        Ok(Value::String(stance_role_label.to_string()))
    })?;
    insert_if_referenced(&mut values, placeholders, "workflow_pattern", || {
        Ok(Value::String(
            "Workflow -> Stage/Sub-workflow -> Agent workers -> Reducer -> state artifact"
                .to_string(),
        ))
    })?;
    insert_if_referenced(&mut values, placeholders, "run_id", || {
        Ok(Value::String(
            state
                .get("run_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
        ))
    })?;
    insert_if_referenced(&mut values, placeholders, "date", || {
        Ok(Value::String(
            state
                .get("current_date")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
        ))
    })?;
    insert_if_referenced(&mut values, placeholders, "window_days", || {
        Ok(state.get("window_days").cloned().unwrap_or(Value::Null))
    })?;
    insert_if_referenced(&mut values, placeholders, "portfolio_decision", || {
        Ok(Value::String(serde_json::to_string_pretty(
            &state
                .get("final_trade_decision")
                .cloned()
                .unwrap_or(Value::Null),
        )?))
    })?;
    insert_if_referenced(&mut values, placeholders, "allocation_context", || {
        Ok(Value::String(serde_json::to_string_pretty(
            &state
                .get("allocation_context")
                .cloned()
                .unwrap_or(Value::Null),
        )?))
    })?;
    insert_if_referenced(&mut values, placeholders, "alpaca_mode", || {
        Ok(Value::String(
            if state.get("mock").and_then(Value::as_bool) == Some(true)
                || state.get("debug").and_then(Value::as_bool) == Some(true)
            {
                "disabled"
            } else {
                "live"
            }
            .to_string(),
        ))
    })?;
    insert_if_referenced(&mut values, placeholders, "phase3_context", || {
        Ok(Value::String(serde_json::to_string_pretty(
            &phase3_context(state),
        )?))
    })?;
    insert_if_referenced(&mut values, placeholders, "retrieval_bootstrap", || {
        Ok(Value::String(serde_json::to_string_pretty(
            &retrieval_bootstrap(state, phase),
        )?))
    })?;
    insert_if_referenced(&mut values, placeholders, "phase4_control_context", || {
        Ok(Value::String(serde_json::to_string_pretty(
            &phase4_control_context(state),
        )?))
    })?;
    insert_if_referenced(&mut values, placeholders, "phase5_control_context", || {
        Ok(Value::String(serde_json::to_string_pretty(
            &phase5_control_context(state),
        )?))
    })?;
    insert_if_referenced(&mut values, placeholders, "phase6_control_context", || {
        Ok(Value::String(serde_json::to_string_pretty(
            &phase6_control_context(state),
        )?))
    })?;
    insert_if_referenced(&mut values, placeholders, "common_ground", || {
        Ok(Value::String(serde_json::to_string_pretty(
            &state.get("common_ground").cloned().unwrap_or(Value::Null),
        )?))
    })?;
    insert_if_referenced(&mut values, placeholders, "reflection_task", || {
        Ok(Value::String(serde_json::to_string_pretty(
            &state.get("reflection_task").cloned().unwrap_or(Value::Null),
        )?))
    })?;
    insert_if_referenced(&mut values, placeholders, "summary_source_payload", || {
        Ok(Value::String(serde_json::to_string_pretty(
            &state
                .get("_summary_source_payload")
                .cloned()
                .unwrap_or(Value::Null),
        )?))
    })?;
    insert_if_referenced(
        &mut values,
        placeholders,
        "summary_validation_instruction",
        || {
            Ok(Value::String(
                state
                    .get("_phase1_summary_validation_retry")
                    .or_else(|| state.get("_phase5_summary_validation_retry"))
                    .or_else(|| state.get("_phase6_summary_validation_retry"))
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned(),
            ))
        },
    )?;
    insert_if_referenced(
        &mut values,
        placeholders,
        "research_validation_instruction",
        || {
            Ok(Value::String(
                state
                    .get("_phase3_research_validation_retry")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned(),
            ))
        },
    )?;
    insert_if_referenced(
        &mut values,
        placeholders,
        "topic_generation_validation_instruction",
        || {
            Ok(Value::String(
                state
                    .get("_phase2_topic_generation_validation_retry")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned(),
            ))
        },
    )?;

    Ok(Value::Object(values))
}
