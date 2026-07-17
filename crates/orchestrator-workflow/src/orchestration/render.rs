use anyhow::{bail, Context, Result};
use orchestrator_core::{
    analyst_artifact_schema, final_validation_schema, portfolio_allocation_schema,
    replace_placeholders, research_artifact_schema, risk_constraints_schema, trade_intent_schema,
};
use serde_json::{json, Value};
use std::path::PathBuf;

use super::plugin_loader::ComponentRegistry;
use super::state::{tickers_from_state, topic_state};

pub(crate) fn mode_prompt_path(base: &std::path::Path, state: &Value) -> PathBuf {
    if state.get("mode").and_then(Value::as_str) != Some("monitor") {
        return base.to_path_buf();
    }
    let Some(stem) = base.file_stem().and_then(|value| value.to_str()) else {
        return base.to_path_buf();
    };
    let candidate = base.with_file_name(format!("{stem}_monitor.md"));
    if candidate.exists() {
        candidate
    } else {
        base.to_path_buf()
    }
}

/// Load a shared prompt component from `prompts/common/<file_name>` relative to
/// the role prompt path. Missing components resolve to an empty string so a role
/// prompt that does not reference the placeholder is unaffected.
fn prompts_dir_from_prompt_path(prompt_path: Option<&std::path::Path>) -> Option<PathBuf> {
    let path = prompt_path?;
    for ancestor in path.ancestors() {
        if ancestor.join("common").is_dir()
            || ancestor.join("components").is_dir()
            || ancestor.join("roles").is_dir()
        {
            return Some(ancestor.to_path_buf());
        }
    }
    path.parent()?.parent().map(PathBuf::from)
}

fn common_component(prompt_path: Option<&std::path::Path>, file_name: &str) -> Result<String> {
    let Some(prompts_dir) = prompts_dir_from_prompt_path(prompt_path) else {
        return Ok(String::new());
    };
    let common_path = prompts_dir.join("common").join(file_name);
    if common_path.exists() {
        std::fs::read_to_string(&common_path)
            .with_context(|| format!("failed to read prompt template {}", common_path.display()))
    } else {
        Ok(String::new())
    }
}

fn compact_evidence(value: &Value) -> Value {
    const FIELDS: &[&str] = &[
        "claim",
        "evidence_type",
        "source",
        "timestamp",
        "source_tier",
        "first_source",
        "is_derivative_repost",
        "evidence_age",
        "source_confidence",
    ];
    let Some(object) = value.as_object() else {
        return Value::Null;
    };
    Value::Object(
        FIELDS
            .iter()
            .filter_map(|field| {
                object
                    .get(*field)
                    .map(|value| ((*field).to_string(), value.clone()))
            })
            .collect(),
    )
}

fn compact_role_summary(value: &Value) -> Value {
    let evidence = value
        .get("key_evidence")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .take(3)
        .map(compact_evidence)
        .collect::<Vec<_>>();
    json!({
        "role": value.get("role").cloned().unwrap_or(Value::Null),
        "status": value.get("status").cloned().unwrap_or(Value::Null),
        "stance": value.get("stance").cloned().unwrap_or(Value::Null),
        "confidence": value.get("confidence").cloned().unwrap_or(Value::Null),
        "key_evidence": evidence,
        "evidence_type_summary": value.get("evidence_type_summary").cloned().unwrap_or(Value::Null),
        "weaknesses": value.get("weaknesses").cloned().unwrap_or_else(|| json!([])),
        "source_node_ids": value.get("source_node_ids").cloned().unwrap_or_else(|| json!([]))
    })
}

fn compact_phase1_ticker(value: &Value) -> Value {
    let role_summaries = value
        .get("role_summaries")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(compact_role_summary)
        .collect::<Vec<_>>();
    // No weighted_probability_base here — weighting is phase 2/3, not phase1 index.
    json!({
        "evidence_quality": value.get("evidence_quality").cloned().unwrap_or(Value::Null),
        "role_summaries": role_summaries,
        "cross_analyst_conflicts": value.get("cross_analyst_conflicts").cloned().unwrap_or_else(|| json!([])),
        "decision_hinges": value.get("decision_hinges").cloned().unwrap_or_else(|| json!([])),
        "missing_evidence": value.get("missing_evidence").cloned().unwrap_or_else(|| json!([])),
        "independent_signals": value.get("independent_signals").cloned().unwrap_or_else(|| json!([])),
        "duplicate_signals": value.get("duplicate_signals").cloned().unwrap_or_else(|| json!([])),
        "state_summary": value.get("state_summary").cloned().unwrap_or(Value::Null)
    })
}

fn compact_phase1_per_ticker(phase1: &Value) -> Value {
    Value::Object(
        phase1
            .get("per_ticker")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
            .map(|(ticker, value)| (ticker.clone(), compact_phase1_ticker(value)))
            .collect(),
    )
}

/// Phase 1 materialised index fork for Phase 2/3 prompts.
fn phase1_index_fork(state: &Value) -> Value {
    let from_index = state
        .get("phase00_tables")
        .and_then(|tables| tables.get("1"))
        .filter(|value| !value.is_null());
    let phase1 = state.get("phase1_index").cloned().unwrap_or(Value::Null);
    let source = if phase1.get("status").is_some() {
        "phase1_index"
    } else if from_index.is_some() {
        "phase00_tables.1"
    } else {
        "missing"
    };
    json!({
        "source": source,
        "artifact_type": phase1.get("artifact_type").cloned().unwrap_or(Value::Null),
        "status": phase1.get("status").cloned().unwrap_or(Value::Null),
        "evidence_quality": phase1.get("evidence_quality").cloned().unwrap_or(Value::Null),
        "weighted_probability_base": state
            .get("weighted_probability_base")
            .cloned()
            .unwrap_or(Value::Null),
        "topic_candidates": phase1.get("topic_candidates").cloned().unwrap_or_else(|| json!([])),
        "cross_analyst_conflicts_summary": phase1
            .get("cross_analyst_conflicts_summary")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "per_ticker": compact_phase1_per_ticker(&phase1),
        "brief_md": state.get("phase1_brief_md").cloned().unwrap_or(Value::Null),
        "index_checks": phase1
            .get("index_checks")
            .or_else(|| phase1.get("reducer_checks"))
            .cloned()
            .unwrap_or_else(|| json!({})),
        "phase00_table": from_index.cloned().unwrap_or(Value::Null),
        "note": "Phase 1 index organizes evidence only. weighted_probability_base is filled at phase 2/3."
    })
}

/// Prior phase compressor summaries for any downstream role.
fn prior_phase_summaries(state: &Value, current_phase: i64) -> Value {
    let compress = state.get("phase_compress").cloned().unwrap_or(Value::Null);
    let phase1 = phase1_index_fork(state);
    let mut phases = Vec::new();
    if phase1.get("status").is_some() || phase1.get("brief_md").is_some() {
        phases.push(json!({
            "source_phase": 1,
            "recency_weight": 1.0 + 0.15 * 1.0,
            "payload": phase1
        }));
    }
    if current_phase > 2 {
        if let Some(debate) = state.get("debate_state_artifact") {
            phases.push(json!({
                "source_phase": 2,
                "recency_weight": 1.0 + 0.15 * 2.0,
                "payload": {
                    "status": debate.get("status"),
                    "convergence_status": debate.get("convergence_status"),
                    "topic_briefs": debate.get("topic_briefs").cloned().unwrap_or_else(|| json!([])),
                    "brief_md": state.get("debate_brief_md"),
                }
            }));
        }
    }
    json!({
        "current_phase": current_phase,
        "attention_rule": "Prefer higher recency_weight (more recent source_phase). Expand details via read_run_context kinds phase_summaries / phase_summary_details / attention_expand.",
        "phases": phases,
        "phase_compress_status": compress,
    })
}

fn phase3_context(state: &Value) -> Value {
    let phase1 = state.get("phase1_index").cloned().unwrap_or(Value::Null);
    let debate = state
        .get("debate_state_artifact")
        .cloned()
        .unwrap_or(Value::Null);
    let phase00_tables = state
        .get("phase00_tables")
        .cloned()
        .unwrap_or_else(|| json!({}));

    json!({
        "phase1": {
            "status": phase1.get("status").cloned().unwrap_or(Value::Null),
            "evidence_quality": phase1.get("evidence_quality").cloned().unwrap_or(Value::Null),
            "per_ticker": compact_phase1_per_ticker(&phase1),
            "index_checks": phase1
                .get("index_checks")
                .or_else(|| phase1.get("reducer_checks"))
                .cloned()
                .unwrap_or_else(|| json!({}))
        },
        "weighted_probability_base": state
            .get("weighted_probability_base")
            .cloned()
            .unwrap_or(Value::Null),
        "analyst_weights": state.get("analyst_weights").cloned().unwrap_or(Value::Null),
        "phase2_5": {
            "status": debate.get("status").cloned().unwrap_or(Value::Null),
            "convergence_status": debate.get("convergence_status").cloned().unwrap_or(Value::Null),
            "reason": debate.get("reason").cloned().unwrap_or(Value::Null),
            "topic_briefs": debate.get("topic_briefs").cloned().unwrap_or_else(|| json!([])),
            "per_ticker": debate.get("per_ticker").cloned().unwrap_or_else(|| json!({})),
            "reducer_checks": debate.get("reducer_checks").cloned().unwrap_or_else(|| json!({}))
        },
        "phase00_tables": phase00_tables,
        "prior_memory": state.get("prior_memory").cloned().unwrap_or(Value::Null),
        "track_record": state.get("track_record").cloned().unwrap_or(Value::Null),
        "agent_accuracy": state.get("agent_accuracy").cloned().unwrap_or(Value::Null)
    })
}

fn compact_object_fields(value: &Value, fields: &[&str]) -> Value {
    Value::Object(
        fields
            .iter()
            .filter_map(|field| {
                value
                    .get(*field)
                    .map(|item| ((*field).to_string(), item.clone()))
            })
            .collect(),
    )
}

fn compact_research_plan(state: &Value) -> Value {
    const FIELDS: &[&str] = &[
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
    ];
    let plan = state.get("research_plan").unwrap_or(&Value::Null);
    let mut compact = compact_object_fields(plan, FIELDS);
    compact["per_ticker"] = Value::Object(
        plan.get("per_ticker")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
            .map(|(ticker, payload)| (ticker.clone(), compact_object_fields(payload, FIELDS)))
            .collect(),
    );
    compact
}

fn compact_risk_history(state: &Value) -> Value {
    const FIELDS: &[&str] = &[
        "role",
        "stance",
        "argument",
        "recommended_adjustment",
        "unique_risk_contribution",
        "disagreement_with_prior",
        "no_new_information",
        "stop_type",
        "max_drawdown_pct",
        "position_cap_pct",
        "rebalance_trigger",
        "risk_off_trigger",
        "review_window",
        "cash_hedge_recommendation",
        "constraint_confidence",
    ];
    Value::Array(
        state
            .get("risk_debate_state")
            .and_then(|value| value.get("history"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|turn| compact_object_fields(turn.get("artifact").unwrap_or(turn), FIELDS))
            .collect(),
    )
}

fn compact_trader_plan(state: &Value) -> Value {
    compact_object_fields(
        state.get("trader_investment_plan").unwrap_or(&Value::Null),
        &[
            "action",
            "position_size",
            "entry_price",
            "stop_loss",
            "rationale",
            "status",
        ],
    )
}

fn risk_context(state: &Value) -> Value {
    json!({
        "research_plan": compact_research_plan(state),
        "trader_plan": compact_trader_plan(state),
        "phase1_evidence_quality": state
            .get("phase1_index")
            .and_then(|value| value.get("evidence_quality"))
            .cloned()
            .unwrap_or(Value::Null),
        "prior_risk_arguments": compact_risk_history(state)
    })
}

fn portfolio_context(state: &Value) -> Value {
    json!({
        "research_plan": compact_research_plan(state),
        "trader_plan": compact_trader_plan(state),
        "risk_history": compact_risk_history(state)
    })
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn render_prompt(
    state: &Value,
    role: &str,
    phase: i64,
    kind: &str,
    round: Option<i64>,
    topic_id: Option<&str>,
    prompt_path: Option<&std::path::Path>,
    component_registry: Option<&ComponentRegistry>,
) -> Result<String> {
    let discovered_registry = if component_registry.is_none() {
        prompt_path
            .and_then(|_| prompts_dir_from_prompt_path(prompt_path))
            .map(|prompts_dir| ComponentRegistry::discover(&prompts_dir))
            .transpose()?
    } else {
        None
    };
    render_prompt_with_plugins(
        state,
        role,
        phase,
        kind,
        round,
        topic_id,
        prompt_path,
        component_registry.or(discovered_registry.as_ref()),
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn render_prompt_with_plugins(
    state: &Value,
    role: &str,
    phase: i64,
    kind: &str,
    round: Option<i64>,
    topic_id: Option<&str>,
    prompt_path: Option<&std::path::Path>,
    component_registry: Option<&ComponentRegistry>,
) -> Result<String> {
    let tickers = tickers_from_state(state);
    let ticker = state
        .get("ticker")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .or_else(|| tickers.first().map(String::as_str))
        .unwrap_or("");
    let template = if let Some(path) = prompt_path {
        std::fs::read_to_string(path)
            .with_context(|| format!("failed to read prompt template {}", path.display()))?
    } else {
        "Return only artifact JSON for role {role}, kind {kind}, phase {phase}, and tickers {tickers}. Include per_ticker for every ticker.".to_string()
    };
    let current_topic_state = topic_id
        .and_then(|id| topic_state(state, id))
        .unwrap_or(Value::Null);
    let current_topic = current_topic_state
        .get("topic")
        .cloned()
        .unwrap_or(Value::Null);
    let current_controller = current_topic_state
        .get("controller_artifact")
        .cloned()
        .unwrap_or(Value::Null);
    let blocked_repeats = current_controller
        .get("blocked_repeats")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let next_agenda = current_controller
        .get("next_agenda")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let analyst_output_contract_template =
        common_component(prompt_path, "analyst_output_contract.md")?;
    let anti_injection_template = common_component(prompt_path, "anti_injection.md")?;
    let research_calibration_template = common_component(prompt_path, "research_calibration.md")?;
    let research_drivers_template = common_component(prompt_path, "research_drivers.md")?;
    let risk_analyst_template = common_component(prompt_path, "risk_analyst.md")?;
    let leveraged_etf_rules_template = common_component(prompt_path, "leveraged_etf_rules.md")?;
    let analyst_output_structure_template =
        common_component(prompt_path, "analyst_output_structure.md")?;
    // Render components with the schema in scope so a `{analyst_artifact_schema}`
    // placeholder inside the contract is resolved before the component text is
    // spliced into the role prompt (the outer pass runs once, in key order, and
    // would otherwise leave the nested placeholder untouched).
    let component_values = json!({
        "ticker": ticker,
        "tickers": tickers.join(","),
        "analyst_artifact_schema": analyst_artifact_schema(),
        "research_artifact_schema": research_artifact_schema(),
        "trade_intent_schema": trade_intent_schema(),
        "risk_constraints_schema": risk_constraints_schema(),
        "final_validation_schema": final_validation_schema(),
        "portfolio_allocation_schema": portfolio_allocation_schema(),
    });
    let analyst_output_contract =
        replace_placeholders(&analyst_output_contract_template, &component_values);
    let anti_injection = replace_placeholders(&anti_injection_template, &component_values);
    let research_calibration =
        replace_placeholders(&research_calibration_template, &component_values);
    let research_drivers = replace_placeholders(&research_drivers_template, &component_values);
    let leveraged_etf_rules =
        replace_placeholders(&leveraged_etf_rules_template, &component_values);
    let analyst_output_structure =
        replace_placeholders(&analyst_output_structure_template, &component_values);
    let stance_role_label = match role {
        "risk.aggressive" => "aggressive",
        "risk.neutral" => "neutral",
        "risk.conservative" => "conservative",
        _ => "",
    };
    let static_values = json!({
        "ticker": ticker,
        "tickers": tickers.join(","),
        "common_ticker_prompt": "",
        "analyst_output_contract": analyst_output_contract,
        "anti_injection": anti_injection,
        "research_calibration": research_calibration,
        "research_drivers": research_drivers,
        "leveraged_etf_rules": leveraged_etf_rules,
        "analyst_output_structure": analyst_output_structure,
        "analyst_artifact_schema": analyst_artifact_schema(),
        "research_artifact_schema": research_artifact_schema(),
        "trade_intent_schema": trade_intent_schema(),
        "risk_constraints_schema": risk_constraints_schema(),
        "final_validation_schema": final_validation_schema(),
        "portfolio_allocation_schema": portfolio_allocation_schema(),
        "role": role,
        "phase": phase,
        "kind": kind,
        "lang": state.get("lang").and_then(Value::as_str).unwrap_or("zh"),
        "side": "",
        "side_label": "",
        "opponent": "",
        "opponent_label": "",
        "stance": stance_role_label,
        "stance_label": stance_role_label,
        "stance_intro": "",
        "stance_rules": "",
        "stance_schema_extra": "",
        "researcher_body": "",
        "workflow_pattern": "Workflow -> Stage/Sub-workflow -> Agent workers -> Reducer -> state artifact"
    });
    let dynamic_values = json!({
        "run_id": state.get("run_id").and_then(Value::as_str).unwrap_or(""),
        "date": state.get("current_date").and_then(Value::as_str).unwrap_or(""),
        "window_days": state.get("window_days").cloned().unwrap_or(Value::Null),
        "round": round.unwrap_or_default(),
        "topic_id": topic_id.unwrap_or(""),
        "topic": serde_json::to_string_pretty(&current_topic)?,
        "blocked_repeats": serde_json::to_string_pretty(&blocked_repeats)?,
        "next_agenda": serde_json::to_string_pretty(&next_agenda)?,
        "analyst_reports": serde_json::to_string_pretty(&state.get("analyst_reports").cloned().unwrap_or(Value::Null))?,
        "research_plan": serde_json::to_string_pretty(&state.get("research_plan").cloned().unwrap_or(Value::Null))?,
        "trader_plan": serde_json::to_string_pretty(&state.get("trader_investment_plan").cloned().unwrap_or(Value::Null))?,
        "risk_history": serde_json::to_string_pretty(&state.get("risk_debate_state").and_then(|v| v.get("history")).cloned().unwrap_or_else(|| json!([])))?,
        "portfolio_decision": serde_json::to_string_pretty(&state.get("final_trade_decision").cloned().unwrap_or(Value::Null))?,
        "allocation_context": serde_json::to_string_pretty(&state.get("allocation_context").cloned().unwrap_or(Value::Null))?,
        "risk_context": serde_json::to_string_pretty(&risk_context(state))?,
        "portfolio_context": serde_json::to_string_pretty(&portfolio_context(state))?,
        "phase3_context": serde_json::to_string_pretty(&phase3_context(state))?,
        "phase1_index": serde_json::to_string_pretty(&phase1_index_fork(state))?,
        // Alias for newer prompts / phase00-era templates.
        "phase1_index": serde_json::to_string_pretty(&phase1_index_fork(state))?,
        "phase00_context": serde_json::to_string_pretty(&state.get("phase00_tables").cloned().unwrap_or(serde_json::json!({})))?,
        "common_ground": serde_json::to_string_pretty(
            &state
                .get("common_ground")
                .or_else(|| state.get("topic_generation_artifact").and_then(|a| a.get("common_ground")))
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        )?,
        "prior_phase_summaries": serde_json::to_string_pretty(&prior_phase_summaries(state, phase))?,
    });
    let mut values = static_values;
    if let (Some(static_map), Some(dynamic_map)) =
        (values.as_object_mut(), dynamic_values.as_object())
    {
        for (key, value) in dynamic_map {
            static_map.insert(key.clone(), value.clone());
        }
    }
    if let Some(registry) = component_registry {
        registry.render_for_role(role, &mut values)?;
    }
    if template.contains("{common_ticker_prompt}")
        && values
            .get("common_ticker_prompt")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
    {
        let path = prompt_path
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "<inline prompt>".to_string());
        bail!(
            "prompt {path} references {{common_ticker_prompt}} but no enabled ticker component injected it for role {role}"
        );
    }
    // Risk-tier prompts use a shared component. Render it against the value set,
    // then expose the result so role files can include it via one placeholder.
    let risk_analyst_body = replace_placeholders(&risk_analyst_template, &values);
    if let Some(map) = values.as_object_mut() {
        map.insert(
            "risk_analyst_body".to_string(),
            Value::String(risk_analyst_body),
        );
    }
    Ok(replace_placeholders(&template, &values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestration::config::resolve_versioned_prompt_path;
    use crate::orchestration::plugin_loader::ComponentRegistry;
    use serde_json::json;
    use tempfile::TempDir;

    fn write_ticker_component(prompts: &std::path::Path, body: &str) {
        std::fs::create_dir_all(prompts.join("components/ticker")).unwrap();
        std::fs::write(
            prompts.join("components/ticker/manifest.toml"),
            r#"name = "ticker"
injection_points = ["*"]
priority = 10
placeholder_key = "common_ticker_prompt"
required_variables = ["ticker", "tickers"]
"#,
        )
        .unwrap();
        std::fs::write(prompts.join("components/ticker/component.md"), body).unwrap();
    }

    #[test]
    fn render_prompt_injects_common_ticker_prompt() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("analysts")).unwrap();
        write_ticker_component(&prompts, "Ticker boundary: {ticker}; all: {tickers}");
        let prompt_path = prompts.join("analysts/test.md");
        std::fs::write(&prompt_path, "Role prompt\n{common_ticker_prompt}").unwrap();
        let state = json!({"ticker": "TQQQ", "tickers": ["TQQQ", "VIX"]});

        let prompt = render_prompt(
            &state,
            "analyst.test",
            1,
            "analysis",
            None,
            None,
            Some(&prompt_path),
            None,
        )
        .unwrap();

        assert!(prompt.contains("Ticker boundary: TQQQ; all: TQQQ,VIX"));
    }

    #[test]
    fn render_prompt_with_plugins_overrides_legacy_component() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("common")).unwrap();
        std::fs::create_dir_all(prompts.join("analysts")).unwrap();
        std::fs::write(prompts.join("common/ticker.md"), "LEGACY {ticker}").unwrap();
        write_ticker_component(&prompts, "PLUGIN {ticker}");
        let prompt_path = prompts.join("analysts/technical.md");
        std::fs::write(&prompt_path, "{common_ticker_prompt}").unwrap();
        let state = json!({"ticker": "QQQ", "tickers": ["QQQ"]});
        let registry = ComponentRegistry::discover(&prompts).unwrap();

        let prompt = render_prompt_with_plugins(
            &state,
            "analyst.technical",
            1,
            "analysis",
            None,
            None,
            Some(&prompt_path),
            Some(&registry),
        )
        .unwrap();

        assert!(prompt.contains("PLUGIN QQQ"));
        assert!(!prompt.contains("LEGACY QQQ"));
    }

    #[test]
    fn render_prompt_injects_shared_components() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("common")).unwrap();
        std::fs::create_dir_all(prompts.join("analysts")).unwrap();
        write_ticker_component(&prompts, "TICK {ticker}");
        std::fs::write(
            prompts.join("common/analyst_output_contract.md"),
            "CONTRACT for {ticker}",
        )
        .unwrap();
        std::fs::write(
            prompts.join("common/anti_injection.md"),
            "NO-INJECT boundary",
        )
        .unwrap();
        let prompt_path = prompts.join("analysts/technical.md");
        std::fs::write(
            &prompt_path,
            "{common_ticker_prompt}\n{anti_injection}\n{analyst_output_contract}",
        )
        .unwrap();
        let state = json!({"ticker": "QQQ", "tickers": ["QQQ", "SOXX"]});

        let prompt = render_prompt(
            &state,
            "analyst.technical",
            1,
            "analysis",
            None,
            None,
            Some(&prompt_path),
            None,
        )
        .unwrap();

        assert!(prompt.contains("TICK QQQ"));
        assert!(prompt.contains("CONTRACT for QQQ"));
        assert!(prompt.contains("NO-INJECT boundary"));
    }

    #[test]
    fn schema_placeholder_resolves_inside_component() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("common")).unwrap();
        std::fs::create_dir_all(prompts.join("analysts")).unwrap();
        std::fs::write(
            prompts.join("common/analyst_output_contract.md"),
            "schema:\n{analyst_artifact_schema}",
        )
        .unwrap();
        let prompt_path = prompts.join("analysts/technical.md");
        std::fs::write(&prompt_path, "{analyst_output_contract}").unwrap();
        let state = json!({"ticker": "QQQ", "tickers": ["QQQ"]});

        let prompt = render_prompt(
            &state,
            "analyst.technical",
            1,
            "analysis",
            None,
            None,
            Some(&prompt_path),
            None,
        )
        .unwrap();

        // The nested schema placeholder must be expanded, not left literal.
        assert!(!prompt.contains("{analyst_artifact_schema}"));
        assert!(prompt.contains("direction"));
        assert!(prompt.contains("confidence"));
    }

    #[test]
    fn missing_component_expands_to_empty() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("common")).unwrap();
        std::fs::create_dir_all(prompts.join("analysts")).unwrap();
        // No analyst_output_contract.md / anti_injection.md on disk.
        let prompt_path = prompts.join("analysts/technical.md");
        std::fs::write(
            &prompt_path,
            "start\n{anti_injection}\n{analyst_output_contract}\nend",
        )
        .unwrap();
        let state = json!({"ticker": "QQQ", "tickers": ["QQQ"]});

        let prompt = render_prompt(
            &state,
            "analyst.technical",
            1,
            "analysis",
            None,
            None,
            Some(&prompt_path),
            None,
        )
        .unwrap();

        assert!(prompt.contains("start"));
        assert!(prompt.contains("end"));
        assert!(!prompt.contains("{anti_injection}"));
        assert!(!prompt.contains("{analyst_output_contract}"));
    }

    #[test]
    fn bull_and_bear_initial_prompts_are_standalone() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("common")).unwrap();
        std::fs::create_dir_all(prompts.join("researchers")).unwrap();
        write_ticker_component(&prompts, "TICK {ticker}");
        std::fs::write(
            prompts.join("common/researcher_seed.md"),
            "SHOULD NOT LOAD {side}",
        )
        .unwrap();
        std::fs::write(
            prompts.join("researchers/bull_initial.md"),
            "看多研究员\n{common_ticker_prompt}\nrole=researcher.bull.initial artifact=bull_seed_packet field=known_bear_constraint",
        )
        .unwrap();
        std::fs::write(
            prompts.join("researchers/bear_initial.md"),
            "看空研究员\n{common_ticker_prompt}\nrole=researcher.bear.initial artifact=bear_seed_packet field=known_bull_constraint",
        )
        .unwrap();
        let state = json!({"ticker": "QQQ", "tickers": ["QQQ"]});

        let bull = render_prompt(
            &state,
            "researcher.bull.initial",
            2,
            "bull_seed",
            None,
            None,
            Some(&prompts.join("researchers/bull_initial.md")),
            None,
        )
        .unwrap();
        let bear = render_prompt(
            &state,
            "researcher.bear.initial",
            2,
            "bear_seed",
            None,
            None,
            Some(&prompts.join("researchers/bear_initial.md")),
            None,
        )
        .unwrap();

        assert!(bull.contains("看多研究员"));
        assert!(bull.contains("role=researcher.bull.initial"));
        assert!(bull.contains("artifact=bull_seed_packet"));
        assert!(bull.contains("field=known_bear_constraint"));
        assert!(bear.contains("看空研究员"));
        assert!(bear.contains("role=researcher.bear.initial"));
        assert!(bear.contains("artifact=bear_seed_packet"));
        assert!(bear.contains("field=known_bull_constraint"));
        for prompt in [&bull, &bear] {
            assert!(prompt.contains("TICK QQQ"));
            assert!(!prompt.contains("{researcher_body}"));
            assert!(!prompt.contains("{side}"));
            assert!(!prompt.contains("SHOULD NOT LOAD"));
        }
    }

    #[test]
    fn interaction_role_uses_standalone_interaction_prompt() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("common")).unwrap();
        std::fs::create_dir_all(prompts.join("researchers")).unwrap();
        write_ticker_component(&prompts, "TICK {ticker}");
        std::fs::write(prompts.join("common/researcher_seed.md"), "SEED {side}").unwrap();
        std::fs::write(
            prompts.join("common/researcher_interaction.md"),
            "SHOULD NOT LOAD {side}",
        )
        .unwrap();
        std::fs::write(
            prompts.join("researchers/bull_interaction.md"),
            "看多研究员\n{common_ticker_prompt}\nrole=researcher.bull.interaction artifact=bull_debate_packet target=看空 claim",
        )
        .unwrap();
        let state = json!({"ticker": "QQQ", "tickers": ["QQQ"]});

        let out = render_prompt(
            &state,
            "researcher.bull.interaction",
            2,
            "bull_packet",
            Some(2),
            None,
            Some(&prompts.join("researchers/bull_interaction.md")),
            None,
        )
        .unwrap();

        assert!(out.contains("看多研究员"));
        assert!(out.contains("TICK QQQ"));
        assert!(out.contains("role=researcher.bull.interaction"));
        assert!(out.contains("artifact=bull_debate_packet"));
        assert!(out.contains("看空 claim"));
        assert!(!out.contains("SEED"));
        assert!(!out.contains("SHOULD NOT LOAD"));
        assert!(!out.contains("{researcher_body}"));
        assert!(!out.contains("{side}"));
    }

    #[test]
    fn risk_tiers_share_body_with_markdown_stance_content() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("common")).unwrap();
        std::fs::create_dir_all(prompts.join("risk")).unwrap();
        std::fs::write(
            prompts.join("common/risk_analyst.md"),
            "shared body {trader_plan} {analyst_reports} {risk_history}",
        )
        .unwrap();
        std::fs::write(
            prompts.join("risk/aggressive.md"),
            "激进风险分析师\n{risk_analyst_body}\n\"key_risks_accepted\": [\"接受的风险\"]",
        )
        .unwrap();
        std::fs::write(
            prompts.join("risk/conservative.md"),
            "保守风险分析师\n{risk_analyst_body}\n\"key_risks\": [\"主要风险\"]",
        )
        .unwrap();
        let state = json!({"ticker": "QQQ", "tickers": ["QQQ"]});

        let aggressive = render_prompt(
            &state,
            "risk.aggressive",
            5,
            "risk_argument",
            None,
            None,
            Some(&prompts.join("risk/aggressive.md")),
            None,
        )
        .unwrap();
        let conservative = render_prompt(
            &state,
            "risk.conservative",
            5,
            "risk_argument",
            None,
            None,
            Some(&prompts.join("risk/conservative.md")),
            None,
        )
        .unwrap();

        assert!(aggressive.contains("激进风险分析师"));
        assert!(aggressive.contains("key_risks_accepted"));
        assert!(conservative.contains("保守风险分析师"));
        assert!(conservative.contains("\"key_risks\""));
        for prompt in [&aggressive, &conservative] {
            assert!(!prompt.contains("{risk_analyst_body}"));
            assert!(!prompt.contains("{stance_label}"));
            assert!(!prompt.contains("{stance_intro}"));
            assert!(!prompt.contains("{stance_rules}"));
            assert!(!prompt.contains("{stance_schema_extra}"));
        }
    }

    // ---- Golden regression over the real prompt pack -----------------------
    //
    // Renders every shipped role prompt against a representative mock state and
    // asserts that no known placeholder token survives and the output is
    // non-trivial. This catches (a) a role prompt referencing a placeholder the
    // renderer never sets, and (b) a shared component that fails to expand.
    // Literal `{` from JSON examples is fine — we only look for the specific
    // `{token}` names the renderer is responsible for.

    fn project_prompts_dir() -> std::path::PathBuf {
        // render.rs -> orchestration -> src -> orchestrator-workflow -> crates -> repo
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("prompts")
    }

    /// Placeholder tokens the renderer owns. If any survive a render, either the
    /// prompt referenced an unknown key or a component failed to expand.
    const KNOWN_PLACEHOLDERS: &[&str] = &[
        "{common_ticker_prompt}",
        "{analyst_output_contract}",
        "{anti_injection}",
        "{research_calibration}",
        "{research_drivers}",
        "{leveraged_etf_rules}",
        "{analyst_output_structure}",
        "{analyst_artifact_schema}",
        "{research_artifact_schema}",
        "{trade_intent_schema}",
        "{risk_constraints_schema}",
        "{final_validation_schema}",
        "{portfolio_allocation_schema}",
        "{risk_analyst_body}",
        "{date}",
        "{window_days}",
        "{topic_id}",
        "{topic}",
        "{round}",
        "{trader_plan}",
        "{research_plan}",
        "{analyst_reports}",
        "{risk_history}",
        "{risk_context}",
        "{portfolio_context}",
        "{allocation_context}",
        "{phase3_context}",
        "{phase1_index}",
        "{prior_phase_summaries}",
    ];

    fn golden_mock_state() -> Value {
        golden_mock_state_with_date("2026-07-03")
    }

    fn golden_mock_state_with_date(date: &str) -> Value {
        json!({
            "ticker": "QQQ",
            "tickers": ["QQQ", "SOXX", "VIX"],
            "current_date": date,
            "window_days": 5,
            "lang": "zh",
            "run_id": "golden-run",
            "analyst_reports": {"analyst.technical": {"per_ticker": {}}},
            "research_plan": {"rating": "Hold"},
            "trader_investment_plan": {"action": "Hold"},
            "risk_debate_state": {"history": []},
            "final_trade_decision": {"rating": "Hold"},
            "allocation_context": {"investable_assets": ["QQQ", "SOXX"]}
        })
    }

    #[test]
    fn phase3_context_preserves_canonical_inputs_without_turn_history() {
        let context = phase3_context(&json!({
            "phase1_index": {
                "status": "insufficient",
                "weighted_probability_base": {"QQQ": {"long_probability": 0.5}},
                "per_ticker": {"QQQ": {
                    "decision_hinges": ["price confirmation"],
                    "role_summaries": [{
                        "role": "analyst.technical",
                        "status": "ready",
                        "stance": "neutral",
                        "confidence": 0.5,
                        "summary": "full analyst report must not be forwarded",
                        "key_evidence": [
                            {"claim": "one", "evidence_type": "fact", "report": "drop"},
                            {"claim": "two", "evidence_type": "opinion"},
                            {"claim": "three", "evidence_type": "fact"},
                            {"claim": "four", "evidence_type": "fact"}
                        ]
                    }]
                }}
            },
            "debate_state_artifact": {
                "status": "skipped_no_actionable_evidence",
                "topic_briefs": [{"topic_id": "QQQ-gap"}],
                "debate_turns": [{"should_not": "be forwarded"}]
            },
            "prior_memory": {"items": []},
            "track_record": {"sample_size": 2},
            "agent_accuracy": {"analyst.technical": 0.7}
        }));

        assert_eq!(context["phase1"]["status"], "insufficient");
        assert_eq!(
            context["phase2_5"]["topic_briefs"][0]["topic_id"],
            "QQQ-gap"
        );
        assert!(context["phase2_5"].get("debate_turns").is_none());
        assert_eq!(context["track_record"]["sample_size"], 2);
        let role = &context["phase1"]["per_ticker"]["QQQ"]["role_summaries"][0];
        assert!(role.get("summary").is_none());
        assert_eq!(role["key_evidence"].as_array().unwrap().len(), 3);
        assert!(role["key_evidence"][0].get("report").is_none());
    }

    #[test]
    fn downstream_contexts_drop_full_analyst_and_risk_payloads() {
        let state = json!({
            "research_plan": {
                "rating": "Hold",
                "long_probability": 0.5,
                "short_probability": 0.5,
                "report": "DROP_FULL_RESEARCH_REPORT",
                "per_ticker": {"QQQ": {
                    "rating": "Hold",
                    "long_probability": 0.5,
                    "short_probability": 0.5,
                    "report": "DROP_TICKER_REPORT"
                }}
            },
            "trader_investment_plan": {"action": "Hold", "position_size": "0%"},
            "analyst_reports": {"analyst.technical": {"report": "DROP_ANALYST_REPORT"}},
            "risk_debate_state": {"history": [{"artifact": {
                "role": "risk.aggressive",
                "stance": "aggressive",
                "argument": "compact argument",
                "raw_context": "DROP_RISK_CONTEXT"
            }}]}
        });

        let risk = serde_json::to_string(&risk_context(&state)).unwrap();
        let portfolio = serde_json::to_string(&portfolio_context(&state)).unwrap();
        for context in [&risk, &portfolio] {
            assert!(!context.contains("DROP_FULL_RESEARCH_REPORT"));
            assert!(!context.contains("DROP_TICKER_REPORT"));
            assert!(!context.contains("DROP_ANALYST_REPORT"));
            assert!(!context.contains("DROP_RISK_CONTEXT"));
            assert!(context.contains("compact argument"));
        }
    }

    #[test]
    fn shipped_analyst_contract_delegates_shape_to_runtime_validation() {
        let contract = std::fs::read_to_string(
            project_prompts_dir().join("common/analyst_output_contract.md"),
        )
        .unwrap();

        assert!(contract.contains("运行时 schema"));
        assert!(!contract.contains("{analyst_artifact_schema}"));
        assert!(!contract.contains("顶层结构"));
        assert!(!contract.contains("```json"));
    }

    #[test]
    fn static_prefix_is_stable_across_dynamic_changes() {
        let prompts = project_prompts_dir();
        if !prompts.exists() {
            return;
        }
        let path = prompts.join("analysts/technical.md");
        let prompt_a = render_prompt(
            &golden_mock_state_with_date("2026-07-01"),
            "analyst.technical",
            1,
            "artifact",
            None,
            None,
            Some(&path),
            None,
        )
        .unwrap();
        let prompt_b = render_prompt(
            &golden_mock_state_with_date("2026-07-06"),
            "analyst.technical",
            1,
            "artifact",
            None,
            None,
            Some(&path),
            None,
        )
        .unwrap();
        let split_marker = "<!-- DYNAMIC SUFFIX";
        let prefix_a = prompt_a.split(split_marker).next().unwrap_or("");
        let prefix_b = prompt_b.split(split_marker).next().unwrap_or("");

        assert_eq!(
            prefix_a, prefix_b,
            "Static prefix must be identical across calls with different dates"
        );
    }

    #[test]
    fn versioned_prompt_path_resolves_correctly() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts/analysts");
        std::fs::create_dir_all(&prompts).unwrap();
        std::fs::write(prompts.join("technical.md"), "v1 content").unwrap();
        std::fs::write(prompts.join("technical_v2.md"), "v2 content").unwrap();

        let base = prompts.join("technical.md");
        let v1 = resolve_versioned_prompt_path(&base, Some("v1")).unwrap();
        let absent = resolve_versioned_prompt_path(&base, None).unwrap();
        let v2 = resolve_versioned_prompt_path(&base, Some("v2")).unwrap();
        let v3_fallback = resolve_versioned_prompt_path(&base, Some("v3")).unwrap();
        assert_eq!(v1, base);
        assert_eq!(absent, base);
        assert_eq!(v2, prompts.join("technical_v2.md"));
        assert_eq!(v3_fallback, base);
    }

    #[test]
    fn golden_all_role_prompts_render_without_unresolved_placeholders() {
        let prompts = project_prompts_dir();
        if !prompts.exists() {
            // Skip in environments without the prompt pack (e.g. packaged crate).
            return;
        }
        let state = golden_mock_state();
        let plugin_registry = ComponentRegistry::discover(&prompts).unwrap();
        let mut known_placeholders = KNOWN_PLACEHOLDERS
            .iter()
            .map(|value| (*value).to_string())
            .collect::<Vec<_>>();
        known_placeholders.extend(
            plugin_registry
                .placeholder_keys()
                .into_iter()
                .map(|key| format!("{{{key}}}")),
        );
        // (role, relative prompt path, kind)
        let cases: &[(&str, &str, &str)] = &[
            ("analyst.technical", "analysts/technical.md", "artifact"),
            ("analyst.news_macro", "analysts/news_macro.md", "artifact"),
            ("analyst.youtube", "analysts/youtube.md", "artifact"),
            ("analyst.reddit", "analysts/reddit.md", "artifact"),
            ("analyst.x", "analysts/x.md", "artifact"),
            (
                "researcher.bull.initial",
                "researchers/bull.md",
                "bull_seed",
            ),
            (
                "researcher.bear.initial",
                "researchers/bear.md",
                "bear_seed",
            ),
            (
                "researcher.bull.interaction",
                "researchers/bull.md",
                "bull_packet",
            ),
            (
                "researcher.bear.interaction",
                "researchers/bear.md",
                "bear_packet",
            ),
            (
                "researcher.bull.warmup",
                "researchers/bull.md",
                "warmup_ack",
            ),
            (
                "researcher.bear.warmup",
                "researchers/bear.md",
                "warmup_ack",
            ),
            (
                "mediator.topic",
                "mediators/topic_generation.md",
                "topic_generation",
            ),
            (
                "mediator.topic_controller",
                "mediators/topic_controller.md",
                "controller_packet",
            ),
            (
                "manager.research",
                "managers/research_manager.md",
                "artifact",
            ),
            ("trader", "traders/trader.md", "artifact"),
            ("risk.aggressive", "risk/aggressive.md", "risk_argument"),
            ("risk.neutral", "risk/neutral.md", "risk_argument"),
            ("risk.conservative", "risk/conservative.md", "risk_argument"),
            (
                "portfolio.manager",
                "managers/portfolio_manager.md",
                "artifact",
            ),
            ("allocation.manager", "allocation/manager.md", "artifact"),
        ];

        for (role, rel, kind) in cases {
            let path = prompts.join(rel);
            assert!(path.exists(), "missing prompt file {}", path.display());
            let prompt = render_prompt(
                &state,
                role,
                1,
                kind,
                Some(2),
                Some("QQQ-aggregate"),
                Some(&path),
                None,
            )
            .unwrap_or_else(|e| panic!("render failed for {role} ({rel}): {e}"));

            assert!(
                prompt.trim().len() > 40,
                "rendered prompt for {role} ({rel}) is suspiciously short"
            );
            for token in &known_placeholders {
                assert!(
                    !prompt.contains(token),
                    "unresolved placeholder {token} in {role} ({rel})"
                );
            }
        }
    }

    #[test]
    fn golden_analyst_prompts_carry_runtime_contract_and_boundaries() {
        let prompts = project_prompts_dir();
        if !prompts.exists() {
            return;
        }
        let state = golden_mock_state();
        for rel in [
            "analysts/technical.md",
            "analysts/news_macro.md",
            "analysts/reddit.md",
            "analysts/x.md",
            "analysts/youtube.md",
        ] {
            let path = prompts.join(rel);
            let role = format!(
                "analyst.{}",
                rel.trim_start_matches("analysts/").trim_end_matches(".md")
            );
            let prompt =
                render_prompt(&state, &role, 1, "artifact", None, None, Some(&path), None).unwrap();
            // The model still receives the key behavioral contract, while the
            // runtime validator remains the only source of structural truth.
            assert!(
                prompt.contains("direction"),
                "{rel} missing direction field"
            );
            assert!(
                prompt.contains("confidence"),
                "{rel} missing confidence field"
            );
            assert!(!prompt.contains("顶层结构"), "{rel} embeds a JSON shape");
            assert!(
                !prompt.contains("{analyst_artifact_schema}"),
                "{rel} contains a schema placeholder"
            );
            // Anti-injection boundary must be present for external-content roles.
            assert!(
                prompt.contains("外部内容边界") || prompt.contains("不是给你的指令"),
                "{rel} missing anti-injection boundary"
            );
        }
    }
}
