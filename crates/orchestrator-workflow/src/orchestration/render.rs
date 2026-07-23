use anyhow::{bail, Context, Result};
use orchestrator_core::{
    analyst_artifact_schema, final_validation_schema, portfolio_allocation_schema,
    replace_placeholders, research_artifact_schema, risk_constraints_schema, trade_intent_schema,
};
use serde_json::{json, Value};
use std::path::PathBuf;

use super::lifecycle::{tickers_from_state, topic_state};
use orchestrator_core::ComponentRegistry;

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
        if ancestor.join("common").is_dir() || ancestor.join("roles").is_dir() {
            return Some(ancestor.to_path_buf());
        }
    }
    path.parent()?.parent().map(PathBuf::from)
}

fn prompt_component(prompt_path: Option<&std::path::Path>, relative_path: &str) -> Result<String> {
    let Some(prompts_dir) = prompts_dir_from_prompt_path(prompt_path) else {
        return Ok(String::new());
    };
    let component_path = prompts_dir.join(relative_path);
    if component_path.exists() {
        std::fs::read_to_string(&component_path).with_context(|| {
            format!(
                "failed to read prompt template {}",
                component_path.display()
            )
        })
    } else {
        Ok(String::new())
    }
}

fn phase3_context(state: &Value) -> Value {
    let input_tickers = tickers_from_state(state);
    let primary_ticker = input_tickers.first().cloned();

    json!({
        "input_tickers": input_tickers,
        "primary_ticker": primary_ticker,
        "weighted_probability_base": state
            .get("weighted_probability_base")
            .cloned()
            .unwrap_or(Value::Null),
        "analyst_weights": state.get("analyst_weights").cloned().unwrap_or(Value::Null),
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
        "missing_data_convergence",
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
        "prior_risk_arguments": compact_risk_history(state),
        "overnight_gap_scenario": state
            .get("overnight_gap_scenario")
            .cloned()
            .unwrap_or_else(|| json!({"pct": -0.03, "source": "runtime_default"}))
    })
}

fn contains_leveraged_etf(tickers: &[String]) -> bool {
    tickers.iter().any(|ticker| {
        matches!(
            ticker.trim().to_ascii_uppercase().as_str(),
            "TQQQ" | "SQQQ" | "SOXL" | "SOXS" | "UPRO" | "SPXU"
        )
    })
}

fn portfolio_context(state: &Value) -> Value {
    json!({
        "research_plan": compact_research_plan(state),
        "trader_plan": compact_trader_plan(state),
        "risk_history": compact_risk_history(state),
        "investable_assets": state.get("investable_assets").cloned().unwrap_or_else(|| json!([]))
    })
}

fn prior_phase_summaries(state: &Value, current_phase: i64) -> Value {
    let Some(raw) = state.get("phase_summary_memory") else {
        return json!({"query": "phase_summaries", "item_count": 0, "items": []});
    };
    let run_id = state.get("run_id").and_then(Value::as_str).unwrap_or("");
    orchestrator_sql::PhaseSummaryMemoryIndex::from_state_value(raw)
        .list_visible_summaries(run_id, current_phase, None)
        .unwrap_or_else(|_| json!({"query": "phase_summaries", "item_count": 0, "items": []}))
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
    let path = prompt_path.with_context(|| format!("missing prompt path for role {role}"))?;
    let template = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read prompt template {}", path.display()))?;
    let current_topic_state = topic_id
        .and_then(|id| topic_state(state, id))
        .unwrap_or(Value::Null);
    let current_topic = current_topic_state
        .get("topic")
        .cloned()
        .unwrap_or(Value::Null);
    let analyst_output_contract_template =
        prompt_component(prompt_path, "common/analyst_output_contract.md")?;
    let anti_injection_template = prompt_component(prompt_path, "common/anti_injection.md")?;
    let research_calibration_template =
        prompt_component(prompt_path, "common/research_calibration.md")?;
    let research_drivers_template = prompt_component(prompt_path, "common/research_drivers.md")?;
    let analysis_trace_contract_template =
        prompt_component(prompt_path, "common/analysis_trace.md")?;
    let risk_analyst_template = prompt_component(prompt_path, "phase5/risk_analyst.md")?;
    let leveraged_etf_rules_template =
        prompt_component(prompt_path, "common/leveraged_etf_rules.md")?;
    let experience_contract_template = prompt_component(prompt_path, "common/experience.md")?;
    // Render components with the schema in scope so a `{analyst_artifact_schema}`
    // placeholder inside the contract is resolved before the component text is
    // spliced into the role prompt (the outer pass runs once, in key order, and
    // would otherwise leave the nested placeholder untouched).
    let component_values = json!({
        "ticker": ticker,
        "tickers": tickers.join(","),
        "role": role,
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
    let analysis_trace_contract =
        replace_placeholders(&analysis_trace_contract_template, &component_values);
    let leveraged_etf_rules = if contains_leveraged_etf(&tickers) {
        replace_placeholders(&leveraged_etf_rules_template, &component_values)
    } else {
        String::new()
    };
    let experience_contract =
        replace_placeholders(&experience_contract_template, &component_values);
    let stance_role_label = role.strip_prefix("risk.").unwrap_or("");
    let static_values = json!({
        "ticker": ticker,
        "tickers": tickers.join(","),
        "common_ticker_prompt": "",
        "analyst_output_contract": analyst_output_contract,
        "anti_injection": anti_injection,
        "research_calibration": research_calibration,
        "research_drivers": research_drivers,
        "analysis_trace_contract": analysis_trace_contract,
        "leveraged_etf_rules": leveraged_etf_rules,
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
        "analyst_reports": serde_json::to_string_pretty(&state.get("analyst_reports").cloned().unwrap_or(Value::Null))?,
        "research_plan": serde_json::to_string_pretty(&state.get("research_plan").cloned().unwrap_or(Value::Null))?,
        "trader_plan": serde_json::to_string_pretty(&state.get("trader_investment_plan").cloned().unwrap_or(Value::Null))?,
        "risk_history": serde_json::to_string_pretty(&state.get("risk_debate_state").and_then(|v| v.get("history")).cloned().unwrap_or_else(|| json!([])))?,
        "portfolio_decision": serde_json::to_string_pretty(&state.get("final_trade_decision").cloned().unwrap_or(Value::Null))?,
        "allocation_context": serde_json::to_string_pretty(&state.get("allocation_context").cloned().unwrap_or(Value::Null))?,
        "risk_context": serde_json::to_string_pretty(&risk_context(state))?,
        "portfolio_context": serde_json::to_string_pretty(&portfolio_context(state))?,
        "ai4trade_mode": if state.get("mock").and_then(Value::as_bool) == Some(true)
            || state.get("debug").and_then(Value::as_bool) == Some(true)
        {
            "disabled"
        } else {
            "live"
        },
        "phase3_context": serde_json::to_string_pretty(&phase3_context(state))?,
        "phase1_index": serde_json::to_string_pretty(&state.get("phase1_index").cloned().unwrap_or(Value::Null))?,
        "prior_phase_summaries": serde_json::to_string_pretty(&prior_phase_summaries(state, phase))?,
        "common_ground": serde_json::to_string_pretty(&state.get("common_ground").cloned().unwrap_or(Value::Null))?,
        "reflection_task": serde_json::to_string_pretty(&state.get("reflection_task").cloned().unwrap_or(Value::Null))?,
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
    let rendered = replace_placeholders(&template, &values);
    if (1..=6).contains(&phase) && !experience_contract.trim().is_empty() {
        Ok(format!("{rendered}\n\n{experience_contract}"))
    } else {
        Ok(rendered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestration::config::resolve_versioned_prompt_path;
    use orchestrator_core::ComponentRegistry;
    use serde_json::json;
    use tempfile::TempDir;

    fn write_ticker_component(prompts: &std::path::Path, body: &str) {
        std::fs::create_dir_all(prompts.join("common/components/ticker")).unwrap();
        std::fs::write(
            prompts.join("common/components/ticker/manifest.toml"),
            r#"name = "ticker"
injection_points = ["*"]
priority = 10
placeholder_key = "common_ticker_prompt"
required_variables = ["ticker", "tickers"]
"#,
        )
        .unwrap();
        std::fs::write(prompts.join("common/components/ticker/component.md"), body).unwrap();
    }

    #[test]
    fn render_prompt_injects_common_ticker_prompt() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("phase1")).unwrap();
        write_ticker_component(&prompts, "Ticker boundary: {ticker}; all: {tickers}");
        let prompt_path = prompts.join("phase1/test.md");
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
        std::fs::create_dir_all(prompts.join("phase1")).unwrap();
        std::fs::write(prompts.join("common/ticker.md"), "LEGACY {ticker}").unwrap();
        write_ticker_component(&prompts, "PLUGIN {ticker}");
        let prompt_path = prompts.join("phase1/technical.md");
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
        std::fs::create_dir_all(prompts.join("phase1")).unwrap();
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
        let prompt_path = prompts.join("phase1/technical.md");
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
        std::fs::create_dir_all(prompts.join("phase1")).unwrap();
        std::fs::write(
            prompts.join("common/analyst_output_contract.md"),
            "schema:\n{analyst_artifact_schema}",
        )
        .unwrap();
        let prompt_path = prompts.join("phase1/technical.md");
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
        std::fs::create_dir_all(prompts.join("phase1")).unwrap();
        // No analyst_output_contract.md / anti_injection.md on disk.
        let prompt_path = prompts.join("phase1/technical.md");
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
        std::fs::create_dir_all(prompts.join("phase2")).unwrap();
        write_ticker_component(&prompts, "TICK {ticker}");
        std::fs::write(
            prompts.join("common/researcher_seed.md"),
            "SHOULD NOT LOAD {side}",
        )
        .unwrap();
        std::fs::write(
            prompts.join("phase2/bull_initial.md"),
            "看多研究员\n{common_ticker_prompt}\nrole=researcher.bull.initial artifact=bull_seed_packet field=known_bear_constraint",
        )
        .unwrap();
        std::fs::write(
            prompts.join("phase2/bear_initial.md"),
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
            Some(&prompts.join("phase2/bull_initial.md")),
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
            Some(&prompts.join("phase2/bear_initial.md")),
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
        std::fs::create_dir_all(prompts.join("phase2")).unwrap();
        write_ticker_component(&prompts, "TICK {ticker}");
        std::fs::write(prompts.join("common/researcher_seed.md"), "SEED {side}").unwrap();
        std::fs::write(
            prompts.join("common/researcher_interaction.md"),
            "SHOULD NOT LOAD {side}",
        )
        .unwrap();
        std::fs::write(
            prompts.join("phase2/bull_interaction.md"),
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
            Some(&prompts.join("phase2/bull_interaction.md")),
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
    fn integrated_risk_review_expands_shared_body() {
        let temp = TempDir::new().unwrap();
        let prompts = temp.path().join("prompts");
        std::fs::create_dir_all(prompts.join("common")).unwrap();
        std::fs::create_dir_all(prompts.join("phase5")).unwrap();
        std::fs::write(
            prompts.join("phase5/risk_analyst.md"),
            "shared body {trader_plan} {analyst_reports} {risk_history}",
        )
        .unwrap();
        std::fs::write(
            prompts.join("phase5/conservative.md"),
            "保守风险分析师\n{risk_analyst_body}\n\"key_risks\": [\"主要风险\"]",
        )
        .unwrap();
        let state = json!({"ticker": "QQQ", "tickers": ["QQQ"]});

        let conservative = render_prompt(
            &state,
            "risk.conservative",
            5,
            "risk_argument",
            None,
            None,
            Some(&prompts.join("phase5/conservative.md")),
            None,
        )
        .unwrap();

        assert!(conservative.contains("保守风险分析师"));
        assert!(conservative.contains("\"key_risks\""));
        for placeholder in [
            "{risk_analyst_body}",
            "{stance_label}",
            "{stance_intro}",
            "{stance_rules}",
            "{stance_schema_extra}",
        ] {
            assert!(!conservative.contains(placeholder));
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
        "{analysis_trace_contract}",
        "{leveraged_etf_rules}",
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
        "{reflection_task}",
        "{phase3_context}",
        "{phase1_index}",
        "{phase_summary_context}",
        "{prior_phase_summaries}",
        "{common_ground}",
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
    fn phase3_context_only_injects_deterministic_inputs() {
        let context = phase3_context(&json!({
            "tickers": ["QQQ"],
            "weighted_probability_base": {"QQQ": {"long_probability": 0.5}},
            "analyst_weights": {"analyst.technical": 0.5, "analyst.news_macro": 0.5},
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

        assert!(context.get("phase1").is_none());
        assert!(context.get("phase2_5").is_none());
        assert!(context.get("phase_summary_tables").is_none());
        assert_eq!(
            context["weighted_probability_base"]["QQQ"]["long_probability"],
            0.5
        );
        assert_eq!(context["track_record"]["sample_size"], 2);
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
        assert!(contract.contains("每个 `per_ticker.<TICKER>` 必须使用以下字段与类型"));
    }

    #[test]
    fn static_prefix_is_stable_across_dynamic_changes() {
        let prompts = project_prompts_dir();
        if !prompts.exists() {
            return;
        }
        let path = prompts.join("phase1/technical.md");
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
        let prompts = temp.path().join("prompts/phase1");
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
            ("analyst.technical", "phase1/technical.md", "artifact"),
            ("analyst.news_macro", "phase1/news_macro.md", "artifact"),
            (
                "mediator.topic",
                "phase2/topic_generator.md",
                "topic_generation",
            ),
            ("researcher.bull.initial", "phase2/bull.md", "bull_seed"),
            ("researcher.bear.initial", "phase2/bear.md", "bear_seed"),
            (
                "researcher.bull.interaction",
                "phase2/bull.md",
                "bull_packet",
            ),
            (
                "researcher.bear.interaction",
                "phase2/bear.md",
                "bear_packet",
            ),
            (
                "mediator.topic_controller",
                "phase2/topic_controller.md",
                "controller_packet",
            ),
            ("manager.research", "phase3/research_manager.md", "artifact"),
            ("trader", "phase4/trader.md", "artifact"),
            ("risk.aggressive", "phase5/aggressive.md", "risk_argument"),
            ("risk.neutral", "phase5/neutral.md", "risk_argument"),
            (
                "risk.conservative",
                "phase5/conservative.md",
                "risk_argument",
            ),
            (
                "portfolio.manager",
                "phase6/portfolio_manager.md",
                "artifact",
            ),
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
    fn phase2_plus_and_summary_prompts_include_public_analysis_trace_contract() {
        let prompts = project_prompts_dir();
        if !prompts.exists() {
            return;
        }
        let state = golden_mock_state();
        let cases: &[(&str, i64, &str, &str)] = &[
            (
                "compressor.phase_summary",
                0,
                "phase_summary",
                "phase_summary/phase_summary.md",
            ),
            (
                "mediator.topic",
                2,
                "topic_generation",
                "phase2/topic_generator.md",
            ),
            ("researcher.bull.initial", 2, "bull_seed", "phase2/bull.md"),
            (
                "researcher.bear.interaction",
                2,
                "bear_packet",
                "phase2/bear.md",
            ),
            (
                "mediator.topic_controller",
                2,
                "controller_packet",
                "phase2/topic_controller.md",
            ),
            (
                "manager.research",
                3,
                "artifact",
                "phase3/research_manager.md",
            ),
            ("trader", 4, "artifact", "phase4/trader.md"),
            (
                "risk.aggressive",
                5,
                "risk_argument",
                "phase5/aggressive.md",
            ),
            ("risk.neutral", 5, "risk_argument", "phase5/neutral.md"),
            (
                "risk.conservative",
                5,
                "risk_argument",
                "phase5/conservative.md",
            ),
            (
                "portfolio.manager",
                6,
                "artifact",
                "phase6/portfolio_manager.md",
            ),
        ];

        for (role, phase, kind, relative) in cases {
            let prompt = render_prompt(
                &state,
                role,
                *phase,
                kind,
                Some(1),
                Some("QQQ-aggregate"),
                Some(&prompts.join(relative)),
                None,
            )
            .unwrap_or_else(|error| panic!("render failed for {role} ({relative}): {error}"));
            assert!(
                prompt.contains("# 公共可审计分析轨迹"),
                "{relative} did not receive the shared analysis trace contract"
            );
            assert!(
                !prompt.contains("{analysis_trace_contract}"),
                "{relative} retained an unresolved analysis trace placeholder"
            );
        }

        let summary =
            std::fs::read_to_string(prompts.join("phase_summary/phase_summary.md")).unwrap();
        assert!(summary.contains("summary_json.analysis_process.trace_status"));
        assert!(summary.contains("\"analysis_process\""));
        assert!(summary.contains("source_phase >= 2"));
    }

    #[test]
    fn golden_analyst_prompts_carry_runtime_contract_and_boundaries() {
        let prompts = project_prompts_dir();
        if !prompts.exists() {
            return;
        }
        let state = golden_mock_state();
        for rel in ["phase1/technical.md", "phase1/news_macro.md"] {
            let path = prompts.join(rel);
            let role = format!(
                "analyst.{}",
                rel.trim_start_matches("phase1/").trim_end_matches(".md")
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

    #[test]
    fn research_manager_injects_calibration_and_semantic_drivers() {
        let prompts = project_prompts_dir();
        let rendered = render_prompt(
            &golden_mock_state(),
            "manager.research",
            3,
            "artifact",
            None,
            None,
            Some(&prompts.join("phase3/research_manager.md")),
            None,
        )
        .unwrap();

        assert!(rendered.contains("duplicate_evidence_discount"));
        assert!(rendered.contains("probability_drivers"));
        assert!(rendered.contains("direction`: `increase | decrease | neutral"));
        assert!(!rendered.contains("{research_calibration}"));
        assert!(!rendered.contains("{research_drivers}"));
        assert!(!rendered.contains("```json"));
    }

    #[test]
    fn leveraged_etf_rules_are_injected_only_for_leveraged_output_scope() {
        let prompts = project_prompts_dir();
        let path = prompts.join("phase1/technical.md");
        let ordinary = render_prompt(
            &golden_mock_state(),
            "analyst.technical",
            1,
            "artifact",
            None,
            None,
            Some(&path),
            None,
        )
        .unwrap();
        assert!(!ordinary.contains("## 杠杆 ETF 补充规则"));
        assert!(!ordinary.contains("SQQQ 与 QQQ 反向"));

        let mut leveraged = golden_mock_state();
        leveraged["ticker"] = json!("TQQQ");
        leveraged["tickers"] = json!(["TQQQ"]);
        let leveraged = render_prompt(
            &leveraged,
            "analyst.technical",
            1,
            "artifact",
            None,
            None,
            Some(&path),
            None,
        )
        .unwrap();
        assert!(leveraged.contains("## 杠杆 ETF 补充规则"));
        assert!(leveraged.contains("SQQQ 与 QQQ 反向"));
    }

    #[test]
    fn technical_prompt_lists_the_runtime_direction_and_source_tier_enums() {
        let prompts = project_prompts_dir();
        let prompt = render_prompt(
            &golden_mock_state(),
            "analyst.technical",
            1,
            "artifact",
            None,
            None,
            Some(&prompts.join("phase1/technical.md")),
            None,
        )
        .unwrap();

        assert!(prompt.contains("`bullish`、`bearish`、`neutral`、`mixed` 或 `unobserved`"));
        assert!(prompt.contains("不得输出组合标签（例如 `neutral_bullish`）"));
        assert!(prompt.contains("一律填写 `unknown`"));
        assert!(prompt.contains("绝不填写 `T1_reference`"));
        assert!(
            prompt.contains("`priced_in` 只能为文本 `already_priced`、`under_priced` 或 `unclear`")
        );
        assert!(prompt.contains("\"claim\": \"可核验事实或明确观点\""));
    }

    #[test]
    fn downstream_prompts_enforce_single_authority_chain() {
        let prompts = project_prompts_dir();
        let trader = render_prompt(
            &golden_mock_state(),
            "trader",
            4,
            "artifact",
            None,
            None,
            Some(&prompts.join("phase4/trader.md")),
            None,
        )
        .unwrap();
        assert!(trader.contains("research_plan` / Phase 3 是唯一市场结论"));

        for role in ["risk.aggressive", "risk.neutral", "risk.conservative"] {
            let stance = role.strip_prefix("risk.").unwrap();
            let risk = render_prompt(
                &golden_mock_state(),
                role,
                5,
                "risk_argument",
                Some(1),
                None,
                Some(&prompts.join(format!("phase5/{stance}.md"))),
                None,
            )
            .unwrap();
            assert!(risk.contains(stance));
            assert!(risk.contains("风险委员会"));
            assert!(risk.contains("overnight_gap_scenario"));
        }
    }

    #[test]
    fn topic_controller_uses_only_canonical_control_fields() {
        let content =
            std::fs::read_to_string(project_prompts_dir().join("phase2/topic_controller.md"))
                .unwrap();
        assert!(content.contains("blocked_claims"));
        assert!(content.contains("next_steers"));
        assert!(!content.contains("blocked_repeats"));
        assert!(!content.contains("next_agenda"));
    }

    #[test]
    fn active_prompts_do_not_reference_removed_social_sources() {
        let prompts = project_prompts_dir();
        for relative in [
            "phase1/technical.md",
            "phase1/news_macro.md",
            "common/analyst_output_contract.md",
            "phase3/research_manager.md",
            "phase2/bull.md",
            "phase2/bear.md",
        ] {
            let content = std::fs::read_to_string(prompts.join(relative)).unwrap();
            for removed in ["YouTube", "Reddit", "Twitter"] {
                assert!(!content.contains(removed), "{relative} mentions {removed}");
            }
        }
    }
}
