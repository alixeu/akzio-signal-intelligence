use anyhow::Result;
use orchestrator_core::run_slug;
use serde_json::{json, Value};
use std::path::Path;

pub(crate) fn run_id_for(tickers: &[String], date: &str, run_dir: &Path) -> String {
    format!(
        "{}-{}-{}",
        run_slug(tickers).to_ascii_lowercase(),
        date,
        run_dir
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("run")
    )
}

pub(crate) fn set_phase_status(state: &mut Value, phase: i64, status: &str) {
    if !state.get("phase_status").is_some_and(Value::is_object) {
        state["phase_status"] = json!({});
    }
    state["phase_status"][phase.to_string()] = Value::String(status.to_string());
}

pub(crate) fn tickers_from_state(state: &Value) -> Vec<String> {
    state
        .get("tickers")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn write_json(path: &Path, value: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_string_pretty(value)?)?;
    Ok(())
}

pub(crate) fn write_final_summary(run_dir: &Path, state: &Value) -> Result<()> {
    let research = state.get("research_plan").unwrap_or(&Value::Null);
    let trader = state.get("trader_investment_plan").unwrap_or(&Value::Null);
    let final_decision = state.get("final_trade_decision").unwrap_or(&Value::Null);
    let allocation = state.get("portfolio_allocation").unwrap_or(&Value::Null);
    let mut summary = format!(
        "# ETF Allocation Summary\n\n- ticker: {}\n- research_rating: {}\n- long_probability: {}\n- short_probability: {}\n\n{}\n",
        state.get("ticker").and_then(Value::as_str).unwrap_or(""),
        research.get("rating").and_then(Value::as_str).unwrap_or(""),
        research.get("long_probability").map(Value::to_string).unwrap_or_default(),
        research.get("short_probability").map(Value::to_string).unwrap_or_default(),
        research.get("probability_rationale").and_then(Value::as_str).unwrap_or("")
    );
    if trader.is_object() || final_decision.is_object() {
        summary.push_str("\n## Trade Decision\n\n");
        summary.push_str(&format!(
            "- trader_action: {}\n- trader_position_size: {}\n- final_rating: {}\n- horizon: {}\n\n",
            trader.get("action").and_then(Value::as_str).unwrap_or(""),
            trader
                .get("position_size")
                .and_then(Value::as_str)
                .unwrap_or(""),
            final_decision
                .get("rating")
                .and_then(Value::as_str)
                .unwrap_or(""),
            final_decision
                .get("horizon")
                .and_then(Value::as_str)
                .unwrap_or("")
        ));
    }
    if allocation.is_object() {
        summary.push_str("\n## Portfolio Allocation\n\n");
        summary.push_str(&format!(
            "- vix_regime: {}\n- total_equity_exposure: {}\n- correlation_note: {}\n\n",
            allocation
                .get("vix_regime")
                .and_then(Value::as_str)
                .unwrap_or(""),
            allocation
                .get("total_equity_exposure")
                .map(Value::to_string)
                .unwrap_or_default(),
            allocation
                .get("correlation_note")
                .and_then(Value::as_str)
                .unwrap_or("")
        ));
        if let Some(weights) = allocation.get("weights").and_then(Value::as_object) {
            summary.push_str("| Asset | Weight | Rationale |\n| --- | ---: | --- |\n");
            for (asset, payload) in weights {
                let weight = payload.get("weight").and_then(Value::as_f64).unwrap_or(0.0);
                let rationale = payload
                    .get("rationale")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                summary.push_str(&format!(
                    "| {asset} | {:.1}% | {} |\n",
                    weight * 100.0,
                    rationale
                ));
            }
            summary.push('\n');
        }
        if let Some(text) = allocation.get("summary").and_then(Value::as_str) {
            summary.push_str(text);
            summary.push('\n');
        }
    }
    std::fs::write(run_dir.join("final_summary.md"), summary)?;
    Ok(())
}

pub(crate) fn topic_state(state: &Value, topic_id: &str) -> Option<Value> {
    state
        .get("topic_debate_states")
        .and_then(Value::as_object)
        .and_then(|items| items.get(topic_id))
        .cloned()
}

pub(crate) fn upsert_topic_debate_state(state: &mut Value, topic_id: &str, topic_state: Value) {
    if !state
        .get("topic_debate_states")
        .is_some_and(Value::is_object)
    {
        state["topic_debate_states"] = json!({});
    }
    if let Some(items) = state["topic_debate_states"].as_object_mut() {
        items.insert(topic_id.to_string(), topic_state);
    }
}

pub(crate) fn append_topic_turn(state: &mut Value, topic_id: &str, turn: Value) {
    if !state
        .get("topic_debate_states")
        .is_some_and(Value::is_object)
    {
        state["topic_debate_states"] = json!({});
    }
    if let Some(items) = state["topic_debate_states"].as_object_mut() {
        let entry = items.entry(topic_id.to_string()).or_insert_with(|| {
            json!({
                "topic": {"topic_id": topic_id, "topic": topic_id, "tickers": []},
                "turns": [],
                "controller_artifacts": []
            })
        });
        if !entry.get("turns").is_some_and(Value::is_array) {
            entry["turns"] = json!([]);
        }
        if let Some(turns) = entry["turns"].as_array_mut() {
            turns.push(turn);
        }
    }
}

pub(crate) fn set_topic_controller_state(state: &mut Value, topic_id: &str, artifact: Value) {
    if !state
        .get("topic_debate_states")
        .is_some_and(Value::is_object)
    {
        state["topic_debate_states"] = json!({});
    }
    if let Some(items) = state["topic_debate_states"].as_object_mut() {
        let entry = items.entry(topic_id.to_string()).or_insert_with(|| {
            json!({
                "topic": {"topic_id": topic_id, "topic": topic_id, "tickers": []},
                "turns": [],
                "controller_artifacts": []
            })
        });
        entry["controller_artifact"] = artifact;
    }
}

pub(crate) fn append_topic_controller_artifact(state: &mut Value, topic_id: &str, artifact: Value) {
    if !state
        .get("topic_debate_states")
        .is_some_and(Value::is_object)
    {
        state["topic_debate_states"] = json!({});
    }
    if let Some(items) = state["topic_debate_states"].as_object_mut() {
        let entry = items.entry(topic_id.to_string()).or_insert_with(|| {
            json!({
                "topic": {"topic_id": topic_id, "topic": topic_id, "tickers": []},
                "turns": [],
                "controller_artifacts": []
            })
        });
        if !entry
            .get("controller_artifacts")
            .is_some_and(Value::is_array)
        {
            entry["controller_artifacts"] = json!([]);
        }
        if let Some(items) = entry["controller_artifacts"].as_array_mut() {
            items.push(artifact);
        }
    }
}
