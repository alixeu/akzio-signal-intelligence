use orchestrator_core::run_slug;
use serde_json::{json, Value};

pub(crate) fn run_id_for(tickers: &[String], date: &str) -> String {
    format!("{}-{}-exec", run_slug(tickers).to_ascii_lowercase(), date)
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

pub(crate) fn topic_state(state: &Value, topic_id: &str) -> Option<Value> {
    state
        .get("topic_debate_states")
        .and_then(Value::as_object)
        .and_then(|items| items.get(topic_id))
        .cloned()
}

pub(crate) fn research_plan_to_trade_intent(research_plan: &Value) -> Value {
    let rating = research_plan
        .get("rating")
        .and_then(Value::as_str)
        .unwrap_or("");
    let action = match rating {
        "Buy" | "Overweight" => "Buy",
        "Sell" | "Underweight" => "Sell",
        _ => "Hold",
    };
    let rationale = research_plan
        .get("probability_rationale")
        .and_then(Value::as_str)
        .or_else(|| research_plan.get("plan").and_then(Value::as_str))
        .unwrap_or("Research data is missing or not decisive.");
    json!({
        "action": action,
        "candidate_action": action,
        "execution_decision": if action == "Hold" { "hold" } else { "execute_candidate" },
        "entry_price": null,
        "stop_loss": null,
        "position_size_pct_max": if action == "Hold" { 0.0 } else { 0.30 },
        "blockers": [],
        "rationale": format!("{action} mapped conservatively from research_plan. {rationale}")
    })
}

#[cfg(test)]
mod tests {
    use super::run_id_for;

    #[test]
    fn run_id_does_not_depend_on_filesystem_path() {
        assert_eq!(
            run_id_for(&["QQQ".into(), "SOXX".into(), "VIX".into()], "2026-07-10"),
            "qqq_soxx_vix-2026-07-10-exec"
        );
    }
}
