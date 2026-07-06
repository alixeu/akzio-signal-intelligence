use serde_json::{json, Value};

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
        push_conflict(&mut violations, field, field, research_plan, downstream);
    }
    for downstream_field in ["plan", "thesis", "investment_thesis", "market_thesis"] {
        push_conflict(
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

fn push_conflict(
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
mod tests {
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
