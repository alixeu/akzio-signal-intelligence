use serde_json::{json, Value};

pub(crate) fn research_plan_to_trade_intent(research_plan: &Value) -> Value {
    let rating = research_plan
        .get("rating")
        .and_then(Value::as_str)
        .unwrap_or("");
    let long_probability = research_plan
        .get("long_probability")
        .and_then(Value::as_f64);
    let short_probability = research_plan
        .get("short_probability")
        .and_then(Value::as_f64);
    let action = trade_action(rating, long_probability, short_probability);
    json!({
        "action": action,
        "entry_price": null,
        "stop_loss": null,
        "position_size": position_size(action),
        "rationale": rationale(action, research_plan),
        "method": "conservative_research_plan_mapping",
        "source": "research_plan"
    })
}

fn trade_action(
    rating: &str,
    long_probability: Option<f64>,
    short_probability: Option<f64>,
) -> &'static str {
    let rating = rating.to_ascii_lowercase();
    if matches!(
        rating.as_str(),
        "buy" | "strong buy" | "long" | "bullish" | "overweight"
    ) && long_probability.is_some_and(|probability| probability >= 0.60)
    {
        "Buy"
    } else if matches!(
        rating.as_str(),
        "sell" | "strong sell" | "short" | "bearish" | "underweight"
    ) && (short_probability.is_some_and(|probability| probability >= 0.60)
        || long_probability.is_some_and(|probability| probability <= 0.40))
    {
        "Sell"
    } else {
        "Hold"
    }
}

fn position_size(action: &str) -> &'static str {
    match action {
        "Buy" | "Sell" => "0%-30%",
        _ => "0%",
    }
}

fn rationale(action: &str, research_plan: &Value) -> String {
    let suffix = research_plan
        .get("probability_rationale")
        .and_then(Value::as_str)
        .or_else(|| research_plan.get("plan").and_then(Value::as_str))
        .unwrap_or("Research data is missing or not decisive.");
    format!("{action} mapped conservatively from research_plan. {suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_supported_buy_without_copying_market_decision_fields() {
        let research_plan = json!({
            "rating": "Buy",
            "long_probability": 0.67,
            "short_probability": 0.33,
            "plan": "Upside thesis stays in research.",
            "probability_rationale": "Bull case has enough support."
        });

        let intent = research_plan_to_trade_intent(&research_plan);

        assert_eq!(intent["action"], "Buy");
        assert_eq!(intent["entry_price"], Value::Null);
        assert_eq!(intent["stop_loss"], Value::Null);
        assert_eq!(intent["position_size"], "0%-30%");
        assert!(intent.get("rating").is_none());
        assert!(intent.get("long_probability").is_none());
        assert!(intent.get("short_probability").is_none());
        assert!(intent.get("plan").is_none());
    }

    #[test]
    fn holds_when_rating_or_probability_is_missing_or_neutral() {
        for research_plan in [
            json!({"rating": "Buy", "long_probability": 0.59}),
            json!({"long_probability": 0.80, "short_probability": 0.20}),
            json!({"rating": "Hold", "long_probability": 0.80}),
            json!({}),
        ] {
            let intent = research_plan_to_trade_intent(&research_plan);
            assert_eq!(intent["action"], "Hold");
            assert_eq!(intent["position_size"], "0%");
        }
    }

    #[test]
    fn maps_supported_sell_from_short_or_low_long_probability() {
        let short_supported = research_plan_to_trade_intent(&json!({
            "rating": "Sell",
            "long_probability": 0.45,
            "short_probability": 0.61
        }));
        let low_long_supported = research_plan_to_trade_intent(&json!({
            "rating": "Bearish",
            "long_probability": 0.39
        }));

        assert_eq!(short_supported["action"], "Sell");
        assert_eq!(low_long_supported["action"], "Sell");
    }
}
