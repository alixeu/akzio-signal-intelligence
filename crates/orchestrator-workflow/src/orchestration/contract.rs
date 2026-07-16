use orchestrator_core::{
    final_validation_schema, portfolio_allocation_schema, risk_constraints_schema,
    trade_intent_schema, validate_risk_constraints, FinalValidation, PortfolioAllocation,
    RiskConstraints, TradeIntent,
};
use serde_json::{json, Value};

struct PhaseContract {
    phase: i64,
    name: &'static str,
    state_field: &'static str,
    responsibility: &'static str,
}

const CONTRACTS: &[PhaseContract] = &[
    PhaseContract {
        phase: 1,
        name: "EvidenceBundle",
        state_field: "analyst_reports",
        responsibility: "collect and standardize raw evidence",
    },
    PhaseContract {
        phase: 15,
        name: "EvidenceState",
        state_field: "phase1_state_artifact",
        responsibility: "compress neutral evidence",
    },
    PhaseContract {
        phase: 2,
        name: "TopicPlan",
        state_field: "topic_generation_artifact",
        responsibility: "select debate topics from EvidenceState",
    },
    PhaseContract {
        phase: 25,
        name: "DebateSummary",
        state_field: "debate_state_artifact",
        responsibility: "compress structured debate",
    },
    PhaseContract {
        phase: 3,
        name: "ResearchDecision",
        state_field: "research_plan",
        responsibility: "own probability, rating, and market thesis",
    },
    PhaseContract {
        phase: 4,
        name: "TradeIntent",
        state_field: "trader_investment_plan",
        responsibility: "map ResearchDecision to executable intent",
    },
    PhaseContract {
        phase: 5,
        name: "RiskConstraints",
        state_field: "risk_debate_state",
        responsibility: "add risk constraints when policy triggers",
    },
    PhaseContract {
        phase: 6,
        name: "FinalValidation",
        state_field: "final_trade_decision",
        responsibility: "merge constraints without changing market truth",
    },
    PhaseContract {
        phase: 7,
        name: "PortfolioAllocation",
        state_field: "portfolio_allocation",
        responsibility: "allocate with Rust-enforced hard constraints",
    },
];

pub(crate) fn record_contracts(state: &mut Value) {
    let contracts = CONTRACTS
        .iter()
        .map(|contract| {
            let mut value = json!({
                "phase": contract.phase,
                "name": contract.name,
                "state_field": contract.state_field,
                "responsibility": contract.responsibility,
            });
            if let Some(schema) = contract_schema(contract.name) {
                value["schema"] = Value::String(schema);
            }
            value
        })
        .collect::<Vec<_>>();
    let violations = CONTRACTS
        .iter()
        .filter(|contract| phase_done(state, contract.phase))
        .flat_map(|contract| contract_violations(state, contract))
        .collect::<Vec<_>>();

    state["workflow_contracts"] = Value::Array(contracts);
    state["contract_violations"] = Value::Array(violations);
}

fn contract_violations(state: &Value, contract: &PhaseContract) -> Vec<Value> {
    let Some(payload) = state
        .get(contract.state_field)
        .filter(|value| !value.is_null())
    else {
        return vec![json!({
            "phase": contract.phase,
            "contract": contract.name,
            "missing_state_field": contract.state_field,
        })];
    };

    validate_contract_payload(contract.name, payload)
        .map(|message| {
            vec![json!({
                "phase": contract.phase,
                "contract": contract.name,
                "invalid_state_field": contract.state_field,
                "message": message,
            })]
        })
        .unwrap_or_default()
}

fn contract_schema(name: &str) -> Option<String> {
    match name {
        "TradeIntent" => Some(trade_intent_schema()),
        "RiskConstraints" => Some(risk_constraints_schema()),
        "FinalValidation" => Some(final_validation_schema()),
        "PortfolioAllocation" => Some(portfolio_allocation_schema()),
        _ => None,
    }
}

fn phase_done(state: &Value, phase: i64) -> bool {
    matches!(
        state
            .get("phase_status")
            .and_then(Value::as_object)
            .and_then(|value| value.get(&phase.to_string()))
            .and_then(Value::as_str),
        // Selective policy may finish a phase via derived/skipped artifacts
        // rather than a full LLM run; those still count as completed for
        // contract presence checks.
        Some("done") | Some("derived") | Some("skipped")
    )
}

fn validate_contract_payload(name: &str, payload: &Value) -> Option<String> {
    match name {
        "EvidenceState" => validate_evidence_state_payload(payload),
        "TopicPlan" => validate_topic_plan_payload(payload),
        "DebateSummary" => validate_debate_summary_payload(payload),
        "TradeIntent" => validate_trade_intent_payload(payload),
        "RiskConstraints" => validate_risk_constraints_payload(payload),
        "FinalValidation" => validate_final_validation_payload(payload),
        "PortfolioAllocation" => validate_portfolio_allocation_payload(payload),
        _ => None,
    }
}

fn validate_evidence_state_payload(payload: &Value) -> Option<String> {
    if payload.get("artifact_type").and_then(Value::as_str) != Some("phase1_state_artifact") {
        return Some("artifact_type must be phase1_state_artifact".to_string());
    }
    let Some(evidence_quality) = payload.get("evidence_quality").and_then(Value::as_object) else {
        return Some("evidence_quality is missing".to_string());
    };
    let Some(quality_status) = evidence_quality.get("status").and_then(Value::as_str) else {
        return Some("evidence_quality.status is missing".to_string());
    };
    if !matches!(
        quality_status,
        "actionable" | "partial" | "insufficient" | "blocked"
    ) {
        return Some(format!(
            "evidence_quality.status has invalid value {quality_status}"
        ));
    }
    let Some(per_ticker) = payload.get("per_ticker").and_then(Value::as_object) else {
        return Some("per_ticker is missing".to_string());
    };
    if per_ticker.is_empty() {
        return Some("per_ticker must contain at least one ticker".to_string());
    }
    for (ticker, ticker_state) in per_ticker {
        let Some(ticker_quality) = ticker_state
            .get("evidence_quality")
            .and_then(Value::as_object)
        else {
            return Some(format!("per_ticker.{ticker}.evidence_quality is missing"));
        };
        let valid_status = ticker_quality
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|value| matches!(value, "actionable" | "insufficient"));
        if !valid_status {
            return Some(format!(
                "per_ticker.{ticker}.evidence_quality.status is invalid"
            ));
        }
        let valid_basis = ticker_quality
            .get("confidence_basis")
            .and_then(Value::as_str)
            .is_some_and(|value| matches!(value, "evidence_available" | "data_insufficient"));
        if !valid_basis {
            return Some(format!(
                "per_ticker.{ticker}.evidence_quality.confidence_basis is invalid"
            ));
        }
    }
    None
}

fn validate_topic_plan_payload(payload: &Value) -> Option<String> {
    if payload.get("artifact_type").and_then(Value::as_str)
        != Some("phase2_topic_generation_artifact")
    {
        return Some("artifact_type must be phase2_topic_generation_artifact".to_string());
    }
    let Some(actionable) = payload.get("actionable").and_then(Value::as_bool) else {
        return Some("actionable is missing or not a boolean".to_string());
    };
    let Some(topics) = payload.get("topics").and_then(Value::as_array) else {
        return Some("topics is missing or not an array".to_string());
    };
    if !actionable && !topics.is_empty() {
        return Some("non-actionable topic plan must have topics=[]".to_string());
    }
    if !actionable
        && payload.get("status").and_then(Value::as_str) != Some("skipped_no_actionable_evidence")
    {
        return Some(
            "non-actionable topic plan must be skipped_no_actionable_evidence".to_string(),
        );
    }
    if actionable && topics.is_empty() {
        return Some("actionable topic plan must contain at least one topic".to_string());
    }
    None
}

fn validate_debate_summary_payload(payload: &Value) -> Option<String> {
    if payload.get("artifact_type").and_then(Value::as_str)
        != Some("phase2_5_debate_state_artifact")
    {
        return Some("artifact_type must be phase2_5_debate_state_artifact".to_string());
    }
    let Some(status) = payload.get("status").and_then(Value::as_str) else {
        return Some("debate status is missing".to_string());
    };
    let Some(convergence_status) = payload.get("convergence_status").and_then(Value::as_str) else {
        return Some("convergence_status is missing".to_string());
    };
    if !matches!(
        status,
        "ready" | "not_converged" | "skipped_no_actionable_evidence"
    ) {
        return Some(format!("debate status has invalid value {status}"));
    }
    if !matches!(
        convergence_status,
        "converged_or_pending_review" | "not_converged" | "skipped"
    ) {
        return Some(format!(
            "convergence_status has invalid value {convergence_status}"
        ));
    }
    let Some(topic_briefs) = payload.get("topic_briefs").and_then(Value::as_array) else {
        return Some("topic_briefs is missing or not an array".to_string());
    };
    if status == "skipped_no_actionable_evidence" && !topic_briefs.is_empty() {
        return Some("skipped debate summary must have topic_briefs=[]".to_string());
    }
    None
}

fn validate_trade_intent_payload(payload: &Value) -> Option<String> {
    serde_json::from_value::<TradeIntent>(payload.clone())
        .err()
        .map(|error| error.to_string())
        .or_else(|| required_string_error(payload, "position_size"))
}

fn validate_risk_constraints_payload(payload: &Value) -> Option<String> {
    if payload.get("status").and_then(Value::as_str) == Some("skipped")
        || risk_constraints_are_degraded(payload)
    {
        return None;
    }
    if let Ok(parsed) = serde_json::from_value::<RiskConstraints>(payload.clone()) {
        let combined = validate_risk_constraints(&parsed)
            .err()
            .or_else(|| required_string_error(payload, "recommended_adjustment"));
        if let Some(error) = combined {
            return Some(error);
        }
        return None;
    }

    let Some(history) = payload.get("history").and_then(Value::as_array) else {
        return serde_json::from_value::<RiskConstraints>(payload.clone())
            .err()
            .map(|error| error.to_string());
    };
    for (index, turn) in history.iter().enumerate() {
        let Some(artifact) = turn.get("artifact") else {
            return Some(format!("history[{index}].artifact is missing"));
        };
        if risk_constraints_are_degraded(artifact) {
            continue;
        }
        match serde_json::from_value::<RiskConstraints>(artifact.clone()) {
            Ok(parsed) => {
                let combined = validate_risk_constraints(&parsed)
                    .err()
                    .or_else(|| required_string_error(artifact, "recommended_adjustment"));
                if let Some(error) = combined {
                    return Some(format!("history[{index}].artifact: {error}"));
                }
            }
            Err(error) => {
                return Some(format!("history[{index}].artifact: {}", error));
            }
        }
    }
    None
}

fn risk_constraints_are_degraded(payload: &Value) -> bool {
    payload.get("artifact_type").and_then(Value::as_str) == Some("degraded_risk_perspective")
        || payload.get("degraded").and_then(Value::as_bool) == Some(true)
        || payload.get("usable").and_then(Value::as_bool) == Some(false)
        || matches!(
            payload.get("status").and_then(Value::as_str),
            Some("degraded" | "missing" | "error")
        )
}

fn validate_final_validation_payload(payload: &Value) -> Option<String> {
    serde_json::from_value::<FinalValidation>(payload.clone())
        .err()
        .map(|error| error.to_string())
        .or_else(|| required_string_error(payload, "execution_summary"))
}

fn validate_portfolio_allocation_payload(payload: &Value) -> Option<String> {
    serde_json::from_value::<PortfolioAllocation>(payload.clone())
        .err()
        .map(|error| error.to_string())
}

fn required_string_error(payload: &Value, field: &str) -> Option<String> {
    let valid = payload
        .get(field)
        .and_then(Value::as_str)
        .is_some_and(|value| !value.trim().is_empty());
    if valid {
        None
    } else {
        Some(format!(
            "required contract field {field} is missing or empty"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_missing_field_for_completed_phase_only() {
        let mut state = json!({
            "phase_status": {"3": "done", "4": "done"},
            "research_plan": {}
        });

        record_contracts(&mut state);

        assert_eq!(state["workflow_contracts"].as_array().unwrap().len(), 9);
        assert_eq!(state["contract_violations"][0]["phase"], 4);
        assert_eq!(
            state["contract_violations"][0]["missing_state_field"],
            "trader_investment_plan"
        );
    }

    #[test]
    fn rejects_phase1_state_without_evidence_quality_contract() {
        let mut state = json!({
            "phase_status": {"15": "done"},
            "phase1_state_artifact": {
                "artifact_type": "phase1_state_artifact",
                "status": "ready",
                "per_ticker": {"QQQ": {}}
            }
        });

        record_contracts(&mut state);

        assert_eq!(state["contract_violations"][0]["phase"], 15);
        assert!(state["contract_violations"][0]["message"]
            .as_str()
            .unwrap()
            .contains("evidence_quality"));
    }

    #[test]
    fn accepts_explicit_no_evidence_topic_and_debate_skips() {
        let mut state = json!({
            "phase_status": {"2": "done", "25": "done"},
            "topic_generation_artifact": {
                "artifact_type": "phase2_topic_generation_artifact",
                "status": "skipped_no_actionable_evidence",
                "actionable": false,
                "topics": []
            },
            "debate_state_artifact": {
                "artifact_type": "phase2_5_debate_state_artifact",
                "status": "skipped_no_actionable_evidence",
                "convergence_status": "skipped",
                "topic_briefs": []
            }
        });

        record_contracts(&mut state);

        assert_eq!(state["contract_violations"], json!([]));
    }

    #[test]
    fn rejects_ready_debate_summary_without_convergence_status() {
        let mut state = json!({
            "phase_status": {"25": "done"},
            "debate_state_artifact": {
                "artifact_type": "phase2_5_debate_state_artifact",
                "status": "ready",
                "topic_briefs": []
            }
        });

        record_contracts(&mut state);

        assert_eq!(state["contract_violations"][0]["phase"], 25);
        assert!(state["contract_violations"][0]["message"]
            .as_str()
            .unwrap()
            .contains("convergence_status"));
    }

    #[test]
    fn downstream_contracts_include_machine_schema() {
        let mut state = json!({});

        record_contracts(&mut state);

        let contracts = state["workflow_contracts"].as_array().unwrap();
        for name in [
            "TradeIntent",
            "RiskConstraints",
            "FinalValidation",
            "PortfolioAllocation",
        ] {
            let item = contracts
                .iter()
                .find(|contract| contract["name"] == name)
                .unwrap();
            assert!(item["schema"].as_str().unwrap().contains("properties"));
        }
        let evidence = contracts
            .iter()
            .find(|contract| contract["name"] == "EvidenceBundle")
            .unwrap();
        assert!(evidence.get("schema").is_none());
    }

    #[test]
    fn reports_invalid_downstream_contract_payload() {
        let mut state = json!({
            "phase_status": {"4": "done"},
            "trader_investment_plan": {"action": "Buy"}
        });

        record_contracts(&mut state);

        assert_eq!(state["contract_violations"][0]["phase"], 4);
        assert_eq!(state["contract_violations"][0]["contract"], "TradeIntent");
        assert_eq!(
            state["contract_violations"][0]["invalid_state_field"],
            "trader_investment_plan"
        );
        assert!(state["contract_violations"][0]["message"]
            .as_str()
            .unwrap()
            .contains("position_size"));
    }

    #[test]
    fn validates_risk_constraints_inside_debate_history() {
        let mut state = json!({
            "phase_status": {"5": "done"},
            "risk_debate_state": {
                "history": [
                    {
                        "artifact": {
                            "stance": "neutral",
                            "argument": "No additional constraint.",
                            "recommended_adjustment": "none"
                        }
                    }
                ]
            }
        });

        record_contracts(&mut state);

        assert_eq!(state["contract_violations"], json!([]));
    }

    #[test]
    fn accepts_skipped_risk_review_contract() {
        let mut state = json!({
            "phase_status": {"5": "done"},
            "risk_debate_state": {
                "status": "skipped",
                "history": [],
                "constraints": []
            }
        });

        record_contracts(&mut state);

        assert_eq!(state["contract_violations"], json!([]));
    }

    #[test]
    fn excludes_degraded_risk_perspectives_from_constraint_validation() {
        let mut state = json!({
            "phase_status": {"5": "done"},
            "risk_debate_state": {
                "history": [
                    {
                        "artifact": {
                            "artifact_type": "degraded_risk_perspective",
                            "status": "degraded",
                            "degraded": true,
                            "usable": false,
                            "missing_perspective": "risk.conservative",
                            "degraded_reason": "stream failed"
                        }
                    },
                    {
                        "artifact": {
                            "stance": "neutral",
                            "argument": "No additional constraint.",
                            "recommended_adjustment": "none",
                            "position_cap_pct": 0.4
                        }
                    }
                ]
            }
        });

        record_contracts(&mut state);

        assert_eq!(state["contract_violations"], json!([]));
    }

    #[test]
    fn reports_incomplete_direct_risk_constraints() {
        let mut state = json!({
            "phase_status": {"5": "done"},
            "risk_debate_state": {
                "stance": "neutral",
                "argument": "Risk review ran but did not state constraints."
            }
        });

        record_contracts(&mut state);

        assert_eq!(state["contract_violations"][0]["phase"], 5);
        assert!(state["contract_violations"][0]["message"]
            .as_str()
            .unwrap()
            .contains("recommended_adjustment"));
    }
}
