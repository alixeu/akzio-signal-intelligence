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
        "TradeIntent" => validate_trade_intent_payload(payload),
        "RiskConstraints" => validate_risk_constraints_payload(payload),
        "FinalValidation" => validate_final_validation_payload(payload),
        "PortfolioAllocation" => validate_portfolio_allocation_payload(payload),
        _ => None,
    }
}

fn validate_trade_intent_payload(payload: &Value) -> Option<String> {
    serde_json::from_value::<TradeIntent>(payload.clone())
        .err()
        .map(|error| error.to_string())
        .or_else(|| required_string_error(payload, "position_size"))
}

fn validate_risk_constraints_payload(payload: &Value) -> Option<String> {
    if payload.get("status").and_then(Value::as_str) == Some("skipped") {
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
