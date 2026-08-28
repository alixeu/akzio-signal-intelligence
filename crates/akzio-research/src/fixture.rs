//! Deterministic model fixture used by Debug and Paper Dry Run.

use std::collections::BTreeMap;

use akzio_model::ModelClient;
use serde_json::Value;

pub fn fixture_claim_output() -> Value {
    serde_json::json!({
        "schema_version": akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        "topic": "fixture_market_regime",
        "statement": "The governed fixture evidence supports a neutral fixture claim.",
        "horizon": "t5",
        "stance": "neutral",
        "materiality_ppm": 500_000,
        "confidence_ppm": 500_000,
        "grounds": [{
            "evidence": {
                "artifact_id": akzio_model::FIXTURE_CONTEXT_EVIDENCE_ID,
                "kind": "normalized_evidence"
            },
            "support": "The selected governed fixture evidence is the stated support.",
            "role": "descriptive",
            "assets": [],
            "domain": null
        }],
        "evidence_gaps": []
    })
}

pub fn fixture_critique_output() -> Value {
    serde_json::json!({
        "schema_version": akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        "target": {
            "artifact_id": akzio_model::FIXTURE_CONTEXT_CLAIM_ID,
            "kind": "claim"
        },
        "topic": "fixture_market_regime",
        "severity": "low",
        "blocker": false,
        "rationale": "The fixture records an explicit evidence gap rather than inventing a rebuttal.",
        "grounds": [],
        "evidence_gaps": [{
            "topic": "fixture_depth",
            "rationale": "No additional governed detail was selected for the fixture critique.",
            "impact": "warning",
            "supplemental_needs": []
        }]
    })
}

pub fn fixture_model_client() -> ModelClient {
    let planner = serde_json::json!({
        "schema_version": akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        "topology_id": "active",
        "tasks": {
            "analyst": {
                "recipe_id": "research.analyst",
                "objective": "Produce a fixture claim",
                "depends_on": [],
                "priority": 80,
                "evidence_needs": []
            }
        },
        "stop_reason": "fixture planner has no configured evidence adapter"
    });
    let forecasts = akzio_domain::Asset::EXECUTABLE
        .into_iter()
        .flat_map(|asset| {
            ["t1", "t3", "t5"].into_iter().map(move |horizon| {
                serde_json::json!({
                    "asset": asset.symbol(),
                    "horizon": horizon,
                    "positive_return_probability_ppm": 500000,
                    "expected_return_ppm": 0,
                })
            })
        })
        .collect::<Vec<_>>();
    let responses = |output: Value| {
        let output = serde_json::json!({
            "result": output,
            "deliberation": {
                "selected_path": "fixture path",
                "alternatives": [],
                "alternative_match_ppm": [],
                "uncertainties": [],
                "uncertainty_weight_ppm": [],
                "basis_artifact_ids": [],
                "confidence_ppm": 1000000
            }
        });
        vec![
            serde_json::json!({
                "output": [{
                    "type": "message",
                    "content": [{"type": "output_text", "text": "fixture research memo"}]
                }]
            }),
            serde_json::json!({
                "output": [{
                    "type": "function_call",
                    "call_id": "fixture-submit",
                    "name": "submit_result",
                    "arguments": serde_json::to_string(&output).expect("static fixture JSON")
                }]
            }),
        ]
    };
    ModelClient::fixture_by_purpose(BTreeMap::from([
        ("research.planner".to_owned(), responses(planner)),
        (
            "research.analyst".to_owned(),
            responses(fixture_claim_output()),
        ),
        (
            "research.critic".to_owned(),
            responses(fixture_critique_output()),
        ),
        (
            "research.synthesizer".to_owned(),
            responses(serde_json::json!({
                "summary": "fixture decision draft",
                "confidence_ppm": 500000,
                "forecasts": forecasts,
                "claims": [{
                    "artifact_id": akzio_model::FIXTURE_CONTEXT_CLAIM_ID,
                    "kind": "claim"
                }],
                "critiques": [],
                "evidence": [{
                    "artifact_id": akzio_model::FIXTURE_CONTEXT_EVIDENCE_ID,
                    "kind": "normalized_evidence"
                }],
                "material_conflicts": [],
                "hard_blockers": [],
                "soft_warnings": []
            })),
        ),
    ]))
}
