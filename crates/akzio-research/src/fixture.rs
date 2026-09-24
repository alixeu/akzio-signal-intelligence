//! Deterministic model fixture used by the formal Paper and PositionPlan graphs.

// Fixture 只模拟模型在 Submit 边界返回的结构化数据；它不模拟券商、时钟、成交、
// Policy 或 Outcome。正式 Runtime 仍会重新绑定 Manifest 引用、校验 Contract，
// 并把中性/零目标结果与后续 Decision、Execution、Paper 状态分开持久化。
use std::collections::BTreeMap;

use akzio_model::ModelClient;
use serde_json::Value;

pub fn fixture_claim_output() -> Value {
    // Claim 明确保持 neutral，避免离线样例把“有输出”误读成有方向性证据。
    serde_json::json!({
        "schema_version": akzio_domain::DOMAIN_SCHEMA_VERSION,
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
    // Critic 记录信息不足而非凭空制造反证；这仍然是结构化审查产物，不是 Gate 放行。
    serde_json::json!({
        "schema_version": akzio_domain::DOMAIN_SCHEMA_VERSION,
        "target": {
            "artifact_id": akzio_model::FIXTURE_CONTEXT_CLAIM_ID,
            "kind": "claim"
        },
        "topic": "fixture_market_regime",
        "severity": "low",
        "blocker": false,
        "verification_status": "not_enough_information",
        "supporting_refs": [], "conflicting_refs": [],
        "rationale": "The fixture records an explicit evidence gap rather than inventing a rebuttal.",
        "grounds": [],
        "evidence_gaps": [{
            "topic": "fixture_depth",
            "rationale": "No additional governed detail was selected for the fixture critique.",
            "impact": "warning",
            "retriable": false, "supplemental_requests": [], "assets": [], "horizons": []
        }]
    })
}

pub fn fixture_model_client() -> ModelClient {
    // 每个 purpose/phase 使用相同的闭包构造 Submit 响应；BTreeMap 使 fixture 选择
    // 按角色隔离，重复请求不会消耗另一任务的响应或跨 Run 共享可变状态。
    let mut claim = fixture_claim_output();
    claim["horizon"] = serde_json::json!("$fixture.task.horizon");
    claim["grounds"][0]["evidence"] = serde_json::json!(
        "$fixture.schema.first_ref:/properties/result/properties/grounds/items/properties/evidence"
    );
    let mut critique = fixture_critique_output();
    critique["target"] =
        serde_json::json!("$fixture.schema.first_ref:/properties/result/properties/target");
    let forecasts = akzio_domain::Asset::EXECUTABLE
        .into_iter()
        .flat_map(|asset| {
            ["t1", "t3", "t5"].into_iter().map(move |horizon| {
                let holding_days = match horizon {
                    "t1" => 1,
                    "t3" => 3,
                    _ => 5,
                };
                serde_json::json!({
                    "asset": asset.symbol(),
                    "horizon": horizon,
                    "positive_return_probability_ppm": 500000,
                    "expected_return_ppm": 0,
                    "thesis": {
                        "thesis_valid_until": "2030-01-01T00:00:00Z",
                        "expected_holding_period_days": holding_days,
                        "exit_condition": "exit at horizon",
                        "invalidation_conditions": ["forecast invalidated"]
                    }
                })
            })
        })
        .collect::<Vec<_>>();
    let research_allocations = akzio_domain::Asset::EXECUTABLE
        .into_iter()
        .map(|asset| {
            serde_json::json!({
                "asset": asset.symbol(),
                "target_weight_ppm": 0,
                "supporting_horizons": [],
                "evidence_refs": [],
                "rationale": "The deterministic fixture contains no directional research basis.",
                "abstention_reason": "fixture has no qualified directional evidence"
            })
        })
        .collect::<Vec<_>>();
    let responses = |output: Value| {
        // Research 角色没有 Draft；Outcome 的两阶段 fixture 在 ModelClient 侧另行配置。
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
        [
            serde_json::Value::Null, // Research has no Draft fixture response.
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
    ModelClient::fixture_by_purpose_phase(BTreeMap::from([
        (
            "research.proposal_reviewer".to_owned(),
            responses(
                serde_json::json!({"assessments":akzio_domain::proposal_review_keys().into_iter().map(|scope| serde_json::json!({"scope":scope,"accepted":true,"rationale":"The deterministic fixture explicitly abstains and makes no calibration claim.","evidence_refs":[],"issues":[]})).collect::<Vec<_>>()}),
            ),
        ),
        ("research.analyst".to_owned(), responses(claim)),
        ("research.critic".to_owned(), responses(critique)),
        (
            "research.synthesizer".to_owned(),
            responses(serde_json::json!({
                "numeric_basis": akzio_domain::proposal_review_keys().into_iter().map(|scope| serde_json::json!({
                    "scope":scope,"inputs":["$fixture.schema.first_ref:/properties/result/properties/numeric_basis/items/properties/inputs/items"],
                    "units":"ppm","method":"Neutral fixture: probability 500000, return and invested weights zero; cash 1000000.",
                    "assumptions":"No directional support in the offline fixture.","uncertainty":"Not empirically calibrated; explicit abstention."})).collect::<Vec<_>>(),
                "summary": "fixture decision draft",
                "confidence_ppm": 500000,
                "forecasts": forecasts,
                "research_allocation": {
                    "cash_weight_ppm": 1000000,
                    "allocations": research_allocations
                },
                "claims": "$fixture.schema.all_refs:/properties/result/properties/claims/items",
                "critiques": "$fixture.schema.all_refs:/properties/result/properties/critiques/items",
                "evidence": "$fixture.schema.all_refs:/properties/result/properties/evidence/items",
                "material_conflicts": [],
                "hard_blockers": [],
                "soft_warnings": []
            })),
        ),
    ]))
}

#[cfg(test)]
mod phase_fixture_tests {
    use super::*;
    use akzio_model::{ModelInput, ModelRequest, ModelToolChoice, ModelToolDefinition};
    use serde_json::json;

    fn request(horizon: &str, evidence_id: &str) -> ModelRequest {
        ModelRequest {
            instructions: "offline fixture".into(),
            input: ModelInput::Fresh {
                text: json!({
                    "objective": format!("[research_horizon={horizon}] Offline scoped research"),
                    "context_manifest": format!("manifest-{horizon}"),
                    "context": [{"kind": "normalized_evidence", "artifact_id": evidence_id}]
                })
                .to_string(),
            },
            max_output_tokens: 5000,
            reasoning_effort: None,
            tools: vec![],
            tool_choice: ModelToolChoice::Auto,
            fixture_key: Some("research.analyst".into()),
        }
    }

    fn submit(mut request: ModelRequest, evidence_id: &str) -> ModelRequest {
        request.tool_choice = ModelToolChoice::RequiredFunction("submit_result".into());
        request.tools = vec![ModelToolDefinition {
            name: "submit_result".into(),
            description: "Submit scoped fixture".into(),
            strict: true,
            input_schema: json!({"properties":{"result":{"properties":{"grounds":{"items":{"properties":{"evidence":{"properties":{"artifact_id":{"enum":[evidence_id]}}}}}}}}}}),
        }];
        request
    }

    #[tokio::test]
    async fn concurrent_formal_fixture_tasks_keep_phase_horizon_and_references_isolated() {
        let client = fixture_model_client();
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(3));
        let tasks = ["t1", "t3", "t5"].map(|horizon| {
            let client = client.clone();
            let barrier = barrier.clone();
            async move {
                let evidence = akzio_domain::ContentHash::of_bytes(horizon.as_bytes()).to_string();
                let request = request(horizon, &evidence);
                assert!(
                    client.respond(request.clone()).await.is_err(),
                    "research Draft is retired"
                );
                barrier.wait().await;
                let submit = submit(request, &evidence);
                let result = client.respond(submit.clone()).await.unwrap();
                let repeat = client.respond(submit).await.unwrap();
                assert_eq!(
                    result.tool_calls, repeat.tool_calls,
                    "replaying one request must not consume another task's response"
                );
                let value = &result.tool_calls[0].arguments;
                assert_eq!(value["result"]["horizon"], horizon);
                assert_eq!(value["result"]["stance"], "neutral");
                assert_eq!(
                    value["result"]["grounds"][0]["evidence"]["artifact_id"],
                    evidence
                );
            }
        });
        futures::future::join_all(tasks).await;
    }

    #[tokio::test]
    async fn fixture_templates_use_only_current_bound_schema_not_stale_context() {
        let client = fixture_model_client();
        let old = akzio_domain::ContentHash::of_bytes(b"old-context").to_string();
        let current = akzio_domain::ContentHash::of_bytes(b"current-enum").to_string();
        let request = request("t1", &old);
        let mut submit = submit(request, &current);
        let output = client.respond(submit.clone()).await.unwrap();
        assert_eq!(
            output.tool_calls[0].arguments["result"]["grounds"][0]["evidence"]["artifact_id"],
            current
        );
        submit.tools[0].input_schema["properties"]["result"]["properties"]["grounds"]["items"]
            ["properties"]["evidence"]["properties"]["artifact_id"]["enum"] = json!([]);
        assert!(
            client.respond(submit).await.is_err(),
            "an empty allowed enum must not fall back to old context"
        );
    }

    #[tokio::test]
    async fn fault_injection_sequences_keep_consumption_and_invalid_output_semantics() {
        let raw = json!({"output":[{"type":"function_call", "name":"submit_result", "call_id":"fault-injection", "arguments":"{\"horizon\":\"intentionally_invalid\"}"}]});
        let request = request("t1", "unused");
        let sequence = ModelClient::fixture_sequence([raw.clone()]);
        let output = sequence.respond(request.clone()).await.unwrap();
        assert_eq!(
            output.tool_calls[0].arguments["horizon"],
            "intentionally_invalid"
        );
        assert!(matches!(
            sequence.respond(request.clone()).await,
            Err(akzio_model::ModelError::FixtureExhausted)
        ));
        let by_purpose = ModelClient::fixture_by_purpose(BTreeMap::from([(
            "research.analyst".into(),
            vec![raw],
        )]));
        by_purpose.respond(request.clone()).await.unwrap();
        assert!(matches!(
            by_purpose.respond(request).await,
            Err(akzio_model::ModelError::FixtureExhausted)
        ));
    }
}
