//! Deterministic model fixture used by the formal Paper and PositionPlan graphs.

// Fixture 只模拟模型在 Submit 边界返回的结构化数据；它不模拟券商、时钟、成交、
// Policy 或 Outcome。正式 Runtime 仍会重新绑定 Manifest 引用、校验 Contract，
// 并把中性/零目标结果与后续 Decision、Execution、Paper 状态分开持久化。
// 文件导读：`lib.rs` 将这些构造器重新导出，当前调用方包括隔离 Debug 的 fixture
// daemon、`debug verify-fixture` 和研究质量流程的 offline 阶段。`fixture_model_client`
// 按 purpose/phase 生成本地 Responses 模板；`akzio-model` 再依据当前请求 Schema 解析
// 占位引用，最后仍由 AgentRuntime 校验。测试覆盖并发 horizon、Schema 绑定与故障序列；
// 建议先读两个固定输出，再读 client 构造器和 `phase_fixture_tests`。
use std::collections::BTreeMap;

use akzio_model::ModelClient;
use serde_json::Value;

// 每次创建一份新的 Claim JSON 值，供离线模型适配器或质量实验作为中性样例；
// 这里构造的是模型输入/输出数据，不会自行验证引用或写入 Store。
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

// 返回“信息不足”的 Critique fixture；它示范结构化字段，不表示 Rust Gate 已认可 Claim。
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

// 为每个研究 purpose 提供固定阶段表：研究 Draft 槽位为 Null，Submit 槽位才有结果。
// 返回的 ModelClient 只模拟模型响应；请求方仍须提供当前 Schema，Rust 校验仍决定是否生成 Artifact。
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
    // 外层 flat_map 对每个可执行资产生成一组 horizon；内层 move 闭包取得该资产值，
    // 数组迭代器随后由 collect 消费，产出固定的 4×3 Forecast 列表。
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
    // 每个资产都明确为零研究权重并把全部额度留在现金；这只是 fixture 的研究提案字段。
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
    // 闭包接收并拥有一个角色输出，封装成统一的 result + deliberation 提交参数；
    // 返回数组按 ModelClient 约定分别占据 Draft 与 Submit 两个阶段槽位。
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
                    // fixture Value 是本地固定数据；序列化失败会由 expect 作为程序错误 panic，
                    // 不会吞掉真实 provider 或用户输入中的序列化错误。
                    "arguments": serde_json::to_string(&output).expect("static fixture JSON")
                }]
            }),
        ]
    };
    ModelClient::fixture_by_purpose_phase(BTreeMap::from([
        (
            "research.proposal_reviewer".to_owned(),
            // 对每个冻结 Review scope 生成一个固定的 accepted 模板；空引用/问题只是
            // 此离线提案的回答输入，仍由 Runtime 的正式 ProposalReview validator 校验。
            responses(
                serde_json::json!({"assessments":akzio_domain::proposal_review_keys().into_iter().map(|scope| serde_json::json!({"scope":scope,"accepted":true,"rationale":"The deterministic fixture explicitly abstains and makes no calibration claim.","evidence_refs":[],"issues":[]})).collect::<Vec<_>>()}),
            ),
        ),
        ("research.analyst".to_owned(), responses(claim)),
        ("research.critic".to_owned(), responses(critique)),
        (
            "research.synthesizer".to_owned(),
            // proposal_review_keys 的迭代器逐个生成数值依据行；其中 inputs 是 Schema 标记，
            // 在 ModelClient fixture 层按这次请求允许的 Artifact ID 物化。
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

// `cfg(test)` 只在测试构建时编入；以下请求构造器和异步测试仅驱动本地 ModelClient，
// 不启动 daemon、访问 Store 或发送网络请求。
#[cfg(test)]
mod phase_fixture_tests {
    use super::*;
    use akzio_model::{ModelInput, ModelRequest, ModelToolChoice, ModelToolDefinition};
    use serde_json::json;

    // 用拥有型 ModelRequest 表示一个 horizon-scoped Analyst 请求；参数借用只用于
    // 格式化/JSON 构造，返回后 prompt 与 context 字符串均由请求自身持有。
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

    // 消费传入的 request 并为其附加只允许 evidence_id 的 Submit Schema；
    // `evidence_id` 的借用在 JSON 构造时复制进 enum，不会成为返回值的悬垂引用。
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

    // `async move` 将每个 horizon、client clone 和 barrier clone 捕获进独立 Future；
    // join_all 同时驱动三项任务，barrier 确认并发请求不会串用 horizon 或引用。
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

    // 当前 Submit Schema 是占位符的唯一绑定来源：即使旧 Context 含旧 ID，也必须采用
    // schema enum 中的新 ID；空 enum 不允许回退到旧材料。
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

    // 故障注入序列与 purpose 队列都是有限输入；一次消费后第二次明确 Exhausted，
    // 不能循环复用旧响应伪造成功。
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
