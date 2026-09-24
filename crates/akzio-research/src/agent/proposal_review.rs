// ProposalReview 的 Schema 生成与 identity 绑定是研究提案进入 DecisionGate 前的
// 最后一道模型协议边界。Rust 负责把精确 proposal hash、Manifest、Contract 写回
// Review，不允许模型自报身份；Review 通过也只说明 17 个 scope 的依据经过审查。
fn reviewed_research_schema(mut schema: Value) -> Value {
    // 当前结构化研究把旧 supplemental_needs 换成类型化请求；实际资源与 cutoff
    // 仍由 Rust 的补采协调节点冻结，Schema 不直接授权采集。
    if let Some(gap) = schema.pointer_mut("/properties/evidence_gaps/items") {
        gap["properties"]
            .as_object_mut()
            .unwrap()
            .remove("supplemental_needs");
        gap["required"]
            .as_array_mut()
            .unwrap()
            .retain(|v| v != "supplemental_needs");
        gap["required"]
            .as_array_mut()
            .unwrap()
            .push(json!("supplemental_requests"));
        gap["properties"]["supplemental_requests"] = json!({"type":"array","maxItems":8,"items":{
            "type":"object","additionalProperties":false,
            "properties":{"kind":{"type":"string","enum":["news","price","macro"]},
                "assets":{"type":"array","minItems":1,"maxItems":4,"uniqueItems":true,"items":{"type":"string","enum":["TQQQ","QQQ","SOXX","SOXL"]}},
                "series":{"type":"array","uniqueItems":true,"maxItems":5,"items":{"type":"string","enum":["DFF","DFII10","VIXCLS","DGS2","DGS10"]}},
                "query":{"type":"string","minLength":1,"maxLength":2048}},
            "required":["kind","assets","series","query"]}});
    }
    schema
}
fn reviewed_proposal_schema() -> Value {
    // numeric_basis 固定覆盖 12 个 Forecast、4 个资产 allocation 和 cash 共 17 项，
    // 让 Reviewer 能逐项判断单位、方法、假设和不确定性，而不是只看 summary。
    let mut schema = decision_proposal_output_schema();
    schema["properties"]["numeric_basis"] = json!({"type":"array","minItems":17,"maxItems":17,
        "items":{"type":"object","additionalProperties":false,"properties":{
            "scope":{"type":"string","enum":akzio_domain::proposal_review_keys()},
            "inputs":{"type":"array","minItems":1,"items":artifact_ref_schema(&["claim","critique","normalized_evidence","semantic_detail"])},
            "units":{"type":"string","minLength":1},"method":{"type":"string","minLength":1},
            "assumptions":{"type":"string","minLength":1},"uncertainty":{"type":"string","minLength":1}},
            "required":["scope","inputs","units","method","assumptions","uncertainty"]}});
    schema["required"]
        .as_array_mut()
        .unwrap()
        .push(json!("numeric_basis"));
    schema
}
fn proposal_review_schema() -> Value {
    // 每个 scope 都必须产生 assessment；issues 只描述可检查的修订问题，不能成为
    // 隐含的交易授权或 Policy 激活信号。
    let mut schema = json!({"type":"object","additionalProperties":false,"properties":{
        "proposal":artifact_ref_schema(&["decision_proposal"]),
        "proposal_hash":{"type":"string"},"manifest":artifact_ref_schema(&["context_manifest"]),"contract_hash":{"type":"string"},
        "assessments":{"type":"array","minItems":17,"maxItems":17,"items":{
            "type":"object","additionalProperties":false,"properties":{
                "scope":{"type":"string","enum":akzio_domain::proposal_review_keys()},
                "accepted":{"type":"boolean"},"rationale":{"type":"string","minLength":1},
                "evidence_refs":{"type":"array","items":artifact_ref_schema(&["claim","critique","normalized_evidence","semantic_detail"])}},
            "required":["scope","accepted","rationale","evidence_refs"]}}},
        "required":["proposal","proposal_hash","manifest","contract_hash","assessments"]});
    let assessment = &mut schema["properties"]["assessments"]["items"];
    assessment["properties"]["issues"] = json!({"type":"array","maxItems":3,"items":{
        "type":"object","additionalProperties":false,"properties":{
            "category":{"type":"string","enum":["support_missing","source_qualification","conflicting_support","temporal_mismatch","unit_error","estimate_basis_mismatch","allocation_inconsistency"]},
            "field_path":{"type":"string","minLength":1,"maxLength":256},
            "correction_criterion":{"type":"string","minLength":1,"maxLength":2048},
            "evidence_refs":{"type":"array","items":artifact_ref_schema(&["claim","critique","normalized_evidence","semantic_detail"])}},
        "required":["category","field_path","correction_criterion","evidence_refs"]}});
    assessment["required"]
        .as_array_mut()
        .unwrap()
        .push(json!("issues"));
    schema
}
fn remove_review_identity(schema: &mut Value) {
    // 模型 wire 不填写 proposal/manifest/contract identity；这些字段由当前 Manifest
    // 和 Store artifact hash 注入，避免模型把旧 Review 绑定到新提案。
    let result = &mut schema["properties"]["result"];
    for key in ["proposal", "proposal_hash", "manifest", "contract_hash"] {
        result["properties"].as_object_mut().unwrap().remove(key);
        result["required"]
            .as_array_mut()
            .unwrap()
            .retain(|v| v != key);
    }
}
fn bind_review_identity(
    store: &Store,
    manifest: &ContextManifest,
    contract: &AgentContract,
    arguments: &mut Value,
) -> ResearchResult<()> {
    // Review 只能绑定 Manifest 中唯一的 DecisionProposal，并使用其实际 blob hash。
    // 这里成功只是形成可验证的 Review payload，后续仍需 Rust Review/Decision Gate。
    let proposals = manifest
        .payload
        .selections
        .iter()
        .filter(|s| s.artifact.kind == ArtifactKind::DecisionProposal)
        .collect::<Vec<_>>();
    let [selection] = proposals.as_slice() else {
        return Err(ResearchError::InvalidOutput(
            "review requires exactly one selected proposal".into(),
        ));
    };
    let artifact = store.artifact(&selection.artifact.artifact_id)?;
    let result = &mut arguments["result"];
    result["proposal"] = json!(selection.artifact);
    result["proposal_hash"] = json!(artifact.blob.hash);
    result["manifest"] =
        json!({"artifact_id":manifest.artifact.artifact_id,"kind":"context_manifest"});
    result["contract_hash"] = json!(contract.contract_hash);
    Ok(())
}
