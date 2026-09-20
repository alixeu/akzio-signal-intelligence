//! Explicitly opted-in network test. The normal suite never spends LLM tokens.
use super::*;
use akzio_domain::EvidenceAcquisitionMode;
use serde_json::Value;

#[tokio::test]
#[ignore = "requires AKZIO_REAL_LLM_CONFIG and AKZIO_REAL_LLM_OUTPUT; calls deployment LLM routes"]
async fn real_news_uses_deployment_routes_and_verifies_sources() -> Result<()> {
    // 该 ignored 测试只在显式提供部署配置和新输出目录时运行；它验证真实 NewsWeb
    // adapter 的路由、模型审查和证据质量，不创建 Store Run，也不进入 Decision/Execution。
    let config_path = PathBuf::from(
        std::env::var("AKZIO_REAL_LLM_CONFIG").context("set the deployment config path")?,
    );
    let output = PathBuf::from(
        std::env::var("AKZIO_REAL_LLM_OUTPUT").context("set a new workspace output directory")?,
    );
    fs::create_dir(&output)?;
    let mut config = read_config_file(&config_path)?;
    // Explicit local-test correction for a deployment file with transposed
    // endpoint/key fields. Never changes that file or silently falls back.
    let swapped = std::env::var("AKZIO_REAL_LLM_SWAP_ENDPOINT_FIELDS").as_deref() == Ok("1");
    if swapped {
        // 仅在内存中修正专门测试开关指定的字段顺序；部署文件本身不被改写，
        // 未启用开关时不会静默猜测或回退 endpoint/key。
        let model = config
            .model
            .as_mut()
            .context("deployment model is required")?;
        std::mem::swap(&mut model.base_url, &mut model.api_key);
    }
    resolve_model_configuration(&mut config)?;
    let model = config
        .model
        .as_ref()
        .context("deployment model is required")?;
    let discovery = model
        .routes
        .get("evidence.news_web")
        .map(|r| model.for_route(r))
        .unwrap_or_else(|| model.clone());
    let reviewer = model
        .routes
        .get("research.critic")
        .map(|r| model.for_route(r))
        .unwrap_or_else(|| model.clone());
    // identity.json 记录实际选择的 discovery/reviewer 路由和配置文件哈希，便于把
    // 真实模型证据与本次测试边界绑定；broker_calls=0 不是 Paper 订单证明，而是作用域声明。
    fs::write(
        output.join("identity.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "config_path":config_path,"config_hash":akzio_domain::ContentHash::of_bytes(&fs::read(&config_path)?),
            "endpoint_key_fields_swapped_in_memory":swapped,
            "discovery":{"model":discovery.model,"reasoning_effort":discovery.reasoning_effort},
            "reviewer":{"model":reviewer.model,"reasoning_effort":reviewer.reasoning_effort},
            "broker_calls":0,"scope":"production news router and source review; no Store or execution"
        }))?,
    )?;
    let adapter = akzio_ingest::configured_news_evidence_transport(model)?;
    let end = chrono::Utc::now().date_naive();
    let start = end - ChronoDuration::days(7);
    let need = akzio_domain::EvidenceNeed {
        schema_version: akzio_domain::DOMAIN_SCHEMA_VERSION,
        source_family: "news_web".into(),
        resource: format!("news:QQQ:{start}:{end}:market"),
        max_age_secs: 604800,
    };
    let request = EvidenceRequest {
        source: EvidenceSource::NewsWeb,
        resource: need.resource.clone(),
        max_age: ChronoDuration::days(7),
        acquisition_mode: akzio_domain::evidence_acquisition_mode(RunPurpose::PositionPlan, &need),
    };
    let result = adapter.acquire(&request).await;
    if let Err(error) = &result {
        // Provider 失败先落 failure.json，再由 ? 传播错误；失败现场保留但不伪造可用证据。
        fs::write(
            output.join("failure.json"),
            serde_json::to_vec_pretty(
                &serde_json::json!({"stage":"acquire","error":error.to_string()}),
            )?,
        )?;
    }
    let acquired = result?;
    // 原始响应、规范化 payload 和质量报告分别保存，后续断言同时覆盖来源审查状态、
    // 引用完整性、时间基准和“未做人审”的事实，HTTP 成功本身不足以通过测试。
    fs::write(output.join("raw.ndjson"), &acquired.raw)?;
    fs::write(
        output.join("normalized.json"),
        serde_json::to_vec_pretty(&acquired.normalized)?,
    )?;
    fs::write(
        output.join("quality.json"),
        serde_json::to_vec_pretty(&acquired.quality)?,
    )?;
    assert_eq!(
        request.acquisition_mode,
        EvidenceAcquisitionMode::ModelReviewed
    );
    assert_eq!(
        acquired
            .normalized
            .pointer("/source_review/reviewer/model")
            .and_then(Value::as_str),
        Some(reviewer.model.as_str())
    );
    assert_eq!(
        acquired
            .normalized
            .pointer("/source_review/status")
            .and_then(Value::as_str),
        Some("model_reviewed"),
        "inspect normalized.json and raw.ndjson; an HTTP success is not enough"
    );
    assert!(acquired.quality.citations_complete);
    assert!(!acquired.normalized["reviewed_facts"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(
        acquired.normalized["source_review"]["human_review"],
        "not_performed"
    );
    assert_eq!(acquired.normalized["source_document"]["fetch_count"], 0);
    let time_basis = akzio_ingest::EvidenceRuntime::validate_acquired_evidence(
        &request,
        &acquired,
        chrono::Utc::now(),
    )?;
    // time_basis 只证明这次 Evidence payload 满足获取时效/来源校验；它仍不是授权的
    // DecisionProposal、Decision 或 ExecutionVerdict。
    fs::write(
        output.join("validated-time-basis.json"),
        serde_json::to_vec_pretty(&time_basis)?,
    )?;
    println!(
        "real news source review passed; evidence: {}",
        output.display()
    );
    Ok(())
}
