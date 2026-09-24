//! Read-only adapter capability probe. Never creates Runs, permits or artifacts.
//! Success means acquisition/adapter validation only, not EvidenceGate acceptance.
use akzio_domain::{
    evidence_acquisition_mode, paper_session_evidence_needs, ContentHash, RunPurpose,
};
use akzio_ingest::{
    AlpacaMarketDataFeed, AlpacaPaperEvidenceTransport, AsyncEvidenceAdapter, EvidenceRequest,
    EvidenceSource, FredDirectTransport, OfficialInstrumentEvidenceTransport,
};
use chrono::{Duration, Utc};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 第一个参数是 YYYY-MM-DD 的研究 Session，第二个可选参数只保留匹配 resource 前缀的需求；缺少日期会返回 usage 错误。
    let session = std::env::args()
        .nth(1)
        .ok_or("usage: evidence_preflight YYYY-MM-DD")?;
    // 先验证日期格式，再创建三个只读适配器；? 把配置错误保留为本示例的失败结果。
    chrono::NaiveDate::parse_from_str(&session, "%Y-%m-%d")?;
    let alpaca = AlpacaPaperEvidenceTransport::from_env(Some(AlpacaMarketDataFeed::Iex))?;
    let fred = FredDirectTransport::from_env()?;
    let official = OfficialInstrumentEvidenceTransport::new()?;
    let prefix = std::env::args().nth(2).unwrap_or_default();
    // 每一项 EvidenceNeed 会按 source_family 选择直接来源；&dyn trait 让不同适配器共享同一个异步调用入口。
    for need in paper_session_evidence_needs(&session)
        .into_iter()
        .filter(|n| n.resource.starts_with(&prefix))
    {
        let criticality = need.criticality();
        let (source, adapter): (_, &dyn AsyncEvidenceAdapter) = match need.source_family.as_str() {
            "alpaca" => (EvidenceSource::Alpaca, &alpaca),
            "fred" => (EvidenceSource::Fred, &fred),
            "news_web" => (EvidenceSource::NewsWeb, &official),
            _ => {
                // 没有注册直接适配器的来源只报告 NOT_PROBED，不把未探测误报成不可用或已采集。
                println!(
                    "{}",
                    json!({"resource":need.resource,"provider":need.source_family,
                    "criticality":criticality,"state":"NOT_PROBED",
                    "reason":"no direct preflight adapter is registered for this source family"})
                );
                continue;
            }
        };
        let started = Utc::now();
        // acquisition_mode 由正式 RunPurpose 和 EvidenceNeed 决定，示例只复用该授权选择，不自行放宽权限。
        let acquisition_mode = evidence_acquisition_mode(RunPurpose::PositionPlan, &need);
        // acquire_at 是异步 Result；await 等待网络适配器完成，后面的 match 将成功与各类失败分开输出。
        let result = adapter
            .acquire_at(
                &EvidenceRequest {
                    source,
                    resource: need.resource.clone(),
                    max_age: Duration::seconds(i64::try_from(need.max_age_secs)?),
                    acquisition_mode,
                },
                started,
            )
            .await;
        let observation = match result {
            Ok(value) => {
                // 只有 paper.quotes 需要额外的结构化 Option 校验；其他资源用 None 表示该项不适用。
                let quote_validation = if need.resource == "paper.quotes" {
                    Some(
                        akzio_ingest::decode_paper_quotes(
                            &value.normalized,
                            session.clone(),
                            value.observed_at,
                        )
                        .map_err(|e| e.to_string())
                        .and_then(|q| q.validate().map(|_| "PASS").map_err(|e| e.to_string())),
                    )
                } else {
                    None
                };
                // 运行时校验只确认采集结果的来源、时间和形状；materialization 明确说明本示例没有 Store 写入。
                let validation = akzio_ingest::EvidenceRuntime::validate_acquired_evidence(
                    &EvidenceRequest {
                        source,
                        resource: need.resource.clone(),
                        max_age: Duration::seconds(i64::try_from(need.max_age_secs)?),
                        acquisition_mode,
                    },
                    &value,
                    Utc::now(),
                )
                .map_err(|e| e.to_string());
                json!({"state":"ACQUIRED","retrieved_at":value.observed_at,
                "content_hash":ContentHash::of_bytes(&value.raw),"bytes":value.raw.len(),
                "contracts":value.normalized.get("snapshots").and_then(serde_json::Value::as_object).map(|v|v.len()),
                "next_page_token_present":value.normalized.get("next_page_token").and_then(serde_json::Value::as_str).is_some_and(|s|!s.is_empty()),
                "provider_timestamp":value.normalized.get("timestamp"),
                "source_document":value.normalized.get("source_document"),
                "quote_validation":quote_validation,
                "quotes":if need.resource == "paper.quotes" { value.normalized.get("quotes") } else { None },
                "validation": validation, "materialization":"NOT_RUN: no Store writes"})
            }
            Err(error) => json!({"state":"UNAVAILABLE","error":error_observation(&error),
                "normalization":"NOT_RUN"}),
        };
        // 每个需求输出一条 JSON，包含 provider、关键时间、原始字节摘要和校验观察，便于逐项判断而非汇总猜测。
        println!(
            "{}",
            json!({"resource":need.resource,"provider":need.source_family,
            "criticality":criticality,"started_at":started,"finished_at":Utc::now(),
            "observation":observation})
        );
    }
    Ok(())
}

fn error_observation(error: &akzio_ingest::EvidenceAdapterError) -> serde_json::Value {
    // 将 Result 的错误枚举转换成不泄露凭据和完整 URL 的诊断对象；retryable 只描述适配器层是否允许重试。
    use akzio_ingest::EvidenceAdapterError::*;
    let (class, status, retryable, message) = match error {
        Unauthorized(code) => (
            "authorization",
            Some(*code),
            false,
            "authentication or entitlement denied",
        ),
        Permanent(code) => (
            "permanent_request",
            Some(*code),
            false,
            "provider rejected request",
        ),
        RateLimited { .. } => ("rate_limit", Some(429), true, "provider rate limit"),
        DataQuality(message) => ("data_quality", None, false, message.as_str()),
        Transport(message) if message.starts_with("Alpaca option chain") => {
            ("adapter_validation", None, false, message.as_str())
        }
        Transport(_) => (
            "transport",
            None,
            true,
            "transport failed; raw URL and credentials withheld",
        ),
        Pending(_) => ("pending", None, true, "provider data pending"),
        NotConfigured(_) => (
            "adapter_unavailable",
            None,
            false,
            "no authorized provider is configured",
        ),
        NativeWeb { kind, .. } => (
            kind.as_str(),
            None,
            false,
            "native web search did not produce verified evidence",
        ),
        Policy { .. } => (
            "policy",
            None,
            false,
            "governed source policy rejected acquisition",
        ),
        MissingFixture(_) => (
            "fixture_rejected",
            None,
            false,
            "fixtures are not evidence preflight",
        ),
        SourceMismatch => (
            "source_mismatch",
            None,
            false,
            "adapter does not serve requested source",
        ),
    };
    json!({"class":class,"http_status":status,"retryable":retryable,"message":message})
}
