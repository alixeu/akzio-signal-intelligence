//! Read-only adapter capability probe. Never creates Runs, permits or artifacts.
//! `ACQUIRED` means adapter returned bytes; inspect per-item validation separately.
// 文件导读：该示例从正式 paper_session_evidence_needs 取有界需求，按 source_family 选择
// Alpaca/FRED/官方直连 adapter，逐项输出 acquisition 与只读校验观察。缺 adapter 的来源
// 标为 NOT_PROBED。即使输出 ACQUIRED，也须另看 validation 是否为 Ok；
// 这不是 EvidenceGate 接受，且不写 Store 或启动业务 Run。
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
    // 每一项 EvidenceNeed 按 source_family 选择直接来源；&dyn trait 是借用的
    // trait object，让不同具体适配器共享 acquire_at 调用。news_web 在此仅接
    // official adapter，近期新闻和 earnings calendar 会得到 NotConfigured，
    // 不能据此断言正式 native-web 路径不可用。
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
                // 只读校验确认来源、时间和形状，Err 也作为 validation 字段写入
                // ACQUIRED 观察；materialization 字段说明本示例没有 Store 写入。
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
    // 错误枚举转成诊断对象；多数传输/策略分支只显示固定文案，但
    // DataQuality 和特定 Transport 分支会原样显示 message，输出前仍须审查
    // 运行环境及日志可见范围。retryable 只是本例的适配器层分类。
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
