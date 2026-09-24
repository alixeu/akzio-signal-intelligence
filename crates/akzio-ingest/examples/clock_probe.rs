//! Read-only diagnosis through the production Paper evidence adapter.
use akzio_ingest::{
    AlpacaMarketDataFeed, AlpacaPaperEvidenceTransport, AsyncEvidenceAdapter,
    EvidenceAcquisitionMode, EvidenceRequest, EvidenceSource,
};
use chrono::{Duration, Utc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 该示例不接收位置参数；凭据、Paper 地址和其他连接配置由环境变量读取，行情源显式固定为 IEX。
    let adapter = AlpacaPaperEvidenceTransport::from_env(Some(AlpacaMarketDataFeed::Iex))?;
    // started 同时作为本次请求的观察起点和 cutoff，输出中的时间字段用于区分采集耗时与提供方时间。
    let started = Utc::now();
    // AsyncEvidenceAdapter 的 acquire_at 返回异步 Result；await 等待适配器完成，? 将配置或采集错误交给 main。
    let acquired = adapter
        .acquire_at(
            &EvidenceRequest {
                source: EvidenceSource::Alpaca,
                resource: "paper.clock".into(),
                max_age: Duration::seconds(60),
                acquisition_mode: EvidenceAcquisitionMode::VerifiedSource,
            },
            started,
        )
        .await?;
    // 这里重新用同一资源和年龄约束校验已采集结果；校验只读取证据，不创建 Run、Store 或其他持久化对象。
    let validation = akzio_ingest::EvidenceRuntime::validate_acquired_evidence(
        &EvidenceRequest {
            source: EvidenceSource::Alpaca,
            resource: "paper.clock".into(),
            max_age: Duration::seconds(60),
            acquisition_mode: EvidenceAcquisitionMode::VerifiedSource,
        },
        &acquired,
        Utc::now(),
    )
    .map_err(|e| e.to_string());
    // stdout 输出一条 JSON：既包含调用起止时间和 provider timestamp，也保留 is_open 与时间校验结果供人工诊断。
    println!(
        "{}",
        serde_json::json!({"started_at": started, "received_at": acquired.observed_at,
        "finished_at": Utc::now(), "provider_timestamp": acquired.normalized.get("timestamp"),
        "is_open": acquired.normalized.get("is_open"), "temporal_validation": validation})
    );
    Ok(())
}
