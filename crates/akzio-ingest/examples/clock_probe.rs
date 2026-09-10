//! Read-only diagnosis through the production Paper evidence adapter.
use akzio_ingest::{
    AlpacaMarketDataFeed, AlpacaPaperEvidenceTransport, AsyncEvidenceAdapter,
    EvidenceAcquisitionMode, EvidenceRequest, EvidenceSource,
};
use chrono::{Duration, Utc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let adapter = AlpacaPaperEvidenceTransport::from_env(Some(AlpacaMarketDataFeed::Iex))?;
    let started = Utc::now();
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
    println!(
        "{}",
        serde_json::json!({"started_at": started, "received_at": acquired.observed_at,
        "finished_at": Utc::now(), "provider_timestamp": acquired.normalized.get("timestamp"),
        "is_open": acquired.normalized.get("is_open"), "temporal_validation": validation})
    );
    Ok(())
}
