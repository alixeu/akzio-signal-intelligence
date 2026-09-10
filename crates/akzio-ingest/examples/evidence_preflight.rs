//! Read-only adapter capability probe. Never creates Runs, permits or artifacts.
//! Success means acquisition/adapter validation only, not EvidenceGate acceptance.
use akzio_domain::{paper_session_evidence_needs, ContentHash};
use akzio_ingest::{
    AlpacaMarketDataFeed, AlpacaPaperEvidenceTransport, AsyncEvidenceAdapter,
    EvidenceAcquisitionMode, EvidenceRequest, EvidenceSource, FredDirectTransport,
};
use chrono::{Duration, Utc};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = std::env::args()
        .nth(1)
        .ok_or("usage: evidence_preflight YYYY-MM-DD")?;
    chrono::NaiveDate::parse_from_str(&session, "%Y-%m-%d")?;
    let alpaca = AlpacaPaperEvidenceTransport::from_env(Some(AlpacaMarketDataFeed::Iex))?;
    let fred = FredDirectTransport::from_env()?;
    let prefix = std::env::args().nth(2).unwrap_or_default();
    for need in paper_session_evidence_needs(&session)
        .into_iter()
        .filter(|n| n.resource.starts_with(&prefix))
    {
        let criticality = need.criticality();
        let (source, adapter): (_, &dyn AsyncEvidenceAdapter) = match need.source_family.as_str() {
            "alpaca" => (EvidenceSource::Alpaca, &alpaca),
            "fred" => (EvidenceSource::Fred, &fred),
            _ => {
                println!(
                    "{}",
                    json!({"resource":need.resource,"provider":need.source_family,
                    "criticality":criticality,"state":"NOT_PROBED",
                    "reason":"production registry requires verified native-web capability; this probe does not grant it"})
                );
                continue;
            }
        };
        let started = Utc::now();
        let result = adapter
            .acquire_at(
                &EvidenceRequest {
                    source,
                    resource: need.resource.clone(),
                    max_age: Duration::seconds(i64::try_from(need.max_age_secs)?),
                    acquisition_mode: EvidenceAcquisitionMode::VerifiedSource,
                },
                started,
            )
            .await;
        let observation = match result {
            Ok(value) => {
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
                let validation = akzio_ingest::EvidenceRuntime::validate_acquired_evidence(
                    &EvidenceRequest {
                        source,
                        resource: need.resource.clone(),
                        max_age: Duration::seconds(i64::try_from(need.max_age_secs)?),
                        acquisition_mode: EvidenceAcquisitionMode::VerifiedSource,
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
                "quote_validation":quote_validation,
                "quotes":if need.resource == "paper.quotes" { value.normalized.get("quotes") } else { None },
                "validation": validation, "materialization":"NOT_RUN: no Store writes"})
            }
            Err(error) => json!({"state":"UNAVAILABLE","error":error_observation(&error),
                "normalization":"NOT_RUN"}),
        };
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
