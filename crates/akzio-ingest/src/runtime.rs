//! Rust-owned, allowlisted Evidence Runtime for the rebuilt v2 path.
//!
//! Adapters acquire bytes; agents only receive immutable artifacts. The
//! enclosing `TaskRuntime` commits a completed task attempt through `V2Store`.

use std::collections::{BTreeMap, BTreeSet};
use std::env;

use akzio_domain::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactProvenance, ArtifactRef, Asset, ContentHash,
    DomainError, EvidenceAcquisitionMode, EvidenceNeed, TaskWritePermit, V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_model::{ModelClient, ModelInput, ModelRequest, ModelToolChoice, NativeWebPolicy};
use akzio_store::v2::{StoreError, V2Store};
use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc, Weekday};
use futures::future::BoxFuture;
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[cfg(test)]
use akzio_domain::{ArtifactOrigin, BlobRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceSource {
    Alpaca,
    SecEdgar,
    Fred,
    NewsWeb,
}

impl EvidenceSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Alpaca => "alpaca",
            Self::SecEdgar => "sec_edgar",
            Self::Fred => "fred",
            Self::NewsWeb => "news_web",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceRequest {
    pub source: EvidenceSource,
    pub resource: String,
    pub max_age: Duration,
    /// Rust-owned acquisition policy for this request. Adapters may read it but
    /// never widen it, and no model output participates in choosing it.
    pub acquisition_mode: EvidenceAcquisitionMode,
}

impl EvidenceRequest {
    fn validate(&self) -> Result<(), EvidenceRuntimeError> {
        if self.resource.trim().is_empty()
            || self.resource.chars().count() > 2_048
            || self.max_age <= Duration::zero()
            || self.max_age > Duration::days(7)
        {
            return Err(EvidenceRuntimeError::InvalidRequest);
        }
        GovernedResource::parse(self.source, &self.resource)?;
        Ok(())
    }
}

/// Finite, Rust-owned resource vocabulary. The persisted `EvidenceNeed`
/// remains a canonical string, but every adapter request is parsed into one
/// of these bounded forms before transport.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GovernedResource {
    AlpacaAccount,
    AlpacaPositions,
    AlpacaOpenOrders,
    AlpacaFills {
        session: NaiveDate,
    },
    AlpacaClock,
    AlpacaQuotes,
    AlpacaQuote {
        asset: Asset,
    },
    AlpacaBars {
        asset: Asset,
        start: Option<NaiveDate>,
        limit: u16,
    },
    SecSubmissions {
        cik: String,
    },
    SecCompanyFacts {
        cik: String,
    },
    SecFiling {
        cik: String,
        accession: String,
        primary_document: String,
    },
    Fred {
        series_id: String,
        window_start: Option<NaiveDate>,
        window_end: Option<NaiveDate>,
    },
    FredVintages {
        series_id: String,
        window_start: Option<NaiveDate>,
        window_end: Option<NaiveDate>,
    },
    NewsWeb {
        query: String,
    },
    LegacyFixture {
        source: EvidenceSource,
        resource: String,
    },
}

impl GovernedResource {
    pub fn parse(source: EvidenceSource, resource: &str) -> Result<Self, EvidenceRuntimeError> {
        let resource = resource.trim();
        if resource.is_empty() || resource.chars().count() > 2_048 {
            return Err(EvidenceRuntimeError::InvalidRequest);
        }
        match source {
            EvidenceSource::Alpaca => Self::parse_alpaca(resource),
            EvidenceSource::SecEdgar => Self::parse_sec(resource),
            EvidenceSource::Fred => Self::parse_fred(resource),
            EvidenceSource::NewsWeb => {
                let query = resource
                    .strip_prefix("news:")
                    .or_else(|| resource.strip_prefix("query:"))
                    .unwrap_or(resource)
                    .trim();
                if query.is_empty() || query.chars().count() > 2_000 {
                    return Err(EvidenceRuntimeError::InvalidRequest);
                }
                Ok(Self::NewsWeb {
                    query: query.to_owned(),
                })
            }
        }
    }

    fn parse_alpaca(resource: &str) -> Result<Self, EvidenceRuntimeError> {
        match resource {
            "paper.account" => return Ok(Self::AlpacaAccount),
            "paper.positions" => return Ok(Self::AlpacaPositions),
            "paper.open_orders" => return Ok(Self::AlpacaOpenOrders),
            "paper.clock" => return Ok(Self::AlpacaClock),
            "paper.quotes" => return Ok(Self::AlpacaQuotes),
            value if value.starts_with("paper.fills:") => {
                return Ok(Self::AlpacaFills {
                    session: NaiveDate::parse_from_str(
                        value.trim_start_matches("paper.fills:"),
                        "%Y-%m-%d",
                    )
                    .map_err(|_| EvidenceRuntimeError::InvalidRequest)?,
                });
            }
            "quote" | "bars" => {
                return Ok(Self::LegacyFixture {
                    source: EvidenceSource::Alpaca,
                    resource: resource.to_owned(),
                })
            }
            _ => {}
        }
        let parts = resource.split(':').collect::<Vec<_>>();
        match parts.as_slice() {
            ["quote", symbol] => Ok(Self::AlpacaQuote {
                asset: Asset::try_from(*symbol)
                    .map_err(|_| EvidenceRuntimeError::InvalidRequest)?,
            }),
            ["bars", symbol, timeframe] if *timeframe == "1d" => Ok(Self::AlpacaBars {
                asset: Asset::try_from(*symbol)
                    .map_err(|_| EvidenceRuntimeError::InvalidRequest)?,
                start: None,
                limit: 1,
            }),
            ["bars", symbol, timeframe, start] if *timeframe == "1d" => Ok(Self::AlpacaBars {
                asset: Asset::try_from(*symbol)
                    .map_err(|_| EvidenceRuntimeError::InvalidRequest)?,
                start: Some(
                    NaiveDate::parse_from_str(start, "%Y-%m-%d")
                        .map_err(|_| EvidenceRuntimeError::InvalidRequest)?,
                ),
                limit: 1,
            }),
            ["bars", symbol, timeframe, start, limit] if *timeframe == "1d" => {
                let limit = limit
                    .parse::<u16>()
                    .map_err(|_| EvidenceRuntimeError::InvalidRequest)?;
                if !(1..=252).contains(&limit) {
                    return Err(EvidenceRuntimeError::InvalidRequest);
                }
                Ok(Self::AlpacaBars {
                    asset: Asset::try_from(*symbol)
                        .map_err(|_| EvidenceRuntimeError::InvalidRequest)?,
                    start: Some(
                        NaiveDate::parse_from_str(start, "%Y-%m-%d")
                            .map_err(|_| EvidenceRuntimeError::InvalidRequest)?,
                    ),
                    limit,
                })
            }
            _ => Err(EvidenceRuntimeError::InvalidRequest),
        }
    }

    fn parse_fred(resource: &str) -> Result<Self, EvidenceRuntimeError> {
        let parts = resource.split(':').collect::<Vec<_>>();
        if !(2..=4).contains(&parts.len()) || !matches!(parts[0], "series" | "vintages") {
            return Err(EvidenceRuntimeError::InvalidRequest);
        }
        let series_id = parts[1];
        if series_id.is_empty()
            || series_id.chars().count() > 64
            || !series_id
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || "._-".contains(character))
        {
            return Err(EvidenceRuntimeError::InvalidRequest);
        }
        let window_start = parts
            .get(2)
            .map(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d"))
            .transpose()
            .map_err(|_| EvidenceRuntimeError::InvalidRequest)?;
        let window_end = parts
            .get(3)
            .map(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d"))
            .transpose()
            .map_err(|_| EvidenceRuntimeError::InvalidRequest)?;
        if let (Some(start), Some(end)) = (window_start, window_end) {
            if end < start || end.signed_duration_since(start) > Duration::days(366) {
                return Err(EvidenceRuntimeError::InvalidRequest);
            }
        }
        if parts[0] == "series" {
            Ok(Self::Fred {
                series_id: series_id.to_owned(),
                window_start,
                window_end,
            })
        } else {
            Ok(Self::FredVintages {
                series_id: series_id.to_owned(),
                window_start,
                window_end,
            })
        }
    }

    fn parse_sec(resource: &str) -> Result<Self, EvidenceRuntimeError> {
        let parts = resource.split(':').collect::<Vec<_>>();
        match parts.as_slice() {
            ["sec" | "submissions", cik] => Ok(Self::SecSubmissions {
                cik: normalized_cik(cik)?,
            }),
            ["companyfacts", cik] => Ok(Self::SecCompanyFacts {
                cik: normalized_cik(cik)?,
            }),
            ["filing", cik, accession, primary_document]
                if valid_accession(accession) && valid_primary_document(primary_document) =>
            {
                Ok(Self::SecFiling {
                    cik: normalized_cik(cik)?,
                    accession: (*accession).to_owned(),
                    primary_document: (*primary_document).to_owned(),
                })
            }
            _ => Err(EvidenceRuntimeError::InvalidRequest),
        }
    }
}

fn normalized_cik(value: &str) -> Result<String, EvidenceRuntimeError> {
    let digits = value.strip_prefix("CIK").unwrap_or(value);
    if digits.is_empty() || digits.len() > 10 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(EvidenceRuntimeError::InvalidRequest);
    }
    let number = digits
        .parse::<u64>()
        .map_err(|_| EvidenceRuntimeError::InvalidRequest)?;
    if number == 0 {
        return Err(EvidenceRuntimeError::InvalidRequest);
    }
    Ok(format!("{number:010}"))
}

fn valid_accession(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 20
        && bytes[10] == b'-'
        && bytes[13] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| matches!(index, 10 | 13) || byte.is_ascii_digit())
}

fn valid_primary_document(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && !value.starts_with('.')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// Strict OHLCV quality gate used by the production Alpaca adapter. Fixture
/// payloads may still use a minimal close-only shape, but provider data must
/// carry a timestamped, positive and internally consistent daily bar.
pub fn validate_daily_bar_payload(value: &Value) -> Result<(), EvidenceRuntimeError> {
    let bars = value
        .get("bars")
        .and_then(Value::as_array)
        .filter(|bars| !bars.is_empty())
        .ok_or(EvidenceRuntimeError::InvalidAcquisition)?;
    let mut dates = BTreeSet::new();
    for bar in bars {
        let timestamp = bar
            .get("t")
            .or_else(|| bar.get("timestamp"))
            .and_then(Value::as_str)
            .ok_or(EvidenceRuntimeError::InvalidAcquisition)?;
        let date = DateTime::parse_from_rfc3339(timestamp)
            .map_err(|_| EvidenceRuntimeError::InvalidAcquisition)?
            .date_naive();
        if matches!(date.weekday(), Weekday::Sat | Weekday::Sun) || !dates.insert(date) {
            return Err(EvidenceRuntimeError::InvalidAcquisition);
        }
        let open = positive_market_number(bar.get("o"))?;
        let high = positive_market_number(bar.get("h"))?;
        let low = positive_market_number(bar.get("l"))?;
        let close = positive_market_number(bar.get("c"))?;
        let volume = positive_market_number(bar.get("v"))?;
        if high < open.max(close) || low > open.min(close) || volume <= 0.0 {
            return Err(EvidenceRuntimeError::InvalidAcquisition);
        }
    }
    Ok(())
}

fn positive_market_number(value: Option<&Value>) -> Result<f64, EvidenceRuntimeError> {
    let value = value.ok_or(EvidenceRuntimeError::InvalidAcquisition)?;
    let number = value
        .as_f64()
        .or_else(|| value.as_str().and_then(|value| value.parse::<f64>().ok()))
        .filter(|value| value.is_finite() && *value > 0.0)
        .ok_or(EvidenceRuntimeError::InvalidAcquisition)?;
    Ok(number)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvidenceCitation {
    pub start_byte: usize,
    pub end_byte: usize,
    pub quote: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvidenceProvenance {
    pub document_id: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
    pub observed_at: DateTime<Utc>,
    pub revision: Option<String>,
    pub source_uri: String,
    pub dedupe_key: String,
    pub citations: Vec<EvidenceCitation>,
}

impl EvidenceProvenance {
    fn validate(
        &self,
        raw: &[u8],
        source_uri: &str,
        observed_at: DateTime<Utc>,
    ) -> Result<(), EvidenceRuntimeError> {
        if self.source_uri != source_uri
            || self.observed_at != observed_at
            || self.dedupe_key.trim().is_empty()
        {
            return Err(EvidenceRuntimeError::InvalidProvenance);
        }
        if self
            .document_id
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
            || self
                .revision
                .as_deref()
                .is_some_and(|value| value.trim().is_empty())
        {
            return Err(EvidenceRuntimeError::InvalidProvenance);
        }
        for citation in &self.citations {
            if citation.start_byte >= citation.end_byte
                || raw.get(citation.start_byte..citation.end_byte)
                    != Some(citation.quote.as_bytes())
                || citation.quote.trim().is_empty()
            {
                return Err(EvidenceRuntimeError::InvalidCitation);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvidenceQuality {
    pub completeness_ppm: u32,
    pub citations_complete: bool,
    pub normalized: bool,
}

impl Default for EvidenceQuality {
    fn default() -> Self {
        Self {
            completeness_ppm: 1_000_000,
            citations_complete: true,
            normalized: true,
        }
    }
}

impl EvidenceQuality {
    fn validate(&self) -> Result<(), EvidenceRuntimeError> {
        if self.completeness_ppm > 1_000_000 || !self.normalized {
            return Err(EvidenceRuntimeError::InvalidQuality);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AcquiredEvidence {
    pub raw: Vec<u8>,
    pub media_type: String,
    pub source_uri: String,
    pub observed_at: DateTime<Utc>,
    pub normalized: Value,
    pub provenance: EvidenceProvenance,
    pub quality: EvidenceQuality,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NormalizedEvidencePayload {
    pub schema_version: u32,
    pub source: EvidenceSource,
    pub resource: String,
    pub need: ArtifactRef,
    pub raw: ArtifactRef,
    pub observed_at: DateTime<Utc>,
    pub value: Value,
    pub provenance: EvidenceProvenance,
    pub quality: EvidenceQuality,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EvidenceBundle {
    pub raw: Artifact,
    pub normalized: Artifact,
}

/// Read one byte offset out of a persisted claim binding.
pub(crate) fn claim_binding_byte(binding: &Value, field: &str) -> Option<usize> {
    binding
        .get(field)
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
}

/// Governed URI rules shared by provider citations and materialized evidence.
///
/// Provider citations are checked against this before any independent HTTPS
/// request, so a credential-bearing or fragment-carrying URL never reaches the
/// network; the materialization path re-checks the sealed `source_uri`.
pub(crate) fn governed_source_uri_is_safe(source_uri: &str) -> bool {
    let Ok(parsed) = Url::parse(source_uri) else {
        return false;
    };
    parsed.username().is_empty()
        && parsed.password().is_none()
        && parsed.fragment().is_none()
        && !parsed.query_pairs().any(|(key, _)| {
            let key = key.to_ascii_lowercase();
            key.contains("token")
                || key.contains("secret")
                || key.contains("password")
                || key.contains("api_key")
                || key == "key"
                || key.contains("authorization")
        })
}

#[path = "adapters.rs"]
mod adapters;
pub use adapters::{
    AlpacaMarketDataFeed, AlpacaPaperEvidenceTransport, AsyncEvidenceAdapter, EvidenceAdapter,
    EvidenceAdapterError, FixtureEvidenceAdapter,
};

pub fn model_native_web_evidence_transport(
    client: ModelClient,
    source: EvidenceSource,
) -> EvidenceRuntimeResult<std::sync::Arc<dyn AsyncEvidenceAdapter>> {
    Ok(std::sync::Arc::new(
        adapters::ModelNativeWebEvidenceTransport::for_source(client, source)?,
    ))
}

#[cfg(test)]
fn model_native_web_evidence_transport_with_fetcher(
    client: ModelClient,
    source: EvidenceSource,
    fetcher: std::sync::Arc<dyn adapters::SourceDocumentFetcher>,
) -> std::sync::Arc<dyn AsyncEvidenceAdapter> {
    std::sync::Arc::new(
        adapters::ModelNativeWebEvidenceTransport::for_source_with_fetcher(client, source, fetcher),
    )
}

#[cfg(test)]
fn model_native_web_evidence_transport_without_fetcher(
    client: ModelClient,
    source: EvidenceSource,
) -> std::sync::Arc<dyn AsyncEvidenceAdapter> {
    std::sync::Arc::new(
        adapters::ModelNativeWebEvidenceTransport::for_source_without_fetcher(client, source),
    )
}
#[derive(Debug, Error)]
pub enum EvidenceRuntimeError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Adapter(#[from] EvidenceAdapterError),
    #[error("evidence source {0:?} is not allowlisted")]
    SourceNotAllowed(EvidenceSource),
    #[error("evidence request is invalid")]
    InvalidRequest,
    #[error("evidence request does not reference a committed EvidenceNeed in this run")]
    InvalidEvidenceNeed,
    #[error("acquired evidence is stale")]
    StaleEvidence,
    #[error("acquired evidence is empty or lacks a media type")]
    InvalidAcquisition,
    #[error("acquired evidence source URI is invalid or contains credentials")]
    UnsafeSourceUri,
    #[error("acquired evidence provenance is invalid")]
    InvalidProvenance,
    #[error("acquired evidence citation is invalid")]
    InvalidCitation,
    #[error("acquired evidence quality is invalid")]
    InvalidQuality,
    #[error("semantic detail must cite normalized evidence")]
    DetailRequiresNormalizedEvidence,
}

pub type EvidenceRuntimeResult<T> = Result<T, EvidenceRuntimeError>;

#[derive(Debug, Clone)]
pub struct EvidenceRuntime {
    store: V2Store,
    allowed_sources: BTreeSet<EvidenceSource>,
}
include!("runtime_parts/materialize_raw.rs");
include!("runtime_parts/materialize_normalized.rs");
#[cfg(test)]
#[path = "tests.rs"]
mod tests;
