//! Rust-owned, allowlisted Evidence Runtime for the rebuilt v2 path.
//!
//! Adapters acquire bytes; agents only receive immutable artifacts. The
//! enclosing `TaskRuntime` commits a completed task attempt through `V2Store`.

use std::collections::{BTreeMap, BTreeSet};
use std::env;

use akzio_domain::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance, ArtifactRef,
    Asset, ContentHash, DomainError, EvidenceNeed, TaskWritePermit, V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_model::{ModelClient, ModelRequest, NativeWebPolicy};
use akzio_store::v2::{StoreError, V2Store};
use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc, Weekday};
use futures::future::BoxFuture;
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

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
    AlpacaClock,
    AlpacaQuotes,
    AlpacaQuote {
        asset: Asset,
    },
    AlpacaBars {
        asset: Asset,
        start: Option<NaiveDate>,
        limit: u8,
    },
    SecEdgar {
        locator: String,
    },
    Fred {
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
            EvidenceSource::SecEdgar => {
                let (prefix, locator) = resource
                    .split_once(':')
                    .ok_or(EvidenceRuntimeError::InvalidRequest)?;
                if !matches!(prefix, "sec" | "filing" | "companyfacts")
                    || locator.is_empty()
                    || locator.chars().count() > 256
                    || !locator.chars().all(|character| {
                        character.is_ascii_alphanumeric() || "._-".contains(character)
                    })
                {
                    return Err(EvidenceRuntimeError::InvalidRequest);
                }
                Ok(Self::SecEdgar {
                    locator: locator.to_owned(),
                })
            }
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
            "paper.clock" => return Ok(Self::AlpacaClock),
            "paper.quotes" => return Ok(Self::AlpacaQuotes),
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
                    .parse::<u8>()
                    .map_err(|_| EvidenceRuntimeError::InvalidRequest)?;
                if !(1..=32).contains(&limit) {
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
        if !(2..=4).contains(&parts.len()) || parts[0] != "series" {
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
        Ok(Self::Fred {
            series_id: series_id.to_owned(),
            window_start,
            window_end,
        })
    }
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
        let adjustment = bar
            .get("adjustment")
            .and_then(Value::as_str)
            .ok_or(EvidenceRuntimeError::InvalidAcquisition)?;
        if adjustment != "all" {
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
        raw_len: usize,
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
                || citation.end_byte > raw_len
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

#[derive(Debug, Clone, PartialEq)]
pub struct DetailInput {
    pub normalized: ArtifactRef,
    pub value: Value,
}

#[derive(Debug, Error)]
pub enum EvidenceAdapterError {
    #[error("fixture for {0} is unavailable")]
    MissingFixture(String),
    #[error("adapter source does not match request")]
    SourceMismatch,
    #[error("governed evidence transport failed: {0}")]
    Transport(String),
}

pub trait EvidenceAdapter: Send + Sync {
    fn source(&self) -> EvidenceSource;

    fn acquire(&self, request: &EvidenceRequest) -> Result<AcquiredEvidence, EvidenceAdapterError>;
}

pub trait AsyncEvidenceAdapter: Send + Sync {
    fn source(&self) -> EvidenceSource;

    fn acquire<'a>(
        &'a self,
        request: &'a EvidenceRequest,
    ) -> BoxFuture<'a, Result<AcquiredEvidence, EvidenceAdapterError>>;
}

/// Rust-injected transport for governed evidence adapters. It accepts a
/// source enum and resource instead of an arbitrary URL, so model code never
/// gets a route to network access.
pub trait GovernedEvidenceTransport: Send + Sync {
    fn acquire(
        &self,
        source: EvidenceSource,
        resource: &str,
    ) -> Result<AcquiredEvidence, EvidenceAdapterError>;
}

pub trait AsyncGovernedEvidenceTransport: Send + Sync {
    fn acquire<'a>(
        &'a self,
        source: EvidenceSource,
        resource: &'a str,
    ) -> BoxFuture<'a, Result<AcquiredEvidence, EvidenceAdapterError>>;
}

/// Rust-owned Alpaca Paper market-data transport. The resource language is
/// deliberately finite; callers cannot pass an arbitrary URL or endpoint.
#[derive(Clone)]
pub struct AlpacaPaperEvidenceTransport {
    client: Client,
    base_url: String,
    key_id: String,
    secret_key: String,
}

impl std::fmt::Debug for AlpacaPaperEvidenceTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlpacaPaperEvidenceTransport")
            .field("base_url", &self.base_url)
            .field("key_id", &"<redacted>")
            .field("secret_key", &"<redacted>")
            .finish()
    }
}

impl AlpacaPaperEvidenceTransport {
    pub fn from_env() -> Result<Self, EvidenceAdapterError> {
        let base_url = env::var("ALPACA_PAPER_BASE_URL")
            .unwrap_or_else(|_| "https://paper-api.alpaca.markets".to_owned());
        let key_id = env::var("ALPACA_API_KEY")
            .map_err(|_| EvidenceAdapterError::Transport("ALPACA_API_KEY is not set".to_owned()))?;
        let secret_key = env::var("ALPACA_API_SECRET").map_err(|_| {
            EvidenceAdapterError::Transport("ALPACA_API_SECRET is not set".to_owned())
        })?;
        Self::new(base_url, key_id, secret_key)
    }

    pub fn new(
        base_url: impl Into<String>,
        key_id: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Result<Self, EvidenceAdapterError> {
        let supplied = base_url.into();
        let parsed = Url::parse(supplied.trim())
            .map_err(|_| EvidenceAdapterError::Transport("non-Paper Alpaca endpoint".to_owned()))?;
        if parsed.scheme() != "https"
            || parsed.host_str() != Some("paper-api.alpaca.markets")
            || parsed.port().is_some()
            || parsed.username() != ""
            || parsed.password().is_some()
            || parsed.path() != "/"
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(EvidenceAdapterError::Transport(
                "non-Paper Alpaca endpoint".to_owned(),
            ));
        }
        let key_id = key_id.into();
        let secret_key = secret_key.into();
        if key_id.trim().is_empty() || secret_key.trim().is_empty() {
            return Err(EvidenceAdapterError::Transport(
                "Alpaca credentials are empty".to_owned(),
            ));
        }
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|error| EvidenceAdapterError::Transport(error.to_string()))?;
        Ok(Self {
            client,
            base_url: "https://paper-api.alpaca.markets".to_owned(),
            key_id,
            secret_key,
        })
    }

    fn path_for(resource: &str) -> Result<String, EvidenceAdapterError> {
        match resource {
            "paper.account" => Ok("/v2/account".to_owned()),
            "paper.clock" => Ok("/v2/clock".to_owned()),
            "paper.quotes" => Ok("/v2/stocks/quotes/latest?symbols=TQQQ,QQQ,SOXX,SOXL".to_owned()),
            value if value.starts_with("quote:") => {
                let asset = value.strip_prefix("quote:").unwrap_or_default();
                let asset = Asset::try_from(asset).map_err(|_| {
                    EvidenceAdapterError::Transport("asset is outside the v2 universe".to_owned())
                })?;
                Ok(format!("/v2/stocks/{}/quotes/latest", asset.symbol()))
            }
            value if value.starts_with("bars:") => {
                let mut parts = value.split(':');
                let _ = parts.next();
                let asset = parts.next().ok_or_else(|| {
                    EvidenceAdapterError::Transport("invalid Alpaca bars resource".to_owned())
                })?;
                let timeframe = parts.next().ok_or_else(|| {
                    EvidenceAdapterError::Transport("invalid Alpaca bars resource".to_owned())
                })?;
                if timeframe != "1d" {
                    return Err(EvidenceAdapterError::Transport(
                        "only one-day bars are allowed".to_owned(),
                    ));
                }
                let asset = Asset::try_from(asset).map_err(|_| {
                    EvidenceAdapterError::Transport("asset is outside the v2 universe".to_owned())
                })?;
                let start = parts.next();
                let limit = parts.next().unwrap_or("1");
                if parts.next().is_some() {
                    return Err(EvidenceAdapterError::Transport(
                        "invalid Alpaca bars resource".to_owned(),
                    ));
                }
                let limit = limit.parse::<u8>().map_err(|_| {
                    EvidenceAdapterError::Transport("invalid Alpaca bars limit".to_owned())
                })?;
                if !(1..=32).contains(&limit) {
                    return Err(EvidenceAdapterError::Transport(
                        "Alpaca bars limit outside 1..=32".to_owned(),
                    ));
                }
                if let Some(start) = start {
                    chrono::NaiveDate::parse_from_str(start, "%Y-%m-%d").map_err(|_| {
                        EvidenceAdapterError::Transport("invalid Alpaca bars start date".to_owned())
                    })?;
                }
                let mut path = format!(
                    "/v2/stocks/{}/bars?timeframe=1Day&limit={limit}&adjustment=all",
                    asset.symbol()
                );
                if let Some(start) = start {
                    path.push_str("&start=");
                    path.push_str(start);
                }
                Ok(path)
            }
            _ => Err(EvidenceAdapterError::Transport(
                "Alpaca resource is not allowlisted".to_owned(),
            )),
        }
    }

    async fn acquire_inner(
        &self,
        source: EvidenceSource,
        resource: &str,
    ) -> Result<AcquiredEvidence, EvidenceAdapterError> {
        if source != EvidenceSource::Alpaca {
            return Err(EvidenceAdapterError::SourceMismatch);
        }
        let path = Self::path_for(resource)?;
        let url = format!("{}{}", self.base_url, path);
        let response = self
            .client
            .get(&url)
            .header("APCA-API-KEY-ID", &self.key_id)
            .header("APCA-API-SECRET-KEY", &self.secret_key)
            .send()
            .await
            .map_err(|error| EvidenceAdapterError::Transport(error.to_string()))?;
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|error| EvidenceAdapterError::Transport(error.to_string()))?;
        if !status.is_success() {
            return Err(EvidenceAdapterError::Transport(format!(
                "Alpaca returned HTTP {}",
                status.as_u16()
            )));
        }
        let normalized: Value = serde_json::from_slice(&body)
            .map_err(|error| EvidenceAdapterError::Transport(error.to_string()))?;
        if resource.starts_with("bars:") {
            validate_daily_bar_payload(&normalized)
                .map_err(|error| EvidenceAdapterError::Transport(error.to_string()))?;
        }
        let observed_at = Utc::now();
        let source_uri = url;
        Ok(AcquiredEvidence {
            raw: body.to_vec(),
            media_type: "application/json".to_owned(),
            source_uri: source_uri.clone(),
            observed_at,
            normalized,
            provenance: EvidenceProvenance {
                document_id: Some(resource.to_owned()),
                published_at: None,
                observed_at,
                revision: None,
                source_uri,
                dedupe_key: format!("alpaca:{}", ContentHash::of_bytes(&body)),
                citations: vec![],
            },
            quality: EvidenceQuality::default(),
        })
    }
}

impl AsyncGovernedEvidenceTransport for AlpacaPaperEvidenceTransport {
    fn acquire<'a>(
        &'a self,
        source: EvidenceSource,
        resource: &'a str,
    ) -> BoxFuture<'a, Result<AcquiredEvidence, EvidenceAdapterError>> {
        Box::pin(self.acquire_inner(source, resource))
    }
}

impl AsyncEvidenceAdapter for AlpacaPaperEvidenceTransport {
    fn source(&self) -> EvidenceSource {
        EvidenceSource::Alpaca
    }

    fn acquire<'a>(
        &'a self,
        request: &'a EvidenceRequest,
    ) -> BoxFuture<'a, Result<AcquiredEvidence, EvidenceAdapterError>> {
        Box::pin(async move {
            if request.source != EvidenceSource::Alpaca {
                return Err(EvidenceAdapterError::SourceMismatch);
            }
            self.acquire_inner(request.source, &request.resource).await
        })
    }
}

#[derive(Clone)]
pub struct ModelNativeWebEvidenceTransport {
    client: ModelClient,
    policy: NativeWebPolicy,
    source: EvidenceSource,
}

impl std::fmt::Debug for ModelNativeWebEvidenceTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ModelNativeWebEvidenceTransport")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl ModelNativeWebEvidenceTransport {
    pub fn new(client: ModelClient, policy: NativeWebPolicy) -> Self {
        Self {
            client,
            policy,
            source: EvidenceSource::NewsWeb,
        }
    }

    pub fn for_source(client: ModelClient, source: EvidenceSource) -> Self {
        let policy = NativeWebPolicy {
            allowed_hosts: match source {
                EvidenceSource::SecEdgar => vec!["sec.gov".to_owned(), "www.sec.gov".to_owned()],
                EvidenceSource::Fred => vec!["fred.stlouisfed.org".to_owned()],
                EvidenceSource::NewsWeb => vec![
                    "reuters.com".to_owned(),
                    "www.reuters.com".to_owned(),
                    "apnews.com".to_owned(),
                    "www.apnews.com".to_owned(),
                ],
                EvidenceSource::Alpaca => Vec::new(),
            },
            ..NativeWebPolicy::default()
        };
        Self {
            client,
            policy,
            source,
        }
    }

    async fn acquire_inner(
        &self,
        source: EvidenceSource,
        resource: &str,
    ) -> Result<AcquiredEvidence, EvidenceAdapterError> {
        if source == EvidenceSource::Alpaca {
            return Err(EvidenceAdapterError::SourceMismatch);
        }
        let request = ModelRequest {
            instructions: "Use only the Rust-approved native web tool. Return verifiable citations for every material fact.".to_owned(),
            input: serde_json::json!({
                "source_family": source.as_str(),
                "research_intent": resource,
            })
            .to_string(),
            schema_name: None,
            schema: None,
            max_output_tokens: 2_000,
            tools: vec![self.policy.tool_definition()],
        };
        let response = self
            .client
            .respond(request)
            .await
            .map_err(|error| EvidenceAdapterError::Transport(error.to_string()))?;
        if !response.tool_calls.is_empty() {
            self.policy
                .validate_tool_calls(&response.tool_calls)
                .map_err(|error| EvidenceAdapterError::Transport(error.to_string()))?;
        }
        let citations = self
            .policy
            .extract_citations(&response.raw)
            .map_err(|error| EvidenceAdapterError::Transport(error.to_string()))?;
        let raw_value = serde_json::json!({
                "source_family": source,
                "resource": resource,
                "provider_request": response.request_body,
                "output_text": response.output_text,
                "citations": citations,
                "provider_result": response.raw,
        });
        let raw = serde_json::to_vec(&raw_value)
            .map_err(|error| EvidenceAdapterError::Transport(error.to_string()))?;
        let observed_at = Utc::now();
        let source_uri = citations
            .first()
            .map(|citation| citation.uri.clone())
            .ok_or_else(|| EvidenceAdapterError::Transport("missing citation URI".to_owned()))?;
        let provenance_citations = citations
            .iter()
            .map(|citation| EvidenceCitation {
                start_byte: 0,
                end_byte: raw.len(),
                quote: citation
                    .excerpt
                    .clone()
                    .unwrap_or_else(|| citation.uri.clone()),
            })
            .collect();
        let primary = citations
            .first()
            .ok_or_else(|| EvidenceAdapterError::Transport("missing citation URI".to_owned()))?;
        Ok(AcquiredEvidence {
            raw: raw.clone(),
            media_type: "application/json".to_owned(),
            source_uri: source_uri.clone(),
            observed_at,
            normalized: raw_value,
            provenance: EvidenceProvenance {
                document_id: primary
                    .document_id
                    .clone()
                    .or_else(|| Some(resource.to_owned())),
                published_at: primary
                    .published_at
                    .as_deref()
                    .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
                    .map(|value| value.with_timezone(&Utc)),
                observed_at,
                revision: primary.revision.clone(),
                source_uri,
                dedupe_key: format!(
                    "native-web:{}:{}",
                    primary.document_id.as_deref().unwrap_or(resource),
                    primary.revision.as_deref().unwrap_or("latest")
                ),
                citations: provenance_citations,
            },
            quality: EvidenceQuality {
                completeness_ppm: 1_000_000,
                citations_complete: true,
                normalized: true,
            },
        })
    }
}

impl AsyncEvidenceAdapter for ModelNativeWebEvidenceTransport {
    fn source(&self) -> EvidenceSource {
        self.source
    }

    fn acquire<'a>(
        &'a self,
        request: &'a EvidenceRequest,
    ) -> BoxFuture<'a, Result<AcquiredEvidence, EvidenceAdapterError>> {
        Box::pin(async move {
            if request.source != self.source {
                return Err(EvidenceAdapterError::SourceMismatch);
            }
            self.acquire_inner(request.source, &request.resource).await
        })
    }
}

macro_rules! governed_adapter {
    ($name:ident, $source:expr) => {
        #[derive(Debug, Clone)]
        pub struct $name<T> {
            transport: T,
        }

        impl<T> $name<T> {
            pub fn new(transport: T) -> Self {
                Self { transport }
            }
        }

        impl<T: GovernedEvidenceTransport> EvidenceAdapter for $name<T> {
            fn source(&self) -> EvidenceSource {
                $source
            }

            fn acquire(
                &self,
                request: &EvidenceRequest,
            ) -> Result<AcquiredEvidence, EvidenceAdapterError> {
                if request.source != $source {
                    return Err(EvidenceAdapterError::SourceMismatch);
                }
                let mut acquired = self.transport.acquire($source, &request.resource)?;
                acquired.normalized = serde_json::json!({
                    "adapter": $source.as_str(),
                    "resource": request.resource,
                    "payload": acquired.normalized,
                });
                Ok(acquired)
            }
        }
    };
}

governed_adapter!(AlpacaEvidenceAdapter, EvidenceSource::Alpaca);
governed_adapter!(SecEdgarEvidenceAdapter, EvidenceSource::SecEdgar);
governed_adapter!(FredEvidenceAdapter, EvidenceSource::Fred);
governed_adapter!(NewsWebEvidenceAdapter, EvidenceSource::NewsWeb);

/// Local-only adapter for deterministic test and replay input. It has no
/// filesystem, network, or model capability.
#[derive(Debug, Clone)]
pub struct FixtureEvidenceAdapter {
    source: EvidenceSource,
    responses: BTreeMap<String, AcquiredEvidence>,
}

impl FixtureEvidenceAdapter {
    pub fn new(
        source: EvidenceSource,
        responses: impl IntoIterator<Item = (String, AcquiredEvidence)>,
    ) -> Self {
        Self {
            source,
            responses: responses.into_iter().collect(),
        }
    }
}

impl EvidenceAdapter for FixtureEvidenceAdapter {
    fn source(&self) -> EvidenceSource {
        self.source
    }

    fn acquire(&self, request: &EvidenceRequest) -> Result<AcquiredEvidence, EvidenceAdapterError> {
        if request.source != self.source {
            return Err(EvidenceAdapterError::SourceMismatch);
        }
        self.responses
            .get(&request.resource)
            .cloned()
            .ok_or_else(|| EvidenceAdapterError::MissingFixture(request.resource.clone()))
    }
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

impl EvidenceRuntime {
    pub fn new(store: V2Store, allowed_sources: impl IntoIterator<Item = EvidenceSource>) -> Self {
        Self {
            store,
            allowed_sources: allowed_sources.into_iter().collect(),
        }
    }

    pub fn store(&self) -> &V2Store {
        &self.store
    }

    /// Construct raw and normalized evidence artifacts. The caller returns
    /// them to `TaskRuntime`, which atomically commits the attempt.
    pub fn acquire_and_normalize<A: EvidenceAdapter + ?Sized>(
        &self,
        permit: &TaskWritePermit,
        need: &ArtifactRef,
        request: &EvidenceRequest,
        adapter: &A,
        now: DateTime<Utc>,
    ) -> EvidenceRuntimeResult<EvidenceBundle> {
        request.validate()?;
        if need.kind != ArtifactKind::EvidenceNeed {
            return Err(EvidenceRuntimeError::InvalidEvidenceNeed);
        }
        let need_artifact = self.store.artifact(&need.artifact_id)?;
        if need_artifact.kind != ArtifactKind::EvidenceNeed
            || need_artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(&permit.run_id)
        {
            return Err(EvidenceRuntimeError::InvalidEvidenceNeed);
        }
        let declared: EvidenceNeed =
            serde_json::from_slice(&self.store.read_blob(&need_artifact.blob)?)?;
        declared.validate()?;
        let declared_max_age = i64::try_from(declared.max_age_secs)
            .map(Duration::seconds)
            .map_err(|_| EvidenceRuntimeError::InvalidEvidenceNeed)?;
        if declared.source_family != request.source.as_str()
            || declared.resource != request.resource
            || declared_max_age != request.max_age
        {
            return Err(EvidenceRuntimeError::InvalidEvidenceNeed);
        }
        if !self.allowed_sources.contains(&request.source) {
            return Err(EvidenceRuntimeError::SourceNotAllowed(request.source));
        }
        if adapter.source() != request.source {
            return Err(EvidenceAdapterError::SourceMismatch.into());
        }
        let acquired = adapter.acquire(request)?;
        if acquired.raw.is_empty()
            || acquired.media_type.trim().is_empty()
            || acquired.source_uri.trim().is_empty()
        {
            return Err(EvidenceRuntimeError::InvalidAcquisition);
        }
        acquired.provenance.validate(
            acquired.raw.len(),
            &acquired.source_uri,
            acquired.observed_at,
        )?;
        acquired.quality.validate()?;
        Self::validate_source_uri(&acquired.source_uri)?;
        if now.signed_duration_since(acquired.observed_at) > request.max_age {
            return Err(EvidenceRuntimeError::StaleEvidence);
        }

        let raw = Artifact::new(
            ArtifactKind::RawEvidence,
            self.store.put_bytes(&acquired.raw, &acquired.media_type)?,
            format!("akzio.ingest.{}.raw", request.source.as_str()),
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: request.source.as_str().to_owned(),
                observed_at: Some(acquired.observed_at),
                retrieved_at: now,
                source_uri: Some(acquired.source_uri.clone()),
                confidence_ppm: 1_000_000,
                producer_contract_hash: permit.contract_hash.clone(),
            },
            task_origin(permit),
            vec![],
            now,
        )?;
        let raw_ref = ArtifactRef {
            artifact_id: raw.artifact_id.clone(),
            kind: ArtifactKind::RawEvidence,
        };
        let normalized_payload = NormalizedEvidencePayload {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            source: request.source,
            resource: request.resource.clone(),
            need: need.clone(),
            raw: raw_ref.clone(),
            observed_at: acquired.observed_at,
            value: acquired.normalized,
            provenance: acquired.provenance,
            quality: acquired.quality,
        };
        let normalized = Artifact::new(
            ArtifactKind::NormalizedEvidence,
            self.store.put_json(&normalized_payload)?,
            format!("akzio.ingest.{}.normalized", request.source.as_str()),
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: request.source.as_str().to_owned(),
                observed_at: Some(acquired.observed_at),
                retrieved_at: now,
                source_uri: Some(acquired.source_uri),
                confidence_ppm: 1_000_000,
                producer_contract_hash: permit.contract_hash.clone(),
            },
            task_origin(permit),
            vec![raw_ref, need.clone()],
            now,
        )?;
        Ok(EvidenceBundle { raw, normalized })
    }

    pub async fn acquire_and_normalize_async<A: AsyncEvidenceAdapter + ?Sized>(
        &self,
        permit: &TaskWritePermit,
        need: &ArtifactRef,
        request: &EvidenceRequest,
        adapter: &A,
        now: DateTime<Utc>,
    ) -> EvidenceRuntimeResult<EvidenceBundle> {
        request.validate()?;
        if need.kind != ArtifactKind::EvidenceNeed {
            return Err(EvidenceRuntimeError::InvalidEvidenceNeed);
        }
        let need_artifact = self.store.artifact(&need.artifact_id)?;
        if need_artifact.kind != ArtifactKind::EvidenceNeed
            || need_artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(&permit.run_id)
        {
            return Err(EvidenceRuntimeError::InvalidEvidenceNeed);
        }
        let declared: EvidenceNeed =
            serde_json::from_slice(&self.store.read_blob(&need_artifact.blob)?)?;
        declared.validate()?;
        let declared_max_age = i64::try_from(declared.max_age_secs)
            .map(Duration::seconds)
            .map_err(|_| EvidenceRuntimeError::InvalidEvidenceNeed)?;
        if declared.source_family != request.source.as_str()
            || declared.resource != request.resource
            || declared_max_age != request.max_age
        {
            return Err(EvidenceRuntimeError::InvalidEvidenceNeed);
        }
        if !self.allowed_sources.contains(&request.source) {
            return Err(EvidenceRuntimeError::SourceNotAllowed(request.source));
        }
        if adapter.source() != request.source {
            return Err(EvidenceAdapterError::SourceMismatch.into());
        }
        let acquired = adapter.acquire(request).await?;
        if acquired.raw.is_empty()
            || acquired.media_type.trim().is_empty()
            || acquired.source_uri.trim().is_empty()
        {
            return Err(EvidenceRuntimeError::InvalidAcquisition);
        }
        acquired.provenance.validate(
            acquired.raw.len(),
            &acquired.source_uri,
            acquired.observed_at,
        )?;
        acquired.quality.validate()?;
        Self::validate_source_uri(&acquired.source_uri)?;
        if now.signed_duration_since(acquired.observed_at) > request.max_age {
            return Err(EvidenceRuntimeError::StaleEvidence);
        }
        let raw = Artifact::new(
            ArtifactKind::RawEvidence,
            self.store.put_bytes(&acquired.raw, &acquired.media_type)?,
            format!("akzio.ingest.{}.raw", request.source.as_str()),
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: request.source.as_str().to_owned(),
                observed_at: Some(acquired.observed_at),
                retrieved_at: now,
                source_uri: Some(acquired.source_uri.clone()),
                confidence_ppm: 1_000_000,
                producer_contract_hash: permit.contract_hash.clone(),
            },
            task_origin(permit),
            vec![],
            now,
        )?;
        let raw_ref = ArtifactRef {
            artifact_id: raw.artifact_id.clone(),
            kind: ArtifactKind::RawEvidence,
        };
        let normalized_payload = NormalizedEvidencePayload {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            source: request.source,
            resource: request.resource.clone(),
            need: need.clone(),
            raw: raw_ref.clone(),
            observed_at: acquired.observed_at,
            value: acquired.normalized,
            provenance: acquired.provenance,
            quality: acquired.quality,
        };
        let quality_ppm = normalized_payload.quality.completeness_ppm;
        let normalized = Artifact::new(
            ArtifactKind::NormalizedEvidence,
            self.store.put_json(&normalized_payload)?,
            format!("akzio.ingest.{}.normalized", request.source.as_str()),
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: request.source.as_str().to_owned(),
                observed_at: Some(acquired.observed_at),
                retrieved_at: now,
                source_uri: Some(acquired.source_uri),
                confidence_ppm: quality_ppm,
                producer_contract_hash: permit.contract_hash.clone(),
            },
            task_origin(permit),
            vec![raw_ref, need.clone()],
            now,
        )?;
        Ok(EvidenceBundle { raw, normalized })
    }

    /// Materialize a loss-bounded semantic detail in a separate task. The
    /// caller must cite an already sealed normalized artifact.
    pub fn materialize_detail(
        &self,
        permit: &TaskWritePermit,
        input: DetailInput,
        now: DateTime<Utc>,
    ) -> EvidenceRuntimeResult<Artifact> {
        if input.normalized.kind != ArtifactKind::NormalizedEvidence {
            return Err(EvidenceRuntimeError::DetailRequiresNormalizedEvidence);
        }
        let normalized = self.store.artifact(&input.normalized.artifact_id)?;
        if normalized.kind != ArtifactKind::NormalizedEvidence {
            return Err(EvidenceRuntimeError::DetailRequiresNormalizedEvidence);
        }
        let detail = Artifact::new(
            ArtifactKind::SemanticDetail,
            self.store.put_json(&input.value)?,
            "akzio.ingest.semantic_detail",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: normalized.provenance.source_family.clone(),
                observed_at: normalized.provenance.observed_at,
                retrieved_at: now,
                source_uri: normalized.provenance.source_uri.clone(),
                confidence_ppm: normalized.provenance.confidence_ppm,
                producer_contract_hash: permit.contract_hash.clone(),
            },
            task_origin(permit),
            vec![input.normalized],
            now,
        )?;
        Ok(detail)
    }

    fn validate_source_uri(source_uri: &str) -> EvidenceRuntimeResult<()> {
        let parsed = Url::parse(source_uri).map_err(|_| EvidenceRuntimeError::UnsafeSourceUri)?;
        if parsed.username() != ""
            || parsed.password().is_some()
            || parsed.fragment().is_some()
            || parsed.query_pairs().any(|(key, _)| {
                let key = key.to_ascii_lowercase();
                key.contains("token")
                    || key.contains("secret")
                    || key.contains("password")
                    || key.contains("api_key")
                    || key == "key"
                    || key.contains("authorization")
            })
        {
            return Err(EvidenceRuntimeError::UnsafeSourceUri);
        }
        Ok(())
    }
}

fn task_origin(permit: &TaskWritePermit) -> Option<ArtifactOrigin> {
    Some(ArtifactOrigin {
        run_id: Some(permit.run_id.clone()),
        task_id: Some(permit.task_id.clone()),
        attempt_id: Some(permit.attempt_id.clone()),
        contract_hash: permit.contract_hash.clone(),
    })
}

#[cfg(test)]
mod tests {
    use akzio_domain::{
        FailureDisposition, RetryPolicy, RunId, RunPurpose, TaskBudget, TaskId, TaskRecipeId,
        TaskStatus, WorkflowGraph, WorkflowNode,
    };
    use akzio_store::v2::{StoredRun, WorkflowCommit};
    use tempfile::tempdir;

    use super::*;

    fn budget() -> TaskBudget {
        TaskBudget {
            max_input_tokens: 64,
            max_output_tokens: 32,
            max_wall_time_secs: 10,
            max_tool_calls: 1,
        }
    }

    fn retry() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 1,
            retry_transport: true,
            retry_rate_limited: true,
            retry_invalid_output: false,
        }
    }

    #[derive(Clone)]
    struct FixtureTransport {
        evidence: AcquiredEvidence,
    }

    impl GovernedEvidenceTransport for FixtureTransport {
        fn acquire(
            &self,
            _source: EvidenceSource,
            _resource: &str,
        ) -> Result<AcquiredEvidence, EvidenceAdapterError> {
            Ok(self.evidence.clone())
        }
    }

    fn transport() -> FixtureTransport {
        let observed_at = Utc::now();
        FixtureTransport {
            evidence: AcquiredEvidence {
                raw: br#"{\"fixture\":true}"#.to_vec(),
                media_type: "application/json".to_owned(),
                source_uri: "fixture://governed/resource".to_owned(),
                observed_at,
                normalized: serde_json::json!({"fixture": true}),
                provenance: EvidenceProvenance {
                    document_id: Some("fixture-governed".to_owned()),
                    published_at: None,
                    observed_at,
                    revision: Some("1".to_owned()),
                    source_uri: "fixture://governed/resource".to_owned(),
                    dedupe_key: "fixture:governed:resource".to_owned(),
                    citations: vec![EvidenceCitation {
                        start_byte: 0,
                        end_byte: 18,
                        quote: "{\"fixture\":true}".to_owned(),
                    }],
                },
                quality: EvidenceQuality::default(),
            },
        }
    }

    fn assert_governed_adapter<A: EvidenceAdapter>(
        adapter: A,
        source: EvidenceSource,
        other: EvidenceSource,
    ) {
        let response = adapter
            .acquire(&EvidenceRequest {
                source,
                resource: "resource".to_owned(),
                max_age: Duration::seconds(30),
            })
            .unwrap();
        assert_eq!(response.normalized["adapter"], source.as_str());
        assert_eq!(response.normalized["resource"], "resource");
        assert_eq!(response.normalized["payload"]["fixture"], true);
        assert!(matches!(
            adapter.acquire(&EvidenceRequest {
                source: other,
                resource: "resource".to_owned(),
                max_age: Duration::seconds(30),
            }),
            Err(EvidenceAdapterError::SourceMismatch)
        ));
    }

    #[test]
    fn governed_adapters_are_source_typed_and_local_transport_only() {
        assert_governed_adapter(
            AlpacaEvidenceAdapter::new(transport()),
            EvidenceSource::Alpaca,
            EvidenceSource::SecEdgar,
        );
        assert_governed_adapter(
            SecEdgarEvidenceAdapter::new(transport()),
            EvidenceSource::SecEdgar,
            EvidenceSource::Fred,
        );
        assert_governed_adapter(
            FredEvidenceAdapter::new(transport()),
            EvidenceSource::Fred,
            EvidenceSource::NewsWeb,
        );
        assert_governed_adapter(
            NewsWebEvidenceAdapter::new(transport()),
            EvidenceSource::NewsWeb,
            EvidenceSource::Alpaca,
        );
    }

    #[test]
    fn source_uri_rejects_credentials_and_query_parameters() {
        assert!(EvidenceRuntime::validate_source_uri("fixture://alpaca/quote").is_ok());
        assert!(matches!(
            EvidenceRuntime::validate_source_uri("https://key:secret@example.test/evidence"),
            Err(EvidenceRuntimeError::UnsafeSourceUri)
        ));
        assert!(matches!(
            EvidenceRuntime::validate_source_uri("https://example.test/evidence?token=secret"),
            Err(EvidenceRuntimeError::UnsafeSourceUri)
        ));
        assert!(EvidenceRuntime::validate_source_uri(
            "https://fred.stlouisfed.org/series/DFII10?cosd=2020-01-01"
        )
        .is_ok());
    }

    fn install_run(store: &V2Store, now: DateTime<Utc>, tasks: usize) -> RunId {
        let graph = WorkflowGraph {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            topology_id: "fixture".to_owned(),
            nodes: (0..tasks)
                .map(|index| WorkflowNode {
                    task_id: TaskId::new(),
                    recipe_id: TaskRecipeId::new(format!("evidence.fixture.{index}")).unwrap(),
                    contract_hash: None,
                    objective: "seal evidence".to_owned(),
                    dependencies: vec![],
                    input_artifacts: vec![],
                    priority: 50,
                    budget: budget(),
                    retry: retry(),
                    on_failure: FailureDisposition::FailRun,
                    parent_task_id: None,
                })
                .collect(),
        };
        let graph_artifact = Artifact::new(
            ArtifactKind::WorkflowGraph,
            store.put_json(&graph).unwrap(),
            "fixture.workflow",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "fixture".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            None,
            vec![],
            now,
        )
        .unwrap();
        let run = StoredRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Debug,
            topology_id: graph.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: now,
        };
        store
            .commit_workflow(&WorkflowCommit {
                run: run.clone(),
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();
        run.run_id
    }

    fn fixture(now: DateTime<Utc>) -> FixtureEvidenceAdapter {
        FixtureEvidenceAdapter::new(
            EvidenceSource::Alpaca,
            [(
                "quote".to_owned(),
                AcquiredEvidence {
                    raw: br#"{\"quote\": \"fixture\"}"#.to_vec(),
                    media_type: "application/json".to_owned(),
                    source_uri: "fixture://alpaca/quote".to_owned(),
                    observed_at: now,
                    normalized: serde_json::json!({"symbol": "QQQ", "price": 1}),
                    provenance: EvidenceProvenance {
                        document_id: Some("fixture-quote".to_owned()),
                        published_at: None,
                        observed_at: now,
                        revision: Some("1".to_owned()),
                        source_uri: "fixture://alpaca/quote".to_owned(),
                        dedupe_key: "fixture:alpaca:quote".to_owned(),
                        citations: vec![],
                    },
                    quality: EvidenceQuality::default(),
                },
            )],
        )
    }

    fn evidence_need(
        store: &V2Store,
        task: &akzio_store::v2::ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> ArtifactRef {
        let payload = akzio_domain::EvidenceNeed {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            source_family: "alpaca".to_owned(),
            resource: "quote".to_owned(),
            max_age_secs: 30,
        };
        let artifact = Artifact::new(
            ArtifactKind::EvidenceNeed,
            store.put_json(&payload).unwrap(),
            "fixture.planner",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.workflow.planner".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: task.permit.contract_hash.clone(),
            },
            Some(ArtifactOrigin {
                run_id: Some(task.run_id.clone()),
                task_id: Some(task.node.task_id.clone()),
                attempt_id: Some(task.permit.attempt_id.clone()),
                contract_hash: task.permit.contract_hash.clone(),
            }),
            vec![],
            now,
        )
        .unwrap();
        store
            .write_task_artifact(
                &task.permit,
                &artifact,
                "planner.evidence_need_created",
                now,
            )
            .unwrap();
        ArtifactRef {
            artifact_id: artifact.artifact_id,
            kind: ArtifactKind::EvidenceNeed,
        }
    }

    #[test]
    fn acquisition_returns_uncommitted_artifacts_until_task_runtime_commits() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        let run_id = install_run(&store, now, 1);
        let claimed = store
            .claim_next_task("evidence-worker", now, Duration::seconds(30))
            .unwrap()
            .unwrap();
        let need = evidence_need(&store, &claimed, now);
        let events_before = store.events_after(&run_id, 0, 10).unwrap();
        let runtime = EvidenceRuntime::new(store.clone(), [EvidenceSource::Alpaca]);
        let sealed = runtime
            .acquire_and_normalize(
                &claimed.permit,
                &need,
                &EvidenceRequest {
                    source: EvidenceSource::Alpaca,
                    resource: "quote".to_owned(),
                    max_age: Duration::seconds(30),
                },
                &fixture(now),
                now,
            )
            .unwrap();
        assert_eq!(sealed.raw.kind, ArtifactKind::RawEvidence);
        assert_eq!(sealed.normalized.kind, ArtifactKind::NormalizedEvidence);
        let mut expected_source_refs = vec![
            ArtifactRef {
                artifact_id: sealed.raw.artifact_id.clone(),
                kind: ArtifactKind::RawEvidence,
            },
            need.clone(),
        ];
        expected_source_refs.sort();
        assert_eq!(sealed.normalized.source_refs, expected_source_refs);
        assert!(matches!(
            store.artifact(&sealed.raw.artifact_id),
            Err(akzio_store::v2::StoreError::MissingArtifact(_))
        ));
        assert!(matches!(
            store.artifact(&sealed.normalized.artifact_id),
            Err(akzio_store::v2::StoreError::MissingArtifact(_))
        ));
        assert_eq!(store.events_after(&run_id, 0, 10).unwrap(), events_before);

        store
            .commit_attempt(
                &claimed.permit,
                &[sealed.raw.clone(), sealed.normalized.clone()],
                TaskStatus::Succeeded,
                now,
            )
            .unwrap();

        assert_eq!(store.artifact(&sealed.raw.artifact_id).unwrap(), sealed.raw);
        assert_eq!(
            store.artifact(&sealed.normalized.artifact_id).unwrap(),
            sealed.normalized
        );
        assert_eq!(
            store
                .artifacts_referencing(&need.artifact_id, Some(ArtifactKind::NormalizedEvidence))
                .unwrap(),
            vec![sealed.normalized.clone()]
        );
        let events_after = store.events_after(&run_id, 0, 10).unwrap();
        assert_eq!(events_after.len(), events_before.len() + 3);
        assert_eq!(
            events_after
                .iter()
                .filter(|event| event.event_type == "task.succeeded")
                .count(),
            1
        );
        store.verify_integrity().unwrap();
    }

    #[test]
    fn stale_or_unallowlisted_evidence_never_writes_task_output() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        let run_id = install_run(&store, now, 1);
        let claimed = store
            .claim_next_task("evidence-worker", now, Duration::seconds(30))
            .unwrap()
            .unwrap();
        let need = evidence_need(&store, &claimed, now);
        let permit = claimed.permit;
        let events_before = store.events_after(&run_id, 0, 10).unwrap();
        let stale = FixtureEvidenceAdapter::new(
            EvidenceSource::Alpaca,
            [(
                "quote".to_owned(),
                AcquiredEvidence {
                    raw: b"fixture".to_vec(),
                    media_type: "application/json".to_owned(),
                    source_uri: "fixture://alpaca/quote".to_owned(),
                    observed_at: now - Duration::minutes(5),
                    normalized: serde_json::json!({}),
                    provenance: EvidenceProvenance {
                        document_id: Some("fixture-stale".to_owned()),
                        published_at: None,
                        observed_at: now - Duration::minutes(5),
                        revision: Some("1".to_owned()),
                        source_uri: "fixture://alpaca/quote".to_owned(),
                        dedupe_key: "fixture:alpaca:stale".to_owned(),
                        citations: vec![],
                    },
                    quality: EvidenceQuality::default(),
                },
            )],
        );
        let runtime = EvidenceRuntime::new(store.clone(), [EvidenceSource::Alpaca]);
        assert!(matches!(
            runtime.acquire_and_normalize(
                &permit,
                &need,
                &EvidenceRequest {
                    source: EvidenceSource::Alpaca,
                    resource: "bars".to_owned(),
                    max_age: Duration::seconds(30),
                },
                &stale,
                now,
            ),
            Err(EvidenceRuntimeError::InvalidEvidenceNeed)
        ));
        assert!(matches!(
            runtime.acquire_and_normalize(
                &permit,
                &need,
                &EvidenceRequest {
                    source: EvidenceSource::Alpaca,
                    resource: "quote".to_owned(),
                    max_age: Duration::seconds(30),
                },
                &stale,
                now,
            ),
            Err(EvidenceRuntimeError::StaleEvidence)
        ));
        assert_eq!(store.events_after(&run_id, 0, 10).unwrap(), events_before);
        assert!(matches!(
            EvidenceRuntime::new(store, [EvidenceSource::Fred]).acquire_and_normalize(
                &permit,
                &need,
                &EvidenceRequest {
                    source: EvidenceSource::Alpaca,
                    resource: "quote".to_owned(),
                    max_age: Duration::seconds(30),
                },
                &fixture(now),
                now,
            ),
            Err(EvidenceRuntimeError::SourceNotAllowed(
                EvidenceSource::Alpaca
            ))
        ));
    }

    #[test]
    fn semantic_detail_is_constructed_then_committed_by_task_runtime() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        install_run(&store, now, 2);
        let first = store
            .claim_next_task("evidence-worker", now, Duration::seconds(30))
            .unwrap()
            .unwrap();
        let need = evidence_need(&store, &first, now);
        let runtime = EvidenceRuntime::new(store.clone(), [EvidenceSource::Alpaca]);
        let sealed = runtime
            .acquire_and_normalize(
                &first.permit,
                &need,
                &EvidenceRequest {
                    source: EvidenceSource::Alpaca,
                    resource: "quote".to_owned(),
                    max_age: Duration::seconds(30),
                },
                &fixture(now),
                now,
            )
            .unwrap();
        store
            .commit_attempt(
                &first.permit,
                &[sealed.raw.clone(), sealed.normalized.clone()],
                TaskStatus::Succeeded,
                now,
            )
            .unwrap();
        let second = store
            .claim_next_task("detail-worker", now, Duration::seconds(30))
            .unwrap()
            .unwrap();
        let detail = runtime
            .materialize_detail(
                &second.permit,
                DetailInput {
                    normalized: ArtifactRef {
                        artifact_id: sealed.normalized.artifact_id.clone(),
                        kind: ArtifactKind::NormalizedEvidence,
                    },
                    value: serde_json::json!({"summary": "fixture"}),
                },
                now,
            )
            .unwrap();
        assert_eq!(detail.kind, ArtifactKind::SemanticDetail);
        assert_eq!(detail.source_refs.len(), 1);
        assert!(matches!(
            store.artifact(&detail.artifact_id),
            Err(akzio_store::v2::StoreError::MissingArtifact(_))
        ));
        store
            .commit_attempt(
                &second.permit,
                std::slice::from_ref(&detail),
                TaskStatus::Succeeded,
                now,
            )
            .unwrap();
        assert_eq!(store.artifact(&detail.artifact_id).unwrap(), detail);
        store.verify_integrity().unwrap();
    }

    #[test]
    fn alpaca_paper_transport_is_endpoint_and_resource_fenced() {
        assert!(
            AlpacaPaperEvidenceTransport::new("https://api.alpaca.markets", "key", "secret")
                .is_err()
        );
        assert_eq!(
            AlpacaPaperEvidenceTransport::path_for("bars:QQQ:1d").unwrap(),
            "/v2/stocks/QQQ/bars?timeframe=1Day&limit=1&adjustment=all"
        );
        assert_eq!(
            AlpacaPaperEvidenceTransport::path_for("bars:QQQ:1d:2026-08-01:6").unwrap(),
            "/v2/stocks/QQQ/bars?timeframe=1Day&limit=6&adjustment=all&start=2026-08-01"
        );
        assert!(AlpacaPaperEvidenceTransport::path_for("bars:QQQ:1d:2026-08-01:33").is_err());
        assert!(AlpacaPaperEvidenceTransport::path_for("bars:SPY:1d").is_err());
        assert!(AlpacaPaperEvidenceTransport::path_for("bars:QQQ:5m").is_err());
        assert!(AlpacaPaperEvidenceTransport::path_for("https://example.com").is_err());
    }

    #[tokio::test]
    async fn native_web_transport_requires_allowlisted_citations() {
        let client = ModelClient::Fixture(serde_json::json!({
            "output_text": "DFII10 evidence",
            "citations": [{
                "url": "https://fred.stlouisfed.org/series/DFII10",
                "title": "FRED",
                "text": "real yield"
            }]
        }));
        let transport = ModelNativeWebEvidenceTransport::for_source(client, EvidenceSource::Fred);
        let evidence = transport
            .acquire(&EvidenceRequest {
                source: EvidenceSource::Fred,
                resource: "series:DFII10".to_owned(),
                max_age: Duration::minutes(5),
            })
            .await
            .unwrap();
        assert_eq!(
            evidence.provenance.source_uri,
            "https://fred.stlouisfed.org/series/DFII10"
        );
        assert_eq!(evidence.provenance.citations.len(), 1);
        assert_eq!(evidence.quality.completeness_ppm, 1_000_000);
    }

    #[test]
    fn governed_resource_schema_bounds_sources_windows_and_assets() {
        assert_eq!(
            GovernedResource::parse(EvidenceSource::Alpaca, "bars:QQQ:1d:2026-08-01:6").unwrap(),
            GovernedResource::AlpacaBars {
                asset: Asset::Qqq,
                start: Some(NaiveDate::from_ymd_opt(2026, 8, 1).unwrap()),
                limit: 6,
            }
        );
        assert!(GovernedResource::parse(EvidenceSource::Alpaca, "bars:SPY:1d").is_err());
        assert!(GovernedResource::parse(EvidenceSource::Alpaca, "bars:QQQ:5m").is_err());
        assert!(GovernedResource::parse(
            EvidenceSource::Fred,
            "series:DFII10:2026-08-01:2028-08-01"
        )
        .is_err());
        assert_eq!(
            GovernedResource::parse(EvidenceSource::NewsWeb, "news:semiconductor supply chain")
                .unwrap(),
            GovernedResource::NewsWeb {
                query: "semiconductor supply chain".to_owned(),
            }
        );
    }

    #[test]
    fn daily_bar_quality_gate_rejects_missing_ohlcv_weekends_and_duplicates() {
        let valid = serde_json::json!({
            "bars": [
                {
                    "t": "2026-08-10T20:00:00Z",
                    "o": 100.0,
                    "h": 105.0,
                    "l": 99.0,
                    "c": 103.0,
                    "v": 1000,
                    "adjustment": "all"
                }
            ]
        });
        validate_daily_bar_payload(&valid).unwrap();

        let mut missing = valid.clone();
        missing["bars"][0].as_object_mut().unwrap().remove("v");
        assert!(validate_daily_bar_payload(&missing).is_err());

        let mut missing_adjustment = valid.clone();
        missing_adjustment["bars"][0]
            .as_object_mut()
            .unwrap()
            .remove("adjustment");
        assert!(validate_daily_bar_payload(&missing_adjustment).is_err());
        let mut split_unadjusted = valid.clone();
        split_unadjusted["bars"][0]["adjustment"] = serde_json::json!("raw");
        assert!(validate_daily_bar_payload(&split_unadjusted).is_err());

        let weekend = serde_json::json!({
            "bars": [{
                "t": "2026-08-09T20:00:00Z",
                "o": 100.0,
                "h": 105.0,
                "l": 99.0,
                "c": 103.0,
                "v": 1000
            }]
        });
        assert!(validate_daily_bar_payload(&weekend).is_err());

        let duplicate = serde_json::json!({
            "bars": [
                {"t":"2026-08-10T20:00:00Z","o":100,"h":105,"l":99,"c":103,"v":1000},
                {"t":"2026-08-10T21:00:00Z","o":100,"h":106,"l":98,"c":104,"v":1100}
            ]
        });
        assert!(validate_daily_bar_payload(&duplicate).is_err());
    }
}
