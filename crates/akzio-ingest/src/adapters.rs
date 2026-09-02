use super::*;
use akzio_model::{ModelError, NativeWebCitation};
use futures::StreamExt;
use reqwest::header::{ACCEPT, CONTENT_TYPE, ETAG, LAST_MODIFIED};
use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
    time::Duration as StdDuration,
};

#[derive(Debug, Error)]
pub enum EvidenceAdapterError {
    #[error("fixture for {0} is unavailable")]
    MissingFixture(String),
    #[error("adapter source does not match request")]
    SourceMismatch,
    #[error("governed evidence transport failed: {0}")]
    Transport(String),
    #[error("governed evidence policy rejected {evidence_source:?} {resource}: {reason}")]
    Policy {
        evidence_source: EvidenceSource,
        resource: String,
        reason: String,
    },
}

fn model_error(error: ModelError, source: EvidenceSource, resource: &str) -> EvidenceAdapterError {
    let reason = error.to_string();
    match error {
        ModelError::NativeWebUnavailable
        | ModelError::NativeWebToolNotAllowed
        | ModelError::NativeWebArgumentsInvalid
        | ModelError::NativeWebCitationsMissing
        | ModelError::NativeWebUnsafeCitation { .. }
        | ModelError::NativeWebLimitExceeded => EvidenceAdapterError::Policy {
            evidence_source: source,
            resource: resource.to_owned(),
            reason,
        },
        _ => EvidenceAdapterError::Transport(reason),
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlpacaMarketDataFeed {
    Iex,
    Sip,
}

impl AlpacaMarketDataFeed {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Iex => "iex",
            Self::Sip => "sip",
        }
    }
}

/// Rust-owned Alpaca Paper market-data transport. The resource language is
/// deliberately finite; callers cannot pass an arbitrary URL or endpoint.
#[derive(Clone)]
pub struct AlpacaPaperEvidenceTransport {
    client: Client,
    base_url: String,
    market_data_url: String,
    key_id: String,
    secret_key: String,
    market_data_feed: Option<AlpacaMarketDataFeed>,
}

impl std::fmt::Debug for AlpacaPaperEvidenceTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlpacaPaperEvidenceTransport")
            .field("base_url", &self.base_url)
            .field("market_data_url", &self.market_data_url)
            .field("key_id", &"<redacted>")
            .field("secret_key", &"<redacted>")
            .field("market_data_feed", &self.market_data_feed)
            .finish()
    }
}

impl AlpacaPaperEvidenceTransport {
    pub fn from_env(
        market_data_feed: Option<AlpacaMarketDataFeed>,
    ) -> Result<Self, EvidenceAdapterError> {
        let base_url = env::var("ALPACA_PAPER_BASE_URL")
            .unwrap_or_else(|_| "https://paper-api.alpaca.markets".to_owned());
        let key_id = env::var("ALPACA_API_KEY")
            .map_err(|_| EvidenceAdapterError::Transport("ALPACA_API_KEY is not set".to_owned()))?;
        let secret_key = env::var("ALPACA_API_SECRET").map_err(|_| {
            EvidenceAdapterError::Transport("ALPACA_API_SECRET is not set".to_owned())
        })?;
        Self::new(base_url, key_id, secret_key, market_data_feed)
    }

    pub fn new(
        base_url: impl Into<String>,
        key_id: impl Into<String>,
        secret_key: impl Into<String>,
        market_data_feed: Option<AlpacaMarketDataFeed>,
    ) -> Result<Self, EvidenceAdapterError> {
        let supplied = base_url.into();
        if !matches!(
            supplied.trim(),
            "https://paper-api.alpaca.markets" | "https://paper-api.alpaca.markets/"
        ) {
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
            .http1_only()
            .local_address(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
            .connect_timeout(std::time::Duration::from_secs(15))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|error| EvidenceAdapterError::Transport(error.to_string()))?;
        Ok(Self {
            client,
            base_url: "https://paper-api.alpaca.markets".to_owned(),
            market_data_url: "https://data.alpaca.markets".to_owned(),
            key_id,
            secret_key,
            market_data_feed,
        })
    }

    pub(super) fn path_for(resource: &str) -> Result<String, EvidenceAdapterError> {
        match resource {
            "paper.account" => Ok("/v2/account".to_owned()),
            "paper.positions" => Ok("/v2/positions".to_owned()),
            "paper.open_orders" => {
                Ok("/v2/orders?status=open&limit=500&direction=asc&nested=true".to_owned())
            }
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
                let limit = limit.parse::<u16>().map_err(|_| {
                    EvidenceAdapterError::Transport("invalid Alpaca bars limit".to_owned())
                })?;
                if !(1..=252).contains(&limit) {
                    return Err(EvidenceAdapterError::Transport(
                        "Alpaca bars limit outside 1..=252".to_owned(),
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
            value if value.starts_with("paper.fills:") => {
                let session_key = value.trim_start_matches("paper.fills:");
                chrono::NaiveDate::parse_from_str(session_key, "%Y-%m-%d").map_err(|_| {
                    EvidenceAdapterError::Transport("invalid Paper fills session date".to_owned())
                })?;
                Ok(format!(
                    "/v2/account/activities/FILL?date={session_key}&direction=asc&page_size=100"
                ))
            }
            value if value.starts_with("observer.qqq_history:") => {
                let mut parts = value.split(':');
                let _ = parts.next();
                let range = parts.next().ok_or_else(|| {
                    EvidenceAdapterError::Transport("invalid observer QQQ range".to_owned())
                })?;
                let start = parts.next().ok_or_else(|| {
                    EvidenceAdapterError::Transport("invalid observer QQQ start".to_owned())
                })?;
                if parts.next().is_some() {
                    return Err(EvidenceAdapterError::Transport(
                        "invalid observer QQQ resource".to_owned(),
                    ));
                }
                chrono::NaiveDate::parse_from_str(start, "%Y-%m-%d").map_err(|_| {
                    EvidenceAdapterError::Transport("invalid observer QQQ start".to_owned())
                })?;
                let timeframe = match range {
                    "1d" => "5Min",
                    "1w" => "1Hour",
                    "1m" | "3m" => "1Day",
                    _ => {
                        return Err(EvidenceAdapterError::Transport(
                            "invalid observer QQQ range".to_owned(),
                        ));
                    }
                };
                Ok(format!(
                "/v2/stocks/QQQ/bars?timeframe={timeframe}&limit=1000&adjustment=all&start={start}"
            ))
            }
            _ => Err(EvidenceAdapterError::Transport(
                "Alpaca resource is not allowlisted".to_owned(),
            )),
        }
    }

    fn uses_market_data(resource: &str) -> bool {
        resource == "paper.quotes"
            || resource.starts_with("quote:")
            || resource.starts_with("bars:")
            || resource.starts_with("observer.qqq_history:")
    }

    pub(super) fn configured_path_for(
        &self,
        resource: &str,
    ) -> Result<String, EvidenceAdapterError> {
        let mut path = Self::path_for(resource)?;
        if Self::uses_market_data(resource) {
            if let Some(feed) = self.market_data_feed {
                path.push(if path.contains('?') { '&' } else { '?' });
                path.push_str("feed=");
                path.push_str(feed.as_str());
            }
        }
        Ok(path)
    }

    pub(super) fn base_url_for(&self, resource: &str) -> &str {
        if Self::uses_market_data(resource) {
            &self.market_data_url
        } else {
            &self.base_url
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
        let path = self.configured_path_for(resource)?;
        let url = format!("{}{}", self.base_url_for(resource), path);
        let response = {
            let mut attempt = 1_u64;
            loop {
                match self
                    .client
                    .get(&url)
                    .header("APCA-API-KEY-ID", &self.key_id)
                    .header("APCA-API-SECRET-KEY", &self.secret_key)
                    .send()
                    .await
                {
                    Ok(response) => break response,
                    Err(_error) if attempt < 5 => {
                        tokio::time::sleep(std::time::Duration::from_millis(250 * attempt)).await;
                        attempt += 1;
                    }
                    Err(error) => {
                        return Err(EvidenceAdapterError::Transport(error.to_string()));
                    }
                }
            }
        };
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

const MAX_SOURCE_DOCUMENT_BYTES: usize = 2 * 1024 * 1024;
const MIN_SOURCE_QUOTE_BYTES: usize = 16;
const MAX_SOURCE_QUOTE_BYTES: usize = 4 * 1024;

#[derive(Debug, Clone)]
pub(super) struct SourceDocumentSnapshot {
    pub(super) body: Vec<u8>,
    pub(super) media_type: String,
    pub(super) fetched_at: DateTime<Utc>,
    pub(super) status_code: u16,
    pub(super) etag: Option<String>,
    pub(super) last_modified: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SourceDocumentFailureKind {
    Redirect,
    HttpStatus,
    BodyTooLarge,
    UnsupportedMediaType,
    EmptyBody,
    Transport,
}

impl SourceDocumentFailureKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Redirect => "redirect",
            Self::HttpStatus => "http_status",
            Self::BodyTooLarge => "body_too_large",
            Self::UnsupportedMediaType => "unsupported_media_type",
            Self::EmptyBody => "empty_body",
            Self::Transport => "transport",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SourceDocumentFetchError {
    kind: SourceDocumentFailureKind,
    message: String,
    status_code: Option<u16>,
}

impl SourceDocumentFetchError {
    pub(super) fn new(kind: SourceDocumentFailureKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            status_code: None,
        }
    }

    fn with_status(
        kind: SourceDocumentFailureKind,
        status_code: u16,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            status_code: Some(status_code),
        }
    }
}

impl std::fmt::Display for SourceDocumentFetchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

/// Fetch one policy-validated source URI as an immutable response-body snapshot.
/// Implementations must reject redirects, unsupported media types, empty bodies,
/// and bodies larger than `MAX_SOURCE_DOCUMENT_BYTES`.
pub(super) trait SourceDocumentFetcher: Send + Sync {
    fn fetch<'a>(
        &'a self,
        uri: &'a str,
    ) -> BoxFuture<'a, Result<SourceDocumentSnapshot, SourceDocumentFetchError>>;
}

#[derive(Clone)]
struct HttpSourceDocumentFetcher {
    client: Client,
}

impl HttpSourceDocumentFetcher {
    fn new() -> Result<Self, EvidenceAdapterError> {
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(StdDuration::from_secs(10))
            .timeout(StdDuration::from_secs(20))
            .user_agent("akzio-source-snapshot/0.2")
            .build()
            .map_err(|error| EvidenceAdapterError::Transport(error.to_string()))?;
        Ok(Self { client })
    }
}

impl SourceDocumentFetcher for HttpSourceDocumentFetcher {
    fn fetch<'a>(
        &'a self,
        uri: &'a str,
    ) -> BoxFuture<'a, Result<SourceDocumentSnapshot, SourceDocumentFetchError>> {
        Box::pin(async move {
            let response = self
                .client
                .get(uri)
                .header(
                    ACCEPT,
                    "text/html,application/xhtml+xml,text/plain,application/json;q=0.8",
                )
                .send()
                .await
                .map_err(|error| {
                    SourceDocumentFetchError::new(
                        SourceDocumentFailureKind::Transport,
                        error.to_string(),
                    )
                })?;
            let status = response.status();
            if status.is_redirection() {
                return Err(SourceDocumentFetchError::with_status(
                    SourceDocumentFailureKind::Redirect,
                    status.as_u16(),
                    format!(
                        "source document returned redirect HTTP {} without redirect following",
                        status
                    ),
                ));
            }
            if !status.is_success() {
                return Err(SourceDocumentFetchError::with_status(
                    SourceDocumentFailureKind::HttpStatus,
                    status.as_u16(),
                    format!("source document returned HTTP {status}"),
                ));
            }
            if response
                .content_length()
                .is_some_and(|length| length > MAX_SOURCE_DOCUMENT_BYTES as u64)
            {
                return Err(SourceDocumentFetchError::new(
                    SourceDocumentFailureKind::BodyTooLarge,
                    "source document exceeds byte limit",
                ));
            }

            let media_type = response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.split(';').next())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_ascii_lowercase)
                .filter(|value| {
                    value.starts_with("text/")
                        || matches!(value.as_str(), "application/json" | "application/xhtml+xml")
                })
                .ok_or_else(|| {
                    SourceDocumentFetchError::new(
                        SourceDocumentFailureKind::UnsupportedMediaType,
                        "source document media type is missing or unsupported",
                    )
                })?;
            let etag = response
                .headers()
                .get(ETAG)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let last_modified = response
                .headers()
                .get(LAST_MODIFIED)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let capacity = response
                .content_length()
                .unwrap_or_default()
                .min(MAX_SOURCE_DOCUMENT_BYTES as u64) as usize;
            let mut body = Vec::with_capacity(capacity);
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|error| {
                    SourceDocumentFetchError::new(
                        SourceDocumentFailureKind::Transport,
                        error.to_string(),
                    )
                })?;
                if chunk.len() > MAX_SOURCE_DOCUMENT_BYTES.saturating_sub(body.len()) {
                    return Err(SourceDocumentFetchError::new(
                        SourceDocumentFailureKind::BodyTooLarge,
                        "source document exceeds byte limit",
                    ));
                }
                body.extend_from_slice(&chunk);
            }
            if body.is_empty() {
                return Err(SourceDocumentFetchError::new(
                    SourceDocumentFailureKind::EmptyBody,
                    "source document body is empty",
                ));
            }

            Ok(SourceDocumentSnapshot {
                body,
                media_type,
                fetched_at: Utc::now(),
                status_code: status.as_u16(),
                etag,
                last_modified,
            })
        })
    }
}

struct SourceMaterialization {
    raw: Vec<u8>,
    media_type: String,
    observed_at: DateTime<Utc>,
    citations: Vec<EvidenceCitation>,
    quality: EvidenceQuality,
    revision: Option<String>,
    dedupe_key: String,
    metadata: Value,
}

/// One provider search, reduced to the Rust-owned identity that a later audit
/// can compare against. `payload_hash` covers the provider result alone, so it
/// stays stable even though the persisted envelope also carries the request.
struct ProviderSearchIdentity {
    response_id: Option<String>,
    request_hash: String,
    payload_hash: String,
    searched_at: DateTime<Utc>,
}

impl ProviderSearchIdentity {
    fn new(raw: &Value, request_body: &Value) -> Result<Self, EvidenceAdapterError> {
        let json_hash = |value: &Value| -> Result<String, EvidenceAdapterError> {
            let bytes = serde_json::to_vec(value)
                .map_err(|error| EvidenceAdapterError::Transport(error.to_string()))?;
            Ok(ContentHash::of_bytes(&bytes).to_string())
        };
        Ok(Self {
            response_id: raw.get("id").and_then(Value::as_str).map(str::to_owned),
            request_hash: json_hash(request_body)?,
            payload_hash: json_hash(raw)?,
            searched_at: Utc::now(),
        })
    }

    fn provenance(&self) -> Value {
        serde_json::json!({
            "provider_response_id": self.response_id,
            "provider_request_hash": self.request_hash,
            "provider_payload_hash": self.payload_hash,
        })
    }
}

/// Every provider citation that canonicalizes to one source URL. Aggregating
/// before acquisition keeps `required_source_count`, the concatenated bundle and
/// the completeness denominator counted per source document, not per citation.
struct AggregatedSource {
    canonical_url: String,
    provider_urls: Vec<String>,
    excerpts: Vec<String>,
}

struct SourceDocumentResult {
    source: AggregatedSource,
    fetched: Result<SourceDocumentSnapshot, SourceDocumentFetchError>,
}

fn aggregate_sources_by_canonical_url(
    citations: &[NativeWebCitation],
) -> Result<Vec<AggregatedSource>, EvidenceAdapterError> {
    let mut order = Vec::<AggregatedSource>::new();
    let mut index = BTreeMap::<String, usize>::new();
    for citation in citations {
        let canonical_url = canonical_source_url(&citation.uri)?;
        let position = *index.entry(canonical_url.clone()).or_insert_with(|| {
            order.push(AggregatedSource {
                canonical_url,
                provider_urls: Vec::new(),
                excerpts: Vec::new(),
            });
            order.len() - 1
        });
        let source = &mut order[position];
        if !source.provider_urls.contains(&citation.uri) {
            source.provider_urls.push(citation.uri.clone());
        }
        if let Some(excerpt) = citation
            .excerpt
            .as_deref()
            .map(str::trim)
            .filter(|excerpt| !excerpt.is_empty())
        {
            let excerpt = excerpt.to_owned();
            if !source.excerpts.contains(&excerpt) {
                source.excerpts.push(excerpt);
            }
        }
    }
    for source in &mut order {
        source.provider_urls.sort();
        source.excerpts.sort();
    }
    Ok(order)
}

fn canonical_source_url(uri: &str) -> Result<String, EvidenceAdapterError> {
    let mut parsed = reqwest::Url::parse(uri)
        .map_err(|error| EvidenceAdapterError::Transport(error.to_string()))?;
    parsed.set_fragment(None);
    Ok(parsed.to_string())
}

fn source_snapshot_identity(canonical_url: &str, discriminator: &str) -> String {
    let mut identity = Vec::with_capacity(canonical_url.len() + discriminator.len() + 1);
    identity.extend_from_slice(canonical_url.as_bytes());
    identity.push(0);
    identity.extend_from_slice(discriminator.as_bytes());
    ContentHash::of_bytes(&identity).to_string()
}

impl SourceMaterialization {
    fn provider_attributed(
        raw: Vec<u8>,
        citations: &[NativeWebCitation],
        primary: &NativeWebCitation,
        snapshot_error: Option<String>,
        identity: &ProviderSearchIdentity,
    ) -> Result<Self, EvidenceAdapterError> {
        let required_source_count = aggregate_sources_by_canonical_url(citations)?.len();
        let citations = citations
            .iter()
            .map(|citation| {
                let needle = citation.uri.as_bytes();
                let start_byte = raw
                    .windows(needle.len())
                    .position(|window| window == needle)
                    .ok_or_else(|| {
                        EvidenceAdapterError::Transport(
                            "native web citation missing retained provider payload".to_owned(),
                        )
                    })?;
                Ok(EvidenceCitation {
                    start_byte,
                    end_byte: start_byte + needle.len(),
                    quote: citation.uri.clone(),
                })
            })
            .collect::<Result<Vec<_>, EvidenceAdapterError>>()?;

        let mut metadata = serde_json::json!({
            "status": "provider_attributed_unverified",
            "snapshot_error": snapshot_error,
            "acquisition_mode": EvidenceAcquisitionMode::DiscoveryOnly.as_str(),
            "acquisition_policy_version": akzio_domain::EVIDENCE_ACQUISITION_POLICY_VERSION,
            "acquisition_policy_hash": akzio_domain::evidence_acquisition_policy_hash().to_string(),
            "source_closure": "provider_attributed",
            "required_source_count": required_source_count,
            "verified_source_count": 0,
            "fetch_count": 0,
            "exact_quote_count": 0,
            "search_completed_at": identity.searched_at,
        });
        merge_object(&mut metadata, identity.provenance());

        Ok(Self {
            raw,
            media_type: "application/json".to_owned(),
            observed_at: identity.searched_at,
            citations,
            quality: EvidenceQuality {
                completeness_ppm: 250_000,
                citations_complete: false,
                normalized: true,
            },
            revision: primary
                .revision
                .clone()
                .or_else(|| Some(identity.payload_hash.clone())),
            dedupe_key: format!(
                "native-web:{}:{}",
                identity.request_hash, identity.payload_hash
            ),
            metadata,
        })
    }

    fn source_documents(
        provider_raw: Vec<u8>,
        documents: Vec<SourceDocumentResult>,
        identity: &ProviderSearchIdentity,
    ) -> Result<Self, EvidenceAdapterError> {
        let source_count = documents.len();
        let mut raw = Vec::new();
        let mut citations = Vec::new();
        let mut source_artifacts = Vec::with_capacity(source_count);
        let mut identity_parts = Vec::with_capacity(source_count);
        let mut observed_at = None;
        let mut successful_sources = 0_usize;
        let mut verified_sources = 0_usize;
        let mut exact_quote_count = 0_usize;
        let mut sole_media_type = None;

        for SourceDocumentResult {
            source:
                AggregatedSource {
                    canonical_url,
                    provider_urls,
                    excerpts,
                },
            fetched,
        } in documents
        {
            match fetched {
                Ok(snapshot) => {
                    successful_sources += 1;
                    observed_at = Some(
                        observed_at.map_or(snapshot.fetched_at, |current: DateTime<Utc>| {
                            current.max(snapshot.fetched_at)
                        }),
                    );
                    sole_media_type.get_or_insert_with(|| snapshot.media_type.clone());

                    let source_start_byte = raw.len();
                    raw.extend_from_slice(&snapshot.body);
                    let source_end_byte = raw.len();
                    let content_hash = ContentHash::of_bytes(&snapshot.body).to_string();
                    let snapshot_id = source_snapshot_identity(&canonical_url, &content_hash);

                    // Every excerpt attributed to this source document must land
                    // exactly, otherwise the source stays unverified.
                    let mut claim_bindings = Vec::with_capacity(excerpts.len());
                    for excerpt in &excerpts {
                        let Some(relative_start) = Some(excerpt.as_str())
                            .filter(|quote| {
                                (MIN_SOURCE_QUOTE_BYTES..=MAX_SOURCE_QUOTE_BYTES)
                                    .contains(&quote.len())
                            })
                            .and_then(|quote| {
                                snapshot
                                    .body
                                    .windows(quote.len())
                                    .position(|window| window == quote.as_bytes())
                            })
                        else {
                            claim_bindings.clear();
                            break;
                        };
                        let start_byte = source_start_byte + relative_start;
                        let end_byte = start_byte + excerpt.len();
                        claim_bindings.push(serde_json::json!({
                            "status": "exact_quote",
                            "quote": excerpt,
                            "source_start_byte": relative_start,
                            "source_end_byte": relative_start + excerpt.len(),
                            "bundle_start_byte": start_byte,
                            "bundle_end_byte": end_byte,
                        }));
                    }
                    if claim_bindings.is_empty() {
                        claim_bindings.push(serde_json::json!({
                            "status": "missing_exact_quote",
                        }));
                    } else {
                        verified_sources += 1;
                        exact_quote_count += claim_bindings.len();
                        for binding in &claim_bindings {
                            citations.push(EvidenceCitation {
                                start_byte: binding_byte(binding, "bundle_start_byte")?,
                                end_byte: binding_byte(binding, "bundle_end_byte")?,
                                quote: binding
                                    .get("quote")
                                    .and_then(Value::as_str)
                                    .unwrap_or_default()
                                    .to_owned(),
                            });
                        }
                    }

                    identity_parts.push(serde_json::json!({
                        "canonical_url": canonical_url,
                        "snapshot_id": snapshot_id,
                        "content_hash": content_hash,
                    }));
                    source_artifacts.push(serde_json::json!({
                        "status": "snapshot",
                        "provider_url": provider_urls.first(),
                        "provider_urls": provider_urls,
                        "canonical_url": canonical_url,
                        "snapshot_id": snapshot_id,
                        "content_hash": content_hash,
                        "media_type": snapshot.media_type,
                        "body_bytes": snapshot.body.len(),
                        "bundle_start_byte": source_start_byte,
                        "bundle_end_byte": source_end_byte,
                        "status_code": snapshot.status_code,
                        "fetched_at": snapshot.fetched_at,
                        "etag": snapshot.etag,
                        "last_modified": snapshot.last_modified,
                        "claim_binding": claim_bindings[0],
                        "claim_bindings": claim_bindings,
                    }));
                }
                Err(error) => {
                    let failure_identity =
                        source_snapshot_identity(&canonical_url, error.kind.as_str());
                    identity_parts.push(serde_json::json!({
                        "canonical_url": canonical_url,
                        "failure_kind": error.kind.as_str(),
                        "failure_identity": failure_identity,
                    }));
                    source_artifacts.push(serde_json::json!({
                        "status": "fetch_failed",
                        "provider_url": provider_urls.first(),
                        "provider_urls": provider_urls,
                        "canonical_url": canonical_url,
                        "failure_kind": error.kind.as_str(),
                        "failure_identity": failure_identity,
                        "status_code": error.status_code,
                        "message": error.message,
                    }));
                }
            }
        }

        let identity_bytes = serde_json::to_vec(&identity_parts)
            .map_err(|error| EvidenceAdapterError::Transport(error.to_string()))?;
        let revision = ContentHash::of_bytes(&identity_bytes).to_string();
        let citations_complete = source_count > 0 && verified_sources == source_count;
        let completeness_ppm =
            u32::try_from(verified_sources.saturating_mul(1_000_000) / source_count.max(1))
                .unwrap_or(1_000_000);
        let status = if citations_complete {
            "source_snapshots_complete"
        } else if successful_sources > 0 {
            "source_snapshots_partial"
        } else {
            "provider_attributed_unverified"
        };
        let raw = if successful_sources > 0 {
            raw
        } else {
            provider_raw
        };
        let media_type = match successful_sources {
            0 => "application/json".to_owned(),
            1 => sole_media_type.unwrap_or_else(|| "application/octet-stream".to_owned()),
            _ => "application/vnd.akzio.news-web-source-bundle".to_owned(),
        };
        let mut metadata = serde_json::json!({
            "status": status,
            "source_count": source_count,
            "successful_source_count": successful_sources,
            "verified_source_count": verified_sources,
            "acquisition_mode": EvidenceAcquisitionMode::VerifiedSource.as_str(),
            "acquisition_policy_version": akzio_domain::EVIDENCE_ACQUISITION_POLICY_VERSION,
            "acquisition_policy_hash": akzio_domain::evidence_acquisition_policy_hash().to_string(),
            "source_closure": if citations_complete {
                "complete"
            } else if successful_sources > 0 {
                "partial"
            } else {
                "provider_attributed"
            },
            "required_source_count": source_count,
            "fetch_count": source_count,
            "exact_quote_count": exact_quote_count,
            "sources": source_artifacts,
        });
        merge_object(&mut metadata, identity.provenance());

        Ok(Self {
            raw,
            media_type,
            observed_at: observed_at.unwrap_or(identity.searched_at),
            citations,
            quality: EvidenceQuality {
                completeness_ppm,
                citations_complete,
                normalized: true,
            },
            revision: Some(revision.clone()),
            dedupe_key: format!("source-document-set:{revision}"),
            metadata,
        })
    }
}

fn binding_byte(binding: &Value, field: &str) -> Result<usize, EvidenceAdapterError> {
    claim_binding_byte(binding, field)
        .ok_or_else(|| EvidenceAdapterError::Transport(format!("claim binding is missing {field}")))
}

fn merge_object(target: &mut Value, extra: Value) {
    let (Some(target), Some(extra)) = (target.as_object_mut(), extra.as_object()) else {
        return;
    };
    for (key, value) in extra {
        target.insert(key.clone(), value.clone());
    }
}

/// Comma-joined failure kinds for telemetry only; the durable record keeps the
/// per-source failure detail.
fn fetch_failure_kinds(metadata: &Value) -> String {
    metadata
        .get("sources")
        .and_then(Value::as_array)
        .map(|sources| {
            sources
                .iter()
                .filter_map(|source| source.get("failure_kind").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default()
}

#[derive(Clone)]
pub(crate) struct ModelNativeWebEvidenceTransport {
    client: ModelClient,
    policy: NativeWebPolicy,
    source: EvidenceSource,
    source_document: Option<Arc<dyn SourceDocumentFetcher>>,
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
    pub(crate) fn for_source(
        client: ModelClient,
        source: EvidenceSource,
    ) -> Result<Self, EvidenceAdapterError> {
        let source_document = (source == EvidenceSource::NewsWeb)
            .then(HttpSourceDocumentFetcher::new)
            .transpose()?
            .map(|fetcher| Arc::new(fetcher) as Arc<dyn SourceDocumentFetcher>);
        Ok(Self::new(client, source, source_document))
    }

    #[cfg(test)]
    pub(super) fn for_source_with_fetcher(
        client: ModelClient,
        source: EvidenceSource,
        fetcher: Arc<dyn SourceDocumentFetcher>,
    ) -> Self {
        Self::new(client, source, Some(fetcher))
    }

    /// Provider-mediated transport with no independent fetcher at all. A
    /// `VerifiedSource` request against it must fail closed.
    #[cfg(test)]
    pub(super) fn for_source_without_fetcher(client: ModelClient, source: EvidenceSource) -> Self {
        Self::new(client, source, None)
    }

    fn new(
        client: ModelClient,
        source: EvidenceSource,
        source_document: Option<Arc<dyn SourceDocumentFetcher>>,
    ) -> Self {
        Self {
            client,
            policy: NativeWebPolicy {
                allowed_hosts: match source {
                    EvidenceSource::SecEdgar => {
                        vec!["sec.gov".to_owned(), "www.sec.gov".to_owned()]
                    }
                    EvidenceSource::Fred => vec!["fred.stlouisfed.org".to_owned()],
                    EvidenceSource::NewsWeb => vec![
                        "reuters.com".to_owned(),
                        "www.reuters.com".to_owned(),
                        "apnews.com".to_owned(),
                        "www.apnews.com".to_owned(),
                        "etfchannel.com".to_owned(),
                        "m.etfchannel.com".to_owned(),
                        "www.etfchannel.com".to_owned(),
                        "etf.com".to_owned(),
                        "www.etf.com".to_owned(),
                    ],
                    EvidenceSource::Alpaca => Vec::new(),
                },
                ..NativeWebPolicy::default()
            },
            source,
            source_document,
        }
    }

    async fn acquire_inner(
        &self,
        source: EvidenceSource,
        resource: &str,
        acquisition_mode: EvidenceAcquisitionMode,
    ) -> Result<AcquiredEvidence, EvidenceAdapterError> {
        if source == EvidenceSource::Alpaca {
            return Err(EvidenceAdapterError::SourceMismatch);
        }
        let request = ModelRequest {
            instructions: "Use only the Rust-approved native web tool. Return verifiable citations for every material fact.".to_owned(),
            input: ModelInput::Fresh {
                text: serde_json::json!({
                    "source_family": source.as_str(),
                    "research_intent": resource,
                })
                .to_string(),
            },
            max_output_tokens: 2_000,
            tools: vec![self.policy.tool_definition()],
            tool_choice: ModelToolChoice::Auto,
            fixture_key: None,
        };
        // One acquisition performs exactly one provider request; the response may
        // contain any provider search action the Rust policy already accepts.
        let search_started = std::time::Instant::now();
        let response = self
            .client
            .respond(request)
            .await
            .map_err(|error| model_error(error, source, resource))?;
        let provider_search_ms = search_started.elapsed().as_millis();
        self.policy
            .validate_provider_response(&response.raw)
            .map_err(|error| model_error(error, source, resource))?;
        if !response.tool_calls.is_empty() {
            self.policy
                .validate_tool_calls(&response.tool_calls)
                .map_err(|error| model_error(error, source, resource))?;
        }
        let citations = self
            .policy
            .extract_citations(&response.raw)
            .map_err(|error| model_error(error, source, resource))?;
        // Full URL safety closes before any network request, and covers every
        // citation rather than only the one that becomes `source_uri`.
        for citation in &citations {
            if !governed_source_uri_is_safe(&citation.uri) {
                return Err(EvidenceAdapterError::Policy {
                    evidence_source: source,
                    resource: resource.to_owned(),
                    reason: format!(
                        "citation URI carries credentials, a fragment, or a sensitive query: {}",
                        citation.uri
                    ),
                });
            }
        }
        let primary = citations
            .iter()
            .find(|citation| {
                citation
                    .excerpt
                    .as_deref()
                    .is_some_and(|quote| !quote.trim().is_empty())
            })
            .or_else(|| citations.first())
            .cloned()
            .ok_or_else(|| EvidenceAdapterError::Transport("missing citation URI".to_owned()))?;
        let identity = ProviderSearchIdentity::new(&response.raw, &response.request_body)?;
        let provider_value = serde_json::json!({
            "source_family": source,
            "resource": resource,
            "provider_request": response.request_body,
            "output_text": response.output_text,
            "citations": citations,
            "provider_result": response.raw,
        });
        let provider_raw = serde_json::to_vec(&provider_value)
            .map_err(|error| EvidenceAdapterError::Transport(error.to_string()))?;
        let fetch_started = std::time::Instant::now();
        let materialization = if acquisition_mode.requires_independent_fetch() {
            let fetcher =
                self.source_document
                    .as_ref()
                    .ok_or_else(|| EvidenceAdapterError::Policy {
                        evidence_source: source,
                        resource: resource.to_owned(),
                        reason: "VerifiedSource acquisition has no independent source fetcher"
                            .to_owned(),
                    })?;
            let aggregated = aggregate_sources_by_canonical_url(&citations)?;
            let mut documents = Vec::with_capacity(aggregated.len());
            for source in aggregated {
                let fetched = fetcher.fetch(&source.canonical_url).await;
                documents.push(SourceDocumentResult { source, fetched });
            }
            SourceMaterialization::source_documents(provider_raw, documents, &identity)?
        } else {
            SourceMaterialization::provider_attributed(
                provider_raw,
                &citations,
                &primary,
                None,
                &identity,
            )?
        };
        let SourceMaterialization {
            raw,
            media_type,
            observed_at,
            citations: provenance_citations,
            quality,
            revision,
            dedupe_key,
            metadata,
        } = materialization;
        let metadata_count = |field: &str| -> u64 {
            metadata
                .get(field)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default()
        };
        tracing::debug!(
            evidence_source = source.as_str(),
            resource,
            acquisition_mode = acquisition_mode.as_str(),
            provider_search_ms = provider_search_ms as u64,
            independent_fetch_ms = fetch_started.elapsed().as_millis() as u64,
            fetch_count = metadata_count("fetch_count"),
            verified_source_count = metadata_count("verified_source_count"),
            required_source_count = metadata_count("required_source_count"),
            exact_quote_count = metadata_count("exact_quote_count"),
            fetch_failure_kinds = %fetch_failure_kinds(&metadata),
            source_closure = metadata
                .get("source_closure")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default(),
            "native web evidence acquired"
        );
        let source_uri = primary.uri.clone();
        let mut normalized = provider_value;
        normalized
            .as_object_mut()
            .expect("provider envelope is an object")
            .insert("source_document".to_owned(), metadata);

        Ok(AcquiredEvidence {
            raw,
            media_type,
            source_uri: source_uri.clone(),
            observed_at,
            normalized,
            provenance: EvidenceProvenance {
                document_id: primary
                    .document_id
                    .clone()
                    .or_else(|| Some(source_uri.clone())),
                published_at: primary
                    .published_at
                    .as_deref()
                    .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
                    .map(|value| value.with_timezone(&Utc)),
                observed_at,
                revision,
                source_uri,
                dedupe_key,
                citations: provenance_citations,
            },
            quality,
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
            self.acquire_inner(request.source, &request.resource, request.acquisition_mode)
                .await
        })
    }
}

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

impl AsyncEvidenceAdapter for FixtureEvidenceAdapter {
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
            self.responses
                .get(&request.resource)
                .cloned()
                .ok_or_else(|| EvidenceAdapterError::MissingFixture(request.resource.clone()))
        })
    }
}

#[cfg(test)]
mod source_document_fetcher_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn serve_once(response: &'static [u8]) -> String {
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 1_024];
            let _ = socket.read(&mut request).await;
            socket.write_all(response).await.unwrap();
        });
        format!("http://{address}/source")
    }

    #[tokio::test]
    async fn source_document_fetcher_rejects_redirects_and_non_success_statuses() {
        let cases: &[(&[u8], SourceDocumentFailureKind, u16)] = &[
            (
                b"HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:9/blocked\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                SourceDocumentFailureKind::Redirect,
                302,
            ),
            (
                b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                SourceDocumentFailureKind::HttpStatus,
                503,
            ),
        ];

        for (response, expected_kind, expected_status) in cases {
            let uri = serve_once(response).await;
            let error = HttpSourceDocumentFetcher::new()
                .unwrap()
                .fetch(&uri)
                .await
                .unwrap_err();

            assert_eq!(&error.kind, expected_kind);
            assert_eq!(error.status_code, Some(*expected_status));
            assert!(error.message.contains(&format!("HTTP {expected_status}")));
        }
    }

    #[tokio::test]
    async fn source_document_fetcher_rejects_declared_oversize_bodies() {
        let uri = serve_once(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 2097153\r\nConnection: close\r\n\r\n",
        )
        .await;
        let error = HttpSourceDocumentFetcher::new()
            .unwrap()
            .fetch(&uri)
            .await
            .unwrap_err();

        assert_eq!(error.kind, SourceDocumentFailureKind::BodyTooLarge);
        assert!(error.message.contains("byte limit"));
    }

    #[tokio::test]
    async fn source_document_fetcher_retains_exact_response_body() {
        let uri = serve_once(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nETag: fixture-etag\r\nContent-Length: 8\r\nConnection: close\r\n\r\nevidence",
        )
        .await;
        let snapshot = HttpSourceDocumentFetcher::new()
            .unwrap()
            .fetch(&uri)
            .await
            .unwrap();

        assert_eq!(snapshot.body, b"evidence");
        assert_eq!(snapshot.media_type, "text/html");
        assert_eq!(snapshot.status_code, 200);
        assert_eq!(snapshot.etag.as_deref(), Some("fixture-etag"));
    }

    #[tokio::test]
    async fn source_document_fetcher_rejects_empty_bodies() {
        let uri = serve_once(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )
        .await;
        let error = HttpSourceDocumentFetcher::new()
            .unwrap()
            .fetch(&uri)
            .await
            .unwrap_err();

        assert_eq!(error.kind, SourceDocumentFailureKind::EmptyBody);
    }

    #[tokio::test]
    async fn source_document_fetcher_retains_both_validator_headers() {
        let uri = serve_once(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nETag: \"v7\"\r\nLast-Modified: Sat, 29 Aug 2026 12:00:00 GMT\r\nContent-Length: 6\r\nConnection: close\r\n\r\nsealed",
        )
        .await;
        let snapshot = HttpSourceDocumentFetcher::new()
            .unwrap()
            .fetch(&uri)
            .await
            .unwrap();

        assert_eq!(snapshot.etag.as_deref(), Some("\"v7\""));
        assert_eq!(
            snapshot.last_modified.as_deref(),
            Some("Sat, 29 Aug 2026 12:00:00 GMT")
        );
    }

    /// An undeclared body cannot be trusted to stop at the limit, so the stream
    /// itself must be bounded rather than the `Content-Length` header alone.
    #[tokio::test]
    async fn source_document_fetcher_bounds_undeclared_streamed_bodies() {
        static CHUNK: [u8; 65_536] = [b'a'; 65_536];
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 1_024];
            let _ = socket.read(&mut request).await;
            if socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nConnection: close\r\n\r\n",
                )
                .await
                .is_err()
            {
                return;
            }
            // 33 x 64 KiB is just past the 2 MiB ceiling.
            for _ in 0..33 {
                if socket.write_all(&CHUNK).await.is_err() {
                    return;
                }
            }
        });
        let error = HttpSourceDocumentFetcher::new()
            .unwrap()
            .fetch(&format!("http://{address}/source"))
            .await
            .unwrap_err();

        assert_eq!(error.kind, SourceDocumentFailureKind::BodyTooLarge);
    }

    #[test]
    fn canonical_aggregation_collapses_fragment_variants_and_duplicates() {
        let citation = |uri: &str, excerpt: &str| NativeWebCitation {
            uri: uri.to_owned(),
            title: None,
            excerpt: Some(excerpt.to_owned()),
            published_at: None,
            revision: None,
            document_id: None,
        };
        let aggregated = aggregate_sources_by_canonical_url(&[
            citation("https://www.reuters.com/story#first", "first quote"),
            citation("https://www.reuters.com/story#second", "second quote"),
            citation("https://www.reuters.com/story", "first quote"),
            citation("https://apnews.com/story", "other quote"),
        ])
        .unwrap();

        assert_eq!(aggregated.len(), 2);
        assert_eq!(aggregated[0].canonical_url, "https://www.reuters.com/story");
        assert_eq!(aggregated[0].provider_urls.len(), 3);
        assert_eq!(aggregated[0].excerpts, ["first quote", "second quote"]);
        assert_eq!(aggregated[1].canonical_url, "https://apnews.com/story");
        assert_eq!(aggregated[1].excerpts, ["other quote"]);
    }
}
