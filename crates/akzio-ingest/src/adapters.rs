use super::*;
use akzio_model::ModelError;
use std::net::{IpAddr, Ipv4Addr};

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

#[derive(Clone)]
pub(crate) struct ModelNativeWebEvidenceTransport {
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
    pub(crate) fn for_source(client: ModelClient, source: EvidenceSource) -> Self {
        let policy = NativeWebPolicy {
            allowed_hosts: match source {
                EvidenceSource::SecEdgar => vec!["sec.gov".to_owned(), "www.sec.gov".to_owned()],
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
        let response = self
            .client
            .respond(request)
            .await
            .map_err(|error| model_error(error, source, resource))?;
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
        let provenance_citations = citations
            .iter()
            .map(|citation| {
                let needle = citation.uri.as_bytes();
                let start_byte = raw
                    .windows(needle.len())
                    .position(|window| window == needle)
                    .ok_or_else(|| {
                        EvidenceAdapterError::Transport(
                            "native web citation missing from retained raw payload".to_owned(),
                        )
                    })?;
                Ok(EvidenceCitation {
                    start_byte,
                    end_byte: start_byte + needle.len(),
                    quote: citation.uri.clone(),
                })
            })
            .collect::<Result<Vec<_>, EvidenceAdapterError>>()?;
        let observed_at = Utc::now();
        let source_uri = citations
            .first()
            .map(|citation| citation.uri.clone())
            .ok_or_else(|| EvidenceAdapterError::Transport("missing citation URI".to_owned()))?;
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
