use std::{env, sync::Arc, time::Duration};

use akzio_domain::ContentHash;
use chrono::Utc;
use futures::future::BoxFuture;
use reqwest::{header, Client, Url};
use serde_json::{json, Value};
use tokio::{sync::Mutex, time::Instant};

use crate::runtime::{
    AcquiredEvidence, AsyncEvidenceAdapter, EvidenceAdapterError, EvidenceProvenance,
    EvidenceQuality, EvidenceRequest, EvidenceSource, GovernedResource,
};

const SEC_DATA_BASE: &str = "https://data.sec.gov";
const SEC_ARCHIVES_BASE: &str = "https://www.sec.gov";
const FRED_BASE: &str = "https://api.stlouisfed.org";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FredPayloadKind {
    Observations,
    Vintages,
    ReleaseCalendar,
}

#[derive(Debug)]
struct RateGate {
    next: Mutex<Instant>,
    interval: Duration,
}

impl RateGate {
    fn new(interval: Duration) -> Self {
        Self {
            next: Mutex::new(Instant::now()),
            interval,
        }
    }

    async fn wait(&self) {
        let mut next = self.next.lock().await;
        let now = Instant::now();
        if *next > now {
            tokio::time::sleep_until(*next).await;
        }
        *next = Instant::now() + self.interval;
    }
}

#[derive(Clone)]
pub struct SecEdgarDirectTransport {
    client: Client,
    user_agent: String,
    gate: Arc<RateGate>,
}

impl std::fmt::Debug for SecEdgarDirectTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecEdgarDirectTransport")
            .field("user_agent", &"<redacted>")
            .finish()
    }
}

impl SecEdgarDirectTransport {
    pub fn from_env() -> Result<Self, EvidenceAdapterError> {
        let user_agent = env::var("SEC_USER_AGENT")
            .map_err(|_| EvidenceAdapterError::Transport("SEC_USER_AGENT is not set".to_owned()))?;
        Self::new(user_agent)
    }

    pub fn new(user_agent: impl Into<String>) -> Result<Self, EvidenceAdapterError> {
        let user_agent = user_agent.into();
        if user_agent.trim().is_empty()
            || user_agent.len() > 256
            || user_agent.contains(['\r', '\n'])
        {
            return Err(EvidenceAdapterError::Transport(
                "SEC_USER_AGENT is invalid".to_owned(),
            ));
        }
        let client = Client::builder()
            .http1_only()
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(|_| EvidenceAdapterError::Transport("SEC client setup failed".to_owned()))?;
        Ok(Self {
            client,
            user_agent,
            // Below the SEC's published fair-access ceiling.
            gate: Arc::new(RateGate::new(Duration::from_millis(125))),
        })
    }

    fn request_for(resource: &str) -> Result<(Url, bool), EvidenceAdapterError> {
        let parsed = GovernedResource::parse(EvidenceSource::SecEdgar, resource)
            .map_err(|_| EvidenceAdapterError::DataQuality("invalid SEC resource".to_owned()))?;
        let (url, json_body) = match parsed {
            GovernedResource::SecSubmissions { cik } => {
                (format!("{SEC_DATA_BASE}/submissions/CIK{cik}.json"), true)
            }
            GovernedResource::SecCompanyFacts { cik } => (
                format!("{SEC_DATA_BASE}/api/xbrl/companyfacts/CIK{cik}.json"),
                true,
            ),
            GovernedResource::SecFiling {
                cik,
                accession,
                primary_document,
            } => (
                format!(
                    "{SEC_ARCHIVES_BASE}/Archives/edgar/data/{}/{}/{}",
                    cik.trim_start_matches('0'),
                    accession.replace('-', ""),
                    primary_document
                ),
                false,
            ),
            _ => {
                return Err(EvidenceAdapterError::Transport(
                    "invalid SEC resource".to_owned(),
                ));
            }
        };
        Url::parse(&url)
            .map(|url| (url, json_body))
            .map_err(|_| EvidenceAdapterError::DataQuality("invalid SEC resource".to_owned()))
    }

    async fn acquire_inner(
        &self,
        source: EvidenceSource,
        resource: &str,
    ) -> Result<AcquiredEvidence, EvidenceAdapterError> {
        if source != EvidenceSource::SecEdgar {
            return Err(EvidenceAdapterError::SourceMismatch);
        }
        let (url, json_body) = Self::request_for(resource)?;
        self.gate.wait().await;
        let response = self
            .client
            .get(url.clone())
            .header(header::USER_AGENT, &self.user_agent)
            .header(header::ACCEPT_ENCODING, "gzip, deflate")
            .send()
            .await
            .map_err(|_| EvidenceAdapterError::Transport("SEC request failed".to_owned()))?;
        crate::runtime::classify_evidence_response(&response)?;
        let revision = response
            .headers()
            .get(header::ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let body = response
            .bytes()
            .await
            .map_err(|_| EvidenceAdapterError::Transport("SEC response read failed".to_owned()))?;
        if body.is_empty() {
            return Err(EvidenceAdapterError::Transport(
                "SEC returned an empty response".to_owned(),
            ));
        }
        let normalized = if json_body {
            let value: Value = serde_json::from_slice(&body)
                .map_err(|_| EvidenceAdapterError::DataQuality("invalid SEC JSON".to_owned()))?;
            validate_sec_payload(resource, &value)?;
            value
        } else {
            json!({
                "resource": resource,
                "bytes": body.len(),
                "content_hash": ContentHash::of_bytes(&body),
            })
        };
        let observed_at = Utc::now();
        let source_uri = url.to_string();
        Ok(AcquiredEvidence {
            raw: body.to_vec(),
            media_type: if json_body {
                "application/json".to_owned()
            } else {
                "text/html".to_owned()
            },
            source_uri: source_uri.clone(),
            observed_at,
            normalized,
            provenance: EvidenceProvenance {
                document_id: Some(resource.to_owned()),
                published_at: None,
                observed_at,
                revision,
                source_uri,
                dedupe_key: format!("sec:{}", ContentHash::of_bytes(&body)),
                citations: vec![],
            },
            quality: EvidenceQuality::default(),
        })
    }
}

impl AsyncEvidenceAdapter for SecEdgarDirectTransport {
    fn source(&self) -> EvidenceSource {
        EvidenceSource::SecEdgar
    }

    fn acquire<'a>(
        &'a self,
        request: &'a EvidenceRequest,
    ) -> BoxFuture<'a, Result<AcquiredEvidence, EvidenceAdapterError>> {
        Box::pin(async move {
            if request.source != EvidenceSource::SecEdgar {
                return Err(EvidenceAdapterError::SourceMismatch);
            }
            self.acquire_inner(request.source, &request.resource).await
        })
    }
}

#[derive(Clone)]
pub struct FredDirectTransport {
    client: Client,
    api_key: String,
    gate: Arc<RateGate>,
}

impl std::fmt::Debug for FredDirectTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FredDirectTransport")
            .field("api_key", &"<redacted>")
            .finish()
    }
}

impl FredDirectTransport {
    pub fn from_env() -> Result<Self, EvidenceAdapterError> {
        let api_key = env::var("FRED_API_KEY")
            .map_err(|_| EvidenceAdapterError::Transport("FRED_API_KEY is not set".to_owned()))?;
        Self::new(api_key)
    }

    pub fn new(api_key: impl Into<String>) -> Result<Self, EvidenceAdapterError> {
        let api_key = api_key.into();
        if api_key.trim().is_empty() || api_key.len() > 128 || api_key.contains(['\r', '\n']) {
            return Err(EvidenceAdapterError::Transport(
                "FRED_API_KEY is invalid".to_owned(),
            ));
        }
        let client = Client::builder()
            .http1_only()
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(|_| EvidenceAdapterError::Transport("FRED client setup failed".to_owned()))?;
        Ok(Self {
            client,
            api_key,
            gate: Arc::new(RateGate::new(Duration::from_millis(250))),
        })
    }

    fn request_for(resource: &str) -> Result<(Url, Url, FredPayloadKind), EvidenceAdapterError> {
        let parsed = GovernedResource::parse(EvidenceSource::Fred, resource)
            .map_err(|_| EvidenceAdapterError::DataQuality("invalid FRED resource".to_owned()))?;
        let (path, series_id, start, end, vintage, kind) = match parsed {
            GovernedResource::Fred {
                series_id,
                window_start,
                window_end,
                vintage,
            } => (
                "/fred/series/observations",
                series_id,
                window_start,
                window_end,
                vintage,
                FredPayloadKind::Observations,
            ),
            GovernedResource::FredVintages {
                series_id,
                window_start,
                window_end,
            } => (
                "/fred/series/vintagedates",
                series_id,
                window_start,
                window_end,
                None,
                FredPayloadKind::Vintages,
            ),
            GovernedResource::FredReleaseCalendar {
                window_start,
                window_end,
                vintage,
            } => (
                "/fred/releases/dates",
                String::new(),
                Some(window_start),
                Some(window_end),
                Some(vintage),
                FredPayloadKind::ReleaseCalendar,
            ),
            _ => {
                return Err(EvidenceAdapterError::Transport(
                    "invalid FRED resource".to_owned(),
                ));
            }
        };
        let mut public_url = Url::parse(FRED_BASE)
            .map_err(|_| EvidenceAdapterError::Transport("FRED URL setup failed".to_owned()))?;
        public_url.set_path(path);
        {
            let mut query = public_url.query_pairs_mut();
            if !series_id.is_empty() {
                query.append_pair("series_id", &series_id);
            }
            query.append_pair("file_type", "json");
            if kind == FredPayloadKind::Observations {
                if let Some(start) = start {
                    query.append_pair("observation_start", &start.to_string());
                }
                if let Some(end) = end {
                    query.append_pair("observation_end", &end.to_string());
                }
            } else if kind == FredPayloadKind::Vintages {
                if let Some(start) = start {
                    query.append_pair("realtime_start", &start.to_string());
                }
                if let Some(end) = end {
                    query.append_pair("realtime_end", &end.to_string());
                }
            } else {
                query.append_pair("include_release_dates_with_no_data", "true");
                query.append_pair("limit", "1000");
                query.append_pair("order_by", "release_date");
                query.append_pair("sort_order", "desc");
            }
            if let Some(vintage) = vintage {
                query.append_pair("realtime_start", &vintage.to_string());
                query.append_pair("realtime_end", &vintage.to_string());
            }
        }
        let request_url = public_url.clone();
        Ok((request_url, public_url, kind))
    }

    async fn acquire_inner(
        &self,
        source: EvidenceSource,
        resource: &str,
    ) -> Result<AcquiredEvidence, EvidenceAdapterError> {
        if source != EvidenceSource::Fred {
            return Err(EvidenceAdapterError::SourceMismatch);
        }
        let (mut request_url, public_url, kind) = Self::request_for(resource)?;
        request_url
            .query_pairs_mut()
            .append_pair("api_key", &self.api_key);
        self.gate.wait().await;
        let response = self
            .client
            .get(request_url)
            .send()
            .await
            .map_err(|_| EvidenceAdapterError::Transport("FRED request failed".to_owned()))?;
        crate::runtime::classify_evidence_response(&response)?;
        let body = response
            .bytes()
            .await
            .map_err(|_| EvidenceAdapterError::Transport("FRED response read failed".to_owned()))?;
        let mut normalized: Value = serde_json::from_slice(&body)
            .map_err(|_| EvidenceAdapterError::DataQuality("invalid FRED JSON".to_owned()))?;
        validate_fred_payload(kind, &normalized)?;
        let governed = GovernedResource::parse(EvidenceSource::Fred, resource)
            .map_err(|_| EvidenceAdapterError::DataQuality("invalid FRED resource".to_owned()))?;
        if let GovernedResource::Fred {
            vintage: Some(vintage),
            ..
        }
        | GovernedResource::FredReleaseCalendar { vintage, .. } = &governed
        {
            validate_fred_vintage(*vintage, &normalized)?;
        }
        if let GovernedResource::FredReleaseCalendar {
            window_start,
            window_end,
            ..
        } = &governed
        {
            let dates = normalized
                .get_mut("release_dates")
                .and_then(Value::as_array_mut)
                .ok_or_else(|| {
                    EvidenceAdapterError::DataQuality("invalid FRED release calendar".to_owned())
                })?;
            dates.retain(|entry| {
                entry
                    .get("date")
                    .and_then(Value::as_str)
                    .and_then(|value| chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").ok())
                    .is_some_and(|date| (*window_start..=*window_end).contains(&date))
            });
        }
        let observed_at = Utc::now();
        let source_uri = public_url.to_string();
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
                dedupe_key: format!("fred:{}", ContentHash::of_bytes(&body)),
                citations: vec![],
            },
            quality: EvidenceQuality::default(),
        })
    }
}

impl AsyncEvidenceAdapter for FredDirectTransport {
    fn source(&self) -> EvidenceSource {
        EvidenceSource::Fred
    }

    fn acquire<'a>(
        &'a self,
        request: &'a EvidenceRequest,
    ) -> BoxFuture<'a, Result<AcquiredEvidence, EvidenceAdapterError>> {
        Box::pin(async move {
            if request.source != EvidenceSource::Fred {
                return Err(EvidenceAdapterError::SourceMismatch);
            }
            self.acquire_inner(request.source, &request.resource).await
        })
    }
}

fn validate_sec_payload(resource: &str, value: &Value) -> Result<(), EvidenceAdapterError> {
    let valid = if resource.starts_with("companyfacts:") {
        value.get("cik").and_then(Value::as_u64).is_some()
            && value.get("facts").and_then(Value::as_object).is_some()
    } else {
        value.get("cik").and_then(Value::as_str).is_some()
            && value.get("name").and_then(Value::as_str).is_some()
            && value
                .pointer("/filings/recent/form")
                .and_then(Value::as_array)
                .is_some()
            && value
                .pointer("/filings/recent/accessionNumber")
                .and_then(Value::as_array)
                .is_some()
    };
    if valid {
        Ok(())
    } else {
        Err(EvidenceAdapterError::Transport(
            "invalid SEC response shape".to_owned(),
        ))
    }
}

fn validate_fred_payload(kind: FredPayloadKind, value: &Value) -> Result<(), EvidenceAdapterError> {
    let field = match kind {
        FredPayloadKind::Observations => "observations",
        FredPayloadKind::Vintages => "vintage_dates",
        FredPayloadKind::ReleaseCalendar => "release_dates",
    };
    if value.get(field).and_then(Value::as_array).is_some() {
        Ok(())
    } else {
        Err(EvidenceAdapterError::Transport(
            "invalid FRED response shape".to_owned(),
        ))
    }
}

fn validate_fred_vintage(
    vintage: chrono::NaiveDate,
    value: &Value,
) -> Result<(), EvidenceAdapterError> {
    let expected = vintage.to_string();
    if value.get("realtime_start").and_then(Value::as_str) == Some(expected.as_str())
        && value.get("realtime_end").and_then(Value::as_str) == Some(expected.as_str())
    {
        Ok(())
    } else {
        Err(EvidenceAdapterError::Transport(
            "FRED response does not match requested vintage".to_owned(),
        ))
    }
}
