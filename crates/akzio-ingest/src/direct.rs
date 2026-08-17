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
            .map_err(|_| EvidenceAdapterError::Transport("invalid SEC resource".to_owned()))?;
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
                ))
            }
        };
        Url::parse(&url)
            .map(|url| (url, json_body))
            .map_err(|_| EvidenceAdapterError::Transport("invalid SEC resource".to_owned()))
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
        let status = response.status();
        let revision = response
            .headers()
            .get(header::ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let body = response
            .bytes()
            .await
            .map_err(|_| EvidenceAdapterError::Transport("SEC response read failed".to_owned()))?;
        if !status.is_success() {
            return Err(EvidenceAdapterError::Transport(format!(
                "SEC returned HTTP {}",
                status.as_u16()
            )));
        }
        if body.is_empty() {
            return Err(EvidenceAdapterError::Transport(
                "SEC returned an empty response".to_owned(),
            ));
        }
        let normalized = if json_body {
            let value: Value = serde_json::from_slice(&body)
                .map_err(|_| EvidenceAdapterError::Transport("invalid SEC JSON".to_owned()))?;
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

    fn request_for(resource: &str) -> Result<(Url, Url, bool), EvidenceAdapterError> {
        let parsed = GovernedResource::parse(EvidenceSource::Fred, resource)
            .map_err(|_| EvidenceAdapterError::Transport("invalid FRED resource".to_owned()))?;
        let (path, series_id, start, end, observations) = match parsed {
            GovernedResource::Fred {
                series_id,
                window_start,
                window_end,
            } => (
                "/fred/series/observations",
                series_id,
                window_start,
                window_end,
                true,
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
                false,
            ),
            _ => {
                return Err(EvidenceAdapterError::Transport(
                    "invalid FRED resource".to_owned(),
                ))
            }
        };
        let mut public_url = Url::parse(FRED_BASE)
            .map_err(|_| EvidenceAdapterError::Transport("FRED URL setup failed".to_owned()))?;
        public_url.set_path(path);
        {
            let mut query = public_url.query_pairs_mut();
            query.append_pair("series_id", &series_id);
            query.append_pair("file_type", "json");
            if let Some(start) = start {
                query.append_pair(
                    if observations {
                        "observation_start"
                    } else {
                        "realtime_start"
                    },
                    &start.to_string(),
                );
            }
            if let Some(end) = end {
                query.append_pair(
                    if observations {
                        "observation_end"
                    } else {
                        "realtime_end"
                    },
                    &end.to_string(),
                );
            }
        }
        let request_url = public_url.clone();
        Ok((request_url, public_url, observations))
    }

    async fn acquire_inner(
        &self,
        source: EvidenceSource,
        resource: &str,
    ) -> Result<AcquiredEvidence, EvidenceAdapterError> {
        if source != EvidenceSource::Fred {
            return Err(EvidenceAdapterError::SourceMismatch);
        }
        let (mut request_url, public_url, observations) = Self::request_for(resource)?;
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
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|_| EvidenceAdapterError::Transport("FRED response read failed".to_owned()))?;
        if !status.is_success() {
            return Err(EvidenceAdapterError::Transport(format!(
                "FRED returned HTTP {}",
                status.as_u16()
            )));
        }
        let normalized: Value = serde_json::from_slice(&body)
            .map_err(|_| EvidenceAdapterError::Transport("invalid FRED JSON".to_owned()))?;
        validate_fred_payload(observations, &normalized)?;
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

fn validate_fred_payload(observations: bool, value: &Value) -> Result<(), EvidenceAdapterError> {
    let field = if observations {
        "observations"
    } else {
        "vintage_dates"
    };
    if value.get(field).and_then(Value::as_array).is_some() {
        Ok(())
    } else {
        Err(EvidenceAdapterError::Transport(
            "invalid FRED response shape".to_owned(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sec_resources_map_only_to_official_hosts() {
        let cases = [
            (
                "submissions:CIK320193",
                "https://data.sec.gov/submissions/CIK0000320193.json",
            ),
            (
                "companyfacts:320193",
                "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json",
            ),
            (
                "filing:320193:0000320193-24-000123:a10-k20240928.htm",
                "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/a10-k20240928.htm",
            ),
        ];
        for (resource, expected) in cases {
            let (url, _) = SecEdgarDirectTransport::request_for(resource).unwrap();
            assert_eq!(url.as_str(), expected);
        }
        assert!(SecEdgarDirectTransport::request_for("filing:320193:../../etc:passwd").is_err());
    }

    #[test]
    fn official_sec_shapes_are_accepted() {
        validate_sec_payload(
            "submissions:320193",
            &json!({
                "cik": "0000320193",
                "name": "Apple Inc.",
                "filings": {"recent": {"form": ["10-K"], "accessionNumber": ["0000320193-24-000123"]}}
            }),
        )
        .unwrap();
        validate_sec_payload(
            "companyfacts:320193",
            &json!({"cik": 320193, "facts": {"us-gaap": {}}}),
        )
        .unwrap();
    }

    #[test]
    fn fred_key_never_enters_public_uri_or_debug() {
        let adapter = FredDirectTransport::new("top-secret-key").unwrap();
        let (_, public_url, _) =
            FredDirectTransport::request_for("series:DGS10:2026-01-01:2026-08-17").unwrap();
        assert!(!public_url.as_str().contains("top-secret-key"));
        assert!(!format!("{adapter:?}").contains("top-secret-key"));
    }

    #[test]
    fn official_fred_shapes_are_accepted() {
        validate_fred_payload(
            true,
            &json!({"observations": [{"date": "2026-08-14", "value": "4.25"}]}),
        )
        .unwrap();
        validate_fred_payload(false, &json!({"vintage_dates": ["2026-08-14"]})).unwrap();
    }
}
