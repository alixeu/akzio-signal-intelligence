//! Legacy document-oriented market ingestion awaiting owner-phase replacement.
//!
//! The daemon never lets a model read mutable market endpoints directly.  This
//! module snapshots every configured source into CAS first, then returns one
//! immutable manifest that a workflow pins for its entire lifetime.

use std::collections::{BTreeMap, BTreeSet};

use akzio_context::legacy::{ContextBroker, NewJsonDocument};
use akzio_domain::{
    Asset, DocumentId, DocumentKind, DocumentLifecycle, DocumentOrigin, DocumentRecord, Provenance,
    RunId,
};
use chrono::{DateTime, Utc};
use reqwest::{Client, StatusCode};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("missing {0}")]
    MissingEnvironment(&'static str),
    #[error("asset configuration must contain exactly TQQQ, QQQ, SOXX, SOXL")]
    InvalidAssets,
    #[error("request to {url} failed: {source}")]
    Transport { url: String, source: reqwest::Error },
    #[error("request to {url} returned HTTP {status}: {body}")]
    Http {
        url: String,
        status: StatusCode,
        body: String,
    },
    #[error(transparent)]
    Context(#[from] akzio_context::legacy::ContextError),
}

pub type Result<T> = std::result::Result<T, IngestError>;

#[derive(Debug, Clone)]
pub struct AlpacaCredentials {
    pub key_id: String,
    pub secret_key: String,
}

impl AlpacaCredentials {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            key_id: std::env::var("ALPACA_API_KEY")
                .map_err(|_| IngestError::MissingEnvironment("ALPACA_API_KEY"))?,
            secret_key: std::env::var("ALPACA_API_SECRET")
                .map_err(|_| IngestError::MissingEnvironment("ALPACA_API_SECRET"))?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct AlpacaEndpoints {
    pub trading_base: String,
    pub data_base: String,
}

impl Default for AlpacaEndpoints {
    fn default() -> Self {
        Self {
            trading_base: "https://paper-api.alpaca.markets".to_owned(),
            data_base: "https://data.alpaca.markets".to_owned(),
        }
    }
}

impl AlpacaEndpoints {
    fn trading_url(&self, path: &str) -> String {
        format!("{}{}", self.trading_base.trim_end_matches('/'), path)
    }

    fn data_url(&self, path: &str) -> String {
        format!("{}{}", self.data_base.trim_end_matches('/'), path)
    }
}

#[derive(Debug, Clone)]
pub struct IngestConfig {
    pub assets: BTreeSet<Asset>,
    pub bars_limit: u16,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            assets: Asset::EXECUTABLE.into_iter().collect(),
            bars_limit: 30,
        }
    }
}

impl IngestConfig {
    pub fn validate(&self) -> Result<()> {
        let expected = Asset::EXECUTABLE.into_iter().collect::<BTreeSet<_>>();
        if self.assets != expected || self.bars_limit == 0 {
            return Err(IngestError::InvalidAssets);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SealedInput {
    pub manifest: DocumentRecord,
    pub normalized: DocumentRecord,
    pub raw_documents: Vec<DocumentRecord>,
}

impl SealedInput {
    pub fn document_ids(&self) -> Vec<DocumentId> {
        self.raw_documents
            .iter()
            .map(|document| document.document_id.clone())
            .chain(std::iter::once(self.normalized.document_id.clone()))
            .chain(std::iter::once(self.manifest.document_id.clone()))
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct Ingestor {
    client: Client,
    credentials: AlpacaCredentials,
    endpoints: AlpacaEndpoints,
    config: IngestConfig,
}

impl Ingestor {
    pub fn new(
        credentials: AlpacaCredentials,
        endpoints: AlpacaEndpoints,
        config: IngestConfig,
    ) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            client: Client::new(),
            credentials,
            endpoints,
            config,
        })
    }

    pub fn from_env(config: IngestConfig) -> Result<Self> {
        Self::new(
            AlpacaCredentials::from_env()?,
            AlpacaEndpoints::default(),
            config,
        )
    }

    /// Seal the exact input surface used by a run.  There is intentionally no
    /// mutable reference in the returned value: every later read uses its
    /// document IDs and CAS content hash.
    pub async fn seal(
        &self,
        broker: &ContextBroker,
        run_id: &RunId,
        origin: DocumentOrigin,
        now: DateTime<Utc>,
    ) -> Result<SealedInput> {
        let mut raw_documents = Vec::new();
        let account = self.fetch_alpaca_json("account", "/v2/account").await?;
        raw_documents.push(self.record_raw(
            broker,
            run_id,
            "alpaca.account",
            &account,
            &origin,
            now,
        )?);

        let positions = self.fetch_alpaca_json("positions", "/v2/positions").await?;
        raw_documents.push(self.record_raw(
            broker,
            run_id,
            "alpaca.positions",
            &positions,
            &origin,
            now,
        )?);

        let mut market = BTreeMap::new();
        for asset in &self.config.assets {
            let symbol = asset.symbol();
            let quote_path = format!("/v2/stocks/{symbol}/quotes/latest?feed=iex");
            let bars_path = format!(
                "/v2/stocks/{symbol}/bars?timeframe=1Day&limit={}&feed=iex",
                self.config.bars_limit
            );
            let quote = self.fetch_data_json("quote", &quote_path).await?;
            let quote_document = self.record_raw(
                broker,
                run_id,
                &format!("alpaca.quote.{symbol}"),
                &quote,
                &origin,
                now,
            )?;
            let bars = self.fetch_data_json("bars", &bars_path).await?;
            let bars_document = self.record_raw(
                broker,
                run_id,
                &format!("alpaca.bars.{symbol}"),
                &bars,
                &origin,
                now,
            )?;
            market.insert(
                symbol.to_owned(),
                serde_json::json!({
                    "quote": quote.payload,
                    "bars": bars.payload,
                    "quote_document_id": quote_document.document_id,
                    "bars_document_id": bars_document.document_id,
                }),
            );
            raw_documents.push(quote_document);
            raw_documents.push(bars_document);
        }

        let source_refs = raw_documents
            .iter()
            .map(|document| document.document_id.clone())
            .collect::<Vec<_>>();
        let normalized_value = serde_json::json!({
            "schema_version": 1,
            "kind": "sealed_market_input",
            "run_id": run_id,
            "sealed_at": now,
            "account": account.payload,
            "positions": positions.payload,
            "market": market,
        });
        let normalized = broker.record_json_with_provenance(
            NewJsonDocument {
                kind: DocumentKind::NormalizedEvidence,
                producer: "ingest.normalize".to_owned(),
                run_id: Some(run_id.clone()),
                lifecycle: DocumentLifecycle::Canonical,
                source_refs: source_refs.clone(),
                origin: Some(origin.clone()),
                value: &normalized_value,
                created_at: now,
            },
            Provenance {
                source: "akzio.ingest".to_owned(),
                observed_at: Some(now),
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                contract_hash: None,
            },
        )?;
        let manifest_value = serde_json::json!({
            "schema_version": 1,
            "run_id": run_id,
            "sealed_at": now,
            "normalized_document_id": normalized.document_id,
            "raw_document_ids": source_refs,
        });
        let manifest = broker.record_json_with_provenance(
            NewJsonDocument {
                kind: DocumentKind::ContextManifest,
                producer: "ingest.seal".to_owned(),
                run_id: Some(run_id.clone()),
                lifecycle: DocumentLifecycle::RunScoped,
                source_refs: vec![normalized.document_id.clone()],
                origin: Some(origin.clone()),
                value: &manifest_value,
                created_at: now,
            },
            Provenance::local("akzio.ingest", now),
        )?;

        Ok(SealedInput {
            manifest,
            normalized,
            raw_documents,
        })
    }

    async fn fetch_alpaca_json(&self, source: &str, path: &str) -> Result<FetchedJson> {
        let url = self.endpoints.trading_url(path);
        self.fetch_json(source, url, true).await
    }

    async fn fetch_data_json(&self, source: &str, path: &str) -> Result<FetchedJson> {
        let url = self.endpoints.data_url(path);
        self.fetch_json(source, url, true).await
    }

    async fn fetch_json(
        &self,
        source: &str,
        url: String,
        alpaca_auth: bool,
    ) -> Result<FetchedJson> {
        let request = self.client.get(&url);
        let request = if alpaca_auth {
            request
                .header("APCA-API-KEY-ID", &self.credentials.key_id)
                .header("APCA-API-SECRET-KEY", &self.credentials.secret_key)
        } else {
            request
        };
        let response = request
            .send()
            .await
            .map_err(|source| IngestError::Transport {
                url: url.clone(),
                source,
            })?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|source| IngestError::Transport {
                url: url.clone(),
                source,
            })?;
        if !status.is_success() {
            return Err(IngestError::Http { url, status, body });
        }
        let payload = serde_json::from_str(&body).unwrap_or(Value::String(body));
        Ok(FetchedJson {
            source: source.to_owned(),
            url,
            payload,
        })
    }

    fn record_raw(
        &self,
        broker: &ContextBroker,
        run_id: &RunId,
        source: &str,
        fetched: &FetchedJson,
        origin: &DocumentOrigin,
        now: DateTime<Utc>,
    ) -> Result<DocumentRecord> {
        broker
            .record_json_with_provenance(
                NewJsonDocument {
                    kind: DocumentKind::RawEvidence,
                    producer: format!("ingest.{}", fetched.source),
                    run_id: Some(run_id.clone()),
                    lifecycle: DocumentLifecycle::Canonical,
                    source_refs: Vec::new(),
                    origin: Some(origin.clone()),
                    value: &fetched.payload,
                    created_at: now,
                },
                Provenance {
                    source: source.to_owned(),
                    observed_at: extract_observed_at(&fetched.payload),
                    retrieved_at: now,
                    source_uri: Some(fetched.url.clone()),
                    confidence_ppm: 1_000_000,
                    contract_hash: None,
                },
            )
            .map_err(Into::into)
    }
}

#[derive(Debug, Clone)]
struct FetchedJson {
    source: String,
    url: String,
    payload: Value,
}

fn extract_observed_at(value: &Value) -> Option<DateTime<Utc>> {
    let candidates = [
        value.pointer("/timestamp"),
        value.pointer("/t"),
        value.pointer("/quote/t"),
        value.pointer("/bar/t"),
    ];
    candidates.into_iter().flatten().find_map(|value| {
        value
            .as_str()
            .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
            .map(|time| time.with_timezone(&Utc))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asset_scope_is_exact() {
        assert!(IngestConfig::default().validate().is_ok());
        let mut config = IngestConfig::default();
        config.assets.remove(&Asset::Soxl);
        assert!(matches!(config.validate(), Err(IngestError::InvalidAssets)));
    }

    #[test]
    fn observed_time_accepts_standard_quote_shape() {
        let value = serde_json::json!({"quote": {"t": "2026-08-06T12:00:00Z"}});
        assert_eq!(
            extract_observed_at(&value).unwrap(),
            DateTime::parse_from_rfc3339("2026-08-06T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc)
        );
    }
}
