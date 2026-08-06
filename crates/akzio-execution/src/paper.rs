//! Thin, idempotent Alpaca Paper adapter.
//!
//! It intentionally owns broker protocol details only.  Target construction,
//! risk limits, quote freshness, and all approval decisions stay in the parent
//! execution module and Runtime gates.

use std::env;

use akzio_domain::ContentHash;
use chrono::{DateTime, NaiveDate};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::{ExecutionPlan, MoneyMicros, OrderIntent, OrderSide};

#[derive(Debug, Error)]
pub enum PaperError {
    #[error("ALPACA_API_KEY is not set")]
    MissingKey,
    #[error("ALPACA_API_SECRET is not set")]
    MissingSecret,
    #[error("Paper execution rejects non-Paper endpoint {0}")]
    NonPaperEndpoint(String),
    #[error("broker request to {url} failed: {source}")]
    Transport { url: String, source: reqwest::Error },
    #[error("broker request to {url} returned HTTP {status}: {body}")]
    Http {
        url: String,
        status: StatusCode,
        body: String,
    },
    #[error("Alpaca Paper market is closed")]
    MarketClosed,
    #[error("broker response omitted {0}")]
    MissingField(&'static str),
    #[error("broker clock timestamp is invalid: {0}")]
    InvalidClock(String),
    #[error("order quantity rounds to zero")]
    ZeroQuantity,
    #[error("one repricing attempt is already consumed")]
    RepriceConsumed,
}

pub type Result<T> = std::result::Result<T, PaperError>;

#[derive(Debug, Clone)]
pub struct PaperCredentials {
    pub key_id: String,
    pub secret_key: String,
}

impl PaperCredentials {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            key_id: env::var("ALPACA_API_KEY").map_err(|_| PaperError::MissingKey)?,
            secret_key: env::var("ALPACA_API_SECRET").map_err(|_| PaperError::MissingSecret)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaperOrderReceipt {
    pub client_order_id: String,
    pub broker_order_id: String,
    pub symbol: String,
    pub status: String,
    pub reused: bool,
    pub reprice_count: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaperExecution {
    pub plan_hash: ContentHash,
    pub orders: Vec<PaperOrderReceipt>,
}

/// Broker-authoritative open-session state. `session_date` comes from the
/// broker timestamp, so Paper scheduling does not depend on a local timezone
/// or a hand-maintained holiday calendar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketClock {
    pub is_open: bool,
    pub session_date: NaiveDate,
}

#[derive(Debug, Clone)]
pub struct AlpacaPaper {
    client: Client,
    base_url: String,
    credentials: PaperCredentials,
}

impl AlpacaPaper {
    pub fn new(base_url: impl Into<String>, credentials: PaperCredentials) -> Result<Self> {
        let base_url = base_url.into().trim_end_matches('/').to_owned();
        let local_test_endpoint = base_url.contains("localhost") || base_url.contains("127.0.0.1");
        if !local_test_endpoint && !base_url.contains("paper-api.alpaca.markets") {
            return Err(PaperError::NonPaperEndpoint(base_url));
        }
        Ok(Self {
            client: Client::new(),
            base_url,
            credentials,
        })
    }

    pub fn from_env() -> Result<Self> {
        let base_url = env::var("ALPACA_PAPER_BASE_URL")
            .unwrap_or_else(|_| "https://paper-api.alpaca.markets".to_owned());
        Self::new(base_url, PaperCredentials::from_env()?)
    }

    pub async fn account(&self) -> Result<Value> {
        self.get_json("/v2/account").await
    }

    pub async fn positions(&self) -> Result<Value> {
        self.get_json("/v2/positions").await
    }

    pub async fn market_clock(&self) -> Result<MarketClock> {
        let clock = self.get_json("/v2/clock").await?;
        market_clock_from_value(&clock)
    }

    pub async fn execute(&self, plan: &ExecutionPlan) -> Result<PaperExecution> {
        self.assert_market_open().await?;
        let mut orders = Vec::with_capacity(plan.orders.len());
        for (index, order) in plan.orders.iter().enumerate() {
            let client_order_id = client_order_id(&plan.plan_hash, index, 0);
            let receipt = match self.lookup(&client_order_id).await? {
                Some(receipt) => PaperOrderReceipt {
                    reused: true,
                    reprice_count: 0,
                    ..receipt
                },
                None => self.submit_order(order, &client_order_id, 0).await?,
            };
            orders.push(receipt);
        }
        Ok(PaperExecution {
            plan_hash: plan.plan_hash.clone(),
            orders,
        })
    }

    pub async fn reconcile(&self, execution: &PaperExecution) -> Result<PaperExecution> {
        let mut orders = Vec::with_capacity(execution.orders.len());
        for receipt in &execution.orders {
            orders.push(
                self.get_order(
                    &receipt.broker_order_id,
                    &receipt.client_order_id,
                    receipt.reprice_count,
                )
                .await?,
            );
        }
        Ok(PaperExecution {
            plan_hash: execution.plan_hash.clone(),
            orders,
        })
    }

    /// The caller supplies a newly gate-validated replacement intent.  This
    /// adapter guarantees exactly one cancellation/replacement lineage.
    pub async fn cancel_and_replace_once(
        &self,
        receipt: &PaperOrderReceipt,
        replacement: &OrderIntent,
    ) -> Result<PaperOrderReceipt> {
        if receipt.reprice_count >= 1 {
            return Err(PaperError::RepriceConsumed);
        }
        self.delete(&format!("/v2/orders/{}", receipt.broker_order_id))
            .await?;
        let client_order_id = replacement_client_order_id(&receipt.client_order_id);
        self.submit_order(replacement, &client_order_id, 1).await
    }

    async fn assert_market_open(&self) -> Result<()> {
        if !self.market_clock().await?.is_open {
            return Err(PaperError::MarketClosed);
        }
        Ok(())
    }

    async fn lookup(&self, client_order_id: &str) -> Result<Option<PaperOrderReceipt>> {
        let url = self.url("/v2/orders:by_client_order_id");
        let response = self
            .authorized(
                self.client
                    .get(&url)
                    .query(&[("client_order_id", client_order_id)]),
            )
            .send()
            .await
            .map_err(|source| PaperError::Transport {
                url: url.clone(),
                source,
            })?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|source| PaperError::Transport {
                url: url.clone(),
                source,
            })?;
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            return Err(PaperError::Http { url, status, body });
        }
        let value = parse_value(&body);
        Ok(Some(receipt_from_value(value, client_order_id, false, 0)?))
    }

    async fn submit_order(
        &self,
        order: &OrderIntent,
        client_order_id: &str,
        reprice_count: u8,
    ) -> Result<PaperOrderReceipt> {
        let url = self.url("/v2/orders");
        let body = serde_json::json!({
            "symbol": order.asset.symbol(),
            "qty": quantity_string(order)?,
            "side": side_name(order.side),
            "type": "limit",
            "time_in_force": "day",
            "limit_price": money_string(order.limit_price),
            "extended_hours": false,
            "client_order_id": client_order_id,
        });
        let value = self.post_json(&url, body).await?;
        receipt_from_value(value, client_order_id, false, reprice_count)
    }

    async fn get_order(
        &self,
        broker_order_id: &str,
        client_order_id: &str,
        reprice_count: u8,
    ) -> Result<PaperOrderReceipt> {
        let value = self
            .get_json(&format!("/v2/orders/{broker_order_id}"))
            .await?;
        receipt_from_value(value, client_order_id, false, reprice_count)
    }

    async fn get_json(&self, path: &str) -> Result<Value> {
        let url = self.url(path);
        let response = self
            .authorized(self.client.get(&url))
            .send()
            .await
            .map_err(|source| PaperError::Transport {
                url: url.clone(),
                source,
            })?;
        self.response_json(url, response).await
    }

    async fn post_json(&self, url: &str, body: Value) -> Result<Value> {
        let response = self
            .authorized(self.client.post(url).json(&body))
            .send()
            .await
            .map_err(|source| PaperError::Transport {
                url: url.to_owned(),
                source,
            })?;
        self.response_json(url.to_owned(), response).await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let url = self.url(path);
        let response = self
            .authorized(self.client.delete(&url))
            .send()
            .await
            .map_err(|source| PaperError::Transport {
                url: url.clone(),
                source,
            })?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|source| PaperError::Transport {
                url: url.clone(),
                source,
            })?;
        if !status.is_success() {
            return Err(PaperError::Http { url, status, body });
        }
        Ok(())
    }

    async fn response_json(&self, url: String, response: reqwest::Response) -> Result<Value> {
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|source| PaperError::Transport {
                url: url.clone(),
                source,
            })?;
        if !status.is_success() {
            return Err(PaperError::Http { url, status, body });
        }
        Ok(parse_value(&body))
    }

    fn authorized(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request
            .header("APCA-API-KEY-ID", &self.credentials.key_id)
            .header("APCA-API-SECRET-KEY", &self.credentials.secret_key)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }
}

fn receipt_from_value(
    value: Value,
    fallback_client_order_id: &str,
    reused: bool,
    reprice_count: u8,
) -> Result<PaperOrderReceipt> {
    let broker_order_id = required_string(&value, "id")?;
    let symbol = required_string(&value, "symbol")?;
    let status = required_string(&value, "status")?;
    let client_order_id = value
        .get("client_order_id")
        .and_then(Value::as_str)
        .unwrap_or(fallback_client_order_id)
        .to_owned();
    Ok(PaperOrderReceipt {
        client_order_id,
        broker_order_id,
        symbol,
        status,
        reused,
        reprice_count,
    })
}

fn required_string(value: &Value, field: &'static str) -> Result<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or(PaperError::MissingField(field))
}

fn parse_value(body: &str) -> Value {
    serde_json::from_str(body).unwrap_or_else(|_| Value::String(body.to_owned()))
}

fn market_clock_from_value(clock: &Value) -> Result<MarketClock> {
    let is_open = clock
        .get("is_open")
        .and_then(Value::as_bool)
        .ok_or(PaperError::MissingField("clock.is_open"))?;
    let timestamp = clock
        .get("timestamp")
        .and_then(Value::as_str)
        .ok_or(PaperError::MissingField("clock.timestamp"))?;
    let observed_at = DateTime::parse_from_rfc3339(timestamp)
        .map_err(|error| PaperError::InvalidClock(error.to_string()))?;
    Ok(MarketClock {
        is_open,
        session_date: observed_at.date_naive(),
    })
}

fn side_name(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn money_string(value: MoneyMicros) -> String {
    let whole = value.0 / 1_000_000;
    let fraction = value.0.unsigned_abs() % 1_000_000;
    format!("{whole}.{fraction:06}")
}

fn quantity_string(order: &OrderIntent) -> Result<String> {
    let quantity_millionths = order
        .notional
        .0
        .saturating_mul(1_000_000)
        .checked_div(order.limit_price.0)
        .unwrap_or_default();
    if quantity_millionths <= 0 {
        return Err(PaperError::ZeroQuantity);
    }
    let whole = quantity_millionths / 1_000_000;
    let fraction = quantity_millionths.unsigned_abs() % 1_000_000;
    Ok(format!("{whole}.{fraction:06}"))
}

pub fn client_order_id(plan_hash: &ContentHash, order_index: usize, reprice_count: u8) -> String {
    let prefix = &plan_hash.as_str()[..16];
    format!("akzio-v2-{prefix}-{order_index}-r{reprice_count}")
}

fn replacement_client_order_id(previous: &str) -> String {
    let base = previous.split("-r").next().unwrap_or(previous);
    format!("{base}-r1")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MoneyMicros, OrderIntent};
    use akzio_domain::{Asset, ContentHash};

    #[test]
    fn ids_are_deterministic_and_bounded() {
        let hash = ContentHash::of_bytes(b"plan");
        let id = client_order_id(&hash, 12, 0);
        assert!(id.starts_with("akzio-v2-"));
        assert!(id.len() <= 48);
        assert_eq!(
            replacement_client_order_id(&id),
            format!("{}-r1", id.split("-r").next().unwrap())
        );
    }

    #[test]
    fn limit_notional_becomes_fractional_quantity() {
        let order = OrderIntent {
            asset: Asset::Tqqq,
            side: OrderSide::Buy,
            notional: MoneyMicros::from_usd_cents(10_000),
            limit_price: MoneyMicros::from_usd_cents(2_500),
        };
        assert_eq!(quantity_string(&order).unwrap(), "4.000000");
    }

    #[test]
    fn real_endpoint_must_be_paper() {
        let credentials = PaperCredentials {
            key_id: "key".to_owned(),
            secret_key: "secret".to_owned(),
        };
        assert!(matches!(
            AlpacaPaper::new("https://api.alpaca.markets", credentials),
            Err(PaperError::NonPaperEndpoint(_))
        ));
    }

    #[test]
    fn market_clock_uses_broker_session_date() {
        let clock = market_clock_from_value(&serde_json::json!({
            "is_open": true,
            "timestamp": "2026-08-06T10:00:00-04:00",
        }))
        .unwrap();
        assert!(clock.is_open);
        assert_eq!(
            clock.session_date,
            chrono::NaiveDate::from_ymd_opt(2026, 8, 6).unwrap()
        );
        assert!(matches!(
            market_clock_from_value(&serde_json::json!({"is_open": true})),
            Err(PaperError::MissingField("clock.timestamp"))
        ));
    }
}
