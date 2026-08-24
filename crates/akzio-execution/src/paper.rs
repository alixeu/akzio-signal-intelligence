//! Thin, idempotent Alpaca Paper adapter.
//!
//! It intentionally owns broker protocol details only.  Target construction,
//! risk limits, quote freshness, and all approval decisions stay in the parent
//! execution module and Runtime gates.

use std::{
    env,
    future::Future,
    net::{IpAddr, Ipv4Addr},
    pin::Pin,
};

use akzio_domain::{
    Artifact, ArtifactKind, ArtifactRef, Asset, ContentHash, DomainError, ExecutionContext,
    FreezeState, OrderReceipt, OrderReceiptState, PaperCommitment, PaperReprice, RunPurpose,
    TaskWritePermit,
};
use akzio_store::v2::{DaemonLease, StoreError, V2Store};
use chrono::{DateTime, NaiveDate, Utc};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::{
    ExecutionPlan, MoneyMicros, OrderIntent, OrderSide, ReconciliationError, ReconciliationInput,
    ReconciliationOutput, V2ReconciliationRuntime,
};

#[derive(Debug, Error)]
pub enum PaperError {
    #[error(transparent)]
    Domain(#[from] DomainError),
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
    #[error("broker returned invalid quantity for {0}")]
    InvalidQuantity(&'static str),
    #[error("original Paper order is no longer eligible for the durable reprice")]
    RepricePriorClosed,
    #[error("Paper commitment is invalid: {0}")]
    InvalidCommitment(String),
    #[error("Paper commitment plan hash does not match the submitted plan")]
    CommitmentPlanHashMismatch,
    #[error("Paper commitment client order ID does not match plan order for {0}")]
    CommitmentClientOrderMismatch(Asset),
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
    pub requested_quantity_micros: i64,
    pub filled_quantity_micros: i64,
    pub remaining_quantity_micros: i64,
    pub average_fill_price: Option<MoneyMicros>,
    pub broker_updated_at: DateTime<Utc>,
    pub reason: Option<String>,
    pub reused: bool,
    pub reprice_count: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaperExecution {
    pub plan_hash: ContentHash,
    pub orders: Vec<PaperOrderReceipt>,
}

/// Minimal broker protocol used by Rust-gated execution. The production
/// implementation is Alpaca Paper only; fixtures use an in-memory fake rather
/// than weakening endpoint validation with localhost exceptions.
/// v2 broker protocol. It accepts only a durable Rust-owned commitment and
/// the allocation plan it commits to; callers cannot submit a naked plan.
pub trait CommittedPaperBroker: Send + Sync {
    fn execute_commitment<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        plan: &'a ExecutionPlan,
    ) -> Pin<Box<dyn Future<Output = Result<PaperExecution>> + Send + 'a>>;

    fn replace_commitment_once<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        reprice: &'a PaperReprice,
        replacement: &'a OrderIntent,
    ) -> Pin<Box<dyn Future<Output = Result<PaperOrderReceipt>> + Send + 'a>>;

    fn reconcile_commitment<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        execution: &'a PaperExecution,
    ) -> Pin<Box<dyn Future<Output = Result<PaperExecution>> + Send + 'a>>;
}

/// Input for the task that is allowed to submit an already durable Paper
/// commitment. Creating the commitment is a separate, scheduler-fenced task;
/// this task can only replay that exact commitment through a broker.
#[path = "paper_dispatch.rs"]
mod paper_dispatch;
pub use paper_dispatch::{
    PaperDispatchError, PaperDispatchFailpoint, PaperDispatchInput, PaperDispatchOutput,
    PaperDispatchResult, PaperRepriceDispatchInput, V2PaperDispatchRuntime,
};
fn receipt_state(status: &str) -> PaperDispatchResult<OrderReceiptState> {
    match status.trim().to_ascii_lowercase().as_str() {
        "new"
        | "accepted"
        | "pending_new"
        | "accepted_for_bidding"
        | "pending_replace"
        | "pending_cancel" => Ok(OrderReceiptState::Accepted),
        "partially_filled" => Ok(OrderReceiptState::PartiallyFilled),
        "filled" => Ok(OrderReceiptState::Filled),
        "canceled" | "expired" | "done_for_day" | "stopped" | "suspended" => {
            Ok(OrderReceiptState::Canceled)
        }
        "rejected" => Ok(OrderReceiptState::Rejected),
        "failed" => Ok(OrderReceiptState::Failed),
        other => Err(PaperDispatchError::UnsupportedReceiptStatus(
            other.to_owned(),
        )),
    }
}

/// Broker-authoritative open-session state. `session_date` comes from the
/// broker timestamp, so Paper scheduling does not depend on a local timezone
/// or a hand-maintained holiday calendar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketClock {
    pub is_open: bool,
    pub session_date: NaiveDate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortfolioHistoryRange {
    OneDay,
    OneWeek,
    OneMonth,
    ThreeMonths,
}

impl PortfolioHistoryRange {
    fn path(self) -> &'static str {
        match self {
            Self::OneDay => "/v2/account/portfolio/history?period=1D&timeframe=5Min",
            Self::OneWeek => "/v2/account/portfolio/history?period=1W&timeframe=1H",
            Self::OneMonth => "/v2/account/portfolio/history?period=1M&timeframe=1D",
            Self::ThreeMonths => "/v2/account/portfolio/history?period=3M&timeframe=1D",
        }
    }
}

#[derive(Debug, Clone)]
pub struct AlpacaPaper {
    client: Client,
    base_url: String,
    credentials: PaperCredentials,
}

impl AlpacaPaper {
    pub fn new(base_url: impl Into<String>, credentials: PaperCredentials) -> Result<Self> {
        let supplied = base_url.into();
        let parsed = reqwest::Url::parse(supplied.trim())
            .map_err(|_| PaperError::NonPaperEndpoint(supplied.clone()))?;
        if parsed.scheme() != "https"
            || parsed.host_str() != Some("paper-api.alpaca.markets")
            || parsed.port().is_some()
            || parsed.username() != ""
            || parsed.password().is_some()
            || parsed.path() != "/"
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(PaperError::NonPaperEndpoint(supplied));
        }
        let client = Client::builder()
            .http1_only()
            .local_address(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|source| PaperError::Transport {
                url: supplied.clone(),
                source,
            })?;
        Ok(Self {
            client,
            base_url: "https://paper-api.alpaca.markets".to_owned(),
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

    pub async fn portfolio_history(&self, range: PortfolioHistoryRange) -> Result<Value> {
        self.get_json(range.path()).await
    }

    pub async fn market_clock(&self) -> Result<MarketClock> {
        let clock = self.get_json("/v2/clock").await?;
        market_clock_from_value(&clock)
    }

    async fn execute_committed(
        &self,
        commitment: &PaperCommitment,
        plan: &ExecutionPlan,
    ) -> Result<PaperExecution> {
        self.validate_commitment(commitment, plan)?;
        self.assert_market_open().await?;
        let mut orders = Vec::with_capacity(plan.orders.len());
        for order in &plan.orders {
            let client_order_id = commitment
                .client_order_ids
                .get(&order.asset)
                .ok_or(PaperError::CommitmentClientOrderMismatch(order.asset))?;
            let receipt = match self.lookup(client_order_id).await? {
                Some(receipt) => PaperOrderReceipt {
                    reused: true,
                    reprice_count: 0,
                    ..receipt
                },
                None => self.submit_order(order, client_order_id, 0).await?,
            };
            orders.push(receipt);
        }
        Ok(PaperExecution {
            plan_hash: plan.plan_hash.clone(),
            orders,
        })
    }

    async fn reconcile_committed(
        &self,
        commitment: &PaperCommitment,
        execution: &PaperExecution,
    ) -> Result<PaperExecution> {
        if execution.plan_hash != commitment.plan_hash {
            return Err(PaperError::CommitmentPlanHashMismatch);
        }
        let mut orders = Vec::with_capacity(execution.orders.len());
        for receipt in &execution.orders {
            let asset = Asset::try_from(receipt.symbol.as_str())?;
            let original = commitment
                .client_order_ids
                .get(&asset)
                .ok_or(PaperError::CommitmentClientOrderMismatch(asset))?;
            if receipt.client_order_id != *original
                && receipt.client_order_id != replacement_client_order_id(original)
            {
                return Err(PaperError::CommitmentClientOrderMismatch(asset));
            }
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
    /// Execute only the pre-recorded r0 -> r1 reprice lineage. A retry first
    /// looks up the deterministic replacement ID, so a crash after submission
    /// never creates another Paper order.
    pub async fn execute_reprice_committed(
        &self,
        commitment: &PaperCommitment,
        reprice: &PaperReprice,
        replacement: &OrderIntent,
    ) -> Result<PaperOrderReceipt> {
        self.validate_reprice(commitment, reprice, replacement)?;
        if let Some(existing) = self.lookup(&reprice.replacement_client_order_id).await? {
            return Ok(PaperOrderReceipt {
                reused: true,
                reprice_count: 1,
                ..existing
            });
        }
        self.assert_market_open().await?;
        let prior = self
            .get_order(
                &reprice.prior_broker_order_id,
                &reprice.prior_client_order_id,
                0,
            )
            .await?;
        match prior.status.trim().to_ascii_lowercase().as_str() {
            "new"
            | "accepted"
            | "pending_new"
            | "accepted_for_bidding"
            | "pending_replace"
            | "pending_cancel"
            | "partially_filled" => {
                let value = self
                    .patch_json(
                        &format!("/v2/orders/{}", reprice.prior_broker_order_id),
                        reprice_request(
                            replacement.limit_price,
                            &reprice.replacement_client_order_id,
                        ),
                    )
                    .await?;
                receipt_from_value(value, &reprice.replacement_client_order_id, false, 1)
            }
            "canceled" | "expired" | "done_for_day" => Err(PaperError::RepricePriorClosed),
            _ => Err(PaperError::RepricePriorClosed),
        }
    }

    fn validate_commitment(
        &self,
        commitment: &PaperCommitment,
        plan: &ExecutionPlan,
    ) -> Result<()> {
        commitment
            .validate()
            .map_err(|error| PaperError::InvalidCommitment(error.to_string()))?;
        plan.validate()?;
        if commitment.plan_hash != plan.plan_hash {
            return Err(PaperError::CommitmentPlanHashMismatch);
        }
        if commitment.broker_session != plan.broker_session {
            return Err(PaperError::InvalidCommitment(
                "broker session does not match execution plan".to_owned(),
            ));
        }
        if commitment.client_order_ids.len() != plan.orders.len() {
            return Err(PaperError::InvalidCommitment(
                "client order count does not match allocation plan".to_owned(),
            ));
        }
        for (index, order) in plan.orders.iter().enumerate() {
            let expected = client_order_id(&commitment.broker_session, &plan.plan_hash, index, 0);
            if commitment.client_order_ids.get(&order.asset) != Some(&expected) {
                return Err(PaperError::CommitmentClientOrderMismatch(order.asset));
            }
        }
        Ok(())
    }

    fn validate_reprice(
        &self,
        commitment: &PaperCommitment,
        reprice: &PaperReprice,
        replacement: &OrderIntent,
    ) -> Result<()> {
        commitment
            .validate()
            .map_err(|error| PaperError::InvalidCommitment(error.to_string()))?;
        reprice
            .validate()
            .map_err(|error| PaperError::InvalidCommitment(error.to_string()))?;
        if commitment.client_order_ids.get(&reprice.asset) != Some(&reprice.prior_client_order_id)
            || replacement.asset != reprice.asset
            || replacement.limit_price != reprice.replacement_limit_price
            || reprice.replacement_client_order_id
                != replacement_client_order_id(&reprice.prior_client_order_id)
        {
            return Err(PaperError::InvalidCommitment(
                "reprice does not match committed order lineage".to_owned(),
            ));
        }
        Ok(())
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
        let response = {
            let mut attempt = 1_u64;
            loop {
                match self.authorized(self.client.get(&url)).send().await {
                    Ok(response) => break response,
                    Err(_source) if attempt < 5 => {
                        tokio::time::sleep(std::time::Duration::from_millis(250 * attempt)).await;
                        attempt += 1;
                    }
                    Err(source) => {
                        return Err(PaperError::Transport {
                            url: url.clone(),
                            source,
                        });
                    }
                }
            }
        };
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

    async fn patch_json(&self, path: &str, body: Value) -> Result<Value> {
        let url = self.url(path);
        let response = self
            .authorized(self.client.patch(&url).json(&body))
            .send()
            .await
            .map_err(|source| PaperError::Transport {
                url: url.clone(),
                source,
            })?;
        self.response_json(url, response).await
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

impl CommittedPaperBroker for AlpacaPaper {
    fn execute_commitment<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        plan: &'a ExecutionPlan,
    ) -> Pin<Box<dyn Future<Output = Result<PaperExecution>> + Send + 'a>> {
        Box::pin(AlpacaPaper::execute_committed(self, commitment, plan))
    }

    fn replace_commitment_once<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        reprice: &'a PaperReprice,
        replacement: &'a OrderIntent,
    ) -> Pin<Box<dyn Future<Output = Result<PaperOrderReceipt>> + Send + 'a>> {
        Box::pin(AlpacaPaper::execute_reprice_committed(
            self,
            commitment,
            reprice,
            replacement,
        ))
    }

    fn reconcile_commitment<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        execution: &'a PaperExecution,
    ) -> Pin<Box<dyn Future<Output = Result<PaperExecution>> + Send + 'a>> {
        Box::pin(AlpacaPaper::reconcile_committed(
            self, commitment, execution,
        ))
    }
}

fn receipt_from_value(
    value: Value,
    expected_client_order_id: &str,
    reused: bool,
    reprice_count: u8,
) -> Result<PaperOrderReceipt> {
    let broker_order_id = required_string(&value, "id")?;
    let symbol = required_string(&value, "symbol")?;
    let status = required_string(&value, "status")?;
    let client_order_id = required_string(&value, "client_order_id")?;
    if client_order_id != expected_client_order_id {
        return Err(PaperError::InvalidCommitment(
            "broker client order ID does not match durable commitment".to_owned(),
        ));
    }
    let requested_quantity_micros = decimal_micros(&required_string(&value, "qty")?)?;
    let filled_quantity_micros = decimal_micros(&required_string(&value, "filled_qty")?)?;
    let remaining_quantity_micros = requested_quantity_micros
        .checked_sub(filled_quantity_micros)
        .filter(|quantity| *quantity >= 0)
        .ok_or(PaperError::InvalidQuantity("filled_qty"))?;
    let average_fill_price = value
        .get("filled_avg_price")
        .and_then(Value::as_str)
        .filter(|price| !price.trim().is_empty())
        .map(decimal_micros)
        .transpose()?
        .map(MoneyMicros);
    let broker_updated_at = DateTime::parse_from_rfc3339(&required_string(&value, "updated_at")?)
        .map_err(|error| PaperError::InvalidClock(error.to_string()))?
        .with_timezone(&Utc);
    let reason = ["reject_reason", "cancel_reason"]
        .into_iter()
        .find_map(|field| value.get(field).and_then(Value::as_str))
        .map(str::trim)
        .filter(|reason| !reason.is_empty())
        .map(ToOwned::to_owned);
    Ok(PaperOrderReceipt {
        client_order_id,
        broker_order_id,
        symbol,
        status,
        requested_quantity_micros,
        filled_quantity_micros,
        remaining_quantity_micros,
        average_fill_price,
        broker_updated_at,
        reason,
        reused,
        reprice_count,
    })
}

fn decimal_micros(value: &str) -> Result<i64> {
    let value = value.trim();
    let (negative, value) = match value.strip_prefix('-') {
        Some(value) => (true, value),
        None => (false, value.strip_prefix('+').unwrap_or(value)),
    };
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() > 6
    {
        return Err(PaperError::InvalidQuantity("decimal"));
    }
    let whole = whole
        .parse::<i64>()
        .map_err(|_| PaperError::InvalidQuantity("decimal"))?;
    let fraction = if fraction.is_empty() {
        0
    } else {
        fraction
            .parse::<i64>()
            .map_err(|_| PaperError::InvalidQuantity("decimal"))?
            .checked_mul(10_i64.pow((6 - fraction.len()) as u32))
            .ok_or(PaperError::InvalidQuantity("decimal"))?
    };
    let micros = whole
        .checked_mul(1_000_000)
        .and_then(|whole| whole.checked_add(fraction))
        .ok_or(PaperError::InvalidQuantity("decimal"))?;
    Ok(if negative { -micros } else { micros })
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

fn reprice_request(limit_price: MoneyMicros, client_order_id: &str) -> Value {
    serde_json::json!({
        "limit_price": money_string(limit_price),
        "client_order_id": client_order_id,
    })
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

pub fn client_order_id(
    broker_session: &str,
    plan_hash: &ContentHash,
    order_index: usize,
    reprice_count: u8,
) -> String {
    let identity =
        ContentHash::of_bytes(format!("{broker_session}\0{plan_hash}\0{order_index}").as_bytes());
    let prefix = &identity.as_str()[..16];
    format!("akzio-v2-{prefix}-{order_index}-r{reprice_count}")
}

fn replacement_client_order_id(previous: &str) -> String {
    let base = previous.split("-r").next().unwrap_or(previous);
    format!("{base}-r1")
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
