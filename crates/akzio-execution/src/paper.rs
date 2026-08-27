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
    FreezeState, OrderReceipt, OrderReceiptState, PaperCommitment, RunPurpose, TaskWritePermit,
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
    PaperDispatchResult, V2PaperDispatchRuntime,
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

pub fn is_alpaca_paper_base_url(supplied: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(supplied.trim()) else {
        return false;
    };
    parsed.scheme() == "https"
        && parsed.host_str() == Some("paper-api.alpaca.markets")
        && parsed.port().is_none()
        && parsed.username().is_empty()
        && parsed.password().is_none()
        && parsed.path() == "/"
        && parsed.query().is_none()
        && parsed.fragment().is_none()
}

include!("paper_parts/execute.rs");
include!("paper_parts/reconcile.rs");
include!("paper_parts/transport.rs");
include!("paper_parts/broker.rs");
include!("paper_parts/protocol.rs");
#[cfg(test)]
#[path = "tests.rs"]
mod tests;
