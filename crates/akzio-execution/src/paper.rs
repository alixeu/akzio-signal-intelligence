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
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactRef, Asset, ContentHash, DomainError,
    ExecutionContext, FreezeState, OrderReceipt, OrderReceiptState, PaperCancel, PaperCancelId,
    PaperCancelReason, PaperCommitment, PaperReprice, RunPurpose, TaskWritePermit,
};
use akzio_store::{DaemonLease, Store, StoreError};
use chrono::{DateTime, NaiveDate, Utc};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::{
    ExecutionPlan, MoneyMicros, OrderIntent, OrderSide, ReconciliationError, ReconciliationInput,
    ReconciliationOutput, ReconciliationRuntime,
};

// 文件导读：本模块定义 Alpaca Paper adapter 的凭据、receipt、broker trait 与 HTTP 协议类型，
// 并通过 include 合并 execute/reconcile/transport/protocol 实现。上游 ExecutionVerdict
// 通过 PaperCommitmentRuntime 先持久化唯一 Commitment，PaperDispatchRuntime 才能调用这里；
// 回执仍要进入 Reconciliation，accepted 不等于成交。Decision、目标和执行授权不由 adapter 决定。
#[derive(Debug, Error)]
pub enum PaperError {
    // 错误边界区分配置/认证、endpoint policy、transport/HTTP、审批和 broker 字段，不把 accepted 当成 filled。
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
    #[error("frozen decision, approval or execution snapshot no longer authorizes submission")]
    SubmissionUnauthorized,
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
    // 凭据只从环境读取并保存在受控 client 中；本结构派生 Debug 会格式化全部字段，
    // 因此调用方不得把完整凭据写入日志或错误文本，避免回显 secret_key。
    pub key_id: String,
    pub secret_key: String,
}

impl PaperCredentials {
    pub fn from_env() -> Result<Self> {
        // 按 key、secret 的顺序读取环境变量，`?` 把第一个缺项映射为专用错误并停止；
        // 成功时两个 String 被移动进返回的 credentials，HTTP I/O 尚未发生。
        Ok(Self {
            key_id: env::var("ALPACA_API_KEY").map_err(|_| PaperError::MissingKey)?,
            secret_key: env::var("ALPACA_API_SECRET").map_err(|_| PaperError::MissingSecret)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaperOrderReceipt {
    // receipt 保留 broker authoritative 状态和数量/时间；reused/reprice_count 用于幂等审计而非 UI 猜测。
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
    // execution 绑定已提交 plan_hash，orders 是 Paper broker 的结果投影，不等价于真实成交闭环。
    pub plan_hash: ContentHash,
    pub orders: Vec<PaperOrderReceipt>,
}

// 授权计算独立于 broker trait；私有模块只暴露短时发送许可，不修改冻结 plan hash。
mod submission_authorization;
pub use submission_authorization::PaperSubmissionAuthorization;

/// 已持久化 Commitment 的 Broker 接口；生产实现仅接受 Alpaca Paper endpoint。
/// Trait 形状要求承诺和对应计划一起传入，但持久化身份仍由 Dispatch/Store 核验。
// Trait 对象边界返回装箱 Future：`'a` 将 Future 的有效期约束在所有借入参数之内；
// Pin 让被轮询的 future 保持固定地址，Send 允许 Tokio 将其交给可跨线程调度的任务。
// Future 本身惰性，只有调用方 await/调度器 poll 才运行；Send + Sync 约束 broker 可安全共享。
pub trait CommittedPaperBroker: Send + Sync {
    // 新建订单只能提交已经持久化的 commitment、其计划及临时授权；实现不得接收裸计划。
    fn execute_commitment<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        plan: &'a ExecutionPlan,
        authorization: &'a PaperSubmissionAuthorization,
    ) -> Pin<Box<dyn Future<Output = Result<PaperExecution>> + Send + 'a>>;

    // 只刷新 commitment 已关联的 broker orders，不创建新的订单身份。
    fn reconcile_commitment<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        execution: &'a PaperExecution,
    ) -> Pin<Box<dyn Future<Output = Result<PaperExecution>> + Send + 'a>>;

    // 取消必须提供由 Rust/Store 生成并校验的 cancel intent。
    fn cancel_order<'a>(
        &'a self,
        intent: &'a PaperCancel,
    ) -> Pin<Box<dyn Future<Output = Result<PaperOrderReceipt>> + Send + 'a>>;

    // 改价除 durable reprice intent 外还要有仍在有效期内的发送授权。
    fn replace_order<'a>(
        &'a self,
        intent: &'a PaperReprice,
        authorization: &'a PaperSubmissionAuthorization,
    ) -> Pin<Box<dyn Future<Output = Result<PaperOrderReceipt>> + Send + 'a>>;
}

/// Input for the task that is allowed to submit an already durable Paper
/// commitment. Creating the commitment is a separate, scheduler-fenced task;
/// this task can only replay that exact commitment through a broker.
#[path = "paper_dispatch.rs"]
mod paper_dispatch;
pub use paper_dispatch::{
    PaperDispatchError, PaperDispatchFailpoint, PaperDispatchInput, PaperDispatchOutput,
    PaperDispatchResult, PaperDispatchRuntime, DEFAULT_PAPER_SETTLEMENT_ACTION_GRACE_SECS,
    DEFAULT_PAPER_SETTLEMENT_TIMEOUT_SECS,
};
fn receipt_state(status: &str) -> PaperDispatchResult<OrderReceiptState> {
    // `match` 穷尽已知 wire status；输入先 trim/转小写，未列出的状态保留原规范化文本
    // 并返回 UnsupportedReceiptStatus，调用链不能把未知值降级为成功或失败。
    // provider 状态只映射到领域 receipt enum；未知状态返回错误，不能静默降级为 failed/accepted。
    match status.trim().to_ascii_lowercase().as_str() {
        "new" | "accepted" | "pending_new" | "accepted_for_bidding" => {
            Ok(OrderReceiptState::Accepted)
        }
        "pending_replace" => Ok(OrderReceiptState::PendingReplace),
        "pending_cancel" => Ok(OrderReceiptState::PendingCancel),
        "partially_filled" => Ok(OrderReceiptState::PartiallyFilled),
        "filled" => Ok(OrderReceiptState::Filled),
        "done_for_day" => Ok(OrderReceiptState::DoneForDay),
        "canceled" => Ok(OrderReceiptState::Canceled),
        "expired" => Ok(OrderReceiptState::Expired),
        "replaced" => Ok(OrderReceiptState::Replaced),
        "stopped" => Ok(OrderReceiptState::Stopped),
        "rejected" => Ok(OrderReceiptState::Rejected),
        "suspended" => Ok(OrderReceiptState::Suspended),
        "calculated" => Ok(OrderReceiptState::Calculated),
        "failed" => Ok(OrderReceiptState::Failed),
        other => Err(PaperDispatchError::UnsupportedReceiptStatus(
            other.to_owned(),
        )),
    }
}

/// Broker clock 与交易日历共同推导的 session 状态；Closed 时不代表可创建 Paper
/// 订单，`session_date` 也不能单独证明时段或资产资格。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketClock {
    // clock 的 session_date/session 来自 broker timestamp 与交易日历，Paper 调度不使用本地时区猜测。
    pub is_open: bool,
    pub session_date: NaiveDate,
    pub session: akzio_domain::TradingSessionSnapshot,
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
        // range 只选择固定 Alpaca history endpoint/query，调用方仍需独立处理缺失 benchmark/样本。
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
    // AlpacaPaper 保存受控 HTTP client、已验证 Paper base URL 和凭据；不提供 Live endpoint fallback。
    // 因本结构也派生 Debug，格式化整个 adapter 会递归暴露 credentials.secret_key；不要将其写入日志。
    client: Client,
    base_url: String,
    credentials: PaperCredentials,
}

pub fn is_alpaca_paper_base_url(supplied: &str) -> bool {
    // endpoint 校验同时限制 HTTPS、精确 host、无端口/用户信息/query/fragment 和根路径，避免 redirect/兼容 URL 绕过 Paper 绝缘。
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

// 协议、transport、execute 和 reconcile 分区通过 include 共享上述类型，但权限与错误边界仍由本模块统一定义。
include!("paper/execute.rs");
include!("paper/reconcile.rs");
include!("paper/transport.rs");
include!("paper/broker.rs");
include!("paper/protocol.rs");
