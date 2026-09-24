//! Rust-owned Paper execution policy and deterministic order planning.

// 文件导读：本 crate 把研究产出的 DecisionContext 转成受 Rust 约束的目标、订单、
// ExecutionVerdict 和 Paper Commitment。DecisionGate 只决定目标与研究状态，
// ExecutionGate 重新核验账户/报价/时钟及风控，真正的 Broker I/O 还要等 Commitment
// 已在 Store 中持久化之后；因此这里的数值、来源和哈希都服务于可恢复的 Paper 边界。

pub mod allocation;
pub mod calibration;
pub mod decision_gate;
pub mod execution_gate;
pub mod paper;
pub mod paper_commitment;
pub mod policy;
mod pretrade_safety;
pub mod reconciliation;
pub mod snapshot;

pub use allocation::{AllocationError, AllocationInput, AllocationRuntime};
pub use calibration::{
    build_offline_decision_policy, DecisionPolicyArtifact, DecisionPolicyProvenance,
    HistoricalForecastProvenance, HistoricalForecastSample, HistoricalPricePoint,
    HistoricalPriceSeries, OfflineCalibrationError, OfflineCalibrationInput,
    OfflineCalibrationResult, OfflineRiskLimits,
};
pub use decision_gate::{
    AssetRiskCalibration, DecisionGateError, DecisionGateInput, DecisionGateOutput, DecisionPolicy,
    DecisionRuntime, ForecastCalibrationBin, ForecastCalibrationScope, FrozenForecastCalibration,
    PortfolioRiskModel,
};
pub use execution_gate::{
    ExecutionGateError, ExecutionGateInput, ExecutionGateOutput, ExecutionRuntime,
};
pub use paper::{
    PaperDispatchError, PaperDispatchFailpoint, PaperDispatchInput, PaperDispatchOutput,
    PaperDispatchRuntime, DEFAULT_PAPER_SETTLEMENT_ACTION_GRACE_SECS,
    DEFAULT_PAPER_SETTLEMENT_TIMEOUT_SECS,
};
pub use paper_commitment::{
    PaperCommitmentError, PaperCommitmentInput, PaperCommitmentOutput, PaperCommitmentRuntime,
};
pub use policy::ExecutionGatePolicy;
pub use pretrade_safety::{
    PreTradeSafetyAssessment, PreTradeSafetyEvidence, PreTradeSafetyInput, PreTradeSafetyPolicy,
};
pub use reconciliation::{
    ReconciliationError, ReconciliationInput, ReconciliationOutput, ReconciliationRuntime,
};
pub use snapshot::{
    materialize_snapshot_artifact, ExecutionSnapshotPayload, SnapshotArtifactError,
};

use std::collections::BTreeSet;

use akzio_domain::{content_hash_json, ArtifactProvenance, Asset, ContentHash, TaskWritePermit};
pub use akzio_domain::{
    AccountSnapshot, ExecutionPlan, MarketClockSnapshot, MoneyMicros, OrderIntent, OrderSide,
    Position, Quote, QuoteSnapshot, WeightPpm,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub(crate) fn trusted_execution_provenance(
    permit: &TaskWritePermit,
    now: DateTime<Utc>,
) -> ArtifactProvenance {
    // 执行侧产物统一继承当前 task permit 的 Contract 身份；这只构造 provenance，
    // 不负责提交 Artifact，提交仍由各阶段自己的 fenced Store 事务完成。
    ArtifactProvenance {
        source_family: "akzio.execution".to_owned(),
        observed_at: Some(now),
        retrieved_at: now,
        source_uri: None,
        confidence_ppm: 1_000_000,
        producer_contract_hash: permit.contract_hash.clone(),
    }
}

const WEIGHT_SCALE: i128 = 1_000_000;
const BPS_SCALE: i64 = 10_000;
const PAPER_PRICE_TICK_MICROS: i64 = 10_000;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ExecutionError {
    #[error("execution policy is invalid")]
    InvalidPolicy,
    #[error("target asset {0} is not permitted")]
    ForbiddenAsset(Asset),
    #[error("target weights exceed {0} ppm gross exposure limit")]
    GrossExposureExceeded(u32),
    #[error("account is not available for trading")]
    AccountBlocked,
    #[error("buying power is insufficient")]
    InsufficientBuyingPower,
    #[error("quote for {0} is missing")]
    MissingQuote(Asset),
    #[error("quote for {0} is too old")]
    StaleQuote(Asset),
    #[error("quote for {0} is invalid or too wide")]
    InvalidQuote(Asset),
    #[error("position for {0} is short")]
    ShortPosition(Asset),
    #[error("new notional exceeds the per-run limit")]
    NewNotionalExceeded,
    #[error("daily turnover limit exceeded")]
    DailyTurnoverExceeded,
    #[error("target weight for {0} is invalid")]
    InvalidWeight(Asset),
    #[error("target produces no executable order")]
    NoExecutableOrder,
}

pub type Result<T> = std::result::Result<T, ExecutionError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPolicy {
    pub assets: BTreeSet<Asset>,
    pub max_gross_weight: WeightPpm,
    pub max_new_notional: MoneyMicros,
    pub max_daily_turnover: WeightPpm,
    pub max_account_age_secs: i64,
    pub max_quote_age_secs: i64,
    pub max_clock_age_secs: i64,
    pub max_future_skew_secs: i64,
    pub max_snapshot_skew_secs: i64,
    pub max_spread_bps: u32,
    pub limit_protection_bps: u32,
}

impl ExecutionPolicy {
    pub fn validate(&self) -> Result<()> {
        // 先锁定四个允许执行的 ETF 和所有时间/金额上限，避免调用方用一个“看似
        // 合法”的自定义资产集合绕过 Paper 执行边界。
        let executable_assets = Asset::EXECUTABLE.into_iter().collect::<BTreeSet<_>>();
        if self.assets != executable_assets
            || self.max_gross_weight.0 > WeightPpm::SCALE
            || self.max_daily_turnover.0 > WeightPpm::SCALE
            || self.max_new_notional.0 <= 0
            || self.max_account_age_secs < 0
            || self.max_quote_age_secs < 0
            || self.max_clock_age_secs < 0
            || self.max_future_skew_secs < 0
            || self.max_snapshot_skew_secs < 0
            || self.max_spread_bps > 10_000
            || self.limit_protection_bps > 10_000
        {
            return Err(ExecutionError::InvalidPolicy);
        }
        Ok(())
    }

    pub fn policy_hash(&self) -> Result<ContentHash> {
        // 只有通过同一套校验的策略才参与哈希，后续 ExecutionPlan 会用它绑定风控版本。
        self.validate()?;
        let value = serde_json::to_value(self).map_err(|_| ExecutionError::InvalidPolicy)?;
        content_hash_json(&value).map_err(|_| ExecutionError::InvalidPolicy)
    }
}

impl Default for ExecutionPolicy {
    fn default() -> Self {
        // 默认值是 Paper 的保守运行上限，不代表审批或校准已经就绪；DecisionPolicy
        // 的 fail-closed 状态仍由上层单独判断。
        Self {
            assets: Asset::EXECUTABLE.into_iter().collect(),
            max_gross_weight: WeightPpm(1_000_000),
            max_new_notional: MoneyMicros::from_usd_cents(2_000_000),
            max_daily_turnover: WeightPpm(1_000_000),
            max_account_age_secs: 5,
            max_quote_age_secs: 5,
            max_clock_age_secs: 5,
            max_future_skew_secs: 15,
            max_snapshot_skew_secs: 15,
            max_spread_bps: 20,
            limit_protection_bps: 10,
        }
    }
}

fn scaled_weight(equity: MoneyMicros, weight: WeightPpm) -> MoneyMicros {
    let value = i128::from(equity.0).saturating_mul(i128::from(weight.0)) / WEIGHT_SCALE;
    MoneyMicros(i64::try_from(value).unwrap_or(i64::MAX))
}

pub(crate) fn validate_quote(
    max_age_secs: i64,
    max_future_skew_secs: i64,
    max_spread_bps: u32,
    asset: Asset,
    quote: Quote,
    now: DateTime<Utc>,
) -> Result<()> {
    // 报价同时检查新鲜度、未来偏移、正 bid/ask 和价差；任一项失败都在订单生成前
    // 返回，卖单收益不会被当作买单购买力的先验保证。
    let age = now.signed_duration_since(quote.observed_at);
    if age > chrono::Duration::seconds(max_age_secs)
        || age < -chrono::Duration::seconds(max_future_skew_secs)
    {
        return Err(ExecutionError::StaleQuote(asset));
    }
    if quote.bid.0 <= 0 || quote.ask.0 <= quote.bid.0 {
        return Err(ExecutionError::InvalidQuote(asset));
    }
    let midpoint = (quote.bid.0 + quote.ask.0) / 2;
    let spread_bps = (quote.ask.0 - quote.bid.0).saturating_mul(BPS_SCALE) / midpoint;
    if spread_bps > i64::from(max_spread_bps) {
        return Err(ExecutionError::InvalidQuote(asset));
    }
    Ok(())
}

pub(crate) fn protected_limit_price(
    quote: Quote,
    side: OrderSide,
    protection_bps: u32,
) -> MoneyMicros {
    // 用报价一侧加/减固定保护幅度，并按 Paper 价格 tick 做方向一致的舍入，确保
    // 订单 wire 值仍由 Rust 从已验证 Quote 确定地产生。
    let protection = i64::from(protection_bps);
    let raw = match side {
        OrderSide::Buy => quote.ask.0.saturating_mul(BPS_SCALE + protection) / BPS_SCALE,
        OrderSide::Sell => quote.bid.0.saturating_mul(BPS_SCALE - protection) / BPS_SCALE,
    };
    let rounded = match side {
        OrderSide::Buy => raw / PAPER_PRICE_TICK_MICROS * PAPER_PRICE_TICK_MICROS,
        OrderSide::Sell => {
            let ticks = raw / PAPER_PRICE_TICK_MICROS;
            let ticks = ticks.saturating_add(i64::from(raw % PAPER_PRICE_TICK_MICROS != 0));
            ticks.saturating_mul(PAPER_PRICE_TICK_MICROS)
        }
    };
    MoneyMicros(rounded)
}
