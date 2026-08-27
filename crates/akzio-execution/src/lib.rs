//! Rust-owned Paper execution policy and deterministic order planning.

pub mod allocation;
pub mod decision_gate;
pub mod execution_gate;
pub mod paper;
pub mod paper_commitment;
pub mod policy;
pub mod reconciliation;
pub mod snapshot;

pub use allocation::{AllocationError, AllocationInput, V2AllocationRuntime};
pub use decision_gate::{
    DecisionGateError, DecisionGateInput, DecisionGateOutput, DecisionPolicy, V2DecisionRuntime,
};
pub use execution_gate::{
    ExecutionGateError, ExecutionGateInput, ExecutionGateOutput, V2ExecutionRuntime,
};
pub use paper::{
    PaperDispatchError, PaperDispatchFailpoint, PaperDispatchInput, PaperDispatchOutput,
    V2PaperDispatchRuntime,
};
pub use paper_commitment::{
    PaperCommitmentError, PaperCommitmentInput, PaperCommitmentOutput, V2PaperCommitmentRuntime,
};
pub use policy::ExecutionGatePolicy;
pub use reconciliation::{
    ReconciliationError, ReconciliationInput, ReconciliationOutput, V2ReconciliationRuntime,
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
        self.validate()?;
        let value = serde_json::to_value(self).map_err(|_| ExecutionError::InvalidPolicy)?;
        content_hash_json(&value).map_err(|_| ExecutionError::InvalidPolicy)
    }
}

impl Default for ExecutionPolicy {
    fn default() -> Self {
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

#[cfg(test)]
mod policy_tests {
    use super::*;

    fn policy() -> ExecutionPolicy {
        ExecutionPolicy {
            assets: Asset::EXECUTABLE.into_iter().collect(),
            max_gross_weight: WeightPpm(1_000_000),
            max_new_notional: MoneyMicros::from_usd_cents(1),
            max_daily_turnover: WeightPpm(1_000_000),
            max_account_age_secs: 1,
            max_quote_age_secs: 1,
            max_clock_age_secs: 1,
            max_future_skew_secs: 1,
            max_snapshot_skew_secs: 1,
            max_spread_bps: 1,
            limit_protection_bps: 1,
        }
    }

    #[test]
    fn policy_requires_the_exact_v2_asset_universe() {
        let mut valid = policy();
        valid.validate().unwrap();

        valid.assets.remove(&Asset::Soxl);
        assert_eq!(valid.validate(), Err(ExecutionError::InvalidPolicy));
    }

    #[test]
    fn quote_validation_rejects_future_provider_timestamps() {
        let now = Utc::now();
        assert_eq!(
            validate_quote(
                5,
                1,
                20,
                Asset::Qqq,
                Quote {
                    bid: MoneyMicros::from_usd_cents(10_000),
                    ask: MoneyMicros::from_usd_cents(10_001),
                    observed_at: now + chrono::Duration::seconds(2),
                },
                now,
            ),
            Err(ExecutionError::StaleQuote(Asset::Qqq))
        );
    }

    #[test]
    fn default_policy_tolerates_bounded_broker_clock_skew() {
        let now = Utc::now();
        let policy = ExecutionPolicy::default();
        let quote = |seconds| Quote {
            bid: MoneyMicros::from_usd_cents(10_000),
            ask: MoneyMicros::from_usd_cents(10_001),
            observed_at: now + chrono::Duration::seconds(seconds),
        };
        assert!(validate_quote(
            policy.max_quote_age_secs,
            policy.max_future_skew_secs,
            policy.max_spread_bps,
            Asset::Qqq,
            quote(10),
            now,
        )
        .is_ok());
        assert_eq!(
            validate_quote(
                policy.max_quote_age_secs,
                policy.max_future_skew_secs,
                policy.max_spread_bps,
                Asset::Qqq,
                quote(16),
                now,
            ),
            Err(ExecutionError::StaleQuote(Asset::Qqq))
        );
        assert_eq!(policy.max_snapshot_skew_secs, 15);
    }

    #[test]
    fn protected_limits_round_to_broker_ticks_without_weakening_protection() {
        let quote = Quote {
            bid: MoneyMicros(76_500_000),
            ask: MoneyMicros(76_510_000),
            observed_at: Utc::now(),
        };
        assert_eq!(
            protected_limit_price(quote, OrderSide::Buy, 10),
            MoneyMicros(76_580_000)
        );
        assert_eq!(
            protected_limit_price(quote, OrderSide::Sell, 10),
            MoneyMicros(76_430_000)
        );
    }
}
