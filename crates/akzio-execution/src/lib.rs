//! Rust-owned Paper execution policy and deterministic order planning.

pub mod allocation;
pub mod decision_gate;
pub mod execution_gate;
pub mod paper;
pub mod paper_commitment;
pub mod policy;
pub mod reconciliation;
pub mod reprice;

pub use allocation::{AllocationError, AllocationInput, V2AllocationRuntime};
pub use decision_gate::{
    DecisionGateError, DecisionGateInput, DecisionGateOutput, DecisionPolicy, V2DecisionRuntime,
};
pub use execution_gate::{
    ExecutionGateError, ExecutionGateInput, ExecutionGateOutput, V2ExecutionRuntime,
};
pub use paper::{
    PaperDispatchError, PaperDispatchInput, PaperDispatchOutput, PaperRepriceDispatchInput,
    V2PaperDispatchRuntime,
};
pub use paper_commitment::{
    PaperCommitmentError, PaperCommitmentInput, PaperCommitmentOutput, V2PaperCommitmentRuntime,
};
pub use policy::ExecutionGatePolicy;
pub use reconciliation::{
    ReconciliationError, ReconciliationInput, ReconciliationOutput, V2ReconciliationRuntime,
};
pub use reprice::{RepriceError, RepriceInput, RepriceOutput, V2RepriceRuntime};

use std::collections::BTreeSet;

use akzio_domain::{
    content_hash_json, ArtifactRef, Asset, ContentHash, FactorExposure, TargetPortfolio,
    V2_DOMAIN_SCHEMA_VERSION,
};
pub use akzio_domain::{
    AccountSnapshot, ExecutionPlan, MarketClockSnapshot, MoneyMicros, OrderIntent, OrderSide,
    Position, Quote, QuoteSnapshot, WeightPpm,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const WEIGHT_SCALE: i128 = 1_000_000;
const BPS_SCALE: i64 = 10_000;

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
            max_spread_bps: 20,
            limit_protection_bps: 10,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn build_execution_plan(
    policy: &ExecutionPolicy,
    decision_context: ArtifactRef,
    account_snapshot: ArtifactRef,
    quote_snapshot: ArtifactRef,
    market_clock_snapshot: ArtifactRef,
    target: &TargetPortfolio,
    account: &AccountSnapshot,
    quotes: &QuoteSnapshot,
    now: DateTime<Utc>,
) -> Result<ExecutionPlan> {
    policy.validate()?;
    if !account.active || account.trading_blocked || account.equity.0 <= 0 {
        return Err(ExecutionError::AccountBlocked);
    }

    target
        .validate_universe()
        .map_err(|_| ExecutionError::InvalidPolicy)?;
    let gross = target
        .weights
        .iter()
        .try_fold(0_u32, |sum, (asset, weight)| {
            if !policy.assets.contains(asset) {
                return Err(ExecutionError::ForbiddenAsset(*asset));
            }
            if weight.0 > WeightPpm::SCALE {
                return Err(ExecutionError::InvalidWeight(*asset));
            }
            sum.checked_add(weight.0)
                .ok_or(ExecutionError::GrossExposureExceeded(
                    policy.max_gross_weight.0,
                ))
        })?;
    if gross > policy.max_gross_weight.0 {
        return Err(ExecutionError::GrossExposureExceeded(
            policy.max_gross_weight.0,
        ));
    }

    let mut orders = Vec::new();
    let mut new_notional = 0_i128;
    let mut order_turnover = 0_i128;
    for asset in Asset::EXECUTABLE {
        let target_value = scaled_weight(account.equity, target.weights[&asset]);
        let current_value = account
            .positions
            .get(&asset)
            .map_or(MoneyMicros::ZERO, |position| position.market_value);
        let delta = i128::from(target_value.0) - i128::from(current_value.0);
        if delta == 0 {
            continue;
        }
        if target_value.0 < 0 || current_value.0 < 0 {
            return Err(ExecutionError::ShortPosition(asset));
        }
        let quote = *quotes
            .quotes
            .get(&asset)
            .ok_or(ExecutionError::MissingQuote(asset))?;
        validate_quote(
            policy.max_quote_age_secs,
            policy.max_spread_bps,
            asset,
            quote,
            now,
        )?;
        let side = if delta > 0 {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        let notional = MoneyMicros(
            i64::try_from(delta.unsigned_abs()).map_err(|_| ExecutionError::NewNotionalExceeded)?,
        );
        if side == OrderSide::Buy {
            new_notional = new_notional.saturating_add(i128::from(notional.0));
        }
        order_turnover = order_turnover.saturating_add(i128::from(notional.0));
        orders.push(OrderIntent {
            asset,
            side,
            notional,
            limit_price: protected_limit_price(quote, side, policy.limit_protection_bps),
        });
    }

    if orders.is_empty() {
        return Err(ExecutionError::NoExecutableOrder);
    }
    if new_notional > i128::from(policy.max_new_notional.0) {
        return Err(ExecutionError::NewNotionalExceeded);
    }
    if new_notional > i128::from(account.buying_power.0) {
        return Err(ExecutionError::InsufficientBuyingPower);
    }

    let total_turnover = i128::from(account.day_turnover.0).saturating_add(order_turnover);
    let turnover_ppm = total_turnover
        .saturating_mul(WEIGHT_SCALE)
        .checked_div(i128::from(account.equity.0))
        .and_then(|value| u32::try_from(value).ok())
        .ok_or(ExecutionError::DailyTurnoverExceeded)?;
    if turnover_ppm > policy.max_daily_turnover.0 {
        return Err(ExecutionError::DailyTurnoverExceeded);
    }

    orders.sort_by_key(|order| (matches!(order.side, OrderSide::Buy), order.asset));
    let factor_exposure =
        FactorExposure::from_target(target).map_err(|_| ExecutionError::InvalidPolicy)?;
    let mut plan = ExecutionPlan {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        decision_context,
        account_snapshot,
        quote_snapshot,
        market_clock_snapshot,
        policy_hash: policy.policy_hash()?,
        target: target.clone(),
        orders,
        gross_exposure_ppm: gross,
        net_exposure_ppm: i64::from(gross),
        factor_exposure,
        turnover_ppm,
        broker_session: account.broker_session.clone(),
        created_at: now,
        plan_hash: ContentHash::of_bytes(b"pending execution plan hash"),
    };
    plan.refresh_hash()
        .map_err(|_| ExecutionError::InvalidPolicy)?;
    Ok(plan)
}

fn scaled_weight(equity: MoneyMicros, weight: WeightPpm) -> MoneyMicros {
    let value = i128::from(equity.0).saturating_mul(i128::from(weight.0)) / WEIGHT_SCALE;
    MoneyMicros(i64::try_from(value).unwrap_or(i64::MAX))
}

pub(crate) fn validate_quote(
    max_age_secs: i64,
    max_spread_bps: u32,
    asset: Asset,
    quote: Quote,
    now: DateTime<Utc>,
) -> Result<()> {
    if now.signed_duration_since(quote.observed_at) > chrono::Duration::seconds(max_age_secs) {
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
    match side {
        OrderSide::Buy => {
            MoneyMicros(quote.ask.0.saturating_mul(BPS_SCALE + protection) / BPS_SCALE)
        }
        OrderSide::Sell => {
            MoneyMicros(quote.bid.0.saturating_mul(BPS_SCALE - protection) / BPS_SCALE)
        }
    }
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
}
