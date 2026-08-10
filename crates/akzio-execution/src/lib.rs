//! Rust-owned Paper execution policy and deterministic order planning.
//!
//! This module has no model dependency.  An agent may explain a decision, but
//! only this module can turn a target allocation into broker-safe order intent.

pub mod allocation;
pub mod execution_gate;
pub mod paper;
pub mod paper_commitment;
pub mod policy;
pub mod reconciliation;
pub mod reprice;
pub mod runtime;

pub use allocation::{AllocationError, AllocationInput, V2AllocationRuntime};
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

pub use runtime::{
    DecisionGatePolicy, ExecutionRunContext, ExecutionRuntime, ExecutionRuntimeError,
};

use std::collections::{BTreeMap, BTreeSet};

use akzio_domain::{content_hash_json, Asset, ContentHash};
pub use akzio_domain::{MoneyMicros, WeightPpm};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const WEIGHT_SCALE: i64 = 1_000_000;
const BPS_SCALE: i64 = 10_000;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ExecutionError {
    #[error("target asset {0} is not permitted")]
    ForbiddenAsset(Asset),
    #[error("target weights exceed the {0} ppm gross exposure limit")]
    GrossExposureExceeded(u32),
    #[error("account is not available for trading")]
    AccountBlocked,
    #[error("buying power is insufficient")]
    InsufficientBuyingPower,
    #[error("quote for {0} is too old")]
    StaleQuote(Asset),
    #[error("quote for {0} has an invalid or too-wide spread")]
    InvalidQuote(Asset),
    #[error("position for {0} would become short")]
    ShortPosition(Asset),
    #[error("new notional exceeds the per-run limit")]
    NewNotionalExceeded,
    #[error("daily turnover exceeds the policy limit")]
    DailyTurnoverExceeded,
    #[error("target weight for {0} is invalid")]
    InvalidWeight(Asset),
}

pub type Result<T> = std::result::Result<T, ExecutionError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPolicy {
    pub assets: BTreeSet<Asset>,
    pub max_gross_weight: WeightPpm,
    pub max_new_notional: MoneyMicros,
    pub max_daily_turnover: WeightPpm,
    pub max_quote_age_secs: i64,
    pub max_spread_bps: u32,
    pub limit_protection_bps: u32,
}

impl Default for ExecutionPolicy {
    fn default() -> Self {
        Self {
            assets: Asset::EXECUTABLE.into_iter().collect(),
            max_gross_weight: WeightPpm(200_000),
            max_new_notional: MoneyMicros::from_usd_cents(2_500_000),
            max_daily_turnover: WeightPpm(300_000),
            max_quote_age_secs: 5,
            max_spread_bps: 20,
            limit_protection_bps: 10,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Target {
    pub asset: Asset,
    pub weight: WeightPpm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub market_value: MoneyMicros,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountSnapshot {
    pub equity: MoneyMicros,
    pub buying_power: MoneyMicros,
    pub active: bool,
    pub trading_blocked: bool,
    pub positions: BTreeMap<Asset, Position>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quote {
    pub bid: MoneyMicros,
    pub ask: MoneyMicros,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderIntent {
    pub asset: Asset,
    pub side: OrderSide,
    pub notional: MoneyMicros,
    pub limit_price: MoneyMicros,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPlan {
    pub policy: ExecutionPolicy,
    pub targets: Vec<Target>,
    pub orders: Vec<OrderIntent>,
    pub plan_hash: ContentHash,
}

pub fn build_execution_plan(
    policy: ExecutionPolicy,
    account: &AccountSnapshot,
    quotes: &BTreeMap<Asset, Quote>,
    targets: &[Target],
    daily_turnover: MoneyMicros,
    now: DateTime<Utc>,
) -> Result<ExecutionPlan> {
    if !account.active || account.trading_blocked || account.equity.0 <= 0 {
        return Err(ExecutionError::AccountBlocked);
    }
    let gross = targets.iter().try_fold(0_u32, |sum, target| {
        if !policy.assets.contains(&target.asset) {
            return Err(ExecutionError::ForbiddenAsset(target.asset));
        }
        sum.checked_add(target.weight.0)
            .ok_or(ExecutionError::GrossExposureExceeded(
                policy.max_gross_weight.0,
            ))
    })?;
    if gross > policy.max_gross_weight.0 {
        return Err(ExecutionError::GrossExposureExceeded(
            policy.max_gross_weight.0,
        ));
    }
    if daily_turnover.0 > scaled_weight(account.equity, policy.max_daily_turnover).0 {
        return Err(ExecutionError::DailyTurnoverExceeded);
    }

    let mut orders = Vec::new();
    let mut new_notional = MoneyMicros::ZERO;
    for target in targets {
        let target_value = scaled_weight(account.equity, target.weight);
        let current_value = account
            .positions
            .get(&target.asset)
            .map_or(MoneyMicros::ZERO, |position| position.market_value);
        let delta = target_value.0 - current_value.0;
        if delta == 0 {
            continue;
        }
        if target_value.0 < 0 || current_value.0 < 0 {
            return Err(ExecutionError::ShortPosition(target.asset));
        }
        let quote = quotes
            .get(&target.asset)
            .copied()
            .ok_or(ExecutionError::StaleQuote(target.asset))?;
        validate_quote(
            policy.max_quote_age_secs,
            policy.max_spread_bps,
            target.asset,
            quote,
            now,
        )?;
        let side = if delta > 0 {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        let notional = MoneyMicros(delta.abs());
        if side == OrderSide::Buy {
            new_notional.0 += notional.0;
        }
        let limit_price = protected_limit_price(quote, side, policy.limit_protection_bps);
        orders.push(OrderIntent {
            asset: target.asset,
            side,
            notional,
            limit_price,
        });
    }
    if new_notional.0 > policy.max_new_notional.0 {
        return Err(ExecutionError::NewNotionalExceeded);
    }
    if new_notional.0 > account.buying_power.0 {
        return Err(ExecutionError::InsufficientBuyingPower);
    }
    orders.sort_by_key(|order| (matches!(order.side, OrderSide::Buy), order.asset));
    let payload = serde_json::to_value((&policy, targets, &orders))
        .expect("execution plan payload must serialize");
    Ok(ExecutionPlan {
        policy,
        targets: targets.to_vec(),
        orders,
        plan_hash: content_hash_json(&payload).expect("execution plan payload must hash"),
    })
}

fn scaled_weight(equity: MoneyMicros, weight: WeightPpm) -> MoneyMicros {
    MoneyMicros(equity.0.saturating_mul(weight.0 as i64) / WEIGHT_SCALE)
}

pub(crate) fn validate_quote(
    max_age_secs: i64,
    max_spread_bps: u32,
    asset: Asset,
    quote: Quote,
    now: DateTime<Utc>,
) -> Result<()> {
    if now - quote.observed_at > chrono::Duration::seconds(max_age_secs) {
        return Err(ExecutionError::StaleQuote(asset));
    }
    if quote.bid.0 <= 0 || quote.ask.0 <= quote.bid.0 {
        return Err(ExecutionError::InvalidQuote(asset));
    }
    let midpoint = (quote.bid.0 + quote.ask.0) / 2;
    let spread_bps = (quote.ask.0 - quote.bid.0).saturating_mul(BPS_SCALE) / midpoint;
    if spread_bps > max_spread_bps as i64 {
        return Err(ExecutionError::InvalidQuote(asset));
    }
    Ok(())
}

pub(crate) fn protected_limit_price(
    quote: Quote,
    side: OrderSide,
    protection_bps: u32,
) -> MoneyMicros {
    let protection = protection_bps as i64;
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
mod tests {
    use super::*;

    fn account() -> AccountSnapshot {
        AccountSnapshot {
            equity: MoneyMicros::from_usd_cents(1_000_000),
            buying_power: MoneyMicros::from_usd_cents(1_000_000),
            active: true,
            trading_blocked: false,
            positions: BTreeMap::new(),
        }
    }

    fn quotes(now: DateTime<Utc>) -> BTreeMap<Asset, Quote> {
        Asset::EXECUTABLE
            .into_iter()
            .map(|asset| {
                (
                    asset,
                    Quote {
                        bid: MoneyMicros::from_usd_cents(10_000),
                        ask: MoneyMicros::from_usd_cents(10_001),
                        observed_at: now,
                    },
                )
            })
            .collect()
    }

    #[test]
    fn global_exposure_cap_is_shared_by_all_executable_assets() {
        let now = Utc::now();
        let targets = [
            Target {
                asset: Asset::Tqqq,
                weight: WeightPpm(100_000),
            },
            Target {
                asset: Asset::Qqq,
                weight: WeightPpm(100_001),
            },
        ];
        assert_eq!(
            build_execution_plan(
                ExecutionPolicy::default(),
                &account(),
                &quotes(now),
                &targets,
                MoneyMicros::ZERO,
                now
            ),
            Err(ExecutionError::GrossExposureExceeded(200_000))
        );
    }

    #[test]
    fn plan_uses_a_fresh_protected_limit_order() {
        let now = Utc::now();
        let plan = build_execution_plan(
            ExecutionPolicy::default(),
            &account(),
            &quotes(now),
            &[Target {
                asset: Asset::Tqqq,
                weight: WeightPpm(100_000),
            }],
            MoneyMicros::ZERO,
            now,
        )
        .unwrap();
        assert_eq!(plan.orders.len(), 1);
        assert_eq!(plan.orders[0].side, OrderSide::Buy);
        assert!(plan.orders[0].limit_price.0 > MoneyMicros::from_usd_cents(10_001).0);
    }

    #[test]
    fn stale_quotes_fail_closed() {
        let now = Utc::now();
        let mut quotes = quotes(now - chrono::Duration::seconds(6));
        assert_eq!(
            build_execution_plan(
                ExecutionPolicy::default(),
                &account(),
                &quotes,
                &[Target {
                    asset: Asset::Soxl,
                    weight: WeightPpm(1)
                }],
                MoneyMicros::ZERO,
                now,
            ),
            Err(ExecutionError::StaleQuote(Asset::Soxl))
        );
        quotes.clear();
    }
}
