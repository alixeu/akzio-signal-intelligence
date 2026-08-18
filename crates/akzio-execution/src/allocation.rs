//! Model-free conversion from a typed decision and broker snapshots to orders.

use akzio_domain::{
    AccountSnapshot, ArtifactRef, Asset, ContentHash, DecisionContext, DomainError, ExecutionPlan,
    FactorExposure, MarketClockSnapshot, MoneyMicros, OrderIntent, OrderSide, QuoteSnapshot,
    WeightPpm, V2_DOMAIN_SCHEMA_VERSION,
};
use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::{
    protected_limit_price, scaled_weight, validate_quote, ExecutionError, ExecutionPolicy,
    WEIGHT_SCALE,
};

#[derive(Debug, Error)]
pub enum AllocationError {
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Execution(#[from] ExecutionError),
    #[error("execution allocation requires an accepted decision context")]
    DecisionRejected,
    #[error("execution snapshots do not describe the same broker session")]
    SessionMismatch,
    #[error("market is closed")]
    MarketClosed,
}

pub type AllocationResult<T> = std::result::Result<T, AllocationError>;

#[derive(Debug, Clone)]
pub struct AllocationInput {
    pub decision_context_ref: ArtifactRef,
    pub decision_context: DecisionContext,
    pub account_snapshot_ref: ArtifactRef,
    pub account: AccountSnapshot,
    pub quote_snapshot_ref: ArtifactRef,
    pub quotes: QuoteSnapshot,
    pub market_clock_snapshot_ref: ArtifactRef,
    pub clock: MarketClockSnapshot,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct V2AllocationRuntime {
    policy: ExecutionPolicy,
}

impl V2AllocationRuntime {
    pub fn new(policy: ExecutionPolicy) -> AllocationResult<Self> {
        policy.validate()?;
        Ok(Self { policy })
    }

    pub fn policy(&self) -> &ExecutionPolicy {
        &self.policy
    }

    pub fn allocate(&self, input: &AllocationInput) -> AllocationResult<ExecutionPlan> {
        input.decision_context.validate()?;
        input.account.validate()?;
        input.quotes.validate()?;
        input.clock.validate()?;
        if !input.decision_context.accepted() {
            return Err(AllocationError::DecisionRejected);
        }
        if input.account.broker_session != input.quotes.broker_session
            || input.account.broker_session != input.clock.broker_session
        {
            return Err(AllocationError::SessionMismatch);
        }
        if !input.clock.is_open {
            return Err(AllocationError::MarketClosed);
        }
        Ok(build_execution_plan(&self.policy, input)?)
    }
}

fn build_execution_plan(
    policy: &ExecutionPolicy,
    input: &AllocationInput,
) -> std::result::Result<ExecutionPlan, ExecutionError> {
    let target = &input.decision_context.target;
    let account = &input.account;
    let quotes = &input.quotes;
    let now = input.now;
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
            policy.max_future_skew_secs,
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
        decision_context: input.decision_context_ref.clone(),
        account_snapshot: input.account_snapshot_ref.clone(),
        quote_snapshot: input.quote_snapshot_ref.clone(),
        market_clock_snapshot: input.market_clock_snapshot_ref.clone(),
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use akzio_domain::{
        ArtifactId, ArtifactKind, Asset, ContentHash, DecisionId, HardBlocker, MoneyMicros,
        Position, Quote, RunId, TargetPortfolio, WeightPpm, V2_SCHEMA_VERSION,
    };
    use chrono::Utc;

    use super::*;

    fn reference(kind: ArtifactKind, name: &[u8]) -> ArtifactRef {
        ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(name)),
            kind,
        }
    }

    fn policy() -> ExecutionPolicy {
        ExecutionPolicy {
            assets: Asset::EXECUTABLE.into_iter().collect::<BTreeSet<_>>(),
            max_gross_weight: WeightPpm(500_000),
            max_new_notional: MoneyMicros::from_usd_cents(1_000_000),
            max_daily_turnover: WeightPpm(500_000),
            max_account_age_secs: 5,
            max_quote_age_secs: 5,
            max_clock_age_secs: 5,
            max_future_skew_secs: 1,
            max_snapshot_skew_secs: 2,
            max_spread_bps: 20,
            limit_protection_bps: 10,
        }
    }

    fn input(blockers: Vec<HardBlocker>) -> AllocationInput {
        let now = Utc::now();
        let mut target = TargetPortfolio::zeroed();
        target.weights.insert(Asset::Tqqq, WeightPpm(100_000));
        AllocationInput {
            decision_context_ref: reference(ArtifactKind::DecisionContext, b"decision"),
            decision_context: DecisionContext {
                schema_version: V2_SCHEMA_VERSION,
                decision_id: DecisionId::new(),
                run_id: RunId::new(),
                claims: vec![reference(ArtifactKind::Claim, b"claim")],
                critiques: vec![],
                evidence: vec![],
                policy_influences: vec![],
                material_conflicts: vec![],
                hard_blockers: blockers,
                soft_warnings: vec![],
                decision_policy_hash: ContentHash::of_bytes(b"fixture-decision-policy"),
                target,
                created_at: now,
            },
            account_snapshot_ref: reference(ArtifactKind::NormalizedEvidence, b"account"),
            account: AccountSnapshot {
                schema_version: V2_SCHEMA_VERSION,
                broker_session: "2026-08-10".to_owned(),
                observed_at: now,
                equity: MoneyMicros::from_usd_cents(1_000_000),
                buying_power: MoneyMicros::from_usd_cents(1_000_000),
                day_turnover: MoneyMicros::ZERO,
                active: true,
                trading_blocked: false,
                positions: BTreeMap::<Asset, Position>::new(),
                external_positions: BTreeSet::new(),
                open_order_ids: BTreeSet::new(),
            },
            quote_snapshot_ref: reference(ArtifactKind::NormalizedEvidence, b"quotes"),
            quotes: QuoteSnapshot {
                schema_version: V2_SCHEMA_VERSION,
                broker_session: "2026-08-10".to_owned(),
                observed_at: now,
                quotes: BTreeMap::from([(
                    Asset::Tqqq,
                    Quote {
                        bid: MoneyMicros::from_usd_cents(10_000),
                        ask: MoneyMicros::from_usd_cents(10_010),
                        observed_at: now,
                    },
                )]),
            },
            market_clock_snapshot_ref: reference(ArtifactKind::NormalizedEvidence, b"clock"),
            clock: MarketClockSnapshot {
                schema_version: V2_SCHEMA_VERSION,
                broker_session: "2026-08-10".to_owned(),
                is_open: true,
                observed_at: now,
            },
            now,
        }
    }

    #[test]
    fn accepted_decision_is_allocated_by_explicit_policy() {
        let plan = V2AllocationRuntime::new(policy())
            .unwrap()
            .allocate(&input(vec![]))
            .unwrap();
        assert_eq!(plan.orders.len(), 1);
        assert!(plan.validate().is_ok());
    }

    #[test]
    fn blocked_decision_is_not_allocated() {
        let error = V2AllocationRuntime::new(policy())
            .unwrap()
            .allocate(&input(vec![HardBlocker::Frozen]))
            .unwrap_err();
        assert!(matches!(error, AllocationError::DecisionRejected));
    }
}
