//! Model-free conversion from a typed decision and broker snapshots to orders.

use akzio_domain::{
    AccountSnapshot, ArtifactRef, DecisionContext, DomainError, ExecutionPlan, MarketClockSnapshot,
    QuoteSnapshot,
};
use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::{build_execution_plan, ExecutionError, ExecutionPolicy};

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
        Ok(build_execution_plan(
            &self.policy,
            input.decision_context_ref.clone(),
            input.account_snapshot_ref.clone(),
            input.quote_snapshot_ref.clone(),
            input.market_clock_snapshot_ref.clone(),
            &input.decision_context.target,
            &input.account,
            &input.quotes,
            input.now,
        )?)
    }
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
