//! Model-free target-to-order allocation.
//!
//! A decision context supplies target weights only. This module owns the
//! conversion to protected order intents using Rust policy, account state and
//! quote freshness; no model output reaches the Paper adapter directly.

use std::collections::BTreeMap;

use akzio_domain::{Asset, DecisionContext, DomainError};
use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::{
    build_execution_plan, AccountSnapshot, ExecutionError, ExecutionPlan, ExecutionPolicy,
    MoneyMicros, Quote, Target,
};

#[derive(Debug, Error)]
pub enum AllocationError {
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Execution(#[from] ExecutionError),
    #[error("execution allocation requires an accepted decision context")]
    DecisionRejected,
}

pub type AllocationResult<T> = std::result::Result<T, AllocationError>;

#[derive(Debug, Clone)]
pub struct AllocationInput {
    pub decision_context: DecisionContext,
    pub account: AccountSnapshot,
    pub quotes: BTreeMap<Asset, Quote>,
    pub daily_turnover: MoneyMicros,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct V2AllocationRuntime {
    policy: ExecutionPolicy,
}

impl V2AllocationRuntime {
    pub fn new(policy: ExecutionPolicy) -> Self {
        Self { policy }
    }

    pub fn policy(&self) -> &ExecutionPolicy {
        &self.policy
    }

    pub fn allocate(&self, input: &AllocationInput) -> AllocationResult<ExecutionPlan> {
        input.decision_context.validate()?;
        if !input.decision_context.accepted() {
            return Err(AllocationError::DecisionRejected);
        }
        let targets = Asset::EXECUTABLE
            .into_iter()
            .map(|asset| Target {
                asset,
                weight: *input
                    .decision_context
                    .target
                    .weights
                    .get(&asset)
                    .expect("validated target portfolio contains every executable asset"),
            })
            .collect::<Vec<_>>();
        Ok(build_execution_plan(
            self.policy.clone(),
            &input.account,
            &input.quotes,
            &targets,
            input.daily_turnover,
            input.now,
        )?)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use akzio_domain::{
        ArtifactId, ArtifactKind, ArtifactRef, DecisionId, HardBlocker, RunId, TargetPortfolio,
        WeightPpm, REBUILD_SCHEMA_VERSION,
    };
    use chrono::Utc;

    use crate::{AccountSnapshot, ExecutionPolicy, MoneyMicros, Position, Quote};

    use super::{AllocationError, AllocationInput, V2AllocationRuntime};

    fn decision(blockers: Vec<HardBlocker>) -> akzio_domain::DecisionContext {
        let mut target = TargetPortfolio::zeroed();
        target
            .weights
            .insert(akzio_domain::Asset::Tqqq, WeightPpm(100_000));
        akzio_domain::DecisionContext {
            schema_version: REBUILD_SCHEMA_VERSION,
            decision_id: DecisionId::new(),
            run_id: RunId::new(),
            claims: vec![ArtifactRef {
                artifact_id: ArtifactId(akzio_domain::ContentHash::of_bytes(b"claim")),
                kind: ArtifactKind::Claim,
            }],
            critiques: vec![],
            evidence: vec![],
            material_conflicts: vec![],
            hard_blockers: blockers,
            soft_warnings: vec![],
            target,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn accepted_decision_context_is_allocated_by_rust_policy() {
        let now = Utc::now();
        let plan = V2AllocationRuntime::new(ExecutionPolicy::default())
            .allocate(&AllocationInput {
                decision_context: decision(vec![]),
                account: AccountSnapshot {
                    equity: MoneyMicros::from_usd_cents(1_000_000),
                    buying_power: MoneyMicros::from_usd_cents(1_000_000),
                    active: true,
                    trading_blocked: false,
                    positions: BTreeMap::<akzio_domain::Asset, Position>::new(),
                },
                quotes: BTreeMap::from([(
                    akzio_domain::Asset::Tqqq,
                    Quote {
                        bid: MoneyMicros::from_usd_cents(10_000),
                        ask: MoneyMicros::from_usd_cents(10_010),
                        observed_at: now,
                    },
                )]),
                daily_turnover: MoneyMicros::ZERO,
                now,
            })
            .unwrap();

        assert_eq!(plan.orders.len(), 1);
        assert_eq!(plan.orders[0].asset, akzio_domain::Asset::Tqqq);
    }

    #[test]
    fn blocked_decision_cannot_be_allocated() {
        let error = V2AllocationRuntime::new(ExecutionPolicy::default())
            .allocate(&AllocationInput {
                decision_context: decision(vec![HardBlocker::Frozen]),
                account: AccountSnapshot {
                    equity: MoneyMicros::from_usd_cents(1_000_000),
                    buying_power: MoneyMicros::from_usd_cents(1_000_000),
                    active: true,
                    trading_blocked: false,
                    positions: BTreeMap::new(),
                },
                quotes: BTreeMap::new(),
                daily_turnover: MoneyMicros::ZERO,
                now: Utc::now(),
            })
            .unwrap_err();

        assert!(matches!(error, AllocationError::DecisionRejected));
    }
}
