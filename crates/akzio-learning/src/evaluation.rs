//! Canonical, outcome-backed learning runtime for Akzio v2.
//!
//! Callers provide governed observations, never precomputed learning metrics.
//! Rust materializes T+1/T+3/T+5 windows and Store-owned run purpose remains
//! the canonicality authority.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;
use thiserror::Error;

use akzio_domain::{
    content_hash_json, AccountSnapshot, Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin,
    ArtifactProvenance, ArtifactRef, Asset, CandidatePolicy, CandidatePolicyState, ContentHash,
    DecisionHorizon, DomainError, Evaluation, EvaluationId, ExecutionPlan, Experience,
    ExperienceId, Forecast, Lesson, LessonId, LessonLifecycle, LessonOrigin, LessonScope,
    MemoryLifecycle, MoneyMicros, OrderReceipt, OrderReceiptState, OrderSide, Outcome,
    OutcomeCostModel, OutcomeExecutionLineage, OutcomeHorizon, OutcomeSchedule, OutcomeWindow,
    PolicyState, PolicySubject, PolicyTransition, PolicyTransitionId, Retrospective,
    RetrospectiveDraft, RetrospectiveStatus, RunPurpose, TargetPortfolio, TaskWritePermit,
    TopologyId, WeightPpm, V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_store::v2::{
    DaemonLease, PolicyEvaluationCommit, PolicyHead, ShadowPairCompletion, ShadowPairWriteResult,
    StoreError, V2Store,
};

const PPM_ONE: u32 = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationPolicy {
    pub minimum_evidence_completeness_ppm: u32,
    pub minimum_risk_recall_ppm: u32,
    pub minimum_fresh_pairs_per_horizon: u64,
}

impl Default for EvaluationPolicy {
    fn default() -> Self {
        Self {
            minimum_evidence_completeness_ppm: 900_000,
            minimum_risk_recall_ppm: 900_000,
            minimum_fresh_pairs_per_horizon: 1,
        }
    }
}

impl EvaluationPolicy {
    /// Degradation must rest on observed evidence, never on absent evidence.
    /// A window whose `risk_recall_ppm` is `None` was never measured, so it
    /// cannot prove degradation; `risk_recall_is_measured` gates promotion
    /// separately so an unmeasured outcome neither promotes nor demotes.
    pub fn outcome_is_degraded(&self, outcome: &Outcome) -> bool {
        outcome.windows.iter().any(|window| {
            window.evidence_completeness_ppm < self.minimum_evidence_completeness_ppm
                || window
                    .risk_recall_ppm
                    .is_some_and(|value| value < self.minimum_risk_recall_ppm)
        })
    }

    /// True only when every window carries a measured risk recall. Forward
    /// policy transitions require this; without it an outcome that silently
    /// skipped risk measurement could buy a promotion.
    pub fn risk_recall_is_measured(&self, outcome: &Outcome) -> bool {
        outcome
            .windows
            .iter()
            .all(|window| window.risk_recall_ppm.is_some())
    }

    fn validate(&self) -> Result<(), EvaluationError> {
        if self.minimum_evidence_completeness_ppm > PPM_ONE
            || self.minimum_risk_recall_ppm > PPM_ONE
            || self.minimum_fresh_pairs_per_horizon == 0
        {
            return Err(EvaluationError::InvalidPolicy);
        }
        Ok(())
    }
}

/// One governed future price surface for a due schedule horizon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GovernedHorizonObservation {
    pub horizon: OutcomeHorizon,
    pub completed_trading_sessions: u8,
    pub observed_trading_day: NaiveDate,
    pub future_prices: BTreeMap<Asset, MoneyMicros>,
    pub expected_evidence_count: u64,
    pub observed_evidence_count: u64,
    pub expected_risk_count: u64,
    pub detected_risk_count: Option<u64>,
}

pub fn horizon_observations(
    bars_by_asset: &BTreeMap<Asset, BTreeMap<NaiveDate, MoneyMicros>>,
    common_dates: &[NaiveDate],
    expected_risk_count: u64,
) -> EvaluationRuntimeResult<Vec<GovernedHorizonObservation>> {
    let completed_sessions = u8::try_from(common_dates.len()).unwrap_or(u8::MAX);
    OutcomeHorizon::ALL
        .into_iter()
        .filter(|horizon| horizon.is_due_after(completed_sessions))
        .map(|horizon| {
            let index = usize::from(horizon.trading_days()) - 1;
            let observed_trading_day = *common_dates
                .get(index)
                .ok_or(EvaluationError::UnalignedBars)?;
            let future_prices =
                Asset::EXECUTABLE
                    .into_iter()
                    .try_fold(BTreeMap::new(), |mut prices, asset| {
                        let price = bars_by_asset
                            .get(&asset)
                            .and_then(|bars| bars.get(&observed_trading_day))
                            .copied()
                            .ok_or(EvaluationError::UnalignedBars)?;
                        prices.insert(asset, price);
                        Ok::<_, EvaluationError>(prices)
                    })?;
            Ok(GovernedHorizonObservation {
                horizon,
                completed_trading_sessions: completed_sessions,
                observed_trading_day,
                future_prices,
                expected_evidence_count: Asset::EXECUTABLE.len() as u64,
                observed_evidence_count: Asset::EXECUTABLE.len() as u64,
                expected_risk_count,
                detected_risk_count: None,
            })
        })
        .collect()
}

/// Raw inputs from which Rust deterministically materializes a sealed Outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutcomeMaterializationInput {
    pub schedule: OutcomeSchedule,
    pub schedule_artifact: ArtifactRef,
    pub target: TargetPortfolio,
    pub forecasts: Vec<Forecast>,
    pub baseline_prices: BTreeMap<Asset, MoneyMicros>,
    pub observations: Vec<GovernedHorizonObservation>,
    pub market_evidence: Vec<ArtifactRef>,
    pub cost_model: OutcomeCostModel,
    pub sealed_at: DateTime<Utc>,
}

/// Derive the realized portfolio target from a validated Paper account and fills.
pub fn realized_execution_target(
    account: &AccountSnapshot,
    execution: &OutcomeExecutionLineage,
    plan: Option<&ExecutionPlan>,
    receipts: &[OrderReceipt],
) -> EvaluationRuntimeResult<TargetPortfolio> {
    account.validate()?;
    let mut values = Asset::EXECUTABLE
        .into_iter()
        .map(|asset| {
            let value = account
                .positions
                .get(&asset)
                .map_or(0_i128, |position| i128::from(position.market_value.0));
            (asset, value)
        })
        .collect::<BTreeMap<_, _>>();

    if matches!(execution, OutcomeExecutionLineage::ReconciledPaper { .. }) {
        let plan = plan.ok_or(EvaluationError::InvalidMaterialization("execution plan"))?;
        plan.validate()?;
        for receipt in receipts {
            if receipt.state != OrderReceiptState::Filled {
                return Err(EvaluationError::InvalidMaterialization(
                    "non-filled broker receipt",
                ));
            }
            let fill_price =
                receipt
                    .average_fill_price
                    .ok_or(EvaluationError::InvalidMaterialization(
                        "filled receipt missing price",
                    ))?;
            let order = plan
                .orders
                .iter()
                .find(|order| order.asset == receipt.asset)
                .ok_or(EvaluationError::InvalidMaterialization(
                    "filled receipt is not in execution plan",
                ))?;
            let fill_value = i128::from(receipt.filled_quantity_micros)
                .saturating_mul(i128::from(fill_price.0))
                .saturating_div(1_000_000);
            let signed = match order.side {
                OrderSide::Buy => fill_value,
                OrderSide::Sell => -fill_value,
            };
            let value = values.get_mut(&receipt.asset).expect("v2 asset is indexed");
            *value = value.saturating_add(signed);
            if *value < 0 {
                return Err(EvaluationError::InvalidMaterialization(
                    "execution fills produce a short realized position",
                ));
            }
        }
    }

    let equity = i128::from(account.equity.0);
    let weights = values
        .into_iter()
        .map(|(asset, value)| {
            let ppm = value
                .saturating_mul(1_000_000)
                .checked_div(equity)
                .and_then(|value| u32::try_from(value).ok())
                .ok_or(EvaluationError::InvalidMaterialization(
                    "realized position weight",
                ))?;
            Ok((asset, WeightPpm(ppm)))
        })
        .collect::<EvaluationRuntimeResult<BTreeMap<_, _>>>()?;
    let target = TargetPortfolio { weights };
    target.validate_universe()?;
    Ok(target)
}

pub struct ShadowObservation {
    pub parent_decision: ArtifactRef,
    pub execution_context: ArtifactRef,
    pub candidate_decision: ArtifactRef,
    pub candidate_contract_hash: ContentHash,
    pub candidate_topology_id: String,
    pub horizon: OutcomeHorizon,
    pub parent_outcome: ArtifactRef,
    pub candidate_outcome: ArtifactRef,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CandidatePolicyInput {
    pub baseline: ArtifactRef,
    pub candidate: ArtifactRef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationInput {
    pub permit: TaskWritePermit,
    pub subject: PolicySubject,
    pub hypothesis_id: String,
    pub materialization: OutcomeMaterializationInput,
    pub contract_hash: ContentHash,
    pub topology_id: TopologyId,
    pub candidate_policy: Option<CandidatePolicyInput>,
    pub token_cost: Option<u64>,
    pub latency_millis: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationResult {
    pub outcome: ArtifactRef,
    pub experience: ArtifactRef,
    pub evaluation: ArtifactRef,
    pub candidate_policy: Option<ArtifactRef>,
    pub policy_head: Option<PolicyHead>,
    pub fresh_pairs_by_horizon: [u64; 3],
}

#[derive(Debug, Error)]
pub enum EvaluationError {
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("canonical learning rejects non-Paper run purpose {0:?}")]
    NonCanonicalPurpose(RunPurpose),
    #[error("evaluation policy has an invalid threshold")]
    InvalidPolicy,
    #[error("policy subject does not match persisted state")]
    SubjectStateMismatch,
    #[error("hypothesis id must be non-empty")]
    EmptyHypothesis,
    #[error("candidate policy input invalid: {0}")]
    InvalidCandidatePolicy(&'static str),
    #[error("outcome materialization is invalid: {0}")]
    InvalidMaterialization(&'static str),
    #[error("outcome materialization arithmetic overflow")]
    ArithmeticOverflow,
    #[error("Paper outcome bars are not aligned")]
    UnalignedBars,
}

pub type EvaluationRuntimeResult<T> = Result<T, EvaluationError>;

#[derive(Debug, Clone)]
pub struct EvaluationRuntime {
    pub(crate) store: V2Store,
    policy: EvaluationPolicy,
}

include!("evaluation_parts/runtime_setup.rs");
include!("evaluation_parts/materialization.rs");
include!("evaluation_parts/outcomes.rs");
include!("evaluation_parts/policy_learning.rs");
include!("evaluation_parts/materialize_outcome.rs");
include!("evaluation_parts/materialize_partial.rs");
#[path = "metrics.rs"]
mod metrics;
use metrics::{
    bounded_ratio_ppm, calibration_quality_ppm, execution_verdict, index_forecasts,
    index_observations, marginal_utility, next_state_with_fresh_pairs, portfolio_return_ppm, price,
    reference, require_canonical_purpose, return_ppm, stable_id, validate_prices,
};
#[cfg(test)]
#[path = "tests.rs"]
mod tests;
