//! Canonical, Paper-outcome-backed learning authority for Akzio.
//!
//! The crate exposes only the typed evaluation runtime and immutable policy history.

mod campaign;
mod evaluation;
mod lesson_evidence;
mod outcome_schedule;
mod qualification;

use akzio_domain::{ArtifactProvenance, TaskWritePermit};
use chrono::{DateTime, Utc};

pub(crate) fn trusted_learning_provenance(
    permit: &TaskWritePermit,
    now: DateTime<Utc>,
) -> ArtifactProvenance {
    ArtifactProvenance {
        source_family: "akzio-learning".to_owned(),
        observed_at: Some(now),
        retrieved_at: now,
        source_uri: None,
        confidence_ppm: 1_000_000,
        producer_contract_hash: permit.contract_hash.clone(),
    }
}

pub use campaign::{evaluate_canary_cohort, CanaryCampaignRuntime, CanaryError};
pub use evaluation::{
    apply_risk_ground_truth_assessments, daily_observations, horizon_observations,
    materialize_outcome, materialize_partial_outcome, realized_execution,
    realized_execution_at_prices, realized_execution_target, CandidatePolicyInput, EvaluationError,
    EvaluationInput, EvaluationPolicy, EvaluationResult, EvaluationRuntime,
    EvaluationRuntimeResult, GovernedDailyObservation, GovernedHorizonObservation,
    GovernedRiskRecall, ObservedExecutionMetrics, OutcomeMaterializationInput, RealizedExecution,
    SealedEvaluationInput, ShadowObservation, AKZIO_MIN_CALIBRATION_SAMPLES,
    AKZIO_MIN_FRESH_PAIRS_PER_HORIZON,
};
pub use outcome_schedule::{
    OutcomeScheduleError, OutcomeScheduleInput, OutcomeScheduleOutput, OutcomeScheduleResult,
    OutcomeSchedulingRuntime,
};
pub use qualification::{
    run_offline_model_qualification, OfflineModelQualificationInput,
    OfflineModelQualificationResult, QualificationRunError, QualificationScenarioObservation,
    QualificationStage,
};
