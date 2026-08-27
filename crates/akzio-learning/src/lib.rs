//! Canonical, Paper-outcome-backed learning authority for Akzio v2.
//!
//! The crate exposes only the typed evaluation runtime and immutable policy history.

mod campaign;
mod evaluation;
mod frozen_eval;
mod outcome_schedule;

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

pub use campaign::{
    CanaryBundleComparison, CanaryCampaignRuntime, CanaryError, CanaryHorizonMetrics,
    CanarySubjectComparison,
};
pub use evaluation::{
    horizon_observations, materialize_outcome, materialize_partial_outcome,
    realized_execution_target, CandidatePolicyInput, EvaluationError, EvaluationInput,
    EvaluationPolicy, EvaluationResult, EvaluationRuntime, EvaluationRuntimeResult,
    GovernedHorizonObservation, OutcomeMaterializationInput, ShadowObservation,
};
pub use frozen_eval::{
    evaluate_frozen_evidence, FrozenEvidenceEvalError, FrozenEvidenceMetrics, FrozenEvidenceRecord,
    FrozenEvidenceSet,
};
pub use outcome_schedule::{
    OutcomeScheduleError, OutcomeScheduleInput, OutcomeScheduleOutput, OutcomeScheduleResult,
    OutcomeSchedulingRuntime,
};
