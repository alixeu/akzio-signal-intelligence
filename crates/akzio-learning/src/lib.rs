//! Canonical, Paper-outcome-backed learning authority for Akzio v2.
//!
//! The crate exposes only the typed evaluation runtime and immutable policy history.

mod evaluation;
mod frozen_eval;
mod outcome_schedule;

pub use akzio_domain::PolicySubject;
pub use evaluation::{
    materialize_outcome, CandidatePolicyInput, EvaluationError, EvaluationInput, EvaluationPolicy,
    EvaluationResult, EvaluationRuntime, EvaluationRuntimeResult, GovernedHorizonObservation,
    OutcomeMaterializationInput, ShadowObservation,
};
pub use frozen_eval::{
    evaluate_frozen_evidence, FrozenEvidenceEvalError, FrozenEvidenceMetrics, FrozenEvidenceRecord,
    FrozenEvidenceSet,
};
pub use outcome_schedule::{
    OutcomeScheduleError, OutcomeScheduleInput, OutcomeScheduleOutput, OutcomeScheduleResult,
    OutcomeSchedulingRuntime,
};
