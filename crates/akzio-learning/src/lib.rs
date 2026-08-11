//! Canonical, Paper-outcome-backed learning authority for Akzio v2.
//!
//! The crate exposes only the typed evaluation runtime. Legacy document
//! ledgers and mutable topology selectors have no v2 compatibility surface.

mod outcome_schedule;
mod rebuild;

pub use akzio_domain::PolicySubject;
pub use outcome_schedule::{
    OutcomeScheduleError, OutcomeScheduleInput, OutcomeScheduleOutput, OutcomeScheduleResult,
    OutcomeSchedulingRuntime,
};
pub use rebuild::{
    materialize_outcome, CandidatePolicyInput, EvaluationInput, EvaluationPolicy, EvaluationResult,
    GovernedHorizonObservation, OutcomeMaterializationInput, RebuildEvaluationError,
    RebuildEvaluationResult, RebuildEvaluationRuntime, ShadowObservation,
};
