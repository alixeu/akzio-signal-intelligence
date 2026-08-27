//! Typed v2 execution gate derived only from persisted broker artifacts.

use std::collections::BTreeSet;

use akzio_domain::{
    AccountSnapshot, Artifact, ArtifactKind, ArtifactLifecycle, ArtifactRef, CandidatePolicy,
    ContextManifestPayload, DecisionContext, DomainError, ExecutionContext, ExecutionVerdict,
    Experience, FreezeState, HardBlocker, MarketClockSnapshot, MoneyMicros, NoOrder, PolicySubject,
    QuoteSnapshot, RunPurpose, TaskStatus, TaskWritePermit,
};
use akzio_store::v2::{StoreError, V2Store};
use chrono::{DateTime, Duration, Utc};
use serde::de::DeserializeOwned;
use thiserror::Error;

#[cfg(test)]
use akzio_domain::{ArtifactOrigin, ArtifactProvenance};

use crate::{
    AllocationError, AllocationInput, ExecutionError, ExecutionGatePolicy, ExecutionPolicy,
    V2AllocationRuntime,
};

#[derive(Debug, Error)]
pub enum ExecutionGateError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("expected {expected:?} artifact, found {actual:?}")]
    WrongArtifactKind {
        expected: ArtifactKind,
        actual: ArtifactKind,
    },
    #[error("decision context does not belong to the execution task run")]
    DecisionRunMismatch,
    #[error("execution gate integrity failure: {0}")]
    Integrity(&'static str),
}

pub type ExecutionGateResult<T> = std::result::Result<T, ExecutionGateError>;

#[derive(Debug, Clone)]
pub struct ExecutionGateInput {
    pub permit: TaskWritePermit,
    pub decision_context: ArtifactRef,
    pub account_snapshot: Option<ArtifactRef>,
    pub quote_snapshot: Option<ArtifactRef>,
    pub market_clock_snapshot: Option<ArtifactRef>,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ExecutionGateOutput {
    pub execution_plan: Option<Artifact>,
    pub execution_context: Artifact,
    pub verdict: Artifact,
}

#[derive(Debug, Clone)]
pub struct V2ExecutionRuntime {
    store: V2Store,
    allocation: V2AllocationRuntime,
    gate_policy: ExecutionGatePolicy,
}

include!("execution_gate_parts/core.rs");
include!("execution_gate_parts/snapshots.rs");
include!("execution_gate_parts/validation.rs");
include!("execution_gate_parts/helpers.rs");
#[cfg(test)]
#[path = "execution_gate/tests.rs"]
mod tests;
