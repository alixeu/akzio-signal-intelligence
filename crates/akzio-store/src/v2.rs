//! The source-incompatible v2 Store surface.
//!
//! The crate root re-exports this exact surface.

pub use crate::store_v2::{
    AlertSeverity, BackupManifest, ClaimedAttempt, DaemonLease, ExecutionCommit,
    ExecutionCommitResult, PolicyEvaluationCommit, PolicyEvaluationResult, PolicyHead,
    PolicyShadowPairSnapshot, PolicyTransitionRecord, RepriceCommit, RepriceCommitResult,
    RetryTaskResult, SessionReservation, SessionSlot, SessionSlotReservation, ShadowPairCompletion,
    ShadowPairWriteResult, StoreAlert, StoreError, StoreMetrics, StoreResult as Result,
    StoredActiveAttempt, StoredContract, StoredEvent, StoredRun, StoredShadowPair, StoredTask,
    StoredTaskSnapshot, SucceededAttemptProof, V2Store, WorkflowCommit, WorkflowPatchCommit,
    WorkflowRevision, WorkflowSnapshot,
};
