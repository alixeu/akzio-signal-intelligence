//! The source-incompatible v2 Store surface.
//!
//! The crate root re-exports this exact surface. New runtime code may import
//! either path; the only old implementation is isolated under the legacy
//! module until its owning replacement phase removes it.

pub use crate::store_v2::{
    ClaimedRebuildTask as ClaimedAttempt, DaemonLease, ExecutionCommit, ExecutionCommitResult,
    PolicyEvaluationCommit, PolicyEvaluationResult, PolicyHead, PolicyShadowPairSnapshot,
    PolicyTransitionRecord, RebuildRun as StoredRun, RebuildStore as V2Store,
    RebuildStoreError as StoreError, RebuildStoreResult as Result, RebuildTask as StoredTask,
    RepriceCommit, RepriceCommitResult, RetryTaskResult, SessionReservation, SessionSlot,
    SessionSlotReservation, ShadowPairCompletion, ShadowPairWriteResult, StoredActiveAttempt,
    StoredRebuildEvent as StoredEvent, StoredShadowPair, StoredTaskSnapshot, WorkflowCommit,
    WorkflowPatchCommit, WorkflowRevision, WorkflowSnapshot,
};
