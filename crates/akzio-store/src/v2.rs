//! The source-incompatible v2 Store surface.
//!
//! The crate root re-exports this exact surface.

pub use crate::store_v2::{
    AlertSeverity, BackupManifest, ClaimedAttempt, DaemonLease, ExecutionCommit,
    ExecutionCommitResult, LessonUsage, LessonWriteResult, PolicyEvaluationCommit,
    PolicyEvaluationResult, PolicyHead, PolicyShadowPairSnapshot, PolicyTransitionRecord,
    RepriceCommit, RepriceCommitResult, RetryTaskResult, RunExportArtifact, RunExportManifest,
    SessionReservation, SessionSlot, SessionSlotReservation, ShadowPairCompletion,
    ShadowPairWriteResult, StorageInventory, StoreAlert, StoreError, StoreMetrics,
    StoreResult as Result, StoredActiveAttempt, StoredContract, StoredEvent, StoredLesson,
    StoredRun, StoredShadowPair, StoredTask, StoredTaskSnapshot, SucceededAttemptProof,
    TrajectoryEntry, TrajectoryModelMetadata, TrajectoryToolLifecycle, V2Store, WorkflowCommit,
    WorkflowPatchCommit, WorkflowRevision, WorkflowSnapshot,
};
