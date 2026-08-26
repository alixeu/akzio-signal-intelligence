//! The sole durable persistence authority for Akzio v2.
//!
//! The root surface exports only the source-incompatible v2 CAS, SQLite graph,
//! append-only events, permits, leases, slots, policy transitions, and Doctor.
mod store_v2;

pub mod v2 {
    pub use crate::store_v2::{
        AlertSeverity, BackupManifest, CanaryCampaignHead, ClaimedAttempt, DaemonLease,
        ExecutionCommit, ExecutionCommitResult, LessonUsage, LessonWriteResult,
        PolicyEvaluationCommit, PolicyEvaluationResult, PolicyHead, PolicyShadowPairSnapshot,
        PolicyTransitionRecord, RepriceCommit, RepriceCommitResult, RetryTaskResult,
        RunExportArtifact, RunExportManifest, SessionReservation, SessionSlot,
        SessionSlotReservation, ShadowPairCompletion, ShadowPairWriteResult, StorageInventory,
        StoreAlert, StoreError, StoreMetrics, StoreResult as Result, StoredActiveAttempt,
        StoredCanarySession, StoredContract, StoredEvent, StoredLesson, StoredRun,
        StoredShadowPair, StoredTask, StoredTaskSnapshot, SucceededAttemptProof, TrajectoryEntry,
        TrajectoryModelMetadata, TrajectoryToolLifecycle, V2Store, WorkflowCommit,
        WorkflowPatchCommit, WorkflowRevision, WorkflowSnapshot,
    };
}

pub use v2::*;
