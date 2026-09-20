//! The sole durable persistence authority for Akzio.
//!
//! The root surface exports only the source-incompatible CAS, SQLite graph,
//! append-only events, permits, leases, slots, policy transitions, and Doctor.
mod store;
pub use crate::store::{
    DebugArtifactView, DebugAttemptView, DebugBundleIntegrity, DebugBundleManifest,
    DebugBundleRawAccess, DebugNodeView, DebugRunView, ResearchAudit, ResearchAuditRecord,
    RunCheckpoint, RunControlView, RunEventPage, RunEventView, RunInspection,
};

pub use crate::store::{
    AlertSeverity, BackupManifest, CanaryCampaignHead, ClaimedAttempt, DaemonLease,
    DecisionPolicyDescriptor, ExecutionCommit, ExecutionCommitResult, LessonRevalidationScan,
    LessonUsage, LessonWriteResult, MaintenanceLeaseDeferral, PolicyEvaluationCommit,
    PolicyEvaluationResult, PolicyHead, PolicyShadowPairSnapshot, PolicyTransitionRecord,
    ReleaseEvidenceExpectations, RetryTaskResult, RunExportArtifact, RunExportManifest,
    RunLifecycleHealth, RunModelUsage, SessionReservation, SessionSlot, SessionSlotReservation,
    ShadowPairCompletion, ShadowPairWriteResult, StorageInventory, Store, StoreAlert, StoreError,
    StoreMetrics, StoreResult as Result, StoredActiveAttempt, StoredCanarySession, StoredContract,
    StoredDecisionPolicy, StoredEvent, StoredLesson, StoredRun, StoredShadowPair, StoredTask,
    StoredTaskSnapshot, SucceededAttemptProof, TaskWorkload, TrajectoryEntry,
    TrajectoryModelMetadata, TrajectoryToolLifecycle, WorkflowCommit, WorkflowRevision,
    WorkflowSnapshot,
};
