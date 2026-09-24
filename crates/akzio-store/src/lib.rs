//! The sole durable persistence authority for Akzio.
//!
//! The root surface exports only the source-incompatible CAS, SQLite graph,
//! append-only events, permits, leases, slots, policy transitions, and Doctor.
// 文件导读：本 crate 的公开边界只暴露 Store 及其只读投影/提交结果；具体的
// SQLite 表、CAS BLOB、事务、租约和完整性校验都留在 store 模块内部，避免调用方绕过存储约束。
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
