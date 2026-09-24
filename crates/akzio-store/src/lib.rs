//! The sole durable persistence authority for Akzio.
//!
//! The root surface exports only the source-incompatible CAS, SQLite graph,
//! append-only events, permits, leases, slots, policy transitions, and Doctor.
// 文件导读：本 crate 的公开边界只暴露 Store 及其只读投影/提交结果；具体的
// SQLite 表、CAS BLOB、事务、租约和完整性校验都留在 store 模块内部，避免调用方绕过存储约束。
// `mod store` 保持实现私有；下面的 `pub use` 把选定类型重导出到 crate 根，
// 不会因此暴露 `rusqlite::Connection` 或把私有子模块整体变成公开 API。
mod store;
// 只读诊断投影单独导出，便于调用方检查 Run/Bundle，但读取结果不是新的持久化状态。
pub use crate::store::{
    DebugArtifactView, DebugAttemptView, DebugBundleIntegrity, DebugBundleManifest,
    DebugBundleRawAccess, DebugNodeView, DebugRunView, ResearchAudit, ResearchAuditRecord,
    RunCheckpoint, RunControlView, RunEventPage, RunEventView, RunInspection,
};

// 其余导出包含显式提交输入、提交结果及重建后的持久化投影；具体授权仍由 Store 方法核验。
// `StoreResult as Result` 只重命名公开路径，不改写 `Result<T, StoreError>` 的错误传播语义。
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
