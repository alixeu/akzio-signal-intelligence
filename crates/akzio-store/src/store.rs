//! Store implementation for source-incompatible Akzio authority.
//!
//! `Store` deliberately uses a different database filename and metadata
//! marker from `Store`; callers must choose a new Store Root rather than run a
//! silent in-place migration.

// Store 是 SQLite/CAS、workflow、policy、lease、debug 和 learning 的唯一持久化权威；各 include 共享同一事务边界。
mod attempt;
mod blob;
mod canary;
mod debug;
mod debug_bundle;
mod decision_policy;
mod doctor;
mod execution;
mod experiment;
mod learning;
mod lease;
mod lesson;
mod maintenance;
mod migration;
mod release;
mod research_quality;
mod research_review;
mod run_control;
mod schema;
pub use run_control::{RunCheckpoint, RunControlView, RunEventPage, RunEventView, RunInspection};
mod trajectory;
mod workflow;

pub use canary::{CanaryCampaignHead, StoredCanarySession};
pub use debug::{DebugArtifactView, DebugAttemptView, DebugNodeView, DebugRunView};
pub use debug_bundle::{DebugBundleIntegrity, DebugBundleManifest, DebugBundleRawAccess};
pub use decision_policy::{DecisionPolicyDescriptor, StoredDecisionPolicy};
pub use lesson::{LessonRevalidationScan, LessonUsage, LessonWriteResult, StoredLesson};
pub use maintenance::MaintenanceLeaseDeferral;
pub use research_review::{ResearchAudit, ResearchAuditRecord};

// prelude/public_types 定义共享类型；impl/free_* 按职责拆分方法，但不创建并行状态存储或绕过 Store。
include!("store/prelude.rs");
include!("store/public_types.rs");

include!("store/impl_core.rs");
include!("store/impl_workflow.rs");
include!("store/impl_queries.rs");
include!("store/impl_history.rs");
include!("store/impl_learning.rs");

include!("store/free_validation.rs");
include!("store/free_lifecycle.rs");
include!("store/free_trajectory.rs");
include!("store/free_events.rs");
include!("store/free_policy_helpers.rs");
include!("store/impl_attempt.rs");
include!("store/free_reads.rs");
include!("store/free_policy_reads.rs");
include!("store/free_paper_checks.rs");
