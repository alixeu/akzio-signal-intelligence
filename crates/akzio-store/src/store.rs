//! Store implementation for source-incompatible Akzio authority.
//!
//! `Store` deliberately uses a different database filename and metadata
//! marker from `Store`; callers must choose a new Store Root rather than run a
//! silent in-place migration.

mod attempt;
mod blob;
mod canary;
mod debug;
mod debug_bundle;
mod doctor;
mod execution;
mod experiment;
mod learning;
mod lease;
mod lesson;
mod maintenance;
mod migration;
mod release;
mod schema;
mod trajectory;
mod workflow;

pub use canary::{CanaryCampaignHead, StoredCanarySession};
pub use debug::{DebugArtifactView, DebugAttemptView, DebugNodeView, DebugRunView};
pub use debug_bundle::{DebugBundleIntegrity, DebugBundleManifest, DebugBundleRawAccess};
pub use lesson::{LessonRevalidationScan, LessonUsage, LessonWriteResult, StoredLesson};
pub use maintenance::MaintenanceLeaseDeferral;

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
