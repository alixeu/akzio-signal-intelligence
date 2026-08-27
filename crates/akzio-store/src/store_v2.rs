//! Store implementation for source-incompatible Akzio v2 authority.
//!
//! `V2Store` deliberately uses a different database filename and metadata
//! marker from `V2Store`; callers must choose a new Store Root rather than run a
//! silent in-place migration.

mod blob;
mod canary;
mod doctor;
mod execution;
mod learning;
mod lease;
mod lesson;
mod schema;
mod trajectory;
mod workflow;

pub use canary::{CanaryCampaignHead, StoredCanarySession};
pub use lesson::{LessonUsage, LessonWriteResult, StoredLesson};

include!("store_v2_parts/prelude.rs");
include!("store_v2_parts/public_types.rs");

include!("store_v2_parts/impl_core.rs");
include!("store_v2_parts/impl_workflow.rs");
include!("store_v2_parts/impl_queries.rs");
include!("store_v2_parts/impl_history.rs");
include!("store_v2_parts/impl_learning.rs");

include!("store_v2_parts/free_validation.rs");
include!("store_v2_parts/free_lifecycle.rs");
include!("store_v2_parts/free_trajectory.rs");
include!("store_v2_parts/free_events.rs");
include!("store_v2_parts/free_policy_helpers.rs");
include!("store_v2_parts/impl_attempt.rs");
include!("store_v2_parts/free_reads.rs");
include!("store_v2_parts/free_policy_reads.rs");
include!("store_v2_parts/free_paper_checks.rs");

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

include!("store_v2_parts/fixture.rs");
