//! Durable campaign head and session reservations.
//!
//! Campaign writes are fenced by the daemon lease in the same SQLite
//! transaction as the state change.  The learning runtime owns verdict
//! calculation; this module only persists the validated result.

use akzio_domain::{
    Artifact, ArtifactKind, ArtifactLifecycle, CanaryCampaignSpec, CanaryCampaignStatus,
    CanarySessionReservation, CanaryVerdict, ContentHash, PaperApprovalScope, PaperLaunchApproval,
    RunId, RunPurpose, RuntimeManifest,
};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};

use super::{
    assert_daemon_lease, parse_time, run_purpose_from_connection, DaemonLease, SessionReservation,
    SessionSlotReservation, StoreError, StoreResult, V2Store, WorkflowCommit,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryCampaignHead {
    pub spec: CanaryCampaignSpec,
    pub status: CanaryCampaignStatus,
    pub last_verdict: Option<CanaryVerdict>,
    pub revision: u64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredCanarySession {
    pub reservation: CanarySessionReservation,
}
include!("canary_parts/history.rs");
include!("canary_parts/stage.rs");
include!("canary_parts/reservation.rs");
include!("canary_parts/helpers.rs");

#[cfg(test)]
#[path = "canary_parts/tests.rs"]
mod tests;
