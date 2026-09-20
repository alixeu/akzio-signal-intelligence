//! Durable campaign head and session reservations.
//!
//! Campaign writes are fenced by the daemon lease in the same SQLite
//! transaction as the state change.  The learning runtime owns verdict
//! calculation; this module only persists the validated result.

use std::collections::BTreeSet;

use akzio_domain::{
    content_hash_json, AgentContract, Artifact, ArtifactKind, ArtifactLifecycle,
    CanaryCampaignSpec, CanaryCampaignStatus, CanaryCohortEvaluation, CanaryPairedObservation,
    CanarySessionReservation, CanaryVerdict, ContentHash, ExperimentTrial, ExperimentTrialStatus,
    OutcomeHorizon, PaperApprovalScope, PaperLaunchApproval, PolicySubject, RunId, RunPurpose,
    RuntimeManifest, SearchBiasCertificate, WorkflowGraph,
};
use chrono::{DateTime, NaiveDate, Utc};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};

use super::{
    assert_daemon_lease, parse_time, run_purpose_from_connection, DaemonLease, SessionReservation,
    SessionSlotReservation, Store, StoreError, StoreResult, WorkflowCommit,
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

struct CohortSessionColumns {
    cohort_id: String,
    campaign_id: String,
    stage_json: String,
    session_key: String,
    market_day: String,
    regime: String,
    parent_run_id: String,
    contract_shadow_run_id: String,
    topology_shadow_run_id: String,
    bundle_shadow_run_id: String,
    scheduler_epoch: i64,
    reserved_at: String,
}

// 按 SQL 列顺序提取 cohort session 原始字段；类型/业务绑定在后续转换函数中统一校验。
fn cohort_session_columns(row: &rusqlite::Row<'_>) -> rusqlite::Result<CohortSessionColumns> {
    Ok(CohortSessionColumns {
        cohort_id: row.get(0)?,
        campaign_id: row.get(1)?,
        stage_json: row.get(2)?,
        session_key: row.get(3)?,
        market_day: row.get(4)?,
        regime: row.get(5)?,
        parent_run_id: row.get(6)?,
        contract_shadow_run_id: row.get(7)?,
        topology_shadow_run_id: row.get(8)?,
        bundle_shadow_run_id: row.get(9)?,
        scheduler_epoch: row.get(10)?,
        reserved_at: row.get(11)?,
    })
}

// 将 SQL 字段恢复为带 schema、日期、阶段和四条 Run lineage 的 CanarySessionReservation。
// 数值/时间解析失败或领域校验失败都阻断读取，不返回部分可信的 session。
fn stored_cohort_session_from_columns(
    columns: CohortSessionColumns,
) -> StoreResult<StoredCanarySession> {
    let scheduler_epoch = u64::try_from(columns.scheduler_epoch)
        .map_err(|_| StoreError::Integrity("negative canary scheduler epoch".to_owned()))?;
    let reservation = CanarySessionReservation {
        schema_version: akzio_domain::DOMAIN_SCHEMA_VERSION,
        campaign_id: ContentHash::new(columns.campaign_id)?,
        level: serde_json::from_str(&columns.stage_json)?,
        session_key: columns.session_key,
        cohort_id: Some(ContentHash::new(columns.cohort_id)?),
        market_day: Some(
            NaiveDate::parse_from_str(&columns.market_day, "%Y-%m-%d").map_err(|error| {
                StoreError::Integrity(format!(
                    "invalid canary cohort market day {}: {error}",
                    columns.market_day
                ))
            })?,
        ),
        regime: Some(columns.regime),
        parent_run_id: RunId(columns.parent_run_id),
        contract_shadow_run_id: RunId(columns.contract_shadow_run_id),
        topology_shadow_run_id: RunId(columns.topology_shadow_run_id),
        bundle_shadow_run_id: RunId(columns.bundle_shadow_run_id),
        scheduler_epoch,
        reserved_at: parse_time(&columns.reserved_at)?,
    };
    reservation.validate()?;
    Ok(StoredCanarySession { reservation })
}
include!("canary/history.rs");
include!("canary/stage.rs");
include!("canary/reservation.rs");
include!("canary/helpers.rs");
include!("canary/cohort.rs");
