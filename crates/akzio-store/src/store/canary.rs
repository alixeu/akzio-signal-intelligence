//! Durable campaign head and session reservations.
//!
//! Campaign writes are fenced by the daemon lease in the same SQLite
//! transaction as the state change.  The learning runtime owns verdict
//! calculation; this module only persists the validated result.
// 文件导读：Canary 同时保存 campaign head、legacy/paired session 和四条 Run lineage；
// 本文件只负责把 SQL 列转换为领域 reservation，是否晋级由 learning 侧计算并提交。
// 先读本文件的 SQL 行解码，再沿 include! 阅读 history/stage/reservation/cohort；这些物理文件
// 在编译时并入本模块，读写共享 `Connection`/`Transaction` 类型，但 promotion/verdict 仍不在 Store 计算。
// `Row<'_>` 的生命周期只覆盖 SQLite 回调；转换成拥有型列后才能在回调外继续解析。

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

// rusqlite 的 Row 仅在回调期间有效；按 SELECT 列顺序拷贝为拥有 String/i64 的中间值，
// 让后续领域解析不再借用 SQLite 行。SQL 类型转换错误原样交给 query_row/query_map。
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

// 消费已拥有的列结构并恢复领域 reservation：负 epoch、坏 hash/JSON/日期/时间或 validate 失败
// 都返回 StoreError；成功结果绑定 campaign、stage、session、market day/regime 与四个 Run ID。
// struct 字段从 columns 移动到 reservation，转换完成后不再保留第二份原始字符串副本。
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
