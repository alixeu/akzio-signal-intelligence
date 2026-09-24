//! Fenced, scheduler-owned Paper session reservation.
//!
//! This module owns leadership and session timing only. Workflow compilation
//! remains in `akzio-runtime`; Paper commitment and broker I/O remain in
//! `akzio-execution`.

// 文件导读：scheduler 只负责 broker-authoritative session、lease fencing、proposal 选择和
// 每日 Paper slot reservation。它不执行研究模型、不计算 Decision、不提交订单；后续 worker
// 才按 graph 推进 Evidence → research → Decision → Execution → Paper/Outcome。lease/slot
// 成功是调度事实，不能越级说明 Run、submission、fill 或 Outcome 完成。
// Rust 机制：`BrokerSessionClock`/`PaperWorkflowSource` 是 Send + Sync trait，方法返回带
// 生命周期的 boxed Future；`Arc<Mutex<Option<DaemonLease>>>` 缓存租约且处理 poisoning；
// `StoreExecutor` 串行化持久化，`?Sized` 让引用型 trait object 可注入。

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration as StdDuration,
};

use akzio_domain::{
    paper_session_evidence_needs, AgentContract, Artifact, ArtifactKind, ArtifactLifecycle,
    ArtifactOrigin, ArtifactProvenance, ArtifactRef, CanaryCampaignStatus,
    CanarySessionReservation, DomainError, PolicySubject, RunId, RunPurpose, RuntimeManifest,
    TopologyId, WorkflowProposal, DOMAIN_SCHEMA_VERSION, STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID,
};
use akzio_execution::paper::AlpacaPaper;
use akzio_ingest::AlpacaMarketDataFeed;
use akzio_runtime::{RuntimeError, StoreExecutor, WorkflowRuntime};
use akzio_store::{DaemonLease, SessionSlotReservation, Store, StoreError, StoredCanarySession};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use thiserror::Error;
use tokio::sync::watch;

pub const SCHEDULER_LEASE_NAME: &str = "akzio.local.scheduler";

#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("invalid scheduler owner")]
    InvalidOwner,
    #[error("scheduler is not the active leader")]
    NotLeader,
    #[error("scheduler lease state poisoned")]
    LeasePoisoned,
    #[error("broker session key must be an ISO-8601 trading date: {0}")]
    InvalidSessionKey(String),
    #[error("broker session clock failed: {0}")]
    Clock(String),
    #[error("no durable Paper workflow proposal is available")]
    WorkflowUnavailable,
    #[error("workflow proposal is not owned by a Paper run")]
    WorkflowNotPaper,
}

pub type SchedulerResult<T> = std::result::Result<T, SchedulerError>;

/// Producer of the scheduler-minted, session-specific snapshot needs. Only
/// these may be carried in a stored proposal and re-minted per session.
const PAPER_SNAPSHOT_PRODUCER: &str = "scheduler.paper_snapshot";

/// A broker-authoritative clock returns an open session date; local wall-clock
/// dates and hand-maintained market calendars never create Paper slots.
pub trait BrokerSessionClock: Send + Sync {
    fn open_session_key<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = SchedulerResult<Option<String>>> + Send + 'a>>;

    fn paper_account_id<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = SchedulerResult<String>> + Send + 'a>>;
}

/// Production adapter over the already fail-closed Alpaca Paper client. Its
/// constructor is deliberately injected, so daemon construction does not read
/// credentials or perform network I/O.
pub struct AlpacaPaperSessionClock {
    paper: AlpacaPaper,
}

impl AlpacaPaperSessionClock {
    pub fn new(paper: AlpacaPaper) -> Self {
        // 只保存已经构造且已通过 Paper endpoint 检查的 client；构造 clock 本身不发请求。
        Self { paper }
    }
}

impl BrokerSessionClock for AlpacaPaperSessionClock {
    fn open_session_key<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = SchedulerResult<Option<String>>> + Send + 'a>> {
        // Future 在 await 时读取 broker clock；Closed 返回 None，绝不使用本机日期创建 slot。
        Box::pin(async move {
            let clock = self
                .paper
                .market_clock()
                .await
                .map_err(|error| SchedulerError::Clock(error.to_string()))?;
            Ok((clock.session.kind != akzio_domain::TradingSession::Closed)
                .then(|| clock.session_date.to_string()))
        })
    }

    fn paper_account_id<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = SchedulerResult<String>> + Send + 'a>> {
        // 账户 ID 只用于 approval/runtime binding；缺 id 是 Clock 错误，不回退到配置字符串。
        Box::pin(async move {
            self.paper
                .account()
                .await
                .map_err(|error| SchedulerError::Clock(error.to_string()))?
                .get("id")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned)
                .ok_or_else(|| SchedulerError::Clock("broker account id missing".to_owned()))
        })
    }
}

/// Provides only a Rust-validated `WorkflowProposal`; it cannot create a Run
/// or bypass the scheduler lease.
pub trait PaperWorkflowSource: Send + Sync {
    fn proposal<'a>(
        &'a self,
        session_key: &'a str,
    ) -> Pin<Box<dyn Future<Output = SchedulerResult<WorkflowProposal>> + Send + 'a>>;
}

/// Prefers a durable Paper `WorkflowProposal` from `Store`. A bounded,
/// Rust-compiled bootstrap may be supplied for the first scheduler session;
/// reservation persists that proposal atomically with the Paper run.
#[derive(Clone)]
pub struct StorePaperWorkflowSource {
    store: Store,
    store_executor: StoreExecutor,
    bootstrap: Option<(WorkflowRuntime, String)>,
}

impl StorePaperWorkflowSource {
    pub fn new(store: Store) -> Self {
        // source 与 StoreExecutor 指向同一 Store；bootstrap 默认为 None，避免凭空生成 proposal。
        Self {
            store_executor: StoreExecutor::new(store.clone()),
            store,
            bootstrap: None,
        }
    }

    pub fn with_store_executor(mut self, store_executor: StoreExecutor) -> Self {
        // builder 只替换 executor 句柄，保留 store/proposal 选择语义。
        self.store_executor = store_executor;
        self
    }

    pub fn with_bootstrap(
        mut self,
        workflow: WorkflowRuntime,
        topology_id: impl Into<String>,
    ) -> Self {
        // bootstrap 仅供首次 scheduler session 使用，实际 reservation 仍由 WorkflowRuntime
        // 和 lease 事务持久化，不在这里创建 Run。
        self.bootstrap = Some((workflow, topology_id.into()));
        self
    }

    /// A stored proposal that still references another Run's compiled
    /// `EvidenceNeed` can never be reserved, because reservation rejects
    /// cross-run RunScoped evidence fail-closed. Such a proposal must be
    /// skipped during selection; otherwise it stays the newest durable Paper
    /// proposal and stalls every future session.
    ///
    /// Scheduler snapshots are excluded: reservation drops and re-mints those
    /// for the new session, so carrying them is harmless.
    pub(crate) fn references_foreign_run_scoped_need(
        &self,
        proposal: &WorkflowProposal,
    ) -> SchedulerResult<bool> {
        // 扫描 proposal 的 RunScoped EvidenceNeed；跨 Run 输入会让后续 reservation fail closed，
        // 因此在选择阶段跳过，而不是改写历史 Artifact。
        for task in proposal.tasks.values() {
            for reference in &task.evidence_needs {
                let artifact = self.store.artifact(&reference.artifact_id)?;
                if artifact.kind != ArtifactKind::EvidenceNeed
                    || artifact.lifecycle != ArtifactLifecycle::RunScoped
                    || artifact.producer == PAPER_SNAPSHOT_PRODUCER
                {
                    continue;
                }
                if artifact
                    .origin
                    .as_ref()
                    .and_then(|origin| origin.run_id.as_ref())
                    .is_some()
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn proposal_sync(&self) -> SchedulerResult<WorkflowProposal> {
        // 同步读取 active/candidate topology head 和最近 Paper proposal；优先 canonical durable
        // proposal，找不到才使用受控 bootstrap，避免每 tick 重新编译或混入旧 Run 输入。
        let candidate_subject = PolicySubject::Topology(TopologyId(
            STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID.to_owned(),
        ));
        let preferred_topology = self
            .store
            .policy_head(&candidate_subject)?
            .filter(|head| {
                head.state
                    == akzio_domain::PolicyState::Topology(
                        akzio_domain::CandidatePolicyState::Active,
                    )
            })
            .map(|_| STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID)
            .unwrap_or("active");
        let mut latest_paper = None;
        for artifact in self
            .store
            .recent_artifacts_by_kind(ArtifactKind::WorkflowProposal, 500)?
        {
            let Some(run_id) = artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
            else {
                continue;
            };
            if self.store.run_purpose(run_id)? == RunPurpose::Paper {
                let proposal: WorkflowProposal =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                if proposal.topology_id == preferred_topology {
                    if self.references_foreign_run_scoped_need(&proposal)? {
                        continue;
                    }
                    latest_paper = Some(proposal);
                    break;
                }
            }
        }

        let Some(proposal) = latest_paper else {
            return self
                .bootstrap
                .as_ref()
                .map(|(workflow, topology_id)| {
                    workflow.approved_paper_proposal(topology_id.clone())
                })
                .transpose()?
                .ok_or(SchedulerError::WorkflowUnavailable);
        };
        Ok(proposal)
    }
}

impl PaperWorkflowSource for StorePaperWorkflowSource {
    fn proposal<'a>(
        &'a self,
        _session_key: &'a str,
    ) -> Pin<Box<dyn Future<Output = SchedulerResult<WorkflowProposal>> + Send + 'a>> {
        // clone source 后把只读选择闭包放入 StoreExecutor；返回的 proposal 仍需 tick 再绑定
        // 当前 session 的 snapshots/lease。
        Box::pin(async move {
            let source = self.clone();
            self.store_executor
                .execute(move |_| source.proposal_sync())
                .await?
        })
    }
}

#[derive(Clone)]
pub struct PaperScheduler {
    store: Store,
    store_executor: StoreExecutor,
    workflow: WorkflowRuntime,
    owner_id: String,
    lease_duration: Duration,
    lease: Arc<Mutex<Option<DaemonLease>>>,
    market_data_feed: Option<AlpacaMarketDataFeed>,
    runtime_identity_hash: Option<akzio_domain::ContentHash>,
}

#[path = "scheduler/canary.rs"]
mod canary;
#[path = "scheduler/lease.rs"]
mod lease;
#[path = "scheduler/scheduler_core.rs"]
mod scheduler_core;
#[path = "scheduler/scheduler_tick.rs"]
mod scheduler_tick;
#[path = "scheduler/serve.rs"]
mod serve;
