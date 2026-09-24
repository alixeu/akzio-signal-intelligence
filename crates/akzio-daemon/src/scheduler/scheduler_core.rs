// 文件导读：scheduler_core 构造 PaperScheduler、配置 executor/feed/runtime identity，并
// 提供 approval binding、snapshot Need 和 session reservation。它只创建可验证的输入与
// lease-protected reservation，不提前领取研究 task 或触发 broker I/O。
// Rust 机制：builder 方法按值接收/返回 `Self` 以形成不可变配置链；`&self` 借用 Store，
// 迭代器 `map/collect` 生成 Artifact；`Option` 表示尚无 feed/identity/approval，错误通过
// `SchedulerResult` 传递。

use super::*;

impl PaperScheduler {
    pub fn new(store: Store, workflow: WorkflowRuntime, owner_id: String) -> SchedulerResult<Self> {
        // owner_id 是 lease fencing 的持久身份；空 owner 立即拒绝，避免多个 scheduler 共享
        // 一个不可审计的 owner。
        if owner_id.trim().is_empty() {
            return Err(SchedulerError::InvalidOwner);
        }
        Ok(Self {
            store_executor: StoreExecutor::new(store.clone()),
            store,
            workflow,
            owner_id,
            lease_duration: Duration::seconds(30),
            lease: Arc::new(Mutex::new(None)),
            market_data_feed: None,
            runtime_identity_hash: None,
        })
    }

    pub fn with_store_executor(mut self, store_executor: StoreExecutor) -> Self {
        // 只替换串行执行句柄，不复制/迁移 Store；调用方必须保证 executor 对应同一 Store。
        self.store_executor = store_executor;
        self
    }

    pub fn with_market_data_feed(mut self, market_data_feed: Option<AlpacaMarketDataFeed>) -> Self {
        // feed 进入 scheduler identity/approval 比对；None 表示尚未配置，不自动选默认 feed。
        self.market_data_feed = market_data_feed;
        self
    }

    pub fn with_runtime_identity_hash(
        mut self,
        runtime_identity_hash: Option<akzio_domain::ContentHash>,
    ) -> Self {
        // identity hash 只用于 reservation 与 approval binding 的等值检查，不产生 approval。
        self.runtime_identity_hash = runtime_identity_hash;
        self
    }

    pub fn with_lease_duration(mut self, lease_duration: Duration) -> SchedulerResult<Self> {
        // lease duration 必须为正；它影响恢复边界，不改变 task retry budget 或业务状态。
        if lease_duration <= Duration::zero() {
            return Err(SchedulerError::InvalidOwner);
        }
        self.lease_duration = lease_duration;
        Ok(self)
    }

    pub(crate) fn current_approval_binding(&self) -> SchedulerResult<Option<(Artifact, Artifact)>> {
        // 从最新 PaperLaunchApproval 的 RuntimeManifest source_ref 读取绑定；缺 source_ref
        // 是 workflow unavailable，而不是选择另一个旧 approval。
        let Some(approval) = self
            .store
            .latest_artifact_by_kind(ArtifactKind::PaperLaunchApproval)?
        else {
            return Ok(None);
        };
        let manifest_ref = approval
            .source_refs
            .first()
            .filter(|reference| reference.kind == ArtifactKind::RuntimeManifest)
            .ok_or(SchedulerError::WorkflowUnavailable)?;
        let manifest = self.store.artifact(&manifest_ref.artifact_id)?;
        Ok(Some((manifest, approval)))
    }

    pub fn owner_id(&self) -> &str {
        // 只读借用 owner，供 Outcome lease 名称和诊断使用。
        &self.owner_id
    }

    pub fn active_lease(&self, now: DateTime<Utc>) -> SchedulerResult<DaemonLease> {
        // 对外的 active lease 入口统一走 heartbeat/acquire，调用方不能直接伪造 epoch。
        self.acquire_or_renew(now)
    }

    pub fn reserve_canary_session(
        &self,
        reservation: &akzio_domain::CanarySessionReservation,
    ) -> SchedulerResult<StoredCanarySession> {
        // Canary reservation 必须使用 reservation 内冻结的 scheduler_epoch；epoch 变化表示
        // 失去领导权，拒绝写入而不重试旧 reservation。
        let lease = self.acquire_or_renew(reservation.reserved_at)?;
        if lease.epoch != reservation.scheduler_epoch {
            return Err(SchedulerError::NotLeader);
        }
        Ok(self.store.reserve_canary_session(&lease, reservation)?)
    }

    pub(crate) fn paper_snapshot_artifacts(
        &self,
        run_id: &RunId,
        session_key: &str,
        now: DateTime<Utc>,
    ) -> SchedulerResult<Vec<Artifact>> {
        // 每个 session 重新 mint 40 项 Need，并绑定新的 run_id；只提交 setup，不采集 provider。
        paper_session_evidence_needs(session_key)
            .into_iter()
            .map(|need| {
                need.validate()?;
                Ok(Artifact::new(
                    ArtifactKind::EvidenceNeed,
                    // The returned Artifact must be committed by this scheduler's Store.
                    self.store.stage_json(&need)?,
                    "scheduler.paper_snapshot",
                    ArtifactLifecycle::RunScoped,
                    ArtifactProvenance {
                        source_family: "akzio.scheduler".to_owned(),
                        observed_at: None,
                        retrieved_at: now,
                        source_uri: None,
                        confidence_ppm: 1_000_000,
                        producer_contract_hash: None,
                    },
                    Some(ArtifactOrigin {
                        run_id: Some(run_id.clone()),
                        task_id: None,
                        attempt_id: None,
                        contract_hash: None,
                    }),
                    Vec::new(),
                    now,
                )?)
            })
            .collect()
    }

    pub fn reserve_session(
        &self,
        session_key: &str,
        proposal: &WorkflowProposal,
        now: DateTime<Utc>,
    ) -> SchedulerResult<SessionSlotReservation> {
        // 无额外 setup 的简化入口仍先校验日期并取得 lease，再交给 WorkflowRuntime 原子预约。
        self.reserve_session_with_inputs(session_key, proposal, &[], now)
    }

    pub fn reserve_session_with_inputs(
        &self,
        session_key: &str,
        proposal: &WorkflowProposal,
        setup_artifacts: &[Artifact],
        now: DateTime<Utc>,
    ) -> SchedulerResult<SessionSlotReservation> {
        // setup Artifact 与 proposal 一起进入 Store 事务，避免先有 slot 后丢输入。
        NaiveDate::parse_from_str(session_key, "%Y-%m-%d")
            .map_err(|_| SchedulerError::InvalidSessionKey(session_key.to_owned()))?;
        let lease = self.acquire_or_renew(now)?;
        Ok(self.workflow.reserve_paper_session_with_inputs(
            &lease,
            session_key,
            proposal,
            setup_artifacts,
            now,
        )?)
    }

    pub fn reserve_session_with_inputs_for_run(
        &self,
        run_id: RunId,
        session_key: &str,
        proposal: &WorkflowProposal,
        setup_artifacts: &[Artifact],
        now: DateTime<Utc>,
    ) -> SchedulerResult<SessionSlotReservation> {
        // caller 指定 RunId 时保持 lineage；日期/lease 仍由 scheduler 再次验证，不能复用旧 epoch。
        NaiveDate::parse_from_str(session_key, "%Y-%m-%d")
            .map_err(|_| SchedulerError::InvalidSessionKey(session_key.to_owned()))?;
        let lease = self.acquire_or_renew(now)?;
        Ok(self.workflow.reserve_paper_session_with_inputs_for_run(
            &lease,
            run_id,
            session_key,
            proposal,
            setup_artifacts,
            now,
        )?)
    }
}
