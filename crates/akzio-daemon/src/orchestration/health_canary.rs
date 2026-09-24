// 文件导读：本文件管理 worker supervision、health/ready 投影以及 Canary stage/resume。
// health 读取 Store lease/metrics/Policy 状态，ready 只检查进程配置和 broker 注入；Canary
// campaign 的 staged/advance 仍不是 active Contract/Topology、Paper fill 或学习晋升证明。
// Rust 机制：`Arc`/闭包把 daemon 克隆进 TaskHandler；`watch` 只读关闭状态；Store 错误经
// `Result` 传播，health 用 `Option` 表示没有 scheduler lease/approval，而不是默认健康。

use super::*;

impl Daemon {
    /// Worker supervision contains no research, execution, or learning policy.
    pub async fn serve_workers(&self, shutdown: watch::Receiver<bool>) -> Result<()> {
        // auto_paper 时拒绝缺少 clock/source 的普通 worker 入口，避免 T0 scheduler 漏启；
        // 非 Paper Core 则只监督现有 durable tasks。
        if self.paper.auto_paper {
            return Err(DaemonError::InvalidInput(
                "auto_paper requires a broker session clock and Paper workflow source".to_owned(),
            ));
        }
        self.serve_worker_pool(shutdown).await
    }

    pub(super) async fn serve_worker_pool(&self, shutdown: watch::Receiver<bool>) -> Result<()> {
        // handler 闭包捕获 Daemon 的 Arc-backed clone；每个 claim 得到独立 Future，WorkerPool
        // await 它们并负责异常回收，当前函数没有额外业务状态机。
        let daemon = self.clone();
        let handler: TaskHandler = Arc::new(move |task| {
            let daemon = daemon.clone();
            Box::pin(async move { daemon.execute_task(task).await })
        });
        let pool = WorkerPool::new(
            self.task_runtime.clone(),
            self.transport.worker_pool.clone(),
        );
        let pool = if self.debug_enabled() {
            pool
        } else {
            pool.with_lesson_revalidation(self.store.clone())
        };
        pool.serve(handler, shutdown).await?;
        Ok(())
    }

    pub fn health(&self) -> Result<DaemonHealth> {
        // 这是从持久化 lease/freeze/metrics 和内存 Policy 派生的一次性快照；过期 lease
        // 被过滤，health 查询本身不续约也不运行 scheduler。
        let lease = self
            .store
            .daemon_lease(SCHEDULER_LEASE_NAME)?
            .filter(|lease| lease.expires_at > Utc::now());
        let frozen = self
            .store
            .latest_artifact_by_kind(ArtifactKind::FreezeState)?
            .map(|artifact| {
                let state: FreezeState =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                state
                    .validate()
                    .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
                Ok::<_, DaemonError>(state.frozen)
            })
            .transpose()?
            .unwrap_or(false);
        let metrics = self.store.metrics(Utc::now())?;
        let policy = self.decision_runtime.policy();
        let decision_policy_status = self
            .debug_control
            .as_ref()
            .map(|config| config.decision_policy_status.clone())
            .filter(|status| !status.is_empty())
            .unwrap_or_else(|| {
                if policy.decision_capable() {
                    "ready_for_current_decision".to_owned()
                } else if policy.asset_calibrations.is_empty() {
                    "unconfigured_fail_closed".to_owned()
                } else {
                    "validated_but_insufficient_samples".to_owned()
                }
            });
        let store_scope = if self.store.debug_environment()?.is_some() {
            "isolated_debug"
        } else {
            "canonical"
        };
        Ok(DaemonHealth {
            status: if self.paper.auto_paper && lease.is_none() {
                "paper_scheduler_fail_closed".to_owned()
            } else {
                "ok".to_owned()
            },
            store_scope: store_scope.to_owned(),
            formal_run_bundle_supported: store_scope == "canonical" && !self.debug_enabled(),
            frozen,
            decision_policy_status,
            decision_policy_hash: policy.policy_hash()?,
            decision_policy_input_hash: self
                .debug_control
                .as_ref()
                .and_then(|config| config.decision_policy_input_hash.clone()),
            decision_capable: policy.decision_capable(),
            news_web_status: self.news_web_status.clone(),
            news_web_route: self.news_web_route.clone(),
            scheduler_owner: lease.as_ref().map(|lease| lease.owner_id.clone()),
            scheduler_epoch: lease.map(|lease| lease.epoch),
            alerts: metrics.alerts(),
            metrics,
        })
    }

    /// Readiness covers process configuration. Durable run/task failures stay
    /// visible in health and Observer data, but must not prevent inspection or
    /// recovery after a restart.
    pub fn ready(&self) -> Result<DaemonHealth> {
        // 只在 health 基础上检查当前配置中 Paper broker 是否已注入；不扫描 Run 失败，
        // 以便故障后的 inspect/recovery 仍可访问。
        let health = self.health()?;
        if self.paper.auto_paper && self.paper.paper_broker.is_none() {
            return Err(DaemonError::Unavailable(
                "Paper broker is not injected".to_owned(),
            ));
        }
        Ok(health)
    }

    pub fn canary_status(&self) -> Result<Option<akzio_store::CanaryCampaignHead>> {
        // 只读返回 active campaign head；None 表示 Store 尚无 head。
        Ok(self.store.active_canary_campaign()?)
    }

    pub fn stage_canary_campaign(
        &self,
        spec: akzio_domain::CanaryCampaignSpec,
    ) -> Result<akzio_store::CanaryCampaignHead> {
        // 获取/续租 scheduler leader lease 后登记 staged campaign；stage 不会激活候选策略。
        let now = Utc::now();
        let lease = self.paper.scheduler.active_lease(now)?;
        Ok(self.store.stage_canary_campaign(&lease, &spec, now)?)
    }

    pub fn resume_canary_campaign(
        &self,
        campaign_id: &ContentHash,
    ) -> Result<akzio_store::CanaryCampaignHead> {
        // 仅 staged campaign 可提交 Advance 转换；其他状态原样返回，持久化和期望状态检查
        // 由 Store 完成。
        let now = Utc::now();
        let lease = self.paper.scheduler.active_lease(now)?;
        let current = self
            .store
            .canary_campaign(campaign_id)?
            .ok_or_else(|| DaemonError::InvalidInput("canary campaign not found".to_owned()))?;
        if current.status == akzio_domain::CanaryCampaignStatus::Staged {
            return Ok(self.store.transition_canary_campaign(
                &lease,
                campaign_id,
                akzio_domain::CanaryCampaignStatus::Staged,
                akzio_domain::CanaryVerdict::Advance,
                now,
            )?);
        }
        Ok(current)
    }
}
