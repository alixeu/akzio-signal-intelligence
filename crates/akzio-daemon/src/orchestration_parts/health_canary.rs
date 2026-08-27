impl Daemon {
    /// Worker supervision contains no research, execution, or learning policy.
    pub async fn serve_workers(&self, shutdown: watch::Receiver<bool>) -> Result<()> {
        if self.paper.auto_paper {
            return Err(DaemonError::InvalidInput(
                "auto_paper requires a broker session clock and Paper workflow source".to_owned(),
            ));
        }
        self.serve_worker_pool(shutdown).await
    }

    async fn serve_worker_pool(&self, shutdown: watch::Receiver<bool>) -> Result<()> {
        let daemon = self.clone();
        let handler: TaskHandler = Arc::new(move |task| {
            let daemon = daemon.clone();
            Box::pin(async move { daemon.execute_task(task).await })
        });
        WorkerPool::new(
            self.task_runtime.clone(),
            self.transport.worker_pool.clone(),
        )
        .serve(handler, shutdown)
        .await?;
        Ok(())
    }

    pub fn health(&self) -> Result<DaemonHealth> {
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
        Ok(DaemonHealth {
        status: if self.paper.auto_paper && lease.is_none() {
                "paper_scheduler_fail_closed".to_owned()
            } else {
                "ok".to_owned()
            },
            frozen,
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
        let health = self.health()?;
        if self.paper.auto_paper && self.paper.paper_broker.is_none() {
            return Err(DaemonError::Unavailable(
                "Paper broker is not injected".to_owned(),
            ));
        }
        Ok(health)
    }

    pub fn canary_status(&self) -> Result<Option<akzio_store::v2::CanaryCampaignHead>> {
        Ok(self.store.active_canary_campaign()?)
    }

    pub fn stage_canary_campaign(
        &self,
        spec: akzio_domain::CanaryCampaignSpec,
    ) -> Result<akzio_store::v2::CanaryCampaignHead> {
        let now = Utc::now();
        let lease = self.paper.scheduler.active_lease(now)?;
        Ok(self.store.stage_canary_campaign(&lease, &spec, now)?)
    }

    pub fn resume_canary_campaign(
        &self,
        campaign_id: &ContentHash,
    ) -> Result<akzio_store::v2::CanaryCampaignHead> {
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
