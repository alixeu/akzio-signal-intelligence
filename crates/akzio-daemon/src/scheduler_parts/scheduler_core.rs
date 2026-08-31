impl PaperScheduler {
    pub fn new(
        store: V2Store,
        workflow: WorkflowRuntime,
        owner_id: String,
    ) -> SchedulerResult<Self> {
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
        self.store_executor = store_executor;
        self
    }

    pub fn with_market_data_feed(mut self, market_data_feed: Option<AlpacaMarketDataFeed>) -> Self {
        self.market_data_feed = market_data_feed;
        self
    }

    pub fn with_runtime_identity_hash(
        mut self,
        runtime_identity_hash: Option<akzio_domain::ContentHash>,
    ) -> Self {
        self.runtime_identity_hash = runtime_identity_hash;
        self
    }

    pub fn with_lease_duration(mut self, lease_duration: Duration) -> SchedulerResult<Self> {
        if lease_duration <= Duration::zero() {
            return Err(SchedulerError::InvalidOwner);
        }
        self.lease_duration = lease_duration;
        Ok(self)
    }

    fn current_approval_binding(&self) -> SchedulerResult<Option<(Artifact, Artifact)>> {
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
        &self.owner_id
    }

    pub fn active_lease(&self, now: DateTime<Utc>) -> SchedulerResult<DaemonLease> {
        self.acquire_or_renew(now)
    }

    pub fn reserve_canary_session(
        &self,
        reservation: &akzio_domain::CanarySessionReservation,
    ) -> SchedulerResult<StoredCanarySession> {
        let lease = self.acquire_or_renew(reservation.reserved_at)?;
        if lease.epoch != reservation.scheduler_epoch {
            return Err(SchedulerError::NotLeader);
        }
        Ok(self.store.reserve_canary_session(&lease, reservation)?)
    }

    fn paper_snapshot_artifacts(
        &self,
        run_id: &RunId,
        session_key: &str,
        now: DateTime<Utc>,
    ) -> SchedulerResult<Vec<Artifact>> {
        paper_session_evidence_needs(session_key)
            .into_iter()
            .map(|need| {
                need.validate()?;
                Ok(Artifact::new(
                    ArtifactKind::EvidenceNeed,
                    self.store.put_json(&need)?,
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
        self.reserve_session_with_inputs(session_key, proposal, &[], now)
    }

    pub fn reserve_session_with_inputs(
        &self,
        session_key: &str,
        proposal: &WorkflowProposal,
        setup_artifacts: &[Artifact],
        now: DateTime<Utc>,
    ) -> SchedulerResult<SessionSlotReservation> {
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

    pub fn reserve_approved_session(
        &self,
        run_id: RunId,
        session_key: &str,
        now: DateTime<Utc>,
    ) -> SchedulerResult<SessionSlotReservation> {
        NaiveDate::parse_from_str(session_key, "%Y-%m-%d")
            .map_err(|_| SchedulerError::InvalidSessionKey(session_key.to_owned()))?;
        let lease = self.acquire_or_renew(now)?;
        let setup_artifacts = self.paper_snapshot_artifacts(&run_id, session_key, now)?;
        Ok(self.workflow.reserve_approved_paper_session(
            &lease,
            run_id,
            session_key,
            "paper.approved.v1",
            &setup_artifacts,
            now,
        )?)
    }
}
