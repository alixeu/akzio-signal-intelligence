use super::*;

impl Daemon {
    pub fn reserve_paper_session_with_inputs(
        &self,
        session_key: &str,
        proposal: &WorkflowProposal,
        setup_artifacts: &[Artifact],
        now: DateTime<Utc>,
    ) -> Result<akzio_store::v2::SessionSlotReservation> {
        Ok(self.paper.scheduler.reserve_session_with_inputs(
            session_key,
            proposal,
            setup_artifacts,
            now,
        )?)
    }

    pub fn reserve_paper_session_with_inputs_for_run(
        &self,
        run_id: RunId,
        session_key: &str,
        proposal: &WorkflowProposal,
        setup_artifacts: &[Artifact],
        now: DateTime<Utc>,
    ) -> Result<akzio_store::v2::SessionSlotReservation> {
        Ok(self.paper.scheduler.reserve_session_with_inputs_for_run(
            run_id,
            session_key,
            proposal,
            setup_artifacts,
            now,
        )?)
    }

    pub async fn serve_scheduler<C, P>(
        &self,
        clock: &C,
        source: &P,
        poll_interval: std::time::Duration,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()>
    where
        C: BrokerSessionClock + ?Sized,
        P: PaperWorkflowSource + ?Sized,
    {
        if !self.paper.auto_paper {
            return Err(DaemonError::InvalidInput(
                "Paper scheduler requires auto_paper=true".to_owned(),
            ));
        }
        self.paper
            .scheduler
            .serve(clock, source, poll_interval, shutdown)
            .await?;
        Ok(())
    }

    /// Runs the only automatic Paper entrypoint: a broker-authoritative clock,
    /// a Rust-validated workflow source, and the worker pool share shutdown.
    pub async fn serve_with_paper_scheduler<C, P>(
        &self,
        clock: &C,
        source: &P,
        poll_interval: std::time::Duration,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()>
    where
        C: BrokerSessionClock + ?Sized,
        P: PaperWorkflowSource + ?Sized,
    {
        if !self.paper.auto_paper {
            return Err(DaemonError::InvalidInput(
                "Paper scheduler requires auto_paper=true".to_owned(),
            ));
        }
        tokio::try_join!(
            self.serve_scheduler(clock, source, poll_interval, shutdown.clone()),
            self.serve_worker_pool(shutdown),
        )?;
        Ok(())
    }

    pub(crate) async fn request_cancel(&self, run_id: &RunId, reason: &str) -> Result<u64> {
        Ok(u64::from(
            self.task_runtime
                .request_cancel(run_id, reason, Utc::now())
                .await?,
        ))
    }

    pub(crate) fn retry_run(&self, source_run_id: &RunId) -> Result<RunId> {
        match self.store.run_purpose(source_run_id)? {
            RunPurpose::Debug | RunPurpose::PositionPlan | RunPurpose::PaperDryRun => {}
            RunPurpose::Paper => {
                return Err(DaemonError::InvalidInput(
                    "Paper runs are scheduler-owned and cannot be retried by an operator"
                        .to_owned(),
                ));
            }
            RunPurpose::Replay | RunPurpose::Shadow => {
                return Err(DaemonError::InvalidInput(
                    "only Debug and Paper Dry Run runs may be retried by an operator".to_owned(),
                ));
            }
        }
        Ok(self.workflow.retry_run(source_run_id, Utc::now())?)
    }

    pub(crate) fn replay_report(&self, run_id: &RunId) -> Result<ReplayReport> {
        let snapshot = self.workflow.replay_run(run_id)?;
        Ok(ReplayReport {
            run_id: snapshot.run.run_id,
            purpose: snapshot.run.purpose,
            status: snapshot.status,
            revision: snapshot.revision.revision,
            task_count: snapshot.tasks.len(),
            terminal_task_count: snapshot
                .tasks
                .iter()
                .filter(|task| {
                    matches!(
                        task.status,
                        TaskStatus::Succeeded
                            | TaskStatus::Failed
                            | TaskStatus::Cancelled
                            | TaskStatus::Skipped
                    )
                })
                .count(),
            event_cursor: snapshot.event_cursor,
            cancel_requested: snapshot.cancel_requested,
        })
    }

    pub(crate) fn retrospectives(&self, run_id: &RunId) -> Result<Vec<RetrospectiveView>> {
        let run_purpose = self.store.run_purpose(run_id)?;
        if !matches!(run_purpose, RunPurpose::Paper) {
            return Ok(Vec::new());
        }
        self.store
            .retrospectives(run_id)?
            .into_iter()
            .map(|artifact| {
                let payload: Retrospective =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                payload.validate().map_err(|error| {
                    DaemonError::InvalidInput(format!(
                        "invalid retrospective crossed query gate: {error}"
                    ))
                })?;
                Ok(RetrospectiveView {
                    artifact_id: artifact.artifact_id,
                    payload,
                })
            })
            .collect()
    }

    pub(crate) fn trajectory(&self, run_id: &RunId) -> Result<Vec<TrajectoryEntry>> {
        Ok(self.store.trajectory(run_id)?)
    }

    pub(crate) fn set_freeze(&self, frozen: bool, reason: String) -> Result<DaemonHealth> {
        self.store.write_freeze_state(frozen, reason, Utc::now())?;
        self.health()
    }
}
