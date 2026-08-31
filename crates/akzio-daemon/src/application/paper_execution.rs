use crate::*;

/// Deterministic decision, execution, commitment and reconciliation capability.
pub(crate) struct PaperExecution<'a> {
    daemon: &'a Daemon,
}

impl<'a> PaperExecution<'a> {
    pub(crate) const fn new(daemon: &'a Daemon) -> Self {
        Self { daemon }
    }

    pub(crate) async fn session_is_current(&self, task: &ClaimedAttempt) -> Result<bool> {
        let Some(paper) = self.daemon.paper.paper_observer.as_ref() else {
            return Ok(true);
        };
        let expected = self
            .daemon
            .store
            .session_slot_for_run(&task.run_id)?
            .map(|slot| slot.session_key)
            .ok_or_else(|| DaemonError::InvalidInput("Paper run has no session slot".to_owned()))?;
        let clock = paper
            .market_clock()
            .await
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        Ok(clock.is_open && clock.session_date.to_string() == expected)
    }

    pub(crate) fn decision_gate(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        let proposal = self
            .daemon
            .terminal_input(task, ArtifactKind::DecisionProposal)?;
        self.daemon.decision_runtime.decide(&DecisionGateInput {
            permit: task.permit.clone(),
            proposal,
            now,
        })?;
        Ok(TaskCompletion::Committed)
    }

    pub(crate) async fn execution_gate(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        let decision_context = self
            .daemon
            .terminal_input(task, ArtifactKind::DecisionContext)?;
        let (account_snapshot, quote_snapshot, market_clock_snapshot) = if self
            .daemon
            .production_evidence
            .contains_key(&EvidenceSource::Alpaca)
            && self.daemon.store.run_purpose(&task.run_id)? == RunPurpose::Paper
        {
            self.daemon.refresh_execution_snapshots(task, now).await?
        } else {
            self.daemon.execution_snapshot_inputs(task)?
        };
        let gate_now = Utc::now();
        // Snapshot acquisition is a separately governed Evidence path. Until a
        // provider returns typed, task-bound snapshots, the execution runtime
        // emits a durable NoOrder rather than guessing from arbitrary evidence.
        let output = self
            .daemon
            .execution_runtime
            .evaluate(&ExecutionGateInput {
                permit: task.permit.clone(),
                decision_context,
                account_snapshot,
                quote_snapshot,
                market_clock_snapshot,
                now: gate_now,
            })?;
        self.daemon
            .execution_runtime
            .commit(&task.permit, &output, gate_now)?;
        Ok(TaskCompletion::Committed)
    }

    pub(crate) fn commit(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        let verdict = self
            .daemon
            .terminal_input(task, ArtifactKind::ExecutionVerdict)?;
        let verdict_payload: ExecutionVerdict = self.daemon.read_artifact_payload(&verdict)?;
        verdict_payload
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        let ExecutionVerdict::Accepted { execution_context } = verdict_payload else {
            return Ok(TaskCompletion::NoOutput);
        };
        let context: ExecutionContext = self.daemon.read_artifact_payload(&execution_context)?;
        context
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        let session_key = context.broker_session.ok_or_else(|| {
            DaemonError::InvalidInput("accepted execution verdict has no broker session".to_owned())
        })?;
        let lease = self.daemon.paper.scheduler.active_lease(now)?;
        self.daemon
            .paper_commitment_runtime
            .commit(&PaperCommitmentInput {
                lease,
                permit: task.permit.clone(),
                verdict,
                session_key,
                now,
            })?;
        Ok(TaskCompletion::Committed)
    }

    pub(crate) async fn reconcile(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        if self.daemon.store.run_purpose(&task.run_id)? != RunPurpose::Paper {
            return Ok(TaskCompletion::NoOutput);
        }
        let verdict = self
            .daemon
            .terminal_input(task, ArtifactKind::ExecutionVerdict)?;
        let verdict_payload: ExecutionVerdict = self.daemon.read_artifact_payload(&verdict)?;
        verdict_payload
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        if matches!(verdict_payload, ExecutionVerdict::NoOrder { .. }) {
            return Ok(TaskCompletion::NoOutput);
        }
        let commitment = self
            .daemon
            .terminal_input(task, ArtifactKind::ExecutionCommitment)?;
        let broker = self.daemon.paper.paper_broker.as_ref().ok_or_else(|| {
            DaemonError::Unavailable(
                "Paper reconciliation requires an injected Alpaca Paper broker adapter".to_owned(),
            )
        })?;
        let lease = self.daemon.paper.scheduler.active_lease(now)?;
        self.daemon
            .paper_dispatch_runtime
            .dispatch(
                broker.as_ref(),
                &PaperDispatchInput {
                    lease,
                    permit: task.permit.clone(),
                    commitment,
                    now,
                },
            )
            .await?;
        Ok(TaskCompletion::Committed)
    }
}
