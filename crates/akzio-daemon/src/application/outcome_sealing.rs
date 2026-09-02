use crate::*;

/// Schedules and seals outcome lineage for terminal workflow tasks.
pub(crate) struct OutcomeSealing<'a> {
    daemon: &'a Daemon,
}

impl<'a> OutcomeSealing<'a> {
    pub(crate) const fn new(daemon: &'a Daemon) -> Self {
        Self { daemon }
    }

    pub(crate) async fn execute(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        let purpose = self.daemon.store.run_purpose(&task.run_id)?;
        if purpose != RunPurpose::Paper {
            return if purpose == RunPurpose::Shadow {
                self.daemon.execute_shadow_evaluate(task, now).await
            } else {
                Ok(TaskCompletion::NoOutput)
            };
        }
        let decision = self.daemon.terminal_input(task, ArtifactKind::Decision)?;
        let decision_context = self
            .daemon
            .terminal_input(task, ArtifactKind::DecisionContext)?;
        let execution_context = self
            .daemon
            .terminal_input(task, ArtifactKind::ExecutionContext)?;
        let verdict = self
            .daemon
            .terminal_input(task, ArtifactKind::ExecutionVerdict)?;
        let verdict_payload: ExecutionVerdict = self.daemon.read_artifact_payload(&verdict)?;
        verdict_payload
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        let execution = match verdict_payload {
            ExecutionVerdict::NoOrder { .. } => OutcomeExecutionLineage::NoOrder {
                execution_verdict: verdict,
            },
            ExecutionVerdict::Accepted { .. } => OutcomeExecutionLineage::ReconciledPaper {
                execution_verdict: verdict,
                commitment: self
                    .daemon
                    .terminal_input(task, ArtifactKind::ExecutionCommitment)?,
                reconciliation: self
                    .daemon
                    .terminal_input(task, ArtifactKind::Reconciliation)?,
            },
        };
        let baseline_trading_day = self.daemon.paper_baseline_day(&task.run_id)?;
        let output = self
            .daemon
            .outcome_scheduling_runtime
            .schedule(&OutcomeScheduleInput {
                permit: task.permit.clone(),
                decision,
                decision_context,
                execution_context,
                execution,
                baseline_trading_day,
                now,
            })?;
        self.daemon
            .outcome_scheduling_runtime
            .commit(&task.permit, &output, now)?;
        Ok(TaskCompletion::Committed)
    }
}
