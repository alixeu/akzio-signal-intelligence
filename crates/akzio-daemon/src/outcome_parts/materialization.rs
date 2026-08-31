use super::*;

impl Daemon {
    pub(crate) fn realized_execution_target(
        &self,
        schedule: &OutcomeSchedule,
        execution_context: &ExecutionContext,
    ) -> Result<TargetPortfolio> {
        let account_reference = execution_context.account_snapshot.as_ref().ok_or_else(|| {
            DaemonError::InvalidInput(
                "Outcome execution context has no account snapshot".to_owned(),
            )
        })?;
        let account: AccountSnapshot = self.read_artifact_payload(account_reference)?;
        let mut plan = None;
        let mut receipts = Vec::new();
        if let OutcomeExecutionLineage::ReconciledPaper { reconciliation, .. } = &schedule.execution
        {
            let reconciliation: Reconciliation = self.read_artifact_payload(reconciliation)?;
            if reconciliation.state != ReconciliationState::Complete {
                return Err(DaemonError::InvalidInput(
                    "Outcome requires complete reconciliation".to_owned(),
                ));
            }
            let plan_reference = execution_context.execution_plan.as_ref().ok_or_else(|| {
                DaemonError::InvalidInput("Outcome execution context has no plan".to_owned())
            })?;
            plan = Some(self.read_artifact_payload(plan_reference)?);
            for receipt_reference in &reconciliation.broker_receipts {
                receipts.push(self.read_artifact_payload(receipt_reference)?);
            }
        }
        akzio_learning::realized_execution_target(
            &account,
            &schedule.execution,
            plan.as_ref(),
            &receipts,
        )
        .map_err(|error| DaemonError::InvalidInput(error.to_string()))
    }
}
