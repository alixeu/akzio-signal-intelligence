// 文件导读：materialization 从冻结 Account/Quote、Execution lineage、Plan/Receipt 重建
// Outcome 的 realized target/metrics。NoOrder 与 ReconciledPaper 的来源不同，只有 complete
// reconciliation 才读取成交回执；计算结果仍需 Outcome window seal 和后续 learning gate。
// Rust 机制：枚举模式匹配保证 NoOrder/Accepted 不混淆；`Option` 表示可选 plan/receipt；
// 泛型 `read_artifact_payload<T>` 通过 serde 取得强类型，`Result` 在缺失/未完成对账时拒绝。

use super::*;

impl Daemon {
    pub(crate) fn realized_execution_target(
        &self,
        schedule: &OutcomeSchedule,
        execution_context: &ExecutionContext,
    ) -> Result<TargetPortfolio> {
        // 只返回同一 realized_execution 重建出的 target，避免调用方从 Decision target
        // 误读成交后敞口。
        Ok(self.realized_execution(schedule, execution_context)?.target)
    }

    pub(crate) fn realized_execution(
        &self,
        schedule: &OutcomeSchedule,
        execution_context: &ExecutionContext,
    ) -> Result<akzio_learning::RealizedExecution> {
        // account 是所有 lineage 的必要 baseline；Accepted path 额外要求 complete
        // reconciliation/plan/receipts，NoOrder 则保留无成交语义交给 learning runtime。
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
        let quotes: QuoteSnapshot =
            self.read_artifact_payload(execution_context.quote_snapshot.as_ref().ok_or_else(
                || DaemonError::InvalidInput("Outcome baseline quotes missing".to_owned()),
            )?)?;
        quotes.validate()?;
        let prices = quotes
            .quotes
            .iter()
            .map(|(asset, quote)| {
                let midpoint = i128::from(quote.bid.0) + i128::from(quote.ask.0);
                (*asset, MoneyMicros((midpoint / 2) as i64))
            })
            .collect();
        akzio_learning::realized_execution_at_prices(
            &account,
            &schedule.execution,
            plan.as_ref(),
            &receipts,
            self.paper.outcome_cost_model,
            &prices,
        )
        .map_err(|error| DaemonError::InvalidInput(error.to_string()))
    }
}
