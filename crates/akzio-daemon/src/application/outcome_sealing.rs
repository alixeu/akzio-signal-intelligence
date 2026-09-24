// 文件导读：OutcomeSealing 在 Paper terminal graph 完成后读取 Decision、Context、Verdict
// 和必要的 Commitment/Reconciliation，提交冻结 OutcomeSchedule；PositionPlan 不创建
// schedule，Shadow 进入独立评估。Committed 只代表后续 T+1/T+3/T+5 已排期，不能代表
// Paper fill、sealed Outcome 或 learning eligibility。
// Rust 机制：借用 Daemon 的门面按 `RunPurpose` 分支；`ExecutionVerdict` 枚举保证 NoOrder
// 与 Accepted lineage 不混淆；`?` 让缺失 Artifact/校验错误在写 schedule 前 fail closed。

use crate::*;

/// Schedules and seals outcome lineage for terminal workflow tasks.
pub(crate) struct OutcomeSealing<'a> {
    daemon: &'a Daemon,
}

impl<'a> OutcomeSealing<'a> {
    // 保存 Daemon 借用；Outcome 的计算和学习资格仍由 outcome runtime/Store 负责。
    pub(crate) const fn new(daemon: &'a Daemon) -> Self {
        Self { daemon }
    }

    // 为 Paper Run 组装 Decision、Execution 和 Outcome 的冻结血缘，并提交
    // OutcomeSchedule；Committed 只表示调度已持久化，不表示 T+1/T+3/T+5 已完成或已学习。
    pub(crate) async fn execute(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        let purpose = self.daemon.store.run_purpose(&task.run_id)?;
        if purpose != RunPurpose::Paper {
            // Shadow 走独立的评估入口，PositionPlan/其他 purpose 不创建 OutcomeSchedule；
            // NoOutput 是该节点对当前 purpose 不适用，不是业务链路整体成功。
            return if purpose == RunPurpose::Shadow {
                self.daemon.execute_shadow_evaluate(task, now).await
            } else {
                Ok(TaskCompletion::NoOutput)
            };
        }
        // 下面的 terminal_input 只读取本 Run 已成功节点的终态 Artifact；任何缺失或
        // 类型不匹配都会在 Store/Daemon 边界返回错误，不用不完整输入推导 Outcome。
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
        // NoOrder 仍保留 Decision 到 ExecutionVerdict 的 lineage；只有 Accepted 才要求
        // 已持久化的 Commitment 和 Reconciliation，不能把受理订单写成已成交。
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
        // commit 只建立后续交易日窗口的 OutcomeSchedule；后续 worker 才按真实共同
        // 交易 Session 评估并可能进入封存/学习资格检查。
        self.daemon
            .outcome_scheduling_runtime
            .commit(&task.permit, &output, now)?;
        Ok(TaskCompletion::Committed)
    }
}
