//! Durable Paper outcome scheduling.
//!
//! Scheduling is deliberately separate from outcome materialization: a Paper
//! terminal chain records the exact decision/execution lineage now, while only
//! later governed market observations may seal an `Outcome` and affect policy.

use akzio_domain::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactRef, Decision, DecisionContext, DomainError,
    ExecutionContext, ExecutionVerdict, OutcomeExecutionLineage, OutcomeId, OutcomeSchedule,
    PaperCommitment, Reconciliation, ReconciliationState, RunPurpose, TaskStatus, TaskWritePermit,
    DOMAIN_SCHEMA_VERSION,
};
use akzio_store::{Store, StoreError};
use chrono::{DateTime, NaiveDate, Utc};
use serde::de::DeserializeOwned;
use thiserror::Error;

// 文件导读：OutcomeSchedule 是 T0 Paper 终态到未来 T+1/T+3/T+5 worker 的不可变桥梁；
// 本文件只验证并提交 lineage，不读取未来行情、不计算指标，也不提前改变 Policy。

#[derive(Debug, Error)]
pub enum OutcomeScheduleError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("outcome schedule requires Paper run, got {0:?}")]
    NonPaperRun(RunPurpose),
    #[error("expected {expected:?} artifact, found {actual:?}")]
    WrongArtifactKind {
        expected: ArtifactKind,
        actual: ArtifactKind,
    },
    #[error("outcome schedule artifact lineage is invalid: {0}")]
    InvalidLineage(&'static str),
}

pub type OutcomeScheduleResult<T> = std::result::Result<T, OutcomeScheduleError>;

#[derive(Debug, Clone)]
pub struct OutcomeScheduleInput {
    pub permit: TaskWritePermit,
    pub decision: ArtifactRef,
    pub decision_context: ArtifactRef,
    pub execution_context: ArtifactRef,
    pub execution: OutcomeExecutionLineage,
    pub baseline_trading_day: NaiveDate,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct OutcomeScheduleOutput {
    pub schedule: Artifact,
}

/// Owns the immutable schedule written after a Paper terminal chain. It does
/// not materialize outcomes, mutate memory, or select policy transitions.
#[derive(Debug, Clone)]
pub struct OutcomeSchedulingRuntime {
    store: Store,
    enqueue_worker: bool,
}

impl OutcomeSchedulingRuntime {
    // Store 按值交给 runtime 持有；worker 默认关闭，所以 new 只准备配置而不排队任务。
    pub fn new(store: Store) -> Self {
        // 默认不启用 worker：new 本身不排队任务；schedule 会经 Store stage blob，
        // 但是否安装 post-terminal worker 只在 commit 时由显式配置决定。
        Self {
            store,
            enqueue_worker: false,
        }
    }

    // self 按值接收，mut 仅允许改动当前 runtime 的 enqueue_worker；返回修改后的所有权，
    // 调用者需接住返回值才能保留开关设置。
    pub fn with_worker_enabled(mut self, enabled: bool) -> Self {
        // 该开关只选择两种既有 Store commit 入口，不改变 schedule payload 或 Paper 资格。
        self.enqueue_worker = enabled;
        self
    }

    pub fn schedule(
        &self,
        input: &OutcomeScheduleInput,
    ) -> OutcomeScheduleResult<OutcomeScheduleOutput> {
        // input 以共享借用提供 permit 和 ArtifactRef；这里先从 Store 读取并校验完整 Paper
        // 决策/执行 lineage，再 stage 一个 schedule Artifact 返回。OutcomeScheduleOutput
        // 只是待 commit 的结果，不代表 Artifact、worker 或 Outcome 已正式提交。
        // schedule 先核验 permit 和 Paper purpose，再从同一 Store/CAS 读取 Decision、Context、
        // ExecutionContext 与执行终态；任何 lineage 不一致都在写入前返回错误。
        self.store.validate_task_permit(&input.permit)?;
        let purpose = self.store.run_purpose(&input.permit.run_id)?;
        if purpose != RunPurpose::Paper {
            return Err(OutcomeScheduleError::NonPaperRun(purpose));
        }

        let decision = self.load_expected(&input.decision, ArtifactKind::Decision)?;
        let decision_payload: Decision = self.read_payload(&decision)?;
        decision_payload.validate()?;
        if decision_payload.decision_context != input.decision_context
            || !decision.source_refs.contains(&input.decision_context)
        {
            return Err(OutcomeScheduleError::InvalidLineage("decision_context"));
        }

        let context = self.load_expected(&input.decision_context, ArtifactKind::DecisionContext)?;
        let context_payload: DecisionContext = self.read_payload(&context)?;
        context_payload.validate()?;
        if context_payload.run_id != input.permit.run_id {
            return Err(OutcomeScheduleError::InvalidLineage("decision_run"));
        }

        let execution_context =
            self.load_expected(&input.execution_context, ArtifactKind::ExecutionContext)?;
        let execution_context_payload: ExecutionContext = self.read_payload(&execution_context)?;
        execution_context_payload.validate()?;
        if execution_context_payload.run_id != input.permit.run_id
            || execution_context_payload.decision_context != input.decision_context
            || !execution_context
                .source_refs
                .contains(&input.decision_context)
        {
            return Err(OutcomeScheduleError::InvalidLineage("execution_context"));
        }

        self.validate_execution_lineage(&input.execution, &input.execution_context)?;
        let payload = OutcomeSchedule {
            schema_version: DOMAIN_SCHEMA_VERSION,
            outcome_id: OutcomeId::new(),
            decision: input.decision.clone(),
            decision_context: input.decision_context.clone(),
            execution_context: input.execution_context.clone(),
            execution: input.execution.clone(),
            baseline_trading_day: input.baseline_trading_day,
            created_at: input.now,
        };
        payload.validate()?;

        let mut source_refs = vec![
            input.decision.clone(),
            input.decision_context.clone(),
            input.execution_context.clone(),
        ];
        match &input.execution {
            // NoOrder 只绑定 verdict；真实 Paper lineage 还保留 commitment 与 reconciliation，
            // 让后续学习能区分 Decision 被接受、订单已提交和已完成对账。
            OutcomeExecutionLineage::NoOrder { execution_verdict } => {
                source_refs.push(execution_verdict.clone());
            }
            OutcomeExecutionLineage::ReconciledPaper {
                execution_verdict,
                commitment,
                reconciliation,
            } => {
                source_refs.extend([
                    execution_verdict.clone(),
                    commitment.clone(),
                    reconciliation.clone(),
                ]);
            }
        }
        let schedule = Artifact::new(
            ArtifactKind::OutcomeSchedule,
            // `commit` must consume this prepared Artifact through the same Store.
            self.store.stage_json(&payload)?,
            "learning.outcome_schedule",
            ArtifactLifecycle::Canonical,
            crate::trusted_learning_provenance(&input.permit, input.now),
            Some(input.permit.artifact_origin()),
            source_refs,
            input.now,
        )?;
        // 这里仅 stage payload 并返回 Artifact；尚未 commit，所以调用者丢弃 output 不会留下
        // 一个看似已创建的 OutcomeSchedule 或未来 worker。
        Ok(OutcomeScheduleOutput { schedule })
    }

    pub fn commit(
        &self,
        permit: &TaskWritePermit,
        output: &OutcomeScheduleOutput,
        now: DateTime<Utc>,
    ) -> OutcomeScheduleResult<()> {
        // permit/output 共享借用，commit 选择启用 worker 的原子 Store API 或普通 Attempt API；
        // Ok 仅表示该提交入口成功，排期尚未等待真实行情，也未生成 sealed Outcome。
        // 启用 worker 时 Store 把 schedule、成功 Attempt 和 post-terminal worker 放进同一
        // 事务；否则只按普通 succeeded Attempt 提交 schedule。两条路径都不会密封 Outcome。
        if self.enqueue_worker {
            self.store
                .commit_outcome_schedule_with_worker(permit, &output.schedule, now)?;
        } else {
            self.store.commit_attempt(
                permit,
                std::slice::from_ref(&output.schedule),
                TaskStatus::Succeeded,
                now,
            )?;
        }
        Ok(())
    }

    fn validate_execution_lineage(
        &self,
        lineage: &OutcomeExecutionLineage,
        execution_context: &ArtifactRef,
    ) -> OutcomeScheduleResult<()> {
        // 按 lineage 的 enum 变体穷尽分支并从 Store 解码对应 CAS：NoOrder 到 verdict 为止；
        // ReconciledPaper 必须走 Accepted → Commitment → Complete Reconciliation 的来源链。
        // 每步失败立即 Err，因此后续 schedule 不会被创建。
        // NoOrder 只需绑定 NoOrder verdict/context；ReconciledPaper 还必须依次绑定 Accepted
        // verdict、ExecutionCommitment 和 Complete Reconciliation。Complete 仅证明对账终态，
        // 不把每张订单的 accepted 当成 filled；实际成交仍以 receipt 内容核验。
        match lineage {
            OutcomeExecutionLineage::NoOrder { execution_verdict } => {
                let verdict =
                    self.load_expected(execution_verdict, ArtifactKind::ExecutionVerdict)?;
                let payload: ExecutionVerdict = self.read_payload(&verdict)?;
                payload.validate()?;
                let ExecutionVerdict::NoOrder { no_order } = payload else {
                    return Err(OutcomeScheduleError::InvalidLineage("no_order_verdict"));
                };
                if no_order.execution_context != *execution_context
                    || !verdict.source_refs.contains(execution_context)
                {
                    return Err(OutcomeScheduleError::InvalidLineage("no_order_context"));
                }
            }
            OutcomeExecutionLineage::ReconciledPaper {
                execution_verdict,
                commitment,
                reconciliation,
            } => {
                let verdict =
                    self.load_expected(execution_verdict, ArtifactKind::ExecutionVerdict)?;
                let payload: ExecutionVerdict = self.read_payload(&verdict)?;
                payload.validate()?;
                let ExecutionVerdict::Accepted {
                    execution_context: accepted_context,
                } = payload
                else {
                    return Err(OutcomeScheduleError::InvalidLineage("accepted_verdict"));
                };
                if accepted_context != *execution_context
                    || !verdict.source_refs.contains(execution_context)
                {
                    return Err(OutcomeScheduleError::InvalidLineage("accepted_context"));
                }

                let commitment_artifact =
                    self.load_expected(commitment, ArtifactKind::ExecutionCommitment)?;
                let commitment_payload: PaperCommitment =
                    self.read_payload(&commitment_artifact)?;
                commitment_payload.validate()?;
                if commitment_payload.execution_context != *execution_context
                    || !commitment_artifact.source_refs.contains(execution_verdict)
                {
                    return Err(OutcomeScheduleError::InvalidLineage("commitment"));
                }

                let reconciliation_artifact =
                    self.load_expected(reconciliation, ArtifactKind::Reconciliation)?;
                let reconciliation_payload: Reconciliation =
                    self.read_payload(&reconciliation_artifact)?;
                reconciliation_payload.validate()?;
                require_complete_reconciliation(reconciliation_payload.state)?;
                if reconciliation_payload.commitment != *commitment
                    || !reconciliation_artifact.source_refs.contains(commitment)
                {
                    return Err(OutcomeScheduleError::InvalidLineage("reconciliation"));
                }
            }
        }
        Ok(())
    }

    fn load_expected(
        &self,
        reference: &ArtifactRef,
        expected: ArtifactKind,
    ) -> OutcomeScheduleResult<Artifact> {
        // 用 Artifact ID 从唯一 Store 读取，再同时比对调用方声明 kind 与持久化 kind；
        // 只返回真实匹配的 Artifact，不会因为引用字段声明正确就信任底层对象。
        let artifact = self.store.artifact(&reference.artifact_id)?;
        if reference.kind != expected || artifact.kind != expected {
            return Err(OutcomeScheduleError::WrongArtifactKind {
                expected,
                actual: artifact.kind,
            });
        }
        Ok(artifact)
    }

    fn read_payload<T: DeserializeOwned>(&self, artifact: &Artifact) -> OutcomeScheduleResult<T> {
        // T: DeserializeOwned 表示反序列化出的 T 不借用临时 BLOB 字节；泛型具体类型在调用点确定，
        // 错误由 JSON/Store 到 OutcomeScheduleError 的 From 转换后传播。
        Ok(serde_json::from_slice(
            &self.store.read_blob(&artifact.blob)?,
        )?)
    }
}

fn require_complete_reconciliation(state: ReconciliationState) -> OutcomeScheduleResult<()> {
    // Complete 才表示该 lineage 可作为已完成执行来源；其他状态返回 InvalidLineage，
    // 不把 accepted、partial fill 或待处理自动升级为完成。
    // 只有完整对账才允许建立“已执行” Outcome lineage；部分成交/待成交继续留在执行链。
    if state != ReconciliationState::Complete {
        return Err(OutcomeScheduleError::InvalidLineage(
            "reconciliation_not_complete",
        ));
    }
    Ok(())
}
