//! Fenced durable Paper commitment for the execution path.
//!
//! This module performs no network I/O itself; upstream snapshot refresh may
//! already have read broker/market data. It checks an Accepted verdict and
//! plan closure, then asks Store to fence the scheduler lease,
//! session slot and active permit while persisting the commitment. Dispatch
//! must reload that durable identity before any broker write.

// 文件导读：PaperCommitmentRuntime 是 Decision/Execution 与 Broker 之间的持久化断点。
// 它只读取并校验 Paper run、Accepted ExecutionVerdict、完整 ExecutionContext/Plan、审批
// 和 session slot，生成确定性 client_order_id 后交给 Store fenced transaction 提交；
// 本方法不发网络请求（此前 Gate 可已做只读快照刷新）。重复领取同一 session 时
// 返回已存在且逐项相同的 commitment，不会新建第二套订单身份。

use akzio_domain::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactRef, Asset, DomainError, ExecutionContext,
    ExecutionVerdict, FreezeState, OrderSide, PaperCommitment, PaperCommitmentId, RunPurpose,
    TaskWritePermit,
};
use akzio_store::{DaemonLease, ExecutionCommit, Store, StoreError};
use chrono::{DateTime, Utc};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PaperCommitmentError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("expected {expected:?} artifact, found {actual:?}")]
    WrongArtifactKind {
        expected: ArtifactKind,
        actual: ArtifactKind,
    },
    #[error("Paper commitment requires a Paper run, got {0:?}")]
    NonPaperRun(RunPurpose),
    #[error("Paper commitment requires an accepted execution verdict")]
    VerdictRejected,
    #[error("accepted verdict execution context does not match the stored context")]
    VerdictContextMismatch,
    #[error("Paper commitment session does not match execution context")]
    SessionMismatch,
    #[error("frozen execution context cannot create a Paper commitment")]
    Frozen,
    #[error("execution context has no persisted allocation plan")]
    MissingAllocationPlan,
    #[error("allocation plan hash does not match execution context")]
    PlanHashMismatch,
    #[error("legacy allocation risk semantics cannot create a new Paper commitment")]
    LegacyRiskModel,
    #[error("allocation plan contains multiple orders for {0}")]
    DuplicateAssetOrder(Asset),
    #[error("session already contains a different Paper commitment")]
    ExistingCommitmentMismatch,
    #[error("Paper approval expired before commitment")]
    ApprovalExpired,
    #[error("execution plan exceeds approved maximum notional")]
    ApprovalNotionalExceeded,
    #[error("Paper approval is missing")]
    ApprovalMissing,
    #[error("execution plan notional overflow")]
    ApprovalNotionalOverflow,
}

pub type PaperCommitmentResult<T> = std::result::Result<T, PaperCommitmentError>;

fn require_current_risk_model(allocation: &crate::ExecutionPlan) -> PaperCommitmentResult<()> {
    // 通过共享借用查询计划的序列化语义；当前风险模型不匹配返回专用错误，不改写历史 plan。
    // 旧风险模型只允许历史读取，不能生成新的 Paper side effect；这里把版本边界放在
    // Commitment 前，而不是让 Broker 或 Reconcile 再猜测风险语义。
    if !allocation.uses_current_factor_exposure_model()? {
        return Err(PaperCommitmentError::LegacyRiskModel);
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct PaperCommitmentInput {
    pub lease: DaemonLease,
    pub permit: TaskWritePermit,
    pub verdict: ArtifactRef,
    pub session_key: String,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct PaperCommitmentOutput {
    pub commitment: Artifact,
    pub newly_committed: bool,
}

#[derive(Debug, Clone)]
pub struct PaperCommitmentRuntime {
    store: Store,
}

impl PaperCommitmentRuntime {
    pub fn new(store: Store) -> Self {
        // `Store` 按值移入并由 runtime 持有；Store 句柄代表统一持久化层，不在 runtime
        // 另建一份内存 commitment 状态。
        Self { store }
    }

    /// Persist or recover the one commitment permitted in this broker session.
    /// The result is durable before a Paper adapter can receive its client IDs.
    pub fn commit(
        &self,
        input: &PaperCommitmentInput,
    ) -> PaperCommitmentResult<PaperCommitmentOutput> {
        // 输入→确认 Paper purpose/freeze→读取并验证 Verdict/Context/Plan 闭包→核对 approval
        // 与金额→构造稳定 client IDs→检查 session slot→提交 fenced commitment。成功返回
        // 的 Artifact 才能被 dispatch 交给 Broker；已有同一 commitment 则走恢复分支。
        // 参数借用直到本方法结束；Store/JSON/Domain 错误由 `?` 向 scheduler 返回。
        // Purpose、Freeze 与 Verdict 失败发生在 commit_execution 之前，不会产生 Broker I/O。
        let purpose = self.store.run_purpose(&input.permit.run_id)?;
        if purpose != RunPurpose::Paper {
            return Err(PaperCommitmentError::NonPaperRun(purpose));
        }
        // Freeze Artifact 可缺省（表示当前没有已保存的 frozen=true 状态）；若存在则读取、
        // 验证 payload，frozen=true 立即拒绝。此处没有写 FreezeState 或修改账户。
        if let Some(freeze_artifact) = self
            .store
            .latest_artifact_by_kind(ArtifactKind::FreezeState)?
        {
            let freeze: FreezeState =
                serde_json::from_slice(&self.store.read_blob(&freeze_artifact.blob)?)?;
            freeze.validate()?;
            if freeze.frozen {
                return Err(PaperCommitmentError::Frozen);
            }
        }

        // Verdict 必须从 Store 的真实 Artifact 解码并通过 Domain 校验；按值匹配会把
        // Accepted 分支的 execution_context 引用移入局部变量，NoOrder 分支直接返回。
        let verdict_artifact =
            self.load_expected(&input.verdict, ArtifactKind::ExecutionVerdict)?;
        let verdict: ExecutionVerdict =
            serde_json::from_slice(&self.store.read_blob(&verdict_artifact.blob)?)?;
        verdict.validate()?;
        let ExecutionVerdict::Accepted { execution_context } = verdict else {
            return Err(PaperCommitmentError::VerdictRejected);
        };
        // 先检查 verdict→context 引用，再验 Context 的完整 plan closure；context.run_id、
        // Verdict source_refs 和 session_key 都必须与本次 permit/输入一致。
        let context_artifact =
            self.load_expected(&execution_context, ArtifactKind::ExecutionContext)?;
        let context: ExecutionContext =
            serde_json::from_slice(&self.store.read_blob(&context_artifact.blob)?)?;
        context.validate()?;
        context.validate_complete_plan_closure()?;
        if context.run_id != input.permit.run_id
            || !verdict_artifact
                .source_refs
                .iter()
                .any(|source| source == &execution_context)
        {
            return Err(PaperCommitmentError::VerdictContextMismatch);
        }
        if context.broker_session.as_deref() != Some(input.session_key.as_str()) {
            return Err(PaperCommitmentError::SessionMismatch);
        }
        if context.frozen {
            return Err(PaperCommitmentError::Frozen);
        }
        // Option<ArtifactRef> 缺失表示没有 ExecutionPlan，不能用 Decision target 冒充计划；
        // clone 后还要求 context Artifact 的 source_refs 真正声明了该计划。
        let allocation_reference = context
            .execution_plan
            .clone()
            .ok_or(PaperCommitmentError::MissingAllocationPlan)?;
        if !context_artifact.source_refs.contains(&allocation_reference) {
            return Err(PaperCommitmentError::MissingAllocationPlan);
        }
        let allocation_artifact =
            self.load_expected(&allocation_reference, ArtifactKind::ExecutionPlan)?;
        let allocation: crate::ExecutionPlan =
            serde_json::from_slice(&self.store.read_blob(&allocation_artifact.blob)?)?;
        allocation.validate()?;
        require_current_risk_model(&allocation)?;
        if allocation.plan_hash
            != context
                .plan_hash
                .as_ref()
                .ok_or(PaperCommitmentError::PlanHashMismatch)?
                .clone()
            || allocation.broker_session != input.session_key
        {
            return Err(PaperCommitmentError::PlanHashMismatch);
        }
        // 此处再检查 Paper approval 自身的过期时间与买单名义额；完整 qualification、
        // manifest 和 Gate blocker 已由上游执行链核验，不能把这项局部复查当全量授权。
        // try_fold 只加 Buy，溢出/超批准额或缺 approval 都阻断 commitment。
        if let Some((manifest, approval)) =
            self.store.paper_approval_for_run(&input.permit.run_id)?
        {
            if approval.expires_at < input.now {
                return Err(PaperCommitmentError::ApprovalExpired);
            }
            let buy_notional = allocation
                .orders
                .iter()
                .filter(|order| order.side == OrderSide::Buy)
                .try_fold(0_i64, |total, order| total.checked_add(order.notional.0))
                .ok_or(PaperCommitmentError::ApprovalNotionalOverflow)?;
            if buy_notional > manifest.maximum_notional.0 {
                return Err(PaperCommitmentError::ApprovalNotionalExceeded);
            }
        } else {
            return Err(PaperCommitmentError::ApprovalMissing);
        }
        // 以订单索引和 plan hash 构造每资产确定 ID；BTreeMap 保证映射唯一，重复同资产
        // 订单会在进入 Store fenced transaction 前拒绝。
        let mut client_order_ids = std::collections::BTreeMap::new();
        for (index, order) in allocation.orders.iter().enumerate() {
            let client_order_id =
                crate::paper::client_order_id(&input.session_key, &allocation.plan_hash, index, 0);
            if client_order_ids
                .insert(order.asset, client_order_id)
                .is_some()
            {
                return Err(PaperCommitmentError::DuplicateAssetOrder(order.asset));
            }
        }

        // 已有 session slot 若已指向 Commitment，只允许逐项完全相同的 plan/context/IDs 恢复；
        // 这是读路径，不创建第二个 Artifact 或另一个订单身份。
        if let Some(slot) = self.store.session_slot(&input.session_key)? {
            if let Some(existing_id) = slot.commitment_artifact_id {
                let existing_artifact = self.store.artifact(&existing_id)?;
                if existing_artifact.kind != ArtifactKind::ExecutionCommitment {
                    return Err(PaperCommitmentError::WrongArtifactKind {
                        expected: ArtifactKind::ExecutionCommitment,
                        actual: existing_artifact.kind,
                    });
                }
                let existing: PaperCommitment =
                    serde_json::from_slice(&self.store.read_blob(&existing_artifact.blob)?)?;
                existing.validate()?;
                if existing.execution_context != execution_context
                    || existing.plan_hash
                        != context
                            .plan_hash
                            .as_ref()
                            .ok_or(PaperCommitmentError::PlanHashMismatch)?
                            .clone()
                    || existing.broker_session != input.session_key
                    || existing.client_order_ids != client_order_ids
                {
                    return Err(PaperCommitmentError::ExistingCommitmentMismatch);
                }
                return Ok(PaperCommitmentOutput {
                    commitment: existing_artifact,
                    newly_committed: false,
                });
            }
        }

        // 新 commitment 的 payload 以 input.now 记录业务时间；后续 commit_execution 使用
        // 当前 UTC 时间验证 scheduler lease 并在一个 Store 事务中持久化 session slot 与 Artifact。
        let payload = PaperCommitment {
            commitment_id: PaperCommitmentId::new(),
            execution_context: execution_context.clone(),
            plan_hash: context
                .plan_hash
                .ok_or(PaperCommitmentError::PlanHashMismatch)?,
            broker_session: input.session_key.clone(),
            client_order_ids,
            created_at: input.now,
        };
        payload.validate()?;
        let commitment = Artifact::new(
            ArtifactKind::ExecutionCommitment,
            self.store.stage_json(&payload)?,
            "execution.paper_commitment",
            ArtifactLifecycle::Canonical,
            crate::trusted_execution_provenance(&input.permit, input.now),
            Some(input.permit.artifact_origin()),
            vec![input.verdict.clone(), execution_context],
            input.now,
        )?;
        let result = self.store.commit_execution(
            &input.lease,
            &ExecutionCommit {
                session_key: input.session_key.clone(),
                permit: input.permit.clone(),
                commitment: commitment.clone(),
                committed_at: Utc::now(),
            },
        )?;
        // 新写入时返回刚构造的 Artifact；若事务发现已有同一幂等 commitment，则重新从 Store
        // 读取其权威版本。只有此方法返回后，上层 dispatch 才可能接触 Paper adapter。
        let commitment = if result.newly_committed {
            commitment
        } else {
            self.store.artifact(&result.commitment_artifact_id)?
        };

        Ok(PaperCommitmentOutput {
            commitment,
            newly_committed: result.newly_committed,
        })
    }

    fn load_expected(
        &self,
        reference: &ArtifactRef,
        expected: ArtifactKind,
    ) -> PaperCommitmentResult<Artifact> {
        // 传入引用只借用；返回 Artifact 拥有完整元数据，后续 caller 再读取其 blob。
        // ArtifactRef 的声明 kind 与 Store 实际 kind 双重核对，避免只按 hash 读取错误载荷。
        let artifact = self.store.artifact(&reference.artifact_id)?;
        if reference.kind != expected || artifact.kind != expected {
            return Err(PaperCommitmentError::WrongArtifactKind {
                expected,
                actual: artifact.kind,
            });
        }
        Ok(artifact)
    }
}
