use super::*;

// 文件导读：PaperDispatchRuntime 是“已持久化 Commitment → Broker effect → 对账”的异步
// 调度层。它先验证 Paper run、debug broker policy、lease、permit 和 freeze，再记录
// effect intent，之后才允许 execute_commitment；恢复时复用 intent/订单 ID，轮询回执，
// 对 stale Regular-hours 订单按有界策略创建 reprice/cancel。Extended/Overnight 不因短
// 轮询超时盲目撤单，pending/partial 只写进度，只有 Reconciliation Complete 才结算 effect。

#[derive(Debug, Clone)]
pub struct PaperDispatchInput {
    pub lease: DaemonLease,
    pub permit: TaskWritePermit,
    pub commitment: ArtifactRef,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct PaperDispatchOutput {
    pub commitment: Artifact,
    pub execution: PaperExecution,
    pub reconciliation: ReconciliationOutput,
    pub settled: bool,
}

#[derive(Debug, Error)]
pub enum PaperDispatchError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Broker(#[from] PaperError),
    #[error(transparent)]
    Reconciliation(#[from] ReconciliationError),
    #[error("expected {expected:?} artifact, found {actual:?}")]
    WrongArtifactKind {
        expected: ArtifactKind,
        actual: ArtifactKind,
    },
    #[error("Paper dispatch requires Paper run, got {0:?}")]
    NonPaperRun(RunPurpose),
    #[error("execution is frozen")]
    Frozen,
    #[error("commitment is not the durable session commitment")]
    CommitmentNotDurable,
    #[error("commitment does not retain its execution context")]
    CommitmentContextMissing,
    #[error("commitment execution context does not match dispatch run or plan")]
    ContextMismatch,
    #[error("execution context has no persisted allocation plan")]
    MissingAllocationPlan,
    #[error("allocation plan hash does not match commitment")]
    PlanHashMismatch,
    #[error("broker response plan hash does not match commitment")]
    BrokerPlanHashMismatch,
    #[error("broker returned unsupported order status {0}")]
    UnsupportedReceiptStatus(String),
    #[error("broker returned replacement order for {0} without durable reprice intent")]
    ReplacementWithoutIntent(Asset),
}

pub type PaperDispatchResult<T> = std::result::Result<T, PaperDispatchError>;

/// Explicit, opt-in process crash used to verify recovery after the durable
/// broker effect intent commits and before the first broker request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PaperDispatchFailpoint {
    #[default]
    Disabled,
    ExitAfterEffectIntent,
}

impl PaperDispatchFailpoint {
    pub fn from_env() -> Self {
        // 诊断开关只用于验证“intent 已落盘、首个 broker 请求尚未发生”的崩溃恢复位置。
        Self::from_value(
            std::env::var("AKZIO_DIAGNOSTIC_CRASH_AFTER_EFFECT_INTENT")
                .ok()
                .as_deref(),
        )
    }

    fn from_value(value: Option<&str>) -> Self {
        // 仅接受显式的 1，其他环境值保持关闭，避免配置拼写意外触发进程退出。
        match value {
            Some("1") => Self::ExitAfterEffectIntent,
            _ => Self::Disabled,
        }
    }

    fn trigger_after_effect_intent(self) {
        // failpoint 位于 durable effect intent 之后、Broker I/O 之前，正好模拟最危险的未知状态。
        if matches!(self, Self::ExitAfterEffectIntent) {
            eprintln!("[akzio-diagnostic] exiting after durable execution.effect.intent (code 86)");
            std::process::exit(86);
        }
    }
}

pub const DEFAULT_PAPER_SETTLEMENT_TIMEOUT_SECS: u64 = 15;
pub const DEFAULT_PAPER_SETTLEMENT_ACTION_GRACE_SECS: u64 = 60;
const MAX_PAPER_REPRICES_PER_ORDER: u8 = 1;

#[derive(Debug, Clone)]
pub struct PaperDispatchRuntime {
    store: Store,
    execution_policy: crate::ExecutionPolicy,
    settlement_timeout: std::time::Duration,
    settlement_action_grace: std::time::Duration,
    failpoint: PaperDispatchFailpoint,
}

struct CommittedPlanContext {
    commitment_artifact: Artifact,
    commitment: PaperCommitment,
    plan: ExecutionPlan,
}

#[derive(Debug, Clone, Default)]
struct DurableOrderActions {
    reprices: Vec<ArtifactRef>,
    cancels: Vec<ArtifactRef>,
}

impl PaperDispatchRuntime {
    pub fn new(store: Store) -> Self {
        // 初始化默认执行策略、短 settlement 轮询和较长 action grace；这些是调度策略，
        // 不会改变上游 Decision/ExecutionGate 的风控边界。
        Self {
            store,
            execution_policy: crate::ExecutionPolicy::default(),
            settlement_timeout: std::time::Duration::from_secs(
                DEFAULT_PAPER_SETTLEMENT_TIMEOUT_SECS,
            ),
            settlement_action_grace: std::time::Duration::from_secs(
                DEFAULT_PAPER_SETTLEMENT_ACTION_GRACE_SECS,
            ),
            failpoint: PaperDispatchFailpoint::Disabled,
        }
    }

    pub fn with_execution_policy(mut self, policy: crate::ExecutionPolicy) -> Self {
        // builder 通过值接收策略，冻结本次 runtime 使用的执行限制。
        self.execution_policy = policy;
        self
    }

    pub fn with_settlement_timeout(mut self, settlement_timeout: std::time::Duration) -> Self {
        // 只调整等待 broker 终态的观察预算，超时不会把 accepted/partial 改成 filled。
        self.settlement_timeout = settlement_timeout;
        self
    }

    pub fn with_settlement_action_grace(
        mut self,
        settlement_action_grace: std::time::Duration,
    ) -> Self {
        // action grace 决定 Regular 未结订单何时进入有限的 reprice/cancel 处置，不适用于
        // Extended-hours 订单的长生命周期。
        self.settlement_action_grace = settlement_action_grace;
        self
    }

    pub fn with_failpoint(mut self, failpoint: PaperDispatchFailpoint) -> Self {
        // 仅替换诊断 failpoint，不改变 commitment、订单或 Store 内容。
        self.failpoint = failpoint;
        self
    }

    pub async fn dispatch<B: CommittedPaperBroker + ?Sized>(
        &self,
        broker: &B,
        input: &PaperDispatchInput,
    ) -> PaperDispatchResult<PaperDispatchOutput> {
        // 主流程：校验权限与 durable plan→记录 effect intent→执行/恢复 broker commitment→
        // 持续对账→必要时恢复 durable reprice/cancel→生成 Reconciliation。lease 在每个
        // 可能阻塞或产生副作用的边界重新校验，失败/取消会留下可恢复的 progress。
        self.require_paper_run(&input.permit)?;
        self.store.assert_debug_broker_write(&input.permit.run_id)?;
        let simulated_only = self
            .store
            .debug_session(&input.permit.run_id)?
            .is_some_and(|s| {
                s.identity.broker_write_policy == akzio_domain::DebugBrokerPolicy::SimulatedOnly
            });
        if simulated_only {
            return Err(PaperError::InvalidCommitment(
                "legacy simulated broker authority is retired".into(),
            )
            .into());
        }
        let CommittedPlanContext {
            commitment_artifact,
            commitment,
            plan,
        } = self.load_committed_plan(&input.permit, &input.commitment)?;
        let authorization = self.submission_authorization(&input.permit, &plan)?;

        self.ensure_unfrozen()?;
        self.store.validate_daemon_lease(&input.lease, Utc::now())?;
        self.store.validate_task_permit(&input.permit)?;
        let recovered = self.store.record_paper_effect_intent(
            &input.lease,
            &input.permit,
            &input.commitment,
            input.now,
        )?;
        self.failpoint.trigger_after_effect_intent();
        let mut execution = broker
            .execute_commitment(&commitment, &plan, &authorization)
            .await?;
        if execution.plan_hash != commitment.plan_hash {
            return Err(PaperDispatchError::BrokerPlanHashMismatch);
        }
        let mut actions = self.durable_order_actions(&input.commitment, &execution)?;
        self.resume_durable_order_actions(broker, input, &actions, &authorization, &mut execution)
            .await?;
        self.store.validate_daemon_lease(&input.lease, Utc::now())?;
        execution = reconcile_until_settled(
            &self.store,
            &input.lease,
            broker,
            &commitment,
            &execution,
            self.settlement_timeout,
        )
        .await?;
        if execution.plan_hash != commitment.plan_hash {
            return Err(PaperDispatchError::BrokerPlanHashMismatch);
        }
        let broker_receipts = execution
            .orders
            .iter()
            .map(|receipt| broker_receipt(receipt, &commitment, input.now))
            .collect::<PaperDispatchResult<Vec<_>>>()?;
        let reconciliation_runtime = ReconciliationRuntime::new(self.store.clone());
        actions = self.durable_order_actions(&input.commitment, &execution)?;
        let mut reconciliation = reconciliation_runtime.reconcile(&ReconciliationInput {
            permit: input.permit.clone(),
            commitment: input.commitment.clone(),
            reprices: actions.reprices.clone(),
            cancels: actions.cancels.clone(),
            broker_receipts,
            now: input.now,
        })?;
        let mut settled = self.reconciliation_is_settled(&reconciliation)?;
        // Extended day orders may remain live through the trade date's sessions.
        // A short polling timeout is not a reason to cancel an accepted order.
        // Regular-hours repricing/cancellation behavior remains unchanged.
        if !settled
            && !plan.orders.iter().any(|order| order.extended_hours)
            && self.has_stale_open_order(&execution, input.now)?
        {
            reconciliation_runtime.write_progress(
                &input.lease,
                &input.permit,
                &reconciliation,
                Utc::now(),
            )?;
            self.act_on_stale_orders(
                broker,
                input,
                &plan,
                &authorization,
                &reconciliation,
                &mut execution,
            )
            .await?;
            execution = reconcile_until_settled(
                &self.store,
                &input.lease,
                broker,
                &commitment,
                &execution,
                std::time::Duration::ZERO,
            )
            .await?;
            actions = self.durable_order_actions(&input.commitment, &execution)?;
            let broker_receipts = execution
                .orders
                .iter()
                .map(|receipt| broker_receipt(receipt, &commitment, input.now))
                .collect::<PaperDispatchResult<Vec<_>>>()?;
            reconciliation = reconciliation_runtime.reconcile(&ReconciliationInput {
                permit: input.permit.clone(),
                commitment: input.commitment.clone(),
                reprices: actions.reprices.clone(),
                cancels: actions.cancels.clone(),
                broker_receipts,
                now: input.now,
            })?;
            settled = self.reconciliation_is_settled(&reconciliation)?;
        }
        if settled {
            reconciliation_runtime.commit_with_effect(
                &input.lease,
                &input.permit,
                &reconciliation,
                &input.commitment,
                recovered,
                Utc::now(),
            )?;
        } else {
            reconciliation_runtime.write_progress(
                &input.lease,
                &input.permit,
                &reconciliation,
                Utc::now(),
            )?;
        }

        Ok(PaperDispatchOutput {
            commitment: commitment_artifact,
            execution,
            reconciliation,
            settled,
        })
    }

    fn reconciliation_is_settled(
        &self,
        output: &ReconciliationOutput,
    ) -> PaperDispatchResult<bool> {
        // 只读取 Reconciliation payload 的领域终态；调度层不以“收到响应”代替 Complete。
        let payload: akzio_domain::Reconciliation =
            serde_json::from_slice(&self.store.read_blob(&output.reconciliation.blob)?)?;
        Ok(payload.state == akzio_domain::ReconciliationState::Complete)
    }

    fn durable_order_actions(
        &self,
        commitment: &ArtifactRef,
        execution: &PaperExecution,
    ) -> PaperDispatchResult<DurableOrderActions> {
        // 以 commitment+asset 查询已持久化 action intent，并拒绝 broker 自行返回而未被
        // Store 记录的 replacement，保持外部副作用与内部 lineage 一一对应。
        let mut actions = DurableOrderActions::default();
        for receipt in &execution.orders {
            let asset = Asset::try_from(receipt.symbol.as_str())?;
            let reprice = self.store.reprice_for(commitment, asset)?;
            if receipt.reprice_count > 0 && reprice.is_none() {
                return Err(PaperDispatchError::ReplacementWithoutIntent(asset));
            }
            if let Some(reprice) = reprice {
                actions.reprices.push(artifact_ref(&reprice));
            }
            if let Some(cancel) = self.store.cancel_for(commitment, asset)? {
                actions.cancels.push(artifact_ref(&cancel));
            }
        }
        Ok(actions)
    }

    async fn resume_durable_order_actions<B: CommittedPaperBroker + ?Sized>(
        &self,
        broker: &B,
        input: &PaperDispatchInput,
        actions: &DurableOrderActions,
        authorization: &PaperSubmissionAuthorization,
        execution: &mut PaperExecution,
    ) -> PaperDispatchResult<()> {
        // 每个 reprice/cancel 都先 record effect intent，再调用 Broker，成功后 settle effect；
        // 已结算 intent 只恢复读取结果，未授权的 replacement 保留 pending 并继续风险降低路径。
        for reference in &actions.reprices {
            let artifact = self.load_expected(reference, ArtifactKind::ExecutionReprice)?;
            let intent: PaperReprice =
                serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
            let recovered = match self.store.record_paper_effect_intent(
                &input.lease,
                &input.permit,
                reference,
                Utc::now(),
            ) {
                Ok(recovered) => Some(recovered),
                Err(StoreError::PaperEffectAlreadySettled(id)) if id == reference.artifact_id => {
                    None
                }
                Err(error) => return Err(error.into()),
            };
            if let Some(recovered) = recovered {
                self.store.validate_daemon_lease(&input.lease, Utc::now())?;
                let receipt = match broker.replace_order(&intent, authorization).await {
                    Ok(receipt) => receipt,
                    // Keep the uncertain intent pending, preserve known receipts,
                    // and allow the risk-reducing cancellation path below.
                    Err(PaperError::SubmissionUnauthorized) => continue,
                    Err(error) => return Err(error.into()),
                };
                self.store.validate_daemon_lease(&input.lease, Utc::now())?;
                self.store.settle_paper_effect(
                    &input.lease,
                    &input.permit,
                    reference,
                    recovered,
                    Utc::now(),
                )?;
                replace_execution_receipt(execution, receipt)?;
            }
        }
        for reference in &actions.cancels {
            let artifact = self.load_expected(reference, ArtifactKind::ExecutionCancel)?;
            let intent: PaperCancel =
                serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
            let recovered = match self.store.record_paper_effect_intent(
                &input.lease,
                &input.permit,
                reference,
                Utc::now(),
            ) {
                Ok(recovered) => Some(recovered),
                Err(StoreError::PaperEffectAlreadySettled(id)) if id == reference.artifact_id => {
                    None
                }
                Err(error) => return Err(error.into()),
            };
            if let Some(recovered) = recovered {
                self.store.validate_daemon_lease(&input.lease, Utc::now())?;
                let receipt = broker.cancel_order(&intent).await?;
                self.store.validate_daemon_lease(&input.lease, Utc::now())?;
                self.store.settle_paper_effect(
                    &input.lease,
                    &input.permit,
                    reference,
                    recovered,
                    Utc::now(),
                )?;
                replace_execution_receipt(execution, receipt)?;
            }
        }
        Ok(())
    }

    fn has_stale_open_order(
        &self,
        execution: &PaperExecution,
        input_now: DateTime<Utc>,
    ) -> PaperDispatchResult<bool> {
        // 把 broker status 映射为可取消集合，再以 broker_updated_at 与 grace 比较；未知状态
        // 直接报错，不把未知订单当成 stale。
        let wall_now = Utc::now();
        let now = if input_now > wall_now {
            input_now
        } else {
            wall_now
        };
        execution.orders.iter().try_fold(false, |stale, receipt| {
            let state = receipt_state(&receipt.status)?;
            let cancelable = matches!(
                state,
                OrderReceiptState::Accepted
                    | OrderReceiptState::PartiallyFilled
                    | OrderReceiptState::DoneForDay
                    | OrderReceiptState::Stopped
                    | OrderReceiptState::Suspended
                    | OrderReceiptState::Calculated
            );
            let elapsed = now
                .signed_duration_since(receipt.broker_updated_at)
                .to_std()
                .unwrap_or_default();
            Ok(stale || (cancelable && elapsed >= self.settlement_action_grace))
        })
    }

    async fn act_on_stale_orders<B: CommittedPaperBroker + ?Sized>(
        &self,
        broker: &B,
        input: &PaperDispatchInput,
        plan: &ExecutionPlan,
        authorization: &PaperSubmissionAuthorization,
        reconciliation: &ReconciliationOutput,
        execution: &mut PaperExecution,
    ) -> PaperDispatchResult<()> {
        // 从当前 execution 筛出超过 grace 的 Regular 未终态订单，最多创建一次 repricing，
        // 后续只允许取消或继续对账；reprice 使用冻结 plan price，不追逐市场价格。
        let wall_now = Utc::now();
        let now = if input.now > wall_now {
            input.now
        } else {
            wall_now
        };
        let candidates = execution
            .orders
            .iter()
            .filter_map(|receipt| {
                let state = receipt_state(&receipt.status).ok()?;
                let cancelable = matches!(
                    state,
                    OrderReceiptState::Accepted
                        | OrderReceiptState::PartiallyFilled
                        | OrderReceiptState::DoneForDay
                        | OrderReceiptState::Stopped
                        | OrderReceiptState::Suspended
                        | OrderReceiptState::Calculated
                );
                let elapsed = now
                    .signed_duration_since(receipt.broker_updated_at)
                    .to_std()
                    .unwrap_or_default();
                (cancelable && elapsed >= self.settlement_action_grace).then(|| receipt.clone())
            })
            .collect::<Vec<_>>();

        for receipt in candidates {
            let asset = Asset::try_from(receipt.symbol.as_str())?;
            if self.store.cancel_for(&input.commitment, asset)?.is_some() {
                continue;
            }
            let prior_receipt = reconciliation
                .receipts
                .iter()
                .find_map(|artifact| {
                    let payload: OrderReceipt =
                        serde_json::from_slice(&self.store.read_blob(&artifact.blob).ok()?).ok()?;
                    (payload.asset == asset).then(|| artifact_ref(artifact))
                })
                .ok_or(PaperDispatchError::ReplacementWithoutIntent(asset))?;
            let state = receipt_state(&receipt.status)?;
            if matches!(
                state,
                OrderReceiptState::Accepted | OrderReceiptState::PartiallyFilled
            ) && receipt.reprice_count < MAX_PAPER_REPRICES_PER_ORDER
                && self.store.reprice_for(&input.commitment, asset)?.is_none()
                && authorization
                    .assert_current(&plan.plan_hash, Utc::now())
                    .is_ok()
            {
                let order = plan
                    .orders
                    .iter()
                    .find(|order| order.asset == asset)
                    .ok_or(PaperError::CommitmentClientOrderMismatch(asset))?;
                let payload = PaperReprice {
                    schema_version: akzio_domain::DOMAIN_SCHEMA_VERSION,
                    reprice_id: akzio_domain::PaperRepriceId::new(),
                    commitment: input.commitment.clone(),
                    prior_receipt: prior_receipt.clone(),
                    asset,
                    prior_client_order_id: receipt.client_order_id.clone(),
                    replacement_client_order_id: replacement_client_order_id(
                        &receipt.client_order_id,
                    ),
                    prior_broker_order_id: receipt.broker_order_id.clone(),
                    // The settlement policy is not an alpha or price-discovery policy.
                    // Re-submit the frozen plan price rather than chasing the market.
                    replacement_limit_price: order.limit_price,
                    created_at: now,
                };
                payload.validate()?;
                let artifact = Artifact::new(
                    ArtifactKind::ExecutionReprice,
                    self.store.stage_json(&payload)?,
                    "execution.reprice",
                    ArtifactLifecycle::Canonical,
                    crate::trusted_execution_provenance(&input.permit, now),
                    Some(input.permit.artifact_origin()),
                    vec![input.commitment.clone(), prior_receipt],
                    now,
                )?;
                let committed = self.store.commit_execution_reprice_intent(
                    &input.lease,
                    &input.permit,
                    &artifact,
                    now,
                )?;
                let reference = artifact_ref(&committed.artifact);
                let durable_payload: PaperReprice =
                    serde_json::from_slice(&self.store.read_blob(&committed.artifact.blob)?)?;
                let recovered = self.store.record_paper_effect_intent(
                    &input.lease,
                    &input.permit,
                    &reference,
                    Utc::now(),
                )?;
                self.store.validate_daemon_lease(&input.lease, Utc::now())?;
                let replacement = match broker.replace_order(&durable_payload, authorization).await
                {
                    Ok(receipt) => receipt,
                    Err(PaperError::SubmissionUnauthorized) => continue,
                    Err(error) => return Err(error.into()),
                };
                self.store.validate_daemon_lease(&input.lease, Utc::now())?;
                self.store.settle_paper_effect(
                    &input.lease,
                    &input.permit,
                    &reference,
                    recovered,
                    Utc::now(),
                )?;
                replace_execution_receipt(execution, replacement)?;
                continue;
            }
            let payload = PaperCancel {
                schema_version: akzio_domain::DOMAIN_SCHEMA_VERSION,
                cancel_id: PaperCancelId::new(),
                commitment: input.commitment.clone(),
                prior_receipt: prior_receipt.clone(),
                asset,
                client_order_id: receipt.client_order_id.clone(),
                broker_order_id: receipt.broker_order_id.clone(),
                reason: PaperCancelReason::SettlementTimeout,
                created_at: now,
            };
            payload.validate()?;
            let artifact = Artifact::new(
                ArtifactKind::ExecutionCancel,
                self.store.stage_json(&payload)?,
                "execution.cancel",
                ArtifactLifecycle::Canonical,
                crate::trusted_execution_provenance(&input.permit, now),
                Some(input.permit.artifact_origin()),
                vec![input.commitment.clone(), prior_receipt],
                now,
            )?;
            let committed = self.store.commit_execution_cancel_intent(
                &input.lease,
                &input.permit,
                &artifact,
                now,
            )?;
            let reference = artifact_ref(&committed.artifact);
            let durable_payload: PaperCancel =
                serde_json::from_slice(&self.store.read_blob(&committed.artifact.blob)?)?;
            let recovered = self.store.record_paper_effect_intent(
                &input.lease,
                &input.permit,
                &reference,
                Utc::now(),
            )?;
            self.store.validate_daemon_lease(&input.lease, Utc::now())?;
            let canceled = broker.cancel_order(&durable_payload).await?;
            self.store.validate_daemon_lease(&input.lease, Utc::now())?;
            self.store.settle_paper_effect(
                &input.lease,
                &input.permit,
                &reference,
                recovered,
                Utc::now(),
            )?;
            replace_execution_receipt(execution, canceled)?;
        }
        Ok(())
    }

    fn require_paper_run(&self, permit: &TaskWritePermit) -> PaperDispatchResult<()> {
        // Dispatch 只接受 Paper purpose；PositionPlan 到 Decision 结束，不得借此进入 Broker。
        let purpose = self.store.run_purpose(&permit.run_id)?;
        if purpose != RunPurpose::Paper {
            return Err(PaperDispatchError::NonPaperRun(purpose));
        }
        Ok(())
    }

    fn submission_authorization(
        &self,
        permit: &TaskWritePermit,
        plan: &ExecutionPlan,
    ) -> PaperDispatchResult<PaperSubmissionAuthorization> {
        // 从 permit/run 的冻结决策、approval 与执行快照构造临时发送窗口；窗口缺失时仍可
        // 读取既有订单，但不会授权新的 effect。
        let read = |reference: &ArtifactRef, kind| -> PaperDispatchResult<Vec<u8>> {
            let artifact = self.load_expected(reference, kind)?;
            Ok(self.store.read_blob(&artifact.blob)?)
        };
        let decision: akzio_domain::DecisionContext = serde_json::from_slice(&read(
            &plan.decision_context,
            ArtifactKind::DecisionContext,
        )?)?;
        decision.validate()?;
        if decision.run_id != permit.run_id {
            return Err(PaperDispatchError::ContextMismatch);
        }
        let account_artifact =
            self.load_expected(&plan.account_snapshot, ArtifactKind::NormalizedEvidence)?;
        let account = serde_json::from_slice(&self.store.read_blob(&account_artifact.blob)?)?;
        let mut components = Vec::new();
        if account_artifact.producer == "execution.snapshot.account" {
            for reference in account_artifact
                .source_refs
                .iter()
                .filter(|source| source.kind == ArtifactKind::NormalizedEvidence)
            {
                let artifact = self.load_expected(reference, ArtifactKind::NormalizedEvidence)?;
                let component = serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                components.push((artifact, component));
            }
        }
        let account_observations = submission_authorization::frozen_account_observations(
            &account_artifact,
            &account,
            &components,
        );
        let quotes = serde_json::from_slice(&read(
            &plan.quote_snapshot,
            ArtifactKind::NormalizedEvidence,
        )?)?;
        let clock = serde_json::from_slice(&read(
            &plan.market_clock_snapshot,
            ArtifactKind::NormalizedEvidence,
        )?)?;
        let approval_expiry =
            self.store
                .paper_approval_for_run(&permit.run_id)?
                .map(|(manifest, approval)| {
                    manifest
                        .expires_at
                        .min(approval.expires_at)
                        .min(approval.qualification.expires_at)
                });
        let mut authorization = PaperSubmissionAuthorization::from_frozen_sources(
            plan,
            &self.execution_policy,
            decision.validity.as_ref(),
            approval_expiry,
            &account,
            &quotes,
            &clock,
        )?;
        authorization
            .restrict_account_observations(account_observations.as_deref(), &self.execution_policy);
        Ok(authorization)
    }

    fn load_committed_plan(
        &self,
        permit: &TaskWritePermit,
        commitment_reference: &ArtifactRef,
    ) -> PaperDispatchResult<CommittedPlanContext> {
        // 读取 session slot 指向的唯一 commitment，再沿 ExecutionContext→ExecutionPlan
        // 闭包验证 run/session/plan hash；不接受调用方仅凭一个 ArtifactRef 拼装计划。
        let commitment_artifact =
            self.load_expected(commitment_reference, ArtifactKind::ExecutionCommitment)?;
        let commitment: PaperCommitment =
            serde_json::from_slice(&self.store.read_blob(&commitment_artifact.blob)?)?;
        commitment.validate()?;
        let slot = self
            .store
            .session_slot(&commitment.broker_session)?
            .ok_or(PaperDispatchError::CommitmentNotDurable)?;
        if slot.workflow.run.run_id != permit.run_id
            || slot.commitment_artifact_id.as_ref() != Some(&commitment_reference.artifact_id)
        {
            return Err(PaperDispatchError::CommitmentNotDurable);
        }
        if !commitment_artifact
            .source_refs
            .iter()
            .any(|source| source == &commitment.execution_context)
        {
            return Err(PaperDispatchError::CommitmentContextMissing);
        }

        let context_artifact = self.load_expected(
            &commitment.execution_context,
            ArtifactKind::ExecutionContext,
        )?;
        let context: ExecutionContext =
            serde_json::from_slice(&self.store.read_blob(&context_artifact.blob)?)?;
        context.validate()?;
        context.validate_complete_plan_closure()?;
        if context.run_id != permit.run_id
            || context.broker_session.as_deref() != Some(commitment.broker_session.as_str())
            || context.plan_hash.as_ref() != Some(&commitment.plan_hash)
        {
            return Err(PaperDispatchError::ContextMismatch);
        }
        let plan_reference = context
            .execution_plan
            .ok_or(PaperDispatchError::MissingAllocationPlan)?;
        if !context_artifact.source_refs.contains(&plan_reference) {
            return Err(PaperDispatchError::MissingAllocationPlan);
        }
        let plan_artifact = self.load_expected(&plan_reference, ArtifactKind::ExecutionPlan)?;
        let plan: ExecutionPlan =
            serde_json::from_slice(&self.store.read_blob(&plan_artifact.blob)?)?;
        plan.validate()?;
        if plan.plan_hash != commitment.plan_hash
            || plan.broker_session != commitment.broker_session
        {
            return Err(PaperDispatchError::PlanHashMismatch);
        }

        Ok(CommittedPlanContext {
            commitment_artifact,
            commitment,
            plan,
        })
    }

    fn load_expected(
        &self,
        reference: &ArtifactRef,
        expected: ArtifactKind,
    ) -> PaperDispatchResult<Artifact> {
        // 所有 dispatch 读取都通过 kind 双重检查，避免把其他 Artifact 当作执行载荷。
        let artifact = self.store.artifact(&reference.artifact_id)?;
        if reference.kind != expected || artifact.kind != expected {
            return Err(PaperDispatchError::WrongArtifactKind {
                expected,
                actual: artifact.kind,
            });
        }
        Ok(artifact)
    }

    fn ensure_unfrozen(&self) -> PaperDispatchResult<()> {
        // 全局 FreezeState 是发送前的最终 fail-closed 开关；冻结只阻断新副作用，恢复读取
        // 的语义由各 Broker 查询和对账路径保留。
        let Some(freeze_artifact) = self
            .store
            .latest_artifact_by_kind(ArtifactKind::FreezeState)?
        else {
            return Ok(());
        };
        let freeze: FreezeState =
            serde_json::from_slice(&self.store.read_blob(&freeze_artifact.blob)?)?;
        freeze.validate()?;
        if freeze.frozen {
            return Err(PaperDispatchError::Frozen);
        }
        Ok(())
    }
}

fn artifact_ref(artifact: &Artifact) -> ArtifactRef {
    // 从已加载 Artifact 生成不复制 blob 的类型化引用，用于 action/reconciliation lineage。
    ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: artifact.kind,
    }
}

fn replace_execution_receipt(
    execution: &mut PaperExecution,
    replacement: PaperOrderReceipt,
) -> PaperDispatchResult<()> {
    // 按 symbol 替换内存 execution 中的已知回执；资产必须能映射到 commitment，不能追加
    // 未声明的订单。
    let asset = Asset::try_from(replacement.symbol.as_str())?;
    let receipt = execution
        .orders
        .iter_mut()
        .find(|receipt| receipt.symbol == asset.symbol())
        .ok_or(PaperError::CommitmentClientOrderMismatch(asset))?;
    *receipt = replacement;
    Ok(())
}

async fn reconcile_until_settled<B: CommittedPaperBroker + ?Sized>(
    store: &Store,
    lease: &DaemonLease,
    broker: &B,
    commitment: &PaperCommitment,
    submitted: &PaperExecution,
    settlement_timeout: std::time::Duration,
) -> PaperDispatchResult<PaperExecution> {
    // 在 settlement deadline 内以固定间隔刷新；每轮先校验 daemon lease，超时只返回最新
    // 观察值，让上层写 progress，而不是合成终态。
    let deadline = tokio::time::Instant::now() + settlement_timeout;
    loop {
        store.validate_daemon_lease(lease, Utc::now())?;
        let execution = broker.reconcile_commitment(commitment, submitted).await?;
        if execution_is_settled(commitment, &execution)? || tokio::time::Instant::now() >= deadline
        {
            return Ok(execution);
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

pub(super) fn execution_is_settled(
    commitment: &PaperCommitment,
    execution: &PaperExecution,
) -> PaperDispatchResult<bool> {
    // 只有所有 commitment 资产各有唯一原/替换回执，且每个状态都是无 successor 的终态，
    // 才返回 true；缺单、重复、错误 client ID 或 plan hash 都分别保持 false/错误。
    let mut assets = std::collections::BTreeSet::new();
    for receipt in &execution.orders {
        let asset = Asset::try_from(receipt.symbol.as_str())?;
        let original = commitment
            .client_order_ids
            .get(&asset)
            .ok_or(PaperError::CommitmentClientOrderMismatch(asset))?;
        if !assets.insert(asset)
            || (receipt.client_order_id != *original
                && receipt.client_order_id != replacement_client_order_id(original))
        {
            return Err(PaperError::CommitmentClientOrderMismatch(asset).into());
        }
    }
    if execution.plan_hash != commitment.plan_hash {
        return Err(PaperDispatchError::BrokerPlanHashMismatch);
    }
    if assets.len() != commitment.client_order_ids.len() {
        return Ok(false);
    }
    execution.orders.iter().try_fold(true, |settled, receipt| {
        Ok(settled && receipt_state(&receipt.status)?.is_final_without_successor())
    })
}

fn broker_receipt(
    receipt: &PaperOrderReceipt,
    commitment: &PaperCommitment,
    observed_at: DateTime<Utc>,
) -> PaperDispatchResult<OrderReceipt> {
    // 将 adapter receipt 转成领域 OrderReceipt 并绑定 commitment plan hash；状态解析失败
    // 会阻断对账，而不是默认为 accepted。
    let asset = Asset::try_from(receipt.symbol.as_str())?;
    Ok(OrderReceipt {
        plan_hash: commitment.plan_hash.clone(),
        asset,
        client_order_id: receipt.client_order_id.clone(),
        broker_order_id: receipt.broker_order_id.clone(),
        state: receipt_state(&receipt.status)?,
        requested_quantity_micros: receipt.requested_quantity_micros,
        filled_quantity_micros: receipt.filled_quantity_micros,
        remaining_quantity_micros: receipt.remaining_quantity_micros,
        average_fill_price: receipt.average_fill_price,
        broker_updated_at: receipt.broker_updated_at,
        reason: receipt.reason.clone(),
        observed_at,
    })
}

#[cfg(test)]
mod settlement_tests {
    use super::*;

    #[test]
    fn terminal_subset_does_not_settle_full_commitment() {
        // 验证“部分订单已终态”仍不能关闭包含其他资产的完整 Commitment。
        let hash = ContentHash::of_bytes(b"commitment plan");
        let commitment = PaperCommitment {
            commitment_id: akzio_domain::PaperCommitmentId::new(),
            execution_context: ArtifactRef {
                artifact_id: akzio_domain::ArtifactId(ContentHash::of_bytes(b"context")),
                kind: ArtifactKind::ExecutionContext,
            },
            plan_hash: hash.clone(),
            broker_session: "2026-09-09".into(),
            client_order_ids: [(Asset::Qqq, "qqq".into()), (Asset::Soxx, "soxx".into())].into(),
            created_at: Utc::now(),
        };
        let receipt = |asset: Asset, id: &str| PaperOrderReceipt {
            client_order_id: id.into(),
            broker_order_id: format!("broker-{id}"),
            symbol: asset.symbol().into(),
            status: "filled".into(),
            requested_quantity_micros: 1_000_000,
            filled_quantity_micros: 1_000_000,
            remaining_quantity_micros: 0,
            average_fill_price: Some(MoneyMicros(10_000_000)),
            broker_updated_at: Utc::now(),
            reason: None,
            reused: true,
            reprice_count: 0,
        };
        let mut execution = PaperExecution {
            plan_hash: hash,
            orders: vec![],
        };
        assert!(!execution_is_settled(&commitment, &execution).unwrap());
        execution.orders.push(receipt(Asset::Qqq, "qqq"));
        assert!(!execution_is_settled(&commitment, &execution).unwrap());
        execution.orders.push(receipt(Asset::Soxx, "soxx"));
        assert!(execution_is_settled(&commitment, &execution).unwrap());
        execution.orders[1] = receipt(Asset::Qqq, "qqq");
        assert!(execution_is_settled(&commitment, &execution).is_err());
        execution.orders[1] = receipt(Asset::Soxx, "unrelated-id");
        assert!(execution_is_settled(&commitment, &execution).is_err());
    }
}
