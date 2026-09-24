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
        // 只读取诊断环境变量并把 owned String 转为临时 Option<&str>；from_value 不保存该借用，
        // 环境变量缺失/非 Unicode 时按关闭处理。
        Self::from_value(
            std::env::var("AKZIO_DIAGNOSTIC_CRASH_AFTER_EFFECT_INTENT")
                .ok()
                .as_deref(),
        )
    }

    fn from_value(value: Option<&str>) -> Self {
        // value 只借用到 match 结束；仅精确字符串 "1" 启用，其余 Some/None 均保持关闭。
        match value {
            Some("1") => Self::ExitAfterEffectIntent,
            _ => Self::Disabled,
        }
    }

    fn trigger_after_effect_intent(self) {
        // self 是 Copy 枚举值；命中时进程直接 exit(86)，不会运行正常 Rust 栈展开/Drop，
        // 故只允许在受控诊断配置下验证已落盘 intent 的崩溃恢复。
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
        // Store 按值移入并由 runtime 持有；设置默认执行策略与观察时限，不执行 Store/Broker I/O。
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
        // builder 消费 self 并返回新拥有的 Self，便于链式配置；policy 也按值移入，不共享可变副本。
        self.execution_policy = policy;
        self
    }

    pub fn with_settlement_timeout(mut self, settlement_timeout: std::time::Duration) -> Self {
        // builder 按值接收 Duration 并消费 self；只改本地观察预算，超时不会把 accepted/partial 改成 filled。
        self.settlement_timeout = settlement_timeout;
        self
    }

    pub fn with_settlement_action_grace(
        mut self,
        settlement_action_grace: std::time::Duration,
    ) -> Self {
        // builder 按值替换 action grace；它只决定 Regular 未结订单何时进入有限处置，
        // 不影响 Extended-hours 订单的长生命周期。
        self.settlement_action_grace = settlement_action_grace;
        self
    }

    pub fn with_failpoint(mut self, failpoint: PaperDispatchFailpoint) -> Self {
        // builder 消费/返回 Self，只替换诊断枚举；真正触发会在 dispatch 已记录 effect intent 后 exit。
        self.failpoint = failpoint;
        self
    }

    pub async fn dispatch<B: CommittedPaperBroker + ?Sized>(
        &self,
        broker: &B,
        input: &PaperDispatchInput,
    ) -> PaperDispatchResult<PaperDispatchOutput> {
        // 主流程：校验权限与 durable plan→记录 effect intent→执行/恢复 broker commitment→
        // 持续对账→必要时恢复 durable reprice/cancel→生成 Reconciliation。lease 在进入
        // Broker 阶段及若干持久化/对账边界复查；并非每个内部 GET/POST 后都原子续租。
        // 失败/取消保留已写的 effect intent 或中间进度，不承诺一定生成 Reconciliation。
        // B 是编译期泛型 trait bound，`?Sized` 允许传递 dyn broker；broker/input/self 都借用
        // 到异步 Future 完成。调用本方法只创建 Future，调用者 await 才会启动后续 I/O。
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
        // 先从 durable commitment 闭包加载拥有的三项数据，再根据冻结 Decision/approval/
        // snapshots 构造临时授权；授权缺 window 时仍可恢复读取但无法 POST/PATCH。
        let CommittedPlanContext {
            commitment_artifact,
            commitment,
            plan,
        } = self.load_committed_plan(&input.permit, &input.commitment)?;
        let authorization = self.submission_authorization(&input.permit, &plan)?;

        self.ensure_unfrozen()?;
        self.store.validate_daemon_lease(&input.lease, Utc::now())?;
        self.store.validate_task_permit(&input.permit)?;
        // 先在 Store 持久化 effect intent，再允许 Broker 写入；两个系统不是一个原子事务。
        // 若进程在此后崩溃/取消，Future 局部值会 Drop，但 Store intent 保留供下次恢复。
        let recovered = self.store.record_paper_effect_intent(
            &input.lease,
            &input.permit,
            &input.commitment,
            input.now,
        )?;
        self.failpoint.trigger_after_effect_intent();
        // Broker Future 在此 await 时执行；POST/查询发生后任何错误都向上返回，已记录的
        // intent 不回滚。后续 dispatch 通过同一 commitment/client ID 查询实际外部效果。
        let mut execution = broker
            .execute_commitment(&commitment, &plan, &authorization)
            .await?;
        if execution.plan_hash != commitment.plan_hash {
            return Err(PaperDispatchError::BrokerPlanHashMismatch);
        }
        // 执行回执必须与 plan_hash 相同；再从 Store 重建已有 reprice/cancel intents，并恢复它们。
        let mut actions = self.durable_order_actions(&input.commitment, &execution)?;
        self.resume_durable_order_actions(broker, input, &actions, &authorization, &mut execution)
            .await?;
        // 完成可能阻塞的 Broker/action 阶段后重新校验 lease，再开始轮询对账；
        // lease 失效时拒绝提交新的持久化结果。
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
        // 将 adapter receipt 映射成领域 OrderReceipt；未知状态/资产/client ID 错误会阻断
        // reconciliation，不把 broker accepted 解释为成交完成。
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
        // settled 由领域 ReconciliationState::Complete 决定，而不是 dispatch 返回 Ok、
        // HTTP accepted 或短轮询超时。
        let mut settled = self.reconciliation_is_settled(&reconciliation)?;
        // Extended day orders may remain live through the trade date's sessions.
        // A short polling timeout is not a reason to cancel an accepted order.
        // Regular-hours repricing/cancellation behavior remains unchanged.
        if !settled
            && !plan.orders.iter().any(|order| order.extended_hours)
            && self.has_stale_open_order(&execution, input.now)?
        {
            // 在任何 stale action 外部效果前先保存当前 reconciliation progress；只对非 extended
            // plan 走原 Regular 超时处置。写 progress 成功不表示完成，也不删除 effect intent。
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
            // action 后只做零时长的即时刷新，避免在同一 stale 分支再次等待；结果仍可能未 settled。
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
        // Complete 才把 progress 和原 Paper effect 一起结算；否则只写可恢复 progress。
        // 任一 Store 错误经 `?` 返回，不返回伪造 settled=true。
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
        // 只从输出 Artifact 的 blob 解码领域 Reconciliation；不访问 Broker/写 Store。
        // `Ok(false)` 是合法未完成状态，JSON/Store 错误仍是 Err。
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
        // commitment 和 execution 均只读借用；每条 receipt 转为受控 Asset 后再按
        // commitment+asset 查询 durable intents，BTree 派生引用列表只含 Store 已持久化项。
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
        // 异步 Future 顺序处理已有 intents；reference/authorization/execution 都借用传入对象，
        // await 时不持有本地 Mutex。每个外部调用前后都以 lease fence Store 写入。
        for reference in &actions.reprices {
            let artifact = self.load_expected(reference, ArtifactKind::ExecutionReprice)?;
            let intent: PaperReprice =
                serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
            // effect 已结算时 Store 返回明确 sentinel，None 表示只恢复后续 receipt，不再调用 Broker；
            // 新 intent 则 Some(recovered) 允许执行尚未确认的外部替换。
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
                // 授权过期只跳过 reprice 并继续下一类风险降低 cancel；intent 保持未结算供恢复。
                let receipt = match broker.replace_order(&intent, authorization).await {
                    Ok(receipt) => receipt,
                    // Keep the uncertain intent pending, preserve known receipts,
                    // and allow the risk-reducing cancellation path below.
                    Err(PaperError::SubmissionUnauthorized) => continue,
                    Err(error) => return Err(error.into()),
                };
                self.store.validate_daemon_lease(&input.lease, Utc::now())?;
                // 外部替换回执取得且 lease 仍有效后才 settle intent，再把回执替换进本地 execution；
                // 如果 settle 前崩溃，下一轮依靠 Store intent 与 broker client ID 对账。
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
        // Cancel 按相同 intent→lease→Broker→lease→settle 顺序处理；取消是风险降低路径，
        // 不依赖仍有效的“新增发送”授权窗口。
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
        // 以 input time 与实际 wall clock 中较晚者评估 grace，避免过期输入把等待时间倒退。
        let wall_now = Utc::now();
        let now = if input_now > wall_now {
            input_now
        } else {
            wall_now
        };
        // try_fold 对每条 receipt 解析状态；未知状态的 `?` 返回 Err 整批不能判 stale。
        // elapsed 的负 duration 无法转 std::time::Duration，unwrap_or_default 将其按零等待处理。
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
        // 此 async Future 由 dispatch await 驱动；候选快照只含可取消且超过 grace 的 cloned
        // receipts，随后按资产检查已有取消/改价 intent，执行一项后更新本地 execution。
        let wall_now = Utc::now();
        let now = if input.now > wall_now {
            input.now
        } else {
            wall_now
        };
        // filter_map 对状态和时限进行筛选，collect 才实际消费 iterator；这里的 .ok()? 会
        // 略过未知状态，但调用方前置 has_stale_open_order 已用严格解析检查过整组状态。
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

        // candidates 拥有 cloned receipt，不再借用 execution.orders，因此循环中可通过
        // replace_execution_receipt 可变更新原 execution。
        for receipt in candidates {
            let asset = Asset::try_from(receipt.symbol.as_str())?;
            // 已存在 cancel intent 的订单不再产生新的 reprice/cancel 副作用。
            if self.store.cancel_for(&input.commitment, asset)?.is_some() {
                continue;
            }
            // 找到 Reconciliation 已保存的该资产 prior receipt 作为新 intent 的 lineage；
            // 失败解析项被过滤，最终缺失时明确报 ReplacementWithoutIntent。
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
            // 仅 Accepted/PartiallyFilled 可尝试一次 frozen-plan-price reprice；其它可取消状态、
            // 超过次数、已有 intent 或授权失效都会落入下方 cancel 路径。
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
                // 先构造并 validate intent Artifact，再由 Store fenced 方法持久化；
                // client ID 固定为 r1、价格复制冻结 plan，不根据实时行情重新计算。
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
                // durable reprice intent 保存后才登记 effect intent、校验 lease 并调用 Broker PATCH；
                // 外部 PATCH 与随后 settle 是分开的步骤，崩溃后由 successor 查询恢复。
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
            // 若未执行改价，则创建受控 cancel intent；持久化成功之前不会请求 Broker DELETE。
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
            // Cancel effect 同样先持久化 intent；await 返回已知回执后再次 fence 并 settle。
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
        // permit 只读借用，Store purpose 查询失败经 `?` 返回；PositionPlan 即使已完成 Decision
        // 也在此拒绝，不进入 session slot 或 Broker 路径。
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
        // 从 permit/run 的冻结决策、approval 与执行快照构造临时发送窗口；窗口缺失时
        // 不能新建订单或替换订单；已有订单的读取和受控取消分别走恢复/降险分支。
        // 局部闭包共享借用 self，按传入 ArtifactRef/kind 先核对类型再返回 owned blob bytes；
        // 读取错误从闭包 Result 经外层 `?` 传播。
        let read = |reference: &ArtifactRef, kind| -> PaperDispatchResult<Vec<u8>> {
            let artifact = self.load_expected(reference, kind)?;
            Ok(self.store.read_blob(&artifact.blob)?)
        };
        // 首先读取并校验冻结 DecisionContext 的 Run 身份；随后同一 helper 读取 plan 记录的
        // account/quote/clock 快照，不使用刷新后的新值替代原计划来源。
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
        // 多来源账户快照会逐个解码 normalized components，并要求 component 观察时间完整；
        // 未能证明时返回 None，restrict_account_observations 会撤销发送窗口。
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
        // expiry 同时受 approval manifest、approval 本身和 qualification 期限限制；
        // 缺审批得到 None，from_frozen_sources 会保留只读恢复能力但不给发送授权。
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
        // 输入只是候选引用；实际持久身份由 session slot 再确认。每次 Store read/serde/domain
        // 错误都经 `?` 返回，不从其他 Run/Artifact 拼凑可执行计划。
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

        // 沿 commitment 保留的 context 引用读取，检查 Run/session/plan hash，再由 context
        // 声明的 ExecutionPlan 引用读取完整 payload。
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
        // 取走 context.execution_plan 这个 Option（context 后续不再使用）；缺失或未列入
        // source_refs 都返回 MissingAllocationPlan。
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
        // reference 只共享借用，返回的 Store Artifact 由调用者拥有并负责继续读 blob。
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
        // 全局 FreezeState 是本次 dispatch 的前置开关；冻结时当前方法在 Broker 查询
        // 之前就返回 Frozen。既有外部效果仍在 Broker/Store，但此路径此刻不继续对账。
        // None 表示 Store 当前无 FreezeState；Some 时读取并 Domain validate，true 才拒绝。
        // 此方法只读状态，不清除或改写冻结记录。
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
    // 只 clone 内容寻址 ID 并复制 kind，返回类型化引用，不复制/读取 blob 正文。
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
    // replacement 按值移入，随后通过 symbol 映射资产并在 Vec 中查找唯一既有 receipt；
    // 只替换本地内存结果，不单独持久化或调用 Broker。
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
    // 每次调用产生由上层 await 驱动的 Future；B 可为 trait object。deadline 用单调时钟，
    // timeout=0 时仍先执行一轮 lease 校验和 Broker reconcile，再判断是否到期。
    let deadline = tokio::time::Instant::now() + settlement_timeout;
    loop {
        store.validate_daemon_lease(lease, Utc::now())?;
        // 当前 iteration 顺序为校验 lease→await 只读 reconcile→检查终态/截止时间；
        // broker 或 lease 错误立即返回，不在本函数持锁等待。
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
    // 此轮询停止谓词只要求 commitment 每个资产有唯一原/替换回执且状态在终态集合；
    // 它本身不查询 Store 中是否仍有未观察到的 durable reprice successor。
    // 缺单返回 false，重复/错误 client ID 或 plan hash 返回 Err；完整 settlement 仍由
    // 后续 ReconciliationState 再核对 successor 闭包。
    // execution/commitment 均共享借用；逐条验证回执 asset/client ID 唯一性，未知状态会在
    // 后面的 try_fold 通过 Result 返回 Err，而不是 false。
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
    // 只有回执资产数覆盖整个 commitment，且每个状态本身为终态才为 true；
    // `try_fold` 累积 bool 并传播 receipt_state 错误。
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
    // receipt/commitment 只读借用；symbol 与状态都通过封闭解析，字段复制进新领域值，
    // 因而 adapter 原回执仍可供调用者返回/审计。
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

// 仅测试构建编译：用局部值验证完整 Commitment 的覆盖条件，不访问 Store 或 Broker。
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
        // 该闭包不捕获外部状态；每次调用都根据参数新建 owned receipt，id 仅在构造字段时复制。
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
        // 逐步补齐 commitment 中的两个资产；重复资产或无关 ID 必须是错误，而不是 settled。
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
