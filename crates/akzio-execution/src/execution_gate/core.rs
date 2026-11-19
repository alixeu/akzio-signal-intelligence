// 文件导读：该文本通过 `include!` 并入 ExecutionRuntime，实现构造、主评估和 fenced commit。
// 入口 evaluate 从持久化 DecisionContext 与执行快照产生可选 ExecutionPlan、ExecutionContext
// 和 ExecutionVerdict；快照/Gate blocker 会累积为 NoOrder。此处只 stage 结果，commit 才将
// Artifact 与 task 终态一起交给 Store；PaperCommitment 只持久化承诺。上游快照可有
// 只读 Broker/行情 I/O；订单写请求与对账由后续 Dispatch/Reconcile 路径负责。
struct AllocatedPlanRisk {
    before_target: akzio_domain::TargetPortfolio,
    full_fill_target: akzio_domain::TargetPortfolio,
    projected_drawdown_ppm: u32,
    risk_increasing: bool,
    exceeds_frozen_limits: bool,
}

#[derive(Clone, Copy, Default)]
struct FillWeightBounds {
    lower: u32,
    upper: u32,
}

impl FillWeightBounds {
    fn enclose(numerator: i128, denominator: i128) -> Option<Self> {
        if numerator < 0 || denominator <= 0 {
            return None;
        }
        let lower = u32::try_from(numerator / denominator).ok()?;
        let upper = lower.checked_add(u32::from(numerator % denominator != 0))?;
        (upper <= akzio_domain::WeightPpm::SCALE).then_some(Self { lower, upper })
    }
}

#[derive(Clone, Copy, Default)]
struct AssetFillWeights {
    before: FillWeightBounds,
    after: FillWeightBounds,
    sold: FillWeightBounds,
}

impl ExecutionRuntime {
    // Store 按值移入 runtime 并由其持有；策略也在构造期验证，失败时返回 Gate 错误，
    // 不会留下半初始化的 runtime 或触发 Store/Broker I/O。
    pub fn new(
        store: Store,
        execution_policy: ExecutionPolicy,
        gate_policy: ExecutionGatePolicy,
    ) -> ExecutionGateResult<Self> {
        // 构造时同时冻结 allocation policy、Gate policy 和 pre-trade policy；任何非法配置
        // 在真正读取执行快照前失败，避免运行中途出现一半旧限制、一半新限制。
        let allocation = AllocationRuntime::new(execution_policy)
            .map_err(|_| ExecutionGateError::Integrity("execution policy"))?;
        gate_policy.validate()?;
        let pretrade_safety = PreTradeSafetyPolicy {
            capacity: gate_policy.capacity.clone(),
            compliance: gate_policy.compliance.clone(),
        };
        Ok(Self {
            store,
            startup_decision_policy_hash: None,
            allocation,
            gate_policy,
            pretrade_safety,
        })
    }

    pub fn with_startup_decision_policy_hash(mut self, policy_hash: Option<ContentHash>) -> Self {
        self.startup_decision_policy_hash = policy_hash;
        self
    }

    // 只返回执行限额的共享引用；调用者可读取但不能通过它更改已构造的 runtime 策略。
    pub fn execution_policy(&self) -> &ExecutionPolicy {
        // 只读暴露订单计算上限，调用方不能通过返回值改变 runtime。
        self.allocation.policy()
    }

    // 二次 Gate 策略同样只读借出，避免评估过程中与配置写入并发。
    pub fn gate_policy(&self) -> &ExecutionGatePolicy {
        // 只读暴露二次风控策略，Mandate/Capacity/Compliance 仍由 evaluate 统一组合。
        &self.gate_policy
    }

    pub fn evaluate(&self, input: &ExecutionGateInput) -> ExecutionGateResult<ExecutionGateOutput> {
        // 主流程按“输入 Artifact→来源/时效校验→收集 blockers→条件性分配→安全/mandate
        // 再核验→持久化 context/verdict”推进。即使 blocker 阻断 plan，仍生成完整的
        // ExecutionContext/NoOrder，保留失败原因和后续 Outcome lineage；这里绝不发单。
        // `input` 由调用者共享借用，Store 查询和解码都不消费输入引用。这里的 `?`
        // 将 Store/Domain/JSON 错误返回给当前 task；与可解释的安全 blocker 不同，
        // 结构损坏或持久化读取失败会中止本次 evaluate，不生成 NoOrder Artifact。
        self.validate_input(input)?;
        let purpose = self.store.run_purpose(&input.permit.run_id)?;
        let decision_artifact =
            self.load_expected(&input.decision_context, ArtifactKind::DecisionContext)?;
        let decision: DecisionContext = self.read_payload(&decision_artifact)?;
        decision.validate()?;
        if decision.run_id != input.permit.run_id {
            return Err(ExecutionGateError::DecisionRunMismatch);
        }
        self.validate_decision_provenance(&decision_artifact, &decision)?;
        let decision_process_quality = decision
            .investment_logic
            .as_ref()
            .map(|trace| trace.quality.clone());
        let decision_valid_at_execution = decision
            .validity
            .as_ref()
            .is_some_and(|validity| validity.is_valid_at(input.now));

        // 从 Decision 自带 blocker 起步，再把“此刻已过期/缺 provenance/有重大冲突”
        // 作为独立原因加入有序集合；集合去重但保留所有不同阻断原因。
        let mut blockers = decision
            .hard_blockers
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if !decision_valid_at_execution {
            blockers.insert(HardBlocker::StaleDecision);
        }
        if decision.horizon_trace.is_none() || decision.investment_logic.is_none() {
            blockers.insert(HardBlocker::InvalidProvenance);
        }
        if !decision.material_conflicts.is_empty() {
            blockers.insert(HardBlocker::MaterialConflict);
        }
        // 执行 Gate 只为正式 Paper Run 产生可执行资格。PositionPlan 即使持有 Decision
        // 也不会进入 Paper 执行；Approval 的身份、资格时间和 mandate hash 均再次核验。
        if purpose != RunPurpose::Paper {
            blockers.insert(HardBlocker::NonCanonicalRun);
        } else if let Some((manifest, approval)) =
            self.store.paper_approval_for_run(&input.permit.run_id)?
        {
            let qualification_key = manifest.runtime_identity().qualification_key()?;
            let approved_policy = Some(&manifest.decision_policy_hash);
            if manifest.decision_policy_hash != decision.decision_policy_hash
                || self.startup_decision_policy_hash.as_ref() != approved_policy
                || self.store.active_decision_policy_hash()?.as_ref() != approved_policy
                || !ModelQualificationGate::permits(
                    &qualification_key,
                    &approval.qualification,
                    input.now,
                )
            {
                blockers.insert(HardBlocker::UnqualifiedRuntime);
            }
            if approval.mandate.identity_hash() != self.gate_policy.mandate.identity_hash() {
                blockers.insert(HardBlocker::MandateViolation);
            }
        } else {
            blockers.insert(HardBlocker::UnqualifiedRuntime);
        }
        let frozen = self.frozen()?;
        if frozen {
            blockers.insert(HardBlocker::Frozen);
        }

        // 三种快照分别加载；缺失时 helper 记录 blocker 并返回 None，坏 Artifact/JSON
        // 则以 Err 提前退出。报价刷新时的解析错误额外保留 InvalidQuote 语义。
        let account = self.load_account(input, &mut blockers)?;
        let quotes =
            self.load_quotes(input, &mut blockers, input.quote_validation_error.is_some())?;
        if input.quote_validation_error.is_some() {
            blockers.insert(HardBlocker::InvalidQuote);
        }
        let clock = self.load_clock(input, &mut blockers)?;
        self.derive_snapshot_blockers(
            account.as_ref().map(|(_, payload)| payload),
            quotes.as_ref().map(|(_, payload)| payload),
            clock.as_ref().map(|(_, payload)| payload),
            input.now,
            &mut blockers,
        );

        // 只有当前 blocker 集为空才尝试 allocation，因此缺审批、陈旧快照等不会产生 plan。
        // 后续 Gate 仍可能在 plan 已计算后追加 blocker；此时 plan 会作为审计输入保留，
        // 但最终 Verdict 是 NoOrder，不会因此直接调用 Broker。
        let mut plan_payload = None;
        let mut mandate_assessment = None;
        let mut pretrade_safety = None;
        if blockers.is_empty() {
            let (_, account_payload) = account
                .as_ref()
                .ok_or(ExecutionGateError::Integrity("account snapshot closure"))?;
            let (_, quote_payload) = quotes
                .as_ref()
                .ok_or(ExecutionGateError::Integrity("quote snapshot closure"))?;
            let (_, clock_payload) = clock
                .as_ref()
                .ok_or(ExecutionGateError::Integrity("clock snapshot closure"))?;
            // Paper 订单额度来自该 Run 绑定的 approval；其他 purpose 不走执行授权，
            // 这里的分支仅保留非 Paper 的策略上限值，不会覆盖上面的 NonCanonical blocker。
            let maximum_total_notional = if purpose == RunPurpose::Paper {
                self.store
                    .paper_approval_for_run(&input.permit.run_id)?
                    .map_or(MoneyMicros::ZERO, |(manifest, _)| manifest.maximum_notional)
            } else {
                self.allocation.policy().max_new_notional
            };
            // Keep the frozen input available for checking the achieved plan.
            let allocation_input = AllocationInput {
                decision_context_ref: input.decision_context.clone(),
                decision_context: decision,
                account_snapshot_ref: input.account_snapshot.clone().expect("checked above"),
                account: account_payload.clone(),
                quote_snapshot_ref: input.quote_snapshot.clone().expect("checked above"),
                quotes: quote_payload.clone(),
                market_clock_snapshot_ref: input
                    .market_clock_snapshot
                    .clone()
                    .expect("checked above"),
                clock: clock_payload.clone(),
                now: input.now,
            };
            let allocation = self
                .allocation
                .allocate_with_limit(&allocation_input, maximum_total_notional);
            match allocation {
                Ok(plan) => {
                    plan.validate()?;
                    // Overnight 除一般报价校验外，还要求 overnight/BOATS feed 与本 session
                    // 允许的资产范围匹配；这里追加 blocker 而不抹掉 plan 或改写快照。
                    if clock_payload.trading_session() == akzio_domain::TradingSession::Overnight {
                        if !matches!(quote_payload.feed.as_deref(), Some("boats" | "overnight")) {
                            blockers.insert(HardBlocker::InvalidQuote);
                        }
                        if clock_payload.session.as_ref().is_none_or(|session| {
                            plan.orders
                                .iter()
                                .any(|order| !session.overnight_assets.contains(&order.asset))
                        }) {
                            blockers.insert(HardBlocker::MarketClosed);
                        }
                    }
                    (mandate_assessment, pretrade_safety) = self.assess_allocated_plan(
                        &allocation_input,
                        &plan,
                        input.pretrade_safety.as_ref(),
                        &mut blockers,
                    )?;
                    plan_payload = Some(plan);
                }
                // 业务性 allocation 错误被映射为稳定 HardBlocker，仍可输出 NoOrder；
                // 只有 Store/序列化/领域结构等错误经 `?` 才会中止整个方法。
                Err(error) => self.allocation_blockers(error, &mut blockers),
            }
        }

        // Artifact 只在本地构造并 stage；`transpose` 把 Option<Result<_>> 转成
        // Result<Option<_>>，所以 plan 不存在时是 Ok(None)，stage 失败则传播 Err。
        let execution_plan = plan_payload
            .as_ref()
            .map(|plan| {
                self.artifact(
                    ArtifactKind::ExecutionPlan,
                    "execution.plan",
                    plan,
                    vec![
                        plan.decision_context.clone(),
                        plan.account_snapshot.clone(),
                        plan.quote_snapshot.clone(),
                        plan.market_clock_snapshot.clone(),
                    ],
                    input,
                )
            })
            .transpose()?;
        let execution_plan_ref = execution_plan.as_ref().map(artifact_ref);
        let portfolio_consistent = plan_payload.is_some()
            && !blockers.contains(&HardBlocker::FactorLimit)
            && !blockers.contains(&HardBlocker::PairExposureLimit)
            && !blockers.contains(&HardBlocker::TurnoverLimit);
        let action_feasible = plan_payload.is_some()
            && input.account_snapshot.is_some()
            && input.quote_snapshot.is_some()
            && input.market_clock_snapshot.is_some()
            && decision_valid_at_execution
            && pretrade_safety
                .as_ref()
                .is_some_and(|assessment| assessment.permits_execution)
            && blockers.is_empty();
        // `and_then` 对 Option 继续计算：任一阶段质量或 mandate 缺失都会得到 None；
        // closure 中的 `?` 退出 closure（不是 evaluate），结果不会被伪造为 0 分质量。
        let final_process_quality = decision_process_quality.and_then(|quality| {
            let mandate = mandate_assessment.as_ref()?;
            quality.execution_finalized(
                if mandate.permitted() {
                    akzio_domain::WeightPpm::SCALE
                } else {
                    0
                },
                if portfolio_consistent {
                    akzio_domain::WeightPpm::SCALE
                } else {
                    0
                },
                if action_feasible {
                    akzio_domain::WeightPpm::SCALE
                } else {
                    0
                },
            )
        });

        // Context 保存输入 ArtifactRef、经 Gate 派生的 plan/exposure 与审查结果；没有
        // plan 的 NoOrder 仍可带 Decision/snapshot lineage 供后续 Outcome 追踪。
        let execution_context_payload = ExecutionContext {
            schema_version: akzio_domain::DOMAIN_SCHEMA_VERSION,
            run_id: input.permit.run_id.clone(),
            decision_context: input.decision_context.clone(),
            account_snapshot: input.account_snapshot.clone(),
            quote_snapshot: input.quote_snapshot.clone(),
            market_clock_snapshot: input.market_clock_snapshot.clone(),
            execution_plan: execution_plan_ref.clone(),
            factor_exposure: plan_payload
                .as_ref()
                .map(|plan| plan.factor_exposure.clone()),
            turnover_ppm: plan_payload.as_ref().map(|plan| plan.turnover_ppm),
            plan_hash: plan_payload.as_ref().map(|plan| plan.plan_hash.clone()),
            broker_session: plan_payload
                .as_ref()
                .map(|plan| plan.broker_session.clone()),
            mandate_assessment,
            pretrade_safety,
            final_process_quality,
            frozen,
            created_at: input.now,
        };
        execution_context_payload.validate()?;
        if blockers.is_empty() {
            execution_context_payload.validate_complete_plan_closure()?;
        }

        // lineage 只引用实际可用的来源；Option::extend(None/Some) 将可选快照展开为 0/1 项。
        let mut context_sources = vec![input.decision_context.clone()];
        context_sources.extend(input.account_snapshot.clone());
        context_sources.extend(input.quote_snapshot.clone());
        context_sources.extend(input.market_clock_snapshot.clone());
        context_sources.extend(execution_plan_ref);
        let execution_context = self.artifact(
            ArtifactKind::ExecutionContext,
            "execution.context",
            &execution_context_payload,
            context_sources,
            input,
        )?;
        let execution_context_ref = artifact_ref(&execution_context);

        // blockers 被消费为 NoOrder 的稳定列表；只有集合完全为空才表示 Gate 已受理执行
        // 计划。Accepted 仍不是订单提交或成交；后续先持久化 PaperCommitment，再经
        // Dispatch/Reconcile 对 Broker 发请求及确认回执。
        let verdict_payload = if blockers.is_empty() {
            ExecutionVerdict::Accepted {
                execution_context: execution_context_ref.clone(),
            }
        } else {
            ExecutionVerdict::NoOrder {
                no_order: NoOrder {
                    execution_context: execution_context_ref.clone(),
                    blockers: blockers.into_iter().collect(),
                    created_at: input.now,
                },
            }
        };
        verdict_payload.validate()?;
        let verdict = self.artifact(
            ArtifactKind::ExecutionVerdict,
            "execution.verdict",
            &verdict_payload,
            vec![execution_context_ref],
            input,
        )?;
        Ok(ExecutionGateOutput {
            execution_plan,
            execution_context,
            verdict,
        })
    }

    /// Atomically persists the optional plan, context, verdict and task terminal state.
    pub fn commit(
        &self,
        permit: &TaskWritePermit,
        output: &ExecutionGateOutput,
        now: DateTime<Utc>,
    ) -> ExecutionGateResult<()> {
        // 把可选 plan、context、verdict 与 task 成功状态一次提交；提交是 ExecutionGate 的
        // durable 边界，之后才可能由独立 Commitment task 继续，而不是在 evaluate 内隐式写 Broker。
        let mut artifacts = Vec::with_capacity(3);
        artifacts.extend(output.execution_plan.clone());
        artifacts.push(output.execution_context.clone());
        artifacts.push(output.verdict.clone());
        self.store
            .commit_attempt(permit, &artifacts, TaskStatus::Succeeded, now)?;
        Ok(())
    }

    fn validate_input(&self, input: &ExecutionGateInput) -> ExecutionGateResult<()> {
        // 只检查调用方声明的引用 kind；实体存在、实际 kind 与来源 lineage 在加载阶段核实。
        // 先做轻量引用 kind 检查，避免用错误类型的 ArtifactRef 进入更深的 Store/JSON 解析。
        if input.decision_context.kind != ArtifactKind::DecisionContext
            || input
                .account_snapshot
                .as_ref()
                .is_some_and(|reference| reference.kind != ArtifactKind::NormalizedEvidence)
            || input
                .quote_snapshot
                .as_ref()
                .is_some_and(|reference| reference.kind != ArtifactKind::NormalizedEvidence)
            || input
                .market_clock_snapshot
                .as_ref()
                .is_some_and(|reference| reference.kind != ArtifactKind::NormalizedEvidence)
        {
            return Err(ExecutionGateError::Integrity("input artifact kinds"));
        }
        Ok(())
    }
    fn frozen_plan_risk(
        &self,
        input: &AllocationInput,
        plan: &crate::ExecutionPlan,
    ) -> ExecutionGateResult<Option<AllocatedPlanRisk>> {
        let decision = &input.decision_context;
        let Some(stored) = self
            .store
            .decision_policy_by_hash(&decision.decision_policy_hash)?
        else {
            return Ok(None);
        };
        let bytes = self.store.read_blob(&stored.artifact.blob)?;
        let Ok(envelope) = crate::DecisionPolicyArtifact::decode_strict(&bytes) else {
            return Ok(None);
        };
        let provenance = &envelope.provenance;
        let descriptor = &stored.descriptor;
        let scope = envelope.policy.active_forecast_calibration.as_ref();
        if provenance.output_hash != decision.decision_policy_hash
            || provenance.provider_id.as_ref() != Some(&descriptor.provider_id)
            || provenance.model_route.as_ref() != Some(&descriptor.model_route)
            || provenance.contract_hash.as_ref() != Some(&descriptor.contract_hash)
            || scope.is_none_or(|scope| {
                scope.model_id != descriptor.model_id
                    || scope.model_version_hash != descriptor.model_version_hash
            })
            || !envelope.policy.decision_capable()
            || match &decision.portfolio_risk.risk_model_hash {
                Some(hash) => hash != &provenance.risk_model_hash,
                // A calibrated decision can legitimately abstain on every asset.
                // Its frozen policy still supplies the risk of existing positions.
                None => decision.target.weights.values().any(|weight| weight.0 != 0),
            }
        {
            return Ok(None);
        }
        Ok(self.assess_fill_corners(input, plan, &envelope.policy))
    }

    fn frozen_fill_weights(
        input: &AllocationInput,
        asset: akzio_domain::Asset,
        order: Option<&crate::OrderIntent>,
    ) -> Option<AssetFillWeights> {
        let equity = i128::from(input.account.equity.0);
        if equity <= 0 {
            return None;
        }
        let (held, market_value) = input
            .account
            .positions
            .get(&asset)
            .map_or((0, 0), |position| {
                (
                    i128::from(position.quantity_micros),
                    i128::from(position.market_value.0),
                )
            });
        let scale = i128::from(akzio_domain::WeightPpm::SCALE);
        if (held, market_value) != (0, 0) && !(held > 0 && market_value > 0) {
            return None;
        }
        let before = FillWeightBounds::enclose(market_value.checked_mul(scale)?, equity)?;
        let Some(order) = order else {
            return Some(AssetFillWeights {
                before,
                after: before,
                sold: FillWeightBounds::default(),
            });
        };
        let quantity = i128::from(crate::paper::order_quantity_millionths(order).ok()?);
        let shares = match order.side {
            crate::OrderSide::Buy => held.checked_add(quantity)?,
            crate::OrderSide::Sell => held.checked_sub(quantity)?,
        };
        if shares < 0 {
            return None;
        }

        // Held assets use the frozen account mark (market value / held shares).
        // A new holding uses the midpoint of the validated frozen quote.
        let (numerator, denominator) = if held > 0 {
            (market_value.checked_mul(scale)?, held.checked_mul(equity)?)
        } else {
            let quote = input.quotes.quotes.get(&asset)?;
            quote.validate().ok()?;
            // Millionth shares and ppm weights cancel their common scale.
            (
                i128::from(quote.bid.0) + i128::from(quote.ask.0),
                2 * equity,
            )
        };
        let after = FillWeightBounds::enclose(numerator.checked_mul(shares)?, denominator)?;
        let sold = if order.side == crate::OrderSide::Sell {
            FillWeightBounds::enclose(numerator.checked_mul(quantity)?, denominator)?
        } else {
            FillWeightBounds::default()
        };
        Some(AssetFillWeights {
            before,
            after,
            sold,
        })
    }

    fn sales_can_increase_variance(
        fills: &[AssetFillWeights; akzio_domain::Asset::EXECUTABLE.len()],
        risk_model: &crate::PortfolioRiskModel,
    ) -> Option<bool> {
        use akzio_domain::Asset;

        // For each full/no-fill corner, bound the change directly:
        // variance(after) - variance(before) = -soldᵀ Σ (after + before).
        // An unfilled asset has identically zero sold weight. Bounding the two
        // variances separately would lose that cancellation and can incorrectly
        // classify a proven reduction as an increase even at the no-fill corner.
        // The symmetric covariance certificate makes variance convex in fills,
        // so the change at these corners bounds every partial sale as well.
        // All linear risks have unsigned coefficients and decrease with sales.
        let covariance = &risk_model.covariance_ppm_squared;
        for mask in 1..(1 << fills.len()) {
            let mut upper_change = 0_i128;
            for (i, left) in Asset::EXECUTABLE.iter().enumerate() {
                if mask & (1 << i) == 0 || fills[i].sold.upper == 0 {
                    continue;
                }
                let mut lower_gradient = 0_i128;
                for (j, right) in Asset::EXECUTABLE.iter().enumerate() {
                    let coefficient = i128::from(*covariance.get(left)?.get(right)?);
                    let before = fills[j].before;
                    let after = if mask & (1 << j) == 0 {
                        before
                    } else {
                        fills[j].after
                    };
                    let sum = if coefficient < 0 {
                        before.upper + after.upper
                    } else {
                        before.lower + after.lower
                    };
                    lower_gradient += coefficient * i128::from(sum);
                }
                let sold = fills[i].sold;
                let weight = if lower_gradient < 0 {
                    sold.upper
                } else {
                    sold.lower
                };
                upper_change -= lower_gradient * i128::from(weight);
            }
            if upper_change > 0 {
                return Some(true);
            }
        }
        Some(false)
    }

    fn assess_fill_corners(
        &self,
        input: &AllocationInput,
        plan: &crate::ExecutionPlan,
        policy: &crate::DecisionPolicy,
    ) -> Option<AllocatedPlanRisk> {
        use akzio_domain::{Asset, TargetPortfolio, WeightPpm};

        let unique_assets = plan
            .orders
            .iter()
            .map(|order| order.asset)
            .collect::<BTreeSet<_>>();
        if plan.orders.is_empty()
            || plan.orders.len() > Asset::EXECUTABLE.len()
            || unique_assets.len() != plan.orders.len()
        {
            return None;
        }
        let negative_terms = policy.portfolio_risk_model.certified_fill_covariance()?;
        let mandate = &self.gate_policy.mandate;
        let gross_limit = self
            .allocation
            .policy()
            .max_gross_weight
            .0
            .min(mandate.maximum_gross_exposure.0)
            .min(WeightPpm::SCALE - mandate.minimum_cash_reserve.0);

        let mut fills = [AssetFillWeights::default(); Asset::EXECUTABLE.len()];
        let mut before_target = TargetPortfolio::zeroed();
        let mut full_fill_target = TargetPortfolio::zeroed();
        let mut upper_target = TargetPortfolio::zeroed();
        for (index, asset) in Asset::EXECUTABLE.into_iter().enumerate() {
            let order = plan.orders.iter().find(|order| order.asset == asset);
            let weights = Self::frozen_fill_weights(input, asset, order)?;
            fills[index] = weights;
            before_target
                .weights
                .insert(asset, WeightPpm(weights.before.upper));
            full_fill_target
                .weights
                .insert(asset, WeightPpm(weights.after.upper));
            upper_target.weights.insert(
                asset,
                WeightPpm(weights.before.upper.max(weights.after.upper)),
            );
        }
        // The nonnegative linear risks use the componentwise upper point.
        // Variance can increase at mixed corners when covariance is negative.
        // The certified PSD matrix bounds it at the vertices of the enclosing
        // box, including the fractional weight of every asset with no order.
        let has_buy = plan
            .orders
            .iter()
            .any(|order| order.side == crate::OrderSide::Buy);
        let risk_increasing =
            has_buy || Self::sales_can_increase_variance(&fills, &policy.portfolio_risk_model)?;
        let mut risk = policy.fill_risk(&upper_target).ok()?;
        let mut corner = upper_target.clone();
        for mask in 0..(1 << fills.len()) - 1 {
            for (index, asset) in Asset::EXECUTABLE.into_iter().enumerate() {
                let AssetFillWeights { before, after, .. } = fills[index];
                let weight = if mask & (1 << index) == 0 {
                    before.lower.min(after.lower)
                } else {
                    before.upper.max(after.upper)
                };
                corner.weights.insert(asset, WeightPpm(weight));
            }
            risk.variance_numerator = risk
                .variance_numerator
                .max(policy.fill_variance_numerator(&corner).ok()?);
        }
        let exceeds_frozen_limits = risk_increasing
            && !policy
                .fits_fill_target(&upper_target, &risk, negative_terms, gross_limit)
                .ok()?;
        let projected_drawdown_ppm = if risk_increasing {
            risk.expected_shortfall_ppm
        } else {
            policy
                .fill_risk(&full_fill_target)
                .ok()?
                .expected_shortfall_ppm
        };
        Some(AllocatedPlanRisk {
            before_target,
            full_fill_target,
            projected_drawdown_ppm,
            risk_increasing,
            exceeds_frozen_limits,
        })
    }

    fn assess_allocated_plan(
        &self,
        input: &AllocationInput,
        plan: &crate::ExecutionPlan,
        pretrade: Option<&PreTradeSafetyEvidence>,
        blockers: &mut BTreeSet<HardBlocker>,
    ) -> ExecutionGateResult<(
        Option<akzio_domain::MandateAssessment>,
        Option<crate::PreTradeSafetyAssessment>,
    )> {
        // Report independent static plan limits even when frozen risk cannot
        // be reconstructed. The persisted plan keeps its original valuation.
        blockers.extend(
            self.gate_policy
                .blockers_for(&plan.factor_exposure, plan.turnover_ppm),
        );
        let Some(risk) = self.frozen_plan_risk(input, plan)? else {
            blockers.insert(HardBlocker::InvalidProvenance);
            return Ok((None, None));
        };
        if risk.exceeds_frozen_limits {
            blockers.insert(HardBlocker::FactorLimit);
        }
        let wire_exposure = akzio_domain::FactorExposure::from_target(&risk.full_fill_target)?;
        blockers.extend(
            self.gate_policy
                .blockers_for(&wire_exposure, plan.turnover_ppm),
        );
        use akzio_domain::{MandateActionType, MandateViolation, WeightPpm};

        // Use the same upper rounding on both portfolios. Mixing rounding
        // directions can make a proven reduction appear to increase gross.
        let mandate_policy = &self.gate_policy.mandate;
        let mut mandate = mandate_policy.assess(
            &risk.before_target,
            &risk.full_fill_target,
            WeightPpm(risk.projected_drawdown_ppm),
            WeightPpm(plan.turnover_ppm),
        );
        let prohibited = &mandate_policy.prohibited_actions;
        // Every formed plan trades, even when a share delta is below one ppm.
        if prohibited.contains(&MandateActionType::Trade)
            || (risk.risk_increasing && prohibited.contains(&MandateActionType::IncreaseRisk))
        {
            mandate
                .violations
                .insert(MandateViolation::ProhibitedAction);
        }
        if !mandate.permitted() {
            blockers.insert(HardBlocker::MandateViolation);
        }
        // 安全证据由 Gate 外部采集但仅作为输入值；存在时由 Rust 再计算 capacity、
        // classification、compliance、dependency。缺失时明确补齐三个 blocker。
        let pretrade_safety = if let Some(evidence) = pretrade {
            let ordered_assets = plan
                .orders
                .iter()
                .map(|order| order.asset)
                .collect::<BTreeSet<_>>();
            let assessment = self.pretrade_safety.assess(&PreTradeSafetyInput {
                target: risk.full_fill_target,
                ordered_assets,
                risk_increasing: risk.risk_increasing,
                account_equity: input.account.equity,
                homogeneous_agent_count: evidence.homogeneous_agent_count,
                average_daily_dollar_volume: evidence.average_daily_dollar_volume.clone(),
                information_classifications: evidence.information_classifications.clone(),
                recent_compliance_activity: evidence.recent_compliance_activity.clone(),
                dependency_closure: evidence.dependency_closure.clone(),
                now: input.now,
            })?;
            if assessment.risk_increasing
                && (assessment.capacity_scenario.is_none()
                    || assessment
                        .capacity_scenario
                        .as_ref()
                        .is_some_and(|scenario| !scenario.within_capacity))
            {
                blockers.insert(HardBlocker::CapacityLimit);
            }
            if !assessment.missing_information_classifications.is_empty()
                || assessment
                    .compliance_violations_by_asset
                    .values()
                    .any(|violations| !violations.is_empty())
            {
                blockers.insert(HardBlocker::ComplianceViolation);
            }
            if (assessment.risk_increasing && !assessment.dependency_permits_new_risk)
                || (!assessment.risk_increasing && !assessment.dependency_permits_risk_reduction)
            {
                blockers.insert(HardBlocker::DependencyDegraded);
            }
            // 即使 assessment 返回，也只有 permits_execution=true 且全局 blockers
            // 最终为空时才会 Accepted；此处仅记录 assessment 和各 Gate 原因。
            Some(assessment)
        } else {
            blockers.extend([
                HardBlocker::CapacityLimit,
                HardBlocker::ComplianceViolation,
                HardBlocker::DependencyDegraded,
            ]);
            None
        };
        Ok((Some(mandate), pretrade_safety))
    }
}
