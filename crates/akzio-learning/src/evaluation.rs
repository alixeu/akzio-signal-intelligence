//! Canonical, outcome-backed learning runtime for Akzio.
//!
//! Callers provide governed observations, never precomputed learning metrics.
//! Rust materializes T+1/T+3/T+5 windows and Store-owned run purpose remains
//! the canonicality authority.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;
use thiserror::Error;

use akzio_domain::{
    content_hash_json, AccountSnapshot, Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin,
    ArtifactProvenance, ArtifactRef, Asset, CalibrationReport, CandidatePolicy,
    CandidatePolicyState, ContentHash, CountedRatio, DecisionHorizon, DomainError, Evaluation,
    EvaluationId, ExecutionPlan, Experience, ExperienceId, Forecast, ForecastCalibrationBin,
    ForecastScore, Lesson, LessonId, LessonLifecycle, LessonOrigin, LessonScope, MemoryLifecycle,
    MoneyMicros, OrderReceipt, OrderSide, Outcome, OutcomeBenchmark, OutcomeBenchmarkAttribution,
    OutcomeBenchmarkNavPoint, OutcomeBenchmarkResult, OutcomeBenchmarkUnavailableReason,
    OutcomeCostModel, OutcomeExecutionLineage, OutcomeHorizon, OutcomeNavPoint,
    OutcomeOrderCostAttribution, OutcomeSchedule, OutcomeWindow, PolicyState, PolicySubject,
    PolicyTransition, PolicyTransitionId, Retrospective, RetrospectiveDraft, RetrospectiveStatus,
    RiskGroundTruthAssessment, RunPurpose, TargetPortfolio, TaskWritePermit, TopologyId, WeightPpm,
    DOMAIN_SCHEMA_VERSION, FORECAST_CALIBRATION_BIN_COUNT, OUTCOME_BENCHMARK_DEFINITION_VERSION,
};
use akzio_store::{
    DaemonLease, PolicyEvaluationCommit, PolicyHead, ShadowPairCompletion, ShadowPairWriteResult,
    Store, StoreError,
};

const PPM_ONE: u32 = 1_000_000;

// 文件导读：EvaluationRuntime 只接收受治理的观察和已存在的 Artifact 引用；T+1/T+3/T+5
// 的数值由 Rust 重建，后续模块再把密封 T+5 结果交给 Store 的 Policy/Lesson 事务。

/// Akzio policy, not an external statistical standard. Fewer than three fresh
/// paired outcomes per horizon cannot advance memory by default.
pub const AKZIO_MIN_FRESH_PAIRS_PER_HORIZON: u64 = 3;

/// Akzio policy, not an external statistical standard. A ten-bin ECE report
/// needs enough observations that it is not merely relabelling one event.
pub const AKZIO_MIN_CALIBRATION_SAMPLES: u64 = 30;

/// Akzio policy. Five daily returns are enough for a diagnostic beta/tracking
/// estimate; ES/CVaR remains unmeasured until a materially longer sample.
const AKZIO_MIN_PATH_RISK_SAMPLES: usize = 5;
const AKZIO_MIN_EXPECTED_SHORTFALL_SAMPLES: usize = 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationPolicy {
    pub minimum_evidence_completeness_ppm: u32,
    pub minimum_risk_recall_ppm: u32,
    pub minimum_fresh_pairs_per_horizon: u64,
}

impl Default for EvaluationPolicy {
    // 默认值只定义晋级所需阈值；构造默认 policy 不读取 Store，也不代表任何 Outcome 已合格。
    fn default() -> Self {
        Self {
            minimum_evidence_completeness_ppm: 900_000,
            minimum_risk_recall_ppm: 900_000,
            minimum_fresh_pairs_per_horizon: AKZIO_MIN_FRESH_PAIRS_PER_HORIZON,
        }
    }
}

impl EvaluationPolicy {
    /// Degradation must rest on observed evidence, never on absent evidence.
    /// A window whose `risk_recall_ppm` is `None` was never measured, so it
    /// cannot prove degradation; `risk_recall_is_measured` gates promotion
    /// separately so an unmeasured outcome neither promotes nor demotes.
    // 共享借用 Outcome，只检查已测量值是否低于策略下限；None 不参与降级判断，是否允许晋级
    // 由独立的 risk_recall_is_measured gate 决定。
    pub fn outcome_is_degraded(&self, outcome: &Outcome) -> bool {
        outcome.windows.iter().any(|window| {
            window
                .evidence_completeness_ppm
                .is_some_and(|value| value < self.minimum_evidence_completeness_ppm)
                || window
                    .risk_recall_ppm
                    .is_some_and(|value| value < self.minimum_risk_recall_ppm)
        })
    }

    /// True only when every window carries a measured risk recall. Forward
    /// policy transitions require this; without it an outcome that silently
    /// skipped risk measurement could buy a promotion.
    // all 逐窗口消费迭代器；空 windows 按 all 的集合语义会返回 true，因此完整 Outcome 的
    // 非空结构还由领域校验保证，不能只凭这个辅助方法判定整体资格。
    pub fn risk_recall_is_measured(&self, outcome: &Outcome) -> bool {
        outcome
            .windows
            .iter()
            .all(|window| window.risk_recall_ppm.is_some())
    }

    // 同样只报告测量完整性，不计算平均值或替缺失窗口填默认分数。
    pub fn evidence_completeness_is_measured(&self, outcome: &Outcome) -> bool {
        outcome
            .windows
            .iter()
            .all(|window| window.evidence_completeness_ppm.is_some())
    }

    // 校验 ppm 上限与非零 fresh-pair 阈值；错误由 EvaluationRuntime::new 的 ? 传播，
    // 因而非法策略不能构造运行时。
    fn validate(&self) -> Result<(), EvaluationError> {
        if self.minimum_evidence_completeness_ppm > PPM_ONE
            || self.minimum_risk_recall_ppm > PPM_ONE
            || self.minimum_fresh_pairs_per_horizon == 0
        {
            return Err(EvaluationError::InvalidPolicy);
        }
        Ok(())
    }
}

/// One governed future price surface for a due schedule horizon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GovernedHorizonObservation {
    pub horizon: OutcomeHorizon,
    pub completed_trading_sessions: u8,
    pub observed_trading_day: NaiveDate,
    pub future_prices: BTreeMap<Asset, MoneyMicros>,
    pub expected_evidence_count: u64,
    pub observed_evidence_count: u64,
    pub risk_recall: Option<GovernedRiskRecall>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GovernedRiskRecall {
    pub assessment: ArtifactRef,
    pub expected_risk_ids: BTreeSet<String>,
    pub detected_risk_ids: BTreeSet<String>,
}

impl GovernedRiskRecall {
    // assessment 必须指向风险真值 Artifact，expected 集合非空且 detected 是其子集；
    // 这里仅验证已提供的测量，不访问 Store 核验 Artifact 正文。
    fn validate(&self) -> EvaluationRuntimeResult<()> {
        if self.assessment.kind != ArtifactKind::RiskGroundTruthAssessment
            || self.expected_risk_ids.is_empty()
            || !self.detected_risk_ids.is_subset(&self.expected_risk_ids)
        {
            return Err(EvaluationError::InvalidMaterialization(
                "risk ground truth measurement",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GovernedDailyObservation {
    pub observed_trading_day: NaiveDate,
    pub future_prices: BTreeMap<Asset, MoneyMicros>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObservedExecutionMetrics {
    pub turnover_ppm: u32,
    pub transaction_cost_ppm: u32,
    /// Limit-price diagnostic; not an additional cost when signed effects exist.
    pub implementation_shortfall_ppm: u32,
    pub implementation_effect_ppm: Option<i64>,
    pub initial_valuation_effect_ppm: Option<i64>,
    pub order_cost_attributions: Vec<OutcomeOrderCostAttribution>,
}

impl ObservedExecutionMetrics {
    // 合并两个有符号估值调整；checked_add 防止溢出并将失败转换为 EvaluationError。
    fn valuation_adjustment_ppm(&self) -> EvaluationRuntimeResult<i64> {
        self.implementation_effect_ppm
            .unwrap_or(0)
            .checked_add(self.initial_valuation_effect_ppm.unwrap_or(0))
            .ok_or(EvaluationError::ArithmeticOverflow)
    }
    // 若已有真实 signed implementation effect，就不再把 limit shortfall 重复计作滑点成本。
    fn deductible_slippage_ppm(&self) -> u32 {
        if self.implementation_effect_ppm.is_some() {
            0
        } else {
            self.implementation_shortfall_ppm
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RealizedExecution {
    pub target: TargetPortfolio,
    pub metrics: ObservedExecutionMetrics,
    /// Quantities after unique terminal receipts; values use one valuation basis.
    pub quantities: BTreeMap<Asset, i64>,
    pub cash: MoneyMicros,
}

pub fn horizon_observations(
    bars_by_asset: &BTreeMap<Asset, BTreeMap<NaiveDate, MoneyMicros>>,
    common_dates: &[NaiveDate],
    expected_evidence_count: u64,
    observed_evidence_count: u64,
) -> EvaluationRuntimeResult<Vec<GovernedHorizonObservation>> {
    // 嵌套 BTreeMap 按 Asset/NaiveDate 查价；闭包和迭代器最终由 collect 消费，
    // 任意到期 horizon 缺共同日期或任一资产价格时 ? 使整批返回 Err，不返回部分 Vec。
    // 调用方须提供已对齐、按交易 Session 排列的 common_dates；此函数只按索引
    // 检查已到期期限的四资产价格，不独立验证日期单调性或交易所日历。
    if observed_evidence_count > expected_evidence_count {
        return Err(EvaluationError::InvalidMaterialization(
            "evidence observation count",
        ));
    }
    let completed_sessions = u8::try_from(common_dates.len()).unwrap_or(u8::MAX);
    OutcomeHorizon::ALL
        .into_iter()
        .filter(|horizon| horizon.is_due_after(completed_sessions))
        .map(|horizon| {
            let index = usize::from(horizon.trading_days()) - 1;
            let observed_trading_day = *common_dates
                .get(index)
                .ok_or(EvaluationError::UnalignedBars)?;
            let future_prices =
                Asset::EXECUTABLE
                    .into_iter()
                    .try_fold(BTreeMap::new(), |mut prices, asset| {
                        let price = bars_by_asset
                            .get(&asset)
                            .and_then(|bars| bars.get(&observed_trading_day))
                            .copied()
                            .ok_or(EvaluationError::UnalignedBars)?;
                        prices.insert(asset, price);
                        Ok::<_, EvaluationError>(prices)
                    })?;
            Ok(GovernedHorizonObservation {
                horizon,
                completed_trading_sessions: completed_sessions,
                observed_trading_day,
                future_prices,
                expected_evidence_count,
                observed_evidence_count,
                risk_recall: None,
            })
        })
        .collect()
}

pub fn daily_observations(
    bars_by_asset: &BTreeMap<Asset, BTreeMap<NaiveDate, MoneyMicros>>,
    common_dates: &[NaiveDate],
) -> EvaluationRuntimeResult<Vec<GovernedDailyObservation>> {
    // 每个 common_dates 项生成一个日观察；try_fold 按四个可执行资产累积价格表，
    // collect 只有在所有日期均成功时才返回完整结果。
    // 日频路径给回撤、tracking error、beta、Sortino 和 benchmark 使用；它同样要求每个
    // 输入日期都有四资产价格；日期是否为共同交易 Session 由上游保证，本函数不查询日历。
    common_dates
        .iter()
        .map(|observed_trading_day| {
            let future_prices =
                Asset::EXECUTABLE
                    .into_iter()
                    .try_fold(BTreeMap::new(), |mut prices, asset| {
                        let price = bars_by_asset
                            .get(&asset)
                            .and_then(|bars| bars.get(observed_trading_day))
                            .copied()
                            .ok_or(EvaluationError::UnalignedBars)?;
                        prices.insert(asset, price);
                        Ok::<_, EvaluationError>(prices)
                    })?;
            Ok(GovernedDailyObservation {
                observed_trading_day: *observed_trading_day,
                future_prices,
            })
        })
        .collect()
}

/// Raw inputs from which Rust deterministically materializes a sealed Outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutcomeMaterializationInput {
    pub schedule: OutcomeSchedule,
    pub schedule_artifact: ArtifactRef,
    pub target: TargetPortfolio,
    pub forecasts: Vec<Forecast>,
    pub baseline_prices: BTreeMap<Asset, MoneyMicros>,
    pub observations: Vec<GovernedHorizonObservation>,
    pub daily_observations: Vec<GovernedDailyObservation>,
    pub market_evidence: Vec<ArtifactRef>,
    pub cost_model: OutcomeCostModel,
    pub observed_execution: Option<ObservedExecutionMetrics>,
    pub sealed_at: DateTime<Utc>,
}

/// Derive the realized portfolio target from a validated Paper account and fills.
pub fn realized_execution_target(
    account: &AccountSnapshot,
    execution: &OutcomeExecutionLineage,
    plan: Option<&ExecutionPlan>,
    receipts: &[OrderReceipt],
) -> EvaluationRuntimeResult<TargetPortfolio> {
    // 仅保留 realized_execution 计算结果中的 target；账户、lineage、plan 与 receipts 全部借用，
    // 默认成本模型按值传入，任何执行事实校验错误继续作为 Result 返回。
    // `?` 在拆出 .target 前传播错误，不能把无效 Receipt 的目标当成成功返回值。
    Ok(realized_execution(
        account,
        execution,
        plan,
        receipts,
        OutcomeCostModel::default(),
    )?
    .target)
}

/// Derive achieved weights and fill-driven turnover/costs. Transaction cost is
/// the configured rate applied to actual fill turnover. Implementation
/// shortfall is the adverse difference between average fill and the immutable
/// plan limit; favorable price improvement is not counted as negative cost.
pub fn realized_execution(
    account: &AccountSnapshot,
    execution: &OutcomeExecutionLineage,
    plan: Option<&ExecutionPlan>,
    receipts: &[OrderReceipt],
    cost_model: OutcomeCostModel,
) -> EvaluationRuntimeResult<RealizedExecution> {
    // 此兼容入口只从账户 mark 或订单限价构造估值价格，缺失时沿用实现中的 1 美元默认值；
    // 随后清空无法由这些输入证实的 signed effect/baseline，避免把估算结果伪装成真实报价。
    account.validate()?;
    cost_model.validate()?;
    // 兼容入口只能看到 account mark 和 plan limit，不能伪造 arrival/baseline quote；
    // 后面清空 signed implementation effect，避免把 limit 诊断与真实估值差额重复计费。
    let prices = Asset::EXECUTABLE
        .into_iter()
        .map(|asset| {
            let price = account
                .positions
                .get(&asset)
                .filter(|p| p.quantity_micros > 0)
                .map(|p| i128::from(p.market_value.0) * 1_000_000 / i128::from(p.quantity_micros))
                .or_else(|| {
                    plan.and_then(|p| p.orders.iter().find(|o| o.asset == asset))
                        .map(|o| i128::from(o.limit_price.0))
                })
                .unwrap_or(1_000_000);
            Ok((
                asset,
                MoneyMicros(i64::try_from(price).map_err(|_| EvaluationError::ArithmeticOverflow)?),
            ))
        })
        .collect::<EvaluationRuntimeResult<BTreeMap<_, _>>>()?;
    let mut realized =
        realized_execution_at_prices(account, execution, plan, receipts, cost_model, &prices)?;
    // This compatibility entrypoint only has account marks/limits, not quotes.
    realized.metrics.implementation_effect_ppm = None;
    realized.metrics.initial_valuation_effect_ppm = None;
    for attribution in &mut realized.metrics.order_cost_attributions {
        attribution.baseline_mid = None;
        attribution.baseline_to_fill_effect = None;
    }
    Ok(realized)
}

/// Reconstruct shares and cash, then value remaining shares at caller-supplied
/// prices. The caller must bind them to immutable execution-context quotes;
/// this pure function checks positivity/universe, not CAS provenance.
/// Later account rebalances are not included.
pub fn realized_execution_at_prices(
    account: &AccountSnapshot,
    execution: &OutcomeExecutionLineage,
    plan: Option<&ExecutionPlan>,
    receipts: &[OrderReceipt],
    cost_model: OutcomeCostModel,
    prices: &BTreeMap<Asset, MoneyMicros>,
) -> EvaluationRuntimeResult<RealizedExecution> {
    // account/lineage/plan/receipts/prices 都以借用读取，cost_model 按值消费。
    // 返回的是基于传入冻结价格与终态成交重建的结果；无效任一 receipt 会拒绝整个 Outcome，
    // 本计算不写 Store，也不包含成交后的账户再平衡。
    account.validate()?;
    cost_model.validate()?;
    validate_prices(prices)?;
    // 先从账户持仓与 equity 重建数量和现金，再只应用唯一终态 Receipt；随后用冻结的
    // execution-context midpoint 估值，不能把之后账户的再平衡混进这个 Outcome。
    let mut quantities = Asset::EXECUTABLE
        .into_iter()
        .map(|asset| {
            (
                asset,
                account
                    .positions
                    .get(&asset)
                    .map_or(0, |p| p.quantity_micros),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let equity = i128::from(account.equity.0);
    let mut cash = equity
        - account
            .positions
            .values()
            .map(|p| i128::from(p.market_value.0))
            .sum::<i128>();
    let mut seen_receipts = BTreeMap::new();
    let mut seen_assets = BTreeSet::new();

    let mut fill_notional_micros = 0_i128;
    let mut shortfall_micros = 0_i128;
    let mut implementation_effect_micros = 0_i128;
    let initial_valuation_micros = account
        .positions
        .iter()
        .map(|(asset, position)| {
            i128::from(position.quantity_micros) * i128::from(prices[asset].0) / 1_000_000
                - i128::from(position.market_value.0)
        })
        .sum::<i128>();
    let mut order_cost_attributions = Vec::with_capacity(receipts.len());
    if matches!(execution, OutcomeExecutionLineage::ReconciledPaper { .. }) {
        // 只有 ReconciledPaper 分支消费 plan 与 receipts；NoOrder 分支稍后明确拒绝任何 plan/fill，
        // 因而研究意图仍可被评估，但不会被伪造成实际成交。
        // ReconciledPaper 必须有完整 ExecutionPlan 和每个订单的终态 receipt；NoOrder
        // 则禁止携带 plan/fill，二者是互斥的执行证据边界。
        let plan = plan.ok_or(EvaluationError::InvalidMaterialization("execution plan"))?;
        plan.validate()?;
        for receipt in receipts {
            receipt.validate()?;
            if let Some(previous) = seen_receipts.insert(receipt.client_order_id.clone(), receipt) {
                // 恢复重放的完全相同 receipt 可幂等跳过；同一 client_order_id 的不同内容
                // 是冲突，而不是“多一笔成交”。
                if previous == receipt {
                    continue;
                }
                return Err(EvaluationError::InvalidMaterialization(
                    "conflicting duplicate receipt",
                ));
            }
            if !seen_assets.insert(receipt.asset) {
                return Err(EvaluationError::InvalidMaterialization(
                    "multiple terminal orders for asset",
                ));
            }
            if !receipt.state.is_final_without_successor() {
                return Err(EvaluationError::InvalidMaterialization(
                    "non-terminal broker receipt",
                ));
            }
            if receipt.plan_hash != plan.plan_hash {
                return Err(EvaluationError::InvalidMaterialization(
                    "broker receipt plan hash",
                ));
            }
            let order = plan
                .orders
                .iter()
                .find(|order| order.asset == receipt.asset)
                .ok_or(EvaluationError::InvalidMaterialization(
                    "broker receipt is not in execution plan",
                ))?;
            if i128::from(receipt.requested_quantity_micros)
                > i128::from(order.notional.0) * 1_000_000 / i128::from(order.limit_price.0)
            {
                return Err(EvaluationError::InvalidMaterialization(
                    "receipt order quantity mismatch",
                ));
            }
            let limit_shortfall = receipt.average_fill_price.map(|fill_price| {
                MoneyMicros(
                    match order.side {
                        OrderSide::Buy => fill_price.0.saturating_sub(order.limit_price.0),
                        OrderSide::Sell => order.limit_price.0.saturating_sub(fill_price.0),
                    }
                    .max(0),
                )
            });
            order_cost_attributions.push(OutcomeOrderCostAttribution {
                asset: receipt.asset,
                side: order.side,
                filled_quantity_micros: receipt.filled_quantity_micros,
                unfilled_quantity_micros: receipt.remaining_quantity_micros,
                limit_price: order.limit_price,
                decision_mid: None,
                arrival_mid: None,
                baseline_mid: Some(prices[&receipt.asset]),
                baseline_to_fill_effect: receipt.average_fill_price.map(|fill| {
                    MoneyMicros(match order.side {
                        OrderSide::Buy => prices[&receipt.asset].0 - fill.0,
                        OrderSide::Sell => fill.0 - prices[&receipt.asset].0,
                    })
                }),
                fill_vwap: receipt.average_fill_price,
                limit_shortfall,
                spread_cost: None,
                market_impact: None,
                unfilled_opportunity_cost: None,
            });
            if receipt.filled_quantity_micros == 0 {
                continue;
            }
            let fill_price =
                receipt
                    .average_fill_price
                    .ok_or(EvaluationError::InvalidMaterialization(
                        "filled receipt missing price",
                    ))?;
            let fill_value = i128::from(receipt.filled_quantity_micros)
                .saturating_mul(i128::from(fill_price.0))
                .saturating_div(1_000_000);
            fill_notional_micros = fill_notional_micros.saturating_add(fill_value);
            let adverse_price = match order.side {
                OrderSide::Buy => fill_price.0.saturating_sub(order.limit_price.0),
                OrderSide::Sell => order.limit_price.0.saturating_sub(fill_price.0),
            }
            .max(0);
            shortfall_micros = shortfall_micros.saturating_add(
                i128::from(receipt.filled_quantity_micros)
                    .saturating_mul(i128::from(adverse_price))
                    .saturating_div(1_000_000),
            );
            let baseline_effect = match order.side {
                OrderSide::Buy => i128::from(prices[&receipt.asset].0) - i128::from(fill_price.0),
                OrderSide::Sell => i128::from(fill_price.0) - i128::from(prices[&receipt.asset].0),
            };
            implementation_effect_micros +=
                i128::from(receipt.filled_quantity_micros) * baseline_effect / 1_000_000;
            let signed_quantity = match order.side {
                OrderSide::Buy => {
                    cash -= fill_value;
                    receipt.filled_quantity_micros
                }
                OrderSide::Sell => {
                    cash += fill_value;
                    -receipt.filled_quantity_micros
                }
            };
            let quantity = quantities
                .get_mut(&receipt.asset)
                .expect("asset is indexed");
            *quantity = quantity
                .checked_add(signed_quantity)
                .ok_or(EvaluationError::ArithmeticOverflow)?;
            if *quantity < 0 {
                return Err(EvaluationError::InvalidMaterialization(
                    "execution fills produce a short realized position",
                ));
            }
        }
        if seen_assets.len() != plan.orders.len() {
            // 每个 plan order 都必须有一个唯一终态回执；若缺回执，当前局部重建结果不会返回。
            return Err(EvaluationError::InvalidMaterialization(
                "missing terminal receipt",
            ));
        }
    } else if plan.is_some() || !receipts.is_empty() {
        return Err(EvaluationError::InvalidMaterialization(
            "NoOrder contains fills",
        ));
    }
    let weights = quantities
        .iter()
        .map(|(asset, quantity)| {
            let value = i128::from(*quantity) * i128::from(prices[asset].0) / 1_000_000;
            let ppm = u32::try_from(value * 1_000_000 / equity)
                .map_err(|_| EvaluationError::InvalidMaterialization("realized position weight"))?;
            Ok((*asset, WeightPpm(ppm)))
        })
        .collect::<EvaluationRuntimeResult<BTreeMap<_, _>>>()?;
    // 权重、turnover、fee 和 signed price effect 都以同一 equity/ppm 基准计算；负仓位
    // 或算术溢出会返回错误，cash 则按现有账户/成交重建结果保留，不在此处另加规则。
    let target = TargetPortfolio { weights };
    target.validate_universe()?;
    let turnover_ppm = ratio_of_equity_ppm(fill_notional_micros, equity)?;
    let fees = fill_notional_micros * i128::from(cost_model.transaction_cost_ppm) / 1_000_000;
    let transaction_cost_ppm = ratio_of_equity_ppm(fees, equity)?;
    let implementation_shortfall_ppm = ratio_of_equity_ppm(shortfall_micros, equity)?;
    Ok(RealizedExecution {
        target,
        quantities,
        cash: MoneyMicros(
            i64::try_from(cash - fees).map_err(|_| EvaluationError::ArithmeticOverflow)?,
        ),
        metrics: ObservedExecutionMetrics {
            turnover_ppm,
            transaction_cost_ppm,
            implementation_shortfall_ppm,
            implementation_effect_ppm: Some(
                i64::try_from(implementation_effect_micros * 1_000_000 / equity)
                    .map_err(|_| EvaluationError::ArithmeticOverflow)?,
            ),
            initial_valuation_effect_ppm: Some(
                i64::try_from(initial_valuation_micros * 1_000_000 / equity)
                    .map_err(|_| EvaluationError::ArithmeticOverflow)?,
            ),
            order_cost_attributions,
        },
    })
}

fn ratio_of_equity_ppm(value: i128, equity: i128) -> EvaluationRuntimeResult<u32> {
    // 将金额比例转为 ppm 前先验证分子与 equity；转换溢出通过 Result 返回，而非截断。
    // 这是所有执行成本比例的共同分母检查：equity 必须为正，负值表示输入或方向不合法。
    if value < 0 || equity <= 0 {
        return Err(EvaluationError::InvalidMaterialization("execution ratio"));
    }
    u32::try_from(value.saturating_mul(i128::from(PPM_ONE)) / equity)
        .map_err(|_| EvaluationError::ArithmeticOverflow)
}

pub struct ShadowObservation {
    pub parent_decision: ArtifactRef,
    pub execution_context: ArtifactRef,
    pub candidate_decision: ArtifactRef,
    pub candidate_contract_hash: ContentHash,
    pub candidate_topology_id: String,
    pub horizon: OutcomeHorizon,
    pub parent_outcome: ArtifactRef,
    pub candidate_outcome: ArtifactRef,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CandidatePolicyInput {
    pub baseline: ArtifactRef,
    pub candidate: ArtifactRef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationInput {
    pub permit: TaskWritePermit,
    pub subject: PolicySubject,
    pub hypothesis_id: String,
    pub materialization: OutcomeMaterializationInput,
    pub contract_hash: ContentHash,
    pub topology_id: TopologyId,
    pub candidate_policy: Option<CandidatePolicyInput>,
    pub token_cost: Option<u64>,
    pub latency_millis: Option<u64>,
}

/// Re-evaluate immutable numeric facts through the ordinary learning gates.
/// No prices, metrics, or replacement producer identities are accepted here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedEvaluationInput {
    pub complete_task: bool,
    pub permit: TaskWritePermit,
    pub subject: PolicySubject,
    pub hypothesis_id: String,
    pub outcome: ArtifactRef,
    pub contract_hash: ContentHash,
    pub topology_id: TopologyId,
    pub candidate_policy: Option<CandidatePolicyInput>,
    pub token_cost: Option<u64>,
    pub latency_millis: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationResult {
    pub outcome: ArtifactRef,
    pub experience: ArtifactRef,
    pub evaluation: ArtifactRef,
    pub candidate_policy: Option<ArtifactRef>,
    pub policy_head: Option<PolicyHead>,
    pub fresh_pairs_by_horizon: [u64; 3],
}

#[derive(Debug, Error)]
pub enum EvaluationError {
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("canonical learning rejects non-Paper run purpose {0:?}")]
    NonCanonicalPurpose(RunPurpose),
    #[error("evaluation policy has an invalid threshold")]
    InvalidPolicy,
    #[error("policy subject does not match persisted state")]
    SubjectStateMismatch,
    #[error("hypothesis id must be non-empty")]
    EmptyHypothesis,
    #[error("candidate policy input invalid: {0}")]
    InvalidCandidatePolicy(&'static str),
    #[error("outcome materialization is invalid: {0}")]
    InvalidMaterialization(&'static str),
    #[error("outcome materialization arithmetic overflow")]
    ArithmeticOverflow,
    #[error("Paper outcome bars are not aligned")]
    UnalignedBars,
}

pub type EvaluationRuntimeResult<T> = Result<T, EvaluationError>;

#[derive(Debug, Clone)]
pub struct EvaluationRuntime {
    pub(crate) store: Store,
    policy: EvaluationPolicy,
}

// include! 在编译期把拆分文件的 Rust 项目文本插入当前 evaluation 模块，因此这些 impl
// 共享本文件的导入、私有类型和可见性；它们不是运行时读取的 Prompt/模板，也不额外创建 crate。
include!("evaluation/runtime_setup.rs");
include!("evaluation/materialization.rs");
include!("evaluation/risk_ground_truth.rs");
include!("evaluation/outcomes.rs");
include!("evaluation/policy_learning.rs");
include!("evaluation/benchmarks.rs");
include!("evaluation/materialize_outcome.rs");
include!("evaluation/materialize_partial.rs");
#[path = "metrics.rs"]
mod metrics;
pub(crate) use metrics::aggregate_calibration_report;
use metrics::{
    build_nav_path, counted_ratio, execution_verdict, forecast_score, index_forecasts,
    index_observations, marginal_utility, next_state_with_fresh_pairs, path_risk_metrics,
    portfolio_return_ppm, price, reference, require_canonical_purpose, return_ppm, stable_id,
    validate_prices,
};
