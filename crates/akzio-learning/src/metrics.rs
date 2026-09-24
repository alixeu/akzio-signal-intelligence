use super::*;

// 文件导读：本模块集中放置 Rust 权威的 ppm 收益、forecast score、NAV 和风险指标；
// 任何 None 都表示当前证据未测量或不可识别，不会被转成一个看似精确的默认通过值。

/// Index the model's per-asset up-probabilities by horizon.
///
/// Deliberately returns the per-asset probabilities rather than a
/// portfolio-weighted scalar. `positive_return_probability_ppm` forecasts a
/// single asset's own direction, so the only event it can be scored against is
/// that asset's realized direction. A position-weighted average of marginal
/// probabilities is not the probability that the portfolio return is positive
/// (that needs the assets' joint distribution), and weighting by the target
/// portfolio would fold the allocation decision into the forecast-quality
/// measurement.
pub(super) fn index_forecasts(
    forecasts: &[Forecast],
) -> EvaluationRuntimeResult<BTreeMap<OutcomeHorizon, BTreeMap<Asset, u32>>> {
    // forecast 按 horizon/asset 去重；单资产 forecast 允许只评估该资产，四资产 forecast
    // 必须覆盖完整执行全集，不能用 target weight 把 allocation 决策混进边际概率质量。
    let mut by_horizon = BTreeMap::<OutcomeHorizon, BTreeMap<Asset, u32>>::new();
    for forecast in forecasts {
        forecast.validate()?;
        let horizon = match forecast.horizon {
            DecisionHorizon::T1 => OutcomeHorizon::T1,
            DecisionHorizon::T3 => OutcomeHorizon::T3,
            DecisionHorizon::T5 => OutcomeHorizon::T5,
        };
        if by_horizon
            .entry(horizon)
            .or_default()
            .insert(forecast.asset, forecast.positive_return_probability_ppm)
            .is_some()
        {
            return Err(EvaluationError::InvalidMaterialization(
                "duplicate forecast horizon",
            ));
        }
    }
    if by_horizon.len() != OutcomeHorizon::ALL.len() {
        return Err(EvaluationError::InvalidMaterialization(
            "missing forecast horizon",
        ));
    }
    for by_asset in by_horizon.values() {
        if by_asset.len() != 1
            && (by_asset.len() != Asset::EXECUTABLE.len()
                || Asset::EXECUTABLE
                    .into_iter()
                    .any(|asset| !by_asset.contains_key(&asset)))
        {
            return Err(EvaluationError::InvalidMaterialization(
                "forecast asset coverage",
            ));
        }
    }
    Ok(by_horizon)
}

/// Brier score for one binary forecast, in ppm.
///
/// `(p - o)^2` where `o` is 1 when the asset actually rose. Lower is better and
/// the range is `[0, PPM_ONE]`.
fn asset_brier_ppm(probability_ppm: u32, realized_positive: bool) -> u32 {
    // Brier 是单资产二元事件的 (p-o)^2；概率和结果都在 0..=1_000_000 ppm，越低越好。
    let outcome_ppm = if realized_positive {
        i64::from(PPM_ONE)
    } else {
        0
    };
    let difference = i64::from(probability_ppm) - outcome_ppm;
    let squared = i128::from(difference) * i128::from(difference) / i128::from(PPM_ONE);
    u32::try_from(squared).unwrap_or(PPM_ONE)
}

/// Score one horizon of binary forecasts. This is deliberately named a
/// forecast score: a single event per asset cannot establish calibration.
pub(super) fn forecast_score(
    probabilities_by_asset: &BTreeMap<Asset, u32>,
    baseline_prices: &BTreeMap<Asset, MoneyMicros>,
    future_prices: &BTreeMap<Asset, MoneyMicros>,
) -> EvaluationRuntimeResult<Option<ForecastScore>> {
    // 一个 horizon 的 ForecastScore 只报告该次事件的 Brier 与 bin 汇总；它不是 calibration，
    // calibration 要等多个 Outcome 的独立 score 达到策略最小样本数后再聚合。
    if probabilities_by_asset.is_empty() {
        return Ok(None);
    }
    let mut bins = [ForecastCalibrationBin::default(); FORECAST_CALIBRATION_BIN_COUNT];
    let mut total_brier_ppm = 0_u64;
    for (asset, probability_ppm) in probabilities_by_asset {
        let realized = return_ppm(
            price(baseline_prices, *asset)?,
            price(future_prices, *asset)?,
        )?;
        let brier_ppm = asset_brier_ppm(*probability_ppm, realized > 0);
        total_brier_ppm = total_brier_ppm.saturating_add(u64::from(brier_ppm));
        let bin_index = usize::try_from(
            u64::from(*probability_ppm).saturating_mul(FORECAST_CALIBRATION_BIN_COUNT as u64)
                / u64::from(PPM_ONE),
        )
        .unwrap_or(FORECAST_CALIBRATION_BIN_COUNT - 1)
        .min(FORECAST_CALIBRATION_BIN_COUNT - 1);
        let bin = &mut bins[bin_index];
        bin.sample_count = bin.sample_count.saturating_add(1);
        bin.probability_sum_ppm = bin
            .probability_sum_ppm
            .saturating_add(u64::from(*probability_ppm));
        bin.positive_count = bin.positive_count.saturating_add(u32::from(realized > 0));
        bin.brier_sum_ppm = bin.brier_sum_ppm.saturating_add(u64::from(brier_ppm));
    }
    let sample_count = u32::try_from(probabilities_by_asset.len())
        .map_err(|_| EvaluationError::ArithmeticOverflow)?;
    let mean_brier_ppm = u32::try_from(total_brier_ppm / u64::from(sample_count))
        .map_err(|_| EvaluationError::ArithmeticOverflow)?;
    let score = ForecastScore {
        sample_count,
        mean_brier_ppm,
        bins,
    };
    score.validate()?;
    Ok(Some(score))
}

/// Aggregate independently stored forecast scores into a calibration report.
/// `minimum_samples` is supplied by Akzio policy; the function returns `None`
/// instead of presenting a small sample as calibrated.
pub(crate) fn aggregate_calibration_report(
    scores: impl IntoIterator<Item = ForecastScore>,
    minimum_samples: u64,
) -> Option<CalibrationReport> {
    // 这里按各 score 的 sample_count 加权汇总 Brier，并计算分 bin 的 ECE；样本不足直接
    // 返回 None，让上层把“未测量”保留为 Defer/不可晋级，而不是把小样本命名为 calibrated。
    let mut sample_count = 0_u64;
    let mut brier_sum_ppm = 0_u128;
    let mut bins = [ForecastCalibrationBin::default(); FORECAST_CALIBRATION_BIN_COUNT];
    for score in scores {
        sample_count = sample_count.saturating_add(u64::from(score.sample_count));
        brier_sum_ppm = brier_sum_ppm.saturating_add(
            u128::from(score.mean_brier_ppm).saturating_mul(u128::from(score.sample_count)),
        );
        for (target, source) in bins.iter_mut().zip(score.bins) {
            target.sample_count = target.sample_count.saturating_add(source.sample_count);
            target.probability_sum_ppm = target
                .probability_sum_ppm
                .saturating_add(source.probability_sum_ppm);
            target.positive_count = target.positive_count.saturating_add(source.positive_count);
            target.brier_sum_ppm = target.brier_sum_ppm.saturating_add(source.brier_sum_ppm);
        }
    }
    if sample_count < minimum_samples || sample_count == 0 {
        return None;
    }
    let ece_numerator = bins.iter().fold(0_u128, |total, bin| {
        if bin.sample_count == 0 {
            return total;
        }
        let count = u128::from(bin.sample_count);
        let mean_probability = u128::from(bin.probability_sum_ppm) / count;
        let observed_frequency =
            u128::from(bin.positive_count).saturating_mul(u128::from(PPM_ONE)) / count;
        total.saturating_add(
            mean_probability
                .abs_diff(observed_frequency)
                .saturating_mul(count),
        )
    });
    Some(CalibrationReport {
        sample_count,
        mean_brier_ppm: u32::try_from(brier_sum_ppm / u128::from(sample_count)).ok()?,
        expected_calibration_error_ppm: u32::try_from(ece_numerator / u128::from(sample_count))
            .ok()?,
    })
}

pub(super) fn index_observations<'a>(
    schedule: &OutcomeSchedule,
    observations: &'a [GovernedHorizonObservation],
) -> EvaluationRuntimeResult<BTreeMap<OutcomeHorizon, &'a GovernedHorizonObservation>> {
    // 每个 T+1/T+3/T+5 只能有一条已经 due 且晚于 baseline 的观察；四资产价格面的完整性
    // 由 validate_prices 再检查，重复或缺窗口都在构造 Outcome 前失败。
    let mut indexed = BTreeMap::new();
    for observation in observations {
        if !observation
            .horizon
            .is_due_after(observation.completed_trading_sessions)
            || observation.observed_trading_day <= schedule.baseline_trading_day
        {
            return Err(EvaluationError::InvalidMaterialization(
                "horizon is not due",
            ));
        }
        validate_prices(&observation.future_prices)?;
        if indexed.insert(observation.horizon, observation).is_some() {
            return Err(EvaluationError::InvalidMaterialization(
                "duplicate observation horizon",
            ));
        }
    }
    if indexed.len() != OutcomeHorizon::ALL.len() {
        return Err(EvaluationError::InvalidMaterialization(
            "missing observation horizon",
        ));
    }
    Ok(indexed)
}

pub(super) fn validate_prices(
    prices: &BTreeMap<Asset, MoneyMicros>,
) -> EvaluationRuntimeResult<()> {
    // 价格面必须恰好覆盖四只可执行 ETF 且每个价格为正；多余或缺失资产都不可用于指标。
    if prices.len() != Asset::EXECUTABLE.len()
        || Asset::EXECUTABLE
            .into_iter()
            .any(|asset| prices.get(&asset).is_none_or(|price| price.0 <= 0))
    {
        return Err(EvaluationError::InvalidMaterialization(
            "price surface must contain positive prices for the exact universe",
        ));
    }
    Ok(())
}

pub(super) fn price(
    prices: &BTreeMap<Asset, MoneyMicros>,
    asset: Asset,
) -> EvaluationRuntimeResult<MoneyMicros> {
    prices
        .get(&asset)
        .copied()
        .ok_or(EvaluationError::InvalidMaterialization(
            "price surface is incomplete",
        ))
}

pub(super) fn return_ppm(
    baseline: MoneyMicros,
    future: MoneyMicros,
) -> EvaluationRuntimeResult<i64> {
    // 价格收益按 (future-baseline)/baseline × 1_000_000 计算；用 i128 中间值后再检查 i64。
    if baseline.0 <= 0 || future.0 <= 0 {
        return Err(EvaluationError::InvalidMaterialization(
            "prices must be positive",
        ));
    }
    i64::try_from(
        (i128::from(future.0) - i128::from(baseline.0)) * i128::from(PPM_ONE)
            / i128::from(baseline.0),
    )
    .map_err(|_| EvaluationError::ArithmeticOverflow)
}

pub(super) fn portfolio_return_ppm(
    target: &TargetPortfolio,
    baseline: &BTreeMap<Asset, MoneyMicros>,
    future: &BTreeMap<Asset, MoneyMicros>,
) -> EvaluationRuntimeResult<i64> {
    // 先算每个资产收益，再按目标 WeightPpm 加权；这是 portfolio return，不是资产概率的
    // 加权概率，权重总和和资产全集由 TargetPortfolio/domain 校验负责。
    let weighted = target
        .weights
        .iter()
        .try_fold(0_i128, |sum, (asset, weight)| {
            let asset_return = return_ppm(price(baseline, *asset)?, price(future, *asset)?)?;
            sum.checked_add(i128::from(weight.0) * i128::from(asset_return))
                .ok_or(EvaluationError::ArithmeticOverflow)
        })?;
    i64::try_from(weighted / i128::from(PPM_ONE)).map_err(|_| EvaluationError::ArithmeticOverflow)
}

pub(super) fn bounded_ratio_ppm(expected: u64, observed: u64) -> u32 {
    if expected == 0 {
        return 0;
    }
    let numerator = u128::from(observed.min(expected)) * u128::from(PPM_ONE);
    u32::try_from(numerator / u128::from(expected)).unwrap_or(PPM_ONE)
}

pub(super) fn counted_ratio(expected: u64, observed: Option<u64>) -> Option<CountedRatio> {
    // observed 缺失或超过 expected 时返回 None；有效比例同时保存 Wilson 95% 下界，供
    // 证据完整度/风险召回的审计使用，而不是只留下一个百分比。
    let observed = observed?;
    if expected == 0 || observed > expected {
        return None;
    }
    let ratio_ppm = bounded_ratio_ppm(expected, observed);
    Some(CountedRatio {
        expected_count: expected,
        observed_count: observed,
        ratio_ppm,
        lower_confidence_ppm: wilson_lower_bound_ppm(expected, observed),
    })
}

fn wilson_lower_bound_ppm(expected: u64, observed: u64) -> Option<u32> {
    if expected < 5 || observed > expected {
        return None;
    }
    let n = expected as f64;
    let p = observed as f64 / n;
    let z = 1.959_963_984_540_054_f64;
    let z2 = z * z;
    let center = p + z2 / (2.0 * n);
    let margin = z * ((p * (1.0 - p) + z2 / (4.0 * n)) / n).sqrt();
    let lower = ((center - margin) / (1.0 + z2 / n)).clamp(0.0, 1.0);
    Some((lower * f64::from(PPM_ONE)).round() as u32)
}

pub(super) fn build_nav_path(
    target: &TargetPortfolio,
    baseline: &BTreeMap<Asset, MoneyMicros>,
    observations: &[GovernedDailyObservation],
    transaction_cost_ppm: u32,
    slippage_ppm: u32,
    valuation_adjustment_ppm: i64,
) -> EvaluationRuntimeResult<Vec<OutcomeNavPoint>> {
    // NAV 从 1_000_000 ppm 起步，每个共同交易日叠加组合收益、估值调整并扣成本/滑点；
    // benchmark 同步用 QQQ 路径，任一 NAV 非正都终止该物化。
    let mut path = Vec::with_capacity(observations.len());
    let mut previous_portfolio_nav = i64::from(PPM_ONE);
    let mut previous_benchmark_nav = i64::from(PPM_ONE);
    for observation in observations {
        validate_prices(&observation.future_prices)?;
        let gross_return = portfolio_return_ppm(target, baseline, &observation.future_prices)?;
        let benchmark_return = return_ppm(
            price(baseline, Asset::Qqq)?,
            price(&observation.future_prices, Asset::Qqq)?,
        )?;
        let portfolio_nav = i64::from(PPM_ONE)
            .checked_add(gross_return)
            .and_then(|value| value.checked_add(valuation_adjustment_ppm))
            .and_then(|value| value.checked_sub(i64::from(transaction_cost_ppm)))
            .and_then(|value| value.checked_sub(i64::from(slippage_ppm)))
            .ok_or(EvaluationError::ArithmeticOverflow)?;
        let benchmark_nav = i64::from(PPM_ONE)
            .checked_add(benchmark_return)
            .ok_or(EvaluationError::ArithmeticOverflow)?;
        if portfolio_nav <= 0 || benchmark_nav <= 0 {
            return Err(EvaluationError::InvalidMaterialization("non-positive NAV"));
        }
        let portfolio_daily_return_ppm = period_return_ppm(previous_portfolio_nav, portfolio_nav)?;
        let benchmark_daily_return_ppm = period_return_ppm(previous_benchmark_nav, benchmark_nav)?;
        path.push(OutcomeNavPoint {
            observed_trading_day: observation.observed_trading_day,
            portfolio_nav_ppm: portfolio_nav,
            benchmark_nav_ppm: benchmark_nav,
            portfolio_daily_return_ppm,
            benchmark_daily_return_ppm,
        });
        previous_portfolio_nav = portfolio_nav;
        previous_benchmark_nav = benchmark_nav;
    }
    Ok(path)
}

fn period_return_ppm(previous: i64, current: i64) -> EvaluationRuntimeResult<i64> {
    if previous <= 0 || current <= 0 {
        return Err(EvaluationError::InvalidMaterialization("NAV period return"));
    }
    i64::try_from(
        (i128::from(current) - i128::from(previous)) * i128::from(PPM_ONE) / i128::from(previous),
    )
    .map_err(|_| EvaluationError::ArithmeticOverflow)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct PathRiskMetrics {
    pub maximum_drawdown_ppm: Option<u32>,
    pub tracking_error_ppm: Option<u32>,
    pub beta_ppm: Option<i64>,
    pub expected_shortfall_ppm: Option<u32>,
    pub sortino_ratio_ppm: Option<i64>,
}

pub(super) fn path_risk_metrics(path: &[OutcomeNavPoint]) -> PathRiskMetrics {
    // 最大回撤只要有一条路径就可测；tracking error/beta/Sortino 至少需要 5 个日样本，
    // expected shortfall 另需 20 个样本，样本不足的字段保持 None。
    if path.is_empty() {
        return PathRiskMetrics::default();
    }
    let mut peak = i64::from(PPM_ONE);
    let mut maximum_drawdown_ppm = 0_u32;
    for point in path {
        peak = peak.max(point.portfolio_nav_ppm);
        let drawdown = if point.portfolio_nav_ppm >= peak {
            0
        } else {
            u32::try_from(
                (i128::from(peak) - i128::from(point.portfolio_nav_ppm)) * i128::from(PPM_ONE)
                    / i128::from(peak),
            )
            .unwrap_or(PPM_ONE)
        };
        maximum_drawdown_ppm = maximum_drawdown_ppm.max(drawdown);
    }

    let mut result = PathRiskMetrics {
        maximum_drawdown_ppm: Some(maximum_drawdown_ppm),
        ..PathRiskMetrics::default()
    };
    if path.len() >= AKZIO_MIN_PATH_RISK_SAMPLES {
        let portfolio = path
            .iter()
            .map(|point| point.portfolio_daily_return_ppm as f64)
            .collect::<Vec<_>>();
        let benchmark = path
            .iter()
            .map(|point| point.benchmark_daily_return_ppm as f64)
            .collect::<Vec<_>>();
        let active = portfolio
            .iter()
            .zip(&benchmark)
            .map(|(portfolio, benchmark)| portfolio - benchmark)
            .collect::<Vec<_>>();
        result.tracking_error_ppm = standard_deviation_ppm(&active);
        result.beta_ppm = beta_ppm(&portfolio, &benchmark);
        result.sortino_ratio_ppm = sortino_ratio_ppm(&portfolio);
    }
    if path.len() >= AKZIO_MIN_EXPECTED_SHORTFALL_SAMPLES {
        let mut losses = path
            .iter()
            .map(|point| point.portfolio_daily_return_ppm.saturating_neg().max(0) as u64)
            .collect::<Vec<_>>();
        losses.sort_unstable_by(|left, right| right.cmp(left));
        let tail_count = losses.len().div_ceil(20).max(1);
        let total = losses
            .into_iter()
            .take(tail_count)
            .fold(0_u128, |sum, loss| sum.saturating_add(u128::from(loss)));
        result.expected_shortfall_ppm = u32::try_from(total / tail_count as u128)
            .ok()
            .map(|value| value.min(PPM_ONE));
    }
    result
}

fn standard_deviation_ppm(values: &[f64]) -> Option<u32> {
    if values.len() < 2 {
        return None;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (values.len() - 1) as f64;
    Some(variance.sqrt().round().clamp(0.0, f64::from(PPM_ONE)) as u32)
}

fn beta_ppm(portfolio: &[f64], benchmark: &[f64]) -> Option<i64> {
    if portfolio.len() != benchmark.len() || portfolio.len() < 2 {
        return None;
    }
    let portfolio_mean = portfolio.iter().sum::<f64>() / portfolio.len() as f64;
    let benchmark_mean = benchmark.iter().sum::<f64>() / benchmark.len() as f64;
    let covariance = portfolio
        .iter()
        .zip(benchmark)
        .map(|(portfolio, benchmark)| (portfolio - portfolio_mean) * (benchmark - benchmark_mean))
        .sum::<f64>();
    let benchmark_variance = benchmark
        .iter()
        .map(|benchmark| (benchmark - benchmark_mean).powi(2))
        .sum::<f64>();
    if benchmark_variance <= f64::EPSILON {
        return None;
    }
    Some((covariance / benchmark_variance * f64::from(PPM_ONE)).round() as i64)
}

fn sortino_ratio_ppm(returns: &[f64]) -> Option<i64> {
    if returns.len() < 2 {
        return None;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let downside = returns
        .iter()
        .filter(|value| **value < 0.0)
        .map(|value| value.powi(2))
        .sum::<f64>();
    if downside <= f64::EPSILON {
        return None;
    }
    let downside_deviation = (downside / returns.len() as f64).sqrt();
    Some((mean / downside_deviation * f64::from(PPM_ONE)).round() as i64)
}

pub(super) fn execution_verdict(lineage: &OutcomeExecutionLineage) -> &ArtifactRef {
    // NoOrder 与 ReconciledPaper 都必须沿用执行阶段已经生成的 verdict 引用，Outcome 不在
    // 学习层重新推断“是否下单”。
    match lineage {
        OutcomeExecutionLineage::NoOrder { execution_verdict }
        | OutcomeExecutionLineage::ReconciledPaper {
            execution_verdict, ..
        } => execution_verdict,
    }
}

pub(super) fn require_canonical_purpose(purpose: RunPurpose) -> EvaluationRuntimeResult<()> {
    // 运行 purpose 是 Store 中的权威资格；调用方不能通过传入 Paper 形状的数据替换非 Paper。
    if purpose.is_canonical_learning() {
        Ok(())
    } else {
        Err(EvaluationError::NonCanonicalPurpose(purpose))
    }
}

pub(super) fn reference(artifact: &Artifact) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: artifact.kind,
    }
}

pub(super) fn stable_id(value: &serde_json::Value) -> EvaluationRuntimeResult<String> {
    Ok(content_hash_json(value)?.as_str().to_owned())
}

pub(super) fn marginal_utility(outcome: &Outcome) -> i64 {
    // Evaluation 的 marginal utility 是所有已封存窗口 utility_ppm 的算术平均；窗口集合
    // 不完整时上游 sealed 校验先失败，这里不用单独窗口冒充 T+5。
    let total = outcome
        .windows
        .iter()
        .fold(0_i128, |sum, window| sum + i128::from(window.utility_ppm));
    let average = total / i128::try_from(outcome.windows.len()).unwrap_or(1);
    average.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

/// Promote memory only after a fresh T+1/T+3/T+5 evaluation passes quality
/// gates; contract/topology promotion remains owned by their canary policy.
///
/// `risk_recall_measured` is a hard precondition for any forward transition.
/// When risk recall was never measured the subject holds its current state:
/// unmeasured evidence must not be readable as either a pass or a failure.
pub(super) fn next_state_with_fresh_pairs(
    current: PolicyState,
    target: Option<PolicyState>,
    degraded: bool,
    risk_recall_measured: bool,
    fresh_pairs_by_horizon: [u64; 3],
    minimum_fresh_pairs_per_horizon: u64,
) -> PolicyState {
    // 先按 degradation 或目标状态算候选 next，再只允许合法的 forward transition 受资格门
    // 控制：每个 T+1/T+3/T+5 都有新 pair 且 risk recall 已测量才可晋级，缺失就保持 current。
    use CandidatePolicyState as Candidate;
    use MemoryLifecycle as Memory;

    let next = if degraded {
        match current {
            PolicyState::Memory(Memory::Contested) => PolicyState::Memory(Memory::Retired),
            PolicyState::Memory(Memory::Retired) => current,
            PolicyState::Memory(_) => PolicyState::Memory(Memory::Contested),
            PolicyState::Contract(Candidate::Candidate)
            | PolicyState::Topology(Candidate::Candidate) => current,
            PolicyState::Contract(_) => PolicyState::Contract(Candidate::Candidate),
            PolicyState::Topology(_) => PolicyState::Topology(Candidate::Candidate),
        }
    } else {
        target.unwrap_or(match current {
            PolicyState::Memory(Memory::Candidate) => PolicyState::Memory(Memory::Active),
            PolicyState::Memory(Memory::Active) => PolicyState::Memory(Memory::Proven),
            _ => current,
        })
    };

    if !is_forward_transition(current, next) {
        return next;
    }
    if !risk_recall_measured {
        return current;
    }
    if fresh_pairs_by_horizon
        .iter()
        .all(|&count| count >= minimum_fresh_pairs_per_horizon)
    {
        next
    } else {
        current
    }
}

fn is_forward_transition(from: PolicyState, to: PolicyState) -> bool {
    // 这个匹配表把 Memory/Contract/Topology 的晋级路径冻结在领域允许的相邻状态，避免
    // caller 通过传入任意 PolicyState 跳过 Candidate 或 Canary 阶段。
    use CandidatePolicyState as Candidate;
    use MemoryLifecycle as Memory;

    matches!(
        (from, to),
        (
            PolicyState::Memory(Memory::Candidate),
            PolicyState::Memory(Memory::Active)
        ) | (
            PolicyState::Memory(Memory::Active),
            PolicyState::Memory(Memory::Proven)
        ) | (
            PolicyState::Memory(Memory::Contested),
            PolicyState::Memory(Memory::Active)
        ) | (
            PolicyState::Contract(Candidate::Candidate),
            PolicyState::Contract(Candidate::Canary10)
        ) | (
            PolicyState::Contract(Candidate::Canary10),
            PolicyState::Contract(Candidate::Canary25)
        ) | (
            PolicyState::Contract(Candidate::Canary25),
            PolicyState::Contract(Candidate::Canary50)
        ) | (
            PolicyState::Contract(Candidate::Canary50),
            PolicyState::Contract(Candidate::Active)
        ) | (
            PolicyState::Topology(Candidate::Candidate),
            PolicyState::Topology(Candidate::Canary10)
        ) | (
            PolicyState::Topology(Candidate::Canary10),
            PolicyState::Topology(Candidate::Canary25)
        ) | (
            PolicyState::Topology(Candidate::Canary25),
            PolicyState::Topology(Candidate::Canary50)
        ) | (
            PolicyState::Topology(Candidate::Canary50),
            PolicyState::Topology(Candidate::Active)
        )
    )
}
