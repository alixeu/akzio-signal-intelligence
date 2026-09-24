const NO_LLM_QQQ_WEIGHT_PPM: u32 = 500_000;
const NO_LLM_SOXX_WEIGHT_PPM: u32 = 500_000;

// 文件导读：benchmark 只对同一冻结观察路径做归因；它报告可用或不可用的原因，
// 不会把样本不足、退化参考路径或非正 NAV 静默转换成 0 收益。

pub(super) struct OutcomeBenchmarkAttributionInput<'a> {
    pub target: &'a TargetPortfolio,
    pub baseline_prices: &'a BTreeMap<Asset, MoneyMicros>,
    pub daily_observations: &'a [GovernedDailyObservation],
    pub observed_through: NaiveDate,
    pub horizon: OutcomeHorizon,
    pub portfolio_return_ppm: i64,
    pub valuation_adjustment_ppm: i64,
    pub transaction_cost_ppm: u32,
    pub slippage_ppm: u32,
}

pub(super) fn outcome_benchmark_attributions(
    input: OutcomeBenchmarkAttributionInput<'_>,
) -> EvaluationRuntimeResult<Vec<OutcomeBenchmarkAttribution>> {
    // 只截取 observed_through 之前已经治理的日频观察；portfolio_net_return 先合并
    // 估值调整并扣除真实交易成本/可扣滑点，再与各 benchmark 的净值路径比较。
    let governed_observations = input
        .daily_observations
        .iter()
        .filter(|observation| observation.observed_trading_day <= input.observed_through)
        .cloned()
        .collect::<Vec<_>>();
    let portfolio_net_return_ppm = input
        .portfolio_return_ppm
        .checked_add(input.valuation_adjustment_ppm)
        .and_then(|value| value.checked_sub(i64::from(input.transaction_cost_ppm)))
        .and_then(|value| value.checked_sub(i64::from(input.slippage_ppm)))
        .ok_or(EvaluationError::ArithmeticOverflow)?;
    let gross_portfolio_path = build_nav_path(
        input.target,
        input.baseline_prices,
        &governed_observations,
        0,
        0,
        0,
    )?;
    let qqq_path = build_nav_path(
        &benchmark_target(OutcomeBenchmark::Qqq),
        input.baseline_prices,
        &governed_observations,
        0,
        0,
        0,
    )?;

    OutcomeBenchmark::ALL
        .into_iter()
        .map(|benchmark| {
            // 每类 benchmark 有自己的最小样本数，同时不能早于当前 Outcome horizon；
            // 不满足时保留 Unavailable(reason)，这样部分窗口仍可审计而不是伪造结果。
            let required_samples = benchmark
                .minimum_samples()
                .max(u32::from(input.horizon.trading_days()));
            if governed_observations.len() < required_samples as usize {
                return unavailable_attribution(
                    benchmark,
                    OutcomeBenchmarkUnavailableReason::InsufficientGovernedSamples,
                    governed_observations.len(),
                    required_samples,
                );
            }
            if !governed_observations
                .iter()
                .any(|observation| observation.observed_trading_day == input.observed_through)
            {
                return unavailable_attribution(
                    benchmark,
                    OutcomeBenchmarkUnavailableReason::MissingObservedHorizonPath,
                    governed_observations.len(),
                    required_samples,
                );
            }

            match benchmark {
                OutcomeBenchmark::Cash => available_attribution(
                    benchmark,
                    cash_nav_path(&governed_observations),
                    portfolio_net_return_ppm,
                    None,
                ),
                OutcomeBenchmark::Qqq => available_attribution(
                    benchmark,
                    benchmark_nav_path(&qqq_path),
                    portfolio_net_return_ppm,
                    None,
                ),
                OutcomeBenchmark::Soxx
                | OutcomeBenchmark::FourAssetEqualWeight
                | OutcomeBenchmark::NoLlmDeterministic => {
                    let path = build_nav_path(
                        &benchmark_target(benchmark),
                        input.baseline_prices,
                        &governed_observations,
                        0,
                        0,
                        0,
                    )?;
                    available_attribution(
                        benchmark,
                        benchmark_nav_path(&path),
                        portfolio_net_return_ppm,
                        None,
                    )
                }
                OutcomeBenchmark::BetaMatchedQqq => {
                    // beta 缩放需要投资组合与 QQQ 日收益长度相同且参考方差非退化。
                    let Some(scale_ppm) = beta_scale_ppm(&gross_portfolio_path, &qqq_path) else {
                        return unavailable_attribution(
                            benchmark,
                            OutcomeBenchmarkUnavailableReason::DegenerateReferencePath,
                            governed_observations.len(),
                            required_samples,
                        );
                    };
                    scaled_qqq_attribution(
                        benchmark,
                        &qqq_path,
                        scale_ppm,
                        portfolio_net_return_ppm,
                        required_samples,
                    )
                }
                OutcomeBenchmark::VolatilityTargetedQqq => {
                    // 波动率缩放同样要求两个路径有足够样本和正的有限标准差；scale 只
                    // 用于构造诊断 benchmark NAV，不改变原始 portfolio 结果。
                    let Some(scale_ppm) = volatility_scale_ppm(&gross_portfolio_path, &qqq_path)
                    else {
                        return unavailable_attribution(
                            benchmark,
                            OutcomeBenchmarkUnavailableReason::DegenerateReferencePath,
                            governed_observations.len(),
                            required_samples,
                        );
                    };
                    scaled_qqq_attribution(
                        benchmark,
                        &qqq_path,
                        scale_ppm,
                        portfolio_net_return_ppm,
                        required_samples,
                    )
                }
            }
        })
        .collect()
}

fn benchmark_target(benchmark: OutcomeBenchmark) -> TargetPortfolio {
    // benchmark target 复用四资产执行全集；Cash/派生 QQQ benchmark 不在这里伪造持仓。
    let mut target = TargetPortfolio::zeroed();
    match benchmark {
        OutcomeBenchmark::Qqq => {
            target.weights.insert(Asset::Qqq, WeightPpm(PPM_ONE));
        }
        OutcomeBenchmark::Soxx => {
            target.weights.insert(Asset::Soxx, WeightPpm(PPM_ONE));
        }
        OutcomeBenchmark::FourAssetEqualWeight => {
            for asset in Asset::EXECUTABLE {
                target.weights.insert(asset, WeightPpm(PPM_ONE / 4));
            }
        }
        OutcomeBenchmark::NoLlmDeterministic => {
            target
                .weights
                .insert(Asset::Qqq, WeightPpm(NO_LLM_QQQ_WEIGHT_PPM));
            target
                .weights
                .insert(Asset::Soxx, WeightPpm(NO_LLM_SOXX_WEIGHT_PPM));
        }
        OutcomeBenchmark::Cash
        | OutcomeBenchmark::BetaMatchedQqq
        | OutcomeBenchmark::VolatilityTargetedQqq => {}
    }
    target
}

fn cash_nav_path(observations: &[GovernedDailyObservation]) -> Vec<OutcomeBenchmarkNavPoint> {
    observations
        .iter()
        .map(|observation| OutcomeBenchmarkNavPoint {
            observed_trading_day: observation.observed_trading_day,
            nav_ppm: i64::from(PPM_ONE),
            daily_return_ppm: 0,
        })
        .collect()
}

fn benchmark_nav_path(path: &[OutcomeNavPoint]) -> Vec<OutcomeBenchmarkNavPoint> {
    path.iter()
        .map(|point| OutcomeBenchmarkNavPoint {
            observed_trading_day: point.observed_trading_day,
            nav_ppm: point.portfolio_nav_ppm,
            daily_return_ppm: point.portfolio_daily_return_ppm,
        })
        .collect()
}

fn available_attribution(
    benchmark: OutcomeBenchmark,
    nav_path: Vec<OutcomeBenchmarkNavPoint>,
    portfolio_net_return_ppm: i64,
    scale_ppm: Option<i64>,
) -> EvaluationRuntimeResult<OutcomeBenchmarkAttribution> {
    // active_return = 组合净收益 - benchmark 最终 NAV 收益；definition hash 随结果保存，
    // 让同一 benchmark 口径的变化不会悄悄重解释历史 Outcome。
    let benchmark_return_ppm = nav_path
        .last()
        .ok_or(EvaluationError::InvalidMaterialization(
            "missing governed benchmark path",
        ))?
        .nav_ppm
        .checked_sub(i64::from(PPM_ONE))
        .ok_or(EvaluationError::ArithmeticOverflow)?;
    let active_return_ppm = portfolio_net_return_ppm
        .checked_sub(benchmark_return_ppm)
        .ok_or(EvaluationError::ArithmeticOverflow)?;
    Ok(OutcomeBenchmarkAttribution {
        benchmark,
        definition_version: OUTCOME_BENCHMARK_DEFINITION_VERSION,
        definition_hash: benchmark.definition_hash()?,
        result: OutcomeBenchmarkResult::Available {
            benchmark_return_ppm,
            active_return_ppm,
            scale_ppm,
            nav_path,
        },
    })
}

fn unavailable_attribution(
    benchmark: OutcomeBenchmark,
    reason: OutcomeBenchmarkUnavailableReason,
    observed_samples: usize,
    required_samples: u32,
) -> EvaluationRuntimeResult<OutcomeBenchmarkAttribution> {
    Ok(OutcomeBenchmarkAttribution {
        benchmark,
        definition_version: OUTCOME_BENCHMARK_DEFINITION_VERSION,
        definition_hash: benchmark.definition_hash()?,
        result: OutcomeBenchmarkResult::Unavailable {
            reason,
            observed_samples: u32::try_from(observed_samples).unwrap_or(u32::MAX),
            required_samples,
        },
    })
}

fn scaled_qqq_attribution(
    benchmark: OutcomeBenchmark,
    qqq_path: &[OutcomeNavPoint],
    scale_ppm: i64,
    portfolio_net_return_ppm: i64,
    required_samples: u32,
) -> EvaluationRuntimeResult<OutcomeBenchmarkAttribution> {
    // 每天按 scale 缩放 QQQ 的日收益并递推 NAV；一旦派生 NAV 非正，该 benchmark 只记为
    // unavailable，避免用无效路径算 active return。
    let mut previous_nav = i64::from(PPM_ONE);
    let mut path = Vec::with_capacity(qqq_path.len());
    for point in qqq_path {
        let scaled_daily_return = i64::try_from(
            i128::from(point.portfolio_daily_return_ppm) * i128::from(scale_ppm)
                / i128::from(PPM_ONE),
        )
        .map_err(|_| EvaluationError::ArithmeticOverflow)?;
        let nav = i64::try_from(
            i128::from(previous_nav) * i128::from(i64::from(PPM_ONE) + scaled_daily_return)
                / i128::from(PPM_ONE),
        )
        .map_err(|_| EvaluationError::ArithmeticOverflow)?;
        if nav <= 0 {
            return unavailable_attribution(
                benchmark,
                OutcomeBenchmarkUnavailableReason::NonPositiveDerivedNav,
                qqq_path.len(),
                required_samples,
            );
        }
        let daily_return_ppm = period_return(previous_nav, nav)?;
        path.push(OutcomeBenchmarkNavPoint {
            observed_trading_day: point.observed_trading_day,
            nav_ppm: nav,
            daily_return_ppm,
        });
        previous_nav = nav;
    }
    available_attribution(benchmark, path, portfolio_net_return_ppm, Some(scale_ppm))
}

fn beta_scale_ppm(portfolio: &[OutcomeNavPoint], qqq: &[OutcomeNavPoint]) -> Option<i64> {
    // beta = Cov(portfolio, QQQ) / Var(QQQ)，返回 ppm；方差为零时没有可识别的缩放因子。
    let portfolio_returns = portfolio
        .iter()
        .map(|point| point.portfolio_daily_return_ppm as f64)
        .collect::<Vec<_>>();
    let qqq_returns = qqq
        .iter()
        .map(|point| point.portfolio_daily_return_ppm as f64)
        .collect::<Vec<_>>();
    if portfolio_returns.len() != qqq_returns.len()
        || portfolio_returns.len() < OutcomeBenchmark::BetaMatchedQqq.minimum_samples() as usize
    {
        return None;
    }
    let portfolio_mean = mean(&portfolio_returns);
    let qqq_mean = mean(&qqq_returns);
    let covariance = portfolio_returns
        .iter()
        .zip(&qqq_returns)
        .map(|(portfolio, qqq)| (portfolio - portfolio_mean) * (qqq - qqq_mean))
        .sum::<f64>()
        / (portfolio_returns.len() - 1) as f64;
    let variance = qqq_returns
        .iter()
        .map(|qqq| (qqq - qqq_mean).powi(2))
        .sum::<f64>()
        / (qqq_returns.len() - 1) as f64;
    positive_scale_ppm(covariance / variance)
}

fn volatility_scale_ppm(portfolio: &[OutcomeNavPoint], qqq: &[OutcomeNavPoint]) -> Option<i64> {
    // volatility target 用 portfolio 波动率 / QQQ 波动率；标准差函数会拒绝太短或退化路径。
    let portfolio_returns = portfolio
        .iter()
        .map(|point| point.portfolio_daily_return_ppm as f64)
        .collect::<Vec<_>>();
    let qqq_returns = qqq
        .iter()
        .map(|point| point.portfolio_daily_return_ppm as f64)
        .collect::<Vec<_>>();
    if portfolio_returns.len() != qqq_returns.len()
        || portfolio_returns.len()
            < OutcomeBenchmark::VolatilityTargetedQqq.minimum_samples() as usize
    {
        return None;
    }
    let portfolio_volatility = sample_standard_deviation(&portfolio_returns)?;
    let qqq_volatility = sample_standard_deviation(&qqq_returns)?;
    positive_scale_ppm(portfolio_volatility / qqq_volatility)
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

fn sample_standard_deviation(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let mean = mean(values);
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (values.len() - 1) as f64;
    (variance.is_finite() && variance > f64::EPSILON).then_some(variance.sqrt())
}

fn positive_scale_ppm(scale: f64) -> Option<i64> {
    if !scale.is_finite() || scale <= 0.0 {
        return None;
    }
    let scaled = (scale * f64::from(PPM_ONE)).round();
    (scaled <= i64::MAX as f64).then_some(scaled as i64)
}

fn period_return(previous: i64, current: i64) -> EvaluationRuntimeResult<i64> {
    if previous <= 0 || current <= 0 {
        return Err(EvaluationError::InvalidMaterialization(
            "benchmark NAV period return",
        ));
    }
    i64::try_from(
        (i128::from(current) - i128::from(previous)) * i128::from(PPM_ONE) / i128::from(previous),
    )
    .map_err(|_| EvaluationError::ArithmeticOverflow)
}
