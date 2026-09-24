/// Deterministically derives all OutcomeWindow metrics from governed facts.
pub fn materialize_outcome(
    input: &OutcomeMaterializationInput,
) -> EvaluationRuntimeResult<Outcome> {
    // 完整入口要求 T+1、T+3、T+5 三个窗口都能从同一 schedule/forecast/观察集合得到；
    // 只有 validate_sealed 通过后，结果才具备进入 canonical learning 的形态。
    input.validate_base()?;

    let forecasts = index_forecasts(&input.forecasts)?;
    let observations = index_observations(&input.schedule, &input.observations)?;
    let (execution, full_nav_path) = input.execution_and_nav()?;

    let mut windows = Vec::with_capacity(OutcomeHorizon::ALL.len());
    // 每个 horizon 独立计算收益、forecast score、证据/风险真值比例和路径指标，
    // 但共用同一执行重建与日频 NAV 路径，避免三个窗口采用不同的执行口径。
    for horizon in OutcomeHorizon::ALL {
        let probabilities_by_asset = forecasts
            .get(&horizon)
            .expect("index_forecasts requires all horizons");
        let observation = observations
            .get(&horizon)
            .expect("index_observations requires all horizons");
        windows.push(materialize_outcome_window(
            horizon,
            observation,
            input,
            probabilities_by_asset,
            &execution,
            &full_nav_path,
        )?);
    }

    let outcome = input.outcome_snapshot(&execution, windows, Some(input.sealed_at));
    outcome.validate_sealed()?;
    Ok(outcome)
}

impl OutcomeMaterializationInput {
    fn validate_base(&self) -> EvaluationRuntimeResult<()> {
        // 先验证 schedule、目标组合、成本模型和精确四资产正价格面；这是所有后续
        // 指标的输入边界，错误会在任何 Outcome Artifact 写入前返回。
        self.schedule.validate()?;
        if self.schedule_artifact.kind != ArtifactKind::OutcomeSchedule {
            return Err(EvaluationError::InvalidMaterialization(
                "schedule artifact kind",
            ));
        }
        self.target.validate_universe()?;
        self.cost_model.validate()?;
        validate_prices(&self.baseline_prices)?;
        Ok(())
    }

    fn execution_and_nav(
        &self,
    ) -> EvaluationRuntimeResult<(ObservedExecutionMetrics, Vec<OutcomeNavPoint>)> {
        // 没有实际执行度量时使用成本模型的诊断默认值；有真实执行度量时保留 fill-driven
        // turnover、signed valuation effect 与唯一可扣 slippage，再构造冻结后敞口 NAV。
        let execution = self
            .observed_execution
            .clone()
            .unwrap_or(ObservedExecutionMetrics {
                turnover_ppm: 0,
                transaction_cost_ppm: self.cost_model.transaction_cost_ppm,
                implementation_shortfall_ppm: self.cost_model.slippage_ppm,
                order_cost_attributions: Vec::new(),
                ..ObservedExecutionMetrics::default()
            });
        let full_nav_path = build_nav_path(
            &self.target,
            &self.baseline_prices,
            &self.daily_observations,
            execution.transaction_cost_ppm,
            execution.deductible_slippage_ppm(),
            execution.valuation_adjustment_ppm()?,
        )?;
        Ok((execution, full_nav_path))
    }

    fn outcome_snapshot(
        &self,
        execution: &ObservedExecutionMetrics,
        windows: Vec<OutcomeWindow>,
        sealed_at: Option<DateTime<Utc>>,
    ) -> Outcome {
        // implementation_effect 的存在区分 frozen_post_execution_exposure 的 v3
        // 与兼容入口的 v2 口径；market_evidence 去重排序只影响确定性输出，不改变事实。
        let mut market_evidence = self.market_evidence.clone();
        market_evidence.sort();
        market_evidence.dedup();
        Outcome {
            schema_version: DOMAIN_SCHEMA_VERSION,
            outcome_id: self.schedule.outcome_id.clone(),
            schedule: self.schedule_artifact.clone(),
            market_evidence,
            metric_basis: Some(if execution.implementation_effect_ppm.is_some() {
                akzio_domain::OutcomeMetricBasis::FrozenPostExecutionExposureV3
            } else {
                akzio_domain::OutcomeMetricBasis::FrozenPostExecutionExposureV2
            }),
            windows,
            sealed_at,
        }
    }
}

fn materialize_outcome_window(
    horizon: OutcomeHorizon,
    observation: &GovernedHorizonObservation,
    input: &OutcomeMaterializationInput,
    probabilities_by_asset: &BTreeMap<Asset, u32>,
    execution: &ObservedExecutionMetrics,
    full_nav_path: &[OutcomeNavPoint],
) -> EvaluationRuntimeResult<OutcomeWindow> {
    // utility 是组合收益加估值调整，再扣 QQQ benchmark、交易成本和可扣滑点；
    // limit shortfall 仅作为 order attribution 诊断，不在这里重复扣除。
    let portfolio_return_ppm = portfolio_return_ppm(
        &input.target,
        &input.baseline_prices,
        &observation.future_prices,
    )?;
    let benchmark_return_ppm = return_ppm(
        price(&input.baseline_prices, Asset::Qqq)?,
        price(&observation.future_prices, Asset::Qqq)?,
    )?;
    let utility_ppm = portfolio_return_ppm
        .checked_add(execution.valuation_adjustment_ppm()?)
        .and_then(|value| value.checked_sub(benchmark_return_ppm))
        .and_then(|value| value.checked_sub(i64::from(execution.transaction_cost_ppm)))
        .and_then(|value| value.checked_sub(i64::from(execution.deductible_slippage_ppm())))
        .ok_or(EvaluationError::ArithmeticOverflow)?;
    let nav_path = full_nav_path
        .iter()
        .copied()
        .filter(|point| point.observed_trading_day <= observation.observed_trading_day)
        .collect::<Vec<_>>();
    // 当前窗口只看截至 observed_trading_day 的 NAV 前缀；后面的日线不能泄漏到较早
    // 的 T+1/T+3 指标中。
    let benchmark_attributions =
        outcome_benchmark_attributions(OutcomeBenchmarkAttributionInput {
            target: &input.target,
            baseline_prices: &input.baseline_prices,
            daily_observations: &input.daily_observations,
            observed_through: observation.observed_trading_day,
            horizon,
            portfolio_return_ppm,
            valuation_adjustment_ppm: execution.valuation_adjustment_ppm()?,
            transaction_cost_ppm: execution.transaction_cost_ppm,
            slippage_ppm: execution.deductible_slippage_ppm(),
        })?;
    let path_risk = path_risk_metrics(&nav_path);
    let evidence_counts = counted_ratio(
        observation.expected_evidence_count,
        Some(observation.observed_evidence_count),
    );
    // evidence/risk ratio 保留 expected 与 observed 的计数和 Wilson 下界；缺少外部
    // RiskGroundTruthAssessment 时 risk_recall 保持 None，不能默认成满分。
    let (risk_recall_counts, risk_ground_truth) = match &observation.risk_recall {
        Some(measurement) => {
            measurement.validate()?;
            (
                counted_ratio(
                    measurement.expected_risk_ids.len() as u64,
                    Some(measurement.detected_risk_ids.len() as u64),
                ),
                Some(measurement.assessment.clone()),
            )
        }
        None => (None, None),
    };
    Ok(OutcomeWindow {
        implementation_effect_ppm: execution.implementation_effect_ppm,
        initial_valuation_effect_ppm: execution.initial_valuation_effect_ppm,
        horizon,
        observed_trading_day: observation.observed_trading_day,
        portfolio_return_ppm,
        benchmark_return_ppm,
        transaction_cost_ppm: execution.transaction_cost_ppm,
        slippage_ppm: execution.deductible_slippage_ppm(),
        utility_ppm,
        calibration_ppm: None,
        forecast_score: forecast_score(
            probabilities_by_asset,
            &input.baseline_prices,
            &observation.future_prices,
        )?,
        evidence_completeness_ppm: evidence_counts.map(|counts| counts.ratio_ppm),
        risk_recall_ppm: risk_recall_counts.map(|counts| counts.ratio_ppm),
        evidence_counts,
        risk_recall_counts,
        risk_ground_truth,
        turnover_ppm: input
            .observed_execution
            .as_ref()
            .map(|_| execution.turnover_ppm),
        maximum_drawdown_ppm: path_risk.maximum_drawdown_ppm,
        tracking_error_ppm: path_risk.tracking_error_ppm,
        beta_ppm: path_risk.beta_ppm,
        expected_shortfall_ppm: path_risk.expected_shortfall_ppm,
        sortino_ratio_ppm: path_risk.sortino_ratio_ppm,
        nav_path,
        benchmark_attributions,
        order_cost_attributions: execution.order_cost_attributions.clone(),
    })
}
