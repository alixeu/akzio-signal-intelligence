/// Deterministically derives all OutcomeWindow metrics from governed facts.
pub fn materialize_outcome(
    input: &OutcomeMaterializationInput,
) -> EvaluationRuntimeResult<Outcome> {
    input.validate_base()?;

    let forecasts = index_forecasts(&input.forecasts)?;
    let observations = index_observations(&input.schedule, &input.observations)?;
    let (execution, full_nav_path) = input.execution_and_nav()?;

    let mut windows = Vec::with_capacity(OutcomeHorizon::ALL.len());
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
