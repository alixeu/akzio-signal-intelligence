/// Materializes the currently due prefix of an outcome for T+1/T+3
/// diagnostics.  These snapshots remain RunScoped and unsealed; only the
/// complete three-window result is eligible for canonical learning.
pub fn materialize_partial_outcome(
    input: &OutcomeMaterializationInput,
) -> EvaluationRuntimeResult<Outcome> {
    input.schedule.validate()?;
    if input.schedule_artifact.kind != ArtifactKind::OutcomeSchedule {
        return Err(EvaluationError::InvalidMaterialization(
            "schedule artifact kind",
        ));
    }
    input.target.validate_universe()?;
    input.cost_model.validate()?;
    validate_prices(&input.baseline_prices)?;

    let forecasts = index_forecasts(&input.forecasts)?;
    let mut observations = BTreeMap::new();
    for observation in &input.observations {
        if !observation
            .horizon
            .is_due_after(observation.completed_trading_sessions)
            || observation.observed_trading_day <= input.schedule.baseline_trading_day
        {
            return Err(EvaluationError::InvalidMaterialization("horizon not due"));
        }
        validate_prices(&observation.future_prices)?;
        if observations
            .insert(observation.horizon, observation)
            .is_some()
        {
            return Err(EvaluationError::InvalidMaterialization(
                "duplicate observation horizon",
            ));
        }
    }
    if observations.is_empty() {
        return Err(EvaluationError::InvalidMaterialization(
            "missing due observation",
        ));
    }

    let mut market_evidence = input.market_evidence.clone();
    market_evidence.sort();
    market_evidence.dedup();
    let mut windows = Vec::with_capacity(observations.len());
    for (horizon, observation) in observations {
        let probabilities_by_asset = forecasts
            .get(&horizon)
            .expect("index_forecasts requires all horizons");
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
            .checked_sub(benchmark_return_ppm)
            .and_then(|value| value.checked_sub(i64::from(input.cost_model.transaction_cost_ppm)))
            .and_then(|value| value.checked_sub(i64::from(input.cost_model.slippage_ppm)))
            .ok_or(EvaluationError::ArithmeticOverflow)?;
        windows.push(OutcomeWindow {
            horizon,
            observed_trading_day: observation.observed_trading_day,
            portfolio_return_ppm,
            benchmark_return_ppm,
            transaction_cost_ppm: input.cost_model.transaction_cost_ppm,
            slippage_ppm: input.cost_model.slippage_ppm,
            utility_ppm,
            calibration_ppm: calibration_quality_ppm(
                probabilities_by_asset,
                &input.baseline_prices,
                &observation.future_prices,
            )?,
            evidence_completeness_ppm: bounded_ratio_ppm(
                observation.expected_evidence_count,
                observation.observed_evidence_count,
            ),
            risk_recall_ppm: observation
                .detected_risk_count
                .map(|detected| bounded_ratio_ppm(observation.expected_risk_count, detected)),
        });
    }
    windows.sort_by_key(|window| window.horizon);

    let outcome = Outcome {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        outcome_id: input.schedule.outcome_id.clone(),
        schedule: input.schedule_artifact.clone(),
        market_evidence,
        windows,
        sealed_at: None,
    };
    outcome.validate()?;
    Ok(outcome)
}
