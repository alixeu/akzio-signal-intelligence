/// Materializes the currently due prefix of an outcome for T+1/T+3
/// diagnostics.  These snapshots remain RunScoped and unsealed; only the
/// complete three-window result is eligible for canonical learning.
pub fn materialize_partial_outcome(
    input: &OutcomeMaterializationInput,
) -> EvaluationRuntimeResult<Outcome> {
    input.validate_base()?;

    let forecasts = index_forecasts(&input.forecasts)?;
    let (execution, full_nav_path) = input.execution_and_nav()?;
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

    let mut windows = Vec::with_capacity(observations.len());
    for (horizon, observation) in observations {
        let probabilities_by_asset = forecasts
            .get(&horizon)
            .expect("index_forecasts requires all horizons");
        windows.push(materialize_outcome_window(
            horizon,
            observation,
            input,
            probabilities_by_asset,
            &execution,
            &full_nav_path,
        )?);
    }
    windows.sort_by_key(|window| window.horizon);

    let outcome = input.outcome_snapshot(&execution, windows, None);
    outcome.validate()?;
    Ok(outcome)
}
