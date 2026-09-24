/// Materializes the currently due prefix of an outcome for T+1/T+3
/// diagnostics.  These snapshots remain RunScoped and unsealed; only the
/// complete three-window result is eligible for canonical learning.
pub fn materialize_partial_outcome(
    input: &OutcomeMaterializationInput,
) -> EvaluationRuntimeResult<Outcome> {
    // Partial 只取当前已经到期的 T+1 或 T+3 前缀；它是 RunScoped 诊断快照，不是
    // sealed Outcome，也不会单独满足 T+5 learning 资格。
    input.validate_base()?;

    let forecasts = index_forecasts(&input.forecasts)?;
    let (execution, full_nav_path) = input.execution_and_nav()?;
    let mut observations = BTreeMap::new();
    for observation in &input.observations {
        // completed_trading_sessions 以四资产共同 Session 计数；自然日或单资产新价格
        // 都不能让一个 horizon 提前到期，重复 horizon 也必须显式失败。
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

    // Store 的 partial 写入路径还会检查 T1=1 个窗口、T3=完整 T1/T3 前缀以及 RunScoped
    // 生命周期；这里的 validate() 只确认当前未封存快照自身可解码。
    let outcome = input.outcome_snapshot(&execution, windows, None);
    outcome.validate()?;
    Ok(outcome)
}
