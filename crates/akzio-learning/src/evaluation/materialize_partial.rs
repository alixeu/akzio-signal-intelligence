// 文件导读：本模块由 evaluation.rs 通过 include! 编入 Evaluation 模块，构造未封存的
// 诊断 Outcome 值；T+1/T+3 前缀形状和 RunScoped 生命周期由后续 Store 写入入口强制，
// 本纯函数自身不限制传入 observations 必须恰为 T1/T3，也不推进 Policy。

/// Materializes the supplied due observations into an unsealed Outcome value.
/// The fenced partial-commit API, not this function, enforces the T1/T3 prefix
/// and RunScoped lifecycle. A separate full sealed path gates learning.
// 输入以共享借用读取，不会消耗调用者持有的 schedule、价格或预测；返回值是新构造的
// 未封存 Outcome。各项缺失或违反到期规则时通过 EvaluationError 提前返回，不产生 Store 写入；
// RunScoped 生命周期由后续 Artifact 构造和 Store 提交决定，本纯函数只构造 Outcome 值。
pub fn materialize_partial_outcome(
    input: &OutcomeMaterializationInput,
) -> EvaluationRuntimeResult<Outcome> {
    // 逐条检查传入观察已到期、价格完整，再构造未封存值；不会自动筛掉 T5
    // 或补齐缺失前缀。Store 的 partial 事务才检查阶段与 RunScoped 形状。
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
