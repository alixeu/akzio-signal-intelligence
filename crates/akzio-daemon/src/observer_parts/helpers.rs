use super::*;

pub(super) fn observer_run_telemetry(trajectory: &[TrajectoryEntry]) -> ObserverRunTelemetry {
    ObserverRunTelemetry {
        model_id: trajectory
            .iter()
            .rev()
            .find_map(|entry| entry.model.as_ref()?.model_id.clone()),
        latency_millis: trajectory
            .iter()
            .rev()
            .find_map(|entry| entry.latency_millis),
        input_tokens: trajectory
            .iter()
            .filter_map(|entry| entry.input_tokens)
            .try_fold(0_u64, u64::checked_add),
        output_tokens: trajectory
            .iter()
            .filter_map(|entry| entry.output_tokens)
            .try_fold(0_u64, u64::checked_add),
        tool_calls: trajectory
            .iter()
            .filter(|entry| entry.tool.is_some() && entry.event_type.contains("called"))
            .count(),
        turns: trajectory
            .iter()
            .filter(|entry| entry.turn.is_some() && entry.model.is_some())
            .count(),
    }
}

pub(super) fn observer_broker_order_ids(run: &ObserverRunDetail) -> BTreeSet<String> {
    run.artifacts
        .iter()
        .filter(|artifact| artifact.kind == ArtifactKind::OrderReceipt)
        .filter_map(|artifact| {
            artifact
                .payload
                .get("broker_order_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .collect()
}

pub(super) fn parse_portfolio(
    account: &Value,
    positions: &Value,
    broker_session: &str,
    market_open: bool,
) -> Result<ObserverPortfolio> {
    let equity = provider_money(account, "equity")?.0;
    let buying_power = provider_money(account, "buying_power")?.0;
    let last_equity = account
        .get("last_equity")
        .and_then(parse_money_micros)
        .map(|value| value.0);
    let day_pnl = last_equity.and_then(|previous| equity.checked_sub(previous));
    let day_pnl_ppm = day_pnl.zip(last_equity).and_then(|(pnl, previous)| {
        (previous != 0)
            .then(|| i128::from(pnl) * 1_000_000 / i128::from(previous))
            .and_then(|value| i64::try_from(value).ok())
    });
    let positions = positions
        .as_array()
        .ok_or_else(|| {
            DaemonError::InvalidInput("Paper positions payload is not an array".to_owned())
        })?
        .iter()
        .map(|position| {
            let symbol = position
                .get("symbol")
                .and_then(Value::as_str)
                .filter(|symbol| !symbol.trim().is_empty())
                .ok_or_else(|| {
                    DaemonError::InvalidInput("Paper position symbol missing".to_owned())
                })?;
            Ok(ObserverPosition {
                symbol: symbol.to_owned(),
                quantity_micros: observer_number_micros(position, "qty")?,
                market_value_micros: provider_money(position, "market_value")?.0,
                average_entry_price_micros: observer_optional_micros(position, "avg_entry_price"),
                unrealized_pnl_micros: observer_optional_micros(position, "unrealized_pl"),
                unrealized_pnl_ppm: observer_optional_micros(position, "unrealized_plpc"),
                sparkline_ppm: Vec::new(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ObserverPortfolio {
        broker_session: broker_session.to_owned(),
        market_open,
        status: account
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned(),
        equity_micros: equity,
        last_equity_micros: last_equity,
        buying_power_micros: buying_power,
        day_pnl_micros: day_pnl,
        day_pnl_ppm,
        realized_pnl_micros: None,
        realized_pnl_ppm: None,
        fills: ObserverSection::pending("No managed fill projection was requested"),
        analytics: ObserverSection::pending("Portfolio analytics are loading"),
        positions,
    })
}

pub(super) fn parse_portfolio_history(
    range: ObserverPortfolioRange,
    value: &Value,
) -> Result<ObserverPortfolioHistory> {
    let timestamps = value
        .get("timestamp")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            DaemonError::InvalidInput("Portfolio history timestamps missing".to_owned())
        })?;
    let equity = value
        .get("equity")
        .and_then(Value::as_array)
        .ok_or_else(|| DaemonError::InvalidInput("Portfolio history equity missing".to_owned()))?;
    let profit_loss = value.get("profit_loss").and_then(Value::as_array);
    let profit_loss_pct = value.get("profit_loss_pct").and_then(Value::as_array);
    let mut points = Vec::with_capacity(timestamps.len().min(equity.len()));
    for index in 0..timestamps.len().min(equity.len()) {
        let timestamp = timestamps[index]
            .as_i64()
            .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
            .ok_or_else(|| {
                DaemonError::InvalidInput("Portfolio history timestamp invalid".to_owned())
            })?;
        let equity_micros = parse_money_micros(&equity[index])
            .ok_or_else(|| {
                DaemonError::InvalidInput("Portfolio history equity invalid".to_owned())
            })?
            .0;
        points.push(ObserverPortfolioHistoryPoint {
            timestamp,
            equity_micros,
            profit_loss_micros: profit_loss
                .and_then(|values| values.get(index))
                .and_then(parse_money_micros)
                .map(|value| value.0),
            profit_loss_ppm: profit_loss_pct
                .and_then(|values| values.get(index))
                .and_then(parse_money_micros)
                .map(|value| value.0),
            benchmark_equity_micros: None,
        });
    }
    if points.is_empty() {
        return Err(DaemonError::Unavailable(
            "Portfolio history returned no observations".to_owned(),
        ));
    }
    Ok(ObserverPortfolioHistory {
        range,
        benchmark_symbol: "QQQ",
        points,
    })
}

pub(super) fn outcome_average_utility(outcome: &Outcome) -> i64 {
    if outcome.windows.is_empty() {
        return 0;
    }
    let total = outcome.windows.iter().fold(0_i128, |sum, window| {
        sum.saturating_add(i128::from(window.utility_ppm))
    });
    i64::try_from(total / outcome.windows.len() as i128).unwrap_or_else(|_| {
        if total.is_negative() {
            i64::MIN
        } else {
            i64::MAX
        }
    })
}

pub(super) fn observer_number_micros(value: &Value, field: &str) -> Result<i64> {
    value
        .get(field)
        .and_then(parse_money_micros)
        .map(|value| value.0)
        .ok_or_else(|| DaemonError::InvalidInput(format!("Paper provider field {field} invalid")))
}

pub(super) fn observer_optional_micros(value: &Value, field: &str) -> Option<i64> {
    value
        .get(field)
        .and_then(parse_money_micros)
        .map(|value| value.0)
}
