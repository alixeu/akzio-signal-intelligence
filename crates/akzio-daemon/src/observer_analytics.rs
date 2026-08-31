use std::collections::{BTreeMap, BTreeSet};

use akzio_domain::{
    Asset, CandidatePolicyState, MoneyMicros, Outcome, OutcomeHorizon, PolicyState, TargetPortfolio,
};
use akzio_ingest::parse_money_micros;
use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;
use serde_json::Value;

const PPM: f64 = 1_000_000.0;
const MIN_RISK_RETURNS: usize = 20;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ObserverBrokerFill {
    pub activity_id: String,
    pub broker_order_id: String,
    pub symbol: String,
    pub side: String,
    pub quantity_micros: i64,
    pub price_micros: i64,
    pub transaction_at: DateTime<Utc>,
    pub venue: Option<String>,
    pub source: &'static str,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ObserverBarPoint {
    pub timestamp: DateTime<Utc>,
    pub close_micros: i64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ObserverPortfolioAnalytics {
    pub benchmark_symbol: &'static str,
    pub lookback: &'static str,
    pub sample_count: usize,
    pub beta_ppm: Option<i64>,
    pub volatility_ppm: i64,
    pub max_drawdown_ppm: i64,
    pub var_95_micros: i64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ObserverOutcomeStatistics {
    pub horizon: OutcomeHorizon,
    pub sample_count: usize,
    pub win_rate_ppm: Option<i64>,
    pub profit_factor_ppm: Option<i64>,
    pub sharpe_ppm: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ObserverOutcomeComparisonPoint {
    pub trading_day: NaiveDate,
    pub portfolio_ppm: i64,
    pub benchmark_ppm: i64,
}

#[derive(Debug, Clone, Copy)]
struct CostBasis {
    quantity_micros: i64,
    average_price_micros: i64,
}

pub(crate) fn readiness_ppm(
    ready: bool,
    frozen: bool,
    auto_paper: bool,
    scheduler_owner_present: bool,
) -> u32 {
    if ready && !frozen && (!auto_paper || scheduler_owner_present) {
        1_000_000
    } else {
        0
    }
}

pub(crate) fn parse_fill_activities(
    value: &Value,
    broker_order_ids: &BTreeSet<String>,
) -> Result<Vec<ObserverBrokerFill>, String> {
    let activities = value
        .as_array()
        .ok_or_else(|| "Alpaca fill activities payload is not an array".to_owned())?;
    if activities.len() >= 100 {
        return Err("Alpaca fill activities reached the bounded 100-row limit".to_owned());
    }
    let mut fills = activities
        .iter()
        .filter_map(|activity| {
            let order_id = activity.get("order_id")?.as_str()?.trim();
            broker_order_ids
                .contains(order_id)
                .then_some((activity, order_id))
        })
        .map(|(activity, order_id)| {
            let required = |field: &'static str| {
                activity
                    .get(field)
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| format!("Alpaca fill activity missing {field}"))
            };
            let transaction_at = DateTime::parse_from_rfc3339(required("transaction_time")?)
                .map_err(|error| format!("Alpaca fill timestamp invalid: {error}"))?
                .with_timezone(&Utc);
            Ok(ObserverBrokerFill {
                activity_id: required("id")?.to_owned(),
                broker_order_id: order_id.to_owned(),
                symbol: required("symbol")?.to_ascii_uppercase(),
                side: required("side")?.to_ascii_lowercase(),
                quantity_micros: json_micros(
                    activity
                        .get("qty")
                        .ok_or_else(|| "Alpaca fill activity missing qty".to_owned())?,
                )?,
                price_micros: json_micros(
                    activity
                        .get("price")
                        .ok_or_else(|| "Alpaca fill activity missing price".to_owned())?,
                )?,
                transaction_at,
                venue: None,
                source: "alpaca_fill_activity",
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    fills.sort_by(|left, right| {
        left.transaction_at
            .cmp(&right.transaction_at)
            .then_with(|| left.activity_id.cmp(&right.activity_id))
    });
    Ok(fills)
}

pub(crate) fn managed_realized_pnl(
    opening_positions: &Value,
    fills: &[ObserverBrokerFill],
) -> Result<i64, String> {
    let mut basis = parse_opening_cost_basis(opening_positions)?;
    let mut realized = 0_i128;
    for fill in fills {
        if fill.quantity_micros <= 0 || fill.price_micros <= 0 {
            return Err("Alpaca fill contains a non-positive quantity or price".to_owned());
        }
        match fill.side.as_str() {
            "buy" => {
                let entry = basis.entry(fill.symbol.clone()).or_insert(CostBasis {
                    quantity_micros: 0,
                    average_price_micros: 0,
                });
                let prior_value = i128::from(entry.quantity_micros)
                    .saturating_mul(i128::from(entry.average_price_micros));
                let fill_value =
                    i128::from(fill.quantity_micros).saturating_mul(i128::from(fill.price_micros));
                entry.quantity_micros = entry
                    .quantity_micros
                    .checked_add(fill.quantity_micros)
                    .ok_or_else(|| "managed position quantity overflow".to_owned())?;
                entry.average_price_micros = i64::try_from(
                    prior_value
                        .saturating_add(fill_value)
                        .checked_div(i128::from(entry.quantity_micros))
                        .ok_or_else(|| "managed average price division failed".to_owned())?,
                )
                .map_err(|_| "managed average price overflow".to_owned())?;
            }
            "sell" => {
                let entry = basis.get_mut(&fill.symbol).ok_or_else(|| {
                    format!("opening cost basis missing for {} sell", fill.symbol)
                })?;
                if fill.quantity_micros > entry.quantity_micros {
                    return Err(format!(
                        "managed sell exceeds opening quantity for {}",
                        fill.symbol
                    ));
                }
                realized = realized.saturating_add(
                    i128::from(fill.quantity_micros)
                        .saturating_mul(i128::from(fill.price_micros - entry.average_price_micros))
                        / 1_000_000,
                );
                entry.quantity_micros -= fill.quantity_micros;
            }
            side => return Err(format!("unsupported Alpaca fill side {side}")),
        }
    }
    i64::try_from(realized).map_err(|_| "managed realized P&L overflow".to_owned())
}

pub(crate) fn parse_bar_series(value: &Value) -> Result<Vec<ObserverBarPoint>, String> {
    let bars = value
        .get("bars")
        .and_then(Value::as_array)
        .ok_or_else(|| "Alpaca bars payload is missing bars".to_owned())?;
    let mut points = bars
        .iter()
        .map(|bar| {
            let timestamp = bar
                .get("t")
                .or_else(|| bar.get("timestamp"))
                .and_then(Value::as_str)
                .ok_or_else(|| "Alpaca bar timestamp missing".to_owned())?;
            let timestamp = DateTime::parse_from_rfc3339(timestamp)
                .map_err(|error| format!("Alpaca bar timestamp invalid: {error}"))?
                .with_timezone(&Utc);
            let close_micros = json_micros(
                bar.get("c")
                    .or_else(|| bar.get("close"))
                    .ok_or_else(|| "Alpaca bar close missing".to_owned())?,
            )?;
            if close_micros <= 0 {
                return Err("Alpaca bar close is non-positive".to_owned());
            }
            Ok(ObserverBarPoint {
                timestamp,
                close_micros,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    points.sort_by_key(|point| point.timestamp);
    points.dedup_by_key(|point| point.timestamp);
    if points.is_empty() {
        return Err("Alpaca bars payload is empty".to_owned());
    }
    Ok(points)
}

pub(crate) fn benchmark_equity_series(
    portfolio: &[(DateTime<Utc>, i64)],
    benchmark: &[ObserverBarPoint],
) -> Vec<Option<i64>> {
    let Some((_, opening_equity)) = portfolio.first().copied() else {
        return Vec::new();
    };
    let Some(first_close) = benchmark.first().map(|point| point.close_micros) else {
        return vec![None; portfolio.len()];
    };
    let mut benchmark_index = 0;
    portfolio
        .iter()
        .map(|(timestamp, _)| {
            while benchmark_index + 1 < benchmark.len()
                && benchmark[benchmark_index + 1].timestamp <= *timestamp
            {
                benchmark_index += 1;
            }
            let close = benchmark
                .get(benchmark_index)
                .filter(|point| point.timestamp <= *timestamp)?
                .close_micros;
            i64::try_from(
                i128::from(opening_equity)
                    .saturating_mul(i128::from(close))
                    .checked_div(i128::from(first_close))?,
            )
            .ok()
        })
        .collect()
}

pub(crate) fn portfolio_analytics(
    portfolio: &[(DateTime<Utc>, i64)],
    benchmark: &[ObserverBarPoint],
    current_equity_micros: i64,
) -> Result<ObserverPortfolioAnalytics, String> {
    let portfolio_by_day = daily_last(portfolio.iter().copied());
    let benchmark_by_day = daily_last(
        benchmark
            .iter()
            .map(|point| (point.timestamp, point.close_micros)),
    );
    let aligned = portfolio_by_day
        .iter()
        .filter_map(|(day, equity)| benchmark_by_day.get(day).map(|close| (*equity, *close)))
        .collect::<Vec<_>>();
    let mut portfolio_returns = Vec::new();
    let mut benchmark_returns = Vec::new();
    for pair in aligned.windows(2) {
        let ((prior_equity, prior_benchmark), (equity, benchmark)) = (pair[0], pair[1]);
        if prior_equity <= 0 || prior_benchmark <= 0 {
            return Err("portfolio analytics contains a non-positive baseline".to_owned());
        }
        portfolio_returns.push(equity as f64 / prior_equity as f64 - 1.0);
        benchmark_returns.push(benchmark as f64 / prior_benchmark as f64 - 1.0);
    }
    if portfolio_returns.len() < MIN_RISK_RETURNS {
        return Err(format!(
            "portfolio analytics needs at least {MIN_RISK_RETURNS} aligned daily returns"
        ));
    }
    let portfolio_mean = mean(&portfolio_returns);
    let benchmark_mean = mean(&benchmark_returns);
    let benchmark_variance = sample_variance(&benchmark_returns, benchmark_mean);
    let beta_ppm = (benchmark_variance > 0.0).then(|| {
        let covariance = portfolio_returns
            .iter()
            .zip(&benchmark_returns)
            .map(|(portfolio, benchmark)| {
                (portfolio - portfolio_mean) * (benchmark - benchmark_mean)
            })
            .sum::<f64>()
            / (portfolio_returns.len() - 1) as f64;
        rounded_ppm(covariance / benchmark_variance)
    });
    let volatility_ppm =
        rounded_ppm(sample_variance(&portfolio_returns, portfolio_mean).sqrt() * 252_f64.sqrt());
    let max_drawdown_ppm = max_drawdown_ppm(aligned.iter().map(|(equity, _)| *equity));
    let mut sorted_returns = portfolio_returns.clone();
    sorted_returns.sort_by(f64::total_cmp);
    let percentile_index = ((sorted_returns.len() - 1) as f64 * 0.05).floor() as usize;
    let var_return = sorted_returns[percentile_index].min(0.0).abs();
    let var_95_micros = (current_equity_micros as f64 * var_return)
        .round()
        .clamp(0.0, i64::MAX as f64) as i64;
    Ok(ObserverPortfolioAnalytics {
        benchmark_symbol: "QQQ",
        lookback: "3m",
        sample_count: portfolio_returns.len(),
        beta_ppm,
        volatility_ppm,
        max_drawdown_ppm,
        var_95_micros,
    })
}

pub(crate) fn outcome_statistics(outcomes: &[Outcome]) -> Vec<ObserverOutcomeStatistics> {
    OutcomeHorizon::ALL
        .into_iter()
        .map(|horizon| {
            let values = outcomes
                .iter()
                .filter_map(|outcome| {
                    outcome
                        .windows
                        .iter()
                        .find(|window| window.horizon == horizon)
                        .map(|window| window.utility_ppm)
                })
                .collect::<Vec<_>>();
            let win_rate_ppm = (!values.is_empty()).then(|| {
                i64::try_from(
                    values.iter().filter(|value| **value > 0).count() * 1_000_000 / values.len(),
                )
                .unwrap_or(1_000_000)
            });
            let positive = values
                .iter()
                .filter(|value| **value > 0)
                .map(|value| i128::from(*value))
                .sum::<i128>();
            let negative = values
                .iter()
                .filter(|value| **value < 0)
                .map(|value| i128::from(*value).abs())
                .sum::<i128>();
            let profit_factor_ppm = (positive > 0 && negative > 0)
                .then(|| positive.saturating_mul(1_000_000) / negative)
                .and_then(|value| i64::try_from(value).ok());
            let sharpe_ppm = (values.len() >= 20)
                .then(|| {
                    let values = values
                        .iter()
                        .map(|value| *value as f64 / PPM)
                        .collect::<Vec<_>>();
                    let average = mean(&values);
                    let deviation = sample_variance(&values, average).sqrt();
                    (deviation > 0.0).then(|| {
                        rounded_ppm(
                            average / deviation
                                * (252.0 / f64::from(horizon.trading_days())).sqrt(),
                        )
                    })
                })
                .flatten();
            ObserverOutcomeStatistics {
                horizon,
                sample_count: values.len(),
                win_rate_ppm,
                profit_factor_ppm,
                sharpe_ppm,
            }
        })
        .collect()
}

pub(crate) fn outcome_comparison(
    target: &TargetPortfolio,
    baseline_prices: &BTreeMap<Asset, MoneyMicros>,
    bars_by_asset: &BTreeMap<Asset, BTreeMap<NaiveDate, MoneyMicros>>,
    baseline_day: NaiveDate,
) -> Result<Vec<ObserverOutcomeComparisonPoint>, String> {
    let mut common_days = bars_by_asset
        .values()
        .next()
        .map(|bars| bars.keys().copied().collect::<BTreeSet<_>>())
        .unwrap_or_default();
    for bars in bars_by_asset.values().skip(1) {
        common_days.retain(|day| bars.contains_key(day));
    }
    common_days.retain(|day| *day > baseline_day);
    let mut points = vec![ObserverOutcomeComparisonPoint {
        trading_day: baseline_day,
        portfolio_ppm: 1_000_000,
        benchmark_ppm: 1_000_000,
    }];
    for day in common_days {
        let weighted_return = target
            .weights
            .iter()
            .try_fold(0_i128, |sum, (asset, weight)| {
                let baseline = baseline_prices
                    .get(asset)
                    .filter(|price| price.0 > 0)
                    .ok_or_else(|| {
                        format!("outcome baseline price missing for {}", asset.symbol())
                    })?;
                let future = bars_by_asset
                    .get(asset)
                    .and_then(|bars| bars.get(&day))
                    .filter(|price| price.0 > 0)
                    .ok_or_else(|| {
                        format!("outcome future price missing for {}", asset.symbol())
                    })?;
                let return_ppm = (i128::from(future.0) - i128::from(baseline.0))
                    .saturating_mul(1_000_000)
                    / i128::from(baseline.0);
                Ok::<_, String>(sum.saturating_add(return_ppm * i128::from(weight.0)))
            })?
            / 1_000_000;
        let qqq_baseline = baseline_prices
            .get(&Asset::Qqq)
            .filter(|price| price.0 > 0)
            .ok_or_else(|| "outcome QQQ baseline price missing".to_owned())?;
        let qqq_future = bars_by_asset
            .get(&Asset::Qqq)
            .and_then(|bars| bars.get(&day))
            .filter(|price| price.0 > 0)
            .ok_or_else(|| "outcome QQQ future price missing".to_owned())?;
        let benchmark_return = (i128::from(qqq_future.0) - i128::from(qqq_baseline.0))
            .saturating_mul(1_000_000)
            / i128::from(qqq_baseline.0);
        points.push(ObserverOutcomeComparisonPoint {
            trading_day: day,
            portfolio_ppm: i64::try_from(1_000_000_i128.saturating_add(weighted_return))
                .map_err(|_| "outcome portfolio comparison overflow".to_owned())?,
            benchmark_ppm: i64::try_from(1_000_000_i128.saturating_add(benchmark_return))
                .map_err(|_| "outcome benchmark comparison overflow".to_owned())?,
        });
    }
    Ok(points)
}

pub(crate) fn comparison_max_drawdown_ppm(
    points: &[ObserverOutcomeComparisonPoint],
) -> Option<i64> {
    (!points.is_empty()).then(|| max_drawdown_ppm(points.iter().map(|point| point.portfolio_ppm)))
}

pub(crate) fn compounded_ppm(values: &[i64]) -> Option<i64> {
    (!values.is_empty()).then(|| {
        let compounded = values
            .iter()
            .fold(1.0, |total, value| total * (1.0 + *value as f64 / PPM))
            - 1.0;
        rounded_ppm(compounded)
    })
}

pub(crate) const fn policy_exposure_ppm(state: PolicyState) -> Option<u32> {
    match state {
        PolicyState::Memory(_) => None,
        PolicyState::Contract(CandidatePolicyState::Active)
        | PolicyState::Topology(CandidatePolicyState::Active) => Some(1_000_000),
        PolicyState::Contract(_) | PolicyState::Topology(_) => None,
    }
}

fn parse_opening_cost_basis(value: &Value) -> Result<BTreeMap<String, CostBasis>, String> {
    value
        .as_array()
        .ok_or_else(|| "opening Paper positions payload is not an array".to_owned())?
        .iter()
        .map(|position| {
            let symbol = position
                .get("symbol")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| "opening Paper position symbol missing".to_owned())?
                .to_ascii_uppercase();
            let quantity_micros = json_micros(
                position
                    .get("qty")
                    .ok_or_else(|| "opening Paper position qty missing".to_owned())?,
            )?;
            let average_price_micros = json_micros(
                position
                    .get("avg_entry_price")
                    .ok_or_else(|| "opening Paper position average price missing".to_owned())?,
            )?;
            Ok((
                symbol,
                CostBasis {
                    quantity_micros,
                    average_price_micros,
                },
            ))
        })
        .collect()
}

fn daily_last(values: impl IntoIterator<Item = (DateTime<Utc>, i64)>) -> BTreeMap<NaiveDate, i64> {
    values
        .into_iter()
        .map(|(timestamp, value)| (timestamp.date_naive(), value))
        .collect()
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

fn sample_variance(values: &[f64], average: f64) -> f64 {
    values
        .iter()
        .map(|value| (value - average).powi(2))
        .sum::<f64>()
        / (values.len() - 1) as f64
}

fn max_drawdown_ppm(values: impl IntoIterator<Item = i64>) -> i64 {
    let mut peak = 0_i64;
    let mut drawdown = 0.0_f64;
    for value in values {
        peak = peak.max(value);
        if peak > 0 {
            drawdown = drawdown.max(1.0 - value as f64 / peak as f64);
        }
    }
    rounded_ppm(drawdown)
}

fn rounded_ppm(value: f64) -> i64 {
    (value * PPM)
        .round()
        .clamp(i64::MIN as f64, i64::MAX as f64) as i64
}

fn json_micros(value: &Value) -> Result<i64, String> {
    parse_money_micros(value)
        .map(|money| money.0)
        .ok_or_else(|| "decimal value is invalid".to_owned())
}

#[cfg(test)]
#[path = "observer_analytics/tests.rs"]
mod tests;
