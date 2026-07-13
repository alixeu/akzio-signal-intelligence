use orchestrator_core::config_get;
use rusqlite::Connection;
use serde_json::{json, Value};
use std::collections::BTreeMap;

use super::config::AllocationConfig;

pub(crate) fn compute_allocation_context(
    state: &Value,
    conn: &Connection,
    config: &AllocationConfig,
) -> Value {
    let tickers = state
        .get("tickers")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let investable = if config.investable_assets.is_empty() {
        tickers
            .iter()
            .filter(|t| t.as_str() != config.regime_signal)
            .cloned()
            .collect::<Vec<_>>()
    } else {
        config.investable_assets.clone()
    };

    let vix_info = query_vix_regime(
        conn,
        &config.regime_signal,
        &config.regime_thresholds,
        &config.regime_labels,
    );

    let per_ticker = investable
        .iter()
        .map(|ticker| {
            let research = state
                .get("research_plan")
                .and_then(|rp| rp.get("per_ticker"))
                .and_then(|pt| pt.get(ticker))
                .cloned()
                .unwrap_or_else(|| {
                    state
                        .get("research_plan")
                        .cloned()
                        .unwrap_or_else(|| json!({}))
                });
            let rating = research
                .get("rating")
                .and_then(Value::as_str)
                .or_else(|| {
                    state
                        .get("research_plan")
                        .and_then(|rp| rp.get("rating"))
                        .and_then(Value::as_str)
                })
                .unwrap_or("Hold");
            let long_prob = research
                .get("long_probability")
                .and_then(Value::as_f64)
                .or_else(|| {
                    state
                        .get("research_plan")
                        .and_then(|rp| rp.get("long_probability"))
                        .and_then(Value::as_f64)
                })
                .unwrap_or(0.5);
            let vol_pct =
                query_latest_indicator(conn, ticker, &config.vol_indicator).unwrap_or(0.0);
            let thesis = research
                .get("plan")
                .and_then(Value::as_str)
                .or_else(|| {
                    state
                        .get("research_plan")
                        .and_then(|rp| rp.get("plan"))
                        .and_then(Value::as_str)
                })
                .unwrap_or("");
            (
                ticker.clone(),
                json!({
                    "rating": rating,
                    "long_probability": long_prob,
                    "vol_pct": vol_pct,
                    "thesis": thesis
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();

    let correlation_60d = if investable.len() >= 2 {
        query_correlation(
            conn,
            &investable[0],
            &investable[1],
            config.correlation_window_days,
        )
    } else {
        None
    };
    let correlation_warning = match correlation_60d {
        Some(corr) if corr > 0.85 => "高度相关, 需控制集中度",
        Some(_) => "相关性适中",
        None => "相关性数据不足",
    };

    json!({
        "investable_assets": investable,
        "vix": vix_info,
        "per_ticker": per_ticker,
        "research_plan": state.get("research_plan").cloned().unwrap_or(Value::Null),
        "trader_plan": state.get("trader_investment_plan").cloned().unwrap_or(Value::Null),
        "risk_debate_state": state.get("risk_debate_state").cloned().unwrap_or(Value::Null),
        "final_trade_decision": state.get("final_trade_decision").cloned().unwrap_or(Value::Null),
        "correlation_60d": correlation_60d,
        "correlation_warning": correlation_warning,
        "max_single_position": config.max_single_position
    })
}

pub(crate) fn normalize_allocation(
    raw: &Value,
    context: &Value,
    config: &AllocationConfig,
) -> Value {
    let investable = context
        .get("investable_assets")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let allowed_keys: Vec<&str> = investable
        .iter()
        .map(String::as_str)
        .chain(std::iter::once("cash_hedge"))
        .collect();

    let raw_weights = raw
        .get("weights")
        .or_else(|| raw.get("allocation"))
        .cloned()
        .unwrap_or_else(|| json!({}));

    let mut weights: BTreeMap<String, f64> = BTreeMap::new();
    let mut rationales: BTreeMap<String, String> = BTreeMap::new();

    if let Some(obj) = raw_weights.as_object() {
        for (key, val) in obj {
            if !allowed_keys.iter().any(|k| k == key) {
                continue;
            }
            let weight = if let Some(w) = val.as_f64() {
                w
            } else if let Some(obj) = val.as_object() {
                obj.get("weight").and_then(Value::as_f64).unwrap_or(0.0)
            } else {
                0.0
            };
            let rationale = val
                .as_object()
                .and_then(|o| o.get("rationale"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if weight > 0.0 {
                weights.insert(key.clone(), weight);
                rationales.insert(key.clone(), rationale);
            }
        }
    }

    if weights.is_empty() {
        return fallback_inverse_vol(context, config, "llm_output_empty");
    }

    for w in weights.values_mut() {
        if *w < 0.0 {
            *w = 0.0;
        }
    }

    let total: f64 = weights.values().sum();
    if total <= 0.0 {
        return fallback_inverse_vol(context, config, "total_zero");
    }
    if (total - 1.0).abs() > 0.001 {
        for w in weights.values_mut() {
            *w /= total;
        }
    }

    let max_pos = config.max_single_position;
    let mut excess = 0.0;
    for ticker in &investable {
        if let Some(w) = weights.get_mut(ticker) {
            if *w > max_pos {
                excess += *w - max_pos;
                *w = max_pos;
            }
        }
    }
    if excess > 0.0 {
        *weights.entry("cash_hedge".to_string()).or_insert(0.0) += excess;
    }

    let weights_json: BTreeMap<String, Value> = weights
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                json!({
                    "weight": (*v * 10_000.0).round() / 10_000.0,
                    "rationale": rationales.get(k).cloned().unwrap_or_default()
                }),
            )
        })
        .collect();

    let total_equity: f64 = investable.iter().filter_map(|t| weights.get(t)).sum();

    json!({
        "weights": weights_json,
        "total_equity_exposure": (total_equity * 10_000.0).round() / 10_000.0,
        "vix_regime": raw.get("vix_regime").cloned()
            .or_else(|| context.get("vix").and_then(|v| v.get("regime")).cloned())
            .unwrap_or_else(|| json!("unknown")),
        "correlation_note": raw.get("correlation_note").cloned()
            .or_else(|| context.get("correlation_warning").cloned())
            .unwrap_or_else(|| json!("")),
        "summary": raw.get("summary").and_then(Value::as_str).unwrap_or(""),
        "allocation_method": "llm"
    })
}

fn fallback_inverse_vol(context: &Value, config: &AllocationConfig, reason: &str) -> Value {
    let investable = context
        .get("investable_assets")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let vix_regime = context
        .get("vix")
        .and_then(|v| v.get("regime"))
        .and_then(Value::as_str)
        .unwrap_or("normal");
    let equity_budget = match vix_regime {
        "risk_on" => 0.95,
        "normal" => 0.80,
        "elevated" => 0.60,
        "defensive" => 0.30,
        _ => 0.70,
    };

    let vols: Vec<(String, f64)> = investable
        .iter()
        .map(|t| {
            let vol = context
                .get("per_ticker")
                .and_then(|pt| pt.get(t))
                .and_then(|v| v.get("vol_pct"))
                .and_then(Value::as_f64)
                .unwrap_or(0.02)
                .max(0.001);
            (t.clone(), vol)
        })
        .collect();

    if vols.is_empty() {
        return json!({
            "weights": {
                "cash_hedge": {
                    "weight": 1.0,
                    "rationale": format!("No investable tickers available; fallback reason={}", reason)
                }
            },
            "total_equity_exposure": 0.0,
            "vix_regime": vix_regime,
            "correlation_note": context.get("correlation_warning").cloned().unwrap_or_else(|| json!("")),
            "summary": format!("Fallback cash allocation (reason: {})", reason),
            "allocation_method": "fallback_cash"
        });
    }

    let inv_vol_sum: f64 = vols.iter().map(|(_, v)| 1.0 / v).sum();
    let mut weights = BTreeMap::new();
    for (ticker, vol) in &vols {
        let raw_w = (1.0 / vol) / inv_vol_sum * equity_budget;
        let capped = raw_w.min(config.max_single_position);
        weights.insert(
            ticker.clone(),
            json!({
                "weight": (capped * 10_000.0).round() / 10_000.0,
                "rationale": format!("Inverse-vol fallback: vol={:.4}, regime={}", vol, vix_regime)
            }),
        );
    }
    let equity_actual: f64 = weights
        .values()
        .filter_map(|v| v.get("weight").and_then(Value::as_f64))
        .sum();
    let cash = 1.0 - equity_actual;
    weights.insert(
        "cash_hedge".to_string(),
        json!({
            "weight": (cash * 10_000.0).round() / 10_000.0,
            "rationale": format!("VIX regime={} → equity budget {:.0}%", vix_regime, equity_budget * 100.0)
        }),
    );

    json!({
        "weights": weights,
        "total_equity_exposure": (equity_actual * 10_000.0).round() / 10_000.0,
        "vix_regime": vix_regime,
        "correlation_note": context.get("correlation_warning").cloned().unwrap_or_else(|| json!("")),
        "summary": format!("Fallback inverse-vol allocation (reason: {})", reason),
        "allocation_method": "fallback_inverse_vol"
    })
}

fn query_vix_regime(
    conn: &Connection,
    signal: &str,
    thresholds: &[f64],
    labels: &[String],
) -> Value {
    let level = query_latest_indicator(conn, signal, "Close").unwrap_or(0.0);
    let (regime, budget_hint) = classify_regime(level, thresholds, labels);
    json!({
        "level": level,
        "regime": regime,
        "equity_budget_hint": budget_hint
    })
}

fn classify_regime(level: f64, thresholds: &[f64], labels: &[String]) -> (String, String) {
    let idx = thresholds
        .iter()
        .position(|&t| level < t)
        .unwrap_or(thresholds.len());
    let regime = labels
        .get(idx)
        .cloned()
        .unwrap_or_else(|| "defensive".to_string());
    let budget = match regime.as_str() {
        "risk_on" => "0.80-1.00",
        "normal" => "0.60-0.90",
        "elevated" => "0.30-0.70",
        "defensive" => "0.00-0.40",
        _ => "0.40-0.80",
    };
    (regime, budget.to_string())
}

fn query_latest_indicator(conn: &Connection, ticker: &str, indicator: &str) -> Option<f64> {
    conn.query_row(
        "SELECT indicator_value FROM technical_indicators
         WHERE ticker = ? AND indicator_name = ? AND interval = '1d'
         ORDER BY kline_time DESC LIMIT 1",
        rusqlite::params![ticker, indicator],
        |row| row.get::<_, f64>(0),
    )
    .ok()
}

fn query_correlation(
    conn: &Connection,
    ticker_a: &str,
    ticker_b: &str,
    window: usize,
) -> Option<f64> {
    let mut stmt = conn
        .prepare(
            "SELECT a.kline_time, a.indicator_value AS close_a, b.indicator_value AS close_b
         FROM technical_indicators a
         JOIN technical_indicators b ON a.kline_time = b.kline_time
         WHERE a.ticker = ? AND a.indicator_name = 'Close' AND a.interval = '1d'
           AND b.ticker = ? AND b.indicator_name = 'Close' AND b.interval = '1d'
         ORDER BY a.kline_time DESC LIMIT ?",
        )
        .ok()?;

    let rows = stmt
        .query_map(
            rusqlite::params![ticker_a, ticker_b, window as i64 + 1],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<f64>>(1)?,
                    row.get::<_, Option<f64>>(2)?,
                ))
            },
        )
        .ok()?
        .filter_map(|r| r.ok())
        .collect::<Vec<_>>();

    let mut closes_a: Vec<f64> = Vec::new();
    let mut closes_b: Vec<f64> = Vec::new();
    for (_, a, b) in rows.into_iter().rev() {
        if let (Some(a), Some(b)) = (a, b) {
            closes_a.push(a);
            closes_b.push(b);
        }
    }
    if closes_a.len() < 10 {
        return None;
    }

    let rets_a = log_returns(&closes_a);
    let rets_b = log_returns(&closes_b);
    pearson_correlation(&rets_a, &rets_b)
}

fn log_returns(prices: &[f64]) -> Vec<f64> {
    prices
        .windows(2)
        .filter_map(|w| {
            if w[0] > 0.0 && w[1] > 0.0 {
                Some((w[1] / w[0]).ln())
            } else {
                None
            }
        })
        .collect()
}

fn pearson_correlation(a: &[f64], b: &[f64]) -> Option<f64> {
    let n = a.len().min(b.len()) as f64;
    if n < 3.0 {
        return None;
    }
    let mean_a = a.iter().sum::<f64>() / n;
    let mean_b = b.iter().sum::<f64>() / n;
    let cov = a
        .iter()
        .zip(b.iter())
        .map(|(x, y)| (x - mean_a) * (y - mean_b))
        .sum::<f64>()
        / n;
    let std_a = (a.iter().map(|x| (x - mean_a).powi(2)).sum::<f64>() / n).sqrt();
    let std_b = (b.iter().map(|y| (y - mean_b).powi(2)).sum::<f64>() / n).sqrt();
    if std_a > 0.0 && std_b > 0.0 {
        Some((cov / (std_a * std_b) * 10_000.0).round() / 10_000.0)
    } else {
        None
    }
}

impl AllocationConfig {
    pub(crate) fn from_value(config: &Value) -> Self {
        let investable = config_get(config, "orchestrator.allocation.investable_assets")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(Value::as_str)
                    .map(ToString::to_string)
                    .collect()
            })
            .unwrap_or_default();
        let regime_signal = config_get(config, "orchestrator.allocation.regime_signal")
            .and_then(Value::as_str)
            .unwrap_or("VIX")
            .to_string();
        let regime_thresholds = config_get(config, "orchestrator.allocation.regime_thresholds")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                    })
                    .collect()
            })
            .unwrap_or_else(|| vec![15.0, 20.0, 30.0]);
        let regime_labels = config_get(config, "orchestrator.allocation.regime_labels")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(Value::as_str)
                    .map(ToString::to_string)
                    .collect()
            })
            .unwrap_or_else(|| {
                ["risk_on", "normal", "elevated", "defensive"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect()
            });
        let correlation_window =
            config_get(config, "orchestrator.allocation.correlation_window_days")
                .and_then(Value::as_i64)
                .unwrap_or(60) as usize;
        let max_single = config_get(config, "orchestrator.allocation.max_single_position")
            .and_then(Value::as_f64)
            .unwrap_or(0.70);
        let vol_indicator = config_get(config, "orchestrator.allocation.vol_indicator")
            .and_then(Value::as_str)
            .unwrap_or("STD20")
            .to_string();
        Self {
            investable_assets: investable,
            regime_signal,
            regime_thresholds,
            regime_labels,
            correlation_window_days: correlation_window,
            max_single_position: max_single,
            vol_indicator,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> AllocationConfig {
        AllocationConfig {
            investable_assets: vec!["QQQ".to_string(), "SOXX".to_string()],
            regime_signal: "VIX".to_string(),
            regime_thresholds: vec![15.0, 20.0, 30.0],
            regime_labels: vec![
                "risk_on".to_string(),
                "normal".to_string(),
                "elevated".to_string(),
                "defensive".to_string(),
            ],
            correlation_window_days: 60,
            max_single_position: 0.70,
            vol_indicator: "STD20".to_string(),
        }
    }

    fn test_context() -> Value {
        json!({
            "investable_assets": ["QQQ", "SOXX"],
            "vix": {"level": 22.0, "regime": "elevated", "equity_budget_hint": "0.30-0.70"},
            "per_ticker": {
                "QQQ": {"vol_pct": 0.01},
                "SOXX": {"vol_pct": 0.02}
            },
            "correlation_warning": "高度相关, 需控制集中度"
        })
    }

    #[test]
    fn normalize_allocation_filters_invalid_assets_and_moves_cap_excess_to_cash() {
        let allocation = normalize_allocation(
            &json!({
                "weights": {
                    "QQQ": {"weight": 0.9, "rationale": "qqq"},
                    "SOXX": {"weight": 0.2, "rationale": "soxx"},
                    "VIX": {"weight": 0.1, "rationale": "invalid"}
                },
                "summary": "summary"
            }),
            &test_context(),
            &test_config(),
        );
        let weights = allocation
            .get("weights")
            .and_then(Value::as_object)
            .unwrap();
        assert!(!weights.contains_key("VIX"));
        assert_eq!(weights["QQQ"]["weight"], json!(0.7));
        let sum = weights
            .values()
            .map(|value| value.get("weight").and_then(Value::as_f64).unwrap())
            .sum::<f64>();
        assert!((sum - 1.0).abs() < 0.0001, "sum={sum}");
        assert!(weights["cash_hedge"]["weight"].as_f64().unwrap() > 0.0);
    }

    #[test]
    fn empty_llm_weights_fall_back_to_inverse_vol() {
        let allocation =
            normalize_allocation(&json!({"weights": {}}), &test_context(), &test_config());
        assert_eq!(
            allocation["allocation_method"],
            json!("fallback_inverse_vol")
        );
        assert_eq!(allocation["total_equity_exposure"], json!(0.6));
        assert_eq!(allocation["weights"]["QQQ"]["weight"], json!(0.4));
        assert_eq!(allocation["weights"]["SOXX"]["weight"], json!(0.2));
        assert_eq!(allocation["weights"]["cash_hedge"]["weight"], json!(0.4));
    }

    #[test]
    fn pearson_correlation_uses_log_returns() {
        let a = log_returns(&[100.0, 101.0, 102.0, 103.0, 104.0]);
        let b = log_returns(&[50.0, 50.5, 51.0, 51.5, 52.0]);
        let corr = pearson_correlation(&a, &b).unwrap();
        assert!(corr > 0.99, "corr={corr}");
    }
}
