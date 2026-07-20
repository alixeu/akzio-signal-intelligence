use orchestrator_core::{closes_for_correlation, config_get, latest_indicator};
use orchestrator_sql::load_technical_csv;
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

pub(crate) fn allocation_prompt_context(context: &Value) -> Value {
    let risk_constraints = context
        .get("risk_debate_state")
        .and_then(|value| value.get("history"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|turn| turn.get("artifact"))
        .map(|artifact| {
            json!({
                "role": artifact.get("role").cloned().unwrap_or(Value::Null),
                "stance": artifact.get("stance").cloned().unwrap_or(Value::Null),
                "position_cap_pct": artifact.get("position_cap_pct").cloned().unwrap_or(Value::Null),
                "max_drawdown_pct": artifact.get("max_drawdown_pct").cloned().unwrap_or(Value::Null),
                "rebalance_trigger": artifact.get("rebalance_trigger").cloned().unwrap_or(Value::Null),
                "risk_off_trigger": artifact.get("risk_off_trigger").cloned().unwrap_or(Value::Null),
                "unique_risk_contribution": artifact.get("unique_risk_contribution").cloned().unwrap_or(Value::Null)
            })
        })
        .collect::<Vec<_>>();
    json!({
        "investable_assets": context.get("investable_assets").cloned().unwrap_or_else(|| json!([])),
        "vix": context.get("vix").cloned().unwrap_or(Value::Null),
        "per_ticker": context.get("per_ticker").cloned().unwrap_or_else(|| json!({})),
        "trader_plan": context.get("trader_plan").map(|plan| json!({
            "action": plan.get("action").cloned().unwrap_or(Value::Null),
            "position_size": plan.get("position_size").cloned().unwrap_or(Value::Null),
            "rationale": plan.get("rationale").cloned().unwrap_or(Value::Null)
        })).unwrap_or(Value::Null),
        "risk_constraints": risk_constraints,
        "final_trade_decision": context.get("final_trade_decision").map(|decision| json!({
            "rating": decision.get("rating").cloned().unwrap_or(Value::Null),
            "execution_summary": decision.get("execution_summary").cloned().unwrap_or(Value::Null),
            "risk_controls": decision.get("risk_controls").cloned().unwrap_or_else(|| json!([]))
        })).unwrap_or(Value::Null),
        "correlation_60d": context.get("correlation_60d").cloned().unwrap_or(Value::Null),
        "correlation_warning": context.get("correlation_warning").cloned().unwrap_or(Value::Null),
        "max_single_position": context.get("max_single_position").cloned().unwrap_or(Value::Null)
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

    let allocation_payload = allocation_payload(raw).unwrap_or(raw);
    let raw_weights = allocation_payload
        .get("weights")
        .or_else(|| allocation_payload.get("allocation"))
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

    if trader_plan_has_zero_position(context) {
        return cash_only_allocation(
            context,
            "LLM allocation conflicts with the upstream 0% trader position",
        );
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
    if total < 1.0 - 0.001 {
        *weights.entry("cash_hedge".to_string()).or_insert(0.0) += 1.0 - total;
    } else if total > 1.0 + 0.001 {
        for w in weights.values_mut() {
            *w /= total;
        }
    }

    let max_pos = effective_position_cap(context, config);
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

    let equity_before_cap = investable
        .iter()
        .filter_map(|ticker| weights.get(ticker))
        .sum::<f64>();
    if let Some(total_cap) = trader_plan_position_cap(context) {
        if equity_before_cap > total_cap + f64::EPSILON {
            if total_cap <= f64::EPSILON {
                return cash_only_allocation(
                    context,
                    "LLM allocation conflicts with the upstream 0% trader position",
                );
            }
            let scale = total_cap / equity_before_cap;
            for ticker in &investable {
                if let Some(weight) = weights.get_mut(ticker) {
                    *weight *= scale;
                }
            }
            weights.insert("cash_hedge".to_string(), 1.0 - total_cap);
            rationales
                .entry("cash_hedge".to_string())
                .or_insert_with(|| "Cash absorbs the upstream total-exposure cap.".to_string());
        }
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
        "vix_regime": allocation_payload.get("vix_regime").cloned()
            .or_else(|| context.get("vix").and_then(|v| v.get("regime")).cloned())
            .unwrap_or_else(|| json!("unknown")),
        "correlation_note": allocation_payload.get("correlation_note").cloned()
            .or_else(|| context.get("correlation_warning").cloned())
            .unwrap_or_else(|| json!("")),
        "equity_budget_deviation": equity_budget_deviation(context, total_equity),
        "summary": allocation_payload.get("summary").and_then(Value::as_str).unwrap_or(""),
        "allocation_method": "llm"
    })
}

fn allocation_payload(raw: &Value) -> Option<&Value> {
    if has_allocation_weights(raw) {
        return Some(raw);
    }

    raw.get("report")
        .filter(|report| has_allocation_weights(report))
}

fn has_allocation_weights(value: &Value) -> bool {
    value
        .get("weights")
        .or_else(|| value.get("allocation"))
        .is_some_and(Value::is_object)
}

fn fallback_inverse_vol(context: &Value, config: &AllocationConfig, reason: &str) -> Value {
    if trader_plan_has_zero_position(context) {
        return cash_only_allocation(
            context,
            &format!("Upstream trader plan has a 0% position; fallback reason={reason}"),
        );
    }

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
    let regime_equity_budget: f64 = match vix_regime {
        "risk_on" => 0.95,
        "normal" => 0.80,
        "elevated" => 0.60,
        "defensive" => 0.30,
        _ => 0.70,
    };
    let equity_budget = trader_plan_position_cap(context)
        .map(|position_cap| regime_equity_budget.min(position_cap))
        .unwrap_or(regime_equity_budget);

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
        return cash_only_allocation(
            context,
            &format!("No investable tickers available; fallback reason={reason}"),
        );
    }

    let inv_vol_sum: f64 = vols.iter().map(|(_, v)| 1.0 / v).sum();
    let mut weights = BTreeMap::new();
    for (ticker, vol) in &vols {
        let raw_w = (1.0 / vol) / inv_vol_sum * equity_budget;
        let capped = raw_w.min(effective_position_cap(context, config));
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
        "equity_budget_deviation": equity_budget_deviation(context, equity_actual),
        "correlation_note": context.get("correlation_warning").cloned().unwrap_or_else(|| json!("")),
        "summary": format!("Fallback inverse-vol allocation (reason: {})", reason),
        "allocation_method": "fallback_inverse_vol"
    })
}

fn trader_plan_has_zero_position(context: &Value) -> bool {
    trader_plan_position_cap(context).is_some_and(|position| position <= f64::EPSILON)
}

fn effective_position_cap(context: &Value, config: &AllocationConfig) -> f64 {
    let configured_cap = config.max_single_position.clamp(0.0, 1.0);
    let trader_cap = trader_plan_position_cap(context).unwrap_or(1.0);
    let risk_cap = active_risk_position_cap(context).unwrap_or(1.0);
    configured_cap.min(trader_cap).min(risk_cap)
}

fn active_risk_position_cap(context: &Value) -> Option<f64> {
    let risk_state = context.get("risk_debate_state")?;
    let direct = std::iter::once(risk_state);
    let history = risk_state
        .get("history")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|turn| turn.get("artifact"));
    let constraints = risk_state
        .get("constraints")
        .and_then(Value::as_array)
        .into_iter()
        .flatten();

    direct
        .chain(history)
        .chain(constraints)
        .filter(|artifact| !risk_artifact_is_degraded(artifact))
        .filter_map(|artifact| artifact.get("position_cap_pct"))
        .filter_map(position_fraction)
        .min_by(f64::total_cmp)
}

fn risk_artifact_is_degraded(artifact: &Value) -> bool {
    artifact.get("degraded").and_then(Value::as_bool) == Some(true)
        || artifact.get("usable").and_then(Value::as_bool) == Some(false)
        || matches!(
            artifact.get("status").and_then(Value::as_str),
            Some("degraded" | "missing" | "error" | "skipped")
        )
}

fn trader_plan_position_cap(context: &Value) -> Option<f64> {
    let plan = context.get("trader_plan")?;
    match plan.get("action").and_then(Value::as_str) {
        Some(action) if action.eq_ignore_ascii_case("hold") => Some(0.0),
        Some(action)
            if action.eq_ignore_ascii_case("buy") || action.eq_ignore_ascii_case("sell") =>
        {
            Some(
                plan.get("position_size")
                    .and_then(position_fraction)
                    .unwrap_or(0.0),
            )
        }
        _ => Some(0.0),
    }
}

fn position_fraction(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64().map(|position| position.clamp(0.0, 1.0)),
        Value::String(position) => {
            let trimmed = position.trim();
            if let Some(percent) = trimmed.strip_suffix('%') {
                if let Ok(value) = percent.trim().parse::<f64>() {
                    return Some((value / 100.0).clamp(0.0, 1.0));
                }
            }
            if let Ok(value) = trimmed.parse::<f64>() {
                return Some(value.clamp(0.0, 1.0));
            }

            let uses_percent = trimmed.contains('%');
            trimmed
                .split(|character: char| {
                    character == '-' || character == '/' || character.is_whitespace()
                })
                .filter_map(|part| part.trim().trim_end_matches('%').parse::<f64>().ok())
                .map(|value| if uses_percent { value / 100.0 } else { value })
                .map(|value| value.clamp(0.0, 1.0))
                .max_by(f64::total_cmp)
        }
        _ => None,
    }
}

fn cash_only_allocation(context: &Value, rationale: &str) -> Value {
    let vix_regime = context
        .get("vix")
        .and_then(|v| v.get("regime"))
        .and_then(Value::as_str)
        .unwrap_or("normal");

    let budget_hint = context
        .get("vix")
        .and_then(|v| v.get("equity_budget_hint"))
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    json!({
        "weights": {
            "cash_hedge": {
                "weight": 1.0,
                "rationale": rationale
            }
        },
        "total_equity_exposure": 0.0,
        "vix_regime": vix_regime,
        "equity_budget_hint": budget_hint,
        "equity_budget_deviation": equity_budget_deviation(context, 0.0),
        "correlation_note": context.get("correlation_warning").cloned().unwrap_or_else(|| json!("")),
        "summary": format!("Fallback cash allocation ({rationale})"),
        "allocation_method": "fallback_cash"
    })
}

fn equity_budget_deviation(context: &Value, actual: f64) -> Value {
    let hint = context
        .get("vix")
        .and_then(|value| value.get("equity_budget_hint"))
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let bounds = hint
        .split_once('-')
        .and_then(|(low, high)| Some((low.parse::<f64>().ok()?, high.parse::<f64>().ok()?)));
    let (status, amount) = match bounds {
        Some((low, _)) if actual < low => ("material_below_hint", low - actual),
        Some((_, high)) if actual > high => ("material_above_hint", actual - high),
        Some(_) => ("within_hint", 0.0),
        None => ("unknown_hint", 0.0),
    };
    json!({
        "status": status,
        "actual_equity_exposure": actual,
        "hint": hint,
        "absolute_deviation": amount,
        "explanation": if status == "material_below_hint" {
            "Upstream trader/risk constraints override the non-binding VIX regime hint."
        } else if status == "material_above_hint" {
            "The proposed exposure exceeds the non-binding VIX regime hint and requires explicit upstream conviction."
        } else {
            "Equity exposure is consistent with the VIX regime hint."
        }
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

fn query_latest_indicator(_conn: &Connection, ticker: &str, indicator: &str) -> Option<f64> {
    let rows = load_technical_csv(ticker, "1d");
    latest_indicator(&rows, indicator)
}

fn query_correlation(
    _conn: &Connection,
    ticker_a: &str,
    ticker_b: &str,
    window: usize,
) -> Option<f64> {
    let rows_a = load_technical_csv(ticker_a, "1d");
    let rows_b = load_technical_csv(ticker_b, "1d");

    let closes_a = closes_for_correlation(&rows_a, window + 1);
    let closes_b = closes_for_correlation(&rows_b, window + 1);

    // Align by date
    let dates_b: std::collections::HashMap<&str, f64> = closes_b
        .iter()
        .map(|(d, c)| (d.get(..10).unwrap_or(d.as_str()), *c))
        .collect();

    let mut aligned_a = Vec::new();
    let mut aligned_b = Vec::new();
    for (date, close_a) in &closes_a {
        let day = date.get(..10).unwrap_or(date.as_str());
        if let Some(&close_b) = dates_b.get(day) {
            aligned_a.push(*close_a);
            aligned_b.push(close_b);
        }
    }

    if aligned_a.len() < 10 {
        return None;
    }

    let rets_a = log_returns(&aligned_a);
    let rets_b = log_returns(&aligned_b);
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
    fn normalize_allocation_accepts_one_legacy_report_wrapper() {
        let allocation = normalize_allocation(
            &json!({
                "report": {
                    "weights": {
                        "QQQ": {"weight": 0.7, "rationale": "legacy wrapper"},
                        "cash_hedge": {"weight": 0.3, "rationale": "cash"}
                    }
                }
            }),
            &test_context(),
            &test_config(),
        );

        assert_eq!(allocation["allocation_method"], json!("llm"));
        assert_eq!(allocation["weights"]["QQQ"]["weight"], json!(0.7));
        assert_eq!(allocation["weights"]["cash_hedge"]["weight"], json!(0.3));
    }

    #[test]
    fn normalize_allocation_does_not_recursively_unwrap_legacy_reports() {
        let allocation = normalize_allocation(
            &json!({
                "report": {
                    "report": {
                        "weights": {"QQQ": {"weight": 1.0}}
                    }
                }
            }),
            &test_context(),
            &test_config(),
        );

        assert_eq!(
            allocation["allocation_method"],
            json!("fallback_inverse_vol")
        );
        assert_ne!(allocation["weights"]["QQQ"]["weight"], json!(0.7));
    }

    #[test]
    fn empty_llm_weights_respect_zero_percent_trader_position() {
        let mut context = test_context();
        context["trader_plan"] = json!({"action": "Hold", "position_size": "0%"});

        let allocation = normalize_allocation(&json!({"weights": {}}), &context, &test_config());

        assert_eq!(allocation["allocation_method"], json!("fallback_cash"));
        assert_eq!(allocation["total_equity_exposure"], json!(0.0));
        assert_eq!(allocation["weights"]["cash_hedge"]["weight"], json!(1.0));
        assert!(allocation["weights"].get("QQQ").is_none());
        assert!(allocation["weights"].get("SOXX").is_none());
    }

    #[test]
    fn empty_llm_weights_keep_inverse_vol_fallback_for_positive_trader_position() {
        let mut context = test_context();
        context["trader_plan"] = json!({"action": "Buy", "position_size": "25%"});

        let allocation = normalize_allocation(&json!({"weights": {}}), &context, &test_config());

        assert_eq!(
            allocation["allocation_method"],
            json!("fallback_inverse_vol")
        );
        assert_eq!(allocation["total_equity_exposure"], json!(0.25));
        assert_eq!(allocation["weights"]["cash_hedge"]["weight"], json!(0.75));
    }

    #[test]
    fn valid_llm_weights_cannot_override_zero_percent_trader_position() {
        let mut context = test_context();
        context["trader_plan"] = json!({"action": "Hold", "position_size": "0%"});

        let allocation = normalize_allocation(
            &json!({
                "weights": {
                    "QQQ": {"weight": 0.4, "rationale": "invalid exposure"},
                    "SOXX": {"weight": 0.4, "rationale": "invalid exposure"},
                    "cash_hedge": {"weight": 0.2, "rationale": "cash"}
                }
            }),
            &context,
            &test_config(),
        );

        assert_eq!(allocation["allocation_method"], json!("fallback_cash"));
        assert_eq!(allocation["total_equity_exposure"], json!(0.0));
        assert_eq!(allocation["weights"]["cash_hedge"]["weight"], json!(1.0));
    }

    #[test]
    fn valid_llm_weights_are_scaled_to_explicit_trader_position_cap() {
        let mut context = test_context();
        context["trader_plan"] = json!({"action": "Buy", "position_size": "10%"});

        let allocation = normalize_allocation(
            &json!({
                "weights": {
                    "QQQ": {"weight": 0.4, "rationale": "qqq"},
                    "SOXX": {"weight": 0.4, "rationale": "soxx"},
                    "cash_hedge": {"weight": 0.2, "rationale": "cash"}
                }
            }),
            &context,
            &test_config(),
        );

        assert_eq!(allocation["total_equity_exposure"], json!(0.1));
        assert_eq!(allocation["weights"]["QQQ"]["weight"], json!(0.05));
        assert_eq!(allocation["weights"]["SOXX"]["weight"], json!(0.05));
        assert_eq!(allocation["weights"]["cash_hedge"]["weight"], json!(0.9));
    }

    #[test]
    fn trader_position_range_uses_its_upper_bound_as_total_exposure_cap() {
        let mut context = test_context();
        context["trader_plan"] = json!({"action": "Buy", "position_size": "10%-25%"});

        let allocation = normalize_allocation(
            &json!({
                "weights": {
                    "QQQ": {"weight": 0.4, "rationale": "qqq"},
                    "SOXX": {"weight": 0.4, "rationale": "soxx"},
                    "cash_hedge": {"weight": 0.2, "rationale": "cash"}
                }
            }),
            &context,
            &test_config(),
        );

        assert_eq!(allocation["total_equity_exposure"], json!(0.25));
        assert_eq!(allocation["weights"]["QQQ"]["weight"], json!(0.125));
        assert_eq!(allocation["weights"]["SOXX"]["weight"], json!(0.125));
        assert_eq!(allocation["weights"]["cash_hedge"]["weight"], json!(0.75));
    }

    #[test]
    fn hold_action_forces_cash_even_when_position_range_is_positive() {
        let mut context = test_context();
        context["trader_plan"] = json!({"action": "Hold", "position_size": "0%-30%"});

        let allocation = normalize_allocation(
            &json!({
                "weights": {
                    "QQQ": {"weight": 0.6, "rationale": "should be rejected"},
                    "cash_hedge": {"weight": 0.4, "rationale": "cash"}
                }
            }),
            &context,
            &test_config(),
        );

        assert_eq!(allocation["allocation_method"], "fallback_cash");
        assert_eq!(allocation["total_equity_exposure"], 0.0);
        assert_eq!(allocation["weights"]["cash_hedge"]["weight"], 1.0);
    }

    #[test]
    fn malformed_trader_plan_fails_closed_to_cash() {
        let mut context = test_context();
        context["trader_plan"] = json!({"status": "degraded", "error": "missing action"});

        let allocation = normalize_allocation(
            &json!({
                "weights": {
                    "QQQ": {"weight": 0.6, "rationale": "must not survive"},
                    "cash_hedge": {"weight": 0.4, "rationale": "cash"}
                }
            }),
            &context,
            &test_config(),
        );

        assert_eq!(allocation["allocation_method"], "fallback_cash");
        assert_eq!(allocation["total_equity_exposure"], 0.0);
    }

    #[test]
    fn partial_llm_weights_are_completed_with_cash_without_scaling_up_equity() {
        let allocation = normalize_allocation(
            &json!({
                "weights": {"QQQ": {"weight": 0.10, "rationale": "small position"}}
            }),
            &test_context(),
            &test_config(),
        );

        assert_eq!(allocation["weights"]["QQQ"]["weight"], 0.10);
        assert_eq!(allocation["weights"]["cash_hedge"]["weight"], 0.90);
        assert_eq!(allocation["total_equity_exposure"], 0.10);
    }

    #[test]
    fn valid_risk_position_cap_limits_each_investable_asset() {
        let mut context = test_context();
        context["risk_debate_state"] = json!({
            "history": [
                {
                    "role": "risk.conservative",
                    "artifact": {
                        "status": "completed",
                        "stance": "conditional",
                        "recommended_adjustment": "cap each position",
                        "position_cap_pct": 0.25
                    }
                }
            ]
        });

        let allocation = normalize_allocation(
            &json!({
                "weights": {
                    "QQQ": {"weight": 0.6, "rationale": "qqq"},
                    "SOXX": {"weight": 0.2, "rationale": "soxx"},
                    "cash_hedge": {"weight": 0.2, "rationale": "cash"}
                }
            }),
            &context,
            &test_config(),
        );

        assert_eq!(allocation["weights"]["QQQ"]["weight"], json!(0.25));
        assert_eq!(allocation["weights"]["SOXX"]["weight"], json!(0.2));
        assert_eq!(allocation["weights"]["cash_hedge"]["weight"], json!(0.55));
    }

    #[test]
    fn inverse_vol_fallback_respects_valid_risk_position_cap() {
        let mut context = test_context();
        context["risk_debate_state"] = json!({
            "history": [
                {
                    "artifact": {
                        "status": "completed",
                        "stance": "conditional",
                        "recommended_adjustment": "cap each position",
                        "position_cap_pct": 0.15
                    }
                }
            ]
        });

        let allocation = normalize_allocation(&json!({"weights": {}}), &context, &test_config());

        assert_eq!(
            allocation["allocation_method"],
            json!("fallback_inverse_vol")
        );
        assert_eq!(allocation["weights"]["QQQ"]["weight"], json!(0.15));
        assert_eq!(allocation["weights"]["SOXX"]["weight"], json!(0.15));
        assert_eq!(allocation["weights"]["cash_hedge"]["weight"], json!(0.7));
    }

    #[test]
    fn zero_risk_position_cap_vetoes_llm_equity() {
        let mut context = test_context();
        context["risk_debate_state"] = json!({
            "history": [{"artifact": {
                "status": "completed",
                "stance": "conservative",
                "position_cap_pct": 0.0
            }}]
        });

        let allocation = normalize_allocation(
            &json!({
                "weights": {
                    "QQQ": {"weight": 0.6},
                    "cash_hedge": {"weight": 0.4}
                }
            }),
            &context,
            &test_config(),
        );

        assert_eq!(allocation["total_equity_exposure"], 0.0);
        assert_eq!(allocation["weights"]["cash_hedge"]["weight"], 1.0);
        assert_eq!(
            allocation["equity_budget_deviation"]["status"],
            "material_below_hint"
        );
        assert_eq!(
            allocation["equity_budget_deviation"]["absolute_deviation"],
            0.3
        );
    }

    #[test]
    fn zero_risk_position_cap_vetoes_inverse_vol_fallback() {
        let mut context = test_context();
        context["risk_debate_state"] = json!({
            "history": [{"artifact": {
                "status": "completed",
                "stance": "conservative",
                "position_cap_pct": 0.0
            }}]
        });

        let allocation = normalize_allocation(&json!({"weights": {}}), &context, &test_config());

        assert_eq!(allocation["total_equity_exposure"], 0.0);
        assert_eq!(allocation["weights"]["cash_hedge"]["weight"], 1.0);
    }

    #[test]
    fn degraded_risk_position_cap_does_not_constrain_allocation() {
        let mut context = test_context();
        context["risk_debate_state"] = json!({
            "history": [
                {
                    "artifact": {
                        "artifact_type": "degraded_risk_perspective",
                        "status": "degraded",
                        "degraded": true,
                        "usable": false,
                        "missing_perspective": "risk.conservative",
                        "degraded_reason": "stream failed",
                        "position_cap_pct": 0.05
                    }
                }
            ]
        });

        let allocation = normalize_allocation(
            &json!({
                "weights": {
                    "QQQ": {"weight": 0.6, "rationale": "qqq"},
                    "SOXX": {"weight": 0.2, "rationale": "soxx"},
                    "cash_hedge": {"weight": 0.2, "rationale": "cash"}
                }
            }),
            &context,
            &test_config(),
        );

        assert_eq!(allocation["weights"]["QQQ"]["weight"], json!(0.6));
        assert_eq!(allocation["weights"]["SOXX"]["weight"], json!(0.2));
        assert_eq!(allocation["weights"]["cash_hedge"]["weight"], json!(0.2));
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
