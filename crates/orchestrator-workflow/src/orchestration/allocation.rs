use anyhow::{bail, Result};
use orchestrator_core::{config_get, AllocationWeight, PortfolioAllocation};
use serde_json::{json, Value};
use std::collections::BTreeMap;

use super::config::AllocationConfig;

pub(crate) fn market_snapshot_from_technical(
    technical: &Value,
    config: &AllocationConfig,
) -> Result<Value> {
    let mut per_ticker = serde_json::Map::new();
    let mut regime_level = None;
    for snapshot in technical
        .get("snapshots")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(ticker) = snapshot.get("ticker").and_then(Value::as_str) else {
            continue;
        };
        let Some(daily) = snapshot
            .get("intervals")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .find(|interval| interval.get("interval").and_then(Value::as_str) == Some("daily"))
        else {
            continue;
        };
        let latest_close = daily.pointer("/latest/close").and_then(Value::as_f64);
        let vol_pct = daily
            .get("signals")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .find(|signal| signal.get("kind").and_then(Value::as_str) == Some("volatility"))
            .and_then(|signal| signal.get("realized_volatility"))
            .and_then(Value::as_f64);
        if ticker == config.regime_signal {
            regime_level = latest_close;
        }
        per_ticker.insert(
            ticker.to_owned(),
            json!({
                "latest_close": latest_close,
                "vol_pct": vol_pct,
                "as_of": daily.pointer("/latest/date").cloned().unwrap_or(Value::Null),
                "status": daily.get("status").cloned().unwrap_or(Value::Null),
            }),
        );
    }
    let vix = if let Some(level) = regime_level {
        let (regime, equity_budget_hint) =
            classify_regime(level, &config.regime_thresholds, &config.regime_labels);
        json!({
            "signal": config.regime_signal,
            "level": level,
            "regime": regime,
            "equity_budget_hint": equity_budget_hint,
            "status": "available"
        })
    } else {
        unavailable_vix(config)
    };
    Ok(json!({
        "source": "filestore.run_input.technical.daily",
        "vix": vix,
        "per_ticker": per_ticker,
        "correlation_60d": null
    }))
}

pub(crate) fn compute_allocation_context(
    state: &Value,
    config: &AllocationConfig,
) -> Result<Value> {
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

    let vix_info = state
        .pointer("/market_snapshot/vix")
        .cloned()
        .unwrap_or_else(|| unavailable_vix(config));

    let research_plan = state
        .get("research_plan")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow::anyhow!("allocation context requires a validated research_plan"))?;
    let research_per_ticker = research_plan.get("per_ticker").and_then(Value::as_object);
    let per_ticker = investable
        .iter()
        .map(|ticker| -> Result<(String, Value)> {
            let research = research_per_ticker
                .and_then(|items| items.get(ticker))
                .or_else(|| {
                    research_plan
                        .get("primary")
                        .filter(|_| investable.len() == 1)
                })
                .ok_or_else(|| anyhow::anyhow!("research_plan missing ticker {ticker}"))?;
            let rating = research
                .get("rating")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| anyhow::anyhow!("research_plan rating missing for {ticker}"))?;
            let long_prob = research
                .get("long_probability")
                .or_else(|| research.get("final_probability"))
                .and_then(Value::as_f64)
                .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
                .ok_or_else(|| {
                    anyhow::anyhow!("research_plan probability missing or invalid for {ticker}")
                })?;
            let vol_pct = state
                .pointer(&format!("/market_snapshot/per_ticker/{ticker}/vol_pct"))
                .and_then(Value::as_f64);
            let thesis = research.get("plan").and_then(Value::as_str).unwrap_or("");
            Ok((
                ticker.clone(),
                json!({
                    "rating": rating,
                    "long_probability": long_prob,
                    "vol_pct": vol_pct,
                    "thesis": thesis
                }),
            ))
        })
        .collect::<Result<serde_json::Map<_, _>>>()?;

    let correlation_60d = state
        .pointer("/market_snapshot/correlation_60d")
        .and_then(Value::as_f64);
    let correlation_warning = match correlation_60d {
        Some(corr) if corr > 0.85 => "高度相关, 需控制集中度",
        Some(_) => "相关性适中",
        None => "相关性数据不足",
    };
    let trader_plans = state
        .pointer("/trader_investment_plan/per_ticker")
        .cloned()
        .unwrap_or(Value::Null);

    Ok(json!({
        "investable_assets": investable,
        "vix": vix_info,
        "per_ticker": per_ticker,
        "research_plan": state.get("research_plan").cloned().unwrap_or(Value::Null),
        "trader_plans": trader_plans,
        "risk_debate_state": state.get("risk_debate_state").cloned().unwrap_or(Value::Null),
        "final_trade_decision": state.get("final_trade_decision").cloned().unwrap_or(Value::Null),
        "correlation_60d": correlation_60d,
        "correlation_warning": correlation_warning,
        "max_single_position": config.max_single_position
    }))
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

    if all_trader_plans_have_zero_position(context) {
        return cash_only_allocation(
            context,
            "LLM allocation conflicts with all upstream trader plans being capped at 0%",
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

    let mut excess = 0.0;
    for ticker in &investable {
        if let Some(w) = weights.get_mut(ticker) {
            let max_pos = effective_position_cap(context, config, ticker);
            if *w > max_pos {
                excess += *w - max_pos;
                *w = max_pos;
            }
        }
    }
    if excess > 0.0 {
        *weights.entry("cash_hedge".to_string()).or_insert(0.0) += excess;
    }

    let weights_json: BTreeMap<String, AllocationWeight> = weights
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                AllocationWeight {
                    weight: (*v * 10_000.0).round() / 10_000.0,
                    rationale: rationales.get(k).cloned().unwrap_or_default(),
                },
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
        "equity_budget_deviation": equity_budget_deviation(context, total_equity),
        "summary": raw.get("summary").and_then(Value::as_str).unwrap_or(""),
        "allocation_method": "llm"
    })
}

/// Build the final allocation without an LLM. Research owns probability;
/// this function only applies volatility, regime, exposure and concentration
/// constraints, then verifies the exact persisted payload.
pub(crate) fn derive_guarded_allocation(
    state: &Value,
    context: &Value,
    config: &AllocationConfig,
) -> Result<Value> {
    validate_allocation_research_input(state, context)?;
    let mut allocation = normalize_allocation(&json!({"weights": {}}), context, config);
    allocation = apply_phase6_execution_constraints(allocation, context, config)?;
    allocation["allocation_method"] = json!("rust_inverse_vol_guardrails");
    validate_allocation_output(&allocation, context, config)?;
    Ok(allocation)
}

fn apply_phase6_execution_constraints(
    mut allocation: Value,
    context: &Value,
    config: &AllocationConfig,
) -> Result<Value> {
    let Some(constraints) = context
        .get("final_trade_decision")
        .and_then(|decision| decision.get("per_asset"))
        .and_then(Value::as_object)
    else {
        return Ok(allocation);
    };
    let investable = context
        .get("investable_assets")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    let weights = allocation
        .get_mut("weights")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| anyhow::anyhow!("allocation weights missing"))?;
    let mut equity_total: f64 = 0.0;
    for ticker in investable {
        let Some(constraint) = constraints.get(ticker) else {
            continue;
        };
        let current = constraint
            .get("current_weight")
            .and_then(Value::as_f64)
            .filter(|weight| weight.is_finite())
            .unwrap_or(0.0)
            .clamp(0.0, 1.0);
        let cap = constraint
            .get("max_target_weight")
            .and_then(Value::as_f64)
            .filter(|weight| weight.is_finite())
            .unwrap_or(current)
            .clamp(0.0, effective_position_cap(context, config, ticker));
        let delta = constraint
            .get("max_weight_delta")
            .and_then(Value::as_f64)
            .filter(|weight| weight.is_finite())
            .unwrap_or(0.0)
            .clamp(0.0, 1.0);
        let status = constraint
            .get("execution_status")
            .and_then(Value::as_str)
            .unwrap_or("wait");
        let direction = constraint
            .get("direction_constraint")
            .and_then(Value::as_str)
            .unwrap_or("unchanged");
        let desired = weights
            .get(ticker)
            .and_then(|entry| entry.get("weight"))
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let mut target = desired.min(cap).clamp(current - delta, current + delta);
        if status == "wait" || direction == "unchanged" {
            target = current;
        } else if direction == "increase_only" {
            target = target.max(current);
        } else if direction == "decrease_only" || status == "downgrade" {
            target = target.min(current);
        }
        target = target.clamp(0.0, cap.max(current));
        equity_total += target;
        weights.insert(
            ticker.to_string(),
            json!({
                "weight": (target * 10_000.0).round() / 10_000.0,
                "rationale": "Phase 7 projection of Phase 6 semantic execution constraints."
            }),
        );
    }
    if equity_total > 1.0 + 0.001 {
        bail!("Phase 6 current-weight constraints exceed total portfolio capacity");
    }
    weights.insert(
        "cash_hedge".to_string(),
        json!({
            "weight": ((1.0 - equity_total) * 10_000.0).round() / 10_000.0,
            "rationale": "Residual cash after Phase 6 execution constraints."
        }),
    );
    allocation["total_equity_exposure"] = json!((equity_total * 10_000.0).round() / 10_000.0);
    allocation["summary"] =
        json!("Rust allocation projected through Phase 6 per-asset constraints.");
    Ok(allocation)
}

fn validate_allocation_research_input(state: &Value, context: &Value) -> Result<()> {
    let research_value = state
        .get("research_plan")
        .ok_or_else(|| anyhow::anyhow!("allocation requires a validated research_plan"))?;
    let research = research_value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("allocation requires a validated research_plan"))?;
    let tickers = context
        .get("investable_assets")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("allocation investable_assets missing"))?;
    if tickers.is_empty() {
        bail!("allocation requires at least one investable asset");
    }
    for ticker in tickers.iter().filter_map(Value::as_str) {
        let payload = research
            .get("per_ticker")
            .and_then(Value::as_object)
            .and_then(|items| items.get(ticker))
            .or_else(|| {
                (tickers.len() == 1)
                    .then(|| research.get("primary"))
                    .flatten()
            })
            .ok_or_else(|| anyhow::anyhow!("research_plan missing ticker {ticker}"))?;
        let probability = payload
            .get("final_probability")
            .or_else(|| payload.get("long_probability"))
            .and_then(Value::as_f64)
            .ok_or_else(|| anyhow::anyhow!("research_plan probability missing for {ticker}"))?;
        if !probability.is_finite() || !(0.0..=1.0).contains(&probability) {
            bail!("research_plan probability invalid for {ticker}: {probability}");
        }
    }
    Ok(())
}

fn validate_allocation_output(
    allocation: &Value,
    context: &Value,
    config: &AllocationConfig,
) -> Result<()> {
    serde_json::from_value::<PortfolioAllocation>(allocation.clone())
        .map_err(|error| anyhow::anyhow!("allocation contract invalid: {error}"))?;
    let weights = allocation
        .get("weights")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow::anyhow!("allocation weights missing"))?;
    let investable = context
        .get("investable_assets")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    let mut total = 0.0;
    for (asset, entry) in weights {
        if asset != "cash_hedge" && !investable.contains(&asset.as_str()) {
            bail!("allocation contains non-investable asset {asset}");
        }
        let weight = entry
            .get("weight")
            .and_then(Value::as_f64)
            .ok_or_else(|| anyhow::anyhow!("allocation weight missing for {asset}"))?;
        if !weight.is_finite() || weight < 0.0 {
            bail!("allocation weight invalid for {asset}: {weight}");
        }
        if asset != "cash_hedge" && weight > effective_position_cap(context, config, asset) + 0.0001
        {
            bail!("allocation weight exceeds cap for {asset}: {weight}");
        }
        total += weight;
    }
    if (total - 1.0).abs() > 0.001 {
        bail!("allocation weights must sum to 1.0, got {total}");
    }
    Ok(())
}

fn fallback_inverse_vol(context: &Value, config: &AllocationConfig, reason: &str) -> Value {
    if all_trader_plans_have_zero_position(context) {
        return cash_only_allocation(
            context,
            &format!("All upstream trader plans have a 0% position; fallback reason={reason}"),
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
    let equity_budget = regime_equity_budget;

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
        let capped = raw_w.min(effective_position_cap(context, config, ticker));
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

fn all_trader_plans_have_zero_position(context: &Value) -> bool {
    let Some(investable) = context.get("investable_assets").and_then(Value::as_array) else {
        return false;
    };
    !investable.is_empty()
        && investable.iter().filter_map(Value::as_str).all(|ticker| {
            trader_plan_position_cap(context, ticker).is_some_and(|cap| cap <= f64::EPSILON)
        })
}

fn effective_position_cap(context: &Value, config: &AllocationConfig, ticker: &str) -> f64 {
    let configured_cap = config.max_single_position.clamp(0.0, 1.0);
    let trader_cap = trader_plan_position_cap(context, ticker).unwrap_or(1.0);
    let risk_cap = active_risk_position_cap(context, ticker).unwrap_or(1.0);
    configured_cap.min(trader_cap).min(risk_cap)
}

fn active_risk_position_cap(context: &Value, ticker: &str) -> Option<f64> {
    let risk_state = context.get("risk_debate_state")?;
    let direct = std::iter::once(risk_state);
    let history = risk_state
        .get("history")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|turn| turn.get("artifact").unwrap_or(turn));
    let constraints = risk_state
        .get("constraints")
        .and_then(Value::as_array)
        .into_iter()
        .flatten();

    direct
        .chain(history)
        .chain(constraints)
        .filter(|artifact| !risk_artifact_is_degraded(artifact))
        .filter_map(|artifact| artifact.get("payload").or(Some(artifact)))
        .filter_map(|payload| {
            payload
                .pointer(&format!("/per_asset/{ticker}/position_cap_pct"))
                .or_else(|| payload.get("position_cap_pct"))
        })
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

fn trader_plan_position_cap(context: &Value, ticker: &str) -> Option<f64> {
    let plan = context
        .pointer(&format!("/trader_plans/{ticker}"))
        .or_else(|| context.get("trader_plan"))?;
    match plan.get("action").and_then(Value::as_str) {
        Some(action) if action.eq_ignore_ascii_case("hold") => Some(0.0),
        Some(action)
            if action.eq_ignore_ascii_case("buy") || action.eq_ignore_ascii_case("sell") =>
        {
            Some(
                plan.get("position_size_pct_max")
                    .and_then(position_fraction)
                    .unwrap_or(0.0),
            )
        }
        _ => Some(0.0),
    }
}

fn position_fraction(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number
            .as_f64()
            .filter(|position| position.is_finite() && (0.0..=1.0).contains(position)),
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

fn unavailable_vix(config: &AllocationConfig) -> Value {
    json!({
        "signal": config.regime_signal,
        "level": null,
        "regime": "defensive",
        "equity_budget_hint": "0.00-0.40",
        "status": "data_gap",
        "reason": "run-local technical snapshot has no VIX projection; fail closed"
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
        let max_single = config_get(config, "orchestrator.allocation.max_single_position")
            .and_then(Value::as_f64)
            .unwrap_or(0.70);
        Self {
            investable_assets: investable,
            regime_signal,
            regime_thresholds,
            regime_labels,
            max_single_position: max_single,
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
            max_single_position: 0.70,
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
    fn technical_snapshot_projects_vix_and_asset_volatility() {
        let snapshot = market_snapshot_from_technical(
            &json!({
                "snapshots": [
                    {"ticker": "QQQ", "intervals": [{
                        "interval": "daily", "status": "ok",
                        "latest": {"date": "2026-07-30", "close": 600.0},
                        "signals": [{"kind": "volatility", "realized_volatility": 0.012}]
                    }]},
                    {"ticker": "VIX", "intervals": [{
                        "interval": "daily", "status": "ok",
                        "latest": {"date": "2026-07-30", "close": 22.0},
                        "signals": [{"kind": "volatility", "realized_volatility": 0.08}]
                    }]}
                ]
            }),
            &test_config(),
        )
        .unwrap();

        assert_eq!(snapshot["vix"]["regime"], "elevated");
        assert_eq!(snapshot["per_ticker"]["QQQ"]["vol_pct"], 0.012);
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
    fn empty_llm_weights_respect_zero_percent_trader_position() {
        let mut context = test_context();
        context["trader_plan"] = json!({"action": "Hold", "position_size_pct_max": 0.0});

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
        context["trader_plan"] = json!({"action": "Buy", "position_size_pct_max": 0.25});

        let allocation = normalize_allocation(&json!({"weights": {}}), &context, &test_config());

        assert_eq!(
            allocation["allocation_method"],
            json!("fallback_inverse_vol")
        );
        assert_eq!(allocation["total_equity_exposure"], json!(0.45));
        assert_eq!(allocation["weights"]["QQQ"]["weight"], json!(0.25));
        assert_eq!(allocation["weights"]["SOXX"]["weight"], json!(0.2));
        assert_eq!(allocation["weights"]["cash_hedge"]["weight"], json!(0.55));
    }

    #[test]
    fn valid_llm_weights_cannot_override_zero_percent_trader_position() {
        let mut context = test_context();
        context["trader_plan"] = json!({"action": "Hold", "position_size_pct_max": 0.0});

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
    fn one_hold_does_not_veto_another_investable_asset() {
        let mut context = test_context();
        context["trader_plans"] = json!({
            "QQQ": {"action": "Hold", "position_size_pct_max": 0.0},
            "SOXX": {"action": "Buy", "position_size_pct_max": 0.3}
        });

        let allocation = normalize_allocation(
            &json!({
                "weights": {
                    "QQQ": {"weight": 0.4},
                    "SOXX": {"weight": 0.4},
                    "cash_hedge": {"weight": 0.2}
                }
            }),
            &context,
            &test_config(),
        );

        assert_eq!(allocation["weights"]["QQQ"]["weight"], json!(0.0));
        assert_eq!(allocation["weights"]["SOXX"]["weight"], json!(0.3));
        assert_eq!(allocation["weights"]["cash_hedge"]["weight"], json!(0.7));
    }

    #[test]
    fn valid_llm_weights_are_capped_per_asset_by_trader_position_cap() {
        let mut context = test_context();
        context["trader_plan"] = json!({"action": "Buy", "position_size_pct_max": 0.1});

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

        assert_eq!(allocation["total_equity_exposure"], json!(0.2));
        assert_eq!(allocation["weights"]["QQQ"]["weight"], json!(0.1));
        assert_eq!(allocation["weights"]["SOXX"]["weight"], json!(0.1));
        assert_eq!(allocation["weights"]["cash_hedge"]["weight"], json!(0.8));
    }

    #[test]
    fn trader_numeric_position_cap_limits_each_asset() {
        let mut context = test_context();
        context["trader_plan"] = json!({"action": "Buy", "position_size_pct_max": 0.25});

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

        assert_eq!(allocation["total_equity_exposure"], json!(0.5));
        assert_eq!(allocation["weights"]["QQQ"]["weight"], json!(0.25));
        assert_eq!(allocation["weights"]["SOXX"]["weight"], json!(0.25));
        assert_eq!(allocation["weights"]["cash_hedge"]["weight"], json!(0.5));
    }

    #[test]
    fn hold_action_forces_cash_even_when_numeric_cap_is_positive() {
        let mut context = test_context();
        context["trader_plan"] = json!({"action": "Hold", "position_size_pct_max": 0.3});

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
                        "payload": {
                        "stance": "conditional",
                        "recommended_adjustment": "cap each position",
                        "position_cap_pct": 0.25
                        }
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
                        "payload": {
                        "stance": "conditional",
                        "recommended_adjustment": "cap each position",
                        "position_cap_pct": 0.15
                        }
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
                "payload": {
                "stance": "conservative",
                "position_cap_pct": 0.0
                }
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
                "payload": {
                "stance": "conservative",
                "position_cap_pct": 0.0
                }
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
    fn guarded_allocation_requires_probability_for_every_investable_ticker() {
        let state = json!({
            "research_plan": {"per_ticker": {"QQQ": {"long_probability": 0.61}}}
        });

        let error = derive_guarded_allocation(&state, &test_context(), &test_config()).unwrap_err();

        assert!(error.to_string().contains("missing ticker SOXX"));
    }

    #[test]
    fn allocation_context_rejects_missing_probability_instead_of_defaulting_to_neutral() {
        let state = json!({
            "tickers": ["QQQ", "SOXX", "VIX"],
            "research_plan": {"per_ticker": {
                    "QQQ": {"rating": "Buy", "long_probability": 0.61},
                    "SOXX": {"rating": "Hold"}
                }}
        });
        let error = compute_allocation_context(&state, &test_config()).unwrap_err();

        assert!(error
            .to_string()
            .contains("probability missing or invalid for SOXX"));
    }

    #[test]
    fn guarded_allocation_is_finite_capped_and_excludes_regime_signal() {
        let state = json!({
            "research_plan": {"per_ticker": {
                    "QQQ": {"long_probability": 0.61},
                    "SOXX": {"final_probability": 0.58}
                }}
        });

        let mut context = test_context();
        context["final_trade_decision"] = state["final_trade_decision"].clone();
        let allocation = derive_guarded_allocation(&state, &context, &test_config()).unwrap();

        assert_eq!(
            allocation["allocation_method"],
            "rust_inverse_vol_guardrails"
        );
        assert!(allocation["weights"].get("VIX").is_none());
        let total = allocation["weights"]
            .as_object()
            .unwrap()
            .values()
            .map(|entry| entry["weight"].as_f64().unwrap())
            .sum::<f64>();
        assert!((total - 1.0).abs() <= 0.001);
    }

    #[test]
    fn phase6_wait_constraints_preserve_current_weight() {
        let state = json!({
            "research_plan": {"per_ticker": {
                    "QQQ": {"long_probability": 0.61},
                    "SOXX": {"long_probability": 0.58}
                }},
            "final_trade_decision": {
                "execution_status": "wait",
                "per_asset": {
                    "QQQ": {"direction_constraint": "unchanged", "execution_status": "wait", "current_weight": 0.0, "max_target_weight": 0.0, "max_weight_delta": 0.0, "binding_risk_controls": []},
                    "SOXX": {"direction_constraint": "unchanged", "execution_status": "wait", "current_weight": 0.0, "max_target_weight": 0.0, "max_weight_delta": 0.0, "binding_risk_controls": []}
                }
            }
        });

        let mut context = test_context();
        context["final_trade_decision"] = state["final_trade_decision"].clone();
        let allocation = derive_guarded_allocation(&state, &context, &test_config()).unwrap();

        assert_eq!(
            allocation["allocation_method"],
            "rust_inverse_vol_guardrails"
        );
        assert_eq!(allocation["total_equity_exposure"], 0.0);
        assert_eq!(allocation["weights"]["cash_hedge"]["weight"], 1.0);
    }
}
