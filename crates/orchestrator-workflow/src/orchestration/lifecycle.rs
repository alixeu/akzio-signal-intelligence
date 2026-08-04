use anyhow::{Context, Result};
use orchestrator_core::{md5_3, run_slug};
use orchestrator_store::RunLocation;
use serde_json::{json, Value};
use std::collections::BTreeSet;

pub(crate) fn run_id_for(tickers: &[String], date: &str) -> String {
    run_id_for_seed(tickers, date, "exec")
}

pub(crate) fn run_id_for_seed(tickers: &[String], date: &str, seed: &str) -> String {
    let prefix = run_slug(tickers).to_ascii_lowercase().replace('_', "-");
    format!("{prefix}-{}", md5_3(format!("{date}\x1f{seed}")))
}

/// Debug runs are intentionally stable per ticker universe.  Reuse remains
/// safe only while the manifest's code/config/prompt identity matches; the
/// executor rejects drift before reopening this workspace.
pub(crate) fn debug_run_id_for(tickers: &[String]) -> String {
    let prefix = run_slug(tickers).to_ascii_lowercase().replace('_', "-");
    format!("{prefix}-debug")
}

/// Resolve the FileStore location from persisted state while preserving the
/// real market date inside state. Only debug runs opt into the date-free
/// `runs/debug/` partition.
pub(crate) fn run_location_from_state(state: &Value) -> Result<RunLocation> {
    let current_date = state
        .get("current_date")
        .and_then(Value::as_str)
        .context("run location requires current_date")?;
    let run_id = state
        .get("run_id")
        .and_then(Value::as_str)
        .context("run location requires run_id")?;
    match state.get("storage_namespace").and_then(Value::as_str) {
        None => RunLocation::new(current_date, run_id).map_err(Into::into),
        Some("debug") => RunLocation::debug(current_date, run_id).map_err(Into::into),
        Some(namespace) => anyhow::bail!("unsupported run storage_namespace {namespace:?}"),
    }
}

pub(crate) fn set_phase_status(state: &mut Value, phase: i64, status: &str) {
    if !state.get("phase_status").is_some_and(Value::is_object) {
        state["phase_status"] = json!({});
    }
    state["phase_status"][phase.to_string()] = Value::String(status.to_string());
}

pub(crate) fn tickers_from_state(state: &Value) -> Vec<String> {
    state
        .get("analysis_universe")
        .or_else(|| state.get("tickers"))
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn investable_assets_from_state(state: &Value) -> Vec<String> {
    state
        .get("investable_assets")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn validate_asset_scope(
    analysis_universe: &[String],
    investable_assets: &[String],
    regime_signal: &str,
) -> anyhow::Result<()> {
    if investable_assets.is_empty() {
        anyhow::bail!("orchestrator.allocation.investable_assets is required")
    }
    let universe = analysis_universe.iter().collect::<BTreeSet<_>>();
    let unique_investable = investable_assets.iter().collect::<BTreeSet<_>>();
    if unique_investable.len() != investable_assets.len() {
        anyhow::bail!("orchestrator.allocation.investable_assets contains duplicates")
    }
    for asset in investable_assets {
        if !universe.contains(asset) {
            anyhow::bail!("investable asset {asset} is not in orchestrator.analysis_universe")
        }
    }
    if investable_assets.iter().any(|asset| asset == regime_signal) {
        anyhow::bail!("regime signal {regime_signal} cannot be an investable asset")
    }
    if !regime_signal.is_empty() && !analysis_universe.iter().any(|asset| asset == regime_signal) {
        anyhow::bail!("regime signal {regime_signal} is not in orchestrator.analysis_universe")
    }
    Ok(())
}

pub(crate) fn research_plan_to_trade_intent(research_plan: &Value) -> Value {
    let rating = research_plan
        .get("rating")
        .and_then(Value::as_str)
        .unwrap_or("");
    let action = match rating {
        "Buy" | "Overweight" => "Buy",
        "Sell" | "Underweight" => "Sell",
        _ => "Hold",
    };
    let long_probability = research_plan
        .get("long_probability")
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && (0.0..=1.0).contains(value));
    let position_size_pct_max = long_probability
        .map(|probability| probability_position_cap(action, probability))
        // A missing Phase 3 probability must never turn a rating label into an
        // executable default allocation.
        .unwrap_or(0.0);
    let rationale = research_plan
        .get("probability_rationale")
        .and_then(Value::as_str)
        .or_else(|| research_plan.get("plan").and_then(Value::as_str))
        .unwrap_or("Research data is missing or not decisive.");
    json!({
        "action": action,
        "candidate_action": action,
        "execution_decision": if action == "Hold" { "hold" } else { "execute_candidate" },
        "entry_price": null,
        "stop_loss": null,
        "position_size_pct_max": position_size_pct_max,
        "research_probability_position_cap": position_size_pct_max,
        "probability_plan_projection": {
            "authority": "rust_probability_risk_budget_v1",
            "long_probability": long_probability,
            "candidate_action": action,
            "max_position_size_pct": position_size_pct_max,
            "status": if long_probability.is_some() { "available" } else { "missing_probability_blocks_execution" },
        },
        "blockers": [],
        "rationale": format!("{action} mapped conservatively from research_plan. {rationale}")
    })
}

/// Converts a research probability into a bounded maximum position size.  The
/// action threshold remains an explicit policy boundary, while execution
/// strength is continuous within each actionable region so 0.68 and 0.95 do
/// not silently become the same plan.
fn probability_position_cap(action: &str, long_probability: f64) -> f64 {
    let normalized_edge = match action {
        "Buy" => ((long_probability - 0.56) / (1.0 - 0.56)).clamp(0.0, 1.0),
        "Sell" => ((0.45 - long_probability) / 0.45).clamp(0.0, 1.0),
        _ => return 0.0,
    };
    // 5% at the actionability threshold, scaling continuously to the 30%
    // research budget only at an extreme probability.
    ((0.05 + 0.25 * normalized_edge) * 1_000_000.0).round() / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::{
        debug_run_id_for, probability_position_cap, research_plan_to_trade_intent, run_id_for,
        run_id_for_seed, run_location_from_state, validate_asset_scope,
    };
    use serde_json::json;

    #[test]
    fn run_id_does_not_depend_on_filesystem_path() {
        assert_eq!(
            run_id_for(&["QQQ".into(), "SOXX".into(), "VIX".into()], "2026-07-10"),
            "qqq-soxx-vix-0f6864"
        );
        assert_eq!(
            debug_run_id_for(&["QQQ".into(), "SOXX".into(), "VIX".into()]),
            "qqq-soxx-vix-debug"
        );
        assert_eq!(
            run_id_for_seed(
                &["QQQ".into(), "SOXX".into(), "VIX".into()],
                "2026-07-10",
                "debug:config"
            ),
            "qqq-soxx-vix-1493b0"
        );
    }

    #[test]
    fn debug_state_resolves_to_the_stable_partition() {
        let location = run_location_from_state(&json!({
            "current_date": "2026-07-31",
            "run_id": "qqq-soxx-vix-debug",
            "storage_namespace": "debug",
        }))
        .unwrap();
        assert_eq!(
            location.relative_root().to_string_lossy(),
            "runs/debug/qqq-soxx-vix-debug"
        );
    }

    #[test]
    fn asset_scope_separates_analysis_from_execution() {
        validate_asset_scope(
            &["QQQ".into(), "SOXX".into(), "VIX".into()],
            &["QQQ".into(), "SOXX".into()],
            "VIX",
        )
        .unwrap();
    }

    #[test]
    fn asset_scope_rejects_regime_signal_as_investable() {
        let error = validate_asset_scope(
            &["QQQ".into(), "VIX".into()],
            &["QQQ".into(), "VIX".into()],
            "VIX",
        )
        .unwrap_err();
        assert!(error.to_string().contains("cannot be an investable asset"));
    }

    #[test]
    fn probability_position_cap_scales_with_probability_inside_actionable_regions() {
        assert_eq!(probability_position_cap("Hold", 0.55), 0.0);
        assert!(probability_position_cap("Buy", 0.68) < probability_position_cap("Buy", 0.95));
        assert!(probability_position_cap("Sell", 0.40) < probability_position_cap("Sell", 0.20));

        let plan = research_plan_to_trade_intent(&json!({
            "rating": "Overweight",
            "long_probability": 0.68,
            "probability_rationale": "fixture"
        }));
        assert_eq!(plan["candidate_action"], "Buy");
        assert_eq!(
            plan["position_size_pct_max"],
            probability_position_cap("Buy", 0.68)
        );
    }
}
