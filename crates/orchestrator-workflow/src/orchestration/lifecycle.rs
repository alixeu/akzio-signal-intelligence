use orchestrator_core::{md5_3, run_slug};
use serde_json::{json, Value};
use std::collections::BTreeSet;

pub(crate) fn run_id_for(tickers: &[String], date: &str) -> String {
    run_id_for_seed(tickers, date, "exec")
}

pub(crate) fn run_id_for_seed(tickers: &[String], date: &str, seed: &str) -> String {
    let prefix = run_slug(tickers).to_ascii_lowercase().replace('_', "-");
    format!("{prefix}-{}", md5_3(format!("{date}\x1f{seed}")))
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
        "position_size_pct_max": if action == "Hold" { 0.0 } else { 0.30 },
        "blockers": [],
        "rationale": format!("{action} mapped conservatively from research_plan. {rationale}")
    })
}

#[cfg(test)]
mod tests {
    use super::{run_id_for, run_id_for_seed, validate_asset_scope};

    #[test]
    fn run_id_does_not_depend_on_filesystem_path() {
        assert_eq!(
            run_id_for(&["QQQ".into(), "SOXX".into(), "VIX".into()], "2026-07-10"),
            "qqq-soxx-vix-0f6864"
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
}
