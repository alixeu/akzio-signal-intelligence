use serde_json::{json, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkflowPolicyMode {
    Legacy,
    Selective,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkflowPolicyReason {
    LowConfidence,
    ProbabilityNearNeutral,
    HighVolatility,
    HighCorrelation,
    HighPosition,
    HighRiskFlag,
    TradeResearchConflict,
    PolicyForcePortfolioReview,
    ResearchDegraded,
}

impl WorkflowPolicyReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::LowConfidence => "LOW_CONFIDENCE",
            Self::ProbabilityNearNeutral => "PROBABILITY_NEAR_NEUTRAL",
            Self::HighVolatility => "HIGH_VOLATILITY",
            Self::HighCorrelation => "HIGH_CORRELATION",
            Self::HighPosition => "HIGH_POSITION",
            Self::HighRiskFlag => "HIGH_RISK_FLAG",
            Self::TradeResearchConflict => "TRADE_RESEARCH_CONFLICT",
            Self::PolicyForcePortfolioReview => "POLICY_FORCE_PORTFOLIO_REVIEW",
            Self::ResearchDegraded => "RESEARCH_DEGRADED",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkflowPolicyDecision {
    pub(crate) need_trader: bool,
    pub(crate) need_risk_review: bool,
    pub(crate) need_portfolio_review: bool,
    pub(crate) reasons: Vec<WorkflowPolicyReason>,
    pub(crate) skipped_phases: Vec<&'static str>,
    pub(crate) mode: WorkflowPolicyMode,
    pub(crate) evaluated_at_phase: i64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct WorkflowPolicyThresholds {
    pub(crate) min_confidence: f64,
    pub(crate) neutral_probability_band: f64,
    pub(crate) max_volatility: f64,
    pub(crate) max_correlation: f64,
    pub(crate) max_position: f64,
}

impl Default for WorkflowPolicyThresholds {
    fn default() -> Self {
        Self {
            min_confidence: 0.55,
            neutral_probability_band: 0.05,
            max_volatility: 0.03,
            max_correlation: 0.85,
            max_position: 0.70,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct WorkflowPolicySignals {
    pub(crate) confidence: Option<f64>,
    pub(crate) long_probability: Option<f64>,
    pub(crate) volatility: Option<f64>,
    pub(crate) correlation: Option<f64>,
    /// Estimated / proposed max single-name weight in [0, 1].
    pub(crate) proposed_position: Option<f64>,
    pub(crate) high_risk_flag: bool,
    pub(crate) trade_research_conflict: bool,
    pub(crate) force_portfolio_review: bool,
    pub(crate) research_degraded: bool,
}

pub(crate) fn legacy_workflow_policy(evaluated_at_phase: i64) -> WorkflowPolicyDecision {
    WorkflowPolicyDecision {
        need_trader: true,
        need_risk_review: true,
        need_portfolio_review: true,
        reasons: Vec::new(),
        skipped_phases: Vec::new(),
        mode: WorkflowPolicyMode::Legacy,
        evaluated_at_phase,
    }
}

pub(crate) fn evaluate_workflow_policy(
    mode: WorkflowPolicyMode,
    evaluated_at_phase: i64,
    signals: &WorkflowPolicySignals,
    thresholds: &WorkflowPolicyThresholds,
) -> WorkflowPolicyDecision {
    if mode == WorkflowPolicyMode::Legacy {
        return legacy_workflow_policy(evaluated_at_phase);
    }

    let mut reasons = Vec::new();
    if signals
        .confidence
        .is_some_and(|value| value < thresholds.min_confidence)
    {
        reasons.push(WorkflowPolicyReason::LowConfidence);
    }
    if signals
        .long_probability
        .is_some_and(|value| (value - 0.5).abs() <= thresholds.neutral_probability_band)
    {
        reasons.push(WorkflowPolicyReason::ProbabilityNearNeutral);
    }
    if signals
        .volatility
        .is_some_and(|value| value > thresholds.max_volatility)
    {
        reasons.push(WorkflowPolicyReason::HighVolatility);
    }
    if signals
        .correlation
        .is_some_and(|value| value > thresholds.max_correlation)
    {
        reasons.push(WorkflowPolicyReason::HighCorrelation);
    }
    if signals.high_risk_flag {
        reasons.push(WorkflowPolicyReason::HighRiskFlag);
    }
    if signals.trade_research_conflict {
        reasons.push(WorkflowPolicyReason::TradeResearchConflict);
    }
    if signals.force_portfolio_review {
        reasons.push(WorkflowPolicyReason::PolicyForcePortfolioReview);
    }
    if signals
        .proposed_position
        .is_some_and(|value| value > thresholds.max_position)
    {
        reasons.push(WorkflowPolicyReason::HighPosition);
    }
    if signals.research_degraded {
        reasons.push(WorkflowPolicyReason::ResearchDegraded);
    }

    let need_trader = signals.trade_research_conflict;
    let need_risk_review = has_any(
        &reasons,
        &[
            WorkflowPolicyReason::LowConfidence,
            WorkflowPolicyReason::ProbabilityNearNeutral,
            WorkflowPolicyReason::HighVolatility,
            WorkflowPolicyReason::HighCorrelation,
            WorkflowPolicyReason::HighPosition,
            WorkflowPolicyReason::HighRiskFlag,
            WorkflowPolicyReason::TradeResearchConflict,
            WorkflowPolicyReason::ResearchDegraded,
        ],
    );
    let need_portfolio_review = has_any(
        &reasons,
        &[
            WorkflowPolicyReason::HighCorrelation,
            WorkflowPolicyReason::HighPosition,
            WorkflowPolicyReason::PolicyForcePortfolioReview,
            WorkflowPolicyReason::ResearchDegraded,
        ],
    );

    WorkflowPolicyDecision {
        need_trader,
        need_risk_review,
        need_portfolio_review,
        reasons,
        skipped_phases: skipped_phases(need_trader, need_risk_review, need_portfolio_review),
        mode,
        evaluated_at_phase,
    }
}

pub(crate) fn workflow_policy_value(decision: &WorkflowPolicyDecision) -> Value {
    json!({
        "need_trader": decision.need_trader,
        "need_risk_review": decision.need_risk_review,
        "need_portfolio_review": decision.need_portfolio_review,
        "reasons": decision.reasons.iter().map(|reason| reason.as_str()).collect::<Vec<_>>(),
        "skipped_phases": decision.skipped_phases,
        "mode": match decision.mode {
            WorkflowPolicyMode::Legacy => "legacy",
            WorkflowPolicyMode::Selective => "selective",
        },
        "evaluated_at_phase": decision.evaluated_at_phase,
    })
}

pub(crate) fn record_workflow_policy(state: &mut Value, decision: &WorkflowPolicyDecision) {
    let value = workflow_policy_value(decision);
    state["workflow_policy"] = value.clone();
    if !state.get("policy_decisions").is_some_and(Value::is_array) {
        state["policy_decisions"] = json!([]);
    }
    if let Some(items) = state["policy_decisions"].as_array_mut() {
        items.push(value.clone());
    }
    state["skipped_phases"] = value
        .get("skipped_phases")
        .cloned()
        .unwrap_or_else(|| json!([]));
    if !state.get("workflow_metrics").is_some_and(Value::is_object) {
        state["workflow_metrics"] = json!({});
    }
    state["workflow_metrics"]["llm_calls_skipped_estimate"] = json!(decision.skipped_phases.len());
    state["workflow_metrics"]["skipped_phases"] = value
        .get("skipped_phases")
        .cloned()
        .unwrap_or_else(|| json!([]));
    state["workflow_metrics"]["policy_reasons"] =
        value.get("reasons").cloned().unwrap_or_else(|| json!([]));
    state["workflow_metrics"]["policy_mode"] = value
        .get("mode")
        .cloned()
        .unwrap_or_else(|| json!("legacy"));
}

fn has_any(reasons: &[WorkflowPolicyReason], expected: &[WorkflowPolicyReason]) -> bool {
    expected.iter().any(|reason| reasons.contains(reason))
}

fn skipped_phases(
    need_trader: bool,
    need_risk_review: bool,
    need_portfolio_review: bool,
) -> Vec<&'static str> {
    let mut phases = Vec::new();
    if !need_trader {
        phases.push("trader");
    }
    if !need_risk_review {
        phases.push("risk_review");
    }
    if !need_portfolio_review {
        phases.push("portfolio_review");
    }
    phases
}

// --- preflight enforcement (merged from preflight.rs) ---

use anyhow::{bail, Result};
use orchestrator_ingest::{jin10, technical};

use super::config::RuntimeConfig;
use super::degraded::record_preflight_result;

pub(crate) fn enforce_preflight_policy(
    state: &mut Value,
    role: &str,
    #[allow(unused_variables)] config: &RuntimeConfig,
) -> Result<()> {
    let Some(tool) = preflight_tool_for_role_with_config(role, config) else {
        return Ok(());
    };
    let Some(status) = preflight_status(state, tool) else {
        return Ok(());
    };
    if status.get("status").and_then(Value::as_str) == Some("error") {
        let message = status
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("preflight failed")
            .to_string();
        if super::config::is_critical_role(config, role) {
            bail!("critical preflight {tool} for role {role} failed: {message}");
        }
        if !state.get("degraded_roles").is_some_and(Value::is_array) {
            state["degraded_roles"] = json!([]);
        }
        if let Some(items) = state["degraded_roles"].as_array_mut() {
            items.push(json!({
                "role": role,
                "phase": 1,
                "kind": "preflight",
                "tool": tool,
                "message": message
            }));
        }
    }
    Ok(())
}

fn preflight_tool_for_role_with_config(role: &str, config: &RuntimeConfig) -> Option<&'static str> {
    preflight_tool_from_registry(role, &config.agent_registry)
}

fn preflight_tool_from_registry(
    role: &str,
    registry: &orchestrator_core::role_registry::AgentRegistry,
) -> Option<&'static str> {
    match registry
        .get(role)
        .and_then(|def| def.preflight_tool.as_deref())
    {
        Some("read_technical_context") => Some("read_technical_context"),
        Some("read_jin10_context") => Some("read_jin10_context"),
        _ => None,
    }
}

pub(crate) async fn run_phase1_preflight(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    role: &str,
    #[allow(unused_variables)] config: &RuntimeConfig,
) -> Result<()> {
    match preflight_tool_for_role_with_config(role, config) {
        Some("read_technical_context") => run_technical_csv_preflight(conn, state).await,
        Some("read_jin10_context") => run_jin10_preflight(conn, state).await,
        _ => Ok(()),
    }
}

pub(crate) async fn run_technical_csv_preflight(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
) -> Result<()> {
    let tool = "read_technical_context";
    if preflight_status(state, tool).is_some() {
        return Ok(());
    }
    if state
        .get("tech_refresh_enabled")
        .and_then(Value::as_bool)
        .is_some_and(|enabled| !enabled)
    {
        let result = import_technical_universe(conn, state).map(|summary| {
            json!({
                "status": "success",
                "refresh": "skipped",
                "source": "existing_ingest_files",
                "sqlite": summary
            })
        });
        record_preflight_result(state, tool, result);
        return Ok(());
    }

    let symbols = state
        .get("analysis_universe")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(",")
        })
        .filter(|value| !value.is_empty());
    let end = state
        .get("current_date")
        .and_then(Value::as_str)
        .map(str::to_string);

    let result = technical::run(technical::TechnicalArgs {
        symbols,
        start: None,
        end,
        days: None,
        intervals: String::new(),
        timeout: None,
        sleep: None,
    })
    .await
    .and_then(|ingest| {
        let sqlite = import_technical_universe(conn, state)?;
        Ok(json!({"status": "success", "ingest": ingest, "sqlite": sqlite}))
    });
    record_preflight_result(state, tool, result);
    Ok(())
}

fn import_technical_universe(conn: &mut rusqlite::Connection, state: &Value) -> Result<Value> {
    let tickers = state
        .get("analysis_universe")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::trim)
        .filter(|ticker| !ticker.is_empty())
        .collect::<Vec<_>>();
    if tickers.is_empty() {
        bail!("technical SQLite import requires analysis_universe");
    }
    let csv_dir = orchestrator_core::default_technical_csv_dir();
    let mut series = 0usize;
    let mut rows = 0usize;
    for ticker in tickers {
        for interval in ["daily", "3h", "20min"] {
            let path = orchestrator_core::technical_csv_path(&csv_dir, ticker, interval)
                .ok_or_else(|| anyhow::anyhow!("unsupported technical interval {interval}"))?;
            rows += orchestrator_sql::import_technical_csv(conn, ticker, interval, &path)?;
            series += 1;
        }
    }
    Ok(json!({
        "table": "technical_series",
        "series": series,
        "rows": rows
    }))
}

pub(crate) async fn run_jin10_preflight(
    _conn: &mut rusqlite::Connection,
    state: &mut Value,
) -> Result<()> {
    let tool = "read_jin10_context";
    if preflight_status(state, tool).is_some() {
        return Ok(());
    }
    let lookback_hours = state
        .get("jin10_lookback_hours")
        .and_then(Value::as_f64)
        .unwrap_or(24.0);
    let result = jin10::run(jin10::Jin10Args {
        channel: None,
        vip: None,
        classify: None,
        lookback_hours: Some(lookback_hours),
        pages: None,
        sleep: None,
        timeout: None,
        output: String::new(),
        jsonl: String::new(),
        pretty: false,
    })
    .await
    .and_then(|payload| {
        let csv = payload.get("csv").cloned().unwrap_or(Value::Null);
        let rows = csv.get("rows").and_then(Value::as_u64).unwrap_or_default();
        if rows == 0 {
            bail!("Jin10 refresh returned no non-empty, timestamped news items");
        }
        Ok(json!({
            "status": "success",
            "csv": csv,
            "sqlite_rows": 0,
            "persistence": "deferred_until_attention_scored"
        }))
    });
    record_preflight_result(state, tool, result);
    Ok(())
}

fn preflight_status<'a>(state: &'a Value, name: &str) -> Option<&'a Value> {
    state.get("preflight").and_then(|items| items.get(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn selective(signals: WorkflowPolicySignals) -> WorkflowPolicyDecision {
        evaluate_workflow_policy(
            WorkflowPolicyMode::Selective,
            3,
            &signals,
            &WorkflowPolicyThresholds::default(),
        )
    }

    #[test]
    fn legacy_policy_keeps_all_followup_phases() {
        let decision = evaluate_workflow_policy(
            WorkflowPolicyMode::Legacy,
            3,
            &WorkflowPolicySignals::default(),
            &WorkflowPolicyThresholds::default(),
        );
        assert!(decision.need_trader);
        assert!(decision.need_risk_review);
        assert!(decision.need_portfolio_review);
        assert!(decision.reasons.is_empty());
        assert!(decision.skipped_phases.is_empty());
        assert_eq!(decision.mode, WorkflowPolicyMode::Legacy);
        assert_eq!(decision.evaluated_at_phase, 3);
    }

    #[test]
    fn low_confidence_and_neutral_probability_trigger_risk_review() {
        let decision = selective(WorkflowPolicySignals {
            confidence: Some(0.40),
            long_probability: Some(0.52),
            ..WorkflowPolicySignals::default()
        });
        assert!(!decision.need_trader);
        assert!(decision.need_risk_review);
        assert!(!decision.need_portfolio_review);
        assert_eq!(
            decision.reasons,
            vec![
                WorkflowPolicyReason::LowConfidence,
                WorkflowPolicyReason::ProbabilityNearNeutral
            ]
        );
        assert_eq!(
            WorkflowPolicyReason::LowConfidence.as_str(),
            "LOW_CONFIDENCE"
        );
    }

    #[test]
    fn market_risks_trigger_risk_review() {
        let decision = selective(WorkflowPolicySignals {
            volatility: Some(0.05),
            correlation: Some(0.90),
            high_risk_flag: true,
            ..WorkflowPolicySignals::default()
        });
        assert!(!decision.need_trader);
        assert!(decision.need_risk_review);
        assert!(decision.need_portfolio_review);
        assert_eq!(
            decision.reasons,
            vec![
                WorkflowPolicyReason::HighVolatility,
                WorkflowPolicyReason::HighCorrelation,
                WorkflowPolicyReason::HighRiskFlag
            ]
        );
    }

    #[test]
    fn trade_research_conflict_triggers_trader_and_risk_review() {
        let decision = selective(WorkflowPolicySignals {
            trade_research_conflict: true,
            force_portfolio_review: true,
            ..WorkflowPolicySignals::default()
        });
        assert!(decision.need_trader);
        assert!(decision.need_risk_review);
        assert!(decision.need_portfolio_review);
        assert_eq!(
            decision.reasons,
            vec![
                WorkflowPolicyReason::TradeResearchConflict,
                WorkflowPolicyReason::PolicyForcePortfolioReview
            ]
        );
    }

    #[test]
    fn quiet_selective_policy_skips_all_followup_phases() {
        let decision = selective(WorkflowPolicySignals::default());
        assert!(!decision.need_trader);
        assert!(!decision.need_risk_review);
        assert!(!decision.need_portfolio_review);
        assert_eq!(
            decision.skipped_phases,
            vec!["trader", "risk_review", "portfolio_review"]
        );
        assert_eq!(decision.mode, WorkflowPolicyMode::Selective);
    }

    #[test]
    fn recording_policy_preserves_existing_workflow_metrics() {
        let decision = selective(WorkflowPolicySignals::default());
        let mut state = json!({
            "workflow_metrics": {
                "role_job_count": 9,
                "llm_call_count": 9
            }
        });

        record_workflow_policy(&mut state, &decision);

        assert_eq!(state["workflow_metrics"]["role_job_count"], 9);
        assert_eq!(state["workflow_metrics"]["llm_call_count"], 9);
        assert_eq!(state["workflow_metrics"]["policy_mode"], "selective");
        assert_eq!(state["workflow_metrics"]["llm_calls_skipped_estimate"], 3);
    }

    #[test]
    fn selective_high_position_triggers_reviews() {
        let decision = selective(WorkflowPolicySignals {
            proposed_position: Some(0.85),
            ..WorkflowPolicySignals::default()
        });
        assert!(decision.need_risk_review);
        assert!(decision.need_portfolio_review);
        assert!(decision
            .reasons
            .contains(&WorkflowPolicyReason::HighPosition));
    }

    #[test]
    fn selective_research_degraded_triggers_reviews() {
        let decision = selective(WorkflowPolicySignals {
            research_degraded: true,
            ..WorkflowPolicySignals::default()
        });
        assert!(decision.need_risk_review);
        assert!(decision.need_portfolio_review);
        assert!(decision
            .reasons
            .contains(&WorkflowPolicyReason::ResearchDegraded));
    }
}
