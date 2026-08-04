use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use thiserror::Error;

/// Canonical Phase 3 decision fields validated by Store finalizers.
///
/// This is deliberately not an artifact envelope and has no LLM JSON
/// normalization path. File metadata and per-ticker aggregation belong to
/// `orchestrator-store`'s persisted canonical artifact.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ResearchDecision {
    pub rating: String,
    pub long_probability: f64,
    pub short_probability: f64,
    /// Why the probability is confident or uncertain: evidence_balanced,
    /// data_insufficient, conflicting_evidence, or directional_evidence.
    pub confidence_basis: String,
    /// Required for Hold: evidence_balanced, evidence_insufficient, or
    /// conflicting_evidence.
    pub hold_reason: Option<String>,
    pub plan: String,
    pub probability_rationale: String,
    /// Three-scenario analysis: bull, base, bear.
    pub scenarios: Option<Scenarios>,
}

/// A single scenario (bull, base, or bear) in the research manager's
/// scenario analysis output.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Scenario {
    /// Probability of this scenario (0.0-1.0). All scenarios must sum to 1.0.
    pub probability: f64,
    /// Conditional probability that the long outcome is realized if this
    /// scenario occurs.  This is deliberately separate from `probability`:
    /// the latter is a probability mass over regimes, while this field is the
    /// payoff-direction probability inside that regime.  Their weighted sum
    /// is the research decision's `long_probability`.
    pub conditional_long_probability: f64,
    /// Key drivers that would cause this scenario to play out (1-3 items).
    pub drivers: Vec<String>,
    /// Observable triggers that would shift probability toward this scenario (1-3 items).
    pub triggers: Vec<String>,
    /// What would confirm this scenario is the active path.
    pub confirmation: String,
}

/// Container for the three scenarios.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Scenarios {
    pub bull: Scenario,
    pub base: Scenario,
    pub bear: Scenario,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TradeIntent {
    pub action: String,
    /// Rust-owned rating mapping before the Trader applies semantic blockers.
    pub candidate_action: String,
    /// execute_candidate | hold
    pub execution_decision: String,
    pub entry_price: Option<String>,
    pub stop_loss: Option<String>,
    /// Numeric maximum portfolio fraction in [0.0, 1.0].
    ///
    /// Canonical Contract v2 deliberately has no free-form `position_size`
    /// field: callers must supply this machine-readable cap directly.
    pub position_size_pct_max: f64,
    pub blockers: Vec<String>,
    pub rationale: String,
}

/// The only stop semantics persisted by Canonical Contract v2.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StopType {
    Hard,
    Soft,
    None,
}

/// The only evidence classifications persisted by Canonical Contract v2.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceType {
    Fact,
    Opinion,
    Inference,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RiskConstraints {
    pub stance: String,
    pub argument: String,
    /// The stance-specific constraint or counterargument not already supplied
    /// by prior risk turns.
    pub unique_risk_contribution: String,
    /// Explicit agreement/disagreement with the strongest prior constraint.
    pub disagreement_with_prior: String,
    /// True only when the role found no genuine incremental constraint.
    pub no_new_information: bool,
    pub recommended_adjustment: String,
    /// hard | soft | none
    pub stop_type: StopType,
    /// 0.0-1.0 fraction of capital at risk before stopping.
    pub max_drawdown_pct: f64,
    /// 0.0-1.0 maximum single-position weight cap.
    pub position_cap_pct: f64,
    /// Condition that triggers a portfolio rebalance.
    pub rebalance_trigger: String,
    /// Condition that forces a risk-off / de-risk event.
    pub risk_off_trigger: String,
    /// How long until the risk view is revisited (human readable).
    pub review_window: String,
    /// Cash-hedge recommendation (size / instrument / rationale).
    pub cash_hedge_recommendation: String,
    /// 0.0-1.0 confidence in the constraints themselves.
    pub constraint_confidence: f64,
}

/// A Phase 5 control that is binding on a Phase 6 per-asset decision.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BindingRiskControl {
    pub control: String,
    pub source_refs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AssetExecutionConstraint {
    /// increase_only | decrease_only | unchanged
    pub direction_constraint: String,
    /// execute | wait | downgrade
    pub execution_status: String,
    /// Runtime-sourced current portfolio weight, 0.0-1.0.
    pub current_weight: f64,
    /// Hard ceiling for the Phase 7 target weight, 0.0-1.0.
    pub max_target_weight: f64,
    /// Largest absolute Phase 7 move from current_weight, 0.0-1.0.
    pub max_weight_delta: f64,
    pub binding_risk_controls: Vec<BindingRiskControl>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FinalValidation {
    pub rating: String,
    /// execute | wait | downgrade
    pub execution_status: String,
    pub execution_summary: String,
    pub investment_thesis: String,
    pub target_price: Option<String>,
    pub horizon: String,
    pub rationale: String,
    /// Per-asset semantic constraints for Rust-owned allocation and execution.
    pub per_asset: BTreeMap<String, AssetExecutionConstraint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AllocationWeight {
    pub weight: f64,
    pub rationale: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PortfolioAllocation {
    pub weights: BTreeMap<String, AllocationWeight>,
    pub total_equity_exposure: f64,
    pub vix_regime: String,
    pub correlation_note: String,
    pub summary: String,
}

/// Per-ticker evidence assessment used by the AnalystReport Store builder.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AnalystTickerArtifact {
    /// bullish | bearish | neutral | mixed | unobserved
    pub direction: String,
    /// Evidence-consistency / clarity, 0.0-1.0 (NOT 0-100, NOT upside probability).
    pub confidence: f64,
    /// Explicit 1-5 trading-day probability that the ticker's long outcome is
    /// realized. This is deliberately distinct from `confidence`, which is a
    /// quality assessment of the evidence supporting the estimate.
    pub long_probability: f64,
    /// Full prose analysis for this ticker (may contain sections / Markdown tables).
    pub report: String,
    /// The 2-3 most decisive evidence items.
    ///
    /// Structured source-backed evidence; the evidence type is always explicit.
    pub key_evidence: Vec<EvidenceItem>,
    /// already_priced | under_priced | unclear
    pub priced_in: String,
    /// low | medium | high
    pub echo_chamber_risk: String,
    /// low | medium | high
    pub crowded_consensus_risk: String,
    /// Observations that would strengthen or overturn the current call.
    pub validation_triggers: Vec<String>,
    /// Data gaps and uncertainties; empty array when none.
    pub data_gaps: Vec<String>,
}

/// Canonical evidence-type tokens accepted by runtime validators and reducers.
pub const CANONICAL_EVIDENCE_TYPES: &[&str] = &["fact", "opinion", "inference"];

/// A single piece of evidence with type classification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EvidenceItem {
    /// The evidence claim in 1-2 sentences.
    pub claim: String,
    /// Evidence type: fact | opinion | inference.
    pub evidence_type: EvidenceType,
    /// Where the evidence came from (tool name, data source, URL description).
    pub source: String,
    /// ISO date when the evidence was observed or published.
    pub timestamp: String,
    /// When the underlying market-relevant event happened, if distinct from
    /// publication or ingestion time.  Optional only because some first-party
    /// technical snapshots have no separate event clock.
    #[serde(default)]
    pub event_time: Option<String>,
    /// When the source made the evidence public, if known.
    #[serde(default)]
    pub published_time: Option<String>,
    /// When Akzio captured the source, if known.
    #[serde(default)]
    pub ingested_time: Option<String>,
    /// The source's stated as-of time, if it has one.
    #[serde(default)]
    pub as_of: Option<String>,
    /// IANA or explicit-offset timezone for the supplied clocks, if known.
    #[serde(default)]
    pub timezone: Option<String>,
    /// Source quality tier: official | major_media | professional_research |
    /// longform_analysis | unknown.
    pub source_tier: String,
    /// Earliest traceable origin of the information (attribution).
    pub first_source: String,
    /// Whether this is a repost / derivative of earlier-reported information.
    pub is_derivative_repost: bool,
    /// Human-readable evidence age: "0-2d" | "3-5d" | "6-10d" | "10d+" | "unknown".
    pub evidence_age: String,
    /// 0.0-1.0 confidence in the quality of the source.
    pub source_confidence: f64,
    /// Complete stable IDs returned by the runtime evidence tools.
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

pub fn validate_evidence_types(
    artifact: &AnalystTickerArtifact,
) -> std::result::Result<(), String> {
    const ALLOWED_SOURCE_TIERS: &[&str] = &[
        "official",
        "major_media",
        "professional_research",
        "longform_analysis",
        "unknown",
    ];
    for evidence in &artifact.key_evidence {
        if !evidence.source_tier.is_empty()
            && !ALLOWED_SOURCE_TIERS.contains(&evidence.source_tier.as_str())
        {
            return Err(format!(
                "invalid source_tier '{}' in evidence '{}'; must be one of: {}",
                evidence.source_tier,
                evidence.claim,
                ALLOWED_SOURCE_TIERS.join(", ")
            ));
        }
        for (field, value) in [
            ("echo_chamber_risk", artifact.echo_chamber_risk.as_str()),
            (
                "crowded_consensus_risk",
                artifact.crowded_consensus_risk.as_str(),
            ),
        ] {
            if !value.is_empty() && !["low", "medium", "high", "unknown"].contains(&value) {
                return Err(format!(
                    "invalid {field} '{value}'; must be low, medium, high, unknown, or empty"
                ));
            }
        }
    }
    Ok(())
}

/// Validate that analyst evidence is real, attributable, timely, and unique.
/// Structural type checks live in [`validate_evidence_types`]; this function
/// owns the stronger admission contract used by live workflow artifacts.
pub fn validate_evidence_quality(
    artifact: &AnalystTickerArtifact,
) -> std::result::Result<(), String> {
    if artifact.report.trim().is_empty() {
        return Err("report must not be empty".to_string());
    }
    if artifact.key_evidence.is_empty() {
        return Err("key_evidence must contain at least one source-backed item".to_string());
    }

    let mut seen = std::collections::BTreeSet::new();
    for evidence in &artifact.key_evidence {
        if evidence.claim.trim().is_empty() {
            return Err("evidence claim must not be empty".to_string());
        }
        if evidence.source.trim().is_empty() {
            return Err(format!(
                "evidence '{}' must include a source",
                evidence.claim
            ));
        }
        if evidence.timestamp.trim().is_empty() {
            return Err(format!(
                "evidence '{}' must include an observation/publication timestamp",
                evidence.claim
            ));
        }
        if !evidence.source_confidence.is_finite()
            || !(0.0..=1.0).contains(&evidence.source_confidence)
        {
            return Err(format!(
                "source_confidence {} out of range for evidence '{}'; must be finite and in [0.0, 1.0]",
                evidence.source_confidence, evidence.claim
            ));
        }
        let dedupe_key = format!(
            "{}\u{1f}{}\u{1f}{}",
            evidence.claim.trim().to_lowercase(),
            evidence.source.trim().to_lowercase(),
            evidence.timestamp.trim()
        );
        if !seen.insert(dedupe_key) {
            return Err(format!("duplicate evidence item: '{}'", evidence.claim));
        }
    }
    Ok(())
}

/// Validate machine-read fields on an analyst per-ticker payload.
///
/// Enforces the contract promised by `analyst_output_contract.md`:
/// `direction`, `confidence`, and `long_probability` must exist and be legal,
/// and evidence typing must pass `validate_evidence_types`.
pub fn validate_analyst_ticker_artifact(
    artifact: &AnalystTickerArtifact,
) -> std::result::Result<(), String> {
    const ALLOWED_DIRECTIONS: &[&str] = &["bullish", "bearish", "neutral", "mixed", "unobserved"];
    if !ALLOWED_DIRECTIONS.contains(&artifact.direction.as_str()) {
        return Err(format!(
            "invalid direction '{}'; must be one of: {}",
            artifact.direction,
            ALLOWED_DIRECTIONS.join(", ")
        ));
    }
    if !(0.0..=1.0).contains(&artifact.confidence) {
        return Err(format!(
            "confidence {} out of range; must be in [0.0, 1.0]",
            artifact.confidence
        ));
    }
    if !artifact.long_probability.is_finite() || !(0.0..=1.0).contains(&artifact.long_probability) {
        return Err(format!(
            "long_probability {} out of range; must be finite and in [0.0, 1.0]",
            artifact.long_probability
        ));
    }
    let direction_matches_probability = match artifact.direction.as_str() {
        "bullish" => artifact.long_probability > 0.5,
        "bearish" => artifact.long_probability < 0.5,
        "neutral" => (artifact.long_probability - 0.5).abs() <= 0.000001,
        "mixed" => (0.4..=0.6).contains(&artifact.long_probability),
        // `unobserved` is a non-contributing diagnostic state. The neutral
        // sentinel prevents it from carrying a hidden directional estimate.
        "unobserved" => (artifact.long_probability - 0.5).abs() <= 0.000001,
        _ => false,
    };
    if !direction_matches_probability {
        return Err(format!(
            "direction {} conflicts with long_probability {}",
            artifact.direction, artifact.long_probability
        ));
    }
    validate_evidence_types(artifact)?;
    validate_evidence_quality(artifact)
}

/// Validate a parsed Canonical Contract v2 `RiskConstraints` artifact.
pub fn validate_risk_constraints(artifact: &RiskConstraints) -> std::result::Result<(), String> {
    if artifact.max_drawdown_pct != 0.0 && !(0.0..=1.0).contains(&artifact.max_drawdown_pct) {
        return Err(format!(
            "max_drawdown_pct {} out of range; must be in [0.0, 1.0] when specified",
            artifact.max_drawdown_pct
        ));
    }
    if artifact.position_cap_pct != 0.0 && !(0.0..=1.0).contains(&artifact.position_cap_pct) {
        return Err(format!(
            "position_cap_pct {} out of range; must be in [0.0, 1.0] when specified",
            artifact.position_cap_pct
        ));
    }
    if artifact.constraint_confidence != 0.0
        && !(0.0..=1.0).contains(&artifact.constraint_confidence)
    {
        return Err(format!(
            "constraint_confidence {} out of range; must be in [0.0, 1.0] when specified",
            artifact.constraint_confidence
        ));
    }
    Ok(())
}

/// Validate the machine-readable sizing and hold semantics of a v2 trade intent.
pub fn validate_trade_intent(artifact: &TradeIntent) -> std::result::Result<(), String> {
    if !matches!(artifact.action.as_str(), "Buy" | "Sell" | "Hold") {
        return Err("trade intent action must be Buy, Sell, or Hold".to_string());
    }
    if !matches!(artifact.candidate_action.as_str(), "Buy" | "Sell" | "Hold") {
        return Err("trade intent candidate_action must be Buy, Sell, or Hold".to_string());
    }
    if !matches!(
        artifact.execution_decision.as_str(),
        "execute_candidate" | "hold"
    ) {
        return Err(
            "trade intent execution_decision must be execute_candidate or hold".to_string(),
        );
    }
    if !artifact.position_size_pct_max.is_finite()
        || !(0.0..=1.0).contains(&artifact.position_size_pct_max)
    {
        return Err("position_size_pct_max must be finite and in [0.0, 1.0]".to_string());
    }
    if (artifact.action == "Hold" || artifact.execution_decision == "hold")
        && artifact.position_size_pct_max > f64::EPSILON
    {
        return Err("held trade intent must use position_size_pct_max=0".to_string());
    }
    if artifact.rationale.trim().is_empty() {
        return Err("trade intent rationale must not be empty".to_string());
    }
    Ok(())
}

/// Validate Phase 6 constraints consumed by the Rust-owned allocation engine.
pub fn validate_asset_execution_constraint(
    artifact: &AssetExecutionConstraint,
) -> std::result::Result<(), String> {
    if !matches!(
        artifact.direction_constraint.as_str(),
        "increase_only" | "decrease_only" | "unchanged"
    ) {
        return Err(
            "direction_constraint must be increase_only, decrease_only, or unchanged".to_string(),
        );
    }
    if !matches!(
        artifact.execution_status.as_str(),
        "execute" | "wait" | "downgrade"
    ) {
        return Err("execution_status must be execute, wait, or downgrade".to_string());
    }
    for (field, value) in [
        ("current_weight", artifact.current_weight),
        ("max_target_weight", artifact.max_target_weight),
        ("max_weight_delta", artifact.max_weight_delta),
    ] {
        if !value.is_finite() || !(0.0..=1.0).contains(&value) {
            return Err(format!("{field} must be finite and in [0.0, 1.0]"));
        }
    }
    for control in &artifact.binding_risk_controls {
        if control.control.trim().is_empty() {
            return Err("binding risk control must not be empty".to_string());
        }
        if control.source_refs.is_empty()
            || control
                .source_refs
                .iter()
                .any(|reference| reference.trim().is_empty())
        {
            return Err(
                "binding risk control source_refs must contain non-empty source references"
                    .to_string(),
            );
        }
    }
    Ok(())
}

/// Validate a Phase 6 v2 artifact, including every per-asset constraint.
pub fn validate_final_validation(artifact: &FinalValidation) -> std::result::Result<(), String> {
    if !matches!(
        artifact.execution_status.as_str(),
        "execute" | "wait" | "downgrade"
    ) {
        return Err("execution_status must be execute, wait, or downgrade".to_string());
    }
    if artifact.execution_summary.trim().is_empty() {
        return Err("execution_summary must not be empty".to_string());
    }
    for (ticker, constraint) in &artifact.per_asset {
        validate_asset_execution_constraint(constraint)
            .map_err(|error| format!("per_asset.{ticker}: {error}"))?;
    }
    Ok(())
}

#[derive(Debug, Error, PartialEq)]
pub enum ValidationError {
    #[error("probability field {0} is invalid")]
    InvalidProbability(String),
    #[error("long_probability + short_probability must be approximately 1.0")]
    ProbabilitySum,
    #[error("scenario probabilities must sum to 1.0 (got {0})")]
    ScenarioProbabilitySum(f64),
    #[error("long_probability ({long}) inconsistent with scenarios (expected ~{expected})")]
    InconsistentLongProbability { long: f64, expected: f64 },
    #[error("confidence_basis is invalid: {0}")]
    InvalidConfidenceBasis(String),
    #[error("hold_reason is invalid: {0}")]
    InvalidHoldReason(String),
    #[error("research decision field is invalid: {0}")]
    InvalidResearchField(String),
}

pub fn normalize_probability(value: &Value) -> Option<f64> {
    let parsed = match value {
        Value::Number(number) => number.as_f64()?,
        Value::String(text) => {
            let trimmed = text.trim();
            if let Some(percent) = trimmed.strip_suffix('%') {
                percent.trim().parse::<f64>().ok()? / 100.0
            } else {
                trimmed.parse::<f64>().ok()?
            }
        }
        _ => return None,
    };
    if (0.0..=1.0).contains(&parsed) {
        Some((parsed * 10_000.0).round() / 10_000.0)
    } else if (1.0..=100.0).contains(&parsed) {
        Some(((parsed / 100.0) * 10_000.0).round() / 10_000.0)
    } else {
        None
    }
}

/// Deterministic five-level research rating derived from long probability.
/// Prompt text may explain the semantics, but Rust owns this mapping.
pub fn research_rating_for_probability(long_probability: f64) -> &'static str {
    if long_probability >= 0.68 {
        "Buy"
    } else if long_probability >= 0.56 {
        "Overweight"
    } else if long_probability >= 0.45 {
        "Hold"
    } else if long_probability >= 0.33 {
        "Underweight"
    } else {
        "Sell"
    }
}

pub fn validate_research_decision(
    artifact: &ResearchDecision,
) -> std::result::Result<(), ValidationError> {
    let valid_confidence_basis = [
        "evidence_balanced",
        "data_insufficient",
        "conflicting_evidence",
        "directional_evidence",
    ];
    if !valid_confidence_basis.contains(&artifact.confidence_basis.as_str()) {
        return Err(ValidationError::InvalidConfidenceBasis(
            artifact.confidence_basis.clone(),
        ));
    }
    let expected_rating = research_rating_for_probability(artifact.long_probability);
    if artifact.rating != expected_rating {
        return Err(ValidationError::InvalidResearchField(format!(
            "rating {:?} does not match Rust mapping {:?} for long_probability {}",
            artifact.rating, expected_rating, artifact.long_probability
        )));
    }
    if artifact.rating.eq_ignore_ascii_case("hold") {
        let expected_hold_reason = match artifact.confidence_basis.as_str() {
            "evidence_balanced" => "evidence_balanced",
            "data_insufficient" => "evidence_insufficient",
            "conflicting_evidence" => "conflicting_evidence",
            other => {
                return Err(ValidationError::InvalidHoldReason(format!(
                    "Hold cannot use confidence_basis={other}"
                )))
            }
        };
        if artifact.hold_reason.as_deref() != Some(expected_hold_reason) {
            return Err(ValidationError::InvalidHoldReason(format!(
                "expected {expected_hold_reason} for confidence_basis={}",
                artifact.confidence_basis
            )));
        }
    }
    if artifact.plan.trim().is_empty() {
        return Err(ValidationError::InvalidResearchField(
            "plan must not be empty".to_string(),
        ));
    }
    if artifact.probability_rationale.trim().is_empty() {
        return Err(ValidationError::InvalidResearchField(
            "probability_rationale must not be empty".to_string(),
        ));
    }
    if !(0.0..=1.0).contains(&artifact.long_probability) {
        return Err(ValidationError::InvalidProbability(
            "long_probability".to_string(),
        ));
    }
    if !(0.0..=1.0).contains(&artifact.short_probability) {
        return Err(ValidationError::InvalidProbability(
            "short_probability".to_string(),
        ));
    }
    if (artifact.long_probability + artifact.short_probability - 1.0).abs() > 0.03 {
        return Err(ValidationError::ProbabilitySum);
    }
    if let Some(scenarios) = &artifact.scenarios {
        let sum =
            scenarios.bull.probability + scenarios.base.probability + scenarios.bear.probability;
        if (sum - 1.0).abs() > 0.03 {
            return Err(ValidationError::ScenarioProbabilitySum(sum));
        }

        let expected_long = scenario_expected_long_probability(scenarios);
        if (artifact.long_probability - expected_long).abs() > 0.05 {
            return Err(ValidationError::InconsistentLongProbability {
                long: artifact.long_probability,
                expected: expected_long,
            });
        }

        for (name, scenario) in [
            ("bull", &scenarios.bull),
            ("base", &scenarios.base),
            ("bear", &scenarios.bear),
        ] {
            if !(0.0..=1.0).contains(&scenario.probability) {
                return Err(ValidationError::InvalidProbability(format!(
                    "scenario {name}.probability"
                )));
            }
            if !(0.0..=1.0).contains(&scenario.conditional_long_probability) {
                return Err(ValidationError::InvalidProbability(format!(
                    "scenario {name}.conditional_long_probability"
                )));
            }
            if scenario.drivers.is_empty() {
                return Err(ValidationError::InvalidProbability(format!(
                    "scenario {name} must have at least 1 driver"
                )));
            }
            if scenario.triggers.is_empty() {
                return Err(ValidationError::InvalidProbability(format!(
                    "scenario {name} must have at least 1 trigger"
                )));
            }
            if scenario
                .drivers
                .iter()
                .any(|driver| scenario_driver_is_missing_evidence_placeholder(driver))
            {
                return Err(ValidationError::InvalidProbability(format!(
                    "scenario {name} drivers must describe causal market factors, not missing evidence"
                )));
            }
        }
        if scenarios.bull.conditional_long_probability < scenarios.base.conditional_long_probability
            || scenarios.base.conditional_long_probability
                < scenarios.bear.conditional_long_probability
        {
            return Err(ValidationError::InvalidResearchField(
                "scenario conditional_long_probability must be ordered bull >= base >= bear"
                    .to_owned(),
            ));
        }
    }
    Ok(())
}

/// The expected long-outcome probability implied by a fully specified
/// three-regime scenario tree.  Keeping the calculation here avoids an
/// undocumented assumption such as treating every base scenario as 50/50.
pub fn scenario_expected_long_probability(scenarios: &Scenarios) -> f64 {
    scenarios.bull.probability * scenarios.bull.conditional_long_probability
        + scenarios.base.probability * scenarios.base.conditional_long_probability
        + scenarios.bear.probability * scenarios.bear.conditional_long_probability
}

fn scenario_driver_is_missing_evidence_placeholder(driver: &str) -> bool {
    let normalized = driver.trim().to_ascii_lowercase();
    [
        "no actionable",
        "no evidence",
        "evidence is insufficient",
        "evidence insufficient",
        "missing evidence",
        "缺乏证据",
        "证据不足",
        "没有证据",
    ]
    .iter()
    .any(|phrase| normalized.contains(phrase))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn valid_analyst_ticker_artifact() -> AnalystTickerArtifact {
        AnalystTickerArtifact {
            direction: "bullish".to_string(),
            confidence: 0.7,
            long_probability: 0.62,
            report: "QQQ remains above its 20-day average.".to_string(),
            key_evidence: vec![EvidenceItem {
                claim: "QQQ closed above its 20-day average.".to_string(),
                evidence_type: EvidenceType::Fact,
                source: "Yahoo Finance daily OHLCV".to_string(),
                timestamp: "2026-07-22".to_string(),
                event_time: None,
                published_time: None,
                ingested_time: None,
                as_of: Some("2026-07-22".to_string()),
                timezone: None,
                source_tier: "official".to_string(),
                first_source: "Yahoo Finance".to_string(),
                is_derivative_repost: false,
                evidence_age: "0-2d".to_string(),
                source_confidence: 0.9,
                evidence_refs: vec![format!("technical-{}", "a".repeat(64))],
            }],
            priced_in: "unclear".to_string(),
            echo_chamber_risk: "low".to_string(),
            crowded_consensus_risk: "low".to_string(),
            validation_triggers: vec!["Close below the 20-day average".to_string()],
            data_gaps: Vec::new(),
        }
    }

    #[test]
    fn analyst_validation_rejects_empty_evidence() {
        let mut artifact = valid_analyst_ticker_artifact();
        artifact.key_evidence.clear();

        let error = validate_analyst_ticker_artifact(&artifact).unwrap_err();

        assert!(error.contains("key_evidence"));
    }

    #[test]
    fn analyst_rejects_legacy_evidence_source_tier() {
        let mut artifact = valid_analyst_ticker_artifact();
        artifact.key_evidence[0].source_tier = "T1_reference".to_string();

        assert!(validate_analyst_ticker_artifact(&artifact).is_err());
    }

    #[test]
    fn analyst_validation_rejects_unattributed_evidence() {
        let mut artifact = valid_analyst_ticker_artifact();
        artifact.key_evidence[0].source.clear();

        let error = validate_analyst_ticker_artifact(&artifact).unwrap_err();

        assert!(error.contains("source"));
    }

    #[test]
    fn analyst_validation_rejects_duplicate_evidence() {
        let mut artifact = valid_analyst_ticker_artifact();
        artifact.key_evidence.push(artifact.key_evidence[0].clone());

        let error = validate_analyst_ticker_artifact(&artifact).unwrap_err();

        assert!(error.contains("duplicate evidence"));
    }

    #[test]
    fn normalizes_percent_strings() {
        assert_eq!(normalize_probability(&json!("68%")), Some(0.68));
        assert_eq!(normalize_probability(&json!(68)), Some(0.68));
        assert_eq!(normalize_probability(&json!(0.68)), Some(0.68));
    }

    #[test]
    fn evidence_item_deserializes_structured() {
        let json = r#"{
            "claim": "CPI came in at 3.2%",
            "evidence_type": "fact",
            "source": "BLS via Jin10",
            "timestamp": "2026-07-06",
            "source_tier": "official",
            "first_source": "BLS release",
            "is_derivative_repost": false,
            "evidence_age": "0-2d",
            "source_confidence": 0.9
        }"#;
        let item: EvidenceItem = serde_json::from_str(json).unwrap();
        assert_eq!(item.claim, "CPI came in at 3.2%");
        assert_eq!(item.evidence_type, EvidenceType::Fact);
        assert_eq!(item.source, "BLS via Jin10");
        assert_eq!(item.timestamp, "2026-07-06");
    }

    #[test]
    fn analyst_artifact_rejects_legacy_string_evidence() {
        let json = r#"{
            "direction": "bullish",
            "confidence": 0.7,
            "key_evidence": ["simple string evidence"]
        }"#;
        assert!(serde_json::from_str::<AnalystTickerArtifact>(json).is_err());
    }

    #[test]
    fn analyst_artifact_accepts_structured_evidence() {
        let json = r#"{
            "direction": "bullish",
            "confidence": 0.7,
            "long_probability": 0.62,
            "report": "CPI evidence supports the call.",
            "key_evidence": [
                {"claim": "CPI 3.2%", "evidence_type": "fact", "source": "BLS", "timestamp": "2026-07-06", "source_tier":"official", "first_source":"BLS", "is_derivative_repost":false, "evidence_age":"0-2d", "source_confidence":0.9}
            ],
            "priced_in": "unclear",
            "echo_chamber_risk": "low",
            "crowded_consensus_risk": "low",
            "validation_triggers": [],
            "data_gaps": []
        }"#;
        let artifact: AnalystTickerArtifact = serde_json::from_str(json).unwrap();
        assert_eq!(artifact.key_evidence[0].claim, "CPI 3.2%");
        assert_eq!(artifact.key_evidence[0].evidence_type, EvidenceType::Fact);
    }

    #[test]
    fn analyst_artifact_rejects_legacy_evidence_types() {
        let json = r#"{
            "direction": "mixed",
            "confidence": 0.5,
            "key_evidence": [
                {"claim": "Options rumor", "evidence_type": "speculation", "source": "unverified market report"}
            ]
        }"#;
        assert!(serde_json::from_str::<AnalystTickerArtifact>(json).is_err());
    }

    #[test]
    fn evidence_type_is_closed_to_v2_variants() {
        for value in ["fact", "opinion", "inference"] {
            let json = format!("{{\"claim\":\"x\",\"evidence_type\":\"{value}\",\"source\":\"s\",\"timestamp\":\"2026-07-06\",\"source_tier\":\"official\",\"first_source\":\"s\",\"is_derivative_repost\":false,\"evidence_age\":\"0-2d\",\"source_confidence\":0.9}}");
            serde_json::from_str::<EvidenceItem>(&json).unwrap();
        }
        assert!(serde_json::from_str::<EvidenceItem>(
            r#"{"claim":"x","evidence_type":"speculation"}"#
        )
        .is_err());
    }

    #[test]
    fn analyst_artifact_rejects_legacy_aliases_and_duplicate_fields() {
        let json = r#"{
            "direction": "bearish",
            "confidence": 0.62,
            "crowded_consensus_risk": "medium",
            "report": "short prose",
            "key_evidence": [{
                "evidence_type": "fact",
                "source": "Yahoo Finance",
                "timestamp": "2026-07-22",
                "source_tier": "major_media",
                "evidence_age": "0-2d",
                "catalyst_age": "0-2d",
                "assessment": "半导体权重同步走弱",
                "source_confidence": 0.72
            }]
        }"#;
        assert!(serde_json::from_str::<AnalystTickerArtifact>(json).is_err());
        assert!(serde_json::from_str::<EvidenceItem>(
            r#"{"claim":"x","evidence_type":"fact","catalyst_age":"0-2d"}"#
        )
        .is_err());
    }

    #[test]
    fn validate_evidence_types_rejects_invalid_source_tier() {
        let artifact = AnalystTickerArtifact {
            direction: "bullish".to_string(),
            confidence: 0.7,
            long_probability: 0.62,
            report: String::new(),
            key_evidence: vec![EvidenceItem {
                claim: "a claim".to_string(),
                evidence_type: EvidenceType::Fact,
                source: String::new(),
                timestamp: String::new(),
                event_time: None,
                published_time: None,
                ingested_time: None,
                as_of: None,
                timezone: None,
                source_tier: "garbage".to_string(),
                first_source: String::new(),
                is_derivative_repost: false,
                evidence_age: String::new(),
                source_confidence: 0.0,
                evidence_refs: Vec::new(),
            }],
            priced_in: String::new(),
            echo_chamber_risk: String::new(),
            crowded_consensus_risk: String::new(),
            validation_triggers: Vec::new(),
            data_gaps: Vec::new(),
        };
        let error = validate_evidence_types(&artifact).unwrap_err();
        assert!(error.contains("invalid source_tier 'garbage'"));
    }

    #[test]
    fn validate_evidence_types_rejects_invalid_echo_chamber_risk() {
        let artifact = AnalystTickerArtifact {
            direction: "bullish".to_string(),
            confidence: 0.7,
            long_probability: 0.62,
            report: String::new(),
            key_evidence: vec![EvidenceItem {
                claim: "a claim".to_string(),
                evidence_type: EvidenceType::Fact,
                source: String::new(),
                timestamp: String::new(),
                event_time: None,
                published_time: None,
                ingested_time: None,
                as_of: None,
                timezone: None,
                source_tier: String::new(),
                first_source: String::new(),
                is_derivative_repost: false,
                evidence_age: String::new(),
                source_confidence: 0.0,
                evidence_refs: Vec::new(),
            }],
            priced_in: String::new(),
            echo_chamber_risk: "extreme".to_string(),
            crowded_consensus_risk: String::new(),
            validation_triggers: Vec::new(),
            data_gaps: Vec::new(),
        };
        let error = validate_evidence_types(&artifact).unwrap_err();
        assert!(error.contains("invalid echo_chamber_risk 'extreme'"));
    }

    #[test]
    fn validate_risk_constraints_rejects_out_of_range_drawdown() {
        let artifact = RiskConstraints {
            stance: "neutral".to_string(),
            argument: String::new(),
            unique_risk_contribution: String::new(),
            disagreement_with_prior: String::new(),
            no_new_information: false,
            recommended_adjustment: String::new(),
            stop_type: StopType::None,
            max_drawdown_pct: 1.5,
            position_cap_pct: 0.0,
            rebalance_trigger: String::new(),
            risk_off_trigger: String::new(),
            review_window: String::new(),
            cash_hedge_recommendation: String::new(),
            constraint_confidence: 0.0,
        };
        let error = validate_risk_constraints(&artifact).unwrap_err();
        assert!(error.contains("max_drawdown_pct 1.5 out of range"));
    }

    #[test]
    fn risk_constraints_reject_legacy_stop_type() {
        let error = serde_json::from_str::<RiskConstraints>(
            r#"{"stance":"neutral","stop_type":"trailing"}"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("trailing"));
    }

    #[test]
    fn analyst_artifact_with_new_fields_round_trips() {
        let json = r#"{
            "direction": "bullish",
            "confidence": 0.7,
            "long_probability": 0.62,
            "report": "CPI evidence supports the call.",
            "echo_chamber_risk": "medium",
            "crowded_consensus_risk": "high",
            "key_evidence": [
                {
                    "claim": "CPI 3.2%",
                    "evidence_type": "fact",
                    "source": "BLS",
                    "timestamp": "2026-07-06",
                    "source_tier": "official",
                    "first_source": "BLS release",
                    "is_derivative_repost": false,
                    "evidence_age": "0-2d",
                    "source_confidence": 0.9
                }
            ],
            "priced_in": "unclear",
            "validation_triggers": [],
            "data_gaps": []
        }"#;
        let artifact: AnalystTickerArtifact = serde_json::from_str(json).unwrap();
        assert_eq!(artifact.echo_chamber_risk, "medium");
        assert_eq!(artifact.crowded_consensus_risk, "high");
        assert_eq!(artifact.key_evidence[0].source_tier, "official");
        assert_eq!(artifact.key_evidence[0].first_source, "BLS release");
        assert!(!artifact.key_evidence[0].is_derivative_repost);
        assert_eq!(artifact.key_evidence[0].evidence_age, "0-2d");
        assert!((artifact.key_evidence[0].source_confidence - 0.9).abs() < f64::EPSILON);
    }

    #[test]
    fn risk_constraints_with_new_fields_round_trips() {
        let json = r#"{
            "stance": "conservative",
            "argument": "Volatility remains elevated.",
            "unique_risk_contribution": "Gap risk is not covered by the base stop.",
            "disagreement_with_prior": "none",
            "no_new_information": false,
            "recommended_adjustment": "Cap exposure at 50%.",
            "stop_type": "soft",
            "max_drawdown_pct": 0.15,
            "position_cap_pct": 0.5,
            "rebalance_trigger": "VIX > 25",
            "risk_off_trigger": "Overnight gap > 3%",
            "review_window": "3d",
            "cash_hedge_recommendation": "Hold 20% cash.",
            "constraint_confidence": 0.8
        }"#;
        let artifact: RiskConstraints = serde_json::from_str(json).unwrap();
        assert_eq!(artifact.stop_type, StopType::Soft);
        assert!((artifact.max_drawdown_pct - 0.15).abs() < f64::EPSILON);
        assert!((artifact.position_cap_pct - 0.5).abs() < f64::EPSILON);
        assert_eq!(artifact.rebalance_trigger, "VIX > 25");
        assert_eq!(artifact.risk_off_trigger, "Overnight gap > 3%");
        assert_eq!(artifact.review_window, "3d");
        assert_eq!(artifact.cash_hedge_recommendation, "Hold 20% cash.");
        assert!((artifact.constraint_confidence - 0.8).abs() < f64::EPSILON);
        assert!(validate_risk_constraints(&artifact).is_ok());
    }

    #[test]
    fn trade_intent_v2_requires_numeric_cap_and_rejects_legacy_size() {
        let valid: TradeIntent = serde_json::from_str(
            r#"{
                "action":"Buy",
                "candidate_action":"Buy",
                "execution_decision":"execute_candidate",
                "position_size_pct_max":0.25,
                "blockers":[],
                "rationale":"The evidence supports a bounded entry."
            }"#,
        )
        .unwrap();
        validate_trade_intent(&valid).unwrap();

        assert!(serde_json::from_str::<TradeIntent>(
            r#"{"action":"Buy","candidate_action":"Buy","execution_decision":"execute_candidate","blockers":[],"rationale":"missing cap"}"#
        )
        .is_err());
    }

    #[test]
    fn trade_intent_v2_enforces_hold_zero_cap() {
        let artifact = TradeIntent {
            action: "Hold".to_string(),
            candidate_action: "Hold".to_string(),
            execution_decision: "hold".to_string(),
            entry_price: None,
            stop_loss: None,
            position_size_pct_max: 0.1,
            blockers: Vec::new(),
            rationale: "No executable edge exists.".to_string(),
        };
        assert!(validate_trade_intent(&artifact)
            .unwrap_err()
            .contains("position_size_pct_max=0"));
    }

    #[test]
    fn asset_execution_constraint_v2_requires_structured_binding_refs() {
        let constraint: AssetExecutionConstraint = serde_json::from_str(
            r#"{
                "direction_constraint":"increase_only",
                "execution_status":"execute",
                "current_weight":0.1,
                "max_target_weight":0.25,
                "max_weight_delta":0.15,
                "binding_risk_controls":[{
                    "control":"Cap exposure during elevated volatility.",
                    "source_refs":["idx-risk-qqq", "detail-risk-qqq-1"]
                }]
            }"#,
        )
        .unwrap();
        validate_asset_execution_constraint(&constraint).unwrap();

        let legacy = r#"{
            "direction_constraint":"increase_only",
            "execution_status":"execute",
            "current_weight":0.1,
            "max_target_weight":0.25,
            "max_weight_delta":0.15,
            "binding_risk_controls":["Cap exposure"]
        }"#;
        assert!(serde_json::from_str::<AssetExecutionConstraint>(legacy).is_err());
    }

    fn valid_scenarios() -> Scenarios {
        Scenarios {
            bull: Scenario {
                probability: 0.35,
                conditional_long_probability: 0.8,
                drivers: vec!["Fed cut".to_string()],
                triggers: vec!["FOMC minutes".to_string()],
                confirmation: "Close above 500".to_string(),
            },
            base: Scenario {
                probability: 0.45,
                conditional_long_probability: 0.6,
                drivers: vec!["Range-bound".to_string()],
                triggers: vec!["VIX below 20".to_string()],
                confirmation: "5 days in range".to_string(),
            },
            bear: Scenario {
                probability: 0.20,
                conditional_long_probability: 0.125,
                drivers: vec!["Inflation".to_string()],
                triggers: vec!["CPI above 3.5%".to_string()],
                confirmation: "Close below 475".to_string(),
            },
        }
    }

    fn research_decision_with_scenarios(scenarios: Option<Scenarios>) -> ResearchDecision {
        ResearchDecision {
            rating: "Overweight".to_string(),
            long_probability: 0.575,
            short_probability: 0.425,
            confidence_basis: "directional_evidence".to_string(),
            hold_reason: None,
            plan: "Monitor validation triggers.".to_string(),
            probability_rationale: "Evidence is balanced near the base probability.".to_string(),
            scenarios,
        }
    }

    #[test]
    fn research_decision_with_scenarios_validates() {
        let artifact = research_decision_with_scenarios(Some(valid_scenarios()));
        assert!(validate_research_decision(&artifact).is_ok());
    }

    #[test]
    fn scenario_probabilities_must_sum_to_one() {
        let artifact = research_decision_with_scenarios(Some(Scenarios {
            bull: Scenario {
                probability: 0.4,
                conditional_long_probability: 0.8,
                drivers: vec!["x".into()],
                triggers: vec!["y".into()],
                confirmation: "z".into(),
            },
            base: Scenario {
                probability: 0.4,
                conditional_long_probability: 0.5,
                drivers: vec!["x".into()],
                triggers: vec!["y".into()],
                confirmation: "z".into(),
            },
            bear: Scenario {
                probability: 0.4,
                conditional_long_probability: 0.2,
                drivers: vec!["x".into()],
                triggers: vec!["y".into()],
                confirmation: "z".into(),
            },
        }));

        assert!(matches!(
            validate_research_decision(&artifact),
            Err(ValidationError::ScenarioProbabilitySum(sum)) if (sum - 1.2).abs() < 0.001
        ));
    }

    #[test]
    fn inconsistent_long_probability_is_rejected() {
        let artifact = ResearchDecision {
            rating: "Buy".to_string(),
            long_probability: 0.7,
            short_probability: 0.3,
            ..research_decision_with_scenarios(Some(Scenarios {
                bull: Scenario {
                    probability: 0.2,
                    conditional_long_probability: 0.8,
                    drivers: vec!["x".into()],
                    triggers: vec!["y".into()],
                    confirmation: "z".into(),
                },
                base: Scenario {
                    probability: 0.5,
                    conditional_long_probability: 0.5,
                    drivers: vec!["x".into()],
                    triggers: vec!["y".into()],
                    confirmation: "z".into(),
                },
                bear: Scenario {
                    probability: 0.3,
                    conditional_long_probability: 0.1,
                    drivers: vec!["x".into()],
                    triggers: vec!["y".into()],
                    confirmation: "z".into(),
                },
            }))
        };

        assert!(matches!(
            validate_research_decision(&artifact),
            Err(ValidationError::InconsistentLongProbability { long, expected })
                if (long - 0.7).abs() < 0.001 && (expected - 0.44).abs() < 0.001
        ));
    }

    #[test]
    fn scenario_tree_uses_conditional_outcome_probabilities() {
        let scenarios = valid_scenarios();

        assert!((scenario_expected_long_probability(&scenarios) - 0.575).abs() < 0.000001);
        assert!(
            validate_research_decision(&research_decision_with_scenarios(Some(scenarios))).is_ok()
        );
    }

    #[test]
    fn scenario_tree_rejects_reversed_bull_and_bear_conditionals() {
        let mut scenarios = valid_scenarios();
        scenarios.bull.conditional_long_probability = 0.4;
        let mut artifact = research_decision_with_scenarios(Some(scenarios));
        artifact.long_probability = 0.435;
        artifact.short_probability = 0.565;
        artifact.rating = "Underweight".to_owned();

        assert!(matches!(
            validate_research_decision(&artifact),
            Err(ValidationError::InvalidResearchField(message))
                if message.contains("bull >= base >= bear")
        ));
    }

    #[test]
    fn research_decision_requires_a_confidence_basis() {
        let mut artifact = research_decision_with_scenarios(None);
        artifact.confidence_basis.clear();

        assert!(matches!(
            validate_research_decision(&artifact),
            Err(ValidationError::InvalidConfidenceBasis(_))
        ));
    }

    #[test]
    fn hold_research_decision_requires_a_hold_reason() {
        let mut artifact = research_decision_with_scenarios(None);
        artifact.rating = "Hold".to_string();
        artifact.long_probability = 0.5;
        artifact.short_probability = 0.5;
        artifact.confidence_basis = "evidence_balanced".to_string();
        artifact.hold_reason = None;

        assert!(matches!(
            validate_research_decision(&artifact),
            Err(ValidationError::InvalidHoldReason(_))
        ));
    }

    #[test]
    fn scenario_drivers_are_required() {
        let mut scenarios = valid_scenarios();
        scenarios.bull.drivers.clear();
        let artifact = research_decision_with_scenarios(Some(scenarios));

        assert!(matches!(
            validate_research_decision(&artifact),
            Err(ValidationError::InvalidProbability(message))
                if message == "scenario bull must have at least 1 driver"
        ));
    }

    #[test]
    fn scenario_drivers_reject_missing_evidence_as_a_causal_factor() {
        let mut scenarios = valid_scenarios();
        scenarios.bull.drivers = vec!["No actionable bullish evidence is available".into()];
        let artifact = research_decision_with_scenarios(Some(scenarios));

        assert!(matches!(
            validate_research_decision(&artifact),
            Err(ValidationError::InvalidProbability(message))
                if message.contains("causal market factors")
        ));
    }

    #[test]
    fn scenario_triggers_are_required() {
        let mut scenarios = valid_scenarios();
        scenarios.bear.triggers.clear();
        let artifact = research_decision_with_scenarios(Some(scenarios));

        assert!(matches!(
            validate_research_decision(&artifact),
            Err(ValidationError::InvalidProbability(message))
                if message == "scenario bear must have at least 1 trigger"
        ));
    }

    #[test]
    fn research_decision_without_scenarios_still_validates() {
        let artifact = research_decision_with_scenarios(None);
        assert!(validate_research_decision(&artifact).is_ok());
    }

    #[test]
    fn research_decision_without_scenarios_deserializes() {
        let json = r#"{
            "rating": "Hold",
            "long_probability": 0.55,
            "short_probability": 0.45,
            "confidence_basis": "evidence_balanced",
            "hold_reason": "evidence_balanced",
            "plan": "Monitor validation triggers.",
            "probability_rationale": "Evidence is balanced."
        }"#;
        let artifact: ResearchDecision = serde_json::from_str(json).unwrap();
        assert_eq!(artifact.scenarios, None);
        assert!(validate_research_decision(&artifact).is_ok());
    }

    #[test]
    fn rust_owns_research_rating_mapping() {
        for (probability, rating) in [
            (0.68, "Buy"),
            (0.56, "Overweight"),
            (0.50, "Hold"),
            (0.33, "Underweight"),
            (0.32, "Sell"),
        ] {
            assert_eq!(research_rating_for_probability(probability), rating);
        }
    }

    #[test]
    fn validate_analyst_ticker_artifact_rejects_bad_direction() {
        let artifact = AnalystTickerArtifact {
            direction: "sideways".to_string(),
            confidence: 0.5,
            long_probability: 0.5,
            report: String::new(),
            key_evidence: Vec::new(),
            priced_in: String::new(),
            echo_chamber_risk: String::new(),
            crowded_consensus_risk: String::new(),
            validation_triggers: Vec::new(),
            data_gaps: Vec::new(),
        };
        let err = validate_analyst_ticker_artifact(&artifact).unwrap_err();
        assert!(err.contains("invalid direction"));
    }

    #[test]
    fn analyst_validation_keeps_probability_distinct_from_evidence_quality() {
        let mut artifact = valid_analyst_ticker_artifact();
        artifact.long_probability = 0.4;

        let error = validate_analyst_ticker_artifact(&artifact).unwrap_err();

        assert!(error.contains("conflicts with long_probability"));
    }

    #[test]
    fn validate_analyst_ticker_artifact_accepts_valid_payload() {
        let artifact = valid_analyst_ticker_artifact();
        validate_analyst_ticker_artifact(&artifact).unwrap();
    }
}
