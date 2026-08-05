use super::{
    AnalystTickerArtifact, AssetExecutionConstraint, FinalValidation, ResearchDecision,
    RiskConstraints, Scenarios, TradeIntent,
};
use serde_json::Value;
use thiserror::Error;

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
        let explicit_unobserved_gap = artifact.direction == "unobserved"
            && artifact.confidence.abs() <= f64::EPSILON
            && (artifact.long_probability - 0.5).abs() <= 0.000001
            && !artifact.data_gaps.is_empty();
        if explicit_unobserved_gap {
            return Ok(());
        }
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
