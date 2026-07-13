use anyhow::{anyhow, Context, Result};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArtifactEnvelope {
    pub id: String,
    pub role: String,
    #[serde(default)]
    pub report: String,
    #[serde(default)]
    pub per_ticker: BTreeMap<String, Value>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ResearchArtifact {
    pub rating: String,
    pub long_probability: f64,
    pub short_probability: f64,
    #[serde(default)]
    pub plan: String,
    #[serde(default)]
    pub probability_rationale: String,
    /// Three-scenario analysis: bull, base, bear. Optional for backward
    /// compatibility with legacy research artifacts.
    #[serde(default)]
    pub scenarios: Option<Scenarios>,
    #[serde(default)]
    pub per_ticker: BTreeMap<String, Value>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// A single scenario (bull, base, or bear) in the research manager's
/// scenario analysis output.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct Scenario {
    /// Probability of this scenario (0.0-1.0). All scenarios must sum to 1.0.
    pub probability: f64,
    /// Key drivers that would cause this scenario to play out (1-3 items).
    #[serde(default)]
    pub drivers: Vec<String>,
    /// Observable triggers that would shift probability toward this scenario (1-3 items).
    #[serde(default)]
    pub triggers: Vec<String>,
    /// What would confirm this scenario is the active path.
    #[serde(default)]
    pub confirmation: String,
}

/// Container for the three scenarios.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct Scenarios {
    pub bull: Scenario,
    pub base: Scenario,
    pub bear: Scenario,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct TradeIntent {
    pub action: String,
    #[serde(default)]
    pub entry_price: Option<String>,
    #[serde(default)]
    pub stop_loss: Option<String>,
    #[serde(default)]
    pub position_size: String,
    #[serde(default)]
    pub rationale: String,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct RiskConstraints {
    pub stance: String,
    #[serde(default)]
    pub argument: String,
    #[serde(default)]
    pub recommended_adjustment: String,
    /// none | tight | trailing | event_based | time_based
    #[serde(default)]
    pub stop_type: String,
    /// 0.0-1.0 fraction of capital at risk before stopping.
    #[serde(default)]
    pub max_drawdown_pct: f64,
    /// 0.0-1.0 maximum single-position weight cap.
    #[serde(default)]
    pub position_cap_pct: f64,
    /// Condition that triggers a portfolio rebalance.
    #[serde(default)]
    pub rebalance_trigger: String,
    /// Condition that forces a risk-off / de-risk event.
    #[serde(default)]
    pub risk_off_trigger: String,
    /// How long until the risk view is revisited (human readable).
    #[serde(default)]
    pub review_window: String,
    /// Cash-hedge recommendation (size / instrument / rationale).
    #[serde(default)]
    pub cash_hedge_recommendation: String,
    /// 0.0-1.0 confidence in the constraints themselves.
    #[serde(default)]
    pub constraint_confidence: f64,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct FinalValidation {
    pub rating: String,
    #[serde(default)]
    pub execution_summary: String,
    #[serde(default)]
    pub investment_thesis: String,
    #[serde(default)]
    pub target_price: Option<String>,
    #[serde(default)]
    pub horizon: String,
    #[serde(default)]
    pub risk_controls: Vec<String>,
    #[serde(default)]
    pub rationale: String,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct PortfolioAllocation {
    pub weights: BTreeMap<String, Value>,
    pub total_equity_exposure: f64,
    #[serde(default)]
    pub vix_regime: String,
    #[serde(default)]
    pub correlation_note: String,
    #[serde(default)]
    pub summary: String,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Per-ticker payload every analyst (technical / news_macro / reddit / x /
/// youtube) must emit. This is the single source of truth for the analyst
/// output contract: `prompts/common/analyst_output_contract.md` documents it in
/// prose, and `analyst_artifact_schema()` derives the machine schema injected
/// into those prompts so the two never drift.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct AnalystTickerArtifact {
    /// bullish | bearish | neutral | mixed | unobserved
    pub direction: String,
    /// Evidence-consistency / clarity, 0.0-1.0 (NOT 0-100, NOT upside probability).
    pub confidence: f64,
    /// Full prose analysis for this ticker (may contain sections / Markdown tables).
    #[serde(default)]
    pub report: String,
    /// The 2-3 most decisive evidence items.
    ///
    /// Structured `EvidenceItem` objects are preferred. Legacy plain-string
    /// entries are accepted during deserialization and normalized to
    /// `evidence_type = "unclassified"`.
    #[serde(default, deserialize_with = "deserialize_key_evidence")]
    pub key_evidence: Vec<EvidenceItem>,
    /// already_priced | under_priced | unclear
    #[serde(default)]
    pub priced_in: String,
    /// low | medium | high
    #[serde(default)]
    pub echo_chamber_risk: String,
    /// low | medium | high
    #[serde(default)]
    pub crowded_consensus_risk: String,
    /// Observations that would strengthen or overturn the current call.
    #[serde(default)]
    pub validation_triggers: Vec<String>,
    /// Data gaps and uncertainties; empty array when none.
    #[serde(default)]
    pub data_gaps: Vec<String>,
}

/// A single piece of evidence with type classification.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct EvidenceItem {
    /// The evidence claim in 1-2 sentences.
    pub claim: String,
    /// Evidence type: "fact" | "opinion" | "speculation" | "unclassified".
    pub evidence_type: String,
    /// Where the evidence came from (tool name, data source, URL description).
    #[serde(default)]
    pub source: String,
    /// ISO date when the evidence was observed or published.
    #[serde(default)]
    pub timestamp: String,
    /// Source quality tier: official | major_media | professional_research |
    /// longform_analysis | social_verified | social_unverified | unknown.
    #[serde(default)]
    pub source_tier: String,
    /// Earliest traceable origin of the information (attribution).
    #[serde(default)]
    pub first_source: String,
    /// Whether this is a repost / derivative of earlier-reported information.
    #[serde(default)]
    pub is_derivative_repost: bool,
    /// Human-readable evidence age: "0-2d" | "3-5d" | "6-10d" | "10d+" | "unknown".
    #[serde(default)]
    pub evidence_age: String,
    /// 0.0-1.0 confidence in the quality of the source.
    #[serde(default)]
    pub source_confidence: f64,
}

/// Deserialize key_evidence accepting both structured objects and plain strings.
/// Plain strings are converted to EvidenceItem with evidence_type="unclassified".
fn deserialize_key_evidence<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<EvidenceItem>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    let raw: Vec<Value> = Vec::deserialize(deserializer)?;
    raw.into_iter()
        .map(|value| match value {
            Value::String(text) => Ok(EvidenceItem {
                claim: text,
                evidence_type: "unclassified".to_string(),
                source: String::new(),
                timestamp: String::new(),
                source_tier: String::new(),
                first_source: String::new(),
                is_derivative_repost: false,
                evidence_age: String::new(),
                source_confidence: 0.0,
            }),
            Value::Object(_) => serde_json::from_value::<EvidenceItem>(value)
                .map_err(|error| Error::custom(format!("invalid evidence item: {error}"))),
            _ => Err(Error::custom("evidence item must be string or object")),
        })
        .collect()
}

pub fn validate_evidence_types(
    artifact: &AnalystTickerArtifact,
) -> std::result::Result<(), String> {
    const ALLOWED_SOURCE_TIERS: &[&str] = &[
        "official",
        "major_media",
        "professional_research",
        "longform_analysis",
        "social_verified",
        "social_unverified",
        "unknown",
    ];
    for evidence in &artifact.key_evidence {
        match evidence.evidence_type.as_str() {
            "fact" | "opinion" | "speculation" | "unclassified" => {}
            other => {
                return Err(format!(
                    "invalid evidence_type '{other}' in evidence '{}'; must be fact, opinion, or speculation",
                    evidence.claim
                ));
            }
        }
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
            if !value.is_empty() && !["low", "medium", "high"].contains(&value) {
                return Err(format!(
                    "invalid {field} '{value}'; must be low, medium, high, or empty"
                ));
            }
        }
    }
    Ok(())
}

/// Validate machine-read fields on an analyst per-ticker payload.
///
/// Enforces the contract promised by `analyst_output_contract.md`:
/// `direction` and `confidence` must exist and be legal, and evidence typing
/// must pass `validate_evidence_types`.
pub fn validate_analyst_ticker_artifact(
    artifact: &AnalystTickerArtifact,
) -> std::result::Result<(), String> {
    const ALLOWED_DIRECTIONS: &[&str] =
        &["bullish", "bearish", "neutral", "mixed", "unobserved"];
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
    validate_evidence_types(artifact)
}


/// Validate a parsed `RiskConstraints` artifact for well-formed enum and
/// range values. Tolerant of empty / zero (unspecified) fields so legacy
/// artifacts continue to deserialize.
pub fn validate_risk_constraints(artifact: &RiskConstraints) -> std::result::Result<(), String> {
    const ALLOWED_STOP_TYPES: &[&str] =
        &["none", "tight", "trailing", "event_based", "time_based", ""];
    if !ALLOWED_STOP_TYPES.contains(&artifact.stop_type.as_str()) {
        return Err(format!(
            "invalid stop_type '{}'; must be one of: {}",
            artifact.stop_type,
            ALLOWED_STOP_TYPES
                .iter()
                .filter(|s| !s.is_empty())
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
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

/// Compact JSON Schema (no `$schema`/metadata noise) for a schemars type,
/// suitable for injecting into a prompt. Returns a pretty-printed string.
pub fn schema_for<T: JsonSchema>() -> String {
    let schema = schemars::schema_for!(T);
    serde_json::to_string_pretty(&schema).unwrap_or_else(|_| "{}".to_string())
}

/// Schema string for a single analyst per-ticker payload.
pub fn analyst_artifact_schema() -> String {
    schema_for::<AnalystTickerArtifact>()
}

/// Schema string for the research manager artifact.
pub fn research_artifact_schema() -> String {
    schema_for::<ResearchArtifact>()
}

pub fn trade_intent_schema() -> String {
    schema_for::<TradeIntent>()
}

pub fn risk_constraints_schema() -> String {
    schema_for::<RiskConstraints>()
}

pub fn final_validation_schema() -> String {
    schema_for::<FinalValidation>()
}

pub fn portfolio_allocation_schema() -> String {
    schema_for::<PortfolioAllocation>()
}

#[derive(Debug, Error, PartialEq)]
pub enum ValidationError {
    #[error("missing per_ticker payload for {0}")]
    MissingTicker(String),
    #[error("probability field {0} is invalid")]
    InvalidProbability(String),
    #[error("long_probability + short_probability must be approximately 1.0")]
    ProbabilitySum,
    #[error("scenario probabilities must sum to 1.0 (got {0})")]
    ScenarioProbabilitySum(f64),
    #[error("long_probability ({long}) inconsistent with scenarios (expected ~{expected})")]
    InconsistentLongProbability { long: f64, expected: f64 },
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

pub fn extract_json_artifact(text: &str) -> Result<Value> {
    const START: &str = "=== ARTIFACT_JSON_START ===";
    const END: &str = "=== ARTIFACT_JSON_END ===";
    let candidate = if let Some(start) = text.find(START) {
        let after = &text[start + START.len()..];
        let end = after
            .find(END)
            .ok_or_else(|| anyhow!("artifact end marker missing"))?;
        &after[..end]
    } else {
        text
    };
    serde_json::from_str(candidate.trim()).context("failed to parse artifact JSON")
}

pub fn validate_research_artifact(
    artifact: &ResearchArtifact,
    tickers: &[String],
) -> std::result::Result<(), ValidationError> {
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

        let expected_long = scenarios.bull.probability + 0.5 * scenarios.base.probability;
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
        }
    }
    for ticker in tickers {
        if !artifact.per_ticker.contains_key(ticker) {
            return Err(ValidationError::MissingTicker(ticker.clone()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_marker_wrapped_json() {
        assert_eq!(
            extract_json_artifact(
                "x\n=== ARTIFACT_JSON_START ===\n{\"ok\":true}\n=== ARTIFACT_JSON_END ==="
            )
            .unwrap(),
            json!({"ok": true})
        );
    }

    #[test]
    fn normalizes_percent_strings() {
        assert_eq!(normalize_probability(&json!("68%")), Some(0.68));
        assert_eq!(normalize_probability(&json!(68)), Some(0.68));
        assert_eq!(normalize_probability(&json!(0.68)), Some(0.68));
    }

    #[test]
    fn analyst_schema_lists_machine_fields() {
        let schema = analyst_artifact_schema();
        for field in [
            "direction",
            "confidence",
            "report",
            "data_gaps",
            "key_evidence",
            "evidence_type",
        ] {
            assert!(schema.contains(field), "schema missing field {field}");
        }
        // Must be valid JSON.
        serde_json::from_str::<Value>(&schema).expect("analyst schema is valid JSON");
    }

    #[test]
    fn evidence_item_deserializes_structured() {
        let json = r#"{
            "claim": "CPI came in at 3.2%",
            "evidence_type": "fact",
            "source": "BLS via Jin10",
            "timestamp": "2026-07-06"
        }"#;
        let item: EvidenceItem = serde_json::from_str(json).unwrap();
        assert_eq!(item.claim, "CPI came in at 3.2%");
        assert_eq!(item.evidence_type, "fact");
        assert_eq!(item.source, "BLS via Jin10");
        assert_eq!(item.timestamp, "2026-07-06");
    }

    #[test]
    fn analyst_artifact_accepts_legacy_string_evidence() {
        let json = r#"{
            "direction": "bullish",
            "confidence": 0.7,
            "key_evidence": ["simple string evidence"]
        }"#;
        let artifact: AnalystTickerArtifact = serde_json::from_str(json).unwrap();
        assert_eq!(artifact.key_evidence.len(), 1);
        assert_eq!(artifact.key_evidence[0].claim, "simple string evidence");
        assert_eq!(artifact.key_evidence[0].evidence_type, "unclassified");
    }

    #[test]
    fn analyst_artifact_accepts_structured_evidence() {
        let json = r#"{
            "direction": "bullish",
            "confidence": 0.7,
            "key_evidence": [
                {"claim": "CPI 3.2%", "evidence_type": "fact", "source": "BLS", "timestamp": "2026-07-06"}
            ]
        }"#;
        let artifact: AnalystTickerArtifact = serde_json::from_str(json).unwrap();
        assert_eq!(artifact.key_evidence[0].claim, "CPI 3.2%");
        assert_eq!(artifact.key_evidence[0].evidence_type, "fact");
    }

    #[test]
    fn analyst_artifact_accepts_mixed_evidence_formats() {
        let json = r#"{
            "direction": "mixed",
            "confidence": 0.5,
            "key_evidence": [
                "legacy observation",
                {"claim": "Options rumor", "evidence_type": "speculation", "source": "Reddit"}
            ]
        }"#;
        let artifact: AnalystTickerArtifact = serde_json::from_str(json).unwrap();
        assert_eq!(artifact.key_evidence.len(), 2);
        assert_eq!(artifact.key_evidence[0].evidence_type, "unclassified");
        assert_eq!(artifact.key_evidence[1].evidence_type, "speculation");
        assert_eq!(artifact.key_evidence[1].timestamp, "");
    }

    #[test]
    fn validate_evidence_types_rejects_invalid_type() {
        let artifact = AnalystTickerArtifact {
            direction: "bullish".to_string(),
            confidence: 0.7,
            report: String::new(),
            key_evidence: vec![EvidenceItem {
                claim: "ambiguous claim".to_string(),
                evidence_type: "rumor".to_string(),
                source: String::new(),
                timestamp: String::new(),
                source_tier: String::new(),
                first_source: String::new(),
                is_derivative_repost: false,
                evidence_age: String::new(),
                source_confidence: 0.0,
            }],
            priced_in: String::new(),
            echo_chamber_risk: String::new(),
            crowded_consensus_risk: String::new(),
            validation_triggers: Vec::new(),
            data_gaps: Vec::new(),
        };

        let error = validate_evidence_types(&artifact).unwrap_err();
        assert!(error.contains("invalid evidence_type 'rumor'"));
    }

    #[test]
    fn analyst_schema_lists_new_quality_fields() {
        let schema = analyst_artifact_schema();
        for field in [
            "source_tier",
            "first_source",
            "is_derivative_repost",
            "evidence_age",
            "source_confidence",
            "echo_chamber_risk",
            "crowded_consensus_risk",
        ] {
            assert!(
                schema.contains(field),
                "analyst schema missing field {field}"
            );
        }
        serde_json::from_str::<Value>(&schema).expect("analyst schema is valid JSON");
    }

    #[test]
    fn risk_constraints_schema_lists_new_structured_fields() {
        let schema = risk_constraints_schema();
        for field in [
            "stop_type",
            "max_drawdown_pct",
            "position_cap_pct",
            "rebalance_trigger",
            "risk_off_trigger",
            "review_window",
            "cash_hedge_recommendation",
            "constraint_confidence",
        ] {
            assert!(schema.contains(field), "risk schema missing field {field}");
        }
        serde_json::from_str::<Value>(&schema).expect("risk schema is valid JSON");
    }

    #[test]
    fn validate_evidence_types_rejects_invalid_source_tier() {
        let artifact = AnalystTickerArtifact {
            direction: "bullish".to_string(),
            confidence: 0.7,
            report: String::new(),
            key_evidence: vec![EvidenceItem {
                claim: "a claim".to_string(),
                evidence_type: "fact".to_string(),
                source: String::new(),
                timestamp: String::new(),
                source_tier: "garbage".to_string(),
                first_source: String::new(),
                is_derivative_repost: false,
                evidence_age: String::new(),
                source_confidence: 0.0,
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
            report: String::new(),
            key_evidence: vec![EvidenceItem {
                claim: "a claim".to_string(),
                evidence_type: "fact".to_string(),
                source: String::new(),
                timestamp: String::new(),
                source_tier: String::new(),
                first_source: String::new(),
                is_derivative_repost: false,
                evidence_age: String::new(),
                source_confidence: 0.0,
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
            recommended_adjustment: String::new(),
            stop_type: String::new(),
            max_drawdown_pct: 1.5,
            position_cap_pct: 0.0,
            rebalance_trigger: String::new(),
            risk_off_trigger: String::new(),
            review_window: String::new(),
            cash_hedge_recommendation: String::new(),
            constraint_confidence: 0.0,
            extra: Map::new(),
        };
        let error = validate_risk_constraints(&artifact).unwrap_err();
        assert!(error.contains("max_drawdown_pct 1.5 out of range"));
    }

    #[test]
    fn validate_risk_constraints_rejects_invalid_stop_type() {
        let artifact = RiskConstraints {
            stance: "neutral".to_string(),
            argument: String::new(),
            recommended_adjustment: String::new(),
            stop_type: "weird".to_string(),
            max_drawdown_pct: 0.0,
            position_cap_pct: 0.0,
            rebalance_trigger: String::new(),
            risk_off_trigger: String::new(),
            review_window: String::new(),
            cash_hedge_recommendation: String::new(),
            constraint_confidence: 0.0,
            extra: Map::new(),
        };
        let error = validate_risk_constraints(&artifact).unwrap_err();
        assert!(error.contains("invalid stop_type 'weird'"));
    }

    #[test]
    fn analyst_artifact_with_new_fields_round_trips() {
        let json = r#"{
            "direction": "bullish",
            "confidence": 0.7,
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
            ]
        }"#;
        let artifact: AnalystTickerArtifact = serde_json::from_str(json).unwrap();
        assert_eq!(artifact.echo_chamber_risk, "medium");
        assert_eq!(artifact.crowded_consensus_risk, "high");
        assert_eq!(artifact.key_evidence[0].source_tier, "official");
        assert_eq!(artifact.key_evidence[0].first_source, "BLS release");
        assert!(!artifact.key_evidence[0].is_derivative_repost);
        assert_eq!(artifact.key_evidence[0].evidence_age, "0-2d");
        assert!((artifact.key_evidence[0].source_confidence - 0.9).abs() < f64::EPSILON);

        // Legacy artifact without new fields still deserializes via serde(default).
        let legacy: AnalystTickerArtifact =
            serde_json::from_str(r#"{"direction":"neutral","confidence":0.5}"#).unwrap();
        assert_eq!(legacy.echo_chamber_risk, "");
        assert_eq!(legacy.crowded_consensus_risk, "");
        assert!(legacy.key_evidence.is_empty());
    }

    #[test]
    fn risk_constraints_with_new_fields_round_trips() {
        let json = r#"{
            "stance": "conservative",
            "recommended_adjustment": "Cap exposure at 50%.",
            "stop_type": "trailing",
            "max_drawdown_pct": 0.15,
            "position_cap_pct": 0.5,
            "rebalance_trigger": "VIX > 25",
            "risk_off_trigger": "Overnight gap > 3%",
            "review_window": "3d",
            "cash_hedge_recommendation": "Hold 20% cash.",
            "constraint_confidence": 0.8
        }"#;
        let artifact: RiskConstraints = serde_json::from_str(json).unwrap();
        assert_eq!(artifact.stop_type, "trailing");
        assert!((artifact.max_drawdown_pct - 0.15).abs() < f64::EPSILON);
        assert!((artifact.position_cap_pct - 0.5).abs() < f64::EPSILON);
        assert_eq!(artifact.rebalance_trigger, "VIX > 25");
        assert_eq!(artifact.risk_off_trigger, "Overnight gap > 3%");
        assert_eq!(artifact.review_window, "3d");
        assert_eq!(artifact.cash_hedge_recommendation, "Hold 20% cash.");
        assert!((artifact.constraint_confidence - 0.8).abs() < f64::EPSILON);
        assert!(validate_risk_constraints(&artifact).is_ok());
    }

    fn valid_scenarios() -> Scenarios {
        Scenarios {
            bull: Scenario {
                probability: 0.35,
                drivers: vec!["Fed cut".to_string()],
                triggers: vec!["FOMC minutes".to_string()],
                confirmation: "Close above 500".to_string(),
            },
            base: Scenario {
                probability: 0.45,
                drivers: vec!["Range-bound".to_string()],
                triggers: vec!["VIX below 20".to_string()],
                confirmation: "5 days in range".to_string(),
            },
            bear: Scenario {
                probability: 0.20,
                drivers: vec!["Inflation".to_string()],
                triggers: vec!["CPI above 3.5%".to_string()],
                confirmation: "Close below 475".to_string(),
            },
        }
    }

    fn research_artifact_with_scenarios(scenarios: Option<Scenarios>) -> ResearchArtifact {
        ResearchArtifact {
            rating: "Hold".to_string(),
            long_probability: 0.575,
            short_probability: 0.425,
            plan: String::new(),
            probability_rationale: String::new(),
            scenarios,
            per_ticker: BTreeMap::new(),
            extra: Map::new(),
        }
    }

    #[test]
    fn research_schema_lists_probability_fields() {
        let schema = research_artifact_schema();
        for field in [
            "rating",
            "long_probability",
            "short_probability",
            "scenarios",
            "Scenario",
            "Scenarios",
            "drivers",
            "triggers",
            "confirmation",
        ] {
            assert!(schema.contains(field), "schema missing field {field}");
        }
        serde_json::from_str::<Value>(&schema).expect("research schema is valid JSON");
    }

    #[test]
    fn research_artifact_with_scenarios_validates() {
        let artifact = research_artifact_with_scenarios(Some(valid_scenarios()));
        assert!(validate_research_artifact(&artifact, &[]).is_ok());
    }

    #[test]
    fn scenario_probabilities_must_sum_to_one() {
        let artifact = research_artifact_with_scenarios(Some(Scenarios {
            bull: Scenario {
                probability: 0.4,
                drivers: vec!["x".into()],
                triggers: vec!["y".into()],
                confirmation: "z".into(),
            },
            base: Scenario {
                probability: 0.4,
                drivers: vec!["x".into()],
                triggers: vec!["y".into()],
                confirmation: "z".into(),
            },
            bear: Scenario {
                probability: 0.4,
                drivers: vec!["x".into()],
                triggers: vec!["y".into()],
                confirmation: "z".into(),
            },
        }));

        assert!(matches!(
            validate_research_artifact(&artifact, &[]),
            Err(ValidationError::ScenarioProbabilitySum(sum)) if (sum - 1.2).abs() < 0.001
        ));
    }

    #[test]
    fn inconsistent_long_probability_is_rejected() {
        let artifact = ResearchArtifact {
            long_probability: 0.7,
            short_probability: 0.3,
            ..research_artifact_with_scenarios(Some(Scenarios {
                bull: Scenario {
                    probability: 0.2,
                    drivers: vec!["x".into()],
                    triggers: vec!["y".into()],
                    confirmation: "z".into(),
                },
                base: Scenario {
                    probability: 0.5,
                    drivers: vec!["x".into()],
                    triggers: vec!["y".into()],
                    confirmation: "z".into(),
                },
                bear: Scenario {
                    probability: 0.3,
                    drivers: vec!["x".into()],
                    triggers: vec!["y".into()],
                    confirmation: "z".into(),
                },
            }))
        };

        assert!(matches!(
            validate_research_artifact(&artifact, &[]),
            Err(ValidationError::InconsistentLongProbability { long, expected })
                if (long - 0.7).abs() < 0.001 && (expected - 0.45).abs() < 0.001
        ));
    }

    #[test]
    fn scenario_drivers_are_required() {
        let mut scenarios = valid_scenarios();
        scenarios.bull.drivers.clear();
        let artifact = research_artifact_with_scenarios(Some(scenarios));

        assert!(matches!(
            validate_research_artifact(&artifact, &[]),
            Err(ValidationError::InvalidProbability(message))
                if message == "scenario bull must have at least 1 driver"
        ));
    }

    #[test]
    fn scenario_triggers_are_required() {
        let mut scenarios = valid_scenarios();
        scenarios.bear.triggers.clear();
        let artifact = research_artifact_with_scenarios(Some(scenarios));

        assert!(matches!(
            validate_research_artifact(&artifact, &[]),
            Err(ValidationError::InvalidProbability(message))
                if message == "scenario bear must have at least 1 trigger"
        ));
    }

    #[test]
    fn research_artifact_without_scenarios_still_validates() {
        let artifact = research_artifact_with_scenarios(None);
        assert!(validate_research_artifact(&artifact, &[]).is_ok());
    }

    #[test]
    fn research_artifact_without_scenarios_deserializes() {
        let json = r#"{
            "rating": "Hold",
            "long_probability": 0.55,
            "short_probability": 0.45
        }"#;
        let artifact: ResearchArtifact = serde_json::from_str(json).unwrap();
        assert_eq!(artifact.scenarios, None);
        assert!(validate_research_artifact(&artifact, &[]).is_ok());
    }

    #[test]
    fn downstream_contract_schemas_list_machine_fields() {
        for (schema, fields) in [
            (
                trade_intent_schema(),
                vec!["action", "entry_price", "position_size"],
            ),
            (
                risk_constraints_schema(),
                vec!["stance", "argument", "recommended_adjustment"],
            ),
            (
                final_validation_schema(),
                vec!["rating", "execution_summary", "risk_controls"],
            ),
            (
                portfolio_allocation_schema(),
                vec!["weights", "total_equity_exposure", "vix_regime"],
            ),
        ] {
            serde_json::from_str::<Value>(&schema).expect("contract schema is valid JSON");
            for field in fields {
                assert!(schema.contains(field), "schema missing field {field}");
            }
        }
    }


    #[test]
    fn validate_analyst_ticker_artifact_rejects_bad_direction() {
        let artifact = AnalystTickerArtifact {
            direction: "sideways".to_string(),
            confidence: 0.5,
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
    fn validate_analyst_ticker_artifact_accepts_valid_payload() {
        let artifact = AnalystTickerArtifact {
            direction: "bullish".to_string(),
            confidence: 0.7,
            report: "ok".to_string(),
            key_evidence: Vec::new(),
            priced_in: String::new(),
            echo_chamber_risk: String::new(),
            crowded_consensus_risk: String::new(),
            validation_triggers: Vec::new(),
            data_gaps: Vec::new(),
        };
        validate_analyst_ticker_artifact(&artifact).unwrap();
    }

}
