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
            }],
            priced_in: String::new(),
            validation_triggers: Vec::new(),
            data_gaps: Vec::new(),
        };

        let error = validate_evidence_types(&artifact).unwrap_err();
        assert!(error.contains("invalid evidence_type 'rumor'"));
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
}
