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
    #[serde(default)]
    pub per_ticker: BTreeMap<String, Value>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
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
    /// The 2-3 most decisive observations.
    #[serde(default)]
    pub key_evidence: Vec<String>,
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
        for field in ["direction", "confidence", "report", "data_gaps"] {
            assert!(schema.contains(field), "schema missing field {field}");
        }
        // Must be valid JSON.
        serde_json::from_str::<Value>(&schema).expect("analyst schema is valid JSON");
    }

    #[test]
    fn research_schema_lists_probability_fields() {
        let schema = research_artifact_schema();
        for field in ["rating", "long_probability", "short_probability"] {
            assert!(schema.contains(field), "schema missing field {field}");
        }
        serde_json::from_str::<Value>(&schema).expect("research schema is valid JSON");
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
