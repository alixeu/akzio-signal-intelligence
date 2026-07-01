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
}
