//! Scoring engine for prompt evaluation.
//!
//! Scores artifacts across five dimensions:
//! 1. JSON validity — is the artifact a JSON object?
//! 2. Schema compliance — are required fields present?
//! 3. Field completeness — are report/per_ticker non-empty and within bounds?
//! 4. Direction reasonableness — direction in expected set, confidence in range.
//! 5. Evidence quality — key_evidence items present, non-empty, within bounds.
//!
//! Each dimension produces a 0-100 score. The aggregate is a weighted sum
//! (weights defined per test case, should total 100).

use crate::{EvalCase, EvalExpected};
use serde_json::Value;
use std::collections::BTreeMap;

/// Result of scoring a single artifact.
#[derive(Debug, Clone)]
pub struct EvalScore {
    /// Weighted aggregate score (0-100).
    pub aggregate: f64,
    /// Per-dimension scores (0-100 each).
    pub dimensions: BTreeMap<String, f64>,
    /// Human-readable details about scoring decisions.
    pub details: Vec<String>,
}

/// Score an artifact against an eval case's expectations.
pub fn score_artifact(artifact: &Value, case: &EvalCase) -> EvalScore {
    let mut dimensions = BTreeMap::new();
    let mut details = Vec::new();

    // 1. JSON validity (0 or 100)
    let json_validity = if artifact.is_object() { 100.0 } else { 0.0 };
    dimensions.insert("json_validity".to_string(), json_validity);
    if json_validity == 0.0 {
        details.push("artifact is not a JSON object".to_string());
    }

    // 2. Schema compliance — required fields exist
    let schema_compliance = check_schema_compliance(artifact, &case.expected.required_fields);
    dimensions.insert("schema_compliance".to_string(), schema_compliance);

    // 3. Field completeness — report/per_ticker non-empty, within bounds
    let field_completeness = check_field_completeness(artifact, &case.expected, &mut details);
    dimensions.insert("field_completeness".to_string(), field_completeness);

    // 4. Direction reasonableness — direction in set, confidence in range
    let direction_reasonableness =
        check_direction_reasonableness(artifact, &case.expected, &mut details);
    dimensions.insert(
        "direction_reasonableness".to_string(),
        direction_reasonableness,
    );

    // 5. Evidence quality — key_evidence count, non-empty
    let evidence_quality = check_evidence_quality(artifact, &case.expected, &mut details);
    dimensions.insert("evidence_quality".to_string(), evidence_quality);

    // Weighted aggregate
    let total_weight: f64 = case.dimensions.values().map(|dw| dw.weight).sum();
    let aggregate = if total_weight > 0.0 {
        case.dimensions
            .iter()
            .filter_map(|(dim, dw)| {
                dimensions
                    .get(dim)
                    .map(|&score| score * dw.weight / total_weight)
            })
            .sum()
    } else {
        0.0
    };

    EvalScore {
        aggregate,
        dimensions,
        details,
    }
}

fn check_schema_compliance(artifact: &Value, required_fields: &[String]) -> f64 {
    if required_fields.is_empty() {
        return 100.0;
    }
    let present = required_fields
        .iter()
        .filter(|field| artifact.get(field).is_some())
        .count();
    (present as f64 / required_fields.len() as f64) * 100.0
}

fn check_field_completeness(
    artifact: &Value,
    expected: &EvalExpected,
    details: &mut Vec<String>,
) -> f64 {
    if expected.required_fields.is_empty() {
        // Fall back to report/per_ticker checks
        return check_report_and_per_ticker(artifact, expected, details);
    }

    // Check that all required fields are non-null
    let non_null = expected
        .required_fields
        .iter()
        .filter(|field| artifact.get(field).is_some_and(|v| !v.is_null()))
        .count();
    let mut score = (non_null as f64 / expected.required_fields.len() as f64) * 100.0;

    // Additionally check report/per_ticker if present in required_fields
    if expected.required_fields.iter().any(|f| f == "report") {
        score = score.min(check_report_and_per_ticker(artifact, expected, details));
    }

    score
}

fn check_report_and_per_ticker(
    artifact: &Value,
    expected: &EvalExpected,
    details: &mut Vec<String>,
) -> f64 {
    let mut score = 0.0;
    let mut checks = 0;

    // Check report is non-empty and within bounds
    if let Some(report) = artifact.get("report").and_then(Value::as_str) {
        checks += 1;
        let len = report.len();
        if len > 0 {
            score += 50.0;
        } else {
            details.push("report is empty".to_string());
        }
        if let Some(min) = expected.min_report_chars {
            if len >= min {
                score += 25.0;
            } else {
                details.push(format!("report length {len} below min {min}"));
            }
        }
        if let Some(max) = expected.max_report_chars {
            if len <= max {
                score += 25.0;
            } else {
                details.push(format!("report length {len} above max {max}"));
            }
        }
    } else {
        checks += 1;
    }

    // Check per_ticker exists and is a non-empty object
    if artifact.get("per_ticker").is_some_and(Value::is_object) {
        score += 50.0;
        checks += 1;
    } else {
        checks += 1;
    }

    if checks > 0 {
        score / checks as f64
    } else {
        0.0
    }
}

fn check_direction_reasonableness(
    artifact: &Value,
    expected: &EvalExpected,
    _details: &mut Vec<String>,
) -> f64 {
    let mut score = 0.0;

    // Direction check
    if let Some(directions) = &expected.direction {
        let dir = artifact
            .get("direction")
            .and_then(Value::as_str)
            .or_else(|| {
                // Check first per_ticker entry for direction
                artifact
                    .get("per_ticker")
                    .and_then(Value::as_object)
                    .and_then(|map| map.values().next())
                    .and_then(|v| v.get("direction"))
                    .and_then(Value::as_str)
            });
        if let Some(dir) = dir {
            if directions.iter().any(|d| d == dir) {
                score += 50.0;
            }
        }
    } else {
        score += 50.0;
    }

    // Confidence check
    if let Some(range) = &expected.confidence_range {
        let conf = artifact
            .get("confidence")
            .and_then(Value::as_f64)
            .or_else(|| {
                // Check first per_ticker entry for confidence
                artifact
                    .get("per_ticker")
                    .and_then(Value::as_object)
                    .and_then(|map| map.values().next())
                    .and_then(|v| v.get("confidence"))
                    .and_then(Value::as_f64)
            });
        if let Some(conf) = conf {
            if conf >= range[0] && conf <= range[1] {
                score += 50.0;
            }
        }
    } else {
        score += 50.0;
    }

    score
}

fn check_evidence_quality(
    artifact: &Value,
    expected: &EvalExpected,
    _details: &mut Vec<String>,
) -> f64 {
    // Collect key_evidence from top-level or per_ticker entries
    let evidence: Vec<Value> = artifact
        .get("key_evidence")
        .and_then(Value::as_array)
        .cloned()
        .or_else(|| {
            // Aggregate key_evidence from per_ticker entries
            let per_ticker = artifact.get("per_ticker")?.as_object()?;
            let mut all: Vec<Value> = Vec::new();
            for ticker_artifact in per_ticker.values() {
                if let Some(items) = ticker_artifact
                    .get("key_evidence")
                    .and_then(Value::as_array)
                {
                    all.extend(items.iter().cloned());
                }
            }
            if all.is_empty() {
                None
            } else {
                Some(all)
            }
        })
        .unwrap_or_default();

    let count = evidence.len();
    let mut score: f64 = 0.0;

    if count > 0 {
        score += 50.0;
    }

    if let Some(min) = expected.key_evidence_min_items {
        if count >= min {
            score += 25.0;
        }
    } else {
        score += 25.0;
    }

    if let Some(max) = expected.key_evidence_max_items {
        if count <= max {
            score += 25.0;
        }
    } else {
        score += 25.0;
    }

    // Check evidence items are non-empty strings
    if count > 0 {
        let non_empty = evidence
            .iter()
            .filter(|e| e.as_str().is_some_and(|s| !s.trim().is_empty()))
            .count();
        if non_empty == count {
            score = score.max(75.0);
        }
    }

    score
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DimensionWeight, EvalCase, EvalExpected, EvalInput, EvalMode};
    use serde_json::json;
    use std::collections::BTreeMap;

    fn make_case(expected: EvalExpected) -> EvalCase {
        let mut dimensions = BTreeMap::new();
        for dim in [
            "json_validity",
            "schema_compliance",
            "field_completeness",
            "direction_reasonableness",
            "evidence_quality",
        ] {
            dimensions.insert(dim.to_string(), DimensionWeight { weight: 20.0 });
        }
        EvalCase {
            test_id: "test".to_string(),
            description: "test".to_string(),
            role: "analyst.technical".to_string(),
            phase: 1,
            kind: "artifact".to_string(),
            input: EvalInput {
                ticker: "TQQQ".to_string(),
                tickers: vec!["TQQQ".to_string()],
                date: "2026-07-06".to_string(),
                mock_db_path: None,
                state_overrides: json!({}),
            },
            expected,
            dimensions,
            mode: EvalMode::Mock,
            baseline_score: None,
        }
    }

    #[test]
    fn valid_analyst_artifact_scores_well() {
        let artifact = json!({
            "id": "analyst.technical",
            "role": "analyst.technical",
            "report": "A solid technical analysis report with enough characters.",
            "per_ticker": {
                "TQQQ": {
                    "direction": "neutral",
                    "confidence": 0.5,
                    "report": "Mock report for TQQQ.",
                    "key_evidence": ["RSI at 50", "MACD crossover"]
                }
            }
        });
        let case = make_case(EvalExpected {
            direction: Some(vec!["bullish".into(), "bearish".into(), "neutral".into()]),
            confidence_range: Some([0.3, 0.9]),
            required_fields: vec!["report".into(), "per_ticker".into()],
            min_report_chars: Some(10),
            max_report_chars: Some(5000),
            key_evidence_min_items: Some(1),
            key_evidence_max_items: Some(10),
        });
        let score = score_artifact(&artifact, &case);
        assert!(score.dimensions["json_validity"] >= 100.0);
        assert!(score.dimensions["schema_compliance"] >= 100.0);
        assert!(score.dimensions["field_completeness"] > 0.0);
        assert!(score.dimensions["direction_reasonableness"] >= 100.0);
        assert!(score.dimensions["evidence_quality"] >= 75.0);
        assert!(score.aggregate > 50.0);
    }

    #[test]
    fn missing_fields_reduce_schema_compliance() {
        let artifact = json!({"id": "x"});
        let case = make_case(EvalExpected {
            direction: None,
            confidence_range: None,
            required_fields: vec!["report".into(), "per_ticker".into(), "rating".into()],
            min_report_chars: None,
            max_report_chars: None,
            key_evidence_min_items: None,
            key_evidence_max_items: None,
        });
        let score = score_artifact(&artifact, &case);
        assert!(score.dimensions["schema_compliance"] < 100.0);
    }

    #[test]
    fn bad_direction_scores_zero_for_direction() {
        let artifact = json!({
            "direction": "sideways",
            "confidence": 0.5
        });
        let case = make_case(EvalExpected {
            direction: Some(vec!["bullish".into(), "bearish".into(), "neutral".into()]),
            confidence_range: Some([0.3, 0.9]),
            required_fields: vec![],
            min_report_chars: None,
            max_report_chars: None,
            key_evidence_min_items: None,
            key_evidence_max_items: None,
        });
        let score = score_artifact(&artifact, &case);
        // Direction not in set (0), confidence in range (50) → total 50
        assert_eq!(score.dimensions["direction_reasonableness"], 50.0);
    }

    #[test]
    fn non_object_artifact_fails_json_validity() {
        let artifact = json!("not an object");
        let case = make_case(EvalExpected {
            direction: None,
            confidence_range: None,
            required_fields: vec![],
            min_report_chars: None,
            max_report_chars: None,
            key_evidence_min_items: None,
            key_evidence_max_items: None,
        });
        let score = score_artifact(&artifact, &case);
        assert_eq!(score.dimensions["json_validity"], 0.0);
    }

    #[test]
    fn per_ticker_direction_is_found() {
        let artifact = json!({
            "per_ticker": {
                "TQQQ": {"direction": "bullish", "confidence": 0.7}
            }
        });
        let case = make_case(EvalExpected {
            direction: Some(vec!["bullish".into(), "bearish".into()]),
            confidence_range: Some([0.5, 0.9]),
            required_fields: vec![],
            min_report_chars: None,
            max_report_chars: None,
            key_evidence_min_items: None,
            key_evidence_max_items: None,
        });
        let score = score_artifact(&artifact, &case);
        assert_eq!(score.dimensions["direction_reasonableness"], 100.0);
    }
}
