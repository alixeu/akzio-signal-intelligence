//! Baseline management — load, save, and compare eval scores against baselines.
//!
//! The baseline file (`tests/eval/baseline.json`) stores the last known-good
//! scores per test case. Regression detection flags any case whose aggregate
//! score drops more than 10 points below its baseline.

use crate::runner::CaseResult;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

/// The threshold below which a regression is flagged.
pub const REGRESSION_THRESHOLD: f64 = 10.0;

/// Baseline file structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Baseline {
    pub version: u32,
    pub updated_at: String,
    pub test_cases: BTreeMap<String, BaselineEntry>,
}

/// Per-test-case baseline entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaselineEntry {
    pub aggregate: f64,
    pub dimensions: BTreeMap<String, f64>,
}

/// A regression report for a single test case.
#[derive(Debug, Clone)]
pub struct RegressionReport {
    pub test_id: String,
    pub baseline_score: f64,
    pub current_score: f64,
    pub delta: f64,
    pub dimensions: BTreeMap<String, f64>,
}

/// Load a baseline from a JSON file.
pub fn load_baseline(path: &Path) -> Result<Baseline> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read baseline file: {}", path.display()))?;
    serde_json::from_str(&text)
        .with_context(|| format!("failed to parse baseline JSON: {}", path.display()))
}

/// Save current results as the new baseline.
pub fn save_baseline(path: &Path, results: &[CaseResult]) -> Result<()> {
    let baseline = Baseline {
        version: 1,
        updated_at: chrono::Utc::now().to_rfc3339(),
        test_cases: results
            .iter()
            .map(|r| {
                (
                    r.test_id.clone(),
                    BaselineEntry {
                        aggregate: r.score.aggregate,
                        dimensions: r.score.dimensions.clone(),
                    },
                )
            })
            .collect(),
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_string_pretty(&baseline)?)?;
    Ok(())
}

/// Check results against baseline and return regression reports for cases
/// whose aggregate score dropped more than `REGRESSION_THRESHOLD` points.
pub fn check_regression(results: &[CaseResult], baseline: &Baseline) -> Vec<RegressionReport> {
    results
        .iter()
        .filter_map(|r| {
            let base = baseline.test_cases.get(&r.test_id)?;
            let delta = r.score.aggregate - base.aggregate;
            if delta < -REGRESSION_THRESHOLD {
                Some(RegressionReport {
                    test_id: r.test_id.clone(),
                    baseline_score: base.aggregate,
                    current_score: r.score.aggregate,
                    delta,
                    dimensions: r.score.dimensions.clone(),
                })
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runner::CaseResult;
    use crate::scoring::EvalScore;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn make_result(test_id: &str, aggregate: f64) -> CaseResult {
        let mut dimensions = BTreeMap::new();
        dimensions.insert("json_validity".to_string(), 100.0);
        dimensions.insert("schema_compliance".to_string(), 80.0);
        dimensions.insert("field_completeness".to_string(), 60.0);
        dimensions.insert("direction_reasonableness".to_string(), 50.0);
        dimensions.insert("evidence_quality".to_string(), 50.0);
        CaseResult {
            test_id: test_id.to_string(),
            score: EvalScore {
                aggregate,
                dimensions,
                details: vec![],
            },
            regression: false,
            artifact: json!({}),
        }
    }

    #[test]
    fn save_and_load_baseline_roundtrip() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("baseline.json");
        let results = vec![make_result("test_a", 85.0), make_result("test_b", 70.0)];
        save_baseline(&path, &results).unwrap();
        let loaded = load_baseline(&path).unwrap();
        assert_eq!(loaded.version, 1);
        assert_eq!(loaded.test_cases.len(), 2);
        assert!((loaded.test_cases["test_a"].aggregate - 85.0).abs() < 0.01);
    }

    #[test]
    fn regression_detected_when_score_drops() {
        let results = vec![make_result("test_a", 50.0)];
        let mut baseline = Baseline {
            version: 1,
            updated_at: "2026-07-06T00:00:00Z".to_string(),
            test_cases: BTreeMap::new(),
        };
        baseline.test_cases.insert(
            "test_a".to_string(),
            BaselineEntry {
                aggregate: 80.0,
                dimensions: BTreeMap::new(),
            },
        );
        let regressions = check_regression(&results, &baseline);
        assert_eq!(regressions.len(), 1);
        assert!((regressions[0].delta - (-30.0)).abs() < 0.01);
    }

    #[test]
    fn no_regression_when_score_within_threshold() {
        let results = vec![make_result("test_a", 75.0)];
        let mut baseline = Baseline {
            version: 1,
            updated_at: "2026-07-06T00:00:00Z".to_string(),
            test_cases: BTreeMap::new(),
        };
        baseline.test_cases.insert(
            "test_a".to_string(),
            BaselineEntry {
                aggregate: 80.0,
                dimensions: BTreeMap::new(),
            },
        );
        let regressions = check_regression(&results, &baseline);
        assert!(regressions.is_empty());
    }

    #[test]
    fn missing_baseline_entry_is_not_a_regression() {
        let results = vec![make_result("test_new", 50.0)];
        let baseline = Baseline {
            version: 1,
            updated_at: "2026-07-06T00:00:00Z".to_string(),
            test_cases: BTreeMap::new(),
        };
        let regressions = check_regression(&results, &baseline);
        assert!(regressions.is_empty());
    }
}
