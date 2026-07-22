//! Eval runner — executes test cases and scores the resulting artifacts.
//!
//! In mock mode, the runner calls `orchestrator_llm::mock_role_artifact` to
//! obtain a canned artifact (matching the production mock path in
//! `role_jobs.rs`) and then scores it. Live mode is stubbed out pending
//! exposure of `render_prompt` / `RuntimeConfig` from `orchestrator-workflow`.

use super::scoring::{score_artifact, EvalScore};
use super::{EvalCase, EvalMode};
use anyhow::{bail, Result};
use serde_json::{json, Value};

/// Result of running a single eval case.
#[derive(Debug, Clone)]
pub struct CaseResult {
    pub test_id: String,
    pub score: EvalScore,
    pub regression: bool,
    pub artifact: Value,
}

/// Runs eval cases against mock or live artifacts.
pub struct EvalRunner;

impl EvalRunner {
    pub fn new() -> Self {
        Self
    }

    /// Run a single eval case and return its scored result.
    pub fn run_case(&self, case: &EvalCase) -> Result<CaseResult> {
        let artifact = match case.mode {
            EvalMode::Mock => self.run_mock(case),
            EvalMode::Live => {
                bail!(
                    "live mode is not yet implemented for eval; \
                     orchestrator-workflow render_prompt is pub(crate) \
                     and not accessible from outside the crate. \
                     Use mock mode for CI regression checks."
                )
            }
        }?;

        let score = score_artifact(&artifact, case);

        let regression = case
            .baseline_score
            .is_some_and(|baseline| score.aggregate < baseline - 10.0);

        Ok(CaseResult {
            test_id: case.test_id.clone(),
            score,
            regression,
            artifact,
        })
    }

    /// Run all eval cases, returning results in order.
    pub fn run_all(&self, cases: &[EvalCase]) -> Result<Vec<CaseResult>> {
        cases.iter().map(|c| self.run_case(c)).collect()
    }

    fn run_mock(&self, case: &EvalCase) -> Result<Value> {
        let mut artifact = orchestrator_llm::mock_role_artifact(&case.role, &case.input.tickers);
        artifact["phase"] = json!(case.phase);
        artifact["kind"] = json!(case.kind);
        artifact["eval_test_id"] = json!(case.test_id);
        Ok(artifact)
    }
}

impl Default for EvalRunner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::super::{DimensionWeight, EvalCase, EvalExpected, EvalInput, EvalMode};
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn make_case(role: &str, tickers: &[&str]) -> EvalCase {
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
            test_id: format!("test_{}", role.replace('.', "_")),
            description: "test".to_string(),
            role: role.to_string(),
            phase: 1,
            kind: "artifact".to_string(),
            input: EvalInput {
                ticker: tickers[0].to_string(),
                tickers: tickers.iter().map(|t| t.to_string()).collect(),
                date: "2026-07-06".to_string(),
                mock_db_path: None,
                state_overrides: json!({}),
            },
            expected: EvalExpected {
                direction: None,
                confidence_range: None,
                required_fields: vec![],
                min_report_chars: None,
                max_report_chars: None,
                key_evidence_min_items: None,
                key_evidence_max_items: None,
            },
            dimensions,
            mode: EvalMode::Mock,
            baseline_score: None,
        }
    }

    #[test]
    fn mock_analyst_produces_valid_artifact() {
        let runner = EvalRunner::new();
        let case = make_case("analyst.technical", &["TQQQ"]);
        let result = runner.run_case(&case).unwrap();
        assert!(result.artifact.is_object());
        assert!(result.score.aggregate > 0.0);
    }

    #[test]
    fn mock_trader_produces_valid_artifact() {
        let runner = EvalRunner::new();
        let case = make_case("trader", &["TQQQ"]);
        let result = runner.run_case(&case).unwrap();
        assert!(result.artifact.is_object());
        assert!(result.score.dimensions["json_validity"] == 100.0);
    }

    #[test]
    fn live_mode_returns_error() {
        let runner = EvalRunner::new();
        let mut case = make_case("analyst.technical", &["TQQQ"]);
        case.mode = EvalMode::Live;
        let result = runner.run_case(&case);
        assert!(result.is_err());
    }

    #[test]
    fn regression_detected_when_score_drops() {
        let runner = EvalRunner::new();
        let mut case = make_case("analyst.technical", &["TQQQ"]);
        // Set a baseline so high that the mock artifact will always regress
        case.baseline_score = Some(200.0);
        let result = runner.run_case(&case).unwrap();
        assert!(result.regression, "expected regression flag to be set");
    }

    #[test]
    fn no_regression_when_score_within_threshold() {
        let runner = EvalRunner::new();
        let mut case = make_case("analyst.technical", &["TQQQ"]);
        case.baseline_score = Some(0.0);
        let result = runner.run_case(&case).unwrap();
        assert!(!result.regression, "expected no regression flag");
    }
}
