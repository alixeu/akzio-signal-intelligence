//! Prompt evaluation suite data structures.

pub mod baseline;
pub mod runner;
pub mod scoring;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// A single prompt-evaluation test case.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalCase {
    pub test_id: String,
    pub description: String,
    pub role: String,
    pub phase: i64,
    pub kind: String,
    pub input: EvalInput,
    pub expected: EvalExpected,
    pub dimensions: BTreeMap<String, DimensionWeight>,
    pub mode: EvalMode,
    #[serde(default)]
    pub baseline_score: Option<f64>,
}

/// Input parameters for an eval case.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalInput {
    pub ticker: String,
    pub tickers: Vec<String>,
    pub date: String,
    #[serde(default)]
    pub mock_db_path: Option<String>,
    #[serde(default)]
    pub state_overrides: Value,
}

/// Expected output constraints used by the scoring engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalExpected {
    #[serde(default)]
    pub direction: Option<Vec<String>>,
    #[serde(default)]
    pub confidence_range: Option<[f64; 2]>,
    #[serde(default)]
    pub required_fields: Vec<String>,
    #[serde(default)]
    pub min_report_chars: Option<usize>,
    #[serde(default)]
    pub max_report_chars: Option<usize>,
    #[serde(default)]
    pub key_evidence_min_items: Option<usize>,
    #[serde(default)]
    pub key_evidence_max_items: Option<usize>,
}

/// Weight for a scoring dimension (0-100).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DimensionWeight {
    pub weight: f64,
}

/// Execution mode for an eval case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EvalMode {
    Mock,
    Live,
}
