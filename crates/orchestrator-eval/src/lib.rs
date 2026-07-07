//! Prompt evaluation suite data structures.
//!
//! Defines the test-case format used by the `orchestrator-eval` binary to
//! score LLM artifacts produced by each workflow role. Test cases are JSON
//! files loaded from `tests/eval/cases/`; each case declares a role, an
//! input state, expected output constraints, and per-dimension weights.

pub mod baseline;
pub mod runner;
pub mod scoring;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// A single prompt-evaluation test case.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalCase {
    /// Unique identifier, e.g. `technical_basic_tqqq`.
    pub test_id: String,
    /// Human-readable description.
    pub description: String,
    /// Role string, e.g. `analyst.technical`, `trader`, `allocation.manager`.
    pub role: String,
    /// Workflow phase number (1-7).
    pub phase: i64,
    /// Prompt kind, e.g. `artifact`, `risk_argument`, `bull_seed`.
    pub kind: String,
    /// Input state for the test case.
    pub input: EvalInput,
    /// Expected output constraints for scoring.
    pub expected: EvalExpected,
    /// Per-dimension weights (should sum to 100).
    pub dimensions: BTreeMap<String, DimensionWeight>,
    /// Execution mode: `mock` (canned artifact) or `live` (LLM gateway).
    pub mode: EvalMode,
    /// Optional inline baseline aggregate score for quick regression checks.
    #[serde(default)]
    pub baseline_score: Option<f64>,
}

/// Input parameters for an eval case.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalInput {
    /// Primary ticker, e.g. `TQQQ`.
    pub ticker: String,
    /// All tickers in scope (including VIX, SOXX, etc.).
    pub tickers: Vec<String>,
    /// Run date in `YYYY-MM-DD` format.
    pub date: String,
    /// Optional path to a pre-seeded SQLite fixture.
    #[serde(default)]
    pub mock_db_path: Option<String>,
    /// Additional state keys merged into the run state.
    #[serde(default)]
    pub state_overrides: Value,
}

/// Expected output constraints used by the scoring engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalExpected {
    /// Acceptable direction values; `None` means direction is not checked.
    #[serde(default)]
    pub direction: Option<Vec<String>>,
    /// `[min, max]` confidence range; `None` means confidence is not checked.
    #[serde(default)]
    pub confidence_range: Option<[f64; 2]>,
    /// Fields that must be present in the artifact (top-level).
    #[serde(default)]
    pub required_fields: Vec<String>,
    /// Minimum report length in characters.
    #[serde(default)]
    pub min_report_chars: Option<usize>,
    /// Maximum report length in characters.
    #[serde(default)]
    pub max_report_chars: Option<usize>,
    /// Minimum number of `key_evidence` items.
    #[serde(default)]
    pub key_evidence_min_items: Option<usize>,
    /// Maximum number of `key_evidence` items.
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
    /// Use canned mock artifacts (no API cost).
    Mock,
    /// Call the live LLM gateway (requires `LLM_GATEWAY_API_KEY`).
    Live,
}
