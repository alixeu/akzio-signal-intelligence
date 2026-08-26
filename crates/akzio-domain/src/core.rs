//! The versioned, Rust-owned language shared by every Akzio v2 module.
//!
//! This crate deliberately contains no database, model, network, or filesystem
//! dependency.  It is the single source of truth for contracts, workflow
//! state, evidence references, and execution intent.

//! Foundational scalar types shared by the v2 domain graph.

use std::{collections::BTreeMap, fmt};

use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DomainError {
    #[error("asset {0:?} is not executable by Akzio v2")]
    UnsupportedAsset(String),
    #[error("content hash must be a lowercase sha256 hex digest")]
    InvalidContentHash,
    #[error("{field} must not be empty")]
    EmptyField { field: &'static str },
    #[error("task graph contains a duplicate task id {0}")]
    DuplicateTaskId(TaskId),
    #[error("task {task} references unknown dependency {dependency}")]
    UnknownDependency { task: TaskId, dependency: TaskId },
    #[error("unknown lifecycle event type {0}")]
    UnknownLifecycleEventType(String),
    #[error("evidence source {0} is not allowed by the installed recipe")]
    EvidenceSourceNotAllowed(String),
    #[error("task graph contains a cycle")]
    CyclicPlan,
    #[error("budget {field} must be positive")]
    InvalidBudget { field: &'static str },
    #[error("target portfolio must include exactly TQQQ, QQQ, SOXX, and SOXL")]
    InvalidTargetUniverse,
    #[error("decision confidence must be at most one million ppm")]
    InvalidDecisionConfidence,
    #[error(
        "decision forecasts must cover every executable asset at 1, 3, and 5 trading days exactly"
    )]
    InvalidDecisionForecastHorizons,
    #[error("decision forecast probability must be at most one million ppm")]
    InvalidDecisionForecastProbability,
    #[error("a document attempt origin requires a task origin")]
    AttemptOriginWithoutTask,
    #[error("raw evidence may only be read through a Rust-controlled tool")]
    RawEvidenceDirectContext,
    #[error("Paper reprice must be the single deterministic r0 to r1 lineage")]
    InvalidRepriceLineage,
    #[error("execution plan hash does not match its payload")]
    ExecutionPlanHashMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Asset {
    Tqqq,
    Qqq,
    Soxx,
    Soxl,
}

impl Asset {
    pub const EXECUTABLE: [Self; 4] = [Self::Tqqq, Self::Qqq, Self::Soxx, Self::Soxl];

    pub const fn symbol(self) -> &'static str {
        match self {
            Self::Tqqq => "TQQQ",
            Self::Qqq => "QQQ",
            Self::Soxx => "SOXX",
            Self::Soxl => "SOXL",
        }
    }
}

impl fmt::Display for Asset {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.symbol())
    }
}

impl TryFrom<&str> for Asset {
    type Error = DomainError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.trim().to_ascii_uppercase().as_str() {
            "TQQQ" => Ok(Self::Tqqq),
            "QQQ" => Ok(Self::Qqq),
            "SOXX" => Ok(Self::Soxx),
            "SOXL" => Ok(Self::Soxl),
            other => Err(DomainError::UnsupportedAsset(other.to_owned())),
        }
    }
}

impl<'de> Deserialize<'de> for Asset {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_from(value.as_str()).map_err(D::Error::custom)
    }
}

/// Exact portfolio weight, expressed in parts per million.
///
/// Integer weights keep model JSON, execution policy, hashing, and replay on
/// the same arithmetic surface.  Floats are intentionally not admitted to
/// canonical decision documents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WeightPpm(pub u32);

impl WeightPpm {
    pub const ZERO: Self = Self(0);
    pub const SCALE: u32 = 1_000_000;
}

/// Signed money in millionths of a USD.  Execution accepts integer money only
/// so an order plan has a stable content hash across platforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MoneyMicros(pub i64);

impl MoneyMicros {
    pub const ZERO: Self = Self(0);

    pub const fn from_usd_cents(cents: i64) -> Self {
        Self(cents.saturating_mul(10_000))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContentHash(String);

impl ContentHash {
    pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(DomainError::InvalidContentHash);
        }
        Ok(Self(value))
    }

    pub fn of_bytes(bytes: &[u8]) -> Self {
        Self(format!("{:x}", Sha256::digest(bytes)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ContentHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

pub fn canonical_json_bytes(value: &Value) -> Result<Vec<u8>, serde_json::Error> {
    fn canonicalize(value: &Value) -> Value {
        match value {
            Value::Array(items) => Value::Array(items.iter().map(canonicalize).collect()),
            Value::Object(items) => Value::Object(
                items
                    .iter()
                    .map(|(key, value)| (key.clone(), canonicalize(value)))
                    .collect(),
            ),
            value => value.clone(),
        }
    }

    serde_json::to_vec(&canonicalize(value))
}

pub fn content_hash_json(value: &Value) -> Result<ContentHash, serde_json::Error> {
    canonical_json_bytes(value).map(|bytes| ContentHash::of_bytes(&bytes))
}

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            pub fn new() -> Self {
                let value = Uuid::new_v4().simple().to_string();
                Self(value[..16].to_owned())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

id_type!(RunId);
id_type!(TaskId);
id_type!(AttemptId);
id_type!(LeaseId);
id_type!(ContractId);
id_type!(TopologyId);
id_type!(DecisionId);
id_type!(MemoryId);

/// Canonical target portfolio.  Cash is intentionally implicit: every
/// executable asset must be present, and unallocated equity remains cash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetPortfolio {
    pub weights: BTreeMap<Asset, WeightPpm>,
}

impl TargetPortfolio {
    pub fn zeroed() -> Self {
        Self {
            weights: Asset::EXECUTABLE
                .into_iter()
                .map(|asset| (asset, WeightPpm::ZERO))
                .collect(),
        }
    }

    pub fn validate_universe(&self) -> Result<(), DomainError> {
        if self.weights.len() != Asset::EXECUTABLE.len()
            || !Asset::EXECUTABLE
                .into_iter()
                .all(|asset| self.weights.contains_key(&asset))
        {
            return Err(DomainError::InvalidTargetUniverse);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRef {
    pub hash: ContentHash,
    pub media_type: String,
    pub bytes: u64,
}

impl BlobRef {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.media_type.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "blob_ref.media_type",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunPurpose {
    Debug,
    Paper,
    PaperDryRun,
    Replay,
    Shadow,
}

impl RunPurpose {
    pub const fn is_canonical_learning(self) -> bool {
        matches!(self, Self::Paper)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Leased,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Skipped,
}

impl TaskStatus {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Skipped
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    Queued,
    Leased,
    Running,
    DecisionCompleted,
    Completed,
    CompletedWithExecutionRejection,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskBudget {
    pub max_input_tokens: u32,
    pub max_output_tokens: u32,
    pub max_wall_time_secs: u32,
    pub max_tool_calls: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolKind {
    ReadEvidence,
    ReadRawEvidence,
    FetchWebEvidence,
    ReadMarketData,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolGrant {
    pub kind: ToolKind,
    /// Empty means the tool is source-agnostic.  Otherwise every requested
    /// source must be explicitly present in this allowlist.
    pub allowed_sources: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u8,
    pub initial_backoff_ms: u64,
    pub retry_transport: bool,
    pub retry_rate_limited: bool,
    pub retry_invalid_output: bool,
}

impl RetryPolicy {
    pub const fn none() -> Self {
        Self {
            max_attempts: 1,
            initial_backoff_ms: 0,
            retry_transport: false,
            retry_rate_limited: false,
            retry_invalid_output: false,
        }
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.max_attempts == 0 {
            return Err(DomainError::InvalidBudget {
                field: "retry.max_attempts",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminationPolicy {
    pub max_child_tasks: u16,
    pub max_depth: u16,
    pub require_evidence: bool,
    pub stop_when_evidence_complete: bool,
}

impl TerminationPolicy {
    pub const fn leaf() -> Self {
        Self {
            max_child_tasks: 0,
            max_depth: 0,
            require_evidence: true,
            stop_when_evidence_complete: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureDisposition {
    FailRun,
    FailTask,
    SkipTask,
}

impl TaskBudget {
    pub fn validate(&self) -> Result<(), DomainError> {
        for (field, value) in [
            ("max_input_tokens", self.max_input_tokens),
            ("max_output_tokens", self.max_output_tokens),
            ("max_wall_time_secs", self.max_wall_time_secs),
        ] {
            if value == 0 {
                return Err(DomainError::InvalidBudget { field });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_runtime_ids_are_sixteen_lowercase_hex_characters() {
        for value in [RunId::new().0, TaskId::new().0, AttemptId::new().0] {
            assert_eq!(value.len(), 16);
            assert!(value.bytes().all(|byte| byte.is_ascii_hexdigit()));
            assert_eq!(value, value.to_ascii_lowercase());
        }
    }

    #[test]
    fn executable_assets_are_exactly_the_v2_universe() {
        assert_eq!(Asset::EXECUTABLE.len(), 4);
        assert_eq!(Asset::try_from("SOXL").unwrap(), Asset::Soxl);
        assert!(Asset::try_from("VIX").is_err());
    }

    #[test]
    fn only_paper_runs_are_canonical_learning() {
        assert!(RunPurpose::Paper.is_canonical_learning());
        for purpose in [
            RunPurpose::Debug,
            RunPurpose::Replay,
            RunPurpose::PaperDryRun,
            RunPurpose::Shadow,
        ] {
            assert!(!purpose.is_canonical_learning());
        }
        assert_eq!(
            serde_json::to_string(&RunPurpose::Replay).unwrap(),
            "\"replay\""
        );
    }
}
