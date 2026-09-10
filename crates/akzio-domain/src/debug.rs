//! Execution control changes scheduling authority, never a business contract.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{ArtifactRef, AttemptId, ContentHash, RunId, RunPurpose, TaskId};

fn default_debug_policy_status() -> String {
    "legacy_unknown".to_owned()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DebugStatus {
    Running,
    PauseRequested,
    Paused,
    Stepping,
    Completed,
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DebugExecutionMode {
    Manual,
    Continuous,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DebugLlmMode {
    Real,
    Fixture,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DebugBrokerPolicy {
    Forbidden,
    PaperAllowed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DebugLearningScope {
    Isolated,
    Canonical,
}

/// Immutable CAS identity. Mutable scheduling fields live in the Store head.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DebugSessionIdentity {
    pub version: u32,
    pub debug_session_id: String,
    pub store_identity: String,
    pub run_id: RunId,
    pub run_purpose: RunPurpose,
    pub llm_mode: DebugLlmMode,
    pub broker_write_policy: DebugBrokerPolicy,
    pub learning_scope: DebugLearningScope,
    pub code_revision: String,
    pub runtime_identity: ContentHash,
    /// Rust-owned observation of the policy loading path. The source path is
    /// intentionally not persisted; the input hash and status are enough to
    /// distinguish unconfigured/default from a validated frozen artifact.
    #[serde(default = "default_debug_policy_status")]
    pub decision_policy_status: String,
    #[serde(default)]
    pub decision_policy_input_hash: Option<ContentHash>,
    pub contract_hashes: Vec<ContentHash>,
    pub dataset: Vec<ArtifactRef>,
    pub parent_run_id: Option<RunId>,
    pub parent_task_id: Option<TaskId>,
    pub parent_artifacts: Vec<ArtifactRef>,
    pub reason: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DebugSession {
    pub identity: DebugSessionIdentity,
    pub revision: u64,
    pub status: DebugStatus,
    pub execution_mode: DebugExecutionMode,
    /// Available only until the claim transaction consumes it.
    pub permitted_task_id: Option<TaskId>,
    pub active_attempt_id: Option<AttemptId>,
    pub paused_at_task_id: Option<TaskId>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DebugAction {
    Pause,
    Resume,
    Step,
    RetryNode,
    Abort,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DebugControlRequest {
    pub action: DebugAction,
    pub expected_revision: u64,
    pub task_id: Option<TaskId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AcceptanceResult {
    Pass,
    Fail,
    Blocked,
    NotRun,
    NotApplicable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AcceptanceCategory {
    Input,
    Time,
    Authorization,
    LlmCall,
    ToolUse,
    Schema,
    Semantic,
    Numeric,
    Unit,
    Provenance,
    Persistence,
    Idempotency,
    SideEffect,
    Recovery,
    Budget,
    UiConsistency,
    LearningEligibility,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptanceCheck {
    pub check_id: String,
    pub category: AcceptanceCategory,
    pub expected: String,
    pub actual: String,
    pub result: AcceptanceResult,
    pub evidence_refs: Vec<ArtifactRef>,
    pub message: String,
}

/// Business outcomes and validation outcomes are deliberately independent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageAcceptance {
    pub version: u32,
    pub run_id: RunId,
    pub task_id: TaskId,
    pub attempt_id: AttemptId,
    pub stage: String,
    pub business_result: String,
    pub test_result: AcceptanceResult,
    pub checks: Vec<AcceptanceCheck>,
    pub created_at: DateTime<Utc>,
}
