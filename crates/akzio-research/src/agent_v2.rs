//! Contract-driven Agent runtime for the v2 system.

use akzio_domain::{AttemptId, RunId, TaskId};
use akzio_model::ModelStreamEvent;
use std::sync::Arc;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration as StdDuration, Instant},
};
use tokio::sync::broadcast;

use akzio_context::v2::{ContextBroker, ContextError, ContextManifest};
#[cfg(test)]
use akzio_domain::RuntimeTaskClass;
use akzio_domain::{
    validate_decision_evidence_sufficiency, AgentContract, AgentOutputEnvelope, Artifact,
    ArtifactId, ArtifactKind, ArtifactLifecycle, ArtifactProvenance, ArtifactRef, Asset,
    ContextPolicy, ContractId, ContractPurpose, DecisionDraft, DeliberationPolicy, DomainError,
    EvidenceGroundRole, FailureDisposition, LifecycleEventType, OutputContract, PromptBundle,
    ReadGrant, ResearchClaim, ResearchCritique, ResearchResolution, ResearchShard, RetryPolicy,
    RunPurpose, TaskBudget, TaskRecipeId, TaskWritePermit, TerminationPolicy, ToolGrant, ToolKind,
    ToolSpec, WorkflowNode, V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_model::{
    ModelCallTrace, ModelCapabilitySnapshot, ModelClient, ModelContinuation, ModelError,
    ModelInput, ModelRequest, ModelToolChoice, ModelToolDefinition, ModelToolOutput,
};
use akzio_runtime::v2::{RecipeCatalogue, RetryCause, RuntimeError, StoreExecutor};
use akzio_store::v2::{StoreError, StoredContract, V2Store};
use chrono::{DateTime, Duration, Utc};
use futures::future::BoxFuture;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

mod catalogue;
mod schemas;
mod tools;
mod validation;

use akzio_domain::{
    GOVERNED_EVIDENCE_SOURCE_FAMILIES, LEARNING_OUTCOME_WORKER_RECIPE_ID,
    RESEARCH_ANALYST_RECIPE_ID, RESEARCH_CRITIC_RECIPE_ID, RESEARCH_SYNTHESIZER_RECIPE_ID,
};
#[cfg(test)]
use catalogue::recipe_evidence_sources;
pub use catalogue::{
    ActiveResearchCatalogue, ContractCatalogue, InstalledContract, ACTIVE_RESEARCH_MAX_NODES,
};
use catalogue::{
    ACTIVE_CONTRACT_VERSION, ACTIVE_PROMPT_BUNDLE_VERSION, PLANNER_CHILD_RECIPE_IDS,
    PLANNER_MAX_DRAFT_TASKS, PLANNER_RECIPE_ID, RFC3339_TIMESTAMP_PATTERN,
    SHARED_GOVERNANCE_PROMPT,
};
use schemas::*;
use tools::*;
use validation::*;
include!("agent_v2_parts/errors_catalogue.rs");
include!("agent_v2_parts/model_types.rs");
include!("agent_v2_parts/runtime_type.rs");
include!("agent_v2_parts/recovery.rs");
include!("agent_v2_parts/runtime_core.rs");
include!("agent_v2_parts/runtime_run.rs");
include!("agent_v2_parts/runtime_helpers.rs");
include!("agent_v2_parts/helpers.rs");
include!("agent_v2_parts/decision_tests.rs");

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
