//! Contract-driven Agent runtime for the system.

use akzio_domain::{AttemptId, RunId, TaskId};
use akzio_model::ModelStreamEvent;
use std::sync::Arc;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration as StdDuration, Instant},
};
use tokio::sync::broadcast;

use akzio_context::{
    ContextBroker, ContextError, ContextManifest, ContextMaterialization, ContextReadResult,
};
use akzio_domain::{
    validate_decision_evidence_sufficiency, AgentContract, AgentOutputEnvelope, Artifact,
    ArtifactId, ArtifactKind, ArtifactLifecycle, ArtifactProvenance, ArtifactRef, Asset,
    ClaimVerificationStatus, ContextPolicy, ContractId, ContractPurpose, DecisionDraft,
    DeliberationPolicy, DomainError, EvidenceGroundRole, FailureDisposition, LifecycleEventType,
    OutputContract, PromptBundle, ReadGrant, ResearchClaim, ResearchCritique, ResearchResolution,
    ResearchShard, RetryPolicy, RunPurpose, TaskBudget, TaskRecipeId, TaskWritePermit,
    TerminationPolicy, ToolGrant, ToolKind, ToolSpec, WorkflowNode, DOMAIN_SCHEMA_VERSION,
};
use akzio_model::{
    ModelBudgetPolicy, ModelCallTrace, ModelCapabilitySnapshot, ModelClient, ModelContinuation,
    ModelError, ModelInput, ModelPricingSnapshot, ModelRequest, ModelToolChoice,
    ModelToolDefinition, ModelToolOutput, ModelUsage,
};
use akzio_runtime::{RecipeCatalogue, RetryCause, RuntimeError, StoreExecutor};
use akzio_store::{Store, StoreError, StoredContract};
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
include!("agent/errors_catalogue.rs");
include!("agent/model_types.rs");
include!("agent/budget.rs");
include!("agent/projection.rs");
include!("agent/runtime_type.rs");
include!("agent/recovery.rs");
include!("agent/runtime_core.rs");
include!("agent/runtime_run.rs");
include!("agent/runtime_helpers.rs");
include!("agent/helpers.rs");
