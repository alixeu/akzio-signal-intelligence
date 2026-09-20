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
    ResearchShard, RetryPolicy, TaskBudget, TaskWritePermit, TerminationPolicy, ToolGrant,
    ToolKind, ToolSpec, WorkflowNode, DOMAIN_SCHEMA_VERSION,
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
mod prompts;
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
use catalogue::{ACTIVE_CONTRACT_VERSION, ACTIVE_PROMPT_BUNDLE_VERSION, RFC3339_TIMESTAMP_PATTERN};
use prompts::SHARED_GOVERNANCE_PROMPT;
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
include!("agent/structured.rs");
include!("agent/proposal_review.rs");
include!("agent/runtime_helpers.rs");
include!("agent/helpers.rs");

#[cfg(test)]
#[path = "agent/fixture_tests.rs"]
mod formal_fixture_tests;

#[cfg(test)]
#[path = "agent/late_model_tests.rs"]
mod late_model_tests;

#[cfg(test)]
#[path = "agent/deadline_tests.rs"]
mod deadline_tests;

#[cfg(test)]
#[path = "agent/prompt_render_tests.rs"]
mod prompt_render_tests;

#[cfg(test)]
use akzio_domain::{RunPurpose, TaskRecipeId};

#[cfg(test)]
#[path = "agent/review_context_tests.rs"]
mod review_context_tests;
