//! Rust-owned fixed workflow compilation for the runtime.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    time::Duration as StdDuration,
};

use akzio_domain::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance, ArtifactRef,
    AttemptId, ClaimStance, ContentHash, ContractPurpose, DomainError, FailureDisposition,
    LifecycleEventType, ResearchClaim, RetryPolicy, RunId, RunPurpose, RuntimeTaskClass,
    TaskBudget, TaskId, TaskRecipe, TaskRecipeId, TaskStatus, TaskWritePermit, WorkflowGraph,
    WorkflowNode, WorkflowProposal, WorkflowStatus, DOMAIN_SCHEMA_VERSION,
};
use akzio_store::{
    ClaimedAttempt, DaemonLease, RetryTaskResult, SessionReservation, SessionSlotReservation,
    Store, StoreError, StoredEvent, StoredRun, WorkflowCommit, WorkflowSnapshot,
};
use chrono::{DateTime, Duration, Utc};
use thiserror::Error;

mod catalogue;
mod compilation;
mod replay;
mod store_executor;
mod task;
mod workflow;

const POST_TERMINAL_WORKER_RECIPE_ID: &str = akzio_domain::LEARNING_OUTCOME_WORKER_RECIPE_ID;

pub use catalogue::{
    active_recipe_catalogue, rust_terminal_recipes, ActiveContractRecipe, RecipeCatalogue,
    TerminalRecipeSet, DECISION_GATE_RECIPE_ID, EVALUATE_RECIPE_ID, EVIDENCE_GATE_RECIPE_ID,
    EXECUTION_GATE_RECIPE_ID, PAPER_COMMIT_RECIPE_ID, RECONCILE_RECIPE_ID,
};
pub use compilation::should_run_structured_critique;
pub use store_executor::{
    StoreExecutor, StoreExecutorTelemetry, StoreMaintenanceKind, StoreMaintenanceOutcome,
    StoreMaintenanceState,
};
pub use task::TaskRuntime;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("legacy_workflow_retired")]
    LegacyWorkflowRetired,
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("blocking store executor failed: {0}")]
    StoreExecutor(String),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("recipe {0} is missing")]
    MissingRecipe(TaskRecipeId),
    #[error("active contract purpose is not allowed: {0}")]
    UnexpectedActiveContractPurpose(String),
    #[error("active contract purpose appears more than once: {0}")]
    DuplicateActiveContractPurpose(String),
    #[error("active contract is missing: {0}")]
    MissingActiveContract(&'static str),
    #[error("active contract {purpose} outputs {actual:?}, expected {expected:?}")]
    ActiveContractOutputMismatch {
        purpose: String,
        expected: ArtifactKind,
        actual: ArtifactKind,
    },
    #[error("active contract {0} is not the canonical Store head")]
    NonCanonicalActiveContract(String),
    #[error("Proposal may not schedule Rust terminal recipe {0}")]
    TerminalRecipeInProposal(TaskRecipeId),
    #[error("terminal recipe {recipe} has class {actual:?}, expected {expected:?}")]
    InvalidTerminalRecipe {
        recipe: TaskRecipeId,
        actual: RuntimeTaskClass,
        expected: RuntimeTaskClass,
    },
    #[error("proposal would exceed the configured workflow node limit")]
    WorkflowNodeLimit,
    #[error("run {0} must be terminal before it can be retried")]
    RetryRunNotTerminal(RunId),
    #[error("proposal task {task} exceeds child limit for recipe {recipe}")]
    WorkflowFanoutLimit { task: String, recipe: TaskRecipeId },
    #[error("proposal task {task} exceeds depth limit for recipe {recipe}")]
    WorkflowDepthLimit { task: String, recipe: TaskRecipeId },
    #[error("agent task {task} has multiple direct agent dependencies")]
    MultipleAgentParents { task: String },
    #[error("task lease duration must be positive")]
    InvalidTaskLeaseDuration,
    #[error("task retry backoff exceeds supported duration")]
    InvalidRetryBackoff,
    #[error("workflow node {0} diverges from its installed recipe")]
    NodeRecipeMismatch(akzio_domain::TaskId),
    #[error("workflow is missing required terminal gate {0}")]
    MissingTerminalGate(TaskRecipeId),
    #[error("workflow is missing required evidence gate {0}")]
    MissingEvidenceGate(TaskRecipeId),
    #[error("workflow includes unexpected terminal gate {0}")]
    UnexpectedTerminalGate(TaskRecipeId),
    #[error("workflow terminal gate {0} has an invalid dependency chain")]
    InvalidTerminalDependencies(TaskRecipeId),
    #[error("research node {0} may not depend on a terminal gate")]
    ResearchDependsOnTerminal(akzio_domain::TaskId),
    #[error("research root {0} bypasses the required evidence gate")]
    ResearchBypassesEvidence(akzio_domain::TaskId),
    #[error("task {0} has an invalid EvidenceNeed input")]
    InvalidEvidenceNeed(akzio_domain::TaskId),
    #[error("Evidence Gate input plan does not match research EvidenceNeeds")]
    InvalidEvidencePlan,
    #[error("workflow replay for run {run_id} diverged: {reason}")]
    ReplayDiverged { run_id: RunId, reason: String },
}

pub type RuntimeResult<T> = Result<T, RuntimeError>;

const ANALYST_RECIPE_ID: &str = akzio_domain::RESEARCH_ANALYST_RECIPE_ID;
const CRITIC_RECIPE_ID: &str = akzio_domain::RESEARCH_CRITIC_RECIPE_ID;
const SYNTHESIZER_RECIPE_ID: &str = akzio_domain::RESEARCH_SYNTHESIZER_RECIPE_ID;
pub const STRUCTURED_CRITIQUE_MATERIALITY_PPM: u32 = 500_000;
pub const STRUCTURED_CRITIQUE_CONFIDENCE_PPM: u32 = 500_000;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplayedWorkflowRevision {
    cursor: i64,
    graph_artifact: Artifact,
    graph: WorkflowGraph,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplayedTask {
    node: WorkflowNode,
    status: TaskStatus,
    active_attempt_id: Option<AttemptId>,
    attempt_count: u64,
    finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct ReplayedWorkflow {
    revisions: Vec<ReplayedWorkflowRevision>,
    tasks: BTreeMap<TaskId, ReplayedTask>,
    cancel_requested: bool,
    saw_task_start: bool,
    event_cursor: i64,
}

#[derive(Debug, Clone)]
pub struct WorkflowRuntime {
    research_settings: akzio_domain::ResearchSettings,
    agent_budgets: BTreeMap<String, TaskBudget>,
    store: Store,
    catalogue: RecipeCatalogue,
}

impl WorkflowRuntime {
    pub fn new(store: Store, catalogue: RecipeCatalogue) -> Self {
        Self {
            store,
            catalogue,
            research_settings: Default::default(),
            agent_budgets: akzio_domain::AgentBudgetConfig::default().resolved(),
        }
    }

    pub fn with_research_settings(
        mut self,
        settings: &akzio_domain::ResearchSettings,
    ) -> RuntimeResult<Self> {
        settings.validate()?;
        self.research_settings = settings.clone();
        Ok(self)
    }

    pub fn with_agent_budgets(
        mut self,
        config: &akzio_domain::AgentBudgetConfig,
    ) -> RuntimeResult<Self> {
        config.validate()?;
        self.agent_budgets = config.resolved();
        Ok(self)
    }

    fn resolved_budget(&self, recipe: &TaskRecipe) -> TaskBudget {
        self.agent_budgets
            .get(recipe.purpose.as_str())
            .cloned()
            .unwrap_or_else(|| recipe.budget.clone())
    }

    pub fn recipe(&self, recipe_id: &TaskRecipeId) -> RuntimeResult<&TaskRecipe> {
        self.catalogue.recipe(recipe_id)
    }
}

mod reducer;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryCause {
    Transport,
    RateLimited,
    InvalidOutput,
    Timeout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeOutcome {
    Succeeded(Vec<Artifact>),
    /// A Rust gate can succeed after forwarding already durable lineage without
    /// manufacturing a duplicate artifact. The task transition remains in the
    /// Store event log; the upstream artifact remains the source of truth.
    NoOutput,
    Committed,
    Failed,
    Skipped,
    Cancelled,
    DeferredUntil(DateTime<Utc>),
    Retry(RetryCause),
    RetryAfter(RetryCause, DateTime<Utc>),
}

/// Compatibility name for existing business handlers; there is one result protocol.
pub type TaskCompletion = NodeOutcome;
mod node;
pub use node::{NodeContext, NodeExecutor};

fn required_terminal<'a>(
    terminals: &BTreeMap<TaskRecipeId, &'a WorkflowNode>,
    recipe_id: &TaskRecipeId,
) -> RuntimeResult<&'a WorkflowNode> {
    terminals
        .get(recipe_id)
        .copied()
        .ok_or_else(|| RuntimeError::MissingTerminalGate(recipe_id.clone()))
}

fn leaf_ids(nodes: &[WorkflowNode]) -> Vec<akzio_domain::TaskId> {
    let depended_on = nodes
        .iter()
        .flat_map(|node| node.dependencies.iter().cloned())
        .collect::<BTreeSet<_>>();
    nodes
        .iter()
        .map(|node| node.task_id.clone())
        .filter(|task_id| !depended_on.contains(task_id))
        .collect()
}
