//! Dynamic workflow lowering for the v2 runtime.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    time::Duration as StdDuration,
};

use akzio_domain::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance, ArtifactRef,
    AttemptId, ClaimStance, DomainError, EvidenceNeed, ResearchClaim, RunId, RunPurpose,
    RuntimeTaskClass, TaskId, TaskRecipe, TaskRecipeId, TaskStatus, WorkflowGraph, WorkflowNode,
    WorkflowProposal, WorkflowProposalDraft, WorkflowProposalTask, WorkflowStatus,
    STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID, V2_SCHEMA_VERSION,
};
use akzio_store::v2::{
    ClaimedAttempt, DaemonLease, RetryTaskResult, SessionReservation, SessionSlotReservation,
    StoreError, StoredEvent, StoredRun, V2Store, WorkflowCommit, WorkflowPatchCommit,
    WorkflowRevision, WorkflowSnapshot,
};
use chrono::{DateTime, Duration, Utc};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("recipe {0} is missing")]
    MissingRecipe(TaskRecipeId),
    #[error("Planner may not schedule Rust terminal recipe {0}")]
    TerminalRecipeInProposal(TaskRecipeId),
    #[error(
        "planner may not schedule research.critic; Rust inserts at most one conditional critic"
    )]
    PlannerSchedulesCritic,
    #[error("planner may not schedule more than one research.synthesizer")]
    PlannerSchedulesMultipleSynthesizers,
    #[error("terminal recipe {recipe} has class {actual:?}, expected {expected:?}")]
    InvalidTerminalRecipe {
        recipe: TaskRecipeId,
        actual: RuntimeTaskClass,
        expected: RuntimeTaskClass,
    },
    #[error("workflow already contains a terminal execution gate")]
    TerminalGateAlreadyPresent,
    #[error("proposal would exceed the configured workflow node limit")]
    WorkflowNodeLimit,
    #[error("Paper workflow {0} is frozen once submitted")]
    FrozenPaperWorkflow(RunId),
    #[error("run {0} must be terminal before it can be retried")]
    RetryRunNotTerminal(RunId),
    #[error("run purpose {0:?} cannot be retried through the operator surface")]
    RetryPurpose(RunPurpose),
    #[error("planner task {task} exceeds child limit for recipe {recipe}")]
    WorkflowFanoutLimit { task: String, recipe: TaskRecipeId },
    #[error("planner task {task} exceeds depth limit for recipe {recipe}")]
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
    #[error("workflow contains more than one planner task")]
    DuplicatePlannerTask,
    #[error("workflow patch must be authored by an active planner attempt")]
    PlannerPermitRequired,
    #[error("Paper workflows require a precompiled proposal")]
    PaperWorkflowRequiresPrecompiledProposal,
    #[error("workflow replay for run {run_id} diverged: {reason}")]
    ReplayDiverged { run_id: RunId, reason: String },
}

pub type RuntimeResult<T> = Result<T, RuntimeError>;

const ANALYST_RECIPE_ID: &str = "research.analyst";
const CRITIC_RECIPE_ID: &str = "research.critic";
const SYNTHESIZER_RECIPE_ID: &str = "research.synthesizer";
const STRUCTURED_CRITIC_ALIAS_PREFIX: &str = "structured_critic";
pub const STRUCTURED_CRITIQUE_MATERIALITY_PPM: u32 = 500_000;
pub const STRUCTURED_CRITIQUE_CONFIDENCE_PPM: u32 = 500_000;

/// Returns whether the bounded Critic task should consume the supplied claims.
/// Rust owns this decision so a planner or model cannot add debate rounds.
pub fn should_run_structured_critique(claims: &[ResearchClaim]) -> bool {
    claims.iter().any(|claim| {
        claim.materiality_ppm >= STRUCTURED_CRITIQUE_MATERIALITY_PPM
            && (!claim.evidence_gaps.is_empty()
                || claim.confidence_ppm <= STRUCTURED_CRITIQUE_CONFIDENCE_PPM)
    }) || claims.iter().enumerate().any(|(index, claim)| {
        claims[index + 1..].iter().any(|other| {
            claim.topic == other.topic
                && claim.horizon == other.horizon
                && matches!(
                    (claim.stance, other.stance),
                    (ClaimStance::Bullish, ClaimStance::Bearish)
                        | (ClaimStance::Bearish, ClaimStance::Bullish)
                )
        })
    })
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalRecipeSet {
    pub evidence_gate: TaskRecipeId,
    pub decision_gate: TaskRecipeId,
    pub execution_gate: TaskRecipeId,
    pub paper_commit: TaskRecipeId,
    pub reconcile: TaskRecipeId,
    pub evaluate: TaskRecipeId,
}

#[derive(Debug, Clone)]
pub struct RecipeCatalogue {
    recipes: BTreeMap<TaskRecipeId, TaskRecipe>,
    planner: TaskRecipeId,
    terminals: TerminalRecipeSet,
    max_nodes: usize,
}

impl RecipeCatalogue {
    pub fn new(
        recipes: impl IntoIterator<Item = TaskRecipe>,
        planner: TaskRecipeId,
        terminals: TerminalRecipeSet,
        max_nodes: usize,
    ) -> RuntimeResult<Self> {
        let recipes = recipes
            .into_iter()
            .map(|recipe| {
                recipe.validate()?;
                Ok((recipe.recipe_id.clone(), recipe))
            })
            .collect::<Result<BTreeMap<_, _>, DomainError>>()?;
        let catalogue = Self {
            recipes,
            planner,
            terminals,
            max_nodes,
        };
        if catalogue.max_nodes == 0 {
            return Err(RuntimeError::WorkflowNodeLimit);
        }
        catalogue.assert_planner(&catalogue.planner)?;
        catalogue.assert_terminal(
            &catalogue.terminals.evidence_gate,
            RuntimeTaskClass::Evidence,
        )?;
        catalogue.assert_terminal(
            &catalogue.terminals.decision_gate,
            RuntimeTaskClass::DecisionGate,
        )?;
        catalogue.assert_terminal(
            &catalogue.terminals.execution_gate,
            RuntimeTaskClass::ExecutionGate,
        )?;
        catalogue.assert_terminal(
            &catalogue.terminals.paper_commit,
            RuntimeTaskClass::PaperCommit,
        )?;
        catalogue.assert_terminal(&catalogue.terminals.reconcile, RuntimeTaskClass::Reconcile)?;
        catalogue.assert_terminal(&catalogue.terminals.evaluate, RuntimeTaskClass::Evaluate)?;
        Ok(catalogue)
    }

    fn assert_planner(&self, recipe_id: &TaskRecipeId) -> RuntimeResult<()> {
        let recipe = self.recipe(recipe_id)?;
        if recipe.task_class != RuntimeTaskClass::Agent || recipe.contract_hash.is_none() {
            return Err(RuntimeError::PlannerPermitRequired);
        }
        Ok(())
    }

    pub fn recipe(&self, recipe_id: &TaskRecipeId) -> RuntimeResult<&TaskRecipe> {
        self.recipes
            .get(recipe_id)
            .ok_or_else(|| RuntimeError::MissingRecipe(recipe_id.clone()))
    }

    fn assert_terminal(
        &self,
        recipe_id: &TaskRecipeId,
        expected: RuntimeTaskClass,
    ) -> RuntimeResult<()> {
        let recipe = self.recipe(recipe_id)?;
        if recipe.task_class != expected {
            return Err(RuntimeError::InvalidTerminalRecipe {
                recipe: recipe_id.clone(),
                actual: recipe.task_class,
                expected,
            });
        }
        Ok(())
    }

    fn is_terminal(&self, recipe_id: &TaskRecipeId) -> bool {
        [
            &self.terminals.decision_gate,
            &self.terminals.execution_gate,
            &self.terminals.paper_commit,
            &self.terminals.reconcile,
            &self.terminals.evaluate,
        ]
        .into_iter()
        .any(|terminal| terminal == recipe_id)
    }

    fn is_rust_gate(&self, recipe_id: &TaskRecipeId) -> bool {
        recipe_id == &self.terminals.evidence_gate || self.is_terminal(recipe_id)
    }
}

#[derive(Debug, Clone)]
pub struct WorkflowRuntime {
    store: V2Store,
    catalogue: RecipeCatalogue,
}

impl WorkflowRuntime {
    pub fn new(store: V2Store, catalogue: RecipeCatalogue) -> Self {
        Self { store, catalogue }
    }

    pub fn catalogue(&self) -> &RecipeCatalogue {
        &self.catalogue
    }

    /// Compile a model proposal into a graph plus Rust-owned terminal gates. The
    /// proposal cannot name any gate recipe, create a contract, or omit final
    /// audit/evaluation transitions.
    pub fn lower(
        &self,
        purpose: RunPurpose,
        proposal: &WorkflowProposal,
    ) -> RuntimeResult<WorkflowGraph> {
        let nodes = self.lower_research_nodes(proposal)?;
        self.with_terminal_gates(purpose, proposal.topology_id.clone(), nodes)
    }

    /// Creates the non-Paper bootstrap graph whose sole model task is the
    /// installed Planner. The Planner may extend this graph only while its
    /// active attempt still owns a valid write permit.
    pub fn bootstrap(
        &self,
        purpose: RunPurpose,
        topology_id: impl Into<String>,
    ) -> RuntimeResult<WorkflowGraph> {
        if purpose == RunPurpose::Paper {
            return Err(RuntimeError::PaperWorkflowRequiresPrecompiledProposal);
        }
        let topology_id = topology_id.into();
        if topology_id.trim().is_empty() {
            return Err(RuntimeError::Domain(DomainError::EmptyField {
                field: "workflow.topology_id",
            }));
        }
        let recipe = self.catalogue.recipe(&self.catalogue.planner)?;
        let planner = WorkflowNode {
            task_id: akzio_domain::TaskId::new(),
            recipe_id: recipe.recipe_id.clone(),
            contract_hash: recipe.contract_hash.clone(),
            objective: "Produce a bounded workflow proposal".to_owned(),
            dependencies: vec![],
            input_artifacts: vec![],
            priority: recipe.priority_ceiling,
            budget: recipe.budget.clone(),
            retry: recipe.retry.clone(),
            on_failure: recipe.on_failure,
            parent_task_id: None,
        };
        self.with_terminal_gates(purpose, topology_id, vec![planner])
    }

    pub fn submit(
        &self,
        run_id: RunId,
        purpose: RunPurpose,
        graph: WorkflowGraph,
        now: DateTime<Utc>,
    ) -> RuntimeResult<Artifact> {
        graph.validate()?;
        self.validate_compiled_graph(purpose, &graph)?;
        let graph_artifact = self.graph_artifact(&graph, vec![], now)?;
        self.store.commit_workflow(&WorkflowCommit {
            run: StoredRun {
                run_id,
                purpose,
                topology_id: graph.topology_id.clone(),
                graph_artifact_id: graph_artifact.artifact_id.clone(),
                created_at: now,
            },
            graph: graph_artifact.clone(),
            nodes: graph.nodes,
        })?;
        Ok(graph_artifact)
    }

    /// Load the exact durable graph/task state for crash recovery. Recovery
    /// never re-lowers a proposal or allocates replacement task IDs.
    /// Freeze one fully compiled Paper workflow into its broker-session slot.
    /// A duplicate session returns the already durable graph and task IDs; it
    /// never regenerates a replacement graph after a scheduler restart.
    pub fn reserve_paper_session(
        &self,
        lease: &DaemonLease,
        session_key: impl Into<String>,
        proposal: &WorkflowProposal,
        now: DateTime<Utc>,
    ) -> RuntimeResult<SessionSlotReservation> {
        self.reserve_paper_session_with_inputs(lease, session_key, proposal, &[], now)
    }

    /// As [`Self::reserve_paper_session`], but atomically installs the
    /// scheduler-owned immutable `EvidenceNeed` artifacts referenced by the
    /// compiled graph.
    pub fn reserve_paper_session_with_inputs(
        &self,
        lease: &DaemonLease,
        session_key: impl Into<String>,
        proposal: &WorkflowProposal,
        setup_artifacts: &[Artifact],
        now: DateTime<Utc>,
    ) -> RuntimeResult<SessionSlotReservation> {
        self.reserve_paper_session_with_inputs_for_run(
            lease,
            RunId::new(),
            session_key,
            proposal,
            setup_artifacts,
            now,
        )
    }

    /// Reserve the exact caller-allocated run identity. This exists for the
    /// scheduler's preflight transaction, which binds immutable evidence need
    /// artifacts to the same Run before it becomes visible.
    pub fn reserve_paper_session_with_inputs_for_run(
        &self,
        lease: &DaemonLease,
        run_id: RunId,
        session_key: impl Into<String>,
        proposal: &WorkflowProposal,
        setup_artifacts: &[Artifact],
        now: DateTime<Utc>,
    ) -> RuntimeResult<SessionSlotReservation> {
        let session_key = session_key.into();
        if let Some(slot) = self.store.session_slot(&session_key)? {
            return Ok(SessionSlotReservation {
                slot,
                newly_reserved: false,
            });
        }

        let graph = self.lower(RunPurpose::Paper, proposal)?;
        let graph_artifact = self.graph_artifact(&graph, Vec::new(), now)?;
        let workflow = WorkflowCommit {
            run: StoredRun {
                run_id,
                purpose: RunPurpose::Paper,
                topology_id: graph.topology_id.clone(),
                graph_artifact_id: graph_artifact.artifact_id.clone(),
                created_at: now,
            },
            graph: graph_artifact,
            nodes: graph.nodes,
        };
        Ok(self.store.reserve_session_slot(
            lease,
            &SessionReservation {
                session_key,
                workflow,
                setup_artifacts: setup_artifacts.to_vec(),
                reserved_at: now,
            },
        )?)
    }

    /// Build the Rust-owned, precompiled Paper proposal used for the first
    /// scheduler session. It contains no model output and cannot be patched
    /// after the Paper graph is frozen.
    pub fn approved_paper_proposal(
        &self,
        topology_id: impl Into<String>,
    ) -> RuntimeResult<WorkflowProposal> {
        let analyst = self
            .catalogue
            .recipe(&TaskRecipeId::new(ANALYST_RECIPE_ID)?)?;
        let synthesizer = self
            .catalogue
            .recipe(&TaskRecipeId::new(SYNTHESIZER_RECIPE_ID)?)?;
        let proposal = WorkflowProposal {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: topology_id.into(),
            tasks: BTreeMap::from([
                (
                    "analyst".to_owned(),
                    akzio_domain::WorkflowProposalTask {
                        recipe_id: analyst.recipe_id.clone(),
                        objective: "Assess governed Paper market evidence".to_owned(),
                        depends_on: Vec::new(),
                        priority: analyst.priority_ceiling,
                        evidence_needs: Vec::new(),
                    },
                ),
                (
                    "synthesizer".to_owned(),
                    akzio_domain::WorkflowProposalTask {
                        recipe_id: synthesizer.recipe_id.clone(),
                        objective:
                            "Synthesize approved Paper research into a bounded decision proposal"
                                .to_owned(),
                        depends_on: vec!["analyst".to_owned()],
                        priority: synthesizer.priority_ceiling,
                        evidence_needs: Vec::new(),
                    },
                ),
            ]),
            stop_reason: Some("rust-approved Paper provisioning".to_owned()),
        };
        proposal.validate(&self.catalogue.recipes)?;
        self.validate_proposal_limits(&proposal)?;
        Ok(proposal)
    }

    pub fn reserve_approved_paper_session(
        &self,
        lease: &DaemonLease,
        run_id: RunId,
        session_key: impl Into<String>,
        topology_id: impl Into<String>,
        setup_artifacts: &[Artifact],
        now: DateTime<Utc>,
    ) -> RuntimeResult<SessionSlotReservation> {
        let mut proposal = self.approved_paper_proposal(topology_id)?;
        let snapshot_refs = setup_artifacts
            .iter()
            .map(|artifact| ArtifactRef {
                artifact_id: artifact.artifact_id.clone(),
                kind: artifact.kind,
            })
            .collect::<Vec<_>>();
        proposal
            .tasks
            .get_mut("analyst")
            .ok_or(RuntimeError::MissingRecipe(TaskRecipeId::new(
                ANALYST_RECIPE_ID,
            )?))?
            .evidence_needs = snapshot_refs;
        proposal.validate(&self.catalogue.recipes)?;
        self.validate_proposal_limits(&proposal)?;
        let proposal_artifact = Artifact::new(
            ArtifactKind::WorkflowProposal,
            self.store.put_json(&proposal)?,
            "runtime.paper_provisioning",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.runtime".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            Some(ArtifactOrigin {
                run_id: Some(run_id.clone()),
                task_id: None,
                attempt_id: None,
                contract_hash: None,
            }),
            proposal
                .tasks
                .values()
                .flat_map(|task| task.evidence_needs.iter().cloned())
                .collect(),
            now,
        )?;
        let session_key = session_key.into();
        let graph = self.lower(RunPurpose::Paper, &proposal)?;
        let graph_artifact = self.graph_artifact(
            &graph,
            vec![ArtifactRef {
                artifact_id: proposal_artifact.artifact_id.clone(),
                kind: ArtifactKind::WorkflowProposal,
            }],
            now,
        )?;
        let workflow = WorkflowCommit {
            run: StoredRun {
                run_id,
                purpose: RunPurpose::Paper,
                topology_id: graph.topology_id.clone(),
                graph_artifact_id: graph_artifact.artifact_id.clone(),
                created_at: now,
            },
            graph: graph_artifact,
            nodes: graph.nodes,
        };
        Ok(self.store.reserve_paper_session_with_proposal(
            lease,
            &SessionReservation {
                session_key,
                workflow,
                setup_artifacts: setup_artifacts.to_vec(),
                reserved_at: now,
            },
            &proposal_artifact,
        )?)
    }

    pub fn recover(&self, run_id: &RunId) -> RuntimeResult<WorkflowSnapshot> {
        let snapshot = self.store.workflow_snapshot(run_id)?;
        self.validate_compiled_graph(snapshot.run.purpose, &snapshot.revision.graph)?;
        Ok(snapshot)
    }

    /// Reconstruct a Run from its append-only event stream and durable task
    /// history, rejecting a snapshot that cannot be derived from that history.
    /// Create a fresh noncanonical rerun. Paper, Shadow and Replay workloads
    /// have distinct owner flows and can never be retried through HTTP.
    pub fn retry_run(&self, source_run_id: &RunId, now: DateTime<Utc>) -> RuntimeResult<RunId> {
        let source = self.replay_run(source_run_id)?;
        if !matches!(
            source.status,
            WorkflowStatus::Completed
                | WorkflowStatus::CompletedWithExecutionRejection
                | WorkflowStatus::Failed
                | WorkflowStatus::Cancelled
        ) {
            return Err(RuntimeError::RetryRunNotTerminal(source_run_id.clone()));
        }
        if !matches!(
            source.run.purpose,
            RunPurpose::Debug | RunPurpose::PaperDryRun
        ) {
            return Err(RuntimeError::RetryPurpose(source.run.purpose));
        }
        let run_id = RunId::new();
        let graph = self.bootstrap(source.run.purpose, source.run.topology_id.clone())?;
        self.submit(run_id.clone(), source.run.purpose, graph, now)?;
        Ok(run_id)
    }

    pub fn replay_run(&self, run_id: &RunId) -> RuntimeResult<WorkflowSnapshot> {
        let replay = self.reduce_history(run_id)?;
        self.validate_replay_revisions(run_id, &replay)?;
        let snapshot = self.store.workflow_snapshot(run_id)?;
        self.validate_replay_snapshot(run_id, &replay, &snapshot)?;
        self.validate_compiled_graph(snapshot.run.purpose, &snapshot.revision.graph)?;
        Ok(snapshot)
    }

    /// Replay an immutable graph revision through the event reducer and the
    /// current v2 invariants. This never trusts a revision row by itself.
    pub fn replay_revision(
        &self,
        run_id: &RunId,
        revision: u64,
    ) -> RuntimeResult<WorkflowRevision> {
        let replay = self.reduce_history(run_id)?;
        self.validate_replay_revisions(run_id, &replay)?;
        let reduced = replay.revisions.get(revision as usize).ok_or_else(|| {
            Self::replay_error(
                run_id,
                format!("missing graph revision {revision} in event stream"),
            )
        })?;
        let durable = self.store.workflow_revision(run_id, revision)?;
        if durable.graph_artifact != reduced.graph_artifact
            || durable.graph != reduced.graph
            || durable.created_at != reduced.created_at
        {
            return Err(Self::replay_error(
                run_id,
                format!("revision {revision} differs from its workflow event"),
            ));
        }
        self.validate_compiled_graph(self.store.run_purpose(run_id)?, &durable.graph)?;
        Ok(durable)
    }

    fn reduce_history(&self, run_id: &RunId) -> RuntimeResult<ReplayedWorkflow> {
        let events = self.replay_events(run_id)?;
        let mut replay = ReplayedWorkflow::default();
        for event in &events {
            self.reduce_event(run_id, &mut replay, event)?;
        }
        if replay.revisions.is_empty() {
            return Err(Self::replay_error(
                run_id,
                "workflow.created is missing from durable event history",
            ));
        }
        Ok(replay)
    }

    fn replay_events(&self, run_id: &RunId) -> RuntimeResult<Vec<StoredEvent>> {
        const PAGE_SIZE: usize = 256;

        let mut events = Vec::new();
        let mut after = 0;
        loop {
            let page = self.store.events_after(run_id, after, PAGE_SIZE)?;
            let Some(last) = page.last() else {
                break;
            };
            if last.cursor <= after {
                return Err(Self::replay_error(
                    run_id,
                    "event cursor did not advance while paging history",
                ));
            }
            after = last.cursor;
            events.extend(page);
        }
        Ok(events)
    }

    fn reduce_event(
        &self,
        run_id: &RunId,
        replay: &mut ReplayedWorkflow,
        event: &StoredEvent,
    ) -> RuntimeResult<()> {
        if event.run_id != *run_id {
            return Err(Self::replay_error(
                run_id,
                format!("event {} belongs to another run", event.cursor),
            ));
        }
        match event.event_type.as_str() {
            "workflow.created" => self.reduce_graph_event(run_id, replay, event, true)?,
            "workflow.patched" => self.reduce_graph_event(run_id, replay, event, false)?,
            "task.started" => {
                let task = Self::replay_task_mut(run_id, replay, event)?;
                let attempt_id = event.attempt_id.clone().ok_or_else(|| {
                    Self::replay_error(run_id, "task.started is missing its attempt id")
                })?;
                if task.status != TaskStatus::Pending || task.active_attempt_id.is_some() {
                    return Err(Self::replay_error(
                        run_id,
                        format!(
                            "task {} started from a non-pending state",
                            task.node.task_id
                        ),
                    ));
                }
                task.status = TaskStatus::Running;
                task.active_attempt_id = Some(attempt_id);
                task.attempt_count += 1;
                task.finished_at = None;
                replay.saw_task_start = true;
            }
            "task.retry_scheduled" | "task.recovered" => {
                let task = Self::replay_task_mut(run_id, replay, event)?;
                Self::assert_active_attempt(run_id, task, event)?;
                task.status = TaskStatus::Pending;
                task.active_attempt_id = None;
                task.finished_at = None;
            }
            "task.succeeded" | "task.failed" | "task.skipped" => {
                let task = Self::replay_task_mut(run_id, replay, event)?;
                Self::assert_active_attempt(run_id, task, event)?;
                task.status = match event.event_type.as_str() {
                    "task.succeeded" => TaskStatus::Succeeded,
                    "task.failed" => TaskStatus::Failed,
                    "task.skipped" => TaskStatus::Skipped,
                    _ => unreachable!("matched terminal task event"),
                };
                task.active_attempt_id = None;
                task.finished_at = Some(event.created_at);
            }
            "task.cancelled" => {
                let task = Self::replay_task_mut(run_id, replay, event)?;
                if event.attempt_id.is_some() {
                    Self::assert_active_attempt(run_id, task, event)?;
                } else if task.status != TaskStatus::Pending {
                    return Err(Self::replay_error(
                        run_id,
                        format!(
                            "queued cancellation for non-pending task {}",
                            task.node.task_id
                        ),
                    ));
                }
                task.status = TaskStatus::Cancelled;
                task.active_attempt_id = None;
                task.finished_at = Some(event.created_at);
            }
            "task.retry_exhausted" | "task.recovery_exhausted" => {
                let task = Self::replay_task_mut(run_id, replay, event)?;
                if !task.status.is_terminal() {
                    return Err(Self::replay_error(
                        run_id,
                        format!("{} precedes a terminal task state", event.event_type),
                    ));
                }
            }
            "run.cancel_requested" => {
                if event.task_id.is_some() || event.attempt_id.is_some() {
                    return Err(Self::replay_error(
                        run_id,
                        "run.cancel_requested unexpectedly names a task attempt",
                    ));
                }
                if replay.cancel_requested {
                    return Err(Self::replay_error(
                        run_id,
                        "run.cancel_requested appears more than once",
                    ));
                }
                replay.cancel_requested = true;
            }
            _ if event.artifact_id.is_some() => {
                self.reduce_artifact_trace_event(run_id, replay, event)?;
            }
            other => {
                return Err(Self::replay_error(
                    run_id,
                    format!("unknown durable event type {other}"),
                ));
            }
        }
        replay.event_cursor = event.cursor;
        Ok(())
    }

    /// Artifact-bearing events are intentionally extensible: task runtimes
    /// emit domain-specific trace events through `write_task_artifact`. Their
    /// authority is the artifact origin, never an event-type allowlist.
    fn reduce_artifact_trace_event(
        &self,
        run_id: &RunId,
        replay: &ReplayedWorkflow,
        event: &StoredEvent,
    ) -> RuntimeResult<()> {
        if event.event_type.trim().is_empty() {
            return Err(Self::replay_error(
                run_id,
                "artifact trace event has an empty event type",
            ));
        }
        let task_id = event.task_id.as_ref().ok_or_else(|| {
            Self::replay_error(
                run_id,
                format!("{} is missing its task id", event.event_type),
            )
        })?;
        let attempt_id = event.attempt_id.as_ref().ok_or_else(|| {
            Self::replay_error(
                run_id,
                format!("{} is missing its attempt id", event.event_type),
            )
        })?;
        let artifact_id = event.artifact_id.as_ref().ok_or_else(|| {
            Self::replay_error(
                run_id,
                format!("{} is missing its artifact id", event.event_type),
            )
        })?;
        let task = replay.tasks.get(task_id).ok_or_else(|| {
            Self::replay_error(
                run_id,
                format!("{} references unknown task {task_id}", event.event_type),
            )
        })?;
        Self::assert_active_attempt(run_id, task, event)?;
        let artifact = self.store.artifact(artifact_id)?;
        artifact.validate()?;
        let origin = artifact.origin.as_ref().ok_or_else(|| {
            Self::replay_error(
                run_id,
                format!("{} artifact has no task origin", event.event_type),
            )
        })?;
        if origin.run_id.as_ref() != Some(run_id)
            || origin.task_id.as_ref() != Some(task_id)
            || origin.attempt_id.as_ref() != Some(attempt_id)
            || origin.contract_hash.as_ref() != task.node.contract_hash.as_ref()
        {
            return Err(Self::replay_error(
                run_id,
                format!(
                    "{} artifact origin does not match task attempt",
                    event.event_type
                ),
            ));
        }
        Ok(())
    }

    fn reduce_graph_event(
        &self,
        run_id: &RunId,
        replay: &mut ReplayedWorkflow,
        event: &StoredEvent,
        initial: bool,
    ) -> RuntimeResult<()> {
        if initial && (event.task_id.is_some() || event.attempt_id.is_some()) {
            return Err(Self::replay_error(
                run_id,
                format!("{} unexpectedly names a task attempt", event.event_type),
            ));
        }
        if !initial && (event.task_id.is_some() || event.attempt_id.is_some()) {
            let task = Self::replay_task_mut(run_id, replay, event)?;
            Self::assert_active_attempt(run_id, task, event)?;
        }
        if initial != replay.revisions.is_empty() {
            return Err(Self::replay_error(
                run_id,
                format!("{} appears out of graph revision order", event.event_type),
            ));
        }
        let artifact_id = event.artifact_id.as_ref().ok_or_else(|| {
            Self::replay_error(
                run_id,
                format!("{} is missing its graph artifact", event.event_type),
            )
        })?;
        let graph_artifact = self.store.artifact(artifact_id)?;
        if graph_artifact.kind != ArtifactKind::WorkflowGraph {
            return Err(Self::replay_error(
                run_id,
                format!(
                    "{} references a non-workflow graph artifact",
                    event.event_type
                ),
            ));
        }
        let graph: WorkflowGraph =
            serde_json::from_slice(&self.store.read_blob(&graph_artifact.blob)?)?;
        graph.validate()?;

        if let Some(previous) = replay.revisions.last() {
            if previous.graph.topology_id != graph.topology_id {
                return Err(Self::replay_error(
                    run_id,
                    "workflow.patched changed the topology id",
                ));
            }
            let next_ids = graph
                .nodes
                .iter()
                .map(|node| node.task_id.clone())
                .collect::<BTreeSet<_>>();
            for node in &previous.graph.nodes {
                if !next_ids.contains(&node.task_id) {
                    return Err(Self::replay_error(
                        run_id,
                        format!("workflow.patched removed task {}", node.task_id),
                    ));
                }
            }
        }

        for node in &graph.nodes {
            match replay.tasks.get_mut(&node.task_id) {
                Some(task) => {
                    if task.status != TaskStatus::Pending && task.node != *node {
                        return Err(Self::replay_error(
                            run_id,
                            format!("workflow.patched rewrote non-pending task {}", node.task_id),
                        ));
                    }
                    task.node = node.clone();
                }
                None => {
                    replay.tasks.insert(
                        node.task_id.clone(),
                        ReplayedTask {
                            node: node.clone(),
                            status: TaskStatus::Pending,
                            active_attempt_id: None,
                            attempt_count: 0,
                            finished_at: None,
                        },
                    );
                }
            }
        }
        replay.revisions.push(ReplayedWorkflowRevision {
            cursor: event.cursor,
            graph_artifact,
            graph,
            created_at: event.created_at,
        });
        Ok(())
    }

    fn replay_task_mut<'a>(
        run_id: &RunId,
        replay: &'a mut ReplayedWorkflow,
        event: &StoredEvent,
    ) -> RuntimeResult<&'a mut ReplayedTask> {
        let task_id = event.task_id.as_ref().ok_or_else(|| {
            Self::replay_error(
                run_id,
                format!("{} is missing its task id", event.event_type),
            )
        })?;
        replay.tasks.get_mut(task_id).ok_or_else(|| {
            Self::replay_error(
                run_id,
                format!("{} references unknown task {task_id}", event.event_type),
            )
        })
    }

    fn assert_active_attempt(
        run_id: &RunId,
        task: &ReplayedTask,
        event: &StoredEvent,
    ) -> RuntimeResult<()> {
        let attempt_id = event.attempt_id.as_ref().ok_or_else(|| {
            Self::replay_error(
                run_id,
                format!("{} is missing its attempt id", event.event_type),
            )
        })?;
        if task.status != TaskStatus::Running || task.active_attempt_id.as_ref() != Some(attempt_id)
        {
            return Err(Self::replay_error(
                run_id,
                format!(
                    "{} does not match task {} active attempt",
                    event.event_type, task.node.task_id
                ),
            ));
        }
        Ok(())
    }

    fn validate_replay_revisions(
        &self,
        run_id: &RunId,
        replay: &ReplayedWorkflow,
    ) -> RuntimeResult<()> {
        let purpose = self.store.run_purpose(run_id)?;
        for (index, reduced) in replay.revisions.iter().enumerate() {
            let revision = u64::try_from(index).map_err(|_| {
                Self::replay_error(run_id, "workflow revision index does not fit u64")
            })?;
            let durable = self.store.workflow_revision(run_id, revision)?;
            if durable.graph_artifact != reduced.graph_artifact
                || durable.graph != reduced.graph
                || durable.created_at != reduced.created_at
            {
                return Err(Self::replay_error(
                    run_id,
                    format!("revision {revision} differs from event history"),
                ));
            }
            self.validate_compiled_graph(purpose, &reduced.graph)?;
        }
        Ok(())
    }

    fn validate_replay_snapshot(
        &self,
        run_id: &RunId,
        replay: &ReplayedWorkflow,
        snapshot: &WorkflowSnapshot,
    ) -> RuntimeResult<()> {
        let latest = replay.revisions.last().ok_or_else(|| {
            Self::replay_error(run_id, "workflow snapshot has no reduced graph revision")
        })?;
        let expected_revision = u64::try_from(replay.revisions.len() - 1)
            .map_err(|_| Self::replay_error(run_id, "workflow revision count does not fit u64"))?;
        if snapshot.revision.revision != expected_revision
            || snapshot.revision.graph_artifact != latest.graph_artifact
            || snapshot.revision.graph != latest.graph
            || snapshot.revision.created_at != latest.created_at
        {
            return Err(Self::replay_error(
                run_id,
                "latest workflow snapshot differs from reduced graph history",
            ));
        }
        if snapshot.event_cursor != replay.event_cursor {
            return Err(Self::replay_error(
                run_id,
                "workflow snapshot event cursor differs from reduced history",
            ));
        }
        if snapshot.cancel_requested != replay.cancel_requested {
            return Err(Self::replay_error(
                run_id,
                "workflow cancellation marker differs from reduced history",
            ));
        }

        let stored_tasks = snapshot
            .tasks
            .iter()
            .map(|task| (task.node.task_id.clone(), task))
            .collect::<BTreeMap<_, _>>();
        if stored_tasks.len() != replay.tasks.len() {
            return Err(Self::replay_error(
                run_id,
                "workflow task count differs from reduced graph history",
            ));
        }
        for (task_id, reduced) in &replay.tasks {
            let stored = stored_tasks.get(task_id).ok_or_else(|| {
                Self::replay_error(run_id, format!("snapshot is missing task {task_id}"))
            })?;
            let stored_attempt = stored
                .active_attempt
                .as_ref()
                .map(|attempt| attempt.permit.attempt_id.clone());
            if stored.node != reduced.node
                || stored.status != reduced.status
                || stored.attempt_count != reduced.attempt_count
                || stored_attempt != reduced.active_attempt_id
                || stored.finished_at != reduced.finished_at
            {
                return Err(Self::replay_error(
                    run_id,
                    format!("task {task_id} differs from reduced event/task history"),
                ));
            }
        }

        let expected_status = if replay
            .tasks
            .values()
            .any(|task| matches!(task.status, TaskStatus::Pending | TaskStatus::Running))
        {
            if replay.saw_task_start {
                WorkflowStatus::Running
            } else {
                WorkflowStatus::Queued
            }
        } else if replay
            .tasks
            .values()
            .any(|task| task.status == TaskStatus::Failed)
        {
            WorkflowStatus::Failed
        } else if replay
            .tasks
            .values()
            .all(|task| task.status == TaskStatus::Cancelled)
        {
            WorkflowStatus::Cancelled
        } else {
            WorkflowStatus::Completed
        };
        if snapshot.status != expected_status
            || (expected_status == WorkflowStatus::Running && snapshot.finished_at.is_some())
            || (expected_status == WorkflowStatus::Queued && snapshot.finished_at.is_some())
            || (matches!(
                expected_status,
                WorkflowStatus::Completed | WorkflowStatus::Failed | WorkflowStatus::Cancelled
            ) && snapshot.finished_at.is_none())
        {
            return Err(Self::replay_error(
                run_id,
                "workflow status differs from reduced task history",
            ));
        }
        Ok(())
    }

    fn replay_error(run_id: &RunId, reason: impl Into<String>) -> RuntimeError {
        RuntimeError::ReplayDiverged {
            run_id: run_id.clone(),
            reason: reason.into(),
        }
    }

    /// Applies a Planner proposal to a bootstrap graph. It adds all research nodes
    /// and then one immutable terminal chain; a later Planner cannot patch gates.
    pub fn apply_planner_output(
        &self,
        planner: &ClaimedAttempt,
        previous_graph_artifact: &Artifact,
        previous_graph: &WorkflowGraph,
        planner_output: &Artifact,
        now: DateTime<Utc>,
    ) -> RuntimeResult<Artifact> {
        self.assert_planner_attempt(planner)?;
        if planner_output.kind != ArtifactKind::WorkflowProposalDraft {
            return Err(RuntimeError::Store(
                StoreError::InvalidWorkflowProposalArtifact,
            ));
        }
        let draft: WorkflowProposalDraft =
            serde_json::from_slice(&self.store.read_blob(&planner_output.blob)?)?;
        draft.validate(&self.catalogue.recipes)?;
        let (proposal, evidence_needs, proposal_artifact) =
            self.materialize_planner_output(planner, planner_output, draft, now)?;
        let run_id = &planner.run_id;
        let purpose = self.store.run_purpose(run_id)?;
        if purpose == RunPurpose::Paper {
            return Err(RuntimeError::FrozenPaperWorkflow(run_id.clone()));
        }
        previous_graph.validate()?;
        self.validate_compiled_graph(purpose, previous_graph)?;
        if previous_graph.topology_id != proposal.topology_id {
            return Err(RuntimeError::Domain(DomainError::EmptyField {
                field: "workflow_proposal.topology_id",
            }));
        }
        let mut nodes = previous_graph.nodes.clone();
        let evidence_index = nodes
            .iter()
            .position(|node| node.recipe_id == self.catalogue.terminals.evidence_gate)
            .ok_or_else(|| {
                RuntimeError::MissingEvidenceGate(self.catalogue.terminals.evidence_gate.clone())
            })?;
        let evidence_task_id = nodes[evidence_index].task_id.clone();
        let mut added_nodes = self.lower_research_nodes(&proposal)?;
        self.attach_evidence_gate(&mut added_nodes, &evidence_task_id);
        nodes.extend(added_nodes.iter().cloned());
        let decision_index = nodes
            .iter()
            .position(|node| node.recipe_id == self.catalogue.terminals.decision_gate)
            .ok_or_else(|| {
                RuntimeError::MissingTerminalGate(self.catalogue.terminals.decision_gate.clone())
            })?;
        let mut decision = nodes[decision_index].clone();
        let research = nodes
            .iter()
            .filter(|node| !self.is_terminal_node(node) && node.recipe_id != self.catalogue.planner)
            .cloned()
            .collect::<Vec<_>>();
        let mut evidence = nodes[evidence_index].clone();
        evidence.input_artifacts = self.aggregate_evidence_needs(&research)?;
        nodes[evidence_index] = evidence.clone();
        decision.dependencies = if research.is_empty() {
            vec![evidence_task_id]
        } else {
            leaf_ids(&research)
        };
        nodes[decision_index] = decision.clone();
        let graph = WorkflowGraph {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: previous_graph.topology_id.clone(),
            nodes,
        };
        graph.validate()?;
        self.validate_compiled_graph(purpose, &graph)?;
        let next_artifact = self.graph_artifact(
            &graph,
            vec![
                ArtifactRef {
                    artifact_id: previous_graph_artifact.artifact_id.clone(),
                    kind: ArtifactKind::WorkflowGraph,
                },
                ArtifactRef {
                    artifact_id: proposal_artifact.artifact_id.clone(),
                    kind: ArtifactKind::WorkflowProposal,
                },
            ],
            now,
        )?;
        self.store.commit_workflow_patch(&WorkflowPatchCommit {
            permit: planner.permit.clone(),
            previous_graph_artifact_id: previous_graph_artifact.artifact_id.clone(),
            planner_output: planner_output.clone(),
            evidence_needs,
            proposal: proposal_artifact.clone(),
            next_graph: next_artifact.clone(),
            added_nodes,
            updated_nodes: vec![evidence, decision],
            completed_at: now,
        })?;
        Ok(next_artifact)
    }

    fn materialize_planner_output(
        &self,
        planner: &ClaimedAttempt,
        planner_output: &Artifact,
        mut draft: WorkflowProposalDraft,
        now: DateTime<Utc>,
    ) -> RuntimeResult<(WorkflowProposal, Vec<Artifact>, Artifact)> {
        self.insert_structured_critic(&mut draft)?;
        let planner_output_ref = ArtifactRef {
            artifact_id: planner_output.artifact_id.clone(),
            kind: ArtifactKind::WorkflowProposalDraft,
        };
        let origin = ArtifactOrigin {
            run_id: Some(planner.run_id.clone()),
            task_id: Some(planner.node.task_id.clone()),
            attempt_id: Some(planner.permit.attempt_id.clone()),
            contract_hash: planner.permit.contract_hash.clone(),
        };
        let provenance = ArtifactProvenance {
            source_family: "akzio.workflow.planner".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: planner.permit.contract_hash.clone(),
        };

        let mut need_artifacts = BTreeMap::<EvidenceNeed, Artifact>::new();
        let mut tasks = BTreeMap::new();
        for (alias, task) in draft.tasks {
            let mut declared_needs = task.evidence_needs;
            declared_needs.extend(
                task.research_intents
                    .into_iter()
                    .map(|intent| intent.evidence_need())
                    .collect::<Result<Vec<_>, _>>()?,
            );
            let mut evidence_needs = Vec::with_capacity(declared_needs.len());
            for need in declared_needs {
                let artifact = if let Some(artifact) = need_artifacts.get(&need) {
                    artifact.clone()
                } else {
                    let artifact = Artifact::new(
                        ArtifactKind::EvidenceNeed,
                        self.store.put_json(&need)?,
                        "runtime.planner.evidence_need",
                        ArtifactLifecycle::RunScoped,
                        provenance.clone(),
                        Some(origin.clone()),
                        vec![planner_output_ref.clone()],
                        now,
                    )?;
                    need_artifacts.insert(need.clone(), artifact.clone());
                    artifact
                };
                evidence_needs.push(ArtifactRef {
                    artifact_id: artifact.artifact_id,
                    kind: ArtifactKind::EvidenceNeed,
                });
            }
            tasks.insert(
                alias,
                WorkflowProposalTask {
                    recipe_id: task.recipe_id,
                    objective: task.objective,
                    depends_on: task.depends_on,
                    priority: task.priority,
                    evidence_needs,
                },
            );
        }

        let proposal = WorkflowProposal {
            schema_version: draft.schema_version,
            topology_id: draft.topology_id,
            tasks,
            stop_reason: draft.stop_reason,
        };
        proposal.validate(&self.catalogue.recipes)?;
        self.validate_proposal_limits(&proposal)?;

        let evidence_needs = need_artifacts.into_values().collect::<Vec<_>>();
        let proposal_sources = std::iter::once(planner_output_ref)
            .chain(evidence_needs.iter().map(|artifact| ArtifactRef {
                artifact_id: artifact.artifact_id.clone(),
                kind: ArtifactKind::EvidenceNeed,
            }))
            .collect();
        let proposal_artifact = Artifact::new(
            ArtifactKind::WorkflowProposal,
            self.store.put_json(&proposal)?,
            "runtime.planner.proposal",
            ArtifactLifecycle::RunScoped,
            provenance,
            Some(origin),
            proposal_sources,
            now,
        )?;
        Ok((proposal, evidence_needs, proposal_artifact))
    }

    fn insert_structured_critic(&self, draft: &mut WorkflowProposalDraft) -> RuntimeResult<()> {
        if draft.topology_id != STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID {
            return Ok(());
        }
        let analyst_aliases = draft
            .tasks
            .iter()
            .filter_map(|(alias, task)| {
                (task.recipe_id.as_str() == ANALYST_RECIPE_ID).then_some(alias.clone())
            })
            .collect::<Vec<_>>();
        if analyst_aliases.is_empty() {
            return Ok(());
        }
        if draft
            .tasks
            .values()
            .any(|task| task.recipe_id.as_str() == CRITIC_RECIPE_ID)
        {
            return Err(RuntimeError::PlannerSchedulesCritic);
        }
        if draft
            .tasks
            .values()
            .filter(|task| task.recipe_id.as_str() == SYNTHESIZER_RECIPE_ID)
            .count()
            > 1
        {
            return Err(RuntimeError::PlannerSchedulesMultipleSynthesizers);
        }

        let critic_recipe_id = TaskRecipeId::new(CRITIC_RECIPE_ID)?;
        let critic_recipe = self.catalogue.recipe(&critic_recipe_id)?;
        let mut suffix = 0;
        let critic_alias = loop {
            let alias = if suffix == 0 {
                STRUCTURED_CRITIC_ALIAS_PREFIX.to_owned()
            } else {
                format!("{STRUCTURED_CRITIC_ALIAS_PREFIX}_{suffix}")
            };
            if !draft.tasks.contains_key(&alias) {
                break alias;
            }
            suffix += 1;
        };

        draft.tasks.insert(
            critic_alias.clone(),
            akzio_domain::WorkflowProposalDraftTask {
                recipe_id: critic_recipe_id,
                objective: "Perform one evidence-bound critique of the analyst claims when Rust detects material uncertainty or directional conflict.".to_owned(),
                depends_on: analyst_aliases.clone(),
            priority: critic_recipe.priority_ceiling,
            evidence_needs: Vec::new(),
            research_intents: Vec::new(),
        },
        );
        for task in draft
            .tasks
            .values_mut()
            .filter(|task| task.recipe_id.as_str() == SYNTHESIZER_RECIPE_ID)
        {
            task.depends_on
                .retain(|dependency| !analyst_aliases.contains(dependency));
            task.depends_on.push(critic_alias.clone());
            task.depends_on.sort();
            task.depends_on.dedup();
        }
        draft.validate(&self.catalogue.recipes)?;
        Ok(())
    }

    fn assert_planner_attempt(&self, planner: &ClaimedAttempt) -> RuntimeResult<()> {
        let recipe = self.catalogue.recipe(&self.catalogue.planner)?;
        if planner.node.recipe_id != self.catalogue.planner
            || planner.node.contract_hash != recipe.contract_hash
            || planner.permit.contract_hash != recipe.contract_hash
        {
            return Err(RuntimeError::PlannerPermitRequired);
        }
        Ok(())
    }

    fn validate_compiled_graph(
        &self,
        purpose: RunPurpose,
        graph: &WorkflowGraph,
    ) -> RuntimeResult<()> {
        self.validate_evidence_gate(graph)?;
        let mut terminals = BTreeMap::<TaskRecipeId, &WorkflowNode>::new();
        let mut research = Vec::new();
        for node in &graph.nodes {
            let recipe = self.catalogue.recipe(&node.recipe_id)?;
            if node.contract_hash != recipe.contract_hash
                || node.budget != recipe.budget
                || node.retry != recipe.retry
                || node.on_failure != recipe.on_failure
                || node.priority > recipe.priority_ceiling
            {
                return Err(RuntimeError::NodeRecipeMismatch(node.task_id.clone()));
            }
            if matches!(
                recipe.task_class,
                RuntimeTaskClass::DecisionGate
                    | RuntimeTaskClass::ExecutionGate
                    | RuntimeTaskClass::PaperCommit
                    | RuntimeTaskClass::Reconcile
                    | RuntimeTaskClass::Evaluate
            ) {
                if !self.catalogue.is_terminal(&node.recipe_id)
                    || terminals.insert(node.recipe_id.clone(), node).is_some()
                {
                    return Err(RuntimeError::UnexpectedTerminalGate(node.recipe_id.clone()));
                }
            } else {
                research.push(node.clone());
            }
        }
        if research.is_empty() {
            return Err(RuntimeError::WorkflowNodeLimit);
        }

        let decision = required_terminal(&terminals, &self.catalogue.terminals.decision_gate)?;
        let execution = required_terminal(&terminals, &self.catalogue.terminals.execution_gate)?;
        let reconcile = required_terminal(&terminals, &self.catalogue.terminals.reconcile)?;
        let evaluate = required_terminal(&terminals, &self.catalogue.terminals.evaluate)?;
        let paper = terminals
            .get(&self.catalogue.terminals.paper_commit)
            .copied();
        if purpose == RunPurpose::Paper && paper.is_none() {
            return Err(RuntimeError::MissingTerminalGate(
                self.catalogue.terminals.paper_commit.clone(),
            ));
        }
        if purpose != RunPurpose::Paper && paper.is_some() {
            return Err(RuntimeError::UnexpectedTerminalGate(
                self.catalogue.terminals.paper_commit.clone(),
            ));
        }

        let terminal_ids = terminals
            .values()
            .map(|node| node.task_id.clone())
            .collect::<BTreeSet<_>>();
        if research.iter().any(|node| {
            node.dependencies
                .iter()
                .any(|dependency| terminal_ids.contains(dependency))
        }) {
            let offender = research
                .iter()
                .find(|node| {
                    node.dependencies
                        .iter()
                        .any(|dependency| terminal_ids.contains(dependency))
                })
                .expect("research dependency predicate found an offender");
            return Err(RuntimeError::ResearchDependsOnTerminal(
                offender.task_id.clone(),
            ));
        }

        let expected_decision_dependencies =
            leaf_ids(&research).into_iter().collect::<BTreeSet<_>>();
        if decision
            .dependencies
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            != expected_decision_dependencies
        {
            return Err(RuntimeError::InvalidTerminalDependencies(
                self.catalogue.terminals.decision_gate.clone(),
            ));
        }
        if execution.dependencies != vec![decision.task_id.clone()] {
            return Err(RuntimeError::InvalidTerminalDependencies(
                self.catalogue.terminals.execution_gate.clone(),
            ));
        }
        let predecessor = if let Some(paper) = paper {
            if paper.dependencies != vec![execution.task_id.clone()] {
                return Err(RuntimeError::InvalidTerminalDependencies(
                    self.catalogue.terminals.paper_commit.clone(),
                ));
            }
            paper.task_id.clone()
        } else {
            execution.task_id.clone()
        };
        if reconcile.dependencies != vec![predecessor] {
            return Err(RuntimeError::InvalidTerminalDependencies(
                self.catalogue.terminals.reconcile.clone(),
            ));
        }
        if evaluate.dependencies != vec![reconcile.task_id.clone()] {
            return Err(RuntimeError::InvalidTerminalDependencies(
                self.catalogue.terminals.evaluate.clone(),
            ));
        }
        Ok(())
    }

    fn validate_evidence_gate(&self, graph: &WorkflowGraph) -> RuntimeResult<()> {
        let evidence_nodes = graph
            .nodes
            .iter()
            .filter(|node| node.recipe_id == self.catalogue.terminals.evidence_gate)
            .collect::<Vec<_>>();
        let Some(evidence) = evidence_nodes.first().copied() else {
            return Err(RuntimeError::MissingEvidenceGate(
                self.catalogue.terminals.evidence_gate.clone(),
            ));
        };
        if evidence_nodes.len() != 1 {
            return Err(RuntimeError::UnexpectedTerminalGate(
                self.catalogue.terminals.evidence_gate.clone(),
            ));
        }

        let planner_nodes = graph
            .nodes
            .iter()
            .filter(|node| node.recipe_id == self.catalogue.planner)
            .collect::<Vec<_>>();
        if planner_nodes.len() > 1 {
            return Err(RuntimeError::DuplicatePlannerTask);
        }
        let expected_evidence_dependencies = planner_nodes
            .first()
            .map(|planner| vec![planner.task_id.clone()])
            .unwrap_or_default();
        if evidence.dependencies != expected_evidence_dependencies {
            return Err(RuntimeError::InvalidTerminalDependencies(
                self.catalogue.terminals.evidence_gate.clone(),
            ));
        }

        let research = graph
            .nodes
            .iter()
            .filter(|node| node.recipe_id != self.catalogue.planner && !self.is_terminal_node(node))
            .cloned()
            .collect::<Vec<_>>();
        let research_ids = research
            .iter()
            .map(|node| node.task_id.clone())
            .collect::<BTreeSet<_>>();
        if evidence.input_artifacts != self.aggregate_evidence_needs(&research)? {
            return Err(RuntimeError::InvalidEvidencePlan);
        }
        for node in &research {
            let has_research_parent = node
                .dependencies
                .iter()
                .any(|dependency| research_ids.contains(dependency));
            if !has_research_parent && node.dependencies != vec![evidence.task_id.clone()] {
                return Err(RuntimeError::ResearchBypassesEvidence(node.task_id.clone()));
            }
        }
        Ok(())
    }

    fn lower_research_nodes(
        &self,
        proposal: &WorkflowProposal,
    ) -> RuntimeResult<Vec<WorkflowNode>> {
        proposal.validate(&self.catalogue.recipes)?;
        self.validate_proposal_limits(proposal)?;
        if proposal.tasks.len() > self.catalogue.max_nodes {
            return Err(RuntimeError::WorkflowNodeLimit);
        }
        let ids = proposal
            .tasks
            .keys()
            .map(|alias| (alias.clone(), akzio_domain::TaskId::new()))
            .collect::<BTreeMap<_, _>>();
        proposal
            .tasks
            .iter()
            .map(|(alias, task)| {
                let recipe = self.catalogue.recipe(&task.recipe_id)?;
                if self.catalogue.is_rust_gate(&task.recipe_id)
                    || matches!(
                        recipe.task_class,
                        RuntimeTaskClass::DecisionGate
                            | RuntimeTaskClass::ExecutionGate
                            | RuntimeTaskClass::PaperCommit
                            | RuntimeTaskClass::Reconcile
                            | RuntimeTaskClass::Evaluate
                    )
                {
                    return Err(RuntimeError::TerminalRecipeInProposal(
                        task.recipe_id.clone(),
                    ));
                }
                let agent_parents = task
                    .depends_on
                    .iter()
                    .filter(|dependency| {
                        proposal
                            .tasks
                            .get(*dependency)
                            .and_then(|parent| self.catalogue.recipe(&parent.recipe_id).ok())
                            .is_some_and(|parent| parent.task_class == RuntimeTaskClass::Agent)
                    })
                    .collect::<Vec<_>>();
                if agent_parents.len() > 1 {
                    return Err(RuntimeError::MultipleAgentParents {
                        task: alias.clone(),
                    });
                }
                Ok(WorkflowNode {
                    task_id: ids[alias].clone(),
                    recipe_id: recipe.recipe_id.clone(),
                    contract_hash: recipe.contract_hash.clone(),
                    objective: task.objective.clone(),
                    dependencies: task
                        .depends_on
                        .iter()
                        .map(|dependency| ids[dependency].clone())
                        .collect(),
                    input_artifacts: task.evidence_needs.clone(),
                    priority: task.priority,
                    budget: recipe.budget.clone(),
                    retry: recipe.retry.clone(),
                    on_failure: recipe.on_failure,
                    parent_task_id: agent_parents
                        .first()
                        .map(|dependency| ids[*dependency].clone()),
                })
            })
            .collect()
    }

    fn validate_proposal_limits(&self, proposal: &WorkflowProposal) -> RuntimeResult<()> {
        let mut children = BTreeMap::<String, Vec<String>>::new();
        for (alias, task) in &proposal.tasks {
            for dependency in &task.depends_on {
                let parent = proposal
                    .tasks
                    .get(dependency)
                    .expect("proposal validation checked dependencies");
                let parent_recipe = self.catalogue.recipe(&parent.recipe_id)?;
                let descendants = children.entry(dependency.clone()).or_default();
                descendants.push(alias.clone());
                if descendants.len() > usize::from(parent_recipe.max_children) {
                    return Err(RuntimeError::WorkflowFanoutLimit {
                        task: dependency.clone(),
                        recipe: parent.recipe_id.clone(),
                    });
                }
            }
        }
        for (root, task) in &proposal.tasks {
            let recipe = self.catalogue.recipe(&task.recipe_id)?;
            let mut stack = vec![(root.clone(), 0_u16)];
            while let Some((alias, depth)) = stack.pop() {
                if depth > recipe.max_depth {
                    return Err(RuntimeError::WorkflowDepthLimit {
                        task: alias,
                        recipe: recipe.recipe_id.clone(),
                    });
                }
                if let Some(descendants) = children.get(&alias) {
                    stack.extend(descendants.iter().cloned().map(|child| (child, depth + 1)));
                }
            }
        }
        Ok(())
    }

    fn with_terminal_gates(
        &self,
        purpose: RunPurpose,
        topology_id: String,
        mut nodes: Vec<WorkflowNode>,
    ) -> RuntimeResult<WorkflowGraph> {
        let planners = nodes
            .iter()
            .filter(|node| node.recipe_id == self.catalogue.planner)
            .collect::<Vec<_>>();
        if planners.len() > 1 {
            return Err(RuntimeError::DuplicatePlannerTask);
        }

        let evidence_dependencies = planners
            .first()
            .map(|planner| vec![planner.task_id.clone()])
            .unwrap_or_default();
        let evidence_needs = self.aggregate_evidence_needs(&nodes)?;
        let mut evidence = self.gate_node(
            &self.catalogue.terminals.evidence_gate,
            evidence_dependencies,
        )?;
        evidence.input_artifacts = evidence_needs;
        let evidence_task_id = evidence.task_id.clone();
        for node in &mut nodes {
            if node.recipe_id != self.catalogue.planner && node.dependencies.is_empty() {
                node.dependencies.push(evidence_task_id.clone());
            }
        }

        let research = nodes
            .iter()
            .filter(|node| node.recipe_id != self.catalogue.planner)
            .cloned()
            .collect::<Vec<_>>();
        let decision_dependencies = if research.is_empty() {
            vec![evidence_task_id]
        } else {
            leaf_ids(&research)
        };
        let decision = self.gate_node(
            &self.catalogue.terminals.decision_gate,
            decision_dependencies,
        )?;
        let execution = self.gate_node(
            &self.catalogue.terminals.execution_gate,
            vec![decision.task_id.clone()],
        )?;
        let predecessor = if purpose == RunPurpose::Paper {
            let paper = self.gate_node(
                &self.catalogue.terminals.paper_commit,
                vec![execution.task_id.clone()],
            )?;
            let task_id = paper.task_id.clone();
            nodes.push(paper);
            task_id
        } else {
            execution.task_id.clone()
        };
        let reconcile = self.gate_node(&self.catalogue.terminals.reconcile, vec![predecessor])?;
        let evaluate = self.gate_node(
            &self.catalogue.terminals.evaluate,
            vec![reconcile.task_id.clone()],
        )?;
        nodes.push(evidence);
        nodes.extend([decision, execution, reconcile, evaluate]);
        if nodes.len() > self.catalogue.max_nodes {
            return Err(RuntimeError::WorkflowNodeLimit);
        }
        let graph = WorkflowGraph {
            schema_version: V2_SCHEMA_VERSION,
            topology_id,
            nodes,
        };
        graph.validate()?;
        Ok(graph)
    }

    fn attach_evidence_gate(
        &self,
        nodes: &mut [WorkflowNode],
        evidence_task_id: &akzio_domain::TaskId,
    ) {
        for node in nodes {
            if node.recipe_id != self.catalogue.planner && node.dependencies.is_empty() {
                node.dependencies.push(evidence_task_id.clone());
            }
        }
    }

    fn aggregate_evidence_needs(&self, nodes: &[WorkflowNode]) -> RuntimeResult<Vec<ArtifactRef>> {
        let mut needs = BTreeMap::new();
        for node in nodes {
            let mut task_needs = BTreeSet::new();
            for reference in &node.input_artifacts {
                if reference.kind != ArtifactKind::EvidenceNeed
                    || !task_needs.insert(reference.artifact_id.clone())
                {
                    return Err(RuntimeError::InvalidEvidenceNeed(node.task_id.clone()));
                }
                needs
                    .entry(reference.artifact_id.clone())
                    .or_insert_with(|| reference.clone());
            }
        }
        Ok(needs.into_values().collect())
    }

    fn gate_node(
        &self,
        recipe_id: &TaskRecipeId,
        dependencies: Vec<akzio_domain::TaskId>,
    ) -> RuntimeResult<WorkflowNode> {
        let recipe = self.catalogue.recipe(recipe_id)?;
        Ok(WorkflowNode {
            task_id: akzio_domain::TaskId::new(),
            recipe_id: recipe.recipe_id.clone(),
            contract_hash: None,
            objective: format!("Rust-owned {}", recipe.purpose.as_str()),
            dependencies,
            input_artifacts: vec![],
            priority: recipe.priority_ceiling,
            budget: recipe.budget.clone(),
            retry: recipe.retry.clone(),
            on_failure: recipe.on_failure,
            parent_task_id: None,
        })
    }

    fn graph_artifact(
        &self,
        graph: &WorkflowGraph,
        source_refs: Vec<ArtifactRef>,
        now: DateTime<Utc>,
    ) -> RuntimeResult<Artifact> {
        Ok(Artifact::new(
            ArtifactKind::WorkflowGraph,
            self.store.put_json(graph)?,
            "runtime.workflow",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.runtime".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            None,
            source_refs,
            now,
        )?)
    }

    fn is_terminal_node(&self, node: &WorkflowNode) -> bool {
        self.catalogue.recipe(&node.recipe_id).is_ok_and(|recipe| {
            matches!(
                recipe.task_class,
                RuntimeTaskClass::Evidence
                    | RuntimeTaskClass::DecisionGate
                    | RuntimeTaskClass::ExecutionGate
                    | RuntimeTaskClass::PaperCommit
                    | RuntimeTaskClass::Reconcile
                    | RuntimeTaskClass::Evaluate
            )
        })
    }
}

/// A handler may only report business completion. Claiming, heartbeats,
/// timeout, cancellation, retry eligibility, and durable terminal commits are
/// owned by this runtime and its `V2Store` transaction surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryCause {
    Transport,
    RateLimited,
    InvalidOutput,
    Timeout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskCompletion {
    Succeeded(Vec<Artifact>),
    /// A Rust gate can succeed after forwarding already durable lineage without
    /// manufacturing a duplicate artifact. The task transition remains in the
    /// Store event log; the upstream artifact remains the source of truth.
    NoOutput,
    Committed,
    Failed,
    Skipped,
    Cancelled,
    Retry(RetryCause),
}

#[derive(Debug, Clone)]
pub struct TaskRuntime {
    store: V2Store,
    lease_duration: Duration,
}

impl TaskRuntime {
    pub fn new(store: V2Store) -> Self {
        Self {
            store,
            lease_duration: Duration::seconds(30),
        }
    }

    pub fn with_lease_duration(mut self, lease_duration: Duration) -> RuntimeResult<Self> {
        if lease_duration <= Duration::zero() {
            return Err(RuntimeError::InvalidTaskLeaseDuration);
        }
        self.lease_duration = lease_duration;
        Ok(self)
    }

    pub fn store(&self) -> &V2Store {
        &self.store
    }

    /// Request cooperative cancellation through the Store-owned task state
    /// machine. A worker observes the durable flag between heartbeats.
    pub fn request_cancel(
        &self,
        run_id: &RunId,
        reason: &str,
        now: DateTime<Utc>,
    ) -> RuntimeResult<bool> {
        Ok(self.store.request_run_cancel(run_id, reason, now)?)
    }

    pub async fn run_one<F, Fut>(&self, worker_id: &str, handle: F) -> RuntimeResult<bool>
    where
        F: FnOnce(ClaimedAttempt) -> Fut,
        Fut: Future<Output = TaskCompletion>,
    {
        let now = Utc::now();
        self.store.recover_expired_tasks(now)?;
        let Some(task) = self
            .store
            .claim_next_task(worker_id, now, self.lease_duration)?
        else {
            return Ok(false);
        };
        if self.store.run_cancel_requested(&task.run_id)? {
            self.store
                .finish_task(&task.permit, TaskStatus::Cancelled, Utc::now())?;
            return Ok(true);
        }

        let heartbeat_millis = u64::try_from(self.lease_duration.num_milliseconds())
            .map_err(|_| RuntimeError::InvalidTaskLeaseDuration)?
            .saturating_div(3)
            .max(1);
        let mut heartbeat = tokio::time::interval(StdDuration::from_millis(heartbeat_millis));
        heartbeat.tick().await;
        let mut handler = Box::pin(handle(task.clone()));
        let timeout = tokio::time::sleep(StdDuration::from_secs(u64::from(
            task.node.budget.max_wall_time_secs,
        )));
        tokio::pin!(timeout);
        let completion = loop {
            tokio::select! {
                result = &mut handler => break result,
                _ = heartbeat.tick() => {
                    if self.store.run_cancel_requested(&task.run_id)? {
                        break TaskCompletion::Cancelled;
                    }
                    self.store.heartbeat_task(
                        &task.permit,
                        Utc::now() + self.lease_duration,
                    )?;
                }
                _ = &mut timeout => break TaskCompletion::Retry(RetryCause::Timeout),
            }
        };
        self.finish(&task, completion, Utc::now())?;
        Ok(true)
    }

    fn finish(
        &self,
        task: &ClaimedAttempt,
        completion: TaskCompletion,
        now: DateTime<Utc>,
    ) -> RuntimeResult<()> {
        match completion {
            TaskCompletion::Succeeded(artifacts) => {
                self.store
                    .commit_attempt(&task.permit, &artifacts, TaskStatus::Succeeded, now)?;
            }
            TaskCompletion::NoOutput => {
                self.store
                    .finish_task(&task.permit, TaskStatus::Succeeded, now)?;
            }
            TaskCompletion::Committed => {
                self.store
                    .verify_attempt_terminal(&task.permit, TaskStatus::Succeeded)?;
            }
            TaskCompletion::Failed => {
                self.store
                    .finish_task(&task.permit, TaskStatus::Failed, now)?;
            }
            TaskCompletion::Skipped => {
                self.store
                    .finish_task(&task.permit, TaskStatus::Skipped, now)?;
            }
            TaskCompletion::Cancelled => {
                self.store
                    .finish_task(&task.permit, TaskStatus::Cancelled, now)?;
            }
            TaskCompletion::Retry(cause) => {
                if self.retry_allowed(task, cause) {
                    let retry_at = self.retry_at(task, now)?;
                    match self.store.retry_task(&task.permit, retry_at, now)? {
                        RetryTaskResult::Requeued | RetryTaskResult::Terminal(_) => {}
                    }
                } else {
                    self.store
                        .finish_task(&task.permit, TaskStatus::Failed, now)?;
                }
            }
        }
        Ok(())
    }

    fn retry_allowed(&self, task: &ClaimedAttempt, cause: RetryCause) -> bool {
        match cause {
            RetryCause::Transport | RetryCause::Timeout => task.node.retry.retry_transport,
            RetryCause::RateLimited => task.node.retry.retry_rate_limited,
            RetryCause::InvalidOutput => task.node.retry.retry_invalid_output,
        }
    }

    fn retry_at(&self, task: &ClaimedAttempt, now: DateTime<Utc>) -> RuntimeResult<DateTime<Utc>> {
        let milliseconds = i64::try_from(task.node.retry.initial_backoff_ms)
            .map_err(|_| RuntimeError::InvalidRetryBackoff)?;
        now.checked_add_signed(Duration::milliseconds(milliseconds))
            .ok_or(RuntimeError::InvalidRetryBackoff)
    }
}

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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use akzio_domain::{
        ArtifactOrigin, ClaimStance, ContentHash, ContractPurpose, DecisionHorizon, EvidenceGap,
        EvidenceNeed, FailureDisposition, ResearchClaim, RetryPolicy, TaskBudget,
        WorkflowProposalDraft, WorkflowProposalDraftTask, WorkflowProposalTask,
    };
    use tempfile::tempdir;

    use super::*;

    fn budget() -> TaskBudget {
        TaskBudget {
            max_input_tokens: 100,
            max_output_tokens: 50,
            max_wall_time_secs: 30,
            max_tool_calls: 2,
        }
    }

    fn retry() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 2,
            initial_backoff_ms: 0,
            retry_transport: true,
            retry_rate_limited: true,
            retry_invalid_output: false,
        }
    }

    fn claim(
        stance: ClaimStance,
        materiality_ppm: u32,
        confidence_ppm: u32,
        has_gap: bool,
    ) -> ResearchClaim {
        ResearchClaim {
            schema_version: V2_SCHEMA_VERSION,
            topic: "TQQQ regime".to_owned(),
            statement: "fixture claim".to_owned(),
            horizon: DecisionHorizon::T5,
            stance,
            materiality_ppm,
            confidence_ppm,
            grounds: vec![],
            evidence_gaps: has_gap
                .then(|| EvidenceGap {
                    topic: "fixture gap".to_owned(),
                    rationale: "fixture uncertainty".to_owned(),
                })
                .into_iter()
                .collect(),
        }
    }

    fn recipe(id: &str, class: RuntimeTaskClass, agent: bool) -> TaskRecipe {
        TaskRecipe {
            recipe_id: TaskRecipeId::new(id).unwrap(),
            purpose: ContractPurpose::new(id).unwrap(),
            contract_hash: agent.then(|| ContentHash::of_bytes(id.as_bytes())),
            task_class: class,
            allowed_evidence_sources: if agent {
                BTreeSet::from(["alpaca".to_owned()])
            } else {
                BTreeSet::new()
            },
            max_children: 8,
            max_depth: 2,
            priority_ceiling: 100,
            budget: budget(),
            retry: retry(),
            on_failure: FailureDisposition::FailRun,
        }
    }

    fn catalogue() -> RecipeCatalogue {
        let mut analyst = recipe("research.analyst", RuntimeTaskClass::Agent, true);
        analyst.max_children = 1;
        analyst.max_depth = 1;
        RecipeCatalogue::new(
            [
                recipe("research.planner", RuntimeTaskClass::Agent, true),
                analyst,
                recipe("research.critic", RuntimeTaskClass::Agent, true),
                recipe("gate.evidence", RuntimeTaskClass::Evidence, false),
                recipe("gate.decision", RuntimeTaskClass::DecisionGate, false),
                recipe("gate.execution", RuntimeTaskClass::ExecutionGate, false),
                recipe("gate.paper", RuntimeTaskClass::PaperCommit, false),
                recipe("gate.reconcile", RuntimeTaskClass::Reconcile, false),
                recipe("gate.evaluate", RuntimeTaskClass::Evaluate, false),
                recipe("learning.outcome_worker", RuntimeTaskClass::Evaluate, false),
            ],
            TaskRecipeId::new("research.planner").unwrap(),
            TerminalRecipeSet {
                evidence_gate: TaskRecipeId::new("gate.evidence").unwrap(),
                decision_gate: TaskRecipeId::new("gate.decision").unwrap(),
                execution_gate: TaskRecipeId::new("gate.execution").unwrap(),
                paper_commit: TaskRecipeId::new("gate.paper").unwrap(),
                reconcile: TaskRecipeId::new("gate.reconcile").unwrap(),
                evaluate: TaskRecipeId::new("gate.evaluate").unwrap(),
            },
            32,
        )
        .unwrap()
    }

    fn proposal() -> WorkflowProposal {
        WorkflowProposal {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: "active".to_owned(),
            tasks: BTreeMap::from([
                (
                    "analyst".to_owned(),
                    WorkflowProposalTask {
                        recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                        objective: "analyse evidence".to_owned(),
                        depends_on: vec![],
                        priority: 80,
                        evidence_needs: vec![],
                    },
                ),
                (
                    "critic".to_owned(),
                    WorkflowProposalTask {
                        recipe_id: TaskRecipeId::new("research.critic").unwrap(),
                        objective: "challenge claim".to_owned(),
                        depends_on: vec!["analyst".to_owned()],
                        priority: 70,
                        evidence_needs: vec![],
                    },
                ),
            ]),
            stop_reason: None,
        }
    }

    fn planner_output_artifact(
        store: &V2Store,
        planner: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Artifact {
        let draft = WorkflowProposalDraft {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: "active".to_owned(),
            tasks: BTreeMap::from([(
                "analyst".to_owned(),
                WorkflowProposalDraftTask {
                    recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                    objective: "analyse evidence".to_owned(),
                    depends_on: vec![],
                    priority: 80,
                    evidence_needs: vec![EvidenceNeed {
                        schema_version: V2_SCHEMA_VERSION,
                        source_family: "alpaca".to_owned(),
                        resource: "bars:TQQQ:1d".to_owned(),
                        max_age_secs: 86_400,
                    }],
                    research_intents: vec![],
                },
            )]),
            stop_reason: None,
        };
        Artifact::new(
            ArtifactKind::WorkflowProposalDraft,
            store.put_json(&draft).unwrap(),
            "agent.planner",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.agent".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: planner.permit.contract_hash.clone(),
            },
            Some(ArtifactOrigin {
                run_id: Some(planner.run_id.clone()),
                task_id: Some(planner.node.task_id.clone()),
                attempt_id: Some(planner.permit.attempt_id.clone()),
                contract_hash: planner.permit.contract_hash.clone(),
            }),
            vec![],
            now,
        )
        .unwrap()
    }

    fn task_artifact(store: &V2Store, task: &ClaimedAttempt, now: DateTime<Utc>) -> Artifact {
        let blob = store
            .put_bytes(b"task output", "text/plain; charset=utf-8")
            .unwrap();
        Artifact::new(
            ArtifactKind::AgentTurn,
            blob,
            "runtime.fixture",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "fixture".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: task.permit.contract_hash.clone(),
            },
            Some(ArtifactOrigin {
                run_id: Some(task.run_id.clone()),
                task_id: Some(task.node.task_id.clone()),
                attempt_id: Some(task.permit.attempt_id.clone()),
                contract_hash: task.permit.contract_hash.clone(),
            }),
            vec![],
            now,
        )
        .unwrap()
    }

    #[test]
    fn planner_graph_gets_non_bypassable_terminal_gates() {
        let root = tempdir().unwrap();
        let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), catalogue());
        let graph = runtime.bootstrap(RunPurpose::Debug, "active").unwrap();
        assert_eq!(graph.nodes.len(), 6);
        assert!(graph
            .nodes
            .iter()
            .any(|node| node.recipe_id.as_str() == "gate.evidence"));
        assert!(graph
            .nodes
            .iter()
            .any(|node| node.recipe_id.as_str() == "gate.decision"));
        assert!(!graph
            .nodes
            .iter()
            .any(|node| node.recipe_id.as_str() == "gate.paper"));
        graph.validate().unwrap();
    }

    #[test]
    fn structured_critique_requires_material_uncertainty_or_opposed_stances() {
        let clean = claim(ClaimStance::Neutral, 499_999, 500_001, false);
        assert!(!should_run_structured_critique(&[clean]));

        let gap = claim(ClaimStance::Neutral, 500_000, 900_000, true);
        assert!(should_run_structured_critique(&[gap]));

        let low_confidence = claim(ClaimStance::Neutral, 500_000, 500_000, false);
        assert!(should_run_structured_critique(&[low_confidence]));

        let bullish = claim(ClaimStance::Bullish, 1, 1_000_000, false);
        let bearish = claim(ClaimStance::Bearish, 1, 1_000_000, false);
        assert!(should_run_structured_critique(&[bullish, bearish]));
    }

    #[test]
    fn planner_cannot_schedule_critic_directly() {
        let root = tempdir().unwrap();
        let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), catalogue());
        let mut draft = WorkflowProposalDraft {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID.to_owned(),
            tasks: BTreeMap::from([
                (
                    "analyst".to_owned(),
                    WorkflowProposalDraftTask {
                        recipe_id: TaskRecipeId::new(ANALYST_RECIPE_ID).unwrap(),
                        objective: "analyse evidence".to_owned(),
                        depends_on: vec![],
                        priority: 80,
                        evidence_needs: vec![],
                        research_intents: vec![],
                    },
                ),
                (
                    "critic".to_owned(),
                    WorkflowProposalDraftTask {
                        recipe_id: TaskRecipeId::new(CRITIC_RECIPE_ID).unwrap(),
                        objective: "challenge claim".to_owned(),
                        depends_on: vec!["analyst".to_owned()],
                        priority: 70,
                        evidence_needs: vec![],
                        research_intents: vec![],
                    },
                ),
            ]),
            stop_reason: None,
        };
        assert!(matches!(
            runtime.insert_structured_critic(&mut draft),
            Err(RuntimeError::PlannerSchedulesCritic)
        ));
    }

    #[test]
    fn structured_critique_is_reserved_for_the_candidate_topology() {
        let root = tempdir().unwrap();
        let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), catalogue());
        let analyst = WorkflowProposalDraftTask {
            recipe_id: TaskRecipeId::new(ANALYST_RECIPE_ID).unwrap(),
            objective: "analyse evidence".to_owned(),
            depends_on: vec![],
            priority: 80,
            evidence_needs: vec![],
            research_intents: vec![],
        };
        let mut active = WorkflowProposalDraft {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: "active".to_owned(),
            tasks: BTreeMap::from([("analyst".to_owned(), analyst.clone())]),
            stop_reason: None,
        };
        runtime.insert_structured_critic(&mut active).unwrap();
        assert_eq!(active.tasks.len(), 1);

        let mut candidate = WorkflowProposalDraft {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID.to_owned(),
            tasks: BTreeMap::from([("analyst".to_owned(), analyst)]),
            stop_reason: None,
        };
        runtime.insert_structured_critic(&mut candidate).unwrap();
        let critic = candidate
            .tasks
            .values()
            .find(|task| task.recipe_id.as_str() == CRITIC_RECIPE_ID)
            .unwrap();
        assert_eq!(critic.depends_on, vec!["analyst"]);
    }

    #[test]
    fn planner_cannot_schedule_a_terminal_gate() {
        let root = tempdir().unwrap();
        let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), catalogue());
        let mut proposal = proposal();
        proposal.tasks.insert(
            "escape".to_owned(),
            WorkflowProposalTask {
                recipe_id: TaskRecipeId::new("gate.execution").unwrap(),
                objective: "bypass".to_owned(),
                depends_on: vec![],
                priority: 100,
                evidence_needs: vec![],
            },
        );
        assert!(matches!(
            runtime.lower(RunPurpose::Debug, &proposal),
            Err(RuntimeError::TerminalRecipeInProposal(_))
        ));
    }

    #[test]
    fn proposal_lowering_enforces_recipe_fanout_and_depth_limits() {
        let root = tempdir().unwrap();
        let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), catalogue());

        let mut fanout = proposal();
        fanout.tasks.insert(
            "parallel".to_owned(),
            WorkflowProposalTask {
                recipe_id: TaskRecipeId::new("research.critic").unwrap(),
                objective: "parallel review".to_owned(),
                depends_on: vec!["analyst".to_owned()],
                priority: 60,
                evidence_needs: vec![],
            },
        );
        assert!(matches!(
            runtime.lower(RunPurpose::Debug, &fanout),
            Err(RuntimeError::WorkflowFanoutLimit { .. })
        ));

        let mut depth = proposal();
        depth.tasks.insert(
            "grandchild".to_owned(),
            WorkflowProposalTask {
                recipe_id: TaskRecipeId::new("research.critic").unwrap(),
                objective: "deeper review".to_owned(),
                depends_on: vec!["critic".to_owned()],
                priority: 60,
                evidence_needs: vec![],
            },
        );
        assert!(matches!(
            runtime.lower(RunPurpose::Debug, &depth),
            Err(RuntimeError::WorkflowDepthLimit { .. })
        ));
    }

    #[test]
    fn proposal_rejects_cycles_unknown_recipes_and_priority_escalation() {
        let root = tempdir().unwrap();
        let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), catalogue());

        let mut cyclic = proposal();
        cyclic.tasks.get_mut("analyst").unwrap().depends_on = vec!["critic".to_owned()];
        assert!(matches!(
            runtime.lower(RunPurpose::Debug, &cyclic),
            Err(RuntimeError::Domain(DomainError::CyclicPlan))
        ));

        let mut unknown = proposal();
        unknown.tasks.get_mut("analyst").unwrap().recipe_id =
            TaskRecipeId::new("research.uninstalled").unwrap();
        assert!(matches!(
            runtime.lower(RunPurpose::Debug, &unknown),
            Err(RuntimeError::Domain(DomainError::EmptyField {
                field: "workflow_proposal.recipe"
            }))
        ));

        let mut escalated = proposal();
        escalated.tasks.get_mut("analyst").unwrap().priority = 101;
        assert!(matches!(
            runtime.lower(RunPurpose::Debug, &escalated),
            Err(RuntimeError::Domain(DomainError::InvalidBudget { .. }))
        ));
    }

    #[test]
    fn evidence_gate_aggregates_unique_evidence_needs_and_rejects_other_kinds() {
        let root = tempdir().unwrap();
        let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), catalogue());
        let need = ArtifactRef {
            artifact_id: akzio_domain::ArtifactId(ContentHash::of_bytes(b"evidence-need")),
            kind: ArtifactKind::EvidenceNeed,
        };
        let mut proposed = proposal();
        proposed.tasks.get_mut("analyst").unwrap().evidence_needs = vec![need.clone()];
        proposed.tasks.get_mut("critic").unwrap().evidence_needs = vec![need.clone()];

        let graph = runtime.lower(RunPurpose::Debug, &proposed).unwrap();
        let evidence_gate = graph
            .nodes
            .iter()
            .find(|node| node.recipe_id.as_str() == "gate.evidence")
            .unwrap();
        assert_eq!(evidence_gate.input_artifacts, vec![need]);

        proposed.tasks.get_mut("analyst").unwrap().evidence_needs[0].kind = ArtifactKind::Claim;
        assert!(matches!(
            runtime.lower(RunPurpose::Debug, &proposed),
            Err(RuntimeError::Domain(DomainError::EmptyField {
                field: "workflow_proposal.evidence_needs"
            }))
        ));
    }

    #[test]
    fn independent_research_nodes_are_claimable_in_parallel() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let runtime = WorkflowRuntime::new(store.clone(), catalogue());
        let mut parallel = proposal();
        parallel.tasks.insert(
            "parallel".to_owned(),
            WorkflowProposalTask {
                recipe_id: TaskRecipeId::new("research.critic").unwrap(),
                objective: "independent review".to_owned(),
                depends_on: vec![],
                priority: 60,
                evidence_needs: vec![],
            },
        );
        let graph = runtime.lower(RunPurpose::Debug, &parallel).unwrap();
        let run_id = RunId::new();
        runtime
            .submit(run_id, RunPurpose::Debug, graph, Utc::now())
            .unwrap();

        let evidence = store
            .claim_next_task("evidence-worker", Utc::now(), Duration::seconds(30))
            .unwrap()
            .unwrap();
        assert_eq!(evidence.node.recipe_id.as_str(), "gate.evidence");
        let evidence_output = task_artifact(&store, &evidence, Utc::now());
        store
            .commit_attempt(
                &evidence.permit,
                &[evidence_output],
                TaskStatus::Succeeded,
                Utc::now(),
            )
            .unwrap();

        let first = store
            .claim_next_task("worker-a", Utc::now(), Duration::seconds(30))
            .unwrap()
            .unwrap();
        let second = store
            .claim_next_task("worker-b", Utc::now(), Duration::seconds(30))
            .unwrap()
            .unwrap();
        assert_ne!(first.node.task_id, second.node.task_id);
        assert_eq!(first.node.objective, "analyse evidence");
        assert_eq!(second.node.objective, "independent review");
    }

    #[test]
    fn dynamic_patch_extends_research_without_replacing_terminal_chain() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let runtime = WorkflowRuntime::new(store.clone(), catalogue());
        let graph = runtime.bootstrap(RunPurpose::Debug, "active").unwrap();
        let run_id = RunId::new();
        let first = runtime
            .submit(run_id.clone(), RunPurpose::Debug, graph.clone(), Utc::now())
            .unwrap();
        let planner = store
            .claim_next_task("planner-worker", Utc::now(), Duration::seconds(30))
            .unwrap()
            .unwrap();
        assert_eq!(planner.node.recipe_id.as_str(), "research.planner");
        let planner_output = planner_output_artifact(&store, &planner, Utc::now());
        let second = runtime
            .apply_planner_output(&planner, &first, &graph, &planner_output, Utc::now())
            .unwrap();
        let patched: WorkflowGraph =
            serde_json::from_slice(&store.read_blob(&second.blob).unwrap()).unwrap();
        assert_eq!(
            store.artifact(&planner_output.artifact_id).unwrap(),
            planner_output
        );
        assert!(second.source_refs.contains(&ArtifactRef {
            artifact_id: first.artifact_id.clone(),
            kind: ArtifactKind::WorkflowGraph,
        }));
        let proposal_ref = second
            .source_refs
            .iter()
            .find(|reference| reference.kind == ArtifactKind::WorkflowProposal)
            .unwrap();
        let stored_proposal = store.artifact(&proposal_ref.artifact_id).unwrap();
        assert!(stored_proposal.source_refs.iter().any(|reference| {
            reference.artifact_id == planner_output.artifact_id
                && reference.kind == ArtifactKind::WorkflowProposalDraft
        }));
        assert!(matches!(
            store.validate_task_permit(&planner.permit),
            Err(StoreError::StalePermit(_))
        ));
        let recovered = runtime.recover(&run_id).unwrap();
        assert_eq!(recovered.revision.revision, 1);
        assert_eq!(recovered.revision.graph, patched);
        assert_eq!(runtime.replay_run(&run_id).unwrap(), recovered);
        assert_eq!(runtime.replay_revision(&run_id, 0).unwrap().graph, graph);
        assert_eq!(
            recovered
                .tasks
                .iter()
                .map(|task| task.node.task_id.clone())
                .collect::<BTreeSet<_>>(),
            patched
                .nodes
                .iter()
                .map(|node| node.task_id.clone())
                .collect::<BTreeSet<_>>()
        );
        let evidence = store
            .claim_next_task("evidence-worker", Utc::now(), Duration::seconds(30))
            .unwrap()
            .unwrap();
        assert_eq!(evidence.node.recipe_id.as_str(), "gate.evidence");
        assert!(store
            .events_after(&run_id, 0, 100)
            .unwrap()
            .iter()
            .any(|event| {
                event.event_type == "workflow.patched"
                    && event.artifact_id.as_ref() == Some(&second.artifact_id)
            }));
        for recipe in [
            "gate.evidence",
            "gate.decision",
            "gate.execution",
            "gate.reconcile",
            "gate.evaluate",
        ] {
            let before = graph
                .nodes
                .iter()
                .find(|node| node.recipe_id.as_str() == recipe)
                .unwrap();
            let after = patched
                .nodes
                .iter()
                .find(|node| node.recipe_id.as_str() == recipe)
                .unwrap();
            assert_eq!(before.task_id, after.task_id);
        }
        let decision = patched
            .nodes
            .iter()
            .find(|node| node.recipe_id.as_str() == "gate.decision")
            .unwrap();
        assert_eq!(decision.dependencies.len(), 1);
        assert!(matches!(
            runtime.bootstrap(RunPurpose::Paper, "active"),
            Err(RuntimeError::PaperWorkflowRequiresPrecompiledProposal)
        ));
    }

    #[test]
    fn replay_rejects_unknown_durable_event_types() {
        let root = tempdir().unwrap();
        let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), catalogue());
        let run_id = RunId::new();
        let event = StoredEvent {
            cursor: 1,
            run_id: run_id.clone(),
            task_id: None,
            attempt_id: None,
            event_type: "unknown.replay.event".to_owned(),
            artifact_id: None,
            created_at: Utc::now(),
        };

        assert!(matches!(
            runtime.reduce_event(&run_id, &mut ReplayedWorkflow::default(), &event),
            Err(RuntimeError::ReplayDiverged { .. })
        ));
    }

    #[test]
    fn replay_accepts_task_artifact_trace_events_with_matching_origin() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let runtime = WorkflowRuntime::new(store.clone(), catalogue());
        let run_id = RunId::new();
        let now = Utc::now();
        runtime
            .submit(
                run_id.clone(),
                RunPurpose::Debug,
                runtime.bootstrap(RunPurpose::Debug, "active").unwrap(),
                now,
            )
            .unwrap();
        let claimed = store
            .claim_next_task("trace-worker", now, Duration::seconds(30))
            .unwrap()
            .unwrap();
        let artifact = task_artifact(&store, &claimed, now);
        store
            .write_task_artifact(&claimed.permit, &artifact, "agent.turn_completed", now)
            .unwrap();

        assert_eq!(
            runtime.replay_run(&run_id).unwrap(),
            store.workflow_snapshot(&run_id).unwrap()
        );
    }

    #[test]
    fn replay_rejects_snapshot_task_divergence() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let runtime = WorkflowRuntime::new(store.clone(), catalogue());
        let graph = runtime.lower(RunPurpose::Debug, &proposal()).unwrap();
        let run_id = RunId::new();
        runtime
            .submit(run_id.clone(), RunPurpose::Debug, graph, Utc::now())
            .unwrap();
        let replay = runtime.reduce_history(&run_id).unwrap();
        let mut forged = store.workflow_snapshot(&run_id).unwrap();
        forged.tasks[0].status = TaskStatus::Succeeded;

        assert!(matches!(
            runtime.validate_replay_snapshot(&run_id, &replay, &forged),
            Err(RuntimeError::ReplayDiverged { .. })
        ));
    }

    #[test]
    fn planner_proposal_cannot_be_replayed_after_atomic_commit() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let workflow = WorkflowRuntime::new(store.clone(), catalogue());
        let graph = workflow.bootstrap(RunPurpose::Debug, "active").unwrap();
        let run_id = RunId::new();
        let first = workflow
            .submit(run_id.clone(), RunPurpose::Debug, graph.clone(), Utc::now())
            .unwrap();
        let planner = store
            .claim_next_task("planner-worker", Utc::now(), Duration::seconds(30))
            .unwrap()
            .unwrap();
        let planner_output = planner_output_artifact(&store, &planner, Utc::now());
        let second = workflow
            .apply_planner_output(&planner, &first, &graph, &planner_output, Utc::now())
            .unwrap();
        let patched: WorkflowGraph =
            serde_json::from_slice(&store.read_blob(&second.blob).unwrap()).unwrap();
        let events_before = store.events_after(&run_id, 0, 100).unwrap();

        assert!(matches!(
            workflow
                .apply_planner_output(&planner, &second, &patched, &planner_output, Utc::now(),),
            Err(RuntimeError::Store(StoreError::StalePermit(_)))
        ));
        assert_eq!(store.events_after(&run_id, 0, 100).unwrap(), events_before);
    }

    #[tokio::test]
    async fn task_runtime_accepts_only_store_verified_committed_attempts() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let workflow = WorkflowRuntime::new(store.clone(), catalogue());
        let graph = workflow.bootstrap(RunPurpose::Debug, "active").unwrap();
        let run_id = RunId::new();
        let first = workflow
            .submit(run_id, RunPurpose::Debug, graph.clone(), Utc::now())
            .unwrap();
        let tasks = TaskRuntime::new(store.clone());
        let handler_store = store.clone();
        let handler_workflow = workflow.clone();
        assert!(tasks
            .run_one("planner-worker", move |planner| {
                let planner_output = planner_output_artifact(&handler_store, &planner, Utc::now());
                handler_workflow
                    .apply_planner_output(&planner, &first, &graph, &planner_output, Utc::now())
                    .unwrap();
                async { TaskCompletion::Committed }
            })
            .await
            .unwrap());

        assert!(matches!(
            tasks
                .run_one("untrusted-worker", |_| async { TaskCompletion::Committed })
                .await,
            Err(RuntimeError::Store(StoreError::StalePermit(_)))
        ));
    }

    #[tokio::test]
    async fn task_runtime_retries_then_commits_outputs_with_a_new_attempt() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let workflow = WorkflowRuntime::new(store.clone(), catalogue());
        let graph = workflow.lower(RunPurpose::Debug, &proposal()).unwrap();
        let run_id = RunId::new();
        workflow
            .submit(run_id.clone(), RunPurpose::Debug, graph, Utc::now())
            .unwrap();
        let tasks = TaskRuntime::new(store.clone())
            .with_lease_duration(Duration::seconds(3))
            .unwrap();
        assert!(tasks
            .run_one("worker", |_| async {
                TaskCompletion::Retry(RetryCause::Transport)
            })
            .await
            .unwrap());
        assert!(tasks
            .run_one("worker", move |task| {
                let artifact = task_artifact(&store, &task, Utc::now());
                async move { TaskCompletion::Succeeded(vec![artifact]) }
            })
            .await
            .unwrap());
        let events = tasks.store().events_after(&run_id, 0, 100).unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event_type == "task.retry_scheduled")
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event_type == "task.started")
                .count(),
            2
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event_type == "task.succeeded")
                .count(),
            1
        );
        let first_page = tasks.store().events_after(&run_id, 0, 2).unwrap();
        let cursor = first_page.last().unwrap().cursor;
        let mut replay = first_page;
        replay.extend(tasks.store().events_after(&run_id, cursor, 100).unwrap());
        assert_eq!(replay, events);
        assert_eq!(
            workflow.replay_run(&run_id).unwrap(),
            tasks.store().workflow_snapshot(&run_id).unwrap()
        );
    }

    #[tokio::test]
    async fn task_runtime_recovers_expired_attempt_and_honors_cancel_requests() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let workflow = WorkflowRuntime::new(store.clone(), catalogue());
        let graph = workflow.lower(RunPurpose::Debug, &proposal()).unwrap();
        let run_id = RunId::new();
        workflow
            .submit(run_id.clone(), RunPurpose::Debug, graph, Utc::now())
            .unwrap();
        let abandoned = store
            .claim_next_task("crashed-worker", Utc::now(), Duration::milliseconds(-1))
            .unwrap()
            .unwrap();
        let abandoned_task_id = abandoned.node.task_id.clone();
        let before_recovery = workflow.recover(&run_id).unwrap();
        assert_eq!(
            before_recovery
                .tasks
                .iter()
                .find(|task| task.node.task_id == abandoned_task_id)
                .unwrap()
                .active_attempt
                .as_ref()
                .unwrap()
                .permit,
            abandoned.permit
        );
        let tasks = TaskRuntime::new(store.clone())
            .with_lease_duration(Duration::seconds(3))
            .unwrap();
        let old_permit = abandoned.permit.clone();
        let old_attempt_id = old_permit.attempt_id.clone();
        let old_epoch = old_permit.epoch;
        assert!(tasks
            .run_one("recovery-worker", move |task| {
                assert_ne!(task.permit.attempt_id, old_attempt_id);
                assert!(task.permit.epoch > old_epoch);
                let artifact = task_artifact(&store, &task, Utc::now());
                async move { TaskCompletion::Succeeded(vec![artifact]) }
            })
            .await
            .unwrap());
        let after_recovery = workflow.recover(&run_id).unwrap();
        assert_eq!(after_recovery.revision, before_recovery.revision);
        assert_eq!(
            after_recovery
                .tasks
                .iter()
                .map(|task| task.node.task_id.clone())
                .collect::<BTreeSet<_>>(),
            before_recovery
                .tasks
                .iter()
                .map(|task| task.node.task_id.clone())
                .collect::<BTreeSet<_>>()
        );
        let recovered_task = after_recovery
            .tasks
            .iter()
            .find(|task| task.node.task_id == abandoned_task_id)
            .unwrap();
        assert_eq!(recovered_task.status, TaskStatus::Succeeded);
        assert_eq!(recovered_task.attempt_count, 2);
        assert!(recovered_task.active_attempt.is_none());
        assert_eq!(workflow.replay_run(&run_id).unwrap(), after_recovery);
        assert!(matches!(
            tasks
                .store()
                .finish_task(&old_permit, TaskStatus::Skipped, Utc::now()),
            Err(StoreError::StalePermit(_))
        ));
        assert!(tasks
            .store()
            .request_run_cancel(&run_id, "operator", Utc::now())
            .unwrap());
        assert!(!tasks
            .store()
            .request_run_cancel(&run_id, "operator", Utc::now())
            .unwrap());
        assert!(!tasks
            .run_one("cancelled-worker", |_| async {
                panic!("cancelled run must not dispatch")
            })
            .await
            .unwrap());
        let events = tasks.store().events_after(&run_id, 0, 100).unwrap();
        assert!(events
            .iter()
            .any(|event| event.event_type == "task.recovered"));
        assert!(events
            .iter()
            .any(|event| event.event_type == "run.cancel_requested"));
        assert_eq!(
            workflow.replay_run(&run_id).unwrap(),
            tasks.store().workflow_snapshot(&run_id).unwrap()
        );
    }

    #[test]
    fn submit_rejects_graphs_that_bypass_or_mutate_rust_terminal_gates() {
        let root = tempdir().unwrap();
        let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), catalogue());
        let mut missing_gate = runtime.lower(RunPurpose::Debug, &proposal()).unwrap();
        missing_gate
            .nodes
            .retain(|node| node.recipe_id.as_str() != "gate.evaluate");
        missing_gate.validate().unwrap();
        assert!(matches!(
            runtime.submit(RunId::new(), RunPurpose::Debug, missing_gate, Utc::now()),
            Err(RuntimeError::MissingTerminalGate(_))
        ));

        let mut altered_gate = runtime.lower(RunPurpose::Debug, &proposal()).unwrap();
        altered_gate
            .nodes
            .iter_mut()
            .find(|node| node.recipe_id.as_str() == "gate.execution")
            .unwrap()
            .dependencies
            .clear();
        altered_gate.validate().unwrap();
        assert!(matches!(
            runtime.submit(RunId::new(), RunPurpose::Debug, altered_gate, Utc::now()),
            Err(RuntimeError::InvalidTerminalDependencies(_))
        ));
    }

    #[test]
    fn submit_rejects_nodes_that_diverge_from_the_installed_recipe() {
        let root = tempdir().unwrap();
        let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), catalogue());
        let mut graph = runtime.lower(RunPurpose::Debug, &proposal()).unwrap();
        graph
            .nodes
            .iter_mut()
            .find(|node| node.recipe_id.as_str() == "research.analyst")
            .unwrap()
            .budget
            .max_output_tokens = 49;
        graph.validate().unwrap();
        assert!(matches!(
            runtime.submit(RunId::new(), RunPurpose::Debug, graph, Utc::now()),
            Err(RuntimeError::NodeRecipeMismatch(_))
        ));
    }
}
