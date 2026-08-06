//! Rust-owned planning, task scheduling, and non-bypassable workflow gates.
//!
//! Models propose plans.  [`WorkflowCompiler`] owns which plans are legal and
//! [`WorkflowRuntime`] owns durable execution.  Neither knows prompt text nor
//! database schema internals.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    time::Duration as StdDuration,
};

use akzio_domain::{
    DocumentOrigin, EventEnvelope, FailureDisposition, RunId, RunPurpose, TaskBudget, TaskId,
    TaskKind, TaskSpec, TaskStatus, WorkflowPlan,
};
use akzio_store::{ClaimedTask, RetryTaskResult, StoreError, V2Store};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("workflow is missing required foundation task {0:?}")]
    MissingFoundation(TaskKind),
    #[error("workflow contains more than one singleton task {0:?}")]
    DuplicateSingleton(TaskKind),
    #[error("task {task_id:?} must depend on {ancestor:?}")]
    MissingAncestor { task_id: TaskId, ancestor: TaskKind },
    #[error("a shadow workflow must not create another shadow task")]
    RecursiveShadow,
    #[error("planner patch attempts to alter protected task {0:?}")]
    ProtectedTaskMutation(TaskId),
    #[error("plan patch references unknown task {0}")]
    UnknownPatchTask(TaskId),
    #[error("planner patch may not add reserved lifecycle task {0:?}")]
    ReservedPlannerTask(TaskKind),
    #[error("planner patch may only attach newly added work to a synthesizer")]
    IllegalPlannerDependency,
    #[error("task {0:?} is not legal for run purpose {1:?}")]
    IllegalPurpose(TaskKind, RunPurpose),
    #[error("task handler failed: {0}")]
    Handler(String),
    #[error("cannot recover scheduled run {run_id}: {reason}")]
    RecoveryMismatch { run_id: RunId, reason: String },
}

pub type Result<T> = std::result::Result<T, RuntimeError>;

/// The only owner of task leases, time budgets, retries, and task events.
/// Workers provide business results; they never mutate task state directly.
#[derive(Debug, Clone)]
pub struct TaskRuntime {
    store: V2Store,
    lease_duration: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskCompletion {
    Succeeded,
    Retry {
        retry_at: DateTime<Utc>,
        error: String,
    },
    Failed {
        error: String,
    },
    Skipped {
        reason: String,
    },
    Cancelled {
        reason: String,
    },
}

impl TaskRuntime {
    pub fn new(store: V2Store) -> Self {
        Self {
            store,
            lease_duration: Duration::seconds(30),
        }
    }

    pub fn with_lease_duration(mut self, lease_duration: Duration) -> Self {
        self.lease_duration = lease_duration;
        self
    }

    pub async fn run_one_async<F, Fut>(&self, worker_id: &str, handle: F) -> Result<bool>
    where
        F: FnOnce(ClaimedTask) -> Fut,
        Fut: Future<Output = TaskCompletion>,
    {
        let now = Utc::now();
        let Some(task) = self
            .store
            .claim_next_task(worker_id, now, now + self.lease_duration)?
        else {
            return Ok(false);
        };
        if self.store.run_cancel_requested(&task.run_id)? {
            self.finish(
                &task,
                TaskCompletion::Cancelled {
                    reason: "run cancelled before task started".to_owned(),
                },
            )?;
            self.store.refresh_run_status(&task.run_id, Utc::now())?;
            return Ok(true);
        }
        self.store.start_task(&task.lease, now)?;
        self.store.refresh_run_status(&task.run_id, now)?;
        self.record(&task, "task.started", None, now)?;

        let heartbeat_every = StdDuration::from_secs(
            self.lease_duration
                .num_seconds()
                .max(3)
                .unsigned_abs()
                .div_ceil(3),
        );
        let mut heartbeat = tokio::time::interval(heartbeat_every);
        heartbeat.tick().await;
        let mut handler = Box::pin(handle(task.clone()));
        let execution = async {
            loop {
                tokio::select! {
                    completion = &mut handler => break Ok::<TaskCompletion, RuntimeError>(completion),
                    _ = heartbeat.tick() => {
                        self.store.heartbeat(
                            &task.lease,
                            Utc::now() + self.lease_duration,
                        )?;
                    }
                }
            }
        };
        let timeout = StdDuration::from_secs(u64::from(task.budget.max_wall_time_secs));
        let completion = match tokio::time::timeout(timeout, execution).await {
            Ok(completion) => completion?,
            Err(_) => TaskCompletion::Retry {
                retry_at: Utc::now() + Duration::seconds(1),
                error: format!(
                    "task exceeded its {} second wall-time budget",
                    task.budget.max_wall_time_secs
                ),
            },
        };
        let completion = if self.store.run_cancel_requested(&task.run_id)? {
            TaskCompletion::Cancelled {
                reason: "run cancelled while task was running".to_owned(),
            }
        } else {
            completion
        };
        self.finish(&task, completion)?;
        self.store.refresh_run_status(&task.run_id, Utc::now())?;
        Ok(true)
    }

    fn finish(&self, task: &ClaimedTask, completion: TaskCompletion) -> Result<()> {
        match completion {
            TaskCompletion::Succeeded => {
                self.store
                    .complete_task(&task.lease, TaskStatus::Succeeded, task.on_failure)?;
                self.record(task, "task.succeeded", None, Utc::now())?;
            }
            TaskCompletion::Retry { retry_at, error } => {
                let payload = self.error_payload(&error)?;
                match self.store.retry_task(
                    &task.lease,
                    retry_at,
                    Some(&payload),
                    task.on_failure,
                )? {
                    RetryTaskResult::Requeued => {
                        self.record(task, "task.retry_scheduled", Some(payload), Utc::now())?;
                    }
                    RetryTaskResult::Terminal(TaskStatus::Skipped) => {
                        self.record(task, "task.skipped", Some(payload), Utc::now())?;
                    }
                    RetryTaskResult::Terminal(TaskStatus::Cancelled) => {
                        self.record(task, "task.cancelled", Some(payload), Utc::now())?;
                    }
                    RetryTaskResult::Terminal(_) => {
                        self.record(task, "task.failed", Some(payload), Utc::now())?;
                    }
                }
            }
            TaskCompletion::Failed { error } => {
                let payload = self.error_payload(&error)?;
                let status =
                    self.store
                        .complete_task(&task.lease, TaskStatus::Failed, task.on_failure)?;
                self.record(
                    task,
                    if status == TaskStatus::Skipped {
                        "task.skipped"
                    } else {
                        "task.failed"
                    },
                    Some(payload),
                    Utc::now(),
                )?;
            }
            TaskCompletion::Skipped { reason } => {
                let payload = self.error_payload(&reason)?;
                self.store
                    .complete_task(&task.lease, TaskStatus::Skipped, task.on_failure)?;
                self.record(task, "task.skipped", Some(payload), Utc::now())?;
            }
            TaskCompletion::Cancelled { reason } => {
                let payload = self.error_payload(&reason)?;
                self.store
                    .complete_task(&task.lease, TaskStatus::Cancelled, task.on_failure)?;
                self.record(task, "task.cancelled", Some(payload), Utc::now())?;
            }
        }
        Ok(())
    }

    fn error_payload(&self, error: &str) -> Result<akzio_domain::BlobRef> {
        self.store
            .put_bytes(error.as_bytes(), "text/plain; charset=utf-8")
            .map_err(RuntimeError::from)
    }

    fn record(
        &self,
        task: &ClaimedTask,
        event_type: &str,
        payload: Option<akzio_domain::BlobRef>,
        created_at: DateTime<Utc>,
    ) -> Result<()> {
        self.store.append_event(&EventEnvelope {
            schema_version: akzio_domain::V2_SCHEMA_VERSION,
            run_id: task.run_id.clone(),
            task_id: Some(task.task_id.clone()),
            attempt_id: Some(task.attempt_id.clone()),
            contract_hash: task.contract_hash.clone(),
            causation_id: Some(task.lease.lease_id.0.clone()),
            event_type: event_type.to_owned(),
            payload_document_id: None,
            payload,
            created_at,
        })?;
        Ok(())
    }

    pub fn store(&self) -> &V2Store {
        &self.store
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledWorkflow {
    pub purpose: RunPurpose,
    pub plan: WorkflowPlan,
    pub topological_order: Vec<TaskId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanPatch {
    pub add_tasks: Vec<akzio_domain::TaskSpec>,
    pub add_dependencies: Vec<DependencyPatch>,
    pub skip_optional_tasks: Vec<TaskId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyPatch {
    pub task_id: TaskId,
    pub depends_on_task_id: TaskId,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct WorkflowCompiler;

impl WorkflowCompiler {
    pub fn compile(&self, purpose: RunPurpose, plan: WorkflowPlan) -> Result<CompiledWorkflow> {
        plan.validate()
            .map_err(|error| RuntimeError::Handler(error.to_string()))?;
        if purpose == RunPurpose::Shadow
            && plan.tasks.iter().any(|task| task.kind == TaskKind::Shadow)
        {
            return Err(RuntimeError::RecursiveShadow);
        }
        validate_foundation(&plan)?;
        validate_singletons(&plan)?;
        validate_dependencies(purpose, &plan)?;
        let order = topological_order(&plan);
        Ok(CompiledWorkflow {
            purpose,
            plan,
            topological_order: order,
        })
    }

    pub fn apply_planner_patch(
        &self,
        workflow: &CompiledWorkflow,
        patch: PlanPatch,
    ) -> Result<CompiledWorkflow> {
        for task_id in &patch.skip_optional_tasks {
            let task = workflow
                .plan
                .tasks
                .iter()
                .find(|task| task.task_id == *task_id)
                .ok_or_else(|| RuntimeError::UnknownPatchTask(task_id.clone()))?;
            if task.on_failure != FailureDisposition::SkipTask || !planner_mutable_task(task.kind) {
                return Err(RuntimeError::ProtectedTaskMutation(task_id.clone()));
            }
        }
        let skipped = patch
            .skip_optional_tasks
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut plan = workflow.plan.clone();
        plan.tasks.retain(|task| !skipped.contains(&task.task_id));
        if let Some(task) = patch
            .add_tasks
            .iter()
            .find(|task| !planner_addable_task(task.kind))
        {
            return Err(RuntimeError::ReservedPlannerTask(task.kind));
        }
        let added_ids = patch
            .add_tasks
            .iter()
            .map(|task| task.task_id.clone())
            .collect::<BTreeSet<_>>();
        plan.tasks.extend(patch.add_tasks);
        for dependency in patch.add_dependencies {
            if !added_ids.contains(&dependency.depends_on_task_id) {
                return Err(RuntimeError::IllegalPlannerDependency);
            }
            let task = plan
                .tasks
                .iter_mut()
                .find(|task| task.task_id == dependency.task_id)
                .ok_or_else(|| RuntimeError::UnknownPatchTask(dependency.task_id.clone()))?;
            if task.kind != TaskKind::SynthesizeDecision {
                return Err(RuntimeError::IllegalPlannerDependency);
            }
            if !task.dependencies.contains(&dependency.depends_on_task_id) {
                task.dependencies.push(dependency.depends_on_task_id);
            }
        }
        self.compile(workflow.purpose, plan)
    }
}

#[derive(Debug, Clone)]
pub struct WorkflowRuntime {
    store: V2Store,
    compiler: WorkflowCompiler,
}

impl WorkflowRuntime {
    pub fn new(store: V2Store) -> Self {
        Self {
            store,
            compiler: WorkflowCompiler,
        }
    }

    pub fn submit(
        &self,
        run_id: &RunId,
        purpose: RunPurpose,
        workflow: WorkflowPlan,
        created_at: DateTime<Utc>,
    ) -> Result<CompiledWorkflow> {
        let compiled = self.compiler.compile(purpose, workflow)?;
        self.store
            .create_run(run_id, purpose, &compiled.plan.topology_id.0, created_at)?;
        let plan_document_id =
            self.store
                .write_workflow_plan(run_id, &compiled.plan, None, None, created_at)?;
        for task in &compiled.plan.tasks {
            self.store.enqueue_task_spec(run_id, task, created_at)?;
        }
        for task in &compiled.plan.tasks {
            for dependency in &task.dependencies {
                self.store.add_task_dependency(&task.task_id, dependency)?;
            }
        }
        self.store.append_event(&EventEnvelope {
            schema_version: akzio_domain::V2_SCHEMA_VERSION,
            run_id: run_id.clone(),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
            causation_id: None,
            event_type: "workflow.submitted".to_owned(),
            payload_document_id: Some(plan_document_id.clone()),
            payload: Some(self.store.read_document(&plan_document_id)?.blob),
            created_at,
        })?;
        Ok(compiled)
    }

    /// Complete a schedule reservation that may have survived a daemon crash.
    /// The caller must supply the exact plan persisted with the reservation;
    /// this method never regenerates task IDs or rewrites a different plan.
    pub fn submit_or_recover(
        &self,
        run_id: &RunId,
        purpose: RunPurpose,
        workflow: WorkflowPlan,
        created_at: DateTime<Utc>,
    ) -> Result<CompiledWorkflow> {
        let compiled = self.compiler.compile(purpose, workflow)?;
        if !self.store.run_exists(run_id)? {
            return self.submit(run_id, purpose, compiled.plan.clone(), created_at);
        }
        if self.store.run_purpose(run_id)? != purpose
            || self.store.run_topology_id(run_id)? != compiled.plan.topology_id
        {
            return Err(RuntimeError::RecoveryMismatch {
                run_id: run_id.clone(),
                reason: "purpose or topology differs from the reserved plan".to_owned(),
            });
        }
        let plan_document_id = match self.store.workflow_plan(run_id) {
            Ok(existing) if existing == compiled.plan => {
                self.store.workflow_plan_document(run_id)?
            }
            Ok(_) => {
                return Err(RuntimeError::RecoveryMismatch {
                    run_id: run_id.clone(),
                    reason: "persisted workflow plan differs from the reservation".to_owned(),
                });
            }
            Err(StoreError::MissingWorkflowPlan(_)) => {
                self.store
                    .write_workflow_plan(run_id, &compiled.plan, None, None, created_at)?
            }
            Err(error) => return Err(error.into()),
        };
        for task in &compiled.plan.tasks {
            if !self.store.task_exists(&task.task_id)? {
                self.store.enqueue_task_spec(run_id, task, created_at)?;
            }
        }
        for task in &compiled.plan.tasks {
            for dependency in &task.dependencies {
                self.store.add_task_dependency(&task.task_id, dependency)?;
            }
        }
        self.store.append_event(&EventEnvelope {
            schema_version: akzio_domain::V2_SCHEMA_VERSION,
            run_id: run_id.clone(),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
            causation_id: None,
            event_type: "workflow.recovered".to_owned(),
            payload_document_id: Some(plan_document_id.clone()),
            payload: Some(self.store.read_document(&plan_document_id)?.blob),
            created_at,
        })?;
        Ok(compiled)
    }

    pub fn load(&self, run_id: &RunId) -> Result<CompiledWorkflow> {
        let purpose = self.store.run_purpose(run_id)?;
        self.compiler
            .compile(purpose, self.store.workflow_plan(run_id)?)
    }

    pub fn apply_planner_patch_to_run(
        &self,
        run_id: &RunId,
        current: &CompiledWorkflow,
        planner_task: &ClaimedTask,
        patch: PlanPatch,
        created_at: DateTime<Utc>,
    ) -> Result<CompiledWorkflow> {
        let updated = self.compiler.apply_planner_patch(current, patch)?;
        self.persist_patch(
            run_id,
            current,
            &updated,
            Some(planner_task),
            "workflow.planner_patched",
            created_at,
        )?;
        Ok(updated)
    }

    /// Append the one lifecycle task Rust permits after a completed task.
    /// The planner cannot create these tasks, so a research topology can be
    /// dynamic without making decision or execution safety optional.
    pub fn advance_after_task(
        &self,
        task: &ClaimedTask,
        created_at: DateTime<Utc>,
    ) -> Result<Option<CompiledWorkflow>> {
        let current = self.load(&task.run_id)?;
        let Some(next_kind) = lifecycle_successor(current.purpose, task.kind) else {
            return Ok(None);
        };
        if current
            .plan
            .tasks
            .iter()
            .any(|existing| existing.kind == next_kind)
        {
            return Ok(Some(current));
        }
        let mut updated_plan = current.plan.clone();
        updated_plan.tasks.push(lifecycle_task(
            next_kind,
            task.task_id.clone(),
            task.task_id.clone(),
        ));
        let updated = self.compiler.compile(current.purpose, updated_plan)?;
        self.persist_patch(
            &task.run_id,
            &current,
            &updated,
            Some(task),
            "workflow.lifecycle_advanced",
            created_at,
        )?;
        Ok(Some(updated))
    }

    fn persist_patch(
        &self,
        run_id: &RunId,
        current: &CompiledWorkflow,
        updated: &CompiledWorkflow,
        source_task: Option<&ClaimedTask>,
        event_type: &str,
        created_at: DateTime<Utc>,
    ) -> Result<()> {
        let old_ids = current
            .plan
            .tasks
            .iter()
            .map(|task| task.task_id.clone())
            .collect::<BTreeSet<_>>();
        for task in updated
            .plan
            .tasks
            .iter()
            .filter(|task| !old_ids.contains(&task.task_id))
        {
            self.store.enqueue_task_spec(run_id, task, created_at)?;
        }
        for task in &updated.plan.tasks {
            for dependency in &task.dependencies {
                self.store.add_task_dependency(&task.task_id, dependency)?;
            }
        }
        let prior = self.store.workflow_plan_document(run_id)?;
        let origin = source_task.map(task_origin);
        let plan_document_id = self.store.write_workflow_plan(
            run_id,
            &updated.plan,
            Some(prior),
            origin,
            created_at,
        )?;
        self.store.append_event(&EventEnvelope {
            schema_version: akzio_domain::V2_SCHEMA_VERSION,
            run_id: run_id.clone(),
            task_id: source_task.map(|task| task.task_id.clone()),
            attempt_id: source_task.map(|task| task.attempt_id.clone()),
            contract_hash: source_task.and_then(|task| task.contract_hash.clone()),
            causation_id: source_task.map(|task| task.lease.lease_id.0.clone()),
            event_type: event_type.to_owned(),
            payload_document_id: Some(plan_document_id.clone()),
            payload: Some(self.store.read_document(&plan_document_id)?.blob),
            created_at,
        })?;
        Ok(())
    }

    pub fn compiler(&self) -> WorkflowCompiler {
        self.compiler
    }

    pub fn store(&self) -> &V2Store {
        &self.store
    }
}

fn validate_foundation(plan: &WorkflowPlan) -> Result<()> {
    let ingest = foundation_task(plan, TaskKind::Ingest)?;
    let memory = foundation_task(plan, TaskKind::MemoryOverlay)?;
    let planner = foundation_task(plan, TaskKind::Plan)?;
    require_ancestor(plan, planner, ingest)?;
    require_ancestor(plan, planner, memory)?;
    Ok(())
}

fn validate_singletons(plan: &WorkflowPlan) -> Result<()> {
    for kind in [
        TaskKind::SynthesizeDecision,
        TaskKind::DecisionGate,
        TaskKind::ExecutionGate,
        TaskKind::ExecutePaper,
        TaskKind::Reconcile,
        TaskKind::Evaluate,
        TaskKind::Shadow,
    ] {
        if plan.tasks.iter().filter(|task| task.kind == kind).count() > 1 {
            return Err(RuntimeError::DuplicateSingleton(kind));
        }
    }
    Ok(())
}

fn validate_dependencies(purpose: RunPurpose, plan: &WorkflowPlan) -> Result<()> {
    let planner = foundation_task(plan, TaskKind::Plan)?;
    for task in &plan.tasks {
        if matches!(task.kind, TaskKind::Investigate | TaskKind::Challenge) {
            require_ancestor(plan, task, planner)?;
        }
    }
    if let Some(synthesizer) = task_by_kind(plan, TaskKind::SynthesizeDecision) {
        require_ancestor(plan, synthesizer, planner)?;
    }
    if let Some(decision) = task_by_kind(plan, TaskKind::DecisionGate) {
        require_kind_ancestor(plan, decision, TaskKind::SynthesizeDecision)?;
    }
    if let Some(execution) = task_by_kind(plan, TaskKind::ExecutionGate) {
        require_kind_ancestor(plan, execution, TaskKind::DecisionGate)?;
    }
    if let Some(paper) = task_by_kind(plan, TaskKind::ExecutePaper) {
        if !matches!(purpose, RunPurpose::Paper | RunPurpose::PaperDryRun) {
            return Err(RuntimeError::IllegalPurpose(
                TaskKind::ExecutePaper,
                purpose,
            ));
        }
        require_kind_ancestor(plan, paper, TaskKind::ExecutionGate)?;
    }
    if let Some(reconcile) = task_by_kind(plan, TaskKind::Reconcile) {
        let predecessor = if matches!(purpose, RunPurpose::Paper | RunPurpose::PaperDryRun) {
            TaskKind::ExecutePaper
        } else {
            TaskKind::ExecutionGate
        };
        require_kind_ancestor(plan, reconcile, predecessor)?;
    }
    if let Some(evaluate) = task_by_kind(plan, TaskKind::Evaluate) {
        require_kind_ancestor(plan, evaluate, TaskKind::Reconcile)?;
    }
    if let Some(shadow) = task_by_kind(plan, TaskKind::Shadow) {
        if purpose == RunPurpose::Shadow {
            return Err(RuntimeError::RecursiveShadow);
        }
        require_kind_ancestor(plan, shadow, TaskKind::Evaluate)?;
    }
    Ok(())
}

fn foundation_task(plan: &WorkflowPlan, kind: TaskKind) -> Result<&TaskSpec> {
    let mut matches = plan.tasks.iter().filter(|task| task.kind == kind);
    let task = matches
        .next()
        .ok_or(RuntimeError::MissingFoundation(kind))?;
    if matches.next().is_some() {
        return Err(RuntimeError::DuplicateSingleton(kind));
    }
    Ok(task)
}

fn task_by_kind(plan: &WorkflowPlan, kind: TaskKind) -> Option<&TaskSpec> {
    plan.tasks.iter().find(|task| task.kind == kind)
}

fn require_kind_ancestor(plan: &WorkflowPlan, task: &TaskSpec, kind: TaskKind) -> Result<()> {
    let Some(ancestor) = task_by_kind(plan, kind) else {
        return Err(RuntimeError::MissingAncestor {
            task_id: task.task_id.clone(),
            ancestor: kind,
        });
    };
    require_ancestor(plan, task, ancestor)
}

fn require_ancestor(plan: &WorkflowPlan, task: &TaskSpec, ancestor: &TaskSpec) -> Result<()> {
    if depends_on(plan, &task.task_id, &ancestor.task_id) {
        Ok(())
    } else {
        Err(RuntimeError::MissingAncestor {
            task_id: task.task_id.clone(),
            ancestor: ancestor.kind,
        })
    }
}

const fn planner_addable_task(kind: TaskKind) -> bool {
    matches!(
        kind,
        TaskKind::Investigate | TaskKind::Challenge | TaskKind::SynthesizeDecision
    )
}

const fn planner_mutable_task(kind: TaskKind) -> bool {
    matches!(kind, TaskKind::Investigate | TaskKind::Challenge)
}

fn lifecycle_successor(purpose: RunPurpose, completed: TaskKind) -> Option<TaskKind> {
    match completed {
        TaskKind::SynthesizeDecision => Some(TaskKind::DecisionGate),
        TaskKind::DecisionGate => Some(TaskKind::ExecutionGate),
        TaskKind::ExecutionGate => Some(
            if matches!(purpose, RunPurpose::Paper | RunPurpose::PaperDryRun) {
                TaskKind::ExecutePaper
            } else {
                TaskKind::Reconcile
            },
        ),
        TaskKind::ExecutePaper => Some(TaskKind::Reconcile),
        TaskKind::Reconcile => Some(TaskKind::Evaluate),
        TaskKind::Evaluate if purpose != RunPurpose::Shadow => Some(TaskKind::Shadow),
        _ => None,
    }
}

fn lifecycle_task(kind: TaskKind, dependency: TaskId, parent_task_id: TaskId) -> TaskSpec {
    TaskSpec {
        task_id: TaskId::new(),
        kind,
        objective: format!("Rust-owned {kind:?} lifecycle transition"),
        contract_hash: None,
        dependencies: vec![dependency],
        input_refs: vec![],
        budget: TaskBudget {
            max_input_tokens: 1_024,
            max_output_tokens: 256,
            max_wall_time_secs: 120,
            max_tool_calls: 0,
        },
        on_failure: if matches!(kind, TaskKind::Evaluate | TaskKind::Shadow) {
            FailureDisposition::SkipTask
        } else {
            FailureDisposition::FailRun
        },
        priority: 100,
        max_attempts: 3,
        parent_task_id: Some(parent_task_id),
    }
}

fn task_origin(task: &ClaimedTask) -> DocumentOrigin {
    DocumentOrigin::task(
        task.task_id.clone(),
        task.attempt_id.clone(),
        task.contract_hash.clone(),
    )
}

fn depends_on(plan: &WorkflowPlan, task_id: &TaskId, ancestor: &TaskId) -> bool {
    let tasks = plan
        .tasks
        .iter()
        .map(|task| (&task.task_id, task))
        .collect::<BTreeMap<_, _>>();
    let mut pending = vec![task_id];
    let mut visited = BTreeSet::new();
    while let Some(current) = pending.pop() {
        let Some(task) = tasks.get(current) else {
            continue;
        };
        for dependency in &task.dependencies {
            if dependency == ancestor {
                return true;
            }
            if visited.insert(dependency) {
                pending.push(dependency);
            }
        }
    }
    false
}

fn topological_order(plan: &WorkflowPlan) -> Vec<TaskId> {
    let mut remaining = plan
        .tasks
        .iter()
        .map(|task| {
            (
                task.task_id.clone(),
                task.dependencies.iter().cloned().collect::<BTreeSet<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut output = Vec::with_capacity(remaining.len());
    while !remaining.is_empty() {
        let ready = remaining
            .iter()
            .filter_map(|(task, dependencies)| dependencies.is_empty().then_some(task.clone()))
            .collect::<Vec<_>>();
        debug_assert!(!ready.is_empty(), "plan validation rejects cycles");
        for task in ready {
            remaining.remove(&task);
            for dependencies in remaining.values_mut() {
                dependencies.remove(&task);
            }
            output.push(task);
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use akzio_domain::{TaskBudget, TaskSpec, TopologyId, V2_SCHEMA_VERSION};
    use tempfile::tempdir;

    fn budget() -> TaskBudget {
        TaskBudget {
            max_input_tokens: 1,
            max_output_tokens: 1,
            max_wall_time_secs: 1,
            max_tool_calls: 0,
        }
    }

    fn plan(purpose: RunPurpose) -> WorkflowPlan {
        let task = |kind, dependencies| TaskSpec {
            task_id: TaskId::new(),
            kind,
            objective: format!("{kind:?}"),
            contract_hash: None,
            dependencies,
            input_refs: vec![],
            budget: budget(),
            on_failure: FailureDisposition::FailRun,
            priority: 100,
            max_attempts: 1,
            parent_task_id: None,
        };
        let ingest = task(TaskKind::Ingest, vec![]);
        let overlay = task(TaskKind::MemoryOverlay, vec![]);
        let planner = task(
            TaskKind::Plan,
            vec![ingest.task_id.clone(), overlay.task_id.clone()],
        );
        let synthesizer = task(TaskKind::SynthesizeDecision, vec![planner.task_id.clone()]);
        let decision = task(TaskKind::DecisionGate, vec![synthesizer.task_id.clone()]);
        let execution = task(TaskKind::ExecutionGate, vec![decision.task_id.clone()]);
        let mut tasks = vec![
            ingest,
            overlay,
            planner,
            synthesizer,
            decision,
            execution.clone(),
        ];
        let mut predecessor = execution.task_id;
        if matches!(purpose, RunPurpose::Paper | RunPurpose::PaperDryRun) {
            let paper = task(TaskKind::ExecutePaper, vec![predecessor]);
            predecessor = paper.task_id.clone();
            tasks.push(paper);
        }
        let reconcile = task(TaskKind::Reconcile, vec![predecessor]);
        let evaluate = task(TaskKind::Evaluate, vec![reconcile.task_id.clone()]);
        tasks.extend([reconcile, evaluate.clone()]);
        if purpose != RunPurpose::Shadow {
            tasks.push(task(TaskKind::Shadow, vec![evaluate.task_id]));
        }
        WorkflowPlan {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: TopologyId::new(),
            tasks,
        }
    }

    #[test]
    fn compiler_accepts_an_incomplete_dynamic_lifecycle() {
        let mut plan = plan(RunPurpose::Debug);
        plan.tasks.retain(|task| {
            matches!(
                task.kind,
                TaskKind::Ingest | TaskKind::MemoryOverlay | TaskKind::Plan
            )
        });
        assert!(WorkflowCompiler.compile(RunPurpose::Debug, plan).is_ok());
    }

    #[test]
    fn compiler_rejects_execution_before_decision() {
        let mut plan = plan(RunPurpose::Debug);
        plan.tasks.retain(|task| {
            matches!(
                task.kind,
                TaskKind::Ingest | TaskKind::MemoryOverlay | TaskKind::Plan
            )
        });
        let planner_id = plan
            .tasks
            .iter()
            .find(|task| task.kind == TaskKind::Plan)
            .unwrap()
            .task_id
            .clone();
        plan.tasks.push(TaskSpec {
            task_id: TaskId::new(),
            kind: TaskKind::ExecutionGate,
            objective: "illegal execution".to_owned(),
            contract_hash: None,
            dependencies: vec![planner_id],
            input_refs: vec![],
            budget: budget(),
            on_failure: FailureDisposition::FailRun,
            priority: 100,
            max_attempts: 1,
            parent_task_id: None,
        });
        assert!(matches!(
            WorkflowCompiler.compile(RunPurpose::Debug, plan),
            Err(RuntimeError::MissingAncestor {
                ancestor: TaskKind::DecisionGate,
                ..
            })
        ));
    }

    #[test]
    fn compiler_rejects_recursive_shadow_run() {
        let mut plan = plan(RunPurpose::Shadow);
        plan.tasks.push(TaskSpec {
            task_id: TaskId::new(),
            kind: TaskKind::Shadow,
            objective: "recursive shadow".to_owned(),
            contract_hash: None,
            dependencies: vec![],
            input_refs: vec![],
            budget: budget(),
            on_failure: FailureDisposition::FailTask,
            priority: 1,
            max_attempts: 1,
            parent_task_id: None,
        });
        assert!(matches!(
            WorkflowCompiler.compile(RunPurpose::Shadow, plan),
            Err(RuntimeError::RecursiveShadow)
        ));
    }

    #[test]
    fn compiler_requires_research_to_follow_planning() {
        let mut plan = plan(RunPurpose::Debug);
        let investigator = TaskSpec {
            task_id: TaskId::new(),
            kind: TaskKind::Investigate,
            objective: "unsafe research".to_owned(),
            contract_hash: None,
            dependencies: vec![],
            input_refs: vec![],
            budget: budget(),
            on_failure: FailureDisposition::FailTask,
            priority: 50,
            max_attempts: 1,
            parent_task_id: None,
        };
        let task_id = investigator.task_id.clone();
        plan.tasks.push(investigator);
        assert!(matches!(
            WorkflowCompiler.compile(RunPurpose::Debug, plan),
            Err(RuntimeError::MissingAncestor {
                task_id: id,
                ancestor: TaskKind::Plan,
            }) if id == task_id
        ));
    }

    #[tokio::test]
    async fn task_runtime_respects_persisted_dependencies() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        let runtime = WorkflowRuntime::new(store.clone());
        let tasks = TaskRuntime::new(store);
        let run = RunId::new();
        let plan = plan(RunPurpose::Debug);
        let expected_tasks = plan.tasks.len();
        runtime
            .submit(&run, RunPurpose::Debug, plan, Utc::now())
            .unwrap();
        let mut executed = 0;
        while tasks
            .run_one_async("test", |_| async { TaskCompletion::Succeeded })
            .await
            .unwrap()
        {
            executed += 1;
        }
        assert_eq!(executed, expected_tasks);
        assert!(runtime.store().event_count(&run).unwrap() > executed as u64);
    }
}
