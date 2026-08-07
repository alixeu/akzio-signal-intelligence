//! Dynamic workflow lowering for the rebuilt v2 runtime.

use std::collections::{BTreeMap, BTreeSet};

use akzio_domain::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactProvenance, ArtifactRef, DomainError,
    RunId, RunPurpose, RuntimeTaskClass, TaskRecipe, TaskRecipeId, WorkflowGraph, WorkflowNode,
    WorkflowProposal, REBUILD_SCHEMA_VERSION,
};
use akzio_store::{RebuildRun, RebuildStore, RebuildStoreError, WorkflowCommit};
use chrono::{DateTime, Utc};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RebuildRuntimeError {
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Store(#[from] RebuildStoreError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("recipe {0} is missing")]
    MissingRecipe(TaskRecipeId),
    #[error("Planner may not schedule Rust terminal recipe {0}")]
    TerminalRecipeInProposal(TaskRecipeId),
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
}

pub type RebuildRuntimeResult<T> = Result<T, RebuildRuntimeError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalRecipeSet {
    pub decision_gate: TaskRecipeId,
    pub execution_gate: TaskRecipeId,
    pub paper_commit: TaskRecipeId,
    pub reconcile: TaskRecipeId,
    pub evaluate: TaskRecipeId,
}

#[derive(Debug, Clone)]
pub struct RecipeCatalogue {
    recipes: BTreeMap<TaskRecipeId, TaskRecipe>,
    terminals: TerminalRecipeSet,
    max_nodes: usize,
}

impl RecipeCatalogue {
    pub fn new(
        recipes: impl IntoIterator<Item = TaskRecipe>,
        terminals: TerminalRecipeSet,
        max_nodes: usize,
    ) -> RebuildRuntimeResult<Self> {
        let recipes = recipes
            .into_iter()
            .map(|recipe| {
                recipe.validate()?;
                Ok((recipe.recipe_id.clone(), recipe))
            })
            .collect::<Result<BTreeMap<_, _>, DomainError>>()?;
        let catalogue = Self {
            recipes,
            terminals,
            max_nodes,
        };
        if catalogue.max_nodes == 0 {
            return Err(RebuildRuntimeError::WorkflowNodeLimit);
        }
        catalogue.assert_terminal(&catalogue.terminals.decision_gate, RuntimeTaskClass::DecisionGate)?;
        catalogue.assert_terminal(&catalogue.terminals.execution_gate, RuntimeTaskClass::ExecutionGate)?;
        catalogue.assert_terminal(&catalogue.terminals.paper_commit, RuntimeTaskClass::PaperCommit)?;
        catalogue.assert_terminal(&catalogue.terminals.reconcile, RuntimeTaskClass::Reconcile)?;
        catalogue.assert_terminal(&catalogue.terminals.evaluate, RuntimeTaskClass::Evaluate)?;
        Ok(catalogue)
    }

    pub fn recipe(&self, recipe_id: &TaskRecipeId) -> RebuildRuntimeResult<&TaskRecipe> {
        self.recipes
            .get(recipe_id)
            .ok_or_else(|| RebuildRuntimeError::MissingRecipe(recipe_id.clone()))
    }

    fn assert_terminal(
        &self,
        recipe_id: &TaskRecipeId,
        expected: RuntimeTaskClass,
    ) -> RebuildRuntimeResult<()> {
        let recipe = self.recipe(recipe_id)?;
        if recipe.task_class != expected {
            return Err(RebuildRuntimeError::InvalidTerminalRecipe {
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
}

#[derive(Debug, Clone)]
pub struct RebuildWorkflowRuntime {
    store: RebuildStore,
    catalogue: RecipeCatalogue,
}

impl RebuildWorkflowRuntime {
    pub fn new(store: RebuildStore, catalogue: RecipeCatalogue) -> Self {
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
    ) -> RebuildRuntimeResult<WorkflowGraph> {
        proposal.validate(&self.catalogue.recipes)?;
        let nodes = self.lower_research_nodes(proposal)?;
        self.with_terminal_gates(purpose, proposal.topology_id.clone(), nodes)
    }

    pub fn submit(
        &self,
        run_id: RunId,
        purpose: RunPurpose,
        graph: WorkflowGraph,
        now: DateTime<Utc>,
    ) -> RebuildRuntimeResult<Artifact> {
        graph.validate()?;
        let graph_artifact = self.graph_artifact(&graph, None, now)?;
        self.store.commit_workflow(&WorkflowCommit {
            run: RebuildRun {
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

    /// Applies a Planner proposal to a bootstrap graph. It adds all research nodes
    /// and then one immutable terminal chain; a later Planner cannot patch gates.
    pub fn apply_proposal(
        &self,
        run_id: &RunId,
        purpose: RunPurpose,
        previous_graph_artifact: &Artifact,
        previous_graph: &WorkflowGraph,
        proposal: &WorkflowProposal,
        now: DateTime<Utc>,
    ) -> RebuildRuntimeResult<Artifact> {
        if previous_graph.nodes.iter().any(|node| self.is_terminal_node(node)) {
            return Err(RebuildRuntimeError::TerminalGateAlreadyPresent);
        }
        if previous_graph.topology_id != proposal.topology_id {
            return Err(RebuildRuntimeError::Domain(DomainError::EmptyField {
                field: "workflow_proposal.topology_id",
            }));
        }
        let mut nodes = previous_graph.nodes.clone();
        let research = self.lower_research_nodes(proposal)?;
        nodes.extend(research);
        let graph = self.with_terminal_gates(purpose, previous_graph.topology_id.clone(), nodes)?;
        let next_artifact = self.graph_artifact(
            &graph,
            Some(ArtifactRef {
                artifact_id: previous_graph_artifact.artifact_id.clone(),
                kind: ArtifactKind::WorkflowGraph,
            }),
            now,
        )?;
        let known = previous_graph
            .nodes
            .iter()
            .map(|node| node.task_id.clone())
            .collect::<BTreeSet<_>>();
        let added_nodes = graph
            .nodes
            .iter()
            .filter(|node| !known.contains(&node.task_id))
            .cloned()
            .collect::<Vec<_>>();
        self.store.append_workflow_patch(
            run_id,
            &previous_graph_artifact.artifact_id,
            &next_artifact,
            &added_nodes,
            now,
        )?;
        Ok(next_artifact)
    }

    fn lower_research_nodes(
        &self,
        proposal: &WorkflowProposal,
    ) -> RebuildRuntimeResult<Vec<WorkflowNode>> {
        if proposal.tasks.len() > self.catalogue.max_nodes {
            return Err(RebuildRuntimeError::WorkflowNodeLimit);
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
                if self.catalogue.is_terminal(&task.recipe_id)
                    || matches!(
                        recipe.task_class,
                        RuntimeTaskClass::DecisionGate
                            | RuntimeTaskClass::ExecutionGate
                            | RuntimeTaskClass::PaperCommit
                            | RuntimeTaskClass::Reconcile
                            | RuntimeTaskClass::Evaluate
                    )
                {
                    return Err(RebuildRuntimeError::TerminalRecipeInProposal(
                        task.recipe_id.clone(),
                    ));
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
                    parent_task_id: None,
                })
            })
            .collect()
    }

    fn with_terminal_gates(
        &self,
        purpose: RunPurpose,
        topology_id: String,
        mut nodes: Vec<WorkflowNode>,
    ) -> RebuildRuntimeResult<WorkflowGraph> {
        let leaves = leaf_ids(&nodes);
        if leaves.is_empty() {
            return Err(RebuildRuntimeError::WorkflowNodeLimit);
        }
        let decision = self.gate_node(&self.catalogue.terminals.decision_gate, leaves)?;
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
        nodes.extend([decision, execution, reconcile, evaluate]);
        if nodes.len() > self.catalogue.max_nodes {
            return Err(RebuildRuntimeError::WorkflowNodeLimit);
        }
        let graph = WorkflowGraph {
            schema_version: REBUILD_SCHEMA_VERSION,
            topology_id,
            nodes,
        };
        graph.validate()?;
        Ok(graph)
    }

    fn gate_node(
        &self,
        recipe_id: &TaskRecipeId,
        dependencies: Vec<akzio_domain::TaskId>,
    ) -> RebuildRuntimeResult<WorkflowNode> {
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
        previous: Option<ArtifactRef>,
        now: DateTime<Utc>,
    ) -> RebuildRuntimeResult<Artifact> {
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
            previous.into_iter().collect(),
            now,
        )?)
    }

    fn is_terminal_node(&self, node: &WorkflowNode) -> bool {
        self.catalogue.recipe(&node.recipe_id).is_ok_and(|recipe| {
            matches!(
                recipe.task_class,
                RuntimeTaskClass::DecisionGate
                    | RuntimeTaskClass::ExecutionGate
                    | RuntimeTaskClass::PaperCommit
                    | RuntimeTaskClass::Reconcile
                    | RuntimeTaskClass::Evaluate
            )
        })
    }
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
    use std::collections::BTreeMap;

    use akzio_domain::{
        ContentHash, ContractPurpose, FailureDisposition, RetryPolicy, TaskBudget,
        WorkflowProposalTask,
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
            max_attempts: 1,
            initial_backoff_ms: 1,
            retry_transport: true,
            retry_rate_limited: true,
            retry_invalid_output: false,
        }
    }

    fn recipe(id: &str, class: RuntimeTaskClass, agent: bool) -> TaskRecipe {
        TaskRecipe {
            recipe_id: TaskRecipeId::new(id).unwrap(),
            purpose: ContractPurpose::new(id).unwrap(),
            contract_hash: agent.then(|| ContentHash::of_bytes(id.as_bytes())),
            task_class: class,
            max_children: 8,
            max_depth: 2,
            priority_ceiling: 100,
            budget: budget(),
            retry: retry(),
            on_failure: FailureDisposition::FailRun,
        }
    }

    fn catalogue() -> RecipeCatalogue {
        RecipeCatalogue::new(
            [
                recipe("research.analyst", RuntimeTaskClass::Agent, true),
                recipe("research.critic", RuntimeTaskClass::Agent, true),
                recipe("gate.decision", RuntimeTaskClass::DecisionGate, false),
                recipe("gate.execution", RuntimeTaskClass::ExecutionGate, false),
                recipe("gate.paper", RuntimeTaskClass::PaperCommit, false),
                recipe("gate.reconcile", RuntimeTaskClass::Reconcile, false),
                recipe("gate.evaluate", RuntimeTaskClass::Evaluate, false),
            ],
            TerminalRecipeSet {
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
            schema_version: REBUILD_SCHEMA_VERSION,
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

    #[test]
    fn planner_graph_gets_non_bypassable_terminal_gates() {
        let root = tempdir().unwrap();
        let runtime = RebuildWorkflowRuntime::new(RebuildStore::open(root.path()).unwrap(), catalogue());
        let graph = runtime.lower(RunPurpose::Debug, &proposal()).unwrap();
        assert_eq!(graph.nodes.len(), 6);
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
    fn planner_cannot_schedule_a_terminal_gate() {
        let root = tempdir().unwrap();
        let runtime = RebuildWorkflowRuntime::new(RebuildStore::open(root.path()).unwrap(), catalogue());
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
            Err(RebuildRuntimeError::TerminalRecipeInProposal(_))
        ));
    }
}
