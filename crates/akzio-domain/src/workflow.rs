//! Planner proposal and compiled workflow vocabulary.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::artifact::{ArtifactKind, ArtifactRef};
use crate::contract::{TaskRecipe, TaskRecipeId};
use crate::schema::V2_SCHEMA_VERSION;
use crate::{
    ContentHash, DomainError, FailureDisposition, ResearchIntent, ResearchShard, RetryPolicy,
    TaskBudget, TaskId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeTaskClass {
    Evidence,
    Agent,
    DecisionGate,
    ExecutionGate,
    PaperCommit,
    Reconcile,
    Evaluate,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EvidenceNeed {
    pub schema_version: u32,
    pub source_family: String,
    pub resource: String,
    pub max_age_secs: u64,
}

impl EvidenceNeed {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_SCHEMA_VERSION
            || self.source_family.trim().is_empty()
            || self.resource.trim().is_empty()
            || self.max_age_secs == 0
        {
            return Err(DomainError::EmptyField {
                field: "evidence_need",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowProposalDraftTask {
    pub recipe_id: TaskRecipeId,
    pub objective: String,
    pub depends_on: Vec<String>,
    pub priority: u8,
    pub evidence_needs: Vec<EvidenceNeed>,
    #[serde(default)]
    pub research_intents: Vec<ResearchIntent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowProposalDraft {
    pub schema_version: u32,
    pub topology_id: String,
    pub tasks: BTreeMap<String, WorkflowProposalDraftTask>,
    pub stop_reason: Option<String>,
}

impl WorkflowProposalDraft {
    pub fn validate(
        &self,
        recipes: &BTreeMap<TaskRecipeId, TaskRecipe>,
    ) -> Result<(), DomainError> {
        if self.schema_version != V2_SCHEMA_VERSION || self.topology_id.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "workflow_proposal_draft.identity",
            });
        }
        if self.tasks.is_empty() {
            return Err(DomainError::EmptyField {
                field: "workflow_proposal_draft.tasks",
            });
        }
        for (alias, task) in &self.tasks {
            if alias.trim().is_empty() || task.objective.trim().is_empty() || task.priority > 100 {
                return Err(DomainError::InvalidBudget {
                    field: "workflow_proposal_draft.task",
                });
            }
            let recipe = recipes
                .get(&task.recipe_id)
                .ok_or(DomainError::EmptyField {
                    field: "workflow_proposal_draft.recipe",
                })?;
            if task.priority > recipe.priority_ceiling {
                return Err(DomainError::InvalidBudget {
                    field: "workflow_proposal_draft.priority",
                });
            }
            let unique_needs = task.evidence_needs.iter().collect::<BTreeSet<_>>();
            if unique_needs.len() != task.evidence_needs.len() {
                return Err(DomainError::EmptyField {
                    field: "workflow_proposal_draft.evidence_needs",
                });
            }
            for need in &task.evidence_needs {
                need.validate()?;
                if !recipe
                    .allowed_evidence_sources
                    .contains(&need.source_family)
                {
                    return Err(DomainError::EvidenceSourceNotAllowed(
                        need.source_family.clone(),
                    ));
                }
            }
            let mut unique_intents = BTreeSet::new();
            let mut shard_counts = BTreeMap::<ResearchShard, usize>::new();
            for intent in &task.research_intents {
                intent.validate()?;
                let need = intent.evidence_need()?;
                if !unique_intents.insert(need.clone()) {
                    return Err(DomainError::EmptyField {
                        field: "workflow_proposal_draft.research_intents",
                    });
                }
                if !recipe
                    .allowed_evidence_sources
                    .contains(&need.source_family)
                {
                    return Err(DomainError::EvidenceSourceNotAllowed(need.source_family));
                }
                let count = shard_counts.entry(intent.shard()).or_default();
                *count += 1;
                if *count > 4 || task.research_intents.len() > 8 {
                    return Err(DomainError::InvalidBudget {
                        field: "workflow_proposal_draft.research_shards",
                    });
                }
            }
            if task
                .depends_on
                .iter()
                .any(|dependency| !self.tasks.contains_key(dependency))
            {
                return Err(DomainError::UnknownDependency {
                    task: TaskId(alias.clone()),
                    dependency: TaskId("proposal alias".to_owned()),
                });
            }
        }

        fn visit(
            alias: &str,
            tasks: &BTreeMap<String, WorkflowProposalDraftTask>,
            states: &mut BTreeMap<String, u8>,
        ) -> Result<(), DomainError> {
            match states.get(alias).copied() {
                Some(1) => return Err(DomainError::CyclicPlan),
                Some(2) => return Ok(()),
                _ => {}
            }
            states.insert(alias.to_owned(), 1);
            for dependency in &tasks[alias].depends_on {
                visit(dependency, tasks, states)?;
            }
            states.insert(alias.to_owned(), 2);
            Ok(())
        }

        let mut states = BTreeMap::new();
        for alias in self.tasks.keys() {
            visit(alias, &self.tasks, &mut states)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowProposalTask {
    pub recipe_id: TaskRecipeId,
    pub objective: String,
    pub depends_on: Vec<String>,
    pub priority: u8,
    pub evidence_needs: Vec<ArtifactRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowProposal {
    pub schema_version: u32,
    pub topology_id: String,
    pub tasks: BTreeMap<String, WorkflowProposalTask>,
    pub stop_reason: Option<String>,
}

impl WorkflowProposal {
    pub fn validate(
        &self,
        recipes: &BTreeMap<TaskRecipeId, TaskRecipe>,
    ) -> Result<(), DomainError> {
        if self.schema_version != V2_SCHEMA_VERSION || self.topology_id.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "workflow_proposal.identity",
            });
        }
        if self.tasks.is_empty() {
            return Err(DomainError::EmptyField {
                field: "workflow_proposal.tasks",
            });
        }
        for (alias, task) in &self.tasks {
            if alias.trim().is_empty() || task.objective.trim().is_empty() || task.priority > 100 {
                return Err(DomainError::InvalidBudget {
                    field: "workflow_proposal.task",
                });
            }
            let recipe = recipes
                .get(&task.recipe_id)
                .ok_or(DomainError::EmptyField {
                    field: "workflow_proposal.recipe",
                })?;
            if task.priority > recipe.priority_ceiling {
                return Err(DomainError::InvalidBudget {
                    field: "workflow_proposal.priority",
                });
            }
            let evidence_need_ids = task
                .evidence_needs
                .iter()
                .map(|reference| reference.artifact_id.clone())
                .collect::<BTreeSet<_>>();
            if evidence_need_ids.len() != task.evidence_needs.len()
                || task
                    .evidence_needs
                    .iter()
                    .any(|reference| reference.kind != ArtifactKind::EvidenceNeed)
            {
                return Err(DomainError::EmptyField {
                    field: "workflow_proposal.evidence_needs",
                });
            }
            if task
                .depends_on
                .iter()
                .any(|dependency| !self.tasks.contains_key(dependency))
            {
                return Err(DomainError::UnknownDependency {
                    task: TaskId(alias.clone()),
                    dependency: TaskId("proposal alias".to_owned()),
                });
            }
        }

        fn visit(
            alias: &str,
            tasks: &BTreeMap<String, WorkflowProposalTask>,
            states: &mut BTreeMap<String, u8>,
        ) -> Result<(), DomainError> {
            match states.get(alias).copied() {
                Some(1) => return Err(DomainError::CyclicPlan),
                Some(2) => return Ok(()),
                _ => {}
            }
            states.insert(alias.to_owned(), 1);
            for dependency in &tasks[alias].depends_on {
                visit(dependency, tasks, states)?;
            }
            states.insert(alias.to_owned(), 2);
            Ok(())
        }

        let mut states = BTreeMap::new();
        for alias in self.tasks.keys() {
            visit(alias, &self.tasks, &mut states)?;
        }
        Ok(())
    }
}

/// A fully lowered, immutable graph. Only `WorkflowRuntime` may construct this
/// from a proposal and the installed recipe catalogue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowNode {
    pub task_id: TaskId,
    pub recipe_id: TaskRecipeId,
    pub contract_hash: Option<ContentHash>,
    pub objective: String,
    pub dependencies: Vec<TaskId>,
    pub input_artifacts: Vec<ArtifactRef>,
    pub priority: u8,
    pub budget: TaskBudget,
    pub retry: RetryPolicy,
    pub on_failure: FailureDisposition,
    pub parent_task_id: Option<TaskId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowGraph {
    pub schema_version: u32,
    pub topology_id: String,
    pub nodes: Vec<WorkflowNode>,
}

impl WorkflowGraph {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_SCHEMA_VERSION || self.topology_id.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "workflow_graph.identity",
            });
        }
        if self.nodes.is_empty() {
            return Err(DomainError::EmptyField {
                field: "workflow_graph.nodes",
            });
        }
        let nodes = self
            .nodes
            .iter()
            .map(|node| (node.task_id.clone(), node))
            .collect::<BTreeMap<_, _>>();
        if nodes.len() != self.nodes.len() {
            return Err(DomainError::DuplicateTaskId(
                self.nodes.first().expect("nonempty nodes").task_id.clone(),
            ));
        }
        for node in &self.nodes {
            if node.objective.trim().is_empty() || node.priority > 100 {
                return Err(DomainError::InvalidBudget {
                    field: "workflow_graph.node",
                });
            }
            node.budget.validate()?;
            node.retry.validate()?;
            if node
                .dependencies
                .iter()
                .any(|dependency| !nodes.contains_key(dependency))
            {
                return Err(DomainError::UnknownDependency {
                    task: node.task_id.clone(),
                    dependency: node
                        .dependencies
                        .first()
                        .expect("dependency exists")
                        .clone(),
                });
            }
        }

        fn visit(
            node_id: &TaskId,
            nodes: &BTreeMap<TaskId, &WorkflowNode>,
            states: &mut BTreeMap<TaskId, u8>,
        ) -> Result<(), DomainError> {
            match states.get(node_id).copied() {
                Some(1) => return Err(DomainError::CyclicPlan),
                Some(2) => return Ok(()),
                _ => {}
            }
            states.insert(node_id.clone(), 1);
            for dependency in &nodes[node_id].dependencies {
                visit(dependency, nodes, states)?;
            }
            states.insert(node_id.clone(), 2);
            Ok(())
        }

        let mut states = BTreeMap::new();
        for node_id in nodes.keys() {
            visit(node_id, &nodes, &mut states)?;
        }
        Ok(())
    }
}
