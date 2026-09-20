//! Frozen workflow control metadata. Prose is never an execution authority.
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    content_hash_json, ContentHash, DecisionHorizon, DomainError, RunPurpose, TaskBudget, TaskId,
    WorkflowGraph, WorkflowNode,
};

pub const WORKFLOW_DEFINITION_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSpec {
    pub key: String,
    pub horizon: Option<DecisionHorizon>,
    pub research_round: Option<u8>,
    pub proposal_revision: Option<u8>,
}

impl NodeSpec {
    pub fn horizon_name(&self) -> Option<&'static str> {
        self.horizon.map(|h| match h {
            DecisionHorizon::T1 => "t1",
            DecisionHorizon::T3 => "t3",
            DecisionHorizon::T5 => "t5",
        })
    }

    pub fn stage(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            horizon: None,
            research_round: None,
            proposal_revision: None,
        }
    }

    pub fn validate(&self, recipe: &str) -> Result<(), DomainError> {
        let research_pair = matches!(
            recipe,
            crate::RESEARCH_ANALYST_RECIPE_ID | crate::RESEARCH_CRITIC_RECIPE_ID
        );
        let proposal = matches!(
            recipe,
            crate::RESEARCH_SYNTHESIZER_RECIPE_ID | crate::RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID
        );
        if self.key.is_empty()
            || !self
                .key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
            || (research_pair
                && (self.horizon.is_none() || !matches!(self.research_round, Some(0 | 1))))
            || (!research_pair && self.research_round.is_some())
            || (proposal != self.proposal_revision.is_some())
            || self.proposal_revision.is_some_and(|r| r > 5)
            || (!research_pair
                && recipe != crate::LEARNING_OUTCOME_WORKER_RECIPE_ID
                && self.horizon.is_some())
        {
            return Err(DomainError::EmptyField {
                field: "workflow.node_spec",
            });
        }
        Ok(())
    }
}

/// Read adapter for already frozen historical nodes. New definitions always
/// carry NodeSpec; callers must never persist this inferred value over old CAS.
pub fn historical_node_spec(key: &str, objective: &str) -> NodeSpec {
    fn marker<'a>(text: &'a str, name: &str) -> Option<&'a str> {
        text.split_once(&format!("[{name}="))?
            .1
            .split_once(']')
            .map(|v| v.0)
    }
    let horizon = marker(objective, "research_horizon")
        .or_else(|| marker(objective, "outcome_horizon"))
        .or_else(|| {
            ["t1", "t3", "t5"].into_iter().find(|h| {
                objective.contains(&format!(" {h} Claim"))
                    || objective.contains(&format!(" {h} Critique"))
            })
        });
    NodeSpec {
        key: key.into(),
        horizon: match horizon {
            Some("t1") => Some(DecisionHorizon::T1),
            Some("t3") => Some(DecisionHorizon::T3),
            Some("t5") => Some(DecisionHorizon::T5),
            _ => None,
        },
        research_round: marker(objective, "research_round").and_then(|v| v.parse().ok()),
        proposal_revision: marker(objective, "proposal_revision").and_then(|v| v.parse().ok()),
    }
}

/// Tolerant read projection for partial/legacy export payloads. An invalid
/// explicit spec is unknown; it must not fall back to prose.
pub fn projected_node_spec(node: &serde_json::Value) -> Option<NodeSpec> {
    if let Some(spec) = node.get("spec").filter(|v| !v.is_null()) {
        return serde_json::from_value(spec.clone()).ok();
    }
    Some(historical_node_spec(
        node["task_id"].as_str().unwrap_or("historical"),
        node["objective"].as_str()?,
    ))
}

impl WorkflowNode {
    pub fn execution_spec(&self) -> NodeSpec {
        self.spec
            .clone()
            .unwrap_or_else(|| historical_node_spec(&self.task_id.0, &self.objective))
    }

    /// Render the existing model-facing scope notation from typed authority.
    /// This preserves the research request protocol while stored objective is prose.
    pub fn model_objective(&self) -> String {
        let Some(spec) = &self.spec else {
            return self.objective.clone();
        };
        let mut parts = Vec::new();
        if let Some(horizon) = spec.horizon_name() {
            let key = if self.recipe_id.as_str() == crate::LEARNING_OUTCOME_WORKER_RECIPE_ID {
                "outcome_horizon"
            } else {
                "research_horizon"
            };
            parts.push(format!("[{key}={horizon}]"));
        }
        if let Some(round) = spec.research_round {
            parts.push(format!("[research_round={round}]"));
        }
        if let Some(revision) = spec.proposal_revision {
            parts.push(format!("[proposal_revision={revision}]"));
        }
        parts.push(self.objective.clone());
        parts.join(" ")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowDefinition {
    pub version: u32,
    pub proposal: crate::WorkflowProposal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlueprintNode {
    pub spec: NodeSpec,
    pub recipe_id: String,
    pub contract_hash: Option<ContentHash>,
    pub dependencies: Vec<String>,
    pub parent: Option<String>,
    pub priority: u8,
    pub budget: TaskBudget,
    pub retry: crate::RetryPolicy,
    pub on_failure: crate::FailureDisposition,
}

/// Inspectable definition with no task ids, handler closures or side effects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowBlueprint {
    pub definition_version: Option<u32>,
    pub definition_hash: ContentHash,
    pub topology_id: String,
    pub purpose: RunPurpose,
    pub nodes: Vec<BlueprintNode>,
    pub entry: Vec<String>,
    pub finish: Vec<String>,
}

impl WorkflowGraph {
    pub fn blueprint(&self, purpose: RunPurpose) -> Result<WorkflowBlueprint, DomainError> {
        self.validate()?;
        let keys: BTreeMap<&TaskId, String> = self
            .nodes
            .iter()
            .map(|n| (&n.task_id, n.execution_spec().key))
            .collect();
        let mut nodes = self
            .nodes
            .iter()
            .map(|node| {
                let mut dependencies = node
                    .dependencies
                    .iter()
                    .map(|id| keys[id].clone())
                    .collect::<Vec<_>>();
                dependencies.sort();
                BlueprintNode {
                    spec: node.execution_spec(),
                    recipe_id: node.recipe_id.to_string(),
                    contract_hash: node.contract_hash.clone(),
                    dependencies,
                    parent: node.parent_task_id.as_ref().map(|id| keys[id].clone()),
                    priority: node.priority,
                    budget: node.budget.clone(),
                    retry: node.retry.clone(),
                    on_failure: node.on_failure,
                }
            })
            .collect::<Vec<_>>();
        nodes.sort_by(|a, b| a.spec.key.cmp(&b.spec.key));
        let parents: BTreeSet<&String> = nodes.iter().flat_map(|n| &n.dependencies).collect();
        let entry = nodes
            .iter()
            .filter(|n| n.dependencies.is_empty())
            .map(|n| n.spec.key.clone())
            .collect();
        let finish = nodes
            .iter()
            .filter(|n| !parents.contains(&n.spec.key))
            .map(|n| n.spec.key.clone())
            .collect();
        let definition_hash = content_hash_json(&serde_json::json!([
            self.definition_version,
            purpose,
            &nodes
        ]))
        .map_err(|_| DomainError::EmptyField {
            field: "workflow.definition_hash",
        })?;
        Ok(WorkflowBlueprint {
            definition_version: self.definition_version,
            definition_hash,
            topology_id: self.topology_id.clone(),
            purpose,
            nodes,
            entry,
            finish,
        })
    }
}

impl WorkflowBlueprint {
    pub fn mermaid(&self) -> String {
        let ids: BTreeMap<_, _> = self
            .nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (&n.spec.key, format!("n{i}")))
            .collect();
        let mut result = String::from("flowchart TD\n");
        for node in &self.nodes {
            // Labels use validated logical keys, never user-supplied prose.
            result.push_str(&format!(
                "  {}[\"{}\"]\n",
                ids[&node.spec.key],
                node.spec.key.replace('"', "")
            ));
            for dependency in &node.dependencies {
                result.push_str(&format!(
                    "  {} --> {}\n",
                    ids[dependency], ids[&node.spec.key]
                ));
            }
        }
        result
    }
}
