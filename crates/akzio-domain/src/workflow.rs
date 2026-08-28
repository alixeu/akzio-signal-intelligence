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
    /// Bounds mirror `ResearchIntent::validate`. An `EvidenceNeed` is the
    /// lowered form of an intent, so a need built directly by Rust or proposed
    /// by a model may not widen the source vocabulary, the resource length, or
    /// the freshness window the evidence runtime will accept.
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_SCHEMA_VERSION
            || self.source_family.trim().is_empty()
            || self.resource.trim().is_empty()
            || self.resource.chars().count() > 2_048
            || !(1..=86_400 * 7).contains(&self.max_age_secs)
        {
            return Err(DomainError::EmptyField {
                field: "evidence_need",
            });
        }
        if !matches!(
            self.source_family.as_str(),
            "alpaca" | "sec_edgar" | "fred" | "news_web"
        ) {
            return Err(DomainError::EvidenceSourceNotAllowed(
                self.source_family.clone(),
            ));
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

/// Calendar lookback for the daily-bar snapshot. It must stay wide enough that
/// `PAPER_BARS_LIMIT` sessions actually exist inside the window; roughly 252
/// trading days fall in 366 calendar days, so 400 leaves headroom for holidays.
pub const PAPER_BARS_LOOKBACK_DAYS: i64 = 400;
/// Daily bars every Paper analyst shard is entitled to. The planner may not
/// lower it, so a shard can always compute a one-year structure.
pub const PAPER_BARS_LIMIT: u16 = 252;
const PAPER_NEWS_LOOKBACK_DAYS: i64 = 14;
const PAPER_MACRO_LOOKBACK_DAYS: i64 = 366;
const PAPER_FRED_SERIES: [&str; 3] = ["DFF", "DFII10", "VIXCLS"];
const PAPER_NEED_MAX_AGE_SECS: u64 = 7 * 24 * 60 * 60;
const PAPER_BROKER_NEED_MAX_AGE_SECS: u64 = 300;

/// The complete evidence vocabulary of one Paper session, keyed by its broker
/// session date.
///
/// This is domain policy, not scheduling mechanics: the scheduler mints these
/// needs, the planner normalizes model-declared needs against the same bounds,
/// and dispatch re-derives the set to validate a task's granted inputs. All
/// three must agree, so the vocabulary lives here rather than in any one of
/// them.
pub fn paper_session_evidence_needs(session_key: &str) -> Vec<EvidenceNeed> {
    let lookback = |days: i64| {
        chrono::NaiveDate::parse_from_str(session_key, "%Y-%m-%d")
            .ok()
            .and_then(|date| date.checked_sub_signed(chrono::Duration::days(days)))
            .map(|date| date.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| session_key.to_owned())
    };
    let bars_start = lookback(PAPER_BARS_LOOKBACK_DAYS);
    let news_start = lookback(PAPER_NEWS_LOOKBACK_DAYS);
    let macro_start = lookback(PAPER_MACRO_LOOKBACK_DAYS);
    let mut resources = vec![
        "paper.account".to_owned(),
        "paper.positions".to_owned(),
        "paper.open_orders".to_owned(),
        format!("paper.fills:{session_key}"),
        "paper.quotes".to_owned(),
        "paper.clock".to_owned(),
    ];
    resources.extend(
        crate::Asset::EXECUTABLE
            .into_iter()
            .map(|asset| format!("bars:{}:1d:{bars_start}:{PAPER_BARS_LIMIT}", asset.symbol())),
    );
    resources.extend(
        crate::Asset::EXECUTABLE
            .into_iter()
            .map(|asset| format!("news:{}:{news_start}:{session_key}:market", asset.symbol())),
    );
    resources.extend(
        PAPER_FRED_SERIES
            .into_iter()
            .map(|series| format!("series:{series}:{macro_start}:{session_key}")),
    );
    resources
        .into_iter()
        .map(|resource| EvidenceNeed {
            schema_version: V2_SCHEMA_VERSION,
            source_family: paper_need_source_family(&resource).to_owned(),
            max_age_secs: if resource.starts_with("paper.") {
                PAPER_BROKER_NEED_MAX_AGE_SECS
            } else {
                PAPER_NEED_MAX_AGE_SECS
            },
            resource,
        })
        .collect()
}

fn paper_need_source_family(resource: &str) -> &'static str {
    if resource.starts_with("bars:") || resource.starts_with("paper.") {
        "alpaca"
    } else if resource.starts_with("news:") {
        "news_web"
    } else {
        "fred"
    }
}

/// Raise any model-declared daily-bar need up to `PAPER_BARS_LIMIT`. A planner
/// may widen a window but never shrink the entitlement below what a Paper shard
/// needs, so this is a floor rather than a rejection.
pub fn normalize_paper_bars_limit(needs: &mut [EvidenceNeed]) {
    for need in needs {
        if need.source_family != "alpaca" {
            continue;
        }
        let mut parts = need.resource.split(':').collect::<Vec<_>>();
        if parts.len() != 5 || parts[0] != "bars" || parts[2] != "1d" {
            continue;
        }
        let Ok(limit) = parts[4].parse::<u16>() else {
            continue;
        };
        if limit < PAPER_BARS_LIMIT {
            let floor = PAPER_BARS_LIMIT.to_string();
            parts[4] = &floor;
            need.resource = parts.join(":");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{EvidenceNeed, V2_SCHEMA_VERSION};
    use crate::DomainError;

    fn need(source_family: &str, max_age_secs: u64) -> EvidenceNeed {
        EvidenceNeed {
            schema_version: V2_SCHEMA_VERSION,
            source_family: source_family.to_owned(),
            resource: "bars:TQQQ:1d".to_owned(),
            max_age_secs,
        }
    }

    #[test]
    fn evidence_need_bounds_match_the_lowered_research_intent() {
        need("alpaca", 86_400 * 7).validate().unwrap();
        assert!(matches!(
            need("alpaca", 86_400 * 7 + 1).validate(),
            Err(DomainError::EmptyField {
                field: "evidence_need"
            })
        ));
        assert!(matches!(
            need("alpaca", 0).validate(),
            Err(DomainError::EmptyField {
                field: "evidence_need"
            })
        ));
        assert!(matches!(
            need("observatory", 300).validate(),
            Err(DomainError::EvidenceSourceNotAllowed(family)) if family == "observatory"
        ));
        let mut oversized = need("news_web", 300);
        oversized.resource = "n".repeat(2_049);
        assert!(matches!(
            oversized.validate(),
            Err(DomainError::EmptyField {
                field: "evidence_need"
            })
        ));
    }

    #[test]
    fn paper_session_needs_are_valid_and_cover_every_shard_domain() {
        let needs = super::paper_session_evidence_needs("2026-08-28");
        for need in &needs {
            need.validate().unwrap();
        }
        assert_eq!(needs.len(), 6 + 4 + 4 + 3);
        // One daily-bar entitlement per executable asset, at the shared floor.
        for asset in crate::Asset::EXECUTABLE {
            let resource = format!("bars:{}:1d:2025-07-24:252", asset.symbol());
            assert!(
                needs.iter().any(|need| need.resource == resource),
                "missing {resource}"
            );
        }
        assert!(needs
            .iter()
            .any(|need| need.resource == "paper.fills:2026-08-28"));
        assert!(needs.iter().any(
            |need| need.resource == "series:VIXCLS:2025-08-27:2026-08-28"
                && need.source_family == "fred"
        ));
        assert!(needs
            .iter()
            .filter(|need| need.resource.starts_with("paper."))
            .all(|need| need.max_age_secs == 300));
    }

    #[test]
    fn paper_bars_lookback_window_can_hold_the_bar_limit() {
        // Roughly 252 of 366 calendar days are trading sessions. The lookback
        // must exceed that ratio or the minted need is unsatisfiable.
        let sessions_in_window = super::PAPER_BARS_LOOKBACK_DAYS * 252 / 366;
        assert!(
            sessions_in_window >= i64::from(super::PAPER_BARS_LIMIT),
            "{sessions_in_window} sessions cannot supply {} bars",
            super::PAPER_BARS_LIMIT
        );
    }

    #[test]
    fn normalize_raises_a_short_bars_limit_and_leaves_others_alone() {
        let mut needs = vec![
            EvidenceNeed {
                schema_version: V2_SCHEMA_VERSION,
                source_family: "alpaca".to_owned(),
                resource: "bars:TQQQ:1d:2026-01-01:30".to_owned(),
                max_age_secs: 300,
            },
            EvidenceNeed {
                schema_version: V2_SCHEMA_VERSION,
                source_family: "alpaca".to_owned(),
                resource: "bars:QQQ:1d:2026-01-01:400".to_owned(),
                max_age_secs: 300,
            },
            EvidenceNeed {
                schema_version: V2_SCHEMA_VERSION,
                source_family: "news_web".to_owned(),
                resource: "news:QQQ:2026-01-01:2026-01-02:market".to_owned(),
                max_age_secs: 300,
            },
        ];
        super::normalize_paper_bars_limit(&mut needs);
        assert_eq!(needs[0].resource, "bars:TQQQ:1d:2026-01-01:252");
        assert_eq!(needs[1].resource, "bars:QQQ:1d:2026-01-01:400");
        assert_eq!(needs[2].resource, "news:QQQ:2026-01-01:2026-01-02:market");
    }
}
