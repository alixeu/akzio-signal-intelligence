//! Planner proposal and compiled workflow vocabulary.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::artifact::{ArtifactKind, ArtifactRef};
use crate::contract::{TaskRecipe, TaskRecipeId};
use crate::schema::SCHEMA_VERSION;
use crate::{
    ContentHash, DomainError, FailureDisposition, ResearchIntent, ResearchShard, RetryPolicy,
    RunPurpose, TaskBudget, TaskId,
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
        if self.schema_version != SCHEMA_VERSION
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
        if self.schema_version != SCHEMA_VERSION || self.topology_id.trim().is_empty() {
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

        validate_proposal_acyclic(&self.tasks, |task| &task.depends_on)
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
        if self.schema_version != SCHEMA_VERSION || self.topology_id.trim().is_empty() {
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

        validate_proposal_acyclic(&self.tasks, |task| &task.depends_on)
    }
}

// Each proposal validates unknown dependencies before entering this shared DFS.
fn validate_proposal_acyclic<T>(
    tasks: &BTreeMap<String, T>,
    dependencies: fn(&T) -> &[String],
) -> Result<(), DomainError> {
    fn visit<T>(
        alias: &str,
        tasks: &BTreeMap<String, T>,
        dependencies: fn(&T) -> &[String],
        states: &mut BTreeMap<String, u8>,
    ) -> Result<(), DomainError> {
        match states.get(alias).copied() {
            Some(1) => return Err(DomainError::CyclicPlan),
            Some(2) => return Ok(()),
            _ => {}
        }
        states.insert(alias.to_owned(), 1);
        for dependency in dependencies(&tasks[alias]) {
            visit(dependency, tasks, dependencies, states)?;
        }
        states.insert(alias.to_owned(), 2);
        Ok(())
    }

    let mut states = BTreeMap::new();
    for alias in tasks.keys() {
        visit(alias, tasks, dependencies, &mut states)?;
    }
    Ok(())
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
        if self.schema_version != SCHEMA_VERSION || self.topology_id.trim().is_empty() {
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
    // FRED realtime periods are date-granular. Use the previous calendar date
    // so a same-day revision cannot cross the Paper decision cutoff.
    let macro_vintage = lookback(1);
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
            .map(|series| format!("series:{series}:{macro_start}:{session_key}:{macro_vintage}")),
    );
    let mut needs = resources
        .into_iter()
        .map(|resource| EvidenceNeed {
            schema_version: SCHEMA_VERSION,
            source_family: paper_need_source_family(&resource).to_owned(),
            max_age_secs: if resource.starts_with("paper.") {
                PAPER_BROKER_NEED_MAX_AGE_SECS
            } else {
                PAPER_NEED_MAX_AGE_SECS
            },
            resource,
        })
        .collect::<BTreeSet<_>>();
    needs.extend(crate::instrument_evidence_needs_for_session(session_key));
    needs.into_iter().collect()
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

/// Whether Rust performs its own independent source-document acquisition after
/// a provider-mediated search, or accepts provider attribution alone.
///
/// This is acquisition policy for provider-mediated evidence. A direct API
/// transport is already its own independent source, so those families always
/// report `VerifiedSource`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceAcquisitionMode {
    /// One provider search, no independent fetch. The result may carry provider
    /// attribution only and can never present itself as a source document.
    DiscoveryOnly,
    /// One provider search followed by an independent Rust fetch of every cited
    /// source, exact quote binding, and per-source closure accounting.
    VerifiedSource,
}

impl EvidenceAcquisitionMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DiscoveryOnly => "discovery_only",
            Self::VerifiedSource => "verified_source",
        }
    }

    pub const fn requires_independent_fetch(self) -> bool {
        matches!(self, Self::VerifiedSource)
    }
}

/// Identity of the acquisition mapping below. It is persisted next to acquired
/// evidence so a later canonical consumer can detect that the policy changed
/// instead of silently re-deriving a mode with newer code.
pub const EVIDENCE_ACQUISITION_POLICY_VERSION: u32 = 2;

/// Rust-owned acquisition policy. Neither model output, citation JSON, nor tool
/// arguments participate: the mode is a function of the run purpose and the
/// committed `EvidenceNeed` alone, so a model cannot promote `DiscoveryOnly`
/// into `VerifiedSource`.
pub fn evidence_acquisition_mode(
    purpose: RunPurpose,
    need: &EvidenceNeed,
) -> EvidenceAcquisitionMode {
    if need.source_family != "news_web" {
        return EvidenceAcquisitionMode::VerifiedSource;
    }
    match purpose {
        RunPurpose::Paper => EvidenceAcquisitionMode::VerifiedSource,
        RunPurpose::Debug
        | RunPurpose::PositionPlan
        | RunPurpose::PaperDryRun
        | RunPurpose::Replay
        | RunPurpose::Shadow => EvidenceAcquisitionMode::DiscoveryOnly,
    }
}

/// Hash the policy version together with the complete purpose-by-family
/// mapping it produces, so any change to `evidence_acquisition_mode` changes
/// the recorded identity.
pub fn evidence_acquisition_policy_hash() -> ContentHash {
    const PURPOSES: [RunPurpose; 6] = [
        RunPurpose::Debug,
        RunPurpose::PositionPlan,
        RunPurpose::Paper,
        RunPurpose::PaperDryRun,
        RunPurpose::Replay,
        RunPurpose::Shadow,
    ];
    let mut identity =
        format!("akzio.evidence_acquisition_policy:v{EVIDENCE_ACQUISITION_POLICY_VERSION}");
    for family in crate::GOVERNED_EVIDENCE_SOURCE_FAMILIES {
        let need = EvidenceNeed {
            schema_version: SCHEMA_VERSION,
            source_family: family.to_owned(),
            resource: "policy.probe".to_owned(),
            max_age_secs: 1,
        };
        for purpose in PURPOSES {
            identity.push_str(&format!(
                "|{family}:{purpose:?}={}",
                evidence_acquisition_mode(purpose, &need).as_str()
            ));
        }
    }
    ContentHash::of_bytes(identity.as_bytes())
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

/// Acquisition failure policy is independent of how many needs are requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceCriticality {
    ExecutionSafety,
    DirectionalResearch,
    Enhancement,
}
impl EvidenceNeed {
    pub fn criticality(&self) -> EvidenceCriticality {
        if self.source_family == "alpaca" && self.resource.starts_with("paper.") {
            EvidenceCriticality::ExecutionSafety
        } else if self.resource.starts_with("bars:")
            || self.resource.starts_with("news:")
            || self.resource.starts_with("series:")
            || !crate::instrument_evidence_keys_for_resource(&self.resource).is_empty()
        {
            EvidenceCriticality::DirectionalResearch
        } else {
            EvidenceCriticality::Enhancement
        }
    }
}
