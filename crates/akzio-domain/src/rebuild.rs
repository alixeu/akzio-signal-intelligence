//! Rebuilt v2 vocabulary.
//!
//! This module is intentionally introduced beside the former vocabulary while the
//! workspace is migrated. Its types are the only types new runtime code may use;
//! the old document/role/task types are removed once every crate crosses this seam.

use std::{collections::{BTreeMap, BTreeSet}, fmt};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    content_hash_json, BlobRef, ContentHash, ContractId, DocumentLifecycle, DomainError,
    FailureDisposition, LeaseId, RetryPolicy, RunId, TaskBudget, TaskId, TerminationPolicy,
    ToolGrant,
};

/// A Store Root with this schema is intentionally incompatible with the previous
/// v2 database. It is a rebuild, not a migration layer.
pub const REBUILD_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ArtifactId(pub ContentHash);

impl ArtifactId {
    pub fn as_hash(&self) -> &ContentHash {
        &self.0
    }
}

impl fmt::Display for ArtifactId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    RawEvidence,
    NormalizedEvidence,
    SemanticDetail,
    ContextManifest,
    ContextRepair,
    EvidenceNeed,
    Contract,
    WorkflowProposal,
    WorkflowGraph,
    AgentTurn,
    ToolCall,
    ToolResult,
    Claim,
    Critique,
    DecisionProposal,
    DecisionContext,
    Decision,
    ExecutionContext,
    ExecutionVerdict,
    ExecutionPlan,
    ExecutionCommitment,
    OrderReceipt,
    Experience,
    Outcome,
    Evaluation,
    CandidatePolicy,
    FreezeState,
}

impl ArtifactKind {
    pub const fn is_evidence(self) -> bool {
        matches!(
            self,
            Self::RawEvidence | Self::NormalizedEvidence | Self::SemanticDetail
        )
    }

    pub const fn can_be_canonical(self) -> bool {
        !matches!(self, Self::AgentTurn | Self::ToolCall | Self::ToolResult)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactLifecycle {
    Ephemeral,
    RunScoped,
    Canonical,
}

impl ArtifactLifecycle {
    pub const fn from_document(lifecycle: DocumentLifecycle) -> Self {
        match lifecycle {
            DocumentLifecycle::Ephemeral => Self::Ephemeral,
            DocumentLifecycle::RunScoped => Self::RunScoped,
            DocumentLifecycle::Canonical => Self::Canonical,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub artifact_id: ArtifactId,
    pub kind: ArtifactKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactProvenance {
    /// Rust-owned adapter/source family, never a model-provided URL or provider.
    pub source_family: String,
    pub observed_at: Option<DateTime<Utc>>,
    pub retrieved_at: DateTime<Utc>,
    pub source_uri: Option<String>,
    pub confidence_ppm: u32,
    pub producer_contract_hash: Option<ContentHash>,
}

impl ArtifactProvenance {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.source_family.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "artifact.provenance.source_family",
            });
        }
        if self.confidence_ppm > 1_000_000 {
            return Err(DomainError::InvalidBudget {
                field: "artifact.provenance.confidence_ppm",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactOrigin {
    pub run_id: Option<RunId>,
    pub task_id: Option<TaskId>,
    pub attempt_id: Option<crate::AttemptId>,
    pub contract_hash: Option<ContentHash>,
}

impl ArtifactOrigin {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.attempt_id.is_some() && self.task_id.is_none() {
            return Err(DomainError::AttemptOriginWithoutTask);
        }
        Ok(())
    }
}

/// Immutable typed metadata for a CAS blob. The identity covers the metadata and
/// payload reference, therefore a caller cannot substitute provenance under an
/// existing artifact ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Artifact {
    pub schema_version: u32,
    pub artifact_id: ArtifactId,
    pub kind: ArtifactKind,
    pub blob: BlobRef,
    pub producer: String,
    pub lifecycle: ArtifactLifecycle,
    pub provenance: ArtifactProvenance,
    pub origin: Option<ArtifactOrigin>,
    pub source_refs: Vec<ArtifactRef>,
    pub created_at: DateTime<Utc>,
}

impl Artifact {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        kind: ArtifactKind,
        blob: BlobRef,
        producer: impl Into<String>,
        lifecycle: ArtifactLifecycle,
        provenance: ArtifactProvenance,
        origin: Option<ArtifactOrigin>,
        source_refs: Vec<ArtifactRef>,
        created_at: DateTime<Utc>,
    ) -> Result<Self, DomainError> {
        let producer = producer.into();
        let mut artifact = Self {
            schema_version: REBUILD_SCHEMA_VERSION,
            artifact_id: ArtifactId(ContentHash::of_bytes(b"uninitialized artifact")),
            kind,
            blob,
            producer,
            lifecycle,
            provenance,
            origin,
            source_refs,
            created_at,
        };
        artifact.artifact_id = ArtifactId(artifact.expected_hash()?);
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn expected_hash(&self) -> Result<ContentHash, DomainError> {
        let mut value = serde_json::to_value(self)
            .map_err(|_| DomainError::EmptyField { field: "artifact.serialize" })?;
        value
            .as_object_mut()
            .expect("artifact serializes to object")
            .remove("artifact_id");
        content_hash_json(&value)
            .map_err(|_| DomainError::EmptyField { field: "artifact.serialize" })
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != REBUILD_SCHEMA_VERSION {
            return Err(DomainError::EmptyField {
                field: "artifact.schema_version",
            });
        }
        if self.producer.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "artifact.producer",
            });
        }
        self.blob.validate()?;
        self.provenance.validate()?;
        if let Some(origin) = &self.origin {
            origin.validate()?;
        }
        if self.lifecycle == ArtifactLifecycle::Canonical && !self.kind.can_be_canonical() {
            return Err(DomainError::EmptyField {
                field: "artifact.canonical_kind",
            });
        }
        if self.artifact_id.0 != self.expected_hash()? {
            return Err(DomainError::InvalidContentHash);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContractPurpose(String);

impl ContractPurpose {
    pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "contract.purpose",
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextPolicy {
    pub permitted_kinds: BTreeSet<ArtifactKind>,
    pub permitted_source_families: BTreeSet<String>,
    pub max_artifacts: u16,
    pub max_bytes: u64,
    pub max_tokens: u32,
    pub allow_raw_reread: bool,
}

impl ContextPolicy {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.permitted_kinds.is_empty()
            || self.max_artifacts == 0
            || self.max_bytes == 0
            || self.max_tokens == 0
        {
            return Err(DomainError::InvalidBudget {
                field: "contract.context_policy",
            });
        }
        if self.permitted_kinds.contains(&ArtifactKind::RawEvidence) {
            return Err(DomainError::RawEvidenceDirectContext);
        }
        if self
            .permitted_source_families
            .iter()
            .any(|family| family.trim().is_empty())
        {
            return Err(DomainError::EmptyField {
                field: "contract.context_policy.permitted_source_families",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputContract {
    pub artifact_kind: ArtifactKind,
    pub schema: BlobRef,
}

impl OutputContract {
    fn validate(&self) -> Result<(), DomainError> {
        self.schema.validate()
    }
}

/// One canonical definition drives prompt, runtime access, schema validation,
/// retry/termination limits, and task policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractSpec {
    pub schema_version: u32,
    pub contract_id: ContractId,
    pub version: u32,
    pub purpose: ContractPurpose,
    pub responsibility: String,
    pub prompt: BlobRef,
    pub context: ContextPolicy,
    pub tool_grants: Vec<ToolGrant>,
    pub output: OutputContract,
    pub budget: TaskBudget,
    pub retry: RetryPolicy,
    pub termination: TerminationPolicy,
    pub on_failure: FailureDisposition,
    pub contract_hash: ContentHash,
}

impl ContractSpec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        contract_id: ContractId,
        version: u32,
        purpose: ContractPurpose,
        responsibility: impl Into<String>,
        prompt: BlobRef,
        context: ContextPolicy,
        tool_grants: Vec<ToolGrant>,
        output: OutputContract,
        budget: TaskBudget,
        retry: RetryPolicy,
        termination: TerminationPolicy,
        on_failure: FailureDisposition,
    ) -> Result<Self, DomainError> {
        let responsibility = responsibility.into();
        let mut contract = Self {
            schema_version: REBUILD_SCHEMA_VERSION,
            contract_id,
            version,
            purpose,
            responsibility,
            prompt,
            context,
            tool_grants,
            output,
            budget,
            retry,
            termination,
            on_failure,
            contract_hash: ContentHash::of_bytes(b"uninitialized contract"),
        };
        contract.contract_hash = contract.expected_hash()?;
        contract.validate()?;
        Ok(contract)
    }

    pub fn expected_hash(&self) -> Result<ContentHash, DomainError> {
        let mut value = serde_json::to_value(self)
            .map_err(|_| DomainError::EmptyField { field: "contract.serialize" })?;
        value
            .as_object_mut()
            .expect("contract serializes to object")
            .remove("contract_hash");
        content_hash_json(&value)
            .map_err(|_| DomainError::EmptyField { field: "contract.serialize" })
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != REBUILD_SCHEMA_VERSION {
            return Err(DomainError::EmptyField {
                field: "contract.schema_version",
            });
        }
        if self.version == 0 || self.responsibility.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "contract.identity",
            });
        }
        self.prompt.validate()?;
        self.context.validate()?;
        self.output.validate()?;
        self.budget.validate()?;
        self.retry.validate()?;
        if self.contract_hash != self.expected_hash()? {
            return Err(DomainError::InvalidContentHash);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskRecipeId(String);

impl TaskRecipeId {
    pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "task_recipe.id",
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TaskRecipeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRecipe {
    pub recipe_id: TaskRecipeId,
    pub purpose: ContractPurpose,
    pub contract_hash: Option<ContentHash>,
    pub task_class: RuntimeTaskClass,
    pub max_children: u16,
    pub max_depth: u16,
    pub priority_ceiling: u8,
    pub budget: TaskBudget,
    pub retry: RetryPolicy,
    pub on_failure: FailureDisposition,
}

impl TaskRecipe {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.priority_ceiling > 100 {
            return Err(DomainError::InvalidBudget {
                field: "task_recipe.priority_ceiling",
            });
        }
        self.budget.validate()?;
        self.retry.validate()?;
        if self.task_class == RuntimeTaskClass::Agent && self.contract_hash.is_none() {
            return Err(DomainError::EmptyField {
                field: "task_recipe.contract_hash",
            });
        }
        Ok(())
    }
}

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
    pub fn validate(&self, recipes: &BTreeMap<TaskRecipeId, TaskRecipe>) -> Result<(), DomainError> {
        if self.schema_version != REBUILD_SCHEMA_VERSION || self.topology_id.trim().is_empty() {
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
            let recipe = recipes.get(&task.recipe_id).ok_or(DomainError::EmptyField {
                field: "workflow_proposal.recipe",
            })?;
            if task.priority > recipe.priority_ceiling {
                return Err(DomainError::InvalidBudget {
                    field: "workflow_proposal.priority",
                });
            }
            if task.depends_on.iter().any(|dependency| !self.tasks.contains_key(dependency)) {
                return Err(DomainError::UnknownDependency {
                    task: TaskId(alias.clone()),
                    dependency: TaskId("proposal alias".to_owned()),
                });
            }
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
        if self.schema_version != REBUILD_SCHEMA_VERSION || self.topology_id.trim().is_empty() {
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
                self.nodes
                    .first()
                    .expect("nonempty nodes")
                    .task_id
                    .clone(),
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSelection {
    pub artifact: ArtifactRef,
    pub reason: String,
    pub estimated_tokens: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextManifestPayload {
    pub schema_version: u32,
    pub contract_hash: ContentHash,
    pub selections: Vec<ContextSelection>,
    pub total_bytes: u64,
    pub estimated_tokens: u32,
    pub input_hash: ContentHash,
}

impl ContextManifestPayload {
    pub fn validate(&self, policy: &ContextPolicy) -> Result<(), DomainError> {
        if self.schema_version != REBUILD_SCHEMA_VERSION
            || self.selections.is_empty()
            || self.selections.len() > usize::from(policy.max_artifacts)
            || self.total_bytes > policy.max_bytes
            || self.estimated_tokens > policy.max_tokens
        {
            return Err(DomainError::InvalidBudget {
                field: "context_manifest",
            });
        }
        if self.selections.iter().any(|selection| {
            selection.reason.trim().is_empty()
                || selection.estimated_tokens == 0
                || !policy.permitted_kinds.contains(&selection.artifact.kind)
        }) {
            return Err(DomainError::EmptyField {
                field: "context_manifest.selection",
            });
        }
        Ok(())
    }
}

/// Ephemeral, task-scoped authorization derived from a persisted manifest. It is
/// never model-produced and never grants a broader artifact surface than the
/// manifest selection/closure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextGrant {
    pub manifest_artifact_id: ArtifactId,
    pub contract_hash: ContentHash,
    pub readable: BTreeSet<ArtifactId>,
    pub raw_source_closure: BTreeSet<ArtifactId>,
    pub expires_at: DateTime<Utc>,
}

impl ContextGrant {
    pub fn permits(&self, artifact_id: &ArtifactId, raw: bool, now: DateTime<Utc>) -> bool {
        now <= self.expires_at
            && if raw {
                self.raw_source_closure.contains(artifact_id)
            } else {
                self.readable.contains(artifact_id)
            }
    }
}

/// Authorizes exactly one running attempt to create an artifact or commit a result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskWritePermit {
    pub run_id: RunId,
    pub task_id: TaskId,
    pub attempt_id: crate::AttemptId,
    pub lease_id: LeaseId,
    pub epoch: u64,
    pub contract_hash: Option<ContentHash>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockerSeverity {
    Hard,
    Soft,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionBlocker {
    pub code: String,
    pub severity: BlockerSeverity,
    pub scope: String,
    pub explanation: String,
    pub source_refs: Vec<ArtifactRef>,
}

impl DecisionBlocker {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.code.trim().is_empty()
            || self.scope.trim().is_empty()
            || self.explanation.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "decision_blocker",
            });
        }
        if self.severity == BlockerSeverity::Hard && self.source_refs.is_empty() {
            return Err(DomainError::EmptyField {
                field: "decision_blocker.source_refs",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorLimits {
    pub global_leveraged_equity_ppm: u32,
    pub nasdaq_ppm: u32,
    pub semiconductor_ppm: u32,
    pub paired_index_ppm: u32,
}

impl FactorLimits {
    pub fn validate(&self) -> Result<(), DomainError> {
        if [
            self.global_leveraged_equity_ppm,
            self.nasdaq_ppm,
            self.semiconductor_ppm,
            self.paired_index_ppm,
        ]
        .into_iter()
        .any(|value| value > 1_000_000)
        {
            return Err(DomainError::InvalidBudget {
                field: "factor_limits",
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::{AttemptId, ToolKind};

    fn blob(value: &[u8]) -> BlobRef {
        BlobRef {
            hash: ContentHash::of_bytes(value),
            media_type: "application/json".to_owned(),
            bytes: value.len() as u64,
        }
    }

    fn provenance() -> ArtifactProvenance {
        ArtifactProvenance {
            source_family: "fixture.market".to_owned(),
            observed_at: None,
            retrieved_at: Utc::now(),
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        }
    }

    #[test]
    fn artifact_identity_commits_metadata_and_payload() {
        let artifact = Artifact::new(
            ArtifactKind::NormalizedEvidence,
            blob(b"payload"),
            "fixture",
            ArtifactLifecycle::RunScoped,
            provenance(),
            None,
            vec![],
            Utc::now(),
        )
        .unwrap();
        artifact.validate().unwrap();

        let mut substituted = artifact.clone();
        substituted.producer = "different".to_owned();
        assert_eq!(substituted.validate(), Err(DomainError::InvalidContentHash));
    }

    #[test]
    fn contract_hash_rejects_prompt_or_grant_substitution() {
        let contract = ContractSpec::new(
            ContractId::new(),
            1,
            ContractPurpose::new("research.analyst").unwrap(),
            "derive claims",
            blob(b"prompt"),
            ContextPolicy {
                permitted_kinds: BTreeSet::from([ArtifactKind::NormalizedEvidence]),
                permitted_source_families: BTreeSet::from(["market".to_owned()]),
                max_artifacts: 4,
                max_bytes: 1024,
                max_tokens: 256,
                allow_raw_reread: true,
            },
            vec![ToolGrant {
                kind: ToolKind::ReadEvidence,
                allowed_sources: vec!["market".to_owned()],
            }],
            OutputContract {
                artifact_kind: ArtifactKind::Claim,
                schema: blob(b"schema"),
            },
            TaskBudget {
                max_input_tokens: 256,
                max_output_tokens: 128,
                max_wall_time_secs: 30,
                max_tool_calls: 2,
            },
            RetryPolicy {
                max_attempts: 1,
                initial_backoff_ms: 1,
                retry_transport: true,
                retry_rate_limited: true,
                retry_invalid_output: false,
            },
            TerminationPolicy::leaf(),
            FailureDisposition::FailTask,
        )
        .unwrap();
        contract.validate().unwrap();

        let mut substituted = contract.clone();
        substituted.prompt = blob(b"different prompt");
        assert_eq!(substituted.validate(), Err(DomainError::InvalidContentHash));
    }

    #[test]
    fn hard_blocker_requires_evidence() {
        let blocker = DecisionBlocker {
            code: "stale_quote".to_owned(),
            severity: BlockerSeverity::Hard,
            scope: "TQQQ".to_owned(),
            explanation: "quote expired".to_owned(),
            source_refs: vec![],
        };
        assert!(blocker.validate().is_err());
    }

    #[test]
    fn write_permit_is_attempt_specific() {
        let permit = TaskWritePermit {
            run_id: RunId::new(),
            task_id: TaskId::new(),
            attempt_id: AttemptId::new(),
            lease_id: LeaseId::new(),
            epoch: 1,
            contract_hash: None,
        };
        assert_ne!(permit.attempt_id.0, AttemptId::new().0);
    }
}
