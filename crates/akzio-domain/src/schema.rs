//! Canonical v2 artifact, contract, workflow, and authority schema.
//!
//! This module is intentionally introduced beside the former vocabulary while the
//! workspace is migrated. Its types are the only types new runtime code may use;
//! the old document/role/task types are removed once every crate crosses this seam.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    content_hash_json, BlobRef, ContentHash, ContractId, DocumentLifecycle, DomainError,
    FailureDisposition, LeaseId, ResearchIntent, ResearchShard, RetryPolicy, RunId, TaskBudget,
    TaskId, TerminationPolicy, ToolGrant, ToolKind,
};

/// A Store Root with this schema is intentionally incompatible with the previous
/// v2 database. It is a fresh schema, not a migration layer.
pub const V2_SCHEMA_VERSION: u32 = 10;

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
    WorkflowProposalDraft,
    WorkflowProposal,
    WorkflowGraph,
    RuntimeManifest,
    PaperLaunchApproval,
    AgentTurn,
    DeliberationNote,
    ToolCall,
    ToolResult,
    Claim,
    Critique,
    Resolution,
    DecisionProposal,
    DecisionContext,
    Decision,
    ExecutionContext,
    ExecutionVerdict,
    ExecutionPlan,
    ExecutionCommitment,
    ExecutionReprice,
    OrderReceipt,
    Reconciliation,
    OutcomeSchedule,
    RetrospectiveDraft,
    Retrospective,
    AttemptRelation,
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
        matches!(
            self,
            Self::RawEvidence
                | Self::NormalizedEvidence
                | Self::SemanticDetail
                | Self::Contract
                | Self::Claim
                | Self::Critique
                | Self::DecisionContext
                | Self::Decision
                | Self::ExecutionContext
                | Self::ExecutionVerdict
                | Self::ExecutionPlan
                | Self::ExecutionCommitment
                | Self::ExecutionReprice
                | Self::OrderReceipt
                | Self::Reconciliation
                | Self::OutcomeSchedule
                | Self::Retrospective
                | Self::Experience
                | Self::Outcome
                | Self::Evaluation
                | Self::CandidatePolicy
                | Self::FreezeState
                | Self::RuntimeManifest
                | Self::PaperLaunchApproval
        )
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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
        let mut source_refs = source_refs;
        source_refs.sort();
        let mut artifact = Self {
            schema_version: V2_SCHEMA_VERSION,
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
        let mut canonical = self.clone();
        canonical.source_refs.sort();
        let mut value = serde_json::to_value(canonical).map_err(|_| DomainError::EmptyField {
            field: "artifact.serialize",
        })?;
        value
            .as_object_mut()
            .expect("artifact serializes to object")
            .remove("artifact_id");
        content_hash_json(&value).map_err(|_| DomainError::EmptyField {
            field: "artifact.serialize",
        })
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_SCHEMA_VERSION {
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
        self.validate_source_refs()?;
        if self.lifecycle == ArtifactLifecycle::Canonical && !self.kind.can_be_canonical() {
            return Err(DomainError::EmptyField {
                field: "artifact.canonical_kind",
            });
        }
        let lifecycle_allowed = match self.kind {
            ArtifactKind::DeliberationNote
            | ArtifactKind::RetrospectiveDraft
            | ArtifactKind::AttemptRelation => self.lifecycle == ArtifactLifecycle::RunScoped,
            ArtifactKind::Retrospective => {
                matches!(
                    self.lifecycle,
                    ArtifactLifecycle::RunScoped | ArtifactLifecycle::Canonical
                )
            }
            _ => true,
        };
        if !lifecycle_allowed {
            return Err(DomainError::EmptyField {
                field: "artifact.lifecycle",
            });
        }
        if self.artifact_id.0 != self.expected_hash()? {
            return Err(DomainError::InvalidContentHash);
        }
        Ok(())
    }

    fn validate_source_refs(&self) -> Result<(), DomainError> {
        if self.source_refs.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(DomainError::EmptyField {
                field: "artifact.source_refs",
            });
        }
        let mut seen = BTreeSet::new();

        if self.source_refs.iter().any(|reference| {
            reference.artifact_id == self.artifact_id || !seen.insert(reference.artifact_id.clone())
        }) {
            return Err(DomainError::EmptyField {
                field: "artifact.source_refs",
            });
        }

        match self.kind {
            ArtifactKind::RawEvidence if !self.source_refs.is_empty() => {
                Err(DomainError::EmptyField {
                    field: "artifact.raw_source_refs",
                })
            }
            ArtifactKind::NormalizedEvidence
                if !self.source_refs.is_empty()
                    && !self
                        .source_refs
                        .iter()
                        .any(|reference| reference.kind == ArtifactKind::RawEvidence) =>
            {
                Err(DomainError::EmptyField {
                    field: "artifact.normalized_source_refs",
                })
            }
            ArtifactKind::SemanticDetail
                if !self.source_refs.is_empty()
                    && !self.source_refs.iter().any(|reference| {
                        matches!(
                            reference.kind,
                            ArtifactKind::RawEvidence | ArtifactKind::NormalizedEvidence
                        )
                    }) =>
            {
                Err(DomainError::EmptyField {
                    field: "artifact.detail_source_refs",
                })
            }
            ArtifactKind::Claim
                if !self.source_refs.iter().any(|reference| {
                    matches!(
                        reference.kind,
                        ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail
                    )
                }) =>
            {
                Err(DomainError::EmptyField {
                    field: "artifact.claim_source_refs",
                })
            }
            ArtifactKind::Critique
                if !self
                    .source_refs
                    .iter()
                    .any(|reference| reference.kind == ArtifactKind::Claim) =>
            {
                Err(DomainError::EmptyField {
                    field: "artifact.critique_source_refs",
                })
            }
            ArtifactKind::Resolution
                if !self
                    .source_refs
                    .iter()
                    .any(|reference| reference.kind == ArtifactKind::Claim)
                    || !self
                        .source_refs
                        .iter()
                        .any(|reference| reference.kind == ArtifactKind::Critique) =>
            {
                Err(DomainError::EmptyField {
                    field: "artifact.resolution_source_refs",
                })
            }
            _ => Ok(()),
        }
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
    /// Minimum selected artifacts required before an Agent task can start.
    /// A zero value is reserved for the bootstrap Planner, which creates the
    /// first governed evidence requests from an intentionally empty context.
    pub min_artifacts: u16,
    pub max_artifacts: u16,
    pub max_bytes: u64,
    pub max_tokens: u32,
    pub allow_raw_reread: bool,
}

impl ContextPolicy {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.permitted_kinds.is_empty()
            || self.min_artifacts > self.max_artifacts
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

/// The maximum context and tool authority an active contract may delegate to
/// a candidate. Candidate contracts may narrow this surface, never widen it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateCapabilityCeiling {
    pub context: ContextPolicy,
    pub tool_grants: Vec<ToolGrant>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputContract {
    pub artifact_kind: ArtifactKind,
    pub schema: BlobRef,
}

/// Versioned model instructions composed from shared governance and a
/// contract-specific role prompt. Both blobs contribute to the Contract hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptBundle {
    pub version: u32,
    pub governance: BlobRef,
    pub role: BlobRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DeliberationPolicy {
    #[default]
    Disabled,
    Required,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliberationSummary {
    pub selected_path: String,
    #[serde(default)]
    pub alternatives: Vec<String>,
    #[serde(default)]
    pub uncertainties: Vec<String>,
    #[serde(default)]
    pub basis_artifact_ids: Vec<ArtifactId>,
    pub confidence_ppm: u32,
}

impl DeliberationSummary {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.selected_path.trim().is_empty()
            || self.selected_path.chars().count() > 1_000
            || self.alternatives.len() > 3
            || self.uncertainties.len() > 3
            || self.basis_artifact_ids.len() > 8
            || self.confidence_ppm > 1_000_000
            || self
                .alternatives
                .iter()
                .any(|item| item.trim().is_empty() || item.chars().count() > 500)
            || self
                .uncertainties
                .iter()
                .any(|item| item.trim().is_empty() || item.chars().count() > 500)
        {
            return Err(DomainError::InvalidBudget {
                field: "deliberation.summary",
            });
        }
        let mut basis = BTreeSet::new();
        if self
            .basis_artifact_ids
            .iter()
            .any(|artifact_id| !basis.insert(artifact_id.clone()))
        {
            return Err(DomainError::EmptyField {
                field: "deliberation.basis_artifact_ids",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentOutputEnvelope {
    pub result: Value,
    pub deliberation: DeliberationSummary,
}

impl PromptBundle {
    fn validate(&self) -> Result<(), DomainError> {
        if self.version == 0 {
            return Err(DomainError::EmptyField {
                field: "contract.prompt.version",
            });
        }
        self.governance.validate()?;
        self.role.validate()
    }
}

/// A model-visible function declaration. Rust binds it to an already granted
/// tool kind; a contract cannot advertise a function it is not allowed to run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub kind: ToolKind,
    pub input_schema: BlobRef,
    pub strict: bool,
}

impl ToolSpec {
    fn validate(&self) -> Result<(), DomainError> {
        let valid_name = self.name.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || (index > 0 && byte == b'_')
        });
        if self.name.is_empty() || !valid_name || self.description.trim().is_empty() || !self.strict
        {
            return Err(DomainError::EmptyField {
                field: "contract.tool_specs",
            });
        }
        self.input_schema.validate()
    }
}

impl CandidateCapabilityCeiling {
    pub fn validate(&self) -> Result<(), DomainError> {
        self.context.validate()?;
        validate_tool_grants(&self.tool_grants, &self.context)
    }

    fn permits(&self, context: &ContextPolicy, tool_grants: &[ToolGrant]) -> bool {
        context
            .permitted_kinds
            .is_subset(&self.context.permitted_kinds)
            && context
                .permitted_source_families
                .is_subset(&self.context.permitted_source_families)
            && context.min_artifacts >= self.context.min_artifacts
            && context.max_artifacts <= self.context.max_artifacts
            && context.max_bytes <= self.context.max_bytes
            && context.max_tokens <= self.context.max_tokens
            && (!context.allow_raw_reread || self.context.allow_raw_reread)
            && tool_grants.iter().all(|requested| {
                self.tool_grants.iter().any(|allowed| {
                    allowed.kind == requested.kind
                        && requested
                            .allowed_sources
                            .iter()
                            .all(|source| allowed.allowed_sources.contains(source))
                })
            })
    }
}

fn validate_tool_grants(
    tool_grants: &[ToolGrant],
    context: &ContextPolicy,
) -> Result<(), DomainError> {
    if tool_grants.iter().any(|grant| {
        !matches!(
            grant.kind,
            ToolKind::ReadEvidence | ToolKind::ReadRawEvidence
        ) || grant.allowed_sources.is_empty()
            || grant.allowed_sources.iter().any(|source| {
                source.trim().is_empty() || !context.permitted_source_families.contains(source)
            })
            || (grant.kind == ToolKind::ReadRawEvidence && !context.allow_raw_reread)
    }) {
        return Err(DomainError::EmptyField {
            field: "contract.tool_grants",
        });
    }
    Ok(())
}

fn validate_tool_specs(
    tool_specs: &[ToolSpec],
    tool_grants: &[ToolGrant],
) -> Result<(), DomainError> {
    let mut names = BTreeSet::new();
    let mut kinds = BTreeSet::new();
    for spec in tool_specs {
        spec.validate()?;
        if !names.insert(spec.name.as_str())
            || !kinds.insert(spec.kind)
            || !tool_grants.iter().any(|grant| grant.kind == spec.kind)
        {
            return Err(DomainError::EmptyField {
                field: "contract.tool_specs",
            });
        }
    }
    if tool_grants
        .iter()
        .any(|grant| !tool_specs.iter().any(|spec| spec.kind == grant.kind))
    {
        return Err(DomainError::EmptyField {
            field: "contract.tool_specs",
        });
    }
    Ok(())
}

impl OutputContract {
    fn validate(&self) -> Result<(), DomainError> {
        self.schema.validate()
    }
}

/// One canonical definition drives prompt, runtime access, schema validation,
/// retry/termination limits, and task policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentContract {
    pub schema_version: u32,
    pub contract_id: ContractId,
    pub version: u32,
    pub purpose: ContractPurpose,
    pub responsibility: String,
    pub prompt: PromptBundle,
    pub context: ContextPolicy,
    pub tool_grants: Vec<ToolGrant>,
    pub tool_specs: Vec<ToolSpec>,
    pub candidate_capability_ceiling: CandidateCapabilityCeiling,
    pub output: OutputContract,
    #[serde(default)]
    pub deliberation_policy: DeliberationPolicy,
    pub budget: TaskBudget,
    pub retry: RetryPolicy,
    pub termination: TerminationPolicy,
    pub on_failure: FailureDisposition,
    pub contract_hash: ContentHash,
}

impl AgentContract {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        contract_id: ContractId,
        version: u32,
        purpose: ContractPurpose,
        responsibility: impl Into<String>,
        prompt: PromptBundle,
        context: ContextPolicy,
        tool_grants: Vec<ToolGrant>,
        tool_specs: Vec<ToolSpec>,
        output: OutputContract,
        budget: TaskBudget,
        retry: RetryPolicy,
        termination: TerminationPolicy,
        on_failure: FailureDisposition,
    ) -> Result<Self, DomainError> {
        let responsibility = responsibility.into();
        let mut contract = Self {
            schema_version: V2_SCHEMA_VERSION,
            contract_id,
            version,
            purpose,
            responsibility,
            prompt,
            candidate_capability_ceiling: CandidateCapabilityCeiling {
                context: context.clone(),
                tool_grants: tool_grants.clone(),
            },
            context,
            tool_grants,
            tool_specs,
            output,
            deliberation_policy: DeliberationPolicy::Disabled,
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
        self.expected_hash_with_fields(true)
    }

    fn expected_hash_with_fields(
        &self,
        include_deliberation_policy: bool,
    ) -> Result<ContentHash, DomainError> {
        let mut value = serde_json::to_value(self).map_err(|_| DomainError::EmptyField {
            field: "contract.serialize",
        })?;
        let object = value
            .as_object_mut()
            .expect("contract serializes to object");
        object.remove("contract_hash");
        if !include_deliberation_policy {
            object.remove("deliberation_policy");
        }
        content_hash_json(&value).map_err(|_| DomainError::EmptyField {
            field: "contract.serialize",
        })
    }

    pub fn with_candidate_capability_ceiling(
        mut self,
        candidate_capability_ceiling: CandidateCapabilityCeiling,
    ) -> Result<Self, DomainError> {
        self.candidate_capability_ceiling = candidate_capability_ceiling;
        self.contract_hash = self.expected_hash()?;
        self.validate()?;
        Ok(self)
    }

    pub fn permits_candidate(&self, candidate: &Self) -> bool {
        self.candidate_capability_ceiling
            .permits(&candidate.context, &candidate.tool_grants)
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_SCHEMA_VERSION {
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
        self.candidate_capability_ceiling.validate()?;
        if !self
            .candidate_capability_ceiling
            .permits(&self.context, &self.tool_grants)
        {
            return Err(DomainError::EmptyField {
                field: "contract.candidate_capability_ceiling",
            });
        }
        self.output.validate()?;
        self.budget.validate()?;
        self.retry.validate()?;
        validate_tool_grants(&self.tool_grants, &self.context)?;
        validate_tool_specs(&self.tool_specs, &self.tool_grants)?;
        if !matches!(
            self.output.artifact_kind,
            ArtifactKind::EvidenceNeed
                | ArtifactKind::WorkflowProposalDraft
                | ArtifactKind::WorkflowProposal
                | ArtifactKind::Claim
                | ArtifactKind::Critique
                | ArtifactKind::Resolution
                | ArtifactKind::DecisionProposal
                | ArtifactKind::RetrospectiveDraft
        ) {
            return Err(DomainError::EmptyField {
                field: "contract.output.artifact_kind",
            });
        }
        let expected_hash = self.expected_hash()?;
        let legacy_hash = (self.deliberation_policy == DeliberationPolicy::Disabled)
            .then(|| self.expected_hash_with_fields(false))
            .transpose()?;
        if self.contract_hash != expected_hash && legacy_hash.as_ref() != Some(&self.contract_hash)
        {
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
    pub allowed_evidence_sources: BTreeSet<String>,
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
        if self
            .allowed_evidence_sources
            .iter()
            .any(|source| source.trim().is_empty())
        {
            return Err(DomainError::EmptyField {
                field: "task_recipe.allowed_evidence_sources",
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
        if self.schema_version != V2_SCHEMA_VERSION
            || self.selections.len() < usize::from(policy.min_artifacts)
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

/// Rust-owned attenuation contract for projecting one persisted context
/// manifest into a child task. It carries artifact references only; raw
/// evidence and model transcripts are never delegation inputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextProjection {
    pub parent_manifest: ArtifactRef,
    pub allowed: Vec<ArtifactRef>,
    pub reason: String,
}

impl ContextProjection {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.parent_manifest.kind != ArtifactKind::ContextManifest
            || self.reason.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "context_projection",
            });
        }

        let mut seen = BTreeSet::new();
        if self.allowed.iter().any(|reference| {
            reference.kind == ArtifactKind::RawEvidence
                || !seen.insert(reference.artifact_id.clone())
        }) {
            return Err(DomainError::EmptyField {
                field: "context_projection.allowed",
            });
        }

        Ok(())
    }
}

/// Ephemeral, task-scoped authorization derived from a persisted manifest. It is
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadGrant {
    pub manifest_artifact_id: ArtifactId,
    pub run_id: RunId,
    pub task_id: TaskId,
    pub attempt_id: crate::AttemptId,
    pub lease_id: LeaseId,
    pub epoch: u64,
    pub contract_hash: ContentHash,
    pub readable: BTreeSet<ArtifactId>,
    pub raw_source_closure: BTreeSet<ArtifactId>,
    pub expires_at: DateTime<Utc>,
}

impl ReadGrant {
    pub fn matches_permit(&self, permit: &TaskWritePermit) -> bool {
        self.run_id == permit.run_id
            && self.task_id == permit.task_id
            && self.attempt_id == permit.attempt_id
            && self.lease_id == permit.lease_id
            && self.epoch == permit.epoch
            && permit.contract_hash.as_ref() == Some(&self.contract_hash)
    }

    pub fn permits(&self, artifact_id: &ArtifactId, raw: bool, now: DateTime<Utc>) -> bool {
        now < self.expires_at
            && if raw {
                self.raw_source_closure.contains(artifact_id)
            } else {
                self.readable.contains(artifact_id)
            }
    }
}

/// Authorizes exactly one running attempt to create an artifact or commit a result.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    fn artifact_identity_canonicalizes_source_reference_order() {
        let first = ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"z-source")),
            kind: ArtifactKind::ToolCall,
        };
        let second = ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"a-source")),
            kind: ArtifactKind::NormalizedEvidence,
        };
        let artifact = Artifact::new(
            ArtifactKind::Claim,
            blob(b"claim"),
            "fixture",
            ArtifactLifecycle::RunScoped,
            provenance(),
            None,
            vec![first.clone(), second.clone()],
            Utc::now(),
        )
        .unwrap();

        assert_eq!(artifact.source_refs, vec![second, first]);

        let mut reordered = artifact.clone();
        reordered.source_refs.reverse();
        assert_eq!(
            reordered.validate(),
            Err(DomainError::EmptyField {
                field: "artifact.source_refs",
            })
        );
        assert_eq!(reordered.expected_hash().unwrap(), artifact.artifact_id.0);
    }

    #[test]
    fn planner_artifacts_cannot_be_canonical() {
        for kind in [
            ArtifactKind::EvidenceNeed,
            ArtifactKind::WorkflowProposalDraft,
            ArtifactKind::WorkflowProposal,
            ArtifactKind::WorkflowGraph,
            ArtifactKind::DecisionProposal,
        ] {
            assert!(!kind.can_be_canonical());
        }
    }

    #[test]
    fn normalized_evidence_rejects_a_non_raw_source() {
        let source = ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"detail")),
            kind: ArtifactKind::SemanticDetail,
        };

        assert!(matches!(
            Artifact::new(
                ArtifactKind::NormalizedEvidence,
                blob(b"normalized"),
                "fixture",
                ArtifactLifecycle::RunScoped,
                provenance(),
                None,
                vec![source],
                Utc::now(),
            ),
            Err(DomainError::EmptyField {
                field: "artifact.normalized_source_refs"
            })
        ));
    }

    #[test]
    fn contract_hash_rejects_prompt_or_grant_substitution() {
        let contract = AgentContract::new(
            ContractId::new(),
            1,
            ContractPurpose::new("research.analyst").unwrap(),
            "derive claims",
            PromptBundle {
                version: 1,
                governance: blob(b"governance"),
                role: blob(b"prompt"),
            },
            ContextPolicy {
                permitted_kinds: BTreeSet::from([ArtifactKind::NormalizedEvidence]),
                permitted_source_families: BTreeSet::from(["market".to_owned()]),
                min_artifacts: 1,
                max_artifacts: 4,
                max_bytes: 1024,
                max_tokens: 256,
                allow_raw_reread: true,
            },
            vec![ToolGrant {
                kind: ToolKind::ReadEvidence,
                allowed_sources: vec!["market".to_owned()],
            }],
            vec![ToolSpec {
                name: "read_artifact".to_owned(),
                description: "read granted artifact".to_owned(),
                kind: ToolKind::ReadEvidence,
                input_schema: blob(b"tool schema"),
                strict: true,
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
        substituted.prompt.role = blob(b"different prompt");
        assert_eq!(substituted.validate(), Err(DomainError::InvalidContentHash));

        let mut substituted_tool = contract.clone();
        substituted_tool.tool_specs[0].description = "different tool description".to_owned();
        assert_eq!(
            substituted_tool.validate(),
            Err(DomainError::InvalidContentHash)
        );

        let mut expanded = contract.clone();
        expanded.tool_grants = vec![ToolGrant {
            kind: ToolKind::FetchWebEvidence,
            allowed_sources: vec!["news".to_owned()],
        }];
        expanded.contract_hash = expanded.expected_hash().unwrap();
        assert!(expanded.validate().is_err());

        expanded = contract.clone();
        expanded.tool_grants = vec![ToolGrant {
            kind: ToolKind::ReadEvidence,
            allowed_sources: vec!["news".to_owned()],
        }];
        expanded.contract_hash = expanded.expected_hash().unwrap();
        assert!(expanded.validate().is_err());

        let mut candidate = contract.clone();
        candidate
            .context
            .permitted_source_families
            .insert("news".to_owned());
        candidate.tool_grants = vec![ToolGrant {
            kind: ToolKind::ReadEvidence,
            allowed_sources: vec!["market".to_owned(), "news".to_owned()],
        }];
        candidate.candidate_capability_ceiling = CandidateCapabilityCeiling {
            context: candidate.context.clone(),
            tool_grants: candidate.tool_grants.clone(),
        };
        candidate.contract_hash = candidate.expected_hash().unwrap();
        candidate.validate().unwrap();
        assert!(!contract.permits_candidate(&candidate));

        let active = contract
            .clone()
            .with_candidate_capability_ceiling(candidate.candidate_capability_ceiling.clone())
            .unwrap();
        assert!(active.permits_candidate(&candidate));

        expanded = contract;
        expanded.context.allow_raw_reread = false;
        expanded.tool_grants = vec![ToolGrant {
            kind: ToolKind::ReadRawEvidence,
            allowed_sources: vec!["market".to_owned()],
        }];
        expanded.contract_hash = expanded.expected_hash().unwrap();
        assert!(expanded.validate().is_err());
    }

    #[test]
    fn context_minimum_is_validated_and_candidates_cannot_lower_it() {
        let policy = ContextPolicy {
            permitted_kinds: BTreeSet::from([ArtifactKind::NormalizedEvidence]),
            permitted_source_families: BTreeSet::from(["market".to_owned()]),
            min_artifacts: 1,
            max_artifacts: 4,
            max_bytes: 1024,
            max_tokens: 256,
            allow_raw_reread: false,
        };
        policy.validate().unwrap();
        let ceiling = CandidateCapabilityCeiling {
            context: policy.clone(),
            tool_grants: vec![],
        };

        let mut lower_minimum = policy.clone();
        lower_minimum.min_artifacts = 0;
        assert!(!ceiling.permits(&lower_minimum, &[]));

        let mut stricter_minimum = policy.clone();
        stricter_minimum.min_artifacts = 2;
        assert!(ceiling.permits(&stricter_minimum, &[]));

        let mut invalid = policy;
        invalid.min_artifacts = invalid.max_artifacts + 1;
        assert!(invalid.validate().is_err());
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
    fn workflow_proposal_rejects_unknown_recipes_and_cycles() {
        let recipe_id = TaskRecipeId::new("analyst").unwrap();
        let recipe = TaskRecipe {
            recipe_id: recipe_id.clone(),
            purpose: ContractPurpose::new("research.analyst").unwrap(),
            contract_hash: Some(ContentHash::of_bytes(b"fixture-contract")),
            task_class: RuntimeTaskClass::Agent,
            allowed_evidence_sources: BTreeSet::new(),
            max_children: 4,
            max_depth: 4,
            priority_ceiling: 80,
            budget: TaskBudget {
                max_input_tokens: 256,
                max_output_tokens: 128,
                max_wall_time_secs: 30,
                max_tool_calls: 2,
            },
            retry: RetryPolicy {
                max_attempts: 1,
                initial_backoff_ms: 1,
                retry_transport: true,
                retry_rate_limited: true,
                retry_invalid_output: false,
            },
            on_failure: FailureDisposition::FailTask,
        };
        let recipes = BTreeMap::from([(recipe_id.clone(), recipe)]);
        let mut proposal = WorkflowProposal {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: "fixture".to_owned(),
            tasks: BTreeMap::from([
                (
                    "analyst".to_owned(),
                    WorkflowProposalTask {
                        recipe_id: recipe_id.clone(),
                        objective: "analyze evidence".to_owned(),
                        depends_on: vec![],
                        priority: 50,
                        evidence_needs: vec![],
                    },
                ),
                (
                    "critic".to_owned(),
                    WorkflowProposalTask {
                        recipe_id,
                        objective: "challenge the analysis".to_owned(),
                        depends_on: vec!["analyst".to_owned()],
                        priority: 50,
                        evidence_needs: vec![],
                    },
                ),
            ]),
            stop_reason: None,
        };
        proposal.validate(&recipes).unwrap();

        proposal
            .tasks
            .get_mut("analyst")
            .unwrap()
            .depends_on
            .push("critic".to_owned());
        assert_eq!(proposal.validate(&recipes), Err(DomainError::CyclicPlan));

        proposal
            .tasks
            .get_mut("analyst")
            .unwrap()
            .depends_on
            .clear();
        proposal.tasks.get_mut("critic").unwrap().recipe_id =
            TaskRecipeId::new("uninstalled").unwrap();
        assert!(matches!(
            proposal.validate(&recipes),
            Err(DomainError::EmptyField {
                field: "workflow_proposal.recipe"
            })
        ));
    }

    #[test]
    fn workflow_proposal_draft_limits_evidence_to_recipe_sources() {
        let recipe_id = TaskRecipeId::new("research.analyst").unwrap();
        let recipe = TaskRecipe {
            recipe_id: recipe_id.clone(),
            purpose: ContractPurpose::new("research.analyst").unwrap(),
            contract_hash: Some(ContentHash::of_bytes(b"fixture-contract")),
            task_class: RuntimeTaskClass::Agent,
            allowed_evidence_sources: BTreeSet::from(["alpaca".to_owned()]),
            max_children: 4,
            max_depth: 4,
            priority_ceiling: 80,
            budget: TaskBudget {
                max_input_tokens: 256,
                max_output_tokens: 128,
                max_wall_time_secs: 30,
                max_tool_calls: 2,
            },
            retry: RetryPolicy {
                max_attempts: 1,
                initial_backoff_ms: 1,
                retry_transport: true,
                retry_rate_limited: true,
                retry_invalid_output: false,
            },
            on_failure: FailureDisposition::FailTask,
        };
        let recipes = BTreeMap::from([(recipe_id.clone(), recipe)]);
        let mut draft = WorkflowProposalDraft {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: "fixture".to_owned(),
            tasks: BTreeMap::from([(
                "analyst".to_owned(),
                WorkflowProposalDraftTask {
                    recipe_id,
                    objective: "analyze governed market evidence".to_owned(),
                    depends_on: vec![],
                    priority: 50,
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
        draft.validate(&recipes).unwrap();

        draft.tasks.get_mut("analyst").unwrap().evidence_needs[0].source_family =
            "uninstalled-web".to_owned();
        assert_eq!(
            draft.validate(&recipes),
            Err(DomainError::EvidenceSourceNotAllowed(
                "uninstalled-web".to_owned()
            ))
        );
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

    #[test]
    fn read_grant_is_bound_to_the_minting_permit() {
        let permit = TaskWritePermit {
            run_id: RunId::new(),
            task_id: TaskId::new(),
            attempt_id: AttemptId::new(),
            lease_id: LeaseId::new(),
            epoch: 7,
            contract_hash: Some(ContentHash::of_bytes(b"contract")),
        };
        let mut grant = ReadGrant {
            manifest_artifact_id: ArtifactId(ContentHash::of_bytes(b"manifest")),
            run_id: permit.run_id.clone(),
            task_id: permit.task_id.clone(),
            attempt_id: permit.attempt_id.clone(),
            lease_id: permit.lease_id.clone(),
            epoch: permit.epoch,
            contract_hash: permit.contract_hash.clone().unwrap(),
            readable: BTreeSet::new(),
            raw_source_closure: BTreeSet::new(),
            expires_at: Utc::now(),
        };

        assert!(grant.matches_permit(&permit));
        grant.epoch += 1;
        assert!(!grant.matches_permit(&permit));
    }

    #[test]
    fn disabled_deliberation_contracts_keep_legacy_hashes_valid() {
        let contract = AgentContract::new(
            ContractId::new(),
            1,
            ContractPurpose::new("research.analyst").unwrap(),
            "derive claims",
            PromptBundle {
                version: 1,
                governance: blob(b"governance"),
                role: blob(b"prompt"),
            },
            ContextPolicy {
                permitted_kinds: BTreeSet::from([ArtifactKind::NormalizedEvidence]),
                permitted_source_families: BTreeSet::from(["market".to_owned()]),
                min_artifacts: 1,
                max_artifacts: 4,
                max_bytes: 1024,
                max_tokens: 256,
                allow_raw_reread: true,
            },
            vec![ToolGrant {
                kind: ToolKind::ReadEvidence,
                allowed_sources: vec!["market".to_owned()],
            }],
            vec![ToolSpec {
                name: "read_artifact".to_owned(),
                description: "read granted artifact".to_owned(),
                kind: ToolKind::ReadEvidence,
                input_schema: blob(b"tool schema"),
                strict: true,
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

        let mut legacy_value = serde_json::to_value(&contract).unwrap();
        legacy_value
            .as_object_mut()
            .unwrap()
            .remove("deliberation_policy");
        legacy_value
            .as_object_mut()
            .unwrap()
            .remove("contract_hash");
        let legacy_hash = content_hash_json(&legacy_value).unwrap();
        legacy_value.as_object_mut().unwrap().insert(
            "contract_hash".to_owned(),
            serde_json::Value::String(legacy_hash.as_str().to_owned()),
        );
        let legacy: AgentContract = serde_json::from_value(legacy_value).unwrap();
        assert_eq!(legacy.deliberation_policy, DeliberationPolicy::Disabled);
        legacy.validate().unwrap();
    }
}
