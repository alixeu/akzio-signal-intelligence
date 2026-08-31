//! Versioned model contract vocabulary.

use std::{collections::BTreeSet, fmt};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::artifact::{ArtifactId, ArtifactKind};
use crate::schema::V2_SCHEMA_VERSION;
use crate::workflow::RuntimeTaskClass;
use crate::{
    content_hash_json, BlobRef, ContentHash, ContractId, DomainError, FailureDisposition,
    RetryPolicy, TaskBudget, TerminationPolicy, ToolGrant, ToolKind,
};

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
    pub alternative_match_ppm: Vec<u32>,
    #[serde(default)]
    pub uncertainties: Vec<String>,
    #[serde(default)]
    pub uncertainty_weight_ppm: Vec<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assessment_source: Option<String>,
    #[serde(default)]
    pub basis_artifact_ids: Vec<ArtifactId>,
    pub confidence_ppm: u32,
}

impl DeliberationSummary {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.selected_path.trim().is_empty()
            || self.selected_path.chars().count() > 1_000
            || (!self.alternative_match_ppm.is_empty() && self.alternatives.len() > 3)
            || (!self.alternative_match_ppm.is_empty()
                && self.alternative_match_ppm.len() != self.alternatives.len())
            || (!self.uncertainty_weight_ppm.is_empty() && self.uncertainties.len() > 3)
            || (!self.uncertainty_weight_ppm.is_empty()
                && self.uncertainty_weight_ppm.len() != self.uncertainties.len())
            || self.basis_artifact_ids.len() > 8
            || self.confidence_ppm > 1_000_000
            || self
                .alternative_match_ppm
                .iter()
                .any(|value| *value > 1_000_000)
            || self
                .uncertainty_weight_ppm
                .iter()
                .any(|value| *value > 1_000_000)
            || self
                .assessment_source
                .as_deref()
                .is_some_and(|source| source != "model_assessed")
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
        if !self.uncertainty_weight_ppm.is_empty()
            && self
                .uncertainty_weight_ppm
                .iter()
                .try_fold(0_u32, |sum, value| sum.checked_add(*value))
                != Some(1_000_000 - self.confidence_ppm)
        {
            return Err(DomainError::InvalidBudget {
                field: "deliberation.uncertainty_weight_ppm",
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

    pub fn validate_model_assessment(&self) -> Result<(), DomainError> {
        self.validate()?;
        if self.assessment_source.as_deref() != Some("model_assessed")
            || self.alternatives.len() > 3
            || self.alternative_match_ppm.len() != self.alternatives.len()
            || self.uncertainties.len() > 3
            || self.uncertainty_weight_ppm.len() != self.uncertainties.len()
        {
            return Err(DomainError::InvalidBudget {
                field: "deliberation.summary",
            });
        }
        if self
            .uncertainty_weight_ppm
            .iter()
            .try_fold(0_u32, |sum, value| sum.checked_add(*value))
            != Some(1_000_000 - self.confidence_ppm)
        {
            return Err(DomainError::InvalidBudget {
                field: "deliberation.uncertainty_weight_ppm",
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

    pub(super) fn permits(&self, context: &ContextPolicy, tool_grants: &[ToolGrant]) -> bool {
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
    for spec in tool_specs {
        spec.validate()?;
        if !names.insert(spec.name.as_str())
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
        let mut value = serde_json::to_value(self).map_err(|_| DomainError::EmptyField {
            field: "contract.serialize",
        })?;
        let object = value
            .as_object_mut()
            .expect("contract serializes to an object");
        object.remove("contract_hash");
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
        if self.contract_hash != expected_hash {
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
