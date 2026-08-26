//! Contract-driven Agent runtime for the v2 system.

use akzio_domain::{AttemptId, RunId, TaskId};
use akzio_model::ModelStreamEvent;
use std::sync::Arc;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration as StdDuration, Instant},
};
use tokio::sync::broadcast;

use akzio_context::v2::{ContextBroker, ContextError, ContextManifest};
use akzio_domain::{
    AgentContract, AgentOutputEnvelope, Artifact, ArtifactId, ArtifactKind, ArtifactLifecycle,
    ArtifactProvenance, ArtifactRef, ContextPolicy, ContractId, ContractPurpose, DecisionDraft,
    DeliberationPolicy, DomainError, FailureDisposition, LifecycleEventType, OutputContract,
    PromptBundle, ReadGrant, ResearchClaim, ResearchCritique, ResearchResolution, RetryPolicy,
    RunPurpose, RuntimeTaskClass, TaskBudget, TaskRecipe, TaskRecipeId, TaskWritePermit,
    TerminationPolicy, ToolGrant, ToolKind, ToolSpec, WorkflowNode, V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_model::{
    ModelCallTrace, ModelCapabilitySnapshot, ModelClient, ModelContinuation, ModelError,
    ModelInput, ModelRequest, ModelToolChoice, ModelToolDefinition, ModelToolOutput,
};
use akzio_runtime::v2::{RecipeCatalogue, RetryCause, RuntimeError};
use akzio_store::v2::{StoreError, StoredContract, V2Store};
use chrono::{DateTime, Duration, Utc};
use futures::future::BoxFuture;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

mod catalogue;
mod schemas;
mod tools;
mod validation;

use akzio_domain::{
    GOVERNED_EVIDENCE_SOURCE_FAMILIES, LEARNING_OUTCOME_WORKER_RECIPE_ID,
    RESEARCH_ANALYST_RECIPE_ID, RESEARCH_CRITIC_RECIPE_ID, RESEARCH_SYNTHESIZER_RECIPE_ID,
};
use catalogue::{
    ActiveRecipePolicy, ACTIVE_CONTRACT_VERSION, ACTIVE_PROMPT_BUNDLE_VERSION,
    ACTIVE_RECIPE_POLICIES, PLANNER_CHILD_RECIPE_IDS, PLANNER_MAX_DRAFT_TASKS, PLANNER_RECIPE_ID,
    RFC3339_TIMESTAMP_PATTERN, SHARED_GOVERNANCE_PROMPT,
};
pub use catalogue::{
    ActiveResearchCatalogue, ContractCatalogue, InstalledContract, ACTIVE_RESEARCH_MAX_NODES,
};
use schemas::*;
use tools::*;
use validation::*;

#[derive(Debug, Error)]
pub enum ResearchError {
    #[error(transparent)]
    Context(#[from] ContextError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error("task has no Agent Contract hash")]
    MissingContractHash,
    #[error("Agent Contract {0} is not installed")]
    UnknownContract(akzio_domain::ContentHash),
    #[error("task contract hash and recipe contract hash do not match")]
    ContractMismatch,
    #[error("workflow node task does not match the write permit task")]
    TaskMismatch,
    #[error("workflow node policy diverges from its installed Agent Contract")]
    NodePolicyMismatch,
    #[error("Agent Contract {0} appears more than once in the catalogue")]
    DuplicateContract(akzio_domain::ContentHash),
    #[error("Agent Contract {contract_id:?} version {version} appears more than once")]
    DuplicateContractVersion {
        contract_id: akzio_domain::ContractId,
        version: u32,
    },
    #[error("active research contract purpose is not allowed: {0}")]
    UnexpectedActiveContractPurpose(String),
    #[error("active research contract purpose appears more than once: {0}")]
    DuplicateActiveContractPurpose(String),
    #[error("active research contract is missing: {0}")]
    MissingActiveContract(&'static str),
    #[error("active research contract {purpose} outputs {actual:?}, expected {expected:?}")]
    ActiveContractOutputMismatch {
        purpose: String,
        expected: ArtifactKind,
        actual: ArtifactKind,
    },
    #[error("active research contract {0} differs from the canonical definition")]
    NonCanonicalActiveContract(String),
    #[error("candidate contract {candidate} expands active contract {active} capability")]
    CandidateCapabilityExpansion {
        active: akzio_domain::ContentHash,
        candidate: akzio_domain::ContentHash,
    },
    #[error("model capability mismatch for {capability} ({provider_id}/{model_id})")]
    CapabilityMismatch {
        capability: &'static str,
        provider_id: String,
        model_id: String,
    },
    #[error("ReadGrant does not match the active task permit")]
    GrantPermitMismatch,
    #[error("Agent output did not satisfy Contract schema: {0}")]
    InvalidOutput(String),
    #[error("Agent model failed: {0}")]
    Model(String),
    #[error("Agent model rate limited: {0}")]
    RateLimited(String),
    #[error("Agent model {error_class} failed: {message}")]
    ModelDebug {
        error_class: &'static str,
        message: String,
        trace: ModelCallTrace,
    },
    #[error("tool {0} is not granted by the Agent Contract")]
    ToolNotGranted(String),
    #[error("invalid model ToolSpec: {0}")]
    InvalidToolSpec(String),
    #[error("tool {tool} is not granted for source family {source_family}")]
    ToolSourceNotGranted { tool: String, source_family: String },
    #[error("Agent exceeded its Contract tool-call budget")]
    ToolBudgetExceeded,
    #[error("Agent exceeded its derived provider-call budget")]
    ModelCallBudgetExceeded,
    #[error("Agent request used {actual} tokens but Contract permits at most {maximum}")]
    InputBudgetExceeded { actual: u32, maximum: u32 },
    #[error("Agent output used {actual} tokens but Contract permits at most {maximum}")]
    OutputBudgetExceeded { actual: u32, maximum: u32 },
    #[error("Agent exceeded its Contract wall-time budget of {maximum_secs} seconds")]
    WallTimeExceeded { maximum_secs: u32 },
    #[error("Agent completed without a final output")]
    MissingFinalOutput,
    #[error("Agent submission response is ambiguous")]
    AmbiguousSubmission,
    #[error("Agent model refused the task: {0}")]
    ModelRefused(String),
}

impl ResearchError {
    pub fn retry_cause(&self) -> Option<RetryCause> {
        match self {
            Self::InvalidOutput(_) | Self::MissingFinalOutput => Some(RetryCause::InvalidOutput),
            Self::ModelDebug {
                error_class: "invalid_output",
                ..
            } => Some(RetryCause::InvalidOutput),
            _ => None,
        }
    }
}

pub type ResearchResult<T> = Result<T, ResearchError>;

fn active_recipe_policy(purpose: &str) -> Option<ActiveRecipePolicy> {
    ACTIVE_RECIPE_POLICIES
        .iter()
        .copied()
        .find(|policy| policy.purpose == purpose)
}

struct CanonicalContractDefinition {
    purpose: &'static str,
    responsibility: &'static str,
    output_kind: ArtifactKind,
    output_schema: Value,
    permitted_kinds: BTreeSet<ArtifactKind>,
    min_context_artifacts: u16,
    budget: TaskBudget,
    termination: TerminationPolicy,
    on_failure: FailureDisposition,
}

fn canonical_active_contracts(store: &V2Store) -> ResearchResult<Vec<AgentContract>> {
    [
        CanonicalContractDefinition {
            purpose: PLANNER_RECIPE_ID,
            responsibility: "Lower a bounded research objective into a WorkflowProposalDraft using only installed research recipes and inline EvidenceNeed requests.",
            output_kind: ArtifactKind::WorkflowProposalDraft,
            output_schema: planner_draft_output_schema(),
            permitted_kinds: BTreeSet::from([
                ArtifactKind::NormalizedEvidence,
                ArtifactKind::SemanticDetail,
                ArtifactKind::Claim,
                ArtifactKind::Critique,
            ]),
            min_context_artifacts: 0,
            budget: TaskBudget {
                max_input_tokens: 12_000,
                max_output_tokens: 2_000,
                max_wall_time_secs: 120,
                max_tool_calls: 4,
            },
            termination: TerminationPolicy {
                max_child_tasks: PLANNER_MAX_DRAFT_TASKS,
                max_depth: 2,
                require_evidence: false,
                stop_when_evidence_complete: true,
            },
            on_failure: FailureDisposition::FailRun,
        },
        CanonicalContractDefinition {
            purpose: RESEARCH_ANALYST_RECIPE_ID,
            responsibility: "Produce evidence-linked, bounded research claims for one shard of the approved workflow.",
            output_kind: ArtifactKind::Claim,
            output_schema: claim_output_schema(),
            permitted_kinds: BTreeSet::from([
                ArtifactKind::NormalizedEvidence,
                ArtifactKind::SemanticDetail,
            ]),
            min_context_artifacts: 1,
            budget: TaskBudget {
                max_input_tokens: 8_000,
                max_output_tokens: 1_500,
                max_wall_time_secs: 120,
                max_tool_calls: 4,
            },
            termination: TerminationPolicy {
                max_child_tasks: 2,
                max_depth: 2,
                require_evidence: true,
                stop_when_evidence_complete: true,
            },
            on_failure: FailureDisposition::FailTask,
        },
        CanonicalContractDefinition {
            purpose: RESEARCH_CRITIC_RECIPE_ID,
            responsibility: "Challenge material claims and surface evidence or risk gaps without changing facts or execution authority.",
            output_kind: ArtifactKind::Critique,
            output_schema: critique_output_schema(),
 permitted_kinds: BTreeSet::from([
 ArtifactKind::Claim,
 ArtifactKind::SemanticDetail,
 ArtifactKind::DeliberationNote,
 ]),
            min_context_artifacts: 1,
            budget: TaskBudget {
                max_input_tokens: 6_000,
                max_output_tokens: 1_500,
                max_wall_time_secs: 90,
                max_tool_calls: 2,
            },
            termination: TerminationPolicy {
                max_child_tasks: 1,
                max_depth: 1,
                require_evidence: true,
                stop_when_evidence_complete: true,
            },
            on_failure: FailureDisposition::SkipTask,
        },
        CanonicalContractDefinition {
            purpose: RESEARCH_SYNTHESIZER_RECIPE_ID,
            responsibility: "Synthesize approved claims and critiques into a DecisionProposal with typed blockers for Rust-owned gates.",
            output_kind: ArtifactKind::DecisionProposal,
            output_schema: decision_proposal_output_schema(),
 permitted_kinds: BTreeSet::from([
                ArtifactKind::Claim,
                ArtifactKind::Critique,
                ArtifactKind::Lesson,
                ArtifactKind::Experience,
 ArtifactKind::CandidatePolicy,
 ArtifactKind::NormalizedEvidence,
 ArtifactKind::SemanticDetail,
 ArtifactKind::DeliberationNote,
 ]),
            min_context_artifacts: 1,
            budget: TaskBudget {
                max_input_tokens: 12_000,
                max_output_tokens: 2_000,
                max_wall_time_secs: 120,
                max_tool_calls: 2,
            },
            termination: TerminationPolicy::leaf(),
            on_failure: FailureDisposition::FailRun,
        },
        CanonicalContractDefinition {
            purpose: LEARNING_OUTCOME_WORKER_RECIPE_ID,
            responsibility: "Produce a bounded retrospective draft from the governed Paper decision and outcome evidence chain.",
            output_kind: ArtifactKind::RetrospectiveDraft,
            output_schema: retrospective_draft_output_schema(),
            permitted_kinds: BTreeSet::from([
                ArtifactKind::Decision,
                ArtifactKind::DecisionContext,
                ArtifactKind::ExecutionContext,
                ArtifactKind::ExecutionVerdict,
                ArtifactKind::ExecutionCommitment,
                ArtifactKind::OrderReceipt,
                ArtifactKind::Reconciliation,
                ArtifactKind::OutcomeSchedule,
                ArtifactKind::Outcome,
                ArtifactKind::NormalizedEvidence,
                ArtifactKind::SemanticDetail,
                ArtifactKind::DeliberationNote,
                ArtifactKind::Retrospective,
            ]),
            min_context_artifacts: 1,
            budget: TaskBudget {
                max_input_tokens: 12_000,
                max_output_tokens: 2_500,
                max_wall_time_secs: 180,
                max_tool_calls: 2,
            },
            termination: TerminationPolicy::leaf(),
            on_failure: FailureDisposition::FailTask,
        },
    ]
    .into_iter()
    .map(|definition| canonical_active_contract(store, definition))
    .collect()
}

fn canonical_active_contract(
    store: &V2Store,
    definition: CanonicalContractDefinition,
) -> ResearchResult<AgentContract> {
    let base_prompt = two_phase_role_prompt(definition.purpose)?;
    let role_prompt = match definition.purpose {
        RESEARCH_SYNTHESIZER_RECIPE_ID => format!(
            "{}\n\nAlways return exactly 12 forecasts: one for each executable asset (TQQQ, QQQ, SOXX, SOXL) at each horizon (t1, t3, t5), even when the proposal is blocked; for blocked proposals use neutral zero forecasts and explain the blocker in hard_blockers and summary. In deliberation.basis_artifact_ids and result references, use only artifact IDs that appear as top-level selections in the current ContextManifest; do not copy nested evidence IDs unless they are also selected. Preserve each selected artifact's exact kind: use claim only for claim refs, critique only for critique refs, and normalized_evidence or semantic_detail only when that exact kind is selected. ContextManifest deliberation_note selections may appear in basis_artifact_ids but must not be relabeled as result claims, critiques, or evidence.",
            base_prompt
        ),
        RESEARCH_ANALYST_RECIPE_ID => format!(
            "{}\n\nKeep evidence_gaps to at most 2 items; combine overlapping limitations into concise, evidence-grounded gaps. Preserve the exact artifact kind shown in ContextManifest selections; do not relabel normalized_evidence as semantic_detail or vice versa. For every grounds.evidence reference, copy the exact 64-character artifact_id and exact kind from a top-level context item. Never use the ContextManifest ID, a resource name, or an alias as an evidence artifact_id. Include at least one ground when readable evidence is present.",
            base_prompt
        ),
        _ => base_prompt,
    };
    let role_prompt = format!(
        "{role_prompt}\n\nUse at most 3 alternatives and at most 3 uncertainties. Use at most 8 evidence-relevant IDs in deliberation.basis_artifact_ids. Provide one alternative_match_ppm value for each alternative. Provide one uncertainty_weight_ppm value for each uncertainty; those weights must sum exactly to 1000000 - confidence_ppm. Use empty score arrays when the corresponding text array is empty. These scores are model-assessed metadata, not observed market facts."
    );
    let prompt = PromptBundle {
        version: ACTIVE_PROMPT_BUNDLE_VERSION,
        governance: store.put_bytes(SHARED_GOVERNANCE_PROMPT.as_bytes(), "text/plain")?,
        role: store.put_bytes(role_prompt.as_bytes(), "text/plain")?,
    };
    let schema = store.put_json(&deliberation_output_schema(&definition.output_schema))?;
    let mut contract = AgentContract::new(
        ContractId(format!("akzio.v2.{}", definition.purpose)),
        ACTIVE_CONTRACT_VERSION,
        ContractPurpose::new(definition.purpose)?,
        definition.responsibility,
        prompt,
        ContextPolicy {
            permitted_kinds: definition.permitted_kinds,
            permitted_source_families: governed_context_sources(),
            min_artifacts: definition.min_context_artifacts,
            max_artifacts: 24,
            max_bytes: 128 * 1024,
            max_tokens: definition.budget.max_input_tokens,
            allow_raw_reread: false,
        },
        evidence_read_grants(),
        evidence_read_tool_specs(store)?,
        OutputContract {
            artifact_kind: definition.output_kind,
            schema,
        },
        definition.budget,
        active_retry_policy(),
        definition.termination,
        definition.on_failure,
    )?;
    contract.deliberation_policy = DeliberationPolicy::Required;
    contract.contract_hash = contract.expected_hash()?;
    contract.validate()?;
    Ok(contract)
}

fn two_phase_role_prompt(purpose: &str) -> ResearchResult<String> {
    let prompt = match purpose {
        PLANNER_RECIPE_ID => "You are Akzio's bounded research planner. In Draft, explain the bounded workflow, required evidence, dependencies, and uncertainty. In Submit, produce WorkflowProposalDraft through submit_result. You may name only research.analyst and research.synthesizer recipes and express evidence needs inline. Numeric bounds are strict: priority 0-100, max_age_secs 1-604800, max_results 1-32, at most 4 assets and 7 tasks. window_start and window_end must be null or RFC3339 timestamps. Do not construct ArtifactRef values, widen capabilities, submit a decision, or submit an order.",
        RESEARCH_ANALYST_RECIPE_ID => "You are Akzio's research analyst. In Draft, write an evidence-grounded memo covering the claim, support, counter-evidence, gaps, and uncertainty. In Submit, produce Claim through submit_result. Use only granted context artifacts. Do not call external systems, widen sources, change topology, submit decisions, or submit orders.",
        RESEARCH_CRITIC_RECIPE_ID => "You are Akzio's research critic. In Draft, write a concise critique memo covering counter-evidence, unsupported assumptions, gaps, and uncertainty. In Submit, produce Critique through submit_result. Challenge supplied claims using granted context. Do not invent evidence, widen sources or tools, alter workflow, produce a decision, or submit an order.",
        RESEARCH_SYNTHESIZER_RECIPE_ID => "You are Akzio's research synthesizer. In Draft, write a decision memo reconciling claims, critiques, blockers, alternatives, and uncertainty. In Submit, produce DecisionProposal through submit_result. Use only artifacts selected by ContextManifest. Do not change evidence, bypass DecisionGate, submit an order, or expand any capability.",
        LEARNING_OUTCOME_WORKER_RECIPE_ID => "You are Akzio's governed outcome reviewer. In Draft, write a bounded retrospective memo from granted decision, execution, outcomes, market evidence, deliberation notes, and prior retrospectives. In Submit, produce RetrospectiveDraft through submit_result. Never emit authoritative returns, calibration, slippage, risk recall, or policy decisions.",
        _ => return Err(ResearchError::UnexpectedActiveContractPurpose(purpose.to_owned())),
    };
    Ok(prompt.to_owned())
}

fn governed_context_sources() -> BTreeSet<String> {
    GOVERNED_EVIDENCE_SOURCE_FAMILIES
        .into_iter()
        .chain(["akzio.agent", "akzio.operator"])
        .map(str::to_owned)
        .collect()
}

fn evidence_read_grants() -> Vec<ToolGrant> {
    vec![ToolGrant {
        kind: ToolKind::ReadEvidence,
        allowed_sources: GOVERNED_EVIDENCE_SOURCE_FAMILIES
            .into_iter()
            .map(str::to_owned)
            .collect(),
    }]
}

fn recipe_evidence_sources(contract: &AgentContract) -> BTreeSet<String> {
    contract
        .tool_grants
        .iter()
        .filter(|grant| grant.kind == ToolKind::ReadEvidence)
        .flat_map(|grant| grant.allowed_sources.iter().cloned())
        .collect()
}

fn active_retry_policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 2,
        initial_backoff_ms: 250,
        retry_transport: true,
        retry_rate_limited: true,
        retry_invalid_output: true,
    }
}

fn research_output_source_refs(
    store: &V2Store,
    kind: ArtifactKind,
    output: &Value,
    manifest: &ContextManifest,
) -> ResearchResult<Vec<ArtifactRef>> {
    let refs = match kind {
        ArtifactKind::Claim => {
            let claim: ResearchClaim = serde_json::from_value(output.clone()).map_err(|error| {
                ResearchError::InvalidOutput(format!("invalid Claim payload: {error}"))
            })?;
            claim
                .validate()
                .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;
            claim.source_refs()
        }
        ArtifactKind::Critique => {
            let critique: ResearchCritique =
                serde_json::from_value(output.clone()).map_err(|error| {
                    ResearchError::InvalidOutput(format!("invalid Critique payload: {error}"))
                })?;
            critique
                .validate()
                .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;
            critique.source_refs()
        }
        ArtifactKind::Resolution => {
            validate_schema_value(output, &resolution_output_schema(), "$")
                .map_err(ResearchError::InvalidOutput)?;
            let resolution: ResearchResolution =
                serde_json::from_value(output.clone()).map_err(|error| {
                    ResearchError::InvalidOutput(format!("invalid Resolution payload: {error}"))
                })?;
            resolution
                .validate()
                .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;
            resolution.source_refs()
        }
        ArtifactKind::RetrospectiveDraft => {
            let draft: akzio_domain::RetrospectiveDraft = serde_json::from_value(output.clone())
                .map_err(|error| {
                    ResearchError::InvalidOutput(format!(
                        "invalid RetrospectiveDraft payload: {error}"
                    ))
                })?;
            draft
                .validate()
                .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;
            let mut refs = draft.source_refs.clone();
            refs.extend(
                draft
                    .findings
                    .iter()
                    .flat_map(|finding| finding.artifact_refs.iter().cloned()),
            );
            refs.sort();
            refs.dedup();
            refs
        }
        ArtifactKind::DecisionProposal => {
            let proposal: DecisionDraft =
                serde_json::from_value(output.clone()).map_err(|error| {
                    ResearchError::InvalidOutput(format!(
                        "invalid DecisionProposal payload: {error}"
                    ))
                })?;
            proposal
                .validate()
                .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;

            let selected = manifest
                .payload
                .selections
                .iter()
                .map(|selection| selection.artifact.clone())
                .collect::<BTreeSet<_>>();
            let selected_claims = selected
                .iter()
                .filter(|reference| reference.kind == ArtifactKind::Claim)
                .cloned()
                .collect::<BTreeSet<_>>();
            let selected_critiques = selected
                .iter()
                .filter(|reference| reference.kind == ArtifactKind::Critique)
                .cloned()
                .collect::<BTreeSet<_>>();
            let submitted_claims = proposal.claims.iter().cloned().collect::<BTreeSet<_>>();
            let submitted_critiques = proposal.critiques.iter().cloned().collect::<BTreeSet<_>>();

            if submitted_claims.is_empty()
                && (!selected_claims.is_empty() || !proposal.evidence.is_empty())
            {
                return Err(ResearchError::InvalidOutput(
                    "DecisionProposal dropped all claims selected by ContextManifest".to_owned(),
                ));
            }
            if !selected_claims.is_subset(&submitted_claims) {
                return Err(ResearchError::InvalidOutput(
                    "DecisionProposal claims do not close over ContextManifest".to_owned(),
                ));
            }
            if !selected_critiques.is_subset(&submitted_critiques) {
                return Err(ResearchError::InvalidOutput(
                    "DecisionProposal critiques do not close over ContextManifest".to_owned(),
                ));
            }

            let declared_evidence = proposal.evidence.iter().cloned().collect::<BTreeSet<_>>();
            let mut refs = proposal
                .claims
                .iter()
                .chain(proposal.critiques.iter())
                .chain(proposal.evidence.iter())
                .cloned()
                .collect::<Vec<_>>();

            for reference in proposal.claims.iter().chain(proposal.critiques.iter()) {
                let artifact = store.artifact(&reference.artifact_id)?;
                let payload = store.read_blob(&artifact.blob)?;
                let source_refs = match reference.kind {
                    ArtifactKind::Claim => {
                        let claim: ResearchClaim = serde_json::from_slice(&payload)?;
                        claim
                            .validate()
                            .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;
                        claim.source_refs()
                    }
                    ArtifactKind::Critique => {
                        let critique: ResearchCritique = serde_json::from_slice(&payload)?;
                        critique
                            .validate()
                            .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;
                        critique.source_refs()
                    }
                    _ => unreachable!("DecisionProposal references are schema-bounded"),
                };
                if source_refs
                    .iter()
                    .any(|source| !declared_evidence.contains(source))
                {
                    return Err(ResearchError::InvalidOutput(
                        "DecisionProposal evidence does not close over claim/critique grounds"
                            .to_owned(),
                    ));
                }
                refs.extend(source_refs);
            }
            refs.sort();
            refs.dedup();
            refs
        }
        _ => return Ok(vec![]),
    };
    let selected = manifest
        .payload
        .selections
        .iter()
        .map(|selection| selection.artifact.clone())
        .collect::<BTreeSet<_>>();
    if refs.iter().any(|reference| !selected.contains(reference)) {
        return Err(ResearchError::InvalidOutput(
            "research artifact cited an artifact outside ContextManifest".to_owned(),
        ));
    }
    Ok(refs)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentToolCall {
    pub call_id: String,
    pub name: String,
    pub arguments: Value,
}

pub const TERMINAL_SUBMISSION_TOOL: &str = "submit_result";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentTurnPhase {
    Draft,
    Submit,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentTerminalDefinition {
    pub description: String,
    pub input_schema: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentTerminalSubmission {
    pub call_id: String,
    pub arguments: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentModelRequest {
    pub contract_hash: akzio_domain::ContentHash,
    pub purpose: String,
    pub phase: AgentTurnPhase,
    pub prompt: String,
    pub objective: String,
    pub manifest_artifact_id: ArtifactId,
    pub context: Vec<Value>,
    pub continuation: Option<ModelContinuation>,
    pub tool_outputs: Vec<ModelToolOutput>,
    pub continuation_instruction: Option<String>,
    pub max_output_tokens: u32,
    pub tools: Vec<AgentToolDefinition>,
    pub terminal: Option<AgentTerminalDefinition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub strict: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentTurnTelemetry {
    pub latency_millis: u64,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentModelTurn {
    pub assistant_text: Option<String>,
    pub tool_calls: Vec<AgentToolCall>,
    pub terminal_submission: Option<AgentTerminalSubmission>,
    pub continuation: ModelContinuation,
    pub telemetry: Option<AgentTurnTelemetry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_debug: Option<ModelCallTrace>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
#[allow(clippy::enum_variant_names)]
pub enum AgentReasoningEvent {
    ReasoningStart {
        run_id: RunId,
        task_id: TaskId,
        attempt_id: AttemptId,
        purpose: String,
        turn: u16,
    },
    ReasoningDelta {
        run_id: RunId,
        task_id: TaskId,
        attempt_id: AttemptId,
        purpose: String,
        turn: u16,
        delta: String,
    },
    ReasoningEnd {
        run_id: RunId,
        task_id: TaskId,
        attempt_id: AttemptId,
        purpose: String,
        turn: u16,
    },
}

impl AgentReasoningEvent {
    pub fn event_name(&self) -> &'static str {
        match self {
            Self::ReasoningStart { .. } => "reasoning-start",
            Self::ReasoningDelta { .. } => "reasoning-delta",
            Self::ReasoningEnd { .. } => "reasoning-end",
        }
    }

    pub fn run_id(&self) -> &RunId {
        match self {
            Self::ReasoningStart { run_id, .. }
            | Self::ReasoningDelta { run_id, .. }
            | Self::ReasoningEnd { run_id, .. } => run_id,
        }
    }
}

type ModelEventSink = Arc<dyn Fn(ModelStreamEvent) + Send + Sync>;

#[derive(Debug, Clone, PartialEq)]
struct ToolResult {
    value: Value,
    artifact: Artifact,
}

struct TurnRecord<'a> {
    permit: &'a TaskWritePermit,
    contract: &'a AgentContract,
    manifest: &'a ContextManifest,
    turn: u16,
    attempt: u8,
    now: DateTime<Utc>,
}

/// Deliberately tiny seam. The production `akzio-model` adapter and fixture tests
/// both implement this; no execution/policy authority crosses it.
pub trait AgentModel: Send + Sync {
    fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        ModelCapabilitySnapshot::unknown()
    }

    fn response_language(&self) -> Option<&str> {
        None
    }

    fn turn<'a>(
        &'a self,
        request: AgentModelRequest,
    ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>>;

    fn turn_with_events<'a>(
        &'a self,
        request: AgentModelRequest,
        _on_event: ModelEventSink,
    ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
        self.turn(request)
    }
}

#[derive(Debug, Clone)]
pub struct ModelClientAdapter {
    client: ModelClient,
    debug: bool,
    response_language: String,
}

impl ModelClientAdapter {
    pub fn new(client: ModelClient) -> Self {
        Self::with_debug(client, false)
    }

    pub fn with_debug(client: ModelClient, debug: bool) -> Self {
        Self::with_response_language(client, debug, "简体中文")
    }

    pub fn with_response_language(
        client: ModelClient,
        debug: bool,
        response_language: impl Into<String>,
    ) -> Self {
        Self {
            client,
            debug,
            response_language: response_language.into(),
        }
    }
}

impl AgentModel for ModelClientAdapter {
    fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        self.client.capability_snapshot()
    }

    fn response_language(&self) -> Option<&str> {
        Some(&self.response_language)
    }

    fn turn<'a>(
        &'a self,
        request: AgentModelRequest,
    ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
        self.turn_with_events(request, Arc::new(|_| {}))
    }

    fn turn_with_events<'a>(
        &'a self,
        request: AgentModelRequest,
        on_event: ModelEventSink,
    ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
        Box::pin(async move {
            let terminal_name = request
                .terminal
                .as_ref()
                .map(|_| TERMINAL_SUBMISSION_TOOL.to_owned());
            let input = match request.continuation {
                Some(continuation) => ModelInput::Continue {
                    continuation,
                    tool_outputs: request.tool_outputs,
                    instruction: request.continuation_instruction,
                },
                None => ModelInput::Fresh {
                    text: serde_json::to_string(&json!({
                        "objective": request.objective,
                        "context_manifest": request.manifest_artifact_id,
                        "context": request.context,
                    }))?,
                },
            };
            let mut tools = request
                .tools
                .into_iter()
                .map(|tool| ModelToolDefinition {
                    name: tool.name,
                    description: tool.description,
                    input_schema: tool.input_schema,
                    strict: tool.strict,
                })
                .collect::<Vec<_>>();
            if let Some(terminal) = request.terminal {
                tools.push(ModelToolDefinition {
                    name: TERMINAL_SUBMISSION_TOOL.to_owned(),
                    description: terminal.description,
                    input_schema: terminal.input_schema,
                    strict: true,
                });
            }
            let tool_choice = match terminal_name {
                Some(name) => ModelToolChoice::RequiredFunction(name),
                None if tools.is_empty() => ModelToolChoice::None,
                None => ModelToolChoice::Auto,
            };
            let request = ModelRequest {
                instructions: request.prompt,
                input,
                max_output_tokens: request.max_output_tokens,
                tools,
                tool_choice,
                fixture_key: Some(request.purpose),
            };
            let debug_request = self.debug.then(|| self.client.request_body(&request));
            let started = Instant::now();
            let response = self
                .client
                .respond_with_events(request, move |event| on_event(event))
                .await
                .map_err(|error| {
                    let trace = debug_request.map(|request| ModelCallTrace {
                        request,
                        result: model_error_result(&error),
                    });
                    model_client_error(error, trace)
                })?;
            let telemetry = AgentTurnTelemetry {
                latency_millis: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                input_tokens: response
                    .raw
                    .pointer("/usage/input_tokens")
                    .and_then(Value::as_u64),
                output_tokens: response
                    .raw
                    .pointer("/usage/output_tokens")
                    .and_then(Value::as_u64),
            };
            let model_debug = self.debug.then(|| ModelCallTrace {
                request: response.request_body.clone(),
                result: response.raw.clone(),
            });
            let assistant_text = (!response.output_text.trim().is_empty())
                .then(|| response.output_text.trim().to_owned());
            let mut terminal_submission = None;
            let mut tool_calls = Vec::new();
            for call in response.tool_calls {
                if call.name == TERMINAL_SUBMISSION_TOOL {
                    if terminal_submission.is_some() {
                        return Err(ResearchError::AmbiguousSubmission);
                    }
                    terminal_submission = Some(AgentTerminalSubmission {
                        call_id: call.call_id,
                        arguments: call.arguments,
                    });
                } else {
                    tool_calls.push(AgentToolCall {
                        call_id: call.call_id,
                        name: call.name,
                        arguments: call.arguments,
                    });
                }
            }
            Ok(AgentModelTurn {
                assistant_text,
                tool_calls,
                terminal_submission,
                continuation: response.continuation,
                telemetry: Some(telemetry),
                model_debug,
            })
        })
    }
}

#[derive(Debug, Clone)]
pub struct AgentRuntime {
    store: V2Store,
    context: ContextBroker,
    catalogue: ContractCatalogue,
    grant_ttl: Duration,
    reasoning_events: Option<broadcast::Sender<AgentReasoningEvent>>,
}

impl AgentRuntime {
    pub fn new(store: V2Store, catalogue: ContractCatalogue, grant_ttl: Duration) -> Self {
        Self {
            context: ContextBroker::new(store.clone()),
            store,
            catalogue,
            grant_ttl,
            reasoning_events: None,
        }
    }

    pub fn with_reasoning_events(
        mut self,
        reasoning_events: broadcast::Sender<AgentReasoningEvent>,
    ) -> Self {
        self.reasoning_events = Some(reasoning_events);
        self
    }

    pub fn catalogue(&self) -> &ContractCatalogue {
        &self.catalogue
    }

    fn validate_authority_permit(&self, permit: &TaskWritePermit) -> ResearchResult<()> {
        Ok(self.store.validate_task_permit(permit)?)
    }

    fn load_parent_succeeded_attempt(
        &self,
        run_id: &RunId,
        parent_task_id: &TaskId,
    ) -> ResearchResult<akzio_store::v2::SucceededAttemptProof> {
        Ok(self
            .store
            .current_succeeded_attempt(run_id, parent_task_id)?)
    }

    pub(super) fn read_authority_blob(
        &self,
        blob_ref: &akzio_domain::BlobRef,
    ) -> ResearchResult<Vec<u8>> {
        Ok(self.store.read_blob(blob_ref)?)
    }

    fn run_purpose_for(&self, run_id: &RunId) -> ResearchResult<RunPurpose> {
        Ok(self.store.run_purpose(run_id)?)
    }

    pub async fn run(
        &self,
        permit: &TaskWritePermit,
        node: &WorkflowNode,
        candidates: impl IntoIterator<Item = ArtifactRef>,
        model: &dyn AgentModel,
        now: DateTime<Utc>,
    ) -> ResearchResult<Artifact> {
        self.validate_authority_permit(permit)?;
        if permit.task_id != node.task_id {
            return Err(ResearchError::TaskMismatch);
        }
        let contract_hash = node
            .contract_hash
            .as_ref()
            .ok_or(ResearchError::MissingContractHash)?;
        if permit.contract_hash.as_ref() != Some(contract_hash) {
            return Err(ResearchError::ContractMismatch);
        }
        let installed = self.catalogue.get(contract_hash)?;
        if node.budget != installed.contract.budget
            || node.retry != installed.contract.retry
            || node.on_failure != installed.contract.on_failure
        {
            return Err(ResearchError::NodePolicyMismatch);
        }
        let manifest = if let Some(parent_task_id) = &node.parent_task_id {
            if !node.dependencies.contains(parent_task_id) {
                return Err(ResearchError::InvalidOutput(
                    "parent task is not a declared dependency".to_owned(),
                ));
            }
            let proof = self.load_parent_succeeded_attempt(&permit.run_id, parent_task_id)?;
            let parent_contract_hash = proof.contract_hash.as_ref().ok_or_else(|| {
                ResearchError::InvalidOutput("parent attempt has no contract hash".to_owned())
            })?;
            let parent_contract = &self.catalogue.get(parent_contract_hash)?.contract;
            self.context.assemble_child_from_proof(
                &proof,
                parent_contract,
                permit,
                &installed.contract,
                now,
                self.grant_ttl,
            )?
        } else {
            self.context
                .assemble(permit, &installed.contract, candidates, now, self.grant_ttl)?
        };
        if !manifest.grant.matches_permit(permit) {
            return Err(ResearchError::GrantPermitMismatch);
        }
        let context = self.context_values(permit, &installed.contract, &manifest, now)?;
        let governance =
            String::from_utf8(self.read_authority_blob(&installed.contract.prompt.governance)?)
                .map_err(|_| {
                    ResearchError::InvalidOutput("governance prompt is not UTF-8".to_owned())
                })?;
        let role = String::from_utf8(self.read_authority_blob(&installed.contract.prompt.role)?)
            .map_err(|_| ResearchError::InvalidOutput("role prompt is not UTF-8".to_owned()))?;
        let response_language = model.response_language().unwrap_or("简体中文").trim();
        let prompt = format!(
            "{governance}\n\n{role}\n\nDuring Draft, use granted read tools as needed, then return a concise, auditable research memo in {response_language}. State conclusions, evidence, counter-evidence, and uncertainty without exposing hidden chain-of-thought. During Submit, call submit_result exactly once; keep JSON property names, enum literals, identifiers, symbols, and cited source text unchanged."
        );
        let output_schema: Value =
            serde_json::from_slice(&self.read_authority_blob(&installed.contract.output.schema)?)?;
        let run_purpose = self.run_purpose_for(&permit.run_id)?;
        let tools = if !should_advertise_read_tools(
            run_purpose,
            context.len(),
            installed.contract.budget.max_tool_calls,
        ) {
            Vec::new()
        } else {
            model_tool_definitions(&self.store, &installed.contract)?
        };
        let mut continuation = None;
        let mut pending_tool_outputs = Vec::new();
        let mut trace_refs = Vec::new();
        let mut tool_calls = 0_u16;
        let mut model_turn = 0_u16;
        let mut provider_calls = 0_u32;
        let max_provider_calls = u32::from(installed.contract.retry.max_attempts)
            .saturating_mul(u32::from(installed.contract.budget.max_tool_calls) + 3);
        let mut phase = AgentTurnPhase::Draft;
        let mut submission_attempts = 0_u8;
        let started = Instant::now();
        let wall_time =
            StdDuration::from_secs(u64::from(installed.contract.budget.max_wall_time_secs));
        loop {
            if started.elapsed() > wall_time {
                return Err(ResearchError::WallTimeExceeded {
                    maximum_secs: installed.contract.budget.max_wall_time_secs,
                });
            }
            let request = AgentModelRequest {
                contract_hash: installed.contract.contract_hash.clone(),
                purpose: installed.contract.purpose.as_str().to_owned(),
                phase,
                prompt: prompt.clone(),
                objective: node.objective.clone(),
                manifest_artifact_id: manifest.artifact.artifact_id.clone(),
                context: if continuation.is_none() {
                    context.clone()
                } else {
                    Vec::new()
                },
                continuation: continuation.clone(),
                tool_outputs: pending_tool_outputs.clone(),
                continuation_instruction: (phase == AgentTurnPhase::Submit
                    && pending_tool_outputs.is_empty())
                .then(|| {
                    "The research memo is complete. Call submit_result exactly once with the final contract output. Do not call any other tool or add assistant text."
                        .to_owned()
                }),
                max_output_tokens: installed.contract.budget.max_output_tokens,
                tools: if phase == AgentTurnPhase::Draft {
                    tools.clone()
                } else {
                    Vec::new()
                },
                terminal: (phase == AgentTurnPhase::Submit).then(|| AgentTerminalDefinition {
                    description: format!(
                        "Submit the final {} contract output for Rust validation. This has no side effects.",
                        installed.contract.purpose.as_str()
                    ),
                    input_schema: output_schema.clone(),
                }),
            };
            let input_tokens = estimate_tokens(&request)?;
            if input_tokens > installed.contract.budget.max_input_tokens {
                return Err(ResearchError::InputBudgetExceeded {
                    actual: input_tokens,
                    maximum: installed.contract.budget.max_input_tokens,
                });
            }
            let tool_set_hash = tool_set_hash(&request)?;
            let mut turn_attempt = 1_u8;
            let (turn, capability_snapshot, capability_snapshot_hash, request_hash) = loop {
                let capability_snapshot = model.capability_snapshot();
                let capability_snapshot_hash = capability_snapshot_hash(&capability_snapshot)?;
                if let Err(capability) = validate_model_capabilities(&capability_snapshot, &request)
                {
                    let turn_now = logical_now(now, started.elapsed());
                    let failed_turn = self.record_failed_turn(
                        &TurnRecord {
                            permit,
                            contract: &installed.contract,
                            manifest: &manifest,
                            turn: model_turn,
                            attempt: turn_attempt,
                            now: turn_now,
                        },
                        &request,
                        "capability_mismatch",
                        None,
                        None,
                        false,
                        &capability_snapshot,
                        &capability_snapshot_hash,
                        &tool_set_hash,
                    )?;
                    trace_refs.push(ArtifactRef {
                        artifact_id: failed_turn.artifact_id,
                        kind: ArtifactKind::AgentTurn,
                    });
                    return Err(capability);
                }
                let request_hash = model_request_hash(&request)?;
                self.validate_authority_permit(permit)?;
                self.store.append_task_event(
                    permit,
                    LifecycleEventType::AgentTurnStarted,
                    logical_now(now, started.elapsed()),
                )?;
                let sender = self.reasoning_events.clone();
                let run_id = permit.run_id.clone();
                let task_id = permit.task_id.clone();
                let attempt_id = permit.attempt_id.clone();
                let purpose = request.purpose.clone();
                let on_event: ModelEventSink = Arc::new(move |event| {
                    let Some(sender) = &sender else {
                        return;
                    };
                    let event = match event {
                        ModelStreamEvent::ReasoningStart => AgentReasoningEvent::ReasoningStart {
                            run_id: run_id.clone(),
                            task_id: task_id.clone(),
                            attempt_id: attempt_id.clone(),
                            purpose: purpose.clone(),
                            turn: model_turn,
                        },
                        ModelStreamEvent::ReasoningDelta(delta) => {
                            AgentReasoningEvent::ReasoningDelta {
                                run_id: run_id.clone(),
                                task_id: task_id.clone(),
                                attempt_id: attempt_id.clone(),
                                purpose: purpose.clone(),
                                turn: model_turn,
                                delta,
                            }
                        }
                        ModelStreamEvent::ReasoningEnd => AgentReasoningEvent::ReasoningEnd {
                            run_id: run_id.clone(),
                            task_id: task_id.clone(),
                            attempt_id: attempt_id.clone(),
                            purpose: purpose.clone(),
                            turn: model_turn,
                        },
                    };
                    let _ = sender.send(event);
                });
                if provider_calls >= max_provider_calls {
                    return Err(ResearchError::ModelCallBudgetExceeded);
                }
                provider_calls = provider_calls.saturating_add(1);
                match model.turn_with_events(request.clone(), on_event).await {
                    Ok(turn) => {
                        break (
                            turn,
                            capability_snapshot,
                            capability_snapshot_hash,
                            request_hash,
                        );
                    }
                    Err(error) => {
                        let retryable = retryable_model_error(&error, &installed.contract.retry);
                        let will_retry =
                            retryable && turn_attempt < installed.contract.retry.max_attempts;
                        let turn_now = logical_now(now, started.elapsed());
                        let failed_turn = self.record_failed_turn(
                            &TurnRecord {
                                permit,
                                contract: &installed.contract,
                                manifest: &manifest,
                                turn: model_turn,
                                attempt: turn_attempt,
                                now: turn_now,
                            },
                            &request,
                            model_error_class(&error),
                            Some(research_error_detail(&error)),
                            model_debug_trace(&error),
                            will_retry,
                            &capability_snapshot,
                            &capability_snapshot_hash,
                            &tool_set_hash,
                        )?;
                        trace_refs.push(ArtifactRef {
                            artifact_id: failed_turn.artifact_id,
                            kind: ArtifactKind::AgentTurn,
                        });
                        if !will_retry {
                            return Err(error);
                        }
                        let backoff = StdDuration::from_millis(
                            installed
                                .contract
                                .retry
                                .initial_backoff_ms
                                .saturating_mul(u64::from(turn_attempt)),
                        );
                        if backoff > wall_time.saturating_sub(started.elapsed()) {
                            return Err(ResearchError::WallTimeExceeded {
                                maximum_secs: installed.contract.budget.max_wall_time_secs,
                            });
                        }
                        if !backoff.is_zero() {
                            tokio::time::sleep(backoff).await;
                        }
                        turn_attempt = turn_attempt.saturating_add(1);
                    }
                }
            };
            if started.elapsed() > wall_time {
                let turn_now = logical_now(now, started.elapsed());
                let failed_turn = self.record_failed_turn(
                    &TurnRecord {
                        permit,
                        contract: &installed.contract,
                        manifest: &manifest,
                        turn: model_turn,
                        attempt: turn_attempt,
                        now: turn_now,
                    },
                    &request,
                    "wall_time",
                    None,
                    None,
                    false,
                    &capability_snapshot,
                    &capability_snapshot_hash,
                    &tool_set_hash,
                )?;
                trace_refs.push(ArtifactRef {
                    artifact_id: failed_turn.artifact_id,
                    kind: ArtifactKind::AgentTurn,
                });
                return Err(ResearchError::WallTimeExceeded {
                    maximum_secs: installed.contract.budget.max_wall_time_secs,
                });
            }
            let turn_now = logical_now(now, started.elapsed());
            let turn_artifact = self.record_turn(
                &TurnRecord {
                    permit,
                    contract: &installed.contract,
                    manifest: &manifest,
                    turn: model_turn,
                    attempt: turn_attempt,
                    now: turn_now,
                },
                &request,
                &turn,
                &capability_snapshot,
                &capability_snapshot_hash,
                &tool_set_hash,
            )?;
            trace_refs.push(ArtifactRef {
                artifact_id: turn_artifact.artifact_id,
                kind: ArtifactKind::AgentTurn,
            });
            continuation = Some(turn.continuation.clone());
            pending_tool_outputs.clear();
            if phase == AgentTurnPhase::Draft && turn.terminal_submission.is_some() {
                return Err(ResearchError::AmbiguousSubmission);
            }
            if phase == AgentTurnPhase::Draft && !turn.tool_calls.is_empty() {
                let next = tool_calls.saturating_add(turn.tool_calls.len() as u16);
                if next > installed.contract.budget.max_tool_calls {
                    return Err(ResearchError::ToolBudgetExceeded);
                }
                for call in turn.tool_calls {
                    let call_id = call.call_id.clone();
                    let tool_result = self.execute_tool(
                        permit,
                        &installed.contract,
                        &manifest.grant,
                        &call,
                        &request_hash,
                        turn_now,
                    )?;
                    trace_refs.push(ArtifactRef {
                        artifact_id: tool_result.artifact.artifact_id.clone(),
                        kind: ArtifactKind::ToolResult,
                    });
                    pending_tool_outputs.push(ModelToolOutput {
                        call_id,
                        output: tool_result.value,
                    });
                }
                tool_calls = next;
                model_turn = model_turn.saturating_add(1);
                continue;
            }
            if phase == AgentTurnPhase::Draft {
                let memo = turn
                    .assistant_text
                    .as_deref()
                    .filter(|text| !text.trim().is_empty())
                    .ok_or(ResearchError::MissingFinalOutput)?;
                let output_tokens = estimate_tokens(&memo)?;
                if output_tokens > installed.contract.budget.max_output_tokens {
                    return Err(ResearchError::OutputBudgetExceeded {
                        actual: output_tokens,
                        maximum: installed.contract.budget.max_output_tokens,
                    });
                }
                phase = AgentTurnPhase::Submit;
                model_turn = model_turn.saturating_add(1);
                continue;
            }

            if !turn.tool_calls.is_empty() || turn.assistant_text.is_some() {
                return Err(ResearchError::AmbiguousSubmission);
            }
            let submission = turn
                .terminal_submission
                .ok_or(ResearchError::MissingFinalOutput)?;
            let output_tokens = estimate_tokens(&submission.arguments)?;
            if output_tokens > installed.contract.budget.max_output_tokens {
                return Err(ResearchError::OutputBudgetExceeded {
                    actual: output_tokens,
                    maximum: installed.contract.budget.max_output_tokens,
                });
            }

            let validated = (|| {
                validate_submission_schema(
                    &self.store,
                    &installed.contract,
                    &submission.arguments,
                )?;
                let (output, deliberation_note) = self.extract_deliberation(
                    permit,
                    &installed.contract,
                    &manifest,
                    submission.arguments.clone(),
                    turn_now,
                )?;
                validate_output_schema(&self.store, &installed.contract, &output)?;
                let research_sources = research_output_source_refs(
                    &self.store,
                    installed.contract.output.artifact_kind,
                    &output,
                    &manifest,
                )?;
                Ok::<_, ResearchError>((output, deliberation_note, research_sources))
            })();

            let (output, deliberation_note, research_sources) = match validated {
                Ok(validated) => validated,
                Err(error @ ResearchError::InvalidOutput(_))
                    if submission_attempts.saturating_add(1)
                        < installed.contract.retry.max_attempts =>
                {
                    submission_attempts = submission_attempts.saturating_add(1);
                    pending_tool_outputs.push(ModelToolOutput {
                        call_id: submission.call_id,
                        output: json!({
                            "ok": false,
                            "error": "invalid_submission",
                            "message": error.to_string(),
                        }),
                    });
                    model_turn = model_turn.saturating_add(1);
                    continue;
                }
                Err(error) => return Err(error),
            };
            if let Some(note) = deliberation_note {
                self.store.write_task_artifact(
                    permit,
                    &note,
                    LifecycleEventType::DeliberationNoteCreated,
                    turn_now,
                )?;
                trace_refs.push(ArtifactRef {
                    artifact_id: note.artifact_id,
                    kind: ArtifactKind::DeliberationNote,
                });
            }
            let output_artifact = Artifact::new(
                installed.contract.output.artifact_kind,
                self.store.put_json(&output)?,
                format!("agent.{}", installed.contract.purpose.as_str()),
                ArtifactLifecycle::RunScoped,
                ArtifactProvenance {
                    source_family: "akzio.agent".to_owned(),
                    observed_at: None,
                    retrieved_at: turn_now,
                    source_uri: None,
                    confidence_ppm: 1_000_000,
                    producer_contract_hash: Some(installed.contract.contract_hash.clone()),
                },
                Some(permit.artifact_origin()),
                std::iter::once(ArtifactRef {
                    artifact_id: manifest.artifact.artifact_id.clone(),
                    kind: ArtifactKind::ContextManifest,
                })
                .chain(trace_refs)
                .chain(research_sources)
                .collect(),
                turn_now,
            )?;
            return Ok(output_artifact);
        }
    }

    fn extract_deliberation(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        manifest: &ContextManifest,
        output: Value,
        now: DateTime<Utc>,
    ) -> ResearchResult<(Value, Option<Artifact>)> {
        if contract.deliberation_policy == DeliberationPolicy::Disabled {
            return Ok((output, None));
        }
        let mut envelope: AgentOutputEnvelope =
            serde_json::from_value(output).map_err(|error| {
                ResearchError::InvalidOutput(format!("deliberation envelope: {error}"))
            })?;
        envelope.deliberation.assessment_source = Some("model_assessed".to_owned());
        envelope
            .deliberation
            .validate_model_assessment()
            .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;

        let selected = manifest
            .payload
            .selections
            .iter()
            .map(|selection| {
                (
                    selection.artifact.artifact_id.clone(),
                    selection.artifact.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut basis_refs = Vec::new();
        for basis_id in &envelope.deliberation.basis_artifact_ids {
            if *basis_id == manifest.artifact.artifact_id {
                basis_refs.push(ArtifactRef {
                    artifact_id: basis_id.clone(),
                    kind: ArtifactKind::ContextManifest,
                });
            } else if let Some(reference) = selected.get(basis_id) {
                basis_refs.push(reference.clone());
            } else {
                return Err(ResearchError::InvalidOutput(
                    "deliberation basis is outside the ContextManifest".to_owned(),
                ));
            }
        }
        let note = Artifact::new(
            ArtifactKind::DeliberationNote,
            self.store.put_json(&envelope.deliberation)?,
            format!("agent.deliberation.{}", contract.purpose.as_str()),
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.agent".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: envelope.deliberation.confidence_ppm,
                producer_contract_hash: Some(contract.contract_hash.clone()),
            },
            Some(permit.artifact_origin()),
            std::iter::once(ArtifactRef {
                artifact_id: manifest.artifact.artifact_id.clone(),
                kind: ArtifactKind::ContextManifest,
            })
            .chain(basis_refs)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
            now,
        )?;
        Ok((envelope.result, Some(note)))
    }

    fn context_values(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        manifest: &ContextManifest,
        now: DateTime<Utc>,
    ) -> ResearchResult<Vec<Value>> {
        if !manifest.grant.matches_permit(permit) {
            return Err(ResearchError::GrantPermitMismatch);
        }
        manifest
            .payload
            .selections
            .iter()
            .map(|selection| {
                let (artifact, value) = self.context.read_document(
                    permit,
                    contract,
                    &manifest.grant,
                    &selection.artifact.artifact_id,
                    now,
                )?;
                Ok(json!({
                    "artifact_id": artifact.artifact_id,
                    "kind": artifact.kind,
                    "provenance": artifact.provenance,
                    "value": value,
                }))
            })
            .collect()
    }

    fn record_turn(
        &self,
        record: &TurnRecord<'_>,
        request: &AgentModelRequest,
        response: &AgentModelTurn,
        capability_snapshot: &ModelCapabilitySnapshot,
        capability_snapshot_hash: &akzio_domain::ContentHash,
        tool_set_hash: &akzio_domain::ContentHash,
    ) -> ResearchResult<Artifact> {
        let request_hash = model_request_hash(request)?;
        let artifact = Artifact::new(
            ArtifactKind::AgentTurn,
            self.store.put_json(&json!({
                "turn": record.turn,
                "attempt": record.attempt,
                "contract_hash": record.contract.contract_hash,
                "context_manifest": record.manifest.artifact.artifact_id,
                "request_hash": request_hash,
                "capability_snapshot": capability_snapshot,
                "capability_snapshot_hash": capability_snapshot_hash,
                "tool_set_hash": tool_set_hash,
                "request": request,
                "response": response,
            }))?,
            format!("agent.turn.{}", record.contract.purpose.as_str()),
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.agent".to_owned(),
                observed_at: None,
                retrieved_at: record.now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: Some(record.contract.contract_hash.clone()),
            },
            Some(record.permit.artifact_origin()),
            vec![ArtifactRef {
                artifact_id: record.manifest.artifact.artifact_id.clone(),
                kind: ArtifactKind::ContextManifest,
            }],
            record.now,
        )?;
        self.store.write_task_artifact(
            record.permit,
            &artifact,
            LifecycleEventType::AgentTurnCompleted,
            record.now,
        )?;
        Ok(artifact)
    }

    #[allow(clippy::too_many_arguments)]
    fn record_failed_turn(
        &self,
        record: &TurnRecord<'_>,
        request: &AgentModelRequest,
        error_class: &str,
        error_detail: Option<Value>,
        model_debug: Option<&ModelCallTrace>,
        will_retry: bool,
        capability_snapshot: &ModelCapabilitySnapshot,
        capability_snapshot_hash: &akzio_domain::ContentHash,
        tool_set_hash: &akzio_domain::ContentHash,
    ) -> ResearchResult<Artifact> {
        let request_hash = model_request_hash(request)?;
        let mut trace = json!({
            "turn": record.turn,
            "attempt": record.attempt,
            "contract_hash": record.contract.contract_hash,
            "context_manifest": record.manifest.artifact.artifact_id,
            "request_hash": request_hash,
            "capability_snapshot": capability_snapshot,
            "capability_snapshot_hash": capability_snapshot_hash,
            "tool_set_hash": tool_set_hash,
            "request": request,
            "error_class": error_class,
            "will_retry": will_retry,
        });
        if let Some(error_detail) = error_detail {
            trace["error_detail"] = error_detail;
        }
        if let Some(model_debug) = model_debug {
            trace["model_debug"] = serde_json::to_value(model_debug)?;
        }
        let artifact = Artifact::new(
            ArtifactKind::AgentTurn,
            self.store.put_json(&trace)?,
            format!("agent.turn.{}", record.contract.purpose.as_str()),
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.agent".to_owned(),
                observed_at: None,
                retrieved_at: record.now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: Some(record.contract.contract_hash.clone()),
            },
            Some(record.permit.artifact_origin()),
            vec![ArtifactRef {
                artifact_id: record.manifest.artifact.artifact_id.clone(),
                kind: ArtifactKind::ContextManifest,
            }],
            record.now,
        )?;
        self.store.write_task_artifact(
            record.permit,
            &artifact,
            if will_retry {
                LifecycleEventType::AgentTurnRetryableFailed
            } else {
                LifecycleEventType::AgentTurnFailed
            },
            record.now,
        )?;
        Ok(artifact)
    }
}

fn should_advertise_read_tools(
    purpose: RunPurpose,
    context_len: usize,
    max_tool_calls: u16,
) -> bool {
    context_len > 0
        && context_len <= usize::from(max_tool_calls)
        && purpose != RunPurpose::PaperDryRun
}

fn estimate_tokens<T: Serialize>(value: &T) -> ResearchResult<u32> {
    Ok(akzio_domain::estimate_json_tokens(value)?)
}

fn model_request_hash(request: &AgentModelRequest) -> ResearchResult<akzio_domain::ContentHash> {
    Ok(akzio_domain::content_hash_json(&serde_json::to_value(
        request,
    )?)?)
}

fn capability_snapshot_hash(
    snapshot: &ModelCapabilitySnapshot,
) -> ResearchResult<akzio_domain::ContentHash> {
    Ok(akzio_domain::content_hash_json(&serde_json::to_value(
        snapshot,
    )?)?)
}

fn research_error_detail(error: &ResearchError) -> Value {
    match error {
        ResearchError::Model(message)
        | ResearchError::RateLimited(message)
        | ResearchError::InvalidOutput(message) => json!({
            "kind": model_error_class(error),
            "message": sanitize_provider_text(message),
        }),
        ResearchError::ModelDebug {
            error_class,
            message,
            ..
        } => json!({
            "kind": error_class,
            "message": sanitize_provider_text(message),
        }),
        _ => json!({ "kind": model_error_class(error) }),
    }
}

fn sanitize_provider_text(value: &str) -> String {
    let mut sanitized = value
        .replace("Authorization", "[redacted-header]")
        .replace("authorization", "[redacted-header]")
        .replace("api_key", "[redacted-key]")
        .replace("api-key", "[redacted-key]");
    if sanitized.chars().count() > 512 {
        sanitized = sanitized.chars().take(512).collect();
        sanitized.push_str("...");
    }
    sanitized
}

fn model_error_result(error: &ModelError) -> Value {
    match error {
        ModelError::Http { status, body } => json!({
            "status": status.as_u16(),
            "body": serde_json::from_str::<Value>(body)
                .unwrap_or_else(|_| Value::String(body.clone())),
        }),
        ModelError::Transport(error) => json!({
            "error": "transport",
            "message": sanitize_provider_text(&error.to_string()),
        }),
        ModelError::InvalidStream(_) => json!({"error": "invalid_stream"}),
        ModelError::Refused(message) => json!({"error": "refused", "message": message}),
        ModelError::Incomplete(reason) => json!({"error": "incomplete", "reason": reason}),
        ModelError::MissingOutput => json!({"error": "missing_output"}),
        ModelError::FixtureExhausted => json!({"error": "fixture_exhausted"}),
        ModelError::NativeWebUnavailable
        | ModelError::NativeWebToolNotAllowed
        | ModelError::NativeWebArgumentsInvalid
        | ModelError::NativeWebCitationsMissing
        | ModelError::NativeWebUnsafeCitation
        | ModelError::NativeWebLimitExceeded => json!({"error": "native_web_contract"}),
        ModelError::EmptyBaseUrl
        | ModelError::EmptyApiKey
        | ModelError::EmptyModel
        | ModelError::EmptyReasoningEffort => json!({"error": "configuration"}),
    }
}

fn model_client_error(error: ModelError, trace: Option<ModelCallTrace>) -> ResearchError {
    let (error_class, message) = match error {
        ModelError::Transport(error) => ("transport", sanitize_provider_text(&error.to_string())),
        ModelError::Http { status, body } if status.as_u16() == 429 => (
            "rate_limited",
            format!("HTTP 429: {}", sanitize_provider_text(&body)),
        ),
        ModelError::Http { status, body } => (
            "transport",
            format!(
                "HTTP {}: {}",
                status.as_u16(),
                sanitize_provider_text(&body)
            ),
        ),
        ModelError::EmptyBaseUrl => ("configuration", "invalid base URL".to_owned()),
        ModelError::EmptyApiKey => ("configuration", "missing API key".to_owned()),
        ModelError::EmptyModel => ("configuration", "missing model name".to_owned()),
        ModelError::EmptyReasoningEffort => {
            ("configuration", "missing reasoning effort".to_owned())
        }
        ModelError::InvalidStream(_) => ("invalid_output", "invalid response stream".to_owned()),
        ModelError::Refused(message) => return ResearchError::ModelRefused(message),
        ModelError::Incomplete(reason) => ("invalid_output", format!("incomplete: {reason}")),
        ModelError::NativeWebUnavailable
        | ModelError::NativeWebToolNotAllowed
        | ModelError::NativeWebArgumentsInvalid
        | ModelError::NativeWebCitationsMissing
        | ModelError::NativeWebUnsafeCitation
        | ModelError::NativeWebLimitExceeded => (
            "native_web_contract",
            "native web contract rejected response".to_owned(),
        ),
        ModelError::MissingOutput => ("invalid_output", "missing model output".to_owned()),
        ModelError::FixtureExhausted => ("transport", "fixture sequence exhausted".to_owned()),
    };
    if let Some(trace) = trace {
        return ResearchError::ModelDebug {
            error_class,
            message,
            trace,
        };
    }
    match error_class {
        "rate_limited" => ResearchError::RateLimited(message),
        "invalid_output" => ResearchError::InvalidOutput(message),
        _ => ResearchError::Model(message),
    }
}

fn model_debug_trace(error: &ResearchError) -> Option<&ModelCallTrace> {
    match error {
        ResearchError::ModelDebug { trace, .. } => Some(trace),
        _ => None,
    }
}

fn logical_now(start: DateTime<Utc>, elapsed: StdDuration) -> DateTime<Utc> {
    start + Duration::from_std(elapsed).unwrap_or_else(|_| Duration::seconds(i64::MAX))
}

fn retryable_model_error(error: &ResearchError, retry: &akzio_domain::RetryPolicy) -> bool {
    match error {
        ResearchError::InvalidOutput(_) | ResearchError::MissingFinalOutput => {
            retry.retry_invalid_output
        }
        ResearchError::Model(_) => retry.retry_transport,
        ResearchError::RateLimited(_) => retry.retry_rate_limited,
        ResearchError::ModelDebug { error_class, .. } if *error_class == "invalid_output" => {
            retry.retry_invalid_output
        }
        ResearchError::ModelDebug { error_class, .. } if *error_class == "transport" => {
            retry.retry_transport
        }
        ResearchError::ModelDebug { error_class, .. } if *error_class == "rate_limited" => {
            retry.retry_rate_limited
        }
        _ => false,
    }
}

fn model_error_class(error: &ResearchError) -> &'static str {
    match error {
        ResearchError::Model(_) => "transport",
        ResearchError::RateLimited(_) => "rate_limited",
        ResearchError::ModelDebug { error_class, .. } => error_class,
        _ => "other",
    }
}

#[cfg(test)]
mod decision_proposal_tests {
    use super::*;

    fn provenance() -> ArtifactProvenance {
        ArtifactProvenance {
            source_family: "fixture".to_owned(),
            observed_at: None,
            retrieved_at: Utc::now(),
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        }
    }

    fn proposal(
        claims: Vec<ArtifactRef>,
        evidence: Vec<ArtifactRef>,
        hard_blockers: Vec<akzio_domain::HardBlocker>,
    ) -> DecisionDraft {
        let forecasts = akzio_domain::Asset::EXECUTABLE
            .into_iter()
            .flat_map(|asset| {
                [
                    akzio_domain::DecisionHorizon::T1,
                    akzio_domain::DecisionHorizon::T3,
                    akzio_domain::DecisionHorizon::T5,
                ]
                .into_iter()
                .map(move |horizon| akzio_domain::Forecast {
                    asset,
                    horizon,
                    positive_return_probability_ppm: 500_000,
                    expected_return_ppm: 0,
                })
            })
            .collect();
        DecisionDraft {
            summary: "bounded decision".to_owned(),
            confidence_ppm: 700_000,
            forecasts,
            claims,
            critiques: Vec::new(),
            evidence,
            applied_learning_refs: Vec::new(),
            rejected_learning_refs: Vec::new(),
            material_conflicts: Vec::new(),
            hard_blockers,
            soft_warnings: Vec::new(),
        }
    }

    #[test]
    fn decision_proposal_requires_claim_and_evidence_closure() {
        let root = tempfile::tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        let evidence = Artifact::new(
            ArtifactKind::NormalizedEvidence,
            store.put_json(&serde_json::json!({"price": 100})).unwrap(),
            "fixture.evidence",
            ArtifactLifecycle::RunScoped,
            provenance(),
            None,
            Vec::new(),
            now,
        )
        .unwrap();
        let evidence_ref = ArtifactRef {
            artifact_id: evidence.artifact_id.clone(),
            kind: ArtifactKind::NormalizedEvidence,
        };
        let claim_payload = ResearchClaim {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            topic: "market".to_owned(),
            statement: "Evidence is neutral.".to_owned(),
            horizon: akzio_domain::DecisionHorizon::T1,
            stance: akzio_domain::ClaimStance::Neutral,
            materiality_ppm: 700_000,
            confidence_ppm: 700_000,
            grounds: vec![akzio_domain::EvidenceGround {
                evidence: evidence_ref.clone(),
                support: "Observed fixture evidence.".to_owned(),
            }],
            evidence_gaps: Vec::new(),
        };
        let claim = Artifact::new(
            ArtifactKind::Claim,
            store.put_json(&claim_payload).unwrap(),
            "fixture.claim",
            ArtifactLifecycle::RunScoped,
            provenance(),
            None,
            claim_payload.source_refs(),
            now,
        )
        .unwrap();
        let claim_ref = ArtifactRef {
            artifact_id: claim.artifact_id.clone(),
            kind: ArtifactKind::Claim,
        };
        let contract_hash = akzio_domain::ContentHash::of_bytes(b"contract");
        let manifest_payload = akzio_domain::ContextManifestPayload {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            contract_hash: contract_hash.clone(),
            selections: vec![
                akzio_domain::ContextSelection {
                    artifact: claim_ref.clone(),
                    reason: "approved claim".to_owned(),
                    estimated_tokens: 1,
                },
                akzio_domain::ContextSelection {
                    artifact: evidence_ref.clone(),
                    reason: "claim ground".to_owned(),
                    estimated_tokens: 1,
                },
            ],
            total_bytes: 2,
            estimated_tokens: 2,
            input_hash: akzio_domain::ContentHash::of_bytes(b"input"),
        };
        let manifest_artifact = Artifact::new(
            ArtifactKind::ContextManifest,
            store.put_json(&manifest_payload).unwrap(),
            "fixture.manifest",
            ArtifactLifecycle::RunScoped,
            provenance(),
            None,
            Vec::new(),
            now,
        )
        .unwrap();
        let manifest = ContextManifest {
            artifact: manifest_artifact.clone(),
            payload: manifest_payload,
            grant: akzio_domain::ReadGrant {
                manifest_artifact_id: manifest_artifact.artifact_id.clone(),
                run_id: RunId::new(),
                task_id: TaskId::new(),
                attempt_id: akzio_domain::AttemptId::new(),
                lease_id: akzio_domain::LeaseId::new(),
                epoch: 1,
                contract_hash,
                readable: BTreeSet::from([claim.artifact_id.clone(), evidence.artifact_id.clone()]),
                raw_source_closure: BTreeSet::new(),
                expires_at: now + Duration::hours(1),
            },
        };

        let dropped = proposal(
            Vec::new(),
            vec![evidence_ref.clone()],
            vec![akzio_domain::HardBlocker::MissingEvidence],
        );
        let dropped_error = research_output_source_refs(
            &store,
            ArtifactKind::DecisionProposal,
            &serde_json::to_value(dropped).unwrap(),
            &manifest,
        )
        .unwrap_err();
        assert!(dropped_error.to_string().contains("dropped all claims"));
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
