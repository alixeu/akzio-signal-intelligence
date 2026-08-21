//! Contract-driven Agent runtime for the v2 system.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration as StdDuration, Instant},
};

use akzio_context::v2::{ContextBroker, ContextError, ContextManifest};
use akzio_domain::{
    AgentContract, AgentOutputEnvelope, Artifact, ArtifactId, ArtifactKind, ArtifactLifecycle,
    ArtifactProvenance, ArtifactRef, ContextPolicy, ContractId, ContractPurpose,
    DeliberationPolicy, DomainError, FailureDisposition, LifecycleEventType, OutputContract,
    PromptBundle, ReadGrant, ResearchClaim, ResearchCritique, ResearchResolution, RetryPolicy,
    RunPurpose, RuntimeTaskClass, TaskBudget, TaskRecipe, TaskRecipeId, TaskWritePermit,
    TerminationPolicy, ToolGrant, ToolKind, ToolSpec, WorkflowNode, V2_DOMAIN_SCHEMA_VERSION,
    V2_SCHEMA_VERSION,
};
use akzio_model::{
    ModelCallTrace, ModelCapabilitySnapshot, ModelClient, ModelError, ModelRequest,
    ModelToolDefinition,
};
use akzio_runtime::v2::{RecipeCatalogue, RetryCause, RuntimeError, TerminalRecipeSet};
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

use catalogue::{
    ActiveRecipePolicy, ACTIVE_CONTRACT_VERSION, ACTIVE_PROMPT_BUNDLE_VERSION,
    ACTIVE_RECIPE_POLICIES, DECISION_GATE_RECIPE_ID, EVALUATE_RECIPE_ID, EVIDENCE_GATE_RECIPE_ID,
    EXECUTION_GATE_RECIPE_ID, GOVERNED_EVIDENCE_SOURCE_FAMILIES, OUTCOME_WORKER_RECIPE_ID,
    PAPER_COMMIT_RECIPE_ID, PLANNER_CHILD_RECIPE_IDS, PLANNER_MAX_DRAFT_TASKS, PLANNER_RECIPE_ID,
    RECONCILE_RECIPE_ID, RFC3339_TIMESTAMP_PATTERN, SHARED_GOVERNANCE_PROMPT,
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
    #[error("Agent request used {actual} tokens but Contract permits at most {maximum}")]
    InputBudgetExceeded { actual: u32, maximum: u32 },
    #[error("Agent output used {actual} tokens but Contract permits at most {maximum}")]
    OutputBudgetExceeded { actual: u32, maximum: u32 },
    #[error("Agent exceeded its Contract wall-time budget of {maximum_secs} seconds")]
    WallTimeExceeded { maximum_secs: u32 },
    #[error("Agent completed without a final output")]
    MissingFinalOutput,
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
    prompt: &'static str,
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
            prompt: "You are Akzio's bounded research planner. Return only JSON matching the WorkflowProposalDraft schema. You may name only research.analyst and research.synthesizer recipes, and express evidence needs inline. Numeric bounds are strict: priority 0-100, max_age_secs 1-604800, max_results 1-32 (prefer 1-20), at most 4 assets and 7 tasks. window_start and window_end must be null or RFC3339 timestamps; never use natural-language values such as latest. Rust may insert one conditional critic only for the structured-critique candidate topology. Do not construct ArtifactRef values, request sources or tools beyond the contract, submit a decision, or submit an order.",
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
                max_wall_time_secs: 60,
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
            purpose: "research.analyst",
            responsibility: "Produce evidence-linked, bounded research claims for one shard of the approved workflow.",
            prompt: "You are Akzio's research analyst. Return only a JSON Claim. Use granted context and read only artifacts named by the ContextManifest. Do not call external systems, widen sources, change topology, submit decisions, or submit orders.",
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
                max_wall_time_secs: 60,
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
            purpose: "research.critic",
            responsibility: "Challenge material claims and surface evidence or risk gaps without changing facts or execution authority.",
            prompt: "You are Akzio's research critic. Return only a JSON Critique. Challenge supplied claims using granted context. Do not invent evidence, widen sources or tools, alter the workflow, produce a decision, or submit an order.",
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
                max_wall_time_secs: 45,
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
            purpose: "research.synthesizer",
            responsibility: "Synthesize approved claims and critiques into a DecisionProposal with typed blockers for Rust-owned gates.",
            prompt: "You are Akzio's research synthesizer. Return only a JSON DecisionProposal matching the supplied schema. Use only artifacts selected by the ContextManifest. Do not change evidence, bypass the DecisionGate, submit an order, or expand any capability.",
            output_kind: ArtifactKind::DecisionProposal,
            output_schema: decision_proposal_output_schema(),
            permitted_kinds: BTreeSet::from([
                ArtifactKind::Claim,
                ArtifactKind::Critique,
                ArtifactKind::Lesson,
                ArtifactKind::Retrospective,
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
                max_wall_time_secs: 60,
                max_tool_calls: 2,
            },
            termination: TerminationPolicy::leaf(),
            on_failure: FailureDisposition::FailRun,
        },
        CanonicalContractDefinition {
            purpose: OUTCOME_WORKER_RECIPE_ID,
            responsibility: "Produce a bounded retrospective draft from the governed Paper decision and outcome evidence chain.",
            prompt: "You are Akzio's governed outcome reviewer. Return only a RetrospectiveDraft envelope. Use only the granted decision, execution, outcome schedule, market evidence, deliberation notes, and prior retrospectives. Never emit authoritative returns, calibration, slippage, risk recall, or policy decisions.",
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
                max_wall_time_secs: 90,
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
    let role_prompt = match definition.purpose {
        "research.synthesizer" => format!(
            "{}\n\nAlways return exactly 12 forecasts: one for each executable asset (TQQQ, QQQ, SOXX, SOXL) at each horizon (t1, t3, t5), even when the proposal is blocked; for blocked proposals use neutral zero forecasts and explain the blocker in hard_blockers and summary. In deliberation.basis_artifact_ids and result references, use only artifact IDs that appear as top-level selections in the current ContextManifest; do not copy nested evidence IDs unless they are also selected. Preserve each selected artifact's exact kind: use claim only for claim refs, critique only for critique refs, and normalized_evidence or semantic_detail only when that exact kind is selected. ContextManifest deliberation_note selections may appear in basis_artifact_ids but must not be relabeled as result claims, critiques, or evidence.",
            definition.prompt
        ),
        "research.analyst" => format!(
            "{}\n\nKeep evidence_gaps to at most 2 items; combine overlapping limitations into concise, evidence-grounded gaps. Preserve the exact artifact kind shown in ContextManifest selections; do not relabel normalized_evidence as semantic_detail or vice versa.",
            definition.prompt
        ),
        _ => definition.prompt.to_owned(),
    };
    let role_prompt = format!(
        "{role_prompt}\n\nUse at most 8 evidence-relevant IDs in deliberation.basis_artifact_ids. When the output schema contains applied_learning_refs and rejected_learning_refs, list only top-level ContextManifest learning artifacts: applied_learning_refs are lessons or experiences you actually relied on, rejected_learning_refs are reviewed learning artifacts you intentionally did not apply, and both arrays must be empty when no learning artifact was used. Never invent or copy nested artifact IDs."
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

fn governed_context_sources() -> BTreeSet<String> {
    GOVERNED_EVIDENCE_SOURCE_FAMILIES
        .into_iter()
        .chain(["akzio.agent", "akzio.operator", "akzio.learning"])
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

fn rust_terminal_recipes() -> ResearchResult<(Vec<TaskRecipe>, TerminalRecipeSet)> {
    let evidence = rust_gate_recipe(EVIDENCE_GATE_RECIPE_ID, RuntimeTaskClass::Evidence)?;
    let decision = rust_gate_recipe(DECISION_GATE_RECIPE_ID, RuntimeTaskClass::DecisionGate)?;
    let execution = rust_gate_recipe(EXECUTION_GATE_RECIPE_ID, RuntimeTaskClass::ExecutionGate)?;
    let paper = rust_gate_recipe(PAPER_COMMIT_RECIPE_ID, RuntimeTaskClass::PaperCommit)?;
    let reconcile = rust_gate_recipe(RECONCILE_RECIPE_ID, RuntimeTaskClass::Reconcile)?;
    let evaluate = rust_gate_recipe(EVALUATE_RECIPE_ID, RuntimeTaskClass::Evaluate)?;
    let terminals = TerminalRecipeSet {
        evidence_gate: evidence.recipe_id.clone(),
        decision_gate: decision.recipe_id.clone(),
        execution_gate: execution.recipe_id.clone(),
        paper_commit: paper.recipe_id.clone(),
        reconcile: reconcile.recipe_id.clone(),
        evaluate: evaluate.recipe_id.clone(),
    };
    Ok((
        vec![evidence, decision, execution, paper, reconcile, evaluate],
        terminals,
    ))
}

fn rust_gate_recipe(recipe_id: &str, task_class: RuntimeTaskClass) -> ResearchResult<TaskRecipe> {
    let retry = if task_class == RuntimeTaskClass::Evidence {
        RetryPolicy {
            max_attempts: 5,
            initial_backoff_ms: 1_000,
            retry_transport: true,
            retry_rate_limited: true,
            retry_invalid_output: false,
        }
    } else {
        RetryPolicy::none()
    };
    Ok(TaskRecipe {
        recipe_id: TaskRecipeId::new(recipe_id)?,
        purpose: ContractPurpose::new(recipe_id)?,
        contract_hash: None,
        task_class,
        allowed_evidence_sources: BTreeSet::new(),
        max_children: 0,
        max_depth: 0,
        priority_ceiling: 100,
        budget: TaskBudget {
            max_input_tokens: 1,
            max_output_tokens: 1,
            max_wall_time_secs: 30,
            max_tool_calls: 0,
        },
        retry,
        on_failure: FailureDisposition::FailRun,
    })
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentToolCall {
    pub call_id: String,
    pub name: String,
    pub arguments: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentModelRequest {
    pub contract_hash: akzio_domain::ContentHash,
    pub purpose: String,
    pub prompt: String,
    pub objective: String,
    pub manifest_artifact_id: ArtifactId,
    pub context: Vec<Value>,
    pub prior_tool_results: Vec<Value>,
    pub output_schema: Value,
    pub max_output_tokens: u32,
    pub tools: Vec<AgentToolDefinition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub strict: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentModelTurn {
    pub output: Option<Value>,
    pub tool_calls: Vec<AgentToolCall>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_debug: Option<ModelCallTrace>,
}

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

    fn turn<'a>(
        &'a self,
        request: AgentModelRequest,
    ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>>;
}

#[derive(Debug, Clone)]
pub struct ModelClientAdapter {
    client: ModelClient,
    debug: bool,
}

impl ModelClientAdapter {
    pub fn new(client: ModelClient) -> Self {
        Self::with_debug(client, false)
    }

    pub fn with_debug(client: ModelClient, debug: bool) -> Self {
        Self { client, debug }
    }
}

impl AgentModel for ModelClientAdapter {
    fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        self.client.capability_snapshot()
    }

    fn turn<'a>(
        &'a self,
        request: AgentModelRequest,
    ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
        Box::pin(async move {
            let request = ModelRequest {
                instructions: request.prompt,
                input: serde_json::to_string(&json!({
                    "objective": request.objective,
                    "context_manifest": request.manifest_artifact_id,
                    "context": request.context,
                    "prior_tool_results": request.prior_tool_results,
                }))?,
                schema_name: Some(request.purpose),
                schema: Some(request.output_schema),
                max_output_tokens: request.max_output_tokens,
                tools: request
                    .tools
                    .into_iter()
                    .map(|tool| ModelToolDefinition {
                        name: tool.name,
                        description: tool.description,
                        input_schema: tool.input_schema,
                        strict: tool.strict,
                    })
                    .collect(),
            };
            let debug_request = self.debug.then(|| self.client.request_body(&request));
            let response = self.client.respond(request).await.map_err(|error| {
                let trace = debug_request.map(|request| ModelCallTrace {
                    request,
                    result: model_error_result(&error),
                });
                model_client_error(error, trace)
            })?;
            let model_debug = self.debug.then(|| ModelCallTrace {
                request: response.request_body.clone(),
                result: response.raw.clone(),
            });
            let output = (!response.output_text.trim().is_empty())
                .then(|| serde_json::from_str(&response.output_text))
                .transpose()
                .map_err(|error| {
                    model_output_error(format!("model output JSON: {error}"), model_debug.clone())
                })?;
            let tool_calls = response
                .tool_calls
                .into_iter()
                .map(|call| AgentToolCall {
                    call_id: call.call_id,
                    name: call.name,
                    arguments: call.arguments,
                })
                .collect();
            Ok(AgentModelTurn {
                output,
                tool_calls,
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
}

impl AgentRuntime {
    pub fn new(store: V2Store, catalogue: ContractCatalogue, grant_ttl: Duration) -> Self {
        Self {
            context: ContextBroker::new(store.clone()),
            store,
            catalogue,
            grant_ttl,
        }
    }

    pub fn catalogue(&self) -> &ContractCatalogue {
        &self.catalogue
    }

    pub async fn run(
        &self,
        permit: &TaskWritePermit,
        node: &WorkflowNode,
        candidates: impl IntoIterator<Item = ArtifactRef>,
        model: &dyn AgentModel,
        now: DateTime<Utc>,
    ) -> ResearchResult<Artifact> {
        self.store.validate_task_permit(permit)?;
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
            let proof = self
                .store
                .current_succeeded_attempt(&permit.run_id, parent_task_id)?;
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
        let governance = String::from_utf8(
            self.store
                .read_blob(&installed.contract.prompt.governance)?,
        )
        .map_err(|_| ResearchError::InvalidOutput("governance prompt is not UTF-8".to_owned()))?;
        let role = String::from_utf8(self.store.read_blob(&installed.contract.prompt.role)?)
            .map_err(|_| ResearchError::InvalidOutput("role prompt is not UTF-8".to_owned()))?;
        let prompt = format!("{governance}\n\n{role}");
        let output_schema: Value =
            serde_json::from_slice(&self.store.read_blob(&installed.contract.output.schema)?)?;
        let run_purpose = self.store.run_purpose(&permit.run_id)?;
        let tools = if !should_advertise_read_tools(
            run_purpose,
            context.len(),
            installed.contract.budget.max_tool_calls,
        ) {
            Vec::new()
        } else {
            model_tool_definitions(&self.store, &installed.contract)?
        };
        let mut tool_results = Vec::new();
        let mut trace_refs = Vec::new();
        let mut tool_calls = 0_u16;
        let mut model_turn = 0_u16;
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
                prompt: prompt.clone(),
                objective: node.objective.clone(),
                manifest_artifact_id: manifest.artifact.artifact_id.clone(),
                context: context.clone(),
                prior_tool_results: tool_results.clone(),
                output_schema: output_schema.clone(),
                max_output_tokens: installed.contract.budget.max_output_tokens,
                tools: tools.clone(),
            };
            let input_tokens = estimate_tokens(&request)?;
            if input_tokens > installed.contract.budget.max_input_tokens {
                return Err(ResearchError::InputBudgetExceeded {
                    actual: input_tokens,
                    maximum: installed.contract.budget.max_input_tokens,
                });
            }
            let tool_set_hash = tool_set_hash(&request.tools)?;
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
                self.store.validate_task_permit(permit)?;
                self.store.append_task_event(
                    permit,
                    LifecycleEventType::AgentTurnStarted,
                    logical_now(now, started.elapsed()),
                )?;
                match model.turn(request.clone()).await {
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
            if !turn.tool_calls.is_empty() {
                let next = tool_calls.saturating_add(turn.tool_calls.len() as u16);
                if next > installed.contract.budget.max_tool_calls {
                    return Err(ResearchError::ToolBudgetExceeded);
                }
                for call in turn.tool_calls {
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
                    tool_results.push(tool_result.value);
                }
                tool_calls = next;
                model_turn = model_turn.saturating_add(1);
                continue;
            }
            let raw_output = turn.output.ok_or(ResearchError::MissingFinalOutput)?;
            let output_tokens = estimate_tokens(&raw_output)?;
            if output_tokens > installed.contract.budget.max_output_tokens {
                return Err(ResearchError::OutputBudgetExceeded {
                    actual: output_tokens,
                    maximum: installed.contract.budget.max_output_tokens,
                });
            }
            let (output, deliberation_note) = self.extract_deliberation(
                permit,
                &installed.contract,
                &manifest,
                raw_output,
                turn_now,
            )?;
            validate_output_schema(&self.store, &installed.contract, &output)?;
            let research_sources = research_output_source_refs(
                installed.contract.output.artifact_kind,
                &output,
                &manifest,
            )?;
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
        let envelope: AgentOutputEnvelope = serde_json::from_value(output).map_err(|error| {
            ResearchError::InvalidOutput(format!("deliberation envelope: {error}"))
        })?;
        envelope
            .deliberation
            .validate()
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
                let artifact = self.context.read(
                    permit,
                    contract,
                    &manifest.grant,
                    &selection.artifact.artifact_id,
                    now,
                )?;
                let bytes = self.store.read_blob(&artifact.blob)?;
                let value = serde_json::from_slice(&bytes).unwrap_or_else(|_| {
                    Value::String(String::from_utf8_lossy(&bytes).into_owned())
                });
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
        && !matches!(purpose, RunPurpose::Debug | RunPurpose::PaperDryRun)
}

fn estimate_tokens<T: Serialize>(value: &T) -> ResearchResult<u32> {
    let bytes = serde_json::to_vec(value)?.len() as u64;
    Ok(u32::try_from(bytes.div_ceil(4).max(1)).unwrap_or(u32::MAX))
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

fn model_error_result(error: &ModelError) -> Value {
    match error {
        ModelError::Http { status, body } => json!({
            "status": status.as_u16(),
            "body": serde_json::from_str::<Value>(body)
                .unwrap_or_else(|_| Value::String(body.clone())),
        }),
        ModelError::Transport(_) => json!({"error": "transport"}),
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
        ModelError::Transport(_) => ("transport", "transport".to_owned()),
        ModelError::Http { status, .. } if status.as_u16() == 429 => {
            ("rate_limited", "HTTP 429".to_owned())
        }
        ModelError::Http { status, .. } => ("transport", format!("HTTP {}", status.as_u16())),
        ModelError::EmptyBaseUrl => ("configuration", "invalid base URL".to_owned()),
        ModelError::EmptyApiKey => ("configuration", "missing API key".to_owned()),
        ModelError::EmptyModel => ("configuration", "missing model name".to_owned()),
        ModelError::EmptyReasoningEffort => {
            ("configuration", "missing reasoning effort".to_owned())
        }
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

fn model_output_error(message: String, trace: Option<ModelCallTrace>) -> ResearchError {
    match trace {
        Some(trace) => ResearchError::ModelDebug {
            error_class: "invalid_output",
            message,
            trace,
        },
        None => ResearchError::InvalidOutput(message),
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
#[path = "tests.rs"]
mod tests;
