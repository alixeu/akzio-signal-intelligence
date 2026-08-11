//! Contract-driven Agent runtime for the v2 system.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration as StdDuration, Instant},
};

use akzio_context::v2::{ContextBroker, ContextError, ContextManifest};
use akzio_domain::{
    AgentContract, Artifact, ArtifactId, ArtifactKind, ArtifactLifecycle, ArtifactOrigin,
    ArtifactProvenance, ArtifactRef, ContextPolicy, ContractId, ContractPurpose, DomainError,
    FailureDisposition, OutputContract, PromptBundle, ReadGrant, ResearchClaim, ResearchCritique,
    ResearchResolution, RetryPolicy, RuntimeTaskClass, TaskBudget, TaskRecipe, TaskRecipeId,
    TaskWritePermit, TerminationPolicy, ToolGrant, ToolKind, ToolSpec, WorkflowNode,
    V2_SCHEMA_VERSION,
};
use akzio_model::{ModelCallTrace, ModelClient, ModelError, ModelRequest, ModelToolDefinition};
use akzio_runtime::v2::{RecipeCatalogue, RuntimeError, TerminalRecipeSet};
use akzio_store::v2::{StoreError, StoredContract, V2Store};
use chrono::{DateTime, Duration, Utc};
use futures::future::BoxFuture;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

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

pub type ResearchResult<T> = Result<T, ResearchError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstalledContract {
    pub contract: AgentContract,
    pub artifact: Artifact,
}

/// The bounded initial topology is expressed as installed Contracts, never as
/// a role registry. Daemon bootstrap consumes this pair atomically at the API
/// boundary: contracts drive model turns; recipes drive Rust DAG lowering.
#[derive(Debug, Clone)]
pub struct ActiveResearchCatalogue {
    pub contracts: ContractCatalogue,
    pub recipes: RecipeCatalogue,
}

impl ActiveResearchCatalogue {
    /// Restore the Store-owned active heads, bootstrapping only a fresh Store
    /// with the immutable Rust-defined defaults. Candidates deliberately have
    /// no execution path until a canonical Paper-backed transition promotes
    /// their persisted head.
    pub fn install(store: &V2Store, now: DateTime<Utc>) -> ResearchResult<Self> {
        let contracts = ContractCatalogue::load_or_bootstrap_active(
            store,
            canonical_active_contracts(store)?,
            now,
        )?;
        let recipes = contracts.active_recipe_catalogue(store)?;
        Ok(Self { contracts, recipes })
    }

    /// Persist a capability-bounded candidate beneath an installed Active
    /// Contract. The Store, rather than this process-local catalogue, owns its
    /// immutable installation and later policy-driven activation.
    pub fn install_candidate(
        &self,
        store: &V2Store,
        active_contract_hash: &akzio_domain::ContentHash,
        candidate: &AgentContract,
        now: DateTime<Utc>,
    ) -> ResearchResult<InstalledContract> {
        self.contracts
            .install_candidate(store, active_contract_hash, candidate, now)
    }
}

pub const ACTIVE_RESEARCH_MAX_NODES: usize = 32;

const ACTIVE_CONTRACT_VERSION: u32 = 1;
const ACTIVE_PROMPT_BUNDLE_VERSION: u32 = 1;
const SHARED_GOVERNANCE_PROMPT: &str = "Follow the installed Akzio Contract exactly. Rust owns state, evidence access, budgets, workflow gates, and Paper-only execution. Use only ContextManifest-granted artifacts and the declared tools. Never access arbitrary files, network resources, credentials, databases, or execution controls. Return only the requested strict JSON output.";
const PLANNER_RECIPE_ID: &str = "research.planner";
const PLANNER_CHILD_RECIPE_IDS: [&str; 2] = ["research.analyst", "research.synthesizer"];
const GOVERNED_EVIDENCE_SOURCE_FAMILIES: [&str; 4] = ["alpaca", "sec_edgar", "fred", "news_web"];
const PLANNER_MAX_DRAFT_TASKS: u16 = 7;
const EVIDENCE_GATE_RECIPE_ID: &str = "gate.evidence";
const DECISION_GATE_RECIPE_ID: &str = "gate.decision";
const EXECUTION_GATE_RECIPE_ID: &str = "gate.execution";
const PAPER_COMMIT_RECIPE_ID: &str = "gate.paper";
const RECONCILE_RECIPE_ID: &str = "gate.reconcile";
const EVALUATE_RECIPE_ID: &str = "gate.evaluate";

#[derive(Debug, Clone, Copy)]
struct ActiveRecipePolicy {
    purpose: &'static str,
    output_kind: ArtifactKind,
    priority_ceiling: u8,
}

const ACTIVE_RECIPE_POLICIES: [ActiveRecipePolicy; 4] = [
    ActiveRecipePolicy {
        purpose: PLANNER_RECIPE_ID,
        output_kind: ArtifactKind::WorkflowProposalDraft,
        priority_ceiling: 100,
    },
    ActiveRecipePolicy {
        purpose: "research.analyst",
        output_kind: ArtifactKind::Claim,
        priority_ceiling: 90,
    },
    ActiveRecipePolicy {
        purpose: "research.critic",
        output_kind: ArtifactKind::Critique,
        priority_ceiling: 80,
    },
    ActiveRecipePolicy {
        purpose: "research.synthesizer",
        output_kind: ArtifactKind::DecisionProposal,
        priority_ceiling: 100,
    },
];

#[derive(Debug, Clone, Default)]
pub struct ContractCatalogue {
    by_hash: BTreeMap<akzio_domain::ContentHash, InstalledContract>,
    by_identity: BTreeMap<(akzio_domain::ContractId, u32), akzio_domain::ContentHash>,
}

impl ContractCatalogue {
    fn load_or_bootstrap_active(
        store: &V2Store,
        contracts: impl IntoIterator<Item = AgentContract>,
        now: DateTime<Utc>,
    ) -> ResearchResult<Self> {
        let contracts = contracts.into_iter().collect::<Vec<_>>();
        validate_unique_contracts(&contracts)?;
        let mut by_hash = BTreeMap::new();
        let mut by_identity = BTreeMap::new();
        for contract in contracts {
            let stored = match store.active_contract(&contract.purpose)? {
                Some(stored) => stored,
                None => store.install_active_contract(&contract, now)?,
            };
            let contract = stored.contract;
            contract.validate()?;
            model_tool_definitions(store, &contract)?;
            if by_hash.contains_key(&contract.contract_hash) {
                return Err(ResearchError::DuplicateContract(
                    contract.contract_hash.clone(),
                ));
            }
            let identity = (contract.contract_id.clone(), contract.version);
            if by_identity.contains_key(&identity) {
                return Err(ResearchError::DuplicateContractVersion {
                    contract_id: contract.contract_id.clone(),
                    version: contract.version,
                });
            }
            let contract_hash = contract.contract_hash.clone();
            by_hash.insert(
                contract_hash.clone(),
                InstalledContract {
                    contract,
                    artifact: stored.artifact,
                },
            );
            by_identity.insert(identity, contract_hash);
        }
        Ok(Self {
            by_hash,
            by_identity,
        })
    }

    #[cfg(test)]
    fn install(
        store: &V2Store,
        contracts: impl IntoIterator<Item = AgentContract>,
        now: DateTime<Utc>,
    ) -> ResearchResult<Self> {
        Self::load_or_bootstrap_active(store, contracts, now)
    }

    pub fn get(&self, hash: &akzio_domain::ContentHash) -> ResearchResult<&InstalledContract> {
        self.by_hash
            .get(hash)
            .ok_or_else(|| ResearchError::UnknownContract(hash.clone()))
    }

    pub fn contracts(&self) -> impl Iterator<Item = &InstalledContract> {
        self.by_hash.values()
    }

    pub fn contract_hash_for(
        &self,
        contract_id: &akzio_domain::ContractId,
        version: u32,
    ) -> Option<&akzio_domain::ContentHash> {
        self.by_identity.get(&(contract_id.clone(), version))
    }

    /// Lower only Store-owned Active Contract heads into agent recipes.
    /// The recipe limits come from each contract's termination/budget/retry
    /// policy; Rust owns the fixed priority ceilings and terminal gate recipes.
    ///
    /// This method rejects unknown purposes and candidates that are not the
    /// current durable head rather than silently granting a new recipe.
    pub fn active_recipe_catalogue(&self, store: &V2Store) -> ResearchResult<RecipeCatalogue> {
        let mut installed_purposes = BTreeSet::new();
        let mut recipes = Vec::with_capacity(ACTIVE_RECIPE_POLICIES.len() + 6);

        for installed in self.contracts() {
            let purpose = installed.contract.purpose.as_str();
            let policy = active_recipe_policy(purpose).ok_or_else(|| {
                ResearchError::UnexpectedActiveContractPurpose(purpose.to_owned())
            })?;
            if !installed_purposes.insert(purpose.to_owned()) {
                return Err(ResearchError::DuplicateActiveContractPurpose(
                    purpose.to_owned(),
                ));
            }
            if installed.contract.output.artifact_kind != policy.output_kind {
                return Err(ResearchError::ActiveContractOutputMismatch {
                    purpose: purpose.to_owned(),
                    expected: policy.output_kind,
                    actual: installed.contract.output.artifact_kind,
                });
            }
            let active = store
                .active_contract(&installed.contract.purpose)?
                .ok_or_else(|| ResearchError::NonCanonicalActiveContract(purpose.to_owned()))?;
            if active.contract.contract_hash != installed.contract.contract_hash
                || active.artifact != installed.artifact
            {
                return Err(ResearchError::NonCanonicalActiveContract(
                    purpose.to_owned(),
                ));
            }

            recipes.push(TaskRecipe {
                recipe_id: TaskRecipeId::new(purpose)?,
                purpose: installed.contract.purpose.clone(),
                contract_hash: Some(installed.contract.contract_hash.clone()),
                task_class: RuntimeTaskClass::Agent,
                allowed_evidence_sources: recipe_evidence_sources(&installed.contract),
                max_children: installed.contract.termination.max_child_tasks,
                max_depth: installed.contract.termination.max_depth,
                priority_ceiling: policy.priority_ceiling,
                budget: installed.contract.budget.clone(),
                retry: installed.contract.retry.clone(),
                on_failure: installed.contract.on_failure,
            });
        }

        for policy in ACTIVE_RECIPE_POLICIES {
            if !installed_purposes.contains(policy.purpose) {
                return Err(ResearchError::MissingActiveContract(policy.purpose));
            }
        }

        let (terminal_recipes, terminals) = rust_terminal_recipes()?;
        recipes.extend(terminal_recipes);
        Ok(RecipeCatalogue::new(
            recipes,
            TaskRecipeId::new(PLANNER_RECIPE_ID)?,
            terminals,
            ACTIVE_RESEARCH_MAX_NODES,
        )?)
    }

    /// Candidate contracts are data for later shadow evaluation. This gate
    /// proves they cannot request a wider source or tool surface than the
    /// installed active contract that sponsors them.
    pub fn validate_candidate(
        &self,
        active_hash: &akzio_domain::ContentHash,
        candidate: &AgentContract,
    ) -> ResearchResult<()> {
        candidate.validate()?;
        let active = self.get(active_hash)?;
        if active.contract.permits_candidate(candidate) {
            Ok(())
        } else {
            Err(ResearchError::CandidateCapabilityExpansion {
                active: active_hash.clone(),
                candidate: candidate.contract_hash.clone(),
            })
        }
    }

    pub fn install_candidate(
        &self,
        store: &V2Store,
        active_contract_hash: &akzio_domain::ContentHash,
        candidate: &AgentContract,
        now: DateTime<Utc>,
    ) -> ResearchResult<InstalledContract> {
        self.validate_candidate(active_contract_hash, candidate)?;
        model_tool_definitions(store, candidate)?;
        let stored = store.install_candidate_contract(active_contract_hash, candidate, now)?;
        Ok(installed_contract(stored))
    }
}

fn installed_contract(stored: StoredContract) -> InstalledContract {
    InstalledContract {
        contract: stored.contract,
        artifact: stored.artifact,
    }
}

fn validate_unique_contracts(contracts: &[AgentContract]) -> ResearchResult<()> {
    let mut hashes = BTreeSet::new();
    let mut identities = BTreeSet::new();
    for contract in contracts {
        contract.validate()?;
        if !hashes.insert(contract.contract_hash.clone()) {
            return Err(ResearchError::DuplicateContract(
                contract.contract_hash.clone(),
            ));
        }
        let identity = (contract.contract_id.clone(), contract.version);
        if !identities.insert(identity) {
            return Err(ResearchError::DuplicateContractVersion {
                contract_id: contract.contract_id.clone(),
                version: contract.version,
            });
        }
    }
    Ok(())
}

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
            prompt: "You are Akzio's bounded research planner. Return only JSON matching the WorkflowProposalDraft schema. You may name only research.analyst and research.synthesizer recipes, and express evidence needs inline. Rust may insert one conditional critic only for the structured-critique candidate topology. Do not construct ArtifactRef values, request sources or tools beyond the contract, submit a decision, or submit an order.",
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
                    ArtifactKind::Experience,
                    ArtifactKind::CandidatePolicy,
                    ArtifactKind::NormalizedEvidence,
                    ArtifactKind::SemanticDetail,
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
    ]
    .into_iter()
    .map(|definition| canonical_active_contract(store, definition))
    .collect()
}

fn canonical_active_contract(
    store: &V2Store,
    definition: CanonicalContractDefinition,
) -> ResearchResult<AgentContract> {
    let prompt = PromptBundle {
        version: ACTIVE_PROMPT_BUNDLE_VERSION,
        governance: store.put_bytes(SHARED_GOVERNANCE_PROMPT.as_bytes(), "text/plain")?,
        role: store.put_bytes(definition.prompt.as_bytes(), "text/plain")?,
    };
    let schema = store.put_json(&definition.output_schema)?;
    Ok(AgentContract::new(
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
    )?)
}

fn governed_context_sources() -> BTreeSet<String> {
    GOVERNED_EVIDENCE_SOURCE_FAMILIES
        .into_iter()
        .chain(["akzio.agent"])
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

fn artifact_id_tool_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {"artifact_id": {"type": "string", "minLength": 1}},
        "required": ["artifact_id"],
        "additionalProperties": false,
    })
}

fn evidence_read_tool_specs(store: &V2Store) -> ResearchResult<Vec<ToolSpec>> {
    Ok(vec![ToolSpec {
        name: "read_artifact".to_owned(),
        description: "Read one artifact explicitly granted by ContextManifest.".to_owned(),
        kind: ToolKind::ReadEvidence,
        input_schema: store.put_json(&artifact_id_tool_input_schema())?,
        strict: true,
    }])
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

fn planner_draft_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "schema_version": {
                "type": "integer",
                "enum": [V2_SCHEMA_VERSION]
            },
            "topology_id": {
                "type": "string",
                "minLength": 1,
                "maxLength": 128
            },
            "tasks": {
                "type": "object",
                "properties": {},
                "required": [],
                "minProperties": 1,
                "maxProperties": PLANNER_MAX_DRAFT_TASKS,
                "additionalProperties": planner_draft_task_schema()
            },
            "stop_reason": {
                "type": "string",
                "minLength": 1,
                "maxLength": 1024
            }
        },
        "required": ["schema_version", "topology_id", "tasks"],
        "additionalProperties": false
    })
}

fn planner_draft_task_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "recipe_id": {
                "type": "string",
                "enum": PLANNER_CHILD_RECIPE_IDS
            },
            "objective": {
                "type": "string",
                "minLength": 1,
                "maxLength": 2048
            },
            "depends_on": {
                "type": "array",
                "items": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 128
                },
                "maxItems": PLANNER_MAX_DRAFT_TASKS
            },
            "priority": {
                "type": "integer",
                "minimum": 0,
                "maximum": 100
            },
        "evidence_needs": {
            "type": "array",
            "items": evidence_need_output_schema(),
            "maxItems": PLANNER_MAX_DRAFT_TASKS
        },
        "research_intents": {
            "type": "array",
            "items": research_intent_output_schema(),
            "maxItems": PLANNER_MAX_DRAFT_TASKS
        }
        },
        "required": [
            "recipe_id",
            "objective",
            "depends_on",
            "priority",
            "evidence_needs"
        ],
        "additionalProperties": false
    })
}

fn research_intent_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "schema_version": {"type": "integer", "enum": [V2_SCHEMA_VERSION]},
            "source_family": {"type": "string", "enum": GOVERNED_EVIDENCE_SOURCE_FAMILIES},
            "resource": {"type": "string", "minLength": 1, "maxLength": 2048},
            "query": {"type": "string", "minLength": 1, "maxLength": 2000},
            "assets": {
                "type": "array",
                "uniqueItems": true,
                "maxItems": 4,
                "items": {"type": "string", "enum": ["TQQQ", "QQQ", "SOXX", "SOXL"]}
            },
            "window_start": {"type": ["string", "null"]},
            "window_end": {"type": ["string", "null"]},
            "max_age_secs": {"type": "integer", "minimum": 1, "maximum": 604800},
            "max_results": {"type": "integer", "minimum": 1, "maximum": 32}
        },
        "required": [
            "schema_version", "source_family", "resource", "query", "assets",
            "window_start", "window_end", "max_age_secs", "max_results"
        ],
        "additionalProperties": false
    })
}

fn evidence_need_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "schema_version": {
                "type": "integer",
                "enum": [V2_SCHEMA_VERSION]
            },
            "source_family": {
                "type": "string",
                "enum": GOVERNED_EVIDENCE_SOURCE_FAMILIES
            },
            "resource": {
                "type": "string",
                "minLength": 1,
                "maxLength": 512
            },
            "max_age_secs": {
                "type": "integer",
                "minimum": 1
            }
        },
        "required": ["schema_version", "source_family", "resource", "max_age_secs"],
        "additionalProperties": false
    })
}

fn claim_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "schema_version": { "type": "integer", "enum": [V2_SCHEMA_VERSION] },
            "topic": { "type": "string", "minLength": 1, "maxLength": 128 },
            "statement": { "type": "string", "minLength": 1, "maxLength": 2048 },
            "horizon": { "type": "string", "enum": ["t1", "t3", "t5"] },
            "stance": { "type": "string", "enum": ["bullish", "bearish", "neutral"] },
            "materiality_ppm": { "type": "integer", "minimum": 0, "maximum": 1_000_000 },
            "confidence_ppm": { "type": "integer", "minimum": 0, "maximum": 1_000_000 },
            "grounds": {
                "type": "array",
                "minItems": 1,
                "maxItems": 8,
                "items": evidence_ground_schema()
            },
            "evidence_gaps": {
                "type": "array",
                "maxItems": 2,
                "items": evidence_gap_schema()
            }
        },
        "required": [
            "schema_version", "topic", "statement", "horizon", "stance", "materiality_ppm",
            "confidence_ppm", "grounds", "evidence_gaps"
        ],
        "additionalProperties": false
    })
}

fn critique_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "schema_version": { "type": "integer", "enum": [V2_SCHEMA_VERSION] },
            "target": artifact_ref_schema(&["claim"]),
            "topic": { "type": "string", "minLength": 1, "maxLength": 128 },
            "severity": { "type": "string", "enum": ["low", "medium", "high"] },
            "blocker": { "type": "boolean" },
            "rationale": { "type": "string", "minLength": 1, "maxLength": 2048 },
            "grounds": {
                "type": "array",
                "maxItems": 8,
                "items": evidence_ground_schema()
            },
            "evidence_gaps": {
                "type": "array",
                "maxItems": 2,
                "items": evidence_gap_schema()
            }
        },
        "required": [
            "schema_version", "target", "topic", "severity", "blocker", "rationale", "grounds",
            "evidence_gaps"
        ],
        "additionalProperties": false
    })
}

fn resolution_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "schema_version": { "type": "integer", "enum": [V2_SCHEMA_VERSION] },
            "claim": artifact_ref_schema(&["claim"]),
            "critique": artifact_ref_schema(&["critique"]),
            "disposition": { "type": "string", "enum": ["accepted", "rebutted", "unresolved"] },
            "rationale": { "type": "string", "minLength": 1, "maxLength": 2048 },
            "grounds": {
                "type": "array",
                "minItems": 1,
                "maxItems": 8,
                "items": evidence_ground_schema()
            },
            "remaining_gaps": {
                "type": "array",
                "maxItems": 2,
                "items": evidence_gap_schema()
            }
        },
        "required": [
            "schema_version", "claim", "critique", "disposition", "rationale", "grounds",
            "remaining_gaps"
        ],
        "additionalProperties": false
    })
}

fn evidence_ground_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "evidence": artifact_ref_schema(&["normalized_evidence", "semantic_detail"]),
            "support": { "type": "string", "minLength": 1, "maxLength": 2048 }
        },
        "required": ["evidence", "support"],
        "additionalProperties": false
    })
}

fn evidence_gap_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "topic": { "type": "string", "minLength": 1, "maxLength": 128 },
            "rationale": { "type": "string", "minLength": 1, "maxLength": 2048 }
        },
        "required": ["topic", "rationale"],
        "additionalProperties": false
    })
}

fn decision_proposal_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "summary": { "type": "string", "minLength": 1 },
            "confidence_ppm": { "type": "integer", "minimum": 0, "maximum": 1000000 },
            "forecasts": {
                "type": "array",
                "minItems": 12,
                "maxItems": 12,
                "items": {
                    "type": "object",
                    "properties": {
                        "asset": { "type": "string", "enum": ["TQQQ", "QQQ", "SOXX", "SOXL"] },
                        "horizon": { "type": "string", "enum": ["t1", "t3", "t5"] },
                        "positive_return_probability_ppm": {
                            "type": "integer",
                            "minimum": 0,
                            "maximum": 1000000
                        },
                        "expected_return_ppm": { "type": "integer" }
                    },
                    "required": [
                        "asset",
                        "horizon",
                        "positive_return_probability_ppm",
                        "expected_return_ppm"
                    ],
                    "additionalProperties": false
                }
            },
            "claims": { "type": "array", "items": artifact_ref_schema(&["claim"]) },
            "critiques": { "type": "array", "items": artifact_ref_schema(&["critique"]) },
            "evidence": {
                "type": "array",
                "items": artifact_ref_schema(&["normalized_evidence", "semantic_detail"])
            },
            "material_conflicts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "claim": artifact_ref_schema(&["claim"]),
                        "critique": artifact_ref_schema(&["critique"]),
                        "topic": { "type": "string", "minLength": 1 },
                        "rationale": { "type": "string", "minLength": 1 }
                    },
                    "required": ["claim", "critique", "topic", "rationale"],
                    "additionalProperties": false
                }
            },
            "hard_blockers": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": [
                        "unsupported_universe", "no_executable_order", "frozen",
                        "missing_evidence", "invalid_provenance", "material_conflict",
                        "stale_quote", "missing_quote", "stale_account", "missing_account",
                        "market_closed", "factor_limit", "pair_exposure_limit",
                        "turnover_limit", "plan_hash_mismatch", "duplicate_commitment",
                        "non_paper_endpoint", "non_canonical_run", "recovery_incomplete"
                    ]
                }
            },
            "soft_warnings": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": [
                        "low_confidence", "incomplete_evidence", "elevated_turnover",
                        "slow_model_response", "stale_noncritical_evidence"
                    ]
                }
            }
        },
        "required": [
            "summary", "confidence_ppm", "forecasts", "claims", "critiques",
            "evidence", "material_conflicts", "hard_blockers", "soft_warnings"
        ],
        "additionalProperties": false
    })
}

fn artifact_ref_schema(kinds: &[&str]) -> Value {
    json!({
        "type": "object",
        "properties": {
            "artifact_id": {
                "type": "string",
                "pattern": "^[0-9a-f]{64}$"
            },
            "kind": { "type": "string", "enum": kinds }
        },
        "required": ["artifact_id", "kind"],
        "additionalProperties": false
    })
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
        retry: RetryPolicy::none(),
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
        let manifest =
            self.context
                .assemble(permit, &installed.contract, candidates, now, self.grant_ttl)?;
        if !manifest.grant.matches_permit(permit) {
            return Err(ResearchError::GrantPermitMismatch);
        }
        let context = self.context_values(permit, &manifest, now)?;
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
        let tools = model_tool_definitions(&self.store, &installed.contract)?;
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
            let request_hash = model_request_hash(&request)?;
            let mut turn_attempt = 1_u8;
            let turn = loop {
                match model.turn(request.clone()).await {
                    Ok(turn) => break turn,
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
            let output = turn.output.ok_or(ResearchError::MissingFinalOutput)?;
            let output_tokens = estimate_tokens(&output)?;
            if output_tokens > installed.contract.budget.max_output_tokens {
                return Err(ResearchError::OutputBudgetExceeded {
                    actual: output_tokens,
                    maximum: installed.contract.budget.max_output_tokens,
                });
            }
            validate_output_schema(&self.store, &installed.contract, &output)?;
            let research_sources = research_output_source_refs(
                installed.contract.output.artifact_kind,
                &output,
                &manifest,
            )?;
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
                Some(task_origin(permit)),
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

    fn context_values(
        &self,
        permit: &TaskWritePermit,
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
                let artifact =
                    self.context
                        .read(&manifest.grant, &selection.artifact.artifact_id, now)?;
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
            Some(task_origin(record.permit)),
            vec![ArtifactRef {
                artifact_id: record.manifest.artifact.artifact_id.clone(),
                kind: ArtifactKind::ContextManifest,
            }],
            record.now,
        )?;
        self.store.write_task_artifact(
            record.permit,
            &artifact,
            "agent.turn_completed",
            record.now,
        )?;
        Ok(artifact)
    }

    fn record_failed_turn(
        &self,
        record: &TurnRecord<'_>,
        request: &AgentModelRequest,
        error_class: &str,
        model_debug: Option<&ModelCallTrace>,
        will_retry: bool,
    ) -> ResearchResult<Artifact> {
        let request_hash = model_request_hash(request)?;
        let mut trace = json!({
            "turn": record.turn,
            "attempt": record.attempt,
            "contract_hash": record.contract.contract_hash,
            "context_manifest": record.manifest.artifact.artifact_id,
            "request_hash": request_hash,
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
            Some(task_origin(record.permit)),
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
                "agent.turn_retryable_failed"
            } else {
                "agent.turn_failed"
            },
            record.now,
        )?;
        Ok(artifact)
    }

    fn execute_tool(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        grant: &ReadGrant,
        call: &AgentToolCall,
        request_hash: &akzio_domain::ContentHash,
        now: DateTime<Utc>,
    ) -> ResearchResult<ToolResult> {
        let call_artifact = Artifact::new(
            ArtifactKind::ToolCall,
            self.store.put_json(&json!({
                "request_hash": request_hash,
                "call": call,
            }))?,
            "agent.tool",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.tool".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: Some(contract.contract_hash.clone()),
            },
            Some(task_origin(permit)),
            vec![ArtifactRef {
                artifact_id: grant.manifest_artifact_id.clone(),
                kind: ArtifactKind::ContextManifest,
            }],
            now,
        )?;
        self.store
            .write_task_artifact(permit, &call_artifact, "tool.called", now)?;

        match self.execute_tool_inner(permit, contract, grant, call, now) {
            Ok((artifact, value)) => {
                let result_artifact = Artifact::new(
                    ArtifactKind::ToolResult,
                    self.store.put_json(&json!({
                        "request_hash": request_hash,
                        "call_id": call.call_id,
                        "name": call.name,
                        "ok": true,
                        "value": value,
                    }))?,
                    "agent.tool",
                    ArtifactLifecycle::RunScoped,
                    ArtifactProvenance {
                        source_family: "akzio.tool".to_owned(),
                        observed_at: None,
                        retrieved_at: now,
                        source_uri: None,
                        confidence_ppm: 1_000_000,
                        producer_contract_hash: Some(contract.contract_hash.clone()),
                    },
                    Some(task_origin(permit)),
                    vec![
                        ArtifactRef {
                            artifact_id: call_artifact.artifact_id.clone(),
                            kind: ArtifactKind::ToolCall,
                        },
                        ArtifactRef {
                            artifact_id: artifact.artifact_id.clone(),
                            kind: artifact.kind,
                        },
                    ],
                    now,
                )?;
                self.store
                    .write_task_artifact(permit, &result_artifact, "tool.completed", now)?;
                Ok(ToolResult {
                    value: json!({
                        "call_id": call.call_id,
                        "artifact_id": artifact.artifact_id,
                        "kind": artifact.kind,
                        "ok": true,
                        "value": value,
                    }),
                    artifact: result_artifact,
                })
            }
            Err(error) => {
                let result_artifact = Artifact::new(
                    ArtifactKind::ToolResult,
                    self.store.put_json(&json!({
                        "request_hash": request_hash,
                        "call_id": call.call_id,
                        "name": call.name,
                        "ok": false,
                        "error": {
                            "code": tool_error_code(&error),
                            "message": error.to_string(),
                        },
                    }))?,
                    "agent.tool",
                    ArtifactLifecycle::RunScoped,
                    ArtifactProvenance {
                        source_family: "akzio.tool".to_owned(),
                        observed_at: None,
                        retrieved_at: now,
                        source_uri: None,
                        confidence_ppm: 1_000_000,
                        producer_contract_hash: Some(contract.contract_hash.clone()),
                    },
                    Some(task_origin(permit)),
                    vec![ArtifactRef {
                        artifact_id: call_artifact.artifact_id.clone(),
                        kind: ArtifactKind::ToolCall,
                    }],
                    now,
                )?;
                self.store
                    .write_task_artifact(permit, &result_artifact, "tool.failed", now)?;
                Err(error)
            }
        }
    }

    fn execute_tool_inner(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        grant: &ReadGrant,
        call: &AgentToolCall,
        now: DateTime<Utc>,
    ) -> ResearchResult<(Artifact, Value)> {
        if !grant.matches_permit(permit) {
            return Err(ResearchError::GrantPermitMismatch);
        }
        let tool = contract
            .tool_specs
            .iter()
            .find(|spec| spec.name == call.name)
            .ok_or_else(|| ResearchError::ToolNotGranted(call.name.clone()))?;
        let artifact_id = strict_artifact_id_argument(&call.arguments, &call.name)?;
        if !contract
            .tool_grants
            .iter()
            .any(|grant| grant.kind == tool.kind)
        {
            return Err(ResearchError::ToolNotGranted(call.name.clone()));
        }
        let raw = tool.kind == akzio_domain::ToolKind::ReadRawEvidence;
        let artifact = if raw {
            self.context.read_raw(grant, &artifact_id, now)?
        } else {
            self.context.read(grant, &artifact_id, now)?
        };
        if !contract
            .tool_grants
            .iter()
            .filter(|tool_grant| tool_grant.kind == tool.kind)
            .any(|tool_grant| {
                tool_grant.allowed_sources.is_empty()
                    || tool_grant
                        .allowed_sources
                        .iter()
                        .any(|source| source == &artifact.provenance.source_family)
            })
        {
            return Err(ResearchError::ToolSourceNotGranted {
                tool: call.name.clone(),
                source_family: artifact.provenance.source_family.clone(),
            });
        }
        let bytes = self.store.read_blob(&artifact.blob)?;
        let value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
        Ok((artifact, value))
    }
}

fn tool_error_code(error: &ResearchError) -> &'static str {
    match error {
        ResearchError::GrantPermitMismatch => "grant_permit_mismatch",
        ResearchError::ToolNotGranted(_) => "tool_not_granted",
        ResearchError::ToolSourceNotGranted { .. } => "tool_source_not_granted",
        ResearchError::InvalidOutput(_) => "invalid_tool_arguments",
        ResearchError::Context(_) => "context_read_rejected",
        _ => "tool_execution_failed",
    }
}

fn strict_artifact_id_argument(arguments: &Value, tool_name: &str) -> ResearchResult<ArtifactId> {
    let object = arguments.as_object().ok_or_else(|| {
        ResearchError::InvalidOutput(format!(
            "tool {tool_name} arguments do not match its strict schema"
        ))
    })?;
    if object.len() != 1 {
        return Err(ResearchError::InvalidOutput(format!(
            "tool {tool_name} arguments do not match its strict schema"
        )));
    }
    let artifact_id = object
        .get("artifact_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ResearchError::InvalidOutput(format!("tool {tool_name} omitted artifact_id"))
        })?;
    Ok(ArtifactId(akzio_domain::ContentHash::new(artifact_id)?))
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

fn model_tool_definitions(
    store: &V2Store,
    contract: &AgentContract,
) -> ResearchResult<Vec<AgentToolDefinition>> {
    contract
        .tool_specs
        .iter()
        .map(|spec| {
            let input_schema: Value =
                serde_json::from_slice(&store.read_blob(&spec.input_schema)?)?;
            if input_schema != artifact_id_tool_input_schema() {
                return Err(ResearchError::InvalidToolSpec(format!(
                    "{} must use the strict artifact_id input schema",
                    spec.name
                )));
            }
            Ok(AgentToolDefinition {
                name: spec.name.clone(),
                description: spec.description.clone(),
                input_schema,
                strict: spec.strict,
            })
        })
        .collect()
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
    if error_class == "rate_limited" {
        ResearchError::RateLimited(message)
    } else {
        ResearchError::Model(message)
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
        ResearchError::Model(_) => retry.retry_transport,
        ResearchError::RateLimited(_) => retry.retry_rate_limited,
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

fn task_origin(permit: &TaskWritePermit) -> ArtifactOrigin {
    ArtifactOrigin {
        run_id: Some(permit.run_id.clone()),
        task_id: Some(permit.task_id.clone()),
        attempt_id: Some(permit.attempt_id.clone()),
        contract_hash: permit.contract_hash.clone(),
    }
}

/// A minimal, deterministic subset of JSON Schema sufficient for the contracts
/// owned by this workspace. Contract authors must not rely on an unvalidated schema
/// keyword; unsupported shapes are rejected rather than prompt-softened.
fn validate_output_schema(
    store: &V2Store,
    contract: &AgentContract,
    output: &Value,
) -> ResearchResult<()> {
    let schema: Value = serde_json::from_slice(&store.read_blob(&contract.output.schema)?)?;
    validate_schema_value(output, &schema, "$").map_err(ResearchError::InvalidOutput)?;
    if schema.get("type").and_then(Value::as_str) != Some("object") || !output.is_object() {
        return Err(ResearchError::InvalidOutput(
            "schema and output must both be JSON objects".to_owned(),
        ));
    }
    let required = schema
        .get("required")
        .and_then(Value::as_array)
        .ok_or_else(|| ResearchError::InvalidOutput("schema.required missing".to_owned()))?;
    for field in required {
        let Some(field) = field.as_str() else {
            return Err(ResearchError::InvalidOutput(
                "schema.required must contain strings".to_owned(),
            ));
        };
        if output.get(field).is_none() {
            return Err(ResearchError::InvalidOutput(format!(
                "required field {field} is missing"
            )));
        }
    }
    Ok(())
}

fn validate_schema_value(value: &Value, schema: &Value, path: &str) -> Result<(), String> {
    let definition = schema
        .as_object()
        .ok_or_else(|| format!("{path} schema must be an object"))?;
    for key in definition.keys() {
        if !matches!(
            key.as_str(),
            "type"
                | "enum"
                | "properties"
                | "required"
                | "additionalProperties"
                | "items"
                | "minimum"
                | "maximum"
                | "pattern"
                | "minLength"
                | "maxLength"
                | "minItems"
                | "maxItems"
                | "minProperties"
                | "maxProperties"
        ) {
            return Err(format!("{path} schema keyword {key} is unsupported"));
        }
    }
    let kind = definition
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{path} schema.type must be a string"))?;
    let valid_kind = match kind {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "boolean" => value.is_boolean(),
        "number" => value.is_number(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "null" => value.is_null(),
        _ => return Err(format!("{path} schema.type {kind} is unsupported")),
    };
    if !valid_kind {
        return Err(format!("{path} must be a {kind}"));
    }
    validate_schema_bounds(value, definition, kind, path)?;
    if let Some(options) = definition.get("enum") {
        let options = options
            .as_array()
            .ok_or_else(|| format!("{path} schema.enum must be an array"))?;
        if !options.iter().any(|option| option == value) {
            return Err(format!("{path} is not an allowed enum value"));
        }
    }

    match kind {
        "object" => validate_object_schema(value, definition, path),
        "array" => {
            let item_schema = definition
                .get("items")
                .ok_or_else(|| format!("{path} array schema.items missing"))?;
            for (index, item) in value
                .as_array()
                .expect("validated array")
                .iter()
                .enumerate()
            {
                validate_schema_value(item, item_schema, &format!("{path}[{index}]"))?;
            }
            if definition.contains_key("properties")
                || definition.contains_key("required")
                || definition.contains_key("additionalProperties")
            {
                return Err(format!("{path} array schema contains object-only keywords"));
            }
            Ok(())
        }
        _ => {
            if definition.contains_key("properties")
                || definition.contains_key("required")
                || definition.contains_key("additionalProperties")
                || definition.contains_key("items")
            {
                return Err(format!("{path} scalar schema contains container keywords"));
            }
            Ok(())
        }
    }
}

fn validate_schema_bounds(
    value: &Value,
    definition: &serde_json::Map<String, Value>,
    kind: &str,
    path: &str,
) -> Result<(), String> {
    match kind {
        "integer" | "number" => {
            let actual = value
                .as_f64()
                .ok_or_else(|| format!("{path} must be numeric"))?;
            for (keyword, accepts) in [("minimum", true), ("maximum", false)] {
                if let Some(bound) = definition.get(keyword) {
                    let bound = bound
                        .as_f64()
                        .ok_or_else(|| format!("{path} schema.{keyword} must be numeric"))?;
                    if (accepts && actual < bound) || (!accepts && actual > bound) {
                        return Err(format!("{path} violates schema.{keyword}"));
                    }
                }
            }
            if definition.contains_key("minLength")
                || definition.contains_key("maxLength")
                || definition.contains_key("pattern")
                || definition.contains_key("minItems")
                || definition.contains_key("maxItems")
                || definition.contains_key("minProperties")
                || definition.contains_key("maxProperties")
            {
                return Err(format!(
                    "{path} numeric schema contains incompatible bounds"
                ));
            }
        }
        "string" => {
            validate_size_bounds(
                value.as_str().expect("validated string").chars().count(),
                definition,
                "minLength",
                "maxLength",
                path,
            )?;
            if let Some(pattern) = definition.get("pattern") {
                let pattern = pattern
                    .as_str()
                    .ok_or_else(|| format!("{path} schema.pattern must be a string"))?;
                let pattern = Regex::new(pattern)
                    .map_err(|error| format!("{path} schema.pattern is invalid: {error}"))?;
                if !pattern.is_match(value.as_str().expect("validated string")) {
                    return Err(format!("{path} violates schema.pattern"));
                }
            }
            if definition.contains_key("minimum")
                || definition.contains_key("maximum")
                || definition.contains_key("minItems")
                || definition.contains_key("maxItems")
                || definition.contains_key("minProperties")
                || definition.contains_key("maxProperties")
            {
                return Err(format!("{path} string schema contains incompatible bounds"));
            }
        }
        "array" => {
            validate_size_bounds(
                value.as_array().expect("validated array").len(),
                definition,
                "minItems",
                "maxItems",
                path,
            )?;
            if definition.contains_key("minimum")
                || definition.contains_key("maximum")
                || definition.contains_key("minLength")
                || definition.contains_key("maxLength")
                || definition.contains_key("pattern")
                || definition.contains_key("minProperties")
                || definition.contains_key("maxProperties")
            {
                return Err(format!("{path} array schema contains incompatible bounds"));
            }
        }
        "object" => {
            validate_size_bounds(
                value.as_object().expect("validated object").len(),
                definition,
                "minProperties",
                "maxProperties",
                path,
            )?;
            if definition.contains_key("minimum")
                || definition.contains_key("maximum")
                || definition.contains_key("minLength")
                || definition.contains_key("maxLength")
                || definition.contains_key("pattern")
                || definition.contains_key("minItems")
                || definition.contains_key("maxItems")
            {
                return Err(format!("{path} object schema contains incompatible bounds"));
            }
        }
        _ => {
            if definition.contains_key("minimum")
                || definition.contains_key("maximum")
                || definition.contains_key("minLength")
                || definition.contains_key("maxLength")
                || definition.contains_key("pattern")
                || definition.contains_key("minItems")
                || definition.contains_key("maxItems")
                || definition.contains_key("minProperties")
                || definition.contains_key("maxProperties")
            {
                return Err(format!("{path} scalar schema contains bounds"));
            }
        }
    }
    Ok(())
}

fn validate_size_bounds(
    actual: usize,
    definition: &serde_json::Map<String, Value>,
    minimum_key: &str,
    maximum_key: &str,
    path: &str,
) -> Result<(), String> {
    let parse_bound = |key: &str| -> Result<Option<usize>, String> {
        definition
            .get(key)
            .map(|value| {
                value
                    .as_u64()
                    .and_then(|value| usize::try_from(value).ok())
                    .ok_or_else(|| format!("{path} schema.{key} must be a non-negative integer"))
            })
            .transpose()
    };
    if let Some(minimum) = parse_bound(minimum_key)? {
        if actual < minimum {
            return Err(format!("{path} violates schema.{minimum_key}"));
        }
    }
    if let Some(maximum) = parse_bound(maximum_key)? {
        if actual > maximum {
            return Err(format!("{path} violates schema.{maximum_key}"));
        }
    }
    Ok(())
}

fn validate_object_schema(
    value: &Value,
    definition: &serde_json::Map<String, Value>,
    path: &str,
) -> Result<(), String> {
    if definition.contains_key("items") {
        return Err(format!("{path} object schema contains array-only items"));
    }
    let properties = definition
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| format!("{path} object schema.properties missing"))?;
    let required = definition
        .get("required")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{path} object schema.required missing"))?;
    let additional_properties = definition
        .get("additionalProperties")
        .cloned()
        .unwrap_or(Value::Bool(false));
    let object = value.as_object().expect("validated object");
    for required_name in required {
        let name = required_name
            .as_str()
            .ok_or_else(|| format!("{path} schema.required must contain strings"))?;
        if !properties.contains_key(name) {
            return Err(format!(
                "{path} required field {name} has no property schema"
            ));
        }
        if !object.contains_key(name) {
            return Err(format!("{path}.{name} is required"));
        }
    }
    for (name, item) in object {
        match properties.get(name) {
            Some(property_schema) => {
                validate_schema_value(item, property_schema, &format!("{path}.{name}"))?;
            }
            None if additional_properties == Value::Bool(true) => {}
            None if additional_properties == Value::Bool(false) => {
                return Err(format!("{path}.{name} is not allowed"));
            }
            None if additional_properties.is_object() => {
                validate_schema_value(item, &additional_properties, &format!("{path}.{name}"))?;
            }
            None => {
                return Err(format!(
                    "{path} schema.additionalProperties must be a boolean or schema object"
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        sync::atomic::{AtomicU8, Ordering},
    };

    use akzio_domain::{
        ArtifactLifecycle, ContextPolicy, ContractId, ContractPurpose, FailureDisposition,
        OutputContract, PromptBundle, RetryPolicy, TaskBudget, TaskRecipeId, TaskStatus,
        TerminationPolicy, ToolGrant, ToolKind, ToolSpec, WorkflowGraph, WorkflowNode,
        V2_SCHEMA_VERSION,
    };
    use akzio_store::v2::{StoredRun, WorkflowCommit};
    use tempfile::tempdir;

    use super::*;

    #[derive(Debug)]
    struct ToolThenOutputModel {
        evidence_id: ArtifactId,
        calls: AtomicU8,
    }

    impl AgentModel for ToolThenOutputModel {
        fn turn<'a>(
            &'a self,
            request: AgentModelRequest,
        ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
            Box::pin(async move {
                match self.calls.fetch_add(1, Ordering::SeqCst) {
                    0 => Err(ResearchError::ModelDebug {
                        error_class: "transport",
                        message: "transient fixture failure".to_owned(),
                        trace: ModelCallTrace {
                            request: json!({"fixture": "failed-provider-request"}),
                            result: json!({"error": "fixture-transport"}),
                        },
                    }),
                    1 => {
                        assert!(request.prior_tool_results.is_empty());
                        Ok(AgentModelTurn {
                            output: None,
                            tool_calls: vec![AgentToolCall {
                                call_id: "fixture-read-evidence".to_owned(),
                                name: "read_artifact".to_owned(),
                                arguments: json!({"artifact_id": self.evidence_id.0.as_str()}),
                            }],
                            model_debug: Some(ModelCallTrace {
                                request: json!({"fixture": "provider-request"}),
                                result: json!({"fixture": "provider-result"}),
                            }),
                        })
                    }
                    2 => {
                        assert_eq!(request.prior_tool_results.len(), 1);
                        assert_eq!(
                            request.prior_tool_results[0]["value"],
                            json!({"price": 100})
                        );
                        Ok(AgentModelTurn {
                            output: Some(json!({
                                        "schema_version": V2_SCHEMA_VERSION,
                                        "topic": "market_regime",
                                        "statement": "The selected price evidence supports the stated regime claim.",
                                        "horizon": "t5",
                                        "stance": "bullish",
                                        "materiality_ppm": 800_000,
                                        "confidence_ppm": 700_000,
                                        "grounds": [{
                                            "evidence": {
                                                "artifact_id": self.evidence_id.0.as_str(),
                                                "kind": "normalized_evidence"
                                            },
                                            "support": "The governed evidence supplied the price used in this claim."
                                        }],
                                        "evidence_gaps": []
                            })),
                            tool_calls: vec![],
                            model_debug: None,
                        })
                    }
                    _ => panic!("runtime requested an unexpected extra model turn"),
                }
            })
        }
    }

    #[derive(Debug)]
    struct FixedModel(AgentModelTurn);

    impl AgentModel for FixedModel {
        fn turn<'a>(
            &'a self,
            _: AgentModelRequest,
        ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
            Box::pin(async move { Ok(self.0.clone()) })
        }
    }

    #[derive(Debug)]
    struct DelayedToolModel {
        evidence_id: ArtifactId,
    }

    impl AgentModel for DelayedToolModel {
        fn turn<'a>(
            &'a self,
            _: AgentModelRequest,
        ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
            let evidence_id = self.evidence_id.clone();
            Box::pin(async move {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                Ok(AgentModelTurn {
                    output: None,
                    tool_calls: vec![AgentToolCall {
                        call_id: "fixture-expired-grant".to_owned(),
                        name: "read_artifact".to_owned(),
                        arguments: json!({"artifact_id": evidence_id.0.as_str()}),
                    }],
                    model_debug: None,
                })
            })
        }
    }

    #[derive(Debug)]
    struct SlowOutputModel;

    impl AgentModel for SlowOutputModel {
        fn turn<'a>(
            &'a self,
            _: AgentModelRequest,
        ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
            Box::pin(async move {
                tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
                Ok(AgentModelTurn {
                    output: Some(json!({"summary":"too late"})),
                    tool_calls: vec![],
                    model_debug: None,
                })
            })
        }
    }

    fn contract(store: &V2Store) -> AgentContract {
        AgentContract::new(
            ContractId::new(),
            1,
            ContractPurpose::new("research.analyst").unwrap(),
            "produce a claim",
            PromptBundle {
                version: 1,
                governance: store.put_bytes(b"governance", "text/plain").unwrap(),
                role: store.put_bytes(b"prompt", "text/plain").unwrap(),
            },
            ContextPolicy {
                permitted_kinds: BTreeSet::from([ArtifactKind::NormalizedEvidence]),
                permitted_source_families: BTreeSet::from(["market".to_owned()]),
                min_artifacts: 1,
                max_artifacts: 4,
                max_bytes: 4096,
                max_tokens: 1024,
                allow_raw_reread: false,
            },
            vec![ToolGrant {
                kind: ToolKind::ReadEvidence,
                allowed_sources: vec!["market".to_owned()],
            }],
            vec![ToolSpec {
                name: "read_artifact".to_owned(),
                description: "read granted artifact".to_owned(),
                kind: ToolKind::ReadEvidence,
                input_schema: store.put_json(&artifact_id_tool_input_schema()).unwrap(),
                strict: true,
            }],
            OutputContract {
                artifact_kind: ArtifactKind::Claim,
                schema: store.put_json(&claim_output_schema()).unwrap(),
            },
            TaskBudget {
                max_input_tokens: 1024,
                max_output_tokens: 128,
                max_wall_time_secs: 30,
                max_tool_calls: 2,
            },
            RetryPolicy {
                max_attempts: 2,
                initial_backoff_ms: 1,
                retry_transport: true,
                retry_rate_limited: true,
                retry_invalid_output: false,
            },
            TerminationPolicy::leaf(),
            FailureDisposition::FailRun,
        )
        .unwrap()
    }

    fn provenance() -> ArtifactProvenance {
        ArtifactProvenance {
            source_family: "market".to_owned(),
            observed_at: None,
            retrieved_at: Utc::now(),
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        }
    }

    struct Fixture {
        _root: tempfile::TempDir,
        store: V2Store,
        catalogue: ContractCatalogue,
        claimed: akzio_store::v2::ClaimedAttempt,
        evidence: Artifact,
    }

    fn fixture_with(configure: impl FnOnce(&mut AgentContract)) -> Fixture {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let mut contract = contract(&store);
        configure(&mut contract);
        contract.candidate_capability_ceiling.context = contract.context.clone();
        contract.candidate_capability_ceiling.tool_grants = contract.tool_grants.clone();
        contract.contract_hash = contract.expected_hash().unwrap();
        contract.validate().unwrap();
        let catalogue = ContractCatalogue::install(&store, [contract.clone()], Utc::now()).unwrap();
        let node = WorkflowNode {
            task_id: akzio_domain::TaskId::new(),
            recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
            contract_hash: Some(contract.contract_hash.clone()),
            objective: "claim".to_owned(),
            dependencies: vec![],
            input_artifacts: vec![],
            priority: 50,
            budget: contract.budget.clone(),
            retry: contract.retry.clone(),
            on_failure: FailureDisposition::FailRun,
            parent_task_id: None,
        };
        let graph = WorkflowGraph {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: "test".to_owned(),
            nodes: vec![node.clone()],
        };
        let graph_artifact = Artifact::new(
            ArtifactKind::WorkflowGraph,
            store.put_json(&graph).unwrap(),
            "fixture",
            ArtifactLifecycle::RunScoped,
            provenance(),
            None,
            vec![],
            Utc::now(),
        )
        .unwrap();
        let run = StoredRun {
            run_id: akzio_domain::RunId::new(),
            purpose: akzio_domain::RunPurpose::Debug,
            topology_id: graph.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: Utc::now(),
        };
        store
            .commit_workflow(&WorkflowCommit {
                run,
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();
        let claimed = store
            .claim_next_task("fixture", Utc::now(), Duration::seconds(60))
            .unwrap()
            .unwrap();
        let evidence = Artifact::new(
            ArtifactKind::NormalizedEvidence,
            store
                .put_bytes(br#"{"price":100}"#, "application/json")
                .unwrap(),
            "fixture",
            ArtifactLifecycle::RunScoped,
            provenance(),
            Some(task_origin(&claimed.permit)),
            vec![],
            Utc::now(),
        )
        .unwrap();
        store
            .write_task_artifact(
                &claimed.permit,
                &evidence,
                "evidence.normalized",
                Utc::now(),
            )
            .unwrap();
        Fixture {
            _root: root,
            store,
            catalogue,
            claimed,
            evidence,
        }
    }

    #[test]
    fn planner_draft_schema_is_closed_and_governed() {
        let schema = planner_draft_output_schema();
        let valid = serde_json::json!({
            "schema_version": V2_SCHEMA_VERSION,
            "topology_id": "active",
            "tasks": {
                "analyst": {
                    "recipe_id": "research.analyst",
                    "objective": "analyse governed TQQQ evidence",
                    "depends_on": [],
                    "priority": 50,
                    "evidence_needs": [{
                        "schema_version": V2_SCHEMA_VERSION,
                        "source_family": "alpaca",
                        "resource": "bars:TQQQ:1d",
                        "max_age_secs": 86400
                    }]
                }
            }
        });
        validate_schema_value(&valid, &schema, "$").unwrap();

        let mut invalid_version = valid.clone();
        invalid_version["schema_version"] = serde_json::json!(V2_SCHEMA_VERSION + 1);
        assert!(validate_schema_value(&invalid_version, &schema, "$").is_err());

        let mut invalid_recipe = valid.clone();
        invalid_recipe["tasks"]["analyst"]["recipe_id"] = serde_json::json!("gate.paper");
        assert!(validate_schema_value(&invalid_recipe, &schema, "$").is_err());

        let mut invalid_source = valid.clone();
        invalid_source["tasks"]["analyst"]["evidence_needs"][0]["source_family"] =
            serde_json::json!("uninstalled-web");
        assert!(validate_schema_value(&invalid_source, &schema, "$").is_err());

        let mut invalid_priority = valid.clone();
        invalid_priority["tasks"]["analyst"]["priority"] = serde_json::json!(101);
        assert!(validate_schema_value(&invalid_priority, &schema, "$").is_err());

        let mut artifact_ref = valid.clone();
        artifact_ref["tasks"]["analyst"]["artifact_id"] = serde_json::json!("sha256:forged");
        assert!(validate_schema_value(&artifact_ref, &schema, "$").is_err());

        let mut tool_or_role = valid;
        tool_or_role["tasks"]["analyst"]["tool"] = serde_json::json!("fetch_web");
        assert!(validate_schema_value(&tool_or_role, &schema, "$").is_err());
    }

    #[test]
    fn active_catalogue_installs_canonical_contracts_and_bounded_recipes() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let active = ActiveResearchCatalogue::install(&store, Utc::now()).unwrap();
        let expected = [
            (PLANNER_RECIPE_ID, ArtifactKind::WorkflowProposalDraft),
            ("research.analyst", ArtifactKind::Claim),
            ("research.critic", ArtifactKind::Critique),
            ("research.synthesizer", ArtifactKind::DecisionProposal),
        ];

        assert_eq!(active.contracts.contracts().count(), expected.len());
        for (purpose, output_kind) in expected {
            let installed = active
                .contracts
                .contracts()
                .find(|installed| installed.contract.purpose.as_str() == purpose)
                .unwrap();
            assert_eq!(installed.contract.output.artifact_kind, output_kind);
            assert_eq!(
                installed.contract.context.min_artifacts,
                if purpose == PLANNER_RECIPE_ID { 0 } else { 1 }
            );
            assert_eq!(
                installed.contract.termination.require_evidence,
                purpose != PLANNER_RECIPE_ID
            );
            let recipe = active
                .recipes
                .recipe(&TaskRecipeId::new(purpose).unwrap())
                .unwrap();
            assert_eq!(
                recipe.contract_hash.as_ref(),
                Some(&installed.contract.contract_hash)
            );
            assert_eq!(recipe.budget, installed.contract.budget);
            assert_eq!(recipe.retry, installed.contract.retry);
            assert_eq!(recipe.on_failure, installed.contract.on_failure);
            assert_eq!(
                recipe.max_children,
                installed.contract.termination.max_child_tasks
            );
            assert_eq!(recipe.max_depth, installed.contract.termination.max_depth);
            assert_eq!(
                recipe.allowed_evidence_sources,
                recipe_evidence_sources(&installed.contract)
            );
        }

        for (recipe_id, task_class) in [
            (EVIDENCE_GATE_RECIPE_ID, RuntimeTaskClass::Evidence),
            (DECISION_GATE_RECIPE_ID, RuntimeTaskClass::DecisionGate),
            (EXECUTION_GATE_RECIPE_ID, RuntimeTaskClass::ExecutionGate),
            (PAPER_COMMIT_RECIPE_ID, RuntimeTaskClass::PaperCommit),
            (RECONCILE_RECIPE_ID, RuntimeTaskClass::Reconcile),
            (EVALUATE_RECIPE_ID, RuntimeTaskClass::Evaluate),
        ] {
            let recipe = active
                .recipes
                .recipe(&TaskRecipeId::new(recipe_id).unwrap())
                .unwrap();
            assert_eq!(recipe.task_class, task_class);
            assert_eq!(recipe.contract_hash, None);
            assert!(recipe.allowed_evidence_sources.is_empty());
        }
        store.verify_integrity().unwrap();
    }

    #[test]
    fn active_catalogue_restores_store_owned_heads_after_restart() {
        let root = tempdir().unwrap();
        let now = Utc::now();
        let store = V2Store::open(root.path()).unwrap();
        let first = ActiveResearchCatalogue::install(&store, now).unwrap();
        let expected = first
            .contracts
            .contracts()
            .map(|installed| {
                (
                    installed.contract.purpose.as_str().to_owned(),
                    installed.contract.contract_hash.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        drop(first);
        drop(store);

        let reopened = V2Store::open(root.path()).unwrap();
        let restored =
            ActiveResearchCatalogue::install(&reopened, now + Duration::seconds(1)).unwrap();
        let actual = restored
            .contracts
            .contracts()
            .map(|installed| {
                (
                    installed.contract.purpose.as_str().to_owned(),
                    installed.contract.contract_hash.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();

        assert_eq!(actual, expected);
        reopened.verify_integrity().unwrap();
    }

    #[test]
    fn candidate_install_is_durable_bounded_and_non_executable() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        let active = ActiveResearchCatalogue::install(&store, now).unwrap();
        let baseline = active
            .contracts
            .contracts()
            .find(|installed| installed.contract.purpose.as_str() == PLANNER_RECIPE_ID)
            .unwrap()
            .contract
            .clone();
        let mut candidate = baseline.clone();
        candidate.version += 1;
        candidate.contract_hash = candidate.expected_hash().unwrap();
        candidate.validate().unwrap();

        let installed = active
            .install_candidate(&store, &baseline.contract_hash, &candidate, now)
            .unwrap();
        assert_eq!(installed.contract, candidate);
        assert_eq!(
            store
                .active_contract(&baseline.purpose)
                .unwrap()
                .unwrap()
                .contract
                .contract_hash,
            baseline.contract_hash
        );
        assert_eq!(
            store
                .contract_installation(&candidate.contract_hash)
                .unwrap()
                .unwrap()
                .baseline_contract_hash,
            Some(baseline.contract_hash.clone())
        );
        assert!(active.contracts.get(&candidate.contract_hash).is_err());

        let mut expanded = candidate;
        expanded.version += 1;
        expanded
            .context
            .permitted_source_families
            .insert("unapproved_source".to_owned());
        expanded.candidate_capability_ceiling = akzio_domain::CandidateCapabilityCeiling {
            context: expanded.context.clone(),
            tool_grants: expanded.tool_grants.clone(),
        };
        expanded.contract_hash = expanded.expected_hash().unwrap();
        expanded.validate().unwrap();
        assert!(matches!(
            active.install_candidate(&store, &baseline.contract_hash, &expanded, now),
            Err(ResearchError::CandidateCapabilityExpansion { .. })
        ));
        store.verify_integrity().unwrap();
    }

    #[test]
    fn decision_proposal_schema_matches_typed_decision_draft() {
        let schema = decision_proposal_output_schema();
        let forecasts = akzio_domain::Asset::EXECUTABLE
            .into_iter()
            .flat_map(|asset| {
                ["t1", "t3", "t5"].into_iter().map(move |horizon| {
                    json!({
                        "asset": asset.symbol(),
                        "horizon": horizon,
                        "positive_return_probability_ppm": 500000,
                        "expected_return_ppm": 0,
                    })
                })
            })
            .collect::<Vec<_>>();
        let valid = json!({
            "summary": "blocked fixture decision",
            "confidence_ppm": 500000,
            "forecasts": forecasts,
            "claims": [],
            "critiques": [],
            "evidence": [],
            "material_conflicts": [],
            "hard_blockers": ["missing_evidence"],
            "soft_warnings": []
        });

        validate_schema_value(&valid, &schema, "$").unwrap();
        serde_json::from_value::<akzio_domain::DecisionDraft>(valid)
            .unwrap()
            .validate()
            .unwrap();
        for invalid in [
            json!({
                "summary": "invalid",
                "confidence_ppm": 500000,
                "blockers": ["anything"],
                "asset_views": {}
            }),
            json!({
                "summary": "extra field",
                "targets": {
                    "weights": { "TQQQ": 0, "QQQ": 0, "SOXX": 0, "SOXL": 0 }
                },
                "confidence_ppm": 500000,
                "forecasts": [],
                "claims": [],
                "critiques": [],
                "evidence": [],
                "material_conflicts": [],
                "hard_blockers": ["missing_evidence"],
                "soft_warnings": [],
                "authority": "paper"
            }),
        ] {
            assert!(validate_schema_value(&invalid, &schema, "$").is_err());
        }
    }

    #[test]
    fn artifact_reference_schema_enforces_sha256_pattern() {
        let schema = artifact_ref_schema(&["claim"]);
        let valid = json!({
            "artifact_id": "a".repeat(64),
            "kind": "claim",
        });
        validate_schema_value(&valid, &schema, "$").unwrap();

        let invalid = json!({
            "artifact_id": "not-a-content-hash",
            "kind": "claim",
        });
        assert!(validate_schema_value(&invalid, &schema, "$").is_err());
    }

    #[test]
    fn active_catalogue_rejects_planner_that_does_not_output_a_draft() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let mut contracts = canonical_active_contracts(&store).unwrap();
        let planner = contracts
            .iter_mut()
            .find(|contract| contract.purpose.as_str() == PLANNER_RECIPE_ID)
            .unwrap();
        planner.output.artifact_kind = ArtifactKind::WorkflowProposal;
        planner.contract_hash = planner.expected_hash().unwrap();
        planner.validate().unwrap();
        let catalogue = ContractCatalogue::install(&store, contracts, Utc::now()).unwrap();

        assert!(matches!(
            catalogue.active_recipe_catalogue(&store),
            Err(ResearchError::ActiveContractOutputMismatch {
                purpose,
                expected: ArtifactKind::WorkflowProposalDraft,
                actual: ArtifactKind::WorkflowProposal,
            }) if purpose == PLANNER_RECIPE_ID
        ));
    }

    #[test]
    fn active_catalogue_rejects_candidate_or_unknown_contract_recipe() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let mut contracts = canonical_active_contracts(&store).unwrap();
        let mut candidate = contracts
            .iter()
            .find(|contract| contract.purpose.as_str() == "research.analyst")
            .unwrap()
            .clone();
        candidate.contract_id = ContractId("akzio.v2.research.candidate".to_owned());
        candidate.version = 2;
        candidate.purpose = ContractPurpose::new("research.candidate").unwrap();
        candidate.responsibility = "candidate data only".to_owned();
        candidate.contract_hash = candidate.expected_hash().unwrap();
        candidate.validate().unwrap();
        contracts.push(candidate);
        let catalogue = ContractCatalogue::install(&store, contracts, Utc::now()).unwrap();

        assert!(matches!(
            catalogue.active_recipe_catalogue(&store),
            Err(ResearchError::UnexpectedActiveContractPurpose(purpose))
                if purpose == "research.candidate"
        ));
    }

    #[test]
    fn contract_catalogue_rejects_duplicate_hash_and_identity_version() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let contract = contract(&store);

        let catalogue = ContractCatalogue::install(&store, [contract.clone()], Utc::now()).unwrap();
        assert_eq!(
            catalogue.contract_hash_for(&contract.contract_id, contract.version),
            Some(&contract.contract_hash)
        );

        assert!(matches!(
            ContractCatalogue::install(&store, [contract.clone(), contract.clone()], Utc::now(),),
            Err(ResearchError::DuplicateContract(_))
        ));

        let mut changed = contract.clone();
        changed.responsibility = "different responsibility".to_owned();
        changed.contract_hash = changed.expected_hash().unwrap();
        changed.validate().unwrap();
        assert!(matches!(
            ContractCatalogue::install(&store, [contract, changed], Utc::now()),
            Err(ResearchError::DuplicateContractVersion { .. })
        ));
    }

    #[test]
    fn contract_catalogue_rejects_candidate_capability_expansion() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let active = contract(&store);
        let catalogue = ContractCatalogue::install(&store, [active.clone()], Utc::now()).unwrap();

        let mut candidate = active.clone();
        candidate
            .context
            .permitted_source_families
            .insert("news".to_owned());
        candidate.tool_grants[0]
            .allowed_sources
            .push("news".to_owned());
        candidate.candidate_capability_ceiling = akzio_domain::CandidateCapabilityCeiling {
            context: candidate.context.clone(),
            tool_grants: candidate.tool_grants.clone(),
        };
        candidate.contract_hash = candidate.expected_hash().unwrap();
        candidate.validate().unwrap();

        assert!(matches!(
            catalogue.validate_candidate(&active.contract_hash, &candidate),
            Err(ResearchError::CandidateCapabilityExpansion { .. })
        ));

        let mut narrowed = active.clone();
        narrowed.budget.max_input_tokens /= 2;
        narrowed.contract_hash = narrowed.expected_hash().unwrap();
        narrowed.validate().unwrap();
        catalogue
            .validate_candidate(&active.contract_hash, &narrowed)
            .unwrap();
    }

    #[tokio::test]
    async fn model_client_adapter_debug_trace_retains_the_provider_request_and_result() {
        let artifact_hash = akzio_domain::ContentHash::of_bytes(b"fixture-artifact");
        let adapter = ModelClientAdapter::with_debug(
            akzio_model::ModelClient::Fixture(json!({
                "output_text": "",
                "tool_calls": [{
                    "call_id": "fixture-tool",
                    "name": "read_artifact",
                    "arguments": {"artifact_id": artifact_hash.as_str()},
                }],
            })),
            true,
        );
        let response = adapter
            .turn(AgentModelRequest {
                contract_hash: akzio_domain::ContentHash::of_bytes(b"fixture-contract"),
                purpose: "research.analyst".to_owned(),
                prompt: "fixture prompt".to_owned(),
                objective: "fixture objective".to_owned(),
                manifest_artifact_id: ArtifactId(akzio_domain::ContentHash::of_bytes(
                    b"fixture-manifest",
                )),
                context: vec![],
                prior_tool_results: vec![],
                output_schema: json!({
                    "type": "object",
                    "properties": {"summary": {"type": "string"}},
                    "required": ["summary"],
                    "additionalProperties": false,
                }),
                max_output_tokens: 32,
                tools: vec![AgentToolDefinition {
                    name: "read_artifact".to_owned(),
                    description: "fixture".to_owned(),
                    input_schema: json!({
                        "type": "object",
                        "properties": {"artifact_id": {"type": "string"}},
                        "required": ["artifact_id"],
                        "additionalProperties": false,
                    }),
                    strict: true,
                }],
            })
            .await
            .unwrap();

        assert!(response.output.is_none());
        assert_eq!(response.tool_calls.len(), 1);
        assert_eq!(response.tool_calls[0].name, "read_artifact");
        assert_eq!(
            response.tool_calls[0].arguments["artifact_id"],
            artifact_hash.to_string()
        );
        let trace = response.model_debug.expect("debug trace is retained");
        assert_eq!(trace.request["model"], "fixture");
        assert_eq!(trace.request["tools"][0]["strict"], true);
        assert_eq!(trace.result["tool_calls"][0]["call_id"], "fixture-tool");
    }

    #[tokio::test]
    async fn agent_runtime_rejects_a_grant_that_expires_during_a_model_turn() {
        let Fixture {
            _root,
            store,
            catalogue,
            claimed,
            evidence,
        } = fixture_with(|_| {});
        let runtime = AgentRuntime::new(store.clone(), catalogue, Duration::milliseconds(1));
        let model = DelayedToolModel {
            evidence_id: evidence.artifact_id.clone(),
        };

        assert!(matches!(
            runtime
                .run(
                    &claimed.permit,
                    &claimed.node,
                    [ArtifactRef {
                        artifact_id: evidence.artifact_id,
                        kind: ArtifactKind::NormalizedEvidence,
                    }],
                    &model,
                    Utc::now(),
                )
                .await,
            Err(ResearchError::Context(ContextError::GrantDenied { .. }))
        ));
        store.verify_integrity().unwrap();
    }

    #[tokio::test]
    async fn agent_runtime_enforces_tool_source_family_scope() {
        let Fixture {
            _root,
            store,
            catalogue,
            claimed,
            ..
        } = fixture_with(|contract| {
            contract
                .context
                .permitted_source_families
                .insert("news".to_owned());
        });
        let news = Artifact::new(
            ArtifactKind::NormalizedEvidence,
            store
                .put_bytes(br#"{"headline":"fixture"}"#, "application/json")
                .unwrap(),
            "fixture.news",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "news".to_owned(),
                ..provenance()
            },
            Some(task_origin(&claimed.permit)),
            vec![],
            Utc::now(),
        )
        .unwrap();
        store
            .write_task_artifact(&claimed.permit, &news, "evidence.normalized", Utc::now())
            .unwrap();
        let runtime = AgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));
        let model = FixedModel(AgentModelTurn {
            output: None,
            tool_calls: vec![AgentToolCall {
                call_id: "fixture-news-denied".to_owned(),
                name: "read_artifact".to_owned(),
                arguments: json!({"artifact_id": news.artifact_id.0.as_str()}),
            }],
            model_debug: None,
        });

        assert!(matches!(
            runtime
                .run(
                    &claimed.permit,
                    &claimed.node,
                    [ArtifactRef {
                        artifact_id: news.artifact_id,
                        kind: ArtifactKind::NormalizedEvidence,
                    }],
                    &model,
                    Utc::now(),
                )
                .await,
            Err(ResearchError::ToolSourceNotGranted { .. })
        ));
        let failure_id = store
            .events_after(&claimed.run_id, 0, 100)
            .unwrap()
            .into_iter()
            .find(|event| event.event_type == "tool.failed")
            .and_then(|event| event.artifact_id)
            .expect("failed tool result is durable");
        let failure = store.artifact(&failure_id).unwrap();
        let trace: Value =
            serde_json::from_slice(&store.read_blob(&failure.blob).unwrap()).unwrap();
        assert_eq!(trace["ok"], false);
        assert_eq!(trace["error"]["code"], "tool_source_not_granted");
        assert!(failure
            .source_refs
            .iter()
            .any(|reference| reference.kind == ArtifactKind::ToolCall));
        store.verify_integrity().unwrap();
    }

    #[tokio::test]
    async fn agent_runtime_records_invalid_tool_arguments_before_rejecting() {
        let Fixture {
            _root,
            store,
            catalogue,
            claimed,
            evidence,
        } = fixture_with(|_| {});
        let runtime = AgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));
        let model = FixedModel(AgentModelTurn {
            output: None,
            tool_calls: vec![AgentToolCall {
                call_id: "fixture-invalid-arguments".to_owned(),
                name: "read_artifact".to_owned(),
                arguments: json!({"unexpected": true}),
            }],
            model_debug: None,
        });

        assert!(matches!(
            runtime
                .run(
                    &claimed.permit,
                    &claimed.node,
                    [ArtifactRef {
                        artifact_id: evidence.artifact_id,
                        kind: ArtifactKind::NormalizedEvidence,
                    }],
                    &model,
                    Utc::now(),
                )
                .await,
            Err(ResearchError::InvalidOutput(_))
        ));

        let failure_id = store
            .events_after(&claimed.run_id, 0, 100)
            .unwrap()
            .into_iter()
            .find(|event| event.event_type == "tool.failed")
            .and_then(|event| event.artifact_id)
            .expect("invalid tool result is durable");
        let failure = store.artifact(&failure_id).unwrap();
        let trace: Value =
            serde_json::from_slice(&store.read_blob(&failure.blob).unwrap()).unwrap();
        assert_eq!(trace["ok"], false);
        assert_eq!(trace["error"]["code"], "invalid_tool_arguments");
        let call = failure
            .source_refs
            .iter()
            .find(|reference| reference.kind == ArtifactKind::ToolCall)
            .and_then(|reference| store.artifact(&reference.artifact_id).ok())
            .expect("invalid tool call is durable");
        let call_trace: Value =
            serde_json::from_slice(&store.read_blob(&call.blob).unwrap()).unwrap();
        assert_eq!(call_trace["call"]["arguments"], json!({"unexpected": true}));
    }

    #[tokio::test]
    async fn agent_runtime_records_and_rejects_an_overdue_model_turn() {
        let Fixture {
            _root,
            store,
            catalogue,
            claimed,
            evidence,
        } = fixture_with(|contract| contract.budget.max_wall_time_secs = 1);
        let runtime = AgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));

        assert!(matches!(
            runtime
                .run(
                    &claimed.permit,
                    &claimed.node,
                    [ArtifactRef {
                        artifact_id: evidence.artifact_id,
                        kind: ArtifactKind::NormalizedEvidence,
                    }],
                    &SlowOutputModel,
                    Utc::now(),
                )
                .await,
            Err(ResearchError::WallTimeExceeded { maximum_secs: 1 })
        ));
        store.verify_integrity().unwrap();
    }

    #[tokio::test]
    async fn agent_runtime_records_complete_tool_trace_and_contract_validated_claim() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let contract = contract(&store);
        let catalogue = ContractCatalogue::install(&store, [contract.clone()], Utc::now()).unwrap();
        let node = WorkflowNode {
            task_id: akzio_domain::TaskId::new(),
            recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
            contract_hash: Some(contract.contract_hash.clone()),
            objective: "claim".to_owned(),
            dependencies: vec![],
            input_artifacts: vec![],
            priority: 50,
            budget: contract.budget.clone(),
            retry: contract.retry.clone(),
            on_failure: FailureDisposition::FailRun,
            parent_task_id: None,
        };
        let graph = WorkflowGraph {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: "test".to_owned(),
            nodes: vec![node.clone()],
        };
        let graph_artifact = Artifact::new(
            ArtifactKind::WorkflowGraph,
            store.put_json(&graph).unwrap(),
            "fixture",
            ArtifactLifecycle::RunScoped,
            provenance(),
            None,
            vec![],
            Utc::now(),
        )
        .unwrap();
        let run = StoredRun {
            run_id: akzio_domain::RunId::new(),
            purpose: akzio_domain::RunPurpose::Debug,
            topology_id: graph.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: Utc::now(),
        };
        store
            .commit_workflow(&WorkflowCommit {
                run,
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();
        let claimed = store
            .claim_next_task("fixture", Utc::now(), Duration::seconds(60))
            .unwrap()
            .unwrap();
        let evidence = Artifact::new(
            ArtifactKind::NormalizedEvidence,
            store
                .put_bytes(br#"{"price":100}"#, "application/json")
                .unwrap(),
            "fixture",
            ArtifactLifecycle::RunScoped,
            provenance(),
            Some(task_origin(&claimed.permit)),
            vec![],
            Utc::now(),
        )
        .unwrap();
        store
            .write_task_artifact(
                &claimed.permit,
                &evidence,
                "evidence.normalized",
                Utc::now(),
            )
            .unwrap();
        let runtime = AgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));
        let model = ToolThenOutputModel {
            evidence_id: evidence.artifact_id.clone(),
            calls: AtomicU8::new(0),
        };
        let output = runtime
            .run(
                &claimed.permit,
                &claimed.node,
                [ArtifactRef {
                    artifact_id: evidence.artifact_id.clone(),
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &model,
                Utc::now(),
            )
            .await
            .unwrap();
        assert_eq!(output.kind, ArtifactKind::Claim);
        assert!(matches!(
            store.artifact(&output.artifact_id),
            Err(StoreError::MissingArtifact(id)) if id == output.artifact_id
        ));
        assert_eq!(model.calls.load(Ordering::SeqCst), 3);
        assert!(output
            .source_refs
            .iter()
            .any(|source| source.kind == ArtifactKind::ContextManifest));
        assert!(output
            .source_refs
            .iter()
            .any(|source| source.kind == ArtifactKind::AgentTurn));
        assert!(output.source_refs.iter().any(|source| {
            source.kind == ArtifactKind::NormalizedEvidence
                && source.artifact_id == evidence.artifact_id
        }));
        let tool_result = output
            .source_refs
            .iter()
            .find(|source| source.kind == ArtifactKind::ToolResult)
            .expect("output retains the tool result trace");
        let tool_result = store.artifact(&tool_result.artifact_id).unwrap();
        assert!(tool_result
            .source_refs
            .iter()
            .any(|source| source.kind == ArtifactKind::ToolCall));
        assert!(tool_result
            .source_refs
            .iter()
            .any(|source| source.artifact_id == evidence.artifact_id));
        let tool_trace: Value =
            serde_json::from_slice(&store.read_blob(&tool_result.blob).unwrap()).unwrap();
        assert!(tool_trace["request_hash"].as_str().is_some());
        let tool_call = tool_result
            .source_refs
            .iter()
            .find(|source| source.kind == ArtifactKind::ToolCall)
            .and_then(|source| store.artifact(&source.artifact_id).ok())
            .expect("tool call trace is durable");
        let tool_call_trace: Value =
            serde_json::from_slice(&store.read_blob(&tool_call.blob).unwrap()).unwrap();
        assert_eq!(
            tool_call_trace["call"]["arguments"]["artifact_id"],
            evidence.artifact_id.0.as_str()
        );
        let turn_trace = output
            .source_refs
            .iter()
            .filter(|source| source.kind == ArtifactKind::AgentTurn)
            .filter_map(|source| store.artifact(&source.artifact_id).ok())
            .map(|artifact| {
                serde_json::from_slice::<Value>(&store.read_blob(&artifact.blob).unwrap()).unwrap()
            })
            .find(|trace| {
                trace["response"]["model_debug"]["request"]["fixture"] == "provider-request"
            })
            .expect("agent turn trace retains request and response");
        assert!(turn_trace["request_hash"].as_str().is_some());
        assert_eq!(turn_trace["request"]["tools"][0]["strict"], true);
        assert_eq!(
            turn_trace["response"]["model_debug"]["request"]["fixture"],
            "provider-request"
        );
        assert_eq!(
            turn_trace["response"]["model_debug"]["result"]["fixture"],
            "provider-result"
        );
        let failed_turn_trace = output
            .source_refs
            .iter()
            .filter(|source| source.kind == ArtifactKind::AgentTurn)
            .filter_map(|source| store.artifact(&source.artifact_id).ok())
            .map(|artifact| {
                serde_json::from_slice::<Value>(&store.read_blob(&artifact.blob).unwrap()).unwrap()
            })
            .find(|trace| trace["error_class"] == "transport")
            .expect("failed agent turn trace is durable");
        assert_eq!(
            failed_turn_trace["model_debug"]["request"]["fixture"],
            "failed-provider-request"
        );
        assert_eq!(
            failed_turn_trace["model_debug"]["result"]["error"],
            "fixture-transport"
        );

        let malformed = FixedModel(AgentModelTurn {
            output: Some(json!({"summary": 42})),
            tool_calls: vec![],
            model_debug: None,
        });
        assert!(matches!(
            runtime
                .run(
                    &claimed.permit,
                    &claimed.node,
                    [ArtifactRef {
                        artifact_id: evidence.artifact_id.clone(),
                        kind: ArtifactKind::NormalizedEvidence,
                    }],
                    &malformed,
                    Utc::now(),
                )
                .await,
            Err(ResearchError::InvalidOutput(_))
        ));

        let denied_tool = FixedModel(AgentModelTurn {
            output: None,
            tool_calls: vec![AgentToolCall {
                call_id: "fixture-denied-raw".to_owned(),
                name: "read_raw_evidence".to_owned(),
                arguments: json!({"artifact_id": evidence.artifact_id.0.as_str()}),
            }],
            model_debug: None,
        });
        assert!(matches!(
            runtime
                .run(
                    &claimed.permit,
                    &claimed.node,
                    [ArtifactRef {
                        artifact_id: evidence.artifact_id.clone(),
                        kind: ArtifactKind::NormalizedEvidence,
                    }],
                    &denied_tool,
                    Utc::now(),
                )
                .await,
            Err(ResearchError::ToolNotGranted(_))
        ));

        let over_budget_tools = FixedModel(AgentModelTurn {
            output: None,
            tool_calls: (0..3)
                .map(|index| AgentToolCall {
                    call_id: format!("fixture-over-budget-{index}"),
                    name: "read_artifact".to_owned(),
                    arguments: json!({"artifact_id": evidence.artifact_id.0.as_str()}),
                })
                .collect(),
            model_debug: None,
        });
        assert!(matches!(
            runtime
                .run(
                    &claimed.permit,
                    &claimed.node,
                    [ArtifactRef {
                        artifact_id: evidence.artifact_id.clone(),
                        kind: ArtifactKind::NormalizedEvidence,
                    }],
                    &over_budget_tools,
                    Utc::now(),
                )
                .await,
            Err(ResearchError::ToolBudgetExceeded)
        ));

        let mut mismatched_node = claimed.node.clone();
        mismatched_node.budget.max_tool_calls = 1;
        assert!(matches!(
            runtime
                .run(
                    &claimed.permit,
                    &mismatched_node,
                    std::iter::empty::<ArtifactRef>(),
                    &malformed,
                    Utc::now(),
                )
                .await,
            Err(ResearchError::NodePolicyMismatch)
        ));
        store
            .commit_attempt(
                &claimed.permit,
                std::slice::from_ref(&output),
                TaskStatus::Succeeded,
                Utc::now(),
            )
            .unwrap();
        let persisted = store.artifact(&output.artifact_id).unwrap();
        assert_eq!(persisted.artifact_id, output.artifact_id);
        assert_eq!(persisted.kind, output.kind);
        store.verify_integrity().unwrap();
    }

    #[tokio::test]
    async fn agent_runtime_enforces_input_and_output_token_budgets() {
        let Fixture {
            _root,
            store,
            catalogue,
            claimed,
            evidence,
        } = fixture_with(|contract| contract.budget.max_input_tokens = 1);
        let runtime = AgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));
        let output = FixedModel(AgentModelTurn {
            output: Some(json!({"summary":"source-linked claim"})),
            tool_calls: vec![],
            model_debug: None,
        });
        let result = runtime
            .run(
                &claimed.permit,
                &claimed.node,
                [ArtifactRef {
                    artifact_id: evidence.artifact_id,
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &output,
                Utc::now(),
            )
            .await;
        assert!(
            matches!(&result, Err(ResearchError::InputBudgetExceeded { .. })),
            "{result:?}"
        );
        store.verify_integrity().unwrap();

        let Fixture {
            _root,
            store,
            catalogue,
            claimed,
            evidence,
        } = fixture_with(|contract| contract.budget.max_output_tokens = 1);
        let runtime = AgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));
        assert!(matches!(
            runtime
                .run(
                    &claimed.permit,
                    &claimed.node,
                    [ArtifactRef {
                        artifact_id: evidence.artifact_id,
                        kind: ArtifactKind::NormalizedEvidence,
                    }],
                    &output,
                    Utc::now(),
                )
                .await,
            Err(ResearchError::OutputBudgetExceeded { .. })
        ));
        store.verify_integrity().unwrap();
    }
}
