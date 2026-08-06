//! Versioned, Rust-owned research contracts and dynamic research planning.
//!
//! A model can propose work only through an installed Contract. It cannot mint
//! new roles, tools, schemas, or workflow gates at runtime.

mod tools;

pub use tools::{SourceRegistry, ToolCallContext, ToolExecution, ToolRuntime};

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

use akzio_context::{ContextBroker, ContextManifest, ContextRequest, NewJsonDocument};
use akzio_domain::{
    content_hash_json, AgentContract, ContractId, DecisionDraft, DocumentId, DocumentKind,
    DocumentLifecycle, DocumentOrigin, DocumentRecord, FailureDisposition, Provenance, RetryPolicy,
    TaskBudget, TaskId, TaskKind, TaskSpec, TerminationPolicy, ToolGrant, ToolKind, TopologyId,
    WorkflowPlan, V2_SCHEMA_VERSION,
};
use akzio_model::{ModelClient, ModelError, ModelRequest};
use akzio_runtime::{PlanPatch, WorkflowRuntime};
use akzio_store::ClaimedTask;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentRole {
    Planner,
    Investigator,
    Challenger,
    Synthesizer,
}

pub const BASELINE_TOPOLOGY_ID: &str = "research-baseline-v2";
pub const INVESTIGATOR_ONLY_TOPOLOGY_ID: &str = "research-investigator-only-v2";

pub fn baseline_topology() -> TopologyId {
    TopologyId(BASELINE_TOPOLOGY_ID.to_owned())
}

pub fn shadow_topology(parent: &TopologyId) -> Option<TopologyId> {
    (parent.0 == BASELINE_TOPOLOGY_ID).then(|| TopologyId(INVESTIGATOR_ONLY_TOPOLOGY_ID.to_owned()))
}

fn topology_allows(topology_id: &TopologyId, role: AgentRole) -> bool {
    topology_id.0 != INVESTIGATOR_ONLY_TOPOLOGY_ID || role != AgentRole::Challenger
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannerProposal {
    pub summary: String,
    pub tasks: Vec<PlannerTaskProposal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannerTaskProposal {
    pub role: PlannedResearchRole,
    pub question: String,
    pub priority: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlannedResearchRole {
    Investigator,
    Challenger,
}

impl PlannedResearchRole {
    const fn agent_role(self) -> AgentRole {
        match self {
            Self::Investigator => AgentRole::Investigator,
            Self::Challenger => AgentRole::Challenger,
        }
    }

    const fn task_kind(self) -> TaskKind {
        match self {
            Self::Investigator => TaskKind::Investigate,
            Self::Challenger => TaskKind::Challenge,
        }
    }
}

impl AgentRole {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Planner => "planner.research",
            Self::Investigator => "investigator.evidence",
            Self::Challenger => "challenger.adversarial",
            Self::Synthesizer => "synthesizer.decision",
        }
    }

    const fn task_kind(self) -> TaskKind {
        match self {
            Self::Planner => TaskKind::Plan,
            Self::Investigator => TaskKind::Investigate,
            Self::Challenger => TaskKind::Challenge,
            Self::Synthesizer => TaskKind::SynthesizeDecision,
        }
    }
}

#[derive(Debug, Error)]
pub enum ResearchError {
    #[error(transparent)]
    Context(#[from] akzio_context::ContextError),
    #[error(transparent)]
    Model(#[from] ModelError),
    #[error(transparent)]
    Store(#[from] akzio_store::StoreError),
    #[error(transparent)]
    Tool(#[from] tools::ToolError),
    #[error(transparent)]
    Runtime(#[from] akzio_runtime::RuntimeError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("contract prompt is not valid UTF-8")]
    InvalidPromptUtf8,
    #[error("agent response is not valid JSON")]
    InvalidJson,
    #[error("agent output violates contract: {0}")]
    InvalidOutput(String),
    #[error("planner proposes more child tasks than its Contract permits")]
    PlannerTaskLimit,
    #[error("task {task:?} does not match its installed Contract")]
    ContractTaskMismatch { task: TaskKind },
}

pub type Result<T> = std::result::Result<T, ResearchError>;

#[derive(Debug, Clone)]
pub struct InstalledContract {
    pub role: AgentRole,
    pub contract: AgentContract,
    pub document: DocumentRecord,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputSpec {
    PlannerProposal,
    Claims,
    Challenge,
    DecisionDraft,
}

impl OutputSpec {
    const fn name(self) -> &'static str {
        match self {
            Self::PlannerProposal => "workflow_plan",
            Self::Claims => "claims",
            Self::Challenge => "challenge",
            Self::DecisionDraft => "decision_draft",
        }
    }

    fn from_name(name: &str) -> Result<Self> {
        match name {
            "workflow_plan" => Ok(Self::PlannerProposal),
            "claims" => Ok(Self::Claims),
            "challenge" => Ok(Self::Challenge),
            "decision_draft" => Ok(Self::DecisionDraft),
            other => Err(ResearchError::InvalidOutput(format!(
                "unknown contract output type {other}"
            ))),
        }
    }

    fn schema(self) -> Value {
        match self {
            Self::PlannerProposal => serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "summary": {"type": "string"},
                    "tasks": {
                        "type": "array",
                        "maxItems": 8,
                        "items": {
                            "type": "object",
                            "additionalProperties": false,
                            "properties": {
                                "role": {"enum": ["investigator", "challenger"]},
                                "question": {"type": "string"},
                                "priority": {"type": "integer", "minimum": 0, "maximum": 100}
                            },
                            "required": ["role", "question", "priority"]
                        }
                    }
                },
                "required": ["summary", "tasks"]
            }),
            Self::Claims => serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "summary": {"type": "string"},
                    "claims": {"type": "array"}
                },
                "required": ["summary", "claims"]
            }),
            Self::Challenge => serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "summary": {"type": "string"},
                    "verdict": {"enum": ["supported", "contested", "unresolved"]},
                    "arguments": {"type": "array"}
                },
                "required": ["summary", "verdict", "arguments"]
            }),
            Self::DecisionDraft => serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "summary": {"type": "string"},
                    "targets": {
                        "type": "object",
                        "additionalProperties": false,
                        "properties": {
                            "weights": {
                                "type": "object",
                                "additionalProperties": false,
                                "properties": {
                                    "TQQQ": {"type": "integer", "minimum": 0, "maximum": 1000000},
                                    "QQQ": {"type": "integer", "minimum": 0, "maximum": 1000000},
                                    "SOXX": {"type": "integer", "minimum": 0, "maximum": 1000000},
                                    "SOXL": {"type": "integer", "minimum": 0, "maximum": 1000000}
                                },
                                "required": ["TQQQ", "QQQ", "SOXX", "SOXL"]
                            }
                        },
                        "required": ["weights"]
                    },
                    "confidence_ppm": {"type": "integer", "minimum": 0, "maximum": 1000000},
                    "forecasts": {
                        "type": "array",
                        "minItems": 3,
                        "maxItems": 3,
                        "items": {
                            "type": "object",
                            "additionalProperties": false,
                            "properties": {
                                "trading_days": {"enum": [1, 3, 5]},
                                "positive_return_probability_ppm": {"type": "integer", "minimum": 0, "maximum": 1000000},
                                "expected_return_ppm": {"type": "integer"}
                            },
                            "required": [
                                "trading_days",
                                "positive_return_probability_ppm",
                                "expected_return_ppm"
                            ]
                        }
                    },
                    "blockers": {"type": "array", "items": {"type": "string"}},
                    "claim_refs": {"type": "array", "items": {"type": "string"}}
                },
                "required": [
                    "summary",
                    "targets",
                    "confidence_ppm",
                    "forecasts",
                    "blockers",
                    "claim_refs"
                ]
            }),
        }
    }

    fn validate(self, output: &Value) -> Result<DocumentKind> {
        let summary = output
            .get("summary")
            .and_then(Value::as_str)
            .filter(|summary| !summary.trim().is_empty())
            .ok_or_else(|| ResearchError::InvalidOutput("summary is required".to_owned()))?;
        let _ = summary;

        match self {
            Self::PlannerProposal => {
                let proposal = serde_json::from_value::<PlannerProposal>(output.clone())
                    .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;
                if proposal
                    .tasks
                    .iter()
                    .any(|task| task.question.trim().is_empty() || task.priority > 100)
                {
                    return Err(ResearchError::InvalidOutput(
                        "planner has an empty question or invalid priority".to_owned(),
                    ));
                }
                Ok(DocumentKind::PlannerProposal)
            }
            Self::Claims => output
                .get("claims")
                .and_then(Value::as_array)
                .map(|_| DocumentKind::AgentClaim)
                .ok_or_else(|| ResearchError::InvalidOutput("claims are required".to_owned())),
            Self::Challenge => output
                .get("verdict")
                .and_then(Value::as_str)
                .filter(|value| matches!(*value, "supported" | "contested" | "unresolved"))
                .map(|_| DocumentKind::Challenge)
                .ok_or_else(|| {
                    ResearchError::InvalidOutput("valid challenge verdict is required".to_owned())
                }),
            Self::DecisionDraft => {
                let draft = serde_json::from_value::<DecisionDraft>(output.clone())
                    .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;
                draft
                    .validate()
                    .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;
                Ok(DocumentKind::DecisionDraft)
            }
        }
    }
}

#[derive(Debug, Clone)]
struct ContractDefinition {
    role: AgentRole,
    responsibility: &'static str,
    input_kinds: &'static [DocumentKind],
    output: OutputSpec,
    tool_grants: Vec<ToolGrant>,
    retry: RetryPolicy,
    termination: TerminationPolicy,
    on_failure: FailureDisposition,
}

#[derive(Debug, Clone)]
pub struct ContractRegistry {
    by_role: BTreeMap<AgentRole, InstalledContract>,
}

impl ContractRegistry {
    pub fn install(broker: &ContextBroker, created_at: DateTime<Utc>) -> Result<Self> {
        let by_role = default_contract_definitions()
            .into_iter()
            .map(|definition| install_contract(broker, definition, created_at))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|installed| (installed.role, installed))
            .collect();
        Ok(Self { by_role })
    }

    pub fn get(&self, role: AgentRole) -> Result<&InstalledContract> {
        self.by_role.get(&role).ok_or_else(|| {
            ResearchError::InvalidOutput(format!("missing installed Contract {role:?}"))
        })
    }

    pub fn installed(&self) -> Vec<InstalledContract> {
        self.by_role.values().cloned().collect()
    }

    fn role_for_hash(&self, hash: &akzio_domain::ContentHash) -> Option<AgentRole> {
        self.by_role.iter().find_map(|(role, installed)| {
            (installed.contract.contract_hash == *hash).then_some(*role)
        })
    }
}

fn default_contract_definitions() -> Vec<ContractDefinition> {
    let retry = RetryPolicy {
        max_attempts: 3,
        initial_backoff_ms: 250,
        retry_transport: true,
        retry_rate_limited: true,
        retry_invalid_output: true,
    };
    let read_evidence = || ToolGrant {
        kind: ToolKind::ReadEvidence,
        allowed_sources: vec!["market".to_owned(), "internal".to_owned()],
    };

    vec![
         ContractDefinition {
             role: AgentRole::Planner,
             responsibility: "Plan bounded research only from supplied evidence. Select useful investigator or challenger work; never decide trades.",
             input_kinds: &[
                 DocumentKind::NormalizedEvidence,
                 DocumentKind::SemanticDetail,
                 DocumentKind::Memory,
             ],
             output: OutputSpec::PlannerProposal,
             tool_grants: vec![read_evidence()],
             retry: retry.clone(),
             termination: TerminationPolicy {
                 max_child_tasks: 8,
                 max_depth: 2,
                 require_evidence: true,
                 stop_when_evidence_complete: true,
             },
            on_failure: FailureDisposition::FailRun,
         },
         ContractDefinition {
             role: AgentRole::Investigator,
             responsibility: "Turn permitted evidence into falsifiable, source-linked claims. State uncertainty explicitly.",
             input_kinds: &[
                 DocumentKind::NormalizedEvidence,
                 DocumentKind::SemanticDetail,
                 DocumentKind::PlannerProposal,
             ],
             output: OutputSpec::Claims,
             tool_grants: vec![
                 read_evidence(),
                 ToolGrant {
                     kind: ToolKind::ReadRawEvidence,
                     allowed_sources: vec!["market".to_owned()],
                 },
                 ToolGrant {
                     kind: ToolKind::ReadMarketData,
                     allowed_sources: vec!["market".to_owned()],
                 },
             ],
             retry: retry.clone(),
             termination: TerminationPolicy::leaf(),
            on_failure: FailureDisposition::FailTask,
         },
         ContractDefinition {
             role: AgentRole::Challenger,
             responsibility: "Attack named claims with supplied evidence. Return supported, contested, or unresolved.",
             input_kinds: &[
                 DocumentKind::SemanticDetail,
                 DocumentKind::AgentClaim,
                 DocumentKind::Challenge,
                 DocumentKind::PlannerProposal,
             ],
             output: OutputSpec::Challenge,
             tool_grants: vec![read_evidence()],
             retry: retry.clone(),
             termination: TerminationPolicy::leaf(),
            on_failure: FailureDisposition::FailTask,
         },
         ContractDefinition {
             role: AgentRole::Synthesizer,
             responsibility: "Produce a calibrated 1, 3, and 5 trading-day decision draft with blockers. Never create orders.",
             input_kinds: &[
                 DocumentKind::SemanticDetail,
                 DocumentKind::AgentClaim,
                 DocumentKind::Challenge,
                 DocumentKind::Memory,
             ],
             output: OutputSpec::DecisionDraft,
             tool_grants: vec![read_evidence()],
             retry,
             termination: TerminationPolicy::leaf(),
            on_failure: FailureDisposition::FailRun,
         },
     ]
}

fn install_contract(
    broker: &ContextBroker,
    definition: ContractDefinition,
    created_at: DateTime<Utc>,
) -> Result<InstalledContract> {
    let prompt = format!(
        "You are {}. {} Use only Rust-approved tools. Return only the required JSON object.",
        definition.role.name(),
        definition.responsibility,
    );
    let prompt = broker
        .store()
        .put_bytes(prompt.as_bytes(), "text/plain")
        .map_err(akzio_context::ContextError::from)?;
    let schema = akzio_domain::canonical_json_bytes(&definition.output.schema())?;
    let output_schema = broker
        .store()
        .put_bytes(&schema, "application/schema+json")
        .map_err(akzio_context::ContextError::from)?;
    let budget = TaskBudget {
        max_input_tokens: 32_000,
        max_output_tokens: 4_000,
        max_wall_time_secs: 180,
        max_tool_calls: 4,
    };
    let hash = content_hash_json(&serde_json::json!({
        "role": definition.role,
        "responsibility": definition.responsibility,
        "input_kinds": definition.input_kinds,
        "output_type": definition.output.name(),
        "prompt": prompt.hash,
        "output_schema": output_schema.hash,
        "budget": budget,
        "tool_grants": definition.tool_grants,
        "retry": definition.retry,
        "termination": definition.termination,
        "on_failure": definition.on_failure,
    }))?;
    let contract = AgentContract {
        schema_version: V2_SCHEMA_VERSION,
        contract_id: ContractId(hash.as_str().to_owned()),
        version: 2,
        agent_kind: definition.role.name().to_owned(),
        responsibility: definition.responsibility.to_owned(),
        prompt,
        input_context_kinds: definition.input_kinds.to_vec(),
        tool_grants: definition.tool_grants,
        output_type: definition.output.name().to_owned(),
        output_schema,
        budget,
        retry: definition.retry,
        termination: definition.termination,
        on_failure: definition.on_failure,
        contract_hash: hash,
    };
    contract
        .validate()
        .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;
    persist_contract(broker, definition.role, contract, created_at)
}

fn persist_contract(
    broker: &ContextBroker,
    role: AgentRole,
    contract: AgentContract,
    created_at: DateTime<Utc>,
) -> Result<InstalledContract> {
    if let Ok(document_id) = broker.store().contract_document(&contract.contract_hash) {
        return Ok(InstalledContract {
            role,
            contract,
            document: broker
                .store()
                .read_document(&document_id)
                .map_err(akzio_context::ContextError::from)?,
        });
    }
    let document = broker.record_json(NewJsonDocument {
        kind: DocumentKind::ContractBundle,
        producer: "contracts.registry".to_owned(),
        run_id: None,
        lifecycle: DocumentLifecycle::Canonical,
        source_refs: vec![],
        origin: None,
        value: &serde_json::to_value(&contract)?,
        created_at,
    })?;
    broker
        .store()
        .register_contract(&contract.contract_hash, &document.document_id)
        .map_err(akzio_context::ContextError::from)?;
    Ok(InstalledContract {
        role,
        contract,
        document,
    })
}

async fn invoke_agent_with_tools(
    broker: &ContextBroker,
    client: &ModelClient,
    contract: &AgentContract,
    context: &ContextManifest,
    objective: &str,
    context_manifest: &DocumentRecord,
    task: &ClaimedTask,
    created_at: DateTime<Utc>,
) -> Result<DocumentRecord> {
    let prompt = String::from_utf8(
        broker
            .store()
            .read_blob(&contract.prompt)
            .map_err(akzio_context::ContextError::from)?,
    )
    .map_err(|_| ResearchError::InvalidPromptUtf8)?;
    let schema: Value = serde_json::from_slice(
        &broker
            .store()
            .read_blob(&contract.output_schema)
            .map_err(akzio_context::ContextError::from)?,
    )?;
    let context_json = context
        .documents
        .iter()
        .map(|document| {
            Ok(serde_json::json!({
                "document_id": document.document_id,
                "kind": document.kind,
                "provenance": document.provenance,
                "source_refs": document.source_refs,
                "value": broker.read_json(document)?,
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    let tools = ToolRuntime::new(broker.clone(), SourceRegistry::default());
    let tool_context = ToolCallContext {
        run_id: task.run_id.clone(),
        task_id: task.task_id.clone(),
        attempt_id: task.attempt_id.clone(),
        contract_hash: contract.contract_hash.clone(),
        context_manifest_id: Some(context_manifest.document_id.clone()),
    };
    let definitions = (contract.budget.max_tool_calls > 0)
        .then(|| tools.definitions(contract))
        .unwrap_or_default();
    let mut tool_results = Vec::new();
    let mut tool_result_refs = Vec::new();
    let mut used_tool_calls = 0_u16;
    let mut turn_index = 0_u16;

    loop {
        let started = Instant::now();
        let response = client
            .respond(ModelRequest {
                instructions: prompt.clone(),
                input: serde_json::to_string(&serde_json::json!({
                    "objective": objective,
                    "context_manifest_id": context_manifest.document_id,
                    "context": context_json,
                    "tool_results": tool_results,
                }))
                .expect("agent request JSON serializes"),
                schema_name: Some(contract.output_type.clone()),
                schema: Some(schema.clone()),
                max_output_tokens: contract.budget.max_output_tokens,
                tools: definitions.clone(),
            })
            .await?;

        let turn_document = broker.record_json(NewJsonDocument {
            kind: DocumentKind::AgentTurn,
            producer: format!("{}.turn", contract.agent_kind),
            run_id: Some(task.run_id.clone()),
            lifecycle: DocumentLifecycle::RunScoped,
            source_refs: std::iter::once(context_manifest.document_id.clone())
                .chain(tool_result_refs.iter().cloned())
                .collect(),
            origin: Some(DocumentOrigin::task(
                task.task_id.clone(),
                task.attempt_id.clone(),
                Some(contract.contract_hash.clone()),
            )),
            value: &serde_json::json!({
                "turn": turn_index,
                "objective": objective,
                "context_manifest_id": context_manifest.document_id,
                "contract_hash": contract.contract_hash,
                "tool_result_ids": tool_result_refs,
                "latency_ms": started.elapsed().as_millis() as u64,
                "response": response.raw.clone(),
                "output_text": response.output_text.clone(),
                "tool_calls": response.tool_calls.clone(),
            }),
            created_at,
        })?;
        broker.store().append_event(&akzio_domain::EventEnvelope {
            schema_version: V2_SCHEMA_VERSION,
            run_id: task.run_id.clone(),
            task_id: Some(task.task_id.clone()),
            attempt_id: Some(task.attempt_id.clone()),
            contract_hash: Some(contract.contract_hash.clone()),
            causation_id: Some(format!("agent-turn-{turn_index}")),
            event_type: "agent.turn.completed".to_owned(),
            payload_document_id: Some(turn_document.document_id.clone()),
            payload: Some(turn_document.blob.clone()),
            created_at,
        })?;
        turn_index = turn_index.saturating_add(1);

        if response.tool_calls.is_empty() {
            let output = serde_json::from_str::<Value>(&response.output_text)
                .map_err(|_| ResearchError::InvalidJson)?;
            let output_kind = OutputSpec::from_name(&contract.output_type)?.validate(&output)?;
            let source_refs = std::iter::once(context_manifest.document_id.clone())
                .chain(
                    context
                        .documents
                        .iter()
                        .map(|document| document.document_id.clone()),
                )
                .chain(tool_result_refs)
                .collect();
            return broker
                .record_json(NewJsonDocument {
                    kind: output_kind,
                    producer: contract.agent_kind.clone(),
                    run_id: Some(task.run_id.clone()),
                    lifecycle: DocumentLifecycle::RunScoped,
                    source_refs,
                    origin: Some(DocumentOrigin::task(
                        task.task_id.clone(),
                        task.attempt_id.clone(),
                        Some(contract.contract_hash.clone()),
                    )),
                    value: &output,
                    created_at,
                })
                .map_err(ResearchError::from);
        }

        for call in response.tool_calls {
            if used_tool_calls >= contract.budget.max_tool_calls {
                return Err(ResearchError::InvalidOutput(
                    "agent exceeded its Contract tool budget".to_owned(),
                ));
            }
            used_tool_calls += 1;
            let execution = tools.execute(contract, &tool_context, &call, created_at)?;
            tool_result_refs.push(execution.result_document.document_id.clone());
            tool_results.push(serde_json::json!({
                "call_id": call.call_id,
                "name": call.name,
                "result_document_id": execution.result_document.document_id,
                "result": execution.model_result,
            }));
        }
    }
}

pub async fn execute_research_task(
    broker: &ContextBroker,
    runtime: &WorkflowRuntime,
    client: &ModelClient,
    registry: &ContractRegistry,
    task: &ClaimedTask,
    now: DateTime<Utc>,
) -> Result<DocumentRecord> {
    let contract_hash = task.contract_hash.as_ref().ok_or_else(|| {
        ResearchError::InvalidOutput("agent task has no Contract hash".to_owned())
    })?;
    let role = registry
        .role_for_hash(contract_hash)
        .ok_or(ResearchError::ContractTaskMismatch { task: task.kind })?;
    if role.task_kind() != task.kind {
        return Err(ResearchError::ContractTaskMismatch { task: task.kind });
    }
    let contract_document = broker.store().contract_document(contract_hash)?;
    let contract_record = broker.store().read_document(&contract_document)?;
    let contract: AgentContract = serde_json::from_value(broker.read_json(&contract_record)?)?;
    contract
        .validate()
        .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;

    let allowed_kinds = contract
        .input_context_kinds
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut document_ids = Vec::new();
    let mut explicit_document_ids = BTreeSet::new();
    for document_id in broker.store().task_input_refs(&task.task_id)? {
        let document = broker.store().read_document(&document_id)?;
        if !allowed_kinds.contains(&document.kind) {
            return Err(ResearchError::InvalidOutput(format!(
                "task input {} is not allowed by {}",
                document.document_id, contract.agent_kind
            )));
        }
        explicit_document_ids.insert(document.document_id.clone());
        document_ids.push(document.document_id);
    }
    for document in broker.store().documents_for_run(&task.run_id)? {
        if allowed_kinds.contains(&document.kind) && !document_ids.contains(&document.document_id) {
            document_ids.push(document.document_id);
        }
    }
    let context = broker.assemble(
        &ContextRequest {
            allowed_kinds,
            explicit_document_ids,
            max_documents: 64,
            max_bytes: 256 * 1024,
            max_tokens: contract.budget.max_input_tokens,
            policy_version: 1,
        },
        document_ids,
    )?;
    let context_manifest = broker.record_manifest(
        format!("context.{}", contract.agent_kind),
        Some(task.run_id.clone()),
        &context,
        Provenance {
            source: "akzio.context".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            contract_hash: Some(contract.contract_hash.clone()),
        },
        Some(DocumentOrigin::task(
            task.task_id.clone(),
            task.attempt_id.clone(),
            Some(contract.contract_hash.clone()),
        )),
        now,
    )?;
    let output = invoke_agent_with_tools(
        broker,
        client,
        &contract,
        &context,
        &task.objective,
        &context_manifest,
        task,
        now,
    )
    .await?;

    if task.kind == TaskKind::Plan {
        let proposal: PlannerProposal = serde_json::from_value(broker.read_json(&output)?)
            .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;
        let workflow = runtime.load(&task.run_id)?;
        let patch = planner_patch_from_proposal(
            &workflow.plan,
            &registry.installed(),
            output.document_id.clone(),
            &proposal,
        )?;
        if !patch.add_tasks.is_empty() {
            runtime.apply_planner_patch_to_run(&task.run_id, &workflow, task, patch, now)?;
        }
    }
    Ok(output)
}

pub fn bootstrap_workflow(
    _purpose: akzio_domain::RunPurpose,
    topology_id: TopologyId,
    contracts: &[InstalledContract],
) -> WorkflowPlan {
    let contract_for = |role| {
        contracts
            .iter()
            .find(|contract| contract.role == role)
            .map(|contract| contract.contract.contract_hash.clone())
    };
    let gate_budget = || TaskBudget {
        max_input_tokens: 1_024,
        max_output_tokens: 256,
        max_wall_time_secs: 120,
        max_tool_calls: 0,
    };
    let task = |kind: TaskKind,
                role: Option<AgentRole>,
                on_failure: FailureDisposition,
                dependencies: Vec<TaskId>| {
        let contract = role.and_then(contract_for);
        TaskSpec {
            task_id: TaskId::new(),
            kind,
            objective: format!("{kind:?} task"),
            contract_hash: contract.clone(),
            dependencies,
            input_refs: vec![],
            budget: contract
                .clone()
                .and_then(|hash| {
                    contracts
                        .iter()
                        .find(|item| item.contract.contract_hash == hash)
                        .map(|item| item.contract.budget.clone())
                })
                .unwrap_or_else(gate_budget),
            on_failure,
            priority: if on_failure == FailureDisposition::FailRun {
                100
            } else {
                50
            },
            max_attempts: contract
                .clone()
                .and_then(|hash| {
                    contracts
                        .iter()
                        .find(|item| item.contract.contract_hash == hash)
                        .map(|item| item.contract.retry.max_attempts)
                })
                .unwrap_or(1),
            parent_task_id: None,
        }
    };

    let ingest = task(TaskKind::Ingest, None, FailureDisposition::FailRun, vec![]);
    let overlay = task(
        TaskKind::MemoryOverlay,
        None,
        FailureDisposition::SkipTask,
        vec![],
    );
    let mut planner = task(
        TaskKind::Plan,
        Some(AgentRole::Planner),
        FailureDisposition::FailRun,
        vec![ingest.task_id.clone(), overlay.task_id.clone()],
    );
    planner.objective = if topology_id.0 == INVESTIGATOR_ONLY_TOPOLOGY_ID {
        "Plan investigator-only research; do not create challenger tasks.".to_owned()
    } else {
        "Plan bounded evidence-driven research; create challenger tasks only when useful."
            .to_owned()
    };
    WorkflowPlan {
        schema_version: V2_SCHEMA_VERSION,
        topology_id,
        tasks: vec![ingest, overlay, planner],
    }
}

pub fn planner_patch_from_proposal(
    workflow: &WorkflowPlan,
    contracts: &[InstalledContract],
    planner_document_id: DocumentId,
    proposal: &PlannerProposal,
) -> Result<PlanPatch> {
    let planner = contracts
        .iter()
        .find(|contract| contract.role == AgentRole::Planner)
        .ok_or_else(|| {
            ResearchError::InvalidOutput("planner Contract is not installed".to_owned())
        })?;
    if proposal.tasks.len() > usize::from(planner.contract.termination.max_child_tasks) {
        return Err(ResearchError::PlannerTaskLimit);
    }
    let planner_task = workflow
        .tasks
        .iter()
        .find(|task| task.kind == TaskKind::Plan)
        .ok_or_else(|| ResearchError::InvalidOutput("workflow has no planner task".to_owned()))?;
    if workflow
        .tasks
        .iter()
        .any(|task| task.input_refs.contains(&planner_document_id))
    {
        return Ok(PlanPatch {
            add_tasks: vec![],
            add_dependencies: vec![],
            skip_optional_tasks: vec![],
        });
    }

    let mut added = Vec::with_capacity(proposal.tasks.len());
    let mut investigator_ids = Vec::new();
    for (index, proposed) in proposal.tasks.iter().enumerate() {
        if proposed.question.trim().is_empty() || proposed.priority > 100 {
            return Err(ResearchError::InvalidOutput(
                "planner has an empty question or invalid priority".to_owned(),
            ));
        }
        let role = proposed.role.agent_role();
        if !topology_allows(&workflow.topology_id, role) {
            continue;
        }
        let contract = contracts
            .iter()
            .find(|installed| installed.role == role)
            .ok_or_else(|| {
                ResearchError::InvalidOutput(format!("{role:?} Contract is not installed"))
            })?;
        let task_id = TaskId(format!("planner-{}-{index}", planner_document_id.0));
        let mut dependencies = vec![planner_task.task_id.clone()];
        if proposed.role == PlannedResearchRole::Challenger {
            dependencies.extend(investigator_ids.iter().cloned());
        } else {
            investigator_ids.push(task_id.clone());
        }
        added.push(TaskSpec {
            task_id,
            kind: proposed.role.task_kind(),
            objective: proposed.question.clone(),
            contract_hash: Some(contract.contract.contract_hash.clone()),
            dependencies,
            input_refs: vec![planner_document_id.clone()],
            budget: contract.contract.budget.clone(),
            on_failure: contract.contract.on_failure,
            priority: proposed.priority,
            max_attempts: contract.contract.retry.max_attempts,
            parent_task_id: Some(planner_task.task_id.clone()),
        });
    }
    let synthesizer = contracts
        .iter()
        .find(|installed| installed.role == AgentRole::Synthesizer)
        .ok_or_else(|| {
            ResearchError::InvalidOutput("Synthesizer Contract is not installed".to_owned())
        })?;
    let synthesis_dependencies = std::iter::once(planner_task.task_id.clone())
        .chain(added.iter().map(|task| task.task_id.clone()))
        .collect();
    added.push(TaskSpec {
        task_id: TaskId(format!("planner-synthesis-{}", planner_document_id.0)),
        kind: TaskKind::SynthesizeDecision,
        objective: "Synthesize a calibrated decision from planner-selected evidence.".to_owned(),
        contract_hash: Some(synthesizer.contract.contract_hash.clone()),
        dependencies: synthesis_dependencies,
        input_refs: vec![],
        budget: synthesizer.contract.budget.clone(),
        on_failure: synthesizer.contract.on_failure,
        priority: 100,
        max_attempts: synthesizer.contract.retry.max_attempts,
        parent_task_id: Some(planner_task.task_id.clone()),
    });
    Ok(PlanPatch {
        add_tasks: added,
        add_dependencies: vec![],
        skip_optional_tasks: vec![],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use akzio_domain::{ContentHash, RunId, RunPurpose};
    use akzio_store::V2Store;
    use tempfile::tempdir;

    #[test]
    fn contracts_are_single_source_and_hash_unique() {
        let directory = tempdir().unwrap();
        let broker = ContextBroker::new(V2Store::open(directory.path()).unwrap());
        let contracts = ContractRegistry::install(&broker, Utc::now()).unwrap();
        assert_eq!(contracts.installed().len(), 4);
        assert_eq!(
            contracts
                .installed()
                .iter()
                .map(|item| item.contract.contract_hash.clone())
                .collect::<BTreeSet<_>>()
                .len(),
            4
        );
    }

    #[test]
    fn decision_contract_requires_forecasts() {
        let output = serde_json::json!({
            "summary": "stay in cash",
            "targets": {"weights": {"TQQQ": 0, "QQQ": 0, "SOXX": 0, "SOXL": 0}},
            "confidence_ppm": 500_000,
            "blockers": [],
            "claim_refs": []
        });
        assert!(matches!(
            OutputSpec::DecisionDraft.validate(&output),
            Err(ResearchError::InvalidOutput(_))
        ));
    }

    #[test]
    fn bootstrap_contains_only_the_dynamic_foundation() {
        let directory = tempdir().unwrap();
        let broker = ContextBroker::new(V2Store::open(directory.path()).unwrap());
        let registry = ContractRegistry::install(&broker, Utc::now()).unwrap();
        let plan = bootstrap_workflow(
            RunPurpose::Paper,
            baseline_topology(),
            &registry.installed(),
        );
        assert_eq!(plan.tasks.len(), 3);
        assert!(plan.tasks.iter().any(|task| task.kind == TaskKind::Ingest));
        assert!(plan
            .tasks
            .iter()
            .any(|task| task.kind == TaskKind::MemoryOverlay));
        assert!(plan.tasks.iter().any(|task| task.kind == TaskKind::Plan));
        assert!(plan.tasks.iter().all(|task| {
            !matches!(
                task.kind,
                TaskKind::DecisionGate | TaskKind::ExecutionGate | TaskKind::ExecutePaper
            )
        }));
    }

    #[tokio::test]
    async fn tool_loop_persists_call_result_and_final_claim() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        let broker = ContextBroker::new(store.clone());
        let registry = ContractRegistry::install(&broker, Utc::now()).unwrap();
        let contract = registry
            .get(AgentRole::Investigator)
            .unwrap()
            .contract
            .clone();
        let now = Utc::now();
        let run = RunId::new();
        store
            .create_run(&run, RunPurpose::Debug, "test", now)
            .unwrap();
        let evidence = broker
            .record_json_with_provenance(
                NewJsonDocument {
                    kind: DocumentKind::NormalizedEvidence,
                    producer: "test.evidence".to_owned(),
                    run_id: Some(run.clone()),
                    lifecycle: DocumentLifecycle::RunScoped,
                    source_refs: vec![],
                    origin: None,
                    value: &serde_json::json!({"symbol": "TQQQ", "price": 100}),
                    created_at: now,
                },
                Provenance {
                    source: "akzio.fixture".to_owned(),
                    observed_at: None,
                    retrieved_at: now,
                    source_uri: None,
                    confidence_ppm: 1_000_000,
                    contract_hash: None,
                },
            )
            .unwrap();
        let task_id = TaskId::new();
        store
            .enqueue_task_with_contract(
                &run,
                &task_id,
                TaskKind::Investigate,
                Some(&contract.contract_hash),
                FailureDisposition::FailTask,
                now,
            )
            .unwrap();
        let task = store
            .claim_next_task("fixture", now, now + chrono::Duration::seconds(30))
            .unwrap()
            .unwrap();
        let context = ContextManifest {
            documents: vec![evidence.clone()],
            selection: vec![],
            total_bytes: evidence.blob.bytes,
            estimated_tokens: 64,
            policy_version: 1,
            input_hash: ContentHash::of_bytes(b"fixture-context"),
        };
        let client = ModelClient::fixture_sequence([
            serde_json::json!({"output": [{
                "type": "function_call",
                "call_id": "call-1",
                "name": "read_evidence",
                "arguments": format!("{{\"document_id\":\"{}\"}}", evidence.document_id),
            }]}),
            serde_json::json!({"output_text": r#"{"summary":"tool-backed claim","claims":[]}"#}),
        ]);
        let manifest = broker
            .record_manifest(
                "context.fixture",
                Some(run.clone()),
                &context,
                Provenance::local("test", now),
                None,
                now,
            )
            .unwrap();
        let output = invoke_agent_with_tools(
            &broker,
            &client,
            &contract,
            &context,
            "inspect fixture evidence",
            &manifest,
            &task,
            now,
        )
        .await
        .unwrap();
        assert_eq!(output.kind, DocumentKind::AgentClaim);
        let documents = store.documents_for_run(&run).unwrap();
        assert!(documents
            .iter()
            .any(|document| document.kind == DocumentKind::ToolCall));
        assert!(documents
            .iter()
            .any(|document| document.kind == DocumentKind::ToolResult));
        let turn = documents
            .iter()
            .find(|document| document.kind == DocumentKind::AgentTurn)
            .expect("every model response is persisted as an agent turn");
        assert_eq!(
            turn.origin
                .as_ref()
                .and_then(|origin| origin.task_id.clone()),
            Some(task.task_id.clone())
        );
        assert_eq!(
            turn.origin
                .as_ref()
                .and_then(|origin| origin.attempt_id.clone()),
            Some(task.attempt_id.clone())
        );
        store.verify_integrity().unwrap();
    }
}
