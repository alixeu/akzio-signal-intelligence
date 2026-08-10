//! Contract-driven Agent runtime for the v2 system.

use std::{
    collections::BTreeMap,
    time::{Duration as StdDuration, Instant},
};

use akzio_context::v2::{
    ContextBroker as RebuildContextBroker, ContextError as RebuildContextError,
    ContextManifest as RebuildContextManifest,
};
use akzio_domain::{
    AgentContract, Artifact, ArtifactId, ArtifactKind, ArtifactLifecycle, ArtifactOrigin,
    ArtifactProvenance, ArtifactRef, DomainError, ReadGrant, TaskWritePermit, WorkflowNode,
};
use akzio_model::{ModelClient, ModelError, ModelRequest, ModelToolDefinition};
use akzio_store::v2::{StoreError, V2Store};
use chrono::{DateTime, Duration, Utc};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RebuildResearchError {
    #[error(transparent)]
    Context(#[from] RebuildContextError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
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
    #[error("tool {0} is not granted by the Agent Contract")]
    ToolNotGranted(String),
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

pub type RebuildResearchResult<T> = Result<T, RebuildResearchError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstalledContract {
    pub contract: AgentContract,
    pub artifact: Artifact,
}

#[derive(Debug, Clone, Default)]
pub struct RebuildContractCatalogue {
    by_hash: BTreeMap<akzio_domain::ContentHash, InstalledContract>,
    by_identity: BTreeMap<(akzio_domain::ContractId, u32), akzio_domain::ContentHash>,
}

impl RebuildContractCatalogue {
    pub fn install(
        store: &V2Store,
        contracts: impl IntoIterator<Item = AgentContract>,
        now: DateTime<Utc>,
    ) -> RebuildResearchResult<Self> {
        let mut by_hash = BTreeMap::new();
        let mut by_identity = BTreeMap::new();
        for contract in contracts {
            contract.validate()?;
            if by_hash.contains_key(&contract.contract_hash) {
                return Err(RebuildResearchError::DuplicateContract(
                    contract.contract_hash.clone(),
                ));
            }
            let identity = (contract.contract_id.clone(), contract.version);
            let contract_hash = contract.contract_hash.clone();
            if by_identity.contains_key(&identity) {
                return Err(RebuildResearchError::DuplicateContractVersion {
                    contract_id: contract.contract_id.clone(),
                    version: contract.version,
                });
            }
            let artifact = Artifact::new(
                ArtifactKind::Contract,
                store.put_json(&contract)?,
                "research.contract_catalogue",
                ArtifactLifecycle::Canonical,
                ArtifactProvenance {
                    source_family: "akzio.contract_catalogue".to_owned(),
                    observed_at: None,
                    retrieved_at: now,
                    source_uri: None,
                    confidence_ppm: 1_000_000,
                    producer_contract_hash: None,
                },
                None,
                vec![],
                now,
            )?;
            store.write_bootstrap_artifact(&artifact)?;
            by_hash.insert(
                contract_hash.clone(),
                InstalledContract { contract, artifact },
            );
            by_identity.insert(identity, contract_hash);
        }
        Ok(Self {
            by_hash,
            by_identity,
        })
    }

    pub fn get(
        &self,
        hash: &akzio_domain::ContentHash,
    ) -> RebuildResearchResult<&InstalledContract> {
        self.by_hash
            .get(hash)
            .ok_or_else(|| RebuildResearchError::UnknownContract(hash.clone()))
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

    /// Candidate contracts are data for later shadow evaluation. This gate
    /// proves they cannot request a wider source or tool surface than the
    /// installed active contract that sponsors them.
    pub fn validate_candidate(
        &self,
        active_hash: &akzio_domain::ContentHash,
        candidate: &AgentContract,
    ) -> RebuildResearchResult<()> {
        candidate.validate()?;
        let active = self.get(active_hash)?;
        if active.contract.permits_candidate(candidate) {
            Ok(())
        } else {
            Err(RebuildResearchError::CandidateCapabilityExpansion {
                active: active_hash.clone(),
                candidate: candidate.contract_hash.clone(),
            })
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentToolCall {
    pub call_id: String,
    pub name: String,
    pub artifact_id: ArtifactId,
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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentModelTurn {
    pub output: Option<Value>,
    pub tool_calls: Vec<AgentToolCall>,
}

#[derive(Debug, Clone, PartialEq)]
struct ToolResult {
    value: Value,
    artifact: Artifact,
}

struct TurnRecord<'a> {
    permit: &'a TaskWritePermit,
    contract: &'a AgentContract,
    manifest: &'a RebuildContextManifest,
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
    ) -> BoxFuture<'a, RebuildResearchResult<AgentModelTurn>>;
}

#[derive(Debug, Clone)]
pub struct ModelClientAdapter {
    client: ModelClient,
}

impl ModelClientAdapter {
    pub fn new(client: ModelClient) -> Self {
        Self { client }
    }
}

impl AgentModel for ModelClientAdapter {
    fn turn<'a>(
        &'a self,
        request: AgentModelRequest,
    ) -> BoxFuture<'a, RebuildResearchResult<AgentModelTurn>> {
        Box::pin(async move {
            let response = self
                .client
                .respond(ModelRequest {
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
                        })
                        .collect(),
                })
                .await
                .map_err(model_client_error)?;
            let output = (!response.output_text.trim().is_empty())
                .then(|| serde_json::from_str(&response.output_text))
                .transpose()
                .map_err(|error| {
                    RebuildResearchError::InvalidOutput(format!("model output JSON: {error}"))
                })?;
            let tool_calls = response
                .tool_calls
                .into_iter()
                .map(|call| {
                    let artifact_id = call
                        .arguments
                        .get("artifact_id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            RebuildResearchError::InvalidOutput(format!(
                                "tool {} omitted artifact_id",
                                call.name
                            ))
                        })?;
                    Ok(AgentToolCall {
                        call_id: call.call_id,
                        name: call.name,
                        artifact_id: ArtifactId(akzio_domain::ContentHash::new(artifact_id)?),
                    })
                })
                .collect::<RebuildResearchResult<Vec<_>>>()?;
            Ok(AgentModelTurn { output, tool_calls })
        })
    }
}

#[derive(Debug, Clone)]
pub struct RebuildAgentRuntime {
    store: V2Store,
    context: RebuildContextBroker,
    catalogue: RebuildContractCatalogue,
    grant_ttl: Duration,
}

impl RebuildAgentRuntime {
    pub fn new(store: V2Store, catalogue: RebuildContractCatalogue, grant_ttl: Duration) -> Self {
        Self {
            context: RebuildContextBroker::new(store.clone()),
            store,
            catalogue,
            grant_ttl,
        }
    }

    pub fn catalogue(&self) -> &RebuildContractCatalogue {
        &self.catalogue
    }

    pub async fn run(
        &self,
        permit: &TaskWritePermit,
        node: &WorkflowNode,
        candidates: impl IntoIterator<Item = ArtifactRef>,
        model: &dyn AgentModel,
        now: DateTime<Utc>,
    ) -> RebuildResearchResult<Artifact> {
        let contract_hash = node
            .contract_hash
            .as_ref()
            .ok_or(RebuildResearchError::MissingContractHash)?;
        if permit.contract_hash.as_ref() != Some(contract_hash) {
            return Err(RebuildResearchError::ContractMismatch);
        }
        let installed = self.catalogue.get(contract_hash)?;
        if node.budget != installed.contract.budget
            || node.retry != installed.contract.retry
            || node.on_failure != installed.contract.on_failure
        {
            return Err(RebuildResearchError::NodePolicyMismatch);
        }
        let manifest =
            self.context
                .assemble(permit, &installed.contract, candidates, now, self.grant_ttl)?;
        if !manifest.grant.matches_permit(permit) {
            return Err(RebuildResearchError::GrantPermitMismatch);
        }
        let context = self.context_values(permit, &manifest, now)?;
        let prompt = String::from_utf8(self.store.read_blob(&installed.contract.prompt)?)
            .map_err(|_| RebuildResearchError::InvalidOutput("prompt is not UTF-8".to_owned()))?;
        let output_schema: Value =
            serde_json::from_slice(&self.store.read_blob(&installed.contract.output.schema)?)?;
        let tools = model_tool_definitions(&installed.contract);
        let mut tool_results = Vec::new();
        let mut trace_refs = Vec::new();
        let mut tool_calls = 0_u16;
        let mut model_turn = 0_u16;
        let started = Instant::now();
        let wall_time =
            StdDuration::from_secs(u64::from(installed.contract.budget.max_wall_time_secs));
        loop {
            if started.elapsed() > wall_time {
                return Err(RebuildResearchError::WallTimeExceeded {
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
                return Err(RebuildResearchError::InputBudgetExceeded {
                    actual: input_tokens,
                    maximum: installed.contract.budget.max_input_tokens,
                });
            }
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
                            model_error_class(&error),
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
                            return Err(RebuildResearchError::WallTimeExceeded {
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
                    "wall_time",
                    false,
                )?;
                trace_refs.push(ArtifactRef {
                    artifact_id: failed_turn.artifact_id,
                    kind: ArtifactKind::AgentTurn,
                });
                return Err(RebuildResearchError::WallTimeExceeded {
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
                &turn,
            )?;
            trace_refs.push(ArtifactRef {
                artifact_id: turn_artifact.artifact_id,
                kind: ArtifactKind::AgentTurn,
            });
            if !turn.tool_calls.is_empty() {
                let next = tool_calls.saturating_add(turn.tool_calls.len() as u16);
                if next > installed.contract.budget.max_tool_calls {
                    return Err(RebuildResearchError::ToolBudgetExceeded);
                }
                for call in turn.tool_calls {
                    let tool_result = self.execute_tool(
                        permit,
                        &installed.contract,
                        &manifest.grant,
                        &call,
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
            let output = turn
                .output
                .ok_or(RebuildResearchError::MissingFinalOutput)?;
            let output_tokens = estimate_tokens(&output)?;
            if output_tokens > installed.contract.budget.max_output_tokens {
                return Err(RebuildResearchError::OutputBudgetExceeded {
                    actual: output_tokens,
                    maximum: installed.contract.budget.max_output_tokens,
                });
            }
            validate_output_schema(&self.store, &installed.contract, &output)?;
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
                .collect(),
                turn_now,
            )?;
            return Ok(output_artifact);
        }
    }

    fn context_values(
        &self,
        permit: &TaskWritePermit,
        manifest: &RebuildContextManifest,
        now: DateTime<Utc>,
    ) -> RebuildResearchResult<Vec<Value>> {
        if !manifest.grant.matches_permit(permit) {
            return Err(RebuildResearchError::GrantPermitMismatch);
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
        response: &AgentModelTurn,
    ) -> RebuildResearchResult<Artifact> {
        let artifact = Artifact::new(
            ArtifactKind::AgentTurn,
            self.store.put_json(&json!({
                "turn": record.turn,
                "attempt": record.attempt,
                "contract_hash": record.contract.contract_hash,
                "context_manifest": record.manifest.artifact.artifact_id,
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
        error_class: &str,
        will_retry: bool,
    ) -> RebuildResearchResult<Artifact> {
        let artifact = Artifact::new(
            ArtifactKind::AgentTurn,
            self.store.put_json(&json!({
                "turn": record.turn,
                "attempt": record.attempt,
                "contract_hash": record.contract.contract_hash,
                "context_manifest": record.manifest.artifact.artifact_id,
                "error_class": error_class,
                "will_retry": will_retry,
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
        now: DateTime<Utc>,
    ) -> RebuildResearchResult<ToolResult> {
        if !grant.matches_permit(permit) {
            return Err(RebuildResearchError::GrantPermitMismatch);
        }
        let tool = match call.name.as_str() {
            "read_artifact" => akzio_domain::ToolKind::ReadEvidence,
            "read_raw_evidence" => akzio_domain::ToolKind::ReadRawEvidence,
            _ => return Err(RebuildResearchError::ToolNotGranted(call.name.clone())),
        };
        if !contract.tool_grants.iter().any(|grant| grant.kind == tool) {
            return Err(RebuildResearchError::ToolNotGranted(call.name.clone()));
        }
        let raw = tool == akzio_domain::ToolKind::ReadRawEvidence;
        let artifact = if raw {
            self.context.read_raw(grant, &call.artifact_id, now)?
        } else {
            self.context.read(grant, &call.artifact_id, now)?
        };
        if !contract
            .tool_grants
            .iter()
            .filter(|tool_grant| tool_grant.kind == tool)
            .any(|tool_grant| {
                tool_grant.allowed_sources.is_empty()
                    || tool_grant
                        .allowed_sources
                        .iter()
                        .any(|source| source == &artifact.provenance.source_family)
            })
        {
            return Err(RebuildResearchError::ToolSourceNotGranted {
                tool: call.name.clone(),
                source_family: artifact.provenance.source_family.clone(),
            });
        }
        let bytes = self.store.read_blob(&artifact.blob)?;
        let value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
        let call_artifact = Artifact::new(
            ArtifactKind::ToolCall,
            self.store.put_json(call)?,
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
        let result_artifact = Artifact::new(
            ArtifactKind::ToolResult,
            self.store.put_json(&value)?,
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
                "value": value,
            }),
            artifact: result_artifact,
        })
    }
}

fn estimate_tokens<T: Serialize>(value: &T) -> RebuildResearchResult<u32> {
    let bytes = serde_json::to_vec(value)?.len() as u64;
    Ok(u32::try_from(bytes.div_ceil(4).max(1)).unwrap_or(u32::MAX))
}

fn model_tool_definitions(contract: &AgentContract) -> Vec<AgentToolDefinition> {
    contract
        .tool_grants
        .iter()
        .filter_map(|grant| match grant.kind {
            akzio_domain::ToolKind::ReadEvidence => Some((
                "read_artifact",
                "Read one artifact explicitly granted by the ContextManifest.",
            )),
            akzio_domain::ToolKind::ReadRawEvidence => Some((
                "read_raw_evidence",
                "Read one explicitly granted raw evidence artifact.",
            )),
            _ => None,
        })
        .map(|(name, description)| AgentToolDefinition {
            name: name.to_owned(),
            description: description.to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {"artifact_id": {"type": "string"}},
                "required": ["artifact_id"],
                "additionalProperties": false,
            }),
        })
        .collect()
}

fn model_client_error(error: ModelError) -> RebuildResearchError {
    match error {
        ModelError::Transport(_) => RebuildResearchError::Model("transport".to_owned()),
        ModelError::Http { status, .. } if status.as_u16() == 429 => {
            RebuildResearchError::RateLimited("HTTP 429".to_owned())
        }
        ModelError::Http { status, .. } => {
            RebuildResearchError::Model(format!("HTTP {}", status.as_u16()))
        }
        ModelError::EmptyBaseUrl => RebuildResearchError::Model("invalid base URL".to_owned()),
        ModelError::MissingOutput => RebuildResearchError::Model("missing model output".to_owned()),
        ModelError::FixtureExhausted => {
            RebuildResearchError::Model("fixture sequence exhausted".to_owned())
        }
        ModelError::MissingEnvironment(_) => {
            RebuildResearchError::Model("model configuration missing".to_owned())
        }
    }
}

fn logical_now(start: DateTime<Utc>, elapsed: StdDuration) -> DateTime<Utc> {
    start + Duration::from_std(elapsed).unwrap_or_else(|_| Duration::seconds(i64::MAX))
}

fn retryable_model_error(error: &RebuildResearchError, retry: &akzio_domain::RetryPolicy) -> bool {
    matches!(
        (error, retry),
        (
            RebuildResearchError::Model(_),
            akzio_domain::RetryPolicy {
                retry_transport: true,
                ..
            }
        ) | (
            RebuildResearchError::RateLimited(_),
            akzio_domain::RetryPolicy {
                retry_rate_limited: true,
                ..
            }
        )
    )
}

fn model_error_class(error: &RebuildResearchError) -> &'static str {
    match error {
        RebuildResearchError::Model(_) => "transport",
        RebuildResearchError::RateLimited(_) => "rate_limited",
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
) -> RebuildResearchResult<()> {
    let schema: Value = serde_json::from_slice(&store.read_blob(&contract.output.schema)?)?;
    validate_schema_value(output, &schema, "$").map_err(RebuildResearchError::InvalidOutput)?;
    if schema.get("type").and_then(Value::as_str) != Some("object") || !output.is_object() {
        return Err(RebuildResearchError::InvalidOutput(
            "schema and output must both be JSON objects".to_owned(),
        ));
    }
    let required = schema
        .get("required")
        .and_then(Value::as_array)
        .ok_or_else(|| RebuildResearchError::InvalidOutput("schema.required missing".to_owned()))?;
    for field in required {
        let Some(field) = field.as_str() else {
            return Err(RebuildResearchError::InvalidOutput(
                "schema.required must contain strings".to_owned(),
            ));
        };
        if output.get(field).is_none() {
            return Err(RebuildResearchError::InvalidOutput(format!(
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
            "type" | "enum" | "properties" | "required" | "additionalProperties" | "items"
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
        .and_then(Value::as_bool)
        .unwrap_or(false);
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
            None if additional_properties => {}
            None => return Err(format!("{path}.{name} is not allowed")),
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
        OutputContract, RetryPolicy, TaskBudget, TaskRecipeId, TaskStatus, TerminationPolicy,
        ToolGrant, ToolKind, WorkflowGraph, WorkflowNode, REBUILD_SCHEMA_VERSION,
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
        ) -> BoxFuture<'a, RebuildResearchResult<AgentModelTurn>> {
            Box::pin(async move {
                match self.calls.fetch_add(1, Ordering::SeqCst) {
                    0 => Err(RebuildResearchError::Model(
                        "transient fixture failure".to_owned(),
                    )),
                    1 => {
                        assert!(request.prior_tool_results.is_empty());
                        Ok(AgentModelTurn {
                            output: None,
                            tool_calls: vec![AgentToolCall {
                                call_id: "fixture-read-evidence".to_owned(),
                                name: "read_artifact".to_owned(),
                                artifact_id: self.evidence_id.clone(),
                            }],
                        })
                    }
                    2 => {
                        assert_eq!(request.prior_tool_results.len(), 1);
                        assert_eq!(
                            request.prior_tool_results[0]["value"],
                            json!({"price": 100})
                        );
                        Ok(AgentModelTurn {
                            output: Some(json!({"summary":"source-linked claim"})),
                            tool_calls: vec![],
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
        ) -> BoxFuture<'a, RebuildResearchResult<AgentModelTurn>> {
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
        ) -> BoxFuture<'a, RebuildResearchResult<AgentModelTurn>> {
            let evidence_id = self.evidence_id.clone();
            Box::pin(async move {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                Ok(AgentModelTurn {
                    output: None,
                    tool_calls: vec![AgentToolCall {
                        call_id: "fixture-expired-grant".to_owned(),
                        name: "read_artifact".to_owned(),
                        artifact_id: evidence_id,
                    }],
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
        ) -> BoxFuture<'a, RebuildResearchResult<AgentModelTurn>> {
            Box::pin(async move {
                tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
                Ok(AgentModelTurn {
                    output: Some(json!({"summary":"too late"})),
                    tool_calls: vec![],
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
            store.put_bytes(b"prompt", "text/plain").unwrap(),
            ContextPolicy {
                permitted_kinds: BTreeSet::from([ArtifactKind::NormalizedEvidence]),
                permitted_source_families: BTreeSet::from(["market".to_owned()]),
                max_artifacts: 4,
                max_bytes: 4096,
                max_tokens: 1024,
                allow_raw_reread: false,
            },
            vec![ToolGrant {
                kind: ToolKind::ReadEvidence,
                allowed_sources: vec!["market".to_owned()],
            }],
            OutputContract {
                artifact_kind: ArtifactKind::Claim,
                schema: store
                    .put_bytes(
                        br#"{"type":"object","properties":{"summary":{"type":"string"}},"required":["summary"],"additionalProperties":false}"#,
                        "application/json",
                    )
                    .unwrap(),
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
        catalogue: RebuildContractCatalogue,
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
        let catalogue =
            RebuildContractCatalogue::install(&store, [contract.clone()], Utc::now()).unwrap();
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
            schema_version: REBUILD_SCHEMA_VERSION,
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
    fn contract_catalogue_rejects_duplicate_hash_and_identity_version() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let contract = contract(&store);

        let catalogue =
            RebuildContractCatalogue::install(&store, [contract.clone()], Utc::now()).unwrap();
        assert_eq!(
            catalogue.contract_hash_for(&contract.contract_id, contract.version),
            Some(&contract.contract_hash)
        );

        assert!(matches!(
            RebuildContractCatalogue::install(
                &store,
                [contract.clone(), contract.clone()],
                Utc::now(),
            ),
            Err(RebuildResearchError::DuplicateContract(_))
        ));

        let mut changed = contract.clone();
        changed.responsibility = "different responsibility".to_owned();
        changed.contract_hash = changed.expected_hash().unwrap();
        changed.validate().unwrap();
        assert!(matches!(
            RebuildContractCatalogue::install(&store, [contract, changed], Utc::now()),
            Err(RebuildResearchError::DuplicateContractVersion { .. })
        ));
    }

    #[test]
    fn contract_catalogue_rejects_candidate_capability_expansion() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let active = contract(&store);
        let catalogue =
            RebuildContractCatalogue::install(&store, [active.clone()], Utc::now()).unwrap();

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
            Err(RebuildResearchError::CandidateCapabilityExpansion { .. })
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
    async fn model_client_adapter_decodes_only_the_contract_tool_shape() {
        let artifact_hash = akzio_domain::ContentHash::of_bytes(b"fixture-artifact");
        let adapter = ModelClientAdapter::new(akzio_model::ModelClient::Fixture(json!({
            "output_text": "",
            "tool_calls": [{
                "call_id": "fixture-tool",
                "name": "read_artifact",
                "arguments": {"artifact_id": artifact_hash.as_str()},
            }],
        })));
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
                }],
            })
            .await
            .unwrap();

        assert!(response.output.is_none());
        assert_eq!(response.tool_calls.len(), 1);
        assert_eq!(response.tool_calls[0].name, "read_artifact");
        assert_eq!(response.tool_calls[0].artifact_id.0, artifact_hash);
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
        let runtime = RebuildAgentRuntime::new(store.clone(), catalogue, Duration::milliseconds(1));
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
            Err(RebuildResearchError::Context(
                RebuildContextError::GrantDenied { .. }
            ))
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
        let runtime = RebuildAgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));
        let model = FixedModel(AgentModelTurn {
            output: None,
            tool_calls: vec![AgentToolCall {
                call_id: "fixture-news-denied".to_owned(),
                name: "read_artifact".to_owned(),
                artifact_id: news.artifact_id.clone(),
            }],
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
            Err(RebuildResearchError::ToolSourceNotGranted { .. })
        ));
        store.verify_integrity().unwrap();
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
        let runtime = RebuildAgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));

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
            Err(RebuildResearchError::WallTimeExceeded { maximum_secs: 1 })
        ));
        store.verify_integrity().unwrap();
    }

    #[tokio::test]
    async fn agent_runtime_records_complete_tool_trace_and_contract_validated_claim() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let contract = contract(&store);
        let catalogue =
            RebuildContractCatalogue::install(&store, [contract.clone()], Utc::now()).unwrap();
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
            schema_version: REBUILD_SCHEMA_VERSION,
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
        let runtime = RebuildAgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));
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

        let malformed = FixedModel(AgentModelTurn {
            output: Some(json!({"summary": 42})),
            tool_calls: vec![],
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
            Err(RebuildResearchError::InvalidOutput(_))
        ));

        let denied_tool = FixedModel(AgentModelTurn {
            output: None,
            tool_calls: vec![AgentToolCall {
                call_id: "fixture-denied-raw".to_owned(),
                name: "read_raw_evidence".to_owned(),
                artifact_id: evidence.artifact_id.clone(),
            }],
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
            Err(RebuildResearchError::ToolNotGranted(_))
        ));

        let over_budget_tools = FixedModel(AgentModelTurn {
            output: None,
            tool_calls: (0..3)
                .map(|index| AgentToolCall {
                    call_id: format!("fixture-over-budget-{index}"),
                    name: "read_artifact".to_owned(),
                    artifact_id: evidence.artifact_id.clone(),
                })
                .collect(),
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
            Err(RebuildResearchError::ToolBudgetExceeded)
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
            Err(RebuildResearchError::NodePolicyMismatch)
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
        let runtime = RebuildAgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));
        let output = FixedModel(AgentModelTurn {
            output: Some(json!({"summary":"source-linked claim"})),
            tool_calls: vec![],
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
            matches!(
                &result,
                Err(RebuildResearchError::InputBudgetExceeded { .. })
            ),
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
        let runtime = RebuildAgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));
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
            Err(RebuildResearchError::OutputBudgetExceeded { .. })
        ));
        store.verify_integrity().unwrap();
    }
}
