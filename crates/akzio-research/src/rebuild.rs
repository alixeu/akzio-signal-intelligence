//! Contract-driven Agent runtime for the rebuilt v2 system.

use std::{collections::BTreeMap, sync::Arc};

use akzio_context::{RebuildContextBroker, RebuildContextError, RebuildContextManifest};
use akzio_domain::{
    Artifact, ArtifactId, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance,
    ArtifactRef, ContextGrant, ContractSpec, DomainError, TaskWritePermit, WorkflowNode,
};
use akzio_store::{RebuildStore, RebuildStoreError};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RebuildResearchError {
    #[error(transparent)]
    Context(#[from] RebuildContextError),
    #[error(transparent)]
    Store(#[from] RebuildStoreError),
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
    #[error("Agent output did not satisfy Contract schema: {0}")]
    InvalidOutput(String),
    #[error("Agent model failed: {0}")]
    Model(String),
    #[error("tool {0} is not granted by the Agent Contract")]
    ToolNotGranted(String),
    #[error("Agent exceeded its Contract tool-call budget")]
    ToolBudgetExceeded,
    #[error("Agent completed without a final output")]
    MissingFinalOutput,
}

pub type RebuildResearchResult<T> = Result<T, RebuildResearchError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstalledContract {
    pub contract: ContractSpec,
    pub artifact: Artifact,
}

#[derive(Debug, Clone, Default)]
pub struct RebuildContractCatalogue {
    by_hash: BTreeMap<akzio_domain::ContentHash, InstalledContract>,
}

impl RebuildContractCatalogue {
    pub fn install(
        store: &RebuildStore,
        contracts: impl IntoIterator<Item = ContractSpec>,
        now: DateTime<Utc>,
    ) -> RebuildResearchResult<Self> {
        let mut by_hash = BTreeMap::new();
        for contract in contracts {
            contract.validate()?;
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
                contract.contract_hash.clone(),
                InstalledContract { contract, artifact },
            );
        }
        Ok(Self { by_hash })
    }

    pub fn get(&self, hash: &akzio_domain::ContentHash) -> RebuildResearchResult<&InstalledContract> {
        self.by_hash
            .get(hash)
            .ok_or_else(|| RebuildResearchError::UnknownContract(hash.clone()))
    }

    pub fn contracts(&self) -> impl Iterator<Item = &InstalledContract> {
        self.by_hash.values()
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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentModelTurn {
    pub output: Option<Value>,
    pub tool_calls: Vec<AgentToolCall>,
}

/// Deliberately tiny seam. The production `akzio-model` adapter and fixture tests
/// both implement this; no execution/policy authority crosses it.
pub trait AgentModel: Send + Sync {
    fn turn(&self, request: AgentModelRequest) -> RebuildResearchResult<AgentModelTurn>;
}

#[derive(Debug, Clone)]
pub struct RebuildAgentRuntime {
    store: RebuildStore,
    context: RebuildContextBroker,
    catalogue: RebuildContractCatalogue,
    grant_ttl: Duration,
}

impl RebuildAgentRuntime {
    pub fn new(
        store: RebuildStore,
        catalogue: RebuildContractCatalogue,
        grant_ttl: Duration,
    ) -> Self {
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

    pub fn run(
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
        let manifest = self.context.assemble(
            permit,
            &installed.contract,
            candidates,
            now,
            self.grant_ttl,
        )?;
        let context = self.context_values(&manifest)?;
        let prompt = String::from_utf8(self.store.read_blob(&installed.contract.prompt)?)
            .map_err(|_| RebuildResearchError::InvalidOutput("prompt is not UTF-8".to_owned()))?;
        let mut tool_results = Vec::new();
        let mut turns = 0_u16;
        loop {
            let turn = model.turn(AgentModelRequest {
                contract_hash: installed.contract.contract_hash.clone(),
                purpose: installed.contract.purpose.as_str().to_owned(),
                prompt: prompt.clone(),
                objective: node.objective.clone(),
                manifest_artifact_id: manifest.artifact.artifact_id.clone(),
                context: context.clone(),
                prior_tool_results: tool_results.clone(),
            })?;
            self.record_turn(permit, &installed.contract, &manifest, turns, &turn, now)?;
            if !turn.tool_calls.is_empty() {
                let next = turns.saturating_add(turn.tool_calls.len() as u16);
                if next > u16::from(installed.contract.budget.max_tool_calls) {
                    return Err(RebuildResearchError::ToolBudgetExceeded);
                }
                for call in turn.tool_calls {
                    tool_results.push(self.execute_tool(permit, &installed.contract, &manifest.grant, &call, now)?);
                }
                turns = next;
                continue;
            }
            let output = turn.output.ok_or(RebuildResearchError::MissingFinalOutput)?;
            validate_output_schema(&self.store, &installed.contract, &output)?;
            let output_artifact = Artifact::new(
                installed.contract.output.artifact_kind,
                self.store.put_json(&output)?,
                format!("agent.{}", installed.contract.purpose.as_str()),
                ArtifactLifecycle::RunScoped,
                ArtifactProvenance {
                    source_family: "akzio.agent".to_owned(),
                    observed_at: None,
                    retrieved_at: now,
                    source_uri: None,
                    confidence_ppm: 1_000_000,
                    producer_contract_hash: Some(installed.contract.contract_hash.clone()),
                },
                Some(task_origin(permit)),
                vec![ArtifactRef {
                    artifact_id: manifest.artifact.artifact_id.clone(),
                    kind: ArtifactKind::ContextManifest,
                }],
                now,
            )?;
            self.store
                .write_task_artifact(permit, &output_artifact, "agent.output_created", now)?;
            return Ok(output_artifact);
        }
    }

    fn context_values(&self, manifest: &RebuildContextManifest) -> RebuildResearchResult<Vec<Value>> {
        manifest
            .payload
            .selections
            .iter()
            .map(|selection| {
                let artifact = self
                    .context
                    .read(&manifest.grant, &selection.artifact.artifact_id, Utc::now())?;
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
        permit: &TaskWritePermit,
        contract: &ContractSpec,
        manifest: &RebuildContextManifest,
        turn: u16,
        response: &AgentModelTurn,
        now: DateTime<Utc>,
    ) -> RebuildResearchResult<()> {
        let artifact = Artifact::new(
            ArtifactKind::AgentTurn,
            self.store.put_json(&json!({
                "turn": turn,
                "contract_hash": contract.contract_hash,
                "context_manifest": manifest.artifact.artifact_id,
                "response": response,
            }))?,
            format!("agent.turn.{}", contract.purpose.as_str()),
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.agent".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: Some(contract.contract_hash.clone()),
            },
            Some(task_origin(permit)),
            vec![ArtifactRef {
                artifact_id: manifest.artifact.artifact_id.clone(),
                kind: ArtifactKind::ContextManifest,
            }],
            now,
        )?;
        self.store
            .write_task_artifact(permit, &artifact, "agent.turn_completed", now)?;
        Ok(())
    }

    fn execute_tool(
        &self,
        permit: &TaskWritePermit,
        contract: &ContractSpec,
        grant: &ContextGrant,
        call: &AgentToolCall,
        now: DateTime<Utc>,
    ) -> RebuildResearchResult<Value> {
        let tool = match call.name.as_str() {
            "read_artifact" => akzio_domain::ToolKind::ReadEvidence,
            "read_raw_evidence" => akzio_domain::ToolKind::ReadRawEvidence,
            "read_market_data" => akzio_domain::ToolKind::ReadMarketData,
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
        Ok(json!({
            "call_id": call.call_id,
            "artifact_id": artifact.artifact_id,
            "kind": artifact.kind,
            "value": value,
        }))
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
    store: &RebuildStore,
    contract: &ContractSpec,
    output: &Value,
) -> RebuildResearchResult<()> {
    let schema: Value = serde_json::from_slice(&store.read_blob(&contract.output.schema)?)?;
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use akzio_domain::{
        ArtifactLifecycle, ContractId, ContractPurpose, ContextPolicy, FailureDisposition,
        OutputContract, RetryPolicy, TaskBudget, TaskRecipeId, TerminationPolicy,
        ToolGrant, ToolKind, WorkflowGraph, WorkflowNode, REBUILD_SCHEMA_VERSION,
    };
    use akzio_store::{RebuildRun, WorkflowCommit};
    use tempfile::tempdir;

    use super::*;

    #[derive(Debug)]
    struct FixtureModel;

    impl AgentModel for FixtureModel {
        fn turn(&self, _: AgentModelRequest) -> RebuildResearchResult<AgentModelTurn> {
            Ok(AgentModelTurn {
                output: Some(json!({"summary":"source-linked claim"})),
                tool_calls: vec![],
            })
        }
    }

    fn contract(store: &RebuildStore) -> ContractSpec {
        ContractSpec::new(
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
                    .put_bytes(br#"{"type":"object","required":["summary"]}"#, "application/json")
                    .unwrap(),
            },
            TaskBudget {
                max_input_tokens: 1024,
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

    #[test]
    fn agent_runtime_records_manifest_turn_and_contract_validated_claim() {
        let root = tempdir().unwrap();
        let store = RebuildStore::open(root.path()).unwrap();
        let contract = contract(&store);
        let catalogue = RebuildContractCatalogue::install(&store, [contract.clone()], Utc::now()).unwrap();
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
        let run = RebuildRun {
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
            store.put_bytes(br#"{"price":100}"#, "application/json").unwrap(),
            "fixture",
            ArtifactLifecycle::RunScoped,
            provenance(),
            Some(task_origin(&claimed.permit)),
            vec![],
            Utc::now(),
        )
        .unwrap();
        store
            .write_task_artifact(&claimed.permit, &evidence, "evidence.normalized", Utc::now())
            .unwrap();
        let runtime = RebuildAgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));
        let output = runtime
            .run(
                &claimed.permit,
                &claimed.node,
                [ArtifactRef {
                    artifact_id: evidence.artifact_id,
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &FixtureModel,
                Utc::now(),
            )
            .unwrap();
        assert_eq!(output.kind, ArtifactKind::Claim);
        assert!(output.source_refs.iter().any(|source| source.kind == ArtifactKind::ContextManifest));
        store.verify_integrity().unwrap();
    }
}
