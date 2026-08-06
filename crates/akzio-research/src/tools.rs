//! Rust-owned, provenance-preserving tools available to contract-bound agents.
//!
//! Tools never accept a URL or a filesystem path from the model. A model can
//! only reread a durable document whose source belongs to the named registry.

use std::collections::{BTreeMap, BTreeSet};

use akzio_context::{ContextBroker, NewJsonDocument};
use akzio_domain::{
    AgentContract, AttemptId, ContentHash, DocumentId, DocumentKind, DocumentLifecycle,
    DocumentOrigin, DocumentRecord, EventEnvelope, RunId, TaskId, ToolGrant, ToolKind,
    V2_SCHEMA_VERSION,
};
use akzio_model::{ModelToolCall, ModelToolDefinition};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

const MAX_TOOL_DOCUMENT_BYTES: u64 = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRule {
    pub name: String,
    pub prefixes: Vec<String>,
    pub allow_raw: bool,
}

/// Fixed, Rust-configured source catalog. A source name in an agent Contract
/// is a registry capability, never an untrusted model-provided location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRegistry {
    rules: BTreeMap<String, SourceRule>,
}

impl SourceRegistry {
    pub fn market_only() -> Self {
        Self::new([
            SourceRule {
                name: "market".to_owned(),
                prefixes: vec!["alpaca.".to_owned()],
                allow_raw: true,
            },
            SourceRule {
                name: "internal".to_owned(),
                prefixes: vec!["akzio.".to_owned(), "memory.".to_owned()],
                allow_raw: false,
            },
        ])
    }

    pub fn new(rules: impl IntoIterator<Item = SourceRule>) -> Self {
        Self {
            rules: rules
                .into_iter()
                .map(|rule| (rule.name.clone(), rule))
                .collect(),
        }
    }

    fn permits(&self, grant: &ToolGrant, source: &str, needs_raw: bool) -> bool {
        let names: BTreeSet<_> = if grant.allowed_sources.is_empty() {
            self.rules.keys().cloned().collect()
        } else {
            grant.allowed_sources.iter().cloned().collect()
        };
        names.into_iter().any(|name| {
            self.rules.get(&name).is_some_and(|rule| {
                (!needs_raw || rule.allow_raw)
                    && rule
                        .prefixes
                        .iter()
                        .any(|prefix| source.starts_with(prefix))
            })
        })
    }
}

impl Default for SourceRegistry {
    fn default() -> Self {
        Self::market_only()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCallContext {
    pub run_id: RunId,
    pub task_id: TaskId,
    pub attempt_id: AttemptId,
    pub contract_hash: ContentHash,
    pub context_manifest_id: Option<DocumentId>,
}

#[derive(Debug, Clone)]
pub struct ToolExecution {
    pub result_document: DocumentRecord,
    pub model_result: Value,
}

#[derive(Debug, Error)]
pub enum ToolError {
    #[error(transparent)]
    Context(#[from] akzio_context::ContextError),
    #[error(transparent)]
    Store(#[from] akzio_store::StoreError),
    #[error("unknown tool {0}")]
    UnknownTool(String),
    #[error("contract does not grant {0:?}")]
    NotGranted(ToolKind),
    #[error("tool argument document_id is required")]
    MissingDocumentId,
    #[error("tool argument document_id is invalid")]
    InvalidDocumentId,
    #[error("{tool:?} cannot read document kind {kind:?}")]
    ForbiddenKind { tool: ToolKind, kind: DocumentKind },
    #[error("source {source_name:?} is not permitted by the contract")]
    ForbiddenSource { source_name: String },
    #[error("document exceeds controlled reread limit")]
    DocumentTooLarge,
}

pub type Result<T> = std::result::Result<T, ToolError>;

#[derive(Debug, Clone)]
pub struct ToolRuntime {
    broker: ContextBroker,
    sources: SourceRegistry,
}

impl ToolRuntime {
    pub fn new(broker: ContextBroker, sources: SourceRegistry) -> Self {
        Self { broker, sources }
    }

    pub fn definitions(&self, contract: &AgentContract) -> Vec<ModelToolDefinition> {
        let granted = contract
            .tool_grants
            .iter()
            .map(|grant| grant.kind)
            .collect::<BTreeSet<_>>();
        [
            (
                ToolKind::ReadEvidence,
                "read_evidence",
                "Read one permitted normalized, semantic, claim, challenge, or memory document by durable ID.",
            ),
            (
                ToolKind::ReadRawEvidence,
                "read_raw_evidence",
                "Controlled reread of one permitted raw market evidence document by durable ID.",
            ),
            (
                ToolKind::ReadMarketData,
                "read_market_data",
                "Read one sealed Alpaca market or account document by durable ID.",
            ),
        ]
        .into_iter()
        .filter(|(kind, _, _)| granted.contains(kind))
        .map(|(_, name, description)| ModelToolDefinition {
            name: name.to_owned(),
            description: description.to_owned(),
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {"document_id": {"type": "string"}},
                "required": ["document_id"]
            }),
        })
        .collect()
    }

    /// Persist both sides of a tool call. Policy failures are tool results,
    /// allowing the model to recover without bypassing Rust authority.
    pub fn execute(
        &self,
        contract: &AgentContract,
        context: &ToolCallContext,
        call: &ModelToolCall,
        now: DateTime<Utc>,
    ) -> Result<ToolExecution> {
        let call_document = self.broker.record_json(NewJsonDocument {
            kind: DocumentKind::ToolCall,
            producer: contract.agent_kind.clone(),
            run_id: Some(context.run_id.clone()),
            lifecycle: DocumentLifecycle::RunScoped,
            source_refs: context.context_manifest_id.iter().cloned().collect(),
            origin: Some(DocumentOrigin::task(
                context.task_id.clone(),
                context.attempt_id.clone(),
                Some(context.contract_hash.clone()),
            )),
            value: &json!({
                "call_id": call.call_id,
                "name": call.name,
                "arguments": call.arguments,
            }),
            created_at: now,
        })?;
        self.broker.store().append_event(&EventEnvelope {
            schema_version: V2_SCHEMA_VERSION,
            run_id: context.run_id.clone(),
            task_id: Some(context.task_id.clone()),
            attempt_id: Some(context.attempt_id.clone()),
            contract_hash: Some(context.contract_hash.clone()),
            causation_id: Some(call.call_id.clone()),
            event_type: "tool.called".to_owned(),
            payload_document_id: Some(call_document.document_id.clone()),
            payload: Some(call_document.blob.clone()),
            created_at: now,
        })?;

        let result = self.read(contract, call);
        let (model_result, source_refs) = match result {
            Ok((value, document_id)) => (
                json!({"ok": true, "value": value}),
                vec![call_document.document_id, document_id],
            ),
            Err(error) => (
                json!({"ok": false, "error": error.to_string()}),
                vec![call_document.document_id],
            ),
        };
        let result_document = self.broker.record_json(NewJsonDocument {
            kind: DocumentKind::ToolResult,
            producer: "runtime.tool".to_owned(),
            run_id: Some(context.run_id.clone()),
            lifecycle: DocumentLifecycle::RunScoped,
            source_refs,
            origin: Some(DocumentOrigin::task(
                context.task_id.clone(),
                context.attempt_id.clone(),
                Some(context.contract_hash.clone()),
            )),
            value: &json!({
                "call_id": call.call_id,
                "name": call.name,
                "result": model_result,
            }),
            created_at: now,
        })?;
        self.broker.store().append_event(&EventEnvelope {
            schema_version: V2_SCHEMA_VERSION,
            run_id: context.run_id.clone(),
            task_id: Some(context.task_id.clone()),
            attempt_id: Some(context.attempt_id.clone()),
            contract_hash: Some(context.contract_hash.clone()),
            causation_id: Some(call.call_id.clone()),
            event_type: "tool.completed".to_owned(),
            payload_document_id: Some(result_document.document_id.clone()),
            payload: Some(result_document.blob.clone()),
            created_at: now,
        })?;
        Ok(ToolExecution {
            result_document,
            model_result,
        })
    }

    fn read(&self, contract: &AgentContract, call: &ModelToolCall) -> Result<(Value, DocumentId)> {
        let kind = tool_kind(&call.name)?;
        let grant = contract
            .tool_grants
            .iter()
            .find(|grant| grant.kind == kind)
            .ok_or(ToolError::NotGranted(kind))?;
        let document_id = call
            .arguments
            .get("document_id")
            .and_then(Value::as_str)
            .ok_or(ToolError::MissingDocumentId)
            .and_then(|id| {
                (!id.trim().is_empty())
                    .then(|| DocumentId(id.to_owned()))
                    .ok_or(ToolError::InvalidDocumentId)
            })?;
        let document = self.broker.store().read_document(&document_id)?;
        let needs_raw = kind == ToolKind::ReadRawEvidence;
        if !allowed_kind(kind, document.kind) {
            return Err(ToolError::ForbiddenKind {
                tool: kind,
                kind: document.kind,
            });
        }
        if document.blob.bytes > MAX_TOOL_DOCUMENT_BYTES {
            return Err(ToolError::DocumentTooLarge);
        }
        if !self
            .sources
            .permits(grant, &document.provenance.source, needs_raw)
        {
            return Err(ToolError::ForbiddenSource {
                source_name: document.provenance.source,
            });
        }
        let value = self.broker.read_json(&document)?;
        Ok((
            json!({
                "document_id": document.document_id,
                "kind": document.kind,
                "producer": document.producer,
                "source_refs": document.source_refs,
                "provenance": document.provenance,
                "value": value,
            }),
            document_id,
        ))
    }
}

fn tool_kind(name: &str) -> Result<ToolKind> {
    match name {
        "read_evidence" => Ok(ToolKind::ReadEvidence),
        "read_raw_evidence" => Ok(ToolKind::ReadRawEvidence),
        "read_market_data" => Ok(ToolKind::ReadMarketData),
        other => Err(ToolError::UnknownTool(other.to_owned())),
    }
}

fn allowed_kind(tool: ToolKind, kind: DocumentKind) -> bool {
    match tool {
        ToolKind::ReadEvidence => !matches!(kind, DocumentKind::RawEvidence),
        ToolKind::ReadRawEvidence => kind == DocumentKind::RawEvidence,
        ToolKind::ReadMarketData => matches!(
            kind,
            DocumentKind::RawEvidence | DocumentKind::NormalizedEvidence
        ),
        ToolKind::FetchWebEvidence => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use akzio_context::NewJsonDocument;
    use akzio_domain::{
        BlobRef, ContractId, FailureDisposition, RetryPolicy, TaskBudget, TerminationPolicy,
    };
    use akzio_store::V2Store;
    use tempfile::tempdir;

    fn contract() -> AgentContract {
        let hash = ContentHash::of_bytes(b"contract");
        AgentContract {
            schema_version: V2_SCHEMA_VERSION,
            contract_id: ContractId::new(),
            version: 1,
            agent_kind: "test.agent".to_owned(),
            responsibility: "test".to_owned(),
            prompt: BlobRef {
                hash: hash.clone(),
                media_type: "text/plain".to_owned(),
                bytes: 1,
            },
            input_context_kinds: vec![DocumentKind::RawEvidence],
            tool_grants: vec![ToolGrant {
                kind: ToolKind::ReadRawEvidence,
                allowed_sources: vec!["market".to_owned()],
            }],
            output_type: "claims".to_owned(),
            output_schema: BlobRef {
                hash: hash.clone(),
                media_type: "application/json".to_owned(),
                bytes: 1,
            },
            budget: TaskBudget {
                max_input_tokens: 1,
                max_output_tokens: 1,
                max_wall_time_secs: 1,
                max_tool_calls: 1,
            },
            retry: RetryPolicy::none(),
            termination: TerminationPolicy::leaf(),
            on_failure: FailureDisposition::FailTask,
            contract_hash: hash,
        }
    }

    #[test]
    fn tool_reread_is_source_and_contract_bound() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        let broker = ContextBroker::new(store.clone());
        let now = Utc::now();
        let run = RunId::new();
        let task_id = TaskId::new();
        store
            .create_run(&run, akzio_domain::RunPurpose::Debug, "test", now)
            .unwrap();
        store
            .enqueue_task(&run, &task_id, akzio_domain::TaskKind::Investigate, now)
            .unwrap();
        let raw = broker
            .record_json_with_provenance(
                NewJsonDocument {
                    kind: DocumentKind::RawEvidence,
                    producer: "ingest.quote".to_owned(),
                    run_id: Some(run.clone()),
                    lifecycle: DocumentLifecycle::Canonical,
                    source_refs: vec![],
                    origin: None,
                    value: &json!({"price": 10}),
                    created_at: now,
                },
                akzio_domain::Provenance {
                    source: "alpaca.quote.TQQQ".to_owned(),
                    observed_at: Some(now),
                    retrieved_at: now,
                    source_uri: None,
                    confidence_ppm: 1_000_000,
                    contract_hash: None,
                },
            )
            .unwrap();
        let runtime = ToolRuntime::new(broker.clone(), SourceRegistry::default());
        let context = ToolCallContext {
            run_id: run,
            task_id,
            attempt_id: AttemptId::new(),
            contract_hash: contract().contract_hash.clone(),
            context_manifest_id: None,
        };
        let result = runtime
            .execute(
                &contract(),
                &context,
                &ModelToolCall {
                    call_id: "call-1".to_owned(),
                    name: "read_raw_evidence".to_owned(),
                    arguments: json!({"document_id": raw.document_id}),
                },
                now,
            )
            .unwrap();
        assert_eq!(result.model_result["ok"], true);
        assert_eq!(broker.store().event_count(&context.run_id).unwrap(), 2);
    }
}
