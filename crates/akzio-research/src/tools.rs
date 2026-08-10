//! Rust-owned, provenance-preserving tools available to contract-bound agents.
//!
//! Tools never accept a URL or a filesystem path from the model. A model can
//! only reread a durable document whose source belongs to the named registry.

use std::collections::{BTreeMap, BTreeSet};

use akzio_context::legacy::{ContextBroker, ContextError, NewJsonDocument};
use akzio_domain::{
    AttemptId, ContentHash, DocumentId, DocumentKind, DocumentLifecycle, DocumentOrigin,
    DocumentRecord, EventEnvelope, LegacyAgentContract, RunId, TaskId, ToolGrant, ToolKind,
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
        if grant.allowed_sources.is_empty() {
            return false;
        }
        let names: BTreeSet<_> = grant.allowed_sources.iter().cloned().collect();
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
    Context(#[from] ContextError),
    #[error(transparent)]
    Store(#[from] akzio_store::legacy::StoreError),
    #[error("unknown tool {0}")]
    UnknownTool(String),
    #[error("contract does not grant {0:?}")]
    NotGranted(ToolKind),
    #[error("tool argument document_id is required")]
    MissingDocumentId,
    #[error("tool argument document_id is invalid")]
    InvalidDocumentId,
    #[error("tool call is missing its task-bound context manifest")]
    MissingContextManifest,
    #[error("tool context manifest does not belong to this task attempt")]
    InvalidContextManifest,
    #[error("document {0} is outside the active context manifest")]
    OutsideContextManifest(DocumentId),
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

    pub fn definitions(&self, contract: &LegacyAgentContract) -> Vec<ModelToolDefinition> {
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
        contract: &LegacyAgentContract,
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

        let result = self.read(contract, context, call);
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

    fn read(
        &self,
        contract: &LegacyAgentContract,
        context: &ToolCallContext,
        call: &ModelToolCall,
    ) -> Result<(Value, DocumentId)> {
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
        self.ensure_manifest_allows(context, &document_id)?;
        let document = self.broker.store().read_document(&document_id)?;
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
            .permits(grant, &document.provenance.source, false)
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
    fn ensure_manifest_allows(
        &self,
        context: &ToolCallContext,
        document_id: &DocumentId,
    ) -> Result<()> {
        let manifest_id = context
            .context_manifest_id
            .as_ref()
            .ok_or(ToolError::MissingContextManifest)?;
        let manifest = self.broker.store().read_document(manifest_id)?;
        let origin_matches = manifest.origin.as_ref().is_some_and(|origin| {
            origin.task_id.as_ref() == Some(&context.task_id)
                && origin.attempt_id.as_ref() == Some(&context.attempt_id)
                && origin.contract_hash.as_ref() == Some(&context.contract_hash)
        });
        if manifest.kind != DocumentKind::ContextManifest
            || manifest.run_id.as_ref() != Some(&context.run_id)
            || !origin_matches
        {
            return Err(ToolError::InvalidContextManifest);
        }
        let value = self.broker.read_json(&manifest)?;
        let selected = value
            .get("documents")
            .and_then(Value::as_array)
            .ok_or(ToolError::InvalidContextManifest)?;
        if selected.iter().any(|entry| {
            entry.get("document_id").and_then(Value::as_str) == Some(document_id.0.as_str())
        }) {
            Ok(())
        } else {
            Err(ToolError::OutsideContextManifest(document_id.clone()))
        }
    }
}

fn tool_kind(name: &str) -> Result<ToolKind> {
    match name {
        "read_evidence" => Ok(ToolKind::ReadEvidence),
        other => Err(ToolError::UnknownTool(other.to_owned())),
    }
}

fn allowed_kind(tool: ToolKind, kind: DocumentKind) -> bool {
    match tool {
        ToolKind::ReadEvidence => !matches!(kind, DocumentKind::RawEvidence),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use akzio_context::legacy::NewJsonDocument;
    use akzio_domain::{
        BlobRef, ContractId, FailureDisposition, RetryPolicy, TaskBudget, TerminationPolicy,
    };
    use akzio_store::legacy::V2Store;
    use tempfile::tempdir;

    fn contract() -> LegacyAgentContract {
        let hash = ContentHash::of_bytes(b"contract");
        LegacyAgentContract {
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
            input_context_kinds: vec![DocumentKind::NormalizedEvidence],
            tool_grants: vec![ToolGrant {
                kind: ToolKind::ReadEvidence,
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
                    kind: DocumentKind::NormalizedEvidence,
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
        let contract = contract();
        let attempt_id = AttemptId::new();
        let manifest = broker
            .record_json(NewJsonDocument {
                kind: DocumentKind::ContextManifest,
                producer: "context.test.agent".to_owned(),
                run_id: Some(run.clone()),
                lifecycle: DocumentLifecycle::RunScoped,
                source_refs: vec![raw.document_id.clone()],
                origin: Some(DocumentOrigin::task(
                    task_id.clone(),
                    attempt_id.clone(),
                    Some(contract.contract_hash.clone()),
                )),
                value: &json!({"documents": [{"document_id": raw.document_id}]}),
                created_at: now,
            })
            .unwrap();
        let runtime = ToolRuntime::new(broker.clone(), SourceRegistry::default());
        let context = ToolCallContext {
            run_id: run,
            task_id,
            attempt_id,
            contract_hash: contract.contract_hash.clone(),
            context_manifest_id: Some(manifest.document_id),
        };
        let result = runtime
            .execute(
                &contract,
                &context,
                &ModelToolCall {
                    call_id: "call-1".to_owned(),
                    name: "read_evidence".to_owned(),
                    arguments: json!({"document_id": raw.document_id}),
                },
                now,
            )
            .unwrap();
        assert_eq!(result.model_result["ok"], true);

        let outside = broker
            .record_json_with_provenance(
                NewJsonDocument {
                    kind: DocumentKind::NormalizedEvidence,
                    producer: "ingest.quote".to_owned(),
                    run_id: Some(context.run_id.clone()),
                    lifecycle: DocumentLifecycle::Canonical,
                    source_refs: vec![],
                    origin: None,
                    value: &json!({"price": 11}),
                    created_at: now,
                },
                akzio_domain::Provenance {
                    source: "alpaca.quote.QQQ".to_owned(),
                    observed_at: Some(now),
                    retrieved_at: now,
                    source_uri: None,
                    confidence_ppm: 1_000_000,
                    contract_hash: None,
                },
            )
            .unwrap();
        let rejected = runtime
            .execute(
                &contract,
                &context,
                &ModelToolCall {
                    call_id: "call-2".to_owned(),
                    name: "read_evidence".to_owned(),
                    arguments: json!({"document_id": outside.document_id}),
                },
                now,
            )
            .unwrap();
        assert_eq!(rejected.model_result["ok"], false);
        assert!(rejected.model_result["error"]
            .as_str()
            .unwrap()
            .contains("outside the active context manifest"));
        assert_eq!(broker.store().event_count(&context.run_id).unwrap(), 4);
    }
}
