//! Provenance-preserving context construction for Akzio v2 agents.
//!
//! A caller can only hand an agent a [`ContextManifest`].  The manifest lists
//! immutable documents and their byte budget; raw evidence is never silently
//! copied into an untracked prompt string.

use std::collections::{BTreeMap, BTreeSet};

use akzio_domain::{
    canonical_json_bytes, content_hash_json, ContentHash, DocumentId, DocumentKind,
    DocumentLifecycle, DocumentOrigin, DocumentRecord, Provenance, RunId,
};
use akzio_store::V2Store;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

mod rebuild;
pub use rebuild::*;

#[derive(Debug, Error)]
pub enum ContextError {
    #[error(transparent)]
    Store(#[from] akzio_store::StoreError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("context document {document_id} is not allowed by this contract")]
    ForbiddenKind { document_id: DocumentId },
    #[error("context exceeds its {max_bytes} byte budget")]
    BudgetExceeded { max_bytes: u64 },
    #[error("context exceeds its {max_tokens} token budget")]
    TokenBudgetExceeded { max_tokens: u32 },
    #[error("raw evidence is only available through a Rust-controlled tool reread")]
    RawEvidenceRequiresTool,
}

pub type Result<T> = std::result::Result<T, ContextError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextRequest {
    pub allowed_kinds: BTreeSet<DocumentKind>,
    /// Inputs explicitly attached to a task always win over ranked run context.
    pub explicit_document_ids: BTreeSet<DocumentId>,
    pub max_documents: usize,
    pub max_bytes: u64,
    /// Conservative input-token budget, enforced alongside the byte cap.
    pub max_tokens: u32,
    /// Changes whenever Context selection or rendering semantics change.
    pub policy_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextSelectionReason {
    ExplicitTaskInput,
    RecentSemanticDetail,
    RecentClaim,
    RecentChallenge,
    ProvenMemory,
    NormalizedEvidence,
    OtherContractInput,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSelection {
    pub document_id: DocumentId,
    pub reason: ContextSelectionReason,
    pub estimated_tokens: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextManifest {
    pub documents: Vec<DocumentRecord>,
    pub selection: Vec<ContextSelection>,
    pub total_bytes: u64,
    pub estimated_tokens: u32,
    pub policy_version: u32,
    pub input_hash: ContentHash,
}

pub struct NewJsonDocument<'a> {
    pub kind: DocumentKind,
    pub producer: String,
    pub run_id: Option<RunId>,
    pub lifecycle: DocumentLifecycle,
    pub source_refs: Vec<DocumentId>,
    pub origin: Option<DocumentOrigin>,
    pub value: &'a Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ContextBroker {
    store: V2Store,
}

impl ContextBroker {
    pub fn new(store: V2Store) -> Self {
        Self { store }
    }

    pub fn record_json(&self, input: NewJsonDocument<'_>) -> Result<DocumentRecord> {
        let provenance = Provenance::local(input.producer.clone(), input.created_at);
        self.record_json_with_provenance(input, provenance)
    }

    pub fn record_json_with_provenance(
        &self,
        input: NewJsonDocument<'_>,
        provenance: Provenance,
    ) -> Result<DocumentRecord> {
        let bytes = canonical_json_bytes(input.value)?;
        let blob = self.store.put_bytes(&bytes, "application/json")?;
        let document = DocumentRecord {
            document_id: DocumentId::new(),
            kind: input.kind,
            blob,
            producer: input.producer,
            run_id: input.run_id,
            lifecycle: input.lifecycle,
            source_refs: input.source_refs,
            provenance,
            origin: input.origin,
            created_at: input.created_at,
        };
        self.store.register_document(&document)?;
        Ok(document)
    }

    pub fn derive_detail(
        &self,
        producer: impl Into<String>,
        run_id: Option<RunId>,
        source_refs: Vec<DocumentId>,
        value: &Value,
        created_at: DateTime<Utc>,
    ) -> Result<DocumentRecord> {
        self.record_json(NewJsonDocument {
            kind: DocumentKind::SemanticDetail,
            producer: producer.into(),
            run_id,
            lifecycle: DocumentLifecycle::Canonical,
            source_refs,
            origin: None,
            value,
            created_at,
        })
    }

    /// Persist the exact evidence surface supplied to one agent attempt. The
    /// model never receives an unrecorded prompt context.
    pub fn record_manifest(
        &self,
        producer: impl Into<String>,
        run_id: Option<RunId>,
        manifest: &ContextManifest,
        provenance: Provenance,
        origin: Option<DocumentOrigin>,
        created_at: DateTime<Utc>,
    ) -> Result<DocumentRecord> {
        let source_refs = manifest
            .documents
            .iter()
            .map(|document| document.document_id.clone())
            .collect::<Vec<_>>();
        let value = serde_json::json!({
            "documents": manifest.documents.iter().map(|document| serde_json::json!({
                "document_id": document.document_id,
                "kind": document.kind,
                "blob": document.blob,
                "producer": document.producer,
                    "source_refs": document.source_refs,
                    "provenance": document.provenance,
                    "origin": document.origin,
            })).collect::<Vec<_>>(),
            "selection": manifest.selection,
            "total_bytes": manifest.total_bytes,
            "estimated_tokens": manifest.estimated_tokens,
            "policy_version": manifest.policy_version,
            "input_hash": manifest.input_hash,
        });
        self.record_json_with_provenance(
            NewJsonDocument {
                kind: DocumentKind::ContextManifest,
                producer: producer.into(),
                run_id,
                lifecycle: DocumentLifecycle::RunScoped,
                source_refs,
                origin,
                value: &value,
                created_at,
            },
            provenance,
        )
    }

    pub fn assemble(
        &self,
        request: &ContextRequest,
        document_ids: impl IntoIterator<Item = DocumentId>,
    ) -> Result<ContextManifest> {
        if request.allowed_kinds.contains(&DocumentKind::RawEvidence) {
            return Err(ContextError::RawEvidenceRequiresTool);
        }
        let mut candidates = BTreeMap::new();
        for document_id in document_ids {
            let document = self.store.read_document(&document_id)?;
            if document.kind == DocumentKind::RawEvidence
                || !request.allowed_kinds.contains(&document.kind)
            {
                return Err(ContextError::ForbiddenKind { document_id });
            }
            candidates
                .entry(document.document_id.clone())
                .or_insert(document);
        }

        let mut ranked = candidates.into_values().collect::<Vec<_>>();
        ranked.sort_by(|left, right| {
            context_rank(request, left)
                .cmp(&context_rank(request, right))
                .then_with(|| {
                    right
                        .provenance
                        .confidence_ppm
                        .cmp(&left.provenance.confidence_ppm)
                })
                .then_with(|| right.created_at.cmp(&left.created_at))
                .then_with(|| left.document_id.cmp(&right.document_id))
        });

        let mut documents = Vec::new();
        let mut selection = Vec::new();
        let mut total_bytes = 0_u64;
        let mut estimated_tokens = 0_u32;
        for document in ranked {
            if documents.len() == request.max_documents {
                break;
            }
            let next_bytes = total_bytes.saturating_add(document.blob.bytes);
            let document_tokens = estimate_document_tokens(&document);
            let next_tokens = estimated_tokens.saturating_add(document_tokens);
            let explicit = request
                .explicit_document_ids
                .contains(&document.document_id);
            if next_bytes > request.max_bytes {
                if explicit {
                    return Err(ContextError::BudgetExceeded {
                        max_bytes: request.max_bytes,
                    });
                }
                continue;
            }
            if next_tokens > request.max_tokens {
                if explicit {
                    return Err(ContextError::TokenBudgetExceeded {
                        max_tokens: request.max_tokens,
                    });
                }
                continue;
            }
            total_bytes = next_bytes;
            estimated_tokens = next_tokens;
            selection.push(ContextSelection {
                document_id: document.document_id.clone(),
                reason: context_reason(request, &document),
                estimated_tokens: document_tokens,
            });
            documents.push(document);
        }
        let input_hash = content_hash_json(&serde_json::json!({
            "policy_version": request.policy_version,
            "documents": documents.iter().map(|document| serde_json::json!({
                "document_id": document.document_id.to_string(),
                "blob_hash": document.blob.hash.to_string(),
            })).collect::<Vec<_>>(),
        }))?;
        Ok(ContextManifest {
            documents,
            selection,
            total_bytes,
            estimated_tokens,
            policy_version: request.policy_version,
            input_hash,
        })
    }

    pub fn read_json(&self, document: &DocumentRecord) -> Result<Value> {
        let bytes = self.store.read_blob(&document.blob)?;
        serde_json::from_slice(&bytes).map_err(ContextError::from)
    }

    pub fn store(&self) -> &V2Store {
        &self.store
    }
}

fn context_rank(request: &ContextRequest, document: &DocumentRecord) -> u8 {
    if request
        .explicit_document_ids
        .contains(&document.document_id)
    {
        return 0;
    }
    match document.kind {
        DocumentKind::SemanticDetail | DocumentKind::CompactedContext => 1,
        DocumentKind::AgentClaim => 2,
        DocumentKind::Challenge => 3,
        DocumentKind::Memory => 4,
        DocumentKind::NormalizedEvidence => 5,
        _ => 6,
    }
}

fn context_reason(request: &ContextRequest, document: &DocumentRecord) -> ContextSelectionReason {
    if request
        .explicit_document_ids
        .contains(&document.document_id)
    {
        return ContextSelectionReason::ExplicitTaskInput;
    }
    match document.kind {
        DocumentKind::SemanticDetail | DocumentKind::CompactedContext => {
            ContextSelectionReason::RecentSemanticDetail
        }
        DocumentKind::AgentClaim => ContextSelectionReason::RecentClaim,
        DocumentKind::Challenge => ContextSelectionReason::RecentChallenge,
        DocumentKind::Memory => ContextSelectionReason::ProvenMemory,
        DocumentKind::NormalizedEvidence => ContextSelectionReason::NormalizedEvidence,
        _ => ContextSelectionReason::OtherContractInput,
    }
}

fn estimate_document_tokens(document: &DocumentRecord) -> u32 {
    let bytes = document.blob.bytes.saturating_add(3) / 4;
    u32::try_from(bytes.saturating_add(16)).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use akzio_domain::DocumentKind;
    use tempfile::tempdir;

    #[test]
    fn broker_preserves_source_refs_and_enforces_contract_scope() {
        let directory = tempdir().unwrap();
        let broker = ContextBroker::new(V2Store::open(directory.path()).unwrap());
        let raw = broker
            .record_json(NewJsonDocument {
                kind: DocumentKind::RawEvidence,
                producer: "ingest.market".to_owned(),
                run_id: None,
                lifecycle: DocumentLifecycle::Canonical,
                source_refs: vec![],
                origin: None,
                value: &serde_json::json!({"symbol": "TQQQ", "price": 100}),
                created_at: Utc::now(),
            })
            .unwrap();
        let detail = broker
            .derive_detail(
                "investigator.evidence",
                None,
                vec![raw.document_id.clone()],
                &serde_json::json!({"claim": "TQQQ moved"}),
                Utc::now(),
            )
            .unwrap();
        let allowed = broker
            .assemble(
                &ContextRequest {
                    allowed_kinds: BTreeSet::from([DocumentKind::SemanticDetail]),
                    explicit_document_ids: BTreeSet::new(),
                    max_documents: 1,
                    max_bytes: 1_000,
                    max_tokens: 1_000,
                    policy_version: 1,
                },
                [detail.document_id.clone()],
            )
            .unwrap();
        assert_eq!(allowed.documents[0].source_refs, vec![raw.document_id]);
        assert!(matches!(
            broker.assemble(
                &ContextRequest {
                    allowed_kinds: BTreeSet::from([DocumentKind::SemanticDetail]),
                    explicit_document_ids: BTreeSet::new(),
                    max_documents: 1,
                    max_bytes: 1_000,
                    max_tokens: 1_000,
                    policy_version: 1,
                },
                [allowed.documents[0].source_refs[0].clone()],
            ),
            Err(ContextError::ForbiddenKind { .. })
        ));
    }

    #[test]
    fn broker_records_why_context_documents_were_selected() {
        let directory = tempdir().unwrap();
        let broker = ContextBroker::new(V2Store::open(directory.path()).unwrap());
        let now = Utc::now();
        let normalized = broker
            .record_json(NewJsonDocument {
                kind: DocumentKind::NormalizedEvidence,
                producer: "ingest.fixture".to_owned(),
                run_id: None,
                lifecycle: DocumentLifecycle::Canonical,
                source_refs: vec![],
                origin: None,
                value: &serde_json::json!({"symbol": "QQQ"}),
                created_at: now,
            })
            .unwrap();
        let detail = broker
            .derive_detail(
                "research.fixture",
                None,
                vec![normalized.document_id.clone()],
                &serde_json::json!({"summary": "recent detail"}),
                now + chrono::Duration::seconds(1),
            )
            .unwrap();
        let manifest = broker
            .assemble(
                &ContextRequest {
                    allowed_kinds: BTreeSet::from([
                        DocumentKind::NormalizedEvidence,
                        DocumentKind::SemanticDetail,
                    ]),
                    explicit_document_ids: BTreeSet::from([normalized.document_id.clone()]),
                    max_documents: 2,
                    max_bytes: 1_000,
                    max_tokens: 1_000,
                    policy_version: 1,
                },
                [detail.document_id.clone(), normalized.document_id.clone()],
            )
            .unwrap();
        assert_eq!(manifest.documents[0].document_id, normalized.document_id);
        assert_eq!(
            manifest.selection[0].reason,
            ContextSelectionReason::ExplicitTaskInput
        );
        assert_eq!(manifest.documents[1].document_id, detail.document_id);
        assert_eq!(
            manifest.selection[1].reason,
            ContextSelectionReason::RecentSemanticDetail
        );
        assert!(manifest.estimated_tokens > 0);
        assert!(!manifest.input_hash.as_str().is_empty());
    }

    #[test]
    fn raw_evidence_and_explicit_token_overflow_fail_closed() {
        let directory = tempdir().unwrap();
        let broker = ContextBroker::new(V2Store::open(directory.path()).unwrap());
        let raw = broker
            .record_json(NewJsonDocument {
                kind: DocumentKind::RawEvidence,
                producer: "ingest.fixture".to_owned(),
                run_id: None,
                lifecycle: DocumentLifecycle::Canonical,
                source_refs: vec![],
                origin: None,
                value: &serde_json::json!({"payload": "too large for direct context"}),
                created_at: Utc::now(),
            })
            .unwrap();
        let request = ContextRequest {
            allowed_kinds: BTreeSet::from([DocumentKind::RawEvidence]),
            explicit_document_ids: BTreeSet::from([raw.document_id.clone()]),
            max_documents: 1,
            max_bytes: 10_000,
            max_tokens: 10_000,
            policy_version: 1,
        };
        assert!(matches!(
            broker.assemble(&request, [raw.document_id.clone()]),
            Err(ContextError::RawEvidenceRequiresTool)
        ));

        let detail = broker
            .derive_detail(
                "test.detail",
                None,
                vec![raw.document_id],
                &serde_json::json!({"claim": "bounded detail"}),
                Utc::now(),
            )
            .unwrap();
        let request = ContextRequest {
            allowed_kinds: BTreeSet::from([DocumentKind::SemanticDetail]),
            explicit_document_ids: BTreeSet::from([detail.document_id.clone()]),
            max_documents: 1,
            max_bytes: 10_000,
            max_tokens: 1,
            policy_version: 1,
        };
        assert!(matches!(
            broker.assemble(&request, [detail.document_id]),
            Err(ContextError::TokenBudgetExceeded { .. })
        ));
    }
}
