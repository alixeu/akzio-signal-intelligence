//! Rust-owned, allowlisted Evidence Runtime for the rebuilt v2 path.
//!
//! Adapters acquire bytes; agents only receive immutable artifacts. The
//! enclosing `TaskRuntime` commits a completed task attempt through `V2Store`.

use std::collections::{BTreeMap, BTreeSet};

use akzio_domain::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance, ArtifactRef,
    DomainError, TaskWritePermit, V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_store::v2::{StoreError, V2Store};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceSource {
    Alpaca,
    SecEdgar,
    Fred,
    NewsWeb,
}

impl EvidenceSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Alpaca => "alpaca",
            Self::SecEdgar => "sec_edgar",
            Self::Fred => "fred",
            Self::NewsWeb => "news_web",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceRequest {
    pub source: EvidenceSource,
    pub resource: String,
    pub max_age: Duration,
}

impl EvidenceRequest {
    fn validate(&self) -> Result<(), EvidenceRuntimeError> {
        if self.resource.trim().is_empty() || self.max_age <= Duration::zero() {
            return Err(EvidenceRuntimeError::InvalidRequest);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AcquiredEvidence {
    pub raw: Vec<u8>,
    pub media_type: String,
    pub source_uri: String,
    pub observed_at: DateTime<Utc>,
    pub normalized: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NormalizedEvidencePayload {
    pub schema_version: u32,
    pub source: EvidenceSource,
    pub resource: String,
    pub raw: ArtifactRef,
    pub observed_at: DateTime<Utc>,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EvidenceBundle {
    pub raw: Artifact,
    pub normalized: Artifact,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DetailInput {
    pub normalized: ArtifactRef,
    pub value: Value,
}

#[derive(Debug, Error)]
pub enum EvidenceAdapterError {
    #[error("fixture for {0} is unavailable")]
    MissingFixture(String),
    #[error("adapter source does not match request")]
    SourceMismatch,
}

pub trait EvidenceAdapter: Send + Sync {
    fn source(&self) -> EvidenceSource;

    fn acquire(&self, request: &EvidenceRequest) -> Result<AcquiredEvidence, EvidenceAdapterError>;
}

/// Local-only adapter for deterministic test and replay input. It has no
/// filesystem, network, or model capability.
#[derive(Debug, Clone)]
pub struct FixtureEvidenceAdapter {
    source: EvidenceSource,
    responses: BTreeMap<String, AcquiredEvidence>,
}

impl FixtureEvidenceAdapter {
    pub fn new(
        source: EvidenceSource,
        responses: impl IntoIterator<Item = (String, AcquiredEvidence)>,
    ) -> Self {
        Self {
            source,
            responses: responses.into_iter().collect(),
        }
    }
}

impl EvidenceAdapter for FixtureEvidenceAdapter {
    fn source(&self) -> EvidenceSource {
        self.source
    }

    fn acquire(&self, request: &EvidenceRequest) -> Result<AcquiredEvidence, EvidenceAdapterError> {
        if request.source != self.source {
            return Err(EvidenceAdapterError::SourceMismatch);
        }
        self.responses
            .get(&request.resource)
            .cloned()
            .ok_or_else(|| EvidenceAdapterError::MissingFixture(request.resource.clone()))
    }
}

#[derive(Debug, Error)]
pub enum EvidenceRuntimeError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Adapter(#[from] EvidenceAdapterError),
    #[error("evidence source {0:?} is not allowlisted")]
    SourceNotAllowed(EvidenceSource),
    #[error("evidence request is invalid")]
    InvalidRequest,
    #[error("acquired evidence is stale")]
    StaleEvidence,
    #[error("acquired evidence is empty or lacks a media type")]
    InvalidAcquisition,
    #[error("semantic detail must cite normalized evidence")]
    DetailRequiresNormalizedEvidence,
}

pub type EvidenceRuntimeResult<T> = Result<T, EvidenceRuntimeError>;

#[derive(Debug, Clone)]
pub struct EvidenceRuntime {
    store: V2Store,
    allowed_sources: BTreeSet<EvidenceSource>,
}

impl EvidenceRuntime {
    pub fn new(store: V2Store, allowed_sources: impl IntoIterator<Item = EvidenceSource>) -> Self {
        Self {
            store,
            allowed_sources: allowed_sources.into_iter().collect(),
        }
    }

    pub fn store(&self) -> &V2Store {
        &self.store
    }

    /// Construct raw and normalized evidence artifacts. The caller returns
    /// them to `TaskRuntime`, which atomically commits the attempt.
    pub fn acquire_and_normalize<A: EvidenceAdapter>(
        &self,
        permit: &TaskWritePermit,
        request: &EvidenceRequest,
        adapter: &A,
        now: DateTime<Utc>,
    ) -> EvidenceRuntimeResult<EvidenceBundle> {
        request.validate()?;
        if !self.allowed_sources.contains(&request.source) {
            return Err(EvidenceRuntimeError::SourceNotAllowed(request.source));
        }
        if adapter.source() != request.source {
            return Err(EvidenceAdapterError::SourceMismatch.into());
        }
        let acquired = adapter.acquire(request)?;
        if acquired.raw.is_empty()
            || acquired.media_type.trim().is_empty()
            || acquired.source_uri.trim().is_empty()
        {
            return Err(EvidenceRuntimeError::InvalidAcquisition);
        }
        if now.signed_duration_since(acquired.observed_at) > request.max_age {
            return Err(EvidenceRuntimeError::StaleEvidence);
        }

        let raw = Artifact::new(
            ArtifactKind::RawEvidence,
            self.store.put_bytes(&acquired.raw, &acquired.media_type)?,
            format!("akzio.ingest.{}.raw", request.source.as_str()),
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: request.source.as_str().to_owned(),
                observed_at: Some(acquired.observed_at),
                retrieved_at: now,
                source_uri: Some(acquired.source_uri.clone()),
                confidence_ppm: 1_000_000,
                producer_contract_hash: permit.contract_hash.clone(),
            },
            task_origin(permit),
            vec![],
            now,
        )?;
        let raw_ref = ArtifactRef {
            artifact_id: raw.artifact_id.clone(),
            kind: ArtifactKind::RawEvidence,
        };
        let normalized_payload = NormalizedEvidencePayload {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            source: request.source,
            resource: request.resource.clone(),
            raw: raw_ref.clone(),
            observed_at: acquired.observed_at,
            value: acquired.normalized,
        };
        let normalized = Artifact::new(
            ArtifactKind::NormalizedEvidence,
            self.store.put_json(&normalized_payload)?,
            format!("akzio.ingest.{}.normalized", request.source.as_str()),
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: request.source.as_str().to_owned(),
                observed_at: Some(acquired.observed_at),
                retrieved_at: now,
                source_uri: Some(acquired.source_uri),
                confidence_ppm: 1_000_000,
                producer_contract_hash: permit.contract_hash.clone(),
            },
            task_origin(permit),
            vec![raw_ref],
            now,
        )?;
        Ok(EvidenceBundle { raw, normalized })
    }

    /// Materialize a loss-bounded semantic detail in a separate task. The
    /// caller must cite an already sealed normalized artifact.
    pub fn materialize_detail(
        &self,
        permit: &TaskWritePermit,
        input: DetailInput,
        now: DateTime<Utc>,
    ) -> EvidenceRuntimeResult<Artifact> {
        if input.normalized.kind != ArtifactKind::NormalizedEvidence {
            return Err(EvidenceRuntimeError::DetailRequiresNormalizedEvidence);
        }
        let normalized = self.store.artifact(&input.normalized.artifact_id)?;
        if normalized.kind != ArtifactKind::NormalizedEvidence {
            return Err(EvidenceRuntimeError::DetailRequiresNormalizedEvidence);
        }
        let detail = Artifact::new(
            ArtifactKind::SemanticDetail,
            self.store.put_json(&input.value)?,
            "akzio.ingest.semantic_detail",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: normalized.provenance.source_family.clone(),
                observed_at: normalized.provenance.observed_at,
                retrieved_at: now,
                source_uri: normalized.provenance.source_uri.clone(),
                confidence_ppm: normalized.provenance.confidence_ppm,
                producer_contract_hash: permit.contract_hash.clone(),
            },
            task_origin(permit),
            vec![input.normalized],
            now,
        )?;
        Ok(detail)
    }
}

fn task_origin(permit: &TaskWritePermit) -> Option<ArtifactOrigin> {
    Some(ArtifactOrigin {
        run_id: Some(permit.run_id.clone()),
        task_id: Some(permit.task_id.clone()),
        attempt_id: Some(permit.attempt_id.clone()),
        contract_hash: permit.contract_hash.clone(),
    })
}

#[cfg(test)]
mod tests {
    use akzio_domain::{
        FailureDisposition, RetryPolicy, RunId, RunPurpose, TaskBudget, TaskId, TaskRecipeId,
        TaskStatus, WorkflowGraph, WorkflowNode,
    };
    use akzio_store::v2::{StoredRun, WorkflowCommit};
    use tempfile::tempdir;

    use super::*;

    fn budget() -> TaskBudget {
        TaskBudget {
            max_input_tokens: 64,
            max_output_tokens: 32,
            max_wall_time_secs: 10,
            max_tool_calls: 1,
        }
    }

    fn retry() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 1,
            retry_transport: true,
            retry_rate_limited: true,
            retry_invalid_output: false,
        }
    }

    fn install_run(store: &V2Store, now: DateTime<Utc>, tasks: usize) -> RunId {
        let graph = WorkflowGraph {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            topology_id: "fixture".to_owned(),
            nodes: (0..tasks)
                .map(|index| WorkflowNode {
                    task_id: TaskId::new(),
                    recipe_id: TaskRecipeId::new(format!("evidence.fixture.{index}")).unwrap(),
                    contract_hash: None,
                    objective: "seal evidence".to_owned(),
                    dependencies: vec![],
                    input_artifacts: vec![],
                    priority: 50,
                    budget: budget(),
                    retry: retry(),
                    on_failure: FailureDisposition::FailRun,
                    parent_task_id: None,
                })
                .collect(),
        };
        let graph_artifact = Artifact::new(
            ArtifactKind::WorkflowGraph,
            store.put_json(&graph).unwrap(),
            "fixture.workflow",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "fixture".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            None,
            vec![],
            now,
        )
        .unwrap();
        let run = StoredRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Debug,
            topology_id: graph.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: now,
        };
        store
            .commit_workflow(&WorkflowCommit {
                run: run.clone(),
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();
        run.run_id
    }

    fn fixture(now: DateTime<Utc>) -> FixtureEvidenceAdapter {
        FixtureEvidenceAdapter::new(
            EvidenceSource::Alpaca,
            [(
                "quote".to_owned(),
                AcquiredEvidence {
                    raw: br#"{\"quote\": \"fixture\"}"#.to_vec(),
                    media_type: "application/json".to_owned(),
                    source_uri: "fixture://alpaca/quote".to_owned(),
                    observed_at: now,
                    normalized: serde_json::json!({"symbol": "QQQ", "price": 1}),
                },
            )],
        )
    }

    #[test]
    fn acquisition_returns_uncommitted_artifacts_until_task_runtime_commits() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        let run_id = install_run(&store, now, 1);
        let claimed = store
            .claim_next_task("evidence-worker", now, Duration::seconds(30))
            .unwrap()
            .unwrap();
        let events_before = store.events_after(&run_id, 0, 10).unwrap();
        let runtime = EvidenceRuntime::new(store.clone(), [EvidenceSource::Alpaca]);
        let sealed = runtime
            .acquire_and_normalize(
                &claimed.permit,
                &EvidenceRequest {
                    source: EvidenceSource::Alpaca,
                    resource: "quote".to_owned(),
                    max_age: Duration::seconds(30),
                },
                &fixture(now),
                now,
            )
            .unwrap();
        assert_eq!(sealed.raw.kind, ArtifactKind::RawEvidence);
        assert_eq!(sealed.normalized.kind, ArtifactKind::NormalizedEvidence);
        assert_eq!(
            sealed.normalized.source_refs,
            vec![ArtifactRef {
                artifact_id: sealed.raw.artifact_id.clone(),
                kind: ArtifactKind::RawEvidence,
            }]
        );
        assert!(matches!(
            store.artifact(&sealed.raw.artifact_id),
            Err(akzio_store::v2::StoreError::MissingArtifact(_))
        ));
        assert!(matches!(
            store.artifact(&sealed.normalized.artifact_id),
            Err(akzio_store::v2::StoreError::MissingArtifact(_))
        ));
        assert_eq!(store.events_after(&run_id, 0, 10).unwrap(), events_before);

        store
            .commit_attempt(
                &claimed.permit,
                &[sealed.raw.clone(), sealed.normalized.clone()],
                TaskStatus::Succeeded,
                now,
            )
            .unwrap();

        assert_eq!(store.artifact(&sealed.raw.artifact_id).unwrap(), sealed.raw);
        assert_eq!(
            store.artifact(&sealed.normalized.artifact_id).unwrap(),
            sealed.normalized
        );
        let events_after = store.events_after(&run_id, 0, 10).unwrap();
        assert_eq!(events_after.len(), events_before.len() + 3);
        assert_eq!(
            events_after
                .iter()
                .filter(|event| event.event_type == "task.succeeded")
                .count(),
            1
        );
        store.verify_integrity().unwrap();
    }

    #[test]
    fn stale_or_unallowlisted_evidence_never_writes_task_output() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        let run_id = install_run(&store, now, 1);
        let permit = store
            .claim_next_task("evidence-worker", now, Duration::seconds(30))
            .unwrap()
            .unwrap()
            .permit;
        let stale = FixtureEvidenceAdapter::new(
            EvidenceSource::Alpaca,
            [(
                "quote".to_owned(),
                AcquiredEvidence {
                    raw: b"fixture".to_vec(),
                    media_type: "application/json".to_owned(),
                    source_uri: "fixture://alpaca/quote".to_owned(),
                    observed_at: now - Duration::minutes(5),
                    normalized: serde_json::json!({}),
                },
            )],
        );
        let runtime = EvidenceRuntime::new(store.clone(), [EvidenceSource::Alpaca]);
        assert!(matches!(
            runtime.acquire_and_normalize(
                &permit,
                &EvidenceRequest {
                    source: EvidenceSource::Alpaca,
                    resource: "quote".to_owned(),
                    max_age: Duration::seconds(30),
                },
                &stale,
                now,
            ),
            Err(EvidenceRuntimeError::StaleEvidence)
        ));
        assert_eq!(store.events_after(&run_id, 0, 10).unwrap().len(), 2);
        assert!(matches!(
            EvidenceRuntime::new(store, [EvidenceSource::Fred]).acquire_and_normalize(
                &permit,
                &EvidenceRequest {
                    source: EvidenceSource::Alpaca,
                    resource: "quote".to_owned(),
                    max_age: Duration::seconds(30),
                },
                &fixture(now),
                now,
            ),
            Err(EvidenceRuntimeError::SourceNotAllowed(
                EvidenceSource::Alpaca
            ))
        ));
    }

    #[test]
    fn semantic_detail_is_constructed_then_committed_by_task_runtime() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        install_run(&store, now, 2);
        let first = store
            .claim_next_task("evidence-worker", now, Duration::seconds(30))
            .unwrap()
            .unwrap();
        let runtime = EvidenceRuntime::new(store.clone(), [EvidenceSource::Alpaca]);
        let sealed = runtime
            .acquire_and_normalize(
                &first.permit,
                &EvidenceRequest {
                    source: EvidenceSource::Alpaca,
                    resource: "quote".to_owned(),
                    max_age: Duration::seconds(30),
                },
                &fixture(now),
                now,
            )
            .unwrap();
        store
            .commit_attempt(
                &first.permit,
                &[sealed.raw.clone(), sealed.normalized.clone()],
                TaskStatus::Succeeded,
                now,
            )
            .unwrap();
        let second = store
            .claim_next_task("detail-worker", now, Duration::seconds(30))
            .unwrap()
            .unwrap();
        let detail = runtime
            .materialize_detail(
                &second.permit,
                DetailInput {
                    normalized: ArtifactRef {
                        artifact_id: sealed.normalized.artifact_id.clone(),
                        kind: ArtifactKind::NormalizedEvidence,
                    },
                    value: serde_json::json!({"summary": "fixture"}),
                },
                now,
            )
            .unwrap();
        assert_eq!(detail.kind, ArtifactKind::SemanticDetail);
        assert_eq!(detail.source_refs.len(), 1);
        assert!(matches!(
            store.artifact(&detail.artifact_id),
            Err(akzio_store::v2::StoreError::MissingArtifact(_))
        ));
        store
            .commit_attempt(
                &second.permit,
                std::slice::from_ref(&detail),
                TaskStatus::Succeeded,
                now,
            )
            .unwrap();
        assert_eq!(store.artifact(&detail.artifact_id).unwrap(), detail);
        store.verify_integrity().unwrap();
    }
}
