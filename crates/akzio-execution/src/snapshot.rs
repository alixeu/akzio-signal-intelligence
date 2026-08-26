use chrono::{DateTime, Utc};
use thiserror::Error;

use akzio_domain::{
    AccountSnapshot, Artifact, ArtifactKind, ArtifactLifecycle, ArtifactProvenance, ArtifactRef,
    DomainError, MarketClockSnapshot, QuoteSnapshot, TaskWritePermit,
};
use akzio_store::{StoreError, V2Store};

#[derive(Debug, Error)]
pub enum SnapshotArtifactError {
    #[error("{0}")]
    InvalidInput(String),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

mod sealed {
    pub trait Sealed {}

    impl Sealed for akzio_domain::AccountSnapshot {}
    impl Sealed for akzio_domain::QuoteSnapshot {}
    impl Sealed for akzio_domain::MarketClockSnapshot {}
}

pub trait ExecutionSnapshotPayload: serde::Serialize + sealed::Sealed {
    fn validate_snapshot(&self) -> Result<(), DomainError>;
}

impl ExecutionSnapshotPayload for AccountSnapshot {
    fn validate_snapshot(&self) -> Result<(), DomainError> {
        self.validate()
    }
}

impl ExecutionSnapshotPayload for QuoteSnapshot {
    fn validate_snapshot(&self) -> Result<(), DomainError> {
        self.validate()
    }
}

impl ExecutionSnapshotPayload for MarketClockSnapshot {
    fn validate_snapshot(&self) -> Result<(), DomainError> {
        self.validate()
    }
}

pub struct SnapshotArtifactMaterializer;

impl SnapshotArtifactMaterializer {
    #[allow(clippy::too_many_arguments)]
    pub fn materialize<T: ExecutionSnapshotPayload>(
        store: &V2Store,
        permit: &TaskWritePermit,
        normalized_sources: &[&Artifact],
        producer: &str,
        payload: &T,
        observed_at: DateTime<Utc>,
        source_uri: Option<String>,
        now: DateTime<Utc>,
    ) -> Result<Artifact, SnapshotArtifactError> {
        payload.validate_snapshot()?;
        let first_normalized = normalized_sources.first().ok_or_else(|| {
            SnapshotArtifactError::InvalidInput(
                "execution snapshot has no normalized sources".to_owned(),
            )
        })?;

        let expected_origin = permit.artifact_origin();
        let mut source_refs = Vec::with_capacity(normalized_sources.len() * 2);
        for normalized in normalized_sources {
            if normalized.kind != ArtifactKind::NormalizedEvidence
                || normalized.lifecycle != ArtifactLifecycle::RunScoped
                || normalized.origin.as_ref() != Some(&expected_origin)
                || normalized.provenance.source_family != first_normalized.provenance.source_family
            {
                return Err(SnapshotArtifactError::InvalidInput(
                    "execution snapshot source is not permit-bound normalized evidence".to_owned(),
                ));
            }
            let raw_source = normalized
                .source_refs
                .iter()
                .find(|source_ref| source_ref.kind == ArtifactKind::RawEvidence)
                .ok_or_else(|| {
                    SnapshotArtifactError::InvalidInput(
                        "governed normalized evidence has no RawEvidence source".to_owned(),
                    )
                })?;

            source_refs.push(raw_source.clone());
            source_refs.push(ArtifactRef {
                artifact_id: normalized.artifact_id.clone(),
                kind: ArtifactKind::NormalizedEvidence,
            });
        }
        source_refs.sort();
        source_refs.dedup();

        let blob = store.put_json(payload)?;
        let provenance = ArtifactProvenance {
            source_family: first_normalized.provenance.source_family.clone(),
            observed_at: Some(observed_at),
            retrieved_at: now,
            source_uri,
            confidence_ppm: first_normalized.provenance.confidence_ppm,
            producer_contract_hash: permit.contract_hash.clone(),
        };

        Ok(Artifact::new(
            ArtifactKind::NormalizedEvidence,
            blob,
            producer,
            ArtifactLifecycle::Canonical,
            provenance,
            Some(permit.artifact_origin()),
            source_refs,
            now,
        )?)
    }
}

#[cfg(test)]
mod tests {
    use akzio_domain::{
        AccountSnapshot, ArtifactOrigin, FailureDisposition, MoneyMicros, RetryPolicy, RunId,
        RunPurpose, TaskBudget, TaskId, TaskRecipeId, WorkflowGraph, WorkflowNode,
        V2_DOMAIN_SCHEMA_VERSION,
    };
    use akzio_store::v2::{StoredRun, WorkflowCommit};
    use chrono::Duration;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn invalid_account_snapshot_is_rejected_before_materialization() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        let node = WorkflowNode {
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new("execution.snapshot").unwrap(),
            contract_hash: None,
            objective: "materialize execution snapshot".to_owned(),
            dependencies: vec![],
            input_artifacts: vec![],
            priority: 100,
            budget: TaskBudget {
                max_input_tokens: 1,
                max_output_tokens: 1,
                max_wall_time_secs: 30,
                max_tool_calls: 0,
            },
            retry: RetryPolicy::none(),
            on_failure: FailureDisposition::FailRun,
            parent_task_id: None,
        };
        let graph = WorkflowGraph {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            topology_id: "snapshot-test".to_owned(),
            nodes: vec![node],
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
        store
            .commit_workflow(&WorkflowCommit {
                run: StoredRun {
                    run_id: RunId::new(),
                    purpose: RunPurpose::Paper,
                    topology_id: graph.topology_id.clone(),
                    graph_artifact_id: graph_artifact.artifact_id.clone(),
                    created_at: now,
                },
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();
        let permit = store
            .claim_next_task("snapshot-test", now, Duration::seconds(30))
            .unwrap()
            .unwrap()
            .permit;
        let origin = Some(ArtifactOrigin {
            run_id: Some(permit.run_id.clone()),
            task_id: Some(permit.task_id.clone()),
            attempt_id: Some(permit.attempt_id.clone()),
            contract_hash: permit.contract_hash.clone(),
        });
        let raw = Artifact::new(
            ArtifactKind::RawEvidence,
            store.put_bytes(b"{}", "application/json").unwrap(),
            "akzio.ingest.alpaca.raw",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "alpaca".to_owned(),
                observed_at: Some(now),
                retrieved_at: now,
                source_uri: Some("fixture://account".to_owned()),
                confidence_ppm: 1_000_000,
                producer_contract_hash: permit.contract_hash.clone(),
            },
            origin.clone(),
            vec![],
            now,
        )
        .unwrap();
        let normalized = Artifact::new(
            ArtifactKind::NormalizedEvidence,
            store
                .put_json(&serde_json::json!({"account": true}))
                .unwrap(),
            "akzio.ingest.alpaca.normalized",
            ArtifactLifecycle::RunScoped,
            raw.provenance.clone(),
            origin,
            vec![ArtifactRef {
                artifact_id: raw.artifact_id,
                kind: ArtifactKind::RawEvidence,
            }],
            now,
        )
        .unwrap();
        let invalid = AccountSnapshot {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            broker_session: "2026-08-25".to_owned(),
            observed_at: now,
            equity: MoneyMicros::ZERO,
            buying_power: MoneyMicros::ZERO,
            day_turnover: MoneyMicros::ZERO,
            active: true,
            trading_blocked: false,
            positions: std::collections::BTreeMap::new(),
            external_positions: std::collections::BTreeSet::new(),
            open_order_ids: std::collections::BTreeSet::new(),
        };

        let error = SnapshotArtifactMaterializer::materialize(
            &store,
            &permit,
            &[&normalized],
            "execution.snapshot.account",
            &invalid,
            now,
            None,
            now,
        )
        .unwrap_err();

        assert!(matches!(error, SnapshotArtifactError::Domain(_)));
    }
}
