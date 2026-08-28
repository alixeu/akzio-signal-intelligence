use akzio_domain::{
    FailureDisposition, LifecycleEventType, RetryPolicy, RunId, RunPurpose, TaskBudget, TaskId,
    TaskRecipeId, TaskStatus, WorkflowGraph, WorkflowNode,
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

#[derive(Clone)]
struct ParityAdapter {
    evidence: AcquiredEvidence,
}

impl EvidenceAdapter for ParityAdapter {
    fn source(&self) -> EvidenceSource {
        EvidenceSource::Alpaca
    }

    fn acquire(
        &self,
        _request: &EvidenceRequest,
    ) -> Result<AcquiredEvidence, EvidenceAdapterError> {
        Ok(self.evidence.clone())
    }
}

impl AsyncEvidenceAdapter for ParityAdapter {
    fn source(&self) -> EvidenceSource {
        EvidenceSource::Alpaca
    }

    fn acquire<'a>(
        &'a self,
        _request: &'a EvidenceRequest,
    ) -> BoxFuture<'a, Result<AcquiredEvidence, EvidenceAdapterError>> {
        let evidence = self.evidence.clone();
        Box::pin(async move { Ok(evidence) })
    }
}

fn fixture_acquired() -> AcquiredEvidence {
    let observed_at = Utc::now();
    AcquiredEvidence {
        raw: br#"{\"fixture\":true}"#.to_vec(),
        media_type: "application/json".to_owned(),
        source_uri: "fixture://governed/resource".to_owned(),
        observed_at,
        normalized: serde_json::json!({"fixture": true}),
        provenance: EvidenceProvenance {
            document_id: Some("fixture-governed".to_owned()),
            published_at: None,
            observed_at,
            revision: Some("1".to_owned()),
            source_uri: "fixture://governed/resource".to_owned(),
            dedupe_key: "fixture:governed:resource".to_owned(),
            citations: vec![EvidenceCitation {
                start_byte: 0,
                end_byte: 18,
                quote: "{\"fixture\":true}".to_owned(),
            }],
        },
        quality: EvidenceQuality::default(),
    }
}

#[test]
fn fixture_adapters_are_source_typed() {
    let pairs = [
        (EvidenceSource::Alpaca, EvidenceSource::SecEdgar),
        (EvidenceSource::SecEdgar, EvidenceSource::Fred),
        (EvidenceSource::Fred, EvidenceSource::NewsWeb),
        (EvidenceSource::NewsWeb, EvidenceSource::Alpaca),
    ];
    for (source, other) in pairs {
        let adapter =
            FixtureEvidenceAdapter::new(source, [("resource".to_owned(), fixture_acquired())]);
        let response = EvidenceAdapter::acquire(
            &adapter,
            &EvidenceRequest {
                source,
                resource: "resource".to_owned(),
                max_age: Duration::seconds(30),
            },
        )
            .unwrap();
        assert_eq!(response.normalized["fixture"], true);
        assert!(matches!(
            EvidenceAdapter::acquire(
                &adapter,
                &EvidenceRequest {
                source: other,
                resource: "resource".to_owned(),
                max_age: Duration::seconds(30),
                },
            ),
            Err(EvidenceAdapterError::SourceMismatch)
        ));
    }
}

#[test]
fn source_uri_rejects_credentials_and_query_parameters() {
    assert!(EvidenceRuntime::validate_source_uri("fixture://alpaca/quote").is_ok());
    assert!(matches!(
        EvidenceRuntime::validate_source_uri("https://key:secret@example.test/evidence"),
        Err(EvidenceRuntimeError::UnsafeSourceUri)
    ));
    assert!(matches!(
        EvidenceRuntime::validate_source_uri("https://example.test/evidence?token=secret"),
        Err(EvidenceRuntimeError::UnsafeSourceUri)
    ));
    assert!(EvidenceRuntime::validate_source_uri(
        "https://fred.stlouisfed.org/series/DFII10?cosd=2020-01-01"
    )
    .is_ok());
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
                provenance: EvidenceProvenance {
                    document_id: Some("fixture-quote".to_owned()),
                    published_at: None,
                    observed_at: now,
                    revision: Some("1".to_owned()),
                    source_uri: "fixture://alpaca/quote".to_owned(),
                    dedupe_key: "fixture:alpaca:quote".to_owned(),
                    citations: vec![],
                },
                quality: EvidenceQuality::default(),
            },
        )],
    )
}

fn evidence_need(
    store: &V2Store,
    task: &akzio_store::v2::ClaimedAttempt,
    now: DateTime<Utc>,
) -> ArtifactRef {
    let payload = akzio_domain::EvidenceNeed {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        source_family: "alpaca".to_owned(),
        resource: "quote".to_owned(),
        max_age_secs: 30,
    };
    let artifact = Artifact::new(
        ArtifactKind::EvidenceNeed,
        store.put_json(&payload).unwrap(),
        "fixture.planner",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.workflow.planner".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: task.permit.contract_hash.clone(),
        },
        Some(ArtifactOrigin {
            run_id: Some(task.run_id.clone()),
            task_id: Some(task.node.task_id.clone()),
            attempt_id: Some(task.permit.attempt_id.clone()),
            contract_hash: task.permit.contract_hash.clone(),
        }),
        vec![],
        now,
    )
    .unwrap();
    store
        .write_task_artifact(
            &task.permit,
            &artifact,
            LifecycleEventType::PlannerEvidenceNeedCreated,
            now,
        )
        .unwrap();
    ArtifactRef {
        artifact_id: artifact.artifact_id,
        kind: ArtifactKind::EvidenceNeed,
    }
}
