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
struct FixtureTransport {
    evidence: AcquiredEvidence,
}

impl GovernedEvidenceTransport for FixtureTransport {
    fn acquire(
        &self,
        _source: EvidenceSource,
        _resource: &str,
    ) -> Result<AcquiredEvidence, EvidenceAdapterError> {
        Ok(self.evidence.clone())
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

fn transport() -> FixtureTransport {
    let observed_at = Utc::now();
    FixtureTransport {
        evidence: AcquiredEvidence {
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
        },
    }
}

fn assert_governed_adapter<A: EvidenceAdapter>(
    adapter: A,
    source: EvidenceSource,
    other: EvidenceSource,
) {
    let response = adapter
        .acquire(&EvidenceRequest {
            source,
            resource: "resource".to_owned(),
            max_age: Duration::seconds(30),
        })
        .unwrap();
    assert_eq!(response.normalized["adapter"], source.as_str());
    assert_eq!(response.normalized["resource"], "resource");
    assert_eq!(response.normalized["payload"]["fixture"], true);
    assert!(matches!(
        adapter.acquire(&EvidenceRequest {
            source: other,
            resource: "resource".to_owned(),
            max_age: Duration::seconds(30),
        }),
        Err(EvidenceAdapterError::SourceMismatch)
    ));
}

#[test]
fn governed_adapters_are_source_typed_and_local_transport_only() {
    assert_governed_adapter(
        AlpacaEvidenceAdapter::new(transport()),
        EvidenceSource::Alpaca,
        EvidenceSource::SecEdgar,
    );
    assert_governed_adapter(
        SecEdgarEvidenceAdapter::new(transport()),
        EvidenceSource::SecEdgar,
        EvidenceSource::Fred,
    );
    assert_governed_adapter(
        FredEvidenceAdapter::new(transport()),
        EvidenceSource::Fred,
        EvidenceSource::NewsWeb,
    );
    assert_governed_adapter(
        NewsWebEvidenceAdapter::new(transport()),
        EvidenceSource::NewsWeb,
        EvidenceSource::Alpaca,
    );
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

#[tokio::test]
async fn sync_and_async_materialization_preserve_confidence_semantics() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    install_run(&store, now, 1);
    let claimed = store
        .claim_next_task("evidence-parity-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap();
    let need = evidence_need(&store, &claimed, now);
    let request = EvidenceRequest {
        source: EvidenceSource::Alpaca,
        resource: "quote".to_owned(),
        max_age: Duration::seconds(30),
    };
    let adapter = ParityAdapter {
        evidence: AcquiredEvidence {
            raw: br#"{"fixture":true}"#.to_vec(),
            media_type: "application/json".to_owned(),
            source_uri: "fixture://alpaca/quote".to_owned(),
            observed_at: now,
            normalized: serde_json::json!({"symbol": "QQQ", "price": 1}),
            provenance: EvidenceProvenance {
                document_id: Some("fixture-parity".to_owned()),
                published_at: None,
                observed_at: now,
                revision: Some("1".to_owned()),
                source_uri: "fixture://alpaca/quote".to_owned(),
                dedupe_key: "fixture:parity".to_owned(),
                citations: vec![],
            },
            quality: EvidenceQuality {
                completeness_ppm: 250_000,
                citations_complete: false,
                normalized: true,
            },
        },
    };
    let runtime = EvidenceRuntime::new(store.clone(), [EvidenceSource::Alpaca]);

    let synchronous = runtime
        .acquire_and_normalize(&claimed.permit, &need, &request, &adapter, now)
        .unwrap();
    let asynchronous = runtime
        .acquire_and_normalize_async(&claimed.permit, &need, &request, &adapter, now)
        .await
        .unwrap();

    assert_eq!(synchronous.raw.provenance.confidence_ppm, 1_000_000);
    assert_eq!(asynchronous.raw.provenance.confidence_ppm, 1_000_000);
    assert_eq!(synchronous.normalized.provenance.confidence_ppm, 1_000_000);
    assert_eq!(asynchronous.normalized.provenance.confidence_ppm, 250_000);
    let payload: NormalizedEvidencePayload =
        serde_json::from_slice(&store.read_blob(&synchronous.normalized.blob).unwrap()).unwrap();
    assert_eq!(payload.quality.completeness_ppm, 250_000);
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
    let need = evidence_need(&store, &claimed, now);
    let events_before = store.events_after(&run_id, 0, 10).unwrap();
    let runtime = EvidenceRuntime::new(store.clone(), [EvidenceSource::Alpaca]);
    let sealed = runtime
        .acquire_and_normalize(
            &claimed.permit,
            &need,
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
    let mut expected_source_refs = vec![
        ArtifactRef {
            artifact_id: sealed.raw.artifact_id.clone(),
            kind: ArtifactKind::RawEvidence,
        },
        need.clone(),
    ];
    expected_source_refs.sort();
    assert_eq!(sealed.normalized.source_refs, expected_source_refs);
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
    assert_eq!(
        store
            .artifacts_referencing(&need.artifact_id, Some(ArtifactKind::NormalizedEvidence))
            .unwrap(),
        vec![sealed.normalized.clone()]
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
    let claimed = store
        .claim_next_task("evidence-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap();
    let need = evidence_need(&store, &claimed, now);
    let permit = claimed.permit;
    let events_before = store.events_after(&run_id, 0, 10).unwrap();
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
                provenance: EvidenceProvenance {
                    document_id: Some("fixture-stale".to_owned()),
                    published_at: None,
                    observed_at: now - Duration::minutes(5),
                    revision: Some("1".to_owned()),
                    source_uri: "fixture://alpaca/quote".to_owned(),
                    dedupe_key: "fixture:alpaca:stale".to_owned(),
                    citations: vec![],
                },
                quality: EvidenceQuality::default(),
            },
        )],
    );
    let runtime = EvidenceRuntime::new(store.clone(), [EvidenceSource::Alpaca]);
    assert!(matches!(
        runtime.acquire_and_normalize(
            &permit,
            &need,
            &EvidenceRequest {
                source: EvidenceSource::Alpaca,
                resource: "bars".to_owned(),
                max_age: Duration::seconds(30),
            },
            &stale,
            now,
        ),
        Err(EvidenceRuntimeError::InvalidEvidenceNeed)
    ));
    assert!(matches!(
        runtime.acquire_and_normalize(
            &permit,
            &need,
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
    assert_eq!(store.events_after(&run_id, 0, 10).unwrap(), events_before);
    assert!(matches!(
        EvidenceRuntime::new(store, [EvidenceSource::Fred]).acquire_and_normalize(
            &permit,
            &need,
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
    let need = evidence_need(&store, &first, now);
    let runtime = EvidenceRuntime::new(store.clone(), [EvidenceSource::Alpaca]);
    let sealed = runtime
        .acquire_and_normalize(
            &first.permit,
            &need,
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

#[test]
fn alpaca_paper_transport_is_endpoint_and_resource_fenced() {
    assert!(
        AlpacaPaperEvidenceTransport::new("https://api.alpaca.markets", "key", "secret", None,)
            .is_err()
    );
    assert_eq!(
        AlpacaPaperEvidenceTransport::path_for("bars:QQQ:1d").unwrap(),
        "/v2/stocks/QQQ/bars?timeframe=1Day&limit=1&adjustment=all"
    );
    assert_eq!(
        AlpacaPaperEvidenceTransport::path_for("bars:QQQ:1d:2026-08-01:6").unwrap(),
        "/v2/stocks/QQQ/bars?timeframe=1Day&limit=6&adjustment=all&start=2026-08-01"
    );
    assert_eq!(
        AlpacaPaperEvidenceTransport::path_for("observer.qqq_history:1d:2026-08-20").unwrap(),
        "/v2/stocks/QQQ/bars?timeframe=5Min&limit=1000&adjustment=all&start=2026-08-20"
    );
    assert_eq!(
        AlpacaPaperEvidenceTransport::path_for("observer.qqq_history:3m:2026-05-12").unwrap(),
        "/v2/stocks/QQQ/bars?timeframe=1Day&limit=1000&adjustment=all&start=2026-05-12"
    );
    assert!(AlpacaPaperEvidenceTransport::path_for("observer.qqq_history:all:2026-05-12").is_err());
    assert!(AlpacaPaperEvidenceTransport::path_for("bars:QQQ:1d:2026-08-01:33").is_err());
    assert!(AlpacaPaperEvidenceTransport::path_for("bars:SPY:1d").is_err());
    assert!(AlpacaPaperEvidenceTransport::path_for("bars:QQQ:5m").is_err());
    assert!(AlpacaPaperEvidenceTransport::path_for("https://example.com").is_err());

    let transport = AlpacaPaperEvidenceTransport::new(
        "https://paper-api.alpaca.markets",
        "key",
        "secret",
        Some(AlpacaMarketDataFeed::Iex),
    )
    .unwrap();
    assert_eq!(
        transport.base_url_for("paper.account"),
        "https://paper-api.alpaca.markets"
    );
    assert_eq!(
        transport.base_url_for("paper.clock"),
        "https://paper-api.alpaca.markets"
    );
    assert_eq!(
        transport.base_url_for("paper.quotes"),
        "https://data.alpaca.markets"
    );
    assert_eq!(
        transport.base_url_for("bars:QQQ:1d"),
        "https://data.alpaca.markets"
    );
    assert_eq!(
        transport.configured_path_for("paper.quotes").unwrap(),
        "/v2/stocks/quotes/latest?symbols=TQQQ,QQQ,SOXX,SOXL&feed=iex"
    );
    assert_eq!(
        transport.configured_path_for("bars:QQQ:1d").unwrap(),
        "/v2/stocks/QQQ/bars?timeframe=1Day&limit=1&adjustment=all&feed=iex"
    );
}

#[tokio::test]
async fn native_web_transport_requires_allowlisted_citations() {
    let client = ModelClient::Fixture(serde_json::json!({
        "output_text": "DFII10 evidence",
        "citations": [{
            "url": "https://fred.stlouisfed.org/series/DFII10",
            "title": "FRED",
            "text": "real yield"
        }]
    }));
    let transport = ModelNativeWebEvidenceTransport::for_source(client, EvidenceSource::Fred);
    let evidence = transport
        .acquire(&EvidenceRequest {
            source: EvidenceSource::Fred,
            resource: "series:DFII10".to_owned(),
            max_age: Duration::minutes(5),
        })
        .await
        .unwrap();
    assert_eq!(
        evidence.provenance.source_uri,
        "https://fred.stlouisfed.org/series/DFII10"
    );
    assert!(evidence.provenance.citations.is_empty());
    assert_eq!(evidence.quality.completeness_ppm, 0);
    assert!(!evidence.quality.citations_complete);
}

#[test]
fn governed_resource_schema_bounds_sources_windows_and_assets() {
    assert_eq!(
        GovernedResource::parse(EvidenceSource::Alpaca, "bars:QQQ:1d:2026-08-01:6").unwrap(),
        GovernedResource::AlpacaBars {
            asset: Asset::Qqq,
            start: Some(NaiveDate::from_ymd_opt(2026, 8, 1).unwrap()),
            limit: 6,
        }
    );
    assert!(GovernedResource::parse(EvidenceSource::Alpaca, "bars:SPY:1d").is_err());
    assert!(GovernedResource::parse(EvidenceSource::Alpaca, "bars:QQQ:5m").is_err());
    assert!(
        GovernedResource::parse(EvidenceSource::Alpaca, "observer.qqq_history:1d:2026-08-20")
            .is_err()
    );
    assert!(
        GovernedResource::parse(EvidenceSource::Fred, "series:DFII10:2026-08-01:2028-08-01")
            .is_err()
    );
    assert_eq!(
        GovernedResource::parse(EvidenceSource::NewsWeb, "news:semiconductor supply chain")
            .unwrap(),
        GovernedResource::NewsWeb {
            query: "semiconductor supply chain".to_owned(),
        }
    );
}

#[test]
fn daily_bar_quality_gate_rejects_missing_ohlcv_weekends_and_duplicates() {
    let valid = serde_json::json!({
        "bars": [
            {"t":"2026-08-10T20:00:00Z","o":100.0,"h":105.0,"l":99.0,"c":103.0,"v":1000}
        ]
    });
    validate_daily_bar_payload(&valid).unwrap();

    let mut missing = valid;
    missing["bars"][0].as_object_mut().unwrap().remove("v");
    assert!(validate_daily_bar_payload(&missing).is_err());

    let weekend = serde_json::json!({
        "bars": [
            {"t":"2026-08-09T20:00:00Z","o":100.0,"h":105.0,"l":99.0,"c":103.0,"v":1000}
        ]
    });
    assert!(validate_daily_bar_payload(&weekend).is_err());

    let duplicate = serde_json::json!({
        "bars": [
            {"t":"2026-08-10T20:00:00Z","o":100,"h":105,"l":99,"c":103,"v":1000},
            {"t":"2026-08-10T21:00:00Z","o":100,"h":106,"l":98,"c":104,"v":1100}
        ]
    });
    assert!(validate_daily_bar_payload(&duplicate).is_err());
}
