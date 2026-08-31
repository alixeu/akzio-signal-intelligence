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

    let inventory_before = store.storage_inventory().unwrap();
    let acquired = runtime
        .acquire_validated_async(&claimed.permit, &need, &request, &adapter, now)
        .await
        .unwrap();
    let inventory_after_acquire = store.storage_inventory().unwrap();
    assert_eq!(inventory_after_acquire.blob_count, inventory_before.blob_count);
    assert_eq!(
        inventory_after_acquire.unreferenced_blob_count,
        inventory_before.unreferenced_blob_count
    );
    let inspected = runtime
        .materialize_validated(&claimed.permit, &need, &request, acquired, now)
        .unwrap();
    assert_eq!(inspected.raw.kind, ArtifactKind::RawEvidence);
    assert_eq!(inspected.normalized.kind, ArtifactKind::NormalizedEvidence);

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
fn provenance_rejects_quote_mismatch() {
    let mut acquired = fixture_acquired();
    acquired.provenance.citations[0].quote = "different".to_owned();

    assert!(matches!(
        acquired.provenance.validate(
            &acquired.raw,
            &acquired.source_uri,
            acquired.observed_at,
        ),
        Err(EvidenceRuntimeError::InvalidCitation)
    ));
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
