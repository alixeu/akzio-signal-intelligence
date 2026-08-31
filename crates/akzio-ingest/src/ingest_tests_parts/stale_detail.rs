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
                acquisition_mode: EvidenceAcquisitionMode::VerifiedSource,
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
                acquisition_mode: EvidenceAcquisitionMode::VerifiedSource,
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
                acquisition_mode: EvidenceAcquisitionMode::VerifiedSource,
            },
            &fixture(now),
            now,
        ),
        Err(EvidenceRuntimeError::SourceNotAllowed(
            EvidenceSource::Alpaca
        ))
    ));
}
