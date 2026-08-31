#[test]
fn release_bundle_reads_only_store_state_and_is_deterministic() {
    let fixture = approved_execution_commit_fixture(
        MoneyMicros::from_usd_cents(100_000),
        Duration::hours(8),
    );
    let expectations = ReleaseEvidenceExpectations::default();
    let first = fixture
        .store
        .release_evidence_bundle(&fixture.permit.run_id, &expectations)
        .unwrap();
    let second = fixture
        .store
        .release_evidence_bundle(&fixture.permit.run_id, &expectations)
        .unwrap();
    assert_eq!(first.bundle_hash, second.bundle_hash);
    assert_eq!(first.body.purpose, RunPurpose::Paper);
    assert!(first.body.runtime.is_some());
    assert!(first.body.workflow.is_some());
    assert_eq!(first.status, akzio_domain::ReleaseEvidenceStatus::NotApprovable);
}

#[test]
fn release_bundle_reports_drift_and_never_serializes_account_or_credentials() {
    let fixture = approved_execution_commit_fixture(
        MoneyMicros::from_usd_cents(100_000),
        Duration::hours(8),
    );
    let bundle = fixture
        .store
        .release_evidence_bundle(
            &fixture.permit.run_id,
            &ReleaseEvidenceExpectations {
                config_hash: Some(ContentHash::of_bytes(b"different-config")),
                workflow_hash: Some(ContentHash::of_bytes(b"different-workflow")),
                broker_account_fingerprint: Some(ContentHash::of_bytes(b"different-account")),
                daemon_owner_id: Some("different-owner".to_owned()),
                daemon_epoch: Some(fixture.lease.epoch.saturating_add(1)),
            },
        )
        .unwrap();
    for issue in [
        akzio_domain::ReleaseEvidenceIssue::ConfigHashDrift,
        akzio_domain::ReleaseEvidenceIssue::WorkflowHashDrift,
        akzio_domain::ReleaseEvidenceIssue::BrokerAccountMismatch,
        akzio_domain::ReleaseEvidenceIssue::StaleDaemonEpoch,
    ] {
        assert!(bundle.issues.contains(&issue));
    }
    let json = serde_json::to_string(&bundle).unwrap();
    assert!(!json.contains("fixture-account"));
    assert!(!json.contains("api_key"));
    assert!(!json.contains("api_secret"));
}

#[test]
fn noncanonical_store_run_cannot_materialize_an_approvable_bundle() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    let bundle = fixture
        .store
        .release_evidence_bundle(&fixture.run.run_id, &ReleaseEvidenceExpectations::default())
        .unwrap();
    assert_eq!(
        bundle.status,
        akzio_domain::ReleaseEvidenceStatus::NotApprovable
    );
    assert!(bundle.issues.iter().any(|issue| matches!(
        issue,
        akzio_domain::ReleaseEvidenceIssue::NonCanonicalRun { .. }
    )));
}
