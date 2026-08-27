#[test]
fn task_artifact_lifecycle_matrix_is_enforced_without_partial_writes() {
    for purpose in [
        RunPurpose::Debug,
        RunPurpose::PaperDryRun,
        RunPurpose::Replay,
        RunPurpose::Shadow,
    ] {
        for lifecycle in [ArtifactLifecycle::Ephemeral, ArtifactLifecycle::Canonical] {
            let fixture = task_artifact_fixture(purpose);
            let artifact = lifecycle_test_artifact(&fixture, lifecycle, "rejected");
            let event_count = fixture
                .store
                .events_after(&fixture.run.run_id, 0, 100)
                .unwrap()
                .len();

            assert!(matches!(
                fixture.store.write_task_artifact(
                    &fixture.permit,
                    &artifact,
                    LifecycleEventType::ClaimCreated,
                    fixture.now,
                ),
                Err(StoreError::InvalidTaskArtifactLifecycle { purpose: actual, lifecycle: rejected })
                    if actual == purpose && rejected == lifecycle
            ));
            assert!(matches!(
                fixture.store.artifact(&artifact.artifact_id),
                Err(StoreError::MissingArtifact(_))
            ));
            assert_eq!(
                fixture
                    .store
                    .events_after(&fixture.run.run_id, 0, 100)
                    .unwrap()
                    .len(),
                event_count
            );
            fixture.store.verify_integrity().unwrap();
        }
    }

    for purpose in [
        RunPurpose::Debug,
        RunPurpose::Paper,
        RunPurpose::PaperDryRun,
        RunPurpose::Replay,
        RunPurpose::Shadow,
    ] {
        let fixture = task_artifact_fixture(purpose);
        let artifact = lifecycle_test_artifact(&fixture, ArtifactLifecycle::RunScoped, "accepted");
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &artifact,
                LifecycleEventType::ClaimCreated,
                fixture.now,
            )
            .unwrap();
        assert_eq!(
            fixture.store.artifact(&artifact.artifact_id).unwrap(),
            artifact
        );
        fixture.store.verify_integrity().unwrap();
    }

    let fixture = task_artifact_fixture(RunPurpose::Paper);
    let artifact = lifecycle_test_artifact(&fixture, ArtifactLifecycle::Canonical, "paper");
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &artifact,
            LifecycleEventType::ClaimCreated,
            fixture.now,
        )
        .unwrap();
    assert_eq!(
        fixture.store.artifact(&artifact.artifact_id).unwrap(),
        artifact
    );
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn attempt_commit_lifecycle_rejection_is_atomic_and_paper_canonical_is_allowed() {
    for purpose in [
        RunPurpose::Debug,
        RunPurpose::PaperDryRun,
        RunPurpose::Replay,
        RunPurpose::Shadow,
    ] {
        let fixture = task_artifact_fixture(purpose);
        let artifact = lifecycle_test_artifact(&fixture, ArtifactLifecycle::Canonical, "rejected");
        let event_count = fixture
            .store
            .events_after(&fixture.run.run_id, 0, 100)
            .unwrap()
            .len();

        assert!(matches!(
            fixture.store.commit_attempt(
                &fixture.permit,
                std::slice::from_ref(&artifact),
                TaskStatus::Succeeded,
                fixture.now,
            ),
            Err(StoreError::InvalidTaskArtifactLifecycle { purpose: actual, lifecycle: ArtifactLifecycle::Canonical })
                if actual == purpose
        ));
        assert!(matches!(
            fixture.store.artifact(&artifact.artifact_id),
            Err(StoreError::MissingArtifact(_))
        ));
        assert!(matches!(
            fixture
                .store
                .committed_task_outputs(&fixture.run.run_id, &fixture.permit.task_id),
            Err(StoreError::CommittedOutputTask { .. })
        ));
        assert_eq!(
            fixture
                .store
                .events_after(&fixture.run.run_id, 0, 100)
                .unwrap()
                .len(),
            event_count
        );
        assert_eq!(
            fixture
                .store
                .workflow_snapshot(&fixture.run.run_id)
                .unwrap()
                .tasks[0]
                .status,
            TaskStatus::Running
        );
        fixture.store.verify_integrity().unwrap();
    }

    let fixture = task_artifact_fixture(RunPurpose::Paper);
    let artifact = lifecycle_test_artifact(&fixture, ArtifactLifecycle::Canonical, "paper");
    fixture
        .store
        .commit_attempt(
            &fixture.permit,
            std::slice::from_ref(&artifact),
            TaskStatus::Succeeded,
            fixture.now,
        )
        .unwrap();
    assert_eq!(
        fixture
            .store
            .committed_task_outputs(&fixture.run.run_id, &fixture.permit.task_id)
            .unwrap(),
        vec![artifact]
    );
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn stale_permit_rejects_before_task_artifact_lifecycle() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    let stale = fixture.permit.clone();
    fixture
        .store
        .recover_expired_tasks(fixture.now + Duration::seconds(31))
        .unwrap();
    let artifact = lifecycle_test_artifact(&fixture, ArtifactLifecycle::Canonical, "stale");

    assert!(matches!(
        fixture.store.write_task_artifact(
            &stale,
            &artifact,
            LifecycleEventType::ClaimCreated,
            fixture.now,
        ),
        Err(StoreError::StalePermit(_))
    ));
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn bootstrap_freeze_state_remains_outside_task_artifact_firewall() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    let freeze = fixture
        .store
        .write_freeze_state(true, "lifecycle firewall test", fixture.now)
        .unwrap();

    assert_eq!(freeze.kind, ArtifactKind::FreezeState);
    assert_eq!(freeze.lifecycle, ArtifactLifecycle::Canonical);
    assert_eq!(fixture.store.artifact(&freeze.artifact_id).unwrap(), freeze);
    fixture.store.verify_integrity().unwrap();
}
