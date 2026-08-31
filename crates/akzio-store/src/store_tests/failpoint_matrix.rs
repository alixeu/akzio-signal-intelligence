#[test]
fn terminal_commit_ack_loss_cannot_duplicate_canonical_outputs() {
    let fixture = task_artifact_fixture_with_retry(RunPurpose::Debug, 2);
    let output = lifecycle_test_artifact(&fixture, ArtifactLifecycle::RunScoped, "terminal");

    fixture
        .store
        .commit_attempt(
            &fixture.permit,
            std::slice::from_ref(&output),
            TaskStatus::Succeeded,
            fixture.now,
        )
        .unwrap();
    assert!(fixture
        .store
        .commit_attempt(
            &fixture.permit,
            std::slice::from_ref(&output),
            TaskStatus::Succeeded,
            fixture.now + Duration::milliseconds(1),
        )
        .is_err());
    assert_eq!(
        fixture
            .store
            .recover_expired_tasks(fixture.now + Duration::hours(1))
            .unwrap(),
        0
    );

    let proof = fixture
        .store
        .current_succeeded_attempt(&fixture.run.run_id, &fixture.permit.task_id)
        .unwrap();
    assert_eq!(proof.outputs.len(), 1);
    assert_eq!(proof.outputs[0].artifact_id, output.artifact_id);
    let succeeded = fixture
        .store
        .events_after(&fixture.run.run_id, 0, 100)
        .unwrap()
        .into_iter()
        .filter(|event| event.event_type == LifecycleEventType::TaskSucceeded.as_str())
        .count();
    assert_eq!(succeeded, 1);
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn heartbeat_loss_recovers_once_and_fences_the_expired_permit() {
    let fixture = task_artifact_fixture_with_retry(RunPurpose::Debug, 2);
    assert_eq!(
        fixture
            .store
            .recover_expired_tasks(fixture.now + Duration::seconds(31))
            .unwrap(),
        1
    );
    assert_eq!(
        fixture
            .store
            .recover_expired_tasks(fixture.now + Duration::seconds(31))
            .unwrap(),
        0
    );
    assert!(matches!(
        fixture.store.finish_task(
            &fixture.permit,
            TaskStatus::Succeeded,
            fixture.now + Duration::seconds(32),
        ),
        Err(StoreError::StalePermit(_))
    ));
    let recovered = fixture
        .store
        .claim_next_task(
            "recovery-worker",
            fixture.now + Duration::seconds(32),
            Duration::seconds(30),
        )
        .unwrap()
        .unwrap();
    assert_ne!(recovered.permit.attempt_id, fixture.permit.attempt_id);
    assert!(recovered.permit.epoch > fixture.permit.epoch);
    fixture.store.verify_integrity().unwrap();
}
