#[test]
fn outcome_worker_defers_without_consuming_retry_or_failing_completed_run() {
    let fixture = PolicyCommitFixture::memory();
    let outcome_payload: Outcome = fixture
        .store
        .read_artifact_payload(&fixture.outcome)
        .unwrap();
    let stored_schedule = fixture
        .store
        .artifact(&outcome_payload.schedule.artifact_id)
        .unwrap();
    let mut payload: OutcomeSchedule = fixture
        .store
        .read_artifact_payload(&stored_schedule)
        .unwrap();
    payload.outcome_id = akzio_domain::OutcomeId::new();
    payload.created_at = fixture.now + Duration::seconds(1);
    let schedule = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::OutcomeSchedule,
        &payload,
        outcome_schedule_source_refs(&payload),
        ArtifactLifecycle::Canonical,
        payload.created_at,
    );

    fixture
        .store
        .commit_outcome_schedule_with_worker(&fixture.permit, &schedule, fixture.now)
        .unwrap();
    let completed = fixture
        .store
        .workflow_snapshot(&fixture.run.run_id)
        .unwrap();
    assert_eq!(completed.status, WorkflowStatus::Completed);
    let worker = fixture
        .store
        .claim_next_task("outcome-worker", fixture.now, Duration::seconds(30))
        .unwrap()
        .expect("completed Paper run must keep its outcome worker claimable");
    assert_eq!(
        worker.node.recipe_id.as_str(),
        POST_TERMINAL_WORKER_RECIPE_ID
    );

    let ready_at = fixture.now + Duration::days(1);
    fixture
        .store
        .defer_task(&worker.permit, ready_at, fixture.now)
        .unwrap();
    let deferred = fixture
        .store
        .workflow_snapshot(&fixture.run.run_id)
        .unwrap();
    assert_eq!(deferred.status, WorkflowStatus::Completed);
    assert!(fixture
        .store
        .claim_next_task(
            "too-early-outcome-worker",
            ready_at - Duration::seconds(1),
            Duration::seconds(30),
        )
        .unwrap()
        .is_none());
    assert!(fixture
        .store
        .events_after(&fixture.run.run_id, 0, 100)
        .unwrap()
        .iter()
        .any(|event| event.event_type == LifecycleEventType::TaskDeferred.as_str()));
    assert!(!fixture
        .store
        .events_after(&fixture.run.run_id, 0, 100)
        .unwrap()
        .iter()
        .any(|event| {
            matches!(
                event.lifecycle_kind().unwrap(),
                LifecycleEventType::TaskRetryScheduled | LifecycleEventType::TaskRetryExhausted
            )
        }));

    let resumed = fixture
        .store
        .claim_next_task("resumed-outcome-worker", ready_at, Duration::seconds(30))
        .unwrap()
        .expect("deferred outcome worker must reactivate when due");
    fixture
        .store
        .finish_task(&resumed.permit, TaskStatus::Failed, ready_at)
        .unwrap();
    assert_eq!(
        fixture
            .store
            .workflow_snapshot(&fixture.run.run_id)
            .unwrap()
            .status,
        WorkflowStatus::Completed
    );
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn daemon_lease_validation_and_fenced_attempt_fail_closed() {
    let fixture = execution_commit_fixture();
    fixture
        .store
        .validate_daemon_lease(&fixture.lease, fixture.now)
        .unwrap();
    let successor_now = fixture.now + Duration::seconds(31);
    let successor = fixture
        .store
        .acquire_daemon_lease(
            "scheduler",
            "successor",
            successor_now,
            successor_now + Duration::seconds(30),
        )
        .unwrap()
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .validate_daemon_lease(&fixture.lease, successor_now),
        Err(StoreError::SchedulerFenced(_))
    ));
    fixture
        .store
        .validate_daemon_lease(&successor, successor_now)
        .unwrap();

    let receipt = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::OrderReceipt,
        &serde_json::json!({"receipt": true}),
        vec![],
        ArtifactLifecycle::Canonical,
        successor_now,
    );
    assert!(matches!(
        fixture.store.commit_fenced_attempt(
            &fixture.lease,
            &fixture.permit,
            std::slice::from_ref(&receipt),
            TaskStatus::Succeeded,
            successor_now,
        ),
        Err(StoreError::SchedulerFenced(_))
    ));
    assert!(matches!(
        fixture.store.artifact(&receipt.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
    fixture.store.validate_task_permit(&fixture.permit).unwrap();

    fixture
        .store
        .commit_fenced_attempt(
            &successor,
            &fixture.permit,
            std::slice::from_ref(&receipt),
            TaskStatus::Succeeded,
            successor_now,
        )
        .unwrap();
    assert_eq!(
        fixture.store.artifact(&receipt.artifact_id).unwrap().kind,
        ArtifactKind::OrderReceipt
    );
}
