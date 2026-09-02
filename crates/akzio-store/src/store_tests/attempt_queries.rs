#[test]
fn attempt_queries_preserve_retry_recovery_lineage_and_exact_event_scope() {
    let fixture = task_artifact_fixture_with_retry(RunPurpose::Debug, 3);
    let first = fixture.permit.clone();
    assert_eq!(
        fixture
            .store
            .retry_task(&first, fixture.now, fixture.now)
            .unwrap(),
        RetryTaskResult::Requeued
    );

    let second = fixture
        .store
        .claim_next_task(
            "retry-worker",
            fixture.now + Duration::seconds(1),
            Duration::seconds(30),
        )
        .unwrap()
        .unwrap()
        .permit;
    assert_eq!(
        fixture
            .store
            .attempt_relation(&second.attempt_id)
            .unwrap()
            .map(|relation| (relation.parent_attempt_id, relation.relation)),
        Some((first.attempt_id.clone(), AttemptRelationKind::Retry))
    );

    assert_eq!(
        fixture
            .store
            .recover_expired_tasks(fixture.now + Duration::seconds(32))
            .unwrap(),
        1
    );
    let third = fixture
        .store
        .claim_next_task(
            "recovery-worker",
            fixture.now + Duration::seconds(33),
            Duration::seconds(30),
        )
        .unwrap()
        .unwrap()
        .permit;
    assert_eq!(
        fixture
            .store
            .attempt_relation(&third.attempt_id)
            .unwrap()
            .map(|relation| (relation.parent_attempt_id, relation.relation)),
        Some((second.attempt_id, AttemptRelationKind::Recovery))
    );
    assert_eq!(fixture.store.attempt_relation(&first.attempt_id).unwrap(), None);
    assert_eq!(fixture.store.attempt_relation(&AttemptId::new()).unwrap(), None);

    let events = fixture
        .store
        .attempt_events(&fixture.run.run_id, &third.task_id, &third.attempt_id)
        .unwrap();
    assert!(events.windows(2).all(|events| events[0].cursor < events[1].cursor));
    assert!(events.iter().all(|event| {
        event.run_id == fixture.run.run_id
            && event.task_id.as_ref() == Some(&third.task_id)
            && event.attempt_id.as_ref() == Some(&third.attempt_id)
    }));
    assert!(events.iter().any(|event| {
        event.lifecycle_kind().unwrap() == LifecycleEventType::AttemptRelationCreated
    }));

    let foreign_run = RunId::new();
    let foreign_task = TaskId::new();
    let foreign_attempt = AttemptId::new();
    for empty in [
        fixture
            .store
            .attempt_events(&foreign_run, &third.task_id, &third.attempt_id)
            .unwrap(),
        fixture
            .store
            .attempt_events(&fixture.run.run_id, &foreign_task, &third.attempt_id)
            .unwrap(),
        fixture
            .store
            .attempt_events(&fixture.run.run_id, &third.task_id, &foreign_attempt)
            .unwrap(),
    ] {
        assert!(empty.is_empty());
    }

    let changes_before = fixture.store.connection().unwrap().total_changes();
    fixture
        .store
        .attempt_relation(&third.attempt_id)
        .unwrap();
    fixture
        .store
        .attempt_events(&fixture.run.run_id, &third.task_id, &third.attempt_id)
        .unwrap();
    assert_eq!(fixture.store.connection().unwrap().total_changes(), changes_before);
}
