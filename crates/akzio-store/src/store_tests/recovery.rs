#[test]
fn retry_attempts_close_started_turns_and_preserve_attempt_order() {
    let fixture = task_artifact_fixture_with_retry(RunPurpose::Debug, 2);
    fixture
        .store
        .append_task_event(
            &fixture.permit,
            LifecycleEventType::AgentTurnStarted,
            fixture.now,
        )
        .unwrap();
    assert_eq!(
        fixture
            .store
            .retry_task(&fixture.permit, fixture.now, fixture.now)
            .unwrap(),
        RetryTaskResult::Requeued
    );

    let second = fixture
        .store
        .claim_next_task(
            "lifecycle-worker-2",
            fixture.now + Duration::seconds(1),
            Duration::seconds(30),
        )
        .unwrap()
        .unwrap();
    assert_ne!(fixture.permit.attempt_id, second.permit.attempt_id);
    fixture
        .store
        .append_task_event(
            &second.permit,
            LifecycleEventType::AgentTurnStarted,
            fixture.now + Duration::seconds(1),
        )
        .unwrap();
    let second_fixture = TaskArtifactFixture {
        _root: fixture._root,
        store: fixture.store,
        run: fixture.run,
        permit: second.permit,
        now: fixture.now + Duration::seconds(1),
    };
    let turn = agent_turn_artifact(&second_fixture, "retry-completed");
    second_fixture
        .store
        .write_task_artifact(
            &second_fixture.permit,
            &turn,
            LifecycleEventType::AgentTurnCompleted,
            second_fixture.now,
        )
        .unwrap();
    second_fixture
        .store
        .finish_task(
            &second_fixture.permit,
            TaskStatus::Succeeded,
            second_fixture.now,
        )
        .unwrap();

    let events = second_fixture
        .store
        .events_after(&second_fixture.run.run_id, 0, 100)
        .unwrap();
    let lifecycle: Vec<_> = events
        .iter()
        .filter(|event| {
            matches!(
                event.lifecycle_kind().unwrap(),
                LifecycleEventType::AgentTurnStarted
                    | LifecycleEventType::AgentTurnCompleted
                    | LifecycleEventType::TaskRetryScheduled
                    | LifecycleEventType::TaskSucceeded
            )
        })
        .collect();
    assert_eq!(
        lifecycle
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>(),
        vec![
            LifecycleEventType::AgentTurnStarted.as_str(),
            LifecycleEventType::TaskRetryScheduled.as_str(),
            LifecycleEventType::AgentTurnStarted.as_str(),
            LifecycleEventType::AgentTurnCompleted.as_str(),
            LifecycleEventType::TaskSucceeded.as_str(),
        ]
    );
    assert_eq!(
        lifecycle[0].attempt_id,
        Some(fixture.permit.attempt_id.clone())
    );
    assert_eq!(
        lifecycle[2].attempt_id,
        Some(second_fixture.permit.attempt_id.clone())
    );
    assert_eq!(
        lifecycle[3].attempt_id,
        Some(second_fixture.permit.attempt_id.clone())
    );
    second_fixture.store.verify_integrity().unwrap();
}

#[test]
fn recovery_and_cancel_close_unfinished_agent_turns() {
    let recovered = task_artifact_fixture(RunPurpose::Debug);
    recovered
        .store
        .append_task_event(
            &recovered.permit,
            LifecycleEventType::AgentTurnStarted,
            recovered.now,
        )
        .unwrap();
    assert_eq!(
        recovered
            .store
            .recover_expired_tasks(recovered.now + Duration::seconds(31))
            .unwrap(),
        1
    );
    let recovery_events = recovered
        .store
        .events_after(&recovered.run.run_id, 0, 100)
        .unwrap();
    assert!(recovery_events.iter().any(|event| {
        matches!(
            event.lifecycle_kind().unwrap(),
            LifecycleEventType::TaskRecovered | LifecycleEventType::TaskRecoveryExhausted
        )
    }));
    recovered.store.verify_integrity().unwrap();

    let cancelled = task_artifact_fixture(RunPurpose::Debug);
    cancelled
        .store
        .append_task_event(
            &cancelled.permit,
            LifecycleEventType::AgentTurnStarted,
            cancelled.now,
        )
        .unwrap();
    cancelled
        .store
        .finish_task(&cancelled.permit, TaskStatus::Cancelled, cancelled.now)
        .unwrap();
    assert_eq!(
        cancelled
            .store
            .workflow_snapshot(&cancelled.run.run_id)
            .unwrap()
            .tasks[0]
            .status,
        TaskStatus::Cancelled
    );
    cancelled.store.verify_integrity().unwrap();
}

#[test]
fn context_lifecycle_validator_rejects_wrong_kind_and_preserves_legacy_manifest() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    let wrong = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::Decision,
        &serde_json::json!({"wrong": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    let before = fixture
        .store
        .events_after(&fixture.run.run_id, 0, 100)
        .unwrap()
        .len();
    assert!(matches!(
        fixture.store.write_task_artifact(
            &fixture.permit,
            &wrong,
            LifecycleEventType::ContextManifestCreated,
            fixture.now,
        ),
        Err(StoreError::Integrity(_))
    ));
    assert_eq!(
        fixture
            .store
            .events_after(&fixture.run.run_id, 0, 100)
            .unwrap()
            .len(),
        before
    );
    assert!(matches!(
        fixture.store.artifact(&wrong.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));

    let manifest = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ContextManifest,
        &serde_json::json!({"manifest": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &manifest,
            LifecycleEventType::ContextManifestCreated,
            fixture.now,
        )
        .unwrap();
    fixture
        .store
        .commit_attempt(
            &fixture.permit,
            std::slice::from_ref(&manifest),
            TaskStatus::Succeeded,
            fixture.now,
        )
        .unwrap();
    let proof = fixture
        .store
        .current_succeeded_attempt(&fixture.run.run_id, &fixture.permit.task_id)
        .unwrap();
    assert_eq!(
        proof.context_manifest,
        Some(ArtifactRef {
            artifact_id: manifest.artifact_id,
            kind: ArtifactKind::ContextManifest,
        })
    );
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn gate_lifecycle_validator_enforces_event_kind_and_legacy_aliases() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    let wrong = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::Decision,
        &serde_json::json!({"wrong": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    let before = fixture
        .store
        .events_after(&fixture.run.run_id, 0, 100)
        .unwrap()
        .len();
    assert!(matches!(
        fixture.store.write_task_artifact(
            &fixture.permit,
            &wrong,
            LifecycleEventType::ExecutionPlanCreated,
            fixture.now,
        ),
        Err(StoreError::Integrity(_))
    ));
    assert_eq!(
        fixture
            .store
            .events_after(&fixture.run.run_id, 0, 100)
            .unwrap()
            .len(),
        before
    );
    assert!(matches!(
        fixture.store.artifact(&wrong.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));

    let context = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ExecutionContext,
        &serde_json::json!({"context": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &context,
            LifecycleEventType::ExecutionContextCreated,
            fixture.now,
        )
        .unwrap();

    let verdict = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ExecutionVerdict,
        &serde_json::json!({"verdict": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &verdict,
            LifecycleEventType::ExecutionVerdictCreated,
            fixture.now,
        )
        .unwrap();
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn gate_lifecycle_validator_rejects_forged_origin() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    let foreign_run = RunId::new();
    let forged = Artifact::new(
        ArtifactKind::ExecutionPlan,
        fixture
            .store
            .put_json(&serde_json::json!({"plan": true}))
            .unwrap(),
        "fixture.plan",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "fixture".to_owned(),
            observed_at: None,
            retrieved_at: fixture.now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: fixture.permit.contract_hash.clone(),
        },
        Some(ArtifactOrigin {
            run_id: Some(foreign_run),
            task_id: Some(fixture.permit.task_id.clone()),
            attempt_id: Some(fixture.permit.attempt_id.clone()),
            contract_hash: fixture.permit.contract_hash.clone(),
        }),
        vec![],
        fixture.now,
    )
    .unwrap();
    {
        let mut connection = fixture.store.connection.lock().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        insert_artifact(&transaction, &forged).unwrap();
        transaction
            .execute(
                r#"INSERT INTO rebuild_events
                       (run_id, task_id, attempt_id, event_type, artifact_id, created_at)
                       VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
                params![
                    fixture.run.run_id.0,
                    fixture.permit.task_id.0,
                    fixture.permit.attempt_id.0,
                    LifecycleEventType::ExecutionPlanCreated.as_str(),
                    forged.artifact_id.0.as_str(),
                    fixture.now.to_rfc3339(),
                ],
            )
            .unwrap();
        transaction.commit().unwrap();
    }
    assert!(matches!(
        fixture.store.events_after(&fixture.run.run_id, 0, 100),
        Err(StoreError::Integrity(message))
            if message.contains("origin")
    ));
    assert!(matches!(
        fixture.store.verify_integrity(),
        Err(StoreError::Integrity(message))
            if message.contains("origin")
    ));
}
