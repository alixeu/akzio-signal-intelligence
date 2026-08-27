#[test]
fn trajectory_handles_opaque_agent_turn_payload() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    fixture
        .store
        .append_task_event(
            &fixture.permit,
            LifecycleEventType::AgentTurnStarted,
            fixture.now,
        )
        .unwrap();
    let turn = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::AgentTurn,
        &serde_json::json!("opaque fixture turn"),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &turn,
            LifecycleEventType::AgentTurnCompleted,
            fixture.now,
        )
        .unwrap();

    let entries = fixture.store.trajectory(&fixture.run.run_id).unwrap();
    let entry = entries
        .iter()
        .find(|entry| entry.artifact_id.as_ref() == Some(&turn.artifact_id))
        .expect("opaque agent turn trajectory entry");
    assert_eq!(entry.artifact_kind, Some(ArtifactKind::AgentTurn));
    assert!(entry.model.is_none());
}

#[test]
fn lifecycle_event_shapes_accept_current_store_exceptions() {
    let fixture = execution_commit_fixture();
    let valid_cases = [
        (LifecycleEventType::WorkflowCreated, false, false, true),
        (LifecycleEventType::RunCancelRequested, false, false, false),
        (LifecycleEventType::OutcomeWorkerEnqueued, true, false, true),
        (LifecycleEventType::TaskCancelled, true, false, false),
        (LifecycleEventType::TaskStarted, true, true, false),
        (LifecycleEventType::TaskRetryScheduled, true, true, false),
        (LifecycleEventType::TaskSucceeded, true, true, true),
        (LifecycleEventType::ArtifactCommitted, true, true, true),
        (LifecycleEventType::ExecutionCommitted, true, true, true),
        (LifecycleEventType::PolicyEvaluated, true, true, true),
        (LifecycleEventType::ShadowPairCompleted, true, true, true),
    ];

    for (event_type, has_task_id, has_attempt_id, has_artifact_id) in valid_cases {
        assert!(
            validate_event_shape(event_type, has_task_id, has_attempt_id, has_artifact_id).is_ok(),
            "unexpectedly rejected {:?}",
            event_type
        );
    }

    let invalid_cases = [
        (LifecycleEventType::WorkflowCreated, true, false, true),
        (LifecycleEventType::RunCancelRequested, false, false, true),
        (LifecycleEventType::OutcomeWorkerEnqueued, true, true, true),
        (LifecycleEventType::TaskStarted, true, false, false),
        (LifecycleEventType::ArtifactCommitted, true, false, true),
    ];

    for (event_type, has_task_id, has_attempt_id, has_artifact_id) in invalid_cases {
        assert!(
            matches!(
                validate_event_shape(event_type, has_task_id, has_attempt_id, has_artifact_id),
                Err(StoreError::InvalidLifecycleEventShape { event_type: value })
            if value == event_type.as_str()
            ),
            "unexpectedly accepted {:?}",
            event_type
        );
    }

    let mut connection = fixture.store.connection.lock().unwrap();
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    assert!(matches!(
        append_event(
            &transaction,
            &fixture.permit.run_id,
            None,
            Some(&fixture.permit.attempt_id),
            LifecycleEventType::ArtifactCommitted,
            None,
            fixture.now,
        ),
        Err(StoreError::Domain(DomainError::AttemptOriginWithoutTask))
    ));
}

#[test]
fn doctor_rejects_forged_paper_effect_event_shape() {
    let fixture = execution_commit_fixture();
    {
        let mut connection = fixture.store.connection.lock().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        transaction
            .execute(
                r#"INSERT INTO rebuild_events
                       (run_id, task_id, attempt_id, event_type, artifact_id, created_at)
                       VALUES (?1, NULL, NULL, ?2, NULL, ?3)"#,
                params![
                    fixture.permit.run_id.0,
                    LifecycleEventType::ExecutionEffectIntent.as_str(),
                    fixture.now.to_rfc3339(),
                ],
            )
            .unwrap();
        transaction.commit().unwrap();
    }

    assert!(matches!(
        fixture.store.verify_integrity(),
        Err(StoreError::Integrity(message))
            if message.contains("invalid shape")
                && message.contains("execution.effect.intent")
    ));
}

#[test]
fn events_after_rejects_forged_paper_effect_event_shape() {
    let fixture = execution_commit_fixture();
    {
        let mut connection = fixture.store.connection.lock().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        transaction
            .execute(
                r#"INSERT INTO rebuild_events
                       (run_id, task_id, attempt_id, event_type, artifact_id, created_at)
                       VALUES (?1, NULL, NULL, ?2, NULL, ?3)"#,
                params![
                    fixture.permit.run_id.0,
                    LifecycleEventType::ExecutionEffectSettled.as_str(),
                    fixture.now.to_rfc3339(),
                ],
            )
            .unwrap();
        transaction.commit().unwrap();
    }

    assert!(matches!(
        fixture.store.events_after(&fixture.permit.run_id, 0, 100),
        Err(StoreError::InvalidLifecycleEventShape { event_type })
            if event_type == LifecycleEventType::ExecutionEffectSettled.as_str()
    ));
}

fn insert_paper_effect_event(
    fixture: &ExecutionCommitFixture,
    effect: &ArtifactRef,
    event_type: LifecycleEventType,
) {
    let mut connection = fixture.store.connection.lock().unwrap();
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    transaction
        .execute(
            r#"INSERT INTO rebuild_events
                   (run_id, task_id, attempt_id, event_type, artifact_id, created_at)
                   VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
            params![
                fixture.permit.run_id.0,
                fixture.permit.task_id.0,
                fixture.permit.attempt_id.0,
                event_type.as_str(),
                effect.artifact_id.0.as_str(),
                fixture.now.to_rfc3339(),
            ],
        )
        .unwrap();
    transaction.commit().unwrap();
}

#[test]
fn paper_effect_history_requires_prior_intent_and_single_terminal() {
    let fixture = execution_commit_fixture();
    let effect = artifact_ref(&fixture.commitment);
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &fixture.commitment,
            LifecycleEventType::ExecutionCommitted,
            fixture.now,
        )
        .unwrap();
    insert_paper_effect_event(
        &fixture,
        &effect,
        LifecycleEventType::ExecutionEffectSettled,
    );
    assert!(matches!(
        fixture.store.events_after(&fixture.permit.run_id, 0, 100),
        Err(StoreError::Integrity(message))
            if message.contains("has no prior intent")
    ));

    let fixture = execution_commit_fixture();
    let effect = artifact_ref(&fixture.commitment);
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &fixture.commitment,
            LifecycleEventType::ExecutionCommitted,
            fixture.now,
        )
        .unwrap();
    insert_paper_effect_event(&fixture, &effect, LifecycleEventType::ExecutionEffectIntent);
    insert_paper_effect_event(
        &fixture,
        &effect,
        LifecycleEventType::ExecutionEffectSettled,
    );
    insert_paper_effect_event(&fixture, &effect, LifecycleEventType::ExecutionEffectIntent);
    assert!(matches!(
        fixture.store.verify_integrity(),
        Err(StoreError::Integrity(message))
            if message.contains("intent after terminal")
    ));

    let fixture = execution_commit_fixture();
    let effect = artifact_ref(&fixture.commitment);
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &fixture.commitment,
            LifecycleEventType::ExecutionCommitted,
            fixture.now,
        )
        .unwrap();
    insert_paper_effect_event(&fixture, &effect, LifecycleEventType::ExecutionEffectIntent);
    insert_paper_effect_event(
        &fixture,
        &effect,
        LifecycleEventType::ExecutionEffectSettled,
    );
    insert_paper_effect_event(
        &fixture,
        &effect,
        LifecycleEventType::ExecutionEffectRecovered,
    );
    assert!(matches!(
        fixture.store.verify_integrity(),
        Err(StoreError::Integrity(message))
            if message.contains("duplicate terminal event")
    ));
}

#[test]
fn tool_lifecycle_allows_completed_call_and_blocks_pending_success() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    let call = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ToolCall,
        &serde_json::json!({"call_id": "fixture-call"}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &call,
            LifecycleEventType::ToolCalled,
            fixture.now,
        )
        .unwrap();
    let result = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ToolResult,
        &serde_json::json!({"call_id": "fixture-call", "ok": true}),
        vec![artifact_ref(&call)],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &result,
            LifecycleEventType::ToolCompleted,
            fixture.now,
        )
        .unwrap();
    let output = lifecycle_test_artifact(&fixture, ArtifactLifecycle::RunScoped, "output");
    fixture
        .store
        .commit_attempt(
            &fixture.permit,
            std::slice::from_ref(&output),
            TaskStatus::Succeeded,
            fixture.now,
        )
        .unwrap();
    fixture.store.verify_integrity().unwrap();

    let pending = task_artifact_fixture(RunPurpose::Debug);
    let pending_call = permit_artifact(
        &pending.store,
        &pending.permit,
        ArtifactKind::ToolCall,
        &serde_json::json!({"call_id": "pending-call"}),
        vec![],
        ArtifactLifecycle::RunScoped,
        pending.now,
    );
    pending
        .store
        .write_task_artifact(
            &pending.permit,
            &pending_call,
            LifecycleEventType::ToolCalled,
            pending.now,
        )
        .unwrap();
    let output = lifecycle_test_artifact(&pending, ArtifactLifecycle::RunScoped, "pending-output");
    assert!(matches!(
        pending.store.commit_attempt(
            &pending.permit,
            std::slice::from_ref(&output),
            TaskStatus::Succeeded,
            pending.now,
        ),
        Err(StoreError::Integrity(message)) if message.contains("pending tool calls")
    ));
    assert!(matches!(
        pending.store.artifact(&output.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
    assert!(pending
        .store
        .events_after(&pending.run.run_id, 0, 100)
        .unwrap()
        .iter()
        .all(|event| event.event_type != LifecycleEventType::TaskSucceeded.as_str()));
}

#[test]
fn tool_lifecycle_failure_can_close_pending_call() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    let call = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ToolCall,
        &serde_json::json!({"call_id": "failed-call"}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &call,
            LifecycleEventType::ToolCalled,
            fixture.now,
        )
        .unwrap();
    fixture
        .store
        .finish_task(&fixture.permit, TaskStatus::Failed, fixture.now)
        .unwrap();
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn events_after_validates_effect_history_beyond_page() {
    let fixture = execution_commit_fixture();
    let effect = artifact_ref(&fixture.commitment);
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &fixture.commitment,
            LifecycleEventType::ExecutionCommitted,
            fixture.now,
        )
        .unwrap();
    insert_paper_effect_event(
        &fixture,
        &effect,
        LifecycleEventType::ExecutionEffectSettled,
    );
    assert!(fixture
        .store
        .events_after(&fixture.permit.run_id, i64::MAX, 1)
        .is_err());
}

#[test]
fn events_after_rejects_tool_history_beyond_page() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    {
        let mut connection = fixture.store.connection.lock().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        transaction
            .execute(
                r#"INSERT INTO rebuild_events
                       (run_id, task_id, attempt_id, event_type, artifact_id, created_at)
                       VALUES (?1, ?2, ?3, ?4, NULL, ?5)"#,
                params![
                    fixture.permit.run_id.0,
                    fixture.permit.task_id.0,
                    fixture.permit.attempt_id.0,
                    LifecycleEventType::ToolCalled.as_str(),
                    fixture.now.to_rfc3339(),
                ],
            )
            .unwrap();
        transaction.commit().unwrap();
    }
    assert!(matches!(
        fixture.store.events_after(&fixture.run.run_id, i64::MAX, 1),
        Err(StoreError::Integrity(message)) if message.contains("has no artifact")
    ));
}
