#[test]
fn doctor_rejects_no_order_schedule_with_accepted_verdict() {
    let fixture = PolicyCommitFixture::memory();
    fixture.store.verify_integrity().unwrap();
    let schedule = fixture
        .store
        .latest_artifact_by_kind(ArtifactKind::OutcomeSchedule)
        .unwrap()
        .unwrap();
    let execution_context = fixture
        .store
        .latest_artifact_by_kind(ArtifactKind::ExecutionContext)
        .unwrap()
        .unwrap();
    let accepted_verdict = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ExecutionVerdict,
        &ExecutionVerdict::Accepted {
            execution_context: artifact_ref(&execution_context),
        },
        vec![artifact_ref(&execution_context)],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    let mut payload: OutcomeSchedule =
        serde_json::from_slice(&fixture.store.read_blob(&schedule.blob).unwrap()).unwrap();
    payload.execution = OutcomeExecutionLineage::NoOrder {
        execution_verdict: artifact_ref(&accepted_verdict),
    };
    let forged_schedule = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::OutcomeSchedule,
        &payload,
        outcome_schedule_source_refs(&payload),
        ArtifactLifecycle::Canonical,
        fixture.now,
    );
    {
        let mut connection = fixture.store.connection.lock().unwrap();
        let transaction = connection.transaction().unwrap();
        insert_artifact(&transaction, &accepted_verdict).unwrap();
        insert_artifact(&transaction, &forged_schedule).unwrap();
        transaction.commit().unwrap();
    }

    assert!(matches!(
        fixture.store.verify_integrity(),
        Err(StoreError::Integrity(message)) if message.contains("execution lineage")
    ));
}

#[test]
fn doctor_rejects_stale_policy_head() {
    let fixture = PolicyCommitFixture::memory();
    let commit = fixture.commit(
        fixture
            .store
            .policy_shadow_pair_snapshot(&fixture.subject)
            .unwrap(),
    );
    fixture.store.record_policy_evaluation(&commit).unwrap();
    fixture.store.verify_integrity().unwrap();

    let stale_transition = PolicyTransition {
        transition_id: PolicyTransitionId::new(),
        created_at: fixture.now + Duration::seconds(1),
        ..fixture.transition.clone()
    };
    {
        let mut connection = fixture.store.connection.lock().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let event_cursor = append_event(
            &transaction,
            &fixture.run.run_id,
            Some(&fixture.permit.task_id),
            Some(&fixture.permit.attempt_id),
            LifecycleEventType::PolicyTransitioned,
            Some(&fixture.evaluation.artifact_id),
            stale_transition.created_at,
        )
        .unwrap();
        transaction
            .execute(
                r#"INSERT INTO rebuild_policy_transitions
                       (transition_id, subject_id, subject_json, from_state_json, to_state_json,
                        evaluation_artifact_id, run_id, revision, created_at, event_cursor)
                       VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"#,
                params![
                    stale_transition.transition_id.0,
                    fixture.subject.subject_id(),
                    serde_json::to_string(&fixture.subject).unwrap(),
                    serde_json::to_string(&stale_transition.from).unwrap(),
                    serde_json::to_string(&stale_transition.to).unwrap(),
                    fixture.evaluation.artifact_id.0.as_str(),
                    fixture.run.run_id.0,
                    2_u64,
                    stale_transition.created_at.to_rfc3339(),
                    event_cursor,
                ],
            )
            .unwrap();
        transaction.commit().unwrap();
    }

    let corrupted = fixture.store.verify_integrity();
    assert!(matches!(
        &corrupted,
        Err(StoreError::Integrity(message)) if message.contains("stale")
    ));
}

#[test]
fn paper_effect_events_require_complete_lineage_at_append_boundary() {
    let fixture = execution_commit_fixture();
    let event_types = [
        LifecycleEventType::ExecutionEffectIntent,
        LifecycleEventType::ExecutionEffectRecovered,
        LifecycleEventType::ExecutionEffectSettled,
    ];
    let cases = [
        (
            None,
            Some(&fixture.permit.attempt_id),
            Some(&fixture.commitment.artifact_id),
        ),
        (
            Some(&fixture.permit.task_id),
            None,
            Some(&fixture.commitment.artifact_id),
        ),
        (
            Some(&fixture.permit.task_id),
            Some(&fixture.permit.attempt_id),
            None,
        ),
    ];

    for lifecycle_type in event_types {
        for (task_id, attempt_id, artifact_id) in cases {
            let mut connection = fixture.store.connection.lock().unwrap();
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            assert!(matches!(
                append_event(
                    &transaction,
                    &fixture.permit.run_id,
                    task_id,
                    attempt_id,
                    lifecycle_type,
                    artifact_id,
                    fixture.now,
                ),
                Err(StoreError::InvalidLifecycleEventShape { event_type: value })
                if value == lifecycle_type.as_str()
            ));
        }
    }
}

#[test]
fn trajectory_redacts_provider_and_tool_payloads() {
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
        &serde_json::json!({
            "turn": 1,
            "attempt": 1,
            "contract_hash": "contract",
            "request_hash": "request-hash",
            "capability_snapshot": {
                "provider_id": "fixture-provider",
                "model_id": "fixture-model",
                "reasoning_effort": "high",
                "supports_tool_calls": true,
                "supports_stateless_continuation": true,
                "native_web_tool": false,
                "source": "fixture"
                },
                "capability_snapshot_hash": "capability-hash",
                "tool_set_hash": "tool-hash",
            "request": {"phase": "draft", "secret": "provider-request"},
            "response": {
                "assistant_text": "bounded fixture research memo",
                "telemetry": {
                    "latency_millis": 321,
                    "input_tokens": 123,
                    "output_tokens": 45
                },
                "secret": "provider-result"
            }
        }),
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

    let call = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ToolCall,
        &serde_json::json!({
            "call": {
                "call_id": "call-1",
                "name": "read_artifact",
                "arguments": {"secret": "tool-arguments"}
            }
        }),
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
        &serde_json::json!({
            "call_id": "call-1",
            "name": "read_artifact",
            "ok": true,
            "value": {"secret": "tool-result"}
        }),
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

    let note = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::DeliberationNote,
        &serde_json::json!({
            "selected_path": "use the governed evidence",
            "alternatives": ["defer"],
            "uncertainties": ["fixture uncertainty"],
            "basis_artifact_ids": [],
            "confidence_ppm": 750_000
        }),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &note,
            LifecycleEventType::DeliberationNoteCreated,
            fixture.now,
        )
        .unwrap();
    let output = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::DecisionProposal,
        &serde_json::json!({"statement": "fixture output"}),
        vec![artifact_ref(&note)],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .commit_attempt(
            &fixture.permit,
            std::slice::from_ref(&output),
            TaskStatus::Succeeded,
            fixture.now,
        )
        .unwrap();

    let entries = fixture.store.trajectory(&fixture.run.run_id).unwrap();
    let recent = fixture
        .store
        .recent_trajectory(&fixture.run.run_id, 2)
        .unwrap();
    assert!(recent.len() <= 2);
    assert_eq!(
        recent,
        entries[entries.len().saturating_sub(recent.len())..]
    );
    let recent_outputs = fixture
        .store
        .recent_artifacts_by_kind(ArtifactKind::DecisionProposal, 1)
        .unwrap();
    assert_eq!(
        recent_outputs.first().map(|artifact| &artifact.artifact_id),
        Some(&output.artifact_id)
    );
    assert!(entries
        .windows(2)
        .all(|pair| pair[0].cursor < pair[1].cursor));
    let model = entries
        .iter()
        .find(|entry| entry.artifact_kind == Some(ArtifactKind::AgentTurn))
        .expect("model trajectory entry");
    assert_eq!(
        model
            .model
            .as_ref()
            .and_then(|value| value.model_id.as_deref()),
        Some("fixture-model")
    );
    assert_eq!(model.latency_millis, Some(321));
    assert_eq!(model.input_tokens, Some(123));
    assert_eq!(model.output_tokens, Some(45));
    assert_eq!(model.phase.as_deref(), Some("draft"));
    assert_eq!(
        model.assistant_text.as_deref(),
        Some("bounded fixture research memo")
    );
    let model_json = serde_json::to_string(model).unwrap();
    assert!(!model_json.contains("provider-request"));
    assert!(!model_json.contains("provider-result"));

    let tool = entries
        .iter()
        .find(|entry| entry.tool.is_some())
        .expect("tool trajectory entry");
    assert_eq!(
        tool.tool
            .as_ref()
            .and_then(|value| value.call_id.as_deref()),
        Some("call-1")
    );
    let tool_json = serde_json::to_string(tool).unwrap();
    assert!(!tool_json.contains("tool-arguments"));
    assert!(!tool_json.contains("tool-result"));

    assert!(entries.iter().any(|entry| {
        entry
            .deliberation
            .as_ref()
            .is_some_and(|value| value.selected_path == "use the governed evidence")
    }));
    let output_entry = entries
        .iter()
        .find(|entry| entry.artifact_id.as_ref() == Some(&output.artifact_id))
        .expect("output trajectory entry");
    assert!(output_entry
        .output_refs
        .iter()
        .any(|reference| reference.artifact_id == note.artifact_id));
}
