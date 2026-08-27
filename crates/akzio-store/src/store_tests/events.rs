#[test]
fn paper_effect_intent_is_idempotent_and_settlement_requires_intent() {
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

    assert!(matches!(
        fixture.store.commit_fenced_attempt_with_effect(
            &fixture.lease,
            &fixture.permit,
            std::slice::from_ref(&fixture.commitment),
            &effect,
            false,
            fixture.now,
        ),
        Err(StoreError::MissingPaperEffectIntent(_))
    ));

    assert!(!fixture
        .store
        .record_paper_effect_intent(&fixture.lease, &fixture.permit, &effect, fixture.now,)
        .unwrap());
    assert!(fixture
        .store
        .record_paper_effect_intent(&fixture.lease, &fixture.permit, &effect, fixture.now,)
        .unwrap());

    let intent_count = fixture
        .store
        .events_after(&fixture.permit.run_id, 0, 100)
        .unwrap()
        .into_iter()
        .filter(|event| {
            event.event_type == LifecycleEventType::ExecutionEffectIntent.as_str()
                && event.artifact_id.as_ref() == Some(&effect.artifact_id)
        })
        .count();
    assert_eq!(intent_count, 1);
}

#[test]
fn paper_effect_settlement_rejects_non_paper_run() {
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
    fixture
        .store
        .record_paper_effect_intent(&fixture.lease, &fixture.permit, &effect, fixture.now)
        .unwrap();
    fixture
        .store
        .connection
        .lock()
        .unwrap()
        .execute(
            "UPDATE rebuild_runs SET purpose = ?1 WHERE run_id = ?2",
            params![enum_name(RunPurpose::Debug), fixture.permit.run_id.0],
        )
        .unwrap();

    assert!(matches!(
        fixture.store.commit_fenced_attempt_with_effect(
            &fixture.lease,
            &fixture.permit,
            std::slice::from_ref(&fixture.commitment),
            &effect,
            false,
            fixture.now,
        ),
        Err(StoreError::NonCanonicalLearningPurpose(RunPurpose::Debug))
    ));

    let events = fixture
        .store
        .events_after(&fixture.permit.run_id, 0, 100)
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.artifact_id.as_ref() == Some(&effect.artifact_id))
            .filter(|event| {
                matches!(
                    event.event_type.as_str(),
                    "execution.effect.intent"
                        | "execution.effect.settled"
                        | "execution.effect.recovered"
                )
            })
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["execution.effect.intent"]
    );
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn paper_effect_settlement_rolls_back_and_can_retry_after_failure() {
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
    fixture
        .store
        .record_paper_effect_intent(&fixture.lease, &fixture.permit, &effect, fixture.now)
        .unwrap();
    {
        let connection = fixture.store.connection.lock().unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER fail_paper_effect_settlement BEFORE INSERT ON rebuild_events \
                     WHEN NEW.event_type = 'execution.effect.settled' \
                     BEGIN SELECT RAISE(ABORT, 'injected settlement failure'); END;",
            )
            .unwrap();
    }

    assert!(matches!(
        fixture.store.commit_fenced_attempt_with_effect(
            &fixture.lease,
            &fixture.permit,
            std::slice::from_ref(&fixture.commitment),
            &effect,
            false,
            fixture.now,
        ),
        Err(StoreError::Sql(_))
    ));
    assert!(fixture
        .store
        .events_after(&fixture.permit.run_id, 0, 100)
        .unwrap()
        .iter()
        .all(|event| event.event_type != LifecycleEventType::ExecutionEffectSettled.as_str()));

    {
        let connection = fixture.store.connection.lock().unwrap();
        connection
            .execute_batch("DROP TRIGGER fail_paper_effect_settlement;")
            .unwrap();
    }
    fixture
        .store
        .commit_fenced_attempt_with_effect(
            &fixture.lease,
            &fixture.permit,
            std::slice::from_ref(&fixture.commitment),
            &effect,
            false,
            fixture.now,
        )
        .unwrap();
    assert!(matches!(
        fixture.store.commit_fenced_attempt_with_effect(
            &fixture.lease,
            &fixture.permit,
            std::slice::from_ref(&fixture.commitment),
            &effect,
            false,
            fixture.now,
        ),
        Err(StoreError::StalePermit(_)) | Err(StoreError::PaperEffectAlreadySettled(_))
    ));
    fixture.store.verify_integrity().unwrap();
}
