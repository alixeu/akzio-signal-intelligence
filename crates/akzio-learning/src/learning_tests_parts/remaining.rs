#[test]
fn shadow_outcome_schedule_requires_run_scoped_mixed_closure() {
    let fixture = RuntimeFixture::new();
    let now = fixture_time();
    fixture
        .store
        .request_run_cancel(&fixture.paper_run_id, "isolate schedule boundary test", now)
        .unwrap();

    let debug_run = fixture_workflow(
        &fixture.store,
        RunPurpose::Debug,
        1,
        None,
        now - Duration::days(2),
    );
    let debug_permit = claim_fixture_task(&fixture.store, "debug-context", now);
    assert_eq!(debug_permit.run_id, debug_run.run_id);
    let debug_context = fixture_artifact(
        &fixture.store,
        Some(&debug_permit),
        ArtifactKind::DecisionContext,
        ArtifactLifecycle::RunScoped,
        &serde_json::json!({"context": "debug"}),
        vec![],
        now,
    );
    fixture
        .store
        .commit_attempt(
            &debug_permit,
            std::slice::from_ref(&debug_context),
            TaskStatus::Succeeded,
            now,
        )
        .unwrap();

    let shadow_run = fixture_workflow(
        &fixture.store,
        RunPurpose::Shadow,
        1,
        Some(fixture.candidate_contract_hash.clone()),
        now - Duration::days(1),
    );
    let shadow_permit = claim_fixture_task(&fixture.store, "shadow-schedule", now);
    assert_eq!(shadow_permit.run_id, shadow_run.run_id);
    let candidate_decision = fixture_artifact(
        &fixture.store,
        Some(&shadow_permit),
        ArtifactKind::Decision,
        ArtifactLifecycle::RunScoped,
        &serde_json::json!({"candidate": "schedule-boundary"}),
        vec![],
        now,
    );
    fixture
        .store
        .write_task_artifact(
            &shadow_permit,
            &candidate_decision,
            LifecycleEventType::ShadowDecisionCreated,
            now,
        )
        .unwrap();

    let build_outcome = |decision_context: ArtifactRef,
                         schedule_lifecycle: ArtifactLifecycle|
     -> Result<(Artifact, Artifact), StoreError> {
        let mut schedule = fixture.materialization.schedule.clone();
        schedule.outcome_id = OutcomeId::new();
        schedule.decision = artifact_reference(&candidate_decision);
        schedule.decision_context = decision_context;
        schedule.created_at = now;
        let schedule_artifact = fixture_artifact(
            &fixture.store,
            Some(&shadow_permit),
            ArtifactKind::OutcomeSchedule,
            schedule_lifecycle,
            &schedule,
            vec![
                schedule.decision.clone(),
                schedule.decision_context.clone(),
                schedule.execution_context.clone(),
                execution_verdict(&schedule.execution).clone(),
            ],
            now,
        );
        fixture.store.write_task_artifact(
            &shadow_permit,
            &schedule_artifact,
            LifecycleEventType::ShadowOutcomeScheduleCreated,
            now,
        )?;

        let mut materialization = fixture.materialization.clone();
        materialization.schedule = schedule;
        materialization.schedule_artifact = artifact_reference(&schedule_artifact);
        let outcome = materialize_outcome(&materialization).unwrap();
        let outcome_artifact = fixture_artifact(
            &fixture.store,
            Some(&shadow_permit),
            ArtifactKind::Outcome,
            ArtifactLifecycle::RunScoped,
            &outcome,
            std::iter::once(materialization.schedule_artifact.clone())
                .chain(materialization.market_evidence.iter().cloned())
                .collect(),
            materialization.sealed_at,
        );
        Ok((schedule_artifact, outcome_artifact))
    };

    let paper_decision_context = fixture.materialization.schedule.decision_context.clone();
    assert!(matches!(
        build_outcome(paper_decision_context.clone(), ArtifactLifecycle::Canonical),
        Err(StoreError::InvalidTaskArtifactLifecycle {
            purpose: RunPurpose::Shadow,
            lifecycle: ArtifactLifecycle::Canonical,
        })
    ));

    let (_, debug_closure_outcome) = build_outcome(
        artifact_reference(&debug_context),
        ArtifactLifecycle::RunScoped,
    )
    .unwrap();
    assert!(matches!(
        fixture.store.commit_outcomes(
            &shadow_permit,
            &[debug_closure_outcome],
            fixture.materialization.sealed_at,
        ),
        Err(StoreError::InvalidLearningCommit(
            "learning_artifact.run_purpose"
        ))
    ));

    let (mixed_schedule_artifact, mixed_outcome) =
        build_outcome(paper_decision_context, ArtifactLifecycle::RunScoped).unwrap();
    fixture
        .store
        .commit_outcomes(
            &shadow_permit,
            &[mixed_outcome],
            fixture.materialization.sealed_at,
        )
        .unwrap();
    assert_eq!(
        mixed_schedule_artifact.lifecycle,
        ArtifactLifecycle::RunScoped
    );
    let schedule: OutcomeSchedule = serde_json::from_slice(
        &fixture
            .store
            .read_blob(&mixed_schedule_artifact.blob)
            .unwrap(),
    )
    .unwrap();
    let purpose = |reference: &ArtifactRef| {
        let artifact = fixture.store.artifact(&reference.artifact_id).unwrap();
        let run_id = artifact.origin.unwrap().run_id.unwrap();
        fixture.store.run_purpose(&run_id).unwrap()
    };
    assert_eq!(purpose(&schedule.decision), RunPurpose::Shadow);
    assert_eq!(purpose(&schedule.decision_context), RunPurpose::Paper);
    assert_eq!(purpose(&schedule.execution_context), RunPurpose::Paper);
    assert_eq!(
        purpose(execution_verdict(&schedule.execution)),
        RunPurpose::Paper
    );
}
