#[test]
fn rust_materializes_returns_calibration_completeness_and_recall() {
    let outcome = materialize_outcome(&materialization()).unwrap();
    assert_eq!(outcome.schedule.kind, ArtifactKind::OutcomeSchedule);
    assert_eq!(outcome.windows.len(), 3);

    let t1 = &outcome.windows[0];
    assert_eq!(t1.portfolio_return_ppm, 100_000);
    assert_eq!(t1.benchmark_return_ppm, 50_000);
    assert_eq!(t1.transaction_cost_ppm, 100);
    assert_eq!(t1.slippage_ppm, 50);
    assert_eq!(t1.utility_ppm, 49_850);
    assert_eq!(t1.calibration_ppm, None);
    assert_eq!(t1.evidence_completeness_ppm, 750_000);
    assert_eq!(t1.risk_recall_ppm, Some(500_000));

    let t3 = &outcome.windows[1];
    assert_eq!(t3.portfolio_return_ppm, -100_000);
    assert_eq!(t3.benchmark_return_ppm, -50_000);
    assert_eq!(t3.utility_ppm, -50_150);
    assert_eq!(t3.calibration_ppm, None);
}

#[test]
fn partial_materializer_seals_only_the_due_prefix() {
    let mut input = materialization();
    input
        .observations
        .retain(|observation| observation.horizon == OutcomeHorizon::T1);
    let outcome = materialize_partial_outcome(&input).unwrap();
    assert_eq!(outcome.windows.len(), 1);
    assert_eq!(outcome.windows[0].horizon, OutcomeHorizon::T1);
    assert!(outcome.sealed_at.is_none());
    assert!(materialize_outcome(&input).is_err());
}

#[test]
fn materializer_rejects_duplicate_and_missing_horizons() {
    let mut missing = materialization();
    missing.observations.pop();
    assert!(matches!(
        materialize_outcome(&missing),
        Err(EvaluationError::InvalidMaterialization(
            "missing observation horizon"
        ))
    ));

    let mut duplicate = materialization();
    duplicate
        .observations
        .push(duplicate.observations[0].clone());
    assert!(matches!(
        materialize_outcome(&duplicate),
        Err(EvaluationError::InvalidMaterialization(
            "duplicate observation horizon"
        ))
    ));

    let mut duplicate_forecast = materialization();
    duplicate_forecast
        .forecasts
        .push(forecast(DecisionHorizon::T1, 500_000));
    assert!(matches!(
        materialize_outcome(&duplicate_forecast),
        Err(EvaluationError::InvalidMaterialization(
            "duplicate forecast horizon"
        ))
    ));
}

#[test]
fn materializer_rejects_not_due_and_incomplete_price_surfaces() {
    let mut not_due = materialization();
    not_due.observations[2].completed_trading_sessions = 4;
    assert!(matches!(
        materialize_outcome(&not_due),
        Err(EvaluationError::InvalidMaterialization(
            "horizon is not due"
        ))
    ));

    let mut incomplete = materialization();
    incomplete.observations[0].future_prices.remove(&Asset::Qqq);
    assert!(matches!(
        materialize_outcome(&incomplete),
        Err(EvaluationError::InvalidMaterialization(_))
    ));
}

#[test]
fn materializer_rejects_cost_model_above_one_hundred_percent() {
    let mut input = materialization();
    input.cost_model.transaction_cost_ppm = 1_000_001;

    assert!(matches!(
        materialize_outcome(&input),
        Err(EvaluationError::Domain(DomainError::InvalidBudget {
            field: "outcome.cost_model"
        }))
    ));
}

#[test]
fn every_nonpaper_purpose_is_rejected_for_canonical_learning() {
    for purpose in [
        RunPurpose::Debug,
        RunPurpose::Replay,
        RunPurpose::PaperDryRun,
        RunPurpose::Shadow,
    ] {
        assert!(matches!(
            require_canonical_purpose(purpose),
            Err(EvaluationError::NonCanonicalPurpose(actual)) if actual == purpose
        ));
    }
    require_canonical_purpose(RunPurpose::Paper).unwrap();
}

#[test]
fn nonpaper_evaluation_cannot_write_learning_state_or_events() {
    for purpose in [
        RunPurpose::Debug,
        RunPurpose::Replay,
        RunPurpose::PaperDryRun,
        RunPurpose::Shadow,
    ] {
        let fixture = RuntimeFixture::new();
        let blocked_paper = fixture.claim_evaluation("block-paper-queue");
        fixture
            .store
            .finish_task(&blocked_paper, TaskStatus::Cancelled, fixture_time())
            .unwrap();
        let run = fixture_workflow(&fixture.store, purpose, 1, None, fixture_time());
        let permit = claim_fixture_task(&fixture.store, "nonpaper", fixture_time());
        assert_eq!(permit.run_id, run.run_id);
        let subject = PolicySubject::Memory(MemoryId::new());
        let error = fixture
            .runtime
            .evaluate(EvaluationInput {
                permit: permit.clone(),
                subject: subject.clone(),
                hypothesis_id: "must-not-persist".to_owned(),
                materialization: fixture.materialization.clone(),
                contract_hash: ContentHash::of_bytes(b"active-contract"),
                topology_id: TopologyId("active-topology".to_owned()),
                candidate_policy: None,
                token_cost: Some(1),
                latency_millis: Some(1),
            })
            .unwrap_err();
        assert!(matches!(
            error,
            EvaluationError::NonCanonicalPurpose(actual) if actual == purpose
        ));
        assert!(fixture.store.policy_head(&subject).unwrap().is_none());
        assert_eq!(
            fixture
                .store
                .policy_shadow_pair_snapshot(&subject)
                .unwrap()
                .through_cursor,
            0
        );
        assert!(fixture
            .store
            .events_after(&run.run_id, 0, 100)
            .unwrap()
            .iter()
            .all(|event| !matches!(
                event.event_type.as_str(),
                "policy.evaluated" | "policy.transitioned" | "artifact.committed"
            )));
        fixture
            .store
            .finish_task(&permit, TaskStatus::Cancelled, fixture_time())
            .unwrap();
        fixture.store.verify_integrity().unwrap();
    }
}

#[test]
fn memory_lifecycle_requires_pairs_and_degrades_to_retirement() {
    let subject = PolicySubject::Memory(MemoryId::new());
    assert_eq!(
        subject.initial_state(),
        PolicyState::Memory(MemoryLifecycle::Candidate)
    );
    assert_eq!(
        next_state_with_fresh_pairs(subject.initial_state(), None, false, [0, 0, 0], 1),
        PolicyState::Memory(MemoryLifecycle::Candidate)
    );
    assert_eq!(
        next_state_with_fresh_pairs(
            PolicyState::Memory(MemoryLifecycle::Active),
            None,
            false,
            [1, 1, 1],
            1,
        ),
        PolicyState::Memory(MemoryLifecycle::Proven)
    );
    assert_eq!(
        next_state_with_fresh_pairs(
            PolicyState::Memory(MemoryLifecycle::Proven),
            None,
            true,
            [1, 1, 1],
            2,
        ),
        PolicyState::Memory(MemoryLifecycle::Contested)
    );
    assert_eq!(
        next_state_with_fresh_pairs(
            PolicyState::Memory(MemoryLifecycle::Contested),
            None,
            true,
            [0, 0, 0],
            2,
        ),
        PolicyState::Memory(MemoryLifecycle::Retired)
    );
}
