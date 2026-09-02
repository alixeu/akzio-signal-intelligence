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
    // TQQQ forecast 0.8, TQQQ rose: brier (0.8 - 1)^2 = 0.04, quality 0.96.
    assert_eq!(t1.calibration_ppm, Some(960_000));
    assert_eq!(t1.evidence_completeness_ppm, 750_000);
    assert_eq!(t1.risk_recall_ppm, Some(500_000));

    let t3 = &outcome.windows[1];
    assert_eq!(t3.portfolio_return_ppm, -100_000);
    assert_eq!(t3.benchmark_return_ppm, -50_000);
    assert_eq!(t3.utility_ppm, -50_150);
    // TQQQ forecast 0.2, TQQQ fell: brier (0.2 - 0)^2 = 0.04, quality 0.96.
    assert_eq!(t3.calibration_ppm, Some(960_000));

    // T+5 forecast 0.5 with a flat close: brier 0.25, quality 0.75. This is the
    // reason the default `minimum_confidence_ppm` of 800_000 cannot be reused
    // unexamined: a permanently uncertain forecaster scores a fixed 750_000.
    assert_eq!(outcome.windows[2].calibration_ppm, Some(750_000));
}

/// Calibration is a forecast-quality axis and must stay independent of utility.
/// It is deliberately NOT part of `outcome_is_degraded`: a single window carries
/// one binary observation, so a random bad draw on any horizon would otherwise
/// push the global Memory subject toward the terminal Retired state, and
/// demotions are not gated on fresh pairs.
#[test]
fn calibration_is_reported_but_does_not_gate_degradation() {
    let policy = EvaluationPolicy::default();
    let mut outcome = materialize_outcome(&materialization()).unwrap();
    for window in &mut outcome.windows {
        window.evidence_completeness_ppm = 1_000_000;
        window.risk_recall_ppm = Some(1_000_000);
        window.calibration_ppm = Some(0);
    }
    assert!(
        !policy.outcome_is_degraded(&outcome),
        "worst-possible calibration must not by itself mark an outcome degraded"
    );

    for window in &mut outcome.windows {
        window.calibration_ppm = Some(1_000_000);
    }
    assert!(!policy.outcome_is_degraded(&outcome));
}

/// The three extremes of the Brier score, scored per asset and macro-averaged.
#[test]
fn brier_calibration_covers_perfect_inverted_and_maximally_uncertain_forecasts() {
    fn calibration_for(probability_ppm: u32, rises: bool) -> Vec<Option<u32>> {
        let mut input = materialization();
        input.forecasts = vec![
            forecast(DecisionHorizon::T1, probability_ppm),
            forecast(DecisionHorizon::T3, probability_ppm),
            forecast(DecisionHorizon::T5, probability_ppm),
        ];
        let future = if rises { 110_000_000 } else { 90_000_000 };
        input.observations = vec![
            observation(OutcomeHorizon::T1, 1, 4, prices(future, 100_000_000)),
            observation(OutcomeHorizon::T3, 3, 6, prices(future, 100_000_000)),
            observation(OutcomeHorizon::T5, 5, 10, prices(future, 100_000_000)),
        ];
        materialize_outcome(&input)
            .unwrap()
            .windows
            .iter()
            .map(|window| window.calibration_ppm)
            .collect()
    }

    // Always right: brier 0, quality 1.0.
    assert_eq!(calibration_for(1_000_000, true), vec![Some(1_000_000); 3]);
    assert_eq!(calibration_for(0, false), vec![Some(1_000_000); 3]);

    // Always wrong: brier 1.0, quality 0.
    assert_eq!(calibration_for(1_000_000, false), vec![Some(0); 3]);
    assert_eq!(calibration_for(0, true), vec![Some(0); 3]);

    // Constant 0.5: brier 0.25 regardless of what happened, quality 0.75.
    assert_eq!(calibration_for(500_000, true), vec![Some(750_000); 3]);
    assert_eq!(calibration_for(500_000, false), vec![Some(750_000); 3]);
}

/// Forecast quality must not depend on the allocation decision. The macro
/// average is unweighted, so re-weighting the target portfolio cannot move
/// `calibration_ppm` even though it does move `portfolio_return_ppm`.
#[test]
fn calibration_ignores_target_weights() {
    let baseline = materialize_outcome(&materialization()).unwrap();

    let mut reweighted_input = materialization();
    reweighted_input.target = TargetPortfolio {
        weights: BTreeMap::from([
            (Asset::Tqqq, WeightPpm(250_000)),
            (Asset::Qqq, WeightPpm(750_000)),
            (Asset::Soxx, WeightPpm::ZERO),
            (Asset::Soxl, WeightPpm::ZERO),
        ]),
    };
    let reweighted = materialize_outcome(&reweighted_input).unwrap();

    for (left, right) in baseline.windows.iter().zip(reweighted.windows.iter()) {
        assert_eq!(
            left.calibration_ppm, right.calibration_ppm,
            "allocation must not pollute forecast-quality measurement"
        );
    }
    assert_ne!(
        baseline.windows[0].portfolio_return_ppm,
        reweighted.windows[0].portfolio_return_ppm,
        "the reweighting must actually change the portfolio return"
    );
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
        next_state_with_fresh_pairs(subject.initial_state(), None, false, true, [0, 0, 0], 1),
        PolicyState::Memory(MemoryLifecycle::Candidate)
    );
    assert_eq!(
        next_state_with_fresh_pairs(
            PolicyState::Memory(MemoryLifecycle::Active),
            None,
            false,
            true,
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
            true,
            [0, 0, 0],
            2,
        ),
        PolicyState::Memory(MemoryLifecycle::Retired)
    );
}

/// Unmeasured risk recall must hold the subject in place: it is neither a pass
/// that buys a promotion nor a failure that proves degradation.
#[test]
fn unmeasured_risk_recall_neither_promotes_nor_demotes() {
    assert_eq!(
        next_state_with_fresh_pairs(
            PolicyState::Memory(MemoryLifecycle::Candidate),
            None,
            false,
            false,
            [1, 1, 1],
            1,
        ),
        PolicyState::Memory(MemoryLifecycle::Candidate),
        "unmeasured risk recall must not promote Candidate to Active"
    );
    assert_eq!(
        next_state_with_fresh_pairs(
            PolicyState::Memory(MemoryLifecycle::Active),
            None,
            false,
            false,
            [9, 9, 9],
            1,
        ),
        PolicyState::Memory(MemoryLifecycle::Active),
        "abundant fresh pairs must not substitute for a measured risk recall"
    );
    assert_eq!(
        next_state_with_fresh_pairs(
            PolicyState::Memory(MemoryLifecycle::Candidate),
            None,
            false,
            false,
            [1, 1, 1],
            1,
        ),
        PolicyState::Memory(MemoryLifecycle::Candidate),
    );
    // Measured evidence-completeness degradation still demotes even when risk
    // recall itself was never measured; demotion is not gated on measurement.
    assert_eq!(
        next_state_with_fresh_pairs(
            PolicyState::Memory(MemoryLifecycle::Active),
            None,
            true,
            false,
            [1, 1, 1],
            1,
        ),
        PolicyState::Memory(MemoryLifecycle::Contested)
    );
}

/// An outcome that never measured risk recall is not degraded. Absent evidence
/// must not read as failing evidence, otherwise every real Paper outcome walks
/// Candidate -> Contested -> Retired and the Experience channel closes forever.
/// Covers the daemon's real collection path end to end. `horizon_observations`
/// is what `collect_outcome_materialization` calls, and it still returns
/// `detected_risk_count: None`, so every real Paper outcome is unmeasured today.
/// Before this change that `None` read as degraded and walked the global Memory
/// subject Candidate -> Contested -> Retired on two outcomes, permanently
/// closing the Experience channel. It must now hold instead.
#[test]
fn daemon_collection_path_yields_unmeasured_recall_that_holds_state() {
    let bars_by_asset = Asset::EXECUTABLE
        .into_iter()
        .map(|asset| {
            let bars = (4..=10)
                .map(|offset| (day(offset), MoneyMicros(100_000_000)))
                .collect::<BTreeMap<_, _>>();
            (asset, bars)
        })
        .collect::<BTreeMap<_, _>>();
    let common_dates = (4..=10).map(day).collect::<Vec<_>>();

    // Exactly what crates/akzio-daemon/src/outcome_parts/collection.rs passes:
    // hard_blockers.len() + material_conflicts.len().
    let observations = horizon_observations(&bars_by_asset, &common_dates, 2).unwrap();
    assert_eq!(observations.len(), 3);
    assert!(
        observations
            .iter()
            .all(|observation| observation.detected_risk_count.is_none()),
        "the daemon collection path does not measure detected risk yet"
    );

    let mut input = materialization();
    input.observations = observations;
    input.baseline_prices = prices(100_000_000, 100_000_000);
    let outcome = materialize_outcome(&input).unwrap();
    assert!(
        outcome
            .windows
            .iter()
            .all(|window| window.risk_recall_ppm.is_none()),
        "an unmeasured observation must not synthesize a recall value"
    );

    let policy = EvaluationPolicy::default();
    assert!(
        !policy.outcome_is_degraded(&outcome),
        "the real collection path must not be degraded purely for lacking risk measurement"
    );
    assert!(!policy.risk_recall_is_measured(&outcome));

    // The chain the task brief identified: Candidate would previously be pushed
    // to Contested, then to the terminal Retired state, on consecutive outcomes.
    let mut state = PolicyState::Memory(MemoryLifecycle::Candidate);
    for _ in 0..3 {
        state = next_state_with_fresh_pairs(
            state,
            None,
            policy.outcome_is_degraded(&outcome),
            policy.risk_recall_is_measured(&outcome),
            [1, 1, 1],
            policy.minimum_fresh_pairs_per_horizon,
        );
    }
    assert_eq!(
        state,
        PolicyState::Memory(MemoryLifecycle::Candidate),
        "unmeasured outcomes must hold Candidate, neither retiring it nor promoting it"
    );

    // With risk recall actually measured and passing, the same chain promotes.
    let mut measured = outcome.clone();
    for window in &mut measured.windows {
        window.evidence_completeness_ppm = 1_000_000;
        window.risk_recall_ppm = Some(1_000_000);
    }
    assert!(!policy.outcome_is_degraded(&measured));
    assert!(policy.risk_recall_is_measured(&measured));
    let promoted = next_state_with_fresh_pairs(
        PolicyState::Memory(MemoryLifecycle::Candidate),
        None,
        false,
        true,
        [1, 1, 1],
        policy.minimum_fresh_pairs_per_horizon,
    );
    assert_eq!(
        promoted,
        PolicyState::Memory(MemoryLifecycle::Active),
        "measured, passing risk recall is what unlocks Candidate -> Active"
    );
}

#[test]
fn unmeasured_risk_recall_is_not_degradation() {
    let policy = EvaluationPolicy::default();
    let mut outcome = materialize_outcome(&materialization()).unwrap();
    for window in &mut outcome.windows {
        window.evidence_completeness_ppm = 1_000_000;
        window.risk_recall_ppm = None;
    }
    assert!(
        !policy.outcome_is_degraded(&outcome),
        "unmeasured risk recall must not count as degradation"
    );
    assert!(
        !policy.risk_recall_is_measured(&outcome),
        "unmeasured risk recall must still block promotion"
    );

    for window in &mut outcome.windows {
        window.risk_recall_ppm = Some(0);
    }
    assert!(
        policy.outcome_is_degraded(&outcome),
        "a measured zero recall is real degradation"
    );
    assert!(policy.risk_recall_is_measured(&outcome));

    for window in &mut outcome.windows {
        window.risk_recall_ppm = Some(1_000_000);
    }
    assert!(!policy.outcome_is_degraded(&outcome));
    assert!(policy.risk_recall_is_measured(&outcome));
}
