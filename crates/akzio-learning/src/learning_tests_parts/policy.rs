#[test]
fn canonical_evaluation_promotes_memory_only_after_fresh_pairs() {
    let fixture = RuntimeFixture::new();
    let mut prior_cursor = 0;

    for batch in 0..3 {
        let permit = fixture.claim_evaluation(&format!("evaluation-disabled-{batch}"));
        fixture.record_pair_batch(&permit, batch);
        let result = fixture.evaluate(permit, "forward-transition-disabled");
        assert_eq!(result.fresh_pairs_by_horizon, [1, 1, 1]);
        let expected_state = if batch == 0 {
            PolicyState::Memory(MemoryLifecycle::Active)
        } else {
            PolicyState::Memory(MemoryLifecycle::Proven)
        };
        assert_eq!(
            result.policy_head.as_ref().map(|head| head.state),
            Some(expected_state)
        );

        let cursor = fixture
            .store
            .policy_shadow_pair_snapshot(&fixture.subject)
            .unwrap()
            .through_cursor;
        assert!(cursor > prior_cursor);
        prior_cursor = cursor;
    }

    let replay_permit = fixture.claim_evaluation("evaluation-old-pairs");
    let old_pairs = fixture.evaluate(replay_permit, "old-pairs-cannot-replay");
    assert_eq!(old_pairs.fresh_pairs_by_horizon, [0, 0, 0]);
    assert_eq!(
        old_pairs.policy_head.as_ref().map(|head| head.state),
        Some(PolicyState::Memory(MemoryLifecycle::Proven))
    );
    assert_eq!(
        fixture
            .store
            .policy_transitions(&fixture.subject)
            .unwrap()
            .len(),
        2
    );

    let evaluated = fixture
        .store
        .events_after(&fixture.paper_run_id, 0, 100)
        .unwrap()
        .into_iter()
        .filter(|event| event.event_type == "policy.evaluated")
        .count();
    assert_eq!(evaluated, 4);
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn topology_shadow_pair_must_name_the_candidate_subject() {
    let fixture = RuntimeFixture::new();
    let permit = fixture.claim_evaluation("structured-critique-mismatch");
    let subject = PolicySubject::Topology(TopologyId(
        STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID.to_owned(),
    ));
    let (candidate_decision, candidate_outcome) = &fixture.candidates[0];

    assert!(matches!(
        fixture.runtime.record_shadow_pair(
            &permit,
            &subject,
            ShadowObservation {
                parent_decision: fixture.parent_decision.clone(),
                execution_context: fixture.execution_context.clone(),
                candidate_decision: candidate_decision.clone(),
                candidate_contract_hash: fixture.candidate_contract_hash.clone(),
                candidate_topology_id: fixture.candidate_topology_id.clone(),
                horizon: OutcomeHorizon::T1,
                parent_outcome: fixture.parent_outcome.clone(),
                candidate_outcome: candidate_outcome.clone(),
                completed_at: fixture.pair_completed_at,
            },
        ),
        Err(EvaluationError::InvalidCandidatePolicy(
            "shadow_topology_id"
        ))
    ));
    assert!(fixture.store.policy_head(&subject).unwrap().is_none());
}

#[test]
fn topology_forward_promotion_is_disabled_and_degradation_rolls_back() {
    let fixture = RuntimeFixture::new();
    let subject = PolicySubject::Topology(TopologyId(fixture.candidate_topology_id.clone()));
    let permit = fixture.claim_evaluation("topology-forward-disabled");
    fixture.record_pair_batch_for(&permit, 0, &subject);

    let result = fixture.evaluate_for(
        permit,
        "topology-forward-disabled",
        subject.clone(),
        Some(CandidatePolicyInput {
            baseline: fixture.active_topology.clone(),
            candidate: fixture.candidate_topology.clone(),
        }),
        fixture.materialization.clone(),
    );

    assert_eq!(result.fresh_pairs_by_horizon, [1, 1, 1]);
    assert!(result.policy_head.is_none());
    assert!(result.candidate_policy.is_some());
    assert!(fixture
        .store
        .policy_transitions(&subject)
        .unwrap()
        .is_empty());
    assert_eq!(
        next_state_with_fresh_pairs(
            PolicyState::Topology(CandidatePolicyState::Canary50),
            None,
            true,
            true,
            [0, 0, 0],
            2,
        ),
        PolicyState::Topology(CandidatePolicyState::Candidate)
    );
    fixture.store.verify_integrity().unwrap();
}
