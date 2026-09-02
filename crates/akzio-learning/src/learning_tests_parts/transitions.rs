#[test]
fn forced_canary_state_records_topology_policy_transition() {
    let fixture = RuntimeFixture::new();
    let subject = PolicySubject::Topology(TopologyId(fixture.candidate_topology_id.clone()));
    let permit = fixture.claim_evaluation("topology-canary-transition");
    fixture.record_pair_batch_for(&permit, 0, &subject);

    let result = fixture
        .runtime
        .evaluate_with_lease_at_state(
            None,
            EvaluationInput {
                permit,
                subject: subject.clone(),
                hypothesis_id: "topology-canary-transition".to_owned(),
                materialization: fixture.materialization.clone(),
                contract_hash: fixture.candidate_contract_hash.clone(),
                topology_id: TopologyId(fixture.candidate_topology_id.clone()),
                candidate_policy: Some(CandidatePolicyInput {
                    baseline: fixture.active_topology.clone(),
                    candidate: fixture.candidate_topology.clone(),
                }),
                token_cost: None,
                latency_millis: None,
            },
            None,
            PolicyState::Topology(CandidatePolicyState::Canary10),
        )
        .unwrap();

    assert_eq!(
        result.policy_head.as_ref().map(|head| head.state),
        Some(PolicyState::Topology(CandidatePolicyState::Canary10))
    );
    assert_eq!(fixture.store.policy_transitions(&subject).unwrap().len(), 1);
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn forced_canary_transitions_obey_threshold_but_rollback_does_not() {
    let fixture = RuntimeFixture::with_policy(EvaluationPolicy {
        minimum_fresh_pairs_per_horizon: 2,
        ..EvaluationPolicy::default()
    });
    let subject = PolicySubject::Topology(TopologyId(fixture.candidate_topology_id.clone()));
    let candidate = || {
        Some(CandidatePolicyInput {
            baseline: fixture.active_topology.clone(),
            candidate: fixture.candidate_topology.clone(),
        })
    };

    let promote = fixture.claim_evaluation("topology-canary-threshold-promote");
    fixture.record_pair_batch_for(&promote, 0, &subject);
    fixture.record_pair_batch_for(&promote, 1, &subject);
    let promoted = fixture.evaluate_at_state_for(
        promote,
        "topology-canary-threshold-promote",
        subject.clone(),
        candidate(),
        fixture.materialization.clone(),
        PolicyState::Topology(CandidatePolicyState::Canary10),
    );
    assert_eq!(promoted.fresh_pairs_by_horizon, [2, 2, 2]);
    assert_eq!(
        promoted.policy_head.as_ref().map(|head| head.state),
        Some(PolicyState::Topology(CandidatePolicyState::Canary10))
    );

    let blocked = fixture.claim_evaluation("topology-canary-threshold-blocked");
    fixture.record_pair_batch_for(&blocked, 2, &subject);
    let blocked = fixture.evaluate_at_state_for(
        blocked,
        "topology-canary-threshold-blocked",
        subject.clone(),
        candidate(),
        fixture.materialization.clone(),
        PolicyState::Topology(CandidatePolicyState::Canary25),
    );
    assert_eq!(blocked.fresh_pairs_by_horizon, [1, 1, 1]);
    assert_eq!(
        blocked.policy_head.as_ref().map(|head| head.state),
        Some(PolicyState::Topology(CandidatePolicyState::Canary10))
    );
    assert_eq!(
        fixture
            .store
            .artifact(&blocked.evaluation.artifact_id)
            .unwrap()
            .artifact_id,
        blocked.evaluation.artifact_id
    );

    let rollback = fixture.claim_evaluation("topology-canary-threshold-rollback");
    let rolled_back = fixture.evaluate_at_state_for(
        rollback,
        "topology-canary-threshold-rollback",
        subject.clone(),
        candidate(),
        fixture.materialization.clone(),
        PolicyState::Topology(CandidatePolicyState::Candidate),
    );
    assert_eq!(rolled_back.fresh_pairs_by_horizon, [0, 0, 0]);
    assert_eq!(
        rolled_back.policy_head.as_ref().map(|head| head.state),
        Some(PolicyState::Topology(CandidatePolicyState::Candidate))
    );
    assert_eq!(fixture.store.policy_transitions(&subject).unwrap().len(), 2);
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn missing_fresh_pair_horizon_keeps_evaluation_without_transition() {
    let fixture = RuntimeFixture::new();
    let permit = fixture.claim_evaluation("missing-fresh-pair-horizon");
    fixture.record_pair_horizons_for(
        &permit,
        0,
        &fixture.subject,
        [OutcomeHorizon::T1, OutcomeHorizon::T3],
    );

    let result = fixture.evaluate(permit, "missing-fresh-pair-horizon");

    assert_eq!(result.fresh_pairs_by_horizon, [1, 1, 0]);
    assert!(result.policy_head.is_none());
    assert_eq!(
        fixture
            .store
            .artifact(&result.evaluation.artifact_id)
            .unwrap()
            .artifact_id,
        result.evaluation.artifact_id
    );
    assert!(fixture
        .store
        .policy_transitions(&fixture.subject)
        .unwrap()
        .is_empty());
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn contract_candidate_materializes_a_bound_policy_artifact() {
    let fixture = RuntimeFixture::new();
    let now = fixture_time();
    let (baseline, baseline_ref) = fixture_contract(&fixture.store, "baseline", now);
    let (candidate, candidate_ref) = fixture_contract(&fixture.store, "candidate", now);
    assert!(baseline.permits_candidate(&candidate));
    let subject = PolicySubject::Contract(candidate.contract_hash);
    let permit = fixture.claim_evaluation("contract-candidate");
    let task_id = permit.task_id.clone();
    let result = fixture.evaluate_for(
        permit,
        "contract-candidate",
        subject.clone(),
        Some(CandidatePolicyInput {
            baseline: baseline_ref.clone(),
            candidate: candidate_ref.clone(),
        }),
        fixture.materialization.clone(),
    );
    assert_eq!(result.fresh_pairs_by_horizon, [0, 0, 0]);
    assert!(result.policy_head.is_none());
    let policy_ref = result.candidate_policy.unwrap();
    let artifact = fixture.store.artifact(&policy_ref.artifact_id).unwrap();
    let policy: CandidatePolicy =
        serde_json::from_slice(&fixture.store.read_blob(&artifact.blob).unwrap()).unwrap();
    assert_eq!(policy.subject, subject);
    assert_eq!(policy.baseline, baseline_ref);
    assert_eq!(policy.candidate, candidate_ref);
    assert_eq!(policy.source_evaluation, result.evaluation);
    assert!(fixture
        .store
        .committed_task_outputs(&fixture.paper_run_id, &task_id)
        .unwrap()
        .iter()
        .any(|output| output.artifact_id == policy_ref.artifact_id));
    fixture.store.verify_integrity().unwrap();
}
