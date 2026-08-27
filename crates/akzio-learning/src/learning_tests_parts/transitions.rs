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
fn contract_candidate_materializes_a_bound_policy_artifact() {
    let fixture = RuntimeFixture::new();
    let now = fixture_time();
    let (baseline, baseline_ref) = fixture_contract(&fixture.store, "baseline", now);
    let (candidate, candidate_ref) = fixture_contract(&fixture.store, "candidate", now);
    assert!(baseline.permits_candidate(&candidate));
    let subject = PolicySubject::Contract(candidate.contract_hash.clone());
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
