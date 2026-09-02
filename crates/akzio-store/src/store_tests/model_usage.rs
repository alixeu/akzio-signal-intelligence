// `token_cost` is the cost half of the utility/cost tradeoff, so its basis has
// to be what the provider actually charged. These tests pin the aggregation
// against the two ways a token basis silently lies: counting one provider call
// twice because several lifecycle events name it, and reporting a partial sum as
// if it were the whole bill.

fn agent_turn(
    fixture: &PolicyCommitFixture,
    turn: u32,
    telemetry: serde_json::Value,
) -> akzio_domain::Artifact {
    permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::AgentTurn,
        &serde_json::json!({
            "turn": turn,
            "contract_hash": "contract",
            "request": {"phase": "draft"},
            "response": {
                "assistant_text": "bounded fixture memo",
                "telemetry": telemetry
            }
        }),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    )
}

fn record_turn(
    fixture: &PolicyCommitFixture,
    artifact: &akzio_domain::Artifact,
    event: LifecycleEventType,
) {
    fixture
        .store
        .write_task_artifact(&fixture.permit, artifact, event, fixture.now)
        .unwrap();
}

#[test]
fn run_model_usage_counts_each_provider_call_once() {
    let fixture = PolicyCommitFixture::memory();
    let first = agent_turn(
        &fixture,
        1,
        serde_json::json!({
            "latency_millis": 300,
            "input_tokens": 100,
            "cached_input_tokens": 20,
            "output_tokens": 40,
            "reasoning_tokens": 7
        }),
    );
    record_turn(&fixture, &first, LifecycleEventType::AgentTurnCompleted);
    let second = agent_turn(
        &fixture,
        2,
        serde_json::json!({"latency_millis": 100, "input_tokens": 10, "output_tokens": 5}),
    );
    record_turn(&fixture, &second, LifecycleEventType::AgentTurnFailed);

    let usage = fixture
        .store
        .run_model_usage(&fixture.permit.run_id)
        .unwrap();
    assert_eq!(usage.turns, 2, "a failed turn still consumed input tokens");
    assert_eq!(usage.turns_missing_usage, 0);
    assert_eq!(usage.input_tokens, 110);
    assert_eq!(usage.cached_input_tokens, 20);
    assert_eq!(usage.output_tokens, 45);
    assert_eq!(usage.reasoning_tokens, 7);
    assert_eq!(usage.latency_millis, 400);
    // Cached input is cheaper, not free: dropping it would understate a
    // context-heavy arm.
    assert_eq!(usage.billable_tokens_if_complete(), Some(182));
}

/// A run with an unreported turn must yield no basis at all. In a token-matched
/// comparison an understated arm reads as the cheaper arm, which is the same
/// failure as passing an estimate off as a measurement.
#[test]
fn run_model_usage_withholds_the_basis_when_a_turn_went_unreported() {
    let fixture = PolicyCommitFixture::memory();
    let reported = agent_turn(
        &fixture,
        1,
        serde_json::json!({"latency_millis": 10, "input_tokens": 100, "output_tokens": 10}),
    );
    record_turn(&fixture, &reported, LifecycleEventType::AgentTurnCompleted);
    // Latency arrived but no token category did, which is exactly the daemon
    // path that used to report `token_cost: None` by hand.
    let unreported = agent_turn(&fixture, 2, serde_json::json!({"latency_millis": 20}));
    record_turn(&fixture, &unreported, LifecycleEventType::AgentTurnCompleted);

    let usage = fixture
        .store
        .run_model_usage(&fixture.permit.run_id)
        .unwrap();
    assert_eq!(usage.turns, 2);
    assert_eq!(usage.turns_missing_usage, 1);
    assert!(!usage.is_complete());
    assert_eq!(
        usage.billable_tokens(),
        110,
        "the observed part is still reportable as an observation"
    );
    assert_eq!(
        usage.billable_tokens_if_complete(),
        None,
        "but never as this run's cost"
    );
    assert_eq!(usage.latency_millis_if_complete(), None);
}

#[test]
fn run_with_no_model_turns_is_incomplete_rather_than_free() {
    let fixture = PolicyCommitFixture::memory();
    let usage = fixture
        .store
        .run_model_usage(&fixture.permit.run_id)
        .unwrap();
    assert_eq!(usage, RunModelUsage::default());
    assert!(!usage.is_complete());
    assert_eq!(usage.billable_tokens_if_complete(), None);
}

/// The cost that belongs beside a policy's realized utility is what producing the
/// decision spent, and the producing run is only recoverable from the artifact's
/// own origin.
#[test]
fn model_usage_is_charged_to_the_run_that_produced_the_artifact() {
    let fixture = PolicyCommitFixture::memory();
    let turn = agent_turn(
        &fixture,
        1,
        serde_json::json!({"latency_millis": 5, "input_tokens": 60, "output_tokens": 6}),
    );
    record_turn(&fixture, &turn, LifecycleEventType::AgentTurnCompleted);
    let decision = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::Decision,
        &serde_json::json!({"decision": "fixture"}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    record_turn(
        &fixture,
        &decision,
        LifecycleEventType::FixtureGenericWrite,
    );

    let usage = fixture
        .store
        .model_usage_for_producing_run(&artifact_ref(&decision))
        .unwrap();
    assert_eq!(usage.billable_tokens_if_complete(), Some(66));

    // A bootstrap artifact has no run origin, so it can carry no run cost.
    let orphan = fixture
        .store
        .write_freeze_state(false, "usage fixture", fixture.now)
        .unwrap();
    assert!(orphan.origin.is_none());
    let usage = fixture
        .store
        .model_usage_for_producing_run(&artifact_ref(&orphan))
        .unwrap();
    assert!(!usage.is_complete());
    assert_eq!(usage.billable_tokens(), 0);
}
