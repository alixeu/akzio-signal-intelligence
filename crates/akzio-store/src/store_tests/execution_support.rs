fn execution_commit_fixture() -> ExecutionCommitFixture {
    execution_commit_fixture_with_approval(None)
}

fn approved_execution_commit_fixture(
    maximum_notional: MoneyMicros,
    valid_for: Duration,
) -> ExecutionCommitFixture {
    execution_commit_fixture_with_approval(Some((maximum_notional, valid_for)))
}

fn execution_commit_fixture_with_approval(
    approval: Option<(MoneyMicros, Duration)>,
) -> ExecutionCommitFixture {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let lease = store
        .acquire_daemon_lease(
            "scheduler",
            "fixture-daemon",
            now,
            now + Duration::seconds(30),
        )
        .unwrap()
        .unwrap();
    let graph = graph();
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let session_key = if approval.is_some() {
        "2026-08-25"
    } else {
        "paper:fixture"
    };
    let reservation = SessionReservation {
        session_key: session_key.to_owned(),
        workflow: WorkflowCommit {
            run: StoredRun {
                run_id: RunId::new(),
                purpose: RunPurpose::Paper,
                topology_id: graph.topology_id.clone(),
                graph_artifact_id: graph_artifact.artifact_id.clone(),
                created_at: now,
            },
            graph: graph_artifact,
            nodes: graph.nodes,
        },
        setup_artifacts: vec![],
        reserved_at: now,
    };
    if let Some((maximum_notional, valid_for)) = approval {
        reserve_approved_test_session_with_limits(
            &store,
            &lease,
            &reservation,
            maximum_notional,
            now + valid_for,
        );
    } else {
        store.reserve_session_slot(&lease, &reservation).unwrap();
    }
    let permit = store
        .claim_next_task("fixture-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    let commitment = valid_execution_commitment(&store, &permit, session_key, now);
    ExecutionCommitFixture {
        _root: root,
        store,
        lease,
        permit,
        commitment,
        now,
    }
}
