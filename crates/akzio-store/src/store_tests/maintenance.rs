#[test]
fn maintenance_defers_only_leases_live_when_the_window_started() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let mut workflow = graph();
    let mut second = workflow.nodes[0].clone();
    second.task_id = TaskId::new();
    second.objective = "second maintenance fixture".to_owned();
    workflow.nodes.push(second);
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&workflow).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Debug,
        topology_id: workflow.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: Utc::now(),
    };
    store
        .commit_workflow(&WorkflowCommit {
            run,
            graph: graph_artifact,
            nodes: workflow.nodes,
        })
        .unwrap();
    let live = store
        .claim_next_task("live-worker", Utc::now(), Duration::seconds(30))
        .unwrap()
        .unwrap();
    let expired = store
        .claim_next_task("expired-worker", Utc::now(), Duration::seconds(30))
        .unwrap()
        .unwrap();
    let started_at = Utc::now() + Duration::seconds(5);
    store
        .heartbeat_task(&live.permit, started_at + Duration::seconds(10))
        .unwrap();
    store
        .heartbeat_task(&expired.permit, started_at - Duration::seconds(1))
        .unwrap();
    let daemon = store
        .acquire_daemon_lease(
            "fixture-maintenance",
            "fixture-owner",
            started_at,
            started_at + Duration::seconds(10),
        )
        .unwrap()
        .unwrap();

    let deferred = store
        .defer_live_leases_for_maintenance(started_at, started_at + Duration::seconds(20))
        .unwrap();

    assert_eq!(deferred.task_leases, 1);
    assert_eq!(deferred.daemon_leases, 1);
    assert_eq!(
        store
            .recover_expired_tasks(started_at + Duration::seconds(15))
            .unwrap(),
        1
    );
    store
        .validate_daemon_lease(&daemon, started_at + Duration::seconds(25))
        .unwrap();
}
