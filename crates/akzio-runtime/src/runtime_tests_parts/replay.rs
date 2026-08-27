#[test]
fn replay_rejects_unknown_durable_event_types() {
    let root = tempdir().unwrap();
    let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), catalogue());
    let run_id = RunId::new();
    let event = StoredEvent {
        cursor: 1,
        run_id: run_id.clone(),
        task_id: None,
        attempt_id: None,
        event_type: "unknown.replay.event".to_owned(),
        artifact_id: None,
        created_at: Utc::now(),
    };

    assert!(matches!(
        runtime.reduce_event(&run_id, &mut ReplayedWorkflow::default(), &event),
        Err(RuntimeError::ReplayDiverged { .. })
    ));
}

#[test]
fn replay_accepts_task_artifact_trace_events_with_matching_origin() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let runtime = WorkflowRuntime::new(store.clone(), catalogue());
    let run_id = RunId::new();
    let now = Utc::now();
    runtime
        .submit(
            run_id.clone(),
            RunPurpose::Debug,
            runtime.bootstrap(RunPurpose::Debug, "active").unwrap(),
            now,
        )
        .unwrap();
    let claimed = store
        .claim_next_task("trace-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap();
    store
        .append_task_event(&claimed.permit, LifecycleEventType::AgentTurnStarted, now)
        .unwrap();
    let artifact = task_artifact(&store, &claimed, now);
    store
        .write_task_artifact(
            &claimed.permit,
            &artifact,
            LifecycleEventType::AgentTurnCompleted,
            now,
        )
        .unwrap();

    assert_eq!(
        runtime.replay_run(&run_id).unwrap(),
        store.workflow_snapshot(&run_id).unwrap()
    );
}

#[test]
fn replay_rejects_snapshot_task_divergence() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let runtime = WorkflowRuntime::new(store.clone(), catalogue());
    let graph = runtime.lower(RunPurpose::Debug, &proposal()).unwrap();
    let run_id = RunId::new();
    runtime
        .submit(run_id.clone(), RunPurpose::Debug, graph, Utc::now())
        .unwrap();
    let replay = runtime.reduce_history(&run_id).unwrap();
    let mut forged = store.workflow_snapshot(&run_id).unwrap();
    forged.tasks[0].status = TaskStatus::Succeeded;

    assert!(matches!(
        runtime.validate_replay_snapshot(&run_id, &replay, &forged),
        Err(RuntimeError::ReplayDiverged { .. })
    ));
}

#[test]
fn planner_proposal_cannot_be_replayed_after_atomic_commit() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let workflow = WorkflowRuntime::new(store.clone(), catalogue());
    let graph = workflow.bootstrap(RunPurpose::Debug, "active").unwrap();
    let run_id = RunId::new();
    let first = workflow
        .submit(run_id.clone(), RunPurpose::Debug, graph.clone(), Utc::now())
        .unwrap();
    let planner = store
        .claim_next_task("planner-worker", Utc::now(), Duration::seconds(30))
        .unwrap()
        .unwrap();
    let planner_output = planner_output_artifact(&store, &planner, Utc::now());
    let second = workflow
        .apply_planner_output(&planner, &first, &graph, &planner_output, Utc::now())
        .unwrap();
    let patched: WorkflowGraph =
        serde_json::from_slice(&store.read_blob(&second.blob).unwrap()).unwrap();
    let events_before = store.events_after(&run_id, 0, 100).unwrap();

    assert!(matches!(
        workflow.apply_planner_output(&planner, &second, &patched, &planner_output, Utc::now(),),
        Err(RuntimeError::Store(StoreError::StalePermit(_)))
    ));
    assert_eq!(store.events_after(&run_id, 0, 100).unwrap(), events_before);
}
