#[test]
fn task_runtime_replays_exhausted_recovery_as_terminal_failure() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let mut recipes = catalogue();
    recipes
        .recipes
        .values_mut()
        .for_each(|recipe| recipe.retry.max_attempts = 1);
    let workflow = WorkflowRuntime::new(store.clone(), recipes);
    let graph = workflow.lower(RunPurpose::Debug, &proposal()).unwrap();
    let run_id = RunId::new();
    workflow
        .submit(run_id.clone(), RunPurpose::Debug, graph, Utc::now())
        .unwrap();
    let abandoned = store
        .claim_next_task("crashed-worker", Utc::now(), Duration::milliseconds(-1))
        .unwrap()
        .unwrap();
    store.recover_expired_tasks(Utc::now()).unwrap();

    let events = store.events_after(&run_id, 0, 100).unwrap();
    let task_events = events
        .iter()
        .filter(|event| event.task_id.as_ref() == Some(&abandoned.node.task_id))
        .collect::<Vec<_>>();
    assert_eq!(
        task_events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["task.started", "task.recovery_exhausted", "task.failed"]
    );
    assert_eq!(
        task_events[1].attempt_id.as_ref(),
        Some(&abandoned.permit.attempt_id)
    );
    assert_eq!(
        task_events[2].attempt_id.as_ref(),
        Some(&abandoned.permit.attempt_id)
    );
    let snapshot = store.workflow_snapshot(&run_id).unwrap();
    let failed = snapshot
        .tasks
        .iter()
        .find(|task| task.node.task_id == abandoned.node.task_id)
        .unwrap();
    assert_eq!(failed.status, TaskStatus::Failed);
    assert_eq!(failed.attempt_count, 1);
    assert_eq!(workflow.replay_run(&run_id).unwrap(), snapshot);
}

#[test]
fn submit_rejects_graphs_that_bypass_or_mutate_rust_terminal_gates() {
    let root = tempdir().unwrap();
    let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), catalogue());
    let mut missing_gate = runtime.lower(RunPurpose::Debug, &proposal()).unwrap();
    missing_gate
        .nodes
        .retain(|node| node.recipe_id.as_str() != "gate.evaluate");
    missing_gate.validate().unwrap();
    assert!(matches!(
        runtime.submit(RunId::new(), RunPurpose::Debug, missing_gate, Utc::now()),
        Err(RuntimeError::MissingTerminalGate(_))
    ));

    let mut altered_gate = runtime.lower(RunPurpose::Debug, &proposal()).unwrap();
    altered_gate
        .nodes
        .iter_mut()
        .find(|node| node.recipe_id.as_str() == "gate.execution")
        .unwrap()
        .dependencies
        .clear();
    altered_gate.validate().unwrap();
    assert!(matches!(
        runtime.submit(RunId::new(), RunPurpose::Debug, altered_gate, Utc::now()),
        Err(RuntimeError::InvalidTerminalDependencies(_))
    ));
}

#[test]
fn submit_rejects_nodes_that_diverge_from_the_installed_recipe() {
    let root = tempdir().unwrap();
    let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), catalogue());
    let mut graph = runtime.lower(RunPurpose::Debug, &proposal()).unwrap();
    graph
        .nodes
        .iter_mut()
        .find(|node| node.recipe_id.as_str() == "research.analyst")
        .unwrap()
        .budget
        .max_output_tokens = 49;
    graph.validate().unwrap();
    assert!(matches!(
        runtime.submit(RunId::new(), RunPurpose::Debug, graph, Utc::now()),
        Err(RuntimeError::NodeRecipeMismatch(_))
    ));
}
