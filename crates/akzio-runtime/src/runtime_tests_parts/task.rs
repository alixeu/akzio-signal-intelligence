#[tokio::test]
async fn task_runtime_accepts_only_store_verified_committed_attempts() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let workflow = WorkflowRuntime::new(store.clone(), catalogue());
    let graph = workflow.bootstrap(RunPurpose::Debug, "active").unwrap();
    let run_id = RunId::new();
    let first = workflow
        .submit(run_id, RunPurpose::Debug, graph.clone(), Utc::now())
        .unwrap();
    let tasks = TaskRuntime::new(store.clone());
    let handler_store = store.clone();
    let handler_workflow = workflow.clone();
    assert!(tasks
        .run_one("planner-worker", move |planner| {
            let planner_output = planner_output_artifact(&handler_store, &planner, Utc::now());
            handler_workflow
                .apply_planner_output(&planner, &first, &graph, &planner_output, Utc::now())
                .unwrap();
            async { TaskCompletion::Committed }
        })
        .await
        .unwrap());

    assert!(matches!(
        tasks
            .run_one("untrusted-worker", |_| async { TaskCompletion::Committed })
            .await,
        Err(RuntimeError::Store(StoreError::StalePermit(_)))
    ));
}

#[tokio::test]
async fn task_runtime_retries_then_commits_outputs_with_a_new_attempt() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let workflow = WorkflowRuntime::new(store.clone(), catalogue());
    let graph = workflow.lower(RunPurpose::Debug, &proposal()).unwrap();
    let run_id = RunId::new();
    workflow
        .submit(run_id.clone(), RunPurpose::Debug, graph, Utc::now())
        .unwrap();
    let tasks = TaskRuntime::new(store.clone())
        .with_lease_duration(Duration::seconds(3))
        .unwrap();
    assert!(tasks
        .run_one("worker", |_| async {
            TaskCompletion::Retry(RetryCause::Transport)
        })
        .await
        .unwrap());
    assert!(tasks
        .run_one("worker", move |task| {
            let artifact = task_artifact(&store, &task, Utc::now());
            async move { TaskCompletion::Succeeded(vec![artifact]) }
        })
        .await
        .unwrap());
    let events = tasks.store().events_after(&run_id, 0, 100).unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == "task.retry_scheduled")
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == "task.started")
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == "task.succeeded")
            .count(),
        1
    );
    let first_page = tasks.store().events_after(&run_id, 0, 2).unwrap();
    let cursor = first_page.last().unwrap().cursor;
    let mut replay = first_page;
    replay.extend(tasks.store().events_after(&run_id, cursor, 100).unwrap());
    assert_eq!(replay, events);
    assert_eq!(
        workflow.replay_run(&run_id).unwrap(),
        tasks.store().workflow_snapshot(&run_id).unwrap()
    );
}

#[tokio::test]
async fn task_runtime_replays_exhausted_retry_as_terminal_failure() {
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
    let tasks = TaskRuntime::new(store);

    assert!(tasks
        .run_one("worker", |_| async {
            TaskCompletion::Retry(RetryCause::Transport)
        })
        .await
        .unwrap());

    let events = tasks.store().events_after(&run_id, 0, 100).unwrap();
    let exhausted = events
        .iter()
        .find(|event| event.event_type == "task.retry_exhausted")
        .unwrap();
    let task_id = exhausted.task_id.as_ref().unwrap();
    let attempt_id = exhausted.attempt_id.as_ref().unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.task_id.as_ref() == Some(task_id))
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["task.started", "task.retry_exhausted", "task.failed"]
    );
    assert_eq!(
        events
            .iter()
            .find(|event| {
                event.task_id.as_ref() == Some(task_id) && event.event_type == "task.failed"
            })
            .unwrap()
            .attempt_id
            .as_ref(),
        Some(attempt_id)
    );
    let snapshot = tasks.store().workflow_snapshot(&run_id).unwrap();
    let failed = snapshot
        .tasks
        .iter()
        .find(|task| &task.node.task_id == task_id)
        .unwrap();
    assert_eq!(failed.status, TaskStatus::Failed);
    assert_eq!(failed.attempt_count, 1);
    assert_eq!(workflow.replay_run(&run_id).unwrap(), snapshot);
}

#[tokio::test]
async fn task_runtime_recovery_is_explicit_and_honors_cancel_requests() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let workflow = WorkflowRuntime::new(store.clone(), catalogue());
    let graph = workflow.lower(RunPurpose::Debug, &proposal()).unwrap();
    let run_id = RunId::new();
    workflow
        .submit(run_id.clone(), RunPurpose::Debug, graph, Utc::now())
        .unwrap();
    let abandoned = store
        .claim_next_task("crashed-worker", Utc::now(), Duration::milliseconds(-1))
        .unwrap()
        .unwrap();
    let abandoned_task_id = abandoned.node.task_id.clone();
    let before_recovery = workflow.recover(&run_id).unwrap();
    assert_eq!(
        before_recovery
            .tasks
            .iter()
            .find(|task| task.node.task_id == abandoned_task_id)
            .unwrap()
            .active_attempt
            .as_ref()
            .unwrap()
            .permit,
        abandoned.permit
    );
    let tasks = TaskRuntime::new(store.clone())
        .with_lease_duration(Duration::seconds(3))
        .unwrap();
    let old_permit = abandoned.permit.clone();
    let old_attempt_id = old_permit.attempt_id.clone();
    let old_epoch = old_permit.epoch;
    assert!(!tasks
        .run_one("pre-recovery-worker", |_| async {
            panic!("run_one must not recover expired tasks")
        })
        .await
        .unwrap());
    assert_eq!(
        tasks.recover_expired_tasks(Utc::now()).await.unwrap(),
        1
    );
    assert!(tasks
        .run_one("recovery-worker", move |task| {
            assert_ne!(task.permit.attempt_id, old_attempt_id);
            assert!(task.permit.epoch > old_epoch);
            let artifact = task_artifact(&store, &task, Utc::now());
            async move { TaskCompletion::Succeeded(vec![artifact]) }
        })
        .await
        .unwrap());
    let after_recovery = workflow.recover(&run_id).unwrap();
    assert_eq!(after_recovery.revision, before_recovery.revision);
    assert_eq!(
        after_recovery
            .tasks
            .iter()
            .map(|task| task.node.task_id.clone())
            .collect::<BTreeSet<_>>(),
        before_recovery
            .tasks
            .iter()
            .map(|task| task.node.task_id.clone())
            .collect::<BTreeSet<_>>()
    );
    let recovered_task = after_recovery
        .tasks
        .iter()
        .find(|task| task.node.task_id == abandoned_task_id)
        .unwrap();
    assert_eq!(recovered_task.status, TaskStatus::Succeeded);
    assert_eq!(recovered_task.attempt_count, 2);
    assert!(recovered_task.active_attempt.is_none());
    assert_eq!(workflow.replay_run(&run_id).unwrap(), after_recovery);
    assert!(matches!(
        tasks
            .store()
            .finish_task(&old_permit, TaskStatus::Skipped, Utc::now()),
        Err(StoreError::StalePermit(_))
    ));
    assert!(tasks
        .store()
        .request_run_cancel(&run_id, "operator", Utc::now())
        .unwrap());
    assert!(!tasks
        .store()
        .request_run_cancel(&run_id, "operator", Utc::now())
        .unwrap());
    assert!(!tasks
        .run_one("cancelled-worker", |_| async {
            panic!("cancelled run must not dispatch")
        })
        .await
        .unwrap());
    let events = tasks.store().events_after(&run_id, 0, 100).unwrap();
    assert!(events
        .iter()
        .any(|event| event.event_type == "task.recovered"));
    assert!(events
        .iter()
        .any(|event| event.event_type == "run.cancel_requested"));
    assert_eq!(
        workflow.replay_run(&run_id).unwrap(),
        tasks.store().workflow_snapshot(&run_id).unwrap()
    );
}
