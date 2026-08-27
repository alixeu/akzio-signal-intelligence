#[test]
fn export_run_rejects_raw_model_payloads_for_non_debug_runs() {
    let fixture = task_artifact_fixture(RunPurpose::PaperDryRun);
    let export_parent = tempdir().unwrap();
    let target = export_parent.path().join("run-export");

    assert!(matches!(
        fixture.store.export_run(&fixture.run.run_id, &target, true),
        Err(StoreError::RawModelExportNotAllowed(
            RunPurpose::PaperDryRun
        ))
    ));
}

fn task_artifact_fixture_with_retry(purpose: RunPurpose, max_attempts: u8) -> TaskArtifactFixture {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let mut graph = graph();
    graph.nodes[0].retry.max_attempts = max_attempts;
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: now,
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let permit = store
        .claim_next_task("lifecycle-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    TaskArtifactFixture {
        _root: root,
        store,
        run,
        permit,
        now,
    }
}

fn agent_turn_artifact(fixture: &TaskArtifactFixture, label: &str) -> Artifact {
    permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::AgentTurn,
        &serde_json::json!({"label": label}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    )
}

#[test]
fn agent_turn_started_is_durable_and_duplicate_write_rolls_back() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    fixture
        .store
        .append_task_event(
            &fixture.permit,
            LifecycleEventType::AgentTurnStarted,
            fixture.now,
        )
        .unwrap();

    let events = fixture
        .store
        .events_after(&fixture.run.run_id, 0, 100)
        .unwrap();
    assert_eq!(events.len(), 3);
    let started = events
        .iter()
        .find(|event| event.event_type == LifecycleEventType::AgentTurnStarted.as_str())
        .unwrap();
    assert_eq!(started.task_id, Some(fixture.permit.task_id.clone()));
    assert_eq!(started.attempt_id, Some(fixture.permit.attempt_id.clone()));
    assert!(started.artifact_id.is_none());

    assert!(matches!(
        fixture.store.append_task_event(
            &fixture.permit,
            LifecycleEventType::AgentTurnStarted,
            fixture.now,
        ),
        Err(StoreError::Integrity(_))
    ));
    let after_duplicate = fixture
        .store
        .events_after(&fixture.run.run_id, 0, 100)
        .unwrap();
    assert_eq!(after_duplicate.len(), events.len());
    assert_eq!(
        after_duplicate
            .iter()
            .filter(|event| event.event_type == LifecycleEventType::AgentTurnStarted.as_str())
            .count(),
        1
    );
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn agent_turn_rejects_distinct_terminal_without_new_start() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    fixture
        .store
        .append_task_event(
            &fixture.permit,
            LifecycleEventType::AgentTurnStarted,
            fixture.now,
        )
        .unwrap();

    let completed = agent_turn_artifact(&fixture, "completed");
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &completed,
            LifecycleEventType::AgentTurnCompleted,
            fixture.now,
        )
        .unwrap();

    let failed = agent_turn_artifact(&fixture, "failed");
    assert!(matches!(
        fixture.store.write_task_artifact(
            &fixture.permit,
            &failed,
            LifecycleEventType::AgentTurnFailed,
            fixture.now,
        ),
        Err(StoreError::Integrity(_))
    ));

    let events = fixture
        .store
        .events_after(&fixture.run.run_id, 0, 100)
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == LifecycleEventType::AgentTurnFailed.as_str())
            .count(),
        0
    );
    assert!(matches!(
        fixture.store.artifact(&failed.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn agent_turn_started_rejects_stale_epoch_without_writing() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    let mut stale = fixture.permit.clone();
    stale.epoch += 1;
    let before = fixture
        .store
        .events_after(&fixture.run.run_id, 0, 100)
        .unwrap()
        .len();

    assert!(matches!(
        fixture
            .store
            .append_task_event(&stale, LifecycleEventType::AgentTurnStarted, fixture.now,),
        Err(StoreError::StalePermit(_))
    ));
    assert_eq!(
        fixture
            .store
            .events_after(&fixture.run.run_id, 0, 100)
            .unwrap()
            .len(),
        before
    );
    assert_eq!(
        fixture
            .store
            .workflow_snapshot(&fixture.run.run_id)
            .unwrap()
            .tasks[0]
            .status,
        TaskStatus::Running
    );
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn pending_agent_turn_blocks_success_until_completed() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    fixture
        .store
        .append_task_event(
            &fixture.permit,
            LifecycleEventType::AgentTurnStarted,
            fixture.now,
        )
        .unwrap();

    assert!(matches!(
        fixture
            .store
            .finish_task(&fixture.permit, TaskStatus::Succeeded, fixture.now),
        Err(StoreError::Integrity(_))
    ));
    assert_eq!(
        fixture
            .store
            .workflow_snapshot(&fixture.run.run_id)
            .unwrap()
            .tasks[0]
            .status,
        TaskStatus::Running
    );

    let turn = agent_turn_artifact(&fixture, "completed");
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &turn,
            LifecycleEventType::AgentTurnCompleted,
            fixture.now,
        )
        .unwrap();
    fixture
        .store
        .finish_task(&fixture.permit, TaskStatus::Succeeded, fixture.now)
        .unwrap();
    assert_eq!(
        fixture
            .store
            .workflow_snapshot(&fixture.run.run_id)
            .unwrap()
            .tasks[0]
            .status,
        TaskStatus::Succeeded
    );
    fixture.store.verify_integrity().unwrap();
}
