#[test]
fn workflow_commit_accepts_out_of_order_nodes_and_preserves_dependencies() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let mut graph = graph();
    let parent = graph.nodes[0].clone();
    let mut child = parent.clone();
    child.task_id = TaskId::new();
    child.objective = "dependent analysis".to_owned();
    child.dependencies = vec![parent.task_id.clone()];
    graph.nodes = vec![child, parent.clone()];
    graph.validate().unwrap();
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Debug,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: Utc::now(),
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let claimed = store
        .claim_next_task("worker", Utc::now(), Duration::seconds(30))
        .unwrap()
        .unwrap();
    assert_eq!(claimed.node.task_id, parent.task_id);
    store.verify_integrity().unwrap();
}

#[test]
fn retry_and_cancellation_are_durable_and_fenced() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let mut graph = graph();
    graph.nodes[0].retry.max_attempts = 2;
    graph.nodes[0].retry.initial_backoff_ms = 0;
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Debug,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: Utc::now(),
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let first = store
        .claim_next_task("worker-a", Utc::now(), Duration::seconds(30))
        .unwrap()
        .unwrap();
    assert_eq!(
        store
            .retry_task(&first.permit, Utc::now(), Utc::now())
            .unwrap(),
        RetryTaskResult::Requeued
    );
    let second = store
        .claim_next_task("worker-b", Utc::now(), Duration::seconds(30))
        .unwrap()
        .unwrap();
    assert_ne!(first.permit.attempt_id, second.permit.attempt_id);
    assert!(store
        .request_run_cancel(&run.run_id, "operator", Utc::now())
        .unwrap());
    assert!(store.run_cancel_requested(&run.run_id).unwrap());
    assert!(matches!(
        store.finish_task(&first.permit, TaskStatus::Cancelled, Utc::now()),
        Err(StoreError::StalePermit(_))
    ));
    store
        .finish_task(&second.permit, TaskStatus::Cancelled, Utc::now())
        .unwrap();
    let events = store.events_after(&run.run_id, 0, 100).unwrap();
    assert!(events
        .iter()
        .any(|event| event.event_type == "task.retry_scheduled"));
    assert!(events
        .iter()
        .any(|event| event.event_type == "run.cancel_requested"));
    store.verify_integrity().unwrap();
}

#[test]
fn workflow_snapshot_ignores_dependency_ordering() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let mut graph = graph();
    let mut first = graph.nodes.remove(0);
    first.task_id = TaskId("task-b".to_owned());
    let mut second = first.clone();
    second.task_id = TaskId("task-a".to_owned());
    second.recipe_id = TaskRecipeId::new("research.synthesizer").unwrap();
    let mut child = first.clone();
    child.task_id = TaskId("task-c".to_owned());
    child.recipe_id = TaskRecipeId::new("gate.decision").unwrap();
    child.dependencies = vec![first.task_id.clone(), second.task_id.clone()];
    graph.nodes = vec![first.clone(), second.clone(), child.clone()];
    graph.validate().unwrap();

    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Debug,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: Utc::now(),
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();

    let snapshot = store.workflow_snapshot(&run.run_id).unwrap();
    let stored_child = snapshot
        .tasks
        .iter()
        .find(|task| task.node.task_id == child.task_id)
        .unwrap();
    assert_eq!(
        stored_child.node.dependencies,
        vec![second.task_id, first.task_id]
    );
}

#[test]
fn workflow_commit_is_atomic_and_claim_yields_a_permit() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let graph = graph();
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Debug,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: Utc::now(),
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes.clone(),
        })
        .unwrap();
    let claimed = store
        .claim_next_task("worker", Utc::now(), Duration::seconds(30))
        .unwrap()
        .unwrap();
    assert_eq!(claimed.run_id, run.run_id);
    assert_eq!(claimed.node.task_id, graph.nodes[0].task_id);
    assert_eq!(store.events_after(&run.run_id, 0, 10).unwrap().len(), 2);
    store.verify_integrity().unwrap();
}

#[test]
fn attempt_commit_is_atomic_with_outputs_and_terminal_event() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let graph = graph();
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Debug,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: Utc::now(),
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let claimed = store
        .claim_next_task("worker", Utc::now(), Duration::seconds(30))
        .unwrap()
        .unwrap();
    let turn = artifact(
        &store,
        ArtifactKind::AgentTurn,
        "intermediate turn",
        Some(ArtifactOrigin {
            run_id: Some(claimed.permit.run_id.clone()),
            task_id: Some(claimed.permit.task_id.clone()),
            attempt_id: Some(claimed.permit.attempt_id.clone()),
            contract_hash: None,
        }),
    );
    store
        .write_task_artifact(
            &claimed.permit,
            &turn,
            LifecycleEventType::AgentTurn,
            Utc::now(),
        )
        .unwrap();
    assert!(matches!(
        store.committed_attempt_outputs(&claimed.permit.task_id, &claimed.permit.attempt_id),
        Err(StoreError::CommittedOutputAttempt { .. })
    ));
    let evidence = artifact(
        &store,
        ArtifactKind::NormalizedEvidence,
        "claim evidence",
        Some(ArtifactOrigin {
            run_id: Some(claimed.permit.run_id.clone()),
            task_id: Some(claimed.permit.task_id.clone()),
            attempt_id: Some(claimed.permit.attempt_id.clone()),
            contract_hash: None,
        }),
    );
    let output = artifact_with_refs(
        &store,
        ArtifactKind::Claim,
        "claim",
        Some(ArtifactOrigin {
            run_id: Some(claimed.permit.run_id.clone()),
            task_id: Some(claimed.permit.task_id.clone()),
            attempt_id: Some(claimed.permit.attempt_id.clone()),
            contract_hash: None,
        }),
        vec![artifact_ref(&evidence)],
    );

    store
        .commit_attempt(
            &claimed.permit,
            &[evidence.clone(), output.clone()],
            TaskStatus::Succeeded,
            Utc::now(),
        )
        .unwrap();

    assert_eq!(
        store
            .committed_attempt_outputs(&claimed.permit.task_id, &claimed.permit.attempt_id)
            .unwrap(),
        vec![evidence.clone(), output.clone()]
    );
    assert_eq!(
        store
            .committed_task_outputs(&run.run_id, &claimed.permit.task_id)
            .unwrap(),
        vec![evidence, output]
    );
    assert_eq!(store.events_after(&run.run_id, 0, 10).unwrap().len(), 6);
    assert!(store
        .claim_next_task("worker", Utc::now(), Duration::seconds(30))
        .unwrap()
        .is_none());
    store.verify_integrity().unwrap();
}

#[test]
fn attempt_commit_resolves_same_batch_evidence_closure_before_persisting() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let graph = graph();
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Debug,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: Utc::now(),
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let claimed = store
        .claim_next_task("worker", Utc::now(), Duration::seconds(30))
        .unwrap()
        .unwrap();
    let origin = Some(ArtifactOrigin {
        run_id: Some(claimed.permit.run_id.clone()),
        task_id: Some(claimed.permit.task_id.clone()),
        attempt_id: Some(claimed.permit.attempt_id.clone()),
        contract_hash: None,
    });
    let raw = artifact(&store, ArtifactKind::RawEvidence, "raw", origin.clone());
    let normalized = Artifact::new(
        ArtifactKind::NormalizedEvidence,
        store.put_bytes(b"normalized", "application/json").unwrap(),
        "fixture.normalized",
        ArtifactLifecycle::RunScoped,
        raw.provenance.clone(),
        origin.clone(),
        vec![ArtifactRef {
            artifact_id: raw.artifact_id.clone(),
            kind: ArtifactKind::RawEvidence,
        }],
        Utc::now(),
    )
    .unwrap();
    let missing = Artifact::new(
        ArtifactKind::NormalizedEvidence,
        store.put_bytes(b"missing", "application/json").unwrap(),
        "fixture.normalized",
        ArtifactLifecycle::RunScoped,
        raw.provenance.clone(),
        origin,
        vec![ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"missing raw")),
            kind: ArtifactKind::RawEvidence,
        }],
        Utc::now(),
    )
    .unwrap();

    assert!(matches!(
        store.commit_attempt(
            &claimed.permit,
            std::slice::from_ref(&missing),
            TaskStatus::Succeeded,
            Utc::now(),
        ),
        Err(StoreError::InvalidArtifactClosure(_))
    ));
    assert!(matches!(
        store.artifact(&missing.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));

    store
        .commit_attempt(
            &claimed.permit,
            &[normalized.clone(), raw.clone()],
            TaskStatus::Succeeded,
            Utc::now(),
        )
        .unwrap();
    assert_eq!(
        store
            .committed_task_outputs(&run.run_id, &claimed.permit.task_id)
            .unwrap(),
        vec![normalized, raw]
    );
    store.verify_integrity().unwrap();
}

#[test]
fn attempt_commit_rolls_back_when_terminal_event_write_fails() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let graph = graph();
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Debug,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: Utc::now(),
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let claimed = store
        .claim_next_task("worker", Utc::now(), Duration::seconds(30))
        .unwrap()
        .unwrap();
    let evidence = artifact(
        &store,
        ArtifactKind::NormalizedEvidence,
        "claim evidence",
        Some(ArtifactOrigin {
            run_id: Some(claimed.permit.run_id.clone()),
            task_id: Some(claimed.permit.task_id.clone()),
            attempt_id: Some(claimed.permit.attempt_id.clone()),
            contract_hash: None,
        }),
    );
    let output = artifact_with_refs(
        &store,
        ArtifactKind::Claim,
        "claim",
        Some(ArtifactOrigin {
            run_id: Some(claimed.permit.run_id.clone()),
            task_id: Some(claimed.permit.task_id.clone()),
            attempt_id: Some(claimed.permit.attempt_id.clone()),
            contract_hash: None,
        }),
        vec![artifact_ref(&evidence)],
    );
    {
        let connection = store.connection.lock().unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER fail_terminal_event BEFORE INSERT ON rebuild_events
                     WHEN NEW.event_type = 'task.succeeded'
                     BEGIN SELECT RAISE(ABORT, 'injected terminal event failure'); END;",
            )
            .unwrap();
    }
    assert!(matches!(
        store.commit_attempt(
            &claimed.permit,
            &[evidence.clone(), output.clone()],
            TaskStatus::Succeeded,
            Utc::now()
        ),
        Err(StoreError::Sql(_))
    ));
    {
        let connection = store.connection.lock().unwrap();
        connection
            .execute_batch("DROP TRIGGER fail_terminal_event;")
            .unwrap();
    }
    assert!(matches!(
        store.artifact(&output.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
    assert_eq!(store.events_after(&run.run_id, 0, 10).unwrap().len(), 2);
    store
        .commit_attempt(
            &claimed.permit,
            &[evidence, output],
            TaskStatus::Succeeded,
            Utc::now(),
        )
        .unwrap();
    store.verify_integrity().unwrap();
}
