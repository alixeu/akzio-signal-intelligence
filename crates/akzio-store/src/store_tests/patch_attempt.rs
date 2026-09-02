#[test]
fn workflow_patch_rolls_back_proposal_graph_tasks_events_and_planner_completion() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let planner_contract = ContentHash::of_bytes(b"planner-contract");
    let planner = WorkflowNode {
        task_id: TaskId::new(),
        recipe_id: TaskRecipeId::new("research.planner").unwrap(),
        contract_hash: Some(planner_contract.clone()),
        objective: "plan".to_owned(),
        dependencies: vec![],
        input_artifacts: vec![],
        priority: 90,
        budget: budget(),
        retry: retry(),
        on_failure: FailureDisposition::FailRun,
        parent_task_id: None,
    };
    let planner_task_id = planner.task_id.clone();
    let evidence = WorkflowNode {
        task_id: TaskId::new(),
        recipe_id: TaskRecipeId::new("gate.evidence").unwrap(),
        contract_hash: None,
        objective: "evidence gate".to_owned(),
        dependencies: vec![planner.task_id.clone()],
        input_artifacts: vec![],
        priority: 80,
        budget: budget(),
        retry: retry(),
        on_failure: FailureDisposition::FailRun,
        parent_task_id: None,
    };
    let decision = WorkflowNode {
        task_id: TaskId::new(),
        recipe_id: TaskRecipeId::new("gate.decision").unwrap(),
        contract_hash: None,
        objective: "decision gate".to_owned(),
        dependencies: vec![evidence.task_id.clone()],
        input_artifacts: vec![],
        priority: 70,
        budget: budget(),
        retry: retry(),
        on_failure: FailureDisposition::FailRun,
        parent_task_id: None,
    };
    let graph = WorkflowGraph {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "active".to_owned(),
        nodes: vec![planner.clone(), evidence.clone(), decision.clone()],
    };
    graph.validate().unwrap();
    let graph_artifact = Artifact::new(
        ArtifactKind::WorkflowGraph,
        store.put_json(&graph).unwrap(),
        "runtime.workflow",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.runtime".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        },
        None,
        vec![],
        now,
    )
    .unwrap();
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Debug,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: now,
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact.clone(),
            nodes: graph.nodes,
        })
        .unwrap();
    let claimed = store
        .claim_next_task("planner-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap();
    let evidence_need = artifact(
        &store,
        ArtifactKind::EvidenceNeed,
        "evidence need",
        Some(ArtifactOrigin {
            run_id: Some(run.run_id.clone()),
            task_id: Some(planner.task_id.clone()),
            attempt_id: Some(claimed.permit.attempt_id.clone()),
            contract_hash: claimed.permit.contract_hash.clone(),
        }),
    );
    let evidence_need_ref = ArtifactRef {
        artifact_id: evidence_need.artifact_id.clone(),
        kind: ArtifactKind::EvidenceNeed,
    };
    let planner_output = artifact(
        &store,
        ArtifactKind::WorkflowProposalDraft,
        "planner output",
        Some(ArtifactOrigin {
            run_id: Some(run.run_id.clone()),
            task_id: Some(planner.task_id.clone()),
            attempt_id: Some(claimed.permit.attempt_id.clone()),
            contract_hash: claimed.permit.contract_hash.clone(),
        }),
    );

    let proposal = WorkflowProposal {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "active".to_owned(),
        tasks: std::collections::BTreeMap::from([(
            "analyst".to_owned(),
            WorkflowProposalTask {
                recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                objective: "analyse".to_owned(),
                depends_on: vec![],
                priority: 60,
                evidence_needs: vec![evidence_need_ref.clone()],
            },
        )]),
        stop_reason: None,
    };
    let proposal_artifact = Artifact::new(
        ArtifactKind::WorkflowProposal,
        store.put_json(&proposal).unwrap(),
        "agent.planner",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.agent".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: Some(planner_contract),
        },
        Some(ArtifactOrigin {
            run_id: Some(run.run_id.clone()),
            task_id: Some(planner.task_id.clone()),
            attempt_id: Some(claimed.permit.attempt_id.clone()),
            contract_hash: claimed.permit.contract_hash.clone(),
        }),
        vec![
            ArtifactRef {
                artifact_id: planner_output.artifact_id.clone(),
                kind: ArtifactKind::WorkflowProposalDraft,
            },
            evidence_need_ref.clone(),
        ],
        now,
    )
    .unwrap();
    let added = WorkflowNode {
        task_id: TaskId::new(),
        recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
        contract_hash: Some(ContentHash::of_bytes(b"analyst-contract")),
        objective: "analyse".to_owned(),
        dependencies: vec![evidence.task_id.clone()],
        input_artifacts: vec![evidence_need_ref.clone()],
        priority: 60,
        budget: budget(),
        retry: retry(),
        on_failure: FailureDisposition::FailRun,
        parent_task_id: None,
    };
    let mut updated_evidence = evidence;
    updated_evidence.input_artifacts = vec![evidence_need_ref.clone()];
    let mut updated_decision = decision;
    updated_decision.dependencies = vec![added.task_id.clone()];
    let next_graph = WorkflowGraph {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "active".to_owned(),
        nodes: vec![
            planner,
            updated_evidence.clone(),
            added.clone(),
            updated_decision.clone(),
        ],
    };
    next_graph.validate().unwrap();
    let next_graph_artifact = Artifact::new(
        ArtifactKind::WorkflowGraph,
        store.put_json(&next_graph).unwrap(),
        "runtime.workflow",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.runtime".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        },
        None,
        vec![
            ArtifactRef {
                artifact_id: graph_artifact.artifact_id.clone(),
                kind: ArtifactKind::WorkflowGraph,
            },
            ArtifactRef {
                artifact_id: proposal_artifact.artifact_id.clone(),
                kind: ArtifactKind::WorkflowProposal,
            },
        ],
        now,
    )
    .unwrap();
    let patch = WorkflowPatchCommit {
        permit: claimed.permit.clone(),
        previous_graph_artifact_id: graph_artifact.artifact_id.clone(),
        planner_output: planner_output.clone(),
        evidence_needs: vec![evidence_need.clone()],
        proposal: proposal_artifact.clone(),
        next_graph: next_graph_artifact.clone(),
        added_nodes: vec![added.clone()],
        updated_nodes: vec![updated_evidence, updated_decision],
        completed_at: now,
    };

    {
        let connection = store.connection.lock().unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER fail_workflow_patch_completion BEFORE INSERT ON rebuild_events
                     WHEN NEW.event_type = 'task.succeeded'
                     BEGIN SELECT RAISE(ABORT, 'injected workflow patch failure'); END;",
            )
            .unwrap();
    }
    assert!(matches!(
        store.commit_workflow_patch(&patch),
        Err(StoreError::Sql(_))
    ));
    {
        let connection = store.connection.lock().unwrap();
        connection
            .execute_batch("DROP TRIGGER fail_workflow_patch_completion;")
            .unwrap();
        let revisions: u64 = connection
            .query_row(
                "SELECT COUNT(*) FROM rebuild_workflow_revisions WHERE run_id = ?1",
                params![run.run_id.0],
                |row| row.get(0),
            )
            .unwrap();
        let added_tasks: u64 = connection
            .query_row(
                "SELECT COUNT(*) FROM rebuild_tasks WHERE task_id = ?1",
                params![added.task_id.0],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(revisions, 1);
        assert_eq!(added_tasks, 0);
    }
    assert!(matches!(
        store.artifact(&planner_output.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
    assert!(matches!(
        store.artifact(&evidence_need.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
    assert!(matches!(
        store.artifact(&proposal_artifact.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
    assert!(matches!(
        store.artifact(&next_graph_artifact.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
    assert_eq!(store.events_after(&run.run_id, 0, 100).unwrap().len(), 2);
    store.validate_task_permit(&claimed.permit).unwrap();

    store.commit_workflow_patch(&patch).unwrap();
    assert_eq!(
        store.artifact(&planner_output.artifact_id).unwrap(),
        planner_output
    );
    assert_eq!(
        store.artifact(&evidence_need.artifact_id).unwrap(),
        evidence_need
    );
    assert_eq!(
        store.artifact(&proposal_artifact.artifact_id).unwrap(),
        proposal_artifact
    );
    assert_eq!(
        store
            .committed_task_outputs(&run.run_id, &planner_task_id)
            .unwrap(),
        vec![proposal_artifact.clone()]
    );
    assert_eq!(
        store
            .committed_attempt_outputs(&planner_task_id, &claimed.permit.attempt_id)
            .unwrap(),
        vec![proposal_artifact]
    );
    let stored_graph = store.artifact(&next_graph_artifact.artifact_id).unwrap();
    assert_eq!(stored_graph.artifact_id, next_graph_artifact.artifact_id);
    let mut stored_refs = stored_graph.source_refs;
    let mut expected_refs = next_graph_artifact.source_refs;
    stored_refs.sort_by(|left, right| {
        left.artifact_id
            .cmp(&right.artifact_id)
            .then(left.kind.cmp(&right.kind))
    });
    expected_refs.sort_by(|left, right| {
        left.artifact_id
            .cmp(&right.artifact_id)
            .then(left.kind.cmp(&right.kind))
    });
    assert_eq!(stored_refs, expected_refs);
    assert!(matches!(
        store.validate_task_permit(&claimed.permit),
        Err(StoreError::StalePermit(_))
    ));
    let claimed_evidence = store
        .claim_next_task("evidence-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap();
    assert_eq!(
        claimed_evidence.node.input_artifacts,
        vec![evidence_need_ref]
    );
    let initial_revision = store.workflow_revision(&run.run_id, 0).unwrap();
    let current_revision = store.workflow_revision(&run.run_id, 1).unwrap();
    assert_eq!(
        initial_revision.graph_artifact.artifact_id,
        graph_artifact.artifact_id
    );
    assert_eq!(
        current_revision.graph_artifact.artifact_id,
        next_graph_artifact.artifact_id
    );
    let snapshot = store.workflow_snapshot(&run.run_id).unwrap();
    assert_eq!(snapshot.status, WorkflowStatus::Running);
    assert_eq!(snapshot.revision, current_revision);
    assert_eq!(snapshot.tasks.len(), 4);
    assert_eq!(
        snapshot.event_cursor,
        store
            .events_after(&run.run_id, 0, 100)
            .unwrap()
            .last()
            .unwrap()
            .cursor
    );
    let evidence_snapshot = snapshot
        .tasks
        .iter()
        .find(|task| task.node.task_id == claimed_evidence.node.task_id)
        .unwrap();
    assert_eq!(evidence_snapshot.attempt_count, 1);
    assert_eq!(
        evidence_snapshot.active_attempt.as_ref().unwrap().permit,
        claimed_evidence.permit
    );
    store.verify_integrity().unwrap();
    {
        let connection = store.connection.lock().unwrap();
        connection
            .execute(
                "DELETE FROM rebuild_artifact_refs WHERE artifact_id = ?1 AND source_kind = ?2",
                params![
                    next_graph_artifact.artifact_id.0.as_str(),
                    enum_name(ArtifactKind::WorkflowProposal)
                ],
            )
            .unwrap();
    }
    assert!(store.verify_integrity().is_err());
}
