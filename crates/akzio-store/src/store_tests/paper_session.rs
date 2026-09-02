#[test]
fn doctor_rejects_corrupt_execution_lineage() {
    let fixture = execution_commit_fixture();
    let payload: PaperCommitment =
        serde_json::from_slice(&fixture.store.read_blob(&fixture.commitment.blob).unwrap())
            .unwrap();
    let context = fixture
        .commitment
        .source_refs
        .iter()
        .find(|source| source.kind == ArtifactKind::ExecutionContext)
        .unwrap()
        .clone();
    let invalid = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ExecutionCommitment,
        &payload,
        vec![context],
        ArtifactLifecycle::Canonical,
        fixture.now,
    );
    {
        let mut connection = fixture.store.connection.lock().unwrap();
        let transaction = connection.transaction().unwrap();
        insert_artifact(&transaction, &invalid).unwrap();
        transaction
                .execute(
                    "UPDATE rebuild_session_slots SET commitment_artifact_id = ?1, committed_at = ?2 WHERE session_key = ?3",
                    params![
                        invalid.artifact_id.0.as_str(),
                        fixture.now.to_rfc3339(),
                        "paper:fixture",
                    ],
                )
                .unwrap();
        transaction.commit().unwrap();
    }
    let error = fixture.store.verify_integrity().unwrap_err();
    assert!(
        matches!(
            &error,
            StoreError::Integrity(message)
                if message.contains("commitment lineage is invalid")
        ),
        "{error}"
    );

    let fixture = execution_commit_fixture();
    let payload: PaperCommitment =
        serde_json::from_slice(&fixture.store.read_blob(&fixture.commitment.blob).unwrap())
            .unwrap();
    let invalid = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ExecutionCommitment,
        &payload,
        fixture.commitment.source_refs.clone(),
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    {
        let mut connection = fixture.store.connection.lock().unwrap();
        let transaction = connection.transaction().unwrap();
        insert_artifact(&transaction, &invalid).unwrap();
        transaction
                .execute(
                    "UPDATE rebuild_session_slots SET commitment_artifact_id = ?1, committed_at = ?2 WHERE session_key = ?3",
                    params![
                        invalid.artifact_id.0.as_str(),
                        fixture.now.to_rfc3339(),
                        "paper:fixture",
                    ],
                )
                .unwrap();
        transaction.commit().unwrap();
    }
    let error = fixture.store.verify_integrity().unwrap_err();
    assert!(
        matches!(
            &error,
            StoreError::Integrity(message)
                if message.contains("commitment lineage is invalid")
        ),
        "{error}"
    );
}

#[test]
fn approved_paper_reservation_rejects_mismatched_proposal_and_keeps_store_atomic() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let lease = store
        .acquire_daemon_lease("scheduler", "daemon-a", now, now + Duration::seconds(30))
        .unwrap()
        .unwrap();
    let graph = graph();
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Paper,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: now,
    };
    let workflow = WorkflowCommit {
        run: run.clone(),
        graph: graph_artifact,
        nodes: graph.nodes,
    };
    let proposal_payload = WorkflowProposal {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: run.topology_id.clone(),
        tasks: BTreeMap::from([(
            "analyst".to_owned(),
            WorkflowProposalTask {
                recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                objective: "analyze".to_owned(),
                depends_on: vec![],
                priority: 50,
                evidence_needs: vec![],
            },
        )]),
        stop_reason: Some("fixture".to_owned()),
    };
    let mut proposal = artifact(
        &store,
        ArtifactKind::WorkflowProposal,
        &serde_json::to_string(&proposal_payload).unwrap(),
        Some(ArtifactOrigin {
            run_id: Some(run.run_id.clone()),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        }),
    );
    proposal.producer = "runtime.paper_provisioning".to_owned();
    proposal.lifecycle = ArtifactLifecycle::RunScoped;
    let reservation = SessionReservation {
        session_key: "2026-08-12".to_owned(),
        workflow,
        setup_artifacts: vec![],
        reserved_at: now,
    };
    let mut wrong_proposal = proposal;
    wrong_proposal.origin = Some(ArtifactOrigin {
        run_id: Some(RunId::new()),
        task_id: None,
        attempt_id: None,
        contract_hash: None,
    });
    assert!(matches!(
        store.reserve_paper_session_with_proposal(&lease, &reservation, &wrong_proposal),
        Err(StoreError::InvalidSessionSlot(_))
    ));
    assert!(store.session_slot("2026-08-12").unwrap().is_none());
    assert!(matches!(
        store.run_purpose(&run.run_id),
        Err(StoreError::MissingRun(_))
    ));
    store.verify_integrity().unwrap();
}

#[test]
fn approved_paper_reservation_rejects_source_closure_mismatch_atomically() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let lease = store
        .acquire_daemon_lease("scheduler", "daemon-a", now, now + Duration::seconds(30))
        .unwrap()
        .unwrap();
    let graph = graph();
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Paper,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: now,
    };
    let workflow = WorkflowCommit {
        run: run.clone(),
        graph: graph_artifact,
        nodes: graph.nodes,
    };
    let setup = artifact(
        &store,
        ArtifactKind::EvidenceNeed,
        "{}",
        Some(ArtifactOrigin {
            run_id: Some(run.run_id.clone()),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        }),
    );
    let proposal_payload = WorkflowProposal {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: run.topology_id.clone(),
        tasks: BTreeMap::from([(
            "analyst".to_owned(),
            WorkflowProposalTask {
                recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                objective: "analyze".to_owned(),
                depends_on: vec![],
                priority: 50,
                evidence_needs: vec![],
            },
        )]),
        stop_reason: Some("fixture".to_owned()),
    };
    let mut proposal = artifact_with_refs(
        &store,
        ArtifactKind::WorkflowProposal,
        &serde_json::to_string(&proposal_payload).unwrap(),
        Some(ArtifactOrigin {
            run_id: Some(run.run_id.clone()),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        }),
        vec![],
    );
    proposal.producer = "runtime.paper_provisioning".to_owned();
    proposal.artifact_id = ArtifactId(proposal.expected_hash().unwrap());
    let reservation = SessionReservation {
        session_key: "2026-08-12-source-closure".to_owned(),
        workflow,
        setup_artifacts: vec![setup],
        reserved_at: now,
    };
    assert!(matches!(
        store.reserve_paper_session_with_proposal(&lease, &reservation, &proposal),
        Err(StoreError::InvalidWorkflowProposalArtifact)
    ));
    assert!(store
        .session_slot("2026-08-12-source-closure")
        .unwrap()
        .is_none());
    assert!(matches!(
        store.run_purpose(&run.run_id),
        Err(StoreError::MissingRun(_))
    ));
    store.verify_integrity().unwrap();
}

#[test]
fn approved_paper_reservation_is_idempotent_for_duplicate_session() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let lease = store
        .acquire_daemon_lease("scheduler", "daemon-a", now, now + Duration::seconds(30))
        .unwrap()
        .unwrap();
    let graph = graph();
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Paper,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: now,
    };
    let workflow = WorkflowCommit {
        run: run.clone(),
        graph: graph_artifact,
        nodes: graph.nodes,
    };
    let proposal_payload = WorkflowProposal {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: run.topology_id.clone(),
        tasks: BTreeMap::from([(
            "analyst".to_owned(),
            WorkflowProposalTask {
                recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                objective: "analyze".to_owned(),
                depends_on: vec![],
                priority: 50,
                evidence_needs: vec![],
            },
        )]),
        stop_reason: Some("fixture".to_owned()),
    };
    let mut proposal = artifact(
        &store,
        ArtifactKind::WorkflowProposal,
        &serde_json::to_string(&proposal_payload).unwrap(),
        Some(ArtifactOrigin {
            run_id: Some(run.run_id),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        }),
    );
    proposal.producer = "runtime.paper_provisioning".to_owned();
    proposal.artifact_id = ArtifactId(proposal.expected_hash().unwrap());
    let reservation = SessionReservation {
        session_key: "2026-08-12".to_owned(),
        workflow,
        setup_artifacts: vec![],
        reserved_at: now,
    };
    let first = store
        .reserve_paper_session_with_proposal(&lease, &reservation, &proposal)
        .unwrap();
    let second = store
        .reserve_paper_session_with_proposal(&lease, &reservation, &proposal)
        .unwrap();
    assert!(first.newly_reserved);
    assert!(!second.newly_reserved);
    assert_eq!(
        first.slot.workflow.run.run_id,
        second.slot.workflow.run.run_id
    );
    let successor = store
        .acquire_daemon_lease(
            "scheduler",
            "daemon-b",
            now + Duration::seconds(31),
            now + Duration::seconds(61),
        )
        .unwrap()
        .unwrap();
    assert_eq!(successor.epoch, lease.epoch + 1);
    assert!(matches!(
        store.reserve_paper_session_with_proposal(&lease, &reservation, &proposal),
        Err(StoreError::SchedulerFenced(_))
    ));
    store.verify_integrity().unwrap();
}

#[test]
fn session_slot_is_fenced_and_reuses_the_frozen_workflow() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let session_key = "2026-08-25";
    let first_lease = store
        .acquire_daemon_lease("scheduler", "daemon-a", now, now + Duration::seconds(30))
        .unwrap()
        .unwrap();

    let first_graph = graph();
    let first_graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&first_graph).unwrap(),
        None,
    );
    let first_workflow = WorkflowCommit {
        run: StoredRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Paper,
            topology_id: first_graph.topology_id.clone(),
            graph_artifact_id: first_graph_artifact.artifact_id.clone(),
            created_at: now,
        },
        graph: first_graph_artifact,
        nodes: first_graph.nodes,
    };
    let first = reserve_approved_test_session(
        &store,
        &first_lease,
        &SessionReservation {
            session_key: session_key.to_owned(),
            workflow: first_workflow.clone(),
            setup_artifacts: vec![],
            reserved_at: now,
        },
    );
    assert!(first.newly_reserved);

    let mut replacement_graph = graph();
    replacement_graph.nodes[0].objective = "replacement plan".to_owned();
    let replacement_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&replacement_graph).unwrap(),
        None,
    );
    let replacement_workflow = WorkflowCommit {
        run: StoredRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Paper,
            topology_id: replacement_graph.topology_id.clone(),
            graph_artifact_id: replacement_artifact.artifact_id.clone(),
            created_at: now,
        },
        graph: replacement_artifact,
        nodes: replacement_graph.nodes,
    };
    let duplicate = store
        .reserve_session_slot(
            &first_lease,
            &SessionReservation {
                session_key: session_key.to_owned(),
                workflow: replacement_workflow.clone(),
                setup_artifacts: vec![],
                reserved_at: now,
            },
        )
        .unwrap();
    assert!(!duplicate.newly_reserved);
    assert_eq!(
        duplicate.slot.workflow.run.run_id,
        first_workflow.run.run_id
    );
    assert_eq!(
        duplicate.slot.workflow.graph.artifact_id,
        first_workflow.graph.artifact_id
    );

    let claimed = store
        .claim_next_task("execution-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap();
    let commitment = valid_execution_commitment(&store, &claimed.permit, session_key, now);
    {
        let connection = store.connection.lock().unwrap();
        connection
                .execute_batch(
                    "CREATE TRIGGER fail_execution_task_completion BEFORE INSERT ON rebuild_events \
                     WHEN NEW.event_type = 'task.succeeded' \
                     BEGIN SELECT RAISE(ABORT, 'injected execution completion event failure'); END;",
                )
                .unwrap();
    }
    assert!(matches!(
        store.commit_execution(
            &first_lease,
            &ExecutionCommit {
                session_key: session_key.to_owned(),
                permit: claimed.permit.clone(),
                commitment: commitment.clone(),
                committed_at: now,
            },
        ),
        Err(StoreError::Sql(_))
    ));
    assert_eq!(
        store
            .session_slot(session_key)
            .unwrap()
            .unwrap()
            .commitment_artifact_id,
        None
    );
    assert!(matches!(
        store.artifact(&commitment.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
    assert!(store
        .events_after(&claimed.permit.run_id, 0, 20)
        .unwrap()
        .iter()
        .all(|event| event.event_type != "execution.committed"));
    {
        let connection = store.connection.lock().unwrap();
        connection
            .execute_batch("DROP TRIGGER fail_execution_task_completion;")
            .unwrap();
    }
    let committed = store
        .commit_execution(
            &first_lease,
            &ExecutionCommit {
                session_key: session_key.to_owned(),
                permit: claimed.permit.clone(),
                commitment: commitment.clone(),
                committed_at: now,
            },
        )
        .unwrap();
    assert!(committed.newly_committed);
    let outputs = store
        .committed_task_outputs(&claimed.permit.run_id, &claimed.permit.task_id)
        .unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].artifact_id, commitment.artifact_id);
    assert!(matches!(
        store.commit_execution(
            &first_lease,
            &ExecutionCommit {
                session_key: session_key.to_owned(),
                permit: claimed.permit.clone(),
                commitment: commitment.clone(),
                committed_at: now,
            },
        ),
        Err(StoreError::StalePermit(_))
    ));
    let events = store.events_after(&claimed.permit.run_id, 0, 20).unwrap();
    assert!(events.iter().any(|event| {
        event.event_type == "execution.committed"
            && event.artifact_id.as_ref() == Some(&commitment.artifact_id)
    }));
    assert!(events.iter().any(|event| {
        event.event_type == "task.succeeded"
            && event.task_id.as_ref() == Some(&claimed.permit.task_id)
            && event.attempt_id.as_ref() == Some(&claimed.permit.attempt_id)
            && event.artifact_id.as_ref() == Some(&commitment.artifact_id)
    }));
    assert_eq!(
        store
            .session_slot(session_key)
            .unwrap()
            .unwrap()
            .commitment_artifact_id,
        Some(commitment.artifact_id.clone())
    );
    store.verify_integrity().unwrap();

    let successor_now = now + Duration::seconds(31);
    let successor = store
        .acquire_daemon_lease(
            "scheduler",
            "daemon-b",
            successor_now,
            successor_now + Duration::seconds(30),
        )
        .unwrap()
        .unwrap();
    assert_eq!(successor.epoch, first_lease.epoch + 1);
    assert!(matches!(
        store.commit_execution(
            &first_lease,
            &ExecutionCommit {
                session_key: session_key.to_owned(),
                permit: claimed.permit,
                commitment,
                committed_at: successor_now,
            },
        ),
        Err(StoreError::SchedulerFenced(_))
    ));
    assert!(matches!(
        store.reserve_session_slot(
            &first_lease,
            &SessionReservation {
                session_key: "paper:fixture-b".to_owned(),
                workflow: replacement_workflow,
                setup_artifacts: vec![],
                reserved_at: successor_now,
            },
        ),
        Err(StoreError::SchedulerFenced(_))
    ));
}
