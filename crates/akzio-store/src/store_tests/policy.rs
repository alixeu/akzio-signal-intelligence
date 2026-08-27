#[test]
fn doctor_rejects_a_corrupt_session_slot() {
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
    store
        .reserve_session_slot(
            &lease,
            &SessionReservation {
                session_key: "paper:fixture-corrupt".to_owned(),
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
            },
        )
        .unwrap();
    {
        let connection = store.connection.lock().unwrap();
        connection
            .execute(
                "UPDATE rebuild_session_slots SET topology_id = 'corrupt' WHERE session_key = ?1",
                params!["paper:fixture-corrupt"],
            )
            .unwrap();
    }
    assert!(matches!(
        store.verify_integrity(),
        Err(StoreError::Integrity(message)) if message.contains("topology mismatch")
    ));
}

#[test]
fn policy_transition_is_atomic_with_learning_artifacts_and_terminal_event() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let mut graph = graph();
    let seed = graph.nodes[0].clone();
    let mut evaluation_node = seed.clone();
    evaluation_node.task_id = TaskId::new();
    evaluation_node.dependencies = vec![seed.task_id.clone()];
    graph.nodes = vec![seed, evaluation_node];
    graph.validate().unwrap();
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
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let seed_permit = store
        .claim_next_task("seed-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;

    let make_artifact = |permit: &TaskWritePermit,
                         kind: ArtifactKind,
                         payload: serde_json::Value,
                         source_refs: Vec<ArtifactRef>,
                         lifecycle: ArtifactLifecycle| {
        Artifact::new(
            kind,
            store.put_json(&payload).unwrap(),
            "fixture",
            lifecycle,
            ArtifactProvenance {
                source_family: "fixture".to_owned(),
                observed_at: Some(now),
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: permit.contract_hash.clone(),
            },
            Some(ArtifactOrigin {
                run_id: Some(permit.run_id.clone()),
                task_id: Some(permit.task_id.clone()),
                attempt_id: Some(permit.attempt_id.clone()),
                contract_hash: permit.contract_hash.clone(),
            }),
            source_refs,
            now,
        )
        .unwrap()
    };
    let reference = |artifact: &Artifact| ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: artifact.kind,
    };
    let raw = make_artifact(
        &seed_permit,
        ArtifactKind::RawEvidence,
        serde_json::json!({"raw": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
    );
    let normalized = make_artifact(
        &seed_permit,
        ArtifactKind::NormalizedEvidence,
        serde_json::json!({"normalized": true}),
        vec![reference(&raw)],
        ArtifactLifecycle::RunScoped,
    );
    let execution_context = make_artifact(
        &seed_permit,
        ArtifactKind::ExecutionContext,
        serde_json::json!({"execution": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
    );
    let decision = make_artifact(
        &seed_permit,
        ArtifactKind::Decision,
        serde_json::json!({"decision": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
    );
    let decision_context = make_artifact(
        &seed_permit,
        ArtifactKind::DecisionContext,
        serde_json::json!({"context": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
    );
    let verdict_payload = ExecutionVerdict::NoOrder {
        no_order: akzio_domain::NoOrder {
            execution_context: reference(&execution_context),
            blockers: vec![akzio_domain::HardBlocker::Frozen],
            created_at: now,
        },
    };
    let verdict = make_artifact(
        &seed_permit,
        ArtifactKind::ExecutionVerdict,
        serde_json::to_value(&verdict_payload).unwrap(),
        vec![reference(&execution_context)],
        ArtifactLifecycle::RunScoped,
    );
    let outcome_id = akzio_domain::OutcomeId::new();
    let schedule_payload = OutcomeSchedule {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        outcome_id: outcome_id.clone(),
        decision: reference(&decision),
        decision_context: reference(&decision_context),
        execution_context: reference(&execution_context),
        execution: OutcomeExecutionLineage::NoOrder {
            execution_verdict: reference(&verdict),
        },
        baseline_trading_day: now.date_naive(),
        created_at: now,
    };
    let schedule = make_artifact(
        &seed_permit,
        ArtifactKind::OutcomeSchedule,
        serde_json::to_value(&schedule_payload).unwrap(),
        vec![
            schedule_payload.decision.clone(),
            schedule_payload.decision_context.clone(),
            schedule_payload.execution_context.clone(),
            reference(&verdict),
        ],
        ArtifactLifecycle::Canonical,
    );
    store
        .commit_attempt(
            &seed_permit,
            &[
                raw,
                normalized.clone(),
                execution_context.clone(),
                decision.clone(),
                decision_context.clone(),
                verdict.clone(),
                schedule.clone(),
            ],
            TaskStatus::Succeeded,
            now,
        )
        .unwrap();

    let evaluation_permit = store
        .claim_next_task("evaluation-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    let execution_ref = reference(&execution_context);
    let evidence_ref = reference(&normalized);
    let outcome_payload = Outcome {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        outcome_id,
        schedule: reference(&schedule),
        market_evidence: vec![evidence_ref.clone()],
        windows: [
            akzio_domain::OutcomeHorizon::T1,
            akzio_domain::OutcomeHorizon::T3,
            akzio_domain::OutcomeHorizon::T5,
        ]
        .into_iter()
        .map(|horizon| akzio_domain::OutcomeWindow {
            horizon,
            observed_trading_day: now.date_naive()
                + chrono::Days::new(u64::from(horizon.trading_days())),
            portfolio_return_ppm: 1,
            benchmark_return_ppm: 0,
            transaction_cost_ppm: 0,
            slippage_ppm: 0,
            utility_ppm: 1,
            calibration_ppm: Some(1_000_000),
            evidence_completeness_ppm: 1_000_000,
            risk_recall_ppm: Some(1_000_000),
        })
        .collect(),
        sealed_at: Some(now),
    };
    let outcome = make_artifact(
        &evaluation_permit,
        ArtifactKind::Outcome,
        serde_json::to_value(&outcome_payload).unwrap(),
        vec![reference(&schedule), evidence_ref],
        ArtifactLifecycle::Canonical,
    );
    let outcome_ref = reference(&outcome);
    let final_retrospective = retrospective_artifact(&store, &evaluation_permit, &outcome, now);
    let retrospective_ref = reference(&final_retrospective);
    let subject = PolicySubject::Memory(akzio_domain::MemoryId::new());
    let experience_payload = Experience {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        experience_id: akzio_domain::ExperienceId::new(),
        subject: subject.clone(),
        hypothesis_id: "fixture".to_owned(),
        decision: reference(&decision),
        decision_context: reference(&decision_context),
        execution_context: execution_ref.clone(),
        policy_verdict: reference(&verdict),
        outcome: outcome_ref.clone(),
        contract_hash: ContentHash::of_bytes(b"fixture-contract"),
        topology_id: akzio_domain::TopologyId("fixture-topology".to_owned()),
        policy_state: PolicyState::Memory(akzio_domain::MemoryLifecycle::Candidate),
        created_at: now,
    };
    let experience = make_artifact(
        &evaluation_permit,
        ArtifactKind::Experience,
        serde_json::to_value(&experience_payload).unwrap(),
        vec![
            experience_payload.decision.clone(),
            experience_payload.decision_context.clone(),
            experience_payload.execution_context.clone(),
            experience_payload.policy_verdict.clone(),
            experience_payload.outcome.clone(),
            retrospective_ref.clone(),
        ],
        ArtifactLifecycle::Canonical,
    );
    let experience_ref = reference(&experience);
    let evaluation_payload = Evaluation {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        evaluation_id: akzio_domain::EvaluationId::new(),
        outcome: outcome_ref.clone(),
        experience: experience_ref.clone(),
        marginal_utility_ppm: 1,
        token_cost: Some(1),
        latency_millis: Some(1),
        created_at: now,
    };
    let evaluation = make_artifact(
        &evaluation_permit,
        ArtifactKind::Evaluation,
        serde_json::to_value(&evaluation_payload).unwrap(),
        vec![outcome_ref, experience_ref, retrospective_ref],
        ArtifactLifecycle::Canonical,
    );
    let pair_snapshot = store.policy_shadow_pair_snapshot(&subject).unwrap();
    let commit = PolicyEvaluationCommit {
        permit: evaluation_permit,
        outcome: outcome.clone(),
        final_retrospective,
        experience,
        evaluation: evaluation.clone(),
        candidate_policy: None,
        subject: subject.clone(),
        from: PolicyState::Memory(akzio_domain::MemoryLifecycle::Candidate),
        to: PolicyState::Memory(akzio_domain::MemoryLifecycle::Active),
        pair_snapshot,
        transition: Some(PolicyTransition {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            transition_id: PolicyTransitionId::new(),
            subject: subject.clone(),
            from: PolicyState::Memory(akzio_domain::MemoryLifecycle::Candidate),
            to: PolicyState::Memory(akzio_domain::MemoryLifecycle::Active),
            evaluation: reference(&evaluation),
            created_at: now,
        }),
        completed_at: now,
    };
    {
        let connection = store.connection.lock().unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER fail_policy_event BEFORE INSERT ON rebuild_events \
                     WHEN NEW.event_type = 'policy.transitioned' \
                     BEGIN SELECT RAISE(ABORT, 'injected policy event failure'); END;",
            )
            .unwrap();
    }
    let failed = store.record_policy_evaluation(&commit);
    assert!(
        matches!(&failed, Err(StoreError::Sql(_))),
        "unexpected policy transition result: {failed:?}"
    );
    {
        let connection = store.connection.lock().unwrap();
        connection
            .execute_batch("DROP TRIGGER fail_policy_event;")
            .unwrap();
    }
    assert!(store.policy_head(&subject).unwrap().is_none());
    assert!(matches!(
        store.artifact(&outcome.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
    assert!(store
        .events_after(&run.run_id, 0, 100)
        .unwrap()
        .iter()
        .all(|event| event.event_type != "policy.transitioned"));

    let recorded = store.record_policy_evaluation(&commit).unwrap();
    assert!(recorded.newly_recorded);
    assert!(recorded.policy_head.is_some());
    assert_eq!(store.policy_transitions(&subject).unwrap().len(), 1);
    store.verify_integrity().unwrap();

    store
        .connection
        .lock()
        .unwrap()
        .execute(
            "UPDATE rebuild_policy_consumption_heads \
                 SET consumed_pair_cursor = 999 WHERE subject_id = ?1",
            params![subject.subject_id()],
        )
        .unwrap();
    let corrupted = store.verify_integrity();
    assert!(
        matches!(&corrupted, Err(StoreError::Integrity(_))),
        "unexpected Doctor result after policy cursor corruption: {corrupted:?}"
    );
}

#[test]
fn generic_learning_artifacts_require_specialized_atomic_apis() {
    let fixture = PolicyCommitFixture::memory();
    let candidate_policy = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::CandidatePolicy,
        &serde_json::json!({"candidate": true}),
        vec![],
        ArtifactLifecycle::Canonical,
        fixture.now,
    );

    for protected in [
        fixture.outcome.clone(),
        fixture.experience.clone(),
        fixture.evaluation.clone(),
        candidate_policy,
    ] {
        assert!(matches!(
            fixture.store.write_task_artifact(
                &fixture.permit,
                &protected,
                LifecycleEventType::FixtureGenericWrite,
                fixture.now,
            ),
            Err(StoreError::InvalidLearningCommit(
                "learning_artifact.atomic_commit_required"
            ))
        ));
        assert!(matches!(
            fixture.store.commit_attempt(
                &fixture.permit,
                &[protected],
                TaskStatus::Succeeded,
                fixture.now,
            ),
            Err(StoreError::InvalidLearningCommit(
                "learning_artifact.atomic_commit_required"
            ))
        ));
    }
}

#[test]
fn old_v7_policy_evaluation_shape_is_rejected() {
    let root = tempdir().unwrap();
    let database = root.path().join(DATABASE_FILE);
    let connection = Connection::open(database).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE rebuild_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO rebuild_metadata (key, value) VALUES ('schema_version', '7');
                 CREATE TABLE rebuild_policy_evaluations (
                    evaluation_artifact_id TEXT PRIMARY KEY,
                    subject_id TEXT NOT NULL,
                    outcome_artifact_id TEXT NOT NULL,
                    experience_artifact_id TEXT NOT NULL
                 );",
        )
        .unwrap();
    drop(connection);

    assert!(matches!(
        V2Store::open(root.path()),
        Err(StoreError::IncompatibleStoreRoot(path)) if path == root.path()
    ));
}

#[test]
fn policy_snapshot_does_not_consume_pairs_completed_after_cutoff() {
    let fixture = PolicyCommitFixture::memory();
    let first_cursor =
        fixture.insert_pair("snapshot-before-cutoff", OutcomeHorizon::T1, fixture.now);
    let snapshot = fixture
        .store
        .policy_shadow_pair_snapshot(&fixture.subject)
        .unwrap();
    assert_eq!(snapshot.through_cursor, first_cursor);
    assert_eq!(snapshot.counts_by_horizon, [1, 0, 0]);

    let second_cursor = fixture.insert_pair(
        "snapshot-after-cutoff",
        OutcomeHorizon::T3,
        fixture.now + Duration::seconds(1),
    );
    let recorded = fixture
        .store
        .record_policy_evaluation(&fixture.commit(snapshot))
        .unwrap();
    assert_eq!(recorded.consumed_pair_cursor, first_cursor);

    let remaining = fixture
        .store
        .policy_shadow_pair_snapshot(&fixture.subject)
        .unwrap();
    assert_eq!(remaining.after_cursor, first_cursor);
    assert_eq!(remaining.through_cursor, second_cursor);
    assert_eq!(remaining.counts_by_horizon, [0, 1, 0]);
}

#[test]
fn doctor_rejects_candidate_reverse_binding_corruption() {
    let fixture = PolicyCommitFixture::topology();
    let commit = fixture.commit(
        fixture
            .store
            .policy_shadow_pair_snapshot(&fixture.subject)
            .unwrap(),
    );
    fixture.store.record_policy_evaluation(&commit).unwrap();
    fixture.store.verify_integrity().unwrap();

    let original = commit.candidate_policy.as_ref().unwrap();
    let forged = Artifact::new(
        ArtifactKind::CandidatePolicy,
        original.blob.clone(),
        "fixture.policy.reverse-corruption",
        ArtifactLifecycle::Canonical,
        original.provenance.clone(),
        original.origin.clone(),
        original.source_refs.clone(),
        original.created_at + Duration::microseconds(1),
    )
    .unwrap();
    {
        let mut connection = fixture.store.connection.lock().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        insert_artifact(&transaction, &forged).unwrap();
        transaction
            .execute(
                "UPDATE rebuild_policy_evaluations
                     SET candidate_policy_artifact_id = ?1
                     WHERE evaluation_artifact_id = ?2",
                params![
                    forged.artifact_id.0.as_str(),
                    fixture.evaluation.artifact_id.0.as_str(),
                ],
            )
            .unwrap();
        transaction.commit().unwrap();
    }

    match fixture.store.verify_integrity() {
        Err(StoreError::Integrity(_)) => {}
        other => panic!("unexpected Doctor result: {other:?}"),
    }
}
