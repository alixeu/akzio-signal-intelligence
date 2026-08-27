#[test]
fn stale_permit_cannot_write_an_artifact() {
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
            run,
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let claimed = store
        .claim_next_task("worker", Utc::now(), Duration::milliseconds(-1))
        .unwrap()
        .unwrap();
    store.recover_expired_tasks(Utc::now()).unwrap();
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
    let artifact = artifact_with_refs(
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
    assert!(matches!(
        store.write_task_artifact(
            &claimed.permit,
            &artifact,
            LifecycleEventType::ClaimCreated,
            Utc::now()
        ),
        Err(StoreError::StalePermit(_))
    ));
}

#[test]
fn bootstrapped_contract_must_not_carry_task_origin() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let artifact = store
        .contract_artifact(&contract(&store, 1), Utc::now())
        .unwrap();
    store.write_bootstrap_artifact(&artifact).unwrap();
    store.verify_integrity().unwrap();
}

#[test]
fn execution_commitment_requires_a_consumed_paper_approval() {
    let fixture = execution_commit_fixture();

    assert!(matches!(
        fixture.store.commit_execution(
            &fixture.lease,
            &ExecutionCommit {
                session_key: "paper:fixture".to_owned(),
                permit: fixture.permit,
                commitment: fixture.commitment,
                committed_at: fixture.now,
            },
        ),
        Err(StoreError::InvalidSessionSlot(_))
    ));
}

#[test]
fn execution_commitment_rejects_approval_notional_overrun() {
    let fixture =
        approved_execution_commit_fixture(MoneyMicros::from_usd_cents(1), Duration::hours(8));

    assert!(matches!(
        fixture.store.commit_execution(
            &fixture.lease,
            &ExecutionCommit {
                session_key: "2026-08-25".to_owned(),
                permit: fixture.permit,
                commitment: fixture.commitment,
                committed_at: fixture.now,
            },
        ),
        Err(StoreError::InvalidSessionSlot(_))
    ));
}

#[test]
fn execution_commitment_rejects_expired_approval() {
    let fixture = approved_execution_commit_fixture(
        MoneyMicros::from_usd_cents(100_000),
        Duration::seconds(1),
    );

    assert!(matches!(
        fixture.store.commit_execution(
            &fixture.lease,
            &ExecutionCommit {
                session_key: "2026-08-25".to_owned(),
                permit: fixture.permit,
                commitment: fixture.commitment,
                committed_at: fixture.now + Duration::seconds(2),
            },
        ),
        Err(StoreError::InvalidSessionSlot(_))
    ));
}

#[test]
fn execution_commitment_lineage_fails_closed() {
    let fixture = execution_commit_fixture();
    let mut commitment = fixture.commitment.clone();
    commitment.lifecycle = ArtifactLifecycle::RunScoped;
    assert!(fixture
        .store
        .commit_execution(
            &fixture.lease,
            &ExecutionCommit {
                session_key: "paper:fixture".to_owned(),
                permit: fixture.permit.clone(),
                commitment,
                committed_at: fixture.now,
            },
        )
        .is_err());

    let fixture = execution_commit_fixture();
    let mut commitment = fixture.commitment.clone();
    commitment
        .source_refs
        .retain(|source| source.kind != ArtifactKind::ExecutionVerdict);
    assert!(fixture
        .store
        .commit_execution(
            &fixture.lease,
            &ExecutionCommit {
                session_key: "paper:fixture".to_owned(),
                permit: fixture.permit.clone(),
                commitment,
                committed_at: fixture.now,
            },
        )
        .is_err());

    let fixture = execution_commit_fixture();
    let mut commitment = fixture.commitment.clone();
    let verdict = commitment
        .source_refs
        .iter()
        .find(|source| source.kind == ArtifactKind::ExecutionVerdict)
        .unwrap()
        .clone();
    commitment.source_refs.push(verdict);
    assert!(fixture
        .store
        .commit_execution(
            &fixture.lease,
            &ExecutionCommit {
                session_key: "paper:fixture".to_owned(),
                permit: fixture.permit.clone(),
                commitment,
                committed_at: fixture.now,
            },
        )
        .is_err());

    let fixture = execution_commit_fixture();
    let mut commitment = fixture.commitment.clone();
    let context = commitment
        .source_refs
        .iter()
        .find(|source| source.kind == ArtifactKind::ExecutionContext)
        .unwrap()
        .clone();
    let no_order = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ExecutionVerdict,
        &ExecutionVerdict::NoOrder {
            no_order: NoOrder {
                execution_context: context.clone(),
                blockers: vec![HardBlocker::Frozen],
                created_at: fixture.now,
            },
        },
        vec![context.clone()],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &no_order,
            LifecycleEventType::ExecutionVerdictNoOrder,
            fixture.now,
        )
        .unwrap();
    commitment.source_refs = vec![artifact_ref(&no_order), context];
    assert!(fixture
        .store
        .commit_execution(
            &fixture.lease,
            &ExecutionCommit {
                session_key: "paper:fixture".to_owned(),
                permit: fixture.permit.clone(),
                commitment,
                committed_at: fixture.now,
            },
        )
        .is_err());

    let fixture = execution_commit_fixture();
    let mut commitment = fixture.commitment.clone();
    let context_index = commitment
        .source_refs
        .iter()
        .position(|source| source.kind == ArtifactKind::ExecutionContext)
        .unwrap();
    commitment.source_refs[context_index] = ArtifactRef {
        artifact_id: ArtifactId(ContentHash::of_bytes(b"wrong-context")),
        kind: ArtifactKind::ExecutionContext,
    };
    assert!(fixture
        .store
        .commit_execution(
            &fixture.lease,
            &ExecutionCommit {
                session_key: "paper:fixture".to_owned(),
                permit: fixture.permit.clone(),
                commitment,
                committed_at: fixture.now,
            },
        )
        .is_err());

    let fixture = execution_commit_fixture();
    let context_ref = fixture
        .commitment
        .source_refs
        .iter()
        .find(|source| source.kind == ArtifactKind::ExecutionContext)
        .unwrap();
    let context = fixture.store.artifact(&context_ref.artifact_id).unwrap();
    let plan_ref = context
        .source_refs
        .iter()
        .find(|source| source.kind == ArtifactKind::ExecutionPlan)
        .unwrap();
    let wrong_plan = fixture
        .store
        .put_json(&serde_json::json!({
            "plan_hash": ContentHash::of_bytes(b"wrong-plan")
        }))
        .unwrap();
    fixture
            .store
            .connection
            .lock()
            .unwrap()
            .execute(
                "UPDATE rebuild_artifacts SET blob_hash = ?1, media_type = ?2, bytes = ?3 WHERE artifact_id = ?4",
                params![
                    wrong_plan.hash.as_str(),
                    wrong_plan.media_type,
                    wrong_plan.bytes,
                    plan_ref.artifact_id.0.as_str(),
                ],
            )
            .unwrap();
    assert!(fixture
        .store
        .commit_execution(
            &fixture.lease,
            &ExecutionCommit {
                session_key: "paper:fixture".to_owned(),
                permit: fixture.permit.clone(),
                commitment: fixture.commitment,
                committed_at: fixture.now,
            },
        )
        .is_err());
}

#[test]
fn stale_outcome_lease_rejects_artifact_write_without_partial_commit() {
    let fixture = execution_commit_fixture();
    let stale = fixture.now + Duration::seconds(31);
    let successor = fixture
        .store
        .acquire_daemon_lease(
            "scheduler",
            "successor",
            stale,
            stale + Duration::seconds(30),
        )
        .unwrap()
        .unwrap();
    let evidence = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::NormalizedEvidence,
        &serde_json::json!({"outcome": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
        stale,
    );
    assert!(matches!(
        fixture.store.write_task_artifact_fenced(
            Some(&fixture.lease),
            &fixture.permit,
            &evidence,
            LifecycleEventType::OutcomeEvidence,
            stale,
        ),
        Err(StoreError::SchedulerFenced(_))
    ));
    assert!(matches!(
        fixture.store.artifact(&evidence.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
    fixture
        .store
        .write_task_artifact_fenced(
            Some(&successor),
            &fixture.permit,
            &evidence,
            LifecycleEventType::OutcomeEvidence,
            stale,
        )
        .unwrap();
    assert_eq!(
        fixture.store.artifact(&evidence.artifact_id).unwrap().kind,
        ArtifactKind::NormalizedEvidence
    );
}

#[test]
fn stale_outcome_lease_rejects_canonical_policy_evaluation() {
    let fixture = PolicyCommitFixture::memory();
    let lease_now = fixture.now;
    let lease = fixture
        .store
        .acquire_daemon_lease(
            "outcome-worker",
            "worker-a",
            lease_now,
            lease_now + Duration::seconds(30),
        )
        .unwrap()
        .unwrap();
    let stale = lease_now + Duration::seconds(31);
    fixture
        .store
        .acquire_daemon_lease(
            "outcome-worker",
            "worker-b",
            stale,
            stale + Duration::seconds(30),
        )
        .unwrap()
        .unwrap();
    let commit = fixture.commit(
        fixture
            .store
            .policy_shadow_pair_snapshot(&fixture.subject)
            .unwrap(),
    );
    assert!(matches!(
        fixture
            .store
            .record_policy_evaluation_fenced(Some(&lease), &commit),
        Err(StoreError::SchedulerFenced(_))
    ));
    assert!(fixture
        .store
        .policy_head(&fixture.subject)
        .unwrap()
        .is_none());
    assert!(matches!(
        fixture.store.artifact(&commit.evaluation.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
}

#[test]
fn outcome_schedule_worker_enqueue_is_idempotent_for_same_permit() {
    let fixture = PolicyCommitFixture::memory();
    let outcome_payload: Outcome = fixture
        .store
        .read_artifact_payload(&fixture.outcome)
        .unwrap();
    let stored_schedule = fixture
        .store
        .artifact(&outcome_payload.schedule.artifact_id)
        .unwrap();
    let mut payload: OutcomeSchedule = fixture
        .store
        .read_artifact_payload(&stored_schedule)
        .unwrap();
    payload.outcome_id = akzio_domain::OutcomeId::new();
    payload.created_at = fixture.now + Duration::seconds(1);
    let schedule = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::OutcomeSchedule,
        &payload,
        outcome_schedule_source_refs(&payload),
        ArtifactLifecycle::Canonical,
        payload.created_at,
    );

    fixture
        .store
        .commit_outcome_schedule_with_worker(&fixture.permit, &schedule, fixture.now)
        .unwrap();
    fixture
        .store
        .commit_outcome_schedule_with_worker(
            &fixture.permit,
            &schedule,
            fixture.now + Duration::seconds(1),
        )
        .unwrap();

    let worker_count = fixture
        .store
        .connection
        .lock()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM rebuild_tasks WHERE run_id = ?1 AND recipe_id = ?2",
            params![fixture.run.run_id.0, POST_TERMINAL_WORKER_RECIPE_ID],
            |row| row.get::<_, u64>(0),
        )
        .unwrap();
    assert_eq!(worker_count, 1);
    let enqueued_events = fixture
        .store
        .events_after(&fixture.run.run_id, 0, 100)
        .unwrap()
        .into_iter()
        .filter(|event| event.event_type == "outcome.worker.enqueued")
        .count();
    assert_eq!(enqueued_events, 1);
    fixture.store.verify_integrity().unwrap();
}
