#[tokio::test]
async fn auto_paper_requires_an_injected_scheduler_loop() {
    let directory = tempdir().unwrap();
    let mut daemon_config = config(directory.path().to_path_buf());
    daemon_config.auto_paper = true;
    let daemon = Daemon::with_model(daemon_config, fixture_model_client()).unwrap();
    let (_shutdown, receiver) = watch::channel(false);

    assert!(matches!(
        daemon.serve_workers(receiver).await,
        Err(DaemonError::InvalidInput(_))
    ));
}

#[tokio::test]
async fn paper_scheduler_does_not_reserve_when_clock_is_closed() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let session_key = Utc::now().date_naive().to_string();
    let clock = StaticSessionClock(None);
    let source = StaticPaperWorkflowSource::new(paper_proposal());

    let reservation = daemon
        .paper
        .scheduler
        .tick(&clock, &source, Utc::now())
        .await
        .unwrap();

    assert!(reservation.is_none());
    assert!(daemon.store().session_slot(&session_key).unwrap().is_none());
}

#[tokio::test]
async fn auto_paper_supervisor_reserves_an_open_broker_session() {
    let directory = tempdir().unwrap();
    let mut daemon_config = config(directory.path().to_path_buf());
    daemon_config.auto_paper = true;
    let daemon = Daemon::with_model(daemon_config, fixture_model_client()).unwrap();
    let session_key = Utc::now().date_naive().to_string();
    install_test_paper_approval(
        daemon.store(),
        NaiveDate::parse_from_str(&session_key, "%Y-%m-%d").unwrap(),
        Utc::now(),
    );
    let clock = Arc::new(StaticSessionClock(Some(session_key.clone())));
    let source = Arc::new(StaticPaperWorkflowSource::new(paper_proposal()));
    let (shutdown, receiver) = watch::channel(false);
    let supervised = daemon.clone();
    let task = tokio::spawn(async move {
        supervised
            .serve_with_paper_scheduler(
                clock.as_ref(),
                source.as_ref(),
                std::time::Duration::from_millis(1),
                receiver,
            )
            .await
    });

    let mut reserved = None;
    for _ in 0..50 {
        reserved = daemon.store().session_slot(&session_key).unwrap();
        if reserved.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    shutdown.send(true).unwrap();
    assert!(task.await.unwrap().is_ok());
    assert!(reserved.is_some());
    let run_id = reserved.unwrap().workflow.run.run_id;
    let snapshot = daemon.store().workflow_snapshot(&run_id).unwrap();
    let snapshot_resources = snapshot
        .tasks
        .iter()
        .flat_map(|task| task.node.input_artifacts.iter())
        .filter_map(|reference| {
            let artifact = daemon.store().artifact(&reference.artifact_id).unwrap();
            (artifact.producer == "scheduler.paper_snapshot").then(|| {
                let need: EvidenceNeed =
                    serde_json::from_slice(&daemon.store().read_blob(&artifact.blob).unwrap())
                        .unwrap();
                assert_eq!(
                    artifact
                        .origin
                        .as_ref()
                        .and_then(|origin| origin.run_id.as_ref()),
                    Some(&run_id)
                );
                need.resource
            })
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        snapshot_resources,
        paper_session_evidence_needs(&session_key)
            .into_iter()
            .map(|need| need.resource)
            .collect::<BTreeSet<_>>()
    );
    let events = daemon.store().events_after(&run_id, 0, 256).unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.event_type == LifecycleEventType::SchedulerSnapshotNeedCreated.as_str()
            })
            .count(),
        paper_session_evidence_needs(&session_key).len()
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.event_type
                    == LifecycleEventType::SchedulerWorkflowProposalCreated.as_str()
            })
            .count(),
        1
    );
    daemon.store().verify_integrity().unwrap();
}

#[tokio::test]
async fn paper_health_is_ready_when_scheduler_holds_a_lease() {
    let directory = tempdir().unwrap();
    let mut daemon_config = config(directory.path().to_path_buf());
    daemon_config.auto_paper = true;
    let daemon = Daemon::with_model(daemon_config, fixture_model_client()).unwrap();
    let now = Utc::now();
    let session_key = now.date_naive().to_string();
    install_test_paper_approval(daemon.store(), now.date_naive(), now);
    let clock = StaticSessionClock(Some(session_key));
    let source = StaticPaperWorkflowSource::new(paper_proposal());

    daemon
        .paper
        .scheduler
        .tick(&clock, &source, now)
        .await
        .unwrap()
        .expect("Paper scheduler should reserve the approved session");

    let health = daemon.health().unwrap();
    assert_eq!(health.status, "ok");
    assert!(health.scheduler_owner.is_some());
    assert!(health.scheduler_epoch.is_some());
}

#[tokio::test]
async fn paper_scheduler_renews_lease_for_an_existing_session_slot() {
    let directory = tempdir().unwrap();
    let mut daemon_config = config(directory.path().to_path_buf());
    daemon_config.auto_paper = true;
    let daemon = Daemon::with_model(daemon_config, fixture_model_client()).unwrap();
    let now = Utc::now();
    let session_key = now.date_naive().to_string();
    install_test_paper_approval(daemon.store(), now.date_naive(), now);
    let clock = StaticSessionClock(Some(session_key));
    let source = StaticPaperWorkflowSource::new(paper_proposal());

    let first = daemon
        .paper
        .scheduler
        .tick(&clock, &source, now)
        .await
        .unwrap()
        .expect("Paper scheduler should reserve the approved session");
    let second = daemon
        .paper
        .scheduler
        .tick(&clock, &source, now + ChronoDuration::seconds(31))
        .await
        .unwrap()
        .expect("Paper scheduler should return the existing session slot");

    assert_eq!(
        second.slot.workflow.run.run_id,
        first.slot.workflow.run.run_id
    );
    let lease = daemon
        .store()
        .daemon_lease(SCHEDULER_LEASE_NAME)
        .unwrap()
        .expect("scheduler lease should be renewed");
    assert!(lease.expires_at > now + ChronoDuration::seconds(31));
}

#[tokio::test]
async fn auto_paper_requires_a_durable_workflow_proposal() {
    let directory = tempdir().unwrap();
    let mut daemon_config = config(directory.path().to_path_buf());
    daemon_config.auto_paper = true;
    let daemon = Daemon::with_model(daemon_config, fixture_model_client()).unwrap();
    let session_key = Utc::now().date_naive().to_string();
    let clock = StaticSessionClock(Some(session_key.clone()));
    let source = StorePaperWorkflowSource::new(daemon.store().clone());

    assert!(matches!(
        source.proposal("preflight").await,
        Err(SchedulerError::WorkflowUnavailable)
    ));
    assert!(daemon
        .paper
        .scheduler
        .tick(&clock, &source, Utc::now())
        .await
        .unwrap()
        .is_none());
    assert!(daemon.store().session_slot(&session_key).unwrap().is_none());
    daemon.store().verify_integrity().unwrap();
}

#[tokio::test]
async fn auto_paper_source_bootstraps_the_first_approved_proposal() {
    let directory = tempdir().unwrap();
    let mut daemon_config = config(directory.path().to_path_buf());
    daemon_config.auto_paper = true;
    let daemon = Daemon::with_model(daemon_config, fixture_model_client()).unwrap();

    let proposal = daemon
        .paper_workflow_source()
        .proposal("preflight")
        .await
        .unwrap();

    assert_eq!(proposal.topology_id, "active");
    assert_eq!(proposal.tasks.len(), 2);
    assert_eq!(
        proposal.tasks["analyst"].recipe_id.as_str(),
        "research.analyst"
    );
    assert_eq!(
        proposal.tasks["synthesizer"].recipe_id.as_str(),
        "research.synthesizer"
    );
    daemon.store().verify_integrity().unwrap();
}

#[tokio::test]
async fn auto_paper_source_ignores_a_newer_debug_proposal() {
    let directory = tempdir().unwrap();
    let mut daemon_config = config(directory.path().to_path_buf());
    daemon_config.auto_paper = true;
    let daemon = Daemon::with_model(daemon_config, fixture_model_client()).unwrap();
    daemon.submit_default(RunPurpose::Debug).unwrap();
    assert!(daemon.run_one("debug-proposal-fixture").await.unwrap());

    let proposal = daemon
        .paper_workflow_source()
        .proposal("preflight")
        .await
        .unwrap();

    assert_eq!(proposal.topology_id, "active");
    assert_eq!(proposal.tasks.len(), 2);
    daemon.store().verify_integrity().unwrap();
}

#[tokio::test]
async fn auto_paper_source_skips_a_proposal_carrying_a_foreign_planner_need() {
    let directory = tempdir().unwrap();
    let mut daemon_config = config(directory.path().to_path_buf());
    daemon_config.auto_paper = true;
    let daemon = Daemon::with_model(daemon_config, fixture_model_client()).unwrap();
    let old_run_id = daemon.submit_default(RunPurpose::Debug).unwrap();
    let now = Utc::now();
    let claimed = daemon
        .store()
        .claim_next_task("foreign-need-fixture", now, Duration::seconds(30))
        .unwrap()
        .unwrap();
    let need = EvidenceNeed {
        schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        source_family: "alpaca".to_owned(),
        resource: "bars:TQQQ:1d".to_owned(),
        max_age_secs: 86_400,
    };
    let need_artifact = Artifact::new(
        ArtifactKind::EvidenceNeed,
        daemon.store().put_json(&need).unwrap(),
        "runtime.planner.evidence_need",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.workflow.planner".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: claimed.permit.contract_hash.clone(),
        },
        Some(ArtifactOrigin {
            run_id: Some(old_run_id),
            task_id: Some(claimed.permit.task_id.clone()),
            attempt_id: Some(claimed.permit.attempt_id.clone()),
            contract_hash: claimed.permit.contract_hash.clone(),
        }),
        Vec::new(),
        now,
    )
    .unwrap();
    daemon
        .store()
        .write_task_artifact(
            &claimed.permit,
            &need_artifact,
            LifecycleEventType::PlannerEvidenceNeedCreated,
            now,
        )
        .unwrap();

    let source = daemon.paper_workflow_source();
    let clean = paper_proposal();
    assert!(!source.references_foreign_run_scoped_need(&clean).unwrap());

    let mut poisoned = paper_proposal();
    poisoned.tasks.insert(
        "analyst".to_owned(),
        WorkflowProposalTask {
            recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
            objective: "Assess stale fixture evidence".to_owned(),
            depends_on: vec![],
            priority: 90,
            evidence_needs: vec![ArtifactRef {
                artifact_id: need_artifact.artifact_id,
                kind: ArtifactKind::EvidenceNeed,
            }],
        },
    );
    assert!(source.references_foreign_run_scoped_need(&poisoned).unwrap());

    // Scheduler snapshots are re-minted per session, so carrying one forward
    // never blocks selection.
    let snapshot_run_id = RunId::new();
    let snapshot =
        scheduler_snapshot_need(daemon.store(), &snapshot_run_id, PAPER_ACCOUNT_RESOURCE, now);
    daemon
        .reserve_paper_session_with_inputs_for_run(
            snapshot_run_id,
            &now.date_naive().to_string(),
            &paper_proposal(),
            std::slice::from_ref(&snapshot),
            now,
        )
        .unwrap();
    let mut with_snapshot = paper_proposal();
    with_snapshot
        .tasks
        .get_mut("synthesizer")
        .unwrap()
        .evidence_needs = vec![ArtifactRef {
        artifact_id: snapshot.artifact_id,
        kind: ArtifactKind::EvidenceNeed,
    }];
    assert!(!source
        .references_foreign_run_scoped_need(&with_snapshot)
        .unwrap());
    daemon.store().verify_integrity().unwrap();
}

#[tokio::test]
async fn paper_scheduler_rejects_cross_run_run_scoped_evidence_needs() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let old_run_id = daemon.submit_default(RunPurpose::Debug).unwrap();
    let now = Utc::now();
    let claimed = daemon
        .store()
        .claim_next_task("cross-run-fixture", now, Duration::seconds(30))
        .unwrap()
        .unwrap();
    assert_eq!(claimed.run_id, old_run_id);
    let need = EvidenceNeed {
        schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        source_family: "alpaca".to_owned(),
        resource: "bars:TQQQ:1d".to_owned(),
        max_age_secs: 86_400,
    };
    let need_artifact = Artifact::new(
        ArtifactKind::EvidenceNeed,
        daemon.store().put_json(&need).unwrap(),
        "runtime.planner.evidence_need",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.workflow.planner".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: claimed.permit.contract_hash.clone(),
        },
        Some(ArtifactOrigin {
            run_id: Some(old_run_id.clone()),
            task_id: Some(claimed.permit.task_id.clone()),
            attempt_id: Some(claimed.permit.attempt_id.clone()),
            contract_hash: claimed.permit.contract_hash.clone(),
        }),
        Vec::new(),
        now,
    )
    .unwrap();
    daemon
        .store()
        .write_task_artifact(
            &claimed.permit,
            &need_artifact,
            LifecycleEventType::PlannerEvidenceNeedCreated,
            now,
        )
        .unwrap();

    let mut proposal = paper_proposal();
    proposal.tasks.insert(
        "analyst".to_owned(),
        WorkflowProposalTask {
            recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
            objective: "Assess stale fixture evidence".to_owned(),
            depends_on: vec![],
            priority: 90,
            evidence_needs: vec![ArtifactRef {
                artifact_id: need_artifact.artifact_id,
                kind: ArtifactKind::EvidenceNeed,
            }],
        },
    );
    proposal.tasks.get_mut("synthesizer").unwrap().depends_on = vec!["analyst".to_owned()];
    let session_key = now.date_naive().to_string();
    install_test_paper_approval(daemon.store(), now.date_naive(), now);
    let clock = StaticSessionClock(Some(session_key.clone()));
    let source = StaticPaperWorkflowSource::new(proposal);
    assert!(matches!(
        daemon.paper.scheduler.tick(&clock, &source, now).await,
        Err(SchedulerError::WorkflowUnavailable)
    ));
    assert!(daemon.store().session_slot(&session_key).unwrap().is_none());
    daemon.store().verify_integrity().unwrap();
}

#[tokio::test]
async fn paper_scheduler_does_not_carry_scheduler_snapshots_into_new_run() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let now = Utc::now();
    let old_session = now.date_naive().to_string();
    let old_run_id = RunId::new();
    let old_snapshot =
        scheduler_snapshot_need(daemon.store(), &old_run_id, PAPER_ACCOUNT_RESOURCE, now);
    daemon
        .reserve_paper_session_with_inputs_for_run(
            old_run_id.clone(),
            &old_session,
            &paper_proposal(),
            std::slice::from_ref(&old_snapshot),
            now,
        )
        .unwrap();

    let old_snapshot_ref = ArtifactRef {
        artifact_id: old_snapshot.artifact_id.clone(),
        kind: ArtifactKind::EvidenceNeed,
    };
    let mut new_proposal = paper_proposal();
    new_proposal.tasks.insert(
        "analyst".to_owned(),
        WorkflowProposalTask {
            recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
            objective: "Refresh scheduler-owned Paper snapshots".to_owned(),
            depends_on: vec![],
            priority: 90,
            evidence_needs: vec![old_snapshot_ref.clone()],
        },
    );
    new_proposal
        .tasks
        .get_mut("synthesizer")
        .unwrap()
        .depends_on = vec!["analyst".to_owned()];

    let new_session = (now.date_naive() + chrono::Days::new(1)).to_string();
    install_test_paper_approval(
        daemon.store(),
        NaiveDate::parse_from_str(&new_session, "%Y-%m-%d").unwrap(),
        now,
    );
    let clock = StaticSessionClock(Some(new_session));
    let source = StaticPaperWorkflowSource::new(new_proposal);
    let reservation = daemon
        .paper
        .scheduler
        .tick(&clock, &source, now + Duration::seconds(1))
        .await
        .unwrap()
        .expect("new Paper session must be reserved");
    let new_run_id = reservation.slot.workflow.run.run_id;
    assert_ne!(new_run_id, old_run_id);

    let snapshot = daemon.store().workflow_snapshot(&new_run_id).unwrap();
    let snapshot_refs = snapshot
        .tasks
        .iter()
        .flat_map(|task| task.node.input_artifacts.iter())
        .filter(|reference| reference.kind == ArtifactKind::EvidenceNeed)
        .cloned()
        .collect::<Vec<_>>();
    assert!(!snapshot_refs.contains(&old_snapshot_ref));
    assert!(snapshot_refs.iter().all(|reference| {
        daemon
            .store()
            .artifact(&reference.artifact_id)
            .unwrap()
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.as_ref())
            == Some(&new_run_id)
    }));
    daemon.store().verify_integrity().unwrap();
}
