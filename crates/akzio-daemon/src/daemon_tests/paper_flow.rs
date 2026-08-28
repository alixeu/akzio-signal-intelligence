#[tokio::test]
async fn scheduler_owned_paper_run_forwards_no_order_and_schedules_outcome() {
    let directory = tempdir().unwrap();
    let observed_at = Utc::now();
    let fixture_evidence = BTreeMap::from([(
        EvidenceSource::Alpaca,
        BTreeMap::from([(
            "bars:TQQQ:1d".to_owned(),
            AcquiredEvidence {
                raw: br#"{\"bars\":[{\"close\":100}]}"#.to_vec(),
                media_type: "application/json".to_owned(),
                source_uri: "fixture://alpaca/bars/TQQQ/1d".to_owned(),
                observed_at,
                normalized: serde_json::json!({"close": 100}),
                provenance: EvidenceProvenance {
                    document_id: Some("fixture-bars".to_owned()),
                    published_at: None,
                    observed_at,
                    revision: Some("1".to_owned()),
                    source_uri: "fixture://alpaca/bars/TQQQ/1d".to_owned(),
                    dedupe_key: "fixture:alpaca:bars:TQQQ:1d".to_owned(),
                    citations: vec![],
                },
                quality: EvidenceQuality::default(),
            },
        )]),
    )]);
    let daemon = Daemon::with_fixture_evidence(
        config(directory.path().to_path_buf()),
        scheduler_fixture_model_client(),
        fixture_evidence,
    )
    .unwrap();
    let now = Utc::now();
    let paper_run_id = RunId::new();
    let session_key = now.date_naive().to_string();
    let setup_artifacts = paper_session_evidence_needs(&session_key)
        .iter()
        .map(|need| scheduler_snapshot_need(daemon.store(), &paper_run_id, &need.resource, now))
        .collect::<Vec<_>>();
    let snapshot_refs = setup_artifacts
        .iter()
        .map(|artifact| ArtifactRef {
            artifact_id: artifact.artifact_id.clone(),
            kind: ArtifactKind::EvidenceNeed,
        })
        .collect::<Vec<_>>();
    let mut proposal = paper_proposal();
    proposal
        .tasks
        .get_mut("synthesizer")
        .unwrap()
        .evidence_needs = snapshot_refs;
    let slot = daemon
        .reserve_paper_session_with_inputs_for_run(
            paper_run_id,
            &session_key,
            &proposal,
            &setup_artifacts,
            now,
        )
        .unwrap();
    assert!(slot.newly_reserved);
    let run_id = slot.slot.workflow.run.run_id.clone();

    for _ in 0..32 {
        if !daemon.run_one("paper-fixture").await.unwrap() {
            break;
        }
    }

    let snapshot = daemon.store().workflow_snapshot(&run_id).unwrap();
    assert_eq!(daemon.workflow.replay_run(&run_id).unwrap(), snapshot);
    assert!(
        snapshot
            .tasks
            .iter()
            .all(|task| task.status == TaskStatus::Succeeded),
        "statuses: {:?}",
        snapshot
            .tasks
            .iter()
            .map(|task| format!("{}={:?}", task.node.recipe_id, task.status))
            .collect::<Vec<_>>()
    );
    let schedule = daemon
        .store()
        .latest_artifact_by_kind(ArtifactKind::OutcomeSchedule)
        .unwrap()
        .expect("Paper terminal chain must schedule future outcome");
    let payload: OutcomeSchedule =
        serde_json::from_slice(&daemon.store().read_blob(&schedule.blob).unwrap()).unwrap();
    assert_eq!(payload.baseline_trading_day, now.date_naive());
    assert!(matches!(
        payload.execution,
        OutcomeExecutionLineage::NoOrder { .. }
    ));
    assert!(daemon.store().session_slot(&session_key).unwrap().is_some());
    daemon.store().verify_integrity().unwrap();
}

#[tokio::test]
async fn paper_fixture_snapshots_reach_accepted_commit_reconcile_and_outcome_schedule() {
    let directory = tempdir().unwrap();
    let now = Utc::now();
    let session_key = now.date_naive().to_string();
    let account = serde_json::json!({
        "status": "ACTIVE",
        "equity": "10000",
        "buying_power": "10000",
        "trading_blocked": false
    });
    let quotes = QuoteSnapshot {
        schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        broker_session: session_key.clone(),
        observed_at: now,
        quotes: Asset::EXECUTABLE
            .into_iter()
            .map(|asset| {
                (
                    asset,
                    Quote {
                        bid: MoneyMicros::from_usd_cents(10_000),
                        ask: MoneyMicros::from_usd_cents(10_010),
                        observed_at: now,
                    },
                )
            })
            .collect(),
    };
    let clock = MarketClockSnapshot {
        schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        broker_session: session_key.clone(),
        is_open: true,
        observed_at: now,
    };
    let mut evidence = [
        (
            PAPER_ACCOUNT_RESOURCE,
            serde_json::to_value(&account).unwrap(),
        ),
        (
            PAPER_QUOTES_RESOURCE,
            serde_json::to_value(&quotes).unwrap(),
        ),
        (PAPER_CLOCK_RESOURCE, serde_json::to_value(&clock).unwrap()),
    ]
    .into_iter()
    .map(|(resource, normalized)| {
        (
            resource.to_owned(),
            AcquiredEvidence {
                raw: serde_json::to_vec(&normalized).unwrap(),
                media_type: "application/json".to_owned(),
                source_uri: format!("fixture://alpaca/{resource}"),
                observed_at: now,
                normalized,
                provenance: EvidenceProvenance {
                    document_id: Some(format!("fixture-{resource}")),
                    published_at: None,
                    observed_at: now,
                    revision: Some("1".to_owned()),
                    source_uri: format!("fixture://alpaca/{resource}"),
                    dedupe_key: format!("fixture:alpaca:{resource}"),
                    citations: vec![],
                },
                quality: EvidenceQuality::default(),
            },
        )
    })
    .collect::<BTreeMap<_, _>>();
    let fills_resource = format!("paper.fills:{session_key}");
    for resource in [
        PAPER_POSITIONS_RESOURCE.to_owned(),
        PAPER_OPEN_ORDERS_RESOURCE.to_owned(),
        fills_resource.clone(),
    ] {
        let normalized = serde_json::json!([]);
        evidence.insert(
            resource.clone(),
            AcquiredEvidence {
                raw: serde_json::to_vec(&normalized).unwrap(),
                media_type: "application/json".to_owned(),
                source_uri: format!("fixture://alpaca/{resource}"),
                observed_at: now,
                normalized,
                provenance: EvidenceProvenance {
                    document_id: Some(format!("fixture-{resource}")),
                    published_at: None,
                    observed_at: now,
                    revision: Some("1".to_owned()),
                    source_uri: format!("fixture://alpaca/{resource}"),
                    dedupe_key: format!("fixture:alpaca:{resource}"),
                    citations: vec![],
                },
                quality: EvidenceQuality::default(),
            },
        );
    }
    let execution_evidence = evidence.clone();
    let responses = Arc::new(Mutex::new(VecDeque::from(two_phase_responses(
        fixture_claim_output(),
    ))));
    let broker = Arc::new(FakePaperBroker::default());
    let daemon = Daemon::with_fixture_evidence(
        config(directory.path().to_path_buf()),
        ModelClient::FixtureSequence(responses.clone()),
        BTreeMap::from([(EvidenceSource::Alpaca, evidence)]),
    )
    .unwrap();
    let mut daemon = daemon.with_paper_broker(broker.clone());
    daemon.production_evidence = Arc::new(BTreeMap::from([(
        EvidenceSource::Alpaca,
        Arc::new(OutcomeBarsAdapter::new(now.date_naive(), now).with_responses(execution_evidence))
            as Arc<dyn AsyncEvidenceAdapter>,
    )]));
    let paper_run_id = RunId::new();
    let setup_artifacts = paper_session_evidence_needs(&session_key)
        .iter()
        .map(|need| scheduler_snapshot_need(daemon.store(), &paper_run_id, &need.resource, now))
        .collect::<Vec<_>>();
    let snapshot_refs = setup_artifacts
        .iter()
        .map(|artifact| ArtifactRef {
            artifact_id: artifact.artifact_id.clone(),
            kind: ArtifactKind::EvidenceNeed,
        })
        .collect::<Vec<_>>();
    let mut proposal = paper_proposal();
    proposal.tasks.insert(
        "analyst".to_owned(),
        WorkflowProposalTask {
            recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
            objective: "Assess governed Paper snapshots".to_owned(),
            depends_on: vec![],
            priority: 90,
            evidence_needs: snapshot_refs,
        },
    );
    proposal.tasks.get_mut("synthesizer").unwrap().depends_on = vec!["analyst".to_owned()];
    let (manifest, approval) = install_test_paper_approval(
        daemon.store(),
        NaiveDate::parse_from_str(&session_key, "%Y-%m-%d").unwrap(),
        now,
    );
    let lease = daemon.paper.scheduler.active_lease(now).unwrap();
    let slot = daemon
        .workflow
        .reserve_paper_session_with_inputs_for_run_approved(
            &lease,
            paper_run_id,
            &session_key,
            &proposal,
            &setup_artifacts,
            &manifest,
            &approval,
            now,
        )
        .unwrap();
    let run_id = slot.slot.workflow.run.run_id.clone();

    let evidence_task = daemon
        .store()
        .claim_next_task("accepted-paper-evidence", now, Duration::seconds(30))
        .unwrap()
        .unwrap();
    let evidence_outputs = daemon
        .acquire_evidence(&evidence_task, now)
        .await
        .expect("fixture snapshots must be valid governed evidence");
    daemon
        .store()
        .commit_attempt(
            &evidence_task.permit,
            &evidence_outputs,
            TaskStatus::Succeeded,
            now,
        )
        .unwrap();
    daemon.outcome_scheduling_runtime =
        OutcomeSchedulingRuntime::new(daemon.store.clone()).with_worker_enabled(true);

    assert!(daemon.run_one("accepted-paper-analyst").await.unwrap());
    let analyst_task = daemon
        .store()
        .workflow_snapshot(&run_id)
        .unwrap()
        .tasks
        .into_iter()
        .find(|task| task.node.recipe_id.as_str() == "research.analyst")
        .expect("fixture workflow must contain analyst")
        .node
        .task_id;
    let claim = daemon
        .store()
        .committed_task_outputs(&run_id, &analyst_task)
        .unwrap()
        .into_iter()
        .find(|artifact| artifact.kind == ArtifactKind::Claim)
        .expect("analyst must emit a Claim");
    let claim_payload: akzio_domain::ResearchClaim =
        serde_json::from_slice(&daemon.store().read_blob(&claim.blob).unwrap()).unwrap();
    responses.lock().unwrap().extend(accepted_paper_decision(
        ArtifactRef {
            artifact_id: claim.artifact_id,
            kind: ArtifactKind::Claim,
        },
        claim_payload.source_refs(),
    ));
    assert!(daemon.run_one("accepted-paper-synthesizer").await.unwrap());
    let synthesizer_task = daemon
        .store()
        .workflow_snapshot(&run_id)
        .unwrap()
        .tasks
        .into_iter()
        .find(|task| task.node.recipe_id.as_str() == "research.synthesizer")
        .unwrap()
        .node
        .task_id;
    let synthesizer_manifest = daemon
        .store()
        .events_after(&run_id, 0, 256)
        .unwrap()
        .into_iter()
        .find(|event| {
            event.task_id.as_ref() == Some(&synthesizer_task)
                && event.event_type == LifecycleEventType::ContextManifestCreated.as_str()
        })
        .and_then(|event| event.artifact_id)
        .and_then(|artifact_id| daemon.store().artifact(&artifact_id).ok())
        .unwrap();
    let synthesizer_manifest: ContextManifestPayload = serde_json::from_slice(
        &daemon
            .store()
            .read_blob(&synthesizer_manifest.blob)
            .unwrap(),
    )
    .unwrap();
        assert!(synthesizer_manifest
            .selections
            .iter()
            .any(|selection| selection.artifact.kind == ArtifactKind::NormalizedEvidence));
        assert!(synthesizer_manifest
            .selections
            .iter()
            .any(|selection| selection.artifact.kind == ArtifactKind::Claim));

    for _ in 0..5 {
        assert!(daemon.run_one("accepted-paper-gates").await.unwrap());
    }
    for _ in 0..32 {
        if !daemon.run_one("accepted-paper-fixture").await.unwrap() {
            break;
        }
    }

    let snapshot = daemon.store().workflow_snapshot(&run_id).unwrap();
    assert!(
        snapshot
            .tasks
            .iter()
            .all(|task| task.status == TaskStatus::Succeeded),
        "statuses: {:?}",
        snapshot
            .tasks
            .iter()
            .map(|task| format!("{}={:?}", task.node.recipe_id, task.status))
            .collect::<Vec<_>>()
    );
    let outcome_task = snapshot
        .tasks
        .iter()
        .find(|task| {
            task.node.recipe_id.as_str() == akzio_domain::LEARNING_OUTCOME_WORKER_RECIPE_ID
        })
        .expect("Paper run must retain an outcome worker task");
    let outcome_contract_hash = outcome_task
        .node
        .contract_hash
        .as_ref()
        .expect("Paper outcome worker must retain its contract hash");
    let outcome_contract = daemon.agents.contract(outcome_contract_hash).unwrap();
    assert_eq!(outcome_task.node.budget, outcome_contract.contract.budget);
    assert_eq!(outcome_task.node.retry, outcome_contract.contract.retry);
    assert_eq!(
        outcome_task.node.on_failure,
        outcome_contract.contract.on_failure
    );
    assert!(outcome_task
        .node
        .input_artifacts
        .iter()
        .any(|reference| reference.kind == ArtifactKind::DeliberationNote));
    let outcome_manifest_artifact = daemon
        .store()
        .events_after(&run_id, 0, 256)
        .unwrap()
        .into_iter()
        .filter(|event| {
            event.task_id.as_ref() == Some(&outcome_task.node.task_id)
                && event.event_type == LifecycleEventType::ContextManifestCreated.as_str()
        })
        .filter_map(|event| event.artifact_id)
        .next_back()
        .and_then(|artifact_id| daemon.store().artifact(&artifact_id).ok())
        .expect("outcome worker must assemble a governed context manifest");
    let outcome_manifest_payload: ContextManifestPayload = serde_json::from_slice(
        &daemon
            .store()
            .read_blob(&outcome_manifest_artifact.blob)
            .unwrap(),
    )
    .unwrap();
    assert!(outcome_manifest_payload
        .selections
        .iter()
        .any(|selection| selection.artifact.kind == ArtifactKind::DeliberationNote));
    // Provenance confidence is Rust-owned: the ContextManifest broker ranks
    // candidates by it, so a note must not inherit the model's self-reported
    // deliberation confidence. The self-report stays in the payload.
    let note = outcome_manifest_payload
        .selections
        .iter()
        .find(|selection| selection.artifact.kind == ArtifactKind::DeliberationNote)
        .and_then(|selection| daemon.store().artifact(&selection.artifact.artifact_id).ok())
        .expect("deliberation note is durable");
    assert_eq!(note.provenance.confidence_ppm, 1_000_000);
    let note_payload: serde_json::Value =
        serde_json::from_slice(&daemon.store().read_blob(&note.blob).unwrap()).unwrap();
    assert_eq!(note_payload["confidence_ppm"], 750_000);
    assert_eq!(broker.submissions.load(Ordering::SeqCst), 0);
    let schedule = daemon
        .store()
        .latest_artifact_by_kind(ArtifactKind::OutcomeSchedule)
        .unwrap()
        .expect("accepted fixture Paper chain must schedule an outcome");
    let payload: OutcomeSchedule =
        serde_json::from_slice(&daemon.store().read_blob(&schedule.blob).unwrap()).unwrap();
    assert!(matches!(
        payload.execution,
        OutcomeExecutionLineage::NoOrder { .. }
    ));
    assert!(daemon
        .store()
        .artifacts_referencing(&schedule.artifact_id, None)
        .unwrap()
        .iter()
        .any(|artifact| artifact.kind == ArtifactKind::Outcome));
    let outcome = daemon
        .store()
        .latest_artifact_by_kind(ArtifactKind::Outcome)
        .unwrap()
        .expect("outcome worker must seal a Paper Outcome");
    let outcome: Outcome =
        serde_json::from_slice(&daemon.store().read_blob(&outcome.blob).unwrap()).unwrap();
    assert_eq!(outcome.windows.len(), 3);
    assert!(daemon
        .store()
        .latest_artifact_by_kind(ArtifactKind::Evaluation)
        .unwrap()
        .is_none());
    let final_retrospective = daemon
        .store()
        .latest_artifact_by_kind(ArtifactKind::Retrospective)
        .unwrap()
        .expect("model-unavailable Paper run must retain a final retrospective");
    let final_retrospective: Retrospective =
        serde_json::from_slice(&daemon.store().read_blob(&final_retrospective.blob).unwrap())
            .unwrap();
    assert_eq!(
        final_retrospective.status,
        RetrospectiveStatus::ModelUnavailable
    );
    let retrospectives = daemon.store().retrospectives(&run_id).unwrap();
    let mut horizons = retrospectives
        .iter()
        .map(|artifact| {
            let payload: Retrospective =
                serde_json::from_slice(&daemon.store().read_blob(&artifact.blob).unwrap()).unwrap();
            (payload.horizon, artifact.lifecycle)
        })
        .collect::<Vec<_>>();
    horizons.sort_by_key(|(horizon, _)| *horizon);
    assert_eq!(horizons.len(), 3);
    assert_eq!(horizons[0].0, OutcomeHorizon::T1);
    assert_eq!(horizons[1].0, OutcomeHorizon::T3);
    assert_eq!(horizons[2].0, OutcomeHorizon::T5);
    assert_eq!(horizons[0].1, ArtifactLifecycle::RunScoped);
    assert_eq!(horizons[1].1, ArtifactLifecycle::RunScoped);
    assert_eq!(horizons[2].1, ArtifactLifecycle::Canonical);
    assert!(daemon
        .store()
        .events_after(&run_id, 0, 256)
        .unwrap()
        .iter()
    .all(|event| event.event_type != "execution.committed"));
    let observer = daemon.observer_snapshot().await.unwrap();
    let observer_outcome = observer.outcome.data.expect("sealed Outcome is observable");
    assert!(observer_outcome
        .horizons
        .iter()
        .all(|horizon| horizon.window.is_some()));
    let observer_learning = observer
        .learning
        .data
        .expect("durable learning artifacts are observable");
    assert!(observer_learning
        .artifacts
        .iter()
        .any(|artifact| artifact.kind == ArtifactKind::Outcome));
    assert!(observer_learning
        .artifacts
        .iter()
        .any(|artifact| artifact.kind == ArtifactKind::Retrospective));
    daemon.store().verify_integrity().unwrap();
}

#[test]
fn scheduler_fences_stale_daemon_and_reuses_frozen_session_workflow() {
    let directory = tempdir().unwrap();
    let first = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let second = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let now = Utc::now();
    let session_key = now.date_naive().to_string();
    let first_slot = first
        .reserve_paper_session(&session_key, &paper_proposal(), now)
        .unwrap();
    assert!(matches!(
        second.reserve_paper_session(&session_key, &paper_proposal(), now),
        Err(DaemonError::Scheduler(SchedulerError::NotLeader))
    ));

    let recovered = second
        .reserve_paper_session(&session_key, &paper_proposal(), now + Duration::seconds(31))
        .unwrap();
    assert!(!recovered.newly_reserved);
    assert_eq!(
        recovered.slot.workflow.run.run_id,
        first_slot.slot.workflow.run.run_id
    );
    assert!(matches!(
        first.reserve_paper_session(&session_key, &paper_proposal(), now + Duration::seconds(31),),
        Err(DaemonError::Scheduler(SchedulerError::NotLeader))
    ));
    first.store().verify_integrity().unwrap();
}
