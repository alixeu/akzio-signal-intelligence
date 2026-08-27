fn fixture_daemon(config: &Config) -> Result<Daemon> {
    Ok(Daemon::with_model(
        DaemonConfig {
            store_root: config.daemon.store_root.clone(),
            http_token: "fixture-only".to_owned(),
            observer_token: None,
            worker_count: config.daemon.worker_count.unwrap_or(2),
            auto_paper: false,
            market_data_feed: config.execution.market_data_feed,
            outcome_cost_model: OutcomeCostModel {
                transaction_cost_ppm: config.execution.transaction_cost_ppm,
                slippage_ppm: config.execution.slippage_ppm,
            },
            runtime_identity_hash: None,
        },
        fixture_model_client(),
    )?)
}

async fn diagnostic_test(config: Config, command: TestCommand) -> Result<()> {
    match command {
        TestCommand::CrashRecovery => {
            let (daemon, client) = fixture_daemon_http(&config).await?;
            let run_id = daemon.submit_default(RunPurpose::Debug)?;
            let now = Utc::now();
            if !client
                .store_claim_next("crash-recovery-fixture", now, 30)
                .await?
            {
                bail!("fixture run had no claimable task");
            }
            let recovered = client
                .store_recover_expired(now + ChronoDuration::seconds(31))
                .await?;
            if recovered == 0 {
                bail!("expired fixture attempt was not recovered");
            }
            client.store_doctor().await?;
            println!(
                "{}",
                serde_json::json!({
                    "test": "crash-recovery",
                    "run_id": run_id,
                    "recovered_attempts": recovered,
                    "fixture": true,
                    "evidence": "offline/store-recovery"
                })
            );
        }
        TestCommand::ConcurrentRuns => {
            let (daemon, client) = fixture_daemon_http(&config).await?;
            let first = daemon.submit_default(RunPurpose::Debug)?;
            let second = daemon.submit_default(RunPurpose::Debug)?;
            if first == second {
                bail!("fixture runs unexpectedly share a RunId");
            }
            while daemon.run_one("concurrent-runs-fixture").await? {}
            let first_snapshot = client.store_workflow(&first).await?;
            let second_snapshot = client.store_workflow(&second).await?;
            if !matches!(
                first_snapshot.status,
                WorkflowStatus::Completed
                    | WorkflowStatus::CompletedWithExecutionRejection
                    | WorkflowStatus::Failed
            ) || !matches!(
                second_snapshot.status,
                WorkflowStatus::Completed
                    | WorkflowStatus::CompletedWithExecutionRejection
                    | WorkflowStatus::Failed
            ) {
                bail!("concurrent fixture runs did not reach terminal status");
            }
            client.store_doctor().await?;
            println!(
                "{}",
                serde_json::json!({
                    "test": "concurrent-runs",
                    "run_ids": [first, second],
                    "fixture": true,
                    "evidence": "offline/store-concurrency"
                })
            );
        }
        TestCommand::EvidenceIntegrity => {
            let (daemon, client) = fixture_daemon_http(&config).await?;
            let run_id = daemon.submit_default(RunPurpose::Debug)?;
            while daemon.run_one("evidence-integrity-fixture").await? {}
            let events = client.store_events(&run_id, 0, 10_000).await?;
            let artifact_events = events
                .iter()
                .filter(|event| event.artifact_id.is_some())
                .count();
            if artifact_events == 0 {
                bail!("fixture run produced no artifact closure to audit");
            }
            for event in events.iter().filter_map(|event| event.artifact_id.as_ref()) {
                let artifact = client.store_artifact(event).await?;
                if matches!(artifact.kind, ArtifactKind::RawEvidence)
                    && artifact.lifecycle == akzio_domain::ArtifactLifecycle::Canonical
                {
                    bail!("raw evidence unexpectedly became canonical");
                }
            }
            client.store_doctor().await?;
            println!(
                "{}",
                serde_json::json!({
                    "test": "evidence-integrity",
                    "run_id": run_id,
                    "artifact_events": artifact_events,
                    "fixture": true,
                    "evidence": "offline/store-closure"
                })
            );
        }
        TestCommand::LearningTransitions => {
            let (daemon, client) = fixture_daemon_http(&config).await?;
            let run_id = daemon.submit_default(RunPurpose::PaperDryRun)?;
            while daemon.run_one("learning-transition-fixture").await? {}
            let events = client.store_events(&run_id, 0, 10_000).await?;
            let transitions = events
                .iter()
                .filter(|event| event.event_type == "policy.transitioned")
                .count();
            if transitions != 0 {
                bail!("noncanonical fixture run transitioned policy state");
            }
            client.store_doctor().await?;
            println!(
                "{}",
                serde_json::json!({
                    "test": "learning-transitions",
                    "run_id": run_id,
                    "policy_transitions": transitions,
                    "fixture": true,
                    "evidence": "offline/noncanonical-boundary"
                })
            );
        }
        TestCommand::FrozenEvidence => {
            let hash = |seed: &str| akzio_domain::ContentHash::of_bytes(seed.as_bytes());
            let record = |case_id: &str, schema_ok: bool| FrozenEvidenceRecord {
                case_id: case_id.to_owned(),
                model_version: "fixture-model-v1".to_owned(),
                prompt_hash: hash("fixture-prompt-v1"),
                contract_hash: hash("fixture-contract-v1"),
                planner_schema_ok: schema_ok,
                claim_schema_ok: schema_ok,
                critique_schema_ok: schema_ok,
                decision_proposal_schema_ok: schema_ok,
                expected_evidence: 4,
                observed_evidence: if schema_ok { 4 } else { 3 },
                expected_blockers: BTreeSet::from([akzio_domain::HardBlocker::MissingEvidence]),
                detected_blockers: if schema_ok {
                    BTreeSet::from([akzio_domain::HardBlocker::MissingEvidence])
                } else {
                    BTreeSet::new()
                },
                input_tokens: 120,
                output_tokens: 80,
                cost_micros: 15,
                latency_millis: if schema_ok { 240 } else { 310 },
            };
            let metrics = evaluate_frozen_evidence(&FrozenEvidenceSet {
                set_id: "cli-frozen-evidence-fixture".to_owned(),
                records: vec![record("case-accepted", true), record("case-blocked", false)],
            })?;
            println!(
                "{}",
                serde_json::json!({
                    "test": "frozen-evidence",
                    "fixture": true,
                    "evidence": "offline/frozen-evidence",
                    "metrics": metrics,
                })
            );
        }
        TestCommand::StoreCorruption => {
            let (daemon, client) = fixture_daemon_http(&config).await?;
            let run_id = daemon.submit_default(RunPurpose::Debug)?;
            while daemon.run_one("store-corruption-fixture").await? {}
            let artifact_ref = client
                .store_events(&run_id, 0, 10_000)
                .await?
                .into_iter()
                .find_map(|event| event.artifact_id)
                .context("fixture run produced no artifact to corrupt")?;
            let artifact = client.store_artifact(&artifact_ref).await?;
            if !client
                .store_diagnose_corruption(&artifact.artifact_id)
                .await?
            {
                bail!("Store Doctor accepted a corrupted CAS blob");
            }
            println!(
                "{}",
                serde_json::json!({
                    "test": "store-corruption",
                    "run_id": run_id,
                    "fixture": true,
                    "evidence": "offline/store-doctor-corruption",
                    "doctor_rejected": true,
                })
            );
        }
        TestCommand::FreezeRecovery => {
            let (_, client) = fixture_daemon_http(&config).await?;
            client
                .store_freeze(true, "fixture freeze", Utc::now())
                .await?;
            let frozen = client
                .store_latest_artifact(ArtifactKind::FreezeState)
                .await?
                .context("freeze artifact missing after reopen")?;
            client
                .store_freeze(false, "fixture unfreeze", Utc::now())
                .await?;
            let unfrozen = client
                .store_latest_artifact(ArtifactKind::FreezeState)
                .await?
                .context("unfreeze artifact missing after reopen")?;
            client.store_doctor().await?;
            println!(
                "{}",
                serde_json::json!({
                    "test": "freeze-recovery",
                    "fixture": true,
                    "evidence": "offline/freeze-persistence",
                    "frozen_artifact": frozen.artifact_id,
                    "unfrozen_artifact": unfrozen.artifact_id,
                })
            );
        }
        TestCommand::LeaseTakeover => {
            let (_, client) = fixture_daemon_http(&config).await?;
            let now = Utc::now();
            let first = client
                .store_acquire_lease(
                    "paper-scheduler",
                    "fixture-owner-a",
                    now,
                    now + ChronoDuration::seconds(10),
                )
                .await?
                .context("first fixture lease was not acquired")?;
            if client
                .store_acquire_lease(
                    "paper-scheduler",
                    "fixture-owner-b",
                    now + ChronoDuration::seconds(1),
                    now + ChronoDuration::seconds(5),
                )
                .await?
                .is_some()
            {
                bail!("live daemon lease incorrectly stolen");
            }
            let successor = client
                .store_acquire_lease(
                    "paper-scheduler",
                    "fixture-owner-b",
                    now + ChronoDuration::seconds(11),
                    now + ChronoDuration::seconds(21),
                )
                .await?
                .context("expired fixture lease not taken over")?;
            if successor.epoch <= first.epoch
                || client
                    .store_validate_lease(&first, now + ChronoDuration::seconds(11))
                    .await?
            {
                bail!("stale daemon lease remained valid after takeover");
            }
            client.store_doctor().await?;
            println!(
                "{}",
                serde_json::json!({
                    "test": "lease-takeover",
                    "fixture": true,
                    "evidence": "offline/daemon-lease-fence",
                    "old_epoch": first.epoch,
                    "new_epoch": successor.epoch,
                })
            );
        }
        TestCommand::Retrospective => {
            let (_, client) = fixture_daemon_http(&config).await?;
            client.store_doctor().await?;
            let latest = client.store_latest_retrospective().await?;
            let latest_horizon = latest
                .as_ref()
                .map(|payload| {
                    payload.validate()?;
                    Ok::<_, anyhow::Error>(payload.horizon)
                })
                .transpose()?;
            println!(
                "{}",
                serde_json::json!({
                    "test": "retrospective",
                    "ok": true,
                    "latest_horizon": latest_horizon,
                    "evidence": "offline/store-doctor"
                })
            );
        }
    }
    Ok(())
}

fn print_json<T: Serialize>(response: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(response)?);
    Ok(())
}

async fn fixture_daemon_http(config: &Config) -> Result<(Daemon, ControlApiClient)> {
    let daemon = fixture_daemon(config)?;
    let (shutdown, receiver) = watch::channel(false);
    let server_daemon = daemon.clone();
    let address = config.daemon.http_addr;
    tokio::spawn(async move {
        let _shutdown = shutdown;
        let _ = server_daemon.serve_http(address, receiver).await;
    });
    let client = ControlApiClient::new(address, "fixture-only".to_owned())?;
    for _ in 0..50 {
        if client.health().await.is_ok() {
            return Ok((daemon, client));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    bail!("fixture daemon HTTP server did not become ready")
}
