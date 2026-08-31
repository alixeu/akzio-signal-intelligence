async fn dispatch_control(command: Command, config: &Config, config_path: &Path) -> Result<()> {
    match command {
        Command::Daemon { command } => match command {
            DaemonAction::Serve => serve(config, config_path).await,
            DaemonAction::Health => {
                print_json(&ControlApiClient::from_config(config)?.health().await?)
            }
            DaemonAction::Ready => {
                print_json(&ControlApiClient::from_config(config)?.ready().await?)
            }
            DaemonAction::Freeze { reason } => print_json(
                &ControlApiClient::from_config(config)?
                    .set_freeze(true, &reason)
                    .await?,
            ),
            DaemonAction::Unfreeze { reason } => print_json(
                &ControlApiClient::from_config(config)?
                    .set_freeze(false, &reason)
                    .await?,
            ),
        },
        Command::Run { command } => {
            let client = ControlApiClient::from_config(config)?;
            match command {
                RunCommand::Submit { purpose } => print_json(&client.submit(purpose.into()).await?),
                RunCommand::Replay { run_id } => print_json(&client.replay(&run_id).await?),
                RunCommand::Retrospectives { run_id } => {
                    print_json(&client.retrospectives(&run_id).await?)
                }
                RunCommand::Trajectory { run_id } => print_json(&client.trajectory(&run_id).await?),
                RunCommand::Events { run_id, after } => client.events(&run_id, after).await,
                RunCommand::Cancel { run_id } => print_json(&client.cancel(&run_id).await?),
                RunCommand::Retry { run_id } => print_json(&client.retry(&run_id).await?),
                RunCommand::FixtureDebug | RunCommand::PaperDryRun => {
                    bail!("fixture commands must be dispatched through the fixture handler")
                }
            }
        }
        Command::Canary { command } => {
            let client = ControlApiClient::from_config(config)?;
            match command {
                CanaryCommand::Stage { spec } => {
                    let payload = fs::read_to_string(spec).context("read canary campaign spec")?;
                    let spec: CanaryCampaignSpec = serde_json::from_str(&payload)
                        .context("parse canary campaign spec JSON")?;
                    print_json(&client.canary_stage(&spec).await?)
                }
                CanaryCommand::Status => print_json(&client.canary_status().await?),
                CanaryCommand::Resume { campaign_id } => {
                    let campaign_id =
                        ContentHash::new(campaign_id).context("parse campaign hash")?;
                    print_json(&client.canary_resume(&campaign_id).await?)
                }
            }
        }
        Command::ObservatoryConfig { .. } => {
            bail!("ObservatoryConfig is handled before control dispatch")
        }
        Command::Test { .. } => {
            bail!("diagnostic commands must be dispatched through the diagnostics handler")
        }
        Command::Store { .. } => {
            bail!("store commands must be dispatched through the store handler")
        }
    }
}

async fn dispatch_diagnostics(command: TestCommand, config: Config) -> Result<()> {
    diagnostic_test(config, command).await
}

async fn dispatch_fixture(command: RunCommand, config: Config) -> Result<()> {
    match command {
        RunCommand::FixtureDebug => fixture_debug(config).await,
        RunCommand::PaperDryRun => paper_dry_run(config).await,
        _ => bail!("non-fixture run command must be dispatched through the control handler"),
    }
}

async fn dispatch_store(
    command: StoreCommand,
    config: &Config,
    config_path: &Path,
) -> Result<()> {
    let client = ControlApiClient::from_config(config)?;
    match command {
        StoreCommand::Doctor => print_json(&client.store_doctor().await?),
        StoreCommand::Inventory => print_json(&client.store_inventory().await?),
        StoreCommand::Metrics => print_json(&client.store_metrics().await?),
        StoreCommand::Alerts => print_json(&client.store_alerts().await?),
        StoreCommand::PaperSession { session_key } => {
            let slot = client
                .store_session(&session_key)
                .await?
                .map(PaperSessionView::from);
            print_json(&slot)
        }
        StoreCommand::ApprovePaper {
            session_key,
            operator,
            reason,
            max_notional_usd_cents,
            valid_hours,
        } => {
            approve_paper(
                config,
                config_path,
                &session_key,
                &operator,
                &reason,
                max_notional_usd_cents,
                valid_hours,
            )
            .await
        }
        StoreCommand::Backup { target } => print_json(&client.store_backup(&target).await?),
        StoreCommand::Restore { source, target } => {
            print_json(&client.store_restore(&source, &target).await?)
        }
        StoreCommand::ExportRun {
            run_id,
            target,
            include_raw_model,
        } => print_json(
            &client
                .store_export_run(&run_id, &target, include_raw_model)
                .await?,
        ),
        StoreCommand::ReleaseEvidence { run_id, target } => {
            let run_id = RunId(run_id);
            let bundle = client.store_release_evidence(&run_id).await?;
            if let Some(target) = target {
                export_release_evidence_bundle(&bundle, &target)?;
                print_json(&serde_json::json!({
                    "run_id": run_id,
                    "target": target,
                    "bundle_hash": bundle.bundle_hash,
                    "status": bundle.status,
                }))
            } else {
                print_json(&bundle)
            }
        }
        StoreCommand::Lesson { command } => lesson::run(config, command).await,
    }
}

fn export_release_evidence_bundle(bundle: &ReleaseEvidenceBundle, target: &Path) -> Result<()> {
    if target.exists() {
        bail!("release evidence target already exists: {}", target.display());
    }
    let parent = target
        .parent()
        .context("release evidence target has no parent")?;
    fs::create_dir_all(parent).with_context(|| {
        format!("create release evidence directory {}", parent.display())
    })?;
    let bytes = serde_json::to_vec_pretty(bundle).context("serialize release evidence bundle")?;
    fs::write(target, bytes)
        .with_context(|| format!("write release evidence bundle {}", target.display()))
}
