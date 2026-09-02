#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

fn resolve_env_placeholder(value: &str, field: &str) -> Result<String> {
    let Some(name) = value.strip_prefix('$') else {
        return Ok(value.to_owned());
    };
    let (name, suffix) = name.split_once('/').unwrap_or((name, ""));
    if name.is_empty() {
        bail!("{field} environment placeholder is empty");
    }
    let value = std::env::var(name)
        .with_context(|| format!("missing environment variable {name} for {field}"))?;
    Ok(format!("{value}{suffix}"))
}

fn daemon_token(settings: &DaemonSettings) -> Result<String> {
    load_or_create_daemon_token(settings)
}

fn daemon_token_path(settings: &DaemonSettings) -> PathBuf {
    settings.store_root.join(".daemon-token")
}

fn validate_daemon_token(value: String, source: &str) -> Result<String> {
    if value.trim().is_empty() || value.contains(['\r', '\n']) {
        bail!("daemon token from {source} must be nonempty and contain no newlines");
    }
    Ok(value)
}

fn load_or_create_daemon_token(settings: &DaemonSettings) -> Result<String> {
    let path = daemon_token_path(settings);
    if path.exists() {
        return read_daemon_token_file(&path);
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create daemon token directory {}", parent.display()))?;
    }

    let token = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let temp_path = path.with_file_name(format!(
        ".daemon-token.{}.tmp",
        uuid::Uuid::new_v4().simple()
    ));
    let mut file = match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temp_path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(error).with_context(|| {
                format!("daemon token temp path already exists: {}", temp_path.display())
            });
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("create daemon token temp file {}", temp_path.display()));
        }
    };

    if let Err(error) = std::io::Write::write_all(&mut file, token.as_bytes())
        .and_then(|_| file.sync_all())
    {
        let _ = fs::remove_file(&temp_path);
        return Err(error)
            .with_context(|| format!("write daemon token temp file {}", temp_path.display()));
    }

    match fs::hard_link(&temp_path, &path) {
        Ok(()) => {
            let _ = fs::remove_file(&temp_path);
            Ok(token)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let _ = fs::remove_file(&temp_path);
            read_daemon_token_file(&path)
        }
        Err(error) => {
            let _ = fs::remove_file(&temp_path);
            Err(error).with_context(|| {
                format!(
                    "publish daemon token file {} from {}",
                    path.display(),
                    temp_path.display()
                )
            })
        }
    }
}

fn read_daemon_token_file(path: &std::path::Path) -> Result<String> {
    enforce_daemon_token_permissions(path)?;
    validate_daemon_token(
        fs::read_to_string(path)
            .with_context(|| format!("read daemon token file {}", path.display()))?,
        &format!("file {}", path.display()),
    )
}

#[cfg(unix)]
fn enforce_daemon_token_permissions(path: &std::path::Path) -> Result<()> {
    let mut permissions = fs::metadata(path)
        .with_context(|| format!("inspect daemon token file {}", path.display()))?
        .permissions();
    if permissions.mode() & 0o777 != 0o600 {
        permissions.set_mode(0o600);
        fs::set_permissions(path, permissions)
            .with_context(|| format!("secure daemon token file {}", path.display()))?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn enforce_daemon_token_permissions(_path: &std::path::Path) -> Result<()> {
    Ok(())
}

async fn serve(config: &Config, config_path: &Path) -> Result<()> {
    let auto_paper = config.daemon.auto_paper.unwrap_or(false);
    let token = daemon_token(&config.daemon)?;
    let model = config
        .model
        .clone()
        .context("missing [model] configuration for daemon serve")?;
    let decision_policy = decision_policy_from_config(config, config_path)?;
    let model_capabilities = probe_configured_model_capabilities(&model)
        .await
        .context("probe configured model capabilities before daemon startup")?;
    let runtime_identity_hash = if auto_paper {
        Some(
            runtime_identity_from_config(config, config_path, &model_capabilities)?
                .identity_hash()?,
        )
    } else {
        None
    };
    let historical_evaluation_condition = match config.execution.experiment_profile {
        ExperimentProfile::PaperResearch => Some(ExperimentCondition::PostCutoffForward),
        ExperimentProfile::HistoricalEval => config.execution.historical_evaluation_condition,
        ExperimentProfile::Fixture | ExperimentProfile::PaperEngineering => None,
    };
    let model_knowledge_cutoff = historical_evaluation_condition
        .is_some()
        .then(|| {
            model
                .knowledge_cutoff
                .as_deref()
                .context("historical projection requires model.knowledge_cutoff")
                .and_then(|value| {
                    NaiveDate::parse_from_str(value, "%Y-%m-%d")
                        .context("parse model.knowledge_cutoff")
                })
        })
        .transpose()?;
    let daemon = Daemon::open(
        DaemonConfig {
            store_root: config.daemon.store_root.clone(),
            http_token: token,
            worker_count: config.daemon.worker_count.unwrap_or(4),
            auto_paper,
            market_data_feed: config.execution.market_data_feed,
            outcome_cost_model: OutcomeCostModel {
                transaction_cost_ppm: config.execution.transaction_cost_ppm,
                slippage_ppm: config.execution.slippage_ppm,
            },
            decision_policy,
            runtime_identity_hash,
            historical_evaluation_condition,
            model_knowledge_cutoff,
        },
        model,
        model_capabilities,
    )?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        if let Err(error) = wait_for_shutdown_signal().await {
            eprintln!("daemon shutdown signal handler failed: {error}");
        }
        let _ = shutdown_tx.send(true);
    });
    let paper = if auto_paper {
        Some(AlpacaPaper::from_env().context("construct Alpaca Paper client")?)
    } else {
        None
    };
    let clock = paper
        .as_ref()
        .map(|paper| AlpacaPaperSessionClock::new(paper.clone()));
    let daemon = match paper {
        Some(paper) => Arc::new(
            daemon
                .with_paper_observer(paper.clone())
                .with_paper_broker(Arc::new(paper)),
        ),
        None => Arc::new(daemon),
    };
    let http_daemon = daemon.clone();
    if auto_paper {
        let source = daemon.paper_workflow_source();
        source
            .proposal("preflight")
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))
            .context("load Paper workflow proposal")?;
        let clock = clock
            .as_ref()
            .context("Paper scheduler clock was not initialized")?;
        tokio::try_join!(
            http_daemon.serve_http(config.daemon.http_addr, shutdown_rx.clone()),
            daemon
                .serve_with_paper_scheduler(clock, &source, Duration::from_secs(30), shutdown_rx,),
        )?;
    } else {
        tokio::try_join!(
            http_daemon.serve_http(config.daemon.http_addr, shutdown_rx.clone()),
            http_daemon.serve_workers(shutdown_rx),
        )?;
    }
    Ok(())
}

async fn wait_for_shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut terminate = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("wait for Ctrl-C"),
            _ = terminate.recv() => Ok(()),
            result = wait_for_parent_stdin_eof() => result,
        }
    }

    #[cfg(not(unix))]
    {
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("wait for Ctrl-C"),
            result = wait_for_parent_stdin_eof() => result,
        }
    }
}

async fn wait_for_parent_stdin_eof() -> Result<()> {
    if std::env::var_os("AKZIO_EXIT_ON_STDIN_EOF").as_deref() != Some(std::ffi::OsStr::new("1")) {
        std::future::pending::<()>().await;
        unreachable!();
    }
    use tokio::io::AsyncReadExt;
    let mut stdin = tokio::io::stdin();
    let mut byte = [0_u8; 1];
    while stdin
        .read(&mut byte)
        .await
        .context("wait for parent stdin EOF")?
        != 0
    {}
    Ok(())
}

fn fixture_daemon(config: &Config) -> Result<Daemon> {
    Ok(Daemon::with_model(
        DaemonConfig {
            store_root: config.daemon.store_root.clone(),
            http_token: "fixture-only".to_owned(),
            worker_count: config.daemon.worker_count.unwrap_or(2),
            auto_paper: false,
            market_data_feed: config.execution.market_data_feed,
            outcome_cost_model: OutcomeCostModel {
                transaction_cost_ppm: config.execution.transaction_cost_ppm,
                slippage_ppm: config.execution.slippage_ppm,
            },
            decision_policy: DecisionPolicy::default(),
            runtime_identity_hash: None,
            historical_evaluation_condition: None,
            model_knowledge_cutoff: None,
        },
        fixture_model_client(),
    )?)
}

fn print_json<T: Serialize>(response: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(response)?);
    Ok(())
}

async fn fixture_debug(config: Config) -> Result<()> {
    let (report, _) = run_fixture_purpose(config, RunPurpose::PaperDryRun).await?;
    if report.status != WorkflowStatus::Completed {
        bail!(
            "fixture Debug workflow did not complete: {:?}",
            report.status
        );
    }
    println!(
        "{}",
        serde_json::json!({
            "run_id": report.run_id,
            // `fixture-debug` drives the PaperDryRun fixture path. Report the
            // Store-owned purpose so this is never read as Debug or as Paper
            // acceptance evidence.
            "purpose": report.purpose,
            "status": report.status,
            "fixture": true,
            "evidence": "fixture/offline"
        })
    );
    Ok(())
}

async fn paper_dry_run(config: Config) -> Result<()> {
    let (report, canonical_learning_events) =
        run_fixture_purpose(config, RunPurpose::PaperDryRun).await?;
    if report.status != WorkflowStatus::Completed {
        bail!(
            "Paper Dry Run workflow did not complete: {:?}",
            report.status
        );
    }
    if canonical_learning_events != 0 {
        bail!("Paper Dry Run produced canonical learning transition");
    }
    println!(
        "{}",
        serde_json::json!({
            "run_id": report.run_id,
            "purpose": "paper_dry_run",
            "status": format!("{:?}", report.status),
            "canonical_learning_events": canonical_learning_events,
            "fixture": true,
            "evidence": "fixture/offline"
        })
    );
    Ok(())
}

async fn run_fixture_purpose(config: Config, purpose: RunPurpose) -> Result<(ReplayReport, usize)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("bind ephemeral fixture control API")?;
    let addr = listener.local_addr()?;
    let token = "fixture-only".to_owned();
    let daemon = fixture_daemon(&config)?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let serve_daemon = daemon.clone();
    let server = tokio::spawn(async move {
        tokio::try_join!(
            serve_daemon.serve_http_listener(listener, shutdown_rx.clone()),
            serve_daemon.serve_workers(shutdown_rx),
        )
    });
    let client = ControlApiClient::new(addr, token)?;
    let mut ready = false;
    for _ in 0..100 {
        if client.health().await.is_ok() {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if !ready {
        let _ = shutdown_tx.send(true);
        let _ = server.await;
        bail!("fixture daemon HTTP control API did not become ready");
    }
    let submitted = match client.submit(purpose).await {
        Ok(submitted) => submitted,
        Err(error) => {
            let _ = shutdown_tx.send(true);
            let _ = server.await;
            return Err(error);
        }
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    let report = loop {
        match client.replay(&submitted.run_id.0).await {
            Ok(report)
                if matches!(
                    report.status,
                    WorkflowStatus::Completed
                        | WorkflowStatus::CompletedWithExecutionRejection
                        | WorkflowStatus::Failed
                        | WorkflowStatus::Cancelled
                ) =>
            {
                break report;
            }
            Ok(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok(report) => {
                let _ = shutdown_tx.send(true);
                let _ = server.await;
                bail!(
                    "fixture workflow did not reach a terminal status: {:?}",
                    report.status
                );
            }
            Err(error) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let _ = error;
            }
            Err(error) => {
                let _ = shutdown_tx.send(true);
                let _ = server.await;
                return Err(error);
            }
        }
    };
    let canonical_learning_events = client
        .store_events(&submitted.run_id, 0, 10_000)
        .await?
        .iter()
        .filter(|event| event.event_type == "policy.transitioned")
        .count();
    client.store_doctor().await?;
    let _ = shutdown_tx.send(true);
    match server.await {
        Ok(Ok(_)) => Ok((report, canonical_learning_events)),
        Ok(Err(error)) => Err(anyhow::anyhow!(error)),
        Err(error) => Err(error.into()),
    }
}
