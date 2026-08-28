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
    std::env::var(&settings.token_env).with_context(|| {
        format!(
            "missing daemon token environment variable {}",
            settings.token_env
        )
    })
}

async fn serve(config: &Config, config_path: &Path) -> Result<()> {
    let auto_paper = config.daemon.auto_paper.unwrap_or(false);
    let token = daemon_token(&config.daemon)?;
    let observer_token = config
        .daemon
        .observer_token_env
        .as_deref()
        .map(|name| {
            std::env::var(name)
                .with_context(|| format!("missing observer token environment variable {name}"))
        })
        .transpose()?;
    if observer_token.as_deref().is_some_and(str::is_empty) {
        bail!("observer token environment variable must not be empty");
    }
    let model = config
        .model
        .clone()
        .context("missing [model] configuration for daemon serve")?;
    let runtime_identity_hash = if auto_paper {
        Some(runtime_identity_from_config(config, config_path)?.identity_hash()?)
    } else {
        None
    };
    let daemon = Daemon::open(
        DaemonConfig {
            store_root: config.daemon.store_root.clone(),
            http_token: token,
            observer_token,
            worker_count: config.daemon.worker_count.unwrap_or(4),
            auto_paper,
            market_data_feed: config.execution.market_data_feed,
            outcome_cost_model: OutcomeCostModel {
                transaction_cost_ppm: config.execution.transaction_cost_ppm,
                slippage_ppm: config.execution.slippage_ppm,
            },
            runtime_identity_hash,
        },
        model,
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
