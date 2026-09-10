fn default_outcome_processing() -> bool {
    true
}

#[derive(Debug, Subcommand)]
enum DebugCommand {
    /// Call the configured real transport and report provider metadata, without creating a Run.
    Preflight {
        /// Resolve the same per-recipe override/default selection used by Daemon, without model I/O.
        #[arg(long)]
        resolve_only: bool,
    },
    /// Serve the existing deterministic fixture adapters; never calls a provider.
    ServeFixture,
    Prepare {
        #[arg(long)]
        session: String,
        #[arg(long, default_value = "paper", value_parser = ["paper", "position-plan"])]
        purpose: String,
        #[arg(long)]
        paper_allowed: bool,
        /// Exercise the legacy CI fixture with the same persisted controller.
        #[arg(long)]
        fixture_controller: bool,
    },
    Nodes {
        run_id: String,
    },
    Pause {
        run_id: String,
    },
    Resume {
        run_id: String,
    },
    Step {
        run_id: String,
        #[arg(long)]
        task: String,
        #[arg(long, default_value_t = 300)]
        wait_seconds: u64,
    },
    RetryNode {
        run_id: String,
        #[arg(long)]
        task: String,
        #[arg(long, default_value_t = 300)]
        wait_seconds: u64,
    },
    Inspect {
        run_id: String,
        #[arg(long)]
        task: Option<String>,
        #[arg(long)]
        attempt: Option<String>,
    },
    Fork {
        run_id: String,
        #[arg(long)]
        task: String,
        #[arg(long)]
        reason: String,
        #[arg(long)]
        experiment_id: Option<String>,
    },
    /// A new experiment from Run identity, including when the first stage failed.
    Experiment {
        run_id: String,
        /// Independent PositionPlan experiment requesting one authorized range read.
        #[arg(long)]
        read_range_probe: bool,
        #[arg(long)]
        reason: String,
        #[arg(long)]
        experiment_id: Option<String>,
    },
    /// Append a typed acceptance result with evidence references.
    Acceptance {
        run_id: String,
        #[arg(long)]
        input: PathBuf,
    },
    /// Export a read-only, share-safe diagnostic bundle without rerunning work.
    ExportBundle {
        run_id: String,
        #[arg(long)]
        out: PathBuf,
        /// Explicit offline Store Root. No daemon, model, broker, migration or repair is started.
        #[arg(long)]
        store: Option<PathBuf>,
    },
}

async fn dispatch_debug(command: DebugCommand, config: &Config, _config_path: &Path) -> Result<()> {
    use akzio_domain::{DebugAction, DebugControlRequest, DebugSession, TaskId};
    if matches!(command, DebugCommand::ServeFixture) {
        if !config.daemon.debug_control
            || config.daemon.auto_paper.unwrap_or(false)
            || config.execution.experiment_profile != ExperimentProfile::Fixture
        {
            bail!("serve-fixture requires debug_control=true, auto_paper=false, experiment_profile=fixture");
        }
        let daemon = fixture_daemon(config)?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        tokio::spawn(async move {
            let _ = wait_for_shutdown_signal().await;
            let _ = shutdown_tx.send(true);
        });
        return tokio::try_join!(
            daemon.serve_http(config.daemon.http_addr, shutdown_rx.clone()),
            daemon.serve_workers(shutdown_rx)
        )
        .map(|_| ())
        .map_err(Into::into);
    }
    if let DebugCommand::Preflight { resolve_only } = command {
        if !config.daemon.debug_control || config.daemon.auto_paper.unwrap_or(false) {
            bail!("preflight requires isolated Debug configuration with auto_paper=false");
        }
        let model = config
            .model
            .as_ref()
            .context("missing model configuration")?;
        if resolve_only {
            let roles = ["research.analyst", "research.critic", "research.synthesizer", "learning.outcome_worker"];
            let resolutions = roles.map(|role| {
                let route = model.routes.get(role);
                let resolved = route.map(|r| model.for_route(r)).unwrap_or_else(|| model.clone());
                serde_json::json!({"role":role,"configured_route":route,
                    "fallback":if route.is_some() { "role override" } else { "shared default" },
                    "provider":"openai_responses","requested_model":resolved.model,
                    "reasoning_effort":resolved.reasoning_effort,"release_date":resolved.release_date,
                    "knowledge_cutoff":resolved.knowledge_cutoff,"actual_model":null})
            });
            return print_json(&resolutions);
        }
        let mut reports = std::collections::BTreeMap::new();
        let (capabilities, calls) = akzio_model::ModelClient::from_config(model)?
            .probe_capabilities_audited()
            .await?;
        reports.insert(
            "default".to_owned(),
            serde_json::json!({"capabilities": capabilities, "calls": calls}),
        );
        for (purpose, route) in &model.routes {
            let (capabilities, calls) =
                akzio_model::ModelClient::from_config(&model.for_route(route))?
                    .probe_capabilities_audited()
                    .await?;
            reports.insert(
                purpose.clone(),
                serde_json::json!({"capabilities": capabilities, "calls": calls}),
            );
        }
        return print_json(&reports);
    }
    if let DebugCommand::ExportBundle { run_id, out, store } = command {
        if let Some(store_root) = store {
            let store = Store::open_existing(store_root)
                .context("open explicit Store Root in offline read-only mode")?;
            let manifest = store.export_debug_bundle(&RunId(run_id), out)?;
            return print_json(&manifest);
        }
        let client = ControlApiClient::from_config(config)?;
        return print_json(&client.store_export_debug_bundle(&run_id, &out).await?);
    }
    let client = ControlApiClient::from_config(config)?;
    match command {
        DebugCommand::ServeFixture
        | DebugCommand::Preflight { .. }
        | DebugCommand::ExportBundle { .. } => unreachable!(),
        DebugCommand::Prepare {
            session,
            purpose,
            paper_allowed,
            fixture_controller,
        } => print_json(
            &client
                .debug_prepare(&akzio_daemon::DebugPrepareRequest {
                    session_key: session,
                    purpose: if purpose == "position-plan" { RunPurpose::PositionPlan } else { RunPurpose::Paper },
                    paper_allowed,
                    fixture_controller,
                })
                .await?,
        ),
        DebugCommand::Inspect {
            run_id,
            task,
            attempt,
        } => print_json(
            &client
                .debug_inspect(&run_id, task.as_deref(), attempt.as_deref())
                .await?,
        ),
        DebugCommand::Nodes { run_id } => {
            let value = client.debug_inspect(&run_id, None, None).await?;
            print_json(&serde_json::json!({"session":value["session"],"nodes":value["nodes"]}))
        }
        DebugCommand::Fork {
            run_id,
            task,
            reason,
            experiment_id,
        } => print_json(
            &client
                .debug_fork(
                    &run_id,
                    &akzio_daemon::DebugForkRequest {
                        task_id: Some(TaskId(task)),
                        read_range_probe: false,
                        experiment_id: experiment_id.map(RunId).unwrap_or_default(),
                        reason,
                    },
                )
                .await?,
        ),
        DebugCommand::Experiment {
            run_id,
            read_range_probe,
            reason,
            experiment_id,
        } => print_json(
            &client
                .debug_fork(
                    &run_id,
                    &akzio_daemon::DebugForkRequest {
                        task_id: None,
                        read_range_probe,
                        experiment_id: experiment_id.map(RunId).unwrap_or_default(),
                        reason,
                    },
                )
                .await?,
        ),
        DebugCommand::Acceptance { run_id, input } => {
            let value: akzio_domain::StageAcceptance = serde_json::from_slice(&fs::read(input)?)?;
            print_json(&client.debug_acceptance(&run_id, &value).await?)
        }
        command => {
            let (run_id, action, task, wait_seconds) = match command {
                DebugCommand::Pause { run_id } => (run_id, DebugAction::Pause, None, 0),
                DebugCommand::Resume { run_id } => (run_id, DebugAction::Resume, None, 0),
                DebugCommand::Step {
                    run_id,
                    task,
                    wait_seconds,
                } => (run_id, DebugAction::Step, Some(TaskId(task)), wait_seconds),
                DebugCommand::RetryNode {
                    run_id,
                    task,
                    wait_seconds,
                } => (
                    run_id,
                    DebugAction::RetryNode,
                    Some(TaskId(task)),
                    wait_seconds,
                ),
                _ => unreachable!(),
            };
            let view = client.debug_inspect(&run_id, None, None).await?;
            let session: DebugSession = serde_json::from_value(view["session"].clone())?;
            let granted = client
                .debug_control(
                    &run_id,
                    &DebugControlRequest {
                        action,
                        expected_revision: session.revision,
                        task_id: task.clone(),
                    },
                )
                .await?;
            if wait_seconds == 0 {
                return print_json(&granted);
            }
            let deadline =
                tokio::time::Instant::now() + Duration::from_secs(wait_seconds.min(3600));
            let mut refresh = tokio::time::interval(Duration::from_millis(250));
            loop {
                refresh.tick().await;
                let value = client.debug_inspect(&run_id, None, None).await?;
                let state = value["session"]["status"].as_str().unwrap_or("unknown");
                if !matches!(state, "stepping" | "pause_requested") {
                    let selected = value["nodes"].as_array().and_then(|nodes| {
                        nodes.iter().find(|n| {
                            n["task"]["node"]["task_id"].as_str()
                                == task.as_ref().map(|t| t.0.as_str())
                        })
                    });
                    let next = value["nodes"]
                        .as_array()
                        .map(|nodes| {
                            nodes
                                .iter()
                                .filter(|n| n["step_eligible"] == true)
                                .cloned()
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    return print_json(
                        &serde_json::json!({"session":value["session"],"selected_task":selected,"acceptance":value["acceptance"],"next_ready_tasks":next}),
                    );
                }
                if tokio::time::Instant::now() >= deadline {
                    print_json(&value["session"])?;
                    bail!("observation timeout; persisted step remains active, inspect before issuing another control");
                }
            }
        }
    }
}
