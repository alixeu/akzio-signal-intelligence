// 文件导读：Debug CLI 连接隔离 Core 的 prepare/inspect/step/resume/fork/export 入口，并
// 保持 fixture、真实模型探测和只读导出彼此分离。CLI 只发送带 expected revision 的控制
// 请求，Store/CAS 才决定 claim、lease、状态和权限；accepted/Completed 或导出成功都不
// 等于 Decision、Paper submission、fill 或 Outcome/learning 完成。
// Rust 机制：clap derive 宏生成强类型枚举；异步控制使用 `Future`/`await`，`watch` 负责
// 关闭传播，`Arc::make_mut` 只在测试 fixture 独占时复制写时数据，`Result`/`Option` 明确
// 区分缺失资源、阻断和实际错误。

fn default_outcome_processing() -> bool {
    // Debug/fixture 默认保留 Outcome worker；这只决定后续评估是否可处理，
    // 不会把 PositionPlan 的研究或 Decision 结果提升为 Paper 执行。
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
    /// Verify the formal nine-node PositionPlan with isolated deterministic adapters.
    VerifyFixture,
    /// Evaluate the installed Reviewer on fixed synthetic cases in an isolated Store.
    VerifyResearchQuality {
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value = "offline", value_parser = ["offline", "baseline", "candidate", "report"])]
        phase: String,
    },
    Prepare {
        #[arg(long)]
        session: String,
        #[arg(long, default_value = "paper", value_parser = ["paper", "position-plan"])]
        purpose: String,
        #[arg(long)]
        paper_allowed: bool,
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
    // 这里区分三类入口：fixture 服务/验证使用本地隔离实现，Preflight/ExportBundle
    // 可完全只读运行，其余控制命令通过已认证的 daemon API 操作现有 Debug Run。
    if matches!(command, DebugCommand::ServeFixture) {
        // ServeFixture 只在显式 fixture 配置下启动 HTTP 和 worker；启动服务本身不代表
        // Run 已完成，也不允许 auto_paper 或非 fixture profile 混入。
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
        // resolve_only 只展开配置中的角色路由；普通 Preflight 才会调用 provider 做能力
        // 探测，而且所有探测结果仅打印为报告，不创建研究 Run 或写 Store。
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
        // 提供 Store Root 时走离线只读导出；否则把同一请求交给 daemon。两条路径都只
        // 导出已有 Run 的诊断资料，不重新执行节点、模型或 Broker 操作。
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
        | DebugCommand::ExportBundle { .. } | DebugCommand::VerifyFixture
        | DebugCommand::VerifyResearchQuality { .. } => unreachable!(),
        DebugCommand::Prepare {
            session,
            purpose,
            paper_allowed,
        } => print_json(
            &client
                .debug_prepare(&akzio_daemon::DebugPrepareRequest {
                    session_key: session,
                    purpose: if purpose == "position-plan" { RunPurpose::PositionPlan } else { RunPurpose::Paper },
                    paper_allowed,
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
            // Nodes 是 inspection 的裁剪投影，不能替代完整的 session revision 或
            // acceptance 信息。
            let value = client.debug_inspect(&run_id, None, None).await?;
            print_json(&serde_json::json!({"session":value["session"],"nodes":value["nodes"],"research":value["research"]}))
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
                        experiment_id: experiment_id.map(RunId).unwrap_or_default(),
                        reason,
                    },
                )
                .await?,
        ),
        DebugCommand::Experiment {
            run_id,
            reason,
            experiment_id,
        } => print_json(
            &client
                .debug_fork(
                    &run_id,
                    &akzio_daemon::DebugForkRequest {
                        task_id: None,
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
            // 控制动作先读取当前 DebugSession revision，再以 CAS 语义提交；若需要等待，
            // 后续轮询只观察持久化状态，不会在超时后自动重复发送动作。
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
            // 等待窗口最多 3600 秒；超时只报告仍在 stepping/pause_requested 的事实，
            // 不撤销已受理的控制请求，也不暗示节点已经完成。
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

/// A fresh isolated Store and explicit adapters; no config, provider or broker credentials are read.
async fn verify_fixture() -> Result<()> {
    use akzio_domain::{DebugAction, DebugControlRequest, TaskStatus};
    // Fixture 验证从新的 .akzio 子目录开始，使用固定模型/适配器和 forbidden Broker，
    // 只验正式 PositionPlan 拓扑、Store Doctor 与诊断导出，不验真实模型或 Paper 业务。
    let output = std::env::current_dir()?.join(".akzio").join(format!("verify-fixture-{}", RunId::new()));
    fs::create_dir_all(&output)?;
    let store_root = output.join("store");
    let daemon = Daemon::with_model(DaemonConfig {
        agent_budget: Default::default(),
            research_settings: Default::default(),
        debug_control: Some(akzio_daemon::DebugCoreConfig {
            code_revision: source_revision()?,
            runtime_identity: ContentHash::of_bytes(b"formal-fixture-v67"),
            decision_policy_status: "fixture_default".into(),
            decision_policy_input_hash: None,
            decision_policy_artifact: None,
        }),
        outcome_processing: true,
        store_root: store_root.clone(),
        http_token: "fixture-only".into(),
        worker_count: 1,
        auto_paper: false,
        market_data_feed: Some(AlpacaMarketDataFeed::Sip),
        outcome_cost_model: OutcomeCostModel { transaction_cost_ppm: 0, slippage_ppm: 0 },
        decision_policy: DecisionPolicy::default(),
        runtime_identity_hash: None,
        historical_evaluation_condition: None,
        model_knowledge_cutoff: None,
    }, fixture_model_client())?;
    let session = daemon.prepare_debug(&akzio_daemon::DebugPrepareRequest {
        purpose: RunPurpose::PositionPlan,
        session_key: chrono::Utc::now().format("%Y-%m-%d").to_string(),
        paper_allowed: false,
    })?;
    let run = &session.identity.run_id;
    let execution: Result<()> = async {
        // 每次只领取一个当前 ready 节点，并在 worker 执行后重新读取 inspection；
        // 因此循环依赖持久化的节点数量和 step eligibility，而不是本地推断拓扑完成。
        for _ in 0..daemon.inspect_debug(run, None, None)?.nodes.len() {
            let view = daemon.inspect_debug(run, None, None)?;
            let ready = view.nodes.iter().find(|n| n.step_eligible).context("formal fixture has no ready node")?;
            daemon.control_debug(run, &DebugControlRequest {
                action: DebugAction::Step, expected_revision: view.session.revision,
                task_id: Some(ready.task.node.task_id.clone()),
            })?;
            if !daemon.run_one("verify-fixture").await? { bail!("fixture task was not claimed"); }
        }
        Ok(())
    }.await;
    let view = daemon.inspect_debug(run, None, None)?;
    // 即使某个阶段失败，也先保存节点视图、Doctor 和 bundle 结果，再由 passed 决定
    // 命令是否返回错误，保留失败现场供离线诊断。
    fs::write(output.join("nodes.json"), serde_json::to_vec_pretty(&view)?)?;
    let store = Store::open(&store_root)?;
    let doctor = store.verify_integrity();
    let export = store.export_debug_bundle(run, output.join("bundle"));
    let passed = execution.is_ok() && doctor.is_ok() && export.is_ok()
        && view.nodes.len() == 21 && view.nodes.iter().all(|n| matches!(n.task.status, TaskStatus::Succeeded | TaskStatus::Skipped))
        && session.identity.broker_write_policy == akzio_domain::DebugBrokerPolicy::Forbidden;
    let report = serde_json::json!({"run_id":run,"purpose":"position_plan","passed":passed,
        "evidence":"fixture/offline","evidence_directory":output,"nodes":view.nodes.len(),
        "doctor":doctor.as_ref().map(|_| "pass").map_err(ToString::to_string),
        "execution_error":execution.err().map(|e| e.to_string()),"export_error":export.err().map(|e| e.to_string())});
    fs::write(output.join("summary.json"), serde_json::to_vec_pretty(&report)?)?;
    print_json(&report)?;
    if !passed { bail!("formal fixture verification failed; see {}", output.display()); }
    Ok(())
}

#[cfg(test)]
mod retired_cli_tests {
    use super::*;
    #[test]
    fn legacy_creation_commands_are_not_accepted() {
        for args in [vec!["akzio","run","fixture-debug"],vec!["akzio","run","paper-dry-run"],vec!["akzio","run","submit","debug"],vec!["akzio","debug","prepare","--session","2026-09-22","--fixture-controller"]] {
            assert!(Cli::try_parse_from(args).is_err());
        }
        assert!(Cli::try_parse_from(["akzio","debug","verify-fixture"]).is_ok());
        assert!(toml::from_str::<akzio_domain::AgentSettings>("[budget.planner]\nmax_output_tokens=2000").is_err());
    }
}
