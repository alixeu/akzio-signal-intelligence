// 文件导读：本文件承载 CLI 的配置加载、daemon token、服务启动和关闭信号逻辑。它把
// 本地配置解析为 daemon 的启动输入，再由 daemon 负责 HTTP、worker、scheduler 和
// Store 事务；token 可用、服务启动或 scheduler tick 成功，都不能越级说明 Run、订单、
// fill 或跨交易日 Outcome 已完成。
// Rust 机制：`#[cfg]` 让 Unix 权限实现与其他平台实现分别编译；`Arc` 共享 daemon，
// `watch` 广播关闭状态，`tokio::spawn` 拥有后台 Future，`try_join!` 等待并行服务且
// 任一错误都会传播，避免后台任务静默脱离生命周期。

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

fn resolve_env_placeholder(value: &str, field: &str) -> Result<String> {
    // 只解析以 '$' 开头的环境占位符，并把第一个 '/' 之后的内容作为字面后缀；
    // 非占位符原样返回，缺失变量或空变量名直接阻断配置加载。
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
    // 统一复用“读取或安全创建”逻辑，保证 HTTP client/server 使用同一个 Store Root 下
    // 的 token；token 可用不代表 daemon 已通过 Paper 或执行授权。
    load_or_create_daemon_token(settings)
}

fn daemon_token_path(settings: &DaemonSettings) -> PathBuf {
    // Token 与 Store Root 绑定，存放在该根目录的隐藏文件中，不进入 CAS Artifact。
    settings.store_root.join(".daemon-token")
}

fn validate_daemon_token(value: String, source: &str) -> Result<String> {
    // HTTP 认证 token 必须非空且不能跨行，避免从文件或环境读取时引入额外 header 内容。
    if value.trim().is_empty() || value.contains(['\r', '\n']) {
        bail!("daemon token from {source} must be nonempty and contain no newlines");
    }
    Ok(value)
}

fn load_or_create_daemon_token(settings: &DaemonSettings) -> Result<String> {
    // 已有 token 只读并校验；首次创建使用独占临时文件、0600 权限、sync 和 hard-link
    // 发布，遇到并发创建则删除自己的临时文件并读取已发布版本，不覆盖现有 token。
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
    // 读取前先收紧 Unix 权限，再校验文本内容；权限修复是认证文件的本地副作用，
    // 不涉及 Store 业务状态。
    enforce_daemon_token_permissions(path)?;
    validate_daemon_token(
        fs::read_to_string(path)
            .with_context(|| format!("read daemon token file {}", path.display()))?,
        &format!("file {}", path.display()),
    )
}

#[cfg(unix)]
fn enforce_daemon_token_permissions(path: &std::path::Path) -> Result<()> {
    // 仅修正权限位，不改写 token 内容；非 0600 时尽力收紧并把失败向上传播。
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
    // 非 Unix 平台没有本模块可用的权限位接口，内容校验仍由调用方执行。
    Ok(())
}

async fn serve(config: &Config, config_path: &Path) -> Result<()> {
    let run_log = akzio_daemon::RunLogger::open()
        .context("正式 Core 启动需要私有运行日志目录")?;
    let recovery_store = Store::open(&config.daemon.store_root)?;
    if recovery_store.debug_environment()?.is_some() {
        bail!("正式 Core 拒绝使用旧隔离 Debug Store；未发起模型或交易 I/O");
    }
    run_log.recover_orphans(&recovery_store)?;
    run_log.attach_store(recovery_store)?;
    // 组装并启动 daemon：先读取 SQL active Policy、校验 Debug/Paper 的启动前置，再探测
    // 模型能力和构造 RuntimeIdentity，最后才创建 HTTP/worker/scheduler。启动成功不等于
    // Run、Decision、ExecutionVerdict、PaperCommit 或成交已经完成。
    let auto_paper = config.daemon.auto_paper.unwrap_or(false);
    let model = config
        .model
        .clone()
        .context("missing [model] configuration for daemon serve")?;
    run_log.register_secret(&model.api_key)?;
    let loaded_policy = load_decision_policy_from_config(config)?;
    let decision_policy = loaded_policy.policy.clone();
    let token = daemon_token(&config.daemon)?;
    run_log.register_secret(&token)?;
    for key in ["ALPACA_API_KEY", "ALPACA_API_SECRET", "FRED_API_KEY"] {
        if let Ok(secret) = std::env::var(key) {
            run_log.register_secret(&secret)?;
        }
    }
    // Provider capability probe 发生在 Daemon::open 之前；失败则不会启动 worker 或 scheduler。
    let startup_scope = akzio_model::ModelAuditContext::startup(run_log.core_session());
    run_log.record(&startup_scope, "core.startup", serde_json::json!({"status":"started"}))?;
    let probe_result = akzio_model::MODEL_AUDIT_CONTEXT
        .scope(
            startup_scope.clone(),
            akzio_model::probe_configured_model_capabilities_with_audit(
                &model,
                run_log.clone(),
            ),
        )
        .await;
    run_log.record(
        &startup_scope,
        "core.probe",
        serde_json::json!({"status":if probe_result.is_ok() {"completed"} else {"failed"}}),
    )?;
    if let Some(receipt) =
        run_log.seal_startup(if probe_result.is_ok() {"completed"} else {"failed"})?
    {
        run_log.confirm_startup(&receipt)?;
    }
    let model_capabilities = probe_result
        .context("probe configured model capabilities before daemon startup")?;
    let runtime_identity_hash = if auto_paper || config.daemon.manual_paper {
        // 只有会影响 Paper/Debug 身份的模式才绑定完整 RuntimeIdentity；普通研究服务不
        // 通过这个可选字段伪造审批或 Decision 身份。
        Some(
            runtime_identity_from_config_with_policy(
                config,
                config_path,
                &model_capabilities,
                &decision_policy,
            )?
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
            agent_budget: config.agent.budget.clone(),
            research_settings: config.agent.research.clone(),
            outcome_processing: config.daemon.outcome_processing,
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
            run_log: Some(run_log.clone()),
        },
        model,
        model_capabilities,
    )?;
    // shutdown channel 同时交给 HTTP 和 worker/scheduler；任一关闭信号只请求停止，不会
    // 撤销已经持久化的 Run 或订单状态。
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        if let Err(error) = wait_for_shutdown_signal().await {
            eprintln!("daemon shutdown signal handler failed: {error}");
        }
        let _ = shutdown_tx.send(true);
    });
    let paper = if auto_paper || config.daemon.manual_paper {
        // Debug/Paper 模式需要 Paper client 供观察或正式链路使用；普通研究服务不构造
        // Broker 连接，从而保持未授权路径没有外部交易 I/O。
        let guard_log = run_log.clone();
        let guard = Arc::new(move |kind: PaperWriteKind| {
            let scope = akzio_model::MODEL_AUDIT_CONTEXT
                .try_with(Clone::clone)
                .map_err(|_| PaperError::RunLogUnavailable("broker write has no Run scope".into()))?;
            if kind != PaperWriteKind::Cancel
                && guard_log
                    .run_cancel_requested(&scope.run_id)
                    .map_err(|error| PaperError::RunLogUnavailable(error.to_string()))?
            {
                return Err(PaperError::RunCancelled);
            }
            if kind == PaperWriteKind::Cancel
                && (guard_log.assert_available().is_err()
                    || guard_log.is_uncertain_run(&scope.run_id))
            {
                // The dispatch path must have already persisted the exact
                // protective cancel intent; audit failure cannot strand it.
                return Ok(());
            }
            let event = match kind {
                PaperWriteKind::Submit => "rust.broker.submit_http",
                PaperWriteKind::Replace => "rust.broker.replace_http",
                PaperWriteKind::Cancel => "rust.broker.cancel_http",
            };
            guard_log
                .record(&scope, event, serde_json::json!({"status":"before_http_io"}))
                .map_err(|error| PaperError::RunLogUnavailable(error.to_string()))
        });
        Some(
            AlpacaPaper::from_env()
                .context("construct Alpaca Paper client")?
                .with_write_guard(guard),
        )
    } else {
        None
    };
    let clock = paper
        .as_ref()
        .map(|paper| AlpacaPaperSessionClock::new(paper.clone()));
    let daemon = match paper {
        // Arc 克隆只共享已构造的 daemon；with_paper_* 注册观察器和 Broker 边界，实际订单
        // 仍须通过运行时 Gate、Commitment 和 Store effect intent。
        Some(paper) => Arc::new(
            daemon
                .with_paper_observer(paper.clone())
                .with_paper_broker(Arc::new(paper)),
        ),
        None => Arc::new(daemon),
    };
    let http_daemon = daemon.clone();
    if auto_paper {
        // Scheduler 启动前必须能加载 Paper workflow proposal；随后 HTTP 与 Paper scheduler
        // 并行运行。proposal 被加载不代表某个 Run 已完成或订单已提交。
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
                .serve_with_paper_scheduler(clock, &source, Duration::from_secs(30), shutdown_rx.clone(),),
            daemon.supervise_run_archives(shutdown_rx),
        )?;
    } else {
        // 非 auto_paper 只提供 HTTP 和普通 worker，不启动交易 Session scheduler。
        tokio::try_join!(
            http_daemon.serve_http(config.daemon.http_addr, shutdown_rx.clone()),
            http_daemon.serve_workers(shutdown_rx.clone()),
            daemon.supervise_run_archives(shutdown_rx),
        )?;
    }
    // Workers have joined before the last incomplete stage is sealed.
    daemon.archive_remaining_after_shutdown()?;
    Ok(())
}

async fn wait_for_shutdown_signal() -> Result<()> {
    // 统一等待 Ctrl-C、SIGTERM（Unix）或显式开启的父进程 stdin EOF；返回只表示收到
    // 停止信号，调用方负责把 watch 值传播给服务组件。
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
    // 默认永久等待且不读取 stdin；只有 AKZIO_EXIT_ON_STDIN_EOF=1 时才把父进程 EOF
    // 作为关闭信号，避免普通管道状态意外终止 daemon。
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

fn print_json<T: Serialize>(response: &T) -> Result<()> {
    // 所有 CLI 分支通过同一 pretty JSON 边界输出结构化结果；序列化失败向调用方传播，
    // 不把半截响应当作成功。
    println!("{}", serde_json::to_string_pretty(response)?);
    Ok(())
}
