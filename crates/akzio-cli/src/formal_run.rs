//! 正式 Run 的薄客户端：复用 Core，或显式 auto-serve 后等待 ready，再提交一次。
//! 不轮询 Run 至完成，不直接打开 Store，不把研究建议提升为执行权限。

use super::*;

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub(crate) enum FormalPurpose {
    PositionPlan,
    Paper,
}

impl FormalPurpose {
    fn wire(self) -> &'static str {
        match self {
            Self::PositionPlan => "position_plan",
            Self::Paper => "paper",
        }
    }
}

pub(crate) fn handles(command: &RunCommand) -> bool {
    matches!(
        command,
        RunCommand::Start { .. } | RunCommand::Detail { .. } | RunCommand::ExportBundle { .. }
    )
}

// 不采用 load_config 的环境覆盖：客户端必须使用配置指定的正式 Store 身份。
// 环境中的同一路径可用，不同路径拒绝；绝不打印配置正文、token 或 provider 错误正文。
fn validate_settings(
    settings: &DaemonSettings,
    purpose: Option<FormalPurpose>,
    store_override: Option<&Path>,
) -> Result<()> {
    if settings.debug_control {
        bail!("正式 Run 客户端拒绝 debug_control=true；请使用已有 canonical Core");
    }
    if !settings.store_root.is_absolute() {
        bail!("daemon.store_root 必须为绝对路径；客户端不猜测 Core 的工作目录");
    }
    if !settings.http_addr.ip().is_loopback() {
        bail!("daemon.http_addr 必须为回环地址；未发出 HTTP 请求");
    }
    if let Some(other) = store_override {
        if other.canonicalize()? != settings.store_root.canonicalize()? {
            bail!("AKZIO_STORE_ROOT 与配置 Store 不一致；未发出 HTTP 请求");
        }
    }
    if matches!(purpose, Some(FormalPurpose::Paper))
        && !(settings.manual_paper || settings.auto_paper.unwrap_or(false))
    {
        bail!("配置未启用 manual_paper/auto_paper；客户端不会修改配置或审批");
    }
    Ok(())
}

fn port_is_listening(address: SocketAddr) -> Result<bool> {
    // 只把明确的 ConnectionRefused 当作“未运行”。占用、认证失败、超时和其他
    // 网络错误都不能触发第二个 Core，更不能向未知服务提交 Run。
    if !address.ip().is_loopback() {
        bail!("Core 地址不是回环地址；拒绝探测或启动");
    }
    match std::net::TcpStream::connect_timeout(&address, Duration::from_millis(500)) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => Ok(false),
        Err(error) => Err(error).context("检测已有 Core 端口；不自动启动第二个 Core"),
    }
}

fn prepare_formal_log_root(store_root: &Path) -> Result<PathBuf> {
    // home/store 与 home/logs 共享同一根；正式 CLI 日志也放在该 home 下，
    // 而不是仓库 .akzio/。不打开或复制 Store/token，也不覆盖已有 core.log。
    let home = store_root.parent().context("正式 Store 缺少 home 父目录")?;
    let log_root = home.join("logs");
    match fs::symlink_metadata(&log_root) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
        Ok(_) => bail!("正式 Core 日志目录不是普通目录或是符号链接"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                fs::DirBuilder::new()
                    .mode(0o700)
                    .create(&log_root)
                    .context("创建正式 Core 日志目录")?;
            }
            #[cfg(not(unix))]
            fs::create_dir(&log_root).context("创建正式 Core 日志目录")?;
        }
        Err(error) => return Err(error).context("检查正式 Core 日志目录"),
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&log_root, fs::Permissions::from_mode(0o700))
            .context("限制正式 Core 日志目录权限")?;
    }
    log_root.canonicalize().context("解析正式 Core 日志目录")
}

fn spawn_formal_core(config_path: &Path, store_root: &Path) -> Result<impl FnMut() -> Result<()>> {
    let log_root = prepare_formal_log_root(store_root)?;
    let log = log_root.join(format!("core-{}.log", uuid::Uuid::new_v4()));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let output = options.open(&log).context("创建仅本用户可读的 Core 日志")?;
    let config = config_path
        .canonicalize()
        .context("定位正式 Core 配置路径")?;
    let binary = std::env::current_exe().context("定位当前 Rust CLI 二进制")?;

    // nohup + 独立进程组让 Core 在脚本返回和终端断开后继续处理 T1/T3/T5。
    // stdin EOF 退出仅供受控 child 使用；CLI 自动启动不继承这一行为。
    let mut command = std::process::Command::new("nohup");
    command
        .arg(binary)
        .arg("--config")
        .arg(&config)
        .arg("daemon")
        .arg("serve")
        .current_dir(config.parent().context("配置缺少父目录")?)
        .env("AKZIO_EXIT_ON_STDIN_EOF", "0")
        .stdin(std::process::Stdio::null())
        .stdout(output.try_clone().context("复制 Core 日志句柄")?)
        .stderr(output);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = command.spawn().context("启动当前源码的正式 Core")?;
    println!(
        "CORE_STARTED_PID={}\nCORE_LOG={}",
        child.id(),
        log.display()
    );
    Ok(move || {
        if let Some(status) = child.try_wait().context("查询刚启动的 Core 状态")? {
            bail!("Core 在 ready 前退出 ({status})；请检查 CORE_LOG，不会提交 Run");
        }
        Ok(())
    })
}

async fn ensure_formal_core<F, C>(
    config: &Config,
    purpose: FormalPurpose,
    launch: F,
    timeout: Duration,
) -> Result<ControlApiClient>
where
    F: FnOnce() -> Result<C>,
    C: FnMut() -> Result<()>,
{
    if port_is_listening(config.daemon.http_addr)? {
        // 端口有服务就只能使用匹配 Store token 和 canonical /ready 的原服务。
        // 不因无 token、认证错误或旧版服务而重启、覆盖或误接管。
        let client = ControlApiClient::from_config(config)
            .context("端口已有服务但无法使用所选 Store token；不启动第二个 Core")?;
        require_formal_ready(&client).await?;
        return Ok(client);
    }
    if matches!(purpose, FormalPurpose::PositionPlan) && config.daemon.auto_paper.unwrap_or(false) {
        bail!("配置 auto_paper=true；自动启动会运行 Paper scheduler，拒绝把 PositionPlan 变成交易入口");
    }

    let mut check_child = launch()?;
    let deadline = std::time::Instant::now() + timeout;
    loop {
        check_child()?;
        if port_is_listening(config.daemon.http_addr)? {
            let token = config.daemon.store_root.join(".daemon-token");
            if token.exists() {
                let client = ControlApiClient::from_config(config)
                    .context("新 Core 的 token 无法读取；不会提交 Run")?;
                require_formal_ready(&client).await?;
                return Ok(client);
            }
        }
        if std::time::Instant::now() >= deadline {
            bail!("等待正式 Core ready 超时；请检查 CORE_LOG。Core 可能仍在启动，勿重复发起 Run");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn require_formal_ready(client: &ControlApiClient) -> Result<()> {
    // 有意读取最小 JSON 投影；旧版 Core 缺字段时明确拒绝，不用默认值推断 canonical。
    let ready: serde_json::Value = client
        .json(
            client
                .request(Method::GET, client.endpoint(&["ready"]))
                .timeout(Duration::from_secs(10)),
        )
        .await
        .context("正式 Core 无法通过 /ready；请先启动对应 Core，未提交 Run")?;
    if ready["status"] != "ok" || ready["store_scope"] != "canonical" {
        bail!("Core 未 ready 或不是 canonical Store；未提交 Run");
    }
    if ready["formal_run_bundle_supported"] != true {
        bail!("Core 缺少正式 Run bundle 接口；请更新并重启 Core，未提交 Run");
    }
    if ready["run_log_ready"] != true {
        bail!("Core 缺少完整私有运行日志能力或日志已阻断；未提交 Run");
    }
    Ok(())
}

async fn start(client: &ControlApiClient, purpose: FormalPurpose) -> Result<serde_json::Value> {
    require_formal_ready(client).await?;
    const AMBIGUOUS: &str =
        "RUN_SUBMISSION=unknown_after_post；可能已创建 Run，请先查询原 Store/Session，勿直接重试";
    // 仅一次 POST。Paper 同 Session 的复用属于 Rust scheduler；PositionPlan 没有客户端幂等性。
    let response = client
        .request(Method::POST, client.endpoint(&["runs"]))
        .timeout(Duration::from_secs(60))
        .json(&serde_json::json!({ "purpose": purpose.wire() }))
        .send()
        .await
        .map_err(|_| anyhow::anyhow!(AMBIGUOUS))?;
    let status = response.status();
    if status.is_server_error() {
        bail!("{AMBIGUOUS} (HTTP {status})");
    }
    if !status.is_success() {
        // 不回显服务端正文，避免 token/provider 敏感文本被错误响应带回终端。
        bail!("RUN_SUBMISSION=rejected (HTTP {status})；请检查 Core 的交易时段、审批及错误日志");
    }
    let result: serde_json::Value = response
        .json()
        .await
        .map_err(|_| anyhow::anyhow!(AMBIGUOUS))?;
    let run_id = result["run_id"]
        .as_str()
        .filter(|id| !id.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!(AMBIGUOUS))?;
    Ok(serde_json::json!({
        "run_id": run_id,
        "purpose": purpose.wire(),
        "submission_status": "submitted_or_reused",
        "note": "这里只确认提交/复用；用 run replay/detail/retrospectives 检查研究、Decision、执行和 Outcome"
    }))
}

fn validate_export_target(target: &Path) -> Result<()> {
    if !target.is_absolute() {
        bail!("导出目标必须为新的绝对目录路径，避免客户端与 Core 工作目录不同");
    }
    match fs::symlink_metadata(target) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Ok(_) => bail!("导出目标已存在（含符号链接），不会覆盖"),
        Err(error) => Err(error).context("检查导出目标"),
    }
}

fn validate_export_manifest(manifest: &serde_json::Value, run_id: &str) -> Result<()> {
    let integrity = &manifest["integrity"];
    if !matches!(integrity["status"].as_str(), Some("complete" | "partial"))
        || manifest["run_id"].as_str() != Some(run_id)
        || manifest["missing"]
            .as_array()
            .is_none_or(|rows| !rows.is_empty())
        || [
            "unknown_after_crash_calls",
            "corruption_or_missing_blobs",
            "dropped_records",
        ]
        .iter()
        .any(|key| integrity[*key].as_u64().is_none_or(|count| count > 0))
    {
        // 授权省略可为 partial；未知/损坏/丢记录不能当成零。原 Run 与导出完整性独立。
        bail!("导出状态异常；保留 Core 生成的目录，不能据此撤销或重跑原 Run");
    }
    Ok(())
}

pub(crate) async fn execute(command: &RunCommand, config_path: &Path) -> Result<()> {
    // TOML 的错误链可能附带原始配置行（包括凭据）；正式客户端只给出安全错误，不回显正文。
    let config = read_config_file(config_path).map_err(|_| {
        anyhow::anyhow!("无法读取/解析正式配置；请检查路径与 TOML，配置正文不会输出")
    })?;
    let purpose = match command {
        RunCommand::Start { purpose, .. } => Some(*purpose),
        _ => None,
    };
    let store_override = std::env::var_os("AKZIO_STORE_ROOT").map(PathBuf::from);
    validate_settings(&config.daemon, purpose, store_override.as_deref())?;
    if let RunCommand::ExportBundle { target, .. } = command {
        validate_export_target(target)?;
    }
    let client = match command {
        RunCommand::Start {
            purpose,
            auto_serve: true,
        } => {
            ensure_formal_core(
                &config,
                *purpose,
                || spawn_formal_core(config_path, &config.daemon.store_root),
                Duration::from_secs(300),
            )
            .await?
        }
        _ => ControlApiClient::from_config(&config)?,
    };
    match command {
        RunCommand::Start { purpose, .. } => print_json(&start(&client, *purpose).await?),
        RunCommand::Detail { run_id } => {
            require_formal_ready(&client).await?;
            let detail: serde_json::Value = client
                .json(
                    client
                        .request(
                            Method::GET,
                            client.endpoint(&["v1", "observer", "runs", run_id]),
                        )
                        .timeout(Duration::from_secs(30)),
                )
                .await?;
            if detail["workflow"]["run"]["run_id"].as_str() != Some(run_id.as_str()) {
                bail!("Observer 返回的 Run ID 不匹配；拒绝展示为目标 Run");
            }
            // 完整保留有界投影中的研究分配、正式目标、回执和缺失信息，不把缺失填成零。
            print_json(&detail)
        }
        RunCommand::ExportBundle { run_id, target } => {
            require_formal_ready(&client).await?;
            let manifest: serde_json::Value = client
                .json(
                    client
                        .request(
                            Method::POST,
                            client.endpoint(&["control", "store", "export-run-bundle"]),
                        )
                        .timeout(Duration::from_secs(300))
                        .json(&serde_json::json!({ "run_id": run_id, "target": target })),
                )
                .await
                .context("导出未确认；不自动重试，请检查目标目录。原 Run 不受影响")?;
            print_json(&manifest)?;
            validate_export_manifest(&manifest, run_id)
        }
        _ => bail!("不是正式 Run 客户端命令"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener};
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    };

    // 本地 HTTP 协议替身，不构造 Rust daemon/Store、模型或 Alpaca 客户端。
    // None 表示服务端收到请求后断连，用来验证提交不确定时不自动重试。
    struct Core {
        address: SocketAddr,
        requests: Arc<Mutex<Vec<String>>>,
        stop: Arc<AtomicBool>,
        thread: Option<std::thread::JoinHandle<()>>,
    }

    impl Core {
        fn new(replies: Vec<Option<(u16, &'static str)>>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            Self::on(listener, replies)
        }

        fn on(listener: TcpListener, replies: Vec<Option<(u16, &'static str)>>) -> Self {
            let address = listener.local_addr().unwrap();
            listener.set_nonblocking(true).unwrap();
            let requests = Arc::new(Mutex::new(Vec::new()));
            let recorded = requests.clone();
            let stop = Arc::new(AtomicBool::new(false));
            let stopped = stop.clone();
            let thread = std::thread::spawn(move || {
                let mut replies = replies.into_iter();
                while !stopped.load(Ordering::SeqCst) {
                    let (mut stream, _) = match listener.accept() {
                        Ok(accepted) => accepted,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(2));
                            continue;
                        }
                        Err(error) => panic!("{error}"),
                    };
                    // macOS 的 accepted socket 可继承非阻塞标志；测试按完整请求读完再应答。
                    stream.set_nonblocking(false).unwrap();
                    stream
                        .set_read_timeout(Some(Duration::from_secs(2)))
                        .unwrap();
                    let mut bytes = Vec::new();
                    let mut chunk = [0_u8; 4096];
                    loop {
                        let count = stream.read(&mut chunk).unwrap();
                        if count == 0 {
                            break;
                        }
                        bytes.extend_from_slice(&chunk[..count]);
                        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                            let headers = String::from_utf8_lossy(&bytes[..end]).to_lowercase();
                            let length = headers
                                .lines()
                                .find_map(|line| {
                                    line.strip_prefix("content-length:")
                                        .map(|v| v.trim().parse::<usize>().unwrap())
                                })
                                .unwrap_or(0);
                            if bytes.len() >= end + 4 + length {
                                break;
                            }
                        }
                    }
                    // 端口占用探测只建立并关闭 TCP，不发送 HTTP，也不消费预设响应。
                    if bytes.is_empty() {
                        continue;
                    }
                    recorded
                        .lock()
                        .unwrap()
                        .push(String::from_utf8(bytes).unwrap());
                    if let Some((status, body)) = replies.next().unwrap_or(Some((500, "{}"))) {
                        // Location 指向同一替身也不允许跟随；否则会记录第三次请求。
                        write!(stream, "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nLocation: http://{address}/capture\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
                    }
                }
            });
            Self {
                address,
                requests,
                stop,
                thread: Some(thread),
            }
        }

        fn client(&self) -> ControlApiClient {
            ControlApiClient::new(self.address, "private-test-token".into()).unwrap()
        }
    }

    impl Drop for Core {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            let joined = self.thread.take().unwrap().join();
            if !std::thread::panicking() {
                joined.unwrap();
            }
        }
    }

    const READY: &str = r#"{"status":"ok","store_scope":"canonical","formal_run_bundle_supported":true,"run_log_ready":true}"#;

    fn test_config(address: SocketAddr) -> (PathBuf, Config) {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/formal-client-tests")
            .join(uuid::Uuid::new_v4().to_string());
        fs::create_dir_all(root.join("store")).unwrap();
        let config_path = root.join("config.toml");
        fs::write(root.join("store/.daemon-token"), "private-test-token").unwrap();
        fs::write(
            &config_path,
            format!(
                "[daemon]\nstore_root = \"{}\"\nhttp_addr = \"{address}\"\nmanual_paper = true\n\
                 [execution]\nassets = [\"TQQQ\", \"QQQ\", \"SOXX\", \"SOXL\"]\n",
                root.join("store").display(),
            ),
        )
        .unwrap();
        let config = read_config_file(&config_path).unwrap();
        (root, config)
    }

    #[tokio::test]
    async fn missing_core_launches_once_waits_for_authenticated_ready_then_submits_once() {
        let port = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = port.local_addr().unwrap();
        drop(port);
        let (root, config) = test_config(address);
        // 首次启动时 Store/token 尚不存在；由 Core 启动过程创建后才允许认证 /ready。
        fs::remove_file(config.daemon.store_root.join(".daemon-token")).unwrap();
        let mut server = None;
        let mut launches = 0;
        let client = ensure_formal_core(
            &config,
            FormalPurpose::Paper,
            || {
                launches += 1;
                fs::write(
                    config.daemon.store_root.join(".daemon-token"),
                    "private-test-token",
                )
                .unwrap();
                server = Some(Core::on(
                    TcpListener::bind(address).unwrap(),
                    vec![
                        Some((200, READY)),
                        Some((200, READY)),
                        Some((200, r#"{"run_id":"run-1"}"#)),
                    ],
                ));
                Ok(|| Ok(()))
            },
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        assert_eq!(launches, 1);
        assert_eq!(
            start(&client, FormalPurpose::Paper).await.unwrap()["run_id"],
            "run-1"
        );
        let requests = server.as_ref().unwrap().requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests[0].starts_with("GET /ready "));
        assert!(requests[1].starts_with("GET /ready "));
        assert!(requests[2].starts_with("POST /runs "));
        drop(requests);
        drop(server);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn occupied_port_reuses_matching_core_and_never_launches_another() {
        let server = Core::new(vec![
            Some((200, READY)),
            Some((200, READY)),
            Some((200, r#"{"run_id":"run-1"}"#)),
        ]);
        let (root, config) = test_config(server.address);
        let mut launches = 0;
        let client = ensure_formal_core(
            &config,
            FormalPurpose::PositionPlan,
            || {
                launches += 1;
                Ok(|| Ok(()))
            },
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        assert_eq!(launches, 0);
        assert_eq!(
            start(&client, FormalPurpose::PositionPlan).await.unwrap()["run_id"],
            "run-1"
        );
        assert_eq!(server.requests.lock().unwrap().len(), 3);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn occupied_port_with_wrong_token_does_not_launch_or_submit() {
        let server = Core::new(vec![Some((401, r#"{"error":"private-test-token"}"#))]);
        let (root, config) = test_config(server.address);
        let mut launches = 0;
        let error = ensure_formal_core(
            &config,
            FormalPurpose::Paper,
            || {
                launches += 1;
                Ok(|| Ok(()))
            },
            Duration::from_secs(2),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(launches, 0);
        assert!(!format!("{error:#}").contains("private-test-token"));
        assert_eq!(server.requests.lock().unwrap().len(), 1);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn starting_position_plan_cannot_enable_an_auto_paper_daemon() {
        let port = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = port.local_addr().unwrap();
        drop(port);
        let (root, mut config) = test_config(address);
        config.daemon.auto_paper = Some(true);
        let mut launches = 0;
        let error = ensure_formal_core(
            &config,
            FormalPurpose::PositionPlan,
            || {
                launches += 1;
                Ok(|| Ok(()))
            },
            Duration::from_secs(2),
        )
        .await
        .err()
        .unwrap();
        assert!(format!("{error:#}").contains("auto_paper=true"));
        assert_eq!(launches, 0);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn failed_core_startup_never_submits_a_run() {
        let port = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = port.local_addr().unwrap();
        drop(port);
        let (root, config) = test_config(address);
        let mut launches = 0;
        let error = ensure_formal_core(
            &config,
            FormalPurpose::Paper,
            || {
                launches += 1;
                Ok(|| bail!("Core 在 ready 前退出；检查 CORE_LOG"))
            },
            Duration::from_secs(2),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(launches, 1);
        assert!(error.to_string().contains("CORE_LOG"));
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn formal_submission_uses_one_post_and_does_not_claim_completion() {
        for purpose in [FormalPurpose::PositionPlan, FormalPurpose::Paper] {
            let core = Core::new(vec![
                Some((200, READY)),
                Some((200, r#"{"run_id":"run-1"}"#)),
            ]);
            let response = start(&core.client(), purpose).await.unwrap();
            assert_eq!(response["run_id"], "run-1");
            assert_eq!(response["purpose"], purpose.wire());
            assert_eq!(response["submission_status"], "submitted_or_reused");
            assert!(response.get("completed").is_none());
            let requests = core.requests.lock().unwrap();
            assert_eq!(requests.len(), 2);
            assert!(requests[0].starts_with("GET /ready "));
            assert!(requests[1].starts_with("POST /runs "));
            assert!(requests[1].ends_with(&format!(r#"{{"purpose":"{}"}}"#, purpose.wire())));
        }
    }

    #[tokio::test]
    async fn isolated_old_unready_unauthorized_and_redirected_core_never_get_a_post() {
        for (status, body) in [
            (
                200,
                r#"{"status":"ok","store_scope":"isolated_debug","formal_run_bundle_supported":true}"#,
            ),
            (200, r#"{"status":"ok","store_scope":"canonical"}"#),
            (
                200,
                r#"{"status":"starting","store_scope":"canonical","formal_run_bundle_supported":true}"#,
            ),
            (401, r#"{"error":"private-test-token"}"#),
            (307, "{}"),
        ] {
            let core = Core::new(vec![Some((status, body))]);
            let error = start(&core.client(), FormalPurpose::Paper)
                .await
                .unwrap_err();
            assert!(!format!("{error:#}").contains("private-test-token"));
            assert_eq!(core.requests.lock().unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn ambiguous_submission_is_never_retried_or_relabelled_not_started() {
        for reply in [
            None,
            Some((500, r#"{"error":"private-test-token"}"#)),
            Some((200, r#"{"run_id":"#)),
            Some((200, "{}")),
            Some((200, r#"{"run_id":" "}"#)),
        ] {
            let core = Core::new(vec![Some((200, READY)), reply]);
            let error = start(&core.client(), FormalPurpose::PositionPlan)
                .await
                .unwrap_err();
            let message = format!("{error:#}");
            assert!(message.contains("unknown_after_post"), "{message}");
            assert!(!message.contains("private-test-token"));
            assert_eq!(core.requests.lock().unwrap().len(), 2);
        }
    }

    #[tokio::test]
    async fn rejected_paper_or_redirect_is_not_a_run_and_does_not_leak_body() {
        for status in [400, 401, 409, 307] {
            let core = Core::new(vec![
                Some((200, READY)),
                Some((status, r#"{"error":"private-test-token"}"#)),
            ]);
            let error = start(&core.client(), FormalPurpose::Paper)
                .await
                .unwrap_err();
            assert!(error.to_string().contains("RUN_SUBMISSION=rejected"));
            assert!(!format!("{error:#}").contains("private-test-token"));
            assert_eq!(core.requests.lock().unwrap().len(), 2);
        }
    }

    #[test]
    fn local_settings_preserve_formal_identity_and_paper_opt_in() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut config = read_config_file(&root.join("../../config/akzio.toml")).unwrap();
        assert!(validate_settings(&config.daemon, None, None).is_err());
        config.daemon.store_root = root.to_path_buf();
        assert!(validate_settings(&config.daemon, Some(FormalPurpose::PositionPlan), None).is_ok());
        assert!(validate_settings(&config.daemon, Some(FormalPurpose::Paper), None).is_err());
        config.daemon.manual_paper = true;
        assert!(validate_settings(&config.daemon, Some(FormalPurpose::Paper), Some(root)).is_ok());
        assert!(validate_settings(&config.daemon, None, Some(&root.join("src"))).is_err());
        config.daemon.debug_control = true;
        assert!(validate_settings(&config.daemon, None, None).is_err());
    }

    #[test]
    fn formal_cli_requires_explicit_supported_purpose_and_preserves_export_path() {
        for args in [
            vec!["akzio", "run", "start"],
            vec!["akzio", "run", "start", "--purpose", "shadow"],
            vec![
                "akzio",
                "run",
                "start",
                "--purpose",
                "paper",
                "--research-only",
            ],
            vec![
                "akzio",
                "run",
                "start",
                "--purpose",
                "paper",
                "--faker-online",
            ],
        ] {
            assert!(Cli::try_parse_from(args).is_err());
        }
        for purpose in ["paper", "position-plan"] {
            assert!(Cli::try_parse_from(["akzio", "run", "start", "--purpose", purpose]).is_ok());
            assert!(Cli::try_parse_from([
                "akzio",
                "run",
                "start",
                "--purpose",
                purpose,
                "--auto-serve"
            ])
            .is_ok());
        }
        assert!(validate_export_target(Path::new("relative")).is_err());
        assert!(validate_export_target(Path::new(env!("CARGO_MANIFEST_DIR"))).is_err());
    }

    #[test]
    fn client_rejects_non_loopback_and_invalid_tokens_before_http() {
        assert!(ControlApiClient::new("192.0.2.1:80".parse().unwrap(), "token".into()).is_err());
        for token in ["", "token\n", "token\r", " "] {
            assert!(ControlApiClient::new("127.0.0.1:1".parse().unwrap(), token.into()).is_err());
        }
    }

    #[test]
    fn export_allows_authorized_partial_but_not_missing_corrupt_or_unknown() {
        let valid = serde_json::json!({
            "run_id": "run-1", "missing": [],
            "integrity": {"status":"partial", "uncaptured_payloads":1,
                "unknown_after_crash_calls":0, "corruption_or_missing_blobs":0, "dropped_records":0}
        });
        assert!(validate_export_manifest(&valid, "run-1").is_ok());
        assert!(validate_export_manifest(&valid, "other-run").is_err());
        for key in [
            "unknown_after_crash_calls",
            "corruption_or_missing_blobs",
            "dropped_records",
        ] {
            let mut bad = valid.clone();
            bad["integrity"][key] = serde_json::json!(1);
            assert!(validate_export_manifest(&bad, "run-1").is_err());
            bad["integrity"].as_object_mut().unwrap().remove(key);
            assert!(validate_export_manifest(&bad, "run-1").is_err());
        }
        let mut missing = valid;
        missing["missing"] = serde_json::json!([{"reason":"missing_blob"}]);
        assert!(validate_export_manifest(&missing, "run-1").is_err());
    }

    #[test]
    fn spawned_core_logs_follow_the_formal_store_home_and_reject_symlinks() {
        let home = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/formal-client-tests")
            .join(uuid::Uuid::new_v4().to_string());
        fs::create_dir_all(home.join("store")).unwrap();
        let log_root = prepare_formal_log_root(&home.join("store")).unwrap();
        assert_eq!(log_root, home.join("logs").canonicalize().unwrap());
        assert!(!log_root.starts_with(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.akzio")));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&log_root).unwrap().permissions().mode() & 0o777,
                0o700
            );
            fs::remove_dir(&log_root).unwrap();
            std::os::unix::fs::symlink(home.join("store"), home.join("logs")).unwrap();
            assert!(prepare_formal_log_root(&home.join("store")).is_err());
        }
        fs::remove_dir_all(home).unwrap();
    }

    #[tokio::test]
    async fn config_errors_hide_source_lines_and_export_rejects_dangling_links() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/formal-client-tests")
            .join(uuid::Uuid::new_v4().to_string());
        fs::create_dir_all(&root).unwrap();
        let config = root.join("invalid.toml");
        fs::write(&config, "token = \"private-config-secret\" broken").unwrap();
        let error = execute(
            &RunCommand::Detail {
                run_id: "run-1".into(),
            },
            &config,
        )
        .await
        .unwrap_err();
        assert!(!format!("{error:#}").contains("private-config-secret"));
        assert!(validate_export_target(&root.join("new")).is_ok());
        #[cfg(unix)]
        {
            let link = root.join("dangling");
            std::os::unix::fs::symlink(root.join("missing"), &link).unwrap();
            assert!(validate_export_target(&link).is_err());
        }
        fs::remove_dir_all(root).unwrap();
    }
}
