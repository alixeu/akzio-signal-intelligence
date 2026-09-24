// 文件导读：这里是 CLI 的异步入口。先处理不需要 daemon 配置的离线/本地命令，再把
// 需要服务端权限的命令交给 dispatch；因此配置解析、模型探测、HTTP 认证和 Store
// 生命周期都在各自明确的边界内发生，CLI 的 `Ok(())` 不会被解释成业务流水线完成。
// Rust 机制：`#[tokio::main]` 宏生成 Tokio runtime；`async fn` 返回 Future，`?` 沿
// `anyhow::Result` 传播错误，`match` 对命令枚举做穷尽分派，避免遗漏某个权限分支。

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    // 先处理不需要常规运行时配置的本地入口，避免离线质量报告、配置编辑、校准和
    // fixture 验证被模型凭据、Paper 环境或 daemon 启动前置条件阻断。
    if let Command::Debug { command: DebugCommand::VerifyResearchQuality { out, phase } } = &cli.command {
        // offline/report 不构造模型客户端；candidate 等阶段按专用 reviewer 路由（无则
        // 回退 critic）和 Synthesizer 路由创建客户端，报告失败才返回非零。
        let (model, synth_model) = if matches!(phase.as_str(), "offline" | "report") { (None,None) } else {
            let config = read_config_file(&cli.config)?;
            let base = config.model.context("missing model configuration")?;
            let resolved = base.routes.get("research.proposal_reviewer")
                .or_else(|| base.routes.get("research.critic"))
                .map(|route| base.for_route(route)).unwrap_or_else(||base.clone());
            let synth = base.routes.get("research.synthesizer").map(|route|base.for_route(route)).unwrap_or(base);
            (Some(akzio_model::ModelClient::from_config(&resolved)?),Some(akzio_model::ModelClient::from_config(&synth)?))
        };
        let report = akzio_research::quality::verify(out, phase, model, synth_model).await?;
        print_json(&report)?;
        if matches!(phase.as_str(), "candidate" | "report") && report["passed"] == false {
            bail!("research quality acceptance failed; see the report above");
        }
        return Ok(());
    }
    if let Command::ObservatoryConfig { config, command } = &cli.command {
        // Observatory 配置命令直接读写指定文件，不加载常规 daemon 配置，也不启动服务。
        return handle_observatory_config(config, command);
    }
    if let Command::ModelQualification { command } = &cli.command {
        // 模型资格报告是本地离线数据处理，输入输出由子命令自行管理。
        return handle_model_qualification(command);
    }
    if let Command::Calibration { command } = &cli.command {
        // 校准命令直接操作 Store Artifact 生命周期；不因普通 CLI 启动而自动激活 Policy。
        return handle_calibration(command, &cli.config);
    }
    if let Command::Evidence { command: EvidenceCommand::MarketAudit { output, option_feed } } = &cli.command {
        // Acquisition audit uses only a new isolated Store, never a scheduler, model or broker writer.
        let config = read_config_file(&cli.config)?;
        return market_audit(&config, output, *option_feed).await;
    }
    if matches!(cli.command, Command::Debug { command: DebugCommand::VerifyFixture }) {
        // 正式 fixture 拓扑使用自身的隔离 Store 和 fixture adapters，不能混用用户配置。
        return verify_fixture().await;
    }
    // 其余命令才加载完整配置并应用执行/模型/Store 约束；成功加载仍只是进入 dispatch，
    // 实际 Run、Decision、Execution 或 Paper 写入由对应控制面继续决定。
    let config_path = cli.config.clone();
    let config = load_config(&config_path)?;

    match cli.command {
        Command::Workflow { command } => dispatch_control(Command::Workflow { command }, &config, &config_path).await,
        Command::Debug { command } => dispatch_debug(command, &config, &config_path).await,
        Command::ObservatoryConfig { .. } => unreachable!("handled before config loading"),
        Command::Daemon { command } => {
            dispatch_control(Command::Daemon { command }, &config, &config_path).await
        }
        Command::Run { command } => {
            dispatch_control(Command::Run { command }, &config, &config_path).await
        }
        Command::Store { command } => dispatch_store(command, &config, &config_path).await,
        Command::Canary { command } => {
            dispatch_control(Command::Canary { command }, &config, &config_path).await
        }
        Command::ModelQualification { .. } => unreachable!("handled before config loading"),
        Command::Calibration { .. } => unreachable!("handled before config loading"),
        Command::Evidence { command } => run_evidence_command(&command, &config).await,
    }
}
