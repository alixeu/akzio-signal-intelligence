// 文件导读：这里是 CLI 的异步入口。先处理不需要 daemon 配置的离线/本地命令，再把
// 需要服务端权限的命令交给 dispatch；因此配置解析、模型探测、HTTP 认证和 Store
// 生命周期都在各自明确的边界内发生，CLI 的 `Ok(())` 不会被解释成业务流水线完成。
// Rust 机制：`#[tokio::main]` 宏生成 Tokio runtime；`async fn` 返回 Future，`?` 沿
// `anyhow::Result` 传播错误，`match` 对命令枚举做穷尽分派，避免遗漏某个权限分支。

#[tokio::main]
// CLI 参数被解析为拥有型枚举；早期本地分支直接返回，只有其余命令读取完整配置后才
// 生成对应控制 Future 并 await。`&cli.command` 是临时借用，后续 match 再按值消费命令。
async fn main() -> Result<()> {
    let cli = Cli::parse();
    // 正式运行客户端只读配置/token，不解析模型凭据或启动 Core。与 daemon serve 的
    // 完整配置加载分开，避免一次提交或导出隐式初始化 Store、probe 模型或激活 Policy。
    if let Command::Run { command } = &cli.command {
        if formal_run::handles(command) {
            return formal_run::execute(command, &cli.config).await;
        }
    }
    // 先处理不需要常规运行时配置的本地入口，避免离线质量报告、配置编辑、校准和
    // fixture 验证被模型凭据、Paper 环境或 daemon 启动前置条件阻断。
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
    // 其余命令才加载完整配置并应用执行/模型/Store 约束；成功加载仍只是进入 dispatch，
    // 实际 Run、Decision、Execution 或 Paper 写入由对应控制面继续决定。
    let config_path = cli.config.clone();
    let config = load_config(&config_path)?;

    match cli.command {
        Command::Workflow { command } => dispatch_control(Command::Workflow { command }, &config, &config_path).await,
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
    }
}

#[cfg(test)]
mod retired_debug_cli_tests {
    use super::*;

    #[test]
    fn no_debug_subcommand_or_config_switch_remains_executable() {
        assert!(Cli::try_parse_from(["akzio", "debug", "prepare"]).is_err());
        assert!(Cli::try_parse_from(["akzio", "debug", "verify-fixture"]).is_err());
        assert!(Cli::try_parse_from(["akzio", "evidence", "market-audit"]).is_err());
        assert!(Cli::try_parse_from(["akzio", "evidence", "preflight"]).is_err());
        for flag in ["true", "false"] {
            let source = format!(
                "[daemon]\ndebug_control={flag}\nstore_root=\"/tmp/test-store\"\n\
                 http_addr=\"127.0.0.1:7342\"\n[execution]\nassets=[\"TQQQ\",\"QQQ\",\"SOXX\",\"SOXL\"]\n"
            );
            let error = toml::from_str::<Config>(&source)
                .expect_err("retired flag must not reopen a Debug Core");
            assert!(error.to_string().contains("daemon.debug_control is retired"));
        }
    }
}
