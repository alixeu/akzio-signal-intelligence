#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if let Command::ObservatoryConfig { config, command } = &cli.command {
        return handle_observatory_config(config, command);
    }
    let config_path = cli.config.clone();
    let config = load_config(&config_path)?;

    match cli.command {
        Command::ObservatoryConfig { .. } => unreachable!("handled before config loading"),
        Command::Daemon { command } => {
            dispatch_control(Command::Daemon { command }, &config, &config_path).await
        }
        Command::Run { command }
            if matches!(command, RunCommand::FixtureDebug | RunCommand::PaperDryRun) =>
        {
            dispatch_fixture(command, config).await
        }
        Command::Run { command } => {
            dispatch_control(Command::Run { command }, &config, &config_path).await
        }
        Command::Test { command } => dispatch_diagnostics(command, config).await,
        Command::Store { command } => dispatch_store(command, &config, &config_path).await,
        Command::Canary { command } => {
            dispatch_control(Command::Canary { command }, &config, &config_path).await
        }
    }
}
