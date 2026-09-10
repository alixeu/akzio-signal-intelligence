#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if let Command::ObservatoryConfig { config, command } = &cli.command {
        return handle_observatory_config(config, command);
    }
    if let Command::ModelQualification { command } = &cli.command {
        return handle_model_qualification(command);
    }
    if let Command::Calibration { command } = &cli.command {
        return handle_calibration(command, &cli.config);
    }
    let config_path = cli.config.clone();
    let config = load_config(&config_path)?;

    match cli.command {
        Command::Debug { command } => dispatch_debug(command, &config, &config_path).await,
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
        Command::Store { command } => dispatch_store(command, &config, &config_path).await,
        Command::Canary { command } => {
            dispatch_control(Command::Canary { command }, &config, &config_path).await
        }
        Command::ModelQualification { .. } => unreachable!("handled before config loading"),
        Command::Calibration { .. } => unreachable!("handled before config loading"),
        Command::Evidence { command } => run_evidence_command(&command, &config).await,
    }
}
