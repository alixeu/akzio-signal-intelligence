fn handle_model_qualification(command: &ModelQualificationCommand) -> Result<()> {
    match command {
        ModelQualificationCommand::Assemble { input, output } => {
            assemble_model_qualification(input, output)
        }
    }
}

fn assemble_model_qualification(input: &Path, output: &Path) -> Result<()> {
    let payload = fs::read(input)
        .with_context(|| format!("read model qualification input {}", input.display()))?;
    let input: akzio_learning::OfflineModelQualificationInput = serde_json::from_slice(&payload)
        .with_context(|| format!("parse model qualification input {}", input.display()))?;
    let result = akzio_learning::run_offline_model_qualification(&input)
        .context("assemble offline model qualification report")?;
    let mut report = serde_json::to_vec_pretty(&result.report)
        .context("serialize model qualification report")?;
    report.push(b'\n');
    fs::write(output, report)
        .with_context(|| format!("write model qualification report {}", output.display()))?;
    if !result.report.complete() {
        bail!(
            "offline model qualification did not pass; audit report written to {}",
            output.display()
        );
    }
    Ok(())
}

