fn handle_model_qualification(command: &ModelQualificationCommand) -> Result<()> {
    // 资格子命令只分派离线报告组装；它不会探测模型、修改 Policy 或向 Paper 发送请求。
    match command {
        ModelQualificationCommand::Assemble { input, output } => {
            assemble_model_qualification(input, output)
        }
    }
}

fn assemble_model_qualification(input: &Path, output: &Path) -> Result<()> {
    // 读取并校验离线输入后先写完整审计报告，再依据 report.complete() 决定命令是否失败；
    // 因此失败也会留下可检查的报告，但不应被解释为已完成 Paper approval。
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
