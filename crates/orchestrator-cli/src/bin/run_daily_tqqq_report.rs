use anyhow::Result;
use clap::Parser;
use orchestrator_cli::{exec, init_tracing, report};

/// Run the daily workflow and then build its report from the same FileStore
/// root. `--send-report` is an explicit external-side-effect authorization.
#[derive(Parser)]
#[command(name = "run-daily-tqqq-report")]
struct Cli {
    #[command(flatten)]
    exec_args: exec::ExecArgs,
    #[arg(long)]
    send_report: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let store_root = cli.exec_args.store_root.clone();
    let workflow = exec::run(cli.exec_args).await?;
    let report = report::run(report::ReportArgs {
        mode: if cli.send_report {
            report::ReportMode::BuildAndSend
        } else {
            report::ReportMode::Build
        },
        store_root,
    })?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "workflow": workflow,
            "report": report,
        }))?
    );
    Ok(())
}
