use anyhow::Result;
use clap::Parser;
use orchestrator_cli::{exec, init_tracing, report};

/// Run the daily workflow and then build (or explicitly send) its report from
/// the same FileStore root.  `--skip-send` is intentionally explicit so test
/// and mock invocations never contact an SMTP service.
#[derive(Parser)]
#[command(name = "run-daily-tqqq-report")]
struct Cli {
    #[command(flatten)]
    exec_args: exec::ExecArgs,
    #[arg(long)]
    skip_send: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let store_root = cli.exec_args.store_root.clone();
    let workflow = exec::run(cli.exec_args).await?;
    let report = report::run(report::ReportArgs {
        mode: if cli.skip_send {
            report::ReportMode::Build
        } else {
            report::ReportMode::BuildAndSend
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
