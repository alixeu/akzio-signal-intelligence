use anyhow::Result;
use clap::Parser;
use orchestrator_cli::{init_tracing, report};

#[derive(Parser)]
#[command(name = "report-email")]
struct Cli {
    #[command(flatten)]
    args: report::ReportArgs,
}

fn main() -> Result<()> {
    init_tracing();
    let result = report::run(Cli::parse().args)?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
