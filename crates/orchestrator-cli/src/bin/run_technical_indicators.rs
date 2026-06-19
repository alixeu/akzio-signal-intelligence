use anyhow::Result;
use clap::Parser;
use orchestrator_cli::{init_tracing, technical};

#[derive(Parser)]
#[command(name = "run-technical-indicators")]
struct Cli {
    #[command(flatten)]
    args: technical::TechnicalArgs,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let result = technical::run(Cli::parse().args).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
