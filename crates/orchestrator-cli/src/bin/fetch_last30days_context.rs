use anyhow::Result;
use clap::Parser;
use orchestrator_cli::{init_tracing, social};

#[derive(Parser)]
#[command(name = "fetch-last30days-context")]
struct Cli {
    #[command(flatten)]
    args: social::SocialArgs,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let result = social::run(Cli::parse().args).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
