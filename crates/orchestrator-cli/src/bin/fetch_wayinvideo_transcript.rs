use anyhow::Result;
use clap::Parser;
use orchestrator_cli::{init_tracing, wayinvideo};

#[derive(Parser)]
#[command(name = "fetch-wayinvideo-transcript")]
struct Cli {
    #[command(flatten)]
    args: wayinvideo::WayinVideoArgs,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let result = wayinvideo::run(Cli::parse().args).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
