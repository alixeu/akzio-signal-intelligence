use anyhow::Result;
use clap::Parser;
use orchestrator_cli::{init_tracing, youtube};

#[derive(Parser)]
#[command(name = "fetch-youtube-transcript")]
struct Cli {
    #[command(flatten)]
    args: youtube::YoutubeArgs,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let result = youtube::run(Cli::parse().args).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
