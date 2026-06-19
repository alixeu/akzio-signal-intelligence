use anyhow::Result;
use clap::Parser;
use orchestrator_cli::{exec, init_tracing};

#[derive(Parser)]
#[command(
    name = "orchestrator-exec",
    about = "Run stock-analysis workers via Rig."
)]
struct Cli {
    #[command(flatten)]
    args: exec::ExecArgs,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let result = exec::run(cli.args).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
