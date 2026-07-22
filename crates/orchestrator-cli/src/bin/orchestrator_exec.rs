use anyhow::Result;
use clap::Parser;
use orchestrator_cli::{exec, init_tracing};

#[derive(Parser)]
#[command(
    name = "orchestrator-exec",
    about = "Run stock-analysis workers via the agent loop."
)]
struct Cli {
    #[command(flatten)]
    args: exec::ExecArgs,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let is_debug = cli.args.debug;
    let result = exec::run(cli.args).await?;
    if !is_debug {
        println!("{}", serde_json::to_string_pretty(&result)?);
    }
    Ok(())
}
