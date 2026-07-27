use anyhow::Result;
use clap::Parser;
use orchestrator_cli::{exec, init_tracing_with_debug};

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
    let cli = Cli::parse();
    let is_debug = cli.args.debug;
    init_tracing_with_debug(is_debug);
    let result = exec::run(cli.args).await?;
    if !is_debug {
        println!("{}", serde_json::to_string_pretty(&result)?);
    }
    Ok(())
}
