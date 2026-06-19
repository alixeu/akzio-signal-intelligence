use anyhow::Result;
use clap::Parser;
use orchestrator_cli::{init_tracing, jin10};

#[derive(Parser)]
#[command(name = "fetch-jin10-flash")]
struct Cli {
    #[command(flatten)]
    args: jin10::Jin10Args,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let pretty = cli.args.pretty;
    let result = jin10::run(cli.args).await?;
    if pretty {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        println!("{}", serde_json::to_string(&result)?);
    }
    Ok(())
}
