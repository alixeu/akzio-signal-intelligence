use anyhow::Result;
use clap::{Parser, Subcommand};
use orchestrator_cli::{init_tracing, jin10, technical};

#[derive(Parser)]
#[command(name = "orchestrator-ingest", about = "Unified data ingestion CLI")]
struct Cli {
    #[command(subcommand)]
    command: IngestCommand,
}

#[derive(Subcommand)]
enum IngestCommand {
    /// Fetch Jin10 flash news
    Jin10Flash {
        #[command(flatten)]
        args: jin10::Jin10Args,
    },
    /// Run technical indicators
    TechnicalIndicators {
        #[command(flatten)]
        args: technical::TechnicalArgs,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    match cli.command {
        IngestCommand::Jin10Flash { args } => {
            let pretty = args.pretty;
            let result = jin10::run(args).await?;
            if pretty {
                println!("{}", serde_json::to_string_pretty(&result)?);
            } else {
                println!("{}", serde_json::to_string(&result)?);
            }
        }
        IngestCommand::TechnicalIndicators { args } => {
            let result = technical::run(args).await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
    }
    Ok(())
}
