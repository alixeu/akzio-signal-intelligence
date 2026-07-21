use anyhow::Result;
use clap::{Parser, Subcommand};
use orchestrator_cli::{init_tracing, jin10, social, technical, wayinvideo, youtube};

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
    /// Fetch YouTube video transcript
    YoutubeTranscript {
        #[command(flatten)]
        args: youtube::YoutubeArgs,
    },
    /// Fetch WayinVideo transcript
    WayinvideoTranscript {
        #[command(flatten)]
        args: wayinvideo::WayinVideoArgs,
    },
    /// Fetch last 30 days social context
    Last30daysContext {
        #[command(flatten)]
        args: social::SocialArgs,
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
        IngestCommand::YoutubeTranscript { args } => {
            let result = youtube::run(args).await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        IngestCommand::WayinvideoTranscript { args } => {
            let result = wayinvideo::run(args).await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        IngestCommand::Last30daysContext { args } => {
            let result = social::run(args).await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        IngestCommand::TechnicalIndicators { args } => {
            let result = technical::run(args).await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
    }
    Ok(())
}
