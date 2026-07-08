use anyhow::Result;
use clap::Parser;
use orchestrator_cli::{
    init_tracing,
    memory_promote::{promote_memories, PromoteMode, PromoteOptions},
};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "memory-promote",
    about = "Promote candidate experiences into long-term memory."
)]
struct Args {
    #[arg(long)]
    db_path: Option<PathBuf>,
    #[arg(long, default_value = "auto")]
    mode: String,
    #[arg(long, default_value_t = 0.6)]
    min_quality: f64,
    #[arg(long, default_value_t = 5)]
    min_samples: usize,
    #[arg(long, default_value_t = 0.6)]
    min_confidence: f64,
}

fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    let db_path = args
        .db_path
        .or_else(|| std::env::var_os("ORCH_DB_PATH").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("outputs/orchestrator.sqlite"));
    let conn = orchestrator_sql::connect(&db_path)?;
    let summary = promote_memories(
        &conn,
        &PromoteOptions {
            mode: PromoteMode::parse(&args.mode),
            min_quality: args.min_quality.clamp(0.0, 1.0),
            min_samples: args.min_samples,
            min_confidence: args.min_confidence.clamp(0.0, 1.0),
        },
    )?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}
