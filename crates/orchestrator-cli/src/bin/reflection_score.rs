use anyhow::Result;
use chrono::Utc;
use clap::Parser;
use orchestrator_cli::{
    init_tracing,
    reflection_score::{score_predictions, ScoreOptions},
};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "reflection-score",
    about = "Score expired reflection predictions against stored Close prices."
)]
struct Args {
    #[arg(long)]
    db_path: Option<PathBuf>,
    #[arg(long)]
    as_of: Option<String>,
    #[arg(long, default_value_t = 100)]
    limit: usize,
    #[arg(long, default_value = "1d")]
    interval: String,
}

fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    let db_path = args
        .db_path
        .or_else(|| std::env::var_os("ORCH_DB_PATH").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("outputs/orchestrator.sqlite"));
    let as_of = args
        .as_of
        .unwrap_or_else(|| Utc::now().date_naive().to_string());
    let conn = orchestrator_sql::connect(&db_path)?;
    let summary = score_predictions(
        &conn,
        &ScoreOptions {
            as_of,
            limit: args.limit,
            interval: args.interval,
        },
    )?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}
