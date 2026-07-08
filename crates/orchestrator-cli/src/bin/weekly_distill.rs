use anyhow::Result;
use chrono::{Duration, Utc};
use clap::Parser;
use orchestrator_cli::{
    init_tracing,
    weekly_distill::{distill_weekly, DistillOptions},
};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "weekly-distill",
    about = "Distill scored reflection outcomes into candidate experiences."
)]
struct Args {
    #[arg(long)]
    db_path: Option<PathBuf>,
    #[arg(long)]
    since: Option<String>,
    #[arg(long)]
    until: Option<String>,
    #[arg(long, default_value_t = 3)]
    min_samples: usize,
}

fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    let db_path = args
        .db_path
        .or_else(|| std::env::var_os("ORCH_DB_PATH").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("outputs/orchestrator.sqlite"));
    let until = args
        .until
        .unwrap_or_else(|| Utc::now().date_naive().to_string());
    let since = args
        .since
        .unwrap_or_else(|| (Utc::now().date_naive() - Duration::days(7)).to_string());
    let conn = orchestrator_sql::connect(&db_path)?;
    let summary = distill_weekly(
        &conn,
        &DistillOptions {
            since,
            until,
            min_samples: args.min_samples,
        },
    )?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}
