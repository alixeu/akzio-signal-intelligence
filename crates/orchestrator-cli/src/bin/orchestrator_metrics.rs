use anyhow::Result;
use clap::Parser;
use orchestrator_cli::init_tracing;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "orchestrator-metrics",
    about = "Query prompt-level metrics recorded by orchestrator runs."
)]
struct Args {
    #[arg(long, alias = "run")]
    run_id: Option<String>,
    #[arg(long)]
    role: Option<String>,
    #[arg(long)]
    db_path: Option<PathBuf>,
}

fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    let db_path = args
        .db_path
        .or_else(|| std::env::var_os("ORCH_DB_PATH").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("outputs/orchestrator.sqlite"));
    let conn = orchestrator_sql::connect(&db_path)?;

    let output = if args.run_id.is_some() || args.role.is_some() {
        orchestrator_sql::metrics::query_metrics_by_run_and_role(
            &conn,
            args.run_id.as_deref(),
            args.role.as_deref(),
        )?
    } else {
        orchestrator_sql::metrics::query_summary(&conn)?
    };
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}
