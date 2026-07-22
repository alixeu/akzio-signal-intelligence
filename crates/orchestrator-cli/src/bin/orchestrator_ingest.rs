use anyhow::Result;
use clap::{Parser, Subcommand};
use orchestrator_cli::{init_tracing, jin10, technical};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(name = "orchestrator-ingest", about = "Unified data ingestion CLI")]
struct Cli {
    /// Import technical source data into the run SQLite database. Jin10 is deferred until scored.
    #[arg(long, global = true)]
    db_path: Option<PathBuf>,
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
            let mut result = jin10::run(args).await?;
            if cli.db_path.is_some() {
                result["sqlite"] = json!({
                    "table": "jin10_items",
                    "rows": 0,
                    "persistence": "deferred_until_attention_scored"
                });
            }
            if pretty {
                println!("{}", serde_json::to_string_pretty(&result)?);
            } else {
                println!("{}", serde_json::to_string(&result)?);
            }
        }
        IngestCommand::TechnicalIndicators { args } => {
            let mut result = technical::run(args).await?;
            if let Some(db_path) = &cli.db_path {
                let mut conn = orchestrator_sql::connect(db_path)?;
                result["sqlite"] = import_technical_result(&mut conn, &result)?;
            }
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
    }
    Ok(())
}

fn import_technical_result(conn: &mut rusqlite::Connection, result: &Value) -> Result<Value> {
    let output_dir = result
        .get("output_dir")
        .and_then(Value::as_str)
        .map(Path::new)
        .ok_or_else(|| anyhow::anyhow!("technical ingest output_dir missing"))?;
    let symbols = result
        .get("symbols")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("technical ingest symbols missing"))?;
    let intervals = result
        .get("intervals")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("technical ingest intervals missing"))?;
    let mut series = 0usize;
    let mut rows = 0usize;
    for symbol in symbols.iter().filter_map(Value::as_str) {
        for interval in intervals.iter().filter_map(Value::as_str) {
            let path = orchestrator_core::technical_csv_path(output_dir, symbol, interval)
                .ok_or_else(|| anyhow::anyhow!("unsupported technical interval {interval}"))?;
            rows += orchestrator_sql::import_technical_csv(conn, symbol, interval, &path)?;
            series += 1;
        }
    }
    Ok(json!({"table": "technical_series", "series": series, "rows": rows}))
}
