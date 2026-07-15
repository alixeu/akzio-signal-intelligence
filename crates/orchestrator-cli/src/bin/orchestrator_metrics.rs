use anyhow::Result;
use clap::{Parser, Subcommand};
use orchestrator_cli::init_tracing;
use rusqlite::Connection;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "orchestrator-metrics",
    about = "Query token usage and cost metrics from orchestrator runs."
)]
struct Args {
    #[arg(long, alias = "run")]
    run_id: Option<String>,
    #[arg(long)]
    db_path: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Cost and token summary grouped by role
    ByRole,
    /// Cost and token summary grouped by phase
    ByPhase,
    /// Cache hit rate across all recorded calls
    CacheHitRate,
    /// Show calls where context_warning was triggered
    ContextWarnings,
    /// Run-level summary (requires --run-id)
    RunSummary,
}

fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    let db_path = args
        .db_path
        .or_else(|| std::env::var_os("ORCH_DB_PATH").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("outputs/orchestrator.sqlite"));
    let conn = orchestrator_sql::connect(&db_path)?;

    match args.command.unwrap_or(Cmd::RunSummary) {
        Cmd::ByRole => by_role(&conn, args.run_id.as_deref())?,
        Cmd::ByPhase => by_phase(&conn, args.run_id.as_deref())?,
        Cmd::CacheHitRate => cache_hit_rate(&conn, args.run_id.as_deref())?,
        Cmd::ContextWarnings => context_warnings(&conn, args.run_id.as_deref())?,
        Cmd::RunSummary => run_summary(&conn, args.run_id.as_deref())?,
    }
    Ok(())
}

fn run_filter(run_id: Option<&str>) -> &'static str {
    if run_id.is_some() {
        "WHERE run_id = ?1"
    } else {
        ""
    }
}

fn by_role(conn: &Connection, run_id: Option<&str>) -> Result<()> {
    let sql = format!(
        "SELECT role, COUNT(*) as calls, \
         SUM(input_tokens), SUM(output_tokens), SUM(cached_tokens), \
         SUM(reasoning_tokens), SUM(total_tokens), SUM(cost_usd) \
         FROM agent_events {} GROUP BY role ORDER BY SUM(cost_usd) DESC",
        run_filter(run_id)
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = if let Some(rid) = run_id {
        stmt.query(rusqlite::params![rid])?
    } else {
        stmt.query([])?
    };
    println!(
        "{:<30} {:>5} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "ROLE", "CALLS", "INPUT", "OUTPUT", "CACHED", "REASON", "TOTAL", "COST_USD"
    );
    while let Some(row) = rows.next()? {
        println!(
            "{:<30} {:>5} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10.4}",
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, f64>(7)?,
        );
    }
    Ok(())
}

fn by_phase(conn: &Connection, run_id: Option<&str>) -> Result<()> {
    let sql = format!(
        "SELECT phase, COUNT(*) as calls, \
         SUM(input_tokens), SUM(output_tokens), SUM(cached_tokens), \
         SUM(reasoning_tokens), SUM(total_tokens), SUM(cost_usd) \
         FROM agent_events {} GROUP BY phase ORDER BY phase",
        run_filter(run_id)
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = if let Some(rid) = run_id {
        stmt.query(rusqlite::params![rid])?
    } else {
        stmt.query([])?
    };
    println!(
        "{:<8} {:>5} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "PHASE", "CALLS", "INPUT", "OUTPUT", "CACHED", "REASON", "TOTAL", "COST_USD"
    );
    while let Some(row) = rows.next()? {
        let phase: Option<i64> = row.get(0)?;
        let label = phase.map_or_else(|| "-".to_string(), |p| p.to_string());
        println!(
            "{:<8} {:>5} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10.4}",
            label,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, f64>(7)?,
        );
    }
    Ok(())
}

fn cache_hit_rate(conn: &Connection, run_id: Option<&str>) -> Result<()> {
    let sql = format!(
        "SELECT COALESCE(SUM(cached_tokens),0), COALESCE(SUM(input_tokens),0), \
         CASE WHEN SUM(input_tokens) > 0 \
              THEN ROUND(SUM(cached_tokens) * 100.0 / SUM(input_tokens), 2) \
              ELSE 0 END \
         FROM agent_events {}",
        run_filter(run_id)
    );
    let mut stmt = conn.prepare(&sql)?;
    let (cached, input, rate): (i64, i64, f64) = if let Some(rid) = run_id {
        stmt.query_row(rusqlite::params![rid], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?
    } else {
        stmt.query_row([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
    };
    println!("Cached tokens:  {cached}");
    println!("Input tokens:   {input}");
    println!("Cache hit rate: {rate:.2}%");
    Ok(())
}

fn context_warnings(conn: &Connection, run_id: Option<&str>) -> Result<()> {
    let where_clause = if run_id.is_some() {
        "WHERE context_warning = 1 AND run_id = ?1"
    } else {
        "WHERE context_warning = 1"
    };
    let sql = format!(
        "SELECT turn_id, run_id, phase, role, model, input_tokens, total_tokens, cost_usd \
         FROM agent_events {where_clause} ORDER BY input_tokens DESC"
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = if let Some(rid) = run_id {
        stmt.query(rusqlite::params![rid])?
    } else {
        stmt.query([])?
    };
    println!(
        "{:<26} {:<12} {:>5} {:<25} {:<16} {:>10} {:>10} {:>10}",
        "TURN_ID", "RUN_ID", "PHASE", "ROLE", "MODEL", "INPUT", "TOTAL", "COST_USD"
    );
    let mut count = 0;
    while let Some(row) = rows.next()? {
        let phase: Option<i64> = row.get(2)?;
        let phase_label = phase.map_or_else(|| "-".to_string(), |p| p.to_string());
        println!(
            "{:<26} {:<12} {:>5} {:<25} {:<16} {:>10} {:>10} {:>10.4}",
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            phase_label,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, f64>(7)?,
        );
        count += 1;
    }
    if count == 0 {
        println!("No context warnings found.");
    }
    Ok(())
}

fn run_summary(conn: &Connection, run_id: Option<&str>) -> Result<()> {
    let sql = format!(
        "SELECT COUNT(*), COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0), \
         COALESCE(SUM(cached_tokens),0), COALESCE(SUM(reasoning_tokens),0), \
         COALESCE(SUM(total_tokens),0), COALESCE(SUM(cost_usd),0), \
         COALESCE(SUM(elapsed_ms),0), COALESCE(SUM(context_warning),0) \
         FROM agent_events {}",
        run_filter(run_id)
    );
    let mut stmt = conn.prepare(&sql)?;
    let (calls, input, output, cached, reasoning, total, cost, elapsed_ms, warnings): (
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        f64,
        i64,
        i64,
    ) = if let Some(rid) = run_id {
        stmt.query_row(rusqlite::params![rid], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
            ))
        })?
    } else {
        stmt.query_row([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
            ))
        })?
    };
    let cache_rate = if input > 0 {
        cached as f64 * 100.0 / input as f64
    } else {
        0.0
    };
    let non_cached = input - cached;
    let visible_output = output - reasoning;
    if let Some(rid) = run_id {
        println!("Run: {rid}");
    } else {
        println!("All runs");
    }
    println!("LLM calls:              {calls}");
    println!("Total input tokens:     {input}");
    println!("  Cached:               {cached}");
    println!("  Non-cached:           {non_cached}");
    println!("Total output tokens:    {output}");
    println!("  Reasoning:            {reasoning}");
    println!("  Visible:              {visible_output}");
    println!("Total tokens:           {total}");
    println!("Cache hit rate:         {cache_rate:.2}%");
    println!("Total cost (USD):       ${cost:.4}");
    println!("Total elapsed (ms):     {elapsed_ms}");
    println!("Context warnings:       {warnings}");
    Ok(())
}
