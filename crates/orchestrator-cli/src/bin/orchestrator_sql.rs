use anyhow::Result;
use clap::Parser;
use orchestrator_cli::{init_tracing, sql_cli};

#[derive(Parser)]
#[command(
    name = "orchestrator-sql",
    about = "SQLite helpers for stock-analysis orchestrator."
)]
struct Cli {
    #[command(flatten)]
    args: sql_cli::SqlArgs,
}

fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let result = sql_cli::run(cli.args)?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
