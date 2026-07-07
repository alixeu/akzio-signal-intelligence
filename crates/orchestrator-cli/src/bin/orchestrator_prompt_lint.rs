//! Prompt template lint CLI.
//!
//! Validates all prompt files under `prompts/` for placeholder completeness,
//! schema reference validity, common component existence, orphan placeholders,
//! file size, duplicate content, and anti-injection presence.
//!
//! Usage:
//!   cargo run -p orchestrator-cli --bin orchestrator-prompt-lint -- [options]
//!
//! Options:
//!   --prompts-dir <PATH>   Path to prompts directory (default: prompts)
//!   --format <FORMAT>      Output format: json or text (default: json)
//!   --strict               Exit with error code on warnings

use anyhow::{bail, Result};
use clap::Parser;
use std::path::PathBuf;

mod lint;

#[derive(Parser)]
#[command(
    name = "orchestrator-prompt-lint",
    about = "Lint prompt template files for placeholder and structural issues."
)]
struct Args {
    /// Path to prompts directory.
    #[arg(long, default_value = "prompts")]
    prompts_dir: PathBuf,

    /// Output format: json or text.
    #[arg(long, default_value = "json")]
    format: String,

    /// Exit with error code on warnings.
    #[arg(long)]
    strict: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let report = lint::run_all_checks(&args.prompts_dir)?;
    match args.format.as_str() {
        "json" => println!("{}", serde_json::to_string_pretty(&report)?),
        "text" => lint::print_text_report(&report),
        other => bail!("unknown format: {other}"),
    }
    let has_errors = report.issues.iter().any(|i| i.severity == "error");
    let has_warnings = report.issues.iter().any(|i| i.severity == "warning");
    if has_errors || (args.strict && has_warnings) {
        std::process::exit(1);
    }
    Ok(())
}
