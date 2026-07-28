use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::Local;
use clap::{Parser, ValueEnum};
use orchestrator_core::{load_config, project_path, RunPurpose};

#[derive(Debug, Parser)]
#[command(name = "orchestrator-memory")]
#[command(about = "Run deterministic Outcome materialization against FileStore Decisions")]
struct Args {
    /// Project configuration containing the strict evaluation/benchmark policy.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Canonical FileStore root. Defaults to the configured store root.
    #[arg(long)]
    store_root: Option<PathBuf>,
    /// Date for the evaluation-run receipt/report directory.
    #[arg(long)]
    evaluation_date: Option<String>,
    /// Explicit evaluation run identity; callers cannot choose outcome IDs.
    #[arg(long)]
    evaluation_run_id: String,
    #[arg(long, value_enum, default_value_t = PurposeArg::Paper)]
    purpose: PurposeArg,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum PurposeArg {
    Paper,
    Live,
    Replay,
    MigrationFixture,
}

impl From<PurposeArg> for RunPurpose {
    fn from(value: PurposeArg) -> Self {
        match value {
            PurposeArg::Paper => Self::Paper,
            PurposeArg::Live => Self::Live,
            PurposeArg::Replay => Self::Replay,
            PurposeArg::MigrationFixture => Self::MigrationFixture,
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let config_path = args
        .config
        .unwrap_or_else(|| project_path("config/config.yaml"));
    let config = load_config(Some(&config_path))
        .with_context(|| format!("failed to load {}", config_path.display()))?;
    let store_root = args.store_root.unwrap_or_else(|| {
        config
            .pointer("/orchestrator/store/root")
            .and_then(serde_json::Value::as_str)
            .map(project_path)
            .unwrap_or_else(|| project_path("outputs/store"))
    });
    let evaluation_date = args
        .evaluation_date
        .unwrap_or_else(|| Local::now().date_naive().to_string());
    let report = orchestrator_workflow::evaluation::materialize_from_config(
        &config,
        &store_root,
        &evaluation_date,
        &args.evaluation_run_id,
        args.purpose.into(),
    )?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
