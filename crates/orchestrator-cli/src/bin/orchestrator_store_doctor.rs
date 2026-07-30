//! FileStore integrity inspection and derived-cache rebuilds.
//!
//! This CLI intentionally accepts only the FileStore root. It has no database
//! path, table, or generic file mutation surface.

use std::{collections::BTreeMap, path::PathBuf};

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use orchestrator_store::{
    inspect_store, rebuild_experience_stats, rebuild_experience_views, rebuild_index_catalog,
    rebuild_run_manifest, FileStore, FileStoreOptions, IndexKind, RunCompactionMode, RunLocation,
    RunManifestInit, RunStore,
};

#[derive(Parser)]
#[command(
    name = "orchestrator-store-doctor",
    about = "Inspect or rebuild FileStore metadata."
)]
struct Cli {
    /// The single FileStore root. No alternate run directory or database path exists.
    #[arg(long, default_value = "outputs/store")]
    store_root: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Check authoritative files, JSONL recovery, hashes, paths, indexes, and sessions.
    Inspect,
    /// Rebuild one run manifest from finalized Draft references.
    RebuildRunManifest {
        #[arg(long)]
        workflow_date: String,
        #[arg(long)]
        run_id: String,
        #[arg(long)]
        workflow_version: String,
        #[arg(long)]
        git_sha: String,
        #[arg(long)]
        config_hash: String,
        #[arg(long)]
        role_profile_registry_hash: String,
        #[arg(long)]
        created_at: String,
    },
    /// Rebuild the non-authoritative catalog for phase summaries or experience.
    RebuildIndexCatalog {
        #[arg(long, value_enum)]
        kind: CatalogKind,
        /// Required only for phase_summary catalogs.
        #[arg(long)]
        workflow_date: Option<String>,
        /// Required only for phase_summary catalogs.
        #[arg(long)]
        run_id: Option<String>,
        #[arg(long)]
        generated_at: String,
    },
    /// Rebuild dynamic experience occurrence counts and levels.
    RebuildExperienceStats {
        #[arg(long)]
        generated_at: String,
    },
    /// Rebuild every derived Experience View from append-only Event files.
    RebuildExperienceViews {
        #[arg(long)]
        generated_at: String,
    },
    /// Preview or apply completed-run archives and runtime projection cleanup.
    CompactRun {
        #[arg(long)]
        workflow_date: String,
        #[arg(long)]
        run_id: String,
        /// Apply the deletion. Without this flag the command is a dry run.
        #[arg(long)]
        apply: bool,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum CatalogKind {
    PhaseSummary,
    Experience,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let store = FileStore::open(&cli.store_root, FileStoreOptions::default())?;
    let output = match cli.command {
        Command::Inspect => serde_json::to_value(inspect_store(&store))?,
        Command::RebuildRunManifest {
            workflow_date,
            run_id,
            workflow_version,
            git_sha,
            config_hash,
            role_profile_registry_hash,
            created_at,
        } => {
            let location = RunLocation::new(workflow_date, run_id)?;
            serde_json::to_value(rebuild_run_manifest(
                &store,
                RunManifestInit {
                    location,
                    workflow_version,
                    prompt_versions: BTreeMap::new(),
                    git_sha,
                    config_hash,
                    role_profile_registry_hash,
                    created_at,
                },
            )?)?
        }
        Command::RebuildIndexCatalog {
            kind,
            workflow_date,
            run_id,
            generated_at,
        } => {
            let kind = match kind {
                CatalogKind::PhaseSummary => IndexKind::PhaseSummary,
                CatalogKind::Experience => IndexKind::Experience,
            };
            let run = match (workflow_date, run_id) {
                (Some(date), Some(id)) => Some(RunLocation::new(date, id)?),
                (None, None) => None,
                _ => anyhow::bail!(
                    "--workflow-date and --run-id must be supplied together for a phase_summary catalog"
                ),
            };
            if matches!(kind, IndexKind::PhaseSummary) && run.is_none() {
                anyhow::bail!("phase_summary catalog requires --workflow-date and --run-id")
            }
            if matches!(kind, IndexKind::Experience) && run.is_some() {
                anyhow::bail!("experience catalog must not select a run")
            }
            serde_json::to_value(rebuild_index_catalog(
                &store,
                kind,
                run.as_ref(),
                generated_at,
            )?)?
        }
        Command::RebuildExperienceStats { generated_at } => {
            serde_json::to_value(rebuild_experience_stats(&store, generated_at)?)?
        }
        Command::RebuildExperienceViews { generated_at } => {
            serde_json::to_value(rebuild_experience_views(&store, &generated_at)?)?
        }
        Command::CompactRun {
            workflow_date,
            run_id,
            apply,
        } => {
            let location = RunLocation::new(workflow_date, run_id)?;
            let run = RunStore::new(store.clone(), location);
            serde_json::to_value(run.compact_completed_run(if apply {
                RunCompactionMode::Apply
            } else {
                RunCompactionMode::DryRun
            })?)?
        }
    };
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}
