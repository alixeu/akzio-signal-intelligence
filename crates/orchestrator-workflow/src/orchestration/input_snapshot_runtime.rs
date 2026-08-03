//! FileStore binding of Phase 1 market-data inputs.
//!
//! Legacy roles keep using their existing readers. A role that has explicitly
//! migrated to FileStore receives a run-local immutable copy captured from the
//! stable files under `data/technical` and `data/jin10`.

use std::{fs, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use chrono::Utc;
use orchestrator_llm::tools::FileStoreInputSnapshot;
use orchestrator_store::{
    capture_run_inputs, read_input_snapshot_manifest, write_input_payload, FileStore,
    FileStoreOptions, InputSnapshotManifest, InputSource,
};
use serde_json::Value;

use super::{config::RuntimeConfig, lifecycle::run_location_from_state};

/// Bind the full, Rust-planned Phase 1 source set exactly once for a run.
/// A resumed run reuses the published hash manifest and run-local payloads;
/// later changes to the mutable source files cannot affect the run.
pub(crate) fn capture_phase1_file_store_inputs(
    state: &Value,
    config: &RuntimeConfig,
    sources: &[InputSource],
) -> Result<FileStoreInputSnapshot> {
    let store_root = required_string(state, "store_root")?;
    let current_date = required_string(state, "current_date")?;
    let run_id = required_string(state, "run_id")?;
    let location = run_location_from_state(state)?;
    let store = FileStore::open(
        &store_root,
        FileStoreOptions {
            atomic_fsync: config.store.atomic_fsync,
            stale_temp_age: Some(Duration::from_secs(config.store.stale_temp_age_sec)),
        },
    )?;

    let manifest_path = InputSnapshotManifest::relative_path(&location)?;
    if store.exists(&manifest_path)? {
        // `capture_run_inputs` compares the requested source set. Reading the
        // manifest first makes the no-reread recovery rule explicit here.
        let _ = read_input_snapshot_manifest(&store, &location)?;
    } else {
        for source in sources {
            let source_path = mutable_source_path(source)?;
            let payload = fs::read(&source_path).with_context(|| {
                format!(
                    "FileStore Phase 1 input capture failed reading {}",
                    source_path.display()
                )
            })?;
            write_input_payload(&store, source.clone(), &payload, Utc::now().to_rfc3339())?;
        }
    }
    capture_run_inputs(&store, &location, sources, Utc::now().to_rfc3339())?;

    Ok(FileStoreInputSnapshot {
        store_root: PathBuf::from(store_root),
        run_id,
        current_date,
        storage_namespace: state
            .get("storage_namespace")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    })
}

pub(crate) fn phase1_input_sources(
    current_date: &str,
    needs_technical: bool,
    needs_jin10: bool,
    tickers: &[String],
) -> Result<Vec<InputSource>> {
    let mut sources = Vec::new();
    if needs_technical {
        for ticker in tickers {
            for interval in ["daily", "3h", "20min"] {
                sources.push(InputSource::technical(ticker.clone(), interval)?);
            }
        }
    }
    if needs_jin10 {
        sources.push(InputSource::jin10(
            current_date.to_owned(),
            orchestrator_store::Jin10Format::Csv,
        )?);
    }
    Ok(sources)
}

fn mutable_source_path(source: &InputSource) -> Result<PathBuf> {
    match source {
        InputSource::Technical { ticker, interval } => orchestrator_core::technical_csv_path(
            &orchestrator_core::default_technical_csv_dir(),
            ticker,
            interval,
        )
        .with_context(|| format!("unsupported technical interval {interval:?}")),
        InputSource::Jin10 {
            workflow_date,
            format,
        } => match format {
            orchestrator_store::Jin10Format::Csv => Ok(orchestrator_core::jin10_csv_path(
                &orchestrator_core::default_jin10_csv_dir(),
                workflow_date,
            )),
            orchestrator_store::Jin10Format::Jsonl => anyhow::bail!(
                "FileStore Phase 1 Jin10 capture does not support JSONL without a typed parser"
            ),
        },
    }
}

fn required_string(state: &Value, field: &str) -> Result<String> {
    state
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .with_context(|| format!("state.{field} is required for FileStore input capture"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_plan_is_fixed_by_role_requirements_and_current_date() {
        let sources = phase1_input_sources(
            "2026-07-27",
            true,
            true,
            &["QQQ".to_owned(), "SOXX".to_owned()],
        )
        .unwrap();
        assert_eq!(sources.len(), 7);
        assert!(sources.iter().any(|source| matches!(
            source,
            InputSource::Jin10 { workflow_date, .. } if workflow_date == "2026-07-27"
        )));
    }
}
