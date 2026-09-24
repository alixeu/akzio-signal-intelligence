//! Private, fail-closed local audit of formal runs. This is a derived recording,
//! never a source of workflow, order, or policy authority.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};

use akzio_domain::{ContentHash, RunId};
use akzio_model::{ModelAuditContext, ModelAuditSink};
use akzio_store::Store;
use chrono::{DateTime, Local, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;
use zip::{write::SimpleFileOptions, CompressionMethod, ZipArchive, ZipWriter};

#[derive(Debug, Error)]
pub enum RunLogError {
    #[error("RunLogUnavailable: {0}")]
    Unavailable(String),
}

pub type Result<T> = std::result::Result<T, RunLogError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveReceipt {
    pub run_id: String,
    pub phase: String,
    pub path: PathBuf,
    pub segment_id: String,
    pub event_count: u64,
    pub event_cursor: i64,
    pub stage_status: String,
    pub integrity_status: String,
    pub sha256: ContentHash,
}

#[derive(Clone, Serialize)]
struct PreviousSegment {
    segment_id: String,
    phase: String,
    path: PathBuf,
    sha256: ContentHash,
}

#[derive(Clone)]
struct Segment {
    id: String,
    phase: String,
    started_at: DateTime<Utc>,
    local_date: String,
    local_offset: String,
    source: PathBuf,
    core_session: String,
    count: u64,
    previous: Option<PreviousSegment>,
    start_event_cursor: i64,
    active_attempts: BTreeSet<String>,
    store_identity: Option<String>,
}

impl Segment {
    fn flow_after(&self) -> i64 {
        if self.phase == "startup" || (self.phase == "T0" && self.previous.is_none()) {
            0
        } else {
            // The first audit record follows the Store's task-start event.
            // Use this segment's own starting cursor rather than a previous
            // phase's archive watermark: an Outcome may already be running
            // when T0's ZIP is finally published.
            self.start_event_cursor.saturating_sub(1)
        }
    }
}

#[derive(Default)]
struct SourceInspection {
    count: u64,
    pending: Vec<ModelAuditContext>,
    unknown: u64,
    active_attempts: BTreeSet<String>,
    completed_calls: Vec<ModelAuditContext>,
    unreconciled: u64,
    unreconciled_call_ids: BTreeSet<String>,
}

/// The Store cursor is frozen by the caller before ZIP creation; the Store
/// projection is read a page at a time rather than buffering the whole Run.
enum FlowSource<'a> {
    Inline(&'a [Value]),
    Store {
        store: &'a Store,
        after: i64,
        ceiling: i64,
    },
}

#[derive(Default)]
struct State {
    segments: BTreeMap<(String, String), Segment>,
    unavailable: Option<String>,
    secrets: Vec<String>,
    uncertain_runs: BTreeSet<String>,
}

/// The interface is intentionally small: append an observed event, or seal a
/// stage. Synchronous writes prevent silently losing records on a dropped
/// broadcast receiver or an asynchronous writer task.
pub struct RunLogger {
    root: PathBuf,
    core_session: String,
    state: Mutex<State>,
    store: OnceLock<Store>,
    #[cfg(test)]
    fail_once: Mutex<Option<&'static str>>,
}

impl std::fmt::Debug for RunLogger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunLogger")
            .field("root", &self.root)
            .field("core_session", &self.core_session)
            .finish_non_exhaustive()
    }
}

impl RunLogger {
    pub fn open() -> Result<Arc<Self>> {
        // HOME may be overridden by a launcher; use the effective OS user's
        // home so an environment variable cannot redirect private Run logs.
        let root = effective_user_home()?.join(".akzio").join("log");
        ensure_private_directory(&root)?;
        Ok(Arc::new(Self {
            root,
            core_session: Uuid::new_v4().to_string(),
            state: Mutex::new(State::default()),
            store: OnceLock::new(),
            #[cfg(test)]
            fail_once: Mutex::new(None),
        }))
    }

    #[cfg(test)]
    pub(crate) fn for_test(root: PathBuf) -> Arc<Self> {
        ensure_private_directory(&root).unwrap();
        Arc::new(Self {
            root,
            core_session: Uuid::new_v4().to_string(),
            state: Mutex::new(State::default()),
            store: OnceLock::new(),
            fail_once: Mutex::new(None),
        })
    }

    pub fn attach_store(&self, store: Store) -> Result<()> {
        if let Some(existing) = self.store.get() {
            if existing.root() != store.root() {
                return Err(unavailable("Run logger cannot be rebound to another Store"));
            }
            return Ok(());
        }
        let uncertain = store
            .uncertain_run_log_ids()
            .map_err(|error| unavailable(error.to_string()))?;
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| unavailable("log lock poisoned"))?;
            state
                .uncertain_runs
                .extend(uncertain.into_iter().map(|run| run.0));
        }
        self.store
            .set(store)
            .map_err(|_| unavailable("Run logger Store attachment conflicted"))
    }

    fn fail_here(&self, _point: &'static str) -> Result<()> {
        #[cfg(test)]
        if self
            .fail_once
            .lock()
            .map_err(|_| unavailable("test fault lock poisoned"))?
            .take_if(|value| *value == _point)
            .is_some()
        {
            return Err(unavailable(format!("injected {_point} failure")));
        }
        Ok(())
    }

    pub fn core_session(&self) -> &str {
        &self.core_session
    }

    pub fn register_secret(&self, secret: &str) -> Result<()> {
        if secret.len() >= 8 {
            let mut state = self
                .state
                .lock()
                .map_err(|_| unavailable("log lock poisoned"))?;
            if !state.secrets.iter().any(|known| known == secret) {
                state.secrets.push(secret.to_owned());
            }
        }
        Ok(())
    }

    /// Recover only files created by this logger. Never reinterpret an
    /// incomplete provider call as a successful response or a fresh budget.
    /// A corrupt/ambiguous orphan blocks startup before any new model I/O.
    pub fn recover_orphans(&self, store: &Store) -> Result<usize> {
        self.attach_store(store.clone())?;
        let mut count = 0;
        for date_entry in fs::read_dir(&self.root).map_err(|e| unavailable(e.to_string()))? {
            let date_entry = date_entry.map_err(|e| unavailable(e.to_string()))?;
            let date = date_entry.file_name().to_string_lossy().into_owned();
            if date.len() != 8 || !date.bytes().all(|byte| byte.is_ascii_digit()) {
                continue;
            }
            let day_dir = date_entry.path();
            ensure_private_directory(&day_dir)?;
            let inflight = day_dir.join(".inflight");
            if !inflight.exists() {
                continue;
            }
            ensure_private_directory(&inflight)?;
            for entry in fs::read_dir(&inflight).map_err(|e| unavailable(e.to_string()))? {
                let entry = entry.map_err(|e| unavailable(e.to_string()))?;
                let source = entry.path();
                if source.extension().and_then(|value| value.to_str()) != Some("jsonl") {
                    return Err(unavailable("unexpected unfinished Run log file"));
                }
                let metadata =
                    fs::symlink_metadata(&source).map_err(|e| unavailable(e.to_string()))?;
                if !metadata.is_file() || metadata.file_type().is_symlink() {
                    return Err(unavailable("inflight Run log is not a regular file"));
                }
                let mut first = Vec::new();
                BufReader::new(open_regular_file(&source)?)
                    .read_until(b'\n', &mut first)
                    .map_err(|e| unavailable(e.to_string()))?;
                let entry: Value =
                    serde_json::from_slice(&first).map_err(|e| unavailable(e.to_string()))?;
                let string_field = |key: &str| -> Result<String> {
                    entry[key]
                        .as_str()
                        .filter(|value| !value.is_empty())
                        .map(str::to_owned)
                        .ok_or_else(|| unavailable(format!("unfinished Run log missing {key}")))
                };
                let run_id = string_field("run_id")?;
                let phase = string_field("phase")?;
                validate_log_identity(&run_id, &phase)?;
                let segment_id = source
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .and_then(|stem| stem.strip_prefix(&format!("{run_id}-{phase}-")))
                    .filter(|id| Uuid::parse_str(id).is_ok())
                    .ok_or_else(|| {
                        unavailable("unfinished Run log filename differs from its contents")
                    })?
                    .to_owned();
                if !matches!(phase.as_str(), "startup" | "T0" | "T1" | "T3" | "T5") {
                    return Err(unavailable("unsupported unfinished Run log stage"));
                }
                let started_at = entry["time_utc"]
                    .as_str()
                    .ok_or_else(|| unavailable("unfinished Run log has no start time"))?
                    .parse::<DateTime<Utc>>()
                    .map_err(|e| unavailable(e.to_string()))?;
                let source_store_identity = entry["store_identity"].as_str().map(str::to_owned);
                if source_store_identity
                    .as_deref()
                    .is_some_and(|identity| identity != store_log_identity(store))
                {
                    // Other formal Stores share this user's private log root.
                    // Leave their still-open segments untouched for their own
                    // Core rather than treating them as corrupt or sealing
                    // an actively written stage.
                    continue;
                }
                let inspection = inspect_source(&source)?;
                let mut segment = Segment {
                    id: segment_id,
                    phase: phase.clone(),
                    started_at,
                    local_date: date.clone(),
                    local_offset: string_field("local_offset")?,
                    source,
                    core_session: string_field("core_session")?,
                    count: inspection.count,
                    previous: self.previous_archive(&run_id, &phase)?,
                    start_event_cursor: entry["segment_start_cursor"].as_i64().unwrap_or(0),
                    active_attempts: inspection.active_attempts.clone(),
                    store_identity: source_store_identity,
                };
                let run = RunId(run_id.clone());
                let mut unaccounted = Vec::new();
                if phase != "startup" {
                    for scope in &inspection.completed_calls {
                        if !store
                            .run_log_model_call_accounted(
                                &run,
                                scope.task_id.as_deref(),
                                scope.attempt_id.as_deref(),
                                scope.call_id.as_deref(),
                            )
                            .map_err(|error| unavailable(error.to_string()))?
                        {
                            unaccounted.push(scope.clone());
                        }
                    }
                }
                let path = day_dir.join(if phase == "startup" {
                    format!("core-startup-{}.zip", segment.id)
                } else {
                    format!("run-{run_id}-{phase}-{}.zip", segment.id)
                });
                let receipt = if path.exists() {
                    self.verify_published(&run_id, &segment, &path)?
                } else {
                    let temporary = path.with_file_name(format!(
                        ".{}.zip.tmp",
                        path.file_stem()
                            .and_then(|value| value.to_str())
                            .ok_or_else(|| unavailable("invalid Run ZIP filename"))?
                    ));
                    if temporary.exists() {
                        // A complete, synced ZIP may have been left just before
                        // its no-overwrite publication. Incomplete temp files
                        // are preserved for inspection, never silently erased.
                        open_regular_file(&temporary)?
                            .sync_all()
                            .map_err(|error| unavailable(error.to_string()))?;
                        self.verify_published(&run_id, &segment, &temporary)?;
                        fs::hard_link(&temporary, &path)
                            .map_err(|error| unavailable(error.to_string()))?;
                        File::open(&day_dir)
                            .and_then(|directory| directory.sync_all())
                            .map_err(|error| unavailable(error.to_string()))?;
                        fs::remove_file(&temporary)
                            .map_err(|error| unavailable(error.to_string()))?;
                        self.verify_published(&run_id, &segment, &path)?
                    } else {
                        // No ZIP has been frozen yet. Explicitly mark every
                        // request lacking a terminal as unknown; never invent
                        // a response or authorize a duplicate model call.
                        let key = (run_id.clone(), phase.clone());
                        let mut recovery = State::default();
                        recovery.segments.insert(key.clone(), segment);
                        let interrupted_status = if !inspection.pending.is_empty() {
                            "interrupted_unknown_after_crash"
                        } else if !unaccounted.is_empty() {
                            "interrupted_unreconciled_after_crash"
                        } else {
                            "interrupted_after_crash"
                        };
                        for scope in inspection.pending {
                            self.record_locked(
                                &mut recovery,
                                &scope,
                                "llm.unknown_after_crash",
                                json!({"reason":"call result was not durably observed before Core restart"}),
                            )?;
                        }
                        for scope in &unaccounted {
                            if scope
                                .call_id
                                .as_ref()
                                .is_some_and(|id| inspection.unreconciled_call_ids.contains(id))
                            {
                                continue;
                            }
                            self.record_locked(
                                &mut recovery,
                                scope,
                                "llm.unreconciled_after_crash",
                                json!({"reason":"provider response was logged but no durable Store accounting could be proven"}),
                            )?;
                        }
                        segment = recovery
                            .segments
                            .remove(&key)
                            .ok_or_else(|| unavailable("recovered Run segment is missing"))?;
                        let (cursor, after) = if phase == "startup" {
                            (0, 0)
                        } else {
                            let cursor = store
                                .workflow_snapshot(&run)
                                .map_err(|error| unavailable(error.to_string()))?
                                .event_cursor;
                            (cursor, segment.flow_after())
                        };
                        let flow = if phase == "startup" {
                            FlowSource::Inline(&[])
                        } else {
                            FlowSource::Store {
                                store,
                                after,
                                ceiling: cursor,
                            }
                        };
                        self.seal_segment(&run_id, &segment, interrupted_status, cursor, flow)?
                    }
                };
                if phase != "startup" {
                    let prior = store
                        .run_log_status(&run)
                        .map_err(|e| unavailable(e.to_string()))?;
                    if !prior.iter().any(|entry| {
                        entry["status"] == "archived"
                            && entry["detail"]["segment_id"] == receipt.segment_id
                    }) {
                        store
                            .record_run_log_status(
                                &run,
                                &phase,
                                "archived",
                                &serde_json::to_value(&receipt)
                                    .map_err(|e| unavailable(e.to_string()))?,
                            )
                            .map_err(|e| unavailable(e.to_string()))?;
                    }
                    let block_reason =
                        if receipt.integrity_status == "partial_unknown_provider_result" {
                            Some("unknown_provider_result")
                        } else if receipt.integrity_status == "partial_unreconciled_provider_result"
                            || !unaccounted.is_empty()
                        {
                            Some("unreconciled_provider_result")
                        } else {
                            None
                        };
                    if let Some(reason) = block_reason {
                        if !prior.iter().any(|record| {
                            record["status"] == "blocked" && record["detail"]["reason"] == reason
                        }) {
                            store
                                .record_run_log_status(
                                    &run,
                                    &phase,
                                    "blocked",
                                    &json!({
                                        "reason": reason,
                                        "segment_id": &receipt.segment_id,
                                    }),
                                )
                                .map_err(|error| unavailable(error.to_string()))?;
                        }
                        self.state
                            .lock()
                            .map_err(|_| unavailable("log lock poisoned"))?
                            .uncertain_runs
                            .insert(run_id.clone());
                    }
                }
                if phase == "startup" {
                    self.confirm_startup(&receipt)?;
                } else {
                    self.confirm_after_store(store, &receipt)?;
                }
                count += 1;
            }
        }
        // The Run graph and its T0 log requirement are published atomically
        // in the Store. A crash before the first plaintext append therefore
        // cannot turn that Run into an unlogged worker after restart. Older
        // Runs without this explicit requirement are not backfilled.
        for run in store
            .unstarted_required_run_logs()
            .map_err(|error| unavailable(error.to_string()))?
        {
            self.record(
                &ModelAuditContext {
                    run_id: run.0,
                    phase: "T0".into(),
                    task_id: None,
                    attempt_id: None,
                    call_id: None,
                    retry_index: None,
                },
                "rust.run.recovered_before_first_task",
                json!({"status":"no_model_call_observed","source":"store.run_log.required"}),
            )?;
            count += 1;
        }
        Ok(count)
    }

    fn verify_published(
        &self,
        run: &str,
        segment: &Segment,
        path: &Path,
    ) -> Result<ArchiveReceipt> {
        let meta = fs::symlink_metadata(path).map_err(|e| unavailable(e.to_string()))?;
        if !meta.is_file() || meta.file_type().is_symlink() {
            return Err(unavailable("published Run ZIP is not a regular file"));
        }
        let mut archive =
            ZipArchive::new(open_regular_file(path)?).map_err(|e| unavailable(e.to_string()))?;
        let expected = [
            "events.jsonl",
            "llm_calls.jsonl",
            "rust_flow.jsonl",
            "summary.md",
            "manifest.json",
            "checksums.sha256",
        ];
        if archive.len() != expected.len() {
            return Err(unavailable("Run ZIP has missing or unexpected entries"));
        }
        let mut names = BTreeSet::new();
        for index in 0..archive.len() {
            let entry = archive
                .by_index(index)
                .map_err(|e| unavailable(e.to_string()))?;
            if !expected.contains(&entry.name()) || !names.insert(entry.name().to_owned()) {
                return Err(unavailable("Run ZIP has duplicate or unexpected entries"));
            }
        }
        let manifest: Value = {
            let mut file = archive
                .by_name("manifest.json")
                .map_err(|e| unavailable(e.to_string()))?;
            serde_json::from_reader(&mut file).map_err(|e| unavailable(e.to_string()))?
        };
        if manifest["run_id"] != run
            || manifest["phase"] != segment.phase
            || manifest["segment_id"] != segment.id
            || manifest["core_session"] != segment.core_session
            || manifest["local_date"] != segment.local_date
            || manifest["local_offset"] != segment.local_offset
            || manifest["event_count"] != segment.count
            || manifest["segment_start_cursor"] != segment.start_event_cursor
            || manifest["store_identity"]
                != serde_json::to_value(&segment.store_identity)
                    .map_err(|error| unavailable(error.to_string()))?
            || manifest["previous_segment"]
                != serde_json::to_value(&segment.previous)
                    .map_err(|error| unavailable(error.to_string()))?
        {
            return Err(unavailable(
                "published Run ZIP identity differs from source",
            ));
        }
        let checksums = {
            let mut checksums = String::new();
            archive
                .by_name("checksums.sha256")
                .map_err(|e| unavailable(e.to_string()))?
                .read_to_string(&mut checksums)
                .map_err(|e| unavailable(e.to_string()))?;
            parse_checksums(&checksums)?
        };
        if checksums.len() != expected.len() - 1 {
            return Err(unavailable("Run ZIP checksum list is incomplete"));
        }
        for name in expected.iter().filter(|name| **name != "checksums.sha256") {
            let expected_hash = checksums
                .get(*name)
                .ok_or_else(|| unavailable("Run ZIP checksum entry is missing"))?;
            let actual = hash_reader(
                archive
                    .by_name(name)
                    .map_err(|e| unavailable(e.to_string()))?,
            )?;
            if actual != *expected_hash {
                return Err(unavailable(format!("Run ZIP {name} checksum mismatch")));
            }
        }
        if checksums["events.jsonl"] != hash_reader(open_regular_file(&segment.source)?)?
            || manifest["integrity"]["events_sha256"] != checksums["events.jsonl"].as_str()
        {
            return Err(unavailable("published Run ZIP content differs from source"));
        }
        let inspection = inspect_source(&segment.source)?;
        let expected_integrity = if inspection.unknown > 0 {
            "partial_unknown_provider_result"
        } else if inspection.unreconciled > 0 {
            "partial_unreconciled_provider_result"
        } else if !inspection.active_attempts.is_empty() {
            "partial_unclosed_rust_task"
        } else {
            "complete"
        };
        if !inspection.pending.is_empty()
            || manifest["integrity"]["status"] != expected_integrity
            || manifest["integrity"]["unknown_provider_calls"] != inspection.unknown
            || manifest["integrity"]["unreconciled_provider_calls"] != inspection.unreconciled
            || manifest["integrity"]["unclosed_rust_tasks"]
                .as_u64()
                .unwrap_or(0)
                != inspection.active_attempts.len() as u64
        {
            return Err(unavailable(
                "Run ZIP model-call completeness differs from its source",
            ));
        }
        Ok(ArchiveReceipt {
            run_id: run.into(),
            phase: segment.phase.clone(),
            path: path.into(),
            segment_id: segment.id.clone(),
            event_count: segment.count,
            event_cursor: manifest["event_cursor"]
                .as_i64()
                .ok_or_else(|| unavailable("Run ZIP has no frozen event cursor"))?,
            stage_status: manifest["status"]
                .as_str()
                .ok_or_else(|| unavailable("Run ZIP has no stage status"))?
                .into(),
            integrity_status: manifest["integrity"]["status"]
                .as_str()
                .ok_or_else(|| unavailable("Run ZIP has no integrity status"))?
                .into(),
            sha256: hash_reader(open_regular_file(path)?)?,
        })
    }

    pub(crate) fn pending(&self) -> Result<Vec<(RunId, String)>> {
        let state = self
            .state
            .lock()
            .map_err(|_| unavailable("log lock poisoned"))?;
        Ok(state
            .segments
            .keys()
            .filter(|(run, _)| !run.starts_with("core-startup-"))
            .map(|(run, phase)| (RunId(run.clone()), phase.clone()))
            .collect())
    }

    pub(crate) fn segment_started_at(&self, run_id: &RunId, phase: &str) -> Result<DateTime<Utc>> {
        self.state
            .lock()
            .map_err(|_| unavailable("log lock poisoned"))?
            .segments
            .get(&(run_id.0.clone(), phase.to_owned()))
            .map(|segment| segment.started_at)
            .ok_or_else(|| unavailable("Run log segment disappeared before stage inspection"))
    }

    pub(crate) fn flow_after(&self, run_id: &RunId, phase: &str) -> Result<i64> {
        self.state
            .lock()
            .map_err(|_| unavailable("log lock poisoned"))?
            .segments
            .get(&(run_id.0.clone(), phase.to_owned()))
            .map(Segment::flow_after)
            .ok_or_else(|| unavailable("Run log segment disappeared before archive"))
    }

    pub(crate) fn has_active_task(&self, run_id: &RunId, phase: &str) -> Result<bool> {
        Ok(!self
            .state
            .lock()
            .map_err(|_| unavailable("log lock poisoned"))?
            .segments
            .get(&(run_id.0.clone(), phase.to_owned()))
            .ok_or_else(|| unavailable("Run log segment disappeared before task check"))?
            .active_attempts
            .is_empty())
    }

    fn previous_archive(&self, run_id: &str, phase: &str) -> Result<Option<PreviousSegment>> {
        if phase == "startup" {
            return Ok(None);
        }
        let Some(store) = self.store.get() else {
            return Ok(None);
        };
        let statuses = store
            .run_log_status(&RunId(run_id.to_owned()))
            .map_err(|error| unavailable(error.to_string()))?;
        statuses
            .into_iter()
            .rev()
            .find(|status| status["phase"] == phase && status["status"] == "archived")
            .map(|status| {
                let receipt: ArchiveReceipt = serde_json::from_value(status["detail"].clone())
                    .map_err(|error| unavailable(error.to_string()))?;
                Ok(PreviousSegment {
                    segment_id: receipt.segment_id,
                    phase: receipt.phase,
                    path: receipt.path,
                    sha256: receipt.sha256,
                })
            })
            .transpose()
    }

    pub fn assert_available(&self) -> Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| unavailable("log lock poisoned"))?;
        if let Some(reason) = &state.unavailable {
            return Err(RunLogError::Unavailable(reason.clone()));
        }
        ensure_private_directory(&self.root)
    }

    pub fn is_uncertain_run(&self, run_id: &str) -> bool {
        self.state
            .lock()
            .map_or(true, |state| state.uncertain_runs.contains(run_id))
    }

    /// The Store cancellation event is authoritative. A broker write guard
    /// checks it again immediately before every POST/PATCH, not only when a
    /// Reconcile task was first claimed.
    pub fn run_cancel_requested(&self, run_id: &str) -> Result<bool> {
        self.store
            .get()
            .ok_or_else(|| unavailable("Run logger has no attached Store"))?
            .run_cancel_requested(&RunId(run_id.to_owned()))
            .map_err(|error| unavailable(error.to_string()))
    }

    pub(crate) fn mark_unavailable(&self, reason: &str) -> Result<()> {
        self.state
            .lock()
            .map_err(|_| unavailable("log lock poisoned"))?
            .unavailable = Some(reason.to_owned());
        Ok(())
    }

    pub fn record(&self, context: &ModelAuditContext, kind: &str, payload: Value) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("log lock poisoned"))?;
        if let Some(reason) = &state.unavailable {
            return Err(RunLogError::Unavailable(reason.clone()));
        }
        if state.uncertain_runs.contains(&context.run_id) {
            return Err(unavailable(
                "unknown provider result requires explicit Run-level recovery review",
            ));
        }
        let key = (context.run_id.clone(), context.phase.clone());
        let new_segment = !state.segments.contains_key(&key);
        let result = self.record_locked(&mut state, context, kind, payload);
        let result = result.and_then(|()| {
            if !new_segment || context.phase == "startup" {
                return Ok(());
            }
            if let (Some(segment), Some(store)) = (state.segments.get(&key), self.store.get()) {
                store
                    .record_run_log_status(
                        &RunId(context.run_id.clone()),
                        &context.phase,
                        "opened",
                        &json!({
                            "segment_id": segment.id,
                            "started_at_utc": segment.started_at,
                            "local_date": segment.local_date,
                            "local_offset": segment.local_offset,
                            "core_session": segment.core_session
                        }),
                    )
                    .map_err(|error| unavailable(error.to_string()))?;
            }
            Ok(())
        });
        let result = result.and_then(|()| {
            if kind != "llm.unknown_after_cancel" {
                return Ok(());
            }
            if context.phase == "startup" {
                return Err(unavailable(
                    "startup model call ended without a known result",
                ));
            }
            if let Some(store) = self.store.get() {
                store
                    .record_run_log_status(
                        &RunId(context.run_id.clone()),
                        &context.phase,
                        "blocked",
                        &json!({"reason":"unknown_provider_result"}),
                    )
                    .map_err(|error| unavailable(error.to_string()))?;
            }
            state.uncertain_runs.insert(context.run_id.clone());
            Ok(())
        });
        if let Err(error) = &result {
            state.unavailable = Some(error.to_string());
            if context.phase != "startup" {
                if let Some(store) = self.store.get() {
                    let _ = store.record_run_log_status(
                        &RunId(context.run_id.clone()),
                        &context.phase,
                        "blocked",
                        &json!({"reason":error.to_string()}),
                    );
                }
            }
        }
        result
    }

    fn record_locked(
        &self,
        state: &mut State,
        context: &ModelAuditContext,
        kind: &str,
        mut payload: Value,
    ) -> Result<()> {
        validate_log_identity(&context.run_id, &context.phase)?;
        let key = (context.run_id.clone(), context.phase.clone());
        if !state.segments.contains_key(&key) {
            let previous = self.previous_archive(&context.run_id, &context.phase)?;
            let store_identity = self.store.get().map(store_log_identity);
            let start_event_cursor = if context.phase == "startup" {
                0
            } else if let Some(store) = self.store.get() {
                store
                    .workflow_snapshot(&RunId(context.run_id.clone()))
                    .map_err(|error| unavailable(error.to_string()))?
                    .event_cursor
            } else {
                0
            };
            let local_now = Local::now();
            let local_date = local_now.format("%Y%m%d").to_string();
            let dir = self.root.join(&local_date).join(".inflight");
            ensure_private_directory(&dir)?;
            let id = Uuid::new_v4().to_string();
            let source = dir.join(format!("{}-{}-{id}.jsonl", key.0, key.1));
            create_private_file(&source)?;
            state.segments.insert(
                key.clone(),
                Segment {
                    id,
                    phase: key.1.clone(),
                    started_at: local_now.with_timezone(&Utc),
                    local_date,
                    local_offset: local_now.format("%:z").to_string(),
                    source,
                    core_session: self.core_session.clone(),
                    count: 0,
                    previous,
                    start_event_cursor,
                    active_attempts: BTreeSet::new(),
                    store_identity,
                },
            );
        }
        redact_credentials(&mut payload, &state.secrets);
        let segment = state
            .segments
            .get_mut(&key)
            .ok_or_else(|| unavailable("missing newly created segment"))?;
        let mut line = serde_json::to_vec(&json!({
            "time_utc": Utc::now(),
            "core_session": segment.core_session,
            "local_offset": segment.local_offset,
            "segment_start_cursor": segment.start_event_cursor,
            "store_identity": segment.store_identity,
            "run_id": context.run_id,
            "phase": context.phase,
            "task_id": context.task_id,
            "attempt_id": context.attempt_id,
            "call_id": context.call_id,
            "retry_index": context.retry_index,
            "kind": kind,
            "payload": payload
        }))
        .map_err(|error| unavailable(error.to_string()))?;
        line.push(b'\n');
        ensure_private_directory(
            segment
                .source
                .parent()
                .ok_or_else(|| unavailable("segment has no parent"))?,
        )?;
        let metadata = fs::symlink_metadata(&segment.source)
            .map_err(|error| unavailable(error.to_string()))?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(unavailable("segment was replaced by a symlink or non-file"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o777 != 0o600 {
                return Err(unavailable(
                    "Run log plaintext is not private (expected 0600)",
                ));
            }
        }
        let mut options = OpenOptions::new();
        options.append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        let mut file = options
            .open(&segment.source)
            .map_err(|error| unavailable(error.to_string()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let opened = file
                .metadata()
                .map_err(|error| unavailable(error.to_string()))?;
            if opened.ino() != metadata.ino() || opened.dev() != metadata.dev() {
                return Err(unavailable("Run log plaintext changed during secure open"));
            }
        }
        file.write_all(&line)
            .map_err(|error| unavailable(error.to_string()))?;
        self.fail_here("record_sync")?;
        file.sync_data()
            .map_err(|error| unavailable(error.to_string()))?;
        segment.count += 1;
        if let Some(attempt_id) = &context.attempt_id {
            match kind {
                "rust.task.claimed" => {
                    segment.active_attempts.insert(attempt_id.clone());
                }
                "rust.task.result" => {
                    segment.active_attempts.remove(attempt_id);
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// A failed archive never deletes the source. A published ZIP is never
    /// overwritten, including after a crash between publication and receipt.
    #[cfg(test)]
    pub(crate) fn seal(
        &self,
        run_id: &RunId,
        phase: &str,
        status: &str,
        event_cursor: i64,
        rust_flow: &[Value],
    ) -> Result<Option<ArchiveReceipt>> {
        self.seal_with_flow(
            run_id,
            phase,
            status,
            event_cursor,
            FlowSource::Inline(rust_flow),
        )
    }

    pub(crate) fn seal_with_store(
        &self,
        run_id: &RunId,
        phase: &str,
        status: &str,
        store: &Store,
        after: i64,
        ceiling: i64,
    ) -> Result<Option<ArchiveReceipt>> {
        self.seal_with_flow(
            run_id,
            phase,
            status,
            ceiling,
            FlowSource::Store {
                store,
                after,
                ceiling,
            },
        )
    }

    fn seal_with_flow(
        &self,
        run_id: &RunId,
        phase: &str,
        status: &str,
        event_cursor: i64,
        flow: FlowSource<'_>,
    ) -> Result<Option<ArchiveReceipt>> {
        let key = (run_id.0.clone(), phase.to_owned());
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("log lock poisoned"))?;
        if let Some(reason) = &state.unavailable {
            return Err(RunLogError::Unavailable(reason.clone()));
        }
        let Some(segment) = state.segments.get(&key) else {
            return Ok(None);
        };
        let result = self.seal_segment(&key.0, segment, status, event_cursor, flow);
        match result {
            Ok(receipt) => {
                state.segments.remove(&key);
                Ok(Some(receipt))
            }
            Err(error) => {
                state.unavailable = Some(error.to_string());
                Err(error)
            }
        }
    }

    pub fn seal_startup(&self, status: &str) -> Result<Option<ArchiveReceipt>> {
        self.seal_with_flow(
            &RunId(format!("core-startup-{}", self.core_session)),
            "startup",
            status,
            0,
            FlowSource::Inline(&[]),
        )
    }

    fn seal_segment(
        &self,
        run_id: &str,
        segment: &Segment,
        status: &str,
        event_cursor: i64,
        flow: FlowSource<'_>,
    ) -> Result<ArchiveReceipt> {
        let output_dir = self.root.join(&segment.local_date);
        ensure_private_directory(&output_dir)?;
        let stem = if segment.phase == "startup" {
            format!("core-startup-{}", segment.id)
        } else {
            format!("run-{run_id}-{}-{}", segment.phase, segment.id)
        };
        let temporary = output_dir.join(format!(".{stem}.zip.tmp"));
        let final_path = output_dir.join(format!("{stem}.zip"));
        let inspection = inspect_source(&segment.source)?;
        if inspection.count != segment.count || !inspection.pending.is_empty() {
            return Err(unavailable(
                "Run log has unclosed model calls or changed event count",
            ));
        }
        if inspection.unreconciled == 0 {
            if let FlowSource::Store { store, .. } = &flow {
                for scope in &inspection.completed_calls {
                    if !store
                        .run_log_model_call_accounted(
                            &RunId(run_id.to_owned()),
                            scope.task_id.as_deref(),
                            scope.attempt_id.as_deref(),
                            scope.call_id.as_deref(),
                        )
                        .map_err(|error| unavailable(error.to_string()))?
                    {
                        return Err(unavailable(
                            "provider response has no durable Store accounting; archive and new work are blocked",
                        ));
                    }
                }
            }
        }
        let summary = format!(
            "# Run {run_id} · {}\n\nStatus: {status}\nCore session: {}\nLocal start: {} ({})\nRust event cursor: {event_cursor}\nUnknown provider calls: {}\nUnreconciled provider calls: {}\n\nA ZIP is a private diagnostic derivative, not a business or fill authority.\n",
            segment.phase, segment.core_session, segment.local_date, segment.local_offset,
            inspection.unknown, inspection.unreconciled,
        );
        let file = create_private_file(&temporary)?;
        let mut writer = ZipWriter::new(file);
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
        let mut hashes = BTreeMap::new();
        hashes.insert(
            "events.jsonl",
            zip_entry(
                &mut writer,
                "events.jsonl",
                open_regular_file(&segment.source)?,
                options,
            )?,
        );
        let llm_hash = {
            writer
                .start_file("llm_calls.jsonl", options)
                .map_err(|e| unavailable(e.to_string()))?;
            let mut output = HashingWriter::new(&mut writer);
            for_each_event(&segment.source, |line, value| {
                if value["kind"]
                    .as_str()
                    .is_some_and(|kind| kind.starts_with("llm."))
                {
                    output
                        .write_all(line)
                        .map_err(|error| unavailable(error.to_string()))?;
                }
                Ok(())
            })?;
            output.finish()
        };
        hashes.insert("llm_calls.jsonl", llm_hash);
        let (flow_hash, flow_count, start_cursor) = write_flow(&mut writer, run_id, flow, options)?;
        hashes.insert("rust_flow.jsonl", flow_hash);
        hashes.insert(
            "summary.md",
            zip_entry(&mut writer, "summary.md", summary.as_bytes(), options)?,
        );
        let manifest = json!({
            "version": 1,
            "run_id": run_id,
            "phase": segment.phase,
            "segment_id": segment.id,
            "core_session": segment.core_session,
            "started_at_utc": segment.started_at,
            "ended_at_utc": Utc::now(),
            "local_date": segment.local_date,
            "local_offset": segment.local_offset,
            "status": status,
            "event_count": segment.count,
            "rust_event_count": flow_count,
            "event_start_cursor": start_cursor,
            "segment_start_cursor": segment.start_event_cursor,
            "store_identity": segment.store_identity,
            "event_cursor": event_cursor,
            "previous_segment": segment.previous,
            "integrity": {
                "status": if inspection.unknown > 0 {"partial_unknown_provider_result"}
                    else if inspection.unreconciled > 0 {"partial_unreconciled_provider_result"}
                    else if !inspection.active_attempts.is_empty() {"partial_unclosed_rust_task"}
                    else {"complete"},
                "unknown_provider_calls": inspection.unknown,
                "unreconciled_provider_calls": inspection.unreconciled,
                "unclosed_rust_tasks": inspection.active_attempts.len(),
                "events_sha256": hashes["events.jsonl"]
            },
            "privacy": "private_local_credential_redacted_not_share_safe"
        });
        let manifest_bytes =
            serde_json::to_vec_pretty(&manifest).map_err(|e| unavailable(e.to_string()))?;
        hashes.insert(
            "manifest.json",
            zip_entry(
                &mut writer,
                "manifest.json",
                manifest_bytes.as_slice(),
                options,
            )?,
        );
        let checksums = hashes
            .iter()
            .map(|(name, hash)| format!("{hash}  {name}\n"))
            .collect::<String>();
        zip_entry(
            &mut writer,
            "checksums.sha256",
            checksums.as_bytes(),
            options,
        )?;
        let file = writer.finish().map_err(|e| unavailable(e.to_string()))?;
        self.fail_here("zip_sync")?;
        file.sync_all().map_err(|e| unavailable(e.to_string()))?;
        self.fail_here("zip_verify")?;
        self.verify_published(run_id, segment, &temporary)?;
        self.fail_here("zip_publish")?;
        fs::hard_link(&temporary, &final_path).map_err(|e| unavailable(e.to_string()))?;
        File::open(&output_dir)
            .and_then(|dir| dir.sync_all())
            .map_err(|e| unavailable(e.to_string()))?;
        fs::remove_file(&temporary).map_err(|e| unavailable(e.to_string()))?;
        self.verify_published(run_id, segment, &final_path)
    }

    /// The startup probe has no Run ID in the Store; its own verified ZIP is
    /// the only receipt eligible for this narrowly scoped cleanup.
    pub fn confirm_startup(&self, receipt: &ArchiveReceipt) -> Result<()> {
        if receipt.phase != "startup"
            || receipt
                .run_id
                .strip_prefix("core-startup-")
                .is_none_or(|id| Uuid::parse_str(id).is_err())
        {
            return Err(unavailable("invalid startup receipt"));
        }
        self.cleanup_source(receipt)
    }

    /// Only an exact Store-confirmed archive receipt can authorize removal of
    /// this segment's plaintext. ZIP and historical files are never removed.
    pub(crate) fn confirm_after_store(
        &self,
        store: &Store,
        receipt: &ArchiveReceipt,
    ) -> Result<()> {
        if receipt.phase == "startup"
            || !store
                .run_log_status(&RunId(receipt.run_id.clone()))
                .map_err(|error| unavailable(error.to_string()))?
                .iter()
                .any(|entry| {
                    entry["status"] == "archived"
                        && entry["phase"] == receipt.phase
                        && entry["detail"]["path"] == receipt.path.to_string_lossy().as_ref()
                        && entry["detail"]["segment_id"] == receipt.segment_id
                        && entry["detail"]["sha256"] == receipt.sha256.as_str()
                })
        {
            return Err(unavailable(
                "Run log has no matching durable archive confirmation",
            ));
        }
        self.cleanup_source(receipt)
    }

    fn cleanup_source(&self, receipt: &ArchiveReceipt) -> Result<()> {
        validate_log_identity(&receipt.run_id, &receipt.phase)?;
        if Uuid::parse_str(&receipt.segment_id).is_err() {
            return Err(unavailable("invalid Run log segment ID"));
        }
        let day = receipt
            .path
            .parent()
            .ok_or_else(|| unavailable("archive has no parent"))?;
        if day.parent() != Some(self.root.as_path())
            || !day
                .file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|value| value.len() == 8 && value.bytes().all(|b| b.is_ascii_digit()))
        {
            return Err(unavailable("Run ZIP path is outside the private log root"));
        }
        let stem = if receipt.phase == "startup" {
            format!("core-startup-{}", receipt.segment_id)
        } else {
            format!(
                "run-{}-{}-{}",
                receipt.run_id, receipt.phase, receipt.segment_id
            )
        };
        if receipt.path != day.join(format!("{stem}.zip"))
            || hash_reader(open_regular_file(&receipt.path)?)? != receipt.sha256
        {
            return Err(unavailable(
                "Run ZIP identity or content changed before cleanup",
            ));
        }
        let source = receipt
            .path
            .parent()
            .ok_or_else(|| unavailable("archive has no parent"))?
            .join(".inflight")
            .join(format!(
                "{}-{}-{}.jsonl",
                receipt.run_id, receipt.phase, receipt.segment_id
            ));
        ensure_private_directory(
            source
                .parent()
                .ok_or_else(|| unavailable("inflight source has no parent"))?,
        )?;
        open_regular_file(&source)?;
        fs::remove_file(source).map_err(|error| unavailable(error.to_string()))
    }
}

fn effective_user_home() -> Result<PathBuf> {
    #[cfg(unix)]
    {
        use std::{ffi::CStr, os::unix::ffi::OsStrExt};
        let mut size = 16 * 1024;
        loop {
            let mut buffer = vec![0_u8; size];
            let mut account = std::mem::MaybeUninit::<libc::passwd>::uninit();
            let mut result = std::ptr::null_mut();
            // SAFETY: getpwuid_r writes into distinct, appropriately sized
            // buffers owned by this call. ERANGE is retried with more space.
            let code = unsafe {
                libc::getpwuid_r(
                    libc::geteuid(),
                    account.as_mut_ptr(),
                    buffer.as_mut_ptr().cast(),
                    buffer.len(),
                    &mut result,
                )
            };
            if code == libc::ERANGE && size < 1024 * 1024 {
                size *= 2;
                continue;
            }
            if code != 0 || result.is_null() {
                return Err(unavailable(format!(
                    "cannot resolve effective user's home directory: {code}"
                )));
            }
            // SAFETY: a successful getpwuid_r points into `buffer`, which
            // remains alive while the home bytes are copied to PathBuf.
            let directory = unsafe { (*result).pw_dir };
            if directory.is_null() {
                return Err(unavailable("effective user has no home directory"));
            }
            let home = unsafe { CStr::from_ptr(directory) };
            let path = PathBuf::from(std::ffi::OsStr::from_bytes(home.to_bytes()));
            if !path.is_absolute() {
                return Err(unavailable("effective user's home must be absolute"));
            }
            return Ok(path);
        }
    }
    #[cfg(not(unix))]
    {
        let home = std::env::var_os("HOME")
            .filter(|value| !value.is_empty())
            .ok_or_else(|| unavailable("HOME is not configured"))?;
        let path = PathBuf::from(home);
        if !path.is_absolute() {
            return Err(unavailable("HOME must be absolute"));
        }
        Ok(path)
    }
}

fn store_log_identity(store: &Store) -> String {
    // A non-secret fingerprint separates independent canonical Store Roots
    // sharing the same ~/.akzio/log directory. Never serialize the full
    // configured path or database into a private Run archive.
    ContentHash::of_bytes(store.root().to_string_lossy().as_bytes())
        .as_str()
        .to_owned()
}

fn validate_log_identity(run_id: &str, phase: &str) -> Result<()> {
    if run_id.is_empty()
        || !run_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        || !matches!(phase, "startup" | "T0" | "T1" | "T3" | "T5")
        || (phase == "startup") != run_id.starts_with("core-startup-")
        || (phase == "startup"
            && run_id
                .strip_prefix("core-startup-")
                .is_none_or(|id| Uuid::parse_str(id).is_err()))
    {
        return Err(unavailable("Run log identity is not a safe path component"));
    }
    Ok(())
}

fn write_flow(
    writer: &mut ZipWriter<File>,
    run_id: &str,
    flow: FlowSource<'_>,
    options: SimpleFileOptions,
) -> Result<(ContentHash, u64, i64)> {
    writer
        .start_file("rust_flow.jsonl", options)
        .map_err(|error| unavailable(error.to_string()))?;
    let mut output = HashingWriter::new(writer);
    let mut count = 0;
    let start_cursor = match flow {
        FlowSource::Inline(events) => {
            for event in events {
                serde_json::to_writer(&mut output, event)
                    .map_err(|error| unavailable(error.to_string()))?;
                output
                    .write_all(b"\n")
                    .map_err(|error| unavailable(error.to_string()))?;
                count += 1;
            }
            0
        }
        FlowSource::Store {
            store,
            after,
            ceiling,
        } => {
            if after > ceiling {
                return Err(unavailable("Run event cursor moved backwards"));
            }
            let mut cursor = after;
            while cursor < ceiling {
                let previous = cursor;
                let page = store
                    .events_after(&RunId(run_id.to_owned()), cursor, 256)
                    .map_err(|error| unavailable(error.to_string()))?;
                if page.is_empty() {
                    return Err(unavailable("Run event window has a missing event"));
                }
                for event in page {
                    if event.cursor > ceiling {
                        break;
                    }
                    if event.cursor <= cursor {
                        return Err(unavailable("Run event window is out of order"));
                    }
                    serde_json::to_writer(&mut output, &event)
                        .map_err(|error| unavailable(error.to_string()))?;
                    output
                        .write_all(b"\n")
                        .map_err(|error| unavailable(error.to_string()))?;
                    count += 1;
                    cursor = event.cursor;
                }
                if cursor == previous {
                    return Err(unavailable("Run event window did not advance"));
                }
            }
            after
        }
    };
    Ok((output.finish(), count, start_cursor))
}

fn zip_entry(
    writer: &mut ZipWriter<File>,
    name: &str,
    mut source: impl Read,
    options: SimpleFileOptions,
) -> Result<ContentHash> {
    writer
        .start_file(name, options)
        .map_err(|error| unavailable(error.to_string()))?;
    let mut output = HashingWriter::new(writer);
    io::copy(&mut source, &mut output).map_err(|error| unavailable(error.to_string()))?;
    Ok(output.finish())
}

struct HashingWriter<'a, W: Write> {
    inner: &'a mut W,
    hash: Sha256,
}

impl<'a, W: Write> HashingWriter<'a, W> {
    fn new(inner: &'a mut W) -> Self {
        Self {
            inner,
            hash: Sha256::new(),
        }
    }

    fn finish(self) -> ContentHash {
        ContentHash::new(format!("{:x}", self.hash.finalize()))
            .expect("SHA-256 always produces a lowercase 64-byte hex digest")
    }
}

impl<W: Write> Write for HashingWriter<'_, W> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buffer)?;
        self.hash.update(&buffer[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn hash_reader(mut reader: impl Read) -> Result<ContentHash> {
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let size = reader
            .read(&mut buffer)
            .map_err(|error| unavailable(error.to_string()))?;
        if size == 0 {
            break;
        }
        hash.update(&buffer[..size]);
    }
    ContentHash::new(format!("{:x}", hash.finalize()))
        .map_err(|error| unavailable(error.to_string()))
}

fn parse_checksums(source: &str) -> Result<BTreeMap<String, ContentHash>> {
    let mut checksums = BTreeMap::new();
    for line in source.lines() {
        let (hash, name) = line
            .split_once("  ")
            .ok_or_else(|| unavailable("invalid Run ZIP checksum record"))?;
        if name.is_empty()
            || checksums
                .insert(
                    name.to_owned(),
                    ContentHash::new(hash).map_err(|error| unavailable(error.to_string()))?,
                )
                .is_some()
        {
            return Err(unavailable("duplicate or invalid Run ZIP checksum entry"));
        }
    }
    Ok(checksums)
}

fn open_regular_file(path: &Path) -> Result<File> {
    let meta = fs::symlink_metadata(path).map_err(|error| unavailable(error.to_string()))?;
    if !meta.is_file() || meta.file_type().is_symlink() {
        return Err(unavailable("Run log path is not a regular file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if meta.permissions().mode() & 0o777 != 0o600 {
            return Err(unavailable("Run log file is not private (expected 0600)"));
        }
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .map_err(|error| unavailable(error.to_string()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let opened = file
            .metadata()
            .map_err(|error| unavailable(error.to_string()))?;
        if opened.ino() != meta.ino() || opened.dev() != meta.dev() {
            return Err(unavailable("Run log file changed during secure open"));
        }
    }
    Ok(file)
}

/// A record may be large; only a single NDJSON record is held at once. The
/// archive itself, and every other event, are streamed in bounded chunks.
fn for_each_event(path: &Path, mut visit: impl FnMut(&[u8], &Value) -> Result<()>) -> Result<u64> {
    let mut input = BufReader::new(open_regular_file(path)?);
    let mut line = Vec::new();
    let mut count = 0;
    loop {
        line.clear();
        let size = input
            .read_until(b'\n', &mut line)
            .map_err(|error| unavailable(error.to_string()))?;
        if size == 0 {
            break;
        }
        if !line.ends_with(b"\n") {
            return Err(unavailable(
                "unfinished Run log contains a partial JSON record",
            ));
        }
        let event: Value =
            serde_json::from_slice(&line).map_err(|error| unavailable(error.to_string()))?;
        visit(&line, &event)?;
        count += 1;
    }
    if count == 0 {
        return Err(unavailable("empty unfinished Run log needs inspection"));
    }
    Ok(count)
}

fn inspect_source(path: &Path) -> Result<SourceInspection> {
    let mut inspection = SourceInspection::default();
    let mut identity: Option<(String, String, String, String, Option<String>)> = None;
    let count = for_each_event(path, |_, event| {
        let field = |name: &str| -> Result<&str> {
            event[name]
                .as_str()
                .ok_or_else(|| unavailable(format!("Run log entry has no {name}")))
        };
        let current = (
            field("run_id")?,
            field("phase")?,
            field("core_session")?,
            field("local_offset")?,
            event["store_identity"].as_str(),
        );
        if let Some(expected) = &identity {
            if current
                != (
                    expected.0.as_str(),
                    expected.1.as_str(),
                    expected.2.as_str(),
                    expected.3.as_str(),
                    expected.4.as_deref(),
                )
            {
                return Err(unavailable(
                    "Run log mixes different Run, phase or Core identities",
                ));
            }
        } else {
            validate_log_identity(current.0, current.1)?;
            identity = Some((
                current.0.to_owned(),
                current.1.to_owned(),
                current.2.to_owned(),
                current.3.to_owned(),
                current.4.map(str::to_owned),
            ));
        }
        let kind = event["kind"]
            .as_str()
            .ok_or_else(|| unavailable("Run log entry has no kind"))?;
        match kind {
            "rust.task.claimed" => {
                if let Some(attempt_id) = event["attempt_id"].as_str() {
                    inspection.active_attempts.insert(attempt_id.to_owned());
                }
            }
            "rust.task.result" => {
                if let Some(attempt_id) = event["attempt_id"].as_str() {
                    inspection.active_attempts.remove(attempt_id);
                }
            }
            "llm.request" => inspection.pending.push(ModelAuditContext {
                run_id: current.0.to_owned(),
                phase: current.1.to_owned(),
                task_id: event["task_id"].as_str().map(str::to_owned),
                attempt_id: event["attempt_id"].as_str().map(str::to_owned),
                call_id: event["call_id"].as_str().map(str::to_owned),
                retry_index: event["retry_index"]
                    .as_u64()
                    .and_then(|value| u32::try_from(value).ok()),
            }),
            "llm.terminal" | "llm.unknown_after_crash" | "llm.unknown_after_cancel" => {
                if kind != "llm.terminal" {
                    inspection.unknown += 1;
                }
                let call_id = event["call_id"].as_str();
                let index = inspection
                    .pending
                    .iter()
                    .position(|context| context.call_id.as_deref() == call_id)
                    .ok_or_else(|| unavailable("Run log has an unmatched model terminal"))?;
                let scope = inspection.pending.remove(index);
                if kind == "llm.terminal" {
                    inspection.completed_calls.push(scope);
                }
            }
            "llm.unreconciled_after_crash" => {
                let call_id = event["call_id"]
                    .as_str()
                    .ok_or_else(|| unavailable("unreconciled model call has no ID"))?;
                if !inspection
                    .completed_calls
                    .iter()
                    .any(|context| context.call_id.as_deref() == Some(call_id))
                    || !inspection.unreconciled_call_ids.insert(call_id.to_owned())
                {
                    return Err(unavailable(
                        "Run log has an unmatched or duplicate unreconciled call",
                    ));
                }
                inspection.unreconciled += 1;
            }
            _ => {}
        }
        Ok(())
    })?;
    inspection.count = count;
    Ok(inspection)
}

fn unavailable(message: impl Into<String>) -> RunLogError {
    RunLogError::Unavailable(message.into())
}

fn create_private_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
        options.custom_flags(libc::O_NOFOLLOW);
    }
    options
        .open(path)
        .map_err(|error| unavailable(error.to_string()))
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    let mut ancestor = PathBuf::new();
    for part in path.components() {
        ancestor.push(part);
        match fs::symlink_metadata(&ancestor) {
            Ok(meta) if meta.is_dir() && !meta.file_type().is_symlink() => {}
            Ok(_) => return Err(unavailable("log path contains a symlink or non-directory")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut builder = fs::DirBuilder::new();
                #[cfg(unix)]
                {
                    use std::os::unix::fs::DirBuilderExt;
                    builder.mode(0o700);
                }
                builder
                    .create(&ancestor)
                    .map_err(|e| unavailable(e.to_string()))?;
            }
            Err(error) => return Err(unavailable(error.to_string())),
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|error| unavailable(error.to_string()))?;
    }
    Ok(())
}

fn redact_credentials(value: &mut Value, secrets: &[String]) {
    match value {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                // Provider/tool payloads may spell credential keys with
                // hyphens, underscores or mixed case. Keep usage fields such
                // as input_tokens/output_tokens intact.
                let key = key
                    .chars()
                    .filter(char::is_ascii_alphanumeric)
                    .map(|character| character.to_ascii_lowercase())
                    .collect::<String>();
                if [
                    "authorization",
                    "apikey",
                    "secret",
                    "password",
                    "cookie",
                    "credential",
                    "accesstoken",
                    "authtoken",
                    "refreshtoken",
                ]
                .iter()
                .any(|part| key.contains(part))
                    || key.ends_with("token")
                {
                    *child = Value::String("<redacted>".into());
                } else {
                    redact_credentials(child, secrets);
                }
            }
        }
        Value::Array(items) => items
            .iter_mut()
            .for_each(|item| redact_credentials(item, secrets)),
        Value::String(text) => {
            for secret in secrets {
                if text.contains(secret) {
                    *text = text.replace(secret, "<redacted>");
                }
            }
            for name in [
                "OPENAI_API_KEY",
                "ALPACA_API_KEY",
                "ALPACA_API_SECRET",
                "FRED_API_KEY",
                "AKZIO_DAEMON_TOKEN",
            ] {
                if let Ok(secret) = std::env::var(name) {
                    if secret.len() >= 8 && text.contains(&secret) {
                        *text = text.replace(&secret, "<redacted>");
                    }
                }
            }
            static CREDENTIAL_PATTERN: OnceLock<Regex> = OnceLock::new();
            let pattern = CREDENTIAL_PATTERN.get_or_init(|| {
                Regex::new(
                    r#"(?i)\b(?:bearer[ \t]+[a-z0-9._~+/-]{8,}|sk-[a-z0-9_-]{8,}|(?:api[_-]?key|access[_-]?token|authorization)\s*[:=]\s*[^\s,;"'}]{8,})"#,
                )
                .expect("static credential pattern compiles")
            });
            if pattern.is_match(text) {
                *text = pattern.replace_all(text, "<redacted>").into_owned();
            }
        }
        _ => {}
    }
}

impl ModelAuditSink for RunLogger {
    fn record_model(
        &self,
        context: &ModelAuditContext,
        kind: &str,
        payload: Value,
    ) -> std::result::Result<(), String> {
        self.record(context, kind, payload)
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use akzio_domain::{
        Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance,
        FailureDisposition, RetryPolicy, RunPurpose, TaskBudget, TaskId, TaskRecipeId, TaskStatus,
        WorkflowGraph, WorkflowNode, DOMAIN_SCHEMA_VERSION,
    };
    use akzio_store::{StoredRun, WorkflowCommit};

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new() -> Self {
            let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/run-log-tests")
                .join(Uuid::new_v4().to_string());
            fs::create_dir_all(&path).unwrap();
            Self(path.canonicalize().unwrap())
        }

        fn logger(&self) -> RunLogger {
            let root = self.0.join("log");
            ensure_private_directory(&root).unwrap();
            RunLogger {
                root,
                core_session: Uuid::new_v4().to_string(),
                state: Mutex::new(State::default()),
                store: OnceLock::new(),
                fail_once: Mutex::new(None),
            }
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }

    fn formal_fixture_run(store: &Store) -> RunId {
        let now = Utc::now();
        let run_id = RunId::new();
        let node = WorkflowNode {
            spec: None,
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new("test.audit").unwrap(),
            contract_hash: None,
            objective: "Offline logging fixture".into(),
            dependencies: vec![],
            input_artifacts: vec![],
            priority: 1,
            budget: TaskBudget {
                max_input_tokens: 1,
                max_output_tokens: 1,
                max_wall_time_secs: 1,
                max_tool_calls: akzio_domain::budget::ToolCallLimit::Limited(0),
            },
            retry: RetryPolicy::none(),
            on_failure: FailureDisposition::FailRun,
            parent_task_id: None,
        };
        let graph = WorkflowGraph {
            definition_version: None,
            schema_version: DOMAIN_SCHEMA_VERSION,
            topology_id: "private-log-fixture".into(),
            nodes: vec![node.clone()],
            agent_budgets: Default::default(),
        };
        let artifact = Artifact::new(
            ArtifactKind::WorkflowGraph,
            store.stage_json(&graph).unwrap(),
            "runtime.workflow",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.runtime".into(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            Some(ArtifactOrigin {
                run_id: Some(run_id.clone()),
                task_id: None,
                attempt_id: None,
                contract_hash: None,
            }),
            vec![],
            now,
        )
        .unwrap();
        store
            .commit_workflow(&WorkflowCommit {
                run: StoredRun {
                    run_id: run_id.clone(),
                    purpose: RunPurpose::PositionPlan,
                    topology_id: graph.topology_id,
                    graph_artifact_id: artifact.artifact_id.clone(),
                    created_at: now,
                },
                graph: artifact,
                nodes: vec![node],
            })
            .unwrap();
        run_id
    }

    #[test]
    fn private_stage_zip_keeps_actual_model_body_and_rust_trace_without_credentials() {
        let root = TestRoot::new();
        let logger = root.logger();
        logger.register_secret("private-test-key").unwrap();
        let run = RunId(format!("run-{}", Uuid::new_v4()));
        let scope = ModelAuditContext {
            run_id: run.0.clone(),
            phase: "T0".into(),
            task_id: Some("task-A".into()),
            attempt_id: Some("attempt-A".into()),
            call_id: Some("call-A".into()),
            retry_index: Some(0),
        };
        logger
            .record(
                &scope,
                "llm.request",
                json!({
                    "request": {"instructions":"use private-test-key and sk-abcdefgh123456789",
                        "max_output_tokens": 64, "input": "authorized context",
                        "usage": {"output_tokens": 64}},
                    "authorization": "private-test-key",
                    "X-API-Key": "other-unregistered-secret-value",
                }),
            )
            .unwrap();
        logger
            .record(
                &scope,
                "llm.terminal",
                json!({"response":{"output":"legitimate model output"}}),
            )
            .unwrap();
        let receipt = logger
            .seal(
                &run,
                "T0",
                "completed",
                42,
                &[json!({"cursor":42,"event_type":"task.succeeded"})],
            )
            .unwrap()
            .unwrap();
        assert!(receipt.path.starts_with(&root.0));
        assert_eq!(receipt.event_count, 2);
        assert_eq!(receipt.event_cursor, 42);
        let mut archive = ZipArchive::new(File::open(&receipt.path).unwrap()).unwrap();
        let read = |archive: &mut ZipArchive<File>, name: &str| -> String {
            let mut output = String::new();
            archive
                .by_name(name)
                .unwrap()
                .read_to_string(&mut output)
                .unwrap();
            output
        };
        let calls = read(&mut archive, "llm_calls.jsonl");
        let flow = read(&mut archive, "rust_flow.jsonl");
        let manifest = read(&mut archive, "manifest.json");
        assert!(calls.contains("authorized context"));
        assert!(calls.contains("legitimate model output"));
        assert!(calls.contains("\"max_output_tokens\":64"));
        assert!(!calls.contains("private-test-key"));
        assert!(!calls.contains("sk-abcdefgh123456789"));
        assert!(!calls.contains("other-unregistered-secret-value"));
        assert!(calls.contains("\"output_tokens\":64"));
        assert!(calls.contains("<redacted>"));
        assert!(flow.contains("task.succeeded"));
        assert!(manifest.contains("\"event_cursor\": 42"));
        drop(archive);
        let store = Store::open(root.0.join("store")).unwrap();
        assert!(logger.confirm_after_store(&store, &receipt).is_err());
        assert_eq!(logger.pending().unwrap().len(), 0);
        assert_eq!(
            fs::read_dir(receipt.path.parent().unwrap().join(".inflight"))
                .unwrap()
                .count(),
            1,
            "unconfirmed formal plaintext must remain recoverable"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&receipt.path).unwrap().permissions().mode() & 0o777,
                0o600
            );
            assert_eq!(
                fs::metadata(receipt.path.parent().unwrap())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }
    }

    #[test]
    fn orphaned_startup_is_sealed_as_interrupted_not_completed() {
        let root = TestRoot::new();
        let logger = root.logger();
        let startup = ModelAuditContext::startup(logger.core_session());
        logger
            .record(
                &startup,
                "llm.request",
                json!({"request":{"input":"test-only local probe"}}),
            )
            .unwrap();
        drop(logger);
        let replacement = root.logger();
        let store = Store::open(root.0.join("store")).unwrap();
        assert_eq!(replacement.recover_orphans(&store).unwrap(), 1);
        assert_eq!(replacement.recover_orphans(&store).unwrap(), 0);
        let day = Local::now().format("%Y%m%d").to_string();
        let zip = fs::read_dir(replacement.root.join(day))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("zip"))
            .unwrap();
        let mut archive = ZipArchive::new(File::open(zip).unwrap()).unwrap();
        let manifest: Value =
            serde_json::from_reader(archive.by_name("manifest.json").unwrap()).unwrap();
        assert_eq!(manifest["status"], "interrupted_unknown_after_crash");
    }

    #[test]
    fn failed_logger_blocks_new_calls_and_rejects_symlink_paths() {
        let root = TestRoot::new();
        let logger = root.logger();
        logger.mark_unavailable("simulated disk failure").unwrap();
        let scope = ModelAuditContext::startup(logger.core_session());
        assert!(logger.record(&scope, "llm.request", json!({})).is_err());
        assert!(logger.assert_available().is_err());
        #[cfg(unix)]
        {
            let link = root.0.join("linked");
            std::os::unix::fs::symlink(root.0.join("log"), &link).unwrap();
            assert!(ensure_private_directory(&link.join("20260930")).is_err());
        }
    }

    #[test]
    fn crashed_request_is_explicitly_unknown_and_recovered_without_a_second_call() {
        let root = TestRoot::new();
        let logger = root.logger();
        let scope = ModelAuditContext::startup(logger.core_session());
        logger
            .record(
                &scope,
                "llm.request",
                json!({"request":{"input":"sent before crash"}}),
            )
            .unwrap();
        drop(logger);

        let replacement = root.logger();
        let store = Store::open(root.0.join("store")).unwrap();
        assert_eq!(replacement.recover_orphans(&store).unwrap(), 1);
        let zip = fs::read_dir(
            replacement
                .root
                .join(Local::now().format("%Y%m%d").to_string()),
        )
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|ext| ext == "zip"))
        .unwrap();
        let mut archive = ZipArchive::new(File::open(zip).unwrap()).unwrap();
        let mut events = String::new();
        archive
            .by_name("events.jsonl")
            .unwrap()
            .read_to_string(&mut events)
            .unwrap();
        assert!(events.contains("\"kind\":\"llm.unknown_after_crash\""));
        assert!(!events.contains("\"kind\":\"llm.terminal\""));
    }

    #[test]
    fn published_zip_with_modified_summary_is_not_confirmed_or_cleaned() {
        let root = TestRoot::new();
        let logger = root.logger();
        let scope = ModelAuditContext::startup(logger.core_session());
        logger
            .record(&scope, "core.startup", json!({"status":"started"}))
            .unwrap();
        let receipt = logger.seal_startup("completed").unwrap().unwrap();
        let source = receipt
            .path
            .parent()
            .unwrap()
            .join(".inflight")
            .join(format!(
                "{}-startup-{}.jsonl",
                receipt.run_id, receipt.segment_id
            ));
        let mut old = ZipArchive::new(File::open(&receipt.path).unwrap()).unwrap();
        let mut changed = Vec::new();
        {
            let mut writer = ZipWriter::new(std::io::Cursor::new(&mut changed));
            for index in 0..old.len() {
                let mut entry = old.by_index(index).unwrap();
                let name = entry.name().to_owned();
                let mut body = Vec::new();
                entry.read_to_end(&mut body).unwrap();
                if name == "summary.md" {
                    body = b"FORGED SUMMARY".to_vec();
                }
                writer
                    .start_file(name, SimpleFileOptions::default())
                    .unwrap();
                writer.write_all(&body).unwrap();
            }
            writer.finish().unwrap();
        }
        fs::write(&receipt.path, changed).unwrap();
        drop(old);
        let replacement = root.logger();
        let store = Store::open(root.0.join("store")).unwrap();
        assert!(replacement.recover_orphans(&store).is_err());
        assert!(source.exists(), "failed archive must retain its plaintext");
    }

    #[test]
    fn restart_confirms_a_published_startup_zip_without_overwriting_it() {
        let root = TestRoot::new();
        let logger = root.logger();
        logger
            .record(
                &ModelAuditContext::startup(logger.core_session()),
                "core.startup",
                json!({"status":"started"}),
            )
            .unwrap();
        let receipt = logger.seal_startup("completed").unwrap().unwrap();
        let original_hash = receipt.sha256.clone();
        drop(logger);
        let replacement = root.logger();
        let store = Store::open(root.0.join("store")).unwrap();
        assert_eq!(replacement.recover_orphans(&store).unwrap(), 1);
        assert_eq!(replacement.recover_orphans(&store).unwrap(), 0);
        assert_eq!(
            hash_reader(File::open(&receipt.path).unwrap()).unwrap(),
            original_hash
        );
    }

    #[test]
    fn restart_publishes_an_already_validated_temporary_zip_without_new_model_calls() {
        let root = TestRoot::new();
        let logger = root.logger();
        logger
            .record(
                &ModelAuditContext::startup(logger.core_session()),
                "core.startup",
                json!({"status":"started"}),
            )
            .unwrap();
        let receipt = logger.seal_startup("completed").unwrap().unwrap();
        let temporary = receipt.path.with_file_name(format!(
            ".{}.zip.tmp",
            receipt.path.file_stem().unwrap().to_string_lossy()
        ));
        fs::rename(&receipt.path, &temporary).unwrap();
        drop(logger);
        let replacement = root.logger();
        let store = Store::open(root.0.join("store")).unwrap();
        assert_eq!(replacement.recover_orphans(&store).unwrap(), 1);
        assert!(receipt.path.is_file());
        assert_eq!(
            hash_reader(File::open(&receipt.path).unwrap()).unwrap(),
            receipt.sha256
        );
        assert!(!temporary.exists());
    }

    #[test]
    fn a_foreign_run_record_cannot_be_sealed_into_another_runs_zip() {
        let root = TestRoot::new();
        let logger = root.logger();
        logger
            .record(
                &ModelAuditContext::startup(logger.core_session()),
                "core.startup",
                json!({"status":"started"}),
            )
            .unwrap();
        let source = fs::read_dir(
            logger
                .root
                .join(Local::now().format("%Y%m%d").to_string())
                .join(".inflight"),
        )
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
        let mut injected: Value =
            serde_json::from_slice(fs::read(&source).unwrap().trim_ascii()).unwrap();
        injected["run_id"] = json!(RunId::new().0);
        let mut file = OpenOptions::new().append(true).open(source).unwrap();
        writeln!(file, "{injected}").unwrap();
        file.sync_all().unwrap();
        drop(logger);
        let replacement = root.logger();
        let store = Store::open(root.0.join("store")).unwrap();
        assert!(replacement.recover_orphans(&store).is_err());
    }

    #[test]
    fn multiple_runs_and_a_large_response_keep_distinct_immutable_segments() {
        let root = TestRoot::new();
        let logger = root.logger();
        let first = RunId::new();
        let second = RunId::new();
        let payload = "R".repeat(4 * 1024 * 1024);
        let context = |run: &RunId| ModelAuditContext {
            run_id: run.0.clone(),
            phase: "T0".into(),
            task_id: Some("task".into()),
            attempt_id: Some("attempt".into()),
            call_id: Some("call".into()),
            retry_index: Some(0),
        };
        logger
            .record(&context(&first), "llm.request", json!({"input":"first"}))
            .unwrap();
        logger
            .record(&context(&second), "llm.request", json!({"input":"second"}))
            .unwrap();
        logger
            .record(&context(&first), "llm.terminal", json!({"output":payload}))
            .unwrap();
        logger
            .record(&context(&second), "llm.terminal", json!({"output":"short"}))
            .unwrap();
        let receipt = logger
            .seal(&first, "T0", "completed", 17, &[])
            .unwrap()
            .unwrap();
        let other = logger
            .seal(&second, "T0", "completed", 23, &[])
            .unwrap()
            .unwrap();
        assert_ne!(receipt.path, other.path);
        let mut first_zip = ZipArchive::new(File::open(&receipt.path).unwrap()).unwrap();
        let mut first_calls = String::new();
        first_zip
            .by_name("llm_calls.jsonl")
            .unwrap()
            .read_to_string(&mut first_calls)
            .unwrap();
        assert!(first_calls.contains(&payload));
        assert!(!first_calls.contains("\"input\":\"second\""));
        let original_hash = receipt.sha256;
        logger
            .record(
                &context(&first),
                "rust.task.claimed",
                json!({"continuation":true}),
            )
            .unwrap();
        let continuation = logger
            .seal(&first, "T0", "resumed", 24, &[])
            .unwrap()
            .unwrap();
        assert_ne!(receipt.path, continuation.path);
        assert_eq!(
            hash_reader(File::open(&receipt.path).unwrap()).unwrap(),
            original_hash
        );
    }

    #[test]
    fn failed_sync_verification_and_publication_keep_plaintext_and_block_new_calls() {
        for point in ["record_sync", "zip_sync", "zip_verify", "zip_publish"] {
            let root = TestRoot::new();
            let logger = root.logger();
            let scope = ModelAuditContext::startup(logger.core_session());
            if point == "record_sync" {
                logger.fail_once.lock().unwrap().replace(point);
                assert!(logger.record(&scope, "core.startup", json!({})).is_err());
            } else {
                logger.record(&scope, "core.startup", json!({})).unwrap();
                logger.fail_once.lock().unwrap().replace(point);
                assert!(logger.seal_startup("completed").is_err());
            }
            assert!(
                logger.assert_available().is_err(),
                "{point} must block new calls"
            );
            assert!(logger.record(&scope, "llm.request", json!({})).is_err());
            let day = logger.root.join(Local::now().format("%Y%m%d").to_string());
            assert_eq!(fs::read_dir(day.join(".inflight")).unwrap().count(), 1);
            assert_eq!(
                fs::read_dir(&day)
                    .unwrap()
                    .filter(|entry| entry
                        .as_ref()
                        .unwrap()
                        .path()
                        .extension()
                        .is_some_and(|ext| ext == "zip"))
                    .count(),
                0,
                "{point} cannot claim a published archive"
            );
            drop(logger);
            let replacement = root.logger();
            let store = Store::open(root.0.join("store")).unwrap();
            assert_eq!(replacement.recover_orphans(&store).unwrap(), 1, "{point}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_non_private_plaintext_file_blocks_sealing_without_deletion() {
        use std::os::unix::fs::PermissionsExt;
        let root = TestRoot::new();
        let logger = root.logger();
        let scope = ModelAuditContext::startup(logger.core_session());
        logger.record(&scope, "core.startup", json!({})).unwrap();
        let source = fs::read_dir(
            logger
                .root
                .join(Local::now().format("%Y%m%d").to_string())
                .join(".inflight"),
        )
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
        fs::set_permissions(&source, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(logger.seal_startup("completed").is_err());
        assert!(logger.assert_available().is_err());
        assert!(source.exists());
    }

    #[test]
    fn unconfirmed_cancelled_model_call_never_authorizes_a_second_call_in_the_same_run() {
        let root = TestRoot::new();
        let logger = root.logger();
        let run = RunId::new();
        let scope = ModelAuditContext {
            run_id: run.0.clone(),
            phase: "T0".into(),
            task_id: Some("research-task".into()),
            attempt_id: Some("attempt-1".into()),
            call_id: Some("model-call-1".into()),
            retry_index: Some(0),
        };
        logger
            .record(&scope, "llm.request", json!({"request":{"input":"first"}}))
            .unwrap();
        logger
            .record(
                &scope,
                "llm.unknown_after_cancel",
                json!({"outcome":"unknown"}),
            )
            .unwrap();
        let mut second = scope.clone();
        second.call_id = Some("model-call-2".into());
        second.retry_index = Some(1);
        assert!(logger.record(&second, "llm.request", json!({})).is_err());
        let receipt = logger
            .seal(&run, "T0", "stopped_incomplete", 0, &[])
            .unwrap()
            .unwrap();
        assert_eq!(receipt.integrity_status, "partial_unknown_provider_result");
    }

    #[test]
    fn a_confirmed_partial_zip_keeps_its_run_blocked_after_restart() {
        let root = TestRoot::new();
        let store = Store::open(root.0.join("store")).unwrap();
        let run = formal_fixture_run(&store);
        let logger = root.logger();
        logger.attach_store(store.clone()).unwrap();
        let scope = ModelAuditContext {
            run_id: run.0.clone(),
            phase: "T0".into(),
            task_id: Some("task-1".into()),
            attempt_id: Some("attempt-1".into()),
            call_id: Some("call-1".into()),
            retry_index: Some(0),
        };
        logger
            .record(&scope, "llm.request", json!({"request":{"input":"sent"}}))
            .unwrap();
        logger
            .record(
                &scope,
                "llm.unknown_after_cancel",
                json!({"outcome":"unknown"}),
            )
            .unwrap();
        assert!(store.uncertain_run_log_ids().unwrap().contains(&run));
        let ceiling = store.workflow_snapshot(&run).unwrap().event_cursor;
        let receipt = logger
            .seal_with_store(&run, "T0", "stopped_incomplete", &store, 0, ceiling)
            .unwrap()
            .unwrap();
        store
            .record_run_log_status(
                &run,
                "T0",
                "archived",
                &serde_json::to_value(&receipt).unwrap(),
            )
            .unwrap();
        logger.confirm_after_store(&store, &receipt).unwrap();
        drop(logger);
        let replacement = root.logger();
        replacement.attach_store(store).unwrap();
        assert!(replacement.is_uncertain_run(&run.0));
        assert!(replacement
            .record(&scope, "llm.request", json!({}))
            .is_err());
        assert!(receipt.path.exists());
    }

    #[test]
    fn formal_crash_with_an_unanswered_model_request_never_replays_it_on_restart() {
        let root = TestRoot::new();
        let store = Store::open(root.0.join("store")).unwrap();
        let run = formal_fixture_run(&store);
        let logger = root.logger();
        logger.attach_store(store.clone()).unwrap();
        let scope = ModelAuditContext {
            run_id: run.0.clone(),
            phase: "T0".into(),
            task_id: Some("task".into()),
            attempt_id: Some("attempt".into()),
            call_id: Some("call-before-crash".into()),
            retry_index: Some(0),
        };
        logger
            .record(
                &scope,
                "llm.request",
                json!({"request":{"input":"unknown"}}),
            )
            .unwrap();
        drop(logger);
        let replacement = root.logger();
        assert_eq!(replacement.recover_orphans(&store).unwrap(), 1);
        replacement.attach_store(store.clone()).unwrap();
        assert!(replacement.is_uncertain_run(&run.0));
        let mut retry = scope.clone();
        retry.retry_index = Some(1);
        retry.call_id = Some("blind-retry".into());
        assert!(replacement
            .record(&retry, "llm.request", json!({}))
            .is_err());
        let statuses = store.run_log_status(&run).unwrap();
        assert!(statuses.iter().any(|value| {
            value["status"] == "archived"
                && value["detail"]["integrity_status"] == "partial_unknown_provider_result"
        }));
        assert!(statuses.iter().any(|value| {
            value["status"] == "blocked" && value["detail"]["reason"] == "unknown_provider_result"
        }));
    }

    #[test]
    fn a_formal_run_committed_before_the_first_log_is_recovered_from_its_store_marker() {
        let root = TestRoot::new();
        let store = Store::open(root.0.join("store")).unwrap();
        let run = formal_fixture_run(&store);
        assert_eq!(
            store.unstarted_required_run_logs().unwrap(),
            vec![run.clone()]
        );

        let logger = root.logger();
        assert_eq!(logger.recover_orphans(&store).unwrap(), 1);
        assert_eq!(logger.pending().unwrap(), vec![(run.clone(), "T0".into())]);
        assert!(store
            .run_log_status(&run)
            .unwrap()
            .iter()
            .any(|record| { record["status"] == "opened" && record["phase"] == "T0" }));
        let ceiling = store.workflow_snapshot(&run).unwrap().event_cursor;
        let receipt = logger
            .seal_with_store(&run, "T0", "stopped_incomplete", &store, 0, ceiling)
            .unwrap()
            .unwrap();
        store
            .record_run_log_status(
                &run,
                "T0",
                "archived",
                &serde_json::to_value(&receipt).unwrap(),
            )
            .unwrap();
        logger.confirm_after_store(&store, &receipt).unwrap();
        drop(logger);

        let replacement = root.logger();
        assert_eq!(replacement.recover_orphans(&store).unwrap(), 0);
        assert!(store.unstarted_required_run_logs().unwrap().is_empty());
    }

    #[test]
    fn a_resumed_stage_explicitly_links_its_previous_immutable_zip() {
        let root = TestRoot::new();
        let store = Store::open(root.0.join("store")).unwrap();
        let run = formal_fixture_run(&store);
        let logger = root.logger();
        logger.attach_store(store.clone()).unwrap();
        let scope = ModelAuditContext {
            run_id: run.0.clone(),
            phase: "T0".into(),
            task_id: None,
            attempt_id: None,
            call_id: None,
            retry_index: None,
        };
        logger
            .record(&scope, "rust.run.submitted", json!({"source":"test"}))
            .unwrap();
        let first_cursor = store.workflow_snapshot(&run).unwrap().event_cursor;
        let first = logger
            .seal_with_store(&run, "T0", "stopped_incomplete", &store, 0, first_cursor)
            .unwrap()
            .unwrap();
        store
            .record_run_log_status(
                &run,
                "T0",
                "archived",
                &serde_json::to_value(&first).unwrap(),
            )
            .unwrap();
        logger.confirm_after_store(&store, &first).unwrap();

        logger
            .record(&scope, "rust.task.claimed", json!({"continuation":true}))
            .unwrap();
        let next_cursor = store.workflow_snapshot(&run).unwrap().event_cursor;
        let second = logger
            .seal_with_store(&run, "T0", "resumed", &store, first_cursor, next_cursor)
            .unwrap()
            .unwrap();
        let mut zip = ZipArchive::new(open_regular_file(&second.path).unwrap()).unwrap();
        let manifest: Value =
            serde_json::from_reader(zip.by_name("manifest.json").unwrap()).unwrap();
        assert_eq!(manifest["previous_segment"]["segment_id"], first.segment_id);
        assert_eq!(
            manifest["previous_segment"]["sha256"],
            first.sha256.as_str()
        );
        assert_eq!(
            manifest["previous_segment"]["path"],
            first.path.to_string_lossy().as_ref()
        );
        assert_ne!(first.path, second.path);
    }

    #[test]
    fn a_later_stage_does_not_lose_events_when_t0_archives_after_it_began() {
        let root = TestRoot::new();
        let store = Store::open(root.0.join("store")).unwrap();
        let run = formal_fixture_run(&store);
        let logger = root.logger();
        logger.attach_store(store.clone()).unwrap();
        let scope = |phase: &str| ModelAuditContext {
            run_id: run.0.clone(),
            phase: phase.into(),
            task_id: None,
            attempt_id: None,
            call_id: None,
            retry_index: None,
        };
        logger
            .record(&scope("T0"), "rust.run.submitted", json!({}))
            .unwrap();
        logger
            .record(&scope("T1"), "rust.task.claimed", json!({"stage":"T1"}))
            .unwrap();
        let t1_after = logger.flow_after(&run, "T1").unwrap();
        let ceiling = store.workflow_snapshot(&run).unwrap().event_cursor;
        let t0 = logger
            .seal_with_store(&run, "T0", "completed", &store, 0, ceiling)
            .unwrap()
            .unwrap();
        store
            .record_run_log_status(&run, "T0", "archived", &serde_json::to_value(&t0).unwrap())
            .unwrap();
        logger.confirm_after_store(&store, &t0).unwrap();
        let t1_ceiling = store.workflow_snapshot(&run).unwrap().event_cursor;
        let t1 = logger
            .seal_with_store(&run, "T1", "partial", &store, t1_after, t1_ceiling)
            .unwrap()
            .unwrap();
        let mut zip = ZipArchive::new(open_regular_file(&t1.path).unwrap()).unwrap();
        let manifest: Value =
            serde_json::from_reader(zip.by_name("manifest.json").unwrap()).unwrap();
        assert_eq!(manifest["event_start_cursor"], t1_after);
        assert!(t1_after < t0.event_cursor);
        let mut flow = String::new();
        zip.by_name("rust_flow.jsonl")
            .unwrap()
            .read_to_string(&mut flow)
            .unwrap();
        assert!(
            flow.lines().any(|line| {
                let event: Value = serde_json::from_str(line).unwrap();
                event["cursor"]
                    .as_i64()
                    .is_some_and(|cursor| cursor > t1_after && cursor <= t0.event_cursor)
            }),
            "T1 must retain its own Store events even when T0's ZIP froze a later watermark"
        );
    }

    #[test]
    fn each_formal_stage_has_a_separate_immutable_zip_name() {
        let root = TestRoot::new();
        let logger = root.logger();
        let run = RunId::new();
        let mut paths = BTreeSet::new();
        for (phase, status) in [
            ("T0", "completed"),
            ("T1", "partial"),
            ("T3", "partial"),
            ("T5", "sealed"),
        ] {
            logger
                .record(
                    &ModelAuditContext {
                        run_id: run.0.clone(),
                        phase: phase.into(),
                        task_id: None,
                        attempt_id: None,
                        call_id: None,
                        retry_index: None,
                    },
                    "rust.stage.fixture",
                    json!({"status":status}),
                )
                .unwrap();
            let receipt = logger.seal(&run, phase, status, 0, &[]).unwrap().unwrap();
            assert_eq!(receipt.phase, phase);
            assert!(receipt
                .path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with(&format!("run-{}-{phase}-", run.0)));
            assert!(paths.insert(receipt.path));
        }
        assert_eq!(paths.len(), 4);
    }

    #[test]
    fn a_business_terminal_cannot_be_archived_before_its_task_result_is_logged() {
        let root = TestRoot::new();
        let logger = root.logger();
        let run = RunId::new();
        let scope = ModelAuditContext {
            run_id: run.0.clone(),
            phase: "T0".into(),
            task_id: Some("final-task".into()),
            attempt_id: Some("attempt-final".into()),
            call_id: None,
            retry_index: None,
        };
        logger
            .record(
                &scope,
                "rust.task.claimed",
                json!({"recipe":"gate.decision"}),
            )
            .unwrap();
        assert!(logger.has_active_task(&run, "T0").unwrap());
        logger
            .record(&scope, "rust.task.result", json!({"outcome":"Committed"}))
            .unwrap();
        assert!(!logger.has_active_task(&run, "T0").unwrap());
        let receipt = logger
            .seal(&run, "T0", "completed", 0, &[])
            .unwrap()
            .unwrap();
        assert_eq!(receipt.integrity_status, "complete");
    }

    #[test]
    fn a_crashed_rust_task_is_not_reported_as_a_complete_stage() {
        let root = TestRoot::new();
        let store = Store::open(root.0.join("store")).unwrap();
        let run = formal_fixture_run(&store);
        let logger = root.logger();
        logger.attach_store(store.clone()).unwrap();
        logger
            .record(
                &ModelAuditContext {
                    run_id: run.0.clone(),
                    phase: "T0".into(),
                    task_id: Some("test-task".into()),
                    attempt_id: Some("test-attempt".into()),
                    call_id: None,
                    retry_index: None,
                },
                "rust.task.claimed",
                json!({"recipe":"test.audit"}),
            )
            .unwrap();
        drop(logger);
        let replacement = root.logger();
        assert_eq!(replacement.recover_orphans(&store).unwrap(), 1);
        let record = store
            .run_log_status(&run)
            .unwrap()
            .into_iter()
            .find(|record| record["status"] == "archived")
            .unwrap();
        assert_eq!(record["detail"]["stage_status"], "interrupted_after_crash");
        assert_eq!(
            record["detail"]["integrity_status"],
            "partial_unclosed_rust_task"
        );
    }

    #[test]
    fn a_logged_provider_response_without_store_accounting_blocks_blind_replay() {
        let root = TestRoot::new();
        let store = Store::open(root.0.join("store")).unwrap();
        let run = formal_fixture_run(&store);
        let logger = root.logger();
        logger.attach_store(store.clone()).unwrap();
        let scope = ModelAuditContext {
            run_id: run.0.clone(),
            phase: "T0".into(),
            task_id: Some("uncommitted-task".into()),
            attempt_id: Some("uncommitted-attempt".into()),
            call_id: Some("model-response-not-in-store".into()),
            retry_index: Some(0),
        };
        logger
            .record(&scope, "llm.request", json!({"request":{"input":"sent"}}))
            .unwrap();
        logger
            .record(
                &scope,
                "llm.terminal",
                json!({"response":{"output":"received"}}),
            )
            .unwrap();
        drop(logger);

        let replacement = root.logger();
        assert_eq!(replacement.recover_orphans(&store).unwrap(), 1);
        assert!(replacement.is_uncertain_run(&run.0));
        assert!(replacement
            .record(
                &scope,
                "llm.request",
                json!({"request":{"input":"blind retry"}})
            )
            .is_err());
        let statuses = store.run_log_status(&run).unwrap();
        assert!(statuses.iter().any(|record| {
            record["status"] == "archived"
                && record["detail"]["integrity_status"] == "partial_unreconciled_provider_result"
        }));
        assert!(statuses.iter().any(|record| {
            record["status"] == "blocked"
                && record["detail"]["reason"] == "unreconciled_provider_result"
        }));
        drop(replacement);
        assert_eq!(root.logger().recover_orphans(&store).unwrap(), 0);
    }

    #[test]
    fn a_succeeded_store_attempt_proves_its_completed_provider_call() {
        let root = TestRoot::new();
        let store = Store::open(root.0.join("store")).unwrap();
        let run = formal_fixture_run(&store);
        let claimed = store
            .claim_next_task_for_workload(
                "accounted-model-call",
                Utc::now(),
                chrono::Duration::seconds(30),
                akzio_store::TaskWorkload::Session,
            )
            .unwrap()
            .unwrap();
        let logger = root.logger();
        logger.attach_store(store.clone()).unwrap();
        let scope = ModelAuditContext {
            run_id: run.0.clone(),
            phase: "T0".into(),
            task_id: Some(claimed.node.task_id.0.clone()),
            attempt_id: Some(claimed.permit.attempt_id.0.clone()),
            call_id: Some("successful-test-call".into()),
            retry_index: Some(0),
        };
        logger.record(&scope, "llm.request", json!({})).unwrap();
        logger.record(&scope, "llm.terminal", json!({})).unwrap();
        store
            .finish_task(&claimed.permit, TaskStatus::Succeeded, Utc::now())
            .unwrap();
        drop(logger);
        let replacement = root.logger();
        assert_eq!(replacement.recover_orphans(&store).unwrap(), 1);
        assert!(!replacement.is_uncertain_run(&run.0));
        assert!(store.run_log_status(&run).unwrap().iter().any(|record| {
            record["status"] == "archived" && record["detail"]["integrity_status"] == "complete"
        }));
    }

    #[test]
    fn a_published_but_unconfirmed_zip_cannot_hide_unaccounted_model_usage() {
        let root = TestRoot::new();
        let store = Store::open(root.0.join("store")).unwrap();
        let run = formal_fixture_run(&store);
        let logger = root.logger();
        logger.attach_store(store.clone()).unwrap();
        let scope = ModelAuditContext {
            run_id: run.0.clone(),
            phase: "T0".into(),
            task_id: Some("missing-task".into()),
            attempt_id: Some("missing-attempt".into()),
            call_id: Some("unconfirmed-published-call".into()),
            retry_index: Some(0),
        };
        logger.record(&scope, "llm.request", json!({})).unwrap();
        logger.record(&scope, "llm.terminal", json!({})).unwrap();
        let cursor = store.workflow_snapshot(&run).unwrap().event_cursor;
        // The inline seam simulates an older ZIP published before Store
        // accounting was verified; the formal Store-backed archiver refuses
        // this incomplete state before publication.
        let receipt = logger
            .seal(&run, "T0", "stopped_incomplete", cursor, &[])
            .unwrap()
            .unwrap();
        let original = receipt.sha256;
        drop(logger);
        let replacement = root.logger();
        assert_eq!(replacement.recover_orphans(&store).unwrap(), 1);
        assert_eq!(
            hash_reader(open_regular_file(&receipt.path).unwrap()).unwrap(),
            original
        );
        assert!(replacement.is_uncertain_run(&run.0));
        assert!(store.run_log_status(&run).unwrap().iter().any(|record| {
            record["status"] == "blocked"
                && record["detail"]["reason"] == "unreconciled_provider_result"
        }));
    }

    #[test]
    fn normal_stage_archiving_refuses_an_unaccounted_provider_result() {
        let root = TestRoot::new();
        let store = Store::open(root.0.join("store")).unwrap();
        let run = formal_fixture_run(&store);
        let logger = root.logger();
        logger.attach_store(store.clone()).unwrap();
        let scope = ModelAuditContext {
            run_id: run.0.clone(),
            phase: "T0".into(),
            task_id: Some("not-committed".into()),
            attempt_id: Some("not-accounted".into()),
            call_id: Some("known-response".into()),
            retry_index: Some(0),
        };
        logger.record(&scope, "llm.request", json!({})).unwrap();
        logger.record(&scope, "llm.terminal", json!({})).unwrap();
        let cursor = store.workflow_snapshot(&run).unwrap().event_cursor;
        assert!(logger
            .seal_with_store(&run, "T0", "failed", &store, 0, cursor)
            .is_err());
        assert!(logger.assert_available().is_err());
        let day = logger.root.join(Local::now().format("%Y%m%d").to_string());
        assert_eq!(fs::read_dir(day.join(".inflight")).unwrap().count(), 1);
        assert_eq!(
            fs::read_dir(day)
                .unwrap()
                .filter(|entry| entry
                    .as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|ext| ext == "zip"))
                .count(),
            0
        );
        drop(logger);
        assert_eq!(root.logger().recover_orphans(&store).unwrap(), 1);
    }

    #[test]
    fn recovery_does_not_claim_another_store_roots_open_segment() {
        let root = TestRoot::new();
        let first_store = Store::open(root.0.join("first-store")).unwrap();
        let second_store = Store::open(root.0.join("second-store")).unwrap();
        let run = formal_fixture_run(&first_store);
        let first = root.logger();
        first.attach_store(first_store.clone()).unwrap();
        first
            .record(
                &ModelAuditContext {
                    run_id: run.0.clone(),
                    phase: "T0".into(),
                    task_id: None,
                    attempt_id: None,
                    call_id: None,
                    retry_index: None,
                },
                "rust.run.submitted",
                json!({}),
            )
            .unwrap();
        let second = root.logger();
        assert_eq!(second.recover_orphans(&second_store).unwrap(), 0);
        assert!(first_store
            .run_log_status(&run)
            .unwrap()
            .iter()
            .any(|record| record["status"] == "opened"));
        assert!(!first_store
            .run_log_status(&run)
            .unwrap()
            .iter()
            .any(|record| record["status"] == "archived"));
        drop(first);
        assert_eq!(root.logger().recover_orphans(&first_store).unwrap(), 1);
    }
}
