//! SQLite control plane plus a content-addressed immutable blob store.
//!
//! `V2Store` is intentionally the only public persistence module.  Callers use
//! its transactional methods; SQL tables and blob paths stay implementation
//! details so daemon, runtime, and doctor cannot create competing state models.

use std::{
    fs,
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use akzio_domain::{
    canonical_json_bytes, AttemptId, BlobRef, ContentHash, DocumentId, DocumentKind,
    DocumentLifecycle, DocumentOrigin, DocumentRecord, EventEnvelope, FailureDisposition, LeaseId,
    Provenance, RunId, RunPurpose, TaskBudget, TaskId, TaskKind, TaskSpec, TaskStatus, TopologyId,
    WorkflowPlan, WorkflowStatus,
};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use thiserror::Error;
use uuid::Uuid;

mod rebuild;
pub use rebuild::*;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("io at {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("sqlite: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("invalid store root: {0}")]
    InvalidRoot(String),
    #[error("run {0} already exists")]
    DuplicateRun(RunId),
    #[error("task {0} already exists")]
    DuplicateTask(TaskId),
    #[error("task {0} does not exist")]
    UnknownTask(TaskId),
    #[error("task {task} lease or epoch no longer matches")]
    StaleLease { task: TaskId },
    #[error("blob {0} is missing or corrupt")]
    MissingBlob(ContentHash),
    #[error("document {0} does not exist")]
    UnknownDocument(DocumentId),
    #[error("document {0} already exists")]
    DuplicateDocument(DocumentId),
    #[error("contract {0} does not exist")]
    UnknownContract(ContentHash),
    #[error("run {0} has no persisted workflow plan")]
    MissingWorkflowPlan(RunId),
    #[error("daemon scheduler lease {0} is fenced")]
    SchedulerFenced(String),
}

pub type Result<T> = std::result::Result<T, StoreError>;

#[derive(Debug, Clone)]
pub struct StorePaths {
    pub root: PathBuf,
    pub database: PathBuf,
    pub blobs: PathBuf,
}

impl StorePaths {
    fn new(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        if root.as_os_str().is_empty() {
            return Err(StoreError::InvalidRoot("empty path".to_owned()));
        }
        Ok(Self {
            root: root.to_path_buf(),
            database: root.join("control.sqlite3"),
            blobs: root.join("blobs"),
        })
    }
}

#[derive(Debug, Clone)]
pub struct V2Store {
    paths: StorePaths,
    connection: Arc<Mutex<Connection>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub task_id: TaskId,
    pub lease_id: LeaseId,
    pub epoch: u64,
    pub worker_id: String,
    pub expires_at: DateTime<Utc>,
}

/// A fenced singleton lease for daemon-owned scheduling work. Task execution
/// remains multi-worker; this lease only elects the process allowed to run
/// recovery and future market-time scheduling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonLease {
    pub lease_name: String,
    pub owner_id: String,
    pub epoch: u64,
    pub expires_at: DateTime<Utc>,
}

/// A durable once-per-open-session Paper submission reservation. The workflow
/// plan is content-addressed before a run exists so a new leader can finish a
/// submission interrupted between reservation and workflow creation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaperScheduleSlot {
    pub session_key: String,
    pub run_id: RunId,
    pub plan: WorkflowPlan,
    pub plan_blob: BlobRef,
    pub scheduler_epoch: u64,
    pub created_at: DateTime<Utc>,
    pub submitted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaperScheduleReservation {
    pub slot: PaperScheduleSlot,
    pub newly_reserved: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionCommitmentState {
    Prepared,
    Submitted,
    Reconciled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCommitmentRecord {
    pub plan_hash: ContentHash,
    pub run_id: RunId,
    pub plan_document_id: DocumentId,
    pub state: ExecutionCommitmentState,
    pub commitment_document_id: Option<DocumentId>,
    pub submission_document_id: Option<DocumentId>,
    pub reconciliation_document_id: Option<DocumentId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCommitmentReservation {
    pub record: ExecutionCommitmentRecord,
    pub newly_reserved: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedTask {
    pub task_id: TaskId,
    pub run_id: RunId,
    pub kind: TaskKind,
    pub objective: String,
    pub contract_hash: Option<ContentHash>,
    pub on_failure: FailureDisposition,
    pub attempt_id: AttemptId,
    pub attempt: u8,
    pub max_attempts: u8,
    pub budget: TaskBudget,
    pub lease: Lease,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryTaskResult {
    Requeued,
    Terminal(TaskStatus),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEvent {
    pub cursor: i64,
    pub envelope: EventEnvelope,
}

impl V2Store {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let paths = StorePaths::new(root)?;
        fs::create_dir_all(&paths.blobs).map_err(|source| StoreError::Io {
            path: paths.blobs.clone(),
            source,
        })?;
        let connection = Connection::open(&paths.database)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        initialize(&connection)?;
        Ok(Self {
            paths,
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    pub fn paths(&self) -> &StorePaths {
        &self.paths
    }

    pub fn put_bytes(&self, bytes: &[u8], media_type: impl Into<String>) -> Result<BlobRef> {
        let hash = ContentHash::of_bytes(bytes);
        let relative = Path::new(&hash.as_str()[..2]).join(hash.as_str());
        let path = self.paths.blobs.join(relative);
        if !path.exists() {
            let parent = path.parent().expect("blob path has a parent");
            fs::create_dir_all(parent).map_err(|source| StoreError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
            let temporary = parent.join(format!(".{}.{}.tmp", hash.as_str(), Uuid::new_v4()));
            let mut file = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)
                .map_err(|source| StoreError::Io {
                    path: temporary.clone(),
                    source,
                })?;
            file.write_all(bytes).map_err(|source| StoreError::Io {
                path: temporary.clone(),
                source,
            })?;
            file.sync_all().map_err(|source| StoreError::Io {
                path: temporary.clone(),
                source,
            })?;
            match fs::rename(&temporary, &path) {
                Ok(()) => {}
                Err(source) if source.kind() == ErrorKind::AlreadyExists => {
                    let _ = fs::remove_file(&temporary);
                }
                Err(source) => {
                    let _ = fs::remove_file(&temporary);
                    return Err(StoreError::Io { path, source });
                }
            }
        }
        Ok(BlobRef {
            hash,
            media_type: media_type.into(),
            bytes: bytes.len() as u64,
        })
    }

    pub fn read_blob(&self, blob: &BlobRef) -> Result<Vec<u8>> {
        let path = self.blob_path(&blob.hash);
        let bytes = fs::read(&path).map_err(|source| StoreError::Io {
            path: path.clone(),
            source,
        })?;
        if ContentHash::of_bytes(&bytes) != blob.hash {
            return Err(StoreError::MissingBlob(blob.hash.clone()));
        }
        Ok(bytes)
    }

    pub fn register_document(&self, document: &DocumentRecord) -> Result<()> {
        document
            .validate()
            .map_err(|error| StoreError::InvalidRoot(error.to_string()))?;
        self.read_blob(&document.blob)?;
        let connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.unchecked_transaction()?;
        let provenance = serde_json::to_string(&document.provenance).map_err(|error| {
            StoreError::InvalidRoot(format!("serialize document provenance: {error}"))
        })?;
        let origin = serde_json::to_string(&document.origin).map_err(|error| {
            StoreError::InvalidRoot(format!("serialize document origin: {error}"))
        })?;
        let changed = transaction.execute(
            "INSERT OR IGNORE INTO documents (document_id, kind, blob_hash, media_type, bytes, producer, run_id, lifecycle, provenance_json, origin_json, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                document.document_id.0,
                enum_name(document.kind),
                document.blob.hash.as_str(),
                document.blob.media_type,
                document.blob.bytes,
                document.producer,
                document.run_id.as_ref().map(|id| id.0.as_str()),
                enum_name(document.lifecycle),
                provenance,
                origin,
                document.created_at.to_rfc3339(),
            ],
        )?;
        if changed == 0 {
            return Err(StoreError::DuplicateDocument(document.document_id.clone()));
        }
        for source in &document.source_refs {
            let source_exists: Option<String> = transaction
                .query_row(
                    "SELECT document_id FROM documents WHERE document_id = ?1",
                    params![source.0],
                    |row| row.get(0),
                )
                .optional()?;
            if source_exists.is_none() {
                return Err(StoreError::UnknownDocument(source.clone()));
            }
            transaction.execute(
                "INSERT INTO document_refs (document_id, source_document_id) VALUES (?1, ?2)",
                params![document.document_id.0, source.0],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn read_document(&self, document_id: &DocumentId) -> Result<DocumentRecord> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let row = connection
            .query_row(
            "SELECT kind, blob_hash, media_type, bytes, producer, run_id, lifecycle, provenance_json, origin_json, created_at
             FROM documents WHERE document_id = ?1",
                params![document_id.0],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, u64>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::UnknownDocument(document_id.clone()))?;
        let mut statement = connection.prepare(
            "SELECT source_document_id FROM document_refs WHERE document_id = ?1 ORDER BY source_document_id",
        )?;
        let source_refs = statement
            .query_map(params![document_id.0], |row| Ok(DocumentId(row.get(0)?)))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let provenance = serde_json::from_str::<Provenance>(&row.7).map_err(|error| {
            StoreError::InvalidRoot(format!("invalid document provenance: {error}"))
        })?;
        let origin = serde_json::from_str::<Option<DocumentOrigin>>(&row.8).map_err(|error| {
            StoreError::InvalidRoot(format!("invalid document origin: {error}"))
        })?;
        let created_at = DateTime::parse_from_rfc3339(&row.9)
            .map_err(|error| {
                StoreError::InvalidRoot(format!("invalid document timestamp: {error}"))
            })?
            .with_timezone(&Utc);
        Ok(DocumentRecord {
            document_id: document_id.clone(),
            kind: parse_document_kind(&row.0)?,
            blob: BlobRef {
                hash: ContentHash::new(row.1)
                    .map_err(|error| StoreError::InvalidRoot(error.to_string()))?,
                media_type: row.2,
                bytes: row.3,
            },
            producer: row.4,
            run_id: row.5.map(RunId),
            lifecycle: parse_document_lifecycle(&row.6)?,
            source_refs,
            provenance,
            origin,
            created_at,
        })
    }

    pub fn register_contract(&self, hash: &ContentHash, document_id: &DocumentId) -> Result<()> {
        self.read_document(document_id)?;
        let connection = self.connection.lock().expect("store connection poisoned");
        connection.execute(
            "INSERT OR IGNORE INTO contracts (contract_hash, document_id) VALUES (?1, ?2)",
            params![hash.as_str(), document_id.0],
        )?;
        Ok(())
    }

    pub fn contract_document(&self, hash: &ContentHash) -> Result<DocumentId> {
        let connection = self.connection.lock().expect("store connection poisoned");
        connection
            .query_row(
                "SELECT document_id FROM contracts WHERE contract_hash = ?1",
                params![hash.as_str()],
                |row| Ok(DocumentId(row.get(0)?)),
            )
            .optional()?
            .ok_or_else(|| StoreError::UnknownContract(hash.clone()))
    }

    pub fn documents_for_run(&self, run_id: &RunId) -> Result<Vec<DocumentRecord>> {
        let document_ids = {
            let connection = self.connection.lock().expect("store connection poisoned");
            let mut statement = connection.prepare(
                "SELECT document_id FROM documents WHERE run_id = ?1 ORDER BY created_at, document_id",
            )?;
            let rows = statement.query_map(params![run_id.0], |row| Ok(DocumentId(row.get(0)?)))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        document_ids
            .iter()
            .map(|document_id| self.read_document(document_id))
            .collect()
    }

    pub fn documents_by_kind(&self, kind: DocumentKind) -> Result<Vec<DocumentRecord>> {
        let document_ids = {
            let connection = self.connection.lock().expect("store connection poisoned");
            let mut statement = connection.prepare(
                "SELECT document_id FROM documents WHERE kind = ?1 ORDER BY created_at, document_id",
            )?;
            let rows =
                statement.query_map(params![enum_name(kind)], |row| Ok(DocumentId(row.get(0)?)))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        document_ids
            .iter()
            .map(|document_id| self.read_document(document_id))
            .collect()
    }

    pub fn verify_document_graph(&self) -> Result<()> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let mut statement = connection.prepare(
            "SELECT d.document_id, d.blob_hash, d.media_type, d.bytes
             FROM documents d
             LEFT JOIN documents source ON source.document_id = (
                 SELECT source_document_id FROM document_refs r WHERE r.document_id = d.document_id LIMIT 1
             )
             WHERE (SELECT COUNT(*) FROM document_refs r WHERE r.document_id = d.document_id)
                   != (SELECT COUNT(*) FROM document_refs r JOIN documents s ON s.document_id = r.source_document_id WHERE r.document_id = d.document_id)",
        )?;
        let mut rows = statement.query([])?;
        if let Some(row) = rows.next()? {
            return Err(StoreError::UnknownDocument(DocumentId(row.get(0)?)));
        }
        drop(rows);
        drop(statement);
        let mut documents =
            connection.prepare("SELECT blob_hash, media_type, bytes FROM documents")?;
        let rows = documents.query_map([], |row| {
            Ok(BlobRef {
                hash: ContentHash::new(row.get::<_, String>(0)?)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
                media_type: row.get(1)?,
                bytes: row.get(2)?,
            })
        })?;
        for blob in rows {
            self.read_blob(&blob?)?;
        }
        Ok(())
    }

    /// Validate both immutable evidence and the durable control plane. This is
    /// the only Store Doctor entry point used by operators and tests.
    pub fn verify_integrity(&self) -> Result<()> {
        self.verify_document_graph()?;
        let connection = self.connection.lock().expect("store connection poisoned");

        let mut foreign_keys = connection.prepare("PRAGMA foreign_key_check")?;
        if foreign_keys.query([])?.next()?.is_some() {
            return Err(StoreError::InvalidRoot(
                "foreign key check failed".to_owned(),
            ));
        }
        drop(foreign_keys);

        let orphan_child = connection
            .query_row(
                "SELECT links.child_run_id
                 FROM run_links AS links
                 LEFT JOIN runs AS child ON child.run_id = links.child_run_id
                 WHERE child.run_id IS NULL
                 LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if let Some(run_id) = orphan_child {
            return Err(StoreError::InvalidRoot(format!(
                "child run link points to missing run {run_id}"
            )));
        }
        let mut event_payloads = connection.prepare(
            "SELECT payload_hash, payload_media_type, payload_bytes
             FROM events WHERE payload_hash IS NOT NULL",
        )?;
        let rows = event_payloads.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<u64>>(2)?,
            ))
        })?;
        for row in rows {
            let (hash, media_type, bytes) = row?;
            let blob = BlobRef {
                hash: ContentHash::new(hash)
                    .map_err(|error| StoreError::InvalidRoot(error.to_string()))?,
                media_type: media_type.ok_or_else(|| {
                    StoreError::InvalidRoot("event payload media type missing".to_owned())
                })?,
                bytes: bytes.ok_or_else(|| {
                    StoreError::InvalidRoot("event payload bytes missing".to_owned())
                })?,
            };
            self.read_blob(&blob)?;
        }
        drop(event_payloads);

        let mismatch = connection
            .query_row(
                "SELECT task_id FROM tasks
                 WHERE attempt_count > max_attempts
                    OR attempt_count != (
                        SELECT COUNT(*) FROM task_attempts WHERE task_id = tasks.task_id
                    )
                 LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if let Some(task_id) = mismatch {
            return Err(StoreError::InvalidRoot(format!(
                "task attempt history is inconsistent for {task_id}"
            )));
        }

        let active_mismatch = connection
            .query_row(
                "SELECT task_id FROM tasks
                 WHERE (status IN ('leased', 'running') AND (
                        active_attempt_id IS NULL OR NOT EXISTS (
                            SELECT 1 FROM task_attempts attempt
                            WHERE attempt.attempt_id = tasks.active_attempt_id
                              AND attempt.finished_at IS NULL
                        )
                     ))
                    OR (status NOT IN ('leased', 'running') AND active_attempt_id IS NOT NULL)
                 LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if let Some(task_id) = active_mismatch {
            return Err(StoreError::InvalidRoot(format!(
                "task active attempt is inconsistent for {task_id}"
            )));
        }

        let orphan_event_attempt = connection
            .query_row(
                "SELECT event_id FROM events
                 WHERE attempt_id IS NOT NULL
                   AND NOT EXISTS (
                       SELECT 1 FROM task_attempts WHERE task_attempts.attempt_id = events.attempt_id
                   )
                 LIMIT 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        if let Some(event_id) = orphan_event_attempt {
            return Err(StoreError::InvalidRoot(format!(
                "event {event_id} references an unknown attempt"
            )));
        }

        let origin_rows = {
            let mut origins = connection.prepare(
                "SELECT document_id, origin_json FROM documents WHERE origin_json != 'null'",
            )?;
            let rows = origins
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            rows
        };
        for (document_id, encoded_origin) in origin_rows {
            let origin = serde_json::from_str::<Option<DocumentOrigin>>(&encoded_origin).map_err(
                |error| StoreError::InvalidRoot(format!("invalid document origin: {error}")),
            )?;
            let Some(origin) = origin else {
                continue;
            };
            if let Some(task_id) = origin.task_id {
                let exists = connection
                    .query_row(
                        "SELECT 1 FROM tasks WHERE task_id = ?1",
                        params![task_id.0],
                        |row| row.get::<_, i64>(0),
                    )
                    .optional()?;
                if exists.is_none() {
                    return Err(StoreError::InvalidRoot(format!(
                        "document {document_id} references an unknown task origin"
                    )));
                }
            }
            if let Some(attempt_id) = origin.attempt_id {
                let exists = connection
                    .query_row(
                        "SELECT 1 FROM task_attempts WHERE attempt_id = ?1",
                        params![attempt_id.0],
                        |row| row.get::<_, i64>(0),
                    )
                    .optional()?;
                if exists.is_none() {
                    return Err(StoreError::InvalidRoot(format!(
                        "document {document_id} references an unknown attempt origin"
                    )));
                }
            }
        }
        let invalid_commitment = connection
            .query_row(
                "SELECT plan_hash FROM execution_commitments
                 WHERE state NOT IN ('prepared', 'submitted', 'reconciled')
                    OR (state IN ('submitted', 'reconciled') AND submission_document_id IS NULL)
                    OR (state = 'reconciled' AND reconciliation_document_id IS NULL)
                 LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if let Some(plan_hash) = invalid_commitment {
            return Err(StoreError::InvalidRoot(format!(
                "invalid execution commitment {plan_hash}"
            )));
        }
        drop(connection);
        for slot in self.paper_schedule_slots()? {
            if slot.submitted_at.is_none() {
                continue;
            }
            if !self.run_exists(&slot.run_id)?
                || self.run_purpose(&slot.run_id)? != RunPurpose::Paper
                || self.run_topology_id(&slot.run_id)? != slot.plan.topology_id
                || self.workflow_plan_document(&slot.run_id).is_err()
            {
                return Err(StoreError::InvalidRoot(format!(
                    "submitted paper schedule slot {} is not backed by its Paper workflow",
                    slot.session_key
                )));
            }
        }
        Ok(())
    }

    pub fn create_run(
        &self,
        run_id: &RunId,
        purpose: RunPurpose,
        topology_id: &str,
        created_at: DateTime<Utc>,
    ) -> Result<()> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let changed = connection.execute(
            "INSERT OR IGNORE INTO runs (run_id, purpose, topology_id, status, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![run_id.0, enum_name(purpose), topology_id, enum_name(WorkflowStatus::Queued), created_at.to_rfc3339()],
        )?;
        if changed == 0 {
            return Err(StoreError::DuplicateRun(run_id.clone()));
        }
        Ok(())
    }

    pub fn write_workflow_plan(
        &self,
        run_id: &RunId,
        plan: &WorkflowPlan,
        prior_plan: Option<DocumentId>,
        origin: Option<DocumentOrigin>,
        created_at: DateTime<Utc>,
    ) -> Result<DocumentId> {
        let bytes = canonical_json_bytes(&serde_json::to_value(plan).map_err(|error| {
            StoreError::InvalidRoot(format!("serialize workflow plan: {error}"))
        })?)
        .map_err(|error| StoreError::InvalidRoot(format!("canonicalize workflow plan: {error}")))?;
        let blob = self.put_bytes(&bytes, "application/json")?;
        let document = DocumentRecord {
            document_id: DocumentId::new(),
            kind: DocumentKind::WorkflowPlan,
            blob,
            producer: "runtime.workflow".to_owned(),
            run_id: Some(run_id.clone()),
            lifecycle: DocumentLifecycle::RunScoped,
            source_refs: prior_plan.into_iter().collect(),
            provenance: Provenance::local("akzio.runtime", created_at),
            origin,
            created_at,
        };
        self.register_document(&document)?;
        let connection = self.connection.lock().expect("store connection poisoned");
        let changed = connection.execute(
            "UPDATE runs SET plan_document_id = ?1 WHERE run_id = ?2",
            params![document.document_id.0, run_id.0],
        )?;
        if changed == 0 {
            return Err(StoreError::InvalidRoot(format!(
                "run {run_id} does not exist"
            )));
        }
        Ok(document.document_id)
    }

    pub fn workflow_plan_document(&self, run_id: &RunId) -> Result<DocumentId> {
        let connection = self.connection.lock().expect("store connection poisoned");
        connection
            .query_row(
                "SELECT plan_document_id FROM runs WHERE run_id = ?1",
                params![run_id.0],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten()
            .map(DocumentId)
            .ok_or_else(|| StoreError::MissingWorkflowPlan(run_id.clone()))
    }

    pub fn workflow_plan(&self, run_id: &RunId) -> Result<WorkflowPlan> {
        let document_id = self.workflow_plan_document(run_id)?;
        let document = self.read_document(&document_id)?;
        let bytes = self.read_blob(&document.blob)?;
        serde_json::from_slice(&bytes)
            .map_err(|error| StoreError::InvalidRoot(format!("invalid workflow plan: {error}")))
    }

    pub fn run_purpose(&self, run_id: &RunId) -> Result<RunPurpose> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let value = connection
            .query_row(
                "SELECT purpose FROM runs WHERE run_id = ?1",
                params![run_id.0],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::InvalidRoot(format!("run {run_id} does not exist")))?;
        serde_json::from_value(serde_json::Value::String(value))
            .map_err(|error| StoreError::InvalidRoot(format!("invalid run purpose: {error}")))
    }

    pub fn run_topology_id(&self, run_id: &RunId) -> Result<TopologyId> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let value = connection
            .query_row(
                "SELECT topology_id FROM runs WHERE run_id = ?1",
                params![run_id.0],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::InvalidRoot(format!("run {run_id} does not exist")))?;
        Ok(TopologyId(value))
    }

    pub fn run_exists(&self, run_id: &RunId) -> Result<bool> {
        let connection = self.connection.lock().expect("store connection poisoned");
        Ok(connection
            .query_row(
                "SELECT 1 FROM runs WHERE run_id = ?1",
                params![run_id.0],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .is_some())
    }

    /// Atomically reserve one named child run. The caller may safely retry a
    /// crash after this reservation: the same child ID is returned.
    pub fn reserve_child_run(
        &self,
        parent_run_id: &RunId,
        relation: &str,
        proposed_child_run_id: &RunId,
        created_at: DateTime<Utc>,
    ) -> Result<(RunId, bool)> {
        if relation.trim().is_empty() {
            return Err(StoreError::InvalidRoot(
                "child run relation is empty".to_owned(),
            ));
        }
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let parent_exists = transaction
            .query_row(
                "SELECT 1 FROM runs WHERE run_id = ?1",
                params![parent_run_id.0],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .is_some();
        if !parent_exists {
            return Err(StoreError::InvalidRoot(format!(
                "run {parent_run_id} does not exist"
            )));
        }
        transaction.execute(
            "INSERT OR IGNORE INTO run_links (parent_run_id, relation, child_run_id, created_at) VALUES (?1, ?2, ?3, ?4)",
            params![
                parent_run_id.0,
                relation,
                proposed_child_run_id.0,
                created_at.to_rfc3339(),
            ],
        )?;
        let child = transaction.query_row(
            "SELECT child_run_id FROM run_links WHERE parent_run_id = ?1 AND relation = ?2",
            params![parent_run_id.0, relation],
            |row| row.get::<_, String>(0),
        )?;
        transaction.commit()?;
        Ok((RunId(child.clone()), child == proposed_child_run_id.0))
    }

    pub fn child_run(&self, parent_run_id: &RunId, relation: &str) -> Result<Option<RunId>> {
        let connection = self.connection.lock().expect("store connection poisoned");
        connection
            .query_row(
                "SELECT child_run_id FROM run_links WHERE parent_run_id = ?1 AND relation = ?2",
                params![parent_run_id.0, relation],
                |row| Ok(RunId(row.get(0)?)),
            )
            .optional()
            .map_err(StoreError::from)
    }

    pub fn parent_run(&self, child_run_id: &RunId, relation: &str) -> Result<Option<RunId>> {
        let connection = self.connection.lock().expect("store connection poisoned");
        connection
            .query_row(
                "SELECT parent_run_id FROM run_links WHERE child_run_id = ?1 AND relation = ?2",
                params![child_run_id.0, relation],
                |row| Ok(RunId(row.get(0)?)),
            )
            .optional()
            .map_err(StoreError::from)
    }

    pub fn enqueue_task(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
        kind: TaskKind,
        ready_at: DateTime<Utc>,
    ) -> Result<()> {
        self.enqueue_task_with_contract(
            run_id,
            task_id,
            kind,
            None,
            FailureDisposition::FailRun,
            ready_at,
        )
    }

    pub fn enqueue_task_with_contract(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
        kind: TaskKind,
        contract_hash: Option<&ContentHash>,
        on_failure: FailureDisposition,
        ready_at: DateTime<Utc>,
    ) -> Result<()> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let changed = connection.execute(
            "INSERT OR IGNORE INTO tasks (task_id, run_id, kind, contract_hash, status, ready_at, on_failure, epoch) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0)",
            params![task_id.0, run_id.0, enum_name(kind), contract_hash.map(ContentHash::as_str), enum_name(TaskStatus::Pending), ready_at.to_rfc3339(), enum_name(on_failure)],
        )?;
        if changed == 0 {
            return Err(StoreError::DuplicateTask(task_id.clone()));
        }
        Ok(())
    }

    pub fn enqueue_task_spec(
        &self,
        run_id: &RunId,
        task: &TaskSpec,
        ready_at: DateTime<Utc>,
    ) -> Result<()> {
        for document_id in &task.input_refs {
            self.read_document(document_id)?;
        }
        self.enqueue_task_with_contract(
            run_id,
            &task.task_id,
            task.kind,
            task.contract_hash.as_ref(),
            task.on_failure,
            ready_at,
        )?;
        let connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            "UPDATE tasks
             SET priority = ?1,
                 max_attempts = ?2,
                 parent_task_id = ?3,
                 objective = ?4,
                 max_input_tokens = ?5,
                 max_output_tokens = ?6,
                 max_wall_time_secs = ?7,
                 max_tool_calls = ?8,
                 on_failure = ?9
             WHERE task_id = ?10",
            params![
                task.priority,
                task.max_attempts,
                task.parent_task_id.as_ref().map(|id| id.0.as_str()),
                task.objective.as_str(),
                task.budget.max_input_tokens,
                task.budget.max_output_tokens,
                task.budget.max_wall_time_secs,
                task.budget.max_tool_calls,
                enum_name(task.on_failure),
                task.task_id.0,
            ],
        )?;
        for document_id in &task.input_refs {
            transaction.execute(
                "INSERT INTO task_inputs (task_id, document_id) VALUES (?1, ?2)",
                params![task.task_id.0, document_id.0],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn task_exists(&self, task_id: &TaskId) -> Result<bool> {
        let connection = self.connection.lock().expect("store connection poisoned");
        Ok(connection
            .query_row(
                "SELECT 1 FROM tasks WHERE task_id = ?1",
                params![task_id.0],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .is_some())
    }

    pub fn task_input_refs(&self, task_id: &TaskId) -> Result<Vec<DocumentId>> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let mut statement = connection.prepare(
            "SELECT document_id FROM task_inputs WHERE task_id = ?1 ORDER BY document_id",
        )?;
        let rows = statement.query_map(params![task_id.0], |row| Ok(DocumentId(row.get(0)?)))?;
        let documents = rows.collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(documents)
    }

    pub fn add_task_dependency(&self, task_id: &TaskId, depends_on: &TaskId) -> Result<()> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.unchecked_transaction()?;
        let task_exists: Option<String> = transaction
            .query_row(
                "SELECT task_id FROM tasks WHERE task_id = ?1",
                params![task_id.0],
                |row| row.get(0),
            )
            .optional()?;
        if task_exists.is_none() {
            return Err(StoreError::UnknownTask(task_id.clone()));
        }
        let dependency_exists: Option<String> = transaction
            .query_row(
                "SELECT task_id FROM tasks WHERE task_id = ?1",
                params![depends_on.0],
                |row| row.get(0),
            )
            .optional()?;
        if dependency_exists.is_none() {
            return Err(StoreError::UnknownTask(depends_on.clone()));
        }
        transaction.execute(
            "INSERT OR IGNORE INTO task_dependencies (task_id, depends_on_task_id) VALUES (?1, ?2)",
            params![task_id.0, depends_on.0],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Reclaim work abandoned by a crashed or partitioned worker. A lease can
    /// never be resumed: a later claimant receives a new epoch and attempt.
    pub fn recover_expired_tasks(&self, now: DateTime<Utc>) -> Result<u64> {
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let timestamp = now.to_rfc3339();
        let mut statement = transaction.prepare(
            "SELECT DISTINCT run_id FROM tasks
             WHERE status IN ('leased', 'running')
             AND lease_until IS NOT NULL
             AND lease_until <= ?1",
        )?;
        let affected_runs = statement
            .query_map(params![timestamp], |row| Ok(RunId(row.get(0)?)))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(statement);
        transaction.execute(
            "UPDATE task_attempts
             SET status = 'abandoned', finished_at = ?1
             WHERE finished_at IS NULL
               AND task_id IN (
                   SELECT task_id FROM tasks
                   WHERE status IN ('leased', 'running')
                     AND lease_until IS NOT NULL
                     AND lease_until <= ?1
               )",
            params![timestamp],
        )?;
        let recovered = transaction.execute(
            "UPDATE tasks
             SET status = CASE
                 WHEN cancel_requested = 1 THEN 'cancelled'
                 WHEN attempt_count >= max_attempts AND on_failure = 'skip_task' THEN 'skipped'
                 WHEN attempt_count >= max_attempts THEN 'failed'
                 ELSE 'pending'
             END,
                 lease_id = NULL,
                 lease_worker = NULL,
                 lease_until = NULL,
                 active_attempt_id = NULL,
                 finished_at = CASE
                     WHEN cancel_requested = 1 OR attempt_count >= max_attempts THEN ?1
                     ELSE finished_at
                 END
             WHERE status IN ('leased', 'running')
               AND lease_until IS NOT NULL
               AND lease_until <= ?1",
            params![now.to_rfc3339()],
        )?;
        transaction.commit()?;
        drop(connection);
        for run_id in affected_runs {
            self.refresh_run_status(&run_id, now)?;
        }
        Ok(recovered as u64)
    }

    /// Atomically elect one daemon scheduler. A newly acquired lease always
    /// receives a higher epoch than the prior owner, fencing stale leaders.
    pub fn acquire_daemon_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
        now: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<Option<DaemonLease>> {
        if lease_name.trim().is_empty() || owner_id.trim().is_empty() || expires_at <= now {
            return Err(StoreError::InvalidRoot(
                "invalid daemon lease request".to_owned(),
            ));
        }
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = transaction
            .query_row(
                "SELECT owner_id, epoch, expires_at FROM daemon_leases WHERE lease_name = ?1",
                params![lease_name],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, u64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?;
        let lease = match current {
            None => {
                transaction.execute(
                    "INSERT INTO daemon_leases (lease_name, owner_id, epoch, expires_at, heartbeat_at)
                     VALUES (?1, ?2, 1, ?3, ?4)",
                    params![
                        lease_name,
                        owner_id,
                        expires_at.to_rfc3339(),
                        now.to_rfc3339(),
                    ],
                )?;
                DaemonLease {
                    lease_name: lease_name.to_owned(),
                    owner_id: owner_id.to_owned(),
                    epoch: 1,
                    expires_at,
                }
            }
            Some((current_owner, epoch, current_expiry)) => {
                let current_expiry = parse_daemon_lease_time(&current_expiry)?;
                if current_owner != owner_id && current_expiry > now {
                    transaction.commit()?;
                    return Ok(None);
                }
                let epoch = if current_owner == owner_id && current_expiry > now {
                    epoch
                } else {
                    epoch.saturating_add(1)
                };
                transaction.execute(
                    "UPDATE daemon_leases
                     SET owner_id = ?1, epoch = ?2, expires_at = ?3, heartbeat_at = ?4
                     WHERE lease_name = ?5",
                    params![
                        owner_id,
                        epoch,
                        expires_at.to_rfc3339(),
                        now.to_rfc3339(),
                        lease_name,
                    ],
                )?;
                DaemonLease {
                    lease_name: lease_name.to_owned(),
                    owner_id: owner_id.to_owned(),
                    epoch,
                    expires_at,
                }
            }
        };
        transaction.commit()?;
        Ok(Some(lease))
    }

    /// Returns false when a newer owner has replaced this scheduler.
    pub fn heartbeat_daemon_lease(
        &self,
        lease: &DaemonLease,
        now: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<bool> {
        if expires_at <= now {
            return Err(StoreError::InvalidRoot(
                "daemon lease heartbeat must extend expiry".to_owned(),
            ));
        }
        let connection = self.connection.lock().expect("store connection poisoned");
        let changed = connection.execute(
            "UPDATE daemon_leases
             SET expires_at = ?1, heartbeat_at = ?2
             WHERE lease_name = ?3 AND owner_id = ?4 AND epoch = ?5 AND expires_at > ?2",
            params![
                expires_at.to_rfc3339(),
                now.to_rfc3339(),
                lease.lease_name,
                lease.owner_id,
                lease.epoch,
            ],
        )?;
        Ok(changed == 1)
    }

    pub fn release_daemon_lease(&self, lease: &DaemonLease) -> Result<bool> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let changed = connection.execute(
            "DELETE FROM daemon_leases
             WHERE lease_name = ?1 AND owner_id = ?2 AND epoch = ?3",
            params![lease.lease_name, lease.owner_id, lease.epoch],
        )?;
        Ok(changed == 1)
    }

    pub fn daemon_lease(&self, lease_name: &str) -> Result<Option<DaemonLease>> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let row = connection
            .query_row(
                "SELECT owner_id, epoch, expires_at FROM daemon_leases WHERE lease_name = ?1",
                params![lease_name],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, u64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?;
        row.map(|(owner_id, epoch, expires_at)| {
            Ok(DaemonLease {
                lease_name: lease_name.to_owned(),
                owner_id,
                epoch,
                expires_at: parse_daemon_lease_time(&expires_at)?,
            })
        })
        .transpose()
    }

    /// Reserve a single Paper run for a broker-open market session. The plan
    /// blob is written before the SQLite transaction, then referenced by the
    /// reservation so a successor can resume the exact same task IDs.
    pub fn reserve_paper_schedule_slot(
        &self,
        lease: &DaemonLease,
        session_key: &str,
        proposed_run_id: &RunId,
        plan: &WorkflowPlan,
        now: DateTime<Utc>,
    ) -> Result<PaperScheduleReservation> {
        if session_key.trim().is_empty() {
            return Err(StoreError::InvalidRoot(
                "paper schedule session key is empty".to_owned(),
            ));
        }
        plan.validate()
            .map_err(|error| StoreError::InvalidRoot(error.to_string()))?;
        let plan_bytes = canonical_json_bytes(&serde_json::to_value(plan).map_err(|error| {
            StoreError::InvalidRoot(format!("serialize schedule plan: {error}"))
        })?)
        .map_err(|error| StoreError::InvalidRoot(format!("canonicalize schedule plan: {error}")))?;
        let plan_blob = self.put_bytes(&plan_bytes, "application/json")?;

        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = transaction
            .query_row(
                "SELECT owner_id, epoch, expires_at FROM daemon_leases WHERE lease_name = ?1",
                params![lease.lease_name],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, u64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?;
        let current =
            current.ok_or_else(|| StoreError::SchedulerFenced(lease.lease_name.clone()))?;
        if current.0 != lease.owner_id
            || current.1 != lease.epoch
            || parse_daemon_lease_time(&current.2)? <= now
        {
            return Err(StoreError::SchedulerFenced(lease.lease_name.clone()));
        }
        let newly_reserved = transaction.execute(
            "INSERT OR IGNORE INTO paper_schedule_slots
             (session_key, run_id, plan_blob_hash, plan_media_type, plan_bytes, scheduler_epoch, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                session_key,
                proposed_run_id.0,
                plan_blob.hash.as_str(),
                plan_blob.media_type,
                plan_blob.bytes,
                lease.epoch,
                now.to_rfc3339(),
            ],
        )? == 1;
        transaction.commit()?;
        let slot = self
            .paper_schedule_slot(session_key)?
            .ok_or_else(|| StoreError::InvalidRoot("paper schedule slot disappeared".to_owned()))?;
        Ok(PaperScheduleReservation {
            slot,
            newly_reserved,
        })
    }

    /// Mark a reserved slot as submitted only while the same scheduler epoch
    /// remains leader. Repeated recovery calls are intentionally idempotent.
    pub fn mark_paper_schedule_submitted(
        &self,
        lease: &DaemonLease,
        session_key: &str,
        run_id: &RunId,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = transaction
            .query_row(
                "SELECT owner_id, epoch, expires_at FROM daemon_leases WHERE lease_name = ?1",
                params![lease.lease_name],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, u64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?;
        let current =
            current.ok_or_else(|| StoreError::SchedulerFenced(lease.lease_name.clone()))?;
        if current.0 != lease.owner_id
            || current.1 != lease.epoch
            || parse_daemon_lease_time(&current.2)? <= now
        {
            return Err(StoreError::SchedulerFenced(lease.lease_name.clone()));
        }
        let marked = transaction.execute(
            "UPDATE paper_schedule_slots
             SET submitted_at = ?1
             WHERE session_key = ?2 AND run_id = ?3 AND submitted_at IS NULL",
            params![now.to_rfc3339(), session_key, run_id.0],
        )? == 1;
        transaction.commit()?;
        Ok(marked)
    }

    pub fn paper_schedule_slot(&self, session_key: &str) -> Result<Option<PaperScheduleSlot>> {
        let row = {
            let connection = self.connection.lock().expect("store connection poisoned");
            connection
                .query_row(
                    "SELECT session_key, run_id, plan_blob_hash, plan_media_type, plan_bytes,
                            scheduler_epoch, created_at, submitted_at
                     FROM paper_schedule_slots WHERE session_key = ?1",
                    params![session_key],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, u64>(4)?,
                            row.get::<_, u64>(5)?,
                            row.get::<_, String>(6)?,
                            row.get::<_, Option<String>>(7)?,
                        ))
                    },
                )
                .optional()?
        };
        row.map(|row| self.decode_paper_schedule_slot(row))
            .transpose()
    }

    pub fn paper_schedule_slots(&self) -> Result<Vec<PaperScheduleSlot>> {
        let session_keys = {
            let connection = self.connection.lock().expect("store connection poisoned");
            let mut statement = connection
                .prepare("SELECT session_key FROM paper_schedule_slots ORDER BY session_key")?;
            let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        session_keys
            .iter()
            .map(|session_key| {
                self.paper_schedule_slot(session_key)?.ok_or_else(|| {
                    StoreError::InvalidRoot("paper schedule slot disappeared".to_owned())
                })
            })
            .collect()
    }

    fn decode_paper_schedule_slot(
        &self,
        row: (
            String,
            String,
            String,
            String,
            u64,
            u64,
            String,
            Option<String>,
        ),
    ) -> Result<PaperScheduleSlot> {
        let (
            session_key,
            run_id,
            hash,
            media_type,
            bytes,
            scheduler_epoch,
            created_at,
            submitted_at,
        ) = row;
        if scheduler_epoch == 0 {
            return Err(StoreError::InvalidRoot(format!(
                "paper schedule slot {session_key} has epoch zero"
            )));
        }
        let plan_blob = BlobRef {
            hash: ContentHash::new(hash)
                .map_err(|error| StoreError::InvalidRoot(error.to_string()))?,
            media_type,
            bytes,
        };
        let plan = serde_json::from_slice::<WorkflowPlan>(&self.read_blob(&plan_blob)?).map_err(
            |error| StoreError::InvalidRoot(format!("invalid paper schedule plan: {error}")),
        )?;
        plan.validate()
            .map_err(|error| StoreError::InvalidRoot(error.to_string()))?;
        Ok(PaperScheduleSlot {
            session_key,
            run_id: RunId(run_id),
            plan,
            plan_blob,
            scheduler_epoch,
            created_at: parse_daemon_lease_time(&created_at)?,
            submitted_at: submitted_at
                .as_deref()
                .map(parse_daemon_lease_time)
                .transpose()?,
        })
    }

    /// Reserve the single canonical Paper commitment for an immutable plan.
    /// Retries may resume a prepared commitment, but a second run with the
    /// same plan hash cannot create another broker-visible submission.
    pub fn reserve_execution_commitment(
        &self,
        run_id: &RunId,
        plan_document_id: &DocumentId,
        plan_hash: &ContentHash,
        created_at: DateTime<Utc>,
    ) -> Result<ExecutionCommitmentReservation> {
        self.read_document(plan_document_id)?;
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let newly_reserved = transaction.execute(
            "INSERT OR IGNORE INTO execution_commitments (
                plan_hash, run_id, plan_document_id, state, created_at, updated_at
             ) VALUES (?1, ?2, ?3, 'prepared', ?4, ?4)",
            params![
                plan_hash.as_str(),
                run_id.0,
                plan_document_id.0,
                created_at.to_rfc3339(),
            ],
        )? == 1;
        let row = transaction
            .query_row(
                "SELECT plan_hash, run_id, plan_document_id, state,
                        commitment_document_id, submission_document_id, reconciliation_document_id
                 FROM execution_commitments WHERE plan_hash = ?1",
                params![plan_hash.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| {
                StoreError::InvalidRoot("execution commitment disappeared".to_owned())
            })?;
        transaction.commit()?;
        Ok(ExecutionCommitmentReservation {
            record: execution_commitment_record(row)?,
            newly_reserved,
        })
    }

    pub fn attach_execution_commitment_document(
        &self,
        plan_hash: &ContentHash,
        commitment_document_id: &DocumentId,
        updated_at: DateTime<Utc>,
    ) -> Result<()> {
        self.read_document(commitment_document_id)?;
        let connection = self.connection.lock().expect("store connection poisoned");
        let changed = connection.execute(
            "UPDATE execution_commitments
             SET commitment_document_id = COALESCE(commitment_document_id, ?1), updated_at = ?2
             WHERE plan_hash = ?3
               AND (commitment_document_id IS NULL OR commitment_document_id = ?1)",
            params![
                commitment_document_id.0,
                updated_at.to_rfc3339(),
                plan_hash.as_str(),
            ],
        )?;
        (changed == 1).then_some(()).ok_or_else(|| {
            StoreError::InvalidRoot("execution commitment document conflict".to_owned())
        })
    }

    pub fn mark_execution_submitted(
        &self,
        plan_hash: &ContentHash,
        submission_document_id: &DocumentId,
        updated_at: DateTime<Utc>,
    ) -> Result<()> {
        self.read_document(submission_document_id)?;
        let connection = self.connection.lock().expect("store connection poisoned");
        let changed = connection.execute(
            "UPDATE execution_commitments
             SET submission_document_id = COALESCE(submission_document_id, ?1),
                 state = CASE WHEN state = 'reconciled' THEN state ELSE 'submitted' END,
                 updated_at = ?2
             WHERE plan_hash = ?3
               AND (submission_document_id IS NULL OR submission_document_id = ?1)",
            params![
                submission_document_id.0,
                updated_at.to_rfc3339(),
                plan_hash.as_str(),
            ],
        )?;
        (changed == 1).then_some(()).ok_or_else(|| {
            StoreError::InvalidRoot("execution submission commitment conflict".to_owned())
        })
    }

    pub fn mark_execution_reconciled(
        &self,
        plan_hash: &ContentHash,
        reconciliation_document_id: &DocumentId,
        updated_at: DateTime<Utc>,
    ) -> Result<()> {
        self.read_document(reconciliation_document_id)?;
        let connection = self.connection.lock().expect("store connection poisoned");
        let changed = connection.execute(
            "UPDATE execution_commitments
             SET reconciliation_document_id = COALESCE(reconciliation_document_id, ?1),
                 state = 'reconciled', updated_at = ?2
             WHERE plan_hash = ?3
               AND (reconciliation_document_id IS NULL OR reconciliation_document_id = ?1)",
            params![
                reconciliation_document_id.0,
                updated_at.to_rfc3339(),
                plan_hash.as_str(),
            ],
        )?;
        (changed == 1).then_some(()).ok_or_else(|| {
            StoreError::InvalidRoot("execution reconciliation commitment conflict".to_owned())
        })
    }

    pub fn execution_commitment(
        &self,
        plan_hash: &ContentHash,
    ) -> Result<Option<ExecutionCommitmentRecord>> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let row = connection
            .query_row(
                "SELECT plan_hash, run_id, plan_document_id, state,
                        commitment_document_id, submission_document_id, reconciliation_document_id
                 FROM execution_commitments WHERE plan_hash = ?1",
                params![plan_hash.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                    ))
                },
            )
            .optional()?;
        row.map(execution_commitment_record).transpose()
    }

    pub fn claim_next_task(
        &self,
        worker_id: &str,
        now: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<Option<ClaimedTask>> {
        self.recover_expired_tasks(now)?;
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let candidate = transaction
            .query_row(
            "SELECT task_id, run_id, kind, objective, contract_hash, epoch, attempt_count, max_attempts,
                    on_failure, max_input_tokens, max_output_tokens, max_wall_time_secs, max_tool_calls
                 FROM tasks
            WHERE ((status = 'pending' AND ready_at <= ?1
                        AND NOT EXISTS (
                          SELECT 1 FROM task_dependencies dependency
                          JOIN tasks prerequisite ON prerequisite.task_id = dependency.depends_on_task_id
                          WHERE dependency.task_id = tasks.task_id
                     AND prerequisite.status NOT IN ('succeeded', 'failed', 'cancelled', 'skipped')
                        ))
            OR (status IN ('leased', 'running') AND lease_until <= ?1))
            AND attempt_count < max_attempts
               AND cancel_requested = 0
               AND NOT EXISTS (
                   SELECT 1 FROM runs
                   WHERE runs.run_id = tasks.run_id AND runs.status IN ('failed', 'cancelled')
               )
                 ORDER BY priority DESC, ready_at, task_id
                 LIMIT 1",
                params![now.to_rfc3339()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                    row.get::<_, u64>(5)?,
                    row.get::<_, u8>(6)?,
                    row.get::<_, u8>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, u32>(9)?,
                    row.get::<_, u32>(10)?,
                    row.get::<_, u32>(11)?,
                    row.get::<_, u16>(12)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            task_id,
            run_id,
            kind,
            objective,
            contract_hash,
            prior_epoch,
            attempt_count,
            max_attempts,
            on_failure,
            max_input_tokens,
            max_output_tokens,
            max_wall_time_secs,
            max_tool_calls,
        )) = candidate
        else {
            transaction.commit()?;
            return Ok(None);
        };
        let lease_id = LeaseId::new();
        let epoch = prior_epoch + 1;
        let attempt_id = AttemptId::new();
        let attempt = attempt_count.saturating_add(1);
        transaction.execute(
            "UPDATE tasks SET status = 'leased', lease_id = ?1, lease_worker = ?2, lease_until = ?3,
             epoch = ?4, attempt_count = ?5, active_attempt_id = ?6 WHERE task_id = ?7",
            params![
                lease_id.0,
                worker_id,
                expires_at.to_rfc3339(),
                epoch,
                attempt,
                attempt_id.0,
                task_id,
            ],
        )?;
        transaction.execute(
            "INSERT INTO task_attempts (attempt_id, task_id, run_id, attempt, lease_id, worker_id, status, started_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'leased', ?7)",
            params![
                attempt_id.0,
                task_id,
                run_id,
                attempt,
                lease_id.0,
                worker_id,
                now.to_rfc3339(),
            ],
        )?;
        transaction.commit()?;
        let task_id = TaskId(task_id);
        Ok(Some(ClaimedTask {
            task_id: task_id.clone(),
            run_id: RunId(run_id),
            kind: parse_task_kind(&kind)?,
            objective,
            contract_hash: contract_hash
                .map(ContentHash::new)
                .transpose()
                .map_err(|error| StoreError::InvalidRoot(error.to_string()))?,
            on_failure: parse_failure_disposition(&on_failure)?,
            attempt_id,
            attempt,
            max_attempts,
            budget: TaskBudget {
                max_input_tokens,
                max_output_tokens,
                max_wall_time_secs,
                max_tool_calls,
            },
            lease: Lease {
                task_id,
                lease_id,
                epoch,
                worker_id: worker_id.to_owned(),
                expires_at,
            },
        }))
    }

    pub fn start_task(&self, lease: &Lease, started_at: DateTime<Utc>) -> Result<()> {
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "UPDATE tasks SET status = 'running'
             WHERE task_id = ?1 AND lease_id = ?2 AND epoch = ?3
               AND status = 'leased' AND cancel_requested = 0",
            params![lease.task_id.0, lease.lease_id.0, lease.epoch],
        )?;
        if changed == 0 {
            return Err(StoreError::StaleLease {
                task: lease.task_id.clone(),
            });
        }
        transaction.execute(
            "UPDATE task_attempts SET status = 'running', started_at = ?1
             WHERE task_id = ?2 AND lease_id = ?3 AND finished_at IS NULL",
            params![started_at.to_rfc3339(), lease.task_id.0, lease.lease_id.0,],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn heartbeat(&self, lease: &Lease, expires_at: DateTime<Utc>) -> Result<()> {
        self.update_lease(
            lease,
            "UPDATE tasks SET lease_until = ?1 WHERE task_id = ?2 AND lease_id = ?3 AND epoch = ?4",
            params![
                expires_at.to_rfc3339(),
                lease.task_id.0,
                lease.lease_id.0,
                lease.epoch
            ],
        )
    }

    pub fn complete_task(
        &self,
        lease: &Lease,
        status: TaskStatus,
        on_failure: FailureDisposition,
    ) -> Result<TaskStatus> {
        debug_assert!(status.is_terminal());
        let status = match (status, on_failure) {
            (TaskStatus::Failed, FailureDisposition::SkipTask) => TaskStatus::Skipped,
            _ => status,
        };
        let finished_at = Utc::now();
        self.update_lease(
            lease,
            "UPDATE tasks
             SET status = ?1,
                 finished_at = ?2,
                 lease_id = NULL,
                 lease_worker = NULL,
                 lease_until = NULL,
                 active_attempt_id = NULL
             WHERE task_id = ?3 AND lease_id = ?4 AND epoch = ?5",
            params![
                enum_name(status),
                finished_at.to_rfc3339(),
                lease.task_id.0,
                lease.lease_id.0,
                lease.epoch
            ],
        )?;
        let connection = self.connection.lock().expect("store connection poisoned");
        connection.execute(
            "UPDATE task_attempts SET status = ?1, finished_at = ?2
             WHERE task_id = ?3 AND lease_id = ?4 AND finished_at IS NULL",
            params![
                enum_name(status),
                finished_at.to_rfc3339(),
                lease.task_id.0,
                lease.lease_id.0,
            ],
        )?;
        Ok(status)
    }

    /// Close the current attempt and either put the task back in the queue or
    /// fail it once its contract budget is exhausted. The caller supplies the
    /// next ready time so retry policy remains Rust-owned above the Store.
    pub fn retry_task(
        &self,
        lease: &Lease,
        ready_at: DateTime<Utc>,
        error: Option<&BlobRef>,
        on_failure: FailureDisposition,
    ) -> Result<RetryTaskResult> {
        if let Some(error) = error {
            self.read_blob(error)?;
        }
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (attempt_count, max_attempts, cancel_requested): (u8, u8, i64) = transaction
            .query_row(
                "SELECT attempt_count, max_attempts, cancel_requested FROM tasks
                 WHERE task_id = ?1 AND lease_id = ?2 AND epoch = ?3",
                params![lease.task_id.0, lease.lease_id.0, lease.epoch],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?
            .ok_or_else(|| StoreError::StaleLease {
                task: lease.task_id.clone(),
            })?;
        let finished_at = Utc::now().to_rfc3339();
        let error_hash = error.map(|blob| blob.hash.as_str());
        let requeued = cancel_requested == 0 && attempt_count < max_attempts;
        let status = if requeued {
            TaskStatus::Pending
        } else if cancel_requested != 0 {
            TaskStatus::Cancelled
        } else if on_failure == FailureDisposition::SkipTask {
            TaskStatus::Skipped
        } else {
            TaskStatus::Failed
        };
        transaction.execute(
            "UPDATE tasks
             SET status = ?1,
                 ready_at = ?2,
                 lease_id = NULL,
                 lease_worker = NULL,
                 lease_until = NULL,
                 active_attempt_id = NULL,
                 last_error_blob_hash = ?3,
                 finished_at = CASE WHEN ?4 = 1 THEN ?5 ELSE finished_at END
             WHERE task_id = ?6 AND lease_id = ?7 AND epoch = ?8",
            params![
                enum_name(status),
                ready_at.to_rfc3339(),
                error_hash,
                (!requeued) as i64,
                finished_at,
                lease.task_id.0,
                lease.lease_id.0,
                lease.epoch,
            ],
        )?;
        transaction.execute(
            "UPDATE task_attempts SET status = ?1, finished_at = ?2, error_blob_hash = ?3
             WHERE task_id = ?4 AND lease_id = ?5 AND finished_at IS NULL",
            params![
                if requeued {
                    "retrying"
                } else {
                    match status {
                        TaskStatus::Cancelled => "cancelled",
                        TaskStatus::Skipped => "skipped",
                        _ => "failed",
                    }
                },
                Utc::now().to_rfc3339(),
                error_hash,
                lease.task_id.0,
                lease.lease_id.0,
            ],
        )?;
        transaction.commit()?;
        Ok(if requeued {
            RetryTaskResult::Requeued
        } else {
            RetryTaskResult::Terminal(status)
        })
    }

    pub fn cancel_run(&self, run_id: &RunId, now: DateTime<Utc>) -> Result<u64> {
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let exists = transaction
            .query_row(
                "SELECT 1 FROM runs WHERE run_id = ?1",
                params![run_id.0],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        if exists.is_none() {
            return Err(StoreError::InvalidRoot(format!(
                "run {run_id} does not exist"
            )));
        }
        transaction.execute(
            "UPDATE runs SET cancel_requested = 1 WHERE run_id = ?1",
            params![run_id.0],
        )?;
        transaction.execute(
            "UPDATE tasks SET cancel_requested = 1 WHERE run_id = ?1",
            params![run_id.0],
        )?;
        let cancelled = transaction.execute(
            "UPDATE tasks
             SET status = 'cancelled', finished_at = ?1
             WHERE run_id = ?2 AND status = 'pending'",
            params![now.to_rfc3339(), run_id.0],
        )?;
        transaction.commit()?;
        drop(connection);
        self.refresh_run_status(run_id, now)?;
        Ok(cancelled as u64)
    }

    pub fn run_cancel_requested(&self, run_id: &RunId) -> Result<bool> {
        let connection = self.connection.lock().expect("store connection poisoned");
        connection
            .query_row(
                "SELECT cancel_requested != 0 OR status = 'failed' FROM runs WHERE run_id = ?1",
                params![run_id.0],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .map(|value| value != 0)
            .ok_or_else(|| StoreError::InvalidRoot(format!("run {run_id} does not exist")))
    }

    pub fn run_status(&self, run_id: &RunId) -> Result<WorkflowStatus> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let status = connection
            .query_row(
                "SELECT status FROM runs WHERE run_id = ?1",
                params![run_id.0],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::InvalidRoot(format!("run {run_id} does not exist")))?;
        parse_workflow_status(&status)
    }

    /// Reduce durable task state into the run state. Optional research is
    /// allowed to fail; a required failure cancels work that cannot yield a
    /// valid decision.
    pub fn refresh_run_status(&self, run_id: &RunId, now: DateTime<Utc>) -> Result<WorkflowStatus> {
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let cancelled = transaction
            .query_row(
                "SELECT cancel_requested FROM runs WHERE run_id = ?1",
                params![run_id.0],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::InvalidRoot(format!("run {run_id} does not exist")))?
            != 0;
        let mut statement =
            transaction.prepare("SELECT kind, status, on_failure FROM tasks WHERE run_id = ?1")?;
        let rows = statement.query_map(params![run_id.0], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let tasks = rows.collect::<std::result::Result<Vec<_>, _>>()?;
        drop(statement);

        let mut pending = false;
        let mut leased = false;
        let mut running = false;
        let mut fail_run = false;
        let mut decision_completed = false;
        for (kind, status, on_failure) in tasks {
            match parse_task_status(&status)? {
                TaskStatus::Pending => pending = true,
                TaskStatus::Leased => leased = true,
                TaskStatus::Running => running = true,
                TaskStatus::Succeeded => {
                    decision_completed |= parse_task_kind(&kind)? == TaskKind::DecisionGate;
                }
                TaskStatus::Failed
                    if parse_failure_disposition(&on_failure)? == FailureDisposition::FailRun =>
                {
                    fail_run = true;
                }
                TaskStatus::Failed | TaskStatus::Cancelled | TaskStatus::Skipped => {}
            }
        }

        let status = if fail_run {
            transaction.execute(
                "UPDATE tasks
                 SET status = 'cancelled', finished_at = ?1, cancel_requested = 1
                 WHERE run_id = ?2 AND status = 'pending'",
                params![now.to_rfc3339(), run_id.0],
            )?;
            transaction.execute(
                "UPDATE tasks SET cancel_requested = 1
                 WHERE run_id = ?1 AND status IN ('leased', 'running')",
                params![run_id.0],
            )?;
            WorkflowStatus::Failed
        } else if cancelled {
            WorkflowStatus::Cancelled
        } else if running {
            WorkflowStatus::Running
        } else if leased {
            WorkflowStatus::Leased
        } else if pending && decision_completed {
            WorkflowStatus::DecisionCompleted
        } else if pending {
            WorkflowStatus::Queued
        } else {
            WorkflowStatus::Completed
        };
        let finished_at = matches!(
            status,
            WorkflowStatus::Completed | WorkflowStatus::Failed | WorkflowStatus::Cancelled
        )
        .then(|| now.to_rfc3339());
        transaction.execute(
            "UPDATE runs SET status = ?1, finished_at = ?2 WHERE run_id = ?3",
            params![enum_name(status), finished_at, run_id.0],
        )?;
        transaction.commit()?;
        Ok(status)
    }

    pub fn append_event(&self, event: &EventEnvelope) -> Result<i64> {
        event
            .validate()
            .map_err(|error| StoreError::InvalidRoot(error.to_string()))?;
        if let Some(document_id) = &event.payload_document_id {
            self.read_document(document_id)?;
        }
        let connection = self.connection.lock().expect("store connection poisoned");
        connection.execute(
            "INSERT INTO events (run_id, task_id, attempt_id, contract_hash, causation_id, event_type, payload_document_id, payload_hash, payload_media_type, payload_bytes, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                event.run_id.0,
                event.task_id.as_ref().map(|id| id.0.as_str()),
                event.attempt_id.as_ref().map(|id| id.0.as_str()),
                event.contract_hash.as_ref().map(ContentHash::as_str),
                event.causation_id,
                event.event_type,
                event.payload_document_id.as_ref().map(|id| id.0.as_str()),
                event.payload.as_ref().map(|blob| blob.hash.as_str()),
                event.payload.as_ref().map(|blob| blob.media_type.as_str()),
                event.payload.as_ref().map(|blob| blob.bytes),
                event.created_at.to_rfc3339(),
            ],
        )?;
        Ok(connection.last_insert_rowid())
    }

    pub fn event_count(&self, run_id: &RunId) -> Result<u64> {
        let connection = self.connection.lock().expect("store connection poisoned");
        connection
            .query_row(
                "SELECT COUNT(*) FROM events WHERE run_id = ?1",
                params![run_id.0],
                |row| row.get(0),
            )
            .map_err(StoreError::from)
    }

    pub fn events_after(
        &self,
        run_id: &RunId,
        after: i64,
        limit: usize,
    ) -> Result<Vec<StoredEvent>> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let mut statement = connection.prepare(
            "SELECT event_id, task_id, attempt_id, contract_hash, causation_id, event_type,
                    payload_document_id, payload_hash, payload_media_type, payload_bytes, created_at
             FROM events WHERE run_id = ?1 AND event_id > ?2
             ORDER BY event_id LIMIT ?3",
        )?;
        let rows = statement.query_map(params![run_id.0, after, limit as i64], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<u64>>(9)?,
                row.get::<_, String>(10)?,
            ))
        })?;
        rows.map(|row| {
            let (
                cursor,
                task_id,
                attempt_id,
                contract_hash,
                causation_id,
                event_type,
                payload_document_id,
                payload_hash,
                payload_media_type,
                payload_bytes,
                created_at,
            ) = row?;
            let created_at = DateTime::parse_from_rfc3339(&created_at)
                .map_err(|error| {
                    StoreError::InvalidRoot(format!("invalid event timestamp: {error}"))
                })?
                .with_timezone(&Utc);
            let payload = payload_hash
                .map(|hash| -> Result<BlobRef> {
                    Ok(BlobRef {
                        hash: ContentHash::new(hash)
                            .map_err(|error| StoreError::InvalidRoot(error.to_string()))?,
                        media_type: payload_media_type.clone().ok_or_else(|| {
                            StoreError::InvalidRoot("event payload media type missing".to_owned())
                        })?,
                        bytes: payload_bytes.ok_or_else(|| {
                            StoreError::InvalidRoot("event payload bytes missing".to_owned())
                        })?,
                    })
                })
                .transpose()?;
            Ok(StoredEvent {
                cursor,
                envelope: EventEnvelope {
                    schema_version: akzio_domain::V2_SCHEMA_VERSION,
                    run_id: run_id.clone(),
                    task_id: task_id.map(TaskId),
                    attempt_id: attempt_id.map(akzio_domain::AttemptId),
                    contract_hash: contract_hash
                        .map(ContentHash::new)
                        .transpose()
                        .map_err(|error| StoreError::InvalidRoot(error.to_string()))?,
                    causation_id,
                    event_type,
                    payload_document_id: payload_document_id.map(DocumentId),
                    payload,
                    created_at,
                },
            })
        })
        .collect::<Result<Vec<_>>>()
    }

    fn update_lease<P>(&self, lease: &Lease, statement: &str, parameters: P) -> Result<()>
    where
        P: rusqlite::Params,
    {
        let connection = self.connection.lock().expect("store connection poisoned");
        let changed = connection.execute(statement, parameters)?;
        if changed == 0 {
            return Err(StoreError::StaleLease {
                task: lease.task_id.clone(),
            });
        }
        Ok(())
    }

    fn blob_path(&self, hash: &ContentHash) -> PathBuf {
        self.paths
            .blobs
            .join(&hash.as_str()[..2])
            .join(hash.as_str())
    }
}

fn initialize(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "BEGIN;
         CREATE TABLE IF NOT EXISTS runs (
           run_id TEXT PRIMARY KEY,
           purpose TEXT NOT NULL,
            topology_id TEXT NOT NULL,
            plan_document_id TEXT REFERENCES documents(document_id),
            status TEXT NOT NULL,
            cancel_requested INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            finished_at TEXT
         );
         CREATE TABLE IF NOT EXISTS tasks (
           task_id TEXT PRIMARY KEY,
           run_id TEXT NOT NULL REFERENCES runs(run_id),
            kind TEXT NOT NULL,
            objective TEXT NOT NULL DEFAULT 'ad hoc task',
            contract_hash TEXT,
           status TEXT NOT NULL,
           ready_at TEXT NOT NULL,
           lease_id TEXT,
           lease_worker TEXT,
 lease_until TEXT,
 epoch INTEGER NOT NULL,
            priority INTEGER NOT NULL DEFAULT 50,
            max_attempts INTEGER NOT NULL DEFAULT 2,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            max_input_tokens INTEGER NOT NULL DEFAULT 32000,
             max_output_tokens INTEGER NOT NULL DEFAULT 4000,
             max_wall_time_secs INTEGER NOT NULL DEFAULT 180,
             max_tool_calls INTEGER NOT NULL DEFAULT 4,
 on_failure TEXT NOT NULL DEFAULT 'fail_run',
             active_attempt_id TEXT,
            last_error_blob_hash TEXT,
            parent_task_id TEXT,
 cancel_requested INTEGER NOT NULL DEFAULT 0,
 finished_at TEXT
         );
        CREATE INDEX IF NOT EXISTS tasks_claimable ON tasks(status, ready_at, lease_until);
        CREATE TABLE IF NOT EXISTS daemon_leases (
            lease_name TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            epoch INTEGER NOT NULL,
            expires_at TEXT NOT NULL,
            heartbeat_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS execution_commitments (
            plan_hash TEXT PRIMARY KEY,
            run_id TEXT NOT NULL REFERENCES runs(run_id),
            plan_document_id TEXT NOT NULL REFERENCES documents(document_id),
            state TEXT NOT NULL,
            commitment_document_id TEXT REFERENCES documents(document_id),
            submission_document_id TEXT REFERENCES documents(document_id),
            reconciliation_document_id TEXT REFERENCES documents(document_id),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS task_dependencies (
           task_id TEXT NOT NULL REFERENCES tasks(task_id),
           depends_on_task_id TEXT NOT NULL REFERENCES tasks(task_id),
           PRIMARY KEY (task_id, depends_on_task_id),
           CHECK (task_id != depends_on_task_id)
);
CREATE TABLE IF NOT EXISTS task_inputs (
 task_id TEXT NOT NULL REFERENCES tasks(task_id),
 document_id TEXT NOT NULL REFERENCES documents(document_id),
            PRIMARY KEY (task_id, document_id)
        );
        CREATE TABLE IF NOT EXISTS task_attempts (
            attempt_id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL REFERENCES tasks(task_id),
            run_id TEXT NOT NULL REFERENCES runs(run_id),
            attempt INTEGER NOT NULL,
            lease_id TEXT NOT NULL,
            worker_id TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            error_blob_hash TEXT,
            UNIQUE (task_id, attempt)
        );
        CREATE INDEX IF NOT EXISTS task_attempts_task ON task_attempts(task_id, attempt);
        CREATE TABLE IF NOT EXISTS events (
 event_id INTEGER PRIMARY KEY AUTOINCREMENT,
 run_id TEXT NOT NULL REFERENCES runs(run_id),
 task_id TEXT REFERENCES tasks(task_id),
 attempt_id TEXT,
 contract_hash TEXT,
                causation_id TEXT,
                event_type TEXT NOT NULL,
                payload_document_id TEXT REFERENCES documents(document_id),
                payload_hash TEXT,
 payload_media_type TEXT,
 payload_bytes INTEGER,
 created_at TEXT NOT NULL
         );
         CREATE INDEX IF NOT EXISTS events_run_cursor ON events(run_id, event_id);
         CREATE TABLE IF NOT EXISTS documents (
           document_id TEXT PRIMARY KEY,
           kind TEXT NOT NULL,
           blob_hash TEXT NOT NULL,
           media_type TEXT NOT NULL,
           bytes INTEGER NOT NULL,
 producer TEXT NOT NULL,
                run_id TEXT REFERENCES runs(run_id),
                lifecycle TEXT NOT NULL,
                provenance_json TEXT NOT NULL,
                origin_json TEXT NOT NULL,
                created_at TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS document_refs (
           document_id TEXT NOT NULL REFERENCES documents(document_id),
           source_document_id TEXT NOT NULL REFERENCES documents(document_id),
           PRIMARY KEY (document_id, source_document_id)
         );
         CREATE TABLE IF NOT EXISTS contracts (
           contract_hash TEXT PRIMARY KEY,
           document_id TEXT NOT NULL REFERENCES documents(document_id)
         );
         COMMIT;",
    )?;
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS run_links (
            parent_run_id TEXT NOT NULL REFERENCES runs(run_id),
            relation TEXT NOT NULL,
            child_run_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (parent_run_id, relation)
        );",
    )?;
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS paper_schedule_slots (
            session_key TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            plan_blob_hash TEXT NOT NULL,
            plan_media_type TEXT NOT NULL,
            plan_bytes INTEGER NOT NULL,
            scheduler_epoch INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            submitted_at TEXT
        );",
    )?;
    let has_failure_policy = connection
        .query_row(
            "SELECT 1 FROM pragma_table_info('tasks') WHERE name = 'on_failure' LIMIT 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .is_some();
    if !has_failure_policy {
        return Err(StoreError::InvalidRoot(
            "obsolete Akzio v2 Store schema; use a new Store Root".to_owned(),
        ));
    }
    Ok(())
}

fn enum_name<T: serde::Serialize>(value: T) -> String {
    serde_json::to_value(value)
        .expect("enum serialization cannot fail")
        .as_str()
        .expect("enum serializes to string")
        .to_owned()
}

fn parse_daemon_lease_time(value: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|error| StoreError::InvalidRoot(format!("invalid daemon lease time: {error}")))
}

fn execution_commitment_record(
    row: (
        String,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
    ),
) -> Result<ExecutionCommitmentRecord> {
    let (
        plan_hash,
        run_id,
        plan_document_id,
        state,
        commitment_document_id,
        submission_document_id,
        reconciliation_document_id,
    ) = row;
    Ok(ExecutionCommitmentRecord {
        plan_hash: ContentHash::new(plan_hash)
            .map_err(|error| StoreError::InvalidRoot(error.to_string()))?,
        run_id: RunId(run_id),
        plan_document_id: DocumentId(plan_document_id),
        state: match state.as_str() {
            "prepared" => ExecutionCommitmentState::Prepared,
            "submitted" => ExecutionCommitmentState::Submitted,
            "reconciled" => ExecutionCommitmentState::Reconciled,
            _ => {
                return Err(StoreError::InvalidRoot(format!(
                    "unknown execution commitment state {state:?}"
                )))
            }
        },
        commitment_document_id: commitment_document_id.map(DocumentId),
        submission_document_id: submission_document_id.map(DocumentId),
        reconciliation_document_id: reconciliation_document_id.map(DocumentId),
    })
}

fn parse_task_kind(value: &str) -> Result<TaskKind> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .map_err(|error| StoreError::InvalidRoot(format!("unknown task kind {value:?}: {error}")))
}

fn parse_task_status(value: &str) -> Result<TaskStatus> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .map_err(|error| StoreError::InvalidRoot(format!("unknown task status {value:?}: {error}")))
}

fn parse_failure_disposition(value: &str) -> Result<FailureDisposition> {
    serde_json::from_value(serde_json::Value::String(value.to_owned())).map_err(|error| {
        StoreError::InvalidRoot(format!("unknown failure disposition {value:?}: {error}"))
    })
}

fn parse_workflow_status(value: &str) -> Result<WorkflowStatus> {
    serde_json::from_value(serde_json::Value::String(value.to_owned())).map_err(|error| {
        StoreError::InvalidRoot(format!("unknown workflow status {value:?}: {error}"))
    })
}

fn parse_document_kind(value: &str) -> Result<DocumentKind> {
    serde_json::from_value(serde_json::Value::String(value.to_owned())).map_err(|error| {
        StoreError::InvalidRoot(format!("unknown document kind {value:?}: {error}"))
    })
}

fn parse_document_lifecycle(value: &str) -> Result<DocumentLifecycle> {
    serde_json::from_value(serde_json::Value::String(value.to_owned())).map_err(|error| {
        StoreError::InvalidRoot(format!("unknown document lifecycle {value:?}: {error}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use akzio_domain::{
        DocumentKind, DocumentLifecycle, DocumentOrigin, DocumentRecord, EventEnvelope, Provenance,
        RunPurpose, TaskBudget, TaskSpec, TopologyId, WorkflowPlan, WorkflowStatus,
        V2_SCHEMA_VERSION,
    };
    use tempfile::tempdir;

    fn setup() -> (tempfile::TempDir, V2Store, RunId, TaskId) {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        let run = RunId::new();
        let task = TaskId::new();
        store
            .create_run(&run, RunPurpose::Debug, "default", Utc::now())
            .unwrap();
        store
            .enqueue_task(&run, &task, TaskKind::Plan, Utc::now())
            .unwrap();
        (directory, store, run, task)
    }

    #[test]
    fn cas_deduplicates_and_detects_corruption() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        let first = store.put_bytes(b"evidence", "text/plain").unwrap();
        let second = store.put_bytes(b"evidence", "text/plain").unwrap();
        assert_eq!(first.hash, second.hash);
        assert_eq!(store.read_blob(&first).unwrap(), b"evidence");
    }

    #[test]
    fn epoch_fencing_rejects_a_stale_worker() {
        let (_directory, store, _run, task) = setup();
        let now = Utc::now();
        let first = store
            .claim_next_task("worker-a", now, now + chrono::Duration::seconds(1))
            .unwrap()
            .unwrap();
        assert_eq!(first.attempt, 1);
        store.start_task(&first.lease, now).unwrap();
        let second = store
            .claim_next_task(
                "worker-b",
                now + chrono::Duration::seconds(2),
                now + chrono::Duration::seconds(12),
            )
            .unwrap()
            .unwrap();
        assert_eq!(first.task_id, task);
        assert_eq!(second.task_id, task);
        assert_eq!(second.attempt, 2);
        assert!(matches!(
            store.complete_task(
                &first.lease,
                TaskStatus::Succeeded,
                FailureDisposition::FailRun,
            ),
            Err(StoreError::StaleLease { .. })
        ));
        store
            .complete_task(
                &second.lease,
                TaskStatus::Succeeded,
                FailureDisposition::FailRun,
            )
            .unwrap();
    }

    #[test]
    fn daemon_leader_lease_fences_a_stale_scheduler() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        let now = Utc::now();
        let first = store
            .acquire_daemon_lease(
                "local-scheduler",
                "daemon-a",
                now,
                now + chrono::Duration::seconds(30),
            )
            .unwrap()
            .unwrap();
        assert_eq!(first.epoch, 1);
        assert!(store
            .acquire_daemon_lease(
                "local-scheduler",
                "daemon-b",
                now,
                now + chrono::Duration::seconds(30),
            )
            .unwrap()
            .is_none());

        let takeover_at = now + chrono::Duration::seconds(31);
        let second = store
            .acquire_daemon_lease(
                "local-scheduler",
                "daemon-b",
                takeover_at,
                takeover_at + chrono::Duration::seconds(30),
            )
            .unwrap()
            .unwrap();
        assert_eq!(second.epoch, 2);
        assert!(!store
            .heartbeat_daemon_lease(
                &first,
                takeover_at,
                takeover_at + chrono::Duration::seconds(30),
            )
            .unwrap());
        assert!(store
            .heartbeat_daemon_lease(
                &second,
                takeover_at,
                takeover_at + chrono::Duration::seconds(30),
            )
            .unwrap());
        assert!(!store.release_daemon_lease(&first).unwrap());
        assert!(store.release_daemon_lease(&second).unwrap());
        assert!(store.daemon_lease("local-scheduler").unwrap().is_none());
        store.verify_integrity().unwrap();
    }

    #[test]
    fn execution_commitment_is_singleton_and_tracks_submission() {
        let (_directory, store, run, _task) = setup();
        let now = Utc::now();
        let make_document = |kind, value: &'static [u8]| DocumentRecord {
            document_id: DocumentId::new(),
            kind,
            blob: store.put_bytes(value, "application/json").unwrap(),
            producer: "test.execution".to_owned(),
            run_id: Some(run.clone()),
            lifecycle: DocumentLifecycle::RunScoped,
            source_refs: vec![],
            provenance: Provenance::local("test", now),
            origin: None,
            created_at: now,
        };
        let plan = make_document(DocumentKind::ExecutionPlan, b"{}");
        store.register_document(&plan).unwrap();
        let plan_hash = ContentHash::of_bytes(b"canonical-plan");
        let first = store
            .reserve_execution_commitment(&run, &plan.document_id, &plan_hash, now)
            .unwrap();
        assert!(first.newly_reserved);
        assert_eq!(first.record.state, ExecutionCommitmentState::Prepared);
        let second = store
            .reserve_execution_commitment(&run, &plan.document_id, &plan_hash, now)
            .unwrap();
        assert!(!second.newly_reserved);

        let commitment = make_document(DocumentKind::ExecutionCommitment, b"{}");
        store.register_document(&commitment).unwrap();
        store
            .attach_execution_commitment_document(&plan_hash, &commitment.document_id, now)
            .unwrap();
        let submission = make_document(DocumentKind::OrderState, b"{}");
        store.register_document(&submission).unwrap();
        store
            .mark_execution_submitted(&plan_hash, &submission.document_id, now)
            .unwrap();
        let reconciliation = make_document(DocumentKind::OrderState, b"{}");
        store.register_document(&reconciliation).unwrap();
        store
            .mark_execution_reconciled(&plan_hash, &reconciliation.document_id, now)
            .unwrap();
        let record = store.execution_commitment(&plan_hash).unwrap().unwrap();
        assert_eq!(record.state, ExecutionCommitmentState::Reconciled);
        assert_eq!(record.submission_document_id, Some(submission.document_id));
        assert_eq!(
            record.reconciliation_document_id,
            Some(reconciliation.document_id)
        );
        store.verify_integrity().unwrap();
    }

    #[test]
    fn retry_is_bounded_and_respects_ready_time() {
        let (_directory, store, _run, _task) = setup();
        let now = Utc::now();
        let first = store
            .claim_next_task("worker", now, now + chrono::Duration::seconds(30))
            .unwrap()
            .unwrap();
        store.start_task(&first.lease, now).unwrap();
        let error = store.put_bytes(b"rate limited", "text/plain").unwrap();
        let retry_at = now + chrono::Duration::seconds(10);
        assert_eq!(
            store
                .retry_task(
                    &first.lease,
                    retry_at,
                    Some(&error),
                    FailureDisposition::FailRun,
                )
                .unwrap(),
            RetryTaskResult::Requeued
        );
        assert!(store
            .claim_next_task("worker", now, now + chrono::Duration::seconds(30))
            .unwrap()
            .is_none());
        let second = store
            .claim_next_task("worker", retry_at, retry_at + chrono::Duration::seconds(30))
            .unwrap()
            .unwrap();
        assert_eq!(second.attempt, 2);
        assert_eq!(
            store
                .retry_task(&second.lease, retry_at, None, FailureDisposition::FailRun,)
                .unwrap(),
            RetryTaskResult::Terminal(TaskStatus::Failed)
        );
        assert!(store
            .claim_next_task("worker", retry_at + chrono::Duration::seconds(1), retry_at)
            .unwrap()
            .is_none());
    }

    #[test]
    fn skip_task_failure_becomes_skipped_for_direct_and_retry_paths() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        let run = RunId::new();
        let now = Utc::now();
        store
            .create_run(&run, RunPurpose::Debug, "test", now)
            .unwrap();

        let direct_id = TaskId::new();
        store
            .enqueue_task_with_contract(
                &run,
                &direct_id,
                TaskKind::Investigate,
                None,
                FailureDisposition::SkipTask,
                now,
            )
            .unwrap();
        let direct = store
            .claim_next_task("worker", now, now + chrono::Duration::seconds(30))
            .unwrap()
            .unwrap();
        assert_eq!(
            store
                .complete_task(
                    &direct.lease,
                    TaskStatus::Failed,
                    FailureDisposition::SkipTask,
                )
                .unwrap(),
            TaskStatus::Skipped
        );

        let retry_id = TaskId::new();
        store
            .enqueue_task_with_contract(
                &run,
                &retry_id,
                TaskKind::Investigate,
                None,
                FailureDisposition::SkipTask,
                now,
            )
            .unwrap();
        let first = store
            .claim_next_task("worker", now, now + chrono::Duration::seconds(30))
            .unwrap()
            .unwrap();
        let error = store.put_bytes(b"retry", "text/plain").unwrap();
        assert_eq!(
            store
                .retry_task(
                    &first.lease,
                    now,
                    Some(&error),
                    FailureDisposition::SkipTask,
                )
                .unwrap(),
            RetryTaskResult::Requeued
        );
        let second = store
            .claim_next_task("worker", now, now + chrono::Duration::seconds(30))
            .unwrap()
            .unwrap();
        assert_eq!(second.task_id, retry_id);
        assert_eq!(
            store
                .retry_task(
                    &second.lease,
                    now,
                    Some(&error),
                    FailureDisposition::SkipTask,
                )
                .unwrap(),
            RetryTaskResult::Terminal(TaskStatus::Skipped)
        );
        assert_eq!(
            store.refresh_run_status(&run, now).unwrap(),
            WorkflowStatus::Completed
        );
    }

    #[test]
    fn expired_skip_task_is_skipped_after_lease_recovery() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        let run = RunId::new();
        let now = Utc::now();
        store
            .create_run(&run, RunPurpose::Debug, "test", now)
            .unwrap();
        let task = TaskSpec {
            task_id: TaskId::new(),
            kind: TaskKind::Investigate,
            objective: "optional recovery".to_owned(),
            contract_hash: None,
            dependencies: vec![],
            input_refs: vec![],
            budget: TaskBudget {
                max_input_tokens: 1,
                max_output_tokens: 1,
                max_wall_time_secs: 1,
                max_tool_calls: 0,
            },
            on_failure: FailureDisposition::SkipTask,
            priority: 50,
            max_attempts: 1,
            parent_task_id: None,
        };
        store.enqueue_task_spec(&run, &task, now).unwrap();
        let claimed = store
            .claim_next_task("worker", now, now + chrono::Duration::seconds(1))
            .unwrap()
            .unwrap();
        store.start_task(&claimed.lease, now).unwrap();

        store
            .recover_expired_tasks(now + chrono::Duration::seconds(2))
            .unwrap();

        assert!(store
            .claim_next_task(
                "worker",
                now + chrono::Duration::seconds(2),
                now + chrono::Duration::seconds(30),
            )
            .unwrap()
            .is_none());
        assert_eq!(store.run_status(&run).unwrap(), WorkflowStatus::Completed);
    }

    #[test]
    fn fail_run_cancels_remaining_work() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        let run = RunId::new();
        let now = Utc::now();
        store
            .create_run(&run, RunPurpose::Debug, "test", now)
            .unwrap();
        let failing_id = TaskId::new();
        store
            .enqueue_task_with_contract(
                &run,
                &failing_id,
                TaskKind::Plan,
                None,
                FailureDisposition::FailRun,
                now,
            )
            .unwrap();
        let failing = store
            .claim_next_task("worker", now, now + chrono::Duration::seconds(30))
            .unwrap()
            .unwrap();
        store
            .enqueue_task_with_contract(
                &run,
                &TaskId::new(),
                TaskKind::Investigate,
                None,
                FailureDisposition::FailTask,
                now,
            )
            .unwrap();
        store
            .complete_task(
                &failing.lease,
                TaskStatus::Failed,
                FailureDisposition::FailRun,
            )
            .unwrap();

        assert_eq!(
            store.refresh_run_status(&run, now).unwrap(),
            WorkflowStatus::Failed
        );
        assert!(store.run_cancel_requested(&run).unwrap());
        assert!(store
            .claim_next_task("worker", now, now + chrono::Duration::seconds(30))
            .unwrap()
            .is_none());
    }

    #[test]
    fn cancellation_prevents_new_claims() {
        let (_directory, store, run, _task) = setup();
        assert_eq!(store.cancel_run(&run, Utc::now()).unwrap(), 1);
        assert!(store.run_cancel_requested(&run).unwrap());
        assert!(store
            .claim_next_task(
                "worker",
                Utc::now(),
                Utc::now() + chrono::Duration::seconds(30)
            )
            .unwrap()
            .is_none());
    }

    #[test]
    fn doctor_accepts_completed_attempt_history() {
        let (_directory, store, _run, _task) = setup();
        let now = Utc::now();
        let task = store
            .claim_next_task("worker", now, now + chrono::Duration::seconds(30))
            .unwrap()
            .unwrap();
        store.start_task(&task.lease, now).unwrap();
        store
            .complete_task(
                &task.lease,
                TaskStatus::Succeeded,
                FailureDisposition::FailRun,
            )
            .unwrap();
        store.verify_integrity().unwrap();
    }

    #[test]
    fn event_log_is_durable_and_ordered() {
        let (_directory, store, run, task) = setup();
        let payload = store.put_bytes(b"event payload", "text/plain").unwrap();
        let attempt_id = akzio_domain::AttemptId::new();
        let contract_hash = ContentHash::of_bytes(b"contract");
        let event = EventEnvelope {
            schema_version: V2_SCHEMA_VERSION,
            run_id: run.clone(),
            task_id: Some(task),
            attempt_id: Some(attempt_id),
            contract_hash: Some(contract_hash),
            causation_id: Some("cause-1".to_owned()),
            event_type: "task.queued".to_owned(),
            payload_document_id: None,
            payload: Some(payload),
            created_at: Utc::now(),
        };
        assert_eq!(store.append_event(&event).unwrap(), 1);
        assert_eq!(store.event_count(&run).unwrap(), 1);
        let stored = store.events_after(&run, 0, 10).unwrap();
        assert_eq!(stored[0].cursor, 1);
        assert_eq!(stored[0].envelope, event);
    }

    #[test]
    fn document_origin_and_event_document_reference_round_trip() {
        let (_directory, store, run, _task) = setup();
        let now = Utc::now();
        let claimed = store
            .claim_next_task("worker", now, now + chrono::Duration::seconds(30))
            .unwrap()
            .unwrap();
        store.start_task(&claimed.lease, now).unwrap();
        let blob = store
            .put_bytes(br#"{"kind":"task_result"}"#, "application/json")
            .unwrap();
        let document = DocumentRecord {
            document_id: DocumentId::new(),
            kind: DocumentKind::TaskResult,
            blob: blob.clone(),
            producer: "test.task".to_owned(),
            run_id: Some(run.clone()),
            lifecycle: DocumentLifecycle::RunScoped,
            source_refs: vec![],
            provenance: Provenance::local("test", now),
            origin: Some(DocumentOrigin::task(
                claimed.task_id.clone(),
                claimed.attempt_id.clone(),
                claimed.contract_hash.clone(),
            )),
            created_at: now,
        };
        store.register_document(&document).unwrap();
        let event = EventEnvelope {
            schema_version: V2_SCHEMA_VERSION,
            run_id: run.clone(),
            task_id: Some(claimed.task_id.clone()),
            attempt_id: Some(claimed.attempt_id.clone()),
            contract_hash: claimed.contract_hash.clone(),
            causation_id: Some("test-origin".to_owned()),
            event_type: "task.result_recorded".to_owned(),
            payload_document_id: Some(document.document_id.clone()),
            payload: Some(blob),
            created_at: now,
        };
        store.append_event(&event).unwrap();

        assert_eq!(
            store.read_document(&document.document_id).unwrap(),
            document
        );
        assert_eq!(store.events_after(&run, 0, 1).unwrap()[0].envelope, event);
        store
            .complete_task(
                &claimed.lease,
                TaskStatus::Succeeded,
                FailureDisposition::FailRun,
            )
            .unwrap();
        store.verify_integrity().unwrap();
    }

    #[test]
    fn doctor_rejects_a_document_with_an_unknown_task_origin() {
        let (_directory, store, run, _task) = setup();
        let now = Utc::now();
        let document = DocumentRecord {
            document_id: DocumentId::new(),
            kind: DocumentKind::TaskResult,
            blob: store.put_bytes(b"dangling", "text/plain").unwrap(),
            producer: "test.task".to_owned(),
            run_id: Some(run),
            lifecycle: DocumentLifecycle::RunScoped,
            source_refs: vec![],
            provenance: Provenance::local("test", now),
            origin: Some(DocumentOrigin {
                task_id: Some(TaskId::new()),
                attempt_id: None,
                contract_hash: None,
            }),
            created_at: now,
        };
        store.register_document(&document).unwrap();
        assert!(matches!(
            store.verify_integrity(),
            Err(StoreError::InvalidRoot(message)) if message.contains("unknown task origin")
        ));
    }

    #[test]
    fn optional_failure_unblocks_its_join_and_run_completes() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        let run = RunId::new();
        let now = Utc::now();
        store
            .create_run(&run, RunPurpose::Debug, "test", now)
            .unwrap();
        let budget = TaskBudget {
            max_input_tokens: 1,
            max_output_tokens: 1,
            max_wall_time_secs: 1,
            max_tool_calls: 0,
        };
        let optional = TaskSpec {
            task_id: TaskId::new(),
            kind: TaskKind::Investigate,
            objective: "optional evidence".to_owned(),
            contract_hash: None,
            dependencies: vec![],
            input_refs: vec![],
            budget: budget.clone(),
            on_failure: FailureDisposition::FailTask,
            priority: 100,
            max_attempts: 1,
            parent_task_id: None,
        };
        let join = TaskSpec {
            task_id: TaskId::new(),
            kind: TaskKind::SynthesizeDecision,
            objective: "continue without optional evidence".to_owned(),
            contract_hash: None,
            dependencies: vec![optional.task_id.clone()],
            input_refs: vec![],
            budget,
            on_failure: FailureDisposition::FailRun,
            priority: 90,
            max_attempts: 1,
            parent_task_id: None,
        };
        store.enqueue_task_spec(&run, &optional, now).unwrap();
        store.enqueue_task_spec(&run, &join, now).unwrap();
        store
            .add_task_dependency(&join.task_id, &optional.task_id)
            .unwrap();

        let first = store
            .claim_next_task("worker", now, now + chrono::Duration::seconds(10))
            .unwrap()
            .unwrap();
        assert_eq!(first.on_failure, FailureDisposition::FailTask);
        store.start_task(&first.lease, now).unwrap();
        store
            .complete_task(
                &first.lease,
                TaskStatus::Failed,
                FailureDisposition::FailTask,
            )
            .unwrap();

        let second = store
            .claim_next_task("worker", now, now + chrono::Duration::seconds(10))
            .unwrap()
            .unwrap();
        assert_eq!(second.task_id, join.task_id);
        store.start_task(&second.lease, now).unwrap();
        store
            .complete_task(
                &second.lease,
                TaskStatus::Succeeded,
                FailureDisposition::FailRun,
            )
            .unwrap();
        assert_eq!(
            store.refresh_run_status(&run, now).unwrap(),
            WorkflowStatus::Completed
        );
    }

    #[test]
    fn child_run_reservation_is_idempotent_and_doctor_checked() {
        let (_directory, store, parent, _task) = setup();
        let now = Utc::now();
        let proposed = RunId::new();
        let (child, reserved) = store
            .reserve_child_run(&parent, "shadow", &proposed, now)
            .unwrap();
        assert!(reserved);
        assert_eq!(child, proposed);
        let (same_child, reserved_again) = store
            .reserve_child_run(&parent, "shadow", &RunId::new(), now)
            .unwrap();
        assert!(!reserved_again);
        assert_eq!(same_child, child);
        store
            .create_run(&child, RunPurpose::Shadow, "shadow", now)
            .unwrap();
        assert_eq!(store.child_run(&parent, "shadow").unwrap(), Some(child));
        assert_eq!(store.parent_run(&proposed, "shadow").unwrap(), Some(parent));
        store.verify_integrity().unwrap();
    }

    #[test]
    fn dependency_must_finish_before_a_task_is_claimable() {
        let (_directory, store, run, prerequisite) = setup();
        let dependent = TaskId::new();
        store
            .enqueue_task(&run, &dependent, TaskKind::Investigate, Utc::now())
            .unwrap();
        store
            .add_task_dependency(&dependent, &prerequisite)
            .unwrap();
        let now = Utc::now();
        let first = store
            .claim_next_task("worker", now, now + chrono::Duration::seconds(10))
            .unwrap()
            .unwrap();
        assert_eq!(first.task_id, prerequisite);
        store
            .complete_task(
                &first.lease,
                TaskStatus::Succeeded,
                FailureDisposition::FailRun,
            )
            .unwrap();
        let second = store
            .claim_next_task("worker", now, now + chrono::Duration::seconds(10))
            .unwrap()
            .unwrap();
        assert_eq!(second.task_id, dependent);
    }

    #[test]
    fn paper_schedule_slot_is_singleton_fenced_and_doctor_checked() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        let now = Utc::now();
        let lease = store
            .acquire_daemon_lease(
                "scheduler",
                "daemon-a",
                now,
                now + chrono::Duration::seconds(30),
            )
            .unwrap()
            .unwrap();
        let plan = WorkflowPlan {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: TopologyId("baseline".to_owned()),
            tasks: vec![],
        };
        let first_run = RunId::new();
        let first = store
            .reserve_paper_schedule_slot(&lease, "paper:2026-08-06", &first_run, &plan, now)
            .unwrap();
        assert!(first.newly_reserved);
        let duplicate = store
            .reserve_paper_schedule_slot(&lease, "paper:2026-08-06", &RunId::new(), &plan, now)
            .unwrap();
        assert!(!duplicate.newly_reserved);
        assert_eq!(duplicate.slot.run_id, first_run);
        assert_eq!(duplicate.slot.plan, plan);

        store
            .create_run(&first_run, RunPurpose::Paper, "baseline", now)
            .unwrap();
        store
            .write_workflow_plan(&first_run, &plan, None, None, now)
            .unwrap();
        assert!(store
            .mark_paper_schedule_submitted(&lease, "paper:2026-08-06", &first_run, now)
            .unwrap());
        assert!(!store
            .mark_paper_schedule_submitted(&lease, "paper:2026-08-06", &first_run, now)
            .unwrap());

        let takeover_at = now + chrono::Duration::seconds(31);
        let replacement = store
            .acquire_daemon_lease(
                "scheduler",
                "daemon-b",
                takeover_at,
                takeover_at + chrono::Duration::seconds(30),
            )
            .unwrap()
            .unwrap();
        assert!(matches!(
            store.mark_paper_schedule_submitted(
                &lease,
                "paper:2026-08-06",
                &first_run,
                takeover_at,
            ),
            Err(StoreError::SchedulerFenced(_))
        ));
        let recovered = store
            .reserve_paper_schedule_slot(
                &replacement,
                "paper:2026-08-06",
                &RunId::new(),
                &plan,
                takeover_at,
            )
            .unwrap();
        assert_eq!(recovered.slot.run_id, first_run);
        store.verify_integrity().unwrap();
    }
}
