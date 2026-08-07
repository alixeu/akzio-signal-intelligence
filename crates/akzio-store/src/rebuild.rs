//! Store implementation for the source-incompatible Akzio v2 rebuild.
//!
//! `RebuildStore` deliberately uses a different database filename and metadata
//! marker from `V2Store`; callers must choose a new Store Root rather than run a
//! silent in-place migration.

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use akzio_domain::{
    Artifact, ArtifactId, ArtifactKind, ArtifactOrigin, ArtifactRef, BlobRef, ContentHash,
    DomainError, RunId, RunPurpose, TaskId, TaskRecipeId,
    TaskStatus, TaskWritePermit, WorkflowGraph, WorkflowNode, REBUILD_SCHEMA_VERSION,
};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::Serialize;
use thiserror::Error;

const DATABASE_FILE: &str = "akzio.sqlite3";
const LEGACY_DATABASE_FILE: &str = "control.sqlite3";

#[derive(Debug, Error)]
pub enum RebuildStoreError {
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error("I/O at {path}: {source}")]
    Io { path: PathBuf, source: std::io::Error },
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("incompatible Store Root {0}; create a new rebuilt-v2 root")]
    IncompatibleStoreRoot(PathBuf),
    #[error("artifact {0} does not exist")]
    MissingArtifact(ArtifactId),
    #[error("artifact {0} has an invalid source closure")]
    InvalidArtifactClosure(ArtifactId),
    #[error("workflow graph artifact must have kind workflow_graph")]
    InvalidWorkflowGraphArtifact,
    #[error("workflow graph differs from persisted task graph")]
    WorkflowGraphMismatch,
    #[error("workflow patch is based on a stale graph artifact")]
    StaleWorkflowGraph,
    #[error("task {0} already exists")]
    DuplicateTask(TaskId),
    #[error("run {0} already exists")]
    DuplicateRun(RunId),
    #[error("task write permit is stale for {0}")]
    StalePermit(TaskId),
    #[error("task write permit origin does not match artifact")]
    PermitOriginMismatch,
    #[error("task {0} has unresolved dependencies")]
    UnresolvedDependencies(TaskId),
    #[error("task {0} is not runnable")]
    TaskNotRunnable(TaskId),
    #[error("task {0} does not exist")]
    MissingTask(TaskId),
    #[error("blob {0} is missing or corrupt")]
    MissingBlob(ContentHash),
    #[error("Store Doctor: {0}")]
    Integrity(String),
}

pub type RebuildStoreResult<T> = Result<T, RebuildStoreError>;

#[derive(Debug, Clone)]
pub struct RebuildStore {
    root: Arc<PathBuf>,
    blobs: Arc<PathBuf>,
    connection: Arc<Mutex<Connection>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebuildRun {
    pub run_id: RunId,
    pub purpose: RunPurpose,
    pub topology_id: String,
    pub graph_artifact_id: ArtifactId,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebuildTask {
    pub run_id: RunId,
    pub node: WorkflowNode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowCommit {
    pub run: RebuildRun,
    pub graph: Artifact,
    pub nodes: Vec<WorkflowNode>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedRebuildTask {
    pub run_id: RunId,
    pub node: WorkflowNode,
    pub permit: TaskWritePermit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRebuildEvent {
    pub cursor: i64,
    pub run_id: RunId,
    pub task_id: Option<TaskId>,
    pub attempt_id: Option<akzio_domain::AttemptId>,
    pub event_type: String,
    pub artifact_id: Option<ArtifactId>,
    pub created_at: DateTime<Utc>,
}

impl RebuildStore {
    pub fn open(root: impl AsRef<Path>) -> RebuildStoreResult<Self> {
        let root = root.as_ref().to_path_buf();
        if root.join(LEGACY_DATABASE_FILE).exists() && !root.join(DATABASE_FILE).exists() {
            return Err(RebuildStoreError::IncompatibleStoreRoot(root));
        }
        fs::create_dir_all(root.join("blobs")).map_err(|source| RebuildStoreError::Io {
            path: root.join("blobs"),
            source,
        })?;
        let database = root.join(DATABASE_FILE);
        let mut connection = Connection::open(&database)?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        initialize(&mut connection)?;
        Ok(Self {
            blobs: Arc::new(root.join("blobs")),
            root: Arc::new(root),
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    pub fn root(&self) -> &Path {
        self.root.as_ref()
    }

    pub fn put_bytes(&self, bytes: &[u8], media_type: impl Into<String>) -> RebuildStoreResult<BlobRef> {
        let media_type = media_type.into();
        if media_type.trim().is_empty() {
            return Err(RebuildStoreError::Domain(DomainError::EmptyField {
                field: "blob_ref.media_type",
            }));
        }
        let hash = ContentHash::of_bytes(bytes);
        let path = self.blob_path(&hash);
        if !path.exists() {
            let parent = path.parent().expect("content addressed blob has parent");
            fs::create_dir_all(parent).map_err(|source| RebuildStoreError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
            match OpenOptions::new().create_new(true).write(true).open(&path) {
                Ok(mut file) => {
                    file.write_all(bytes).map_err(|source| RebuildStoreError::Io {
                        path: path.clone(),
                        source,
                    })?;
                    file.sync_all().map_err(|source| RebuildStoreError::Io {
                        path: path.clone(),
                        source,
                    })?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(source) => return Err(RebuildStoreError::Io { path, source }),
            }
        }
        Ok(BlobRef {
            hash,
            media_type,
            bytes: bytes.len() as u64,
        })
    }

    pub fn put_json<T: Serialize>(&self, value: &T) -> RebuildStoreResult<BlobRef> {
        self.put_bytes(&serde_json::to_vec(value)?, "application/json")
    }

    pub fn read_blob(&self, blob: &BlobRef) -> RebuildStoreResult<Vec<u8>> {
        let path = self.blob_path(&blob.hash);
        let bytes = fs::read(&path).map_err(|source| RebuildStoreError::Io {
            path: path.clone(),
            source,
        })?;
        if bytes.len() as u64 != blob.bytes || ContentHash::of_bytes(&bytes) != blob.hash {
            return Err(RebuildStoreError::MissingBlob(blob.hash.clone()));
        }
        Ok(bytes)
    }

    /// Writes a root artifact such as an installed Contract. Bootstrap is deliberately
    /// narrow: a task-origin artifact must use `write_task_artifact` instead.
    pub fn write_bootstrap_artifact(&self, artifact: &Artifact) -> RebuildStoreResult<()> {
        artifact.validate()?;
        if artifact.origin.is_some()
            || !matches!(
                artifact.kind,
                ArtifactKind::Contract | ArtifactKind::CandidatePolicy | ArtifactKind::FreezeState
            )
        {
            return Err(RebuildStoreError::PermitOriginMismatch);
        }
        self.read_blob(&artifact.blob)?;
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        insert_artifact(&transaction, artifact)?;
        transaction.commit()?;
        Ok(())
    }

    /// Commits the frozen workflow graph, Run row, nodes, dependencies, and creation
    /// event as one transaction. A process cannot observe a half-submitted graph.
    pub fn commit_workflow(&self, commit: &WorkflowCommit) -> RebuildStoreResult<()> {
        if commit.graph.kind != ArtifactKind::WorkflowGraph
            || commit.graph.artifact_id != commit.run.graph_artifact_id
        {
            return Err(RebuildStoreError::InvalidWorkflowGraphArtifact);
        }
        commit.graph.validate()?;
        self.read_blob(&commit.graph.blob)?;
        let graph: WorkflowGraph = serde_json::from_slice(&self.read_blob(&commit.graph.blob)?)?;
        graph.validate()?;
        if graph.nodes != commit.nodes || graph.topology_id != commit.run.topology_id {
            return Err(RebuildStoreError::WorkflowGraphMismatch);
        }

        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        insert_artifact(&transaction, &commit.graph)?;
        let inserted = transaction.execute(
            r#"INSERT INTO rebuild_runs
               (run_id, purpose, topology_id, graph_artifact_id, status, created_at)
               VALUES (?1, ?2, ?3, ?4, 'queued', ?5)"#,
            params![
                commit.run.run_id.0,
                enum_name(commit.run.purpose),
                commit.run.topology_id,
                commit.run.graph_artifact_id.0.as_str(),
                commit.run.created_at.to_rfc3339(),
            ],
        )?;
        if inserted != 1 {
            return Err(RebuildStoreError::DuplicateRun(commit.run.run_id.clone()));
        }
        for node in &commit.nodes {
            insert_node(&transaction, &commit.run.run_id, node, commit.run.created_at)?;
        }
        transaction.execute(
            r#"INSERT INTO rebuild_workflow_revisions
               (run_id, revision, graph_artifact_id, created_at)
               VALUES (?1, 0, ?2, ?3)"#,
            params![
                commit.run.run_id.0,
                commit.run.graph_artifact_id.0.as_str(),
                commit.run.created_at.to_rfc3339(),
            ],
        )?;
        append_event(
            &transaction,
            &commit.run.run_id,
            None,
            None,
            "workflow.created",
            Some(&commit.graph.artifact_id),
            commit.run.created_at,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Append a Planner-approved graph revision. The existing graph is never
    /// rewritten: the Run atomically advances to a new immutable graph artifact.
    pub fn append_workflow_patch(
        &self,
        run_id: &RunId,
        previous_graph_artifact_id: &ArtifactId,
        next_graph: &Artifact,
        added_nodes: &[WorkflowNode],
        now: DateTime<Utc>,
    ) -> RebuildStoreResult<()> {
        if next_graph.kind != ArtifactKind::WorkflowGraph {
            return Err(RebuildStoreError::InvalidWorkflowGraphArtifact);
        }
        next_graph.validate()?;
        let graph: WorkflowGraph = serde_json::from_slice(&self.read_blob(&next_graph.blob)?)?;
        graph.validate()?;
        let added_ids = added_nodes
            .iter()
            .map(|node| node.task_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        if added_ids.len() != added_nodes.len()
            || !added_nodes
                .iter()
                .all(|node| graph.nodes.iter().any(|item| item == node))
        {
            return Err(RebuildStoreError::WorkflowGraphMismatch);
        }

        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = transaction
            .query_row(
                "SELECT graph_artifact_id FROM rebuild_runs WHERE run_id = ?1",
                params![run_id.0],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(current) = current else {
            return Err(RebuildStoreError::DuplicateRun(run_id.clone()));
        };
        if current != previous_graph_artifact_id.0.as_str() {
            return Err(RebuildStoreError::StaleWorkflowGraph);
        }
        let existing_ids = transaction
            .prepare("SELECT task_id FROM rebuild_tasks WHERE run_id = ?1")?
            .query_map(params![run_id.0], |row| row.get::<_, String>(0))?
            .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
        let next_ids = graph
            .nodes
            .iter()
            .map(|node| node.task_id.0.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let expected = existing_ids
            .iter()
            .cloned()
            .chain(added_ids.iter().map(|id| id.0.clone()))
            .collect::<std::collections::BTreeSet<_>>();
        if next_ids != expected {
            return Err(RebuildStoreError::WorkflowGraphMismatch);
        }
        insert_artifact(&transaction, next_graph)?;
        for node in added_nodes {
            insert_node(&transaction, run_id, node, now)?;
        }
        transaction.execute(
            "UPDATE rebuild_runs SET graph_artifact_id = ?1 WHERE run_id = ?2",
            params![next_graph.artifact_id.0.as_str(), run_id.0],
        )?;
        let revision = transaction.query_row(
            "SELECT COALESCE(MAX(revision), -1) + 1 FROM rebuild_workflow_revisions WHERE run_id = ?1",
            params![run_id.0],
            |row| row.get::<_, i64>(0),
        )?;
        transaction.execute(
            r#"INSERT INTO rebuild_workflow_revisions
               (run_id, revision, graph_artifact_id, created_at)
               VALUES (?1, ?2, ?3, ?4)"#,
            params![
                run_id.0,
                revision,
                next_graph.artifact_id.0.as_str(),
                now.to_rfc3339(),
            ],
        )?;
        append_event(
            &transaction,
            run_id,
            None,
            None,
            "workflow.patched",
            Some(&next_graph.artifact_id),
            now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn claim_next_task(
        &self,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_for: Duration,
    ) -> RebuildStoreResult<Option<ClaimedRebuildTask>> {
        if worker_id.trim().is_empty() {
            return Err(RebuildStoreError::Domain(DomainError::EmptyField {
                field: "worker_id",
            }));
        }
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let selected = transaction
            .query_row(
                r#"SELECT t.task_id, t.run_id, t.recipe_id, t.objective, t.contract_hash, t.priority,
                           t.budget_json, t.retry_json, t.on_failure, t.parent_task_id
                    FROM rebuild_tasks AS t
                    JOIN rebuild_runs AS r ON r.run_id = t.run_id
                    WHERE t.status = 'queued' AND t.ready_at <= ?1 AND r.status IN ('queued', 'running')
                      AND NOT EXISTS (
                        SELECT 1 FROM rebuild_task_dependencies AS d
                        JOIN rebuild_tasks AS p ON p.task_id = d.depends_on_task_id
                        WHERE d.task_id = t.task_id AND p.status NOT IN ('succeeded', 'skipped')
                      )
                    ORDER BY t.priority DESC, t.task_id ASC LIMIT 1"#,
                params![now.to_rfc3339()],
                |row| row_to_node(row),
            )
            .optional()?;
        let Some((run_id, node)) = selected else {
            transaction.commit()?;
            return Ok(None);
        };
        let permit = TaskWritePermit {
            run_id: run_id.clone(),
            task_id: node.task_id.clone(),
            attempt_id: akzio_domain::AttemptId::new(),
            lease_id: akzio_domain::LeaseId::new(),
            epoch: transaction.query_row(
                "SELECT lease_epoch + 1 FROM rebuild_tasks WHERE task_id = ?1",
                params![node.task_id.0],
                |row| row.get(0),
            )?,
            contract_hash: node.contract_hash.clone(),
        };
        let updated = transaction.execute(
            r#"UPDATE rebuild_tasks
               SET status = 'running', lease_id = ?1, lease_epoch = ?2, active_attempt_id = ?3,
                   lease_until = ?4, worker_id = ?5
               WHERE task_id = ?6 AND status = 'queued'"#,
            params![
                permit.lease_id.0,
                permit.epoch,
                permit.attempt_id.0,
                (now + lease_for).to_rfc3339(),
                worker_id,
                permit.task_id.0,
            ],
        )?;
        if updated != 1 {
            return Err(RebuildStoreError::TaskNotRunnable(permit.task_id));
        }
        transaction.execute(
            r#"INSERT INTO rebuild_attempts
               (attempt_id, task_id, run_id, lease_id, epoch, worker_id, status, started_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'running', ?7)"#,
            params![
                permit.attempt_id.0,
                permit.task_id.0,
                permit.run_id.0,
                permit.lease_id.0,
                permit.epoch,
                worker_id,
                now.to_rfc3339(),
            ],
        )?;
        transaction.execute(
            "UPDATE rebuild_runs SET status = 'running' WHERE run_id = ?1 AND status = 'queued'",
            params![permit.run_id.0],
        )?;
        append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            "task.started",
            None,
            now,
        )?;
        transaction.commit()?;
        Ok(Some(ClaimedRebuildTask {
            run_id,
            node,
            permit,
        }))
    }

    pub fn heartbeat_task(
        &self,
        permit: &TaskWritePermit,
        expires_at: DateTime<Utc>,
    ) -> RebuildStoreResult<()> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let updated = connection.execute(
            r#"UPDATE rebuild_tasks SET lease_until = ?1
               WHERE task_id = ?2 AND status = 'running' AND lease_id = ?3 AND lease_epoch = ?4
                 AND active_attempt_id = ?5"#,
            params![
                expires_at.to_rfc3339(),
                permit.task_id.0,
                permit.lease_id.0,
                permit.epoch,
                permit.attempt_id.0,
            ],
        )?;
        if updated != 1 {
            return Err(RebuildStoreError::StalePermit(permit.task_id.clone()));
        }
        Ok(())
    }

    pub fn write_task_artifact(
        &self,
        permit: &TaskWritePermit,
        artifact: &Artifact,
        event_type: &str,
        now: DateTime<Utc>,
    ) -> RebuildStoreResult<()> {
        artifact.validate()?;
        self.read_blob(&artifact.blob)?;
        if event_type.trim().is_empty() {
            return Err(RebuildStoreError::Domain(DomainError::EmptyField {
                field: "event_type",
            }));
        }
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
        assert_origin_matches(artifact.origin.as_ref(), permit)?;
        insert_artifact(&transaction, artifact)?;
        append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            event_type,
            Some(&artifact.artifact_id),
            now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn finish_task(
        &self,
        permit: &TaskWritePermit,
        status: TaskStatus,
        now: DateTime<Utc>,
    ) -> RebuildStoreResult<()> {
        if !status.is_terminal() {
            return Err(RebuildStoreError::TaskNotRunnable(permit.task_id.clone()));
        }
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
        transaction.execute(
            r#"UPDATE rebuild_tasks
               SET status = ?1, lease_id = NULL, active_attempt_id = NULL, worker_id = NULL,
                   lease_until = NULL, finished_at = ?2
               WHERE task_id = ?3"#,
            params![enum_name(status), now.to_rfc3339(), permit.task_id.0],
        )?;
        transaction.execute(
            "UPDATE rebuild_attempts SET status = ?1, finished_at = ?2 WHERE attempt_id = ?3",
            params![enum_name(status), now.to_rfc3339(), permit.attempt_id.0],
        )?;
        append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            match status {
                TaskStatus::Succeeded => "task.succeeded",
                TaskStatus::Failed => "task.failed",
                TaskStatus::Cancelled => "task.cancelled",
                TaskStatus::Skipped => "task.skipped",
                _ => unreachable!("terminal status checked above"),
            },
            None,
            now,
        )?;
        refresh_run_status(&transaction, &permit.run_id, now)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn recover_expired_tasks(&self, now: DateTime<Utc>) -> RebuildStoreResult<u64> {
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            r#"UPDATE rebuild_tasks
               SET status = 'queued', lease_id = NULL, active_attempt_id = NULL, worker_id = NULL,
                   lease_until = NULL, ready_at = ?1
               WHERE status = 'running' AND lease_until < ?1"#,
            params![now.to_rfc3339()],
        )?;
        transaction.execute(
            r#"UPDATE rebuild_attempts SET status = 'abandoned', finished_at = ?1
               WHERE status = 'running' AND attempt_id NOT IN (
                 SELECT active_attempt_id FROM rebuild_tasks WHERE active_attempt_id IS NOT NULL
               )"#,
            params![now.to_rfc3339()],
        )?;
        transaction.commit()?;
        Ok(changed as u64)
    }

    pub fn artifact(&self, artifact_id: &ArtifactId) -> RebuildStoreResult<Artifact> {
        let connection = self.connection.lock().expect("store connection poisoned");
        read_artifact(&connection, artifact_id)
    }

    pub fn events_after(
        &self,
        run_id: &RunId,
        after: i64,
        limit: usize,
    ) -> RebuildStoreResult<Vec<StoredRebuildEvent>> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let mut statement = connection.prepare(
            r#"SELECT event_id, run_id, task_id, attempt_id, event_type, artifact_id, created_at
               FROM rebuild_events WHERE run_id = ?1 AND event_id > ?2
               ORDER BY event_id ASC LIMIT ?3"#,
        )?;
        let rows = statement.query_map(params![run_id.0, after, limit as i64], |row| {
            Ok(StoredRebuildEvent {
                cursor: row.get(0)?,
                run_id: RunId(row.get(1)?),
                task_id: row.get::<_, Option<String>>(2)?.map(TaskId),
                attempt_id: row.get::<_, Option<String>>(3)?.map(akzio_domain::AttemptId),
                event_type: row.get(4)?,
                artifact_id: row
                    .get::<_, Option<String>>(5)?
                    .map(ContentHash::new)
                    .transpose()
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?
                    .map(ArtifactId),
                created_at: parse_time(&row.get::<_, String>(6)?)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn verify_integrity(&self) -> RebuildStoreResult<()> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let fk = connection
            .query_row("PRAGMA foreign_key_check", [], |_| Ok(()))
            .optional()?;
        if fk.is_some() {
            return Err(RebuildStoreError::Integrity("foreign key check failed".to_owned()));
        }
        let mut statement = connection.prepare(
            "SELECT artifact_id, blob_hash, media_type, bytes FROM rebuild_artifacts ORDER BY artifact_id",
        )?;
        let artifacts = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, u64>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for (artifact_id, hash, media_type, bytes) in artifacts {
            let artifact_id = ArtifactId(ContentHash::new(artifact_id)?);
            self.read_blob(&BlobRef {
                hash: ContentHash::new(hash)?,
                media_type,
                bytes,
            })?;
            let artifact = read_artifact(&connection, &artifact_id)?;
            artifact.validate()?;
        }
        let orphan = connection
            .query_row(
                r#"SELECT t.task_id FROM rebuild_tasks AS t
                    LEFT JOIN rebuild_runs AS r ON r.run_id = t.run_id
                    WHERE r.run_id IS NULL LIMIT 1"#,
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if let Some(task_id) = orphan {
            return Err(RebuildStoreError::Integrity(format!(
                "task {task_id} has no run"
            )));
        }
        Ok(())
    }

    fn blob_path(&self, hash: &ContentHash) -> PathBuf {
        self.blobs.join(&hash.as_str()[..2]).join(hash.as_str())
    }
}

fn initialize(connection: &mut Connection) -> RebuildStoreResult<()> {
    connection.execute_batch(
        "BEGIN;
         CREATE TABLE IF NOT EXISTS rebuild_metadata (
           key TEXT PRIMARY KEY,
           value TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS rebuild_artifacts (
           artifact_id TEXT PRIMARY KEY,
           kind TEXT NOT NULL,
           blob_hash TEXT NOT NULL,
           media_type TEXT NOT NULL,
           bytes INTEGER NOT NULL,
           producer TEXT NOT NULL,
           lifecycle TEXT NOT NULL,
           provenance_json TEXT NOT NULL,
           origin_json TEXT,
           created_at TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS rebuild_artifact_refs (
           artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
           source_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
           source_kind TEXT NOT NULL,
           PRIMARY KEY (artifact_id, source_artifact_id)
         );
         CREATE TABLE IF NOT EXISTS rebuild_runs (
           run_id TEXT PRIMARY KEY,
           purpose TEXT NOT NULL,
           topology_id TEXT NOT NULL,
           graph_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
           status TEXT NOT NULL,
           created_at TEXT NOT NULL,
           finished_at TEXT
         );
         CREATE TABLE IF NOT EXISTS rebuild_workflow_revisions (
           run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
           revision INTEGER NOT NULL,
           graph_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
           created_at TEXT NOT NULL,
           PRIMARY KEY (run_id, revision)
         );
         CREATE TABLE IF NOT EXISTS rebuild_tasks (
           task_id TEXT PRIMARY KEY,
           run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
           recipe_id TEXT NOT NULL,
           objective TEXT NOT NULL,
           contract_hash TEXT,
           priority INTEGER NOT NULL,
           budget_json TEXT NOT NULL,
           retry_json TEXT NOT NULL,
           on_failure TEXT NOT NULL,
           parent_task_id TEXT,
           status TEXT NOT NULL,
           ready_at TEXT NOT NULL,
           lease_id TEXT,
           lease_epoch INTEGER NOT NULL DEFAULT 0,
           active_attempt_id TEXT,
           lease_until TEXT,
           worker_id TEXT,
           finished_at TEXT
         );
         CREATE TABLE IF NOT EXISTS rebuild_task_dependencies (
           task_id TEXT NOT NULL REFERENCES rebuild_tasks(task_id),
           depends_on_task_id TEXT NOT NULL REFERENCES rebuild_tasks(task_id),
           PRIMARY KEY (task_id, depends_on_task_id)
         );
         CREATE TABLE IF NOT EXISTS rebuild_attempts (
           attempt_id TEXT PRIMARY KEY,
           task_id TEXT NOT NULL REFERENCES rebuild_tasks(task_id),
           run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
           lease_id TEXT NOT NULL,
           epoch INTEGER NOT NULL,
           worker_id TEXT NOT NULL,
           status TEXT NOT NULL,
           started_at TEXT NOT NULL,
           finished_at TEXT
         );
         CREATE TABLE IF NOT EXISTS rebuild_events (
           event_id INTEGER PRIMARY KEY AUTOINCREMENT,
           run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
           task_id TEXT REFERENCES rebuild_tasks(task_id),
           attempt_id TEXT REFERENCES rebuild_attempts(attempt_id),
           event_type TEXT NOT NULL,
           artifact_id TEXT REFERENCES rebuild_artifacts(artifact_id),
           created_at TEXT NOT NULL
         );
         CREATE INDEX IF NOT EXISTS rebuild_tasks_claimable
           ON rebuild_tasks(status, ready_at, priority);
         CREATE INDEX IF NOT EXISTS rebuild_events_cursor
           ON rebuild_events(run_id, event_id);
         COMMIT;",
    )?;
    let version = connection
        .query_row(
            "SELECT value FROM rebuild_metadata WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    match version.as_deref() {
        None => {
            connection.execute(
                "INSERT INTO rebuild_metadata (key, value) VALUES ('schema_version', ?1)",
                params![REBUILD_SCHEMA_VERSION.to_string()],
            )?;
        }
        Some(value) if value == REBUILD_SCHEMA_VERSION.to_string() => {}
        Some(_) => {
            return Err(RebuildStoreError::IncompatibleStoreRoot(PathBuf::from(
                DATABASE_FILE,
            )));
        }
    }
    Ok(())
}

fn insert_artifact(transaction: &Transaction<'_>, artifact: &Artifact) -> RebuildStoreResult<()> {
    artifact.validate()?;
    for source in &artifact.source_refs {
        let exists = transaction
            .query_row(
                "SELECT kind FROM rebuild_artifacts WHERE artifact_id = ?1",
                params![source.artifact_id.0.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if exists.as_deref() != Some(&enum_name(source.kind)) {
            return Err(RebuildStoreError::InvalidArtifactClosure(
                artifact.artifact_id.clone(),
            ));
        }
    }
    let inserted = transaction.execute(
        r#"INSERT OR IGNORE INTO rebuild_artifacts
           (artifact_id, kind, blob_hash, media_type, bytes, producer, lifecycle, provenance_json, origin_json, created_at)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"#,
        params![
            artifact.artifact_id.0.as_str(),
            enum_name(artifact.kind),
            artifact.blob.hash.as_str(),
            artifact.blob.media_type,
            artifact.blob.bytes,
            artifact.producer,
            enum_name(artifact.lifecycle),
            serde_json::to_string(&artifact.provenance)?,
            serde_json::to_string(&artifact.origin)?,
            artifact.created_at.to_rfc3339(),
        ],
    )?;
    if inserted == 0 {
        let existing = read_artifact(transaction, &artifact.artifact_id)?;
        if &existing != artifact {
            return Err(RebuildStoreError::Integrity(format!(
                "artifact hash collision {}",
                artifact.artifact_id.0
            )));
        }
        return Ok(());
    }
    for source in &artifact.source_refs {
        transaction.execute(
            r#"INSERT INTO rebuild_artifact_refs
               (artifact_id, source_artifact_id, source_kind)
               VALUES (?1, ?2, ?3)"#,
            params![
                artifact.artifact_id.0.as_str(),
                source.artifact_id.0.as_str(),
                enum_name(source.kind),
            ],
        )?;
    }
    Ok(())
}

fn insert_node(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    node: &WorkflowNode,
    created_at: DateTime<Utc>,
) -> RebuildStoreResult<()> {
    let inserted = transaction.execute(
        r#"INSERT INTO rebuild_tasks
           (task_id, run_id, recipe_id, objective, contract_hash, priority, budget_json, retry_json, on_failure,
            parent_task_id, status, ready_at)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'queued', ?11)"#,
        params![
            node.task_id.0,
            run_id.0,
            node.recipe_id.as_str(),
            node.objective,
            node.contract_hash.as_ref().map(ContentHash::as_str),
            node.priority,
            serde_json::to_string(&node.budget)?,
            serde_json::to_string(&node.retry)?,
            enum_name(node.on_failure),
            node.parent_task_id.as_ref().map(|id| id.0.as_str()),
            created_at.to_rfc3339(),
        ],
    )?;
    if inserted != 1 {
        return Err(RebuildStoreError::DuplicateTask(node.task_id.clone()));
    }
    for dependency in &node.dependencies {
        transaction.execute(
            "INSERT INTO rebuild_task_dependencies (task_id, depends_on_task_id) VALUES (?1, ?2)",
            params![node.task_id.0, dependency.0],
        )?;
    }
    Ok(())
}

fn assert_permit(transaction: &Transaction<'_>, permit: &TaskWritePermit) -> RebuildStoreResult<()> {
    let current = transaction
        .query_row(
            r#"SELECT run_id, status, lease_id, lease_epoch, active_attempt_id, contract_hash
               FROM rebuild_tasks WHERE task_id = ?1"#,
            params![permit.task_id.0],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, u64>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            },
        )
        .optional()?;
    let Some((run_id, status, lease_id, epoch, attempt_id, contract_hash)) = current else {
        return Err(RebuildStoreError::MissingTask(permit.task_id.clone()));
    };
    if run_id != permit.run_id.0
        || status != "running"
        || lease_id.as_deref() != Some(permit.lease_id.0.as_str())
        || epoch != permit.epoch
        || attempt_id.as_deref() != Some(permit.attempt_id.0.as_str())
        || contract_hash
            .as_deref()
            .map(ContentHash::new)
            .transpose()?
            != permit.contract_hash
    {
        return Err(RebuildStoreError::StalePermit(permit.task_id.clone()));
    }
    Ok(())
}

fn assert_origin_matches(
    origin: Option<&ArtifactOrigin>,
    permit: &TaskWritePermit,
) -> RebuildStoreResult<()> {
    let Some(origin) = origin else {
        return Err(RebuildStoreError::PermitOriginMismatch);
    };
    if origin.run_id.as_ref() != Some(&permit.run_id)
        || origin.task_id.as_ref() != Some(&permit.task_id)
        || origin.attempt_id.as_ref() != Some(&permit.attempt_id)
        || origin.contract_hash != permit.contract_hash
    {
        return Err(RebuildStoreError::PermitOriginMismatch);
    }
    Ok(())
}

fn append_event(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    task_id: Option<&TaskId>,
    attempt_id: Option<&akzio_domain::AttemptId>,
    event_type: &str,
    artifact_id: Option<&ArtifactId>,
    created_at: DateTime<Utc>,
) -> RebuildStoreResult<()> {
    transaction.execute(
        r#"INSERT INTO rebuild_events
           (run_id, task_id, attempt_id, event_type, artifact_id, created_at)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
        params![
            run_id.0,
            task_id.map(|id| id.0.as_str()),
            attempt_id.map(|id| id.0.as_str()),
            event_type,
            artifact_id.map(|id| id.0.as_str()),
            created_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn refresh_run_status(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    now: DateTime<Utc>,
) -> RebuildStoreResult<()> {
    let statuses = transaction
        .prepare("SELECT status FROM rebuild_tasks WHERE run_id = ?1")?
        .query_map(params![run_id.0], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    if statuses.is_empty() || statuses.iter().any(|status| status == "running" || status == "queued") {
        return Ok(());
    }
    let status = if statuses.iter().any(|status| status == "failed") {
        "failed"
    } else if statuses.iter().all(|status| status == "cancelled") {
        "cancelled"
    } else {
        "completed"
    };
    transaction.execute(
        "UPDATE rebuild_runs SET status = ?1, finished_at = ?2 WHERE run_id = ?3",
        params![status, now.to_rfc3339(), run_id.0],
    )?;
    Ok(())
}

fn read_artifact(connection: &Connection, artifact_id: &ArtifactId) -> RebuildStoreResult<Artifact> {
    let row = connection
        .query_row(
            r#"SELECT kind, blob_hash, media_type, bytes, producer, lifecycle, provenance_json, origin_json, created_at
               FROM rebuild_artifacts WHERE artifact_id = ?1"#,
            params![artifact_id.0.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, u64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, String>(8)?,
                ))
            },
        )
        .optional()?;
    let Some((kind, hash, media_type, bytes, producer, lifecycle, provenance, origin, created_at)) = row else {
        return Err(RebuildStoreError::MissingArtifact(artifact_id.clone()));
    };
    let mut statement = connection.prepare(
        r#"SELECT source_artifact_id, source_kind
           FROM rebuild_artifact_refs WHERE artifact_id = ?1
           ORDER BY source_artifact_id"#,
    )?;
    let source_refs = statement
        .query_map(params![artifact_id.0.as_str()], |row| {
            Ok(ArtifactRef {
                artifact_id: ArtifactId(
                    ContentHash::new(row.get::<_, String>(0)?)
                        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
                ),
                kind: parse_enum(&row.get::<_, String>(1)?)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Artifact {
        schema_version: REBUILD_SCHEMA_VERSION,
        artifact_id: artifact_id.clone(),
        kind: parse_enum(&kind)?,
        blob: BlobRef {
            hash: ContentHash::new(hash)?,
            media_type,
            bytes,
        },
        producer,
        lifecycle: parse_enum(&lifecycle)?,
        provenance: serde_json::from_str(&provenance)?,
        origin: origin
            .map(|encoded| serde_json::from_str::<Option<ArtifactOrigin>>(&encoded))
            .transpose()?
            .flatten(),
        source_refs,
        created_at: parse_time(&created_at)?,
    })
}

fn row_to_node(row: &rusqlite::Row<'_>) -> rusqlite::Result<(RunId, WorkflowNode)> {
    let task_id = TaskId(row.get(0)?);
    let run_id = RunId(row.get(1)?);
    let recipe_id = TaskRecipeId::new(row.get::<_, String>(2)?)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let budget = serde_json::from_str(&row.get::<_, String>(6)?)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let retry = serde_json::from_str(&row.get::<_, String>(7)?)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let on_failure = parse_enum(&row.get::<_, String>(8)?)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    Ok((
        run_id,
        WorkflowNode {
            task_id,
            recipe_id,
            contract_hash: row
                .get::<_, Option<String>>(4)?
                .map(ContentHash::new)
                .transpose()
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
            objective: row.get(3)?,
            dependencies: Vec::new(),
            input_artifacts: Vec::new(),
            priority: row.get(5)?,
            budget,
            retry,
            on_failure,
            parent_task_id: row.get::<_, Option<String>>(9)?.map(TaskId),
        },
    ))
}

fn parse_time(value: &str) -> RebuildStoreResult<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|error| RebuildStoreError::Integrity(format!("invalid time {value}: {error}")))
}

fn enum_name<T: Serialize>(value: T) -> String {
    serde_json::to_value(value)
        .expect("enum serializes")
        .as_str()
        .expect("enum serializes as string")
        .to_owned()
}

fn parse_enum<T: for<'de> serde::Deserialize<'de>>(value: &str) -> RebuildStoreResult<T> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .map_err(RebuildStoreError::Json)
}

#[cfg(test)]
mod tests {
    use akzio_domain::{
        ArtifactLifecycle, ArtifactProvenance, FailureDisposition, RetryPolicy, TaskBudget,
        TaskRecipeId,
    };
    use tempfile::tempdir;

    use super::*;

    fn budget() -> TaskBudget {
        TaskBudget {
            max_input_tokens: 32,
            max_output_tokens: 16,
            max_wall_time_secs: 10,
            max_tool_calls: 1,
        }
    }

    fn retry() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 1,
            retry_transport: true,
            retry_rate_limited: true,
            retry_invalid_output: false,
        }
    }

    fn artifact(store: &RebuildStore, kind: ArtifactKind, value: &str, origin: Option<ArtifactOrigin>) -> Artifact {
        Artifact::new(
            kind,
            store.put_bytes(value.as_bytes(), "application/json").unwrap(),
            "fixture",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "fixture".to_owned(),
                observed_at: None,
                retrieved_at: Utc::now(),
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            origin,
            vec![],
            Utc::now(),
        )
        .unwrap()
    }

    fn graph() -> WorkflowGraph {
        WorkflowGraph {
            schema_version: REBUILD_SCHEMA_VERSION,
            topology_id: "active".to_owned(),
            nodes: vec![WorkflowNode {
                task_id: TaskId::new(),
                recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                contract_hash: None,
                objective: "analyze".to_owned(),
                dependencies: vec![],
                input_artifacts: vec![],
                priority: 50,
                budget: budget(),
                retry: retry(),
                on_failure: FailureDisposition::FailRun,
                parent_task_id: None,
            }],
        }
    }

    #[test]
    fn legacy_root_is_rejected_instead_of_migrated() {
        let root = tempdir().unwrap();
        fs::write(root.path().join(LEGACY_DATABASE_FILE), b"legacy").unwrap();
        assert!(matches!(
            RebuildStore::open(root.path()),
            Err(RebuildStoreError::IncompatibleStoreRoot(_))
        ));
    }

    #[test]
    fn workflow_commit_is_atomic_and_claim_yields_a_permit() {
        let root = tempdir().unwrap();
        let store = RebuildStore::open(root.path()).unwrap();
        let graph = graph();
        let graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&graph).unwrap(),
            None,
        );
        let run = RebuildRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Debug,
            topology_id: graph.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: Utc::now(),
        };
        store
            .commit_workflow(&WorkflowCommit {
                run: run.clone(),
                graph: graph_artifact,
                nodes: graph.nodes.clone(),
            })
            .unwrap();
        let claimed = store
            .claim_next_task("worker", Utc::now(), Duration::seconds(30))
            .unwrap()
            .unwrap();
        assert_eq!(claimed.run_id, run.run_id);
        assert_eq!(claimed.node.task_id, graph.nodes[0].task_id);
        assert_eq!(store.events_after(&run.run_id, 0, 10).unwrap().len(), 2);
        store.verify_integrity().unwrap();
    }

    #[test]
    fn stale_permit_cannot_write_an_artifact() {
        let root = tempdir().unwrap();
        let store = RebuildStore::open(root.path()).unwrap();
        let graph = graph();
        let graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&graph).unwrap(),
            None,
        );
        let run = RebuildRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Debug,
            topology_id: graph.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: Utc::now(),
        };
        store
            .commit_workflow(&WorkflowCommit {
                run,
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();
        let claimed = store
            .claim_next_task("worker", Utc::now(), Duration::milliseconds(-1))
            .unwrap()
            .unwrap();
        store.recover_expired_tasks(Utc::now()).unwrap();
        let artifact = artifact(
            &store,
            ArtifactKind::Claim,
            "claim",
            Some(ArtifactOrigin {
                run_id: Some(claimed.permit.run_id.clone()),
                task_id: Some(claimed.permit.task_id.clone()),
                attempt_id: Some(claimed.permit.attempt_id.clone()),
                contract_hash: None,
            }),
        );
        assert!(matches!(
            store.write_task_artifact(&claimed.permit, &artifact, "claim.created", Utc::now()),
            Err(RebuildStoreError::StalePermit(_))
        ));
    }

    #[test]
    fn bootstrapped_contract_must_not_carry_task_origin() {
        let root = tempdir().unwrap();
        let store = RebuildStore::open(root.path()).unwrap();
        let artifact = artifact(&store, ArtifactKind::Contract, "contract", None);
        store.write_bootstrap_artifact(&artifact).unwrap();
        store.verify_integrity().unwrap();
    }
}
