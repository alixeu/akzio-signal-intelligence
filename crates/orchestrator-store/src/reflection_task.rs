//! Append-only, outcome-keyed Historical Reflection task ledger.

use std::{collections::BTreeSet, fs, path::PathBuf};

use orchestrator_core::{
    DocumentRef, HistoricalReflectionArtifactV1, ReflectionDisposition, ReflectionTaskKeyV1,
    ReflectionTaskStatus, HISTORICAL_REFLECTION_ARTIFACT_SCHEMA_VERSION,
    REFLECTION_TASK_SCHEMA_VERSION,
};
use serde::{Deserialize, Serialize};

use crate::{
    content_hash, ContentHashDocument, FileSchemaKind, FileStore, JsonlRecord, Result, SafeSlug,
    StoreError, Versioned,
};

pub const REFLECTION_TASK_EVENT_SCHEMA_VERSION: u32 = 1;

impl Versioned for HistoricalReflectionArtifactV1 {
    const SCHEMA_VERSION: u32 = HISTORICAL_REFLECTION_ARTIFACT_SCHEMA_VERSION;
}
impl ContentHashDocument for HistoricalReflectionArtifactV1 {
    fn content_hash(&self) -> &str {
        &self.content_hash
    }
    fn set_content_hash(&mut self, hash: String) {
        self.content_hash = hash;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReflectionTaskV1 {
    pub schema_version: u32,
    pub task_id: String,
    pub key: ReflectionTaskKeyV1,
    pub outcome_ref: DocumentRef,
    pub status: ReflectionTaskStatus,
    pub attempt_count: u32,
    pub claimed_by_run_id: Option<String>,
    pub artifact_ref: Option<DocumentRef>,
    pub terminal_reason: Option<String>,
    pub updated_at: String,
    pub content_hash: String,
}

impl Versioned for ReflectionTaskV1 {
    const SCHEMA_VERSION: u32 = REFLECTION_TASK_SCHEMA_VERSION;
}
impl ContentHashDocument for ReflectionTaskV1 {
    fn content_hash(&self) -> &str {
        &self.content_hash
    }
    fn set_content_hash(&mut self, hash: String) {
        self.content_hash = hash;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReflectionTaskEventV1 {
    pub schema_version: u32,
    pub sequence: u64,
    pub task_id: String,
    pub from_status: Option<ReflectionTaskStatus>,
    pub to_status: ReflectionTaskStatus,
    pub actor_run_id: Option<String>,
    pub reason: Option<String>,
    pub created_at: String,
    pub content_hash: String,
}

impl JsonlRecord for ReflectionTaskEventV1 {
    const SCHEMA_VERSION: u32 = REFLECTION_TASK_EVENT_SCHEMA_VERSION;
    fn schema_version(&self) -> u32 {
        self.schema_version
    }
    fn sequence(&self) -> u64 {
        self.sequence
    }
    fn validate_record(&self) -> std::result::Result<(), String> {
        if self.schema_version != Self::SCHEMA_VERSION
            || self.sequence == 0
            || self.task_id.trim().is_empty()
            || self.created_at.trim().is_empty()
        {
            return Err("schema, sequence, task identity, or timestamp is invalid".to_owned());
        }
        let value = serde_json::to_value(self).map_err(|error| error.to_string())?;
        let expected = content_hash(&value).map_err(|error| error.to_string())?;
        if self.content_hash != expected {
            return Err("event content hash mismatch".to_owned());
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ReflectionTaskLedger {
    store: FileStore,
}

impl ReflectionTaskLedger {
    pub fn new(store: FileStore) -> Self {
        Self { store }
    }

    pub fn create_or_read(
        &self,
        key: ReflectionTaskKeyV1,
        outcome_ref: DocumentRef,
        now: &str,
    ) -> Result<ReflectionTaskV1> {
        validate_key(&key, &outcome_ref)?;
        let task_id = content_hash(&serde_json::json!({"key": key}))?;
        let path = task_path(&task_id)?;
        let _lock = self.store.lock_exclusive(&path)?;
        if self.store.exists(&path)? {
            return self
                .store
                .read_versioned_json(&path, FileSchemaKind::ReflectionTask);
        }
        let task = ReflectionTaskV1 {
            schema_version: REFLECTION_TASK_SCHEMA_VERSION,
            task_id: task_id.clone(),
            key,
            outcome_ref,
            status: ReflectionTaskStatus::Pending,
            attempt_count: 0,
            claimed_by_run_id: None,
            artifact_ref: None,
            terminal_reason: None,
            updated_at: now.to_owned(),
            content_hash: String::new(),
        };
        let task = self.store.write_authoritative_json(&path, task)?;
        self.append_event(&task, None, None, None, now)?;
        Ok(task)
    }

    pub fn read(&self, task_id: &str) -> Result<ReflectionTaskV1> {
        self.store
            .read_versioned_json(&task_path(task_id)?, FileSchemaKind::ReflectionTask)
    }

    /// List validated task snapshots for a scheduler.  Events remain the
    /// audit trail; snapshots are a deterministic current-state projection.
    pub fn list_tasks(&self) -> Result<Vec<ReflectionTaskV1>> {
        let directory = PathBuf::from("knowledge/reflection/tasks");
        let absolute = self.store.root().join(&directory);
        if !absolute.exists() {
            return Ok(Vec::new());
        }
        let mut tasks: Vec<ReflectionTaskV1> = Vec::new();
        for entry in fs::read_dir(&absolute).map_err(|source| StoreError::Io {
            path: absolute.clone(),
            source,
        })? {
            let entry = entry.map_err(|source| StoreError::Io {
                path: absolute.clone(),
                source,
            })?;
            if !entry
                .file_type()
                .map_err(|source| StoreError::Io {
                    path: entry.path(),
                    source,
                })?
                .is_file()
                || entry
                    .path()
                    .extension()
                    .is_none_or(|extension| extension != "json")
            {
                continue;
            }
            tasks.push(self.store.read_versioned_json(
                &directory.join(entry.file_name()),
                FileSchemaKind::ReflectionTask,
            )?);
        }
        tasks.sort_by(|left, right| {
            (&left.updated_at, &left.task_id).cmp(&(&right.updated_at, &right.task_id))
        });
        Ok(tasks)
    }

    pub fn claim(
        &self,
        task_id: &str,
        run_id: &str,
        now: &str,
    ) -> Result<Option<ReflectionTaskV1>> {
        self.transition(task_id, run_id, now, |task| {
            if !matches!(
                task.status,
                ReflectionTaskStatus::Pending | ReflectionTaskStatus::FailedRetryable
            ) {
                return Ok(false);
            }
            task.status = ReflectionTaskStatus::Claimed;
            task.claimed_by_run_id = Some(run_id.to_owned());
            task.attempt_count = task.attempt_count.saturating_add(1);
            Ok(true)
        })
    }

    pub fn complete(
        &self,
        task_id: &str,
        actor_run_id: &str,
        disposition: ReflectionDisposition,
        artifact_ref: DocumentRef,
        reason: Option<String>,
        now: &str,
    ) -> Result<ReflectionTaskV1> {
        let status = match disposition {
            ReflectionDisposition::Learned => ReflectionTaskStatus::Completed,
            ReflectionDisposition::NoReusableMemory => ReflectionTaskStatus::NoReusableMemory,
            ReflectionDisposition::Deferred => ReflectionTaskStatus::Deferred,
            ReflectionDisposition::Contested => ReflectionTaskStatus::Contested,
        };
        self.transition(task_id, actor_run_id, now, |task| {
            if task.status != ReflectionTaskStatus::Claimed
                || task.claimed_by_run_id.as_deref() != Some(actor_run_id)
            {
                return Err(invalid(
                    "reflection task",
                    "only this run's claimed task may complete",
                ));
            }
            task.status = status;
            task.artifact_ref = Some(artifact_ref);
            task.terminal_reason = reason;
            Ok(true)
        })?
        .ok_or_else(|| invalid("reflection task", "task completion was not applied"))
    }

    pub fn mark_duplicate(
        &self,
        task_id: &str,
        actor_run_id: &str,
        artifact_ref: DocumentRef,
        now: &str,
    ) -> Result<ReflectionTaskV1> {
        self.transition(task_id, actor_run_id, now, |task| {
            if task.status != ReflectionTaskStatus::Claimed
                || task.claimed_by_run_id.as_deref() != Some(actor_run_id)
            {
                return Err(invalid(
                    "reflection task",
                    "only this run's claimed task may become duplicate",
                ));
            }
            task.status = ReflectionTaskStatus::Duplicate;
            task.artifact_ref = Some(artifact_ref);
            Ok(true)
        })?
        .ok_or_else(|| {
            invalid(
                "reflection task",
                "task duplicate transition was not applied",
            )
        })
    }

    /// A reflector failure is operational evidence, not a workflow failure.
    /// Keep it in the authoritative Task Ledger so a later scheduler can
    /// retry within a bounded attempt budget without blocking the investment
    /// run that happened to discover the matured Outcome.
    pub fn mark_failed(
        &self,
        task_id: &str,
        actor_run_id: &str,
        reason: String,
        max_attempts: u32,
        now: &str,
    ) -> Result<ReflectionTaskV1> {
        self.transition(task_id, actor_run_id, now, |task| {
            if task.status != ReflectionTaskStatus::Claimed
                || task.claimed_by_run_id.as_deref() != Some(actor_run_id)
            {
                return Err(invalid(
                    "reflection task",
                    "only this run's claimed task may record a failure",
                ));
            }
            task.status = if task.attempt_count >= max_attempts.max(1) {
                ReflectionTaskStatus::FailedPermanent
            } else {
                ReflectionTaskStatus::FailedRetryable
            };
            task.terminal_reason = Some(reason);
            Ok(true)
        })?
        .ok_or_else(|| invalid("reflection task", "task failure transition was not applied"))
    }

    /// Complete a claimed task after the specialized Experience service
    /// performs its idempotent write.
    pub fn complete_learned_with(
        &self,
        task_id: &str,
        actor_run_id: &str,
        artifact_ref: DocumentRef,
        now: &str,
        write_case: impl FnOnce() -> Result<crate::ExperienceCaseDisposition>,
    ) -> Result<ReflectionTaskV1> {
        let path = task_path(task_id)?;
        let _lock = self.store.lock_exclusive(&path)?;
        let mut task: ReflectionTaskV1 = self
            .store
            .read_versioned_json(&path, FileSchemaKind::ReflectionTask)?;
        if task.status != ReflectionTaskStatus::Claimed
            || task.claimed_by_run_id.as_deref() != Some(actor_run_id)
        {
            return Err(invalid(
                "reflection task",
                "only this run's claimed task may write a learned case",
            ));
        }
        let from = task.status;
        let case = write_case()?;
        task.status = match case {
            crate::ExperienceCaseDisposition::DuplicateSourceRun => ReflectionTaskStatus::Duplicate,
            crate::ExperienceCaseDisposition::Created
            | crate::ExperienceCaseDisposition::Appended => ReflectionTaskStatus::Completed,
        };
        task.artifact_ref = Some(artifact_ref);
        task.updated_at = now.to_owned();
        let task = self.store.write_authoritative_json(&path, task)?;
        self.append_event(&task, Some(from), Some(actor_run_id), None, now)?;
        Ok(task)
    }

    /// Reconciliation is performed by the Rust scheduler before it claims
    /// current Outcomes. A task for an Outcome that is no longer current must
    /// never reach a reflector terminal, even when another worker had already
    /// claimed it before a data revision arrived.
    pub fn supersede_non_current_outcomes(
        &self,
        current_outcome_ids: &BTreeSet<String>,
        actor_run_id: &str,
        now: &str,
    ) -> Result<u32> {
        let directory = PathBuf::from("knowledge/reflection/tasks");
        let absolute = self.store.root().join(&directory);
        if !absolute.exists() {
            return Ok(0);
        }
        let mut task_ids = Vec::new();
        for entry in fs::read_dir(&absolute).map_err(|source| StoreError::Io {
            path: absolute.clone(),
            source,
        })? {
            let entry = entry.map_err(|source| StoreError::Io {
                path: absolute.clone(),
                source,
            })?;
            if !entry
                .file_type()
                .map_err(|source| StoreError::Io {
                    path: entry.path(),
                    source,
                })?
                .is_file()
                || entry
                    .path()
                    .extension()
                    .is_none_or(|extension| extension != "json")
            {
                continue;
            }
            let relative = directory.join(entry.file_name());
            let task: ReflectionTaskV1 = self
                .store
                .read_versioned_json(&relative, FileSchemaKind::ReflectionTask)?;
            if !current_outcome_ids.contains(&task.key.outcome_id) {
                task_ids.push(task.task_id);
            }
        }
        let mut changed = 0;
        for task_id in task_ids {
            if self
                .transition(&task_id, actor_run_id, now, |task| {
                    if !matches!(
                        task.status,
                        ReflectionTaskStatus::Pending
                            | ReflectionTaskStatus::Claimed
                            | ReflectionTaskStatus::FailedRetryable
                    ) {
                        return Ok(false);
                    }
                    task.status = ReflectionTaskStatus::Superseded;
                    task.terminal_reason = Some(
                        "outcome is no longer the current revision for its evaluation key"
                            .to_owned(),
                    );
                    Ok(true)
                })?
                .is_some()
            {
                changed += 1;
            }
        }
        Ok(changed)
    }

    pub fn write_artifact(&self, artifact: HistoricalReflectionArtifactV1) -> Result<DocumentRef> {
        validate_artifact(&artifact)?;
        let relative = artifact_path(&artifact.artifact_id)?;
        let _lock = self.store.lock_exclusive(&relative)?;
        let sealed = crate::seal_content_hash(artifact.clone())?;
        if self.store.exists(&relative)? {
            let existing: HistoricalReflectionArtifactV1 = self
                .store
                .read_versioned_json(&relative, FileSchemaKind::HistoricalReflectionArtifact)?;
            if existing.content_hash != sealed.content_hash {
                return Err(invalid(
                    "historical reflection artifact",
                    "immutable artifact identity conflicts",
                ));
            }
            return Ok(DocumentRef {
                document_id: existing.artifact_id,
                relative_path: relative.to_string_lossy().to_string(),
                content_hash: existing.content_hash,
            });
        }
        let sealed = self.store.write_authoritative_json(&relative, artifact)?;
        Ok(DocumentRef {
            document_id: sealed.artifact_id,
            relative_path: relative.to_string_lossy().to_string(),
            content_hash: sealed.content_hash,
        })
    }

    fn transition(
        &self,
        task_id: &str,
        actor: &str,
        now: &str,
        apply: impl FnOnce(&mut ReflectionTaskV1) -> Result<bool>,
    ) -> Result<Option<ReflectionTaskV1>> {
        let path = task_path(task_id)?;
        let _lock = self.store.lock_exclusive(&path)?;
        let mut task: ReflectionTaskV1 = self
            .store
            .read_versioned_json(&path, FileSchemaKind::ReflectionTask)?;
        let from = task.status;
        if !apply(&mut task)? {
            return Ok(None);
        }
        task.updated_at = now.to_owned();
        let task = self.store.write_authoritative_json(&path, task)?;
        self.append_event(
            &task,
            Some(from),
            Some(actor),
            task.terminal_reason.clone(),
            now,
        )?;
        Ok(Some(task))
    }

    fn append_event(
        &self,
        task: &ReflectionTaskV1,
        from: Option<ReflectionTaskStatus>,
        actor: Option<&str>,
        reason: Option<String>,
        now: &str,
    ) -> Result<()> {
        let events = event_path(&task.task_id)?;
        crate::jsonl::append_jsonl_transaction::<ReflectionTaskEventV1>(
            self.store.root(),
            &events,
            move |previous| {
                let sequence = previous.last().map_or(1, |event| event.sequence + 1);
                let mut event = ReflectionTaskEventV1 {
                    schema_version: REFLECTION_TASK_EVENT_SCHEMA_VERSION,
                    sequence,
                    task_id: task.task_id.clone(),
                    from_status: from,
                    to_status: task.status,
                    actor_run_id: actor.map(ToOwned::to_owned),
                    reason,
                    created_at: now.to_owned(),
                    content_hash: String::new(),
                };
                event.content_hash = content_hash(
                    &serde_json::to_value(&event)
                        .map_err(|source| StoreError::JsonSerialize { source })?,
                )?;
                Ok(Some(event))
            },
        )?;
        Ok(())
    }
}

fn validate_key(key: &ReflectionTaskKeyV1, outcome_ref: &DocumentRef) -> Result<()> {
    if key.source_run_id.trim().is_empty()
        || key.ticker.trim().is_empty()
        || key.outcome_id.trim().is_empty()
        || key.outcome_content_hash.trim().is_empty()
        || key.policy_ref.content_hash.trim().is_empty()
        || key.profile_version == 0
        || key.builder_version == 0
        || outcome_ref.document_id != key.outcome_id
        || outcome_ref.content_hash != key.outcome_content_hash
    {
        return Err(invalid(
            "reflection task key",
            "provenance or identity is invalid",
        ));
    }
    Ok(())
}
fn task_path(task_id: &str) -> Result<PathBuf> {
    Ok(PathBuf::from("knowledge/reflection/tasks")
        .join(format!("{}.json", SafeSlug::new("task", task_id)?.as_str())))
}
fn event_path(task_id: &str) -> Result<PathBuf> {
    Ok(PathBuf::from("knowledge/reflection/events").join(format!(
        "{}.jsonl",
        SafeSlug::new("task", task_id)?.as_str()
    )))
}
fn artifact_path(artifact_id: &str) -> Result<PathBuf> {
    Ok(
        PathBuf::from("knowledge/reflection/artifacts").join(format!(
            "{}.json",
            SafeSlug::new("artifact", artifact_id)?.as_str()
        )),
    )
}
fn invalid(kind: &'static str, message: impl Into<String>) -> StoreError {
    StoreError::InvalidDocument {
        kind,
        message: message.into(),
    }
}

fn validate_artifact(artifact: &HistoricalReflectionArtifactV1) -> Result<()> {
    if artifact.schema_version != HISTORICAL_REFLECTION_ARTIFACT_SCHEMA_VERSION
        || artifact.artifact_id.trim().is_empty()
        || artifact.task_id.trim().is_empty()
        || artifact.summary.trim().is_empty()
        || artifact.detail.trim().is_empty()
        || artifact.outcome_ref.document_id != artifact.task_key.outcome_id
        || artifact.outcome_ref.content_hash != artifact.task_key.outcome_content_hash
        || artifact.source_refs.is_empty()
        || artifact.source_refs.iter().any(|reference| {
            reference.document_id.trim().is_empty()
                || reference.relative_path.trim().is_empty()
                || reference.content_hash.trim().is_empty()
        })
    {
        return Err(invalid(
            "historical reflection artifact",
            "identity, disposition, or provenance is invalid",
        ));
    }
    if let Some(phase) = artifact.root_cause_phase {
        if phase == 0 || phase > 8 {
            return Err(invalid(
                "historical reflection artifact",
                "root cause phase is invalid",
            ));
        }
    }
    if artifact
        .propagation_phases
        .iter()
        .any(|phase| *phase == 0 || *phase > 8)
    {
        return Err(invalid(
            "historical reflection artifact",
            "propagation phase is invalid",
        ));
    }
    let learned = artifact.disposition == ReflectionDisposition::Learned;
    let contested = artifact.disposition == ReflectionDisposition::Contested;
    if learned != (artifact.pattern_identity.is_some() && artifact.rule_revision.is_some()) {
        return Err(invalid(
            "historical reflection artifact",
            "only Learned artifacts require both a PatternIdentity and RuleRevision",
        ));
    }
    if !learned
        && !contested
        && (artifact.pattern_identity.is_some() || artifact.rule_revision.is_some())
    {
        return Err(invalid(
            "historical reflection artifact",
            "only Learned or Contested artifacts may identify a Pattern",
        ));
    }
    if contested && artifact.rule_revision.is_some() {
        return Err(invalid(
            "historical reflection artifact",
            "Contested artifacts may not create a RuleRevision",
        ));
    }
    if let Some(pattern) = &artifact.pattern_identity {
        if pattern.root_cause_phase == 0
            || pattern.root_cause_phase > 8
            || artifact.root_cause_phase != Some(pattern.root_cause_phase)
            || pattern.source_role.trim().is_empty()
            || (matches!(pattern.scope, orchestrator_core::Scope::Ticker)
                && pattern.ticker.as_deref() != Some(artifact.task_key.ticker.as_str()))
        {
            return Err(invalid(
                "historical reflection artifact",
                "PatternIdentity is not consistent with the task provenance",
            ));
        }
    }
    if let Some(rule) = &artifact.rule_revision {
        if rule.revision == 0
            || rule.rule.trim().is_empty()
            || rule
                .trigger_conditions
                .iter()
                .any(|value| value.trim().is_empty())
            || rule
                .invalidation_conditions
                .iter()
                .any(|value| value.trim().is_empty())
        {
            return Err(invalid(
                "historical reflection artifact",
                "RuleRevision is invalid",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use super::*;
    use orchestrator_core::PolicyRef;
    use tempfile::tempdir;

    fn key() -> ReflectionTaskKeyV1 {
        ReflectionTaskKeyV1 {
            source_run_id: "source-run".into(),
            ticker: "QQQ".into(),
            outcome_id: "outcome".into(),
            outcome_content_hash: "sha256:outcome".into(),
            policy_ref: PolicyRef {
                policy_id: "policy".into(),
                version: 1,
                content_hash: "sha256:policy".into(),
            },
            profile_version: 1,
            builder_version: 1,
        }
    }
    fn outcome_ref() -> DocumentRef {
        DocumentRef {
            document_id: "outcome".into(),
            relative_path: "knowledge/evaluation/outcomes/outcome.json".into(),
            content_hash: "sha256:outcome".into(),
        }
    }
    #[test]
    fn claimed_task_has_one_terminal_event_and_duplicate_is_rust_only() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), crate::FileStoreOptions::default()).unwrap();
        let ledger = ReflectionTaskLedger::new(store.clone());
        let first = ledger
            .create_or_read(key(), outcome_ref(), "2026-01-01T00:00:00Z")
            .unwrap();
        let again = ledger
            .create_or_read(key(), outcome_ref(), "2026-01-01T00:00:00Z")
            .unwrap();
        assert_eq!(first.task_id, again.task_id);
        assert!(ledger
            .claim(&first.task_id, "reflection-run", "2026-01-02T00:00:00Z")
            .unwrap()
            .is_some());
        let artifact = DocumentRef {
            document_id: "artifact".into(),
            relative_path: "runs/x/artifacts/reflection.json".into(),
            content_hash: "sha256:artifact".into(),
        };
        let terminal = ledger
            .mark_duplicate(
                &first.task_id,
                "reflection-run",
                artifact,
                "2026-01-02T00:01:00Z",
            )
            .unwrap();
        assert_eq!(terminal.status, ReflectionTaskStatus::Duplicate);
        assert!(ledger
            .claim(&first.task_id, "other", "2026-01-03T00:00:00Z")
            .unwrap()
            .is_none());
    }

    #[test]
    fn only_the_claiming_run_may_complete_or_mark_a_task_duplicate() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), crate::FileStoreOptions::default()).unwrap();
        let ledger = ReflectionTaskLedger::new(store);
        let task = ledger
            .create_or_read(key(), outcome_ref(), "2026-01-01T00:00:00Z")
            .unwrap();
        ledger
            .claim(&task.task_id, "run-a", "2026-01-02T00:00:00Z")
            .unwrap();
        let artifact = DocumentRef {
            document_id: "artifact".into(),
            relative_path: "knowledge/reflection/artifacts/artifact.json".into(),
            content_hash: "sha256:artifact".into(),
        };

        assert!(ledger
            .mark_duplicate(
                &task.task_id,
                "run-b",
                artifact.clone(),
                "2026-01-02T00:01:00Z",
            )
            .is_err());
        assert!(ledger
            .complete(
                &task.task_id,
                "run-b",
                ReflectionDisposition::NoReusableMemory,
                artifact,
                None,
                "2026-01-02T00:01:00Z",
            )
            .is_err());
    }

    #[test]
    fn concurrent_claims_have_exactly_one_owner_and_a_complete_event_log() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), crate::FileStoreOptions::default()).unwrap();
        let ledger = ReflectionTaskLedger::new(store);
        let task = ledger
            .create_or_read(key(), outcome_ref(), "2026-01-01T00:00:00Z")
            .unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let left = ledger.clone();
        let right = ledger.clone();
        let task_id = task.task_id.clone();

        let (left_result, right_result) = std::thread::scope(|scope| {
            let left_barrier = Arc::clone(&barrier);
            let left_task_id = task_id.clone();
            let left = scope.spawn(move || {
                left_barrier.wait();
                left.claim(&left_task_id, "run-left", "2026-01-02T00:00:00Z")
            });
            let right_barrier = Arc::clone(&barrier);
            let right = scope.spawn(move || {
                right_barrier.wait();
                right.claim(&task_id, "run-right", "2026-01-02T00:00:00Z")
            });
            barrier.wait();
            (left.join().unwrap(), right.join().unwrap())
        });

        assert_eq!(
            [left_result, right_result]
                .into_iter()
                .filter(|result| result.as_ref().is_ok_and(Option::is_some))
                .count(),
            1
        );
        let events = crate::read_jsonl_strict::<ReflectionTaskEventV1>(
            ledger.store.root(),
            &event_path(&task.task_id).unwrap(),
        )
        .unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].sequence, 1);
        assert_eq!(events[1].sequence, 2);
    }

    #[test]
    fn no_reusable_memory_writes_an_artifact_without_experience_case() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), crate::FileStoreOptions::default()).unwrap();
        let ledger = ReflectionTaskLedger::new(store);
        let task = ledger
            .create_or_read(key(), outcome_ref(), "2026-01-01T00:00:00Z")
            .unwrap();
        let artifact = HistoricalReflectionArtifactV1 {
            schema_version: HISTORICAL_REFLECTION_ARTIFACT_SCHEMA_VERSION,
            artifact_id: "artifact-one".into(),
            task_id: task.task_id,
            task_key: key(),
            disposition: ReflectionDisposition::NoReusableMemory,
            outcome_ref: outcome_ref(),
            source_refs: vec![DocumentRef {
                document_id: "summary-index".into(),
                relative_path: "runs/2026-01-01/source/index/summary/index.json".into(),
                content_hash: "sha256:summary".into(),
            }],
            summary: "No reusable pattern".into(),
            detail: "Insufficient independent evidence".into(),
            root_cause_phase: None,
            propagation_phases: Vec::new(),
            pattern_identity: None,
            rule_revision: None,
            created_at: "2026-01-01T00:00:00Z".into(),
            content_hash: String::new(),
        };
        let reference = ledger.write_artifact(artifact).unwrap();
        assert!(reference
            .relative_path
            .starts_with("knowledge/reflection/artifacts/"));
    }

    #[test]
    fn stale_outcome_task_is_superseded_before_reflection() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), crate::FileStoreOptions::default()).unwrap();
        let ledger = ReflectionTaskLedger::new(store);
        let task = ledger
            .create_or_read(key(), outcome_ref(), "2026-01-01T00:00:00Z")
            .unwrap();
        ledger
            .claim(&task.task_id, "run-a", "2026-01-02T00:00:00Z")
            .unwrap();

        let changed = ledger
            .supersede_non_current_outcomes(
                &BTreeSet::new(),
                "scheduler-run",
                "2026-01-03T00:00:00Z",
            )
            .unwrap();
        assert_eq!(changed, 1);
        assert!(ledger
            .claim(&task.task_id, "run-b", "2026-01-04T00:00:00Z")
            .unwrap()
            .is_none());
    }

    #[test]
    fn learned_terminal_uses_rust_case_result_for_duplicate_status() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), crate::FileStoreOptions::default()).unwrap();
        let ledger = ReflectionTaskLedger::new(store);
        let task = ledger
            .create_or_read(key(), outcome_ref(), "2026-01-01T00:00:00Z")
            .unwrap();
        ledger
            .claim(&task.task_id, "run-a", "2026-01-02T00:00:00Z")
            .unwrap();
        let terminal = ledger
            .complete_learned_with(
                &task.task_id,
                "run-a",
                DocumentRef {
                    document_id: "artifact".into(),
                    relative_path: "knowledge/reflection/artifacts/artifact.json".into(),
                    content_hash: "sha256:artifact".into(),
                },
                "2026-01-02T00:01:00Z",
                || Ok(crate::ExperienceCaseDisposition::DuplicateSourceRun),
            )
            .unwrap();
        assert_eq!(terminal.status, ReflectionTaskStatus::Duplicate);
    }

    #[test]
    fn reflector_failures_are_retryable_only_within_the_attempt_budget() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), crate::FileStoreOptions::default()).unwrap();
        let ledger = ReflectionTaskLedger::new(store);
        let task = ledger
            .create_or_read(key(), outcome_ref(), "2026-01-01T00:00:00Z")
            .unwrap();
        ledger
            .claim(&task.task_id, "run-a", "2026-01-02T00:00:00Z")
            .unwrap();
        let first = ledger
            .mark_failed(
                &task.task_id,
                "run-a",
                "transient terminal failure".into(),
                2,
                "2026-01-02T00:01:00Z",
            )
            .unwrap();
        assert_eq!(first.status, ReflectionTaskStatus::FailedRetryable);
        ledger
            .claim(&task.task_id, "run-b", "2026-01-03T00:00:00Z")
            .unwrap();
        let second = ledger
            .mark_failed(
                &task.task_id,
                "run-b",
                "terminal failure again".into(),
                2,
                "2026-01-03T00:01:00Z",
            )
            .unwrap();
        assert_eq!(second.status, ReflectionTaskStatus::FailedPermanent);
        assert!(ledger
            .claim(&task.task_id, "run-c", "2026-01-04T00:00:00Z")
            .unwrap()
            .is_none());
    }
}
