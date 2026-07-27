//! Crash-recoverable execution records.
//!
//! A broker submission is always recorded before the remote side effect.  A
//! restart must query the broker using that immutable client order id before it
//! considers another submission; the file store is never used as evidence that
//! a missing local receipt means no remote order exists.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    append_jsonl_locked, content_hash, read_jsonl_recover_tail, ContentHashDocument,
    FileSchemaKind, FileStore, JsonlRecord, Result, RunLocation, SafeSlug, StoreError, Versioned,
};

pub const ACCOUNT_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
pub const SUBMISSION_INTENT_SCHEMA_VERSION: u32 = 1;
pub const EXECUTION_EVENT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountSnapshot {
    pub schema_version: u32,
    pub account_id: String,
    pub payload: Value,
    pub observed_at: String,
    pub content_hash: String,
}

impl Versioned for AccountSnapshot {
    const SCHEMA_VERSION: u32 = ACCOUNT_SNAPSHOT_SCHEMA_VERSION;
}
impl ContentHashDocument for AccountSnapshot {
    fn content_hash(&self) -> &str {
        &self.content_hash
    }
    fn set_content_hash(&mut self, hash: String) {
        self.content_hash = hash;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmissionStatus {
    IntentRecorded,
    RemoteOrderRecorded,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmissionIntent {
    pub schema_version: u32,
    /// Rust-generated idempotency boundary, also supplied to the broker as
    /// its client order id when the provider supports one.
    pub submission_id: String,
    pub client_order_id: String,
    pub ticker: String,
    pub side: String,
    pub quantity: f64,
    pub order_payload: Value,
    pub status: SubmissionStatus,
    pub remote_order_id: Option<String>,
    pub failure: Option<String>,
    pub created_at: String,
    pub completed_at: Option<String>,
    pub content_hash: String,
}

impl Versioned for SubmissionIntent {
    const SCHEMA_VERSION: u32 = SUBMISSION_INTENT_SCHEMA_VERSION;
}
impl ContentHashDocument for SubmissionIntent {
    fn content_hash(&self) -> &str {
        &self.content_hash
    }
    fn set_content_hash(&mut self, hash: String) {
        self.content_hash = hash;
    }
}

impl SubmissionIntent {
    fn validate(&self) -> Result<()> {
        if self.submission_id.trim().is_empty()
            || self.client_order_id.trim().is_empty()
            || self.ticker.trim().is_empty()
            || !matches!(self.side.as_str(), "buy" | "sell")
            || !self.quantity.is_finite()
            || self.quantity <= 0.0
            || !self.order_payload.is_object()
            || self.created_at.trim().is_empty()
            || matches!(
                self.status,
                SubmissionStatus::RemoteOrderRecorded | SubmissionStatus::Completed
            ) && self.remote_order_id.as_deref().is_none_or(str::is_empty)
            || matches!(self.status, SubmissionStatus::Failed)
                && self.failure.as_deref().is_none_or(str::is_empty)
        {
            return Err(StoreError::InvalidDocument {
                kind: "submission intent",
                message: "submission identity, order, state, or timestamps are invalid".to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionEvent {
    pub schema_version: u32,
    pub sequence: u64,
    pub event_id: String,
    pub submission_id: String,
    pub remote_order_id: String,
    pub event_kind: String,
    pub payload: Value,
    pub created_at: String,
    pub content_hash: String,
}

/// Minimal Rust-owned broker boundary for a Phase 7 submission. The workflow
/// supplies an Alpaca adapter; the Store owns the crash-recovery ordering.
/// The model is never given this interface.
pub trait OrderGateway {
    /// Return the remote broker order id for a stable client order id, if it
    /// already exists. This lookup must happen before any new submission.
    fn find_by_client_order_id(&self, client_order_id: &str) -> Result<Option<String>>;
    /// Submit the exact persisted payload and return the remote broker id.
    fn submit(&self, intent: &SubmissionIntent) -> Result<String>;
}

/// Execute a submission exactly once across process crashes:
///
/// 1. atomically persist the submission intent;
/// 2. query the broker by client order id;
/// 3. only submit when the broker confirms no prior order;
/// 4. persist the remote id and completion marker.
///
/// A missing local remote receipt is therefore never evidence that it is safe
/// to create another remote order.
pub fn submit_or_recover<G: OrderGateway>(
    store: &FileStore,
    location: &RunLocation,
    intent: SubmissionIntent,
    gateway: &G,
    completed_at: String,
) -> Result<SubmissionIntent> {
    let persisted = record_submission_intent(store, location, intent)?;
    if persisted.status == SubmissionStatus::Completed {
        return Ok(persisted);
    }
    let remote_order_id = if let Some(remote) = persisted.remote_order_id.clone() {
        remote
    } else if let Some(remote) = gateway.find_by_client_order_id(&persisted.client_order_id)? {
        remote
    } else {
        gateway.submit(&persisted)?
    };
    record_remote_order(store, location, &persisted.submission_id, remote_order_id)?;
    complete_submission(store, location, &persisted.submission_id, completed_at)
}

impl JsonlRecord for ExecutionEvent {
    const SCHEMA_VERSION: u32 = EXECUTION_EVENT_SCHEMA_VERSION;
    fn schema_version(&self) -> u32 {
        self.schema_version
    }
    fn sequence(&self) -> u64 {
        self.sequence
    }
    fn validate_record(&self) -> std::result::Result<(), String> {
        if self.sequence == 0
            || self.event_id.trim().is_empty()
            || self.submission_id.trim().is_empty()
            || self.remote_order_id.trim().is_empty()
            || self.event_kind.trim().is_empty()
            || self.created_at.trim().is_empty()
        {
            return Err("execution event fields are invalid".to_owned());
        }
        crate::validate_content_hash(&serde_json::to_value(self).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())
    }
}

fn account_relative(location: &RunLocation) -> Result<std::path::PathBuf> {
    location.child_relative(std::path::Path::new("execution/account.json"))
}
fn submissions_relative(location: &RunLocation, submission_id: &str) -> Result<std::path::PathBuf> {
    let path = std::path::PathBuf::from("execution/submissions").join(format!(
        "{}.json",
        SafeSlug::new("submission", submission_id)?.as_str()
    ));
    location.child_relative(&path)
}
fn events_relative(location: &RunLocation, name: &str) -> Result<std::path::PathBuf> {
    location.child_relative(std::path::Path::new(&format!("execution/{name}.jsonl")))
}

pub fn write_account_snapshot(
    store: &FileStore,
    location: &RunLocation,
    snapshot: AccountSnapshot,
) -> Result<AccountSnapshot> {
    if snapshot.schema_version != ACCOUNT_SNAPSHOT_SCHEMA_VERSION
        || snapshot.account_id.trim().is_empty()
        || !snapshot.payload.is_object()
        || snapshot.observed_at.trim().is_empty()
    {
        return Err(StoreError::InvalidDocument {
            kind: "account snapshot",
            message: "account snapshot is invalid".to_owned(),
        });
    }
    store.write_authoritative_json(&account_relative(location)?, snapshot)
}

pub fn record_submission_intent(
    store: &FileStore,
    location: &RunLocation,
    intent: SubmissionIntent,
) -> Result<SubmissionIntent> {
    intent.validate()?;
    let path = submissions_relative(location, &intent.submission_id)?;
    if store.exists(&path)? {
        let prior: SubmissionIntent =
            store.read_versioned_json(&path, FileSchemaKind::SubmissionIntent)?;
        if prior.client_order_id != intent.client_order_id
            || prior.order_payload != intent.order_payload
        {
            return Err(StoreError::InvalidDocument {
                kind: "submission intent",
                message: "submission id was reused for a different order".to_owned(),
            });
        }
        return Ok(prior);
    }
    store.write_authoritative_json(&path, intent)
}

pub fn record_remote_order(
    store: &FileStore,
    location: &RunLocation,
    submission_id: &str,
    remote_order_id: String,
) -> Result<SubmissionIntent> {
    let path = submissions_relative(location, submission_id)?;
    let mut intent: SubmissionIntent =
        store.read_versioned_json(&path, FileSchemaKind::SubmissionIntent)?;
    intent.validate()?;
    if let Some(existing) = &intent.remote_order_id {
        if existing != &remote_order_id {
            return Err(StoreError::InvalidDocument {
                kind: "submission intent",
                message: "remote order id conflicts with the recorded submission".to_owned(),
            });
        }
        return Ok(intent);
    }
    if remote_order_id.trim().is_empty() {
        return Err(StoreError::InvalidDocument {
            kind: "submission intent",
            message: "remote order id must not be empty".to_owned(),
        });
    }
    intent.remote_order_id = Some(remote_order_id);
    intent.status = SubmissionStatus::RemoteOrderRecorded;
    store.write_authoritative_json(&path, intent)
}

pub fn complete_submission(
    store: &FileStore,
    location: &RunLocation,
    submission_id: &str,
    completed_at: String,
) -> Result<SubmissionIntent> {
    let path = submissions_relative(location, submission_id)?;
    let mut intent: SubmissionIntent =
        store.read_versioned_json(&path, FileSchemaKind::SubmissionIntent)?;
    intent.validate()?;
    if intent.status == SubmissionStatus::Completed {
        return Ok(intent);
    }
    if intent.remote_order_id.is_none() || completed_at.trim().is_empty() {
        return Err(StoreError::InvalidDocument {
            kind: "submission intent",
            message: "completed submission requires remote order id and timestamp".to_owned(),
        });
    }
    intent.status = SubmissionStatus::Completed;
    intent.completed_at = Some(completed_at);
    store.write_authoritative_json(&path, intent)
}

pub fn append_execution_event(
    store: &FileStore,
    location: &RunLocation,
    stream: &str,
    mut event: ExecutionEvent,
) -> Result<ExecutionEvent> {
    if !matches!(stream, "orders" | "fills") {
        return Err(StoreError::InvalidDocument {
            kind: "execution event",
            message: "stream must be orders or fills".to_owned(),
        });
    }
    let relative = events_relative(location, stream)?;
    let lock = relative.with_extension("jsonl.append.lock");
    store.with_exclusive_lock(&lock, || {
        let events = read_jsonl_recover_tail::<ExecutionEvent>(store.root(), &relative)?;
        event.schema_version = EXECUTION_EVENT_SCHEMA_VERSION;
        event.sequence = events.last().map_or(1, |last| last.sequence + 1);
        event.content_hash = content_hash(
            &serde_json::to_value(&event).map_err(|source| StoreError::JsonSerialize { source })?,
        )?;
        append_jsonl_locked(store.root(), &relative, &event)?;
        Ok(event)
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    fn intent() -> SubmissionIntent {
        SubmissionIntent {
            schema_version: SUBMISSION_INTENT_SCHEMA_VERSION,
            submission_id: "submit-1".to_owned(),
            client_order_id: "client-1".to_owned(),
            ticker: "QQQ".to_owned(),
            side: "buy".to_owned(),
            quantity: 1.0,
            order_payload: json!({"type":"market"}),
            status: SubmissionStatus::IntentRecorded,
            remote_order_id: None,
            failure: None,
            created_at: "2026-07-27T00:00:00Z".to_owned(),
            completed_at: None,
            content_hash: String::new(),
        }
    }

    #[test]
    fn submission_intent_precedes_remote_receipt_and_is_idempotent() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), Default::default()).unwrap();
        let location = RunLocation::new("2026-07-27", "run-1").unwrap();
        let initial = record_submission_intent(&store, &location, intent()).unwrap();
        assert_eq!(initial.status, SubmissionStatus::IntentRecorded);
        assert_eq!(
            record_submission_intent(&store, &location, intent()).unwrap(),
            initial
        );
        let remote =
            record_remote_order(&store, &location, "submit-1", "alpaca-1".to_owned()).unwrap();
        assert_eq!(remote.remote_order_id.as_deref(), Some("alpaca-1"));
        let completed = complete_submission(
            &store,
            &location,
            "submit-1",
            "2026-07-27T00:01:00Z".to_owned(),
        )
        .unwrap();
        assert_eq!(completed.status, SubmissionStatus::Completed);
    }

    #[derive(Default)]
    struct FakeGateway {
        existing: Option<String>,
        submits: std::cell::Cell<u32>,
    }

    impl OrderGateway for FakeGateway {
        fn find_by_client_order_id(&self, _client_order_id: &str) -> Result<Option<String>> {
            Ok(self.existing.clone())
        }

        fn submit(&self, _intent: &SubmissionIntent) -> Result<String> {
            self.submits.set(self.submits.get() + 1);
            Ok("submitted-remote-id".to_owned())
        }
    }

    #[test]
    fn recovery_queries_remote_before_submitting_again() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), Default::default()).unwrap();
        let location = RunLocation::new("2026-07-27", "run-1").unwrap();
        let gateway = FakeGateway {
            existing: Some("remote-before-restart".to_owned()),
            ..Default::default()
        };
        let completed = submit_or_recover(
            &store,
            &location,
            intent(),
            &gateway,
            "2026-07-27T00:01:00Z".to_owned(),
        )
        .unwrap();
        assert_eq!(
            completed.remote_order_id.as_deref(),
            Some("remote-before-restart")
        );
        assert_eq!(gateway.submits.get(), 0);
        assert_eq!(completed.status, SubmissionStatus::Completed);
    }

    #[test]
    fn execution_event_assigns_sequence_and_hash_under_lock() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), Default::default()).unwrap();
        let location = RunLocation::new("2026-07-27", "run-1").unwrap();
        let event = |id: &str| ExecutionEvent {
            schema_version: 0,
            sequence: 0,
            event_id: id.to_owned(),
            submission_id: "submit-1".to_owned(),
            remote_order_id: "alpaca-1".to_owned(),
            event_kind: "accepted".to_owned(),
            payload: json!({}),
            created_at: "2026-07-27T00:00:00Z".to_owned(),
            content_hash: String::new(),
        };
        assert_eq!(
            append_execution_event(&store, &location, "orders", event("one"))
                .unwrap()
                .sequence,
            1
        );
        assert_eq!(
            append_execution_event(&store, &location, "orders", event("two"))
                .unwrap()
                .sequence,
            2
        );
    }
}
