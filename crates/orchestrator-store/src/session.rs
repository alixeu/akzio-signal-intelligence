use std::{collections::BTreeMap, fs, path::PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    canonical_json_bytes, content_hash, error::io_error, read_jsonl_recover_tail,
    ContentHashDocument, FileSchemaKind, FileStore, JsonlRecord, Result, RunLocation, SafeSlug,
    StoreError, Versioned,
};

pub const SESSION_MANIFEST_SCHEMA_VERSION: u32 = 1;
pub const SESSION_EVENT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkReference {
    pub fork_from_session_id: String,
    pub fork_from_turn_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Active,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionManifest {
    pub schema_version: u32,
    pub session_id: String,
    pub run_id: String,
    pub role: String,
    pub phase: u8,
    pub profile: String,
    pub fork: Option<ForkReference>,
    pub status: SessionStatus,
    pub created_at: String,
    pub content_hash: String,
}

impl SessionManifest {
    pub fn new(
        location: &SessionLocation,
        role: impl Into<String>,
        phase: u8,
        profile: impl Into<String>,
        fork: Option<ForkReference>,
        created_at: impl Into<String>,
    ) -> Result<Self> {
        let manifest = Self {
            schema_version: SESSION_MANIFEST_SCHEMA_VERSION,
            session_id: location.session_id.clone(),
            run_id: location.run.run_id.clone(),
            role: role.into(),
            phase,
            profile: profile.into(),
            fork,
            status: SessionStatus::Active,
            created_at: created_at.into(),
            content_hash: String::new(),
        };
        manifest.validate_for_location(location)?;
        Ok(manifest)
    }

    pub fn validate_for_location(&self, location: &SessionLocation) -> Result<()> {
        if self.schema_version != SESSION_MANIFEST_SCHEMA_VERSION
            || self.session_id.is_empty()
            || self.run_id != location.run.run_id
            || self.session_id != location.session_id
            || self.role.is_empty()
            || self.profile.is_empty()
            || self.created_at.is_empty()
        {
            return Err(StoreError::InvalidDocument {
                kind: "session manifest",
                message: "session manifest fields do not match its store location".to_owned(),
            });
        }
        if let Some(fork) = &self.fork {
            if fork.fork_from_session_id.is_empty() || fork.fork_from_turn_id.is_empty() {
                return Err(StoreError::InvalidDocument {
                    kind: "session manifest",
                    message: "fork session and turn IDs must be present together".to_owned(),
                });
            }
        }
        Ok(())
    }
}

impl Versioned for SessionManifest {
    const SCHEMA_VERSION: u32 = SESSION_MANIFEST_SCHEMA_VERSION;
}

impl ContentHashDocument for SessionManifest {
    fn content_hash(&self) -> &str {
        &self.content_hash
    }

    fn set_content_hash(&mut self, hash: String) {
        self.content_hash = hash;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionLocation {
    pub run: RunLocation,
    pub session_id: String,
    session_slug: SafeSlug,
}

impl SessionLocation {
    pub fn new(run: RunLocation, session_id: impl Into<String>) -> Result<Self> {
        let session_id = session_id.into();
        if session_id.trim().is_empty() {
            return Err(StoreError::InvalidDocument {
                kind: "session location",
                message: "session_id must not be empty".to_owned(),
            });
        }
        Ok(Self {
            session_slug: SafeSlug::new("session", &session_id)?,
            run,
            session_id,
        })
    }

    pub fn relative_dir(&self) -> PathBuf {
        self.run
            .relative_root()
            .join("sessions")
            .join(self.session_slug.as_str())
    }

    pub fn manifest_relative(&self) -> PathBuf {
        self.relative_dir().join("manifest.json")
    }

    pub fn turn_events_relative(&self, turn_id: &str) -> Result<PathBuf> {
        if turn_id.trim().is_empty() {
            return Err(StoreError::InvalidDocument {
                kind: "session turn",
                message: "turn_id must not be empty".to_owned(),
            });
        }
        Ok(self
            .relative_dir()
            .join(format!("{}.jsonl", SafeSlug::new("turn", turn_id)?)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionEventType {
    User,
    Assistant,
    ToolCall,
    ToolResult,
    EvidenceRead,
    Checkpoint,
    Terminal,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionEvent {
    pub schema_version: u32,
    pub sequence: u64,
    pub event_type: SessionEventType,
    pub session_id: String,
    pub turn_id: String,
    pub role: String,
    pub phase: u8,
    pub fork: Option<ForkReference>,
    pub payload: Value,
    pub created_at: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionEventInput {
    pub event_type: SessionEventType,
    pub turn_id: String,
    pub payload: Value,
    pub created_at: String,
}

impl SessionEvent {
    pub fn new(sequence: u64, session: &SessionManifest, input: SessionEventInput) -> Result<Self> {
        let mut event = Self {
            schema_version: SESSION_EVENT_SCHEMA_VERSION,
            sequence,
            event_type: input.event_type,
            session_id: session.session_id.clone(),
            turn_id: input.turn_id,
            role: session.role.clone(),
            phase: session.phase,
            fork: session.fork.clone(),
            payload: input.payload,
            created_at: input.created_at,
            content_hash: String::new(),
        };
        event.validate_shape()?;
        event.content_hash = event_hash(&event)?;
        Ok(event)
    }

    fn validate_shape(&self) -> Result<()> {
        if self.sequence == 0
            || self.session_id.is_empty()
            || self.turn_id.is_empty()
            || self.role.is_empty()
            || self.created_at.is_empty()
        {
            return Err(StoreError::InvalidDocument {
                kind: "session event",
                message: "session event contains an empty required field or zero sequence"
                    .to_owned(),
            });
        }
        Ok(())
    }
}

impl JsonlRecord for SessionEvent {
    const SCHEMA_VERSION: u32 = SESSION_EVENT_SCHEMA_VERSION;

    fn schema_version(&self) -> u32 {
        self.schema_version
    }

    fn sequence(&self) -> u64 {
        self.sequence
    }

    fn validate_record(&self) -> std::result::Result<(), String> {
        self.validate_shape().map_err(|error| error.to_string())?;
        let expected = event_hash(self).map_err(|error| error.to_string())?;
        if self.content_hash != expected {
            return Err(format!(
                "expected content hash {expected}, found {}",
                self.content_hash
            ));
        }
        Ok(())
    }
}

/// The self-describing hash field is excluded from its own canonical digest.
/// Readers use the same projection, so a JSONL event written before a crash is
/// verifiable without silently weakening event integrity.
fn event_hash(event: &SessionEvent) -> Result<String> {
    let mut value =
        serde_json::to_value(event).map_err(|source| StoreError::JsonSerialize { source })?;
    value["content_hash"] = Value::String(String::new());
    // JSON numbers originating in tool output can be represented as native
    // f64 values. Hash the canonical bytes after one JSON parse round-trip so
    // write-time and recovery-time representations are identical.
    let bytes = canonical_json_bytes(&value)?;
    let normalized =
        serde_json::from_slice(&bytes).map_err(|source| StoreError::JsonSerialize { source })?;
    content_hash(&normalized)
}

/// The only typed source allowed to add an item to VisibleEvidenceSet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceReadEvent {
    pub tool_name: String,
    pub subject_kind: String,
    pub subject_id: String,
    pub source_run_id: String,
    pub source_phase: u8,
    pub ticker: Option<String>,
    pub topic_id: Option<String>,
    pub turn_id: String,
    pub session_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VisibleEvidenceSet {
    entries: BTreeMap<String, EvidenceReadEvent>,
}

impl VisibleEvidenceSet {
    pub fn from_events(events: impl IntoIterator<Item = SessionEvent>) -> Result<Self> {
        Self::from_parent_and_events(Self::default(), events)
    }

    pub fn from_parent_and_events(
        mut parent: Self,
        events: impl IntoIterator<Item = SessionEvent>,
    ) -> Result<Self> {
        for event in events {
            event
                .validate_record()
                .map_err(|message| StoreError::JsonlHash {
                    path: PathBuf::from("<session event>"),
                    message,
                })?;
            if event.event_type != SessionEventType::EvidenceRead {
                continue;
            }
            let evidence: EvidenceReadEvent = serde_json::from_value(event.payload.clone())
                .map_err(|source| StoreError::Json {
                    path: PathBuf::from("<session evidence_read payload>"),
                    source,
                })?;
            if evidence.session_id != event.session_id || evidence.turn_id != event.turn_id {
                return Err(StoreError::InvalidDocument {
                    kind: "evidence_read event",
                    message: "payload session/turn does not match event envelope".to_owned(),
                });
            }
            if evidence.tool_name.is_empty()
                || evidence.subject_kind.is_empty()
                || evidence.subject_id.is_empty()
                || evidence.source_run_id.is_empty()
            {
                return Err(StoreError::InvalidDocument {
                    kind: "evidence_read event",
                    message: "payload contains an empty required evidence field".to_owned(),
                });
            }
            match parent.entries.get(&evidence.subject_id) {
                Some(existing) if !same_evidence_provenance(existing, &evidence) => {
                    return Err(StoreError::InvalidDocument {
                        kind: "visible evidence set",
                        message: format!(
                            "evidence ID `{}` has conflicting provenance",
                            evidence.subject_id
                        ),
                    });
                }
                Some(_) => {}
                None => {
                    parent.entries.insert(evidence.subject_id.clone(), evidence);
                }
            }
        }
        Ok(parent)
    }

    pub fn contains(&self, subject_id: &str) -> bool {
        self.entries.contains_key(subject_id)
    }

    pub fn get(&self, subject_id: &str) -> Option<&EvidenceReadEvent> {
        self.entries.get(subject_id)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Rust services may project the IDs that were actually read into a typed
    /// finalizer. Callers never receive mutable access to the provenance map.
    pub fn ids(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(String::as_str)
    }
}

fn same_evidence_provenance(left: &EvidenceReadEvent, right: &EvidenceReadEvent) -> bool {
    // A snapshot reader and a detail reader may legitimately expose the same
    // immutable source object.  The subject identity and its source scope are
    // authoritative; the reader name is audit metadata, not provenance.
    left.subject_kind == right.subject_kind
        && left.subject_id == right.subject_id
        && left.source_run_id == right.source_run_id
        && left.source_phase == right.source_phase
        && left.ticker == right.ticker
        && left.topic_id == right.topic_id
}

pub fn write_session_manifest(
    store: &FileStore,
    location: &SessionLocation,
    manifest: SessionManifest,
) -> Result<SessionManifest> {
    manifest.validate_for_location(location)?;
    store.write_authoritative_json(&location.manifest_relative(), manifest)
}

pub fn read_session_manifest(
    store: &FileStore,
    location: &SessionLocation,
) -> Result<SessionManifest> {
    let manifest = store.read_versioned_json::<SessionManifest>(
        &location.manifest_relative(),
        FileSchemaKind::SessionManifest,
    )?;
    manifest.validate_for_location(location)?;
    Ok(manifest)
}

pub fn append_session_event(
    store: &FileStore,
    location: &SessionLocation,
    session: &SessionManifest,
    input: SessionEventInput,
) -> Result<SessionEvent> {
    session.validate_for_location(location)?;
    let events_relative = location.turn_events_relative(&input.turn_id)?;
    let lock_relative = events_relative.with_extension("jsonl.append.lock");
    store.with_exclusive_lock(&lock_relative, || {
        let previous = read_jsonl_recover_tail::<SessionEvent>(store.root(), &events_relative)?;
        let sequence = previous.last().map_or(1, |event| event.sequence + 1);
        let event = SessionEvent::new(sequence, session, input)?;
        store.append_jsonl_locked(&events_relative, &event)?;
        Ok(event)
    })
}

pub fn read_session_events(
    store: &FileStore,
    location: &SessionLocation,
) -> Result<Vec<SessionEvent>> {
    let manifest = read_session_manifest(store, location)?;
    let absolute = store.root().join(location.relative_dir());
    if !absolute.exists() {
        return Ok(Vec::new());
    }
    let mut relative_logs = Vec::new();
    for entry in fs::read_dir(&absolute).map_err(|source| io_error(&absolute, source))? {
        let entry = entry.map_err(|source| io_error(&absolute, source))?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(|source| io_error(&path, source))?;
        if metadata.file_type().is_symlink() {
            return Err(StoreError::SymlinkPath { path });
        }
        if metadata.is_file()
            && path
                .extension()
                .is_some_and(|extension| extension == "jsonl")
        {
            let name = entry.file_name();
            relative_logs.push(location.relative_dir().join(name));
        }
    }
    relative_logs.sort();
    let mut events = Vec::new();
    for relative in relative_logs {
        for event in read_jsonl_recover_tail::<SessionEvent>(store.root(), &relative)? {
            validate_event_for_session(&event, &manifest)?;
            events.push(event);
        }
    }
    events.sort_by(|left, right| {
        (&left.created_at, &left.turn_id, left.sequence).cmp(&(
            &right.created_at,
            &right.turn_id,
            right.sequence,
        ))
    });
    Ok(events)
}

fn validate_event_for_session(event: &SessionEvent, session: &SessionManifest) -> Result<()> {
    if event.session_id != session.session_id
        || event.role != session.role
        || event.phase != session.phase
        || event.fork != session.fork
    {
        return Err(StoreError::InvalidDocument {
            kind: "session event",
            message: "event envelope differs from session manifest or fork boundary".to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;

    use super::{
        append_session_event, read_session_events, write_session_manifest, EvidenceReadEvent,
        ForkReference, SessionEvent, SessionEventInput, SessionEventType, SessionLocation,
        SessionManifest, VisibleEvidenceSet,
    };
    use crate::{FileStore, FileStoreOptions, RunLocation};

    fn location() -> SessionLocation {
        SessionLocation::new(
            RunLocation::new("2026-07-27", "run-one").unwrap(),
            "session / child",
        )
        .unwrap()
    }

    #[test]
    fn session_persists_forked_events_and_derived_evidence_only() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = location();
        let session = write_session_manifest(
            &store,
            &location,
            SessionManifest::new(
                &location,
                "researcher.bull",
                2,
                "debate_seed",
                Some(ForkReference {
                    fork_from_session_id: "warmup".to_owned(),
                    fork_from_turn_id: "turn-1".to_owned(),
                }),
                "2026-07-27T00:00:00Z",
            )
            .unwrap(),
        )
        .unwrap();

        append_session_event(
            &store,
            &location,
            &session,
            SessionEventInput {
                event_type: SessionEventType::Assistant,
                turn_id: "turn 1".to_owned(),
                payload: json!({"free_text": "pretend evidence_id is evidence-ignored"}),
                created_at: "2026-07-27T00:00:01Z".to_owned(),
            },
        )
        .unwrap();
        let evidence = EvidenceReadEvent {
            tool_name: "read_technical_snapshot".to_owned(),
            subject_kind: "technical_snapshot".to_owned(),
            subject_id: "evidence-1".to_owned(),
            source_run_id: "run-one".to_owned(),
            source_phase: 1,
            ticker: Some("QQQ".to_owned()),
            topic_id: None,
            turn_id: "turn 1".to_owned(),
            session_id: location.session_id.clone(),
        };
        append_session_event(
            &store,
            &location,
            &session,
            SessionEventInput {
                event_type: SessionEventType::EvidenceRead,
                turn_id: "turn 1".to_owned(),
                payload: serde_json::to_value(evidence).unwrap(),
                created_at: "2026-07-27T00:00:02Z".to_owned(),
            },
        )
        .unwrap();

        let events = read_session_events(&store, &location).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].fork.as_ref().unwrap().fork_from_session_id,
            "warmup"
        );
        let visible = VisibleEvidenceSet::from_events(events).unwrap();
        assert!(visible.contains("evidence-1"));
        assert!(!visible.contains("evidence-ignored"));
    }

    #[test]
    fn evidence_payload_must_match_session_envelope() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = location();
        let session = write_session_manifest(
            &store,
            &location,
            SessionManifest::new(
                &location,
                "analyst.technical",
                1,
                "analyst_report",
                None,
                "2026-07-27T00:00:00Z",
            )
            .unwrap(),
        )
        .unwrap();
        append_session_event(
            &store,
            &location,
            &session,
            SessionEventInput {
                event_type: SessionEventType::EvidenceRead,
                turn_id: "turn-1".to_owned(),
                payload: json!({
                    "tool_name": "read",
                    "subject_kind": "snapshot",
                    "subject_id": "e1",
                    "source_run_id": "run-one",
                    "source_phase": 1,
                    "ticker": "QQQ",
                    "topic_id": null,
                    "turn_id": "another-turn",
                    "session_id": location.session_id,
                }),
                created_at: "2026-07-27T00:00:01Z".to_owned(),
            },
        )
        .unwrap();
        assert!(
            VisibleEvidenceSet::from_events(read_session_events(&store, &location).unwrap())
                .is_err()
        );
    }

    #[test]
    fn same_subject_from_snapshot_and_detail_is_one_visible_evidence_item() {
        let location = location();
        let session = SessionManifest::new(
            &location,
            "analyst.technical",
            1,
            "analyst_report",
            None,
            "2026-07-27T00:00:00Z",
        )
        .unwrap();
        let event = |sequence: u64, tool_name: &str| {
            SessionEvent::new(
                sequence,
                &session,
                SessionEventInput {
                    event_type: SessionEventType::EvidenceRead,
                    turn_id: "turn-1".to_owned(),
                    payload: json!(EvidenceReadEvent {
                        tool_name: tool_name.to_owned(),
                        subject_kind: "technical_signal".to_owned(),
                        subject_id: "QQQ:daily:structure:2026-07-27".to_owned(),
                        source_run_id: "run-one".to_owned(),
                        source_phase: 1,
                        ticker: Some("QQQ".to_owned()),
                        topic_id: None,
                        turn_id: "turn-1".to_owned(),
                        session_id: location.session_id.clone(),
                    }),
                    created_at: "2026-07-27T00:00:00Z".to_owned(),
                },
            )
            .unwrap()
        };
        let snapshot = event(1, "read_technical_snapshot");
        let detail = event(2, "read_technical_detail");
        let visible = VisibleEvidenceSet::from_events([snapshot, detail]).unwrap();
        assert!(visible.contains("QQQ:daily:structure:2026-07-27"));
        assert_eq!(visible.len(), 1);
    }
}
