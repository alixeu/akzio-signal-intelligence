//! Concrete FileStore session persistence for agent-loop history.
//!
//! This module intentionally has no storage backend trait.  A migrated agent
//! receives this FileStore binding or it has no persistent session authority.

use anyhow::{bail, Context, Result};
use orchestrator_store::{
    append_session_event, read_session_events, read_session_manifest, write_session_manifest,
    FileStore, ForkReference, RunLocation, SessionEvent, SessionEventInput, SessionEventType,
    SessionLocation, SessionManifest,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{
    ToolExecutionRecord, ToolExecutionStatus, ToolResultItem, Turn, TurnItem, TurnItemType,
};

/// Rust-owned parameters used to create or recover a session manifest.
#[derive(Debug, Clone)]
pub struct SessionRuntimeSpec {
    pub run: RunLocation,
    pub session_id: String,
    pub role: String,
    pub phase: u8,
    pub profile: String,
    pub fork: Option<ForkReference>,
    pub created_at: String,
}

impl SessionRuntimeSpec {
    fn validate(&self) -> Result<()> {
        if self.session_id.trim().is_empty()
            || self.role.trim().is_empty()
            || self.profile.trim().is_empty()
            || self.created_at.trim().is_empty()
        {
            bail!("FileStore session runtime requires non-empty session, role, profile, and timestamp")
        }
        Ok(())
    }
}

/// Checkpoint payload is fixed by Rust; it is never a model-controlled JSON
/// update surface.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TurnCheckpoint {
    pub item_count: usize,
    pub end_reason: Option<String>,
    pub needs_follow_up: bool,
}

/// Concrete persistent session binding used by FileStore-authoritative roles.
#[derive(Debug, Clone)]
pub struct FileStoreSessionRuntime {
    store: FileStore,
    location: SessionLocation,
    manifest: SessionManifest,
}

impl FileStoreSessionRuntime {
    /// Create a new manifest or recover the same validated manifest after a
    /// process restart. A reused session ID may never change its authority.
    pub fn create_or_load(store: FileStore, spec: SessionRuntimeSpec) -> Result<Self> {
        spec.validate()?;
        let location = SessionLocation::new(spec.run, spec.session_id)?;
        let manifest = if store.exists(&location.manifest_relative())? {
            let manifest = read_session_manifest(&store, &location)?;
            if manifest.role != spec.role
                || manifest.phase != spec.phase
                || manifest.profile != spec.profile
                || manifest.fork != spec.fork
            {
                bail!("existing FileStore session manifest does not match its Rust-owned scope")
            }
            manifest
        } else {
            let manifest = SessionManifest::new(
                &location,
                spec.role,
                spec.phase,
                spec.profile,
                spec.fork,
                spec.created_at,
            )?;
            write_session_manifest(&store, &location, manifest)?
        };
        Ok(Self {
            store,
            location,
            manifest,
        })
    }

    /// Load an existing, content-hash-validated session only.
    pub fn load(store: FileStore, location: SessionLocation) -> Result<Self> {
        let manifest = read_session_manifest(&store, &location)?;
        Ok(Self {
            store,
            location,
            manifest,
        })
    }

    pub fn location(&self) -> &SessionLocation {
        &self.location
    }

    pub fn manifest(&self) -> &SessionManifest {
        &self.manifest
    }

    /// Append an item with its Rust-owned ordinal. The ordinal makes recovery
    /// idempotent without treating a human/model field as an event identity.
    pub fn append_turn_item_at(
        &self,
        turn: &Turn,
        item: &TurnItem,
        item_index: Option<usize>,
        created_at: impl Into<String>,
    ) -> Result<SessionEvent> {
        self.validate_turn(turn)?;
        let event_type = match item.item_type {
            TurnItemType::UserMessage => SessionEventType::User,
            TurnItemType::AssistantMessage
            | TurnItemType::ReasoningSummary
            | TurnItemType::ReasoningState
            | TurnItemType::PlanUpdate => SessionEventType::Assistant,
            TurnItemType::ToolCall => SessionEventType::ToolCall,
            TurnItemType::ToolResult | TurnItemType::NativeWebSearch => {
                SessionEventType::ToolResult
            }
            TurnItemType::SystemContext
            | TurnItemType::DeveloperContext
            | TurnItemType::CompactSummary
            | TurnItemType::InjectedContext => SessionEventType::Checkpoint,
        };
        self.append(
            event_type,
            &turn.turn_id,
            json!({
                "item_type": item.item_type.as_str(),
                "role": item.role,
                "content_text": item.content_text,
                "content_json": item.content_json,
                "tool_call_id": item.tool_call_id,
                "tool_name": item.tool_name,
                "output_item_id": item.output_item_id,
                "phase": item.phase.as_ref().map(|phase| phase.as_str()),
                "status": item.status.as_ref().map(|status| status.as_str()),
                "item_index": item_index,
            }),
            created_at,
        )
    }

    pub fn append_checkpoint(
        &self,
        turn: &Turn,
        checkpoint: TurnCheckpoint,
        created_at: impl Into<String>,
    ) -> Result<SessionEvent> {
        self.validate_turn(turn)?;
        self.append(
            SessionEventType::Checkpoint,
            &turn.turn_id,
            serde_json::to_value(checkpoint).context("serialize session checkpoint")?,
            created_at,
        )
    }

    pub fn append_terminal(
        &self,
        turn: &Turn,
        terminal: &ToolResultItem,
        created_at: impl Into<String>,
    ) -> Result<SessionEvent> {
        self.validate_turn(turn)?;
        if terminal.status != "completed" {
            bail!("only a completed terminal tool result can end a FileStore session turn")
        }
        self.append(
            SessionEventType::Terminal,
            &turn.turn_id,
            serde_json::to_value(terminal).context("serialize terminal tool result")?,
            created_at,
        )
    }

    /// Persist one Rust-owned tool execution transition.  The existing
    /// SessionEventType variants are intentionally reused: a pending/running
    /// transition is a ToolCall, a completed result is a ToolResult, and an
    /// interruption is an Error.  The `kind` discriminator keeps these
    /// records separate from ordinary TurnItem payloads.
    pub fn append_tool_execution(
        &self,
        turn: &Turn,
        record: &ToolExecutionRecord,
        created_at: impl Into<String>,
    ) -> Result<SessionEvent> {
        self.validate_turn(turn)?;
        if record.key.session_id != turn.session_id || record.key.turn_id != turn.turn_id {
            bail!("tool execution key does not match its FileStore turn")
        }
        self.append_tool_execution_record(record, created_at)
    }

    /// Append a tool execution transition from a cancellation guard.  This
    /// variant validates against the session manifest and key only, so a
    /// cancelled Future can record Interrupted without holding a Turn borrow.
    pub fn append_tool_execution_record(
        &self,
        record: &ToolExecutionRecord,
        created_at: impl Into<String>,
    ) -> Result<SessionEvent> {
        if record.key.session_id != self.manifest.session_id
            || record.key.turn_id.trim().is_empty()
            || record.tool_name.trim().is_empty()
        {
            bail!("tool execution record is outside its FileStore session authority")
        }
        if record.status == ToolExecutionStatus::Completed {
            let result = record
                .result
                .as_ref()
                .context("completed tool execution is missing its result")?;
            if result.call_id != record.key.call_id || result.name != record.tool_name {
                bail!("completed tool execution result does not match its key")
            }
        } else if record.result.is_some() {
            bail!("non-completed tool execution cannot contain a result")
        }
        let event_type = match record.status {
            ToolExecutionStatus::Pending | ToolExecutionStatus::Running => {
                SessionEventType::ToolCall
            }
            ToolExecutionStatus::Completed => SessionEventType::ToolResult,
            ToolExecutionStatus::Interrupted => SessionEventType::Error,
        };
        self.append(
            event_type,
            &record.key.turn_id,
            json!({
                "kind": "tool_execution",
                "record": record,
            }),
            created_at,
        )
    }

    /// Persist an interrupted turn before its Future is dropped by an outer
    /// timeout or cancellation. Recovery can then distinguish an unfinished
    /// turn from a cleanly persisted terminal turn without guessing.
    pub fn append_turn_interruption(
        &self,
        turn_id: &str,
        reason: impl Into<String>,
        created_at: impl Into<String>,
    ) -> Result<SessionEvent> {
        if turn_id.trim().is_empty() {
            bail!("turn interruption requires a non-empty turn id")
        }
        self.read_current_turn(turn_id)?;
        self.append(
            SessionEventType::Error,
            turn_id,
            json!({
                "kind": "turn_execution",
                "state": "interrupted",
                "reason": reason.into(),
            }),
            created_at,
        )
    }

    /// Return the latest persisted state for one execution key.
    pub fn read_tool_execution(
        &self,
        turn_id: &str,
        call_id: &str,
    ) -> Result<Option<ToolExecutionRecord>> {
        let mut latest = None;
        for event in self.read_current_turn(turn_id)? {
            if event.payload.get("kind").and_then(Value::as_str) != Some("tool_execution") {
                continue;
            }
            let record: ToolExecutionRecord = serde_json::from_value(
                event
                    .payload
                    .get("record")
                    .cloned()
                    .context("tool execution event is missing its record")?,
            )
            .context("decode persisted tool execution record")?;
            if record.key.turn_id != turn_id || record.key.call_id != call_id {
                continue;
            }
            if record.key.session_id != self.manifest.session_id {
                bail!("persisted tool execution record violates FileStore authority")
            }
            latest = Some(record);
        }
        Ok(latest)
    }

    /// Return the latest state for every tool call in a turn.  Any malformed
    /// matching record is an error rather than a reason to execute blindly.
    pub fn read_tool_executions(&self, turn_id: &str) -> Result<Vec<ToolExecutionRecord>> {
        let events = self.read_current_turn(turn_id)?;
        let mut latest = std::collections::BTreeMap::<String, ToolExecutionRecord>::new();
        for event in events {
            if event.payload.get("kind").and_then(Value::as_str) != Some("tool_execution") {
                continue;
            }
            let record: ToolExecutionRecord = serde_json::from_value(
                event
                    .payload
                    .get("record")
                    .cloned()
                    .context("tool execution event is missing its record")?,
            )
            .context("decode persisted tool execution record")?;
            if record.key.session_id != self.manifest.session_id
                || record.key.turn_id != turn_id
                || record.key.call_id.trim().is_empty()
            {
                bail!("persisted tool execution record violates FileStore authority")
            }
            latest.insert(record.key.call_id.clone(), record);
        }
        Ok(latest.into_values().collect())
    }

    /// Events written by this session only, narrowed to one turn.
    pub fn read_current_turn(&self, turn_id: &str) -> Result<Vec<SessionEvent>> {
        self.read_turn_from(&self.location, turn_id)
    }

    /// Read a parent's immutable fork turn. The parent is resolved solely from
    /// the manifest fork reference; callers cannot select arbitrary sessions.
    pub fn read_fork_turn(&self) -> Result<Vec<SessionEvent>> {
        let fork = self
            .manifest
            .fork
            .as_ref()
            .context("session has no fork reference")?;
        let parent = SessionLocation::new(self.location.run.clone(), &fork.fork_from_session_id)?;
        self.read_turn_from(&parent, &fork.fork_from_turn_id)
    }

    fn append(
        &self,
        event_type: SessionEventType,
        turn_id: &str,
        payload: Value,
        created_at: impl Into<String>,
    ) -> Result<SessionEvent> {
        append_session_event(
            &self.store,
            &self.location,
            &self.manifest,
            SessionEventInput {
                event_type,
                turn_id: turn_id.to_owned(),
                payload,
                created_at: created_at.into(),
            },
        )
        .map_err(Into::into)
    }

    fn read_turn_from(
        &self,
        location: &SessionLocation,
        turn_id: &str,
    ) -> Result<Vec<SessionEvent>> {
        if turn_id.trim().is_empty() {
            bail!("session turn id must not be empty")
        }
        Ok(read_session_events(&self.store, location)?
            .into_iter()
            .filter(|event| event.turn_id == turn_id)
            .collect())
    }

    fn validate_turn(&self, turn: &Turn) -> Result<()> {
        if turn.run_id != self.manifest.run_id
            || turn.session_id != self.manifest.session_id
            || turn.role != self.manifest.role
            || turn.phase != Some(i64::from(self.manifest.phase))
        {
            bail!("turn does not match FileStore session authority")
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_loop::{ToolExecutionKey, TurnItem};
    use orchestrator_store::{FileStoreOptions, SessionStatus};
    use tempfile::TempDir;

    fn runtime(temp: &TempDir, fork: Option<ForkReference>) -> FileStoreSessionRuntime {
        FileStoreSessionRuntime::create_or_load(
            FileStore::open(temp.path(), FileStoreOptions::default()).unwrap(),
            SessionRuntimeSpec {
                run: RunLocation::new("2026-07-27", "run-a").unwrap(),
                session_id: "session-a".to_owned(),
                role: "analyst.technical".to_owned(),
                phase: 1,
                profile: "analyst_report".to_owned(),
                fork,
                created_at: "2026-07-27T00:00:00Z".to_owned(),
            },
        )
        .unwrap()
    }

    fn turn() -> Turn {
        let mut turn = Turn::new(
            "turn-a",
            "session-a",
            "run-a",
            "analyst.technical",
            "prompt",
        );
        turn.phase = Some(1);
        turn
    }

    #[test]
    fn creates_recovers_and_appends_typed_items() {
        let temp = TempDir::new().unwrap();
        let runtime = runtime(&temp, None);
        assert_eq!(runtime.manifest().status, SessionStatus::Active);
        let turn = turn();
        runtime
            .append_turn_item_at(
                &turn,
                &TurnItem::user("prompt"),
                None,
                "2026-07-27T00:00:01Z",
            )
            .unwrap();
        runtime
            .append_checkpoint(
                &turn,
                TurnCheckpoint {
                    item_count: 1,
                    end_reason: None,
                    needs_follow_up: true,
                },
                "2026-07-27T00:00:02Z",
            )
            .unwrap();

        let recovered = FileStoreSessionRuntime::load(
            FileStore::open(temp.path(), FileStoreOptions::default()).unwrap(),
            runtime.location().clone(),
        )
        .unwrap();
        let events = recovered.read_current_turn("turn-a").unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, SessionEventType::User);
        assert_eq!(events[1].event_type, SessionEventType::Checkpoint);
    }

    #[test]
    fn tool_execution_state_is_durable_and_latest_state_wins() {
        let temp = TempDir::new().unwrap();
        let runtime = runtime(&temp, None);
        let turn = turn();
        let key = ToolExecutionKey::new("session-a", "turn-a", "call-a").unwrap();
        runtime
            .append_tool_execution(
                &turn,
                &ToolExecutionRecord::pending(key.clone(), "read"),
                "2026-07-27T00:00:01Z",
            )
            .unwrap();
        runtime
            .append_tool_execution(
                &turn,
                &ToolExecutionRecord::running(key.clone(), "read"),
                "2026-07-27T00:00:02Z",
            )
            .unwrap();
        let result = ToolResultItem {
            call_id: "call-a".to_owned(),
            name: "read".to_owned(),
            status: "completed".to_owned(),
            output: json!({"ok": true}),
            error: None,
        };
        runtime
            .append_tool_execution(
                &turn,
                &ToolExecutionRecord::completed(result.clone(), key),
                "2026-07-27T00:00:03Z",
            )
            .unwrap();

        let recovered = FileStoreSessionRuntime::load(
            FileStore::open(temp.path(), FileStoreOptions::default()).unwrap(),
            runtime.location().clone(),
        )
        .unwrap();
        let record = recovered
            .read_tool_execution("turn-a", "call-a")
            .unwrap()
            .unwrap();
        assert_eq!(record.status, ToolExecutionStatus::Completed);
        assert_eq!(record.result, Some(result));
    }

    #[test]
    fn interrupted_tool_execution_is_preserved_for_fail_closed_recovery() {
        let temp = TempDir::new().unwrap();
        let runtime = runtime(&temp, None);
        let turn = turn();
        let key = ToolExecutionKey::new("session-a", "turn-a", "call-a").unwrap();
        runtime
            .append_tool_execution(
                &turn,
                &ToolExecutionRecord::interrupted(key, "write", "cancelled"),
                "2026-07-27T00:00:01Z",
            )
            .unwrap();
        let records = runtime.read_tool_executions("turn-a").unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, ToolExecutionStatus::Interrupted);
        assert_eq!(records[0].reason.as_deref(), Some("cancelled"));
    }

    #[test]
    fn interrupted_turn_state_is_durable() {
        let temp = TempDir::new().unwrap();
        let runtime = runtime(&temp, None);
        let turn = turn();
        runtime
            .append_turn_item_at(
                &turn,
                &TurnItem::user("prompt"),
                Some(0),
                "2026-07-27T00:00:00Z",
            )
            .unwrap();
        runtime
            .append_turn_interruption(
                "turn-a",
                "outer timeout cancelled agent loop",
                "2026-07-27T00:00:01Z",
            )
            .unwrap();
        let events = runtime.read_current_turn("turn-a").unwrap();
        assert!(events.iter().any(|event| {
            event.event_type == SessionEventType::Error && event.payload["state"] == "interrupted"
        }));
    }

    #[test]
    fn rejects_scope_reuse_and_reads_only_manifest_bound_fork() {
        let temp = TempDir::new().unwrap();
        let parent = runtime(&temp, None);
        let parent_turn = turn();
        parent
            .append_turn_item_at(
                &parent_turn,
                &TurnItem::user("parent prompt"),
                None,
                "2026-07-27T00:00:01Z",
            )
            .unwrap();

        let child = FileStoreSessionRuntime::create_or_load(
            FileStore::open(temp.path(), FileStoreOptions::default()).unwrap(),
            SessionRuntimeSpec {
                run: RunLocation::new("2026-07-27", "run-a").unwrap(),
                session_id: "session-b".to_owned(),
                role: "analyst.technical".to_owned(),
                phase: 1,
                profile: "analyst_report".to_owned(),
                fork: Some(ForkReference {
                    fork_from_session_id: "session-a".to_owned(),
                    fork_from_turn_id: "turn-a".to_owned(),
                }),
                created_at: "2026-07-27T00:01:00Z".to_owned(),
            },
        )
        .unwrap();
        assert_eq!(child.read_fork_turn().unwrap().len(), 1);

        let conflict = FileStoreSessionRuntime::create_or_load(
            FileStore::open(temp.path(), FileStoreOptions::default()).unwrap(),
            SessionRuntimeSpec {
                run: RunLocation::new("2026-07-27", "run-a").unwrap(),
                session_id: "session-a".to_owned(),
                role: "risk.neutral".to_owned(),
                phase: 5,
                profile: "risk_review".to_owned(),
                fork: None,
                created_at: "2026-07-27T00:02:00Z".to_owned(),
            },
        );
        assert!(conflict.is_err());
    }
}
