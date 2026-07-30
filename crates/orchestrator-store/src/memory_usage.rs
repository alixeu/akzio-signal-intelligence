//! Raw, Rust-observed MemoryUsage access ledger.

use std::{fs, path::PathBuf};

use orchestrator_core::{
    DocumentRef, MemoryUsageEventV1, MemoryUsageReportV1, MEMORY_USAGE_EVENT_SCHEMA_VERSION,
    MEMORY_USAGE_REPORT_SCHEMA_VERSION,
};

use crate::{
    append_jsonl, content_hash, ContentHashDocument, FileStore, JsonlRecord, Result, RunLocation,
    SafeSlug, StoreError, Versioned,
};

impl JsonlRecord for MemoryUsageEventV1 {
    const SCHEMA_VERSION: u32 = MEMORY_USAGE_EVENT_SCHEMA_VERSION;
    fn schema_version(&self) -> u32 {
        self.schema_version
    }
    fn sequence(&self) -> u64 {
        self.sequence
    }
    fn validate_record(&self) -> std::result::Result<(), String> {
        if self.schema_version != Self::SCHEMA_VERSION
            || self.sequence == 0
            || self.event_id.trim().is_empty()
            || self.role.trim().is_empty()
            || self.unit_key.trim().is_empty()
            || self.created_at.trim().is_empty()
        {
            return Err("MemoryUsage event identity is invalid".into());
        }
        let expected =
            content_hash(&serde_json::to_value(self).map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())?;
        if expected != self.content_hash {
            return Err("MemoryUsage event content hash mismatch".into());
        }
        let application = matches!(
            self.kind,
            orchestrator_core::MemoryUsageEventKind::Application
        );
        let valid_application = self
            .expanded_pattern_id
            .as_deref()
            .is_some_and(|value| !value.is_empty())
            && self.application_disposition.is_some()
            && self
                .application_reason
                .as_deref()
                .is_some_and(|value| !value.is_empty())
            && self.retrieved_pattern_ids.is_empty()
            && self.lexical_query.is_none()
            && self.retrieval_stop_reason.is_none();
        if (application && !valid_application)
            || (!application
                && (self.application_disposition.is_some() || self.application_reason.is_some()))
        {
            return Err("MemoryUsage application semantics are invalid".into());
        }
        Ok(())
    }
}

impl Versioned for MemoryUsageReportV1 {
    const SCHEMA_VERSION: u32 = MEMORY_USAGE_REPORT_SCHEMA_VERSION;
}
impl ContentHashDocument for MemoryUsageReportV1 {
    fn content_hash(&self) -> &str {
        &self.content_hash
    }
    fn set_content_hash(&mut self, hash: String) {
        self.content_hash = hash;
    }
}

#[derive(Debug, Clone)]
pub struct MemoryUsageLedger {
    store: FileStore,
    location: RunLocation,
}

impl MemoryUsageLedger {
    pub fn new(store: FileStore, location: RunLocation) -> Self {
        Self { store, location }
    }

    pub fn append(&self, mut event: MemoryUsageEventV1) -> Result<MemoryUsageEventV1> {
        validate_event(&event)?;
        let path = event_path(&self.location, &event.unit_key)?;
        let events = if self.store.exists(&path)? {
            crate::read_jsonl_recover_tail::<MemoryUsageEventV1>(self.store.root(), &path)?
        } else {
            Vec::new()
        };
        let next = events.last().map_or(1, |event| event.sequence + 1);
        event.sequence = next;
        event.event_id = content_hash(&serde_json::json!({
            "unit": event.unit_key,
            "sequence": next,
            "kind": event.kind,
            "query": event.lexical_query,
            "expanded": event.expanded_pattern_id,
        }))?;
        event.content_hash = content_hash(
            &serde_json::to_value(&event).map_err(|source| StoreError::JsonSerialize { source })?,
        )?;
        append_jsonl(self.store.root(), &path, &event)?;
        Ok(event)
    }

    pub fn read_all(&self) -> Result<Vec<MemoryUsageEventV1>> {
        let directory = self
            .location
            .child_relative(PathBuf::from("memory/usage/events").as_path())?;
        let absolute = self.store.root().join(&directory);
        if !absolute.exists() {
            return Ok(Vec::new());
        }
        let mut events = Vec::new();
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
                    .is_none_or(|extension| extension != "jsonl")
            {
                continue;
            }
            events.extend(crate::read_jsonl_recover_tail::<MemoryUsageEventV1>(
                self.store.root(),
                &directory.join(entry.file_name()),
            )?);
        }
        events.sort_by(|left, right| {
            (&left.created_at, &left.unit_key, left.sequence).cmp(&(
                &right.created_at,
                &right.unit_key,
                right.sequence,
            ))
        });
        Ok(events)
    }

    pub fn publish_report(&self, created_at: &str) -> Result<DocumentRef> {
        let report = self.build_report(created_at)?;
        let path = PathBuf::from("knowledge/evaluation/memory_usage").join(format!(
            "{}.json",
            SafeSlug::new("memory-report", &report.report_id)?.as_str()
        ));
        let report = self.store.write_authoritative_json(&path, report)?;
        Ok(DocumentRef {
            document_id: report.report_id,
            relative_path: path.to_string_lossy().to_string(),
            content_hash: report.content_hash,
        })
    }

    fn build_report(&self, created_at: &str) -> Result<MemoryUsageReportV1> {
        let events = self.read_all()?;
        let report_id =
            content_hash(&serde_json::json!({"run_id":self.location.run_id,"events":events}))?;
        Ok(MemoryUsageReportV1 {
            schema_version: MEMORY_USAGE_REPORT_SCHEMA_VERSION,
            report_id,
            run_id: self.location.run_id.clone(),
            events,
            created_at: created_at.to_owned(),
            content_hash: String::new(),
        })
    }
}

fn validate_event(event: &MemoryUsageEventV1) -> Result<()> {
    if event.schema_version != MEMORY_USAGE_EVENT_SCHEMA_VERSION
        || event.role.trim().is_empty()
        || event.unit_key.trim().is_empty()
        || event.created_at.trim().is_empty()
    {
        return Err(StoreError::InvalidDocument {
            kind: "memory usage event",
            message: "schema or Rust-owned scope is invalid".into(),
        });
    }
    match event.kind {
        orchestrator_core::MemoryUsageEventKind::Application => {
            if event
                .expanded_pattern_id
                .as_deref()
                .is_none_or(str::is_empty)
                || event.application_disposition.is_none()
                || event
                    .application_reason
                    .as_deref()
                    .is_none_or(str::is_empty)
                || !event.retrieved_pattern_ids.is_empty()
                || event.lexical_query.is_some()
                || event.retrieval_stop_reason.is_some()
            {
                return Err(StoreError::InvalidDocument {
                    kind: "memory usage event",
                    message:
                        "application must identify one Pattern and a reason without search fields"
                            .into(),
                });
            }
        }
        orchestrator_core::MemoryUsageEventKind::Search
        | orchestrator_core::MemoryUsageEventKind::Expand => {
            if event.application_disposition.is_some() || event.application_reason.is_some() {
                return Err(StoreError::InvalidDocument {
                    kind: "memory usage event",
                    message: "search/expand may not carry a model application claim".into(),
                });
            }
        }
    }
    Ok(())
}
fn event_path(location: &RunLocation, unit_key: &str) -> Result<PathBuf> {
    location.child_relative(&PathBuf::from("memory/usage/events").join(format!(
        "{}.jsonl",
        SafeSlug::new("memory-unit", unit_key)?.as_str()
    )))
}
#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::MemoryUsageEventKind;
    use tempfile::tempdir;

    fn event(query: &str) -> MemoryUsageEventV1 {
        MemoryUsageEventV1 {
            schema_version: MEMORY_USAGE_EVENT_SCHEMA_VERSION,
            sequence: 0,
            event_id: String::new(),
            kind: MemoryUsageEventKind::Search,
            role: "manager.research".into(),
            phase: 3,
            ticker: Some("QQQ".into()),
            unit_key: "phase3:manager:QQQ".into(),
            lexical_query: Some(query.into()),
            retrieved_pattern_ids: vec!["pattern".into()],
            expanded_pattern_id: None,
            retrieval_stop_reason: Some("sufficient".into()),
            application_disposition: None,
            application_reason: None,
            created_at: "2026-01-01T00:00:00Z".into(),
            content_hash: String::new(),
        }
    }

    #[test]
    fn published_report_survives_run_cleanup() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), Default::default()).unwrap();
        let ledger = MemoryUsageLedger::new(
            store.clone(),
            RunLocation::new("2026-01-01", "run").unwrap(),
        );
        ledger.append(event("technical")).unwrap();
        ledger.append(event("confirmation")).unwrap();

        let reference = ledger.publish_report("2026-01-02T00:00:00Z").unwrap();

        assert!(reference
            .relative_path
            .starts_with("knowledge/evaluation/memory_usage/"));
        assert!(store
            .exists(std::path::Path::new(&reference.relative_path))
            .unwrap());
        let report: MemoryUsageReportV1 = store
            .read_versioned_json(
                std::path::Path::new(&reference.relative_path),
                crate::FileSchemaKind::MemoryUsageReport,
            )
            .unwrap();
        assert_eq!(report.events.len(), 2);
    }

    #[test]
    fn application_claim_requires_one_pattern_and_reason() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), crate::FileStoreOptions::default()).unwrap();
        let location = RunLocation::new("2026-01-01", "run").unwrap();
        let ledger = MemoryUsageLedger::new(store, location);
        let mut application = event("unused");
        application.kind = orchestrator_core::MemoryUsageEventKind::Application;
        application.lexical_query = None;
        application.retrieved_pattern_ids.clear();
        application.retrieval_stop_reason = None;
        application.expanded_pattern_id = Some("pattern".into());
        application.application_disposition =
            Some(orchestrator_core::MemoryApplicationDisposition::Applied);
        application.application_reason = Some("matches current evidence".into());
        assert!(ledger.append(application).is_ok());
    }
}
