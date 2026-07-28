//! Append-only Experience events and deterministically rebuildable views.

use std::{collections::BTreeSet, path::PathBuf};

use orchestrator_core::{
    DocumentRef, ExperienceLifecyclePolicyV1, ExperienceOperation, ExperienceState,
    PatternIdentityV1, PolicyRef, RuleRevisionV1,
};
use serde::{Deserialize, Serialize};

use crate::{
    append_jsonl_locked, content_hash, ContentHashDocument, FileStore, JsonlRecord, Result,
    SafeSlug, StoreError, Versioned,
};

pub const EXPERIENCE_EVENT_SCHEMA_VERSION: u32 = 3;
pub const EXPERIENCE_VIEW_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExperienceEventV1 {
    pub schema_version: u32,
    pub sequence: u64,
    pub event_id: String,
    pub pattern_id: String,
    pub pattern_identity: Option<PatternIdentityV1>,
    pub rule_revision: Option<RuleRevisionV1>,
    pub operation: ExperienceOperation,
    pub source_run_id: Option<String>,
    pub outcome_id: Option<String>,
    pub source_refs: Vec<DocumentRef>,
    pub policy_ref: Option<PolicyRef>,
    pub independent_date_cluster: Option<String>,
    pub independent_regime: Option<String>,
    pub utility_sample_micros: Option<i64>,
    pub harmful_usage: Option<bool>,
    pub created_at: String,
    pub content_hash: String,
}

impl JsonlRecord for ExperienceEventV1 {
    const SCHEMA_VERSION: u32 = EXPERIENCE_EVENT_SCHEMA_VERSION;
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
            || self.pattern_id.trim().is_empty()
            || self.created_at.trim().is_empty()
        {
            return Err("schema, sequence, identity, or timestamp is invalid".into());
        }
        let expected =
            content_hash(&serde_json::to_value(self).map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())?;
        if expected != self.content_hash {
            return Err("event content hash mismatch".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExperienceViewV1 {
    pub schema_version: u32,
    pub pattern_id: String,
    pub lifecycle_policy_ref: PolicyRef,
    pub state: ExperienceState,
    pub support_count: u32,
    pub contradiction_count: u32,
    pub independent_date_cluster_count: u32,
    pub independent_regime_count: u32,
    pub harmful_usage_count: u32,
    pub harmful_usage_rate_ppm: u32,
    pub utility_ema_micros: Option<i64>,
    pub event_count: u64,
    pub last_supported_at: Option<String>,
    pub last_contradicted_at: Option<String>,
    pub rebuilt_at: String,
    pub content_hash: String,
}
impl Versioned for ExperienceViewV1 {
    const SCHEMA_VERSION: u32 = EXPERIENCE_VIEW_SCHEMA_VERSION;
}
impl ContentHashDocument for ExperienceViewV1 {
    fn content_hash(&self) -> &str {
        &self.content_hash
    }
    fn set_content_hash(&mut self, hash: String) {
        self.content_hash = hash;
    }
}

#[derive(Debug, Clone)]
pub struct ExperienceLedger {
    store: FileStore,
}
impl ExperienceLedger {
    pub fn new(store: FileStore) -> Self {
        Self { store }
    }
    pub fn append(&self, mut event: ExperienceEventV1) -> Result<ExperienceEventV1> {
        validate_event_fields(&event)?;
        let path = event_path(&event.pattern_id)?;
        let lock = lock_path(&event.pattern_id)?;
        self.store.with_exclusive_lock(&lock, || {
            let existing = if self.store.exists(&path)? {
                crate::read_jsonl_recover_tail::<ExperienceEventV1>(self.store.root(), &path)?
            } else {
                Vec::new()
            };
            if let Some(previous) = existing.iter().find(|previous| {
                previous.operation == event.operation
                    && previous.source_run_id == event.source_run_id
                    && previous.outcome_id == event.outcome_id
            }) {
                return Ok(previous.clone());
            }
            let next = existing.last().map_or(1, |item| item.sequence + 1);
            event.sequence = next;
            event.event_id = content_hash(&serde_json::json!({"pattern": event.pattern_id, "sequence": next, "operation": event.operation, "source": event.source_run_id, "outcome": event.outcome_id}))?;
            event.content_hash = content_hash(&serde_json::to_value(&event).map_err(|source| StoreError::JsonSerialize { source })?)?;
            append_jsonl_locked(self.store.root(), &path, &event)?;
            Ok(event.clone())
        })
    }

    /// Read only the typed, rebuildable View documents. Retrieval must not
    /// scan arbitrary files or infer a view from unvalidated JSON.
    pub fn list_views(&self) -> Result<Vec<ExperienceViewV1>> {
        let directory = PathBuf::from("knowledge/experiences/views");
        let absolute = self.store.root().join(&directory);
        if !absolute.exists() {
            return Ok(Vec::new());
        }
        let mut views = Vec::new();
        for entry in std::fs::read_dir(&absolute).map_err(|source| StoreError::Io {
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
            views.push(self.store.read_versioned_json::<ExperienceViewV1>(
                &directory.join(entry.file_name()),
                crate::FileSchemaKind::ExperienceView,
            )?);
        }
        views.sort_by(|left, right| left.pattern_id.cmp(&right.pattern_id));
        Ok(views)
    }

    pub fn read_events(&self, pattern_id: &str) -> Result<Vec<ExperienceEventV1>> {
        let path = event_path(pattern_id)?;
        if !self.store.exists(&path)? {
            return Ok(Vec::new());
        }
        crate::read_jsonl_recover_tail::<ExperienceEventV1>(self.store.root(), &path)
    }
    pub fn rebuild_view(&self, pattern_id: &str, rebuilt_at: &str) -> Result<ExperienceViewV1> {
        self.rebuild_view_with_policy(
            pattern_id,
            rebuilt_at,
            &ExperienceLifecyclePolicyV1::default(),
        )
    }

    pub fn rebuild_view_with_policy(
        &self,
        pattern_id: &str,
        rebuilt_at: &str,
        policy: &ExperienceLifecyclePolicyV1,
    ) -> Result<ExperienceViewV1> {
        let path = event_path(pattern_id)?;
        let events = if self.store.exists(&path)? {
            crate::read_jsonl_recover_tail::<ExperienceEventV1>(self.store.root(), &path)?
        } else {
            Vec::new()
        };
        let mut support = 0u32;
        let mut contradiction = 0u32;
        let mut dates = BTreeSet::new();
        let mut regimes = BTreeSet::new();
        let mut harmful = 0u32;
        let mut utility_ema = None::<i64>;
        let mut state_override = None;
        let mut last_support = None;
        let mut last_contradiction = None;
        for event in &events {
            match event.operation {
                ExperienceOperation::AddSupport => {
                    support += 1;
                    last_support = Some(event.created_at.clone());
                }
                ExperienceOperation::AddContradiction => {
                    contradiction += 1;
                    last_contradiction = Some(event.created_at.clone());
                }
                ExperienceOperation::Supersede => state_override = Some(ExperienceState::Contested),
                ExperienceOperation::Suspend => state_override = Some(ExperienceState::Suspended),
                ExperienceOperation::Reinstate => state_override = None,
                ExperienceOperation::Retire => state_override = Some(ExperienceState::Retired),
            };
            if let Some(value) = &event.independent_date_cluster {
                dates.insert(value.clone());
            }
            if let Some(value) = &event.independent_regime {
                regimes.insert(value.clone());
            }
            if event.harmful_usage == Some(true) {
                harmful += 1;
            }
            if let Some(sample) = event.utility_sample_micros {
                utility_ema = Some(match utility_ema {
                    None => sample,
                    // Fixed-point EMA (alpha=1/4), so rebuilding is exact
                    // across platforms and does not depend on float rounding.
                    Some(previous) => previous + (sample - previous) / 4,
                });
            }
        }
        let harmful_usage_rate_ppm = if events.is_empty() {
            0
        } else {
            (u64::from(harmful) * 1_000_000 / events.len() as u64) as u32
        };
        let state = state_override.unwrap_or_else(|| {
            if contradiction > 0 {
                ExperienceState::Contested
            } else if support >= policy.active_min_support
                && dates.len() as u32 >= policy.active_min_date_clusters
                && regimes.len() as u32 >= policy.active_min_regime_clusters
                && utility_ema.unwrap_or(0) >= policy.active_min_utility_ema_micros
                && harmful_usage_rate_ppm <= policy.active_max_harmful_usage_rate_ppm
            {
                ExperienceState::Active
            } else if support >= policy.repeated_warning_min_support {
                ExperienceState::RepeatedWarning
            } else {
                ExperienceState::Candidate
            }
        });
        let view = ExperienceViewV1 {
            schema_version: EXPERIENCE_VIEW_SCHEMA_VERSION,
            pattern_id: pattern_id.to_owned(),
            lifecycle_policy_ref: policy.policy_ref.clone(),
            state,
            support_count: support,
            contradiction_count: contradiction,
            independent_date_cluster_count: dates.len() as u32,
            independent_regime_count: regimes.len() as u32,
            harmful_usage_count: harmful,
            harmful_usage_rate_ppm,
            utility_ema_micros: utility_ema,
            event_count: events.len() as u64,
            last_supported_at: last_support,
            last_contradicted_at: last_contradiction,
            rebuilt_at: rebuilt_at.to_owned(),
            content_hash: String::new(),
        };
        self.store
            .write_authoritative_json(&view_path(pattern_id)?, view)
    }

    /// Rebuild every derived Experience View from the append-only event
    /// ledger. Event files are the authority; missing/stale views are never
    /// interpreted as evidence.
    pub fn rebuild_all_views(&self, rebuilt_at: &str) -> Result<Vec<ExperienceViewV1>> {
        self.rebuild_all_views_with_policy(rebuilt_at, &ExperienceLifecyclePolicyV1::default())
    }

    pub fn rebuild_all_views_with_policy(
        &self,
        rebuilt_at: &str,
        policy: &ExperienceLifecyclePolicyV1,
    ) -> Result<Vec<ExperienceViewV1>> {
        let directory = PathBuf::from("knowledge/experiences/events");
        let absolute = self.store.root().join(&directory);
        if !absolute.exists() {
            return Ok(Vec::new());
        }
        let mut pattern_ids = BTreeSet::new();
        for entry in std::fs::read_dir(&absolute).map_err(|source| StoreError::Io {
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
            let events = crate::read_jsonl_recover_tail::<ExperienceEventV1>(
                self.store.root(),
                &directory.join(entry.file_name()),
            )?;
            for event in events {
                pattern_ids.insert(event.pattern_id);
            }
        }
        pattern_ids
            .into_iter()
            .map(|pattern_id| self.rebuild_view_with_policy(&pattern_id, rebuilt_at, policy))
            .collect()
    }
}
fn validate_event_fields(event: &ExperienceEventV1) -> Result<()> {
    if event.schema_version != EXPERIENCE_EVENT_SCHEMA_VERSION
        || event.pattern_id.trim().is_empty()
        || event.created_at.trim().is_empty()
    {
        return Err(StoreError::InvalidDocument {
            kind: "experience event",
            message: "schema, pattern identity, or timestamp is invalid".into(),
        });
    }
    if matches!(
        event.operation,
        ExperienceOperation::AddSupport | ExperienceOperation::AddContradiction
    ) && (event.pattern_identity.is_none()
        || event.rule_revision.is_none()
        || event.source_run_id.as_deref().is_none_or(str::is_empty)
        || event.outcome_id.as_deref().is_none_or(str::is_empty)
        || event.source_refs.is_empty()
        || event.source_refs.iter().any(|reference| {
            reference.document_id.trim().is_empty()
                || reference.relative_path.trim().is_empty()
                || reference.content_hash.trim().is_empty()
        }))
    {
        return Err(StoreError::InvalidDocument {
            kind: "experience event",
            message: "support/contradiction requires pattern and outcome provenance".into(),
        });
    }
    Ok(())
}
fn event_path(pattern_id: &str) -> Result<PathBuf> {
    Ok(PathBuf::from("knowledge/experiences/events").join(format!(
        "{}.jsonl",
        SafeSlug::new("pattern", pattern_id)?.as_str()
    )))
}
fn view_path(pattern_id: &str) -> Result<PathBuf> {
    Ok(PathBuf::from("knowledge/experiences/views").join(format!(
        "{}.json",
        SafeSlug::new("pattern", pattern_id)?.as_str()
    )))
}
fn lock_path(pattern_id: &str) -> Result<PathBuf> {
    Ok(PathBuf::from("knowledge/experiences/.locks").join(format!(
        "{}.lock",
        SafeSlug::new("pattern", pattern_id)?.as_str()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{PatternActionKind, Scope, SignalFamily};
    use tempfile::tempdir;
    fn event(operation: ExperienceOperation, timestamp: &str) -> ExperienceEventV1 {
        ExperienceEventV1 {
            schema_version: EXPERIENCE_EVENT_SCHEMA_VERSION,
            sequence: 0,
            event_id: String::new(),
            pattern_id: "pattern".into(),
            pattern_identity: Some(PatternIdentityV1 {
                root_cause_phase: 2,
                source_role: "manager.research".into(),
                scope: Scope::Ticker,
                ticker: Some("QQQ".into()),
                horizon_trading_days: Some(3),
                regime: Default::default(),
                signal_family: SignalFamily::Technical,
                action_kind: PatternActionKind::Hold,
            }),
            rule_revision: Some(RuleRevisionV1 {
                revision: 1,
                rule: "Wait for confirmation".into(),
                trigger_conditions: vec!["signal".into()],
                invalidation_conditions: vec!["breakdown".into()],
            }),
            operation,
            source_run_id: Some(format!("source-{}", &timestamp[..13])),
            outcome_id: Some(format!("outcome-{}", &timestamp[..13])),
            source_refs: vec![DocumentRef {
                document_id: "summary".into(),
                relative_path: "runs/2026/source/index/summary/index.json".into(),
                content_hash: "sha256:summary".into(),
            }],
            policy_ref: None,
            independent_date_cluster: Some(timestamp[..10].into()),
            independent_regime: Some("risk_on".into()),
            utility_sample_micros: None,
            harmful_usage: Some(false),
            created_at: timestamp.into(),
            content_hash: String::new(),
        }
    }
    #[test]
    fn view_is_rebuildable_and_not_promoted_by_case_count_alone() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), crate::FileStoreOptions::default()).unwrap();
        let ledger = ExperienceLedger::new(store);
        ledger
            .append(event(
                ExperienceOperation::AddSupport,
                "2026-01-01T00:00:00Z",
            ))
            .unwrap();
        ledger
            .append(event(
                ExperienceOperation::AddSupport,
                "2026-01-01T01:00:00Z",
            ))
            .unwrap();
        ledger
            .append(event(
                ExperienceOperation::AddSupport,
                "2026-01-01T02:00:00Z",
            ))
            .unwrap();
        let view = ledger
            .rebuild_view("pattern", "2026-01-02T00:00:00Z")
            .unwrap();
        assert_eq!(view.state, ExperienceState::RepeatedWarning);
        assert_eq!(view.support_count, 3);
    }

    #[test]
    fn rebuild_all_recovers_missing_derived_views() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), crate::FileStoreOptions::default()).unwrap();
        let ledger = ExperienceLedger::new(store.clone());
        ledger
            .append(event(
                ExperienceOperation::AddSupport,
                "2026-01-01T00:00:00Z",
            ))
            .unwrap();
        assert_eq!(
            ledger
                .rebuild_all_views("2026-01-02T00:00:00Z")
                .unwrap()
                .len(),
            1
        );
        assert!(store
            .root()
            .join("knowledge/experiences/views")
            .read_dir()
            .unwrap()
            .next()
            .is_some());
    }

    #[test]
    fn support_event_is_idempotent_for_one_source_run_and_outcome() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), crate::FileStoreOptions::default()).unwrap();
        let ledger = ExperienceLedger::new(store);
        let first = ledger
            .append(event(
                ExperienceOperation::AddSupport,
                "2026-01-01T00:00:00Z",
            ))
            .unwrap();
        let second = ledger
            .append(event(
                ExperienceOperation::AddSupport,
                "2026-01-01T00:00:00Z",
            ))
            .unwrap();
        assert_eq!(first.event_id, second.event_id);
        assert_eq!(
            ledger
                .rebuild_view("pattern", "2026-01-02T00:00:00Z")
                .unwrap()
                .support_count,
            1
        );
    }

    #[test]
    fn contradiction_and_suspend_deterministically_demote_a_view() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), crate::FileStoreOptions::default()).unwrap();
        let ledger = ExperienceLedger::new(store);
        ledger
            .append(event(
                ExperienceOperation::AddSupport,
                "2026-01-01T00:00:00Z",
            ))
            .unwrap();
        ledger
            .append(event(
                ExperienceOperation::AddContradiction,
                "2026-01-02T00:00:00Z",
            ))
            .unwrap();
        assert_eq!(
            ledger
                .rebuild_view("pattern", "2026-01-03T00:00:00Z")
                .unwrap()
                .state,
            ExperienceState::Contested
        );
        ledger
            .append(event(ExperienceOperation::Suspend, "2026-01-04T00:00:00Z"))
            .unwrap();
        assert_eq!(
            ledger
                .rebuild_view("pattern", "2026-01-05T00:00:00Z")
                .unwrap()
                .state,
            ExperienceState::Suspended
        );
    }

    #[test]
    fn active_requires_independent_date_clusters_not_case_count_alone() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), crate::FileStoreOptions::default()).unwrap();
        let ledger = ExperienceLedger::new(store);
        for timestamp in [
            "2026-01-01T00:00:00Z",
            "2026-01-02T00:00:00Z",
            "2026-01-03T00:00:00Z",
        ] {
            ledger
                .append(event(ExperienceOperation::AddSupport, timestamp))
                .unwrap();
        }
        assert_eq!(
            ledger
                .rebuild_view("pattern", "2026-01-04T00:00:00Z")
                .unwrap()
                .state,
            ExperienceState::Active
        );
    }

    #[test]
    fn readers_return_only_typed_views_and_pattern_events() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), crate::FileStoreOptions::default()).unwrap();
        let ledger = ExperienceLedger::new(store);
        ledger
            .append(event(
                ExperienceOperation::AddSupport,
                "2026-01-01T00:00:00Z",
            ))
            .unwrap();
        ledger
            .rebuild_view("pattern", "2026-01-02T00:00:00Z")
            .unwrap();
        assert_eq!(ledger.list_views().unwrap().len(), 1);
        assert_eq!(ledger.read_events("pattern").unwrap().len(), 1);
        assert!(ledger.read_events("unknown").unwrap().is_empty());
    }
}
