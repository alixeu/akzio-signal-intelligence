//! Canonical, revisioned outcome persistence.
//!
//! The public interface deliberately owns path construction and persistence
//! authorization.  Callers cannot choose the canonical outcome directory by
//! passing an arbitrary path to a generic FileStore writer.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use orchestrator_core::{
    DecisionSnapshotV2, DocumentRef, EvaluationInputManifestV1, MaterializationBatchReportV1,
    MaterializationGapV1, MaterializationIntegrityIssueV1, MemoryAttributionRecordV1,
    OutcomeHeadV1, OutcomeRecordV1, OutcomeRevisionCommitV1, OutcomeRevisionOperation,
    OutcomeStatus, OutcomeWriteReceiptV1, OutcomeWriteResultKind, PersistenceContextV1,
    PersistenceNamespace, RunPurpose, DECISION_SNAPSHOT_SCHEMA_VERSION,
    EVALUATION_INPUT_MANIFEST_SCHEMA_VERSION, MATERIALIZATION_BATCH_REPORT_SCHEMA_VERSION,
    MATERIALIZATION_GAP_SCHEMA_VERSION, MATERIALIZATION_INTEGRITY_ISSUE_SCHEMA_VERSION,
    MEMORY_ATTRIBUTION_SCHEMA_VERSION, OUTCOME_HEAD_SCHEMA_VERSION, OUTCOME_RECORD_SCHEMA_VERSION,
    OUTCOME_REVISION_COMMIT_SCHEMA_VERSION, OUTCOME_WRITE_RECEIPT_SCHEMA_VERSION,
};
use serde_json::json;

use crate::{
    content_hash, content_hash_bytes, ContentHashDocument, FileSchemaKind, FileStore, Result,
    RunLocation, SafeSlug, StoreError, Versioned,
};

const CANONICAL_ROOT: &str = "knowledge/evaluation";

macro_rules! versioned_hash_document {
    ($type:ty, $version:expr) => {
        impl Versioned for $type {
            const SCHEMA_VERSION: u32 = $version;
        }
        impl ContentHashDocument for $type {
            fn content_hash(&self) -> &str {
                &self.content_hash
            }
            fn set_content_hash(&mut self, hash: String) {
                self.content_hash = hash;
            }
        }
    };
}

versioned_hash_document!(DecisionSnapshotV2, DECISION_SNAPSHOT_SCHEMA_VERSION);
versioned_hash_document!(
    EvaluationInputManifestV1,
    EVALUATION_INPUT_MANIFEST_SCHEMA_VERSION
);
versioned_hash_document!(OutcomeRecordV1, OUTCOME_RECORD_SCHEMA_VERSION);
versioned_hash_document!(
    OutcomeRevisionCommitV1,
    OUTCOME_REVISION_COMMIT_SCHEMA_VERSION
);
versioned_hash_document!(OutcomeHeadV1, OUTCOME_HEAD_SCHEMA_VERSION);
versioned_hash_document!(OutcomeWriteReceiptV1, OUTCOME_WRITE_RECEIPT_SCHEMA_VERSION);
versioned_hash_document!(MaterializationGapV1, MATERIALIZATION_GAP_SCHEMA_VERSION);
versioned_hash_document!(
    MaterializationIntegrityIssueV1,
    MATERIALIZATION_INTEGRITY_ISSUE_SCHEMA_VERSION
);
versioned_hash_document!(
    MaterializationBatchReportV1,
    MATERIALIZATION_BATCH_REPORT_SCHEMA_VERSION
);
versioned_hash_document!(MemoryAttributionRecordV1, MEMORY_ATTRIBUTION_SCHEMA_VERSION);

#[derive(Debug, Clone)]
pub struct EvaluationStore {
    store: FileStore,
    context: PersistenceContextV1,
}

impl EvaluationStore {
    pub fn open(store: FileStore, context: PersistenceContextV1) -> Result<Self> {
        validate_context(&context)?;
        Ok(Self { store, context })
    }

    pub fn context(&self) -> &PersistenceContextV1 {
        &self.context
    }

    pub fn write_decision(
        &self,
        location: &RunLocation,
        decision: DecisionSnapshotV2,
    ) -> Result<DecisionSnapshotV2> {
        self.require_decision_write()?;
        validate_decision(&decision, location)?;
        let relative = self.decision_relative(location, &decision.decision_id)?;
        create_or_validate(
            &self.store,
            &relative,
            FileSchemaKind::DecisionSnapshot,
            decision,
        )
        .map(|(document, _)| document)
    }

    pub fn read_decision(
        &self,
        location: &RunLocation,
        decision_id: &str,
    ) -> Result<DecisionSnapshotV2> {
        self.store.read_versioned_json(
            &self.decision_relative(location, decision_id)?,
            FileSchemaKind::DecisionSnapshot,
        )
    }

    pub fn decision_reference(
        &self,
        location: &RunLocation,
        decision_id: &str,
    ) -> Result<DocumentRef> {
        let decision = self.read_decision(location, decision_id)?;
        let relative = self.decision_relative(location, decision_id)?;
        Ok(document_ref(
            &decision.decision_id,
            &relative,
            &decision.content_hash,
        ))
    }

    /// Enumerate only the Rust-owned DecisionSnapshot directory for one run.
    /// Unknown files, a malformed schema, or a bad content hash fail closed
    /// through `read_versioned_json`; callers must not treat a partial ledger
    /// scan as a valid materialization input.
    pub fn list_decisions(&self, location: &RunLocation) -> Result<Vec<DecisionSnapshotV2>> {
        let directory = match &self.context.namespace {
            PersistenceNamespace::Canonical => {
                location.child_relative(Path::new("learning/v2/decisions"))?
            }
            _ => {
                return Err(invalid(
                    "evaluation reader",
                    "decision scans require canonical namespace",
                ))
            }
        };
        let absolute = self.store.root().join(&directory);
        if !absolute.exists() {
            return Ok(Vec::new());
        }
        let mut decisions = Vec::new();
        for entry in fs::read_dir(&absolute).map_err(|source| StoreError::Io {
            path: absolute.clone(),
            source,
        })? {
            let entry = entry.map_err(|source| StoreError::Io {
                path: absolute.clone(),
                source,
            })?;
            let metadata = entry.metadata().map_err(|source| StoreError::Io {
                path: entry.path(),
                source,
            })?;
            if !metadata.is_file() || !entry.file_name().to_string_lossy().ends_with(".json") {
                continue;
            }
            let relative = directory.join(entry.file_name());
            let decision = self.store.read_versioned_json::<DecisionSnapshotV2>(
                &relative,
                FileSchemaKind::DecisionSnapshot,
            )?;
            validate_decision(&decision, location)?;
            decisions.push(decision);
        }
        decisions.sort_by(|left, right| left.decision_id.cmp(&right.decision_id));
        Ok(decisions)
    }

    /// Publish or reuse one global canonical Outcome and write only a receipt
    /// beneath the evaluation run.  The evaluation-key lock serializes head
    /// transitions; the outcome-id lock enforces cross-run create-if-absent.
    pub fn publish_outcome(
        &self,
        evaluation_run: &RunLocation,
        outcome: OutcomeRecordV1,
        revision_reason: orchestrator_core::OutcomeRevisionReason,
    ) -> Result<OutcomeWriteReceiptV1> {
        self.require_outcome_write()?;
        validate_outcome(&outcome)?;
        let evaluation_lock = self.lock_relative("evaluation-keys", &outcome.evaluation_key)?;
        self.store.with_exclusive_lock(&evaluation_lock, || {
            let mut head = self.read_or_rebuild_head(&outcome.evaluation_key)?;
            let previous = head.current_outcome_id.clone();
            let outcome_lock = self.lock_relative("outcomes", &outcome.outcome_id)?;
            self.store.with_exclusive_lock(&outcome_lock, || {
                // Always validate the immutable record before accepting an
                // idempotent receipt.  An already-current ID with different
                // bytes is a provenance violation, not a duplicate.
                let outcome_relative = self.outcome_relative(&outcome.outcome_id)?;
                let (sealed_outcome, existed) = create_or_validate(
                    &self.store,
                    &outcome_relative,
                    FileSchemaKind::OutcomeRecord,
                    outcome.clone(),
                )?;
                if previous.as_deref() == Some(outcome.outcome_id.as_str()) {
                    return self.write_receipt(
                        evaluation_run,
                        &sealed_outcome,
                        OutcomeWriteResultKind::AlreadyCurrent,
                        None,
                    );
                }
                if outcome.supersedes_outcome_id != previous {
                    return Err(invalid(
                        "outcome record",
                        "supersedes_outcome_id must equal the current outcome head",
                    ));
                }
                let sequence = head.as_of_revision.saturating_add(1);
                let commit = new_publish_commit(
                    &outcome,
                    sequence,
                    optional_hash(&head.content_hash),
                    revision_reason.clone(),
                )?;
                let commit_relative =
                    self.revision_relative(&outcome.evaluation_key, sequence, &commit.commit_id)?;
                let (sealed_commit, _) = create_or_validate(
                    &self.store,
                    &commit_relative,
                    FileSchemaKind::OutcomeRevisionCommit,
                    commit,
                )?;
                apply_commit(&mut head, &sealed_commit)?;
                head.revision_set_hash = revision_set_hash(&head)?;
                let head_relative = self.head_relative(&outcome.evaluation_key)?;
                let sealed_head = self.store.write_authoritative_json(&head_relative, head)?;
                let receipt_result = if existed || previous.is_some() {
                    OutcomeWriteResultKind::PublishedRevision
                } else {
                    OutcomeWriteResultKind::Created
                };
                self.write_receipt(
                    evaluation_run,
                    &sealed_outcome,
                    receipt_result,
                    Some(document_ref(
                        &sealed_commit.commit_id,
                        &commit_relative,
                        &sealed_commit.content_hash,
                    )),
                )
                .inspect(|_receipt| {
                    debug_assert_eq!(
                        sealed_head.current_outcome_id.as_deref(),
                        Some(outcome.outcome_id.as_str())
                    );
                })
            })
        })
    }

    pub fn write_memory_attribution(
        &self,
        attribution: MemoryAttributionRecordV1,
    ) -> Result<MemoryAttributionRecordV1> {
        self.require_outcome_write()?;
        validate_attribution(&attribution)?;
        create_or_validate(
            &self.store,
            &self.attribution_relative(&attribution.outcome_ref.document_id)?,
            FileSchemaKind::MemoryAttribution,
            attribution,
        )
        .map(|(document, _)| document)
    }

    pub fn write_integrity_issue(
        &self,
        issue: MaterializationIntegrityIssueV1,
    ) -> Result<DocumentRef> {
        self.require_outcome_write()?;
        if issue.schema_version != MATERIALIZATION_INTEGRITY_ISSUE_SCHEMA_VERSION
            || issue.issue_id.trim().is_empty()
            || issue.detail.trim().is_empty()
            || issue.created_at.trim().is_empty()
        {
            return Err(invalid(
                "materialization integrity issue",
                "schema, identity, detail, or timestamp is invalid",
            ));
        }
        let relative = self.integrity_issue_relative(&issue.issue_id)?;
        let issue = create_or_validate(
            &self.store,
            &relative,
            FileSchemaKind::MaterializationIntegrityIssue,
            issue,
        )?
        .0;
        Ok(document_ref(
            &issue.issue_id,
            &relative,
            &issue.content_hash,
        ))
    }

    pub fn read_memory_attribution(
        &self,
        outcome_id: &str,
    ) -> Result<Option<MemoryAttributionRecordV1>> {
        let relative = self.attribution_relative(outcome_id)?;
        if !self.store.exists(&relative)? {
            return Ok(None);
        }
        self.store
            .read_versioned_json(&relative, FileSchemaKind::MemoryAttribution)
            .map(Some)
    }

    pub fn write_gap(&self, gap: MaterializationGapV1) -> Result<MaterializationGapV1> {
        self.require_outcome_write()?;
        let relative = self.gap_relative(&gap.evaluation_key, &gap.gap_id)?;
        create_or_validate(
            &self.store,
            &relative,
            FileSchemaKind::MaterializationGap,
            gap,
        )
        .map(|(document, _)| document)
    }

    pub fn gap_reference(&self, evaluation_key: &str, gap_id: &str) -> Result<DocumentRef> {
        let gap = self.store.read_versioned_json::<MaterializationGapV1>(
            &self.gap_relative(evaluation_key, gap_id)?,
            FileSchemaKind::MaterializationGap,
        )?;
        Ok(document_ref(
            &gap.gap_id,
            &self.gap_relative(evaluation_key, gap_id)?,
            &gap.content_hash,
        ))
    }

    /// Persist the exact market-data provenance before publishing a result.
    /// Outcomes can then cite a sealed evaluation-run manifest rather than a
    /// mutable technical CSV path.
    pub fn write_evaluation_input_manifest(
        &self,
        manifest: EvaluationInputManifestV1,
    ) -> Result<EvaluationInputManifestV1> {
        self.require_outcome_write()?;
        validate_evaluation_input_manifest(&manifest, &self.context)?;
        let relative = self.evaluation_input_manifest_relative(&manifest.manifest_id)?;
        create_or_validate(
            &self.store,
            &relative,
            FileSchemaKind::EvaluationInputManifest,
            manifest,
        )
        .map(|(document, _)| document)
    }

    /// Store an immutable raw market-data payload beneath the evaluation run.
    /// The path includes its content hash, preventing an updated provider file
    /// from overwriting the bytes used by a prior materialization attempt.
    pub fn write_evaluation_input_payload(
        &self,
        ticker: &str,
        interval: &str,
        payload: &[u8],
    ) -> Result<DocumentRef> {
        self.require_outcome_write()?;
        if ticker.trim().is_empty() || interval.trim().is_empty() || payload.is_empty() {
            return Err(invalid(
                "evaluation input payload",
                "ticker, interval, and payload are required",
            ));
        }
        let payload_hash = content_hash_bytes(payload);
        let identity = format!("{}\0{}\0{}", ticker, interval, payload_hash);
        let relative = self.evaluation_input_raw_relative(&identity)?;
        if self.store.exists(&relative)? {
            let existing = self.store.read_bytes(&relative)?;
            if existing != payload {
                return Err(invalid(
                    "evaluation input payload",
                    "immutable input identity already exists with different bytes",
                ));
            }
        } else {
            self.store.write_bytes(&relative, payload)?;
        }
        Ok(document_ref(&identity, &relative, &payload_hash))
    }

    pub fn read_evaluation_input_payload(&self, reference: &DocumentRef) -> Result<Vec<u8>> {
        let directory = self.evaluation_input_raw_directory()?;
        let relative = PathBuf::from(&reference.relative_path);
        if !relative.starts_with(&directory) {
            return Err(invalid(
                "evaluation input payload",
                "provenance reference escapes the Rust-owned evaluation input directory",
            ));
        }
        let payload = self.store.read_bytes(&relative)?;
        if content_hash_bytes(&payload) != reference.content_hash {
            return Err(invalid(
                "evaluation input payload",
                "payload hash differs from its provenance reference",
            ));
        }
        Ok(payload)
    }

    pub fn read_evaluation_input_manifest(
        &self,
        manifest_id: &str,
    ) -> Result<EvaluationInputManifestV1> {
        let manifest = self.store.read_versioned_json(
            &self.evaluation_input_manifest_relative(manifest_id)?,
            FileSchemaKind::EvaluationInputManifest,
        )?;
        validate_evaluation_input_manifest(&manifest, &self.context)?;
        Ok(manifest)
    }

    pub fn evaluation_input_manifest_reference(&self, manifest_id: &str) -> Result<DocumentRef> {
        let manifest = self.read_evaluation_input_manifest(manifest_id)?;
        let relative = self.evaluation_input_manifest_relative(manifest_id)?;
        Ok(document_ref(
            &manifest.manifest_id,
            &relative,
            &manifest.content_hash,
        ))
    }

    pub fn write_batch_report(
        &self,
        evaluation_run: &RunLocation,
        report: MaterializationBatchReportV1,
    ) -> Result<MaterializationBatchReportV1> {
        self.require_outcome_write()?;
        let relative = self.report_relative(evaluation_run, &report.batch_id)?;
        create_or_validate(
            &self.store,
            &relative,
            FileSchemaKind::MaterializationBatchReport,
            report,
        )
        .map(|(document, _)| document)
    }

    pub fn read_current_outcome(&self, evaluation_key: &str) -> Result<Option<OutcomeRecordV1>> {
        let head = self.read_or_rebuild_head(evaluation_key)?;
        match head.current_outcome_id {
            Some(outcome_id) => self
                .store
                .read_versioned_json(
                    &self.outcome_relative(&outcome_id)?,
                    FileSchemaKind::OutcomeRecord,
                )
                .map(Some),
            None => Ok(None),
        }
    }

    pub fn outcome_reference(&self, outcome_id: &str) -> Result<DocumentRef> {
        let outcome: OutcomeRecordV1 = self.store.read_versioned_json(
            &self.outcome_relative(outcome_id)?,
            FileSchemaKind::OutcomeRecord,
        )?;
        Ok(document_ref(
            &outcome.outcome_id,
            &self.outcome_relative(outcome_id)?,
            &outcome.content_hash,
        ))
    }

    /// Return only the current member of each evaluation chain in this
    /// store's namespace. Superseded and invalidated Outcome files are
    /// deliberately invisible to scheduling consumers; callers that schedule
    /// production Reflection Tasks must use a canonical reader explicitly.
    pub fn list_current_outcomes(&self) -> Result<Vec<OutcomeRecordV1>> {
        let directory = self.evaluation_root()?.join("views/outcome_heads");
        let absolute = self.store.root().join(&directory);
        if !absolute.exists() {
            return Ok(Vec::new());
        }
        let mut outcomes = Vec::new();
        for entry in fs::read_dir(&absolute).map_err(|source| StoreError::Io {
            path: absolute.clone(),
            source,
        })? {
            let entry = entry.map_err(|source| StoreError::Io {
                path: absolute.clone(),
                source,
            })?;
            if !entry
                .metadata()
                .map_err(|source| StoreError::Io {
                    path: entry.path(),
                    source,
                })?
                .is_file()
                || !entry.file_name().to_string_lossy().ends_with(".json")
            {
                continue;
            }
            let relative = directory.join(entry.file_name());
            let head: OutcomeHeadV1 = self
                .store
                .read_versioned_json(&relative, FileSchemaKind::OutcomeHead)?;
            if let Some(outcome_id) = head.current_outcome_id {
                let outcome: OutcomeRecordV1 = self.store.read_versioned_json(
                    &self.outcome_relative(&outcome_id)?,
                    FileSchemaKind::OutcomeRecord,
                )?;
                outcomes.push(outcome);
            }
        }
        outcomes.sort_by(|left, right| left.evaluation_key.cmp(&right.evaluation_key));
        Ok(outcomes)
    }

    pub fn rebuild_outcome_head(&self, evaluation_key: &str) -> Result<OutcomeHeadV1> {
        let mut commits = self.read_revision_commits(evaluation_key)?;
        commits.sort_by_key(|commit| commit.revision_sequence);
        // A first publish may follow an already-persisted empty head, so the
        // genesis state is part of the revision chain and must be sealed too.
        let mut head = crate::seal_content_hash(empty_head(evaluation_key))?;
        for commit in commits {
            if commit.revision_sequence != head.as_of_revision.saturating_add(1) {
                return Err(invalid(
                    "outcome revision",
                    "revision sequence is not contiguous",
                ));
            }
            if commit.previous_head_hash != optional_hash(&head.content_hash) {
                return Err(invalid(
                    "outcome revision",
                    "previous head hash does not match",
                ));
            }
            apply_commit(&mut head, &commit)?;
            head.revision_set_hash = revision_set_hash(&head)?;
            // Revision commits chain the sealed state of the previous head.
            // Re-sealing after every replay is therefore required before the
            // next commit's previous_head_hash can be verified.
            head = crate::seal_content_hash(head)?;
        }
        self.store
            .write_authoritative_json(&self.head_relative(evaluation_key)?, head)
    }

    fn read_or_rebuild_head(&self, evaluation_key: &str) -> Result<OutcomeHeadV1> {
        let relative = self.head_relative(evaluation_key)?;
        if self.store.exists(&relative)? {
            self.store
                .read_versioned_json(&relative, FileSchemaKind::OutcomeHead)
        } else {
            self.rebuild_outcome_head(evaluation_key)
        }
    }

    fn read_revision_commits(&self, evaluation_key: &str) -> Result<Vec<OutcomeRevisionCommitV1>> {
        let directory = self.revision_directory(evaluation_key)?;
        let absolute = self.store.root().join(&directory);
        if !absolute.exists() {
            return Ok(Vec::new());
        }
        let mut commits = Vec::new();
        for entry in fs::read_dir(&absolute).map_err(|source| StoreError::Io {
            path: absolute.clone(),
            source,
        })? {
            let entry = entry.map_err(|source| StoreError::Io {
                path: absolute.clone(),
                source,
            })?;
            let metadata = entry.metadata().map_err(|source| StoreError::Io {
                path: entry.path(),
                source,
            })?;
            if !metadata.is_file() {
                continue;
            }
            let name = entry.file_name();
            if !name.to_string_lossy().ends_with(".json") {
                continue;
            }
            let relative = directory.join(name);
            commits.push(
                self.store
                    .read_versioned_json(&relative, FileSchemaKind::OutcomeRevisionCommit)?,
            );
        }
        Ok(commits)
    }

    fn write_receipt(
        &self,
        evaluation_run: &RunLocation,
        outcome: &OutcomeRecordV1,
        result: OutcomeWriteResultKind,
        revision_commit_ref: Option<DocumentRef>,
    ) -> Result<OutcomeWriteReceiptV1> {
        let outcome_relative = self.outcome_relative(&outcome.outcome_id)?;
        let receipt_id = content_hash(&json!({
            "evaluation_run_id": evaluation_run.run_id,
            "outcome_id": outcome.outcome_id,
            "result": result,
            "revision_commit_ref": revision_commit_ref,
        }))?;
        let receipt = OutcomeWriteReceiptV1 {
            schema_version: OUTCOME_WRITE_RECEIPT_SCHEMA_VERSION,
            receipt_id: receipt_id.clone(),
            evaluation_run_id: evaluation_run.run_id.clone(),
            outcome_id: outcome.outcome_id.clone(),
            evaluation_key: outcome.evaluation_key.clone(),
            result,
            outcome_ref: document_ref(
                &outcome.outcome_id,
                &outcome_relative,
                &outcome.content_hash,
            ),
            revision_commit_ref,
            created_at: outcome.created_at.clone(),
            content_hash: String::new(),
        };
        let relative = self.receipt_relative(evaluation_run, &receipt_id)?;
        create_or_validate(
            &self.store,
            &relative,
            FileSchemaKind::OutcomeWriteReceipt,
            receipt,
        )
        .map(|(document, _)| document)
    }

    fn require_decision_write(&self) -> Result<()> {
        if matches!(self.context.namespace, PersistenceNamespace::Disabled) {
            return Err(invalid(
                "evaluation writer",
                "run purpose does not permit Decision writes",
            ));
        }
        if matches!(self.context.namespace, PersistenceNamespace::Canonical)
            && (!self.context.run_purpose.may_write_canonical_evaluation()
                || !self.context.canonical_memory_writes_enabled)
        {
            return Err(invalid(
                "evaluation writer",
                "canonical Decision writes require enabled Paper or Live context",
            ));
        }
        Ok(())
    }

    fn require_outcome_write(&self) -> Result<()> {
        self.require_decision_write()
    }

    fn decision_relative(&self, location: &RunLocation, decision_id: &str) -> Result<PathBuf> {
        let filename = format!("{}.json", SafeSlug::new("decision", decision_id)?.as_str());
        match &self.context.namespace {
            PersistenceNamespace::Canonical => {
                location.child_relative(Path::new("learning/v2/decisions").join(filename).as_path())
            }
            PersistenceNamespace::Debug { invocation_id } => namespace_relative(
                "debug",
                invocation_id,
                Path::new("decisions").join(filename).as_path(),
            ),
            PersistenceNamespace::Replay { replay_id } => namespace_relative(
                "replay",
                replay_id,
                Path::new("decisions").join(filename).as_path(),
            ),
            PersistenceNamespace::MigrationFixture { fixture_id } => namespace_relative(
                "fixture",
                fixture_id,
                Path::new("decisions").join(filename).as_path(),
            ),
            PersistenceNamespace::Disabled => Err(invalid(
                "evaluation writer",
                "disabled namespace has no Decision path",
            )),
        }
    }

    fn receipt_relative(&self, location: &RunLocation, receipt_id: &str) -> Result<PathBuf> {
        let filename = format!("{}.json", SafeSlug::new("receipt", receipt_id)?.as_str());
        match &self.context.namespace {
            PersistenceNamespace::Canonical => location.child_relative(
                Path::new("receipts/materialization")
                    .join(filename)
                    .as_path(),
            ),
            PersistenceNamespace::Debug { invocation_id } => namespace_relative(
                "debug",
                invocation_id,
                Path::new("receipts/materialization")
                    .join(filename)
                    .as_path(),
            ),
            PersistenceNamespace::Replay { replay_id } => namespace_relative(
                "replay",
                replay_id,
                Path::new("receipts/materialization")
                    .join(filename)
                    .as_path(),
            ),
            PersistenceNamespace::MigrationFixture { fixture_id } => namespace_relative(
                "fixture",
                fixture_id,
                Path::new("receipts/materialization")
                    .join(filename)
                    .as_path(),
            ),
            PersistenceNamespace::Disabled => Err(invalid(
                "evaluation writer",
                "disabled namespace has no receipt path",
            )),
        }
    }

    fn report_relative(&self, location: &RunLocation, report_id: &str) -> Result<PathBuf> {
        let filename = format!("{}.json", SafeSlug::new("batch", report_id)?.as_str());
        match &self.context.namespace {
            PersistenceNamespace::Canonical => location.child_relative(
                Path::new("reports/materialization")
                    .join(filename)
                    .as_path(),
            ),
            PersistenceNamespace::Debug { invocation_id } => namespace_relative(
                "debug",
                invocation_id,
                Path::new("reports/materialization")
                    .join(filename)
                    .as_path(),
            ),
            PersistenceNamespace::Replay { replay_id } => namespace_relative(
                "replay",
                replay_id,
                Path::new("reports/materialization")
                    .join(filename)
                    .as_path(),
            ),
            PersistenceNamespace::MigrationFixture { fixture_id } => namespace_relative(
                "fixture",
                fixture_id,
                Path::new("reports/materialization")
                    .join(filename)
                    .as_path(),
            ),
            PersistenceNamespace::Disabled => Err(invalid(
                "evaluation writer",
                "disabled namespace has no report path",
            )),
        }
    }

    fn evaluation_input_manifest_relative(&self, manifest_id: &str) -> Result<PathBuf> {
        let filename = format!("{}.json", SafeSlug::new("evalinput", manifest_id)?.as_str());
        Ok(self
            .evaluation_root()?
            .join("inputs/manifests")
            .join(filename))
    }

    fn evaluation_input_raw_directory(&self) -> Result<PathBuf> {
        Ok(self.evaluation_root()?.join("inputs/raw"))
    }

    fn evaluation_input_raw_relative(&self, identity: &str) -> Result<PathBuf> {
        Ok(self.evaluation_input_raw_directory()?.join(format!(
            "{}.csv",
            SafeSlug::new("evalraw", identity)?.as_str()
        )))
    }

    fn evaluation_root(&self) -> Result<PathBuf> {
        evaluation_root_for_context(&self.context)
    }

    fn outcome_relative(&self, outcome_id: &str) -> Result<PathBuf> {
        Ok(self.evaluation_root()?.join("outcomes").join(format!(
            "{}.json",
            SafeSlug::new("outcome", outcome_id)?.as_str()
        )))
    }

    fn attribution_relative(&self, outcome_id: &str) -> Result<PathBuf> {
        Ok(self.evaluation_root()?.join("attributions").join(format!(
            "{}.json",
            SafeSlug::new("attribution", outcome_id)?.as_str()
        )))
    }

    fn integrity_issue_relative(&self, issue_id: &str) -> Result<PathBuf> {
        Ok(self.evaluation_root()?.join("integrity").join(format!(
            "{}.json",
            SafeSlug::new("integrity", issue_id)?.as_str()
        )))
    }

    fn gap_relative(&self, evaluation_key: &str, gap_id: &str) -> Result<PathBuf> {
        Ok(self
            .evaluation_root()?
            .join("gaps")
            .join(SafeSlug::new("eval", evaluation_key)?.as_str())
            .join(format!("{}.json", SafeSlug::new("gap", gap_id)?.as_str())))
    }

    fn revision_directory(&self, evaluation_key: &str) -> Result<PathBuf> {
        Ok(self
            .evaluation_root()?
            .join("revisions")
            .join(SafeSlug::new("eval", evaluation_key)?.as_str()))
    }

    fn revision_relative(
        &self,
        evaluation_key: &str,
        sequence: u64,
        commit_id: &str,
    ) -> Result<PathBuf> {
        Ok(self.revision_directory(evaluation_key)?.join(format!(
            "{sequence:020}-{}.json",
            SafeSlug::new("commit", commit_id)?.as_str()
        )))
    }

    fn head_relative(&self, evaluation_key: &str) -> Result<PathBuf> {
        Ok(self
            .evaluation_root()?
            .join("views/outcome_heads")
            .join(format!(
                "{}.json",
                SafeSlug::new("eval", evaluation_key)?.as_str()
            )))
    }

    fn lock_relative(&self, kind: &str, identity: &str) -> Result<PathBuf> {
        Ok(self
            .evaluation_root()?
            .join(".locks")
            .join(kind)
            .join(format!(
                "{}.lock",
                SafeSlug::new("lock", identity)?.as_str()
            )))
    }
}

fn validate_context(context: &PersistenceContextV1) -> Result<()> {
    let canonical = matches!(context.namespace, PersistenceNamespace::Canonical);
    if canonical && !context.run_purpose.may_write_canonical_evaluation() {
        return Err(invalid(
            "persistence context",
            "only Paper or Live may select canonical namespace",
        ));
    }
    if matches!(context.run_purpose, RunPurpose::Debug)
        && !matches!(context.namespace, PersistenceNamespace::Debug { .. })
    {
        return Err(invalid(
            "persistence context",
            "Debug must use a debug namespace",
        ));
    }
    if matches!(context.run_purpose, RunPurpose::Replay)
        && !matches!(context.namespace, PersistenceNamespace::Replay { .. })
    {
        return Err(invalid(
            "persistence context",
            "Replay must use a replay namespace",
        ));
    }
    if matches!(context.run_purpose, RunPurpose::MigrationFixture)
        && !matches!(
            context.namespace,
            PersistenceNamespace::MigrationFixture { .. }
        )
    {
        return Err(invalid(
            "persistence context",
            "MigrationFixture must use a fixture namespace",
        ));
    }
    if matches!(context.run_purpose, RunPurpose::Mock)
        && !matches!(context.namespace, PersistenceNamespace::Disabled)
    {
        return Err(invalid(
            "persistence context",
            "Mock must disable evaluation persistence",
        ));
    }
    Ok(())
}

fn validate_decision(decision: &DecisionSnapshotV2, location: &RunLocation) -> Result<()> {
    if decision.schema_version != DECISION_SNAPSHOT_SCHEMA_VERSION
        || decision.source_run_id != location.run_id
        || decision.decision_id.trim().is_empty()
        || decision.ticker.trim().is_empty()
        || decision
            .evaluation_spec
            .evaluation_contract_id
            .trim()
            .is_empty()
        || decision.evaluation_spec.horizon_trading_days == 0
    {
        return Err(invalid(
            "decision snapshot",
            "schema, run identity, ticker, or evaluation spec is invalid",
        ));
    }
    if let orchestrator_core::MemoryUsageReferenceStatus::Available { document_ref } =
        &decision.memory_usage_ref
    {
        if document_ref.document_id.trim().is_empty()
            || document_ref.relative_path.trim().is_empty()
            || document_ref.content_hash.trim().is_empty()
            || !Path::new(&document_ref.relative_path).starts_with(
                location
                    .child_relative(Path::new("memory/usage"))
                    .map_err(|_| invalid("decision snapshot", "memory usage path is invalid"))?,
            )
        {
            return Err(invalid(
                "decision snapshot",
                "memory usage reference must point to this run's usage ledger",
            ));
        }
    }
    Ok(())
}

fn validate_evaluation_input_manifest(
    manifest: &EvaluationInputManifestV1,
    context: &PersistenceContextV1,
) -> Result<()> {
    if manifest.schema_version != EVALUATION_INPUT_MANIFEST_SCHEMA_VERSION
        || manifest.manifest_id.trim().is_empty()
        || manifest.run_purpose != context.run_purpose
        || manifest.source_store_fingerprint != context.source_store_fingerprint
        || manifest.created_at.trim().is_empty()
        || manifest.series.is_empty()
    {
        return Err(invalid(
            "evaluation input manifest",
            "schema, identity, timestamp, or market-data series is invalid",
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    let allowed_raw_directory = evaluation_root_for_context(context)?.join("inputs/raw");
    for series in &manifest.series {
        if series.schema_version != orchestrator_core::TECHNICAL_SERIES_PROVENANCE_SCHEMA_VERSION
            || series.ticker.trim().is_empty()
            || series.interval.trim().is_empty()
            || series.provider.trim().is_empty()
            || series.payload_hash.trim().is_empty()
            || series.input_ref.relative_path.trim().is_empty()
            || series.input_ref.content_hash != series.payload_hash
            || !Path::new(&series.input_ref.relative_path).starts_with(&allowed_raw_directory)
            || !seen.insert(format!(
                "{}\0{}\0{:?}",
                series.ticker, series.interval, series.price_basis
            ))
        {
            return Err(invalid(
                "evaluation input manifest",
                "series provenance is invalid or duplicates a ticker/interval/basis",
            ));
        }
    }
    Ok(())
}

fn validate_outcome(outcome: &OutcomeRecordV1) -> Result<()> {
    if outcome.schema_version != OUTCOME_RECORD_SCHEMA_VERSION
        || outcome.outcome_id.trim().is_empty()
        || outcome.evaluation_key.trim().is_empty()
        || outcome.ticker.trim().is_empty()
        || !matches!(
            outcome.market,
            orchestrator_core::OutcomeSection::Available { .. }
        )
        || !matches!(
            outcome.benchmark,
            orchestrator_core::OutcomeSection::Available { .. }
        )
    {
        return Err(invalid(
            "outcome record",
            "schema, identity, ticker, market, or benchmark is invalid",
        ));
    }
    Ok(())
}

fn validate_attribution(attribution: &MemoryAttributionRecordV1) -> Result<()> {
    if attribution.schema_version != MEMORY_ATTRIBUTION_SCHEMA_VERSION
        || attribution.attribution_id.trim().is_empty()
        || attribution.outcome_ref.document_id.trim().is_empty()
        || attribution.decision_ref.document_id.trim().is_empty()
        || attribution
            .memory_usage_report_ref
            .document_id
            .trim()
            .is_empty()
        || attribution.policy_ref.content_hash.trim().is_empty()
        || attribution.created_at.trim().is_empty()
        || attribution.items.iter().any(|item| {
            item.pattern_id.trim().is_empty()
                || item.reason.trim().is_empty()
                || item.usage_event_refs.is_empty()
        })
    {
        return Err(invalid(
            "memory attribution",
            "schema, provenance, policy, or attribution items are invalid",
        ));
    }
    Ok(())
}

fn create_or_validate<T>(
    store: &FileStore,
    relative: &Path,
    kind: FileSchemaKind,
    document: T,
) -> Result<(T, bool)>
where
    T: Versioned + ContentHashDocument + serde::de::DeserializeOwned + Clone,
{
    let sealed = crate::seal_content_hash(document.clone())?;
    if store.exists(relative)? {
        let existing: T = store.read_versioned_json(relative, kind)?;
        if existing.content_hash() != sealed.content_hash() {
            let existing_value = serde_json::to_value(&existing)
                .map_err(|source| StoreError::JsonSerialize { source })?;
            let sealed_value = serde_json::to_value(&sealed)
                .map_err(|source| StoreError::JsonSerialize { source })?;
            let changed = existing_value
                .as_object()
                .into_iter()
                .flat_map(|object| object.keys())
                .filter(|key| existing_value.get(*key) != sealed_value.get(*key))
                .cloned()
                .collect::<Vec<_>>();
            return Err(invalid(
                "evaluation document",
                format!("immutable identity already exists with different content in {changed:?}"),
            ));
        }
        return Ok((existing, true));
    }
    // `write_authoritative_json` performs the one authoritative sealing pass.
    // `sealed` above exists only for an immutable-existing-file comparison.
    Ok((store.write_authoritative_json(relative, document)?, false))
}

fn new_publish_commit(
    outcome: &OutcomeRecordV1,
    sequence: u64,
    previous_head_hash: Option<String>,
    reason: orchestrator_core::OutcomeRevisionReason,
) -> Result<OutcomeRevisionCommitV1> {
    let commit_id = content_hash(&json!({
        "evaluation_key": outcome.evaluation_key,
        "sequence": sequence,
        "outcome_id": outcome.outcome_id,
        "supersedes": outcome.supersedes_outcome_id,
        "previous_head_hash": previous_head_hash,
        "policy": outcome.materialization_policy_ref,
        "reason": reason,
    }))?;
    Ok(OutcomeRevisionCommitV1 {
        schema_version: OUTCOME_REVISION_COMMIT_SCHEMA_VERSION,
        commit_id,
        evaluation_key: outcome.evaluation_key.clone(),
        revision_sequence: sequence,
        operation: OutcomeRevisionOperation::PublishCurrent {
            outcome_id: outcome.outcome_id.clone(),
            supersedes_outcome_id: outcome.supersedes_outcome_id.clone(),
            reason,
        },
        previous_head_hash,
        policy_ref: outcome.materialization_policy_ref.clone(),
        created_at: outcome.created_at.clone(),
        content_hash: String::new(),
    })
}

fn empty_head(evaluation_key: &str) -> OutcomeHeadV1 {
    OutcomeHeadV1 {
        schema_version: OUTCOME_HEAD_SCHEMA_VERSION,
        evaluation_key: evaluation_key.to_owned(),
        current_outcome_id: None,
        statuses: BTreeMap::new(),
        as_of_revision: 0,
        revision_set_hash: String::new(),
        content_hash: String::new(),
    }
}

fn apply_commit(head: &mut OutcomeHeadV1, commit: &OutcomeRevisionCommitV1) -> Result<()> {
    if commit.evaluation_key != head.evaluation_key {
        return Err(invalid(
            "outcome revision",
            "commit evaluation key differs from head",
        ));
    }
    match &commit.operation {
        OutcomeRevisionOperation::PublishCurrent {
            outcome_id,
            supersedes_outcome_id,
            ..
        } => {
            if &head.current_outcome_id != supersedes_outcome_id {
                return Err(invalid(
                    "outcome revision",
                    "publish supersedes value differs from current head",
                ));
            }
            if let Some(old) = head.current_outcome_id.as_ref() {
                head.statuses.insert(old.clone(), OutcomeStatus::Superseded);
            }
            head.statuses
                .insert(outcome_id.clone(), OutcomeStatus::Current);
            head.current_outcome_id = Some(outcome_id.clone());
        }
        OutcomeRevisionOperation::Invalidate { outcome_id, .. } => {
            head.statuses
                .insert(outcome_id.clone(), OutcomeStatus::Invalidated);
            if head.current_outcome_id.as_deref() == Some(outcome_id) {
                head.current_outcome_id = None;
            }
        }
    }
    head.as_of_revision = commit.revision_sequence;
    // `content_hash` is reset before sealing the new derived head.
    head.content_hash.clear();
    Ok(())
}

fn revision_set_hash(head: &OutcomeHeadV1) -> Result<String> {
    content_hash(&json!({
        "evaluation_key": head.evaluation_key,
        "current_outcome_id": head.current_outcome_id,
        "statuses": head.statuses,
        "as_of_revision": head.as_of_revision,
    }))
}

fn optional_hash(hash: &str) -> Option<String> {
    (!hash.is_empty()).then(|| hash.to_owned())
}

fn namespace_relative(kind: &str, identity: &str, suffix: &Path) -> Result<PathBuf> {
    Ok(PathBuf::from("namespaces")
        .join(kind)
        .join(SafeSlug::new(kind, identity)?.as_str())
        .join(suffix))
}

fn evaluation_root_for_context(context: &PersistenceContextV1) -> Result<PathBuf> {
    match &context.namespace {
        PersistenceNamespace::Canonical => Ok(PathBuf::from(CANONICAL_ROOT)),
        PersistenceNamespace::Debug { invocation_id } => {
            namespace_relative("debug", invocation_id, Path::new("evaluation"))
        }
        PersistenceNamespace::Replay { replay_id } => {
            namespace_relative("replay", replay_id, Path::new("evaluation"))
        }
        PersistenceNamespace::MigrationFixture { fixture_id } => {
            namespace_relative("fixture", fixture_id, Path::new("evaluation"))
        }
        PersistenceNamespace::Disabled => Err(invalid(
            "evaluation writer",
            "disabled namespace has no evaluation root",
        )),
    }
}

fn document_ref(document_id: &str, relative: &Path, content_hash: &str) -> DocumentRef {
    DocumentRef {
        document_id: document_id.to_owned(),
        relative_path: relative.to_string_lossy().to_string(),
        content_hash: content_hash.to_owned(),
    }
}

fn invalid(kind: &'static str, message: impl Into<String>) -> StoreError {
    StoreError::InvalidDocument {
        kind,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FileStoreOptions;
    use orchestrator_core::{
        AdjustmentPolicy, BenchmarkOutcome, MarketOutcome, OutcomeSection, PriceBasis, PricePoint,
    };
    use orchestrator_core::{OutcomeSectionUnavailableReason, PolicyRef};
    use tempfile::tempdir;

    fn policy() -> PolicyRef {
        PolicyRef {
            policy_id: "policy".into(),
            version: 1,
            content_hash: "sha256:policy".into(),
        }
    }

    fn context() -> PersistenceContextV1 {
        PersistenceContextV1 {
            run_purpose: RunPurpose::Paper,
            namespace: PersistenceNamespace::Canonical,
            canonical_memory_writes_enabled: true,
            invocation_id: "test".into(),
            config_ref: policy(),
            source_store_fingerprint: "fixture".into(),
        }
    }

    fn reference(id: &str) -> DocumentRef {
        DocumentRef {
            document_id: id.into(),
            relative_path: format!("fixtures/{id}.json"),
            content_hash: format!("sha256:{id}"),
        }
    }

    fn outcome(id: &str, supersedes: Option<&str>) -> OutcomeRecordV1 {
        let p = policy();
        OutcomeRecordV1 {
            schema_version: OUTCOME_RECORD_SCHEMA_VERSION,
            outcome_id: id.into(),
            evaluation_key: "evaluation".into(),
            supersedes_outcome_id: supersedes.map(str::to_owned),
            decision_ref: reference("decision"),
            ticker: "QQQ".into(),
            market: OutcomeSection::Available {
                value: MarketOutcome {
                    provider: "fixture".into(),
                    price_basis: PriceBasis::AdjustedClose,
                    adjustment_policy: AdjustmentPolicy::All,
                    anchor: PricePoint {
                        session: "2026-01-01".into(),
                        price: 100.0,
                        source_ref: reference("anchor"),
                    },
                    exit: PricePoint {
                        session: "2026-01-06".into(),
                        price: 110.0,
                        source_ref: reference("exit"),
                    },
                    asset_return: 0.1,
                    max_adverse_excursion: 0.0,
                    corporate_action_resolved: true,
                },
            },
            benchmark: OutcomeSection::Available {
                value: BenchmarkOutcome {
                    benchmark_id: "SPY".into(),
                    benchmark_policy_ref: p.clone(),
                    provider: "fixture".into(),
                    price_basis: PriceBasis::AdjustedClose,
                    anchor: PricePoint {
                        session: "2026-01-01".into(),
                        price: 100.0,
                        source_ref: reference("banchor"),
                    },
                    exit: PricePoint {
                        session: "2026-01-06".into(),
                        price: 105.0,
                        source_ref: reference("bexit"),
                    },
                    benchmark_return: 0.05,
                    excess_return: 0.05,
                },
            },
            allocation: OutcomeSection::Unavailable {
                reason: OutcomeSectionUnavailableReason::DeferredToLaterMilestone,
            },
            execution: OutcomeSection::Unavailable {
                reason: OutcomeSectionUnavailableReason::NoReliableOrderFillMapping,
            },
            evaluation_input_manifest_ref: reference("inputs"),
            materialization_policy_ref: p.clone(),
            benchmark_policy_ref: p,
            materializer_version: 1,
            created_at: "2026-01-07T00:00:00Z".into(),
            content_hash: String::new(),
        }
    }

    #[test]
    fn identical_outcome_is_global_and_idempotent_across_runs() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), FileStoreOptions::default()).unwrap();
        let evaluation = EvaluationStore::open(store, context()).unwrap();
        let first = RunLocation::new("2026-01-07", "first").unwrap();
        let second = RunLocation::new("2026-01-08", "second").unwrap();
        let one = evaluation
            .publish_outcome(
                &first,
                outcome("outcome-one", None),
                orchestrator_core::OutcomeRevisionReason::InitialMaterialization,
            )
            .unwrap();
        let two = evaluation
            .publish_outcome(
                &second,
                outcome("outcome-one", None),
                orchestrator_core::OutcomeRevisionReason::InitialMaterialization,
            )
            .unwrap();
        assert_eq!(one.result, OutcomeWriteResultKind::Created);
        assert_eq!(two.result, OutcomeWriteResultKind::AlreadyCurrent);
        assert_eq!(
            evaluation
                .read_current_outcome("evaluation")
                .unwrap()
                .unwrap()
                .outcome_id,
            "outcome-one"
        );
    }

    #[test]
    fn revision_supersedes_the_old_current_outcome() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), FileStoreOptions::default()).unwrap();
        let evaluation = EvaluationStore::open(store, context()).unwrap();
        let run = RunLocation::new("2026-01-07", "run").unwrap();
        evaluation
            .publish_outcome(
                &run,
                outcome("one", None),
                orchestrator_core::OutcomeRevisionReason::InitialMaterialization,
            )
            .unwrap();
        evaluation
            .publish_outcome(
                &run,
                outcome("two", Some("one")),
                orchestrator_core::OutcomeRevisionReason::MarketDataRevision,
            )
            .unwrap();
        let current = evaluation
            .read_current_outcome("evaluation")
            .unwrap()
            .unwrap();
        assert_eq!(current.outcome_id, "two");
        let visible = evaluation.list_current_outcomes().unwrap();
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].outcome_id, "two");
    }

    #[test]
    fn debug_context_cannot_open_canonical_writer() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), FileStoreOptions::default()).unwrap();
        let mut context = context();
        context.run_purpose = RunPurpose::Debug;
        assert!(EvaluationStore::open(store, context).is_err());
    }

    #[test]
    fn debug_outcomes_are_isolated_from_canonical_knowledge() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), FileStoreOptions::default()).unwrap();
        let mut debug_context = context();
        debug_context.run_purpose = RunPurpose::Debug;
        debug_context.namespace = PersistenceNamespace::Debug {
            invocation_id: "debug-invocation".into(),
        };
        debug_context.canonical_memory_writes_enabled = false;
        let evaluation = EvaluationStore::open(store.clone(), debug_context).unwrap();
        let run = RunLocation::new("2026-01-07", "debug-run").unwrap();

        evaluation
            .publish_outcome(
                &run,
                outcome("debug-outcome", None),
                orchestrator_core::OutcomeRevisionReason::InitialMaterialization,
            )
            .unwrap();

        assert!(!store
            .exists(Path::new("knowledge/evaluation/outcomes"))
            .unwrap());
        assert!(store
            .exists(&evaluation.outcome_relative("debug-outcome").unwrap())
            .unwrap());
    }

    #[test]
    fn same_outcome_id_with_different_content_fails_closed_even_when_current() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), FileStoreOptions::default()).unwrap();
        let evaluation = EvaluationStore::open(store, context()).unwrap();
        let run = RunLocation::new("2026-01-07", "run").unwrap();
        evaluation
            .publish_outcome(
                &run,
                outcome("one", None),
                orchestrator_core::OutcomeRevisionReason::InitialMaterialization,
            )
            .unwrap();
        let mut conflicting = outcome("one", None);
        conflicting.created_at = "2026-01-08T00:00:00Z".into();
        assert!(evaluation
            .publish_outcome(
                &run,
                conflicting,
                orchestrator_core::OutcomeRevisionReason::InitialMaterialization,
            )
            .is_err());
    }

    #[test]
    fn head_rebuild_preserves_revision_chain() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), FileStoreOptions::default()).unwrap();
        let evaluation = EvaluationStore::open(store.clone(), context()).unwrap();
        let run = RunLocation::new("2026-01-07", "run").unwrap();
        evaluation
            .publish_outcome(
                &run,
                outcome("one", None),
                orchestrator_core::OutcomeRevisionReason::InitialMaterialization,
            )
            .unwrap();
        evaluation
            .publish_outcome(
                &run,
                outcome("two", Some("one")),
                orchestrator_core::OutcomeRevisionReason::MarketDataRevision,
            )
            .unwrap();

        std::fs::remove_file(
            store
                .root()
                .join(evaluation.head_relative("evaluation").unwrap()),
        )
        .unwrap();
        let rebuilt = evaluation.rebuild_outcome_head("evaluation").unwrap();
        assert_eq!(rebuilt.current_outcome_id.as_deref(), Some("two"));
        assert_eq!(
            rebuilt.statuses.get("one"),
            Some(&OutcomeStatus::Superseded)
        );
    }

    #[test]
    fn attribution_is_global_and_idempotent_per_outcome() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), FileStoreOptions::default()).unwrap();
        let evaluation = EvaluationStore::open(store, context()).unwrap();
        let attribution = orchestrator_core::MemoryAttributionRecordV1 {
            schema_version: orchestrator_core::MEMORY_ATTRIBUTION_SCHEMA_VERSION,
            attribution_id: "attribution".into(),
            outcome_ref: reference("outcome"),
            decision_ref: reference("decision"),
            memory_usage_report_ref: reference("memory-usage"),
            policy_ref: policy(),
            items: vec![orchestrator_core::MemoryAttributionItemV1 {
                pattern_id: "pattern".into(),
                label: orchestrator_core::MemoryAttributionLabel::Unverifiable,
                reason: "no counterfactual".into(),
                usage_event_refs: vec![reference("memory-usage")],
            }],
            created_at: "2026-01-01T00:00:00Z".into(),
            content_hash: String::new(),
        };
        evaluation
            .write_memory_attribution(attribution.clone())
            .unwrap();
        evaluation.write_memory_attribution(attribution).unwrap();
        assert!(evaluation
            .read_memory_attribution("outcome")
            .unwrap()
            .is_some());
    }
}
