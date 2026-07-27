//! Concrete, crash-recoverable file storage primitives for orchestrator runs.
//!
//! This crate deliberately exposes a single file-store implementation.  It is
//! not a backend abstraction: callers receive path validation, canonical JSON,
//! atomic replacement, and JSONL recovery in one place.

mod atomic;
mod draft;
mod error;
mod index;
mod input;
mod json;
mod jsonl;
mod manifest;
mod paths;
mod recovery;
mod schema;
mod session;
mod store;

pub use atomic::{
    rename_dir_atomic, write_bytes_atomic, write_bytes_atomic_with_options, write_json_atomic,
    AtomicWriteOptions,
};
pub use draft::{
    append_draft_receipt, create_or_recover_draft, draft_relative, fail_draft,
    finalize_draft_atomic, AnalystReportDraft, ArtifactDraft, ArtifactDraftState, ArtifactScope,
    DebateResponseDraft, DebateSeedDraft, DraftAppendOutcome, DraftFailure, DraftIdempotencyKey,
    DraftLifecycle, DraftProfile, DraftWriteReceipt, FinalizableArtifact, FinalizeDraftOutcome,
    HistoricalReflectionDraft, PhaseSummaryDraft, PortfolioDecisionDraft, ProfileDraftMetadata,
    ResearchDecisionDraft, ResearcherWarmupDraft, RiskReviewDraft, TopicControlDraft,
    TopicGenerationDraft, TradeIntentDraft,
};
pub use error::{Result, StoreError};
pub use index::{
    append_index_detail, create_index, deterministic_experience_index_id, finalize_index,
    read_index_details, read_indexes, AppendIndexDetailInput, CreateIndexInput, DetailPage,
    DetailQuery, DetailSection, Index, IndexDetail, IndexKind, IndexPage, IndexQuery, IndexScope,
    INDEX_DETAIL_SCHEMA_VERSION, INDEX_SCHEMA_VERSION,
};
pub use input::{
    capture_run_inputs, read_input_metadata, read_input_payload, read_input_snapshot_manifest,
    read_snapshotted_input, write_input_payload, DataFileMetadata, InputKind, InputSnapshot,
    InputSnapshotManifest, InputSource, Jin10Format, DATA_FILE_METADATA_SCHEMA_VERSION,
    INPUT_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
};
pub use json::{
    canonical_json_bytes, content_hash, content_hash_bytes, seal_content_hash, set_content_hash,
    validate_content_hash, validate_content_hash_at, ContentHashDocument,
};
pub use jsonl::{append_jsonl_locked, read_jsonl_recover_tail, JsonlEvent, JsonlRecord};
pub use manifest::{
    read_run_manifest, write_run_manifest, FinalizedArtifactRef, ManifestError, PhaseStatus,
    RunLocation, RunManifest, RunManifestInit, RunStatus, RUN_MANIFEST_SCHEMA_VERSION,
};
pub use paths::{validate_relative_path, SafeSlug};
pub use recovery::{rebuild_manifest_from_finalized_artifacts, recover_pending_finalization};
pub use schema::{FileSchemaKind, Versioned};
pub use session::{
    append_session_event, read_session_events, read_session_manifest, write_session_manifest,
    EvidenceReadEvent, ForkReference, SessionEvent, SessionEventInput, SessionEventType,
    SessionLocation, SessionManifest, SessionStatus, VisibleEvidenceSet,
};
pub use store::{read_bytes, read_json, FileStore, FileStoreOptions};
