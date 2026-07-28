//! Concrete, crash-recoverable file storage primitives for orchestrator runs.
//!
//! This crate deliberately exposes a single file-store implementation.  It is
//! not a backend abstraction: callers receive path validation, canonical JSON,
//! atomic replacement, and JSONL recovery in one place.

mod atomic;
mod doctor;
mod domain;
mod draft;
mod error;
mod index;
mod input;
mod json;
mod jsonl;
mod learning;
mod manifest;
mod paths;
mod phase2;
mod recovery;
mod schema;
mod session;
mod store;

pub use orchestrator_core::ToolManagedProfile;

pub use atomic::{
    publish_bytes_atomic, rename_dir_atomic, write_bytes_atomic, write_bytes_atomic_with_options,
    write_json_atomic, AtomicWriteOptions,
};
pub use doctor::{
    inspect_store, rebuild_experience_stats, rebuild_index_catalog, rebuild_run_manifest,
    DoctorIssue, ExperienceStat, ExperienceStats, IndexCatalog, IndexCatalogEntry,
    StoreDoctorReport, EXPERIENCE_STATS_SCHEMA_VERSION, INDEX_CATALOG_SCHEMA_VERSION,
};
pub use domain::{
    append_analyst_data_gap, append_analyst_evidence, append_binding_risk_control,
    append_research_hinge, append_trade_blocker, finalize_analyst_report,
    finalize_portfolio_decision, finalize_research_decision, finalize_risk_review,
    finalize_trade_intent, set_analyst_assessment, set_analyst_invalidation,
    set_portfolio_asset_decision, set_research_decision, set_research_scenarios,
    set_risk_assessment, set_risk_constraints, set_trade_intent, AnalystArtifact,
    AnalystAssessmentInput, AnalystEvidenceInput, CanonicalArtifact, DomainFinalizeOutcome,
    PortfolioAssetDecisionInput, PortfolioDecisionArtifact, PortfolioDecisionFinalizePolicy,
    ResearchDecisionArtifact, ResearchDecisionInput, ResearchDecisionPayload,
    ResearchScenarioInput, RiskAssessmentInput, RiskConstraintsInput, RiskReviewArtifact,
    TradeIntentArtifact, TradeIntentFinalizePolicy, TradeIntentInput,
    DOMAIN_ARTIFACT_SCHEMA_VERSION,
};
pub use draft::{
    append_draft_receipt, apply_typed_draft_command, complete_terminal_draft_without_artifact,
    create_or_recover_draft, draft_relative, fail_draft, finalize_draft_atomic,
    read_draft_for_scope, AnalystAssessmentDraft, AnalystReportDraft, ArtifactDraft,
    ArtifactDraftState, ArtifactScope, DebateClaimDraft, DebateResponseDraft,
    DebateResponseDraftEntry, DebateSeedDraft, DraftAppendOutcome, DraftFailure,
    DraftIdempotencyKey, DraftLifecycle, DraftWriteReceipt, FinalizableArtifact,
    FinalizeDraftOutcome, HistoricalReflectionDraft, Phase2TopicDraft, PhaseSummaryDraft,
    PortfolioAssetDecisionDraft, PortfolioDecisionDraft, ProfileDraftMetadata,
    ResearchDecisionDraft, ResearchDecisionDraftEntry, ResearcherWarmupDraft, RiskAssessmentDraft,
    RiskConstraintDraft, RiskReviewDraft, TopicControlDraft, TopicGenerationDraft,
    TradeIntentDraft, TradeIntentDraftEntry,
};
pub use error::{Result, StoreError};
pub use index::{
    append_index_detail, create_index, deterministic_experience_index_id, experience_level,
    finalize_index, read_index_details, read_indexes, record_experience_case,
    AppendIndexDetailInput, CreateIndexInput, DetailPage, DetailQuery, DetailSection,
    ExperienceCaseDisposition, ExperienceLevel, Index, IndexDetail, IndexKind, IndexPage,
    IndexQuery, IndexScope, RecordExperienceCaseInput, RecordExperienceCaseOutcome,
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
pub use learning::{
    learning_record_relative, read_learning_record, write_learning_record, LearningKind,
    LearningRecord, LEARNING_RECORD_SCHEMA_VERSION,
};
pub use manifest::{
    find_run_location, list_run_locations, read_run_manifest, write_run_manifest,
    FinalizedArtifactRef, ManifestError, PhaseStatus, RunLocation, RunManifest, RunManifestInit,
    RunStatus, RUN_MANIFEST_SCHEMA_VERSION,
};
pub use paths::{validate_relative_path, SafeSlug};
pub use phase2::{
    ClaimStatus, ClaimStatusEntry, Phase2Artifact, Phase2ArtifactPayload, Phase2DraftService,
    SteerRoute, PHASE2_ARTIFACT_SCHEMA_VERSION,
};
pub use recovery::{rebuild_manifest_from_finalized_artifacts, recover_pending_finalization};
pub use schema::{FileSchemaKind, Versioned};
pub use session::{
    append_session_event, read_session_events, read_session_manifest, write_session_manifest,
    EvidenceReadEvent, ForkReference, SessionEvent, SessionEventInput, SessionEventType,
    SessionLocation, SessionManifest, SessionStatus, VisibleEvidenceSet,
};
pub use store::{read_bytes, read_json, FileStore, FileStoreOptions};
