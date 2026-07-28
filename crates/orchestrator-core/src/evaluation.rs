//! Typed, provenance-first records for deterministic decision evaluation.
//!
//! This module intentionally contains no filesystem or provider access.  The
//! Store owns persistence and the workflow owns materialization; both share
//! these strict documents so a run cannot collapse research, trading, risk,
//! allocation, and execution into one ambiguous decision payload.

use serde::{Deserialize, Serialize};

pub const DECISION_SNAPSHOT_SCHEMA_VERSION: u32 = 2;
pub const OUTCOME_RECORD_SCHEMA_VERSION: u32 = 1;
pub const OUTCOME_REVISION_COMMIT_SCHEMA_VERSION: u32 = 1;
pub const OUTCOME_HEAD_SCHEMA_VERSION: u32 = 1;
pub const OUTCOME_WRITE_RECEIPT_SCHEMA_VERSION: u32 = 1;
pub const MATERIALIZATION_GAP_SCHEMA_VERSION: u32 = 1;
pub const MATERIALIZATION_INTEGRITY_ISSUE_SCHEMA_VERSION: u32 = 1;
pub const MATERIALIZATION_BATCH_REPORT_SCHEMA_VERSION: u32 = 1;
pub const EVALUATION_INPUT_MANIFEST_SCHEMA_VERSION: u32 = 1;
pub const TECHNICAL_SERIES_PROVENANCE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DocumentRef {
    pub document_id: String,
    pub relative_path: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyRef {
    pub policy_id: String,
    pub version: u32,
    pub content_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunPurpose {
    Live,
    Paper,
    Debug,
    Mock,
    Replay,
    MigrationFixture,
}

impl RunPurpose {
    pub fn may_write_canonical_evaluation(self) -> bool {
        matches!(self, Self::Live | Self::Paper)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PersistenceNamespace {
    Canonical,
    Debug { invocation_id: String },
    Replay { replay_id: String },
    MigrationFixture { fixture_id: String },
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistenceContextV1 {
    pub run_purpose: RunPurpose,
    pub namespace: PersistenceNamespace,
    pub canonical_memory_writes_enabled: bool,
    pub invocation_id: String,
    pub config_ref: PolicyRef,
    pub source_store_fingerprint: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PriceBasis {
    Close,
    AdjustedClose,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdjustmentPolicy {
    None,
    Splits,
    Dividends,
    All,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CorporateActionCapability {
    ProviderAdjusted,
    ExternalMetadata,
    Unsupported,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TechnicalSeriesProvenanceV1 {
    pub schema_version: u32,
    pub ticker: String,
    pub interval: String,
    pub provider: String,
    pub feed: Option<String>,
    pub price_basis: PriceBasis,
    pub adjustment_policy: AdjustmentPolicy,
    pub corporate_action_capability: CorporateActionCapability,
    /// Immutable FileStore input snapshot or a sealed provider-export record.
    /// This is Rust-owned and never supplied by an agent tool argument.
    pub input_ref: DocumentRef,
    pub payload_hash: String,
    pub coverage_start: String,
    pub coverage_end: String,
    pub created_at: String,
    pub content_hash: String,
}

/// The exact market-data set used by one materialization batch.  Outcomes
/// point here rather than at mutable technical CSV paths so a later provider
/// revision is explicit and auditable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationInputManifestV1 {
    pub schema_version: u32,
    pub manifest_id: String,
    pub run_purpose: RunPurpose,
    pub source_store_fingerprint: String,
    pub series: Vec<TechnicalSeriesProvenanceV1>,
    pub materialization_policy_ref: PolicyRef,
    pub created_at: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionSectionUnavailableReason {
    ArtifactMissing,
    ArtifactNotFinalized,
    ArtifactValidationFailed,
    UpstreamDataGap,
    UnsupportedDecisionKind,
    AccountSnapshotUnavailable,
    ExecutionMappingUnavailable,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum DecisionSection<T> {
    Available {
        value: T,
    },
    Unavailable {
        reason: DecisionSectionUnavailableReason,
        source_refs: Vec<DocumentRef>,
    },
    NotApplicable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForecastDirection {
    Up,
    Down,
    Neutral,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeAction {
    Buy,
    Sell,
    Hold,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionPlanStatus {
    Execute,
    Wait,
    Downgrade,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThesisDecision {
    pub artifact_ref: DocumentRef,
    pub direction: ForecastDirection,
    pub probability: f64,
    pub horizon: String,
    pub invalidation_conditions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeDecision {
    pub artifact_ref: DocumentRef,
    pub action: TradeAction,
    pub entry_condition: Option<String>,
    pub position_size_ceiling: Option<f64>,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskDecision {
    pub artifact_refs: Vec<DocumentRef>,
    pub direction_constraint: String,
    pub max_target_weight: Option<f64>,
    pub max_weight_delta: Option<f64>,
    pub binding_controls: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllocationDecision {
    pub artifact_ref: DocumentRef,
    pub current_weight: Option<f64>,
    pub target_weight: Option<f64>,
    pub cash_weight: Option<f64>,
    pub allocation_policy_version: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPlan {
    pub status: ExecutionPlanStatus,
    pub intended_action: TradeAction,
    pub order_intent_refs: Vec<DocumentRef>,
    pub attributable_execution_expected: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkBindingV1 {
    pub benchmark_id: String,
    pub provider: String,
    pub price_basis: PriceBasis,
    pub policy_ref: PolicyRef,
}

/// A Decision captures the benchmark policy actually in force at decision
/// time. A later config edit cannot retroactively select a benchmark for an
/// old Decision; operators must create an explicit policy revision instead.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum BenchmarkSelectionV1 {
    Configured { binding: BenchmarkBindingV1 },
    Missing { policy_ref: PolicyRef },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationSpec {
    pub evaluation_contract_id: String,
    pub horizon_trading_days: u32,
    pub benchmark_policy_ref: PolicyRef,
    pub benchmark_selection: BenchmarkSelectionV1,
    pub price_basis: PriceBasis,
    pub materialization_policy_ref: PolicyRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryUsageReferenceStatus {
    NotCaptured,
    Available { document_ref: DocumentRef },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionSnapshotV2 {
    pub schema_version: u32,
    pub decision_id: String,
    pub source_run_id: String,
    pub ticker: String,
    pub thesis: DecisionSection<ThesisDecision>,
    pub trade: DecisionSection<TradeDecision>,
    pub risk: DecisionSection<RiskDecision>,
    pub allocation: DecisionSection<AllocationDecision>,
    pub execution_plan: DecisionSection<ExecutionPlan>,
    pub evaluation_spec: EvaluationSpec,
    pub source_artifact_refs: Vec<DocumentRef>,
    pub source_input_refs: Vec<DocumentRef>,
    pub memory_usage_ref: MemoryUsageReferenceStatus,
    pub run_purpose: RunPurpose,
    pub decided_at: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeSectionUnavailableReason {
    DeferredToLaterMilestone,
    NoReliableOrderFillMapping,
    DecisionSectionUnavailable,
    DataIncomplete,
    NotApplicable,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum OutcomeSection<T> {
    Available {
        value: T,
    },
    Unavailable {
        reason: OutcomeSectionUnavailableReason,
    },
    NotApplicable,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PricePoint {
    pub session: String,
    pub price: f64,
    pub source_ref: DocumentRef,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketOutcome {
    pub provider: String,
    pub price_basis: PriceBasis,
    pub adjustment_policy: AdjustmentPolicy,
    pub anchor: PricePoint,
    pub exit: PricePoint,
    pub asset_return: f64,
    pub max_adverse_excursion: f64,
    pub corporate_action_resolved: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkOutcome {
    pub benchmark_id: String,
    pub benchmark_policy_ref: PolicyRef,
    pub provider: String,
    pub price_basis: PriceBasis,
    pub anchor: PricePoint,
    pub exit: PricePoint,
    pub benchmark_return: f64,
    pub excess_return: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllocationOutcome {
    pub target_weight: f64,
    pub current_weight: f64,
    pub counterfactual_contribution: Option<f64>,
    pub allocation_policy_ref: PolicyRef,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ExecutionOutcome {
    Attributed {
        order_refs: Vec<DocumentRef>,
        executed_price: f64,
        executed_quantity: f64,
        realized_pnl: Option<f64>,
    },
    Unavailable {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutcomeRecordV1 {
    pub schema_version: u32,
    pub outcome_id: String,
    pub evaluation_key: String,
    pub supersedes_outcome_id: Option<String>,
    pub decision_ref: DocumentRef,
    pub ticker: String,
    pub market: OutcomeSection<MarketOutcome>,
    pub benchmark: OutcomeSection<BenchmarkOutcome>,
    pub allocation: OutcomeSection<AllocationOutcome>,
    pub execution: OutcomeSection<ExecutionOutcome>,
    pub evaluation_input_manifest_ref: DocumentRef,
    pub materialization_policy_ref: PolicyRef,
    pub benchmark_policy_ref: PolicyRef,
    pub materializer_version: u32,
    pub created_at: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeStatus {
    Current,
    Superseded,
    Invalidated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeRevisionReason {
    InitialMaterialization,
    MarketDataRevision,
    BenchmarkPolicyRevision,
    MaterializationPolicyRevision,
    PriceBasisRevision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeInvalidationReason {
    SourceDataCorrupted,
    ProvenanceInvalid,
    CorporateActionReclassified,
    ManualPolicyInvalidation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OutcomeRevisionOperation {
    PublishCurrent {
        outcome_id: String,
        supersedes_outcome_id: Option<String>,
        reason: OutcomeRevisionReason,
    },
    Invalidate {
        outcome_id: String,
        reason: OutcomeInvalidationReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutcomeRevisionCommitV1 {
    pub schema_version: u32,
    pub commit_id: String,
    pub evaluation_key: String,
    pub revision_sequence: u64,
    pub operation: OutcomeRevisionOperation,
    pub previous_head_hash: Option<String>,
    pub policy_ref: PolicyRef,
    pub created_at: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutcomeHeadV1 {
    pub schema_version: u32,
    pub evaluation_key: String,
    pub current_outcome_id: Option<String>,
    pub statuses: std::collections::BTreeMap<String, OutcomeStatus>,
    pub as_of_revision: u64,
    pub revision_set_hash: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeWriteResultKind {
    Created,
    AlreadyPresent,
    AlreadyCurrent,
    PublishedRevision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutcomeWriteReceiptV1 {
    pub schema_version: u32,
    pub receipt_id: String,
    pub evaluation_run_id: String,
    pub outcome_id: String,
    pub evaluation_key: String,
    pub result: OutcomeWriteResultKind,
    pub outcome_ref: DocumentRef,
    pub revision_commit_ref: Option<DocumentRef>,
    pub created_at: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterializationGapReason {
    NotMatured,
    MarketDataUnavailable,
    MissingBenchmark,
    CorporateActionUnresolved,
    DataIncomplete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializationGapV1 {
    pub schema_version: u32,
    pub gap_id: String,
    pub evaluation_key: String,
    pub decision_ref: DocumentRef,
    pub reason: MaterializationGapReason,
    pub detail: String,
    pub policy_ref: PolicyRef,
    pub created_at: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterializationIntegrityFailureKind {
    LedgerCorruption,
    HashMismatch,
    UnknownSchema,
    PathEscape,
    ProvenanceViolation,
    OutcomeIdCollision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializationIntegrityIssueV1 {
    pub schema_version: u32,
    pub issue_id: String,
    pub evaluation_key: Option<String>,
    pub decision_ref: Option<DocumentRef>,
    pub kind: MaterializationIntegrityFailureKind,
    pub detail: String,
    pub created_at: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterializationResultKind {
    Materialized,
    Gap,
    IntegrityFailure,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializationResultV1 {
    pub decision_id: String,
    pub evaluation_key: Option<String>,
    pub kind: MaterializationResultKind,
    pub document_ref: Option<DocumentRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializationBatchReportV1 {
    pub schema_version: u32,
    pub batch_id: String,
    pub evaluation_run_id: String,
    pub run_purpose: RunPurpose,
    pub results: Vec<MaterializationResultV1>,
    pub created_at: String,
    pub content_hash: String,
}
