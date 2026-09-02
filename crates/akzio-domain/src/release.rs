use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    content_hash_json, ArtifactRef, CanaryCampaignStatus, ContentHash, DomainError, OutcomeHorizon,
    PolicyState, ResearchApprovalDecision, ResearchApprovalReason, RunId, RunPurpose,
    WorkflowGraph, DOMAIN_SCHEMA_VERSION,
};

pub const RELEASE_EVIDENCE_BUNDLE_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseEvidenceEnvironment {
    OfflineFixture,
    Real,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseEvidenceStatus {
    Approvable,
    Incomplete,
    NotApprovable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseEvidenceTier {
    E0Fixture,
    E1HistoricalReplay,
    E2PaperMechanics,
    E3ForwardPaper,
    E4Robustness,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseBrokerEvidenceTrust {
    OfflineFixture,
    RealBroker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseHumanApprovalStatus {
    Approved,
    Pending,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum ReleaseEvidenceIssue {
    NonCanonicalRun { purpose: String },
    DirtyWorktree,
    OfflineFixture,
    FakeBrokerEvidence,
    MissingRuntimeManifest,
    MissingWorkflow,
    MissingContracts,
    MissingProviderCapability,
    MissingSourceSnapshots,
    MissingBrokerEvidence,
    MissingSessionSlot,
    MissingDaemonLease,
    MissingExecution,
    MissingReconciliation,
    MissingOutcome { horizon: OutcomeHorizon },
    MissingLearningTransition,
    MissingHumanApproval,
    HumanApprovalNotApproved,
    ConfigHashDrift,
    WorkflowHashDrift,
    BrokerAccountMismatch,
    StaleDaemonEpoch,
    EngineeringOnlyProfile { profile: String },
    UnsupportedResearchFeed { feed: String },
    MissingModelKnowledgeCutoff,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseRuntimeEvidence {
    pub repository_commit: String,
    pub dirty_worktree: bool,
    pub cargo_lock_hash: ContentHash,
    pub config_hash: ContentHash,
    pub prompt_hash: ContentHash,
    pub contract_hash: ContentHash,
    pub topology_hash: ContentHash,
    pub decision_policy_hash: ContentHash,
    pub execution_policy_hash: ContentHash,
    pub evaluation_policy_hash: ContentHash,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behavior_bundle_hash: Option<ContentHash>,
    pub experiment_profile: String,
    pub rust_toolchain: String,
    pub model_knowledge_cutoff: Option<NaiveDate>,
    pub market_data_feed: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseWorkflowEvidence {
    pub graph: ArtifactRef,
    pub workflow_hash: ContentHash,
    pub plan: WorkflowGraph,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseContractEvidence {
    pub contract_hashes: BTreeSet<ContentHash>,
    pub tool_set_hashes: BTreeSet<ContentHash>,
    pub context_manifest_hashes: BTreeSet<ContentHash>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ReleaseProviderRouteEvidence {
    pub provider_id: String,
    pub model_id: String,
    pub reasoning_effort: Option<String>,
    pub capability_snapshot_hash: ContentHash,
    pub supports_tool_calls: bool,
    pub supports_stateless_continuation: bool,
    pub native_web_tool: bool,
    pub streaming: Option<bool>,
    pub declared_context_limit: Option<u32>,
    pub declared_max_output_tokens: Option<u32>,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ReleaseSourceSnapshotEvidence {
    pub artifact: ArtifactRef,
    pub blob_hash: ContentHash,
    pub source_family: String,
    pub observed_at: Option<DateTime<Utc>>,
    pub retrieved_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ReleaseOrderIdentity {
    pub client_order_id: String,
    pub broker_order_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseBrokerEvidence {
    pub account_fingerprint: ContentHash,
    pub trust: ReleaseBrokerEvidenceTrust,
    pub orders: Vec<ReleaseOrderIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseSessionEvidence {
    pub session_key: String,
    pub scheduler_epoch: u64,
    pub reserved_at: DateTime<Utc>,
    pub committed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseDaemonEvidence {
    pub lease_name: String,
    pub owner_id: String,
    pub epoch: u64,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseExecutionEvidence {
    pub execution_plan: ArtifactRef,
    pub plan_hash: ContentHash,
    pub commitment: ArtifactRef,
    pub commitment_id: String,
    pub reconciliation: Option<ArtifactRef>,
    pub reconciliation_receipts: Vec<ArtifactRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseOutcomeEvidence {
    pub outcome: ArtifactRef,
    pub sealed_at: DateTime<Utc>,
    pub observed_on: NaiveDate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseLearningEvidence {
    pub transition_id: String,
    pub from: PolicyState,
    pub to: PolicyState,
    pub evaluation: ArtifactRef,
    pub transitioned_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseCanaryEvidence {
    pub campaign_id: ContentHash,
    pub status: CanaryCampaignStatus,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleasePostOutcomeApprovalEvidence {
    pub approval_id: ContentHash,
    pub decision: ResearchApprovalDecision,
    pub operator_identity: String,
    pub approved_at: DateTime<Utc>,
    pub reason_codes: BTreeSet<ResearchApprovalReason>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseHumanApprovalEvidence {
    pub status: ReleaseHumanApprovalStatus,
    pub operator_identity: String,
    pub approved_at: Option<DateTime<Utc>>,
    pub approval_hash: ContentHash,
    #[serde(default)]
    pub is_post_outcome_research_approval: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseIntegrityEvidence {
    pub config_hash_matches: bool,
    pub workflow_hash_matches: bool,
    pub broker_account_matches: bool,
    pub daemon_epoch_current: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseEvidenceBody {
    pub run_id: RunId,
    pub purpose: RunPurpose,
    pub environment: ReleaseEvidenceEnvironment,
    pub materialized_at: DateTime<Utc>,
    pub runtime: Option<ReleaseRuntimeEvidence>,
    pub workflow: Option<ReleaseWorkflowEvidence>,
    pub contracts: ReleaseContractEvidence,
    pub provider_routes: BTreeSet<ReleaseProviderRouteEvidence>,
    pub source_snapshots: BTreeSet<ReleaseSourceSnapshotEvidence>,
    pub broker: Option<ReleaseBrokerEvidence>,
    pub session: Option<ReleaseSessionEvidence>,
    pub daemon: Option<ReleaseDaemonEvidence>,
    pub execution: Option<ReleaseExecutionEvidence>,
    pub outcomes: BTreeMap<OutcomeHorizon, ReleaseOutcomeEvidence>,
    pub learning: Option<ReleaseLearningEvidence>,
    pub canary: Option<ReleaseCanaryEvidence>,
    pub human_approval: Option<ReleaseHumanApprovalEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub post_outcome_approval: Option<ReleasePostOutcomeApprovalEvidence>,
    pub integrity: ReleaseIntegrityEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseEvidenceBundle {
    pub schema_version: u32,
    pub bundle_version: u32,
    pub evidence_tier: ReleaseEvidenceTier,
    pub status: ReleaseEvidenceStatus,
    pub issues: BTreeSet<ReleaseEvidenceIssue>,
    pub body: ReleaseEvidenceBody,
    pub bundle_hash: ContentHash,
}

impl ReleaseEvidenceBundle {
    pub fn materialize(body: ReleaseEvidenceBody) -> Result<Self, DomainError> {
        validate_release_body(&body)?;
        let evidence_tier = derive_evidence_tier(&body);
        let (status, issues) = derive_release_state(&body);
        let bundle_hash = release_bundle_hash(evidence_tier, status, &issues, &body)?;
        let bundle = Self {
            schema_version: DOMAIN_SCHEMA_VERSION,
            bundle_version: RELEASE_EVIDENCE_BUNDLE_VERSION,
            evidence_tier,
            status,
            issues,
            body,
            bundle_hash,
        };
        bundle.validate()?;
        Ok(bundle)
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION
            || self.bundle_version != RELEASE_EVIDENCE_BUNDLE_VERSION
        {
            return Err(DomainError::InvalidContentHash);
        }
        validate_release_body(&self.body)?;
        let evidence_tier = derive_evidence_tier(&self.body);
        let (status, issues) = derive_release_state(&self.body);
        if evidence_tier != self.evidence_tier
            || status != self.status
            || issues != self.issues
            || release_bundle_hash(evidence_tier, status, &issues, &self.body)? != self.bundle_hash
        {
            return Err(DomainError::InvalidContentHash);
        }
        Ok(())
    }
}

fn validate_release_body(body: &ReleaseEvidenceBody) -> Result<(), DomainError> {
    if let Some(runtime) = &body.runtime {
        if runtime.repository_commit.trim().is_empty()
            || runtime.rust_toolchain.trim().is_empty()
            || !matches!(
                runtime.experiment_profile.as_str(),
                "fixture" | "paper-engineering" | "paper-research" | "historical-eval"
            )
            || !matches!(runtime.market_data_feed.as_str(), "iex" | "sip")
        {
            return Err(DomainError::EmptyField {
                field: "release.repository_commit",
            });
        }
    }
    if let Some(workflow) = &body.workflow {
        workflow.plan.validate()?;
        if workflow.graph.kind != crate::ArtifactKind::WorkflowGraph
            || workflow.workflow_hash != workflow.graph.artifact_id.0
        {
            return Err(DomainError::InvalidContentHash);
        }
    }
    if body.provider_routes.iter().any(|route| {
        route.provider_id.trim().is_empty()
            || route.model_id.trim().is_empty()
            || route.source.trim().is_empty()
    }) || body
        .source_snapshots
        .iter()
        .any(|source| source.source_family.trim().is_empty())
    {
        return Err(DomainError::EmptyField {
            field: "release.provider_or_source",
        });
    }
    if let Some(broker) = &body.broker {
        if broker.orders.iter().any(|order| {
            order.client_order_id.trim().is_empty() || order.broker_order_id.trim().is_empty()
        }) {
            return Err(DomainError::EmptyField {
                field: "release.order_identity",
            });
        }
    }
    if let Some(session) = &body.session {
        if session.session_key.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "release.session",
            });
        }
    }
    if let Some(daemon) = &body.daemon {
        if daemon.lease_name.trim().is_empty() || daemon.owner_id.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "release.daemon",
            });
        }
    }
    if let Some(execution) = &body.execution {
        if execution.execution_plan.kind != crate::ArtifactKind::ExecutionPlan
            || execution.commitment.kind != crate::ArtifactKind::ExecutionCommitment
            || execution
                .reconciliation
                .as_ref()
                .is_some_and(|value| value.kind != crate::ArtifactKind::Reconciliation)
            || execution
                .reconciliation_receipts
                .iter()
                .any(|value| value.kind != crate::ArtifactKind::OrderReceipt)
            || execution.commitment_id.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "release.execution",
            });
        }
    }
    for (horizon, outcome) in &body.outcomes {
        if outcome.outcome.kind != crate::ArtifactKind::Outcome
            || !OutcomeHorizon::ALL.contains(horizon)
        {
            return Err(DomainError::EmptyField {
                field: "release.outcome",
            });
        }
    }
    if let Some(learning) = &body.learning {
        if learning.transition_id.trim().is_empty()
            || learning.evaluation.kind != crate::ArtifactKind::Evaluation
        {
            return Err(DomainError::EmptyField {
                field: "release.learning",
            });
        }
    }
    if let Some(approval) = &body.human_approval {
        if approval.operator_identity.trim().is_empty()
            || (approval.status == ReleaseHumanApprovalStatus::Approved
                && approval.approved_at.is_none())
        {
            return Err(DomainError::EmptyField {
                field: "release.human_approval",
            });
        }
    }
    Ok(())
}

fn release_bundle_hash(
    evidence_tier: ReleaseEvidenceTier,
    status: ReleaseEvidenceStatus,
    issues: &BTreeSet<ReleaseEvidenceIssue>,
    body: &ReleaseEvidenceBody,
) -> Result<ContentHash, DomainError> {
    content_hash_json(&serde_json::json!({
        "schema_version": DOMAIN_SCHEMA_VERSION,
        "bundle_version": RELEASE_EVIDENCE_BUNDLE_VERSION,
        "evidence_tier": evidence_tier,
        "status": status,
        "issues": issues,
        "body": body,
    }))
    .map_err(|_| DomainError::InvalidContentHash)
}

fn derive_evidence_tier(body: &ReleaseEvidenceBody) -> ReleaseEvidenceTier {
    match (body.environment, body.purpose) {
        (ReleaseEvidenceEnvironment::OfflineFixture, _) => ReleaseEvidenceTier::E0Fixture,
        (_, RunPurpose::Replay | RunPurpose::Shadow) => ReleaseEvidenceTier::E1HistoricalReplay,
        (ReleaseEvidenceEnvironment::Real, RunPurpose::Paper)
            if has_completed_robustness_evidence(body) =>
        {
            ReleaseEvidenceTier::E4Robustness
        }
        (ReleaseEvidenceEnvironment::Real, RunPurpose::Paper)
            if has_complete_forward_outcomes(body) =>
        {
            ReleaseEvidenceTier::E3ForwardPaper
        }
        (ReleaseEvidenceEnvironment::Real, RunPurpose::Paper) => {
            ReleaseEvidenceTier::E2PaperMechanics
        }
        _ => ReleaseEvidenceTier::E0Fixture,
    }
}

fn has_complete_forward_outcomes(body: &ReleaseEvidenceBody) -> bool {
    body.broker
        .as_ref()
        .is_some_and(|broker| broker.trust == ReleaseBrokerEvidenceTrust::RealBroker)
        && body.execution.as_ref().is_some_and(|execution| {
            execution.reconciliation.is_some() && !execution.reconciliation_receipts.is_empty()
        })
        && OutcomeHorizon::ALL
            .into_iter()
            .all(|horizon| body.outcomes.contains_key(&horizon))
}

fn has_completed_robustness_evidence(body: &ReleaseEvidenceBody) -> bool {
    has_complete_forward_outcomes(body)
        && body.learning.is_some()
        && body
            .canary
            .as_ref()
            .is_some_and(|canary| canary.status == CanaryCampaignStatus::Completed)
        && (body
            .post_outcome_approval
            .as_ref()
            .is_some_and(|approval| approval.decision == ResearchApprovalDecision::Approved)
            || body.human_approval.as_ref().is_some_and(|approval| {
                approval.is_post_outcome_research_approval
                    && approval.status == ReleaseHumanApprovalStatus::Approved
                    && approval.approved_at.is_some()
            }))
        && body.integrity.config_hash_matches
        && body.integrity.workflow_hash_matches
        && body.integrity.broker_account_matches
        && body.integrity.daemon_epoch_current
}

fn derive_release_state(
    body: &ReleaseEvidenceBody,
) -> (ReleaseEvidenceStatus, BTreeSet<ReleaseEvidenceIssue>) {
    let mut issues = BTreeSet::new();
    if body.purpose != RunPurpose::Paper {
        issues.insert(ReleaseEvidenceIssue::NonCanonicalRun {
            purpose: serde_json::to_value(body.purpose)
                .ok()
                .and_then(|value| value.as_str().map(str::to_owned))
                .unwrap_or_else(|| "unknown".to_owned()),
        });
    }
    match &body.runtime {
        Some(runtime) => {
            if runtime.dirty_worktree {
                issues.insert(ReleaseEvidenceIssue::DirtyWorktree);
            }
            if body.environment == ReleaseEvidenceEnvironment::Real
                && body.purpose == RunPurpose::Paper
                && runtime.experiment_profile != "paper-research"
            {
                issues.insert(ReleaseEvidenceIssue::EngineeringOnlyProfile {
                    profile: runtime.experiment_profile.clone(),
                });
            }
            if runtime.experiment_profile == "paper-research" && runtime.market_data_feed != "sip" {
                issues.insert(ReleaseEvidenceIssue::UnsupportedResearchFeed {
                    feed: runtime.market_data_feed.clone(),
                });
            }
            if matches!(body.purpose, RunPurpose::Replay | RunPurpose::Shadow)
                && runtime.model_knowledge_cutoff.is_none()
            {
                issues.insert(ReleaseEvidenceIssue::MissingModelKnowledgeCutoff);
            }
        }
        None => {
            issues.insert(ReleaseEvidenceIssue::MissingRuntimeManifest);
        }
    }
    if body.environment == ReleaseEvidenceEnvironment::OfflineFixture {
        issues.insert(ReleaseEvidenceIssue::OfflineFixture);
    }
    if body.workflow.is_none() {
        issues.insert(ReleaseEvidenceIssue::MissingWorkflow);
    }
    if body.contracts.contract_hashes.is_empty()
        || body.contracts.tool_set_hashes.is_empty()
        || body.contracts.context_manifest_hashes.is_empty()
    {
        issues.insert(ReleaseEvidenceIssue::MissingContracts);
    }
    if body.provider_routes.is_empty() {
        issues.insert(ReleaseEvidenceIssue::MissingProviderCapability);
    }
    if body.source_snapshots.is_empty() {
        issues.insert(ReleaseEvidenceIssue::MissingSourceSnapshots);
    }
    match &body.broker {
        Some(broker) => {
            if broker.trust == ReleaseBrokerEvidenceTrust::OfflineFixture {
                issues.insert(ReleaseEvidenceIssue::FakeBrokerEvidence);
            }
            if broker.orders.is_empty() {
                issues.insert(ReleaseEvidenceIssue::MissingBrokerEvidence);
            }
        }
        None => {
            issues.insert(ReleaseEvidenceIssue::MissingBrokerEvidence);
        }
    }
    if body.session.is_none() {
        issues.insert(ReleaseEvidenceIssue::MissingSessionSlot);
    }
    if body.daemon.is_none() {
        issues.insert(ReleaseEvidenceIssue::MissingDaemonLease);
    }
    match &body.execution {
        Some(execution)
            if execution.reconciliation.is_none()
                || execution.reconciliation_receipts.is_empty() =>
        {
            issues.insert(ReleaseEvidenceIssue::MissingReconciliation);
        }
        Some(_) => {}
        None => {
            issues.insert(ReleaseEvidenceIssue::MissingExecution);
        }
    }
    for horizon in OutcomeHorizon::ALL {
        if !body.outcomes.contains_key(&horizon) {
            issues.insert(ReleaseEvidenceIssue::MissingOutcome { horizon });
        }
    }
    if body.learning.is_none() {
        issues.insert(ReleaseEvidenceIssue::MissingLearningTransition);
    }
    match &body.human_approval {
        Some(approval) if approval.status != ReleaseHumanApprovalStatus::Approved => {
            issues.insert(ReleaseEvidenceIssue::HumanApprovalNotApproved);
        }
        Some(_) => {}
        None => {
            issues.insert(ReleaseEvidenceIssue::MissingHumanApproval);
        }
    }
    if !body.integrity.config_hash_matches {
        issues.insert(ReleaseEvidenceIssue::ConfigHashDrift);
    }
    if !body.integrity.workflow_hash_matches {
        issues.insert(ReleaseEvidenceIssue::WorkflowHashDrift);
    }
    if !body.integrity.broker_account_matches {
        issues.insert(ReleaseEvidenceIssue::BrokerAccountMismatch);
    }
    if !body.integrity.daemon_epoch_current {
        issues.insert(ReleaseEvidenceIssue::StaleDaemonEpoch);
    }

    let hard_failure = issues.iter().any(|issue| {
        matches!(
            issue,
            ReleaseEvidenceIssue::NonCanonicalRun { .. }
                | ReleaseEvidenceIssue::DirtyWorktree
                | ReleaseEvidenceIssue::OfflineFixture
                | ReleaseEvidenceIssue::FakeBrokerEvidence
                | ReleaseEvidenceIssue::HumanApprovalNotApproved
                | ReleaseEvidenceIssue::ConfigHashDrift
                | ReleaseEvidenceIssue::WorkflowHashDrift
                | ReleaseEvidenceIssue::BrokerAccountMismatch
                | ReleaseEvidenceIssue::StaleDaemonEpoch
                | ReleaseEvidenceIssue::EngineeringOnlyProfile { .. }
                | ReleaseEvidenceIssue::UnsupportedResearchFeed { .. }
                | ReleaseEvidenceIssue::MissingModelKnowledgeCutoff
        )
    });
    let status = if hard_failure {
        ReleaseEvidenceStatus::NotApprovable
    } else if issues.is_empty() {
        ReleaseEvidenceStatus::Approvable
    } else {
        ReleaseEvidenceStatus::Incomplete
    };
    (status, issues)
}
