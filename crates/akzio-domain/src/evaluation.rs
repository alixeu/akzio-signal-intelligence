//! Outcome-backed learning vocabulary.

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    artifact::{ArtifactKind, ArtifactRef},
    ids::{AttemptId, EvaluationId, ExperienceId, OutcomeId, PolicyTransitionId},
    ContentHash, DomainError, MemoryId, RunId, TaskId, TopologyId, V2_DOMAIN_SCHEMA_VERSION,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeHorizon {
    T1,
    T3,
    T5,
}

impl OutcomeHorizon {
    pub const ALL: [Self; 3] = [Self::T1, Self::T3, Self::T5];

    pub const fn trading_days(self) -> u8 {
        match self {
            Self::T1 => 1,
            Self::T3 => 3,
            Self::T5 => 5,
        }
    }

    /// Due means completed trading sessions after the baseline session, never
    /// elapsed wall-clock days.
    pub const fn is_due_after(self, completed_trading_sessions: u8) -> bool {
        completed_trading_sessions >= self.trading_days()
    }
}

/// Rust-owned execution lineage for a future Paper outcome.
///
/// A rejected decision has a durable `NoOrder` verdict and no broker
/// reconciliation. An accepted decision must retain both the commitment and
/// its reconciliation; an unreconciled commitment cannot be scheduled for
/// canonical learning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OutcomeExecutionLineage {
    NoOrder {
        execution_verdict: ArtifactRef,
    },
    ReconciledPaper {
        execution_verdict: ArtifactRef,
        commitment: ArtifactRef,
        reconciliation: ArtifactRef,
    },
}

impl OutcomeExecutionLineage {
    pub fn validate(&self) -> Result<(), DomainError> {
        match self {
            Self::NoOrder { execution_verdict } => {
                if execution_verdict.kind != ArtifactKind::ExecutionVerdict {
                    return Err(DomainError::EmptyField {
                        field: "outcome_schedule.execution_verdict",
                    });
                }
            }
            Self::ReconciledPaper {
                execution_verdict,
                commitment,
                reconciliation,
            } => {
                if execution_verdict.kind != ArtifactKind::ExecutionVerdict
                    || commitment.kind != ArtifactKind::ExecutionCommitment
                    || reconciliation.kind != ArtifactKind::Reconciliation
                {
                    return Err(DomainError::EmptyField {
                        field: "outcome_schedule.reconciled_lineage",
                    });
                }
            }
        }
        Ok(())
    }
}

/// Durable intent to materialize T+1, T+3, and T+5 observations.
///
/// Store validation later proves that these references form one source
/// closure. The schedule fixes the immutable lineage and leaves market-clock
/// acquisition to the daemon-owned materializer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeSchedule {
    pub schema_version: u32,
    pub outcome_id: OutcomeId,
    pub decision: ArtifactRef,
    pub decision_context: ArtifactRef,
    pub execution_context: ArtifactRef,
    pub execution: OutcomeExecutionLineage,
    pub baseline_trading_day: NaiveDate,
    pub created_at: DateTime<Utc>,
}

impl OutcomeSchedule {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION || self.outcome_id.0.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "outcome_schedule.identity",
            });
        }
        if self.decision.kind != ArtifactKind::Decision
            || self.decision_context.kind != ArtifactKind::DecisionContext
            || self.execution_context.kind != ArtifactKind::ExecutionContext
        {
            return Err(DomainError::EmptyField {
                field: "outcome_schedule.references",
            });
        }
        self.execution.validate()
    }

    pub fn due_horizons(&self, completed_trading_sessions: u8) -> Vec<OutcomeHorizon> {
        OutcomeHorizon::ALL
            .into_iter()
            .filter(|horizon| horizon.is_due_after(completed_trading_sessions))
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeWindow {
    pub horizon: OutcomeHorizon,
    pub observed_trading_day: NaiveDate,
    pub portfolio_return_ppm: i64,
    pub benchmark_return_ppm: i64,
    pub transaction_cost_ppm: u32,
    pub slippage_ppm: u32,
    pub utility_ppm: i64,
    pub calibration_ppm: Option<u32>,
    pub evidence_completeness_ppm: u32,
    pub risk_recall_ppm: Option<u32>,
}

impl OutcomeWindow {
    pub fn validate(&self) -> Result<(), DomainError> {
        if [
            self.calibration_ppm.unwrap_or_default(),
            self.evidence_completeness_ppm,
            self.risk_recall_ppm.unwrap_or_default(),
            self.transaction_cost_ppm,
            self.slippage_ppm,
        ]
        .into_iter()
        .any(|value| value > 1_000_000)
        {
            return Err(DomainError::InvalidBudget {
                field: "outcome_window.ppm",
            });
        }
        Ok(())
    }
}

/// Rust-owned cost assumptions applied to every sealed outcome window.
/// Values are parts-per-million of notional; later Paper reconciliation may
/// replace them with observed fill costs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeCostModel {
    pub transaction_cost_ppm: u32,
    pub slippage_ppm: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetrospectiveCategory {
    Research,
    Evidence,
    Risk,
    Decision,
    Execution,
    Topology,
    Contract,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetrospectiveConclusion {
    Worked,
    Failed,
    Mixed,
    Unresolved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetrospectiveStatus {
    Complete,
    ModelUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetrospectiveFinding {
    pub category: RetrospectiveCategory,
    pub conclusion: RetrospectiveConclusion,
    pub statement: String,
    #[serde(default)]
    pub artifact_refs: Vec<ArtifactRef>,
    pub confidence_ppm: u32,
}

impl RetrospectiveFinding {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.statement.trim().is_empty()
            || self.statement.chars().count() > 4_000
            || self.artifact_refs.len() > 8
            || self.confidence_ppm > 1_000_000
        {
            return Err(DomainError::InvalidBudget {
                field: "retrospective.finding",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetrospectiveDraft {
    pub schema_version: u32,
    pub outcome_id: OutcomeId,
    pub horizon: OutcomeHorizon,
    pub summary: String,
    #[serde(default)]
    pub findings: Vec<RetrospectiveFinding>,
    #[serde(default)]
    pub counterfactuals: Vec<String>,
    #[serde(default)]
    pub lesson_candidates: Vec<String>,
    #[serde(default)]
    pub diagnostic_gaps: Vec<String>,
    #[serde(default)]
    pub source_refs: Vec<ArtifactRef>,
    pub created_at: DateTime<Utc>,
}

impl RetrospectiveDraft {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION
            || self.outcome_id.0.trim().is_empty()
            || self.summary.chars().count() > 4_000
            || self.findings.len() > 12
            || self.source_refs.len() > 8
            || self.counterfactuals.len() > 3
            || self.lesson_candidates.len() > 8
            || self.diagnostic_gaps.len() > 8
            || self
                .counterfactuals
                .iter()
                .any(|item| item.chars().count() > 4_000)
            || self
                .lesson_candidates
                .iter()
                .any(|item| item.chars().count() > 4_000)
            || self
                .diagnostic_gaps
                .iter()
                .any(|item| item.chars().count() > 4_000)
        {
            return Err(DomainError::InvalidBudget {
                field: "retrospective.draft",
            });
        }
        for finding in &self.findings {
            finding.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Retrospective {
    pub schema_version: u32,
    pub outcome_id: OutcomeId,
    pub horizon: OutcomeHorizon,
    pub status: RetrospectiveStatus,
    pub summary: String,
    #[serde(default)]
    pub findings: Vec<RetrospectiveFinding>,
    #[serde(default)]
    pub counterfactuals: Vec<String>,
    #[serde(default)]
    pub lesson_candidates: Vec<String>,
    #[serde(default)]
    pub diagnostic_gaps: Vec<String>,
    #[serde(default)]
    pub source_refs: Vec<ArtifactRef>,
    pub outcome: ArtifactRef,
    pub created_at: DateTime<Utc>,
    pub sealed_at: Option<DateTime<Utc>>,
}

impl Retrospective {
    pub fn validate(&self) -> Result<(), DomainError> {
        let draft = RetrospectiveDraft {
            schema_version: self.schema_version,
            outcome_id: self.outcome_id.clone(),
            horizon: self.horizon,
            summary: self.summary.clone(),
            findings: self.findings.clone(),
            counterfactuals: self.counterfactuals.clone(),
            lesson_candidates: self.lesson_candidates.clone(),
            diagnostic_gaps: self.diagnostic_gaps.clone(),
            source_refs: self.source_refs.clone(),
            created_at: self.created_at,
        };
        draft.validate()?;
        if self.outcome.kind != ArtifactKind::Outcome {
            return Err(DomainError::EmptyField {
                field: "retrospective.outcome",
            });
        }
        if self.horizon == OutcomeHorizon::T5 && self.sealed_at.is_none() {
            return Err(DomainError::EmptyField {
                field: "retrospective.sealed_at",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptRelationKind {
    Retry,
    Recovery,
    Replay,
    Shadow,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptRelation {
    pub schema_version: u32,
    pub run_id: RunId,
    pub task_id: TaskId,
    pub parent_attempt_id: AttemptId,
    pub child_attempt_id: AttemptId,
    pub relation: AttemptRelationKind,
    pub created_at: DateTime<Utc>,
}

impl AttemptRelation {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION
            || self.run_id.0.trim().is_empty()
            || self.task_id.0.trim().is_empty()
            || self.parent_attempt_id.0.trim().is_empty()
            || self.child_attempt_id.0.trim().is_empty()
            || self.parent_attempt_id == self.child_attempt_id
        {
            return Err(DomainError::EmptyField {
                field: "attempt_relation.identity",
            });
        }
        Ok(())
    }
}

impl OutcomeCostModel {
    pub fn validate(self) -> Result<(), DomainError> {
        if self.transaction_cost_ppm > 1_000_000 || self.slippage_ppm > 1_000_000 {
            return Err(DomainError::InvalidBudget {
                field: "outcome.cost_model",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outcome {
    pub schema_version: u32,
    pub outcome_id: OutcomeId,
    pub schedule: ArtifactRef,
    pub market_evidence: Vec<ArtifactRef>,
    pub windows: Vec<OutcomeWindow>,
    pub sealed_at: Option<DateTime<Utc>>,
}

impl Outcome {
    pub fn is_sealed(&self) -> bool {
        self.sealed_at.is_some()
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION || self.outcome_id.0.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "outcome.identity",
            });
        }
        if self.schedule.kind != ArtifactKind::OutcomeSchedule
            || self.market_evidence.is_empty()
            || self.market_evidence.iter().any(|evidence| {
                !matches!(
                    evidence.kind,
                    ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail
                )
            })
        {
            return Err(DomainError::EmptyField {
                field: "outcome.references",
            });
        }

        if self.windows.is_empty() || self.windows.len() > OutcomeHorizon::ALL.len() {
            return Err(DomainError::InvalidBudget {
                field: "outcome.windows",
            });
        }
        let mut observed_days = [None; 3];
        for window in &self.windows {
            window.validate()?;
            let index = match window.horizon {
                OutcomeHorizon::T1 => 0,
                OutcomeHorizon::T3 => 1,
                OutcomeHorizon::T5 => 2,
            };
            if observed_days[index].is_some() {
                return Err(DomainError::InvalidBudget {
                    field: "outcome.windows",
                });
            }
            observed_days[index] = Some(window.observed_trading_day);
        }
        let mut previous_day = None;
        for day in observed_days.into_iter().flatten() {
            if previous_day.is_some_and(|previous| previous >= day) {
                return Err(DomainError::InvalidBudget {
                    field: "outcome.window_trading_days",
                });
            }
            previous_day = Some(day);
        }
        Ok(())
    }

    pub fn validate_sealed(&self) -> Result<(), DomainError> {
        self.validate()?;
        if self.windows.len() != OutcomeHorizon::ALL.len() {
            return Err(DomainError::InvalidBudget {
                field: "outcome.windows",
            });
        }
        self.sealed_at.ok_or(DomainError::EmptyField {
            field: "outcome.sealed_at",
        })?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryLifecycle {
    Candidate,
    Active,
    Proven,
    Contested,
    Retired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidatePolicyState {
    Candidate,
    Canary10,
    Canary25,
    Canary50,
    Active,
}

/// Stable typed namespace for memory, contract, and topology policy heads.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum PolicySubject {
    Memory(MemoryId),
    Contract(ContentHash),
    Topology(TopologyId),
}

impl PolicySubject {
    pub fn validate(&self) -> Result<(), DomainError> {
        let empty = match self {
            Self::Memory(memory_id) => memory_id.0.trim().is_empty(),
            Self::Contract(contract_hash) => contract_hash.as_str().trim().is_empty(),
            Self::Topology(topology_id) => topology_id.0.trim().is_empty(),
        };
        if empty {
            return Err(DomainError::EmptyField {
                field: "policy_subject.id",
            });
        }
        Ok(())
    }

    pub fn subject_id(&self) -> String {
        match self {
            Self::Memory(memory_id) => format!("memory:{}", memory_id.0),
            Self::Contract(contract_hash) => format!("contract:{}", contract_hash.as_str()),
            Self::Topology(topology_id) => format!("topology:{}", topology_id.0),
        }
    }

    pub fn from_subject_id(value: &str) -> Result<Self, DomainError> {
        let (kind, id) = value.split_once(':').ok_or(DomainError::EmptyField {
            field: "policy_subject.id",
        })?;
        let subject = match kind {
            "memory" => Self::Memory(MemoryId(id.to_owned())),
            "contract" => Self::Contract(ContentHash::new(id)?),
            "topology" => Self::Topology(TopologyId(id.to_owned())),
            _ => {
                return Err(DomainError::EmptyField {
                    field: "policy_subject.kind",
                });
            }
        };
        subject.validate()?;
        Ok(subject)
    }

    pub const fn initial_state(&self) -> PolicyState {
        match self {
            Self::Memory(_) => PolicyState::Memory(MemoryLifecycle::Candidate),
            Self::Contract(_) => PolicyState::Contract(CandidatePolicyState::Candidate),
            Self::Topology(_) => PolicyState::Topology(CandidatePolicyState::Candidate),
        }
    }

    pub const fn accepts_state(&self, state: PolicyState) -> bool {
        matches!(
            (self, state),
            (Self::Memory(_), PolicyState::Memory(_))
                | (Self::Contract(_), PolicyState::Contract(_))
                | (Self::Topology(_), PolicyState::Topology(_))
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "state")]
pub enum PolicyState {
    Memory(MemoryLifecycle),
    Contract(CandidatePolicyState),
    Topology(CandidatePolicyState),
}

impl PolicyState {
    pub const fn permits_influence_kind(self, kind: ArtifactKind) -> bool {
        matches!(
            (self, kind),
            (
                Self::Memory(MemoryLifecycle::Active | MemoryLifecycle::Proven),
                ArtifactKind::Experience
            ) | (
                Self::Contract(CandidatePolicyState::Active)
                    | Self::Topology(CandidatePolicyState::Active),
                ArtifactKind::CandidatePolicy
            )
        )
    }
}

/// Immutable candidate contract or topology input for bounded policy evaluation.
/// Its lifecycle is owned by the associated `PolicyTransition` and `PolicyHead`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidatePolicy {
    pub schema_version: u32,
    pub subject: PolicySubject,
    pub baseline: ArtifactRef,
    pub candidate: ArtifactRef,
    pub source_evaluation: ArtifactRef,
    pub created_at: DateTime<Utc>,
}

impl CandidatePolicy {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION {
            return Err(DomainError::EmptyField {
                field: "candidate_policy.schema_version",
            });
        }
        self.subject.validate()?;
        if self.baseline == self.candidate {
            return Err(DomainError::EmptyField {
                field: "candidate_policy.baseline_candidate",
            });
        }
        if self.source_evaluation.kind != ArtifactKind::Evaluation {
            return Err(DomainError::EmptyField {
                field: "candidate_policy.source_evaluation",
            });
        }
        match &self.subject {
            PolicySubject::Memory(_) => Err(DomainError::EmptyField {
                field: "candidate_policy.memory_subject",
            }),
            PolicySubject::Contract(_) => {
                if self.baseline.kind != ArtifactKind::Contract
                    || self.candidate.kind != ArtifactKind::Contract
                {
                    return Err(DomainError::EmptyField {
                        field: "candidate_policy.contract_refs",
                    });
                }
                Ok(())
            }
            PolicySubject::Topology(_) => {
                if self.baseline.kind != ArtifactKind::WorkflowGraph
                    || self.candidate.kind != ArtifactKind::WorkflowGraph
                {
                    return Err(DomainError::EmptyField {
                        field: "candidate_policy.topology_refs",
                    });
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Experience {
    pub schema_version: u32,
    pub experience_id: ExperienceId,
    pub subject: PolicySubject,
    pub hypothesis_id: String,
    pub decision: ArtifactRef,
    pub decision_context: ArtifactRef,
    pub execution_context: ArtifactRef,
    pub policy_verdict: ArtifactRef,
    pub outcome: ArtifactRef,
    pub contract_hash: ContentHash,
    pub topology_id: TopologyId,
    pub policy_state: PolicyState,
    pub created_at: DateTime<Utc>,
}

impl Experience {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION
            || self.experience_id.0.trim().is_empty()
            || self.hypothesis_id.trim().is_empty()
            || self.topology_id.0.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "experience.identity",
            });
        }
        self.subject.validate()?;
        if !self.subject.accepts_state(self.policy_state) {
            return Err(DomainError::EmptyField {
                field: "experience.policy_state",
            });
        }
        match &self.subject {
            PolicySubject::Contract(contract_hash) if contract_hash != &self.contract_hash => {
                return Err(DomainError::EmptyField {
                    field: "experience.contract_subject",
                });
            }
            PolicySubject::Topology(topology_id) if topology_id != &self.topology_id => {
                return Err(DomainError::EmptyField {
                    field: "experience.topology_subject",
                });
            }
            _ => {}
        }
        if self.decision.kind != ArtifactKind::Decision
            || self.decision_context.kind != ArtifactKind::DecisionContext
            || self.execution_context.kind != ArtifactKind::ExecutionContext
            || self.policy_verdict.kind != ArtifactKind::ExecutionVerdict
            || self.outcome.kind != ArtifactKind::Outcome
        {
            return Err(DomainError::EmptyField {
                field: "experience.references",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Evaluation {
    pub schema_version: u32,
    pub evaluation_id: EvaluationId,
    pub outcome: ArtifactRef,
    pub experience: ArtifactRef,
    pub marginal_utility_ppm: i64,
    pub token_cost: Option<u64>,
    pub latency_millis: Option<u64>,
    pub created_at: DateTime<Utc>,
}

impl Evaluation {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION || self.evaluation_id.0.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "evaluation.identity",
            });
        }
        if self.outcome.kind != ArtifactKind::Outcome
            || self.experience.kind != ArtifactKind::Experience
        {
            return Err(DomainError::EmptyField {
                field: "evaluation.references",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyTransition {
    pub schema_version: u32,
    pub transition_id: PolicyTransitionId,
    pub subject: PolicySubject,
    pub from: PolicyState,
    pub to: PolicyState,
    pub evaluation: ArtifactRef,
    pub created_at: DateTime<Utc>,
}

impl PolicyTransition {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION
            || self.transition_id.0.trim().is_empty()
            || self.from == self.to
            || self.evaluation.kind != ArtifactKind::Evaluation
        {
            return Err(DomainError::EmptyField {
                field: "policy_transition",
            });
        }
        self.subject.validate()?;
        if !self.subject.accepts_state(self.from) || !self.subject.accepts_state(self.to) {
            return Err(DomainError::EmptyField {
                field: "policy_transition.subject_state",
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, Utc};

    use super::{
        CandidatePolicy, CandidatePolicyState, Experience, Outcome, OutcomeCostModel,
        OutcomeExecutionLineage, OutcomeHorizon, OutcomeSchedule, OutcomeWindow, PolicyState,
        PolicySubject, PolicyTransition, Retrospective, RetrospectiveStatus,
    };
    use crate::{
        artifact::{ArtifactId, ArtifactKind, ArtifactRef},
        ids::{ExperienceId, OutcomeId, PolicyTransitionId},
        ContentHash, MemoryId, TopologyId,
    };

    fn reference(kind: ArtifactKind, value: &[u8]) -> ArtifactRef {
        ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(value)),
            kind,
        }
    }

    fn candidate_policy(
        subject: PolicySubject,
        baseline: ArtifactRef,
        candidate: ArtifactRef,
    ) -> CandidatePolicy {
        CandidatePolicy {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            subject,
            baseline,
            candidate,
            source_evaluation: reference(ArtifactKind::Evaluation, b"source-evaluation"),
            created_at: Utc::now(),
        }
    }

    fn trading_day(day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 8, day).unwrap()
    }

    fn window(horizon: OutcomeHorizon, day: u32) -> OutcomeWindow {
        OutcomeWindow {
            horizon,
            observed_trading_day: trading_day(day),
            portfolio_return_ppm: 1,
            benchmark_return_ppm: 0,
            transaction_cost_ppm: 0,
            slippage_ppm: 0,
            utility_ppm: 1,
            calibration_ppm: Some(1),
            evidence_completeness_ppm: 1_000_000,
            risk_recall_ppm: Some(1_000_000),
        }
    }

    #[test]
    fn outcome_cost_model_rejects_values_above_one() {
        assert!(OutcomeCostModel {
            transaction_cost_ppm: 1_000_001,
            slippage_ppm: 0,
        }
        .validate()
        .is_err());
    }

    fn reconciled_schedule() -> OutcomeSchedule {
        OutcomeSchedule {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            outcome_id: OutcomeId::new(),
            decision: reference(ArtifactKind::Decision, b"decision"),
            decision_context: reference(ArtifactKind::DecisionContext, b"decision-context"),
            execution_context: reference(ArtifactKind::ExecutionContext, b"execution-context"),
            execution: OutcomeExecutionLineage::ReconciledPaper {
                execution_verdict: reference(ArtifactKind::ExecutionVerdict, b"execution-verdict"),
                commitment: reference(ArtifactKind::ExecutionCommitment, b"commitment"),
                reconciliation: reference(ArtifactKind::Reconciliation, b"reconciliation"),
            },
            baseline_trading_day: trading_day(10),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn outcome_schedule_uses_completed_trading_sessions() {
        let schedule = reconciled_schedule();
        schedule.validate().unwrap();
        assert!(schedule.due_horizons(0).is_empty());
        assert_eq!(schedule.due_horizons(1), vec![OutcomeHorizon::T1]);
        assert_eq!(
            schedule.due_horizons(3),
            vec![OutcomeHorizon::T1, OutcomeHorizon::T3]
        );
        assert_eq!(schedule.due_horizons(5), OutcomeHorizon::ALL);
        assert!(ArtifactKind::OutcomeSchedule.can_be_canonical());
    }

    #[test]
    fn outcome_schedule_distinguishes_no_order_from_reconciliation() {
        let mut schedule = reconciled_schedule();
        schedule.execution = OutcomeExecutionLineage::NoOrder {
            execution_verdict: reference(ArtifactKind::ExecutionVerdict, b"no-order"),
        };
        schedule.validate().unwrap();

        schedule.execution = OutcomeExecutionLineage::ReconciledPaper {
            execution_verdict: reference(ArtifactKind::ExecutionVerdict, b"accepted"),
            commitment: reference(ArtifactKind::ExecutionPlan, b"wrong-kind"),
            reconciliation: reference(ArtifactKind::Reconciliation, b"reconciliation"),
        };
        assert!(schedule.validate().is_err());
    }

    #[test]
    fn learning_requires_a_sealed_complete_outcome() {
        let outcome = Outcome {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            outcome_id: OutcomeId::new(),
            schedule: reference(ArtifactKind::OutcomeSchedule, b"schedule"),
            market_evidence: vec![reference(ArtifactKind::NormalizedEvidence, b"market")],
            windows: vec![
                window(OutcomeHorizon::T1, 11),
                window(OutcomeHorizon::T3, 13),
                window(OutcomeHorizon::T5, 17),
            ],
            sealed_at: None,
        };
        outcome.validate().unwrap();
        assert!(outcome.validate_sealed().is_err());

        let sealed = Outcome {
            sealed_at: Some(Utc::now()),
            ..outcome
        };
        sealed.validate_sealed().unwrap();
    }

    #[test]
    fn unsealed_outcome_accepts_a_due_prefix_but_not_canonical_sealing() {
        let partial = Outcome {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            outcome_id: OutcomeId::new(),
            schedule: reference(ArtifactKind::OutcomeSchedule, b"partial-schedule"),
            market_evidence: vec![reference(
                ArtifactKind::NormalizedEvidence,
                b"partial-market",
            )],
            windows: vec![window(OutcomeHorizon::T1, 11)],
            sealed_at: None,
        };
        partial.validate().unwrap();
        assert!(partial.validate_sealed().is_err());
    }

    #[test]
    fn model_unavailable_t5_retrospective_still_requires_sealing() {
        let outcome = reference(ArtifactKind::Outcome, b"sealed-outcome");
        let retrospective = Retrospective {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            outcome_id: OutcomeId::new(),
            horizon: OutcomeHorizon::T5,
            status: RetrospectiveStatus::ModelUnavailable,
            summary: "model unavailable".to_owned(),
            findings: Vec::new(),
            counterfactuals: Vec::new(),
            lesson_candidates: Vec::new(),
            diagnostic_gaps: vec!["model unavailable".to_owned()],
            source_refs: vec![outcome.clone()],
            outcome,
            created_at: Utc::now(),
            sealed_at: None,
        };
        assert!(retrospective.validate().is_err());
    }

    #[test]
    fn outcome_rejects_non_monotonic_observation_days() {
        let outcome = Outcome {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            outcome_id: OutcomeId::new(),
            schedule: reference(ArtifactKind::OutcomeSchedule, b"schedule"),
            market_evidence: vec![reference(ArtifactKind::SemanticDetail, b"market")],
            windows: vec![
                window(OutcomeHorizon::T1, 11),
                window(OutcomeHorizon::T3, 17),
                window(OutcomeHorizon::T5, 13),
            ],
            sealed_at: Some(Utc::now()),
        };
        assert!(outcome.validate().is_err());
    }

    #[test]
    fn candidate_policy_accepts_contract_and_topology_payloads() {
        let contract_candidate = reference(ArtifactKind::Contract, b"contract-candidate");
        candidate_policy(
            PolicySubject::Contract(contract_candidate.artifact_id.0.clone()),
            reference(ArtifactKind::Contract, b"contract-baseline"),
            contract_candidate,
        )
        .validate()
        .unwrap();

        candidate_policy(
            PolicySubject::Topology(TopologyId::new()),
            reference(ArtifactKind::WorkflowGraph, b"topology-baseline"),
            reference(ArtifactKind::WorkflowGraph, b"topology-candidate"),
        )
        .validate()
        .unwrap();
    }

    #[test]
    fn candidate_policy_rejects_memory_subject() {
        let policy = candidate_policy(
            PolicySubject::Memory(MemoryId::new()),
            reference(ArtifactKind::Contract, b"baseline"),
            reference(ArtifactKind::Contract, b"candidate"),
        );

        assert_eq!(
            policy.validate(),
            Err(crate::DomainError::EmptyField {
                field: "candidate_policy.memory_subject",
            })
        );
    }

    #[test]
    fn candidate_policy_rejects_identical_baseline_and_candidate() {
        let candidate = reference(ArtifactKind::WorkflowGraph, b"same-topology");
        let policy = candidate_policy(
            PolicySubject::Topology(TopologyId::new()),
            candidate.clone(),
            candidate,
        );

        assert_eq!(
            policy.validate(),
            Err(crate::DomainError::EmptyField {
                field: "candidate_policy.baseline_candidate",
            })
        );
    }

    #[test]
    fn candidate_policy_rejects_wrong_artifact_kinds() {
        let contract_candidate = reference(ArtifactKind::WorkflowGraph, b"wrong-contract");
        let contract = candidate_policy(
            PolicySubject::Contract(contract_candidate.artifact_id.0.clone()),
            reference(ArtifactKind::Contract, b"contract-baseline"),
            contract_candidate,
        );
        assert_eq!(
            contract.validate(),
            Err(crate::DomainError::EmptyField {
                field: "candidate_policy.contract_refs",
            })
        );

        let topology = candidate_policy(
            PolicySubject::Topology(TopologyId::new()),
            reference(ArtifactKind::WorkflowGraph, b"topology-baseline"),
            reference(ArtifactKind::Contract, b"wrong-topology"),
        );
        assert_eq!(
            topology.validate(),
            Err(crate::DomainError::EmptyField {
                field: "candidate_policy.topology_refs",
            })
        );
    }

    #[test]
    fn candidate_policy_requires_evaluation_source() {
        let mut policy = candidate_policy(
            PolicySubject::Topology(TopologyId::new()),
            reference(ArtifactKind::WorkflowGraph, b"baseline"),
            reference(ArtifactKind::WorkflowGraph, b"candidate"),
        );
        policy.source_evaluation = reference(ArtifactKind::Outcome, b"wrong-source");

        assert_eq!(
            policy.validate(),
            Err(crate::DomainError::EmptyField {
                field: "candidate_policy.source_evaluation",
            })
        );
    }

    #[test]
    fn candidate_policy_contract_binding_is_store_owned() {
        let policy = candidate_policy(
            PolicySubject::Contract(ContentHash::of_bytes(b"different-contract")),
            reference(ArtifactKind::Contract, b"baseline"),
            reference(ArtifactKind::Contract, b"candidate"),
        );

        policy.validate().unwrap();
    }

    #[test]
    fn experience_and_transition_use_typed_policy_subjects() {
        let contract_hash = ContentHash::of_bytes(b"contract");
        let subject = PolicySubject::Contract(contract_hash.clone());
        let experience = Experience {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            experience_id: ExperienceId::new(),
            subject: subject.clone(),
            hypothesis_id: "stable hypothesis".to_owned(),
            decision: reference(ArtifactKind::Decision, b"decision"),
            decision_context: reference(ArtifactKind::DecisionContext, b"decision-context"),
            execution_context: reference(ArtifactKind::ExecutionContext, b"execution-context"),
            policy_verdict: reference(ArtifactKind::ExecutionVerdict, b"verdict"),
            outcome: reference(ArtifactKind::Outcome, b"outcome"),
            contract_hash,
            topology_id: TopologyId("topology".to_owned()),
            policy_state: PolicyState::Contract(CandidatePolicyState::Canary10),
            created_at: Utc::now(),
        };
        experience.validate().unwrap();

        let transition = PolicyTransition {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            transition_id: PolicyTransitionId::new(),
            subject: subject.clone(),
            from: PolicyState::Contract(CandidatePolicyState::Candidate),
            to: PolicyState::Contract(CandidatePolicyState::Canary10),
            evaluation: reference(ArtifactKind::Evaluation, b"evaluation"),
            created_at: Utc::now(),
        };
        transition.validate().unwrap();
        assert_eq!(
            subject.subject_id(),
            format!("contract:{}", experience.contract_hash)
        );

        let mut mismatched = transition.clone();
        mismatched.subject = PolicySubject::Memory(MemoryId::new());
        assert!(mismatched.validate().is_err());

        let old_shape = serde_json::json!({
            "schema_version": crate::V2_DOMAIN_SCHEMA_VERSION,
            "transition_id": PolicyTransitionId::new(),
            "subject_id": subject.subject_id(),
            "from": {"kind": "contract", "state": "candidate"},
            "to": {"kind": "contract", "state": "canary10"},
            "evaluation": reference(ArtifactKind::Evaluation, b"old-evaluation"),
            "created_at": Utc::now(),
        });
        assert!(serde_json::from_value::<PolicyTransition>(old_shape).is_err());
    }

    #[test]
    fn policy_subject_storage_identity_round_trips_with_namespace() {
        for subject in [
            PolicySubject::Memory(MemoryId::new()),
            PolicySubject::Contract(ContentHash::of_bytes(b"contract")),
            PolicySubject::Topology(TopologyId::new()),
        ] {
            assert_eq!(
                PolicySubject::from_subject_id(&subject.subject_id()).unwrap(),
                subject
            );
        }
        assert!(PolicySubject::from_subject_id("untyped").is_err());
        assert!(PolicySubject::from_subject_id("unknown:value").is_err());
    }
}
