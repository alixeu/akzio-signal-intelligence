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
