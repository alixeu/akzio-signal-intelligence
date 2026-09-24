// 文件导读：定义 Memory/Contract/Topology 的 PolicySubject、状态转换、Experience、
// Evaluation 和 PolicyTransition，保持学习生命周期与业务 Artifact kind 一致。
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
    // 校验三类 policy subject 包装的 ID 非空。
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

    // 以 kind:id 形式生成持久化/查询用的稳定 subject ID。
    pub fn subject_id(&self) -> String {
        match self {
            Self::Memory(memory_id) => format!("memory:{}", memory_id.0),
            Self::Contract(contract_hash) => format!("contract:{}", contract_hash.as_str()),
            Self::Topology(topology_id) => format!("topology:{}", topology_id.0),
        }
    }

    // 解析 kind:id，并对 Contract hash、其他字符串 ID 复用 subject 校验。
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

    // 按 subject 类型返回对应的 Candidate 初始状态。
    pub const fn initial_state(&self) -> PolicyState {
        match self {
            Self::Memory(_) => PolicyState::Memory(MemoryLifecycle::Candidate),
            Self::Contract(_) => PolicyState::Contract(CandidatePolicyState::Candidate),
            Self::Topology(_) => PolicyState::Topology(CandidatePolicyState::Candidate),
        }
    }

    // 只接受与 subject 类型相同的 PolicyState，拒绝跨 namespace 转换。
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
    /// Revocation does not require evidence sufficient to grant new influence.
    // Contested/Retired 或 Candidate 是收紧权限的方向，不要求授予新影响的证据。
    pub const fn is_restriction_to(self, to: Self) -> bool {
        matches!(
            (self, to),
            (
                Self::Memory(_),
                Self::Memory(MemoryLifecycle::Contested | MemoryLifecycle::Retired)
            ) | (
                Self::Contract(_),
                Self::Contract(CandidatePolicyState::Candidate)
            ) | (
                Self::Topology(_),
                Self::Topology(CandidatePolicyState::Candidate)
            )
        )
    }
    // 只有 Active/Proven Memory 可影响 Experience，Active Contract/Topology 可影响 CandidatePolicy。
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
    // 校验 candidate/baseline 不同、来源 Evaluation 存在，并按 subject kind 检查引用类型。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION {
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evaluation_context: Option<ExperienceEvaluationContext>,
    pub policy_state: PolicyState,
    pub created_at: DateTime<Utc>,
}

impl Experience {
    // 校验 Experience 身份、subject/state 绑定、评估上下文资格和五类核心引用。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION
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
            PolicySubject::Contract(contract_hash)
                if self.evaluation_context.is_none() && contract_hash != &self.contract_hash =>
            {
                return Err(DomainError::EmptyField {
                    field: "experience.contract_subject",
                });
            }
            PolicySubject::Topology(topology_id)
                if self.evaluation_context.is_none() && topology_id != &self.topology_id =>
            {
                return Err(DomainError::EmptyField {
                    field: "experience.topology_subject",
                });
            }
            _ => {}
        }
        if let Some(context) = &self.evaluation_context {
            if context.version != 1
                || context.producer_run_id.0.is_empty()
                || context.producer_workflow.kind != ArtifactKind::WorkflowGraph
                || !context
                    .producer_contract_hashes
                    .contains(&self.contract_hash)
                || context.metric_basis.is_none()
                || (context.learning_eligible
                    && (!context.market_window_complete
                        || !context.research_sufficient
                        || !context.retrospective_valid
                        || !context.risk_ground_truth_measured))
            {
                return Err(DomainError::EmptyField {
                    field: "experience.evaluation_context",
                });
            }
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
    // 校验 Evaluation 身份以及 Outcome/Experience 两条来源引用。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION || self.evaluation_id.0.trim().is_empty() {
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
    // 校验状态确实发生变化、评估引用有效且 from/to 均属于 subject namespace。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION
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

/// Producer identity is immutable; evaluator identity and eligibility describe
/// the later review. Absence in legacy artifacts means unknown, never eligible.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExperienceEvaluationContext {
    pub version: u32,
    pub producer_run_id: RunId,
    pub producer_workflow: ArtifactRef,
    pub producer_workflow_revision: u64,
    pub producer_contract_hashes: std::collections::BTreeSet<ContentHash>,
    pub evaluator_contract_hash: Option<ContentHash>,
    pub metric_basis: Option<OutcomeMetricBasis>,
    pub market_window_complete: bool,
    pub research_sufficient: bool,
    pub retrospective_valid: bool,
    pub risk_ground_truth_measured: bool,
    pub learning_eligible: bool,
}
