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
