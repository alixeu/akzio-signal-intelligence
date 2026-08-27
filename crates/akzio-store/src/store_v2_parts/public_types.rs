fn push_alert(
    alerts: &mut Vec<StoreAlert>,
    code: &str,
    status: &str,
    severity: AlertSeverity,
    counts: &BTreeMap<String, u64>,
) {
    if let Some(&count) = counts.get(status).filter(|count| **count > 0) {
        alerts.push(StoreAlert {
            code: code.to_owned(),
            severity,
            count,
        });
    }
}

/// Fenced singleton lease for daemon-owned scheduling work. Task attempts use
/// their own permits; this lease exclusively authorizes session slots and
/// broker-visible commitment transitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonLease {
    pub lease_name: String,
    pub owner_id: String,
    pub epoch: u64,
    pub expires_at: DateTime<Utc>,
}

/// Exact Paper workflow frozen before its run is installed. A recovery must
/// reuse this graph and its task IDs instead of recompiling a new plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionReservation {
    pub session_key: String,
    pub workflow: WorkflowCommit,
    /// Immutable scheduler-owned `EvidenceNeed` inputs installed atomically
    /// with the frozen Paper graph, before any Run becomes visible.
    pub setup_artifacts: Vec<Artifact>,
    pub reserved_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSlot {
    pub session_key: String,
    pub workflow: WorkflowCommit,
    pub scheduler_epoch: u64,
    pub reserved_at: DateTime<Utc>,
    pub commitment_artifact_id: Option<ArtifactId>,
    pub committed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSlotReservation {
    pub slot: SessionSlot,
    pub newly_reserved: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCommit {
    pub session_key: String,
    pub permit: TaskWritePermit,
    pub commitment: Artifact,
    pub committed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCommitResult {
    pub commitment_artifact_id: ArtifactId,
    pub newly_committed: bool,
}

/// Current immutable-history head for a candidate memory, contract, or topology.
/// The transition table remains the source of history; this row is only a
/// transactionally maintained reconstruction cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyHead {
    pub subject: PolicySubject,
    pub state: PolicyState,
    pub revision: u64,
    pub transition_id: PolicyTransitionId,
    /// Durable event cursor for the transition that produced this head.
    pub transition_cursor: i64,
    pub updated_at: DateTime<Utc>,
}

/// Every canonical evaluation is recorded, even when it leaves the policy
/// state unchanged. This closes the freshness cursor so the same shadow pair
/// cannot be reconsidered after a no-op evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyEvaluationCommit {
    pub permit: TaskWritePermit,
    pub outcome: Artifact,
    pub final_retrospective: Artifact,
    pub experience: Artifact,
    pub evaluation: Artifact,
    pub candidate_policy: Option<Artifact>,
    pub subject: PolicySubject,
    pub from: PolicyState,
    pub to: PolicyState,
    /// Store-issued immutable cutoff and counts used to derive this
    /// evaluation. Pairs completed after `through_cursor` remain fresh.
    pub pair_snapshot: PolicyShadowPairSnapshot,
    /// Present only for an actual state transition. No-op evaluations retain
    /// immutable evidence history but do not manufacture an invalid
    /// `PolicyTransition { from == to }`.
    pub transition: Option<PolicyTransition>,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicyShadowPairSnapshot {
    pub after_cursor: i64,
    pub through_cursor: i64,
    pub counts_by_horizon: [u64; 3],
}

impl PolicyShadowPairSnapshot {
    pub const fn count(self, horizon: OutcomeHorizon) -> u64 {
        self.counts_by_horizon[match horizon {
            OutcomeHorizon::T1 => 0,
            OutcomeHorizon::T3 => 1,
            OutcomeHorizon::T5 => 2,
        }]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyEvaluationResult {
    pub policy_head: Option<PolicyHead>,
    pub consumed_pair_cursor: i64,
    pub evaluation_cursor: i64,
    pub newly_recorded: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredPolicyEvaluation {
    subject: PolicySubject,
    outcome_artifact_id: ArtifactId,
    experience_artifact_id: ArtifactId,
    evaluation_artifact_id: ArtifactId,
    candidate_policy_artifact_id: Option<ArtifactId>,
    from: PolicyState,
    to: PolicyState,
    transition_id: Option<PolicyTransitionId>,
    run_id: RunId,
    consumed_pair_cursor: i64,
    event_cursor: i64,
    completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PolicyConsumptionHead {
    subject: PolicySubject,
    consumed_pair_cursor: i64,
    evaluation_artifact_id: ArtifactId,
    evaluation_cursor: i64,
    updated_at: DateTime<Utc>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyTransitionRecord {
    pub transition: PolicyTransition,
    pub run_id: RunId,
    pub revision: u64,
    pub transition_cursor: i64,
}

/// One completed, outcome-backed comparison between the production decision
/// and a candidate. The key intentionally excludes `completed_at`: retries at
/// the same timestamp, or at a later timestamp after a crash, must remain
/// idempotent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ShadowPairCompletion {
    pub subject: PolicySubject,
    pub parent_decision: ArtifactRef,
    pub execution_context: ArtifactRef,
    pub candidate_decision: ArtifactRef,
    pub candidate_contract_hash: ContentHash,
    pub candidate_topology_id: String,
    pub horizon: OutcomeHorizon,
    pub parent_outcome: ArtifactRef,
    pub candidate_outcome: ArtifactRef,
    pub completed_at: DateTime<Utc>,
}

impl ShadowPairCompletion {
    pub fn pair_key(&self) -> StoreResult<ContentHash> {
        let key = serde_json::json!({
            "subject": &self.subject,
            "parent_decision": &self.parent_decision,
            "execution_context": &self.execution_context,
            "candidate_decision": &self.candidate_decision,
            "candidate_contract_hash": &self.candidate_contract_hash,
            "candidate_topology_id": &self.candidate_topology_id,
            "horizon": self.horizon,
        });
        Ok(akzio_domain::content_hash_json(&key)?)
    }

    fn validate(&self) -> StoreResult<()> {
        self.subject.validate()?;
        if self.candidate_topology_id.trim().is_empty() {
            return Err(StoreError::InvalidLearningCommit("shadow_pair.identity"));
        }
        match &self.subject {
            PolicySubject::Contract(contract_hash)
                if contract_hash != &self.candidate_contract_hash =>
            {
                return Err(StoreError::InvalidLearningCommit(
                    "shadow_pair.contract_subject",
                ));
            }
            PolicySubject::Topology(topology_id) if topology_id.0 != self.candidate_topology_id => {
                return Err(StoreError::InvalidLearningCommit(
                    "shadow_pair.topology_subject",
                ));
            }
            _ => {}
        }
        if self.parent_decision.kind != ArtifactKind::Decision
            || self.execution_context.kind != ArtifactKind::ExecutionContext
            || self.candidate_decision.kind != ArtifactKind::Decision
            || self.parent_outcome.kind != ArtifactKind::Outcome
            || self.candidate_outcome.kind != ArtifactKind::Outcome
        {
            return Err(StoreError::InvalidLearningCommit("shadow_pair.references"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredShadowPair {
    pub pair_key: ContentHash,
    pub completion: ShadowPairCompletion,
    /// Durable event cursor for the idempotent pair completion.
    pub completion_cursor: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShadowPairWriteResult {
    Inserted(StoredShadowPair),
    Existing(StoredShadowPair),
}
