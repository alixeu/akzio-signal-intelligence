impl EvaluationRuntime {
    pub fn new(store: V2Store, policy: EvaluationPolicy) -> EvaluationRuntimeResult<Self> {
        policy.validate()?;
        Ok(Self { store, policy })
    }

    pub fn policy(&self) -> &EvaluationPolicy {
        &self.policy
    }

    /// Persists a candidate/production comparison without changing policy.
    pub fn record_shadow_pair(
        &self,
        permit: &TaskWritePermit,
        subject: &PolicySubject,
        observation: ShadowObservation,
    ) -> EvaluationRuntimeResult<ShadowPairWriteResult> {
        self.require_paper(&permit.run_id)?;
        if let PolicySubject::Topology(topology_id) = subject {
            if observation.candidate_topology_id != topology_id.0 {
                return Err(EvaluationError::InvalidCandidatePolicy(
                    "shadow_topology_id",
                ));
            }
        }
        Ok(self.store.complete_shadow_pair(
            permit,
            &ShadowPairCompletion {
                subject: subject.clone(),
                parent_decision: observation.parent_decision,
                execution_context: observation.execution_context,
                candidate_decision: observation.candidate_decision,
                candidate_contract_hash: observation.candidate_contract_hash,
                candidate_topology_id: observation.candidate_topology_id,
                horizon: observation.horizon,
                parent_outcome: observation.parent_outcome,
                candidate_outcome: observation.candidate_outcome,
                completed_at: observation.completed_at,
            },
        )?)
    }

    /// Materializes governed observations, then commits immutable learning
    /// artifacts. Schedule creation is a separate earlier step.
    pub fn evaluate(&self, input: EvaluationInput) -> EvaluationRuntimeResult<EvaluationResult> {
        self.evaluate_with_lease(None, input)
    }

    /// Materializes and commits learning while optionally fencing a daemon
    /// worker lease in the Store transaction.
    pub fn evaluate_with_lease(
        &self,
        lease: Option<&DaemonLease>,
        input: EvaluationInput,
    ) -> EvaluationRuntimeResult<EvaluationResult> {
        self.evaluate_with_retrospective(lease, input, None)
    }

    pub fn evaluate_with_lease_and_retrospective(
        &self,
        lease: Option<&DaemonLease>,
        input: EvaluationInput,
        draft: &RetrospectiveDraft,
    ) -> EvaluationRuntimeResult<EvaluationResult> {
        self.evaluate_with_retrospective(lease, input, Some(draft))
    }

    pub fn evaluate_with_lease_at_state(
        &self,
        lease: Option<&DaemonLease>,
        input: EvaluationInput,
        retrospective_draft: Option<&RetrospectiveDraft>,
        target_state: PolicyState,
    ) -> EvaluationRuntimeResult<EvaluationResult> {
        self.evaluate_with_retrospective_at_state(
            lease,
            input,
            retrospective_draft,
            Some(target_state),
        )
    }

    fn evaluate_with_retrospective(
        &self,
        lease: Option<&DaemonLease>,
        input: EvaluationInput,
        retrospective_draft: Option<&RetrospectiveDraft>,
    ) -> EvaluationRuntimeResult<EvaluationResult> {
        self.evaluate_with_retrospective_at_state(lease, input, retrospective_draft, None)
    }
}
