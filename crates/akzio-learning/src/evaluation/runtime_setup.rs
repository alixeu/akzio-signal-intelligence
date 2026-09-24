impl EvaluationRuntime {
    pub fn new(store: Store, policy: EvaluationPolicy) -> EvaluationRuntimeResult<Self> {
        // 构造只验证 ppm/样本阈值，不打开、迁移或写入 Store；Store 的 canonical/debug
        // 资格仍由每个写入入口和 Store 自身检查。
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
        // Shadow 只在 Paper 上记录 outcome-backed parent/candidate 对照；Store 以比较身份
        // 形成幂等键，完全相同的恢复提交可复用，冲突内容不会覆盖历史 pair。
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
        // 无 lease 的便捷入口仍走同一 T+5 materialize/evaluate/Store 路径，不绕过任何资格检查。
        self.evaluate_with_lease(None, input)
    }

    /// Materializes and commits learning while optionally fencing a daemon
    /// worker lease in the Store transaction.
    pub fn evaluate_with_lease(
        &self,
        lease: Option<&DaemonLease>,
        input: EvaluationInput,
    ) -> EvaluationRuntimeResult<EvaluationResult> {
        // lease 只在有 daemon worker 时由下游 Store 事务核验；是否有 lease 不改变 Outcome
        // 数值计算或 Policy transition 的规则。
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

}
