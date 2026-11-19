// 文件导读：EvaluationRuntime 在这里验证策略并初始化，也负责将 Shadow 对照交给 Store。

impl EvaluationRuntime {
    // Store 与 EvaluationPolicy 被按值移入新 runtime；validate 失败时 ? 把校验错误返回给调用方，
    // 成功时 policy 才与 Store 一起成为 runtime 的所有字段。
    pub fn new(store: Store, policy: EvaluationPolicy) -> EvaluationRuntimeResult<Self> {
        // 构造只验证 ppm/样本阈值，不打开、迁移或写入 Store；Store 的 canonical/debug
        // 资格仍由每个写入入口和 Store 自身检查。
        policy.validate()?;
        Ok(Self { store, policy })
    }

    // 只借出 runtime 内部策略的共享引用，返回引用的有效期受 &self 借用约束；
    // 调用方可以读取阈值，但不能通过该接口改写策略。
    pub fn policy(&self) -> &EvaluationPolicy {
        &self.policy
    }

    /// Persists a candidate/production comparison without changing policy.
    // permit、subject 以共享借用提供授权身份；observation 按值交给本方法，校验后其字段被移入
    // Store completion 请求。只有 complete_shadow_pair 成功，调用方才得到已写入的结果。
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
}
