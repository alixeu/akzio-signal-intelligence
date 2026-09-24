// 文件导读：本模块集中放置 EvaluationRuntime 的构造、只读策略访问，以及评估入口的转发。
// 真正的 Outcome 计算与 CAS 写入由同一 impl 的其他模块拆分实现；先读 evaluate_with_lease，
// 再沿 evaluate_with_retrospective 追踪持久化边界。这里的借用参数只在调用期间有效，
// Store 句柄则由 runtime 持有并供后续阶段复用。

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

    /// Materializes governed observations; committing learning still requires a
    /// valid T5 retrospective draft. Schedule creation is a separate earlier step.
    // input 的所有权转交给带 lease 的统一实现；这个入口本身不创建排期，也不另开写入路径。
    // 当前 evaluate_frozen 要求有效 T+5 RetrospectiveDraft；经此不带 draft 的入口会在该 Gate
    // 以 InvalidMaterialization 返回，故不能把“调用了 evaluate”理解为已评估。
    pub fn evaluate(&self, input: EvaluationInput) -> EvaluationRuntimeResult<EvaluationResult> {
        // 无 lease 的便捷入口仍走同一 T+5 materialize/evaluate_frozen Gate，不绕过任何资格检查。
        self.evaluate_with_lease(None, input)
    }

    /// Uses the same draft-required learning gate with an optional daemon lease;
    /// this overload passes no draft and therefore cannot complete learning.
    // Option<&DaemonLease> 区分“由普通调用者执行”和“由持 lease 的 worker 执行”；
    // 该借用不会被 runtime 保存，后续 Store 事务负责核验其 fencing 身份。
    pub fn evaluate_with_lease(
        &self,
        lease: Option<&DaemonLease>,
        input: EvaluationInput,
    ) -> EvaluationRuntimeResult<EvaluationResult> {
        // lease 只在有 daemon worker 时由下游 Store 事务核验；此 API 与 evaluate 一样不传
        // RetrospectiveDraft，当前实现会在统一 T+5 draft Gate 返回 InvalidMaterialization。
        self.evaluate_with_retrospective(lease, input, None)
    }

    // draft 只以共享借用提供模型已提交的叙事；None/Some 的含义由公共实现统一处理，
    // 本入口不会复制或改写 draft，也不绕过阶段资格。
    pub fn evaluate_with_lease_and_retrospective(
        &self,
        lease: Option<&DaemonLease>,
        input: EvaluationInput,
        draft: &RetrospectiveDraft,
    ) -> EvaluationRuntimeResult<EvaluationResult> {
        self.evaluate_with_retrospective(lease, input, Some(draft))
    }

}
