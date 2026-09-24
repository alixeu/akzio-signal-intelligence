// 文件导读：这里负责 DecisionGate 对学习影响和 Artifact 的最后绑定。Policy influence
// 仅允许 canonical Paper 的 Experience/CandidatePolicy，并且 subject、Store 记录和 active
// policy head 必须一致；artifact helper 只 stage provenance，真正的 task 完成由 decide 的
// 单次 commit_attempt 完成。

impl DecisionRuntime {
    fn validate_policy_influence(&self, reference: &ArtifactRef) -> DecisionGateResult<()> {
        // 读取指定 Artifact 并确认 canonical Paper/lifecycle/kind，再按 kind 解析 subject；
        // 旧、隔离或未授权的学习产物不能改变当前 Decision。
        // `reference` 是只读身份，Store 返回拥有的 Artifact 和 payload；读取/反序列化错误
        // 经 `?` 向 DecisionGate 返回，不会忽略坏的 CAS 内容。
        let artifact = self.store.artifact(&reference.artifact_id)?;
        let canonical_paper = artifact.lifecycle == ArtifactLifecycle::Canonical
            && artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                .is_some_and(|run_id| {
                    // is_ok_and 将 run-purpose 查询错误也视为不满足资格，最终统一转成
                    // InvalidPolicyInfluence；它不会把查询失败放行。
                    self.store
                        .run_purpose(run_id)
                        .is_ok_and(|purpose| purpose == RunPurpose::Paper)
                });
        if artifact.kind != reference.kind || !canonical_paper {
            return Err(DecisionGateError::InvalidPolicyInfluence(
                reference.artifact_id.clone(),
            ));
        }

        // match 按 Artifact kind 穷尽允许的学习 payload；反序列化结果是拥有值，validate
        // 成功后只取 subject 用于和 Store active head 比较。
        let subject = match artifact.kind {
            ArtifactKind::Experience => {
                let payload: Experience =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                payload.validate()?;
                payload.subject
            }
            ArtifactKind::CandidatePolicy => {
                let payload: CandidatePolicy =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                payload.validate()?;
                payload.subject
            }
            _ => {
                return Err(DecisionGateError::InvalidPolicyInfluence(
                    reference.artifact_id.clone(),
                ));
            }
        };
        self.require_active_policy_influence(reference, &subject)
    }

    fn require_active_policy_influence(
        &self,
        reference: &ArtifactRef,
        subject: &PolicySubject,
    ) -> DecisionGateResult<()> {
        // 将历史 recorded subject 与当前 active policy head 双重比较，避免同一 Artifact hash
        // 在不同 subject 或非允许状态下被重新解释。
        // Store 可选 recorded subject 缺失时不触发第一项拒绝，但下面仍必须找到 active head；
        // Option::is_some_and 仅在有历史记录时比较，head 状态必须显式允许该 kind。
        if self
            .store
            .recorded_policy_influence_subject(&reference.artifact_id)?
            .is_some_and(|recorded| recorded != *subject)
            || !self
                .store
                .policy_head(subject)?
                .is_some_and(|head| head.state.permits_influence_kind(reference.kind))
        {
            return Err(DecisionGateError::InvalidPolicyInfluence(
                reference.artifact_id.clone(),
            ));
        }
        Ok(())
    }

    fn load_expected(
        &self,
        reference: &ArtifactRef,
        expected: ArtifactKind,
    ) -> DecisionGateResult<Artifact> {
        // 对 reference 的共享借用不转移调用者 ID；Store 查询返回由当前函数拥有的 Artifact。
        // 只按明确 Artifact kind 读取 Store 载荷，维持 Decision 引用的类型闭包。
        let artifact = self.store.artifact(&reference.artifact_id)?;
        if reference.kind != expected || artifact.kind != expected {
            return Err(DecisionGateError::WrongArtifactKind {
                expected,
                actual: artifact.kind,
            });
        }
        Ok(artifact)
    }

    fn artifact<T: serde::Serialize>(
        &self,
        kind: ArtifactKind,
        producer: &str,
        payload: &T,
        lifecycle: ArtifactLifecycle,
        source_refs: Vec<ArtifactRef>,
        input: &DecisionGateInput,
    ) -> DecisionGateResult<Artifact> {
        // 泛型 payload 在编译期受 Serialize bound 限制；序列化只借用它，Artifact 由本函数
        // 新建返回。该 helper stage CAS 内容，不提交 Attempt 或更新 task 状态。
        // 统一生成带 permit origin 的 Decision 侧 Artifact；lifecycle 由 run purpose 决定，
        // 这里不偷偷把 PositionPlan 提升为 canonical Paper。
        Ok(Artifact::new(
            kind,
            self.store.stage_json(payload)?,
            producer,
            lifecycle,
            crate::trusted_execution_provenance(&input.permit, input.now),
            Some(input.permit.artifact_origin()),
            source_refs,
            input.now,
        )?)
    }
}
