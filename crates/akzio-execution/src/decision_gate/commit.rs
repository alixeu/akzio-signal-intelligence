// 文件导读：这里负责 DecisionGate 对学习影响和 Artifact 的最后绑定。Policy influence
// 仅允许 canonical Paper 的 Experience/CandidatePolicy，并且 subject、Store 记录和 active
// policy head 必须一致；artifact helper 只 stage provenance，真正的 task 完成由 decide 的
// 单次 commit_attempt 完成。

impl DecisionRuntime {
    fn validate_policy_influence(&self, reference: &ArtifactRef) -> DecisionGateResult<()> {
        // 读取指定 Artifact 并确认 canonical Paper/lifecycle/kind，再按 kind 解析 subject；
        // 旧、隔离或未授权的学习产物不能改变当前 Decision。
        let artifact = self.store.artifact(&reference.artifact_id)?;
        let canonical_paper = artifact.lifecycle == ArtifactLifecycle::Canonical
            && artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                .is_some_and(|run_id| {
                    self.store
                        .run_purpose(run_id)
                        .is_ok_and(|purpose| purpose == RunPurpose::Paper)
                });
        if artifact.kind != reference.kind || !canonical_paper {
            return Err(DecisionGateError::InvalidPolicyInfluence(
                reference.artifact_id.clone(),
            ));
        }

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
