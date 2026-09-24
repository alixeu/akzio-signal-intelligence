// 文件导读：该 include 片段集中处理 ExecutionGate 的 payload 解码、Decision 来源闭包、
// 学习 Policy influence 资格、冻结状态读取和结果 Artifact 构造。它把 Store 中的 CAS
// 内容转成拥有的 Rust 类型并校验血缘；不执行 Broker I/O，也不在 evaluate 前发布产物。
impl ExecutionRuntime {
    fn read_payload<T: DeserializeOwned>(&self, artifact: &Artifact) -> ExecutionGateResult<T> {
        // `DeserializeOwned` 要求 T 不借用临时 blob 字节；Store 读取的 Vec 可在解析后释放，
        // 返回的 T 由调用者拥有。该泛型按具体 T 静态分发，serde 错误经 `?` 转成 GateError。
        // 只从 Store blob 反序列化指定类型；所有业务校验由调用方在得到 typed payload 后显式执行。
        Ok(serde_json::from_slice(
            &self.store.read_blob(&artifact.blob)?,
        )?)
    }

    fn validate_decision_provenance(
        &self,
        artifact: &Artifact,
        decision: &DecisionContext,
    ) -> ExecutionGateResult<()> {
        // 检查 DecisionContext 的 run、market-state manifest 以及 claims/critiques/evidence/
        // learning/conflict 的 source_refs 闭包，确保执行使用的不是脱离本 Run 的研究结果。
        // origin 的 run_id 必须与 DecisionContext 相同；之后再检查 validity 声明的唯一
        // MarketState Manifest，并逐一核对研究/学习引用确实属于 Artifact source_refs。
        if artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.as_ref())
            != Some(&decision.run_id)
        {
            return Err(ExecutionGateError::Integrity("decision context origin run"));
        }
        if let Some(validity) = &decision.validity {
            let manifests = artifact
                .source_refs
                .iter()
                .filter(|reference| reference.kind == ArtifactKind::ContextManifest)
                .collect::<Vec<_>>();
            if manifests.len() != 1
                || manifests[0].artifact_id.0 != validity.market_state_hash
            {
                return Err(ExecutionGateError::Integrity(
                    "decision validity market state",
                ));
            }
        }
        // `iter/chain/flat_map` 构造统一的借用迭代链，for 才实际逐项消费它；没有复制
        // 或移动 Decision 内的引用。任一缺失/错 kind 都立即经 `?` 返回完整性错误。
        for reference in decision
            .claims
            .iter()
            .chain(decision.critiques.iter())
            .chain(decision.evidence.iter())
            .chain(decision.policy_influences.iter())
            .chain(decision.applied_learning_refs.iter())
            .chain(decision.rejected_learning_refs.iter())
            .chain(
                decision
                    .material_conflicts
                    .iter()
                    .flat_map(|conflict| [&conflict.claim, &conflict.critique]),
            )
        {
            let source = self.store.artifact(&reference.artifact_id)?;
            if source.kind != reference.kind
                || !artifact
                    .source_refs
                    .iter()
                    .any(|declared| declared == reference)
            {
                return Err(ExecutionGateError::Integrity(
                    "decision context source refs",
                ));
            }
        }
        self.validate_policy_influences(artifact, decision)
    }

    fn validate_policy_influences(
        &self,
        decision_artifact: &Artifact,
        decision: &DecisionContext,
    ) -> ExecutionGateResult<()> {
        // 学习 Experience/CandidatePolicy 只有在同一 Manifest 选中、canonical Paper、subject
        // 与历史记录一致且当前 policy head 允许时才能影响 Decision；任何缺口都 fail closed。
        // 没有学习影响时无需查 Manifest；若有影响，要求恰好一个 Manifest，并将其 selection
        // 集合与声明的 source_refs 全量比较，既拒绝漏引用也拒绝额外未授权引用。
        if decision.policy_influences.is_empty() {
            return Ok(());
        }
        let manifest_refs = decision_artifact
            .source_refs
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::ContextManifest)
            .collect::<Vec<_>>();
        if manifest_refs.len() != 1 {
            return Err(ExecutionGateError::Integrity("policy influence manifest"));
        }
        let manifest = self.store.artifact(&manifest_refs[0].artifact_id)?;
        if manifest.kind != ArtifactKind::ContextManifest
            || manifest
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(&decision.run_id)
        {
            return Err(ExecutionGateError::Integrity("policy influence manifest"));
        }
        let payload: ContextManifestPayload = self.read_payload(&manifest)?;
        // 两个集合使用 ArtifactRef 的有序/去重语义作相等比较；后续每个影响 Artifact
        // 还会继续校验 kind、Run purpose、subject 和 policy head，Manifest 选中不等于已授权。
        let selected = payload
            .selections
            .iter()
            .map(|selection| selection.artifact.clone())
            .collect::<BTreeSet<_>>();
        let declared = manifest
            .source_refs
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if selected != declared
            || decision
                .policy_influences
                .iter()
                .any(|reference| !selected.contains(reference))
        {
            return Err(ExecutionGateError::Integrity("policy influence manifest"));
        }
        // 本循环仅遍历 Decision 已声明的影响；Experience 或 CandidatePolicy 分支各自
        // 解码并验证对应 payload，候选还必须追溯到 canonical Paper 的 Evaluation。
        for reference in &decision.policy_influences {
            let influence = self.store.artifact(&reference.artifact_id)?;
            if influence.kind != reference.kind || !self.is_canonical_paper(&influence)? {
                return Err(ExecutionGateError::Integrity("policy influence authority"));
            }
            let subject: PolicySubject = match reference.kind {
                ArtifactKind::Experience => {
                    let experience: Experience = self.read_payload(&influence)?;
                    experience.validate()?;
                    experience.subject
                }
                ArtifactKind::CandidatePolicy => {
                    let policy: CandidatePolicy = self.read_payload(&influence)?;
                    policy.validate()?;
                    let evaluation = self.store.artifact(&policy.source_evaluation.artifact_id)?;
                    if evaluation.kind != ArtifactKind::Evaluation
                        || !self.is_canonical_paper(&evaluation)?
                    {
                        return Err(ExecutionGateError::Integrity("candidate policy evaluation"));
                    }
                    policy.subject
                }
                _ => {
                    return Err(ExecutionGateError::Integrity(
                        "policy influence artifact kind",
                    ));
                }
            };
            if self
                .store
                .recorded_policy_influence_subject(&reference.artifact_id)?
                .is_some_and(|recorded| recorded != subject)
            {
                return Err(ExecutionGateError::Integrity("policy influence subject"));
            }
            let head = self
                .store
                .policy_head(&subject)?
                .ok_or(ExecutionGateError::Integrity("policy head"))?;
            if !head.state.permits_influence_kind(reference.kind) {
                return Err(ExecutionGateError::Integrity("policy head state"));
            }
        }
        Ok(())
    }

    fn is_canonical_paper(&self, artifact: &Artifact) -> ExecutionGateResult<bool> {
        // lifecycle 和 origin/run purpose 必须同时满足，隔离 Debug 或 RunScoped 产物不能被
        // 提升成 canonical Paper policy influence。
        if artifact.lifecycle != ArtifactLifecycle::Canonical {
            return Ok(false);
        }
        // Option 缺 origin/run_id 时返回 Ok(false)，因为这是资格不满足而非读取故障；
        // run_purpose 查询若失败则由 `?` 作为 Store 错误向上返回。
        let Some(run_id) = artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.as_ref())
        else {
            return Ok(false);
        };
        Ok(self.store.run_purpose(run_id)? == RunPurpose::Paper)
    }

    fn frozen(&self) -> ExecutionGateResult<bool> {
        // 读取最新 canonical FreezeState；没有状态表示未冻结，存在但类型/lifecycle 不对则
        // 报完整性错误，不把坏状态当作可执行。
        // 没有冻结 Artifact 表示 false；找到后要求 canonical 且 payload 自洽，否则不能
        // 把未知/损坏状态当作未冻结。这里是只读 Store 查询。
        let Some(artifact) = self
            .store
            .latest_artifact_by_kind(ArtifactKind::FreezeState)?
        else {
            return Ok(false);
        };
        if artifact.lifecycle != ArtifactLifecycle::Canonical {
            return Err(ExecutionGateError::Integrity("freeze state lifecycle"));
        }
        let state: FreezeState = self.read_payload(&artifact)?;
        state.validate()?;
        Ok(state.frozen)
    }

    fn artifact<T: serde::Serialize>(
        &self,
        kind: ArtifactKind,
        producer: &str,
        payload: &T,
        source_refs: Vec<ArtifactRef>,
        input: &ExecutionGateInput,
    ) -> ExecutionGateResult<Artifact> {
        // T 仅需可序列化，具体 payload 类型由编译期确定；借用 payload 编码为 staged blob，
        // 由 permit 绑定 run/contract provenance。真正写 Artifact 与 task 状态在 commit 完成。
        // 仅 stage ExecutionGate 结果并绑定当前 permit/run provenance，正式发布交给 commit。
        Ok(Artifact::new(
            kind,
            self.store.stage_json(payload)?,
            producer,
            ArtifactLifecycle::RunScoped,
            crate::trusted_execution_provenance(&input.permit, input.now),
            Some(input.permit.artifact_origin()),
            source_refs,
            input.now,
        )?)
    }
}
