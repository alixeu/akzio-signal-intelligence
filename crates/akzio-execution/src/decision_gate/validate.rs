// 文件导读：这些校验函数把 DecisionProposal 的 Artifact provenance、ContextManifest 的
// 选择/隔离/祖先闭包和 DecisionDraft 的引用闭包分层检查。它们只读取 Store 并返回选中的
// ArtifactRef 集合；任何 RawEvidence、错误 lifecycle、跨 task/run 引用或未归因 Lesson/
// Experience 都在 Decision 事务前拒绝。

impl DecisionRuntime {
    fn validate_manifest(
        &self,
        manifest: &Artifact,
        proposal: &Artifact,
        contract_hash: &akzio_domain::ContentHash,
        permit: &TaskWritePermit,
    ) -> DecisionGateResult<BTreeSet<ArtifactRef>> {
        // 先绑定 proposal 与 manifest 的 origin/contract，再校验 payload 的 schema、trust、
        // token/byte 统计和 source closure，最后返回 selected 集合供 draft references 检查。
        // proposal/manifest 都是拥有的 Store Artifact；对 origin 的 `as_ref` 产生短期借用，
        // 用于比较同一个 task/attempt/contract，不把 provenance 从对象中移动出来。
        let proposal_origin = proposal
            .origin
            .as_ref()
            .ok_or(DecisionGateError::InvalidProposalProvenance)?;
        let Some(origin) = manifest.origin.as_ref() else {
            return Err(DecisionGateError::InvalidManifestClosure);
        };
        if manifest.lifecycle != ArtifactLifecycle::RunScoped
            || manifest.producer != "context.research.synthesizer"
            || manifest.provenance.source_family != "akzio.context"
            || manifest.provenance.producer_contract_hash.as_ref() != Some(contract_hash)
            || origin.run_id.as_ref() != Some(&permit.run_id)
            || origin.task_id != proposal_origin.task_id
            || origin.attempt_id != proposal_origin.attempt_id
            || origin.contract_hash.as_ref() != Some(contract_hash)
        {
            return Err(DecisionGateError::InvalidManifestClosure);
        }

        // JSON 解析产出独立拥有的 payload；此后所有 `?` 分别传播 Store、serde、hash 或
        // 递归闭包错误，未完成校验时不会返回任何 selected 引用集合。
        let payload: ContextManifestPayload =
            serde_json::from_slice(&self.store.read_blob(&manifest.blob)?)?;
        if payload.schema_version != DOMAIN_SCHEMA_VERSION
            || payload.contract_hash != *contract_hash
            || payload.selections.is_empty()
            || payload.selections.iter().any(|selection| {
                selection.reason.trim().is_empty() || selection.estimated_tokens == 0
                    || (matches!(
                        selection.artifact.kind,
                        ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail
                    ) && selection.trust != ContextTrust::UntrustedEvidence)
            })
        {
            return Err(DecisionGateError::InvalidManifestClosure);
        }

        // 新建的 visiting 集合只在这次递归调用树中记录当前路径，不是跨任务/跨进程锁；
        // source closure 验证通过后，selected 集合才交给 draft 引用闭包复用。
        self.validate_manifest_source_closure(manifest, &payload, permit, &mut BTreeSet::new())?;
        let selected = payload
            .selections
            .iter()
            .map(|selection| selection.artifact.clone())
            .collect::<BTreeSet<_>>();
        Ok(selected)
    }

    fn validate_manifest_source_closure(
        &self,
        manifest: &Artifact,
        payload: &ContextManifestPayload,
        permit: &TaskWritePermit,
        visiting: &mut BTreeSet<ArtifactId>,
    ) -> DecisionGateResult<()> {
        // 递归检查当前 Manifest 的 selected/quarantined/ancestor 三组引用互斥且完整；
        // visiting 集合用于阻断循环，递归返回时移除当前节点以允许其他分支复用祖先。
        // `visiting` 以可变借用跨递归传递；重复遇到当前路径节点即为 cycle 并立即拒绝。
        // 在正常返回前移除本节点，所以同一祖先被不同分支复用不会被误判成环。
        if !visiting.insert(manifest.artifact_id.clone()) {
            return Err(DecisionGateError::InvalidManifestClosure);
        }
        if payload.schema_version != DOMAIN_SCHEMA_VERSION
            || payload.selections.is_empty()
            || payload.selections.iter().any(|selection| {
                selection.reason.trim().is_empty() || selection.estimated_tokens == 0
                    || (matches!(
                        selection.artifact.kind,
                        ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail
                    ) && selection.trust != ContextTrust::UntrustedEvidence)
            })
        {
            return Err(DecisionGateError::InvalidManifestClosure);
        }

        // 这些 iterator/map 在 `collect` 时实际遍历并克隆 ArtifactRef；BTreeSet 同时用于
        // 去重检查，随后 declared 必须恰好等于选中、隔离和祖先三类 source。
        let selected = payload
            .selections
            .iter()
            .map(|selection| selection.artifact.clone())
            .collect::<BTreeSet<_>>();
        let quarantined = payload
            .quarantined
            .iter()
            .map(|quarantine| quarantine.artifact.clone())
            .collect::<BTreeSet<_>>();
        let ancestors = manifest
            .source_refs
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::ContextManifest)
            .cloned()
            .collect::<BTreeSet<_>>();
        let declared = manifest
            .source_refs
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut expected = selected.clone();
        expected.extend(quarantined.iter().cloned());
        expected.extend(ancestors.iter().cloned());
        // 检查集合大小和原 vector 长度可发现重复项；disjoint 拒绝同一引用同时 selected/
        // quarantined，input_hash 则将 selections 内容绑定到冻结 manifest。
        if selected.len() != payload.selections.len()
            || quarantined.len() != payload.quarantined.len()
            || !selected.is_disjoint(&quarantined)
            || payload.quarantined.iter().any(|quarantine| {
                !matches!(
                    quarantine.artifact.kind,
                    ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail
                ) || quarantine.indicators.is_empty()
            })
            || declared.len() != manifest.source_refs.len()
            || declared != expected
            || payload.input_hash != manifest_input_hash(&payload.selections)?
        {
            return Err(DecisionGateError::InvalidManifestClosure);
        }

        // 从 Store 逐个读取已选 Artifact 元数据核实 kind，并重算完整/投影字节与 token；
        // RawEvidence、AgentTurn、工具请求结果不能直接进入 Agent 可见 Manifest。
        let mut total_bytes = 0_u64;
        let mut projected_bytes = 0_u64;
        let mut estimated_tokens = 0_u32;
        for selection in &payload.selections {
            let artifact = self.store.artifact(&selection.artifact.artifact_id)?;
            if artifact.kind != selection.artifact.kind
                || matches!(
                    artifact.kind,
                    ArtifactKind::RawEvidence
                        | ArtifactKind::AgentTurn
                        | ArtifactKind::ToolCall
                        | ArtifactKind::ToolResult
                )
            {
                return Err(DecisionGateError::InvalidManifestClosure);
            }
            let legacy_tokens = estimate_tokens(artifact.blob.bytes);
            if selection.projected_bytes.is_none() && legacy_tokens != selection.estimated_tokens {
                return Err(DecisionGateError::InvalidManifestClosure);
            }
            total_bytes = total_bytes.saturating_add(artifact.blob.bytes);
            projected_bytes = projected_bytes.saturating_add(
                selection
                    .projected_bytes
                    .unwrap_or(artifact.blob.bytes),
            );
            estimated_tokens = estimated_tokens.saturating_add(selection.estimated_tokens);
        }
        if total_bytes != payload.total_bytes
            || projected_bytes != payload.projected_bytes.unwrap_or(total_bytes)
            || estimated_tokens != payload.estimated_tokens
        {
            return Err(DecisionGateError::InvalidManifestClosure);
        }
        for quarantine in &payload.quarantined {
            let artifact = self.store.artifact(&quarantine.artifact.artifact_id)?;
            if artifact.kind != quarantine.artifact.kind {
                return Err(DecisionGateError::InvalidManifestClosure);
            }
        }

        // 祖先 manifest 先检查不能同时被本层选中，再验证来源 Run/producer/contract，
        // 最后递归核验其自己的 selected/quarantine/ancestor 闭包。
        for parent_ref in ancestors {
            if selected.contains(&parent_ref) {
                return Err(DecisionGateError::InvalidManifestClosure);
            }
            let parent = self.load_expected(&parent_ref, ArtifactKind::ContextManifest)?;
            let Some(origin) = parent.origin.as_ref() else {
                return Err(DecisionGateError::InvalidManifestClosure);
            };
            if parent.lifecycle != ArtifactLifecycle::RunScoped
                || !parent.producer.starts_with("context.")
                || parent.provenance.source_family != "akzio.context"
                || origin.run_id.as_ref() != Some(&permit.run_id)
                || origin.task_id.is_none()
                || origin.attempt_id.is_none()
                || origin.contract_hash.is_none()
                || parent.provenance.producer_contract_hash != origin.contract_hash
            {
                return Err(DecisionGateError::InvalidManifestClosure);
            }
            let parent_payload: ContextManifestPayload =
                serde_json::from_slice(&self.store.read_blob(&parent.blob)?)?;
            if parent_payload.contract_hash != origin.contract_hash.clone().unwrap() {
                return Err(DecisionGateError::InvalidManifestClosure);
            }
            self.validate_manifest_source_closure(&parent, &parent_payload, permit, visiting)?;
        }
        // 仅成功路径移除当前递归节点；若上面返回 Err，整个调用链终止，visiting 随栈帧 Drop。
        visiting.remove(&manifest.artifact_id);
        Ok(())
    }

    fn validate_draft_closure(
        &self,
        draft: &DecisionDraft,
        selected: &BTreeSet<ArtifactRef>,
    ) -> DecisionGateResult<()> {
        // 把 draft 中 claims、critiques、evidence、research allocation 和 learning/conflict
        // 引用逐一限制在 Manifest selected 集合；对 selected 的 Lesson/Experience 还要求
        // 明确 applied 或 rejected，防止“模型看过但未声明影响”的隐式学习。
        // `chain/flat_map` 将多个字段的引用借用拼成单次遍历，for 才触发处理；每项必须
        // 精确落在 Manifest selected 集合，遇到越界立即返回而不继续写 Decision。
        for reference in draft
            .claims
            .iter()
            .chain(draft.critiques.iter())
            .chain(draft.evidence.iter())
            .chain(
                draft
                    .research_allocation
                    .iter()
                    .flat_map(|plan| plan.allocations.iter())
                    .flat_map(|allocation| allocation.evidence_refs.iter()),
            )
            .chain(draft.applied_learning_refs.iter())
            .chain(draft.rejected_learning_refs.iter())
            .chain(
                draft
                    .material_conflicts
                    .iter()
                    .flat_map(|conflict| [&conflict.claim, &conflict.critique]),
            )
        {
            if !selected.contains(reference) {
                return Err(DecisionGateError::ReferenceOutsideManifest(
                    reference.artifact_id.clone(),
                ));
            }
        }
        // Manifest 选中的 Lesson/Experience 即使未被 draft 的其它字段引用，也必须明确
        // applied 或 rejected；这是学习归因，不是隐式应用学习结果。
        for reference in selected.iter().filter(|reference| {
            matches!(
                reference.kind,
                ArtifactKind::Lesson | ArtifactKind::Experience
            )
        }) {
            if !draft.applied_learning_refs.contains(reference)
                && !draft.rejected_learning_refs.contains(reference)
            {
                return Err(DecisionGateError::MissingLearningAttribution(
                    reference.artifact_id.clone(),
                ));
            }
        }
        Ok(())
    }
}
