// 文件导读：这里实现 T+1/T+3 的阶段复盘记录，以及 EvaluationRuntime 组装学习 Artifact 的内部工具。
// 前者只保存 RunScoped 诊断，不推进 canonical Policy；T+5 的完整 Outcome/资格路径在 evaluation.rs
// 主模块中。理解写入顺序时先看 record_partial_retrospective_with_diagnostic_fenced，再看 Store 提交。

impl EvaluationRuntime {
    #[allow(clippy::too_many_arguments)]
    // lease 和 permit 以借用传入，materialization 按值消费；函数先在内存中验证并组装两份
    // RunScoped Artifact，最后让 Store 在一个 fenced 事务中共同提交；
    // 构造 Artifact 时可能先 stage blob，前置错误不会提交正式 Artifact/阶段事件。
    pub fn record_partial_retrospective_with_diagnostic_fenced(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        materialization: OutcomeMaterializationInput,
        horizon: OutcomeHorizon,
        draft: Option<&RetrospectiveDraft>,
        prior_retrospectives: &[ArtifactRef],
        diagnostic: &str,
        now: DateTime<Utc>,
    ) -> EvaluationRuntimeResult<(Artifact, Artifact)> {
        // T+1/T+3 只写当前 due prefix：两份 Artifact 都是 RunScoped，作为可重试诊断
        // 保存，不进入 canonical Policy head，也不满足 T+5 学习转移。
        let outcome = materialize_partial_outcome(&materialization)?;
        if !outcome
            .windows
            .iter()
            .any(|window| window.horizon == horizon)
        {
            return Err(EvaluationError::InvalidMaterialization(
                "partial retrospective horizon",
            ));
        }
        let origin = permit.artifact_origin();
        let provenance = crate::trusted_learning_provenance(permit, now);
        let outcome_artifact = self.artifact_with_lifecycle(
            ArtifactKind::Outcome,
            &outcome,
            std::iter::once(materialization.schedule_artifact)
                .chain(outcome.market_evidence.iter().cloned())
                .chain(outcome.risk_ground_truth_refs())
                .collect(),
            ArtifactLifecycle::RunScoped,
            &origin,
            &provenance,
            now,
        )?;
        let outcome_ref = reference(&outcome_artifact);

        let mut status = RetrospectiveStatus::ModelUnavailable;
        let mut summary = format!("Rust-sealed {horizon:?} retrospective");
        let mut findings = Vec::new();
        let mut counterfactuals = Vec::new();
        let mut lesson_candidates = Vec::new();
        let mut diagnostic_gaps = vec![diagnostic.to_owned()];
        let mut source_refs = prior_retrospectives.to_vec();
        if let Some(draft) = draft {
            if draft.outcome_id != outcome.outcome_id || draft.horizon != horizon {
                return Err(EvaluationError::InvalidMaterialization(
                    "retrospective draft identity",
                ));
            }
            status = RetrospectiveStatus::Complete;
            summary = draft.summary.clone();
            findings = draft.findings.clone();
            counterfactuals = draft.counterfactuals.clone();
            lesson_candidates = draft.lesson_candidates.clone();
            diagnostic_gaps = draft.diagnostic_gaps.clone();
            source_refs.extend(draft.source_refs.clone());
            source_refs.extend(
                draft
                    .lesson_proposals
                    .iter()
                    .flat_map(|p| p.evidence_refs.iter().cloned()),
            );
            source_refs.extend(
                draft
                    .findings
                    .iter()
                    .flat_map(|finding| finding.artifact_refs.iter().cloned()),
            );
        }
        if let Some(draft) = draft {
            source_refs.extend(self.committed_draft_refs(permit, draft)?);
        }
        source_refs.push(outcome_ref.clone());
        source_refs.sort();
        source_refs.dedup();
        let retrospective = Retrospective {
            schema_version: DOMAIN_SCHEMA_VERSION,
            outcome_id: outcome.outcome_id,
            horizon,
            status,
            summary,
            findings,
            counterfactuals,
            lesson_candidates,
            lesson_proposals: draft
                .map(|d| d.lesson_proposals.clone())
                .unwrap_or_default(),
            diagnostic_gaps,
            source_refs: source_refs.clone(),
            outcome: outcome_ref,
            created_at: now,
            sealed_at: Some(now),
        };
        retrospective.validate()?;
        let retrospective_artifact = self.artifact_with_lifecycle(
            ArtifactKind::Retrospective,
            &retrospective,
            source_refs,
            ArtifactLifecycle::RunScoped,
            &origin,
            &provenance,
            now,
        )?;
        // Store 在一个 Immediate 事务中校验 lease/permit、来源闭包和 prefix 形状，再同时
        // 插入 Outcome/Retrospective；重复的完全相同身份可幂等返回，冲突 payload 会失败。
        self.store.record_partial_outcome_retrospective_fenced(
            lease,
            permit,
            &outcome_artifact,
            &retrospective_artifact,
            now,
        )?;
        Ok((outcome_artifact, retrospective_artifact))
    }

    /// Look up an exact matching committed model output, not just cited
    /// documents. An absent match returns an empty Vec here; any requirement
    /// to have one belongs to the later Store/protocol gate.
    // 按 Run 和 ArtifactKind 查询后，再用 attempt_id 与反序列化后的 draft 精确筛选；
    // `?` 将 Store/JSON 错误返回给调用者，匹配的只是引用，不会重写模型产物。
    fn committed_draft_refs(
        &self,
        permit: &TaskWritePermit,
        draft: &RetrospectiveDraft,
    ) -> EvaluationRuntimeResult<Vec<ArtifactRef>> {
        let mut refs = Vec::new();
        for artifact in self
            .store
            .run_artifacts_by_kind(&permit.run_id, ArtifactKind::RetrospectiveDraft)?
        {
            if artifact.origin.as_ref().and_then(|o| o.attempt_id.as_ref())
                != Some(&permit.attempt_id)
            {
                continue;
            }
            let persisted: RetrospectiveDraft =
                serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
            if persisted == *draft {
                refs.push(reference(&artifact));
            }
        }
        Ok(refs)
    }

    // 泛型 T 只要求可序列化，编译器会为具体 payload 类型生成实现；payload/source_refs 均借用或移入
    // 构造流程，Artifact 的 CAS BLOB 由 Store 生成，本方法不提交 task 生命周期。
    fn artifact<T: Serialize>(
        &self,
        kind: ArtifactKind,
        payload: &T,
        source_refs: Vec<ArtifactRef>,
        origin: &ArtifactOrigin,
        provenance: &ArtifactProvenance,
        created_at: DateTime<Utc>,
    ) -> EvaluationRuntimeResult<Artifact> {
        // canonical 只是 Artifact 的生命周期标签；真正是否可见于 Run/Attempt/Policy 索引，
        // 仍由后续专用 Store commit 决定。
        self.artifact_with_lifecycle(
            kind,
            payload,
            source_refs,
            ArtifactLifecycle::Canonical,
            origin,
            provenance,
            created_at,
        )
    }

    #[allow(clippy::too_many_arguments)]
    // lifecycle 与 payload 一同决定待构造 Artifact 的元数据；`stage_json` 先产生 BLOB 引用，
    // 后续任何 Store commit 失败时该 staged 内容是否保留由 Store 的暂存规则决定，不能据此称事务已完成。
    fn artifact_with_lifecycle<T: Serialize>(
        &self,
        kind: ArtifactKind,
        payload: &T,
        source_refs: Vec<ArtifactRef>,
        lifecycle: ArtifactLifecycle,
        origin: &ArtifactOrigin,
        provenance: &ArtifactProvenance,
        created_at: DateTime<Utc>,
    ) -> EvaluationRuntimeResult<Artifact> {
        // stage_json 先得到内容寻址 BLOB，再组装包含 provenance/source_refs 的 Artifact；
        // 本函数本身不收束 task、policy head 或 succeeded-output 索引。
        let blob = self.store.stage_json(payload)?;
        Ok(Artifact::new(
            kind,
            blob,
            "akzio-learning.evaluation",
            lifecycle,
            provenance.clone(),
            Some(origin.clone()),
            source_refs,
            created_at,
        )?)
    }
}
