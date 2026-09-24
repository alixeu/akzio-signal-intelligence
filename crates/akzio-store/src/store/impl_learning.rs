// 文件导读：这里验证 CandidatePolicy、canonical evaluation、OutcomeSchedule execution lineage
// 和 Shadow pair sources；只有 typed payload、purpose、source closure 全部匹配才允许进入学习提交。
// 先读 validate_policy_evaluation_commit_with_connection 理解正式写入前的完整门槛，再读
// read_outcome_schedule/validate_outcome_schedule_execution_lineage 追溯 NoOrder 与 ReconciledPaper 两支。
// 所有 connection-scoped 方法借用调用方的事务连接，以便看到同事务内的 staged Artifact/BLOB。
impl Store {
    // Doctor 从 CandidatePolicy source_evaluation 和 baseline/candidate Artifact 重建候选历史。
    // 注意：非空候选集中的 `self.read_artifact_payload` 会在 Doctor 已持有连接时二次取
    // Store Mutex，存在重入 Integrity 的源码风险；该调用并非 connection-scoped，
    // 不能据注释推断可顺利扫完，也不把风险写作已复现结果。
    fn verify_candidate_policy_history(&self, connection: &Connection) -> StoreResult<()> {
        // 按 kind 扫全量候选，要求 canonical、Paper 来源、payload 有效、source refs 精确，
        // 再与 source evaluation 的 subject/time/artifact ID 交叉核对。
        let artifact_ids = connection
            .prepare(
                "SELECT artifact_id FROM rebuild_artifacts WHERE kind = ?1 ORDER BY artifact_id",
            )?
            .query_map(params![enum_name(ArtifactKind::CandidatePolicy)], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for value in artifact_ids {
            let artifact_id = ArtifactId(ContentHash::new(value)?);
            let artifact = read_artifact(connection, &artifact_id)?;
            if artifact.lifecycle != ArtifactLifecycle::Canonical {
                return Err(StoreError::Integrity(format!(
                    "candidate policy {artifact_id} is noncanonical"
                )));
            }
            assert_artifact_from_paper_with_connection(connection, &artifact).map_err(|error| {
                StoreError::Integrity(format!(
                    "candidate policy {artifact_id} has invalid origin: {error}"
                ))
            })?;
            let policy: CandidatePolicy = self.read_artifact_payload(&artifact)?;
            policy.validate()?;
            if !has_exact_source_refs(
                &artifact,
                &[
                    policy.baseline.clone(),
                    policy.candidate.clone(),
                    policy.source_evaluation.clone(),
                ],
            ) {
                return Err(StoreError::Integrity(format!(
                    "candidate policy {artifact_id} has invalid source closure"
                )));
            }
            let evaluation =
                read_policy_evaluation(connection, &policy.source_evaluation.artifact_id)?
                    .ok_or_else(|| {
                        StoreError::Integrity(format!(
                            "candidate policy {artifact_id} has no source evaluation"
                        ))
                    })?;
            if evaluation.subject != policy.subject
                || evaluation.completed_at != policy.created_at
                || evaluation.candidate_policy_artifact_id.as_ref() != Some(&artifact_id)
            {
                return Err(StoreError::Integrity(format!(
                    "candidate policy {artifact_id} disagrees with source evaluation"
                )));
            }
            // 最后按 Contract/Topology subject 解码 baseline 与 candidate，检查 capability/hash/purpose 绑定。
            self.validate_candidate_policy_sources(connection, &policy)
                .map_err(|error| {
                    StoreError::Integrity(format!(
                        "candidate policy {artifact_id} has invalid binding: {error}"
                    ))
                })?;
        }
        Ok(())
    }

    // 在外层 evaluation 事务中验证 sealed Outcome、T5 narrative、Experience/Evaluation 和 candidate policy 闭包。
    fn validate_policy_evaluation_commit_with_connection(
        &self,
        connection: &Connection,
        commit: &PolicyEvaluationCommit,
    ) -> StoreResult<()> {
        // 输入 commit 由上层准备；先统一验证 Artifact kind/lifecycle/CAS 可读性，再校验 sealed Outcome、
        // T5 Retrospective、Experience/Evaluation 与 candidate policy 的跨对象关联。
        for (artifact, kind) in [
            (&commit.outcome, ArtifactKind::Outcome),
            (&commit.final_retrospective, ArtifactKind::Retrospective),
            (&commit.experience, ArtifactKind::Experience),
            (&commit.evaluation, ArtifactKind::Evaluation),
        ] {
            artifact.validate()?;
            blob::read_blob_bytes_including_staged(
                connection,
                &artifact.blob.hash,
                artifact.blob.bytes,
            )?;
            if artifact.kind != kind || artifact.lifecycle != ArtifactLifecycle::Canonical {
                return Err(StoreError::InvalidLearningCommit(
                    "learning_artifact.kind_or_lifecycle",
                ));
            }
        }
        if let Some(candidate_policy) = &commit.candidate_policy {
            candidate_policy.validate()?;
            blob::read_blob_bytes_including_staged(
                connection,
                &candidate_policy.blob.hash,
                candidate_policy.blob.bytes,
            )?;
            if candidate_policy.kind != ArtifactKind::CandidatePolicy
                || candidate_policy.lifecycle != ArtifactLifecycle::Canonical
            {
                return Err(StoreError::InvalidLearningCommit(
                    "candidate_policy.kind_or_lifecycle",
                ));
            }
        }
        let outcome: Outcome =
            self.read_artifact_payload_with_connection(connection, &commit.outcome)?;
        outcome.validate()?;
        if !outcome.is_sealed() {
            return Err(StoreError::UnsealedOutcome(
                commit.outcome.artifact_id.clone(),
            ));
        }
        let schedule =
            self.read_outcome_schedule_with_connection(connection, &outcome, &[RunPurpose::Paper])?;
        let final_retrospective: akzio_domain::Retrospective =
            self.read_artifact_payload_with_connection(connection, &commit.final_retrospective)?;
        final_retrospective.validate()?;
        let experience_payload: akzio_domain::Experience =
            self.read_artifact_payload_with_connection(connection, &commit.experience)?;
        // 对改变策略且非 restriction-only 的 transition，必须有完整、满足资格布尔项的 evaluation_context；
        // 这不是 Store 重新计算学习指标，而是检查上游 Rust 计算出的证明字段。
        if commit.from != commit.to
            && !commit.from.is_restriction_to(commit.to)
            && experience_payload
                .evaluation_context
                .as_ref()
                .is_none_or(|context| {
                    context.version != 1
                        || !context.learning_eligible
                        || !context.research_sufficient
                        || !context.market_window_complete
                        || !context.retrospective_valid
                        || !context.risk_ground_truth_measured
                })
        {
            return Err(StoreError::InvalidLearningCommit(
                "learning_eligibility_unproven",
            ));
        }

        if let Some(context) = &experience_payload.evaluation_context {
            // producer_workflow Artifact 可为共享 CAS；其 run/revision owner 通过当前 Run 的 revision 行证明。
            let workflow = read_required_artifact(
                connection,
                &context.producer_workflow,
                "experience.producer_workflow",
            )?;
            let graph: WorkflowGraph =
                self.read_artifact_payload_with_connection(connection, &workflow)?;
            let revision = self.workflow_revision_with_connection(
                connection,
                &commit.permit.run_id,
                context.producer_workflow_revision,
            )?;
            if context.producer_run_id != commit.permit.run_id
                // Compiled graphs are CAS values and need not carry a Run
                // origin. The durable Run/revision binding owns that identity.
                || revision.graph_artifact.artifact_id != workflow.artifact_id
                || graph.topology_id != experience_payload.topology_id.0
                || context.metric_basis != outcome.metric_basis
            {
                return Err(StoreError::InvalidLearningCommit(
                    "experience.producer_identity",
                ));
            }
        }

        if final_retrospective.horizon != OutcomeHorizon::T5
            || final_retrospective.status != akzio_domain::RetrospectiveStatus::Complete
            || final_retrospective.outcome.artifact_id != commit.outcome.artifact_id
            || final_retrospective.outcome.kind != ArtifactKind::Outcome
        {
            return Err(StoreError::InvalidLearningCommit(
                "learning_artifact.final_retrospective",
            ));
        }
        // 复读 typed Experience/Evaluation payload 并校验领域结构后，再比对三者的 source/provenance 闭包。
        let experience: Experience =
            self.read_artifact_payload_with_connection(connection, &commit.experience)?;
        experience.validate()?;
        let evaluation: Evaluation =
            self.read_artifact_payload_with_connection(connection, &commit.evaluation)?;
        evaluation.validate()?;

        for reference in std::iter::once(&outcome.schedule)
            .chain(outcome.market_evidence.iter())
            .chain([
                &experience.decision,
                &experience.decision_context,
                &experience.execution_context,
                &experience.policy_verdict,
            ])
        {
            // 这些输入 refs 必须指向声明 kind 且属于 canonical Paper Run；来源不合格阻止整次事务。
            let source = read_artifact(connection, &reference.artifact_id)?;
            if source.kind != reference.kind {
                return Err(StoreError::InvalidLearningCommit(
                    "learning_artifact.source_kind",
                ));
            }
            assert_artifact_from_paper_with_connection(connection, &source)?;
        }

        let outcome_ref = ArtifactRef {
            artifact_id: commit.outcome.artifact_id.clone(),
            kind: ArtifactKind::Outcome,
        };
        let experience_ref = ArtifactRef {
            artifact_id: commit.experience.artifact_id.clone(),
            kind: ArtifactKind::Experience,
        };
        let evaluation_ref = ArtifactRef {
            artifact_id: commit.evaluation.artifact_id.clone(),
            kind: ArtifactKind::Evaluation,
        };
        let retrospective_ref = ArtifactRef {
            artifact_id: commit.final_retrospective.artifact_id.clone(),
            kind: ArtifactKind::Retrospective,
        };
        if !commit
            .final_retrospective
            .source_refs
            .contains(&outcome_ref)
        {
            return Err(StoreError::InvalidLearningCommit(
                "learning_artifact.final_retrospective_source_refs",
            ));
        }
        match (&commit.subject, &commit.candidate_policy) {
            (PolicySubject::Memory(_), None) => {}
            (PolicySubject::Memory(_), Some(_)) => {
                return Err(StoreError::InvalidLearningCommit(
                    "candidate_policy.memory_subject",
                ));
            }
            (PolicySubject::Contract(_) | PolicySubject::Topology(_), None) => {
                return Err(StoreError::InvalidLearningCommit(
                    "candidate_policy.missing",
                ));
            }
            (PolicySubject::Contract(_) | PolicySubject::Topology(_), Some(artifact)) => {
                let candidate_policy: CandidatePolicy =
                    self.read_artifact_payload_with_connection(connection, artifact)?;
                candidate_policy.validate()?;
                if candidate_policy.subject != commit.subject
                    || candidate_policy.source_evaluation != evaluation_ref
                    || candidate_policy.created_at != commit.completed_at
                    || !has_exact_source_refs(
                        artifact,
                        &[
                            candidate_policy.baseline.clone(),
                            candidate_policy.candidate.clone(),
                            candidate_policy.source_evaluation.clone(),
                        ],
                    )
                {
                    return Err(StoreError::InvalidLearningCommit("candidate_policy.links"));
                }
                self.validate_candidate_policy_sources(connection, &candidate_policy)?;
            }
        }
        commit.subject.validate()?;
        if !commit.subject.accepts_state(commit.from) || !commit.subject.accepts_state(commit.to) {
            return Err(StoreError::InvalidLearningCommit(
                "policy_evaluation.subject_state",
            ));
        }
        let transition_matches = match &commit.transition {
            Some(transition) => {
                transition.validate()?;
                transition.subject == commit.subject
                    && transition.from == commit.from
                    && transition.to == commit.to
                    && transition.evaluation == evaluation_ref
                    && transition.created_at == commit.completed_at
            }
            None => commit.from == commit.to,
        };
        // 最后一组精确 link 检查把 Outcome、Experience、Evaluation、Retrospective、Policy state 串起来；
        // source refs 用集合比较并拒绝重复，不按输入顺序产生差异。
        if experience.outcome != outcome_ref
            || evaluation.outcome != outcome_ref
            || evaluation.experience != experience_ref
            || !transition_matches
            || experience.subject != commit.subject
            || experience.policy_state != commit.from
            || experience.decision != schedule.decision
            || experience.decision_context != schedule.decision_context
            || experience.execution_context != schedule.execution_context
        {
            return Err(StoreError::InvalidLearningCommit("learning_artifact.links"));
        }
        if !has_exact_source_refs(
            &commit.outcome,
            &std::iter::once(outcome.schedule.clone())
                .chain(outcome.market_evidence.iter().cloned())
                .chain(outcome.risk_ground_truth_refs())
                .collect::<Vec<_>>(),
        ) || !has_exact_source_refs(
            &commit.experience,
            &[
                experience.decision.clone(),
                experience.decision_context.clone(),
                experience.execution_context.clone(),
                experience.policy_verdict.clone(),
                experience.outcome.clone(),
                retrospective_ref.clone(),
            ]
            .into_iter()
            .chain(
                experience
                    .evaluation_context
                    .as_ref()
                    .map(|context| context.producer_workflow.clone()),
            )
            .collect::<Vec<_>>(),
        ) || !has_exact_source_refs(
            &commit.evaluation,
            &[
                evaluation.outcome.clone(),
                evaluation.experience,
                retrospective_ref,
            ],
        ) {
            return Err(StoreError::InvalidLearningCommit(
                "learning_artifact.source_refs",
            ));
        }
        Ok(())
    }

    // 按 subject 区分 Contract 与 Topology candidate，核对 lifecycle、purpose、hash 和 bounded capability。
    // 本 helper 已收到 Connection，却仍用 `self.read_artifact_payload` 解码 baseline/candidate；
    // 从写事务或 Doctor 路径调用时存在同线程非重入连接错误的源码风险，
    // 区别于真正的领域拒绝，尚未据此断言实际故障。
    fn validate_candidate_policy_sources(
        &self,
        connection: &Connection,
        policy: &CandidatePolicy,
    ) -> StoreResult<()> {
        // 两个 ArtifactRef 先核验其 ID/kind；Contract subject 走 hash/能力子集校验，
        // Topology subject 走 typed graph、candidate ID 和 Paper/Shadow purpose 校验。
        let baseline =
            read_required_artifact(connection, &policy.baseline, "candidate_policy.baseline")?;
        let candidate =
            read_required_artifact(connection, &policy.candidate, "candidate_policy.candidate")?;
        match &policy.subject {
            PolicySubject::Memory(_) => Err(StoreError::InvalidLearningCommit(
                "candidate_policy.memory_subject",
            )),
            PolicySubject::Contract(candidate_hash) => {
                if baseline.lifecycle != ArtifactLifecycle::Canonical
                    || candidate.lifecycle != ArtifactLifecycle::Canonical
                {
                    return Err(StoreError::InvalidLearningCommit(
                        "candidate_policy.contract_lifecycle",
                    ));
                }
                let baseline_contract: AgentContract = self.read_artifact_payload(&baseline)?;
                let candidate_contract: AgentContract = self.read_artifact_payload(&candidate)?;
                baseline_contract.validate()?;
                candidate_contract.validate()?;
                if &candidate_contract.contract_hash != candidate_hash
                    || !baseline_contract.permits_candidate(&candidate_contract)
                {
                    return Err(StoreError::InvalidLearningCommit(
                        "candidate_policy.contract_binding",
                    ));
                }
                Ok(())
            }
            PolicySubject::Topology(topology_id) => {
                let baseline_graph: WorkflowGraph = self.read_artifact_payload(&baseline)?;
                let candidate_graph: WorkflowGraph = self.read_artifact_payload(&candidate)?;
                baseline_graph.validate()?;
                candidate_graph.validate()?;
                if candidate_graph.topology_id != topology_id.0
                    || workflow_graph_run_purpose(connection, &baseline.artifact_id)?
                        != RunPurpose::Paper
                    || workflow_graph_run_purpose(connection, &candidate.artifact_id)?
                        != RunPurpose::Shadow
                {
                    return Err(StoreError::InvalidLearningCommit(
                        "candidate_policy.topology_binding",
                    ));
                }
                Ok(())
            }
        }
    }

    // Shadow pair 必须同时指向 canonical parent、允许的 candidate 和共同 execution context。
    fn assert_shadow_pair_sources_with_connection(
        &self,
        connection: &Connection,
        completion: &ShadowPairCompletion,
    ) -> StoreResult<()> {
        // 读取 completion 五个 source refs，再分别套 canonical Paper 与可接受 Shadow candidate 约束；
        // 最后解码两份 sealed Outcome 并要求它们共享同一个冻结 schedule/execution context。
        let parent_decision = read_required_artifact(
            connection,
            &completion.parent_decision,
            "shadow_pair.parent_decision",
        )?;
        let execution_context = read_required_artifact(
            connection,
            &completion.execution_context,
            "shadow_pair.execution_context",
        )?;
        let candidate_decision = read_required_artifact(
            connection,
            &completion.candidate_decision,
            "shadow_pair.candidate_decision",
        )?;
        let parent_outcome_artifact = read_required_artifact(
            connection,
            &completion.parent_outcome,
            "shadow_pair.parent_outcome",
        )?;
        let candidate_outcome_artifact = read_required_artifact(
            connection,
            &completion.candidate_outcome,
            "shadow_pair.candidate_outcome",
        )?;

        assert_canonical_paper_artifact(connection, &parent_decision)?;
        assert_artifact_from_paper_with_connection(connection, &execution_context)?;
        assert_canonical_paper_artifact(connection, &parent_outcome_artifact)?;
        assert_shadow_candidate_artifact(connection, &candidate_decision)?;
        assert_shadow_candidate_artifact(connection, &candidate_outcome_artifact)?;
        assert_candidate_decision_binding(connection, &candidate_decision, completion)?;

        let parent_outcome: Outcome =
            serde_json::from_slice(&blob::read_blob_with(connection, &parent_outcome_artifact.blob)?)?;
        let candidate_outcome: Outcome =
            serde_json::from_slice(&blob::read_blob_with(connection, &candidate_outcome_artifact.blob)?)?;
        parent_outcome.validate_sealed()?;
        candidate_outcome.validate_sealed()?;

        let parent_schedule = self.read_outcome_schedule_with_connection(
            connection,
            &parent_outcome,
            &[RunPurpose::Paper],
        )?;
        let candidate_schedule = self.read_outcome_schedule_with_connection(
            connection,
            &candidate_outcome,
            &[RunPurpose::Paper, RunPurpose::Shadow],
        )?;
        if parent_schedule.decision != completion.parent_decision
            || candidate_schedule.decision != completion.candidate_decision
            || parent_schedule.execution_context != completion.execution_context
            || candidate_schedule.execution_context != completion.execution_context
        {
            return Err(StoreError::InvalidLearningCommit(
                "shadow_pair.schedule_binding",
            ));
        }
        Ok(())
    }

    // 无事务调用方通过 Store 连接读取 durable/staged payload；调用方已持有连接时使用下一个 helper。
    // T: DeserializeOwned 是“对任意输入生命周期都能反序列化”的拥有型结果边界；
    // 具体 T 在编译期静态实例化，返回 T 不借用临时 Vec<u8>，JSON/CAS 错误转换为 StoreError。
    fn read_artifact_payload<T: DeserializeOwned>(&self, artifact: &Artifact) -> StoreResult<T> {
        Ok(serde_json::from_slice(&self.read_blob(&artifact.blob)?)?)
    }

    // 复用调用方 Connection，确保事务内可见 staged blob 与未提交 Artifact。
    // 同样要求 DeserializeOwned 并对具体 T 静态分发，但通过传入连接读取 TEMP staging；
    // 不会再次获取 mutex 或自行 commit。
    pub(super) fn read_artifact_payload_with_connection<T: DeserializeOwned>(
        &self,
        connection: &Connection,
        artifact: &Artifact,
    ) -> StoreResult<T> {
        Ok(serde_json::from_slice(
            &blob::read_blob_bytes_including_staged(
                connection,
                &artifact.blob.hash,
                artifact.blob.bytes,
            )?,
        )?)
    }

    // 从 Outcome schedule 引用出发校验 purpose/lifecycle、source refs 和 execution lineage。
    fn read_outcome_schedule_with_connection(
        &self,
        connection: &Connection,
        outcome: &Outcome,
        allowed_purposes: &[RunPurpose],
    ) -> StoreResult<OutcomeSchedule> {
        // 先确认 ref/kind、Run purpose 与 canonical/RunScoped lifecycle，再借用同一连接解码 schedule，
        // 精确核对 expected source refs 和每个来源的 purpose。
        if outcome.schedule.kind != ArtifactKind::OutcomeSchedule {
            return Err(StoreError::InvalidLearningCommit("outcome.schedule_kind"));
        }
        let schedule_artifact = read_artifact(connection, &outcome.schedule.artifact_id)?;
        if schedule_artifact.kind != ArtifactKind::OutcomeSchedule {
            return Err(StoreError::InvalidLearningCommit(
                "outcome.schedule_artifact",
            ));
        }
        let schedule_purpose = artifact_run_purpose(connection, &schedule_artifact)?;
        let expected_lifecycle = match schedule_purpose {
            RunPurpose::Paper => ArtifactLifecycle::Canonical,
            RunPurpose::Shadow => ArtifactLifecycle::RunScoped,
            _ => {
                return Err(StoreError::InvalidLearningCommit(
                    "outcome.schedule_artifact",
                ));
            }
        };
        if schedule_artifact.lifecycle != expected_lifecycle {
            return Err(StoreError::InvalidLearningCommit(
                "outcome.schedule_artifact",
            ));
        }
        assert_artifact_from_allowed_purposes(connection, &schedule_artifact, allowed_purposes)?;
        let schedule: OutcomeSchedule =
            self.read_artifact_payload_with_connection(connection, &schedule_artifact)?;
        schedule.validate()?;
        if schedule.outcome_id != outcome.outcome_id {
            return Err(StoreError::InvalidLearningCommit(
                "outcome.schedule_identity",
            ));
        }

        let expected = outcome_schedule_source_refs(&schedule);
        if !has_exact_source_refs(&schedule_artifact, &expected) {
            return Err(StoreError::InvalidLearningCommit(
                "outcome_schedule.source_refs",
            ));
        }
        for reference in &expected {
            let artifact = read_artifact(connection, &reference.artifact_id)?;
            if artifact.kind != reference.kind {
                return Err(StoreError::InvalidLearningCommit(
                    "outcome_schedule.source_kind",
                ));
            }
            assert_artifact_from_allowed_purposes(connection, &artifact, allowed_purposes)?;
        }
        self.validate_outcome_schedule_execution_lineage(connection, &schedule, allowed_purposes)?;
        Ok(schedule)
    }

    // NoOrder 与 ReconciledPaper 分支分别验证 verdict/context，或 commitment/reconciliation receipt 链。
    fn validate_outcome_schedule_execution_lineage(
        &self,
        connection: &Connection,
        schedule: &OutcomeSchedule,
        allowed_purposes: &[RunPurpose],
    ) -> StoreResult<()> {
        // 两个 execution enum 分支共用同一 ExecutionVerdict 读取；NoOrder 只需 verdict 指向冻结 context，
        // ReconciledPaper 还要逐层验证 Commitment 和 Reconciliation 的 Artifact kind/purpose/source。
        let verdict_ref = match &schedule.execution {
            OutcomeExecutionLineage::NoOrder { execution_verdict }
            | OutcomeExecutionLineage::ReconciledPaper {
                execution_verdict, ..
            } => execution_verdict,
        };
        let verdict_artifact = read_artifact(connection, &verdict_ref.artifact_id)?;
        if verdict_artifact.kind != ArtifactKind::ExecutionVerdict {
            return Err(StoreError::InvalidLearningCommit(
                "outcome_schedule.execution_verdict_kind",
            ));
        }
        assert_artifact_from_allowed_purposes(connection, &verdict_artifact, allowed_purposes)?;
        let verdict: ExecutionVerdict =
            serde_json::from_slice(&blob::read_blob_with(connection, &verdict_artifact.blob)?)?;
        verdict.validate()?;

        match (&schedule.execution, verdict) {
            (
                OutcomeExecutionLineage::NoOrder { execution_verdict },
                ExecutionVerdict::NoOrder { no_order },
            ) if execution_verdict == verdict_ref
                && no_order.execution_context == schedule.execution_context =>
            {
                // NoOrder 路径检查 verdict source refs 含冻结 context；没有 commitment/receipt 要求。
                if !verdict_artifact
                    .source_refs
                    .iter()
                    .any(|reference| reference == &schedule.execution_context)
                {
                    return Err(StoreError::InvalidLearningCommit(
                        "outcome_schedule.no_order_context",
                    ));
                }
            }
            (
                OutcomeExecutionLineage::ReconciledPaper {
                    execution_verdict,
                    commitment,
                    reconciliation,
                },
                ExecutionVerdict::Accepted { execution_context },
            ) if execution_verdict == verdict_ref
                && execution_context == schedule.execution_context =>
            {
                // Accepted 路径先核对 commitment 的 context/verdict 来源，再核对 reconciliation 对应 commitment。
                let commitment_artifact = read_artifact(connection, &commitment.artifact_id)?;
                if commitment_artifact.kind != ArtifactKind::ExecutionCommitment {
                    return Err(StoreError::InvalidLearningCommit(
                        "outcome_schedule.commitment_kind",
                    ));
                }
                assert_artifact_from_allowed_purposes(
                    connection,
                    &commitment_artifact,
                    allowed_purposes,
                )?;
                let commitment_payload: PaperCommitment =
                    serde_json::from_slice(&blob::read_blob_with(connection, &commitment_artifact.blob)?)?;
                commitment_payload.validate()?;
                if commitment_payload.execution_context != schedule.execution_context
                    || !commitment_artifact
                        .source_refs
                        .iter()
                        .any(|reference| reference == execution_verdict)
                {
                    return Err(StoreError::InvalidLearningCommit(
                        "outcome_schedule.commitment_lineage",
                    ));
                }

                let reconciliation_artifact =
                    read_artifact(connection, &reconciliation.artifact_id)?;
                if reconciliation_artifact.kind != ArtifactKind::Reconciliation {
                    return Err(StoreError::InvalidLearningCommit(
                        "outcome_schedule.reconciliation_kind",
                    ));
                }
                assert_artifact_from_allowed_purposes(
                    connection,
                    &reconciliation_artifact,
                    allowed_purposes,
                )?;
                let reconciliation_payload: Reconciliation =
                    serde_json::from_slice(&blob::read_blob_with(connection, &reconciliation_artifact.blob)?)?;
                reconciliation_payload.validate()?;
                if reconciliation_payload.commitment != *commitment
                    || !reconciliation_artifact
                        .source_refs
                        .iter()
                        .any(|reference| reference == commitment)
                {
                    return Err(StoreError::InvalidLearningCommit(
                        "outcome_schedule.reconciliation_lineage",
                    ));
                }
            }
            _ => {
                return Err(StoreError::InvalidLearningCommit(
                    "outcome_schedule.execution_lineage",
                ));
            }
        }
        Ok(())
    }
}
