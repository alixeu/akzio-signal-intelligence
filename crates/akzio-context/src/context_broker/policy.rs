impl ContextBroker {
    // 检查单个 Artifact 是否同时满足 ContextPolicy、内部 producer/kind allowlist 和
    // 特殊类型规则；RawEvidence 永远不能直接进入 Manifest，ExPost regime 也不能作为
    // Decision-time Context。该函数只拒绝输入，不修改 Policy 或 Artifact。
    fn assert_context_permitted(
        &self,
        policy: &ContextPolicy,
        artifact: &Artifact,
    ) -> ContextResult<()> {
        if artifact.kind == ArtifactKind::RawEvidence {
            return Err(ContextError::RawEvidenceInManifest);
        }
        if artifact.kind == ArtifactKind::RegimeSnapshot {
            let snapshot: RegimeSnapshot = self.read_payload(artifact)?;
            snapshot.validate()?;
            if snapshot.classification_kind == RegimeClassificationKind::ExPost {
                return Err(ContextError::ForbiddenArtifact {
                    artifact_id: artifact.artifact_id.clone(),
                });
            }
        }
        if !policy.permitted_kinds.contains(&artifact.kind)
            || !governed_internal_source(artifact)
            || (!policy.permitted_source_families.is_empty()
                && !policy
                    .permitted_source_families
                    .contains(&artifact.provenance.source_family))
        {
            return Err(ContextError::ForbiddenArtifact {
                artifact_id: artifact.artifact_id.clone(),
            });
        }
        Ok(())
    }

    fn assert_context_run(
        &self,
        permit: &TaskWritePermit,
        artifact: &Artifact,
    ) -> ContextResult<()> {
        // 普通 RunScoped 材料必须属于当前 Run；Lesson/Experience/CandidatePolicy 通过
        // 独立 overlay 资格，canary 父证据则只能命中 Store 登记的冻结复用关系。
        if artifact.kind == ArtifactKind::NormalizedEvidence
            && self.store.is_canary_parent_evidence(&permit.run_id,&artifact.artifact_id)?
        {
            return Ok(());
        }
        if matches!(
            artifact.kind,
            ArtifactKind::Lesson | ArtifactKind::Experience | ArtifactKind::CandidatePolicy
        ) {
            if self.overlay_is_eligible(artifact)? {
                return Ok(());
            }
        } else if artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.as_ref())
            == Some(&permit.run_id)
        {
            return Ok(());
        }
        Err(ContextError::ForbiddenArtifact {
            artifact_id: artifact.artifact_id.clone(),
        })
    }

    fn overlay_is_eligible(&self, artifact: &Artifact) -> ContextResult<bool> {
        // Lesson 依据 payload/lifecycle 判断；Experience/CandidatePolicy 还要满足记录过的
        // influence subject、canonical learning 来源和当前 Policy head。这里只读资格，不激活/更新 head。
        match artifact.kind {
            ArtifactKind::Lesson => {
                let lesson: Lesson = self.read_payload(artifact)?;
                lesson.validate()?;
                Ok(lesson.lifecycle == LessonLifecycle::Active)
            }
            ArtifactKind::Experience => {
                if !self.is_canonical_paper_artifact(artifact)? {
                    return Ok(false);
                }
                let experience: Experience = self.read_payload(artifact)?;
                experience.validate()?;
                if experience
                    .evaluation_context
                    .as_ref()
                    .is_none_or(|context| !context.learning_eligible)
                    || self
                        .store
                        .recorded_policy_influence_subject(&artifact.artifact_id)?
                        .as_ref()
                        != Some(&experience.subject)
                {
                    return Ok(false);
                }
                Ok(self
                    .store
                    .policy_head(&experience.subject)?
                    .is_some_and(|head| overlay_state_is_eligible(artifact.kind, head.state)))
            }
            ArtifactKind::CandidatePolicy => {
                if !self.is_canonical_paper_artifact(artifact)? {
                    return Ok(false);
                }
                let candidate: CandidatePolicy = self.read_payload(artifact)?;
                candidate.validate()?;
                if self
                    .store
                    .recorded_policy_influence_subject(&artifact.artifact_id)?
                    .as_ref()
                    != Some(&candidate.subject)
                {
                    return Ok(false);
                }
                let evaluation = self
                    .store
                    .artifact(&candidate.source_evaluation.artifact_id)?;
                if evaluation.kind != ArtifactKind::Evaluation
                    || !self.is_canonical_paper_artifact(&evaluation)?
                {
                    return Ok(false);
                }
                let evaluation_payload: akzio_domain::Evaluation =
                    self.read_payload(&evaluation)?;
                let experience_artifact = self
                    .store
                    .artifact(&evaluation_payload.experience.artifact_id)?;
                if experience_artifact.kind != ArtifactKind::Experience
                    || !self.is_canonical_paper_artifact(&experience_artifact)?
                {
                    return Ok(false);
                }
                let experience: Experience = self.read_payload(&experience_artifact)?;
                experience.validate()?;
                if experience.subject != candidate.subject
                    || experience
                        .evaluation_context
                        .as_ref()
                        .is_none_or(|context| !context.learning_eligible)
                    || self
                        .store
                        .recorded_policy_influence_subject(&experience_artifact.artifact_id)?
                        .as_ref()
                        != Some(&candidate.subject)
                {
                    return Ok(false);
                }
                Ok(self
                    .store
                    .policy_head(&candidate.subject)?
                    .is_some_and(|head| overlay_state_is_eligible(artifact.kind, head.state)))
            }
            _ => Ok(true),
        }
    }

    fn is_canonical_paper_artifact(&self, artifact: &Artifact) -> ContextResult<bool> {
        // CandidatePolicy/Experience 只能引用 Canonical 且属于 canonical learning Run 的
        // Artifact；生命周期或 RunPurpose 不符合时直接退出，避免隔离/调试数据进入正式学习。
        if artifact.lifecycle != ArtifactLifecycle::Canonical {
            return Ok(false);
        }
        let Some(run_id) = artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.as_ref())
        else {
            return Ok(false);
        };
        Ok(self.store.run_purpose(run_id)?.is_canonical_learning())
    }

    fn read_payload<T: DeserializeOwned>(&self, artifact: &Artifact) -> ContextResult<T> {
        // 所有 typed payload 都从 Artifact 的 CAS blob 解码；本 helper 不授予权限，调用方
        // 负责先完成 Artifact/Grant 边界校验，serde 错误原样转成 ContextError。
        Ok(serde_json::from_slice(
            &self.store.read_blob(&artifact.blob)?,
        )?)
    }

    fn raw_closure(
        &self,
        policy: &ContextPolicy,
        selections: &[ContextSelection],
    ) -> ContextResult<BTreeSet<ArtifactId>> {
        // 从已选 Artifact 沿 source_refs 做有界去重遍历，只收集符合 source family 的
        // RawEvidence；allow_raw_reread=false 时返回空集合，普通 Manifest 仍不暴露原文。
        if !policy.allow_raw_reread {
            return Ok(BTreeSet::new());
        }
        let mut closure = BTreeSet::new();
        let mut queue = selections
            .iter()
            .map(|selection| selection.artifact.artifact_id.clone())
            .collect::<VecDeque<_>>();
        let mut seen = BTreeSet::new();
        while let Some(artifact_id) = queue.pop_front() {
            if !seen.insert(artifact_id.clone()) {
                continue;
            }
            let artifact = self.store.artifact(&artifact_id)?;
            // 非 RawEvidence 的来源继续入队，RawEvidence 只记录 ID，不会继续展开其内部
            // 引用，因此闭包既不重复读取也不跨越原始证据边界。
            for source in artifact.source_refs {
                let source_artifact = self.store.artifact(&source.artifact_id)?;
                if source_artifact.kind == ArtifactKind::RawEvidence {
                    if policy.permitted_source_families.is_empty()
                        || policy
                            .permitted_source_families
                            .contains(&source_artifact.provenance.source_family)
                    {
                        closure.insert(source_artifact.artifact_id);
                    }
                } else {
                    queue.push_back(source_artifact.artifact_id);
                }
            }
        }
        Ok(closure)
    }
}

/// Exact internal producer/kind pairs. External evidence retains the contract's
/// source allowlist and untrusted-content policy; no namespace wildcard grants.
fn governed_internal_source(artifact: &Artifact) -> bool {
    // 这里是精确的内部 producer/kind 配对；未知 source family 只允许基础外部证据类型，
    // 最终仍要经过 ContextPolicy 的 source allowlist 和不可信内容隔离，不能依赖命名空间通配。
    use ArtifactKind::*;
    match artifact.provenance.source_family.as_str() {
        "akzio.ingest" => {
            artifact.kind == SemanticDetail
                && matches!(
                    artifact.producer.as_str(),
                    "evidence.collection_status"
                        | "canary.evidence_snapshot"
                        | "evidence.option_projection" | "research.supplement.result"
                )
        }
        "akzio.agent" => {
            matches!(
                (artifact.kind, artifact.producer.as_str()),
                (Claim, "agent.research.analyst")
                    | (Critique, "agent.research.critic")
                    | (DecisionProposal, "agent.research.synthesizer")
                    | (ProposalReview, "agent.research.proposal_reviewer")
                    | (RetrospectiveDraft, "agent.learning.outcome_worker")
            ) || (artifact.kind == DeliberationNote
                && artifact.provenance.producer_contract_hash.is_some()
                && matches!(
                    artifact.producer.as_str(),
                    "agent.deliberation.research.analyst"
                        | "agent.deliberation.research.critic"
                        | "agent.deliberation.research.synthesizer"
                        | "agent.deliberation.learning.outcome_worker"
                ))
        }
        "akzio.execution" => matches!(
            (artifact.kind, artifact.producer.as_str()),
            (Decision, "decision.bound")
                | (DecisionContext, "decision.context")
                | (ExecutionContext, "execution.context")
                | (ExecutionVerdict, "execution.verdict")
                | (ExecutionCommitment, "execution.paper_commitment")
                | (ExecutionPlan, "execution.plan")
                | (OrderReceipt, "execution.order_receipt")
                | (Reconciliation, "execution.reconciliation")
        ),
        "akzio-learning" => {
            (artifact.kind == SemanticDetail && artifact.producer == "learning.outcome_stage")
                || (artifact.kind == OutcomeSchedule
                    && artifact.producer == "learning.outcome_schedule")
                || (matches!(
                    artifact.kind,
                    Outcome | Retrospective | Experience | Evaluation | CandidatePolicy
                ) && artifact.producer == "akzio-learning.evaluation")
        }
        "akzio.learning" | "akzio.operator" => {
            artifact.kind == Lesson
                && matches!(
                    artifact.producer.as_str(),
                    "learning.lesson.outcome"
                        | "learning.lesson.operator"
                        | "learning.lesson.lifecycle"
                )
        }
        _ => matches!(
            artifact.kind,
            NormalizedEvidence | SemanticDetail | RegimeSnapshot
        ),
    }
}
