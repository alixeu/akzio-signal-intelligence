// 文件导读：这里实现有界的 reviewed research loop：按冻结 WorkflowNode 的 horizon/
// revision 选择 Claim/Critique/Proposal/Review，必要时记录 revision stop，最多复用一次
// supplemental round 的新事实。模型调用成功只完成研究节点；ProposalReview、DecisionGate
// 和后续 Execution/Paper/Outcome 仍是独立阶段。
// Rust 机制：Artifact 借用先转换为轻量 `ArtifactRef`；BTreeMap/Set 形成稳定候选闭包；
// `AgentRunBudget` 的可变借用跨一次执行传递，`Option`/`Result` 区分跳过、阻断与错误。

use crate::*;
use akzio_domain::ResearchCritique;
use akzio_domain::{ProposalReview, SupplementalRound};

// 只复制 Artifact 的身份信息，避免把 CAS payload 或未授权原文带入候选构造过程。
fn reference(artifact: &Artifact) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: artifact.kind,
    }
}

fn supplement_input_kind(kind: ArtifactKind) -> bool {
    matches!(
        kind,
        ArtifactKind::ContextManifest
            | ArtifactKind::Claim
            | ArtifactKind::Critique
            | ArtifactKind::DecisionProposal
            | ArtifactKind::ProposalReview
            | ArtifactKind::NormalizedEvidence
            | ArtifactKind::SemanticDetail
    )
}

impl Daemon {
    pub(crate) fn completed_supplement_for_request(
        &self,
        ancestors: &mut Vec<Artifact>,
    ) -> Result<(SupplementalRound, bool)> {
        let Some(record) = ancestors
            .iter()
            .find(|a| a.producer == "research.supplement.result")
        else {
            return Ok((SupplementalRound::default(), false));
        };
        let mut round: SupplementalRound = self.read_artifact_payload(&reference(record))?;
        if round.evidence.is_empty() {
            return Ok((round, false));
        }
        let original = ancestors
            .iter()
            .filter(|a| {
                a.kind == ArtifactKind::NormalizedEvidence
                    && !round.evidence.contains(&reference(a))
            })
            .map(|a| self.read_artifact_payload::<NormalizedEvidencePayload>(&reference(a)))
            .collect::<Result<Vec<_>>>()?;
        let cutoff = super::research_supplement::frozen_research_cutoff(&original)?;
        let mut excluded = BTreeSet::new();
        for evidence in &round.evidence {
            let receipt = ancestors
                .iter()
                .find(|artifact| reference(artifact) == *evidence);
            let same_producer_task = |a: &Artifact| {
                a.origin
                    .as_ref()
                    .zip(record.origin.as_ref())
                    .is_some_and(|(a, b)| a.run_id == b.run_id && a.task_id == b.task_id)
            };
            if evidence.kind != ArtifactKind::NormalizedEvidence
                || !record.source_refs.contains(evidence)
                || !receipt.is_some_and(same_producer_task)
            {
                return Err(DaemonError::InvalidInput(
                    "completed supplement evidence is outside its committed outputs".into(),
                ));
            }
            let payload: NormalizedEvidencePayload = self.read_artifact_payload(evidence)?;
            if let Some(reason) =
                super::research_supplement::supplemental_candidate_gap(&payload, cutoff)
            {
                tracing::warn!(artifact_id=%evidence.artifact_id, reason, "completed supplemental receipt excluded from new research request");
                excluded.insert(evidence.artifact_id.clone());
            }
        }
        if excluded.is_empty() {
            return Ok((round, false));
        }
        round
            .evidence
            .retain(|evidence| !excluded.contains(&evidence.artifact_id));
        round.affected_horizons.retain(|horizon| {
            round.dispositions.iter().any(|row| {
                row.horizon == *horizon
                    && row
                        .evidence
                        .iter()
                        .any(|evidence| round.evidence.contains(evidence))
            })
        });
        // The old summary still claims the rejected receipts were admissible. Keep it and its
        // dependent research in CAS, but leave them out of any new model request.
        excluded.insert(record.artifact_id.clone());
        let mut retained = Vec::with_capacity(ancestors.len());
        for artifact in std::mem::take(ancestors) {
            if !self.depends_on_excluded_supplement(&artifact, &excluded)? {
                retained.push(artifact);
            }
        }
        *ancestors = retained;
        Ok((round, true))
    }

    pub(crate) fn unavailable_supplemental_inputs(
        &self,
        task: &ClaimedAttempt,
        roots: impl IntoIterator<Item = ArtifactRef>,
    ) -> Result<BTreeSet<ArtifactId>> {
        // Shadow/Replay evaluate the original frozen evidence by reference.
        if !matches!(
            self.store.run_purpose(&task.run_id)?,
            RunPurpose::Paper | RunPurpose::PositionPlan
        ) {
            return Ok(BTreeSet::new());
        }
        let mut pending = roots.into_iter().collect::<Vec<_>>();
        let mut seen = BTreeSet::new();
        let mut receipts = Vec::new();
        while let Some(source) = pending.pop() {
            if !supplement_input_kind(source.kind) || !seen.insert(source.clone()) {
                continue;
            }
            let artifact = self.store.artifact(&source.artifact_id)?;
            if artifact.kind != source.kind {
                return Err(DaemonError::InvalidInput(
                    "invalid supplemental lineage kind".into(),
                ));
            }
            if artifact.kind == ArtifactKind::NormalizedEvidence {
                for source in artifact
                    .source_refs
                    .iter()
                    .filter(|r| r.kind == ArtifactKind::EvidenceNeed)
                {
                    let need = self.store.artifact(&source.artifact_id)?;
                    if need.producer != "agent.supplemental.evidence_need" {
                        continue;
                    }
                    let valid_origin = artifact
                        .origin
                        .as_ref()
                        .zip(need.origin.as_ref())
                        .is_some_and(|(a, n)| {
                            a.run_id.as_ref() == Some(&task.run_id)
                                && a.run_id == n.run_id
                                && a.task_id.is_some()
                                && a.task_id == n.task_id
                        });
                    let payload: NormalizedEvidencePayload =
                        self.read_artifact_payload(&reference(&artifact))?;
                    if need.kind != ArtifactKind::EvidenceNeed
                        || !valid_origin
                        || payload.need != *source
                    {
                        return Err(DaemonError::InvalidInput(
                            "invalid supplemental receipt lineage".into(),
                        ));
                    }
                    receipts.push((artifact.artifact_id.clone(), payload));
                }
            } else {
                pending.extend(self.supplement_input_sources(&artifact)?);
            }
        }
        if receipts.is_empty() {
            return Ok(BTreeSet::new());
        }

        // Use the successfully committed original Gate, never a downstream
        // receipt's self-reported cutoff or the resumed attempt's wall clock.
        let snapshot = self.store.workflow_snapshot(&task.run_id)?;
        let gate = snapshot
            .tasks
            .iter()
            .find(|stored| stored.node.recipe_id.as_str() == akzio_runtime::EVIDENCE_GATE_RECIPE_ID)
            .ok_or_else(|| DaemonError::InvalidInput("original EvidenceGate missing".into()))?;
        let original = self
            .store
            .committed_task_outputs(&task.run_id, &gate.node.task_id)?;
        let original = original
            .iter()
            .filter(|a| a.kind == ArtifactKind::NormalizedEvidence)
            .map(|a| self.read_artifact_payload(&reference(a)))
            .collect::<Result<Vec<NormalizedEvidencePayload>>>()?;
        let cutoff = super::research_supplement::frozen_research_cutoff(&original)?;
        let mut unavailable = BTreeSet::new();
        for (id, payload) in receipts {
            if let Some(reason) =
                super::research_supplement::supplemental_candidate_gap(&payload, cutoff)
            {
                tracing::warn!(artifact_id=%id, reason, "pre-upgrade supplemental receipt unavailable at T0");
                unavailable.insert(id);
            }
        }
        Ok(unavailable)
    }

    fn supplement_input_sources(&self, artifact: &Artifact) -> Result<Vec<ArtifactRef>> {
        if artifact.kind == ArtifactKind::ContextManifest {
            let manifest: akzio_domain::ContextManifestPayload =
                self.read_artifact_payload(&reference(artifact))?;
            return Ok(manifest
                .selections
                .into_iter()
                .map(|s| s.artifact)
                .collect());
        }
        if matches!(
            artifact.kind,
            ArtifactKind::Claim
                | ArtifactKind::Critique
                | ArtifactKind::DecisionProposal
                | ArtifactKind::ProposalReview
        ) || artifact.producer == "research.supplement.result"
        {
            return Ok(artifact.source_refs.clone());
        }
        Ok(Vec::new())
    }

    pub(crate) fn depends_on_excluded_supplement(
        &self,
        artifact: &Artifact,
        excluded: &BTreeSet<ArtifactId>,
    ) -> Result<bool> {
        if excluded.contains(&artifact.artifact_id) {
            return Ok(true);
        }
        let mut pending = self.supplement_input_sources(artifact)?;
        let mut seen = BTreeSet::new();
        while let Some(reference) = pending.pop() {
            if excluded.contains(&reference.artifact_id) {
                return Ok(true);
            }
            if supplement_input_kind(reference.kind) && seen.insert(reference.clone()) {
                let source = self.store.artifact(&reference.artifact_id)?;
                pending.extend(self.supplement_input_sources(&source)?);
            }
        }
        Ok(false)
    }

    // 执行 reviewed research contract 的有界研究循环：根据 revision、horizon、补采结果
    // 选择本轮输入，必要时写入 revision stop 标记，最后只调用一次受预算约束的 AgentRuntime。
    // 返回的 Succeeded/Skipped 仅描述研究节点状态，不等于 Decision 或 Execution 完成。
    pub(crate) async fn bounded_research(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        // ancestor_outputs 只返回已成功/跳过依赖的正式产物；当前 snapshot 用来解析
        // 每个产物所属 task 的 research_round/proposal_revision，而不是从 Artifact 猜版本。
        let mut ancestors = self.ancestor_outputs(task)?;
        let snapshot = self.store.workflow_snapshot(&task.run_id)?;
        if ancestors
            .iter()
            .any(|a| a.producer == "research.revision.stop")
        {
            // 已有持久化 stop 标记时不再启动新的模型调用，保留之前的拒绝审查结果。
            return Ok(TaskCompletion::Skipped);
        }
        // 通过 Artifact origin 回到冻结 WorkflowNode，供后续 revision/rerun 选择使用；
        // 找不到 origin 的 Artifact 不会被赋予虚构的 revision。
        let spec = |a: &Artifact| {
            a.origin
                .as_ref()
                .and_then(|o| o.task_id.as_ref())
                .and_then(|id| snapshot.tasks.iter().find(|t| &t.node.task_id == id))
                .map(|t| t.node.execution_spec())
        };
        let (round, degraded_supplement) = self.completed_supplement_for_request(&mut ancestors)?;
        let latest_review = ancestors
            .iter()
            .filter(|a| a.kind == ArtifactKind::ProposalReview)
            .max_by_key(|a| spec(a).and_then(|s| s.proposal_revision));
        if latest_review.is_some_and(|a| {
            self.read_artifact_payload::<ProposalReview>(&reference(a))
                .is_ok_and(|r| r.accepted())
        }) {
            // ProposalReviewer 已通过当前提案，后续 Synthesizer 修订节点只需跳过，避免
            // 在审查通过后继续产生另一个未经 Review 绑定的 Proposal。
            return Ok(TaskCompletion::Skipped);
        }
        if task.node.recipe_id.as_str() == akzio_domain::RESEARCH_SYNTHESIZER_RECIPE_ID {
            // Synthesizer 只有在存在至少两次按 proposal_revision 排序的 ProposalReview 时才检查拒绝意见是否停滞；
            // 比较的是持久化 proposal/review 内容，不是本次内存候选的顺序。
            let mut reviews = ancestors
                .iter()
                .filter(|a| a.kind == ArtifactKind::ProposalReview)
                .collect::<Vec<_>>();
            reviews.sort_by_key(|a| spec(a).and_then(|s| s.proposal_revision));
            if reviews.len() >= 2 {
                let last = reviews[reviews.len() - 1];
                let previous = reviews[reviews.len() - 2];
                let current_review: ProposalReview =
                    self.read_artifact_payload(&reference(last))?;
                let previous_review: ProposalReview =
                    self.read_artifact_payload(&reference(previous))?;
                let current = self.read_artifact_payload(&current_review.proposal)?;
                let prior = self.read_artifact_payload(&previous_review.proposal)?;
                if akzio_domain::proposal_revision_stagnated(
                    &prior,
                    &previous_review,
                    &current,
                    &current_review,
                ) {
                    // 相同拒绝范围达到停止条件时写入 RunScoped stop Artifact；该标记保留
                    // 审查血缘并明确 decision_authorized=false，绝不把停止当成 Decision。
                    let artifact = Artifact::new(ArtifactKind::SemanticDetail,
                        self.store.stage_json(&serde_json::json!({"reason":"unchanged_rejected_scopes",
                            "previous_review":reference(previous),"current_review":reference(last),
                            "issues":current_review.assessments.iter().flat_map(|a| a.issues.iter().map(move |i| i.stable_id(&a.scope))).collect::<Vec<_>>(),
                            "decision_authorized":false}))?, "research.revision.stop",ArtifactLifecycle::RunScoped,
                        ArtifactProvenance {source_family:"akzio.runtime".into(),observed_at:None,retrieved_at:now,
                            source_uri:None,confidence_ppm:1_000_000,producer_contract_hash:task.permit.contract_hash.clone()},
                        Some(task.permit.artifact_origin()),vec![reference(previous),reference(last)],now)?;
                    return Ok(TaskCompletion::Succeeded(vec![artifact]));
                }
            }
        }
        let round_artifact = ancestors
            .iter()
            .find(|a| a.producer == "research.supplement.result");
        let horizon = task.node.execution_spec().horizon;
        let refined = task.node.execution_spec().research_round == Some(1);
        if refined && !round.affected_horizons.iter().any(|h| Some(*h) == horizon) {
            // 补采只允许重跑受新增事实影响的 horizon；不相关的 rerun 显式跳过并不改变
            // 原 Claim/Critique 的 CAS 或把未受影响期限标为重新验证。
            return Ok(TaskCompletion::Skipped);
        }
        let mut selected_claims =
            BTreeMap::<akzio_domain::DecisionHorizon, (&Artifact, ResearchClaim)>::new();
        for artifact in ancestors.iter().filter(|a| a.kind == ArtifactKind::Claim) {
            // 每个 horizon 只保留 research_round 最大的 Claim；旧 Claim 仍留在 Store，
            // 但不会与新版本同时送入当前模型上下文。
            let claim: ResearchClaim = self.read_artifact_payload(&reference(artifact))?;
            let rank = spec(artifact)
                .and_then(|s| s.research_round)
                .unwrap_or_default();
            let replace = selected_claims
                .get(&claim.horizon)
                .is_none_or(|(previous, _)| {
                    rank > spec(previous)
                        .and_then(|s| s.research_round)
                        .unwrap_or_default()
                });
            if replace {
                selected_claims.insert(claim.horizon, (artifact, claim));
            }
        }
        let is_critic = task.node.recipe_id.as_str() == akzio_domain::RESEARCH_CRITIC_RECIPE_ID;
        if is_critic {
            // Critic 是单 horizon 节点；没有可结构化审查的 Claim 时以 Skipped 结束，
            // 不制造空 Critique。
            selected_claims.retain(|h, _| Some(*h) == horizon);
            if !should_run_structured_critique(
                &selected_claims
                    .values()
                    .map(|(_, c)| c.clone())
                    .collect::<Vec<_>>(),
            ) {
                return Ok(TaskCompletion::Skipped);
            }
        }
        let is_analyst = task.node.recipe_id.as_str() == akzio_domain::RESEARCH_ANALYST_RECIPE_ID;
        let is_reviewer =
            task.node.recipe_id.as_str() == akzio_domain::RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID;
        let mut seeds = Vec::new();
        if is_analyst {
            // Analyst 首轮从受治理的标准化证据/语义细节开始。补采 rerun 会按 resource
            // 替换旧 NormalizedEvidence，但只替换同一资源，其他来源和原 CAS 保持不变。
            seeds.extend(
                ancestors
                    .iter()
                    .filter(|a| {
                        matches!(
                            a.kind,
                            ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail
                        )
                    })
                    .map(reference),
            );
            // Updated evidence wins per resource for the rerun. Original CAS stays intact.
            if refined {
                // 先读取新补采 payload 的 resource，再过滤旧资源；read_artifact_payload
                // 失败向上返回，不能静默把旧事实当作新事实。
                let new_resources = round
                    .evidence
                    .iter()
                    .map(|r| {
                        self.read_artifact_payload::<serde_json::Value>(r)
                            .map(|v| v["resource"].clone())
                    })
                    .collect::<Result<Vec<_>>>()?;
                seeds.retain(|r| {
                    r.kind != ArtifactKind::NormalizedEvidence
                        || round.evidence.contains(r)
                        || self
                            .read_artifact_payload::<serde_json::Value>(r)
                            .is_ok_and(|v| !new_resources.contains(&v["resource"]))
                });
            }
        } else if is_reviewer {
            // Reviewer 只接受当前冻结 proposal_revision 的 DecisionProposal，避免审查
            // 旧提案或把其他修订的 Proposal 混入当前 revision。
            let current = task.node.execution_spec().proposal_revision;
            seeds.extend(
                ancestors
                    .iter()
                    .filter(|a| {
                        a.kind == ArtifactKind::DecisionProposal
                            && spec(a).and_then(|s| s.proposal_revision) == current
                    })
                    .map(reference),
            );
            if degraded_supplement && seeds.is_empty() {
                return Ok(TaskCompletion::Skipped);
            }
        } else {
            // Synthesizer 使用每个 horizon 的最新 Claim，并只吸收指向这些 Claim 的
            // Critique；ProposalReview/提案和 governed evidence 作为额外上下文加入。
            seeds.extend(selected_claims.values().map(|(a, _)| reference(a)));
            let claim_ids = seeds
                .iter()
                .map(|r| r.artifact_id.clone())
                .collect::<BTreeSet<_>>();
            if !is_critic {
                for a in ancestors
                    .iter()
                    .filter(|a| a.kind == ArtifactKind::Critique)
                {
                    let critique: ResearchCritique = self.read_artifact_payload(&reference(a))?;
                    if claim_ids.contains(&critique.target.artifact_id) {
                        seeds.push(reference(a));
                    }
                }
                if let Some(review) = latest_review {
                    seeds.push(reference(review));
                    let review: ProposalReview = self.read_artifact_payload(&reference(review))?;
                    seeds.push(review.proposal);
                }
            }
            // Calendar binding and optional context still use governed evidence.
            seeds.extend(
                ancestors
                    .iter()
                    .filter(|a| {
                        matches!(
                            a.kind,
                            ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail
                        )
                    })
                    .map(reference),
            );
        }
        if let Some(round) = round_artifact {
            // 把补采结果记录作为候选来源，使模型能看到处置状态和影响范围，而不仅是新事实。
            seeds.push(reference(round));
        }
        let mut selected = BTreeMap::new();
        while let Some(r) = seeds.pop() {
            // 以 ArtifactId 去重并沿允许的研究来源闭包向下展开；BTreeMap 最终给出稳定
            // 候选顺序，便于 ContextManifest 和模型输入审计复现。
            if selected.contains_key(&r.artifact_id) {
                continue;
            }
            let a = self.store.artifact(&r.artifact_id)?;
            if matches!(
                a.kind,
                ArtifactKind::Claim | ArtifactKind::Critique | ArtifactKind::DecisionProposal
            ) {
                // Proposal revisions do not import prior reviews or superseded claims.
                seeds.extend(
                    a.source_refs
                        .iter()
                        .filter(|r| {
                            matches!(
                                r.kind,
                                ArtifactKind::Claim
                                    | ArtifactKind::Critique
                                    | ArtifactKind::NormalizedEvidence
                                    | ArtifactKind::SemanticDetail
                            )
                        })
                        .cloned(),
                );
            }
            selected.insert(r.artifact_id.clone(), r);
        }
        let candidates = selected.into_values().collect::<Vec<_>>();
        if is_analyst {
            // Analyst 进入模型前必须通过 canonical evidence 覆盖检查；检查只报告缺口，
            // 不凭空补采或把缺口转换为中性/成功事实。
            super::research_run::ResearchRun::new(self)
                .validate_canonical_evidence_manifest(task, &candidates)?;
        }
        // AgentRunBudget 跨本轮受限调用传递给 AgentRuntime，累计重试/补采轮次的用量不
        // 在 daemon 层重置；最终 Artifact 由正式提交协议验证后返回。
        let mut budget = AgentRunBudget::new(&task.node.budget, &task.node.retry);
        let output = self
            .agent_session()
            .run(task, candidates, now, &mut budget)
            .await?;
        Ok(TaskCompletion::Succeeded(vec![output]))
    }
}
