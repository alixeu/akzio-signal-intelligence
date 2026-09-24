// 文件导读：ResearchRun 是任务类到研究协议的门面。它按安装 Contract 选择 reviewed
// bounded loop 或兼容的历史任务路径，先验证候选 Context，再调用 AgentRuntime；Analyst
// 的补采失败会保留首轮 Claim，不能把 refinement 失败说成证据已补齐或 Decision 已完成。
// Rust 机制：`&self`/`&ClaimedAttempt` 只读借用 Store/permit；async Future 通过 `await`
// 串接模型和 adapter；`matches!`/枚举分支表达 purpose，`Vec` 候选可扩展但不改 CAS 原文。

use crate::*;

pub(crate) struct ResearchRun<'a> {
    daemon: &'a Daemon,
}

impl<'a> ResearchRun<'a> {
    // 绑定当前 Daemon 的研究相关 Store、AgentSession 和 EvidenceAcquisition 能力。
    pub(crate) const fn new(daemon: &'a Daemon) -> Self {
        Self { daemon }
    }

    /// Executes the explicit research workflow contract, including at most one
    /// governed supplemental analyst round for a blocking Paper evidence gap.
    // 按 Contract 版本选择 reviewed bounded loop 或现有任务路径，完成候选构造、模型
    // 提交及最多一轮补采/Analyst 重跑；返回研究 Artifact，不把受理结果写成 Decision。
    pub(crate) async fn execute(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        if task
            .node
            .contract_hash
            .as_ref()
            .map(|hash| self.daemon.store.contract_installation(hash))
            .transpose()?
            .flatten()
            .is_some_and(|c| c.contract.version >= akzio_domain::REVIEWED_RESEARCH_CONTRACT_VERSION)
        {
            // reviewed contract 的 revision、horizon 和 ProposalReview 选择集中在有界循环；
            // 这里直接返回其任务结果，避免两套候选逻辑叠加。
            return self.daemon.bounded_research(task, now).await;
        }
        // 未达到 reviewed contract 阈值的已存任务沿用通用候选路径；仍经过 AgentSession
        // 的 Permit/Contract/Context 校验，不能因此恢复旧 Planner 的创建或执行能力。
        let candidates = self.daemon.agent_session().candidates(task)?;
        if task.node.recipe_id.as_str() == akzio_domain::RESEARCH_CRITIC_RECIPE_ID {
            // Critic 只有在 Claim 集合满足结构化审查条件时才调用模型；否则以 NoOutput
            // 结束本节点，保留其依赖 Claim 的正式状态。
            let claims = candidates
                .iter()
                .filter(|reference| reference.kind == ArtifactKind::Claim)
                .map(|reference| {
                    self.daemon
                        .read_artifact_payload::<ResearchClaim>(reference)
                })
                .collect::<Result<Vec<_>>>()?;
            if !should_run_structured_critique(&claims) {
                return Ok(TaskCompletion::NoOutput);
            }
        }
        if task.node.recipe_id.as_str() == akzio_domain::RESEARCH_ANALYST_RECIPE_ID {
            // Analyst 在模型调用前检查候选中的 canonical evidence 覆盖；缺口会被记录，
            // 不在此处绕过采集政策。
            self.validate_canonical_evidence_manifest(task, &candidates)?;
        }
        let mut agent_budget = AgentRunBudget::new(&task.node.budget, &task.node.retry);
        let output = self
            .daemon
            .agent_session()
            .run(task, candidates.clone(), now, &mut agent_budget)
            .await?;
        if task.node.recipe_id.as_str() == akzio_domain::RESEARCH_ANALYST_RECIPE_ID
            && matches!(
                self.daemon.store.run_purpose(&task.run_id)?,
                RunPurpose::Paper | RunPurpose::PositionPlan
            )
        {
            // 补采仅适用于 Paper/PositionPlan 的 Analyst Claim，且只处理声明为阻断方向
            // 预测的可补采缺口；其他 purpose 或普通缺口沿首轮输出返回。
            let claim: ResearchClaim =
                serde_json::from_slice(&self.daemon.store.read_blob(&output.blob)?)?;
            let has_supplemental_request = claim.evidence_gaps.iter().any(|gap| {
                gap.impact == akzio_domain::EvidenceGapImpact::BlocksDirectionalForecast
                    && !gap.supplemental_needs.is_empty()
            });
            let supplemental_already_started = self
                .daemon
                .store
                .task_has_supplemental_round(&task.run_id, &task.node.task_id)?;
            if has_supplemental_request && !supplemental_already_started {
                // session key 是研究日边界；在创建补采和重跑前都复核，避免跨日继续使用
                // 原冻结窗口。市场是否开放由后续 ExecutionGate 单独处理。
                if !self.research_session_is_current(task)? {
                    return Err(DaemonError::InvalidInput(
                        "Research session changed before supplemental evidence collection"
                            .to_owned(),
                    ));
                }
                let request_source = output
                    .source_refs
                    .iter()
                    .find(|reference| reference.kind == ArtifactKind::AgentTurn)
                    .cloned()
                    .ok_or_else(|| {
                        DaemonError::InvalidInput(
                            "analyst refinement has no durable AgentTurn source".to_owned(),
                        )
                    })?;
                // prepare_supplemental 同时校验意图范围并在 I/O 前写入 EvidenceNeed；
                // 拒绝只记录 abandoned 事件，首轮 Claim 仍按普通成功输出保留。
                let prepared = match self.daemon.evidence_acquisition().prepare_supplemental(
                    task,
                    &claim,
                    &request_source,
                    &candidates,
                    now,
                ) {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        self.daemon.evidence_acquisition().note_abandoned(
                            task,
                            "supplemental evidence request rejected",
                            &error,
                        )?;
                        Vec::new()
                    }
                };
                if !prepared.is_empty() {
                    // 补采是异步外部 I/O；空引用表示没有可用的新事实，非空引用才允许
                    // 用同一 AgentRunBudget 做一次受影响 Analyst refinement。
                    match self
                        .daemon
                        .evidence_acquisition()
                        .supplemental(task, &prepared, now)
                        .await
                    {
                        Ok(supplemental_refs) if supplemental_refs.is_empty() => {
                            self.daemon.evidence_acquisition().note_abandoned(task,
                                "supplemental round returned no valid facts; original gaps retained", &"no data")?;
                        }
                        Ok(supplemental_refs) => {
                            // 补采完成后再次核对 session，再把新引用附加到首轮候选；不会
                            // 改写原候选 CAS，也不会重置该 Attempt 的累计预算。
                            if !self.research_session_is_current(task)? {
                                return Err(DaemonError::InvalidInput(
                                    "Research session changed before supplemental analyst round"
                                        .to_owned(),
                                ));
                            }
                            let mut refined_candidates = candidates;
                            refined_candidates.extend(supplemental_refs);
                            let refinement_now = Utc::now();
                            match self
                                .daemon
                                .agent_session()
                                .run(task, refined_candidates, refinement_now, &mut agent_budget)
                                .await
                            {
                                Ok(refined) => return Ok(TaskCompletion::Succeeded(vec![refined])),
                                // refinement 失败只记录原因并回退到首轮 Claim；这是一种
                                // 可审计的部分成功，不把补采失败冒充为方向已修复。
                                Err(error) => self.daemon.evidence_acquisition().note_abandoned(
                                    task,
                                    "supplemental analyst round failed",
                                    &error,
                                )?,
                            }
                        }
                        // 采集异常同样保留首轮输出，补采的失败状态由事件记录而非吞掉。
                        Err(error) => self.daemon.evidence_acquisition().note_abandoned(
                            task,
                            "supplemental evidence collection failed",
                            &error,
                        )?,
                    }
                }
            }
        }
        Ok(TaskCompletion::Succeeded(vec![output]))
    }

    // 只比较当前纽约日期与 Run 冻结的 research session key；不判断开市，也不延长
    // Evidence cutoff，调用方据此阻止跨研究日补采/重跑。
    fn research_session_is_current(&self, task: &ClaimedAttempt) -> Result<bool> {
        let expected = self.daemon.research_session_key(&task.run_id)?;
        // Preserve the session date boundary. Market openness belongs to ExecutionGate.
        Ok(Utc::now()
            .with_timezone(&chrono_tz::America::New_York)
            .date_naive()
            .to_string()
            == expected)
    }

    pub(crate) fn validate_canonical_evidence_manifest(
        &self,
        task: &ClaimedAttempt,
        candidates: &[ArtifactRef],
    ) -> Result<()> {
        // 为 Paper、PositionPlan、Replay、Shadow 计算候选证据覆盖；此检查只产生缺口
        // 诊断并允许 Claim 以 scoped neutral forecast 继续，Decision/Execution Gate
        // 仍独立校验其所需输入。
        let purpose = self.daemon.store.run_purpose(&task.run_id)?;
        if !matches!(
            purpose,
            RunPurpose::Paper | RunPurpose::PositionPlan | RunPurpose::Replay | RunPurpose::Shadow
        ) {
            return Ok(());
        }

        let payloads = candidates
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::NormalizedEvidence)
            .filter_map(|reference| self.daemon.store.artifact(&reference.artifact_id).ok())
            .filter_map(|artifact| self.daemon.store.read_blob(&artifact.blob).ok())
            .filter_map(|bytes| serde_json::from_slice::<NormalizedEvidencePayload>(&bytes).ok())
            .collect::<Vec<_>>();
        let session_key = if matches!(purpose, RunPurpose::Paper | RunPurpose::PositionPlan) {
            // 当前运行沿用冻结 session key，不能从候选证据的下载日期反推出研究窗口。
            self.daemon.research_session_key(&task.run_id)?
        } else {
            // 历史/Shadow 运行必须能从所有可解码证据得到唯一 DecisionClock 日期；
            // 多个 cutoff 无法形成一个可审计历史 session，直接拒绝。
            let cutoffs = payloads
                .iter()
                .map(|payload| {
                    payload
                        .time_basis
                        .decision_clock
                        .decision_cutoff
                        .date_naive()
                })
                .collect::<BTreeSet<_>>();
            if cutoffs.len() != 1 {
                return Err(DaemonError::InvalidInput(format!(
                    "canonical historical evidence manifest must have one DecisionClock cutoff, found {}",
                    cutoffs.len()
                )));
            }
            cutoffs
                .into_iter()
                .next()
                .expect("one cutoff checked")
                .format("%Y-%m-%d")
                .to_string()
        };

        let actual = payloads
            .iter()
            .map(|payload| (payload.source.as_str(), payload.resource.as_str()))
            .collect::<BTreeSet<_>>();
        let blockers = akzio_domain::instrument_evidence_requirements_for_session(&session_key)
            .into_iter()
            .filter(|requirement| {
                !actual.contains(&(
                    requirement.need.source_family.as_str(),
                    requirement.need.resource.as_str(),
                ))
            })
            .collect::<Vec<_>>();
        if blockers.is_empty() {
            return Ok(());
        }
        // 只报告前 8 个 resource 作为稳定摘要，完整缺口数量仍由日志字段保留；这里不
        // 自动发起采集，调用方继续依赖 Claim/Synthesizer 的资格和后续 Gate。
        let summary = blockers
            .iter()
            .take(8)
            .map(|blocker| {
                format!(
                    "{}:{}:{}",
                    blocker
                        .key
                        .asset
                        .map_or_else(|| "GLOBAL".to_owned(), |asset| asset.symbol().to_owned()),
                    blocker.key.category.as_str(),
                    blocker.need.resource
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        tracing::warn!(run_id=%task.run_id, missing=blockers.len(), resources=%summary,
            "directional evidence coverage incomplete; retain research with scoped neutral forecasts");
        Ok(())
    }
}
