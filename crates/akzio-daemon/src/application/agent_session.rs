// 文件导读：AgentSession 把已 claim 的 Attempt、候选 Artifact、daemon 选择的 stage model
// 和同一 Attempt 的累计预算交给 AgentRuntime。它只产生 Claim/Critique/Proposal 等研究
// Artifact；ContextManifest/ReadGrant、Prompt Contract 和输出校验仍在 research/context
// runtime，研究输出不自动成为 Decision 或执行许可。
// Rust 机制：结构体持有 `&Daemon` 借用，避免复制运行时状态；`&mut AgentRunBudget` 明确
// 跨调用共享预算；BTreeMap 按 ArtifactId 去重，闭包只捕获受控引用，Result 保留 kind
// 漂移/依赖未完成等错误。

use crate::*;

/// Model-mediated session execution with daemon-owned routing and budget.
pub(crate) struct AgentSession<'a> {
    daemon: &'a Daemon,
}

impl<'a> AgentSession<'a> {
    // 保存 Daemon 的共享借用，后续候选读取和 AgentRuntime 调用都沿用同一运行时边界。
    pub(crate) const fn new(daemon: &'a Daemon) -> Self {
        Self { daemon }
    }

    // 按任务 recipe 选择已配置模型，把候选 Artifact 和同一 Attempt 的累计预算交给
    // AgentRuntime；返回的是研究/审查角色提交并通过运行时校验的单个输出 Artifact，
    // 不是 Decision 或执行许可。
    pub(crate) async fn run(
        &self,
        task: &ClaimedAttempt,
        candidates: Vec<ArtifactRef>,
        now: DateTime<Utc>,
        budget: &mut AgentRunBudget,
    ) -> Result<Artifact> {
        let model = self.daemon.model_for(task.node.recipe_id.as_str());
        Ok(self
            .daemon
            .agents
            .run_with_budget(&task.permit, &task.node, candidates, model, now, budget)
            .await?)
    }

    // 从任务声明、已成功依赖输出和父任务关系构造去重后的候选引用；这里先做 Store
    // 中的存在性与 kind 校验，真正的 ContextManifest、授权投影和 RawEvidence 隔离由
    // AgentRuntime/ContextBroker 继续负责。
    pub(crate) fn candidates(&self, task: &ClaimedAttempt) -> Result<Vec<ArtifactRef>> {
        let mut candidates = BTreeMap::<ArtifactId, ArtifactRef>::new();
        let expand_research_sources = should_expand_research_sources(task.node.recipe_id.as_str());

        for reference in &task.node.input_artifacts {
            self.append_candidate(&mut candidates, reference, expand_research_sources)?;
        }

        if let Some(parent_task_id) = &task.node.parent_task_id {
            // 子任务必须把 parent 声明为 dependency；这里只验证父 Attempt 已成功，
            // 不直接拼接父输出，避免调度层绕过 AgentRuntime 的子上下文收缩策略。
            if !task.node.dependencies.contains(parent_task_id) {
                return Err(DaemonError::InvalidInput(format!(
                    "agent task {} parent {parent_task_id} is not dependency",
                    task.node.task_id
                )));
            }
            let snapshot = self.daemon.store.workflow_snapshot(&task.run_id)?;
            let parent = snapshot
                .tasks
                .iter()
                .find(|stored| stored.node.task_id == *parent_task_id)
                .ok_or_else(|| {
                    DaemonError::InvalidInput(format!(
                        "task {} references missing parent {parent_task_id}",
                        task.node.task_id
                    ))
                })?;
            if parent.status != TaskStatus::Succeeded {
                return Err(DaemonError::UnfinishedDependency {
                    task_id: task.node.task_id.clone(),
                    dependency: parent_task_id.clone(),
                });
            }
            // AgentRuntime/ContextBroker owns parent projection and context policy.
        } else {
            // 没有 parent 时，所有显式依赖都必须已经成功；随后只把其正式成功输出
            // 加入候选，Skipped 或未完成依赖不能被当作可用研究依据。
            let dependencies = task
                .node
                .dependencies
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            if !dependencies.is_empty() {
                let snapshot = self.daemon.store.workflow_snapshot(&task.run_id)?;
                for dependency in &dependencies {
                    let dependency_task = snapshot
                        .tasks
                        .iter()
                        .find(|stored| stored.node.task_id == *dependency)
                        .ok_or_else(|| {
                            DaemonError::InvalidInput(format!(
                                "task {} references missing dependency {dependency}",
                                task.node.task_id
                            ))
                        })?;
                    if dependency_task.status != TaskStatus::Succeeded {
                        return Err(DaemonError::UnfinishedDependency {
                            task_id: task.node.task_id.clone(),
                            dependency: dependency.clone(),
                        });
                    }
                }
                for dependency in dependencies {
                    // 依赖输出可能为空，但依赖本身仍须是成功状态；空结果最终由
                    // MissingTaskContext 拦截（有 parent 的任务则交由上面的投影逻辑）。
                    for artifact in self
                        .daemon
                        .store
                        .succeeded_task_outputs_or_empty(&task.run_id, &dependency)?
                    {
                        self.append_candidate(
                            &mut candidates,
                            &ArtifactRef {
                                artifact_id: artifact.artifact_id,
                                kind: artifact.kind,
                            },
                            expand_research_sources,
                        )?;
                    }
                }
            }
        }

        if candidates.is_empty() && task.node.parent_task_id.is_none() {
            return Err(DaemonError::MissingTaskContext(task.node.task_id.clone()));
        }

        Ok(candidates.into_values().collect())
    }

    // 递归纳入一个已声明的 Artifact 及其受支持的研究来源，同时以 ArtifactId 去重并
    // 拒绝同一 ID 的 kind 漂移；RawEvidence 只用于闭合来源，永不进入模型候选集合。
    fn append_candidate(
        &self,
        candidates: &mut BTreeMap<ArtifactId, ArtifactRef>,
        reference: &ArtifactRef,
        expand_research_sources: bool,
    ) -> Result<()> {
        if let Some(existing) = candidates.get(&reference.artifact_id) {
            if existing.kind != reference.kind {
                return Err(DaemonError::InvalidInput(format!(
                    "artifact {} kind changed from {:?} to {:?}",
                    reference.artifact_id, existing.kind, reference.kind
                )));
            }
            return Ok(());
        }
        let artifact = self.daemon.store.artifact(&reference.artifact_id)?;
        if artifact.kind != reference.kind {
            return Err(DaemonError::InvalidInput(format!(
                "artifact {} kind changed from {:?} to {:?}",
                reference.artifact_id, reference.kind, artifact.kind
            )));
        }
        // Raw evidence is only a source closure; ContextBroker must not put it in
        // the model manifest directly.
        if artifact.kind != ArtifactKind::RawEvidence {
            if artifact.kind == ArtifactKind::SemanticDetail
                && artifact.producer == "canary.evidence_snapshot"
            {
                // Shadow canary 的聚合快照本身是可读投影；仅展开其中精确的
                // NormalizedEvidence 来源，保持父 EvidenceGate 的授权闭包。
                for source in artifact
                    .source_refs
                    .iter()
                    .filter(|source| source.kind == ArtifactKind::NormalizedEvidence)
                {
                    self.append_candidate(candidates, source, expand_research_sources)?;
                }
            }
            candidates.insert(
                artifact.artifact_id.clone(),
                ArtifactRef {
                    artifact_id: artifact.artifact_id,
                    kind: artifact.kind,
                },
            );
            if expand_research_sources {
                // Critic/Synthesizer 需要看到研究输出的依据闭包，但只沿允许的
                // Claim/Critique/标准化证据/语义细节类型递归，不恢复旧 Draft 链路。
                for source in research_output_source_refs(artifact.kind, &artifact.source_refs) {
                    self.append_candidate(candidates, &source, expand_research_sources)?;
                }
            }
        }
        Ok(())
    }
}

// 只有 Claim/Critique 的来源闭包会被研究复核角色展开；其他输出不自动传播来源，
// 以避免把 Decision、Execution 或任意历史 Artifact 混入本次模型上下文。
fn research_output_source_refs(
    kind: ArtifactKind,
    source_refs: &[ArtifactRef],
) -> Vec<ArtifactRef> {
    if !matches!(kind, ArtifactKind::Claim | ArtifactKind::Critique) {
        return Vec::new();
    }
    source_refs
        .iter()
        .filter(|reference| {
            matches!(
                reference.kind,
                ArtifactKind::Claim
                    | ArtifactKind::Critique
                    | ArtifactKind::NormalizedEvidence
                    | ArtifactKind::SemanticDetail
            )
        })
        .cloned()
        .collect()
}

// 研究 Critic 和 Synthesizer 使用完整研究来源闭包；Analyst、Reviewer 及执行节点
// 保持各自的输入选择边界。
fn should_expand_research_sources(recipe_id: &str) -> bool {
    matches!(
        recipe_id,
        akzio_domain::RESEARCH_CRITIC_RECIPE_ID | akzio_domain::RESEARCH_SYNTHESIZER_RECIPE_ID
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn research_output_candidates_include_their_context_source_closure() {
        let source = ArtifactRef {
            artifact_id: ArtifactId(akzio_domain::ContentHash::of_bytes(b"projection")),
            kind: ArtifactKind::SemanticDetail,
        };
        let claim = ArtifactRef {
            artifact_id: ArtifactId(akzio_domain::ContentHash::of_bytes(b"claim")),
            kind: ArtifactKind::Claim,
        };

        let refs =
            research_output_source_refs(ArtifactKind::Critique, &[claim.clone(), source.clone()]);

        assert!(refs.contains(&claim));
        assert!(refs.contains(&source));
    }

    #[test]
    fn critics_and_synthesizers_expand_research_output_sources() {
        assert!(should_expand_research_sources(
            akzio_domain::RESEARCH_CRITIC_RECIPE_ID
        ));
        assert!(should_expand_research_sources(
            akzio_domain::RESEARCH_SYNTHESIZER_RECIPE_ID
        ));
    }
}
