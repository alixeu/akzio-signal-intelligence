use crate::*;

/// Governed evidence collection, including the single supplemental round.
pub(crate) struct EvidenceAcquisition<'a> {
    daemon: &'a Daemon,
}

impl<'a> EvidenceAcquisition<'a> {
    // 绑定当前 Daemon，所有采集和补采都复用同一 Store、适配器与运行模式。
    pub(crate) const fn new(daemon: &'a Daemon) -> Self {
        Self { daemon }
    }

    // 执行 Evidence Gate：Shadow canary 优先复用父 Attempt 的已成功标准化证据，
    // 其他运行交给受治理采集器；空结果是 NoOutput，采到的 Artifact 才作为成功输出，
    // 这里不把 Evidence Gate 的完成写成研究、Decision 或执行完成。
    pub(crate) async fn execute(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        if self.daemon.store.run_purpose(&task.run_id)? == RunPurpose::Shadow
            && self
                .daemon
                .store
                .canary_session_for_run(&task.run_id)?
                .is_some()
        {
            // 父证据尚未产生时只短暂延期，避免把 Shadow 的授权材料猜测成已存在。
            let Some(evidence) = self.daemon.store.canary_parent_evidence(&task.run_id)? else {
                return Ok(TaskCompletion::DeferredUntil(now + Duration::seconds(1)));
            };
            // 只接受精确的 collection-status 产物，并把父证据封装成当前 Run 的
            // SemanticDetail；source_refs 保留父 Artifact 血缘，RawEvidence 仍不进入模型。
            let status = evidence
                .iter()
                .find(|a| a.producer == "evidence.collection_status")
                .ok_or_else(|| {
                    DaemonError::InvalidInput("canary parent collection status missing".to_owned())
                })?;
            let value: Value = serde_json::from_slice(&self.daemon.store.read_blob(&status.blob)?)?;
            let artifact = Artifact::new(
                ArtifactKind::SemanticDetail,
                self.daemon.store.stage_json(&value)?,
                "canary.evidence_snapshot",
                ArtifactLifecycle::RunScoped,
                ArtifactProvenance {
                    source_family: "akzio.ingest".to_owned(),
                    observed_at: status.provenance.observed_at,
                    retrieved_at: now,
                    source_uri: None,
                    confidence_ppm: 1_000_000,
                    producer_contract_hash: None,
                },
                Some(task.permit.artifact_origin()),
                evidence
                    .iter()
                    .map(|a| ArtifactRef {
                        artifact_id: a.artifact_id.clone(),
                        kind: a.kind,
                    })
                    .collect(),
                now,
            )?;
            return Ok(TaskCompletion::Succeeded(vec![artifact]));
        }
        // 正式 Paper/PositionPlan 的 EvidenceNeed 校验、来源采集和部分成功语义在
        // Daemon::acquire_evidence 内完成；本层只映射任务完成边界。
        let artifacts = self.daemon.acquire_evidence(task, now).await?;
        Ok(if artifacts.is_empty() {
            TaskCompletion::NoOutput
        } else {
            TaskCompletion::Succeeded(artifacts)
        })
    }

    // 根据 Analyst 的阻塞性 EvidenceGap 准备最多一轮的类型化 EvidenceNeed，并由
    // 底层方法在 I/O 前写入对应 CAS Artifact；返回引用、Artifact 和解析后的需求。
    pub(crate) fn prepare_supplemental(
        &self,
        task: &ClaimedAttempt,
        claim: &ResearchClaim,
        claim_reference: &ArtifactRef,
        candidates: &[ArtifactRef],
        now: DateTime<Utc>,
    ) -> Result<Vec<(ArtifactRef, Artifact, EvidenceNeed)>> {
        self.daemon
            .prepare_supplemental_needs(task, claim, claim_reference, candidates, now)
    }

    // 按已经持久化的补采需求并发获取标准化证据；返回值仅是新证据引用，不代表
    // 受影响 Horizon 已重跑或 Proposal/Decision 已更新。
    pub(crate) async fn supplemental(
        &self,
        task: &ClaimedAttempt,
        needs: &[(ArtifactRef, Artifact, EvidenceNeed)],
        now: DateTime<Utc>,
    ) -> Result<Vec<ArtifactRef>> {
        self.daemon
            .acquire_supplemental_evidence(task, needs, now)
            .await
    }

    // 记录补采被放弃或失败的可审计事件；它不撤回首轮 Claim，也不把缺口标记为已解决。
    pub(crate) fn note_abandoned(
        &self,
        task: &ClaimedAttempt,
        reason: &str,
        error: &dyn std::fmt::Display,
    ) -> Result<()> {
        self.daemon
            .note_supplemental_round_abandoned(task, reason, error)
    }
}
