// 文件导读：EvidenceAcquisition 是 Evidence Gate 和一次补采的应用门面。它复用 daemon
// 的 adapter/Store/permit，区分 Shadow 父证据复用、首轮 EvidenceNeed 采集和 supplemental
// 新事实；Succeeded 只表示相应 Artifact 已提交，不代表研究、Decision、订单或 Outcome。
// Rust 机制：`&'a Daemon` 让门面不拥有状态；异步方法返回 Future 并在调用方 await，
// `Vec<(ArtifactRef, Artifact, EvidenceNeed)>` 同时携带身份/正文/强类型需求，借用与
// `Result` 防止未授权 payload 进入模型。

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

    // 已注册的 Shadow canary 复用父 Attempt 的标准化证据。
    // Paper/PositionPlan 采集当前 Run 的 EvidenceNeed 并提交采集状态。
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
        let artifacts = self.daemon.acquire_evidence(task, now).await?;
        Ok(TaskCompletion::Succeeded(artifacts))
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
        original_candidates: &[ArtifactRef],
    ) -> Result<Vec<ArtifactRef>> {
        let original = original_candidates
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::NormalizedEvidence)
            .map(|reference| {
                self.daemon
                    .read_artifact_payload::<NormalizedEvidencePayload>(reference)
            })
            .collect::<Result<Vec<_>>>()?;
        let cutoff = super::research_supplement::frozen_research_cutoff(&original)?;
        let references = self
            .daemon
            .acquire_supplemental_evidence(task, needs, cutoff)
            .await?;
        let mut admissible = Vec::with_capacity(references.len());
        for reference in references {
            let payload: NormalizedEvidencePayload =
                self.daemon.read_artifact_payload(&reference)?;
            if let Some(reason) =
                super::research_supplement::supplemental_candidate_gap(&payload, cutoff)
            {
                self.daemon.supplement_record(
                    task,
                    "research.supplement.excluded",
                    &serde_json::json!({
                        "resource": payload.resource,
                        "decision_cutoff": cutoff,
                        "status": "excluded",
                        "reason": reason,
                    }),
                    vec![reference],
                    Utc::now(),
                )?;
            } else {
                admissible.push(reference);
            }
        }
        Ok(admissible)
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
