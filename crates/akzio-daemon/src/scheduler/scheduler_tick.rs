// 文件导读：每次 tick 先问真实 broker session，再检查已有 slot、Canary 状态、冷启动
// Policy/approval、runtime identity 和 proposal，最后在同一 Store executor 下预约新 Run。
// Closed/缺 proposal/身份不匹配是等待，不会伪造 slot；reservation 返回成功也只说明
// T0 graph 已发布，后续 research/Decision/Execution/Paper/Outcome 仍未完成。
// Rust 机制：async trait Future 按阶段 await；`move` 闭包拥有 session/setup 进入 executor；
// `BTreeSet` 比较资源集合，`Option` 表示无 session/无 approval，枚举状态保持 fail closed。

use super::*;

impl PaperScheduler {
    pub async fn tick<C, P>(
        &self,
        clock: &C,
        source: &P,
        now: DateTime<Utc>,
    ) -> SchedulerResult<Option<SessionSlotReservation>>
    where
        C: BrokerSessionClock + ?Sized,
        P: PaperWorkflowSource + ?Sized,
    {
        // tick 的每个早退都是明确的等待/复用语义：闭市、已有 slot、staged canary、缺
        // approval/proposal 或 identity 不匹配均不创建新 Run，也不发送 broker 写请求。
        let Some(session_key) = clock.open_session_key().await? else {
            eprintln!("Paper scheduler waiting: broker market is closed");
            return Ok(None);
        };
        let stored_session_key = session_key.clone();
        if let Some(slot) = self
            .store_executor
            .execute(move |store| store.session_slot(&stored_session_key))
            .await??
        {
            self.acquire_or_renew_async().await?;
            let run_id = slot.workflow.run.run_id.clone();
            let health = self
                .store_executor
                .execute(move |store| store.run_lifecycle_health(&run_id))
                .await??;
            if matches!(
                health.execution_status,
                akzio_domain::WorkflowStatus::Failed | akzio_domain::WorkflowStatus::Cancelled
            ) || health.outcome_worker_failed
            {
                tracing::warn!(session=%session_key, execution=?health.execution_status,
                    outcome_failed=health.outcome_worker_failed,
                    "reserved session requires recovery of its existing tasks; commitment is preserved");
            }
            return Ok(Some(SessionSlotReservation {
                slot,
                newly_reserved: false,
            }));
        }
        if let Some(campaign) = self
            .store_executor
            .execute(|store| store.active_canary_campaign())
            .await??
        {
            if campaign.status == CanaryCampaignStatus::Staged {
                return Ok(None);
            }
            if campaign.status.is_level() {
                return self.tick_canary(&campaign, &session_key, clock, now).await;
            }
        }
        // A first canonical research session must be able to earn its future
        // calibration labels. No active policy means no trading approval is
        // bound, even if an older approval artifact exists in the Store.
        let cold_start = self
            .store_executor
            .execute(|store| {
                store
                    .active_decision_policy()
                    .map(|policy| policy.is_none())
            })
            .await??;
        let binding = if cold_start {
            None
        } else {
            let scheduler = self.clone();
            let Some(binding) = self
                .store_executor
                .execute(move |_| scheduler.current_approval_binding())
                .await??
            else {
                eprintln!("Paper scheduler waiting: no current Paper approval binding");
                return Ok(None);
            };
            Some(binding)
        };
        if let Some((runtime_manifest, _)) = &binding {
            let manifest_blob = runtime_manifest.blob.clone();
            let manifest_payload: RuntimeManifest = serde_json::from_slice(
                &self
                    .store_executor
                    .execute(move |store| store.read_blob(&manifest_blob))
                    .await??,
            )?;
            if let Some(expected) = &self.runtime_identity_hash {
                if manifest_payload.runtime_identity_hash()? != *expected {
                    eprintln!("Paper scheduler waiting: runtime identity does not match approval");
                    return Ok(None);
                }
            }
            let account_id = clock.paper_account_id().await?;
            if manifest_payload.broker_account_id != account_id
                || self
                    .market_data_feed
                    .is_none_or(|feed| manifest_payload.market_data_feed != feed.as_str())
            {
                eprintln!("Paper scheduler waiting: broker account or market-data feed mismatch");
                return Ok(None);
            }
        }
        let proposal = match source.proposal(&session_key).await {
            Ok(proposal) => proposal,
            Err(SchedulerError::WorkflowUnavailable) => {
                eprintln!("Paper scheduler waiting: workflow proposal unavailable");
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let lease = self.acquire_or_renew_async().await?;
        let run_id = RunId::new();
        let mut setup_artifacts = Vec::new();
        let mut proposal = proposal;

        for task in proposal.tasks.values_mut() {
            let mut retained = Vec::with_capacity(task.evidence_needs.len());
            for reference in task.evidence_needs.drain(..) {
                let artifact_id = reference.artifact_id.clone();
                let artifact = self
                    .store_executor
                    .execute(move |store| store.artifact(&artifact_id))
                    .await??;
                if artifact.kind == ArtifactKind::EvidenceNeed
                    && artifact.lifecycle == ArtifactLifecycle::RunScoped
                {
                    let origin_run = artifact
                        .origin
                        .as_ref()
                        .and_then(|origin| origin.run_id.as_ref());
                    if artifact.producer == PAPER_SNAPSHOT_PRODUCER {
                        // Snapshot inputs are session-specific. A new slot must
                        // acquire fresh account/quotes/clock evidence below,
                        // never carry a prior Run's scheduler snapshot forward.
                        if origin_run.is_none() {
                            return Err(SchedulerError::WorkflowUnavailable);
                        }
                        continue;
                    }
                    if origin_run.is_some() {
                        return Err(SchedulerError::WorkflowUnavailable);
                    }
                }
                retained.push(reference);
            }
            task.evidence_needs = retained;
        }

        let snapshot_alias = proposal
            .tasks
            .iter()
            .find_map(|(alias, task)| {
                self.workflow
                    .recipe(&task.recipe_id)
                    .ok()
                    .filter(|recipe| recipe.allowed_evidence_sources.contains("alpaca"))
                    .map(|_| alias.clone())
            })
            .ok_or(SchedulerError::WorkflowUnavailable)?;
        let first_task = proposal
            .tasks
            .get_mut(&snapshot_alias)
            .ok_or(SchedulerError::WorkflowUnavailable)?;
        for need in paper_session_evidence_needs(&session_key) {
            need.validate()?;
            let artifact = Artifact::new(
                ArtifactKind::EvidenceNeed,
                self.store_executor
                    .execute({
                        let need = need.clone();
                        // The later reservation runs on the same serialized Store executor.
                        move |store| store.stage_json(&need)
                    })
                    .await??,
                PAPER_SNAPSHOT_PRODUCER,
                ArtifactLifecycle::RunScoped,
                ArtifactProvenance {
                    source_family: "akzio.scheduler".to_owned(),
                    observed_at: None,
                    retrieved_at: now,
                    source_uri: None,
                    confidence_ppm: 1_000_000,
                    producer_contract_hash: None,
                },
                Some(ArtifactOrigin {
                    run_id: Some(run_id.clone()),
                    task_id: None,
                    attempt_id: None,
                    contract_hash: None,
                }),
                Vec::new(),
                now,
            )?;
            first_task.evidence_needs.push(ArtifactRef {
                artifact_id: artifact.artifact_id.clone(),
                kind: ArtifactKind::EvidenceNeed,
            });
            setup_artifacts.push(artifact);
        }

        let workflow = self.workflow.clone();
        Ok(Some(
            self.store_executor
                .execute(move |_| match binding {
                    Some((runtime_manifest, approval)) => workflow
                        .reserve_paper_session_with_inputs_for_run_approved(
                            &lease,
                            run_id,
                            &session_key,
                            &proposal,
                            &setup_artifacts,
                            &runtime_manifest,
                            &approval,
                            now,
                        ),
                    None => workflow.reserve_paper_session_with_inputs_for_run(
                        &lease,
                        run_id,
                        &session_key,
                        &proposal,
                        &setup_artifacts,
                        now,
                    ),
                })
                .await??,
        ))
    }
}
