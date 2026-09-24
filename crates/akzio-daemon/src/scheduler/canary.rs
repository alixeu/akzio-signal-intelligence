// 文件导读：Canary scheduler 在有效 approval/runtime identity、candidate Contract/Topology、
// broker account/feed 和当前 active analyst 条件都匹配时，原子准备 parent Paper Run 与
// 三个 Shadow Run。它只预约比较实验，不把 candidate 变成 active，也不授予 Paper fill 或
// learning 权限；后续 Outcome/canary evaluation 另行完成。
// Rust 机制：泛型 clock 借用 trait object 并 await 外部观察；多个 `move` 闭包分别拥有
// run/proposal/setup 进入 StoreExecutor；`Option`/`Result` 把缺 approval、缺 cohort、身份
// 漂移转换为等待或 fail closed。

use super::*;

impl PaperScheduler {
    pub(super) async fn tick_canary<C>(
        &self,
        campaign: &akzio_store::CanaryCampaignHead,
        session_key: &str,
        clock: &C,
        now: DateTime<Utc>,
    ) -> SchedulerResult<Option<SessionSlotReservation>>
    where
        C: BrokerSessionClock + ?Sized,
    {
        // 先按 campaign 当前级别取 cohort，并用 broker session 日期确定 regime；
        // cohort 尚未配置或日期不属于任何 regime 时，本轮只等待，不触发写操作。
        let cohort = campaign
            .spec
            .cohort(campaign.status)
            .ok_or(SchedulerError::WorkflowUnavailable)?
            .clone();
        let market_day = NaiveDate::parse_from_str(session_key, "%Y-%m-%d")
            .map_err(|_| SchedulerError::InvalidSessionKey(session_key.to_owned()))?;
        let Some(regime) = cohort.regime_for(market_day).map(str::to_owned) else {
            return Ok(None);
        };
        let campaign_id = campaign.spec.campaign_id.clone();
        let campaign_status = campaign.status;
        let existing_session_key = session_key.to_owned();
        if let Some(existing) = self
            .store_executor
            .execute(move |store| {
                store.canary_session_by_key(&campaign_id, campaign_status, &existing_session_key)
            })
            .await??
        {
            // 相同 campaign/level/session 已有 reservation 时只还原 parent slot；
            // 不重新构造 Shadow Run，也不把旧 session 换成新的 graph。
            let parent_run_id = existing.reservation.parent_run_id;
            let slot = self
                .store_executor
                .execute(move |store| store.session_slot_for_run(&parent_run_id))
                .await??
                .ok_or(SchedulerError::WorkflowUnavailable)?;
            return Ok(Some(SessionSlotReservation {
                slot,
                newly_reserved: false,
            }));
        }
        let scheduler = self.clone();
        let Some((runtime_manifest, approval)) = self
            .store_executor
            .execute(move |_| scheduler.current_approval_binding())
            .await??
        else {
            return Ok(None);
        };
        // Manifest 是已持久化 approval 的身份来源；之后读取 broker account ID 是真实
        // 只读网络请求，用于核对审批绑定，不会提交订单。
        let manifest_blob = runtime_manifest.blob.clone();
        let manifest_payload: RuntimeManifest = serde_json::from_slice(
            &self
                .store_executor
                .execute(move |store| store.read_blob(&manifest_blob))
                .await??,
        )?;
        if let Some(expected) = &self.runtime_identity_hash {
            if manifest_payload.runtime_identity_hash()? != *expected {
                return Ok(None);
            }
        }
        if manifest_payload.code_revision != campaign.spec.source_revision
            || manifest_payload.maximum_notional != campaign.spec.maximum_total_notional
        {
            return Ok(None);
        }
        let account_id = clock.paper_account_id().await?;
        if manifest_payload.broker_account_id != account_id
            || self
                .market_data_feed
                .is_some_and(|feed| manifest_payload.market_data_feed != feed.as_str())
        {
            return Ok(None);
        }

        // candidate Contract 必须是未激活、基于当前 active Contract 的 canonical 候选；
        // 不通过 stage/resume 自动激活该候选。
        let candidate_artifact_id = campaign.spec.candidate_contract.artifact_id.clone();
        let (candidate_artifact, candidate, candidate_installation) = self
            .store_executor
            .execute(move |store| -> SchedulerResult<_> {
                let artifact = store.artifact(&candidate_artifact_id)?;
                let candidate: AgentContract =
                    serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
                let installation = store
                    .contract_installation(&candidate.contract_hash)?
                    .ok_or(SchedulerError::WorkflowUnavailable)?;
                Ok((artifact, candidate, installation))
            })
            .await??;
        candidate.validate()?;
        if candidate_artifact.kind != ArtifactKind::Contract
            || candidate_artifact.lifecycle != ArtifactLifecycle::Canonical
            || candidate.purpose.as_str() != "research.analyst"
            || candidate.contract_hash == campaign.spec.active_contract_hash
            || candidate_installation.activated_at.is_some()
            || candidate_installation.baseline_contract_hash.as_ref()
                != Some(&campaign.spec.active_contract_hash)
            || candidate.contract_hash != cohort.candidate_contract_hash
        {
            return Err(SchedulerError::WorkflowUnavailable);
        }

        // candidate Topology 只从已存 CAS 读取，并与 campaign cohort 的冻结 topology ID 核对。
        let candidate_topology_id = campaign.spec.candidate_topology.artifact_id.clone();
        let (candidate_topology_artifact, candidate_topology) = self
            .store_executor
            .execute(move |store| -> SchedulerResult<_> {
                let artifact = store.artifact(&candidate_topology_id)?;
                let topology: akzio_domain::WorkflowGraph =
                    serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
                Ok((artifact, topology))
            })
            .await??;
        candidate_topology.validate()?;
        if candidate_topology_artifact.kind != ArtifactKind::WorkflowGraph
            || candidate_topology_artifact.lifecycle != ArtifactLifecycle::RunScoped
            || candidate_topology.topology_id != STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID
            || candidate_topology.topology_id != cohort.candidate_topology_id.0
        {
            return Err(SchedulerError::WorkflowUnavailable);
        }

        let active_analyst = self
            .workflow
            .recipe(&akzio_domain::TaskRecipeId::new("research.analyst")?)?;
        if active_analyst.contract_hash.as_ref() != Some(&campaign.spec.active_contract_hash) {
            return Err(SchedulerError::WorkflowUnavailable);
        }

        // 到此才取得 scheduler lease 并准备 parent 与三个 Shadow graph；prepare_* 仅在共享
        // Store 连接暂存未 durable 的 Artifact/commit，不发布 workflow/session 状态。
        let lease = self.acquire_or_renew_async().await?;
        let parent_run_id = RunId::new();
        let scheduler = self.clone();
        let snapshot_run_id = parent_run_id.clone();
        let snapshot_session_key = session_key.to_owned();
        let setup_artifacts = self
            .store_executor
            .execute(move |_| {
                scheduler.paper_snapshot_artifacts(&snapshot_run_id, &snapshot_session_key, now)
            })
            .await??;
        let mut parent_proposal = self.workflow.approved_paper_proposal("active")?;
        let parent_needs = setup_artifacts
            .iter()
            .map(|artifact| ArtifactRef {
                artifact_id: artifact.artifact_id.clone(),
                kind: ArtifactKind::EvidenceNeed,
            })
            .collect::<Vec<_>>();
        for task in parent_proposal
            .tasks
            .values_mut()
            .filter(|t| t.recipe_id.as_str() == "research.analyst")
        {
            task.evidence_needs = parent_needs.clone();
        }
        let workflow = self.workflow.clone();
        let prepared_parent_run_id = parent_run_id.clone();
        let prepared_session_key = session_key.to_owned();
        let prepared_parent_proposal = parent_proposal.clone();
        let prepared_setup_artifacts = setup_artifacts.clone();
        let (parent_reservation, parent_proposal_artifact) = self
            .store_executor
            .execute(move |_| {
                workflow.prepare_approved_paper_session_with_inputs_for_run(
                    prepared_parent_run_id,
                    &prepared_session_key,
                    &prepared_parent_proposal,
                    &prepared_setup_artifacts,
                    now,
                )
            })
            .await??;
        // Shadow 共享 parent 已绑定的冻结 evidence snapshot 引用，但各自有新的 RunId 与
        // Shadow purpose；lower 失败时尚未发布 canary session/slot。
        let snapshot_refs = parent_reservation
            .workflow
            .nodes
            .iter()
            .find(|node| node.recipe_id.as_str() == "research.analyst")
            .map(|node| node.input_artifacts.clone())
            .ok_or(SchedulerError::WorkflowUnavailable)?;

        let mut contract_proposal = parent_proposal.clone();
        for task in contract_proposal
            .tasks
            .values_mut()
            .filter(|t| t.recipe_id.as_str() == "research.analyst")
        {
            task.evidence_needs = snapshot_refs.clone();
        }
        let active_contract_hash = active_analyst
            .contract_hash
            .clone()
            .ok_or(SchedulerError::WorkflowUnavailable)?;

        let contract_shadow_run_id = RunId::new();
        let topology_shadow_run_id = RunId::new();
        let bundle_shadow_run_id = RunId::new();
        let workflow = self.workflow.clone();
        let candidate_contract_hash = candidate.contract_hash.clone();
        let contract_shadow_run = contract_shadow_run_id.clone();
        let contract_shadow = self
            .store_executor
            .execute(move |_| {
                let graph =
                    workflow.lower_shadow(&contract_proposal, Some(&candidate_contract_hash))?;
                workflow.prepare_workflow_commit(
                    contract_shadow_run,
                    RunPurpose::Shadow,
                    graph,
                    now,
                )
            })
            .await??;
        let workflow = self.workflow.clone();
        let topology = candidate_topology.clone();
        let topology_refs = snapshot_refs.clone();
        let topology_active_hash = active_contract_hash.clone();
        let topology_shadow_run = topology_shadow_run_id.clone();
        let topology_shadow = self
            .store_executor
            .execute(move |_| {
                let graph = workflow.lower_shadow_from_graph(
                    &topology,
                    &topology_refs,
                    Some(&topology_active_hash),
                )?;
                workflow.prepare_workflow_commit(
                    topology_shadow_run,
                    RunPurpose::Shadow,
                    graph,
                    now,
                )
            })
            .await??;
        let workflow = self.workflow.clone();
        let bundle_topology = candidate_topology;
        let bundle_refs = snapshot_refs;
        let bundle_contract_hash = candidate.contract_hash.clone();
        let bundle_shadow_run = bundle_shadow_run_id.clone();
        let bundle_shadow = self
            .store_executor
            .execute(move |_| {
                let graph = workflow.lower_shadow_from_graph(
                    &bundle_topology,
                    &bundle_refs,
                    Some(&bundle_contract_hash),
                )?;
                workflow.prepare_workflow_commit(bundle_shadow_run, RunPurpose::Shadow, graph, now)
            })
            .await??;

        // Store 在一个事务中再次检查 lease epoch、approval、parent、三个 Shadow workflow，
        // 并原子提交 session slot/workflow/campaign reservation；事务失败不会留下部分 Run。
        let canary_reservation = CanarySessionReservation {
            schema_version: DOMAIN_SCHEMA_VERSION,
            campaign_id: campaign.spec.campaign_id.clone(),
            level: campaign.status,
            session_key: session_key.to_owned(),
            cohort_id: Some(cohort.cohort_id),
            market_day: Some(market_day),
            regime: Some(regime),
            parent_run_id,
            contract_shadow_run_id,
            topology_shadow_run_id,
            bundle_shadow_run_id,
            scheduler_epoch: lease.epoch,
            reserved_at: now,
        };
        let parent = self
            .store_executor
            .execute(move |store| {
                store.reserve_canary_session_with_workflows(
                    &lease,
                    &parent_reservation,
                    &parent_proposal_artifact,
                    &runtime_manifest,
                    &approval,
                    &[contract_shadow, topology_shadow, bundle_shadow],
                    &canary_reservation,
                )
            })
            .await??;
        Ok(Some(parent))
    }
}
