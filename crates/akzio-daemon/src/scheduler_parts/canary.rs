impl PaperScheduler {
    async fn tick_canary<C>(
        &self,
        campaign: &akzio_store::v2::CanaryCampaignHead,
        session_key: &str,
        clock: &C,
        now: DateTime<Utc>,
    ) -> SchedulerResult<Option<SessionSlotReservation>>
    where
        C: BrokerSessionClock + ?Sized,
    {
        let campaign_id = campaign.spec.campaign_id.clone();
        let campaign_status = campaign.status;
        if self
            .store_executor
            .execute(move |store| store.canary_session(&campaign_id, campaign_status))
            .await??
            .is_some()
        {
            return Err(SchedulerError::WorkflowUnavailable);
        }
        let scheduler = self.clone();
        let Some((runtime_manifest, approval)) = self
            .store_executor
            .execute(move |_| scheduler.current_approval_binding())
            .await??
        else {
            return Ok(None);
        };
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
        let account_id = clock.paper_account_id().await?;
        if manifest_payload.broker_account_id != account_id
            || self
                .market_data_feed
                .is_some_and(|feed| manifest_payload.market_data_feed != feed.as_str())
        {
            return Ok(None);
        }

        let candidate_artifact_id = campaign.spec.candidate_contract.artifact_id.clone();
        let (candidate_artifact, candidate, candidate_installation) = self
            .store_executor
            .execute(move |store| -> SchedulerResult<_> {
                let artifact = store.artifact(&candidate_artifact_id)?;
                let candidate: AgentContract = serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
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
        {
            return Err(SchedulerError::WorkflowUnavailable);
        }

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
        {
            return Err(SchedulerError::WorkflowUnavailable);
        }

        let active_analyst = self
        .workflow
        .recipe(&akzio_domain::TaskRecipeId::new("research.analyst")?)?;
        if active_analyst.contract_hash.as_ref() != Some(&campaign.spec.active_contract_hash) {
            return Err(SchedulerError::WorkflowUnavailable);
        }

        let lease = self.acquire_or_renew_async(now).await?;
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
        parent_proposal
            .tasks
            .get_mut("analyst")
            .ok_or(SchedulerError::WorkflowUnavailable)?
            .evidence_needs = setup_artifacts
            .iter()
            .map(|artifact| ArtifactRef {
                artifact_id: artifact.artifact_id.clone(),
                kind: ArtifactKind::EvidenceNeed,
            })
            .collect();
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
        let snapshot_refs = parent_reservation
            .workflow
            .nodes
            .iter()
            .find(|node| node.recipe_id.as_str() == "research.analyst")
            .map(|node| node.input_artifacts.clone())
            .ok_or(SchedulerError::WorkflowUnavailable)?;

        let mut contract_proposal = parent_proposal.clone();
        contract_proposal
            .tasks
            .get_mut("analyst")
            .ok_or(SchedulerError::WorkflowUnavailable)?
            .evidence_needs = snapshot_refs.clone();
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
                let graph = workflow.lower_shadow(
                    &contract_proposal,
                    Some(&candidate_contract_hash),
                )?;
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
                workflow.prepare_workflow_commit(
                    bundle_shadow_run,
                    RunPurpose::Shadow,
                    graph,
                    now,
                )
            })
            .await??;

        let canary_reservation = CanarySessionReservation {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            campaign_id: campaign.spec.campaign_id.clone(),
            level: campaign.status,
            session_key: session_key.to_owned(),
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
