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
        if self
            .store
            .canary_session(&campaign.spec.campaign_id, campaign.status)?
            .is_some()
        {
            return Err(SchedulerError::WorkflowUnavailable);
        }
        let Some((runtime_manifest, approval)) = self.current_approval_binding()? else {
            return Ok(None);
        };
        let manifest_payload: RuntimeManifest =
            serde_json::from_slice(&self.store.read_blob(&runtime_manifest.blob)?)?;
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

        let candidate_artifact = self
            .store
            .artifact(&campaign.spec.candidate_contract.artifact_id)?;
        let candidate: AgentContract =
            serde_json::from_slice(&self.store.read_blob(&candidate_artifact.blob)?)?;
        candidate.validate()?;
        let candidate_installation = self
            .store
            .contract_installation(&candidate.contract_hash)?
            .ok_or(SchedulerError::WorkflowUnavailable)?;
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

        let candidate_topology_artifact = self
            .store
            .artifact(&campaign.spec.candidate_topology.artifact_id)?;
        let candidate_topology: akzio_domain::WorkflowGraph =
            serde_json::from_slice(&self.store.read_blob(&candidate_topology_artifact.blob)?)?;
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

        let lease = self.acquire_or_renew(now)?;
        let parent_run_id = RunId::new();
        let setup_artifacts = self.paper_snapshot_artifacts(&parent_run_id, session_key, now)?;
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
        let (parent_reservation, parent_proposal_artifact) = self
            .workflow
            .prepare_approved_paper_session_with_inputs_for_run(
                parent_run_id.clone(),
                session_key,
                &parent_proposal,
                &setup_artifacts,
                now,
            )?;
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
            .as_ref()
            .ok_or(SchedulerError::WorkflowUnavailable)?;

        let contract_shadow_run_id = RunId::new();
        let topology_shadow_run_id = RunId::new();
        let bundle_shadow_run_id = RunId::new();
        let contract_shadow = self.workflow.prepare_workflow_commit(
            contract_shadow_run_id.clone(),
            RunPurpose::Shadow,
            self.workflow
                .lower_shadow(&contract_proposal, Some(&candidate.contract_hash))?,
            now,
        )?;
        let topology_shadow = self.workflow.prepare_workflow_commit(
            topology_shadow_run_id.clone(),
            RunPurpose::Shadow,
            self.workflow.lower_shadow_from_graph(
                &candidate_topology,
                &snapshot_refs,
                Some(active_contract_hash),
            )?,
            now,
        )?;
        let bundle_shadow = self.workflow.prepare_workflow_commit(
            bundle_shadow_run_id.clone(),
            RunPurpose::Shadow,
            self.workflow.lower_shadow_from_graph(
                &candidate_topology,
                &snapshot_refs,
                Some(&candidate.contract_hash),
            )?,
            now,
        )?;

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
        let parent = self.store.reserve_canary_session_with_workflows(
            &lease,
            &parent_reservation,
            &parent_proposal_artifact,
            &runtime_manifest,
            &approval,
            &[contract_shadow, topology_shadow, bundle_shadow],
            &canary_reservation,
        )?;
        Ok(Some(parent))
    }
}
