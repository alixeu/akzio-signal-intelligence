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
            self.acquire_or_renew_async(now).await?;
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
        let scheduler = self.clone();
        let Some((runtime_manifest, approval)) = self
            .store_executor
            .execute(move |_| scheduler.current_approval_binding())
            .await??
        else {
            eprintln!("Paper scheduler waiting: no current Paper approval binding");
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
        let proposal = match source.proposal(&session_key).await {
            Ok(proposal) => proposal,
            Err(SchedulerError::WorkflowUnavailable) => {
                eprintln!("Paper scheduler waiting: workflow proposal unavailable");
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let lease = self.acquire_or_renew_async(now).await?;
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
                        move |store| store.put_json(&need)
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
                .execute(move |_| {
                    workflow.reserve_paper_session_with_inputs_for_run_approved(
                    &lease,
                    run_id,
                    &session_key,
                    &proposal,
                    &setup_artifacts,
                    &runtime_manifest,
                    &approval,
                    now,
                )
                })
                .await??,
        ))
    }
}
