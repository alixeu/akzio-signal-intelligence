impl Store {
    pub(super) fn commit_canary_session_transaction(
        transaction: &Transaction<'_>,
        reservation: &CanarySessionReservation,
    ) -> StoreResult<()> {
        let current = read_campaign(transaction, &reservation.campaign_id)?.ok_or_else(|| {
            StoreError::MissingCanaryCampaign(reservation.campaign_id.to_string())
        })?;
        if current.status != reservation.level {
            return Err(StoreError::CanaryCampaignConflict(format!(
                "{} session level {:?} does not match campaign {:?}",
                reservation.campaign_id, reservation.level, current.status
            )));
        }
        validate_session_cohort(&current, reservation)?;
        if run_purpose_from_connection(transaction, &reservation.parent_run_id)?
            != RunPurpose::Paper
            || run_purpose_from_connection(transaction, &reservation.contract_shadow_run_id)?
                != RunPurpose::Shadow
            || run_purpose_from_connection(transaction, &reservation.topology_shadow_run_id)?
                != RunPurpose::Shadow
            || run_purpose_from_connection(transaction, &reservation.bundle_shadow_run_id)?
                != RunPurpose::Shadow
        {
            return Err(StoreError::CanaryCampaignConflict(
                "canary session run purposes".to_owned(),
            ));
        }
        if let Some(cohort_id) = &reservation.cohort_id {
            let market_day = reservation.market_day.ok_or_else(|| {
                StoreError::CanaryCampaignConflict(
                    "canary cohort session has no market day".to_owned(),
                )
            })?;
            let regime = reservation.regime.as_deref().ok_or_else(|| {
                StoreError::CanaryCampaignConflict(
                    "canary cohort session has no regime".to_owned(),
                )
            })?;
            if let Some(existing) =
                read_cohort_session_by_key(transaction, cohort_id, &reservation.session_key)?
            {
                if existing.reservation != *reservation {
                    return Err(StoreError::CanaryCampaignConflict(
                        "canary cohort session is immutable".to_owned(),
                    ));
                }
                return Ok(());
            }
            let duplicate_session: Option<String> = transaction
                .query_row(
                    "SELECT campaign_id FROM rebuild_canary_sessions WHERE session_key = ?1 UNION ALL SELECT campaign_id FROM rebuild_canary_cohort_sessions WHERE session_key = ?1 LIMIT 1",
                    params![reservation.session_key],
                    |row| row.get(0),
                )
                .optional()?;
            if duplicate_session.is_some() {
                return Err(StoreError::CanaryCampaignConflict(
                    reservation.session_key.clone(),
                ));
            }
            transaction.execute(
                "INSERT INTO rebuild_canary_cohort_sessions (cohort_id, campaign_id, stage_json, session_key, market_day, regime, parent_run_id, contract_shadow_run_id, topology_shadow_run_id, bundle_shadow_run_id, scheduler_epoch, reserved_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    cohort_id.as_str(),
                    reservation.campaign_id.as_str(),
                    serde_json::to_string(&reservation.level)?,
                    reservation.session_key,
                    market_day.to_string(),
                    regime,
                    reservation.parent_run_id.0,
                    reservation.contract_shadow_run_id.0,
                    reservation.topology_shadow_run_id.0,
                    reservation.bundle_shadow_run_id.0,
                    reservation.scheduler_epoch,
                    reservation.reserved_at.to_rfc3339(),
                ],
            )?;
            return Ok(());
        }
        if let Some(existing) =
            read_session(transaction, &reservation.campaign_id, reservation.level)?
        {
            if existing.reservation != *reservation {
                return Err(StoreError::CanaryCampaignConflict(format!(
                    "{} already has a different {:?} session",
                    reservation.campaign_id, reservation.level
                )));
            }
            return Ok(());
        }
        let duplicate_session: Option<String> = transaction
            .query_row(
                "SELECT campaign_id FROM rebuild_canary_sessions WHERE session_key = ?1",
                params![reservation.session_key],
                |row| row.get(0),
            )
            .optional()?;
        if duplicate_session.is_some() {
            return Err(StoreError::CanaryCampaignConflict(
                reservation.session_key.clone(),
            ));
        }
        transaction.execute(
            "INSERT INTO rebuild_canary_sessions (campaign_id, level_json, session_key, parent_run_id, contract_shadow_run_id, topology_shadow_run_id, bundle_shadow_run_id, scheduler_epoch, reserved_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                reservation.campaign_id.as_str(),
                serde_json::to_string(&reservation.level)?,
                reservation.session_key,
                reservation.parent_run_id.0,
                reservation.contract_shadow_run_id.0,
                reservation.topology_shadow_run_id.0,
                reservation.bundle_shadow_run_id.0,
                reservation.scheduler_epoch,
                reservation.reserved_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn reserve_canary_session_with_workflows(
        &self,
        lease: &DaemonLease,
        parent: &SessionReservation,
        proposal: &Artifact,
        runtime_manifest: &Artifact,
        approval: &Artifact,
        shadow_workflows: &[WorkflowCommit],
        reservation: &CanarySessionReservation,
    ) -> StoreResult<SessionSlotReservation> {
        if shadow_workflows.len() != 3 {
            return Err(StoreError::CanaryCampaignConflict(
                "canary session requires three shadow workflows".to_owned(),
            ));
        }
        self.validate_paper_session_reservation(parent, proposal)?;
        self.validate_paper_approval_binding(runtime_manifest, approval)?;
        reservation.validate()?;
        if reservation.scheduler_epoch != lease.epoch
            || reservation.session_key != parent.session_key
            || reservation.parent_run_id != parent.workflow.run.run_id
            || shadow_workflows
                .iter()
                .zip([
                    &reservation.contract_shadow_run_id,
                    &reservation.topology_shadow_run_id,
                    &reservation.bundle_shadow_run_id,
                ])
                .any(|(commit, expected)| {
                    commit.run.run_id != *expected
                        || commit.run.purpose != RunPurpose::Shadow
                        || commit.graph.kind != ArtifactKind::WorkflowGraph
                        || commit.graph.artifact_id != commit.run.graph_artifact_id
                })
        {
            return Err(StoreError::CanaryCampaignConflict(
                "canary workflow reservation binding".to_owned(),
            ));
        }
        for shadow in shadow_workflows {
            self.validate_workflow_commit(shadow)?;
        }
        if self.session_slot(&parent.session_key)?.is_some() {
            return Err(StoreError::CanaryCampaignConflict(
                "Paper session already exists without canary reservation".to_owned(),
            ));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, parent.reserved_at)?;
        Self::commit_session_slot_transaction(
            &transaction,
            lease,
            parent,
            proposal,
            Some((runtime_manifest, approval)),
        )?;
        for shadow in shadow_workflows {
            Self::commit_workflow_transaction(&transaction, shadow)?;
        }
        Self::commit_canary_session_transaction(&transaction, reservation)?;
        transaction.commit()?;
        drop(connection);
        let slot = self
            .session_slot(&parent.session_key)?
            .ok_or_else(|| StoreError::Integrity("session slot missing after commit".to_owned()))?;
        Ok(SessionSlotReservation {
            slot,
            newly_reserved: true,
        })
    }

    pub fn canary_session(
        &self,
        campaign_id: &ContentHash,
        level: CanaryCampaignStatus,
    ) -> StoreResult<Option<StoredCanarySession>> {
        if let Some(session) = self.canary_sessions(campaign_id, level)?.into_iter().next() {
            return Ok(Some(session));
        }
        let connection = self.connection()?;
        read_session(&connection, campaign_id, level)
    }

    pub fn canary_sessions(
        &self,
        campaign_id: &ContentHash,
        level: CanaryCampaignStatus,
    ) -> StoreResult<Vec<StoredCanarySession>> {
        let connection = self.connection()?;
        let Some(campaign) = read_campaign(&connection, campaign_id)? else {
            return Ok(Vec::new());
        };
        let Some(cohort) = campaign.spec.cohort(level) else {
            return Ok(Vec::new());
        };
        read_cohort_sessions(&connection, &cohort.cohort_id)
    }

    pub fn canary_session_by_key(
        &self,
        campaign_id: &ContentHash,
        level: CanaryCampaignStatus,
        session_key: &str,
    ) -> StoreResult<Option<StoredCanarySession>> {
        let connection = self.connection()?;
        let Some(campaign) = read_campaign(&connection, campaign_id)? else {
            return Ok(None);
        };
        if let Some(cohort) = campaign.spec.cohort(level) {
            return read_cohort_session_by_key(&connection, &cohort.cohort_id, session_key);
        }
        Ok(read_session(&connection, campaign_id, level)?
            .filter(|session| session.reservation.session_key == session_key))
    }

    pub fn canary_session_for_run(
        &self,
        run_id: &RunId,
    ) -> StoreResult<Option<StoredCanarySession>> {
        let connection = self.connection()?;
        self.canary_session_for_run_with_connection(&connection, run_id)
    }

    pub(crate) fn canary_session_for_run_with_connection(
        &self,
        connection: &Connection,
        run_id: &RunId,
    ) -> StoreResult<Option<StoredCanarySession>> {
        let cohort_reservation: Option<CohortSessionColumns> = connection
            .query_row(
                "SELECT cohort_id, campaign_id, stage_json, session_key, market_day, regime, parent_run_id, contract_shadow_run_id, topology_shadow_run_id, bundle_shadow_run_id, scheduler_epoch, reserved_at FROM rebuild_canary_cohort_sessions WHERE parent_run_id = ?1 OR contract_shadow_run_id = ?1 OR topology_shadow_run_id = ?1 OR bundle_shadow_run_id = ?1 LIMIT 1",
                params![run_id.0],
                |row| {
                    Ok(CohortSessionColumns {
                        cohort_id: row.get(0)?,
                        campaign_id: row.get(1)?,
                        stage_json: row.get(2)?,
                        session_key: row.get(3)?,
                        market_day: row.get(4)?,
                        regime: row.get(5)?,
                        parent_run_id: row.get(6)?,
                        contract_shadow_run_id: row.get(7)?,
                        topology_shadow_run_id: row.get(8)?,
                        bundle_shadow_run_id: row.get(9)?,
                        scheduler_epoch: row.get(10)?,
                        reserved_at: row.get(11)?,
                    })
                },
            )
            .optional()?;
        if let Some(columns) = cohort_reservation {
            return Ok(Some(stored_cohort_session_from_columns(columns)?));
        }
        let row: Option<(String, String)> = connection
            .query_row(
                "SELECT campaign_id, level_json FROM rebuild_canary_sessions WHERE parent_run_id = ?1 OR contract_shadow_run_id = ?1 OR topology_shadow_run_id = ?1 OR bundle_shadow_run_id = ?1 LIMIT 1",
                params![run_id.0],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((campaign_id, level_json)) = row else {
            return Ok(None);
        };
        let campaign_id = ContentHash::new(campaign_id)?;
        let level: CanaryCampaignStatus = serde_json::from_str(&level_json)?;
        read_session(connection, &campaign_id, level)
    }

    fn validate_campaign_artifacts(&self, spec: &CanaryCampaignSpec) -> StoreResult<()> {
        let references = [
            (
                &spec.candidate_contract,
                ArtifactKind::Contract,
                ArtifactLifecycle::Canonical,
            ),
            (
                &spec.candidate_topology,
                ArtifactKind::WorkflowGraph,
                ArtifactLifecycle::RunScoped,
            ),
            (
                &spec.runtime_manifest,
                ArtifactKind::RuntimeManifest,
                ArtifactLifecycle::Canonical,
            ),
            (
                &spec.paper_approval,
                ArtifactKind::PaperLaunchApproval,
                ArtifactLifecycle::Canonical,
            ),
        ];
        for (reference, expected_kind, expected_lifecycle) in references {
            let artifact = self.artifact(&reference.artifact_id)?;
            if artifact.kind != expected_kind
                || artifact.artifact_id != reference.artifact_id
                || artifact.lifecycle != expected_lifecycle
            {
                return Err(StoreError::CanaryCampaignConflict(
                    "campaign artifact closure".to_owned(),
                ));
            }
        }

        let candidate_contract_artifact = self.artifact(&spec.candidate_contract.artifact_id)?;
        let candidate_contract: AgentContract =
            serde_json::from_slice(&self.read_blob(&candidate_contract_artifact.blob)?)?;
        candidate_contract.validate()?;
        let candidate_topology_artifact = self.artifact(&spec.candidate_topology.artifact_id)?;
        let candidate_topology: WorkflowGraph =
            serde_json::from_slice(&self.read_blob(&candidate_topology_artifact.blob)?)?;
        candidate_topology.validate()?;
    if spec.cohorts.iter().any(|cohort| {
            cohort.candidate_contract_hash != candidate_contract.contract_hash
                || cohort.candidate_topology_id.0 != candidate_topology.topology_id
        }) {
            return Err(StoreError::CanaryCampaignConflict(
                "campaign candidate cohort identity".to_owned(),
        ));
    }
    let expected_candidate_hash = content_hash_json(&serde_json::json!({
        "candidate_contract_hash": candidate_contract.contract_hash,
        "candidate_topology_id": candidate_topology.topology_id,
    }))?;
    let mut cohort_certificates = BTreeSet::new();
        for cohort in &spec.cohorts {
            let Some(reference) = &cohort.search_bias_certificate else {
                continue;
            };
            cohort_certificates.insert(reference.clone());
            let artifact = self.artifact(&reference.artifact_id)?;
            if artifact.kind != ArtifactKind::SearchBiasCertificate
                || artifact.lifecycle != ArtifactLifecycle::Canonical
                || artifact.artifact_id != reference.artifact_id
            {
                return Err(StoreError::CanaryCampaignConflict(
                    "campaign search-bias certificate closure".to_owned(),
                ));
            }
            let certificate: SearchBiasCertificate =
                serde_json::from_slice(&self.read_blob(&artifact.blob)?)?;
            certificate.validate()?;
            let is_permitted = if let Some(acceptance_policy) = spec
                .promotion_policy
                .as_ref()
                .and_then(|policy| policy.search_bias_acceptance.as_ref())
            {
                certificate.permits_promotion(acceptance_policy)
            } else {
                certificate.is_promotion_ready()
            };
            if !is_permitted || artifact.source_refs != certificate.trial_refs {
                return Err(StoreError::CanaryCampaignConflict(
                    "campaign search-bias certificate is not promotion-ready".to_owned(),
                ));
            }
            let selected_artifact = self.artifact(&certificate.selected_trial.artifact_id)?;
        let selected_trial: ExperimentTrial =
            serde_json::from_slice(&self.read_blob(&selected_artifact.blob)?)?;
        selected_trial.validate()?;
        let mut complete_trial_refs = self
            .experiment_trial_ledger(&selected_trial.subject)?
            .into_iter()
            .map(|(reference, _)| reference)
            .collect::<Vec<_>>();
        complete_trial_refs.sort();
        let subject_matches_candidate = match &selected_trial.subject {
                PolicySubject::Contract(contract_hash) => {
                    contract_hash == &candidate_contract.contract_hash
                }
                PolicySubject::Topology(topology_id) => {
                    topology_id.0 == candidate_topology.topology_id
                }
                PolicySubject::Memory(_) => false,
            };
            if selected_artifact.kind != ArtifactKind::ExperimentTrial
                || selected_artifact.lifecycle != ArtifactLifecycle::Canonical
            || selected_trial.status != ExperimentTrialStatus::Selected
            || !selected_trial.is_contamination_controlled()
            || selected_trial.candidate_hash != expected_candidate_hash
            || complete_trial_refs != certificate.trial_refs
            || selected_trial.holdout_dataset_id != certificate.holdout_dataset_id
                || !subject_matches_candidate
        {
            return Err(StoreError::CanaryCampaignConflict(
                "campaign search-bias certificate does not bind the selected candidate and holdout"
                    .to_owned(),
            ));
        }
        }
        if cohort_certificates.len() > 1 {
            return Err(StoreError::CanaryCampaignConflict(
                "campaign cohorts must share one immutable search-bias certificate".to_owned(),
            ));
        }

        let manifest_artifact = self.artifact(&spec.runtime_manifest.artifact_id)?;
        let manifest: RuntimeManifest =
            serde_json::from_slice(&self.read_blob(&manifest_artifact.blob)?)?;
        manifest.validate()?;
        if manifest.code_revision != spec.source_revision
            || manifest.maximum_notional != spec.maximum_total_notional
        {
            return Err(StoreError::CanaryCampaignConflict(
                "campaign runtime manifest binding".to_owned(),
            ));
        }

        let approval_artifact = self.artifact(&spec.paper_approval.artifact_id)?;
        let approval: PaperLaunchApproval =
            serde_json::from_slice(&self.read_blob(&approval_artifact.blob)?)?;
        approval.validate()?;
        if approval.scope != PaperApprovalScope::Canary
            || approval.runtime_manifest != spec.runtime_manifest
            || approval.runtime_manifest_hash != manifest.manifest_hash()?
        {
            return Err(StoreError::CanaryCampaignConflict(
                "campaign Paper approval binding".to_owned(),
            ));
        }
        Ok(())
    }
}
