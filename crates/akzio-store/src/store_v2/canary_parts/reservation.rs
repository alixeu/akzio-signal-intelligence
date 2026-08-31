impl V2Store {
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
                "INSERT INTO rebuild_canary_cohort_sessions (cohort_id, campaign_id, stage_json, session_key, reservation_json, parent_run_id, contract_shadow_run_id, topology_shadow_run_id, bundle_shadow_run_id, scheduler_epoch, reserved_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    cohort_id.as_str(),
                    reservation.campaign_id.as_str(),
                    serde_json::to_string(&reservation.level)?,
                    reservation.session_key,
                    serde_json::to_string(reservation)?,
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
        let cohort_reservation: Option<String> = connection
            .query_row(
                "SELECT reservation_json FROM rebuild_canary_cohort_sessions WHERE parent_run_id = ?1 OR contract_shadow_run_id = ?1 OR topology_shadow_run_id = ?1 OR bundle_shadow_run_id = ?1 LIMIT 1",
                params![run_id.0],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(reservation) = cohort_reservation {
            return Ok(Some(StoredCanarySession {
                reservation: serde_json::from_str(&reservation)?,
            }));
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
        read_session(&connection, &campaign_id, level)
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
