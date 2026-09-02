impl Store {
    pub(crate) fn canary_cross_run_reference_allowed(&self, connection: &Connection, child: &Artifact, parent: &Artifact) -> StoreResult<bool> {
        let Some(run_id) = child.origin.as_ref().and_then(|o|o.run_id.as_ref()) else { return Ok(false); };
        if parent.kind == ArtifactKind::NormalizedEvidence
            || (child.kind == ArtifactKind::SemanticDetail
                && child.producer == "canary.evidence_snapshot"
                && child.provenance.source_family == "akzio.ingest"
                && parent.kind == ArtifactKind::SemanticDetail
                && parent.producer == "evidence.collection_status")
        {
            return self.is_canary_parent_evidence_with_connection(connection, run_id, &parent.artifact_id);
        }
        if run_purpose_from_connection(connection, run_id)? != RunPurpose::Shadow
            || child.provenance.source_family != "akzio-learning"
            || !matches!((child.kind, child.producer.as_str()),
                (ArtifactKind::EvidenceNeed, "learning.outcome_worker.need")
                | (ArtifactKind::OutcomeSchedule, "learning.shadow_outcome_schedule"))
        { return Ok(false); }
        let Some(session) = self.canary_session_for_run_with_connection(connection, run_id)? else { return Ok(false); };
        let Some(schedule_artifact) = super::read_run_kind_artifacts(connection, &session.reservation.parent_run_id, ArtifactKind::OutcomeSchedule)?.into_iter().find(|artifact|artifact.lifecycle == ArtifactLifecycle::Canonical) else { return Ok(false); };
        if child.kind == ArtifactKind::EvidenceNeed {
            return Ok(parent.artifact_id == schedule_artifact.artifact_id);
        }
        let schedule: akzio_domain::OutcomeSchedule = self.read_artifact_payload_with_connection(connection, &schedule_artifact)?;
        schedule.validate()?;
        let mut frozen_refs = vec![schedule.execution_context];
        match schedule.execution {
            akzio_domain::OutcomeExecutionLineage::NoOrder { execution_verdict } => frozen_refs.push(execution_verdict),
            akzio_domain::OutcomeExecutionLineage::ReconciledPaper { execution_verdict, commitment, reconciliation } => frozen_refs.extend([execution_verdict, commitment, reconciliation]),
        }
        Ok(frozen_refs.iter().any(|r|r.artifact_id == parent.artifact_id && r.kind == parent.kind))
    }

    /// Check one grant without reloading the entire parent workflow/evidence
    /// set for every Context validation and tool read.
    pub fn is_canary_parent_evidence(&self, shadow_run: &RunId, artifact_id: &akzio_domain::ArtifactId) -> StoreResult<bool> {
        let connection=self.connection()?;
        self.is_canary_parent_evidence_with_connection(&connection, shadow_run, artifact_id)
    }

    fn is_canary_parent_evidence_with_connection(&self, connection: &Connection, shadow_run: &RunId, artifact_id: &akzio_domain::ArtifactId) -> StoreResult<bool> {
        if run_purpose_from_connection(connection, shadow_run)? != RunPurpose::Shadow { return Ok(false); }
        let Some(session)=self.canary_session_for_run_with_connection(connection, shadow_run)? else { return Ok(false); };
        let reservation=session.reservation;
        if ![&reservation.contract_shadow_run_id,&reservation.topology_shadow_run_id,&reservation.bundle_shadow_run_id].contains(&shadow_run) { return Ok(false); }
        Ok(connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM rebuild_attempt_outputs o JOIN rebuild_artifacts r ON r.artifact_id=o.artifact_id WHERE o.artifact_id=?1 AND ((r.kind=?2 AND r.producer GLOB 'akzio.ingest.*.normalized') OR (r.kind=?4 AND r.producer='evidence.collection_status')) AND o.attempt_id=(SELECT a.attempt_id FROM rebuild_tasks t JOIN rebuild_attempts a ON a.task_id=t.task_id WHERE t.run_id=?3 AND t.recipe_id='gate.evidence' AND t.status='succeeded' AND a.status='succeeded' ORDER BY a.finished_at DESC,a.attempt_id DESC LIMIT 1))",
            params![artifact_id.0.as_str(),super::enum_name(ArtifactKind::NormalizedEvidence),reservation.parent_run_id.0,super::enum_name(ArtifactKind::SemanticDetail)],|row|row.get(0))?)
    }
    /// The three registered shadows compare the exact committed T0 evidence.
    /// This is the sole cross-Run evidence grant: later refreshes, arbitrary
    /// parent artifacts and unregistered Shadow runs are not included.
    pub fn canary_parent_evidence(&self, shadow_run: &RunId) -> StoreResult<Option<Vec<Artifact>>> {
        if self.run_purpose(shadow_run)? != RunPurpose::Shadow {
            return Err(StoreError::CanaryCampaignConflict("evidence consumer is not a Shadow run".to_owned()));
        }
        let session = self.canary_session_for_run(shadow_run)?.ok_or_else(||StoreError::CanaryCampaignConflict("unregistered Shadow evidence consumer".to_owned()))?;
        let reservation=&session.reservation;
        if ![&reservation.contract_shadow_run_id,&reservation.topology_shadow_run_id,&reservation.bundle_shadow_run_id].contains(&shadow_run) {
            return Err(StoreError::CanaryCampaignConflict("Shadow evidence binding".to_owned()));
        }
        let parent=self.workflow_snapshot(&reservation.parent_run_id)?;
        let gate=parent.tasks.iter().find(|task|task.node.recipe_id.as_str()=="gate.evidence").ok_or_else(||StoreError::CanaryCampaignConflict("parent evidence gate missing".to_owned()))?;
        if !gate.status.is_terminal() { return Ok(None); }
        if gate.status != akzio_domain::TaskStatus::Succeeded {
            return Err(StoreError::CanaryCampaignConflict("parent evidence gate did not succeed".to_owned()));
        }
        Ok(Some(self.succeeded_task_outputs_or_empty(&reservation.parent_run_id,&gate.node.task_id)?.into_iter().filter(|artifact|
            (artifact.kind==ArtifactKind::NormalizedEvidence && artifact.producer.starts_with("akzio.ingest."))
            || (artifact.kind==ArtifactKind::SemanticDetail && artifact.producer=="evidence.collection_status")
        ).collect()))
    }
    pub(crate) fn verify_canary_campaign_history(
        &self,
        connection: &Connection,
    ) -> StoreResult<()> {
        let active_count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM rebuild_canary_campaigns WHERE active = 1",
            [],
            |row| row.get(0),
        )?;
        if active_count > 1 {
            return Err(StoreError::Integrity(
                "more than one canary campaign is active".to_owned(),
            ));
        }

        let campaign_ids = connection
            .prepare("SELECT campaign_id FROM rebuild_canary_campaigns ORDER BY campaign_id")?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        for campaign_id in campaign_ids {
            let campaign_id = ContentHash::new(campaign_id)?;
            let head = read_campaign(connection, &campaign_id)?.ok_or_else(|| {
                StoreError::Integrity(format!("canary campaign {campaign_id} disappeared"))
            })?;
            head.spec.validate()?;
            let expected_active = i64::from(!matches!(
                head.status,
                CanaryCampaignStatus::Completed | CanaryCampaignStatus::Frozen
            ));
            let active: i64 = connection.query_row(
                "SELECT active FROM rebuild_canary_campaigns WHERE campaign_id = ?1",
                params![campaign_id.as_str()],
                |row| row.get(0),
            )?;
            if active != expected_active {
                return Err(StoreError::Integrity(format!(
                    "canary campaign {campaign_id} active flag disagrees with status"
                )));
            }
        }

        let mut sessions = connection.prepare(
            "SELECT campaign_id, level_json, session_key, parent_run_id, contract_shadow_run_id, topology_shadow_run_id, bundle_shadow_run_id, scheduler_epoch, reserved_at FROM rebuild_canary_sessions ORDER BY campaign_id, level_json",
        )?;
        let rows = sessions.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, u64>(7)?,
                row.get::<_, String>(8)?,
            ))
        })?;
        for row in rows {
            let (
                campaign_id,
                level_json,
                session_key,
                parent_run_id,
                contract_shadow_run_id,
                topology_shadow_run_id,
                bundle_shadow_run_id,
                scheduler_epoch,
                reserved_at,
            ) = row?;
            let campaign_id = ContentHash::new(campaign_id)?;
            let level: CanaryCampaignStatus = serde_json::from_str(&level_json)?;
            let reservation = CanarySessionReservation {
                schema_version: akzio_domain::DOMAIN_SCHEMA_VERSION,
                campaign_id: campaign_id.clone(),
                level,
                session_key,
                cohort_id: None,
                market_day: None,
                regime: None,
                parent_run_id: akzio_domain::RunId(parent_run_id),
                contract_shadow_run_id: akzio_domain::RunId(contract_shadow_run_id),
                topology_shadow_run_id: akzio_domain::RunId(topology_shadow_run_id),
                bundle_shadow_run_id: akzio_domain::RunId(bundle_shadow_run_id),
                scheduler_epoch,
                reserved_at: parse_time(&reserved_at)?,
            };
            reservation.validate()?;
            read_campaign(connection, &campaign_id)?.ok_or_else(|| {
                StoreError::Integrity(format!(
                    "canary session references missing campaign {campaign_id}"
                ))
            })?;
            if run_purpose_from_connection(connection, &reservation.parent_run_id)?
                    != RunPurpose::Paper
                || run_purpose_from_connection(connection, &reservation.contract_shadow_run_id)?
                    != RunPurpose::Shadow
                || run_purpose_from_connection(connection, &reservation.topology_shadow_run_id)?
                    != RunPurpose::Shadow
                || run_purpose_from_connection(connection, &reservation.bundle_shadow_run_id)?
                    != RunPurpose::Shadow
            {
                return Err(StoreError::Integrity(
                    "canary session lineage is invalid".to_owned(),
                ));
            }
        }

    let cohort_sessions = connection
        .prepare(
            "SELECT cohort_id, campaign_id, stage_json, session_key, market_day, regime, parent_run_id, contract_shadow_run_id, topology_shadow_run_id, bundle_shadow_run_id, scheduler_epoch, reserved_at FROM rebuild_canary_cohort_sessions ORDER BY cohort_id, session_key",
        )?
        .query_map([], cohort_session_columns)?
        .collect::<Result<Vec<_>, _>>()?;
    for columns in cohort_sessions {
        let reservation = stored_cohort_session_from_columns(columns)?.reservation;
            let campaign = read_campaign(connection, &reservation.campaign_id)?.ok_or_else(|| {
                StoreError::Integrity("canary cohort session campaign is missing".to_owned())
            })?;
            validate_session_cohort(&campaign, &reservation)?;
            if run_purpose_from_connection(connection, &reservation.parent_run_id)?
                != RunPurpose::Paper
                || run_purpose_from_connection(connection, &reservation.contract_shadow_run_id)?
                    != RunPurpose::Shadow
                || run_purpose_from_connection(connection, &reservation.topology_shadow_run_id)?
                    != RunPurpose::Shadow
                || run_purpose_from_connection(connection, &reservation.bundle_shadow_run_id)?
                    != RunPurpose::Shadow
            {
                return Err(StoreError::Integrity(
                    "canary cohort session lineage is invalid".to_owned(),
                ));
            }
        }

        let observations = connection
            .prepare(
                "SELECT campaign_id, stage_json, observation_json FROM rebuild_canary_observations ORDER BY observation_id",
            )?
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for (campaign_id, stage_json, observation_json) in observations {
            let campaign_id = ContentHash::new(campaign_id)?;
            let stage: CanaryCampaignStatus = serde_json::from_str(&stage_json)?;
            let observation: CanaryPairedObservation = serde_json::from_str(&observation_json)?;
            observation.validate()?;
            let campaign = read_campaign(connection, &campaign_id)?.ok_or_else(|| {
                StoreError::Integrity("canary observation campaign is missing".to_owned())
            })?;
            let cohort = campaign.spec.cohort(stage).ok_or_else(|| {
                StoreError::Integrity("canary observation cohort is missing".to_owned())
            })?;
            if observation.cohort_id != cohort.cohort_id
                || read_cohort_session_by_key(
                    connection,
                    &cohort.cohort_id,
                    &observation.session_key,
                )?
                .is_none()
            {
                return Err(StoreError::Integrity(
                    "canary observation lineage is invalid".to_owned(),
                ));
            }
        }

        let evaluations = connection
            .prepare(
                "SELECT campaign_id, stage_json, evaluation_json FROM rebuild_canary_evaluations ORDER BY evaluation_id",
            )?
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for (campaign_id, stage_json, evaluation_json) in evaluations {
            let campaign_id = ContentHash::new(campaign_id)?;
            let stage: CanaryCampaignStatus = serde_json::from_str(&stage_json)?;
            let evaluation: CanaryCohortEvaluation = serde_json::from_str(&evaluation_json)?;
            evaluation.validate()?;
            let campaign = read_campaign(connection, &campaign_id)?.ok_or_else(|| {
                StoreError::Integrity("canary evaluation campaign is missing".to_owned())
            })?;
            let cohort = campaign.spec.cohort(stage).ok_or_else(|| {
                StoreError::Integrity("canary evaluation cohort is missing".to_owned())
            })?;
            if evaluation.cohort_id != cohort.cohort_id
                || evaluation.promotion_policy_hash != cohort.promotion_policy_hash
            {
                return Err(StoreError::Integrity(
                    "canary evaluation lineage is invalid".to_owned(),
                ));
            }
        }
        Ok(())
    }
}
