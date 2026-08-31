impl V2Store {
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
                schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
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
                "SELECT reservation_json FROM rebuild_canary_cohort_sessions ORDER BY cohort_id, session_key",
            )?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        for reservation_json in cohort_sessions {
            let reservation: CanarySessionReservation = serde_json::from_str(&reservation_json)?;
            reservation.validate()?;
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
