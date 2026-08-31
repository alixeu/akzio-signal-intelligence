impl V2Store {
    pub fn stage_canary_campaign(
        &self,
        lease: &DaemonLease,
        spec: &CanaryCampaignSpec,
        now: DateTime<Utc>,
    ) -> StoreResult<CanaryCampaignHead> {
        spec.validate()?;
        if !spec.has_paired_cohorts() {
            return Err(StoreError::CanaryCampaignConflict(
                "new canary campaigns require paired cohort manifests".to_owned(),
            ));
        }
        self.validate_campaign_artifacts(spec)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;

        if let Some(existing) = read_campaign(&transaction, &spec.campaign_id)? {
            if existing.spec != *spec {
                return Err(StoreError::CanaryCampaignConflict(
                    spec.campaign_id.to_string(),
                ));
            }
            transaction.commit()?;
            return Ok(existing);
        }

        let active_campaign: Option<String> = transaction
            .query_row(
                "SELECT campaign_id FROM rebuild_canary_campaigns WHERE active = 1 LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if active_campaign.is_some() {
            return Err(StoreError::CanaryCampaignConflict(
                active_campaign.unwrap_or_default(),
            ));
        }

        transaction.execute(
            "INSERT INTO rebuild_canary_campaigns (campaign_id, spec_json, status_json, last_verdict_json, revision, active, created_at, updated_at) VALUES (?1, ?2, ?3, NULL, 0, 1, ?4, ?4)",
            params![
                spec.campaign_id.as_str(),
                serde_json::to_string(spec)?,
                serde_json::to_string(&CanaryCampaignStatus::Staged)?,
                now.to_rfc3339(),
            ],
        )?;
        transaction.commit()?;
        Ok(CanaryCampaignHead {
            spec: spec.clone(),
            status: CanaryCampaignStatus::Staged,
            last_verdict: None,
            revision: 0,
            updated_at: now,
        })
    }

    pub fn canary_campaign(
        &self,
        campaign_id: &ContentHash,
    ) -> StoreResult<Option<CanaryCampaignHead>> {
        let connection = self.connection()?;
        read_campaign(&connection, campaign_id)
    }

    pub fn active_canary_campaign(&self) -> StoreResult<Option<CanaryCampaignHead>> {
        let connection = self.connection()?;
        let Some(campaign_id) = connection
            .query_row(
                "SELECT campaign_id FROM rebuild_canary_campaigns WHERE active = 1 LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        else {
            return Ok(None);
        };
        let campaign_id = ContentHash::new(campaign_id)?;
        read_campaign(&connection, &campaign_id)
    }

    pub fn transition_canary_campaign(
        &self,
        lease: &DaemonLease,
        campaign_id: &ContentHash,
        expected_status: CanaryCampaignStatus,
        verdict: CanaryVerdict,
        now: DateTime<Utc>,
    ) -> StoreResult<CanaryCampaignHead> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;
        let current = read_campaign(&transaction, campaign_id)?
            .ok_or_else(|| StoreError::MissingCanaryCampaign(campaign_id.to_string()))?;
        if let Some(idempotent) = idempotent_transition(&current, expected_status, verdict) {
            transaction.commit()?;
            return Ok(idempotent);
        }
        if current.status != expected_status {
            return Err(StoreError::CanaryCampaignConflict(format!(
                "{} expected {:?}, found {:?}",
                campaign_id, expected_status, current.status
            )));
        }
        if verdict == CanaryVerdict::Advance
            && current.status.is_level()
            && current.spec.has_paired_cohorts()
        {
            return Err(StoreError::CanaryCampaignConflict(
                "paired cohort evaluation is required to advance".to_owned(),
            ));
        }
        let updated = transition_campaign_transaction(
            &transaction,
            current,
            campaign_id,
            verdict,
            now,
        )?;
        transaction.commit()?;
        Ok(updated)
    }

    pub fn reserve_canary_session(
        &self,
        lease: &DaemonLease,
        reservation: &CanarySessionReservation,
    ) -> StoreResult<StoredCanarySession> {
        reservation.validate()?;
        if reservation.scheduler_epoch != lease.epoch {
            return Err(StoreError::SchedulerFenced(lease.lease_name.clone()));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, reservation.reserved_at)?;
        Self::commit_canary_session_transaction(&transaction, reservation)?;
        transaction.commit()?;
        drop(connection);
        if let Some(cohort_id) = &reservation.cohort_id {
            return self
                .canary_session_by_key(
                    &reservation.campaign_id,
                    reservation.level,
                    &reservation.session_key,
                )?
                .ok_or_else(|| {
                    StoreError::Integrity(format!(
                        "canary cohort session {cohort_id}/{} missing after commit",
                        reservation.session_key
                    ))
                });
        }
        let connection = self.connection()?;
        read_session(&connection, &reservation.campaign_id, reservation.level)?.ok_or_else(|| {
            StoreError::Integrity("legacy canary session missing after commit".to_owned())
        })
    }
}

fn idempotent_transition(
    current: &CanaryCampaignHead,
    expected_status: CanaryCampaignStatus,
    verdict: CanaryVerdict,
) -> Option<CanaryCampaignHead> {
    if current.last_verdict != Some(verdict) {
        return None;
    }
    let repeated = match verdict {
        CanaryVerdict::Advance => expected_status.next() == Some(current.status),
        CanaryVerdict::Rollback => current.status == CanaryCampaignStatus::Frozen,
        CanaryVerdict::Hold | CanaryVerdict::Defer => current.status == expected_status,
    };
    repeated.then(|| current.clone())
}

fn transition_campaign_transaction(
    transaction: &Transaction<'_>,
    current: CanaryCampaignHead,
    campaign_id: &ContentHash,
    verdict: CanaryVerdict,
    now: DateTime<Utc>,
) -> StoreResult<CanaryCampaignHead> {
    let next_status = match verdict {
        CanaryVerdict::Advance => current.status.next().ok_or_else(|| {
            StoreError::CanaryCampaignConflict(format!(
                "{} cannot advance from {:?}",
                campaign_id, current.status
            ))
        })?,
        CanaryVerdict::Hold | CanaryVerdict::Defer => current.status,
        CanaryVerdict::Rollback => CanaryCampaignStatus::Frozen,
    };
    let revision = current.revision.saturating_add(1);
    let active = i64::from(!matches!(
        next_status,
        CanaryCampaignStatus::Completed | CanaryCampaignStatus::Frozen
    ));
    transaction.execute(
        "UPDATE rebuild_canary_campaigns SET status_json = ?1, last_verdict_json = ?2, revision = ?3, active = ?4, updated_at = ?5 WHERE campaign_id = ?6",
        params![
            serde_json::to_string(&next_status)?,
            serde_json::to_string(&verdict)?,
            revision,
            active,
            now.to_rfc3339(),
            campaign_id.as_str(),
        ],
    )?;
    Ok(CanaryCampaignHead {
        spec: current.spec,
        status: next_status,
        last_verdict: Some(verdict),
        revision,
        updated_at: now,
    })
}
