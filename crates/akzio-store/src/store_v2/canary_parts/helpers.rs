fn read_campaign(
    connection: &Connection,
    campaign_id: &ContentHash,
) -> StoreResult<Option<CanaryCampaignHead>> {
    let row: Option<(String, String, Option<String>, i64, String)> = connection
        .query_row(
            "SELECT spec_json, status_json, last_verdict_json, revision, updated_at FROM rebuild_canary_campaigns WHERE campaign_id = ?1",
            params![campaign_id.as_str()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()?;
    let Some((spec_json, status_json, verdict_json, revision, updated_at)) = row else {
        return Ok(None);
    };
    let revision = u64::try_from(revision)
        .map_err(|_| StoreError::Integrity("negative canary revision".to_owned()))?;
    Ok(Some(CanaryCampaignHead {
        spec: serde_json::from_str(&spec_json)?,
        status: serde_json::from_str(&status_json)?,
        last_verdict: verdict_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?,
        revision,
        updated_at: parse_time(&updated_at)
            .map_err(|error| StoreError::Integrity(error.to_string()))?,
    }))
}

fn read_session(
    connection: &Connection,
    campaign_id: &ContentHash,
    level: CanaryCampaignStatus,
) -> StoreResult<Option<StoredCanarySession>> {
    let row: Option<(String, String, String, String, String, i64, String)> = connection
        .query_row(
            "SELECT session_key, parent_run_id, contract_shadow_run_id, topology_shadow_run_id, bundle_shadow_run_id, scheduler_epoch, reserved_at FROM rebuild_canary_sessions WHERE campaign_id = ?1 AND level_json = ?2",
            params![campaign_id.as_str(), serde_json::to_string(&level)?],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()?;
    let Some((session_key, parent, contract, topology, bundle, scheduler_epoch, reserved_at)) = row
    else {
        return Ok(None);
    };
    let scheduler_epoch = u64::try_from(scheduler_epoch)
        .map_err(|_| StoreError::Integrity("negative canary scheduler epoch".to_owned()))?;
    Ok(Some(StoredCanarySession {
        reservation: CanarySessionReservation {
            schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
            campaign_id: campaign_id.clone(),
            level,
            session_key,
            parent_run_id: akzio_domain::RunId(parent),
            contract_shadow_run_id: akzio_domain::RunId(contract),
            topology_shadow_run_id: akzio_domain::RunId(topology),
            bundle_shadow_run_id: akzio_domain::RunId(bundle),
            scheduler_epoch,
            reserved_at: parse_time(&reserved_at)
                .map_err(|error| StoreError::Integrity(error.to_string()))?,
        },
    }))
}
