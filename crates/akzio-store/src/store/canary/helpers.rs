// 文件导读：这些 helper 只从 campaign/session 表恢复 immutable head 和 reservation，
// 同时兼容旧 level 编码；它们不会产生新的 session，也不会改变 active 标志。
// 从 campaign 表读取一个可选的 immutable head，并把 JSON、revision、时间恢复为领域对象。
// campaign 不存在返回 None；负 revision 或坏 JSON/时间属于 Store 完整性错误。
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

// 按当前阶段读取旧式单 session 表，同时兼容历史 canary10/25/50 的 level 序列化名称。
// 该路径只恢复 legacy session；cohort 绑定字段由 paired cohort 表单独保存。
fn read_session(
    connection: &Connection,
    campaign_id: &ContentHash,
    level: CanaryCampaignStatus,
) -> StoreResult<Option<StoredCanarySession>> {
    let current_level = serde_json::to_string(&level)?;
    let legacy_level = level
        .legacy_storage_name()
        .map(serde_json::to_string)
        .transpose()?;
    let row: Option<(String, String, String, String, String, i64, String)> = connection
        .query_row(
            "SELECT session_key, parent_run_id, contract_shadow_run_id, topology_shadow_run_id, bundle_shadow_run_id, scheduler_epoch, reserved_at FROM rebuild_canary_sessions WHERE campaign_id = ?1 AND (level_json = ?2 OR level_json = ?3)",
            params![campaign_id.as_str(), current_level, legacy_level],
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
            schema_version: akzio_domain::DOMAIN_SCHEMA_VERSION,
            campaign_id: campaign_id.clone(),
            level,
            session_key,
            cohort_id: None,
            market_day: None,
            regime: None,
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

// 通过 cohort_id 和 session_key 读取唯一 paired session，并交给统一列转换器做领域校验。
fn read_cohort_session_by_key(
    connection: &Connection,
    cohort_id: &ContentHash,
    session_key: &str,
) -> StoreResult<Option<StoredCanarySession>> {
    let columns = connection
        .query_row(
            "SELECT cohort_id, campaign_id, stage_json, session_key, market_day, regime, parent_run_id, contract_shadow_run_id, topology_shadow_run_id, bundle_shadow_run_id, scheduler_epoch, reserved_at FROM rebuild_canary_cohort_sessions WHERE cohort_id = ?1 AND session_key = ?2",
            params![cohort_id.as_str(), session_key],
            cohort_session_columns,
        )
        .optional()?;
    columns
        .map(stored_cohort_session_from_columns)
        .transpose()
}

// 读取一个 cohort 的全部 paired sessions，并按 session_key 稳定排序后逐条转换。
fn read_cohort_sessions(
    connection: &Connection,
    cohort_id: &ContentHash,
) -> StoreResult<Vec<StoredCanarySession>> {
    let mut statement = connection.prepare(
        "SELECT cohort_id, campaign_id, stage_json, session_key, market_day, regime, parent_run_id, contract_shadow_run_id, topology_shadow_run_id, bundle_shadow_run_id, scheduler_epoch, reserved_at FROM rebuild_canary_cohort_sessions WHERE cohort_id = ?1 ORDER BY session_key",
    )?;
    let rows = statement
        .query_map(params![cohort_id.as_str()], cohort_session_columns)?
        .collect::<Result<Vec<_>, _>>()?;
    rows.into_iter()
        .map(stored_cohort_session_from_columns)
        .collect()
}

// 根据 campaign 当前阶段判断 reservation 是否必须绑定 cohort、market day 和 regime，
// 或者必须保持 legacy session 的三个字段为空；这里不修改 reservation。
fn validate_session_cohort(
    campaign: &CanaryCampaignHead,
    reservation: &CanarySessionReservation,
) -> StoreResult<()> {
    match campaign.spec.cohort(reservation.level) {
        Some(cohort) => {
            if reservation.cohort_id.as_ref() != Some(&cohort.cohort_id)
                || reservation.market_day.is_none()
                || reservation
                    .market_day
                    .and_then(|day| cohort.regime_for(day))
                    != reservation.regime.as_deref()
            {
                return Err(StoreError::CanaryCampaignConflict(
                    "canary session cohort binding".to_owned(),
                ));
            }
        }
        None => {
            if reservation.cohort_id.is_some()
                || reservation.market_day.is_some()
                || reservation.regime.is_some()
            {
                return Err(StoreError::CanaryCampaignConflict(
                    "legacy canary session cannot bind a cohort".to_owned(),
                ));
            }
        }
    }
    Ok(())
}
