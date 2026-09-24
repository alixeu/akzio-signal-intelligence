// 文件导读：paired cohort 的 observation 以 session+horizon 为幂等键写入，
// evaluation 只允许总结同一批已持久化事实；Store 保存 verdict，不替 learning 计算 verdict。
impl Store {
    // 在 daemon lease 保护的 Immediate 事务中记录当前阶段的 paired observations。
    // 每条 observation 必须绑定已预约 cohort session；相同 identity 可幂等重放，不同内容一律冲突。
    pub fn record_canary_observations(
        &self,
        lease: &DaemonLease,
        campaign_id: &ContentHash,
        stage: CanaryCampaignStatus,
        observations: &[CanaryPairedObservation],
        now: DateTime<Utc>,
    ) -> StoreResult<Vec<CanaryPairedObservation>> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;
        let campaign = read_campaign(&transaction, campaign_id)?
            .ok_or_else(|| StoreError::MissingCanaryCampaign(campaign_id.to_string()))?;
        if campaign.status != stage {
            return Err(StoreError::CanaryCampaignConflict(
                "canary observation stage changed".to_owned(),
            ));
        }
        let cohort = campaign.spec.cohort(stage).ok_or_else(|| {
            StoreError::CanaryCampaignConflict("canary cohort manifest missing".to_owned())
        })?;
        for observation in observations {
            // 先校验领域负载和 session 的 cohort/campaign/stage/交易日/regime 绑定，再允许写入。
            observation.validate()?;
            if observation.cohort_id != cohort.cohort_id {
                return Err(StoreError::CanaryCampaignConflict(
                    "canary observation cohort mismatch".to_owned(),
                ));
            }
            let session = read_cohort_session_by_key(
                &transaction,
                &cohort.cohort_id,
                &observation.session_key,
            )?
            .ok_or_else(|| {
                StoreError::CanaryCampaignConflict(
                    "canary observation session is not reserved".to_owned(),
                )
            })?;
            if session.reservation.campaign_id != *campaign_id
                || session.reservation.level != stage
                || session.reservation.market_day != Some(observation.market_day)
                || session.reservation.regime.as_deref() != Some(observation.regime.as_str())
            {
                return Err(StoreError::CanaryCampaignConflict(
                    "canary observation session binding mismatch".to_owned(),
                ));
            }
            let observation_id = observation.identity_hash();
            let existing: Option<String> = transaction
                .query_row(
                    "SELECT observation_json FROM rebuild_canary_observations WHERE cohort_id = ?1 AND session_key = ?2 AND horizon_json = ?3",
                    params![
                        cohort.cohort_id.as_str(),
                        observation.session_key,
                        serde_json::to_string(&observation.horizon)?,
                    ],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(existing) = existing {
                let existing: CanaryPairedObservation = serde_json::from_str(&existing)?;
                // 已存在的同 session+horizon 只能接受完全相同的不可变 observation。
                if existing != *observation {
                    return Err(StoreError::CanaryCampaignConflict(
                        "canary observation is immutable".to_owned(),
                    ));
                }
                continue;
            }
            // 新 observation 的 id 由完整内容 hash 决定；写入仍受同一 lease/事务保护。
            transaction.execute(
                "INSERT INTO rebuild_canary_observations (observation_id, cohort_id, campaign_id, stage_json, session_key, horizon_json, observation_json, recorded_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    observation_id.as_str(),
                    cohort.cohort_id.as_str(),
                    campaign_id.as_str(),
                    serde_json::to_string(&stage)?,
                    observation.session_key,
                    serde_json::to_string(&observation.horizon)?,
                    serde_json::to_string(observation)?,
                    now.to_rfc3339(),
                ],
            )?;
        }
        transaction.commit()?;
        drop(connection);
        // 释放 Store 连接后再重新读取，避免在非可重入 Mutex 上嵌套获取连接。
        self.canary_observations(&cohort.cohort_id)
    }

    // 按 session_key/horizon 顺序读取 cohort 的不可变 observation，并反序列化为领域值。
    pub fn canary_observations(
        &self,
        cohort_id: &ContentHash,
    ) -> StoreResult<Vec<CanaryPairedObservation>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT observation_json FROM rebuild_canary_observations WHERE cohort_id = ?1 ORDER BY session_key, horizon_json",
        )?;
        let rows = statement
            .query_map(params![cohort_id.as_str()], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|row| Ok(serde_json::from_str(&row)?))
            .collect()
    }

    // 校验 learning 已计算的 cohort evaluation 与 Store 中的 observation 摘要一致，
    // 再在 lease 事务内幂等写入 evaluation 并推进预期阶段。Store 只持久化决定，不计算 verdict。
    pub fn transition_canary_campaign_with_evaluation(
        &self,
        lease: &DaemonLease,
        campaign_id: &ContentHash,
        expected_status: CanaryCampaignStatus,
        evaluation: &CanaryCohortEvaluation,
        now: DateTime<Utc>,
    ) -> StoreResult<CanaryCampaignHead> {
        evaluation.validate()?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;
        let current = read_campaign(&transaction, campaign_id)?
            .ok_or_else(|| StoreError::MissingCanaryCampaign(campaign_id.to_string()))?;
        let cohort = current.spec.cohort(expected_status).ok_or_else(|| {
            StoreError::CanaryCampaignConflict("canary cohort manifest missing".to_owned())
        })?;
        if evaluation.cohort_id != cohort.cohort_id
            || evaluation.promotion_policy_hash != cohort.promotion_policy_hash
            || evaluation.search_bias_certificate != cohort.search_bias_certificate
        {
            return Err(StoreError::CanaryCampaignConflict(
                "canary evaluation binding mismatch".to_owned(),
            ));
        }
        let (observation_set_hash, counts, market_days, regimes) =
            cohort_observation_summary(&transaction, &cohort.cohort_id)?;
        if evaluation.observation_set_hash != observation_set_hash
            || evaluation.paired_sessions_by_horizon != counts
            || evaluation.distinct_market_days != market_days
            || evaluation.covered_regimes != regimes
        {
            return Err(StoreError::CanaryCampaignConflict(
                "canary evaluation does not summarize persisted observations".to_owned(),
            ));
        }
        // evaluation_id 重复时只接受同一 cohort/verdict 的历史内容；不允许覆盖已提交评价。
        let existing: Option<String> = transaction
            .query_row(
                "SELECT evaluation_json FROM rebuild_canary_evaluations WHERE evaluation_id = ?1",
                params![evaluation.evaluation_id.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        let evaluation_exists = if let Some(existing) = existing {
            let existing: CanaryCohortEvaluation = serde_json::from_str(&existing)?;
            if existing.evaluation_id != evaluation.evaluation_id
                || existing.cohort_id != evaluation.cohort_id
                || existing.verdict != evaluation.verdict
            {
                return Err(StoreError::CanaryCampaignConflict(
                    "canary evaluation is immutable".to_owned(),
                ));
            }
            true
        } else {
            false
        };
        if let Some(idempotent) = idempotent_transition(&current, expected_status, evaluation.verdict)
        {
            // 状态已经处于幂等终点时，原 evaluation 也必须已存在，避免只凭状态伪造历史。
            if !evaluation_exists {
                return Err(StoreError::CanaryCampaignConflict(
                    "idempotent canary transition requires the original evaluation".to_owned(),
                ));
            }
            transaction.commit()?;
            return Ok(idempotent);
        }
        if current.status != expected_status {
            return Err(StoreError::CanaryCampaignConflict(format!(
                "{} expected {:?}, found {:?}",
                campaign_id, expected_status, current.status
            )));
        }
        if !evaluation_exists {
            // 评价先与阶段一起落库；后续 transition 失败时整个事务回滚。
            transaction.execute(
                "INSERT INTO rebuild_canary_evaluations (evaluation_id, cohort_id, campaign_id, stage_json, evaluation_json, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    evaluation.evaluation_id.as_str(),
                    evaluation.cohort_id.as_str(),
                    campaign_id.as_str(),
                    serde_json::to_string(&expected_status)?,
                    serde_json::to_string(evaluation)?,
                    now.to_rfc3339(),
                ],
            )?;
        }
        let updated = transition_campaign_transaction(
            &transaction,
            current,
            campaign_id,
            evaluation.verdict,
            now,
        )?;
        transaction.commit()?;
        Ok(updated)
    }
}

// 从持久 observation 计算确定性的集合 hash、各 horizon 数量、交易日集合和 regime 集合，
// 供 transition 校验提交的 evaluation 是否覆盖同一批事实。
fn cohort_observation_summary(
    connection: &Connection,
    cohort_id: &ContentHash,
) -> StoreResult<(ContentHash, [u64; 3], u64, BTreeSet<String>)> {
    let mut statement = connection.prepare(
        "SELECT observation_json FROM rebuild_canary_observations WHERE cohort_id = ?1 ORDER BY session_key, horizon_json",
    )?;
    let rows = statement
        .query_map(params![cohort_id.as_str()], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    let mut hashes = Vec::with_capacity(rows.len());
    let mut counts = [0_u64; 3];
    let mut market_days = BTreeSet::new();
    let mut regimes = BTreeSet::new();
    for row in rows {
        let observation: CanaryPairedObservation = serde_json::from_str(&row)?;
        hashes.push(observation.identity_hash());
        counts[match observation.horizon {
            OutcomeHorizon::T1 => 0,
            OutcomeHorizon::T3 => 1,
            OutcomeHorizon::T5 => 2,
        }] += 1;
        market_days.insert(observation.market_day);
        regimes.insert(observation.regime);
    }
    hashes.sort();
    let hash = content_hash_json(&serde_json::json!(hashes))?;
    Ok((hash, counts, market_days.len() as u64, regimes))
}
