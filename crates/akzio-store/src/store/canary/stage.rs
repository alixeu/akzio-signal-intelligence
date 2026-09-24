// 文件导读：Campaign head 的 stage/transition 使用 daemon lease 和 Immediate 事务；
// 重复 verdict 只返回已有 head，状态推进不会重新计算或覆盖历史评价。
// stage_canary_campaign 建立唯一 active campaign；campaign/active_* 提供只读重建；
// transition_* 根据 caller 给出的 verdict 更新 revision，reservation 另有专门事务入口。
impl Store {
    // 输入经领域校验的 campaign spec、fenced daemon lease 和操作时间；输出新建或同内容重放的 head。
    // campaign validation 在拿写事务前完成；campaign 行仅在 lease 校验通过且没有其他 active 行时创建。
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
            // 相同 campaign_id + 完全相同 spec 是幂等重放；不同内容拒绝覆盖，提交的只读事务不改 revision。
            if existing.spec != *spec {
                return Err(StoreError::CanaryCampaignConflict(
                    spec.campaign_id.to_string(),
                ));
            }
            transaction.commit()?;
            return Ok(existing);
        }

        // 对数据库中 active=1 的行做单例冲突检查；立即事务把该检查与后续 INSERT 串行化。
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

        // 首次安装写 Staged、revision=0、active=1；这里没有输入 verdict，也没有学习侧晋级计算。
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
        // 按唯一 campaign_id 恢复可选 head；坏 JSON/时间不会被当作不存在。
        let connection = self.connection()?;
        read_campaign(&connection, campaign_id)
    }

    pub fn active_canary_campaign(&self) -> StoreResult<Option<CanaryCampaignHead>> {
        // 此读取只用 LIMIT 1 取 active=1 的一行；零行返回 None，多个 active 行不会在这里报错，
        // 全局单例性由写事务和 Doctor 的历史检查保证，不能把一次查询当作完整性证明。
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
        // 输入 caller 已计算的 Advance/Hold/Defer/Rollback verdict 与期望状态；事务内再检查 lease 和 CAS 状态。
        // 返回 head 代表转换已提交或幂等地确认已有终点，不代表候选已执行或 Paper 订单已产生。
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;
        let current = read_campaign(&transaction, campaign_id)?
            .ok_or_else(|| StoreError::MissingCanaryCampaign(campaign_id.to_string()))?;
        if let Some(idempotent) = idempotent_transition(&current, expected_status, verdict) {
            // 重试只接受同一个 last_verdict 与可达终点组合，不追加 revision。
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
            // 配置 paired cohort 的 level 必须先经带 observation summary 的 evaluation 接口推进。
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
        // 将 scheduler 预留绑定到 lease epoch；在 Immediate 事务中重验 daemon lease 并写 session，
        // commit 后释放当前连接锁，再按 paired/legacy 表读取结果，避免 Store Mutex 重入。
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
            // paired session 以 cohort key 回读；legacy session 则用当前阶段的兼容读取器回读。
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
    // 只有 verdict 与已记录 last_verdict 相同才可能幂等；状态是否为该 verdict 的合法终点由下方匹配判断。
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
    // 消费当前 head 并按 verdict 推导 next_status；Advance 在终态失败，Hold/Defer 保持阶段，
    // Rollback 冻结。更新行在调用方事务内生效，revision 饱和递增，active 由新状态重新派生。
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
