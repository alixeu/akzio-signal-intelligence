// 文件导读：Canary reservation 把 Paper parent、三个 Shadow workflow 和 campaign
// session 绑定到同一事务；session_key/cohort 主键提供持久幂等，不代表已经下单或成交。
// 先看 commit_canary_session_transaction 的 legacy/paired 双路径，再看 reserve_*_with_workflows
// 如何把该记录与 Paper slot、四张 workflow 一起提交；后续 canary_session* 全是只读恢复入口。
impl Store {
    // 在调用方事务内把一个 Canary session 写入对应表；先验证 campaign/阶段/cohort/Run purpose，
    // 再用不可变 session_key 做幂等保护。该 helper 只建立 Paper/Shadow 调度预留，不提交订单。
    pub(super) fn commit_canary_session_transaction(
        transaction: &Transaction<'_>,
        reservation: &CanarySessionReservation,
    ) -> StoreResult<()> {
        // 借用外层 Transaction 与 reservation，不拿新连接、不 commit；校验顺序是 campaign 阶段、
        // cohort 形状、四个 Run purpose，再按 paired 或 legacy 主键处理幂等/冲突。
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
            // paired cohort 必须带交易日和 regime；已有相同主键只能逐字段相等地重放。
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
                // 主键已存在时只允许完整 reservation 逐字段相等；幂等重放不更新 reserved_at/epoch。
                if existing.reservation != *reservation {
                    return Err(StoreError::CanaryCampaignConflict(
                        "canary cohort session is immutable".to_owned(),
                    ));
                }
                return Ok(());
            }
            // paired 表插入前同时查 legacy 与 paired 表，避免一个 session_key 在两种 schema 中双占。
            let duplicate_session: Option<String> = transaction
                .query_row(
                    "SELECT campaign_id FROM rebuild_canary_sessions WHERE session_key = ?1 UNION ALL SELECT campaign_id FROM rebuild_canary_cohort_sessions WHERE session_key = ?1 LIMIT 1",
                    params![reservation.session_key],
                    |row| row.get(0),
                )
                .optional()?;
            if duplicate_session.is_some() {
                // session_key 在 legacy 与 paired 两张表之间也必须全局唯一，避免同一交易 session 双占用。
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
        // 没有 cohort_id 时走历史单 session 表；它仍按 campaign+level 幂等，
        // 下方重复检查仅覆盖 legacy 表；不能单凭该查询断言跨 paired 表的 session_key 全局唯一。
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
    // 预校验 Paper parent、审批、三个 Shadow workflow 与 scheduler epoch，随后在同一 Immediate
    // 事务内占用 session slot、写入 workflow 和 Canary reservation；返回 slot 不代表 Execution/PaperCommit。
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
        // 输入需要三张 Shadow WorkflowCommit、Paper parent/Proposal/Approval 和 reservation；输出仅是
        // 已提交的 Paper SessionSlot 快照及 newly_reserved 标志，不代表后续 Research/Decision/Execution 已运行。
        // 事务外先验结构/lineage，事务内再重验 daemon lease 并共同提交所有持久化行。
        if shadow_workflows.len() != 3 {
            return Err(StoreError::CanaryCampaignConflict(
                "canary session requires three shadow workflows".to_owned(),
            ));
        }
        // 这些检查发生在获取写事务前，避免把数量或绑定错误带入 Store 状态变更。
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
            // 每个 Shadow graph 还要经过通用 workflow 校验，包括旧研究链路退役和 Contract 版本边界。
            self.validate_workflow_commit(shadow)?;
        }
        // 先给出清楚的既有 slot 冲突；写事务中的唯一约束/检查仍负责最终防止并发重复。
        if self.session_slot(&parent.session_key)?.is_some() {
            return Err(StoreError::CanaryCampaignConflict(
                "Paper session already exists without canary reservation".to_owned(),
            ));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, parent.reserved_at)?;
        // session slot、parent workflow、Shadow workflow 和 Canary session 必须原子提交，不能部分成功。
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
        // Paper slot/setup、parent graph、三张 Shadow graph 与 reservation 共用这一提交点；
        // 任一写入失败由 Transaction Drop 回滚本批行，不把部分 workflow 留作成功 reservation。
        transaction.commit()?;
        drop(connection);
        // 提交后再读取 slot，避免在仍持有 Store 连接时调用会再次获取连接的查询。
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
        // 输入 campaign+阶段；paired session 优先，只有当前 cohort 没有记录时才回退 legacy 表。
        // 返回 None 表示两种表都未找到，不改变 reservation。
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
        // 输入 campaign+阶段；campaign 不存在或该阶段没有 paired cohort 时返回空集合，
        // 否则按 cohort_id 读取该组排序 session。该接口不含 legacy 单 session，也不改变数据库。
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
        // 输入 campaign、阶段和 session_key，按该阶段配置选择 paired/legacy 表；
        // legacy 查询先按 campaign+level 找行，再由 filter 精确核对 session_key。
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
        // 输入任一 parent/Shadow Run ID；此便捷入口只获取一次 Store guard，再交给连接借用版本查询。
        let connection = self.connection()?;
        self.canary_session_for_run_with_connection(&connection, run_id)
    }

    pub(crate) fn canary_session_for_run_with_connection(
        &self,
        connection: &Connection,
        run_id: &RunId,
    ) -> StoreResult<Option<StoredCanarySession>> {
        // 使用调用方连接先按四个 Run lineage 字段查 paired 表，没命中才查 legacy 表；
        // 输入 Run 可以是 parent 或三类 Shadow 之一，返回 Option 表示该 Run 是否登记在任一 session。
        // 两个 SELECT 都用 OR 精确匹配 run_id，但 LIMIT 1 没有 ORDER BY；若坏数据让一个 Run 匹配多行，
        // 此 helper 不检测歧义，可能返回其中一行。这里只记录当前读取边界，不改变查询语义。
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
            // paired 行已经包含 cohort、交易日和 regime，交给统一转换器校验。
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
        // legacy 行没有 cohort 字段，按 campaign+level 恢复并由 read_session 兼容旧 level 名称。
        read_session(connection, &campaign_id, level)
    }

    // 校验 campaign 引用的 Contract、Topology、RuntimeManifest、PaperApproval 及其 payload/血缘闭包。
    // 这是创建/恢复 Canary 前的输入校验，不会写入 Artifact、激活 candidate 或改变 Paper 权限。
    fn validate_campaign_artifacts(&self, spec: &CanaryCampaignSpec) -> StoreResult<()> {
        // 输入只读的 CanaryCampaignSpec；依次验证四个 Artifact 引用、候选 Contract/Topology、
        // cohort 的候选身份与 bias certificate/trial ledger，最后校验 RuntimeManifest 和 Paper approval。
        // 整个函数跨多个 Store 查询但不写行、不持共同 SQL 事务；任何一项失败都返回 Err 并阻止 campaign staging。
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
            // 先确认引用的 Artifact 身份、kind 和生命周期，防止用 RunScoped/错误类型伪装 canonical 输入。
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
        // Contract 和 WorkflowGraph 的 payload 也必须通过领域校验，不能只相信 Artifact 元数据。
        let candidate_contract: AgentContract =
            serde_json::from_slice(&self.read_blob(&candidate_contract_artifact.blob)?)?;
        candidate_contract.validate()?;
        let candidate_topology_artifact = self.artifact(&spec.candidate_topology.artifact_id)?;
        let candidate_topology: WorkflowGraph =
            serde_json::from_slice(&self.read_blob(&candidate_topology_artifact.blob)?)?;
        candidate_topology.validate()?;
    // 每个 cohort 必须指向本 campaign 同一组候选身份；iter().any 闭包只借用 cohort，
    // 发现第一处不匹配即短路，避免将部分一致的 cohort manifest 接受为完整配置。
    if spec.cohorts.iter().any(|cohort| {
            cohort.candidate_contract_hash != candidate_contract.contract_hash
                || cohort.candidate_topology_id.0 != candidate_topology.topology_id
        }) {
            return Err(StoreError::CanaryCampaignConflict(
                "campaign candidate cohort identity".to_owned(),
        ));
    }
    // 候选 contract hash/topology id 形成 cohort 的固定候选身份，供后续 certificate 绑定。
    let expected_candidate_hash = content_hash_json(&serde_json::json!({
        "candidate_contract_hash": candidate_contract.contract_hash,
        "candidate_topology_id": candidate_topology.topology_id,
    }))?;
    // BTreeSet 去重 cohort 引用，最后要求整组最多对应同一张不可变证书。
    let mut cohort_certificates = BTreeSet::new();
        for cohort in &spec.cohorts {
            let Some(reference) = &cohort.search_bias_certificate else {
                continue;
            };
            // 每个 cohort 的 bias certificate 必须 canonical、可晋级，并完整覆盖 trial ledger。
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
            // 若 spec 配置接受策略则按策略判定，否则使用证书自身 ready 标志；两者都不能替代来源闭包比对。
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
        // 选中的 ExperimentTrial 必须来自同一 immutable trial refs、holdout 和候选身份。
        let selected_trial: ExperimentTrial =
            serde_json::from_slice(&self.read_blob(&selected_artifact.blob)?)?;
        selected_trial.validate()?;
        // 查询该 subject 的完整 trial ledger 并排序，再与证书冻结列表逐项比较；
        // 这能发现选中 trial 之外后来新增或遗漏的实验记录。
        let mut complete_trial_refs = self
            .experiment_trial_ledger(&selected_trial.subject)?
            .into_iter()
            .map(|(reference, _)| reference)
            .collect::<Vec<_>>();
        complete_trial_refs.sort();
        // trial 的 subject 必须是候选 Contract 或 Topology；Memory subject 不属于此 campaign candidate。
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

        // 最后将 Run 来源 revision/notional 与 canonical manifest、Canary scope approval 逐项绑定。
        let manifest_artifact = self.artifact(&spec.runtime_manifest.artifact_id)?;
        // RuntimeManifest 的 source revision/notional 必须与 campaign spec 一致。
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
        // Paper approval 只能是 Canary scope，并且绑定同一 RuntimeManifest hash；通过不等于已执行。
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
