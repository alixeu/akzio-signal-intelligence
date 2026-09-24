// 文件导读：daemon lease 用 owner+递增 epoch+expiry fencing scheduler；Paper session slot
// 由同一 lease 保护并按 session_key 幂等，lease 成功只代表 Store reservation，不代表 broker fill。
// 建议先读 acquire/heartbeat/release 理解调度器所有权，再读 reserve_session_slot 和
// session_slot_for_run 理解数据库提交与恢复；Broker I/O 不在本文件执行。
use super::*;

impl Store {
    /// 原子发布 Paper workflow、proposal、run-scoped inputs 和 session slot；
    /// 此便捷入口不传 approval binding，不能据此推断该 session 已获交易审批。
    // 这是不携带 RuntimeManifest/Approval binding 的便捷入口；其余 Paper graph、proposal、
    // lease 与 slot 仍由被委派方法校验并在同一写事务中持久化。
    pub fn reserve_paper_session_with_proposal(
        &self,
        lease: &DaemonLease,
        reservation: &SessionReservation,
        proposal: &Artifact,
    ) -> StoreResult<SessionSlotReservation> {
        self.reserve_paper_session_with_binding(lease, reservation, proposal, None)
    }

    // 在 reservation 前同时验证 approval、manifest session date/expiry/notional，再绑定二者。
    // 先解码并校验 manifest/approval 的来源/hash、session 日期授权和 expiry；buy notional 留到
    // ExecutionPlan/Commitment 校验阶段。成功后把 binding 交给事务化写入入口，返回 slot reservation，
    // 不表示 workflow 已开始、Paper commitment 已写入或 Broker 已接受订单。
    pub fn reserve_paper_session_with_approval(
        &self,
        lease: &DaemonLease,
        reservation: &SessionReservation,
        proposal: &Artifact,
        runtime_manifest: &Artifact,
        approval: &Artifact,
    ) -> StoreResult<SessionSlotReservation> {
        let (manifest_payload, approval_payload) = self
            .validate_paper_approval_binding(runtime_manifest, approval)
            .map_err(|_| StoreError::InvalidSessionSlot(reservation.session_key.clone()))?;
        let session = chrono::NaiveDate::parse_from_str(&reservation.session_key, "%Y-%m-%d")
            .map_err(|_| StoreError::InvalidSessionSlot(reservation.session_key.clone()))?;
        if approval_payload.runtime_manifest.artifact_id != runtime_manifest.artifact_id
            || approval_payload.runtime_manifest_hash != manifest_payload.manifest_hash()?
            || !manifest_payload.authorizes_new_session(session, reservation.reserved_at)
            || approval_payload.expires_at < reservation.reserved_at
        {
            return Err(StoreError::InvalidSessionSlot(
                reservation.session_key.clone(),
            ));
        }
        self.reserve_paper_session_with_binding(
            lease,
            reservation,
            proposal,
            Some((runtime_manifest, approval)),
        )
    }

    /// Atomically elect one daemon scheduler. A successor always receives a
    /// higher epoch so stale leaders cannot mutate a Paper session slot.
    // 输入 lease 名称、owner 和 now/expiry；IMMEDIATE 事务串行检查同名行。
    // 无记录时 epoch 从 1 开始；未过期记录使调用返回 None；过期记录由新 owner 接管并递增 epoch。
    // 返回 Some 只表示持久化 lease 取得成功，不代表之后任何 session/订单副作用成功。
    pub fn acquire_daemon_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
        now: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> StoreResult<Option<DaemonLease>> {
        if lease_name.trim().is_empty() || owner_id.trim().is_empty() || expires_at <= now {
            return Err(StoreError::InvalidDaemonLease(lease_name.to_owned()));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = transaction
            .query_row(
                "SELECT owner_id, epoch, expires_at FROM rebuild_daemon_leases WHERE lease_name = ?1",
                params![lease_name],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?, row.get::<_, String>(2)?)),
            )
            .optional()?;
        let lease = match current {
            None => {
                transaction.execute(
                    "INSERT INTO rebuild_daemon_leases (lease_name, owner_id, epoch, expires_at, heartbeat_at) VALUES (?1, ?2, 1, ?3, ?4)",
                    params![lease_name, owner_id, expires_at.to_rfc3339(), now.to_rfc3339()],
                )?;
                DaemonLease {
                    lease_name: lease_name.to_owned(),
                    owner_id: owner_id.to_owned(),
                    epoch: 1,
                    expires_at,
                }
            }
            Some((_, _, current_expires_at)) if parse_time(&current_expires_at)? > now => {
                transaction.commit()?;
                return Ok(None);
            }
            Some((_, epoch, _)) => {
                let epoch = epoch.saturating_add(1);
                transaction.execute(
                    "UPDATE rebuild_daemon_leases SET owner_id = ?1, epoch = ?2, expires_at = ?3, heartbeat_at = ?4 WHERE lease_name = ?5",
                    params![owner_id, epoch, expires_at.to_rfc3339(), now.to_rfc3339(), lease_name],
                )?;
                DaemonLease {
                    lease_name: lease_name.to_owned(),
                    owner_id: owner_id.to_owned(),
                    epoch,
                    expires_at,
                }
            }
        };
        transaction.commit()?;
        Ok(Some(lease))
    }

    /// Release only this epoch. Retaining the row preserves fencing monotonicity.
    // 单条条件 UPDATE 要求 name/owner/epoch 匹配且仍未过期；影响 1 行才返回 true。
    // SQLite 对单条语句提供其语句级原子性，但本方法没有多语句业务事务，也不删除历史 epoch 行。
    pub fn release_daemon_lease(
        &self,
        lease: &DaemonLease,
        now: DateTime<Utc>,
    ) -> StoreResult<bool> {
        let connection = self.connection()?;
        Ok(connection.execute(
            "UPDATE rebuild_daemon_leases SET expires_at=?1, heartbeat_at=?1 WHERE lease_name=?2 AND owner_id=?3 AND epoch=?4 AND expires_at > ?1",
            params![now.to_rfc3339(), lease.lease_name, lease.owner_id, lease.epoch],
        )? == 1)
    }

    // 只延长相同 owner+epoch 的未过期 lease，且不覆盖 maintenance 已延长的时间。
    // now/expiry 是调用者时间；找不到匹配行或当前 lease 已到期返回 false。
    // expiry 与当前 expiry 取 max，保证 heartbeat 不缩短维护窗口延长值；更新仅在事务提交后生效。
    pub fn heartbeat_daemon_lease(
        &self,
        lease: &DaemonLease,
        now: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> StoreResult<bool> {
        if expires_at <= now {
            return Err(StoreError::InvalidDaemonLease(lease.lease_name.clone()));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current: Option<String> = transaction.query_row(
            "SELECT expires_at FROM rebuild_daemon_leases WHERE lease_name = ?1 AND owner_id = ?2 AND epoch = ?3",
            params![lease.lease_name, lease.owner_id, lease.epoch],
            |row| row.get(0),
        ).optional()?;
        let Some(current) = current else {
            return Ok(false);
        };
        let current = parse_time(&current)?;
        if current <= now {
            return Ok(false);
        }
        // A heartbeat cannot undo a maintenance window's lease extension.
        let expires_at = expires_at.max(current);
        transaction.execute(
            "UPDATE rebuild_daemon_leases SET expires_at = ?1, heartbeat_at = ?2 WHERE lease_name = ?3 AND owner_id = ?4 AND epoch = ?5",
            params![expires_at.to_rfc3339(), now.to_rfc3339(), lease.lease_name, lease.owner_id, lease.epoch],
        )?;
        transaction.commit()?;
        Ok(true)
    }

    // 读取当前 lease row 并解析时间；查询不续租、不抢占 owner。
    // SQL 按 lease_name 过滤，零行是 None；持久化 epoch/时间解析失败不会被当作缺少 lease。
    pub fn daemon_lease(&self, lease_name: &str) -> StoreResult<Option<DaemonLease>> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT owner_id, epoch, expires_at FROM rebuild_daemon_leases WHERE lease_name = ?1",
                params![lease_name],
                |row| {
                    Ok(DaemonLease {
                        lease_name: lease_name.to_owned(),
                        owner_id: row.get(0)?,
                        epoch: row.get(1)?,
                        expires_at: parse_time(&row.get::<_, String>(2)?).map_err(|error| {
                            rusqlite::Error::ToSqlConversionFailure(Box::new(error))
                        })?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    /// Verify that the caller still owns the current, unexpired daemon epoch.
    /// Broker adapters call this immediately before external Paper I/O.
    // 用读事务检查数据库中 owner/epoch/expiry 与传入 lease 一致；检查返回 Ok 后锁即释放，
    // 这本身不把后续 HTTP I/O 与 SQLite 锁组成跨系统原子操作，调用方仍需在写入边界复核。
    pub fn validate_daemon_lease(
        &self,
        lease: &DaemonLease,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        assert_daemon_lease(&transaction, lease, now)?;
        transaction.commit()?;
        Ok(())
    }

    /// Freeze the exact Paper graph before its Run is installed. A duplicate
    /// session returns the original graph and task IDs without recording the
    /// caller's replacement proposal.
    // 输入 session_key、冻结 workflow/setup Artifact 和 reservation 时间；事务前校验图与输入闭包，
    // 事务中检查 lease。已存在的 session_key 不覆盖或记录新 proposal，最后重读 slot 作为结果。
    // `newly_reserved=false` 表示发现旧 slot，不表示新入参与旧 reservation 相同。
    pub fn reserve_session_slot(
        &self,
        lease: &DaemonLease,
        reservation: &SessionReservation,
    ) -> StoreResult<SessionSlotReservation> {
        if reservation.session_key.trim().is_empty()
            || reservation.workflow.run.purpose != RunPurpose::Paper
            || reservation.workflow.graph.kind != ArtifactKind::WorkflowGraph
            || reservation.workflow.graph.artifact_id != reservation.workflow.run.graph_artifact_id
        {
            return Err(StoreError::InvalidSessionSlot(
                reservation.session_key.clone(),
            ));
        }
        reservation.workflow.graph.validate()?;
        let graph: WorkflowGraph =
            serde_json::from_slice(&self.read_blob(&reservation.workflow.graph.blob)?)?;
        graph.validate()?;
        if graph.nodes != reservation.workflow.nodes
            || graph.topology_id != reservation.workflow.run.topology_id
        {
            return Err(StoreError::WorkflowGraphMismatch);
        }
        for artifact in &reservation.setup_artifacts {
            artifact.validate()?;
            if artifact.kind != ArtifactKind::EvidenceNeed
                || artifact.lifecycle != ArtifactLifecycle::RunScoped
                || artifact
                    .origin
                    .as_ref()
                    .and_then(|origin| origin.run_id.as_ref())
                    != Some(&reservation.workflow.run.run_id)
            {
                return Err(StoreError::InvalidSessionSlot(
                    reservation.session_key.clone(),
                ));
            }
            self.read_blob(&artifact.blob)?;
        }

        let newly_reserved = {
            let mut connection = self.connection()?;
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            assert_daemon_lease(&transaction, lease, reservation.reserved_at)?;
            let exists = transaction
                .query_row(
                    "SELECT 1 FROM rebuild_session_slots WHERE session_key = ?1",
                    params![reservation.session_key],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?;
            if exists.is_some() {
                transaction.commit()?;
                false
            } else {
                for artifact in &reservation.setup_artifacts {
                    insert_artifact(&transaction, artifact)?;
                }
                Self::commit_workflow_transaction(&transaction, &reservation.workflow)?;
                Self::append_session_setup_events(&transaction, reservation, None)?;
                assert_session_slot_run(
                    &transaction,
                    &reservation.session_key,
                    &reservation.workflow.run.run_id,
                )?;
                transaction.execute(
                    "INSERT INTO rebuild_session_slots (session_key, run_id, topology_id, graph_artifact_id, run_created_at, scheduler_epoch, reserved_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        reservation.session_key,
                        reservation.workflow.run.run_id.0,
                        reservation.workflow.run.topology_id,
                        reservation.workflow.run.graph_artifact_id.0.as_str(),
                        reservation.workflow.run.created_at.to_rfc3339(),
                        lease.epoch,
                        reservation.reserved_at.to_rfc3339(),
                    ],
                )?;
                transaction.commit()?;
                true
            }
        };
        let slot = self
            .session_slot(&reservation.session_key)?
            .ok_or_else(|| StoreError::Integrity("session slot disappeared".to_owned()))?;
        Ok(SessionSlotReservation {
            slot,
            newly_reserved,
        })
    }

    // 由 slot 行读取 graph Artifact/CAS payload，再恢复 Paper WorkflowCommit 快照。
    // 先在一次连接查询 slot 列，随后释放 guard，再分别读取 graph Artifact/BLOB；
    // 这些读取未包在同一个显式只读事务中，任何损坏字段或 graph 校验失败均为 Err。
    pub fn session_slot(&self, session_key: &str) -> StoreResult<Option<SessionSlot>> {
        let row = {
            let connection = self.connection()?;
            connection
                .query_row(
                    "SELECT run_id, topology_id, graph_artifact_id, run_created_at, scheduler_epoch, reserved_at, commitment_artifact_id, committed_at FROM rebuild_session_slots WHERE session_key = ?1",
                    params![session_key],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, u64>(4)?,
                            row.get::<_, String>(5)?,
                            row.get::<_, Option<String>>(6)?,
                            row.get::<_, Option<String>>(7)?,
                        ))
                    },
                )
                .optional()?
        };
        row.map(
            |(
                run_id,
                topology_id,
                graph_artifact_id,
                run_created_at,
                scheduler_epoch,
                reserved_at,
                commitment_artifact_id,
                committed_at,
            )| {
                let graph_artifact_id = ArtifactId(ContentHash::new(graph_artifact_id)?);
                let graph_artifact = self.artifact(&graph_artifact_id)?;
                if graph_artifact.kind != ArtifactKind::WorkflowGraph {
                    return Err(StoreError::InvalidSessionSlot(session_key.to_owned()));
                }
                let graph: WorkflowGraph =
                    serde_json::from_slice(&self.read_blob(&graph_artifact.blob)?)?;
                graph.validate()?;
                if graph.topology_id != topology_id {
                    return Err(StoreError::WorkflowGraphMismatch);
                }
                Ok(SessionSlot {
                    session_key: session_key.to_owned(),
                    workflow: WorkflowCommit {
                        run: StoredRun {
                            run_id: RunId(run_id),
                            purpose: RunPurpose::Paper,
                            topology_id,
                            graph_artifact_id,
                            created_at: parse_time(&run_created_at)?,
                        },
                        graph: graph_artifact,
                        nodes: graph.nodes,
                    },
                    scheduler_epoch,
                    reserved_at: parse_time(&reserved_at)?,
                    commitment_artifact_id: commitment_artifact_id
                        .map(ContentHash::new)
                        .transpose()?
                        .map(ArtifactId),
                    committed_at: committed_at.as_deref().map(parse_time).transpose()?,
                })
            },
        )
        .transpose()
    }

    // 通过 session slot 找到消耗的 approval/manifest，并重新校验两者 hash/source binding。
    // 按 Run 关联 slot→approval-consumption，未消费返回 None；找到后解码两个 CAS payload 并校验 hash。
    // 查询和后续 BLOB 解码之间没有共同事务，方法只返回授权记录，不执行 Paper 操作。
    pub fn paper_approval_for_run(
        &self,
        run_id: &RunId,
    ) -> StoreResult<Option<(RuntimeManifest, PaperLaunchApproval)>> {
        let row = {
            let connection = self.connection()?;
            connection
                .query_row(
                    "SELECT c.runtime_manifest_artifact_id, c.approval_artifact_id FROM rebuild_paper_approval_consumptions c JOIN rebuild_session_slots s ON s.session_key = c.session_key WHERE s.run_id = ?1",
                    params![run_id.0],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()?
        };
        let Some((manifest_id, approval_id)) = row else {
            return Ok(None);
        };
        let manifest_artifact = self.artifact(&ArtifactId(ContentHash::new(manifest_id)?))?;
        let approval_artifact = self.artifact(&ArtifactId(ContentHash::new(approval_id)?))?;
        let manifest: RuntimeManifest =
            serde_json::from_slice(&self.read_blob(&manifest_artifact.blob)?)?;
        let approval: PaperLaunchApproval =
            serde_json::from_slice(&self.read_blob(&approval_artifact.blob)?)?;
        manifest.validate()?;
        approval.validate()?;
        if approval.runtime_manifest.artifact_id != manifest_artifact.artifact_id
            || approval.runtime_manifest_hash != manifest.manifest_hash()?
        {
            return Err(StoreError::InvalidSessionSlot(run_id.0.clone()));
        }
        Ok(Some((manifest, approval)))
    }

    /// Returns the frozen broker-session slot for one scheduler-owned Paper
    /// run. A run may never have more than one such slot.
    // 先通过 run_id 取可选 session_key，再调用 slot 恢复方法；两次读不是同一显式 SQLite 快照。
    // 返回 Some 是已持久化 slot 的投影，不是订单提交或成交凭证。
    pub fn session_slot_for_run(&self, run_id: &RunId) -> StoreResult<Option<SessionSlot>> {
        let session_key = {
            let connection = self.connection()?;
            connection
                .query_row(
                    "SELECT session_key FROM rebuild_session_slots WHERE run_id = ?1",
                    params![run_id.0],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
        };
        Ok(session_key
            .as_deref()
            .map(|session_key| self.session_slot(session_key))
            .transpose()?
            .flatten())
    }
}
