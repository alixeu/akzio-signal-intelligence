// 文件导读：Execution 写入先把 Accepted plan/context/verdict 绑定到唯一 session slot，
// 再以 deterministic commitment/reprice/cancel intent 记录 Paper 侧副作用边界；accepted 不等于 fill。
// 阅读顺序建议从 commit_execution 看 lease+Attempt+slot 的共同事务，再看 order-action intent，
// 最后看 effect intent 与 reconciliation 提交；实际 Broker HTTP I/O 由调用方在 durable intent 后执行。
use super::*;

#[derive(Debug, Clone)]
pub struct PaperOrderActionCommitResult {
    pub artifact: Artifact,
    pub recovered: bool,
}

impl Store {
    // 在 daemon lease+Task permit 下验证 plan/context/verdict/approval，再原子写唯一 commitment slot。
    // 外层先解码候选 commitment，事务内重验两种 fencing token、Paper purpose、完整 lineage 和 approval。
    // 同一 session 的相同 Artifact/payload 可以恢复成功 Attempt；不同计划冲突。返回 newly_committed
    // 只表示这次写入是否首次落库，不表示券商接受或成交。
    pub fn commit_execution(
        &self,
        lease: &DaemonLease,
        commit: &ExecutionCommit,
    ) -> StoreResult<ExecutionCommitResult> {
        if commit.session_key.trim().is_empty()
            || commit.commitment.kind != ArtifactKind::ExecutionCommitment
        {
            return Err(StoreError::InvalidSessionSlot(commit.session_key.clone()));
        }
        commit.commitment.validate()?;
        let payload: PaperCommitment =
            serde_json::from_slice(&self.read_blob(&commit.commitment.blob)?)?;
        payload.validate()?;
        if payload.broker_session != commit.session_key
            || !commit
                .commitment
                .source_refs
                .iter()
                .any(|source| source == &payload.execution_context)
        {
            return Err(StoreError::InvalidSessionSlot(commit.session_key.clone()));
        }

        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, commit.committed_at)?;
        assert_permit(&transaction, &commit.permit)?;
        assert_paper_run(&transaction, &commit.permit.run_id)?;
        assert_origin_matches(commit.commitment.origin.as_ref(), &commit.permit)?;
        let plan = self.validate_execution_commitment_lineage(
            &transaction,
            &commit.commitment,
            &payload,
            &commit.permit.run_id,
            &commit.session_key,
        )?;
        self.validate_consumed_paper_approval(
            &transaction,
            &commit.session_key,
            &plan,
            commit.committed_at,
        )?;
        let (_, on_failure) = task_retry_policy(&transaction, &commit.permit.task_id)?;
        let slot = transaction
            .query_row(
                "SELECT run_id, commitment_artifact_id FROM rebuild_session_slots WHERE session_key = ?1",
                params![commit.session_key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()?;
        let Some((run_id, existing_commitment)) = slot else {
            return Err(StoreError::InvalidSessionSlot(commit.session_key.clone()));
        };
        if run_id != commit.permit.run_id.0 {
            return Err(StoreError::InvalidSessionSlot(commit.session_key.clone()));
        }
        // 已占用 slot 进入幂等恢复分支：先尝试同 Artifact ID，再允许 payload identity 相同的旧 Artifact；
        // 两者都不匹配时不修改现存 slot，而是报重复 commitment 冲突。
        if let Some(existing_commitment) = existing_commitment {
            if existing_commitment == commit.commitment.artifact_id.0.as_str() {
                let existing_artifact = read_artifact(
                    &transaction,
                    &ArtifactId(ContentHash::new(existing_commitment)?),
                )?;
                let existing_payload: PaperCommitment = serde_json::from_slice(
                    &blob::read_blob_with(&transaction, &existing_artifact.blob)?,
                )?;
                self.validate_execution_commitment_lineage(
                    &transaction,
                    &existing_artifact,
                    &existing_payload,
                    &commit.permit.run_id,
                    &commit.session_key,
                )?;
                let event_id = append_event(
                    &transaction,
                    &commit.permit.run_id,
                    Some(&commit.permit.task_id),
                    Some(&commit.permit.attempt_id),
                    LifecycleEventType::ArtifactCommitted,
                    Some(&commit.commitment.artifact_id),
                    commit.committed_at,
                )?;
                record_attempt_output(
                    &transaction,
                    &commit.permit,
                    &commit.commitment.artifact_id,
                    event_id,
                )?;
                append_event(
                    &transaction,
                    &commit.permit.run_id,
                    Some(&commit.permit.task_id),
                    Some(&commit.permit.attempt_id),
                    LifecycleEventType::ExecutionCommitmentRecovered,
                    Some(&commit.commitment.artifact_id),
                    commit.committed_at,
                )?;
                finish_permitted_task(
                    &transaction,
                    &commit.permit,
                    TaskStatus::Succeeded,
                    on_failure,
                    Some(&commit.commitment.artifact_id),
                    commit.committed_at,
                )?;
                transaction.commit()?;
                return Ok(ExecutionCommitResult {
                    commitment_artifact_id: commit.commitment.artifact_id.clone(),
                    newly_committed: false,
                });
            }
            // 不同 Artifact ID 的 payload 只有计划/context/session/client IDs 全相同才可复用旧提交。
            let existing_artifact_id = ArtifactId(ContentHash::new(existing_commitment)?);
            let existing_artifact = read_artifact(&transaction, &existing_artifact_id)?;
            if existing_artifact.kind == ArtifactKind::ExecutionCommitment {
                let existing_payload: PaperCommitment = serde_json::from_slice(
                    &blob::read_blob_with(&transaction, &existing_artifact.blob)?,
                )?;
                self.validate_execution_commitment_lineage(
                    &transaction,
                    &existing_artifact,
                    &existing_payload,
                    &commit.permit.run_id,
                    &commit.session_key,
                )?;
                if same_paper_commitment(&existing_payload, &payload) {
                    let event_id = append_event(
                        &transaction,
                        &commit.permit.run_id,
                        Some(&commit.permit.task_id),
                        Some(&commit.permit.attempt_id),
                        LifecycleEventType::ArtifactCommitted,
                        Some(&existing_artifact_id),
                        commit.committed_at,
                    )?;
                    record_attempt_output(
                        &transaction,
                        &commit.permit,
                        &existing_artifact_id,
                        event_id,
                    )?;
                    append_event(
                        &transaction,
                        &commit.permit.run_id,
                        Some(&commit.permit.task_id),
                        Some(&commit.permit.attempt_id),
                        LifecycleEventType::ExecutionCommitmentRecovered,
                        Some(&existing_artifact_id),
                        commit.committed_at,
                    )?;
                    finish_permitted_task(
                        &transaction,
                        &commit.permit,
                        TaskStatus::Succeeded,
                        on_failure,
                        Some(&existing_artifact_id),
                        commit.committed_at,
                    )?;
                    transaction.commit()?;
                    return Ok(ExecutionCommitResult {
                        commitment_artifact_id: existing_artifact_id,
                        newly_committed: false,
                    });
                }
            }
            return Err(StoreError::DuplicateExecutionCommitment(
                commit.session_key.clone(),
            ));
        }
        // 首次占 slot 后，Artifact、output index、ExecutionCommitted 事件和 Task 完成共享该事务提交点。
        insert_artifact(&transaction, &commit.commitment)?;
        transaction.execute(
            "UPDATE rebuild_session_slots SET commitment_artifact_id = ?1, committed_at = ?2 WHERE session_key = ?3 AND commitment_artifact_id IS NULL",
            params![
                commit.commitment.artifact_id.0.as_str(),
                commit.committed_at.to_rfc3339(),
                commit.session_key,
            ],
        )?;
        let event_id = append_event(
            &transaction,
            &commit.permit.run_id,
            Some(&commit.permit.task_id),
            Some(&commit.permit.attempt_id),
            LifecycleEventType::ArtifactCommitted,
            Some(&commit.commitment.artifact_id),
            commit.committed_at,
        )?;
        record_attempt_output(
            &transaction,
            &commit.permit,
            &commit.commitment.artifact_id,
            event_id,
        )?;
        append_event(
            &transaction,
            &commit.permit.run_id,
            Some(&commit.permit.task_id),
            Some(&commit.permit.attempt_id),
            LifecycleEventType::ExecutionCommitted,
            Some(&commit.commitment.artifact_id),
            commit.committed_at,
        )?;
        finish_permitted_task(
            &transaction,
            &commit.permit,
            TaskStatus::Succeeded,
            on_failure,
            Some(&commit.commitment.artifact_id),
            commit.committed_at,
        )?;
        transaction.commit()?;
        Ok(ExecutionCommitResult {
            commitment_artifact_id: commit.commitment.artifact_id.clone(),
            newly_committed: true,
        })
    }

    /// Return the one durable r0 -> r1 intent for an order in a committed
    /// Paper session. The table is only an immutable-history index; callers
    /// still consume the returned artifact and its provenance.
    // 通过 immutable index 查找指定 commitment/asset 的唯一 reprice intent。
    // commitment kind 不匹配立即拒绝；SQL 按 commitment ID+资产 symbol 精确查一行，缺行 None。
    pub fn reprice_for(
        &self,
        commitment: &ArtifactRef,
        asset: Asset,
    ) -> StoreResult<Option<Artifact>> {
        if commitment.kind != ArtifactKind::ExecutionCommitment {
            return Err(StoreError::InvalidExecutionReprice);
        }
        let connection = self.connection()?;
        let artifact_id = connection
            .query_row(
                "SELECT reprice_artifact_id FROM rebuild_execution_reprices \
                 WHERE commitment_artifact_id = ?1 AND asset = ?2",
                params![commitment.artifact_id.0.as_str(), asset.symbol()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        artifact_id
            .map(ContentHash::new)
            .transpose()?
            .map(ArtifactId)
            .map(|artifact_id| read_artifact(&connection, &artifact_id))
            .transpose()
    }

    // 通过 immutable index 查找指定 commitment/asset 的唯一 cancel intent。
    // 返回的是持久化 Artifact 投影，不从 receipt 推断 cancel 已被 Broker 执行。
    pub fn cancel_for(
        &self,
        commitment: &ArtifactRef,
        asset: Asset,
    ) -> StoreResult<Option<Artifact>> {
        if commitment.kind != ArtifactKind::ExecutionCommitment {
            return Err(StoreError::InvalidExecutionCancel);
        }
        let connection = self.connection()?;
        let artifact_id = connection
            .query_row(
                "SELECT cancel_artifact_id FROM rebuild_execution_cancels \
                 WHERE commitment_artifact_id = ?1 AND asset = ?2",
                params![commitment.artifact_id.0.as_str(), asset.symbol()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        artifact_id
            .map(ContentHash::new)
            .transpose()?
            .map(ArtifactId)
            .map(|artifact_id| read_artifact(&connection, &artifact_id))
            .transpose()
    }

    // 解析并验证 Reprice payload，复用通用 order-action intent 持久化/恢复路径。
    // typed payload 在事务外解码；固定传入 reprice 表/列和对应 lifecycle event，不接受外部表名。
    pub fn commit_execution_reprice_intent(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        artifact: &Artifact,
        now: DateTime<Utc>,
    ) -> StoreResult<PaperOrderActionCommitResult> {
        if artifact.kind != ArtifactKind::ExecutionReprice {
            return Err(StoreError::InvalidExecutionReprice);
        }
        let payload: PaperReprice = serde_json::from_slice(&self.read_blob(&artifact.blob)?)?;
        payload.validate()?;
        self.commit_order_action_intent(
            lease,
            permit,
            artifact,
            &payload.commitment,
            &payload.prior_receipt,
            payload.asset,
            &payload.prior_client_order_id,
            &payload.prior_broker_order_id,
            "rebuild_execution_reprices",
            "reprice_artifact_id",
            LifecycleEventType::ExecutionRepriceCommitted,
            LifecycleEventType::ExecutionRepriceRecovered,
            now,
        )
    }

    // 解析并验证 Cancel payload，复用同一 commitment/receipt lineage 和幂等索引。
    // typed payload 在事务外解码；固定传入 cancel 表/列和事件种类，再由共同 helper 原子落库。
    pub fn commit_execution_cancel_intent(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        artifact: &Artifact,
        now: DateTime<Utc>,
    ) -> StoreResult<PaperOrderActionCommitResult> {
        if artifact.kind != ArtifactKind::ExecutionCancel {
            return Err(StoreError::InvalidExecutionCancel);
        }
        let payload: PaperCancel = serde_json::from_slice(&self.read_blob(&artifact.blob)?)?;
        payload.validate()?;
        self.commit_order_action_intent(
            lease,
            permit,
            artifact,
            &payload.commitment,
            &payload.prior_receipt,
            payload.asset,
            &payload.client_order_id,
            &payload.broker_order_id,
            "rebuild_execution_cancels",
            "cancel_artifact_id",
            LifecycleEventType::ExecutionCancelCommitted,
            LifecycleEventType::ExecutionCancelRecovered,
            now,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_order_action_intent(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        artifact: &Artifact,
        commitment: &ArtifactRef,
        prior_receipt: &ArtifactRef,
        asset: Asset,
        client_order_id: &str,
        broker_order_id: &str,
        table: &'static str,
        artifact_column: &'static str,
        committed_event: LifecycleEventType,
        recovered_event: LifecycleEventType,
        now: DateTime<Utc>,
    ) -> StoreResult<PaperOrderActionCommitResult> {
        // table/column/event 仅由上方两个固定 wrapper 提供；先验证不可变输入，再在 Immediate 事务重验
        // daemon lease、Task permit、Paper Run 和 prior receipt lineage。
        artifact.validate()?;
        if artifact.lifecycle != ArtifactLifecycle::Canonical
            || !artifact.source_refs.contains(commitment)
            || !artifact.source_refs.contains(prior_receipt)
        {
            return Err(StoreError::InvalidPaperEffect(artifact.artifact_id.clone()));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;
        assert_permit(&transaction, permit)?;
        assert_paper_run(&transaction, &permit.run_id)?;
        self.validate_order_action_lineage(
            &transaction,
            commitment,
            prior_receipt,
            asset,
            client_order_id,
            broker_order_id,
            &permit.run_id,
        )?;

        let query = format!(
            "SELECT {artifact_column} FROM {table} \
             WHERE commitment_artifact_id = ?1 AND asset = ?2"
        );
        if let Some(existing_id) = transaction
            .query_row(
                &query,
                params![commitment.artifact_id.0.as_str(), asset.symbol()],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        {
            // commitment+asset 索引已有一条 intent 时读取原 Artifact 并记 recovered event；
            // 当前输入只用于确认 receipt lineage，不比较/覆盖已有 payload。
            let existing_id = ArtifactId(ContentHash::new(existing_id)?);
            let existing = read_artifact(&transaction, &existing_id)?;
            append_event(
                &transaction,
                &permit.run_id,
                Some(&permit.task_id),
                Some(&permit.attempt_id),
                recovered_event,
                Some(&existing.artifact_id),
                now,
            )?;
            transaction.commit()?;
            return Ok(PaperOrderActionCommitResult {
                artifact: existing,
                recovered: true,
            });
        }

        assert_origin_matches(artifact.origin.as_ref(), permit)?;
        insert_artifact(&transaction, artifact)?;
        let insert = format!(
            "INSERT INTO {table} \
             (commitment_artifact_id, asset, {artifact_column}, created_at) \
             VALUES (?1, ?2, ?3, ?4)"
        );
        transaction.execute(
            &insert,
            params![
                commitment.artifact_id.0.as_str(),
                asset.symbol(),
                artifact.artifact_id.0.as_str(),
                now.to_rfc3339(),
            ],
        )?;
        append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            committed_event,
            Some(&artifact.artifact_id),
            now,
        )?;
        transaction.commit()?;
        // Ok(recovered=false) 只确认 SQL intent 已提交；外部 adapter 仍需实际请求和后续回执。
        Ok(PaperOrderActionCommitResult {
            artifact: artifact.clone(),
            recovered: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn validate_order_action_lineage(
        &self,
        connection: &Connection,
        commitment: &ArtifactRef,
        prior_receipt: &ArtifactRef,
        asset: Asset,
        client_order_id: &str,
        broker_order_id: &str,
        run_id: &RunId,
    ) -> StoreResult<()> {
        // commitment/receipt Ref kind 与同一 Run 来源先核对，再解码两种 CAS payload；
        // receipt 的 plan hash、资产和 client/broker ID 必须逐字段与调用参数匹配。
        if commitment.kind != ArtifactKind::ExecutionCommitment
            || prior_receipt.kind != ArtifactKind::OrderReceipt
        {
            return Err(StoreError::InvalidPaperEffect(
                prior_receipt.artifact_id.clone(),
            ));
        }
        let commitment_artifact = read_artifact(connection, &commitment.artifact_id)?;
        let receipt_artifact = read_artifact(connection, &prior_receipt.artifact_id)?;
        if commitment_artifact.kind != ArtifactKind::ExecutionCommitment
            || receipt_artifact.kind != ArtifactKind::OrderReceipt
            || commitment_artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run_id)
            || !receipt_artifact.source_refs.contains(commitment)
        {
            return Err(StoreError::InvalidPaperEffect(
                prior_receipt.artifact_id.clone(),
            ));
        }
        let commitment_payload: PaperCommitment = serde_json::from_slice(&blob::read_blob_with(
            connection,
            &commitment_artifact.blob,
        )?)?;
        let receipt: OrderReceipt =
            serde_json::from_slice(&blob::read_blob_with(connection, &receipt_artifact.blob)?)?;
        commitment_payload.validate()?;
        receipt.validate()?;
        if receipt.plan_hash != commitment_payload.plan_hash
            || receipt.asset != asset
            || receipt.client_order_id != client_order_id
            || receipt.broker_order_id != broker_order_id
            || !commitment_payload.client_order_ids.contains_key(&asset)
        {
            return Err(StoreError::InvalidPaperEffect(
                prior_receipt.artifact_id.clone(),
            ));
        }
        Ok(())
    }

    /// 在 Broker I/O 前原子记录 Rust-owned Paper effect intent。
    /// 它不终结 Task；后续对账路径才收束 Attempt。
    // 在外部 broker I/O 前写唯一 durable intent；重复 intent 返回 true 而不重复追加。
    // Artifact 先做只读预检，IMMEDIATE 事务内重验 lease/permit/Paper/Debug policy 和历史事件。
    // true 表示相同 intent 已存在，false 表示新 intent event 已提交；二者都不报告 Broker 返回结果。
    // 注意：当前实现只写 ExecutionEffectIntent event，不调用 finish_permitted_task；
    // Attempt/Task 的最终收束由后续 fenced reconciliation 提交路径完成。
    pub fn record_paper_effect_intent(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        effect: &ArtifactRef,
        now: DateTime<Utc>,
    ) -> StoreResult<bool> {
        self.validate_paper_effect_artifact(effect, &permit.run_id)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;
        assert_permit(&transaction, permit)?;
        assert_paper_run(&transaction, &permit.run_id)?;
        debug::assert_broker_write(&transaction, &permit.run_id)?;
        assert_paper_effect_artifact(&transaction, effect, &permit.run_id)?;
        validate_paper_effect_events(&transaction, Some(&permit.run_id))?;
        if paper_effect_terminal_exists(&transaction, &permit.run_id, &effect.artifact_id)? {
            return Err(StoreError::PaperEffectAlreadySettled(
                effect.artifact_id.clone(),
            ));
        }
        let already_recorded =
            paper_effect_intent_exists(&transaction, &permit.run_id, &effect.artifact_id)?;
        if already_recorded {
            transaction.commit()?;
            return Ok(true);
        }
        // Intent 与外部 HTTP 副作用分开提交；进程在提交后中断时，恢复逻辑能看到未终结的 intent。
        append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::ExecutionEffectIntent,
            Some(&effect.artifact_id),
            now,
        )?;
        transaction.commit()?;
        Ok(false)
    }

    // 只有已有 intent 才能追加 settled/recovered terminal；重复 terminal 视为幂等成功。
    // 本方法只写本地 terminal event；它不发 Broker 请求，也不验证实际成交数量。
    pub fn settle_paper_effect(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        effect: &ArtifactRef,
        recovered: bool,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        self.validate_paper_effect_artifact(effect, &permit.run_id)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;
        assert_permit(&transaction, permit)?;
        assert_paper_run(&transaction, &permit.run_id)?;
        assert_paper_effect_artifact(&transaction, effect, &permit.run_id)?;
        validate_paper_effect_events(&transaction, Some(&permit.run_id))?;
        if paper_effect_terminal_exists(&transaction, &permit.run_id, &effect.artifact_id)? {
            // 已存在 terminal 时只确认幂等结束，不追加第二个 settlement。
            transaction.commit()?;
            return Ok(());
        }
        if !paper_effect_intent_exists(&transaction, &permit.run_id, &effect.artifact_id)? {
            return Err(StoreError::MissingPaperEffectIntent(
                effect.artifact_id.clone(),
            ));
        }
        append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            if recovered {
                LifecycleEventType::ExecutionEffectRecovered
            } else {
                LifecycleEventType::ExecutionEffectSettled
            },
            Some(&effect.artifact_id),
            now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Commit Paper reconciliation artifacts and the effect settlement marker
    /// under the same daemon lease/attempt fence and SQLite transaction.
    // 将 Reconcile Artifact 和 effect terminal 放入同一 fenced Attempt 事务，避免半提交。
    // 仅允许成功 Attempt；事务内共写 Reconciliation outputs、Task/Attempt 完成与 effect terminal。
    // 这提供 SQLite 内原子性，不把之前/之后的 Broker I/O 纳入事务。
    pub fn commit_fenced_attempt_with_effect(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        artifacts: &[Artifact],
        effect: &ArtifactRef,
        recovered: bool,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        self.validate_attempt_commit(permit, artifacts, TaskStatus::Succeeded)?;
        self.validate_paper_effect_artifact(effect, &permit.run_id)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;
        assert_permit(&transaction, permit)?;
        assert_paper_run(&transaction, &permit.run_id)?;
        assert_paper_effect_artifact(&transaction, effect, &permit.run_id)?;
        validate_paper_effect_events(&transaction, Some(&permit.run_id))?;
        if paper_effect_terminal_exists(&transaction, &permit.run_id, &effect.artifact_id)? {
            return Err(StoreError::PaperEffectAlreadySettled(
                effect.artifact_id.clone(),
            ));
        }
        if !paper_effect_intent_exists(&transaction, &permit.run_id, &effect.artifact_id)? {
            return Err(StoreError::MissingPaperEffectIntent(
                effect.artifact_id.clone(),
            ));
        }
        commit_attempt_transaction_with_effect(
            &transaction,
            permit,
            artifacts,
            TaskStatus::Succeeded,
            Some((
                effect,
                if recovered {
                    LifecycleEventType::ExecutionEffectRecovered
                } else {
                    LifecycleEventType::ExecutionEffectSettled
                },
            )),
            now,
        )?;
        transaction.commit()?;
        Ok(())
    }
}
