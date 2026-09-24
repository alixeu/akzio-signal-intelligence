// 文件导读：这里串起 Paper session slot、Run graph、ExecutionCommitment lineage 和
// specialized Artifact 校验；调用方传入的 graph/approval 不会绕过 Store 的事务边界。
// Paper reservation 的调用方在 lease.rs/debug.rs/canary 中；先读 reserve_paper_session_with_binding
// 再读 commit_workflow_transaction，最后沿 execution commitment lineage 向上游核验 plan/context/verdict。
impl Store {
    fn reserve_paper_session_with_binding(
        &self,
        lease: &DaemonLease,
        reservation: &SessionReservation,
        proposal: &Artifact,
        binding: Option<(&Artifact, &Artifact)>,
    ) -> StoreResult<SessionSlotReservation> {
        // 输入 lease、冻结 reservation/proposal 和可选 approval binding；图/来源先校验，
        // Immediate 事务中再验证 daemon epoch 并按 session_key 查重，提交后重读 slot 投影。
        // 已有 key 返回 newly_reserved=false，不写新的 proposal 或替换原 graph。
        self.validate_paper_session_reservation(reservation, proposal)?;

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
                Self::insert_session_slot_transaction(
                    &transaction,
                    lease,
                    reservation,
                    proposal,
                    binding,
                )?;
                transaction.commit()?;
                true
            }
        };
        let slot = self
            .session_slot(&reservation.session_key)?
            .ok_or_else(|| StoreError::Integrity("session slot missing after commit".to_owned()))?;
        Ok(SessionSlotReservation {
            slot,
            newly_reserved,
        })
    }

    pub(super) fn commit_session_slot_transaction(
        transaction: &Transaction<'_>,
        lease: &DaemonLease,
        reservation: &SessionReservation,
        proposal: &Artifact,
        binding: Option<(&Artifact, &Artifact)>,
    ) -> StoreResult<()> {
        // 供 Canary 等组合事务复用：只重验 lease 并调用内部写 helper；借用调用方 Transaction，
        // 不获取 mutex、不自行 commit，reservation 和 slot 留待外层一起提交。
        assert_daemon_lease(transaction, lease, reservation.reserved_at)?;
        Self::insert_session_slot_transaction(transaction, lease, reservation, proposal, binding)
    }

    // Callers retain their lease check and duplicate-session handling before writing.
    fn insert_session_slot_transaction(
        transaction: &Transaction<'_>,
        lease: &DaemonLease,
        reservation: &SessionReservation,
        proposal: &Artifact,
        binding: Option<(&Artifact, &Artifact)>,
    ) -> StoreResult<()> {
        // 顺序是 setup Artifact→proposal→可选 manifest/approval→workflow rows→setup events→slot/consumption。
        // 每步复用同一事务，唯一键/外键或任一 `?` 失败时外层不能提交部分 reservation。
        for artifact in &reservation.setup_artifacts {
            insert_artifact(transaction, artifact)?;
        }
        insert_artifact(transaction, proposal)?;
        if let Some((runtime_manifest, approval)) = binding {
            insert_artifact(transaction, runtime_manifest)?;
            insert_artifact(transaction, approval)?;
        }
        Self::commit_workflow_transaction(transaction, &reservation.workflow)?;
        Self::append_session_setup_events(transaction, reservation, Some(proposal))?;
        assert_session_slot_run(
            transaction,
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
        if let Some((runtime_manifest, approval)) = binding {
            transaction.execute(
                "INSERT INTO rebuild_paper_approval_consumptions (approval_artifact_id, runtime_manifest_artifact_id, session_key, consumed_at) VALUES (?1, ?2, ?3, ?4)",
                params![
                    approval.artifact_id.0.as_str(),
                    runtime_manifest.artifact_id.0.as_str(),
                    reservation.session_key,
                    reservation.reserved_at.to_rfc3339(),
                ],
            )?;
        }
        Ok(())
    }

    /// Record run-level facts for the artifacts a session reservation writes
    /// before any task row exists: the scheduler's session-scoped
    /// `EvidenceNeed`s and, where present, the frozen `WorkflowProposal`. They
    /// are bound to the run, so without these events the Doctor cannot observe
    /// them in the run's event log at all.
    pub(super) fn append_session_setup_events(
        transaction: &Transaction<'_>,
        reservation: &SessionReservation,
        proposal: Option<&Artifact>,
    ) -> StoreResult<()> {
        // 把 reservation 阶段尚无 Task/Attempt 的 EvidenceNeed 以及可选 proposal 纳入 Run event log；
        // None proposal 时只写 snapshot-need events，不创建新的 Artifact。
        Self::append_run_setup_events(transaction, &reservation.workflow.run.run_id,
            &reservation.setup_artifacts, reservation.reserved_at)?;
        if let Some(proposal) = proposal {
            append_event(
                transaction,
                &reservation.workflow.run.run_id,
                None,
                None,
                LifecycleEventType::SchedulerWorkflowProposalCreated,
                Some(&proposal.artifact_id),
                reservation.reserved_at,
            )?;
        }
        Ok(())
    }

    pub(super) fn append_run_setup_events(
        transaction: &Transaction<'_>, run_id: &RunId, setup: &[Artifact], now: DateTime<Utc>,
    ) -> StoreResult<()> {
        // 按输入顺序逐个追加无 task/attempt 的 SchedulerSnapshotNeedCreated event；
        // 不插 Artifact 行、不建立 Attempt output index。
        for artifact in setup {
            append_event(transaction, run_id, None, None,
                LifecycleEventType::SchedulerSnapshotNeedCreated, Some(&artifact.artifact_id), now)?;
        }
        Ok(())
    }

    fn commit_workflow_transaction(
        transaction: &Transaction<'_>,
        commit: &WorkflowCommit,
    ) -> StoreResult<()> {
        // 输入 WorkflowCommit 的 graph/nodes 与 input closure 先校验；在调用方事务内按
        // Artifact→Run/control→Task→dependencies→revision→WorkflowCreated 顺序写入。
        // 此 helper 只执行 SQL，不提交；任一 insert/event/checkpoint 失败整组由外层回滚。
        assert_workflow_input_artifacts(transaction, &commit.nodes)?;
        insert_artifact(transaction, &commit.graph)?;
        let inserted = transaction.execute(
            r#"INSERT INTO rebuild_runs
                (run_id, purpose, topology_id, graph_artifact_id, status, created_at)
                VALUES (?1, ?2, ?3, ?4, 'queued', ?5)"#,
            params![
                commit.run.run_id.0,
                enum_name(commit.run.purpose),
                commit.run.topology_id,
                commit.run.graph_artifact_id.0.as_str(),
                commit.run.created_at.to_rfc3339(),
            ],
        )?;
        if inserted != 1 {
            return Err(StoreError::DuplicateRun(commit.run.run_id.clone()));
        }
        run_control::initialize_control(transaction, &commit.run.run_id, commit.run.created_at)?;
        for node in &commit.nodes {
            insert_task_node(transaction, &commit.run.run_id, node, commit.run.created_at)?;
        }
        for node in &commit.nodes {
            insert_node_dependencies(transaction, node)?;
        }
        transaction.execute(
            r#"INSERT INTO rebuild_workflow_revisions
                (run_id, revision, graph_artifact_id, created_at)
                VALUES (?1, 0, ?2, ?3)"#,
            params![
                commit.run.run_id.0,
                commit.run.graph_artifact_id.0.as_str(),
                commit.run.created_at.to_rfc3339(),
            ],
        )?;
        append_event(
            transaction,
            &commit.run.run_id,
            None,
            None,
            LifecycleEventType::WorkflowCreated,
            Some(&commit.graph.artifact_id),
            commit.run.created_at,
        )?;
        Ok(())
    }

    fn validate_execution_commitment_lineage(
        &self,
        connection: &Connection,
        commitment_artifact: &Artifact,
        commitment: &PaperCommitment,
        run_id: &RunId,
        session_key: &str,
    ) -> StoreResult<ExecutionPlan> {
        // 输入已读 commitment Artifact/payload、目标 Run/session；反向沿 commitment→verdict→context→plan
        // 读取 CAS。每层核对 kind、Run origin、typed payload 与精确 source refs，最终返回完整 ExecutionPlan。
        // 只借用调用方 connection，不拿新锁、不写状态；`invalid` 是捕获 session_key 借用的零参数闭包，
        // 每次调用新建同一种错误，不会在校验过程中改写 session_key。
        let invalid = || StoreError::InvalidSessionSlot(session_key.to_owned());
        if commitment_artifact.kind != ArtifactKind::ExecutionCommitment
            || commitment_artifact.lifecycle != ArtifactLifecycle::Canonical
            || commitment.broker_session != session_key
            || commitment_artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run_id)
        {
            return Err(invalid());
        }

        let verdict_refs = commitment_artifact
            .source_refs
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::ExecutionVerdict)
            .cloned()
            .collect::<Vec<_>>();
        let context_refs = commitment_artifact
            .source_refs
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::ExecutionContext)
            .cloned()
            .collect::<Vec<_>>();
        if verdict_refs.len() != 1
            || context_refs.len() != 1
            || context_refs[0] != commitment.execution_context
            || !has_exact_source_refs(
                commitment_artifact,
                &[verdict_refs[0].clone(), context_refs[0].clone()],
            )
        {
            return Err(invalid());
        }

        let context_ref = &context_refs[0];
        let verdict_artifact = read_artifact(connection, &verdict_refs[0].artifact_id)?;
        if verdict_artifact.kind != ArtifactKind::ExecutionVerdict
            || verdict_artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run_id)
            || !has_exact_source_refs(&verdict_artifact, std::slice::from_ref(context_ref))
        {
            return Err(invalid());
        }
        let verdict: ExecutionVerdict =
            serde_json::from_slice(&blob::read_blob_with(connection, &verdict_artifact.blob)?)?;
        let ExecutionVerdict::Accepted { execution_context } = verdict else {
            return Err(invalid());
        };
        if execution_context != *context_ref {
            return Err(invalid());
        }

        let context_artifact = read_artifact(connection, &context_ref.artifact_id)?;
        if context_artifact.kind != ArtifactKind::ExecutionContext
            || context_artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run_id)
        {
            return Err(invalid());
        }
        let context: ExecutionContext =
            serde_json::from_slice(&blob::read_blob_with(connection, &context_artifact.blob)?)?;
        context.validate_complete_plan_closure()?;
        if context.run_id != *run_id
            || context.broker_session.as_deref() != Some(session_key)
            || context.plan_hash.as_ref() != Some(&commitment.plan_hash)
        {
            return Err(invalid());
        }

        let context_sources = [
            context.decision_context.clone(),
            context.account_snapshot.clone().expect("validated closure"),
            context.quote_snapshot.clone().expect("validated closure"),
            context
                .market_clock_snapshot
                .clone()
                .expect("validated closure"),
            context.execution_plan.clone().expect("validated closure"),
        ];
        // ExecutionContext.validate_complete_plan_closure 已证明这些 Option 存在；此处建立精确引用集合。
        if !has_exact_source_refs(&context_artifact, &context_sources) {
            return Err(invalid());
        }

        let plan_refs = context_artifact
            .source_refs
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::ExecutionPlan)
            .collect::<Vec<_>>();
        if plan_refs.len() != 1 {
            return Err(invalid());
        }
        if context.execution_plan.as_ref() != Some(plan_refs[0]) {
            return Err(invalid());
        }
        let plan_artifact = read_artifact(connection, &plan_refs[0].artifact_id)?;
        if plan_artifact.kind != ArtifactKind::ExecutionPlan
            || plan_artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run_id)
        {
            return Err(invalid());
        }
        let plan: ExecutionPlan = serde_json::from_slice(&blob::read_blob_with(connection, &plan_artifact.blob)?)?;
        plan.validate()?;
        if !has_exact_source_refs(
            &plan_artifact,
            &[
                plan.decision_context.clone(),
                plan.account_snapshot.clone(),
                plan.quote_snapshot.clone(),
                plan.market_clock_snapshot.clone(),
            ],
        ) || plan.decision_context != context.decision_context
            || Some(&plan.account_snapshot) != context.account_snapshot.as_ref()
            || Some(&plan.quote_snapshot) != context.quote_snapshot.as_ref()
            || Some(&plan.market_clock_snapshot) != context.market_clock_snapshot.as_ref()
            || plan.broker_session != session_key
            || context.plan_hash.as_ref() != Some(&plan.plan_hash)
            || plan.plan_hash != commitment.plan_hash
        {
            return Err(invalid());
        }
        Ok(plan)
    }

    fn validate_consumed_paper_approval(
        &self,
        connection: &Connection,
        session_key: &str,
        plan: &ExecutionPlan,
        committed_at: DateTime<Utc>,
    ) -> StoreResult<()> {
        // 通过 session_key 找唯一已消费 binding，累计 Buy orders 的 notional
        //（checked_add 防溢出），并检查 session date/expiry/maximum_notional。
        // 注意：当前调用 `validate_paper_approval_binding` 内部使用 `self.read_blob`，
        // 已持有调用方连接时存在再次获取非重入 Store Mutex 的源码风险；
        // 不能将此路径描述成全程同连接读取，也不以此断言运行故障。
        let invalid = || StoreError::InvalidSessionSlot(session_key.to_owned());
        let binding = connection
            .query_row(
                "SELECT runtime_manifest_artifact_id, approval_artifact_id FROM rebuild_paper_approval_consumptions WHERE session_key = ?1",
                params![session_key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let Some((manifest_id, approval_id)) = binding else {
            return Err(invalid());
        };
        let manifest_artifact =
            read_artifact(connection, &ArtifactId(ContentHash::new(manifest_id)?))?;
        let approval_artifact =
            read_artifact(connection, &ArtifactId(ContentHash::new(approval_id)?))?;
        let (manifest, approval) =
            self.validate_paper_approval_binding(&manifest_artifact, &approval_artifact)?;
        let session =
            chrono::NaiveDate::parse_from_str(session_key, "%Y-%m-%d").map_err(|_| invalid())?;
        let buy_notional = plan
            .orders
            .iter()
            .filter(|order| order.side == OrderSide::Buy)
            .try_fold(0_i64, |total, order| {
                total.checked_add(order.notional.0).ok_or_else(invalid)
            })?;
        if !manifest.authorizes_new_session(session, committed_at)
            || approval.expires_at < committed_at
            || buy_notional > manifest.maximum_notional.0
        {
            return Err(invalid());
        }
        Ok(())
    }

    fn validate_specialized_artifact(&self, artifact: &Artifact) -> StoreResult<()> {
        // 按 ArtifactKind 只对 DeliberationNote/RetrospectiveDraft/Retrospective/AttemptRelation 解码专门 payload；
        // Draft 和 AttemptRelation 还受 RunScoped/source lineage 限制，其他 kind 在此处 no-op。
        match artifact.kind {
            ArtifactKind::DeliberationNote => {
                let summary: akzio_domain::DeliberationSummary =
                    self.read_artifact_payload(artifact)?;
                summary.validate()?;
            }
            ArtifactKind::RetrospectiveDraft => {
                let draft: RetrospectiveDraft = self.read_artifact_payload(artifact)?;
                draft.validate()?;
                if artifact.lifecycle != ArtifactLifecycle::RunScoped {
                    return Err(StoreError::InvalidLearningCommit(
                        "retrospective_draft.lifecycle",
                    ));
                }
                let run_id = artifact
                    .origin
                    .as_ref()
                    .and_then(|origin| origin.run_id.as_ref())
                    .ok_or(StoreError::PermitOriginMismatch)?;
                for source in &artifact.source_refs {
                    // RetrospectiveDraft 的 cross-Run source 不允许，即使对应 Artifact 本身可读也拒绝。
                    let source_artifact = self.artifact(&source.artifact_id)?;
                    if source_artifact
                        .origin
                        .as_ref()
                        .and_then(|origin| origin.run_id.as_ref())
                        .is_some_and(|source_run| source_run != run_id)
                    {
                        return Err(StoreError::InvalidLearningCommit(
                            "retrospective_draft.cross_run_source",
                        ));
                    }
                }
            }
            ArtifactKind::Retrospective => {
                let retrospective: Retrospective = self.read_artifact_payload(artifact)?;
                retrospective.validate()?;
            }
            ArtifactKind::AttemptRelation => {
                let relation: AttemptRelation = self.read_artifact_payload(artifact)?;
                relation.validate()?;
                if artifact.lifecycle != ArtifactLifecycle::RunScoped {
                    return Err(StoreError::InvalidLearningCommit(
                        "attempt_relation.lifecycle",
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn validate_paper_effect_artifact(
        &self,
        effect: &ArtifactRef,
        run_id: &RunId,
    ) -> StoreResult<()> {
        // 先读 Artifact 元数据核对 Ref.kind、canonical lifecycle 和 Run origin，再按 kind 解码并 validate
        // PaperCommitment/PaperReprice/PaperCancel；只做持久化前的 Rust 校验，不执行 Broker 请求。
        let artifact = self.artifact(&effect.artifact_id)?;
        if effect.kind != artifact.kind
            || !matches!(
                artifact.kind,
                ArtifactKind::ExecutionCommitment
                    | ArtifactKind::ExecutionCancel
                    | ArtifactKind::ExecutionReprice
            )
            || artifact.lifecycle != ArtifactLifecycle::Canonical
            || artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run_id)
        {
            return Err(StoreError::InvalidPaperEffect(effect.artifact_id.clone()));
        }
        match artifact.kind {
            ArtifactKind::ExecutionCommitment => {
                let payload: PaperCommitment =
                    serde_json::from_slice(&self.read_blob(&artifact.blob)?)?;
                payload.validate()?;
            }
            ArtifactKind::ExecutionReprice => {
                let payload: PaperReprice =
                    serde_json::from_slice(&self.read_blob(&artifact.blob)?)?;
                payload.validate()?;
            }
            ArtifactKind::ExecutionCancel => {
                let payload: PaperCancel =
                    serde_json::from_slice(&self.read_blob(&artifact.blob)?)?;
                payload.validate()?;
            }
            _ => unreachable!("validated Paper effect kind"),
        }
        Ok(())
    }

    fn validate_attempt_commit(
        &self,
        permit: &TaskWritePermit,
        artifacts: &[Artifact],
        status: TaskStatus,
    ) -> StoreResult<()> {
        // 只允许 terminal TaskStatus；Succeeded 至少要一个 Artifact。逐项验证 Artifact、拒绝通用学习产物、
        // 确认 BLOB 可读并跑 specialized payload 校验，仍未写入 Artifact/Attempt 状态。
        if !status.is_terminal() {
            return Err(StoreError::TaskNotRunnable(permit.task_id.clone()));
        }
        if status == TaskStatus::Succeeded && artifacts.is_empty() {
            return Err(StoreError::Domain(DomainError::EmptyField {
                field: "commit_attempt.artifacts",
            }));
        }
        for artifact in artifacts {
            artifact.validate()?;
            reject_generic_learning_artifact(artifact)?;
            self.read_blob(&artifact.blob)?;
            self.validate_specialized_artifact(artifact)?;
        }
        Ok(())
    }
}
