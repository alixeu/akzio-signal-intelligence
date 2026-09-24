// 文件导读：核心 impl 提供非重入连接 guard、Paper approval/workflow 验证、bootstrap/freeze
// 写入和专用 Artifact 校验；内部函数优先复用已持有 Connection/Transaction，避免锁重入。
// 本文件先读 CONNECTION_HELD/ConnectionGuard，再读 Store 的验证入口和写入入口；同步 SQLite
// 调用不自行创建 Tokio 任务，异步调用方须在自己的边界处理阻塞调度，事务回滚由 RAII 承担。
// thread_local! 为每个 OS 线程生成独立 Cell；它只检测同一线程重入，不是跨线程/跨进程锁。
thread_local! {
    /// Whether this thread currently holds the Store connection. Tracking it
    /// turns a nested acquisition into a diagnosable error instead of a silent,
    /// permanent hang; genuine contention between threads still blocks and
    /// proceeds normally.
    static CONNECTION_HELD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn connection_held_by_current_thread() -> bool {
    // 线程局部标记只检测同线程重入，不把跨线程 SQLite Mutex 竞争误判为错误。
    CONNECTION_HELD.with(std::cell::Cell::get)
}

/// The Store connection guard. Dereferences to [`Connection`] so every existing
/// call site is unchanged, and clears this thread's ownership flag on drop.
pub(super) struct ConnectionGuard<'store> {
    guard: std::sync::MutexGuard<'store, Connection>,
}

impl<'store> ConnectionGuard<'store> {
    // 取得 guard 时登记 owner，Drop 会清除标记；Connection 生命周期仍由 MutexGuard 管理。
    fn new(guard: std::sync::MutexGuard<'store, Connection>) -> Self {
        CONNECTION_HELD.with(|held| held.set(true));
        Self { guard }
    }
}

impl Drop for ConnectionGuard<'_> {
    // 释放 guard 前清除线程局部标记，使后续同线程 acquisition 可以继续。
    fn drop(&mut self) {
        CONNECTION_HELD.with(|held| held.set(false));
    }
}

impl std::ops::Deref for ConnectionGuard<'_> {
    type Target = Connection;

    // 只读调用沿用 rusqlite Connection API，不复制或打开第二连接。
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl std::ops::DerefMut for ConnectionGuard<'_> {
    // 写事务需要可变 Connection，但仍受同一非重入 guard 约束。
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl Store {
    // 返回 Store Root 路径；不检查或创建数据库。
    pub fn root(&self) -> &Path {
        self.root.as_ref()
    }

    /// Acquire the single Store connection.
    ///
    /// The guard is a non-reentrant `std::sync::Mutex`, so acquiring it twice on
    /// one thread would block that thread forever, holding both this lock and
    /// any open SQLite write transaction. Because a silent hang is far worse to
    /// diagnose than an error, a nested acquisition is reported instead of
    /// deadlocking: the fix is always for the inner call to take the caller's
    /// `&Connection`/`&Transaction` rather than reach for the mutex again.
    // 输入是 Store 的共享句柄，输出 guard 持有 Mutex 至离开作用域；重入或 poisoned lock 返回
    // Integrity 错误。guard 内部仍借用 Store，编译器因此阻止它活得比连接所有者更久。
    fn connection(&self) -> StoreResult<ConnectionGuard<'_>> {
        if connection_held_by_current_thread() {
            return Err(StoreError::Integrity(
                "store connection acquired twice on one thread; pass the caller's \
                 connection or transaction to the inner call instead"
                    .to_owned(),
            ));
        }
        let guard = self
            .connection
            .lock()
            .map_err(|_| StoreError::Integrity("store connection poisoned".to_owned()))?;
        Ok(ConnectionGuard::new(guard))
    }

    // 输入 Run ID，反复调用分页 API 并按 cursor 顺序合并为 Vec；每次最多取 256 条。
    // 空页必定结束；短页只有在累计 events.len() 仍小于 256 时结束，所以完整首批后短尾页会再查一次空页。
    // 各页不是同一个 SQLite 事务快照；任一页失败即 Err，不返回已收集的部分 Vec。
    fn read_all_events(&self, run_id: &RunId) -> StoreResult<Vec<StoredEvent>> {
        const PAGE_SIZE: usize = 256;
        let mut after = 0_i64;
        let mut events = Vec::new();
        loop {
            let page = self.events_after(run_id, after, PAGE_SIZE)?;
            if page.is_empty() {
                break;
            }
            after = page.last().expect("non-empty event page").cursor;
            events.extend(page);
            if events.len() < PAGE_SIZE {
                break;
            }
        }
        Ok(events)
    }

    // 输入两个候选 Artifact，先检查 kind/lifecycle/source_refs，再分别读 BLOB 并校验领域负载绑定；
    // 成功返回两个已解析负载，失败则不写 Store。SQL/BLOB 读取没有包在共同事务中。
    fn validate_paper_approval_binding(
        &self,
        runtime_manifest: &Artifact,
        approval: &Artifact,
    ) -> StoreResult<(RuntimeManifest, PaperLaunchApproval)> {
        if runtime_manifest.kind != ArtifactKind::RuntimeManifest
            || approval.kind != ArtifactKind::PaperLaunchApproval
            || runtime_manifest.lifecycle != ArtifactLifecycle::Canonical
            || approval.lifecycle != ArtifactLifecycle::Canonical
            || runtime_manifest.origin.is_some()
            || approval.origin.is_some()
            || approval.source_refs
                != vec![ArtifactRef {
                    artifact_id: runtime_manifest.artifact_id.clone(),
                    kind: ArtifactKind::RuntimeManifest,
                }]
        {
            return Err(StoreError::InvalidSessionSlot(
                "paper-approval-binding".to_owned(),
            ));
        }
        runtime_manifest.validate()?;
        approval.validate()?;
        let manifest_payload: RuntimeManifest =
            serde_json::from_slice(&self.read_blob(&runtime_manifest.blob)?)?;
        let approval_payload: PaperLaunchApproval =
            serde_json::from_slice(&self.read_blob(&approval.blob)?)?;
        manifest_payload.validate()?;
        approval_payload.validate()?;
        if approval_payload.runtime_manifest.artifact_id != runtime_manifest.artifact_id
            || approval_payload.runtime_manifest_hash != manifest_payload.manifest_hash()?
            || approval_payload.expires_at > manifest_payload.expires_at
        {
            return Err(StoreError::InvalidSessionSlot(
                "paper-approval-binding".to_owned(),
            ));
        }
        Ok((manifest_payload, approval_payload))
    }

    pub(super) fn validate_paper_session_reservation(
        &self,
        reservation: &SessionReservation,
        proposal: &Artifact,
    ) -> StoreResult<()> {
        // 输入冻结的 Paper graph、proposal 与 setup EvidenceNeed；输出只表示校验通过，不创建 slot。
        // 先验证 Paper graph/proposal/setup EvidenceNeed 的 Run lineage，再允许 slot 事务写入。
        if reservation.session_key.trim().is_empty()
            || reservation.workflow.run.purpose != RunPurpose::Paper
            || reservation.workflow.graph.kind != ArtifactKind::WorkflowGraph
            || reservation.workflow.graph.artifact_id != reservation.workflow.run.graph_artifact_id
            || proposal.kind != ArtifactKind::WorkflowProposal
            || proposal.producer != "runtime.paper_provisioning"
            || proposal.lifecycle != ArtifactLifecycle::RunScoped
            || proposal
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(&reservation.workflow.run.run_id)
            || reservation.setup_artifacts.iter().any(|artifact| {
                artifact.kind != ArtifactKind::EvidenceNeed
                    || artifact.lifecycle != ArtifactLifecycle::RunScoped
                    || artifact
                        .origin
                        .as_ref()
                        .and_then(|origin| origin.run_id.as_ref())
                        != Some(&reservation.workflow.run.run_id)
            })
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
        let proposal_payload: WorkflowProposal =
            serde_json::from_slice(&self.read_blob(&proposal.blob)?)?;
        let expected_sources = reservation
            .setup_artifacts
            .iter()
            .map(|artifact| ArtifactRef {
                artifact_id: artifact.artifact_id.clone(),
                kind: artifact.kind,
            })
            .collect::<BTreeSet<_>>();
        let actual_sources = proposal
            .source_refs
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let payload_needs = proposal_payload
            .tasks
            .values()
            .flat_map(|task| task.evidence_needs.iter().cloned())
            .collect::<BTreeSet<_>>();
        let expected_sources = expected_sources
            .into_iter()
            .chain(payload_needs)
            .collect::<BTreeSet<_>>();
        if actual_sources != expected_sources {
            return Err(StoreError::InvalidWorkflowProposalArtifact);
        }
        if proposal_payload.topology_id != reservation.workflow.run.topology_id {
            return Err(StoreError::WorkflowGraphMismatch);
        }
        proposal.validate()?;
        for artifact in &reservation.setup_artifacts {
            artifact.validate()?;
            self.read_blob(&artifact.blob)?;
        }
        Ok(())
    }

    // 对准备持久化的 WorkflowCommit 做纯校验：拒绝已退役 purpose/Contract，读取 graph BLOB，
    // 并要求 JSON graph 的 nodes/topology 与 Run 外层字段完全一致；不创建 Run/Task 行。
    pub(super) fn validate_workflow_commit(&self, commit: &WorkflowCommit) -> StoreResult<()> {
        if commit.run.purpose == RunPurpose::PaperDryRun || commit.nodes.iter().any(|n| n.recipe_id.as_str() == "research.planner" || commit.run.purpose == RunPurpose::Debug && n.recipe_id.as_str().starts_with("research.")) {
            return Err(StoreError::DebugControl("legacy_workflow_retired".into()));
        }
        for node in &commit.nodes {
            if let Some(hash) = &node.contract_hash {
                if self.contract_installation(hash)?.is_some_and(|c| matches!(c.contract.purpose.as_str(), "research.analyst" | "research.critic" | "research.synthesizer") && c.contract.version < 65) {
                    return Err(StoreError::DebugControl("legacy_workflow_retired".into()));
                }
            }
        }
        if commit.graph.kind != ArtifactKind::WorkflowGraph
            || commit.graph.artifact_id != commit.run.graph_artifact_id
        {
            return Err(StoreError::InvalidWorkflowGraphArtifact);
        }
        commit.graph.validate()?;
        let graph: WorkflowGraph = serde_json::from_slice(&self.read_blob(&commit.graph.blob)?)?;
        graph.validate()?;
        if graph.nodes != commit.nodes || graph.topology_id != commit.run.topology_id {
            return Err(StoreError::WorkflowGraphMismatch);
        }
        Ok(())
    }

    /// Atomically publishes one runtime manifest and its operator approval.
    /// A scheduler can never observe a half-written approval binding.
    pub fn write_paper_approval_binding(
        &self,
        runtime_manifest: &Artifact,
        approval: &Artifact,
    ) -> StoreResult<()> {
        // 先在事务外读取并验证传入的 manifest/approval 绑定；通过后才以 IMMEDIATE 事务按
        // manifest→approval 顺序插入。任一插入或 commit 失败都会返回 Err，未提交的行随 Transaction Drop 回滚。
        self.validate_paper_approval_binding(runtime_manifest, approval)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        insert_artifact(&transaction, runtime_manifest)?;
        insert_artifact(&transaction, approval)?;
        transaction.commit()?;
        Ok(())
    }

    /// Writes a root artifact such as an installed Contract. Bootstrap is deliberately
    /// narrow: a task-origin artifact must use `write_task_artifact` instead.
    pub fn write_bootstrap_artifact(&self, artifact: &Artifact) -> StoreResult<()> {
        // 只允许无 task origin 的 Contract/FreezeState 根产物；先校验并确认引用 BLOB 可读，
        // 再单独事务插入 Artifact 元数据/引用，提交前失败不会发布该 Artifact。
        artifact.validate()?;
        if artifact.origin.is_some()
            || !matches!(
                artifact.kind,
                ArtifactKind::Contract | ArtifactKind::FreezeState
            )
        {
            return Err(StoreError::PermitOriginMismatch);
        }
        self.read_blob(&artifact.blob)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        insert_artifact(&transaction, artifact)?;
        transaction.commit()?;
        Ok(())
    }

    /// 追加一条不可变的操作员 `FreezeState` Artifact；执行侧读取最新
    /// canonical 冻结状态，而不是在这里维护可变布尔开关。
    pub fn write_freeze_state(
        &self,
        frozen: bool,
        reason: impl Into<String>,
        changed_at: DateTime<Utc>,
    ) -> StoreResult<Artifact> {
        // `impl Into<String>` 接受调用方可转换为 String 的具体类型，
        // `reason.into()` 在这里取得 payload 所需的拥有型文本；不借用临时调用参数。
        // 先校验并暂存 JSON，再复用窄权限 bootstrap 写入 canonical Artifact；返回 Artifact
        // 表示该行已提交，但不会连带修改其他开关或跨进程状态。
        let payload = FreezeState {
            schema_version: DOMAIN_SCHEMA_VERSION,
            frozen,
            reason: reason.into(),
            changed_at,
        };
        payload.validate()?;
        let artifact = Artifact::new(
            ArtifactKind::FreezeState,
            self.stage_json(&payload)?,
            "store.freeze_state",
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: "akzio.operator".to_owned(),
                observed_at: Some(changed_at),
                retrieved_at: changed_at,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            None,
            Vec::new(),
            changed_at,
        )?;
        self.write_bootstrap_artifact(&artifact)?;
        Ok(artifact)
    }

    // 输入有效 AgentContract 的借用和目录时间，生成无 Run origin 的 canonical catalogue Artifact；
    // JSON BLOB 先经 stage_json 处理，Artifact 本身要由安装调用方在后续事务提交。
    fn contract_artifact(
        &self,
        contract: &AgentContract,
        now: DateTime<Utc>,
    ) -> StoreResult<Artifact> {
        Ok(Artifact::new(
            ArtifactKind::Contract,
            self.stage_json(contract)?,
            "research.contract_catalogue",
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: "akzio.contract_catalogue".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            None,
            vec![],
            now,
        )?)
    }

    // 使用调用方 Connection/Transaction 按 contract_hash 查安装行，并 LEFT JOIN 当前 head 的
    // activation 时间；无安装返回 None，有行则核验 Artifact kind/lifecycle 与 payload hash。
    // map 闭包内的 `?` 失败会由 transpose 转成外层 StoreResult，而不是被当成“没有安装”。
    fn stored_contract_with_connection(
        &self,
        connection: &Connection,
        contract_hash: &ContentHash,
    ) -> StoreResult<Option<StoredContract>> {
        let row = connection
            .query_row(
                r#"SELECT contract_artifact_id, baseline_contract_hash, installed_at,
                          activation.activated_at
                   FROM rebuild_contract_installations AS installation
                   LEFT JOIN rebuild_contract_catalogue_heads AS head
                     ON head.contract_hash = installation.contract_hash
                   LEFT JOIN rebuild_contract_activations AS activation
                     ON activation.activation_id = head.activation_id
                   WHERE installation.contract_hash = ?1"#,
                params![contract_hash.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()?;
        row.map(|(artifact_id, baseline, installed_at, activated_at)| {
            let artifact = read_artifact(connection, &ArtifactId(ContentHash::new(artifact_id)?))?;
            if artifact.kind != ArtifactKind::Contract
                || artifact.lifecycle != ArtifactLifecycle::Canonical
            {
                return Err(StoreError::Integrity(format!(
                    "contract {contract_hash} has an invalid artifact"
                )));
            }
            let contract: AgentContract =
                self.read_artifact_payload_with_connection(connection, &artifact)?;
            contract.validate()?;
            if contract.contract_hash != *contract_hash {
                return Err(StoreError::Integrity(format!(
                    "contract installation {contract_hash} payload hash diverges"
                )));
            }
            Ok(StoredContract {
                contract,
                artifact,
                baseline_contract_hash: baseline.map(ContentHash::new).transpose()?,
                installed_at: parse_time(&installed_at)?,
                activated_at: activated_at.map(|value| parse_time(&value)).transpose()?,
            })
        })
        .transpose()
    }

    // 将 Policy transition 投影为 Contract activation/head 变更；输入仍受同一事务保护。
    // 参数 transaction 是外层写入事务的借用，本函数不获取连接锁或自行 commit。
    fn apply_contract_catalogue_transition(
        &self,
        transaction: &Transaction<'_>,
        commit: &PolicyEvaluationCommit,
        transition: &PolicyTransition,
    ) -> StoreResult<()> {
        // 只处理 Contract subject；其他 Policy subject 立即 no-op。整个过程使用调用方事务，
        // 所以安装校验、历史 activation 与目录 head 要么随外层一起提交，要么一同回滚。
        let PolicySubject::Contract(candidate_hash) = &commit.subject else {
            return Ok(());
        };
        let candidate = self
            .stored_contract_with_connection(transaction, candidate_hash)?
            .ok_or_else(|| StoreError::MissingContractInstallation(candidate_hash.clone()))?;
        let Some(baseline_hash) = candidate.baseline_contract_hash.as_ref() else {
            return Err(StoreError::ContractActivationConflict(
                candidate.contract.purpose,
            ));
        };

        match (transition.from, transition.to) {
            (_, PolicyState::Contract(CandidatePolicyState::Active)) => {
                // 激活分支要求 commit 带候选 Policy，并绑定候选 Contract、当前 baseline head
                // 与受限能力；检查通过后追加不可变 activation，再更新可重建的 head 指针。
                let candidate_policy_artifact =
                    commit
                        .candidate_policy
                        .as_ref()
                        .ok_or(StoreError::InvalidLearningCommit(
                            "contract_catalogue.candidate_policy",
                        ))?;
                let candidate_policy: CandidatePolicy = self
                    .read_artifact_payload_with_connection(transaction, candidate_policy_artifact)?;
                if candidate_policy.candidate.artifact_id != candidate.artifact.artifact_id
                    || candidate_policy.baseline.kind != ArtifactKind::Contract
                    || candidate_policy.subject != commit.subject
                {
                    return Err(StoreError::InvalidLearningCommit(
                        "contract_catalogue.candidate_policy_binding",
                    ));
                }
                let Some((current_hash, _)) =
                    contract_catalogue_head(transaction, &candidate.contract.purpose)?
                else {
                    return Err(StoreError::ContractActivationConflict(
                        candidate.contract.purpose.clone(),
                    ));
                };
                let current = self
                    .stored_contract_with_connection(transaction, &current_hash)?
                    .ok_or_else(|| StoreError::MissingContractInstallation(current_hash.clone()))?;
                if current.contract.contract_hash != *baseline_hash
                    || candidate_policy.baseline.artifact_id != current.artifact.artifact_id
                    || !candidate_is_bounded(&current.contract, &candidate.contract)
                {
                    return Err(StoreError::ContractActivationConflict(
                        candidate.contract.purpose.clone(),
                    ));
                }
                let activation_id = append_contract_activation(
                    transaction,
                    &candidate.contract.purpose,
                    Some(&current_hash),
                    candidate_hash,
                    Some(&transition.transition_id),
                    transition.created_at,
                )?;
                set_contract_catalogue_head(
                    transaction,
                    &candidate.contract.purpose,
                    candidate_hash,
                    activation_id,
                )?;
            }
            (PolicyState::Contract(CandidatePolicyState::Active), PolicyState::Contract(_)) => {
                // 撤销/降级分支只允许当前 head 正是该候选，并把 head 指回安装时冻结的 baseline；
                // 历史 activation 仍追加保存，不删除旧记录。
                let Some((current_hash, _)) =
                    contract_catalogue_head(transaction, &candidate.contract.purpose)?
                else {
                    return Err(StoreError::ContractActivationConflict(
                        candidate.contract.purpose.clone(),
                    ));
                };
                if current_hash != *candidate_hash {
                    return Err(StoreError::ContractActivationConflict(
                        candidate.contract.purpose.clone(),
                    ));
                }
                let baseline = self
                    .stored_contract_with_connection(transaction, baseline_hash)?
                    .ok_or_else(|| {
                        StoreError::MissingContractInstallation(baseline_hash.clone())
                    })?;
                if baseline.contract.purpose != candidate.contract.purpose {
                    return Err(StoreError::ContractActivationConflict(
                        candidate.contract.purpose.clone(),
                    ));
                }
                let activation_id = append_contract_activation(
                    transaction,
                    &candidate.contract.purpose,
                    Some(candidate_hash),
                    baseline_hash,
                    Some(&transition.transition_id),
                    transition.created_at,
                )?;
                set_contract_catalogue_head(
                    transaction,
                    &candidate.contract.purpose,
                    baseline_hash,
                    activation_id,
                )?;
            }
            // 不涉及进入或离开 Active 的状态转换不触碰 Contract catalogue。
            _ => {}
        }
        Ok(())
    }
}
