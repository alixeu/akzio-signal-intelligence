//! Persisted execution authority, shared by every claim path and every client.
// 文件导读：Debug 控制把隔离身份、CAS session identity、暂停/单步/恢复和 Broker policy
// 持久化到 Store；inspect 只读投影，claim/settle 才在事务中消费 permit 或推进控制 head。
// 建议先读 configure_debug_environment/reserve_debug_session 建立身份，再读 debug_control 的 CAS
// 状态机与 consume_claim/settle_attempt 的 Attempt 边界，最后读 debug_inspect 的只读投影和 redact。
use super::trajectory::stored_event_from_row;
use super::*;
use akzio_domain::{
    AcceptanceCategory, AcceptanceCheck, AcceptanceResult, DebugAction, DebugBrokerPolicy,
    DebugControlRequest, DebugExecutionMode, DebugLearningScope, DebugSession,
    DebugSessionIdentity, DebugStatus, StageAcceptance,
};

// 将控制层阻断原因统一包装为 DebugControl，调用方可区分于 SQL/Integrity 错误。
fn blocked(reason: impl Into<String>) -> StoreError {
    // Into<String> 接受静态字面量或已拥有的错误文本；只包装控制层拒绝，不触碰连接或数据库。
    StoreError::DebugControl(reason.into())
}

#[derive(Debug, Clone, Serialize)]
pub struct DebugAttemptView {
    pub attempt_id: AttemptId,
    pub status: String,
    pub started_at: String,
    pub finished_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DebugNodeView {
    pub task: StoredTaskSnapshot,
    pub role: String,
    pub horizon: Option<String>,
    pub attempts: Vec<DebugAttemptView>,
    pub business_ready: bool,
    pub step_eligible: bool,
    pub retry_eligible: bool,
    pub blocked_reason: Option<String>,
    pub output_refs: Vec<ArtifactRef>,
    pub budget: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct DebugArtifactView {
    pub artifact: Artifact,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct DebugRunView {
    pub inspection: super::run_control::RunInspection,
    pub research: serde_json::Value,
    pub session: DebugSession,
    pub workflow_status: WorkflowStatus,
    pub execution_evidence: String,
    pub nodes: Vec<DebugNodeView>,
    pub events: Vec<StoredEvent>,
    pub artifacts: Vec<DebugArtifactView>,
    pub acceptance: Vec<StageAcceptance>,
    pub observed_at: DateTime<Utc>,
    pub allowed_actions: Vec<String>,
}

impl Store {
    /// A Store can be marked isolated only before its first Run. The marker is
    /// permanent: opening this Store as an ordinary Core is subsequently denied.
    // enabled 时先尝试比较当前 Root 与 canonical ~/.akzio/store 的 canonicalize 结果；
    // 当前写法用两个 Option 比较，路径都无法解析时也会相等并拒绝，不能把此检查描述为
    // 对所有路径情形都精确识别 canonical Root。随后以 IMMEDIATE 事务读写永久 metadata 标记。
    // 已有标记可重复返回，未标记但已有 Run 会拒绝；函数不会清除隔离身份。
    pub fn configure_debug_environment(&self, enabled: bool) -> StoreResult<Option<String>> {
        if enabled
            && std::env::var_os("HOME").is_some_and(|home| {
                let canonical = PathBuf::from(home).join(".akzio/store");
                self.root.canonicalize().ok() == canonical.canonicalize().ok()
            })
        {
            return Err(blocked(
                "Debug requires a separate Store, never ~/.akzio/store",
            ));
        }
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        // 查询结果 None 表示尚未隔离，Some 表示历史标记已经存在；关闭请求不能撤销该标记。
        let existing = environment_identity(&tx)?;
        if !enabled {
            if existing.is_some() {
                return Err(blocked("isolated Store requires debug_control=true"));
            }
            return Ok(None);
        }
        if let Some(existing) = existing {
            return Ok(Some(existing));
        }
        // 只在 Run 数为零时允许首次标记，保证隔离用途从 Store 的第一个 Run 起就可识别。
        let count: u64 = tx.query_row("SELECT count(*) FROM rebuild_runs", [], |r| r.get(0))?;
        if count != 0 {
            return Err(blocked("debug_control requires a new isolated Store"));
        }
        let identity = format!("debug-store-{}", RunId::new());
        tx.execute(
            "INSERT INTO rebuild_metadata(key,value) VALUES('debug_environment',?1)",
            params![identity],
        )?;
        tx.commit()?;
        Ok(Some(identity))
    }

    // 只读取 metadata 中的隔离 identity，不创建或修复环境标记；无行是 Ok(None)，SQL 错误仍传播。
    pub fn debug_environment(&self) -> StoreResult<Option<String>> {
        environment_identity(&*self.connection()?)
    }

    /// Publish the frozen business graph and its paused control atomically.
    /// No worker can observe the new Run without observing its scheduling gate.
    // 将预校验通过的 Paper reservation 与 DebugSession identity 在同一立即事务发布；
    // `approval_binding=None` 只表示本次 reservation 未携带该可选绑定，不绕过 workflow 校验。
    pub fn reserve_debug_session(
        &self,
        lease: &DaemonLease,
        reservation: &SessionReservation,
        proposal: &Artifact,
        identity: &DebugSessionIdentity,
        approval_binding: Option<(&Artifact, &Artifact)>,
    ) -> StoreResult<DebugSession> {
        // 较贵的图/来源校验先在事务外做；正式插入阶段仍在事务内绑定 lease、Run 和控制 head。
        self.validate_paper_session_reservation(reservation, proposal)?;
        if let Some((manifest, approval)) = approval_binding {
            self.validate_paper_approval_binding(manifest, approval)?;
        }
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        validate_identity(&tx, identity, &reservation.workflow)?;
        Self::commit_session_slot_transaction(&tx, lease, reservation, proposal, approval_binding)?;
        insert_session(&tx, identity)?;
        // 先写 Paper slot/Run graph，再写暂停态 identity/control；任何一步失败都会由 Drop 回滚整组。
        let session = read_session(&tx, &identity.run_id)?.expect("inserted session");
        tx.commit()?;
        Ok(session)
    }

    /// Noncanonical experiment: new IDs and immutable parent artifact lineage.
    /// It uses the ordinary non-Paper lowering and never acquires a session slot.
    // 限定 purpose 为 Debug/PositionPlan；setup Artifact、workflow、Run 事件与 identity/control
    // 同事务安装。返回已提交的 session，不表示 workflow worker 已被领取或执行。
    pub fn commit_debug_experiment(
        &self,
        workflow: &WorkflowCommit,
        setup: &[Artifact],
        identity: &DebugSessionIdentity,
    ) -> StoreResult<DebugSession> {
        if !matches!(
            workflow.run.purpose,
            RunPurpose::Debug | RunPurpose::PositionPlan
        ) {
            return Err(blocked("experiment must be noncanonical"));
        }
        self.validate_workflow_commit(workflow)?;
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        validate_identity(&tx, identity, workflow)?;
        // setup 与 graph/Run 一起进入事务；后续任何 lineage/session 错误不会留下半个实验 Run。
        insert_artifact_batch(&tx, setup)?;
        Self::commit_workflow_transaction(&tx, workflow)?;
        Self::append_run_setup_events(&tx, &workflow.run.run_id, setup, workflow.run.created_at)?;
        insert_session(&tx, identity)?;
        let session = read_session(&tx, &identity.run_id)?.expect("inserted session");
        tx.commit()?;
        Ok(session)
    }

    // 从 run_control head 指向的 CAS identity Artifact 重建 DebugSession；只读，不修复过期 head。
    pub fn debug_session(&self, run_id: &RunId) -> StoreResult<Option<DebugSession>> {
        read_session(&*self.connection()?, run_id)
    }

    /// Compare-and-swap is mandatory. A repeated HTTP POST cannot issue a second
    /// permit, even if the first step has already finished on another worker.
    // 输入预期 revision、控制动作、精确 RuntimeIdentity 与时钟；IMMEDIATE 事务内检查 CAS、
    // 当前 Attempt 数和 Task readiness，再保存新 control head/event；失败不提交任何状态变更。
    pub fn debug_control(
        &self,
        run_id: &RunId,
        request: &DebugControlRequest,
        runtime_identity: &ContentHash,
        now: DateTime<Utc>,
    ) -> StoreResult<DebugSession> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        super::workflow::assert_workflow_executable(&tx, run_id)?;
        let mut session =
            read_session(&tx, run_id)?.ok_or_else(|| blocked("not a Debug session"))?;
        // revision 是客户端读到的 CAS 版本；不一致时直接冲突，防止旧 POST 覆盖另一 worker 的控制结果。
        if session.revision != request.expected_revision {
            return Err(blocked("revision_conflict"));
        }
        if matches!(session.status, DebugStatus::Aborted) {
            return Err(blocked("session_aborted"));
        }
        if request.action != DebugAction::Pause
            && request.action != DebugAction::Abort
            && &session.identity.runtime_identity != runtime_identity
        {
            return Err(blocked("runtime_identity_changed: create a new experiment"));
        }
        // 同步读取当前 Run 的 running Task 数；Pause 可发出协作式请求，Step/Abort 必须等当前执行排空。
        let running = running_count(&tx, run_id)?;
        match request.action {
            DebugAction::Pause => {
                // 清除尚未消费的 Step permit；已有 Attempt 不取消，只把状态标为等待自然收尾的 PauseRequested。
                session.permitted_task_id = None;
                session.execution_mode = DebugExecutionMode::Manual;
                session.status = if running > 0 {
                    DebugStatus::PauseRequested
                } else {
                    DebugStatus::Paused
                };
            }
            DebugAction::Resume => {
                // Resume 只允许 Paused 状态，并切到 Continuous；RuntimeIdentity 已在 match 前校验。
                if session.status != DebugStatus::Paused {
                    return Err(blocked("resume_requires_paused"));
                }
                session.status = DebugStatus::Running;
                session.execution_mode = DebugExecutionMode::Continuous;
                session.permitted_task_id = None;
            }
            DebugAction::Step | DebugAction::RetryNode => {
                // 两种单步动作都必须完全暂停；task_id 是调用者明确选择的唯一节点，不由 Store 猜选。
                if session.status != DebugStatus::Paused || running != 0 {
                    return Err(blocked("step_requires_paused"));
                }
                let task_id = request
                    .task_id
                    .as_ref()
                    .ok_or_else(|| blocked("unique_task_id_required"))?;
                if let Some(reason) = task_blocked_reason(&tx, run_id, task_id, now)? {
                    return Err(blocked(reason));
                }
                // RetryNode 还要有可重试的终态 Attempt；Step 不会因此重置 retry budget。
                if request.action == DebugAction::RetryNode && !retry_eligible(&tx, task_id)? {
                    return Err(blocked(
                        "not_retryable: terminal or exhausted stages require a new experiment",
                    ));
                }
                session.status = DebugStatus::Stepping;
                session.execution_mode = DebugExecutionMode::Manual;
                session.permitted_task_id = Some(task_id.clone());
                session.active_attempt_id = None;
            }
            DebugAction::Abort => {
                // Cooperative debug abort drains active work, never drops futures.
                if running != 0 {
                    return Err(blocked("pause_and_drain_before_abort"));
                }
                session.status = DebugStatus::Aborted;
                session.permitted_task_id = None;
            }
        }
        save_session(&tx, &mut session, now)?;
        tx.commit()?;
        Ok(session)
    }

    /// Defense at the actual effect-intent boundary, including cancel/reprice.
    // 在实际 Broker effect intent 调用边界重读 Store policy；不是 UI 预检查，也不会发出网络请求。
    pub fn assert_debug_broker_write(&self, run_id: &RunId) -> StoreResult<()> {
        assert_broker_write(&*self.connection()?, run_id)
    }

    // 对被禁止的 Broker task，在一个立即事务内暂停 session 并写验收事件；返回 true 表示阻断已持久化，
    // false 表示 policy 允许而本方法没有更改 Store。当前 Attempt permit 不在这里结算/释放。
    pub fn block_debug_broker_task(
        &self,
        permit: &TaskWritePermit,
        evidence: &[ArtifactRef],
        now: DateTime<Utc>,
    ) -> StoreResult<bool> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        // 允许时不修改状态并由事务 Drop 结束；只把明确的 DebugBrokerWriteForbidden 转成持久化暂停，
        // 其他 StoreError 不降级处理。
        match assert_broker_write(&tx, &permit.run_id) {
            Ok(()) => return Ok(false),
            Err(StoreError::DebugBrokerWriteForbidden) => {}
            Err(error) => return Err(error),
        }
        assert_permit(&tx, permit)?;
        let mut session =
            read_session(&tx, &permit.run_id)?.ok_or(StoreError::DebugBrokerWriteForbidden)?;
        session.status = DebugStatus::PauseRequested;
        session.permitted_task_id = None;
        save_session(&tx, &mut session, now)?;
        write_acceptance(
            &tx,
            &StageAcceptance {
                version: 1,
                run_id: permit.run_id.clone(),
                task_id: permit.task_id.clone(),
                attempt_id: permit.attempt_id.clone(),
                stage: "broker_dispatch".into(),
                business_result: "Accepted".into(),
                test_result: AcceptanceResult::Blocked,
                checks: vec![AcceptanceCheck {
                    check_id: "debug.broker_write_policy".into(),
                    category: AcceptanceCategory::SideEffect,
                    expected: "explicit paper_allowed policy".into(),
                    actual: "forbidden".into(),
                    result: AcceptanceResult::Blocked,
                    evidence_refs: evidence.to_vec(),
                    message: "Business prerequisites: PASS; Debug broker write policy: BLOCK"
                        .into(),
                }],
                created_at: now,
            },
        )?;
        tx.commit()?;
        Ok(true)
    }

    // 隔离判定读取 Store metadata 或 Run 的 DebugSession.learning_scope；没有调用方布尔参数可伪造身份。
    pub fn debug_learning_isolated(&self, run_id: &RunId) -> StoreResult<bool> {
        let connection = self.connection()?;
        Ok(environment_identity(&connection)?.is_some()
            || read_session(&connection, run_id)?
                .is_some_and(|s| s.identity.learning_scope == DebugLearningScope::Isolated))
    }

    /// One read transaction, no repairs, no staging, no claim and no network.
    // 用 Deferred 只读事务构造指定 Run/可选 Task/Attempt 的检查投影；不领取 Task、不写 Artifact、
    // 不暂存 BLOB、不触发联网。返回的 business_ready 与 step_eligible 是此读事务时点的观察值。
    pub fn debug_inspect(
        &self,
        run_id: &RunId,
        task_id: Option<&TaskId>,
        attempt_id: Option<&AttemptId>,
        now: DateTime<Utc>,
    ) -> StoreResult<DebugRunView> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let session = read_session(&tx, run_id)?.ok_or_else(|| blocked("not a Debug session"))?;
        let snapshot = self.workflow_snapshot_with_connection(&tx, run_id)?;
        let retired = super::workflow::legacy_workflow(&tx, run_id)?;
        if let Some(task_id) = task_id {
            if !snapshot.tasks.iter().any(|t| &t.node.task_id == task_id) {
                return Err(StoreError::MissingTask(task_id.clone()));
            }
        }
        if let Some(attempt) = attempt_id {
            let exists: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM rebuild_attempts WHERE run_id=?1 AND attempt_id=?2 AND (?3 IS NULL OR task_id=?3))",
                params![run_id.0, attempt.0, task_id.map(|t| t.0.as_str())], |r| r.get(0))?;
            if !exists {
                return Err(blocked("attempt_not_in_selected_run_and_task"));
            }
        }
        // 遍历快照中的 task：可选 task_id 过滤只跳过不匹配节点；attempt_id 另用于尝试、事件和预算过滤。
        let mut nodes = Vec::new();
        for task in snapshot.tasks {
            if task_id.is_some_and(|id| id != &task.node.task_id) {
                continue;
            }
            let attempts = tx.prepare("SELECT attempt_id,status,started_at,finished_at FROM rebuild_attempts WHERE task_id=?1 ORDER BY epoch")?
                .query_map(params![task.node.task_id.0], |r| Ok(DebugAttemptView {
                    attempt_id: AttemptId(r.get(0)?), status: r.get(1)?, started_at: r.get(2)?, finished_at: r.get(3)?,
                }))?.collect::<Result<Vec<_>, _>>()?;
            let reason = if retired {
                Some("legacy_workflow_retired".into())
            } else {
                task_blocked_reason(&tx, run_id, &task.node.task_id, now)?
            };
            let retry = retry_eligible(&tx, &task.node.task_id)?;
            // 输出通过 attempt_outputs→artifacts→attempts 关联并限制到当前 Task；Event 过滤范围则按可选 attempt_id 精确收窄。
            let output_refs = tx.prepare("SELECT a.artifact_id,a.kind FROM rebuild_attempt_outputs o JOIN rebuild_artifacts a ON a.artifact_id=o.artifact_id JOIN rebuild_attempts p ON p.attempt_id=o.attempt_id WHERE p.task_id=?1 ORDER BY o.event_id")?
                .query_map(params![task.node.task_id.0], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?
                .collect::<Result<Vec<_>, _>>()?.into_iter().map(|(id,kind)| Ok(ArtifactRef {artifact_id:ArtifactId(ContentHash::new(id)?),kind:parse_enum(&kind)?})).collect::<StoreResult<Vec<_>>>()?;
            let role = task.node.recipe_id.as_str().to_owned();
            let horizon = task.node.execution_spec().horizon_name().map(str::to_owned);
            let business_ready = reason.is_none();
            let step_eligible = business_ready && session.status == DebugStatus::Paused;
            let blocked_reason = reason.or_else(|| {
                (!step_eligible).then(|| format!("debug_{:?}", session.status).to_lowercase())
            });
            // 用预算审计快照优先，否则从已持久化 AgentTurn 重建可确认的 token 用量。
            let budget = inspect_budget(&tx, &task, attempt_id)?;
            nodes.push(DebugNodeView {
                task,
                role,
                horizon,
                attempts,
                business_ready,
                step_eligible,
                retry_eligible: step_eligible && retry,
                blocked_reason,
                output_refs,
                budget,
            });
        }
        let events = tx.prepare("SELECT event_id,run_id,task_id,attempt_id,event_type,artifact_id,created_at FROM rebuild_events WHERE run_id=?1 AND (?2 IS NULL OR task_id=?2) AND (?3 IS NULL OR attempt_id=?3) ORDER BY event_id")?
            .query_map(params![run_id.0,task_id.map(|t|t.0.as_str()),attempt_id.map(|a|a.0.as_str())], stored_event_from_row)?
            .collect::<Result<Vec<_>,_>>()?;
        let mut ids = BTreeSet::new();
        let mut artifacts = Vec::new();
        let mut acceptance = Vec::new();
        // 事件 Artifact 按 ID 去重后解码；RawEvidence 被完全排除，其它无效 JSON 仅投影为 Null，
        // 再按已定义的键名/字符串模式递归脱敏（不是任意敏感内容检测）。
        // SQL/Artifact 读取失败仍会中止 inspect，而不是返回部分 artifacts。
        for id in events.iter().filter_map(|e| e.artifact_id.as_ref()) {
            if !ids.insert(id.clone()) {
                continue;
            }
            let artifact = read_artifact(&tx, id)?;
            if artifact.kind == ArtifactKind::RawEvidence {
                continue;
            }
            let bytes = blob::read_blob_bytes(&tx, &artifact.blob.hash, artifact.blob.bytes)?;
            let mut payload: serde_json::Value =
                serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
            redact(&mut payload);
            if artifact.producer == "debug.stage_acceptance" {
                acceptance.push(serde_json::from_value(payload.clone())?);
            }
            artifacts.push(DebugArtifactView { artifact, payload });
        }
        // 可用动作先由 session 状态产生，再按 retired/research-only policy 做权限收缩。
        let allowed_actions = match session.status {
            DebugStatus::Paused => vec!["resume", "step", "retry_node", "abort"],
            DebugStatus::Running | DebugStatus::Stepping => vec!["pause"],
            _ => vec![],
        }
        .into_iter()
        .filter(|action| {
            !retired && (*action != "resume" || !session.identity.research_only_without_policy())
        })
        .map(str::to_owned)
        .collect();
        // Execution evidence 是检查投影：PositionPlan 不包含执行阶段；Paper 只据节点与已持久 verdict 区分状态。
        let execution_evidence = if session.identity.run_purpose == RunPurpose::PositionPlan {
            "not_applicable"
        } else if let Some(node) = nodes.iter().find(|n| n.role == "gate.execution") {
            match node.task.status {
                TaskStatus::Running => "refreshing",
                TaskStatus::Succeeded => {
                    if artifacts.iter().any(|a| {
                        a.artifact.kind == ArtifactKind::ExecutionVerdict
                            && a.artifact.origin.as_ref().and_then(|o| o.task_id.as_ref())
                                == Some(&node.task.node.task_id)
                            && a.payload["verdict"] == "accepted"
                    }) {
                        "pass"
                    } else {
                        "blocked"
                    }
                }
                TaskStatus::Failed => "blocked",
                _ if !node.attempts.is_empty() => "blocked",
                _ => "deferred",
            }
        } else {
            "not_applicable"
        }
        .to_owned();
        let tasks = nodes
            .iter()
            .map(|n| serde_json::to_value(&n.task))
            .collect::<Result<Vec<_>, _>>()?;
        let committed = nodes
            .iter()
            .flat_map(|n| n.output_refs.iter().map(|r| r.artifact_id.clone()))
            .collect();
        // 局部 Task/Attempt 查询不声称完整研究进度，research 置 Null；全 Run 才把已提交输出交给共享投影函数。
        let research = if task_id.is_some() || attempt_id.is_some() {
            serde_json::Value::Null
        } else {
            super::research_review::research_progress(
                &tasks,
                &artifacts
                    .iter()
                    .map(|a| (&a.artifact, &a.payload))
                    .collect::<Vec<_>>(),
                &committed,
                session.identity.research_only_without_policy(),
            )
        };
        Ok(DebugRunView {
            inspection: self.inspect_run_with_connection(&tx, run_id)?,
            research,
            execution_evidence,
            session,
            workflow_status: snapshot.status,
            nodes,
            events,
            artifacts,
            acceptance,
            observed_at: now,
            allowed_actions,
        })
    }

    /// Observation only: Agent recovery continues to use AgentTurn and Tool records.
    // 输入当前 permit、运行时预算快照和时间；Debug session 存在时将观测作为 DebugRecord/event 原子追加。
    // 该快照只供 inspect 展示，实际恢复/预算权威仍从 AgentTurn/Tool lifecycle 重建。
    pub fn observe_debug_budget(
        &self,
        permit: &TaskWritePermit,
        snapshot: &serde_json::Value,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        // 非 Debug Run 是明确 no-op；不会为了记录观察而创建控制 identity。
        if read_session(&tx, &permit.run_id)?.is_none() {
            return Ok(());
        }
        assert_permit(&tx, permit)?;
        let artifact = detail_artifact(
            &tx,
            snapshot,
            "debug.budget_snapshot",
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            vec![],
            now,
        )?;
        insert_artifact(&tx, &artifact)?;
        append_event(
            &tx,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::DebugBudgetObserved,
            Some(&artifact.artifact_id),
            now,
        )?;
        tx.commit()?;
        Ok(())
    }

    // 输入 StageAcceptance，先用 Run/Task/Attempt 联合键确认真实 Attempt，再由 write_acceptance 校验内容、
    // 写 RunScoped Artifact 和幂等 event；最终 commit 后返回引用，而非“验收通过”的布尔结论。
    pub fn record_stage_acceptance(
        &self,
        acceptance: &StageAcceptance,
    ) -> StoreResult<ArtifactRef> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let exists: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM rebuild_attempts WHERE run_id=?1 AND task_id=?2 AND attempt_id=?3)",
            params![acceptance.run_id.0,acceptance.task_id.0,acceptance.attempt_id.0], |r|r.get(0))?;
        if !exists {
            return Err(blocked("acceptance_attempt_lineage_mismatch"));
        }
        let reference = write_acceptance(&tx, acceptance)?;
        tx.commit()?;
        Ok(reference)
    }
}

// 从同一连接读取 debug_environment metadata，供 claim/export/learning boundary 共用。
// 只有 metadata 行存在时返回 Some(identity)；查询是只读且不自动写默认标记。
pub(super) fn environment_identity(connection: &Connection) -> StoreResult<Option<String>> {
    Ok(connection
        .query_row(
            "SELECT value FROM rebuild_metadata WHERE key='debug_environment'",
            [],
            |r| r.get(0),
        )
        .optional()?)
}

// 新 DebugSession 必须匹配隔离 Store、Run purpose、contract 集合和 runtime identity。
// `contracts` 从 workflow nodes 的 Some(contract_hash) 收集并去重；它必须与 identity 声明集合完全相等。
fn validate_identity(
    tx: &Transaction<'_>,
    identity: &DebugSessionIdentity,
    workflow: &WorkflowCommit,
) -> StoreResult<()> {
    if environment_identity(tx)?.as_deref() != Some(identity.store_identity.as_str())
        || identity.run_id != workflow.run.run_id
        || identity.run_purpose != workflow.run.purpose
        || identity.learning_scope != DebugLearningScope::Isolated
        || identity.version != 1
        || identity.code_revision.trim().is_empty()
    {
        return Err(blocked("invalid_isolated_identity"));
    }
    let contracts = workflow
        .nodes
        .iter()
        .filter_map(|n| n.contract_hash.clone())
        .collect::<BTreeSet<_>>();
    if contracts != identity.contract_hashes.iter().cloned().collect() {
        return Err(blocked("contract_identity_mismatch"));
    }
    Ok(())
}

// 把 session identity 作为 DebugRecord/CAS 和 run_control head 原子发布，并验证 parent lineage。
// 调用方已持有 Transaction；Artifact、控制 head 和 DebugControlChanged event 共同受外层提交/回滚控制。
fn insert_session(tx: &Transaction<'_>, identity: &DebugSessionIdentity) -> StoreResult<()> {
    let mut sources = identity.dataset.clone();
    sources.extend(identity.parent_artifacts.iter().cloned());
    sources.sort();
    sources.dedup();
    let artifact = detail_artifact(
        tx,
        identity,
        "debug.session_identity",
        &identity.run_id,
        None,
        None,
        sources,
        identity.created_at,
    )?;
    insert_artifact(tx, &artifact)?;
    // ON CONFLICT 仅允许空 identity、revision=0 的初始行被占有；受影响行数不是 1 即表示已有控制权。
    let inserted = tx.execute("INSERT INTO rebuild_run_controls(run_id,identity_artifact_id,runtime_identity,revision,status,execution_mode,updated_at) VALUES(?1,?2,?4,0,'paused','manual',?3) ON CONFLICT(run_id) DO UPDATE SET identity_artifact_id=excluded.identity_artifact_id,runtime_identity=excluded.runtime_identity,status='paused',execution_mode='manual',updated_at=excluded.updated_at WHERE rebuild_run_controls.identity_artifact_id IS NULL AND rebuild_run_controls.revision=0",
        params![identity.run_id.0,artifact.artifact_id.0.as_str(),identity.created_at.to_rfc3339(),identity.runtime_identity.as_str()])?;
    if inserted != 1 {
        return Err(blocked("run_control_already_owned"));
    }
    // The same exact lineage rule is used at publication and by Doctor. This
    // records experiment provenance; it grants no Context access to the parent.
    // 先复核 identity Artifact 的全部 source_refs，再逐个检查 dataset Need 的来源闭包；
    // cross_run_reference_allowed 仅证明 provenance，不授权 Agent 读取父 Run 内容。
    for reference in &artifact.source_refs {
        let parent = read_artifact(tx, &reference.artifact_id)?;
        if parent.origin.as_ref().and_then(|o| o.run_id.as_ref()) != Some(&identity.run_id)
            && !cross_run_reference_allowed(tx, &artifact, &parent)?
        {
            return Err(blocked("invalid_debug_parent_lineage"));
        }
    }
    for reference in &identity.dataset {
        let need = read_artifact(tx, &reference.artifact_id)?;
        for source in &need.source_refs {
            let parent = read_artifact(tx, &source.artifact_id)?;
            if parent.origin.as_ref().and_then(|o| o.run_id.as_ref()) != Some(&identity.run_id)
                && !cross_run_reference_allowed(tx, &need, &parent)?
            {
                return Err(blocked("invalid_debug_dataset_lineage"));
            }
        }
    }
    append_event(
        tx,
        &identity.run_id,
        None,
        None,
        LifecycleEventType::DebugControlChanged,
        Some(&artifact.artifact_id),
        identity.created_at,
    )?;
    Ok(())
}

/// An explicit experiment edge is provenance, never an Agent read grant.
// 仅为登记的 Debug dataset/parent artifact 或已证明的 WorkflowGraph 放行有限跨 Run provenance。
// 返回 bool 而不是抛授权错误：不匹配是 Ok(false)，SQL/Artifact 错误传播为 Err；整个判断只借用连接和对象。
pub(super) fn cross_run_reference_allowed(
    connection: &Connection,
    child: &Artifact,
    parent: &Artifact,
) -> StoreResult<bool> {
    let Some(origin) = child.origin.as_ref() else {
        return Ok(false);
    };
    let Some(run) = origin.run_id.as_ref() else {
        return Ok(false);
    };
    if origin.task_id.is_some() || origin.attempt_id.is_some() {
        return Ok(false);
    }
    let Some(session) = read_session(connection, run)? else {
        return Ok(false);
    };
    let identity = session.identity;
    // WorkflowGraph CAS is shared and has no origin. Its owning Run is proved
    // by rebuild_runs.graph_artifact_id below, never guessed from content.
    // parent Run 优先取真实 ArtifactOrigin；只有无 origin 的 WorkflowGraph 才退回 identity 中明确声明的父 Run。
    let parent_run = parent
        .origin
        .as_ref()
        .and_then(|o| o.run_id.as_ref())
        .or_else(|| {
            (parent.kind == ArtifactKind::WorkflowGraph)
                .then_some(identity.parent_run_id.as_ref())
                .flatten()
        });
    let Some(parent_run) = parent_run else {
        return Ok(false);
    };
    let Some(source) = read_session(connection, parent_run)? else {
        return Ok(false);
    };
    if run == parent_run {
        return Ok(false);
    }
    if identity.parent_run_id.as_ref() != Some(parent_run)
        || identity.learning_scope != DebugLearningScope::Isolated
        || identity.store_identity != source.identity.store_identity
        || environment_identity(connection)?.as_ref() != Some(&identity.store_identity)
    {
        return Ok(false);
    }
    let reference = ArtifactRef {
        artifact_id: parent.artifact_id.clone(),
        kind: parent.kind,
    };
    if child.kind == ArtifactKind::EvidenceNeed {
        // 受控 EvidenceNeed 必须同 producer、BLOB、provenance 绑定，且 parent/child 都已列入各自冻结 dataset。
        return Ok(parent.kind == ArtifactKind::EvidenceNeed
            && child.producer == "scheduler.paper_snapshot"
            && parent.producer == child.producer
            && child.blob == parent.blob
            && child.provenance == parent.provenance
            && child.source_refs == vec![reference.clone()]
            && source.identity.dataset.contains(&reference)
            && identity.dataset.contains(&ArtifactRef {
                artifact_id: child.artifact_id.clone(),
                kind: child.kind,
            }));
    }
    if child.kind != ArtifactKind::DebugRecord
        || child.producer != "debug.session_identity"
        || child.provenance.source_family != "akzio.debug"
        || !identity.parent_artifacts.contains(&reference)
    {
        return Ok(false);
    }
    let registered: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM rebuild_run_controls WHERE run_id=?1 AND identity_artifact_id=?2)",
        params![run.0,child.artifact_id.0.as_str()],|r|r.get(0))?;
    if !registered {
        return Ok(false);
    }
    // parent_task_id 存在时要求该 Artifact 是父 Run/指定 Task 的成功 Attempt output；
    // None 仅允许用 rebuild_runs.graph_artifact_id 证明父 WorkflowGraph 所属关系。
    match identity.parent_task_id {
        Some(task) => Ok(connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM rebuild_attempt_outputs o JOIN rebuild_attempts a ON a.attempt_id=o.attempt_id JOIN rebuild_tasks t ON t.task_id=a.task_id WHERE a.run_id=?1 AND a.task_id=?2 AND a.status='succeeded' AND t.status='succeeded' AND o.artifact_id=?3)",
            params![parent_run.0,task.0,parent.artifact_id.0.as_str()],|r|r.get(0))?),
        None => Ok(parent.kind == ArtifactKind::WorkflowGraph && connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM rebuild_runs WHERE run_id=?1 AND graph_artifact_id=?2)",
            params![parent_run.0,parent.artifact_id.0.as_str()],|r|r.get::<_,bool>(0))?),
    }
}

// 读取 control head 指向的 identity Artifact，并核对 runtime_identity/run_id 不分叉。
// SQL 以 run_id 和非空 identity 限定一行；map 闭包将行解码为 Result，transpose 保留无行与损坏两种结果。
pub(super) fn read_session(
    connection: &Connection,
    run_id: &RunId,
) -> StoreResult<Option<DebugSession>> {
    let row=connection.query_row("SELECT identity_artifact_id,revision,status,execution_mode,permitted_task_id,active_attempt_id,paused_at_task_id,updated_at,runtime_identity FROM rebuild_run_controls WHERE run_id=?1 AND identity_artifact_id IS NOT NULL",params![run_id.0],|r|Ok((r.get::<_,String>(0)?,r.get::<_,u64>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?,r.get::<_,Option<String>>(4)?,r.get::<_,Option<String>>(5)?,r.get::<_,Option<String>>(6)?,r.get::<_,String>(7)?,r.get::<_,String>(8)?))).optional()?;
    row.map(
        |(id, revision, status, mode, task, attempt, paused, updated, runtime)| {
            let artifact = read_artifact(connection, &ArtifactId(ContentHash::new(id)?))?;
            let identity: DebugSessionIdentity = serde_json::from_slice(&blob::read_blob_bytes(
                connection,
                &artifact.blob.hash,
                artifact.blob.bytes,
            )?)?;
            if identity.runtime_identity.as_str() != runtime || identity.run_id != *run_id {
                return Err(StoreError::Integrity(
                    "Debug identity head differs from CAS identity".into(),
                ));
            }
            Ok(DebugSession {
                identity,
                revision,
                status: parse_enum(&status)?,
                execution_mode: parse_enum(&mode)?,
                permitted_task_id: task.map(TaskId),
                active_attempt_id: attempt.map(AttemptId),
                paused_at_task_id: paused.map(TaskId),
                updated_at: parse_time(&updated)?,
            })
        },
    )
    .transpose()
}

// 以旧 revision 为 WHERE 条件更新 control head，再追加 DebugControlChanged Artifact/event；
// 修改的是传入 session 的本地副本，外层事务提交前其他连接不可见，CAS 冲突由 changed!=1 转成阻断。
fn save_session(
    tx: &Transaction<'_>,
    session: &mut DebugSession,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    let old = session.revision;
    session.revision += 1;
    session.updated_at = now;
    let changed=tx.execute("UPDATE rebuild_run_controls SET revision=?1,status=?2,execution_mode=?3,permitted_task_id=?4,active_attempt_id=?5,paused_at_task_id=?6,updated_at=?7 WHERE run_id=?8 AND revision=?9",params![session.revision,enum_name(session.status),enum_name(session.execution_mode),session.permitted_task_id.as_ref().map(|x|x.0.as_str()),session.active_attempt_id.as_ref().map(|x|x.0.as_str()),session.paused_at_task_id.as_ref().map(|x|x.0.as_str()),now.to_rfc3339(),session.identity.run_id.0,old])?;
    if changed != 1 {
        return Err(blocked("revision_conflict"));
    }
    let artifact = detail_artifact(
        tx,
        session,
        "debug.control",
        &session.identity.run_id,
        None,
        None,
        vec![],
        now,
    )?;
    insert_artifact(tx, &artifact)?;
    append_event(
        tx,
        &session.identity.run_id,
        None,
        None,
        LifecycleEventType::DebugControlChanged,
        Some(&artifact.artifact_id),
        now,
    )?;
    Ok(())
}

// 统计 Run 当前 running Task，供 pause/abort/step 状态转换使用。
// Run ID 是唯一过滤参数；聚合 COUNT 返回当前连接看到的 running 行数。
fn running_count(connection: &Connection, run_id: &RunId) -> StoreResult<u64> {
    Ok(connection.query_row(
        "SELECT count(*) FROM rebuild_tasks WHERE run_id=?1 AND status='running'",
        params![run_id.0],
        |r| r.get(0),
    )?)
}

// 输入精确 Run/Task 和判定时间，依次检查 Task queued 状态、research-only Decision 限制、依赖、
// Run 可运行状态、ready_at 与取消请求；返回第一个阻断原因，None 才表示这些 Store 条件均通过。
fn task_blocked_reason(
    connection: &Connection,
    run_id: &RunId,
    task_id: &TaskId,
    now: DateTime<Utc>,
) -> StoreResult<Option<String>> {
    let row=connection.query_row("SELECT t.status,t.ready_at,r.status,t.recipe_id FROM rebuild_tasks t JOIN rebuild_runs r ON r.run_id=t.run_id WHERE t.run_id=?1 AND t.task_id=?2",params![run_id.0,task_id.0],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?))).optional()?.ok_or_else(||StoreError::MissingTask(task_id.clone()))?;
    if row.0 != "queued" {
        return Ok(Some(format!("task_{}", row.0)));
    }
    if row.3 == "gate.decision"
        && read_session(connection, run_id)?
            .is_some_and(|session| session.identity.research_only_without_policy())
    {
        return Ok(Some(
            "research_only_incomplete: DecisionPolicy missing; Decision forbidden".into(),
        ));
    }
    // 依赖表用 task_id 过滤待检查节点，并通过 depends_on_task_id 关联父 Task；只有非 succeeded/skipped 被计为未满足。
    let dependencies:u64=connection.query_row("SELECT count(*) FROM rebuild_task_dependencies d JOIN rebuild_tasks p ON p.task_id=d.depends_on_task_id WHERE d.task_id=?1 AND p.status NOT IN ('succeeded','skipped')",params![task_id.0],|r|r.get(0))?;
    if dependencies > 0 {
        return Ok(Some("dependencies_not_satisfied".into()));
    }
    let run_runnable = matches!(row.2.as_str(), "queued" | "running")
        || (row.2 == "completed" && row.3 == POST_TERMINAL_WORKER_RECIPE_ID);
    if !run_runnable {
        return Ok(Some(format!("run_{}", row.2)));
    }
    if parse_time(&row.1)? > now {
        return Ok(Some(format!("not_due_until:{}", row.1)));
    }
    let cancelled: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM rebuild_run_cancellations WHERE run_id=?1)",
        params![run_id.0],
        |r| r.get(0),
    )?;
    Ok(cancelled.then(|| "run_cancel_requested".into()))
}

// 按 task_id 取 epoch 最大的一条 Attempt；无 Attempt 返回 false，只有 retried/abandoned 可由 Debug 重试。
fn retry_eligible(connection: &Connection, task_id: &TaskId) -> StoreResult<bool> {
    let last = connection
        .query_row(
            "SELECT status FROM rebuild_attempts WHERE task_id=?1 ORDER BY epoch DESC LIMIT 1",
            params![task_id.0],
            |r| r.get::<_, String>(0),
        )
        .optional()?;
    Ok(last.is_some_and(|s| matches!(s.as_str(), "retried" | "abandoned")))
}

// 输入已重建 Task 和可选 Attempt ID；优先读最后一条预算观测，否则从该 Task/Attempt 的 AgentTurn
// telemetry 汇总。缺失类别保持 unknown，不把 task lifetime 计数冒充 Outcome 当前 horizon 的剩余额度。
fn inspect_budget(
    connection: &Connection,
    task: &StoredTaskSnapshot,
    attempt_id: Option<&AttemptId>,
) -> StoreResult<serde_json::Value> {
    let latest:Option<String>=connection.query_row("SELECT artifact_id FROM rebuild_events WHERE task_id=?1 AND (?2 IS NULL OR attempt_id=?2) AND event_type='debug.budget_observed' ORDER BY event_id DESC LIMIT 1",params![task.node.task_id.0, attempt_id.map(|id| id.0.as_str())],|r|r.get(0)).optional()?;
    if let Some(id) = latest {
        let artifact = read_artifact(connection, &ArtifactId(ContentHash::new(id)?))?;
        let payload: serde_json::Value = serde_json::from_slice(&blob::read_blob_bytes(
            connection,
            &artifact.blob.hash,
            artifact.blob.bytes,
        )?)?;
        return Ok(
            serde_json::json!({"resolved":task.node.budget,"limits":task.node.budget,"last_runtime_observation":payload,"observed_at":artifact.created_at,"attempt":artifact.origin.and_then(|o|o.attempt_id),"authority":"AgentRuntime audit snapshot; recovery still derives from AgentTurn and Tool lifecycle"}),
        );
    }
    // AgentTurn 结果按 task_id/可选 attempt_id 与结束事件过滤并 DISTINCT；完整性要求 usage 字段齐全，
    // 且已结束 turn 数与 agent.turn_started 事件数相等。
    let ids=connection.prepare("SELECT DISTINCT artifact_id FROM rebuild_events WHERE task_id=?1 AND (?2 IS NULL OR attempt_id=?2) AND event_type IN ('agent.turn_completed','agent.turn_failed','agent.turn_retryable_failed') AND artifact_id IS NOT NULL")?
        .query_map(params![task.node.task_id.0, attempt_id.map(|id| id.0.as_str())],|r|r.get::<_,String>(0))?.collect::<Result<Vec<_>,_>>()?;
    let mut input = 0u64;
    let mut output = 0u64;
    let mut latency = 0u64;
    let mut complete = true;
    for id in &ids {
        let artifact = read_artifact(connection, &ArtifactId(ContentHash::new(id)?))?;
        let value: serde_json::Value = serde_json::from_slice(&blob::read_blob_bytes(
            connection,
            &artifact.blob.hash,
            artifact.blob.bytes,
        )?)?;
        if let (Some(i), Some(o), Some(l)) = (
            value
                .pointer("/response/telemetry/input_tokens")
                .and_then(serde_json::Value::as_u64),
            value
                .pointer("/response/telemetry/output_tokens")
                .and_then(serde_json::Value::as_u64),
            value
                .pointer("/response/telemetry/latency_millis")
                .and_then(serde_json::Value::as_u64),
        ) {
            input = input.saturating_add(i);
            output = output.saturating_add(o);
            latency = latency.saturating_add(l);
        } else {
            complete = false;
        }
    }
    let calls: u64 = connection.query_row(
        "SELECT count(*) FROM rebuild_events WHERE task_id=?1 AND (?2 IS NULL OR attempt_id=?2) AND event_type='agent.turn_started'",
        params![task.node.task_id.0, attempt_id.map(|id| id.0.as_str())],
        |r| r.get(0),
    )?;
    let tools: u64 = connection.query_row(
        "SELECT count(*) FROM rebuild_events WHERE task_id=?1 AND (?2 IS NULL OR attempt_id=?2) AND event_type='tool.called'",
        params![task.node.task_id.0, attempt_id.map(|id| id.0.as_str())],
        |r| r.get(0),
    )?;
    complete = complete && calls == ids.len() as u64;
    // Outcome budgets reset at committed horizon boundaries, so whole-task
    // lifetime usage is not presented as a current-horizon remainder.
    let remainder_known = calls == 0 && tools == 0;
    Ok(
        serde_json::json!({"resolved":task.node.budget,"limits":task.node.budget,"scope":"task_lifetime_observed_provider_usage","usage_complete":complete,
        "input_tokens_used":complete.then_some(input),"output_tokens_used":complete.then_some(output),"provider_latency_millis":complete.then_some(latency),
        "input_tokens_remaining":remainder_known.then_some(u64::from(task.node.budget.max_input_tokens).saturating_sub(input)),
        "output_tokens_remaining":remainder_known.then_some(u64::from(task.node.budget.max_output_tokens).saturating_sub(output)),
        "tool_calls_used":tools,"provider_calls_started":calls,"tool_calls_remaining":remainder_known.then(|| task.node.budget.max_tool_calls.remaining(tools)).flatten(),
        "authority":"AgentRuntime; missing usage and post-terminal stage remainder remain unknown"}),
    )
}

// 在调用方 claim Transaction 中消费指定 permit；Stepping 必须 task/attempt 精确匹配后清除一次性授权，
// Continuous 只接受 Running，不新增单独 permit 行；非 Debug Run 没有 session 时无操作。
pub(super) fn consume_claim(
    tx: &Transaction<'_>,
    permit: &TaskWritePermit,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    if let Some(mut session) = read_session(tx, &permit.run_id)? {
        if session.status == DebugStatus::Stepping {
            if session.permitted_task_id.as_ref() != Some(&permit.task_id)
                || session.active_attempt_id.is_some()
            {
                return Err(blocked("step_permit_not_available"));
            }
            session.permitted_task_id = None;
            session.active_attempt_id = Some(permit.attempt_id.clone());
            save_session(tx, &mut session, now)?;
        } else if session.status != DebugStatus::Running {
            return Err(blocked("run_is_paused"));
        }
    }
    Ok(())
}

/// Called in every attempt-closing transaction, including defer and recovery.
// 输入关闭中的 permit、结果说明与时钟；非 Debug 委托普通 run_control settle，Debug 则同事务写控制验收，
// 并在当前 Stepping/暂停请求或 Continuous 队列 drained 条件满足时更新 session。
pub(super) fn settle_attempt(
    tx: &Transaction<'_>,
    permit: &TaskWritePermit,
    result: &str,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    let Some(mut session) = read_session(tx, &permit.run_id)? else {
        return super::run_control::settle_continuous(tx, &permit.run_id, now);
    };
    let acceptance = StageAcceptance {
        version: 1,
        run_id: permit.run_id.clone(),
        task_id: permit.task_id.clone(),
        attempt_id: permit.attempt_id.clone(),
        stage: tx.query_row(
            "SELECT recipe_id FROM rebuild_tasks WHERE task_id=?1",
            params![permit.task_id.0],
            |r| r.get(0),
        )?,
        business_result: result.into(),
        test_result: AcceptanceResult::NotRun,
        checks: vec![AcceptanceCheck {
            check_id: "controller.attempt_boundary".into(),
            category: AcceptanceCategory::Persistence,
            expected: "attempt closes within the Store transaction".into(),
            actual: result.into(),
            result: AcceptanceResult::Pass,
            evidence_refs: vec![],
            message: "Controller boundary verified; business acceptance checks have not been run."
                .into(),
        }],
        created_at: now,
    };
    write_acceptance(tx, &acceptance)?;
    // 单步完成或已排空的 PauseRequested 转为 Paused；Continuous 仅在运行与 queued Task 均为零时 Completed。
    if (session.status == DebugStatus::Stepping
        && session.active_attempt_id.as_ref() == Some(&permit.attempt_id))
        || session.status == DebugStatus::PauseRequested && running_count(tx, &permit.run_id)? == 0
    {
        session.status = DebugStatus::Paused;
        session.permitted_task_id = None;
        session.active_attempt_id = None;
        session.paused_at_task_id = Some(permit.task_id.clone());
        save_session(tx, &mut session, now)?;
    } else if session.status == DebugStatus::Running && running_count(tx, &permit.run_id)? == 0 {
        let pending: u64 = tx.query_row(
            "SELECT count(*) FROM rebuild_tasks WHERE run_id=?1 AND status='queued'",
            params![permit.run_id.0],
            |r| r.get(0),
        )?;
        if pending == 0 {
            session.status = DebugStatus::Completed;
            save_session(tx, &mut session, now)?;
        }
    }
    Ok(())
}

// 实际 effect-intent 边界再次检查 Debug Broker policy 和隔离 Store，不能只依赖 UI/预检查。
// 已登记 Debug session 只有 PaperAllowed 才通过；被标记隔离但缺 session 的 Run 一律拒绝。
pub(super) fn assert_broker_write(connection: &Connection, run_id: &RunId) -> StoreResult<()> {
    let session = read_session(connection, run_id)?;
    if session
        .as_ref()
        .is_some_and(|s| s.identity.broker_write_policy != DebugBrokerPolicy::PaperAllowed)
        || environment_identity(connection)?.is_some() && session.is_none()
    {
        return Err(StoreError::DebugBrokerWriteForbidden);
    }
    Ok(())
}

// 在 Outcome enqueue 的外层事务中唤醒通用 continuous control；Completed Debug session 随 execution_mode
// 恢复为 Running 或 Paused。这里只重开调度控制，不触碰 Outcome 任务本身的结果/资格。
pub(super) fn post_terminal_enqueued(
    tx: &Transaction<'_>,
    run_id: &RunId,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    super::run_control::wake_continuous(tx, run_id, now)?;
    if let Some(mut session) = read_session(tx, run_id)? {
        if session.status == DebugStatus::Completed {
            session.status = if session.execution_mode == DebugExecutionMode::Continuous {
                DebugStatus::Running
            } else {
                DebugStatus::Paused
            };
            save_session(tx, &mut session, now)?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
// 生成 RunScoped DebugRecord staging payload，调用方再在事务内 insert 并追加 event。
// T: Serialize 由调用方静态提供；payload 借用序列化成 bytes，连接参数用于同事务 TEMP staging，
// Artifact 的 origin/source_refs 将 provenance 固定到指定 Run/Task/Attempt。
fn detail_artifact<T: Serialize>(
    connection: &Connection,
    payload: &T,
    producer: &str,
    run_id: &RunId,
    task_id: Option<&TaskId>,
    attempt_id: Option<&AttemptId>,
    sources: Vec<ArtifactRef>,
    now: DateTime<Utc>,
) -> StoreResult<Artifact> {
    Ok(Artifact::new(
        ArtifactKind::DebugRecord,
        blob::stage_blob_bytes(
            connection,
            &serde_json::to_vec(payload)?,
            "application/json".into(),
        )?,
        producer,
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.debug".into(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        },
        Some(ArtifactOrigin {
            run_id: Some(run_id.clone()),
            task_id: task_id.cloned(),
            attempt_id: attempt_id.cloned(),
            contract_hash: None,
        }),
        sources,
        now,
    )?)
}

// acceptance 由 checks 的 evidence refs 组成去重 source closure，并按 Artifact hash 幂等追加 event；
// caller 的 Transaction 决定 Artifact、staging promotion 和事件是否一起提交。
fn write_acceptance(tx: &Transaction<'_>, value: &StageAcceptance) -> StoreResult<ArtifactRef> {
    if value.version != 1
        || value.stage.is_empty()
        || value.checks.iter().any(|c| c.check_id.is_empty())
    {
        return Err(blocked("invalid_stage_acceptance"));
    }
    let sources = value
        .checks
        .iter()
        .flat_map(|c| c.evidence_refs.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let artifact = detail_artifact(
        tx,
        value,
        "debug.stage_acceptance",
        &value.run_id,
        Some(&value.task_id),
        Some(&value.attempt_id),
        sources,
        value.created_at,
    )?;
    insert_artifact(tx, &artifact)?;
    // Artifact 可重复 insert；只有完全相同内容尚无 acceptance event 时才追加一次事件。
    let exists:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM rebuild_events WHERE event_type='debug.acceptance_recorded' AND artifact_id=?1)",params![artifact.artifact_id.0.as_str()],|r|r.get(0))?;
    if !exists {
        append_event(
            tx,
            &value.run_id,
            Some(&value.task_id),
            Some(&value.attempt_id),
            LifecycleEventType::StageAcceptanceRecorded,
            Some(&artifact.artifact_id),
            value.created_at,
        )?;
    }
    Ok(ArtifactRef {
        artifact_id: artifact.artifact_id,
        kind: artifact.kind,
    })
}

/// Exact credential field names, independent of token-usage field names.
#[allow(clippy::collapsible_match)]
// 按枚举键名及字符串特征递归替换凭据字段/credential-bearing text，保留 token usage 和普通风险说明；
// 此模式匹配不保证识别任意形式的凭据，不能单靠该函数定义完整导出授权。
// 输入 JSON 的可变借用；对象/数组分支递归借用子值，敏感键直接覆写为 marker，标量其他类型原样保留。
fn redact(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, value) in map {
                let key = key.to_ascii_lowercase();
                // key 临时转换为小写 String 后匹配精确名称或带下划线前缀的敏感字段。
                if [
                    "api_key",
                    "apikey",
                    "authorization",
                    "auth_header",
                    "secret",
                    "password",
                    "credential",
                    "access_token",
                    "refresh_token",
                    "token",
                    "headers",
                    "encrypted_content",
                ]
                .iter()
                .any(|s| key == *s || key.ends_with(&format!("_{s}")))
                {
                    *value = serde_json::Value::String("[REDACTED]".into());
                } else {
                    redact(value);
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                redact(value);
            }
        }
        serde_json::Value::String(text) => {
            // Never project provider debug/raw payloads containing credential text.
            if [
                "Bearer ",
                " sk-",
                "\"sk-",
                "APCA-API-SECRET-KEY",
                "api_key=",
                "api_secret=",
            ]
            .iter()
            .any(|s| text.contains(s))
                || text.starts_with("sk-")
            {
                *text = "[REDACTED credential-bearing text]".into();
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    // 只脱敏凭据和加密内容，usage/risk prose 必须保留可审计值。
    fn inspect_redacts_credentials_but_retains_usage_and_risk_prose() {
        let mut value = serde_json::json!({"api_key":"secret","nested":{"Authorization":"Bearer secret","input_tokens":321,"output_tokens":12,"encrypted_content":"opaque"},"memo":"risk-aware research","error":"Bearer secret"});
        redact(&mut value);
        assert_eq!(value["api_key"], "[REDACTED]");
        assert_eq!(value["nested"]["Authorization"], "[REDACTED]");
        assert_eq!(value["nested"]["encrypted_content"], "[REDACTED]");
        assert_eq!(value["nested"]["input_tokens"], 321);
        assert_eq!(value["memo"], "risk-aware research");
        assert!(!value["error"].as_str().unwrap().contains("secret"));
    }
}
