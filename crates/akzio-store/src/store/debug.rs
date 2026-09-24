//! Read-only historical Debug identity and inspection; no Debug execution authority.
// 历史 Debug 身份、控制 head、事件和验收 CAS 仅供解码与只读 inspect；新 Debug
// Session、step、fork 和控制写入均已退役。正式 task 只使用通用 RunControl。
use super::trajectory::stored_event_from_row;
use super::*;
use akzio_domain::{
    AcceptanceResult, DebugLearningScope, DebugSession, DebugSessionIdentity, StageAcceptance,
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
    // 只读取 metadata 中的隔离 identity，不创建或修复环境标记；无行是 Ok(None)，SQL 错误仍传播。
    pub fn debug_environment(&self) -> StoreResult<Option<String>> {
        environment_identity(&*self.connection()?)
    }

    // 从 run_control head 指向的 CAS identity Artifact 重建 DebugSession；只读，不修复过期 head。
    pub fn debug_session(&self, run_id: &RunId) -> StoreResult<Option<DebugSession>> {
        read_session(&*self.connection()?, run_id)
    }

    /// Defense at the actual effect-intent boundary, including cancel/reprice.
    // 在实际 Broker effect intent 调用边界重读 Store policy；不是 UI 预检查，也不会发出网络请求。
    pub fn assert_broker_write_policy(&self, run_id: &RunId) -> StoreResult<()> {
        assert_broker_write(&*self.connection()?, run_id)
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
            // 输出通过 attempt_outputs→artifacts→attempts 关联并限制到当前 Task；Event 过滤范围则按可选 attempt_id 精确收窄。
            let output_refs = tx.prepare("SELECT a.artifact_id,a.kind FROM rebuild_attempt_outputs o JOIN rebuild_artifacts a ON a.artifact_id=o.artifact_id JOIN rebuild_attempts p ON p.attempt_id=o.attempt_id WHERE p.task_id=?1 ORDER BY o.event_id")?
                .query_map(params![task.node.task_id.0], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?
                .collect::<Result<Vec<_>, _>>()?.into_iter().map(|(id,kind)| Ok(ArtifactRef {artifact_id:ArtifactId(ContentHash::new(id)?),kind:parse_enum(&kind)?})).collect::<StoreResult<Vec<_>>>()?;
            let role = task.node.recipe_id.as_str().to_owned();
            let horizon = task.node.execution_spec().horizon_name().map(str::to_owned);
            let business_ready = false;
            let step_eligible = false;
            let blocked_reason = Some("debug_control_retired".to_owned());
            // 用预算审计快照优先，否则从已持久化 AgentTurn 重建可确认的 token 用量。
            let budget = inspect_budget(&tx, &task, attempt_id)?;
            nodes.push(DebugNodeView {
                task,
                role,
                horizon,
                attempts,
                business_ready,
                step_eligible,
                retry_eligible: false,
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
        let allowed_actions = Vec::new();
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

    /// Keep the existing frozen SubmitRejected lineage for Agent recovery.
    /// The former arbitrary Debug acceptance writer is no longer exposed.
    pub fn record_submit_rejection(
        &self,
        acceptance: &StageAcceptance,
    ) -> StoreResult<ArtifactRef> {
        if acceptance.business_result != "SubmitRejected"
            || acceptance.test_result != AcceptanceResult::Fail
            || acceptance.checks.len() != 1
            || acceptance.checks[0].check_id != "agent.submit_validation"
        {
            return Err(blocked("debug acceptance writes are retired"));
        }
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if read_session(&tx, &acceptance.run_id)?.is_some() {
            return Err(blocked("historical Debug Run cannot write new acceptance"));
        }
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

// Historical Debug identity cannot grant Broker write authority, regardless
// of the old broker_write_policy encoded in that identity.
pub(super) fn assert_broker_write(connection: &Connection, run_id: &RunId) -> StoreResult<()> {
    let old_control_identity: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM rebuild_run_controls \
         WHERE run_id=?1 AND identity_artifact_id IS NOT NULL)",
        params![run_id.0],
        |row| row.get(0),
    )?;
    if old_control_identity
        || environment_identity(connection)?.is_some()
        || run_purpose_from_connection(connection, run_id)? == RunPurpose::Debug
    {
        return Err(StoreError::DebugBrokerWriteForbidden);
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
