//! Shared scheduling head, durable recovery boundaries and read projections.
//! Checkpoints reference the event journal; they never replace its authority.
// 文件导读：RunControl 是 Debug/普通 Run 共用的调度 head，Checkpoint 只是带 graph、
// event cursor 和 control revision 的可验证边界；真正的任务事实仍以 rebuild_events/Task 为准。
// 写路径由 Workflow/Debug 事务调用 initialize/settle/checkpoint helpers；读路径由 recovery、
// Observer 和控制 API 调用 snapshot/inspection/event page，所有 helper 都复用调用方连接。
use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunControlView {
    pub revision: u64,
    pub status: akzio_domain::RunControlStatus,
    pub execution_mode: akzio_domain::RunExecutionMode,
    pub runtime_identity: Option<ContentHash>,
    pub permitted_task_id: Option<TaskId>,
    pub active_attempt_id: Option<AttemptId>,
    pub debug_identity: Option<ArtifactId>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunCheckpoint {
    pub version: u32,
    pub run_id: RunId,
    pub graph: ArtifactRef,
    pub event_cursor: i64,
    pub control_revision: u64,
    pub runtime_identity: Option<ContentHash>,
    pub task_id: Option<TaskId>,
    pub attempt_id: Option<AttemptId>,
    pub boundary: String,
    pub source: Option<ArtifactRef>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunEventView {
    pub cursor: i64,
    pub run_id: RunId,
    pub task_id: Option<TaskId>,
    pub attempt_id: Option<AttemptId>,
    pub event_type: String,
    pub artifact: Option<ArtifactRef>,
    pub source_refs: Vec<ArtifactRef>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunEventPage {
    pub events: Vec<RunEventView>,
    pub next_cursor: i64,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunInspection {
    pub version: u32,
    pub workflow: WorkflowSnapshot,
    pub blueprint: akzio_domain::WorkflowBlueprint,
    pub control: RunControlView,
    pub checkpoint: Option<RunCheckpoint>,
    pub recovery: String,
    pub allowed_actions: Vec<String>,
}

pub(super) fn initialize_control(
    tx: &Transaction<'_>,
    run: &RunId,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    // 输入新 Run ID 与创建时间；在调用方事务插入唯一初始 head。Debug session 后续通过
    // identity_artifact_id 接管，不在此处创建隔离身份。
    tx.execute("INSERT INTO rebuild_run_controls(run_id,revision,status,execution_mode,updated_at) VALUES(?1,0,'running','continuous',?2)", params![run.0, now.to_rfc3339()])?;
    Ok(())
}

pub(super) fn backfill_history(tx: &Transaction<'_>) -> StoreResult<()> {
    // 仅迁移时为缺 head 的 Run 补行；terminal Run 归为 completed，其它状态归 running，
    // INSERT OR IGNORE 保留已有 revision/status，且不独立提交事务。
    tx.execute_batch("INSERT OR IGNORE INTO rebuild_run_controls(run_id,revision,status,execution_mode,updated_at)
        SELECT run_id,0,CASE WHEN status IN ('completed','failed','cancelled') THEN 'completed' ELSE 'running' END,'continuous',created_at FROM rebuild_runs;")?;
    Ok(())
}

// 从唯一 control head 恢复 CAS identity、permit 指针和更新时间。
fn read_control(connection: &Connection, run: &RunId) -> StoreResult<RunControlView> {
    // SQL 以 run_id 查唯一 head；enum、hash 和时间解析失败继续作为 Err，只有确实无行才报 MissingRun。
    let row = connection.query_row("SELECT revision,status,execution_mode,runtime_identity,permitted_task_id,active_attempt_id,identity_artifact_id,updated_at FROM rebuild_run_controls WHERE run_id=?1", params![run.0], |r| Ok((r.get::<_,u64>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,Option<String>>(3)?,r.get::<_,Option<String>>(4)?,r.get::<_,Option<String>>(5)?,r.get::<_,Option<String>>(6)?,r.get::<_,String>(7)?))).optional()?.ok_or_else(|| StoreError::MissingRun(run.clone()))?;
    Ok(RunControlView {
        revision: row.0,
        status: parse_enum(&row.1)?,
        execution_mode: parse_enum(&row.2)?,
        runtime_identity: row.3.map(ContentHash::new).transpose()?,
        permitted_task_id: row.4.map(TaskId),
        active_attempt_id: row.5.map(AttemptId),
        debug_identity: row.6.map(ContentHash::new).transpose()?.map(ArtifactId),
        updated_at: parse_time(&row.7)?,
    })
}

pub(super) fn settle_continuous(
    tx: &Transaction<'_>,
    run: &RunId,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    // 普通 Run 在同一事务按 queued/leased/running Task 是否仍存在派生 status，再保存最新 boundary；
    // identity_artifact_id 非空的 Debug head 不被此 UPDATE 覆盖。
    tx.execute("UPDATE rebuild_run_controls SET revision=revision+1,updated_at=?2,status=CASE WHEN EXISTS(SELECT 1 FROM rebuild_tasks WHERE run_id=?1 AND status IN ('queued','leased','running')) THEN 'running' ELSE 'completed' END WHERE run_id=?1 AND identity_artifact_id IS NULL",params![run.0,now.to_rfc3339()])?;
    save_latest_boundary(tx, run, now)
}

pub(super) fn wake_continuous(
    tx: &Transaction<'_>,
    run: &RunId,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    // Outcome worker 入队时只把普通 Run 的 completed head 改为 running，并增加 revision；
    // 有 Debug identity 的 Run 不走此 UPDATE。
    tx.execute("UPDATE rebuild_run_controls SET status='running',revision=revision+1,updated_at=?2 WHERE run_id=?1 AND identity_artifact_id IS NULL AND status='completed'", params![run.0,now.to_rfc3339()])?;
    Ok(())
}

// 找到最近非 checkpoint 事件，把它封装成新的可验证 boundary。
fn save_latest_boundary(tx: &Transaction<'_>, run: &RunId, now: DateTime<Utc>) -> StoreResult<()> {
    // 忽略 checkpoint 自身的 event，选此 Run 最新业务事件；若没有可选事件，query_row 返回 Err，
    // 外层 Attempt/Run 事务随之回滚。
    let row = tx.query_row("SELECT event_id,event_type,task_id,attempt_id,artifact_id FROM rebuild_events WHERE run_id=?1 AND event_type!='runtime.checkpoint_saved' ORDER BY event_id DESC LIMIT 1",params![run.0],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?,r.get::<_,Option<String>>(2)?,r.get::<_,Option<String>>(3)?,r.get::<_,Option<String>>(4)?)))?;
    save_checkpoint(
        tx,
        run,
        row.0,
        &row.1,
        row.2.map(TaskId).as_ref(),
        row.3.map(AttemptId).as_ref(),
        row.4
            .map(ContentHash::new)
            .transpose()?
            .map(ArtifactId)
            .as_ref(),
        now,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn checkpoint_boundary(
    tx: &Transaction<'_>,
    run: &RunId,
    cursor: i64,
    event: LifecycleEventType,
    task: Option<&TaskId>,
    attempt: Option<&AttemptId>,
    source: Option<&ArtifactId>,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    // 只有指定的生命周期阶段边界触发保存；其余 event 类型返回 Ok(()) 且不写 Artifact。
    if matches!(
        event,
        LifecycleEventType::WorkflowCreated
            | LifecycleEventType::WorkflowPatched
            | LifecycleEventType::DebugControlChanged
            | LifecycleEventType::TaskStarted
            | LifecycleEventType::TaskSucceeded
            | LifecycleEventType::TaskFailed
            | LifecycleEventType::TaskSkipped
            | LifecycleEventType::TaskCancelled
            | LifecycleEventType::TaskRecovered
            | LifecycleEventType::TaskRecoveryExhausted
            | LifecycleEventType::TaskRetryScheduled
            | LifecycleEventType::TaskRetryExhausted
            | LifecycleEventType::TaskDeferred
            | LifecycleEventType::OutcomeWorkerEnqueued
            | LifecycleEventType::RunCancelRequested
    ) {
        save_checkpoint(tx, run, cursor, event.as_str(), task, attempt, source, now)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn save_checkpoint(
    tx: &Transaction<'_>,
    run: &RunId,
    cursor: i64,
    boundary: &str,
    task: Option<&TaskId>,
    attempt: Option<&AttemptId>,
    source: Option<&ArtifactId>,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    // 从当前 control head/Run graph 恢复版本，payload 先在调用方事务的同一连接 TEMP 表暂存，
    // `insert_artifact` 再提升 CAS、写 RunScoped Artifact，随后追加 checkpoint_saved event；
    // `append_event` 对这个事件不再触发新 checkpoint，避免无限递归。
    let control = read_control(tx, run)?;
    let graph_id: String = tx.query_row(
        "SELECT graph_artifact_id FROM rebuild_runs WHERE run_id=?1",
        params![run.0],
        |r| r.get(0),
    )?;
    let graph = ArtifactRef {
        artifact_id: ArtifactId(ContentHash::new(graph_id)?),
        kind: ArtifactKind::WorkflowGraph,
    };
    let source = source
        .map(|id| {
            read_artifact(tx, id).map(|a| ArtifactRef {
                artifact_id: a.artifact_id,
                kind: a.kind,
            })
        })
        .transpose()?;
    let checkpoint = RunCheckpoint {
        version: 1,
        run_id: run.clone(),
        graph: graph.clone(),
        event_cursor: cursor,
        control_revision: control.revision,
        runtime_identity: control.runtime_identity,
        task_id: task.cloned(),
        attempt_id: attempt.cloned(),
        boundary: boundary.into(),
        source: source.clone(),
        created_at: now,
    };
    let sources = std::iter::once(graph)
        .chain(source)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let artifact = Artifact::new(
        ArtifactKind::RuntimeCheckpoint,
        blob::stage_blob_bytes(
            tx,
            &serde_json::to_vec(&checkpoint)?,
            "application/json".into(),
        )?,
        "runtime.checkpoint",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.runtime".into(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        },
        Some(ArtifactOrigin {
            run_id: Some(run.clone()),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        }),
        sources,
        now,
    )?;
    insert_artifact(tx, &artifact)?;
    append_event(
        tx,
        run,
        None,
        None,
        LifecycleEventType::RunCheckpointSaved,
        Some(&artifact.artifact_id),
        now,
    )?;
    Ok(())
}

fn decode_checkpoint(
    connection: &Connection,
    run: &RunId,
    id: &ArtifactId,
    saved_cursor: i64,
) -> StoreResult<RunCheckpoint> {
    // 输入 event 中保存的 checkpoint Artifact ID 和 saved_cursor；解码后核对 Artifact origin/kind、
    // graph revision、原始 boundary event、source refs 与 cursor 先后关系。
    let artifact = read_artifact(connection, id)?;
    let checkpoint: RunCheckpoint = serde_json::from_slice(&blob::read_blob_bytes(
        connection,
        &artifact.blob.hash,
        artifact.blob.bytes,
    )?)?;
    let valid_graph: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM rebuild_workflow_revisions WHERE run_id=?1 AND graph_artifact_id=?2)",params![run.0,checkpoint.graph.artifact_id.0.as_str()],|r|r.get(0))?;
    let valid_boundary: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM rebuild_events WHERE run_id=?1 AND event_id=?2 AND event_type=?3 AND task_id IS ?4 AND attempt_id IS ?5 AND artifact_id IS ?6)",params![run.0,checkpoint.event_cursor,checkpoint.boundary,checkpoint.task_id.as_ref().map(|id|id.0.as_str()),checkpoint.attempt_id.as_ref().map(|id|id.0.as_str()),checkpoint.source.as_ref().map(|r|r.artifact_id.0.as_str())],|r|r.get(0))?;
    let expected = std::iter::once(checkpoint.graph.clone())
        .chain(checkpoint.source.clone())
        .collect::<BTreeSet<_>>();
    if artifact.kind != ArtifactKind::RuntimeCheckpoint
        || artifact.producer != "runtime.checkpoint"
        || artifact.provenance.source_family != "akzio.runtime"
        || artifact.lifecycle != ArtifactLifecycle::RunScoped
        || artifact.origin
            != Some(ArtifactOrigin {
                run_id: Some(run.clone()),
                task_id: None,
                attempt_id: None,
                contract_hash: None,
            })
        || checkpoint.version != 1
        || checkpoint.run_id != *run
        || checkpoint.event_cursor >= saved_cursor
        || checkpoint.graph.kind != ArtifactKind::WorkflowGraph
        || !valid_graph
        || !valid_boundary
        || artifact
            .source_refs
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            != expected
        || checkpoint.created_at != artifact.created_at
    {
        return Err(StoreError::Integrity(
            "invalid runtime checkpoint lineage".into(),
        ));
    }
    Ok(checkpoint)
}

pub(super) fn verify_checkpoints(connection: &Connection) -> StoreResult<()> {
    // Doctor 按 durable event 全量扫 checkpoint_saved 行；任一 payload/lineage 不匹配会停止本次验证，
    // 此函数只读且不修补缺少的 checkpoint。
    let mut query = connection.prepare("SELECT run_id,artifact_id,event_id FROM rebuild_events WHERE event_type='runtime.checkpoint_saved'")?;
    for row in query.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, i64>(2)?,
        ))
    })? {
        let (run, id, cursor) = row?;
        decode_checkpoint(
            connection,
            &RunId(run),
            &ArtifactId(ContentHash::new(id)?),
            cursor,
        )?;
    }
    Ok(())
}

fn latest_checkpoint(connection: &Connection, run: &RunId) -> StoreResult<Option<RunCheckpoint>> {
    // 按 Run 与 event_id DESC 取单条最新 checkpoint；无行 None，有行则把该行 event_id 作为边界复核。
    let row=connection.query_row("SELECT artifact_id,event_id FROM rebuild_events WHERE run_id=?1 AND event_type='runtime.checkpoint_saved' ORDER BY event_id DESC LIMIT 1",params![run.0],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?))).optional()?;
    row.map(|(id, cursor)| {
        decode_checkpoint(connection, run, &ArtifactId(ContentHash::new(id)?), cursor)
    })
    .transpose()
}

impl Store {
    /// Read the journal boundary and graph head in one SQLite snapshot.
    pub fn recovery_snapshot(&self, run: &RunId) -> StoreResult<WorkflowSnapshot> {
        // 在一个 Deferred snapshot 中同时读取 workflow head 与 checkpoint，拒绝 graph 分叉。
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let snapshot = self.workflow_snapshot_with_connection(&tx, run)?;
        if let Some(checkpoint) = latest_checkpoint(&tx, run)? {
            if checkpoint.graph.artifact_id != snapshot.revision.graph_artifact.artifact_id {
                return Err(StoreError::Integrity(
                    "checkpoint graph differs from durable head".into(),
                ));
            }
        }
        Ok(snapshot)
    }

    // 查询指定 Run 最新有效 checkpoint；连接 guard 在读取与解码完成后释放，不修改 head。
    pub fn run_checkpoint(&self, run: &RunId) -> StoreResult<Option<RunCheckpoint>> {
        latest_checkpoint(&*self.connection()?, run)
    }

    // 对外部事件对象先检查类型和无 task/attempt 形状，再从同连接读取 Artifact 校验边界；
    // 该方法不额外查询事件表来证明传入 cursor 与 event 行一致。
    pub fn validate_checkpoint_event(&self, event: &StoredEvent) -> StoreResult<RunCheckpoint> {
        // 外部事件页只能验证无 task/attempt 的 checkpoint_saved 形状，再解析其 CAS payload。
        if event.event_type != "runtime.checkpoint_saved"
            || event.task_id.is_some()
            || event.attempt_id.is_some()
        {
            return Err(StoreError::Integrity("invalid checkpoint event".into()));
        }
        decode_checkpoint(
            &*self.connection()?,
            &event.run_id,
            event
                .artifact_id
                .as_ref()
                .ok_or_else(|| StoreError::Integrity("checkpoint has no artifact".into()))?,
            event.cursor,
        )
    }

    // 以 Deferred 事务组装 workflow/control/checkpoint 投影；任何一项不可读都返回 Err，不做修复。
    pub fn inspect_run(&self, run: &RunId) -> StoreResult<RunInspection> {
        // 公开入口只获取一个 Deferred 连接，具体投影复用 connection-scoped inspect。
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
        self.inspect_run_with_connection(&tx, run)
    }

    pub(super) fn inspect_run_with_connection(
        &self,
        tx: &Connection,
        run: &RunId,
    ) -> StoreResult<RunInspection> {
        // 借用调用方快照连接，组合 workflow/control/checkpoint/session；recovery 与 allowed_actions
        // 都是当时 SQL 状态的派生字符串/列表，不会修改控制权。
        let workflow = self.workflow_snapshot_with_connection(tx, run)?;
        let blueprint = workflow.revision.graph.blueprint(workflow.run.purpose)?;
        let control = read_control(tx, run)?;
        let checkpoint = latest_checkpoint(tx, run)?;
        let recovery = if workflow
            .tasks
            .iter()
            .any(|t| t.status == TaskStatus::Running)
        {
            "lease_governed"
        } else if control.status == akzio_domain::RunControlStatus::Aborted {
            "terminal"
        } else if matches!(
            control.status,
            akzio_domain::RunControlStatus::Paused | akzio_domain::RunControlStatus::PauseRequested
        ) {
            "manual_control"
        } else if workflow
            .tasks
            .iter()
            .any(|t| t.status == TaskStatus::Pending)
        {
            "dependency_or_due_time"
        } else {
            "terminal"
        }
        .into();
        let session = debug::read_session(tx, run)?;
        let retired = super::workflow::legacy_workflow(tx, run)?;
        let allowed_actions = if !retired && control.debug_identity.is_some() {
            match control.status {
                akzio_domain::RunControlStatus::Paused => {
                    vec!["resume", "step", "retry_node", "abort"]
                }
                akzio_domain::RunControlStatus::Running
                | akzio_domain::RunControlStatus::Stepping => vec!["pause"],
                _ => vec![],
            }
        } else {
            vec![]
        }
        .into_iter()
        .filter(|action| {
            *action != "resume"
                || !session
                    .as_ref()
                    .is_some_and(|s| s.identity.research_only_without_policy())
        })
        .map(str::to_owned)
        .collect();
        Ok(RunInspection {
            version: 1,
            workflow,
            blueprint,
            control,
            checkpoint,
            recovery,
            allowed_actions,
        })
    }

    pub fn run_event_page(
        &self,
        run: &RunId,
        after: i64,
        limit: usize,
        task: Option<&TaskId>,
        attempt: Option<&AttemptId>,
    ) -> StoreResult<RunEventPage> {
        // limit 被夹到 1..=500；先确认 Run 存在，再按 run_id、event_id>after 和可选 task/attempt 过滤，
        // 多取一行算 has_more 后截断。查询本身不写事件；Artifact source refs 逐项只读恢复。
        // 这里没有显式 Deferred 事务，跨独立连接的写入可能发生在 event page 与 Artifact reads 之间。
        let limit = limit.clamp(1, 500);
        let connection = self.connection()?;
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM rebuild_runs WHERE run_id=?1)",
            params![run.0],
            |r| r.get(0),
        )?;
        if !exists {
            return Err(StoreError::MissingRun(run.clone()));
        }
        let mut query=connection.prepare("SELECT event_id,run_id,task_id,attempt_id,event_type,artifact_id,created_at FROM rebuild_events WHERE run_id=?1 AND event_id>?2 AND (?3 IS NULL OR task_id=?3) AND (?4 IS NULL OR attempt_id=?4) ORDER BY event_id LIMIT ?5")?;
        let mut events = query
            .query_map(
                params![
                    run.0,
                    after,
                    task.map(|id| id.0.as_str()),
                    attempt.map(|id| id.0.as_str()),
                    limit + 1
                ],
                super::trajectory::stored_event_from_row,
            )?
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = events.len() > limit;
        events.truncate(limit);
        let next_cursor = events.last().map_or(after, |e| e.cursor);
        let events = events
            .into_iter()
            .map(|event| {
                let artifact = event
                    .artifact_id
                    .as_ref()
                    .map(|id| read_artifact(&connection, id))
                    .transpose()?;
                Ok(RunEventView {
                    cursor: event.cursor,
                    run_id: event.run_id,
                    task_id: event.task_id,
                    attempt_id: event.attempt_id,
                    event_type: event.event_type,
                    artifact: artifact.as_ref().map(|a| ArtifactRef {
                        artifact_id: a.artifact_id.clone(),
                        kind: a.kind,
                    }),
                    source_refs: artifact.map(|a| a.source_refs).unwrap_or_default(),
                    created_at: event.created_at,
                })
            })
            .collect::<StoreResult<Vec<_>>>()?;
        Ok(RunEventPage {
            events,
            next_cursor,
            has_more,
        })
    }
}
