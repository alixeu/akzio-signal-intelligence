// 文件导读：Debug HTTP handler 只允许认证 native client 访问隔离 Debug Core，并把
// prepare/control/fork/acceptance 交给 daemon 的 revision/lease/Store 逻辑。inspect 是只读
// 投影；错误统一为结构化状态，不能把受理控制请求写成节点执行、Paper authorization 或
// Outcome 完成。
// Rust 机制：Axum extractor 借用/拥有请求字段；`DebugHttpResult` 统一 JSON 错误类型，
// `run_daemon_store_operation` 以 `FnOnce + Send + 'static` 将闭包串行化，serde 严格反序列化
// 防止未知字段改变控制语义。

#[derive(Debug, Deserialize)]
struct DebugInspectQuery {
    task: Option<TaskId>,
    attempt: Option<akzio_domain::AttemptId>,
}

fn authorize_debug_control(
    daemon: &Daemon,
    headers: &HeaderMap,
) -> std::result::Result<(), StatusCode> {
    // 先复用通用 token，再检查 debug enabled/native origin；顺序保证浏览器或普通 Core
    // 不会进入 Store control mutation。
    authorize(daemon, headers)?;
    if !daemon.debug_enabled() {
        return Err(StatusCode::FORBIDDEN);
    }
    // Native clients use a secret header and no browser origin. Reject browser
    // initiated mutations even if a browser somehow acquired that header.
    if headers.contains_key("origin") || headers.get("sec-fetch-site").is_some_and(|v| v != "none")
    {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(())
}

type DebugHttpResult =
    std::result::Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)>;
fn debug_http_error(error: DaemonError) -> (StatusCode, Json<serde_json::Value>) {
    // MissingRun/Task 是资源不存在，其他控制错误统一为 conflict，提醒客户端重新读取
    // revision/identity，而不是自动重试旧 request。
    let status = if matches!(
        error,
        DaemonError::Store(StoreError::MissingRun(_) | StoreError::MissingTask(_))
    ) {
        StatusCode::NOT_FOUND
    } else {
        StatusCode::CONFLICT
    };
    (status, Json(serde_json::json!({"error":error.to_string()})))
}
fn debug_auth_error(status: StatusCode) -> (StatusCode, Json<serde_json::Value>) {
    // 错误 body 固定且不回显 token/内部 Store 路径；状态码保留 unauthorized/forbidden。
    (
        status,
        Json(
            serde_json::json!({"error":"debug control requires authenticated native client and isolated Debug Core"}),
        ),
    )
}

async fn http_debug_runs(State(daemon): State<Arc<Daemon>>, headers: HeaderMap) -> DebugHttpResult {
    // 列出最近 Debug sessions 的只读 projection；lifecycle health 只用于诊断，不领取任务。
    authorize(&daemon, &headers).map_err(debug_auth_error)?;
    let operation = daemon.clone();
    run_daemon_store_operation(daemon.store_executor.clone(),move||{
        let mut runs=Vec::new();
        for workflow in operation.store.recent_workflows(100)? {
            if let Some(session)=operation.store.debug_session(&workflow.run.run_id)? {
                runs.push(serde_json::json!({"session":session,"lifecycle":operation.store.run_lifecycle_health(&workflow.run.run_id)?}));
            }
        }
        Ok(serde_json::json!({"enabled":operation.debug_enabled(),"store_identity":operation.store.debug_environment()?,"runs":runs}))
    }).await.map(Json).map_err(debug_http_error)
}

async fn http_debug_inspect(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    Query(query): Query<DebugInspectQuery>,
    headers: HeaderMap,
) -> DebugHttpResult {
    // Inspect 只读 session/node/acceptance；task/attempt query 缩小视图，不改变 revision。
    authorize(&daemon, &headers).map_err(debug_auth_error)?;
    let operation = daemon.clone();
    run_daemon_store_operation(daemon.store_executor.clone(), move || {
        Ok(serde_json::to_value(operation.inspect_debug(
            &RunId(run_id),
            query.task.as_ref(),
            query.attempt.as_ref(),
        )?)?)
    })
    .await
    .map(Json)
    .map_err(debug_http_error)
}

async fn http_debug_prepare(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
    Json(request): Json<DebugPrepareRequest>,
) -> DebugHttpResult {
    // Prepare 将请求交给 daemon 的隔离 Store 事务，返回的是 graph/identity/paused head，
    // 不执行第一个节点或授予 broker 写权限。
    authorize_debug_control(&daemon, &headers).map_err(debug_auth_error)?;
    let operation = daemon.clone();
    run_daemon_store_operation(daemon.store_executor.clone(), move || {
        Ok(serde_json::to_value(operation.prepare_debug(&request)?)?)
    })
    .await
    .map(Json)
    .map_err(debug_http_error)
}

async fn http_debug_control(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<akzio_domain::DebugControlRequest>,
) -> DebugHttpResult {
    // Control 携带 expected_revision，由 Store CAS 消费；重复/过期请求必须 conflict，避免
    // UI 重试造成 sibling claim 或隐式扩大预算。
    authorize_debug_control(&daemon, &headers).map_err(debug_auth_error)?;
    let operation = daemon.clone();
    run_daemon_store_operation(daemon.store_executor.clone(), move || {
        Ok(serde_json::to_value(
            operation.control_debug(&RunId(run_id), &request)?,
        )?)
    })
    .await
    .map(Json)
    .map_err(debug_http_error)
}

async fn http_debug_fork(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<DebugForkRequest>,
) -> DebugHttpResult {
    // Fork 创建新的 Run/identity lineage；旧成功输出不复制为新成功状态，也不转移 Paper
    // session/approval。
    authorize_debug_control(&daemon, &headers).map_err(debug_auth_error)?;
    let operation = daemon.clone();
    run_daemon_store_operation(daemon.store_executor.clone(), move || {
        Ok(serde_json::to_value(
            operation.fork_debug(&RunId(run_id), &request)?,
        )?)
    })
    .await
    .map(Json)
    .map_err(debug_http_error)
}

async fn http_debug_acceptance(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<akzio_domain::StageAcceptance>,
) -> DebugHttpResult {
    // Acceptance 只记录 typed evidence/checks，先校验 path RunId 与 DebugSession；记录成功
    // 不改变 workflow status 或授予下一阶段权限。
    authorize_debug_control(&daemon, &headers).map_err(debug_auth_error)?;
    if request.run_id.0 != run_id {
        return Err(debug_auth_error(StatusCode::BAD_REQUEST));
    }
    let operation = daemon.clone();
    run_daemon_store_operation(daemon.store_executor.clone(), move || {
        if operation.store.debug_session(&request.run_id)?.is_none() {
            return Err(DaemonError::InvalidInput("not_debug_session".into()));
        }
        Ok(serde_json::to_value(
            operation.store.record_stage_acceptance(&request)?,
        )?)
    })
    .await
    .map(Json)
    .map_err(debug_http_error)
}
