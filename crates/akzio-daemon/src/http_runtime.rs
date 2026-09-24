// 文件导读：runtime HTTP handler 提供 workflow blueprint、Run inspection 和分页 journal。
// blueprint 只读编译器输出不创建 Run；inspection/journal 只展示 Store 的 durable state，
// 不领取 task、刷新 evidence 或改变状态，因而不能把查询成功当作业务完成。
// Rust 机制：Query/Path 的 serde 解析把分页和 ID 变成强类型；StoreExecutor 闭包拥有
// `RunId` 后在串行 Store 通道执行；`Result<Json<T>, StatusCode>` 显式映射 MissingRun 与内部错误。

#[derive(Debug, Deserialize)]
struct BlueprintQuery {
    purpose: RunPurpose,
}

#[derive(Debug, Deserialize)]
struct JournalQuery {
    #[serde(default)]
    after: i64,
    limit: Option<usize>,
    task_id: Option<akzio_domain::TaskId>,
    attempt_id: Option<akzio_domain::AttemptId>,
}

async fn http_workflow_blueprint(
    State(daemon): State<Arc<Daemon>>,
    Query(query): Query<BlueprintQuery>,
    headers: HeaderMap,
) -> std::result::Result<Json<akzio_domain::WorkflowBlueprint>, StatusCode> {
    // Blueprint 只调用与执行相同的 compiler，返回 deterministic graph projection；它不写
    // Store、不创建 EvidenceNeed/Run，也不探测模型或 broker。
    authorize(&daemon, &headers)?;
    if !matches!(
        query.purpose,
        RunPurpose::PositionPlan | RunPurpose::Paper | RunPurpose::Shadow
    ) {
        return Err(StatusCode::BAD_REQUEST);
    }
    // Uses the exact compiler used by execution. No run, model or evidence is created.
    let definition = daemon
        .workflow
        .research_definition("approved-research")
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let graph = daemon
        .workflow
        .lower(query.purpose, &definition.proposal)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    graph
        .blueprint(query.purpose)
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn http_run_inspection(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<Json<akzio_store::RunInspection>, StatusCode> {
    // Inspection 通过 StoreExecutor 读取 checkpoint/allowed_actions；runtime identity 不匹配
    // 时 daemon 只收紧动作，不能由 HTTP client 自行恢复权限。
    authorize(&daemon, &headers)?;
    let operation = daemon.clone();
    daemon
        .store_executor
        .execute(move |_| operation.runtime_inspection(&RunId(run_id)))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(Json)
        .map_err(|error| match error {
            DaemonError::Store(error) => runtime_store_status(error),
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })
}

impl Daemon {
    pub(crate) fn runtime_inspection(&self, run: &RunId) -> Result<akzio_store::RunInspection> {
        // 这是 observer 的权限投影层：仅按当前 Debug runtime identity 过滤 allowed_actions，
        // 原始 workflow/task/lease 状态仍由 Store 保持不变。
        let mut view = self.store.inspect_run(run)?;
        if view.control.debug_identity.is_some()
            && !self.debug_control.as_ref().is_some_and(|config| {
                Some(&config.runtime_identity) == view.control.runtime_identity.as_ref()
            })
        {
            view.allowed_actions
                .retain(|action| matches!(action.as_str(), "pause" | "abort"));
        }
        Ok(view)
    }
}

async fn http_run_journal(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    Query(query): Query<JournalQuery>,
    headers: HeaderMap,
) -> std::result::Result<Json<akzio_store::RunEventPage>, StatusCode> {
    // Journal 以 after/limit/task/attempt 做只读分页；非法范围在访问 Store 前拒绝，避免
    // 把大范围日志扫描当成运行控制。
    authorize(&daemon, &headers)?;
    if query.after < 0 || query.limit.is_some_and(|limit| !(1..=500).contains(&limit)) {
        return Err(StatusCode::BAD_REQUEST);
    }
    daemon
        .store_executor
        .execute(move |store| {
            store.run_event_page(
                &RunId(run_id),
                query.after,
                query.limit.unwrap_or(100),
                query.task_id.as_ref(),
                query.attempt_id.as_ref(),
            )
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(Json)
        .map_err(runtime_store_status)
}

fn runtime_store_status(error: StoreError) -> StatusCode {
    // 只把 MissingRun 暴露为 404，其余 Store 失败保持 500，避免客户端猜测内部状态。
    match error {
        StoreError::MissingRun(_) => StatusCode::NOT_FOUND,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
