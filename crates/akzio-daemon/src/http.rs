//! Authenticated loopback HTTP transport.

// 文件导读：HTTP transport 是 CLI/Observatory 到 daemon 的回环入口。每个 handler 先做
// token/native-client/Debug 隔离校验，再把查询或写入排队到 StoreExecutor；SSE 只发送
// reasoning/event invalidation。HTTP 200、SSE event 或 Store maintenance 完成只证明该
// transport 操作有界结束，不证明研究/Decision/Execution、Paper submission/fill 或 Outcome。
// Rust 机制：Axum extractor/Router 宏把 Path/Query/Json/State 组合成 handler；SSE 用
// `async_stream::stream!` 生成 Stream，`select!` 同时监听 broadcast、poll 和 shutdown；
// generic `run_store_operation<T, F>` 以 `Send + 'static` 闭包跨 StoreExecutor 线程。

use super::*;
include!("http_debug.rs");
include!("http_runtime.rs");
include!("http_launch.rs");
use crate::observer::{
    ObserverPortfolioHistory, ObserverPortfolioRange, ObserverRunDetail, ObserverSection,
    ObserverSnapshot,
};

#[derive(Debug, Deserialize)]
struct ObserverPortfolioHistoryQuery {
    range: ObserverPortfolioRange,
}

#[derive(Debug, Deserialize)]
struct CanaryResumeRequest {
    campaign_id: ContentHash,
}

#[derive(Debug, Deserialize)]
struct StoreBackupRequest {
    target: PathBuf,
}

#[derive(Debug, Deserialize)]
struct StoreRestoreRequest {
    source: PathBuf,
    target: PathBuf,
}

#[derive(Debug, Deserialize)]
struct StoreExportRunRequest {
    run_id: String,
    target: PathBuf,
    include_raw_model: bool,
}

#[derive(Debug, Deserialize)]
struct StoreExportDebugBundleRequest {
    run_id: String,
    target: PathBuf,
}

#[derive(Debug, Deserialize)]
struct StoreEventsQuery {
    after: Option<i64>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct StoreLessonListQuery {
    lifecycle: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct StoreLessonTransitionRequest {
    lifecycle: LessonLifecycle,
    actor: String,
    reason: String,
}

impl Daemon {
    // Router 只注册控制面路径；状态权限在每个 handler 的 authorize 和 runtime/Store
    // 调用中再次校验，路由存在本身不代表任何业务动作已授权。
    pub fn router(&self) -> Router {
        Router::new()
            .route(
                "/v1/debug/runs",
                get(http_debug_runs).post(http_debug_prepare),
            )
            .route("/v1/debug/runs/{run_id}", get(http_debug_inspect))
            .route("/v1/debug/runs/{run_id}/control", post(http_debug_control))
            .route("/v1/debug/runs/{run_id}/fork", post(http_debug_fork))
            .route(
                "/v1/debug/runs/{run_id}/acceptance",
                post(http_debug_acceptance),
            )
            .route("/runs", post(http_launch_run))
            .route("/health", get(http_health))
            .route("/ready", get(http_ready))
            .route("/v1/observer/snapshot", get(http_observer_snapshot))
            .route("/v1/observer/runs/{run_id}", get(http_observer_run))
            .route("/v1/workflows/blueprint", get(http_workflow_blueprint))
            .route(
                "/v1/observer/runs/{run_id}/inspection",
                get(http_run_inspection),
            )
            .route("/v1/observer/runs/{run_id}/journal", get(http_run_journal))
            .route(
                "/v1/observer/portfolio/history",
                get(http_observer_portfolio_history),
            )
            .route("/v1/observer/events", get(http_observer_events))
            .route("/runs/{run_id}/events", get(http_events))
            .route("/runs/{run_id}/trajectory", get(http_trajectory))
            .route("/runs/{run_id}/retrospectives", get(http_retrospectives))
            .route("/runs/{run_id}/replay", get(http_replay))
            .route("/runs/{run_id}/cancel", post(http_cancel))
            .route("/runs/{run_id}/retry", post(http_retry))
            .route(
                "/runs/{run_id}/repair-narrative",
                post(http_repair_narrative),
            )
            .route("/control/freeze", post(http_freeze))
            .route("/control/unfreeze", post(http_unfreeze))
            .route("/control/paper-approval", post(http_paper_approval))
            .route("/control/canary/stage", post(http_canary_stage))
            .route("/control/canary/status", get(http_canary_status))
            .route("/control/canary/resume", post(http_canary_resume))
            .route("/control/store/doctor", get(http_store_doctor))
            .route("/control/store/inventory", get(http_store_inventory))
            .route("/control/store/metrics", get(http_store_metrics))
            .route("/control/store/executor", get(http_store_executor))
            .route("/control/store/alerts", get(http_store_alerts))
            .route(
                "/control/store/session/{session_key}",
                get(http_store_session),
            )
            .route(
                "/control/store/release-evidence/{run_id}",
                get(http_store_release_evidence),
            )
            .route("/control/store/backup", post(http_store_backup))
            .route("/control/store/restore", post(http_store_restore))
            .route("/control/store/export-run", post(http_store_export_run))
            .route(
                "/control/store/export-debug-bundle",
                post(http_store_export_debug_bundle),
            )
            .route("/control/store/events/{run_id}", get(http_store_events))
            .route("/control/store/lessons", get(http_store_lessons))
            .route("/control/store/lessons/add", post(http_store_lesson_add))
            .route("/control/store/lessons/{lesson_id}", get(http_store_lesson))
            .route(
                "/control/store/lessons/{lesson_id}/usage",
                get(http_store_lesson_usage),
            )
            .route(
                "/control/store/lessons/{lesson_id}/transition",
                post(http_store_lesson_transition),
            )
            .with_state(Arc::new(self.clone()))
    }

    pub async fn serve_http(
        &self,
        address: SocketAddr,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        // 入口先限制 loopback，再绑定 listener；bind 成功只代表传输层监听成功，不启动
        // Paper scheduler，也不创建 Run。
        if !address.ip().is_loopback() {
            return Err(DaemonError::InvalidInput(
                "daemon HTTP control API must bind a loopback address".to_owned(),
            ));
        }
        let listener = TcpListener::bind(address).await?;
        self.serve_http_listener(listener, shutdown).await
    }

    pub async fn serve_http_listener(
        &self,
        listener: TcpListener,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        // listener 复查 loopback 后交给 Axum；graceful shutdown 只结束连接服务，正在执行
        // 的 task 状态仍由 TaskRuntime/Store 恢复。
        let address = listener.local_addr()?;
        if !address.ip().is_loopback() {
            return Err(DaemonError::InvalidInput(
                "daemon HTTP control API must bind a loopback address".to_owned(),
            ));
        }
        axum::serve(
            listener,
            self.router().layer(axum::Extension(shutdown.clone())),
        )
        .with_graceful_shutdown(wait_for_shutdown(shutdown))
        .await
        .map_err(DaemonError::Io)
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    // watch receiver 只观察 supervisor 的停止值；通道关闭也结束等待，不撤销 durable 状态。
    if *shutdown.borrow() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            return;
        }
    }
}

// Close transport streams on graceful shutdown without cancelling task futures.
async fn wait_for_stream_shutdown(shutdown: &Option<axum::Extension<watch::Receiver<bool>>>) {
    // SSE stream 使用可选 Extension：测试/独立 router 没有 shutdown 时保持 pending，正式
    // server 则在 watch=true 后关闭 stream，避免长连接阻塞 HTTP graceful shutdown。
    match shutdown {
        Some(receiver) => wait_for_shutdown(receiver.0.clone()).await,
        None => std::future::pending::<()>().await,
    }
}

async fn http_health(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
) -> std::result::Result<Json<DaemonHealth>, StatusCode> {
    // Health 是认证只读投影；StoreExecutor 保证读取与维护/heartbeat 的串行边界，health
    // 中的 scheduler/Policy 状态不是某个 Run 的完成证明。
    authorize(&daemon, &headers)?;
    let operation = daemon.clone();
    run_daemon_store_operation(daemon.store_executor.clone(), move || operation.health())
        .await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn http_ready(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
) -> std::result::Result<Json<DaemonHealth>, StatusCode> {
    // Ready 在 health 之外检查 Paper broker 注入；SERVICE_UNAVAILABLE 只说明服务当前不可
    // 运行，不把某次 readiness 变成 Paper approval 或 execution authorization。
    authorize(&daemon, &headers)?;
    let operation = daemon.clone();
    run_daemon_store_operation(daemon.store_executor.clone(), move || operation.ready())
        .await
        .map(Json)
        .map_err(|error| match error {
            DaemonError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })
}

async fn http_observer_snapshot(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
) -> std::result::Result<Json<ObserverSnapshot>, StatusCode> {
    // Snapshot 合并 durable observer 与有界外部观察；失败返回 500，不用空 JSON 掩盖未知。
    authorize(&daemon, &headers)?;
    daemon.observer_snapshot().await.map(Json).map_err(|error| {
        eprintln!("observer snapshot failed: {error}");
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

async fn http_observer_run(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<Json<ObserverRunDetail>, StatusCode> {
    // Run detail 是只读审计投影，MissingRun 映射 404；它不 claim task、不重跑模型。
    authorize(&daemon, &headers)?;
    let run_id = RunId(run_id);
    let operation = daemon.clone();
    let operation_run_id = run_id.clone();
    run_daemon_store_operation(daemon.store_executor.clone(), move || {
        operation.observer_run_detail(&operation_run_id)
    })
    .await
    .map(Json)
    .map_err(|error| {
        eprintln!("observer run detail failed for {run_id}: {error}");
        match error {
            DaemonError::Store(StoreError::MissingRun(_)) => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    })
}

async fn http_observer_portfolio_history(
    State(daemon): State<Arc<Daemon>>,
    Query(query): Query<ObserverPortfolioHistoryQuery>,
    headers: HeaderMap,
) -> std::result::Result<Json<ObserverSection<ObserverPortfolioHistory>>, StatusCode> {
    // Portfolio history 只读取 Paper provider 并标记 section 状态；图表/点数不是后续账户
    // NAV 完整账本或 fill 证明。
    authorize(&daemon, &headers)?;
    Ok(Json(daemon.observer_portfolio_history(query.range).await))
}

async fn http_observer_events(
    State(daemon): State<Arc<Daemon>>,
    Query(query): Query<EventQuery>,
    headers: HeaderMap,
    shutdown: Option<axum::Extension<watch::Receiver<bool>>>,
) -> std::result::Result<
    Sse<impl futures::Stream<Item = std::result::Result<Event, Infallible>>>,
    StatusCode,
> {
    // 全局 SSE 同时监听 reasoning broadcast 与 Store event cursor；cursor 只用于增量观察，
    // Lagged/Closed 被显式写成 error/结束，不伪造丢失事件。
    authorize(&daemon, &headers)?;
    let mut cursor = query.after.unwrap_or(0);
    let mut reasoning_events = daemon.reasoning_events.subscribe();
    let mut poll = tokio::time::interval(std::time::Duration::from_millis(500));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let stream = stream! {
        loop {
            tokio::select! {
                _ = wait_for_stream_shutdown(&shutdown) => break,
                event = reasoning_events.recv() => match event {
                    Ok(event) => match serde_json::to_string(&event) {
                        Ok(data) => yield Ok(Event::default()
                            .event(event.event_name())
                            .data(data)),
                        Err(error) => yield Ok(Event::default()
                            .event("error")
                            .data(error.to_string())),
                    },
                    Err(broadcast::error::RecvError::Lagged(skipped)) => yield Ok(Event::default()
                        .event("error")
                        .data(format!("reasoning stream lagged by {skipped} events"))),
                    Err(broadcast::error::RecvError::Closed) => {}
                },
                _ = poll.tick() => match daemon
                    .store_executor
                    .execute(|store| store.event_cursor())
                    .await
                {
                    Ok(Ok(next)) if next > cursor => {
                        cursor = next;
                        match serde_json::to_string(&ObserverInvalidation { cursor }) {
                            Ok(data) => yield Ok(Event::default()
                                .id(cursor.to_string())
                                .event("invalidate")
                                .data(data)),
                            Err(error) => yield Ok(Event::default()
                                .event("error")
                                .data(error.to_string())),
                        }
                    }
                    Ok(Ok(_)) => yield Ok(Event::default().comment("keepalive")),
                    Ok(Err(error)) => yield Ok(Event::default()
                        .event("error")
                        .data(error.to_string())),
                    Err(error) => yield Ok(Event::default()
                        .event("error")
                        .data(error.to_string())),
                }
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

async fn http_events(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    Query(query): Query<EventQuery>,
    headers: HeaderMap,
    shutdown: Option<axum::Extension<watch::Receiver<bool>>>,
) -> std::result::Result<
    Sse<impl futures::Stream<Item = std::result::Result<Event, Infallible>>>,
    StatusCode,
> {
    // Run SSE 过滤同一 run_id 的 reasoning/event；poll 与 stream shutdown 并发竞争，连接
    // 关闭不会取消或回滚已经持久化的 task/commitment。
    authorize(&daemon, &headers)?;
    let run_id = RunId(run_id);
    let mut cursor = query.after.unwrap_or(0);
    let mut reasoning_events = daemon.reasoning_events.subscribe();
    let mut poll = tokio::time::interval(std::time::Duration::from_millis(200));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let stream = stream! {
        loop {
            tokio::select! {
                _ = wait_for_stream_shutdown(&shutdown) => break,
                event = reasoning_events.recv() => match event {
                    Ok(event) if event.run_id() == &run_id => {
                        match serde_json::to_string(&event) {
                            Ok(data) => yield Ok(Event::default()
                                .event(event.event_name())
                                .data(data)),
                            Err(error) => yield Ok(Event::default()
                                .event("error")
                                .data(error.to_string())),
                        }
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(skipped)) => yield Ok(Event::default()
                        .event("error")
                        .data(format!("reasoning stream lagged by {skipped} events"))),
                    Err(broadcast::error::RecvError::Closed) => {}
                },
                _ = poll.tick() => {
                    let event_run_id = run_id.clone();
                    match daemon.store_executor.execute(move |store| {
                        store.events_after(&event_run_id, cursor, EVENT_PAGE_SIZE)
                    }).await {
                    Ok(Ok(events)) if events.is_empty() => {
                        yield Ok(Event::default().comment("keepalive"));
                    }
                    Ok(Ok(events)) => {
                        for event in events {
                            cursor = event.cursor;
                            match serde_json::to_string(&EventView::from(event)) {
                                Ok(data) => {
                                    yield Ok(Event::default().id(cursor.to_string()).event("akzio").data(data));
                                }
                                Err(error) => {
                                    yield Ok(Event::default().event("error").data(error.to_string()));
                                }
                            }
                        }
                    }
                    Ok(Err(error)) => {
                        yield Ok(Event::default().event("error").data(error.to_string()));
                    }
                    Err(error) => {
                        yield Ok(Event::default().event("error").data(error.to_string()));
                    }
                    }
                }
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

async fn http_replay(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<Json<ReplayReport>, StatusCode> {
    // Replay 读取 workflow/lifecycle/cursor 事实；terminal_task_count 只表示 task 状态，
    // 不跨越到 Paper fill 或 Outcome seal。
    authorize(&daemon, &headers)?;
    let operation = daemon.clone();
    run_daemon_store_operation(daemon.store_executor.clone(), move || {
        operation.replay_report(&RunId(run_id))
    })
    .await
    .map(Json)
    .map_err(invalid_input_or_conflict)
}

async fn http_trajectory(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<Json<Vec<TrajectoryEntry>>, StatusCode> {
    authorize(&daemon, &headers)?;
    let operation = daemon.clone();
    run_daemon_store_operation(daemon.store_executor.clone(), move || {
        operation.trajectory(&RunId(run_id))
    })
    .await
    .map(Json)
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn http_retrospectives(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<Json<Vec<RetrospectiveView>>, StatusCode> {
    authorize(&daemon, &headers)?;
    let operation = daemon.clone();
    run_daemon_store_operation(daemon.store_executor.clone(), move || {
        operation.retrospectives(&RunId(run_id))
    })
    .await
    .map(Json)
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn http_cancel(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<Json<RunCancellationResponse>, StatusCode> {
    // Cancel 只请求 TaskRuntime 按取消规则关闭可取消任务；已完成 Artifact/订单/Outcome
    // 不被 HTTP handler 删除或改写。
    authorize(&daemon, &headers)?;
    let run_id = RunId(run_id);
    daemon
        .request_cancel(&run_id, "operator cancellation request")
        .await
        .map(|cancelled_tasks| {
            Json(RunCancellationResponse {
                run_id,
                cancelled_tasks,
            })
        })
        .map_err(invalid_input_or_conflict)
}

async fn http_retry(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<Json<RunRetryResponse>, StatusCode> {
    // Retry 的可执行 purpose、terminal status 和 Debug/Paper 限制由 daemon 检查；返回新
    // RunId 表示新研究图已发布，不是旧 Run 重新完成。
    authorize(&daemon, &headers)?;
    let source_run_id = RunId(run_id);
    let operation = daemon.clone();
    let operation_run_id = source_run_id.clone();
    run_daemon_store_operation(daemon.store_executor.clone(), move || {
        operation.retry_run(&operation_run_id)
    })
    .await
    .map(|run_id| {
        Json(RunRetryResponse {
            source_run_id,
            run_id,
        })
    })
    .map_err(invalid_input_or_conflict)
}

fn invalid_input_or_internal(error: DaemonError) -> StatusCode {
    // 只把明确的 InvalidInput 映射为 400，其余 runtime/store/provider 错误保守返回 500。
    if matches!(error, DaemonError::InvalidInput(_)) {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

fn invalid_input_or_conflict(error: DaemonError) -> StatusCode {
    // 控制请求的非输入错误统一为冲突，提示调用方重新 inspect，而不是重试/猜测状态。
    if matches!(error, DaemonError::InvalidInput(_)) {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::CONFLICT
    }
}

async fn http_freeze(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
    Json(request): Json<FreezeRequest>,
) -> std::result::Result<Json<DaemonHealth>, StatusCode> {
    // Freeze/unfreeze 写入 durable FreezeState 后再返回 health；冻结是调度控制，不等于
    // 取消已有 Paper order 或改变已 sealed Outcome。
    authorize(&daemon, &headers)?;
    let operation = daemon.clone();
    run_daemon_store_operation(daemon.store_executor.clone(), move || {
        operation.set_freeze(true, request.reason)
    })
    .await
    .map(Json)
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn http_unfreeze(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
    Json(request): Json<FreezeRequest>,
) -> std::result::Result<Json<DaemonHealth>, StatusCode> {
    authorize(&daemon, &headers)?;
    let operation = daemon.clone();
    run_daemon_store_operation(daemon.store_executor.clone(), move || {
        operation.set_freeze(false, request.reason)
    })
    .await
    .map(Json)
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn http_paper_approval(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
    Json(request): Json<PaperApprovalRequest>,
) -> std::result::Result<Json<PaperApprovalResponse>, StatusCode> {
    // approval handler 只调用身份、资格、账户和 manifest 持久化流程；approval accepted
    // 仍必须经过 Decision/ExecutionGate，不能直接触发 Paper submission。
    authorize(&daemon, &headers)?;
    daemon
        .approve_paper(request)
        .await
        .map(Json)
        .map_err(invalid_input_or_internal)
}

async fn http_canary_stage(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
    Json(spec): Json<akzio_domain::CanaryCampaignSpec>,
) -> std::result::Result<Json<akzio_store::CanaryCampaignHead>, StatusCode> {
    // Canary stage 只写入 campaign manifest/lease 绑定，candidate 仍不能绕过 active policy。
    authorize(&daemon, &headers)?;
    let operation = daemon.clone();
    run_daemon_store_operation(daemon.store_executor.clone(), move || {
        operation.stage_canary_campaign(spec)
    })
    .await
    .map(Json)
    .map_err(invalid_input_or_internal)
}

async fn http_canary_status(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
) -> std::result::Result<Json<Option<akzio_store::CanaryCampaignHead>>, StatusCode> {
    // Status 是只读 projection；None 表示没有 active campaign，不代表可以直接执行 candidate。
    authorize(&daemon, &headers)?;
    let operation = daemon.clone();
    run_daemon_store_operation(daemon.store_executor.clone(), move || {
        operation.canary_status()
    })
    .await
    .map(Json)
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn http_canary_resume(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
    Json(request): Json<CanaryResumeRequest>,
) -> std::result::Result<Json<akzio_store::CanaryCampaignHead>, StatusCode> {
    // Resume 由 scheduler lease 和 campaign identity fencing；返回 head 不是 paired Outcome
    // 已完成，也不是 candidate 已晋升。
    authorize(&daemon, &headers)?;
    let operation = daemon.clone();
    run_daemon_store_operation(daemon.store_executor.clone(), move || {
        operation.resume_canary_campaign(&request.campaign_id)
    })
    .await
    .map(Json)
    .map_err(invalid_input_or_internal)
}

async fn http_store_doctor(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
) -> std::result::Result<Json<serde_json::Value>, StatusCode> {
    // Doctor 走 maintenance 通道，独立于普通 task queue；完整性通过只说明 CAS/索引可读。
    authorize(&daemon, &headers)?;
    run_store_maintenance(
        daemon.maintenance(),
        StoreMaintenanceKind::Doctor,
        |store| store.verify_integrity(),
    )
    .await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn http_store_inventory(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
) -> std::result::Result<Json<serde_json::Value>, StatusCode> {
    // Inventory 是只读 Store 统计，禁止客户端把返回数量当成有效样本或 Policy readiness。
    authorize(&daemon, &headers)?;
    store_json(Ok(run_store_operation(
        daemon.store_executor.clone(),
        "store.inventory",
        |store| store.storage_inventory(),
    )
    .await?))
}

async fn http_store_metrics(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
) -> std::result::Result<Json<serde_json::Value>, StatusCode> {
    // Metrics 读取队列/lease/告警观察，不修改状态。
    authorize(&daemon, &headers)?;
    store_json(Ok(run_store_operation(
        daemon.store_executor.clone(),
        "store.metrics",
        |store| store.metrics(Utc::now()),
    )
    .await?))
}

async fn http_store_executor(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
) -> std::result::Result<Json<serde_json::Value>, StatusCode> {
    // Executor telemetry 展示队列和维护状态；queued/completed operation 不等同业务完成。
    authorize(&daemon, &headers)?;
    Ok(Json(store_executor_json(&daemon.store_executor)))
}

async fn http_store_alerts(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
) -> std::result::Result<Json<serde_json::Value>, StatusCode> {
    // Alerts 从同一 Store metrics 读取，保留 fail-closed 诊断而不清除历史事件。
    authorize(&daemon, &headers)?;
    let metrics = run_store_operation(daemon.store_executor.clone(), "store.alerts", |store| {
        store.metrics(Utc::now())
    })
    .await?;
    store_json(Ok(metrics.alerts()))
}

async fn http_store_session(
    State(daemon): State<Arc<Daemon>>,
    Path(session_key): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<Json<Option<akzio_store::SessionSlot>>, StatusCode> {
    // Session 查询返回 reservation/commitment 投影；None 不能被 CLI 当成可直接下单。
    authorize(&daemon, &headers)?;
    run_store_operation(
        daemon.store_executor.clone(),
        "store.session",
        move |store| store.session_slot(&session_key),
    )
    .await
    .map(Json)
}

async fn http_store_release_evidence(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> std::result::Result<Json<akzio_domain::ReleaseEvidenceBundle>, StatusCode> {
    // Release evidence 生成脱敏 bundle/hash；导出完整性与 Run/订单/Outcome 业务状态分开。
    authorize(&daemon, &headers)?;
    run_store_operation(
        daemon.store_executor.clone(),
        "store.release_evidence",
        move |store| {
            store.release_evidence_bundle(
                &RunId(run_id),
                &akzio_store::ReleaseEvidenceExpectations::default(),
            )
        },
    )
    .await
    .map(Json)
}

async fn http_store_backup(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
    Json(request): Json<StoreBackupRequest>,
) -> std::result::Result<Json<serde_json::Value>, StatusCode> {
    // Backup 是显式 maintenance 写入外部 target 的操作，完成只说明备份事务返回成功。
    authorize(&daemon, &headers)?;
    store_json(Ok(run_store_maintenance(
        daemon.maintenance(),
        StoreMaintenanceKind::Backup,
        move |store| store.backup_to(request.target),
    )
    .await?))
}

async fn http_store_restore(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
    Json(request): Json<StoreRestoreRequest>,
) -> std::result::Result<Json<serde_json::Value>, StatusCode> {
    // Restore 同样串行经过 maintenance；目标数据库的完整性由结果/Doctor 验证，不在 handler
    // 中推断原 Run 或 Policy 已恢复可执行。
    authorize(&daemon, &headers)?;
    store_json(Ok(run_store_maintenance(
        daemon.maintenance(),
        StoreMaintenanceKind::Restore,
        move |_| {
            akzio_store::Store::restore_from(request.source, request.target)
                .and_then(|store| store.metrics(Utc::now()))
        },
    )
    .await?))
}

async fn http_store_export_run(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
    Json(request): Json<StoreExportRunRequest>,
) -> std::result::Result<Json<serde_json::Value>, StatusCode> {
    // Run export 只读已有 Artifact，并由 Store 脱敏策略控制 raw model；导出成功不是运行成功。
    authorize(&daemon, &headers)?;
    store_json(Ok(run_store_operation(
        daemon.store_executor.clone(),
        "store.export_run",
        move |store| {
            store.export_run(
                &RunId(request.run_id),
                request.target,
                request.include_raw_model,
            )
        },
    )
    .await?))
}

async fn http_store_export_debug_bundle(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
    Json(request): Json<StoreExportDebugBundleRequest>,
) -> std::result::Result<Json<serde_json::Value>, StatusCode> {
    // Debug bundle 是分享用诊断投影，不重跑节点、不调用模型、不发送 broker 请求。
    authorize(&daemon, &headers)?;
    store_json(Ok(run_store_operation(
        daemon.store_executor.clone(),
        "store.export_debug_bundle",
        move |store| store.export_debug_bundle(&RunId(request.run_id), request.target),
    )
    .await?))
}

async fn http_store_lessons(
    State(daemon): State<Arc<Daemon>>,
    Query(query): Query<StoreLessonListQuery>,
    headers: HeaderMap,
) -> std::result::Result<Json<Vec<serde_json::Value>>, StatusCode> {
    // Lesson list 只过滤/展示 Store 生命周期；limit 是读取边界，不创建或激活 Lesson。
    authorize(&daemon, &headers)?;
    let lifecycle = query
        .lifecycle
        .map(|value| {
            serde_json::from_value(serde_json::Value::String(value.to_ascii_lowercase()))
                .map_err(|_| StatusCode::BAD_REQUEST)
        })
        .transpose()?;
    let limit = query.limit.unwrap_or(50);
    let lessons = run_store_operation(
        daemon.store_executor.clone(),
        "store.lessons",
        move |store| store.lessons(lifecycle, limit),
    )
    .await?;
    lessons
        .iter()
        .map(lesson_view_json)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map(Json)
}

async fn http_store_lesson_add(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
    Json(input): Json<LessonInput>,
) -> std::result::Result<Json<serde_json::Value>, StatusCode> {
    // Add 先把原始 operator input 作为 source Artifact，再由 Store 写 Lesson Draft，保证
    // source_refs/provenance 完整；Draft 不等于 Active、Policy 或执行规则。
    authorize(&daemon, &headers)?;
    if input.authored_by.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let now = Utc::now();
    let stored_input = serde_json::to_vec(&input).map_err(|_| StatusCode::BAD_REQUEST)?;
    let source_blob = run_store_operation(
        daemon.store_executor.clone(),
        "store.lesson.source",
        // Staging is connection-local; both executor operations share the daemon Store.
        move |store| store.stage_bytes(&stored_input, "application/json"),
    )
    .await?;
    let source = Artifact::new(
        ArtifactKind::SemanticDetail,
        source_blob,
        "operator.lesson.source",
        ArtifactLifecycle::Canonical,
        ArtifactProvenance {
            source_family: "akzio.operator".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: input.confidence_ppm,
            producer_contract_hash: None,
        },
        None,
        Vec::new(),
        now,
    )
    .map_err(|_| StatusCode::BAD_REQUEST)?;
    let lesson = Lesson {
        schema_version: DOMAIN_SCHEMA_VERSION,
        lesson_id: input.lesson_id.map(LessonId).unwrap_or_default(),
        origin: LessonOrigin::Operator,
        lifecycle: LessonLifecycle::Draft,
        title: input.title,
        statement: input.statement,
        rationale: input.rationale,
        recommended_behavior: input.recommended_behavior,
        exclusions: input.exclusions,
        scope: LessonScope {
            assets: input
                .assets
                .iter()
                .map(|value| Asset::try_from(value.as_str()))
                .collect::<std::result::Result<_, _>>()
                .map_err(|_| StatusCode::BAD_REQUEST)?,
            horizons: input
                .horizons
                .iter()
                .map(|value| parse_lesson_horizon(value))
                .collect::<std::result::Result<_, _>>()
                .map_err(|_| StatusCode::BAD_REQUEST)?,
            regimes: input.regimes.into_iter().collect(),
            decision_stages: input.decision_stages.into_iter().collect(),
        },
        source_refs: vec![ArtifactRef {
            artifact_id: source.artifact_id.clone(),
            kind: source.kind,
        }],
        supersedes: parse_lesson_refs_http(&input.supersedes)?,
        conflicts_with: parse_lesson_refs_http(&input.conflicts_with)?,
        confidence_ppm: input.confidence_ppm,
        authored_by: Some(input.authored_by),
        approved_by: None,
        created_at: now,
        updated_at: now,
        governance: Some(akzio_domain::LessonGovernance::operator_reviewed(now)),
    };
    let result = run_store_operation(
        daemon.store_executor.clone(),
        "store.lesson.add",
        move |store| store.write_lesson(&lesson, &source, now),
    )
    .await
    .map_err(|_| StatusCode::BAD_REQUEST)?;
    lesson_view_json(&result.lesson).map(Json)
}

async fn http_store_lesson(
    State(daemon): State<Arc<Daemon>>,
    Path(lesson_id): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<Json<serde_json::Value>, StatusCode> {
    // Lesson detail 是只读的 artifact/revision 投影。
    authorize(&daemon, &headers)?;
    let lesson = run_store_operation(
        daemon.store_executor.clone(),
        "store.lesson",
        move |store| store.lesson(&LessonId(lesson_id)),
    )
    .await?
    .ok_or(StatusCode::NOT_FOUND)?;
    lesson_view_json(&lesson).map(Json)
}

async fn http_store_lesson_usage(
    State(daemon): State<Arc<Daemon>>,
    Path(lesson_id): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<Json<LessonUsage>, StatusCode> {
    // Usage 读取 lesson 的预算/引用事实，不消耗或刷新使用次数。
    authorize(&daemon, &headers)?;
    run_store_operation(
        daemon.store_executor.clone(),
        "store.lesson.usage",
        move |store| store.lesson_usage(&LessonId(lesson_id)),
    )
    .await
    .map(Json)
}

async fn http_store_lesson_transition(
    State(daemon): State<Arc<Daemon>>,
    Path(lesson_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<StoreLessonTransitionRequest>,
) -> std::result::Result<Json<serde_json::Value>, StatusCode> {
    // Transition 把 actor/reason 送入 Store lifecycle gate；HTTP 成功只表示该状态转移已
    // 通过持久化校验，不代表它已影响 Decision/Execution。
    authorize(&daemon, &headers)?;
    let lesson = run_store_operation(
        daemon.store_executor.clone(),
        "store.lesson.transition",
        move |store| {
            store.transition_lesson(
                &LessonId(lesson_id),
                request.lifecycle,
                &request.actor,
                &request.reason,
                Utc::now(),
            )
        },
    )
    .await
    .map_err(|_| StatusCode::BAD_REQUEST)?;
    lesson_view_json(&lesson).map(Json)
}

fn parse_lesson_horizon(value: &str) -> std::result::Result<DecisionHorizon, ()> {
    // UI 字符串只允许三个明确 horizon，未知输入不默认到 T5。
    match value.trim().to_ascii_lowercase().as_str() {
        "t1" => Ok(DecisionHorizon::T1),
        "t3" => Ok(DecisionHorizon::T3),
        "t5" => Ok(DecisionHorizon::T5),
        _ => Err(()),
    }
}

fn parse_lesson_refs_http(values: &[String]) -> std::result::Result<Vec<ArtifactRef>, StatusCode> {
    // 把外部文本解析成 Lesson ArtifactRef；只建引用，不读取或复制目标 Artifact。
    values
        .iter()
        .map(|value| {
            Ok(ArtifactRef {
                artifact_id: ArtifactId(
                    ContentHash::new(value.trim()).map_err(|_| StatusCode::BAD_REQUEST)?,
                ),
                kind: ArtifactKind::Lesson,
            })
        })
        .collect()
}

fn lesson_view_json(value: &StoredLesson) -> std::result::Result<serde_json::Value, StatusCode> {
    // 将 Store 结构转成稳定 JSON view；serde 失败保持内部错误，不返回半截对象。
    serde_json::to_value(serde_json::json!({
        "artifact": &value.artifact,
        "lesson": &value.lesson,
        "revision": value.revision,
    }))
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

fn store_executor_json(executor: &StoreExecutor) -> serde_json::Value {
    // 读取 executor telemetry 的 snapshot；MaintenanceState 的 Completed 只代表维护任务
    // 结束，不重置或掩盖 lease deferral。
    let telemetry = executor.telemetry();
    let maintenance = match telemetry.maintenance {
        StoreMaintenanceState::Idle => serde_json::json!({ "state": "idle" }),
        StoreMaintenanceState::Running { kind, sequence } => serde_json::json!({
            "state": "running",
            "kind": kind.as_str(),
            "sequence": sequence,
        }),
        StoreMaintenanceState::Completed {
            kind,
            sequence,
            outcome,
            lease_deferral,
        } => serde_json::json!({
            "state": "completed",
            "kind": kind.as_str(),
            "sequence": sequence,
            "outcome": outcome.as_str(),
            "deferred_task_leases": lease_deferral.task_leases,
            "deferred_daemon_leases": lease_deferral.daemon_leases,
        }),
    };
    serde_json::json!({
        "accepting_operations": telemetry.accepting_operations,
        "queued_operation_count": telemetry.queued_operation_count,
        "completed_operation_count": telemetry.completed_operation_count,
        "last_queue_wait_micros": telemetry.last_queue_wait.as_micros(),
        "last_execution_duration_micros": telemetry.last_execution_duration.as_micros(),
        "maintenance": maintenance,
    })
}

fn store_json<T: Serialize>(
    result: std::result::Result<T, StoreError>,
) -> std::result::Result<Json<serde_json::Value>, StatusCode> {
    // 泛型输出统一经过 serde Value；Store error 与序列化 error 都 fail closed。
    result
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
        .and_then(|value| {
            serde_json::to_value(value)
                .map(Json)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
        })
}

async fn run_store_operation<T, F>(
    executor: StoreExecutor,
    operation: &'static str,
    work: F,
) -> std::result::Result<T, StatusCode>
where
    T: Send + 'static,
    F: FnOnce(Store) -> std::result::Result<T, StoreError> + Send + 'static,
{
    // `F: FnOnce + Send + 'static` 让一次 Store 操作拥有捕获值并进入串行 executor；外层
    // executor error 与内层 StoreError 分开记录，调用方只收到 HTTP status。
    executor
        .execute(work)
        .await
        .map_err(|error| {
            tracing::error!(operation, error = %error, "Store executor task failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .map_err(|error| {
            tracing::error!(operation, error = %error, "Store operation failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

async fn run_daemon_store_operation<T, F>(
    executor: StoreExecutor,
    work: F,
) -> std::result::Result<T, DaemonError>
where
    T: Send + 'static,
    F: FnOnce() -> std::result::Result<T, DaemonError> + Send + 'static,
{
    // daemon closure 在 StoreExecutor 线程执行，但不把 Store handle 暴露给 HTTP caller；
    // 两层 Result 用 `?` 保留 executor/runtime 错误。
    executor.execute(move |_| work()).await?
}

pub(super) async fn run_store_maintenance<T>(
    maintenance: crate::application::Maintenance,
    kind: StoreMaintenanceKind,
    work: impl FnOnce(Store) -> std::result::Result<T, StoreError> + Send + 'static,
) -> std::result::Result<T, StatusCode>
where
    T: Send + 'static,
{
    // 维护操作走独立 Maintenance executor，避免 backup/restore/doctor 与普通 mutation 互相
    // 穿插；返回值仍只描述该操作。
    maintenance.run(kind, work).await.map_err(|error| {
        tracing::error!(
            operation = kind.as_str(),
            error = %error,
            "Store maintenance task failed"
        );
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

fn authorize(daemon: &Daemon, headers: &HeaderMap) -> std::result::Result<(), StatusCode> {
    // 认证只接受精确 header 值；失败不泄露 token 对比细节，也不触发 Store/模型 I/O。
    headers
        .get("x-akzio-token")
        .and_then(|value| value.to_str().ok())
        .filter(|value| *value == daemon.transport.http_token)
        .map(|_| ())
        .ok_or(StatusCode::UNAUTHORIZED)
}

async fn http_store_events(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    Query(query): Query<StoreEventsQuery>,
    headers: HeaderMap,
) -> std::result::Result<Json<Vec<StoreEventView>>, StatusCode> {
    // Store events 是分页只读查询，after/limit 只限制投影，不推进 cursor。
    authorize(&daemon, &headers)?;
    run_store_operation(
        daemon.store_executor.clone(),
        "store.events",
        move |store| {
            store.events_after(
                &RunId(run_id),
                query.after.unwrap_or(0),
                query.limit.unwrap_or(EVENT_PAGE_SIZE),
            )
        },
    )
    .await
    .map(|events| {
        Json(
            events
                .into_iter()
                .map(|event| StoreEventView {
                    event_type: event.event_type,
                    artifact_id: event.artifact_id,
                })
                .collect(),
        )
    })
}

async fn http_repair_narrative(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<Json<serde_json::Value>, StatusCode> {
    // Repair handler 只入队 sealed T5 narrative repair task；返回 task_id 不是 repair 已执行
    // 或 learning 已通过的证明。
    authorize(&daemon, &headers)?;
    let operation = daemon.clone();
    run_daemon_store_operation(daemon.store_executor.clone(), move || {
        let task_id = operation
            .store
            .request_outcome_narrative_repair(&RunId(run_id), Utc::now())?;
        Ok(serde_json::json!({"task_id":task_id,"scope":"sealed_t5_narrative_only"}))
    })
    .await
    .map(Json)
    .map_err(invalid_input_or_conflict)
}
