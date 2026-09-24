// 文件导读：Native App 的 `/runs` 入口只复用正式 PositionPlan graph 或 Paper scheduler。
// 它拒绝浏览器来源和隔离 Debug Core；PositionPlan 在 Decision 后结束，Paper 只返回
// scheduler reservation 的 RunId。返回 RunId/HTTP 成功不等于模型完成、Decision 通过、
// Paper submission、fill 或 Outcome 成熟。
// Rust 机制：Axum `State/Json/HeaderMap` extractor 取得请求；闭包 move 进 StoreExecutor
// 取得原子 commit；`match` 穷举 RunPurpose，`Option` 表示 scheduler 尚未创建 slot。
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunLaunchRequest {
    purpose: RunPurpose,
}

async fn http_launch_run(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
    Json(request): Json<RunLaunchRequest>,
) -> DebugHttpResult {
    authorize(&daemon, &headers).map_err(debug_auth_error)?;
    if headers.contains_key("origin") || headers.get("sec-fetch-site").is_some_and(|v| v != "none")
    {
        return Err((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error":"native_client_required"})),
        ));
    }
    if daemon.debug_enabled() {
        return Err((
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error":"日常运行需要连接正式 Core；当前为隔离 Debug Core"})),
        ));
    }
    let run_id = match request.purpose {
        RunPurpose::PositionPlan => {
            let operation = daemon.clone();
            run_daemon_store_operation(daemon.store_executor.clone(), move || {
                let now = Utc::now();
                let session = now
                    .with_timezone(&chrono_tz::America::New_York)
                    .date_naive()
                    .to_string();
                let (workflow, setup) = operation.prepare_position_plan(&session, now)?;
                operation.store.commit_position_plan(&workflow, &setup)?;
                Ok(workflow.run.run_id)
            })
            .await
            .map_err(debug_http_error)?
        }
        RunPurpose::Paper => {
            let paper = daemon.paper.paper_observer.clone().ok_or_else(|| {
                debug_http_error(DaemonError::InvalidInput(
                    "完整模式尚未启用：配置 daemon.manual_paper=true 后重启 Core".into(),
                ))
            })?;
            if daemon.paper.runtime_identity_hash.is_none() {
                return Err(debug_http_error(DaemonError::InvalidInput(
                    "Paper runtime identity is unavailable".into(),
                )));
            }
            let clock = AlpacaPaperSessionClock::new(paper);
            let reservation = daemon
                .paper
                .scheduler
                .tick(&clock, &daemon.paper_workflow_source(), Utc::now())
                .await
                .map_err(|error| debug_http_error(error.into()))?;
            reservation
                .ok_or_else(|| {
                    debug_http_error(DaemonError::InvalidInput(
                        "Paper 暂未启动：请检查交易时段、当前审批及校准状态；未创建订单".into(),
                    ))
                })?
                .slot
                .workflow
                .run
                .run_id
        }
        _ => {
            return Err(debug_http_error(DaemonError::InvalidInput(
                "仅支持 PositionPlan 或 Paper".into(),
            )));
        }
    };
    Ok(Json(serde_json::json!({"run_id":run_id})))
}
