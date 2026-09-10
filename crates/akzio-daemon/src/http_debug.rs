#[derive(Debug, Deserialize)]
struct DebugInspectQuery {
    task: Option<TaskId>,
    attempt: Option<akzio_domain::AttemptId>,
}

fn authorize_debug_control(
    daemon: &Daemon,
    headers: &HeaderMap,
) -> std::result::Result<(), StatusCode> {
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
    (
        status,
        Json(
            serde_json::json!({"error":"debug control requires authenticated native client and isolated Debug Core"}),
        ),
    )
}

async fn http_debug_runs(State(daemon): State<Arc<Daemon>>, headers: HeaderMap) -> DebugHttpResult {
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
