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
    match error {
        StoreError::MissingRun(_) => StatusCode::NOT_FOUND,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
