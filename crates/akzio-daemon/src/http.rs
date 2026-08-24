//! Authenticated loopback HTTP transport.

use super::*;
use crate::observer::{
    ObserverPortfolioHistory, ObserverPortfolioRange, ObserverRunDetail, ObserverSection,
    ObserverSnapshot,
};

#[derive(Debug, Deserialize)]
struct ObserverPortfolioHistoryQuery {
    range: ObserverPortfolioRange,
}

impl Daemon {
    pub fn router(&self) -> Router {
        Router::new()
            .route("/health", get(http_health))
            .route("/ready", get(http_ready))
            .route("/v1/observer/snapshot", get(http_observer_snapshot))
            .route("/v1/observer/runs/{run_id}", get(http_observer_run))
            .route(
                "/v1/observer/portfolio/history",
                get(http_observer_portfolio_history),
            )
            .route("/v1/observer/events", get(http_observer_events))
            .route("/runs/{run_id}/events", get(http_events))
            .route("/runs/{run_id}/trajectory", get(http_trajectory))
            .route("/runs/{run_id}/retrospectives", get(http_retrospectives))
            .route("/runs/{run_id}/replay", get(http_replay))
            .route("/runs", post(http_submit))
            .route("/runs/{run_id}/cancel", post(http_cancel))
            .route("/runs/{run_id}/retry", post(http_retry))
            .route("/control/freeze", post(http_freeze))
            .route("/control/unfreeze", post(http_unfreeze))
            .with_state(Arc::new(self.clone()))
    }

    pub async fn serve_http(
        &self,
        address: SocketAddr,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        if !address.ip().is_loopback() {
            return Err(DaemonError::InvalidInput(
                "daemon HTTP control API must bind a loopback address".to_owned(),
            ));
        }
        let listener = TcpListener::bind(address).await?;
        axum::serve(listener, self.router())
            .with_graceful_shutdown(wait_for_shutdown(shutdown))
            .await
            .map_err(DaemonError::Io)
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            return;
        }
    }
}

async fn http_health(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
) -> std::result::Result<Json<DaemonHealth>, StatusCode> {
    authorize(&daemon, &headers)?;
    daemon
        .health()
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn http_ready(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
) -> std::result::Result<Json<DaemonHealth>, StatusCode> {
    authorize(&daemon, &headers)?;
    daemon.ready().map(Json).map_err(|error| match error {
        DaemonError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    })
}

async fn http_observer_snapshot(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
) -> std::result::Result<Json<ObserverSnapshot>, StatusCode> {
    authorize_observer(&daemon, &headers)?;
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
    authorize_observer(&daemon, &headers)?;
    let run_id = RunId(run_id);
    daemon
        .observer_run_detail(&run_id)
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
    authorize_observer(&daemon, &headers)?;
    Ok(Json(daemon.observer_portfolio_history(query.range).await))
}

async fn http_observer_events(
    State(daemon): State<Arc<Daemon>>,
    Query(query): Query<EventQuery>,
    headers: HeaderMap,
) -> std::result::Result<
    Sse<impl futures::Stream<Item = std::result::Result<Event, Infallible>>>,
    StatusCode,
> {
    authorize_observer(&daemon, &headers)?;
    let mut cursor = query.after.unwrap_or(0);
    let mut reasoning_events = daemon.reasoning_events.subscribe();
    let mut poll = tokio::time::interval(std::time::Duration::from_millis(500));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let stream = stream! {
        loop {
            tokio::select! {
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
                _ = poll.tick() => match daemon.store.event_cursor() {
                    Ok(next) if next > cursor => {
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
                    Ok(_) => yield Ok(Event::default().comment("keepalive")),
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
) -> std::result::Result<
    Sse<impl futures::Stream<Item = std::result::Result<Event, Infallible>>>,
    StatusCode,
> {
    authorize(&daemon, &headers)?;
    let run_id = RunId(run_id);
    let mut cursor = query.after.unwrap_or(0);
    let mut reasoning_events = daemon.reasoning_events.subscribe();
    let mut poll = tokio::time::interval(std::time::Duration::from_millis(200));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let stream = stream! {
        loop {
            tokio::select! {
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
                _ = poll.tick() => match daemon.store.events_after(&run_id, cursor, EVENT_PAGE_SIZE) {
                    Ok(events) if events.is_empty() => {
                        yield Ok(Event::default().comment("keepalive"));
                    }
                    Ok(events) => {
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
                    Err(error) => {
                        yield Ok(Event::default().event("error").data(error.to_string()));
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
    authorize(&daemon, &headers)?;
    daemon
        .replay_report(&RunId(run_id))
        .map(Json)
        .map_err(invalid_input_or_conflict)
}

async fn http_trajectory(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<Json<Vec<TrajectoryEntry>>, StatusCode> {
    authorize(&daemon, &headers)?;
    daemon
        .trajectory(&RunId(run_id))
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn http_retrospectives(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<Json<Vec<RetrospectiveView>>, StatusCode> {
    authorize(&daemon, &headers)?;
    daemon
        .retrospectives(&RunId(run_id))
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn http_submit(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
    Json(request): Json<SubmitRequest>,
) -> std::result::Result<Json<RunSubmissionResponse>, StatusCode> {
    authorize(&daemon, &headers)?;
    daemon
        .submit_default(request.purpose)
        .map(|run_id| Json(RunSubmissionResponse { run_id }))
        .map_err(invalid_input_or_internal)
}

async fn http_cancel(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<Json<RunCancellationResponse>, StatusCode> {
    authorize(&daemon, &headers)?;
    let run_id = RunId(run_id);
    daemon
        .request_cancel(&run_id, "operator cancellation request")
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
    authorize(&daemon, &headers)?;
    let source_run_id = RunId(run_id);
    daemon
        .retry_run(&source_run_id)
        .map(|run_id| {
            Json(RunRetryResponse {
                source_run_id,
                run_id,
            })
        })
        .map_err(invalid_input_or_conflict)
}

fn invalid_input_or_internal(error: DaemonError) -> StatusCode {
    if matches!(error, DaemonError::InvalidInput(_)) {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

fn invalid_input_or_conflict(error: DaemonError) -> StatusCode {
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
    authorize(&daemon, &headers)?;
    daemon
        .set_freeze(true, request.reason)
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn http_unfreeze(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
    Json(request): Json<FreezeRequest>,
) -> std::result::Result<Json<DaemonHealth>, StatusCode> {
    authorize(&daemon, &headers)?;
    daemon
        .set_freeze(false, request.reason)
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

fn authorize(daemon: &Daemon, headers: &HeaderMap) -> std::result::Result<(), StatusCode> {
    headers
        .get("x-akzio-token")
        .and_then(|value| value.to_str().ok())
        .filter(|value| *value == daemon.http_token)
        .map(|_| ())
        .ok_or(StatusCode::UNAUTHORIZED)
}

fn authorize_observer(daemon: &Daemon, headers: &HeaderMap) -> std::result::Result<(), StatusCode> {
    headers
        .get("x-akzio-observer-token")
        .and_then(|value| value.to_str().ok())
        .zip(daemon.observer_token.as_deref())
        .filter(|(provided, expected)| provided == expected)
        .map(|_| ())
        .ok_or(StatusCode::UNAUTHORIZED)
}
