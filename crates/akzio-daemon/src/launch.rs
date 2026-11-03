//! App/CLI HTTP 共用的正式 Run 提交入口。
//! 传输确认与业务完成分开；断开客户端不撤销已持久化的图或 Paper Session。

use super::*;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct FormalRunSubmission {
    pub run_id: RunId,
}

#[derive(Debug, Error)]
pub(crate) enum FormalRunLaunchError {
    #[error(transparent)]
    Rejected(#[from] DaemonError),
    #[error(
        "Run submission may have been committed; inspect the original Run/Session before retrying"
    )]
    Unknown {
        run_id: Option<RunId>,
        #[source]
        error: Box<DaemonError>,
    },
}

impl Daemon {
    /// Admission and tracking are atomic with shutdown. Dropping a client waiter
    /// does not abort the tracked submission or its post-commit audit.
    pub(crate) async fn submit_formal_run(
        &self,
        purpose: RunPurpose,
        source: &'static str,
    ) -> std::result::Result<FormalRunSubmission, FormalRunLaunchError> {
        let task = {
            let tracker = self.transport.submissions.lock().map_err(|_| {
                DaemonError::Unavailable("submission tracker is unavailable".into())
            })?;
            if tracker.is_closed() {
                return Err(DaemonError::Unavailable("Core is shutting down".into()).into());
            }
            let daemon = self.clone();
            tracker.spawn(async move { daemon.launch_formal_run(purpose, source).await })
        };
        task.await.map_err(|_| FormalRunLaunchError::Unknown {
            run_id: None,
            error: Box::new(DaemonError::Unavailable(
                "submission task ended without a known result".into(),
            )),
        })?
    }

    pub(crate) fn close_formal_submissions(&self) -> Result<tokio_util::task::TaskTracker> {
        let tracker =
            self.transport.submissions.lock().map_err(|_| {
                DaemonError::Unavailable("submission tracker is unavailable".into())
            })?;
        tracker.close();
        Ok(tracker.clone())
    }

    pub(crate) async fn launch_formal_run(
        &self,
        purpose: RunPurpose,
        source: &'static str,
    ) -> std::result::Result<FormalRunSubmission, FormalRunLaunchError> {
        self.ensure_run_log_ready()?;
        let operation = self.clone();
        let canonical = self
            .store_executor
            .execute(move |_| operation.store.debug_environment())
            .await
            .map_err(DaemonError::from)?
            .map_err(DaemonError::from)?
            .is_none();
        if !canonical {
            return Err(
                DaemonError::InvalidInput("formal runs require the canonical Core".into()).into(),
            );
        }
        let (run_id, newly_created) = match purpose {
            RunPurpose::PositionPlan => {
                let operation = self.clone();
                let run_id = self
                    .store_executor
                    .execute(
                        move |store| -> std::result::Result<_, FormalRunLaunchError> {
                            operation.ensure_run_log_ready()?;
                            let now = Utc::now();
                            let session = now
                                .with_timezone(&chrono_tz::America::New_York)
                                .date_naive()
                                .to_string();
                            let (workflow, setup) =
                                operation.prepare_position_plan(&session, now)?;
                            if let Err(error) = store.commit_position_plan(&workflow, &setup) {
                                return Err(FormalRunLaunchError::Unknown {
                                    run_id: store
                                        .workflow_snapshot(&workflow.run.run_id)
                                        .ok()
                                        .map(|snapshot| snapshot.run.run_id),
                                    error: Box::new(error.into()),
                                });
                            }
                            Ok(workflow.run.run_id)
                        },
                    )
                    .await
                    .map_err(|error| FormalRunLaunchError::Unknown {
                        run_id: None,
                        error: Box::new(error.into()),
                    })??;
                (run_id, true)
            }
            RunPurpose::Paper => {
                let paper = self.paper.paper_observer.clone().ok_or_else(|| {
                    DaemonError::InvalidInput(
                        "完整模式尚未启用：配置 daemon.manual_paper=true 后重启 Core".into(),
                    )
                })?;
                if self.paper.runtime_identity_hash.is_none() {
                    return Err(DaemonError::InvalidInput(
                        "Paper runtime identity is unavailable".into(),
                    )
                    .into());
                }
                let clock = AlpacaPaperSessionClock::new(paper);
                let reservation = self
                    .paper
                    .scheduler
                    .tick(&clock, &self.paper_workflow_source(), Utc::now())
                    .await
                    .map_err(|error| match error {
                        SchedulerError::SubmissionUnknown { run_id, error } => {
                            FormalRunLaunchError::Unknown {
                                run_id,
                                error: Box::new((*error).into()),
                            }
                        }
                        error => FormalRunLaunchError::Rejected(error.into()),
                    })?
                    .ok_or_else(|| {
                        DaemonError::InvalidInput(
                            "Paper 暂未启动：请检查交易时段、当前审批及校准状态；未创建订单".into(),
                        )
                    })?;
                (
                    reservation.slot.workflow.run.run_id,
                    reservation.newly_reserved,
                )
            }
            _ => {
                return Err(
                    DaemonError::InvalidInput("仅支持 PositionPlan 或 Paper".into()).into(),
                );
            }
        };
        if newly_created {
            if let Some(logger) = &self.run_log {
                let scope = akzio_model::ModelAuditContext {
                    run_id: run_id.0.clone(),
                    phase: "T0".into(),
                    task_id: None,
                    attempt_id: None,
                    call_id: None,
                    retry_index: None,
                };
                logger
                    .record(
                        &scope,
                        "rust.run.submitted",
                        serde_json::json!({"purpose":purpose,"source":source}),
                    )
                    .map_err(|error| FormalRunLaunchError::Unknown {
                        run_id: Some(run_id.clone()),
                        error: Box::new(error.into()),
                    })?;
            }
        }
        Ok(FormalRunSubmission { run_id })
    }
}
