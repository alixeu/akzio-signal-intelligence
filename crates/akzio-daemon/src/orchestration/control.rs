// 文件导读：control 暴露 scheduler/worker 的启动、取消、retry、replay 和 freeze 操作。
// retry 只允许明确的 PositionPlan，Paper 由 scheduler/Session slot 管理；serve 成功只
// 表示后台服务存活，Run、Decision、Paper submission、fill 和 Outcome 仍由各自事件证明。
// 调度器与 Worker 共用停止信号。Paper 入口使用 Core 持有的 Broker 会话时钟。

use super::*;

impl Daemon {
    pub async fn serve_with_paper_scheduler(
        &self,
        source: &StorePaperWorkflowSource,
        poll_interval: std::time::Duration,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        if !self.paper.auto_paper {
            return Err(DaemonError::InvalidInput(
                "Paper scheduler requires auto_paper=true".to_owned(),
            ));
        }
        let clock = self.paper.session_clock.as_ref().ok_or_else(|| {
            DaemonError::InvalidInput("Paper scheduler clock was not initialized".to_owned())
        })?;
        let scheduler_shutdown = shutdown.clone();
        tokio::try_join!(
            async move {
                self.paper
                    .scheduler
                    .serve(clock, source, poll_interval, scheduler_shutdown)
                    .await
                    .map_err(DaemonError::from)
            },
            self.serve_worker_pool(shutdown),
        )?;
        Ok(())
    }

    pub(crate) async fn request_cancel(&self, run_id: &RunId, reason: &str) -> Result<u64> {
        // 将取消意图写入 TaskRuntime/Store 并返回被标记的任务数；当前外部调用是否可取消
        // 以及已提交副作用如何恢复仍由各 handler 的边界决定。
        Ok(u64::from(
            self.task_runtime
                .request_cancel(run_id, reason, Utc::now())
                .await?,
        ))
    }

    pub(crate) fn retry_run(&self, source_run_id: &RunId) -> Result<RunId> {
        // retry 是新 PositionPlan Run，不是复用失败 Attempt；先拒绝退休/非终态身份，再用
        // 当前受控 proposal 创建新 graph，并由 Store 原子发布 graph 与 34 项研究 Need。
        self.ensure_run_log_ready()?;
        self.store.assert_workflow_executable(source_run_id)?;
        match self.store.run_purpose(source_run_id)? {
            RunPurpose::PositionPlan => {}
            RunPurpose::Paper => {
                return Err(DaemonError::InvalidInput(
                    "Paper runs are scheduler-owned and cannot be retried by an operator"
                        .to_owned(),
                ));
            }
            RunPurpose::Debug
            | RunPurpose::PaperDryRun
            | RunPurpose::Replay
            | RunPurpose::Shadow => {
                return Err(DaemonError::InvalidInput(
                    "only PositionPlan runs may be retried by an operator".to_owned(),
                ));
            }
        }
        let source = self.workflow.replay_run(source_run_id)?;
        if !matches!(
            source.status,
            WorkflowStatus::Completed
                | WorkflowStatus::CompletedWithExecutionRejection
                | WorkflowStatus::Failed
                | WorkflowStatus::Cancelled
        ) {
            return Err(
                akzio_runtime::RuntimeError::RetryRunNotTerminal(source_run_id.clone()).into(),
            );
        }
        let now = Utc::now();
        let session = now
            .with_timezone(&chrono_tz::America::New_York)
            .date_naive()
            .to_string();
        let (workflow, setup) = self.prepare_position_plan(RunId::new(), &session, now)?;
        self.store.commit_position_plan(&workflow, &setup)?;
        if let Some(logger) = &self.run_log {
            logger.record(
                &akzio_model::ModelAuditContext {
                    run_id: workflow.run.run_id.0.clone(),
                    phase: "T0".into(),
                    task_id: None,
                    attempt_id: None,
                    call_id: None,
                    retry_index: None,
                },
                "rust.run.submitted",
                serde_json::json!({"purpose":"position_plan","source":"run.retry",
                    "source_run_id":source_run_id}),
            )?;
        }
        Ok(workflow.run.run_id)
    }

    pub(crate) fn replay_report(&self, run_id: &RunId) -> Result<ReplayReport> {
        // 只从 durable workflow 和 lifecycle health 生成计数投影；Failed/Cancelled 也属于
        // terminal_task_count，不等于成功节点或成功 Run。
        let (snapshot, lifecycle) =
            self.workflow.with_replayed_run(run_id, |snapshot, store| {
                #[cfg(test)]
                replay_tests::before_lifecycle();
                Ok((snapshot, store.run_lifecycle_health(run_id)?))
            })?;
        Ok(ReplayReport {
            lifecycle: Some(lifecycle),
            run_id: snapshot.run.run_id,
            purpose: snapshot.run.purpose,
            status: snapshot.status,
            revision: snapshot.revision.revision,
            task_count: snapshot.tasks.len(),
            terminal_task_count: snapshot
                .tasks
                .iter()
                .filter(|task| {
                    matches!(
                        task.status,
                        TaskStatus::Succeeded
                            | TaskStatus::Failed
                            | TaskStatus::Cancelled
                            | TaskStatus::Skipped
                    )
                })
                .count(),
            event_cursor: snapshot.event_cursor,
            cancel_requested: snapshot.cancel_requested,
        })
    }

    pub(crate) fn retrospectives(&self, run_id: &RunId) -> Result<Vec<RetrospectiveView>> {
        // 只有 Paper purpose 暴露历史叙事；每个 payload 在输出前重新 validate，坏条目使
        // 整个查询返回错误，不被悄悄当作空数组。
        let run_purpose = self.store.run_purpose(run_id)?;
        if !matches!(run_purpose, RunPurpose::Paper) {
            return Ok(Vec::new());
        }
        self.store
            .retrospectives(run_id)?
            .into_iter()
            .map(|artifact| {
                let payload: Retrospective =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                payload.validate().map_err(|error| {
                    DaemonError::InvalidInput(format!(
                        "invalid retrospective crossed query gate: {error}"
                    ))
                })?;
                Ok(RetrospectiveView {
                    artifact_id: artifact.artifact_id,
                    payload,
                })
            })
            .collect()
    }

    pub(crate) fn trajectory(&self, run_id: &RunId) -> Result<Vec<TrajectoryEntry>> {
        // 读取同一 Run 的持久化轨迹；该查询不调用模型，也不触发新的评估。
        Ok(self.store.trajectory(run_id)?)
    }

    pub(crate) fn set_freeze(&self, frozen: bool, reason: String) -> Result<DaemonHealth> {
        // 先追加 freeze state Artifact，再从 Store 重新读取生成 health；返回健康视图不代表
        // 被取消的在途请求已撤销或订单已补偿。
        self.store.write_freeze_state(frozen, reason, Utc::now())?;
        self.health()
    }
}

#[cfg(test)]
mod replay_tests {
    use super::*;
    use std::cell::RefCell;
    use std::sync::mpsc::{channel, RecvTimeoutError};

    thread_local! {
        static BEFORE_LIFECYCLE: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    }

    pub(super) fn before_lifecycle() {
        if let Some(write) = BEFORE_LIFECYCLE.with(|hook| hook.borrow_mut().take()) {
            write();
        }
    }

    /// WHY THIS TEST EXISTS (Task #21): A concurrent journal append must not mix Replay's event cursor with a newer lifecycle state.
    /// Do not weaken or delete this assertion.
    #[tokio::test]
    async fn replay_report_uses_one_watermark_during_a_journal_append() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/replay-snapshot-tests")
            .join(RunId::new().0);
        let daemon = Daemon::with_model(
            DaemonConfig {
                agent_budget: Default::default(),
                research_settings: Default::default(),
                outcome_processing: false,
                store_root: root.clone(),
                http_token: "replay-fixture-token".into(),
                worker_count: 1,
                auto_paper: false,
                market_data_feed: None,
                outcome_cost_model: akzio_domain::OutcomeCostModel {
                    transaction_cost_ppm: 0,
                    slippage_ppm: 0,
                },
                decision_policy: Default::default(),
                runtime_identity_hash: None,
                historical_evaluation_condition: None,
                model_knowledge_cutoff: None,
                run_log: None,
            },
            crate::fixture_model_client(),
        )
        .unwrap();
        let run = daemon
            .launch_formal_run(
                RunPurpose::PositionPlan,
                None,
                None,
                "replay-snapshot-fixture",
            )
            .await
            .unwrap()
            .run_id;
        let before = daemon.replay_report(&run).unwrap();
        assert_eq!(before.status, WorkflowStatus::Queued);
        let writer = Store::open(&root).unwrap();
        let (start, read_has_replayed) = channel();
        let (completed, writer_finished) = channel();
        let writer_run = run.clone();
        let writer = std::thread::spawn(move || {
            read_has_replayed.recv().unwrap();
            let log = writer
                .record_run_log_status(&writer_run, "T0", "opened", &serde_json::json!({}))
                .unwrap();
            let claimed = writer
                .claim_next_task_for_workload(
                    "replay-snapshot-writer",
                    Utc::now(),
                    Duration::minutes(1),
                    akzio_store::TaskWorkload::Any,
                )
                .unwrap()
                .unwrap();
            assert_eq!(claimed.run_id, writer_run);
            let _ = completed.send(());
            log.artifact_id
        });
        BEFORE_LIFECYCLE.with(|hook| {
            hook.replace(Some(Box::new(move || {
                start.send(()).unwrap();
                // Rollback-journal SQLite can wait to commit until the read snapshot is released.
                match writer_finished.recv_timeout(std::time::Duration::from_secs(1)) {
                    Ok(()) | Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => panic!("concurrent writer failed"),
                }
            })));
        });
        let during = daemon.replay_report(&run);
        BEFORE_LIFECYCLE.with(|hook| drop(hook.take()));
        let writer_result = writer.join();
        let during = during.unwrap();
        let log_id = writer_result.unwrap();
        assert_eq!(during.event_cursor, before.event_cursor);
        assert_eq!(during.revision, before.revision);
        assert_eq!(during.status, WorkflowStatus::Queued);
        assert_eq!(
            during.lifecycle.unwrap().execution_status,
            WorkflowStatus::Queued,
            "lifecycle and replay must read the same watermark"
        );

        let after = daemon.replay_report(&run).unwrap();
        assert!(after.event_cursor > during.event_cursor);
        assert_eq!(after.status, WorkflowStatus::Running);
        assert_eq!(after.lifecycle.unwrap().execution_status, after.status);
        assert!(daemon
            .store
            .events_after(&run, during.event_cursor, 100)
            .unwrap()
            .iter()
            .any(|event| event.artifact_id.as_ref() == Some(&log_id)));
    }
}
