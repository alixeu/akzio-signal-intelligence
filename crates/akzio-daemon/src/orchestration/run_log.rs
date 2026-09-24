use super::*;

impl Daemon {
    /// Seals only actual durable stage boundaries; an Outcome's wait/poll is
    /// not a finished T1/T3/T5, and a terminal T0 is not a broker fill.
    pub fn archive_ready_run_stages(&self) -> Result<()> {
        let Some(logger) = &self.run_log else {
            return Ok(());
        };
        for (run_id, phase) in logger.pending()? {
            if logger.has_active_task(&run_id, &phase)? {
                continue;
            }
            let snapshot = self.store.workflow_snapshot(&run_id)?;
            let status = if phase == "T0" {
                matches!(
                    snapshot.status,
                    WorkflowStatus::Completed
                        | WorkflowStatus::DecisionCompleted
                        | WorkflowStatus::CompletedWithExecutionRejection
                        | WorkflowStatus::Failed
                        | WorkflowStatus::Cancelled
                )
                .then(|| format!("{:?}", snapshot.status).to_ascii_lowercase())
            } else {
                let mut outcome_tasks = snapshot.tasks.iter().filter(|task| {
                    task.node.recipe_id.as_str() == akzio_domain::LEARNING_OUTCOME_WORKER_RECIPE_ID
                });
                let outcome_busy = outcome_tasks
                    .clone()
                    .any(|task| matches!(task.status, TaskStatus::Running | TaskStatus::Leased));
                if outcome_busy {
                    continue;
                }
                let segment_started_at = logger.segment_started_at(&run_id, &phase)?;
                let mut retrospective_committed = false;
                for artifact in self.store.retrospectives(&run_id)? {
                    if artifact.created_at < segment_started_at {
                        continue;
                    }
                    let retrospective: Retrospective =
                        serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                    retrospective.validate()?;
                    if format!("{:?}", retrospective.horizon).eq_ignore_ascii_case(&phase) {
                        retrospective_committed = true;
                        break;
                    }
                }
                if retrospective_committed {
                    Some(if phase == "T5" { "sealed" } else { "partial" }.to_owned())
                } else if outcome_tasks
                    .clone()
                    .any(|task| task.status == TaskStatus::Failed)
                {
                    Some("failed".to_owned())
                } else if outcome_tasks.any(|task| task.status == TaskStatus::Cancelled) {
                    Some("cancelled".to_owned())
                } else {
                    None
                }
            };
            let Some(status) = status else {
                continue;
            };
            self.archive_one(logger, &run_id, &phase, snapshot.event_cursor, &status)?;
        }
        Ok(())
    }

    /// Called only after HTTP, scheduler and worker futures have joined on a
    /// normal shutdown, so no writer can append to a segment being sealed.
    pub fn archive_remaining_after_shutdown(&self) -> Result<()> {
        self.archive_ready_run_stages()?;
        let Some(logger) = &self.run_log else {
            return Ok(());
        };
        for (run_id, phase) in logger.pending()? {
            let cursor = self.store.workflow_snapshot(&run_id)?.event_cursor;
            self.archive_one(logger, &run_id, &phase, cursor, "stopped_incomplete")?;
        }
        Ok(())
    }

    fn archive_one(
        &self,
        logger: &RunLogger,
        run_id: &RunId,
        phase: &str,
        ceiling: i64,
        status: &str,
    ) -> Result<()> {
        let start_cursor = logger.flow_after(run_id, phase)?;
        let receipt =
            match logger.seal_with_store(run_id, phase, status, &self.store, start_cursor, ceiling)
            {
                Ok(Some(receipt)) => receipt,
                Ok(None) => return Ok(()),
                Err(error) => {
                    let _ = self.store.record_run_log_status(
                        run_id,
                        phase,
                        "blocked",
                        &serde_json::json!({"reason": error.to_string()}),
                    );
                    return Err(error.into());
                }
            };
        if let Err(error) = self.store.record_run_log_status(
            run_id,
            phase,
            "archived",
            &serde_json::to_value(&receipt)?,
        ) {
            logger.mark_unavailable("Store could not confirm Run archive")?;
            return Err(error.into());
        }
        if let Err(error) = logger.confirm_after_store(&self.store, &receipt) {
            logger.mark_unavailable("Run ZIP cleanup or confirmation failed")?;
            return Err(error.into());
        }
        Ok(())
    }

    pub async fn supervise_run_archives(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let daemon = self.clone();
                    match tokio::task::spawn_blocking(move || daemon.archive_ready_run_stages()).await {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => tracing::error!(%error, "Run archive blocked; new work remains paused"),
                        Err(error) => return Err(DaemonError::Unavailable(format!("archive worker panicked: {error}"))),
                    }
                },
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
        }
    }
}
