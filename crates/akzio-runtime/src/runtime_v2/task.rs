use super::*;

#[derive(Debug, Clone)]
pub struct TaskRuntime {
    store: V2Store,
    lease_duration: Duration,
}

impl TaskRuntime {
    pub fn new(store: V2Store) -> Self {
        Self {
            store,
            lease_duration: Duration::seconds(30),
        }
    }

    #[cfg(test)]
    pub(crate) fn store(&self) -> &V2Store {
        &self.store
    }

    pub fn with_lease_duration(mut self, lease_duration: Duration) -> RuntimeResult<Self> {
        if lease_duration <= Duration::zero() {
            return Err(RuntimeError::InvalidTaskLeaseDuration);
        }
        self.lease_duration = lease_duration;
        Ok(self)
    }

    pub fn recover_expired_tasks(&self, now: DateTime<Utc>) -> RuntimeResult<u64> {
        Ok(self.store.recover_expired_tasks(now)?)
    }

    /// Request cooperative cancellation through the Store-owned task state
    /// machine. A worker observes the durable flag between heartbeats.
    pub fn request_cancel(
        &self,
        run_id: &RunId,
        reason: &str,
        now: DateTime<Utc>,
    ) -> RuntimeResult<bool> {
        Ok(self.store.request_run_cancel(run_id, reason, now)?)
    }

    pub async fn run_one<F, Fut>(&self, worker_id: &str, handle: F) -> RuntimeResult<bool>
    where
        F: FnOnce(ClaimedAttempt) -> Fut,
        Fut: Future<Output = TaskCompletion>,
    {
        let now = Utc::now();
        self.store.recover_expired_tasks(now)?;
        let Some(task) = self
            .store
            .claim_next_task(worker_id, now, self.lease_duration)?
        else {
            return Ok(false);
        };
        if self.store.run_cancel_requested(&task.run_id)? {
            self.store
                .finish_task(&task.permit, TaskStatus::Cancelled, Utc::now())?;
            return Ok(true);
        }

        let heartbeat_millis = u64::try_from(self.lease_duration.num_milliseconds())
            .map_err(|_| RuntimeError::InvalidTaskLeaseDuration)?
            .saturating_div(3)
            .max(1);
        let mut heartbeat = tokio::time::interval(StdDuration::from_millis(heartbeat_millis));
        heartbeat.tick().await;
        let mut handler = Box::pin(handle(task.clone()));
        let timeout = tokio::time::sleep(StdDuration::from_secs(u64::from(
            task.node.budget.max_wall_time_secs,
        )));
        tokio::pin!(timeout);
        let completion = loop {
            tokio::select! {
                result = &mut handler => break result,
                _ = heartbeat.tick() => {
                    if self.store.run_cancel_requested(&task.run_id)? {
                        break TaskCompletion::Cancelled;
                    }
                    self.store.heartbeat_task(
                        &task.permit,
                        Utc::now() + self.lease_duration,
                    )?;
                }
                _ = &mut timeout => break TaskCompletion::Retry(RetryCause::Timeout),
            }
        };
        self.finish(&task, completion, Utc::now())?;
        Ok(true)
    }

    pub(super) fn finish(
        &self,
        task: &ClaimedAttempt,
        completion: TaskCompletion,
        now: DateTime<Utc>,
    ) -> RuntimeResult<()> {
        match completion {
            TaskCompletion::Succeeded(artifacts) => {
                self.store
                    .commit_attempt(&task.permit, &artifacts, TaskStatus::Succeeded, now)?;
            }
            TaskCompletion::NoOutput => {
                self.store
                    .finish_task(&task.permit, TaskStatus::Succeeded, now)?;
            }
            TaskCompletion::Committed => {
                self.store
                    .verify_attempt_terminal(&task.permit, TaskStatus::Succeeded)?;
            }
            TaskCompletion::Failed => {
                self.store
                    .finish_task(&task.permit, TaskStatus::Failed, now)?;
            }
            TaskCompletion::Skipped => {
                self.store
                    .finish_task(&task.permit, TaskStatus::Skipped, now)?;
            }
            TaskCompletion::Cancelled => {
                self.store
                    .finish_task(&task.permit, TaskStatus::Cancelled, now)?;
            }
            TaskCompletion::DeferredUntil(ready_at) => {
                self.store.defer_task(&task.permit, ready_at, now)?;
            }
            TaskCompletion::Retry(cause) => {
                if self.retry_allowed(task, cause) {
                    let retry_at = self.retry_at(task, now)?;
                    match self.store.retry_task(&task.permit, retry_at, now)? {
                        RetryTaskResult::Requeued | RetryTaskResult::Terminal(_) => {}
                    }
                } else {
                    self.store
                        .finish_task(&task.permit, TaskStatus::Failed, now)?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn retry_allowed(&self, task: &ClaimedAttempt, cause: RetryCause) -> bool {
        match cause {
            RetryCause::Transport | RetryCause::Timeout => task.node.retry.retry_transport,
            RetryCause::RateLimited => task.node.retry.retry_rate_limited,
            RetryCause::InvalidOutput => task.node.retry.retry_invalid_output,
        }
    }

    pub(super) fn retry_at(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> RuntimeResult<DateTime<Utc>> {
        let milliseconds = i64::try_from(task.node.retry.initial_backoff_ms)
            .map_err(|_| RuntimeError::InvalidRetryBackoff)?;
        now.checked_add_signed(Duration::milliseconds(milliseconds))
            .ok_or(RuntimeError::InvalidRetryBackoff)
    }
}
