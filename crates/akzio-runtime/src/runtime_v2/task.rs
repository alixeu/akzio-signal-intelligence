use super::*;

#[derive(Debug, Clone)]
pub struct TaskRuntime {
    #[cfg(test)]
    store: V2Store,
    store_executor: StoreExecutor,
    lease_duration: Duration,
}

impl TaskRuntime {
    pub fn new(store: V2Store) -> Self {
        Self {
            #[cfg(test)]
            store: store.clone(),
            store_executor: StoreExecutor::new(store),
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

    pub fn with_store_executor(mut self, store_executor: StoreExecutor) -> Self {
        self.store_executor = store_executor;
        self
    }

    pub async fn recover_expired_tasks(&self, now: DateTime<Utc>) -> RuntimeResult<u64> {
        Ok(self
            .store_executor
            .execute(move |store| store.recover_expired_tasks(now))
            .await??)
    }

    pub fn recovery_interval(&self) -> RuntimeResult<StdDuration> {
        Ok(StdDuration::from_millis(self.lease_tick_millis()?))
    }

    /// Request cooperative cancellation through the Store-owned task state
    /// machine. A worker observes the durable flag between heartbeats.
    pub async fn request_cancel(
        &self,
        run_id: &RunId,
        reason: &str,
        now: DateTime<Utc>,
    ) -> RuntimeResult<bool> {
        let run_id = run_id.clone();
        let reason = reason.to_owned();
        Ok(self
            .store_executor
            .execute(move |store| store.request_run_cancel(&run_id, &reason, now))
            .await??)
    }

    async fn cancel_requested(&self, run_id: &RunId) -> RuntimeResult<bool> {
        let run_id = run_id.clone();
        Ok(self
            .store_executor
            .execute(move |store| store.run_cancel_requested(&run_id))
            .await??)
    }

    async fn heartbeat(&self, permit: &TaskWritePermit) -> RuntimeResult<()> {
        let permit = permit.clone();
        let lease_until = Utc::now() + self.lease_duration;
        Ok(self
            .store_executor
            .execute(move |store| store.heartbeat_task(&permit, lease_until))
            .await??)
    }

    /// Claims one ready task. Lease recovery is a separate supervisor duty so
    /// idle worker count cannot multiply global recovery scans.
    pub async fn run_one<F, Fut>(&self, worker_id: &str, handle: F) -> RuntimeResult<bool>
    where
        F: FnOnce(ClaimedAttempt) -> Fut,
        Fut: Future<Output = TaskCompletion>,
    {
        let now = Utc::now();
        let worker_id = worker_id.to_owned();
        let lease_duration = self.lease_duration;
        let Some(task) = self
            .store_executor
            .execute(move |store| store.claim_next_task(&worker_id, now, lease_duration))
            .await??
        else {
            return Ok(false);
        };
        if self.cancel_requested(&task.run_id).await? {
            self.finish(&task, TaskCompletion::Cancelled, Utc::now())
                .await?;
            return Ok(true);
        }

        let mut heartbeat = tokio::time::interval(self.recovery_interval()?);
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
                    if self.cancel_requested(&task.run_id).await? {
                        break TaskCompletion::Cancelled;
                    }
                    self.heartbeat(&task.permit).await?;
                }
                _ = &mut timeout => break TaskCompletion::Retry(RetryCause::Timeout),
            }
        };
        self.finish(&task, completion, Utc::now()).await?;
        Ok(true)
    }

    pub(super) async fn finish(
        &self,
        task: &ClaimedAttempt,
        completion: TaskCompletion,
        now: DateTime<Utc>,
    ) -> RuntimeResult<()> {
        let retry_at = match &completion {
            TaskCompletion::Retry(cause) if self.retry_allowed(task, *cause) => {
                Some(self.retry_at(task, now)?)
            }
            _ => None,
        };
        let task = task.clone();
        self.store_executor
            .execute(move |store| {
                match completion {
                    TaskCompletion::Succeeded(artifacts) => store.commit_attempt(
                        &task.permit,
                        &artifacts,
                        TaskStatus::Succeeded,
                        now,
                    )?,
                    TaskCompletion::NoOutput => {
                        store.finish_task(&task.permit, TaskStatus::Succeeded, now)?
                    }
                    TaskCompletion::Committed => {
                        store.verify_attempt_terminal(&task.permit, TaskStatus::Succeeded)?
                    }
                    TaskCompletion::Failed | TaskCompletion::Retry(_) => {
                        if let Some(retry_at) = retry_at {
                            match store.retry_task(&task.permit, retry_at, now)? {
                                RetryTaskResult::Requeued | RetryTaskResult::Terminal(_) => {}
                            }
                        } else {
                            store.finish_task(&task.permit, TaskStatus::Failed, now)?;
                        }
                    }
                    TaskCompletion::Skipped => {
                        store.finish_task(&task.permit, TaskStatus::Skipped, now)?
                    }
                    TaskCompletion::Cancelled => {
                        store.finish_task(&task.permit, TaskStatus::Cancelled, now)?
                    }
                    TaskCompletion::DeferredUntil(ready_at) => {
                        store.defer_task(&task.permit, ready_at, now)?
                    }
                }
                Ok::<(), StoreError>(())
            })
            .await??;
        Ok(())
    }

    pub(super) fn retry_allowed(&self, task: &ClaimedAttempt, cause: RetryCause) -> bool {
        match cause {
            RetryCause::Transport | RetryCause::Timeout => task.node.retry.retry_transport,
            RetryCause::RateLimited => task.node.retry.retry_rate_limited,
            RetryCause::InvalidOutput => task.node.retry.retry_invalid_output,
        }
    }

    fn lease_tick_millis(&self) -> RuntimeResult<u64> {
        Ok(u64::try_from(self.lease_duration.num_milliseconds())
            .map_err(|_| RuntimeError::InvalidTaskLeaseDuration)?
            .saturating_div(3)
            .max(1))
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
