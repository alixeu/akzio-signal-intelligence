use super::*;

#[derive(Debug, Clone)]
pub struct TaskRuntime {
    store_executor: StoreExecutor,
    lease_duration: Duration,
    outcome_processing: bool,
    debug_identity: Option<ContentHash>,
}

impl TaskRuntime {
    pub fn new(store: Store) -> Self {
        Self {
            store_executor: StoreExecutor::new(store),
            lease_duration: Duration::seconds(30),
            outcome_processing: true,
            debug_identity: None,
        }
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

    pub fn with_outcome_processing(mut self, enabled: bool) -> Self {
        self.outcome_processing = enabled;
        self
    }

    pub fn with_debug_identity(mut self, identity: Option<ContentHash>) -> Self {
        self.debug_identity = identity;
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
        let lease_duration = self.lease_duration;
        Ok(self
            .store_executor
            .execute(move |store| store.heartbeat_task(&permit, Utc::now() + lease_duration))
            .await??)
    }

    /// Claims one ready task. Lease recovery is a separate supervisor duty so
    /// idle worker count cannot multiply global recovery scans.
    pub async fn run_one<F, Fut>(&self, worker_id: &str, handle: F) -> RuntimeResult<bool>
    where
        F: FnOnce(ClaimedAttempt) -> Fut,
        Fut: Future<Output = TaskCompletion>,
    {
        self.run_one_for_workload(worker_id, akzio_store::TaskWorkload::Any, handle)
            .await
    }

    pub async fn run_one_for_workload<F, Fut>(
        &self,
        worker_id: &str,
        workload: akzio_store::TaskWorkload,
        handle: F,
    ) -> RuntimeResult<bool>
    where
        F: FnOnce(ClaimedAttempt) -> Fut,
        Fut: Future<Output = TaskCompletion>,
    {
        if !self.outcome_processing && workload == akzio_store::TaskWorkload::Outcome {
            return Ok(false);
        }
        let workload = if self.outcome_processing {
            workload
        } else {
            akzio_store::TaskWorkload::Session
        };
        let worker_id = worker_id.to_owned();
        let lease_duration = self.lease_duration;
        let identity = self.debug_identity.clone();
        let Some(task) = self
            .store_executor
            .execute(move |store| {
                store.claim_next_task_for_workload_with_identity(
                    &worker_id,
                    Utc::now(),
                    lease_duration,
                    workload,
                    identity.as_ref(),
                )
            })
            .await??
        else {
            return Ok(false);
        };
        if self.cancel_requested(&task.run_id).await? {
            self.finish(&task, TaskCompletion::Cancelled, Utc::now())
                .await?;
            return Ok(true);
        }

        let completion = {
            let mut heartbeat = tokio::time::interval(self.recovery_interval()?);
            heartbeat.tick().await;
            let mut handler = Box::pin(handle(task.clone()));
            // Keep polling the handler while heartbeat Store work waits. A queued
            // semaphore permit may already belong to that handler; awaiting a
            // heartbeat inside a select branch would prevent it from releasing it.
            let monitor = async {
                loop {
                    heartbeat.tick().await;
                    if self.cancel_requested(&task.run_id).await? {
                        return Ok::<_, RuntimeError>(TaskCompletion::Cancelled);
                    }
                    self.heartbeat(&task.permit).await?;
                }
            };
            tokio::pin!(monitor);
            let timeout = tokio::time::sleep(StdDuration::from_secs(u64::from(
                task.node.budget.max_wall_time_secs,
            )));
            tokio::pin!(timeout);
            tokio::select! {
                result = &mut handler => result,
                result = &mut monitor => result?,
                _ = &mut timeout => TaskCompletion::Retry(RetryCause::Timeout),
            }
        }; // Drop both futures and any queued permits before terminal Store work.
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
            TaskCompletion::RetryAfter(cause, requested) if self.retry_allowed(task, *cause) => {
                Some((*requested).max(now))
            }
            TaskCompletion::Retry(cause) if self.retry_allowed(task, *cause) => {
                Some(self.retry_at(task, now).await?)
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
                    TaskCompletion::Failed
                    | TaskCompletion::Retry(_)
                    | TaskCompletion::RetryAfter(_, _) => {
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
                        // A short wait can elapse while the handler or Store
                        // executor is running. It is still a deferral, not a
                        // failed task or a reason to stop the worker pool.
                        store.defer_task(
                            &task.permit,
                            ready_at.max(now + Duration::seconds(1)),
                            now,
                        )?
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

    pub(super) async fn retry_at(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> RuntimeResult<DateTime<Utc>> {
        let task_id = task.node.task_id.clone();
        let attempts = self
            .store_executor
            .execute(move |store| store.failure_attempts_for_current_stage(&task_id))
            .await??;
        let milliseconds = retry_delay_millis(
            task.node.retry.initial_backoff_ms,
            attempts,
            task.node.recipe_id.as_str() == akzio_domain::LEARNING_OUTCOME_WORKER_RECIPE_ID,
        ) as i64;
        now.checked_add_signed(Duration::milliseconds(milliseconds))
            .ok_or(RuntimeError::InvalidRetryBackoff)
    }
}

fn retry_delay_millis(initial: u64, attempts: u64, outcome: bool) -> u64 {
    let initial = if outcome {
        initial.max(30_000)
    } else {
        initial
    };
    initial
        .saturating_mul(1_u64 << attempts.saturating_sub(1).min(10))
        .min(300_000)
}
