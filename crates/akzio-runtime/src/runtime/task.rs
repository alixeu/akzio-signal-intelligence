use super::*;

// TaskRuntime 是 durable Task 与异步 handler 之间的调度层：claim 得到带 epoch 的
// permit，handler Future 与 heartbeat/cancel monitor 并行 poll，最终只允许 Store 按
// permit 写入状态。Future 取消不等于同步 Store 工作取消，Retry/Deferred 也不等于失败。
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
        // 过期 lease 的回收是 supervisor 入口；worker 数量不会各自扫描并重复恢复同一任务。
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
        // heartbeat 延长的是当前 epoch 的 lease；若 permit 已被 fencing，Store 会拒绝，
        // 不能靠本地 Future 继续写入旧 Attempt。
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
        // outcome_processing=false 时直接不领取 Outcome；否则根据配置把 workload
        // 映射到 Session/Outcome，保持双时间轴可独立推进。
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
            // handler、heartbeat/cancel monitor 和硬 wall-time Future 共享一个 select。
            // select 分支退出后先 drop 所有 Future，再进行终态 Store 写入，避免 queued
            // semaphore 或异步锁在 finish 时仍被占用。
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
        // Retry 先计算是否获准以及下一次时间；随后在同一个 StoreExecutor 事务边界
        // 内执行 commit/finish/requeue/defer。handler 返回值不会绕过 task permit。
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
                        // Deferred 是可恢复的等待，不计失败预算；至少推迟一秒，避免
                        // handler 与 Store 延迟造成忙循环。
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
        // 退避次数读取当前 stage 的持久化失败计数；Outcome 至少 30s、上限 5min，
        // 失败计数跨 Attempt 保留而不是由本次 worker 的内存状态决定。
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
