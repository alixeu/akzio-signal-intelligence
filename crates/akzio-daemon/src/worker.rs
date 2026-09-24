//! Durable local worker pool.
//!
//! The pool owns no business policy.  It only turns SQLite-backed task leases
//! into concurrently running handlers and stops cleanly when its supervisor
//! asks it to.  `TaskRuntime` remains the only owner of attempts, heartbeats,
//! timeouts, retries, and terminal task events.

// 文件导读：WorkerPool 只把 Store-backed claim 转成并发 handler Future，并负责 recovery、
// idle polling、Session/Outcome capacity 和 shutdown。业务 handler 的 Deferred/Retry/Failed
// 仍由 TaskRuntime 持久化；worker loop 结束或 handler 返回不代表研究、Decision、Paper
// submission/fill 或 Outcome 已完成。
// Rust 机制：`TaskHandler` 是 `Arc<dyn Fn(...) -> Pin<Box<dyn Future + Send>> + Send + Sync>`，
// 因而可被多个 Tokio worker 共享；`JoinSet` 管理后台 recovery/worker 生命周期；`watch`
// 传播取消，`FnMut` recovery 闭包保留可变调用状态，StalePermit 只释放当前 worker。

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use akzio_runtime::{RuntimeError, TaskCompletion, TaskRuntime};
use akzio_store::{ClaimedAttempt, Store, StoreError};
use chrono::Utc;
use tokio::sync::watch;

pub type TaskHandler = Arc<
    dyn Fn(ClaimedAttempt) -> Pin<Box<dyn Future<Output = TaskCompletion> + Send>> + Send + Sync,
>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerPoolConfig {
    pub worker_count: usize,
    pub idle_poll: Duration,
    pub worker_prefix: String,
}

impl Default for WorkerPoolConfig {
    fn default() -> Self {
        // 默认两个 worker：一个偏 Session、一个偏 Outcome；worker_count 只影响容量，不改变
        // workflow 拓扑或业务预算。
        Self {
            worker_count: 2,
            idle_poll: Duration::from_millis(250),
            worker_prefix: "akzio-local".to_owned(),
        }
    }
}

impl WorkerPoolConfig {
    fn normalized_worker_count(&self) -> usize {
        // 0 被归一为 1，保证服务不会静默没有消费线程；配置校验仍在 CLI/daemon 启动处执行。
        self.worker_count.max(1)
    }
}

#[derive(Debug, Clone)]
pub struct WorkerPool {
    runtime: TaskRuntime,
    config: WorkerPoolConfig,
    lesson_revalidation_store: Option<Store>,
}

impl WorkerPool {
    pub fn new(runtime: TaskRuntime, config: WorkerPoolConfig) -> Self {
        // WorkerPool 只持有 runtime/config 句柄，业务 policy 留在 runtime/handler。
        Self {
            runtime,
            config,
            lesson_revalidation_store: None,
        }
    }

    pub fn with_lesson_revalidation(mut self, store: Store) -> Self {
        // 生产 worker 可附加 revalidation maintenance；Debug fixture 不附加，保持隔离。
        self.lesson_revalidation_store = Some(store);
        self
    }

    /// Run the configured worker count until `shutdown` becomes true.
    ///
    /// A task is never run outside `TaskRuntime`: handler failures must be
    /// converted to a `TaskCompletion` by the business runtime, which keeps
    /// retry policy and every state transition durable.
    pub async fn serve(
        &self,
        handler: TaskHandler,
        shutdown: watch::Receiver<bool>,
    ) -> Result<(), RuntimeError> {
        // 先 recovery，再启动 recovery loop 和 worker JoinSet；任一子任务 panic/错误都会
        // 让 serve 返回，不能遗留未观察的后台 Future。
        let runtime = self.runtime.clone();
        let lesson_revalidation_store = self.lesson_revalidation_store.clone();
        self.serve_with_recovery(handler, shutdown, move || {
            recover_expired_tasks(runtime.clone(), lesson_revalidation_store.clone())
        })
        .await
    }

    async fn serve_with_recovery<F, Fut>(
        &self,
        handler: TaskHandler,
        shutdown: watch::Receiver<bool>,
        mut recover: F,
    ) -> Result<(), RuntimeError>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<u64, RuntimeError>> + Send + 'static,
    {
        // `F/Fut` 的 Send + 'static 约束允许 recovery Future 进入 JoinSet；每个 worker clone
        // runtime/handler/shutdown，TaskRuntime 仍是 claim/heartbeat/retry 的唯一写入者。
        recover().await?;
        let recovery_interval = self.runtime.recovery_interval()?;
        let mut workers = tokio::task::JoinSet::new();
        workers.spawn(recovery_loop(recover, recovery_interval, shutdown.clone()));
        for index in 0..self.config.normalized_worker_count() {
            let runtime = self.runtime.clone();
            let handler = handler.clone();
            let shutdown = shutdown.clone();
            let worker_id = format!("{}-{index}", self.config.worker_prefix);
            let idle_poll = self.config.idle_poll;
            let reserved = if self.config.normalized_worker_count() == 1 {
                akzio_store::TaskWorkload::Any
            } else if index == 0 {
                akzio_store::TaskWorkload::Session
            } else if index == 1 {
                akzio_store::TaskWorkload::Outcome
            } else {
                akzio_store::TaskWorkload::Any
            };
            workers.spawn(async move {
                worker_loop(runtime, worker_id, handler, shutdown, idle_poll, reserved).await
            });
        }

        while let Some(result) = workers.join_next().await {
            result.map_err(|error| {
                RuntimeError::Store(StoreError::Integrity(format!(
                    "worker task panicked: {error}"
                )))
            })??;
        }
        Ok(())
    }
}

async fn recover_expired_tasks(
    runtime: TaskRuntime,
    lesson_revalidation_store: Option<Store>,
) -> Result<u64, RuntimeError> {
    // recovery 先处理过期 Attempt，再可选地 contest 到期 Lesson；两个动作都在 durable Store
    // 边界内，不会重置失败预算或重新发送不确定的外部请求。
    let recovered = runtime.recover_expired_tasks(Utc::now()).await?;
    if let Some(store) = lesson_revalidation_store {
        tokio::task::spawn_blocking(move || store.contest_lessons_due_for_revalidation(Utc::now()))
            .await
            .map_err(|error| {
                RuntimeError::Store(StoreError::Integrity(format!(
                    "lesson revalidation maintenance panicked: {error}"
                )))
            })??;
    }
    Ok(recovered)
}

async fn recovery_loop<F, Fut>(
    mut recover: F,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), RuntimeError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<u64, RuntimeError>>,
{
    // interval 只周期性唤醒 recovery；shutdown 分支优先，避免关闭时再启动一次维护。
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            _ = ticker.tick() => {
                recover().await?;
            }
        }
    }
}

async fn worker_loop(
    runtime: TaskRuntime,
    worker_id: String,
    handler: TaskHandler,
    mut shutdown: watch::Receiver<bool>,
    idle_poll: Duration,
    reserved: akzio_store::TaskWorkload,
) -> Result<(), RuntimeError> {
    // 每轮根据 reserved class/交替偏好领取一个节点；空闲时 sleep 或响应 shutdown，
    // StalePermit 只告警并继续服务，真正的 lease recovery 留给下一轮/Recovery loop。
    let mut prefer_outcome = false;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }

        // Shared workers alternate preference and can use the other queue when
        // idle. Reserved workers provide capacity while the opposite class blocks.
        let preferred = if reserved == akzio_store::TaskWorkload::Any {
            prefer_outcome = !prefer_outcome;
            if prefer_outcome {
                akzio_store::TaskWorkload::Outcome
            } else {
                akzio_store::TaskWorkload::Session
            }
        } else {
            reserved
        };
        let result = runtime
            .execute_ready_node(&worker_id, preferred, handler.as_ref())
            .await;
        let result = if matches!(result, Ok(false)) && reserved == akzio_store::TaskWorkload::Any {
            runtime
                .execute_ready_node(&worker_id, akzio_store::TaskWorkload::Any, handler.as_ref())
                .await
        } else {
            result
        };
        let did_run = match result {
            Ok(did_run) => did_run,
            Err(RuntimeError::Store(StoreError::StalePermit(task_id))) => {
                tracing::warn!(%worker_id, %task_id, "task lease superseded; worker remains available");
                false
            }
            Err(error) => return Err(error),
        };
        if did_run {
            continue;
        }

        tokio::select! {
            _ = tokio::time::sleep(idle_poll) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
        }
    }
}
