//! Execution seam shared by all node handlers. Scheduling remains in TaskRuntime.
// 文件导读：NodeExecutor 只定义“拿到一个已 claim Attempt 后如何异步计算”，不拥有 lease、
// Store 提交或 Gate 权限。Future 被 TaskRuntime select 取消时，完成/恢复仍由 Store
// 的 Attempt 事件处理；闭包适配器不会把 handler 返回值直接变成业务终态。
use super::*;
use std::pin::Pin;

#[derive(Debug, Clone)]
pub struct NodeContext {
    pub task: ClaimedAttempt,
    pub started_at: DateTime<Utc>,
}

pub trait NodeExecutor: Send + Sync {
    // `Pin<Box<dyn Future<...> + Send + '_>>` 把不同 handler 的 Future 擦除为统一类型；
    // `Pin` 使可能自引用的 async 状态机在 poll 期间不被移动，`'_` 允许 Future 借用 self。
    fn execute(
        &self,
        context: NodeContext,
    ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + '_>>;
}

/// Existing handlers enter the same attempt/lease/completion protocol.
impl<F, Fut> NodeExecutor for F
where
    F: Fn(ClaimedAttempt) -> Fut + Send + Sync + ?Sized,
    Fut: Future<Output = NodeOutcome> + Send + 'static,
{
    // 泛型 F/Fut 保留调用方闭包的具体类型；'static 限制闭包生成的 Future 不借用
    // 临时 ClaimedAttempt，和上面的 trait 对自定义 executor 可借用 self 并不矛盾。
    fn execute(
        &self,
        context: NodeContext,
    ) -> Pin<Box<dyn Future<Output = NodeOutcome> + Send + '_>> {
        // 只把 task 所有权移入 handler；NodeContext 的 started_at 是观察时间，不能
        // 覆盖 Store 的 Attempt started_at 或延长该节点预算。
        Box::pin(self(context.task))
    }
}

impl TaskRuntime {
    pub async fn execute_ready_node<E: NodeExecutor + ?Sized>(
        &self,
        worker: &str,
        workload: akzio_store::TaskWorkload,
        executor: &E,
    ) -> RuntimeResult<bool> {
        // `?Sized` 容许传入 trait object 引用；`&E` 是共享借用，
        // ClaimedAttempt 则移动到 NodeContext，最终完成由 TaskRuntime 写回。
        // 所有 Node（包括 Rust Gate 和 Agent）都进入同一个 claim/heartbeat/finish
        // 协议，避免单步入口绕开 lease fencing 或取消检查。
        self.run_one_for_workload(worker, workload, |task| {
            executor.execute(NodeContext {
                task,
                started_at: Utc::now(),
            })
        })
        .await
    }
}
