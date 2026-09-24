//! Execution seam shared by all node handlers. Scheduling remains in TaskRuntime.
// NodeExecutor 只定义“拿到一个已 claim Attempt 后如何异步计算”，不拥有 lease、
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
