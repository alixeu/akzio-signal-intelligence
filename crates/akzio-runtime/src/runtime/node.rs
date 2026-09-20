//! Execution seam shared by all node handlers. Scheduling remains in TaskRuntime.
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
        self.run_one_for_workload(worker, workload, |task| {
            executor.execute(NodeContext {
                task,
                started_at: Utc::now(),
            })
        })
        .await
    }
}
