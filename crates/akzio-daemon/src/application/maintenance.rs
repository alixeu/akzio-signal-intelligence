use akzio_runtime::{RuntimeError, StoreExecutor, StoreMaintenanceKind};
use akzio_store::{Store, StoreError};

/// Serialized maintenance execution outside async worker threads.
#[derive(Clone)]
pub(crate) struct Maintenance {
    executor: StoreExecutor,
}

impl Maintenance {
    // 保存运行时提供的 StoreExecutor；维护操作必须走这条串行执行边界。
    pub(crate) const fn new(executor: StoreExecutor) -> Self {
        Self { executor }
    }

    // 将一次性 Store 闭包移交给 executor 的专用维护通道，并把 StoreError 统一转换为
    // RuntimeError；FnOnce + Send + 'static 约束来自异步执行器对闭包所有权的转移。
    pub(crate) async fn run<T>(
        &self,
        kind: StoreMaintenanceKind,
        work: impl FnOnce(Store) -> std::result::Result<T, StoreError> + Send + 'static,
    ) -> std::result::Result<T, RuntimeError>
    where
        T: Send + 'static,
    {
        self.executor
            .execute_maintenance(kind, move |store| work(store).map_err(RuntimeError::from))
            .await
    }
}
