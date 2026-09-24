// 文件导读：Maintenance 把 Doctor/backup/restore 等维护操作送入专用 StoreExecutor 通道，
// 避免和普通 Store 操作、task lease 或 scheduler heartbeat 交叉写入。维护返回成功只说明
// 该维护事务完成，不改变研究/Decision/Paper/Outcome 的业务语义。
// Rust 机制：`FnOnce + Send + 'static` 让闭包拥有工作并安全跨线程进入 executor；`T: Send`
// 约束结果可离开后台线程，`Clone` 只复制 executor 句柄而不复制 Store 数据库。

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
