use akzio_runtime::{RuntimeError, StoreExecutor, StoreMaintenanceKind};
use akzio_store::{Store, StoreError};

/// Serialized maintenance execution outside async worker threads.
#[derive(Clone)]
pub(crate) struct Maintenance {
    executor: StoreExecutor,
}

impl Maintenance {
    pub(crate) const fn new(executor: StoreExecutor) -> Self {
        Self { executor }
    }

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
