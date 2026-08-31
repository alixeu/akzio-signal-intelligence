use super::*;

/// Runs synchronous `V2Store` operations outside Tokio worker threads.
/// `V2Store` retains canonical serialization through its own connection mutex;
/// this module only owns the async/blocking execution seam.
#[derive(Debug, Clone)]
pub struct StoreExecutor {
    store: V2Store,
    queue: std::sync::Arc<tokio::sync::Semaphore>,
}

impl StoreExecutor {
    pub fn new(store: V2Store) -> Self {
        Self {
            store,
            queue: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
        }
    }

    pub async fn execute<T, F>(&self, operation: F) -> RuntimeResult<T>
    where
        T: Send + 'static,
        F: FnOnce(V2Store) -> T + Send + 'static,
    {
        let permit = self
            .queue
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| RuntimeError::StoreExecutor(error.to_string()))?;
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation(store)
        })
        .await
        .map_err(|error| RuntimeError::StoreExecutor(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;

    #[tokio::test(flavor = "current_thread")]
    async fn store_executor_keeps_the_async_runtime_responsive() {
        let directory = tempdir().unwrap();
        let executor = StoreExecutor::new(V2Store::open(directory.path()).unwrap());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            executor
                .execute(move |store| {
                    let _ = started_tx.send(());
                    std::thread::sleep(StdDuration::from_millis(50));
                    store.verify_integrity()
                })
                .await
        });

        started_rx.await.unwrap();
        tokio::time::timeout(
            StdDuration::from_millis(25),
            tokio::time::sleep(StdDuration::from_millis(1)),
        )
        .await
        .unwrap();
        task.await.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn store_executor_serializes_cloned_callers_before_blocking() {
        let directory = tempdir().unwrap();
        let executor = StoreExecutor::new(V2Store::open(directory.path()).unwrap());
        let active = std::sync::Arc::new(AtomicUsize::new(0));
        let maximum = std::sync::Arc::new(AtomicUsize::new(0));
        let tasks = (0..4)
            .map(|_| {
                let executor = executor.clone();
                let active = active.clone();
                let maximum = maximum.clone();
                tokio::spawn(async move {
                    executor
                        .execute(move |_| {
                            let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                            maximum.fetch_max(current, Ordering::SeqCst);
                            std::thread::sleep(StdDuration::from_millis(10));
                            active.fetch_sub(1, Ordering::SeqCst);
                            Ok::<(), StoreError>(())
                        })
                        .await
                })
            })
            .collect::<Vec<_>>();

        for task in tasks {
            task.await.unwrap().unwrap().unwrap();
        }
        assert_eq!(maximum.load(Ordering::SeqCst), 1);
    }
}
