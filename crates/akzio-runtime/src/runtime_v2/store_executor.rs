use super::*;
use std::{
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

/// Long Store operations that drain normal executor work before they start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreMaintenanceKind {
    Doctor,
    Backup,
    Restore,
    ExportRun,
    Test,
}

impl StoreMaintenanceKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Doctor => "doctor",
            Self::Backup => "backup",
            Self::Restore => "restore",
            Self::ExportRun => "export_run",
            Self::Test => "test",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreMaintenanceOutcome {
    Succeeded,
    Failed,
}

impl StoreMaintenanceOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreMaintenanceState {
    Idle,
    Running {
        kind: StoreMaintenanceKind,
        sequence: u64,
    },
    Completed {
        kind: StoreMaintenanceKind,
        sequence: u64,
        outcome: StoreMaintenanceOutcome,
        lease_deferral: akzio_store::v2::MaintenanceLeaseDeferral,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreExecutorTelemetry {
    pub accepting_operations: bool,
    pub queued_operation_count: usize,
    pub completed_operation_count: u64,
    pub last_queue_wait: StdDuration,
    pub last_execution_duration: StdDuration,
    pub maintenance: StoreMaintenanceState,
}

#[derive(Debug)]
struct StoreExecutorState {
    queue: Arc<tokio::sync::Semaphore>,
    accepting_operations: AtomicBool,
    queued_operation_count: AtomicUsize,
    completed_operation_count: AtomicU64,
    last_queue_wait_nanos: AtomicU64,
    last_execution_nanos: AtomicU64,
    maintenance_sequence: AtomicU64,
    maintenance: tokio::sync::watch::Sender<StoreMaintenanceState>,
}

struct QueuedOperation<'a> {
    count: &'a AtomicUsize,
}

impl<'a> QueuedOperation<'a> {
    fn new(count: &'a AtomicUsize) -> Self {
        count.fetch_add(1, Ordering::Relaxed);
        Self { count }
    }
}

impl Drop for QueuedOperation<'_> {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Runs synchronous `V2Store` operations outside Tokio worker threads.
///
/// `V2Store` retains canonical serialization through its own connection mutex.
/// This executor provides one shared async queue, drained maintenance, lease
/// preservation across maintenance, shutdown/drain, and bounded telemetry.
#[derive(Debug, Clone)]
pub struct StoreExecutor {
    store: V2Store,
    state: Arc<StoreExecutorState>,
}

impl StoreExecutor {
    pub fn new(store: V2Store) -> Self {
        let (maintenance, _) = tokio::sync::watch::channel(StoreMaintenanceState::Idle);
        Self {
            store,
            state: Arc::new(StoreExecutorState {
                queue: Arc::new(tokio::sync::Semaphore::new(1)),
                accepting_operations: AtomicBool::new(true),
                queued_operation_count: AtomicUsize::new(0),
                completed_operation_count: AtomicU64::new(0),
                last_queue_wait_nanos: AtomicU64::new(0),
                last_execution_nanos: AtomicU64::new(0),
                maintenance_sequence: AtomicU64::new(0),
                maintenance,
            }),
        }
    }

    pub async fn execute<T, F>(&self, operation: F) -> RuntimeResult<T>
    where
        T: Send + 'static,
        F: FnOnce(V2Store) -> T + Send + 'static,
    {
        let (permit, queue_wait) = self.acquire_operation_permit().await?;
        let store = self.store.clone();
        let execution_started = Instant::now();
        let result = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation(store)
        })
        .await
        .map_err(|error| RuntimeError::StoreExecutor(error.to_string()));
        self.record_completion(queue_wait, execution_started.elapsed());
        result
    }

    /// Drain prior Store work, run one maintenance operation, and defer every
    /// lease that was live at maintenance start by the elapsed maintenance
    /// duration before normal work and recovery can resume.
    pub async fn execute_maintenance<T, F>(
        &self,
        kind: StoreMaintenanceKind,
        operation: F,
    ) -> RuntimeResult<T>
    where
        T: Send + 'static,
        F: FnOnce(V2Store) -> RuntimeResult<T> + Send + 'static,
    {
        let (permit, queue_wait) = self.acquire_operation_permit().await?;
        let sequence = self
            .state
            .maintenance_sequence
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        self.state
            .maintenance
            .send_replace(StoreMaintenanceState::Running { kind, sequence });
        let store = self.store.clone();
        let execution_started = Instant::now();
        let started_at = Utc::now();
        let result = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let operation_result = catch_unwind(AssertUnwindSafe(|| operation(store.clone())));
            let completed_at = Utc::now();
            let lease_deferral = store.defer_live_leases_for_maintenance(started_at, completed_at);
            match operation_result {
                Ok(Ok(value)) => Ok((value, lease_deferral?)),
                Ok(Err(error)) => {
                    let _ = lease_deferral;
                    Err(error)
                }
                Err(payload) => {
                    let _ = lease_deferral;
                    resume_unwind(payload)
                }
            }
        })
        .await;
        let execution_duration = execution_started.elapsed();
        self.record_completion(queue_wait, execution_duration);

        match result {
            Ok(Ok((value, lease_deferral))) => {
                self.state
                    .maintenance
                    .send_replace(StoreMaintenanceState::Completed {
                        kind,
                        sequence,
                        outcome: StoreMaintenanceOutcome::Succeeded,
                        lease_deferral,
                    });
                Ok(value)
            }
            Ok(Err(error)) => {
                self.complete_failed_maintenance(kind, sequence);
                Err(error)
            }
            Err(error) => {
                self.complete_failed_maintenance(kind, sequence);
                Err(RuntimeError::StoreExecutor(error.to_string()))
            }
        }
    }

    /// Reject new work and wait until the active/queued operation set drains.
    pub async fn shutdown_and_drain(&self) -> RuntimeResult<()> {
        self.state
            .accepting_operations
            .store(false, Ordering::Release);
        let permit = self
            .state
            .queue
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| RuntimeError::StoreExecutor(error.to_string()))?;
        drop(permit);
        Ok(())
    }

    pub fn telemetry(&self) -> StoreExecutorTelemetry {
        StoreExecutorTelemetry {
            accepting_operations: self.state.accepting_operations.load(Ordering::Acquire),
            queued_operation_count: self.state.queued_operation_count.load(Ordering::Relaxed),
            completed_operation_count: self.state.completed_operation_count.load(Ordering::Relaxed),
            last_queue_wait: duration_from_nanos(
                self.state.last_queue_wait_nanos.load(Ordering::Relaxed),
            ),
            last_execution_duration: duration_from_nanos(
                self.state.last_execution_nanos.load(Ordering::Relaxed),
            ),
            maintenance: *self.state.maintenance.borrow(),
        }
    }

    async fn acquire_operation_permit(
        &self,
    ) -> RuntimeResult<(tokio::sync::OwnedSemaphorePermit, StdDuration)> {
        if !self.state.accepting_operations.load(Ordering::Acquire) {
            return Err(RuntimeError::StoreExecutor(
                "Store executor is shut down".to_owned(),
            ));
        }
        let queued_at = Instant::now();
        let queued = QueuedOperation::new(&self.state.queued_operation_count);
        let permit = self.state.queue.clone().acquire_owned().await;
        drop(queued);
        let permit = permit.map_err(|error| RuntimeError::StoreExecutor(error.to_string()))?;
        if !self.state.accepting_operations.load(Ordering::Acquire) {
            drop(permit);
            return Err(RuntimeError::StoreExecutor(
                "Store executor is shut down".to_owned(),
            ));
        }
        Ok((permit, queued_at.elapsed()))
    }

    fn record_completion(&self, queue_wait: StdDuration, execution_duration: StdDuration) {
        self.state
            .last_queue_wait_nanos
            .store(duration_nanos(queue_wait), Ordering::Relaxed);
        self.state
            .last_execution_nanos
            .store(duration_nanos(execution_duration), Ordering::Relaxed);
        self.state
            .completed_operation_count
            .fetch_add(1, Ordering::Relaxed);
    }

    fn complete_failed_maintenance(&self, kind: StoreMaintenanceKind, sequence: u64) {
        self.state
            .maintenance
            .send_replace(StoreMaintenanceState::Completed {
                kind,
                sequence,
                outcome: StoreMaintenanceOutcome::Failed,
                lease_deferral: akzio_store::v2::MaintenanceLeaseDeferral::default(),
            });
    }
}

fn duration_nanos(duration: StdDuration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn duration_from_nanos(nanos: u64) -> StdDuration {
    StdDuration::from_nanos(nanos)
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
    async fn store_executor_serializes_store_operations() {
        let directory = tempdir().unwrap();
        let executor = StoreExecutor::new(V2Store::open(directory.path()).unwrap());
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let tasks = (0..8)
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
                            Ok::<_, StoreError>(())
                        })
                        .await
                })
            })
            .collect::<Vec<_>>();

        for task in tasks {
            task.await.unwrap().unwrap().unwrap();
        }
        assert_eq!(maximum.load(Ordering::SeqCst), 1);
        let telemetry = executor.telemetry();
        assert_eq!(telemetry.completed_operation_count, 8);
        assert_eq!(telemetry.queued_operation_count, 0);
    }

    #[tokio::test]
    async fn maintenance_error_and_panic_release_the_queue() {
        let directory = tempdir().unwrap();
        let executor = StoreExecutor::new(V2Store::open(directory.path()).unwrap());

        let error = executor
            .execute_maintenance(StoreMaintenanceKind::Test, |_| {
                Err::<(), _>(RuntimeError::StoreExecutor("fixture error".to_owned()))
            })
            .await
            .unwrap_err();
        assert!(matches!(error, RuntimeError::StoreExecutor(_)));
        assert!(matches!(
            executor.telemetry().maintenance,
            StoreMaintenanceState::Completed {
                outcome: StoreMaintenanceOutcome::Failed,
                ..
            }
        ));

        let panic = executor
            .execute_maintenance(StoreMaintenanceKind::Test, |_| -> RuntimeResult<()> {
                panic!("fixture maintenance panic")
            })
            .await
            .unwrap_err();
        assert!(matches!(panic, RuntimeError::StoreExecutor(_)));
        executor
            .execute(|store| store.verify_integrity())
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn shutdown_rejects_new_work_after_draining_active_work() {
        let directory = tempdir().unwrap();
        let executor = StoreExecutor::new(V2Store::open(directory.path()).unwrap());
        let running = executor.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let operation = tokio::spawn(async move {
            running
                .execute(move |_| {
                    started_tx.send(()).unwrap();
                    std::thread::sleep(StdDuration::from_millis(30));
                })
                .await
        });
        started_rx.await.unwrap();

        executor.shutdown_and_drain().await.unwrap();
        operation.await.unwrap().unwrap();
        assert!(!executor.telemetry().accepting_operations);
        assert!(executor.execute(|_| ()).await.is_err());
    }

    #[tokio::test]
    async fn backup_drains_normal_reads_and_both_complete() {
        let directory = tempdir().unwrap();
        let store_root = directory.path().join("store");
        let backup_root = directory.path().join("backup");
        let store = V2Store::open(&store_root).unwrap();
        store.put_bytes(b"backup fixture", "text/plain").unwrap();
        let executor = StoreExecutor::new(store);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let maintenance_executor = executor.clone();
        let backup = tokio::spawn(async move {
            maintenance_executor
                .execute_maintenance(StoreMaintenanceKind::Backup, move |store| {
                    started_tx.send(()).unwrap();
                    std::thread::sleep(StdDuration::from_millis(20));
                    Ok(store.backup_to(backup_root)?)
                })
                .await
        });
        started_rx.await.unwrap();
        let read_executor = executor.clone();
        let read = tokio::spawn(async move {
            read_executor
                .execute(|store| store.storage_inventory())
                .await
        });

        let manifest = backup.await.unwrap().unwrap();
        let inventory = read.await.unwrap().unwrap().unwrap();
        assert_eq!(manifest.blob_count, inventory.blob_count);
        assert_eq!(executor.telemetry().queued_operation_count, 0);
        assert!(matches!(
            executor.telemetry().maintenance,
            StoreMaintenanceState::Completed {
                kind: StoreMaintenanceKind::Backup,
                outcome: StoreMaintenanceOutcome::Succeeded,
                ..
            }
        ));
    }
}
