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
    Retention,
    Test,
}

impl StoreMaintenanceKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Doctor => "doctor",
            Self::Backup => "backup",
            Self::Restore => "restore",
            Self::ExportRun => "export_run",
            Self::Retention => "retention",
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
        lease_deferral: akzio_store::MaintenanceLeaseDeferral,
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

/// Runs synchronous `Store` operations outside Tokio worker threads.
///
/// `Store` retains canonical serialization through its own connection mutex.
/// This executor provides one shared async queue, drained maintenance, lease
/// preservation across maintenance, shutdown/drain, and bounded telemetry.
#[derive(Debug, Clone)]
pub struct StoreExecutor {
    store: Store,
    state: Arc<StoreExecutorState>,
}

impl StoreExecutor {
    pub fn new(store: Store) -> Self {
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
        F: FnOnce(Store) -> T + Send + 'static,
    {
        let (permit, queue_wait) = self.acquire_operation_permit().await?;
        let store = self.store.clone();
        let execution_started = Instant::now();
        let executor = self.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let result = catch_unwind(AssertUnwindSafe(|| operation(store)));
            // The work owns completion, even if its awaiting HTTP/task future
            // is cancelled. Publish before releasing the queue to later work.
            executor.record_completion(queue_wait, execution_started.elapsed());
            match result {
                Ok(value) => value,
                Err(payload) => resume_unwind(payload),
            }
        })
        .await
        .map_err(|error| RuntimeError::StoreExecutor(error.to_string()))
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
        F: FnOnce(Store) -> RuntimeResult<T> + Send + 'static,
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
        let executor = self.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let operation_result = catch_unwind(AssertUnwindSafe(|| operation(store.clone())));
            let completed_at = Utc::now();
            let lease_deferral = store.defer_live_leases_for_maintenance(started_at, completed_at);
            let outcome = if matches!(operation_result, Ok(Ok(_))) && lease_deferral.is_ok() {
                StoreMaintenanceOutcome::Succeeded
            } else {
                StoreMaintenanceOutcome::Failed
            };
            executor.record_completion(queue_wait, execution_started.elapsed());
            executor
                .state
                .maintenance
                .send_replace(StoreMaintenanceState::Completed {
                    kind,
                    sequence,
                    outcome,
                    lease_deferral: lease_deferral.as_ref().copied().unwrap_or_default(),
                });
            // Lease preservation and final telemetry belong to the blocking
            // operation, not its waiter. Both finish before another operation
            // may acquire the permit and publish a new Running state.
            match operation_result {
                Ok(Ok(value)) => {
                    lease_deferral?;
                    Ok(value)
                }
                Ok(Err(error)) => Err(error),
                Err(payload) => resume_unwind(payload),
            }
        })
        .await
        .map_err(|error| RuntimeError::StoreExecutor(error.to_string()))?
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
}

fn duration_nanos(duration: StdDuration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn duration_from_nanos(nanos: u64) -> StdDuration {
    StdDuration::from_nanos(nanos)
}

#[cfg(test)]
#[path = "store_executor_tests.rs"]
mod cancellation_tests;
