use super::*;

fn executor() -> StoreExecutor {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../.akzio/audit-20260919/followup-20260920/tests/executor")
        .join(akzio_domain::RunId::new().0);
    StoreExecutor::new(Store::open(root).unwrap())
}

#[tokio::test]
async fn cancelled_maintenance_waiter_cannot_leave_running_or_lose_completion() {
    for outcome in ["success", "error", "panic"] {
        let executor = executor();
        let now = Utc::now();
        let lease = executor
            .store
            .acquire_daemon_lease(
                "maintenance-test",
                "owner",
                now,
                now + Duration::seconds(30),
            )
            .unwrap()
            .unwrap();
        let worker = executor.clone();
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let caller = tokio::spawn(async move {
            worker
                .execute_maintenance(StoreMaintenanceKind::Test, move |_| {
                    started.send(()).unwrap();
                    wait.recv().unwrap();
                    match outcome {
                        "error" => Err(RuntimeError::StoreExecutor("injected failure".into())),
                        "panic" => panic!("injected maintenance panic"),
                        _ => Ok(()),
                    }
                })
                .await
        });
        ready.await.unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert!(matches!(
            executor.telemetry().maintenance,
            StoreMaintenanceState::Running { .. }
        ));
        release.send(()).unwrap();
        // This operation cannot acquire the queue until maintenance has finished.
        let observed = executor.clone();
        let telemetry = executor
            .execute(move |_| observed.telemetry())
            .await
            .unwrap();
        assert_eq!(
            telemetry.completed_operation_count, 1,
            "completion belongs to the actual work, not its cancelled waiter"
        );
        assert!(
            matches!(telemetry.maintenance, StoreMaintenanceState::Completed { outcome: actual, .. }
            if actual == if outcome == "success" { StoreMaintenanceOutcome::Succeeded } else { StoreMaintenanceOutcome::Failed })
        );
        let durable = executor
            .store
            .daemon_lease("maintenance-test")
            .unwrap()
            .unwrap();
        assert_eq!(durable.epoch, lease.epoch);
        assert!(
            durable.expires_at > lease.expires_at,
            "cancellation, failure and panic must still preserve the live lease"
        );
        assert_eq!(executor.telemetry().completed_operation_count, 2);
    }
}

#[tokio::test]
async fn cancelling_queued_maintenance_never_marks_unstarted_work_running() {
    let executor = executor();
    let worker = executor.clone();
    let (started, ready) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let blocker = tokio::spawn(async move {
        worker
            .execute(move |_| {
                started.send(()).unwrap();
                wait.recv().unwrap();
            })
            .await
            .unwrap();
    });
    ready.await.unwrap();
    let worker = executor.clone();
    let caller = tokio::spawn(async move {
        worker
            .execute_maintenance(StoreMaintenanceKind::Test, |_| Ok(()))
            .await
    });
    while executor.telemetry().queued_operation_count == 0 {
        tokio::task::yield_now().await;
    }
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert_eq!(executor.telemetry().queued_operation_count, 0);
    assert_eq!(
        executor.telemetry().maintenance,
        StoreMaintenanceState::Idle
    );
    release.send(()).unwrap();
    blocker.await.unwrap();
    assert_eq!(executor.telemetry().completed_operation_count, 1);
}

#[tokio::test]
async fn cancelled_normal_waiter_still_records_actual_work_completion() {
    let executor = executor();
    let worker = executor.clone();
    let (started, ready) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let caller = tokio::spawn(async move {
        worker
            .execute(move |_| {
                started.send(()).unwrap();
                wait.recv().unwrap();
            })
            .await
    });
    ready.await.unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    let observed = executor.clone();
    let count = executor
        .execute(move |_| observed.telemetry().completed_operation_count)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[test]
fn maintenance_preserves_leases_while_waiting_for_blocking_pool_capacity() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap()
        .block_on(async {
            let executor = executor();
            let (started, ready) = tokio::sync::oneshot::channel();
            let (release, wait) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                wait.recv().unwrap();
            });
            ready.await.unwrap();
            let now = Utc::now();
            let lease = executor.store.acquire_daemon_lease(
                "blocking-pool-test", "owner", now, now + Duration::seconds(1),
            ).unwrap().unwrap();
            let worker = executor.clone();
            let maintenance = tokio::spawn(async move {
                worker.execute_maintenance(StoreMaintenanceKind::Test, |_| Ok(())).await
            });
            tokio::time::timeout(StdDuration::from_secs(2), async {
                while !matches!(executor.telemetry().maintenance, StoreMaintenanceState::Running { .. }) {
                    tokio::task::yield_now().await;
                }
            }).await.unwrap();
            // Maintenance already owns the Store permit but its closure cannot
            // run until the only blocking thread is released. The lease would
            // expire in this interval without preservation from permit admission.
            assert!(Utc::now() < lease.expires_at);
            let wait_until = lease.expires_at + Duration::milliseconds(20);
            tokio::time::sleep((wait_until - Utc::now()).to_std().unwrap()).await;
            assert!(Utc::now() > lease.expires_at);
            release.send(()).unwrap();
            blocker.await.unwrap();
            maintenance.await.unwrap().unwrap();
            let durable = executor.store.daemon_lease("blocking-pool-test").unwrap().unwrap();
            assert_eq!(durable.epoch, lease.epoch);
            assert!(durable.expires_at > Utc::now(), "blocking-pool wait is part of the protected maintenance window");
            assert!(matches!(executor.telemetry().maintenance,
                StoreMaintenanceState::Completed { lease_deferral, .. } if lease_deferral.daemon_leases == 1));
        });
}
