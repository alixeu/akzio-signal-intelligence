#[tokio::test]
async fn stale_scheduler_epoch_never_calls_broker() {
    let PreparedCommitment {
        _directory,
        store,
        now,
        lease,
        commitment,
    } = prepared_commitment();
    let dispatch_permit = store
        .claim_next_task("dispatch-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    let run_id = dispatch_permit.run_id.clone();
    let takeover_at = now + Duration::seconds(31);
    store
        .acquire_daemon_lease(
            "scheduler",
            "successor-daemon",
            takeover_at,
            takeover_at + Duration::seconds(30),
        )
        .unwrap()
        .unwrap();
    let broker = FakeCommittedBroker::new(["filled"]);
    assert!(matches!(
        V2PaperDispatchRuntime::new(store.clone())
            .dispatch(
                &broker,
                &PaperDispatchInput {
                    lease,
                    permit: dispatch_permit,
                    commitment: artifact_ref(&commitment),
                    now: takeover_at,
                },
            )
            .await,
        Err(PaperDispatchError::Store(StoreError::SchedulerFenced(_)))
    ));
    assert_eq!(broker.execute_calls.load(Ordering::SeqCst), 0);
    assert_eq!(broker.reconcile_calls.load(Ordering::SeqCst), 0);
    assert!(!store
        .events_after(&run_id, 0, 100)
        .unwrap()
        .iter()
        .any(|event| {
            event.artifact_id.as_ref() == Some(&commitment.artifact_id)
                && event.event_type == LifecycleEventType::ExecutionEffectIntent.as_str()
        }));
}
