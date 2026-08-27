#[tokio::test]
async fn crash_after_submit_reuses_durable_client_order_id() {
    let PreparedCommitment {
        _directory,
        store,
        now,
        lease,
        commitment,
    } = prepared_commitment();
    let first_permit = store
        .claim_next_task("dispatch-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    let run_id = first_permit.run_id.clone();
    let broker = FakeCommittedBroker::new(["filled"]);
    broker.fail_next_reconcile();
    let runtime = V2PaperDispatchRuntime::new(store.clone());
    assert!(matches!(
        runtime
            .dispatch(
                &broker,
                &PaperDispatchInput {
                    lease,
                    permit: first_permit,
                    commitment: artifact_ref(&commitment),
                    now,
                },
            )
            .await,
        Err(PaperDispatchError::Broker(PaperError::InvalidCommitment(_)))
    ));
    let retry_at = now + Duration::seconds(31);
    assert_eq!(store.recover_expired_tasks(retry_at).unwrap(), 1);
    let retry_lease = store
        .acquire_daemon_lease(
            "scheduler",
            "recovered-daemon",
            retry_at,
            retry_at + Duration::seconds(30),
        )
        .unwrap()
        .unwrap();
    let retry_permit = store
        .claim_next_task("recovered-worker", retry_at, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    let output = runtime
        .dispatch(
            &broker,
            &PaperDispatchInput {
                lease: retry_lease,
                permit: retry_permit,
                commitment: artifact_ref(&commitment),
                now: retry_at,
            },
        )
        .await
        .unwrap();
    let payload: PaperCommitment =
        serde_json::from_slice(&store.read_blob(&commitment.blob).unwrap()).unwrap();
    assert_eq!(
        output.execution.orders[0].client_order_id,
        payload.client_order_ids[&Asset::Qqq]
    );
    assert!(output.execution.orders[0].reused);
    assert_eq!(broker.actual_submit_calls.load(Ordering::SeqCst), 1);
    assert_eq!(broker.lookup_calls.load(Ordering::SeqCst), 1);
    assert_eq!(broker.execute_calls.load(Ordering::SeqCst), 2);
    assert_eq!(broker.reconcile_calls.load(Ordering::SeqCst), 2);
    let events = store.events_after(&run_id, 0, 100).unwrap();
    let intent = events
        .iter()
        .find(|event| {
            event.artifact_id.as_ref() == Some(&commitment.artifact_id)
                && event.event_type == LifecycleEventType::ExecutionEffectIntent.as_str()
        })
        .expect("Paper effect intent is durable before broker I/O");
    let recovered = events
        .iter()
        .find(|event| {
            event.artifact_id.as_ref() == Some(&commitment.artifact_id)
                && event.event_type == LifecycleEventType::ExecutionEffectRecovered.as_str()
        })
        .expect("retry settles the existing Paper effect as recovered");
    assert!(intent.cursor < recovered.cursor);
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.artifact_id.as_ref() == Some(&commitment.artifact_id)
                    && event.event_type == LifecycleEventType::ExecutionEffectIntent.as_str()
            })
            .count(),
        1
    );
}
