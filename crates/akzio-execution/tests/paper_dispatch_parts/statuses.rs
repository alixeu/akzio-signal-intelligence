#[tokio::test]
async fn fake_broker_reports_lifecycle_and_terminal_statuses() {
    let now = Utc::now();
    let plan = plan(
        now,
        artifact_ref_for(ArtifactKind::DecisionContext, b"decision-context"),
        artifact_ref_for(ArtifactKind::NormalizedEvidence, b"account"),
        artifact_ref_for(ArtifactKind::NormalizedEvidence, b"quote"),
        artifact_ref_for(ArtifactKind::NormalizedEvidence, b"clock"),
    );
    let commitment = PaperCommitment {
        commitment_id: PaperCommitmentId::new(),
        execution_context: artifact_ref_for(ArtifactKind::ExecutionContext, b"context"),
        plan_hash: plan.plan_hash.clone(),
        broker_session: plan.broker_session.clone(),
        client_order_ids: BTreeMap::from([(
            Asset::Qqq,
            client_order_id(&plan.broker_session, &plan.plan_hash, 0, 0),
        )]),
        created_at: now,
    };
    let broker = FakeCommittedBroker::new([
        "accepted",
        "partially_filled",
        "filled",
        "canceled",
        "rejected",
    ]);
    let submitted = broker.execute_commitment(&commitment, &plan).await.unwrap();
    for (status, filled, remaining) in [
        ("accepted", 0, 4_000_000),
        ("partially_filled", 2_000_000, 2_000_000),
        ("filled", 4_000_000, 0),
        ("canceled", 0, 4_000_000),
        ("rejected", 0, 4_000_000),
    ] {
        let observation = broker
            .reconcile_commitment(&commitment, &submitted)
            .await
            .unwrap();
        let receipt = &observation.orders[0];
        assert_eq!(receipt.status, status);
        assert_eq!(receipt.filled_quantity_micros, filled);
        assert_eq!(receipt.remaining_quantity_micros, remaining);
        assert_eq!(
            receipt.requested_quantity_micros,
            receipt.filled_quantity_micros + receipt.remaining_quantity_micros
        );
        assert_eq!(
            receipt.reason.is_some(),
            matches!(status, "canceled" | "rejected")
        );
    }
}
