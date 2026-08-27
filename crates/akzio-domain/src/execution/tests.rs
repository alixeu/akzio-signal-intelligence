use super::*;
use crate::{artifact::ArtifactId, WeightPpm};

fn reference(kind: ArtifactKind, name: &[u8]) -> ArtifactRef {
    ArtifactRef {
        artifact_id: ArtifactId(ContentHash::of_bytes(name)),
        kind,
    }
}

fn plan() -> ExecutionPlan {
    let mut target = TargetPortfolio::zeroed();
    target.weights.insert(Asset::Tqqq, WeightPpm(100_000));
    let mut plan = ExecutionPlan {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        decision_context: reference(ArtifactKind::DecisionContext, b"decision"),
        account_snapshot: reference(ArtifactKind::NormalizedEvidence, b"account"),
        quote_snapshot: reference(ArtifactKind::NormalizedEvidence, b"quotes"),
        market_clock_snapshot: reference(ArtifactKind::NormalizedEvidence, b"clock"),
        policy_hash: ContentHash::of_bytes(b"policy"),
        maximum_total_notional: MoneyMicros::from_usd_cents(100_000),
        target: target.clone(),
        orders: vec![OrderIntent {
            asset: Asset::Tqqq,
            side: OrderSide::Buy,
            notional: MoneyMicros::from_usd_cents(10_000),
            limit_price: MoneyMicros::from_usd_cents(5_000),
        }],
        gross_exposure_ppm: 100_000,
        net_exposure_ppm: 100_000,
        factor_exposure: FactorExposure::from_target(&target).unwrap(),
        turnover_ppm: 100_000,
        broker_session: "2026-08-10".to_owned(),
        created_at: Utc::now(),
        plan_hash: ContentHash::of_bytes(b"pending"),
    };
    plan.refresh_hash().unwrap();
    plan
}

#[test]
fn execution_plan_hash_covers_every_payload_field() {
    let mut plan = plan();
    assert!(plan.validate().is_ok());
    plan.orders[0].notional = MoneyMicros::from_usd_cents(20_000);
    assert_eq!(plan.validate(), Err(DomainError::ExecutionPlanHashMismatch));
}

#[test]
fn accepted_context_requires_complete_plan_closure() {
    let context = ExecutionContext {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        run_id: RunId::new(),
        decision_context: reference(ArtifactKind::DecisionContext, b"decision"),
        account_snapshot: None,
        quote_snapshot: None,
        market_clock_snapshot: None,
        execution_plan: None,
        factor_exposure: None,
        turnover_ppm: None,
        plan_hash: None,
        broker_session: None,
        frozen: false,
        created_at: Utc::now(),
    };
    assert!(context.validate().is_ok());
    assert!(context.validate_complete_plan_closure().is_err());
}

#[test]
fn order_receipt_requires_quantity_conservation() {
    let now = Utc::now();
    let mut receipt = OrderReceipt {
        plan_hash: ContentHash::of_bytes(b"plan"),
        asset: Asset::Qqq,
        client_order_id: "client-order".to_owned(),
        broker_order_id: "broker-order".to_owned(),
        state: OrderReceiptState::PartiallyFilled,
        requested_quantity_micros: 10,
        filled_quantity_micros: 4,
        remaining_quantity_micros: 6,
        average_fill_price: Some(MoneyMicros::from_usd_cents(10_000)),
        broker_updated_at: now,
        reason: None,
        observed_at: now,
    };
    assert!(receipt.validate().is_ok());
    receipt.remaining_quantity_micros = 5;
    assert!(receipt.validate().is_err());

    receipt.remaining_quantity_micros = 6;
    receipt.state = OrderReceiptState::Filled;
    assert_eq!(
        receipt.validate(),
        Err(DomainError::InvalidBudget {
            field: "order_receipt.state",
        })
    );
}

#[test]
fn no_order_requires_a_typed_blocker() {
    let no_order = NoOrder {
        execution_context: reference(ArtifactKind::ExecutionContext, b"context"),
        blockers: vec![],
        created_at: Utc::now(),
    };
    assert!(no_order.validate().is_err());
}
