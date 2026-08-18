use super::*;
use std::collections::BTreeMap;

use crate::{ExecutionPlan, MoneyMicros, OrderIntent};
use akzio_domain::{
    ArtifactId, ArtifactKind, ArtifactRef, Asset, ContentHash, FactorExposure, PaperCommitmentId,
    TargetPortfolio, WeightPpm, V2_SCHEMA_VERSION,
};
use chrono::Utc;

fn fixture_plan() -> ExecutionPlan {
    let mut target = TargetPortfolio::zeroed();
    target.weights.insert(Asset::Tqqq, WeightPpm(100_000));
    let mut plan = ExecutionPlan {
        schema_version: V2_SCHEMA_VERSION,
        decision_context: ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"decision")),
            kind: ArtifactKind::DecisionContext,
        },
        account_snapshot: ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"account")),
            kind: ArtifactKind::NormalizedEvidence,
        },
        quote_snapshot: ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"quote")),
            kind: ArtifactKind::NormalizedEvidence,
        },
        market_clock_snapshot: ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"clock")),
            kind: ArtifactKind::NormalizedEvidence,
        },
        policy_hash: ContentHash::of_bytes(b"policy"),
        target: target.clone(),
        orders: vec![OrderIntent {
            asset: Asset::Tqqq,
            side: OrderSide::Buy,
            notional: MoneyMicros::from_usd_cents(10_000),
            limit_price: MoneyMicros::from_usd_cents(2_500),
        }],
        gross_exposure_ppm: 100_000,
        net_exposure_ppm: 100_000,
        factor_exposure: FactorExposure::from_target(&target).unwrap(),
        turnover_ppm: 100_000,
        broker_session: "paper:fixture".to_owned(),
        created_at: Utc::now(),
        plan_hash: ContentHash::of_bytes(b"pending"),
    };
    plan.refresh_hash().unwrap();
    plan
}

#[test]
fn ids_are_deterministic_and_bounded() {
    let hash = ContentHash::of_bytes(b"plan");
    let id = client_order_id("paper:2026-08-10", &hash, 12, 0);
    assert!(id.starts_with("akzio-v2-"));
    assert!(id.len() <= 48);
    assert_eq!(id, client_order_id("paper:2026-08-10", &hash, 12, 0));
    assert_ne!(id, client_order_id("paper:2026-08-11", &hash, 12, 0));
    assert_eq!(
        replacement_client_order_id(&id),
        format!("{}-r1", id.split("-r").next().unwrap())
    );
}

#[test]
fn limit_notional_becomes_fractional_quantity() {
    let order = OrderIntent {
        asset: Asset::Tqqq,
        side: OrderSide::Buy,
        notional: MoneyMicros::from_usd_cents(10_000),
        limit_price: MoneyMicros::from_usd_cents(2_500),
    };
    assert_eq!(quantity_string(&order).unwrap(), "4.000000");
}

#[test]
fn broker_native_reprice_never_resubmits_quantity() {
    let request = reprice_request(MoneyMicros::from_usd_cents(2_500), "replacement-id");
    assert_eq!(request["client_order_id"], "replacement-id");
    assert!(request.get("qty").is_none());
    assert!(request.get("notional").is_none());
}

#[test]
fn receipt_states_cover_lifecycle_and_terminal_outcomes() {
    assert_eq!(
        receipt_state("accepted").unwrap(),
        OrderReceiptState::Accepted
    );
    assert_eq!(
        receipt_state("partially_filled").unwrap(),
        OrderReceiptState::PartiallyFilled
    );
    assert_eq!(receipt_state("filled").unwrap(), OrderReceiptState::Filled);
    assert_eq!(
        receipt_state("canceled").unwrap(),
        OrderReceiptState::Canceled
    );
    assert_eq!(
        receipt_state("rejected").unwrap(),
        OrderReceiptState::Rejected
    );
}

#[test]
fn committed_adapter_rejects_mismatched_plan_or_client_id_before_http() {
    let plan = fixture_plan();
    let credentials = PaperCredentials {
        key_id: "key".to_owned(),
        secret_key: "secret".to_owned(),
    };
    let adapter = AlpacaPaper::new("https://paper-api.alpaca.markets", credentials).unwrap();
    let mut commitment = PaperCommitment {
        commitment_id: PaperCommitmentId::new(),
        execution_context: ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"context")),
            kind: ArtifactKind::ExecutionContext,
        },
        plan_hash: ContentHash::of_bytes(b"other-plan"),
        broker_session: "paper:fixture".to_owned(),
        client_order_ids: BTreeMap::from([(
            Asset::Tqqq,
            client_order_id(&plan.broker_session, &plan.plan_hash, 0, 0),
        )]),
        created_at: chrono::Utc::now(),
    };
    assert!(matches!(
        adapter.validate_commitment(&commitment, &plan),
        Err(PaperError::CommitmentPlanHashMismatch)
    ));

    commitment.plan_hash = plan.plan_hash.clone();
    commitment
        .client_order_ids
        .insert(Asset::Tqqq, "forged-client-order-id".to_owned());
    assert!(matches!(
        adapter.validate_commitment(&commitment, &plan),
        Err(PaperError::CommitmentClientOrderMismatch(Asset::Tqqq))
    ));
}

#[test]
fn adapter_accepts_only_the_exact_alpaca_paper_origin() {
    let credentials = PaperCredentials {
        key_id: "key".to_owned(),
        secret_key: "secret".to_owned(),
    };
    assert!(AlpacaPaper::new("https://paper-api.alpaca.markets/", credentials.clone()).is_ok());
    for endpoint in [
        "https://api.alpaca.markets",
        "http://paper-api.alpaca.markets",
        "https://paper-api.alpaca.markets.evil.test",
        "https://evil.test/paper-api.alpaca.markets",
        "https://paper-api.alpaca.markets/v2",
        "http://127.0.0.1:9999",
    ] {
        assert!(matches!(
            AlpacaPaper::new(endpoint, credentials.clone()),
            Err(PaperError::NonPaperEndpoint(_))
        ));
    }
}

#[test]
fn market_clock_uses_broker_session_date() {
    let clock = market_clock_from_value(&serde_json::json!({
        "is_open": true,
        "timestamp": "2026-08-06T10:00:00-04:00",
    }))
    .unwrap();
    assert!(clock.is_open);
    assert_eq!(
        clock.session_date,
        chrono::NaiveDate::from_ymd_opt(2026, 8, 6).unwrap()
    );
    assert!(matches!(
        market_clock_from_value(&serde_json::json!({"is_open": true})),
        Err(PaperError::MissingField("clock.timestamp"))
    ));
}
