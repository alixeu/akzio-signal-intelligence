use super::*;
use akzio_domain::{
    ArtifactId, ArtifactKind, ArtifactRef, ContentHash, OutcomeId, V2_DOMAIN_SCHEMA_VERSION,
};
use chrono::{Duration, TimeZone};

#[test]
fn policy_exposure_only_reports_true_active_allocation() {
    for state in [
        CandidatePolicyState::Candidate,
        CandidatePolicyState::Canary10,
        CandidatePolicyState::Canary25,
        CandidatePolicyState::Canary50,
    ] {
        assert_eq!(policy_exposure_ppm(PolicyState::Contract(state)), None);
        assert_eq!(policy_exposure_ppm(PolicyState::Topology(state)), None);
    }

    assert_eq!(
        policy_exposure_ppm(PolicyState::Contract(CandidatePolicyState::Active)),
        Some(1_000_000)
    );
    assert_eq!(
        policy_exposure_ppm(PolicyState::Topology(CandidatePolicyState::Active)),
        Some(1_000_000)
    );
}

#[test]
fn managed_realized_pnl_uses_opening_average_cost() {
    let positions = serde_json::json!([
        {"symbol":"QQQ","qty":"2","avg_entry_price":"100"}
    ]);
    let fills = vec![ObserverBrokerFill {
        activity_id: "fill-1".to_owned(),
        broker_order_id: "order-1".to_owned(),
        symbol: "QQQ".to_owned(),
        side: "sell".to_owned(),
        quantity_micros: 1_000_000,
        price_micros: 110_000_000,
        transaction_at: Utc::now(),
        venue: None,
        source: "alpaca_fill_activity",
    }];
    assert_eq!(
        managed_realized_pnl(&positions, &fills).unwrap(),
        10_000_000
    );
}

#[test]
fn fill_activity_projection_filters_orders_and_never_invents_venue() {
    let value = serde_json::json!([
        {
            "id":"fill-1","order_id":"managed","symbol":"QQQ","side":"buy",
            "qty":"1.5","price":"100.25","transaction_time":"2026-08-20T14:30:00Z"
        },
        {
            "id":"fill-2","order_id":"external","symbol":"QQQ","side":"buy",
            "qty":"1","price":"101","transaction_time":"2026-08-20T14:31:00Z"
        }
    ]);
    let fills = parse_fill_activities(&value, &BTreeSet::from(["managed".to_owned()])).unwrap();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].quantity_micros, 1_500_000);
    assert_eq!(fills[0].price_micros, 100_250_000);
    assert_eq!(fills[0].venue, None);
    assert_eq!(fills[0].source, "alpaca_fill_activity");

    let saturated = Value::Array(vec![serde_json::json!({}); 100]);
    assert!(parse_fill_activities(&saturated, &BTreeSet::new()).is_err());
}

#[test]
fn portfolio_risk_requires_and_uses_aligned_daily_returns() {
    let start = Utc.with_ymd_and_hms(2026, 1, 2, 21, 0, 0).unwrap();
    let portfolio = (0..=24)
        .map(|day| (start + Duration::days(day), 100_000_000 + day * 1_000_000))
        .collect::<Vec<_>>();
    let benchmark = (0..=24)
        .map(|day| ObserverBarPoint {
            timestamp: start + Duration::days(day),
            close_micros: 100_000_000 + day * 500_000,
        })
        .collect::<Vec<_>>();
    let analytics = portfolio_analytics(&portfolio, &benchmark, 124_000_000).unwrap();
    assert_eq!(analytics.sample_count, 24);
    assert!(analytics.beta_ppm.is_some());
    assert!(analytics.volatility_ppm >= 0);
    assert_eq!(analytics.max_drawdown_ppm, 0);
}

#[test]
fn outcome_statistics_keep_small_samples_honest() {
    let reference = ArtifactRef {
        artifact_id: ArtifactId(ContentHash::of_bytes(b"schedule")),
        kind: ArtifactKind::OutcomeSchedule,
    };
    let outcome = Outcome {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        outcome_id: OutcomeId("outcome".to_owned()),
        schedule: reference,
        market_evidence: vec![ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"evidence")),
            kind: ArtifactKind::NormalizedEvidence,
        }],
        windows: vec![akzio_domain::OutcomeWindow {
            horizon: OutcomeHorizon::T1,
            observed_trading_day: NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
            portfolio_return_ppm: 20_000,
            benchmark_return_ppm: 10_000,
            transaction_cost_ppm: 100,
            slippage_ppm: 50,
            utility_ppm: 9_850,
            calibration_ppm: None,
            evidence_completeness_ppm: 1_000_000,
            risk_recall_ppm: None,
        }],
        sealed_at: Some(Utc::now()),
    };
    let statistics = outcome_statistics(&[outcome]);
    assert_eq!(statistics[0].win_rate_ppm, Some(1_000_000));
    assert_eq!(statistics[0].profit_factor_ppm, None);
    assert_eq!(statistics[0].sharpe_ppm, None);
}
