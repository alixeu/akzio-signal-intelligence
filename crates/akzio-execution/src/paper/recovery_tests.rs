use super::*;
use akzio_domain::{
    ArtifactId, FactorExposure, PaperCommitmentId, TargetPortfolio, WeightPpm,
    DOMAIN_SCHEMA_VERSION,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn test_authorization(
    plan: &ExecutionPlan,
    observed_at: DateTime<Utc>,
) -> PaperSubmissionAuthorization {
    let (validity, account, quotes, clock) = test_sources(plan, observed_at);
    PaperSubmissionAuthorization::from_frozen_sources(
        plan,
        &crate::ExecutionPolicy::default(),
        Some(&validity),
        Some(validity.valid_until),
        &account,
        &quotes,
        &clock,
    )
    .unwrap()
}

fn test_sources(
    plan: &ExecutionPlan,
    observed_at: DateTime<Utc>,
) -> (
    akzio_domain::DecisionValidity,
    akzio_domain::AccountSnapshot,
    akzio_domain::QuoteSnapshot,
    akzio_domain::MarketClockSnapshot,
) {
    let validity = akzio_domain::DecisionValidity {
        evidence_cutoff: observed_at,
        generated_at: observed_at,
        valid_until: observed_at + chrono::Duration::minutes(1),
        maximum_execution_delay_ms: 60_000,
        market_state_hash: ContentHash::of_bytes(b"fixture"),
    };
    let account = akzio_domain::AccountSnapshot {
        schema_version: DOMAIN_SCHEMA_VERSION,
        broker_session: plan.broker_session.clone(),
        observed_at,
        equity: MoneyMicros(1_000_000_000),
        buying_power: MoneyMicros(1_000_000_000),
        day_turnover: MoneyMicros(0),
        active: true,
        trading_blocked: false,
        positions: Default::default(),
        external_positions: Default::default(),
        open_order_ids: Default::default(),
    };
    let quotes = akzio_domain::QuoteSnapshot {
        feed: None,
        schema_version: DOMAIN_SCHEMA_VERSION,
        broker_session: plan.broker_session.clone(),
        observed_at,
        quotes: plan
            .orders
            .iter()
            .map(|order| {
                (
                    order.asset,
                    akzio_domain::Quote {
                        bid: MoneyMicros(9_990_000),
                        ask: MoneyMicros(10_000_000),
                        observed_at,
                    },
                )
            })
            .collect(),
    };
    let clock = akzio_domain::MarketClockSnapshot {
        session: None,
        schema_version: DOMAIN_SCHEMA_VERSION,
        broker_session: plan.broker_session.clone(),
        is_open: true,
        observed_at,
    };
    (validity, account, quotes, clock)
}

#[tokio::test]
async fn production_paper_client_rejects_redirect_before_second_request() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(AtomicUsize::new(0));
    let observed = requests.clone();
    let server = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            assert!(
                stream.read(&mut request).await.unwrap() > 0,
                "request closed before headers"
            );
            let index = observed.fetch_add(1, Ordering::SeqCst);
            let response = if index == 0 {
                format!(
                    "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{address}/forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
            } else {
                "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}".to_owned()
            };
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    let mut paper = AlpacaPaper::new(
        "https://paper-api.alpaca.markets",
        PaperCredentials {
            key_id: "test".into(),
            secret_key: "test".into(),
        },
    )
    .unwrap();
    // Override only the test URL; retain the exact production client policy.
    paper.base_url = format!("http://{address}");
    let result = paper
        .post_json(&paper.url("/v2/orders"), serde_json::json!({}))
        .await;
    server.abort();
    assert_eq!(
        requests.load(Ordering::SeqCst),
        1,
        "redirect target received broker credentials/body"
    );
    assert!(matches!(
        result,
        Err(PaperError::Http {
            status: StatusCode::TEMPORARY_REDIRECT,
            ..
        })
    ));
}

#[tokio::test]
async fn t11_accepted_request_lost_ack_recovers_by_original_id_without_second_order() {
    recovery_scenario(false).await;
}

#[tokio::test]
async fn durable_unsubmitted_order_cannot_cross_broker_session() {
    recovery_scenario(true).await;
}

async fn recovery_scenario(wrong_initial_session: bool) {
    let reference = |kind| ArtifactRef {
        artifact_id: ArtifactId(ContentHash::of_bytes(format!("{kind:?}").as_bytes())),
        kind,
    };
    let mut target = TargetPortfolio::zeroed();
    target.weights.insert(Asset::Qqq, WeightPpm(10_000));
    let mut plan = ExecutionPlan {
        schema_version: DOMAIN_SCHEMA_VERSION,
        decision_context: reference(ArtifactKind::DecisionContext),
        account_snapshot: reference(ArtifactKind::NormalizedEvidence),
        quote_snapshot: reference(ArtifactKind::NormalizedEvidence),
        market_clock_snapshot: reference(ArtifactKind::NormalizedEvidence),
        policy_hash: crate::ExecutionPolicy::default().policy_hash().unwrap(),
        maximum_total_notional: MoneyMicros(10_000_000),
        factor_exposure: FactorExposure::from_target(&target).unwrap(),
        target,
        orders: vec![OrderIntent {
            extended_hours: false,
            asset: Asset::Qqq,
            side: OrderSide::Buy,
            notional: MoneyMicros(10_000_000),
            limit_price: MoneyMicros(10_000_000),
        }],
        gross_exposure_ppm: 10_000,
        net_exposure_ppm: 10_000,
        turnover_ppm: 10_000,
        broker_session: "2026-09-09".into(),
        created_at: Utc::now(),
        plan_hash: ContentHash::of_bytes(b"pending"),
    };
    plan.refresh_hash().unwrap();
    plan.validate().unwrap();
    let order_id = client_order_id(&plan.broker_session, &plan.plan_hash, 0, 0);
    let commitment = PaperCommitment {
        commitment_id: PaperCommitmentId("test-commitment".into()),
        execution_context: reference(ArtifactKind::ExecutionContext),
        plan_hash: plan.plan_hash.clone(),
        broker_session: plan.broker_session.clone(),
        client_order_ids: std::collections::BTreeMap::from([(Asset::Qqq, order_id.clone())]),
        created_at: plan.created_at,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let writes = Arc::new(AtomicUsize::new(0));
    let count = writes.clone();
    let expected_id = order_id.clone();
    let (stop, mut done) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        loop {
            let accepted = tokio::select! {value=listener.accept()=>value,_=&mut done=>break};
            let (mut stream, _) = accepted.unwrap();
            let mut bytes = Vec::new();
            let mut chunk = [0u8; 4096];
            let header_end = loop {
                let n = stream.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break 0;
                }
                bytes.extend_from_slice(&chunk[..n]);
                if let Some(i) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                    break i + 4;
                }
            };
            if header_end == 0 {
                continue;
            }
            let header = String::from_utf8_lossy(&bytes[..header_end]).into_owned();
            let length = header
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|v| v.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);
            while bytes.len() < header_end + length {
                let n = stream.read(&mut chunk).await.unwrap();
                assert_ne!(n, 0);
                bytes.extend_from_slice(&chunk[..n]);
            }
            let first = header.lines().next().unwrap();
            if first.starts_with("POST /v2/orders ") {
                let body: Value =
                    serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap();
                assert_eq!(body["client_order_id"], expected_id);
                count.fetch_add(1, Ordering::SeqCst);
                // Broker has accepted the order. Drop transport before any ACK.
                drop(stream);
                continue;
            }
            let (status, body) = if first.starts_with("GET /v2/clock ") {
                (
                    200,
                    serde_json::json!({"is_open":true,"timestamp": if wrong_initial_session || count.load(Ordering::SeqCst) > 0 { "2026-09-10T10:00:00-04:00" } else { "2026-09-09T10:00:00-04:00" }}),
                )
            } else if first.starts_with("GET /v2/calendar?") {
                (
                    200,
                    serde_json::json!([{ "date":"2026-09-09", "open":"09:30", "close":"16:00" }, { "date":"2026-09-10", "open":"09:30", "close":"16:00" }]),
                )
            } else if count.load(Ordering::SeqCst) > 0
                && first.contains(&format!("client_order_id={expected_id} "))
            {
                (
                    200,
                    serde_json::json!({"id":"one-economic-order","symbol":"QQQ","status":"filled","client_order_id":expected_id,"qty":"1","filled_qty":"1","filled_avg_price":"10","updated_at":"2026-09-09T14:00:00Z"}),
                )
            } else {
                (404, serde_json::json!({"message":"not found"}))
            };
            let body = body.to_string();
            let response = format!(
                "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    // Test-only private construction. Production constructor still refuses every
    // non-Paper endpoint before making HTTP requests.
    let build = || AlpacaPaper {
        client: Client::builder()
            .no_proxy()
            .timeout(std::time::Duration::from_secs(3))
            .build()
            .unwrap(),
        base_url: format!("http://{address}"),
        credentials: PaperCredentials {
            key_id: "test".into(),
            secret_key: "test".into(),
        },
    };
    let authorization = test_authorization(&plan, Utc::now());
    let first_result = build()
        .execute_committed(&commitment, &plan, &authorization)
        .await;
    if wrong_initial_session {
        let _ = stop.send(());
        server.await.unwrap();
        assert_eq!(
            writes.load(Ordering::SeqCst),
            0,
            "old commitment submitted in a new session"
        );
        assert!(matches!(
            first_result,
            Err(PaperError::InvalidCommitment(_))
        ));
        return;
    }
    assert!(matches!(first_result, Err(PaperError::Transport { .. })));
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    let expired = test_authorization(&plan, Utc::now() - chrono::Duration::minutes(10));
    let recovered = build()
        .execute_committed(&commitment, &plan, &expired)
        .await
        .unwrap();
    assert!(recovered.orders[0].reused);
    assert_eq!(recovered.orders[0].client_order_id, order_id);
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    let _ = stop.send(());
    server.await.unwrap();
}

#[test]
fn production_constructor_rejects_live_and_loopback_before_io() {
    for endpoint in ["https://api.alpaca.markets", "http://127.0.0.1:1"] {
        assert!(matches!(
            AlpacaPaper::new(
                endpoint,
                PaperCredentials {
                    key_id: "test".into(),
                    secret_key: "test".into()
                }
            ),
            Err(PaperError::NonPaperEndpoint(_))
        ));
    }
}

#[tokio::test]
async fn expired_same_session_plan_does_not_send_first_order() {
    bounded_recovery_scenario(false, false).await;
}

#[tokio::test]
async fn partial_previous_session_preserves_existing_receipt_without_new_order() {
    bounded_recovery_scenario(true, false).await;
}

#[tokio::test]
async fn authorization_is_rechecked_after_slow_clock_before_post() {
    bounded_recovery_scenario(false, true).await;
}

async fn bounded_recovery_scenario(partial_previous_session: bool, slow_clock: bool) {
    let mut plan = test_plan();
    if !slow_clock && !partial_previous_session {
        plan.created_at -= chrono::Duration::minutes(10);
    }
    plan.refresh_hash().unwrap();
    let ids = plan
        .orders
        .iter()
        .enumerate()
        .map(|(i, order)| {
            (
                order.asset,
                client_order_id(&plan.broker_session, &plan.plan_hash, i, 0),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let commitment = PaperCommitment {
        commitment_id: PaperCommitmentId("bounded-recovery".into()),
        execution_context: ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"execution")),
            kind: ArtifactKind::ExecutionContext,
        },
        plan_hash: plan.plan_hash.clone(),
        broker_session: plan.broker_session.clone(),
        client_order_ids: ids.clone(),
        created_at: plan.created_at,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let writes = Arc::new(AtomicUsize::new(0));
    let count = writes.clone();
    let clock_reads = Arc::new(AtomicUsize::new(0));
    let observed_clocks = clock_reads.clone();
    let existing_id = ids[&Asset::Qqq].clone();
    let server = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut chunk = [0; 4096];
            let header_end = loop {
                let size = stream.read(&mut chunk).await.unwrap();
                if size == 0 {
                    break 0;
                }
                bytes.extend_from_slice(&chunk[..size]);
                if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                    break end + 4;
                }
            };
            if header_end == 0 {
                continue;
            }
            let header = String::from_utf8_lossy(&bytes[..header_end]).into_owned();
            let first = header.lines().next().unwrap();
            let (status, body) = if first.starts_with("POST ") {
                count.fetch_add(1, Ordering::SeqCst);
                (503, serde_json::json!({"error":"unexpected write"}))
            } else if first.starts_with("GET /v2/clock ") {
                observed_clocks.fetch_add(1, Ordering::SeqCst);
                if slow_clock {
                    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
                }
                (
                    200,
                    serde_json::json!({"is_open":true,"timestamp":if partial_previous_session {"2026-09-10T10:00:00-04:00"} else {"2026-09-09T10:00:00-04:00"}}),
                )
            } else if first.starts_with("GET /v2/calendar?") {
                (
                    200,
                    serde_json::json!([{ "date":"2026-09-09", "open":"09:30", "close":"16:00" }, { "date":"2026-09-10", "open":"09:30", "close":"16:00" }]),
                )
            } else if partial_previous_session
                && first.contains(&format!("client_order_id={existing_id} "))
            {
                (
                    200,
                    serde_json::json!({"id":"existing-fill","symbol":"QQQ","status":"filled","client_order_id":existing_id,"qty":"1","filled_qty":"1","filled_avg_price":"10","updated_at":"2026-09-09T14:00:00Z"}),
                )
            } else {
                (404, serde_json::json!({"message":"not found"}))
            };
            let body = body.to_string();
            stream.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        }
    });
    let paper = AlpacaPaper {
        client: Client::builder()
            .no_proxy()
            .timeout(std::time::Duration::from_secs(3))
            .build()
            .unwrap(),
        base_url: format!("http://{address}"),
        credentials: PaperCredentials {
            key_id: "fixture".into(),
            secret_key: "fixture".into(),
        },
    };
    let authorization = if slow_clock {
        let observed_at = Utc::now();
        let (validity, account, quotes, clock) = test_sources(&plan, observed_at);
        PaperSubmissionAuthorization::from_frozen_sources(
            &plan,
            &crate::ExecutionPolicy::default(),
            Some(&validity),
            Some(observed_at + chrono::Duration::seconds(1)),
            &account,
            &quotes,
            &clock,
        )
        .unwrap()
    } else {
        test_authorization(&plan, plan.created_at)
    };
    let result = paper
        .execute_committed(&commitment, &plan, &authorization)
        .await;
    server.abort();
    if slow_clock {
        assert_eq!(
            clock_reads.load(Ordering::SeqCst),
            1,
            "fixture must expire during the clock request"
        );
    }
    assert_eq!(
        writes.load(Ordering::SeqCst),
        0,
        "expired authorization cannot send a new order"
    );
    if partial_previous_session {
        let execution = result.expect("existing broker facts must survive a missing expired order");
        assert_eq!(execution.orders.len(), 1);
        assert_eq!(execution.orders[0].client_order_id, ids[&Asset::Qqq]);
        assert_eq!(execution.orders[0].filled_quantity_micros, 1_000_000);
    } else {
        assert!(
            result.is_err(),
            "no usable existing order or first-send authority"
        );
    }
}

fn test_plan() -> ExecutionPlan {
    let reference = |label: &str, kind| ArtifactRef {
        artifact_id: ArtifactId(ContentHash::of_bytes(label.as_bytes())),
        kind,
    };
    let now = Utc::now();
    let mut target = TargetPortfolio::zeroed();
    target.weights.insert(Asset::Qqq, WeightPpm(10_000));
    target.weights.insert(Asset::Soxx, WeightPpm(10_000));
    let mut plan = ExecutionPlan {
        schema_version: DOMAIN_SCHEMA_VERSION,
        decision_context: reference("decision", ArtifactKind::DecisionContext),
        account_snapshot: reference("account", ArtifactKind::NormalizedEvidence),
        quote_snapshot: reference("quotes", ArtifactKind::NormalizedEvidence),
        market_clock_snapshot: reference("clock", ArtifactKind::NormalizedEvidence),
        policy_hash: crate::ExecutionPolicy::default().policy_hash().unwrap(),
        maximum_total_notional: MoneyMicros(20_000_000),
        factor_exposure: FactorExposure::from_target(&target).unwrap(),
        target,
        orders: [Asset::Qqq, Asset::Soxx]
            .into_iter()
            .map(|asset| OrderIntent {
                extended_hours: false,
                asset,
                side: OrderSide::Buy,
                notional: MoneyMicros(10_000_000),
                limit_price: MoneyMicros(10_000_000),
            })
            .collect(),
        gross_exposure_ppm: 20_000,
        net_exposure_ppm: 20_000,
        turnover_ppm: 20_000,
        broker_session: "2026-09-09".into(),
        created_at: now,
        plan_hash: ContentHash::of_bytes(b"pending"),
    };
    plan.refresh_hash().unwrap();
    plan.validate().unwrap();
    plan
}

#[test]
fn submission_window_is_intersection_of_frozen_authority() {
    let plan = test_plan();
    let now = plan.created_at;
    let policy = crate::ExecutionPolicy::default();
    let (validity, account, quotes, clock) = test_sources(&plan, now);
    let build = |validity,
                 expiry,
                 account: &akzio_domain::AccountSnapshot,
                 quotes: &akzio_domain::QuoteSnapshot,
                 clock: &akzio_domain::MarketClockSnapshot| {
        PaperSubmissionAuthorization::from_frozen_sources(
            &plan, &policy, validity, expiry, account, quotes, clock,
        )
        .unwrap()
    };
    let fresh = build(
        Some(&validity),
        Some(validity.valid_until),
        &account,
        &quotes,
        &clock,
    );
    assert!(fresh.assert_current(&plan.plan_hash, now).is_ok());
    assert!(fresh
        .assert_current(&plan.plan_hash, now + chrono::Duration::seconds(5))
        .is_ok());
    assert!(fresh
        .assert_current(
            &plan.plan_hash,
            now + chrono::Duration::seconds(5) + chrono::Duration::nanoseconds(1)
        )
        .is_err());
    assert!(fresh
        .assert_current(&plan.plan_hash, now - chrono::Duration::nanoseconds(1))
        .is_err());
    assert!(fresh
        .assert_current(&ContentHash::of_bytes(b"different plan"), now)
        .is_err());
    assert!(
        build(None, Some(validity.valid_until), &account, &quotes, &clock)
            .assert_current(&plan.plan_hash, now)
            .is_err()
    );
    assert!(build(Some(&validity), None, &account, &quotes, &clock)
        .assert_current(&plan.plan_hash, now)
        .is_err());
    let earlier = now + chrono::Duration::seconds(1);
    assert!(
        build(Some(&validity), Some(earlier), &account, &quotes, &clock)
            .assert_current(&plan.plan_hash, earlier + chrono::Duration::nanoseconds(1))
            .is_err()
    );
    let mut short_decision = validity.clone();
    short_decision.valid_until = earlier;
    assert!(build(
        Some(&short_decision),
        Some(validity.valid_until),
        &account,
        &quotes,
        &clock
    )
    .assert_current(&plan.plan_hash, earlier + chrono::Duration::nanoseconds(1))
    .is_err());
    // Every individual source can independently expire the otherwise fresh plan.
    for source in ["account", "quote-envelope", "quote-event", "clock"] {
        let (mut account, mut quotes, mut clock) = (account.clone(), quotes.clone(), clock.clone());
        let stale = now - chrono::Duration::seconds(6);
        match source {
            "account" => account.observed_at = stale,
            "quote-envelope" => quotes.observed_at = stale,
            "quote-event" => quotes.quotes.get_mut(&Asset::Qqq).unwrap().observed_at = stale,
            "clock" => clock.observed_at = stale,
            _ => unreachable!(),
        }
        assert!(
            build(
                Some(&validity),
                Some(validity.valid_until),
                &account,
                &quotes,
                &clock
            )
            .assert_current(&plan.plan_hash, now)
            .is_err(),
            "{source}"
        );
    }
    let mut future_quotes = quotes.clone();
    future_quotes
        .quotes
        .get_mut(&Asset::Qqq)
        .unwrap()
        .observed_at = now + chrono::Duration::seconds(16);
    assert!(build(
        Some(&validity),
        Some(validity.valid_until),
        &account,
        &future_quotes,
        &clock
    )
    .assert_current(&plan.plan_hash, now)
    .is_err());
    let mut other_policy = policy;
    other_policy.max_quote_age_secs += 1;
    let mismatch = PaperSubmissionAuthorization::from_frozen_sources(
        &plan,
        &other_policy,
        Some(&validity),
        Some(validity.valid_until),
        &account,
        &quotes,
        &clock,
    )
    .unwrap();
    assert!(mismatch.assert_current(&plan.plan_hash, now).is_err());
}

fn frozen_account_fixture(
    positions_age_secs: i64,
) -> (
    ExecutionPlan,
    akzio_domain::AccountSnapshot,
    Artifact,
    Vec<(Artifact, submission_authorization::FrozenAccountComponent)>,
) {
    let plan = test_plan();
    let now = plan.created_at;
    let (_, account, _, _) = test_sources(&plan, now);
    let origin = akzio_domain::ArtifactOrigin {
        run_id: Some(akzio_domain::RunId::new()),
        task_id: None,
        attempt_id: None,
        contract_hash: None,
    };
    let mut components = Vec::new();
    let mut refs = Vec::new();
    for resource in [
        "paper.account".to_owned(),
        "paper.positions".into(),
        "paper.open_orders".into(),
        format!("paper.fills:{}", plan.broker_session),
    ] {
        let observed_at = if resource == "paper.positions" {
            now - chrono::Duration::seconds(positions_age_secs)
        } else {
            now
        };
        let reference = |kind| ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(
                format!("{resource}:{kind:?}").as_bytes(),
            )),
            kind,
        };
        let need = reference(ArtifactKind::EvidenceNeed);
        let raw = reference(ArtifactKind::RawEvidence);
        let payload = serde_json::json!({"source":"alpaca", "resource":resource, "observed_at":observed_at, "need":need, "raw":raw});
        let bytes = serde_json::to_vec(&payload).unwrap();
        let artifact = Artifact::new(
            ArtifactKind::NormalizedEvidence,
            akzio_domain::BlobRef {
                hash: ContentHash::of_bytes(&bytes),
                media_type: "application/json".into(),
                bytes: bytes.len() as u64,
            },
            "akzio.ingest.alpaca.normalized",
            ArtifactLifecycle::RunScoped,
            akzio_domain::ArtifactProvenance {
                source_family: "alpaca".into(),
                observed_at: Some(observed_at),
                retrieved_at: now,
                source_uri: Some("fixture://alpaca".into()),
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            Some(origin.clone()),
            vec![need, raw.clone()],
            now,
        )
        .unwrap();
        refs.push(raw);
        refs.push(ArtifactRef {
            artifact_id: artifact.artifact_id.clone(),
            kind: artifact.kind,
        });
        components.push((artifact, serde_json::from_value(payload).unwrap()));
    }
    let account_bytes = serde_json::to_vec(&account).unwrap();
    let snapshot = Artifact::new(
        ArtifactKind::NormalizedEvidence,
        akzio_domain::BlobRef {
            hash: ContentHash::of_bytes(&account_bytes),
            media_type: "application/json".into(),
            bytes: account_bytes.len() as u64,
        },
        "execution.snapshot.account",
        ArtifactLifecycle::Canonical,
        akzio_domain::ArtifactProvenance {
            source_family: "alpaca".into(),
            observed_at: Some(now),
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        },
        Some(origin),
        refs,
        now,
    )
    .unwrap();
    (plan, account, snapshot, components)
}

#[test]
fn historical_max_account_timestamp_cannot_extend_frozen_component_authority() {
    let (plan, account, snapshot, components) = frozen_account_fixture(6);
    let now = plan.created_at;
    let account_bytes = serde_json::to_vec(&account).unwrap();
    let original_snapshot = snapshot.clone();
    let original_plan = plan.clone();
    let observed =
        submission_authorization::frozen_account_observations(&snapshot, &account, &components);
    let mut authorization = test_authorization(&plan, now);
    authorization
        .restrict_account_observations(observed.as_deref(), &crate::ExecutionPolicy::default());
    assert!(
        authorization.assert_current(&plan.plan_hash, now).is_err(),
        "historical max timestamp must not hide six-second-old positions"
    );
    assert_eq!(snapshot, original_snapshot);
    assert_eq!(plan, original_plan);
    assert_eq!(ContentHash::of_bytes(&account_bytes), snapshot.blob.hash);
}

#[test]
fn fresh_frozen_account_lineage_allows_sending_but_incomplete_or_mixed_sources_do_not() {
    let (plan, account, snapshot, mut components) = frozen_account_fixture(0);
    let observations =
        submission_authorization::frozen_account_observations(&snapshot, &account, &components);
    let mut authorization = test_authorization(&plan, plan.created_at);
    authorization
        .restrict_account_observations(observations.as_deref(), &crate::ExecutionPolicy::default());
    assert!(authorization
        .assert_current(&plan.plan_hash, plan.created_at)
        .is_ok());
    let mut unknown_producer = snapshot.clone();
    unknown_producer.producer = "unrecognized.account.snapshot".into();
    assert!(submission_authorization::frozen_account_observations(
        &unknown_producer,
        &account,
        &components
    )
    .is_none());
    let missing = components.pop().unwrap();
    assert!(submission_authorization::frozen_account_observations(
        &snapshot,
        &account,
        &components
    )
    .is_none());
    components.push(missing);
    components[0].1.resource = "paper.unrelated".into();
    assert!(submission_authorization::frozen_account_observations(
        &snapshot,
        &account,
        &components
    )
    .is_none());
    components[0].1.resource = "paper.account".into();
    components[0].0.origin = None;
    assert!(submission_authorization::frozen_account_observations(
        &snapshot,
        &account,
        &components
    )
    .is_none());
}

#[test]
fn future_component_does_not_hide_behind_aggregate_min_timestamp() {
    let (plan, account, snapshot, components) = frozen_account_fixture(-16);
    let observations =
        submission_authorization::frozen_account_observations(&snapshot, &account, &components);
    assert!(
        observations.is_some(),
        "fixture has consistent immutable source metadata"
    );
    let mut authorization = test_authorization(&plan, plan.created_at);
    authorization
        .restrict_account_observations(observations.as_deref(), &crate::ExecutionPolicy::default());
    assert!(authorization
        .assert_current(&plan.plan_hash, plan.created_at)
        .is_err());
}

#[test]
fn governed_single_account_snapshot_keeps_its_original_time_bound() {
    let (plan, account, snapshot, components) = frozen_account_fixture(0);
    let components = components.into_iter().take(1).collect::<Vec<_>>();
    let (source, payload) = &components[0];
    let mut provenance = snapshot.provenance.clone();
    provenance.source_uri = source.provenance.source_uri.clone();
    let single = Artifact::new(
        snapshot.kind,
        snapshot.blob.clone(),
        snapshot.producer.clone(),
        snapshot.lifecycle,
        provenance,
        snapshot.origin.clone(),
        vec![
            payload.raw.clone(),
            ArtifactRef {
                artifact_id: source.artifact_id.clone(),
                kind: source.kind,
            },
        ],
        snapshot.created_at,
    )
    .unwrap();
    let observations =
        submission_authorization::frozen_account_observations(&single, &account, &components);
    assert!(observations.is_some());
    let mut authorization = test_authorization(&plan, plan.created_at);
    authorization
        .restrict_account_observations(observations.as_deref(), &crate::ExecutionPolicy::default());
    assert!(authorization
        .assert_current(&plan.plan_hash, plan.created_at)
        .is_ok());
    assert!(authorization
        .assert_current(
            &plan.plan_hash,
            plan.created_at + chrono::Duration::seconds(6)
        )
        .is_err());
}

#[tokio::test]
async fn expired_reprice_recovers_successor_but_never_sends_patch() {
    for successor_exists in [false, true] {
        let plan = test_plan();
        let authorization = test_authorization(&plan, Utc::now() - chrono::Duration::minutes(10));
        let intent = PaperReprice {
            schema_version: DOMAIN_SCHEMA_VERSION,
            reprice_id: akzio_domain::PaperRepriceId::new(),
            commitment: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::of_bytes(b"commitment")),
                kind: ArtifactKind::ExecutionCommitment,
            },
            prior_receipt: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::of_bytes(b"receipt")),
                kind: ArtifactKind::OrderReceipt,
            },
            asset: Asset::Qqq,
            prior_client_order_id: "fixture-r0".into(),
            replacement_client_order_id: "fixture-r1".into(),
            prior_broker_order_id: "original".into(),
            replacement_limit_price: MoneyMicros(10_000_000),
            created_at: plan.created_at,
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let patches = Arc::new(AtomicUsize::new(0));
        let observed = patches.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = [0; 4096];
                let size = stream.read(&mut bytes).await.unwrap();
                let header = String::from_utf8_lossy(&bytes[..size]);
                let successor = header.contains("client_order_id=fixture-r1 ");
                let (status, body) = if header.starts_with("PATCH ") {
                    observed.fetch_add(1, Ordering::SeqCst);
                    (503, serde_json::json!({"error":"unexpected patch"}))
                } else if successor && !successor_exists {
                    (404, serde_json::json!({"message":"not found"}))
                } else {
                    (
                        200,
                        serde_json::json!({"id":if successor {"successor"} else {"original"},"symbol":"QQQ","status":"new","client_order_id":if successor {"fixture-r1"} else {"fixture-r0"},"qty":"1","filled_qty":"0","filled_avg_price":null,"updated_at":"2026-09-09T14:00:00Z"}),
                    )
                };
                let body = body.to_string();
                stream.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            }
        });
        let paper = AlpacaPaper {
            client: Client::builder()
                .no_proxy()
                .timeout(std::time::Duration::from_secs(3))
                .build()
                .unwrap(),
            base_url: format!("http://{address}"),
            credentials: PaperCredentials {
                key_id: "fixture".into(),
                secret_key: "fixture".into(),
            },
        };
        let result = paper.replace_committed_order(&intent, &authorization).await;
        server.abort();
        assert_eq!(patches.load(Ordering::SeqCst), 0);
        if successor_exists {
            let receipt = result.unwrap();
            assert_eq!(receipt.client_order_id, "fixture-r1");
            assert!(receipt.reused);
        } else {
            assert!(matches!(result, Err(PaperError::SubmissionUnauthorized)));
        }
    }
}

#[test]
fn extended_order_wire_and_frozen_session_authorization() {
    let mut plan = test_plan();
    for order in &mut plan.orders {
        order.extended_hours = true;
    }
    plan.refresh_hash().unwrap();
    let request = order_request(&plan.orders[0], "stable-id").unwrap();
    assert_eq!(request["type"], "limit");
    assert_eq!(request["time_in_force"], "day");
    assert_eq!(request["extended_hours"], true);
    let now = Utc::now();
    let (validity, account, mut quotes, mut clock) = test_sources(&plan, now);
    clock.is_open = false;
    quotes.feed = Some("boats".into());
    clock.session = Some(akzio_domain::TradingSessionSnapshot {
        kind: akzio_domain::TradingSession::Overnight,
        trade_date: now.date_naive(),
        next_open: None,
        ends_at: Some(now + chrono::Duration::seconds(2)),
        overnight_assets: Asset::EXECUTABLE.into(),
    });
    let auth = PaperSubmissionAuthorization::from_frozen_sources(
        &plan,
        &crate::ExecutionPolicy::default(),
        Some(&validity),
        Some(validity.valid_until),
        &account,
        &quotes,
        &clock,
    )
    .unwrap();
    auth.assert_current(&plan.plan_hash, now).unwrap();
    assert!(auth
        .assert_current(&plan.plan_hash, now + chrono::Duration::seconds(3))
        .is_err());
    assert!(auth.matches_session(clock.session.as_ref().unwrap(), "irrelevant-old-run-date"));
    clock.session.as_mut().unwrap().kind = akzio_domain::TradingSession::PreMarket;
    assert!(!auth.matches_session(clock.session.as_ref().unwrap(), "irrelevant-old-run-date"));
    plan.orders[0].extended_hours = false;
    assert_eq!(
        order_request(&plan.orders[0], "stable-id").unwrap()["extended_hours"],
        false
    );
}

#[tokio::test]
async fn native_paper_acceptance_and_partial_fill_are_not_terminal() {
    let mut plan = test_plan();
    plan.orders.truncate(1);
    plan.orders[0].extended_hours = true;
    plan.refresh_hash().unwrap();
    let id = client_order_id(&plan.broker_session, &plan.plan_hash, 0, 0);
    let commitment = PaperCommitment {
        commitment_id: PaperCommitmentId("wire-fixture".into()),
        execution_context: plan.decision_context.clone(),
        plan_hash: plan.plan_hash.clone(),
        broker_session: plan.broker_session.clone(),
        client_order_ids: [(plan.orders[0].asset, id.clone())].into(),
        created_at: plan.created_at,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server_id = id.clone();
    let server = tokio::spawn(async move {
        let mut polls = 0;
        for _ in 0..3 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let header_end = loop {
                let mut buf = [0; 4096];
                let size = stream.read(&mut buf).await.unwrap();
                assert_ne!(size, 0);
                bytes.extend_from_slice(&buf[..size]);
                if let Some(p) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                    break p + 4;
                }
            };
            let header = String::from_utf8_lossy(&bytes[..header_end]);
            let post = header.starts_with("POST /v2/orders ");
            if post {
                let length: usize = header
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|v| v.trim().parse().unwrap())
                    })
                    .unwrap();
                while bytes.len() < header_end + length {
                    let mut buf = [0; 4096];
                    let n = stream.read(&mut buf).await.unwrap();
                    bytes.extend_from_slice(&buf[..n]);
                }
                let request: Value = serde_json::from_slice(&bytes[header_end..]).unwrap();
                assert_eq!(request["extended_hours"], true);
                assert_eq!(request["type"], "limit");
                assert_eq!(request["time_in_force"], "day");
                assert_eq!(request["client_order_id"], server_id);
            } else {
                assert!(header.starts_with("GET /v2/orders/native-fixture "));
                polls += 1;
            }
            let (state, filled) = if post {
                ("accepted", "0")
            } else if polls == 1 {
                ("partially_filled", "0.5")
            } else {
                ("filled", "1")
            };
            let body = serde_json::json!({"id":"native-fixture","client_order_id":server_id,"symbol":"QQQ",
                "status":state,"qty":"1","filled_qty":filled,"filled_avg_price":if post { Value::Null } else { serde_json::json!("10") },
                "updated_at":Utc::now()}).to_string();
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        }
    });
    // Private test-only construction; the public constructor still rejects localhost.
    let broker = AlpacaPaper {
        client: Client::builder().no_proxy().build().unwrap(),
        base_url: format!("http://{address}"),
        credentials: PaperCredentials {
            key_id: "fixture".into(),
            secret_key: "fixture".into(),
        },
    };
    let accepted = broker.submit_order(&plan.orders[0], &id, 0).await.unwrap();
    let execution = PaperExecution {
        plan_hash: plan.plan_hash.clone(),
        orders: vec![accepted],
    };
    assert!(!paper_dispatch::execution_is_settled(&commitment, &execution).unwrap());
    let partial = broker
        .reconcile_committed(&commitment, &execution)
        .await
        .unwrap();
    assert_eq!(partial.orders[0].filled_quantity_micros, 500_000);
    assert!(!paper_dispatch::execution_is_settled(&commitment, &partial).unwrap());
    let filled = broker
        .reconcile_committed(&commitment, &partial)
        .await
        .unwrap();
    assert!(paper_dispatch::execution_is_settled(&commitment, &filled).unwrap());
    server.await.unwrap();
}
