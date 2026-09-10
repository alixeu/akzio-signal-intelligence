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

#[tokio::test]
async fn t11_accepted_request_lost_ack_recovers_by_original_id_without_second_order() {
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
        policy_hash: ContentHash::of_bytes(b"policy"),
        maximum_total_notional: MoneyMicros(10_000_000),
        factor_exposure: FactorExposure::from_target(&target).unwrap(),
        target,
        orders: vec![OrderIntent {
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
                    serde_json::json!({"is_open":true,"timestamp":"2026-09-09T10:00:00-04:00"}),
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
            let response=format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
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
    assert!(matches!(
        build().execute_committed(&commitment, &plan).await,
        Err(PaperError::Transport { .. })
    ));
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    let recovered = build().execute_committed(&commitment, &plan).await.unwrap();
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
