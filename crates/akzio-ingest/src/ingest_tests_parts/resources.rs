fn native_web_fixture(output_text: &str, url: &str) -> serde_json::Value {
    serde_json::json!({
        "output_text": output_text,
        "output": [{
            "type": "web_search_call",
            "status": "completed",
            "action": {
                "type": "search",
                "query": "bounded evidence",
                "sources": [{"url": url}]
            }
        }],
        "citations": [{"url": url, "title": "source", "text": "evidence"}]
    })
}

#[tokio::test]
async fn native_web_transport_requires_allowlisted_citations() {
    let client = ModelClient::Fixture(native_web_fixture(
        "DFII10 evidence",
        "https://fred.stlouisfed.org/series/DFII10",
    ));
    let transport = model_native_web_evidence_transport(client, EvidenceSource::Fred);
    let evidence = transport
        .acquire(&EvidenceRequest {
            source: EvidenceSource::Fred,
            resource: "series:DFII10".to_owned(),
            max_age: Duration::minutes(5),
        })
        .await
        .unwrap();
    assert_eq!(
        evidence.provenance.source_uri,
        "https://fred.stlouisfed.org/series/DFII10"
    );
    assert_eq!(evidence.provenance.citations.len(), 1);
    let citation = &evidence.provenance.citations[0];
    assert_eq!(
        &evidence.raw[citation.start_byte..citation.end_byte],
        citation.quote.as_bytes()
    );
    assert_eq!(evidence.quality.completeness_ppm, 1_000_000);
    assert!(evidence.quality.citations_complete);
}

#[tokio::test]
async fn native_web_transport_reports_policy_failures_without_transport_class() {
    let client = ModelClient::Fixture(native_web_fixture(
        "news",
        "https://example.com/story",
    ));
    let transport = model_native_web_evidence_transport(client, EvidenceSource::NewsWeb);
    let error = transport
        .acquire(&EvidenceRequest {
            source: EvidenceSource::NewsWeb,
            resource: "news:QQQ:2026-08-20:2026-08-27:market".to_owned(),
            max_age: Duration::minutes(5),
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        EvidenceAdapterError::Policy { ref resource, ref reason, .. }
            if resource == "news:QQQ:2026-08-20:2026-08-27:market"
                && reason.contains("https://example.com/story")
    ));
}

#[tokio::test]
async fn native_web_transport_accepts_allowlisted_query_uri() {
    let client = ModelClient::Fixture(native_web_fixture(
        "news",
        "https://www.reuters.com/story?utm_source=fixture",
    ));
    let transport = model_native_web_evidence_transport(client, EvidenceSource::NewsWeb);
    let evidence = transport
        .acquire(&EvidenceRequest {
            source: EvidenceSource::NewsWeb,
            resource: "news:QQQ:2026-08-20:2026-08-27:market".to_owned(),
            max_age: Duration::minutes(5),
        })
        .await
        .unwrap();
    assert_eq!(
        evidence.provenance.source_uri,
        "https://www.reuters.com/story?utm_source=fixture"
    );

    let client = ModelClient::Fixture(native_web_fixture(
        "news",
        "https://m.etfchannel.com/story/?utm_source=openai",
    ));
    let transport = model_native_web_evidence_transport(client, EvidenceSource::NewsWeb);
    assert!(transport
        .acquire(&EvidenceRequest {
            source: EvidenceSource::NewsWeb,
            resource: "news:SOXX:2026-08-20:2026-08-27:market".to_owned(),
            max_age: Duration::minutes(5),
        })
        .await
        .is_ok());

    let client = ModelClient::Fixture(native_web_fixture(
        "news",
        "https://www.etf.com/sections/news/qqq-rises?utm_source=openai",
    ));
    let transport = model_native_web_evidence_transport(client, EvidenceSource::NewsWeb);
    assert!(transport
        .acquire(&EvidenceRequest {
            source: EvidenceSource::NewsWeb,
            resource: "news:QQQ:2026-08-20:2026-08-27:market".to_owned(),
            max_age: Duration::minutes(5),
        })
        .await
        .is_ok());

    let client = ModelClient::Fixture(native_web_fixture(
        "news",
        "https://www.etfchannel.com/story/?utm_source=openai",
    ));
    let transport = model_native_web_evidence_transport(client, EvidenceSource::NewsWeb);
    assert!(transport
        .acquire(&EvidenceRequest {
            source: EvidenceSource::NewsWeb,
            resource: "news:TQQQ:2026-08-20:2026-08-27:market".to_owned(),
            max_age: Duration::minutes(5),
        })
        .await
        .is_ok());
}

#[test]
fn governed_resource_schema_bounds_sources_windows_and_assets() {
    assert_eq!(
        GovernedResource::parse(EvidenceSource::Alpaca, "bars:QQQ:1d:2026-08-01:6").unwrap(),
        GovernedResource::AlpacaBars {
            asset: Asset::Qqq,
            start: Some(NaiveDate::from_ymd_opt(2026, 8, 1).unwrap()),
            limit: 6,
        }
    );
    assert!(GovernedResource::parse(EvidenceSource::Alpaca, "bars:SPY:1d").is_err());
    assert!(GovernedResource::parse(EvidenceSource::Alpaca, "bars:QQQ:5m").is_err());
    assert!(
        GovernedResource::parse(EvidenceSource::Alpaca, "observer.qqq_history:1d:2026-08-20")
            .is_err()
    );
    assert!(
        GovernedResource::parse(EvidenceSource::Fred, "series:DFII10:2026-08-01:2028-08-01")
            .is_err()
    );
    assert_eq!(
        GovernedResource::parse(EvidenceSource::NewsWeb, "news:semiconductor supply chain")
            .unwrap(),
        GovernedResource::NewsWeb {
            query: "semiconductor supply chain".to_owned(),
        }
    );
}

#[test]
fn daily_bar_quality_gate_rejects_missing_ohlcv_weekends_and_duplicates() {
    let valid = serde_json::json!({
        "bars": [
            {"t":"2026-08-10T20:00:00Z","o":100.0,"h":105.0,"l":99.0,"c":103.0,"v":1000}
        ]
    });
    validate_daily_bar_payload(&valid).unwrap();

    let mut missing = valid;
    missing["bars"][0].as_object_mut().unwrap().remove("v");
    assert!(validate_daily_bar_payload(&missing).is_err());

    let weekend = serde_json::json!({
        "bars": [
            {"t":"2026-08-09T20:00:00Z","o":100.0,"h":105.0,"l":99.0,"c":103.0,"v":1000}
        ]
    });
    assert!(validate_daily_bar_payload(&weekend).is_err());

    let duplicate = serde_json::json!({
        "bars": [
            {"t":"2026-08-10T20:00:00Z","o":100,"h":105,"l":99,"c":103,"v":1000},
            {"t":"2026-08-10T21:00:00Z","o":100,"h":106,"l":98,"c":104,"v":1100}
        ]
    });
    assert!(validate_daily_bar_payload(&duplicate).is_err());
}
