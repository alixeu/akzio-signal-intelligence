#[tokio::test]
async fn native_web_transport_requires_allowlisted_citations() {
    let client = ModelClient::Fixture(serde_json::json!({
        "output_text": "DFII10 evidence",
        "citations": [{
            "url": "https://fred.stlouisfed.org/series/DFII10",
            "title": "FRED",
            "text": "real yield"
        }]
    }));
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
    assert!(evidence.provenance.citations.is_empty());
    assert_eq!(evidence.quality.completeness_ppm, 0);
    assert!(!evidence.quality.citations_complete);
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
