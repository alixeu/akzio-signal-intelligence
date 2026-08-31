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
        "citations": [{"url": url, "title": "source", "text": output_text}]
    })
}

#[derive(Clone)]
struct FixtureSourceDocumentFetcher {
    expected_uri: Option<String>,
    snapshot: Option<adapters::SourceDocumentSnapshot>,
}

impl adapters::SourceDocumentFetcher for FixtureSourceDocumentFetcher {
    fn fetch<'a>(
        &'a self,
        uri: &'a str,
    ) -> BoxFuture<'a, Result<adapters::SourceDocumentSnapshot, EvidenceAdapterError>> {
        let result = self
            .expected_uri
            .as_deref()
            .is_none_or(|expected| expected == uri)
            .then(|| self.snapshot.clone())
            .flatten()
            .ok_or_else(|| {
                EvidenceAdapterError::Transport("fixture source document unavailable".to_owned())
            });
        Box::pin(async move { result })
    }
}

fn unavailable_source_document_fetcher() -> std::sync::Arc<dyn adapters::SourceDocumentFetcher> {
    std::sync::Arc::new(FixtureSourceDocumentFetcher {
        expected_uri: None,
        snapshot: None,
    })
}

fn source_document_fetcher(
    uri: &str,
    body: &[u8],
) -> std::sync::Arc<dyn adapters::SourceDocumentFetcher> {
    std::sync::Arc::new(FixtureSourceDocumentFetcher {
        expected_uri: Some(uri.to_owned()),
        snapshot: Some(adapters::SourceDocumentSnapshot {
            body: body.to_vec(),
            media_type: "text/html".to_owned(),
            fetched_at: Utc::now(),
            status_code: 200,
            etag: Some("fixture-etag".to_owned()),
            last_modified: Some("Sat, 29 Aug 2026 12:00:00 GMT".to_owned()),
        }),
    })
}

fn unverified_news_transport(client: ModelClient) -> std::sync::Arc<dyn AsyncEvidenceAdapter> {
    model_native_web_evidence_transport_with_fetcher(
        client,
        EvidenceSource::NewsWeb,
        unavailable_source_document_fetcher(),
    )
}

#[tokio::test]
async fn native_web_transport_requires_allowlisted_citations() {
    let client = ModelClient::Fixture(native_web_fixture(
        "DFII10 evidence",
        "https://fred.stlouisfed.org/series/DFII10",
    ));
    let transport = model_native_web_evidence_transport(client, EvidenceSource::Fred).unwrap();
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
    assert_eq!(evidence.quality.completeness_ppm, 250_000);
    assert!(!evidence.quality.citations_complete);
}

#[tokio::test]
async fn news_web_transport_cites_an_independent_source_snapshot() {
    let uri = "https://www.reuters.com/story";
    let body = b"<html><body>independent evidence snapshot</body></html>";
    let client = ModelClient::Fixture(native_web_fixture("independent evidence snapshot", uri));
    let transport = model_native_web_evidence_transport_with_fetcher(
        client,
        EvidenceSource::NewsWeb,
        source_document_fetcher(uri, body),
    );
    let evidence = transport
        .acquire(&EvidenceRequest {
            source: EvidenceSource::NewsWeb,
            resource: "news:QQQ:2026-08-20:2026-08-27:market".to_owned(),
            max_age: Duration::minutes(5),
        })
        .await
        .unwrap();

    assert_eq!(evidence.raw, body);
    assert_eq!(evidence.media_type, "text/html");
    assert_eq!(evidence.quality.completeness_ppm, 1_000_000);
    assert!(evidence.quality.citations_complete);
    assert_eq!(evidence.provenance.citations.len(), 1);
    let citation = &evidence.provenance.citations[0];
    assert_eq!(citation.quote, "independent evidence snapshot");
    assert_eq!(
        &evidence.raw[citation.start_byte..citation.end_byte],
        citation.quote.as_bytes()
    );
    let content_hash = ContentHash::of_bytes(body).to_string();
    assert_eq!(
        evidence.normalized["source_document"]["status"],
        "source_snapshot_exact_quote"
    );
    assert_eq!(
        evidence.normalized["source_document"]["content_hash"],
        content_hash
    );
    assert_eq!(
        evidence.provenance.revision.as_deref(),
        Some(content_hash.as_str())
    );
}

#[tokio::test]
async fn news_web_source_snapshot_is_sealed_through_v2_store() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let started_at = Utc::now();
    install_run(&store, started_at, 1);
    let claimed = store
        .claim_next_task(
            "news-source-snapshot-worker",
            started_at,
            Duration::seconds(30),
        )
        .unwrap()
        .unwrap();
    let resource = "news:QQQ:2026-08-20:2026-08-27:market";
    let need = evidence_need_for(
        &store,
        &claimed,
        EvidenceSource::NewsWeb.as_str(),
        resource,
        300,
        started_at,
    );
    let uri = "https://www.reuters.com/story";
    let body = b"<html><body>independent evidence snapshot</body></html>";
    let adapter = model_native_web_evidence_transport_with_fetcher(
        ModelClient::Fixture(native_web_fixture("independent evidence snapshot", uri)),
        EvidenceSource::NewsWeb,
        source_document_fetcher(uri, body),
    );
    let request = EvidenceRequest {
        source: EvidenceSource::NewsWeb,
        resource: resource.to_owned(),
        max_age: Duration::seconds(300),
    };
    let runtime = EvidenceRuntime::new(store.clone(), [EvidenceSource::NewsWeb]);
    let acquired = runtime
        .acquire_validated_async(
            &claimed.permit,
            &need,
            &request,
            adapter.as_ref(),
            started_at,
        )
        .await
        .unwrap();
    let committed_at = Utc::now();
    let bundle = runtime
        .materialize_validated(&claimed.permit, &need, &request, acquired, committed_at)
        .unwrap();
    store
        .commit_attempt(
            &claimed.permit,
            &[bundle.raw.clone(), bundle.normalized.clone()],
            TaskStatus::Succeeded,
            committed_at,
        )
        .unwrap();

    assert_eq!(store.read_blob(&bundle.raw.blob).unwrap(), body);
    let payload: NormalizedEvidencePayload =
        serde_json::from_slice(&store.read_blob(&bundle.normalized.blob).unwrap()).unwrap();
    assert_eq!(payload.raw.artifact_id, bundle.raw.artifact_id);
    assert_eq!(payload.provenance.source_uri, uri);
    assert_eq!(payload.provenance.citations.len(), 1);
    let citation = &payload.provenance.citations[0];
    assert_eq!(
        &body[citation.start_byte..citation.end_byte],
        citation.quote.as_bytes()
    );
    assert_eq!(
        payload.value["source_document"]["content_hash"],
        ContentHash::of_bytes(body).to_string()
    );
}

#[tokio::test]
async fn news_web_multi_source_snapshot_stays_partial() {
    let primary_uri = "https://apnews.com/article";
    let secondary_uri = "https://www.reuters.com/story";
    let excerpt = "independent evidence snapshot";
    let body = b"<html><body>independent evidence snapshot</body></html>";
    let client = ModelClient::Fixture(serde_json::json!({
        "output_text": excerpt,
        "output": [{
            "type": "web_search_call",
            "status": "completed",
            "action": {
                "type": "search",
                "query": "bounded evidence",
                "sources": [{"url": primary_uri}, {"url": secondary_uri}]
            }
        }],
        "citations": [
            {"url": primary_uri, "title": "primary", "text": excerpt},
            {
                "url": secondary_uri,
                "title": "secondary",
                "text": "secondary independent source excerpt"
            }
        ]
    }));
    let transport = model_native_web_evidence_transport_with_fetcher(
        client,
        EvidenceSource::NewsWeb,
        source_document_fetcher(primary_uri, body),
    );
    let evidence = transport
        .acquire(&EvidenceRequest {
            source: EvidenceSource::NewsWeb,
            resource: "news:QQQ:2026-08-20:2026-08-27:market".to_owned(),
            max_age: Duration::minutes(5),
        })
        .await
        .unwrap();

    assert_eq!(evidence.raw, body);
    assert_eq!(evidence.provenance.citations.len(), 1);
    assert_eq!(evidence.quality.completeness_ppm, 500_000);
    assert!(!evidence.quality.citations_complete);
    assert_eq!(
        evidence.normalized["source_document"]["status"],
        "source_snapshot_partial_citations"
    );
}

#[tokio::test]
async fn news_web_snapshot_without_exact_quote_stays_incomplete() {
    let uri = "https://www.reuters.com/story";
    let body = b"<html><body>different source text</body></html>";
    let client = ModelClient::Fixture(native_web_fixture("news", uri));
    let transport = model_native_web_evidence_transport_with_fetcher(
        client,
        EvidenceSource::NewsWeb,
        source_document_fetcher(uri, body),
    );
    let evidence = transport
        .acquire(&EvidenceRequest {
            source: EvidenceSource::NewsWeb,
            resource: "news:QQQ:2026-08-20:2026-08-27:market".to_owned(),
            max_age: Duration::minutes(5),
        })
        .await
        .unwrap();

    assert_eq!(evidence.raw, body);
    assert!(evidence.provenance.citations.is_empty());
    assert_eq!(evidence.quality.completeness_ppm, 500_000);
    assert!(!evidence.quality.citations_complete);
    assert_eq!(
        evidence.normalized["source_document"]["status"],
        "source_snapshot_without_exact_quote"
    );
}

#[tokio::test]
async fn native_web_transport_reports_policy_failures_without_transport_class() {
    let client = ModelClient::Fixture(native_web_fixture("news", "https://example.com/story"));
    let transport = unverified_news_transport(client);
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
    let transport = unverified_news_transport(client);
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
    assert_eq!(evidence.media_type, "application/json");
    assert_eq!(evidence.quality.completeness_ppm, 250_000);
    assert!(!evidence.quality.citations_complete);
    assert_eq!(
        evidence.normalized["source_document"]["status"],
        "provider_attributed_unverified"
    );
    assert!(evidence.normalized["source_document"]["snapshot_error"].is_string());

    let client = ModelClient::Fixture(native_web_fixture(
        "news",
        "https://m.etfchannel.com/story/?utm_source=openai",
    ));
    let transport = unverified_news_transport(client);
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
    let transport = unverified_news_transport(client);
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
    let transport = unverified_news_transport(client);
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
