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

fn native_web_multi_fixture(citations: &[(&str, &str)]) -> serde_json::Value {
    let sources = citations
        .iter()
        .map(|(url, _)| serde_json::json!({"url": url}))
        .collect::<Vec<_>>();
    let citations = citations
        .iter()
        .map(|(url, excerpt)| {
            serde_json::json!({"url": url, "title": "source", "text": excerpt})
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "output_text": "multi-source evidence",
        "output": [{
            "type": "web_search_call",
            "status": "completed",
            "action": {
                "type": "search",
                "query": "bounded evidence",
                "sources": sources,
            }
        }],
        "citations": citations,
    })
}

#[derive(Clone)]
struct FixtureSourceDocumentFetcher {
    results: std::collections::BTreeMap<
        String,
        Result<adapters::SourceDocumentSnapshot, adapters::SourceDocumentFetchError>,
    >,
}

impl adapters::SourceDocumentFetcher for FixtureSourceDocumentFetcher {
    fn fetch<'a>(
        &'a self,
        uri: &'a str,
    ) -> BoxFuture<'a, Result<adapters::SourceDocumentSnapshot, adapters::SourceDocumentFetchError>> {
        let result = self.results.get(uri).cloned().unwrap_or_else(|| {
            Err(
                adapters::SourceDocumentFetchError::new(
                    adapters::SourceDocumentFailureKind::Transport,
                    "fixture source document unavailable",
                ),
            )
        });
        Box::pin(async move { result })
    }
}

fn unavailable_source_document_fetcher() -> std::sync::Arc<dyn adapters::SourceDocumentFetcher> {
    std::sync::Arc::new(FixtureSourceDocumentFetcher {
        results: std::collections::BTreeMap::new(),
    })
}

fn fixture_source_snapshot(body: &[u8]) -> adapters::SourceDocumentSnapshot {
    adapters::SourceDocumentSnapshot {
        body: body.to_vec(),
        media_type: "text/html".to_owned(),
        fetched_at: Utc::now(),
        status_code: 200,
        etag: Some("fixture-etag".to_owned()),
        last_modified: Some("Sat, 29 Aug 2026 12:00:00 GMT".to_owned()),
    }
}

fn source_document_results_fetcher(
    results: impl IntoIterator<
        Item = (
            String,
            Result<adapters::SourceDocumentSnapshot, adapters::SourceDocumentFetchError>,
        ),
    >,
) -> std::sync::Arc<dyn adapters::SourceDocumentFetcher> {
    std::sync::Arc::new(FixtureSourceDocumentFetcher {
        results: results.into_iter().collect(),
    })
}

fn source_document_fetcher(
    uri: &str,
    body: &[u8],
) -> std::sync::Arc<dyn adapters::SourceDocumentFetcher> {
    source_document_results_fetcher([(uri.to_owned(), Ok(fixture_source_snapshot(body)))])
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
        "source_snapshots_complete"
    );
    assert_eq!(
        evidence.normalized["source_document"]["sources"][0]["content_hash"],
        content_hash
    );
    assert!(evidence.provenance.revision.is_some());
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
        payload.value["source_document"]["sources"][0]["content_hash"],
        ContentHash::of_bytes(body).to_string()
    );
    let source_blob: BlobRef = serde_json::from_value(
        payload.value["source_document"]["sources"][0]["blob"].clone(),
    )
    .unwrap();
    assert_eq!(store.read_blob(&source_blob).unwrap(), body);
}

#[tokio::test]
async fn news_web_multi_source_snapshots_get_independent_cas_refs() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let started_at = Utc::now();
    install_run(&store, started_at, 1);
    let claimed = store
        .claim_next_task(
            "news-multi-source-worker",
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
    let first_uri = "https://apnews.com/article";
    let second_uri = "https://www.reuters.com/story";
    let failed_uri = "https://www.etf.com/sections/news/missing";
    let first_quote = "first persisted source quote";
    let second_quote = "second persisted source quote";
    let failed_quote = "unavailable persisted source quote";
    let first_body = format!("<html>{first_quote}</html>").into_bytes();
    let second_body = format!("<html>{second_quote}</html>").into_bytes();
    let adapter = model_native_web_evidence_transport_with_fetcher(
        ModelClient::Fixture(native_web_multi_fixture(&[
            (first_uri, first_quote),
            (second_uri, second_quote),
            (failed_uri, failed_quote),
        ])),
        EvidenceSource::NewsWeb,
        source_document_results_fetcher([
            (first_uri.to_owned(), Ok(fixture_source_snapshot(&first_body))),
            (
                second_uri.to_owned(),
                Ok(fixture_source_snapshot(&second_body)),
            ),
        ]),
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
    let mut invalid_binding = acquired.clone();
    invalid_binding.normalized["source_document"]["sources"][0]["claim_binding"]
        ["source_end_byte"] = serde_json::json!(usize::MAX);
    assert!(matches!(
        runtime.materialize_validated(
            &claimed.permit,
            &need,
            &request,
            invalid_binding,
            Utc::now(),
        ),
        Err(EvidenceRuntimeError::InvalidCitation)
    ));
    let bundle = runtime
        .materialize_validated(&claimed.permit, &need, &request, acquired, Utc::now())
        .unwrap();
    let payload: NormalizedEvidencePayload =
        serde_json::from_slice(&store.read_blob(&bundle.normalized.blob).unwrap()).unwrap();
    let sources = payload.value["source_document"]["sources"]
        .as_array()
        .unwrap();
    let source = |uri: &str| {
        sources
            .iter()
            .find(|source| source["canonical_url"] == uri)
            .unwrap()
    };
    let first_source = source(first_uri);
    let second_source = source(second_uri);
    let failed_source = source(failed_uri);
    let first_blob: BlobRef = serde_json::from_value(first_source["blob"].clone()).unwrap();
    let second_blob: BlobRef = serde_json::from_value(second_source["blob"].clone()).unwrap();

    assert_eq!(store.read_blob(&first_blob).unwrap(), first_body);
    assert_eq!(store.read_blob(&second_blob).unwrap(), second_body);
    assert_ne!(first_blob.hash, second_blob.hash);
    assert_ne!(first_source["snapshot_id"], second_source["snapshot_id"]);
    assert_eq!(failed_source["status"], "fetch_failed");
    assert_eq!(failed_source["failure_kind"], "transport");
    assert!(failed_source.get("blob").is_none());
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
        "source_snapshots_partial"
    );
    let sources = evidence.normalized["source_document"]["sources"]
        .as_array()
        .unwrap();
    assert_eq!(sources.len(), 2);
    assert_eq!(sources[0]["status"], "snapshot");
    assert_eq!(sources[1]["status"], "fetch_failed");
    assert_eq!(sources[1]["failure_kind"], "transport");
}

#[tokio::test]
async fn news_web_two_citations_bind_to_independent_snapshots() {
    let first_uri = "https://apnews.com/article";
    let second_uri = "https://www.reuters.com/story";
    let first_quote = "first independent source quote";
    let second_quote = "second independent source quote";
    let first_body = format!("<html><body>{first_quote}</body></html>").into_bytes();
    let second_body = format!("<html><body>{second_quote}</body></html>").into_bytes();
    let client = ModelClient::Fixture(native_web_multi_fixture(&[
        (first_uri, first_quote),
        (second_uri, second_quote),
    ]));
    let fetcher = source_document_results_fetcher([
        (first_uri.to_owned(), Ok(fixture_source_snapshot(&first_body))),
        (
            second_uri.to_owned(),
            Ok(fixture_source_snapshot(&second_body)),
        ),
    ]);
    let transport = model_native_web_evidence_transport_with_fetcher(
        client,
        EvidenceSource::NewsWeb,
        fetcher,
    );

    let evidence = transport
        .acquire(&EvidenceRequest {
            source: EvidenceSource::NewsWeb,
            resource: "news:QQQ:2026-08-20:2026-08-27:market".to_owned(),
            max_age: Duration::minutes(5),
        })
        .await
        .unwrap();

    assert_eq!(evidence.raw, [first_body.clone(), second_body.clone()].concat());
    assert_eq!(evidence.provenance.citations.len(), 2);
    assert_eq!(evidence.quality.completeness_ppm, 1_000_000);
    assert!(evidence.quality.citations_complete);
    for citation in &evidence.provenance.citations {
        assert_eq!(
            &evidence.raw[citation.start_byte..citation.end_byte],
            citation.quote.as_bytes()
        );
    }
    let sources = evidence.normalized["source_document"]["sources"]
        .as_array()
        .unwrap();
    assert_eq!(sources.len(), 2);
    assert_eq!(sources[0]["canonical_url"], first_uri);
    assert_eq!(sources[1]["canonical_url"], second_uri);
    assert_eq!(sources[0]["status_code"], 200);
    assert!(sources[0]["fetched_at"].is_string());
    assert_eq!(sources[0]["etag"], "fixture-etag");
    assert_eq!(
        sources[0]["last_modified"],
        "Sat, 29 Aug 2026 12:00:00 GMT"
    );
    assert_eq!(sources[0]["claim_binding"]["quote"], first_quote);
    assert_eq!(sources[1]["claim_binding"]["quote"], second_quote);
    assert_ne!(sources[0]["snapshot_id"], sources[1]["snapshot_id"]);

    let mut invalid = evidence.clone();
    invalid.provenance.citations[0].end_byte = invalid.raw.len() + 1;
    assert!(matches!(
        invalid.provenance.validate(
            &invalid.raw,
            &invalid.source_uri,
            invalid.observed_at,
        ),
        Err(EvidenceRuntimeError::InvalidCitation)
    ));
}

#[tokio::test]
async fn news_web_identical_bodies_keep_distinct_source_identity() {
    let first_uri = "https://apnews.com/article";
    let second_uri = "https://www.reuters.com/story";
    let quote = "shared independent source quote";
    let body = format!("<html><body>{quote}</body></html>").into_bytes();
    let transport = model_native_web_evidence_transport_with_fetcher(
        ModelClient::Fixture(native_web_multi_fixture(&[
            (first_uri, quote),
            (second_uri, quote),
        ])),
        EvidenceSource::NewsWeb,
        source_document_results_fetcher([
            (first_uri.to_owned(), Ok(fixture_source_snapshot(&body))),
            (second_uri.to_owned(), Ok(fixture_source_snapshot(&body))),
        ]),
    );

    let evidence = transport
        .acquire(&EvidenceRequest {
            source: EvidenceSource::NewsWeb,
            resource: "news:QQQ:2026-08-20:2026-08-27:market".to_owned(),
            max_age: Duration::minutes(5),
        })
        .await
        .unwrap();
    let sources = evidence.normalized["source_document"]["sources"]
        .as_array()
        .unwrap();

    assert_eq!(sources[0]["content_hash"], sources[1]["content_hash"]);
    assert_ne!(sources[0]["snapshot_id"], sources[1]["snapshot_id"]);
    assert_eq!(sources[0]["bundle_start_byte"], 0);
    assert_eq!(sources[0]["bundle_end_byte"], body.len());
    assert_eq!(sources[1]["bundle_start_byte"], body.len());
    assert_eq!(sources[1]["bundle_end_byte"], body.len() * 2);
}

#[tokio::test]
async fn news_web_repeated_materialization_is_idempotent() {
    let first_uri = "https://apnews.com/article";
    let second_uri = "https://www.reuters.com/story";
    let first_quote = "first repeatable source quote";
    let second_quote = "second repeatable source quote";
    let first_body = format!("<html>{first_quote}</html>").into_bytes();
    let second_body = format!("<html>{second_quote}</html>").into_bytes();
    let transport = model_native_web_evidence_transport_with_fetcher(
        ModelClient::Fixture(native_web_multi_fixture(&[
            (first_uri, first_quote),
            (second_uri, second_quote),
        ])),
        EvidenceSource::NewsWeb,
        source_document_results_fetcher([
            (first_uri.to_owned(), Ok(fixture_source_snapshot(&first_body))),
            (
                second_uri.to_owned(),
                Ok(fixture_source_snapshot(&second_body)),
            ),
        ]),
    );
    let request = EvidenceRequest {
        source: EvidenceSource::NewsWeb,
        resource: "news:QQQ:2026-08-20:2026-08-27:market".to_owned(),
        max_age: Duration::minutes(5),
    };

    let first = transport.acquire(&request).await.unwrap();
    let second = transport.acquire(&request).await.unwrap();

    assert_eq!(first.raw, second.raw);
    assert_eq!(first.provenance.revision, second.provenance.revision);
    assert_eq!(first.provenance.dedupe_key, second.provenance.dedupe_key);
    assert_eq!(
        first.normalized["source_document"],
        second.normalized["source_document"]
    );
}

#[tokio::test]
async fn news_web_quote_found_only_in_wrong_source_is_not_verified() {
    let first_uri = "https://apnews.com/article";
    let second_uri = "https://www.reuters.com/story";
    let misplaced_quote = "quote attributed to the first source";
    let second_quote = "quote attributed to the second source";
    let first_body = b"<html><body>unrelated first source</body></html>";
    let second_body = format!(
        "<html><body>{misplaced_quote}; {second_quote}</body></html>"
    )
    .into_bytes();
    let transport = model_native_web_evidence_transport_with_fetcher(
        ModelClient::Fixture(native_web_multi_fixture(&[
            (first_uri, misplaced_quote),
            (second_uri, second_quote),
        ])),
        EvidenceSource::NewsWeb,
        source_document_results_fetcher([
            (
                first_uri.to_owned(),
                Ok(fixture_source_snapshot(first_body)),
            ),
            (
                second_uri.to_owned(),
                Ok(fixture_source_snapshot(&second_body)),
            ),
        ]),
    );

    let evidence = transport
        .acquire(&EvidenceRequest {
            source: EvidenceSource::NewsWeb,
            resource: "news:QQQ:2026-08-20:2026-08-27:market".to_owned(),
            max_age: Duration::minutes(5),
        })
        .await
        .unwrap();
    let sources = evidence.normalized["source_document"]["sources"]
        .as_array()
        .unwrap();

    assert_eq!(evidence.provenance.citations.len(), 1);
    assert_eq!(evidence.provenance.citations[0].quote, second_quote);
    assert_eq!(evidence.quality.completeness_ppm, 500_000);
    assert!(!evidence.quality.citations_complete);
    assert_eq!(
        sources[0]["claim_binding"]["status"],
        "missing_exact_quote"
    );
    assert_eq!(sources[1]["claim_binding"]["status"], "exact_quote");
}

#[tokio::test]
async fn news_web_snapshot_without_exact_quote_stays_incomplete() {
    let uri = "https://www.reuters.com/story";
    let body = b"<html><body>different source text</body></html>";
    let client = ModelClient::Fixture(native_web_fixture(
        "provider statement absent from the source body",
        uri,
    ));
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
    assert_eq!(evidence.quality.completeness_ppm, 0);
    assert!(!evidence.quality.citations_complete);
    assert_eq!(
        evidence.normalized["source_document"]["status"],
        "source_snapshots_partial"
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
    assert_eq!(evidence.quality.completeness_ppm, 0);
    assert!(!evidence.quality.citations_complete);
    assert_eq!(
        evidence.normalized["source_document"]["status"],
        "provider_attributed_unverified"
    );
    assert_eq!(
        evidence.normalized["source_document"]["sources"][0]["failure_kind"],
        "transport"
    );

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

#[tokio::test]
async fn news_web_rejects_non_https_citation_before_fetch() {
    let transport = unverified_news_transport(ModelClient::Fixture(native_web_fixture(
        "news",
        "http://www.reuters.com/story",
    )));

    let error = transport
        .acquire(&EvidenceRequest {
            source: EvidenceSource::NewsWeb,
            resource: "news:QQQ:2026-08-20:2026-08-27:market".to_owned(),
            max_age: Duration::minutes(5),
        })
        .await
        .unwrap_err();

    assert!(matches!(error, EvidenceAdapterError::Policy { .. }));
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
