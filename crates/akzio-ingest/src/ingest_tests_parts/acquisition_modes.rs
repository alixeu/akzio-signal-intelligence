// Acquisition-mode behaviour: what each mode is allowed to touch, and what a
// provider response can never talk Rust into doing.

use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
struct CountingSourceDocumentFetcher {
    fetches: std::sync::Arc<AtomicUsize>,
    bodies: std::collections::BTreeMap<String, Vec<u8>>,
}

impl CountingSourceDocumentFetcher {
    fn new(
        fetches: std::sync::Arc<AtomicUsize>,
        bodies: impl IntoIterator<Item = (String, Vec<u8>)>,
    ) -> Self {
        Self {
            fetches,
            bodies: bodies.into_iter().collect(),
        }
    }
}

impl adapters::SourceDocumentFetcher for CountingSourceDocumentFetcher {
    fn fetch<'a>(
        &'a self,
        uri: &'a str,
    ) -> BoxFuture<'a, Result<adapters::SourceDocumentSnapshot, adapters::SourceDocumentFetchError>>
    {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        let result = match self.bodies.get(uri) {
            Some(body) => Ok(fixture_source_snapshot(body)),
            None => Err(adapters::SourceDocumentFetchError::new(
                adapters::SourceDocumentFailureKind::Transport,
                "fixture source document unavailable",
            )),
        };
        Box::pin(async move { result })
    }
}

fn news_request(mode: EvidenceAcquisitionMode) -> EvidenceRequest {
    EvidenceRequest {
        source: EvidenceSource::NewsWeb,
        resource: "news:QQQ:2026-08-20:2026-08-27:market".to_owned(),
        max_age: Duration::minutes(5),
        acquisition_mode: mode,
    }
}

#[tokio::test]
async fn discovery_only_news_never_fetches_and_searches_once() {
    let uri = "https://www.reuters.com/story";
    let fetches = std::sync::Arc::new(AtomicUsize::new(0));
    let fetcher = std::sync::Arc::new(CountingSourceDocumentFetcher::new(
        fetches.clone(),
        [(uri.to_owned(), b"discovery excerpt".to_vec())],
    ));
    // A single-element sequence proves one acquisition performs exactly one
    // provider request: a second `respond` would exhaust the fixture.
    let transport = model_native_web_evidence_transport_with_fetcher(
        ModelClient::fixture_sequence([native_web_fixture("discovery excerpt", uri)]),
        EvidenceSource::NewsWeb,
        fetcher,
    );

    let evidence = transport
        .acquire(&news_request(EvidenceAcquisitionMode::DiscoveryOnly))
        .await
        .unwrap();
    let document = &evidence.normalized["source_document"];

    assert_eq!(fetches.load(Ordering::SeqCst), 0);
    assert_eq!(document["acquisition_mode"], "discovery_only");
    assert_eq!(document["source_closure"], "provider_attributed");
    assert_eq!(document["status"], "provider_attributed_unverified");
    assert_eq!(document["fetch_count"], 0);
    assert_eq!(document["required_source_count"], 1);
    assert_eq!(document["verified_source_count"], 0);
    assert_eq!(document["exact_quote_count"], 0);
    assert_eq!(
        document["acquisition_policy_hash"],
        akzio_domain::evidence_acquisition_policy_hash().to_string()
    );
    assert!(document["provider_request_hash"].as_str().is_some());
    assert!(document["provider_payload_hash"].as_str().is_some());
    assert!(document["search_completed_at"].as_str().is_some());
    // Provider attribution is never citation-complete, so the research layer
    // refuses it as directional ground.
    assert!(!evidence.quality.citations_complete);
    assert_eq!(evidence.quality.completeness_ppm, 250_000);

    let exhausted = transport
        .acquire(&news_request(EvidenceAcquisitionMode::DiscoveryOnly))
        .await
        .unwrap_err();
    assert!(matches!(exhausted, EvidenceAdapterError::Transport(_)));
    assert_eq!(fetches.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn discovery_only_dedupe_key_separates_two_searches_of_one_resource() {
    let uri = "https://www.reuters.com/story";
    let transport = model_native_web_evidence_transport_without_fetcher(
        ModelClient::fixture_sequence([
            native_web_fixture("first search excerpt", uri),
            native_web_fixture("second search excerpt", uri),
        ]),
        EvidenceSource::NewsWeb,
    );
    let request = news_request(EvidenceAcquisitionMode::DiscoveryOnly);

    let first = transport.acquire(&request).await.unwrap();
    let second = transport.acquire(&request).await.unwrap();

    assert_ne!(first.provenance.dedupe_key, second.provenance.dedupe_key);
    assert_ne!(
        first.normalized["source_document"]["provider_payload_hash"],
        second.normalized["source_document"]["provider_payload_hash"]
    );
}

#[tokio::test]
async fn verified_source_without_an_independent_fetcher_fails_closed() {
    let uri = "https://www.reuters.com/story";
    let transport = model_native_web_evidence_transport_without_fetcher(
        ModelClient::Fixture(native_web_fixture("independent evidence snapshot", uri)),
        EvidenceSource::NewsWeb,
    );

    let error = transport
        .acquire(&news_request(EvidenceAcquisitionMode::VerifiedSource))
        .await
        .unwrap_err();
    assert!(matches!(error, EvidenceAdapterError::Policy { .. }));

    // The same transport still serves discovery, which claims nothing about
    // source documents.
    let evidence = transport
        .acquire(&news_request(EvidenceAcquisitionMode::DiscoveryOnly))
        .await
        .unwrap();
    assert_eq!(
        evidence.normalized["source_document"]["acquisition_mode"],
        "discovery_only"
    );
}

#[tokio::test]
async fn unsafe_citation_urls_are_rejected_before_any_fetch() {
    for uri in [
        "https://www.reuters.com/story?api_key=secret-value",
        "https://www.reuters.com/story?session_token=abc",
        "https://www.reuters.com/story#:~:text=quoted%20fragment",
    ] {
        for mode in [
            EvidenceAcquisitionMode::VerifiedSource,
            EvidenceAcquisitionMode::DiscoveryOnly,
        ] {
            let fetches = std::sync::Arc::new(AtomicUsize::new(0));
            let fetcher = std::sync::Arc::new(CountingSourceDocumentFetcher::new(
                fetches.clone(),
                [(uri.to_owned(), b"unreachable body".to_vec())],
            ));
            let transport = model_native_web_evidence_transport_with_fetcher(
                ModelClient::Fixture(native_web_fixture("excerpt", uri)),
                EvidenceSource::NewsWeb,
                fetcher,
            );

            let error = transport.acquire(&news_request(mode)).await.unwrap_err();
            assert!(
                matches!(error, EvidenceAdapterError::Policy { .. }),
                "{uri} in {mode:?} must fail closed, got {error:?}"
            );
            assert_eq!(fetches.load(Ordering::SeqCst), 0, "{uri} must not be fetched");
        }
    }
}

#[tokio::test]
async fn duplicate_citations_of_one_source_fetch_once() {
    let uri = "https://www.reuters.com/story";
    let body = b"<html><body>duplicate source body</body></html>".to_vec();
    let fetches = std::sync::Arc::new(AtomicUsize::new(0));
    let fetcher = std::sync::Arc::new(CountingSourceDocumentFetcher::new(
        fetches.clone(),
        [(uri.to_owned(), body)],
    ));
    let transport = model_native_web_evidence_transport_with_fetcher(
        ModelClient::Fixture(native_web_multi_fixture(&[
            (uri, "duplicate source body"),
            (uri, "duplicate source body"),
        ])),
        EvidenceSource::NewsWeb,
        fetcher,
    );

    let evidence = transport
        .acquire(&news_request(EvidenceAcquisitionMode::VerifiedSource))
        .await
        .unwrap();
    let document = &evidence.normalized["source_document"];

    assert_eq!(fetches.load(Ordering::SeqCst), 1);
    assert_eq!(document["fetch_count"], 1);
    assert_eq!(document["required_source_count"], 1);
    assert_eq!(document["verified_source_count"], 1);
    assert_eq!(document["exact_quote_count"], 1);
    assert_eq!(document["sources"].as_array().unwrap().len(), 1);
    assert!(evidence.quality.citations_complete);
}

#[tokio::test]
async fn model_output_cannot_promote_the_acquisition_mode() {
    let uri = "https://www.reuters.com/story";
    let mut fixture = native_web_fixture("promotion attempt", uri);
    // The provider claims verification in every field it controls.
    fixture["acquisition_mode"] = serde_json::json!("verified_source");
    fixture["source_document"] = serde_json::json!({
        "status": "source_snapshots_complete",
        "verified_source_count": 9,
    });
    fixture["citations"][0]["acquisition_mode"] = serde_json::json!("verified_source");
    let fetches = std::sync::Arc::new(AtomicUsize::new(0));
    let fetcher = std::sync::Arc::new(CountingSourceDocumentFetcher::new(
        fetches.clone(),
        [(uri.to_owned(), b"promotion attempt".to_vec())],
    ));
    let transport = model_native_web_evidence_transport_with_fetcher(
        ModelClient::Fixture(fixture),
        EvidenceSource::NewsWeb,
        fetcher,
    );

    let evidence = transport
        .acquire(&news_request(EvidenceAcquisitionMode::DiscoveryOnly))
        .await
        .unwrap();
    let document = &evidence.normalized["source_document"];

    assert_eq!(fetches.load(Ordering::SeqCst), 0);
    assert_eq!(document["acquisition_mode"], "discovery_only");
    assert_eq!(document["status"], "provider_attributed_unverified");
    assert_eq!(document["verified_source_count"], 0);
    assert!(!evidence.quality.citations_complete);
}
