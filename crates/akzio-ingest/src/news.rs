//! News discovery and an independent source-reading model call. Neither is an
//! execution approval or a proof of an investment prediction.
use std::{collections::BTreeSet, sync::Arc};

use akzio_domain::ContentHash;
use akzio_model::{
    ModelClient, ModelConfig, ModelInput, ModelRequest, ModelToolChoice, NativeWebPolicy,
};
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    model_native_web_evidence_transport, AcquiredEvidence, AsyncEvidenceAdapter,
    EvidenceAcquisitionMode, EvidenceAdapterError, EvidenceRequest, EvidenceSource,
    GovernedResource, OfficialInstrumentEvidenceTransport,
};

/// Both daemon and real integration tests use this constructor and the same
/// resolved deployment configuration. No test-specific model fallback exists.
pub fn configured_news_evidence_transport(
    config: &ModelConfig,
) -> Result<Arc<dyn AsyncEvidenceAdapter>, EvidenceAdapterError> {
    let discovery = config
        .routes
        .get("evidence.news_web")
        .map(|r| config.for_route(r))
        .unwrap_or_else(|| config.clone());
    let reviewer = config
        .routes
        .get("research.critic")
        .map(|r| config.for_route(r))
        .unwrap_or_else(|| config.clone());
    let client = |c: &ModelConfig| {
        ModelClient::from_config(c).map_err(|e| EvidenceAdapterError::Transport(e.to_string()))
    };
    Ok(Arc::new(NewsRouter {
        official: Arc::new(OfficialInstrumentEvidenceTransport::new()?),
        native: model_native_web_evidence_transport(client(&discovery)?, EvidenceSource::NewsWeb)
            .map_err(|e| EvidenceAdapterError::Transport(e.to_string()))?,
        reviewer: client(&reviewer)?,
        reviewer_identity: json!({"model":reviewer.model,"reasoning_effort":reviewer.reasoning_effort,"route":if config.routes.contains_key("research.critic") {"research.critic"} else {"default"}}),
    }))
}

struct NewsRouter {
    official: Arc<dyn AsyncEvidenceAdapter>,
    native: Arc<dyn AsyncEvidenceAdapter>,
    reviewer: ModelClient,
    reviewer_identity: Value,
}

fn is_official(resource: &GovernedResource) -> bool {
    matches!(
        resource,
        GovernedResource::OfficialFundHoldings { .. }
            | GovernedResource::OfficialIndexMetadata { .. }
            | GovernedResource::OfficialLeveragedEtfTerms { .. }
    )
}

/// Decode the reviewer's JSON envelope, tolerating trailing bytes after the
/// first complete value. Observed real failure: a well-formed
/// `{"sources":[...]}` followed by one stray `}`, which made `from_str` discard
/// five fully reviewed sources. Only bytes after a complete value are ignored —
/// truncated or malformed JSON still fails, and the envelope's own schema is
/// unchanged, so no unverified fact is admitted.
fn parse_review_envelope(text: &str) -> Result<Review, serde_json::Error> {
    let mut stream = serde_json::Deserializer::from_str(text).into_iter::<Review>();
    match stream.next() {
        Some(result) => result,
        None => serde_json::from_str(text),
    }
}

/// The reviewer's JSON envelope. Unknown envelope keys are ignored because the
/// prompt also asks for native citations in response metadata, and some models
/// echo a `metadata` sibling of `sources`. `SourceReview` and `Fact` keep
/// `deny_unknown_fields`, so no per-source or per-fact claim is ever accepted
/// from an unrecognized key.
#[derive(Debug, Clone, Deserialize, Serialize)]
struct Review {
    sources: Vec<SourceReview>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SourceReview {
    url: String,
    status: SourceStatus,
    reason: String,
    /// The prompt requires an empty list for every non-supported source. A
    /// model that omits the key instead means the same thing, so absence is
    /// read as "no facts" rather than discarding the whole review. Facts are
    /// still only accepted from `Supported` sources.
    #[serde(default)]
    facts: Vec<Fact>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum SourceStatus {
    Supported,
    Contradicted,
    Unverifiable,
    Irrelevant,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Fact {
    statement: String,
    published_at: Option<DateTime<Utc>>,
    event_date: chrono::NaiveDate,
    date_basis: String,
}

impl Review {
    fn validate(
        &self,
        urls: &BTreeSet<String>,
        observed: &BTreeSet<String>,
        request: &EvidenceRequest,
        now: DateTime<Utc>,
    ) -> Result<(), String> {
        let returned = self
            .sources
            .iter()
            .map(|s| s.url.clone())
            .collect::<BTreeSet<_>>();
        if &returned != urls || self.sources.len() != urls.len() {
            return Err("review must cover each requested source exactly once".into());
        }
        let window = match GovernedResource::parse(request.source, &request.resource)
            .map_err(|e| e.to_string())?
        {
            GovernedResource::RecentNews {
                window_start,
                window_end,
                ..
            } => Some((window_start, window_end)),
            _ => None,
        };
        for source in &self.sources {
            if source.reason.trim().is_empty() {
                return Err("review reason is missing".into());
            }
            if source.status == SourceStatus::Supported {
                if !observed
                    .iter()
                    .any(|url| url_identity(url) == url_identity(&source.url))
                    || source.facts.is_empty()
                {
                    return Err(if source.facts.is_empty() {
                        "facts_empty"
                    } else {
                        "source_unverified"
                    }
                    .into());
                }
                for fact in &source.facts {
                    if fact.statement.trim().is_empty()
                        || fact.date_basis.trim().is_empty()
                        || fact.published_at.is_some_and(|published| published > now)
                        || (window.is_some() && fact.event_date > now.date_naive())
                        || window.is_some_and(|(start, end)| {
                            fact.event_date < start || fact.event_date > end
                        })
                    {
                        return Err(if fact.statement.trim().is_empty()
                            || fact.date_basis.trim().is_empty()
                        {
                            "facts_empty"
                        } else {
                            "facts_outside_window"
                        }
                        .into());
                    }
                }
            } else if !source.facts.is_empty() {
                return Err("unsupported source cannot contribute facts".into());
            }
        }
        Ok(())
    }
}

// Only known tracking parameters are removed. Host, path and semantic query
// parameters remain part of identity; this does not infer redirects.
fn url_identity(value: &str) -> String {
    let Ok(mut url) = reqwest::Url::parse(value) else {
        return value.to_owned();
    };
    let pairs = url
        .query_pairs()
        .filter(|(key, _)| {
            !matches!(
                key.as_ref(),
                "utm_source" | "utm_medium" | "utm_campaign" | "utm_term" | "utm_content"
            )
        })
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    url.set_query(None);
    if !pairs.is_empty() {
        url.query_pairs_mut().extend_pairs(pairs);
    }
    url.to_string()
}

impl Review {
    fn accepted_facts(
        &self,
        urls: &BTreeSet<String>,
        observed: &BTreeSet<String>,
        request: &EvidenceRequest,
        now: DateTime<Utc>,
    ) -> (Vec<Value>, Vec<Value>) {
        let mut accepted = Vec::new();
        let mut failures = Vec::new();
        for requested in urls {
            if urls
                .iter()
                .filter(|u| url_identity(u) == url_identity(requested))
                .count()
                != 1
            {
                failures.push(json!({"url":requested,"reason":"ambiguous_requested_url_identity"}));
                continue;
            }
            let matches = self
                .sources
                .iter()
                .filter(|s| url_identity(&s.url) == url_identity(requested))
                .collect::<Vec<_>>();
            let [source] = matches.as_slice() else {
                failures.push(json!({"url":requested,"reason":"missing_or_duplicate_source"}));
                continue;
            };
            let one_url = BTreeSet::from([source.url.clone()]);
            // Check each fact independently, retaining source-level failures even
            // for empty or unsupported sources. Never upgrade model review.
            let facts: Vec<Option<&Fact>> = if source.facts.is_empty() {
                vec![None]
            } else {
                source.facts.iter().map(Some).collect()
            };
            for (index, fact) in facts.into_iter().enumerate() {
                let mut single = (*source).clone();
                single.facts = fact.into_iter().cloned().collect();
                match (Review { sources: vec![single] }).validate(&one_url, observed, request, now) {
                    Ok(()) if source.status == SourceStatus::Supported => {
                        if let Some(fact) = fact {
                            accepted.push(json!({"url":requested,"review_url":source.url,
                                "citation_urls":observed.iter().filter(|u| url_identity(u)==url_identity(requested)).collect::<Vec<_>>(),
                                "url_binding":"exact_or_known_tracking_parameters_only",
                                "statement":fact.statement,"published_at":fact.published_at,
                                "event_date":fact.event_date,"date_basis":fact.date_basis}));
                        }
                    }
                    Ok(()) => {}
                    Err(reason) => failures.push(json!({"url":requested,"review_url":source.url,"fact_index":index,"reason":reason})),
                }
            }
        }
        for source in &self.sources {
            if !urls
                .iter()
                .any(|u| url_identity(u) == url_identity(&source.url))
            {
                failures.push(json!({"url":source.url,"reason":"unrequested_source"}));
            }
        }
        (accepted, failures)
    }
}

impl NewsRouter {
    async fn reviewed(
        &self,
        request: &EvidenceRequest,
    ) -> Result<AcquiredEvidence, EvidenceAdapterError> {
        let mut discovery_request = request.clone();
        discovery_request.acquisition_mode = EvidenceAcquisitionMode::DiscoveryOnly;
        let mut acquired = self
            .native
            .acquire(&discovery_request)
            .await
            .map_err(|error| match error {
                EvidenceAdapterError::Transport(message) => {
                    EvidenceAdapterError::Transport(format!("fetch_failed: {message}"))
                }
                other => other,
            })?;
        let mut policy = NativeWebPolicy::default();
        // Read the source restriction used by the actual discovery request.
        let domains = acquired
            .normalized
            .pointer("/provider_request/tools/0/filters/allowed_domains")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                EvidenceAdapterError::Transport("news search domain restriction missing".into())
            })?;
        policy.allowed_hosts = domains
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect();
        let all = acquired
            .normalized
            .get("citations")
            .cloned()
            .unwrap_or(json!([]));
        let urls = all
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|v| v.get("uri").and_then(Value::as_str))
            .take(8)
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        let response = self.reviewer.respond(ModelRequest {
            instructions: include_str!("prompts/source_verifier.md").into(),
            input: ModelInput::Fresh { text: json!({"resource":request.resource,"source_urls":urls,"candidate_summary":acquired.normalized.get("output_text")}).to_string() },
            max_output_tokens: 6000,
            reasoning_effort: None,
            tools: vec![policy.tool_definition()],
            tool_choice: ModelToolChoice::Required,
            fixture_key: None,
        }).await;
        let now = Utc::now();
        let mut accepted = Vec::new();
        let (review_audit, review_error) = match response {
            Ok(response) => {
                let result = (|| -> Result<(Review, BTreeSet<String>), String> {
                    policy
                        .validate_provider_response(&response.raw)
                        .map_err(|e| e.to_string())?;
                    let observed = policy
                        .extract_citations(&response.raw)
                        .map_err(|e| e.to_string())?
                        .into_iter()
                        .map(|c| c.uri)
                        .collect();
                    let text = response.output_text.trim();
                    let text = text
                        .strip_prefix("```json")
                        .and_then(|s| s.strip_suffix("```"))
                        .unwrap_or(text)
                        .trim();
                    let review: Review =
                        parse_review_envelope(text).map_err(|e| format!("malformed_json: {e}"))?;
                    Ok((review, observed))
                })();
                match result {
                    Ok((review, observed)) => {
                        let (facts, failures) =
                            review.accepted_facts(&urls, &observed, request, now);
                        accepted = facts;
                        let error = failures
                            .first()
                            .and_then(|v| v["reason"].as_str())
                            .map(str::to_owned);
                        (
                            json!({"request":response.request_body,"response":response.raw,
                            "review":review,"validation_failures":failures}),
                            error,
                        )
                    }
                    Err(error) => (
                        json!({"request":response.request_body,"response":response.raw}),
                        Some(error),
                    ),
                }
            }
            Err(error) => (
                json!({"error_class":format!("{:?}",std::mem::discriminant(&error))}),
                Some("fetch_failed: source review model call failed".to_owned()),
            ),
        };
        let usable = !accepted.is_empty();
        let mut value = acquired.normalized.clone();
        value["discovery_output_text"] = value["output_text"].clone();
        value["output_text"] = json!(accepted
            .iter()
            .filter_map(|f| f["statement"].as_str())
            .collect::<Vec<_>>()
            .join("\n"));
        value["reviewed_facts"] = json!(accepted);
        value["news_evidence_status"] = json!(news_review_status(usable, review_error.as_deref()));
        // Provider-attributed model review is not a fetched, verified source snapshot.
        value["source_document"]["verified_source_count"] = json!(0);
        value["source_document"]["source_verified"] = json!(false);
        value["source_document"]["model_reviewed_source_count"] = json!(accepted
            .iter()
            .filter_map(|f| f["url"].as_str())
            .collect::<BTreeSet<_>>()
            .len());
        let reviewed_urls = accepted
            .iter()
            .filter_map(|f| f["url"].as_str())
            .collect::<BTreeSet<_>>();
        value["citations"] = json!(all
            .as_array()
            .into_iter()
            .flatten()
            .filter(|c| c
                .get("uri")
                .and_then(Value::as_str)
                .is_some_and(|u| reviewed_urls.contains(u)))
            .collect::<Vec<_>>());
        value["discovered_source_count"] = json!(all.as_array().map_or(0, Vec::len));
        value["source_review"] = json!({"version":2,"status":if usable {"model_reviewed"} else {"unverified"},"reviewer":self.reviewer_identity,"reviewed_at":now,"error":review_error,"audit":review_audit,"human_review":"not_performed","investment_inference":"not_verified","scope":"only reviewed_facts; discovery output is not verified","selected_source_count":urls.len()});
        value["source_review"]["validation_failures"] =
            value["source_review"]["audit"]["validation_failures"].clone();
        value["source_review"]["sources"] = json!(urls.iter().map(|url| {
            let facts = accepted.iter().filter(|fact| fact["url"].as_str() == Some(url.as_str())).count();
            json!({"url":url,"status":if facts > 0 {"model_reviewed"} else {"unverified"},
                "accepted_fact_count":facts,"scope":"only reviewed_facts; rejected text retained in RawEvidence"})
        }).collect::<Vec<_>>());
        value["source_document"]["status"] = json!(if usable {
            "model_reviewed"
        } else {
            "provider_attributed_unverified"
        });
        value["source_document"]["acquisition_mode"] =
            json!(EvidenceAcquisitionMode::ModelReviewed.as_str());
        value["source_document"]["source_closure"] = json!("provider_attributed_model_review");
        // Preserve original provider bytes/URL bindings. Append review bytes to
        // the same raw envelope; no independent source snapshot is invented.
        let review_bytes = serde_json::to_vec(&value["source_review"])
            .map_err(|e| EvidenceAdapterError::Transport(e.to_string()))?;
        acquired.raw.extend_from_slice(b"\n");
        acquired.raw.extend_from_slice(&review_bytes);
        acquired.media_type = "application/x-ndjson".into();
        acquired.provenance.revision = Some(ContentHash::of_bytes(&acquired.raw).to_string());
        acquired.provenance.dedupe_key =
            format!("reviewed-news:{}", ContentHash::of_bytes(&acquired.raw));
        if usable {
            acquired
                .provenance
                .citations
                .retain(|citation| reviewed_urls.contains(citation.quote.as_str()));
        }
        acquired.quality.citations_complete = usable;
        let supported_sources = accepted
            .iter()
            .filter_map(|f| f["url"].as_str())
            .collect::<BTreeSet<_>>();
        acquired.quality.completeness_ppm =
            (supported_sources.len() * 1_000_000 / urls.len().max(1)) as u32;
        if let Some(uri) = supported_sources.first() {
            acquired.source_uri = (*uri).to_owned();
            acquired.provenance.source_uri = (*uri).to_owned();
            acquired.provenance.document_id = Some((*uri).to_owned());
        }
        // Large provider transcripts are retained in RawEvidence only; the
        // research Context sees concise review outcomes and supported facts.
        value["source_review"]
            .as_object_mut()
            .expect("review object")
            .remove("audit");
        value
            .as_object_mut()
            .expect("news envelope")
            .remove("provider_result");
        value
            .as_object_mut()
            .expect("news envelope")
            .remove("provider_request");
        value
            .as_object_mut()
            .expect("news envelope")
            .remove("discovery_output_text");
        acquired.observed_at = now;
        acquired.provenance.observed_at = now;
        acquired.provenance.published_at =
            if accepted.iter().all(|fact| fact["published_at"].is_string()) {
                accepted
                    .iter()
                    .filter_map(|fact| fact["published_at"].as_str())
                    .filter_map(|value| DateTime::parse_from_rfc3339(value).ok())
                    .max()
                    .map(|value| value.with_timezone(&Utc))
            } else {
                None
            };
        acquired.normalized = value;
        Ok(acquired)
    }
}

impl AsyncEvidenceAdapter for NewsRouter {
    fn source(&self) -> EvidenceSource {
        EvidenceSource::NewsWeb
    }
    fn acquire<'a>(
        &'a self,
        request: &'a EvidenceRequest,
    ) -> BoxFuture<'a, Result<AcquiredEvidence, EvidenceAdapterError>> {
        Box::pin(async move {
            if request.source != self.source() {
                return Err(EvidenceAdapterError::SourceMismatch);
            }
            let resource = GovernedResource::parse(request.source, &request.resource)
                .map_err(|e| EvidenceAdapterError::Transport(e.to_string()))?;
            if is_official(&resource) {
                self.official.acquire(request).await
            } else if request.acquisition_mode == EvidenceAcquisitionMode::ModelReviewed {
                self.reviewed(request).await
            } else {
                self.native.acquire(request).await
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn news_and_earnings_are_not_routed_to_product_documents() {
        for resource in [
            "news:QQQ:2026-09-11:2026-09-18:market",
            "research:earnings_event_calendar:QQQ:2026-09-18",
        ] {
            assert!(!is_official(
                &GovernedResource::parse(EvidenceSource::NewsWeb, resource).unwrap()
            ));
        }
        assert!(is_official(
            &GovernedResource::parse(
                EvidenceSource::NewsWeb,
                "research:etf_holdings:QQQ:2026-09-18"
            )
            .unwrap()
        ));
    }

    fn request() -> EvidenceRequest {
        EvidenceRequest {
            source: EvidenceSource::NewsWeb,
            resource: "news:QQQ:2026-09-11:2026-09-18:market".into(),
            max_age: chrono::Duration::days(7),
            acquisition_mode: EvidenceAcquisitionMode::ModelReviewed,
        }
    }

    #[test]
    fn review_rejects_missing_sources_uncited_support_and_future_dates() {
        let url = "https://www.reuters.com/markets/example".to_owned();
        let urls = BTreeSet::from([url.clone()]);
        let now: DateTime<Utc> = "2026-09-18T12:00:00Z".parse().unwrap();
        let mut review = Review {
            sources: vec![SourceReview {
                url,
                status: SourceStatus::Supported,
                reason: "source explicitly reports the observation".into(),
                facts: vec![Fact {
                    statement: "Company reported quarterly results".into(),
                    published_at: Some("2026-09-17T12:00:00Z".parse().unwrap()),
                    event_date: "2026-09-17".parse().unwrap(),
                    date_basis: "Published September 17; company reported today".into(),
                }],
            }],
        };
        assert!(review.validate(&urls, &urls, &request(), now).is_ok());
        let tracked = BTreeSet::from([format!("{}?utm_source=openai", review.sources[0].url)]);
        assert!(review.validate(&urls, &tracked, &request(), now).is_ok());
        assert!(review
            .validate(&urls, &BTreeSet::new(), &request(), now)
            .is_err());
        review.sources[0].facts[0].published_at = Some("2026-09-19T00:00:00Z".parse().unwrap());
        assert!(review.validate(&urls, &urls, &request(), now).is_err());
        review.sources[0].facts[0].published_at = None;
        assert!(review.validate(&urls, &urls, &request(), now).is_ok());
        review.sources[0].facts[0].event_date = "2026-09-10".parse().unwrap();
        assert!(review.validate(&urls, &urls, &request(), now).is_err());
        review.sources.clear();
        assert!(review.validate(&urls, &urls, &request(), now).is_err());
    }

    #[test]
    fn unverifiable_or_contradicted_sources_cannot_contribute_facts() {
        let url = "https://www.reuters.com/markets/example".to_owned();
        let urls = BTreeSet::from([url.clone()]);
        let now = "2026-09-18T12:00:00Z".parse().unwrap();
        let mut review = Review {
            sources: vec![SourceReview {
                url,
                status: SourceStatus::Contradicted,
                reason: "article reports the opposite".into(),
                facts: vec![Fact {
                    statement: "unsupported claim".into(),
                    published_at: Some(now),
                    event_date: now.date_naive(),
                    date_basis: "September 18".into(),
                }],
            }],
        };
        assert!(review.validate(&urls, &urls, &request(), now).is_err());
        review.sources[0].facts.clear();
        assert!(review.validate(&urls, &urls, &request(), now).is_ok());
        review.sources[0].status = SourceStatus::Unverifiable;
        assert!(review
            .validate(&urls, &BTreeSet::new(), &request(), now)
            .is_ok());
    }

    struct RouteMarker(&'static str);
    impl AsyncEvidenceAdapter for RouteMarker {
        fn source(&self) -> EvidenceSource {
            EvidenceSource::NewsWeb
        }
        fn acquire<'a>(
            &'a self,
            _: &'a EvidenceRequest,
        ) -> BoxFuture<'a, Result<AcquiredEvidence, EvidenceAdapterError>> {
            Box::pin(async move { Err(EvidenceAdapterError::NotConfigured(self.0.into())) })
        }
    }
    #[tokio::test]
    async fn acquisition_dispatches_news_and_calendar_to_native_not_official() {
        let router = NewsRouter {
            official: Arc::new(RouteMarker("official")),
            native: Arc::new(RouteMarker("native")),
            reviewer: ModelClient::fixture_sequence([]),
            reviewer_identity: json!({}),
        };
        for (resource, expected) in [
            ("news:QQQ:2026-09-11:2026-09-18:market", "native"),
            ("research:earnings_event_calendar:QQQ:2026-09-18", "native"),
            ("research:etf_holdings:QQQ:2026-09-18", "official"),
        ] {
            let mut req = request();
            req.resource = resource.into();
            assert!(
                matches!(router.acquire(&req).await,Err(EvidenceAdapterError::NotConfigured(marker)) if marker==expected)
            );
        }
    }
}

fn news_review_status(usable: bool, error: Option<&str>) -> &'static str {
    if usable {
        return "model_reviewed";
    }
    match error {
        Some(e) if e.starts_with("fetch_failed") => "fetch_failed",
        Some(e) if e.starts_with("malformed_json") => "malformed_json",
        Some("facts_outside_window") => "facts_outside_window",
        Some("facts_empty") => "facts_empty",
        Some(_) => "source_unverified",
        None => "facts_empty",
    }
}

#[cfg(test)]
mod status_regressions {
    use super::*;
    #[test]
    fn model_review_is_never_source_verification_and_failures_keep_their_cause() {
        assert_eq!(news_review_status(true, None), "model_reviewed");
        assert_ne!(news_review_status(true, None), "source_verified");
        for (error, expected) in [
            ("fetch_failed: timeout", "fetch_failed"),
            ("malformed_json: eof", "malformed_json"),
            ("facts_empty", "facts_empty"),
            ("facts_outside_window", "facts_outside_window"),
        ] {
            assert_eq!(news_review_status(false, Some(error)), expected);
        }
        assert_eq!(news_review_status(false, None), "facts_empty");
    }
}

/// Real reviewer deviations observed in run aa736a60b0c24714 (2026-09-21), each
/// of which discarded a complete multi-source review. Tolerating them must not
/// widen what counts as a verified fact.
#[cfg(test)]
mod review_envelope_regressions {
    use super::*;

    fn source(status: &str, facts: &str) -> String {
        format!(
            r#"{{"url":"https://example.com/a","status":"{status}","reason":"checked"{facts}}}"#
        )
    }

    /// A well-formed envelope followed by one stray `}` previously failed with
    /// "trailing characters", discarding five reviewed sources.
    #[test]
    fn trailing_bytes_after_a_complete_envelope_are_ignored() {
        let body = format!(
            r#"{{"sources":[{}]}}"#,
            source("unverifiable", r#","facts":[]"#)
        );
        let review = parse_review_envelope(&format!("{body}}}")).unwrap();
        assert_eq!(review.sources.len(), 1);
        assert_eq!(review.sources[0].status, SourceStatus::Unverifiable);
    }

    /// Omitting `facts` on a non-supported source means the same as `[]`, which
    /// the prompt already requires.
    #[test]
    fn omitted_facts_on_unsupported_source_is_an_empty_list() {
        let body = format!(r#"{{"sources":[{}]}}"#, source("irrelevant", ""));
        let review = parse_review_envelope(&body).unwrap();
        assert!(review.sources[0].facts.is_empty());
    }

    /// An extra top-level key alongside `sources` is ignored on the envelope.
    #[test]
    fn unknown_envelope_key_is_ignored() {
        let body = format!(
            r#"{{"metadata":{{"note":"x"}},"sources":[{}]}}"#,
            source("unverifiable", r#","facts":[]"#)
        );
        assert_eq!(parse_review_envelope(&body).unwrap().sources.len(), 1);
    }

    /// Tolerance stops at the envelope. Unknown per-source and per-fact keys,
    /// bad status values and truncated JSON must still fail, so a malformed
    /// review can never be read as verification.
    #[test]
    fn schema_violations_and_truncated_json_still_fail() {
        for body in [
            // unknown key inside a source
            r#"{"sources":[{"url":"u","status":"supported","reason":"r","facts":[],"extra":1}]}"#,
            // unknown key inside a fact
            r#"{"sources":[{"url":"u","status":"supported","reason":"r","facts":[{"statement":"s","published_at":null,"event_date":"2026-09-21","date_basis":"b","extra":1}]}]}"#,
            // status outside the enum
            r#"{"sources":[{"url":"u","status":"probably","reason":"r","facts":[]}]}"#,
            // fact missing a required field
            r#"{"sources":[{"url":"u","status":"supported","reason":"r","facts":[{"statement":"s"}]}]}"#,
            // truncated before the envelope completes
            r#"{"sources":[{"url":"u","status":"supported","reason":"r","facts":[]}"#,
            // no JSON value at all
            "not json",
        ] {
            assert!(
                parse_review_envelope(body).is_err(),
                "should have been rejected: {body}"
            );
        }
    }
}

#[cfg(test)]
mod review_partition_tests {
    use super::*;
    #[test]
    fn tracked_source_retains_valid_fact_but_rejects_old_and_uncited_facts() {
        let url = "https://www.reuters.com/markets/example";
        let missing = "https://www.reuters.com/markets/uncited";
        let review: Review = serde_json::from_value(json!({"sources":[
            {"url":format!("{url}?utm_source=openai"),"status":"supported","reason":"reviewed","facts":[
                {"statement":"in window","published_at":null,"event_date":"2026-09-17","date_basis":"dated source"},
                {"statement":"too old","published_at":null,"event_date":"2026-08-31","date_basis":"dated source"}]},
            {"url":missing,"status":"supported","reason":"claimed","facts":[
                {"statement":"uncited","published_at":null,"event_date":"2026-09-17","date_basis":"dated source"}]}
        ]})).unwrap();
        let request = EvidenceRequest {
            source: EvidenceSource::NewsWeb,
            resource: "news:QQQ:2026-09-11:2026-09-18:market".into(),
            max_age: chrono::Duration::days(7),
            acquisition_mode: EvidenceAcquisitionMode::ModelReviewed,
        };
        let (facts, failures) = review.accepted_facts(
            &BTreeSet::from([url.into(), missing.into()]),
            &BTreeSet::from([url.into()]),
            &request,
            "2026-09-18T12:00:00Z".parse().unwrap(),
        );
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0]["url"], url);
        assert_eq!(facts[0]["statement"], "in window");
        assert!(failures
            .iter()
            .any(|f| f["reason"] == "facts_outside_window"));
        assert!(failures.iter().any(|f| f["reason"] == "source_unverified"));
        assert_ne!(
            url_identity(url),
            url_identity(&format!("{url}?article=other"))
        );
        assert_ne!(
            url_identity(url),
            url_identity("https://other.example/markets/example")
        );
    }
}
