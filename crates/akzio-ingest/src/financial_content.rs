use std::collections::BTreeSet;

use akzio_domain::{
    ContentHash, FinancialContentAssessment, FinancialContentIndicator, InformationClassification,
    SourceAuthorityClass,
};
use chrono::{DateTime, Utc};
use reqwest::Url;
use serde_json::Value;
use unicode_normalization::UnicodeNormalization;

use crate::{
    EvidenceProvenance, EvidenceRuntimeError, EvidenceRuntimeResult, EvidenceSource,
    GovernedResource,
};

pub(crate) fn assess_financial_content(
    raw: &[u8],
    normalized: &Value,
    provenance: &EvidenceProvenance,
    source: EvidenceSource,
    resource: &str,
    now: DateTime<Utc>,
) -> EvidenceRuntimeResult<FinancialContentAssessment> {
    let raw_text = String::from_utf8_lossy(raw);
    let normalized_text =
        serde_json::to_string(normalized).map_err(|_| EvidenceRuntimeError::InvalidAcquisition)?;
    let combined = format!("{raw_text}\n{normalized_text}");
    let canonical_text = canonical_visible_text(&combined);
    let lower = canonical_text.to_ascii_lowercase();
    let source_origin = Url::parse(&provenance.source_uri)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
        .ok_or(EvidenceRuntimeError::InvalidProvenance)?;
    let authority = authority_for_host(&source_origin);
    let mut indicators = BTreeSet::new();
    if contains_instruction_like_content(&lower) {
        indicators.insert(FinancialContentIndicator::InstructionLikeContent);
    }
    if contains_hidden_content(&lower) {
        indicators.insert(FinancialContentIndicator::HiddenContent);
    }
    if contains_unicode_anomaly(&combined) {
        indicators.insert(FinancialContentIndicator::UnicodeAnomaly);
    }
    let high_impact = contains_high_impact_claim(&lower);
    let source_count = source_count(normalized);
    let independent_confirmation_clusters = independent_source_clusters(normalized);
    if independent_confirmation_clusters < source_count {
        indicators.insert(FinancialContentIndicator::SyndicatedDuplicate);
    }
    if high_impact
        && (!authority.is_primary_for_high_impact() || independent_confirmation_clusters < 2)
    {
        indicators.insert(FinancialContentIndicator::UnverifiedHighImpactClaim);
    }
    let information_classification = if contains_suspected_mnpi(&lower) {
        InformationClassification::SuspectedMnpi
    } else {
        InformationClassification::Public
    };
    let canonical_entity_ids = canonical_entity_ids(normalized, &canonical_text);
    if entity_identifier_mismatch(source, resource, &canonical_entity_ids)? {
        indicators.insert(FinancialContentIndicator::EntityIdentifierMismatch);
    }
    let assessment = FinancialContentAssessment {
        source_origin,
        syndication_parent: syndication_parent(normalized),
        content_similarity_cluster: content_similarity_cluster(normalized, &canonical_text),
        first_seen_at: provenance.published_at.unwrap_or(now),
        canonical_entity_ids,
        indicators,
        authority,
        information_classification,
        independent_confirmation_clusters,
        high_impact,
    };
    assessment
        .validate()
        .map_err(|_| EvidenceRuntimeError::InvalidAcquisition)?;
    Ok(assessment)
}

fn canonical_visible_text(text: &str) -> String {
    text.nfkc()
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn authority_for_host(host: &str) -> SourceAuthorityClass {
    let host = host.trim_start_matches("www.").to_ascii_lowercase();
    if host.ends_with(".gov") || matches!(host.as_str(), "sec.gov" | "investor.gov" | "finra.org") {
        SourceAuthorityClass::Regulator
    } else if matches!(host.as_str(), "nasdaq.com" | "nyse.com") {
        SourceAuthorityClass::Exchange
    } else if matches!(host.as_str(), "proshares.com" | "direxion.com") {
        SourceAuthorityClass::FundSponsor
    } else if matches!(
        host.as_str(),
        "reuters.com" | "apnews.com" | "bloomberg.com" | "wsj.com" | "ft.com"
    ) {
        SourceAuthorityClass::EstablishedNews
    } else if matches!(
        host.as_str(),
        "reddit.com" | "x.com" | "twitter.com" | "stocktwits.com"
    ) {
        SourceAuthorityClass::SocialMedia
    } else {
        SourceAuthorityClass::Unknown
    }
}

fn contains_hidden_content(text: &str) -> bool {
    [
        "display:none",
        "display: none",
        "visibility:hidden",
        "visibility: hidden",
        "opacity:0",
        "opacity: 0",
        "aria-hidden=\"true\"",
        " hidden=\"hidden\"",
        "<template",
    ]
    .iter()
    .any(|pattern| text.contains(pattern))
}

fn contains_unicode_anomaly(text: &str) -> bool {
    if text.chars().any(|value| {
        matches!(
            value,
            '\u{200B}'
                | '\u{200C}'
                | '\u{200D}'
                | '\u{2060}'
                | '\u{FEFF}'
                | '\u{202A}'..='\u{202E}'
                | '\u{2066}'..='\u{2069}'
        )
    }) {
        return true;
    }
    if text.nfkc().collect::<String>() != text {
        return true;
    }
    text.split_whitespace().any(|token| {
        token
            .chars()
            .any(|character| character.is_ascii_alphabetic())
            && token
                .chars()
                .any(|character| character.is_alphabetic() && !character.is_ascii())
    }) || text.chars().any(|character| {
        matches!(
            character,
            '\u{0391}'..='\u{03A9}'
                | '\u{03B1}'..='\u{03C9}'
                | '\u{0400}'..='\u{04FF}'
                | '\u{0500}'..='\u{052F}'
        )
    })
}

fn contains_instruction_like_content(text: &str) -> bool {
    [
        "ignore previous",
        "ignore all previous",
        "system prompt",
        "developer message",
        "call the tool",
        "submit order",
        "place an order",
        "bypass policy",
    ]
    .iter()
    .any(|pattern| text.contains(pattern))
}

fn contains_high_impact_claim(text: &str) -> bool {
    [
        "merger",
        "acquisition",
        "bankruptcy",
        "delisting",
        "restatement",
        "chief executive resigned",
        "ceo resigned",
        "regulatory penalty",
        "material lawsuit",
        "fund liquidation",
        "etf restructure",
        "index reconstitution",
    ]
    .iter()
    .any(|pattern| text.contains(pattern))
}

fn contains_suspected_mnpi(text: &str) -> bool {
    [
        "material nonpublic information",
        "material non-public information",
        "suspected mnpi",
        "confidential and not for distribution",
        "insider only",
        "not yet public",
    ]
    .iter()
    .any(|pattern| text.contains(pattern))
}

fn source_count(value: &Value) -> u16 {
    value
        .get("source_document")
        .and_then(|document| document.get("sources"))
        .and_then(Value::as_array)
        .and_then(|sources| u16::try_from(sources.len()).ok())
        .unwrap_or(1)
        .max(1)
}

fn independent_source_clusters(value: &Value) -> u16 {
    let clusters = value
        .get("source_document")
        .and_then(|document| document.get("sources"))
        .and_then(Value::as_array)
        .map(|sources| {
            sources
                .iter()
                .filter_map(|source| source.get("content_hash").and_then(Value::as_str))
                .collect::<BTreeSet<_>>()
                .len()
        })
        .unwrap_or(1);
    u16::try_from(clusters).unwrap_or(u16::MAX).max(1)
}

fn content_similarity_cluster(value: &Value, fallback_text: &str) -> ContentHash {
    let hashes = value
        .get("source_document")
        .and_then(|document| document.get("sources"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|source| source.get("content_hash").and_then(Value::as_str))
        .collect::<BTreeSet<_>>();
    if hashes.is_empty() {
        return ContentHash::of_bytes(fallback_text.as_bytes());
    }
    ContentHash::of_bytes(hashes.into_iter().collect::<Vec<_>>().join("|").as_bytes())
}

fn syndication_parent(value: &Value) -> Option<String> {
    value
        .get("syndication_parent")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn canonical_entity_ids(normalized: &Value, text: &str) -> BTreeSet<String> {
    let mut identifiers = ["TQQQ", "QQQ", "SOXX", "SOXL"]
        .into_iter()
        .filter(|symbol| {
            text.split(|character: char| !character.is_ascii_alphanumeric())
                .any(|token| token.eq_ignore_ascii_case(symbol))
        })
        .map(|symbol| format!("ticker:{symbol}"))
        .collect::<BTreeSet<_>>();
    collect_structured_entity_ids(normalized, &mut identifiers);
    identifiers
}

fn collect_structured_entity_ids(value: &Value, identifiers: &mut BTreeSet<String>) {
    match value {
        Value::Object(fields) => {
            for (key, value) in fields {
                if matches!(
                    key.to_ascii_lowercase().as_str(),
                    "cik" | "ticker" | "symbol" | "tickers"
                ) {
                    collect_identifier_value(value, identifiers);
                }
                collect_structured_entity_ids(value, identifiers);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_structured_entity_ids(value, identifiers);
            }
        }
        _ => {}
    }
}

fn collect_identifier_value(value: &Value, identifiers: &mut BTreeSet<String>) {
    match value {
        Value::String(value) => {
            if let Some(identifier) = normalize_entity_identifier(value) {
                identifiers.insert(identifier);
            }
        }
        Value::Number(value) => {
            if let Some(value) = value.as_u64() {
                identifiers.insert(format!("cik:{value:010}"));
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_identifier_value(value, identifiers);
            }
        }
        _ => {}
    }
}

fn normalize_entity_identifier(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.chars().all(|character| character.is_ascii_digit()) {
        let cik = trimmed.parse::<u64>().ok()?;
        return Some(format!("cik:{cik:010}"));
    }
    let upper = trimmed.to_ascii_uppercase();
    (upper.len() <= 8
        && upper
            .chars()
            .all(|character| character.is_ascii_alphanumeric()))
    .then(|| format!("ticker:{upper}"))
}

fn entity_identifier_mismatch(
    source: EvidenceSource,
    resource: &str,
    identifiers: &BTreeSet<String>,
) -> EvidenceRuntimeResult<bool> {
    let expected = match GovernedResource::parse(source, resource)? {
        GovernedResource::NewsWeb { query } => query
            .split(':')
            .next()
            .and_then(normalize_entity_identifier),
        GovernedResource::SecSubmissions { cik }
        | GovernedResource::SecCompanyFacts { cik }
        | GovernedResource::SecFiling { cik, .. } => normalize_entity_identifier(&cik),
        _ => None,
    };
    Ok(
        expected
            .is_some_and(|expected| !identifiers.is_empty() && !identifiers.contains(&expected)),
    )
}
