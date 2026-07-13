use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

/// A detected cross-analyst conflict for one or more tickers.
#[derive(Debug, Clone)]
pub(crate) struct Conflict {
    pub(crate) conflict_type: ConflictType,
    pub(crate) tickers: Vec<String>,
    pub(crate) analysts: Vec<String>,
    pub(crate) description: String,
    pub(crate) severity: Severity,
    pub(crate) details: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConflictType {
    DirectionConflict,
    ConfidenceDivergence,
    EvidenceOverlap,
    EvidenceContradiction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Severity {
    Low,
    Medium,
    High,
}

impl Conflict {
    pub(crate) fn to_json(&self) -> Value {
        let mut object = serde_json::Map::new();
        object.insert("type".to_string(), json!(self.conflict_type.as_str()));
        object.insert("tickers".to_string(), json!(self.tickers));
        object.insert("analysts".to_string(), json!(self.analysts));
        object.insert("description".to_string(), json!(self.description));
        object.insert("severity".to_string(), json!(self.severity.as_str()));
        object.insert("details".to_string(), json!(self.details));

        // Keep common detail fields at the top level as well so downstream LLM
        // prompts can consume the conflict without knowing the Rust internals.
        for (key, value) in &self.details {
            object.insert(key.clone(), value.clone());
        }

        Value::Object(object)
    }
}

impl ConflictType {
    fn as_str(self) -> &'static str {
        match self {
            Self::DirectionConflict => "direction_conflict",
            Self::ConfidenceDivergence => "confidence_divergence",
            Self::EvidenceOverlap => "evidence_overlap",
            Self::EvidenceContradiction => "evidence_contradiction",
        }
    }
}

impl Severity {
    fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

/// Run all cross-analyst conflict detectors for a ticker.
pub(crate) fn detect_all_conflicts(ticker: &str, role_summaries: &[Value]) -> Vec<Conflict> {
    let mut conflicts = Vec::new();
    conflicts.extend(detect_direction_conflicts(ticker, role_summaries));
    conflicts.extend(detect_confidence_divergence(ticker, role_summaries));
    conflicts.extend(detect_evidence_contradiction(ticker, role_summaries));
    conflicts.extend(detect_evidence_overlap(ticker, role_summaries));
    conflicts
}

/// Detect direction conflicts between analysts for a given ticker.
/// A direction conflict exists when one analyst says bullish and another says
/// bearish; neutral, mixed, missing, and unobserved stances are ignored.
pub(crate) fn detect_direction_conflicts(ticker: &str, role_summaries: &[Value]) -> Vec<Conflict> {
    let bullish = role_summaries
        .iter()
        .filter(|summary| normalized_direction(summary) == Some("bullish"))
        .collect::<Vec<_>>();
    let bearish = role_summaries
        .iter()
        .filter(|summary| normalized_direction(summary) == Some("bearish"))
        .collect::<Vec<_>>();

    let mut conflicts = Vec::new();
    for bull in &bullish {
        for bear in &bearish {
            let Some(bull_role) = role_name(bull) else {
                continue;
            };
            let Some(bear_role) = role_name(bear) else {
                continue;
            };
            let bull_conf = confidence(bull, 0.0);
            let bear_conf = confidence(bear, 0.0);
            let severity = if bull_conf > 0.6 && bear_conf > 0.6 {
                Severity::High
            } else if bull_conf > 0.3 || bear_conf > 0.3 {
                Severity::Medium
            } else {
                Severity::Low
            };

            let mut details = BTreeMap::new();
            details.insert(
                "directions".to_string(),
                object_from_pairs(vec![
                    (bull_role, json!("bullish")),
                    (bear_role, json!("bearish")),
                ]),
            );
            details.insert(
                "confidences".to_string(),
                object_from_pairs(vec![
                    (bull_role, json!(bull_conf)),
                    (bear_role, json!(bear_conf)),
                ]),
            );

            conflicts.push(Conflict {
                conflict_type: ConflictType::DirectionConflict,
                tickers: vec![ticker.to_string()],
                analysts: vec![bull_role.to_string(), bear_role.to_string()],
                description: format!(
                    "Direction conflict: {bull_role} is bullish (confidence {bull_conf:.2}) but {bear_role} is bearish (confidence {bear_conf:.2}) on {ticker}",
                ),
                severity,
                details,
            });
        }
    }
    conflicts
}

/// Detect confidence divergence between analysts on the same ticker. A pair is
/// flagged when both analysts contribute a directional stance and their
/// confidence scores differ by at least 0.50.
pub(crate) fn detect_confidence_divergence(
    ticker: &str,
    role_summaries: &[Value],
) -> Vec<Conflict> {
    let contributing = role_summaries
        .iter()
        .filter(|summary| is_directional(summary))
        .collect::<Vec<_>>();

    let mut conflicts = Vec::new();
    for i in 0..contributing.len() {
        for j in (i + 1)..contributing.len() {
            let a = contributing[i];
            let b = contributing[j];
            let conf_a = confidence(a, 0.5);
            let conf_b = confidence(b, 0.5);
            let delta = (conf_a - conf_b).abs();
            if delta < 0.5 {
                continue;
            }

            let Some(role_a) = role_name(a) else {
                continue;
            };
            let Some(role_b) = role_name(b) else {
                continue;
            };
            let severity = if delta >= 0.65 {
                Severity::High
            } else {
                Severity::Medium
            };

            let mut details = BTreeMap::new();
            details.insert(
                "confidences".to_string(),
                object_from_pairs(vec![(role_a, json!(conf_a)), (role_b, json!(conf_b))]),
            );
            details.insert("delta".to_string(), json!(delta));

            conflicts.push(Conflict {
                conflict_type: ConflictType::ConfidenceDivergence,
                tickers: vec![ticker.to_string()],
                analysts: vec![role_a.to_string(), role_b.to_string()],
                description: format!(
                    "Confidence divergence: {role_a} has {conf_a:.2} but {role_b} has {conf_b:.2} (delta {delta:.2}) on {ticker}",
                ),
                severity,
                details,
            });
        }
    }
    conflicts
}

/// Detect evidence overlap between analysts. High keyword overlap means a
/// downstream reducer should treat the evidence as potential double-counting.
pub(crate) fn detect_evidence_overlap(ticker: &str, role_summaries: &[Value]) -> Vec<Conflict> {
    let evidence_pairs = role_summaries
        .iter()
        .filter_map(|summary| {
            let role = role_name(summary)?;
            let evidence = evidence_text(summary);
            if evidence.trim().is_empty() {
                None
            } else {
                Some((role, evidence))
            }
        })
        .collect::<Vec<_>>();

    let mut conflicts = Vec::new();
    for i in 0..evidence_pairs.len() {
        for j in (i + 1)..evidence_pairs.len() {
            let (role_a, evidence_a) = &evidence_pairs[i];
            let (role_b, evidence_b) = &evidence_pairs[j];
            let tokens_a = tokenize(evidence_a);
            let tokens_b = tokenize(evidence_b);
            if tokens_a.is_empty() || tokens_b.is_empty() {
                continue;
            }

            let overlap = jaccard_similarity(&tokens_a, &tokens_b);
            if overlap <= 0.3 {
                continue;
            }

            let shared = tokens_a
                .intersection(&tokens_b)
                .take(10)
                .cloned()
                .collect::<Vec<_>>();
            let severity = if overlap > 0.5 {
                Severity::High
            } else {
                Severity::Medium
            };

            let mut details = BTreeMap::new();
            details.insert("shared_keywords".to_string(), json!(shared));
            details.insert("jaccard_similarity".to_string(), json!(overlap));
            details.insert(
                "evidence_snippets".to_string(),
                object_from_pairs(vec![
                    (*role_a, json!(snippet(evidence_a))),
                    (*role_b, json!(snippet(evidence_b))),
                ]),
            );

            conflicts.push(Conflict {
                conflict_type: ConflictType::EvidenceOverlap,
                tickers: vec![ticker.to_string()],
                analysts: vec![(*role_a).to_string(), (*role_b).to_string()],
                description: format!(
                    "Evidence overlap between {role_a} and {role_b}: Jaccard {overlap:.2}, shared keywords: {}",
                    details
                        .get("shared_keywords")
                        .and_then(Value::as_array)
                        .map(|items| items.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(", "))
                        .unwrap_or_default()
                ),
                severity,
                details,
            });
        }
    }
    conflicts
}

/// Detect evidence contradictions: two analysts cite overlapping meaningful
/// event keywords but interpret them in opposite directions.
pub(crate) fn detect_evidence_contradiction(
    ticker: &str,
    role_summaries: &[Value],
) -> Vec<Conflict> {
    let entries = role_summaries
        .iter()
        .filter_map(|summary| {
            let role = role_name(summary)?;
            let stance = normalized_direction(summary)?;
            if stance != "bullish" && stance != "bearish" {
                return None;
            }
            let evidence = evidence_text(summary);
            if evidence.trim().is_empty() {
                None
            } else {
                Some((role, stance, evidence))
            }
        })
        .collect::<Vec<_>>();

    let mut conflicts = Vec::new();
    for i in 0..entries.len() {
        for j in (i + 1)..entries.len() {
            let (role_a, stance_a, evidence_a) = &entries[i];
            let (role_b, stance_b, evidence_b) = &entries[j];
            let opposite = (*stance_a == "bullish" && *stance_b == "bearish")
                || (*stance_a == "bearish" && *stance_b == "bullish");
            if !opposite {
                continue;
            }

            let tokens_a = tokenize(evidence_a);
            let tokens_b = tokenize(evidence_b);
            let shared = tokens_a
                .intersection(&tokens_b)
                .filter(|token| is_meaningful_keyword(token))
                .cloned()
                .collect::<Vec<_>>();
            if shared.is_empty() {
                continue;
            }

            let mut details = BTreeMap::new();
            details.insert("shared_keywords".to_string(), json!(shared));
            details.insert(
                "directions".to_string(),
                object_from_pairs(vec![(*role_a, json!(stance_a)), (*role_b, json!(stance_b))]),
            );
            details.insert(
                "evidence_snippets".to_string(),
                object_from_pairs(vec![
                    (*role_a, json!(snippet(evidence_a))),
                    (*role_b, json!(snippet(evidence_b))),
                ]),
            );

            conflicts.push(Conflict {
                conflict_type: ConflictType::EvidenceContradiction,
                tickers: vec![ticker.to_string()],
                analysts: vec![(*role_a).to_string(), (*role_b).to_string()],
                description: format!(
                    "Same event cited with different interpretations: {role_a} and {role_b} share keywords [{}] but have opposing directions ({stance_a} vs {stance_b}) on {ticker}",
                    details
                        .get("shared_keywords")
                        .and_then(Value::as_array)
                        .map(|items| items.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(", "))
                        .unwrap_or_default()
                ),
                severity: Severity::High,
                details,
            });
        }
    }
    conflicts
}

fn role_name(summary: &Value) -> Option<&str> {
    summary
        .get("role")
        .and_then(Value::as_str)
        .filter(|role| !role.trim().is_empty())
}

fn normalized_direction(summary: &Value) -> Option<&'static str> {
    match summary.get("stance").and_then(Value::as_str)? {
        "bullish" | "long" | "positive" => Some("bullish"),
        "bearish" | "short" | "negative" => Some("bearish"),
        "neutral" | "mixed" | "unobserved" => Some("neutral"),
        _ => None,
    }
}

fn is_directional(summary: &Value) -> bool {
    matches!(normalized_direction(summary), Some("bullish" | "bearish"))
}

fn confidence(summary: &Value, default: f64) -> f64 {
    summary
        .get("confidence")
        .and_then(Value::as_f64)
        .unwrap_or(default)
        .clamp(0.0, 1.0)
}

fn evidence_text(summary: &Value) -> String {
    summary
        .get("key_evidence")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| match item {
                    Value::String(text) => Some(text.as_str()),
                    Value::Object(object) => object.get("claim").and_then(Value::as_str),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .unwrap_or_default()
}

fn tokenize(text: &str) -> BTreeSet<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|word| word.len() > 2)
        .filter(|word| !is_stopword(word))
        .map(ToString::to_string)
        .collect()
}

fn jaccard_similarity(a: &BTreeSet<String>, b: &BTreeSet<String>) -> f64 {
    let intersection = a.intersection(b).count();
    let union = a.union(b).count();
    if union == 0 {
        0.0
    } else {
        intersection as f64 / union as f64
    }
}

fn is_meaningful_keyword(word: &str) -> bool {
    const DOMAIN_TERMS: &[&str] = &[
        "ai", "cpi", "fed", "fomc", "gdp", "qqq", "tqqq", "spy", "vix", "rsi", "ppi",
    ];
    DOMAIN_TERMS.contains(&word) || (word.len() > 3 && !is_stopword(word))
}

fn is_stopword(word: &str) -> bool {
    const STOPWORDS: &[&str] = &[
        "the", "and", "for", "with", "from", "this", "that", "was", "were", "has", "have", "had",
        "are", "but", "not", "into", "over", "under", "about", "than", "then",
    ];
    STOPWORDS.contains(&word)
}

fn snippet(text: &str) -> String {
    text.chars().take(200).collect()
}

fn object_from_pairs(pairs: Vec<(&str, Value)>) -> Value {
    let mut object = serde_json::Map::new();
    for (key, value) in pairs {
        object.insert(key.to_string(), value);
    }
    Value::Object(object)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn detects_bullish_bearish_direction_conflict() {
        let summaries = vec![
            json!({"role": "analyst.technical", "stance": "bullish", "confidence": 0.7, "key_evidence": ["breakout above 50MA"]}),
            json!({"role": "analyst.news_macro", "stance": "bearish", "confidence": 0.8, "key_evidence": ["Fed hawkish surprise"]}),
        ];

        let conflicts = detect_direction_conflicts("TQQQ", &summaries);

        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].conflict_type, ConflictType::DirectionConflict);
        assert_eq!(conflicts[0].severity, Severity::High);
    }

    #[test]
    fn no_conflict_when_all_bullish() {
        let summaries = vec![
            json!({"role": "analyst.technical", "stance": "bullish", "confidence": 0.7}),
            json!({"role": "analyst.news_macro", "stance": "bullish", "confidence": 0.6}),
        ];

        let conflicts = detect_direction_conflicts("TQQQ", &summaries);

        assert!(conflicts.is_empty());
    }

    #[test]
    fn neutral_and_unobserved_are_excluded_from_direction_conflicts() {
        let summaries = vec![
            json!({"role": "analyst.technical", "stance": "bullish", "confidence": 0.7}),
            json!({"role": "analyst.news_macro", "stance": "neutral", "confidence": 0.9}),
            json!({"role": "analyst.reddit", "stance": "unobserved", "confidence": 0.0}),
        ];

        let conflicts = detect_direction_conflicts("TQQQ", &summaries);

        assert!(conflicts.is_empty());
    }

    #[test]
    fn detects_confidence_divergence() {
        let summaries = vec![
            json!({"role": "analyst.technical", "stance": "bullish", "confidence": 0.85}),
            json!({"role": "analyst.x", "stance": "bullish", "confidence": 0.15}),
        ];

        let conflicts = detect_confidence_divergence("TQQQ", &summaries);

        assert_eq!(conflicts.len(), 1);
        assert!(conflicts[0].description.contains("0.70"));
        assert_eq!(conflicts[0].severity, Severity::High);
    }

    #[test]
    fn confidence_delta_below_threshold_is_not_flagged() {
        let summaries = vec![
            json!({"role": "analyst.technical", "stance": "bullish", "confidence": 0.70}),
            json!({"role": "analyst.x", "stance": "bullish", "confidence": 0.25}),
        ];

        let conflicts = detect_confidence_divergence("TQQQ", &summaries);

        assert!(conflicts.is_empty());
    }

    #[test]
    fn detects_evidence_overlap() {
        let summaries = vec![
            json!({"role": "analyst.youtube", "stance": "bullish", "confidence": 0.5, "key_evidence": ["Rhino Finance QQQ 500 target YouTube"]}),
            json!({"role": "analyst.reddit", "stance": "bullish", "confidence": 0.5, "key_evidence": ["Reddit discusses Rhino QQQ 500 call"]}),
        ];

        let conflicts = detect_evidence_overlap("TQQQ", &summaries);

        assert_eq!(conflicts.len(), 1);
        assert!(conflicts[0].details.contains_key("shared_keywords"));
    }

    #[test]
    fn detects_evidence_contradiction() {
        let summaries = vec![
            json!({"role": "analyst.news_macro", "stance": "bullish", "confidence": 0.7, "key_evidence": ["CPI came in at 3.2 percent"]}),
            json!({"role": "analyst.reddit", "stance": "bearish", "confidence": 0.6, "key_evidence": ["CPI was 3.4 percent bearish"]}),
        ];

        let conflicts = detect_evidence_contradiction("TQQQ", &summaries);

        assert_eq!(conflicts.len(), 1);
        assert_eq!(
            conflicts[0].conflict_type,
            ConflictType::EvidenceContradiction
        );
        assert_eq!(conflicts[0].severity, Severity::High);
    }

    #[test]
    fn detects_evidence_overlap_with_structured_objects() {
        let summaries = vec![
            json!({"role": "analyst.youtube", "stance": "bullish", "confidence": 0.5, "key_evidence": [
                {"claim": "Rhino Finance QQQ 500 target YouTube", "evidence_type": "opinion", "source": "YouTube"}
            ]}),
            json!({"role": "analyst.reddit", "stance": "bullish", "confidence": 0.5, "key_evidence": [
                {"claim": "Reddit discusses Rhino QQQ 500 call", "evidence_type": "opinion", "source": "Reddit"}
            ]}),
        ];

        let conflicts = detect_evidence_overlap("TQQQ", &summaries);

        assert_eq!(conflicts.len(), 1);
        assert!(conflicts[0].details.contains_key("shared_keywords"));
    }

    #[test]
    fn detects_evidence_contradiction_with_structured_objects() {
        let summaries = vec![
            json!({"role": "analyst.news_macro", "stance": "bullish", "confidence": 0.7, "key_evidence": [
                {"claim": "CPI came in at 3.2 percent", "evidence_type": "fact", "source": "BLS"}
            ]}),
            json!({"role": "analyst.reddit", "stance": "bearish", "confidence": 0.6, "key_evidence": [
                {"claim": "CPI was 3.4 percent bearish", "evidence_type": "speculation", "source": "Reddit"}
            ]}),
        ];

        let conflicts = detect_evidence_contradiction("TQQQ", &summaries);

        assert_eq!(conflicts.len(), 1);
        assert_eq!(
            conflicts[0].conflict_type,
            ConflictType::EvidenceContradiction
        );
        assert_eq!(conflicts[0].severity, Severity::High);
    }
}
