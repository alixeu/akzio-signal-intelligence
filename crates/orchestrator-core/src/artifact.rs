use anyhow::{anyhow, Context, Result};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArtifactEnvelope {
    pub id: String,
    pub role: String,
    #[serde(default)]
    pub report: String,
    #[serde(default)]
    pub per_ticker: BTreeMap<String, Value>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ResearchArtifact {
    pub rating: String,
    pub long_probability: f64,
    pub short_probability: f64,
    /// Why the probability is confident or uncertain: evidence_balanced,
    /// data_insufficient, conflicting_evidence, or directional_evidence.
    #[serde(default)]
    pub confidence_basis: String,
    /// Required for Hold: evidence_balanced, evidence_insufficient, or
    /// conflicting_evidence.
    #[serde(default)]
    pub hold_reason: Option<String>,
    #[serde(default)]
    pub plan: String,
    #[serde(default)]
    pub probability_rationale: String,
    /// Three-scenario analysis: bull, base, bear. Optional for backward
    /// compatibility with legacy research artifacts.
    #[serde(default)]
    pub scenarios: Option<Scenarios>,
    #[serde(default)]
    pub per_ticker: BTreeMap<String, Value>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// A single scenario (bull, base, or bear) in the research manager's
/// scenario analysis output.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct Scenario {
    /// Probability of this scenario (0.0-1.0). All scenarios must sum to 1.0.
    pub probability: f64,
    /// Key drivers that would cause this scenario to play out (1-3 items).
    #[serde(default)]
    pub drivers: Vec<String>,
    /// Observable triggers that would shift probability toward this scenario (1-3 items).
    #[serde(default)]
    pub triggers: Vec<String>,
    /// What would confirm this scenario is the active path.
    #[serde(default)]
    pub confirmation: String,
}

/// Container for the three scenarios.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct Scenarios {
    pub bull: Scenario,
    pub base: Scenario,
    pub bear: Scenario,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct TradeIntent {
    pub action: String,
    #[serde(default)]
    pub entry_price: Option<String>,
    #[serde(default)]
    pub stop_loss: Option<String>,
    #[serde(default)]
    pub position_size: String,
    #[serde(default)]
    pub rationale: String,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct RiskConstraints {
    pub stance: String,
    #[serde(default)]
    pub argument: String,
    /// The stance-specific constraint or counterargument not already supplied
    /// by prior risk turns.
    #[serde(default)]
    pub unique_risk_contribution: String,
    /// Explicit agreement/disagreement with the strongest prior constraint.
    #[serde(default)]
    pub disagreement_with_prior: String,
    /// True only when the role found no genuine incremental constraint.
    #[serde(default)]
    pub no_new_information: bool,
    #[serde(default)]
    pub recommended_adjustment: String,
    /// none | tight | trailing | event_based | time_based
    #[serde(default)]
    pub stop_type: String,
    /// 0.0-1.0 fraction of capital at risk before stopping.
    #[serde(default)]
    pub max_drawdown_pct: f64,
    /// 0.0-1.0 maximum single-position weight cap.
    #[serde(default)]
    pub position_cap_pct: f64,
    /// Condition that triggers a portfolio rebalance.
    #[serde(default)]
    pub rebalance_trigger: String,
    /// Condition that forces a risk-off / de-risk event.
    #[serde(default)]
    pub risk_off_trigger: String,
    /// How long until the risk view is revisited (human readable).
    #[serde(default)]
    pub review_window: String,
    /// Cash-hedge recommendation (size / instrument / rationale).
    #[serde(default)]
    pub cash_hedge_recommendation: String,
    /// 0.0-1.0 confidence in the constraints themselves.
    #[serde(default)]
    pub constraint_confidence: f64,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct FinalValidation {
    pub rating: String,
    #[serde(default)]
    pub execution_summary: String,
    #[serde(default)]
    pub investment_thesis: String,
    #[serde(default)]
    pub target_price: Option<String>,
    #[serde(default)]
    pub horizon: String,
    #[serde(default)]
    pub risk_controls: Vec<String>,
    #[serde(default)]
    pub rationale: String,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct PortfolioAllocation {
    pub weights: BTreeMap<String, Value>,
    pub total_equity_exposure: f64,
    #[serde(default)]
    pub vix_regime: String,
    #[serde(default)]
    pub correlation_note: String,
    #[serde(default)]
    pub summary: String,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Per-ticker payload every analyst (technical / news_macro / reddit / x /
/// youtube) must emit. This is the single source of truth for the analyst
/// output contract: `prompts/common/analyst_output_contract.md` documents its
/// behavioral rules, while `analyst_artifact_schema()` and runtime validation
/// remain the sole source of structural truth.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct AnalystTickerArtifact {
    /// bullish | bearish | neutral | mixed | unobserved
    pub direction: String,
    /// Evidence-consistency / clarity, 0.0-1.0 (NOT 0-100, NOT upside probability).
    pub confidence: f64,
    /// Full prose analysis for this ticker (may contain sections / Markdown tables).
    #[serde(default)]
    pub report: String,
    /// The 2-3 most decisive evidence items.
    ///
    /// Structured `EvidenceItem` objects are preferred. Legacy plain-string
    /// entries are accepted during deserialization and normalized to
    /// `evidence_type = "unclassified"`.
    #[serde(default, deserialize_with = "deserialize_key_evidence")]
    pub key_evidence: Vec<EvidenceItem>,
    /// already_priced | under_priced | unclear
    #[serde(default)]
    pub priced_in: String,
    /// low | medium | high
    #[serde(default)]
    pub echo_chamber_risk: String,
    /// low | medium | high
    #[serde(default)]
    pub crowded_consensus_risk: String,
    /// Observations that would strengthen or overturn the current call.
    #[serde(default)]
    pub validation_triggers: Vec<String>,
    /// Data gaps and uncertainties; empty array when none.
    #[serde(default)]
    pub data_gaps: Vec<String>,
}

/// Canonical evidence-type tokens accepted by runtime validators and reducers.
pub const CANONICAL_EVIDENCE_TYPES: &[&str] = &["fact", "opinion", "speculation", "unclassified"];

/// Normalize model-invented / legacy evidence-type labels onto the canonical set.
pub fn normalize_evidence_type(raw: &str) -> String {
    let normalized = raw
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|ch| match ch {
            '-' | ' ' => '_',
            other => other,
        })
        .collect::<String>();
    match normalized.as_str() {
        "fact" | "opinion" | "speculation" | "unclassified" => normalized,
        "fact_provider_standardized"
        | "fact_source_reported"
        | "fact_source"
        | "standardized_fact"
        | "provider_fact"
        | "provider_standardized"
        | "data"
        | "observation"
        | "official_fact"
        | "reported_fact" => "fact".to_string(),
        "derived_calculation"
        | "analyst_interpretation"
        | "interpretation"
        | "analysis"
        | "market_commentary"
        | "issuer_management_claim"
        | "management_claim"
        | "retail_sentiment_sample"
        | "calculation"
        | "derived"
        | "commentary" => "opinion".to_string(),
        "rumor" | "hearsay" | "unverified" | "speculation_only" | "speculative" => {
            "speculation".to_string()
        }
        "" => "unclassified".to_string(),
        _ => "unclassified".to_string(),
    }
}

/// Rewrite `key_evidence[].evidence_type` onto canonical tokens in place.
pub fn normalize_analyst_ticker_artifact(artifact: &mut AnalystTickerArtifact) {
    for item in &mut artifact.key_evidence {
        item.evidence_type = normalize_evidence_type(&item.evidence_type);
    }
}

/// A single piece of evidence with type classification.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct EvidenceItem {
    /// The evidence claim in 1-2 sentences.
    #[serde(default, alias = "summary", alias = "event", alias = "description")]
    pub claim: String,
    /// Evidence type: "fact" | "opinion" | "speculation" | "unclassified".
    #[serde(default, alias = "classification", alias = "type")]
    pub evidence_type: String,
    /// Where the evidence came from (tool name, data source, URL description).
    #[serde(default)]
    pub source: String,
    /// ISO date when the evidence was observed or published.
    #[serde(default)]
    pub timestamp: String,
    /// Source quality tier: official | major_media | professional_research |
    /// longform_analysis | social_verified | social_unverified | unknown.
    #[serde(default)]
    pub source_tier: String,
    /// Earliest traceable origin of the information (attribution).
    #[serde(default)]
    pub first_source: String,
    /// Whether this is a repost / derivative of earlier-reported information.
    #[serde(default)]
    pub is_derivative_repost: bool,
    /// Human-readable evidence age: "0-2d" | "3-5d" | "6-10d" | "10d+" | "unknown".
    #[serde(default, alias = "catalyst_age", alias = "age")]
    pub evidence_age: String,
    /// 0.0-1.0 confidence in the quality of the source.
    #[serde(default)]
    pub source_confidence: f64,
}

fn value_as_string(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(text)) => text.trim().to_string(),
        Some(Value::Number(number)) => number.to_string(),
        Some(Value::Bool(flag)) => flag.to_string(),
        Some(Value::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

fn first_nonempty_string(obj: &Map<String, Value>, keys: &[&str]) -> String {
    for key in keys {
        let text = value_as_string(obj.get(*key));
        if !text.is_empty() {
            return text;
        }
    }
    String::new()
}

/// Normalize a free-form evidence object into [`EvidenceItem`].
/// Handles model drift: both evidence_age+catalyst_age, claim under assessment, source_quality.
pub fn evidence_item_from_value(value: Value) -> Result<EvidenceItem, String> {
    match value {
        Value::String(text) => Ok(EvidenceItem {
            claim: text,
            evidence_type: "unclassified".to_string(),
            source: String::new(),
            timestamp: String::new(),
            source_tier: String::new(),
            first_source: String::new(),
            is_derivative_repost: false,
            evidence_age: String::new(),
            source_confidence: 0.0,
        }),
        Value::Object(obj) => {
            let claim = first_nonempty_string(
                &obj,
                &[
                    "claim",
                    "summary",
                    "event",
                    "description",
                    "assessment",
                    "actual",
                ],
            );
            let evidence_type = normalize_evidence_type(&first_nonempty_string(
                &obj,
                &["evidence_type", "classification", "type"],
            ));
            let source = value_as_string(obj.get("source"));
            let timestamp = value_as_string(obj.get("timestamp"));
            let mut source_tier =
                first_nonempty_string(&obj, &["source_tier", "source_quality"]);
            source_tier = match source_tier.as_str() {
                "industry_media" | "analyst_note" => "professional_research".to_string(),
                "rumor" => "social_unverified".to_string(),
                other => other.to_string(),
            };
            let first_source = value_as_string(obj.get("first_source"));
            let is_derivative_repost = match obj.get("is_derivative_repost") {
                Some(Value::Bool(flag)) => *flag,
                Some(Value::String(text)) => matches!(
                    text.trim().to_ascii_lowercase().as_str(),
                    "true" | "1" | "yes"
                ),
                _ => false,
            };
            let evidence_age =
                first_nonempty_string(&obj, &["evidence_age", "catalyst_age", "age"]);
            let source_confidence = match obj.get("source_confidence") {
                Some(Value::Number(number)) => number.as_f64().unwrap_or(0.0),
                Some(Value::String(text)) => text.parse().unwrap_or(0.0),
                _ => 0.0,
            };
            Ok(EvidenceItem {
                claim,
                evidence_type,
                source,
                timestamp,
                source_tier,
                first_source,
                is_derivative_repost,
                evidence_age,
                source_confidence,
            })
        }
        _ => Err("evidence item must be string or object".to_string()),
    }
}

/// Deserialize key_evidence accepting both structured objects and plain strings.
fn deserialize_key_evidence<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<EvidenceItem>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    let raw: Vec<Value> = Vec::deserialize(deserializer)?;
    raw.into_iter()
        .map(|value| {
            evidence_item_from_value(value)
                .map_err(|error| Error::custom(format!("invalid evidence item: {error}")))
        })
        .collect()
}

pub fn validate_evidence_types(
    artifact: &AnalystTickerArtifact,
) -> std::result::Result<(), String> {
    const ALLOWED_SOURCE_TIERS: &[&str] = &[
        "official",
        "major_media",
        "professional_research",
        "longform_analysis",
        "social_verified",
        "social_unverified",
        "unknown",
    ];
    for evidence in &artifact.key_evidence {
        let canonical = normalize_evidence_type(&evidence.evidence_type);
        if !CANONICAL_EVIDENCE_TYPES.contains(&canonical.as_str()) {
            return Err(format!(
                "invalid evidence_type '{}' in evidence '{}'; must be fact, opinion, or speculation",
                evidence.evidence_type, evidence.claim
            ));
        }
        if !evidence.source_tier.is_empty()
            && !ALLOWED_SOURCE_TIERS.contains(&evidence.source_tier.as_str())
        {
            return Err(format!(
                "invalid source_tier '{}' in evidence '{}'; must be one of: {}",
                evidence.source_tier,
                evidence.claim,
                ALLOWED_SOURCE_TIERS.join(", ")
            ));
        }
        for (field, value) in [
            ("echo_chamber_risk", artifact.echo_chamber_risk.as_str()),
            (
                "crowded_consensus_risk",
                artifact.crowded_consensus_risk.as_str(),
            ),
        ] {
            if !value.is_empty() && !["low", "medium", "high"].contains(&value) {
                return Err(format!(
                    "invalid {field} '{value}'; must be low, medium, high, or empty"
                ));
            }
        }
    }
    Ok(())
}

/// Validate machine-read fields on an analyst per-ticker payload.
///
/// Enforces the contract promised by `analyst_output_contract.md`:
/// `direction` and `confidence` must exist and be legal, and evidence typing
/// must pass `validate_evidence_types`.
pub fn validate_analyst_ticker_artifact(
    artifact: &AnalystTickerArtifact,
) -> std::result::Result<(), String> {
    const ALLOWED_DIRECTIONS: &[&str] = &["bullish", "bearish", "neutral", "mixed", "unobserved"];
    if !ALLOWED_DIRECTIONS.contains(&artifact.direction.as_str()) {
        return Err(format!(
            "invalid direction '{}'; must be one of: {}",
            artifact.direction,
            ALLOWED_DIRECTIONS.join(", ")
        ));
    }
    if !(0.0..=1.0).contains(&artifact.confidence) {
        return Err(format!(
            "confidence {} out of range; must be in [0.0, 1.0]",
            artifact.confidence
        ));
    }
    validate_evidence_types(artifact)
}

/// Validate a parsed `RiskConstraints` artifact for well-formed enum and
/// range values. Tolerant of empty / zero (unspecified) fields so legacy
/// artifacts continue to deserialize.
pub fn validate_risk_constraints(artifact: &RiskConstraints) -> std::result::Result<(), String> {
    const ALLOWED_STOP_TYPES: &[&str] =
        &["none", "tight", "trailing", "event_based", "time_based", ""];
    if !ALLOWED_STOP_TYPES.contains(&artifact.stop_type.as_str()) {
        return Err(format!(
            "invalid stop_type '{}'; must be one of: {}",
            artifact.stop_type,
            ALLOWED_STOP_TYPES
                .iter()
                .filter(|s| !s.is_empty())
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if artifact.max_drawdown_pct != 0.0 && !(0.0..=1.0).contains(&artifact.max_drawdown_pct) {
        return Err(format!(
            "max_drawdown_pct {} out of range; must be in [0.0, 1.0] when specified",
            artifact.max_drawdown_pct
        ));
    }
    if artifact.position_cap_pct != 0.0 && !(0.0..=1.0).contains(&artifact.position_cap_pct) {
        return Err(format!(
            "position_cap_pct {} out of range; must be in [0.0, 1.0] when specified",
            artifact.position_cap_pct
        ));
    }
    if artifact.constraint_confidence != 0.0
        && !(0.0..=1.0).contains(&artifact.constraint_confidence)
    {
        return Err(format!(
            "constraint_confidence {} out of range; must be in [0.0, 1.0] when specified",
            artifact.constraint_confidence
        ));
    }
    Ok(())
}

/// Compact JSON Schema (no `$schema`/metadata noise) for a schemars type,
/// suitable for injecting into a prompt. Returns a pretty-printed string.
pub fn schema_for<T: JsonSchema>() -> String {
    let schema = schemars::schema_for!(T);
    serde_json::to_string_pretty(&schema).unwrap_or_else(|_| "{}".to_string())
}

/// Schema string for a single analyst per-ticker payload.
pub fn analyst_artifact_schema() -> String {
    schema_for::<AnalystTickerArtifact>()
}

/// Schema string for the research manager artifact.
pub fn research_artifact_schema() -> String {
    schema_for::<ResearchArtifact>()
}

pub fn trade_intent_schema() -> String {
    schema_for::<TradeIntent>()
}

pub fn risk_constraints_schema() -> String {
    schema_for::<RiskConstraints>()
}

pub fn final_validation_schema() -> String {
    schema_for::<FinalValidation>()
}

pub fn portfolio_allocation_schema() -> String {
    schema_for::<PortfolioAllocation>()
}

#[derive(Debug, Error, PartialEq)]
pub enum ValidationError {
    #[error("missing per_ticker payload for {0}")]
    MissingTicker(String),
    #[error("probability field {0} is invalid")]
    InvalidProbability(String),
    #[error("long_probability + short_probability must be approximately 1.0")]
    ProbabilitySum,
    #[error("scenario probabilities must sum to 1.0 (got {0})")]
    ScenarioProbabilitySum(f64),
    #[error("long_probability ({long}) inconsistent with scenarios (expected ~{expected})")]
    InconsistentLongProbability { long: f64, expected: f64 },
    #[error("confidence_basis is invalid: {0}")]
    InvalidConfidenceBasis(String),
    #[error("hold_reason is invalid: {0}")]
    InvalidHoldReason(String),
    #[error("research ticker {ticker} is invalid: {reason}")]
    InvalidResearchTicker { ticker: String, reason: String },
    #[error("research artifact field is invalid: {0}")]
    InvalidResearchField(String),
}

pub fn normalize_probability(value: &Value) -> Option<f64> {
    let parsed = match value {
        Value::Number(number) => number.as_f64()?,
        Value::String(text) => {
            let trimmed = text.trim();
            if let Some(percent) = trimmed.strip_suffix('%') {
                percent.trim().parse::<f64>().ok()? / 100.0
            } else {
                trimmed.parse::<f64>().ok()?
            }
        }
        _ => return None,
    };
    if (0.0..=1.0).contains(&parsed) {
        Some((parsed * 10_000.0).round() / 10_000.0)
    } else if (1.0..=100.0).contains(&parsed) {
        Some(((parsed / 100.0) * 10_000.0).round() / 10_000.0)
    } else {
        None
    }
}

pub fn extract_json_artifact(text: &str) -> Result<Value> {
    const START: &str = "=== ARTIFACT_JSON_START ===";
    const END: &str = "=== ARTIFACT_JSON_END ===";
    let candidate = if let Some(start) = text.find(START) {
        let after = &text[start + START.len()..];
        let end = after
            .find(END)
            .ok_or_else(|| anyhow!("artifact end marker missing"))?;
        &after[..end]
    } else {
        text
    };
    serde_json::from_str(candidate.trim()).context("failed to parse artifact JSON")
}

/// Normalize multi-ticker research envelopes before deserializing into
/// [`ResearchArtifact`].
///
/// Models often emit rating/probabilities only under `per_ticker` (and may
/// emit `plan` as a string array). Downstream still expects top-level
/// `rating` / probabilities / confidence basis for single-ticker consumers
/// such as trader mapping and report builders.
pub fn normalize_research_artifact_value(mut value: Value, tickers: &[String]) -> Result<Value> {
    let obj = value
        .as_object_mut()
        .ok_or_else(|| anyhow!("research artifact must be a JSON object"))?;

    if let Some(plan) = obj.get("plan").cloned() {
        if let Some(text) = coerce_plan_to_string(&plan) {
            obj.insert("plan".to_string(), Value::String(text));
        }
    }

    let needs_rating = obj
        .get("rating")
        .and_then(Value::as_str)
        .is_none_or(|value| value.trim().is_empty());
    let needs_long = obj
        .get("long_probability")
        .and_then(normalize_probability)
        .is_none();
    let needs_short = obj
        .get("short_probability")
        .and_then(normalize_probability)
        .is_none();
    let needs_plan = obj
        .get("plan")
        .and_then(Value::as_str)
        .map(|value| value.trim().is_empty())
        .unwrap_or(true);
    let needs_rationale = obj
        .get("probability_rationale")
        .and_then(Value::as_str)
        .map(|value| value.trim().is_empty())
        .unwrap_or(true);
    let needs_confidence_basis = obj
        .get("confidence_basis")
        .and_then(Value::as_str)
        .is_none_or(|value| value.trim().is_empty());
    let needs_hold_reason = obj
        .get("hold_reason")
        .and_then(Value::as_str)
        .is_none_or(|value| value.trim().is_empty());

    if needs_rating
        || needs_long
        || needs_short
        || needs_plan
        || needs_rationale
        || needs_confidence_basis
        || needs_hold_reason
    {
        if let Some(primary) = select_primary_research_ticker(obj, tickers) {
            let payload = obj
                .get("per_ticker")
                .and_then(Value::as_object)
                .and_then(|items| items.get(&primary))
                .cloned()
                .unwrap_or(Value::Null);

            if needs_rating {
                if let Some(rating) = payload
                    .get("rating")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                {
                    obj.insert("rating".to_string(), Value::String(rating.to_string()));
                }
            }
            if needs_long {
                if let Some(probability) = payload
                    .get("long_probability")
                    .and_then(normalize_probability)
                {
                    obj.insert("long_probability".to_string(), json!(probability));
                }
            }
            if needs_short {
                if let Some(probability) = payload
                    .get("short_probability")
                    .and_then(normalize_probability)
                {
                    obj.insert("short_probability".to_string(), json!(probability));
                }
            }
            if needs_plan {
                if let Some(plan) = payload.get("plan").and_then(coerce_plan_to_string) {
                    obj.insert("plan".to_string(), Value::String(plan));
                }
            }
            if needs_rationale {
                if let Some(rationale) = payload
                    .get("probability_rationale")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                {
                    obj.insert(
                        "probability_rationale".to_string(),
                        Value::String(rationale.to_string()),
                    );
                }
            }
            if needs_confidence_basis {
                if let Some(confidence_basis) = payload
                    .get("confidence_basis")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                {
                    obj.insert(
                        "confidence_basis".to_string(),
                        Value::String(confidence_basis.to_string()),
                    );
                }
            }
            if needs_hold_reason {
                if let Some(hold_reason) = payload
                    .get("hold_reason")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                {
                    obj.insert(
                        "hold_reason".to_string(),
                        Value::String(hold_reason.to_string()),
                    );
                }
            }
        }
    }

    Ok(value)
}

fn select_primary_research_ticker(obj: &Map<String, Value>, tickers: &[String]) -> Option<String> {
    let per_ticker = obj.get("per_ticker")?.as_object()?;
    if per_ticker.is_empty() {
        return None;
    }
    for ticker in tickers {
        if per_ticker.contains_key(ticker) {
            return Some(ticker.clone());
        }
    }
    per_ticker.keys().next().cloned()
}

fn coerce_plan_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        Value::Array(items) => {
            let parts = items
                .iter()
                .filter_map(|item| match item {
                    Value::String(text) => {
                        let trimmed = text.trim();
                        (!trimmed.is_empty()).then(|| trimmed.to_string())
                    }
                    Value::Number(number) => Some(number.to_string()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if parts.is_empty() {
                None
            } else {
                Some(parts.join("; "))
            }
        }
        _ => None,
    }
}

pub fn validate_research_artifact(
    artifact: &ResearchArtifact,
    tickers: &[String],
) -> std::result::Result<(), ValidationError> {
    let valid_confidence_basis = [
        "evidence_balanced",
        "data_insufficient",
        "conflicting_evidence",
        "directional_evidence",
    ];
    if !valid_confidence_basis.contains(&artifact.confidence_basis.as_str()) {
        return Err(ValidationError::InvalidConfidenceBasis(
            artifact.confidence_basis.clone(),
        ));
    }
    if artifact.rating.eq_ignore_ascii_case("hold") {
        let expected_hold_reason = match artifact.confidence_basis.as_str() {
            "evidence_balanced" => "evidence_balanced",
            "data_insufficient" => "evidence_insufficient",
            "conflicting_evidence" => "conflicting_evidence",
            other => {
                return Err(ValidationError::InvalidHoldReason(format!(
                    "Hold cannot use confidence_basis={other}"
                )))
            }
        };
        if artifact.hold_reason.as_deref() != Some(expected_hold_reason) {
            return Err(ValidationError::InvalidHoldReason(format!(
                "expected {expected_hold_reason} for confidence_basis={}",
                artifact.confidence_basis
            )));
        }
    }
    if artifact.plan.trim().is_empty() {
        return Err(ValidationError::InvalidResearchField(
            "plan must not be empty".to_string(),
        ));
    }
    if artifact.probability_rationale.trim().is_empty() {
        return Err(ValidationError::InvalidResearchField(
            "probability_rationale must not be empty".to_string(),
        ));
    }
    if !(0.0..=1.0).contains(&artifact.long_probability) {
        return Err(ValidationError::InvalidProbability(
            "long_probability".to_string(),
        ));
    }
    if !(0.0..=1.0).contains(&artifact.short_probability) {
        return Err(ValidationError::InvalidProbability(
            "short_probability".to_string(),
        ));
    }
    if (artifact.long_probability + artifact.short_probability - 1.0).abs() > 0.03 {
        return Err(ValidationError::ProbabilitySum);
    }
    if let Some(scenarios) = &artifact.scenarios {
        let sum =
            scenarios.bull.probability + scenarios.base.probability + scenarios.bear.probability;
        if (sum - 1.0).abs() > 0.03 {
            return Err(ValidationError::ScenarioProbabilitySum(sum));
        }

        let expected_long = scenarios.bull.probability + 0.5 * scenarios.base.probability;
        if (artifact.long_probability - expected_long).abs() > 0.05 {
            return Err(ValidationError::InconsistentLongProbability {
                long: artifact.long_probability,
                expected: expected_long,
            });
        }

        for (name, scenario) in [
            ("bull", &scenarios.bull),
            ("base", &scenarios.base),
            ("bear", &scenarios.bear),
        ] {
            if !(0.0..=1.0).contains(&scenario.probability) {
                return Err(ValidationError::InvalidProbability(format!(
                    "scenario {name}.probability"
                )));
            }
            if scenario.drivers.is_empty() {
                return Err(ValidationError::InvalidProbability(format!(
                    "scenario {name} must have at least 1 driver"
                )));
            }
            if scenario.triggers.is_empty() {
                return Err(ValidationError::InvalidProbability(format!(
                    "scenario {name} must have at least 1 trigger"
                )));
            }
            if scenario
                .drivers
                .iter()
                .any(|driver| scenario_driver_is_missing_evidence_placeholder(driver))
            {
                return Err(ValidationError::InvalidProbability(format!(
                    "scenario {name} drivers must describe causal market factors, not missing evidence"
                )));
            }
        }
    }
    for ticker in tickers {
        let payload = artifact
            .per_ticker
            .get(ticker)
            .ok_or_else(|| ValidationError::MissingTicker(ticker.clone()))?;
        let rating = payload
            .get("rating")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| ValidationError::InvalidResearchTicker {
                ticker: ticker.clone(),
                reason: "rating is missing or empty".to_string(),
            })?;
        let long = payload
            .get("long_probability")
            .and_then(Value::as_f64)
            .filter(|value| (0.0..=1.0).contains(value))
            .ok_or_else(|| ValidationError::InvalidResearchTicker {
                ticker: ticker.clone(),
                reason: "long_probability must be a number in 0..1".to_string(),
            })?;
        let short = payload
            .get("short_probability")
            .and_then(Value::as_f64)
            .filter(|value| (0.0..=1.0).contains(value))
            .ok_or_else(|| ValidationError::InvalidResearchTicker {
                ticker: ticker.clone(),
                reason: "short_probability must be a number in 0..1".to_string(),
            })?;
        if (long + short - 1.0).abs() > 0.03 {
            return Err(ValidationError::InvalidResearchTicker {
                ticker: ticker.clone(),
                reason: "long_probability + short_probability must be approximately 1.0"
                    .to_string(),
            });
        }
        let confidence_basis = payload
            .get("confidence_basis")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !valid_confidence_basis.contains(&confidence_basis) {
            return Err(ValidationError::InvalidResearchTicker {
                ticker: ticker.clone(),
                reason: format!("invalid confidence_basis {confidence_basis:?}"),
            });
        }
        if rating.eq_ignore_ascii_case("hold") {
            let expected = match confidence_basis {
                "evidence_balanced" => "evidence_balanced",
                "data_insufficient" => "evidence_insufficient",
                "conflicting_evidence" => "conflicting_evidence",
                other => {
                    return Err(ValidationError::InvalidResearchTicker {
                        ticker: ticker.clone(),
                        reason: format!("Hold cannot use confidence_basis={other}"),
                    })
                }
            };
            if payload.get("hold_reason").and_then(Value::as_str) != Some(expected) {
                return Err(ValidationError::InvalidResearchTicker {
                    ticker: ticker.clone(),
                    reason: format!("Hold requires hold_reason={expected}"),
                });
            }
        }
        if payload
            .get("plan")
            .and_then(coerce_plan_to_string)
            .is_none_or(|value| value.trim().is_empty())
        {
            return Err(ValidationError::InvalidResearchTicker {
                ticker: ticker.clone(),
                reason: "plan must not be empty".to_string(),
            });
        }
        if payload
            .get("probability_rationale")
            .and_then(Value::as_str)
            .is_none_or(|value| value.trim().is_empty())
        {
            return Err(ValidationError::InvalidResearchTicker {
                ticker: ticker.clone(),
                reason: "probability_rationale must not be empty".to_string(),
            });
        }
    }
    Ok(())
}

fn scenario_driver_is_missing_evidence_placeholder(driver: &str) -> bool {
    let normalized = driver.trim().to_ascii_lowercase();
    [
        "no actionable",
        "no evidence",
        "evidence is insufficient",
        "evidence insufficient",
        "missing evidence",
        "缺乏证据",
        "证据不足",
        "没有证据",
    ]
    .iter()
    .any(|phrase| normalized.contains(phrase))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_marker_wrapped_json() {
        assert_eq!(
            extract_json_artifact(
                "x\n=== ARTIFACT_JSON_START ===\n{\"ok\":true}\n=== ARTIFACT_JSON_END ==="
            )
            .unwrap(),
            json!({"ok": true})
        );
    }

    #[test]
    fn normalizes_percent_strings() {
        assert_eq!(normalize_probability(&json!("68%")), Some(0.68));
        assert_eq!(normalize_probability(&json!(68)), Some(0.68));
        assert_eq!(normalize_probability(&json!(0.68)), Some(0.68));
    }

    #[test]
    fn analyst_schema_lists_machine_fields() {
        let schema = analyst_artifact_schema();
        for field in [
            "direction",
            "confidence",
            "report",
            "data_gaps",
            "key_evidence",
            "evidence_type",
        ] {
            assert!(schema.contains(field), "schema missing field {field}");
        }
        // Must be valid JSON.
        serde_json::from_str::<Value>(&schema).expect("analyst schema is valid JSON");
    }

    #[test]
    fn evidence_item_deserializes_structured() {
        let json = r#"{
            "claim": "CPI came in at 3.2%",
            "evidence_type": "fact",
            "source": "BLS via Jin10",
            "timestamp": "2026-07-06"
        }"#;
        let item: EvidenceItem = serde_json::from_str(json).unwrap();
        assert_eq!(item.claim, "CPI came in at 3.2%");
        assert_eq!(item.evidence_type, "fact");
        assert_eq!(item.source, "BLS via Jin10");
        assert_eq!(item.timestamp, "2026-07-06");
    }

    #[test]
    fn analyst_artifact_accepts_legacy_string_evidence() {
        let json = r#"{
            "direction": "bullish",
            "confidence": 0.7,
            "key_evidence": ["simple string evidence"]
        }"#;
        let artifact: AnalystTickerArtifact = serde_json::from_str(json).unwrap();
        assert_eq!(artifact.key_evidence.len(), 1);
        assert_eq!(artifact.key_evidence[0].claim, "simple string evidence");
        assert_eq!(artifact.key_evidence[0].evidence_type, "unclassified");
    }

    #[test]
    fn analyst_artifact_accepts_structured_evidence() {
        let json = r#"{
            "direction": "bullish",
            "confidence": 0.7,
            "key_evidence": [
                {"claim": "CPI 3.2%", "evidence_type": "fact", "source": "BLS", "timestamp": "2026-07-06"}
            ]
        }"#;
        let artifact: AnalystTickerArtifact = serde_json::from_str(json).unwrap();
        assert_eq!(artifact.key_evidence[0].claim, "CPI 3.2%");
        assert_eq!(artifact.key_evidence[0].evidence_type, "fact");
    }

    #[test]
    fn analyst_artifact_accepts_mixed_evidence_formats() {
        let json = r#"{
            "direction": "mixed",
            "confidence": 0.5,
            "key_evidence": [
                "legacy observation",
                {"claim": "Options rumor", "evidence_type": "speculation", "source": "Reddit"}
            ]
        }"#;
        let artifact: AnalystTickerArtifact = serde_json::from_str(json).unwrap();
        assert_eq!(artifact.key_evidence.len(), 2);
        assert_eq!(artifact.key_evidence[0].evidence_type, "unclassified");
        assert_eq!(artifact.key_evidence[1].evidence_type, "speculation");
        assert_eq!(artifact.key_evidence[1].timestamp, "");
    }

    #[test]
    fn normalize_evidence_type_maps_legacy_labels() {
        assert_eq!(normalize_evidence_type("rumor"), "speculation");
        assert_eq!(normalize_evidence_type("analyst_interpretation"), "opinion");
        let mut artifact = AnalystTickerArtifact {
            direction: "bullish".to_string(),
            confidence: 0.7,
            report: String::new(),
            key_evidence: vec![EvidenceItem {
                claim: "ambiguous claim".to_string(),
                evidence_type: "rumor".to_string(),
                source: String::new(),
                timestamp: String::new(),
                source_tier: String::new(),
                first_source: String::new(),
                is_derivative_repost: false,
                evidence_age: String::new(),
                source_confidence: 0.0,
            }],
            priced_in: String::new(),
            echo_chamber_risk: String::new(),
            crowded_consensus_risk: String::new(),
            validation_triggers: Vec::new(),
            data_gaps: Vec::new(),
        };
        normalize_analyst_ticker_artifact(&mut artifact);
        assert_eq!(artifact.key_evidence[0].evidence_type, "speculation");
        validate_evidence_types(&artifact).unwrap();
    }

    #[test]
    fn analyst_artifact_accepts_duplicate_age_aliases_and_assessment_claim() {
        let json = r#"{
            "direction": "bearish",
            "confidence": 0.62,
            "crowded_consensus_risk": "medium",
            "report": "short prose",
            "key_evidence": [{
                "evidence_type": "fact",
                "source_tier": "major_media",
                "evidence_age": "0-2d",
                "catalyst_age": "0-2d",
                "assessment": "半导体权重同步走弱",
                "source_confidence": 0.72
            }]
        }"#;
        let artifact: AnalystTickerArtifact = serde_json::from_str(json).unwrap();
        assert_eq!(artifact.key_evidence[0].claim, "半导体权重同步走弱");
        assert_eq!(artifact.key_evidence[0].evidence_age, "0-2d");
        validate_analyst_ticker_artifact(&artifact).unwrap();
    }

    #[test]
    fn analyst_schema_lists_new_quality_fields() {
        let schema = analyst_artifact_schema();
        for field in [
            "source_tier",
            "first_source",
            "is_derivative_repost",
            "evidence_age",
            "source_confidence",
            "echo_chamber_risk",
            "crowded_consensus_risk",
        ] {
            assert!(
                schema.contains(field),
                "analyst schema missing field {field}"
            );
        }
        serde_json::from_str::<Value>(&schema).expect("analyst schema is valid JSON");
    }

    #[test]
    fn risk_constraints_schema_lists_new_structured_fields() {
        let schema = risk_constraints_schema();
        for field in [
            "unique_risk_contribution",
            "disagreement_with_prior",
            "no_new_information",
            "stop_type",
            "max_drawdown_pct",
            "position_cap_pct",
            "rebalance_trigger",
            "risk_off_trigger",
            "review_window",
            "cash_hedge_recommendation",
            "constraint_confidence",
        ] {
            assert!(schema.contains(field), "risk schema missing field {field}");
        }
        serde_json::from_str::<Value>(&schema).expect("risk schema is valid JSON");
    }

    #[test]
    fn validate_evidence_types_rejects_invalid_source_tier() {
        let artifact = AnalystTickerArtifact {
            direction: "bullish".to_string(),
            confidence: 0.7,
            report: String::new(),
            key_evidence: vec![EvidenceItem {
                claim: "a claim".to_string(),
                evidence_type: "fact".to_string(),
                source: String::new(),
                timestamp: String::new(),
                source_tier: "garbage".to_string(),
                first_source: String::new(),
                is_derivative_repost: false,
                evidence_age: String::new(),
                source_confidence: 0.0,
            }],
            priced_in: String::new(),
            echo_chamber_risk: String::new(),
            crowded_consensus_risk: String::new(),
            validation_triggers: Vec::new(),
            data_gaps: Vec::new(),
        };
        let error = validate_evidence_types(&artifact).unwrap_err();
        assert!(error.contains("invalid source_tier 'garbage'"));
    }

    #[test]
    fn validate_evidence_types_rejects_invalid_echo_chamber_risk() {
        let artifact = AnalystTickerArtifact {
            direction: "bullish".to_string(),
            confidence: 0.7,
            report: String::new(),
            key_evidence: vec![EvidenceItem {
                claim: "a claim".to_string(),
                evidence_type: "fact".to_string(),
                source: String::new(),
                timestamp: String::new(),
                source_tier: String::new(),
                first_source: String::new(),
                is_derivative_repost: false,
                evidence_age: String::new(),
                source_confidence: 0.0,
            }],
            priced_in: String::new(),
            echo_chamber_risk: "extreme".to_string(),
            crowded_consensus_risk: String::new(),
            validation_triggers: Vec::new(),
            data_gaps: Vec::new(),
        };
        let error = validate_evidence_types(&artifact).unwrap_err();
        assert!(error.contains("invalid echo_chamber_risk 'extreme'"));
    }

    #[test]
    fn validate_risk_constraints_rejects_out_of_range_drawdown() {
        let artifact = RiskConstraints {
            stance: "neutral".to_string(),
            argument: String::new(),
            unique_risk_contribution: String::new(),
            disagreement_with_prior: String::new(),
            no_new_information: false,
            recommended_adjustment: String::new(),
            stop_type: String::new(),
            max_drawdown_pct: 1.5,
            position_cap_pct: 0.0,
            rebalance_trigger: String::new(),
            risk_off_trigger: String::new(),
            review_window: String::new(),
            cash_hedge_recommendation: String::new(),
            constraint_confidence: 0.0,
            extra: Map::new(),
        };
        let error = validate_risk_constraints(&artifact).unwrap_err();
        assert!(error.contains("max_drawdown_pct 1.5 out of range"));
    }

    #[test]
    fn validate_risk_constraints_rejects_invalid_stop_type() {
        let artifact = RiskConstraints {
            stance: "neutral".to_string(),
            argument: String::new(),
            unique_risk_contribution: String::new(),
            disagreement_with_prior: String::new(),
            no_new_information: false,
            recommended_adjustment: String::new(),
            stop_type: "weird".to_string(),
            max_drawdown_pct: 0.0,
            position_cap_pct: 0.0,
            rebalance_trigger: String::new(),
            risk_off_trigger: String::new(),
            review_window: String::new(),
            cash_hedge_recommendation: String::new(),
            constraint_confidence: 0.0,
            extra: Map::new(),
        };
        let error = validate_risk_constraints(&artifact).unwrap_err();
        assert!(error.contains("invalid stop_type 'weird'"));
    }

    #[test]
    fn analyst_artifact_with_new_fields_round_trips() {
        let json = r#"{
            "direction": "bullish",
            "confidence": 0.7,
            "echo_chamber_risk": "medium",
            "crowded_consensus_risk": "high",
            "key_evidence": [
                {
                    "claim": "CPI 3.2%",
                    "evidence_type": "fact",
                    "source": "BLS",
                    "timestamp": "2026-07-06",
                    "source_tier": "official",
                    "first_source": "BLS release",
                    "is_derivative_repost": false,
                    "evidence_age": "0-2d",
                    "source_confidence": 0.9
                }
            ]
        }"#;
        let artifact: AnalystTickerArtifact = serde_json::from_str(json).unwrap();
        assert_eq!(artifact.echo_chamber_risk, "medium");
        assert_eq!(artifact.crowded_consensus_risk, "high");
        assert_eq!(artifact.key_evidence[0].source_tier, "official");
        assert_eq!(artifact.key_evidence[0].first_source, "BLS release");
        assert!(!artifact.key_evidence[0].is_derivative_repost);
        assert_eq!(artifact.key_evidence[0].evidence_age, "0-2d");
        assert!((artifact.key_evidence[0].source_confidence - 0.9).abs() < f64::EPSILON);

        // Legacy artifact without new fields still deserializes via serde(default).
        let legacy: AnalystTickerArtifact =
            serde_json::from_str(r#"{"direction":"neutral","confidence":0.5}"#).unwrap();
        assert_eq!(legacy.echo_chamber_risk, "");
        assert_eq!(legacy.crowded_consensus_risk, "");
        assert!(legacy.key_evidence.is_empty());
    }

    #[test]
    fn risk_constraints_with_new_fields_round_trips() {
        let json = r#"{
            "stance": "conservative",
            "recommended_adjustment": "Cap exposure at 50%.",
            "stop_type": "trailing",
            "max_drawdown_pct": 0.15,
            "position_cap_pct": 0.5,
            "rebalance_trigger": "VIX > 25",
            "risk_off_trigger": "Overnight gap > 3%",
            "review_window": "3d",
            "cash_hedge_recommendation": "Hold 20% cash.",
            "constraint_confidence": 0.8
        }"#;
        let artifact: RiskConstraints = serde_json::from_str(json).unwrap();
        assert_eq!(artifact.stop_type, "trailing");
        assert!((artifact.max_drawdown_pct - 0.15).abs() < f64::EPSILON);
        assert!((artifact.position_cap_pct - 0.5).abs() < f64::EPSILON);
        assert_eq!(artifact.rebalance_trigger, "VIX > 25");
        assert_eq!(artifact.risk_off_trigger, "Overnight gap > 3%");
        assert_eq!(artifact.review_window, "3d");
        assert_eq!(artifact.cash_hedge_recommendation, "Hold 20% cash.");
        assert!((artifact.constraint_confidence - 0.8).abs() < f64::EPSILON);
        assert!(validate_risk_constraints(&artifact).is_ok());
    }

    fn valid_scenarios() -> Scenarios {
        Scenarios {
            bull: Scenario {
                probability: 0.35,
                drivers: vec!["Fed cut".to_string()],
                triggers: vec!["FOMC minutes".to_string()],
                confirmation: "Close above 500".to_string(),
            },
            base: Scenario {
                probability: 0.45,
                drivers: vec!["Range-bound".to_string()],
                triggers: vec!["VIX below 20".to_string()],
                confirmation: "5 days in range".to_string(),
            },
            bear: Scenario {
                probability: 0.20,
                drivers: vec!["Inflation".to_string()],
                triggers: vec!["CPI above 3.5%".to_string()],
                confirmation: "Close below 475".to_string(),
            },
        }
    }

    fn research_artifact_with_scenarios(scenarios: Option<Scenarios>) -> ResearchArtifact {
        ResearchArtifact {
            rating: "Hold".to_string(),
            long_probability: 0.575,
            short_probability: 0.425,
            confidence_basis: "evidence_balanced".to_string(),
            hold_reason: Some("evidence_balanced".to_string()),
            plan: "Monitor validation triggers.".to_string(),
            probability_rationale: "Evidence is balanced near the base probability.".to_string(),
            scenarios,
            per_ticker: BTreeMap::new(),
            extra: Map::new(),
        }
    }

    #[test]
    fn research_schema_lists_probability_fields() {
        let schema = research_artifact_schema();
        for field in [
            "rating",
            "long_probability",
            "short_probability",
            "confidence_basis",
            "hold_reason",
            "scenarios",
            "Scenario",
            "Scenarios",
            "drivers",
            "triggers",
            "confirmation",
        ] {
            assert!(schema.contains(field), "schema missing field {field}");
        }
        serde_json::from_str::<Value>(&schema).expect("research schema is valid JSON");
    }

    #[test]
    fn research_artifact_with_scenarios_validates() {
        let artifact = research_artifact_with_scenarios(Some(valid_scenarios()));
        assert!(validate_research_artifact(&artifact, &[]).is_ok());
    }

    #[test]
    fn scenario_probabilities_must_sum_to_one() {
        let artifact = research_artifact_with_scenarios(Some(Scenarios {
            bull: Scenario {
                probability: 0.4,
                drivers: vec!["x".into()],
                triggers: vec!["y".into()],
                confirmation: "z".into(),
            },
            base: Scenario {
                probability: 0.4,
                drivers: vec!["x".into()],
                triggers: vec!["y".into()],
                confirmation: "z".into(),
            },
            bear: Scenario {
                probability: 0.4,
                drivers: vec!["x".into()],
                triggers: vec!["y".into()],
                confirmation: "z".into(),
            },
        }));

        assert!(matches!(
            validate_research_artifact(&artifact, &[]),
            Err(ValidationError::ScenarioProbabilitySum(sum)) if (sum - 1.2).abs() < 0.001
        ));
    }

    #[test]
    fn inconsistent_long_probability_is_rejected() {
        let artifact = ResearchArtifact {
            long_probability: 0.7,
            short_probability: 0.3,
            ..research_artifact_with_scenarios(Some(Scenarios {
                bull: Scenario {
                    probability: 0.2,
                    drivers: vec!["x".into()],
                    triggers: vec!["y".into()],
                    confirmation: "z".into(),
                },
                base: Scenario {
                    probability: 0.5,
                    drivers: vec!["x".into()],
                    triggers: vec!["y".into()],
                    confirmation: "z".into(),
                },
                bear: Scenario {
                    probability: 0.3,
                    drivers: vec!["x".into()],
                    triggers: vec!["y".into()],
                    confirmation: "z".into(),
                },
            }))
        };

        assert!(matches!(
            validate_research_artifact(&artifact, &[]),
            Err(ValidationError::InconsistentLongProbability { long, expected })
                if (long - 0.7).abs() < 0.001 && (expected - 0.45).abs() < 0.001
        ));
    }

    #[test]
    fn research_artifact_requires_a_confidence_basis() {
        let mut artifact = research_artifact_with_scenarios(None);
        artifact.confidence_basis.clear();

        assert!(matches!(
            validate_research_artifact(&artifact, &["QQQ".to_string()]),
            Err(ValidationError::InvalidConfidenceBasis(_))
        ));
    }

    #[test]
    fn hold_research_artifact_requires_a_hold_reason() {
        let mut artifact = research_artifact_with_scenarios(None);
        artifact.hold_reason = None;

        assert!(matches!(
            validate_research_artifact(&artifact, &["QQQ".to_string()]),
            Err(ValidationError::InvalidHoldReason(_))
        ));
    }

    #[test]
    fn research_artifact_rejects_empty_per_ticker_decision() {
        let mut artifact = research_artifact_with_scenarios(None);
        artifact.per_ticker.insert("QQQ".to_string(), json!({}));

        assert!(matches!(
            validate_research_artifact(&artifact, &["QQQ".to_string()]),
            Err(ValidationError::InvalidResearchTicker { ticker, .. }) if ticker == "QQQ"
        ));
    }

    #[test]
    fn scenario_drivers_are_required() {
        let mut scenarios = valid_scenarios();
        scenarios.bull.drivers.clear();
        let artifact = research_artifact_with_scenarios(Some(scenarios));

        assert!(matches!(
            validate_research_artifact(&artifact, &[]),
            Err(ValidationError::InvalidProbability(message))
                if message == "scenario bull must have at least 1 driver"
        ));
    }

    #[test]
    fn scenario_drivers_reject_missing_evidence_as_a_causal_factor() {
        let mut scenarios = valid_scenarios();
        scenarios.bull.drivers = vec!["No actionable bullish evidence is available".into()];
        let artifact = research_artifact_with_scenarios(Some(scenarios));

        assert!(matches!(
            validate_research_artifact(&artifact, &[]),
            Err(ValidationError::InvalidProbability(message))
                if message.contains("causal market factors")
        ));
    }

    #[test]
    fn scenario_triggers_are_required() {
        let mut scenarios = valid_scenarios();
        scenarios.bear.triggers.clear();
        let artifact = research_artifact_with_scenarios(Some(scenarios));

        assert!(matches!(
            validate_research_artifact(&artifact, &[]),
            Err(ValidationError::InvalidProbability(message))
                if message == "scenario bear must have at least 1 trigger"
        ));
    }

    #[test]
    fn research_artifact_without_scenarios_still_validates() {
        let artifact = research_artifact_with_scenarios(None);
        assert!(validate_research_artifact(&artifact, &[]).is_ok());
    }

    #[test]
    fn research_artifact_without_scenarios_deserializes() {
        let json = r#"{
            "rating": "Hold",
            "long_probability": 0.55,
            "short_probability": 0.45,
            "confidence_basis": "evidence_balanced",
            "hold_reason": "evidence_balanced",
            "plan": "Monitor validation triggers.",
            "probability_rationale": "Evidence is balanced."
        }"#;
        let artifact: ResearchArtifact = serde_json::from_str(json).unwrap();
        assert_eq!(artifact.scenarios, None);
        assert!(validate_research_artifact(&artifact, &[]).is_ok());
    }

    #[test]
    fn normalize_lifts_per_ticker_fields_to_top_level() {
        let value = json!({
            "id": "research-manager",
            "role": "research_manager",
            "status": "completed",
            "report": "compressed evidence only",
            "per_ticker": {
                "QQQ": {
                    "rating": "Overweight",
                    "long_probability": 0.57,
                    "short_probability": 0.43,
                    "confidence_basis": "directional_evidence",
                    "plan": [
                        "Verify volume confirmation",
                        "Watch short-horizon break"
                    ],
                    "probability_rationale": "Near base after duplicate discount."
                },
                "SOXX": {
                    "rating": "Hold",
                    "long_probability": 0.51,
                    "short_probability": 0.49,
                    "confidence_basis": "data_insufficient",
                    "hold_reason": "evidence_insufficient",
                    "plan": "Wait for SOXX-specific confirmation",
                    "probability_rationale": "Insufficient SOXX-specific evidence."
                }
            }
        });

        let normalized =
            normalize_research_artifact_value(value, &["QQQ".to_string(), "SOXX".to_string()])
                .unwrap();
        let artifact: ResearchArtifact = serde_json::from_value(normalized).unwrap();
        assert_eq!(artifact.rating, "Overweight");
        assert!((artifact.long_probability - 0.57).abs() < 1e-9);
        assert!((artifact.short_probability - 0.43).abs() < 1e-9);
        assert_eq!(artifact.confidence_basis, "directional_evidence");
        assert_eq!(artifact.hold_reason, None);
        assert!(artifact.plan.contains("Verify volume confirmation"));
        assert!(artifact
            .probability_rationale
            .contains("duplicate discount"));
        assert!(artifact.per_ticker.contains_key("QQQ"));
        assert!(artifact.per_ticker.contains_key("SOXX"));
        assert!(
            validate_research_artifact(&artifact, &["QQQ".to_string(), "SOXX".to_string()]).is_ok()
        );
    }

    #[test]
    fn normalize_preserves_existing_top_level_fields() {
        let value = json!({
            "rating": "Hold",
            "long_probability": 0.52,
            "short_probability": 0.48,
            "plan": "Keep existing plan",
            "probability_rationale": "Top-level rationale",
            "per_ticker": {
                "QQQ": {
                    "rating": "Buy",
                    "long_probability": 0.7,
                    "short_probability": 0.3,
                    "plan": "Should not replace top-level",
                    "probability_rationale": "Nested rationale"
                }
            }
        });
        let normalized = normalize_research_artifact_value(value, &["QQQ".to_string()]).unwrap();
        let artifact: ResearchArtifact = serde_json::from_value(normalized).unwrap();
        assert_eq!(artifact.rating, "Hold");
        assert!((artifact.long_probability - 0.52).abs() < 1e-9);
        assert_eq!(artifact.plan, "Keep existing plan");
        assert_eq!(artifact.probability_rationale, "Top-level rationale");
    }

    #[test]
    fn normalize_coerces_top_level_plan_array() {
        let value = json!({
            "rating": "Hold",
            "long_probability": 0.5,
            "short_probability": 0.5,
            "plan": ["Watch VIX", "Reassess breadth"]
        });
        let normalized = normalize_research_artifact_value(value, &[]).unwrap();
        let artifact: ResearchArtifact = serde_json::from_value(normalized).unwrap();
        assert_eq!(artifact.plan, "Watch VIX; Reassess breadth");
    }

    #[test]
    fn downstream_contract_schemas_list_machine_fields() {
        for (schema, fields) in [
            (
                trade_intent_schema(),
                vec!["action", "entry_price", "position_size"],
            ),
            (
                risk_constraints_schema(),
                vec!["stance", "argument", "recommended_adjustment"],
            ),
            (
                final_validation_schema(),
                vec!["rating", "execution_summary", "risk_controls"],
            ),
            (
                portfolio_allocation_schema(),
                vec!["weights", "total_equity_exposure", "vix_regime"],
            ),
        ] {
            serde_json::from_str::<Value>(&schema).expect("contract schema is valid JSON");
            for field in fields {
                assert!(schema.contains(field), "schema missing field {field}");
            }
        }
    }

    #[test]
    fn validate_analyst_ticker_artifact_rejects_bad_direction() {
        let artifact = AnalystTickerArtifact {
            direction: "sideways".to_string(),
            confidence: 0.5,
            report: String::new(),
            key_evidence: Vec::new(),
            priced_in: String::new(),
            echo_chamber_risk: String::new(),
            crowded_consensus_risk: String::new(),
            validation_triggers: Vec::new(),
            data_gaps: Vec::new(),
        };
        let err = validate_analyst_ticker_artifact(&artifact).unwrap_err();
        assert!(err.contains("invalid direction"));
    }

    #[test]
    fn validate_analyst_ticker_artifact_accepts_valid_payload() {
        let artifact = AnalystTickerArtifact {
            direction: "bullish".to_string(),
            confidence: 0.7,
            report: "ok".to_string(),
            key_evidence: Vec::new(),
            priced_in: String::new(),
            echo_chamber_risk: String::new(),
            crowded_consensus_risk: String::new(),
            validation_triggers: Vec::new(),
            data_gaps: Vec::new(),
        };
        validate_analyst_ticker_artifact(&artifact).unwrap();
    }
}
