//! Deterministic, bounded Experience retrieval.
//!
//! This first implementation deliberately searches FileStore Views/Events
//! without a vector database. It treats all historical text as untrusted data:
//! ranking derives from typed identity and evidence metrics, while callers
//! retain responsibility for rendering rule text in a fenced data section.

use chrono::NaiveDate;
use orchestrator_core::{ExperienceState, MarketRegime, Scope};
use orchestrator_store::{ExperienceLedger, ExperienceViewV1};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExperienceSearchQuery {
    pub phase: u8,
    pub role: String,
    pub ticker: Option<String>,
    pub horizon_trading_days: Option<u32>,
    pub regime: MarketRegime,
    /// The frozen date of the current run. Experiences learned after this
    /// date are excluded so a replay cannot import future hindsight.
    pub as_of_date: Option<NaiveDate>,
    pub lexical_query: String,
    pub max_results: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetrievalStopReason {
    Sufficient,
    NoMarginalGain,
    NoMatch,
    ConflictUnresolved,
    BudgetExhausted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrievedExperience {
    pub pattern_id: String,
    pub score: i64,
    pub view: ExperienceViewV1,
    pub scope: Scope,
    pub ticker: Option<String>,
    pub horizon_trading_days: Option<u32>,
    pub regime: MarketRegime,
    pub recency_penalty: i64,
    pub rule: String,
    pub trigger_conditions: Vec<String>,
    pub invalidation_conditions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExperienceSearchResult {
    pub items: Vec<RetrievedExperience>,
    pub stop_reason: RetrievalStopReason,
}

pub fn search_experiences(
    ledger: &ExperienceLedger,
    query: &ExperienceSearchQuery,
) -> orchestrator_store::Result<ExperienceSearchResult> {
    let limit = query.max_results.clamp(1, 50);
    let mut candidates = Vec::new();
    for view in ledger.list_views()? {
        if matches!(
            view.state,
            ExperienceState::Suspended | ExperienceState::Retired
        ) {
            continue;
        }
        let events = ledger.read_events(&view.pattern_id)?;
        let Some(event) = events
            .iter()
            .rev()
            .find(|event| event.pattern_identity.is_some() && event.rule_revision.is_some())
        else {
            continue;
        };
        let pattern = event.pattern_identity.as_ref().expect("filtered");
        let rule = event.rule_revision.as_ref().expect("filtered");
        // Applicability is a hard admission check, not a ranking bonus. A
        // QQQ lesson, a different holding horizon, or a regime that has not
        // been observed in the current frozen input must not leak into a
        // current decision merely because its wording scores well.
        if !ticker_matches(
            pattern.scope,
            pattern.ticker.as_deref(),
            query.ticker.as_deref(),
        ) || !horizon_matches(pattern.horizon_trading_days, query.horizon_trading_days)
            || !pattern.regime.is_compatible_with(&query.regime)
            || !experience_is_available_at_as_of(&view, query.as_of_date)
        {
            continue;
        }
        let recency_penalty = experience_recency_penalty(&view, query.as_of_date);
        let mut score = 0i64;
        if pattern.root_cause_phase == query.phase {
            score += 24;
        }
        if pattern.source_role == query.role {
            score += 18;
        }
        score += 16;
        score += 10;
        score += 8;
        score += lexical_score(&query.lexical_query, &rule.rule);
        score += i64::from(view.support_count.min(10));
        score += view
            .utility_ema_micros
            .unwrap_or_default()
            .clamp(-10_000_000, 10_000_000)
            / 1_000_000;
        score -= i64::from(view.contradiction_count) * 12;
        score -= i64::from(view.harmful_usage_rate_ppm) / 50_000;
        score -= recency_penalty;
        score += match view.state {
            ExperienceState::Active => 16,
            ExperienceState::RepeatedWarning => 6,
            ExperienceState::Candidate => 0,
            ExperienceState::Contested => -18,
            ExperienceState::Suspended | ExperienceState::Retired => unreachable!("filtered"),
        };
        candidates.push(RetrievedExperience {
            pattern_id: view.pattern_id.clone(),
            score,
            view,
            scope: pattern.scope,
            ticker: pattern.ticker.clone(),
            horizon_trading_days: pattern.horizon_trading_days,
            regime: pattern.regime.clone(),
            recency_penalty,
            rule: rule.rule.clone(),
            trigger_conditions: rule.trigger_conditions.clone(),
            invalidation_conditions: rule.invalidation_conditions.clone(),
        });
    }
    candidates.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.pattern_id.cmp(&right.pattern_id))
    });
    let all_contested = !candidates.is_empty()
        && candidates
            .iter()
            .all(|candidate| candidate.view.state == ExperienceState::Contested);
    candidates.retain(|candidate| candidate.score > 0);
    let stop_reason = if candidates.is_empty() {
        if all_contested {
            RetrievalStopReason::ConflictUnresolved
        } else {
            RetrievalStopReason::NoMatch
        }
    } else if candidates.len() > limit {
        candidates.truncate(limit);
        RetrievalStopReason::BudgetExhausted
    } else if candidates.len() == limit {
        RetrievalStopReason::Sufficient
    } else {
        RetrievalStopReason::NoMarginalGain
    };
    Ok(ExperienceSearchResult {
        items: candidates,
        stop_reason,
    })
}

fn ticker_matches(scope: Scope, pattern_ticker: Option<&str>, query_ticker: Option<&str>) -> bool {
    match (scope, pattern_ticker, query_ticker) {
        (Scope::Ticker, Some(pattern), Some(query)) => pattern == query,
        (Scope::Ticker, _, _) => false,
        (_, _, _) => true,
    }
}

fn horizon_matches(pattern_horizon: Option<u32>, query_horizon: Option<u32>) -> bool {
    match query_horizon {
        Some(query_horizon) => pattern_horizon == Some(query_horizon),
        // An unknown current horizon cannot justify applying a horizon-bound
        // historical lesson. Only an explicitly horizon-agnostic Pattern may
        // remain visible in that case.
        None => pattern_horizon.is_none(),
    }
}

fn experience_is_available_at_as_of(
    view: &ExperienceViewV1,
    as_of_date: Option<NaiveDate>,
) -> bool {
    let Some(as_of_date) = as_of_date else {
        return true;
    };
    view.last_supported_at
        .as_deref()
        .and_then(parse_experience_date)
        .is_some_and(|supported_at| supported_at <= as_of_date)
}

fn experience_recency_penalty(view: &ExperienceViewV1, as_of_date: Option<NaiveDate>) -> i64 {
    let Some(as_of_date) = as_of_date else {
        return 0;
    };
    let Some(supported_at) = view
        .last_supported_at
        .as_deref()
        .and_then(parse_experience_date)
    else {
        return 0;
    };
    let age_days = (as_of_date - supported_at).num_days().max(0);
    // Four ranking points per 90 calendar days, capped so a sound but older
    // lesson remains inspectable when it is otherwise highly applicable.
    ((age_days / 90) * 4).min(32)
}

fn parse_experience_date(value: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(value.trim().get(..10)?, "%Y-%m-%d").ok()
}

fn lexical_score(query: &str, rule: &str) -> i64 {
    let rule = rule.to_ascii_lowercase();
    query
        .split(|character: char| !character.is_alphanumeric())
        .filter(|token| token.len() >= 3)
        .map(|token| i64::from(rule.contains(&token.to_ascii_lowercase())))
        .sum::<i64>()
        * 3
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{
        DocumentRef, ExperienceOperation, PatternActionKind, PatternIdentityV1, RuleRevisionV1,
        SignalFamily,
    };
    use orchestrator_store::{
        ExperienceEventV1, FileStore, FileStoreOptions, EXPERIENCE_EVENT_SCHEMA_VERSION,
    };

    fn ledger() -> ExperienceLedger {
        let directory = tempfile::tempdir().unwrap();
        // Keep the temporary directory alive for the test process; FileStore
        // owns no external state and this is only a pure ranking fixture.
        let path = directory.keep();
        ExperienceLedger::new(FileStore::open(path, FileStoreOptions::default()).unwrap())
    }

    fn event() -> ExperienceEventV1 {
        ExperienceEventV1 {
            schema_version: EXPERIENCE_EVENT_SCHEMA_VERSION,
            sequence: 0,
            event_id: String::new(),
            pattern_id: "pattern".into(),
            pattern_identity: Some(PatternIdentityV1 {
                root_cause_phase: 3,
                source_role: "manager.research".into(),
                scope: Scope::Ticker,
                ticker: Some("QQQ".into()),
                horizon_trading_days: Some(3),
                regime: MarketRegime {
                    volatility: "normal".into(),
                    ..Default::default()
                },
                signal_family: SignalFamily::Technical,
                action_kind: PatternActionKind::Hold,
            }),
            rule_revision: Some(RuleRevisionV1 {
                revision: 1,
                rule: "Wait for technical confirmation".into(),
                trigger_conditions: vec!["confirmation".into()],
                invalidation_conditions: vec!["breakdown".into()],
            }),
            operation: ExperienceOperation::AddSupport,
            source_run_id: Some("source".into()),
            outcome_id: Some("outcome".into()),
            source_refs: vec![DocumentRef {
                document_id: "summary".into(),
                relative_path: "runs/date/source/index/summary/index.json".into(),
                content_hash: "sha256:summary".into(),
            }],
            policy_ref: None,
            independent_date_cluster: Some("2026-01-01".into()),
            independent_regime: Some("normal".into()),
            utility_sample_micros: Some(1_000_000),
            harmful_usage: Some(false),
            created_at: "2026-01-01T00:00:00Z".into(),
            content_hash: String::new(),
        }
    }

    #[test]
    fn ranking_is_structured_and_bounded() {
        let ledger = ledger();
        ledger.append(event()).unwrap();
        ledger.rebuild_view("pattern", "now").unwrap();
        let result = search_experiences(
            &ledger,
            &ExperienceSearchQuery {
                phase: 3,
                role: "manager.research".into(),
                ticker: Some("QQQ".into()),
                horizon_trading_days: Some(3),
                regime: MarketRegime::default(),
                as_of_date: Some(NaiveDate::from_ymd_opt(2026, 1, 2).unwrap()),
                lexical_query: "technical confirmation".into(),
                max_results: 1,
            },
        )
        .unwrap();
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.stop_reason, RetrievalStopReason::Sufficient);
    }

    #[test]
    fn retrieval_filters_ticker_horizon_regime_and_future_hindsight_before_ranking() {
        let ledger = ledger();
        let base = event();
        ledger.append(base).unwrap();

        let mut wrong_ticker = event();
        wrong_ticker.pattern_id = "wrong-ticker".into();
        wrong_ticker.pattern_identity.as_mut().unwrap().ticker = Some("SOXX".into());
        ledger.append(wrong_ticker).unwrap();

        let mut wrong_horizon = event();
        wrong_horizon.pattern_id = "wrong-horizon".into();
        wrong_horizon
            .pattern_identity
            .as_mut()
            .unwrap()
            .horizon_trading_days = Some(5);
        ledger.append(wrong_horizon).unwrap();

        let mut wrong_regime = event();
        wrong_regime.pattern_id = "wrong-regime".into();
        wrong_regime.pattern_identity.as_mut().unwrap().regime = MarketRegime {
            volatility: "elevated".into(),
            ..Default::default()
        };
        ledger.append(wrong_regime).unwrap();

        let mut future = event();
        future.pattern_id = "future".into();
        future.created_at = "2026-02-01T00:00:00Z".into();
        ledger.append(future).unwrap();

        for pattern_id in [
            "pattern",
            "wrong-ticker",
            "wrong-horizon",
            "wrong-regime",
            "future",
        ] {
            ledger
                .rebuild_view(pattern_id, "2026-02-02T00:00:00Z")
                .unwrap();
        }

        let result = search_experiences(
            &ledger,
            &ExperienceSearchQuery {
                phase: 3,
                role: "manager.research".into(),
                ticker: Some("QQQ".into()),
                horizon_trading_days: Some(3),
                regime: MarketRegime {
                    volatility: "normal".into(),
                    ..Default::default()
                },
                as_of_date: Some(NaiveDate::from_ymd_opt(2026, 1, 15).unwrap()),
                lexical_query: "technical confirmation".into(),
                max_results: 10,
            },
        )
        .unwrap();

        assert_eq!(
            result
                .items
                .iter()
                .map(|item| item.pattern_id.as_str())
                .collect::<Vec<_>>(),
            vec!["pattern"]
        );
        assert_eq!(result.items[0].recency_penalty, 0);
    }

    #[test]
    fn retrieval_applies_a_bounded_recency_penalty() {
        let ledger = ledger();
        ledger.append(event()).unwrap();
        ledger
            .rebuild_view("pattern", "2026-08-01T00:00:00Z")
            .unwrap();

        let result = search_experiences(
            &ledger,
            &ExperienceSearchQuery {
                phase: 3,
                role: "manager.research".into(),
                ticker: Some("QQQ".into()),
                horizon_trading_days: Some(3),
                regime: MarketRegime::default(),
                as_of_date: Some(NaiveDate::from_ymd_opt(2026, 8, 1).unwrap()),
                lexical_query: String::new(),
                max_results: 1,
            },
        )
        .unwrap();

        assert_eq!(result.items.len(), 1);
        assert!(result.items[0].recency_penalty > 0);
    }
}
