//! Deterministic, bounded Experience retrieval.
//!
//! This first implementation deliberately searches FileStore Views/Events
//! without a vector database. It treats all historical text as untrusted data:
//! ranking derives from typed identity and evidence metrics, while callers
//! retain responsibility for rendering rule text in a fenced data section.

use orchestrator_core::{ExperienceState, MarketRegime, Scope};
use orchestrator_store::{ExperienceLedger, ExperienceViewV1};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExperienceSearchQuery {
    pub phase: u8,
    pub role: String,
    pub ticker: Option<String>,
    pub horizon_trading_days: Option<u32>,
    pub regime: MarketRegime,
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
        let mut score = 0i64;
        if pattern.root_cause_phase == query.phase {
            score += 24;
        }
        if pattern.source_role == query.role {
            score += 18;
        }
        if ticker_matches(
            pattern.scope,
            pattern.ticker.as_deref(),
            query.ticker.as_deref(),
        ) {
            score += 16;
        }
        if pattern.horizon_trading_days == query.horizon_trading_days {
            score += 10;
        }
        if pattern.regime.is_compatible_with(&query.regime) {
            score += 8;
        }
        score += lexical_score(&query.lexical_query, &rule.rule);
        score += i64::from(view.support_count.min(10));
        score += view
            .utility_ema_micros
            .unwrap_or_default()
            .clamp(-10_000_000, 10_000_000)
            / 1_000_000;
        score -= i64::from(view.contradiction_count) * 12;
        score -= i64::from(view.harmful_usage_rate_ppm) / 50_000;
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
                regime: MarketRegime::default(),
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
                lexical_query: "technical confirmation".into(),
                max_results: 1,
            },
        )
        .unwrap();
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.stop_reason, RetrievalStopReason::Sufficient);
    }
}
