use super::*;
use crate::observer_analytics::{
    benchmark_equity_series, comparison_max_drawdown_ppm, compounded_ppm, managed_realized_pnl,
    outcome_comparison, outcome_statistics, parse_bar_series, parse_fill_activities,
    policy_exposure_ppm, portfolio_analytics, readiness_ppm, ObserverBrokerFill,
    ObserverOutcomeComparisonPoint, ObserverOutcomeStatistics, ObserverPortfolioAnalytics,
};
use akzio_domain::{
    DecisionProposal, Evaluation, Experience, Outcome, OutcomeHorizon, OutcomeWindow,
    PaperLaunchApproval, PolicyState, PolicyTransition, ResearchCritique, RetrospectiveCategory,
    RuntimeManifest, WorkflowProposalDraft,
};
use chrono::Duration;
use serde::de::DeserializeOwned;
use std::collections::{BTreeMap, BTreeSet};

const OBSERVER_RUN_LIMIT: usize = 20;
const OBSERVER_EVENT_LIMIT: usize = 100;
const OBSERVER_TRAJECTORY_LIMIT: usize = 200;
const OBSERVER_LEARNING_LIMIT: usize = 100;
const OBSERVER_BROKER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ObserverSectionStatus {
    Available,
    Pending,
    Unavailable,
}

#[derive(Debug, Serialize)]
pub(crate) struct ObserverSection<T> {
    pub status: ObserverSectionStatus,
    pub observed_at: Option<DateTime<Utc>>,
    pub reason: Option<String>,
    pub data: Option<T>,
}

impl<T> ObserverSection<T> {
    fn available(observed_at: DateTime<Utc>, data: T) -> Self {
        Self {
            status: ObserverSectionStatus::Available,
            observed_at: Some(observed_at),
            reason: None,
            data: Some(data),
        }
    }

    fn unavailable(reason: impl Into<String>) -> Self {
        Self {
            status: ObserverSectionStatus::Unavailable,
            observed_at: None,
            reason: Some(reason.into()),
            data: None,
        }
    }

    fn pending(reason: impl Into<String>) -> Self {
        Self {
            status: ObserverSectionStatus::Pending,
            observed_at: None,
            reason: Some(reason.into()),
            data: None,
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct ObserverSnapshot {
    pub schema_version: u32,
    pub generated_at: DateTime<Utc>,
    pub event_cursor: i64,
    pub core: ObserverCoreStatus,
    pub current_run: Option<ObserverRunDetail>,
    pub recent_runs: Vec<WorkflowSnapshot>,
    pub run_summaries: Vec<ObserverRunSummary>,
    pub portfolio: ObserverSection<ObserverPortfolio>,
    pub outcome: ObserverSection<ObserverOutcome>,
    pub learning: ObserverSection<ObserverLearning>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ObserverCoreStatus {
    pub ready: bool,
    pub readiness_ppm: u32,
    pub auto_paper: bool,
    pub health: DaemonHealth,
    pub approval: ObserverApprovalStatus,
}

#[derive(Debug, Serialize)]
pub(crate) struct ObserverApprovalStatus {
    pub status: String,
    pub operator_identity: Option<String>,
    pub reason: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ObserverRunDetail {
    pub workflow: WorkflowSnapshot,
    pub events: Vec<EventView>,
    pub trajectory: Vec<TrajectoryEntry>,
    pub artifacts: Vec<ObserverArtifactView>,
    pub telemetry: ObserverRunTelemetry,
}

#[derive(Debug, Serialize)]
pub(crate) struct ObserverRunTelemetry {
    pub model_id: Option<String>,
    pub latency_millis: Option<u64>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub tool_calls: usize,
    pub turns: usize,
}

#[derive(Debug, Serialize)]
pub(crate) struct ObserverRunSummary {
    pub run_id: RunId,
    pub model_id: Option<String>,
    pub latency_millis: Option<u64>,
    pub broker_session: Option<String>,
    pub result_utility_ppm: Option<i64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ObserverArtifactView {
    pub artifact_id: ArtifactId,
    pub kind: ArtifactKind,
    pub created_at: DateTime<Utc>,
    pub payload: Value,
}

#[derive(Debug, Serialize)]
pub(crate) struct ObserverLearning {
    pub artifacts: Vec<ObserverArtifactView>,
    pub policy_transitions: Vec<ObserverPolicyTransition>,
    pub summary: ObserverLearningSummary,
    pub policy_metrics: Vec<ObserverPolicyMetrics>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ObserverLearningSummary {
    pub range_days: u32,
    pub attributed_utility_micros: Option<i64>,
    pub attributed_utility_ppm: Option<i64>,
    pub lesson_candidates: usize,
    pub lesson_candidates_delta: i64,
    pub policies_evolved: usize,
    pub policies_evolved_delta: i64,
    pub impact_areas: Vec<ObserverImpactArea>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ObserverImpactArea {
    pub category: RetrospectiveCategory,
    pub impact_ppm: i64,
}

#[derive(Debug, Serialize)]
pub(crate) struct ObserverPolicyMetrics {
    pub subject: PolicySubject,
    pub state: PolicyState,
    pub sample_count: usize,
    pub win_rate_ppm: Option<i64>,
    pub net_impact_ppm: Option<i64>,
    pub stability_ppm: Option<i64>,
    pub exposure_ppm: Option<u32>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ObserverPolicyTransition {
    pub transition: PolicyTransition,
    pub run_id: RunId,
    pub revision: u64,
    pub transition_cursor: i64,
}

#[derive(Debug, Serialize)]
pub(crate) struct ObserverPortfolio {
    pub broker_session: String,
    pub market_open: bool,
    pub status: String,
    pub equity_micros: i64,
    pub last_equity_micros: Option<i64>,
    pub buying_power_micros: i64,
    pub day_pnl_micros: Option<i64>,
    pub day_pnl_ppm: Option<i64>,
    pub realized_pnl_micros: Option<i64>,
    pub realized_pnl_ppm: Option<i64>,
    pub fills: ObserverSection<Vec<ObserverBrokerFill>>,
    pub analytics: ObserverSection<ObserverPortfolioAnalytics>,
    pub positions: Vec<ObserverPosition>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ObserverPosition {
    pub symbol: String,
    pub quantity_micros: i64,
    pub market_value_micros: i64,
    pub average_entry_price_micros: Option<i64>,
    pub unrealized_pnl_micros: Option<i64>,
    pub unrealized_pnl_ppm: Option<i64>,
    pub sparkline_ppm: Vec<i64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ObserverOutcome {
    pub outcome_id: String,
    pub completed_trading_sessions: u8,
    pub horizons: Vec<ObserverOutcomeHorizon>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ObserverOutcomeHorizon {
    pub horizon: OutcomeHorizon,
    pub progress_ppm: u32,
    pub window: Option<OutcomeWindow>,
    pub sample_count: usize,
    pub win_rate_ppm: Option<i64>,
    pub profit_factor_ppm: Option<i64>,
    pub sharpe_ppm: Option<i64>,
    pub max_drawdown_ppm: Option<i64>,
    pub comparison: Vec<ObserverOutcomeComparisonPoint>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ObserverPortfolioRange {
    #[serde(rename = "1d")]
    OneDay,
    #[serde(rename = "1w")]
    OneWeek,
    #[serde(rename = "1m")]
    OneMonth,
    #[serde(rename = "3m")]
    ThreeMonths,
}

impl ObserverPortfolioRange {
    fn paper_range(self) -> PortfolioHistoryRange {
        match self {
            Self::OneDay => PortfolioHistoryRange::OneDay,
            Self::OneWeek => PortfolioHistoryRange::OneWeek,
            Self::OneMonth => PortfolioHistoryRange::OneMonth,
            Self::ThreeMonths => PortfolioHistoryRange::ThreeMonths,
        }
    }

    fn as_query_value(self) -> &'static str {
        match self {
            Self::OneDay => "1d",
            Self::OneWeek => "1w",
            Self::OneMonth => "1m",
            Self::ThreeMonths => "3m",
        }
    }

    fn benchmark_start(self, now: DateTime<Utc>) -> NaiveDate {
        let days = match self {
            Self::OneDay => 1,
            Self::OneWeek => 8,
            Self::OneMonth => 35,
            Self::ThreeMonths => 100,
        };
        (now - Duration::days(days)).date_naive()
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct ObserverPortfolioHistory {
    pub range: ObserverPortfolioRange,
    pub benchmark_symbol: &'static str,
    pub points: Vec<ObserverPortfolioHistoryPoint>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ObserverPortfolioHistoryPoint {
    pub timestamp: DateTime<Utc>,
    pub equity_micros: i64,
    pub profit_loss_micros: Option<i64>,
    pub profit_loss_ppm: Option<i64>,
    pub benchmark_equity_micros: Option<i64>,
}

impl Daemon {
    pub(crate) async fn observer_snapshot(&self) -> Result<ObserverSnapshot> {
        let generated_at = Utc::now();
        let recent_runs = self.store.recent_workflows(OBSERVER_RUN_LIMIT)?;
        let current_run = recent_runs
            .first()
            .map(|workflow| self.observer_run_detail(&workflow.run.run_id))
            .transpose()?;
        let run_summaries = recent_runs
            .iter()
            .map(|workflow| self.observer_run_summary(&workflow.run.run_id))
            .collect::<Result<Vec<_>>>()?;
        let health = self.health()?;
        let ready = self.ready().is_ok();
        let portfolio = self
            .observer_portfolio(generated_at, current_run.as_ref())
            .await;
        let outcome = self.observer_outcome(generated_at)?;
        let learning = self.observer_learning(generated_at)?;

        Ok(ObserverSnapshot {
            schema_version: 2,
            generated_at,
            event_cursor: self.store.event_cursor()?,
            core: ObserverCoreStatus {
                ready,
                readiness_ppm: readiness_ppm(
                    ready,
                    health.frozen,
                    self.auto_paper,
                    health.scheduler_owner.is_some(),
                ),
                auto_paper: self.auto_paper,
                health,
                approval: self.observer_approval(generated_at)?,
            },
            current_run,
            recent_runs,
            run_summaries,
            portfolio,
            outcome,
            learning,
        })
    }

    pub(crate) fn observer_run_detail(&self, run_id: &RunId) -> Result<ObserverRunDetail> {
        let workflow = self.store.workflow_snapshot(run_id)?;
        let events = self
            .store
            .recent_events(run_id, OBSERVER_EVENT_LIMIT)?
            .into_iter()
            .map(|event| EventView {
                cursor: event.cursor,
                event_type: event.event_type,
                task_id: event.task_id.map(|task| task.0),
                created_at: event.created_at.to_rfc3339(),
            })
            .collect();
        let trajectory = self
            .store
            .recent_trajectory(run_id, OBSERVER_TRAJECTORY_LIMIT)?;
        let artifacts = self.observer_artifacts(&trajectory, |_| true)?;
        let telemetry = observer_run_telemetry(&trajectory);

        Ok(ObserverRunDetail {
            workflow,
            events,
            trajectory,
            artifacts,
            telemetry,
        })
    }

    fn observer_run_summary(&self, run_id: &RunId) -> Result<ObserverRunSummary> {
        let trajectory = self.store.recent_trajectory(run_id, 64)?;
        let telemetry = observer_run_telemetry(&trajectory);
        let result_utility_ppm = trajectory
            .iter()
            .rev()
            .find_map(|entry| {
                (entry.artifact_kind == Some(ArtifactKind::Outcome))
                    .then_some(entry.artifact_id.as_ref())
                    .flatten()
            })
            .map(|artifact_id| -> Result<Option<i64>> {
                let artifact = self.store.artifact(artifact_id)?;
                let outcome: Outcome =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                Ok(outcome
                    .windows
                    .iter()
                    .max_by_key(|window| window.horizon.trading_days())
                    .map(|window| window.utility_ppm))
            })
            .transpose()?
            .flatten();
        Ok(ObserverRunSummary {
            run_id: run_id.clone(),
            model_id: telemetry.model_id,
            latency_millis: telemetry.latency_millis,
            broker_session: self
                .store
                .session_slot_for_run(run_id)?
                .map(|slot| slot.session_key),
            result_utility_ppm,
        })
    }

    pub(crate) async fn observer_portfolio_history(
        &self,
        range: ObserverPortfolioRange,
    ) -> ObserverSection<ObserverPortfolioHistory> {
        let Some(paper) = self.paper_observer.as_ref() else {
            return ObserverSection::unavailable("Alpaca Paper observer is not configured");
        };
        let now = Utc::now();
        let result = tokio::time::timeout(OBSERVER_BROKER_TIMEOUT, async {
            tokio::join!(
                paper.portfolio_history(range.paper_range()),
                self.observer_qqq_bars(range, now)
            )
        })
        .await;
        let Ok((history_result, benchmark_result)) = result else {
            return ObserverSection::unavailable("Alpaca Paper portfolio history timed out");
        };
        let value = match history_result {
            Ok(value) => value,
            Err(error) => return ObserverSection::unavailable(error.to_string()),
        };
        let mut history = match parse_portfolio_history(range, &value) {
            Ok(history) => history,
            Err(error) => return ObserverSection::unavailable(error.to_string()),
        };
        if let Ok(benchmark) = benchmark_result {
            let portfolio = history
                .points
                .iter()
                .map(|point| (point.timestamp, point.equity_micros))
                .collect::<Vec<_>>();
            for (point, benchmark_equity) in history
                .points
                .iter_mut()
                .zip(benchmark_equity_series(&portfolio, &benchmark))
            {
                point.benchmark_equity_micros = benchmark_equity;
            }
        }
        ObserverSection::available(now, history)
    }

    async fn observer_qqq_bars(
        &self,
        range: ObserverPortfolioRange,
        now: DateTime<Utc>,
    ) -> Result<Vec<crate::observer_analytics::ObserverBarPoint>> {
        let adapter = self
            .production_evidence
            .get(&EvidenceSource::Alpaca)
            .ok_or_else(|| {
                DaemonError::Unavailable("Alpaca market data is not configured".to_owned())
            })?;
        let resource = format!(
            "observer.qqq_history:{}:{}",
            range.as_query_value(),
            range.benchmark_start(now)
        );
        let acquired = adapter
            .acquire(&EvidenceRequest {
                source: EvidenceSource::Alpaca,
                resource,
                max_age: Duration::days(1),
            })
            .await
            .map_err(|error| DaemonError::Unavailable(error.to_string()))?;
        let value: Value = serde_json::from_slice(&acquired.raw)?;
        parse_bar_series(&value).map_err(DaemonError::Unavailable)
    }

    fn observer_outcome(
        &self,
        observed_at: DateTime<Utc>,
    ) -> Result<ObserverSection<ObserverOutcome>> {
        let artifacts = self
            .store
            .recent_artifacts_by_kind(ArtifactKind::Outcome, 100)?;
        let mut decoded = artifacts
            .into_iter()
            .map(|artifact| {
                let outcome: Outcome =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                Ok::<_, DaemonError>((artifact, outcome))
            })
            .collect::<Result<Vec<_>>>()?;
        if decoded.is_empty() {
            return Ok(ObserverSection::pending(
                "No canonical Outcome is available yet",
            ));
        }
        decoded.sort_by_key(|(artifact, outcome)| {
            (
                outcome.windows.len(),
                outcome.sealed_at,
                artifact.created_at,
            )
        });
        let (_, current) = decoded.last().expect("non-empty Outcome list");
        let statistics = outcome_statistics(
            &decoded
                .iter()
                .filter(|(_, outcome)| outcome.is_sealed())
                .map(|(_, outcome)| outcome.clone())
                .collect::<Vec<_>>(),
        );
        let comparison = self
            .observer_outcome_comparison(current)
            .unwrap_or_default();
        let completed_trading_sessions = current
            .windows
            .iter()
            .map(|window| window.horizon.trading_days())
            .max()
            .unwrap_or(0);
        let horizons = OutcomeHorizon::ALL
            .into_iter()
            .map(|horizon| {
                let stats = statistics
                    .iter()
                    .find(|stats| stats.horizon == horizon)
                    .cloned()
                    .unwrap_or(ObserverOutcomeStatistics {
                        horizon,
                        sample_count: 0,
                        win_rate_ppm: None,
                        profit_factor_ppm: None,
                        sharpe_ppm: None,
                    });
                let window = current
                    .windows
                    .iter()
                    .find(|window| window.horizon == horizon)
                    .cloned();
                let horizon_comparison = window
                    .as_ref()
                    .map(|window| window.observed_trading_day)
                    .map(|day| {
                        comparison
                            .iter()
                            .filter(|point| point.trading_day <= day)
                            .cloned()
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                ObserverOutcomeHorizon {
                    horizon,
                    progress_ppm: u32::from(completed_trading_sessions.min(horizon.trading_days()))
                        * 1_000_000
                        / u32::from(horizon.trading_days()),
                    window,
                    sample_count: stats.sample_count,
                    win_rate_ppm: stats.win_rate_ppm,
                    profit_factor_ppm: stats.profit_factor_ppm,
                    sharpe_ppm: stats.sharpe_ppm,
                    max_drawdown_ppm: comparison_max_drawdown_ppm(&horizon_comparison),
                    comparison: horizon_comparison,
                }
            })
            .collect();
        Ok(ObserverSection::available(
            observed_at,
            ObserverOutcome {
                outcome_id: current.outcome_id.0.clone(),
                completed_trading_sessions,
                horizons,
            },
        ))
    }

    fn observer_outcome_comparison(
        &self,
        outcome: &Outcome,
    ) -> Result<Vec<ObserverOutcomeComparisonPoint>> {
        let schedule_artifact = self.store.artifact(&outcome.schedule.artifact_id)?;
        let schedule: OutcomeSchedule =
            serde_json::from_slice(&self.store.read_blob(&schedule_artifact.blob)?)?;
        let execution_context_artifact = self
            .store
            .artifact(&schedule.execution_context.artifact_id)?;
        let execution_context: ExecutionContext =
            serde_json::from_slice(&self.store.read_blob(&execution_context_artifact.blob)?)?;
        let target = self.realized_execution_target(&schedule, &execution_context)?;
        let quote_reference = execution_context.quote_snapshot.as_ref().ok_or_else(|| {
            DaemonError::Unavailable("Outcome has no baseline QuoteSnapshot".to_owned())
        })?;
        let quote_artifact = self.store.artifact(&quote_reference.artifact_id)?;
        let quotes: QuoteSnapshot =
            serde_json::from_slice(&self.store.read_blob(&quote_artifact.blob)?)?;
        let baseline_prices = quotes
            .quotes
            .into_iter()
            .map(|(asset, quote)| {
                let midpoint = quote
                    .bid
                    .0
                    .checked_add(quote.ask.0)
                    .and_then(|value| value.checked_div(2))
                    .unwrap_or_default();
                (asset, MoneyMicros(midpoint))
            })
            .collect::<BTreeMap<_, _>>();
        let mut bars_by_asset = BTreeMap::new();
        for reference in &outcome.market_evidence {
            if reference.kind != ArtifactKind::NormalizedEvidence {
                continue;
            }
            let artifact = self.store.artifact(&reference.artifact_id)?;
            let payload: NormalizedEvidencePayload =
                serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
            let mut parts = payload.resource.split(':');
            if parts.next() != Some("bars") {
                continue;
            }
            let Some(symbol) = parts.next() else {
                continue;
            };
            let Ok(asset) = Asset::try_from(symbol) else {
                continue;
            };
            bars_by_asset.insert(
                asset,
                parse_daily_bars(&payload.value, payload.observed_at)?,
            );
        }
        outcome_comparison(
            &target,
            &baseline_prices,
            &bars_by_asset,
            schedule.baseline_trading_day,
        )
        .map_err(DaemonError::Unavailable)
    }

    fn observer_learning(
        &self,
        observed_at: DateTime<Utc>,
    ) -> Result<ObserverSection<ObserverLearning>> {
        let mut artifacts = Vec::new();
        let mut seen = BTreeSet::new();
        for kind in [
            ArtifactKind::OutcomeSchedule,
            ArtifactKind::Outcome,
            ArtifactKind::Retrospective,
            ArtifactKind::Experience,
            ArtifactKind::Evaluation,
        ] {
            for artifact in self
                .store
                .recent_artifacts_by_kind(kind, OBSERVER_LEARNING_LIMIT)?
            {
                if seen.insert(artifact.artifact_id.clone()) {
                    if let Some(view) = self.observer_artifact_view(&artifact)? {
                        artifacts.push(view);
                    }
                }
            }
        }
        artifacts.sort_by_key(|artifact| artifact.created_at);
        if artifacts.len() > OBSERVER_LEARNING_LIMIT {
            artifacts.drain(..artifacts.len() - OBSERVER_LEARNING_LIMIT);
        }
        let mut subjects = Vec::new();
        for artifact in &artifacts {
            if artifact.kind == ArtifactKind::Experience {
                let experience: Experience = serde_json::from_value(artifact.payload.clone())?;
                if !subjects.contains(&experience.subject) {
                    subjects.push(experience.subject);
                }
            }
        }
        let mut policy_transitions = Vec::new();
        for subject in subjects {
            policy_transitions.extend(self.store.policy_transitions(&subject)?.into_iter().map(
                |record| ObserverPolicyTransition {
                    transition: record.transition,
                    run_id: record.run_id,
                    revision: record.revision,
                    transition_cursor: record.transition_cursor,
                },
            ));
        }
        policy_transitions.sort_by_key(|record| record.transition_cursor);
        let (summary, policy_metrics) =
            self.observer_learning_analytics(observed_at, &policy_transitions)?;
        if artifacts.is_empty() && policy_transitions.is_empty() {
            return Ok(ObserverSection::pending(
                "No canonical learning artifacts are available yet",
            ));
        }
        Ok(ObserverSection::available(
            observed_at,
            ObserverLearning {
                artifacts,
                policy_transitions,
                summary,
                policy_metrics,
            },
        ))
    }

    fn observer_learning_analytics(
        &self,
        now: DateTime<Utc>,
        visible_transitions: &[ObserverPolicyTransition],
    ) -> Result<(ObserverLearningSummary, Vec<ObserverPolicyMetrics>)> {
        let outcomes = self.recent_typed_artifacts::<Outcome>(ArtifactKind::Outcome, 100)?;
        let retrospectives =
            self.recent_typed_artifacts::<Retrospective>(ArtifactKind::Retrospective, 200)?;
        let experiences =
            self.recent_typed_artifacts::<Experience>(ArtifactKind::Experience, 200)?;
        let evaluations =
            self.recent_typed_artifacts::<Evaluation>(ArtifactKind::Evaluation, 200)?;
        let current_start = now - Duration::days(30);
        let previous_start = now - Duration::days(60);

        let outcome_by_id = outcomes
            .iter()
            .map(|(artifact, outcome)| (artifact.artifact_id.clone(), outcome.clone()))
            .collect::<BTreeMap<_, _>>();
        let current_outcomes = outcomes
            .iter()
            .filter(|(_, outcome)| {
                outcome
                    .sealed_at
                    .is_some_and(|sealed| sealed >= current_start)
            })
            .collect::<Vec<_>>();
        let current_utilities = current_outcomes
            .iter()
            .map(|(_, outcome)| outcome_average_utility(outcome))
            .collect::<Vec<_>>();
        let attributed_values = current_outcomes
            .iter()
            .filter_map(|(_, outcome)| {
                self.observer_outcome_baseline_equity(outcome)
                    .ok()
                    .and_then(|equity| {
                        i64::try_from(
                            i128::from(equity)
                                .saturating_mul(i128::from(outcome_average_utility(outcome)))
                                / 1_000_000,
                        )
                        .ok()
                    })
            })
            .collect::<Vec<_>>();
        let attributed_utility_micros = (!attributed_values.is_empty()).then(|| {
            attributed_values
                .iter()
                .fold(0_i64, |total, value| total.saturating_add(*value))
        });

        let lesson_count = |start: DateTime<Utc>, end: DateTime<Utc>| {
            retrospectives
                .iter()
                .filter(|(artifact, _)| artifact.created_at >= start && artifact.created_at < end)
                .map(|(_, retrospective)| retrospective.lesson_candidates.len())
                .sum::<usize>()
        };
        let lesson_candidates = lesson_count(current_start, now);
        let previous_lessons = lesson_count(previous_start, current_start);

        let mut all_transitions = visible_transitions
            .iter()
            .map(|record| record.transition.clone())
            .collect::<Vec<_>>();
        let mut transition_ids = all_transitions
            .iter()
            .map(|transition| transition.transition_id.0.clone())
            .collect::<BTreeSet<_>>();
        let mut subjects = experiences
            .iter()
            .map(|(_, experience)| experience.subject.clone())
            .collect::<Vec<_>>();
        subjects.sort();
        subjects.dedup();
        for subject in &subjects {
            for record in self.store.policy_transitions(subject)? {
                if transition_ids.insert(record.transition.transition_id.0.clone()) {
                    all_transitions.push(record.transition);
                }
            }
        }
        all_transitions.sort_by_key(|transition| transition.created_at);
        let transition_count = |start: DateTime<Utc>, end: DateTime<Utc>| {
            all_transitions
                .iter()
                .filter(|transition| transition.created_at >= start && transition.created_at < end)
                .count()
        };
        let policies_evolved = transition_count(current_start, now);
        let previous_policies = transition_count(previous_start, current_start);

        let utility_by_outcome = outcomes
            .iter()
            .map(|(artifact, outcome)| {
                (
                    artifact.artifact_id.clone(),
                    outcome_average_utility(outcome),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut impact_areas = Vec::<(RetrospectiveCategory, i128)>::new();
        for (artifact, retrospective) in &retrospectives {
            if artifact.created_at < current_start || artifact.created_at >= now {
                continue;
            }
            let Some(utility) = utility_by_outcome.get(&retrospective.outcome.artifact_id) else {
                continue;
            };
            let total_confidence = retrospective
                .findings
                .iter()
                .map(|finding| u64::from(finding.confidence_ppm))
                .sum::<u64>();
            if total_confidence == 0 {
                continue;
            }
            for finding in &retrospective.findings {
                let attributed = i128::from(*utility)
                    .saturating_mul(i128::from(finding.confidence_ppm))
                    / i128::from(total_confidence);
                if let Some((_, value)) = impact_areas
                    .iter_mut()
                    .find(|(category, _)| *category == finding.category)
                {
                    *value = value.saturating_add(attributed);
                } else {
                    impact_areas.push((finding.category, attributed));
                }
            }
        }
        impact_areas.sort_by_key(|(_, value)| std::cmp::Reverse(value.abs()));
        let impact_areas = impact_areas
            .into_iter()
            .filter_map(|(category, impact)| {
                i64::try_from(impact)
                    .ok()
                    .map(|impact_ppm| ObserverImpactArea {
                        category,
                        impact_ppm,
                    })
            })
            .collect();

        let experience_by_id = experiences
            .iter()
            .map(|(artifact, experience)| (artifact.artifact_id.clone(), experience.clone()))
            .collect::<BTreeMap<_, _>>();
        let policy = EvaluationPolicy::default();
        let mut grouped =
            Vec::<(PolicySubject, PolicyState, Vec<(DateTime<Utc>, i64, bool)>)>::new();
        for (artifact, evaluation) in &evaluations {
            let Some(experience) = experience_by_id.get(&evaluation.experience.artifact_id) else {
                continue;
            };
            let Some(outcome) = outcome_by_id.get(&evaluation.outcome.artifact_id) else {
                continue;
            };
            let degraded = outcome.windows.iter().any(|window| {
                window.evidence_completeness_ppm < policy.minimum_evidence_completeness_ppm
                    || window
                        .risk_recall_ppm
                        .is_some_and(|value| value < policy.minimum_risk_recall_ppm)
            });
            if let Some((_, state, values)) = grouped
                .iter_mut()
                .find(|(subject, _, _)| *subject == experience.subject)
            {
                *state = experience.policy_state;
                values.push((
                    artifact.created_at,
                    evaluation.marginal_utility_ppm,
                    degraded,
                ));
            } else {
                grouped.push((
                    experience.subject.clone(),
                    experience.policy_state,
                    vec![(
                        artifact.created_at,
                        evaluation.marginal_utility_ppm,
                        degraded,
                    )],
                ));
            }
        }
        let policy_metrics = grouped
            .into_iter()
            .map(
                |(subject, mut state, mut values)| -> Result<ObserverPolicyMetrics> {
                    if let Some(latest) = self.store.policy_transitions(&subject)?.last() {
                        state = latest.transition.to;
                    }
                    values.sort_by_key(|(created_at, _, _)| std::cmp::Reverse(*created_at));
                    values.truncate(20);
                    let utilities = values
                        .iter()
                        .map(|(_, utility, _)| *utility)
                        .collect::<Vec<_>>();
                    let win_rate_ppm = (!values.is_empty()).then(|| {
                        i64::try_from(
                            values.iter().filter(|(_, utility, _)| *utility > 0).count()
                                * 1_000_000
                                / values.len(),
                        )
                        .unwrap_or(1_000_000)
                    });
                    let stability_ppm = (!values.is_empty()).then(|| {
                        i64::try_from(
                            values.iter().filter(|(_, _, degraded)| !*degraded).count() * 1_000_000
                                / values.len(),
                        )
                        .unwrap_or(1_000_000)
                    });
                    Ok(ObserverPolicyMetrics {
                        subject,
                        state,
                        sample_count: values.len(),
                        win_rate_ppm,
                        net_impact_ppm: compounded_ppm(&utilities),
                        stability_ppm,
                        exposure_ppm: policy_exposure_ppm(state),
                    })
                },
            )
            .collect::<Result<Vec<_>>>()?;

        Ok((
            ObserverLearningSummary {
                range_days: 30,
                attributed_utility_micros,
                attributed_utility_ppm: compounded_ppm(&current_utilities),
                lesson_candidates,
                lesson_candidates_delta: i64::try_from(lesson_candidates).unwrap_or(i64::MAX)
                    - i64::try_from(previous_lessons).unwrap_or(i64::MAX),
                policies_evolved,
                policies_evolved_delta: i64::try_from(policies_evolved).unwrap_or(i64::MAX)
                    - i64::try_from(previous_policies).unwrap_or(i64::MAX),
                impact_areas,
            },
            policy_metrics,
        ))
    }

    fn observer_outcome_baseline_equity(&self, outcome: &Outcome) -> Result<i64> {
        let schedule_artifact = self.store.artifact(&outcome.schedule.artifact_id)?;
        let schedule: OutcomeSchedule =
            serde_json::from_slice(&self.store.read_blob(&schedule_artifact.blob)?)?;
        let context_artifact = self
            .store
            .artifact(&schedule.execution_context.artifact_id)?;
        let context: ExecutionContext =
            serde_json::from_slice(&self.store.read_blob(&context_artifact.blob)?)?;
        let account_reference = context.account_snapshot.as_ref().ok_or_else(|| {
            DaemonError::Unavailable("Outcome has no baseline AccountSnapshot".to_owned())
        })?;
        let account_artifact = self.store.artifact(&account_reference.artifact_id)?;
        let account: AccountSnapshot =
            serde_json::from_slice(&self.store.read_blob(&account_artifact.blob)?)?;
        Ok(account.equity.0)
    }

    fn recent_typed_artifacts<T>(
        &self,
        kind: ArtifactKind,
        limit: usize,
    ) -> Result<Vec<(Artifact, T)>>
    where
        T: DeserializeOwned,
    {
        self.store
            .recent_artifacts_by_kind(kind, limit)?
            .into_iter()
            .map(|artifact| {
                let payload = serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                Ok((artifact, payload))
            })
            .collect()
    }

    fn observer_artifacts(
        &self,
        trajectory: &[TrajectoryEntry],
        include: impl Fn(ArtifactKind) -> bool,
    ) -> Result<Vec<ObserverArtifactView>> {
        let mut seen = BTreeSet::new();
        let mut artifacts = Vec::new();
        for entry in trajectory {
            let (Some(artifact_id), Some(kind)) = (&entry.artifact_id, entry.artifact_kind) else {
                continue;
            };
            if !include(kind) || !seen.insert(artifact_id.clone()) {
                continue;
            }
            let artifact = self.store.artifact(artifact_id)?;
            if let Some(view) = self.observer_artifact_view(&artifact)? {
                artifacts.push(view);
            }
        }
        artifacts.sort_by_key(|artifact| artifact.created_at);
        Ok(artifacts)
    }

    fn observer_artifact_view(&self, artifact: &Artifact) -> Result<Option<ObserverArtifactView>> {
        let payload = match artifact.kind {
            ArtifactKind::WorkflowProposalDraft => {
                self.typed_observer_payload::<WorkflowProposalDraft>(artifact)?
            }
            ArtifactKind::Claim => self.typed_observer_payload::<ResearchClaim>(artifact)?,
            ArtifactKind::Critique => self.typed_observer_payload::<ResearchCritique>(artifact)?,
            ArtifactKind::DecisionProposal => {
                self.typed_observer_payload::<DecisionProposal>(artifact)?
            }
            ArtifactKind::DecisionContext => {
                self.typed_observer_payload::<DecisionContext>(artifact)?
            }
            ArtifactKind::Decision => self.typed_observer_payload::<Decision>(artifact)?,
            ArtifactKind::ExecutionContext => {
                self.typed_observer_payload::<ExecutionContext>(artifact)?
            }
            ArtifactKind::ExecutionVerdict => {
                self.typed_observer_payload::<ExecutionVerdict>(artifact)?
            }
            ArtifactKind::ExecutionPlan => {
                self.typed_observer_payload::<ExecutionPlan>(artifact)?
            }
            ArtifactKind::OrderReceipt => self.typed_observer_payload::<OrderReceipt>(artifact)?,
            ArtifactKind::Reconciliation => {
                self.typed_observer_payload::<Reconciliation>(artifact)?
            }
            ArtifactKind::OutcomeSchedule => {
                self.typed_observer_payload::<OutcomeSchedule>(artifact)?
            }
            ArtifactKind::Outcome => self.typed_observer_payload::<Outcome>(artifact)?,
            ArtifactKind::RetrospectiveDraft => {
                self.typed_observer_payload::<RetrospectiveDraft>(artifact)?
            }
            ArtifactKind::Retrospective => {
                self.typed_observer_payload::<Retrospective>(artifact)?
            }
            ArtifactKind::Experience => self.typed_observer_payload::<Experience>(artifact)?,
            ArtifactKind::Evaluation => self.typed_observer_payload::<Evaluation>(artifact)?,
            _ => return Ok(None),
        };
        Ok(Some(ObserverArtifactView {
            artifact_id: artifact.artifact_id.clone(),
            kind: artifact.kind,
            created_at: artifact.created_at,
            payload,
        }))
    }

    fn typed_observer_payload<T>(&self, artifact: &Artifact) -> Result<Value>
    where
        T: DeserializeOwned + Serialize,
    {
        Ok(serde_json::to_value(serde_json::from_slice::<T>(
            &self.store.read_blob(&artifact.blob)?,
        )?)?)
    }

    fn observer_approval(&self, now: DateTime<Utc>) -> Result<ObserverApprovalStatus> {
        let Some(artifact) = self
            .store
            .latest_artifact_by_kind(ArtifactKind::PaperLaunchApproval)?
        else {
            return Ok(ObserverApprovalStatus {
                status: "missing".to_owned(),
                operator_identity: None,
                reason: None,
                expires_at: None,
            });
        };
        let approval: PaperLaunchApproval =
            serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
        let manifest_artifact = self
            .store
            .artifact(&approval.runtime_manifest.artifact_id)?;
        let manifest: RuntimeManifest =
            serde_json::from_slice(&self.store.read_blob(&manifest_artifact.blob)?)?;
        let manifest_identity_hash = manifest.runtime_identity_hash()?;
        let status = if approval.expires_at < now {
            "expired"
        } else if self
            .runtime_identity_hash
            .as_ref()
            .is_some_and(|expected| expected != &manifest_identity_hash)
        {
            "mismatched"
        } else {
            "valid"
        };
        Ok(ObserverApprovalStatus {
            status: status.to_owned(),
            operator_identity: Some(approval.operator_identity),
            reason: Some(approval.reason),
            expires_at: Some(approval.expires_at),
        })
    }

    async fn observer_portfolio(
        &self,
        observed_at: DateTime<Utc>,
        current_run: Option<&ObserverRunDetail>,
    ) -> ObserverSection<ObserverPortfolio> {
        let Some(paper) = self.paper_observer.as_ref() else {
            return ObserverSection::unavailable("Alpaca Paper observer is not configured");
        };
        let current = tokio::time::timeout(OBSERVER_BROKER_TIMEOUT, async {
            tokio::try_join!(paper.account(), paper.positions(), paper.market_clock())
        })
        .await;
        let (account, positions, clock) = match current {
            Ok(Ok(values)) => values,
            Ok(Err(error)) => return ObserverSection::unavailable(error.to_string()),
            Err(_) => {
                return ObserverSection::unavailable("Alpaca Paper account snapshot timed out");
            }
        };
        let mut portfolio = match parse_portfolio(
            &account,
            &positions,
            &clock.session_date.to_string(),
            clock.is_open,
        ) {
            Ok(portfolio) => portfolio,
            Err(error) => return ObserverSection::unavailable(error.to_string()),
        };

        if let Some(run) = current_run {
            if let Ok(sparklines) = self.observer_position_sparklines(run) {
                for position in &mut portfolio.positions {
                    position.sparkline_ppm = sparklines
                        .get(&position.symbol.to_ascii_uppercase())
                        .cloned()
                        .unwrap_or_default();
                }
            }
        }

        let broker_session = clock.session_date.to_string();
        let optional = tokio::time::timeout(OBSERVER_BROKER_TIMEOUT, async {
            tokio::join!(
                paper.portfolio_history(PortfolioHistoryRange::ThreeMonths),
                self.observer_qqq_bars(ObserverPortfolioRange::ThreeMonths, observed_at),
                self.observer_fill_activities(&broker_session)
            )
        })
        .await;
        if let Ok((history_result, bars_result, fills_result)) = optional {
            portfolio.analytics = match (history_result, bars_result) {
                (Ok(history), Ok(bars)) => {
                    parse_portfolio_history(ObserverPortfolioRange::ThreeMonths, &history)
                        .and_then(|history| {
                            portfolio_analytics(
                                &history
                                    .points
                                    .iter()
                                    .map(|point| (point.timestamp, point.equity_micros))
                                    .collect::<Vec<_>>(),
                                &bars,
                                portfolio.equity_micros,
                            )
                            .map_err(DaemonError::Unavailable)
                        })
                        .map(|analytics| ObserverSection::available(observed_at, analytics))
                        .unwrap_or_else(|error| ObserverSection::unavailable(error.to_string()))
                }
                (Err(error), _) => ObserverSection::unavailable(error.to_string()),
                (_, Err(error)) => ObserverSection::unavailable(error.to_string()),
            };

            portfolio.fills = match fills_result {
                Ok(value) => {
                    let order_ids = current_run
                        .map(observer_broker_order_ids)
                        .unwrap_or_default();
                    match parse_fill_activities(&value, &order_ids) {
                        Ok(fills) => {
                            if let Some(run) = current_run {
                                if let (Some(opening_positions), Some(opening_equity)) = (
                                    self.observer_normalized_resource(
                                        run,
                                        PAPER_POSITIONS_RESOURCE,
                                    ),
                                    self.observer_normalized_resource(run, PAPER_ACCOUNT_RESOURCE)
                                        .and_then(|account| {
                                            account
                                                .get("equity")
                                                .and_then(parse_money_micros)
                                                .map(|value| value.0)
                                        }),
                                ) {
                                    if let Ok(realized) =
                                        managed_realized_pnl(&opening_positions, &fills)
                                    {
                                        portfolio.realized_pnl_micros = Some(realized);
                                        portfolio.realized_pnl_ppm = (opening_equity != 0)
                                            .then(|| {
                                                i128::from(realized) * 1_000_000
                                                    / i128::from(opening_equity)
                                            })
                                            .and_then(|value| i64::try_from(value).ok());
                                    }
                                }
                            }
                            ObserverSection::available(observed_at, fills)
                        }
                        Err(error) => ObserverSection::unavailable(error),
                    }
                }
                Err(error) => ObserverSection::unavailable(error.to_string()),
            };
        } else {
            portfolio.analytics = ObserverSection::unavailable("Portfolio analytics timed out");
            portfolio.fills = ObserverSection::unavailable("Alpaca fill activities timed out");
        }
        ObserverSection::available(observed_at, portfolio)
    }

    async fn observer_fill_activities(&self, broker_session: &str) -> Result<Value> {
        let adapter = self
            .production_evidence
            .get(&EvidenceSource::Alpaca)
            .ok_or_else(|| {
                DaemonError::Unavailable("Alpaca evidence is not configured".to_owned())
            })?;
        let acquired = adapter
            .acquire(&EvidenceRequest {
                source: EvidenceSource::Alpaca,
                resource: format!("paper.fills:{broker_session}"),
                max_age: Duration::days(1),
            })
            .await
            .map_err(|error| DaemonError::Unavailable(error.to_string()))?;
        Ok(serde_json::from_slice(&acquired.raw)?)
    }

    fn observer_normalized_resource(
        &self,
        run: &ObserverRunDetail,
        resource: &str,
    ) -> Option<Value> {
        self.store
            .recent_artifacts_by_kind(ArtifactKind::NormalizedEvidence, 500)
            .ok()?
            .into_iter()
            .find_map(|artifact| {
                if artifact.origin.as_ref()?.run_id.as_ref()? != &run.workflow.run.run_id {
                    return None;
                }
                let payload: NormalizedEvidencePayload =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob).ok()?).ok()?;
                (payload.resource == resource).then_some(payload.value)
            })
    }

    fn observer_position_sparklines(
        &self,
        run: &ObserverRunDetail,
    ) -> Result<BTreeMap<String, Vec<i64>>> {
        let mut sparklines = BTreeMap::new();
        for artifact in self
            .store
            .recent_artifacts_by_kind(ArtifactKind::NormalizedEvidence, 500)?
        {
            if artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(&run.workflow.run.run_id)
            {
                continue;
            }
            let payload: NormalizedEvidencePayload =
                serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
            let mut parts = payload.resource.split(':');
            if parts.next() != Some("bars") {
                continue;
            }
            let Some(symbol) = parts.next() else {
                continue;
            };
            let bars = parse_daily_bars(&payload.value, payload.observed_at)?;
            let Some(opening) = bars.values().next().filter(|price| price.0 > 0) else {
                continue;
            };
            let values = bars
                .values()
                .filter_map(|price| {
                    i64::try_from(i128::from(price.0) * 1_000_000 / i128::from(opening.0)).ok()
                })
                .collect::<Vec<_>>();
            sparklines.insert(symbol.to_ascii_uppercase(), values);
        }
        Ok(sparklines)
    }
}

fn observer_run_telemetry(trajectory: &[TrajectoryEntry]) -> ObserverRunTelemetry {
    ObserverRunTelemetry {
        model_id: trajectory
            .iter()
            .rev()
            .find_map(|entry| entry.model.as_ref()?.model_id.clone()),
        latency_millis: trajectory
            .iter()
            .rev()
            .find_map(|entry| entry.latency_millis),
        input_tokens: trajectory
            .iter()
            .filter_map(|entry| entry.input_tokens)
            .try_fold(0_u64, u64::checked_add),
        output_tokens: trajectory
            .iter()
            .filter_map(|entry| entry.output_tokens)
            .try_fold(0_u64, u64::checked_add),
        tool_calls: trajectory
            .iter()
            .filter(|entry| entry.tool.is_some() && entry.event_type.contains("called"))
            .count(),
        turns: trajectory
            .iter()
            .filter(|entry| entry.turn.is_some() && entry.model.is_some())
            .count(),
    }
}

fn observer_broker_order_ids(run: &ObserverRunDetail) -> BTreeSet<String> {
    run.artifacts
        .iter()
        .filter(|artifact| artifact.kind == ArtifactKind::OrderReceipt)
        .filter_map(|artifact| {
            artifact
                .payload
                .get("broker_order_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .collect()
}

fn parse_portfolio(
    account: &Value,
    positions: &Value,
    broker_session: &str,
    market_open: bool,
) -> Result<ObserverPortfolio> {
    let equity = provider_money(account, "equity")?.0;
    let buying_power = provider_money(account, "buying_power")?.0;
    let last_equity = account
        .get("last_equity")
        .and_then(parse_money_micros)
        .map(|value| value.0);
    let day_pnl = last_equity.and_then(|previous| equity.checked_sub(previous));
    let day_pnl_ppm = day_pnl.zip(last_equity).and_then(|(pnl, previous)| {
        (previous != 0)
            .then(|| i128::from(pnl) * 1_000_000 / i128::from(previous))
            .and_then(|value| i64::try_from(value).ok())
    });
    let positions = positions
        .as_array()
        .ok_or_else(|| {
            DaemonError::InvalidInput("Paper positions payload is not an array".to_owned())
        })?
        .iter()
        .map(|position| {
            let symbol = position
                .get("symbol")
                .and_then(Value::as_str)
                .filter(|symbol| !symbol.trim().is_empty())
                .ok_or_else(|| {
                    DaemonError::InvalidInput("Paper position symbol missing".to_owned())
                })?;
            Ok(ObserverPosition {
                symbol: symbol.to_owned(),
                quantity_micros: observer_number_micros(position, "qty")?,
                market_value_micros: provider_money(position, "market_value")?.0,
                average_entry_price_micros: observer_optional_micros(position, "avg_entry_price"),
                unrealized_pnl_micros: observer_optional_micros(position, "unrealized_pl"),
                unrealized_pnl_ppm: observer_optional_micros(position, "unrealized_plpc"),
                sparkline_ppm: Vec::new(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ObserverPortfolio {
        broker_session: broker_session.to_owned(),
        market_open,
        status: account
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned(),
        equity_micros: equity,
        last_equity_micros: last_equity,
        buying_power_micros: buying_power,
        day_pnl_micros: day_pnl,
        day_pnl_ppm,
        realized_pnl_micros: None,
        realized_pnl_ppm: None,
        fills: ObserverSection::pending("No managed fill projection was requested"),
        analytics: ObserverSection::pending("Portfolio analytics are loading"),
        positions,
    })
}

fn parse_portfolio_history(
    range: ObserverPortfolioRange,
    value: &Value,
) -> Result<ObserverPortfolioHistory> {
    let timestamps = value
        .get("timestamp")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            DaemonError::InvalidInput("Portfolio history timestamps missing".to_owned())
        })?;
    let equity = value
        .get("equity")
        .and_then(Value::as_array)
        .ok_or_else(|| DaemonError::InvalidInput("Portfolio history equity missing".to_owned()))?;
    let profit_loss = value.get("profit_loss").and_then(Value::as_array);
    let profit_loss_pct = value.get("profit_loss_pct").and_then(Value::as_array);
    let mut points = Vec::with_capacity(timestamps.len().min(equity.len()));
    for index in 0..timestamps.len().min(equity.len()) {
        let timestamp = timestamps[index]
            .as_i64()
            .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
            .ok_or_else(|| {
                DaemonError::InvalidInput("Portfolio history timestamp invalid".to_owned())
            })?;
        let equity_micros = parse_money_micros(&equity[index])
            .ok_or_else(|| {
                DaemonError::InvalidInput("Portfolio history equity invalid".to_owned())
            })?
            .0;
        points.push(ObserverPortfolioHistoryPoint {
            timestamp,
            equity_micros,
            profit_loss_micros: profit_loss
                .and_then(|values| values.get(index))
                .and_then(parse_money_micros)
                .map(|value| value.0),
            profit_loss_ppm: profit_loss_pct
                .and_then(|values| values.get(index))
                .and_then(parse_money_micros)
                .map(|value| value.0),
            benchmark_equity_micros: None,
        });
    }
    if points.is_empty() {
        return Err(DaemonError::Unavailable(
            "Portfolio history returned no observations".to_owned(),
        ));
    }
    Ok(ObserverPortfolioHistory {
        range,
        benchmark_symbol: "QQQ",
        points,
    })
}

fn outcome_average_utility(outcome: &Outcome) -> i64 {
    if outcome.windows.is_empty() {
        return 0;
    }
    let total = outcome.windows.iter().fold(0_i128, |sum, window| {
        sum.saturating_add(i128::from(window.utility_ppm))
    });
    i64::try_from(total / outcome.windows.len() as i128).unwrap_or_else(|_| {
        if total.is_negative() {
            i64::MIN
        } else {
            i64::MAX
        }
    })
}

fn observer_number_micros(value: &Value, field: &str) -> Result<i64> {
    value
        .get(field)
        .and_then(parse_money_micros)
        .map(|value| value.0)
        .ok_or_else(|| DaemonError::InvalidInput(format!("Paper provider field {field} invalid")))
}

fn observer_optional_micros(value: &Value, field: &str) -> Option<i64> {
    value
        .get(field)
        .and_then(parse_money_micros)
        .map(|value| value.0)
}
