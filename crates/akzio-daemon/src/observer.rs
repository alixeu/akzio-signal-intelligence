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
const OBSERVER_BROKER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

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

#[path = "observer_parts/broker.rs"]
mod broker;
#[path = "observer_parts/helpers.rs"]
mod helpers;
#[path = "observer_parts/learning.rs"]
mod learning;
#[path = "observer_parts/runs.rs"]
mod runs;
#[path = "observer_parts/snapshot.rs"]
mod snapshot;

use helpers::*;
