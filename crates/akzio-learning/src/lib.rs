//! Outcome-driven memory and topology policy.
//!
//! Learning is deliberately advisory.  It can make a source-linked prior
//! available to research, but it cannot alter target weights, execution plans,
//! tool permissions, or an active topology directly.

pub mod rebuild;
mod topology;

pub use rebuild::{
    EvaluationInput, EvaluationPolicy, EvaluationResult, PolicySubject, RebuildEvaluationError,
    RebuildEvaluationResult, RebuildEvaluationRuntime, ShadowObservation,
};
pub use topology::{
    advance_topology, ShadowPair, TopologyLedger, TopologyMetrics, TopologyOutcome, TopologyRecord,
    TopologyState,
};

use std::collections::{BTreeMap, BTreeSet};

use akzio_context::legacy::{ContextBroker, ContextError, NewJsonDocument};
use akzio_domain::{
    Asset, DocumentId, DocumentKind, DocumentLifecycle, DocumentOrigin, DocumentRecord, MemoryId,
    MoneyMicros, PortfolioDecision, Provenance, RunId, RunPurpose, TargetPortfolio,
    V2_SCHEMA_VERSION,
};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub(crate) fn baseline_prices(value: &serde_json::Value) -> Result<BTreeMap<Asset, MoneyMicros>> {
    let quotes = value
        .get("quotes")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| LedgerError::InvalidOutcome("execution context has no quotes".to_owned()))?;
    Asset::EXECUTABLE
        .into_iter()
        .map(|asset| {
            let quote = quotes.get(asset.symbol()).ok_or_else(|| {
                LedgerError::InvalidOutcome(format!(
                    "execution context lacks {} quote",
                    asset.symbol()
                ))
            })?;
            let bid = money_json(quote.get("bid"))?;
            let ask = money_json(quote.get("ask"))?;
            if bid.0 <= 0 || ask.0 <= 0 || ask.0 < bid.0 {
                return Err(LedgerError::InvalidOutcome(format!(
                    "invalid {} baseline quote",
                    asset.symbol()
                )));
            }
            Ok((asset, MoneyMicros((bid.0 + ask.0) / 2)))
        })
        .collect()
}

fn money_json(value: Option<&serde_json::Value>) -> Result<MoneyMicros> {
    let value =
        value.ok_or_else(|| LedgerError::InvalidOutcome("missing money value".to_owned()))?;
    value
        .as_i64()
        .map(MoneyMicros)
        .or_else(|| {
            value
                .as_str()
                .and_then(|value| value.parse::<i64>().ok())
                .map(MoneyMicros)
        })
        .ok_or_else(|| LedgerError::InvalidOutcome("money value is not integer micros".to_owned()))
}

fn materialize(
    schedule_document_id: &DocumentId,
    schedule: &OutcomeSchedule,
    market: &OutcomeMarket,
) -> Result<Option<MaterializedOutcome>> {
    let mut future_prices = BTreeMap::new();
    let mut outcome_day = None;
    for asset in Asset::EXECUTABLE {
        if schedule
            .targets
            .weights
            .get(&asset)
            .copied()
            .unwrap_or(akzio_domain::WeightPpm::ZERO)
            .0
            == 0
            && asset != Asset::Qqq
        {
            continue;
        }
        let Some(close) = close_after(
            market
                .closes
                .get(&asset)
                .map(Vec::as_slice)
                .unwrap_or_default(),
            schedule.baseline_day,
            schedule.horizon_trading_days,
        ) else {
            return Ok(None);
        };
        outcome_day = Some(outcome_day.map_or(close.trading_day, |day: NaiveDate| {
            day.max(close.trading_day)
        }));
        future_prices.insert(asset, close.close);
    }
    let Some(trading_day) = outcome_day else {
        return Ok(None);
    };
    let portfolio_return_ppm =
        portfolio_return_ppm(&schedule.targets, &schedule.baseline_prices, &future_prices)?;
    let benchmark_return_ppm = return_ppm(
        *schedule
            .baseline_prices
            .get(&Asset::Qqq)
            .ok_or_else(|| LedgerError::InvalidOutcome("QQQ baseline absent".to_owned()))?,
        *future_prices
            .get(&Asset::Qqq)
            .ok_or_else(|| LedgerError::InvalidOutcome("QQQ close absent".to_owned()))?,
    )?;
    let utility_ppm = portfolio_return_ppm.saturating_sub(benchmark_return_ppm);
    let sample = OutcomeSample {
        run_id: schedule.decision_run_id.clone(),
        trading_day,
        regime: schedule.regime.clone(),
        utility_micros: utility_ppm,
        harmful: utility_ppm < 0,
        hard_risk_miss: portfolio_return_ppm <= -100_000,
    };
    Ok(Some(MaterializedOutcome {
        schema_version: V2_SCHEMA_VERSION,
        schedule_document_id: schedule_document_id.clone(),
        memory_id: schedule.memory_id.clone(),
        horizon_trading_days: schedule.horizon_trading_days,
        trading_day,
        portfolio_return_ppm,
        benchmark_return_ppm,
        utility_ppm,
        sample,
    }))
}

pub(crate) fn close_after(
    closes: &[DailyClose],
    baseline_day: NaiveDate,
    horizon: u8,
) -> Option<DailyClose> {
    let mut closes = closes
        .iter()
        .filter(|close| close.trading_day > baseline_day)
        .cloned()
        .collect::<Vec<_>>();
    closes.sort_by_key(|close| close.trading_day);
    closes.get(usize::from(horizon.saturating_sub(1))).cloned()
}

pub(crate) fn portfolio_return_ppm(
    targets: &TargetPortfolio,
    baseline: &BTreeMap<Asset, MoneyMicros>,
    future: &BTreeMap<Asset, MoneyMicros>,
) -> Result<i64> {
    targets
        .weights
        .iter()
        .try_fold(0_i128, |sum, (asset, weight)| {
            if weight.0 == 0 {
                return Ok(sum);
            }
            let base = *baseline.get(asset).ok_or_else(|| {
                LedgerError::InvalidOutcome(format!("{} baseline absent", asset.symbol()))
            })?;
            let close = *future.get(asset).ok_or_else(|| {
                LedgerError::InvalidOutcome(format!("{} close absent", asset.symbol()))
            })?;
            Ok(sum + i128::from(return_ppm(base, close)?) * i128::from(weight.0) / 1_000_000)
        })
        .and_then(|value| {
            i64::try_from(value)
                .map_err(|_| LedgerError::InvalidOutcome("portfolio return overflow".to_owned()))
        })
}

pub(crate) fn return_ppm(baseline: MoneyMicros, close: MoneyMicros) -> Result<i64> {
    if baseline.0 <= 0 || close.0 <= 0 {
        return Err(LedgerError::InvalidOutcome(
            "prices must be positive".to_owned(),
        ));
    }
    i64::try_from(
        (i128::from(close.0) - i128::from(baseline.0)) * 1_000_000 / i128::from(baseline.0),
    )
    .map_err(|_| LedgerError::InvalidOutcome("return overflow".to_owned()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryState {
    Candidate,
    Active,
    Proven,
    Contested,
    Retired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeSample {
    pub run_id: RunId,
    pub trading_day: NaiveDate,
    pub regime: String,
    pub utility_micros: i64,
    pub harmful: bool,
    pub hard_risk_miss: bool,
}

/// A single daily close supplied by a sealed future market input. The
/// materializer counts trading sessions, not calendar days.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DailyClose {
    pub trading_day: NaiveDate,
    pub close: MoneyMicros,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeMarket {
    pub closes: BTreeMap<Asset, Vec<DailyClose>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeSchedule {
    pub schema_version: u32,
    pub memory_id: MemoryId,
    pub decision_run_id: RunId,
    pub decision_document_id: DocumentId,
    pub execution_context_id: DocumentId,
    pub horizon_trading_days: u8,
    pub baseline_day: NaiveDate,
    pub baseline_prices: BTreeMap<Asset, MoneyMicros>,
    pub targets: TargetPortfolio,
    pub regime: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializedOutcome {
    pub schema_version: u32,
    pub schedule_document_id: DocumentId,
    pub memory_id: MemoryId,
    pub horizon_trading_days: u8,
    pub trading_day: NaiveDate,
    pub portfolio_return_ppm: i64,
    pub benchmark_return_ppm: i64,
    pub utility_ppm: i64,
    pub sample: OutcomeSample,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryItem {
    pub memory_id: MemoryId,
    pub state: MemoryState,
    pub samples: Vec<OutcomeSample>,
}

impl MemoryItem {
    pub fn new(memory_id: MemoryId) -> Self {
        Self {
            memory_id,
            state: MemoryState::Candidate,
            samples: Vec::new(),
        }
    }

    pub fn observe(&mut self, sample: OutcomeSample) {
        if self.state == MemoryState::Retired
            || self.samples.iter().any(|item| item.run_id == sample.run_id)
        {
            return;
        }
        self.samples.push(sample);
        self.state = derive_memory_state(&self.samples);
    }

    pub fn retire(&mut self) {
        self.state = MemoryState::Retired;
    }
}

/// Minimal, immutable input surfaced to a subsequent research run.  The prior
/// expresses a learned hypothesis; a model still has to produce a fresh draft,
/// and Rust still has to accept it through Decision and Execution gates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryPrior {
    pub memory_id: MemoryId,
    pub state: MemoryState,
    pub summary: String,
    pub evidence_refs: Vec<DocumentId>,
    pub outcome_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMemory {
    pub item: MemoryItem,
    pub summary: String,
}

#[derive(Debug, Error)]
pub enum LedgerError {
    #[error(transparent)]
    Context(#[from] ContextError),
    #[error(transparent)]
    Store(#[from] akzio_store::legacy::StoreError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("memory {0} does not exist")]
    MissingMemory(MemoryId),
    #[error("memory updates require a canonical Paper run, not {0:?}")]
    NonCanonicalPurpose(RunPurpose),
    #[error("memory summary must not be empty")]
    EmptySummary,
    #[error("invalid outcome data: {0}")]
    InvalidOutcome(String),
}

pub type Result<T> = std::result::Result<T, LedgerError>;

#[derive(Debug, Clone)]
pub struct LearningLedger {
    broker: ContextBroker,
    origin: Option<DocumentOrigin>,
}

impl LearningLedger {
    pub fn new(broker: ContextBroker) -> Self {
        Self {
            broker,
            origin: None,
        }
    }

    /// Binds all durable learning writes to the task that caused them.
    pub fn for_task(broker: ContextBroker, origin: DocumentOrigin) -> Self {
        Self {
            broker,
            origin: Some(origin),
        }
    }

    /// Create a candidate only from a Paper run.  Debug, Shadow, and Dry Run
    /// artifacts are intentionally unable to become future decision priors.
    pub fn create_candidate(
        &self,
        purpose: RunPurpose,
        run_id: &RunId,
        summary: impl Into<String>,
        source_refs: Vec<DocumentId>,
        now: DateTime<Utc>,
    ) -> Result<DocumentRecord> {
        self.require_canonical(purpose)?;
        let summary = summary.into();
        if summary.trim().is_empty() {
            return Err(LedgerError::EmptySummary);
        }
        let record = StoredMemory {
            item: MemoryItem::new(MemoryId::new()),
            summary,
        };
        self.persist(run_id, &record, source_refs, now)
    }

    pub fn observe(
        &self,
        purpose: RunPurpose,
        run_id: &RunId,
        memory_id: &MemoryId,
        sample: OutcomeSample,
        outcome_document_id: DocumentId,
        now: DateTime<Utc>,
    ) -> Result<DocumentRecord> {
        self.require_canonical(purpose)?;
        let (previous_document, mut record) = self.latest(memory_id)?;
        record.item.observe(sample);
        let created_at = now.max(previous_document.created_at + chrono::Duration::microseconds(1));
        self.persist(
            run_id,
            &record,
            vec![previous_document.document_id, outcome_document_id],
            created_at,
        )
    }

    pub fn retire(
        &self,
        purpose: RunPurpose,
        run_id: &RunId,
        memory_id: &MemoryId,
        reason_document_id: DocumentId,
        now: DateTime<Utc>,
    ) -> Result<DocumentRecord> {
        self.require_canonical(purpose)?;
        let (previous_document, mut record) = self.latest(memory_id)?;
        record.item.retire();
        let created_at = now.max(previous_document.created_at + chrono::Duration::microseconds(1));
        self.persist(
            run_id,
            &record,
            vec![previous_document.document_id, reason_document_id],
            created_at,
        )
    }

    /// Priors allowed in a fresh research context.  `Active` memories can
    /// guide investigation; only `Proven` memories are eligible to appear in a
    /// final decision context.
    /// Create the immutable T+1/T+3/T+5 outcome schedule for one Paper
    /// decision. Repeated task attempts return the existing schedules.
    pub fn schedule_outcomes(
        &self,
        purpose: RunPurpose,
        run_id: &RunId,
        memory_id: &MemoryId,
        memory_document_id: &DocumentId,
        decision_document_id: &DocumentId,
        execution_context_id: &DocumentId,
        regime: impl Into<String>,
        now: DateTime<Utc>,
    ) -> Result<Vec<DocumentRecord>> {
        self.require_canonical(purpose)?;
        let decision_document = self.broker.store().read_document(decision_document_id)?;
        if decision_document.kind != DocumentKind::Decision {
            return Err(LedgerError::InvalidOutcome(
                "schedule requires Decision".to_owned(),
            ));
        }
        let execution_context = self.broker.store().read_document(execution_context_id)?;
        if execution_context.kind != DocumentKind::ExecutionContext {
            return Err(LedgerError::InvalidOutcome(
                "schedule requires ExecutionContext".to_owned(),
            ));
        }
        let decision: PortfolioDecision =
            serde_json::from_value(self.broker.read_json(&decision_document)?)?;
        decision
            .validate()
            .map_err(|error| LedgerError::InvalidOutcome(error.to_string()))?;
        let baseline_prices = baseline_prices(&self.broker.read_json(&execution_context)?)?;
        let existing = self
            .broker
            .store()
            .documents_for_run(run_id)?
            .into_iter()
            .filter(|document| document.kind == DocumentKind::Outcome)
            .filter_map(|document| {
                serde_json::from_value::<OutcomeSchedule>(self.broker.read_json(&document).ok()?)
                    .ok()
                    .map(|schedule| (document, schedule))
            })
            .collect::<Vec<_>>();
        let regime = regime.into();
        [1_u8, 3, 5]
            .into_iter()
            .map(|horizon_trading_days| {
                if let Some((document, _)) = existing.iter().find(|(_, schedule)| {
                    schedule.memory_id == *memory_id
                        && schedule.decision_document_id == *decision_document_id
                        && schedule.horizon_trading_days == horizon_trading_days
                }) {
                    return Ok(document.clone());
                }
                let schedule = OutcomeSchedule {
                    schema_version: V2_SCHEMA_VERSION,
                    memory_id: memory_id.clone(),
                    decision_run_id: decision.run_id.clone(),
                    decision_document_id: decision_document_id.clone(),
                    execution_context_id: execution_context_id.clone(),
                    horizon_trading_days,
                    baseline_day: now.date_naive(),
                    baseline_prices: baseline_prices.clone(),
                    targets: decision.draft.targets.clone(),
                    regime: regime.clone(),
                };
                self.broker
                    .record_json_with_provenance(
                        NewJsonDocument {
                            kind: DocumentKind::Outcome,
                            producer: "learning.outcome_schedule".to_owned(),
                            run_id: Some(run_id.clone()),
                            lifecycle: DocumentLifecycle::Canonical,
                            source_refs: vec![
                                memory_document_id.clone(),
                                decision_document_id.clone(),
                                execution_context_id.clone(),
                            ],
                            origin: self.origin.clone(),
                            value: &serde_json::to_value(schedule)?,
                            created_at: now,
                        },
                        Provenance::local("akzio.learning", now),
                    )
                    .map_err(Into::into)
            })
            .collect()
    }

    /// Materialize every due schedule using a newly sealed daily-bar surface.
    /// Each materialized outcome immediately feeds the memory state machine.
    pub fn materialize_pending(
        &self,
        purpose: RunPurpose,
        materializer_run_id: &RunId,
        market_document_id: &DocumentId,
        market: &OutcomeMarket,
        now: DateTime<Utc>,
    ) -> Result<Vec<DocumentRecord>> {
        self.require_canonical(purpose)?;
        let documents = self
            .broker
            .store()
            .documents_by_kind(DocumentKind::Outcome)?;
        let mut schedules = Vec::new();
        let mut completed = BTreeSet::new();
        for document in documents {
            let value = self.broker.read_json(&document)?;
            if let Ok(outcome) = serde_json::from_value::<MaterializedOutcome>(value.clone()) {
                completed.insert(outcome.schedule_document_id);
            }
            if let Ok(schedule) = serde_json::from_value::<OutcomeSchedule>(value) {
                schedules.push((document, schedule));
            }
        }
        let mut materialized = Vec::new();
        for (schedule_document, schedule) in schedules {
            if completed.contains(&schedule_document.document_id) {
                continue;
            }
            let Some(outcome) = materialize(&schedule_document.document_id, &schedule, market)?
            else {
                continue;
            };
            let outcome_document = self.broker.record_json_with_provenance(
                NewJsonDocument {
                    kind: DocumentKind::Outcome,
                    producer: "learning.outcome".to_owned(),
                    run_id: Some(materializer_run_id.clone()),
                    lifecycle: DocumentLifecycle::Canonical,
                    source_refs: vec![
                        schedule_document.document_id.clone(),
                        market_document_id.clone(),
                    ],
                    origin: self.origin.clone(),
                    value: &serde_json::to_value(&outcome)?,
                    created_at: now,
                },
                Provenance::local("akzio.learning", now),
            )?;
            self.observe(
                purpose,
                materializer_run_id,
                &schedule.memory_id,
                outcome.sample.clone(),
                outcome_document.document_id.clone(),
                now,
            )?;
            materialized.push(outcome_document);
        }
        Ok(materialized)
    }

    pub fn research_priors(&self) -> Result<Vec<MemoryPrior>> {
        self.priors_for(&[MemoryState::Active, MemoryState::Proven])
    }

    pub fn decision_priors(&self) -> Result<Vec<MemoryPrior>> {
        self.priors_for(&[MemoryState::Proven])
    }

    pub fn research_prior_documents(&self) -> Result<Vec<DocumentRecord>> {
        self.documents_for(&[MemoryState::Active, MemoryState::Proven])
    }

    pub fn decision_prior_documents(&self) -> Result<Vec<DocumentRecord>> {
        self.documents_for(&[MemoryState::Proven])
    }

    pub fn latest(&self, memory_id: &MemoryId) -> Result<(DocumentRecord, StoredMemory)> {
        self.latest_records()?
            .remove(memory_id)
            .ok_or_else(|| LedgerError::MissingMemory(memory_id.clone()))
    }

    fn require_canonical(&self, purpose: RunPurpose) -> Result<()> {
        if purpose.is_canonical_learning() {
            Ok(())
        } else {
            Err(LedgerError::NonCanonicalPurpose(purpose))
        }
    }

    fn priors_for(&self, states: &[MemoryState]) -> Result<Vec<MemoryPrior>> {
        let mut priors = self
            .latest_records()?
            .into_values()
            .filter_map(|(document, memory)| {
                states.contains(&memory.item.state).then_some(MemoryPrior {
                    memory_id: memory.item.memory_id,
                    state: memory.item.state,
                    summary: memory.summary,
                    evidence_refs: document.source_refs,
                    outcome_count: memory.item.samples.len(),
                })
            })
            .collect::<Vec<_>>();
        priors.sort_by(|left, right| left.memory_id.0.cmp(&right.memory_id.0));
        Ok(priors)
    }

    fn documents_for(&self, states: &[MemoryState]) -> Result<Vec<DocumentRecord>> {
        let mut documents = self
            .latest_records()?
            .into_values()
            .filter_map(|(document, memory)| {
                states.contains(&memory.item.state).then_some(document)
            })
            .collect::<Vec<_>>();
        documents.sort_by(|left, right| left.document_id.0.cmp(&right.document_id.0));
        Ok(documents)
    }

    fn latest_records(&self) -> Result<BTreeMap<MemoryId, (DocumentRecord, StoredMemory)>> {
        let mut latest = BTreeMap::<MemoryId, (DocumentRecord, StoredMemory)>::new();
        for document in self
            .broker
            .store()
            .documents_by_kind(DocumentKind::Memory)?
        {
            let memory = serde_json::from_value::<StoredMemory>(self.broker.read_json(&document)?)?;
            let replace = latest
                .get(&memory.item.memory_id)
                .map(|(existing, _)| {
                    (document.created_at, document.document_id.clone())
                        > (existing.created_at, existing.document_id.clone())
                })
                .unwrap_or(true);
            if replace {
                latest.insert(memory.item.memory_id.clone(), (document, memory));
            }
        }
        Ok(latest)
    }

    fn persist(
        &self,
        run_id: &RunId,
        record: &StoredMemory,
        source_refs: Vec<DocumentId>,
        now: DateTime<Utc>,
    ) -> Result<DocumentRecord> {
        let value = serde_json::to_value(record)?;
        Ok(self.broker.record_json_with_provenance(
            NewJsonDocument {
                kind: DocumentKind::Memory,
                producer: "learning.memory".to_owned(),
                run_id: Some(run_id.clone()),
                lifecycle: DocumentLifecycle::Canonical,
                source_refs,
                origin: self.origin.clone(),
                value: &value,
                created_at: now,
            },
            Provenance::local("akzio.learning", now),
        )?)
    }
}

fn derive_memory_state(samples: &[OutcomeSample]) -> MemoryState {
    if samples.iter().any(|sample| sample.hard_risk_miss)
        || samples.iter().filter(|sample| sample.harmful).count() >= 3
    {
        return MemoryState::Contested;
    }
    let dates = samples
        .iter()
        .map(|sample| sample.trading_day)
        .collect::<BTreeSet<_>>();
    let regimes = samples
        .iter()
        .map(|sample| sample.regime.as_str())
        .collect::<BTreeSet<_>>();
    let positive_utility = samples
        .iter()
        .map(|sample| sample.utility_micros)
        .sum::<i64>()
        > 0;
    if samples.len() >= 12 && dates.len() >= 6 && regimes.len() >= 2 && positive_utility {
        MemoryState::Proven
    } else if samples.len() >= 5 && dates.len() >= 3 && regimes.len() >= 2 && positive_utility {
        MemoryState::Active
    } else {
        MemoryState::Candidate
    }
}

#[cfg(test)]
mod tests {
    use akzio_context::legacy::{ContextBroker, NewJsonDocument};
    use akzio_domain::{DocumentKind, DocumentLifecycle, RunId, RunPurpose};
    use akzio_store::legacy::V2Store;
    use chrono::Utc;
    use tempfile::tempdir;

    use super::*;

    fn sample(index: u32) -> OutcomeSample {
        OutcomeSample {
            run_id: RunId(index.to_string()),
            trading_day: NaiveDate::from_ymd_opt(2026, 8, 1 + index % 6).unwrap(),
            regime: if index.is_multiple_of(2) {
                "risk_on".to_owned()
            } else {
                "risk_off".to_owned()
            },
            utility_micros: 10,
            harmful: false,
            hard_risk_miss: false,
        }
    }

    #[test]
    fn memory_promotes_only_after_independent_outcomes() {
        let mut memory = MemoryItem::new(MemoryId::new());
        for index in 0..12 {
            memory.observe(sample(index));
        }
        assert_eq!(memory.state, MemoryState::Proven);
    }

    #[test]
    fn hard_risk_miss_contests_memory_immediately() {
        let mut memory = MemoryItem::new(MemoryId::new());
        let mut outcome = sample(1);
        outcome.hard_risk_miss = true;
        memory.observe(outcome);
        assert_eq!(memory.state, MemoryState::Contested);
    }

    #[test]
    fn debug_runs_cannot_write_memory() {
        let directory = tempdir().unwrap();
        let broker = ContextBroker::new(V2Store::open(directory.path()).unwrap());
        let ledger = LearningLedger::new(broker);
        assert!(matches!(
            ledger.create_candidate(RunPurpose::Debug, &RunId::new(), "x", vec![], Utc::now()),
            Err(LedgerError::NonCanonicalPurpose(RunPurpose::Debug))
        ));
    }

    #[test]
    fn proven_prior_never_contains_a_target_weight() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        let broker = ContextBroker::new(store.clone());
        let now = Utc::now();
        let run = RunId::new();
        store
            .create_run(&run, RunPurpose::Paper, "test", now)
            .unwrap();
        let source = broker
            .record_json(NewJsonDocument {
                kind: DocumentKind::Experience,
                producer: "test".to_owned(),
                run_id: Some(run.clone()),
                lifecycle: DocumentLifecycle::RunScoped,
                source_refs: vec![],
                origin: None,
                value: &serde_json::json!({"summary": "source"}),
                created_at: now,
            })
            .unwrap();
        let ledger = LearningLedger::new(broker);
        let first = ledger
            .create_candidate(
                RunPurpose::Paper,
                &run,
                "avoid unverified macro claims",
                vec![source.document_id.clone()],
                now,
            )
            .unwrap();
        let stored: StoredMemory =
            serde_json::from_value(ledger.broker.read_json(&first).unwrap()).unwrap();
        for index in 0..12 {
            ledger
                .observe(
                    RunPurpose::Paper,
                    &run,
                    &stored.item.memory_id,
                    sample(index),
                    source.document_id.clone(),
                    now + chrono::Duration::seconds(i64::from(index)),
                )
                .unwrap();
        }
        let priors = ledger.decision_priors().unwrap();
        assert_eq!(priors.len(), 1);
        assert_eq!(priors[0].summary, "avoid unverified macro claims");
    }

    #[test]
    fn topology_rolls_back_when_risk_recall_drops() {
        assert_eq!(
            advance_topology(
                TopologyState::Canary10,
                &TopologyMetrics {
                    paired_samples: 12,
                    utility_micros: 1,
                    risk_recall_ppm: 900_000,
                    evidence_completeness_ppm: 900_000,
                },
                &TopologyMetrics {
                    paired_samples: 12,
                    utility_micros: 2,
                    risk_recall_ppm: 899_999,
                    evidence_completeness_ppm: 900_000,
                },
            ),
            TopologyState::RolledBack
        );
    }

    #[test]
    fn paper_outcome_schedule_materializes_from_future_trading_sessions() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        let broker = ContextBroker::new(store.clone());
        let ledger = LearningLedger::new(broker.clone());
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-03T14:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let run = RunId::new();
        store
            .create_run(&run, RunPurpose::Paper, "test", now)
            .unwrap();
        let experience = broker
            .record_json(NewJsonDocument {
                kind: DocumentKind::Experience,
                producer: "test.experience".to_owned(),
                run_id: Some(run.clone()),
                lifecycle: DocumentLifecycle::RunScoped,
                source_refs: vec![],
                origin: None,
                value: &serde_json::json!({"summary": "candidate"}),
                created_at: now,
            })
            .unwrap();
        let memory = ledger
            .create_candidate(
                RunPurpose::Paper,
                &run,
                "candidate",
                vec![experience.document_id.clone()],
                now,
            )
            .unwrap();
        let stored: StoredMemory =
            serde_json::from_value(broker.read_json(&memory).unwrap()).unwrap();
        let mut targets = TargetPortfolio::zeroed();
        targets
            .weights
            .insert(Asset::Tqqq, akzio_domain::WeightPpm(100_000));
        let decision = PortfolioDecision {
            schema_version: V2_SCHEMA_VERSION,
            decision_id: akzio_domain::DecisionId::new(),
            run_id: run.clone(),
            source_document_id: DocumentId::new(),
            context_manifest_id: DocumentId::new(),
            memory_refs: vec![],
            policy_hash: akzio_domain::ContentHash::of_bytes(b"policy"),
            created_at: now,
            valid_until: now + chrono::Duration::hours(1),
            draft: akzio_domain::LegacyDecisionDraft {
                summary: "paper test".to_owned(),
                targets,
                confidence_ppm: 500_000,
                forecasts: vec![
                    akzio_domain::HorizonForecast {
                        trading_days: 1,
                        positive_return_probability_ppm: 500_000,
                        expected_return_ppm: 0,
                    },
                    akzio_domain::HorizonForecast {
                        trading_days: 3,
                        positive_return_probability_ppm: 500_000,
                        expected_return_ppm: 0,
                    },
                    akzio_domain::HorizonForecast {
                        trading_days: 5,
                        positive_return_probability_ppm: 500_000,
                        expected_return_ppm: 0,
                    },
                ],
                blockers: vec![],
                claim_refs: vec![],
            },
        };
        let decision_document = broker
            .record_json(NewJsonDocument {
                kind: DocumentKind::Decision,
                producer: "test.decision".to_owned(),
                run_id: Some(run.clone()),
                lifecycle: DocumentLifecycle::Canonical,
                source_refs: vec![],
                origin: None,
                value: &serde_json::to_value(decision).unwrap(),
                created_at: now,
            })
            .unwrap();
        let quotes = Asset::EXECUTABLE
            .into_iter()
            .map(|asset| {
                (
                    asset.symbol().to_owned(),
                    serde_json::json!({"bid": 100_000_000_i64, "ask": 100_000_000_i64}),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let execution_context = broker
            .record_json(NewJsonDocument {
                kind: DocumentKind::ExecutionContext,
                producer: "test.execution".to_owned(),
                run_id: Some(run.clone()),
                lifecycle: DocumentLifecycle::RunScoped,
                source_refs: vec![decision_document.document_id.clone()],
                origin: None,
                value: &serde_json::json!({"quotes": quotes}),
                created_at: now,
            })
            .unwrap();
        assert_eq!(
            ledger
                .schedule_outcomes(
                    RunPurpose::Paper,
                    &run,
                    &stored.item.memory_id,
                    &memory.document_id,
                    &decision_document.document_id,
                    &execution_context.document_id,
                    "unknown",
                    now,
                )
                .unwrap()
                .len(),
            3
        );
        let market_document = broker
            .record_json(NewJsonDocument {
                kind: DocumentKind::NormalizedEvidence,
                producer: "test.market".to_owned(),
                run_id: Some(run.clone()),
                lifecycle: DocumentLifecycle::Canonical,
                source_refs: vec![],
                origin: None,
                value: &serde_json::json!({"bars": "future"}),
                created_at: now,
            })
            .unwrap();
        let next_day = now.date_naive().succ_opt().unwrap();
        let market = OutcomeMarket {
            closes: Asset::EXECUTABLE
                .into_iter()
                .map(|asset| {
                    (
                        asset,
                        vec![DailyClose {
                            trading_day: next_day,
                            close: MoneyMicros(101_000_000),
                        }],
                    )
                })
                .collect(),
        };
        assert_eq!(
            ledger
                .materialize_pending(
                    RunPurpose::Paper,
                    &run,
                    &market_document.document_id,
                    &market,
                    now + chrono::Duration::days(1),
                )
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            ledger
                .latest(&stored.item.memory_id)
                .unwrap()
                .1
                .item
                .samples
                .len(),
            1
        );
    }
}
