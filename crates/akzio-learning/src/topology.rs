//! Durable topology selection and paired Shadow evaluation.
//!
//! The SQLite-backed document graph is the control plane.  This module keeps
//! topology policy in immutable `Evaluation` documents instead of introducing
//! a second mutable state store.

use std::collections::{BTreeMap, BTreeSet};

use akzio_context::legacy::{ContextBroker, NewJsonDocument};
use akzio_domain::{
    ContentHash, DocumentId, DocumentKind, DocumentLifecycle, DocumentOrigin, DocumentRecord,
    PortfolioDecision, Provenance, RunId, TargetPortfolio, TopologyId, V2_SCHEMA_VERSION,
};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    baseline_prices, close_after, portfolio_return_ppm, return_ppm, LedgerError, OutcomeMarket,
    Result,
};

const TOPOLOGY_STATE_PRODUCER: &str = "learning.topology_state";
const SHADOW_PAIR_PRODUCER: &str = "learning.shadow_pair";
const TOPOLOGY_OUTCOME_PRODUCER: &str = "learning.topology_outcome";
const FULL_PPM: u32 = 1_000_000;
const HARD_RISK_RETURN_PPM: i64 = -100_000;
const MIN_SAMPLES_PER_TRANSITION: u32 = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopologyState {
    Candidate,
    Canary10,
    Canary25,
    Canary50,
    Active,
    RolledBack,
}

impl TopologyState {
    const fn canary_percent(self) -> u8 {
        match self {
            Self::Canary10 => 10,
            Self::Canary25 => 25,
            Self::Canary50 => 50,
            _ => 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TopologyMetrics {
    pub paired_samples: u32,
    pub utility_micros: i64,
    pub risk_recall_ppm: u32,
    pub evidence_completeness_ppm: u32,
}

impl TopologyMetrics {
    fn sample(utility_micros: i64, risk_recall_ppm: u32, evidence_completeness_ppm: u32) -> Self {
        Self {
            paired_samples: 1,
            utility_micros,
            risk_recall_ppm,
            evidence_completeness_ppm,
        }
    }

    fn merge(&mut self, other: &Self) {
        if other.paired_samples == 0 {
            return;
        }
        let prior = u64::from(self.paired_samples);
        let incoming = u64::from(other.paired_samples);
        let total = prior.saturating_add(incoming);
        self.utility_micros = self.utility_micros.saturating_add(other.utility_micros);
        self.risk_recall_ppm =
            weighted_ppm(self.risk_recall_ppm, prior, other.risk_recall_ppm, incoming);
        self.evidence_completeness_ppm = weighted_ppm(
            self.evidence_completeness_ppm,
            prior,
            other.evidence_completeness_ppm,
            incoming,
        );
        self.paired_samples = total.min(u64::from(u32::MAX)) as u32;
    }
}

/// The topology policy is deliberately conservative about safety: a candidate
/// is immediately rolled back on lower risk recall or evidence completeness,
/// and only advances after paired Shadow samples outperform the active graph.
pub fn advance_topology(
    current: TopologyState,
    active: &TopologyMetrics,
    candidate: &TopologyMetrics,
) -> TopologyState {
    if candidate.risk_recall_ppm < active.risk_recall_ppm
        || candidate.evidence_completeness_ppm < active.evidence_completeness_ppm
    {
        return TopologyState::RolledBack;
    }
    if candidate.paired_samples < MIN_SAMPLES_PER_TRANSITION
        || candidate.utility_micros <= active.utility_micros
    {
        return current;
    }
    match current {
        TopologyState::Candidate => TopologyState::Canary10,
        TopologyState::Canary10 => TopologyState::Canary25,
        TopologyState::Canary25 => TopologyState::Canary50,
        TopologyState::Canary50 => TopologyState::Active,
        state => state,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologyRecord {
    pub schema_version: u32,
    pub topology_id: TopologyId,
    pub state: TopologyState,
    pub metrics: TopologyMetrics,
    /// The pair count at the last successful state change.  A candidate must
    /// earn a fresh paired sample window for every canary step.
    pub transition_sample_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShadowPair {
    pub schema_version: u32,
    pub parent_run_id: RunId,
    pub shadow_run_id: RunId,
    pub active_topology_id: TopologyId,
    pub candidate_topology_id: TopologyId,
    pub active_decision_document_id: DocumentId,
    pub execution_context_id: DocumentId,
    pub candidate_decision_document_id: Option<DocumentId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologyOutcome {
    pub schema_version: u32,
    pub pair_document_id: DocumentId,
    pub horizon_trading_days: u8,
    pub trading_day: NaiveDate,
    pub active_topology_id: TopologyId,
    pub candidate_topology_id: TopologyId,
    pub active_portfolio_return_ppm: i64,
    pub candidate_portfolio_return_ppm: i64,
    pub benchmark_return_ppm: i64,
    pub active_metrics: TopologyMetrics,
    pub candidate_metrics: TopologyMetrics,
}

#[derive(Debug, Clone)]
pub struct TopologyLedger {
    broker: ContextBroker,
    origin: Option<DocumentOrigin>,
}

impl TopologyLedger {
    pub fn new(broker: ContextBroker) -> Self {
        Self {
            broker,
            origin: None,
        }
    }

    pub fn for_task(broker: ContextBroker, origin: DocumentOrigin) -> Self {
        Self {
            broker,
            origin: Some(origin),
        }
    }

    pub fn ensure_topology(
        &self,
        run_id: &RunId,
        topology_id: TopologyId,
        state: TopologyState,
        now: DateTime<Utc>,
    ) -> Result<DocumentRecord> {
        if let Some((document, _)) = self.latest_states()?.remove(&topology_id) {
            return Ok(document);
        }
        self.record_state(
            run_id,
            TopologyRecord {
                schema_version: V2_SCHEMA_VERSION,
                topology_id,
                state,
                metrics: TopologyMetrics::default(),
                transition_sample_count: 0,
            },
            vec![],
            now,
        )
    }

    /// Pick a durable topology before a run starts. Canary enrollment is
    /// deterministic from the run ID, so retries cannot silently alter traffic
    /// allocation.
    pub fn topology_for_run(&self, run_id: &RunId, fallback: TopologyId) -> Result<TopologyId> {
        let states = self.latest_states()?;
        let bucket = stable_bucket(run_id);
        let mut canaries = states
            .values()
            .filter_map(|(_, record)| {
                (record.state.canary_percent() > bucket).then_some(record.topology_id.clone())
            })
            .collect::<Vec<_>>();
        canaries.sort();
        if let Some(topology) = canaries.into_iter().next() {
            return Ok(topology);
        }

        states
            .into_values()
            .filter(|(_, record)| record.state == TopologyState::Active)
            .max_by(|(left_document, _), (right_document, _)| {
                (left_document.created_at, &left_document.document_id)
                    .cmp(&(right_document.created_at, &right_document.document_id))
            })
            .map(|(_, record)| record.topology_id)
            .map_or(Ok(fallback), Ok)
    }

    pub fn queue_shadow_pair(
        &self,
        parent_run_id: &RunId,
        shadow_run_id: &RunId,
        active_topology_id: TopologyId,
        candidate_topology_id: TopologyId,
        active_decision_document_id: DocumentId,
        execution_context_id: DocumentId,
        now: DateTime<Utc>,
    ) -> Result<DocumentRecord> {
        if let Some((document, _)) = self.latest_pairs()?.remove(shadow_run_id) {
            return Ok(document);
        }
        self.ensure_topology(
            parent_run_id,
            active_topology_id.clone(),
            TopologyState::Active,
            now,
        )?;
        self.ensure_topology(
            parent_run_id,
            candidate_topology_id.clone(),
            TopologyState::Candidate,
            now,
        )?;
        let source_refs = vec![
            active_decision_document_id.clone(),
            execution_context_id.clone(),
        ];
        self.record_pair(
            parent_run_id,
            ShadowPair {
                schema_version: V2_SCHEMA_VERSION,
                parent_run_id: parent_run_id.clone(),
                shadow_run_id: shadow_run_id.clone(),
                active_topology_id,
                candidate_topology_id,
                active_decision_document_id,
                execution_context_id,
                candidate_decision_document_id: None,
            },
            source_refs,
            now,
        )
    }

    pub fn complete_shadow_pair(
        &self,
        shadow_run_id: &RunId,
        candidate_decision_document_id: DocumentId,
        now: DateTime<Utc>,
    ) -> Result<DocumentRecord> {
        let (previous_document, mut pair) = self
            .latest_pairs()?
            .remove(shadow_run_id)
            .ok_or_else(|| LedgerError::InvalidOutcome("shadow pair is missing".to_owned()))?;
        if pair.candidate_decision_document_id.is_some() {
            return Ok(previous_document);
        }
        let candidate = self
            .broker
            .store()
            .read_document(&candidate_decision_document_id)?;
        if candidate.kind != DocumentKind::Decision {
            return Err(LedgerError::InvalidOutcome(
                "shadow candidate must produce a Decision".to_owned(),
            ));
        }
        pair.candidate_decision_document_id = Some(candidate_decision_document_id.clone());
        self.record_pair(
            shadow_run_id,
            pair,
            vec![
                previous_document.document_id,
                candidate_decision_document_id,
            ],
            now,
        )
    }

    /// Materialize all due paired outcomes from newly sealed Paper market data.
    /// It is idempotent by `(pair_document_id, horizon_trading_days)`.
    pub fn materialize_pending(
        &self,
        materializer_run_id: &RunId,
        market_document_id: &DocumentId,
        market: &OutcomeMarket,
        now: DateTime<Utc>,
    ) -> Result<Vec<DocumentRecord>> {
        let pairs = self.latest_pairs()?;
        let completed = self.completed_outcomes()?;
        let mut materialized = Vec::new();
        let mut affected = BTreeSet::new();

        for (pair_document, pair) in pairs.into_values() {
            if pair.candidate_decision_document_id.is_none() {
                continue;
            }
            for horizon_trading_days in [1_u8, 3, 5] {
                if completed.contains(&(pair_document.document_id.clone(), horizon_trading_days)) {
                    continue;
                }
                let Some(outcome) =
                    self.materialize_pair(&pair_document, &pair, horizon_trading_days, market)?
                else {
                    continue;
                };
                let outcome_document = self.broker.record_json_with_provenance(
                    NewJsonDocument {
                        kind: DocumentKind::Evaluation,
                        producer: TOPOLOGY_OUTCOME_PRODUCER.to_owned(),
                        run_id: Some(materializer_run_id.clone()),
                        lifecycle: DocumentLifecycle::Canonical,
                        source_refs: vec![
                            pair_document.document_id.clone(),
                            market_document_id.clone(),
                        ],
                        origin: self.origin.clone(),
                        value: &serde_json::to_value(&outcome)?,
                        created_at: now,
                    },
                    Provenance::local("akzio.learning", now),
                )?;
                materialized.push(outcome_document);
                affected.insert((
                    pair.active_topology_id.clone(),
                    pair.candidate_topology_id.clone(),
                ));
            }
        }

        for (active_topology_id, candidate_topology_id) in affected {
            self.refresh_states(
                materializer_run_id,
                &active_topology_id,
                &candidate_topology_id,
                now,
            )?;
        }
        Ok(materialized)
    }

    pub fn records(&self) -> Result<Vec<TopologyRecord>> {
        let mut records = self
            .latest_states()?
            .into_values()
            .map(|(_, record)| record)
            .collect::<Vec<_>>();
        records.sort_by(|left, right| left.topology_id.cmp(&right.topology_id));
        Ok(records)
    }

    fn materialize_pair(
        &self,
        pair_document: &DocumentRecord,
        pair: &ShadowPair,
        horizon_trading_days: u8,
        market: &OutcomeMarket,
    ) -> Result<Option<TopologyOutcome>> {
        let execution_context = self
            .broker
            .store()
            .read_document(&pair.execution_context_id)?;
        if execution_context.kind != DocumentKind::ExecutionContext {
            return Err(LedgerError::InvalidOutcome(
                "shadow pair needs an ExecutionContext".to_owned(),
            ));
        }
        let baseline = baseline_prices(&self.broker.read_json(&execution_context)?)?;
        let baseline_day = execution_context.created_at.date_naive();
        let active = self.read_decision(&pair.active_decision_document_id)?;
        let candidate = self.read_decision(
            pair.candidate_decision_document_id
                .as_ref()
                .expect("checked before materialization"),
        )?;
        let Some((trading_day, active_return, benchmark_return)) = realized_returns(
            &active.draft.targets,
            &baseline,
            baseline_day,
            horizon_trading_days,
            market,
        )?
        else {
            return Ok(None);
        };
        let Some((candidate_day, candidate_return, candidate_benchmark)) = realized_returns(
            &candidate.draft.targets,
            &baseline,
            baseline_day,
            horizon_trading_days,
            market,
        )?
        else {
            return Ok(None);
        };
        if trading_day != candidate_day || benchmark_return != candidate_benchmark {
            return Err(LedgerError::InvalidOutcome(
                "paired outcome market is inconsistent".to_owned(),
            ));
        }

        Ok(Some(TopologyOutcome {
            schema_version: V2_SCHEMA_VERSION,
            pair_document_id: pair_document.document_id.clone(),
            horizon_trading_days,
            trading_day,
            active_topology_id: pair.active_topology_id.clone(),
            candidate_topology_id: pair.candidate_topology_id.clone(),
            active_portfolio_return_ppm: active_return,
            candidate_portfolio_return_ppm: candidate_return,
            benchmark_return_ppm: benchmark_return,
            active_metrics: self.metrics_for_decision(&active, active_return, benchmark_return)?,
            candidate_metrics: self.metrics_for_decision(
                &candidate,
                candidate_return,
                benchmark_return,
            )?,
        }))
    }

    fn read_decision(&self, document_id: &DocumentId) -> Result<PortfolioDecision> {
        let document = self.broker.store().read_document(document_id)?;
        if document.kind != DocumentKind::Decision {
            return Err(LedgerError::InvalidOutcome(
                "topology outcome needs a Decision".to_owned(),
            ));
        }
        let decision =
            serde_json::from_value::<PortfolioDecision>(self.broker.read_json(&document)?)?;
        decision
            .validate()
            .map_err(|error| LedgerError::InvalidOutcome(error.to_string()))?;
        Ok(decision)
    }

    fn metrics_for_decision(
        &self,
        decision: &PortfolioDecision,
        portfolio_return_ppm: i64,
        benchmark_return_ppm: i64,
    ) -> Result<TopologyMetrics> {
        let evidence_completeness_ppm = self.evidence_completeness(&decision.draft.claim_refs)?;
        let risk_recall_ppm = if portfolio_return_ppm <= HARD_RISK_RETURN_PPM {
            0
        } else {
            FULL_PPM
        };
        Ok(TopologyMetrics::sample(
            portfolio_return_ppm.saturating_sub(benchmark_return_ppm),
            risk_recall_ppm,
            evidence_completeness_ppm,
        ))
    }

    fn evidence_completeness(&self, claim_refs: &[DocumentId]) -> Result<u32> {
        if claim_refs.is_empty() {
            return Ok(0);
        }
        let complete = claim_refs
            .iter()
            .filter_map(|document_id| self.broker.store().read_document(document_id).ok())
            .filter(|document| {
                document.kind == DocumentKind::AgentClaim && !document.source_refs.is_empty()
            })
            .count();
        Ok((u64::try_from(complete)
            .unwrap_or(u64::MAX)
            .saturating_mul(u64::from(FULL_PPM))
            / u64::try_from(claim_refs.len()).unwrap_or(1)) as u32)
    }

    fn refresh_states(
        &self,
        materializer_run_id: &RunId,
        active_topology_id: &TopologyId,
        candidate_topology_id: &TopologyId,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let states = self.latest_states()?;
        let active_metrics = self.metrics_for_topology(active_topology_id)?;
        let candidate_metrics = self.metrics_for_topology(candidate_topology_id)?;
        let active_record = states
            .get(active_topology_id)
            .map(|(_, record)| record.clone())
            .unwrap_or(TopologyRecord {
                schema_version: V2_SCHEMA_VERSION,
                topology_id: active_topology_id.clone(),
                state: TopologyState::Active,
                metrics: TopologyMetrics::default(),
                transition_sample_count: 0,
            });
        let candidate_record = states
            .get(candidate_topology_id)
            .map(|(_, record)| record.clone())
            .unwrap_or(TopologyRecord {
                schema_version: V2_SCHEMA_VERSION,
                topology_id: candidate_topology_id.clone(),
                state: TopologyState::Candidate,
                metrics: TopologyMetrics::default(),
                transition_sample_count: 0,
            });

        let proposed =
            advance_topology(candidate_record.state, &active_metrics, &candidate_metrics);
        let transition_ready = candidate_metrics.paired_samples
            >= candidate_record
                .transition_sample_count
                .saturating_add(MIN_SAMPLES_PER_TRANSITION);
        let next_state = if proposed == TopologyState::RolledBack || transition_ready {
            proposed
        } else {
            candidate_record.state
        };
        let transition_sample_count = if next_state != candidate_record.state {
            candidate_metrics.paired_samples
        } else {
            candidate_record.transition_sample_count
        };

        self.record_state(
            materializer_run_id,
            TopologyRecord {
                metrics: active_metrics,
                ..active_record
            },
            vec![],
            now,
        )?;
        self.record_state(
            materializer_run_id,
            TopologyRecord {
                metrics: candidate_metrics,
                state: next_state,
                transition_sample_count,
                ..candidate_record
            },
            vec![],
            now,
        )?;
        Ok(())
    }

    fn metrics_for_topology(&self, topology_id: &TopologyId) -> Result<TopologyMetrics> {
        let mut metrics = TopologyMetrics::default();
        for document in self
            .broker
            .store()
            .documents_by_kind(DocumentKind::Evaluation)?
            .into_iter()
            .filter(|document| document.producer == TOPOLOGY_OUTCOME_PRODUCER)
        {
            let outcome =
                serde_json::from_value::<TopologyOutcome>(self.broker.read_json(&document)?)?;
            if &outcome.active_topology_id == topology_id {
                metrics.merge(&outcome.active_metrics);
            }
            if &outcome.candidate_topology_id == topology_id {
                metrics.merge(&outcome.candidate_metrics);
            }
        }
        Ok(metrics)
    }

    fn completed_outcomes(&self) -> Result<BTreeSet<(DocumentId, u8)>> {
        self.broker
            .store()
            .documents_by_kind(DocumentKind::Evaluation)?
            .into_iter()
            .filter(|document| document.producer == TOPOLOGY_OUTCOME_PRODUCER)
            .map(|document| {
                serde_json::from_value::<TopologyOutcome>(self.broker.read_json(&document)?)
                    .map(|outcome| (outcome.pair_document_id, outcome.horizon_trading_days))
                    .map_err(Into::into)
            })
            .collect()
    }

    fn latest_states(&self) -> Result<BTreeMap<TopologyId, (DocumentRecord, TopologyRecord)>> {
        let mut states = BTreeMap::new();
        for document in self
            .broker
            .store()
            .documents_by_kind(DocumentKind::Evaluation)?
            .into_iter()
            .filter(|document| document.producer == TOPOLOGY_STATE_PRODUCER)
        {
            let record =
                serde_json::from_value::<TopologyRecord>(self.broker.read_json(&document)?)?;
            replace_if_newer(&mut states, record.topology_id.clone(), document, record);
        }
        Ok(states)
    }

    fn latest_pairs(&self) -> Result<BTreeMap<RunId, (DocumentRecord, ShadowPair)>> {
        let mut pairs: BTreeMap<RunId, (DocumentRecord, ShadowPair)> = BTreeMap::new();
        for document in self
            .broker
            .store()
            .documents_by_kind(DocumentKind::Evaluation)?
            .into_iter()
            .filter(|document| document.producer == SHADOW_PAIR_PRODUCER)
        {
            let pair = serde_json::from_value::<ShadowPair>(self.broker.read_json(&document)?)?;
            let replace = pairs
                .get(&pair.shadow_run_id)
                .map(|(existing_document, existing_pair)| {
                    match (
                        existing_pair.candidate_decision_document_id.is_some(),
                        pair.candidate_decision_document_id.is_some(),
                    ) {
                        (false, true) => true,
                        (true, false) => false,
                        _ => {
                            (document.created_at, &document.document_id)
                                > (existing_document.created_at, &existing_document.document_id)
                        }
                    }
                })
                .unwrap_or(true);
            if replace {
                pairs.insert(pair.shadow_run_id.clone(), (document, pair));
            }
        }
        Ok(pairs)
    }

    fn record_state(
        &self,
        run_id: &RunId,
        record: TopologyRecord,
        source_refs: Vec<DocumentId>,
        now: DateTime<Utc>,
    ) -> Result<DocumentRecord> {
        Ok(self.broker.record_json_with_provenance(
            NewJsonDocument {
                kind: DocumentKind::Evaluation,
                producer: TOPOLOGY_STATE_PRODUCER.to_owned(),
                run_id: Some(run_id.clone()),
                lifecycle: DocumentLifecycle::Canonical,
                source_refs,
                origin: self.origin.clone(),
                value: &serde_json::to_value(record)?,
                created_at: now,
            },
            Provenance::local("akzio.learning", now),
        )?)
    }

    fn record_pair(
        &self,
        run_id: &RunId,
        pair: ShadowPair,
        source_refs: Vec<DocumentId>,
        now: DateTime<Utc>,
    ) -> Result<DocumentRecord> {
        Ok(self.broker.record_json_with_provenance(
            NewJsonDocument {
                kind: DocumentKind::Evaluation,
                producer: SHADOW_PAIR_PRODUCER.to_owned(),
                run_id: Some(run_id.clone()),
                lifecycle: DocumentLifecycle::Canonical,
                source_refs,
                origin: self.origin.clone(),
                value: &serde_json::to_value(pair)?,
                created_at: now,
            },
            Provenance::local("akzio.learning", now),
        )?)
    }
}

fn realized_returns(
    targets: &TargetPortfolio,
    baseline: &BTreeMap<akzio_domain::Asset, akzio_domain::MoneyMicros>,
    baseline_day: NaiveDate,
    horizon_trading_days: u8,
    market: &OutcomeMarket,
) -> Result<Option<(NaiveDate, i64, i64)>> {
    let mut future_prices = BTreeMap::new();
    let mut trading_day = None;
    for asset in akzio_domain::Asset::EXECUTABLE {
        let Some(close) = close_after(
            market
                .closes
                .get(&asset)
                .map(Vec::as_slice)
                .unwrap_or_default(),
            baseline_day,
            horizon_trading_days,
        ) else {
            return Ok(None);
        };
        trading_day = Some(trading_day.map_or(close.trading_day, |day: NaiveDate| {
            day.max(close.trading_day)
        }));
        future_prices.insert(asset, close.close);
    }
    let Some(trading_day) = trading_day else {
        return Ok(None);
    };
    let portfolio_return_ppm = portfolio_return_ppm(targets, baseline, &future_prices)?;
    let benchmark_return_ppm = return_ppm(
        *baseline
            .get(&akzio_domain::Asset::Qqq)
            .ok_or_else(|| LedgerError::InvalidOutcome("QQQ baseline is absent".to_owned()))?,
        *future_prices
            .get(&akzio_domain::Asset::Qqq)
            .ok_or_else(|| LedgerError::InvalidOutcome("QQQ close is absent".to_owned()))?,
    )?;
    Ok(Some((
        trading_day,
        portfolio_return_ppm,
        benchmark_return_ppm,
    )))
}

fn weighted_ppm(left: u32, left_weight: u64, right: u32, right_weight: u64) -> u32 {
    let total = left_weight.saturating_add(right_weight);
    if total == 0 {
        return 0;
    }
    let weighted = u64::from(left)
        .saturating_mul(left_weight)
        .saturating_add(u64::from(right).saturating_mul(right_weight));
    (weighted / total).min(u64::from(FULL_PPM)) as u32
}

fn stable_bucket(run_id: &RunId) -> u8 {
    u8::from_str_radix(
        &ContentHash::of_bytes(run_id.0.as_bytes()).as_str()[..2],
        16,
    )
    .unwrap_or(99)
        % 100
}

fn replace_if_newer<K: Ord, V>(
    entries: &mut BTreeMap<K, (DocumentRecord, V)>,
    key: K,
    document: DocumentRecord,
    value: V,
) {
    let replace = entries
        .get(&key)
        .map(|(existing, _)| {
            (document.created_at, &document.document_id)
                > (existing.created_at, &existing.document_id)
        })
        .unwrap_or(true);
    if replace {
        entries.insert(key, (document, value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use akzio_context::legacy::NewJsonDocument;
    use akzio_domain::{DocumentLifecycle, RunPurpose};
    use akzio_store::legacy::V2Store;
    use tempfile::tempdir;

    fn metrics(utility_micros: i64, risk_recall_ppm: u32, evidence_ppm: u32) -> TopologyMetrics {
        TopologyMetrics {
            paired_samples: 1,
            utility_micros,
            risk_recall_ppm,
            evidence_completeness_ppm: evidence_ppm,
        }
    }

    fn record_outcome(
        broker: &ContextBroker,
        run_id: &RunId,
        active_topology_id: &TopologyId,
        candidate_topology_id: &TopologyId,
        active_metrics: TopologyMetrics,
        candidate_metrics: TopologyMetrics,
        created_at: DateTime<Utc>,
    ) {
        broker
            .record_json(NewJsonDocument {
                kind: DocumentKind::Evaluation,
                producer: TOPOLOGY_OUTCOME_PRODUCER.to_owned(),
                run_id: Some(run_id.clone()),
                lifecycle: DocumentLifecycle::Canonical,
                source_refs: vec![],
                origin: None,
                value: &serde_json::to_value(TopologyOutcome {
                    schema_version: V2_SCHEMA_VERSION,
                    pair_document_id: DocumentId::new(),
                    horizon_trading_days: 1,
                    trading_day: created_at.date_naive(),
                    active_topology_id: active_topology_id.clone(),
                    candidate_topology_id: candidate_topology_id.clone(),
                    active_portfolio_return_ppm: 0,
                    candidate_portfolio_return_ppm: 0,
                    benchmark_return_ppm: 0,
                    active_metrics,
                    candidate_metrics,
                })
                .unwrap(),
                created_at,
            })
            .unwrap();
    }

    fn decision_for(
        run_id: &RunId,
        asset: akzio_domain::Asset,
        now: DateTime<Utc>,
    ) -> PortfolioDecision {
        let mut targets = TargetPortfolio::zeroed();
        targets
            .weights
            .insert(asset, akzio_domain::WeightPpm(1_000_000));
        PortfolioDecision {
            schema_version: V2_SCHEMA_VERSION,
            decision_id: akzio_domain::DecisionId::new(),
            run_id: run_id.clone(),
            source_document_id: DocumentId::new(),
            context_manifest_id: DocumentId::new(),
            memory_refs: vec![],
            policy_hash: ContentHash::of_bytes(b"test-policy"),
            created_at: now,
            valid_until: now + chrono::Duration::hours(1),
            draft: akzio_domain::LegacyDecisionDraft {
                summary: "topology test".to_owned(),
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
        }
    }

    #[test]
    fn topology_state_is_durable_and_latest_active_topology_wins() {
        let directory = tempdir().unwrap();
        let broker = ContextBroker::new(V2Store::open(directory.path()).unwrap());
        let ledger = TopologyLedger::new(broker.clone());
        let now = Utc::now();
        let run = RunId::new();
        broker
            .store()
            .create_run(&run, RunPurpose::Paper, "baseline", now)
            .unwrap();
        let baseline = TopologyId("baseline".to_owned());
        let candidate = TopologyId("candidate".to_owned());
        ledger
            .ensure_topology(&run, baseline.clone(), TopologyState::Active, now)
            .unwrap();
        ledger
            .ensure_topology(&run, candidate.clone(), TopologyState::Candidate, now)
            .unwrap();
        ledger
            .record_state(
                &run,
                TopologyRecord {
                    schema_version: V2_SCHEMA_VERSION,
                    topology_id: candidate.clone(),
                    state: TopologyState::Active,
                    metrics: TopologyMetrics::default(),
                    transition_sample_count: 0,
                },
                vec![],
                now + chrono::Duration::microseconds(1),
            )
            .unwrap();

        assert_eq!(
            ledger.topology_for_run(&RunId::new(), baseline).unwrap(),
            candidate
        );
        broker.store().verify_integrity().unwrap();
    }

    #[test]
    fn shadow_pair_is_idempotent_until_candidate_decision_arrives() {
        let directory = tempdir().unwrap();
        let broker = ContextBroker::new(V2Store::open(directory.path()).unwrap());
        let ledger = TopologyLedger::new(broker.clone());
        let now = Utc::now();
        let parent = RunId::new();
        let shadow = RunId::new();
        broker
            .store()
            .create_run(&parent, RunPurpose::Paper, "baseline", now)
            .unwrap();
        broker
            .store()
            .create_run(&shadow, RunPurpose::Shadow, "candidate", now)
            .unwrap();
        let decision = broker
            .record_json(NewJsonDocument {
                kind: DocumentKind::Decision,
                producer: "test.decision".to_owned(),
                run_id: Some(parent.clone()),
                lifecycle: DocumentLifecycle::Canonical,
                source_refs: vec![],
                origin: None,
                value: &serde_json::json!({}),
                created_at: now,
            })
            .unwrap();
        let execution = broker
            .record_json(NewJsonDocument {
                kind: DocumentKind::ExecutionContext,
                producer: "test.execution".to_owned(),
                run_id: Some(parent.clone()),
                lifecycle: DocumentLifecycle::RunScoped,
                source_refs: vec![],
                origin: None,
                value: &serde_json::json!({}),
                created_at: now,
            })
            .unwrap();

        let first = ledger
            .queue_shadow_pair(
                &parent,
                &shadow,
                TopologyId("baseline".to_owned()),
                TopologyId("candidate".to_owned()),
                decision.document_id.clone(),
                execution.document_id.clone(),
                now,
            )
            .unwrap();
        let second = ledger
            .queue_shadow_pair(
                &parent,
                &shadow,
                TopologyId("baseline".to_owned()),
                TopologyId("candidate".to_owned()),
                decision.document_id.clone(),
                DocumentId::new(),
                now,
            )
            .unwrap();
        assert_eq!(first.document_id, second.document_id);
        assert_eq!(
            first.source_refs,
            vec![decision.document_id.clone(), execution.document_id]
        );
        let candidate = broker
            .record_json(NewJsonDocument {
                kind: DocumentKind::Decision,
                producer: "test.candidate_decision".to_owned(),
                run_id: Some(shadow.clone()),
                lifecycle: DocumentLifecycle::Canonical,
                source_refs: vec![],
                origin: None,
                value: &serde_json::json!({}),
                created_at: now,
            })
            .unwrap();
        let completed = ledger
            .complete_shadow_pair(&shadow, candidate.document_id.clone(), now)
            .unwrap();
        let repeated = ledger
            .complete_shadow_pair(&shadow, DocumentId::new(), now)
            .unwrap();
        assert_eq!(completed.document_id, repeated.document_id);
        let pair: ShadowPair =
            serde_json::from_value(broker.read_json(&completed).unwrap()).unwrap();
        assert_eq!(pair.parent_run_id, parent);
        assert_eq!(
            pair.candidate_decision_document_id,
            Some(candidate.document_id)
        );
        broker.store().verify_integrity().unwrap();
    }

    #[test]
    fn topology_requires_a_fresh_paired_window_for_each_canary_transition() {
        let directory = tempdir().unwrap();
        let broker = ContextBroker::new(V2Store::open(directory.path()).unwrap());
        let ledger = TopologyLedger::new(broker.clone());
        let now = Utc::now();
        let run = RunId::new();
        let active = TopologyId("active".to_owned());
        let candidate = TopologyId("candidate".to_owned());
        broker
            .store()
            .create_run(&run, RunPurpose::Paper, "topology-test", now)
            .unwrap();
        ledger
            .ensure_topology(&run, active.clone(), TopologyState::Active, now)
            .unwrap();
        ledger
            .ensure_topology(&run, candidate.clone(), TopologyState::Candidate, now)
            .unwrap();

        for offset in 0_i64..12 {
            record_outcome(
                &broker,
                &run,
                &active,
                &candidate,
                metrics(0, FULL_PPM, FULL_PPM),
                metrics(1, FULL_PPM, FULL_PPM),
                now + chrono::Duration::microseconds(offset),
            );
        }
        ledger
            .refresh_states(
                &run,
                &active,
                &candidate,
                now + chrono::Duration::seconds(1),
            )
            .unwrap();
        let first = ledger
            .records()
            .unwrap()
            .into_iter()
            .find(|record| &record.topology_id == &candidate)
            .unwrap();
        assert_eq!(first.state, TopologyState::Canary10);
        assert_eq!(first.transition_sample_count, 12);

        record_outcome(
            &broker,
            &run,
            &active,
            &candidate,
            metrics(0, FULL_PPM, FULL_PPM),
            metrics(1, FULL_PPM, FULL_PPM),
            now + chrono::Duration::microseconds(12),
        );
        ledger
            .refresh_states(
                &run,
                &active,
                &candidate,
                now + chrono::Duration::seconds(2),
            )
            .unwrap();
        assert_eq!(
            ledger
                .records()
                .unwrap()
                .into_iter()
                .find(|record| &record.topology_id == &candidate)
                .unwrap()
                .state,
            TopologyState::Canary10
        );

        for offset in 13_i64..24 {
            record_outcome(
                &broker,
                &run,
                &active,
                &candidate,
                metrics(0, FULL_PPM, FULL_PPM),
                metrics(1, FULL_PPM, FULL_PPM),
                now + chrono::Duration::microseconds(offset),
            );
        }
        ledger
            .refresh_states(
                &run,
                &active,
                &candidate,
                now + chrono::Duration::seconds(3),
            )
            .unwrap();
        let second = ledger
            .records()
            .unwrap()
            .into_iter()
            .find(|record| &record.topology_id == &candidate)
            .unwrap();
        assert_eq!(second.state, TopologyState::Canary25);
        assert_eq!(second.transition_sample_count, 24);
        broker.store().verify_integrity().unwrap();
    }

    #[test]
    fn lower_risk_recall_or_evidence_completeness_rolls_back_a_candidate() {
        let directory = tempdir().unwrap();
        let broker = ContextBroker::new(V2Store::open(directory.path()).unwrap());
        let ledger = TopologyLedger::new(broker.clone());
        let now = Utc::now();
        let run = RunId::new();
        let active = TopologyId("active".to_owned());
        let candidate = TopologyId("candidate".to_owned());
        broker
            .store()
            .create_run(&run, RunPurpose::Paper, "topology-test", now)
            .unwrap();
        ledger
            .ensure_topology(&run, active.clone(), TopologyState::Active, now)
            .unwrap();
        ledger
            .ensure_topology(&run, candidate.clone(), TopologyState::Canary10, now)
            .unwrap();
        record_outcome(
            &broker,
            &run,
            &active,
            &candidate,
            metrics(0, FULL_PPM, FULL_PPM),
            metrics(1, FULL_PPM - 1, FULL_PPM),
            now,
        );
        ledger
            .refresh_states(
                &run,
                &active,
                &candidate,
                now + chrono::Duration::seconds(1),
            )
            .unwrap();
        assert_eq!(
            ledger
                .records()
                .unwrap()
                .into_iter()
                .find(|record| &record.topology_id == &candidate)
                .unwrap()
                .state,
            TopologyState::RolledBack
        );
        broker.store().verify_integrity().unwrap();
    }

    #[test]
    fn due_shadow_outcomes_materialize_once_per_horizon() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        let broker = ContextBroker::new(store.clone());
        let ledger = TopologyLedger::new(broker.clone());
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-03T14:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let parent = RunId::new();
        let shadow = RunId::new();
        let active = TopologyId("baseline".to_owned());
        let candidate = TopologyId("candidate".to_owned());
        store
            .create_run(&parent, RunPurpose::Paper, active.0.as_str(), now)
            .unwrap();
        store
            .create_run(&shadow, RunPurpose::Shadow, candidate.0.as_str(), now)
            .unwrap();
        let active_decision = broker
            .record_json(NewJsonDocument {
                kind: DocumentKind::Decision,
                producer: "test.active_decision".to_owned(),
                run_id: Some(parent.clone()),
                lifecycle: DocumentLifecycle::Canonical,
                source_refs: vec![],
                origin: None,
                value: &serde_json::to_value(decision_for(&parent, akzio_domain::Asset::Tqqq, now))
                    .unwrap(),
                created_at: now,
            })
            .unwrap();
        let quotes = akzio_domain::Asset::EXECUTABLE
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
                producer: "test.execution_context".to_owned(),
                run_id: Some(parent.clone()),
                lifecycle: DocumentLifecycle::RunScoped,
                source_refs: vec![active_decision.document_id.clone()],
                origin: None,
                value: &serde_json::json!({"quotes": quotes}),
                created_at: now,
            })
            .unwrap();
        ledger
            .queue_shadow_pair(
                &parent,
                &shadow,
                active,
                candidate,
                active_decision.document_id,
                execution_context.document_id,
                now,
            )
            .unwrap();
        let candidate_decision = broker
            .record_json(NewJsonDocument {
                kind: DocumentKind::Decision,
                producer: "test.candidate_decision".to_owned(),
                run_id: Some(shadow.clone()),
                lifecycle: DocumentLifecycle::Canonical,
                source_refs: vec![],
                origin: None,
                value: &serde_json::to_value(decision_for(&shadow, akzio_domain::Asset::Qqq, now))
                    .unwrap(),
                created_at: now,
            })
            .unwrap();
        ledger
            .complete_shadow_pair(&shadow, candidate_decision.document_id, now)
            .unwrap();
        let market_document = broker
            .record_json(NewJsonDocument {
                kind: DocumentKind::NormalizedEvidence,
                producer: "test.market".to_owned(),
                run_id: Some(parent.clone()),
                lifecycle: DocumentLifecycle::Canonical,
                source_refs: vec![],
                origin: None,
                value: &serde_json::json!({"bars": "future"}),
                created_at: now,
            })
            .unwrap();
        let first_day = now.date_naive().succ_opt().unwrap();
        let market = OutcomeMarket {
            closes: akzio_domain::Asset::EXECUTABLE
                .into_iter()
                .map(|asset| {
                    (
                        asset,
                        (0_i64..5)
                            .map(|offset| crate::DailyClose {
                                trading_day: first_day + chrono::Duration::days(offset),
                                close: akzio_domain::MoneyMicros(101_000_000 + offset * 1_000_000),
                            })
                            .collect(),
                    )
                })
                .collect(),
        };
        assert_eq!(
            ledger
                .materialize_pending(
                    &parent,
                    &market_document.document_id,
                    &market,
                    now + chrono::Duration::days(5),
                )
                .unwrap()
                .len(),
            3
        );
        assert!(ledger
            .materialize_pending(
                &parent,
                &market_document.document_id,
                &market,
                now + chrono::Duration::days(5),
            )
            .unwrap()
            .is_empty());
        assert_eq!(
            store
                .documents_by_kind(DocumentKind::Evaluation)
                .unwrap()
                .into_iter()
                .filter(|document| document.producer == TOPOLOGY_OUTCOME_PRODUCER)
                .count(),
            3
        );
        store.verify_integrity().unwrap();
    }
}
