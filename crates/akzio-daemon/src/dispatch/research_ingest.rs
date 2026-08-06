//! Sealed research input and outcome-market projection.

use akzio_context::{ContextBroker, NewJsonDocument};
use akzio_domain::{Asset, DocumentKind, DocumentLifecycle, RunPurpose};
use akzio_ingest::{IngestConfig, Ingestor};
use akzio_learning::{DailyClose, LearningLedger, OutcomeMarket, TopologyLedger};
use akzio_store::ClaimedTask;
use chrono::{DateTime, NaiveDate, Utc};
use serde_json::Value;

use super::{task_origin, value::parse_money};
use crate::{Daemon, DaemonError, Result};

impl Daemon {
    pub(super) async fn seal_research_input(
        &self,
        broker: &ContextBroker,
        task: &ClaimedTask,
        purpose: RunPurpose,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let run_id = &task.run_id;
        if purpose == RunPurpose::Shadow {
            let source = self
                .store
                .task_input_refs(&task.task_id)?
                .into_iter()
                .filter_map(|document_id| self.store.read_document(&document_id).ok())
                .find(|document| document.kind == DocumentKind::NormalizedEvidence)
                .ok_or_else(|| {
                    DaemonError::InvalidInput(
                        "shadow run is missing its sealed normalized input".to_owned(),
                    )
                })?;
            let value = broker.read_json(&source)?;
            broker.record_json_with_provenance(
                NewJsonDocument {
                    kind: DocumentKind::NormalizedEvidence,
                    producer: "ingest.shadow_input".to_owned(),
                    run_id: Some(run_id.clone()),
                    lifecycle: DocumentLifecycle::RunScoped,
                    source_refs: vec![source.document_id],
                    origin: Some(task_origin(task)),
                    value: &value,
                    created_at: now,
                },
                akzio_domain::Provenance {
                    source: "akzio.shadow_input".to_owned(),
                    observed_at: source.provenance.observed_at,
                    retrieved_at: now,
                    source_uri: None,
                    confidence_ppm: source.provenance.confidence_ppm,
                    contract_hash: None,
                },
            )?;
            return Ok(());
        }
        if matches!(purpose, RunPurpose::Paper | RunPurpose::PaperDryRun) {
            let sealed = Ingestor::from_env(IngestConfig::default())?
                .seal(broker, run_id, task_origin(task), now)
                .await?;
            if purpose == RunPurpose::Paper {
                let input = broker.read_json(&sealed.normalized)?;
                let market = outcome_market(&input);
                let origin = task_origin(task);
                let outcomes = LearningLedger::for_task(broker.clone(), origin.clone())
                    .materialize_pending(
                        purpose,
                        run_id,
                        &sealed.normalized.document_id,
                        &market,
                        now,
                    )?;
                let topology_outcomes = TopologyLedger::for_task(broker.clone(), origin.clone())
                    .materialize_pending(run_id, &sealed.normalized.document_id, &market, now)?;
                if !outcomes.is_empty() || !topology_outcomes.is_empty() {
                    let mut source_refs = outcomes
                        .iter()
                        .map(|document| document.document_id.clone())
                        .collect::<Vec<_>>();
                    source_refs.extend(
                        topology_outcomes
                            .iter()
                            .map(|document| document.document_id.clone()),
                    );
                    let summary = broker.record_json(NewJsonDocument {
                        kind: DocumentKind::Evaluation,
                        producer: "learning.outcome_materializer".to_owned(),
                        run_id: Some(run_id.clone()),
                        lifecycle: DocumentLifecycle::RunScoped,
                        source_refs,
                        origin: Some(origin),
                        value: &serde_json::json!({
                            "kind": "outcomes_materialized",
                            "memory_count": outcomes.len(),
                            "topology_count": topology_outcomes.len(),
                        }),
                        created_at: now,
                    })?;
                    if !topology_outcomes.is_empty() {
                        self.store.append_event(&akzio_domain::EventEnvelope {
                            schema_version: akzio_domain::V2_SCHEMA_VERSION,
                            run_id: run_id.clone(),
                            task_id: Some(task.task_id.clone()),
                            attempt_id: Some(task.attempt_id.clone()),
                            contract_hash: task.contract_hash.clone(),
                            causation_id: None,
                            event_type: "topology.outcomes_materialized".to_owned(),
                            payload_document_id: Some(summary.document_id.clone()),
                            payload: Some(summary.blob),
                            created_at: now,
                        })?;
                    }
                }
            }
            return Ok(());
        }
        broker.record_json(NewJsonDocument {
            kind: DocumentKind::NormalizedEvidence,
            producer: "ingest.fixture".to_owned(),
            run_id: Some(run_id.clone()),
            lifecycle: DocumentLifecycle::RunScoped,
            source_refs: vec![],
            origin: Some(task_origin(task)),
            value: &serde_json::json!({
                "schema_version": 1,
                "kind": "debug_market_input",
                "assets": ["TQQQ", "QQQ", "SOXX", "SOXL"],
                "observed_at": now,
                "mode": "synthetic"
            }),
            created_at: now,
        })?;
        Ok(())
    }
}

fn outcome_market(input: &Value) -> OutcomeMarket {
    let closes = Asset::EXECUTABLE
        .into_iter()
        .map(|asset| {
            let bars = input
                .pointer(&format!("/market/{}/bars", asset.symbol()))
                .and_then(|value| {
                    value
                        .get("bars")
                        .or_else(|| value.pointer("/data/bars"))
                        .or_else(|| value.as_array().map(|_| value))
                })
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|bar| {
                    let day = bar
                        .get("t")
                        .or_else(|| bar.get("timestamp"))
                        .or_else(|| bar.get("time"))
                        .and_then(Value::as_str)
                        .and_then(parse_trading_day)?;
                    let close = parse_money(
                        bar.get("c")
                            .or_else(|| bar.get("close"))
                            .unwrap_or(&Value::Null),
                    )
                    .ok()?;
                    (close.0 > 0).then_some(DailyClose {
                        trading_day: day,
                        close,
                    })
                })
                .collect::<Vec<_>>();
            (asset, bars)
        })
        .collect();
    OutcomeMarket { closes }
}

fn parse_trading_day(value: &str) -> Option<NaiveDate> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.date_naive())
        .or_else(|| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok())
}
