//! Decision and Paper execution runtime.
//!
//! This module is the only place that turns a model-produced `DecisionDraft`
//! into a canonical portfolio decision, turns that decision into an order
//! plan, and records broker-visible Paper state.  Models never receive an
//! order tool and adapters never accept an un-gated model document.

use std::collections::BTreeMap;

use akzio_context::{ContextBroker, ContextError, NewJsonDocument};
use akzio_domain::{
    content_hash_json, Asset, DecisionDraft, DecisionId, DocumentId, DocumentKind,
    DocumentLifecycle, DocumentOrigin, DocumentRecord, PortfolioDecision, Provenance, RunId,
    RunPurpose, WeightPpm, V2_SCHEMA_VERSION,
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    build_execution_plan,
    paper::{AlpacaPaper, PaperError, PaperExecution},
    AccountSnapshot, ExecutionError, ExecutionPlan, ExecutionPolicy, MoneyMicros, Quote, Target,
};

#[derive(Debug, Error)]
pub enum ExecutionRuntimeError {
    #[error(transparent)]
    Context(#[from] ContextError),
    #[error(transparent)]
    Store(#[from] akzio_store::StoreError),
    #[error(transparent)]
    Domain(#[from] akzio_domain::DomainError),
    #[error(transparent)]
    Policy(#[from] ExecutionError),
    #[error(transparent)]
    Paper(#[from] PaperError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("expected {expected:?} document, found {actual:?}")]
    WrongDocument {
        expected: DocumentKind,
        actual: DocumentKind,
    },
    #[error("decision {0} is outside its valid execution window")]
    ExpiredDecision(DecisionId),
    #[error("decision gross exposure {actual_ppm} ppm exceeds policy {limit_ppm} ppm")]
    GrossExposureExceeded { actual_ppm: u64, limit_ppm: u32 },
    #[error("decision validity window must be positive")]
    InvalidValidityWindow,
    #[error("execution commitment references a different plan hash")]
    CommitmentPlanHashMismatch,
}

pub type Result<T> = std::result::Result<T, ExecutionRuntimeError>;

/// Policy for the non-bypassable decision gate.  It is distinct from the
/// order policy because research may produce a valid target that cannot yet
/// be executed (for example, after its validity window has expired).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionGatePolicy {
    pub max_gross_weight: WeightPpm,
    pub validity_secs: i64,
}

impl Default for DecisionGatePolicy {
    fn default() -> Self {
        Self {
            max_gross_weight: WeightPpm(200_000),
            validity_secs: 60 * 60,
        }
    }
}

impl DecisionGatePolicy {
    pub fn validate(&self) -> std::result::Result<(), ExecutionRuntimeError> {
        if self.validity_secs <= 0 {
            return Err(ExecutionRuntimeError::InvalidValidityWindow);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionRuntime {
    broker: ContextBroker,
    decision_policy: DecisionGatePolicy,
    execution_policy: ExecutionPolicy,
}

/// Immutable task identity supplied by the daemon to every Rust-owned
/// decision or execution transition. It prevents gate artifacts from losing
/// the lease/attempt that produced them.
#[derive(Debug, Clone)]
pub struct ExecutionRunContext {
    pub run_id: RunId,
    pub purpose: RunPurpose,
    pub origin: DocumentOrigin,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PaperCommitment {
    schema_version: u32,
    plan_hash: akzio_domain::ContentHash,
    plan_document_id: DocumentId,
    canonical_run_id: RunId,
}

impl ExecutionRuntime {
    pub fn new(
        broker: ContextBroker,
        decision_policy: DecisionGatePolicy,
        execution_policy: ExecutionPolicy,
    ) -> Result<Self> {
        decision_policy.validate()?;
        Ok(Self {
            broker,
            decision_policy,
            execution_policy,
        })
    }

    pub fn with_defaults(broker: ContextBroker) -> Self {
        Self::new(
            broker,
            DecisionGatePolicy::default(),
            ExecutionPolicy::default(),
        )
        .expect("default execution policies are valid")
    }

    pub fn decision_policy(&self) -> &DecisionGatePolicy {
        &self.decision_policy
    }

    pub fn execution_policy(&self) -> &ExecutionPolicy {
        &self.execution_policy
    }

    /// Bind an agent draft to its immutable evidence surface and Rust policy.
    pub fn finalize_decision(
        &self,
        context: &ExecutionRunContext,
        draft_document_id: &DocumentId,
        context_manifest_id: &DocumentId,
        memory_refs: Vec<DocumentId>,
    ) -> Result<DocumentRecord> {
        let draft_document = self.broker.store().read_document(draft_document_id)?;
        if draft_document.kind != DocumentKind::DecisionDraft {
            return Err(ExecutionRuntimeError::WrongDocument {
                expected: DocumentKind::DecisionDraft,
                actual: draft_document.kind,
            });
        }
        let context_manifest = self.broker.store().read_document(context_manifest_id)?;
        if context_manifest.kind != DocumentKind::ContextManifest {
            return Err(ExecutionRuntimeError::WrongDocument {
                expected: DocumentKind::ContextManifest,
                actual: context_manifest.kind,
            });
        }
        for memory_id in &memory_refs {
            let memory = self.broker.store().read_document(memory_id)?;
            if memory.kind != DocumentKind::Memory {
                return Err(ExecutionRuntimeError::WrongDocument {
                    expected: DocumentKind::Memory,
                    actual: memory.kind,
                });
            }
        }

        let draft =
            serde_json::from_value::<DecisionDraft>(self.broker.read_json(&draft_document)?)?;
        draft.validate()?;
        let gross = draft.targets.gross_weight_ppm();
        if gross > u64::from(self.decision_policy.max_gross_weight.0) {
            return Err(ExecutionRuntimeError::GrossExposureExceeded {
                actual_ppm: gross,
                limit_ppm: self.decision_policy.max_gross_weight.0,
            });
        }

        let policy_hash = content_hash_json(&serde_json::to_value(&self.decision_policy)?)?;
        let decision = PortfolioDecision {
            schema_version: akzio_domain::V2_SCHEMA_VERSION,
            decision_id: DecisionId::new(),
            run_id: context.run_id.clone(),
            source_document_id: draft_document.document_id.clone(),
            context_manifest_id: context_manifest.document_id.clone(),
            memory_refs: memory_refs.clone(),
            policy_hash: policy_hash.clone(),
            created_at: context.now,
            valid_until: context.now + Duration::seconds(self.decision_policy.validity_secs),
            draft,
        };
        decision.validate()?;
        let lifecycle = if context.purpose.is_canonical_learning() {
            DocumentLifecycle::Canonical
        } else {
            DocumentLifecycle::RunScoped
        };
        let value = serde_json::to_value(&decision)?;
        let source_refs = std::iter::once(draft_document.document_id.clone())
            .chain(std::iter::once(context_manifest.document_id.clone()))
            .chain(memory_refs)
            .collect();
        Ok(self.broker.record_json_with_provenance(
            NewJsonDocument {
                kind: DocumentKind::Decision,
                producer: "execution.decision_gate".to_owned(),
                run_id: Some(context.run_id.clone()),
                lifecycle,
                source_refs,
                origin: Some(context.origin.clone()),
                value: &value,
                created_at: context.now,
            },
            Provenance {
                source: "akzio.execution".to_owned(),
                observed_at: Some(context.now),
                retrieved_at: context.now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                contract_hash: Some(policy_hash),
            },
        )?)
    }

    /// Re-check a finalized decision against live account and quote snapshots,
    /// then persist the exact order plan before the broker can be called.
    pub fn build_plan(
        &self,
        context: &ExecutionRunContext,
        decision_document_id: &DocumentId,
        execution_context_id: &DocumentId,
        account: &AccountSnapshot,
        quotes: &BTreeMap<Asset, Quote>,
        daily_turnover: MoneyMicros,
    ) -> Result<DocumentRecord> {
        let decision_document = self.broker.store().read_document(decision_document_id)?;
        if decision_document.kind != DocumentKind::Decision {
            return Err(ExecutionRuntimeError::WrongDocument {
                expected: DocumentKind::Decision,
                actual: decision_document.kind,
            });
        }
        let execution_context = self.broker.store().read_document(execution_context_id)?;
        if execution_context.kind != DocumentKind::ExecutionContext {
            return Err(ExecutionRuntimeError::WrongDocument {
                expected: DocumentKind::ExecutionContext,
                actual: execution_context.kind,
            });
        }
        let decision = serde_json::from_value::<PortfolioDecision>(
            self.broker.read_json(&decision_document)?,
        )?;
        decision.validate()?;
        if decision.valid_until <= context.now {
            return Err(ExecutionRuntimeError::ExpiredDecision(decision.decision_id));
        }
        let targets = Asset::EXECUTABLE
            .into_iter()
            .map(|asset| Target {
                asset,
                weight: *decision
                    .draft
                    .targets
                    .weights
                    .get(&asset)
                    .expect("validated target portfolio contains every executable asset"),
            })
            .collect::<Vec<_>>();
        let plan = build_execution_plan(
            self.execution_policy.clone(),
            account,
            quotes,
            &targets,
            daily_turnover,
            context.now,
        )?;
        self.record_plan(context, &decision_document, &execution_context, &plan)
    }

    pub async fn submit_paper(
        &self,
        context: &ExecutionRunContext,
        plan_document_id: &DocumentId,
        paper: &AlpacaPaper,
    ) -> Result<DocumentRecord> {
        let (plan_document, plan) = self.read_plan(plan_document_id)?;
        let reservation = self.broker.store().reserve_execution_commitment(
            &context.run_id,
            &plan_document.document_id,
            &plan.plan_hash,
            context.now,
        )?;
        if let Some(submission_document_id) = reservation.record.submission_document_id {
            return self.reuse_submission(
                context,
                &plan_document,
                &submission_document_id,
                &plan.plan_hash,
            );
        }
        if let Some(existing) = self.submission_state_for_plan(&plan.plan_hash)? {
            self.broker.store().mark_execution_submitted(
                &plan.plan_hash,
                &existing.document_id,
                context.now,
            )?;
            return self.reuse_submission(
                context,
                &plan_document,
                &existing.document_id,
                &plan.plan_hash,
            );
        }
        if reservation.record.commitment_document_id.is_none() {
            let commitment = self.record_paper_commitment(context, &plan_document, &plan)?;
            self.broker.store().attach_execution_commitment_document(
                &plan.plan_hash,
                &commitment.document_id,
                context.now,
            )?;
        }
        let execution = paper.execute(&plan).await?;
        let submission = self.record_paper_state(
            context,
            &plan_document,
            &execution,
            "execution.paper_submitted",
        )?;
        self.broker.store().mark_execution_submitted(
            &plan.plan_hash,
            &submission.document_id,
            context.now,
        )?;
        Ok(submission)
    }

    pub async fn reconcile_paper(
        &self,
        context: &ExecutionRunContext,
        order_state_document_id: &DocumentId,
        paper: &AlpacaPaper,
    ) -> Result<DocumentRecord> {
        let order_state = self.broker.store().read_document(order_state_document_id)?;
        if order_state.kind != DocumentKind::OrderState {
            return Err(ExecutionRuntimeError::WrongDocument {
                expected: DocumentKind::OrderState,
                actual: order_state.kind,
            });
        }
        let execution =
            serde_json::from_value::<PaperExecution>(self.broker.read_json(&order_state)?)?;
        let reconciled = paper.reconcile(&execution).await?;
        let reconciliation = self.record_paper_state(
            context,
            &order_state,
            &reconciled,
            "execution.paper_reconciled",
        )?;
        self.broker.store().mark_execution_reconciled(
            &execution.plan_hash,
            &reconciliation.document_id,
            context.now,
        )?;
        Ok(reconciliation)
    }

    fn reuse_submission(
        &self,
        context: &ExecutionRunContext,
        plan_document: &DocumentRecord,
        submission_document_id: &DocumentId,
        plan_hash: &akzio_domain::ContentHash,
    ) -> Result<DocumentRecord> {
        let submission = self.broker.store().read_document(submission_document_id)?;
        if submission.kind != DocumentKind::OrderState {
            return Err(ExecutionRuntimeError::WrongDocument {
                expected: DocumentKind::OrderState,
                actual: submission.kind,
            });
        }
        if submission.run_id.as_ref() == Some(&context.run_id) {
            return Ok(submission);
        }
        let execution =
            serde_json::from_value::<PaperExecution>(self.broker.read_json(&submission)?)?;
        if execution.plan_hash != *plan_hash {
            return Err(ExecutionRuntimeError::CommitmentPlanHashMismatch);
        }
        self.record_paper_state_with_sources(
            context,
            vec![plan_document.document_id.clone(), submission.document_id],
            &execution,
            "execution.paper_reused_commitment",
        )
    }

    fn submission_state_for_plan(
        &self,
        plan_hash: &akzio_domain::ContentHash,
    ) -> Result<Option<DocumentRecord>> {
        let mut states = self
            .broker
            .store()
            .documents_by_kind(DocumentKind::OrderState)?
            .into_iter()
            .filter(|document| document.producer == "execution.paper_submitted")
            .filter_map(|document| {
                serde_json::from_value::<PaperExecution>(self.broker.read_json(&document).ok()?)
                    .ok()
                    .filter(|execution| execution.plan_hash == *plan_hash)
                    .map(|_| document)
            })
            .collect::<Vec<_>>();
        states.sort_by(|left, right| {
            (left.created_at, &left.document_id).cmp(&(right.created_at, &right.document_id))
        });
        Ok(states.into_iter().next())
    }

    fn read_plan(&self, document_id: &DocumentId) -> Result<(DocumentRecord, ExecutionPlan)> {
        let document = self.broker.store().read_document(document_id)?;
        if document.kind != DocumentKind::ExecutionPlan {
            return Err(ExecutionRuntimeError::WrongDocument {
                expected: DocumentKind::ExecutionPlan,
                actual: document.kind,
            });
        }
        let plan = serde_json::from_value::<ExecutionPlan>(self.broker.read_json(&document)?)?;
        Ok((document, plan))
    }

    fn record_plan(
        &self,
        context: &ExecutionRunContext,
        decision_document: &DocumentRecord,
        execution_context: &DocumentRecord,
        plan: &ExecutionPlan,
    ) -> Result<DocumentRecord> {
        let value = serde_json::to_value(plan)?;
        Ok(self.broker.record_json_with_provenance(
            NewJsonDocument {
                kind: DocumentKind::ExecutionPlan,
                producer: "execution.gate".to_owned(),
                run_id: Some(context.run_id.clone()),
                lifecycle: DocumentLifecycle::RunScoped,
                source_refs: vec![
                    decision_document.document_id.clone(),
                    execution_context.document_id.clone(),
                ],
                origin: Some(context.origin.clone()),
                value: &value,
                created_at: context.now,
            },
            Provenance {
                source: "akzio.execution".to_owned(),
                observed_at: Some(context.now),
                retrieved_at: context.now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                contract_hash: Some(plan.plan_hash.clone()),
            },
        )?)
    }

    fn record_paper_commitment(
        &self,
        context: &ExecutionRunContext,
        plan_document: &DocumentRecord,
        plan: &ExecutionPlan,
    ) -> Result<DocumentRecord> {
        let value = serde_json::to_value(PaperCommitment {
            schema_version: V2_SCHEMA_VERSION,
            plan_hash: plan.plan_hash.clone(),
            plan_document_id: plan_document.document_id.clone(),
            canonical_run_id: context.run_id.clone(),
        })?;
        Ok(self.broker.record_json_with_provenance(
            NewJsonDocument {
                kind: DocumentKind::ExecutionCommitment,
                producer: "execution.paper_commitment".to_owned(),
                run_id: Some(context.run_id.clone()),
                lifecycle: DocumentLifecycle::Canonical,
                source_refs: vec![plan_document.document_id.clone()],
                origin: Some(context.origin.clone()),
                value: &value,
                created_at: context.now,
            },
            Provenance {
                source: "akzio.execution".to_owned(),
                observed_at: Some(context.now),
                retrieved_at: context.now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                contract_hash: Some(plan.plan_hash.clone()),
            },
        )?)
    }

    fn record_paper_state(
        &self,
        context: &ExecutionRunContext,
        source: &DocumentRecord,
        execution: &PaperExecution,
        producer: &str,
    ) -> Result<DocumentRecord> {
        self.record_paper_state_with_sources(
            context,
            vec![source.document_id.clone()],
            execution,
            producer,
        )
    }

    fn record_paper_state_with_sources(
        &self,
        context: &ExecutionRunContext,
        source_refs: Vec<DocumentId>,
        execution: &PaperExecution,
        producer: &str,
    ) -> Result<DocumentRecord> {
        let value = serde_json::to_value(execution)?;
        Ok(self.broker.record_json_with_provenance(
            NewJsonDocument {
                kind: DocumentKind::OrderState,
                producer: producer.to_owned(),
                run_id: Some(context.run_id.clone()),
                lifecycle: DocumentLifecycle::RunScoped,
                source_refs,
                origin: Some(context.origin.clone()),
                value: &value,
                created_at: context.now,
            },
            Provenance {
                source: "alpaca.paper".to_owned(),
                observed_at: Some(context.now),
                retrieved_at: context.now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                contract_hash: Some(execution.plan_hash.clone()),
            },
        )?)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use akzio_context::ContextBroker;
    use akzio_domain::{
        Asset, DecisionDraft, DocumentKind, DocumentLifecycle, DocumentOrigin, HorizonForecast,
        RunId, RunPurpose, TargetPortfolio, WeightPpm,
    };
    use akzio_store::V2Store;
    use chrono::Utc;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn decision_gate_binds_a_typed_draft_to_a_context_manifest() {
        let directory = tempdir().unwrap();
        let broker = ContextBroker::new(V2Store::open(directory.path()).unwrap());
        let now = Utc::now();
        let run = RunId::new();
        broker
            .store()
            .create_run(&run, RunPurpose::Debug, "test", now)
            .unwrap();
        let draft = DecisionDraft {
            summary: "hold cash while evidence is incomplete".to_owned(),
            targets: TargetPortfolio {
                weights: BTreeMap::from([
                    (Asset::Tqqq, WeightPpm::ZERO),
                    (Asset::Qqq, WeightPpm::ZERO),
                    (Asset::Soxx, WeightPpm::ZERO),
                    (Asset::Soxl, WeightPpm::ZERO),
                ]),
            },
            confidence_ppm: 500_000,
            forecasts: vec![
                HorizonForecast {
                    trading_days: 1,
                    positive_return_probability_ppm: 500_000,
                    expected_return_ppm: 0,
                },
                HorizonForecast {
                    trading_days: 3,
                    positive_return_probability_ppm: 500_000,
                    expected_return_ppm: 0,
                },
                HorizonForecast {
                    trading_days: 5,
                    positive_return_probability_ppm: 500_000,
                    expected_return_ppm: 0,
                },
            ],
            blockers: vec!["fixture".to_owned()],
            claim_refs: vec![],
        };
        let draft_value = serde_json::to_value(draft).unwrap();
        let draft_document = broker
            .record_json(NewJsonDocument {
                kind: DocumentKind::DecisionDraft,
                producer: "synthesizer.decision".to_owned(),
                run_id: Some(run.clone()),
                lifecycle: DocumentLifecycle::RunScoped,
                source_refs: vec![],
                origin: None,
                value: &draft_value,
                created_at: now,
            })
            .unwrap();
        let manifest_value = serde_json::json!({"documents": []});
        let manifest = broker
            .record_json(NewJsonDocument {
                kind: DocumentKind::ContextManifest,
                producer: "context.fixture".to_owned(),
                run_id: Some(run.clone()),
                lifecycle: DocumentLifecycle::RunScoped,
                source_refs: vec![],
                origin: None,
                value: &manifest_value,
                created_at: now,
            })
            .unwrap();

        let runtime = ExecutionRuntime::with_defaults(broker.clone());
        let context = ExecutionRunContext {
            run_id: run.clone(),
            purpose: RunPurpose::Debug,
            origin: DocumentOrigin {
                task_id: None,
                attempt_id: None,
                contract_hash: None,
            },
            now,
        };
        let decision = runtime
            .finalize_decision(
                &context,
                &draft_document.document_id,
                &manifest.document_id,
                vec![],
            )
            .unwrap();
        assert_eq!(decision.kind, DocumentKind::Decision);
        assert_eq!(decision.lifecycle, DocumentLifecycle::RunScoped);
        assert_eq!(decision.source_refs.len(), 2);
        assert_eq!(decision.origin, Some(context.origin));
    }

    #[derive(Clone, Default)]
    struct MockPaper {
        orders:
            std::sync::Arc<std::sync::Mutex<std::collections::BTreeMap<String, serde_json::Value>>>,
        submissions: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    #[derive(serde::Deserialize)]
    struct ClientOrderQuery {
        client_order_id: String,
    }

    async fn mock_clock() -> axum::Json<serde_json::Value> {
        axum::Json(serde_json::json!({
            "is_open": true,
            "timestamp": "2026-08-06T10:00:00-04:00",
        }))
    }

    async fn mock_lookup(
        axum::extract::State(state): axum::extract::State<MockPaper>,
        axum::extract::Query(query): axum::extract::Query<ClientOrderQuery>,
    ) -> std::result::Result<axum::Json<serde_json::Value>, axum::http::StatusCode> {
        state
            .orders
            .lock()
            .unwrap()
            .get(&query.client_order_id)
            .cloned()
            .map(axum::Json)
            .ok_or(axum::http::StatusCode::NOT_FOUND)
    }

    async fn mock_submit(
        axum::extract::State(state): axum::extract::State<MockPaper>,
        axum::Json(body): axum::Json<serde_json::Value>,
    ) -> axum::Json<serde_json::Value> {
        let number = state
            .submissions
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let client_order_id = body["client_order_id"].as_str().unwrap().to_owned();
        let receipt = serde_json::json!({
            "id": format!("broker-{number}"),
            "symbol": body["symbol"],
            "status": "accepted",
            "client_order_id": client_order_id,
        });
        state
            .orders
            .lock()
            .unwrap()
            .insert(client_order_id, receipt.clone());
        axum::Json(receipt)
    }

    async fn mock_order(
        axum::extract::State(state): axum::extract::State<MockPaper>,
        axum::extract::Path(order_id): axum::extract::Path<String>,
    ) -> std::result::Result<axum::Json<serde_json::Value>, axum::http::StatusCode> {
        state
            .orders
            .lock()
            .unwrap()
            .values()
            .find(|order| order["id"] == order_id)
            .cloned()
            .map(axum::Json)
            .ok_or(axum::http::StatusCode::NOT_FOUND)
    }

    fn paper_plan() -> ExecutionPlan {
        ExecutionPlan {
            policy: ExecutionPolicy::default(),
            targets: vec![],
            orders: vec![crate::OrderIntent {
                asset: Asset::Tqqq,
                side: crate::OrderSide::Buy,
                notional: MoneyMicros::from_usd_cents(10_000),
                limit_price: MoneyMicros::from_usd_cents(2_500),
            }],
            plan_hash: akzio_domain::ContentHash::of_bytes(b"paper-commitment-retry"),
        }
    }

    fn paper_context(run_id: RunId, now: chrono::DateTime<Utc>) -> ExecutionRunContext {
        ExecutionRunContext {
            run_id,
            purpose: RunPurpose::Paper,
            origin: DocumentOrigin::task(
                akzio_domain::TaskId::new(),
                akzio_domain::AttemptId::new(),
                None,
            ),
            now,
        }
    }

    #[tokio::test]
    async fn paper_commitment_recovers_prepared_work_and_reuses_cross_run_submission() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        let broker = ContextBroker::new(store.clone());
        let now = Utc::now();
        let first_run = RunId::new();
        store
            .create_run(&first_run, RunPurpose::Paper, "test", now)
            .unwrap();

        let plan = paper_plan();
        let plan_value = serde_json::to_value(&plan).unwrap();
        let first_plan = broker
            .record_json(NewJsonDocument {
                kind: DocumentKind::ExecutionPlan,
                producer: "test.execution_plan".to_owned(),
                run_id: Some(first_run.clone()),
                lifecycle: DocumentLifecycle::RunScoped,
                source_refs: vec![],
                origin: None,
                value: &plan_value,
                created_at: now,
            })
            .unwrap();

        let reserved = store
            .reserve_execution_commitment(&first_run, &first_plan.document_id, &plan.plan_hash, now)
            .unwrap();
        assert!(reserved.newly_reserved);

        let mock = MockPaper::default();
        let app = axum::Router::new()
            .route("/v2/clock", axum::routing::get(mock_clock))
            .route(
                "/v2/orders:by_client_order_id",
                axum::routing::get(mock_lookup),
            )
            .route("/v2/orders", axum::routing::post(mock_submit))
            .route("/v2/orders/{order_id}", axum::routing::get(mock_order))
            .with_state(mock.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let paper = AlpacaPaper::new(
            format!("http://{address}"),
            crate::paper::PaperCredentials {
                key_id: "key".to_owned(),
                secret_key: "secret".to_owned(),
            },
        )
        .unwrap();
        let runtime = ExecutionRuntime::with_defaults(broker.clone());
        let first_context = paper_context(first_run.clone(), now);

        let submission = runtime
            .submit_paper(&first_context, &first_plan.document_id, &paper)
            .await
            .unwrap();
        assert_eq!(
            mock.submissions.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            store
                .execution_commitment(&plan.plan_hash)
                .unwrap()
                .unwrap()
                .state,
            akzio_store::ExecutionCommitmentState::Submitted
        );

        let retry = runtime
            .submit_paper(&first_context, &first_plan.document_id, &paper)
            .await
            .unwrap();
        assert_eq!(retry.document_id, submission.document_id);
        assert_eq!(
            mock.submissions.load(std::sync::atomic::Ordering::SeqCst),
            1
        );

        let second_run = RunId::new();
        store
            .create_run(&second_run, RunPurpose::Paper, "test", now)
            .unwrap();
        let second_plan = broker
            .record_json(NewJsonDocument {
                kind: DocumentKind::ExecutionPlan,
                producer: "test.execution_plan".to_owned(),
                run_id: Some(second_run.clone()),
                lifecycle: DocumentLifecycle::RunScoped,
                source_refs: vec![],
                origin: None,
                value: &plan_value,
                created_at: now,
            })
            .unwrap();
        let reused = runtime
            .submit_paper(
                &paper_context(second_run.clone(), now),
                &second_plan.document_id,
                &paper,
            )
            .await
            .unwrap();
        assert_eq!(reused.run_id, Some(second_run));
        assert_eq!(reused.producer, "execution.paper_reused_commitment");
        assert_eq!(
            mock.submissions.load(std::sync::atomic::Ordering::SeqCst),
            1
        );

        runtime
            .reconcile_paper(&first_context, &submission.document_id, &paper)
            .await
            .unwrap();
        assert_eq!(
            store
                .execution_commitment(&plan.plan_hash)
                .unwrap()
                .unwrap()
                .state,
            akzio_store::ExecutionCommitmentState::Reconciled
        );
        server.abort();
    }
}
