//! Final transport guard: a logging failure after a task was claimed may not
//! turn an already durable Commitment into a new broker submission.

use std::{future::Future, pin::Pin, sync::Arc};

use akzio_domain::{PaperCancel, PaperCommitment, PaperReprice};
use akzio_execution::{
    paper::{
        CommittedPaperBroker, PaperError, PaperExecution, PaperOrderReceipt,
        PaperSubmissionAuthorization,
    },
    ExecutionPlan,
};
use serde_json::json;

use crate::RunLogger;

pub(crate) struct AuditedPaperBroker {
    inner: Arc<dyn CommittedPaperBroker>,
    logger: Arc<RunLogger>,
}

impl AuditedPaperBroker {
    pub(crate) fn new(inner: Arc<dyn CommittedPaperBroker>, logger: Arc<RunLogger>) -> Self {
        Self { inner, logger }
    }

    fn before_write(&self, kind: &str) -> akzio_execution::paper::Result<()> {
        let context = akzio_model::MODEL_AUDIT_CONTEXT
            .try_with(Clone::clone)
            .map_err(|_| PaperError::RunLogUnavailable("missing task audit scope".into()))?;
        if matches!(
            kind,
            "rust.broker.submit_attempt" | "rust.broker.replace_attempt"
        ) && self
            .logger
            .run_cancel_requested(&context.run_id)
            .map_err(|error| PaperError::RunLogUnavailable(error.to_string()))?
        {
            return Err(PaperError::RunCancelled);
        }
        self.logger
            .record(&context, kind, json!({"status":"before_broker_io"}))
            .map_err(|error| PaperError::RunLogUnavailable(error.to_string()))
    }
}

impl CommittedPaperBroker for AuditedPaperBroker {
    fn observe_existing_commitment<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
    ) -> Pin<Box<dyn Future<Output = akzio_execution::paper::Result<PaperExecution>> + Send + 'a>>
    {
        self.inner.observe_existing_commitment(commitment)
    }
    fn execute_commitment<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        plan: &'a ExecutionPlan,
        authorization: &'a PaperSubmissionAuthorization,
    ) -> Pin<Box<dyn Future<Output = akzio_execution::paper::Result<PaperExecution>> + Send + 'a>>
    {
        Box::pin(async move {
            self.before_write("rust.broker.submit_attempt")?;
            self.inner
                .execute_commitment(commitment, plan, authorization)
                .await
        })
    }

    fn reconcile_commitment<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        execution: &'a PaperExecution,
    ) -> Pin<Box<dyn Future<Output = akzio_execution::paper::Result<PaperExecution>> + Send + 'a>>
    {
        self.inner.reconcile_commitment(commitment, execution)
    }

    fn cancel_order<'a>(
        &'a self,
        intent: &'a PaperCancel,
    ) -> Pin<Box<dyn Future<Output = akzio_execution::paper::Result<PaperOrderReceipt>> + Send + 'a>>
    {
        Box::pin(async move {
            // Only an existing Rust/Store-vetted cancel intent reaches this
            // method; a broken logger may not prevent protective cancellation.
            let uncertain = akzio_model::MODEL_AUDIT_CONTEXT
                .try_with(|scope| self.logger.is_uncertain_run(&scope.run_id))
                .unwrap_or(false);
            if self.logger.assert_available().is_ok() && !uncertain {
                self.before_write("rust.broker.cancel_attempt")?;
            }
            self.inner.cancel_order(intent).await
        })
    }

    fn replace_order<'a>(
        &'a self,
        intent: &'a PaperReprice,
        authorization: &'a PaperSubmissionAuthorization,
    ) -> Pin<Box<dyn Future<Output = akzio_execution::paper::Result<PaperOrderReceipt>> + Send + 'a>>
    {
        Box::pin(async move {
            self.before_write("rust.broker.replace_attempt")?;
            self.inner.replace_order(intent, authorization).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use akzio_domain::{
        Artifact, ArtifactId, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance,
        ArtifactRef, Asset, ContentHash, FailureDisposition, PaperCancelId, PaperCancelReason,
        PaperCommitmentId, RetryPolicy, RunId, RunPurpose, TaskBudget, TaskId, TaskRecipeId,
        WorkflowGraph, WorkflowNode, DOMAIN_SCHEMA_VERSION,
    };
    use akzio_store::{Store, StoredRun, WorkflowCommit};
    use chrono::Utc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct ExistingOrders {
        reads: AtomicUsize,
        cancels: AtomicUsize,
    }

    impl CommittedPaperBroker for ExistingOrders {
        fn observe_existing_commitment<'a>(
            &'a self,
            commitment: &'a PaperCommitment,
        ) -> Pin<Box<dyn Future<Output = akzio_execution::paper::Result<PaperExecution>> + Send + 'a>>
        {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                Ok(PaperExecution {
                    plan_hash: commitment.plan_hash.clone(),
                    orders: vec![],
                })
            })
        }

        fn execute_commitment<'a>(
            &'a self,
            _: &'a PaperCommitment,
            _: &'a ExecutionPlan,
            _: &'a PaperSubmissionAuthorization,
        ) -> Pin<Box<dyn Future<Output = akzio_execution::paper::Result<PaperExecution>> + Send + 'a>>
        {
            panic!("logging failure must prevent new orders")
        }

        fn reconcile_commitment<'a>(
            &'a self,
            _: &'a PaperCommitment,
            _: &'a PaperExecution,
        ) -> Pin<Box<dyn Future<Output = akzio_execution::paper::Result<PaperExecution>> + Send + 'a>>
        {
            panic!("this test only observes existing IDs")
        }

        fn cancel_order<'a>(
            &'a self,
            intent: &'a PaperCancel,
        ) -> Pin<
            Box<dyn Future<Output = akzio_execution::paper::Result<PaperOrderReceipt>> + Send + 'a>,
        > {
            self.cancels.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                Ok(PaperOrderReceipt {
                    client_order_id: intent.client_order_id.clone(),
                    broker_order_id: intent.broker_order_id.clone(),
                    symbol: intent.asset.symbol().into(),
                    status: "canceled".into(),
                    requested_quantity_micros: 1_000_000,
                    filled_quantity_micros: 0,
                    remaining_quantity_micros: 1_000_000,
                    average_fill_price: None,
                    broker_updated_at: Utc::now(),
                    reason: None,
                    reused: false,
                    reprice_count: 0,
                })
            })
        }

        fn replace_order<'a>(
            &'a self,
            _: &'a PaperReprice,
            _: &'a PaperSubmissionAuthorization,
        ) -> Pin<
            Box<dyn Future<Output = akzio_execution::paper::Result<PaperOrderReceipt>> + Send + 'a>,
        > {
            panic!("logging failure must prevent repricing")
        }
    }

    #[tokio::test]
    async fn unavailable_log_still_permits_observation_and_existing_protective_cancel() {
        let root = std::fs::canonicalize(std::env::temp_dir())
            .unwrap()
            .join(format!(
                "akzio-audited-broker-test-{}",
                uuid::Uuid::new_v4()
            ));
        let logger = RunLogger::for_test(root.join("log"));
        logger.mark_unavailable("simulated sync failure").unwrap();
        let inner = Arc::new(ExistingOrders {
            reads: AtomicUsize::new(0),
            cancels: AtomicUsize::new(0),
        });
        let guarded = AuditedPaperBroker::new(inner.clone(), logger);
        let hash = ContentHash::of_bytes(b"paper safety plan");
        let commitment = PaperCommitment {
            commitment_id: PaperCommitmentId::new(),
            execution_context: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::of_bytes(b"context")),
                kind: ArtifactKind::ExecutionContext,
            },
            plan_hash: hash,
            broker_session: "2026-09-30".into(),
            client_order_ids: [(Asset::Qqq, "existing-r0".into())].into(),
            created_at: Utc::now(),
        };
        assert!(matches!(
            guarded.before_write("rust.broker.submit_attempt"),
            Err(PaperError::RunLogUnavailable(_))
        ));
        assert_eq!(
            guarded
                .observe_existing_commitment(&commitment)
                .await
                .unwrap()
                .plan_hash,
            commitment.plan_hash
        );
        let cancel = PaperCancel {
            schema_version: DOMAIN_SCHEMA_VERSION,
            cancel_id: PaperCancelId::new(),
            commitment: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::of_bytes(b"commitment")),
                kind: ArtifactKind::ExecutionCommitment,
            },
            prior_receipt: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::of_bytes(b"receipt")),
                kind: ArtifactKind::OrderReceipt,
            },
            asset: Asset::Qqq,
            client_order_id: "existing-r0".into(),
            broker_order_id: "broker-existing".into(),
            reason: PaperCancelReason::RiskReduction,
            created_at: Utc::now(),
        };
        assert_eq!(
            guarded.cancel_order(&cancel).await.unwrap().status,
            "canceled"
        );
        assert_eq!(inner.reads.load(Ordering::SeqCst), 1);
        assert_eq!(inner.cancels.load(Ordering::SeqCst), 1);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cancelling_a_run_prevents_new_broker_orders_before_the_adapter_is_called() {
        let root = std::fs::canonicalize(std::env::temp_dir())
            .unwrap()
            .join(format!(
                "akzio-cancelled-broker-test-{}",
                uuid::Uuid::new_v4()
            ));
        let store = Store::open(root.join("store")).unwrap();
        let now = Utc::now();
        let run = RunId::new();
        let node = WorkflowNode {
            spec: None,
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new("gate.reconcile").unwrap(),
            contract_hash: None,
            objective: "offline cancelled broker guard fixture".into(),
            dependencies: vec![],
            input_artifacts: vec![],
            priority: 1,
            budget: TaskBudget {
                max_input_tokens: 1,
                max_output_tokens: 1,
                max_wall_time_secs: 1,
                max_tool_calls: akzio_domain::budget::ToolCallLimit::Limited(0),
            },
            retry: RetryPolicy::none(),
            on_failure: FailureDisposition::FailRun,
            parent_task_id: None,
        };
        let graph = WorkflowGraph {
            definition_version: None,
            schema_version: DOMAIN_SCHEMA_VERSION,
            topology_id: "cancelled-broker-guard".into(),
            nodes: vec![node.clone()],
            agent_budgets: Default::default(),
        };
        let artifact = Artifact::new(
            ArtifactKind::WorkflowGraph,
            store.stage_json(&graph).unwrap(),
            "runtime.workflow",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.runtime".into(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            Some(ArtifactOrigin {
                run_id: Some(run.clone()),
                task_id: None,
                attempt_id: None,
                contract_hash: None,
            }),
            vec![],
            now,
        )
        .unwrap();
        store
            .commit_workflow(&WorkflowCommit {
                run: StoredRun {
                    run_id: run.clone(),
                    purpose: RunPurpose::Paper,
                    topology_id: graph.topology_id,
                    graph_artifact_id: artifact.artifact_id.clone(),
                    created_at: now,
                },
                graph: artifact,
                nodes: vec![node],
            })
            .unwrap();
        let logger = RunLogger::for_test(root.join("log"));
        logger.attach_store(store.clone()).unwrap();
        assert!(store
            .request_run_cancel(&run, "operator cancelled before write", now)
            .unwrap());
        let inner = Arc::new(ExistingOrders {
            reads: AtomicUsize::new(0),
            cancels: AtomicUsize::new(0),
        });
        let guarded = AuditedPaperBroker::new(inner.clone(), logger);
        let scope = akzio_model::ModelAuditContext {
            run_id: run.0,
            phase: "T0".into(),
            task_id: None,
            attempt_id: None,
            call_id: None,
            retry_index: None,
        };
        akzio_model::MODEL_AUDIT_CONTEXT
            .scope(scope, async {
                assert!(matches!(
                    guarded.before_write("rust.broker.submit_attempt"),
                    Err(PaperError::RunCancelled)
                ));
                assert!(matches!(
                    guarded.before_write("rust.broker.replace_attempt"),
                    Err(PaperError::RunCancelled)
                ));
            })
            .await;
        assert_eq!(inner.reads.load(Ordering::SeqCst), 0);
        assert_eq!(inner.cancels.load(Ordering::SeqCst), 0);
        std::fs::remove_dir_all(root).unwrap();
    }
}
