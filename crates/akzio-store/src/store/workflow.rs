// 文件导读：Workflow 子模块负责 Contract 选择、Run/Task 创建、Attempt 输出和查询；
// graph Artifact、task 行、依赖关系与事件必须在同一提交边界内保持可重放。
// 这里的 `include!` 将六个源码片段在编译期展开到同一模块，不是六份独立的 Store 状态；
// 因而子文件内的私有 helper 可被这些片段互相调用。先读 contracts/commits
// 理解图与 Run 如何安装，再读 tasks/outputs/queries 跟踪领取、提交和重建快照。
use super::*;

include!("workflow/contracts.rs");
include!("workflow/commits.rs");
include!("workflow/tasks.rs");
include!("workflow/outputs.rs");
include!("workflow/queries.rs");
include!("workflow/helpers.rs");
#[cfg(test)]
mod retired_debug_execution_tests {
    use super::*;
    use akzio_domain::{
        ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance, FailureDisposition, RetryPolicy,
        TaskBudget, TaskRecipeId, WorkflowGraph, DOMAIN_SCHEMA_VERSION,
    };

    fn prepared_formal_fixture() -> (Store, WorkflowCommit, PathBuf) {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/retired-debug-execution-tests")
            .join(RunId::new().0);
        let store = Store::open(&root).unwrap();
        let workflow = formal_workflow(&store, RetryPolicy::none());
        (store, workflow, root)
    }

    fn formal_workflow(store: &Store, retry: RetryPolicy) -> WorkflowCommit {
        let now = Utc::now();
        let run_id = RunId::new();
        let node = WorkflowNode {
            spec: None,
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new("gate.reconcile").unwrap(),
            contract_hash: None,
            objective: "offline historical identity guard".into(),
            dependencies: vec![],
            input_artifacts: vec![],
            priority: 1,
            budget: TaskBudget {
                max_input_tokens: 1,
                max_output_tokens: 1,
                max_wall_time_secs: 1,
                max_tool_calls: akzio_domain::budget::ToolCallLimit::Limited(0),
            },
            retry,
            on_failure: FailureDisposition::FailRun,
            parent_task_id: None,
        };
        let graph = WorkflowGraph {
            definition_version: None,
            schema_version: DOMAIN_SCHEMA_VERSION,
            topology_id: "retired-debug-execution-test".into(),
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
                run_id: Some(run_id.clone()),
                task_id: None,
                attempt_id: None,
                contract_hash: None,
            }),
            vec![],
            now,
        )
        .unwrap();
        WorkflowCommit {
            run: StoredRun {
                run_id,
                purpose: RunPurpose::Paper,
                topology_id: graph.topology_id,
                graph_artifact_id: artifact.artifact_id.clone(),
                created_at: now,
            },
            graph: artifact,
            nodes: vec![node],
        }
    }

    fn formal_fixture() -> (Store, RunId, ArtifactId, PathBuf) {
        let (store, workflow, root) = prepared_formal_fixture();
        store.commit_workflow(&workflow).unwrap();
        (
            store,
            workflow.run.run_id,
            workflow.run.graph_artifact_id,
            root,
        )
    }

    /// WHY THIS TEST EXISTS (Task #25): A freeze committed by another Store
    /// after the earlier admission read must prevent new action intents while
    /// allowing exact recovery of intents already committed before freeze.
    #[test]
    fn paper_freeze_blocks_new_order_actions_and_preserves_existing_intents() {
        use akzio_domain::{
            MoneyMicros, OrderReceiptState, PaperCancelId, PaperCancelReason, PaperCommitmentId,
            PaperRepriceId,
        };

        let (store, _, _, root) = formal_fixture();
        let operator = Store::open(&root).unwrap();
        let now = Utc::now();
        let next_second = Duration::seconds(1);
        let lease = store
            .acquire_daemon_lease(
                "scheduler",
                "order-action-test",
                now,
                now + Duration::minutes(1),
            )
            .unwrap()
            .unwrap();
        let permit = store
            .claim_next_task_for_workload(
                "worker",
                now,
                Duration::minutes(1),
                TaskWorkload::Session,
            )
            .unwrap()
            .unwrap()
            .permit;
        let provenance = ArtifactProvenance {
            source_family: "akzio.test".into(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        };
        let make_artifact = |kind, blob, source_refs| {
            Artifact::new(
                kind,
                blob,
                "test.execution",
                ArtifactLifecycle::Canonical,
                provenance.clone(),
                Some(permit.artifact_origin()),
                source_refs,
                now,
            )
            .unwrap()
        };
        let asset = Asset::Qqq;
        let plan_hash = ContentHash::of_bytes(b"order-action test plan");
        let client_id = "qqq-r0";
        let broker_id = "broker-qqq";
        let context = make_artifact(
            ArtifactKind::ExecutionContext,
            store.stage_json(&"fixture execution context").unwrap(),
            vec![],
        );
        let context_ref = ArtifactRef {
            artifact_id: context.artifact_id.clone(),
            kind: context.kind,
        };
        let commitment = make_artifact(
            ArtifactKind::ExecutionCommitment,
            store
                .stage_json(&PaperCommitment {
                    commitment_id: PaperCommitmentId::new(),
                    execution_context: context_ref.clone(),
                    plan_hash: plan_hash.clone(),
                    broker_session: now.date_naive().to_string(),
                    client_order_ids: [(asset, client_id.into())].into(),
                    created_at: now,
                })
                .unwrap(),
            vec![context_ref],
        );
        let commitment_ref = ArtifactRef {
            artifact_id: commitment.artifact_id.clone(),
            kind: commitment.kind,
        };
        let receipt = make_artifact(
            ArtifactKind::OrderReceipt,
            store
                .stage_json(&OrderReceipt {
                    plan_hash,
                    asset,
                    client_order_id: client_id.into(),
                    broker_order_id: broker_id.into(),
                    state: OrderReceiptState::Accepted,
                    requested_quantity_micros: 1_000_000,
                    filled_quantity_micros: 0,
                    remaining_quantity_micros: 1_000_000,
                    average_fill_price: None,
                    broker_updated_at: now,
                    reason: None,
                    observed_at: now,
                })
                .unwrap(),
            vec![commitment_ref.clone()],
        );
        // The fixture seeds upstream outputs. Both action commits use the production entry points.
        {
            let mut connection = store.connection().unwrap();
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            insert_artifact(&transaction, &context).unwrap();
            insert_artifact(&transaction, &commitment).unwrap();
            insert_artifact(&transaction, &receipt).unwrap();
            transaction.commit().unwrap();
        }
        let receipt_ref = ArtifactRef {
            artifact_id: receipt.artifact_id,
            kind: receipt.kind,
        };
        let refs = vec![commitment_ref.clone(), receipt_ref.clone()];
        let cancel = make_artifact(
            ArtifactKind::ExecutionCancel,
            store
                .stage_json(&PaperCancel {
                    schema_version: DOMAIN_SCHEMA_VERSION,
                    cancel_id: PaperCancelId::new(),
                    commitment: commitment_ref.clone(),
                    prior_receipt: receipt_ref.clone(),
                    asset,
                    client_order_id: client_id.into(),
                    broker_order_id: broker_id.into(),
                    reason: PaperCancelReason::SettlementTimeout,
                    created_at: now,
                })
                .unwrap(),
            refs.clone(),
        );
        let reprice = make_artifact(
            ArtifactKind::ExecutionReprice,
            store
                .stage_json(&PaperReprice {
                    schema_version: DOMAIN_SCHEMA_VERSION,
                    reprice_id: PaperRepriceId::new(),
                    commitment: commitment_ref.clone(),
                    prior_receipt: receipt_ref,
                    asset,
                    prior_client_order_id: client_id.into(),
                    replacement_client_order_id: "qqq-r1".into(),
                    prior_broker_order_id: broker_id.into(),
                    replacement_limit_price: MoneyMicros(100_000_000),
                    created_at: now,
                })
                .unwrap(),
            refs,
        );
        let commit_cancel = || store.commit_execution_cancel_intent(&lease, &permit, &cancel, now);
        let commit_reprice =
            || store.commit_execution_reprice_intent(&lease, &permit, &reprice, now);

        assert!(store.read_execution_freeze_state().unwrap().is_none());
        let freeze = operator.write_freeze_state(true, "stop", now).unwrap();
        for result in [commit_reprice(), commit_cancel()] {
            assert!(
                matches!(result, Err(StoreError::ExecutionFrozen { reason }) if reason == "stop")
            );
        }
        let unreadable_freeze = Artifact::new(
            ArtifactKind::FreezeState,
            operator.stage_json(&"invalid freeze payload").unwrap(),
            &freeze.producer,
            ArtifactLifecycle::Canonical,
            freeze.provenance,
            None,
            vec![],
            now + next_second,
        )
        .unwrap();
        operator
            .write_bootstrap_artifact(&unreadable_freeze)
            .unwrap();
        for result in [commit_reprice(), commit_cancel()] {
            assert!(matches!(result, Err(StoreError::Json(_))));
        }
        assert!(store.cancel_for(&commitment_ref, asset).unwrap().is_none());
        assert!(store.reprice_for(&commitment_ref, asset).unwrap().is_none());
        for candidate in [&reprice, &cancel] {
            assert!(matches!(
                store.artifact(&candidate.artifact_id),
                Err(StoreError::MissingArtifact(id)) if id == candidate.artifact_id
            ));
        }

        operator
            .write_freeze_state(false, "resume", now + next_second * 2)
            .unwrap();
        assert!(!commit_reprice().unwrap().recovered);
        assert!(!commit_cancel().unwrap().recovered);
        operator
            .write_freeze_state(true, "stop again", now + next_second * 3)
            .unwrap();
        for (result, original) in [(commit_reprice(), &reprice), (commit_cancel(), &cancel)] {
            let recovered = result.unwrap();
            assert!(recovered.recovered);
            assert_eq!(&recovered.artifact, original);
        }
        let reprice_ref = ArtifactRef {
            artifact_id: reprice.artifact_id,
            kind: reprice.kind,
        };
        let cancel_ref = ArtifactRef {
            artifact_id: cancel.artifact_id,
            kind: cancel.kind,
        };
        assert!(matches!(
            store.record_paper_effect_intent(&lease, &permit, &reprice_ref, now),
            Err(StoreError::ExecutionFrozen { .. })
        ));
        assert!(!store
            .paper_effect_intent_recorded(&permit.run_id, &reprice_ref.artifact_id)
            .unwrap());
        assert!(!store
            .record_paper_effect_intent(&lease, &permit, &cancel_ref, now)
            .unwrap());
        operator
            .write_freeze_state(false, "resume effects", now + next_second * 4)
            .unwrap();
        assert!(!store
            .record_paper_effect_intent(&lease, &permit, &reprice_ref, now)
            .unwrap());
        operator
            .write_freeze_state(true, "stop new effects", now + next_second * 5)
            .unwrap();
        assert!(store
            .record_paper_effect_intent(&lease, &permit, &reprice_ref, now)
            .unwrap());
        drop(operator);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn paper_reservation_readback_failure_preserves_committed_run_id() {
        let (store, workflow, root) = prepared_formal_fixture();
        let run_id = workflow.run.run_id.clone();
        let now = Utc::now();
        let reservation = SessionReservation {
            session_key: now.date_naive().to_string(),
            workflow,
            setup_artifacts: vec![],
            reserved_at: now,
        };
        let lease = store
            .acquire_daemon_lease("scheduler", "fixture", now, now + Duration::minutes(1))
            .unwrap()
            .unwrap();
        let fenced = DaemonLease {
            epoch: lease.epoch + 1,
            ..lease.clone()
        };
        assert!(matches!(
            store.reserve_session_slot(&fenced, &reservation),
            Err(StoreError::SchedulerFenced(_))
        ));
        store
            .connection()
            .unwrap()
            .execute_batch(
                "CREATE TEMP TRIGGER break_slot_readback AFTER INSERT ON rebuild_session_slots
                 BEGIN
                   UPDATE rebuild_session_slots SET reserved_at = 'invalid timestamp'
                   WHERE session_key = NEW.session_key;
                 END;",
            )
            .unwrap();
        assert!(matches!(
            store.reserve_session_slot(&lease, &reservation),
            Err(StoreError::SessionReservationUnknown { run_id: existing, .. })
                if existing == run_id
        ));
        assert_eq!(store.run_purpose(&run_id).unwrap(), RunPurpose::Paper);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    /// WHY THIS TEST EXISTS (Task #18): An expired retired Run must not prevent
    /// recovery of a formal Run in the same Store or change the retired Run's history.
    /// Do not weaken or delete this assertion.
    #[test]
    fn retired_runs_do_not_block_expired_formal_task_recovery() {
        for (purpose, stored_purpose, reason) in [
            (RunPurpose::Debug, "debug", "debug_control_retired"),
            (
                RunPurpose::PaperDryRun,
                "paper_dry_run",
                "legacy_workflow_retired",
            ),
        ] {
            let (store, run_id, _, root) = formal_fixture();
            let claimed = store
                .claim_next_task_for_workload(
                    "prior-worker",
                    Utc::now(),
                    Duration::minutes(2),
                    TaskWorkload::Session,
                )
                .unwrap()
                .unwrap();
            store
                .connection()
                .unwrap()
                .execute(
                    "UPDATE rebuild_runs SET purpose=?1 WHERE run_id=?2",
                    params![stored_purpose, run_id.0],
                )
                .unwrap();
            assert!(matches!(
                store.assert_workflow_executable(&run_id),
                Err(StoreError::DebugControl(found)) if found == reason
            ));
            let before = store.workflow_snapshot(&run_id).unwrap();
            assert_eq!(before.run.purpose, purpose);
            assert!(matches!(
                store.heartbeat_task(&claimed.permit, Utc::now() + Duration::minutes(3)),
                Err(StoreError::DebugControl(found)) if found == reason
            ));

            let formal = formal_workflow(
                &store,
                RetryPolicy {
                    max_attempts: 2,
                    ..RetryPolicy::none()
                },
            );
            store.commit_workflow(&formal).unwrap();
            let formal_claim = store
                .claim_next_task_for_workload(
                    "formal-worker",
                    Utc::now(),
                    Duration::minutes(2),
                    TaskWorkload::Session,
                )
                .unwrap()
                .unwrap();
            assert_eq!(formal_claim.run_id, formal.run.run_id);
            let now = Utc::now() + Duration::minutes(3);
            store
                .connection()
                .unwrap()
                .execute_batch(
                    "CREATE TEMP TRIGGER reject_formal_recovery BEFORE UPDATE ON rebuild_tasks
                      WHEN NEW.status='queued' BEGIN
                        SELECT RAISE(ABORT, 'fixture recovery write failure');
                      END;",
                )
                .unwrap();
            assert!(matches!(
                store.recover_expired_tasks(now),
                Err(StoreError::Sql(error)) if error.to_string().contains("fixture recovery write failure")
            ));
            store
                .connection()
                .unwrap()
                .execute_batch("DROP TRIGGER reject_formal_recovery")
                .unwrap();
            assert_eq!(store.recover_expired_tasks(now).unwrap(), 1);
            assert_eq!(store.recover_expired_tasks(now).unwrap(), 0);
            let resumed = store
                .claim_next_task_for_workload(
                    "recovery-worker",
                    now,
                    Duration::minutes(2),
                    TaskWorkload::Session,
                )
                .unwrap()
                .unwrap();
            assert_eq!(resumed.run_id, formal.run.run_id);
            assert_eq!(resumed.permit.task_id, formal_claim.permit.task_id);
            assert_ne!(resumed.permit.attempt_id, formal_claim.permit.attempt_id);
            store
                .finish_task(&resumed.permit, TaskStatus::Succeeded, now)
                .unwrap();
            assert!(store
                .claim_next_task_for_workload(
                    "no-retired-claim",
                    now,
                    Duration::minutes(2),
                    TaskWorkload::Any,
                )
                .unwrap()
                .is_none());

            let inspection = store.inspect_run(&run_id).unwrap();
            assert_eq!(inspection.recovery, reason);
            assert!(inspection.allowed_actions.is_empty());
            assert_eq!(inspection.workflow, before);
            drop(store);
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    /// WHY THIS TEST EXISTS (Task #18): Row-level retirement must never turn
    /// an isolated Debug Store into a formal recovery target.
    /// Do not weaken or delete this assertion.
    #[test]
    fn isolated_debug_store_still_refuses_expired_task_recovery() {
        let (store, run_id, _, root) = formal_fixture();
        let now = Utc::now();
        store
            .claim_next_task_for_workload(
                "historical-owner",
                now,
                Duration::minutes(2),
                TaskWorkload::Any,
            )
            .unwrap()
            .unwrap();
        store
            .connection()
            .unwrap()
            .execute(
                "INSERT INTO rebuild_metadata(key,value) VALUES('debug_environment','historical-fixture')",
                [],
            )
            .unwrap();
        let before = store.workflow_snapshot(&run_id).unwrap();
        assert!(matches!(
            store.recover_expired_tasks(now + Duration::minutes(3)),
            Err(StoreError::DebugControl(reason)) if reason == "debug_control_retired"
        ));
        assert_eq!(store.workflow_snapshot(&run_id).unwrap(), before);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn old_control_identity_cannot_grant_a_worker_or_broker_access() {
        let (store, run_id, graph_id, root) = formal_fixture();
        {
            let connection = store.connection().unwrap();
            // Only a disposable test Store is changed: the guard must deny
            // even if an old head claims it is running with a known identity.
            connection
                .execute(
                    "UPDATE rebuild_run_controls SET identity_artifact_id=?1, runtime_identity=?1 \
                     WHERE run_id=?2",
                    params![graph_id.0.as_str(), run_id.0],
                )
                .unwrap();
        }
        assert!(matches!(
            store.assert_workflow_executable(&run_id),
            Err(StoreError::DebugControl(reason)) if reason == "debug_control_retired"
        ));
        assert!(store
            .claim_next_task_for_workload(
                "retired-worker",
                Utc::now(),
                Duration::minutes(1),
                TaskWorkload::Any,
            )
            .unwrap()
            .is_none());
        assert!(matches!(
            store.assert_broker_write_policy(&run_id),
            Err(StoreError::DebugBrokerWriteForbidden)
        ));
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
}
#[cfg(test)]
mod cancelled_paper_safety_tests {
    use super::*;
    use akzio_domain::{
        ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance, FailureDisposition,
        RetryPolicy, TaskBudget, TaskRecipeId, WorkflowGraph, DOMAIN_SCHEMA_VERSION,
    };

    /// This fixture tests the Store's scheduling boundary, not the validity of
    /// a Paper plan or an Alpaca effect. The effect event is deliberately
    /// inserted with a test-only transaction after an ordinary task claim.
    fn queued_paper_reconcile_with_effect(store: &Store) -> (RunId, TaskId) {
        let now = Utc::now();
        let run_id = RunId::new();
        let node = WorkflowNode {
            spec: None,
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new("gate.reconcile").unwrap(),
            contract_hash: None,
            objective: "offline safety scheduling fixture".into(),
            dependencies: vec![],
            input_artifacts: vec![],
            priority: 100,
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
            topology_id: "cancelled-paper-reconcile-fixture".into(),
            nodes: vec![node.clone()],
            agent_budgets: Default::default(),
        };
        let graph_artifact = Artifact::new(
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
                run_id: Some(run_id.clone()),
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
                    run_id: run_id.clone(),
                    purpose: RunPurpose::Paper,
                    topology_id: graph.topology_id,
                    graph_artifact_id: graph_artifact.artifact_id.clone(),
                    created_at: now,
                },
                graph: graph_artifact.clone(),
                nodes: vec![node.clone()],
            })
            .unwrap();
        let claimed = store
            .claim_next_task_for_workload(
                "before-cancellation",
                now,
                Duration::seconds(30),
                TaskWorkload::Session,
            )
            .unwrap()
            .unwrap();
        assert_eq!(claimed.node.task_id, node.task_id);
        let commitment = Artifact::new(
            ArtifactKind::ExecutionCommitment,
            store
                .stage_json(&serde_json::json!({"fixture": true}))
                .unwrap(),
            "test.commitment",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.test".into(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            Some(claimed.permit.artifact_origin()),
            vec![],
            now,
        )
        .unwrap();
        {
            let mut connection = store.connection().unwrap();
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            insert_artifact(&transaction, &commitment).unwrap();
            transaction
                .execute(
                    "INSERT INTO rebuild_session_slots \
                 (session_key, run_id, topology_id, graph_artifact_id, run_created_at, \
                  scheduler_epoch, reserved_at, commitment_artifact_id, committed_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, 1, ?5, ?6, ?5)",
                    params![
                        now.date_naive().to_string(),
                        run_id.0,
                        "cancelled-paper-reconcile-fixture",
                        graph_artifact.artifact_id.0.as_str(),
                        now.to_rfc3339(),
                        commitment.artifact_id.0.as_str(),
                    ],
                )
                .unwrap();
            append_event(
                &transaction,
                &run_id,
                Some(&claimed.node.task_id),
                Some(&claimed.permit.attempt_id),
                LifecycleEventType::ExecutionEffectIntent,
                Some(&commitment.artifact_id),
                now,
            )
            .unwrap();
            transaction.commit().unwrap();
        }
        store
            .defer_task(&claimed.permit, now + Duration::seconds(1), now)
            .unwrap();
        (run_id, node.task_id)
    }

    #[test]
    fn cancellation_and_expired_lease_preserve_only_an_existing_effect_reconcile() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/cancelled-paper-safety-tests")
            .join(RunId::new().0);
        let store = Store::open(&root).unwrap();
        let (run, task_id) = queued_paper_reconcile_with_effect(&store);
        let now = Utc::now() + Duration::seconds(2);
        assert!(store.paper_effect_intent_for_run(&run).unwrap());
        assert!(store
            .request_run_cancel(&run, "stop new work", now)
            .unwrap());
        assert!(store.run_cancel_requested(&run).unwrap());
        let safety = store
            .claim_next_task_for_workload(
                "safety-after-cancel",
                now,
                Duration::milliseconds(1),
                TaskWorkload::SafetyReconcile,
            )
            .unwrap()
            .expect("an already recorded effect still needs read-only reconciliation");
        assert_eq!(safety.node.task_id, task_id);
        assert_eq!(
            store
                .recover_expired_tasks(now + Duration::seconds(1))
                .unwrap(),
            1
        );
        let resumed = store
            .claim_next_task_for_workload(
                "safety-after-crash",
                now + Duration::seconds(2),
                Duration::seconds(30),
                TaskWorkload::SafetyReconcile,
            )
            .unwrap()
            .expect("cancellation must not exhaust the safety recovery path");
        assert_eq!(resumed.node.task_id, task_id);
    }
}
