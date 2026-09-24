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

    fn formal_fixture() -> (Store, RunId, ArtifactId, std::path::PathBuf) {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/retired-debug-execution-tests")
            .join(RunId::new().0);
        let store = Store::open(&root).unwrap();
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
            retry: RetryPolicy::none(),
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
        let graph_id = artifact.artifact_id.clone();
        store
            .commit_workflow(&WorkflowCommit {
                run: StoredRun {
                    run_id: run_id.clone(),
                    purpose: RunPurpose::Paper,
                    topology_id: graph.topology_id,
                    graph_artifact_id: graph_id.clone(),
                    created_at: now,
                },
                graph: artifact,
                nodes: vec![node],
            })
            .unwrap();
        (store, run_id, graph_id, root)
    }

    #[test]
    fn historical_debug_purpose_cannot_be_claimed_or_recovered() {
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
        {
            let connection = store.connection().unwrap();
            connection
                .execute(
                    "UPDATE rebuild_runs SET purpose='debug' WHERE run_id=?1",
                    params![run_id.0],
                )
                .unwrap();
        }
        assert!(matches!(
            store.assert_workflow_executable(&run_id),
            Err(StoreError::DebugControl(reason)) if reason == "debug_control_retired"
        ));
        assert!(matches!(
            store.heartbeat_task(&claimed.permit, Utc::now() + Duration::minutes(3)),
            Err(StoreError::DebugControl(reason)) if reason == "debug_control_retired"
        ));
        assert!(matches!(
            store.recover_expired_tasks(Utc::now() + Duration::minutes(3)),
            Err(StoreError::DebugControl(reason)) if reason == "debug_control_retired"
        ));
        assert_eq!(
            store.workflow_snapshot(&run_id).unwrap().run.purpose,
            RunPurpose::Debug
        );
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
