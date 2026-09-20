//! Frozen history import is test-only; production has no retired workflow constructor.
use super::*;

const RUN: &str = "13b88b7ce6fe4c45";
fn archived_store() -> Store {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../.akzio/retired-history-tests")
        .join(RunId::new().0);
    let store = Store::open(root).unwrap();
    let mut connection = store.connection().unwrap();
    let tx = connection.transaction().unwrap();
    tx.execute_batch("PRAGMA defer_foreign_keys=ON;").unwrap();
    // Import the frozen v17 row layout by name; new metadata remains absent.
    // The historical SQL and all CAS bytes remain untouched.
    let history = include_str!("fixtures/retired_paper_dry_run.sql").replace(
        "INSERT INTO \"rebuild_tasks\" VALUES",
        "INSERT INTO rebuild_tasks(task_id,run_id,recipe_id,objective,contract_hash,priority,budget_json,retry_json,on_failure,parent_task_id,input_artifacts_json,status,ready_at,lease_id,lease_epoch,active_attempt_id,lease_until,worker_id,finished_at) VALUES",
    );
    tx.execute_batch(&history).unwrap();
    run_control::backfill_history(&tx).unwrap();
    tx.commit().unwrap();
    drop(connection);
    store
}

#[test]
fn retired_history_keeps_integrity_export_and_frozen_hashes() {
    let store = archived_store();
    let run = RunId(RUN.into());
    store.verify_integrity().unwrap();
    let before = store.workflow_snapshot(&run).unwrap();
    assert_eq!(before.run.purpose, RunPurpose::PaperDryRun);
    assert_eq!(before.status, WorkflowStatus::Completed);
    let inspection = store.inspect_run(&run).unwrap();
    assert!(inspection.allowed_actions.is_empty());
    assert!(inspection.checkpoint.is_none());
    assert!(inspection.blueprint.definition_version.is_none());
    store.check_legacy_workflow_retirement(Utc::now()).unwrap();
    assert!(store
        .assert_workflow_executable(&run)
        .unwrap_err()
        .to_string()
        .contains("legacy_workflow_retired"));
    for action in [
        akzio_domain::DebugAction::Resume,
        akzio_domain::DebugAction::Step,
        akzio_domain::DebugAction::RetryNode,
    ] {
        let error = store
            .debug_control(
                &run,
                &akzio_domain::DebugControlRequest {
                    action,
                    expected_revision: 0,
                    task_id: Some(before.tasks[0].node.task_id.clone()),
                },
                &ContentHash::of_bytes(b"irrelevant-current-runtime"),
                Utc::now(),
            )
            .unwrap_err();
        assert!(error.to_string().contains("legacy_workflow_retired"));
    }
    let target = store
        .root()
        .parent()
        .unwrap()
        .join(format!("export-{}", RunId::new()));
    store.export_run(&run, &target, false).unwrap();
    assert_eq!(before, store.workflow_snapshot(&run).unwrap());
    store.verify_integrity().unwrap();
}

#[test]
fn retired_pending_task_and_live_lease_block_without_changing_state() {
    for live_lease in [false, true] {
        let store = archived_store();
        let now = Utc::now();
        let task: String = {
            let connection = store.connection().unwrap();
            let task: String = connection
                .query_row(
                    "SELECT task_id FROM rebuild_tasks WHERE recipe_id='research.planner'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            // Simulate the pre-upgrade pending checkpoint or a completed row with a live lease.
            // Only the read-only preflight is under test; no legacy task is ever executed.
            if live_lease {
                connection
                    .execute(
                        "UPDATE rebuild_tasks SET lease_until=?1 WHERE task_id=?2",
                        params![(now + Duration::minutes(5)).to_rfc3339(), task],
                    )
                    .unwrap();
            } else {
                connection
                    .execute(
                        "UPDATE rebuild_tasks SET status='queued' WHERE task_id=?1",
                        params![task],
                    )
                    .unwrap();
                connection
                    .execute(
                        "UPDATE rebuild_runs SET status='queued' WHERE run_id=?1",
                        params![RUN],
                    )
                    .unwrap();
                connection
                    .execute(
                        "UPDATE rebuild_run_controls SET status='running' WHERE run_id=?1",
                        params![RUN],
                    )
                    .unwrap();
            }
            task
        };
        let frozen = || {
            let c = store.connection().unwrap();
            c.query_row(
                "SELECT status,lease_until,active_attempt_id FROM rebuild_tasks WHERE task_id=?1",
                params![task],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Option<String>>(1)?,
                        r.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .unwrap()
        };
        let before = frozen();
        let error = store
            .check_legacy_workflow_retirement(now)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("legacy_workflow_retired")
                && error.contains(RUN)
                && error.contains(&task),
            "{error}"
        );
        if !live_lease {
            assert!(store
                .claim_next_task_for_workload("test", now, Duration::minutes(1), TaskWorkload::Any)
                .unwrap_err()
                .to_string()
                .contains("legacy_workflow_retired"));
        }
        assert_eq!(before, frozen());
    }
}
