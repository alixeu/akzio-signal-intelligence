use super::*;

#[test]
fn runtime_checkpoint_and_event_paging_survive_restart_without_releasing_work() {
    let c = Case::new();
    c.control(DebugAction::Step, Some(0));
    let task = c.claim().unwrap();
    c.store
        .finish_task(&task.permit, TaskStatus::Succeeded, Utc::now())
        .unwrap();
    let before = c.store.inspect_run(&c.run).unwrap();
    assert_eq!(before.control.status, DebugStatus::Paused);
    let checkpoint = before.checkpoint.unwrap();
    assert_eq!(checkpoint.control_revision, before.control.revision);
    let reopened = Store::open_existing(c.store.root()).unwrap();
    let restored = reopened.inspect_run(&c.run).unwrap();
    assert_eq!(
        restored.checkpoint.unwrap().event_cursor,
        checkpoint.event_cursor
    );
    assert_eq!(restored.control.revision, before.control.revision);
    assert!(c.claim().is_none());
    let mut after = 0;
    let mut cursors = Vec::new();
    loop {
        let page = reopened
            .run_event_page(&c.run, after, 2, None, None)
            .unwrap();
        cursors.extend(page.events.iter().map(|e| e.cursor));
        after = page.next_cursor;
        if !page.has_more {
            break;
        }
    }
    assert_eq!(
        cursors.len(),
        c.store.events_after(&c.run, 0, 500).unwrap().len()
    );
    assert!(cursors.windows(2).all(|p| p[0] < p[1]));
    let filtered = reopened
        .run_event_page(
            &c.run,
            0,
            500,
            Some(&task.node.task_id),
            Some(&task.permit.attempt_id),
        )
        .unwrap();
    assert!(!filtered.events.is_empty());
    assert!(filtered
        .events
        .iter()
        .all(|e| e.attempt_id.as_ref() == Some(&task.permit.attempt_id)));
    reopened.verify_integrity().unwrap();
}

#[test]
fn runtime_control_for_ordinary_run_settles_and_reading_never_creates_events() {
    let c = Case::with_purpose(RunPurpose::Paper);
    assert!(c.store.debug_session(&c.run).unwrap().is_none());
    while let Some(task) = c.claim() {
        c.store
            .finish_task(&task.permit, TaskStatus::Succeeded, Utc::now())
            .unwrap();
    }
    let inspection = c.store.inspect_run(&c.run).unwrap();
    assert_eq!(inspection.control.status, DebugStatus::Completed);
    assert!(inspection.allowed_actions.is_empty());
    let before = inspection.workflow.event_cursor;
    c.store.run_checkpoint(&c.run).unwrap();
    c.store.recovery_snapshot(&c.run).unwrap();
    c.store.inspect_run(&c.run).unwrap();
    assert_eq!(
        before,
        c.store.workflow_snapshot(&c.run).unwrap().event_cursor
    );
    c.store.verify_integrity().unwrap();
}

#[test]
fn runtime_checkpoint_is_rolled_back_with_stale_attempt_completion() {
    let c = Case::new();
    c.control(DebugAction::Step, Some(0));
    let task = c.claim().unwrap();
    c.store
        .finish_task(&task.permit, TaskStatus::Succeeded, Utc::now())
        .unwrap();
    let before = c.store.workflow_snapshot(&c.run).unwrap().event_cursor;
    assert!(c
        .store
        .finish_task(&task.permit, TaskStatus::Succeeded, Utc::now())
        .is_err());
    assert_eq!(
        c.store.workflow_snapshot(&c.run).unwrap().event_cursor,
        before
    );
    c.store.verify_integrity().unwrap();
}

#[test]
fn runtime_store18_migrates_control_head_and_preserves_graph_cas() {
    let c = Case::new();
    for index in 0..c.tasks.len() {
        c.control(DebugAction::Step, Some(index));
        let task = c.claim().unwrap();
        c.store
            .finish_task(&task.permit, TaskStatus::Succeeded, Utc::now())
            .unwrap();
    }
    c.control(DebugAction::Abort, None);
    let before = c.store.workflow_snapshot(&c.run).unwrap();
    let old_session = c.store.debug_session(&c.run).unwrap().unwrap();
    let root = c.store.root().to_path_buf();
    let connection = rusqlite::Connection::open(root.join("akzio.sqlite3")).unwrap();
    connection
        .execute_batch(
            "ALTER TABLE rebuild_run_controls RENAME TO rebuild_debug_sessions;
        ALTER TABLE rebuild_tasks DROP COLUMN node_spec_json;
        UPDATE rebuild_metadata SET value='17' WHERE key='schema_version';",
        )
        .unwrap();
    drop(connection);
    drop(c.store);
    let reopened = Store::open(&root).unwrap();
    let after = reopened.workflow_snapshot(&c.run).unwrap();
    assert_eq!(
        before.revision.graph_artifact.artifact_id,
        after.revision.graph_artifact.artifact_id
    );
    assert_eq!(before.revision.graph, after.revision.graph);
    assert_eq!(before.event_cursor, after.event_cursor);
    let session = reopened.debug_session(&c.run).unwrap().unwrap();
    assert_eq!(session.identity, old_session.identity);
    assert_eq!(session.revision, old_session.revision);
    assert_eq!(session.status, old_session.status);
    reopened.verify_integrity().unwrap();
}

#[test]
fn runtime_checkpoint_rejects_a_replaced_journal_reference() {
    let c = Case::new();
    let graph = c
        .store
        .workflow_snapshot(&c.run)
        .unwrap()
        .run
        .graph_artifact_id;
    let connection = rusqlite::Connection::open(c.store.root().join("akzio.sqlite3")).unwrap();
    connection
        .execute(
            "UPDATE rebuild_events SET artifact_id=?1 WHERE event_type='runtime.checkpoint_saved'",
            [graph.0.as_str()],
        )
        .unwrap();
    assert!(c.store.recovery_snapshot(&c.run).is_err());
    assert!(c.store.verify_integrity().is_err());
}

#[test]
fn runtime_store18_migration_refuses_pending_v17_work() {
    let c = Case::new();
    let connection = rusqlite::Connection::open(c.store.root().join("akzio.sqlite3")).unwrap();
    connection
        .execute(
            "UPDATE rebuild_metadata SET value='17' WHERE key='schema_version'",
            [],
        )
        .unwrap();
    assert!(matches!(
        Store::open(c.store.root()),
        Err(StoreError::DebugControl(_))
    ));
    let version: String = connection
        .query_row(
            "SELECT value FROM rebuild_metadata WHERE key='schema_version'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(version, "17");
}
