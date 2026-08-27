#[test]
fn metrics_are_empty_for_a_new_store() {
    let directory = tempdir().unwrap();
    let store = V2Store::open(directory.path()).unwrap();
    let metrics = store.metrics(Utc::now()).unwrap();
    assert!(metrics.run_counts.is_empty());
    assert!(metrics.task_counts.is_empty());
    assert!(metrics.attempt_counts.is_empty());
    assert_eq!(metrics.event_count, 0);
    assert_eq!(metrics.active_daemon_leases, 0);
}

#[test]
fn metrics_expose_failed_run_and_attempt_alerts() {
    let metrics = StoreMetrics {
        run_counts: BTreeMap::from([("failed".to_owned(), 2)]),
        task_counts: BTreeMap::new(),
        attempt_counts: BTreeMap::from([("failed".to_owned(), 1)]),
        event_count: 0,
        active_daemon_leases: 0,
    };
    let alerts = metrics.alerts();
    assert_eq!(alerts.len(), 2);
    assert_eq!(alerts[0].code, "failed_runs");
    assert_eq!(alerts[1].code, "failed_attempts");
}

#[test]
fn backup_restore_round_trip_runs_store_doctor() {
    let source_directory = tempdir().unwrap();
    let store = V2Store::open(source_directory.path()).unwrap();
    let blob = store.put_bytes(b"backup-fixture", "text/plain").unwrap();

    let backup_parent = tempdir().unwrap();
    let backup_root = backup_parent.path().join("backup");
    let manifest = store.backup_to(&backup_root).unwrap();
    assert_eq!(manifest.blob_count, 1);
    assert_eq!(manifest.blob_bytes, blob.bytes);

    let restore_parent = tempdir().unwrap();
    let restore_root = restore_parent.path().join("restored");
    let restored = V2Store::restore_from(&backup_root, &restore_root).unwrap();
    let restored_blob = restored.read_blob(&blob).unwrap();
    assert_eq!(restored_blob, b"backup-fixture");
    restored.verify_integrity().unwrap();
}

#[test]
fn open_existing_does_not_create_a_missing_store_root() {
    let parent = tempdir().unwrap();
    let root = parent.path().join("missing");
    assert!(matches!(
        V2Store::open_existing(&root),
        Err(StoreError::Io { .. })
    ));
    assert!(!root.exists());

    let initialized = parent.path().join("initialized");
    V2Store::open(&initialized).unwrap();
    let existing = V2Store::open_existing(&initialized).unwrap();
    assert!(existing.metrics(Utc::now()).unwrap().run_counts.is_empty());
}

#[test]
fn export_run_writes_manifest_and_non_model_payloads() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    let export_parent = tempdir().unwrap();
    let target = export_parent.path().join("run-export");

    let manifest = fixture
        .store
        .export_run(&fixture.run.run_id, &target, false)
        .unwrap();

    assert_eq!(manifest.workflow.run.run_id, fixture.run.run_id);
    assert!(!manifest.include_raw_model);
    assert!(target.join(EXPORT_DATABASE_FILE).is_file());
    assert!(!target.join("manifest.json").exists());
    assert!(!target.join("artifacts").exists());
    assert!(manifest
        .artifacts
        .iter()
        .any(|entry| entry.payload_file.is_some()));
    let export = Connection::open(target.join(EXPORT_DATABASE_FILE)).unwrap();
    let stored_manifest = export
        .query_row(
            "SELECT value FROM export_metadata WHERE key = 'manifest'",
            [],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&stored_manifest).unwrap()["workflow"]["run"]
            ["run_id"],
        serde_json::to_value(&fixture.run.run_id).unwrap()
    );
    assert!(export
        .query_row("SELECT COUNT(*) > 0 FROM rebuild_blobs", [], |row| {
            row.get::<_, bool>(0)
        })
        .unwrap());

    let existing = export_parent.path().join("existing");
    std::fs::create_dir(&existing).unwrap();
    assert!(matches!(
        fixture
            .store
            .export_run(&fixture.run.run_id, &existing, false),
        Err(StoreError::BackupTargetExists(_))
    ));
}
