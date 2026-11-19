// 文件导读：Store 打开路径区分新建、严格只读打开和“存在则只读/不存在才初始化”；
// schema/metadata 迁移由 initialize 统一处理，现有数据库不会在 open_existing 中被改写。
// 入口由 Core/CLI 等调用方选择：open 是显式可写初始化/升级，open_existing 只验证既有版本，
// open_existing_or_initialize 只在数据库文件不存在时走前者；测试围绕版本标签、索引和 lease 阻断。
use super::*;

impl Store {
    /// Prepare the private Store Root without opening or migrating its database.
    pub fn prepare_root(root: &Path) -> StoreResult<()> {
        fs::create_dir_all(root).map_err(|source| StoreError::Io {
            path: root.to_path_buf(),
            source,
        })?;
        secure_directory(root)
    }

    // 新建或升级入口先建立 SQLite 连接并打开 foreign_keys，再把 staging、嵌入 BLOB 索引和文件权限补齐。
    pub fn open(root: impl AsRef<Path>) -> StoreResult<Self> {
        let root = root.as_ref().to_path_buf();
        Self::prepare_root(&root)?;
        let database = root.join(DATABASE_FILE);
        let mut connection = Connection::open(&database)?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        initialize(&mut connection, &root)?;
        blob::initialize_staging(&connection)?;
        backfill_embedded_blob_refs(&mut connection)?;
        secure_file(&database)?;
        Ok(Self {
            root: Arc::new(root),
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    /// Open an already initialized Store Root without creating directories or
    /// mutating the SQLite schema. Read-only CLI commands must use this seam.
    // 只读打开既有数据库，兼容 v18 与当前版本；不创建目录、不运行 initialize/migration。
    // 当前版本必须带有逐单 journal 表，旧版只读打开不会创建这些表。
    pub fn open_existing(root: impl AsRef<Path>) -> StoreResult<Self> {
        let root = root.as_ref().to_path_buf();
        let database = root.join(DATABASE_FILE);
        if !root.is_dir() {
            return Err(StoreError::Io {
                path: root,
                source: std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Store Root directory does not exist",
                ),
            });
        }
        if !database.is_file() {
            return Err(StoreError::IncompatibleStoreRoot(root));
        }

        let connection = Connection::open_with_flags(&database, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        verify_readable_store_schema(&connection, &root)?;
        blob::initialize_staging(&connection)?;

        Ok(Self {
            root: Arc::new(root),
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    /// Run a synchronous read against one SQLite snapshot on a dedicated read-only connection.
    /// The transaction is pinned by the first read. No Store mutex is held across the callback.
    pub fn with_read_snapshot<T, E: From<StoreError>>(
        &self,
        read: impl FnOnce(&Self) -> Result<T, E>,
    ) -> Result<T, E> {
        let snapshot = Self::open_existing(self.root())?;
        snapshot
            .connection()?
            .execute_batch("BEGIN DEFERRED")
            .map_err(StoreError::from)?;
        let result = read(&snapshot);
        let close = snapshot.connection().and_then(|connection| {
            connection.execute_batch("ROLLBACK")?;
            Ok(())
        });
        result.and_then(|value| close.map(|()| value).map_err(E::from))
    }

    /// Open an initialized Store Root read-only, or initialize it when the
    /// database does not exist yet. Existing Stores are never migrated through
    /// this seam.
    // 仅按数据库文件是否存在分流：存在就严格 read-only open_existing，不匹配版本直接 Err；
    // 不存在才走可写 open，并返回 true 标识本次执行了初始化。
    pub fn open_existing_or_initialize(root: impl AsRef<Path>) -> StoreResult<(Self, bool)> {
        let root = root.as_ref();
        if root.join(DATABASE_FILE).is_file() {
            return Self::open_existing(root).map(|store| (store, false));
        }
        Self::open(root).map(|store| (store, true))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // 每个 schema case 使用唯一临时 Root，测试 metadata/index 迁移而不共享数据库。
    fn scratch_root(label: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/store-schema-tests")
            .join(format!("{label}-{}", RunId::new().0))
    }

    // 只读读取 metadata label，用来区分 schema 版本迁移是否提交。
    fn schema_version(root: &Path) -> Option<String> {
        let connection =
            Connection::open_with_flags(root.join(DATABASE_FILE), OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        connection
            .query_row(
                "SELECT value FROM rebuild_metadata WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .unwrap()
    }

    // 只读检查 rebuildable index 是否存在，不触碰 schema。
    fn index_exists(root: &Path, index: &str) -> bool {
        let connection =
            Connection::open_with_flags(root.join(DATABASE_FILE), OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        connection
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?1",
                params![index],
                |_| Ok(()),
            )
            .optional()
            .unwrap()
            .is_some()
    }

    fn relabel_as_legacy(root: &Path, version: &str) {
        let connection = Connection::open(root.join(DATABASE_FILE)).unwrap();
        connection
            .execute_batch(
                "DROP TABLE rebuild_paper_submission_transitions;
                 DROP TABLE rebuild_paper_submission_batches;",
            )
            .unwrap();
        connection
            .execute(
                "UPDATE rebuild_metadata SET value = ?1 WHERE key = 'schema_version'",
                [version],
            )
            .unwrap();
    }

    fn assert_no_submission_tables(root: &Path) {
        let connection =
            Connection::open_with_flags(root.join(DATABASE_FILE), OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        assert!(!migration::table_exists(&connection, "rebuild_paper_submission_batches").unwrap());
        assert!(
            !migration::table_exists(&connection, "rebuild_paper_submission_transitions").unwrap()
        );
    }

    /// WHY THIS TEST EXISTS (Task #30): Old read-only Stores and their snapshots
    /// keep v18. A writable upgrade adds an empty authoritative journal without
    /// manufacturing order admission evidence or rewriting existing payloads.
    /// Do not weaken or delete this assertion.
    #[test]
    fn legacy_read_and_backup_preserve_version_before_writable_upgrade() {
        let root = scratch_root("submission-v18");
        let backup18 = scratch_root("submission-v18-backup");
        let backup19 = scratch_root("submission-v19-backup");
        let store = Store::open(&root).unwrap();
        let original = store
            .write_freeze_state(true, "fixture", Utc::now())
            .unwrap();
        let original_payload = store.read_blob(&original.blob).unwrap();
        drop(store);
        relabel_as_legacy(&root, "18");
        let v18_before = fs::read(root.join(DATABASE_FILE)).unwrap();
        let legacy = Store::open_existing(&root).unwrap();
        legacy.verify_integrity().unwrap();
        assert_eq!(legacy.backup_to(&backup18).unwrap().schema_version, 18);
        assert_eq!(schema_version(&backup18).as_deref(), Some("18"));
        assert_eq!(legacy.artifact(&original.artifact_id).unwrap(), original);
        drop(legacy);
        assert_eq!(fs::read(root.join(DATABASE_FILE)).unwrap(), v18_before);
        assert_no_submission_tables(&root);

        let migrated = Store::open(&root).unwrap();
        assert_eq!(schema_version(&root).as_deref(), Some("19"));
        assert_eq!(migrated.artifact(&original.artifact_id).unwrap(), original);
        assert_eq!(
            migrated.read_blob(&original.blob).unwrap(),
            original_payload
        );
        let empty = migrated
            .connection()
            .unwrap()
            .query_row(
                "SELECT NOT EXISTS(SELECT 1 FROM rebuild_paper_submission_batches)
                    AND NOT EXISTS(SELECT 1 FROM rebuild_paper_submission_transitions)",
                [],
                |row| row.get::<_, bool>(0),
            )
            .unwrap();
        assert!(empty);
        assert_eq!(migrated.backup_to(&backup19).unwrap().schema_version, 19);
        assert_eq!(schema_version(&backup19).as_deref(), Some("19"));
        for root in [&backup18, &backup19] {
            Store::open_existing(root)
                .unwrap()
                .verify_integrity()
                .unwrap();
        }
    }

    /// WHY THIS TEST EXISTS (Task #30): Failed version publication must leave
    /// the old version and neither journal table, so a retry starts from v18.
    /// Do not weaken or delete this assertion.
    #[test]
    fn failed_version_publication_rolls_back_new_schema() {
        let root = scratch_root("submission-upgrade-rollback");
        drop(Store::open(&root).unwrap());
        relabel_as_legacy(&root, "18");
        let connection = Connection::open(root.join(DATABASE_FILE)).unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER fail_version_publication BEFORE UPDATE ON rebuild_metadata
                 WHEN NEW.key = 'schema_version'
                 BEGIN SELECT RAISE(ABORT, 'injected schema version write failure'); END;",
            )
            .unwrap();
        assert!(matches!(Store::open(&root), Err(StoreError::Sql(_))));
        assert_eq!(schema_version(&root).as_deref(), Some("18"));
        assert_no_submission_tables(&root);
        connection
            .execute_batch("DROP TRIGGER fail_version_publication")
            .unwrap();
        Store::open(&root).unwrap().verify_integrity().unwrap();
    }

    /// WHY THIS TEST EXISTS (Task #30): Recreating a lost v19 journal could
    /// erase proof of an admitted order. Opens and Doctor must fail closed.
    /// Do not weaken or delete this assertion.
    #[test]
    fn missing_current_journal_is_not_recreated() {
        let root = scratch_root("submission-missing-journal");
        let store = Store::open(&root).unwrap();
        Connection::open(root.join(DATABASE_FILE))
            .unwrap()
            .execute_batch("DROP TABLE rebuild_paper_submission_transitions;")
            .unwrap();
        assert!(matches!(
            store.verify_integrity(),
            Err(StoreError::Integrity(_))
        ));
        assert!(matches!(
            Store::open_existing(&root),
            Err(StoreError::Integrity(_))
        ));
        assert!(matches!(Store::open(&root), Err(StoreError::Integrity(_))));
        let connection = Connection::open(root.join(DATABASE_FILE)).unwrap();
        assert!(
            !migration::table_exists(&connection, "rebuild_paper_submission_transitions").unwrap()
        );
    }

    /// 本 fixture 先创建当前表，再模拟旧 v16 标签和缺失的可重建索引；
    /// 仅验证这种组合能重新标记，不断言所有历史 v16 库都已经有当前表。
    #[test]
    // 本测试的 current-table/旧 label 组合无 active work，可升级并重建索引，不改 CAS。
    fn older_label_is_upgraded_and_rebuildable_index_is_repaired() {
        let root = scratch_root("upgrade-from-16");
        let store = Store::open(&root).unwrap();
        store.verify_integrity().unwrap();
        drop(store);

        // Reproduce the legacy label and drop the rebuildable index.
        relabel_as_legacy(&root, "16");
        Connection::open(root.join(DATABASE_FILE))
            .unwrap()
            .execute_batch("DROP INDEX IF EXISTS rebuild_artifacts_run_kind;")
            .unwrap();
        assert_eq!(schema_version(&root).as_deref(), Some("16"));
        assert!(!index_exists(&root, "rebuild_artifacts_run_kind"));

        let reopened = Store::open(&root).unwrap();
        assert_eq!(
            schema_version(&root).as_deref(),
            Some(STORE_SCHEMA_VERSION.to_string().as_str())
        );
        assert!(index_exists(&root, "rebuild_artifacts_run_kind"));
        reopened.verify_integrity().unwrap();
        drop(reopened);

        // The relabelled root is now accepted by the read-only seam.
        Store::open_existing(&root)
            .unwrap()
            .verify_integrity()
            .unwrap();
    }

    /// The version label and the schema it describes are committed together, so
    /// a freshly created root is never observable as "current tables, no label".
    #[test]
    // 新 Root 的 schema label、DDL 和 rebuildable index 一起可见。
    fn fresh_root_is_labelled_with_the_current_version() {
        let root = scratch_root("fresh-label");
        let store = Store::open(&root).unwrap();
        assert_eq!(
            schema_version(&root).as_deref(),
            Some(STORE_SCHEMA_VERSION.to_string().as_str())
        );
        assert!(index_exists(&root, "rebuild_artifacts_run_kind"));
        store.verify_integrity().unwrap();
    }

    /// An active daemon lease blocks the relabel: the previous binary may still
    /// be scheduling against this root. The guard runs before any migration
    /// touches the schema, so the label stays where it was.
    #[test]
    // active daemon lease 会在迁移前阻断 label 改写。
    fn active_lease_blocks_the_upgrade() {
        let root = scratch_root("upgrade-blocked");
        let store = Store::open(&root).unwrap();
        let now = Utc::now();
        store
            .acquire_daemon_lease("scheduler", "old-worker", now, now + Duration::minutes(5))
            .unwrap()
            .unwrap();
        drop(store);
        relabel_as_legacy(&root, "18");

        assert!(matches!(
            Store::open(&root),
            Err(StoreError::DebugControl(_))
        ));
        assert_eq!(schema_version(&root).as_deref(), Some("18"));
        assert_no_submission_tables(&root);
    }

    /// WHY THIS TEST EXISTS (Task #30): Queued v18 work blocks the upgrade even
    /// after the previous daemon's lease has gone away.
    /// Do not weaken or delete this assertion.
    #[test]
    fn queued_work_blocks_the_journal_upgrade_without_a_daemon_lease() {
        let root = scratch_root("submission-queued-work");
        let store = Store::open(&root).unwrap();
        let now = Utc::now();
        let run_id = RunId::new();
        let node = WorkflowNode {
            spec: None,
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new("gate.reconcile").unwrap(),
            contract_hash: None,
            objective: "offline queued work fixture".into(),
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
            topology_id: "submission-upgrade-fixture".into(),
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
        store
            .commit_workflow(&WorkflowCommit {
                run: StoredRun {
                    run_id,
                    purpose: RunPurpose::Paper,
                    topology_id: graph.topology_id,
                    graph_artifact_id: artifact.artifact_id.clone(),
                    created_at: now,
                },
                graph: artifact,
                nodes: vec![node],
            })
            .unwrap();
        drop(store);
        relabel_as_legacy(&root, "18");

        assert!(matches!(
            Store::open(&root),
            Err(StoreError::DebugControl(_))
        ));
        assert_eq!(schema_version(&root).as_deref(), Some("18"));
        assert_no_submission_tables(&root);
    }

    /// An unknown future label is refused rather than relabelled downward.
    #[test]
    // 未知 future label 必须拒绝打开，不能向下重标或猜测 schema。
    fn unknown_future_label_is_refused() {
        let root = scratch_root("future-label");
        drop(Store::open(&root).unwrap());
        {
            let connection = Connection::open(root.join(DATABASE_FILE)).unwrap();
            connection
                .execute(
                    "UPDATE rebuild_metadata SET value = '999' WHERE key = 'schema_version'",
                    [],
                )
                .unwrap();
        }
        assert!(matches!(
            Store::open(&root),
            Err(StoreError::IncompatibleStoreRoot(_))
        ));
        assert_eq!(schema_version(&root).as_deref(), Some("999"));
    }

    #[test]
    // 第一次调用创建并初始化，第二次调用只读重开并返回 initialized=false。
    fn existing_or_initialize_creates_once_then_reopens_read_only() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/store-schema-tests")
            .join(RunId::new().0);

        let (created, initialized) = Store::open_existing_or_initialize(&root).unwrap();
        assert!(initialized);
        assert!(root.join(DATABASE_FILE).is_file());
        created.verify_integrity().unwrap();
        drop(created);

        let (reopened, initialized) = Store::open_existing_or_initialize(&root).unwrap();
        assert!(!initialized);
        reopened.verify_integrity().unwrap();
    }
}
