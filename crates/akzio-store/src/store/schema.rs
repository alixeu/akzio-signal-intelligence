use super::*;

impl Store {
    pub fn open(root: impl AsRef<Path>) -> StoreResult<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|source| StoreError::Io {
            path: root.clone(),
            source,
        })?;
        secure_directory(&root)?;
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
        let version = connection
            .query_row(
                "SELECT value FROM rebuild_metadata WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let expected_version = STORE_SCHEMA_VERSION.to_string();
        if version.as_deref() != Some(expected_version.as_str()) {
            return Err(StoreError::IncompatibleStoreRoot(root));
        }
        blob::initialize_staging(&connection)?;

        Ok(Self {
            root: Arc::new(root),
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    /// Open an initialized Store Root read-only, or initialize it when the
    /// database does not exist yet. Existing Stores are never migrated through
    /// this seam.
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

    fn scratch_root(label: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/store-schema-tests")
            .join(format!("{label}-{}", RunId::new().0))
    }

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

    /// A v16 root carries the full v17 table set already, because those tables
    /// are created unconditionally. Reopening it must relabel it to the current
    /// version and leave the rebuildable index in place, without rewriting CAS.
    #[test]
    fn older_label_is_upgraded_and_rebuildable_index_is_repaired() {
        let root = scratch_root("upgrade-from-16");
        let store = Store::open(&root).unwrap();
        store.verify_integrity().unwrap();
        drop(store);

        // Reproduce a root left behind by the previous binary: current tables,
        // an older label, and the rebuildable index dropped.
        {
            let connection = Connection::open(root.join(DATABASE_FILE)).unwrap();
            connection
                .execute(
                    "UPDATE rebuild_metadata SET value = '16' WHERE key = 'schema_version'",
                    [],
                )
                .unwrap();
            connection
                .execute_batch("DROP INDEX IF EXISTS rebuild_artifacts_run_kind;")
                .unwrap();
        }
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
    fn active_lease_blocks_the_upgrade() {
        let root = scratch_root("upgrade-blocked");
        let store = Store::open(&root).unwrap();
        let now = Utc::now();
        store
            .acquire_daemon_lease("scheduler", "old-worker", now, now + Duration::minutes(5))
            .unwrap()
            .unwrap();
        drop(store);
        {
            let connection = Connection::open(root.join(DATABASE_FILE)).unwrap();
            connection
                .execute(
                    "UPDATE rebuild_metadata SET value = '16' WHERE key = 'schema_version'",
                    [],
                )
                .unwrap();
        }

        assert!(matches!(
            Store::open(&root),
            Err(StoreError::DebugControl(_))
        ));
        assert_eq!(schema_version(&root).as_deref(), Some("16"));
    }

    /// An unknown future label is refused rather than relabelled downward.
    #[test]
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
