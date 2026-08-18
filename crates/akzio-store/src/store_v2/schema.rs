use super::*;

impl V2Store {
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

        Ok(Self {
            root: Arc::new(root),
            connection: Arc::new(Mutex::new(connection)),
        })
    }
}
