use super::debug::{environment_identity, read_session};
use super::*;

pub(super) fn initialize_staging(connection: &Connection) -> StoreResult<()> {
    connection.execute_batch(
        r#"CREATE TEMP TABLE IF NOT EXISTS akzio_staged_blobs (
               blob_hash TEXT PRIMARY KEY,
               logical_bytes INTEGER NOT NULL,
               payload BLOB NOT NULL,
               encoding_hint TEXT NOT NULL,
               dependency_blob_hash TEXT,
               start_byte INTEGER,
               end_byte INTEGER
           ) WITHOUT ROWID;"#,
    )?;
    Ok(())
}

impl Store {
    pub fn storage_inventory(&self) -> StoreResult<StorageInventory> {
        let connection = self.connection()?;
        let artifact_count =
            connection.query_row("SELECT COUNT(*) FROM rebuild_artifacts", [], |row| {
                row.get::<_, u64>(0)
            })?;
        let (blob_count, logical_blob_bytes, stored_blob_bytes, compressed_blob_count) =
            connection.query_row(
                "SELECT COUNT(*), COALESCE(SUM(logical_bytes), 0), COALESCE(SUM(stored_bytes), 0), COALESCE(SUM(CASE WHEN encoding IN ('zstd', 'zstd-dictionary-v1') THEN 1 ELSE 0 END), 0) FROM rebuild_blobs",
                [],
                |row| {
                    Ok((
                        row.get::<_, u64>(0)?,
                        row.get::<_, u64>(1)?,
                        row.get::<_, u64>(2)?,
                        row.get::<_, u64>(3)?,
                    ))
                },
            )?;
        let direct_blob_count = connection.query_row(
            "SELECT COUNT(DISTINCT blob_hash) FROM rebuild_artifacts",
            [],
            |row| row.get::<_, u64>(0),
        )?;
        let embedded_blob_count = connection.query_row(
            "SELECT COUNT(DISTINCT blob_hash) FROM rebuild_embedded_blob_refs",
            [],
            |row| row.get::<_, u64>(0),
        )?;
        let unreferenced_query = if blob_dependency_table_exists(&connection)? {
            r#"WITH RECURSIVE reachable(blob_hash) AS (
                   SELECT blob_hash FROM rebuild_artifacts
                   UNION
                   SELECT blob_hash FROM rebuild_embedded_blob_refs
                   UNION
                   SELECT dependency.dependency_blob_hash
                   FROM rebuild_blob_dependencies AS dependency
                   JOIN reachable ON reachable.blob_hash = dependency.blob_hash
               )
               SELECT COUNT(*), COALESCE(SUM(blob.logical_bytes), 0)
               FROM rebuild_blobs AS blob
               WHERE blob.blob_hash NOT IN (SELECT blob_hash FROM reachable)"#
        } else {
            r#"SELECT COUNT(*), COALESCE(SUM(blob.logical_bytes), 0)
               FROM rebuild_blobs AS blob
               WHERE NOT EXISTS (
                   SELECT 1 FROM rebuild_artifacts AS artifact
                   WHERE artifact.blob_hash = blob.blob_hash
               ) AND NOT EXISTS (
                   SELECT 1 FROM rebuild_embedded_blob_refs AS embedded
                   WHERE embedded.blob_hash = blob.blob_hash
               )"#
        };
        let (unreferenced_blob_count, unreferenced_blob_bytes) =
            connection.query_row(unreferenced_query, [], |row| {
                Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?))
            })?;
        Ok(StorageInventory {
            artifact_count,
            blob_count,
            logical_blob_bytes,
            stored_blob_bytes,
            compressed_blob_count,
            direct_blob_count,
            embedded_blob_count,
            unreferenced_blob_count,
            unreferenced_blob_bytes,
        })
    }

    pub fn export_run(
        &self,
        run_id: &RunId,
        target: impl AsRef<Path>,
        include_raw_model: bool,
    ) -> StoreResult<RunExportManifest> {
        self.verify_integrity()?;
        let workflow = self.workflow_snapshot(run_id)?;
        let access_connection = self.connection()?;
        if include_raw_model
            && !raw_model_export_allowed(&access_connection, run_id, workflow.run.purpose)
        {
            return Err(StoreError::RawModelExportNotAllowed(workflow.run.purpose));
        }
        drop(access_connection);
        let target = target.as_ref().to_path_buf();
        if target.starts_with(self.root()) {
            return Err(StoreError::BackupInsideStoreRoot(target));
        }
        if target.exists() {
            return Err(StoreError::BackupTargetExists(target));
        }

        let events = self.read_all_events(run_id)?;
        let trajectory = self.trajectory(run_id)?;
        let mut pending = BTreeSet::new();
        pending.insert(workflow.run.graph_artifact_id.clone());
        for task in &workflow.tasks {
            pending.extend(
                task.node
                    .input_artifacts
                    .iter()
                    .map(|reference| reference.artifact_id.clone()),
            );
        }
        pending.extend(events.iter().filter_map(|event| event.artifact_id.clone()));

        let connection = self.connection()?;
        let mut visited = BTreeSet::new();
        let mut artifacts = Vec::new();
        let mut payloads = Vec::new();
        while let Some(artifact_id) = pending.pop_first() {
            if !visited.insert(artifact_id.clone()) {
                continue;
            }
            let artifact = read_artifact(&connection, &artifact_id)?;
            pending.extend(
                artifact
                    .source_refs
                    .iter()
                    .map(|reference| reference.artifact_id.clone()),
            );
            let raw_model = is_trajectory_redacted_kind(artifact.kind);
            let payload_file = if !raw_model || include_raw_model {
                payloads.push((artifact.blob.clone(), self.read_blob(&artifact.blob)?));
                for (_, _, embedded) in embedded_blob_refs(&connection, &artifact)? {
                    payloads.push((embedded.clone(), self.read_blob(&embedded)?));
                }
                Some(format!("sqlite:{}", artifact.blob.hash))
            } else {
                None
            };
            artifacts.push(RunExportArtifact {
                artifact,
                payload_file,
                raw_model,
            });
        }
        drop(connection);

        artifacts.sort_by(|left, right| left.artifact.artifact_id.cmp(&right.artifact.artifact_id));
        fs::create_dir_all(&target).map_err(|source| StoreError::Io {
            path: target.clone(),
            source,
        })?;
        secure_directory(&target)?;

        let manifest = RunExportManifest {
            schema_version: DOMAIN_SCHEMA_VERSION,
            exported_at: Utc::now(),
            include_raw_model,
            workflow,
            events,
            trajectory,
            artifacts,
        };
        let export_database = target.join(EXPORT_DATABASE_FILE);
        let mut export = Connection::open(&export_database)?;
        export.execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE export_metadata (
               key TEXT PRIMARY KEY,
               value BLOB NOT NULL
             );
             CREATE TABLE rebuild_blobs (
               blob_hash TEXT PRIMARY KEY,
               logical_bytes INTEGER NOT NULL,
               stored_bytes INTEGER NOT NULL,
               encoding TEXT NOT NULL,
               payload BLOB NOT NULL
             );",
        )?;
        let transaction = export.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for (blob, bytes) in &payloads {
            let stored = put_blob_bytes(&transaction, bytes, blob.media_type.clone())?;
            if stored.hash != blob.hash || stored.bytes != blob.bytes {
                return Err(StoreError::Integrity(format!(
                    "export payload {} changed identity",
                    blob.hash
                )));
            }
        }
        transaction.execute(
            "INSERT INTO export_metadata (key, value) VALUES ('manifest', ?1)",
            params![serde_json::to_vec_pretty(&manifest)?],
        )?;
        transaction.commit()?;
        drop(export);
        secure_file(&export_database)?;
        sync_file(&export_database)?;
        Ok(manifest)
    }

    /// Create one self-contained SQLite snapshot. Payloads already live in
    /// `rebuild_blobs`, so no filesystem CAS or sidecar manifest is needed.
    pub fn backup_to(&self, target: impl AsRef<Path>) -> StoreResult<BackupManifest> {
        self.verify_integrity()?;
        let target = target.as_ref().to_path_buf();
        if target.starts_with(self.root()) {
            return Err(StoreError::BackupInsideStoreRoot(target));
        }
        if target.exists() {
            return Err(StoreError::BackupTargetExists(target));
        }
        fs::create_dir_all(&target).map_err(|source| StoreError::Io {
            path: target.clone(),
            source,
        })?;
        secure_directory(&target)?;
        let database = target.join(DATABASE_FILE);
        {
            let connection = self.connection()?;
            let database_sql = database.to_string_lossy().into_owned();
            connection.execute("VACUUM INTO ?1", [&database_sql])?;
        }
        secure_file(&database)?;
        sync_file(&database)?;

        let snapshot = Connection::open_with_flags(&database, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let (blob_count, blob_bytes) = snapshot.query_row(
            "SELECT COUNT(*), COALESCE(SUM(logical_bytes), 0) FROM rebuild_blobs",
            [],
            |row| Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?)),
        )?;
        let database_bytes = fs::metadata(&database)
            .map_err(|source| StoreError::Io {
                path: database.clone(),
                source,
            })?
            .len();
        let database_content = fs::read(&database).map_err(|source| StoreError::Io {
            path: database.clone(),
            source,
        })?;
        Ok(BackupManifest {
            schema_version: STORE_SCHEMA_VERSION,
            database_hash: ContentHash::of_bytes(&database_content),
            database_bytes,
            blob_count,
            blob_bytes,
            created_at: Utc::now(),
        })
    }

    pub fn restore_from(source: impl AsRef<Path>, target: impl AsRef<Path>) -> StoreResult<Self> {
        let source = source.as_ref().to_path_buf();
        let target = target.as_ref().to_path_buf();
        if target.exists() {
            return Err(StoreError::BackupTargetExists(target));
        }
        let database = if source.is_file() {
            source.clone()
        } else {
            source.join(DATABASE_FILE)
        };
        if !database.is_file() {
            return Err(StoreError::InvalidBackup(source));
        }
        let source_root = database
            .parent()
            .ok_or_else(|| StoreError::InvalidBackup(source.clone()))?;
        let source_store = Self::open_existing(source_root)?;
        source_store.verify_integrity()?;
        drop(source_store);

        fs::create_dir_all(&target).map_err(|source_error| StoreError::Io {
            path: target.clone(),
            source: source_error,
        })?;
        secure_directory(&target)?;
        let target_database = target.join(DATABASE_FILE);
        fs::copy(&database, &target_database).map_err(|source_error| StoreError::Io {
            path: target_database.clone(),
            source: source_error,
        })?;
        secure_file(&target_database)?;
        sync_file(&target_database)?;
        let store = Self::open_existing(&target)?;
        store.verify_integrity()?;
        Ok(store)
    }

    pub fn put_bytes(&self, bytes: &[u8], media_type: impl Into<String>) -> StoreResult<BlobRef> {
        let connection = self.connection()?;
        put_blob_bytes(&connection, bytes, media_type.into())
    }

    pub fn put_json<T: Serialize>(&self, value: &T) -> StoreResult<BlobRef> {
        self.put_bytes(&serde_json::to_vec(value)?, "application/json")
    }

    /// Prepare a CAS payload without making it durable. The returned reference
    /// is readable only through this `Store` instance until the same connection
    /// promotes it in the transaction that inserts its referencing Artifact.
    pub fn stage_bytes(&self, bytes: &[u8], media_type: impl Into<String>) -> StoreResult<BlobRef> {
        let connection = self.connection()?;
        stage_blob_bytes(&connection, bytes, media_type.into())
    }

    pub fn stage_json<T: Serialize>(&self, value: &T) -> StoreResult<BlobRef> {
        self.stage_bytes(&serde_json::to_vec(value)?, "application/json")
    }

    /// Stage a logical blob backed by an exact byte range of another blob.
    /// The derived BlobRef keeps the hash and length of the logical slice;
    /// storage representation remains private to the Store.
    pub fn stage_slice(
        &self,
        base: &BlobRef,
        start_byte: usize,
        end_byte: usize,
        media_type: impl Into<String>,
    ) -> StoreResult<BlobRef> {
        let connection = self.connection()?;
        let base_bytes = read_blob_bytes_including_staged(&connection, &base.hash, base.bytes)?;
        let bytes = base_bytes
            .get(start_byte..end_byte)
            .filter(|bytes| !bytes.is_empty())
            .ok_or_else(|| {
                StoreError::Integrity(format!(
                    "invalid blob slice {}..{} of {}",
                    start_byte, end_byte, base.hash
                ))
            })?;
        stage_blob_bytes_with_hint(
            &connection,
            bytes,
            media_type.into(),
            BLOB_ENCODING_SLICE_V1,
            Some((base, start_byte, end_byte)),
        )
    }

    /// Stage JSON that may use another blob as a zstd dictionary. Promotion
    /// selects this representation only when it beats the normal identity/zstd
    /// representation by the configured minimum saving.
    pub fn stage_json_with_dictionary<T: Serialize>(
        &self,
        value: &T,
        dictionary: &BlobRef,
    ) -> StoreResult<BlobRef> {
        let bytes = serde_json::to_vec(value)?;
        let end_byte = usize::try_from(dictionary.bytes)
            .map_err(|_| StoreError::MissingBlob(dictionary.hash.clone()))?;
        let connection = self.connection()?;
        read_blob_bytes_including_staged(&connection, &dictionary.hash, dictionary.bytes)?;
        stage_blob_bytes_with_hint(
            &connection,
            &bytes,
            "application/json".to_owned(),
            BLOB_ENCODING_ZSTD_DICTIONARY_V1,
            Some((dictionary, 0, end_byte)),
        )
    }

    pub fn read_blob(&self, blob: &BlobRef) -> StoreResult<Vec<u8>> {
        let connection = Connection::open_with_flags(
            self.root().join(DATABASE_FILE),
            OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        match read_blob_bytes(&connection, &blob.hash, blob.bytes) {
            Ok(bytes) => Ok(bytes),
            Err(StoreError::MissingBlob(_)) => {
                drop(connection);
                let connection = self.connection()?;
                read_staged_blob_bytes(&connection, &blob.hash, blob.bytes)?
                    .ok_or_else(|| StoreError::MissingBlob(blob.hash.clone()))
            }
            Err(error) => Err(error),
        }
    }
}

/// Raw model detail is a capability of a legacy Debug run or of a matching
/// isolated DebugSession.  RunPurpose alone is deliberately insufficient for
/// Paper/PositionPlan, and a caller-provided flag cannot manufacture the
/// isolated identity.
pub(super) fn raw_model_export_allowed(
    connection: &Connection,
    run_id: &RunId,
    purpose: RunPurpose,
) -> bool {
    if purpose == RunPurpose::Debug {
        return true;
    }
    let Ok(Some(session)) = read_session(connection, run_id) else {
        return false;
    };
    let Ok(store_identity) = environment_identity(connection) else {
        return false;
    };
    session.identity.run_id == *run_id
        && session.identity.run_purpose == purpose
        && session.identity.learning_scope == akzio_domain::DebugLearningScope::Isolated
        && store_identity.as_deref() == Some(session.identity.store_identity.as_str())
}

pub(super) fn stage_blob_bytes(
    connection: &Connection,
    bytes: &[u8],
    media_type: String,
) -> StoreResult<BlobRef> {
    stage_blob_bytes_with_hint(connection, bytes, media_type, BLOB_ENCODING_IDENTITY, None)
}

fn stage_blob_bytes_with_hint(
    connection: &Connection,
    bytes: &[u8],
    media_type: String,
    encoding_hint: &str,
    dependency: Option<(&BlobRef, usize, usize)>,
) -> StoreResult<BlobRef> {
    if media_type.trim().is_empty() {
        return Err(StoreError::Domain(DomainError::EmptyField {
            field: "blob_ref.media_type",
        }));
    }
    let blob = BlobRef {
        hash: ContentHash::of_bytes(bytes),
        media_type,
        bytes: bytes.len() as u64,
    };
    match read_blob_bytes(connection, &blob.hash, blob.bytes) {
        Ok(_) => return Ok(blob),
        Err(StoreError::MissingBlob(_)) => {}
        Err(error) => return Err(error),
    }
    let (dependency_blob_hash, start_byte, end_byte) = dependency
        .map(|(blob, start, end)| {
            (
                Some(blob.hash.as_str()),
                Some(start as u64),
                Some(end as u64),
            )
        })
        .unwrap_or((None, None, None));
    connection.execute(
        r#"INSERT OR IGNORE INTO temp.akzio_staged_blobs
           (blob_hash, logical_bytes, payload, encoding_hint,
            dependency_blob_hash, start_byte, end_byte)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"#,
        params![
            blob.hash.as_str(),
            blob.bytes,
            bytes,
            encoding_hint,
            dependency_blob_hash,
            start_byte,
            end_byte,
        ],
    )?;
    let staged = read_staged_blob_bytes(connection, &blob.hash, blob.bytes)?
        .ok_or_else(|| StoreError::MissingBlob(blob.hash.clone()))?;
    if staged != bytes {
        return Err(StoreError::Integrity(format!(
            "staged blob {} changed identity",
            blob.hash
        )));
    }
    Ok(blob)
}

pub(super) fn read_blob_bytes_including_staged(
    connection: &Connection,
    hash: &ContentHash,
    expected_bytes: u64,
) -> StoreResult<Vec<u8>> {
    if let Some(bytes) = read_staged_blob_bytes(connection, hash, expected_bytes)? {
        return Ok(bytes);
    }
    read_blob_bytes(connection, hash, expected_bytes)
}

pub(super) fn promote_staged_blob(connection: &Connection, blob: &BlobRef) -> StoreResult<()> {
    let staged = connection
        .query_row(
            r#"SELECT encoding_hint, dependency_blob_hash, start_byte, end_byte
               FROM temp.akzio_staged_blobs
               WHERE blob_hash = ?1"#,
            params![blob.hash.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<u64>>(2)?,
                    row.get::<_, Option<u64>>(3)?,
                ))
            },
        )
        .optional()?;
    let Some((encoding_hint, dependency_hash, start_byte, end_byte)) = staged else {
        read_blob_bytes(connection, &blob.hash, blob.bytes)?;
        return Ok(());
    };
    let bytes = read_staged_blob_bytes(connection, &blob.hash, blob.bytes)?
        .ok_or_else(|| StoreError::MissingBlob(blob.hash.clone()))?;
    let stored = match encoding_hint.as_str() {
        BLOB_ENCODING_IDENTITY => put_blob_bytes(connection, &bytes, blob.media_type.clone())?,
        BLOB_ENCODING_SLICE_V1 => {
            let dependency =
                staged_dependency_blob_ref(connection, dependency_hash, start_byte, end_byte)?;
            put_blob_slice(
                connection,
                &bytes,
                blob.media_type.clone(),
                &dependency,
                start_byte.expect("validated staged slice start"),
                end_byte.expect("validated staged slice end"),
            )?
        }
        BLOB_ENCODING_ZSTD_DICTIONARY_V1 => {
            let dependency =
                staged_dependency_blob_ref(connection, dependency_hash, start_byte, end_byte)?;
            if start_byte != Some(0) || end_byte != Some(dependency.bytes) {
                return Err(StoreError::Integrity(format!(
                    "invalid staged dictionary dependency for {}",
                    blob.hash
                )));
            }
            put_blob_bytes_with_dictionary(
                connection,
                &bytes,
                blob.media_type.clone(),
                &dependency,
            )?
        }
        _ => {
            return Err(StoreError::Integrity(format!(
                "unknown staged blob encoding hint {encoding_hint} for {}",
                blob.hash
            )));
        }
    };
    if stored != *blob {
        return Err(StoreError::Integrity(format!(
            "staged blob {} changed identity during promotion",
            blob.hash
        )));
    }
    connection.execute(
        "DELETE FROM temp.akzio_staged_blobs WHERE blob_hash = ?1",
        params![blob.hash.as_str()],
    )?;
    Ok(())
}

fn staged_dependency_blob_ref(
    connection: &Connection,
    dependency_hash: Option<String>,
    start_byte: Option<u64>,
    end_byte: Option<u64>,
) -> StoreResult<BlobRef> {
    let hash = ContentHash::new(dependency_hash.ok_or_else(|| {
        StoreError::Integrity("staged derived blob has no dependency".to_owned())
    })?)?;
    let mut dependency_bytes = connection
        .query_row(
            "SELECT logical_bytes FROM rebuild_blobs WHERE blob_hash = ?1",
            params![hash.as_str()],
            |row| row.get::<_, u64>(0),
        )
        .optional()?;
    if dependency_bytes.is_none() {
        let staged_bytes = connection
            .query_row(
                "SELECT logical_bytes FROM temp.akzio_staged_blobs WHERE blob_hash = ?1",
                params![hash.as_str()],
                |row| row.get::<_, u64>(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::MissingBlob(hash.clone()))?;
        promote_staged_blob(
            connection,
            &BlobRef {
                hash: hash.clone(),
                media_type: "application/octet-stream".to_owned(),
                bytes: staged_bytes,
            },
        )?;
        dependency_bytes = Some(staged_bytes);
    }
    if start_byte.is_none() || end_byte.is_none() {
        return Err(StoreError::Integrity(format!(
            "staged derived blob {} has incomplete range",
            hash
        )));
    }
    Ok(BlobRef {
        hash,
        media_type: "application/octet-stream".to_owned(),
        bytes: dependency_bytes.expect("durable or staged dependency length"),
    })
}

fn read_staged_blob_bytes(
    connection: &Connection,
    hash: &ContentHash,
    expected_bytes: u64,
) -> StoreResult<Option<Vec<u8>>> {
    let staged = connection
        .query_row(
            "SELECT logical_bytes, payload FROM temp.akzio_staged_blobs WHERE blob_hash = ?1",
            params![hash.as_str()],
            |row| Ok((row.get::<_, u64>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()?;
    let Some((logical_bytes, bytes)) = staged else {
        return Ok(None);
    };
    if logical_bytes != expected_bytes
        || bytes.len() as u64 != logical_bytes
        || ContentHash::of_bytes(&bytes) != *hash
    {
        return Err(StoreError::MissingBlob(hash.clone()));
    }
    Ok(Some(bytes))
}

pub(super) fn put_blob_bytes(
    connection: &Connection,
    bytes: &[u8],
    media_type: String,
) -> StoreResult<BlobRef> {
    if media_type.trim().is_empty() {
        return Err(StoreError::Domain(DomainError::EmptyField {
            field: "blob_ref.media_type",
        }));
    }
    let (encoding, stored) = standard_blob_storage(bytes)?;
    insert_blob_storage(connection, bytes, media_type, encoding, stored, None)
}

fn put_blob_slice(
    connection: &Connection,
    bytes: &[u8],
    media_type: String,
    dependency: &BlobRef,
    start_byte: u64,
    end_byte: u64,
) -> StoreResult<BlobRef> {
    let dependency_bytes = read_blob_bytes(connection, &dependency.hash, dependency.bytes)?;
    let start = usize::try_from(start_byte)
        .map_err(|_| StoreError::MissingBlob(dependency.hash.clone()))?;
    let end =
        usize::try_from(end_byte).map_err(|_| StoreError::MissingBlob(dependency.hash.clone()))?;
    if dependency_bytes.get(start..end) != Some(bytes) {
        return Err(StoreError::Integrity(format!(
            "blob slice does not match dependency {}",
            dependency.hash
        )));
    }
    insert_blob_storage(
        connection,
        bytes,
        media_type,
        BLOB_ENCODING_SLICE_V1,
        Vec::new(),
        Some((BLOB_DEPENDENCY_SLICE_V1, dependency, start_byte, end_byte)),
    )
}

fn put_blob_bytes_with_dictionary(
    connection: &Connection,
    bytes: &[u8],
    media_type: String,
    dictionary: &BlobRef,
) -> StoreResult<BlobRef> {
    let (standard_encoding, standard_stored) = standard_blob_storage(bytes)?;
    if bytes.len() < BLOB_COMPRESSION_THRESHOLD {
        return insert_blob_storage(
            connection,
            bytes,
            media_type,
            standard_encoding,
            standard_stored,
            None,
        );
    }
    let dictionary_bytes = read_blob_bytes(connection, &dictionary.hash, dictionary.bytes)?;
    let hash = ContentHash::of_bytes(bytes);
    let dictionary_stored = zstd::bulk::Compressor::with_dictionary(3, &dictionary_bytes)
        .and_then(|mut compressor| compressor.compress(bytes))
        .map_err(|error| {
            StoreError::Integrity(format!("zstd dictionary encode failed for {hash}: {error}"))
        })?;
    if dictionary_stored
        .len()
        .saturating_add(BLOB_COMPRESSION_MIN_SAVINGS)
        >= standard_stored.len()
    {
        return insert_blob_storage(
            connection,
            bytes,
            media_type,
            standard_encoding,
            standard_stored,
            None,
        );
    }
    insert_blob_storage(
        connection,
        bytes,
        media_type,
        BLOB_ENCODING_ZSTD_DICTIONARY_V1,
        dictionary_stored,
        Some((
            BLOB_DEPENDENCY_ZSTD_DICTIONARY_V1,
            dictionary,
            0,
            dictionary.bytes,
        )),
    )
}

fn standard_blob_storage(bytes: &[u8]) -> StoreResult<(&'static str, Vec<u8>)> {
    let hash = ContentHash::of_bytes(bytes);
    let compressed = if bytes.len() >= BLOB_COMPRESSION_THRESHOLD {
        Some(zstd::bulk::compress(bytes, 3).map_err(|error| {
            StoreError::Integrity(format!("zstd encode failed for {hash}: {error}"))
        })?)
    } else {
        None
    };
    Ok(match compressed {
        Some(compressed)
            if compressed
                .len()
                .saturating_add(BLOB_COMPRESSION_MIN_SAVINGS)
                < bytes.len() =>
        {
            (BLOB_ENCODING_ZSTD, compressed)
        }
        _ => (BLOB_ENCODING_IDENTITY, bytes.to_vec()),
    })
}

fn insert_blob_storage(
    connection: &Connection,
    bytes: &[u8],
    media_type: String,
    encoding: &str,
    stored: Vec<u8>,
    dependency: Option<(&str, &BlobRef, u64, u64)>,
) -> StoreResult<BlobRef> {
    let hash = ContentHash::of_bytes(bytes);
    let inserted = connection.execute(
        r#"INSERT OR IGNORE INTO rebuild_blobs
           (blob_hash, logical_bytes, stored_bytes, encoding, payload)
           VALUES (?1, ?2, ?3, ?4, ?5)"#,
        params![
            hash.as_str(),
            bytes.len() as u64,
            stored.len() as u64,
            encoding,
            stored,
        ],
    )?;
    if inserted == 1 {
        if let Some((kind, parent, start_byte, end_byte)) = dependency {
            connection.execute(
                r#"INSERT INTO rebuild_blob_dependencies
                   (blob_hash, ordinal, dependency_kind, dependency_blob_hash,
                    start_byte, end_byte)
                   VALUES (?1, 0, ?2, ?3, ?4, ?5)"#,
                params![
                    hash.as_str(),
                    kind,
                    parent.hash.as_str(),
                    start_byte,
                    end_byte,
                ],
            )?;
        }
    }
    let logical = read_blob_bytes(connection, &hash, bytes.len() as u64)?;
    if logical != bytes {
        return Err(StoreError::Integrity(format!(
            "blob hash collision or inconsistent payload {hash}"
        )));
    }
    Ok(BlobRef {
        hash,
        media_type,
        bytes: bytes.len() as u64,
    })
}

pub(super) fn read_blob_bytes(
    connection: &Connection,
    hash: &ContentHash,
    expected_bytes: u64,
) -> StoreResult<Vec<u8>> {
    let mut visiting = BTreeSet::new();
    let bytes = read_blob_bytes_recursive(connection, hash, &mut visiting, 0)?;
    if bytes.len() as u64 != expected_bytes {
        return Err(StoreError::MissingBlob(hash.clone()));
    }
    Ok(bytes)
}

fn read_blob_bytes_recursive(
    connection: &Connection,
    hash: &ContentHash,
    visiting: &mut BTreeSet<ContentHash>,
    depth: usize,
) -> StoreResult<Vec<u8>> {
    if depth >= BLOB_MAX_DEPENDENCY_DEPTH || !visiting.insert(hash.clone()) {
        return Err(StoreError::Integrity(format!(
            "blob dependency cycle or depth overflow at {hash}"
        )));
    }
    let row = connection
        .query_row(
            "SELECT logical_bytes, stored_bytes, encoding, payload FROM rebuild_blobs WHERE blob_hash = ?1",
            params![hash.as_str()],
            |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                ))
            },
        )
        .optional()?;
    let Some((logical_bytes, stored_bytes, encoding, stored)) = row else {
        return Err(StoreError::MissingBlob(hash.clone()));
    };
    if stored.len() as u64 != stored_bytes {
        return Err(StoreError::MissingBlob(hash.clone()));
    }
    let bytes = match encoding.as_str() {
        BLOB_ENCODING_IDENTITY => stored,
        BLOB_ENCODING_ZSTD => {
            let maximum = usize::try_from(logical_bytes)
                .map_err(|_| StoreError::MissingBlob(hash.clone()))?;
            zstd::bulk::decompress(&stored, maximum)
                .map_err(|_| StoreError::MissingBlob(hash.clone()))?
        }
        BLOB_ENCODING_SLICE_V1 => {
            if !stored.is_empty() {
                return Err(StoreError::MissingBlob(hash.clone()));
            }
            let dependency = read_blob_dependency(connection, hash, BLOB_DEPENDENCY_SLICE_V1)?;
            let parent = read_blob_bytes_recursive(
                connection,
                &dependency.hash,
                visiting,
                depth.saturating_add(1),
            )?;
            let start = usize::try_from(dependency.start_byte)
                .map_err(|_| StoreError::MissingBlob(hash.clone()))?;
            let end = usize::try_from(dependency.end_byte)
                .map_err(|_| StoreError::MissingBlob(hash.clone()))?;
            parent
                .get(start..end)
                .ok_or_else(|| StoreError::MissingBlob(hash.clone()))?
                .to_vec()
        }
        BLOB_ENCODING_ZSTD_DICTIONARY_V1 => {
            let dependency =
                read_blob_dependency(connection, hash, BLOB_DEPENDENCY_ZSTD_DICTIONARY_V1)?;
            let dictionary = read_blob_bytes_recursive(
                connection,
                &dependency.hash,
                visiting,
                depth.saturating_add(1),
            )?;
            if dependency.start_byte != 0 || dependency.end_byte != dictionary.len() as u64 {
                return Err(StoreError::MissingBlob(hash.clone()));
            }
            let maximum = usize::try_from(logical_bytes)
                .map_err(|_| StoreError::MissingBlob(hash.clone()))?;
            zstd::bulk::Decompressor::with_dictionary(&dictionary)
                .and_then(|mut decompressor| decompressor.decompress(&stored, maximum))
                .map_err(|_| StoreError::MissingBlob(hash.clone()))?
        }
        _ => return Err(StoreError::MissingBlob(hash.clone())),
    };
    if bytes.len() as u64 != logical_bytes || ContentHash::of_bytes(&bytes) != *hash {
        return Err(StoreError::MissingBlob(hash.clone()));
    }
    visiting.remove(hash);
    Ok(bytes)
}

struct BlobDependency {
    hash: ContentHash,
    start_byte: u64,
    end_byte: u64,
}

fn read_blob_dependency(
    connection: &Connection,
    hash: &ContentHash,
    expected_kind: &str,
) -> StoreResult<BlobDependency> {
    if !blob_dependency_table_exists(connection)? {
        return Err(StoreError::MissingBlob(hash.clone()));
    }
    let rows = connection
        .prepare(
            r#"SELECT ordinal, dependency_kind, dependency_blob_hash, start_byte, end_byte
               FROM rebuild_blob_dependencies
               WHERE blob_hash = ?1
               ORDER BY ordinal"#,
        )?
        .query_map(params![hash.as_str()], |row| {
            Ok((
                row.get::<_, u64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, u64>(3)?,
                row.get::<_, u64>(4)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let [(ordinal, kind, dependency_hash, start_byte, end_byte)] = rows.as_slice() else {
        return Err(StoreError::MissingBlob(hash.clone()));
    };
    if *ordinal != 0 || kind != expected_kind || end_byte < start_byte {
        return Err(StoreError::MissingBlob(hash.clone()));
    }
    Ok(BlobDependency {
        hash: ContentHash::new(dependency_hash.clone())?,
        start_byte: *start_byte,
        end_byte: *end_byte,
    })
}

pub(super) fn verify_blob_storage(connection: &Connection) -> StoreResult<()> {
    let blobs = connection
        .prepare("SELECT blob_hash, logical_bytes FROM rebuild_blobs ORDER BY blob_hash")?
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    for (hash, logical_bytes) in blobs {
        let hash = ContentHash::new(hash)?;
        read_blob_bytes(connection, &hash, logical_bytes)?;
    }
    if blob_dependency_table_exists(connection)? {
        let dangling_shape = connection
            .query_row(
                r#"SELECT dependency.blob_hash
                   FROM rebuild_blob_dependencies AS dependency
                   JOIN rebuild_blobs AS blob ON blob.blob_hash = dependency.blob_hash
                   WHERE (blob.encoding = ?1 AND dependency.dependency_kind != ?2)
                      OR (blob.encoding = ?3 AND dependency.dependency_kind != ?4)
                      OR blob.encoding NOT IN (?1, ?3)
                   LIMIT 1"#,
                params![
                    BLOB_ENCODING_SLICE_V1,
                    BLOB_DEPENDENCY_SLICE_V1,
                    BLOB_ENCODING_ZSTD_DICTIONARY_V1,
                    BLOB_DEPENDENCY_ZSTD_DICTIONARY_V1,
                ],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if let Some(hash) = dangling_shape {
            return Err(StoreError::Integrity(format!(
                "blob {hash} has invalid storage dependency shape"
            )));
        }
    }
    Ok(())
}

fn blob_dependency_table_exists(connection: &Connection) -> StoreResult<bool> {
    Ok(connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'rebuild_blob_dependencies'",
            [],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}
