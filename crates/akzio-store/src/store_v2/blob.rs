use super::*;

impl V2Store {
    pub fn storage_inventory(&self) -> StoreResult<StorageInventory> {
        let connection = self.connection()?;
        let artifact_count =
            connection.query_row("SELECT COUNT(*) FROM rebuild_artifacts", [], |row| {
                row.get::<_, u64>(0)
            })?;
        let (blob_count, logical_blob_bytes, stored_blob_bytes, compressed_blob_count) =
            connection.query_row(
                "SELECT COUNT(*), COALESCE(SUM(logical_bytes), 0), COALESCE(SUM(stored_bytes), 0), COALESCE(SUM(CASE WHEN encoding = 'zstd' THEN 1 ELSE 0 END), 0) FROM rebuild_blobs",
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
        let (unreferenced_blob_count, unreferenced_blob_bytes) = connection.query_row(
            r#"SELECT COUNT(*), COALESCE(SUM(blob.logical_bytes), 0)
               FROM rebuild_blobs AS blob
               WHERE NOT EXISTS (
                   SELECT 1 FROM rebuild_artifacts AS artifact
                   WHERE artifact.blob_hash = blob.blob_hash
               ) AND NOT EXISTS (
                   SELECT 1 FROM rebuild_embedded_blob_refs AS embedded
                   WHERE embedded.blob_hash = blob.blob_hash
               )"#,
            [],
            |row| Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?)),
        )?;
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
        if include_raw_model && workflow.run.purpose != RunPurpose::Debug {
            return Err(StoreError::RawModelExportNotAllowed(workflow.run.purpose));
        }
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
            schema_version: V2_SCHEMA_VERSION,
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

    pub fn read_blob(&self, blob: &BlobRef) -> StoreResult<Vec<u8>> {
        let connection = Connection::open_with_flags(
            self.root().join(DATABASE_FILE),
            OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        read_blob_bytes(&connection, &blob.hash, blob.bytes)
    }
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
    let hash = ContentHash::of_bytes(bytes);
    let compressed = if bytes.len() >= BLOB_COMPRESSION_THRESHOLD {
        Some(zstd::bulk::compress(bytes, 3).map_err(|error| {
            StoreError::Integrity(format!("zstd encode failed for {hash}: {error}"))
        })?)
    } else {
        None
    };
    let (encoding, stored) = match compressed {
        Some(compressed)
            if compressed
                .len()
                .saturating_add(BLOB_COMPRESSION_MIN_SAVINGS)
                < bytes.len() =>
        {
            (BLOB_ENCODING_ZSTD, compressed)
        }
        _ => (BLOB_ENCODING_IDENTITY, bytes.to_vec()),
    };
    connection.execute(
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
    if logical_bytes != expected_bytes || stored.len() as u64 != stored_bytes {
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
        _ => return Err(StoreError::MissingBlob(hash.clone())),
    };
    if bytes.len() as u64 != logical_bytes || ContentHash::of_bytes(&bytes) != *hash {
        return Err(StoreError::MissingBlob(hash.clone()));
    }
    Ok(bytes)
}
