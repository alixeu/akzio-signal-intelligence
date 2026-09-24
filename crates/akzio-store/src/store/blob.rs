// 文件导读：Blob 生命周期分为同一 SQLite 连接可见的 TEMP staging 和 durable CAS；
// promotion 才把逻辑 hash/长度写入 rebuild_blobs，切片/字典只保存可验证的父依赖。
use super::debug::{environment_identity, read_session};
use super::*;

// 创建连接私有的临时 staging 表；表只服务于当前 SQLite 连接，提交 Artifact 时再提升为持久 BLOB。
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
    // 只读汇总 Artifact、BLOB、压缩和未引用依赖的数量/字节数，不回收或修复任何存储内容。
    // 有依赖表时递归计算可达 BLOB；旧 schema 则只按直接 Artifact/嵌入引用判断未引用项。
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

    // 校验 Store 后导出一个 Run 的工作流、事件、轨迹和 Artifact 闭包到新的 SQLite 目录。
    // raw model 是否可导出由 Store 内的 Debug 身份决定；导出成功不表示原 Run 或下游 Decision 已完成。
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
        // 从图、Task 输入和事件 Artifact 开始，随后沿 source_refs 扩展，形成可复核的 CAS 闭包。
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
            // 原始模型细节只在通过能力检查时写入导出库；被删节的 Artifact 仍保留元数据。
            let artifact = read_artifact(&connection, &artifact_id)?;
            pending.extend(
                artifact
                    .source_refs
                    .iter()
                    .map(|reference| reference.artifact_id.clone()),
            );
            let raw_model = is_trajectory_redacted_kind(artifact.kind);
            let payload_file = if !raw_model || include_raw_model {
                payloads.push((
                    artifact.blob.clone(),
                    read_blob_with(&connection, &artifact.blob)?,
                ));
                for (_, _, embedded) in embedded_blob_refs(&connection, &artifact)? {
                    payloads.push((embedded.clone(), read_blob_with(&connection, &embedded)?));
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
            // 重新按 CAS 写入并比对 hash/长度，防止导出过程改变负载身份。
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
        // manifest 描述的是导出快照；这里没有将导出目录注册回原 Store。
        Ok(manifest)
    }

    // 校验后用 SQLite VACUUM INTO 生成独立快照，并读取快照的哈希和 BLOB 统计。
    // 目标目录必须在 Store Root 外且不存在；该操作不创建新的业务 Artifact。
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

    // 校验源数据库后复制到一个不存在的新目录，再以 Store 身份重新打开并复核完整性。
    // restore 复制的是已验证快照，不负责迁移 schema、重建 Policy 或激活任何运行能力。
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

    // 将原始字节按 CAS hash 写入 durable rebuild_blobs，并返回带媒体类型和逻辑长度的引用。
    // 这是直接持久化入口；Artifact/事件的事务闭包仍由上层提交逻辑负责。
    pub fn put_bytes(&self, bytes: &[u8], media_type: impl Into<String>) -> StoreResult<BlobRef> {
        let connection = self.connection()?;
        put_blob_bytes(&connection, bytes, media_type.into())
    }

    // 先序列化为 JSON，再复用 put_bytes 的 CAS、压缩和完整性校验；不会把 JSON 另存为平铺状态。
    pub fn put_json<T: Serialize>(&self, value: &T) -> StoreResult<BlobRef> {
        self.put_bytes(&serde_json::to_vec(value)?, "application/json")
    }

    // 在当前连接的临时表中准备 CAS 负载；在引用它的 Artifact 提交前，负载尚未 durable。
    /// Prepare a CAS payload without making it durable. The returned reference
    /// is readable only through this `Store` instance until the same connection
    /// promotes it in the transaction that inserts its referencing Artifact.
    pub fn stage_bytes(&self, bytes: &[u8], media_type: impl Into<String>) -> StoreResult<BlobRef> {
        let connection = self.connection()?;
        stage_blob_bytes(&connection, bytes, media_type.into())
    }

    // JSON staging 只改变临时连接状态，持久化时仍由同一连接上的 Artifact 事务提升。
    pub fn stage_json<T: Serialize>(&self, value: &T) -> StoreResult<BlobRef> {
        self.stage_bytes(&serde_json::to_vec(value)?, "application/json")
    }

    // 从已有（或同连接已 staging 的）BLOB 取非空字节区间，并记录父 BLOB 与范围依赖。
    // 返回的引用代表逻辑切片；真正的存储表示由 promotion 阶段决定。
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

    // 先把 JSON 序列化，再校验字典在当前连接可见；promotion 只有在压缩节省达到阈值时才使用字典。
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

    // 在 Store 的唯一连接上读取 BLOB；不能在调用方已持有连接时再次获取 Mutex。
    /// Read one CAS payload on the Store's own connection.
    ///
    /// The connection is acquired exactly once. A second acquisition would
    /// deadlock permanently and silently whenever a caller already holds the
    /// guard, because `connection` is a non-reentrant `std::sync::Mutex`;
    /// store-internal code that already has a connection or transaction must
    /// call [`read_blob_with`] instead of this method.
    ///
    /// Reading on the shared connection is also the only correct choice: a
    /// separately opened connection sees neither the caller's uncommitted
    /// writes nor `temp.akzio_staged_blobs`, which is private to this one.
    pub fn read_blob(&self, blob: &BlobRef) -> StoreResult<Vec<u8>> {
        let connection = self.connection()?;
        read_blob_with(&connection, blob)
    }
}

// 供已持有 Connection/Transaction 的 Store 内部路径读取；优先看连接私有 staging，再读 durable CAS。
/// Connection-scoped CAS read for callers that already hold the Store
/// connection or an open transaction. Staged payloads stay visible, so an
/// Artifact's blob can be read inside the transaction that promotes it.
pub(super) fn read_blob_with(connection: &Connection, blob: &BlobRef) -> StoreResult<Vec<u8>> {
    read_blob_bytes_including_staged(connection, &blob.hash, blob.bytes)
}

// 判断导出是否具备 raw model 能力：Debug 运行直接允许，其他 purpose 必须有匹配的隔离 DebugSession。
// 该谓词只授权导出细节，不把研究、Decision、Paper 或学习资格变成已完成状态。
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

// 使用默认 identity 编码把字节交给带编码提示的 staging 实现。
pub(super) fn stage_blob_bytes(
    connection: &Connection,
    bytes: &[u8],
    media_type: String,
) -> StoreResult<BlobRef> {
    stage_blob_bytes_with_hint(connection, bytes, media_type, BLOB_ENCODING_IDENTITY, None)
}

// 校验媒体类型后按原始字节计算 BlobRef；已有 durable BLOB 可直接复用，否则写入临时表并回读校验。
// dependency 只记录切片/字典的来源元数据，不改变逻辑 hash 或返回长度。
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

// 读取 staging 中同连接可见的负载；没有 staging 时回退到 durable rebuild_blobs。
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

// 将临时 BLOB 按提示提升到 durable CAS，并在删除临时行前确认最终引用仍与原 hash/长度一致。
// 该函数运行在调用方的 Artifact 事务内，失败会让外层事务回滚而不会留下半成品引用。
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
    // 不同提示对应不同的物理编码，但都必须还原到相同的逻辑字节。
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
    // 只有 durable 负载已经完成 identity 校验后才消费临时 staging 行。
    connection.execute(
        "DELETE FROM temp.akzio_staged_blobs WHERE blob_hash = ?1",
        params![blob.hash.as_str()],
    )?;
    Ok(())
}

// 解析派生 BLOB 的父引用；父项若仍在 staging，会先在当前事务中提升为 durable。
// media type 对依赖重建无关，因此返回内部占位类型，调用方只使用 hash/长度/范围。
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

// 读取一行连接私有 staging，并同时核对逻辑长度、实际长度和内容 hash。
// 任一元数据不一致都按缺失/损坏处理，避免把临时表中的错误负载继续传播。
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

// 对 durable CAS 负载选择标准编码后插入；空媒体类型在进入数据库前即拒绝。
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

// 校验字节确实等于父 BLOB 的指定区间，再以 dependency 行存储零 payload 的逻辑切片。
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

// 尝试用指定 BLOB 作为 zstd 字典；小负载或节省不足时回退到普通 identity/zstd 编码。
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

// 对达到阈值的负载尝试 zstd，只有压缩后达到最小节省才采用压缩表示，否则保留原字节。
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

// 以内容 hash 幂等插入 durable BLOB；首次插入时记录派生依赖，随后回读逻辑内容确认 CAS 身份。
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

// 从 durable CAS 读取并递归还原逻辑字节，最后核对调用方声明的长度。
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

// 递归解码单个 BLOB，并沿切片/字典依赖读取父 BLOB；visiting 同时阻止环和过深依赖链。
// 任何长度、编码、解压或 hash 不一致都按 MissingBlob/Integrity 边界返回，不返回未经验证的字节。
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
    // stored_bytes 校验的是数据库中的物理 payload，logical_bytes 则在解码后再次校验。
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
            // 切片编码不保存独立 payload，必须从唯一父 BLOB 的半开区间 [start, end) 重建。
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
            // 字典编码要求依赖范围恰好覆盖整个字典，然后才允许解压逻辑内容。
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

// 从依赖表读取某个派生 BLOB 的唯一依赖，并校验 ordinal、类型和范围形状。
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

// 逐个解码所有 durable BLOB，并额外检查派生编码与依赖类型是否成对匹配。
// 这是完整性检查，不会清理未引用 BLOB，也不会修改 Artifact 生命周期。
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

// 兼容旧 Store 时探测派生 BLOB 依赖表是否存在，结果只影响读取/校验分支。
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    // 每个 Blob 测试使用独立 Root，避免 TEMP staging 与 durable CAS 互相污染。
    fn test_store(label: &str) -> Store {
        Store::open(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/blob-store-tests")
                .join(format!("{label}-{}", RunId::new().0)),
        )
        .unwrap()
    }

    /// `read_blob` acquires the Store connection. It must never acquire it a
    /// second time on the same thread: the guard is a non-reentrant
    /// `std::sync::Mutex`, so a nested acquisition blocks forever with no error
    /// and no poisoning. A staged-but-unpromoted blob is the input that used to
    /// drive `read_blob` into exactly that nested acquisition.
    #[test]
    // staged payload 的读取必须走同一连接，验证不会因非重入 Mutex 发生嵌套死锁。
    fn read_blob_of_staged_payload_does_not_deadlock() {
        let store = test_store("staged-no-deadlock");
        let staged = store
            .stage_bytes(b"staged-payload", "application/json")
            .unwrap();

        let (sender, receiver) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let result = store.read_blob(&staged);
            // Ignore send failures: the receiver has already given up on timeout.
            let _ = sender.send(result.map(|bytes| bytes.len()));
        });

        match receiver.recv_timeout(Duration::from_secs(10)) {
            Ok(Ok(len)) => assert_eq!(len, b"staged-payload".len()),
            Ok(Err(error)) => panic!("staged read failed: {error}"),
            Err(_) => panic!("read_blob deadlocked on a staged payload"),
        }
        worker.join().unwrap();
    }

    /// A staged payload is visible only through the connection that staged it,
    /// so reading it must use that connection rather than a freshly opened one.
    #[test]
    // staging 在 promotion 前只对当前 Store 连接可见。
    fn staged_payload_is_readable_before_promotion() {
        let store = test_store("staged-before-promotion");
        let staged = store.stage_bytes(b"not-yet-durable", "text/plain").unwrap();
        assert_eq!(store.read_blob(&staged).unwrap(), b"not-yet-durable");
    }

    /// A committed payload stays readable after promotion, and its bytes are
    /// unchanged by the storage representation the Store chose.
    #[test]
    // durable promotion 后按 logical bytes 读取，编码选择不改变返回内容。
    fn promoted_payload_round_trips() {
        let store = test_store("promoted-round-trip");
        let durable = store.put_bytes(b"durable-payload", "text/plain").unwrap();
        assert_eq!(store.read_blob(&durable).unwrap(), b"durable-payload");
    }

    /// A length that disagrees with the stored payload is a corrupt reference,
    /// not a cache miss, and must not be silently tolerated.
    #[test]
    // 引用声明长度与 CAS 实际长度不一致时按 MissingBlob 拒绝。
    fn blob_length_mismatch_is_rejected() {
        let store = test_store("length-mismatch");
        let mut reference = store.put_bytes(b"exact-length", "text/plain").unwrap();
        reference.bytes += 1;
        assert!(matches!(
            store.read_blob(&reference),
            Err(StoreError::MissingBlob(_))
        ));
    }
}
