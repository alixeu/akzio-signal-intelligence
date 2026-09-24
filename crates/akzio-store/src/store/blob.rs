// 文件导读：本文件实现 CAS BLOB 从暂存到持久化、压缩/派生编码、读取校验、备份和 Run 导出。
// 先读 stage_* → promote_staged_blob → read_blob_bytes_recursive 理解负载流转，再读
// storage_inventory/export_run/backup_to 理解只读盘点与文件副作用。TEMP staging 只属于单一
// SQLite 连接；Artifact 写事务提升后才进入 durable rebuild_blobs，切片/字典只保存可验证的父依赖。
use super::debug::{environment_identity, read_session};
use super::*;

// 在传入 Connection 上创建连接私有 TEMP 表；它随 SQLite 连接生命周期存在，不是 Store Root 文件。
// 后续 staging 查询必须复用该连接，Artifact 事务中 promotion 才写 durable CAS。
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
    // 无输入过滤条件，返回整个 Store 的 Artifact/BLOB 数量和字节汇总；它只读，不回收或修复数据。
    // 多条 SELECT 在同一连接上依次执行但没有显式事务，因此并发写入时各计数不保证同一时点快照。
    // 有依赖表时用递归 CTE 从 Artifact/嵌入引用沿父依赖计算可达 BLOB；旧 schema 只检查直接引用。
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
        // 通过 schema 能力选择两种查询；递归 CTE 的 UNION 同时去重，避免重复依赖重复计数。
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

    // 输入 Run、一个尚不存在的目标目录、以及是否请求 raw model；此入口用 Path::starts_with 与 Store Root 做路径组件比较，
    // 不 canonicalize target。输出清单和独立 SQLite 导出，后续 payload 写入才在目标库事务中原子提交。
    // 先校验 Store、workflow 与 Debug 导出能力，再按图/Task/event 起点沿 source_refs 做 Artifact 闭包。
    // raw model 能力由 Store 内 Debug 身份决定；导出不推进 Run/Decision。目录和 schema 在 payload 事务前创建，
    // 后续失败不会自动删除目标目录/空 schema；导出库成功也不会注册回原 Store。
    pub fn export_run(
        &self,
        run_id: &RunId,
        target: impl AsRef<Path>,
        include_raw_model: bool,
    ) -> StoreResult<RunExportManifest> {
        self.verify_integrity()?;
        let workflow = self.workflow_snapshot(run_id)?;
        // 借用原 Store 连接完成 raw-model 身份核验；显式 drop 让 Mutex 在后续查询前释放。
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
        // BTreeSet 同时提供待处理去重与确定性取出顺序；visited 阻止 source_refs 环路重复扩展。
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
        // 目标库先创建 schema，再以单个立即事务写全部内容 BLOB 与 manifest；任何循环内错误
        // 退出时 Transaction Drop 回滚该事务，但已创建的文件/表仍保留。
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

    // `impl AsRef<Path>` 接受具体路径类型并在入口转为拥有型 PathBuf；不通过 dyn Trait 分发。
    // 输入不存在的目标目录；此入口同样用 Path::starts_with 对传入路径和 Store Root 做词法组件比较，
    // 不先 canonicalize target。先完整性校验，再由 SQLite VACUUM INTO 生成独立快照，
    // 最后读取快照哈希、文件字节和 BLOB 统计形成 BackupManifest，不创建业务 Artifact。
    // 目标目录在 VACUUM 前创建；失败不会由该方法自动删除目录或数据库残留。
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

    // 输入源文件/备份目录与一个不存在的目标 Root；先只读打开并验证源，再拷贝数据库，
    // 最后将目标按只读既有 Store 重开并复验。它复制的是已验证快照，不迁移 schema、不建 Policy，
    // 也不激活运行能力；中途失败可能留下已创建的目标目录/文件。
    // 与 backup_to/export_run 不同，此入口只检查 target 是否已存在，没有执行 Store Root 路径包含检查；
    // 调用方必须自行选择预期的独立目标 Root，本批只记录当前边界，不改写行为。
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

    // 借用输入字节、取得媒体类型后直接把 CAS BLOB 写进 durable rebuild_blobs，返回逻辑 hash/长度引用。
    // 本方法不同时写 Artifact 或 event；多对象提交与事务边界由上层 Store 方法负责。
    pub fn put_bytes(&self, bytes: &[u8], media_type: impl Into<String>) -> StoreResult<BlobRef> {
        let connection = self.connection()?;
        put_blob_bytes(&connection, bytes, media_type.into())
    }

    // 泛型 T 只需实现 Serialize；具体 T 编译期实例化（静态分发），借用 value 编码为 owned JSON Vec，
    // 再复用 put_bytes 的 CAS/压缩校验。序列化失败经 `?` 转为 StoreError，成功不自动建立 Artifact。
    pub fn put_json<T: Serialize>(&self, value: &T) -> StoreResult<BlobRef> {
        self.put_bytes(&serde_json::to_vec(value)?, "application/json")
    }

    // 借用原始字节；若 durable CAS 已有相同内容则直接复用，否则暂存到当前连接 TEMP 表。
    // 后一种情况下同一 Store 副本共享该连接可读，其他独立连接不可见；返回 BlobRef
    // 本身不保证新负载已 durable，引用它的 Artifact 事务 promotion 后才持久。
    /// 已有 durable CAS 时直接复用；新内容只暂存，不因返回 `BlobRef` 而持久。
    /// 未提升的新负载仅能从这条 Store 连接读取，引用它的 Artifact 事务才会提升。
    pub fn stage_bytes(&self, bytes: &[u8], media_type: impl Into<String>) -> StoreResult<BlobRef> {
        let connection = self.connection()?;
        stage_blob_bytes(&connection, bytes, media_type.into())
    }

    // 泛型 T: Serialize 对具体 T 静态实例化；value 借用序列化成 owned Vec，编码错误发生在拿连接之前，
    // 成功只改变临时连接状态，持久化仍由后续 Artifact 事务提升。
    pub fn stage_json<T: Serialize>(&self, value: &T) -> StoreResult<BlobRef> {
        self.stage_bytes(&serde_json::to_vec(value)?, "application/json")
    }

    // 借用父 BlobRef 和半开区间 [start_byte, end_byte)，先读取父逻辑字节，再要求区间存在且非空；
    // 成功 staging 子字节并记录父引用/范围。越界、反向或空区间都返回 Integrity，不写 durable CAS。
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

    // 泛型 T: Serialize 先静态实例化并把借用 value 序列化为 owned Vec，再把字典长度转为 usize
    // 并校验父 BLOB 在当前连接可读；
    // 转换/读取失败则不生成派生 BLOB。真正比较字典压缩率在 promotion 阶段进行。
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

    // 输入 BlobRef，单次获取 Store 唯一连接并返回校验后的逻辑字节；不可在调用方已持连接时重入。
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

// 供已持有 Connection/Transaction 的内部路径读取；优先看同连接 TEMP staging，再读 durable CAS。
// 该变体不获取 Mutex，调用方必须保证传入连接仍有效且其事务边界符合当前写入流程。
// 借用调用方现有连接读取 BlobRef；既保留同连接未提交/staging 可见性，也避免重新获取非重入 Mutex。
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
    // Debug purpose 是旧诊断路径的直接允许分支；其他 purpose 的任一读取/身份错误都 fail closed 为 false。
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
    // 入参 bytes/media_type 只借用或移动到 BlobRef；依赖是可选父 BlobRef 与逻辑字节范围。
    // 返回的 hash 始终由未压缩逻辑字节计算，encoding_hint 只决定将来 promotion 的物理表示。
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
    // 先查 durable CAS：已存在则复用同一内容身份；仅 MissingBlob 表示可以 staging，其他损坏/SQL 错误传播。
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
    // `INSERT OR IGNORE` 用内容 hash 作主键，使相同逻辑负载可复用已有 staging 行；随后回读逐字节验身份。
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
    // Option::Some 表示临时 payload 命中，None 才回退 durable 表；临时行损坏返回 Err，不伪装为缓存未命中。
    if let Some(bytes) = read_staged_blob_bytes(connection, hash, expected_bytes)? {
        return Ok(bytes);
    }
    read_blob_bytes(connection, hash, expected_bytes)
}

// 将临时 BLOB 按提示提升到 durable CAS，并在删除临时行前确认最终引用仍与原 hash/长度一致。
// 该函数运行在调用方的 Artifact 事务内，失败会让外层事务回滚而不会留下半成品引用。
pub(super) fn promote_staged_blob(connection: &Connection, blob: &BlobRef) -> StoreResult<()> {
    // 由引用它的 Artifact 写事务调用。未 staging 时仅验证 durable 引用；staging 时先解码校验，
    // 再按提示生成 durable 编码并比对整个 BlobRef，最后才删除 TEMP 行，避免先删后写的丢失窗口。
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
    // let-else 将无 staging 行作为明确早退；SQLite 查询错误由上面的 `?` 返回。
    let Some((encoding_hint, dependency_hash, start_byte, end_byte)) = staged else {
        read_blob_bytes(connection, &blob.hash, blob.bytes)?;
        return Ok(());
    };
    let bytes = read_staged_blob_bytes(connection, &blob.hash, blob.bytes)?
        .ok_or_else(|| StoreError::MissingBlob(blob.hash.clone()))?;
    // 不同提示对应不同的物理编码，但都必须还原到相同的逻辑字节。
    // match 穷尽本 Store 支持的提示：普通字节直接存，切片与字典先确保父 BLOB durable 后重建依赖。
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
    // 只有 durable 负载已经完成 identity 校验后才消费临时 staging 行；外层事务失败时此 DELETE 也回滚。
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
    // 输入依赖哈希和可选范围；先查询 durable 父项，若未找到则从本连接 staging 读取并递归提升。
    // ContentHash::new 失败或两处都没有该哈希都会向上传播，不能构造未验证的依赖引用。
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
    // SQL 用 hash 精确查找当前连接临时表；None 是没有行，Some 必须同时匹配声明长度、实际长度和 hash。
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
    // 直接入口只验证媒体类型，再计算标准 identity/zstd 表示并插入；是否在事务内由调用方 Connection 决定。
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
    // 父 BlobRef 被借用而非消费；先读出并校验父逻辑字节，usize::try_from 防止数据库范围超出本机索引宽度。
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
    // 先计算普通表示；小于阈值立即回退。达到阈值后才读字典并压缩，只有满足 MIN_SAVINGS 才记录字典依赖。
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
    // 返回的 encoding 是下列静态字符串字面量之一，因此 `&'static str` 与输入 bytes 生命周期无关。
    // 依据字节长度决定是否创建压缩结果；只有压缩长度加上最小节省仍严格小于原长才选 zstd。
    // 此处能确认选择规则，不能仅由代码断言实际运行更快或总分配更少。
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
    // hash 唯一键实现内容去重；新增行才写 dependency，随后无论新旧都递归读回并逐字节核对。
    // 派生 BLOB 与依赖若由事务连接写入则共同回滚；本函数自身不调用 commit。
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
    // 新建一次调用专属 visiting 集，再递归解码；结果最后还需符合 BlobRef 声明的长度。
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
    // Connection 由上层借用，函数不获取连接锁。深度上限与 visiting 集分别阻断过长依赖链和环；
    // 集合经可变借用传入递归，所有递归读取都属于同一次逻辑读取，任何错误由 `?` 向最外层传播。
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
    // encoding 决定从 payload 直接取值、zstd 解压，或先递归读父 BLOB 再重建切片/字典内容。
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

// 派生编码在数据库中的父哈希和半开区间；只由读取器内部构造，不作为独立公共 API。
// 从依赖表读取某个派生 BLOB 的唯一依赖，并校验 ordinal、类型和范围形状。
fn read_blob_dependency(
    connection: &Connection,
    hash: &ContentHash,
    expected_kind: &str,
) -> StoreResult<BlobDependency> {
    // 先确认旧 schema 具备依赖表，再按 blob_hash 拉全量依赖行；模式解构要求恰有一个 ordinal=0 记录。
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
    // 读取所有 hash/逻辑长度并逐个完整解码校验；任何一项失败都会中止整个 Doctor 检查。
    // 本函数不执行删除或修复，尾部 SQL 另核对派生 encoding 与 dependency_kind 的配对关系。
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
    // sqlite_master 按固定表名判断 schema 能力；无匹配行返回 false，查询错误仍返回 Err。
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
        // label 仅用于隔离 target 下的临时测试 Root；RunId::new 生成互不相同的目录后 open 会初始化 Store。
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
        // move 闭包取得 Store、BlobRef 与 sender 的所有权；新线程调用 Store API 并通过 Channel 发送结果。
        let worker = std::thread::spawn(move || {
            let result = store.read_blob(&staged);
            // Ignore send failures: the receiver has already given up on timeout.
            let _ = sender.send(result.map(|bytes| bytes.len()));
        });

        // recv_timeout 给出明确上限；join 等待线程结束，证明结果路径不会把连接锁永久占住。
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
