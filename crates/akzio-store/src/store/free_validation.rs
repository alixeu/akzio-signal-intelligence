// 文件导读：本文件包含 Store Root 权限、schema 初始化/升级、v18 DDL、Contract catalogue
// 和 Artifact 插入校验；当前初始化 DDL 与 schema label 同一事务提交，但 metadata 表探测/创建及历史 v13/v14
// 迁移在该事务外分阶段完成，迁移不重写既有 CAS/历史 hash。
// 仅可写 `Store::open` 从 schema.rs 调入 initialize；`open_existing` 不迁移 schema。
// 读本文件时先看 initialize 的旧版本分支，
// 再看 insert_artifact 的 CAS promotion/source refs，最后查看文件权限和 Contract helper。
// `cfg(unix)` 只启用 Unix 权限 API；Store SQL 仍通过 rusqlite 连接执行，不建立第二份持久化状态。
// 对已写入的文件调用 sync_all；它只提供文件落盘检查，不是 SQLite 事务提交证明。
fn sync_file(path: &Path) -> StoreResult<()> {
    // Path 按值借用，打开已有文件并请求 fsync；I/O 错误带上路径返回，不改变数据库事务状态。
    let file = fs::File::open(path).map_err(|source| StoreError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    file.sync_all().map_err(|source| StoreError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(())
}

// Unix 下把 Store Root/导出目录权限收紧为 owner-only；非 Unix 平台保持文件系统默认行为。
fn secure_directory(path: &Path) -> StoreResult<()> {
    // cfg(unix) 条件编译使 PermissionsExt 只在 Unix 构建；其他目标编译为空操作并仍返回 Ok。
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(path)
            .map_err(|source| StoreError::Io {
                path: path.to_path_buf(),
                source,
            })?
            .permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(path, permissions).map_err(|source| StoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    }
    Ok(())
}

// Unix 下把 SQLite/导出文件权限设为 0600，不涉及数据库内容或 schema。
fn secure_file(path: &Path) -> StoreResult<()> {
    // 仅在 Unix 改权限位，不改文件字节；文件系统拒绝读取/设置权限时向调用方返回 Io。
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(path)
            .map_err(|source| StoreError::Io {
                path: path.to_path_buf(),
                source,
            })?
            .permissions();
        permissions.set_mode(0o600);
        fs::set_permissions(path, permissions).map_err(|source| StoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    }
    Ok(())
}

/// Refuse to upgrade a Store Root that an older binary may still be working on.
///
/// A table that does not exist yet cannot hold active work, so its absence is
/// "nothing active" rather than an error: the tables are created by the schema
/// batch further down, and a raw `no such table` here would mask the real
/// upgrade decision.
///
/// 与 `workflow::contract_upgrade_blockers` 一样，queued/leased/running Task
/// 都是升级阻断；此处还独立检查未过期 daemon lease，不能把两种检查视作完全同一查询。
// 迁移前只读取 queued/leased/running Task 和未过期 daemon lease，旧 worker 活跃时 fail closed。
// 表不存在视为尚无 active work；先查 Task，若已阻断则不再查 daemon lease。
// expires_at 以本机当前 UTC 时间筛选，返回 Err 表示调用方不得继续旧 Store schema 升级。
fn assert_no_active_work_before_upgrade(connection: &Connection) -> StoreResult<()> {
    let mut blocked = false;
    if migration::table_exists(connection, "rebuild_tasks")? {
        blocked = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM rebuild_tasks WHERE status IN ('queued', 'leased', 'running'))",
            [],
            |row| row.get(0),
        )?;
    }
    if !blocked && migration::table_exists(connection, "rebuild_daemon_leases")? {
        blocked = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM rebuild_daemon_leases WHERE expires_at > ?1)",
            params![Utc::now().to_rfc3339()],
            |row| row.get(0),
        )?;
    }
    if blocked {
        return Err(StoreError::DebugControl(
            "stop old workers and drain active leases before Store 18 migration".into(),
        ));
    }
    Ok(())
}

// 为新建/升级 Store 创建或校验 rebuild_* 表、索引，并在 Immediate 事务中写入当前 metadata.schema_version。
// metadata anchor 表先于此事务确保存在；v13/v14 的两段历史迁移则由上方分阶段完成。
fn initialize(connection: &mut Connection, root: &Path) -> StoreResult<()> {
    // 先在事务外确保 metadata 表并读取旧 version，拒绝结构标签不相容或仍有旧 worker 的 Store；
    // 13/14 的历史迁移先单独提交到 v15，再进入下方 DDL Immediate 事务。
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS rebuild_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);",
    )?;
    let version = connection
        .query_row(
            "SELECT value FROM rebuild_metadata WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if version.is_some()
        && !table_has_column(
            connection,
            "rebuild_policy_evaluations",
            "candidate_policy_artifact_id",
        )?
    {
        return Err(StoreError::IncompatibleStoreRoot(root.to_path_buf()));
    }
    // The upgrade block runs before any migration touches the schema.
    // `migrate_v13_to_v14` drops and rebuilds five tables with foreign keys
    // disabled, so checking afterwards could only report a conflict the
    // migration had already acted on.
    if version.as_deref().is_some_and(|v| v != "18") {
        assert_no_active_work_before_upgrade(connection)?;
    }
    match version.as_deref() {
        None | Some("14") | Some("15") | Some("16") | Some("17") | Some("18") => {}
        Some("13") => migration::migrate_v13_to_v14(connection, root)?,
        Some(_) => {
            return Err(StoreError::IncompatibleStoreRoot(PathBuf::from(
                DATABASE_FILE,
            )));
        }
    }
    // Runs before the schema batch below, because that batch commits
    // `schema_version = 18` and this step still requires the v14 label. Its own
    // Contract-upgrade blockers keep a v14 root readable by the prior binary.
    if matches!(version.as_deref(), Some("13" | "14")) {
        migration::migrate_v14_to_v15(connection)?;
    }
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    // 旧 Task 表可能缺 node_spec_json，只在列缺失时 ALTER；随后批量 CREATE IF NOT EXISTS 保持既有行不重写。
    if migration::table_exists(&transaction, "rebuild_tasks")? && !table_has_column(&transaction, "rebuild_tasks", "node_spec_json")? {
        transaction.execute_batch("ALTER TABLE rebuild_tasks ADD COLUMN node_spec_json TEXT;")?;
    }
    transaction.execute_batch(
        "CREATE TABLE IF NOT EXISTS rebuild_blobs (
    blob_hash TEXT PRIMARY KEY,
    logical_bytes INTEGER NOT NULL,
    stored_bytes INTEGER NOT NULL,
    encoding TEXT NOT NULL,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS rebuild_blob_dependencies (
    blob_hash TEXT NOT NULL REFERENCES rebuild_blobs(blob_hash) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    dependency_kind TEXT NOT NULL,
    dependency_blob_hash TEXT NOT NULL REFERENCES rebuild_blobs(blob_hash)
        DEFERRABLE INITIALLY DEFERRED,
    start_byte INTEGER NOT NULL,
    end_byte INTEGER NOT NULL,
    PRIMARY KEY (blob_hash, ordinal),
    CHECK (blob_hash != dependency_blob_hash),
    CHECK (start_byte >= 0),
    CHECK (end_byte >= start_byte)
);
CREATE TABLE IF NOT EXISTS rebuild_artifacts (
           artifact_id TEXT PRIMARY KEY,
           kind TEXT NOT NULL,
           blob_hash TEXT NOT NULL REFERENCES rebuild_blobs(blob_hash),
           media_type TEXT NOT NULL,
           bytes INTEGER NOT NULL,
           producer TEXT NOT NULL,
           lifecycle TEXT NOT NULL,
           provenance_json TEXT NOT NULL,
           origin_json TEXT,
           created_at TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS rebuild_artifact_refs (
           artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
           source_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
           source_kind TEXT NOT NULL,
           PRIMARY KEY (artifact_id, source_artifact_id)
         );
         CREATE TABLE IF NOT EXISTS rebuild_embedded_blob_refs (
           artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
           role TEXT NOT NULL,
           ordinal INTEGER NOT NULL,
           blob_hash TEXT NOT NULL REFERENCES rebuild_blobs(blob_hash),
           PRIMARY KEY (artifact_id, role, ordinal)
         );
CREATE TABLE IF NOT EXISTS rebuild_runs (
    run_id TEXT PRIMARY KEY,
    purpose TEXT NOT NULL,
    topology_id TEXT NOT NULL,
    graph_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    finished_at TEXT
);
CREATE TABLE IF NOT EXISTS rebuild_run_cancellations (
    run_id TEXT PRIMARY KEY REFERENCES rebuild_runs(run_id),
    reason TEXT NOT NULL,
    requested_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rebuild_workflow_revisions (
           run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
           revision INTEGER NOT NULL,
           graph_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
           created_at TEXT NOT NULL,
           PRIMARY KEY (run_id, revision)
         );
 CREATE TABLE IF NOT EXISTS rebuild_tasks (
           task_id TEXT PRIMARY KEY,
           run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
           recipe_id TEXT NOT NULL,
           objective TEXT NOT NULL,
           contract_hash TEXT,
           priority INTEGER NOT NULL,
           budget_json TEXT NOT NULL,
           retry_json TEXT NOT NULL,
 on_failure TEXT NOT NULL,
 parent_task_id TEXT,
 input_artifacts_json TEXT NOT NULL,
 node_spec_json TEXT,
 status TEXT NOT NULL,
           ready_at TEXT NOT NULL,
           lease_id TEXT,
           lease_epoch INTEGER NOT NULL DEFAULT 0,
           active_attempt_id TEXT,
           lease_until TEXT,
           worker_id TEXT,
           finished_at TEXT
         );
         CREATE TABLE IF NOT EXISTS rebuild_task_dependencies (
           task_id TEXT NOT NULL REFERENCES rebuild_tasks(task_id),
           depends_on_task_id TEXT NOT NULL REFERENCES rebuild_tasks(task_id),
           PRIMARY KEY (task_id, depends_on_task_id)
         );
         CREATE TABLE IF NOT EXISTS rebuild_attempts (
           attempt_id TEXT PRIMARY KEY,
           task_id TEXT NOT NULL REFERENCES rebuild_tasks(task_id),
           run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
           lease_id TEXT NOT NULL,
           epoch INTEGER NOT NULL,
           worker_id TEXT NOT NULL,
           status TEXT NOT NULL,
           started_at TEXT NOT NULL,
           finished_at TEXT
         );
CREATE TABLE IF NOT EXISTS rebuild_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
    task_id TEXT REFERENCES rebuild_tasks(task_id),
    attempt_id TEXT REFERENCES rebuild_attempts(attempt_id),
    event_type TEXT NOT NULL,
    artifact_id TEXT REFERENCES rebuild_artifacts(artifact_id),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rebuild_attempt_outputs (
    attempt_id TEXT NOT NULL REFERENCES rebuild_attempts(attempt_id),
    task_id TEXT NOT NULL REFERENCES rebuild_tasks(task_id),
    artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    event_id INTEGER NOT NULL UNIQUE REFERENCES rebuild_events(event_id),
    PRIMARY KEY (attempt_id, artifact_id)
);
CREATE TABLE IF NOT EXISTS rebuild_daemon_leases (
  lease_name TEXT PRIMARY KEY,
  owner_id TEXT NOT NULL,
  epoch INTEGER NOT NULL,
  expires_at TEXT NOT NULL,
  heartbeat_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rebuild_session_slots (
    session_key TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
  topology_id TEXT NOT NULL,
  graph_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
  run_created_at TEXT NOT NULL,
  scheduler_epoch INTEGER NOT NULL,
  reserved_at TEXT NOT NULL,
    commitment_artifact_id TEXT REFERENCES rebuild_artifacts(artifact_id),
    committed_at TEXT
);
CREATE TABLE IF NOT EXISTS rebuild_paper_approval_consumptions (
    approval_artifact_id TEXT PRIMARY KEY REFERENCES rebuild_artifacts(artifact_id),
    runtime_manifest_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    session_key TEXT NOT NULL UNIQUE REFERENCES rebuild_session_slots(session_key),
    consumed_at TEXT NOT NULL
);
        CREATE TABLE IF NOT EXISTS rebuild_execution_reprices (
    commitment_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    asset TEXT NOT NULL,
    reprice_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    created_at TEXT NOT NULL,
    PRIMARY KEY (commitment_artifact_id, asset),
            UNIQUE (reprice_artifact_id)
        );
        CREATE TABLE IF NOT EXISTS rebuild_execution_cancels (
            commitment_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
            asset TEXT NOT NULL,
            cancel_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
            created_at TEXT NOT NULL,
            PRIMARY KEY (commitment_artifact_id, asset),
            UNIQUE (cancel_artifact_id)
        );
CREATE TABLE IF NOT EXISTS rebuild_policy_transitions (
    transition_id TEXT PRIMARY KEY,
    subject_id TEXT NOT NULL,
    from_state_json TEXT NOT NULL,
    to_state_json TEXT NOT NULL,
    evaluation_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
    revision INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    event_cursor INTEGER NOT NULL UNIQUE REFERENCES rebuild_events(event_id),
    UNIQUE(subject_id, revision)
);
CREATE TABLE IF NOT EXISTS rebuild_contract_installations (
    contract_hash TEXT PRIMARY KEY,
    contract_artifact_id TEXT NOT NULL UNIQUE REFERENCES rebuild_artifacts(artifact_id),
    contract_id TEXT NOT NULL,
    contract_version INTEGER NOT NULL,
    purpose TEXT NOT NULL,
    baseline_contract_hash TEXT REFERENCES rebuild_contract_installations(contract_hash),
    installed_at TEXT NOT NULL,
    UNIQUE(contract_id, contract_version)
);
CREATE TABLE IF NOT EXISTS rebuild_contract_activations (
    activation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    purpose TEXT NOT NULL,
    previous_contract_hash TEXT REFERENCES rebuild_contract_installations(contract_hash),
    contract_hash TEXT NOT NULL REFERENCES rebuild_contract_installations(contract_hash),
    policy_transition_id TEXT UNIQUE REFERENCES rebuild_policy_transitions(transition_id),
    activated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rebuild_contract_catalogue_heads (
    purpose TEXT PRIMARY KEY,
    contract_hash TEXT NOT NULL REFERENCES rebuild_contract_installations(contract_hash),
    activation_id INTEGER NOT NULL UNIQUE REFERENCES rebuild_contract_activations(activation_id)
);
CREATE TABLE IF NOT EXISTS rebuild_policy_evaluations (
    evaluation_artifact_id TEXT PRIMARY KEY REFERENCES rebuild_artifacts(artifact_id),
    subject_id TEXT NOT NULL,
    outcome_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    experience_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    candidate_policy_artifact_id TEXT UNIQUE REFERENCES rebuild_artifacts(artifact_id),
    from_state_json TEXT NOT NULL,
    to_state_json TEXT NOT NULL,
    transition_id TEXT UNIQUE REFERENCES rebuild_policy_transitions(transition_id),
    run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
    consumed_pair_cursor INTEGER NOT NULL,
    event_cursor INTEGER NOT NULL UNIQUE REFERENCES rebuild_events(event_id),
    completed_at TEXT NOT NULL,
    UNIQUE(subject_id, event_cursor)
);
CREATE TABLE IF NOT EXISTS rebuild_policy_consumption_heads (
    subject_id TEXT PRIMARY KEY,
    consumed_pair_cursor INTEGER NOT NULL,
    evaluation_artifact_id TEXT NOT NULL REFERENCES rebuild_policy_evaluations(evaluation_artifact_id),
    evaluation_event_cursor INTEGER NOT NULL UNIQUE REFERENCES rebuild_events(event_id),
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rebuild_policy_heads (
    subject_id TEXT PRIMARY KEY,
    state_json TEXT NOT NULL,
    revision INTEGER NOT NULL,
    transition_id TEXT NOT NULL REFERENCES rebuild_policy_transitions(transition_id),
    transition_event_cursor INTEGER NOT NULL UNIQUE REFERENCES rebuild_events(event_id),
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rebuild_decision_policy_installations (
    policy_hash TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL UNIQUE REFERENCES rebuild_artifacts(artifact_id),
    envelope_hash TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    model_version_hash TEXT NOT NULL,
    model_route TEXT NOT NULL,
    contract_hash TEXT NOT NULL,
    installed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rebuild_decision_policy_activations (
    activation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    previous_policy_hash TEXT REFERENCES rebuild_decision_policy_installations(policy_hash),
    policy_hash TEXT NOT NULL REFERENCES rebuild_decision_policy_installations(policy_hash),
    activated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rebuild_decision_policy_head (
    singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
    policy_hash TEXT NOT NULL REFERENCES rebuild_decision_policy_installations(policy_hash),
    activation_id INTEGER NOT NULL UNIQUE REFERENCES rebuild_decision_policy_activations(activation_id)
);
CREATE TABLE IF NOT EXISTS rebuild_shadow_pairs (
    pair_key TEXT PRIMARY KEY,
    subject_id TEXT NOT NULL,
    parent_decision_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    execution_context_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    candidate_decision_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    candidate_contract_hash TEXT NOT NULL,
    candidate_topology_id TEXT NOT NULL,
    horizon TEXT NOT NULL,
    parent_outcome_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    candidate_outcome_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    completed_at TEXT NOT NULL,
    pair_event_cursor INTEGER NOT NULL UNIQUE REFERENCES rebuild_events(event_id)
);
CREATE TABLE IF NOT EXISTS rebuild_canary_campaigns (
    campaign_id TEXT PRIMARY KEY,
    spec_json TEXT NOT NULL,
    status_json TEXT NOT NULL,
    last_verdict_json TEXT,
    revision INTEGER NOT NULL,
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS rebuild_canary_one_active
    ON rebuild_canary_campaigns(active) WHERE active = 1;
CREATE TABLE IF NOT EXISTS rebuild_canary_sessions (
    campaign_id TEXT NOT NULL REFERENCES rebuild_canary_campaigns(campaign_id),
    level_json TEXT NOT NULL,
    session_key TEXT NOT NULL UNIQUE,
    parent_run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
    contract_shadow_run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
    topology_shadow_run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
    bundle_shadow_run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
    scheduler_epoch INTEGER NOT NULL,
    reserved_at TEXT NOT NULL,
    PRIMARY KEY (campaign_id, level_json)
);
CREATE TABLE IF NOT EXISTS rebuild_canary_cohort_sessions (
    cohort_id TEXT NOT NULL,
    campaign_id TEXT NOT NULL REFERENCES rebuild_canary_campaigns(campaign_id),
    stage_json TEXT NOT NULL,
    session_key TEXT NOT NULL UNIQUE,
    market_day TEXT NOT NULL,
    regime TEXT NOT NULL,
    parent_run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
    contract_shadow_run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
    topology_shadow_run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
    bundle_shadow_run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
    scheduler_epoch INTEGER NOT NULL,
    reserved_at TEXT NOT NULL,
    PRIMARY KEY (cohort_id, session_key)
);
CREATE TABLE IF NOT EXISTS rebuild_canary_observations (
    observation_id TEXT PRIMARY KEY,
    cohort_id TEXT NOT NULL,
    campaign_id TEXT NOT NULL REFERENCES rebuild_canary_campaigns(campaign_id),
    stage_json TEXT NOT NULL,
    session_key TEXT NOT NULL,
    horizon_json TEXT NOT NULL,
    observation_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    UNIQUE (cohort_id, session_key, horizon_json)
);
CREATE TABLE IF NOT EXISTS rebuild_canary_evaluations (
    evaluation_id TEXT PRIMARY KEY,
    cohort_id TEXT NOT NULL,
    campaign_id TEXT NOT NULL REFERENCES rebuild_canary_campaigns(campaign_id),
    stage_json TEXT NOT NULL,
    evaluation_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rebuild_observatory_configuration (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    configuration_json BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS rebuild_run_controls (
    run_id TEXT PRIMARY KEY REFERENCES rebuild_runs(run_id) ON DELETE CASCADE,
    identity_artifact_id TEXT REFERENCES rebuild_artifacts(artifact_id),
    runtime_identity TEXT,
    revision INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('running','pause_requested','paused','stepping','completed','aborted')),
    execution_mode TEXT NOT NULL CHECK(execution_mode IN ('manual','continuous')),
    permitted_task_id TEXT REFERENCES rebuild_tasks(task_id),
    active_attempt_id TEXT REFERENCES rebuild_attempts(attempt_id),
    paused_at_task_id TEXT REFERENCES rebuild_tasks(task_id),
    updated_at TEXT NOT NULL,
    CHECK(permitted_task_id IS NULL OR (status = 'stepping' AND active_attempt_id IS NULL))
);
CREATE INDEX IF NOT EXISTS rebuild_tasks_claimable
    ON rebuild_tasks(status, ready_at, priority);
CREATE INDEX IF NOT EXISTS rebuild_events_cursor
    ON rebuild_events(run_id, event_id);
CREATE INDEX IF NOT EXISTS rebuild_attempt_outputs_cursor
    ON rebuild_attempt_outputs(attempt_id, event_id);
CREATE INDEX IF NOT EXISTS rebuild_blob_dependencies_parent
    ON rebuild_blob_dependencies(dependency_blob_hash, blob_hash);
CREATE INDEX IF NOT EXISTS rebuild_shadow_pairs_freshness
    ON rebuild_shadow_pairs(subject_id, horizon, pair_event_cursor);
DROP INDEX IF EXISTS rebuild_policy_transitions_subject;
DROP INDEX IF EXISTS rebuild_policy_evaluations_subject;
DROP INDEX IF EXISTS rebuild_lesson_events_cursor;
DROP INDEX IF EXISTS rebuild_lesson_evidence_by_lesson;
-- Rebuildable from Artifact metadata alone. Repaired for every version, not
-- just a fresh root: an initializer interrupted at any version can leave it
-- missing, and SQLite rebuilds it without touching CAS payloads.
CREATE INDEX IF NOT EXISTS rebuild_artifacts_run_kind
    ON rebuild_artifacts (json_extract(origin_json, '$.run_id'), kind, created_at, artifact_id);",
    )?;
    if migration::table_exists(&transaction, "rebuild_debug_sessions")? {
        // 兼容旧 Debug head 表时在同一 DDL 事务内搬到统一 run_controls 后删除旧表。
        transaction.execute_batch("INSERT INTO rebuild_run_controls SELECT * FROM rebuild_debug_sessions; DROP TABLE rebuild_debug_sessions;")?;
    }
    // 为历史 Run 补 control head 后确认 required column；任一检查失败不提交 schema label。
    run_control::backfill_history(&transaction)?;
    if !table_has_column(
        &transaction,
        "rebuild_policy_evaluations",
        "candidate_policy_artifact_id",
    )? {
        return Err(StoreError::IncompatibleStoreRoot(root.to_path_buf()));
    }
    // The version label is written in the same transaction as the schema it
    // describes. Committing the tables first would let a crash leave a v18
    // database still labelled v15/v16, which `Store::open_existing` rejects
    // outright, stranding every read-only seam on a structurally valid Store.
    //
    // The shared control migration changes SQL heads only; no CAS,
    // Commitment or hash is rewritten. Old workers reject v18 on reopen rather
    // than silently ignoring the active policy head.
    match version.as_deref() {
        None => {
            transaction.execute(
                "INSERT INTO rebuild_metadata (key, value) VALUES ('schema_version', ?1)",
                params![STORE_SCHEMA_VERSION.to_string()],
            )?;
        }
        Some("18") => {}
        Some(_) => {
            transaction.execute(
                "UPDATE rebuild_metadata SET value = ?1 WHERE key = 'schema_version'",
                params![STORE_SCHEMA_VERSION.to_string()],
            )?;
        }
    }
    // schema DDL 与当前 label 同一事务提交；迁移前的 v13/v14 步骤不与这段 DDL 共用该事务。
    transaction.commit()?;
    Ok(())
}

// 用 PRAGMA table_info 探测历史列，供兼容迁移选择分支。
fn table_has_column(
    connection: &Connection,
    table: &str,
    required_column: &str,
) -> StoreResult<bool> {
    // table 来自 crate 内部固定迁移常量，required_column 是比较值；PRAGMA 结果通过 collect 全量拥有化。
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(columns.iter().any(|column| column == required_column))
}

// 读取 purpose 当前 head 及 activation id；head 只是 immutable activation history 的游标。
fn contract_catalogue_head(
    connection: &Connection,
    purpose: &ContractPurpose,
) -> StoreResult<Option<(ContentHash, i64)>> {
    // purpose 是唯一过滤键；无 head 返回 None，内容 hash 解码失败仍为 Err。
    let row = connection
        .query_row(
            "SELECT contract_hash, activation_id FROM rebuild_contract_catalogue_heads WHERE purpose = ?1",
            params![purpose.as_str()],
            |row| Ok((row.get::<_, String>(0)?, row.get(1)?)),
        )
        .optional()?;
    row.map(|(hash, activation_id)| Ok((ContentHash::new(hash)?, activation_id)))
        .transpose()
}

// contract_id+version 是安装表唯一身份，重复版本直接拒绝而不是覆盖旧 Artifact。
fn assert_contract_identity_available(
    connection: &Connection,
    contract: &AgentContract,
) -> StoreResult<()> {
    // 单条只读查询探测已安装版本；只要存在任意 hash 就返回 DuplicateContractVersion。
    let existing = connection
        .query_row(
            "SELECT contract_hash FROM rebuild_contract_installations WHERE contract_id = ?1 AND contract_version = ?2",
            params![contract.contract_id.0.as_str(), i64::from(contract.version)],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if existing.is_some() {
        return Err(StoreError::DuplicateContractVersion {
            contract_id: contract.contract_id.clone(),
            version: contract.version,
        });
    }
    Ok(())
}

// 写入不可变 Contract installation 元数据，payload/Artifact 已由调用方同一事务准备。
fn insert_contract_installation(
    transaction: &Transaction<'_>,
    contract: &AgentContract,
    artifact: &Artifact,
    baseline_contract_hash: Option<&ContentHash>,
    installed_at: DateTime<Utc>,
) -> StoreResult<()> {
    // 所有值来自调用方已验证的 Contract/Artifact；此 helper 借用外层事务，不创建 CAS、不 commit。
    transaction.execute(
        r#"INSERT INTO rebuild_contract_installations
           (contract_hash, contract_artifact_id, contract_id, contract_version, purpose,
            baseline_contract_hash, installed_at)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"#,
        params![
            contract.contract_hash.as_str(),
            artifact.artifact_id.0.as_str(),
            contract.contract_id.0.as_str(),
            i64::from(contract.version),
            contract.purpose.as_str(),
            baseline_contract_hash.map(ContentHash::as_str),
            installed_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

// 追加 activation history 行并返回 SQLite rowid，后续 head 更新引用该 immutable event。
fn append_contract_activation(
    transaction: &Transaction<'_>,
    purpose: &ContractPurpose,
    previous_contract_hash: Option<&ContentHash>,
    contract_hash: &ContentHash,
    policy_transition_id: Option<&PolicyTransitionId>,
    activated_at: DateTime<Utc>,
) -> StoreResult<i64> {
    // 插入 append-only history 后返回当前事务的 last_insert_rowid；head 更新仍由调用方在同一事务完成。
    transaction.execute(
        r#"INSERT INTO rebuild_contract_activations
           (purpose, previous_contract_hash, contract_hash, policy_transition_id, activated_at)
           VALUES (?1, ?2, ?3, ?4, ?5)"#,
        params![
            purpose.as_str(),
            previous_contract_hash.map(ContentHash::as_str),
            contract_hash.as_str(),
            policy_transition_id.map(|id| id.0.as_str()),
            activated_at.to_rfc3339(),
        ],
    )?;
    Ok(transaction.last_insert_rowid())
}

// 以 purpose 单例 head 指向刚追加的 activation，不删除或改写既有 activation。
fn set_contract_catalogue_head(
    transaction: &Transaction<'_>,
    purpose: &ContractPurpose,
    contract_hash: &ContentHash,
    activation_id: i64,
) -> StoreResult<()> {
    // 单 purpose 主键 UPSERT 只移动可重建游标；原 activation history 行不会被覆盖或删除。
    transaction.execute(
        r#"INSERT INTO rebuild_contract_catalogue_heads (purpose, contract_hash, activation_id)
           VALUES (?1, ?2, ?3)
           ON CONFLICT(purpose) DO UPDATE SET
             contract_hash = excluded.contract_hash,
             activation_id = excluded.activation_id"#,
        params![purpose.as_str(), contract_hash.as_str(), activation_id],
    )?;
    Ok(())
}

// 只做 capability subset 判断：候选不能扩大 output kind、depth、child tasks 或证据要求。
fn candidate_is_bounded(active: &AgentContract, candidate: &AgentContract) -> bool {
    // 首先委托领域 capability 比较，再附加 purpose/output/termination 限制；纯布尔函数不访问 Store。
    active.permits_candidate(candidate)
        && active.purpose == candidate.purpose
        && active.output.artifact_kind == candidate.output.artifact_kind
        && (!active.termination.require_evidence || candidate.termination.require_evidence)
        && candidate.termination.max_child_tasks <= active.termination.max_child_tasks
        && candidate.termination.max_depth <= active.termination.max_depth
}

// Artifact 插入前提升 staged BLOB、检查 source rows；同 ArtifactId 重放只接受完全相同的 immutable 内容。
fn insert_artifact(transaction: &Transaction<'_>, artifact: &Artifact) -> StoreResult<()> {
    // 调用方必须持有事务；先校验领域对象与提升 CAS，再确认每条 source_ref 已存在且 kind 相符。
    // INSERT OR IGNORE 遇到同 ID 时会重读并比较完整 Artifact；只有首次插入才追加 refs/embedded index。
    artifact.validate()?;
    blob::promote_staged_blob(transaction, &artifact.blob)?;
    for source in &artifact.source_refs {
        let exists = transaction
            .query_row(
                "SELECT kind FROM rebuild_artifacts WHERE artifact_id = ?1",
                params![source.artifact_id.0.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if exists.as_deref() != Some(&enum_name(source.kind)) {
            return Err(StoreError::InvalidArtifactClosure(
                artifact.artifact_id.clone(),
            ));
        }
    }
    let inserted = transaction.execute(
        r#"INSERT OR IGNORE INTO rebuild_artifacts
           (artifact_id, kind, blob_hash, media_type, bytes, producer, lifecycle, provenance_json, origin_json, created_at)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"#,
        params![
            artifact.artifact_id.0.as_str(),
            enum_name(artifact.kind),
            artifact.blob.hash.as_str(),
            artifact.blob.media_type,
            artifact.blob.bytes,
            artifact.producer,
            enum_name(artifact.lifecycle),
            serde_json::to_string(&artifact.provenance)?,
            serde_json::to_string(&artifact.origin)?,
            artifact.created_at.to_rfc3339(),
        ],
    )?;
    if inserted == 0 {
        let existing = read_artifact(transaction, &artifact.artifact_id)?;
        if &existing != artifact {
            return Err(StoreError::Integrity(format!(
                "artifact hash collision {}",
                artifact.artifact_id.0
            )));
        }
        return Ok(());
    }
    for source in &artifact.source_refs {
        transaction.execute(
            r#"INSERT INTO rebuild_artifact_refs
               (artifact_id, source_artifact_id, source_kind)
               VALUES (?1, ?2, ?3)"#,
            params![
                artifact.artifact_id.0.as_str(),
                source.artifact_id.0.as_str(),
                enum_name(source.kind),
            ],
        )?;
    }
    index_embedded_blob_refs(transaction, artifact)?;
    Ok(())
}
