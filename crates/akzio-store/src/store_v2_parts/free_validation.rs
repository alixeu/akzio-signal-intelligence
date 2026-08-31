fn sync_file(path: &Path) -> StoreResult<()> {
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

fn secure_directory(path: &Path) -> StoreResult<()> {
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

fn secure_file(path: &Path) -> StoreResult<()> {
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

fn initialize(connection: &mut Connection, root: &Path) -> StoreResult<()> {
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
    if let Some(value) = version.as_deref() {
        if value != STORE_SCHEMA_VERSION.to_string() {
            return Err(StoreError::IncompatibleStoreRoot(PathBuf::from(
                DATABASE_FILE,
            )));
        }
    }
    connection.execute_batch(
        "BEGIN;
        CREATE TABLE IF NOT EXISTS rebuild_blobs (
           blob_hash TEXT PRIMARY KEY,
           logical_bytes INTEGER NOT NULL,
           stored_bytes INTEGER NOT NULL,
           encoding TEXT NOT NULL,
           payload BLOB NOT NULL
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
CREATE TABLE IF NOT EXISTS rebuild_policy_transitions (
    transition_id TEXT PRIMARY KEY,
    subject_id TEXT NOT NULL,
    subject_json TEXT NOT NULL,
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
    subject_json TEXT NOT NULL,
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
    subject_json TEXT NOT NULL,
    consumed_pair_cursor INTEGER NOT NULL,
    evaluation_artifact_id TEXT NOT NULL REFERENCES rebuild_policy_evaluations(evaluation_artifact_id),
    evaluation_event_cursor INTEGER NOT NULL UNIQUE REFERENCES rebuild_events(event_id),
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rebuild_policy_heads (
    subject_id TEXT PRIMARY KEY,
    subject_json TEXT NOT NULL,
    state_json TEXT NOT NULL,
    revision INTEGER NOT NULL,
    transition_id TEXT NOT NULL REFERENCES rebuild_policy_transitions(transition_id),
    transition_event_cursor INTEGER NOT NULL UNIQUE REFERENCES rebuild_events(event_id),
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rebuild_shadow_pairs (
    pair_key TEXT PRIMARY KEY,
    subject_id TEXT NOT NULL,
    subject_json TEXT NOT NULL,
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
    reservation_json TEXT NOT NULL,
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
CREATE INDEX IF NOT EXISTS rebuild_tasks_claimable
    ON rebuild_tasks(status, ready_at, priority);
CREATE INDEX IF NOT EXISTS rebuild_events_cursor
    ON rebuild_events(run_id, event_id);
CREATE INDEX IF NOT EXISTS rebuild_attempt_outputs_cursor
    ON rebuild_attempt_outputs(attempt_id, event_id);
CREATE INDEX IF NOT EXISTS rebuild_policy_transitions_subject
    ON rebuild_policy_transitions(subject_id, revision);
CREATE INDEX IF NOT EXISTS rebuild_policy_evaluations_subject
    ON rebuild_policy_evaluations(subject_id, event_cursor);
CREATE INDEX IF NOT EXISTS rebuild_shadow_pairs_freshness
    ON rebuild_shadow_pairs(subject_id, horizon, pair_event_cursor);
COMMIT;",
    )?;
    if !table_has_column(
        connection,
        "rebuild_policy_evaluations",
        "candidate_policy_artifact_id",
    )? {
        return Err(StoreError::IncompatibleStoreRoot(root.to_path_buf()));
    }
    if version.is_none() {
        connection.execute(
            "INSERT INTO rebuild_metadata (key, value) VALUES ('schema_version', ?1)",
            params![STORE_SCHEMA_VERSION.to_string()],
        )?;
    }
    Ok(())
}

fn table_has_column(
    connection: &Connection,
    table: &str,
    required_column: &str,
) -> StoreResult<bool> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(columns.iter().any(|column| column == required_column))
}

fn contract_catalogue_head(
    connection: &Connection,
    purpose: &ContractPurpose,
) -> StoreResult<Option<(ContentHash, i64)>> {
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

fn assert_contract_identity_available(
    connection: &Connection,
    contract: &AgentContract,
) -> StoreResult<()> {
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

fn insert_contract_installation(
    transaction: &Transaction<'_>,
    contract: &AgentContract,
    artifact: &Artifact,
    baseline_contract_hash: Option<&ContentHash>,
    installed_at: DateTime<Utc>,
) -> StoreResult<()> {
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

fn append_contract_activation(
    transaction: &Transaction<'_>,
    purpose: &ContractPurpose,
    previous_contract_hash: Option<&ContentHash>,
    contract_hash: &ContentHash,
    policy_transition_id: Option<&PolicyTransitionId>,
    activated_at: DateTime<Utc>,
) -> StoreResult<i64> {
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

fn set_contract_catalogue_head(
    transaction: &Transaction<'_>,
    purpose: &ContractPurpose,
    contract_hash: &ContentHash,
    activation_id: i64,
) -> StoreResult<()> {
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

fn candidate_is_bounded(active: &AgentContract, candidate: &AgentContract) -> bool {
    active.permits_candidate(candidate)
        && active.purpose == candidate.purpose
        && active.output.artifact_kind == candidate.output.artifact_kind
        && (!active.termination.require_evidence || candidate.termination.require_evidence)
        && candidate.termination.max_child_tasks <= active.termination.max_child_tasks
        && candidate.termination.max_depth <= active.termination.max_depth
}

fn insert_artifact(transaction: &Transaction<'_>, artifact: &Artifact) -> StoreResult<()> {
    artifact.validate()?;
    blob::read_blob_bytes(transaction, &artifact.blob.hash, artifact.blob.bytes)?;
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
