use super::*;

struct LegacyCanaryReservationRow {
    cohort_id: String,
    campaign_id: String,
    stage_json: String,
    session_key: String,
    reservation_json: String,
    parent_run_id: String,
    contract_shadow_run_id: String,
    topology_shadow_run_id: String,
    bundle_shadow_run_id: String,
    scheduler_epoch: u64,
    reserved_at: String,
}

struct LegacyLessonEvidenceRow {
    evidence_id: String,
    lesson_id: String,
    lesson_artifact_id: String,
    decision_context_artifact_id: String,
    outcome_artifact_id: String,
    attribution: String,
    evidence_json: String,
    recorded_at: String,
}

pub(super) fn migrate_v13_to_v14(connection: &mut Connection, root: &Path) -> StoreResult<()> {
    connection.pragma_update(None, "foreign_keys", "OFF")?;
    let migration = migrate_v13_to_v14_transaction(connection, root);
    let restore = connection.pragma_update(None, "foreign_keys", "ON");

    match (migration, restore) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error.into()),
        (Ok(()), Ok(())) => Ok(()),
    }
}

fn migrate_v13_to_v14_transaction(connection: &mut Connection, root: &Path) -> StoreResult<()> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;

    for table in [
        "rebuild_policy_transitions",
        "rebuild_policy_evaluations",
        "rebuild_policy_consumption_heads",
        "rebuild_policy_heads",
        "rebuild_shadow_pairs",
    ] {
        if !table_has_column(&transaction, table, "subject_json")? {
            return Err(StoreError::IncompatibleStoreRoot(root.to_path_buf()));
        }
        validate_legacy_policy_subjects(&transaction, table)?;
    }

    migrate_policy_tables(&transaction)?;
    migrate_canary_cohort_sessions(&transaction, root)?;
    migrate_lesson_evidence(&transaction, root)?;

    let violation = transaction
        .query_row("PRAGMA foreign_key_check", [], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .optional()?;
    if let Some((table, row_id, parent, foreign_key)) = violation {
        return Err(StoreError::Integrity(format!(
            "v14 migration foreign key violation in {table} row {row_id:?} referencing {parent} key {foreign_key}"
        )));
    }

    let updated = transaction.execute(
        "UPDATE rebuild_metadata SET value = ?1 WHERE key = 'schema_version' AND value = '13'",
        params!["14"],
    )?;
    if updated != 1 {
        return Err(StoreError::IncompatibleStoreRoot(root.to_path_buf()));
    }

    transaction.commit()?;
    Ok(())
}

fn validate_legacy_policy_subjects(transaction: &Transaction<'_>, table: &str) -> StoreResult<()> {
    let mut statement =
        transaction.prepare(&format!("SELECT subject_id, subject_json FROM {table}"))?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    for (subject_id, subject_json) in rows {
        let subject: PolicySubject = serde_json::from_str(&subject_json)?;
        subject.validate()?;
        if subject.subject_id() != subject_id {
            return Err(StoreError::Integrity(format!(
                "legacy {table} subject JSON disagrees with subject_id {subject_id}"
            )));
        }
    }
    Ok(())
}

fn migrate_policy_tables(transaction: &Transaction<'_>) -> StoreResult<()> {
    transaction.execute_batch(
        r#"
CREATE TABLE rebuild_policy_transitions_v14 (
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
CREATE TABLE rebuild_policy_evaluations_v14 (
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
CREATE TABLE rebuild_policy_consumption_heads_v14 (
    subject_id TEXT PRIMARY KEY,
    consumed_pair_cursor INTEGER NOT NULL,
    evaluation_artifact_id TEXT NOT NULL REFERENCES rebuild_policy_evaluations(evaluation_artifact_id),
    evaluation_event_cursor INTEGER NOT NULL UNIQUE REFERENCES rebuild_events(event_id),
    updated_at TEXT NOT NULL
);
CREATE TABLE rebuild_policy_heads_v14 (
    subject_id TEXT PRIMARY KEY,
    state_json TEXT NOT NULL,
    revision INTEGER NOT NULL,
    transition_id TEXT NOT NULL REFERENCES rebuild_policy_transitions(transition_id),
    transition_event_cursor INTEGER NOT NULL UNIQUE REFERENCES rebuild_events(event_id),
    updated_at TEXT NOT NULL
);
CREATE TABLE rebuild_shadow_pairs_v14 (
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

INSERT INTO rebuild_policy_transitions_v14
    (transition_id, subject_id, from_state_json, to_state_json, evaluation_artifact_id,
     run_id, revision, created_at, event_cursor)
SELECT transition_id, subject_id, from_state_json, to_state_json, evaluation_artifact_id,
       run_id, revision, created_at, event_cursor
FROM rebuild_policy_transitions;

INSERT INTO rebuild_policy_evaluations_v14
    (evaluation_artifact_id, subject_id, outcome_artifact_id, experience_artifact_id,
     candidate_policy_artifact_id, from_state_json, to_state_json, transition_id,
     run_id, consumed_pair_cursor, event_cursor, completed_at)
SELECT evaluation_artifact_id, subject_id, outcome_artifact_id, experience_artifact_id,
       candidate_policy_artifact_id, from_state_json, to_state_json, transition_id,
       run_id, consumed_pair_cursor, event_cursor, completed_at
FROM rebuild_policy_evaluations;

INSERT INTO rebuild_policy_consumption_heads_v14
    (subject_id, consumed_pair_cursor, evaluation_artifact_id, evaluation_event_cursor, updated_at)
SELECT subject_id, consumed_pair_cursor, evaluation_artifact_id, evaluation_event_cursor, updated_at
FROM rebuild_policy_consumption_heads;

INSERT INTO rebuild_policy_heads_v14
    (subject_id, state_json, revision, transition_id, transition_event_cursor, updated_at)
SELECT subject_id, state_json, revision, transition_id, transition_event_cursor, updated_at
FROM rebuild_policy_heads;

INSERT INTO rebuild_shadow_pairs_v14
    (pair_key, subject_id, parent_decision_artifact_id, execution_context_artifact_id,
     candidate_decision_artifact_id, candidate_contract_hash, candidate_topology_id,
     horizon, parent_outcome_artifact_id, candidate_outcome_artifact_id, completed_at,
     pair_event_cursor)
SELECT pair_key, subject_id, parent_decision_artifact_id, execution_context_artifact_id,
       candidate_decision_artifact_id, candidate_contract_hash, candidate_topology_id,
       horizon, parent_outcome_artifact_id, candidate_outcome_artifact_id, completed_at,
       pair_event_cursor
FROM rebuild_shadow_pairs;

DROP TABLE rebuild_policy_consumption_heads;
DROP TABLE rebuild_policy_heads;
DROP TABLE rebuild_policy_evaluations;
DROP TABLE rebuild_shadow_pairs;
DROP TABLE rebuild_policy_transitions;

ALTER TABLE rebuild_policy_transitions_v14 RENAME TO rebuild_policy_transitions;
ALTER TABLE rebuild_policy_evaluations_v14 RENAME TO rebuild_policy_evaluations;
ALTER TABLE rebuild_policy_consumption_heads_v14 RENAME TO rebuild_policy_consumption_heads;
ALTER TABLE rebuild_policy_heads_v14 RENAME TO rebuild_policy_heads;
ALTER TABLE rebuild_shadow_pairs_v14 RENAME TO rebuild_shadow_pairs;

CREATE INDEX rebuild_shadow_pairs_freshness
    ON rebuild_shadow_pairs(subject_id, horizon, pair_event_cursor);
"#,
    )?;
    Ok(())
}

fn migrate_canary_cohort_sessions(transaction: &Transaction<'_>, root: &Path) -> StoreResult<()> {
    if !table_has_column(
        transaction,
        "rebuild_canary_cohort_sessions",
        "reservation_json",
    )? {
        return Err(StoreError::IncompatibleStoreRoot(root.to_path_buf()));
    }

    let mut statement = transaction.prepare(
        "SELECT cohort_id, campaign_id, stage_json, session_key, reservation_json, \
         parent_run_id, contract_shadow_run_id, topology_shadow_run_id, bundle_shadow_run_id, \
         scheduler_epoch, reserved_at FROM rebuild_canary_cohort_sessions",
    )?;
    let rows = statement
        .query_map([], |row| {
            Ok(LegacyCanaryReservationRow {
                cohort_id: row.get(0)?,
                campaign_id: row.get(1)?,
                stage_json: row.get(2)?,
                session_key: row.get(3)?,
                reservation_json: row.get(4)?,
                parent_run_id: row.get(5)?,
                contract_shadow_run_id: row.get(6)?,
                topology_shadow_run_id: row.get(7)?,
                bundle_shadow_run_id: row.get(8)?,
                scheduler_epoch: row.get(9)?,
                reserved_at: row.get(10)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);

    transaction.execute_batch(
        r#"
CREATE TABLE rebuild_canary_cohort_sessions_v14 (
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
"#,
    )?;

    for row in rows {
        let reservation: akzio_domain::CanarySessionReservation =
            serde_json::from_str(&row.reservation_json)?;
        reservation.validate()?;
        let stage: akzio_domain::CanaryCampaignStatus = serde_json::from_str(&row.stage_json)?;
        let market_day = reservation.market_day.ok_or_else(|| {
            StoreError::Integrity(format!(
                "legacy canary cohort session {} has no market day",
                row.session_key
            ))
        })?;
        let regime = reservation.regime.as_deref().ok_or_else(|| {
            StoreError::Integrity(format!(
                "legacy canary cohort session {} has no regime",
                row.session_key
            ))
        })?;
        let expected_cohort = reservation.cohort_id.as_ref().ok_or_else(|| {
            StoreError::Integrity(format!(
                "legacy canary cohort session {} has no cohort",
                row.session_key
            ))
        })?;

        if reservation.campaign_id.as_str() != row.campaign_id
            || expected_cohort.as_str() != row.cohort_id
            || reservation.level != stage
            || reservation.session_key != row.session_key
            || reservation.parent_run_id.0 != row.parent_run_id
            || reservation.contract_shadow_run_id.0 != row.contract_shadow_run_id
            || reservation.topology_shadow_run_id.0 != row.topology_shadow_run_id
            || reservation.bundle_shadow_run_id.0 != row.bundle_shadow_run_id
            || reservation.scheduler_epoch != row.scheduler_epoch
            || reservation.reserved_at != parse_time(&row.reserved_at)?
        {
            return Err(StoreError::Integrity(format!(
                "legacy canary cohort session {} JSON disagrees with indexed columns",
                row.session_key
            )));
        }

        transaction.execute(
            "INSERT INTO rebuild_canary_cohort_sessions_v14 \
             (cohort_id, campaign_id, stage_json, session_key, market_day, regime, parent_run_id, \
              contract_shadow_run_id, topology_shadow_run_id, bundle_shadow_run_id, scheduler_epoch, reserved_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                row.cohort_id,
                row.campaign_id,
                row.stage_json,
                row.session_key,
                market_day.to_string(),
                regime,
                row.parent_run_id,
                row.contract_shadow_run_id,
                row.topology_shadow_run_id,
                row.bundle_shadow_run_id,
                row.scheduler_epoch,
                row.reserved_at,
            ],
        )?;
    }

    transaction.execute_batch(
        "DROP TABLE rebuild_canary_cohort_sessions; \
         ALTER TABLE rebuild_canary_cohort_sessions_v14 RENAME TO rebuild_canary_cohort_sessions;",
    )?;
    Ok(())
}

fn migrate_lesson_evidence(transaction: &Transaction<'_>, root: &Path) -> StoreResult<()> {
    if !table_exists(transaction, "rebuild_lesson_evidence")? {
        return Ok(());
    }
    if !table_has_column(transaction, "rebuild_lesson_evidence", "evidence_json")? {
        return Err(StoreError::IncompatibleStoreRoot(root.to_path_buf()));
    }

    let mut statement = transaction.prepare(
        "SELECT evidence_id, lesson_id, lesson_artifact_id, decision_context_artifact_id, \
         outcome_artifact_id, attribution, evidence_json, recorded_at \
         FROM rebuild_lesson_evidence",
    )?;
    let rows = statement
        .query_map([], |row| {
            Ok(LegacyLessonEvidenceRow {
                evidence_id: row.get(0)?,
                lesson_id: row.get(1)?,
                lesson_artifact_id: row.get(2)?,
                decision_context_artifact_id: row.get(3)?,
                outcome_artifact_id: row.get(4)?,
                attribution: row.get(5)?,
                evidence_json: row.get(6)?,
                recorded_at: row.get(7)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);

    transaction.execute_batch(
        r#"
CREATE TABLE rebuild_lesson_evidence_v14 (
    evidence_id TEXT PRIMARY KEY,
    lesson_id TEXT NOT NULL,
    lesson_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    decision_context_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    outcome_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    attribution TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    UNIQUE(lesson_id, decision_context_artifact_id, outcome_artifact_id)
);
"#,
    )?;

    for row in rows {
        let evidence: LessonEvidence = serde_json::from_str(&row.evidence_json)?;
        evidence.validate()?;
        let (lesson_id, decision_context_id, outcome_id) = evidence.idempotency_key();
        if evidence.identity_hash()?.as_str() != row.evidence_id
            || lesson_id != row.lesson_id
            || evidence.lesson_artifact.artifact_id.0.as_str() != row.lesson_artifact_id
            || decision_context_id != row.decision_context_artifact_id
            || outcome_id != row.outcome_artifact_id
            || evidence.attribution.as_str() != row.attribution
            || evidence.recorded_at != parse_time(&row.recorded_at)?
        {
            return Err(StoreError::Integrity(format!(
                "legacy lesson evidence {} JSON disagrees with indexed columns",
                row.evidence_id
            )));
        }
        let metrics_json = serde_json::to_string(&lesson::LessonEvidenceMetrics::from(&evidence))?;
        transaction.execute(
            "INSERT INTO rebuild_lesson_evidence_v14 \
             (evidence_id, lesson_id, lesson_artifact_id, decision_context_artifact_id, \
              outcome_artifact_id, attribution, metrics_json, recorded_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                row.evidence_id,
                row.lesson_id,
                row.lesson_artifact_id,
                row.decision_context_artifact_id,
                row.outcome_artifact_id,
                row.attribution,
                metrics_json,
                row.recorded_at,
            ],
        )?;
    }

    transaction.execute_batch(
        "DROP TABLE rebuild_lesson_evidence; \
         ALTER TABLE rebuild_lesson_evidence_v14 RENAME TO rebuild_lesson_evidence;",
    )?;
    Ok(())
}

fn table_exists(connection: &Connection, table: &str) -> StoreResult<bool> {
    connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            params![table],
            |_| Ok(()),
        )
        .optional()
        .map(|row| row.is_some())
        .map_err(Into::into)
}

/// v15 adds a rebuildable origin/kind expression index, never rewrites CAS or
/// execution commitments. SQLite rebuilds the index directly from metadata.
pub(super) fn migrate_v14_to_v15(connection: &mut Connection) -> StoreResult<()> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    // Keep v14 readable by the prior binary when its immutable tasks cannot
    // yet switch to the release's v18 Contracts. Never strand them in v15.
    if table_exists(&transaction, "rebuild_contract_catalogue_heads")? {
        let hashes = transaction.prepare("SELECT h.contract_hash FROM rebuild_contract_catalogue_heads h JOIN rebuild_contract_installations i ON i.contract_hash=h.contract_hash WHERE i.contract_version < 18 ORDER BY h.purpose")?
            .query_map([], |row| row.get::<_,String>(0))?.collect::<Result<Vec<_>,_>>()?;
        for hash in hashes {
            let hash = ContentHash::new(hash)?;
            let blockers = super::workflow::contract_upgrade_blockers(&transaction, &hash)?;
            if !blockers.is_empty() {
                return Err(StoreError::ContractUpgradeBlocked {
                    active: hash,
                    blockers: blockers.join(", "),
                });
            }
        }
    }
    transaction.execute_batch("CREATE INDEX IF NOT EXISTS rebuild_artifacts_run_kind ON rebuild_artifacts (json_extract(origin_json, '$.run_id'), kind, created_at, artifact_id);")?;
    if transaction.execute(
        "UPDATE rebuild_metadata SET value = '15' WHERE key = 'schema_version' AND value = '14'",
        [],
    )? != 1
    {
        return Err(StoreError::Integrity(
            "v15 migration requires v14 metadata".to_owned(),
        ));
    }
    transaction.commit()?;
    Ok(())
}
