fn index_embedded_blob_refs(connection: &Connection, artifact: &Artifact) -> StoreResult<()> {
    for (role, ordinal, blob) in embedded_blob_refs(connection, artifact)? {
        connection.execute(
            r#"INSERT INTO rebuild_embedded_blob_refs
               (artifact_id, role, ordinal, blob_hash)
               VALUES (?1, ?2, ?3, ?4)"#,
            params![
                artifact.artifact_id.0.as_str(),
                role,
                ordinal,
                blob.hash.as_str(),
            ],
        )?;
    }
    Ok(())
}

fn embedded_blob_refs(
    connection: &Connection,
    artifact: &Artifact,
) -> StoreResult<Vec<(String, u64, BlobRef)>> {
    if artifact.kind != ArtifactKind::Contract {
        return Ok(Vec::new());
    }
    let payload = blob::read_blob_bytes(connection, &artifact.blob.hash, artifact.blob.bytes)?;
    let contract: AgentContract = serde_json::from_slice(&payload)?;
    contract.validate()?;
    let mut refs = vec![
        (
            "prompt.governance".to_owned(),
            0,
            contract.prompt.governance,
        ),
        ("prompt.role".to_owned(), 0, contract.prompt.role),
        ("output.schema".to_owned(), 0, contract.output.schema),
    ];
    refs.extend(
        contract
            .tool_specs
            .into_iter()
            .enumerate()
            .map(|(ordinal, tool)| {
                (
                    "tool.input_schema".to_owned(),
                    ordinal as u64,
                    tool.input_schema,
                )
            }),
    );
    for (_, _, blob) in &refs {
        blob::read_blob_bytes(connection, &blob.hash, blob.bytes)?;
    }
    Ok(refs)
}

/// Inserts a completion batch in source-closure order. A task may create a
/// RawEvidence artifact and its NormalizedEvidence dependent in the same
/// atomic attempt; callers need not rely on input ordering for correctness.
fn insert_artifact_batch(transaction: &Transaction<'_>, artifacts: &[Artifact]) -> StoreResult<()> {
    let mut pending = BTreeMap::<ArtifactId, &Artifact>::new();
    for artifact in artifacts {
        artifact.validate()?;
        if let Some(existing) = pending.insert(artifact.artifact_id.clone(), artifact) {
            if existing != artifact {
                return Err(StoreError::Integrity(format!(
                    "conflicting completion artifacts for {}",
                    artifact.artifact_id
                )));
            }
        }
    }

    while !pending.is_empty() {
        let ready = pending
            .iter()
            .find(|(_, artifact)| {
                artifact
                    .source_refs
                    .iter()
                    .all(|reference| !pending.contains_key(&reference.artifact_id))
            })
            .map(|(artifact_id, _)| artifact_id.clone());
        let Some(artifact_id) = ready else {
            return Err(StoreError::InvalidArtifactClosure(
                pending
                    .first_key_value()
                    .expect("pending batch is non-empty")
                    .0
                    .clone(),
            ));
        };
        let artifact = pending
            .remove(&artifact_id)
            .expect("ready artifact is still pending");
        insert_artifact(transaction, artifact)?;
    }
    Ok(())
}

fn assert_workflow_input_artifacts(
    transaction: &Transaction<'_>,
    nodes: &[WorkflowNode],
) -> StoreResult<()> {
    let mut visited = BTreeSet::new();
    for reference in nodes.iter().flat_map(|node| &node.input_artifacts) {
        assert_artifact_reference_closure(transaction, reference, &mut visited)?;
    }
    Ok(())
}

fn assert_artifact_reference_closure(
    transaction: &Transaction<'_>,
    reference: &ArtifactRef,
    visited: &mut BTreeSet<ArtifactId>,
) -> StoreResult<()> {
    let artifact = read_artifact(transaction, &reference.artifact_id)?;
    if artifact.kind != reference.kind {
        return Err(StoreError::InvalidArtifactClosure(
            reference.artifact_id.clone(),
        ));
    }
    if !visited.insert(reference.artifact_id.clone()) {
        return Ok(());
    }
    for source in &artifact.source_refs {
        assert_artifact_reference_closure(transaction, source, visited)?;
    }
    Ok(())
}

fn insert_task_node(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    node: &WorkflowNode,
    created_at: DateTime<Utc>,
) -> StoreResult<()> {
    let inserted = transaction.execute(
        r#"INSERT INTO rebuild_tasks
 (task_id, run_id, recipe_id, objective, contract_hash, priority, budget_json, retry_json, on_failure,
 parent_task_id, input_artifacts_json, status, ready_at)
 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 'queued', ?12)"#,
        params![
            node.task_id.0,
            run_id.0,
            node.recipe_id.as_str(),
            node.objective,
            node.contract_hash.as_ref().map(ContentHash::as_str),
            node.priority,
            serde_json::to_string(&node.budget)?,
            serde_json::to_string(&node.retry)?,
            enum_name(node.on_failure),
            node.parent_task_id.as_ref().map(|id| id.0.as_str()),
            serde_json::to_string(&node.input_artifacts)?,
            created_at.to_rfc3339(),
        ],
    )?;
    if inserted != 1 {
        return Err(StoreError::DuplicateTask(node.task_id.clone()));
    }
    Ok(())
}

fn insert_node_dependencies(transaction: &Transaction<'_>, node: &WorkflowNode) -> StoreResult<()> {
    for dependency in &node.dependencies {
        transaction.execute(
            "INSERT INTO rebuild_task_dependencies (task_id, depends_on_task_id) VALUES (?1, ?2)",
            params![node.task_id.0, dependency.0],
        )?;
    }
    Ok(())
}

fn task_dependencies(connection: &Connection, task_id: &TaskId) -> StoreResult<Vec<TaskId>> {
    let dependencies = connection
        .prepare(
            "SELECT depends_on_task_id FROM rebuild_task_dependencies \
             WHERE task_id = ?1 ORDER BY depends_on_task_id ASC",
        )?
        .query_map(params![task_id.0], |row| Ok(TaskId(row.get(0)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(dependencies)
}

fn canonical_workflow_node(mut node: WorkflowNode) -> WorkflowNode {
    node.dependencies.sort();
    node
}

fn assert_permit(transaction: &Transaction<'_>, permit: &TaskWritePermit) -> StoreResult<()> {
    let current = transaction
        .query_row(
            r#"SELECT run_id, status, lease_id, lease_epoch, active_attempt_id, contract_hash
               FROM rebuild_tasks WHERE task_id = ?1"#,
            params![permit.task_id.0],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, u64>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            },
        )
        .optional()?;
    let Some((run_id, status, lease_id, epoch, attempt_id, contract_hash)) = current else {
        return Err(StoreError::MissingTask(permit.task_id.clone()));
    };
    if run_id != permit.run_id.0
        || status != "running"
        || lease_id.as_deref() != Some(permit.lease_id.0.as_str())
        || epoch != permit.epoch
        || attempt_id.as_deref() != Some(permit.attempt_id.0.as_str())
        || contract_hash.as_deref().map(ContentHash::new).transpose()? != permit.contract_hash
    {
        return Err(StoreError::StalePermit(permit.task_id.clone()));
    }
    Ok(())
}

fn assert_daemon_lease(
    transaction: &Transaction<'_>,
    lease: &DaemonLease,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    let current = transaction
        .query_row(
            "SELECT owner_id, epoch, expires_at FROM rebuild_daemon_leases WHERE lease_name = ?1",
            params![lease.lease_name],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?;
    let Some((owner_id, epoch, expires_at)) = current else {
        return Err(StoreError::SchedulerFenced(lease.lease_name.clone()));
    };
    if owner_id != lease.owner_id || epoch != lease.epoch || parse_time(&expires_at)? <= now {
        return Err(StoreError::SchedulerFenced(lease.lease_name.clone()));
    }
    Ok(())
}

fn assert_paper_effect_artifact(
    transaction: &Transaction<'_>,
    effect: &ArtifactRef,
    run_id: &RunId,
) -> StoreResult<()> {
    let artifact = read_artifact(transaction, &effect.artifact_id)?;
    if effect.kind != artifact.kind
        || !matches!(
            artifact.kind,
            ArtifactKind::ExecutionCommitment | ArtifactKind::ExecutionReprice
        )
        || artifact.lifecycle != ArtifactLifecycle::Canonical
        || artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.as_ref())
            != Some(run_id)
    {
        return Err(StoreError::InvalidPaperEffect(effect.artifact_id.clone()));
    }
    Ok(())
}

fn paper_effect_intent_exists(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    effect_id: &ArtifactId,
) -> StoreResult<bool> {
    let found = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM rebuild_events WHERE run_id = ?1 AND event_type = ?2 AND artifact_id = ?3)",
        params![
            run_id.0,
            LifecycleEventType::ExecutionEffectIntent.as_str(),
            effect_id.0.as_str(),
        ],
        |row| row.get::<_, i64>(0),
    )?;
    Ok(found != 0)
}

fn validate_paper_effect_events(
    connection: &Connection,
    run_id: Option<&RunId>,
) -> StoreResult<()> {
    let mut statement = connection.prepare(
        r#"SELECT event_id, run_id, event_type, artifact_id
           FROM rebuild_events
           WHERE (?1 IS NULL OR run_id = ?1)
             AND artifact_id IS NOT NULL
             AND event_type IN (?2, ?3, ?4)
           ORDER BY event_id ASC"#,
    )?;
    let rows =
        statement.query_map(
            params![
                run_id.map(|value| value.0.as_str()),
                LifecycleEventType::ExecutionEffectIntent.as_str(),
                LifecycleEventType::ExecutionEffectSettled.as_str(),
                LifecycleEventType::ExecutionEffectRecovered.as_str(),
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    RunId(row.get::<_, String>(1)?),
                    row.get::<_, String>(2)?,
                    ArtifactId(ContentHash::new(row.get::<_, String>(3)?).map_err(|error| {
                        rusqlite::Error::ToSqlConversionFailure(Box::new(error))
                    })?),
                ))
            },
        )?;
    let mut intents = BTreeMap::<(RunId, ArtifactId), i64>::new();
    let mut terminals = BTreeMap::<(RunId, ArtifactId), i64>::new();
    for row in rows {
        let (cursor, event_run_id, event_type, effect_id) = row?;
        let key = (event_run_id, effect_id.clone());
        match event_type.as_str() {
            value if value == LifecycleEventType::ExecutionEffectIntent.as_str() => {
                if terminals.contains_key(&key) {
                    return Err(StoreError::Integrity(format!(
                        "Paper effect {effect_id} has intent after terminal event at cursor {cursor}"
                    )));
                }
                if intents.insert(key, cursor).is_some() {
                    return Err(StoreError::Integrity(format!(
                        "Paper effect {effect_id} has duplicate intent at cursor {cursor}"
                    )));
                }
            }
            value
                if value == LifecycleEventType::ExecutionEffectSettled.as_str()
                    || value == LifecycleEventType::ExecutionEffectRecovered.as_str() =>
            {
                let Some(intent_cursor) = intents.get(&key).copied() else {
                    return Err(StoreError::Integrity(format!(
                        "Paper effect {effect_id} terminal event at cursor {cursor} has no prior intent"
                    )));
                };
                if cursor <= intent_cursor {
                    return Err(StoreError::Integrity(format!(
                        "Paper effect {effect_id} terminal cursor {cursor} is not after intent cursor {intent_cursor}"
                    )));
                }
                if terminals.insert(key, cursor).is_some() {
                    return Err(StoreError::Integrity(format!(
                        "Paper effect {effect_id} has duplicate terminal event at cursor {cursor}"
                    )));
                }
            }
            _ => unreachable!("effect query emits fixed lifecycle types"),
        }
    }
    Ok(())
}
