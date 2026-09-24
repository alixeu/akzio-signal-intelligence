// 文件导读：生命周期辅助函数把 Artifact source closure、embedded BLOB 索引、Task node、
// permit/daemon lease 和 Paper effect 约束放进调用方事务，保证读取与提交使用同一连接视图。
// 写入从 insert_artifact_batch/insert_task_node 接到调用方事务；读取与 lease 校验函数只借用现有
// Connection/Transaction，不获取 Store Mutex，也不会自行 commit。
// 输入已构造 Artifact 与调用方连接；提升嵌入 BLOB 并写索引行，不插 Artifact 主行。
fn index_embedded_blob_refs(connection: &Connection, artifact: &Artifact) -> StoreResult<()> {
    // 每个嵌入 BLOB 先 promotion 到 durable CAS，再写 artifact+role+ordinal 索引；
    // 调用方传入 Transaction 时，两者与 Artifact 一起提交或回滚。
    for (role, ordinal, blob) in embedded_blob_refs(connection, artifact)? {
        blob::promote_staged_blob(connection, &blob)?;
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

// 为历史 NormalizedEvidence 补齐 embedded blob index，写入只在自身 Immediate 事务内发生。
fn backfill_embedded_blob_refs(connection: &mut Connection) -> StoreResult<()> {
    // 先在事务外取缺索引 Artifact ID；空集不创建写事务。非空时一个 Immediate 事务内逐个重读并 INSERT OR IGNORE。
    // 初始 ID 列表与后续写事务不是同一读取快照；已存在索引的行不覆盖。
    let artifact_ids = connection
        .prepare(
            r#"SELECT artifact_id
               FROM rebuild_artifacts AS artifact
               WHERE artifact.kind = ?1
                 AND NOT EXISTS (
                     SELECT 1
                     FROM rebuild_embedded_blob_refs AS embedded
                     WHERE embedded.artifact_id = artifact.artifact_id
                 )
               ORDER BY artifact_id"#,
        )?
        .query_map(
            params![enum_name(ArtifactKind::NormalizedEvidence)],
            |row| row.get::<_, String>(0),
        )?
        .collect::<Result<Vec<_>, _>>()?;
    if artifact_ids.is_empty() {
        return Ok(());
    }
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    for artifact_id in artifact_ids {
        let artifact = read_artifact(&transaction, &ArtifactId(ContentHash::new(artifact_id)?))?;
        for (role, ordinal, blob) in embedded_blob_refs(&transaction, &artifact)? {
            transaction.execute(
                r#"INSERT OR IGNORE INTO rebuild_embedded_blob_refs
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
    }
    transaction.commit()?;
    Ok(())
}

// 按 Artifact kind 从 Contract/NormalizedEvidence payload 提取嵌入 BLOB，并逐个验证可读。
fn embedded_blob_refs(
    connection: &Connection,
    artifact: &Artifact,
) -> StoreResult<Vec<(String, u64, BlobRef)>> {
    // 先读取 Artifact 自身 CAS；Contract 走强类型校验并移动 tool_specs 收集 schema，
    // NormalizedEvidence 尝试 JSON path 提取 source_document.sources[].blob。
    let payload = blob::read_blob_bytes(connection, &artifact.blob.hash, artifact.blob.bytes)?;
    let refs = match artifact.kind {
        ArtifactKind::Contract => {
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
            refs
        }
        ArtifactKind::NormalizedEvidence => {
            // 该 kind 的 payload 若不是 JSON，当前 helper 明确返回空嵌入引用；后续完整性检查仍会校验 Artifact 主 BLOB。
            let Ok(value) = serde_json::from_slice::<serde_json::Value>(&payload) else {
                return Ok(Vec::new());
            };
            value
                .pointer("/value/source_document/sources")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .enumerate()
                .filter_map(|(ordinal, source)| {
                    source.get("blob").cloned().map(|blob| (ordinal, blob))
                })
                .map(|(ordinal, value)| {
                    Ok((
                        "value.source_document.sources.blob".to_owned(),
                        ordinal as u64,
                        serde_json::from_value(value)?,
                    ))
                })
                .collect::<StoreResult<Vec<_>>>()?
        }
        _ => Vec::new(),
    };
    for (_, _, blob) in &refs {
        // 发现的嵌入引用必须在 durable 或同连接 TEMP staging 中可还原，不能只索引未存在的 hash。
        blob::read_blob_bytes_including_staged(connection, &blob.hash, blob.bytes)?;
    }
    Ok(refs)
}

/// Inserts a completion batch in source-closure order. A task may create a
/// RawEvidence artifact and its NormalizedEvidence dependent in the same
/// atomic attempt; callers need not rely on input ordering for correctness.
// 对同一 completion batch 做 source-closure 拓扑排序后插入，允许输入顺序与依赖顺序不同。
// 输入切片被借用；pending 用 ArtifactId 做去重/排序，每次只移除 source refs 已不在 pending 的节点。
// 若存在闭环或外部引用顺序无法满足，本 helper 返回 InvalidArtifactClosure；不负责外层事务提交。
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
        // BTreeMap 迭代按 ID 稳定选第一个可写 Artifact；值是借用的 `&Artifact`，
        // `remove` 只移出该引用，底层 Artifact 仍归输入切片所有。
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

// Workflow node 的所有 input Artifact 必须在创建 Run 前可沿 source_refs 递归解析。
fn assert_workflow_input_artifacts(
    transaction: &Transaction<'_>,
    nodes: &[WorkflowNode],
) -> StoreResult<()> {
    // 所有节点的输入 refs 共用 visited 集，重复父 Artifact 只检查一次；调用方事务内完成整个校验。
    let mut visited = BTreeSet::new();
    for reference in nodes.iter().flat_map(|node| &node.input_artifacts) {
        assert_artifact_reference_closure(transaction, reference, &mut visited)?;
    }
    Ok(())
}

// 深度优先检查一个 ArtifactRef 的 kind 和完整 source closure，visited 防止重复扫描。
fn assert_artifact_reference_closure(
    transaction: &Transaction<'_>,
    reference: &ArtifactRef,
    visited: &mut BTreeSet<ArtifactId>,
) -> StoreResult<()> {
    // 每层都先检查期望 kind，再以 artifact_id 去重并递归其 source_refs；缺 Artifact/坏 kind 立即 Err。
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

// 把领域 WorkflowNode 序列化进 Task 行，初始状态固定为 queued。
fn insert_task_node(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    node: &WorkflowNode,
    created_at: DateTime<Utc>,
) -> StoreResult<()> {
    // 领域预算/retry/spec 编码到 SQL JSON 列；Task ID 主键冲突或未插入恰一行时返回 DuplicateTask。
    let inserted = transaction.execute(
        r#"INSERT INTO rebuild_tasks
 (task_id, run_id, recipe_id, objective, contract_hash, priority, budget_json, retry_json, on_failure,
 parent_task_id, input_artifacts_json, status, ready_at, node_spec_json)
 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 'queued', ?12, ?13)"#,
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
            node.spec.as_ref().map(serde_json::to_string).transpose()?,
        ],
    )?;
    if inserted != 1 {
        return Err(StoreError::DuplicateTask(node.task_id.clone()));
    }
    Ok(())
}

// 依赖表只保存 Task ID 边，节点本体仍以 input_artifacts/budget 等列和 JSON 为准。
fn insert_node_dependencies(transaction: &Transaction<'_>, node: &WorkflowNode) -> StoreResult<()> {
    // 对每条 dependency 插入 child task_id→depends_on_task_id；外键/唯一键错误由事务传播，不在这里排序或提交。
    for dependency in &node.dependencies {
        transaction.execute(
            "INSERT INTO rebuild_task_dependencies (task_id, depends_on_task_id) VALUES (?1, ?2)",
            params![node.task_id.0, dependency.0],
        )?;
    }
    Ok(())
}

// 读取依赖边并按 Task ID 稳定排序，供 snapshot/claim 判断依赖满足。
fn task_dependencies(connection: &Connection, task_id: &TaskId) -> StoreResult<Vec<TaskId>> {
    // task_id 是 SQL 精确筛选键；query_map/collect 在任一行解码失败时不返回部分依赖列表。
    let dependencies = connection
        .prepare(
            "SELECT depends_on_task_id FROM rebuild_task_dependencies \
             WHERE task_id = ?1 ORDER BY depends_on_task_id ASC",
        )?
        .query_map(params![task_id.0], |row| Ok(TaskId(row.get(0)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(dependencies)
}

// 只规范化 dependencies 顺序，用于比较 graph 与 SQL 恢复节点，不改变业务字段。
fn canonical_workflow_node(mut node: WorkflowNode) -> WorkflowNode {
    // 按值取得 node 的所有权，只对 dependencies 排序后把同一 node 移出返回；其它字段保持原值。
    node.dependencies.sort();
    node
}

// 在最终写事务核验 run/status/lease/epoch/attempt/contract 和真实 wall-clock expiry。
fn assert_permit(transaction: &Transaction<'_>, permit: &TaskWritePermit) -> StoreResult<()> {
    // permit 是由 claim 发放的能力值；SQL 以 task_id 查一行并逐字段比较 run/lease/epoch/attempt/contract。
    // 最后用实际 Utc::now() 检查数据库 lease_until，不依赖 Artifact 的历史时间或调用者给定时间。
    let current = transaction
        .query_row(
            r#"SELECT run_id, status, lease_id, lease_epoch, active_attempt_id, contract_hash, lease_until
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
                    row.get::<_, Option<String>>(6)?,
                ))
            },
        )
        .optional()?;
    let Some((run_id, status, lease_id, epoch, attempt_id, contract_hash, lease_until)) = current
    else {
        return Err(StoreError::MissingTask(permit.task_id.clone()));
    };
    if run_id != permit.run_id.0
        || status != "running"
        || lease_id.as_deref() != Some(permit.lease_id.0.as_str())
        || epoch != permit.epoch
        || attempt_id.as_deref() != Some(permit.attempt_id.0.as_str())
        || contract_hash.as_deref().map(ContentHash::new).transpose()? != permit.contract_hash
        // Lease authority uses the actual write time, independently of an
        // artifact's historical observation/fixture timestamp. Recovery is not
        // required to revoke an already expired attempt.
        || lease_until.as_deref().map(parse_time).transpose()?.is_none_or(|until| until <= Utc::now())
    {
        return Err(StoreError::StalePermit(permit.task_id.clone()));
    }
    Ok(())
}

// daemon lease 的 owner+epoch+expiry 必须同时匹配，防止旧 scheduler 在接管后继续写入。
fn assert_daemon_lease(
    transaction: &Transaction<'_>,
    lease: &DaemonLease,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    // lease_name 定位 SQL row；owner/epoch 不同或 persisted expiry<=now 都是 SchedulerFenced。
    // 只读核验，不更新 heartbeat/expiry，锁和事务由调用者拥有。
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

/// A broker-session slot is the exclusive, single record of one scheduler-owned
/// Paper run. `rebuild_session_slots.run_id` carries neither a foreign key nor a
/// uniqueness constraint, so both halves are asserted here, inside the reserving
/// transaction, before the row is written:
///
/// * the run must already exist, otherwise the slot names a run no reader can
///   resolve, and `PRAGMA foreign_key_check` cannot see the dangling reference;
/// * the run must not already own a different slot, which is what
///   `Store::session_slot_for_run` assumes when it resolves at most one row.
fn assert_session_slot_run(
    transaction: &Transaction<'_>,
    session_key: &str,
    run_id: &RunId,
) -> StoreResult<()> {
    // 第一条查询确保 Run 存在；第二条检查 run_id 唯一性假设，只允许无 slot 或相同 session_key。
    let invalid = || StoreError::InvalidSessionSlot(session_key.to_owned());
    let run_exists = transaction
        .query_row(
            "SELECT 1 FROM rebuild_runs WHERE run_id = ?1",
            params![run_id.0],
            |_| Ok(()),
        )
        .optional()?;
    if run_exists.is_none() {
        return Err(invalid());
    }
    let existing = transaction
        .query_row(
            "SELECT session_key FROM rebuild_session_slots WHERE run_id = ?1",
            params![run_id.0],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    match existing {
        None => Ok(()),
        // Re-reserving the identical session is handled by the callers' own
        // duplicate-session check; only a second, different slot is a conflict.
        Some(existing) if existing == session_key => Ok(()),
        Some(_) => Err(invalid()),
    }
}

// effect 引用必须指向同一 Paper Run 的 canonical commitment/reprice/cancel Artifact。
fn assert_paper_effect_artifact(
    transaction: &Transaction<'_>,
    effect: &ArtifactRef,
    run_id: &RunId,
) -> StoreResult<()> {
    // caller 传入的 Ref.kind 必须匹配读回 Artifact kind，且三种合法 kind 都需 canonical、origin.run_id 相同。
    let artifact = read_artifact(transaction, &effect.artifact_id)?;
    if effect.kind != artifact.kind
        || !matches!(
            artifact.kind,
            ArtifactKind::ExecutionCommitment
                | ArtifactKind::ExecutionCancel
                | ArtifactKind::ExecutionReprice
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

// 查找 effect intent 事件；它只证明 Rust intent 已持久化，不证明外部 broker 已接受。
fn paper_effect_intent_exists(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    effect_id: &ArtifactId,
) -> StoreResult<bool> {
    // EXISTS 精确限制 Run、ExecutionEffectIntent 类型和 effect Artifact ID；true 仅证明 intent event 存在。
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

// 按全局 cursor 检查每个 effect 恰有一个 intent 和至多一个 terminal settlement/recovery。
fn validate_paper_effect_events(
    connection: &Connection,
    run_id: Option<&RunId>,
) -> StoreResult<()> {
    // run_id=None 校验全表，Some(run) 限制到一个 Run；依 cursor 重放 intent/terminal 两张 Map。
    // terminal 必须晚于唯一 intent，重复 intent、先 terminal 或重复 terminal 都中止验证。
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
