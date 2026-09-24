// 文件导读：这些窄校验把 Artifact kind/lifecycle/origin 与 Paper/Shadow purpose 绑定，
// 并提供 source-ref 集合、Policy transition 和序列化 enum 的确定性辅助，不计算交易结果。
fn workflow_graph_run_purpose(
    connection: &Connection,
    artifact_id: &ArtifactId,
) -> StoreResult<RunPurpose> {
    // 通过 graph_artifact_id 反查唯一 Run purpose，避免调用方自行声明候选图身份。
    let purpose = connection
        .query_row(
            "SELECT purpose FROM rebuild_runs WHERE graph_artifact_id = ?1",
            params![artifact_id.0.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or_else(|| StoreError::MissingArtifact(artifact_id.clone()))?;
    parse_enum(&purpose)
}

fn artifact_run_purpose(connection: &Connection, artifact: &Artifact) -> StoreResult<RunPurpose> {
    // RunScoped Artifact 必须携带 origin.run_id，再由 SQL Run 行决定 purpose。
    let run_id = artifact
        .origin
        .as_ref()
        .and_then(|origin| origin.run_id.as_ref())
        .ok_or(StoreError::InvalidLearningCommit(
            "learning_artifact.origin",
        ))?;
    run_purpose_from_connection(connection, run_id)
}

fn assert_artifact_from_allowed_purposes(
    connection: &Connection,
    artifact: &Artifact,
    allowed_purposes: &[RunPurpose],
) -> StoreResult<()> {
    // 对 Paper-only 与 Paper/Shadow 混合闭包分别返回不同错误，保留 canonical learning 边界。
    let purpose = artifact_run_purpose(connection, artifact)?;
    if allowed_purposes.contains(&purpose) {
        return Ok(());
    }
    if allowed_purposes == [RunPurpose::Paper] {
        return Err(StoreError::NonCanonicalLearningPurpose(purpose));
    }
    Err(StoreError::InvalidLearningCommit(
        "learning_artifact.run_purpose",
    ))
}

fn assert_artifact_from_paper_with_connection(
    connection: &Connection,
    artifact: &Artifact,
) -> StoreResult<()> {
    // canonical Policy/Outcome 只接受 Paper Run 来源。
    assert_artifact_from_allowed_purposes(connection, artifact, &[RunPurpose::Paper])
}

fn assert_paper_run(transaction: &Transaction<'_>, run_id: &RunId) -> StoreResult<()> {
    // learning/commitment 写事务内再次读取 Run purpose，拒绝 PositionPlan/Debug 冒充 Paper。
    let purpose = run_purpose_from_connection(transaction, run_id)?;
    if purpose != RunPurpose::Paper {
        return Err(StoreError::NonCanonicalLearningPurpose(purpose));
    }
    Ok(())
}

fn read_required_artifact(
    connection: &Connection,
    reference: &ArtifactRef,
    error: &'static str,
) -> StoreResult<Artifact> {
    // 引用 kind 与 SQL Artifact kind 必须同时匹配。
    let artifact = read_artifact(connection, &reference.artifact_id)?;
    if artifact.kind != reference.kind {
        return Err(StoreError::InvalidLearningCommit(error));
    }
    Ok(artifact)
}

fn assert_canonical_paper_artifact(
    connection: &Connection,
    artifact: &Artifact,
) -> StoreResult<()> {
    // parent decision/outcome 必须 canonical 且来自 Paper。
    if artifact.lifecycle != ArtifactLifecycle::Canonical {
        return Err(StoreError::InvalidLearningCommit(
            "shadow_pair.parent_lifecycle",
        ));
    }
    assert_artifact_from_paper_with_connection(connection, artifact)
}

fn assert_shadow_candidate_artifact(
    connection: &Connection,
    artifact: &Artifact,
) -> StoreResult<()> {
    // candidate 可来自 Paper 或 RunScoped Shadow，但 Shadow candidate 不能伪装 canonical。
    match artifact_run_purpose(connection, artifact)? {
        RunPurpose::Paper => Ok(()),
        RunPurpose::Shadow if artifact.lifecycle != ArtifactLifecycle::Canonical => Ok(()),
        RunPurpose::Shadow => Err(StoreError::InvalidLearningCommit(
            "shadow_pair.candidate_shadow_canonical",
        )),
        _ => Err(StoreError::InvalidLearningCommit(
            "shadow_pair.candidate_purpose",
        )),
    }
}

fn assert_candidate_decision_binding(
    connection: &Connection,
    candidate_decision: &Artifact,
    completion: &ShadowPairCompletion,
) -> StoreResult<()> {
    // candidate decision 的 contract/topology 从 origin 或成功 Analyst task lineage 复核。
    let origin = candidate_decision
        .origin
        .as_ref()
        .ok_or(StoreError::InvalidLearningCommit(
            "shadow_pair.candidate_origin",
        ))?;
    let run_id = origin
        .run_id
        .as_ref()
        .ok_or(StoreError::InvalidLearningCommit(
            "shadow_pair.candidate_run",
        ))?;
    let contract_matches = if let Some(hash) = &origin.contract_hash {
        hash == &completion.candidate_contract_hash
    } else if candidate_decision.producer == "decision.bound" {
        // Rust DecisionGate has no model Contract. The tested analyst Contract
        // is bound by the producing Run's successful research tasks.
        let (total, matching): (u64,u64) = connection.query_row(
            "SELECT COUNT(*), COALESCE(SUM(CASE WHEN contract_hash=?2 AND status='succeeded' THEN 1 ELSE 0 END),0) FROM rebuild_tasks WHERE run_id=?1 AND recipe_id='research.analyst'",
            params![run_id.0,completion.candidate_contract_hash.as_str()],|row|Ok((row.get(0)?,row.get(1)?)))?;
        total > 0 && total == matching
    } else { false };
    if !contract_matches {
        return Err(StoreError::InvalidLearningCommit("shadow_pair.candidate_contract"));
    }
    let topology_id = connection
        .query_row(
            "SELECT topology_id FROM rebuild_runs WHERE run_id = ?1",
            params![run_id.0],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or_else(|| StoreError::MissingRun(run_id.clone()))?;
    if topology_id != completion.candidate_topology_id {
        return Err(StoreError::InvalidLearningCommit(
            "shadow_pair.candidate_topology",
        ));
    }
    Ok(())
}

fn outcome_schedule_source_refs(schedule: &OutcomeSchedule) -> Vec<ArtifactRef> {
    // 按 NoOrder/ReconciledPaper 变体展开 schedule 的完整 source closure。
    let mut references = vec![
        schedule.decision.clone(),
        schedule.decision_context.clone(),
        schedule.execution_context.clone(),
    ];
    match &schedule.execution {
        OutcomeExecutionLineage::NoOrder { execution_verdict } => {
            references.push(execution_verdict.clone());
        }
        OutcomeExecutionLineage::ReconciledPaper {
            execution_verdict,
            commitment,
            reconciliation,
        } => {
            references.push(execution_verdict.clone());
            references.push(commitment.clone());
            references.push(reconciliation.clone());
        }
    }
    references
}

#[allow(clippy::match_like_matches_macro)]
fn is_allowed_policy_transition(from: PolicyState, to: PolicyState) -> bool {
    // 只列出领域允许的 memory/contract/topology 状态边，不接受任意 from/to 组合。
    use akzio_domain::{CandidatePolicyState as Candidate, MemoryLifecycle as Memory};

    match (from, to) {
        (
            PolicyState::Memory(Memory::Candidate),
            PolicyState::Memory(Memory::Active | Memory::Contested | Memory::Retired),
        )
        | (
            PolicyState::Memory(Memory::Active),
            PolicyState::Memory(Memory::Proven | Memory::Contested | Memory::Retired),
        )
        | (
            PolicyState::Memory(Memory::Proven),
            PolicyState::Memory(Memory::Contested | Memory::Retired),
        )
        | (
            PolicyState::Memory(Memory::Contested),
            PolicyState::Memory(Memory::Active | Memory::Retired),
        )
        | (
            PolicyState::Contract(Candidate::Candidate),
            PolicyState::Contract(Candidate::Canary10),
        )
        | (
            PolicyState::Contract(Candidate::Canary10),
            PolicyState::Contract(Candidate::Canary25 | Candidate::Candidate),
        )
        | (
            PolicyState::Contract(Candidate::Canary25),
            PolicyState::Contract(Candidate::Canary50 | Candidate::Candidate),
        )
        | (
            PolicyState::Contract(Candidate::Canary50),
            PolicyState::Contract(Candidate::Active | Candidate::Candidate),
        )
        | (PolicyState::Contract(Candidate::Active), PolicyState::Contract(Candidate::Candidate))
        | (
            PolicyState::Topology(Candidate::Candidate),
            PolicyState::Topology(Candidate::Canary10),
        )
        | (
            PolicyState::Topology(Candidate::Canary10),
            PolicyState::Topology(Candidate::Canary25 | Candidate::Candidate),
        )
        | (
            PolicyState::Topology(Candidate::Canary25),
            PolicyState::Topology(Candidate::Canary50 | Candidate::Candidate),
        )
        | (
            PolicyState::Topology(Candidate::Canary50),
            PolicyState::Topology(Candidate::Active | Candidate::Candidate),
        )
        | (PolicyState::Topology(Candidate::Active), PolicyState::Topology(Candidate::Candidate)) => {
            true
        }
        _ => false,
    }
}

fn has_exact_source_refs(artifact: &Artifact, expected: &[ArtifactRef]) -> bool {
    // 用去重后的 (id,kind) 指纹比较，同时拒绝 artifact 自身 source_refs 内的重复项。
    let actual = artifact
        .source_refs
        .iter()
        .map(source_ref_fingerprint)
        .collect::<BTreeSet<_>>();
    let expected_len = expected.len();
    let expected = expected
        .iter()
        .map(source_ref_fingerprint)
        .collect::<BTreeSet<_>>();
    actual.len() == artifact.source_refs.len()
        && expected.len() == expected_len
        && actual == expected
}

fn source_ref_fingerprint(reference: &ArtifactRef) -> (String, String) {
    (
        reference.artifact_id.0.as_str().to_owned(),
        enum_name(reference.kind),
    )
}

fn same_paper_commitment(left: &PaperCommitment, right: &PaperCommitment) -> bool {
    // 幂等恢复只比较计划、context、session 和 client_order_ids，不比较创建时间。
    left.plan_hash == right.plan_hash
        && left.execution_context == right.execution_context
        && left.broker_session == right.broker_session
        && left.client_order_ids == right.client_order_ids
}

fn enum_name<T: Serialize>(value: T) -> String {
    // Store SQL 使用 serde 的 snake_case 字符串作为 enum 列值。
    serde_json::to_value(value)
        .expect("enum serializes")
        .as_str()
        .expect("enum serializes as string")
        .to_owned()
}

fn status_counts(connection: &Connection, table: &str) -> StoreResult<BTreeMap<String, u64>> {
    // 仅供受控的固定表名调用方读取状态聚合；表名不是外部输入入口。
    let sql = format!("SELECT status, COUNT(*) FROM {table} GROUP BY status ORDER BY status");
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
    })?;
    rows.collect::<Result<BTreeMap<_, _>, _>>()
        .map_err(Into::into)
}

fn parse_enum<T: for<'de> serde::Deserialize<'de>>(value: &str) -> StoreResult<T> {
    // 从 SQL 字符串恢复 serde enum，未知值按 Json/Integrity 边界返回。
    serde_json::from_value(serde_json::Value::String(value.to_owned())).map_err(StoreError::Json)
}

fn parse_task_status(value: &str) -> StoreResult<TaskStatus> {
    // Task SQL 的 queued 名称对应领域 Pending，其余状态保持显式映射。
    match value {
        "queued" => Ok(TaskStatus::Pending),
        "running" => Ok(TaskStatus::Running),
        "succeeded" => Ok(TaskStatus::Succeeded),
        "failed" => Ok(TaskStatus::Failed),
        "cancelled" => Ok(TaskStatus::Cancelled),
        "skipped" => Ok(TaskStatus::Skipped),
        other => Err(StoreError::Integrity(format!(
            "invalid task status {other}"
        ))),
    }
}

fn is_trajectory_redacted_kind(kind: ArtifactKind) -> bool {
    matches!(
        kind,
        ArtifactKind::AgentTurn | ArtifactKind::ToolCall | ArtifactKind::ToolResult
    )
}

fn trajectory_output_refs(artifact: &Artifact) -> Vec<ArtifactRef> {
    // trajectory 输出保留自身和非 Raw/非模型细节 source refs，并稳定排序去重。
    let mut refs = vec![ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: artifact.kind,
    }];
    refs.extend(
        artifact
            .source_refs
            .iter()
            .filter(|reference| {
                reference.kind != ArtifactKind::RawEvidence
                    && !is_trajectory_redacted_kind(reference.kind)
            })
            .cloned(),
    );
    refs.sort();
    refs.dedup();
    refs
}

#[cfg(test)]
mod connection_guard_tests {
    use super::*;

    fn test_store(label: &str) -> Store {
        Store::open(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/connection-guard-tests")
                .join(format!("{label}-{}", RunId::new().0)),
        )
        .unwrap()
    }

    /// Acquiring the connection twice on one thread is reported rather than
    /// hanging. Before this, the nested acquisition blocked forever while still
    /// holding the lock, taking the whole process down with no diagnostic.
    #[test]
    fn nested_acquisition_on_one_thread_is_reported() {
        let store = test_store("nested-acquire");
        let guard = store.connection().unwrap();
        let error = match store.connection() {
            Ok(_) => panic!("nested acquire must fail instead of deadlocking"),
            Err(error) => error,
        };
        assert!(
            matches!(&error, StoreError::Integrity(message) if message.contains("twice on one thread")),
            "expected a nested-acquisition diagnostic, got {error:?}"
        );
        drop(guard);

        // The flag is cleared on drop, so the next acquisition succeeds.
        store.connection().unwrap();
    }

    /// The ownership flag is per thread: another thread still blocks and then
    /// proceeds, because cross-thread contention is ordinary serialization.
    #[test]
    fn a_second_thread_still_serializes() {
        let store = test_store("cross-thread-serialize");
        let guard = store.connection().unwrap();
        let other = {
            let store = store.clone();
            std::thread::spawn(move || store.connection().map(|_| ()).map_err(|e| e.to_string()))
        };
        // Release the guard so the waiting thread can make progress.
        drop(guard);
        other.join().unwrap().expect("other thread must acquire");
    }
}
