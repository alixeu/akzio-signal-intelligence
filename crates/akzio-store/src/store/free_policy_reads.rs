// 文件导读：Policy/Shadow 的读 helper 恢复消费 head、transition、pair 和 Run purpose，
// 并核对 cursor 边界；它们只读取 immutable history，不把候选或孤立 Artifact 变成 influence。
fn read_policy_consumption_head(
    connection: &Connection,
    expected_subject: &PolicySubject,
) -> StoreResult<Option<PolicyConsumptionHead>> {
    let subject_id = expected_subject.subject_id();
    let row = connection
        .query_row(
            r#"SELECT consumed_pair_cursor, evaluation_artifact_id,
                       evaluation_event_cursor, updated_at
                FROM rebuild_policy_consumption_heads WHERE subject_id = ?1"#,
            params![subject_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()?;
    let Some((
        consumed_pair_cursor,
        evaluation_artifact_id,
        evaluation_cursor,
        updated_at,
    )) = row
    else {
        return Ok(None);
    };
    let subject = parse_persisted_subject(&subject_id)?;
    if &subject != expected_subject {
        return Err(StoreError::Integrity(format!(
            "policy consumption head {subject_id} subject identity disagrees with lookup"
        )));
    }
    Ok(Some(PolicyConsumptionHead {
        subject,
        consumed_pair_cursor,
        evaluation_artifact_id: ArtifactId(ContentHash::new(evaluation_artifact_id)?),
        evaluation_cursor,
        updated_at: parse_time(&updated_at)?,
    }))
}

// 读取 subject 最新 pair event cursor，作为下一次 evaluation snapshot 的上界候选。
fn max_shadow_pair_cursor(connection: &Connection, subject: &PolicySubject) -> StoreResult<i64> {
    connection
        .query_row(
            "SELECT COALESCE(MAX(pair_event_cursor), 0) FROM rebuild_shadow_pairs WHERE subject_id = ?1",
            params![subject.subject_id()],
            |row| row.get::<_, i64>(0),
        )
        .map_err(Into::into)
}

// 按 frozen cursor 半开区间统计三个 horizon，避免 evaluation 漏算或重复消费 pair。
fn shadow_pair_counts_between(
    connection: &Connection,
    subject: &PolicySubject,
    after_cursor: i64,
    through_cursor: i64,
) -> StoreResult<[u64; 3]> {
    if after_cursor < 0 || through_cursor < after_cursor {
        return Err(StoreError::InvalidLearningCommit(
            "shadow_pair.snapshot_cursor",
        ));
    }
    let mut counts = [0; 3];
    for (index, horizon) in OutcomeHorizon::ALL.into_iter().enumerate() {
        counts[index] = connection.query_row(
            "SELECT COUNT(*) FROM rebuild_shadow_pairs \
             WHERE subject_id = ?1 AND horizon = ?2 \
               AND pair_event_cursor > ?3 AND pair_event_cursor <= ?4",
            params![
                subject.subject_id(),
                enum_name(horizon),
                after_cursor,
                through_cursor
            ],
            |row| row.get(0),
        )?;
    }
    Ok(counts)
}

// 提交 evaluation 前重新比较 after/through/counts，阻止 stale snapshot 越过新 pair。
fn validate_policy_shadow_pair_snapshot(
    connection: &Connection,
    subject: &PolicySubject,
    snapshot: PolicyShadowPairSnapshot,
) -> StoreResult<()> {
    let current_after = read_policy_consumption_head(connection, subject)?
        .map_or(0, |head| head.consumed_pair_cursor);
    if snapshot.after_cursor != current_after {
        return Err(StoreError::InvalidLearningCommit(
            "policy_evaluation.pair_snapshot_stale",
        ));
    }
    let current_max = max_shadow_pair_cursor(connection, subject)?;
    if snapshot.through_cursor < snapshot.after_cursor || snapshot.through_cursor > current_max {
        return Err(StoreError::InvalidLearningCommit(
            "policy_evaluation.pair_snapshot_boundary",
        ));
    }
    if snapshot.through_cursor > snapshot.after_cursor {
        let boundary_exists = connection
            .query_row(
                "SELECT 1 FROM rebuild_shadow_pairs \
                 WHERE subject_id = ?1 AND pair_event_cursor = ?2",
                params![subject.subject_id(), snapshot.through_cursor],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !boundary_exists {
            return Err(StoreError::InvalidLearningCommit(
                "policy_evaluation.pair_snapshot_boundary",
            ));
        }
    }
    if shadow_pair_counts_between(
        connection,
        subject,
        snapshot.after_cursor,
        snapshot.through_cursor,
    )? != snapshot.counts_by_horizon
    {
        return Err(StoreError::InvalidLearningCommit(
            "policy_evaluation.pair_snapshot_counts",
        ));
    }
    Ok(())
}

// Outcome/Experience/Evaluation/CandidatePolicy 必须走 atomic learning commit，不能走通用 Task Artifact API。
fn reject_generic_learning_artifact(artifact: &Artifact) -> StoreResult<()> {
    if matches!(
        artifact.kind,
        ArtifactKind::Outcome
            | ArtifactKind::Experience
            | ArtifactKind::Evaluation
            | ArtifactKind::CandidatePolicy
    ) {
        return Err(StoreError::InvalidLearningCommit(
            "learning_artifact.atomic_commit_required",
        ));
    }
    Ok(())
}

// 幂等重放比较完整 typed identity、Run、transition、cursor 和完成时间。
fn same_policy_evaluation(
    existing: &StoredPolicyEvaluation,
    commit: &PolicyEvaluationCommit,
) -> bool {
    existing.subject == commit.subject
        && existing.outcome_artifact_id == commit.outcome.artifact_id
        && existing.experience_artifact_id == commit.experience.artifact_id
        && existing.evaluation_artifact_id == commit.evaluation.artifact_id
        && existing.candidate_policy_artifact_id
            == commit
                .candidate_policy
                .as_ref()
                .map(|artifact| artifact.artifact_id.clone())
        && existing.from == commit.from
        && existing.to == commit.to
        && existing.transition_id
            == commit
                .transition
                .as_ref()
                .map(|transition| transition.transition_id.clone())
        && existing.run_id == commit.permit.run_id
        && existing.consumed_pair_cursor == commit.pair_snapshot.through_cursor
        && existing.completed_at == commit.completed_at
}

// 从 subject_id 恢复当前 head，并确认 SQL key 解出的 subject 与调用方一致。
fn read_policy_head(
    connection: &Connection,
    expected_subject: &PolicySubject,
) -> StoreResult<Option<PolicyHead>> {
    let subject_id = expected_subject.subject_id();
    let row = connection
        .query_row(
            "SELECT state_json, revision, transition_id, transition_event_cursor, updated_at FROM rebuild_policy_heads WHERE subject_id = ?1",
            params![subject_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )
        .optional()?;
    let Some((state, revision, transition_id, transition_cursor, updated_at)) = row
    else {
        return Ok(None);
    };
    let subject = parse_persisted_subject(&subject_id)?;
    if &subject != expected_subject {
        return Err(StoreError::Integrity(format!(
            "policy head {subject_id} subject identity disagrees with lookup"
        )));
    }
    Ok(Some(PolicyHead {
        subject,
        state: serde_json::from_str(&state)?,
        revision,
        transition_id: PolicyTransitionId(transition_id),
        transition_cursor,
        updated_at: parse_time(&updated_at)?,
    }))
}

// 读取单条 immutable transition 及其 evaluation event cursor。
fn read_policy_transition(
    connection: &Connection,
    transition_id: &PolicyTransitionId,
) -> StoreResult<Option<PolicyTransitionRecord>> {
    let row = connection
        .query_row(
            r#"SELECT subject_id, from_state_json, to_state_json,
                      evaluation_artifact_id, run_id, revision, created_at, event_cursor
               FROM rebuild_policy_transitions WHERE transition_id = ?1"#,
            params![transition_id.0],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, u64>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, i64>(7)?,
                ))
            },
        )
        .optional()?;
    let Some((
        subject_id,
        from,
        to,
        evaluation_id,
        run_id,
        revision,
        created_at,
        transition_cursor,
    )) = row
    else {
        return Ok(None);
    };
        let subject = parse_persisted_subject(&subject_id)?;
    Ok(Some(PolicyTransitionRecord {
        transition: PolicyTransition {
            schema_version: DOMAIN_SCHEMA_VERSION,
            transition_id: transition_id.clone(),
            subject,
            from: serde_json::from_str(&from)?,
            to: serde_json::from_str(&to)?,
            evaluation: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::new(evaluation_id)?),
                kind: ArtifactKind::Evaluation,
            },
            created_at: parse_time(&created_at)?,
        },
        run_id: RunId(run_id),
        revision,
        transition_cursor,
    }))
}

// 按 revision 升序恢复一个 subject 的完整 transition history。
fn read_policy_transitions(
    connection: &Connection,
    expected_subject: &PolicySubject,
) -> StoreResult<Vec<PolicyTransitionRecord>> {
    let subject_id = expected_subject.subject_id();
    let mut statement = connection.prepare(
        r#"SELECT transition_id, from_state_json, to_state_json,
                  evaluation_artifact_id, run_id, revision, created_at, event_cursor
           FROM rebuild_policy_transitions WHERE subject_id = ?1 ORDER BY revision ASC"#,
    )?;
    let rows = statement
        .query_map(params![subject_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, u64>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, i64>(7)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    rows.into_iter()
        .map(|(transition_id, from, to, evaluation_id, run_id, revision, created_at, transition_cursor)| {
            let subject = parse_persisted_subject(&subject_id)?;
            if &subject != expected_subject {
                return Err(StoreError::Integrity(format!(
                    "policy transition {transition_id} subject identity disagrees with key {subject_id}"
                )));
            }
            Ok(PolicyTransitionRecord {
                transition: PolicyTransition {
                    schema_version: DOMAIN_SCHEMA_VERSION,
                    transition_id: PolicyTransitionId(transition_id),
                    subject,
                    from: serde_json::from_str(&from)?,
                    to: serde_json::from_str(&to)?,
                    evaluation: ArtifactRef {
                        artifact_id: ArtifactId(ContentHash::new(evaluation_id)?),
                        kind: ArtifactKind::Evaluation,
                    },
                    created_at: parse_time(&created_at)?,
                },
                run_id: RunId(run_id),
                revision,
                transition_cursor,
            })
        })
        .collect()
}

// 从 pair_key 恢复候选/父决策、Outcome 和 horizon；payload closure 由上层继续验证。
fn read_shadow_pair(
    connection: &Connection,
    pair_key: &ContentHash,
) -> StoreResult<Option<StoredShadowPair>> {
    let row = connection
        .query_row(
            r#"SELECT subject_id, parent_decision_artifact_id, execution_context_artifact_id,
                      candidate_decision_artifact_id, candidate_contract_hash, candidate_topology_id,
                      horizon, parent_outcome_artifact_id, candidate_outcome_artifact_id, completed_at,
                      pair_event_cursor
               FROM rebuild_shadow_pairs WHERE pair_key = ?1"#,
            params![pair_key.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, i64>(10)?,
                ))
            },
        )
        .optional()?;
    let Some((
        subject_id,
        parent_decision,
        execution_context,
        candidate_decision,
        candidate_contract_hash,
        candidate_topology_id,
        horizon,
        parent_outcome,
        candidate_outcome,
        completed_at,
        completion_cursor,
    )) = row
    else {
        return Ok(None);
    };
    Ok(Some(StoredShadowPair {
        pair_key: pair_key.clone(),
        completion: ShadowPairCompletion {
        subject: parse_persisted_subject(&subject_id)?,
            parent_decision: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::new(parent_decision)?),
                kind: ArtifactKind::Decision,
            },
            execution_context: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::new(execution_context)?),
                kind: ArtifactKind::ExecutionContext,
            },
            candidate_decision: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::new(candidate_decision)?),
                kind: ArtifactKind::Decision,
            },
            candidate_contract_hash: ContentHash::new(candidate_contract_hash)?,
            candidate_topology_id,
            horizon: parse_enum(&horizon)?,
            parent_outcome: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::new(parent_outcome)?),
                kind: ArtifactKind::Outcome,
            },
            candidate_outcome: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::new(candidate_outcome)?),
                kind: ArtifactKind::Outcome,
            },
            completed_at: parse_time(&completed_at)?,
        },
        completion_cursor,
    }))
}

// pair 幂等比较故意忽略 completed_at，只比较被比较的决策/context/candidate/horizon identity。
fn same_shadow_pair(left: &ShadowPairCompletion, right: &ShadowPairCompletion) -> bool {
    left.subject == right.subject
        && left.parent_decision == right.parent_decision
        && left.execution_context == right.execution_context
        && left.candidate_decision == right.candidate_decision
        && left.candidate_contract_hash == right.candidate_contract_hash
        && left.candidate_topology_id == right.candidate_topology_id
        && left.horizon == right.horizon
        && left.parent_outcome == right.parent_outcome
        && left.candidate_outcome == right.candidate_outcome
}

// purpose 从 rebuild_runs 读取，不接受调用方传入的替代标签。
fn run_purpose_from_connection(connection: &Connection, run_id: &RunId) -> StoreResult<RunPurpose> {
    let purpose = connection
        .query_row(
            "SELECT purpose FROM rebuild_runs WHERE run_id = ?1",
            params![run_id.0],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or_else(|| StoreError::MissingRun(run_id.clone()))?;
    parse_enum(&purpose)
}

// Run purpose 决定 Task Artifact 可用 lifecycle：Ephemeral 禁止，Canonical 仅 Paper。
fn assert_task_artifact_lifecycle(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    artifact: &Artifact,
) -> StoreResult<()> {
    let purpose = run_purpose_from_connection(transaction, run_id)?;
    let allowed = match artifact.lifecycle {
        ArtifactLifecycle::Ephemeral => false,
        ArtifactLifecycle::RunScoped => true,
        ArtifactLifecycle::Canonical => purpose == RunPurpose::Paper,
    };
    if allowed {
        return Ok(());
    }
    Err(StoreError::InvalidTaskArtifactLifecycle {
        purpose,
        lifecycle: artifact.lifecycle,
    })
}
