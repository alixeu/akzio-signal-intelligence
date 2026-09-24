// 文件导读：canonical PolicyEvaluation 把 sealed Outcome、T5 retrospective、Experience、
// Evaluation、可选 CandidatePolicy 和消费 cursor 放进一个 Immediate 事务；重复 evaluation
// 仍可在同一事务幂等补写 LessonEvidence，但不会重建 policy transition 或消费另一批 pair。
// 此处只持久化 learning crate 已计算并验证的候选结论；Store 负责 CAS、来源闭包、fencing、
// immutable history 和事务提交，不计算投资指标或自动激活 DecisionPolicy。
impl Store {
    /// Commit canonical learning while fencing an optional daemon worker in
    /// the same SQLite transaction as the policy/evaluation writes.
    // 输入 complete_task=false 用于分 subject 的 Canary 阶段性持久进度；true 才写正式 attempt output 并结束 Task。
    // isolated Debug Store 在事务外先拒绝 canonical policy 写入；若传 lease，则事务内再按当前 UTC 校验 scheduler epoch。
    // lesson 表按需创建在学习事务之前，后续验证/SQL 任一失败不会回滚这项 schema 初始化。
    // `Option<&DaemonLease>` 是借用的可选 fence；None 只跳过 daemon fence，不跳过 Paper/permit/lineage 检查。
    pub fn record_policy_evaluation_fenced(
        &self,
        lease: Option<&DaemonLease>,
        commit: &PolicyEvaluationCommit,
    ) -> StoreResult<PolicyEvaluationResult> {
        if self.debug_learning_isolated(&commit.permit.run_id)? {
            return Err(StoreError::DebugControl("isolated_learning_cannot_enter_canonical_policy".into()));
        }
        commit.subject.validate()?;
        if !commit.subject.accepts_state(commit.from) || !commit.subject.accepts_state(commit.to) {
            return Err(StoreError::InvalidLearningCommit(
                "policy_evaluation.subject_state",
            ));
        }
        let subject_id = commit.subject.subject_id();

        if !commit.lesson_evidence.is_empty() {
            self.ensure_lesson_tables()?;
        }

        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(lease) = lease {
            assert_daemon_lease(&transaction, lease, Utc::now())?;
        }
        self.validate_policy_evaluation_commit_with_connection(&transaction, commit)?;

        if let Some(existing) =
            read_policy_evaluation(&transaction, &commit.evaluation.artifact_id)?
        {
            // evaluation Artifact ID 已有记录时必须全字段相同；重复提交只允许重建现有结果，不再插历史/迁移状态。
            if !same_policy_evaluation(&existing, commit) {
                return Err(StoreError::PolicyEvaluationConflict(
                    commit.evaluation.artifact_id.to_string(),
                ));
            }
            if let Some(candidate_policy) = &commit.candidate_policy {
                let stored = read_artifact(&transaction, &candidate_policy.artifact_id)?;
                if stored != *candidate_policy {
                    return Err(StoreError::PolicyEvaluationConflict(
                        commit.evaluation.artifact_id.to_string(),
                    ));
                }
            }
            let consumption = read_policy_consumption_head(&transaction, &commit.subject)?
                .ok_or_else(|| {
                    StoreError::Integrity(format!(
                        "policy evaluation {} has no consumption head",
                        commit.evaluation.artifact_id
                    ))
                })?;
            if consumption.consumed_pair_cursor < existing.consumed_pair_cursor {
                return Err(StoreError::Integrity(format!(
                    "policy evaluation {} consumption cursor regressed",
                    commit.evaluation.artifact_id
                )));
            }
            self.record_lesson_evidence_with_transaction(
                &transaction,
                &commit.lesson_evidence,
                commit.completed_at,
            )?;
            let policy_head = read_policy_head(&transaction, &commit.subject)?;
            // lesson evidence 仍可幂等补记；已有 evaluation 保留原 cursor/head 并报告 newly_recorded=false。
            transaction.commit()?;
            return Ok(PolicyEvaluationResult {
                policy_head,
                consumed_pair_cursor: existing.consumed_pair_cursor,
                evaluation_cursor: existing.event_cursor,
                newly_recorded: false,
            });
        }

        let already_consumed: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM rebuild_policy_evaluations WHERE subject_id=?1 AND outcome_artifact_id=?2)",
            params![subject_id, commit.outcome.artifact_id.0.as_str()], |row| row.get(0))?;
        if already_consumed {
            return Err(StoreError::PolicyEvaluationConflict(
                "outcome already evaluated for subject".to_owned(),
            ));
        }
        // 新 evaluation 必须消费当前有效 permit、Paper Run 和 subject 当前 head；
        // 不能把同一 Outcome 对同一 subject 重复记入新的 evaluation。
        assert_permit(&transaction, &commit.permit)?;
        assert_paper_run(&transaction, &commit.permit.run_id)?;
        let previous = read_policy_head(&transaction, &commit.subject)?;
        match &previous {
            Some(head) if head.state != commit.from => {
                return Err(StoreError::PolicyHeadMismatch(subject_id));
            }
            None if commit.subject.initial_state() != commit.from => {
                return Err(StoreError::PolicyHeadMismatch(subject_id));
            }
            _ => {}
        }
        match &commit.transition {
            Some(transition) => {
                // 状态变化必须满足允许边且 transition ID 尚不存在；None 只允许 from==to 的 no-op evaluation。
                if commit.from == commit.to || !is_allowed_policy_transition(commit.from, commit.to)
                {
                    return Err(StoreError::InvalidLearningCommit("policy_transition.path"));
                }
                if read_policy_transition(&transaction, &transition.transition_id)?.is_some() {
                    return Err(StoreError::PolicyTransitionConflict(
                        transition.transition_id.to_string(),
                    ));
                }
            }
            None if commit.from != commit.to => {
                return Err(StoreError::InvalidLearningCommit(
                    "policy_evaluation.noop_state",
                ));
            }
            None => {}
        }
        // CAS 检查 snapshot.after_cursor、through_cursor 与三个 horizon 计数；
        // 新到达的 pair 不会被悄悄并入这次 evaluation。
        validate_policy_shadow_pair_snapshot(&transaction, &commit.subject, commit.pair_snapshot)?;

        let (_, on_failure) = task_retry_policy(&transaction, &commit.permit.task_id)?;
        for artifact in [
            &commit.outcome,
            &commit.final_retrospective,
            &commit.experience,
            &commit.evaluation,
        ]
        .into_iter()
        .chain(commit.candidate_policy.iter())
        {
            // 同 ID Artifact 若已存在只接受完全相等；缺失时才按当前 permit origin 插入。
            // 每个 payload Artifact 都追加 event；只有 complete_task 时对应 event 进入正式 output index。
            let existing = match read_artifact(&transaction, &artifact.artifact_id) {
                Ok(existing) => Some(existing),
                Err(StoreError::MissingArtifact(_)) => None,
                Err(error) => return Err(error),
            };
            if let Some(existing) = &existing {
                if *existing != *artifact {
                    return Err(StoreError::Integrity(format!(
                        "conflicting learning artifact {}",
                        artifact.artifact_id
                    )));
                }
            } else {
                assert_origin_matches(artifact.origin.as_ref(), &commit.permit)?;
                insert_artifact(&transaction, artifact)?;
            }
            let event_id = append_event(
                &transaction,
                &commit.permit.run_id,
                Some(&commit.permit.task_id),
                Some(&commit.permit.attempt_id),
                LifecycleEventType::ArtifactCommitted,
                Some(&artifact.artifact_id),
                commit.completed_at,
            )?;
            // Multi-subject Canary evaluations are durable, idempotent
            // progress. They can survive an interrupted Attempt and are read
            // through the evaluation ledger, not the succeeded-output index.
            if commit.complete_task {
                record_attempt_output(
                    &transaction,
                    &commit.permit,
                    &artifact.artifact_id,
                    event_id,
                )?;
            }
            if artifact.kind == ArtifactKind::Retrospective {
                append_event(
                    &transaction,
                    &commit.permit.run_id,
                    Some(&commit.permit.task_id),
                    Some(&commit.permit.attempt_id),
                    LifecycleEventType::RetrospectiveCreated,
                    Some(&artifact.artifact_id),
                    commit.completed_at,
                )?;
            }
        }

        self.record_lesson_evidence_with_transaction(
            &transaction,
            &commit.lesson_evidence,
            commit.completed_at,
        )?;

        let consumed_pair_cursor = commit.pair_snapshot.through_cursor;
        let evaluation_cursor = append_event(
            &transaction,
            &commit.permit.run_id,
            Some(&commit.permit.task_id),
            Some(&commit.permit.attempt_id),
            LifecycleEventType::PolicyEvaluated,
            Some(&commit.evaluation.artifact_id),
            commit.completed_at,
        )?;

        let policy_head = if let Some(transition) = &commit.transition {
            // Transition/event/subject head 都受此事务保护；revision 由当前 head +1，首次从 1 起。
            let revision = previous
                .as_ref()
                .map_or(1, |head| head.revision.saturating_add(1));
            let transition_cursor = append_event(
                &transaction,
                &commit.permit.run_id,
                Some(&commit.permit.task_id),
                Some(&commit.permit.attempt_id),
                LifecycleEventType::PolicyTransitioned,
                Some(&commit.evaluation.artifact_id),
                commit.completed_at,
            )?;
            transaction.execute(
                r#"INSERT INTO rebuild_policy_transitions
                (transition_id, subject_id, from_state_json, to_state_json,
                 evaluation_artifact_id, run_id, revision, created_at, event_cursor)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"#,
                params![
                    transition.transition_id.0,
                    subject_id,
                    serde_json::to_string(&commit.from)?,
                    serde_json::to_string(&commit.to)?,
                    commit.evaluation.artifact_id.0.as_str(),
                    commit.permit.run_id.0,
                    revision,
                    transition.created_at.to_rfc3339(),
                    transition_cursor,
                ],
            )?;
            match previous {
                Some(_) => {
                    transaction.execute(
                    "UPDATE rebuild_policy_heads SET state_json = ?1, revision = ?2, transition_id = ?3, transition_event_cursor = ?4, updated_at = ?5 WHERE subject_id = ?6",
                    params![
                        serde_json::to_string(&commit.to)?,
                            revision,
                            transition.transition_id.0,
                            transition_cursor,
                            transition.created_at.to_rfc3339(),
                            commit.subject.subject_id(),
                        ],
                    )?;
                }
                None => {
                    transaction.execute(
                        r#"INSERT INTO rebuild_policy_heads
                        (subject_id, state_json, revision, transition_id,
                         transition_event_cursor, updated_at)
                       VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
                        params![
                            commit.subject.subject_id(),
                            serde_json::to_string(&commit.to)?,
                            revision,
                            transition.transition_id.0,
                            transition_cursor,
                            transition.created_at.to_rfc3339(),
                        ],
                    )?;
                }
            }
            Some(PolicyHead {
                subject: commit.subject.clone(),
                state: commit.to,
                revision,
                transition_id: transition.transition_id.clone(),
                transition_cursor,
                updated_at: transition.created_at,
            })
        } else {
            previous
        };

        if let Some(transition) = &commit.transition {
            // Contract subject 的目录 activation/head 与 policy transition 共用事务；其他 subject 是 no-op。
            self.apply_contract_catalogue_transition(&transaction, commit, transition)?;
        }

        // 最后写 evaluation immutable row 和 pair-consumption head；若完整任务则同事务收束 Task/Attempt。
        transaction.execute(
            r#"INSERT INTO rebuild_policy_evaluations
            (evaluation_artifact_id, subject_id, outcome_artifact_id,
             experience_artifact_id, candidate_policy_artifact_id, from_state_json,
             to_state_json, transition_id, run_id, consumed_pair_cursor, event_cursor,
             completed_at)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)"#,
            params![
                commit.evaluation.artifact_id.0.as_str(),
                commit.subject.subject_id(),
                commit.outcome.artifact_id.0.as_str(),
                commit.experience.artifact_id.0.as_str(),
                commit
                    .candidate_policy
                    .as_ref()
                    .map(|artifact| artifact.artifact_id.0.as_str()),
                serde_json::to_string(&commit.from)?,
                serde_json::to_string(&commit.to)?,
                commit
                    .transition
                    .as_ref()
                    .map(|transition| transition.transition_id.0.as_str()),
                commit.permit.run_id.0,
                consumed_pair_cursor,
                evaluation_cursor,
                commit.completed_at.to_rfc3339(),
            ],
        )?;
        transaction.execute(
            r#"INSERT INTO rebuild_policy_consumption_heads
            (subject_id, consumed_pair_cursor, evaluation_artifact_id,
             evaluation_event_cursor, updated_at)
           VALUES (?1, ?2, ?3, ?4, ?5)
           ON CONFLICT(subject_id) DO UPDATE SET
             consumed_pair_cursor = excluded.consumed_pair_cursor,
                   evaluation_artifact_id = excluded.evaluation_artifact_id,
                   evaluation_event_cursor = excluded.evaluation_event_cursor,
                   updated_at = excluded.updated_at"#,
            params![
                commit.subject.subject_id(),
                consumed_pair_cursor,
                commit.evaluation.artifact_id.0.as_str(),
                evaluation_cursor,
                commit.completed_at.to_rfc3339(),
            ],
        )?;
        if commit.complete_task {
            finish_permitted_task(
                &transaction,
                &commit.permit,
                TaskStatus::Succeeded,
                on_failure,
                Some(&commit.evaluation.artifact_id),
                commit.completed_at,
            )?;
        }
        transaction.commit()?;
        Ok(PolicyEvaluationResult {
            policy_head,
            consumed_pair_cursor,
            evaluation_cursor,
            newly_recorded: true,
        })
    }
}
