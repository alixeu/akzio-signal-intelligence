// 文件导读：本文件提供 Outcome/Retrospective、Policy head/transition 和消费 cursor 的
// 只读历史查询；每个返回值都来自 canonical evaluation 或明确的 Run artifact lineage。
// 阅读顺序可从 outcome/retrospective 的 Run-scoped 查询开始，再看 lifecycle health，
// 最后看 policy subject head/snapshot/influence/transition；这些查询不激活候选。
impl Store {
    /// Authoritative once-per-subject Outcome consumption lookup.
    // subject 与 Outcome Artifact ID 组成 SQL 过滤条件；event_cursor 升序选最早 evaluation，
    // 无行 None，有行后再从 CAS 重建 evaluation Artifact。
    pub fn policy_evaluation_for_outcome(
        &self,
        subject: &PolicySubject,
        outcome: &ArtifactRef,
    ) -> StoreResult<Option<Artifact>> {
        let connection = self.connection()?;
        let id: Option<String> = connection.query_row(
            "SELECT evaluation_artifact_id FROM rebuild_policy_evaluations WHERE subject_id=?1 AND outcome_artifact_id=?2 ORDER BY event_cursor LIMIT 1",
            params![subject.subject_id(), outcome.artifact_id.0.as_str()], |row| row.get(0)).optional()?;
        id.map(|id| read_artifact(&connection, &ArtifactId(ContentHash::new(id)?)))
            .transpose()
    }

    /// 读取指定 Run/Outcome/期限的复盘：允许一条原版，或一条原版加一条
    /// 符合 `valid_retrospective_repair` 的修订；其它重复形状报完整性错误。
    // Run+kind 索引先筛候选，再以 Artifact.origin 的 run_id 与 payload outcome_id/horizon 做精确过滤；
    // 恰有两条时只接受 valid_retrospective_repair 指定方向并返回修复后的新版本。
    pub fn retrospective_for(
        &self,
        run_id: &RunId,
        outcome_id: &OutcomeId,
        horizon: OutcomeHorizon,
    ) -> StoreResult<Option<Artifact>> {
        let connection = self.connection()?;
        let mut matching = Vec::new();
        for artifact in read_run_kind_artifacts(&connection, run_id, ArtifactKind::Retrospective)? {
            let Some(origin) = artifact.origin.as_ref() else {
                continue;
            };
            if origin.run_id.as_ref() != Some(run_id) {
                continue;
            }
            let payload: Retrospective = self.read_artifact_payload_with_connection(&connection, &artifact)?;
            if payload.outcome_id == *outcome_id && payload.horizon == horizon {
                matching.push(artifact);
            }
        }
        if matching.len() == 2 {
            let a: Retrospective = self.read_artifact_payload_with_connection(&connection, &matching[0])?;
            let b: Retrospective = self.read_artifact_payload_with_connection(&connection, &matching[1])?;
            if valid_retrospective_repair(&matching[0], &a, &matching[1], &b) {
                return Ok(Some(matching.remove(1)));
            }
            if valid_retrospective_repair(&matching[1], &b, &matching[0], &a) {
                return Ok(Some(matching.remove(0)));
            }
        }
        match matching.len() {
            0 => Ok(None),
            1 => Ok(matching.pop()),
            _ => Err(StoreError::Integrity(
                "duplicate retrospective identity".to_owned(),
            )),
        }
    }

    /// Explicit, bounded narrative-only retry. It never changes a session slot,
    /// broker commitment, original contract, or numeric Outcome.
    // 从已密封 Outcome 与其 T5 Retrospective 读取事实；事务内检查 Paper Run、已有 worker/repair 数量，
    // 复制冻结的 Outcome worker node 并只增加两个 Artifact 输入，不重写价格或 execution lineage。
    // 最多允许两条 repair Task；已有 queued/running repair 时直接返回该 Task ID、不新建 Task。
    pub fn request_outcome_narrative_repair(
        &self,
        run_id: &RunId,
        now: DateTime<Utc>,
    ) -> StoreResult<TaskId> {
        let outcome = self
            .outcome_for_run(run_id)?
            .ok_or(StoreError::InvalidLearningCommit("sealed_outcome_missing"))?;
        let payload: Outcome = self.read_artifact_payload(&outcome)?;
        let previous = self
            .retrospective_for(run_id, &payload.outcome_id, OutcomeHorizon::T5)?
            .ok_or(StoreError::InvalidLearningCommit("retrospective_missing"))?;
        let retrospective: Retrospective = self.read_artifact_payload(&previous)?;
        // 仅 ModelUnavailable/Complete 可请求叙事修复；其它状态不允许用新调用覆盖已有结论。
        if !matches!(
            retrospective.status,
            RetrospectiveStatus::ModelUnavailable | RetrospectiveStatus::Complete
        ) {
            return Err(StoreError::InvalidLearningCommit(
                "retrospective_already_valid",
            ));
        }
        let snapshot = self.workflow_snapshot(run_id)?;
        let mut node = snapshot
            .tasks
            .iter()
            .find(|t| t.node.recipe_id.as_str() == POST_TERMINAL_WORKER_RECIPE_ID)
            .ok_or(StoreError::InvalidLearningCommit("outcome_worker_missing"))?
            .node
            .clone();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_paper_run(&transaction, run_id)?;
        let mut repairs = transaction.prepare("SELECT task_id, status FROM rebuild_tasks WHERE run_id=?1 AND recipe_id=?2 AND objective LIKE '[narrative_repair]%' ORDER BY task_id")?
            .query_map(params![run_id.0, POST_TERMINAL_WORKER_RECIPE_ID], |row| Ok((row.get::<_,String>(0)?,row.get::<_,String>(1)?)))?
            .collect::<Result<Vec<_>,_>>()?;
        if let Some((id, _)) = repairs
            .iter()
            .find(|(_, status)| status == "queued" || status == "running")
        {
            // 此早退不写数据库；事务 guard 随 return Drop 释放。
            return Ok(TaskId(id.clone()));
        }
        let active_worker: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM rebuild_tasks WHERE run_id=?1 AND recipe_id=?2 AND status IN ('queued','running'))",
            params![run_id.0, POST_TERMINAL_WORKER_RECIPE_ID], |row| row.get(0))?;
        if active_worker {
            return Err(StoreError::InvalidLearningCommit(
                "outcome_worker_still_active",
            ));
        }
        if repairs.len() >= 2 {
            return Err(StoreError::InvalidLearningCommit("narrative_repair_limit"));
        }
        repairs.clear();
        let (hash, _) = contract_catalogue_head(
            &transaction,
            &ContractPurpose::new(POST_TERMINAL_WORKER_RECIPE_ID)?,
        )?
        .ok_or(StoreError::InvalidLearningCommit(
            "outcome_contract_missing",
        ))?;
        let installed = self
            .stored_contract_with_connection(&transaction, &hash)?
            .ok_or_else(|| StoreError::MissingContractInstallation(hash.clone()))?;
        // Contract 来自当前 catalogue head；图里若冻结了 Outcome worker budget，优先继续沿用该 budget。
        node.task_id = TaskId::new();
        node.contract_hash = Some(hash);
        node.budget = snapshot.revision.graph.agent_budgets.get(POST_TERMINAL_WORKER_RECIPE_ID).cloned().unwrap_or(installed.contract.budget);
        node.retry = installed.contract.retry;
        node.on_failure = installed.contract.on_failure;
        node.objective = "[narrative_repair] Repair T5 narrative from the immutable sealed Outcome; no new prices or execution.".to_owned();
        node.dependencies.clear();
        node.parent_task_id = None;
        node.input_artifacts.extend([
            ArtifactRef {
                artifact_id: outcome.artifact_id,
                kind: ArtifactKind::Outcome,
            },
            ArtifactRef {
                artifact_id: previous.artifact_id,
                kind: ArtifactKind::Retrospective,
            },
        ]);
        node.input_artifacts.sort();
        node.input_artifacts.dedup();
        // 新 Task 无 dependencies/parent_task_id，通过已密封 Outcome 和旧 T5 Retrospective 输入建立修复上下文。
        insert_task_node(&transaction, run_id, &node, now)?;
        append_event(
            &transaction,
            run_id,
            Some(&node.task_id),
            None,
            LifecycleEventType::OutcomeWorkerEnqueued,
            Some(&payload.schedule.artifact_id),
            now,
        )?;
        transaction.commit()?;
        Ok(node.task_id)
    }

    pub fn run_artifacts_by_kind(
        &self,
        run_id: &RunId,
        kind: ArtifactKind,
    ) -> StoreResult<Vec<Artifact>> {
        // 按 run_id+kind 的 Artifact 索引读取；不额外筛 lifecycle，也不解码 payload。
        let connection = self.connection()?;
        read_run_kind_artifacts(&connection, run_id, kind)
    }

    pub fn run_lifecycle_health(&self, run_id: &RunId) -> StoreResult<RunLifecycleHealth> {
        // 组合 workflow、schedule/outcome、retrospective、evaluation 与 token usage 多次只读查询；
        // 没有共同 SQL transaction，因此并发新写入可能发生在不同子查询之间。
        let snapshot = self.workflow_snapshot(run_id)?;
        let schedule = self.outcome_schedule_for_run(run_id)?;
        let schedule_payload = schedule
            .as_ref()
            .map(|a| self.read_artifact_payload::<OutcomeSchedule>(a))
            .transpose()?;
        let mut retrospective_status = BTreeMap::new();
        for horizon in OutcomeHorizon::ALL {
            // 每个 horizon 分开查询；Complete 显示 valid，其他已存 Retrospective 显示 unavailable，
            // 无 schedule 为 not_scheduled，有 schedule 无 Retrospective 为 pending。
            let status = if let Some(schedule) = &schedule_payload {
                match self.retrospective_for(run_id, &schedule.outcome_id, horizon)? {
                    Some(artifact) => {
                        let retro: Retrospective = self.read_artifact_payload(&artifact)?;
                        if retro.status == RetrospectiveStatus::Complete {
                            "valid"
                        } else {
                            "unavailable"
                        }
                    }
                    None => "pending",
                }
            } else {
                "not_scheduled"
            };
            retrospective_status.insert(enum_name(horizon), status.to_owned());
        }
        let experiences = self.run_artifacts_by_kind(run_id, ArtifactKind::Experience)?;
        let learning_eligible = experiences
            .last()
            .map(|a| self.read_artifact_payload::<akzio_domain::Experience>(a))
            .transpose()?
            .and_then(|e| e.evaluation_context.map(|c| c.learning_eligible));
        Ok(RunLifecycleHealth {
            execution_status: snapshot.status,
            outcome_scheduled: schedule.is_some(),
            numeric_outcome_sealed: self.outcome_for_run(run_id)?.is_some(),
            retrospective_status,
            outcome_worker_failed: snapshot.tasks.iter().any(|t| {
                t.node.recipe_id.as_str() == POST_TERMINAL_WORKER_RECIPE_ID
                    && t.status == TaskStatus::Failed
            }),
            narrative_repair_pending: snapshot.tasks.iter().any(|t| {
                t.node.objective.starts_with("[narrative_repair]")
                    && matches!(
                        t.status,
                        TaskStatus::Pending | TaskStatus::Leased | TaskStatus::Running
                    )
            }),
            learning_recorded: !self
                .run_artifacts_by_kind(run_id, ArtifactKind::Evaluation)?
                .is_empty(),
            learning_eligible,
            decision_production_usage: schedule_payload
                .as_ref()
                .map(|s| self.model_usage_for_producing_run(&s.decision))
                .transpose()?,
            outcome_usage: self.outcome_model_usage(run_id)?,
            lifecycle_usage: self.run_model_usage(run_id)?,
        })
    }

    /// Returns the accepted outcome for one run/outcome identity.
    /// Repeated evaluation attempts reuse this immutable materialization.
    // 按 Run kind 索引筛 origin、canonical lifecycle、sealed 和 outcome_id；零项 None、一项返回，
    // 多项身份冲突时报 Integrity，不随意选“最新”覆盖。
    pub fn outcome_for(
        &self,
        run_id: &RunId,
        outcome_id: &OutcomeId,
    ) -> StoreResult<Option<Artifact>> {
        let connection = self.connection()?;
        let mut matching = Vec::new();
        for artifact in read_run_kind_artifacts(&connection, run_id, ArtifactKind::Outcome)? {
            let Some(origin) = artifact.origin.as_ref() else {
                continue;
            };
            if origin.run_id.as_ref() != Some(run_id) {
                continue;
            }
            let payload: Outcome = self.read_artifact_payload_with_connection(&connection, &artifact)?;
            if artifact.lifecycle == ArtifactLifecycle::Canonical
                && payload.is_sealed()
                && payload.outcome_id == *outcome_id
            {
                matching.push(artifact);
            }
        }
        match matching.len() {
            0 => Ok(None),
            1 => Ok(matching.pop()),
            _ => Err(StoreError::Integrity(
                "duplicate outcome identity".to_owned(),
            )),
        }
    }

    /// 只读恢复本 Run 唯一 canonical OutcomeSchedule，验证 payload；
    /// 这里不查询 Policy head，更不推进 Outcome 阶段。
    // 本实现实际读取指定 Run 的 Canonical OutcomeSchedule，不读取 Policy head；解码并 validate 后只接受唯一项。
    pub fn outcome_schedule_for_run(&self, run_id: &RunId) -> StoreResult<Option<Artifact>> {
        let connection = self.connection()?;
        let mut matching =
            read_run_kind_artifacts(&connection, run_id, ArtifactKind::OutcomeSchedule)?
                .into_iter()
                .filter(|artifact| {
                    artifact
                        .origin
                        .as_ref()
                        .and_then(|origin| origin.run_id.as_ref())
                        == Some(run_id)
                        && artifact.lifecycle == ArtifactLifecycle::Canonical
                })
                .collect::<Vec<_>>();
        for artifact in &matching {
            let schedule: OutcomeSchedule = self.read_artifact_payload_with_connection(&connection, artifact)?;
            schedule.validate()?;
        }
        matching.sort_by_key(|artifact| artifact.created_at);
        match matching.len() {
            0 => Ok(None),
            1 => Ok(matching.pop()),
            _ => Err(StoreError::Integrity(format!(
                "run {run_id} has multiple OutcomeSchedule artifacts"
            ))),
        }
    }

    // 查找该 Run 唯一 sealed Outcome，接受 Canonical 或 RunScoped 生命周期；多条 sealed 记录视为完整性错误。
    pub fn outcome_for_run(&self, run_id: &RunId) -> StoreResult<Option<Artifact>> {
        let connection = self.connection()?;
        let mut matching = Vec::new();
        for artifact in read_run_kind_artifacts(&connection, run_id, ArtifactKind::Outcome)? {
            if artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run_id)
            {
                continue;
            }
            let outcome: Outcome = self.read_artifact_payload_with_connection(&connection, &artifact)?;
            if outcome.is_sealed()
                && matches!(
                    artifact.lifecycle,
                    ArtifactLifecycle::Canonical | ArtifactLifecycle::RunScoped
                )
            {
                matching.push(artifact);
            }
        }
        matching.sort_by_key(|artifact| artifact.created_at);
        match matching.len() {
            0 => Ok(None),
            1 => Ok(matching.pop()),
            _ => Err(StoreError::Integrity(format!(
                "run {run_id} has multiple sealed Outcome artifacts"
            ))),
        }
    }

    // 领域 validate subject 后按 subject_id 重建当前 policy head；无 transition head 返回 None。
    pub fn policy_head(&self, subject: &PolicySubject) -> StoreResult<Option<PolicyHead>> {
        subject.validate()?;
        let connection = self.connection()?;
        read_policy_head(&connection, subject)
    }

    /// Captures one durable freshness window for all horizons. The returned
    /// cutoff is later committed verbatim; pairs completed after it remain
    /// fresh even if they arrive before evaluation persistence.
    // 一份 Deferred 事务里同时抓 after/through cursor 和 horizon counts；evaluation 后续必须原样重验，
    // 新到达的 pair 不会被悄悄并入已开始的样本窗口。
    pub fn policy_shadow_pair_snapshot(
        &self,
        subject: &PolicySubject,
    ) -> StoreResult<PolicyShadowPairSnapshot> {
        subject.validate()?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let after_cursor = read_policy_consumption_head(&transaction, subject)?
            .map_or(0, |head| head.consumed_pair_cursor);
        let through_cursor = max_shadow_pair_cursor(&transaction, subject)?;
        let counts_by_horizon =
            shadow_pair_counts_between(&transaction, subject, after_cursor, through_cursor)?;
        transaction.commit()?;
        Ok(PolicyShadowPairSnapshot {
            after_cursor,
            through_cursor,
            counts_by_horizon,
        })
    }

    /// Resolves only policy influences that were durably committed by a
    /// canonical evaluation. Arbitrary Experience/CandidatePolicy artifacts
    /// therefore cannot enter Context or Execution provenance.
    // SQL UNION 仅查 evaluation ledger 登记过的 Experience/CandidatePolicy；再核 Artifact kind，
    // 并要求所有匹配行解析成同一个 typed PolicySubject。
    pub fn recorded_policy_influence_subject(
        &self,
        artifact_id: &ArtifactId,
    ) -> StoreResult<Option<PolicySubject>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"SELECT subject_id, 'experience'
                 FROM rebuild_policy_evaluations WHERE experience_artifact_id = ?1
                 UNION ALL
                 SELECT subject_id, 'candidate_policy'
                 FROM rebuild_policy_evaluations WHERE candidate_policy_artifact_id = ?1"#,
        )?;
        let rows = statement
            .query_map(params![artifact_id.0.as_str()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        if rows.is_empty() {
            return Ok(None);
        }
        let artifact = read_artifact(&connection, artifact_id)?;
        let mut resolved = None;
        for (subject_id, influence_kind) in rows {
            let expected_kind = match influence_kind.as_str() {
                "experience" => ArtifactKind::Experience,
                "candidate_policy" => ArtifactKind::CandidatePolicy,
                _ => unreachable!("query emits fixed influence kinds"),
            };
            if artifact.kind != expected_kind {
                return Err(StoreError::Integrity(format!(
                    "policy influence {artifact_id} has invalid kind"
                )));
            }
            let subject = parse_persisted_subject(&subject_id)?;
            if resolved.as_ref().is_some_and(|current| current != &subject) {
                return Err(StoreError::Integrity(format!(
                    "policy influence {artifact_id} has conflicting subjects"
                )));
            }
            resolved = Some(subject);
        }
        Ok(resolved)
    }

    /// Replays immutable policy transitions in revision order. Consumers use
    /// this for audit/replay; mutations remain limited to
    /// `record_policy_evaluation`.
    // 先校验 typed subject，再按 revision 升序重放其 immutable history，不移动当前 policy head。
    pub fn policy_transitions(
        &self,
        subject: &PolicySubject,
    ) -> StoreResult<Vec<PolicyTransitionRecord>> {
        subject.validate()?;
        let connection = self.connection()?;
        read_policy_transitions(&connection, subject)
    }
}
