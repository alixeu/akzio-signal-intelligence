// 文件导读：Lesson 写入保留 source/supersedes/conflicts 的 CAS 闭包，并以独立 head/event
// 维护生命周期；debug 隔离 Store 不能借此把 Lesson 提升为 canonical Active。
// 先读 write_lesson 建立 immutable Artifact/head/event，再读 transition_lesson 的 CAS 更新，
// 最后看 record_lesson_evidence_with_transaction 如何加入外层学习事务而不自行提交。
impl Store {
    /// Persist an operator or outcome-derived Lesson with its source artifact.
    /// The source and Lesson are inserted atomically and a dedicated immutable
    /// lesson event records the actor without inventing a synthetic Run.
    // 输入领域 Lesson、其一个主来源 Artifact 和时间；payload staging 与表初始化发生在业务事务外，
    // 随后的 source/lesson Artifact、head revision=1 和 created event 才共同提交。
    // 相同 lesson_id+相同 payload 返回 existing；同 ID 不同内容返回 Integrity，不覆盖旧 head。
    pub fn write_lesson(
        &self,
        lesson: &Lesson,
        source: &Artifact,
        now: DateTime<Utc>,
    ) -> StoreResult<LessonWriteResult> {
        if self.debug_environment()?.is_some() && matches!(lesson.lifecycle, LessonLifecycle::Active | LessonLifecycle::Contested) {
            return Err(StoreError::InvalidLearningCommit("debug_learning_isolated"));
        }
        self.ensure_lesson_tables()?;
        lesson.validate()?;
        source.validate()?;
        if source.kind == ArtifactKind::Lesson || source.lifecycle != ArtifactLifecycle::Canonical {
            return Err(StoreError::InvalidLearningCommit("lesson.source"));
        }
        let source_ref = ArtifactRef {
            artifact_id: source.artifact_id.clone(),
            kind: source.kind,
        };
        if !lesson.source_refs.contains(&source_ref) {
            return Err(StoreError::InvalidLearningCommit("lesson.source_refs"));
        }

        let blob = self.stage_json(lesson)?;
        // producer/source_family 按 LessonOrigin 固定映射；source_refs 合并领域来源与 supersedes/conflicts。
        let producer = match lesson.origin {
            LessonOrigin::Operator => "learning.lesson.operator",
            LessonOrigin::OutcomeDerived => "learning.lesson.outcome",
        };
        let artifact = Artifact::new(
            ArtifactKind::Lesson,
            blob,
            producer,
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: match lesson.origin {
                    LessonOrigin::Operator => "akzio.operator".to_owned(),
                    LessonOrigin::OutcomeDerived => "akzio.learning".to_owned(),
                },
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: lesson.confidence_ppm,
                producer_contract_hash: None,
            },
            None,
            lesson
                .source_refs
                .iter()
                .cloned()
                .chain(lesson.supersedes.iter().cloned())
                .chain(lesson.conflicts_with.iter().cloned())
                .collect(),
            now,
        )?;

        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if transaction
            .query_row(
                "SELECT 1 FROM rebuild_lesson_heads WHERE lesson_id = ?1",
                params![lesson.lesson_id.0.as_str()],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .is_some()
        {
            // 已有 head 走严格幂等分支，只允许领域 Lesson 完全相等；不更新 actor、时间或 revision。
            let current = self
                .read_lesson_from_transaction(&transaction, &lesson.lesson_id)?
                .ok_or_else(|| StoreError::Integrity("lesson head disappeared".to_owned()))?;
            if current.lesson != *lesson {
                return Err(StoreError::Integrity(format!(
                    "lesson {} conflicts with its immutable head",
                    lesson.lesson_id
                )));
            }
            transaction.commit()?;
            return Ok(LessonWriteResult {
                lesson: current,
                newly_created: false,
            });
        }

        // 新建时先插 source，再核对所有相关 Lesson refs，然后写新 Artifact/head/event；
        // 任一 `?` 失败由 Immediate Transaction Drop 回滚这些行。
        insert_artifact(&transaction, source)?;
        self.validate_related_refs(&transaction, lesson)?;
        insert_artifact(&transaction, &artifact)?;
        transaction.execute(
            "INSERT INTO rebuild_lesson_heads (lesson_id, artifact_id, lifecycle, revision, updated_at) VALUES (?1, ?2, ?3, 1, ?4)",
            params![
                lesson.lesson_id.0.as_str(),
                artifact.artifact_id.0.as_str(),
                enum_name(lesson.lifecycle),
                lesson.updated_at.to_rfc3339(),
            ],
        )?;
        self.insert_lesson_event(
            &transaction,
            lesson,
            &artifact,
            "lesson.created",
            lesson.authored_by.as_deref().unwrap_or("learning.runtime"),
            None,
            now,
        )?;
        transaction.commit()?;
        Ok(LessonWriteResult {
            lesson: StoredLesson {
                artifact,
                lesson: lesson.clone(),
                revision: 1,
            },
            newly_created: true,
        })
    }

    pub(super) fn record_lesson_evidence_with_transaction(
        &self,
        transaction: &rusqlite::Transaction<'_>,
        records: &[LessonEvidence],
        now: DateTime<Utc>,
    ) -> StoreResult<u64> {
        // 借用 PolicyEvaluation 的 Transaction 与记录切片；不会开连接或 commit。
        // 每条记录验证时间、head 与三个 ArtifactRef，按 lesson/context/outcome 唯一键幂等；
        // 返回新增行数，后续外层 evaluation 失败时本批插入也回滚。
        let mut inserted = 0_u64;
        for record in records {
            record.validate()?;
            if record.recorded_at > now {
                return Err(StoreError::InvalidLearningCommit(
                    "lesson_evidence.recorded_at",
                ));
            }
            let (lesson_id, decision_context_id, outcome_id) = record.idempotency_key();
            if transaction
                .query_row(
                    "SELECT 1 FROM rebuild_lesson_heads WHERE lesson_id = ?1",
                    params![lesson_id.as_str()],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?
                .is_none()
            {
                return Err(StoreError::InvalidLearningCommit("lesson_evidence.lesson"));
            }
            // Provenance closure: every reference must resolve to a real artifact
            // of the declared kind before the record becomes durable.
            for reference in [
                &record.lesson_artifact,
                &record.decision_context,
                &record.outcome,
            ] {
        let artifact = read_artifact(transaction, &reference.artifact_id)?;
                artifact.validate()?;
                if artifact.kind != reference.kind {
                    return Err(StoreError::InvalidLearningCommit(
                        "lesson_evidence.reference_kind",
                    ));
                }
            }

            let evidence_id = record.identity_hash()?;
            // metrics_json 只投影按 horizon 的效用/校准数组；完整 identity 留在索引列和 hash 中。
        let metrics_json = serde_json::to_string(&LessonEvidenceMetrics::from(record))?;
        let existing = transaction
            .query_row(
                "SELECT lesson_id, lesson_artifact_id, decision_context_artifact_id, outcome_artifact_id, attribution, metrics_json, recorded_at FROM rebuild_lesson_evidence WHERE lesson_id = ?1 AND decision_context_artifact_id = ?2 AND outcome_artifact_id = ?3",
                params![
                    lesson_id.as_str(),
                    decision_context_id.as_str(),
                    outcome_id.as_str(),
                ],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                    ))
                },
            )
            .optional()?;
        if let Some((
            stored_lesson_id,
            lesson_artifact_id,
            decision_context_artifact_id,
            outcome_artifact_id,
            attribution,
            metrics_json,
            recorded_at,
        )) = existing
        {
            // 同一个 idempotency key 只比较 observation 内容；recorded_at 可以因重放变晚，
            // 但不覆盖首次记录时间，也不重复计数。
            let existing = lesson_evidence_from_columns(
                stored_lesson_id,
                lesson_artifact_id,
                decision_context_artifact_id,
                outcome_artifact_id,
                attribution,
                metrics_json,
                recorded_at,
            )?;
                // Compared on the observation, not on `recorded_at`: a genuine
                // reprocess of the same sealed decision arrives with a later
                // timestamp and must stay a no-op that keeps the first record.
                if !existing.describes_same_observation(record) {
                    return Err(StoreError::Integrity(format!(
                        "lesson evidence for {lesson_id} is immutable"
                    )));
                }
                continue;
            }
            transaction.execute(
            "INSERT INTO rebuild_lesson_evidence (evidence_id, lesson_id, lesson_artifact_id, decision_context_artifact_id, outcome_artifact_id, attribution, metrics_json, recorded_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    evidence_id.as_str(),
                    lesson_id.as_str(),
                    record.lesson_artifact.artifact_id.0.as_str(),
                    decision_context_id.as_str(),
                    outcome_id.as_str(),
                    record.attribution.as_str(),
                metrics_json,
                    record.recorded_at.to_rfc3339(),
                ],
            )?;
            inserted += 1;
        }
        Ok(inserted)
    }

    // 按 lesson_id 过滤并按 recorded_at/evidence_id 稳定排序；旧表形状无 ledger 时返回空 Vec。
    // `collect` 先把 Row 借用转成拥有型 String 元组，随后显式 drop statement/connection，
    // 再解码领域值；`Result` 的 collect 在任一行失败时不返回部分列表。
    pub fn lesson_evidence(&self, lesson_id: &LessonId) -> StoreResult<Vec<LessonEvidence>> {
        let connection = self.connection()?;
        if ensure_lesson_table_set(&connection)? != 3 {
            return Ok(Vec::new());
        }
        let mut statement = connection.prepare(
            "SELECT lesson_id, lesson_artifact_id, decision_context_artifact_id, outcome_artifact_id, attribution, metrics_json, recorded_at FROM rebuild_lesson_evidence WHERE lesson_id = ?1 ORDER BY recorded_at, evidence_id",
        )?;
        let rows = statement
            .query_map(params![lesson_id.0.as_str()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        drop(connection);
        rows.into_iter()
            .map(
                |(
                    lesson_id,
                    lesson_artifact_id,
                    decision_context_artifact_id,
                    outcome_artifact_id,
                    attribution,
                    metrics_json,
                    recorded_at,
                )| {
                    lesson_evidence_from_columns(
                        lesson_id,
                        lesson_artifact_id,
                        decision_context_artifact_id,
                        outcome_artifact_id,
                        attribution,
                        metrics_json,
                        recorded_at,
                    )
                },
            )
            .collect()
    }

    // 读单个可选 head；Lesson 表尚未初始化时返回 None，其余从 Deferred 事务读取 Artifact 与 payload。
    pub fn lesson(&self, lesson_id: &LessonId) -> StoreResult<Option<StoredLesson>> {
        let mut connection = self.connection()?;
        if ensure_lesson_table_set(&connection)? == 0 {
            return Ok(None);
        }
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let value = self.read_lesson_from_transaction(&transaction, lesson_id)?;
        transaction.commit()?;
        Ok(value)
    }

    // lifecycle=None 表示不加生命周期筛选，否则 SQL 精确筛 lifecycle；limit 夹到 1..=500。
    // 先查询有界 ID 列表，再逐个调用 lesson() 重建，故多条 Lesson 不共享一个显式 SQLite snapshot。
    pub fn lessons(
        &self,
        lifecycle: Option<LessonLifecycle>,
        limit: usize,
    ) -> StoreResult<Vec<StoredLesson>> {
        let connection = self.connection()?;
        if ensure_lesson_table_set(&connection)? == 0 {
            return Ok(Vec::new());
        }
        let limit = i64::try_from(limit.clamp(1, 500)).expect("bounded lesson limit fits i64");
        let mut statement = connection.prepare(
            "SELECT lesson_id FROM rebuild_lesson_heads WHERE (?1 IS NULL OR lifecycle = ?1) ORDER BY updated_at DESC, lesson_id DESC LIMIT ?2",
        )?;
        let ids = statement
            .query_map(params![lifecycle.map(enum_name), limit], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        drop(connection);
        let mut lessons = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(lesson) = self.lesson(&LessonId(id))? {
                lessons.push(lesson);
            }
        }
        Ok(lessons)
    }

    /// Scan one SQL snapshot in bounded pages before context relevance ranking.
    /// A relevant older lesson must not disappear behind 50 newer mismatches.
    pub fn active_lessons_snapshot(&self) -> StoreResult<Vec<StoredLesson>> {
        // Deferred 事务中以 lesson_id keyset 每批最多 128 个读取所有 active head；
        // 整轮都借用同一连接快照，cursor 推进到页尾 ID，避免 OFFSET 漏项/重复。
        let mut connection = self.connection()?;
        if ensure_lesson_table_set(&connection)? == 0 { return Ok(Vec::new()); }
        let tx = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let mut cursor = String::new();
        let mut lessons = Vec::new();
        loop {
            let ids = {
                let mut statement = tx.prepare("SELECT lesson_id FROM rebuild_lesson_heads WHERE lifecycle='active' AND lesson_id>?1 ORDER BY lesson_id LIMIT 128")?;
                let page = statement.query_map(params![cursor], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?;
                page
            };
            if ids.is_empty() { break; }
            for id in &ids {
                if let Some(lesson) = self.read_lesson_from_transaction(&tx, &LessonId(id.clone()))? { lessons.push(lesson); }
            }
            cursor = ids.last().expect("nonempty page").clone();
        }
        tx.commit()?;
        Ok(lessons)
    }

    // Usage 是 Artifact.source_refs 对 Lesson Artifact 的反向计数，ContextManifest 与 DecisionContext
    // 分两次读取；latest_used_at 取两类已持久 Artifact 的最大 created_at，不代表用户实际查看过。
    pub fn lesson_usage(&self, lesson_id: &LessonId) -> StoreResult<LessonUsage> {
        let lesson = self
            .lesson(lesson_id)?
            .ok_or_else(|| StoreError::Integrity(format!("missing lesson {lesson_id}")))?;
        let manifests = self.artifacts_referencing(
            &lesson.artifact.artifact_id,
            Some(ArtifactKind::ContextManifest),
        )?;
        let decisions = self.artifacts_referencing(
            &lesson.artifact.artifact_id,
            Some(ArtifactKind::DecisionContext),
        )?;
        let latest_used_at = manifests
            .iter()
            .chain(decisions.iter())
            .map(|artifact| artifact.created_at)
            .max();
        Ok(LessonUsage {
            context_manifests: manifests.len() as u64,
            decision_contexts: decisions.len() as u64,
            latest_used_at,
        })
    }

    /// Move every Active Lesson whose verifier validity or actual usage budget
    /// has elapsed to a new immutable Contested revision. This scan never runs
    /// a verifier and never reactivates a Lesson. Repeated scans are idempotent
    /// because only current Active heads are eligible.
    pub fn contest_lessons_due_for_revalidation(
        &self,
        now: DateTime<Utc>,
    ) -> StoreResult<LessonRevalidationScan> {
        // 先取 Active ID 清单，再逐 Lesson 读取 governance/usage；这不是整轮单一事务快照。
        // 到期或 governance 风险优先标 expired，只有无 validity_due 时才检查 usage 阈值；
        // 每个转换会在自身事务重查当前 head，故并发改变导致失败时不覆盖新 revision。
        self.ensure_lesson_tables()?;
        let lesson_ids = {
            let connection = self.connection()?;
            let mut statement = connection.prepare(
                "SELECT lesson_id FROM rebuild_lesson_heads \
                 WHERE lifecycle = 'active' ORDER BY lesson_id ASC",
            )?;
            let lesson_ids = statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            lesson_ids
        };

        let scanned_active = lesson_ids.len() as u64;
        let mut contested = 0_u64;
        for lesson_id in lesson_ids {
            let lesson_id = LessonId(lesson_id);
            let Some(current) = self.lesson(&lesson_id)? else {
                return Err(StoreError::Integrity(format!(
                    "active lesson head disappeared during revalidation scan: {lesson_id}"
                )));
            };
            if current.lesson.lifecycle != LessonLifecycle::Active {
                continue;
            }
            let governance = current.lesson.governance.as_ref().ok_or_else(|| {
                StoreError::Integrity(format!(
                    "active lesson has no governance during revalidation scan: {lesson_id}"
                ))
            })?;
            let usage = self.lesson_usage(&lesson_id)?;
            let usage_count = usage
                .context_manifests
                .saturating_add(usage.decision_contexts);
            let validity_due = governance.valid_until.is_some_and(|until| now > until)
                || governance.quarantine_reason.is_some()
                || governance.contradiction_count > 0
                || governance.post_use_failure_count > 0;
            let usage_due = usage_count >= governance.max_usage_before_revalidation;
            let reason = if validity_due {
                format!(
                    "[revalidation_due:expired] verifier validity or governance safety state expired; usage_count={usage_count}"
                )
            } else if usage_due {
                format!(
                    "[revalidation_due:usage_budget] actual usage_count={usage_count} reached max_usage_before_revalidation={}",
                    governance.max_usage_before_revalidation
                )
            } else {
                continue;
            };

            // 只调用生命周期变更入口写 immutable successor；不会调用 verifier、自动重新激活或覆盖旧 CAS。
            self.transition_lesson(
                &lesson_id,
                LessonLifecycle::Contested,
                "akzio.daemon.lesson-revalidation",
                &reason,
                now,
            )?;
            contested = contested.saturating_add(1);
        }

        Ok(LessonRevalidationScan {
            scanned_active,
            contested,
        })
    }

    /// Lifecycle changes create a successor artifact; prior revisions remain
    /// immutable and are linked through the Lesson supersedes field.
    // 输入新 lifecycle、actor/reason/time；先在事务外重建旧版本并生成 successor payload，
    // Immediate 事务再 CAS 检查 head Artifact ID、引用闭包与 Active 冲突，最后写 Artifact/head/event。
    // 事务外生成后若 head 已变化则返回 Integrity；旧 Artifact 保留不变。
    pub fn transition_lesson(
        &self,
        lesson_id: &LessonId,
        lifecycle: LessonLifecycle,
        actor: &str,
        reason: &str,
        now: DateTime<Utc>,
    ) -> StoreResult<StoredLesson> {
        if self.debug_environment()?.is_some() && matches!(lifecycle,LessonLifecycle::Active|LessonLifecycle::Contested) {
            return Err(StoreError::DebugControl("isolated_learning_cannot_activate_lessons".into()));
        }
        self.ensure_lesson_tables()?;
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(StoreError::InvalidLearningCommit(
                "lesson.transition_actor_reason",
            ));
        }
        let current = self
            .lesson(lesson_id)?
            .ok_or_else(|| StoreError::Integrity(format!("missing lesson {lesson_id}")))?;
        if current.lesson.lifecycle == lifecycle
            && !matches!(
                lifecycle,
                LessonLifecycle::Active | LessonLifecycle::Contested
            )
        {
            return Ok(current);
        }
        if matches!(current.lesson.lifecycle, LessonLifecycle::Retired) {
            return Err(StoreError::InvalidLearningCommit("lesson.retired"));
        }
        let mut next = current.lesson.clone();
        // 新 revision 以旧 Artifact 加入 supersedes；Active 重新生成治理期限，Contested 记录阻断原因。
        next.lifecycle = lifecycle;
        next.updated_at = now;
        next.supersedes.push(ArtifactRef {
            artifact_id: current.artifact.artifact_id.clone(),
            kind: ArtifactKind::Lesson,
        });
        next.supersedes.sort();
        next.supersedes.dedup();
        if matches!(
            lifecycle,
            LessonLifecycle::Active | LessonLifecycle::Contested
        ) {
            next.approved_by = Some(actor.to_owned());
        }
        match lifecycle {
            LessonLifecycle::Active => {
                next.governance = Some(match next.origin {
                    LessonOrigin::Operator => LessonGovernance::operator_reviewed(now),
                    LessonOrigin::OutcomeDerived => LessonGovernance::outcome_revalidated(now),
                });
            }
            LessonLifecycle::Contested => {
                if let Some(governance) = &mut next.governance {
                    governance.trust_class = akzio_domain::LessonTrustClass::Contested;
                    governance.quarantine_reason = Some(reason.to_owned());
                    if reason.starts_with("[contradiction:") {
                        governance.contradiction_count =
                            governance.contradiction_count.saturating_add(1);
                    } else if reason.starts_with("[post_use_failure:") {
                        governance.post_use_failure_count =
                            governance.post_use_failure_count.saturating_add(1);
                    }
                }
            }
            LessonLifecycle::Draft | LessonLifecycle::Retired => {}
        }
        next.validate()?;

        let blob = self.stage_json(&next)?;
        let artifact = Artifact::new(
            ArtifactKind::Lesson,
            blob,
            "learning.lesson.lifecycle",
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: match next.origin {
                    LessonOrigin::Operator => "akzio.operator".to_owned(),
                    LessonOrigin::OutcomeDerived => "akzio.learning".to_owned(),
                },
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: next.confidence_ppm,
                producer_contract_hash: None,
            },
            None,
            current
                .lesson
                .source_refs
                .iter()
                .cloned()
                .chain(next.supersedes.iter().cloned())
                .chain(next.conflicts_with.iter().cloned())
                .collect(),
            now,
        )?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let head = transaction
            .query_row(
                "SELECT artifact_id, revision FROM rebuild_lesson_heads WHERE lesson_id = ?1",
                params![lesson_id.0.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?)),
            )
            .optional()?;
        let Some((head_artifact, revision)) = head else {
            return Err(StoreError::Integrity(format!("missing lesson {lesson_id}")));
        };
        if head_artifact != current.artifact.artifact_id.0.as_str() {
            return Err(StoreError::Integrity(
                "lesson head changed concurrently".to_owned(),
            ));
        }
        self.validate_related_refs(&transaction, &next)?;
        if lifecycle == LessonLifecycle::Active {
            // Active head 的 conflict refs 逐个解析；同一 Lesson 的自冲突除外，其它 Active 冲突阻止提交。
            for conflict in &next.conflicts_with {
                let conflict_lesson = self.read_lesson_artifact(&transaction, conflict)?;
                let active = transaction
                    .query_row(
                        "SELECT 1 FROM rebuild_lesson_heads WHERE lesson_id = ?1 AND lifecycle = 'active'",
                        params![conflict_lesson.lesson.lesson_id.0.as_str()],
                        |row| row.get::<_, i64>(0),
                    )
                    .optional()?
                    .is_some();
                if active && conflict_lesson.lesson.lesson_id != next.lesson_id {
                    return Err(StoreError::InvalidLearningCommit("lesson.active_conflict"));
                }
            }
        }
        insert_artifact(&transaction, &artifact)?;
        transaction.execute(
            "UPDATE rebuild_lesson_heads SET artifact_id = ?1, lifecycle = ?2, revision = ?3, updated_at = ?4 WHERE lesson_id = ?5",
            params![
                artifact.artifact_id.0.as_str(),
                enum_name(next.lifecycle),
                revision.saturating_add(1),
                now.to_rfc3339(),
                lesson_id.0.as_str(),
            ],
        )?;
        self.insert_lesson_event(
            &transaction,
            &next,
            &artifact,
            "lesson.lifecycle_changed",
            actor,
            Some(reason),
            now,
        )?;
        transaction.commit()?;
        Ok(StoredLesson {
            artifact,
            lesson: next,
            revision: revision.saturating_add(1),
        })
    }

    fn read_lesson_from_transaction(
        &self,
        transaction: &rusqlite::Transaction<'_>,
        lesson_id: &LessonId,
    ) -> StoreResult<Option<StoredLesson>> {
        // 从 head 表读 ID/revision 后，在同一事务连接解码 Artifact 与 CAS；缺 head 为 None，
        // Artifact kind、payload identity 或领域校验错误则返回 Err。
        let Some((artifact_id, revision)) = transaction
            .query_row(
                "SELECT artifact_id, revision FROM rebuild_lesson_heads WHERE lesson_id = ?1",
                params![lesson_id.0.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?)),
            )
            .optional()?
        else {
            return Ok(None);
        };
        let artifact = read_artifact(transaction, &ArtifactId(ContentHash::new(artifact_id)?))?;
        if artifact.kind != ArtifactKind::Lesson {
            return Err(StoreError::Integrity(
                "lesson head has wrong artifact kind".to_owned(),
            ));
        }
        let lesson: Lesson = serde_json::from_slice(&blob::read_blob_bytes(
            transaction,
            &artifact.blob.hash,
            artifact.blob.bytes,
        )?)?;
        lesson.validate()?;
        if &lesson.lesson_id != lesson_id {
            return Err(StoreError::Integrity(
                "lesson head payload identity mismatch".to_owned(),
            ));
        }
        Ok(Some(StoredLesson {
            artifact,
            lesson,
            revision,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_lesson_event(
        &self,
        transaction: &rusqlite::Transaction<'_>,
        lesson: &Lesson,
        artifact: &Artifact,
        event_type: &str,
        actor: &str,
        reason: Option<&str>,
        created_at: DateTime<Utc>,
    ) -> StoreResult<()> {
        // 只在调用方事务内追加 immutable ledger 行；event_type/actor/reason 的允许形状由 Doctor 再核验。
        transaction.execute(
            "INSERT INTO rebuild_lesson_events (lesson_id, artifact_id, event_type, actor, reason, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                lesson.lesson_id.0.as_str(),
                artifact.artifact_id.0.as_str(),
                event_type,
                actor,
                reason,
                created_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    fn read_lesson_artifact(
        &self,
        transaction: &rusqlite::Transaction<'_>,
        reference: &ArtifactRef,
    ) -> StoreResult<StoredLesson> {
        // 通过显式 ArtifactRef 读取 Lesson，不读取当前 head；revision=0 表示这是关联版本而非 head 快照。
        if reference.kind != ArtifactKind::Lesson {
            return Err(StoreError::InvalidLearningCommit("lesson.related_refs"));
        }
        let artifact = read_artifact(transaction, &reference.artifact_id)?;
        if artifact.kind != ArtifactKind::Lesson {
            return Err(StoreError::InvalidLearningCommit("lesson.related_refs"));
        }
        let lesson: Lesson = serde_json::from_slice(&blob::read_blob_bytes(
            transaction,
            &artifact.blob.hash,
            artifact.blob.bytes,
        )?)?;
        lesson.validate()?;
        Ok(StoredLesson {
            artifact,
            lesson,
            revision: 0,
        })
    }

    fn validate_related_refs(
        &self,
        transaction: &rusqlite::Transaction<'_>,
        lesson: &Lesson,
    ) -> StoreResult<()> {
        // 只验证 supersedes/conflicts_with 直接引用存在且 kind 正确；source_refs 另由 write/Doctor 闭包校验。
        for reference in lesson.supersedes.iter().chain(lesson.conflicts_with.iter()) {
            self.read_lesson_artifact(transaction, reference)?;
        }
        Ok(())
    }
}
