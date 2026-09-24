// 文件导读：本文件提供 Attempt 关系、补采额度和 Attempt 事件的只读查询；
// 这些查询验证持久化 lineage，但不会凭读取结果发布输出或改变任务状态。
use super::*;

impl Store {
    // 只检查同一 Run/Task 是否已经记录过补采创建或放弃事件；事件存在表示额度已消费，
    // 不表示 provider 已成功返回事实，也不直接推进 Attempt 或 Decision 状态。
    /// A supplemental round is spent before provider I/O, across attempt recovery.
    pub fn task_has_supplemental_round(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
    ) -> StoreResult<bool> {
        Ok(self.connection()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM rebuild_events WHERE run_id=?1 AND task_id=?2 AND event_type IN ('supplemental.evidence_need_created','supplemental.round_abandoned'))",
            params![run_id.0, task_id.0], |row|row.get(0))?)
    }

    // 从 child Attempt 的关系事件读取唯一的 AttemptRelation，并复核 Artifact 类型、生命周期和 lineage。
    // 没有关系事件返回 None；同一 child 出现多个关系事件属于 Store 完整性错误而不是任意选择其一。
    pub fn attempt_relation(
        &self,
        child_attempt_id: &AttemptId,
    ) -> StoreResult<Option<AttemptRelation>> {
        let artifact = {
            let connection = self.connection()?;
            let mut statement = connection.prepare(
                r#"SELECT artifact_id
                   FROM rebuild_events
                   WHERE attempt_id = ?1 AND event_type = ?2
                   ORDER BY event_id ASC
                   LIMIT 2"#,
            )?;
            let artifact_ids = statement
                .query_map(
                    params![
                        child_attempt_id.0,
                        LifecycleEventType::AttemptRelationCreated.as_str()
                    ],
                    |row| row.get::<_, String>(0),
                )?
                .collect::<Result<Vec<_>, _>>()?;
            // LIMIT 2 用来区分“没有关系”“唯一关系”和“重复关系”，而不是截断合法历史。
            match artifact_ids.as_slice() {
                [] => return Ok(None),
                [artifact_id] => read_artifact(
                    &connection,
                    &ArtifactId(ContentHash::new(artifact_id.clone())?),
                )?,
                _ => {
                    return Err(StoreError::Integrity(format!(
                        "attempt {} has multiple relations",
                        child_attempt_id.0
                    )));
                }
            }
        };

        // 关系 Artifact 必须是 RunScoped 的 AttemptRelation；之后同时校验负载自身和来源绑定。
        if artifact.kind != ArtifactKind::AttemptRelation
            || artifact.lifecycle != ArtifactLifecycle::RunScoped
        {
            return Err(StoreError::Integrity(format!(
                "attempt {} relation event references invalid artifact",
                child_attempt_id.0
            )));
        }
        artifact.validate()?;
        let relation: AttemptRelation = self.read_artifact_payload(&artifact)?;
        relation.validate()?;
        let origin = artifact.origin.as_ref();
        if &relation.child_attempt_id != child_attempt_id
            || origin.is_none_or(|origin| {
                origin.run_id.as_ref() != Some(&relation.run_id)
                    || origin.task_id.as_ref() != Some(&relation.task_id)
                    || origin.attempt_id.as_ref() != Some(child_attempt_id)
            })
        {
            return Err(StoreError::Integrity(format!(
                "attempt {} relation lineage mismatch",
                child_attempt_id.0
            )));
        }
        Ok(Some(relation))
    }

    // 按 Run、Task、Attempt 的精确范围读取追加事件，并对每个事件重新检查可选字段形状。
    // 该方法只读历史；事件列表不会把 Attempt 标记为成功，也不会发布正式输出。
    pub fn attempt_events(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
        attempt_id: &AttemptId,
    ) -> StoreResult<Vec<StoredEvent>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"SELECT event_id, run_id, task_id, attempt_id, event_type, artifact_id, created_at
               FROM rebuild_events
               WHERE run_id = ?1 AND task_id = ?2 AND attempt_id = ?3
               ORDER BY event_id ASC"#,
        )?;
        let events = statement
            .query_map(
                params![run_id.0, task_id.0, attempt_id.0],
                trajectory::stored_event_from_row,
            )?
            // 迭代器消费阶段传播每一行的 SQLite 解码错误，并保持事件 ID 顺序。
            .collect::<Result<Vec<_>, _>>()?;
        for event in &events {
            // lifecycle kind 决定事件应带哪些 task/attempt/artifact 列；形状不符即拒绝读取。
            let event_type = event.lifecycle_kind()?;
            validate_event_shape(
                event_type,
                event.task_id.is_some(),
                event.attempt_id.is_some(),
                event.artifact_id.is_some(),
            )?;
        }
        Ok(events)
    }
}
