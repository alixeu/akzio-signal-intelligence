// 文件导读：这是隔离 Debug 研究质量采样的 durable 预算边界：有限 producer 名称对应调用开始、
// capability 和结果 Artifact；reserve 在 provider I/O 前落盘，重启也不会把已消费额度退回。
// 先读 reserve_research_quality_call 的 IMMEDIATE 事务，再读只按 producer 查询的记录列表。
use super::*;

impl Store {
    // producer 必须属于固定三项 allowlist；之后按 producer 查询并按创建时间/Artifact ID 排序。
    // 每个 Artifact 重新解码，任一坏 ID/行返回 Err；读取不会增加调用额度。
    // ID 列表与随后 Artifact reads 没有包在 Deferred transaction 中，跨独立连接写入不保证同一时点。
    pub fn research_quality_records(&self, producer: &str) -> StoreResult<Vec<Artifact>> {
        if !matches!(
            producer,
            "research.quality.result"
                | "research.quality.capability"
                | "research.quality.call.started"
        ) {
            return Err(StoreError::Integrity(
                "unknown research quality record kind".into(),
            ));
        }
        let connection = self.connection()?;
        let mut statement = connection.prepare("SELECT artifact_id FROM rebuild_artifacts WHERE producer=?1 ORDER BY created_at, artifact_id")?;
        let ids = statement
            .query_map(params![producer], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|id| read_artifact(&connection, &ArtifactId(ContentHash::new(id)?)))
            .collect()
    }
    /// Reserve before provider I/O. Interrupted calls remain consumed; every
    /// phase and resumed invocation shares the same isolated Store allowance.
    // 请求 JSON 先在事务外 staging；事务内再核验 Task permit、Store 隔离标记和全局已预留数 < 40。
    // 达上限/非隔离时事务 Drop，不插入 Artifact/event；之前的 TEMP staging 不是 durable 预约。
    // 成功才把 call.started Artifact 与 event 同提交，
    // 返回 count+1 表示已预留次数，不表示 provider 已调用或成功返回。
    pub fn reserve_research_quality_call(
        &self,
        permit: &TaskWritePermit,
        request: &serde_json::Value,
    ) -> StoreResult<u64> {
        let now = Utc::now();
        let artifact = Artifact::new(
            ArtifactKind::SemanticDetail,
            self.stage_json(request)?,
            "research.quality.call.started",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.runtime".into(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: permit.contract_hash.clone(),
            },
            Some(permit.artifact_origin()),
            vec![],
            now,
        )?;
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&tx, permit)?;
        let isolated: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM rebuild_metadata WHERE key='debug_environment')",
            [],
            |row| row.get(0),
        )?;
        let count: u64 = tx.query_row(
            "SELECT count(*) FROM rebuild_artifacts WHERE producer='research.quality.call.started'",
            [],
            |row| row.get(0),
        )?;
        if !isolated || count >= 40 {
            return Err(StoreError::Integrity(
                "research quality requires an isolated Store and fewer than 40 reserved calls"
                    .into(),
            ));
        }
        insert_artifact(&tx, &artifact)?;
        append_event(
            &tx,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::ArtifactCommitted,
            Some(&artifact.artifact_id),
            now,
        )?;
        tx.commit()?;
        Ok(count + 1)
    }
}
