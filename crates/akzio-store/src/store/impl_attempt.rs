// 文件导读：Attempt relation 在 claim 新尝试时写成 RunScoped Artifact/event，
// 用 parent_attempt_id 表达 retry/recovery lineage；该记录不改变任务的最终状态。
// 私有方法由 Store 的任务领取事务调用，沿用调用方传入的 `Transaction`，不会单独提交；
// payload 经领域校验、JSON 编码和内容寻址写入后，再写 Artifact 与事件；
// `put_blob_bytes(transaction, ...)` 虽写 durable 表，也仍随调用方事务失败整体回滚。
impl Store {
    // `permit` 提供 Run/Task/新 Attempt 与 Contract 来源，父 Attempt 和关系种类描述 lineage；
    // 时间戳由调用方统一提供。返回 `()` 只表示写入语句在当前事务内成功，最终持久化仍取决于外层提交。
    // `Transaction<'_>` 的借用生命周期不允许事务结束前脱离它写入；任一 `?` 出错会交给外层回滚。
    fn record_attempt_relation_in_transaction(
        &self,
        transaction: &Transaction<'_>,
        permit: &TaskWritePermit,
        parent_attempt_id: &AttemptId,
        relation: AttemptRelationKind,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        let payload = AttemptRelation {
            schema_version: DOMAIN_SCHEMA_VERSION,
            run_id: permit.run_id.clone(),
            task_id: permit.task_id.clone(),
            parent_attempt_id: parent_attempt_id.clone(),
            child_attempt_id: permit.attempt_id.clone(),
            relation,
            created_at: now,
        };
        payload.validate()?;
        let artifact = Artifact::new(
            ArtifactKind::AttemptRelation,
            blob::put_blob_bytes(
                transaction,
                &serde_json::to_vec(&payload)?,
                "application/json".to_owned(),
            )?,
            "akzio-store.attempt_relation",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio-store".to_owned(),
                observed_at: Some(now),
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: permit.contract_hash.clone(),
            },
            Some(permit.artifact_origin()),
            Vec::new(),
            now,
        )?;
        insert_artifact(transaction, &artifact)?;
        append_event(
            transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::AttemptRelationCreated,
            Some(&artifact.artifact_id),
            now,
        )?;
        Ok(())
    }
}
