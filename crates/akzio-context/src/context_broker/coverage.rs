impl ContextBroker {
    // 将 Context 召回或来源变化记录成当前 Run 的审计 Artifact；这里只记录
    // 观察结果，不改变 Lesson 的生命周期，也不会把观察直接升级为 Policy。
    fn record_learning_observation(
        &self,
        permit: &TaskWritePermit,
        producer: &str,
        payload: &Value,
        refs: Vec<ArtifactRef>,
        now: DateTime<Utc>,
    ) -> ContextResult<()> {
        // payload 先进入 CAS，再由同一个 TaskWritePermit 提交事件；任一读取、序列化
        // 或 Store 写入错误都会通过 ? 返回，调用方不会得到“部分完成”的成功值。
        let artifact = Artifact::new(
            ArtifactKind::SemanticDetail,
            self.store.stage_json(payload)?,
            producer,
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.context".into(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: permit.contract_hash.clone(),
            },
            Some(permit.artifact_origin()),
            refs,
            now,
        )?;
        self.store.write_task_artifact(
            permit,
            &artifact,
            LifecycleEventType::ArtifactCommitted,
            now,
        )?;
        Ok(())
    }

    // 检查已召回 Lesson 的来源文档是否在同一 source/resource 上出现了更新，
    // 并仅生成 RunScoped 的重验建议；它不替换旧证据、不判定矛盾，也不自动重验 Lesson。
    fn suggest_lesson_revalidation(
        &self,
        permit: &TaskWritePermit,
        learning: &[ArtifactRef],
        evidence: &[ArtifactRef],
        now: DateTime<Utc>,
    ) -> ContextResult<()> {
        for selected in learning.iter().filter(|r| r.kind == ArtifactKind::Lesson) {
            // Lesson 没有治理信息时无法判断重验时间，按当前函数的保守边界跳过它。
            let artifact = self.store.artifact(&selected.artifact_id)?;
            let lesson: Lesson = self.read_payload(&artifact)?;
            let Some(governance) = &lesson.governance else {
                continue;
            };
            let mut updated = BTreeSet::new();
            for prior_ref in lesson
                .source_refs
                .iter()
                .filter(|r| r.kind == ArtifactKind::NormalizedEvidence)
            {
                let prior = self.store.artifact(&prior_ref.artifact_id)?;
                let prior_value = self.document_value(&prior)?;
                let Some(resource) = prior_value["resource"].as_str() else {
                    continue;
                };
                // 只比较同一 source_family、同一逻辑 resource 且创建时间更新的
                // NormalizedEvidence，避免把不同来源或不同资源误报成重验对象。
                for candidate in evidence
                    .iter()
                    .filter(|r| r.kind == ArtifactKind::NormalizedEvidence && *r != prior_ref)
                {
                    let next = self.store.artifact(&candidate.artifact_id)?;
                    if next.provenance.source_family == prior.provenance.source_family
                        && next.created_at > governance.last_revalidated_at
                        && next.blob.hash != prior.blob.hash
                        && self.document_value(&next)?["resource"] == resource
                    {
                        updated.insert(candidate.clone());
                    }
                }
            }
            if !updated.is_empty() {
                let refs = std::iter::once(selected.clone())
                    .chain(updated.iter().cloned())
                    .collect();
                self.record_learning_observation(permit,"learning.revalidation.suggestion",
                    &serde_json::json!({"lesson":selected,"reason":"source_updated_requires_revalidation",
                        "new_evidence":updated,"lifecycle_changed":false,
                        "interpretation":"A changed source warrants review; it does not establish contradiction or causal failure."}),refs,now)?;
            }
        }
        Ok(())
    }
    /// Capture what was a candidate at this exact assembly. Export-time existence
    /// cannot establish availability at an earlier manifest.
    fn record_context_coverage(
        &self,
        permit: &TaskWritePermit,
        manifest: &ContextManifest,
        candidates: &[Artifact],
        exclusion_reasons: &std::collections::BTreeMap<ArtifactId, &str>,
        now: DateTime<Utc>,
    ) -> ContextResult<()> {
        // candidates 是 Manifest 组装时观察到的集合；selected 只反映本次 Manifest
        // 的实际选择，不能用导出时才出现的 Artifact 倒推历史可用性。
        let selected = manifest
            .payload
            .selections
            .iter()
            .map(|s| &s.artifact.artifact_id)
            .collect::<BTreeSet<_>>();
        let records = candidates.iter().map(|a| {
            let value = self.document_value(a)?;
            let provided = selected.contains(&a.artifact_id);
            let unqualified = value["resource"].as_str().is_some_and(|r| r.starts_with("news:"))
                && value.pointer("/value/source_document/source_verified") != Some(&Value::Bool(true));
            let projection = if provided { Some(compact_governed_projection(a.kind, value.clone())) } else { None };
            Ok(serde_json::json!({"artifact_id":a.artifact_id,"kind":a.kind,"resource":value.get("resource"),
                "selection_status":if provided {"provided_material"} else {"collected_but_not_selected"},
                "exclusion_reason":if provided { None } else {exclusion_reasons.get(&a.artifact_id).copied()},
                "source_bytes":a.blob.bytes,
                "projected_bytes":projection.as_ref().map(serde_json::to_vec).transpose()?.map(|v|v.len()),
                "directional_qualification":if unqualified {"source_qualification_insufficient"} else {"subject_to_ground_validation"},
                "projection_omits_detail":projection.as_ref().is_some_and(|p| projection_omits_material(a.kind,&value,p)),
                "availability_scope":"candidate observed at manifest assembly; selection is not directional qualification"}))
        }).collect::<ContextResult<Vec<_>>>()?;
        // 覆盖记录本身是 RunScoped 审计材料，并通过 Manifest source_ref 绑定到本次组装。
        // 写入成功只表示审计 Artifact 已提交，不表示研究提案、Decision 或 Gate 已接受。
        let artifact = Artifact::new(
            ArtifactKind::SemanticDetail,
            self.store.stage_json(
                &serde_json::json!({"manifest":manifest.artifact.artifact_id,"records":records}),
            )?,
            "context.coverage",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.context".into(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: permit.contract_hash.clone(),
            },
            Some(permit.artifact_origin()),
            vec![ArtifactRef {
                artifact_id: manifest.artifact.artifact_id.clone(),
                kind: ArtifactKind::ContextManifest,
            }],
            now,
        )?;
        self.store.write_task_artifact(
            permit,
            &artifact,
            LifecycleEventType::ArtifactCommitted,
            now,
        )?;
        Ok(())
    }
}

// 判断投影是否丢失原值中的任一结构；NormalizedEvidence 要比较其 value 与
// value_summary，其他 Artifact 则比较整个文档。这里只做结构包含检查，不改变文档。
fn projection_omits_material(kind: ArtifactKind, original: &Value, projected: &Value) -> bool {
    fn contains(original: &Value, projected: &Value) -> bool {
        match (original, projected) {
            (Value::Object(left), Value::Object(right)) => left
                .iter()
                .all(|(key, value)| right.get(key).is_some_and(|v| contains(value, v))),
            (Value::Array(left), Value::Array(right)) => {
                left.len() == right.len() && left.iter().zip(right).all(|(a, b)| contains(a, b))
            }
            _ => original == projected,
        }
    }
    if kind == ArtifactKind::NormalizedEvidence && original.get("resource").is_some() {
        !contains(&original["value"], &projected["value_summary"])
    } else {
        !contains(original, projected)
    }
}

#[cfg(test)]
mod coverage_quality_tests {
    use super::*;
    #[test]
    fn projection_rewrapping_is_not_material_omission() {
        let original = serde_json::json!({"source":"alpaca","resource":"quote:QQQ","value":{"price":100,"bars":[1,2]}});
        let projected =
            compact_governed_projection(ArtifactKind::NormalizedEvidence, original.clone());
        assert!(!projection_omits_material(
            ArtifactKind::NormalizedEvidence,
            &original,
            &projected
        ));
        let long = serde_json::json!({"source":"alpaca","resource":"bars:QQQ:day","value":{"bars":[1,2,3,4,5,6]}});
        let projected = compact_governed_projection(ArtifactKind::NormalizedEvidence, long.clone());
        assert!(projection_omits_material(
            ArtifactKind::NormalizedEvidence,
            &long,
            &projected
        ));
    }
}
