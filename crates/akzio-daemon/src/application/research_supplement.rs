// 文件导读：shared supplemental 只从冻结 EvidenceNeed 和 Claim/Critique gap 展开受治理
// 资源，按稳定顺序、每轮最多 8 个 distinct resource、先持久化 started 再做外部 I/O。
// disposition 的 accepted/deduplicated/unknown_after_crash/no_new_facts 只决定受影响
// horizon 是否允许一次 rerun，不改旧 Claim/Critique，也不直接生成 Proposal/Decision。
// Rust 机制：泛型 `supplement_record<T: Serialize>` 统一写 CAS；闭包排序/过滤借用行，
// BTreeMap/Set 保证去重与恢复稳定；async `join_all` 并发采集但所有输出仍用 permit 写入。

use crate::*;
use akzio_domain::ResearchCritique;
use akzio_domain::{
    EvidenceGapImpact, SupplementalDisposition, SupplementalIntent, SupplementalKind,
    SupplementalRound,
};
use serde_json::{json, Value};

// 只保留 Artifact 的 CAS 引用，补采记录通过 source_refs 维护完整请求血缘。
fn artifact_ref(a: &Artifact) -> ArtifactRef {
    ArtifactRef {
        artifact_id: a.artifact_id.clone(),
        kind: a.kind,
    }
}

/// Use the frozen run's resources, never model-supplied dates or routing strings.
// 把模型声明的 SupplementalIntent 绑定到 Run 冻结的 EvidenceNeed；返回的需求已经
// 经过资源解析和领域校验，不能由 intent.query 或任意字符串扩展路由。
fn expand_intent(
    intent: &SupplementalIntent,
    frozen: &[EvidenceNeed],
) -> Result<Vec<EvidenceNeed>> {
    intent.validate()?;
    let keys = match intent.kind {
        SupplementalKind::News => intent
            .assets
            .iter()
            .map(|a| format!("news:{}:", a.symbol()))
            .collect::<Vec<_>>(),
        SupplementalKind::Price => intent
            .assets
            .iter()
            .map(|a| format!("bars:{}:", a.symbol()))
            .collect(),
        SupplementalKind::Macro => intent
            .series
            .iter()
            .map(|s| format!("series:{s}:"))
            .collect(),
    };
    keys.into_iter()
        .map(|key| {
            // News/Price 必须命中同类、同资源的冻结需求；Macro 可沿冻结 series 模板
            // 替换序列名但保留其余窗口/格式，仍要再次经过 GovernedResource 解析。
            let need = frozen
                .iter()
                .find(|n| n.resource.starts_with(&key))
                .cloned()
                .or_else(|| {
                    if intent.kind != SupplementalKind::Macro {
                        return None;
                    }
                    let base = frozen.iter().find(|n| n.resource.starts_with("series:"))?;
                    let mut need = base.clone();
                    let suffix = need.resource.splitn(3, ':').nth(2)?;
                    need.resource = format!("{key}{suffix}");
                    Some(need)
                })
                .ok_or_else(|| {
                    DaemonError::InvalidInput(format!("no governed frozen resource for {key}"))
                })?;
            akzio_ingest::GovernedResource::parse(
                evidence_source(&need.source_family)?,
                &need.resource,
            )?;
            need.validate()?;
            Ok(need)
        })
        .collect()
}

// 提取事实比较所需的稳定字段，忽略 retrieved_at/provider_result 等每次请求都会变的
// 元数据；用于判断补采是否真的带来新事实，而不是只换了响应包装。
fn fact_value(payload: &Value) -> Value {
    if payload["resource"]
        .as_str()
        .is_some_and(|r| r.starts_with("news:"))
    {
        return json!({"facts":payload.pointer("/value/reviewed_facts"),"verified":payload.pointer("/value/source_document/source_verified")});
    }
    match payload["resource"].as_str().unwrap_or_default() {
        r if r.starts_with("bars:") => {
            json!({"bars":payload["value"]["bars"],"feed":payload["value"]["feed"],"adjustment":payload["value"]["adjustment"]})
        }
        r if r.starts_with("series:") => {
            json!({"observations":payload["value"]["observations"],"units":payload["value"]["units"]})
        }
        _ => Value::Null,
    }
}

impl Daemon {
    // 将补采状态/结果写成 RunScoped SemanticDetail，并通过 TaskWritePermit 提交到 Store；
    // 返回 Artifact 供当前 Attempt 输出引用，写入本身不代表证据已被接受。
    fn supplement_record<T: serde::Serialize>(
        &self,
        task: &ClaimedAttempt,
        producer: &str,
        value: &T,
        sources: Vec<ArtifactRef>,
        now: DateTime<Utc>,
    ) -> Result<Artifact> {
        let artifact = Artifact::new(
            ArtifactKind::SemanticDetail,
            self.store.stage_json(value)?,
            producer,
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.ingest".into(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            Some(task.permit.artifact_origin()),
            sources,
            now,
        )?;
        self.store.write_task_artifact(
            &task.permit,
            &artifact,
            LifecycleEventType::ArtifactCommitted,
            now,
        )?;
        Ok(artifact)
    }

    // 协调一次全 Run 补采：读取冻结资源和原始 cutoff，汇总 Claim/Critique 请求，按
    // 稳定顺序去重并在每次外部 I/O 前持久化 started；最终输出处置记录、round 摘要及
    // 新证据。它最多影响一次受影响-horizon rerun，不直接生成 Proposal/Decision。
    pub(crate) async fn shared_research_supplement(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        // ancestor_outputs 只收集本 Run 依赖的成功/跳过输出；snapshot 则提供冻结图，
        // 两者共同限定补采协调节点的输入范围。
        let ancestors = self.ancestor_outputs(task)?;
        let snapshot = self.store.workflow_snapshot(&task.run_id)?;
        if snapshot
            .tasks
            .iter()
            .filter(|t| t.node.recipe_id.as_str() == akzio_domain::RESEARCH_SUPPLEMENT_RECIPE_ID)
            .count()
            != 1
        {
            // 补采额度和恢复语义依赖单一协调节点；图中缺失或重复都无法安全判断
            // “一轮”是否已经使用，因此直接拒绝而不是继续发请求。
            return Err(DaemonError::InvalidInput(
                "a frozen Run must have exactly one supplemental coordination node".into(),
            ));
        }
        let frozen = snapshot
            .tasks
            .iter()
            .flat_map(|t| &t.node.input_artifacts)
            .filter(|r| r.kind == ArtifactKind::EvidenceNeed)
            .map(|r| self.read_artifact_payload::<EvidenceNeed>(r))
            .collect::<Result<Vec<_>>>()?;
        // frozen 需求来自 WorkflowNode 的输入 Artifact，而不是 Claim 中的资源字符串；
        // original 只读取已有 NormalizedEvidence 的 payload，用于确定 cutoff 和新事实比较。
        let original = ancestors
            .iter()
            .filter(|a| a.kind == ArtifactKind::NormalizedEvidence)
            .map(|a| self.read_artifact_payload::<Value>(&artifact_ref(a)))
            .collect::<Result<Vec<_>>>()?;
        let cutoff = original
            .iter()
            .filter_map(|v| {
                v.pointer("/time_basis/decision_clock/decision_cutoff")
                    .and_then(Value::as_str)
            })
            .filter_map(|s| {
                DateTime::parse_from_rfc3339(s)
                    .ok()
                    .map(|t| t.with_timezone(&Utc))
            })
            .min()
            .ok_or_else(|| {
                DaemonError::InvalidInput(
                    "supplemental round requires the original frozen evidence cutoff".into(),
                )
            })?;
        let mut requests = Vec::new();
        for a in ancestors
            .iter()
            .filter(|a| matches!(a.kind, ArtifactKind::Claim | ArtifactKind::Critique))
        {
            // Critique 的 gap 归属其 target Claim；后续 disposition 仍以请求来源 Artifact
            // 记录，避免只按文字 horizon 合并来自不同角色的请求。
            let (horizon, gaps) = if a.kind == ArtifactKind::Claim {
                let claim: ResearchClaim = self.read_artifact_payload(&artifact_ref(a))?;
                (claim.horizon, claim.evidence_gaps)
            } else {
                let critique: ResearchCritique = self.read_artifact_payload(&artifact_ref(a))?;
                let claim: ResearchClaim = self.read_artifact_payload(&critique.target)?;
                (claim.horizon, critique.evidence_gaps)
            };
            for (gap_index, gap) in gaps.into_iter().enumerate() {
                for (request_index, intent) in gap.supplemental_requests.iter().enumerate() {
                    // 先记录影响和可重试性，再决定是否展开资源；非法 scope/资源也要留下
                    // disposition，便于区分“未请求”与“协议被拒绝”。
                    let status = if gap.impact != EvidenceGapImpact::BlocksDirectionalForecast {
                        "skipped_impact"
                    } else if !gap.retriable {
                        "not_retriable"
                    } else {
                        "requested"
                    };
                    let row = SupplementalDisposition {
                        requester: artifact_ref(a),
                        horizon,
                        gap_index,
                        request_index,
                        resource: None,
                        status: status.into(),
                        reason: gap.rationale.clone(),
                        evidence: vec![],
                    };
                    let valid_scope = (gap.assets.is_empty()
                        || intent.assets.iter().all(|a| gap.assets.contains(a)))
                        && (gap.horizons.is_empty() || gap.horizons.contains(&horizon));
                    let expanded = if valid_scope {
                        expand_intent(intent, &frozen)
                    } else {
                        Err(DaemonError::InvalidInput(
                            "request leaves the blocking gap scope".into(),
                        ))
                    };
                    match expanded {
                        Ok(needs) => {
                            // 一个 News/Price intent 可能展开为多个冻结资源；每条资源都
                            // 保留同一 requester、gap_index 和 request_index。
                            for need in needs {
                                let mut row = row.clone();
                                row.resource = Some(need.resource.clone());
                                requests.push((row, Some(need)));
                            }
                        }
                        Err(error) => {
                            // 展开失败仍进入结果，状态为 invalid_protocol；不把错误吞成
                            // 空资源，也不尝试使用模型提供的日期/路由兜底。
                            let mut row = row;
                            row.status = "invalid_protocol".into();
                            row.reason = error.to_string();
                            requests.push((row, None));
                        }
                    }
                }
            }
        }
        fn priority(
            row: &SupplementalDisposition,
        ) -> (akzio_domain::DecisionHorizon, bool, &str, &str) {
            let resource = row.resource.as_deref().unwrap_or("");
            let mut parts = resource.split(':');
            let kind = parts.next().unwrap_or("");
            let asset_or_series = parts.next().unwrap_or("");
            (row.horizon, kind == "series", asset_or_series, kind)
        }
        // priority 只解析资源前缀，用于给共享补采分配稳定的处理顺序，不改变请求内容。
        // 先按 horizon/资源类型/资产稳定排序，再按 requester 和请求位置打破平局，
        // 让共享补采的额度消耗和恢复结果可重复。
        requests.sort_by(|(a, _), (b, _)| {
            priority(a).cmp(&priority(b)).then_with(|| {
                (&a.resource, &a.requester, a.gap_index, a.request_index).cmp(&(
                    &b.resource,
                    &b.requester,
                    b.gap_index,
                    b.request_index,
                ))
            })
        });
        // A start is durable before I/O. Recovery never reissues an uncertain request.
        // history 只筛当前补采节点的 SemanticDetail，避免把其他 Run/节点的 started 或
        // disposition 当成本轮预算；未知完成状态会保留为不可重发。
        let history = self
            .store
            .run_artifacts_by_kind(&task.run_id, ArtifactKind::SemanticDetail)?;
        let history = history
            .into_iter()
            .filter(|a| {
                a.origin.as_ref().and_then(|o| o.task_id.as_ref()) == Some(&task.node.task_id)
            })
            .collect::<Vec<_>>();
        let mut done = BTreeMap::<String, SupplementalDisposition>::new();
        let mut started = BTreeSet::new();
        for a in &history {
            if a.producer == "research.supplement.started" {
                let v: Value = self.read_artifact_payload(&artifact_ref(a))?;
                if let Some(r) = v["resource"].as_str() {
                    started.insert(r.to_owned());
                }
            }
            if a.producer == "research.supplement.disposition" {
                let row: SupplementalDisposition = self.read_artifact_payload(&artifact_ref(a))?;
                if let Some(r) = &row.resource {
                    done.insert(r.clone(), row);
                }
            }
        }
        let mut round = SupplementalRound::default();
        for (mut row, need) in requests {
            if row.status == "requested" {
                // requested 行一定有已通过 expand_intent 的需求；之后按 durable done、
                // started、八条资源上限依次处理，任何命中都不再次发起外部采集。
                let need = need.expect("requested resources parsed above");
                let key = need.resource.clone();
                if let Some(previous) = done.get(&key) {
                    row.status = "deduplicated".into();
                    row.reason = format!("shared resource disposition: {}", previous.status);
                    row.evidence = previous.evidence.clone();
                } else if started.contains(&key) {
                    row.status = "unknown_after_crash".into();
                    row.reason = "request started before interruption; not reissued".into();
                } else if started.len() >= 8 {
                    row.status = "budget_exhausted".into();
                    row.reason = "Run limit: eight distinct resources in one round".into();
                } else {
                    // 在创建 EvidenceNeed 和执行 adapter I/O 前先记录 started；崩溃恢复时
                    // started 但无 disposition 的资源进入 unknown_after_crash。
                    self.supplement_record(
                        task,
                        "research.supplement.started",
                        &json!({"resource":key}),
                        vec![row.requester.clone()],
                        now,
                    )?;
                    started.insert(key.clone());
                    let artifact = Artifact::new(
                        ArtifactKind::EvidenceNeed,
                        self.store.stage_json(&need)?,
                        "agent.supplemental.evidence_need",
                        ArtifactLifecycle::RunScoped,
                        ArtifactProvenance {
                            source_family: "akzio.agent".into(),
                            observed_at: None,
                            retrieved_at: now,
                            source_uri: None,
                            confidence_ppm: 1_000_000,
                            producer_contract_hash: None,
                        },
                        Some(task.permit.artifact_origin()),
                        vec![row.requester.clone()],
                        now,
                    )?;
                    self.store.write_task_artifact(
                        &task.permit,
                        &artifact,
                        LifecycleEventType::SupplementalEvidenceNeedCreated,
                        now,
                    )?;
                    // 底层采集器返回的是已标准化引用；错误保留为 collection_failed，
                    // 成功但未通过事实条件的结果保留 no_new_facts。
                    let refs = self
                        .acquire_supplemental_evidence(
                            task,
                            &[(artifact_ref(&artifact), artifact, need)],
                            cutoff,
                        )
                        .await;
                    match refs {
                        Err(error) => {
                            row.status = "collection_failed".into();
                            row.reason = error.to_string();
                        }
                        Ok(refs) => {
                            row.status = "no_new_facts".into();
                            row.reason = "no new admissible facts within frozen cutoff".into();
                            for r in refs {
                                // 只有 citations 完整、新闻 source_verified、available_at
                                // 不晚于原 cutoff 且事实内容不同的引用，才进入本轮 evidence。
                                let payload: Value = self.read_artifact_payload(&r)?;
                                let available = payload
                                    .pointer("/time_basis/available_at")
                                    .and_then(Value::as_str)
                                    .and_then(|s| DateTime::parse_from_rfc3339(s).ok());
                                let verified = payload["quality"]["citations_complete"] == true
                                    && (!key.starts_with("news:")
                                        || payload
                                            .pointer("/value/source_document/source_verified")
                                            == Some(&Value::Bool(true)));
                                if verified
                                    && available.is_some_and(|t| t.with_timezone(&Utc) <= cutoff)
                                    && !original.iter().any(|v| {
                                        v["resource"] == payload["resource"]
                                            && fact_value(v) == fact_value(&payload)
                                    })
                                {
                                    row.evidence.push(r);
                                }
                            }
                            if !row.evidence.is_empty() {
                                // accepted 只说明新增事实允许一次受影响 horizon rerun；它
                                // 不直接修改原 Claim/Critique 或给予 Decision 权限。
                                row.status = "accepted".into();
                                row.reason =
                                    "new admissible facts; one affected-horizon rerun permitted"
                                        .into();
                            }
                        }
                    }
                    self.supplement_record(
                        task,
                        "research.supplement.disposition",
                        &row,
                        row.evidence
                            .iter()
                            .cloned()
                            .chain([row.requester.clone()])
                            .collect(),
                        Utc::now(),
                    )?;
                    done.insert(key, row.clone());
                }
            }
            if !row.evidence.is_empty() {
                // round 只标记确实带来新事实的 horizon；没有证据的处置记录不会触发 rerun。
                round.affected_horizons.insert(row.horizon);
                round.evidence.extend(row.evidence.clone());
            }
            round.dispositions.push(row);
        }
        round.evidence.sort();
        round.evidence.dedup();
        // 结果记录包含所有 disposition，即使没有新事实或发生错误；输出证据另外附上
        // 供后续 Analyst/Critic 通过 ArtifactRef 读取，CAS 内容本身不被改写。
        let result = self.supplement_record(
            task,
            "research.supplement.result",
            &round,
            round
                .evidence
                .iter()
                .cloned()
                .chain(round.dispositions.iter().map(|d| d.requester.clone()))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            Utc::now(),
        )?;
        let mut outputs = vec![result];
        for r in &round.evidence {
            outputs.push(self.store.artifact(&r.artifact_id)?);
        }
        // Succeeded 只表示补采协调结果已持久化；是否重跑以及重跑哪些 horizon 由下一轮
        // research_loop 根据 affected_horizons 决定。
        Ok(TaskCompletion::Succeeded(outputs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn four_asset_news_expands_only_to_frozen_single_asset_resources() {
        let frozen = Asset::EXECUTABLE
            .iter()
            .map(|a| EvidenceNeed {
                schema_version: DOMAIN_SCHEMA_VERSION,
                source_family: "news_web".into(),
                resource: format!("news:{}:2026-09-08:2026-09-22:market", a.symbol()),
                max_age_secs: 86400,
            })
            .collect::<Vec<_>>();
        let intent = SupplementalIntent {
            kind: SupplementalKind::News,
            assets: Asset::EXECUTABLE.to_vec(),
            series: vec![],
            query: "verify news".into(),
        };
        assert_eq!(expand_intent(&intent, &frozen).unwrap(), frozen);
        assert!(expand_intent(&intent, &frozen[..3]).is_err());
    }
    #[test]
    fn repeated_retrieval_is_not_a_new_fact() {
        let a = json!({"resource":"bars:QQQ:x","value":{"bars":[{"close":123}],"retrieved_at":"a","provider_result":{"id":"1"}}});
        let mut b = a.clone();
        b["value"]["retrieved_at"] = json!("b");
        b["value"]["provider_result"]["id"] = json!("2");
        assert_eq!(fact_value(&a), fact_value(&b));
        b["value"]["bars"][0]["close"] = json!(124);
        assert_ne!(fact_value(&a), fact_value(&b));
    }
}
