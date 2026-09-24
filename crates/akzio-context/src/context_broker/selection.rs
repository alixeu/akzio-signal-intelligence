// 文件职责：把候选 Artifact 按 ContextPolicy、信任边界、来源/运行闭包和预算收敛为 ContextManifest selection。
// 本文件负责 selection 及 child manifest 的授权收缩；它不让模型自行选择文档，也不把 projection 当成新的原始证据。
// 所有外部读取和 Store 写入都沿用 ContextResult；Option 只表示候选或字段不存在，不把未知状态当作允许。
fn financial_content_indicators(value: &Value) -> Option<Vec<String>> {
    // 输入：证据 JSON 的借用；输出：阻断交易的金融内容指标列表，或 None 表示该内容不触发策略。
    // assessment 的 Option 链保持“未提供 financial_content”与“解析失败”在本层均为无指标，不创建虚假 quarantine 原因。
    let assessment = financial_content_assessment(value)?;
    if !assessment.blocks_trading(&FinancialContentPolicy::default()) {
        return None;
    }
    let mut indicators = assessment
        .indicators
        .iter()
        .map(|indicator| format!("financial_{indicator:?}").to_ascii_lowercase())
        .collect::<Vec<_>>();
    if indicators.is_empty() {
        indicators.push(
            format!(
                "financial_information_{:?}",
                assessment.information_classification
            )
            .to_ascii_lowercase(),
        );
    }
    indicators.sort();
    indicators.dedup();
    Some(indicators)
}

fn financial_content_assessment(value: &Value) -> Option<FinancialContentAssessment> {
    // 输入：证据 JSON 的借用；输出：从 financial_content 解码出的 typed assessment，字段不存在或解码失败时为 None。
    // clone 只复制待反序列化的 Value，不把原始 Artifact 的所有权交给解析器。
    let content = value.get("financial_content")?;
    serde_json::from_value(content.clone()).ok()
}

impl ContextBroker {
    /// Measure what the model receives separately from the original CAS
    /// document. Full source bytes remain bounded by max_source_bytes and
    /// range-read authorization; compact projection bytes consume max_bytes.
    fn projection_budget(&self, artifact: &Artifact) -> ContextResult<(u64, u32)> {
        // 输入：Artifact 的借用；输出：projection 字节数和估算 token 数，底层读取/序列化失败经 Result 返回。
        // source blob bytes 与模型收到的 compact projection bytes 分开计算，调用方随后分别比较 source/Context 上限。
        let value = self.document_value(artifact)?;
        let projection = compact_governed_projection(artifact.kind, value);
        let bytes = u64::try_from(serde_json::to_vec(&projection)?.len())
            .map_err(|_| ContextError::BudgetExceeded)?;
        Ok((bytes, estimate_tokens_from_bytes(bytes)))
    }

    fn source_budget(policy: &ContextPolicy) -> u64 {
        // 输入：ContextPolicy 的借用；输出：源 Artifact 字节上限；显式 max_source_bytes 缺失时回退到 max_bytes。
        policy.max_source_bytes.unwrap_or(policy.max_bytes)
    }

    fn partition_untrusted_context(
        &self,
        artifacts: Vec<Artifact>,
    ) -> ContextResult<(Vec<Artifact>, Vec<ContextQuarantine>)> {
        // 输入：拥有所有权的候选 Artifact 列表；输出：允许列表与 quarantine 列表，读取 blob/JSON 失败时返回 Err。
        // 信任类型决定是否检查 instruction/financial indicators；被隔离项只保留 ArtifactRef、原因和指标，不进入 allowed。
        let mut allowed = Vec::with_capacity(artifacts.len());
        let mut quarantined = Vec::new();
        for artifact in artifacts {
            // 循环取得每个 Artifact 的所有权；允许项移动进 allowed，隔离项则只移动其 ID/kind 到 quarantine。
            if context_trust(artifact.kind) == ContextTrust::UntrustedEvidence {
                let bytes = self.store.read_blob(&artifact.blob)?;
                let (reason, indicators) = match serde_json::from_slice::<Value>(&bytes) {
                    // JSON 能解码时先查指令样内容，再查金融内容；两个 Option/Vec 分支都只描述隔离理由。
                    Ok(value) => {
                        let instruction = instruction_indicators(&value);
                        if !instruction.is_empty() {
                            (ContextQuarantineReason::InstructionLikeContent, instruction)
                        } else if let Some(indicators) = financial_content_indicators(&value) {
                            (ContextQuarantineReason::FinancialContentRisk, indicators)
                        } else {
                            (ContextQuarantineReason::InstructionLikeContent, Vec::new())
                        }
                    }
                    Err(_) => (
                        ContextQuarantineReason::InstructionLikeContent,
                        instruction_indicators(&Value::String(
                            String::from_utf8_lossy(&bytes).into_owned(),
                        )),
                    ),
                };
                if !indicators.is_empty() {
                    quarantined.push(ContextQuarantine {
                        artifact: ArtifactRef {
                            artifact_id: artifact.artifact_id,
                            kind: artifact.kind,
                        },
                        reason,
                        indicators,
                    });
                    continue;
                }
            }
            allowed.push(artifact);
        }
        Ok((allowed, quarantined))
    }

    fn learning_query_scope(
        &self,
        permit: &TaskWritePermit,
        policy: &ContextPolicy,
        query: &ContextQueryScope,
        references: &[ArtifactRef],
    ) -> ContextResult<ContextQueryScope> {
        // 输入：permit/policy/query/references 的借用；输出：从 query clone 得到的受控 ContextQueryScope。
        // 先清空调用方标签，再仅从已授权、同 Run、DecisionTime 的 typed snapshot 重建 regimes；任一 Store/校验失败经 Result 返回。
        let mut scope = query.clone();
        // Regime labels are derived only from authorized typed snapshots. No
        // evidence text, arbitrary tag, or caller-supplied label is authoritative.
        scope.regimes.clear();
        for reference in references {
            // 引用切片只被借用；kind/source/policy 过滤在读取 snapshot 前完成，避免未授权引用参与范围推导。
            if reference.kind != ArtifactKind::RegimeSnapshot
                || !policy.permitted_kinds.contains(&ArtifactKind::RegimeSnapshot)
            {
                continue;
            }
            let artifact = self.store.artifact(&reference.artifact_id)?;
            if artifact.kind != ArtifactKind::RegimeSnapshot
                || !governed_internal_source(&artifact)
                || (!policy.permitted_source_families.is_empty()
                    && !policy.permitted_source_families.contains(&artifact.provenance.source_family))
            {
                continue;
            }
            self.assert_context_permitted(policy, &artifact)?;
            self.assert_context_run(permit, &artifact)?;
            let snapshot: RegimeSnapshot = self.read_payload(&artifact)?;
            snapshot.validate()?;
            if snapshot.classification_kind == RegimeClassificationKind::DecisionTime {
                // 只有通过 typed validation 的 DecisionTime 快照才把 canonical labels 追加到拥有的 scope。
                scope.regimes.extend(snapshot.canonical_regime_labels());
            }
        }
        Ok(scope)
    }

    // 输入：父/子 permit 与 Contract、父 Manifest、显式 ContextProjection、当前时间和 grant TTL；输出：收缩后的子 ContextManifest。
    // 该函数以 ContextResult 返回；任何 lineage、授权、解析、预算或 Store 写入失败都提前返回 Err，不用空集合掩盖缺口。
    /// Attenuate a persisted parent manifest into a child attempt grant.
    /// Projection may include parent outputs, but only from the current
    /// succeeded attempt and only when their provenance closes to the parent.
    #[allow(clippy::too_many_arguments)]
    pub fn assemble_child(
        &self,
        parent_permit: &TaskWritePermit,
        parent_contract: &AgentContract,
        parent: &ContextManifest,
        projection: &ContextProjection,
        child_permit: &TaskWritePermit,
        child_contract: &AgentContract,
        now: DateTime<Utc>,
        grant_ttl: Duration,
    ) -> ContextResult<ContextManifest> {
        // 先校验 child Contract/permit 与 parent succeeded Attempt 的身份，再处理 projection 中允许的引用。
        // 父 Manifest 只能作为权限上界；子 policy、信任隔离和 source/projected/token 三套预算会再次收缩结果。
        projection.validate()?;
        child_contract.validate()?;
        if child_permit.contract_hash.as_ref() != Some(&child_contract.contract_hash) {
            return Err(ContextError::InvalidManifestClosure);
        }
        if child_permit.run_id != parent_permit.run_id {
            return Err(ContextError::InvalidManifestClosure);
        }
        let succeeded = self
            .store
            .current_succeeded_attempt(&parent_permit.run_id, &parent_permit.task_id)?;
        if succeeded.attempt_id != parent_permit.attempt_id
            || succeeded.lease_id != parent_permit.lease_id
            || succeeded.epoch != parent_permit.epoch
            || succeeded.contract_hash != parent_permit.contract_hash
        {
            return Err(ContextError::InvalidManifestClosure);
        }
        if projection.parent_manifest.artifact_id != parent.artifact.artifact_id
            || projection.parent_manifest.kind != ArtifactKind::ContextManifest
        {
            return Err(ContextError::InvalidManifestClosure);
        }

        // Reuse the canonical persisted-manifest validation before projecting.
        self.policy_influences_internal(parent_permit, parent_contract, parent, now, false)?;

        let parent_readable = parent
            .payload
            .selections
            .iter()
            .map(|selection| selection.artifact.clone())
            .collect::<BTreeSet<_>>();
        let parent_readable_ids = parent_readable
            .iter()
            .map(|reference| reference.artifact_id.clone())
            .collect::<BTreeSet<_>>();
        if parent.grant.readable != parent_readable_ids {
            return Err(ContextError::InvalidManifestClosure);
        }
        let parent_raw_closure =
            self.raw_closure(&parent_contract.context, &parent.payload.selections)?;
        // 只有 projection 请求了父 Manifest 之外的引用时才读取 succeeded outputs；无此需求保持空 Vec，减少越权面。
        let needs_parent_outputs = projection
            .allowed
            .iter()
            .any(|reference| !parent_readable.contains(reference));
        let parent_outputs = if needs_parent_outputs {
            let mut outputs = succeeded.outputs.clone();
            let deliberation_sources = succeeded
                .outputs
                .iter()
                .flat_map(|output| output.source_refs.iter())
                .filter(|source| is_safe_deliberation_summary(source.kind))
                .cloned()
                .collect::<BTreeSet<_>>();
            for source in deliberation_sources {
                // source_refs 是已提交输出携带的借用关系；逐个回 Store 验证 kind 后才进入候选输出集合。
                let artifact = self.store.artifact(&source.artifact_id)?;
                if artifact.kind != source.kind {
                    return Err(ContextError::InvalidManifestClosure);
                }
                outputs.push(artifact);
            }
            outputs
        } else {
            Vec::new()
        };
        let mut allowed = Vec::with_capacity(projection.allowed.len());
        for reference in &projection.allowed {
            // projection 引用只被借用；已有 parent_readable 可直接复用，其他引用必须在 parent_outputs 中精确匹配并验证 provenance。
            if is_trace_kind(reference.kind) {
                return Err(ContextError::GrantDenied {
                    manifest_id: parent.artifact.artifact_id.clone(),
                    artifact_id: reference.artifact_id.clone(),
                });
            }
            if parent_readable.contains(reference) {
                allowed.push(self.store.artifact(&reference.artifact_id)?);
                continue;
            }
            let Some(output) = parent_outputs.iter().find(|artifact| {
                // `find` 闭包只按 ID+kind 做精确候选定位，真正的父闭包校验随后由 validate_parent_output_provenance 完成。
                artifact.artifact_id == reference.artifact_id && artifact.kind == reference.kind
            }) else {
                return Err(ContextError::GrantDenied {
                    manifest_id: parent.artifact.artifact_id.clone(),
                    artifact_id: reference.artifact_id.clone(),
                });
            };
            self.validate_parent_output_provenance(
                output,
                &projection.parent_manifest,
                &parent_readable,
                &parent_raw_closure,
                parent_permit,
                parent_contract,
            )?;
            allowed.push(output.clone());
        }
        let (mut allowed, quarantined) = self.partition_untrusted_context(allowed)?;
        // quarantine 已经从 allowed 移出；下面只为仍获准的 Claim/Critique 建立直接 evidence 引用集合，用于优先级排序。
        let mut directly_referenced = BTreeSet::new();
        for artifact in &allowed {
            // 这里只读取 typed payload 并借用 allowed；解析/领域校验失败通过 `?` 阻断，而非把无效 Claim 当作普通背景。
            match artifact.kind {
                ArtifactKind::Claim => {
                    let claim: ResearchClaim = self.read_payload(artifact)?;
                    claim.validate()?;
                    directly_referenced.extend(claim.source_refs());
                }
                ArtifactKind::Critique => {
                    let critique: ResearchCritique = self.read_payload(artifact)?;
                    critique.validate()?;
                    directly_referenced.extend(critique.source_refs());
                }
                _ => {}
            }
        }
        allowed.sort_by(|left, right| {
            // 排序闭包只比较 purpose rank、是否被 Claim/Critique 直接引用和 artifact_id；不改变 Artifact 内容或授权集合。
            purpose_rank(child_contract.purpose.as_str(), left)
                .cmp(&purpose_rank(child_contract.purpose.as_str(), right))
                .then_with(|| {
                    let left_reference = ArtifactRef {
                        artifact_id: left.artifact_id.clone(),
                        kind: left.kind,
                    };
                    let right_reference = ArtifactRef {
                        artifact_id: right.artifact_id.clone(),
                        kind: right.kind,
                    };
                    (!directly_referenced.contains(&left_reference))
                        .cmp(&(!directly_referenced.contains(&right_reference)))
                })
                .then_with(|| left.artifact_id.cmp(&right.artifact_id))
        });
        let policy = &child_contract.context;
        let mut selections = Vec::with_capacity(allowed.len());
        let mut total_bytes = 0_u64;
        let mut projected_bytes = 0_u64;
        let mut estimated_tokens = 0_u32;
        for artifact in allowed {
            // 每轮先核对 kind/source/run/overlay eligibility，再以 next_* 临时值做预算试算；超限项 continue，不挤占已接受项。
            let reference = ArtifactRef {
                artifact_id: artifact.artifact_id.clone(),
                kind: artifact.kind,
            };
            self.assert_context_permitted(policy, &artifact)?;
            self.assert_context_run(child_permit, &artifact)?;
            if !self.overlay_is_eligible(&artifact)? {
                continue;
            }
            let (projected, tokens) = self.projection_budget(&artifact)?;
            // source bytes 衡量原始 CAS，projected bytes 衡量模型视图，tokens 是估算值；三者同时受 child policy 约束。
            let next_source_bytes = total_bytes.saturating_add(artifact.blob.bytes);
            let next_projected_bytes = projected_bytes.saturating_add(projected);
            let next_tokens = estimated_tokens.saturating_add(tokens);
            if selections.len() >= usize::from(policy.max_artifacts)
                || next_source_bytes > Self::source_budget(policy)
                || next_projected_bytes > policy.max_bytes
                || next_tokens > policy.max_tokens
            {
                // 预算或数量不足时跳过当前候选；已有 selections 仍保持其原顺序和累计额度。
                continue;
            }
            total_bytes = next_source_bytes;
            projected_bytes = next_projected_bytes;
            estimated_tokens = next_tokens;
            selections.push(ContextSelection {
                artifact: reference,
                reason: if artifact.producer == "evidence.option_projection" {
                    "option_chain_projection".to_owned()
                } else {
                    projection.reason.clone()
                },
                estimated_tokens: tokens,
                projected_bytes: Some(projected),
                trust: context_trust(artifact.kind),
            });
        }
        if selections.len() < usize::from(policy.min_artifacts)
            || (child_contract.purpose.as_str() == RESEARCH_CRITIC_RECIPE_ID
                && (!selections
                    .iter()
                    .any(|selection| selection.artifact.kind == ArtifactKind::Claim)
                    || !selections.iter().any(|selection| {
                        selection.artifact.kind == ArtifactKind::NormalizedEvidence
                    })))
        {
            // min_artifacts 与 Critic 必需的 Claim+NormalizedEvidence 任一不满足，都以 BudgetExceeded fail closed。
            return Err(ContextError::BudgetExceeded);
        }

        // raw_source_closure 必须是 parent 闭包的子集；payload/input_hash 随 selections 固定后才创建新的 Manifest Artifact。
        let raw_source_closure = self.raw_closure(policy, &selections)?;
        if !raw_source_closure.is_subset(&parent_raw_closure) {
            return Err(ContextError::InvalidManifestClosure);
        }
        let payload = ContextManifestPayload {
            schema_version: DOMAIN_SCHEMA_VERSION,
            contract_hash: child_contract.contract_hash.clone(),
            input_hash: manifest_input_hash(&selections)?,
            selections: selections.clone(),
            quarantined: quarantined.clone(),
            total_bytes,
            projected_bytes: Some(projected_bytes),
            estimated_tokens,
        };
        payload.validate(policy)?;
        let artifact = Artifact::new(
            ArtifactKind::ContextManifest,
            self.store.stage_json(&payload)?,
            format!("context.{}", child_contract.purpose.as_str()),
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.context".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: Some(child_contract.contract_hash.clone()),
            },
            Some(ArtifactOrigin {
                run_id: Some(child_permit.run_id.clone()),
                task_id: Some(child_permit.task_id.clone()),
                attempt_id: Some(child_permit.attempt_id.clone()),
                contract_hash: child_permit.contract_hash.clone(),
            }),
            std::iter::once(projection.parent_manifest.clone())
                .chain(
                    selections
                        .iter()
                        .map(|selection| selection.artifact.clone()),
                )
                .chain(
                    quarantined
                        .iter()
                        .map(|quarantine| quarantine.artifact.clone()),
                )
                .collect(),
            now,
        )?;
        self.store.write_task_artifact(
            child_permit,
            &artifact,
            LifecycleEventType::ContextChildManifestCreated,
            now,
        )?;
        // ReadGrant 只列出已接受 selections 的 IDs，并绑定 child permit/epoch/Contract；quarantined 项不获得可读权限。
        let grant = ReadGrant {
            manifest_artifact_id: artifact.artifact_id.clone(),
            run_id: child_permit.run_id.clone(),
            task_id: child_permit.task_id.clone(),
            attempt_id: child_permit.attempt_id.clone(),
            lease_id: child_permit.lease_id.clone(),
            epoch: child_permit.epoch,
            contract_hash: child_contract.contract_hash.clone(),
            readable: selections
                .iter()
                .map(|selection| selection.artifact.artifact_id.clone())
                .collect(),
            raw_source_closure,
            expires_at: now + grant_ttl,
        };
        Ok(ContextManifest {
            artifact,
            payload,
            grant,
        })
    }

    // 输入：只读 succeeded proof、父 Contract、子 permit/Contract、当前时间和 grant TTL；输出：由 proof 恢复并收缩出的子 Manifest。
    // proof 路径不重新激活父写权限；Store 当前成功 Attempt、Manifest provenance 和 child policy 共同决定可见集合。
    /// Project the current succeeded parent attempt without reviving its
    /// write permit. The proof is read-only Store state; the synthetic permit
    /// exists only inside this validation path.
    pub fn assemble_child_from_proof(
        &self,
        proof: &SucceededAttemptProof,
        parent_contract: &AgentContract,
        child_permit: &TaskWritePermit,
        child_contract: &AgentContract,
        now: DateTime<Utc>,
        grant_ttl: Duration,
    ) -> ContextResult<ContextManifest> {
        // 先比较 Store 当前 succeeded Attempt 与 proof 的完整身份，任何不一致都以 InvalidManifestClosure 结束。
        let current = self
            .store
            .current_succeeded_attempt(&proof.run_id, &proof.task_id)?;
        if &current != proof {
            return Err(ContextError::InvalidManifestClosure);
        }
        let manifest_ref = proof
            .context_manifest
            .clone()
            .ok_or(ContextError::InvalidManifestClosure)?;
        let artifact = self.store.artifact(&manifest_ref.artifact_id)?;
        if artifact.kind != ArtifactKind::ContextManifest {
            return Err(ContextError::InvalidManifestClosure);
        }
        let payload: ContextManifestPayload = self.read_payload(&artifact)?;
        // projection 由 child Contract 派生；父 payload 的每项只在 kind 与 source policy 都允许时追加。
        // Parent manifest proves provenance; committed outputs are the child data surface
        // only after Rust applies the child's policy-owned projection.
        let mut projection = derive_child_projection(proof, manifest_ref, child_contract);
        for selection in &payload.selections {
            // 选择记录只被借用；这里先做轻量 policy 过滤，assemble_child 随后仍会执行完整 grant/closure/budget 核验。
            let artifact = self.store.artifact(&selection.artifact.artifact_id)?;
            let kind_allowed = child_contract
                .context
                .permitted_kinds
                .contains(&artifact.kind);
            let source_allowed = child_contract.context.permitted_source_families.is_empty()
                || child_contract
                    .context
                    .permitted_source_families
                    .contains(&artifact.provenance.source_family);
            if kind_allowed && source_allowed {
                projection.allowed.push(selection.artifact.clone());
            }
        }
        // sort/dedup 只规范引用集合，不生成新的 Artifact；后续 parent_permit 是验证用的内部快照，不会复活父 lease。
        projection.allowed.sort();
        projection.allowed.dedup();
        let parent_permit = TaskWritePermit {
            run_id: proof.run_id.clone(),
            task_id: proof.task_id.clone(),
            attempt_id: proof.attempt_id.clone(),
            lease_id: proof.lease_id.clone(),
            epoch: proof.epoch,
            contract_hash: proof.contract_hash.clone(),
        };
        let parent =
            self.restore_manifest_for_proof(proof, parent_contract, artifact, payload, now)?;
        self.assemble_child(
            &parent_permit,
            parent_contract,
            &parent,
            &projection,
            child_permit,
            child_contract,
            now,
            grant_ttl,
        )
    }
}

impl ContextBroker {
    // 该 impl 的私有选择器只准备 Analyst 的平衡候选包；正式 Manifest/ReadGrant 仍由上层授权路径创建。
    fn select_analyst_bundle(
        &self,
        artifacts: &[Artifact],
        policy: &ContextPolicy,
    ) -> ContextResult<Option<Vec<Artifact>>> {
        // 输入：候选 Artifact 切片和 policy 的借用；输出：成功时 Some(拥有的候选 Vec)，无候选时 None，读取/投影失败时 Err。
        // 选择顺序先按 domain/scope 建 key，再在每组内按 confidence、大小和 ID 稳定排序，并逐组尝试预算。
        // Reserve a balanced core before ranking optional documents. A missing
        // domain remains a scoped coverage gap; it does not erase other assets.
        let mut by_key = std::collections::BTreeMap::<String, Vec<Artifact>>::new();
        for artifact in artifacts {
            if artifact.kind == ArtifactKind::SemanticDetail
                && matches!(artifact.producer.as_str(), "evidence.collection_status" | "canary.evidence_snapshot")
            {
                by_key
                    .entry("0:coverage".to_owned())
                    .or_default()
                    .push(artifact.clone());
            }
            let option_projection = artifact.kind == ArtifactKind::SemanticDetail
                && artifact.producer == "evidence.option_projection";
            if artifact.kind != ArtifactKind::NormalizedEvidence && !option_projection {
                continue;
            }
            let payload: Value = serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
            let Some(resource) = payload.get("resource").and_then(Value::as_str) else {
                continue;
            };
            let mut parts = resource.split(':');
            let domain = parts.next().unwrap_or_default();
            let scope = parts.next().unwrap_or_default();
            let key = match domain {
                "bars" | "news" if Asset::try_from(scope).is_ok() => {
                    format!("1:{scope}:{domain}")
                }
                "series" if matches!(scope, "DFF" | "DFII10" | "VIXCLS") => format!("2:{scope}"),
                "research" if scope == "earnings_event_calendar" => format!("3:{}", parts.next().unwrap_or_default()),
                "option_chain" if Asset::try_from(scope).is_ok() => format!("4:{scope}"),
                _ => continue,
            };
            by_key.entry(key).or_default().push(artifact.clone());
        }
        let mut selected = Vec::new();
        let mut source_bytes = 0_u64;
        let mut projected_bytes = 0_u64;
        let mut tokens = 0_u32;
        for candidates in by_key.values_mut() {
            // values_mut 借用每个分组；组内排序不会修改 Artifact，只决定稳定的候选优先级。
            candidates.sort_by_key(|a| {
                (
                    std::cmp::Reverse(a.provenance.confidence_ppm),
                    a.blob.bytes,
                    a.artifact_id.clone(),
                )
            });
            if let Some(artifact) = candidates.iter().find(|a| {
                // `find` 闭包用 projection_budget 计算试算值；错误在此处变成保守的 MAX，使该候选无法突破预算。
                let (projected, estimated) = self.projection_budget(a).unwrap_or((u64::MAX, u32::MAX));
                source_bytes.saturating_add(a.blob.bytes) <= Self::source_budget(policy)
                    && projected_bytes.saturating_add(projected) <= policy.max_bytes
                    && tokens.saturating_add(estimated) <= policy.max_tokens
                    && selected.len() < usize::from(policy.max_artifacts)
            }) {
                // Some 表示本组找到预算内候选；None 只跳过该组，不把缺少一个 domain 误报为全局选择成功。
                let (projected, estimated) = self.projection_budget(artifact)?;
                source_bytes += artifact.blob.bytes;
                projected_bytes += projected;
                tokens += estimated;
                selected.push(artifact.clone());
            }
        }
        Ok((!selected.is_empty()).then_some(selected))
    }
}

#[cfg(test)]
mod event_selection_tests {
    // 测试职责：验证 Analyst bundle 在紧预算下先保留事件，再处理 option 候选，并维持价格/宏观覆盖。
    use super::*;
    // 测试无参数并以断言为输出；候选和 Store 都是测试专用数据，不代表真实 Paper 或运行时授权。
    #[test]
    fn tight_budget_reserves_events_before_options_after_price_and_macro() {
        let root=std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/event-selection-tests").join(akzio_domain::RunId::new().0);
        let store=Store::open(root).unwrap();
        let now=Utc::now();
        let resources=["option_chain:QQQ:2026-09-22:2026-10-22",
            "research:earnings_event_calendar:QQQ:2026-09-22","series:DFF",
            "bars:QQQ:2026-01-01:2026-09-22:1Day"];
        let artifacts=resources.iter().map(|resource| {
            let option=resource.starts_with("option_chain:");
            Artifact::new(if option {ArtifactKind::SemanticDetail} else {ArtifactKind::NormalizedEvidence},
                store.stage_json(&serde_json::json!({"resource":resource,"value":{}})).unwrap(),
                if option {"evidence.option_projection"} else {"evidence.normalize"},
                akzio_domain::ArtifactLifecycle::RunScoped,
                akzio_domain::ArtifactProvenance {source_family:"test".into(), observed_at:Some(now),retrieved_at:now,
                    source_uri:None,confidence_ppm:1_000_000,producer_contract_hash:None},None,vec![],now).unwrap()
        }).collect::<Vec<_>>();
        let broker=ContextBroker::new(store);
        let policy=ContextPolicy {permitted_kinds:Default::default(),permitted_source_families:Default::default(),
            min_artifacts:0,max_artifacts:3,max_bytes:131072,max_source_bytes:None,max_tokens:32000,allow_raw_reread:false};
        let selected=broker.select_analyst_bundle(&artifacts,&policy).unwrap().unwrap();
        assert_eq!(selected.len(),3);
        assert!(!selected.iter().any(|a|a.artifact_id==artifacts[0].artifact_id));
        assert!(selected.iter().any(|a|a.artifact_id==artifacts[1].artifact_id));
    }
}
