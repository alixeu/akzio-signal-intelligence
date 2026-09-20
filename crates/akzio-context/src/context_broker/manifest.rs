impl ContextBroker {
    // 按角色从已经取到的候选 Artifact 中计算不可缺少的输入闭包。这个函数只做
    // Context 完整性校验：它不会抓取新数据，也不会把提案或 Review 视为 Decision。
    fn required_role_inputs(
        &self,
        contract: &AgentContract,
        artifacts: &[Artifact],
    ) -> ContextResult<BTreeSet<ArtifactId>> {
        let purpose = contract.purpose.as_str();
        let missing = |requirement: String| ContextError::MissingRequiredInput {
            purpose: purpose.to_owned(),
            requirement,
        };
        let mut required = BTreeSet::new();
        if purpose == akzio_domain::LEARNING_OUTCOME_WORKER_RECIPE_ID {
            // Outcome Worker 需要 Decision/Execution 上下文、OutcomeSchedule 及阶段包，
            // 并额外追踪 DecisionContext 中声明的 Claim/Critique 引用。
            for kind in [
                ArtifactKind::Decision,
                ArtifactKind::DecisionContext,
                ArtifactKind::ExecutionContext,
                ArtifactKind::OutcomeSchedule,
            ] {
                let artifact = artifacts
                    .iter()
                    .find(|a| a.kind == kind)
                    .ok_or_else(|| missing(format!("{kind:?}")))?;
                required.insert(artifact.artifact_id.clone());
            }
            let stage = artifacts
                .iter()
                .find(|a| a.producer == "learning.outcome_stage" || a.kind == ArtifactKind::Outcome)
                .ok_or_else(|| missing("Rust stage packet or sealed Outcome".to_owned()))?;
            required.insert(stage.artifact_id.clone());
            let context = artifacts
                .iter()
                .find(|a| a.kind == ArtifactKind::DecisionContext)
                .expect("required above");
            let value: Value = self.read_payload(context)?;
            // DecisionContext 的引用必须同时存在于当前候选集合，避免只凭 JSON 中的
            // ArtifactRef 把未进入本次 Context 的材料变成隐含输入。
            for key in ["claims", "critiques"] {
                let refs: Vec<ArtifactRef> = serde_json::from_value(
                    value
                        .get(key)
                        .cloned()
                        .ok_or_else(|| missing(format!("DecisionContext.{key}")))?,
                )?;
                for reference in refs {
                    if !artifacts
                        .iter()
                        .any(|a| a.artifact_id == reference.artifact_id && a.kind == reference.kind)
                    {
                        return Err(missing(format!(
                            "DecisionContext.{key}: {}",
                            reference.artifact_id
                        )));
                    }
                    required.insert(reference.artifact_id);
                }
            }
            required.extend(
                artifacts
                    .iter()
                    .filter(|a| a.kind == ArtifactKind::Retrospective)
                    .map(|a| a.artifact_id.clone()),
            );
        } else if matches!(
            purpose,
            RESEARCH_CRITIC_RECIPE_ID | RESEARCH_SYNTHESIZER_RECIPE_ID | akzio_domain::RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID
        ) {
            // 研究角色先纳入 Claim/Critique，再验证它们的 source_refs；Claim 的依据
            // 必须直接进入必需集合，Critique 的嵌套依据在新 Contract 下也必须闭合。
            for artifact in artifacts
                .iter()
                .filter(|a| matches!(a.kind, ArtifactKind::Claim | ArtifactKind::Critique))
            {
                required.insert(artifact.artifact_id.clone());
                let refs = if artifact.kind == ArtifactKind::Claim {
                    self.read_payload::<ResearchClaim>(artifact)?.source_refs()
                } else {
                    self.read_payload::<ResearchCritique>(artifact)?
                        .source_refs()
                };
                for reference in refs {
                    if !artifacts
                        .iter()
                        .any(|a| a.artifact_id == reference.artifact_id && a.kind == reference.kind)
                    {
                        return Err(missing(format!(
                            "research source {}",
                            reference.artifact_id
                        )));
                    }
                    // Claim grounds are needed to validate the claim itself.
                    // Critique grounds remain durable source closure, but
                    // their compact evidence projections are optional model
                    // inputs so Synthesizer context cannot be exhausted by
                    // repeating every nested semantic detail.
                    if artifact.kind == ArtifactKind::Claim || contract.version >= akzio_domain::REVIEWED_RESEARCH_CONTRACT_VERSION {
                        required.insert(reference.artifact_id);
                    }
                }
            }
        }
        if contract.version >= akzio_domain::REVIEWED_RESEARCH_CONTRACT_VERSION {
            // Reviewed research 还必须携带最终提案、ProposalReview、补采结果及提案
            // 的 numeric/evidence basis；ProposalReviewer 只能面对一个 final proposal。
            required.extend(artifacts.iter().filter(|a| matches!(a.kind, ArtifactKind::DecisionProposal | ArtifactKind::ProposalReview) || a.producer == "research.supplement.result").map(|a| a.artifact_id.clone()));
            for artifact in artifacts.iter().filter(|a| a.kind == ArtifactKind::DecisionProposal) {
                let proposal: akzio_domain::DecisionDraft = self.read_payload(artifact)?;
                for reference in proposal.claims.iter().chain(&proposal.critiques)
                    .chain(proposal.numeric_basis.iter().flat_map(|b| &b.inputs))
                    .chain(proposal.research_allocation.iter().flat_map(|p| &p.allocations).flat_map(|a| &a.evidence_refs)) {
                    if !artifacts.iter().any(|a| a.artifact_id == reference.artifact_id && a.kind == reference.kind) {
                        return Err(missing(format!("proposal basis {}", reference.artifact_id)));
                    }
                    required.insert(reference.artifact_id.clone());
                }
            }
            if purpose == akzio_domain::RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID && artifacts.iter().filter(|a| a.kind == ArtifactKind::DecisionProposal).count() != 1 {
                return Err(missing("exactly one final proposal".into()));
            }
        }
        Ok(required)
    }
    fn extend_synthesizer_critique_targets(
        &self,
        permit: &TaskWritePermit,
        candidates: &mut Vec<ArtifactRef>,
    ) -> ContextResult<()> {
        // Synthesizer 的候选中若有来自 ContextManifest 的 Critique，则把其被审查的
        // Claim 一并加入候选；加入前验证同 Run、同 Contract 和源 Manifest 的选择闭包，
        // 不允许借 Critique 引用扩大到未授权材料。
        let critiques = candidates
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::Critique)
            .cloned()
            .collect::<Vec<_>>();
        for reference in critiques {
            let artifact = self.store.artifact(&reference.artifact_id)?;
            if artifact.kind != ArtifactKind::Critique {
                return Err(ContextError::InvalidManifestClosure);
            }
            if !artifact
                .source_refs
                .iter()
                .any(|source| source.kind == ArtifactKind::ContextManifest)
            {
                continue;
            }
            let critique: ResearchCritique = self.read_payload(&artifact)?;
            critique.validate()?;
            if !artifact.source_refs.contains(&critique.target) {
                return Err(ContextError::InvalidManifestClosure);
            }
            let origin = artifact
                .origin
                .as_ref()
                .ok_or(ContextError::InvalidManifestClosure)?;
            if origin.run_id.as_ref() != Some(&permit.run_id)
                || origin.contract_hash.as_ref()
                    != artifact.provenance.producer_contract_hash.as_ref()
            {
                return Err(ContextError::InvalidManifestClosure);
            }
            let target = self.store.artifact(&critique.target.artifact_id)?;
            if target.kind != ArtifactKind::Claim
                || target
                    .origin
                    .as_ref()
                    .and_then(|target_origin| target_origin.run_id.as_ref())
                    != Some(&permit.run_id)
            {
                return Err(ContextError::InvalidManifestClosure);
            }
            let readable_from_source_manifest = artifact
                .source_refs
                .iter()
                .filter(|source| source.kind == ArtifactKind::ContextManifest)
                .any(|source| {
                    let Ok(manifest) = self.store.artifact(&source.artifact_id) else {
                        return false;
                    };
                    let Some(manifest_origin) = manifest.origin.as_ref() else {
                        return false;
                    };
                    if manifest.kind != ArtifactKind::ContextManifest
                        || manifest_origin.run_id != origin.run_id
                        || manifest_origin.task_id != origin.task_id
                        || manifest_origin.attempt_id != origin.attempt_id
                        || manifest_origin.contract_hash != origin.contract_hash
                        || manifest.provenance.producer_contract_hash
                            != artifact.provenance.producer_contract_hash
                    {
                        return false;
                    }
                    self.read_payload::<ContextManifestPayload>(&manifest)
                        .is_ok_and(|payload| {
                            payload
                                .selections
                                .iter()
                                .any(|selection| selection.artifact == critique.target)
                        })
                });
            if !readable_from_source_manifest {
                return Err(ContextError::InvalidManifestClosure);
            }
            candidates.push(critique.target);
        }
        Ok(())
    }

    // ContextBroker 只持有 Store 句柄；构造本身不打开数据库、不创建 Manifest，也不
    // 产生任何研究或执行状态。
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    /// Reconstructs durable learning influences only from the exact persisted
    /// manifest closure. Current policy heads are rechecked at use time.
    pub fn policy_influences(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        manifest: &ContextManifest,
        now: DateTime<Utc>,
    ) -> ContextResult<Vec<ArtifactRef>> {
        // 返回 Manifest 闭包中实际影响当前 Context 的 Experience/CandidatePolicy 引用；
        // 当前 Policy head 会在使用时重验，返回该列表不等于激活或修改 Policy。
        self.policy_influences_internal(permit, contract, manifest, now, true)
    }

    fn validate_manifest_closure(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        manifest: &ContextManifest,
        now: DateTime<Utc>,
        require_live_grant: bool,
    ) -> ContextResult<Vec<ArtifactRef>> {
        // 重新核对内存 Manifest 与 Store 中的 Artifact/payload、Contract、来源、预算和
        // grant 闭包。require_live_grant=false 仅供成功父 Attempt 的历史证明路径，仍不
        // 放宽 persisted Manifest 的身份与内容校验。
        contract.validate()?;
        if !manifest.grant.matches_permit(permit)
            || manifest.grant.contract_hash != contract.contract_hash
            || manifest.payload.contract_hash != contract.contract_hash
            || (require_live_grant && manifest.grant.expires_at <= now)
        {
            return Err(ContextError::InvalidManifestClosure);
        }

        if require_live_grant {
            // A grant's TTL does not outlive the attempt authority which minted
            // it. Historical succeeded-parent proofs use the separate path.
            self.store.validate_task_permit(permit)?;
        }

        let persisted = self.store.artifact(&manifest.grant.manifest_artifact_id)?;
        persisted.validate()?;
        let expected_producer = format!("context.{}", contract.purpose.as_str());
        let Some(origin) = persisted.origin.as_ref() else {
            return Err(ContextError::InvalidManifestClosure);
        };
        if persisted != manifest.artifact
            || persisted.kind != ArtifactKind::ContextManifest
            || persisted.lifecycle != ArtifactLifecycle::RunScoped
            || persisted.producer != expected_producer
            || persisted.provenance.source_family != "akzio.context"
            || persisted.provenance.producer_contract_hash.as_ref() != Some(&contract.contract_hash)
            || origin.run_id.as_ref() != Some(&permit.run_id)
            || origin.task_id.as_ref() != Some(&permit.task_id)
            || origin.attempt_id.as_ref() != Some(&permit.attempt_id)
            || origin.contract_hash.as_ref() != Some(&contract.contract_hash)
        {
            return Err(ContextError::InvalidManifestClosure);
        }

        let persisted_payload: ContextManifestPayload = self.read_payload(&persisted)?;
        if persisted_payload != manifest.payload
            || persisted_payload.validate(&contract.context).is_err()
        {
            return Err(ContextError::InvalidManifestClosure);
        }

        let mut selected = Vec::with_capacity(persisted_payload.selections.len());
        let mut readable = BTreeSet::new();
        let mut total_bytes = 0_u64;
        let mut projected_bytes = 0_u64;
        let mut estimated_tokens = 0_u32;
        for selection in &persisted_payload.selections {
            // 每个选择都重新读取并计算 projection budget，防止 payload 中篡改 token/byte
            // 计数；selected/readable 也必须是一一对应且不重复的 ArtifactId。
            if !readable.insert(selection.artifact.artifact_id.clone()) {
                return Err(ContextError::InvalidManifestClosure);
            }
            let artifact = self.store.artifact(&selection.artifact.artifact_id)?;
            artifact.validate()?;
            if artifact.kind != selection.artifact.kind {
                return Err(ContextError::InvalidManifestClosure);
            }
            self.assert_context_permitted(&contract.context, &artifact)?;
            self.assert_context_run(permit, &artifact)?;
            let legacy_tokens = estimate_tokens_from_bytes(artifact.blob.bytes);
            let (_, projected_tokens) = self.projection_budget(&artifact)?;
            let expected_tokens = if selection.projected_bytes.is_some() {
                projected_tokens
            } else {
                legacy_tokens
            };
            if selection.estimated_tokens != expected_tokens {
                return Err(ContextError::InvalidManifestClosure);
            }
            total_bytes = total_bytes.saturating_add(artifact.blob.bytes);
            projected_bytes = projected_bytes.saturating_add(
                selection
                    .projected_bytes
                    .unwrap_or(artifact.blob.bytes),
            );
            estimated_tokens = estimated_tokens.saturating_add(selection.estimated_tokens);
            selected.push(selection.artifact.clone());
        }
        let required_inputs = selected
            .iter()
            .map(|r| self.store.artifact(&r.artifact_id))
            .collect::<Result<Vec<_>, _>>()?;
        self.required_role_inputs(contract, &required_inputs)?;
        selected.sort();
        let mut expected_source_refs = selected.clone();
        expected_source_refs.extend(
            persisted_payload
                .quarantined
                .iter()
                .map(|quarantine| quarantine.artifact.clone()),
        );
        expected_source_refs.extend(
            persisted
                .source_refs
                .iter()
                .filter(|reference| reference.kind == ArtifactKind::ContextManifest)
                .cloned(),
        );
        expected_source_refs.sort();
        expected_source_refs.dedup();
        if expected_source_refs != persisted.source_refs
            || manifest.grant.readable != readable
            || manifest.grant.raw_source_closure
                != self.raw_closure(&contract.context, &persisted_payload.selections)?
            || persisted_payload.total_bytes != total_bytes
            || persisted_payload.projected_bytes.unwrap_or(total_bytes) != projected_bytes
            || persisted_payload.estimated_tokens != estimated_tokens
            || persisted_payload.input_hash != manifest_input_hash(&persisted_payload.selections)?
        {
            return Err(ContextError::InvalidManifestClosure);
        }

        // 返回排序后的精确引用供 policy_influences 或子任务闭包继续使用；这里没有
        // Store 写入，也没有对 DecisionGate/ExecutionGate 作任何结论。
        Ok(selected)
    }

    fn policy_influences_internal(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        manifest: &ContextManifest,
        now: DateTime<Utc>,
        require_live_grant: bool,
    ) -> ContextResult<Vec<ArtifactRef>> {
        // 先完整验证 Manifest，再只筛出已验证且当前 overlay head 允许的学习影响；
        // 该读取路径不会创建 CandidatePolicy，也不会改变 active head。
        let selected =
            self.validate_manifest_closure(permit, contract, manifest, now, require_live_grant)?;
        let mut influences = Vec::new();
        for reference in selected {
            if !matches!(
                reference.kind,
                ArtifactKind::Experience | ArtifactKind::CandidatePolicy
            ) {
                continue;
            }
            let artifact = self.store.artifact(&reference.artifact_id)?;
            if artifact.kind != reference.kind || !self.overlay_is_eligible(&artifact)? {
                return Err(ContextError::ForbiddenArtifact {
                    artifact_id: reference.artifact_id,
                });
            }
            influences.push(reference);
        }
        Ok(influences)
    }

    /// Build context from an explicit candidate set only. There is intentionally no
    /// `documents_for_run` fallback: a task's data surface is reproducible from the
    /// manifest and source closure alone.
    #[allow(clippy::too_many_arguments)]
    pub fn assemble(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        query_scope: &ContextQueryScope,
        candidates: impl IntoIterator<Item = ArtifactRef>,
        now: DateTime<Utc>,
        grant_ttl: Duration,
    ) -> ContextResult<ContextManifest> {
        // 输入候选必须由调用方显式提供；其余材料只来自同一 Store 中受 Contract、Run
        // 和 overlay 规则约束的 Lesson/Experience。assemble 的成功结果只是 Context
        // Manifest + ReadGrant，不代表研究提案已通过、更不代表 Decision/Execution。
        contract.validate()?;
        let policy = &contract.context;
        let mut seen = BTreeSet::new();
        let mut candidate_refs = candidates.into_iter().collect::<Vec<_>>();
        if contract.purpose.as_str() == RESEARCH_SYNTHESIZER_RECIPE_ID {
            // Synthesizer 需要闭合 Critique 所审查的 Claim，但闭合仍受父 Manifest
            // provenance 校验，不是对任意 source_ref 的递归放权。
            self.extend_synthesizer_critique_targets(permit, &mut candidate_refs)?;
        }
        let learning_scope = self.learning_query_scope(permit, policy, query_scope, &candidate_refs)?;
        let learning = self.learning_candidates(permit, policy, &learning_scope, now)?;
        // Lesson/Experience 是从 Store 召回的辅助上下文；召回审计与重验建议都是
        // RunScoped 观察，不能把学习材料写成已激活的 CandidatePolicy。
        self.suggest_lesson_revalidation(permit, &learning, &candidate_refs, now)?;
        candidate_refs.extend(learning);
        let artifacts = candidate_refs
            .into_iter()
            .filter(|reference| seen.insert(reference.artifact_id.clone()))
            .map(|reference| self.store.artifact(&reference.artifact_id))
            .collect::<Result<Vec<_>, _>>()?;
        let observed_candidates = artifacts.clone();
        let mut exclusion_reasons = std::collections::BTreeMap::new();
        let mut eligible = Vec::with_capacity(artifacts.len());
        for artifact in artifacts {
            // 先做 producer/kind、RawEvidence、Contract allowlist 和 source family 过滤；
            // 通过后再检查 overlay 与 Run 归属，所有排除原因仅用于后续 coverage 审计。
            if !governed_internal_source(&artifact)
                || artifact.kind == ArtifactKind::RawEvidence
                || !policy.permitted_kinds.contains(&artifact.kind)
                || (!policy.permitted_source_families.is_empty()
                    && !policy
                        .permitted_source_families
                        .contains(&artifact.provenance.source_family))
            {
                exclusion_reasons.insert(artifact.artifact_id.clone(), "contract_or_source_excluded");
                continue;
            }
            if self.overlay_is_eligible(&artifact)? {
                self.assert_context_run(permit, &artifact)?;
                let artifact = if artifact.kind == ArtifactKind::NormalizedEvidence
                    && policy.permitted_kinds.contains(&ArtifactKind::SemanticDetail)
                    && (policy.permitted_source_families.is_empty()
                        || policy
                            .permitted_source_families
                            .contains("akzio.ingest"))
                    && self
                        .document_value(&artifact)?
                        .get("resource")
                        .and_then(Value::as_str)
                        .is_some_and(|resource| resource.starts_with("option_chain:"))
                {
                    self.option_projection_artifact(permit, contract, &artifact, now)?
                } else {
                    artifact
                };
                self.assert_context_run(permit, &artifact)?;
                eligible.push(artifact);
            } else {
                // overlay 资格来自可变 Policy head，当前候选不满足时只排除本次读取，
                // 不修改该 head，也不把失败解释成研究或执行失败。
                exclusion_reasons.insert(artifact.artifact_id.clone(), "overlay_ineligible");
            }
        }
        let (mut artifacts, quarantined) = self.partition_untrusted_context(eligible)?;
        // 不可信证据仍可作为候选，但含指令样/金融风险指标的材料进入 quarantine，
        // 不会进入模型选择；quarantined 记录随 Manifest 保留，便于审计而非当作输入。
        let eligible_ids = artifacts.iter().map(|a| a.artifact_id.clone()).collect::<BTreeSet<_>>();
        for candidate in &observed_candidates {
            if !eligible_ids.contains(&candidate.artifact_id) {
                exclusion_reasons.entry(candidate.artifact_id.clone()).or_insert("quarantined_or_replaced_by_projection");
            }
        }
        let analyst_bundle = if matches!(contract.purpose.as_str(), RESEARCH_ANALYST_RECIPE_ID | RESEARCH_SYNTHESIZER_RECIPE_ID) {
            self.select_analyst_bundle(&artifacts, policy)?
        } else {
            None
        };
        let mut directly_referenced = BTreeSet::new();
        if matches!(
            contract.purpose.as_str(),
            RESEARCH_CRITIC_RECIPE_ID | RESEARCH_SYNTHESIZER_RECIPE_ID | akzio_domain::RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID
        ) {
            for artifact in &artifacts {
                match artifact.kind {
                    ArtifactKind::Claim => {
                        if let Ok(claim) = self.read_payload::<ResearchClaim>(artifact) {
                            if claim.validate().is_ok() {
                                directly_referenced.extend(claim.source_refs());
                            }
                        }
                    }
                    ArtifactKind::Critique => {
                        if let Ok(critique) = self.read_payload::<ResearchCritique>(artifact) {
                            if critique.validate().is_ok() {
                                directly_referenced.extend(critique.source_refs());
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        artifacts.sort_by(|left, right| {
            purpose_rank(contract.purpose.as_str(), left)
                .cmp(&purpose_rank(contract.purpose.as_str(), right))
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
                .then_with(|| {
                    right
                        .provenance
                        .confidence_ppm
                        .cmp(&left.provenance.confidence_ppm)
                })
                .then_with(|| left.artifact_id.cmp(&right.artifact_id))
        });

        if let Some(bundle) = analyst_bundle {
            // Balanced bundle 只改变排序优先级，后续仍统一经过必需集合和预算循环；
            // 它不绕过 max_artifacts/max_bytes/max_tokens。
            let bundle_ids = bundle
                .iter()
                .map(|artifact| artifact.artifact_id.clone())
                .collect::<BTreeSet<_>>();
            let mut prioritized = bundle;
            prioritized.extend(
                artifacts
                    .into_iter()
                    .filter(|artifact| !bundle_ids.contains(&artifact.artifact_id)),
            );
            artifacts = prioritized;
        }

        let required = self.required_role_inputs(contract, &artifacts)?;
        artifacts.sort_by_key(|artifact| !required.contains(&artifact.artifact_id));
        let mut total_bytes = 0_u64;
        let mut projected_bytes = 0_u64;
        let mut estimated_tokens = 0_u32;
        let mut selections = Vec::new();
        for artifact in artifacts {
            // 必需输入优先但仍受三类预算和 Artifact 数量上限约束；可选材料超限时跳过，
            // 必需材料超限则返回 MissingRequiredInput，避免形成看似成功但闭包不完整的上下文。
            let (projected, tokens) = self.projection_budget(&artifact)?;
            let next_source_bytes = total_bytes.saturating_add(artifact.blob.bytes);
            let next_projected_bytes = projected_bytes.saturating_add(projected);
            let next_tokens = estimated_tokens.saturating_add(tokens);
            if selections.len() >= usize::from(policy.max_artifacts)
                || next_source_bytes > Self::source_budget(policy)
                || next_projected_bytes > policy.max_bytes
                || next_tokens > policy.max_tokens
            {
                exclusion_reasons.insert(artifact.artifact_id.clone(), if selections.len() >= usize::from(policy.max_artifacts) {
                    "artifact_budget" } else if next_source_bytes > Self::source_budget(policy) { "source_byte_budget" }
                    else if next_projected_bytes > policy.max_bytes { "projection_byte_budget" } else { "token_budget" });
                if required.contains(&artifact.artifact_id) {
                    return Err(ContextError::MissingRequiredInput {
                        purpose: contract.purpose.as_str().to_owned(),
                        requirement: format!(
                            "{:?} {} exceeds context budget",
                            artifact.kind, artifact.artifact_id
                        ),
                    });
                }
                continue;
            }
            total_bytes = next_source_bytes;
            projected_bytes = next_projected_bytes;
            estimated_tokens = next_tokens;
            selections.push(ContextSelection {
                artifact: ArtifactRef {
                    artifact_id: artifact.artifact_id,
                    kind: artifact.kind,
                },
                reason: if artifact.producer == "evidence.option_projection" {
                    "option_chain_projection".to_owned()
                } else {
                    selection_reason(artifact.kind).to_owned()
                },
                estimated_tokens: tokens,
                projected_bytes: Some(projected),
                trust: context_trust(artifact.kind),
            });
        }
        if selections.len() < usize::from(policy.min_artifacts)
            || (contract.purpose.as_str() == RESEARCH_CRITIC_RECIPE_ID
                && (!selections
                    .iter()
                    .any(|selection| selection.artifact.kind == ArtifactKind::Claim)
                    || !selections.iter().any(|selection| {
                        selection.artifact.kind == ArtifactKind::NormalizedEvidence
                    })))
        {
            return Err(ContextError::BudgetExceeded);
        }
        // Artifact bytes are immutable, but overlay eligibility reads the mutable
        // policy head. Re-check selected artifacts immediately before minting the grant.
        // 这次复核只会缩小 selections；若缩小后低于最小数量，整个 assemble 失败，
        // 不会提交一个缺少关键材料的 Manifest。
        let mut revalidated = Vec::with_capacity(selections.len());
        total_bytes = 0;
        estimated_tokens = 0;
        for mut selection in selections {
            let artifact = self.store.artifact(&selection.artifact.artifact_id)?;
            self.assert_context_permitted(policy, &artifact)?;
            if !self.overlay_is_eligible(&artifact)? {
                exclusion_reasons.insert(artifact.artifact_id.clone(), "overlay_changed_before_grant");
                continue;
            }
            let (projected, tokens) = self.projection_budget(&artifact)?;
            total_bytes = total_bytes.saturating_add(artifact.blob.bytes);
            estimated_tokens = estimated_tokens.saturating_add(tokens);
            selection.estimated_tokens = tokens;
            selection.projected_bytes = Some(projected);
            revalidated.push(selection);
        }
        let selections = revalidated;
        if selections.len() < usize::from(policy.min_artifacts) {
            return Err(ContextError::BudgetExceeded);
        }

        let manifest = self.mint_manifest(
            permit,
            contract,
            selections,
            total_bytes,
            estimated_tokens,
            quarantined,
            now,
            grant_ttl,
        )?;
        if contract.version >= akzio_domain::REVIEWED_RESEARCH_CONTRACT_VERSION {
            // coverage 绑定刚刚铸造的 Manifest，记录组装时观察到的候选和排除原因；它是
            // 可追溯审计 Artifact，不是 Directional qualification 或 ProposalReview。
            self.record_context_coverage(permit, &manifest, &observed_candidates, &exclusion_reasons, now)?;
        }
        Ok(manifest)
    }

    /// Persist a manifest over an already-budgeted selection list and mint its
    /// grant.
    #[allow(clippy::too_many_arguments)]
    fn mint_manifest(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        selections: Vec<ContextSelection>,
        total_bytes: u64,
        estimated_tokens: u32,
        quarantined: Vec<ContextQuarantine>,
        now: DateTime<Utc>,
        grant_ttl: Duration,
    ) -> ContextResult<ContextManifest> {
        // selections 已在 assemble 中完成筛选和预算计算，这里再次读取其 Artifact 来
        // 生成 input_hash、payload 与 source_refs；payload 校验失败或 Store 提交失败时
        // 不返回内存中的 grant。
        let policy = &contract.context;
        let role_inputs = selections
            .iter()
            .map(|s| self.store.artifact(&s.artifact.artifact_id))
            .collect::<Result<Vec<_>, _>>()?;
        self.required_role_inputs(contract, &role_inputs)?;
        let input_hash = manifest_input_hash(&selections)?;
        let projected_bytes = selections
            .iter()
            .map(|selection| selection.projected_bytes.unwrap_or(0))
            .sum::<u64>();
        let payload = ContextManifestPayload {
            schema_version: DOMAIN_SCHEMA_VERSION,
            contract_hash: contract.contract_hash.clone(),
            selections: selections.clone(),
            quarantined: quarantined.clone(),
            total_bytes,
            projected_bytes: Some(projected_bytes),
            estimated_tokens,
            input_hash,
        };
        payload.validate(policy)?;
        let blob = self.store.stage_json(&payload)?;
        let artifact = Artifact::new(
            ArtifactKind::ContextManifest,
            blob,
            format!("context.{}", contract.purpose.as_str()),
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.context".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: Some(contract.contract_hash.clone()),
            },
            Some(permit.artifact_origin()),
            selections
                .iter()
                .map(|selection| selection.artifact.clone())
                .chain(
                    quarantined
                        .iter()
                        .map(|quarantine| quarantine.artifact.clone()),
                )
                .collect(),
            now,
        )?;
        self.store.write_task_artifact(
            permit,
            &artifact,
            LifecycleEventType::ContextManifestCreated,
            now,
        )?;
        // Manifest Artifact 已持久化后才构造当前 Attempt 的 ReadGrant。Grant 只是读取
        // 授权，既不确认 Agent 提交，也不触发 Decision/Execution/Paper 副作用。
        let grant = ReadGrant {
            manifest_artifact_id: artifact.artifact_id.clone(),
            run_id: permit.run_id.clone(),
            task_id: permit.task_id.clone(),
            attempt_id: permit.attempt_id.clone(),
            lease_id: permit.lease_id.clone(),
            epoch: permit.epoch,
            contract_hash: contract.contract_hash.clone(),
            readable: selections
                .iter()
                .map(|selection| selection.artifact.artifact_id.clone())
                .collect(),
            raw_source_closure: self.raw_closure(policy, &selections)?,
            expires_at: now + grant_ttl,
        };
        Ok(ContextManifest {
            artifact,
            payload,
            grant,
        })
    }

    fn learning_candidates(
        &self,
        permit: &TaskWritePermit,
        policy: &ContextPolicy,
        scope: &ContextQueryScope,
        now: DateTime<Utc>,
    ) -> ContextResult<Vec<ArtifactRef>> {
        // 从 Store 的 Active 快照中筛选学习覆盖；scope、usage、治理、来源和 overlay
        // 逐层收窄候选，最终只返回最多四个完整冲突组及有限 Experience 引用。
        let mut candidates = Vec::new();
        if policy.permitted_kinds.contains(&ArtifactKind::Lesson) {
            let mut ranked = Vec::new();
            let mut audit = Vec::new();
            for stored in self.store.active_lessons_snapshot()? {
                let artifact = stored.artifact;
                let mut record = serde_json::json!({"artifact_id":artifact.artifact_id,"lesson_id":stored.lesson.lesson_id,
                    "status":"scope_mismatch","scope":stored.lesson.scope});
                if !scope.matches(&stored.lesson.scope) {
                    audit.push(record);
                    continue;
                }
                let usage = self.store.lesson_usage(&stored.lesson.lesson_id)?;
                let usage_count = usage
                    .context_manifests
                    .saturating_add(usage.decision_contexts);
                if !stored
                    .lesson
                    .is_retrievable(now, usage_count, &scope.regimes)
                {
                    record["status"] = serde_json::json!("governance_ineligible");
                    audit.push(record);
                    continue;
                }
                if policy.permitted_source_families.is_empty()
                    || policy
                        .permitted_source_families
                        .contains(&artifact.provenance.source_family)
                {
                    self.assert_context_permitted(policy, &artifact)?;
                    if self.overlay_is_eligible(&artifact)? {
                        record["status"] = serde_json::json!("eligible");
                        ranked.push((artifact, stored.lesson));
                    } else {
                        record["status"] = serde_json::json!("overlay_ineligible");
                    }
                } else {
                    record["status"] = serde_json::json!("source_family_excluded");
                }
                audit.push(record);
            }
            ranked.sort_by(|(_,a),(_,b)| lesson_relevance(b,scope).cmp(&lesson_relevance(a,scope))
                .then_with(|| b.updated_at.cmp(&a.updated_at)).then_with(|| a.lesson_id.cmp(&b.lesson_id)));
            let mut fingerprints = BTreeSet::new();
            let mut selected_lessons = Vec::new();
            for (artifact,lesson) in &ranked {
                let fingerprint = lesson_retrieval_fingerprint(lesson)?;
                // Explicit conflicts are distinct evidence, even with identical prose.
                if !lesson.conflicts_with.is_empty() || ranked.iter().any(|(_,other)| other.conflicts_with.iter().any(|r| r.artifact_id==artifact.artifact_id)) || fingerprints.insert(fingerprint) {
                    selected_lessons.push((artifact,lesson));
                } else if let Some(record) = audit.iter_mut().find(|r| r["artifact_id"] == serde_json::json!(artifact.artifact_id)) {
                    record["status"] = serde_json::json!("exact_duplicate");
                }
            }
            // When a selected lesson has an eligible explicit conflict, reserve
            // a slot for its counterpart instead of presenting one side alone.
            let chosen = select_lesson_groups(&selected_lessons.iter().map(|(artifact,lesson)|
                (artifact.artifact_id.clone(),lesson.conflicts_with.clone())).collect::<Vec<_>>(),4);
            for record in &mut audit {
                if record["status"] == "eligible" {
                    record["status"] = serde_json::json!(if chosen.iter().any(|id| record["artifact_id"] == serde_json::json!(id)) {"selected"} else {"rank_or_conflict_capacity"});
                }
            }
            let selected_refs = chosen.into_iter().map(|artifact_id| ArtifactRef {artifact_id,kind:ArtifactKind::Lesson}).collect::<Vec<_>>();
            // 审计记录与所选 Lesson 一起通过 TaskWritePermit 写入 RunScoped Artifact；
            // 这保留筛选过程，但不把“selected”解释为 Lesson 已被批准或激活。
            self.record_learning_observation(permit,"learning.retrieval.audit",&serde_json::json!({"scope":scope,
                "selection_limit":4,"ranking":"regime,stage,asset,horizon,updated_at,lesson_id","records":audit}),selected_refs.clone(),now)?;
            candidates.extend(selected_refs);
        }
        if policy.permitted_kinds.contains(&ArtifactKind::Experience) {
            let mut experience_count = 0;
            for artifact in self
                .store
                .recent_artifacts_by_kind(ArtifactKind::Experience, 100)?
            {
                // Experience 只能在当前 Contract 允许的 source family、overlay head 和
                // canonical learning 资格下进入 Context；Retrospective 仅按其 source_ref
                // 作为附带材料，不在这里重新计算 Outcome。
                if policy.permitted_source_families.is_empty()
                    || policy
                        .permitted_source_families
                        .contains(&artifact.provenance.source_family)
                {
                    self.assert_context_permitted(policy, &artifact)?;
                    if self.overlay_is_eligible(&artifact)? {
                        candidates.push(ArtifactRef {
                            artifact_id: artifact.artifact_id,
                            kind: artifact.kind,
                        });
                        for source in &artifact.source_refs {
                            if source.kind == ArtifactKind::Retrospective
                                && policy.permitted_kinds.contains(&source.kind)
                            {
                                candidates.push(source.clone());
                            }
                        }
                        experience_count += 1;
                    }
                }
                if experience_count >= 4 {
                    break;
                }
            }
        }
        Ok(candidates)
    }
}

// 按当前查询 scope 计算 Lesson 的重合维度；返回值只用于确定性排序，不是 Lesson
// 的有效性或交易方向评分。
fn lesson_relevance(lesson: &Lesson, scope: &ContextQueryScope) -> (usize, usize, usize, usize) {
    (lesson.scope.regimes.intersection(&scope.regimes).count(),
        lesson.scope.decision_stages.intersection(&scope.decision_stages).count(),
        lesson.scope.assets.intersection(&scope.assets).count(),
        lesson.scope.horizons.intersection(&scope.horizons).count())
}

// 规范化空白后生成稳定指纹，用于折叠完全重复的 Lesson；scope、排除条件和关键文本
// 都纳入指纹，因此不能把不同适用范围误合并。
fn lesson_retrieval_fingerprint(lesson: &Lesson) -> ContextResult<String> {
    let normalize = |text: &str| text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut exclusions = lesson.exclusions.iter().map(|s| normalize(s)).collect::<Vec<_>>();
    exclusions.sort();
    Ok(serde_json::to_string(&serde_json::json!({"title":normalize(&lesson.title),"rationale":normalize(&lesson.rationale),"statement":normalize(&lesson.statement),
        "recommended_behavior":normalize(&lesson.recommended_behavior),"scope":lesson.scope,"exclusions":exclusions}))?)
}

// 把显式 conflicts_with 构造成无向连通分量，只有完整分量能放入 limit 时才选择；
// 这样冲突观点不会因容量限制被裁成只剩一侧。
fn select_lesson_groups(entries: &[(ArtifactId,Vec<ArtifactRef>)], limit: usize) -> Vec<ArtifactId> {
    let mut adjacency = entries.iter().map(|(id,_)|(id.clone(),BTreeSet::new())).collect::<std::collections::BTreeMap<_,_>>();
    for (id,refs) in entries {
        for reference in refs {
            if adjacency.contains_key(&reference.artifact_id) {
                adjacency.get_mut(id).expect("known lesson").insert(reference.artifact_id.clone());
                adjacency.get_mut(&reference.artifact_id).expect("known conflict").insert(id.clone());
            }
        }
    }
    let mut visited = BTreeSet::new();
    let mut chosen = Vec::new();
    for (id,_) in entries {
        if chosen.len()==limit {break;}
        if visited.contains(id) {continue;}
        let mut component = BTreeSet::new();
        let mut queue = VecDeque::from([id.clone()]);
        while let Some(next) = queue.pop_front() {
            if visited.insert(next.clone()) {
                component.insert(next.clone());
                queue.extend(adjacency[&next].iter().cloned());
            }
        }
        if chosen.len()+component.len()<=limit {
            chosen.extend(entries.iter().filter(|(id,_)|component.contains(id)).map(|(id,_)|id.clone()));
        }
    }
    chosen
}
