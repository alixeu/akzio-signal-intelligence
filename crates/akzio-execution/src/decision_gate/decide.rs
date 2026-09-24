// 文件导读：本文件实现 DecisionRuntime 的主事务。它从 proposal 沿 Manifest 递归读取
// selected/quarantined/ancestor 闭包，加载 Claim/Critique/Review 与 learning attribution，
// 再调用 DecisionPolicy 生成 horizon trace、研究分配复核、目标和风险；最终按 Paper 或
// PositionPlan 选择 Artifact lifecycle，并一次性提交 DecisionContext/Decision。所有
// iterator/closure 只在内存中汇总已持久化证据，不能新增证据、改变 Prompt 或越过 Execution。

impl DecisionRuntime {
    pub fn new(store: Store, policy: DecisionPolicy) -> DecisionGateResult<Self> {
        // `store` 与 `policy` 按值移入 runtime，由本对象持有；Policy 先整体校验，非法时
        // 返回错误而不构造半成品，后续一次 Decision 也不会临时换校准身份。
        policy.validate()?;
        Ok(Self { store, policy })
    }

    pub fn policy(&self) -> &DecisionPolicy {
        // `&self` 共享借用 runtime，返回的策略引用受该借用生命周期约束；上层可读但不能修改。
        &self.policy
    }

    /// Validate, bind, and atomically complete the DecisionGate attempt.
    pub fn decide(&self, input: &DecisionGateInput) -> DecisionGateResult<DecisionGateOutput> {
        // 顺序是 permit/proposal→Manifest 闭包→draft/Claim/Critique 语义→Review/learning
        // 归因→研究计划裁剪→horizon/校准/risk→Decision artifacts→Store 原子提交。任何
        // 失败都停在提交前；Decision 成功也只表示正式决策产物已绑定，不表示 Execution 或成交。
        // `input` 共享借用，Permit、proposal ref 和 now 会在需要绑定输出时显式 clone/复制；
        // 失败的 `?`/early return 都发生在 commit_attempt 前，因此不发布半份 Decision。
        let decision_gate_started = std::time::Instant::now();
        self.store.validate_task_permit(&input.permit)?;

        let proposal = self.load_expected(&input.proposal, ArtifactKind::DecisionProposal)?;
        let proposal_contract = self.validate_proposal(&proposal, &input.permit)?;
        let manifest_ref = unique_manifest_ref(&proposal)?;
        let manifest = self.load_expected(manifest_ref, ArtifactKind::ContextManifest)?;
        let selected =
            self.validate_manifest(&manifest, &proposal, &proposal_contract, &input.permit)?;

        // proposal 是研究 Synthesizer 的提案 Artifact；从其 CAS blob 解出 draft 后，
        // DecisionGate 独立校验引用闭包和语义，不能把模型提案本身当作正式 Decision。
        let draft: DecisionDraft = serde_json::from_slice(&self.store.read_blob(&proposal.blob)?)?;
        draft.validate()?;
        self.validate_draft_closure(&draft, &selected)?;
        let raw_research_plan =
            draft
                .research_allocation
                .as_ref()
                .ok_or(DomainError::EmptyField {
                    field: "decision_draft.research_allocation",
                })?;
        // Semantic evidence sufficiency is unconditional. A producer contract
        // that is not installed, or that predates the rule, cannot vouch for
        // claim semantics, so the gate rejects the proposal rather than
        // silently skipping the check.
        // 读取写入该 proposal 时冻结的 Contract 安装信息。版本决定调用哪套研究语义校验；
        // 缺安装记录或低于最低方向证据版本会拒绝，不会跳过语义验证。
        let installed = self
            .store
            .contract_installation(&proposal_contract)?
            .ok_or(DecisionGateError::UnsupportedProposalContract)?;
        if installed.contract.version < akzio_domain::DIRECTION_BOUND_RESEARCH_CONTRACT_VERSION {
            return Err(DecisionGateError::UnsupportedProposalContract);
        }
        if installed.contract.version >= akzio_domain::REVIEWED_RESEARCH_CONTRACT_VERSION {
            // 新版数值依据和终稿 Review 都必须在决定前可验证；Review 还要授权当前
            // proposal ArtifactRef 与 blob hash，缺失/拒绝会作为 Gate 错误结束本次任务。
            akzio_domain::validate_numeric_bases(&draft.numeric_basis)?;
            let (_, review) = self.store.final_proposal_review(&input.permit.run_id, &input.permit.task_id)?
                .ok_or(DecisionGateError::ProposalReviewRequired)?;
            if !review.authorizes(&input.proposal, &proposal.blob.hash) {
                return Err(DecisionGateError::ProposalReviewRequired);
            }
        }
        // map 闭包为每个 Claim 做 Store kind 校验、CAS 解码和领域 validate；collect 到
        // Result<Vec<_>> 时才实际遍历，并在任一错误处短路整批加载。
        let claim_records = draft
            .claims
            .iter()
            .map(|reference| {
                let artifact = self.load_expected(reference, ArtifactKind::Claim)?;
                let claim: ResearchClaim =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                claim.validate()?;
                Ok((artifact, claim))
            })
            .collect::<DecisionGateResult<Vec<_>>>()?;
        // 下面克隆 payload 形成便于按索引与 draft 引用 zip 的临时 owned Vec；Artifact 与
        // claim_records 仍保留完整 provenance，克隆不代表新增或持久化 Claim。
        let claims = claim_records
            .iter()
            .map(|(_, claim)| claim.clone())
            .collect::<Vec<_>>();
        if installed.contract.version < akzio_domain::STRUCTURED_RESEARCH_CONTRACT_VERSION {
            // 旧于单次结构化提交协议的已冻结 Contract 仍走兼容语义检查；新 Contract 的
            // slots/eligibility 会由下面对应的新校验器处理。
            validate_decision_evidence_sufficiency(&draft, &claims)
                .map_err(|_| DecisionGateError::InsufficientClaimEvidence)?;
        }

        // Critique 同样逐项解码；target 必须指向当前 draft 选中的 Claim，防止外部审查
        // 被拼接进本次 proposal。collect 失败时不会得到部分可用的 critiques。
        let critique_records = draft
            .critiques
            .iter()
            .map(|reference| {
                let artifact = self.load_expected(reference, ArtifactKind::Critique)?;
                let critique: ResearchCritique =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                critique.validate()?;
                if !draft.claims.contains(&critique.target) {
                    return Err(DecisionGateError::InvalidClaimVerification(
                        reference.artifact_id.clone(),
                    ));
                }
                Ok((artifact, critique))
            })
            .collect::<DecisionGateResult<Vec<_>>>()?;
        // 保留完整 critique Artifact records 供 provenance 检查，同时提取 payload 副本供
        // 纯领域资格函数读取；这些克隆只存在于内存。
        let critiques = critique_records
            .iter()
            .map(|(_, critique)| critique.clone())
            .collect::<Vec<_>>();
        // 这里把一个具体函数项选入局部变量（函数指针），其输入/输出签名一致；Contract
        // 版本决定新旧 slot validator，实际验证发生在下面的调用处。
        let validate_slots = if installed.contract.version >= akzio_domain::STRUCTURED_RESEARCH_CONTRACT_VERSION {
            akzio_domain::validate_verified_forecast_slots
        } else { akzio_domain::validate_legacy_verified_forecast_slots };
        validate_slots(
            &draft,
            &claim_records
                .iter()
                .map(|(artifact, claim)| {
                    (
                        ArtifactRef {
                            artifact_id: artifact.artifact_id.clone(),
                            kind: ArtifactKind::Claim,
                        },
                        claim.clone(),
                    )
                })
                .collect::<Vec<_>>(),
            &critiques,
        )
        .map_err(|_| DecisionGateError::InsufficientClaimEvidence)?;
        if installed.contract.version >= akzio_domain::STRUCTURED_RESEARCH_CONTRACT_VERSION {
            akzio_domain::validate_structured_allocation_eligibility(&draft, &claim_records.iter().map(|(artifact, claim)| (
                ArtifactRef { artifact_id: artifact.artifact_id.clone(), kind: ArtifactKind::Claim }, claim.clone()
            )).collect::<Vec<_>>(), &critiques).map_err(|_| DecisionGateError::InsufficientClaimEvidence)?;
        }
        // 先校验研究分配意图，并保留 raw/validated 差异；它不是正式目标组合，更不是订单。
        let research_plan = self.review_research_plan(
            raw_research_plan,
            &draft.forecasts,
            &claim_records,
            &critique_records,
            &input.permit.run_id,
        )?;
        // 仅将确实支撑任一非中性 forecast 的 Claim 归为 active：zip 按 draft 引用与已解码
        // payload 的同序一一配对，filter 闭包只借用二者，collect 后保留引用/载荷借用。
        let active_claims = draft
            .claims
            .iter()
            .zip(&claims)
            .filter(|(_, claim)| {
                draft.forecasts.iter().any(|forecast| {
                    !forecast.is_neutral()
                        && forecast.horizon == claim.horizon
                        && claim.grounds.iter().any(|g| {
                            g.role == akzio_domain::EvidenceGroundRole::Directional
                                && g.assets.contains(&forecast.asset)
                        })
                })
            })
            .collect::<Vec<_>>();
        // 解引用 `&&ArtifactRef` / `&&ResearchClaim` 后 clone，得到本地拥有的 active 列表，
        // 以便后续计算无需把 draft 或 claims 的所有权移出。
        let active_refs = active_claims
            .iter()
            .map(|(reference, _)| (*reference).clone())
            .collect::<Vec<_>>();
        let active_payloads = active_claims
            .iter()
            .map(|(_, claim)| (*claim).clone())
            .collect::<Vec<_>>();
        // 高 materiality 的 active Claim 若没有唯一有效 Critique，会作为 blocker 留在正式
        // DecisionContext，而不是丢掉该研究记录或另造支持结论。
        let critical_claim_unverified = has_unverified_critical_claim(
            &active_refs,
            &active_payloads,
            &critiques,
            &draft.forecasts,
        );

        // 只把已显式 applied 且 kind 为 Experience/CandidatePolicy 的引用作为 policy influence；
        // map 闭包对每项重新查 Store 的资格/active head，任一失败使整个 collect 返回 Err。
        let policy_influences = draft
            .applied_learning_refs
            .iter()
            .filter(|reference| {
                matches!(
                    reference.kind,
                    ArtifactKind::Experience | ArtifactKind::CandidatePolicy
                )
            })
            .map(|reference| {
                self.validate_policy_influence(reference)?;
                Ok(reference.clone())
            })
            .collect::<DecisionGateResult<Vec<_>>>()?;

        // 以 BTreeSet 对已有 blocker 去重并保持稳定次序；后续条件只追加新原因，不清掉提案
        // 自带阻断。MaterialConflict 仅在冲突指向 active Claim 时影响本 Decision。
        let mut hard_blockers = draft.hard_blockers.iter().copied().collect::<BTreeSet<_>>();
        if critical_claim_unverified {
            hard_blockers.insert(HardBlocker::UnverifiedClaim);
        }
        if draft
            .material_conflicts
            .iter()
            .any(|conflict| active_refs.contains(&conflict.claim))
        {
            hard_blockers.insert(HardBlocker::MaterialConflict);
        }

        // bool::then 延迟执行共识来源评估，空 Claim 集得到 None；transpose 把
        // Option<Result<_>> 换为 Result<Option<_>>，评估错误仍向调用者传播。
        let mut consensus_diversity = (!claim_records.is_empty())
            .then(|| self.consensus_diversity_assessment(&claim_records, draft.confidence_ppm))
            .transpose()?;
        let correlated_consensus = consensus_diversity
            .as_ref()
            .is_some_and(|assessment| assessment.participant_count > 1 && !assessment.independent);
        let effective_confidence_ppm = consensus_diversity
            .as_ref()
            .map_or(draft.confidence_ppm, |assessment| {
                self.effective_consensus_confidence_ppm(assessment, draft.confidence_ppm)
            });
        if let Some(assessment) = &mut consensus_diversity {
            assessment.record_confidence(draft.confidence_ppm, effective_confidence_ppm)?;
        }
        let mut soft_warnings = draft.soft_warnings.iter().copied().collect::<BTreeSet<_>>();
        if correlated_consensus {
            soft_warnings.insert(SoftWarning::CorrelatedConsensus);
        }

        // 研究语义检查之后才应用当前 DecisionPolicy：冲突记入 blocker，校准结果生成
        // Rust-owned 目标/风险/trace；这些产物仍不代表 ExecutionGate 已通过。
        let policy_hash = self.policy.policy_hash()?;
        let horizon_trace = self.policy.horizon_trace(input.now, &draft.forecasts)?;
        if !horizon_trace.conflicts.is_empty() {
            hard_blockers.insert(HardBlocker::HorizonConflict);
        }
        let (target, portfolio_risk, runtime_trace) = self.policy.target_with_risk_traced(
            input.now,
            effective_confidence_ppm,
            &draft.forecasts,
        )?;
        // 截止时间从 Manifest selected 引用中寻找不晚于 now 的 observed_at 最大值；
        // 重新读取失败的条目在此 best-effort 过滤掉，空结果退回 now，不把未来观察算进 cutoff。
        let evidence_cutoff = selected
            .iter()
            .filter_map(|reference| self.store.artifact(&reference.artifact_id).ok())
            .filter_map(|artifact| artifact.provenance.observed_at)
            .filter(|observed_at| *observed_at <= input.now)
            .max()
            .unwrap_or(input.now);
        // 执行有效期受最大执行延迟约束；u64→i64 转换失败返回预算错误，不截断时间。
        let policy_valid_until = input.now
            + chrono::Duration::milliseconds(
                i64::try_from(self.policy.maximum_execution_delay_ms).map_err(|_| {
                    DomainError::InvalidBudget {
                        field: "decision_policy.maximum_execution_delay_ms",
                    }
                })?,
            );
        // Decision 同时不能晚于任一 Forecast thesis 的有效期；缺失所有 thesis 是结构错误。
        let thesis_valid_until = draft
            .forecasts
            .iter()
            .filter_map(|forecast| forecast.thesis.as_ref())
            .map(|thesis| thesis.thesis_valid_until)
            .min()
            .ok_or(DomainError::EmptyField {
                field: "decision_draft.forecast_thesis",
            })?;
        let validity = DecisionValidity {
            evidence_cutoff,
            generated_at: input.now,
            valid_until: policy_valid_until.min(thesis_valid_until),
            maximum_execution_delay_ms: self.policy.maximum_execution_delay_ms,
            market_state_hash: manifest.artifact_id.0.clone(),
        };
        // 闭包按值接收 passing/total，把通过比例归一到 ppm；输入为 usize，缩窄失败时
        // 采用既有上限保护，total 为 0 则返回 None 而非把“无数据”标成满分。
        // 这个闭包只把已计数的通过项转换为 ppm；total=0 保持 None，让“没有可评估证据”
        // 与“全部通过”在投资逻辑 trace 中保持不同语义。
        let score_ratio = |passing: usize, total: usize| {
            (total > 0).then(|| {
                u32::try_from(
                    u64::try_from(passing)
                        .unwrap_or(u64::MAX)
                        .saturating_mul(u64::from(WeightPpm::SCALE))
                        / u64::try_from(total).unwrap_or(u64::MAX),
                )
                .unwrap_or(WeightPpm::SCALE)
            })
        };
        // 这些迭代器只构造读取链，最终 collect 才消费它；BTreeSet 去重 Claim/Critique
        // grounds 与 draft evidence 的 ArtifactRef，保留来源身份而非合并正文。
        let observed_events = claim_records
            .iter()
            .flat_map(|(_, claim)| claim.grounds.iter().map(|ground| ground.evidence.clone()))
            .chain(critique_records.iter().flat_map(|(_, critique)| {
                critique
                    .grounds
                    .iter()
                    .map(|ground| ground.evidence.clone())
                    .chain(
                        critique
                            .supporting_refs
                            .iter()
                            .map(|reference| reference.evidence.clone()),
                    )
                    .chain(
                        critique
                            .conflicting_refs
                            .iter()
                            .map(|reference| reference.evidence.clone()),
                    )
            }))
            .chain(draft.evidence.iter().cloned())
            .collect::<BTreeSet<_>>();
        // flat_map/chain 只沿 Claim、Critique 和 draft 的引用收集去重事件，来源是否真的
        // 位于对应 Artifact.source_refs 则由后面的 grounded_event_count 独立计算。
        // grounded 统计独立对照每个 Claim/Critique Artifact 自身的 source_refs；flat_map
        // 闭包使用 move 捕获当前 artifact 引用，以便内层 iterator 活到外层链消费完。
        let grounded_event_count = claim_records
            .iter()
            .flat_map(|(artifact, claim)| {
                claim
                    .grounds
                    .iter()
                    .map(move |ground| artifact.source_refs.contains(&ground.evidence))
            })
            .chain(critique_records.iter().flat_map(|(artifact, critique)| {
                critique
                    .source_refs()
                    .into_iter()
                    .filter(|reference| reference.kind != ArtifactKind::Claim)
                    .map(move |reference| artifact.source_refs.contains(&reference))
            }))
            .filter(|grounded| *grounded)
            .count();
        // 分母按 Claim grounds 与 Critique 非 Claim source refs 计数，随后 score_ratio
        // 在分母为零时保留 None，避免把缺证据解释为“全部 grounded”。
        let total_event_ground_count = claim_records
            .iter()
            .map(|(_, claim)| claim.grounds.len())
            .chain(critique_records.iter().map(|(_, critique)| {
                critique
                    .source_refs()
                    .into_iter()
                    .filter(|reference| reference.kind != ArtifactKind::Claim)
                    .count()
            }))
            .sum();
        let event_grounding_ppm = score_ratio(grounded_event_count, total_event_ground_count);
        // 前提支持率要求 Claim 至少有一个 ground，且每个 ground 都出现在该 Claim Artifact
        // 的 source_refs；filter 仅统计满足完整闭包的 claim。
        let premise_support_ppm = score_ratio(
            claim_records
                .iter()
                .filter(|(artifact, claim)| {
                    !claim.grounds.is_empty()
                        && claim
                            .grounds
                            .iter()
                            .all(|ground| artifact.source_refs.contains(&ground.evidence))
                })
                .count(),
            claim_records.len(),
        );
        // supporting/conflicting 引用合并成借用列表后，分别计算当前权威性与 active Claim
        // 的 Supported/Contradicted 状态；这些是过程质量证据，不会补造缺失的 Critique。
        let verification_refs = critiques
            .iter()
            .flat_map(|critique| {
                critique
                    .supporting_refs
                    .iter()
                    .chain(critique.conflicting_refs.iter())
            })
            .collect::<Vec<_>>();
        let temporal_validity_ppm = score_ratio(
            verification_refs
                .iter()
                .filter(|reference| reference.is_current_authoritative())
                .count(),
            verification_refs.len(),
        );
        let logic_validity_ppm = score_ratio(
            active_refs
                .iter()
                .filter(|claim| {
                    let statuses = critiques
                        .iter()
                        .filter(|critique| critique.target == **claim)
                        .map(|critique| critique.verification_status)
                        .collect::<Vec<_>>();
                    statuses.contains(&ClaimVerificationStatus::Supported)
                        && !statuses.contains(&ClaimVerificationStatus::Contradicted)
                })
                .count(),
            active_refs.len(),
        );
        // 阶段耗时从已保存的 AgentTurn telemetry 汇总；不存在完整遥测时相应字段为 None，
        // 不用“0 毫秒”代替未知。DecisionGate 自身时间以单调 Instant 计时。
        let stage_latencies = DecisionStageLatencies {
            retrieval_latency_millis: None,
            model_latency_millis: self.model_stage_latency_millis(&proposal, &claim_records)?,
            critic_latency_millis: self.critic_stage_latency_millis(&critique_records)?,
            decision_gate_latency_millis: Some(
                u64::try_from(decision_gate_started.elapsed().as_millis()).unwrap_or(u64::MAX),
            ),
        };
        // trace 记录采用/驳回前提、候选替代、选中 target hash 与失效条件；invalidation
        // 经 BTreeSet 排序去重，quality 中尚未经过 ExecutionGate 的三项保持 None。
        let investment_logic = InvestmentLogicTrace {
            observed_events: observed_events.into_iter().collect(),
            accepted_premises: draft.claims.clone(),
            rejected_premises: critiques
                .iter()
                .filter(|critique| {
                    critique.verification_status == ClaimVerificationStatus::Contradicted
                })
                .map(|critique| critique.target.clone())
                .collect(),
            alternatives_considered: draft.critiques.clone(),
            selected_action_hash: content_hash_json(&serde_json::to_value(&target)?)?,
            invalidation_conditions: draft
                .forecasts
                .iter()
                .flat_map(|forecast| {
                    forecast
                        .thesis
                        .iter()
                        .flat_map(|thesis| thesis.invalidation_conditions.iter().cloned())
                })
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            quality: ProcessQualityAssessment {
                event_grounding_ppm,
                premise_support_ppm,
                temporal_validity_ppm,
                logic_validity_ppm,
                mandate_consistency_ppm: None,
                portfolio_consistency_ppm: None,
                action_feasibility_ppm: None,
            },
            consensus_diversity,
            stage_latencies,
        };
        // research_measured_floor 缺少任一可测量分项时也不能满足质量门槛；只追加 blocker，
        // 研究 trace/目标仍保留，最终是否可执行还需下一道 ExecutionGate。
        if self.policy.minimum_process_quality_ppm > 0
            && investment_logic
                .quality
                .research_measured_floor()
                .is_none_or(|floor| floor < self.policy.minimum_process_quality_ppm)
        {
            hard_blockers.insert(HardBlocker::UnverifiedClaim);
        }
        let asset_eligibility = build_asset_eligibility(
            &self.policy,
            input.now,
            effective_confidence_ppm,
            &draft.forecasts,
            &claim_records,
            &critique_records,
            &horizon_trace,
            &portfolio_risk,
            &target,
        );
        // 正式 DecisionContext 同时保留 proposal 的原始研究引用、Rust 校验后的研究计划、
        // 风险目标与 blockers；research_plan 和 target 是不同语义，前者不是可下单计划。
        let context_payload = DecisionContext {
            schema_version: DOMAIN_SCHEMA_VERSION,
            decision_id: akzio_domain::DecisionId::new(),
            run_id: input.permit.run_id.clone(),
            claims: draft.claims.clone(),
            critiques: draft.critiques.clone(),
            evidence: draft.evidence.clone(),
            policy_influences,
            applied_learning_refs: draft.applied_learning_refs.clone(),
            rejected_learning_refs: draft.rejected_learning_refs.clone(),
            material_conflicts: draft.material_conflicts.clone(),
            hard_blockers: hard_blockers.into_iter().collect(),
            soft_warnings: soft_warnings.into_iter().collect(),
            decision_policy_hash: policy_hash,
            behavior_bundle_hash: None,
            portfolio_risk,
            target: target.clone(),
            created_at: input.now,
            validity: Some(validity),
            horizon_trace: Some(horizon_trace),
            investment_logic: Some(investment_logic),
            asset_eligibility,
            runtime_trace: Some(runtime_trace),
            research_plan: Some(research_plan.clone()),
        };
        context_payload.validate()?;

        // 只有 Paper Run 的 Decision artifacts 标成 Canonical；PositionPlan 等其他 purpose
        // 保持 RunScoped，不能因完成 Decision 而进入 Paper 执行或校准样本。
        let lifecycle = match self.store.run_purpose(&input.permit.run_id)? {
            RunPurpose::Paper => ArtifactLifecycle::Canonical,
            _ => ArtifactLifecycle::RunScoped,
        };
        // context 的 lineage 明确包含 proposal、唯一 manifest 和 selected 闭包；
        // quarantined 内容不列为模型有效证据，祖先 Manifest 已由递归校验其自身来源。
        let mut context_sources = Vec::with_capacity(selected.len() + 2);
        context_sources.push(input.proposal.clone());
        context_sources.push(manifest_ref.clone());
        context_sources.extend(selected.iter().cloned());
        let decision_context = self.artifact(
            ArtifactKind::DecisionContext,
            "decision.context",
            &context_payload,
            lifecycle,
            context_sources,
            input,
        )?;
        let context_ref = ArtifactRef {
            artifact_id: decision_context.artifact_id.clone(),
            kind: ArtifactKind::DecisionContext,
        };
        // 正式 Decision 引用 DecisionContext 与 proposal；这里移动 draft.summary/forecasts
        // 和 target 到新 payload，draft 在后续不再使用，研究计划则另行保留。
        let decision_payload = Decision {
            schema_version: DOMAIN_SCHEMA_VERSION,
            decision_context: context_ref.clone(),
            summary: draft.summary,
            targets: target,
            confidence_ppm: effective_confidence_ppm,
            forecasts: draft.forecasts,
            research_plan: Some(research_plan),
            created_at: input.now,
        };
        decision_payload.validate()?;
        let decision = self.artifact(
            ArtifactKind::Decision,
            "decision.bound",
            &decision_payload,
            lifecycle,
            vec![context_ref, input.proposal.clone()],
            input,
        )?;

        // 到这里才一次性提交两份已验证 Artifact 并将当前 Attempt 标为成功；Store 错误会
        // 使 decide 返回 Err。成功仅表示 Decision 持久化完成，不启动 Execution/Paper。
        self.store.commit_attempt(
            &input.permit,
            &[decision_context.clone(), decision.clone()],
            TaskStatus::Succeeded,
            input.now,
        )?;
        Ok(DecisionGateOutput {
            decision_context,
            decision,
        })
    }

    fn review_research_plan(
        &self,
        raw: &ResearchAllocationPlan,
        forecasts: &[Forecast],
        claims: &[(Artifact, ResearchClaim)],
        critiques: &[(Artifact, ResearchCritique)],
        run_id: &akzio_domain::RunId,
    ) -> DecisionGateResult<ResearchPlanReview> {
        // 研究分配是模型表达的意图而非订单：逐项删除没有同资产/同 horizon、price+macro
        // 支撑或 Critique 通过的行，再按静态 gross cap 缩放并把余量归入 cash。最后依据
        // run purpose 和 policy readiness 分别标记 explicit cash、blocked、not_applicable
        // 或 pending execution，绝不把它直接当作 ExecutionPlan。
        // `raw`、forecast 与 records 全以共享借用输入；先 validate 再 clone 出可裁剪副本，
        // 因此 Rust 的归一化不会改写 Synthesizer 原始提案。
        raw.validate()?;
        let mut validated = raw.clone();
        let mut reasons_by_asset = BTreeMap::<Asset, Vec<String>>::new();

        for allocation in &mut validated.allocations {
            // 循环独占借用 clone 后的 allocation；零权重无需收窄其研究期限，`continue`
            // 只跳过当前资产行，不影响其余资产的资格复核。
            if allocation.target_weight_ppm.0 == 0 {
                continue;
            }
            let original_horizons = allocation.supporting_horizons.clone();
            // filter 闭包对每个声明 horizon 搜索同资产同期限、正预期收益的 forecast，
            // 并要求 price/macro Claim 与 Critique 证据闭包通过；collect 保留原顺序。
            let supported_horizons = original_horizons
                .iter()
                .copied()
                .filter(|horizon| {
                    forecasts.iter().any(|forecast| {
                        forecast.asset == allocation.asset
                            && forecast.horizon == *horizon
                            && forecast.expected_return_ppm > 0
                            && research_slot_supported(
                                allocation.asset,
                                *horizon,
                                &allocation.evidence_refs,
                                claims,
                                critiques,
                            )
                    })
                })
                .collect::<Vec<_>>();
            if supported_horizons != original_horizons {
                // 只要有期限被删除，就记录审计原因并用过滤后的 owned Vec 替换副本。
                reasons_by_asset
                    .entry(allocation.asset)
                    .or_default()
                    .push("unsupported_research_horizon_removed".to_owned());
                allocation.supporting_horizons = supported_horizons;
            }
            if allocation.supporting_horizons.is_empty() {
                // 声明范围全部被 Rust 拒绝时，将该资产目标清零并保留 abstention 说明；
                // 这不会删除原始 raw plan，后面 review 会同时返回两者。
                allocation.target_weight_ppm = WeightPpm::ZERO;
                allocation.abstention_reason = Some(
                    "Rust blocked the requested target because no listed horizon passed the research evidence and critique checks."
                        .to_owned(),
                );
                reasons_by_asset
                    .entry(allocation.asset)
                    .or_default()
                    .push("research_support_missing".to_owned());
            }
        }

        // checked fold 计算裁剪前的研究组合 gross；溢出返回 DomainError，而不是继续缩放。
        let gross = validated
            .allocations
            .iter()
            .try_fold(0_u32, |sum, allocation| {
                sum.checked_add(allocation.target_weight_ppm.0)
            })
            .ok_or(DomainError::InvalidBudget {
                field: "research_allocation.gross_weight",
            })?;
        if gross > self.policy.max_gross_weight.0 {
            // 超静态上限时按同一比例缩放所有资产，整数除法向下取整；不能从卖出或现金
            // 收益推导额外额度。target_weight=0 的缩放结果会追加显式原因。
            for allocation in &mut validated.allocations {
                let before = allocation.target_weight_ppm.0;
                let after = u32::try_from(
                    u64::from(before) * u64::from(self.policy.max_gross_weight.0)
                        / u64::from(gross),
                )
                .map_err(|_| DomainError::InvalidBudget {
                    field: "research_allocation.gross_weight",
                })?;
                if after != before {
                    allocation.target_weight_ppm = WeightPpm(after);
                    if after == 0 {
                        allocation.abstention_reason = Some(
                            "Rust removed the target after the static gross exposure cap reduced it below one ppm."
                                .to_owned(),
                        );
                    }
                    reasons_by_asset
                        .entry(allocation.asset)
                        .or_default()
                        .push("static_gross_weight_limit".to_owned());
                }
            }
        }
        // 再次求和后把现金设为总 ppm 的剩余量，使研究组合守恒；这是建议分配，不是账户现金。
        let validated_gross = validated
            .allocations
            .iter()
            .try_fold(0_u32, |sum, allocation| {
                sum.checked_add(allocation.target_weight_ppm.0)
            })
            .ok_or(DomainError::InvalidBudget {
                field: "research_allocation.gross_weight",
            })?;
        validated.cash_weight_ppm = WeightPpm(WeightPpm::SCALE - validated_gross);

        // 将裁剪原因转换成面向每资产的差异记录；remove 消费该资产已累计的原因 Vec，
        // 无原因但权重变化时补一个 Rust validation 标签。
        let mut adjustments = Vec::new();
        for asset in Asset::EXECUTABLE {
            let before = raw.weight(asset);
            let after = validated.weight(asset);
            let reasons = reasons_by_asset.remove(&asset).unwrap_or_default();
            if before != after || !reasons.is_empty() {
                adjustments.push(ResearchPlanAdjustment {
                    asset: Some(asset),
                    from_weight_ppm: before,
                    to_weight_ppm: after,
                    reasons: if reasons.is_empty() {
                        vec!["rust_research_validation".to_owned()]
                    } else {
                        reasons
                    },
                });
            }
        }
        if raw.cash_weight_ppm != validated.cash_weight_ppm {
            // 现金行变化单独记为 asset=None，和四个可执行资产的调整分开。
            adjustments.push(ResearchPlanAdjustment {
                asset: None,
                from_weight_ppm: raw.cash_weight_ppm,
                to_weight_ppm: validated.cash_weight_ppm,
                reasons: vec!["cash_recomputed_after_rust_validation".to_owned()],
            });
        }

        validated.validate()?;
        // 状态首先描述研究计划本身是否有合格非零意图；之后另算 execution_status。
        let status = if validated.has_non_zero_target() {
            ResearchPlanStatus::QualifiedRecommendation
        } else if raw.has_non_zero_target() {
            ResearchPlanStatus::BlockedByResearch
        } else {
            ResearchPlanStatus::ExplicitCash
        };
        let purpose = self.store.run_purpose(run_id)?;
        // 第二层状态区分“研究被阻断”“PositionPlan 不进入执行”“Policy 未就绪”和
        // “等待 ExecutionGate”；Pending 仅表示还没跑 Gate，不表示获准或已有订单。
        let (execution_status, execution_blockers) =
            if matches!(status, ResearchPlanStatus::BlockedByResearch) {
                (
                    ResearchExecutionStatus::Blocked,
                    vec!["research_plan_blocked".to_owned()],
                )
            } else if purpose == RunPurpose::PositionPlan {
                (
                    ResearchExecutionStatus::NotApplicable,
                    vec!["position_plan_does_not_enter_execution".to_owned()],
                )
            } else if !self.policy.decision_capable() {
                (
                    ResearchExecutionStatus::Blocked,
                    vec!["decision_policy_not_ready".to_owned()],
                )
            } else {
                (
                    ResearchExecutionStatus::PendingExecutionGate,
                    vec!["execution_gate_not_run".to_owned()],
                )
            };
        let review = ResearchPlanReview {
            raw: raw.clone(),
            validated,
            adjustments,
            status,
            execution_status,
            execution_blockers,
        };
        review.validate()?;
        Ok(review)
    }

    fn consensus_diversity_assessment(
        &self,
        claim_records: &[(Artifact, ResearchClaim)],
        raw_confidence_ppm: u32,
    ) -> DecisionGateResult<ConsensusDiversityAssessment> {
        // 按 Agent task 身份聚合 capability snapshot hash 与内容相似 cluster；相同来源会
        // 降低独立性/有效置信度，但不会删除 Claim。所有读取仍来自已闭合的 AgentTurn/ground。
        // map key 是 task identity；每项 value 分别去重能力快照 hash 与内容相似 cluster。
        // claim_records 只读借用，缺 task_id 时用 Artifact ID 构造稳定的本次 fallback 分组键。
        let mut grouped = BTreeMap::<String, (BTreeSet<ContentHash>, BTreeSet<ContentHash>)>::new();
        for (claim_artifact, claim) in claim_records {
            let agent_id = claim_artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.task_id.as_ref())
                .map(|task_id| task_id.0.clone())
                .unwrap_or_else(|| format!("claim:{}", claim_artifact.artifact_id));
            let entry = grouped.entry(agent_id).or_default();
            // AgentTurn 仅从 Claim 自身 source_refs 读取；无法解析 snapshot hash 时不插入，
            // 之后 capability_snapshot_hash 会保持 None，而不是猜一个能力身份。
            for turn_ref in claim_artifact
                .source_refs
                .iter()
                .filter(|reference| reference.kind == ArtifactKind::AgentTurn)
            {
                let turn = self.load_expected(turn_ref, ArtifactKind::AgentTurn)?;
                let payload: serde_json::Value =
                    serde_json::from_slice(&self.store.read_blob(&turn.blob)?)?;
                if let Some(value) = payload.get("capability_snapshot_hash") {
                    if let Ok(hash) = serde_json::from_value::<ContentHash>(value.clone()) {
                        entry.0.insert(hash);
                    }
                }
            }
            // 每个 ground 都按其声明 ArtifactKind 重新加载；content cluster 缺失/格式不对时
            // 用该 evidence 的 Artifact ID 作为独立 cluster，保留可追溯来源而不合并。
            for ground in &claim.grounds {
                let evidence = self.load_expected(&ground.evidence, ground.evidence.kind)?;
                let payload: serde_json::Value =
                    serde_json::from_slice(&self.store.read_blob(&evidence.blob)?)?;
                let cluster = payload
                    .get("financial_content")
                    .and_then(|value| value.get("content_similarity_cluster"))
                    .and_then(|value| serde_json::from_value::<ContentHash>(value.clone()).ok())
                    .unwrap_or_else(|| evidence.artifact_id.0.clone());
                entry.1.insert(cluster);
            }
        }

        // into_iter 消费临时 grouped map；只有恰好一个能力 hash 时 then 闭包才取其值，
        // 否则把不一致身份记录为 None。证据 cluster 集合整体移入 contribution。
        let contributions = grouped
            .into_iter()
            .map(
                |(agent_id, (capability_hashes, evidence_clusters))| AgentEvidenceContribution {
                    agent_id,
                    capability_snapshot_hash: (capability_hashes.len() == 1).then(|| {
                        capability_hashes
                            .into_iter()
                            .next()
                            .expect("one capability hash")
                    }),
                    evidence_clusters,
                },
            )
            .collect::<Vec<_>>();
        // Domain 按来源 overlap 阈值计算独立性；此处先记录 raw/raw，decide 随后再写入
        // 根据相关性调整后的有效置信度。
        let mut assessment = ConsensusDiversityAssessment::from_contributions(
            &contributions,
            MAXIMUM_CONSENSUS_SOURCE_OVERLAP_PPM,
        )?;
        assessment.record_confidence(raw_confidence_ppm, raw_confidence_ppm)?;
        Ok(assessment)
    }

    fn effective_consensus_confidence_ppm(
        &self,
        assessment: &ConsensusDiversityAssessment,
        raw_confidence_ppm: u32,
    ) -> u32 {
        // 相关 consensus 将置信度压到最低门槛，部分证据重叠按 cluster/participant 比例缩放；
        // 这是 Decision 侧的审计调整，不是模型重新调用。
        // 多参与者但非独立时把 raw confidence 上限压到 policy minimum；部分重叠则按
        // unique cluster/participant 比例缩放，并夹在 minimum 与原始值之间。
        if assessment.participant_count > 1 && !assessment.independent {
            raw_confidence_ppm.min(self.policy.min_confidence_ppm)
        } else if assessment.participant_count > 1
            && assessment.unique_evidence_clusters < assessment.participant_count
        {
            let cluster_ratio_ppm = (assessment.unique_evidence_clusters as u64 * 1_000_000)
                / (assessment.participant_count as u64);
            let scaled_confidence =
                ((raw_confidence_ppm as u64) * cluster_ratio_ppm / 1_000_000) as u32;
            scaled_confidence
                .max(self.policy.min_confidence_ppm)
                .min(raw_confidence_ppm)
        } else {
            raw_confidence_ppm
        }
    }

    fn model_stage_latency_millis(
        &self,
        proposal: &Artifact,
        claim_records: &[(Artifact, ResearchClaim)],
    ) -> DecisionGateResult<Option<u64>> {
        // 合并 proposal 与 Claim 的 AgentTurn 引用并去重，只有每个 turn 都有 telemetry 才汇总
        // latency；缺任一项返回 None，不以平均值补齐。
        // proposal 与 Claim 来源中 AgentTurn 去重成 BTreeSet；clone 复制引用身份，不复制
        // AgentTurn 正文；最终只读取 telemetry，不把模型返回内容用于决策数据。
        let mut turns = proposal
            .source_refs
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::AgentTurn)
            .cloned()
            .collect::<BTreeSet<_>>();
        for (claim, _) in claim_records {
            turns.extend(
                claim
                    .source_refs
                    .iter()
                    .filter(|reference| reference.kind == ArtifactKind::AgentTurn)
                    .cloned(),
            );
        }
        self.complete_turn_latency_millis(&turns)
    }

    fn critic_stage_latency_millis(
        &self,
        critique_records: &[(Artifact, ResearchCritique)],
    ) -> DecisionGateResult<Option<u64>> {
        // 从 Critique source_refs 收集独立 turn 延迟，语义与 model stage 相同。
        // flat_map 遍历每个 Critique 的来源引用，再筛出 AgentTurn 并去重；空集合由下层返回 None。
        let turns = critique_records
            .iter()
            .flat_map(|(critique, _)| critique.source_refs.iter())
            .filter(|reference| reference.kind == ArtifactKind::AgentTurn)
            .cloned()
            .collect::<BTreeSet<_>>();
        self.complete_turn_latency_millis(&turns)
    }

    fn complete_turn_latency_millis(
        &self,
        turns: &BTreeSet<ArtifactRef>,
    ) -> DecisionGateResult<Option<u64>> {
        // 逐个读取 AgentTurn response.telemetry，累加时使用 saturating_add；缺少完整遥测
        // 只影响审计字段，不阻断已经合法的 Decision。
        // `turns` 是共享借用的有序引用集合；空集合或任何一项缺 latency 都返回 Ok(None)，
        // Store/JSON 读取错误则仍经 `?` 返回 Err。
        if turns.is_empty() {
            return Ok(None);
        }
        let mut total = 0_u64;
        for turn_ref in turns {
            let turn = self.load_expected(turn_ref, ArtifactKind::AgentTurn)?;
            let payload: serde_json::Value =
                serde_json::from_slice(&self.store.read_blob(&turn.blob)?)?;
            // let-else 显式区分 telemetry 缺失与有效 u64；缺一项即不报部分累计数。
            let Some(latency) = payload
                .get("response")
                .and_then(|value| value.get("telemetry"))
                .and_then(|value| value.get("latency_millis"))
                .and_then(serde_json::Value::as_u64)
            else {
                return Ok(None);
            };
            total = total.saturating_add(latency);
        }
        Ok(Some(total))
    }

    fn validate_proposal(
        &self,
        proposal: &Artifact,
        permit: &TaskWritePermit,
    ) -> DecisionGateResult<akzio_domain::ContentHash> {
        // Proposal 必须是本 run 的 agent.research.synthesizer RunScoped Artifact，并把其
        // contract hash 同时绑定到 provenance/origin，阻断脱离当前任务的终稿。
        // origin/contract_hash 是 Option 借用；任何缺失或身份不吻合都返回专用 provenance 错误，
        // 完整匹配时 clone hash 作为后续 Manifest/Contract 查找键。
        let Some(origin) = proposal.origin.as_ref() else {
            return Err(DecisionGateError::InvalidProposalProvenance);
        };
        let Some(contract_hash) = origin.contract_hash.as_ref() else {
            return Err(DecisionGateError::InvalidProposalProvenance);
        };
        if proposal.lifecycle != ArtifactLifecycle::RunScoped
            || proposal.producer != "agent.research.synthesizer"
            || proposal.provenance.source_family != "akzio.agent"
            || proposal.provenance.producer_contract_hash.as_ref() != Some(contract_hash)
            || origin.run_id.as_ref() != Some(&permit.run_id)
            || origin.task_id.is_none()
            || origin.attempt_id.is_none()
        {
            return Err(DecisionGateError::InvalidProposalProvenance);
        }
        Ok(contract_hash.clone())
    }
}

fn research_slot_supported(
    asset: Asset,
    horizon: DecisionHorizon,
    references: &[ArtifactRef],
    claims: &[(Artifact, ResearchClaim)],
    critiques: &[(Artifact, ResearchCritique)],
) -> bool {
    // 判断某资产/horizon 的研究分配是否有 bullish Claim、price+macro directional grounds、
    // 支持该 Claim 的 Critique，以及 references 闭包；新闻可以补充背景，但不能替代这两类
    // 直接方向依据。
    // `any` 在找到首个完整匹配时短路并返回 true；claim_records、critiques 与 references
    // 都只被共享借用，不会因资格查询而移动或改写证据。
    claims.iter().any(|(artifact, claim)| {
        if claim.horizon != horizon
            || claim.stance != akzio_domain::ClaimStance::Bullish
            || claim
                .evidence_gaps
                .iter()
                .any(|gap| gap.blocks_slot(asset, horizon, claim.horizon))
        {
            // 当前 Claim 的期限/立场不匹配，或有覆盖目标 asset+horizon 的阻断缺口，
            // 就只跳过此 Claim，继续看其它 claim。
            return false;
        }
        // 同一 Claim 必须对目标资产包含 price 与 macro 两种 directional ground；
        // 新闻/背景引用不能代替任一直接方向域。
        let domains = claim
            .grounds
            .iter()
            .filter(|ground| {
                ground.role == akzio_domain::EvidenceGroundRole::Directional
                    && ground.assets.contains(&asset)
            })
            .filter_map(|ground| ground.domain)
            .collect::<BTreeSet<_>>();
        if ![
            akzio_domain::ResearchShard::PriceMarketStructure,
            akzio_domain::ResearchShard::Macro,
        ]
        .into_iter()
        .all(|domain| domains.contains(&domain))
        {
            return false;
        }
        // 构造与 Artifact kind 一致的 Claim 引用，再要求至少一条 Critique 正式支持且
        // 未阻断该 slot；references 还必须闭合到 Claim、该 Critique 或方向 grounds 之一。
        let claim_ref = ArtifactRef {
            artifact_id: artifact.artifact_id.clone(),
            kind: ArtifactKind::Claim,
        };
        critiques.iter().any(|(critique_artifact, critique)| {
            critique.target == claim_ref
                && critique.verification_status == ClaimVerificationStatus::Supported
                && !critique.blocks_slot(asset, horizon, claim.horizon)
                && references.iter().any(|reference| {
                    *reference == claim_ref
                        || (reference.kind == ArtifactKind::Critique
                            && reference.artifact_id == critique_artifact.artifact_id)
                        || claim.grounds.iter().any(|ground| {
                            ground.role == akzio_domain::EvidenceGroundRole::Directional
                                && ground.assets.contains(&asset)
                                && ground.evidence == *reference
                        })
                })
        })
    })
}

fn has_unverified_critical_claim(
    claim_refs: &[ArtifactRef],
    claims: &[ResearchClaim],
    critiques: &[ResearchCritique],
    forecasts: &[Forecast],
) -> bool {
    // 只对高 materiality 且真正支持非中性 forecast 的 Claim 要求恰好一个 Supported Critique；
    // 缺失、重复、Contradicted 或 slot blocker 都会让 Decision 保留 UnverifiedClaim。
    // zip 按位置配对引用与 payload（由调用方按同一 draft 顺序建立）；any 找到一个关键
    // 未验证 Claim 就返回 true，非关键 Claim 不触发这个专门 blocker。
    claim_refs
        .iter()
        .zip(claims.iter())
        .any(|(claim_ref, claim)| {
            if claim.materiality_ppm < CRITICAL_CLAIM_MATERIALITY_PPM {
                return false;
            }
            // 对该 Claim 的所有 Critique 计数：零条或两条以上都不满足“唯一权威核验”。
            let mut matching = critiques
                .iter()
                .filter(|critique| critique.target == *claim_ref);
            let Some(verification) = matching.next() else {
                return true;
            };
            if matching.next().is_some()
                || verification.verification_status != ClaimVerificationStatus::Supported
            {
                return true;
            }
            // 只检查该 Claim 真正支持的非中性同 horizon forecast；审查 blocker 若覆盖
            // 对应 asset/horizon 才会使关键 Claim 判为未验证。
            forecasts
                .iter()
                .filter(|forecast| {
                    !forecast.is_neutral()
                        && forecast.horizon == claim.horizon
                        && claim.grounds.iter().any(|ground| {
                            ground.role == akzio_domain::EvidenceGroundRole::Directional
                                && ground.assets.contains(&forecast.asset)
                        })
                })
                .any(|forecast| {
                    verification.blocks_slot(forecast.asset, forecast.horizon, claim.horizon)
                })
        })
}

#[allow(clippy::too_many_arguments)]
fn build_asset_eligibility(
    policy: &DecisionPolicy,
    decision_at: DateTime<Utc>,
    confidence_ppm: u32,
    forecasts: &[Forecast],
    claims: &[(Artifact, ResearchClaim)],
    critiques: &[(Artifact, ResearchCritique)],
    horizon_trace: &HorizonDecisionTrace,
    portfolio_risk: &PortfolioRiskAssessment,
    target: &TargetPortfolio,
) -> BTreeMap<Asset, AssetEligibility> {
    // 为每个可执行资产汇总方向 forecast、Claim/Critique、horizon conflict、校准与最终 target，
    // 形成“为何可/不可进入目标”的只读投影；它不重新计算或修改 target。
    // map 闭包对四个允许资产分别构造独立资格快照，collect 再汇成有序 BTreeMap；
    // 该投影不修改传入 target，也不从研究建议创建执行订单。
    Asset::EXECUTABLE
        .into_iter()
        .map(|asset| {
            // neutral forecast 是显式弃权，先从方向候选中排除；无候选时原因是 NoDirectionalSignal。
            let active_forecasts = forecasts
                .iter()
                .filter(|forecast| forecast.asset == asset && !forecast.is_neutral())
                .collect::<Vec<_>>();
            let directional_forecast_present = !active_forecasts.is_empty();
            // claim_verified 要求每一个活跃 forecast 都有支持相应 stance/horizon 的 Claim，
            // 且存在针对该 Claim 的 Supported、未阻断 Critique。
            let claim_verified = directional_forecast_present
                && active_forecasts.iter().all(|forecast| {
                    claims.iter().any(|(artifact, claim)| {
                        claim.horizon == forecast.horizon
                            && forecast.supported_by_stance(claim.stance)
                            && critiques.iter().any(|(_, critique)| {
                                critique.target
                                    == ArtifactRef {
                                        artifact_id: artifact.artifact_id.clone(),
                                        kind: ArtifactKind::Claim,
                                    }
                                    && critique.verification_status
                                        == ClaimVerificationStatus::Supported
                                    && !critique.blocks_slot(asset, forecast.horizon, claim.horizon)
                            })
                    })
                });
            // directional_evidence 再查每个活跃期限的完整方向 grounds；这里的摘要字段
            // 要求该资产同时出现 Price、Macro、NewsEvent 三个 shard。
            let directional_evidence = directional_forecast_present
                && active_forecasts.iter().all(|forecast| {
                    let domains = claims
                        .iter()
                        .filter(|(artifact, claim)| {
                            claim.horizon == forecast.horizon
                                && forecast.supported_by_stance(claim.stance)
                                && !claim.evidence_gaps.iter().any(|gap| {
                                    gap.blocks_slot(asset, forecast.horizon, claim.horizon)
                                })
                                && critiques.iter().any(|(_, critique)| {
                                    critique.target
                                        == ArtifactRef {
                                            artifact_id: artifact.artifact_id.clone(),
                                            kind: ArtifactKind::Claim,
                                        }
                                        && critique.verification_status
                                            == ClaimVerificationStatus::Supported
                                        && !critique.blocks_slot(
                                            asset,
                                            forecast.horizon,
                                            claim.horizon,
                                        )
                                })
                        })
                        .flat_map(|(_, claim)| &claim.grounds)
                        .filter(|ground| {
                            ground.role == akzio_domain::EvidenceGroundRole::Directional
                                && ground.assets.contains(&asset)
                        })
                        .filter_map(|ground| ground.domain)
                        .collect::<BTreeSet<_>>();
                    [
                        akzio_domain::ResearchShard::PriceMarketStructure,
                        akzio_domain::ResearchShard::Macro,
                        akzio_domain::ResearchShard::NewsEvent,
                    ]
                    .into_iter()
                    .all(|domain| domains.contains(&domain))
                });

            // 资产风险校准与 forecast 校准分开判断：前者看样本/Brier，后者逐项查 active
            // scope、冻结时间、校准 bin；有方向预测时两类资格都必须成立。
            let risk_calibration = policy.asset_calibrations.get(&asset);
            let insufficient_samples = risk_calibration.is_some_and(|calibration| {
                calibration.sample_count < policy.min_calibration_samples
            });
            let risk_calibration_ok = risk_calibration.is_some_and(|calibration| {
                calibration.sample_count >= policy.min_calibration_samples
                    && calibration.brier_score_ppm <= policy.max_brier_score_ppm
            });
            let forecast_calibration_ok = directional_forecast_present
                && forecasts
                    .iter()
                    .filter(|forecast| forecast.asset == asset && !forecast.is_neutral())
                    .all(|forecast| policy.calibrated_forecast(decision_at, forecast).is_some());
            let calibration = risk_calibration_ok && forecast_calibration_ok;

            // 风险可知还要求组合风险模型样本、损失上限、该资产正对角 covariance，以及
            // 本次 target 风险 hash（除非最终全零目标）。
            let risk = risk_calibration_ok
                && policy.portfolio_risk_model.sample_count >= policy.min_calibration_samples
                && policy.portfolio_risk_model.max_expected_shortfall_ppm > 0
                && policy.portfolio_risk_model.max_gap_loss_ppm > 0
                && policy
                    .portfolio_risk_model
                    .covariance_ppm_squared
                    .get(&asset)
                    .and_then(|row| row.get(&asset))
                    .is_some_and(|value| *value > 0)
                && (portfolio_risk.risk_model_hash.is_some()
                    || target.weights.values().all(|weight| weight.0 == 0));

            // reasons 收集彼此独立的证据、校准、风险、信心和 horizon 原因；最终排序去重，
            // eligibility 还要求 Rust 目标权重本身非零。
            let mut reasons = Vec::new();
            if !directional_forecast_present {
                reasons.push(DecisionEligibilityReason::NoDirectionalSignal);
            } else {
                if !directional_evidence {
                    reasons.push(DecisionEligibilityReason::MissingEvidence);
                }
                if !claim_verified {
                    reasons.push(DecisionEligibilityReason::UnverifiedClaim);
                }
                if risk_calibration.is_none() || !forecast_calibration_ok {
                    reasons.push(DecisionEligibilityReason::MissingCalibration);
                }
                if insufficient_samples {
                    reasons.push(DecisionEligibilityReason::InsufficientCalibrationSamples);
                }
            }
            if !risk {
                reasons.push(DecisionEligibilityReason::RiskUnknown);
            }
            if confidence_ppm < policy.min_confidence_ppm {
                reasons.push(DecisionEligibilityReason::ConfidenceTooLow);
            }
            if horizon_trace
                .conflicts
                .iter()
                .any(|conflict| conflict.asset == asset)
            {
                reasons.push(DecisionEligibilityReason::HorizonConflict);
            }
            if directional_forecast_present
                && target
                    .weights
                    .get(&asset)
                    .is_none_or(|weight| weight.0 == 0)
                && reasons.is_empty()
            {
                reasons.push(DecisionEligibilityReason::NoDirectionalSignal);
            }
            reasons.sort();
            reasons.dedup();
            let eligible = target
                .weights
                .get(&asset)
                .is_some_and(|weight| weight.0 > 0)
                && reasons.is_empty();
            (
                asset,
                AssetEligibility {
                    directional_evidence,
                    claim_verified,
                    calibration,
                    risk,
                    eligible,
                    reasons,
                },
            )
        })
        .collect()
}
