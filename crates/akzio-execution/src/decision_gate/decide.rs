// 文件导读：本文件实现 DecisionRuntime 的主事务。它从 proposal 沿 Manifest 递归读取
// selected/quarantined/ancestor 闭包，加载 Claim/Critique/Review 与 learning attribution，
// 再调用 DecisionPolicy 生成 horizon trace、研究分配复核、目标和风险；最终按 Paper 或
// PositionPlan 选择 Artifact lifecycle，并一次性提交 DecisionContext/Decision。所有
// iterator/closure 只在内存中汇总已持久化证据，不能新增证据、改变 Prompt 或越过 Execution。

impl DecisionRuntime {
    pub fn new(store: Store, policy: DecisionPolicy) -> DecisionGateResult<Self> {
        // 入口先校验整份 DecisionPolicy，保证后续 decide 使用的是一个完整的冻结策略。
        policy.validate()?;
        Ok(Self { store, policy })
    }

    pub fn policy(&self) -> &DecisionPolicy {
        // 只读访问策略，供上层展示或测试，不允许在一次 Decision 中间更换校准身份。
        &self.policy
    }

    /// Validate, bind, and atomically complete the DecisionGate attempt.
    pub fn decide(&self, input: &DecisionGateInput) -> DecisionGateResult<DecisionGateOutput> {
        // 顺序是 permit/proposal→Manifest 闭包→draft/Claim/Critique 语义→Review/learning
        // 归因→研究计划裁剪→horizon/校准/risk→Decision artifacts→Store 原子提交。任何
        // 失败都停在提交前；Decision 成功也只表示正式决策产物已绑定，不表示 Execution 或成交。
        let decision_gate_started = std::time::Instant::now();
        self.store.validate_task_permit(&input.permit)?;

        let proposal = self.load_expected(&input.proposal, ArtifactKind::DecisionProposal)?;
        let proposal_contract = self.validate_proposal(&proposal, &input.permit)?;
        let manifest_ref = unique_manifest_ref(&proposal)?;
        let manifest = self.load_expected(manifest_ref, ArtifactKind::ContextManifest)?;
        let selected =
            self.validate_manifest(&manifest, &proposal, &proposal_contract, &input.permit)?;

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
        let installed = self
            .store
            .contract_installation(&proposal_contract)?
            .ok_or(DecisionGateError::UnsupportedProposalContract)?;
        if installed.contract.version < akzio_domain::DIRECTION_BOUND_RESEARCH_CONTRACT_VERSION {
            return Err(DecisionGateError::UnsupportedProposalContract);
        }
        if installed.contract.version >= akzio_domain::REVIEWED_RESEARCH_CONTRACT_VERSION {
            akzio_domain::validate_numeric_bases(&draft.numeric_basis)?;
            let (_, review) = self.store.final_proposal_review(&input.permit.run_id, &input.permit.task_id)?
                .ok_or(DecisionGateError::ProposalReviewRequired)?;
            if !review.authorizes(&input.proposal, &proposal.blob.hash) {
                return Err(DecisionGateError::ProposalReviewRequired);
            }
        }
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
        let claims = claim_records
            .iter()
            .map(|(_, claim)| claim.clone())
            .collect::<Vec<_>>();
        if installed.contract.version < akzio_domain::STRUCTURED_RESEARCH_CONTRACT_VERSION {
            validate_decision_evidence_sufficiency(&draft, &claims)
                .map_err(|_| DecisionGateError::InsufficientClaimEvidence)?;
        }

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
        let critiques = critique_records
            .iter()
            .map(|(_, critique)| critique.clone())
            .collect::<Vec<_>>();
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
        let research_plan = self.review_research_plan(
            raw_research_plan,
            &draft.forecasts,
            &claim_records,
            &critique_records,
            &input.permit.run_id,
        )?;
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
        let active_refs = active_claims
            .iter()
            .map(|(reference, _)| (*reference).clone())
            .collect::<Vec<_>>();
        let active_payloads = active_claims
            .iter()
            .map(|(_, claim)| (*claim).clone())
            .collect::<Vec<_>>();
        let critical_claim_unverified = has_unverified_critical_claim(
            &active_refs,
            &active_payloads,
            &critiques,
            &draft.forecasts,
        );

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
        let evidence_cutoff = selected
            .iter()
            .filter_map(|reference| self.store.artifact(&reference.artifact_id).ok())
            .filter_map(|artifact| artifact.provenance.observed_at)
            .filter(|observed_at| *observed_at <= input.now)
            .max()
            .unwrap_or(input.now);
        let policy_valid_until = input.now
            + chrono::Duration::milliseconds(
                i64::try_from(self.policy.maximum_execution_delay_ms).map_err(|_| {
                    DomainError::InvalidBudget {
                        field: "decision_policy.maximum_execution_delay_ms",
                    }
                })?,
            );
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
        let stage_latencies = DecisionStageLatencies {
            retrieval_latency_millis: None,
            model_latency_millis: self.model_stage_latency_millis(&proposal, &claim_records)?,
            critic_latency_millis: self.critic_stage_latency_millis(&critique_records)?,
            decision_gate_latency_millis: Some(
                u64::try_from(decision_gate_started.elapsed().as_millis()).unwrap_or(u64::MAX),
            ),
        };
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

        let lifecycle = match self.store.run_purpose(&input.permit.run_id)? {
            RunPurpose::Paper => ArtifactLifecycle::Canonical,
            _ => ArtifactLifecycle::RunScoped,
        };
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
        raw.validate()?;
        let mut validated = raw.clone();
        let mut reasons_by_asset = BTreeMap::<Asset, Vec<String>>::new();

        for allocation in &mut validated.allocations {
            if allocation.target_weight_ppm.0 == 0 {
                continue;
            }
            let original_horizons = allocation.supporting_horizons.clone();
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
                reasons_by_asset
                    .entry(allocation.asset)
                    .or_default()
                    .push("unsupported_research_horizon_removed".to_owned());
                allocation.supporting_horizons = supported_horizons;
            }
            if allocation.supporting_horizons.is_empty() {
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
            adjustments.push(ResearchPlanAdjustment {
                asset: None,
                from_weight_ppm: raw.cash_weight_ppm,
                to_weight_ppm: validated.cash_weight_ppm,
                reasons: vec!["cash_recomputed_after_rust_validation".to_owned()],
            });
        }

        validated.validate()?;
        let status = if validated.has_non_zero_target() {
            ResearchPlanStatus::QualifiedRecommendation
        } else if raw.has_non_zero_target() {
            ResearchPlanStatus::BlockedByResearch
        } else {
            ResearchPlanStatus::ExplicitCash
        };
        let purpose = self.store.run_purpose(run_id)?;
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
        let mut grouped = BTreeMap::<String, (BTreeSet<ContentHash>, BTreeSet<ContentHash>)>::new();
        for (claim_artifact, claim) in claim_records {
            let agent_id = claim_artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.task_id.as_ref())
                .map(|task_id| task_id.0.clone())
                .unwrap_or_else(|| format!("claim:{}", claim_artifact.artifact_id));
            let entry = grouped.entry(agent_id).or_default();
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
        if turns.is_empty() {
            return Ok(None);
        }
        let mut total = 0_u64;
        for turn_ref in turns {
            let turn = self.load_expected(turn_ref, ArtifactKind::AgentTurn)?;
            let payload: serde_json::Value =
                serde_json::from_slice(&self.store.read_blob(&turn.blob)?)?;
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
    claims.iter().any(|(artifact, claim)| {
        if claim.horizon != horizon
            || claim.stance != akzio_domain::ClaimStance::Bullish
            || claim
                .evidence_gaps
                .iter()
                .any(|gap| gap.blocks_slot(asset, horizon, claim.horizon))
        {
            return false;
        }
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
    claim_refs
        .iter()
        .zip(claims.iter())
        .any(|(claim_ref, claim)| {
            if claim.materiality_ppm < CRITICAL_CLAIM_MATERIALITY_PPM {
                return false;
            }
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
    Asset::EXECUTABLE
        .into_iter()
        .map(|asset| {
            let active_forecasts = forecasts
                .iter()
                .filter(|forecast| forecast.asset == asset && !forecast.is_neutral())
                .collect::<Vec<_>>();
            let directional_forecast_present = !active_forecasts.is_empty();
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
