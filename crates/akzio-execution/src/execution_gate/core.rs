impl ExecutionRuntime {
    pub fn new(
        store: Store,
        execution_policy: ExecutionPolicy,
        gate_policy: ExecutionGatePolicy,
    ) -> ExecutionGateResult<Self> {
        let allocation = AllocationRuntime::new(execution_policy)
            .map_err(|_| ExecutionGateError::Integrity("execution policy"))?;
        gate_policy.validate()?;
        let pretrade_safety = PreTradeSafetyPolicy {
            capacity: gate_policy.capacity.clone(),
            compliance: gate_policy.compliance.clone(),
        };
        Ok(Self {
            store,
            allocation,
            gate_policy,
            pretrade_safety,
        })
    }

    pub fn execution_policy(&self) -> &ExecutionPolicy {
        self.allocation.policy()
    }

    pub fn gate_policy(&self) -> &ExecutionGatePolicy {
        &self.gate_policy
    }

    pub fn evaluate(&self, input: &ExecutionGateInput) -> ExecutionGateResult<ExecutionGateOutput> {
        self.validate_input(input)?;
        let purpose = self.store.run_purpose(&input.permit.run_id)?;
        let decision_artifact =
            self.load_expected(&input.decision_context, ArtifactKind::DecisionContext)?;
        let decision: DecisionContext = self.read_payload(&decision_artifact)?;
        decision.validate()?;
        if decision.run_id != input.permit.run_id {
            return Err(ExecutionGateError::DecisionRunMismatch);
        }
        self.validate_decision_provenance(&decision_artifact, &decision)?;
        let decision_process_quality = decision
            .investment_logic
            .as_ref()
            .map(|trace| trace.quality.clone());
        let decision_valid_at_execution = decision
            .validity
            .as_ref()
            .is_some_and(|validity| validity.is_valid_at(input.now));

        let mut blockers = decision
            .hard_blockers
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if !decision_valid_at_execution {
            blockers.insert(HardBlocker::StaleDecision);
        }
        if decision.horizon_trace.is_none() || decision.investment_logic.is_none() {
            blockers.insert(HardBlocker::InvalidProvenance);
        }
        if !decision.material_conflicts.is_empty() {
            blockers.insert(HardBlocker::MaterialConflict);
        }
        if purpose != RunPurpose::Paper {
            blockers.insert(HardBlocker::NonCanonicalRun);
        } else if let Some((manifest, approval)) =
            self.store.paper_approval_for_run(&input.permit.run_id)?
        {
            let qualification_key = manifest.runtime_identity().qualification_key()?;
            if !ModelQualificationGate::permits(
                &qualification_key,
                &approval.qualification,
                input.now,
            ) {
                blockers.insert(HardBlocker::UnqualifiedRuntime);
            }
            if approval.mandate.identity_hash() != self.gate_policy.mandate.identity_hash() {
                blockers.insert(HardBlocker::MandateViolation);
            }
        } else {
            blockers.insert(HardBlocker::UnqualifiedRuntime);
        }
        let frozen = self.frozen()?;
        if frozen {
            blockers.insert(HardBlocker::Frozen);
        }

        let account = self.load_account(input, &mut blockers)?;
        let quotes = self.load_quotes(
            input,
            &mut blockers,
            input.quote_validation_error.is_some(),
        )?;
        if input.quote_validation_error.is_some() {
            blockers.insert(HardBlocker::InvalidQuote);
        }
        let clock = self.load_clock(input, &mut blockers)?;
        self.derive_snapshot_blockers(
            account.as_ref().map(|(_, payload)| payload),
            quotes.as_ref().map(|(_, payload)| payload),
            clock.as_ref().map(|(_, payload)| payload),
            input.now,
            &mut blockers,
        );

        let mut plan_payload = None;
        let mut mandate_assessment = None;
        let mut pretrade_safety = None;
        if blockers.is_empty() {
            let (_, account_payload) = account
                .as_ref()
                .ok_or(ExecutionGateError::Integrity("account snapshot closure"))?;
            let (_, quote_payload) = quotes
                .as_ref()
                .ok_or(ExecutionGateError::Integrity("quote snapshot closure"))?;
            let (_, clock_payload) = clock
                .as_ref()
                .ok_or(ExecutionGateError::Integrity("clock snapshot closure"))?;
            let maximum_total_notional = if purpose == RunPurpose::Paper {
                self.store
                    .paper_approval_for_run(&input.permit.run_id)?
                    .map_or(MoneyMicros::ZERO, |(manifest, _)| manifest.maximum_notional)
            } else {
                self.allocation.policy().max_new_notional
            };
            let projected_drawdown_ppm = decision
                .portfolio_risk
                .expected_shortfall_ppm
                .unwrap_or_default();
            let allocation = self.allocation.allocate_with_limit(
                &AllocationInput {
                    decision_context_ref: input.decision_context.clone(),
                    decision_context: decision,
                    account_snapshot_ref: input.account_snapshot.clone().expect("checked above"),
                    account: account_payload.clone(),
                    quote_snapshot_ref: input.quote_snapshot.clone().expect("checked above"),
                    quotes: quote_payload.clone(),
                    market_clock_snapshot_ref: input
                        .market_clock_snapshot
                        .clone()
                        .expect("checked above"),
                    clock: clock_payload.clone(),
                    now: input.now,
                },
                maximum_total_notional,
            );
            match allocation {
                Ok(plan) => {
                    plan.validate()?;
                    blockers.extend(
                        self.gate_policy
                            .blockers_for(&plan.factor_exposure, plan.turnover_ppm),
                    );
                    let mandate = self.gate_policy.assess_mandate(
                        account_payload,
                        &plan.target,
                        projected_drawdown_ppm,
                        plan.turnover_ppm,
                    );
                    if !mandate.permitted() {
                        blockers.insert(HardBlocker::MandateViolation);
                    }
                    mandate_assessment = Some(mandate);
                    if let Some(evidence) = &input.pretrade_safety {
                        let ordered_assets = plan
                            .orders
                            .iter()
                            .map(|order| order.asset)
                            .collect::<BTreeSet<_>>();
                        let risk_increasing = plan
                            .orders
                            .iter()
                            .any(|order| order.side == crate::OrderSide::Buy);
                        let assessment = self.pretrade_safety.assess(&PreTradeSafetyInput {
                            target: plan.target.clone(),
                            ordered_assets,
                            risk_increasing,
                            account_equity: account_payload.equity,
                            homogeneous_agent_count: evidence.homogeneous_agent_count,
                            average_daily_dollar_volume: evidence
                                .average_daily_dollar_volume
                                .clone(),
                            information_classifications: evidence
                                .information_classifications
                                .clone(),
                            recent_compliance_activity: evidence.recent_compliance_activity.clone(),
                            dependency_closure: evidence.dependency_closure.clone(),
                            now: input.now,
                        })?;
                        if assessment.risk_increasing
                            && (assessment.capacity_scenario.is_none()
                                || assessment
                                    .capacity_scenario
                                    .as_ref()
                                    .is_some_and(|scenario| !scenario.within_capacity))
                        {
                            blockers.insert(HardBlocker::CapacityLimit);
                        }
                        if !assessment.missing_information_classifications.is_empty()
                            || assessment
                                .compliance_violations_by_asset
                                .values()
                                .any(|violations| !violations.is_empty())
                        {
                            blockers.insert(HardBlocker::ComplianceViolation);
                        }
                        if (assessment.risk_increasing && !assessment.dependency_permits_new_risk)
                            || (!assessment.risk_increasing
                                && !assessment.dependency_permits_risk_reduction)
                        {
                            blockers.insert(HardBlocker::DependencyDegraded);
                        }
                        pretrade_safety = Some(assessment);
                    } else {
                        blockers.extend([
                            HardBlocker::CapacityLimit,
                            HardBlocker::ComplianceViolation,
                            HardBlocker::DependencyDegraded,
                        ]);
                    }
                    plan_payload = Some(plan);
                }
                Err(error) => self.allocation_blockers(error, &mut blockers),
            }
        }

        let execution_plan = plan_payload
            .as_ref()
            .map(|plan| {
                self.artifact(
                    ArtifactKind::ExecutionPlan,
                    "execution.plan",
                    plan,
                    vec![
                        plan.decision_context.clone(),
                        plan.account_snapshot.clone(),
                        plan.quote_snapshot.clone(),
                        plan.market_clock_snapshot.clone(),
                    ],
                    input,
                )
            })
            .transpose()?;
        let execution_plan_ref = execution_plan.as_ref().map(artifact_ref);
        let portfolio_consistent = plan_payload.is_some()
            && !blockers.contains(&HardBlocker::FactorLimit)
            && !blockers.contains(&HardBlocker::PairExposureLimit)
            && !blockers.contains(&HardBlocker::TurnoverLimit);
        let action_feasible = plan_payload.is_some()
            && input.account_snapshot.is_some()
            && input.quote_snapshot.is_some()
            && input.market_clock_snapshot.is_some()
            && decision_valid_at_execution
            && pretrade_safety
                .as_ref()
                .is_some_and(|assessment| assessment.permits_execution)
            && blockers.is_empty();
        let final_process_quality = decision_process_quality.and_then(|quality| {
            let mandate = mandate_assessment.as_ref()?;
            quality.execution_finalized(
                if mandate.permitted() {
                    akzio_domain::WeightPpm::SCALE
                } else {
                    0
                },
                if portfolio_consistent {
                    akzio_domain::WeightPpm::SCALE
                } else {
                    0
                },
                if action_feasible {
                    akzio_domain::WeightPpm::SCALE
                } else {
                    0
                },
            )
        });

        let execution_context_payload = ExecutionContext {
            schema_version: akzio_domain::DOMAIN_SCHEMA_VERSION,
            run_id: input.permit.run_id.clone(),
            decision_context: input.decision_context.clone(),
            account_snapshot: input.account_snapshot.clone(),
            quote_snapshot: input.quote_snapshot.clone(),
            market_clock_snapshot: input.market_clock_snapshot.clone(),
            execution_plan: execution_plan_ref.clone(),
            factor_exposure: plan_payload
                .as_ref()
                .map(|plan| plan.factor_exposure.clone()),
            turnover_ppm: plan_payload.as_ref().map(|plan| plan.turnover_ppm),
            plan_hash: plan_payload.as_ref().map(|plan| plan.plan_hash.clone()),
            broker_session: plan_payload
                .as_ref()
                .map(|plan| plan.broker_session.clone()),
            mandate_assessment,
            pretrade_safety,
            final_process_quality,
            frozen,
            created_at: input.now,
        };
        execution_context_payload.validate()?;
        if blockers.is_empty() {
            execution_context_payload.validate_complete_plan_closure()?;
        }

        let mut context_sources = vec![input.decision_context.clone()];
        context_sources.extend(input.account_snapshot.clone());
        context_sources.extend(input.quote_snapshot.clone());
        context_sources.extend(input.market_clock_snapshot.clone());
        context_sources.extend(execution_plan_ref);
        let execution_context = self.artifact(
            ArtifactKind::ExecutionContext,
            "execution.context",
            &execution_context_payload,
            context_sources,
            input,
        )?;
        let execution_context_ref = artifact_ref(&execution_context);

        let verdict_payload = if blockers.is_empty() {
            ExecutionVerdict::Accepted {
                execution_context: execution_context_ref.clone(),
            }
        } else {
            ExecutionVerdict::NoOrder {
                no_order: NoOrder {
                    execution_context: execution_context_ref.clone(),
                    blockers: blockers.into_iter().collect(),
                    created_at: input.now,
                },
            }
        };
        verdict_payload.validate()?;
        let verdict = self.artifact(
            ArtifactKind::ExecutionVerdict,
            "execution.verdict",
            &verdict_payload,
            vec![execution_context_ref],
            input,
        )?;
        Ok(ExecutionGateOutput {
            execution_plan,
            execution_context,
            verdict,
        })
    }

    /// Atomically persists the optional plan, context, verdict and task terminal state.
    pub fn commit(
        &self,
        permit: &TaskWritePermit,
        output: &ExecutionGateOutput,
        now: DateTime<Utc>,
    ) -> ExecutionGateResult<()> {
        let mut artifacts = Vec::with_capacity(3);
        artifacts.extend(output.execution_plan.clone());
        artifacts.push(output.execution_context.clone());
        artifacts.push(output.verdict.clone());
        self.store
            .commit_attempt(permit, &artifacts, TaskStatus::Succeeded, now)?;
        Ok(())
    }

    fn validate_input(&self, input: &ExecutionGateInput) -> ExecutionGateResult<()> {
        if input.decision_context.kind != ArtifactKind::DecisionContext
            || input
                .account_snapshot
                .as_ref()
                .is_some_and(|reference| reference.kind != ArtifactKind::NormalizedEvidence)
            || input
                .quote_snapshot
                .as_ref()
                .is_some_and(|reference| reference.kind != ArtifactKind::NormalizedEvidence)
            || input
                .market_clock_snapshot
                .as_ref()
                .is_some_and(|reference| reference.kind != ArtifactKind::NormalizedEvidence)
        {
            return Err(ExecutionGateError::Integrity("input artifact kinds"));
        }
        Ok(())
    }
}
