// 文件导读：Canary completion 汇总父 Outcome 与 contract/topology/bundle 三个 Shadow
// Outcome，建立 paired observations，按 promotion policy 记录 evaluation 并推进 campaign
// verdict。它只影响 candidate 的 policy subject/Canary 状态，不能直接激活 Contract、拓扑、
// Lesson 或 Paper 权限；缺 narrative/Shadow outcome 会保留等待。
// Rust 机制：数组/迭代器按固定 subject 顺序配对；闭包 `metrics` 借用 Daemon 并返回
// `Result`；`Option` 区分 process quality unknown，lease/permit 通过 Store fencing 保证原子写入。

use super::*;

impl Daemon {
    pub(super) fn complete_canary_session(
        &self,
        lease: &DaemonLease,
        task: &ClaimedAttempt,
        session: &akzio_store::StoredCanarySession,
        parent_outcome_artifact: &Artifact,
        materialization: Option<&OutcomeMaterializationInput>,
        retrospective_draft: Option<&RetrospectiveDraft>,
    ) -> Result<bool> {
        // 先锁定 campaign/session level，再读取三个 Shadow sealed Outcome；任何 candidate
        // identity、cohort、process-quality 或 paired metric 不一致都阻断 promotion。
        let campaign = self
            .store
            .canary_campaign(&session.reservation.campaign_id)?
            .ok_or_else(|| {
                DaemonError::InvalidInput("canary campaign disappeared during outcome".to_owned())
            })?;
        if campaign.status != session.reservation.level || retrospective_draft.is_none() {
            // No narrative cannot populate process eligibility. A repair can retry
            // the same frozen cohort later while it remains active.
            self.store.finish_task(
                &task.permit,
                akzio_domain::TaskStatus::Succeeded,
                Utc::now(),
            )?;
            return Ok(true);
        }
        let draft = retrospective_draft.expect("checked narrative");

        let shadow_run_ids = [
            &session.reservation.contract_shadow_run_id,
            &session.reservation.topology_shadow_run_id,
            &session.reservation.bundle_shadow_run_id,
        ];
        let mut shadow_outcomes = Vec::with_capacity(shadow_run_ids.len());
        for run_id in shadow_run_ids {
            let Some(artifact) = self.store.outcome_for_run(run_id)? else {
                return Ok(false);
            };
            let reference = ArtifactRef {
                artifact_id: artifact.artifact_id.clone(),
                kind: ArtifactKind::Outcome,
            };
            let outcome: Outcome = self.read_artifact_payload(&reference)?;
            outcome.validate_sealed()?;
            let schedule: OutcomeSchedule = self.read_artifact_payload(&outcome.schedule)?;
            schedule.validate()?;
            shadow_outcomes.push((artifact, outcome, schedule));
        }

        let parent_outcome_ref = ArtifactRef {
            artifact_id: parent_outcome_artifact.artifact_id.clone(),
            kind: ArtifactKind::Outcome,
        };
        let parent_outcome: Outcome = self.read_artifact_payload(&parent_outcome_ref)?;
        parent_outcome.validate_sealed()?;
        let parent_schedule: OutcomeSchedule =
            self.read_artifact_payload(&parent_outcome.schedule)?;
        let completed_at = Utc::now();

        let candidate_contract_artifact = self
            .store
            .artifact(&campaign.spec.candidate_contract.artifact_id)?;
        let candidate_contract: AgentContract =
            self.read_artifact_payload(&campaign.spec.candidate_contract)?;
        candidate_contract.validate()?;
        if candidate_contract_artifact.kind != ArtifactKind::Contract
            || candidate_contract_artifact.lifecycle != ArtifactLifecycle::Canonical
            || candidate_contract.contract_hash == campaign.spec.active_contract_hash
        {
            return Err(DaemonError::InvalidInput(
                "canary candidate contract binding changed".to_owned(),
            ));
        }

        let candidate_topology_artifact = self
            .store
            .artifact(&campaign.spec.candidate_topology.artifact_id)?;
        let candidate_topology: WorkflowGraph =
            self.read_artifact_payload(&campaign.spec.candidate_topology)?;
        candidate_topology.validate()?;
        if candidate_topology_artifact.kind != ArtifactKind::WorkflowGraph
            || candidate_topology_artifact.lifecycle != ArtifactLifecycle::RunScoped
        {
            return Err(DaemonError::InvalidInput(
                "canary candidate topology binding changed".to_owned(),
            ));
        }

        let active_contract = self
            .store
            .active_contract(&ContractPurpose::new("research.analyst")?)?
            .ok_or_else(|| {
                DaemonError::Unavailable("active analyst contract missing".to_owned())
            })?;
        if active_contract.contract.contract_hash != campaign.spec.active_contract_hash {
            return Err(DaemonError::InvalidInput(
                "canary active contract binding changed".to_owned(),
            ));
        }
        let parent_snapshot = self.store.workflow_snapshot(&task.run_id)?;
        let parent_topology = ArtifactRef {
            artifact_id: parent_snapshot.revision.graph_artifact.artifact_id,
            kind: ArtifactKind::WorkflowGraph,
        };
        let candidate_contract_hash = candidate_contract.contract_hash;
        let candidate_topology_id = candidate_topology.topology_id;
        let contract_subject = PolicySubject::Contract(candidate_contract_hash.clone());
        let topology_subject = PolicySubject::Topology(TopologyId(candidate_topology_id.clone()));
        let bundle_subject = PolicySubject::Memory(MemoryId("paper:default".to_owned()));
        let promotion_policy = campaign.spec.promotion_policy.as_ref().ok_or_else(|| {
            DaemonError::InvalidInput("canary promotion policy missing".to_owned())
        })?;
        let evaluation_policy = EvaluationPolicy {
            minimum_evidence_completeness_ppm: promotion_policy.minimum_evidence_completeness_ppm,
            minimum_risk_recall_ppm: promotion_policy.minimum_risk_recall_ppm,
            minimum_fresh_pairs_per_horizon: promotion_policy
                .required_paired_sessions_per_horizon
                .into_iter()
                .max()
                .expect("canary policy has three horizons"),
        };
        let evaluation = EvaluationRuntime::new(self.store.clone(), evaluation_policy.clone())?;

        let pair_subjects = [
            (&contract_subject, 0_usize),
            (&topology_subject, 1_usize),
            (&bundle_subject, 2_usize),
        ];
        for (subject, index) in pair_subjects {
            let candidate_schedule = &shadow_outcomes[index].2;
            let candidate_outcome = &shadow_outcomes[index].1;
            for horizon in OutcomeHorizon::ALL {
                evaluation.record_shadow_pair(
                    &task.permit,
                    subject,
                    ShadowObservation {
                        parent_decision: parent_schedule.decision.clone(),
                        execution_context: parent_schedule.execution_context.clone(),
                        candidate_decision: candidate_schedule.decision.clone(),
                        candidate_contract_hash: if index == 1 {
                            campaign.spec.active_contract_hash.clone()
                        } else {
                            candidate_contract_hash.clone()
                        },
                        candidate_topology_id: if index == 0 {
                            parent_snapshot.run.topology_id.clone()
                        } else {
                            candidate_topology_id.clone()
                        },
                        horizon,
                        parent_outcome: parent_outcome_ref.clone(),
                        candidate_outcome: ArtifactRef {
                            artifact_id: shadow_outcomes[index].0.artifact_id.clone(),
                            kind: ArtifactKind::Outcome,
                        },
                        completed_at: candidate_outcome
                            .sealed_at
                            .expect("validated shadow outcome is sealed"),
                    },
                )?;
            }
        }

        let cohort = campaign
            .spec
            .cohort(session.reservation.level)
            .ok_or_else(|| {
                DaemonError::InvalidInput("canary cohort manifest missing".to_owned())
            })?;
        if session.reservation.cohort_id.as_ref() != Some(&cohort.cohort_id)
            || materialization.is_some_and(|input| input.cost_model != cohort.cost_model)
        {
            return Err(DaemonError::InvalidInput(
                "canary cohort runtime conditions changed".to_owned(),
            ));
        }
        let market_day = session.reservation.market_day.ok_or_else(|| {
            DaemonError::InvalidInput("canary session market day missing".to_owned())
        })?;
        let regime =
            session.reservation.regime.clone().ok_or_else(|| {
                DaemonError::InvalidInput("canary session regime missing".to_owned())
            })?;
        let metrics =
            |outcome: &Outcome, schedule: &OutcomeSchedule, horizon: OutcomeHorizon| -> Result<_> {
                let execution_context: akzio_domain::ExecutionContext =
                    self.read_artifact_payload(&schedule.execution_context)?;
                execution_context.validate()?;
                let process_quality_ppm = execution_context
                    .final_process_quality
                    .as_ref()
                    .and_then(akzio_domain::ProcessQualityAssessment::measured_floor);
                let mut metrics = outcome
                    .windows
                    .iter()
                    .find(|window| window.horizon == horizon)
                    .map(CanaryPairedOutcomeMetrics::from_outcome_window)
                    .ok_or_else(|| {
                        DaemonError::InvalidInput(format!("canary outcome is missing {horizon:?}"))
                    })?;
                metrics.process_quality_ppm = if retrospective_draft.is_some() {
                    process_quality_ppm
                } else {
                    None
                };
                Ok(metrics)
            };
        let observations = OutcomeHorizon::ALL
            .into_iter()
            .map(|horizon| {
                let parent = metrics(&parent_outcome, &parent_schedule, horizon)?;
                Ok(CanaryPairedObservation {
                    schema_version: DOMAIN_SCHEMA_VERSION,
                    cohort_id: cohort.cohort_id.clone(),
                    session_key: session.reservation.session_key.clone(),
                    market_day,
                    regime: regime.clone(),
                    horizon,
                    asset_universe: cohort.asset_universe.clone(),
                    cost_model: cohort.cost_model,
                    market_calendar_id: cohort.market_calendar_id.clone(),
                    generation_dataset_id: cohort.generation_dataset_id.clone(),
                    promotion_dataset_id: cohort.promotion_dataset_id.clone(),
                    contract: CanaryPairedSubjectMetrics {
                        parent,
                        candidate: metrics(&shadow_outcomes[0].1, &shadow_outcomes[0].2, horizon)?,
                    },
                    topology: CanaryPairedSubjectMetrics {
                        parent,
                        candidate: metrics(&shadow_outcomes[1].1, &shadow_outcomes[1].2, horizon)?,
                    },
                    bundle: CanaryPairedSubjectMetrics {
                        parent,
                        candidate: metrics(&shadow_outcomes[2].1, &shadow_outcomes[2].2, horizon)?,
                    },
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut observations = observations;
        // Older Rust-only runs may already have immutable unmeasured process
        // metrics. Preserve those unknowns; a repaired memo cannot rewrite them.
        let recorded = self.store.canary_observations(&cohort.cohort_id)?;
        for observation in &mut observations {
            if let Some(old) = recorded.iter().find(|old| {
                old.session_key == observation.session_key && old.horizon == observation.horizon
            }) {
                for (new, old) in [
                    (&mut observation.contract, &old.contract),
                    (&mut observation.topology, &old.topology),
                    (&mut observation.bundle, &old.bundle),
                ] {
                    new.parent.process_quality_ppm = old.parent.process_quality_ppm;
                    new.candidate.process_quality_ppm = old.candidate.process_quality_ppm;
                }
            }
        }
        let observations = self.store.record_canary_observations(
            lease,
            &session.reservation.campaign_id,
            session.reservation.level,
            &observations,
            completed_at,
        )?;
        let cohort_evaluation =
            evaluate_canary_cohort(cohort, promotion_policy, &observations, completed_at)?;
        let canary = CanaryCampaignRuntime::new(
            self.store.clone(),
            evaluation_policy
                .minimum_evidence_completeness_ppm
                .min(evaluation_policy.minimum_risk_recall_ppm),
        )?;
        let verdict = cohort_evaluation.verdict;
        // These evaluations materialize the parent Outcome. Candidate identity
        // lives in subject/CandidatePolicy/ShadowPair, never in producer fields.
        let producer_usage = self
            .store
            .model_usage_for_producing_run(&parent_schedule.decision)?;
        let producer_contract_hash = parent_snapshot
            .tasks
            .iter()
            .find(|t| t.node.recipe_id.as_str() == akzio_domain::RESEARCH_SYNTHESIZER_RECIPE_ID)
            .and_then(|t| t.node.contract_hash.clone())
            .ok_or_else(|| {
                DaemonError::InvalidInput("canary parent decision producer missing".to_owned())
            })?;
        let producer_topology = TopologyId(parent_snapshot.run.topology_id);

        let current_contract = self
            .store
            .policy_head(&contract_subject)?
            .map(|head| head.state)
            .unwrap_or_else(|| contract_subject.initial_state());
        evaluation.evaluate_sealed_with_retrospective(
            Some(lease),
            akzio_learning::SealedEvaluationInput {
                complete_task: false,
                permit: task.permit.clone(),
                subject: contract_subject.clone(),
                hypothesis_id: format!(
                    "canary-contract:{}:{}",
                    session.reservation.campaign_id, session.reservation.session_key
                ),
                outcome: parent_outcome_ref.clone(),
                contract_hash: producer_contract_hash.clone(),
                topology_id: producer_topology.clone(),
                candidate_policy: Some(CandidatePolicyInput {
                    baseline: ArtifactRef {
                        artifact_id: active_contract.artifact.artifact_id,
                        kind: ArtifactKind::Contract,
                    },
                    candidate: campaign.spec.candidate_contract.clone(),
                }),
                token_cost: producer_usage.billable_tokens_if_complete(),
                latency_millis: producer_usage.latency_millis_if_complete(),
            },
            draft,
            Some(canary.target_policy_state(
                &contract_subject,
                current_contract,
                session.reservation.level,
                verdict,
            )),
        )?;

        let current_topology = self
            .store
            .policy_head(&topology_subject)?
            .map(|head| head.state)
            .unwrap_or_else(|| topology_subject.initial_state());
        evaluation.evaluate_sealed_with_retrospective(
            Some(lease),
            akzio_learning::SealedEvaluationInput {
                complete_task: false,
                permit: task.permit.clone(),
                subject: topology_subject.clone(),
                hypothesis_id: format!(
                    "canary-topology:{}:{}",
                    session.reservation.campaign_id, session.reservation.session_key
                ),
                outcome: parent_outcome_ref.clone(),
                contract_hash: producer_contract_hash.clone(),
                topology_id: producer_topology.clone(),
                candidate_policy: Some(CandidatePolicyInput {
                    baseline: parent_topology,
                    candidate: campaign.spec.candidate_topology.clone(),
                }),
                token_cost: producer_usage.billable_tokens_if_complete(),
                latency_millis: producer_usage.latency_millis_if_complete(),
            },
            draft,
            Some(canary.target_policy_state(
                &topology_subject,
                current_topology,
                session.reservation.level,
                verdict,
            )),
        )?;

        let current_memory = self
            .store
            .policy_head(&bundle_subject)?
            .map(|head| head.state)
            .unwrap_or_else(|| bundle_subject.initial_state());
        let bundle_input = akzio_learning::SealedEvaluationInput {
            complete_task: false,
            permit: task.permit.clone(),
            subject: bundle_subject.clone(),
            hypothesis_id: format!(
                "canary-bundle:{}:{}",
                session.reservation.campaign_id, session.reservation.session_key
            ),
            outcome: parent_outcome_ref.clone(),
            contract_hash: producer_contract_hash,
            topology_id: producer_topology,
            candidate_policy: None,
            token_cost: producer_usage.billable_tokens_if_complete(),
            latency_millis: producer_usage.latency_millis_if_complete(),
        };
        evaluation.evaluate_sealed_with_retrospective(
            Some(lease),
            bundle_input,
            draft,
            if verdict == akzio_domain::CanaryVerdict::Advance {
                None
            } else {
                Some(current_memory)
            },
        )?;

        canary.apply_cohort_evaluation(
            lease,
            &session.reservation.campaign_id,
            session.reservation.level,
            &cohort_evaluation,
            completed_at,
        )?;
        self.store.finish_task(
            &task.permit,
            akzio_domain::TaskStatus::Succeeded,
            Utc::now(),
        )?;
        Ok(true)
    }
}
