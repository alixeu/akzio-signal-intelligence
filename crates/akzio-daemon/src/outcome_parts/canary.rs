impl Daemon {
    fn complete_canary_session(
        &self,
        lease: &DaemonLease,
        task: &ClaimedAttempt,
        session: &akzio_store::v2::StoredCanarySession,
        parent_outcome_artifact: &Artifact,
        materialization: OutcomeMaterializationInput,
        retrospective_draft: Option<&RetrospectiveDraft>,
    ) -> Result<bool> {
        let campaign = self
            .store
            .canary_campaign(&session.reservation.campaign_id)?
            .ok_or_else(|| {
                DaemonError::InvalidInput("canary campaign disappeared during outcome".to_owned())
            })?;
        if campaign.status != session.reservation.level {
            return Ok(true);
        }

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
        let parent_schedule = materialization.schedule.clone();
        let completed_at = materialization.sealed_at;

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
            artifact_id: parent_snapshot.revision.graph_artifact.artifact_id.clone(),
            kind: ArtifactKind::WorkflowGraph,
        };
        let candidate_contract_hash = candidate_contract.contract_hash.clone();
        let candidate_topology_id = candidate_topology.topology_id.clone();
        let contract_subject = PolicySubject::Contract(candidate_contract_hash.clone());
        let topology_subject = PolicySubject::Topology(TopologyId(candidate_topology_id.clone()));
        let bundle_subject = PolicySubject::Memory(MemoryId("paper:default".to_owned()));
        let promotion_policy = campaign.spec.promotion_policy.as_ref().ok_or_else(|| {
            DaemonError::InvalidInput("canary promotion policy missing".to_owned())
        })?;
        let evaluation_policy = EvaluationPolicy {
            minimum_evidence_completeness_ppm: promotion_policy
                .minimum_evidence_completeness_ppm,
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
                        candidate_contract_hash: candidate_contract_hash.clone(),
                        candidate_topology_id: candidate_topology_id.clone(),
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
            .ok_or_else(|| DaemonError::InvalidInput("canary cohort manifest missing".to_owned()))?;
        if session.reservation.cohort_id.as_ref() != Some(&cohort.cohort_id)
            || materialization.cost_model != cohort.cost_model
        {
            return Err(DaemonError::InvalidInput(
                "canary cohort runtime conditions changed".to_owned(),
            ));
        }
        let market_day = session.reservation.market_day.ok_or_else(|| {
            DaemonError::InvalidInput("canary session market day missing".to_owned())
        })?;
        let regime = session.reservation.regime.clone().ok_or_else(|| {
            DaemonError::InvalidInput("canary session regime missing".to_owned())
        })?;
        let metrics = |outcome: &Outcome, horizon: OutcomeHorizon| -> Result<_> {
            outcome
                .windows
                .iter()
                .find(|window| window.horizon == horizon)
                .map(CanaryPairedOutcomeMetrics::from_outcome_window)
                .ok_or_else(|| {
                    DaemonError::InvalidInput(format!(
                        "canary outcome is missing {horizon:?}"
                    ))
                })
        };
        let observations = OutcomeHorizon::ALL
            .into_iter()
            .map(|horizon| {
                let parent = metrics(&parent_outcome, horizon)?;
                Ok(CanaryPairedObservation {
                    schema_version: V2_DOMAIN_SCHEMA_VERSION,
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
                        candidate: metrics(&shadow_outcomes[0].1, horizon)?,
                    },
                    topology: CanaryPairedSubjectMetrics {
                        parent,
                        candidate: metrics(&shadow_outcomes[1].1, horizon)?,
                    },
                    bundle: CanaryPairedSubjectMetrics {
                        parent,
                        candidate: metrics(&shadow_outcomes[2].1, horizon)?,
                    },
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let observations = self.store.record_canary_observations(
            lease,
            &session.reservation.campaign_id,
            session.reservation.level,
            &observations,
            completed_at,
        )?;
        let cohort_evaluation = evaluate_canary_cohort(
            cohort,
            promotion_policy,
            &observations,
            completed_at,
        )?;
        let canary = CanaryCampaignRuntime::new(
            self.store.clone(),
            evaluation_policy
                .minimum_evidence_completeness_ppm
                .min(evaluation_policy.minimum_risk_recall_ppm),
        )?;
        let verdict = cohort_evaluation.verdict;
        if matches!(
            verdict,
            akzio_domain::CanaryVerdict::Defer | akzio_domain::CanaryVerdict::Hold
        ) {
            return Ok(true);
        }

        let current_contract = self
            .store
            .policy_head(&contract_subject)?
            .map(|head| head.state)
            .unwrap_or_else(|| contract_subject.initial_state());
        evaluation.evaluate_with_lease_at_state(
            Some(lease),
            EvaluationInput {
                permit: task.permit.clone(),
                subject: contract_subject.clone(),
                hypothesis_id: format!(
                    "canary-contract:{}:{}",
                    session.reservation.campaign_id, session.reservation.session_key
                ),
                materialization: materialization.clone(),
                contract_hash: candidate_contract_hash.clone(),
                topology_id: TopologyId(candidate_topology_id.clone()),
                candidate_policy: Some(CandidatePolicyInput {
                    baseline: ArtifactRef {
                        artifact_id: active_contract.artifact.artifact_id.clone(),
                        kind: ArtifactKind::Contract,
                    },
                    candidate: campaign.spec.candidate_contract.clone(),
                }),
                token_cost: None,
                latency_millis: None,
            },
            retrospective_draft,
            canary.target_policy_state(
                &contract_subject,
                current_contract,
                session.reservation.level,
                verdict,
            ),
        )?;

        let current_topology = self
            .store
            .policy_head(&topology_subject)?
            .map(|head| head.state)
            .unwrap_or_else(|| topology_subject.initial_state());
        evaluation.evaluate_with_lease_at_state(
            Some(lease),
            EvaluationInput {
                permit: task.permit.clone(),
                subject: topology_subject.clone(),
                hypothesis_id: format!(
                    "canary-topology:{}:{}",
                    session.reservation.campaign_id, session.reservation.session_key
                ),
                materialization: materialization.clone(),
                contract_hash: candidate_contract_hash.clone(),
                topology_id: TopologyId(candidate_topology_id.clone()),
                candidate_policy: Some(CandidatePolicyInput {
                    baseline: parent_topology,
                    candidate: campaign.spec.candidate_topology.clone(),
                }),
                token_cost: None,
                latency_millis: None,
            },
            retrospective_draft,
            canary.target_policy_state(
                &topology_subject,
                current_topology,
                session.reservation.level,
                verdict,
            ),
        )?;

        let current_memory = self
            .store
            .policy_head(&bundle_subject)?
            .map(|head| head.state)
            .unwrap_or_else(|| bundle_subject.initial_state());
        let bundle_input = EvaluationInput {
            permit: task.permit.clone(),
            subject: bundle_subject.clone(),
            hypothesis_id: format!(
                "canary-bundle:{}:{}",
                session.reservation.campaign_id, session.reservation.session_key
            ),
            materialization,
            contract_hash: candidate_contract_hash,
            topology_id: TopologyId(candidate_topology_id),
            candidate_policy: None,
            token_cost: None,
            latency_millis: None,
        };
        if verdict == akzio_domain::CanaryVerdict::Advance {
            if let Some(draft) = retrospective_draft {
                evaluation.evaluate_with_lease_and_retrospective(
                    Some(lease),
                    bundle_input,
                    draft,
                )?;
            } else {
                evaluation.evaluate_with_lease(Some(lease), bundle_input)?;
            }
        } else {
            evaluation.evaluate_with_lease_at_state(
                Some(lease),
                bundle_input,
                retrospective_draft,
                current_memory,
            )?;
        }

        canary.apply_cohort_evaluation(
            lease,
            &session.reservation.campaign_id,
            session.reservation.level,
            &cohort_evaluation,
            completed_at,
        )?;
        Ok(true)
    }
}
