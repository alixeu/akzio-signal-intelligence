impl RuntimeFixture {
    fn new() -> Self {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = fixture_time();
        let sealed_at = now + Duration::hours(1);
        let candidate_contract_hash = ContentHash::of_bytes(b"candidate-contract");

        let shadow_run = fixture_workflow(
            &store,
            RunPurpose::Shadow,
            1,
            Some(candidate_contract_hash.clone()),
            now,
        );
        let shadow_permit = claim_fixture_task(&store, "shadow-worker", now);
        assert_eq!(shadow_permit.run_id, shadow_run.run_id);
        let candidate_decisions = (0..5)
            .map(|index| {
                let artifact = fixture_artifact(
                    &store,
                    Some(&shadow_permit),
                    ArtifactKind::Decision,
                    ArtifactLifecycle::RunScoped,
                    &serde_json::json!({"candidate": index}),
                    vec![],
                    now,
                );
                store
                    .write_task_artifact(
                        &shadow_permit,
                        &artifact,
                        LifecycleEventType::ShadowDecisionCreated,
                        now,
                    )
                    .unwrap();
                artifact
            })
            .collect::<Vec<_>>();

        let paper_run = fixture_workflow(&store, RunPurpose::Paper, 7, None, now);
        let seed_permit = claim_fixture_task(&store, "paper-seed", now);
        assert_eq!(seed_permit.run_id, paper_run.run_id);
        let evidence = fixture_artifact(
            &store,
            Some(&seed_permit),
            ArtifactKind::NormalizedEvidence,
            ArtifactLifecycle::Canonical,
            &serde_json::json!({"prices": "governed"}),
            vec![],
            now,
        );
        let parent_decision = fixture_artifact(
            &store,
            Some(&seed_permit),
            ArtifactKind::Decision,
            ArtifactLifecycle::Canonical,
            &serde_json::json!({"decision": "parent"}),
            vec![artifact_reference(&evidence)],
            now,
        );
        let decision_context = fixture_artifact(
            &store,
            Some(&seed_permit),
            ArtifactKind::DecisionContext,
            ArtifactLifecycle::Canonical,
            &serde_json::json!({"context": "parent"}),
            vec![artifact_reference(&evidence)],
            now,
        );
        let execution_context = fixture_artifact(
            &store,
            Some(&seed_permit),
            ArtifactKind::ExecutionContext,
            ArtifactLifecycle::Canonical,
            &serde_json::json!({"execution_context": "paper"}),
            vec![artifact_reference(&decision_context)],
            now,
        );
        let execution_context_ref = artifact_reference(&execution_context);
        let verdict_payload = ExecutionVerdict::NoOrder {
            no_order: NoOrder {
                execution_context: execution_context_ref.clone(),
                blockers: vec![HardBlocker::Frozen],
                created_at: now,
            },
        };
        let verdict = fixture_artifact(
            &store,
            Some(&seed_permit),
            ArtifactKind::ExecutionVerdict,
            ArtifactLifecycle::Canonical,
            &verdict_payload,
            vec![execution_context_ref.clone()],
            now,
        );
        let verdict_ref = artifact_reference(&verdict);
        let parent_schedule = OutcomeSchedule {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            outcome_id: OutcomeId::new(),
            decision: artifact_reference(&parent_decision),
            decision_context: artifact_reference(&decision_context),
            execution_context: execution_context_ref.clone(),
            execution: OutcomeExecutionLineage::NoOrder {
                execution_verdict: verdict_ref.clone(),
            },
            baseline_trading_day: day(3),
            created_at: now,
        };
        let parent_schedule_artifact = fixture_artifact(
            &store,
            Some(&seed_permit),
            ArtifactKind::OutcomeSchedule,
            ArtifactLifecycle::Canonical,
            &parent_schedule,
            vec![
                parent_schedule.decision.clone(),
                parent_schedule.decision_context.clone(),
                parent_schedule.execution_context.clone(),
                verdict_ref.clone(),
            ],
            now,
        );
        let evidence_ref = artifact_reference(&evidence);
        let mut parent_materialization = materialization();
        parent_materialization.schedule = parent_schedule;
        parent_materialization.schedule_artifact = artifact_reference(&parent_schedule_artifact);
        parent_materialization.market_evidence = vec![evidence_ref.clone()];
        parent_materialization.sealed_at = sealed_at;
        for observation in &mut parent_materialization.observations {
            observation.observed_evidence_count = observation.expected_evidence_count;
            observation.detected_risk_count = Some(observation.expected_risk_count);
        }
        let parent_outcome_payload = materialize_outcome(&parent_materialization).unwrap();
        let parent_outcome = fixture_artifact(
            &store,
            Some(&seed_permit),
            ArtifactKind::Outcome,
            ArtifactLifecycle::Canonical,
            &parent_outcome_payload,
            vec![
                parent_materialization.schedule_artifact.clone(),
                evidence_ref.clone(),
            ],
            sealed_at,
        );

        let candidate_schedules = candidate_decisions
            .iter()
            .map(|candidate_decision| {
                let schedule = OutcomeSchedule {
                    schema_version: V2_DOMAIN_SCHEMA_VERSION,
                    outcome_id: OutcomeId::new(),
                    decision: artifact_reference(candidate_decision),
                    decision_context: artifact_reference(&decision_context),
                    execution_context: execution_context_ref.clone(),
                    execution: OutcomeExecutionLineage::NoOrder {
                        execution_verdict: verdict_ref.clone(),
                    },
                    baseline_trading_day: day(3),
                    created_at: now,
                };
                let artifact = fixture_artifact(
                    &store,
                    Some(&shadow_permit),
                    ArtifactKind::OutcomeSchedule,
                    ArtifactLifecycle::RunScoped,
                    &schedule,
                    vec![
                        schedule.decision.clone(),
                        schedule.decision_context.clone(),
                        schedule.execution_context.clone(),
                        verdict_ref.clone(),
                    ],
                    now,
                );
                (schedule, artifact)
            })
            .collect::<Vec<_>>();

        let seed_artifacts = vec![
            evidence,
            parent_decision.clone(),
            decision_context,
            execution_context,
            verdict,
            parent_schedule_artifact,
        ];
        for artifact in &seed_artifacts {
            store
                .write_task_artifact(
                    &seed_permit,
                    artifact,
                    LifecycleEventType::PaperSeedArtifactCreated,
                    now,
                )
                .unwrap();
        }
        store
            .commit_outcomes(
                &seed_permit,
                std::slice::from_ref(&parent_outcome),
                sealed_at,
            )
            .unwrap();

        for (_, artifact) in &candidate_schedules {
            store
                .write_task_artifact(
                    &shadow_permit,
                    artifact,
                    LifecycleEventType::ShadowOutcomeScheduleCreated,
                    now,
                )
                .unwrap();
        }

        let candidate_outcomes = candidate_schedules
            .iter()
            .map(|(schedule, schedule_artifact)| {
                let mut input = materialization();
                input.schedule = schedule.clone();
                input.schedule_artifact = artifact_reference(schedule_artifact);
                input.market_evidence = vec![evidence_ref.clone()];
                input.sealed_at = sealed_at;
                let outcome = materialize_outcome(&input).unwrap();
                fixture_artifact(
                    &store,
                    Some(&shadow_permit),
                    ArtifactKind::Outcome,
                    ArtifactLifecycle::RunScoped,
                    &outcome,
                    vec![input.schedule_artifact, evidence_ref.clone()],
                    sealed_at,
                )
            })
            .collect::<Vec<_>>();
        store
            .commit_outcomes(&shadow_permit, &candidate_outcomes, sealed_at)
            .unwrap();

        let candidates = candidate_decisions
            .iter()
            .zip(candidate_outcomes.iter())
            .map(|(decision, outcome)| (artifact_reference(decision), artifact_reference(outcome)))
            .collect();
        let runtime = EvaluationRuntime::new(store.clone(), EvaluationPolicy::default()).unwrap();
        let active_topology = ArtifactRef {
            artifact_id: paper_run.graph_artifact_id.clone(),
            kind: ArtifactKind::WorkflowGraph,
        };
        let candidate_topology = ArtifactRef {
            artifact_id: shadow_run.graph_artifact_id.clone(),
            kind: ArtifactKind::WorkflowGraph,
        };
        Self {
            _root: root,
            store,
            runtime,
            paper_run_id: paper_run.run_id,
            subject: PolicySubject::Memory(MemoryId::new()),
            materialization: parent_materialization,
            parent_decision: artifact_reference(&parent_decision),
            execution_context: execution_context_ref,
            parent_outcome: artifact_reference(&parent_outcome),
            candidates,
            active_topology,
            candidate_topology,
            candidate_contract_hash,
            candidate_topology_id: shadow_run.topology_id,
            pair_completed_at: sealed_at,
        }
    }

    fn claim_evaluation(&self, worker: &str) -> TaskWritePermit {
        let permit = claim_fixture_task(&self.store, worker, fixture_time());
        assert_eq!(permit.run_id, self.paper_run_id);
        permit
    }

    fn record_pair_batch(&self, permit: &TaskWritePermit, batch: usize) {
        self.record_pair_batch_for(permit, batch, &self.subject);
    }

    fn record_pair_batch_for(
        &self,
        permit: &TaskWritePermit,
        batch: usize,
        subject: &PolicySubject,
    ) {
        let (candidate_decision, candidate_outcome) = &self.candidates[batch];
        for horizon in OutcomeHorizon::ALL {
            self.runtime
                .record_shadow_pair(
                    permit,
                    subject,
                    ShadowObservation {
                        parent_decision: self.parent_decision.clone(),
                        execution_context: self.execution_context.clone(),
                        candidate_decision: candidate_decision.clone(),
                        candidate_contract_hash: self.candidate_contract_hash.clone(),
                        candidate_topology_id: self.candidate_topology_id.clone(),
                        horizon,
                        parent_outcome: self.parent_outcome.clone(),
                        candidate_outcome: candidate_outcome.clone(),
                        completed_at: self.pair_completed_at,
                    },
                )
                .unwrap();
        }
    }

    fn evaluate(&self, permit: TaskWritePermit, hypothesis_id: &str) -> EvaluationResult {
        self.evaluate_for(
            permit,
            hypothesis_id,
            self.subject.clone(),
            None,
            self.materialization.clone(),
        )
    }

    fn evaluate_for(
        &self,
        permit: TaskWritePermit,
        hypothesis_id: &str,
        subject: PolicySubject,
        candidate_policy: Option<CandidatePolicyInput>,
        materialization: OutcomeMaterializationInput,
    ) -> EvaluationResult {
        let contract_hash = match &subject {
            PolicySubject::Contract(hash) => hash.clone(),
            _ => ContentHash::of_bytes(b"active-contract"),
        };
        let topology_id = match &subject {
            PolicySubject::Topology(topology_id) => topology_id.clone(),
            _ => TopologyId("active-topology".to_owned()),
        };
        self.runtime
            .evaluate(EvaluationInput {
                permit,
                subject,
                hypothesis_id: hypothesis_id.to_owned(),
                materialization,
                contract_hash,
                topology_id,
                candidate_policy,
                token_cost: Some(10),
                latency_millis: Some(20),
            })
            .unwrap()
    }
}
