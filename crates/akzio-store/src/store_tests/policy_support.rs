struct PolicyCommitFixture {
    _root: tempfile::TempDir,
    store: V2Store,
    run: StoredRun,
    permit: TaskWritePermit,
    subject: PolicySubject,
    outcome: Artifact,
    final_retrospective: Artifact,
    experience: Artifact,
    evaluation: Artifact,
    candidate_policy: Option<Artifact>,
    transition: PolicyTransition,
    seed_artifact_id: ArtifactId,
    now: DateTime<Utc>,
}

impl PolicyCommitFixture {
    fn memory() -> Self {
        Self::new(false)
    }

    fn topology() -> Self {
        Self::new(true)
    }

    fn new(with_candidate: bool) -> Self {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();

        let mut paper_graph = graph();
        paper_graph.topology_id = "policy-paper".to_owned();
        let seed = paper_graph.nodes[0].clone();
        let mut evaluation_node = seed.clone();
        evaluation_node.task_id = TaskId::new();
        evaluation_node.dependencies = vec![seed.task_id.clone()];
        evaluation_node.objective = "evaluate policy".to_owned();
        paper_graph.nodes = vec![seed, evaluation_node];
        paper_graph.validate().unwrap();
        let paper_graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&paper_graph).unwrap(),
            None,
        );
        let paper_graph_ref = artifact_ref(&paper_graph_artifact);
        let run = StoredRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Paper,
            topology_id: paper_graph.topology_id.clone(),
            graph_artifact_id: paper_graph_artifact.artifact_id.clone(),
            created_at: now,
        };
        store
            .commit_workflow(&WorkflowCommit {
                run: run.clone(),
                graph: paper_graph_artifact,
                nodes: paper_graph.nodes,
            })
            .unwrap();

        let seed_permit = store
            .claim_next_task("policy-seed", now, Duration::seconds(30))
            .unwrap()
            .unwrap()
            .permit;
        let normalized = permit_artifact(
            &store,
            &seed_permit,
            ArtifactKind::NormalizedEvidence,
            &serde_json::json!({"normalized": true}),
            vec![],
            ArtifactLifecycle::RunScoped,
            now,
        );
        let decision = permit_artifact(
            &store,
            &seed_permit,
            ArtifactKind::Decision,
            &serde_json::json!({"decision": true}),
            vec![],
            ArtifactLifecycle::RunScoped,
            now,
        );
        let decision_context = permit_artifact(
            &store,
            &seed_permit,
            ArtifactKind::DecisionContext,
            &serde_json::json!({"context": true}),
            vec![],
            ArtifactLifecycle::RunScoped,
            now,
        );
        let execution_context = permit_artifact(
            &store,
            &seed_permit,
            ArtifactKind::ExecutionContext,
            &serde_json::json!({"execution": true}),
            vec![],
            ArtifactLifecycle::RunScoped,
            now,
        );
        let verdict_payload = ExecutionVerdict::NoOrder {
            no_order: akzio_domain::NoOrder {
                execution_context: artifact_ref(&execution_context),
                blockers: vec![akzio_domain::HardBlocker::Frozen],
                created_at: now,
            },
        };
        let verdict = permit_artifact(
            &store,
            &seed_permit,
            ArtifactKind::ExecutionVerdict,
            &verdict_payload,
            vec![artifact_ref(&execution_context)],
            ArtifactLifecycle::RunScoped,
            now,
        );
        let outcome_id = akzio_domain::OutcomeId::new();
        let schedule_payload = OutcomeSchedule {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            outcome_id: outcome_id.clone(),
            decision: artifact_ref(&decision),
            decision_context: artifact_ref(&decision_context),
            execution_context: artifact_ref(&execution_context),
            execution: OutcomeExecutionLineage::NoOrder {
                execution_verdict: artifact_ref(&verdict),
            },
            baseline_trading_day: now.date_naive(),
            created_at: now,
        };
        let schedule = permit_artifact(
            &store,
            &seed_permit,
            ArtifactKind::OutcomeSchedule,
            &schedule_payload,
            vec![
                schedule_payload.decision.clone(),
                schedule_payload.decision_context.clone(),
                schedule_payload.execution_context.clone(),
                artifact_ref(&verdict),
            ],
            ArtifactLifecycle::Canonical,
            now,
        );
        store
            .commit_attempt(
                &seed_permit,
                &[
                    normalized.clone(),
                    decision.clone(),
                    decision_context.clone(),
                    execution_context.clone(),
                    verdict.clone(),
                    schedule.clone(),
                ],
                TaskStatus::Succeeded,
                now,
            )
            .unwrap();

        let permit = store
            .claim_next_task("policy-evaluation", now, Duration::seconds(30))
            .unwrap()
            .unwrap()
            .permit;

        let candidate_graph = if with_candidate {
            let mut candidate_graph = graph();
            candidate_graph.topology_id = "policy-shadow-candidate".to_owned();
            let candidate_graph_artifact = artifact(
                &store,
                ArtifactKind::WorkflowGraph,
                &serde_json::to_string(&candidate_graph).unwrap(),
                None,
            );
            let reference = artifact_ref(&candidate_graph_artifact);
            let candidate_run = StoredRun {
                run_id: RunId::new(),
                purpose: RunPurpose::Shadow,
                topology_id: candidate_graph.topology_id.clone(),
                graph_artifact_id: candidate_graph_artifact.artifact_id.clone(),
                created_at: now,
            };
            store
                .commit_workflow(&WorkflowCommit {
                    run: candidate_run,
                    graph: candidate_graph_artifact,
                    nodes: candidate_graph.nodes,
                })
                .unwrap();
            Some((reference, candidate_graph.topology_id))
        } else {
            None
        };
        let subject = candidate_graph.as_ref().map_or_else(
            || PolicySubject::Memory(akzio_domain::MemoryId::new()),
            |(_, topology_id)| {
                PolicySubject::Topology(akzio_domain::TopologyId(topology_id.clone()))
            },
        );
        let from = subject.initial_state();
        let to = match subject {
            PolicySubject::Memory(_) => PolicyState::Memory(akzio_domain::MemoryLifecycle::Active),
            PolicySubject::Topology(_) => {
                PolicyState::Topology(akzio_domain::CandidatePolicyState::Canary10)
            }
            PolicySubject::Contract(_) => unreachable!(),
        };
        let outcome_payload = Outcome {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            outcome_id,
            schedule: artifact_ref(&schedule),
            market_evidence: vec![artifact_ref(&normalized)],
            windows: OutcomeHorizon::ALL
                .into_iter()
                .map(|horizon| akzio_domain::OutcomeWindow {
                    horizon,
                    observed_trading_day: now.date_naive()
                        + chrono::Days::new(u64::from(horizon.trading_days())),
                    portfolio_return_ppm: 1,
                    benchmark_return_ppm: 0,
                    transaction_cost_ppm: 0,
                    slippage_ppm: 0,
                    utility_ppm: 1,
                    calibration_ppm: Some(1_000_000),
                    evidence_completeness_ppm: 1_000_000,
                    risk_recall_ppm: Some(1_000_000),
                })
                .collect(),
            sealed_at: Some(now),
        };
        let outcome = permit_artifact(
            &store,
            &permit,
            ArtifactKind::Outcome,
            &outcome_payload,
            vec![artifact_ref(&schedule), artifact_ref(&normalized)],
            ArtifactLifecycle::Canonical,
            now,
        );
        let final_retrospective = retrospective_artifact(&store, &permit, &outcome, now);
        let retrospective_ref = artifact_ref(&final_retrospective);
        let experience_payload = Experience {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            experience_id: akzio_domain::ExperienceId::new(),
            subject: subject.clone(),
            hypothesis_id: "fixture".to_owned(),
            decision: artifact_ref(&decision),
            decision_context: artifact_ref(&decision_context),
            execution_context: artifact_ref(&execution_context),
            policy_verdict: artifact_ref(&verdict),
            outcome: artifact_ref(&outcome),
            contract_hash: ContentHash::of_bytes(b"fixture-contract"),
            topology_id: match &subject {
                PolicySubject::Topology(topology_id) => topology_id.clone(),
                _ => akzio_domain::TopologyId("fixture-topology".to_owned()),
            },
            policy_state: from,
            created_at: now,
        };
        let experience = permit_artifact(
            &store,
            &permit,
            ArtifactKind::Experience,
            &experience_payload,
            vec![
                experience_payload.decision.clone(),
                experience_payload.decision_context.clone(),
                experience_payload.execution_context.clone(),
                experience_payload.policy_verdict.clone(),
                experience_payload.outcome.clone(),
                retrospective_ref.clone(),
            ],
            ArtifactLifecycle::Canonical,
            now,
        );
        let evaluation_payload = Evaluation {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            evaluation_id: akzio_domain::EvaluationId::new(),
            outcome: artifact_ref(&outcome),
            experience: artifact_ref(&experience),
            marginal_utility_ppm: 1,
            token_cost: Some(1),
            latency_millis: Some(1),
            created_at: now,
        };
        let evaluation = permit_artifact(
            &store,
            &permit,
            ArtifactKind::Evaluation,
            &evaluation_payload,
            vec![
                artifact_ref(&outcome),
                artifact_ref(&experience),
                retrospective_ref,
            ],
            ArtifactLifecycle::Canonical,
            now,
        );
        let candidate_policy = candidate_graph.map(|(candidate, _)| {
            let payload = CandidatePolicy {
                schema_version: V2_DOMAIN_SCHEMA_VERSION,
                subject: subject.clone(),
                baseline: paper_graph_ref,
                candidate,
                source_evaluation: artifact_ref(&evaluation),
                created_at: now,
            };
            permit_artifact(
                &store,
                &permit,
                ArtifactKind::CandidatePolicy,
                &payload,
                vec![
                    payload.baseline.clone(),
                    payload.candidate.clone(),
                    payload.source_evaluation.clone(),
                ],
                ArtifactLifecycle::Canonical,
                now,
            )
        });
        let transition = PolicyTransition {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            transition_id: PolicyTransitionId::new(),
            subject: subject.clone(),
            from,
            to,
            evaluation: artifact_ref(&evaluation),
            created_at: now,
        };

        Self {
            _root: root,
            store,
            run,
            permit,
            subject,
            outcome,
            final_retrospective,
            experience,
            evaluation,
            candidate_policy,
            transition,
            seed_artifact_id: decision.artifact_id,
            now,
        }
    }

    fn commit(&self, pair_snapshot: PolicyShadowPairSnapshot) -> PolicyEvaluationCommit {
        PolicyEvaluationCommit {
            permit: self.permit.clone(),
            outcome: self.outcome.clone(),
            final_retrospective: self.final_retrospective.clone(),
            experience: self.experience.clone(),
            evaluation: self.evaluation.clone(),
            candidate_policy: self.candidate_policy.clone(),
            subject: self.subject.clone(),
            from: self.transition.from,
            to: self.transition.to,
            pair_snapshot,
            transition: Some(self.transition.clone()),
            completed_at: self.now,
        }
    }

    fn insert_pair(
        &self,
        label: &str,
        horizon: OutcomeHorizon,
        completed_at: DateTime<Utc>,
    ) -> i64 {
        let pair_key = ContentHash::of_bytes(label.as_bytes());
        let mut connection = self.store.connection.lock().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let cursor = append_event(
            &transaction,
            &self.run.run_id,
            Some(&self.permit.task_id),
            Some(&self.permit.attempt_id),
            LifecycleEventType::ShadowPairCompleted,
            Some(&self.seed_artifact_id),
            completed_at,
        )
        .unwrap();
        transaction
            .execute(
                r#"INSERT INTO rebuild_shadow_pairs
                       (pair_key, subject_id, subject_json, parent_decision_artifact_id,
                        execution_context_artifact_id, candidate_decision_artifact_id,
                        candidate_contract_hash, candidate_topology_id, horizon,
                        parent_outcome_artifact_id, candidate_outcome_artifact_id,
                        completed_at, pair_event_cursor)
                       VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)"#,
                params![
                    pair_key.as_str(),
                    self.subject.subject_id(),
                    serde_json::to_string(&self.subject).unwrap(),
                    self.seed_artifact_id.0.as_str(),
                    self.seed_artifact_id.0.as_str(),
                    self.seed_artifact_id.0.as_str(),
                    ContentHash::of_bytes(b"fixture-candidate-contract").as_str(),
                    "fixture-candidate-topology",
                    enum_name(horizon),
                    self.seed_artifact_id.0.as_str(),
                    self.seed_artifact_id.0.as_str(),
                    completed_at.to_rfc3339(),
                    cursor,
                ],
            )
            .unwrap();
        transaction.commit().unwrap();
        cursor
    }
}
