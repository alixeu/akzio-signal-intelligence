#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, Utc};

    use super::{
        CandidatePolicy, CandidatePolicyState, Experience, Outcome, OutcomeCostModel,
        OutcomeExecutionLineage, OutcomeHorizon, OutcomeSchedule, OutcomeWindow, PolicyState,
        PolicySubject, PolicyTransition, Retrospective, RetrospectiveStatus,
    };
    use crate::{
        artifact::{ArtifactId, ArtifactKind, ArtifactRef},
        ExperienceId, OutcomeId, PolicyTransitionId,
        ContentHash, MemoryId, TopologyId,
    };

    fn reference(kind: ArtifactKind, value: &[u8]) -> ArtifactRef {
        ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(value)),
            kind,
        }
    }

    fn candidate_policy(
        subject: PolicySubject,
        baseline: ArtifactRef,
        candidate: ArtifactRef,
    ) -> CandidatePolicy {
        CandidatePolicy {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            subject,
            baseline,
            candidate,
            source_evaluation: reference(ArtifactKind::Evaluation, b"source-evaluation"),
            created_at: Utc::now(),
        }
    }

    fn trading_day(day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 8, day).unwrap()
    }

    fn window(horizon: OutcomeHorizon, day: u32) -> OutcomeWindow {
        OutcomeWindow {
            horizon,
            observed_trading_day: trading_day(day),
            portfolio_return_ppm: 1,
            benchmark_return_ppm: 0,
            transaction_cost_ppm: 0,
            slippage_ppm: 0,
            utility_ppm: 1,
            calibration_ppm: Some(1),
            evidence_completeness_ppm: 1_000_000,
            risk_recall_ppm: Some(1_000_000),
        }
    }

    #[test]
    fn outcome_cost_model_rejects_values_above_one() {
        assert!(OutcomeCostModel {
            transaction_cost_ppm: 1_000_001,
            slippage_ppm: 0,
        }
        .validate()
        .is_err());
    }

    fn reconciled_schedule() -> OutcomeSchedule {
        OutcomeSchedule {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            outcome_id: OutcomeId::new(),
            decision: reference(ArtifactKind::Decision, b"decision"),
            decision_context: reference(ArtifactKind::DecisionContext, b"decision-context"),
            execution_context: reference(ArtifactKind::ExecutionContext, b"execution-context"),
            execution: OutcomeExecutionLineage::ReconciledPaper {
                execution_verdict: reference(ArtifactKind::ExecutionVerdict, b"execution-verdict"),
                commitment: reference(ArtifactKind::ExecutionCommitment, b"commitment"),
                reconciliation: reference(ArtifactKind::Reconciliation, b"reconciliation"),
            },
            baseline_trading_day: trading_day(10),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn outcome_schedule_uses_completed_trading_sessions() {
        let schedule = reconciled_schedule();
        schedule.validate().unwrap();
        assert!(schedule.due_horizons(0).is_empty());
        assert_eq!(schedule.due_horizons(1), vec![OutcomeHorizon::T1]);
        assert_eq!(
            schedule.due_horizons(3),
            vec![OutcomeHorizon::T1, OutcomeHorizon::T3]
        );
        assert_eq!(schedule.due_horizons(5), OutcomeHorizon::ALL);
        assert!(ArtifactKind::OutcomeSchedule.can_be_canonical());
    }

    #[test]
    fn outcome_schedule_distinguishes_no_order_from_reconciliation() {
        let mut schedule = reconciled_schedule();
        schedule.execution = OutcomeExecutionLineage::NoOrder {
            execution_verdict: reference(ArtifactKind::ExecutionVerdict, b"no-order"),
        };
        schedule.validate().unwrap();

        schedule.execution = OutcomeExecutionLineage::ReconciledPaper {
            execution_verdict: reference(ArtifactKind::ExecutionVerdict, b"accepted"),
            commitment: reference(ArtifactKind::ExecutionPlan, b"wrong-kind"),
            reconciliation: reference(ArtifactKind::Reconciliation, b"reconciliation"),
        };
        assert!(schedule.validate().is_err());
    }

    #[test]
    fn learning_requires_a_sealed_complete_outcome() {
        let outcome = Outcome {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            outcome_id: OutcomeId::new(),
            schedule: reference(ArtifactKind::OutcomeSchedule, b"schedule"),
            market_evidence: vec![reference(ArtifactKind::NormalizedEvidence, b"market")],
            windows: vec![
                window(OutcomeHorizon::T1, 11),
                window(OutcomeHorizon::T3, 13),
                window(OutcomeHorizon::T5, 17),
            ],
            sealed_at: None,
        };
        outcome.validate().unwrap();
        assert!(outcome.validate_sealed().is_err());

        let sealed = Outcome {
            sealed_at: Some(Utc::now()),
            ..outcome
        };
        sealed.validate_sealed().unwrap();
    }

    #[test]
    fn unsealed_outcome_accepts_a_due_prefix_but_not_canonical_sealing() {
        let partial = Outcome {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            outcome_id: OutcomeId::new(),
            schedule: reference(ArtifactKind::OutcomeSchedule, b"partial-schedule"),
            market_evidence: vec![reference(
                ArtifactKind::NormalizedEvidence,
                b"partial-market",
            )],
            windows: vec![window(OutcomeHorizon::T1, 11)],
            sealed_at: None,
        };
        partial.validate().unwrap();
        assert!(partial.validate_sealed().is_err());
    }

    #[test]
    fn model_unavailable_t5_retrospective_still_requires_sealing() {
        let outcome = reference(ArtifactKind::Outcome, b"sealed-outcome");
        let retrospective = Retrospective {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            outcome_id: OutcomeId::new(),
            horizon: OutcomeHorizon::T5,
            status: RetrospectiveStatus::ModelUnavailable,
            summary: "model unavailable".to_owned(),
            findings: Vec::new(),
            counterfactuals: Vec::new(),
            lesson_candidates: Vec::new(),
            diagnostic_gaps: vec!["model unavailable".to_owned()],
            source_refs: vec![outcome.clone()],
            outcome,
            created_at: Utc::now(),
            sealed_at: None,
        };
        assert!(retrospective.validate().is_err());
    }

    #[test]
    fn outcome_rejects_non_monotonic_observation_days() {
        let outcome = Outcome {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            outcome_id: OutcomeId::new(),
            schedule: reference(ArtifactKind::OutcomeSchedule, b"schedule"),
            market_evidence: vec![reference(ArtifactKind::SemanticDetail, b"market")],
            windows: vec![
                window(OutcomeHorizon::T1, 11),
                window(OutcomeHorizon::T3, 17),
                window(OutcomeHorizon::T5, 13),
            ],
            sealed_at: Some(Utc::now()),
        };
        assert!(outcome.validate().is_err());
    }

    #[test]
    fn candidate_policy_accepts_contract_and_topology_payloads() {
        let contract_candidate = reference(ArtifactKind::Contract, b"contract-candidate");
        candidate_policy(
            PolicySubject::Contract(contract_candidate.artifact_id.0.clone()),
            reference(ArtifactKind::Contract, b"contract-baseline"),
            contract_candidate,
        )
        .validate()
        .unwrap();

        candidate_policy(
            PolicySubject::Topology(TopologyId::new()),
            reference(ArtifactKind::WorkflowGraph, b"topology-baseline"),
            reference(ArtifactKind::WorkflowGraph, b"topology-candidate"),
        )
        .validate()
        .unwrap();
    }

    #[test]
    fn candidate_policy_rejects_memory_subject() {
        let policy = candidate_policy(
            PolicySubject::Memory(MemoryId::new()),
            reference(ArtifactKind::Contract, b"baseline"),
            reference(ArtifactKind::Contract, b"candidate"),
        );

        assert_eq!(
            policy.validate(),
            Err(crate::DomainError::EmptyField {
                field: "candidate_policy.memory_subject",
            })
        );
    }

    #[test]
    fn candidate_policy_rejects_identical_baseline_and_candidate() {
        let candidate = reference(ArtifactKind::WorkflowGraph, b"same-topology");
        let policy = candidate_policy(
            PolicySubject::Topology(TopologyId::new()),
            candidate.clone(),
            candidate,
        );

        assert_eq!(
            policy.validate(),
            Err(crate::DomainError::EmptyField {
                field: "candidate_policy.baseline_candidate",
            })
        );
    }

    #[test]
    fn candidate_policy_rejects_wrong_artifact_kinds() {
        let contract_candidate = reference(ArtifactKind::WorkflowGraph, b"wrong-contract");
        let contract = candidate_policy(
            PolicySubject::Contract(contract_candidate.artifact_id.0.clone()),
            reference(ArtifactKind::Contract, b"contract-baseline"),
            contract_candidate,
        );
        assert_eq!(
            contract.validate(),
            Err(crate::DomainError::EmptyField {
                field: "candidate_policy.contract_refs",
            })
        );

        let topology = candidate_policy(
            PolicySubject::Topology(TopologyId::new()),
            reference(ArtifactKind::WorkflowGraph, b"topology-baseline"),
            reference(ArtifactKind::Contract, b"wrong-topology"),
        );
        assert_eq!(
            topology.validate(),
            Err(crate::DomainError::EmptyField {
                field: "candidate_policy.topology_refs",
            })
        );
    }

    #[test]
    fn candidate_policy_requires_evaluation_source() {
        let mut policy = candidate_policy(
            PolicySubject::Topology(TopologyId::new()),
            reference(ArtifactKind::WorkflowGraph, b"baseline"),
            reference(ArtifactKind::WorkflowGraph, b"candidate"),
        );
        policy.source_evaluation = reference(ArtifactKind::Outcome, b"wrong-source");

        assert_eq!(
            policy.validate(),
            Err(crate::DomainError::EmptyField {
                field: "candidate_policy.source_evaluation",
            })
        );
    }

    #[test]
    fn candidate_policy_contract_binding_is_store_owned() {
        let policy = candidate_policy(
            PolicySubject::Contract(ContentHash::of_bytes(b"different-contract")),
            reference(ArtifactKind::Contract, b"baseline"),
            reference(ArtifactKind::Contract, b"candidate"),
        );

        policy.validate().unwrap();
    }

    #[test]
    fn experience_and_transition_use_typed_policy_subjects() {
        let contract_hash = ContentHash::of_bytes(b"contract");
        let subject = PolicySubject::Contract(contract_hash.clone());
        let experience = Experience {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            experience_id: ExperienceId::new(),
            subject: subject.clone(),
            hypothesis_id: "stable hypothesis".to_owned(),
            decision: reference(ArtifactKind::Decision, b"decision"),
            decision_context: reference(ArtifactKind::DecisionContext, b"decision-context"),
            execution_context: reference(ArtifactKind::ExecutionContext, b"execution-context"),
            policy_verdict: reference(ArtifactKind::ExecutionVerdict, b"verdict"),
            outcome: reference(ArtifactKind::Outcome, b"outcome"),
            contract_hash,
            topology_id: TopologyId("topology".to_owned()),
            policy_state: PolicyState::Contract(CandidatePolicyState::Canary10),
            created_at: Utc::now(),
        };
        experience.validate().unwrap();

        let transition = PolicyTransition {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            transition_id: PolicyTransitionId::new(),
            subject: subject.clone(),
            from: PolicyState::Contract(CandidatePolicyState::Candidate),
            to: PolicyState::Contract(CandidatePolicyState::Canary10),
            evaluation: reference(ArtifactKind::Evaluation, b"evaluation"),
            created_at: Utc::now(),
        };
        transition.validate().unwrap();
        assert_eq!(
            subject.subject_id(),
            format!("contract:{}", experience.contract_hash)
        );

        let mut mismatched = transition;
        mismatched.subject = PolicySubject::Memory(MemoryId::new());
        assert!(mismatched.validate().is_err());

        let old_shape = serde_json::json!({
            "schema_version": crate::V2_DOMAIN_SCHEMA_VERSION,
            "transition_id": PolicyTransitionId::new(),
            "subject_id": subject.subject_id(),
            "from": {"kind": "contract", "state": "candidate"},
            "to": {"kind": "contract", "state": "canary10"},
            "evaluation": reference(ArtifactKind::Evaluation, b"old-evaluation"),
            "created_at": Utc::now(),
        });
        assert!(serde_json::from_value::<PolicyTransition>(old_shape).is_err());
    }

    #[test]
    fn policy_subject_storage_identity_round_trips_with_namespace() {
        for subject in [
            PolicySubject::Memory(MemoryId::new()),
            PolicySubject::Contract(ContentHash::of_bytes(b"contract")),
            PolicySubject::Topology(TopologyId::new()),
        ] {
            assert_eq!(
                PolicySubject::from_subject_id(&subject.subject_id()).unwrap(),
                subject
            );
        }
        assert!(PolicySubject::from_subject_id("untyped").is_err());
        assert!(PolicySubject::from_subject_id("unknown:value").is_err());
    }
}
