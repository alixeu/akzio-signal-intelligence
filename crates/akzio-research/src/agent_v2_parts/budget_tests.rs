#[cfg(test)]
mod cumulative_cost_budget_tests {
    use super::*;

    fn budget(max_input_tokens: u32, max_output_tokens: u32) -> AgentRunBudget {
        AgentRunBudget::new(
            &TaskBudget {
                max_input_tokens,
                max_output_tokens,
                max_wall_time_secs: 60,
                max_tool_calls: 2,
            },
            &RetryPolicy::none(),
        )
    }

    fn pricing(version: &str) -> ModelPricingSnapshot {
        ModelPricingSnapshot {
            identity: "fixture-route-price".to_owned(),
            version: version.to_owned(),
            input_micros_per_million_tokens: 2_000_000,
            cached_input_micros_per_million_tokens: 1_000_000,
            output_micros_per_million_tokens: 3_000_000,
            reasoning_micros_per_million_tokens: 4_000_000,
        }
    }

    fn policy(max_cost_micros: Option<u64>) -> ModelBudgetPolicy {
        ModelBudgetPolicy {
            route_identity: Some("fixture-route".to_owned()),
            max_cost_micros,
            pricing: Some(pricing("v1")),
        }
    }

    fn usage(
        input_tokens: Option<u64>,
        cached_input_tokens: Option<u64>,
        output_tokens: Option<u64>,
        reasoning_tokens: Option<u64>,
    ) -> AgentTurnTelemetry {
        AgentTurnTelemetry {
            latency_millis: 1,
            input_tokens,
            cached_input_tokens,
            output_tokens,
            reasoning_tokens,
        }
    }

    #[test]
    fn multi_turn_reasoning_and_cached_usage_accumulate_without_double_charge() {
        let mut budget = budget(100, 100);
        budget.attach_budget_policy(&policy(None)).unwrap();
        let telemetry = usage(Some(10), Some(4), Some(5), Some(2));
        budget.record_turn(1, 1, Some(&telemetry)).unwrap();
        budget.record_turn(1, 1, Some(&telemetry)).unwrap();

        assert_eq!(budget.input_tokens, 20);
        assert_eq!(budget.cached_input_tokens, 8);
        assert_eq!(budget.output_tokens, 10);
        assert_eq!(budget.reasoning_tokens, 4);
        // Per turn: 6*2 + 4*1 + 3*3 + 2*4 = 33 micros.
        assert_eq!(budget.cost_micros, 66);
    }

    #[test]
    fn missing_reasoning_is_conservatively_charged_at_the_higher_output_rate() {
        let mut budget = budget(100, 100);
        budget.attach_budget_policy(&policy(None)).unwrap();
        budget
            .record_turn(1, 1, Some(&usage(Some(2), None, Some(3), None)))
            .unwrap();
        assert_eq!(budget.reasoning_tokens, 0);
        assert_eq!(budget.cost_micros, 16); // 2*2 + 3*4
    }

    #[test]
    fn completely_missing_usage_uses_conservative_local_estimates() {
        let mut budget = budget(100, 100);
        budget.attach_budget_policy(&policy(None)).unwrap();
        budget.record_turn(3, 4, None).unwrap();
        assert_eq!((budget.input_tokens, budget.output_tokens), (3, 4));
        assert_eq!(budget.cost_micros, 22); // 3*2 + 4*4
    }

    #[test]
    fn actual_usage_replaces_lower_estimates() {
        let mut budget = budget(100, 100);
        budget.attach_budget_policy(&policy(None)).unwrap();
        budget
            .record_turn(1, 1, Some(&usage(Some(8), Some(0), Some(6), Some(0))))
            .unwrap();
        assert_eq!((budget.input_tokens, budget.output_tokens), (8, 6));
        assert_eq!(budget.cost_micros, 34);
    }

    #[tokio::test]
    async fn provider_fixture_usage_is_normalized_into_agent_telemetry() {
        let adapter = ModelClientAdapter::new(ModelClient::Fixture(json!({
            "output_text": "fixture response",
            "usage": {
                "input_tokens": 12,
                "input_tokens_details": {"cached_tokens": 5},
                "output_tokens": 8,
                "output_tokens_details": {"reasoning_tokens": 3}
            }
        })));
        let turn = adapter
            .turn(AgentModelRequest {
                contract_hash: akzio_domain::ContentHash::of_bytes(b"fixture-contract"),
                purpose: "fixture.usage".to_owned(),
                phase: AgentTurnPhase::Draft,
                prompt: "fixture prompt".to_owned(),
                objective: "fixture objective".to_owned(),
                manifest_artifact_id: ArtifactId(akzio_domain::ContentHash::of_bytes(
                    b"fixture-manifest",
                )),
                context: vec![],
                continuation: None,
                tool_outputs: vec![],
                continuation_instruction: None,
                max_output_tokens: 16,
                tools: vec![],
                terminal: None,
            })
            .await
            .unwrap();
        let telemetry = turn.telemetry.unwrap();
        assert_eq!(telemetry.input_tokens, Some(12));
        assert_eq!(telemetry.cached_input_tokens, Some(5));
        assert_eq!(telemetry.output_tokens, Some(8));
        assert_eq!(telemetry.reasoning_tokens, Some(3));
    }

    #[test]
    fn exact_cost_cap_passes_and_one_more_micro_blocks_the_next_call() {
        let equal_rates = ModelPricingSnapshot {
            identity: "fixture-equal-rate".to_owned(),
            version: "v1".to_owned(),
            input_micros_per_million_tokens: 1_000_000,
            cached_input_micros_per_million_tokens: 1_000_000,
            output_micros_per_million_tokens: 1_000_000,
            reasoning_micros_per_million_tokens: 1_000_000,
        };
        let mut budget = budget(100, 100);
        budget
            .attach_budget_policy(&ModelBudgetPolicy {
                route_identity: Some("fixture-route".to_owned()),
                max_cost_micros: Some(2),
                pricing: Some(equal_rates),
            })
            .unwrap();
        budget.record_turn(1, 1, None).unwrap();
        assert_eq!(budget.cost_micros, 2);
        assert!(matches!(
            budget.output_tokens_for_call(1),
            Err(ResearchError::CostBudgetExceeded {
                actual: 3,
                maximum: 2
            })
        ));
    }

    #[test]
    fn actual_cost_one_micro_over_cap_fails_the_turn() {
        let equal_rates = ModelPricingSnapshot {
            identity: "fixture-over-cap".to_owned(),
            version: "v1".to_owned(),
            input_micros_per_million_tokens: 1_000_000,
            cached_input_micros_per_million_tokens: 1_000_000,
            output_micros_per_million_tokens: 1_000_000,
            reasoning_micros_per_million_tokens: 1_000_000,
        };
        let mut budget = budget(100, 100);
        budget
            .attach_budget_policy(&ModelBudgetPolicy {
                route_identity: Some("fixture-route".to_owned()),
                max_cost_micros: Some(1),
                pricing: Some(equal_rates),
            })
            .unwrap();
        assert!(matches!(
            budget.record_turn(1, 1, None),
            Err(ResearchError::CostBudgetExceeded {
                actual: 2,
                maximum: 1
            })
        ));
    }

    #[test]
    fn per_call_ceiling_is_bounded_by_whole_task_cost_remaining() {
        let mut budget = budget(100, 100);
        budget.attach_budget_policy(&policy(Some(22))).unwrap();
        // Estimated input costs 2 micros, leaving 20; worst output rate is 4.
        assert_eq!(budget.output_tokens_for_call(1).unwrap(), 5);
    }

    #[test]
    fn hard_cost_limit_fails_closed_for_unknown_price_or_failed_usage() {
        let mut missing_price = budget(100, 100);
        assert!(matches!(
            missing_price.attach_budget_policy(&ModelBudgetPolicy {
                route_identity: Some("fixture-route".to_owned()),
                max_cost_micros: Some(10),
                pricing: None,
            }),
            Err(ResearchError::PricingUnavailable)
        ));

        let mut failed = budget(100, 100);
        failed.attach_budget_policy(&policy(Some(100))).unwrap();
        assert!(matches!(
            failed.record_failed_turn(3),
            Err(ResearchError::CostUsageUnknown)
        ));
        assert!(matches!(
            failed.output_tokens_for_call(1),
            Err(ResearchError::CostUsageUnknown)
        ));
    }

    #[test]
    fn pricing_snapshot_drift_is_rejected_by_identity_hash() {
        let mut budget = budget(100, 100);
        budget.attach_budget_policy(&policy(Some(100))).unwrap();
        let drifted = ModelBudgetPolicy {
            route_identity: Some("fixture-route".to_owned()),
            max_cost_micros: Some(100),
            pricing: Some(pricing("v2")),
        };
        assert_ne!(
            budget_policy_hash(&policy(Some(100))).unwrap(),
            budget_policy_hash(&drifted).unwrap()
        );
        assert!(matches!(
            budget.attach_budget_policy(&drifted),
            Err(ResearchError::BudgetPolicyMismatch)
        ));

        let mut route_drift = policy(Some(100));
        route_drift.route_identity = Some("different-route".to_owned());
        assert_ne!(
            budget_policy_hash(&policy(Some(100))).unwrap(),
            budget_policy_hash(&route_drift).unwrap()
        );
        assert!(matches!(
            budget.attach_budget_policy(&route_drift),
            Err(ResearchError::BudgetPolicyMismatch)
        ));
    }

    #[test]
    fn crash_recovery_restores_reasoning_cached_and_cost_ledger() {
        let mut budget = budget(100, 100);
        budget.attach_budget_policy(&policy(Some(100))).unwrap();
        let checkpoint = AgentRecoveryCheckpoint {
            source: AgentRecoverySource::Recovered(vec![AttemptId::new()]),
            phase: AgentTurnPhase::Submit,
            next_model_turn: 2,
            continuation: None,
            pending_tool_outputs: vec![],
            trace_refs: vec![],
            provider_calls: 2,
            tool_calls: 1,
            usage: AgentRecoveryUsage {
                latency_millis: 10,
                input_tokens: 20,
                cached_input_tokens: 8,
                output_tokens: 10,
                reasoning_tokens: 4,
                cost_micros: 66,
                cost_complete: true,
                usage_valid: true,
            },
        };
        budget.restore(&checkpoint).unwrap();
        assert_eq!(budget.model_calls, 2);
        assert_eq!(budget.tool_calls, 1);
        assert_eq!(budget.input_tokens, 20);
        assert_eq!(budget.cached_input_tokens, 8);
        assert_eq!(budget.output_tokens, 10);
        assert_eq!(budget.reasoning_tokens, 4);
        assert_eq!(budget.cost_micros, 66);
    }

    #[test]
    fn retries_inherit_ledger_while_an_explicit_fresh_budget_resets_it() {
        let mut retry_budget = budget(100, 100);
        retry_budget.attach_budget_policy(&policy(None)).unwrap();
        retry_budget.record_turn(4, 3, None).unwrap();
        retry_budget.record_model_call().unwrap();
        assert_eq!((retry_budget.input_tokens, retry_budget.output_tokens), (4, 3));
        assert_eq!(retry_budget.model_calls, 1);

        let mut fresh = budget(100, 100);
        fresh.attach_budget_policy(&policy(None)).unwrap();
        assert_eq!((fresh.input_tokens, fresh.output_tokens), (0, 0));
        assert_eq!(fresh.model_calls, 0);
        assert_eq!(fresh.cost_micros, 0);
    }

    #[test]
    fn model_and_tool_rounds_share_one_cumulative_budget() {
        let mut budget = budget(10, 10);
        budget.attach_budget_policy(&policy(None)).unwrap();
        budget.record_model_call().unwrap();
        budget.record_tool_calls(1).unwrap();
        budget.record_turn(5, 5, None).unwrap();
        budget.record_model_call().unwrap();
        budget.record_tool_calls(1).unwrap();
        budget.record_turn(5, 5, None).unwrap();
        assert!(matches!(
            budget.record_tool_calls(1),
            Err(ResearchError::ToolBudgetExceeded)
        ));
        assert!(matches!(
            budget.record_turn(1, 1, None),
            Err(ResearchError::InputBudgetExceeded { actual: 11, .. })
        ));
    }
}
