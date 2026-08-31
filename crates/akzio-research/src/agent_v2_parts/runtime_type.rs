#[derive(Debug, Clone)]
pub struct AgentRuntime {
    store: V2Store,
    store_executor: StoreExecutor,
    context: ContextBroker,
    catalogue: ContractCatalogue,
    grant_ttl: Duration,
    reasoning_events: Option<broadcast::Sender<AgentReasoningEvent>>,
}

#[derive(Debug)]
pub struct AgentRunBudget {
    started: Instant,
    wall_time: StdDuration,
    max_model_calls: u32,
    model_calls: u32,
    max_input_tokens: u32,
    input_tokens: u32,
    cached_input_tokens: u64,
    max_output_tokens: u32,
    output_tokens: u32,
    reasoning_tokens: u64,
    max_tool_calls: u16,
    tool_calls: u16,
    cost_micros: u64,
    cost_complete: bool,
    budget_policy: Option<ModelBudgetPolicy>,
    budget_policy_hash: Option<akzio_domain::ContentHash>,
}

impl AgentRunBudget {
    pub fn new(policy: &TaskBudget, retry: &RetryPolicy) -> Self {
        Self {
            started: Instant::now(),
            wall_time: StdDuration::from_secs(u64::from(policy.max_wall_time_secs)),
            max_model_calls: u32::from(retry.max_attempts)
                .saturating_mul(u32::from(policy.max_tool_calls) + 3),
            model_calls: 0,
            max_input_tokens: policy.max_input_tokens,
            input_tokens: 0,
            cached_input_tokens: 0,
            max_output_tokens: policy.max_output_tokens,
            output_tokens: 0,
            reasoning_tokens: 0,
            max_tool_calls: policy.max_tool_calls,
            tool_calls: 0,
            cost_micros: 0,
            cost_complete: false,
            budget_policy: None,
            budget_policy_hash: None,
        }
    }

    fn attach_budget_policy(&mut self, policy: &ModelBudgetPolicy) -> ResearchResult<()> {
        validate_budget_policy(policy)?;
        let hash = budget_policy_hash(policy)?;
        let first_policy = self.budget_policy_hash.is_none();
        if self
            .budget_policy_hash
            .as_ref()
            .is_some_and(|existing| existing != &hash)
        {
            return Err(ResearchError::BudgetPolicyMismatch);
        }
        self.budget_policy = Some(policy.clone());
        self.budget_policy_hash = Some(hash);
        if first_policy {
            self.cost_complete = policy.pricing.is_some();
        }
        Ok(())
    }

    fn check_wall(&self) -> ResearchResult<()> {
        if self.started.elapsed() > self.wall_time {
            return Err(ResearchError::WallTimeExceeded {
                maximum_secs: u32::try_from(self.wall_time.as_secs()).unwrap_or(u32::MAX),
            });
        }
        Ok(())
    }

    fn record_model_call(&mut self) -> ResearchResult<()> {
        if self.model_calls >= self.max_model_calls {
            return Err(ResearchError::ModelCallBudgetExceeded);
        }
        self.model_calls = self.model_calls.saturating_add(1);
        Ok(())
    }

    fn checked_input_total(&self, tokens: u32) -> ResearchResult<u32> {
        let actual = self.input_tokens.saturating_add(tokens);
        if actual > self.max_input_tokens {
            return Err(ResearchError::InputBudgetExceeded {
                actual,
                maximum: self.max_input_tokens,
            });
        }
        Ok(actual)
    }

    fn check_input(&self, tokens: u32) -> ResearchResult<()> {
        self.checked_input_total(tokens).map(drop)
    }

    fn record_input(&mut self, tokens: u32) -> ResearchResult<()> {
        self.input_tokens = self.checked_input_total(tokens)?;
        Ok(())
    }

    fn checked_output_total(&self, tokens: u32) -> ResearchResult<u32> {
        let actual = self.output_tokens.saturating_add(tokens);
        if actual > self.max_output_tokens {
            return Err(ResearchError::OutputBudgetExceeded {
                actual,
                maximum: self.max_output_tokens,
            });
        }
        Ok(actual)
    }

    fn remaining_output_tokens(&self) -> ResearchResult<u32> {
        let remaining = self.max_output_tokens.saturating_sub(self.output_tokens);
        (remaining > 0)
            .then_some(remaining)
            .ok_or(ResearchError::OutputBudgetExceeded {
                actual: self.output_tokens.saturating_add(1),
                maximum: self.max_output_tokens,
            })
    }

    /// Per-call output ceiling constrained by both the remaining whole-task
    /// token budget and the configured whole-task cost cap.
    fn output_tokens_for_call(&self, estimated_input: u32) -> ResearchResult<u32> {
        let token_ceiling = self.remaining_output_tokens()?;
        let Some(policy) = &self.budget_policy else {
            return Ok(token_ceiling);
        };
        let Some(maximum) = policy.max_cost_micros else {
            return Ok(token_ceiling);
        };
        if !self.cost_complete {
            return Err(ResearchError::CostUsageUnknown);
        }
        let pricing = policy
            .pricing
            .as_ref()
            .ok_or(ResearchError::PricingUnavailable)?;
        let input_cost = conservative_input_cost_micros(estimated_input, pricing)?;
        let after_input = self
            .cost_micros
            .checked_add(input_cost)
            .ok_or(ResearchError::CostOverflow)?;
        if after_input > maximum {
            return Err(ResearchError::CostBudgetExceeded {
                actual: after_input,
                maximum,
            });
        }
        let affordable = affordable_output_tokens(maximum - after_input, pricing);
        let ceiling = token_ceiling.min(u32::try_from(affordable).unwrap_or(u32::MAX));
        if ceiling == 0 {
            return Err(ResearchError::CostBudgetExceeded {
                actual: maximum.saturating_add(1),
                maximum,
            });
        }
        Ok(ceiling)
    }

    fn record_turn(
        &mut self,
        estimated_input: u32,
        estimated_output: u32,
        telemetry: Option<&AgentTurnTelemetry>,
    ) -> ResearchResult<()> {
        let usage = resolve_model_usage(estimated_input, estimated_output, telemetry);
        let input = u32::try_from(usage.input_tokens).unwrap_or(u32::MAX);
        let output = u32::try_from(usage.output_tokens).unwrap_or(u32::MAX);
        let input_total = self.checked_input_total(input)?;
        let output_total = self.checked_output_total(output)?;
        let cached_input_tokens = self
            .cached_input_tokens
            .checked_add(usage.cached_input_tokens.unwrap_or_default())
            .ok_or(ResearchError::CostOverflow)?;
        let reasoning_tokens = self
            .reasoning_tokens
            .checked_add(usage.reasoning_tokens.unwrap_or_default())
            .ok_or(ResearchError::CostOverflow)?;
        let turn_cost = self
            .budget_policy
            .as_ref()
            .and_then(|policy| policy.pricing.as_ref())
            .map(|pricing| usage_cost_micros(usage, pricing))
            .transpose()?;
        let cost_micros = turn_cost.map_or(Ok(self.cost_micros), |cost| {
            self.cost_micros
                .checked_add(cost)
                .ok_or(ResearchError::CostOverflow)
        })?;
        if let Some(maximum) = self
            .budget_policy
            .as_ref()
            .and_then(|policy| policy.max_cost_micros)
        {
            if cost_micros > maximum {
                return Err(ResearchError::CostBudgetExceeded {
                    actual: cost_micros,
                    maximum,
                });
            }
        }
        self.input_tokens = input_total;
        self.cached_input_tokens = cached_input_tokens;
        self.output_tokens = output_total;
        self.reasoning_tokens = reasoning_tokens;
        self.cost_micros = cost_micros;
        Ok(())
    }

    fn record_failed_turn(&mut self, estimated_input: u32) -> ResearchResult<()> {
        self.record_input(estimated_input)?;
        if self
            .budget_policy
            .as_ref()
            .is_some_and(|policy| policy.pricing.is_some())
        {
            self.cost_complete = false;
        }
        if self
            .budget_policy
            .as_ref()
            .is_some_and(|policy| policy.max_cost_micros.is_some())
        {
            return Err(ResearchError::CostUsageUnknown);
        }
        Ok(())
    }

    fn record_tool_calls(&mut self, calls: u16) -> ResearchResult<()> {
        let actual = self.tool_calls.saturating_add(calls);
        if actual > self.max_tool_calls {
            return Err(ResearchError::ToolBudgetExceeded);
        }
        self.tool_calls = actual;
        Ok(())
    }

    fn restore(&mut self, checkpoint: &AgentRecoveryCheckpoint) -> ResearchResult<()> {
        if !checkpoint.usage.usage_valid {
            return Err(ResearchError::InvalidProviderUsage);
        }
        if checkpoint.provider_calls > self.max_model_calls {
            return Err(ResearchError::ModelCallBudgetExceeded);
        }
        let tool_calls = u16::try_from(checkpoint.tool_calls)
            .ok()
            .filter(|calls| *calls <= self.max_tool_calls)
            .ok_or(ResearchError::ToolBudgetExceeded)?;
        let input_tokens = u32::try_from(checkpoint.usage.input_tokens).map_err(|_| {
            ResearchError::InputBudgetExceeded {
                actual: u32::MAX,
                maximum: self.max_input_tokens,
            }
        })?;
        if input_tokens > self.max_input_tokens {
            return Err(ResearchError::InputBudgetExceeded {
                actual: input_tokens,
                maximum: self.max_input_tokens,
            });
        }
        let output_tokens = u32::try_from(checkpoint.usage.output_tokens).map_err(|_| {
            ResearchError::OutputBudgetExceeded {
                actual: u32::MAX,
                maximum: self.max_output_tokens,
            }
        })?;
        if output_tokens > self.max_output_tokens {
            return Err(ResearchError::OutputBudgetExceeded {
                actual: output_tokens,
                maximum: self.max_output_tokens,
            });
        }
        if !checkpoint.usage.cost_complete
            && self
                .budget_policy
                .as_ref()
                .is_some_and(|policy| policy.max_cost_micros.is_some())
        {
            return Err(ResearchError::CostUsageUnknown);
        }
        if let Some(maximum) = self
            .budget_policy
            .as_ref()
            .and_then(|policy| policy.max_cost_micros)
        {
            if checkpoint.usage.cost_micros > maximum {
                return Err(ResearchError::CostBudgetExceeded {
                    actual: checkpoint.usage.cost_micros,
                    maximum,
                });
            }
        }

        self.model_calls = checkpoint.provider_calls;
        self.tool_calls = tool_calls;
        self.input_tokens = input_tokens;
        self.cached_input_tokens = checkpoint.usage.cached_input_tokens;
        self.output_tokens = output_tokens;
        self.reasoning_tokens = checkpoint.usage.reasoning_tokens;
        self.cost_micros = checkpoint.usage.cost_micros;
        self.cost_complete = checkpoint.usage.cost_complete
            && self
                .budget_policy
                .as_ref()
                .is_some_and(|policy| policy.pricing.is_some());
        Ok(())
    }
}

#[cfg(test)]
mod run_budget_tests {
    use super::*;

    fn budget(max_input_tokens: u32, max_output_tokens: u32) -> AgentRunBudget {
        AgentRunBudget::new(
            &TaskBudget {
                max_input_tokens,
                max_output_tokens,
                max_wall_time_secs: 60,
                max_tool_calls: 2,
            },
            &RetryPolicy {
                max_attempts: 1,
                initial_backoff_ms: 0,
                retry_transport: false,
                retry_rate_limited: false,
                retry_invalid_output: false,
            },
        )
    }

    fn telemetry(input_tokens: Option<u64>, output_tokens: Option<u64>) -> AgentTurnTelemetry {
        AgentTurnTelemetry {
            latency_millis: 1,
            input_tokens,
            cached_input_tokens: None,
            output_tokens,
            reasoning_tokens: None,
        }
    }

    #[test]
    fn task_token_budget_rejects_multi_turn_cumulative_usage() {
        let mut input = budget(10, 100);
        input.record_turn(6, 1, None).unwrap();
        assert!(matches!(
            input.record_turn(5, 1, None),
            Err(ResearchError::InputBudgetExceeded { actual: 11, .. })
        ));

        let mut output = budget(100, 10);
        output.record_turn(1, 6, None).unwrap();
        assert!(matches!(
            output.record_turn(1, 5, None),
            Err(ResearchError::OutputBudgetExceeded { actual: 11, .. })
        ));
    }

    #[test]
    fn reported_usage_replaces_estimates_without_double_counting() {
        let mut budget = budget(10, 10);
        let telemetry = telemetry(Some(2), Some(2));
        for _ in 0..3 {
            budget.check_input(4).unwrap();
            budget.record_turn(4, 4, Some(&telemetry)).unwrap();
        }
        assert_eq!((budget.input_tokens, budget.output_tokens), (6, 6));
    }

    #[test]
    fn missing_reported_usage_falls_back_per_field() {
        let mut budget = budget(10, 10);
        budget
            .record_turn(5, 5, Some(&telemetry(Some(2), None)))
            .unwrap();
        assert_eq!((budget.input_tokens, budget.output_tokens), (2, 5));
    }

    #[test]
    fn single_turn_within_budget_still_passes() {
        let mut budget = budget(10, 10);
        budget.check_input(6).unwrap();
        assert_eq!(budget.remaining_output_tokens().unwrap(), 10);
        budget.record_turn(6, 4, None).unwrap();
        assert_eq!((budget.input_tokens, budget.output_tokens), (6, 4));
    }

    #[test]
    fn failed_retry_input_is_charged_before_next_preflight() {
        let mut budget = budget(10, 10);
        budget.check_input(6).unwrap();
        budget.record_input(6).unwrap();
        assert!(matches!(
            budget.check_input(6),
            Err(ResearchError::InputBudgetExceeded { actual: 12, .. })
        ));
    }
}
