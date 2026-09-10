#[derive(Debug, Clone)]
pub struct AgentRuntime {
    store: Store,
    store_executor: StoreExecutor,
    context: ContextBroker,
    catalogue: ContractCatalogue,
    grant_ttl: Duration,
    historical_projection: Option<HistoricalProjection>,
    reasoning_events: Option<broadcast::Sender<AgentReasoningEvent>>,
}

#[derive(Debug)]
pub struct AgentRunBudget {
    started: Instant,
    wall_time: StdDuration,
    max_model_calls: Option<u32>,
    model_calls: u32,
    max_input_tokens: u32,
    input_tokens: u32,
    cached_input_tokens: u64,
    max_output_tokens: u32,
    output_tokens: u32,
    reasoning_tokens: u64,
    max_tool_calls: akzio_domain::budget::ToolCallLimit,
    tool_calls: u32,
    cost_micros: u64,
    cost_complete: bool,
    budget_policy: Option<ModelBudgetPolicy>,
    budget_policy_hash: Option<akzio_domain::ContentHash>,
}

impl AgentRunBudget {
    fn resolved_policy(&self) -> TaskBudget {
        TaskBudget {
            max_input_tokens: self.max_input_tokens,
            max_output_tokens: self.max_output_tokens,
            max_tool_calls: self.max_tool_calls,
            max_wall_time_secs: self.wall_time.as_secs() as u32,
        }
    }

    fn debug_observation(&self, boundary: &str) -> serde_json::Value {
        serde_json::json!({"resolved":self.resolved_policy(),"boundary":boundary,"scope":"current_agent_budget_including_recovery",
            "input_tokens_used":self.input_tokens,"input_tokens_remaining":self.max_input_tokens.saturating_sub(self.input_tokens),
            "output_tokens_used":self.output_tokens,"output_tokens_remaining":self.max_output_tokens.saturating_sub(self.output_tokens),
            "tool_calls_used":self.tool_calls,"tool_calls_remaining":self.max_tool_calls.remaining(u64::from(self.tool_calls)),
            "model_calls_used":self.model_calls,"model_calls_remaining":self.max_model_calls.map(|limit| limit.saturating_sub(self.model_calls)),
            "elapsed_millis":self.started.elapsed().as_millis(),"wall_time_remaining_millis":self.wall_time.saturating_sub(self.started.elapsed()).as_millis(),
            "cost_micros":self.cost_complete.then_some(self.cost_micros),"budget_policy_hash":self.budget_policy_hash})
    }
    pub fn new(policy: &TaskBudget, retry: &RetryPolicy) -> Self {
        Self {
            started: Instant::now(),
            wall_time: StdDuration::from_secs(u64::from(policy.max_wall_time_secs)),
            max_model_calls: policy
                .max_tool_calls
                .finite()
                .map(|limit| u32::from(retry.max_attempts).saturating_mul(u32::from(limit) + 3)),
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

    fn authorize_model_call(&mut self, input_tokens: u32) -> ResearchResult<()> {
        self.check_input(input_tokens)?;
        self.record_model_call()
    }

    fn record_model_call(&mut self) -> ResearchResult<()> {
        if self
            .max_model_calls
            .is_some_and(|limit| self.model_calls >= limit)
        {
            return Err(ResearchError::ModelCallBudgetExceeded);
        }
        self.model_calls = self.model_calls.saturating_add(1);
        Ok(())
    }

    fn checked_input_total(&self, tokens: u32) -> ResearchResult<u32> {
        let actual =
            self.input_tokens
                .checked_add(tokens)
                .ok_or(ResearchError::InputBudgetExceeded {
                    actual: u32::MAX,
                    maximum: self.max_input_tokens,
                })?;
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
        let actual =
            self.output_tokens
                .checked_add(tokens)
                .ok_or(ResearchError::OutputBudgetExceeded {
                    actual: u32::MAX,
                    maximum: self.max_output_tokens,
                })?;
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

    fn record_tool_calls(&mut self, calls: u32) -> ResearchResult<()> {
        let actual = self
            .tool_calls
            .checked_add(calls)
            .ok_or(ResearchError::ToolBudgetExceeded)?;
        if !self.max_tool_calls.allows(u64::from(actual)) {
            return Err(ResearchError::ToolBudgetExceeded);
        }
        self.tool_calls = actual;
        Ok(())
    }

    fn restore(&mut self, checkpoint: &AgentRecoveryCheckpoint) -> ResearchResult<()> {
        if !checkpoint.usage.usage_valid {
            return Err(ResearchError::InvalidProviderUsage);
        }
        if self
            .max_model_calls
            .is_some_and(|limit| checkpoint.provider_calls > limit)
        {
            return Err(ResearchError::ModelCallBudgetExceeded);
        }
        let tool_calls = checkpoint.tool_calls;
        if !self.max_tool_calls.allows(u64::from(tool_calls)) {
            return Err(ResearchError::ToolBudgetExceeded);
        }
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
mod configured_budget_tests {
    use super::*;

    #[test]
    fn resolved_budget_governs_cumulative_calls_and_inspection() {
        let mut config = akzio_domain::AgentBudgetConfig::default();
        config.analyst.max_input_tokens = Some(1_000_000);
        let frozen = config.resolve("research.analyst").unwrap();
        let mut budget = AgentRunBudget::new(&frozen, &RetryPolicy::none());
        config.analyst.max_input_tokens = Some(10);
        budget.authorize_model_call(256_000).unwrap();
        budget.record_input(256_000).unwrap();
        budget.authorize_model_call(744_000).unwrap();
        budget.record_input(744_000).unwrap();
        assert!(budget.authorize_model_call(1).is_err());
        let view = budget.debug_observation("test");
        assert_eq!(view["resolved"]["max_input_tokens"], 1_000_000);
        assert_eq!(view["input_tokens_used"], 1_000_000);
        assert_eq!(view["input_tokens_remaining"], 0);
        assert_eq!(budget.model_calls, 2);

        let mut small = AgentRunBudget::new(
            &config.resolve("research.analyst").unwrap(),
            &RetryPolicy::none(),
        );
        small.record_input(7).unwrap();
        assert!(small.authorize_model_call(4).is_err());
        assert_eq!(small.model_calls, 0);
        small.authorize_model_call(3).unwrap();
    }

    #[test]
    fn unlimited_tools_and_derived_model_calls_survive_recovery() {
        let policy = akzio_domain::budget::default_agent_budget("research.analyst").unwrap();
        let mut budget = AgentRunBudget::new(&policy, &RetryPolicy::none());
        budget.record_tool_calls(70_000).unwrap();
        for _ in 0..32 {
            budget.authorize_model_call(1).unwrap();
            budget.record_input(1).unwrap();
        }
        assert_eq!(budget.max_model_calls, None);
        let mut recovery = AgentRecoveryCheckpoint::fresh();
        recovery.tool_calls = budget.tool_calls;
        recovery.provider_calls = budget.model_calls;
        recovery.usage.input_tokens = u64::from(budget.input_tokens);
        let mut restored = AgentRunBudget::new(&policy, &RetryPolicy::none());
        restored.restore(&recovery).unwrap();
        restored.record_tool_calls(1).unwrap();
        let view = restored.debug_observation("recovered");
        assert_eq!(view["resolved"]["max_tool_calls"], "unlimited");
        assert_eq!(view["tool_calls_used"], 70_001);
        assert!(view["tool_calls_remaining"].is_null());
        assert!(view["model_calls_remaining"].is_null());
        let mut finite = policy;
        finite.max_tool_calls = akzio_domain::budget::ToolCallLimit::Limited(2);
        let mut limited = AgentRunBudget::new(&finite, &RetryPolicy::none());
        limited.record_tool_calls(2).unwrap();
        assert!(limited.record_tool_calls(1).is_err());
        assert!(limited.restore(&recovery).is_err());
    }

    #[test]
    fn maximum_integer_budget_does_not_hide_accumulation_overflow() {
        let mut policy = akzio_domain::budget::default_agent_budget("research.analyst").unwrap();
        policy.max_input_tokens = u32::MAX;
        policy.max_output_tokens = u32::MAX;
        policy.max_tool_calls = akzio_domain::budget::ToolCallLimit::Limited(u16::MAX);
        let mut budget = AgentRunBudget::new(&policy, &RetryPolicy::none());
        budget.record_input(u32::MAX).unwrap();
        assert!(budget.authorize_model_call(1).is_err());
        budget.output_tokens = u32::MAX;
        assert!(budget.checked_output_total(1).is_err());
        budget.record_tool_calls(u32::from(u16::MAX)).unwrap();
        assert!(budget.record_tool_calls(1).is_err());
    }
}
