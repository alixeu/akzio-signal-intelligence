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
    max_output_tokens: u32,
    output_tokens: u32,
    max_tool_calls: u16,
    tool_calls: u16,
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
            max_output_tokens: policy.max_output_tokens,
            output_tokens: 0,
            max_tool_calls: policy.max_tool_calls,
            tool_calls: 0,
        }
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

    fn record_turn(
        &mut self,
        estimated_input: u32,
        estimated_output: u32,
        telemetry: Option<&AgentTurnTelemetry>,
    ) -> ResearchResult<()> {
        let reported = |tokens| u32::try_from(tokens).unwrap_or(u32::MAX);
        let input = telemetry
            .and_then(|telemetry| telemetry.input_tokens)
            .map_or(estimated_input, reported);
        let output = telemetry
            .and_then(|telemetry| telemetry.output_tokens)
            .map_or(estimated_output, reported);
        let input_total = self.checked_input_total(input)?;
        let output_total = self.checked_output_total(output)?;
        self.input_tokens = input_total;
        self.output_tokens = output_total;
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

        self.model_calls = checkpoint.provider_calls;
        self.tool_calls = tool_calls;
        self.input_tokens = input_tokens;
        self.output_tokens = output_tokens;
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
            output_tokens,
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
