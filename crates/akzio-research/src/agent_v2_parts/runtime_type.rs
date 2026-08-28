#[derive(Debug, Clone)]
pub struct AgentRuntime {
    store: V2Store,
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

    /// `max_input_tokens` is a per-request prompt ceiling, not a run allowance:
    /// the contract catalogue sets `ContextPolicy.max_tokens` to this same
    /// value, so one legitimate request may fill it entirely. Because the
    /// Draft -> Submit contract always sends the manifest at least twice, a
    /// cumulative ceiling would fail-closed on the Submit turn of exactly the
    /// richest contexts. Call volume is bounded by `max_model_calls` instead;
    /// the field keeps the observed peak.
    fn record_input(&mut self, tokens: u32) -> ResearchResult<()> {
        if tokens > self.max_input_tokens {
            return Err(ResearchError::InputBudgetExceeded {
                actual: tokens,
                maximum: self.max_input_tokens,
            });
        }
        self.input_tokens = self.input_tokens.max(tokens);
        Ok(())
    }

    /// Per-response ceiling, for the same reason as `record_input`: it is
    /// forwarded to the provider as the request's `max_output_tokens`.
    fn record_output(&mut self, tokens: u32) -> ResearchResult<()> {
        if tokens > self.max_output_tokens {
            return Err(ResearchError::OutputBudgetExceeded {
                actual: tokens,
                maximum: self.max_output_tokens,
            });
        }
        self.output_tokens = self.output_tokens.max(tokens);
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
}
