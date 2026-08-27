#[derive(Debug, Clone)]
pub struct AgentRuntime {
    store: V2Store,
    context: ContextBroker,
    catalogue: ContractCatalogue,
    grant_ttl: Duration,
    reasoning_events: Option<broadcast::Sender<AgentReasoningEvent>>,
}
