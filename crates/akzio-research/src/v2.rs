//! Versioned contract catalogue and schema-bound Agent runtime.

pub use crate::agent_v2::{
    ActiveResearchCatalogue, AgentModel, AgentModelRequest, AgentModelTurn, AgentReasoningEvent,
    AgentRuntime, AgentTerminalDefinition, AgentTerminalSubmission, AgentToolCall,
    AgentToolDefinition, AgentTurnPhase, ContractCatalogue, InstalledContract, ModelClientAdapter,
    ResearchError, ResearchResult as Result, ACTIVE_RESEARCH_MAX_NODES,
};
