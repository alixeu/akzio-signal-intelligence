//! Versioned contract catalogue and schema-bound Agent runtime.

pub use crate::agent_v2::{
    AgentModel, AgentModelRequest, AgentModelTurn, AgentToolCall, AgentToolDefinition,
    InstalledContract, ModelClientAdapter, RebuildAgentRuntime as AgentRuntime,
    RebuildContractCatalogue as ContractCatalogue, RebuildResearchError as ResearchError,
    RebuildResearchResult as Result,
};
