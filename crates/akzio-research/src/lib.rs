//! Contract-driven research plane for Akzio v2.
//!
//! The root API permits only installed immutable contracts, bound model turns,
//! schema-validated artifacts, and grant-checked tools.

mod agent_v2;
mod fixture;

pub mod v2 {
    pub use crate::agent_v2::{
        ActiveResearchCatalogue, AgentModel, AgentModelRequest, AgentModelTurn,
        AgentReasoningEvent, AgentRuntime, AgentTerminalDefinition, AgentTerminalSubmission,
        AgentToolCall, AgentToolDefinition, AgentTurnPhase, ContractCatalogue, InstalledContract,
        ModelClientAdapter, ResearchError, ResearchResult as Result, ACTIVE_RESEARCH_MAX_NODES,
    };
    pub use crate::fixture::{fixture_claim_output, fixture_critique_output, fixture_model_client};
}

pub use v2::*;
