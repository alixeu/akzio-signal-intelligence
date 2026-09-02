//! Contract-driven research plane for Akzio.
//!
//! The root API permits only installed immutable contracts, bound model turns,
//! schema-validated artifacts, and grant-checked tools.

mod agent;
mod fixture;

pub use crate::agent::{
    ActiveResearchCatalogue, AgentModel, AgentModelRequest, AgentModelTurn, AgentReasoningEvent,
    AgentRunBudget, AgentRuntime, AgentTerminalDefinition, AgentTerminalSubmission, AgentToolCall,
    AgentToolDefinition, AgentTurnPhase, ContractCatalogue, InstalledContract, ModelClientAdapter,
    ResearchError, ResearchResult as Result, ACTIVE_RESEARCH_MAX_NODES,
};
pub use crate::fixture::{fixture_claim_output, fixture_critique_output, fixture_model_client};

use akzio_domain::ContentHash;

fn component_hash(components: &[(&str, &[u8])]) -> ContentHash {
    let mut bytes = Vec::new();
    for (path, component) in components {
        bytes.extend_from_slice(path.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(component);
        bytes.push(0);
    }
    ContentHash::of_bytes(&bytes)
}

pub fn prompt_component_hash() -> ContentHash {
    component_hash(&[
        (
            "crates/akzio-research/src/agent.rs",
            include_bytes!("agent.rs"),
        ),
        (
            "crates/akzio-research/src/agent/catalogue.rs",
            include_bytes!("agent/catalogue.rs"),
        ),
        (
            "crates/akzio-research/src/agent/schemas.rs",
            include_bytes!("agent/schemas.rs"),
        ),
        (
            "crates/akzio-research/src/agent/tools.rs",
            include_bytes!("agent/tools.rs"),
        ),
        (
            "crates/akzio-research/src/agent/validation.rs",
            include_bytes!("agent/validation.rs"),
        ),
        (
            "crates/akzio-research/src/agent/errors_catalogue.rs",
            include_bytes!("agent/errors_catalogue.rs"),
        ),
        (
            "crates/akzio-research/src/agent/model_types.rs",
            include_bytes!("agent/model_types.rs"),
        ),
        (
            "crates/akzio-research/src/agent/budget.rs",
            include_bytes!("agent/budget.rs"),
        ),
        (
            "crates/akzio-research/src/agent/runtime_type.rs",
            include_bytes!("agent/runtime_type.rs"),
        ),
        (
            "crates/akzio-research/src/agent/recovery.rs",
            include_bytes!("agent/recovery.rs"),
        ),
        (
            "crates/akzio-research/src/agent/runtime_core.rs",
            include_bytes!("agent/runtime_core.rs"),
        ),
        (
            "crates/akzio-research/src/agent/runtime_run.rs",
            include_bytes!("agent/runtime_run.rs"),
        ),
        (
            "crates/akzio-research/src/agent/runtime_helpers.rs",
            include_bytes!("agent/runtime_helpers.rs"),
        ),
        (
            "crates/akzio-research/src/agent/helpers.rs",
            include_bytes!("agent/helpers.rs"),
        ),
        ("crates/akzio-research/src/lib.rs", include_bytes!("lib.rs")),
    ])
}

pub fn contract_component_hash() -> ContentHash {
    component_hash(&[
        (
            "crates/akzio-domain/src/contract.rs",
            include_bytes!("../../akzio-domain/src/contract.rs"),
        ),
        (
            "crates/akzio-research/src/agent.rs",
            include_bytes!("agent.rs"),
        ),
        (
            "crates/akzio-research/src/agent/catalogue.rs",
            include_bytes!("agent/catalogue.rs"),
        ),
        (
            "crates/akzio-research/src/agent/schemas.rs",
            include_bytes!("agent/schemas.rs"),
        ),
        (
            "crates/akzio-research/src/agent/tools.rs",
            include_bytes!("agent/tools.rs"),
        ),
        (
            "crates/akzio-research/src/agent/validation.rs",
            include_bytes!("agent/validation.rs"),
        ),
        (
            "crates/akzio-research/src/agent/errors_catalogue.rs",
            include_bytes!("agent/errors_catalogue.rs"),
        ),
        (
            "crates/akzio-research/src/agent/model_types.rs",
            include_bytes!("agent/model_types.rs"),
        ),
        (
            "crates/akzio-research/src/agent/runtime_type.rs",
            include_bytes!("agent/runtime_type.rs"),
        ),
        (
            "crates/akzio-research/src/agent/runtime_core.rs",
            include_bytes!("agent/runtime_core.rs"),
        ),
        (
            "crates/akzio-research/src/agent/runtime_run.rs",
            include_bytes!("agent/runtime_run.rs"),
        ),
        (
            "crates/akzio-research/src/agent/runtime_helpers.rs",
            include_bytes!("agent/runtime_helpers.rs"),
        ),
        (
            "crates/akzio-research/src/agent/helpers.rs",
            include_bytes!("agent/helpers.rs"),
        ),
        ("crates/akzio-research/src/lib.rs", include_bytes!("lib.rs")),
    ])
}
