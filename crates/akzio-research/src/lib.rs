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
            "crates/akzio-research/src/agent_v2.rs",
            include_bytes!("agent_v2.rs"),
        ),
        (
            "crates/akzio-research/src/agent_v2/catalogue.rs",
            include_bytes!("agent_v2/catalogue.rs"),
        ),
        (
            "crates/akzio-research/src/agent_v2/schemas.rs",
            include_bytes!("agent_v2/schemas.rs"),
        ),
        (
            "crates/akzio-research/src/agent_v2/tools.rs",
            include_bytes!("agent_v2/tools.rs"),
        ),
        (
            "crates/akzio-research/src/agent_v2/validation.rs",
            include_bytes!("agent_v2/validation.rs"),
        ),
        (
            "crates/akzio-research/src/agent_v2_parts/errors_catalogue.rs",
            include_bytes!("agent_v2_parts/errors_catalogue.rs"),
        ),
        (
            "crates/akzio-research/src/agent_v2_parts/model_types.rs",
            include_bytes!("agent_v2_parts/model_types.rs"),
        ),
        (
            "crates/akzio-research/src/agent_v2_parts/runtime_type.rs",
            include_bytes!("agent_v2_parts/runtime_type.rs"),
        ),
        (
            "crates/akzio-research/src/agent_v2_parts/runtime_core.rs",
            include_bytes!("agent_v2_parts/runtime_core.rs"),
        ),
        (
            "crates/akzio-research/src/agent_v2_parts/runtime_run.rs",
            include_bytes!("agent_v2_parts/runtime_run.rs"),
        ),
        (
            "crates/akzio-research/src/agent_v2_parts/runtime_helpers.rs",
            include_bytes!("agent_v2_parts/runtime_helpers.rs"),
        ),
        (
            "crates/akzio-research/src/agent_v2_parts/helpers.rs",
            include_bytes!("agent_v2_parts/helpers.rs"),
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
            "crates/akzio-research/src/agent_v2.rs",
            include_bytes!("agent_v2.rs"),
        ),
        (
            "crates/akzio-research/src/agent_v2/catalogue.rs",
            include_bytes!("agent_v2/catalogue.rs"),
        ),
        (
            "crates/akzio-research/src/agent_v2/schemas.rs",
            include_bytes!("agent_v2/schemas.rs"),
        ),
        (
            "crates/akzio-research/src/agent_v2/tools.rs",
            include_bytes!("agent_v2/tools.rs"),
        ),
        (
            "crates/akzio-research/src/agent_v2/validation.rs",
            include_bytes!("agent_v2/validation.rs"),
        ),
        (
            "crates/akzio-research/src/agent_v2_parts/errors_catalogue.rs",
            include_bytes!("agent_v2_parts/errors_catalogue.rs"),
        ),
        (
            "crates/akzio-research/src/agent_v2_parts/model_types.rs",
            include_bytes!("agent_v2_parts/model_types.rs"),
        ),
        (
            "crates/akzio-research/src/agent_v2_parts/runtime_type.rs",
            include_bytes!("agent_v2_parts/runtime_type.rs"),
        ),
        (
            "crates/akzio-research/src/agent_v2_parts/runtime_core.rs",
            include_bytes!("agent_v2_parts/runtime_core.rs"),
        ),
        (
            "crates/akzio-research/src/agent_v2_parts/runtime_run.rs",
            include_bytes!("agent_v2_parts/runtime_run.rs"),
        ),
        (
            "crates/akzio-research/src/agent_v2_parts/runtime_helpers.rs",
            include_bytes!("agent_v2_parts/runtime_helpers.rs"),
        ),
        (
            "crates/akzio-research/src/agent_v2_parts/helpers.rs",
            include_bytes!("agent_v2_parts/helpers.rs"),
        ),
        ("crates/akzio-research/src/lib.rs", include_bytes!("lib.rs")),
    ])
}
