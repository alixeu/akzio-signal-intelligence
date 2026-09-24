//! Contract-driven research plane for Akzio.
//!
//! The root API permits only installed immutable contracts, bound model turns,
//! schema-validated artifacts, and grant-checked tools.

// 研究 crate 的公开边界只负责受 Contract 约束的研究计算：模型可以提交结构化
// 结果，但状态、授权、Context/Evidence 血缘和后续 Gate 仍由 Rust/Store 决定。
// 这里的组件哈希把实际参与运行时身份的源码与 Prompt 所有权一起纳入审计，
// 因此注释变更也会产生新的运行时身份，而不会悄悄复用旧 Run 的证明链。
mod agent;
mod fixture;
mod prompt_registry;
pub mod quality;

pub use crate::agent::{
    ActiveResearchCatalogue, AgentModel, AgentModelRequest, AgentModelTurn, AgentReasoningEvent,
    AgentRunBudget, AgentRuntime, AgentTerminalDefinition, AgentTerminalSubmission, AgentToolCall,
    AgentToolDefinition, AgentTurnPhase, ContractCatalogue, InstalledContract, ModelClientAdapter,
    ResearchError, ResearchResult as Result, ACTIVE_RESEARCH_MAX_NODES,
};
pub use crate::fixture::{fixture_claim_output, fixture_critique_output, fixture_model_client};

use akzio_domain::ContentHash;

fn component_hash(components: &[(&str, &[u8])]) -> ContentHash {
    // 路径分隔符和源码字节都进入同一个 CAS 哈希；BTree/Vec 的顺序由调用方
    // 固定，避免相同内容换文件后仍被误认为同一份 Contract 组件。
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
    // Prompt 哈希不仅覆盖 Markdown，也覆盖负责生成请求、Schema 和工具边界的
    // Rust 代码。这样 Submit/Outcome 协议的渲染规则变化会显式失效旧身份。
    let mut components: Vec<(&str, &[u8])> = vec![
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
    ];
    components.push((
        "crates/akzio-research/src/prompt_registry.rs",
        include_bytes!("prompt_registry.rs"),
    ));
    components.extend(prompt_registry::components(false));
    component_hash(&components)
}

pub fn contract_component_hash() -> ContentHash {
    // Contract 身份还包含 domain 的冻结定义和 ProposalReview 绑定代码；它是
    // 校准/Policy 身份的一部分，不代表模型已经完成研究或 Decision。
    let mut components: Vec<(&str, &[u8])> = vec![
        (
            "crates/akzio-domain/src/research_review.rs",
            include_bytes!("../../akzio-domain/src/research_review.rs"),
        ),
        (
            "crates/akzio-research/src/agent/proposal_review.rs",
            include_bytes!("agent/proposal_review.rs"),
        ),
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
    ];
    components.extend(prompt_registry::components(true));
    component_hash(&components)
}
