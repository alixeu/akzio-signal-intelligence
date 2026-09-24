// 文件导读：集中导出 Akzio 的领域类型、稳定标识符和纯校验辅助函数。
// 本模块只组织编译期的领域边界，不执行数据库、模型、网络、文件或券商 I/O。
// `pub mod` 暴露按职责分区的类型，`pub use` 提供常用类型的 facade；`core`/`schema` 保持 crate 内部实现细节。
//! Stable, Rust-owned domain facade for Akzio.
//!
//! This crate contains schemas and validation only: no database, model,
//! network, filesystem, or broker I/O.

// `macro_rules!` 在编译时把 `$name:ident` 展开为同一套 ID newtype；每个调用点仍生成独立类型，避免不同 ID 字符串互换。
macro_rules! id_type {
    ($name:ident) => {
        #[derive(
            Debug,
            Clone,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            serde::Serialize,
            serde::Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            // 输入无；输出为截取 16 个十六进制字符的新 UUID 标识。
            pub fn new() -> Self {
                let value = uuid::Uuid::new_v4().simple().to_string();
                Self(value[..16].to_owned())
            }
        }

        impl Default for $name {
            // 默认构造复用 new，确保所有 ID 类型使用相同生成规则。
            fn default() -> Self {
                Self::new()
            }
        }

        impl std::fmt::Display for $name {
            // 将包装的字符串直接写入格式化目标，不增加额外编码。
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

pub mod budget;
mod core;
pub use budget::{AgentBudgetConfig, AgentSettings};
mod schema;

pub mod artifact;
pub mod behavior;
pub mod canary;
pub mod context;
pub mod context_scope;
pub mod contract;
pub mod debug;
pub mod decision;
pub mod evaluation;
pub mod event;
pub mod execution;
pub mod experiment;
pub mod ids;
pub mod instrument_evidence;
pub mod lesson;
pub mod longitudinal;
pub mod market_safety;
pub mod qualification;
pub mod regime;
pub mod release;
pub mod research;
pub mod runtime_manifest;
pub mod workflow;
pub mod workflow_definition;

pub use artifact::*;
pub use behavior::*;
pub use canary::*;
pub use context::*;
pub use context_scope::*;
pub use contract::*;
pub use core::*;
pub use debug::*;
pub use decision::*;
pub use evaluation::*;
pub use event::*;
pub use execution::*;
pub use experiment::*;
pub use ids::{
    EvaluationId, ExperienceId, LessonId, OutcomeId, PaperCancelId, PaperCommitmentId,
    PaperRepriceId, PolicyTransitionId, ReconciliationId,
};
pub use instrument_evidence::*;
pub use lesson::*;
pub use longitudinal::*;
pub use market_safety::*;
pub use qualification::*;
pub use regime::*;
pub use release::*;
pub use research::*;
pub use runtime_manifest::*;
pub use schema::FactorLimits;
pub use workflow::*;
pub use workflow_definition::*;

/// Formal schema identity for the source-incompatible domain graph.
pub const DOMAIN_SCHEMA_VERSION: u32 = schema::SCHEMA_VERSION;

// 仅供冻结历史 Contract/Artifact 读取与识别；常量存在不恢复 Planner 创建或执行能力。
pub const RESEARCH_PLANNER_RECIPE_ID: &str = "research.planner";
pub const RESEARCH_ANALYST_RECIPE_ID: &str = "research.analyst";
pub const RESEARCH_CRITIC_RECIPE_ID: &str = "research.critic";
pub const RESEARCH_SYNTHESIZER_RECIPE_ID: &str = "research.synthesizer";
pub const LEARNING_OUTCOME_WORKER_RECIPE_ID: &str = "learning.outcome_worker";
pub const GOVERNED_EVIDENCE_SOURCE_FAMILIES: [&str; 4] =
    ["alpaca", "sec_edgar", "fred", "news_web"];

// 将字节数按约 4 字节一个 token 向上估算，并保证非空输入至少得到 1；TryFrom 在超出 u32 时饱和到上限。
pub fn estimate_tokens_from_bytes(bytes: u64) -> u32 {
    u32::try_from(bytes.div_ceil(4).max(1)).unwrap_or(u32::MAX)
}

// `T: Serialize` 是泛型 trait bound：调用方可传入任意可序列化借用值；JSON 编码失败由 Result 经 `?` 原路返回。
pub fn estimate_json_tokens<T: serde::Serialize>(value: &T) -> Result<u32, serde_json::Error> {
    let bytes = serde_json::to_vec(value)?.len() as u64;
    Ok(estimate_tokens_from_bytes(bytes))
}

pub mod research_review;
pub use research_review::*;
