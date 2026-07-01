use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum DomainError {
    #[error("missing required config key: {0}")]
    ConfigMissing(String),

    #[error("prompt template not found: {0}")]
    PromptNotFound(PathBuf),

    #[error("invalid phase range: from={from}, to={to}")]
    InvalidPhaseRange { from: i64, to: i64 },

    #[error("role {role} timed out after {sec}s")]
    RoleTimeout { role: String, sec: u64 },

    #[error("role {role} failed: {reason}")]
    RoleFailed { role: String, reason: String },

    #[error("data source '{0}' returned no data")]
    EmptyDataSource(String),

    #[error("invalid ticker: {0}")]
    InvalidTicker(String),

    #[error("artifact validation failed: {0}")]
    ArtifactValidation(String),

    #[error("memory update validation failed at item {index}: {detail}")]
    MemoryValidation { index: usize, detail: String },
}
