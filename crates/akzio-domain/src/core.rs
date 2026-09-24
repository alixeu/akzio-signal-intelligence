// 文件导读：提供所有 crate 共用的领域错误、资产/金额/哈希标量、任务预算和生命周期枚举。
// 这里是 Rust 状态与序列化语义的基础层，保持纯计算，不接触外部 I/O。
//! The versioned, Rust-owned language shared by every Akzio module.
//!
//! This crate deliberately contains no database, model, network, or filesystem
//! dependency.  It is the single source of truth for contracts, workflow
//! state, evidence references, and execution intent.

//! Foundational scalar types shared by the domain graph.

use std::{collections::BTreeMap, fmt};

use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DomainError {
    #[error("asset {0:?} is not executable by Akzio")]
    UnsupportedAsset(String),
    #[error("content hash must be a lowercase sha256 hex digest")]
    InvalidContentHash,
    #[error("{field} must not be empty")]
    EmptyField { field: &'static str },
    #[error("task graph contains a duplicate task id {0}")]
    DuplicateTaskId(TaskId),
    #[error("task {task} references unknown dependency {dependency}")]
    UnknownDependency { task: TaskId, dependency: TaskId },
    #[error("unknown lifecycle event type {0}")]
    UnknownLifecycleEventType(String),
    #[error("evidence source {0} is not allowed by the installed recipe")]
    EvidenceSourceNotAllowed(String),
    #[error("task graph contains a cycle")]
    CyclicPlan,
    #[error("budget {field} must be positive")]
    InvalidBudget { field: &'static str },
    #[error("target portfolio must include exactly TQQQ, QQQ, SOXX, and SOXL")]
    InvalidTargetUniverse,
    #[error("decision confidence must be at most one million ppm")]
    InvalidDecisionConfidence,
    #[error(
        "decision forecasts must cover every executable asset at 1, 3, and 5 trading days exactly"
    )]
    InvalidDecisionForecastHorizons,
    #[error("decision forecast probability must be at most one million ppm")]
    InvalidDecisionForecastProbability,
    #[error("evidence ground scope is invalid")]
    InvalidEvidenceGroundScope,
    #[error(
        "verification reference {evidence} in {field} is not one of the submitted grounds; \
         every supporting_refs/conflicting_refs evidence must also appear in grounds with the same artifact_id and kind"
    )]
    VerificationRefOutsideGrounds {
        field: &'static str,
        evidence: String,
    },
    #[error(
        "{field} window {start} .. {end} is invalid: window_end must not precede window_start \
         and the window must span at most 366 days"
    )]
    InvalidEvidenceWindow {
        field: &'static str,
        start: String,
        end: String,
    },
    #[error("decision evidence is insufficient for the submitted forecasts")]
    InsufficientDecisionEvidence,
    #[error("a document attempt origin requires a task origin")]
    AttemptOriginWithoutTask,
    #[error("raw evidence may only be read through a Rust-controlled tool")]
    RawEvidenceDirectContext,
    #[error("Paper reprice must be the single deterministic r0 to r1 lineage")]
    InvalidRepriceLineage,
    #[error("execution plan hash does not match its payload")]
    ExecutionPlanHashMismatch,
    #[error("regime probability distribution is invalid")]
    InvalidDistribution,
    #[error("decision-time regime snapshot as_of ({as_of:?}) cannot exceed decision_at ({decision_at:?})")]
    DecisionTimeLookaheadViolation {
        decision_at: chrono::DateTime<chrono::Utc>,
        as_of: chrono::DateTime<chrono::Utc>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Asset {
    Tqqq,
    Qqq,
    Soxx,
    Soxl,
}

impl Asset {
    pub const EXECUTABLE: [Self; 4] = [Self::Tqqq, Self::Qqq, Self::Soxx, Self::Soxl];

    // 将内部资产枚举映射为系统允许的四个 ETF 代码。
    pub const fn symbol(self) -> &'static str {
        match self {
            Self::Tqqq => "TQQQ",
            Self::Qqq => "QQQ",
            Self::Soxx => "SOXX",
            Self::Soxl => "SOXL",
        }
    }
}

impl fmt::Display for Asset {
    // Display 直接复用 symbol，保证日志/错误文本与 wire 资产代码一致。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.symbol())
    }
}

impl TryFrom<&str> for Asset {
    type Error = DomainError;

    // 先裁剪空白并转大写，再只接受四个可执行资产，其余输入返回 UnsupportedAsset。
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.trim().to_ascii_uppercase().as_str() {
            "TQQQ" => Ok(Self::Tqqq),
            "QQQ" => Ok(Self::Qqq),
            "SOXX" => Ok(Self::Soxx),
            "SOXL" => Ok(Self::Soxl),
            other => Err(DomainError::UnsupportedAsset(other.to_owned())),
        }
    }
}

impl<'de> Deserialize<'de> for Asset {
    // 通过 String 反序列化后复用 TryFrom，确保 JSON 输入和手工解析共享白名单。
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_from(value.as_str()).map_err(D::Error::custom)
    }
}

/// Exact portfolio weight, expressed in parts per million.
///
/// Integer weights keep model JSON, execution policy, hashing, and replay on
/// the same arithmetic surface.  Floats are intentionally not admitted to
/// canonical decision documents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WeightPpm(pub u32);

impl WeightPpm {
    pub const ZERO: Self = Self(0);
    pub const SCALE: u32 = 1_000_000;
}

/// Signed money in millionths of a USD.  Execution accepts integer money only
/// so an order plan has a stable content hash across platforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MoneyMicros(pub i64);

impl MoneyMicros {
    pub const ZERO: Self = Self(0);

    // 把美元分转换为百万分美元；saturating_mul 避免 i64 溢出回绕。
    pub const fn from_usd_cents(cents: i64) -> Self {
        Self(cents.saturating_mul(10_000))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContentHash(String);

impl ContentHash {
    // 只接受小写十六进制 SHA-256 文本；长度或字符不符时拒绝构造。
    pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(DomainError::InvalidContentHash);
        }
        Ok(Self(value))
    }

    // 对任意字节计算 SHA-256，并把摘要编码为小写十六进制字符串。
    pub fn of_bytes(bytes: &[u8]) -> Self {
        Self(format!("{:x}", Sha256::digest(bytes)))
    }

    // 暴露借用的哈希文本，避免为读取生成新的 String。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ContentHash {
    // 直接写出底层摘要文本。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

// 递归规范化 JSON 的嵌套数组/对象后序列化，保证同一值使用稳定字节表示。
pub fn canonical_json_bytes(value: &Value) -> Result<Vec<u8>, serde_json::Error> {
    // 该局部函数按引用匹配 Value；叶子节点克隆，容器节点递归收集新容器。
    fn canonicalize(value: &Value) -> Value {
        match value {
            Value::Array(items) => Value::Array(items.iter().map(canonicalize).collect()),
            Value::Object(items) => Value::Object(
                items
                    .iter()
                    .map(|(key, value)| (key.clone(), canonicalize(value)))
                    .collect(),
            ),
            value => value.clone(),
        }
    }

    serde_json::to_vec(&canonicalize(value))
}

// 复用规范化 JSON 字节计算内容哈希，序列化失败原样向调用方传播。
pub fn content_hash_json(value: &Value) -> Result<ContentHash, serde_json::Error> {
    canonical_json_bytes(value).map(|bytes| ContentHash::of_bytes(&bytes))
}

id_type!(RunId);
id_type!(TaskId);
id_type!(AttemptId);
id_type!(LeaseId);
id_type!(ContractId);
id_type!(TopologyId);
id_type!(DecisionId);
id_type!(MemoryId);

/// Canonical target portfolio.  Cash is intentionally implicit: every
/// executable asset must be present, and unallocated equity remains cash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetPortfolio {
    pub weights: BTreeMap<Asset, WeightPpm>,
}

impl TargetPortfolio {
    // 为全部四个可执行资产建立零权重表；现金保持隐含。
    pub fn zeroed() -> Self {
        Self {
            weights: Asset::EXECUTABLE
                .into_iter()
                .map(|asset| (asset, WeightPpm::ZERO))
                .collect(),
        }
    }

    // 检查权重表既没有缺少可执行资产，也没有额外资产。
    pub fn validate_universe(&self) -> Result<(), DomainError> {
        if self.weights.len() != Asset::EXECUTABLE.len()
            || !Asset::EXECUTABLE
                .into_iter()
                .all(|asset| self.weights.contains_key(&asset))
        {
            return Err(DomainError::InvalidTargetUniverse);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRef {
    pub hash: ContentHash,
    pub media_type: String,
    pub bytes: u64,
}

impl BlobRef {
    // 只校验媒体类型非空；哈希和字节数的语义由更高层 Artifact 校验组合。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.media_type.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "blob_ref.media_type",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunPurpose {
    Debug,
    PositionPlan,
    Paper,
    PaperDryRun,
    Replay,
    Shadow,
}

impl RunPurpose {
    // 只有正式 Paper Run 的目的允许进入 canonical learning 语义。
    pub const fn is_canonical_learning(self) -> bool {
        matches!(self, Self::Paper)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Leased,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Skipped,
}

impl TaskStatus {
    // 终态是不再等待后续执行的四种状态，Pending/Leased/Running 仍可推进。
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Skipped
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    Queued,
    Leased,
    Running,
    DecisionCompleted,
    Completed,
    CompletedWithExecutionRejection,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskBudget {
    pub max_input_tokens: u32,
    pub max_output_tokens: u32,
    pub max_wall_time_secs: u32,
    pub max_tool_calls: crate::budget::ToolCallLimit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolKind {
    ReadEvidence,
    ReadRawEvidence,
    FetchWebEvidence,
    ReadMarketData,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolGrant {
    pub kind: ToolKind,
    /// Empty means the tool is source-agnostic.  Otherwise every requested
    /// source must be explicitly present in this allowlist.
    pub allowed_sources: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u8,
    pub initial_backoff_ms: u64,
    pub retry_transport: bool,
    pub retry_rate_limited: bool,
    pub retry_invalid_output: bool,
}

impl RetryPolicy {
    // 构造一个不重试、单次尝试且无退避的策略。
    pub const fn none() -> Self {
        Self {
            max_attempts: 1,
            initial_backoff_ms: 0,
            retry_transport: false,
            retry_rate_limited: false,
            retry_invalid_output: false,
        }
    }

    // max_attempts 必须为正；其他开关可按角色由调用方组合。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.max_attempts == 0 {
            return Err(DomainError::InvalidBudget {
                field: "retry.max_attempts",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminationPolicy {
    pub max_child_tasks: u16,
    pub max_depth: u16,
    pub require_evidence: bool,
    pub stop_when_evidence_complete: bool,
}

impl TerminationPolicy {
    // 构造不允许创建子任务的叶节点策略，同时要求证据完成后停止。
    pub const fn leaf() -> Self {
        Self {
            max_child_tasks: 0,
            max_depth: 0,
            require_evidence: true,
            stop_when_evidence_complete: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureDisposition {
    FailRun,
    FailTask,
    SkipTask,
}

impl TaskBudget {
    // 输入、输出、墙钟三项都必须为正；工具调用上限由 ToolCallLimit 自身表达。
    pub fn validate(&self) -> Result<(), DomainError> {
        for (field, value) in [
            ("max_input_tokens", self.max_input_tokens),
            ("max_output_tokens", self.max_output_tokens),
            ("max_wall_time_secs", self.max_wall_time_secs),
        ] {
            if value == 0 {
                return Err(DomainError::InvalidBudget { field });
            }
        }
        Ok(())
    }
}
