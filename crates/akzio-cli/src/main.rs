use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{self, Read},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use akzio_daemon::{
    contract_component_hash, fixture_model_client, prompt_component_hash,
    runtime_governance_identity, runtime_policy_identity, topology_component_hash,
    AlpacaMarketDataFeed, AlpacaPaperSessionClock, Daemon, DaemonConfig, DaemonHealth,
    PaperApprovalRequest, PaperApprovalResponse, PaperWorkflowSource, ReplayReport,
    RetrospectiveView, RunCancellationResponse, RunRetryResponse,
};
use akzio_domain::{
    content_hash_json, Asset, CanaryCampaignSpec, ContentHash, ExperimentCondition,
    LessonLifecycle, ModelQualificationReport, OutcomeCostModel, ReleaseEvidenceBundle, RunId,
    RunPurpose, RuntimeIdentity,
};
use akzio_execution::{paper::AlpacaPaper, DecisionPolicy};
use akzio_ingest::{EvidenceRequest, EvidenceSource};
use akzio_model::{
    probe_configured_model_capabilities, ModelCapabilityProbeSet, OpenAIResponsesConfig,
    OPENAI_RESPONSES_PROVIDER_ID,
};
use akzio_store::{CanaryCampaignHead, SessionSlot, Store, StoredRun, TrajectoryEntry};
use anyhow::{bail, Context, Result};
use chrono::{NaiveDate, Utc};
use clap::{Parser, Subcommand};
use futures::StreamExt;
use reqwest::{Client, Method, RequestBuilder, Response, Url};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::watch;

// CLI 是 loopback 控制入口：只解析命令/配置并调用 Rust 权威 API，业务 Gate、Store 写入和 Paper 权限不在参数层绕过。
#[derive(Debug, Parser)]
#[command(name = "akzio", about = "Akzio loopback control client")]
struct Cli {
    // config 只指定配置文件路径；具体 endpoint、Store root、模型和凭据由后续配置校验读取。
    #[arg(long, default_value = "config/akzio.toml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    // 子命令按领域分组，实际 handler 由下方 include 的 cli 模块提供，保持 main.rs 的入口注册稳定。
    Workflow {
        #[command(subcommand)]
        command: WorkflowCommand,
    },
    Debug {
        #[command(subcommand)]
        command: DebugCommand,
    },
    ObservatoryConfig {
        #[arg(long)]
        config: PathBuf,
        #[command(subcommand)]
        command: ObservatoryConfigCommand,
    },
    Daemon {
        #[command(subcommand)]
        command: DaemonAction,
    },
    Run {
        #[command(subcommand)]
        command: RunCommand,
    },
    Store {
        #[command(subcommand)]
        command: StoreCommand,
    },
    Canary {
        #[command(subcommand)]
        command: CanaryCommand,
    },
    ModelQualification {
        #[command(subcommand)]
        command: ModelQualificationCommand,
    },
    Calibration {
        #[command(subcommand)]
        command: CalibrationCommand,
    },
    Evidence {
        #[command(subcommand)]
        command: EvidenceCommand,
    },
}

#[derive(Debug, Subcommand)]
enum CalibrationCommand {
    // Calibration 子命令只通过显式 Store/Artifact 参数访问 SQL 权威；readiness、build、activate 的资格由 Store/Rust 校验。
    /// Read-only report of canonical Paper outcome maturity and calibration gaps.
    Readiness {
        #[arg(long)]
        store: PathBuf,
        #[arg(long, default_value_t = 30)]
        min_samples: u32,
    },
    Preflight {
        #[arg(long)]
        scratch: PathBuf,
    },
    /// Store explicit operator risk limits as an immutable SQL Artifact.
    SetRiskLimits {
        #[arg(long)]
        store: PathBuf,
        #[command(flatten)]
        limits: CalibrationRiskSettings,
    },
    /// Collect a calibration dataset from canonical Outcomes into SQL.
    Collect {
        #[arg(long)]
        store: PathBuf,
        #[arg(long)]
        risk_limits: String,
        #[arg(long, default_value_t = 30)]
        min_samples: u32,
        #[arg(long)]
        training_start: Option<String>,
        #[arg(long)]
        training_end: Option<String>,
    },
    /// Fit a stored dataset and persist a candidate policy without activating it.
    Build {
        #[arg(long)]
        store: PathBuf,
        #[arg(long)]
        dataset: String,
    },
    /// Explicitly activate one stored, validated policy Artifact.
    Activate {
        #[arg(long)]
        store: PathBuf,
        #[arg(long)]
        policy: String,
    },
    /// Copy the active immutable policy into a new or isolated Store.
    Bootstrap {
        #[arg(long)]
        source_store: PathBuf,
        #[arg(long)]
        target_store: PathBuf,
    },
    Inspect {
        #[arg(long)]
        store: PathBuf,
        #[arg(long)]
        artifact: String,
    },
    Validate {
        #[arg(long)]
        store: PathBuf,
        #[arg(long)]
        policy: String,
    },
}

#[derive(Debug, clap::Args)]
struct CalibrationRiskSettings {
    #[arg(long)]
    min_confidence_ppm: u32,
    #[arg(long)]
    max_gross_weight_ppm: u32,
    #[arg(long)]
    maximum_execution_delay_ms: u64,
    #[arg(long)]
    minimum_process_quality_ppm: u32,
    #[arg(long)]
    min_probability_edge_ppm: u32,
    #[arg(long)]
    max_brier_score_ppm: u32,
    #[arg(long)]
    target_annualized_volatility_ppm: u32,
    #[arg(long)]
    max_portfolio_beta_ppm: u32,
    #[arg(long)]
    max_expected_shortfall_ppm: u32,
    #[arg(long)]
    max_gap_loss_ppm: u32,
    #[arg(long)]
    max_capital_weight_ppm: u32,
    #[arg(long)]
    liquidity_weight_cap_ppm: u32,
    #[arg(long)]
    max_leveraged_holding_days: u8,
    #[arg(long)]
    daily_reset_decay_ppm: u32,
}

impl From<&CalibrationRiskSettings> for OfflineRiskLimits {
    fn from(value: &CalibrationRiskSettings) -> Self {
        Self {
            min_confidence_ppm: value.min_confidence_ppm,
            max_gross_weight_ppm: value.max_gross_weight_ppm,
            maximum_execution_delay_ms: value.maximum_execution_delay_ms,
            minimum_process_quality_ppm: value.minimum_process_quality_ppm,
            min_probability_edge_ppm: value.min_probability_edge_ppm,
            max_brier_score_ppm: value.max_brier_score_ppm,
            target_annualized_volatility_ppm: value.target_annualized_volatility_ppm,
            max_portfolio_beta_ppm: value.max_portfolio_beta_ppm,
            max_expected_shortfall_ppm: value.max_expected_shortfall_ppm,
            max_gap_loss_ppm: value.max_gap_loss_ppm,
            max_capital_weight_ppm: value.max_capital_weight_ppm,
            liquidity_weight_cap_ppm: value.liquidity_weight_cap_ppm,
            max_leveraged_holding_days: value.max_leveraged_holding_days,
            daily_reset_decay_ppm: value.daily_reset_decay_ppm,
        }
    }
}

#[derive(Debug, Subcommand)]
enum EvidenceCommand {
    // Evidence 命令是受控采集/预检入口，输出证据观察而不把网络成功直接提升为 Decision 或 Paper 结果。
    MarketAudit {
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value = "indicative")]
        option_feed: akzio_ingest::AlpacaOptionDataFeed,
    },
    Preflight {
        #[arg(long)]
        resource: String,
    },
}

#[derive(Debug, Subcommand)]
enum ModelQualificationCommand {
    // 模型 qualification 通过输入/输出文件交换报告，模型身份和版本验证仍由领域报告规则负责。
    Assemble {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum CanaryCommand {
    // Canary 命令只操作持久化 campaign head；stage/status/resume 不等于已激活生产策略。
    Stage {
        #[arg(long)]
        spec: PathBuf,
    },
    Status,
    Resume {
        campaign_id: String,
    },
}

#[derive(Debug, Subcommand)]
enum ObservatoryConfigCommand {
    // ObservatoryConfig 负责本地配置文件工作流，敏感字段的读取/写入由专用 handler 处理。
    Init {
        #[arg(long)]
        template: PathBuf,
        #[arg(long)]
        store_root: PathBuf,
    },
    Get,
    Set,
}

#[derive(Debug, Subcommand)]
enum DaemonAction {
    // Daemon action 通过 loopback 控制 daemon 生命周期；Freeze/Unfreeze 仍受服务端认证与 Store 状态约束。
    Serve,
    Health,
    Ready,
    Freeze { reason: String },
    Unfreeze { reason: String },
}

#[derive(Debug, Subcommand)]
enum RunCommand {
    // Run 命令读取检查、事件、轨迹和受控取消/重试结果，不在 CLI 侧重写 Run 状态。
    Inspect {
        run_id: String,
    },
    Checkpoint {
        run_id: String,
    },
    Journal {
        run_id: String,
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long, default_value_t = 100)]
        limit: usize,
        #[arg(long)]
        task_id: Option<String>,
        #[arg(long)]
        attempt_id: Option<String>,
    },
    Replay {
        run_id: String,
    },
    Retrospectives {
        run_id: String,
    },
    Trajectory {
        run_id: String,
    },
    Events {
        run_id: String,
        #[arg(long, default_value_t = 0)]
        after: i64,
    },
    Cancel {
        run_id: String,
    },
    Retry {
        run_id: String,
    },
    RepairNarrative {
        run_id: String,
    },
}

#[derive(Debug, Subcommand)]
enum WorkflowCommand {
    // Workflow blueprint 是只读拓扑投影，可选择 JSON 或 Mermaid 展示格式。
    Blueprint {
        #[arg(long, default_value = "position_plan", value_parser = ["position_plan", "paper", "shadow"])]
        purpose: String,
        #[arg(long, default_value = "json", value_parser = ["json", "mermaid"])]
        format: String,
    },
}

#[derive(Debug, Subcommand)]
enum StoreCommand {
    // Store 命令直接使用 Store 的事务/Doctor/导出边界；Backup/Restore 等写操作仍由 handler 明确执行。
    Doctor,
    Inventory,
    Metrics,
    Alerts,
    PaperSession {
        session_key: String,
    },
    ApprovePaper {
        session_key: String,
        #[arg(long)]
        operator: String,
        #[arg(long)]
        reason: String,
        #[arg(long)]
        max_notional_usd_cents: i64,
        #[arg(long, default_value_t = 8)]
        valid_hours: i64,
        #[arg(long)]
        qualification_report: PathBuf,
    },
    Backup {
        target: PathBuf,
    },
    Restore {
        source: PathBuf,
        target: PathBuf,
    },
    ExportRun {
        run_id: String,
        target: PathBuf,
        #[arg(long)]
        include_raw_model: bool,
    },
    ReleaseEvidence {
        run_id: String,
        #[arg(long)]
        target: Option<PathBuf>,
    },
    Lesson {
        #[command(subcommand)]
        command: lesson::LessonCommand,
    },
}

#[derive(Debug, Serialize)]
struct PaperSessionView {
    // Session view 只序列化 Store 返回的预约、scheduler epoch 和 commitment 状态，不推断订单成交。
    session_key: String,
    workflow: StoredRun,
    scheduler_epoch: u64,
    reserved_at: chrono::DateTime<Utc>,
    commitment_artifact_id: Option<akzio_domain::ArtifactId>,
    committed_at: Option<chrono::DateTime<Utc>>,
}

impl From<SessionSlot> for PaperSessionView {
    fn from(slot: SessionSlot) -> Self {
        Self {
            session_key: slot.session_key,
            workflow: slot.workflow.run,
            scheduler_epoch: slot.scheduler_epoch,
            reserved_at: slot.reserved_at,
            commitment_artifact_id: slot.commitment_artifact_id,
            committed_at: slot.committed_at,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    // Config 使用 deny_unknown_fields 保持 CLI 配置与 Rust schema 同步；credentials 单独做默认与脱敏处理。
    #[serde(default)]
    agent: akzio_domain::AgentSettings,
    daemon: DaemonSettings,
    execution: ExecutionSettings,
    model: Option<OpenAIResponsesConfig>,
    #[serde(default)]
    credentials: CredentialsSettings,
    #[serde(default)]
    observatory: ObservatorySettings,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DaemonSettings {
    // daemon settings 决定服务/Debug/Outcome 开关和 Store 位置，不直接授予 Paper 写权限。
    #[serde(default)]
    debug_control: bool,
    #[serde(default = "default_outcome_processing")]
    outcome_processing: bool,
    store_root: PathBuf,
    http_addr: SocketAddr,
    worker_count: Option<usize>,
    auto_paper: Option<bool>,
    #[serde(default)]
    manual_paper: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionSettings {
    // execution settings 是资产、行情 feed 和成本参数的声明；具体风险/审批仍由 execution Gate 校验。
    #[serde(default)]
    experiment_profile: ExperimentProfile,
    #[serde(default)]
    historical_evaluation_condition: Option<ExperimentCondition>,
    assets: Vec<Asset>,
    market_data_feed: Option<AlpacaMarketDataFeed>,
    #[serde(default)]
    transaction_cost_ppm: u32,
    #[serde(default)]
    slippage_ppm: u32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum ExperimentProfile {
    #[default]
    Fixture,
    PaperEngineering,
    PaperResearch,
    HistoricalEval,
}

impl ExperimentProfile {
    fn as_str(self) -> &'static str {
        // profile 只转换为稳定配置标签，不能据此切换 Live Trading 或修改历史 Run。
        match self {
            Self::Fixture => "fixture",
            Self::PaperEngineering => "paper-engineering",
            Self::PaperResearch => "paper-research",
            Self::HistoricalEval => "historical-eval",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObservatorySettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sec_user_agent: Option<String>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialsSettings {
    // 凭据在配置对象中只用于传递给受控客户端；Debug 输出通过自定义 fmt 永不回显 secret。
    #[serde(default)]
    alpaca_api_key: String,
    #[serde(default)]
    alpaca_api_secret: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fred_api_key: Option<String>,
}

impl std::fmt::Debug for CredentialsSettings {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 仅输出 redacted marker，避免日志/错误路径泄露 Alpaca、FRED 或其他 API secret。
        formatter
            .debug_struct("CredentialsSettings")
            .field("alpaca_api_key", &"<redacted>")
            .field("alpaca_api_secret", &"<redacted>")
            .field(
                "fred_api_key",
                &self.fred_api_key.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ObservatoryEditableConfiguration {
    // 这是 App/CLI 间的 camelCase 配置 wire model；它不负责验证模型路由、Paper prerequisites 或权限。
    provider: String,
    #[serde(rename = "llmBaseURL")]
    llm_base_url: String,
    #[serde(rename = "llmAPIKey")]
    llm_api_key: String,
    global_model: String,
    global_reasoning_effort: String,
    global_response_language: String,
    stage_models: BTreeMap<String, akzio_model::OpenAIResponsesRouteConfig>,
    #[serde(rename = "alpacaAPIKey")]
    alpaca_api_key: String,
    #[serde(rename = "alpacaAPISecret")]
    alpaca_api_secret: String,
    #[serde(rename = "fredAPIKey")]
    fred_api_key: Option<String>,
    sec_user_agent: Option<String>,
}

#[derive(Debug, Serialize)]
struct FreezeRequest<'a> {
    // Freeze request 借用调用方 reason，序列化后只作为 loopback 控制请求发送。
    reason: &'a str,
}

// 下面的 include 把大型命令 handler 保持在按领域拆分的模块中；这里仅注册模块，不改变其函数签名或业务流程。
mod http_client;
mod lesson;
use http_client::ControlApiClient;

include!("cli/main.rs");
include!("cli/dispatch.rs");
include!("cli/observatory_config.rs");
include!("cli/identity.rs");
include!("cli/run_commands.rs");
include!("cli/debug_commands.rs");
include!("cli/model_qualification.rs");
include!("cli/calibration.rs");
