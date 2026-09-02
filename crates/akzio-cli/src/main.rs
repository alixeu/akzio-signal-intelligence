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
    RetrospectiveView, RunCancellationResponse, RunRetryResponse, RunSubmissionResponse,
    StoreEventView,
};
use akzio_domain::{
    content_hash_json, Asset, CanaryCampaignSpec, ContentHash, ExperimentCondition,
    LessonLifecycle, ModelQualificationReport, OutcomeCostModel, ReleaseEvidenceBundle, RunId,
    RunPurpose, RuntimeIdentity, WorkflowStatus,
};
use akzio_execution::{paper::AlpacaPaper, DecisionPolicy};
use akzio_model::{
    probe_configured_model_capabilities, ModelCapabilityProbeSet, OpenAIResponsesConfig,
    OPENAI_RESPONSES_PROVIDER_ID,
};
use akzio_store::{CanaryCampaignHead, SessionSlot, StoredRun, TrajectoryEntry};
use anyhow::{bail, Context, Result};
use chrono::{NaiveDate, Utc};
use clap::{Parser, Subcommand, ValueEnum};
use futures::StreamExt;
use reqwest::{Client, Method, RequestBuilder, Response, Url};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::watch;

#[derive(Debug, Parser)]
#[command(name = "akzio", about = "Akzio loopback control client")]
struct Cli {
    #[arg(long, default_value = "config/akzio.toml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
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
}

#[derive(Debug, Subcommand)]
enum ModelQualificationCommand {
    Assemble {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum CanaryCommand {
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
    Serve,
    Health,
    Ready,
    Freeze { reason: String },
    Unfreeze { reason: String },
}

#[derive(Debug, Subcommand)]
enum RunCommand {
    Submit {
        #[arg(value_enum)]
        purpose: PurposeArg,
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
    FixtureDebug,
    PaperDryRun,
}

#[derive(Debug, Subcommand)]
enum StoreCommand {
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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum PurposeArg {
    Debug,
    PaperDryRun,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
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
    store_root: PathBuf,
    http_addr: SocketAddr,
    worker_count: Option<usize>,
    auto_paper: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionSettings {
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
    #[serde(default)]
    decision_policy_path: Option<PathBuf>,
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
    #[serde(default)]
    alpaca_api_key: String,
    #[serde(default)]
    alpaca_api_secret: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fred_api_key: Option<String>,
}

impl std::fmt::Debug for CredentialsSettings {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
struct SubmitRequest {
    purpose: RunPurpose,
}

#[derive(Debug, Serialize)]
struct FreezeRequest<'a> {
    reason: &'a str,
}

mod http_client;
mod lesson;
use http_client::ControlApiClient;

include!("cli/main.rs");
include!("cli/dispatch.rs");
include!("cli/observatory_config.rs");
include!("cli/identity.rs");
include!("cli/run_commands.rs");
include!("cli/model_qualification.rs");
