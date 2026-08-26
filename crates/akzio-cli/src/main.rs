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
    fixture_model_client, AlpacaMarketDataFeed, AlpacaPaperSessionClock, Daemon, DaemonConfig,
    DaemonHealth, PaperApprovalRequest, PaperApprovalResponse, PaperWorkflowSource, ReplayReport,
    RetrospectiveView, RunCancellationResponse, RunRetryResponse, RunSubmissionResponse,
};
use akzio_domain::{
    content_hash_json, ArtifactKind, Asset, CanaryCampaignSpec, ContentHash, Retrospective, RunId,
    RunPurpose, RuntimeIdentity, WorkflowStatus,
};
use akzio_execution::{paper::AlpacaPaper, DecisionPolicy, ExecutionPolicy};
use akzio_learning::{
    evaluate_frozen_evidence, EvaluationPolicy, FrozenEvidenceRecord, FrozenEvidenceSet,
    OutcomeCostModel,
};
use akzio_model::ModelConfig;
use akzio_store::{
    v2::{CanaryCampaignHead, SessionSlot, StoredRun, TrajectoryEntry},
    V2Store,
};
use anyhow::{bail, Context, Result};
use chrono::{Duration as ChronoDuration, Utc};
use clap::{Parser, Subcommand, ValueEnum};
use futures::StreamExt;
use reqwest::{Client, Method, RequestBuilder, Response, Url};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::watch;

#[derive(Debug, Parser)]
#[command(name = "akzio", about = "Akzio v2 loopback control client")]
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
    Test {
        #[command(subcommand)]
        command: TestCommand,
    },
    Store {
        #[command(subcommand)]
        command: StoreCommand,
    },
    Canary {
        #[command(subcommand)]
        command: CanaryCommand,
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
    FixtureDebug,
    PaperDryRun,
}

#[derive(Debug, Subcommand)]
enum TestCommand {
    CrashRecovery,
    ConcurrentRuns,
    EvidenceIntegrity,
    LearningTransitions,
    FrozenEvidence,
    StoreCorruption,
    FreezeRecovery,
    LeaseTakeover,
    Retrospective,
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
    model: Option<ModelConfig>,
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
    token_env: String,
    observer_token_env: Option<String>,
    worker_count: Option<usize>,
    auto_paper: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionSettings {
    assets: Vec<Asset>,
    market_data_feed: Option<AlpacaMarketDataFeed>,
    #[serde(default)]
    transaction_cost_ppm: u32,
    #[serde(default)]
    slippage_ppm: u32,
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
    #[serde(rename = "llmBaseURL")]
    llm_base_url: String,
    #[serde(rename = "llmAPIKey")]
    llm_api_key: String,
    global_model: String,
    global_reasoning_effort: String,
    global_response_language: String,
    stage_models: BTreeMap<String, akzio_model::ModelRouteConfig>,
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
#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if let Command::ObservatoryConfig { config, command } = &cli.command {
        return handle_observatory_config(config, command);
    }
    let config_path = cli.config.clone();
    let config = load_config(&config_path)?;

    match cli.command {
        Command::ObservatoryConfig { .. } => unreachable!("handled before config loading"),
        Command::Daemon { command } => {
            dispatch_control(Command::Daemon { command }, &config, &config_path).await
        }
        Command::Run { command }
            if matches!(command, RunCommand::FixtureDebug | RunCommand::PaperDryRun) =>
        {
            dispatch_fixture(command, config).await
        }
        Command::Run { command } => {
            dispatch_control(Command::Run { command }, &config, &config_path).await
        }
        Command::Test { command } => dispatch_diagnostics(command, config).await,
        Command::Store { command } => dispatch_store(command, &config, &config_path).await,
        Command::Canary { command } => {
            dispatch_control(Command::Canary { command }, &config, &config_path).await
        }
    }
}

async fn dispatch_control(command: Command, config: &Config, config_path: &Path) -> Result<()> {
    match command {
        Command::Daemon { command } => match command {
            DaemonAction::Serve => serve(config, config_path).await,
            DaemonAction::Health => {
                print_json(&ControlApiClient::from_config(config)?.health().await?)
            }
            DaemonAction::Ready => {
                print_json(&ControlApiClient::from_config(config)?.ready().await?)
            }
            DaemonAction::Freeze { reason } => print_json(
                &ControlApiClient::from_config(config)?
                    .set_freeze(true, &reason)
                    .await?,
            ),
            DaemonAction::Unfreeze { reason } => print_json(
                &ControlApiClient::from_config(config)?
                    .set_freeze(false, &reason)
                    .await?,
            ),
        },
        Command::Run { command } => {
            let client = ControlApiClient::from_config(config)?;
            match command {
                RunCommand::Submit { purpose } => print_json(&client.submit(purpose.into()).await?),
                RunCommand::Replay { run_id } => print_json(&client.replay(&run_id).await?),
                RunCommand::Retrospectives { run_id } => {
                    print_json(&client.retrospectives(&run_id).await?)
                }
                RunCommand::Trajectory { run_id } => print_json(&client.trajectory(&run_id).await?),
                RunCommand::Events { run_id, after } => client.events(&run_id, after).await,
                RunCommand::Cancel { run_id } => print_json(&client.cancel(&run_id).await?),
                RunCommand::Retry { run_id } => print_json(&client.retry(&run_id).await?),
                RunCommand::FixtureDebug | RunCommand::PaperDryRun => {
                    bail!("fixture commands must be dispatched through the fixture handler")
                }
            }
        }
        Command::Canary { command } => {
            let client = ControlApiClient::from_config(config)?;
            match command {
                CanaryCommand::Stage { spec } => {
                    let payload = fs::read_to_string(spec).context("read canary campaign spec")?;
                    let spec: CanaryCampaignSpec = serde_json::from_str(&payload)
                        .context("parse canary campaign spec JSON")?;
                    print_json(&client.canary_stage(&spec).await?)
                }
                CanaryCommand::Status => print_json(&client.canary_status().await?),
                CanaryCommand::Resume { campaign_id } => {
                    let campaign_id =
                        ContentHash::new(campaign_id).context("parse campaign hash")?;
                    print_json(&client.canary_resume(&campaign_id).await?)
                }
            }
        }
        Command::ObservatoryConfig { .. } => {
            bail!("ObservatoryConfig is handled before control dispatch")
        }
        Command::Test { .. } => {
            bail!("diagnostic commands must be dispatched through the diagnostics handler")
        }
        Command::Store { .. } => {
            bail!("store commands must be dispatched through the store handler")
        }
    }
}

async fn dispatch_diagnostics(command: TestCommand, config: Config) -> Result<()> {
    diagnostic_test(config, command).await
}

async fn dispatch_fixture(command: RunCommand, config: Config) -> Result<()> {
    match command {
        RunCommand::FixtureDebug => fixture_debug(config).await,
        RunCommand::PaperDryRun => paper_dry_run(config).await,
        _ => bail!("non-fixture run command must be dispatched through the control handler"),
    }
}

async fn dispatch_store(command: StoreCommand, config: &Config, config_path: &Path) -> Result<()> {
    match command {
        StoreCommand::Doctor => admin_verify_store(config),
        StoreCommand::Inventory => admin_print_inventory(config),
        StoreCommand::Metrics => admin_print_metrics(config),
        StoreCommand::Alerts => admin_print_alerts(config),
        StoreCommand::PaperSession { session_key } => admin_print_session(config, &session_key),
        StoreCommand::ApprovePaper {
            session_key,
            operator,
            reason,
            max_notional_usd_cents,
            valid_hours,
        } => {
            approve_paper(
                config,
                config_path,
                &session_key,
                &operator,
                &reason,
                max_notional_usd_cents,
                valid_hours,
            )
            .await
        }
        StoreCommand::Backup { target } => admin_backup_store(config, target),
        StoreCommand::Restore { source, target } => admin_restore_store(source, target),
        StoreCommand::ExportRun {
            run_id,
            target,
            include_raw_model,
        } => admin_export_run(config, run_id, target, include_raw_model),
        StoreCommand::Lesson { command } => lesson::run(&config.daemon.store_root, command),
    }
}

fn open_admin_store(config: &Config) -> Result<V2Store> {
    Ok(V2Store::open_existing(&config.daemon.store_root)?)
}

fn admin_verify_store(config: &Config) -> Result<()> {
    open_admin_store(config)?.verify_integrity()?;
    println!("{{\"ok\":true}}");
    Ok(())
}

fn admin_print_inventory(config: &Config) -> Result<()> {
    print_json(&open_admin_store(config)?.storage_inventory()?)
}

fn admin_print_metrics(config: &Config) -> Result<()> {
    print_json(&open_admin_store(config)?.metrics(Utc::now())?)
}

fn admin_print_alerts(config: &Config) -> Result<()> {
    let metrics = open_admin_store(config)?.metrics(Utc::now())?;
    print_json(&metrics.alerts())
}

fn admin_print_session(config: &Config, session_key: &str) -> Result<()> {
    let slot = open_admin_store(config)?
        .session_slot(session_key)?
        .map(PaperSessionView::from);
    print_json(&slot)
}

fn admin_backup_store(config: &Config, target: PathBuf) -> Result<()> {
    print_json(&open_admin_store(config)?.backup_to(target)?)
}

fn admin_restore_store(source: PathBuf, target: PathBuf) -> Result<()> {
    let store = V2Store::restore_from(source, target)?;
    print_json(&store.metrics(Utc::now())?)
}

fn admin_export_run(
    config: &Config,
    run_id: String,
    target: PathBuf,
    include_raw_model: bool,
) -> Result<()> {
    print_json(&open_admin_store(config)?.export_run(&RunId(run_id), target, include_raw_model)?)
}

fn handle_observatory_config(config_path: &Path, command: &ObservatoryConfigCommand) -> Result<()> {
    match command {
        ObservatoryConfigCommand::Init {
            template,
            store_root,
        } => {
            if config_path.exists() {
                return print_json(&serde_json::json!({ "created": false }));
            }
            let template_config = read_config_file(template)?;
            let mut document = read_config_document(template)?;
            toml_section_mut(&mut document, "daemon")?.insert(
                "store_root".to_owned(),
                toml::Value::String(store_root.to_string_lossy().into_owned()),
            );
            if let Some(model) = template_config.model.as_ref() {
                let model_table = toml_section_mut(&mut document, "model")?;
                model_table.insert(
                    "base_url".to_owned(),
                    toml::Value::String(initial_config_value(&model.base_url)),
                );
                model_table.insert(
                    "api_key".to_owned(),
                    toml::Value::String(initial_config_value(&model.api_key)),
                );
            }
            let credentials = toml_section_mut(&mut document, "credentials")?;
            credentials.insert(
                "alpaca_api_key".to_owned(),
                toml::Value::String(std::env::var("ALPACA_API_KEY").unwrap_or_default()),
            );
            credentials.insert(
                "alpaca_api_secret".to_owned(),
                toml::Value::String(std::env::var("ALPACA_API_SECRET").unwrap_or_default()),
            );
            set_optional_toml_string(
                credentials,
                "fred_api_key",
                std::env::var("FRED_API_KEY").ok(),
            );
            set_optional_toml_string(
                toml_section_mut(&mut document, "observatory")?,
                "sec_user_agent",
                std::env::var("SEC_USER_AGENT").ok(),
            );
            write_config_file(config_path, &document)?;
            print_json(&serde_json::json!({ "created": true }))
        }
        ObservatoryConfigCommand::Get => {
            let config = read_config_file(config_path)?;
            print_json(&editable_observatory_configuration(&config)?)
        }
        ObservatoryConfigCommand::Set => {
            let mut payload = String::new();
            io::stdin()
                .read_to_string(&mut payload)
                .context("read Observatory configuration from stdin")?;
            let configuration: ObservatoryEditableConfiguration =
                serde_json::from_str(&payload).context("parse Observatory configuration JSON")?;
            update_observatory_configuration(config_path, configuration)?;
            print_json(&serde_json::json!({ "ok": true }))
        }
    }
}

fn editable_observatory_configuration(config: &Config) -> Result<ObservatoryEditableConfiguration> {
    let model = config
        .model
        .as_ref()
        .context("Observatory configuration requires [model]")?;
    Ok(ObservatoryEditableConfiguration {
        llm_base_url: model.base_url.clone(),
        llm_api_key: model.api_key.clone(),
        global_model: model.model.clone(),
        global_reasoning_effort: model.reasoning_effort.clone(),
        global_response_language: model.response_language.clone(),
        stage_models: model.routes.clone(),
        alpaca_api_key: config.credentials.alpaca_api_key.clone(),
        alpaca_api_secret: config.credentials.alpaca_api_secret.clone(),
        fred_api_key: config.credentials.fred_api_key.clone(),
        sec_user_agent: config.observatory.sec_user_agent.clone(),
    })
}

fn update_observatory_configuration(
    config_path: &Path,
    configuration: ObservatoryEditableConfiguration,
) -> Result<()> {
    let config = read_config_file(config_path)?;
    let current_model = config
        .model
        .as_ref()
        .context("Observatory configuration requires [model]")?;
    let model = ModelConfig {
        base_url: configuration.llm_base_url.trim().to_owned(),
        model: configuration.global_model.trim().to_owned(),
        api_key: configuration.llm_api_key,
        reasoning_effort: configuration.global_reasoning_effort.trim().to_owned(),
        response_language: configuration.global_response_language.trim().to_owned(),
        debug: current_model.debug,
        routes: configuration.stage_models,
    };
    validate_model_settings(&model)?;

    let mut document = read_config_document(config_path)?;
    let model_table = toml_section_mut(&mut document, "model")?;
    model_table.insert("base_url".to_owned(), toml::Value::String(model.base_url));
    model_table.insert("model".to_owned(), toml::Value::String(model.model));
    model_table.insert("api_key".to_owned(), toml::Value::String(model.api_key));
    model_table.insert(
        "reasoning_effort".to_owned(),
        toml::Value::String(model.reasoning_effort),
    );
    model_table.insert(
        "response_language".to_owned(),
        toml::Value::String(model.response_language),
    );
    model_table.insert(
        "routes".to_owned(),
        toml::Value::try_from(model.routes).context("serialize model routes")?,
    );

    let credentials = toml_section_mut(&mut document, "credentials")?;
    credentials.insert(
        "alpaca_api_key".to_owned(),
        toml::Value::String(configuration.alpaca_api_key),
    );
    credentials.insert(
        "alpaca_api_secret".to_owned(),
        toml::Value::String(configuration.alpaca_api_secret),
    );
    set_optional_toml_string(credentials, "fred_api_key", configuration.fred_api_key);
    set_optional_toml_string(
        toml_section_mut(&mut document, "observatory")?,
        "sec_user_agent",
        configuration.sec_user_agent,
    );
    write_config_file(config_path, &document)
}

#[allow(clippy::too_many_arguments)]
async fn approve_paper(
    config: &Config,
    config_path: &Path,
    session_key: &str,
    operator: &str,
    reason: &str,
    max_notional_usd_cents: i64,
    valid_hours: i64,
) -> Result<()> {
    let _session = chrono::NaiveDate::parse_from_str(session_key, "%Y-%m-%d")
        .context("session_key must be YYYY-MM-DD")?;
    if operator.trim().is_empty()
        || reason.trim().is_empty()
        || max_notional_usd_cents <= 0
        || valid_hours <= 0
        || valid_hours > 24 * 7
    {
        bail!("invalid Paper approval scope");
    }
    let identity = runtime_identity_from_config(config, config_path)?;
    print_json(
        &ControlApiClient::from_config(config)?
            .approve_paper(&PaperApprovalRequest {
                session_key: session_key.to_owned(),
                operator: operator.to_owned(),
                reason: reason.to_owned(),
                max_notional_usd_cents,
                valid_hours,
                identity,
            })
            .await?,
    )
}

type EmbeddedComponent = (&'static str, &'static [u8]);

fn component_hash(components: &[EmbeddedComponent]) -> Result<ContentHash> {
    let mut bytes = Vec::new();
    for (path, component) in components {
        bytes.extend_from_slice(path.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(component);
        bytes.push(0);
    }
    Ok(ContentHash::of_bytes(&bytes))
}

const PROMPT_COMPONENTS: &[EmbeddedComponent] = &[
    (
        "crates/akzio-research/src/agent_v2.rs",
        include_bytes!("../../akzio-research/src/agent_v2.rs"),
    ),
    (
        "crates/akzio-research/src/agent_v2/catalogue.rs",
        include_bytes!("../../akzio-research/src/agent_v2/catalogue.rs"),
    ),
    (
        "crates/akzio-research/src/agent_v2/schemas.rs",
        include_bytes!("../../akzio-research/src/agent_v2/schemas.rs"),
    ),
    (
        "crates/akzio-research/src/lib.rs",
        include_bytes!("../../akzio-research/src/lib.rs"),
    ),
];

const CONTRACT_COMPONENTS: &[EmbeddedComponent] = &[
    (
        "crates/akzio-domain/src/contract.rs",
        include_bytes!("../../akzio-domain/src/contract.rs"),
    ),
    PROMPT_COMPONENTS[0],
    PROMPT_COMPONENTS[1],
    PROMPT_COMPONENTS[2],
    PROMPT_COMPONENTS[3],
];

const TOPOLOGY_COMPONENTS: &[EmbeddedComponent] = &[
    (
        "crates/akzio-runtime/src/runtime_v2.rs",
        include_bytes!("../../akzio-runtime/src/runtime_v2.rs"),
    ),
    (
        "crates/akzio-runtime/src/runtime_v2/catalogue.rs",
        include_bytes!("../../akzio-runtime/src/runtime_v2/catalogue.rs"),
    ),
    (
        "crates/akzio-runtime/src/runtime_v2/planner.rs",
        include_bytes!("../../akzio-runtime/src/runtime_v2/planner.rs"),
    ),
    (
        "crates/akzio-runtime/src/runtime_v2/reducer.rs",
        include_bytes!("../../akzio-runtime/src/runtime_v2/reducer.rs"),
    ),
    (
        "crates/akzio-runtime/src/runtime_v2/replay.rs",
        include_bytes!("../../akzio-runtime/src/runtime_v2/replay.rs"),
    ),
    (
        "crates/akzio-runtime/src/runtime_v2/task.rs",
        include_bytes!("../../akzio-runtime/src/runtime_v2/task.rs"),
    ),
    (
        "crates/akzio-runtime/src/runtime_v2/workflow.rs",
        include_bytes!("../../akzio-runtime/src/runtime_v2/workflow.rs"),
    ),
];

fn runtime_identity_from_config(config: &Config, config_path: &Path) -> Result<RuntimeIdentity> {
    let model = config
        .model
        .as_ref()
        .context("missing [model] configuration")?;
    let feed = config
        .execution
        .market_data_feed
        .context("Paper runtime requires execution.market_data_feed")?;
    let provider_id = Url::parse(&model.base_url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
        .unwrap_or_else(|| model.base_url.clone());
    let execution_policy = ExecutionPolicy::default();
    let evaluation_policy = EvaluationPolicy::default();
    Ok(RuntimeIdentity {
        code_revision: source_revision()?,
        cargo_lock_hash: ContentHash::of_bytes(include_bytes!("../../../Cargo.lock")),
        config_hash: content_hash_json(&serde_json::json!({
            "config_file_hash": redacted_config_hash(config_path)?,
            "daemon": {
                "http_addr": config.daemon.http_addr.to_string(),
                "worker_count": config.daemon.worker_count,
                "auto_paper": config.daemon.auto_paper,
            },
            "execution": {
                "assets": config.execution.assets,
                "market_data_feed": config.execution.market_data_feed,
                "transaction_cost_ppm": config.execution.transaction_cost_ppm,
                "slippage_ppm": config.execution.slippage_ppm,
            },
            "model": {
                "base_url": model.base_url,
                "model": model.model,
                "reasoning_effort": model.reasoning_effort,
                "routes": model.routes,
            },
        }))?,
        provider_id,
        model_id: model.model.clone(),
        prompt_hash: component_hash(PROMPT_COMPONENTS)?,
        contract_hash: component_hash(CONTRACT_COMPONENTS)?,
        topology_hash: component_hash(TOPOLOGY_COMPONENTS)?,
        decision_policy_hash: DecisionPolicy::default().policy_hash()?,
        execution_policy_hash: execution_policy.policy_hash()?,
        evaluation_policy_hash: content_hash_json(&serde_json::json!({
            "minimum_evidence_completeness_ppm": evaluation_policy.minimum_evidence_completeness_ppm,
            "minimum_risk_recall_ppm": evaluation_policy.minimum_risk_recall_ppm,
            "minimum_fresh_pairs_per_horizon": evaluation_policy.minimum_fresh_pairs_per_horizon,
        }))?,
        market_data_feed: feed.as_str().to_owned(),
    })
}

fn redacted_config_hash(config_path: &Path) -> Result<ContentHash> {
    let mut document = read_config_document(config_path)?;
    if let Some(root) = document.as_table_mut() {
        root.remove("credentials");
        if let Some(model) = root.get_mut("model").and_then(toml::Value::as_table_mut) {
            model.remove("api_key");
        }
    }
    Ok(ContentHash::of_bytes(
        toml::to_string(&document)
            .context("serialize redacted v2 TOML")?
            .as_bytes(),
    ))
}

fn source_revision() -> Result<String> {
    Ok(env!("AKZIO_SOURCE_REVISION").to_owned())
}

fn read_config_file(path: &Path) -> Result<Config> {
    fs::read_to_string(path)
        .with_context(|| format!("read v2 config {}", path.display()))
        .and_then(|text| toml::from_str::<Config>(&text).context("parse v2 TOML"))
}

fn read_config_document(path: &Path) -> Result<toml::Value> {
    fs::read_to_string(path)
        .with_context(|| format!("read v2 config {}", path.display()))
        .and_then(|text| toml::from_str::<toml::Value>(&text).context("parse v2 TOML"))
}

fn write_config_file(path: &Path, document: &toml::Value) -> Result<()> {
    let parent = path
        .parent()
        .context("Akzio configuration path has no parent directory")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create Akzio configuration directory {}", parent.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700)).with_context(|| {
            format!("secure Akzio configuration directory {}", parent.display())
        })?;
    }

    let temporary = path.with_extension("toml.tmp");
    let rendered = toml::to_string_pretty(document).context("serialize v2 TOML")?;
    fs::write(&temporary, rendered)
        .with_context(|| format!("write Akzio configuration {}", temporary.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("secure Akzio configuration {}", temporary.display()))?;
    }
    fs::rename(&temporary, path)
        .with_context(|| format!("install Akzio configuration {}", path.display()))?;
    Ok(())
}

fn toml_section_mut<'a>(
    document: &'a mut toml::Value,
    name: &str,
) -> Result<&'a mut toml::map::Map<String, toml::Value>> {
    let root = document
        .as_table_mut()
        .context("Akzio configuration root must be a TOML table")?;
    root.entry(name.to_owned())
        .or_insert_with(|| toml::Value::Table(toml::map::Map::new()))
        .as_table_mut()
        .with_context(|| format!("Akzio configuration [{name}] must be a TOML table"))
}

fn set_optional_toml_string(
    table: &mut toml::map::Map<String, toml::Value>,
    key: &str,
    value: Option<String>,
) {
    if let Some(value) = value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        table.insert(key.to_owned(), toml::Value::String(value));
    } else {
        table.remove(key);
    }
}

fn validate_model_settings(model: &ModelConfig) -> Result<()> {
    if model.base_url.trim().is_empty()
        || model.model.trim().is_empty()
        || model.reasoning_effort.trim().is_empty()
        || model.response_language.trim().is_empty()
    {
        bail!("model base_url, model, reasoning_effort, and response_language must be non-empty");
    }
    for (purpose, route) in &model.routes {
        if !matches!(
            purpose.as_str(),
            "research.planner"
                | "research.analyst"
                | "research.critic"
                | "research.synthesizer"
                | "learning.outcome_worker"
        ) {
            bail!("unsupported model route {purpose}");
        }
        if route.model.trim().is_empty() || route.reasoning_effort.trim().is_empty() {
            bail!("model route {purpose} contains an empty value");
        }
        if route
            .response_language
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            bail!("model route {purpose} contains empty response_language");
        }
    }
    Ok(())
}

fn initial_config_value(value: &str) -> String {
    value
        .strip_prefix('$')
        .and_then(|name| std::env::var(name).ok())
        .unwrap_or_else(|| {
            if value.starts_with('$') {
                String::new()
            } else {
                value.to_owned()
            }
        })
}

fn apply_config_environment(config: &Config) {
    for (name, value) in [
        ("ALPACA_API_KEY", config.credentials.alpaca_api_key.as_str()),
        (
            "ALPACA_API_SECRET",
            config.credentials.alpaca_api_secret.as_str(),
        ),
        (
            "FRED_API_KEY",
            config
                .credentials
                .fred_api_key
                .as_deref()
                .unwrap_or_default(),
        ),
        (
            "SEC_USER_AGENT",
            config
                .observatory
                .sec_user_agent
                .as_deref()
                .unwrap_or_default(),
        ),
    ] {
        if std::env::var_os(name).is_none() && !value.is_empty() {
            std::env::set_var(name, value);
        }
    }
}

fn load_config(path: &Path) -> Result<Config> {
    let mut config = read_config_file(path)?;
    if let Some(model) = config.model.as_mut() {
        model.base_url = resolve_env_placeholder(&model.base_url, "model.base_url")?;
        model.api_key = resolve_env_placeholder(&model.api_key, "model.api_key")?;
        if let Ok(value) = std::env::var("AKZIO_MODEL") {
            model.model = value;
        }
        if let Ok(value) = std::env::var("AKZIO_REASONING_EFFORT") {
            model.reasoning_effort = value;
        }
        if let Ok(value) = std::env::var("AKZIO_RESPONSE_LANGUAGE") {
            model.response_language = value;
        }
        if let Ok(value) = std::env::var("AKZIO_MODEL_ROUTES_JSON") {
            model.routes = serde_json::from_str(&value).context("parse AKZIO_MODEL_ROUTES_JSON")?;
        }
        if model.model.trim().is_empty()
            || model.reasoning_effort.trim().is_empty()
            || model.response_language.trim().is_empty()
        {
            bail!("model, reasoning_effort, and response_language must be non-empty");
        }
        for (purpose, route) in &model.routes {
            if !matches!(
                purpose.as_str(),
                "research.planner"
                    | "research.analyst"
                    | "research.critic"
                    | "research.synthesizer"
                    | "learning.outcome_worker"
            ) {
                bail!("unsupported model route {purpose}");
            }
            if route.model.trim().is_empty() || route.reasoning_effort.trim().is_empty() {
                bail!("model route {purpose} contains an empty value");
            }
            if route
                .response_language
                .as_deref()
                .is_some_and(|value| value.trim().is_empty())
            {
                bail!("model route {purpose} contains an empty response_language");
            }
        }
    }
    if let Some(store_root) = std::env::var_os("AKZIO_STORE_ROOT") {
        config.daemon.store_root = PathBuf::from(store_root);
    }
    apply_config_environment(&config);
    if !config.daemon.http_addr.ip().is_loopback() {
        bail!("daemon.http_addr must be a loopback address");
    }
    if config.daemon.worker_count == Some(0) {
        bail!("daemon.worker_count must be greater than zero");
    }

    let expected = Asset::EXECUTABLE.into_iter().collect::<BTreeSet<_>>();
    let actual = config
        .execution
        .assets
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if actual != expected || config.execution.assets.len() != expected.len() {
        bail!("execution.assets must contain exactly TQQQ, QQQ, SOXX, SOXL");
    }
    OutcomeCostModel {
        transaction_cost_ppm: config.execution.transaction_cost_ppm,
        slippage_ppm: config.execution.slippage_ppm,
    }
    .validate()
    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    if config.daemon.auto_paper.unwrap_or(false)
        && config.execution.transaction_cost_ppm == 0
        && config.execution.slippage_ppm == 0
    {
        bail!("Paper scheduler requires explicit transaction_cost_ppm or slippage_ppm");
    }
    if config.daemon.auto_paper.unwrap_or(false) && config.execution.market_data_feed.is_none() {
        bail!("Paper scheduler requires execution.market_data_feed");
    }
    Ok(config)
}

fn resolve_env_placeholder(value: &str, field: &str) -> Result<String> {
    let Some(name) = value.strip_prefix('$') else {
        return Ok(value.to_owned());
    };
    let (name, suffix) = name.split_once('/').unwrap_or((name, ""));
    if name.is_empty() {
        bail!("{field} environment placeholder is empty");
    }
    let value = std::env::var(name)
        .with_context(|| format!("missing environment variable {name} for {field}"))?;
    Ok(format!("{value}{suffix}"))
}

fn daemon_token(settings: &DaemonSettings) -> Result<String> {
    std::env::var(&settings.token_env).with_context(|| {
        format!(
            "missing daemon token environment variable {}",
            settings.token_env
        )
    })
}

async fn serve(config: &Config, config_path: &Path) -> Result<()> {
    let auto_paper = config.daemon.auto_paper.unwrap_or(false);
    let token = daemon_token(&config.daemon)?;
    let observer_token = config
        .daemon
        .observer_token_env
        .as_deref()
        .map(|name| {
            std::env::var(name)
                .with_context(|| format!("missing observer token environment variable {name}"))
        })
        .transpose()?;
    if observer_token.as_deref().is_some_and(str::is_empty) {
        bail!("observer token environment variable must not be empty");
    }
    let model = config
        .model
        .clone()
        .context("missing [model] configuration for daemon serve")?;
    let runtime_identity_hash = if auto_paper {
        Some(runtime_identity_from_config(config, config_path)?.identity_hash()?)
    } else {
        None
    };
    let daemon = Daemon::open(
        DaemonConfig {
            store_root: config.daemon.store_root.clone(),
            http_token: token,
            observer_token,
            worker_count: config.daemon.worker_count.unwrap_or(4),
            auto_paper,
            market_data_feed: config.execution.market_data_feed,
            outcome_cost_model: OutcomeCostModel {
                transaction_cost_ppm: config.execution.transaction_cost_ppm,
                slippage_ppm: config.execution.slippage_ppm,
            },
            runtime_identity_hash,
        },
        model,
    )?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        if let Err(error) = wait_for_shutdown_signal().await {
            eprintln!("daemon shutdown signal handler failed: {error}");
        }
        let _ = shutdown_tx.send(true);
    });
    let paper = if auto_paper {
        Some(AlpacaPaper::from_env().context("construct Alpaca Paper client")?)
    } else {
        None
    };
    let clock = paper
        .as_ref()
        .map(|paper| AlpacaPaperSessionClock::new(paper.clone()));
    let daemon = match paper {
        Some(paper) => Arc::new(
            daemon
                .with_paper_observer(paper.clone())
                .with_paper_broker(Arc::new(paper)),
        ),
        None => Arc::new(daemon),
    };
    let http_daemon = daemon.clone();
    if auto_paper {
        let source = daemon.paper_workflow_source();
        source
            .proposal("preflight")
            .map_err(|error| anyhow::anyhow!(error.to_string()))
            .context("load Paper workflow proposal")?;
        let clock = clock
            .as_ref()
            .context("Paper scheduler clock was not initialized")?;
        tokio::try_join!(
            http_daemon.serve_http(config.daemon.http_addr, shutdown_rx.clone()),
            daemon
                .serve_with_paper_scheduler(clock, &source, Duration::from_secs(30), shutdown_rx,),
        )?;
    } else {
        tokio::try_join!(
            http_daemon.serve_http(config.daemon.http_addr, shutdown_rx.clone()),
            http_daemon.serve_workers(shutdown_rx),
        )?;
    }
    Ok(())
}

async fn wait_for_shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut terminate = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("wait for Ctrl-C"),
            _ = terminate.recv() => Ok(()),
            result = wait_for_parent_stdin_eof() => result,
        }
    }

    #[cfg(not(unix))]
    {
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("wait for Ctrl-C"),
            result = wait_for_parent_stdin_eof() => result,
        }
    }
}

async fn wait_for_parent_stdin_eof() -> Result<()> {
    if std::env::var_os("AKZIO_EXIT_ON_STDIN_EOF").as_deref() != Some(std::ffi::OsStr::new("1")) {
        std::future::pending::<()>().await;
        unreachable!();
    }
    use tokio::io::AsyncReadExt;
    let mut stdin = tokio::io::stdin();
    let mut byte = [0_u8; 1];
    while stdin
        .read(&mut byte)
        .await
        .context("wait for parent stdin EOF")?
        != 0
    {}
    Ok(())
}

async fn fixture_debug(config: Config) -> Result<()> {
    let report = run_fixture_purpose(config, RunPurpose::Debug).await?;
    if report.status != WorkflowStatus::Completed {
        bail!(
            "fixture Debug workflow did not complete: {:?}",
            report.status
        );
    }
    println!(
        "{}",
        serde_json::json!({
            "run_id": report.run_id,
            "status": report.status,
            "fixture": true,
            "evidence": "fixture/offline"
        })
    );
    Ok(())
}

async fn paper_dry_run(config: Config) -> Result<()> {
    let store_root = config.daemon.store_root.clone();
    let report = run_fixture_purpose(config, RunPurpose::PaperDryRun).await?;
    if report.status != WorkflowStatus::Completed {
        bail!(
            "Paper Dry Run workflow did not complete: {:?}",
            report.status
        );
    }
    let store = V2Store::open_existing(&store_root)?;
    let events = store.events_after(&report.run_id, 0, 10_000)?;
    let canonical_learning_events = events
        .iter()
        .filter(|event| event.event_type == "policy.transitioned")
        .count();
    if canonical_learning_events != 0 {
        bail!("Paper Dry Run produced canonical learning transition");
    }
    store.verify_integrity()?;
    println!(
        "{}",
        serde_json::json!({
            "run_id": report.run_id,
            "purpose": "paper_dry_run",
            "status": format!("{:?}", report.status),
            "canonical_learning_events": canonical_learning_events,
            "fixture": true,
            "evidence": "fixture/offline"
        })
    );
    Ok(())
}

async fn run_fixture_purpose(config: Config, purpose: RunPurpose) -> Result<ReplayReport> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("bind ephemeral fixture control API")?;
    let addr = listener.local_addr()?;
    let token = "fixture-only".to_owned();
    let daemon = fixture_daemon(&config)?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let serve_daemon = daemon.clone();
    let server = tokio::spawn(async move {
        tokio::try_join!(
            serve_daemon.serve_http_listener(listener, shutdown_rx.clone()),
            serve_daemon.serve_workers(shutdown_rx),
        )
    });
    let client = ControlApiClient::new(addr, token)?;
    let mut ready = false;
    for _ in 0..100 {
        if client.health().await.is_ok() {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if !ready {
        let _ = shutdown_tx.send(true);
        let _ = server.await;
        bail!("fixture daemon HTTP control API did not become ready");
    }
    let submitted = match client.submit(purpose).await {
        Ok(submitted) => submitted,
        Err(error) => {
            let _ = shutdown_tx.send(true);
            let _ = server.await;
            return Err(error);
        }
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    let report = loop {
        match client.replay(&submitted.run_id.0).await {
            Ok(report)
                if matches!(
                    report.status,
                    WorkflowStatus::Completed
                        | WorkflowStatus::CompletedWithExecutionRejection
                        | WorkflowStatus::Failed
                        | WorkflowStatus::Cancelled
                ) =>
            {
                break report;
            }
            Ok(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok(report) => {
                let _ = shutdown_tx.send(true);
                let _ = server.await;
                bail!(
                    "fixture workflow did not reach a terminal status: {:?}",
                    report.status
                );
            }
            Err(error) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let _ = error;
            }
            Err(error) => {
                let _ = shutdown_tx.send(true);
                let _ = server.await;
                return Err(error);
            }
        }
    };
    let _ = shutdown_tx.send(true);
    match server.await {
        Ok(Ok(_)) => Ok(report),
        Ok(Err(error)) => Err(anyhow::anyhow!(error)),
        Err(error) => Err(error.into()),
    }
}

fn fixture_daemon(config: &Config) -> Result<Daemon> {
    Ok(Daemon::with_model(
        DaemonConfig {
            store_root: config.daemon.store_root.clone(),
            http_token: "fixture-only".to_owned(),
            observer_token: None,
            worker_count: config.daemon.worker_count.unwrap_or(2),
            auto_paper: false,
            market_data_feed: config.execution.market_data_feed,
            outcome_cost_model: OutcomeCostModel {
                transaction_cost_ppm: config.execution.transaction_cost_ppm,
                slippage_ppm: config.execution.slippage_ppm,
            },
            runtime_identity_hash: None,
        },
        fixture_model_client(),
    )?)
}

async fn diagnostic_test(config: Config, command: TestCommand) -> Result<()> {
    match command {
        TestCommand::CrashRecovery => {
            let daemon = fixture_daemon(&config)?;
            let run_id = daemon.submit_default(RunPurpose::Debug)?;
            let now = Utc::now();
            let _claimed = daemon
                .store()
                .claim_next_task("crash-recovery-fixture", now, ChronoDuration::seconds(30))?
                .context("fixture run had no claimable task")?;
            let recovered = daemon
                .store()
                .recover_expired_tasks(now + ChronoDuration::seconds(31))?;
            if recovered == 0 {
                bail!("expired fixture attempt was not recovered");
            }
            daemon.store().verify_integrity()?;
            println!(
                "{}",
                serde_json::json!({
                    "test": "crash-recovery",
                    "run_id": run_id,
                    "recovered_attempts": recovered,
                    "fixture": true,
                    "evidence": "offline/store-recovery"
                })
            );
        }
        TestCommand::ConcurrentRuns => {
            let daemon = fixture_daemon(&config)?;
            let first = daemon.submit_default(RunPurpose::Debug)?;
            let second = daemon.submit_default(RunPurpose::Debug)?;
            if first == second {
                bail!("fixture runs unexpectedly share a RunId");
            }
            while daemon.run_one("concurrent-runs-fixture").await? {}
            let first_snapshot = daemon.store().workflow_snapshot(&first)?;
            let second_snapshot = daemon.store().workflow_snapshot(&second)?;
            if !matches!(
                first_snapshot.status,
                WorkflowStatus::Completed
                    | WorkflowStatus::CompletedWithExecutionRejection
                    | WorkflowStatus::Failed
            ) || !matches!(
                second_snapshot.status,
                WorkflowStatus::Completed
                    | WorkflowStatus::CompletedWithExecutionRejection
                    | WorkflowStatus::Failed
            ) {
                bail!("concurrent fixture runs did not reach terminal status");
            }
            daemon.store().verify_integrity()?;
            println!(
                "{}",
                serde_json::json!({
                    "test": "concurrent-runs",
                    "run_ids": [first, second],
                    "fixture": true,
                    "evidence": "offline/store-concurrency"
                })
            );
        }
        TestCommand::EvidenceIntegrity => {
            let daemon = fixture_daemon(&config)?;
            let run_id = daemon.submit_default(RunPurpose::Debug)?;
            while daemon.run_one("evidence-integrity-fixture").await? {}
            let events = daemon.store().events_after(&run_id, 0, 10_000)?;
            let artifact_events = events
                .iter()
                .filter(|event| event.artifact_id.is_some())
                .count();
            if artifact_events == 0 {
                bail!("fixture run produced no artifact closure to audit");
            }
            for event in events.iter().filter_map(|event| event.artifact_id.as_ref()) {
                let artifact = daemon.store().artifact(event)?;
                if matches!(artifact.kind, ArtifactKind::RawEvidence)
                    && artifact.lifecycle == akzio_domain::ArtifactLifecycle::Canonical
                {
                    bail!("raw evidence unexpectedly became canonical");
                }
            }
            daemon.store().verify_integrity()?;
            println!(
                "{}",
                serde_json::json!({
                    "test": "evidence-integrity",
                    "run_id": run_id,
                    "artifact_events": artifact_events,
                    "fixture": true,
                    "evidence": "offline/store-closure"
                })
            );
        }
        TestCommand::LearningTransitions => {
            let daemon = fixture_daemon(&config)?;
            let run_id = daemon.submit_default(RunPurpose::PaperDryRun)?;
            while daemon.run_one("learning-transition-fixture").await? {}
            let events = daemon.store().events_after(&run_id, 0, 10_000)?;
            let transitions = events
                .iter()
                .filter(|event| event.event_type == "policy.transitioned")
                .count();
            if transitions != 0 {
                bail!("noncanonical fixture run transitioned policy state");
            }
            daemon.store().verify_integrity()?;
            println!(
                "{}",
                serde_json::json!({
                    "test": "learning-transitions",
                    "run_id": run_id,
                    "policy_transitions": transitions,
                    "fixture": true,
                    "evidence": "offline/noncanonical-boundary"
                })
            );
        }
        TestCommand::FrozenEvidence => {
            let hash = |seed: &str| akzio_domain::ContentHash::of_bytes(seed.as_bytes());
            let record = |case_id: &str, schema_ok: bool| FrozenEvidenceRecord {
                case_id: case_id.to_owned(),
                model_version: "fixture-model-v1".to_owned(),
                prompt_hash: hash("fixture-prompt-v1"),
                contract_hash: hash("fixture-contract-v1"),
                planner_schema_ok: schema_ok,
                claim_schema_ok: schema_ok,
                critique_schema_ok: schema_ok,
                decision_proposal_schema_ok: schema_ok,
                expected_evidence: 4,
                observed_evidence: if schema_ok { 4 } else { 3 },
                expected_blockers: BTreeSet::from([akzio_domain::HardBlocker::MissingEvidence]),
                detected_blockers: if schema_ok {
                    BTreeSet::from([akzio_domain::HardBlocker::MissingEvidence])
                } else {
                    BTreeSet::new()
                },
                input_tokens: 120,
                output_tokens: 80,
                cost_micros: 15,
                latency_millis: if schema_ok { 240 } else { 310 },
            };
            let metrics = evaluate_frozen_evidence(&FrozenEvidenceSet {
                set_id: "cli-frozen-evidence-fixture".to_owned(),
                records: vec![record("case-accepted", true), record("case-blocked", false)],
            })?;
            println!(
                "{}",
                serde_json::json!({
                    "test": "frozen-evidence",
                    "fixture": true,
                    "evidence": "offline/frozen-evidence",
                    "metrics": metrics,
                })
            );
        }
        TestCommand::StoreCorruption => {
            let daemon = fixture_daemon(&config)?;
            let run_id = daemon.submit_default(RunPurpose::Debug)?;
            while daemon.run_one("store-corruption-fixture").await? {}
            let artifact_ref = daemon
                .store()
                .events_after(&run_id, 0, 10_000)?
                .into_iter()
                .find_map(|event| event.artifact_id)
                .context("fixture run produced no artifact to corrupt")?;
            let artifact = daemon.store().artifact(&artifact_ref)?;
            if !daemon
                .store()
                .diagnose_corruption_rejection(&artifact.artifact_id)?
            {
                bail!("Store Doctor accepted a corrupted CAS blob");
            }
            println!(
                "{}",
                serde_json::json!({
                    "test": "store-corruption",
                    "run_id": run_id,
                    "fixture": true,
                    "evidence": "offline/store-doctor-corruption",
                    "doctor_rejected": true,
                })
            );
        }
        TestCommand::FreezeRecovery => {
            let daemon = fixture_daemon(&config)?;
            daemon
                .store()
                .write_freeze_state(true, "fixture freeze", Utc::now())?;
            let frozen_store = V2Store::open(&config.daemon.store_root)?;
            let frozen = frozen_store
                .latest_artifact_by_kind(ArtifactKind::FreezeState)?
                .context("freeze artifact missing after reopen")?;
            daemon
                .store()
                .write_freeze_state(false, "fixture unfreeze", Utc::now())?;
            let unfrozen_store = V2Store::open(&config.daemon.store_root)?;
            let unfrozen = unfrozen_store
                .latest_artifact_by_kind(ArtifactKind::FreezeState)?
                .context("unfreeze artifact missing after reopen")?;
            daemon.store().verify_integrity()?;
            println!(
                "{}",
                serde_json::json!({
                    "test": "freeze-recovery",
                    "fixture": true,
                    "evidence": "offline/freeze-persistence",
                    "frozen_artifact": frozen.artifact_id,
                    "unfrozen_artifact": unfrozen.artifact_id,
                })
            );
        }
        TestCommand::LeaseTakeover => {
            let daemon = fixture_daemon(&config)?;
            let now = Utc::now();
            let first = daemon
                .store()
                .acquire_daemon_lease(
                    "paper-scheduler",
                    "fixture-owner-a",
                    now,
                    now + ChronoDuration::seconds(10),
                )?
                .context("first fixture lease was not acquired")?;
            if daemon
                .store()
                .acquire_daemon_lease(
                    "paper-scheduler",
                    "fixture-owner-b",
                    now + ChronoDuration::seconds(1),
                    now + ChronoDuration::seconds(5),
                )?
                .is_some()
            {
                bail!("live daemon lease was incorrectly stolen");
            }
            let successor = daemon
                .store()
                .acquire_daemon_lease(
                    "paper-scheduler",
                    "fixture-owner-b",
                    now + ChronoDuration::seconds(11),
                    now + ChronoDuration::seconds(21),
                )?
                .context("expired fixture lease was not taken over")?;
            if successor.epoch <= first.epoch
                || daemon
                    .store()
                    .validate_daemon_lease(&first, now + ChronoDuration::seconds(11))
                    .is_ok()
            {
                bail!("stale daemon lease remained valid after takeover");
            }
            daemon.store().verify_integrity()?;
            println!(
                "{}",
                serde_json::json!({
                    "test": "lease-takeover",
                    "fixture": true,
                    "evidence": "offline/daemon-lease-fence",
                    "old_epoch": first.epoch,
                    "new_epoch": successor.epoch,
                })
            );
        }
        TestCommand::Retrospective => {
            let store = V2Store::open(&config.daemon.store_root)?;
            store.verify_integrity()?;
            let latest = store.latest_artifact_by_kind(ArtifactKind::Retrospective)?;
            let latest_horizon = latest
                .as_ref()
                .map(|artifact| {
                    let payload: Retrospective =
                        serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
                    payload.validate()?;
                    Ok::<_, anyhow::Error>(payload.horizon)
                })
                .transpose()?;
            println!(
                "{}",
                serde_json::json!({
                    "test": "retrospective",
                    "ok": true,
                    "latest_horizon": latest_horizon,
                    "evidence": "offline/store-doctor"
                })
            );
        }
    }
    Ok(())
}

fn print_json<T: Serialize>(response: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(response)?);
    Ok(())
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
