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
    DaemonHealth, PaperWorkflowSource, ReplayReport, RetrospectiveView, RunCancellationResponse,
    RunRetryResponse, RunSubmissionResponse,
};
use akzio_domain::{
    content_hash_json, Artifact, ArtifactKind, ArtifactLifecycle, ArtifactProvenance, ArtifactRef,
    Asset, ContentHash, MoneyMicros, PaperApprovalScope, PaperLaunchApproval, Retrospective, RunId,
    RunPurpose, RuntimeIdentity, RuntimeManifest, WorkflowStatus, V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_execution::{paper::AlpacaPaper, DecisionPolicy, ExecutionPolicy};
use akzio_learning::{
    evaluate_frozen_evidence, EvaluationPolicy, FrozenEvidenceRecord, FrozenEvidenceSet,
    OutcomeCostModel,
};
use akzio_model::ModelConfig;
use akzio_store::{
    v2::{SessionSlot, StoredRun, TrajectoryEntry},
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
}

#[derive(Debug, Subcommand)]
enum ObservatoryConfigCommand {
    Init {
        #[arg(long)]
        template: PathBuf,
        #[arg(long)]
        store_root: PathBuf,
        #[arg(long)]
        legacy_store: Option<PathBuf>,
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
        Command::Daemon {
            command: DaemonAction::Serve,
        } => serve(config, &config_path).await,
        Command::Daemon {
            command: DaemonAction::Health,
        } => print_json(&ControlApiClient::from_config(&config)?.health().await?),
        Command::Daemon {
            command: DaemonAction::Ready,
        } => print_json(&ControlApiClient::from_config(&config)?.ready().await?),
        Command::Daemon {
            command: DaemonAction::Freeze { reason },
        } => print_json(
            &ControlApiClient::from_config(&config)?
                .set_freeze(true, &reason)
                .await?,
        ),
        Command::Daemon {
            command: DaemonAction::Unfreeze { reason },
        } => print_json(
            &ControlApiClient::from_config(&config)?
                .set_freeze(false, &reason)
                .await?,
        ),
        Command::Run {
            command: RunCommand::Submit { purpose },
        } => print_json(
            &ControlApiClient::from_config(&config)?
                .submit(purpose.into())
                .await?,
        ),
        Command::Run {
            command: RunCommand::Replay { run_id },
        } => print_json(
            &ControlApiClient::from_config(&config)?
                .replay(&run_id)
                .await?,
        ),
        Command::Run {
            command: RunCommand::Retrospectives { run_id },
        } => print_json(
            &ControlApiClient::from_config(&config)?
                .retrospectives(&run_id)
                .await?,
        ),
        Command::Run {
            command: RunCommand::Trajectory { run_id },
        } => print_json(
            &ControlApiClient::from_config(&config)?
                .trajectory(&run_id)
                .await?,
        ),
        Command::Run {
            command: RunCommand::Events { run_id, after },
        } => {
            ControlApiClient::from_config(&config)?
                .events(&run_id, after)
                .await
        }
        Command::Run {
            command: RunCommand::Cancel { run_id },
        } => print_json(
            &ControlApiClient::from_config(&config)?
                .cancel(&run_id)
                .await?,
        ),
        Command::Run {
            command: RunCommand::Retry { run_id },
        } => print_json(
            &ControlApiClient::from_config(&config)?
                .retry(&run_id)
                .await?,
        ),
        Command::Run {
            command: RunCommand::FixtureDebug,
        } => fixture_debug(config).await,
        Command::Run {
            command: RunCommand::PaperDryRun,
        } => paper_dry_run(config).await,
        Command::Test { command } => diagnostic_test(config, command).await,
        Command::Store {
            command: StoreCommand::Doctor,
        } => {
            V2Store::open_existing(&config.daemon.store_root)?.verify_integrity()?;
            println!("{{\"ok\":true}}");
            Ok(())
        }
        Command::Store {
            command: StoreCommand::Inventory,
        } => print_json(&V2Store::open_existing(&config.daemon.store_root)?.storage_inventory()?),
        Command::Store {
            command: StoreCommand::Metrics,
        } => print_json(&V2Store::open_existing(&config.daemon.store_root)?.metrics(Utc::now())?),
        Command::Store {
            command: StoreCommand::Alerts,
        } => {
            let metrics = V2Store::open_existing(&config.daemon.store_root)?.metrics(Utc::now())?;
            print_json(&metrics.alerts())
        }
        Command::Store {
            command: StoreCommand::PaperSession { session_key },
        } => {
            let slot = V2Store::open_existing(&config.daemon.store_root)?
                .session_slot(&session_key)?
                .map(PaperSessionView::from);
            print_json(&slot)
        }
        Command::Store {
            command:
                StoreCommand::ApprovePaper {
                    session_key,
                    operator,
                    reason,
                    max_notional_usd_cents,
                    valid_hours,
                },
        } => {
            approve_paper(
                &config,
                &config_path,
                &session_key,
                &operator,
                &reason,
                max_notional_usd_cents,
                valid_hours,
            )
            .await
        }
        Command::Store {
            command: StoreCommand::Backup { target },
        } => print_json(&V2Store::open_existing(&config.daemon.store_root)?.backup_to(target)?),
        Command::Store {
            command:
                StoreCommand::ExportRun {
                    run_id,
                    target,
                    include_raw_model,
                },
        } => print_json(
            &V2Store::open_existing(&config.daemon.store_root)?.export_run(
                &RunId(run_id),
                target,
                include_raw_model,
            )?,
        ),
        Command::Store {
            command: StoreCommand::Restore { source, target },
        } => {
            let store = V2Store::restore_from(source, target)?;
            print_json(&store.metrics(Utc::now())?)
        }
    }
}

fn handle_observatory_config(config_path: &Path, command: &ObservatoryConfigCommand) -> Result<()> {
    match command {
        ObservatoryConfigCommand::Init {
            template,
            store_root,
            legacy_store,
        } => {
            if config_path.exists() {
                return print_json(&serde_json::json!({ "created": false }));
            }
            if !store_root.exists()
                && legacy_store
                    .as_ref()
                    .is_some_and(|legacy| legacy.join("akzio.sqlite3").is_file())
            {
                let legacy = legacy_store.as_ref().expect("checked above");
                V2Store::open_existing(legacy)?.backup_to(store_root)?;
                V2Store::open(store_root)?.clear_observatory_configuration()?;
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
    let session = chrono::NaiveDate::parse_from_str(session_key, "%Y-%m-%d")
        .context("session_key must be YYYY-MM-DD")?;
    if operator.trim().is_empty()
        || reason.trim().is_empty()
        || max_notional_usd_cents <= 0
        || valid_hours <= 0
        || valid_hours > 24 * 7
    {
        bail!("invalid Paper approval scope");
    }
    let execution_policy = ExecutionPolicy::default();
    let maximum_notional = MoneyMicros::from_usd_cents(max_notional_usd_cents);
    if maximum_notional.0 > execution_policy.max_new_notional.0 {
        bail!("approval max notional exceeds execution policy");
    }
    let paper = AlpacaPaper::from_env().context("construct Paper broker for approval")?;
    let account = paper
        .account()
        .await
        .context("read Paper account for approval")?;
    let broker_account_id = account
        .get("id")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .context("Paper account id missing")?
        .to_owned();
    let now = Utc::now();
    let identity = runtime_identity_from_config(config, config_path)?;
    let manifest_payload = RuntimeManifest {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        code_revision: identity.code_revision,
        cargo_lock_hash: identity.cargo_lock_hash,
        config_hash: identity.config_hash,
        provider_id: identity.provider_id,
        model_id: identity.model_id,
        prompt_hash: identity.prompt_hash,
        contract_hash: identity.contract_hash,
        topology_hash: identity.topology_hash,
        decision_policy_hash: identity.decision_policy_hash,
        execution_policy_hash: identity.execution_policy_hash,
        evaluation_policy_hash: identity.evaluation_policy_hash,
        market_data_feed: identity.market_data_feed,
        broker_account_id,
        maximum_notional,
        allowed_session_start: session,
        allowed_session_end: session,
        expires_at: now + ChronoDuration::hours(valid_hours),
        created_at: now,
    };
    let manifest_hash = manifest_payload.manifest_hash()?;
    let store = V2Store::open(&config.daemon.store_root)?;
    let manifest = Artifact::new(
        ArtifactKind::RuntimeManifest,
        store.put_json(&manifest_payload)?,
        "runtime.manifest",
        ArtifactLifecycle::Canonical,
        ArtifactProvenance {
            source_family: "akzio.operator".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        },
        None,
        vec![],
        now,
    )?;
    store.write_bootstrap_artifact(&manifest)?;
    let mut approval_payload = PaperLaunchApproval {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        operator_identity: operator.to_owned(),
        runtime_manifest: ArtifactRef {
            artifact_id: manifest.artifact_id.clone(),
            kind: ArtifactKind::RuntimeManifest,
        },
        runtime_manifest_hash: manifest_hash.clone(),
        scope: PaperApprovalScope::Canary,
        reason: reason.to_owned(),
        approved_at: now,
        expires_at: manifest_payload.expires_at,
        approval_hash: ContentHash::of_bytes(b"pending"),
    };
    approval_payload.approval_hash = approval_payload.unsigned_hash()?;
    let approval = Artifact::new(
        ArtifactKind::PaperLaunchApproval,
        store.put_json(&approval_payload)?,
        "operator.paper_approval",
        ArtifactLifecycle::Canonical,
        ArtifactProvenance {
            source_family: "akzio.operator".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        },
        None,
        vec![approval_payload.runtime_manifest.clone()],
        now,
    )?;
    store.write_bootstrap_artifact(&approval)?;
    print_json(&serde_json::json!({
        "session_key": session_key,
        "runtime_manifest_artifact_id": manifest.artifact_id,
        "runtime_manifest_hash": manifest_hash,
        "approval_artifact_id": approval.artifact_id,
        "approval_hash": approval_payload.approval_hash,
        "expires_at": approval_payload.expires_at,
    }))
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
        "crates/akzio-research/src/v2.rs",
        include_bytes!("../../akzio-research/src/v2.rs"),
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
    if name.is_empty() {
        bail!("{field} environment placeholder is empty");
    }
    std::env::var(name).with_context(|| format!("missing environment variable {name} for {field}"))
}

fn daemon_token(settings: &DaemonSettings) -> Result<String> {
    std::env::var(&settings.token_env).with_context(|| {
        format!(
            "missing daemon token environment variable {}",
            settings.token_env
        )
    })
}

async fn serve(config: Config, config_path: &Path) -> Result<()> {
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
        Some(runtime_identity_from_config(&config, config_path)?.identity_hash()?)
    } else {
        None
    };
    let daemon = Daemon::open(
        DaemonConfig {
            store_root: config.daemon.store_root,
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
    let daemon = fixture_daemon(&config)?;
    let run_id = daemon.submit_default(RunPurpose::Debug)?;
    while daemon.run_one("fixture").await? {}
    let snapshot = daemon.store().workflow_snapshot(&run_id)?;
    if snapshot.status != WorkflowStatus::Completed {
        bail!(
            "fixture Debug workflow did not complete: {:?}",
            snapshot.status
        );
    }
    println!(
        "{}",
        serde_json::json!({
            "run_id": run_id,
            "status": snapshot.status,
            "fixture": true,
            "evidence": "fixture/offline"
        })
    );
    Ok(())
}

async fn paper_dry_run(config: Config) -> Result<()> {
    let daemon = fixture_daemon(&config)?;
    let run_id = daemon.submit_default(RunPurpose::PaperDryRun)?;
    while daemon.run_one("paper-dry-run-fixture").await? {}
    let snapshot = daemon.store().workflow_snapshot(&run_id)?;
    if snapshot.status != WorkflowStatus::Completed {
        bail!(
            "Paper Dry Run workflow did not complete: {:?}",
            snapshot.status
        );
    }
    let events = daemon.store().events_after(&run_id, 0, 10_000)?;
    let canonical_learning_events = events
        .iter()
        .filter(|event| event.event_type == "policy.transitioned")
        .count();
    if canonical_learning_events != 0 {
        bail!("Paper Dry Run produced canonical learning transition");
    }
    daemon.store().verify_integrity()?;
    println!(
        "{}",
        serde_json::json!({
            "run_id": run_id,
            "purpose": "paper_dry_run",
            "status": format!("{:?}", snapshot.status),
            "canonical_learning_events": canonical_learning_events,
            "fixture": true,
            "evidence": "fixture/offline"
        })
    );
    Ok(())
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
