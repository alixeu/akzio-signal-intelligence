use std::{
    collections::BTreeSet,
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Command as ProcessCommand,
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
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DaemonSettings {
    store_root: PathBuf,
    http_addr: SocketAddr,
    token_env: String,
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
    let config_path = cli.config.clone();
    let config = load_config(&config_path)?;

    match cli.command {
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

fn component_hash(paths: &[&str]) -> Result<ContentHash> {
    let mut bytes = Vec::new();
    for path in paths {
        bytes.extend_from_slice(path.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(&fs::read(path).with_context(|| format!("read {path}"))?);
        bytes.push(0);
    }
    Ok(ContentHash::of_bytes(&bytes))
}

const PROMPT_COMPONENTS: &[&str] = &[
    "crates/akzio-research/src/agent_v2.rs",
    "crates/akzio-research/src/agent_v2/catalogue.rs",
    "crates/akzio-research/src/agent_v2/schemas.rs",
    "crates/akzio-research/src/v2.rs",
];

const CONTRACT_COMPONENTS: &[&str] = &[
    "crates/akzio-domain/src/contract.rs",
    "crates/akzio-research/src/agent_v2.rs",
    "crates/akzio-research/src/agent_v2/catalogue.rs",
    "crates/akzio-research/src/agent_v2/schemas.rs",
    "crates/akzio-research/src/v2.rs",
];

const TOPOLOGY_COMPONENTS: &[&str] = &[
    "crates/akzio-runtime/src/runtime_v2.rs",
    "crates/akzio-runtime/src/runtime_v2/catalogue.rs",
    "crates/akzio-runtime/src/runtime_v2/planner.rs",
    "crates/akzio-runtime/src/runtime_v2/reducer.rs",
    "crates/akzio-runtime/src/runtime_v2/replay.rs",
    "crates/akzio-runtime/src/runtime_v2/task.rs",
    "crates/akzio-runtime/src/runtime_v2/workflow.rs",
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
        cargo_lock_hash: ContentHash::of_bytes(&fs::read("Cargo.lock").context("read Cargo.lock")?),
        config_hash: ContentHash::of_bytes(&fs::read(config_path).context("read config")?),
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

fn source_revision() -> Result<String> {
    let head = ProcessCommand::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .context("run git rev-parse")?;
    if !head.status.success() {
        bail!("git rev-parse failed");
    }
    let head = String::from_utf8(head.stdout)?.trim().to_owned();
    let diff = ProcessCommand::new("git")
        .args(["diff", "--binary", "HEAD", "--"])
        .output()
        .context("run git diff")?;
    if !diff.status.success() {
        bail!("git diff failed");
    }
    let untracked = ProcessCommand::new("git")
        .args(["ls-files", "--others", "--exclude-standard", "-z"])
        .output()
        .context("list untracked files")?;
    if !untracked.status.success() {
        bail!("git untracked scan failed");
    }
    let mut state = diff.stdout;
    for path in untracked
        .stdout
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
    {
        let path = PathBuf::from(String::from_utf8(path.to_vec())?);
        state.extend_from_slice(path.as_os_str().as_encoded_bytes());
        state.push(0);
        state.extend_from_slice(
            &fs::read(&path).with_context(|| format!("read {}", path.display()))?,
        );
        state.push(0);
    }
    if state.is_empty() {
        Ok(head)
    } else {
        Ok(format!("{head}+worktree:{}", ContentHash::of_bytes(&state)))
    }
}

fn load_config(path: &PathBuf) -> Result<Config> {
    let mut config = std::fs::read_to_string(path)
        .with_context(|| format!("read v2 config {}", path.display()))
        .and_then(|text| toml::from_str::<Config>(&text).context("parse v2 TOML"))?;
    if let Some(model) = config.model.as_mut() {
        model.base_url = resolve_env_placeholder(&model.base_url, "model.base_url")?;
        model.api_key = resolve_env_placeholder(&model.api_key, "model.api_key")?;
    }
    if let Some(store_root) = std::env::var_os("AKZIO_STORE_ROOT") {
        config.daemon.store_root = PathBuf::from(store_root);
    }
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
        Some(paper) => Arc::new(daemon.with_paper_broker(Arc::new(paper))),
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
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.context("wait for Ctrl-C")
    }
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
