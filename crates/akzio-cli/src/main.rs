use std::{collections::BTreeSet, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use akzio_daemon::{
    fixture_model_client, AlpacaPaperSessionClock, Daemon, DaemonConfig, DaemonHealth,
    ReplayReport, RunCancellationResponse, RunRetryResponse, RunSubmissionResponse,
    StorePaperWorkflowSource,
};
use akzio_domain::{ArtifactKind, Asset, RunPurpose, WorkflowStatus};
use akzio_execution::paper::AlpacaPaper;
use akzio_model::ModelConfig;
use akzio_store::V2Store;
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
}

#[derive(Debug, Subcommand)]
enum StoreCommand {
    Doctor,
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
}

#[derive(Debug, Serialize)]
struct SubmitRequest {
    purpose: RunPurpose,
}

#[derive(Debug, Serialize)]
struct FreezeRequest<'a> {
    reason: &'a str,
}

struct ControlApiClient {
    base_url: Url,
    client: Client,
    token: String,
}

impl From<PurposeArg> for RunPurpose {
    fn from(value: PurposeArg) -> Self {
        match value {
            PurposeArg::Debug => Self::Debug,
            PurposeArg::PaperDryRun => Self::PaperDryRun,
        }
    }
}

impl ControlApiClient {
    fn from_config(config: &Config) -> Result<Self> {
        Self::new(config.daemon.http_addr, daemon_token(&config.daemon)?)
    }

    fn new(address: SocketAddr, token: String) -> Result<Self> {
        if !address.ip().is_loopback() {
            bail!("daemon.http_addr must be a loopback address");
        }
        if token.trim().is_empty() || token.contains('\r') || token.contains('\n') {
            bail!("daemon token must be nonempty and contain no newlines");
        }

        Ok(Self {
            base_url: Url::parse(&format!("http://{address}/"))
                .context("build loopback control API URL")?,
            client: Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .context("build loopback control API client")?,
            token,
        })
    }

    fn endpoint(&self, segments: &[&str]) -> Url {
        let mut url = self.base_url.clone();
        let mut path = url
            .path_segments_mut()
            .expect("loopback control API URL must be hierarchical");
        path.pop_if_empty();
        for segment in segments {
            path.push(segment);
        }
        drop(path);
        url
    }

    fn request(&self, method: Method, url: Url) -> RequestBuilder {
        self.client
            .request(method, url)
            .header("x-akzio-token", &self.token)
    }

    async fn json<T: DeserializeOwned>(&self, request: RequestBuilder) -> Result<T> {
        let response = request
            .send()
            .await
            .context("call loopback HTTP control API")?;
        require_success(response)
            .await?
            .json()
            .await
            .context("decode loopback control API response")
    }

    async fn health(&self) -> Result<DaemonHealth> {
        self.json(self.request(Method::GET, self.endpoint(&["health"])))
            .await
    }

    async fn submit(&self, purpose: RunPurpose) -> Result<RunSubmissionResponse> {
        self.json(
            self.request(Method::POST, self.endpoint(&["runs"]))
                .json(&SubmitRequest { purpose }),
        )
        .await
    }

    async fn replay(&self, run_id: &str) -> Result<ReplayReport> {
        self.json(self.request(Method::GET, self.endpoint(&["runs", run_id, "replay"])))
            .await
    }

    async fn cancel(&self, run_id: &str) -> Result<RunCancellationResponse> {
        self.json(self.request(Method::POST, self.endpoint(&["runs", run_id, "cancel"])))
            .await
    }

    async fn retry(&self, run_id: &str) -> Result<RunRetryResponse> {
        self.json(self.request(Method::POST, self.endpoint(&["runs", run_id, "retry"])))
            .await
    }

    async fn set_freeze(&self, frozen: bool, reason: &str) -> Result<DaemonHealth> {
        let action = if frozen { "freeze" } else { "unfreeze" };
        self.json(
            self.request(Method::POST, self.endpoint(&["control", action]))
                .json(&FreezeRequest { reason }),
        )
        .await
    }

    async fn events(&self, run_id: &str, after: i64) -> Result<()> {
        let mut url = self.endpoint(&["runs", run_id, "events"]);
        url.query_pairs_mut()
            .append_pair("after", &after.to_string());
        let response = self
            .request(Method::GET, url)
            .header("accept", "text/event-stream")
            .send()
            .await
            .context("open loopback event stream")?;
        let mut stream = require_success(response).await?.bytes_stream();
        let mut pending = String::new();
        let mut event_data = Vec::new();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("read loopback event stream")?;
            pending.push_str(
                std::str::from_utf8(chunk.as_ref())
                    .context("loopback control API emitted non-UTF-8 SSE")?,
            );

            while let Some(newline) = pending.find('\n') {
                let line = pending.drain(..=newline).collect::<String>();
                let line = line.trim_end_matches(&['\r', '\n'][..]);
                if line.is_empty() {
                    print_sse_data(&mut event_data);
                } else if let Some(data) = line.strip_prefix("data:") {
                    event_data.push(data.strip_prefix(' ').unwrap_or(data).to_owned());
                }
            }
        }
        print_sse_data(&mut event_data);
        Ok(())
    }
}

async fn require_success(response: Response) -> Result<Response> {
    if response.status().is_success() {
        Ok(response)
    } else {
        bail!("loopback control API returned HTTP {}", response.status());
    }
}

fn print_sse_data(event_data: &mut Vec<String>) {
    if !event_data.is_empty() {
        println!("{}", event_data.join("\n"));
        event_data.clear();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = load_config(&cli.config)?;

    match cli.command {
        Command::Daemon {
            command: DaemonAction::Serve,
        } => serve(config).await,
        Command::Daemon {
            command: DaemonAction::Health,
        } => print_json(&ControlApiClient::from_config(&config)?.health().await?),
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
            V2Store::open(&config.daemon.store_root)?.verify_integrity()?;
            println!("{{\"ok\":true}}");
            Ok(())
        }
    }
}

fn load_config(path: &PathBuf) -> Result<Config> {
    let mut config = std::fs::read_to_string(path)
        .with_context(|| format!("read v2 config {}", path.display()))
        .and_then(|text| toml::from_str::<Config>(&text).context("parse v2 TOML"))?;
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
    Ok(config)
}

fn daemon_token(settings: &DaemonSettings) -> Result<String> {
    std::env::var(&settings.token_env).with_context(|| {
        format!(
            "missing daemon token environment variable {}",
            settings.token_env
        )
    })
}

async fn serve(config: Config) -> Result<()> {
    let auto_paper = config.daemon.auto_paper.unwrap_or(false);
    let token = daemon_token(&config.daemon)?;
    let model = config
        .model
        .clone()
        .context("missing [model] configuration for daemon serve")?;
    let daemon = Daemon::open(
        DaemonConfig {
            store_root: config.daemon.store_root,
            http_token: token,
            worker_count: config.daemon.worker_count.unwrap_or(4),
            auto_paper,
        },
        model,
    )?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = shutdown_tx.send(true);
    });
    let http_daemon = Arc::new(daemon.clone());
    if auto_paper {
        let paper = AlpacaPaper::from_env().context("construct Alpaca Paper client")?;
        let clock = AlpacaPaperSessionClock::new(paper.clone());
        let source = StorePaperWorkflowSource::new(daemon.store().clone());
        let scheduler_daemon = Arc::new(daemon.with_paper_broker(Arc::new(paper)));
        tokio::try_join!(
            http_daemon.serve_http(config.daemon.http_addr, shutdown_rx.clone()),
            scheduler_daemon.serve_with_paper_scheduler(
                &clock,
                &source,
                Duration::from_secs(30),
                shutdown_rx,
            ),
        )?;
    } else {
        tokio::try_join!(
            http_daemon.serve_http(config.daemon.http_addr, shutdown_rx.clone()),
            http_daemon.serve_workers(shutdown_rx),
        )?;
    }
    Ok(())
}

async fn fixture_debug(config: Config) -> Result<()> {
    let daemon = fixture_daemon(&config)?;
    let run_id = daemon.submit_default(RunPurpose::Debug)?;
    while daemon.run_one("fixture").await? {}
    println!(
        "{}",
        serde_json::json!({
            "run_id": run_id,
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
    }
    Ok(())
}

fn print_json<T: Serialize>(response: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(response)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::TcpListener,
    };

    fn write_config(directory: &tempfile::TempDir, daemon: &str, assets: &str) -> PathBuf {
        let path = directory.path().join("akzio.toml");
        std::fs::write(
            &path,
            format!(
                "[daemon]\nstore_root='store'\n{daemon}\ntoken_env='TOKEN'\n[execution]\nassets={assets}\n"
            ),
        )
        .unwrap();
        path
    }

    #[test]
    fn config_rejects_a_partial_executable_universe() {
        let directory = tempfile::tempdir().unwrap();
        let path = write_config(&directory, "http_addr='127.0.0.1:1'", "['TQQQ']");

        assert!(load_config(&path).is_err());
    }

    #[test]
    fn config_rejects_legacy_socket_configuration() {
        let directory = tempfile::tempdir().unwrap();
        let path = write_config(
            &directory,
            "http_addr='127.0.0.1:1'\nunix_socket='daemon.sock'",
            "['TQQQ', 'QQQ', 'SOXX', 'SOXL']",
        );

        assert!(load_config(&path).is_err());
    }

    #[test]
    fn config_rejects_non_loopback_control_address() {
        let directory = tempfile::tempdir().unwrap();
        let path = write_config(
            &directory,
            "http_addr='0.0.0.0:1'",
            "['TQQQ', 'QQQ', 'SOXX', 'SOXL']",
        );

        assert!(load_config(&path).is_err());
    }

    #[test]
    fn config_reads_local_model_settings() {
        let directory = tempfile::tempdir().unwrap();
        let path = write_config(
            &directory,
            "http_addr='127.0.0.1:1'",
            "['TQQQ', 'QQQ', 'SOXX', 'SOXL']",
        );
        let mut text = std::fs::read_to_string(&path).unwrap();
        text.push_str(
            "[model]\nbase_url='http://fixture/v1'\nmodel='fixture-model'\napi_key='fixture-key'\nreasoning_effort='high'\ndebug=true\n",
        );
        std::fs::write(&path, text).unwrap();

        let model = load_config(&path).unwrap().model.unwrap();
        assert_eq!(model.base_url, "http://fixture/v1");
        assert_eq!(model.model, "fixture-model");
        assert_eq!(model.reasoning_effort, "high");
        assert!(model.debug);
    }

    #[test]
    fn control_client_refuses_non_loopback_address() {
        assert!(
            ControlApiClient::new("0.0.0.0:1".parse().unwrap(), "fixture-token".to_owned())
                .is_err()
        );
    }

    #[tokio::test]
    async fn control_client_uses_loopback_http_with_token() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut lines = BufReader::new(read).lines();
            let mut request = Vec::new();
            while let Some(line) = lines.next_line().await.unwrap() {
                if line.is_empty() {
                    break;
                }
                request.push(line);
            }
            let body = r#"{"status":"ok","frozen":false,"scheduler_owner":null,"scheduler_epoch":null,"metrics":{"run_counts":{},"task_counts":{},"attempt_counts":{},"event_count":0,"active_daemon_leases":0}}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            write.write_all(response.as_bytes()).await.unwrap();
            request
        });

        let health = ControlApiClient::new(address, "fixture-token".to_owned())
            .unwrap()
            .health()
            .await
            .unwrap();
        assert_eq!(health.status, "ok");
        assert!(!health.frozen);

        let request = server.await.unwrap();
        assert_eq!(
            request.first().map(String::as_str),
            Some("GET /health HTTP/1.1")
        );
        assert!(request
            .iter()
            .any(|line| line.eq_ignore_ascii_case("x-akzio-token: fixture-token")));
    }

    #[test]
    fn help_has_no_unix_control_surface() {
        let mut command = Cli::command();
        let help = command.render_long_help().to_string();
        assert!(!help.to_ascii_lowercase().contains("unix"));
        assert!(Cli::try_parse_from(["akzio", "daemon", "unfreeze", "fixture reason"]).is_ok());
    }
}
