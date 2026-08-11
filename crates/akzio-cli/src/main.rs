use std::{collections::BTreeSet, net::SocketAddr, path::PathBuf, sync::Arc};

use akzio_daemon::{
    fixture_model_client, Daemon, DaemonConfig, DaemonHealth, RunCancellationResponse,
    RunRetryResponse, RunSubmissionResponse,
};
use akzio_domain::{Asset, RunPurpose};
use akzio_store::V2Store;
use anyhow::{bail, Context, Result};
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
    let config = std::fs::read_to_string(path)
        .with_context(|| format!("read v2 config {}", path.display()))
        .and_then(|text| toml::from_str::<Config>(&text).context("parse v2 TOML"))?;
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
    if config.daemon.auto_paper.unwrap_or(false) {
        bail!(
            "daemon.auto_paper=true requires an injected scheduler loop; akzio daemon serve refuses to construct one"
        );
    }
    let token = daemon_token(&config.daemon)?;
    let daemon = Arc::new(Daemon::open(DaemonConfig {
        store_root: config.daemon.store_root,
        http_token: token,
        worker_count: config.daemon.worker_count.unwrap_or(4),
        auto_paper: false,
    })?);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = shutdown_tx.send(true);
    });
    let http_daemon = daemon.clone();
    tokio::try_join!(
        http_daemon.serve_http(config.daemon.http_addr, shutdown_rx.clone()),
        daemon.serve_workers(shutdown_rx),
    )?;
    Ok(())
}

async fn fixture_debug(config: Config) -> Result<()> {
    let daemon = Daemon::with_model(
        DaemonConfig {
            store_root: config.daemon.store_root,
            http_token: "fixture-only".to_owned(),
            worker_count: config.daemon.worker_count.unwrap_or(2),
            auto_paper: false,
        },
        fixture_model_client(),
    )?;
    let run_id = daemon.submit_default(RunPurpose::Debug)?;
    while daemon.run_one("fixture").await? {}
    println!("{}", serde_json::json!({"run_id": run_id, "fixture": true}));
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
            let body =
                r#"{"status":"ok","frozen":false,"scheduler_owner":null,"scheduler_epoch":null}"#;
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
