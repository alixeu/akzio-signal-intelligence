use std::{collections::BTreeSet, net::SocketAddr, path::PathBuf, sync::Arc};

use akzio_daemon::{fixture_model_client, Daemon, DaemonCommand, DaemonConfig, DaemonReply};
use akzio_domain::{Asset, RunPurpose};
use akzio_store::V2Store;
use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use serde::Deserialize;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    sync::watch,
};

#[derive(Debug, Parser)]
#[command(name = "akzio", about = "Akzio v2 daemon client")]
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
        command: DaemonCommandArgs,
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
enum DaemonCommandArgs {
    Serve,
    Health,
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

impl From<PurposeArg> for RunPurpose {
    fn from(value: PurposeArg) -> Self {
        match value {
            PurposeArg::Debug => Self::Debug,
            PurposeArg::PaperDryRun => Self::PaperDryRun,
        }
    }
}

#[derive(Debug, Deserialize)]
struct Config {
    daemon: DaemonSettings,
    execution: ExecutionSettings,
}

#[derive(Debug, Deserialize)]
struct DaemonSettings {
    store_root: PathBuf,
    http_addr: SocketAddr,
    unix_socket: PathBuf,
    token_env: String,
    worker_count: Option<usize>,
    auto_paper: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ExecutionSettings {
    assets: Vec<Asset>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = load_config(&cli.config)?;
    match cli.command {
        Command::Daemon {
            command: DaemonCommandArgs::Serve,
        } => serve(config).await,
        Command::Daemon {
            command: DaemonCommandArgs::Health,
        } => print_reply(unix_command(&config.daemon.unix_socket, DaemonCommand::Health).await?),
        Command::Run {
            command: RunCommand::Submit { purpose },
        } => print_reply(
            unix_command(
                &config.daemon.unix_socket,
                DaemonCommand::Submit {
                    purpose: purpose.into(),
                },
            )
            .await?,
        ),
        Command::Run {
            command: RunCommand::Events { run_id, after },
        } => print_reply(
            unix_command(
                &config.daemon.unix_socket,
                DaemonCommand::Events {
                    run_id: akzio_domain::RunId(run_id),
                    after,
                },
            )
            .await?,
        ),
        Command::Run {
            command: RunCommand::Cancel { run_id },
        } => print_reply(
            unix_command(
                &config.daemon.unix_socket,
                DaemonCommand::Cancel {
                    run_id: akzio_domain::RunId(run_id),
                },
            )
            .await?,
        ),
        Command::Run {
            command: RunCommand::Retry { run_id },
        } => print_reply(
            unix_command(
                &config.daemon.unix_socket,
                DaemonCommand::Retry {
                    run_id: akzio_domain::RunId(run_id),
                },
            )
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

async fn serve(config: Config) -> Result<()> {
    let token = std::env::var(&config.daemon.token_env).with_context(|| {
        format!(
            "missing daemon token environment variable {}",
            config.daemon.token_env
        )
    })?;
    let daemon = Arc::new(Daemon::open(DaemonConfig {
        store_root: config.daemon.store_root,
        http_token: token,
        worker_count: config.daemon.worker_count.unwrap_or(4),
        auto_paper: config.daemon.auto_paper.unwrap_or(true),
    })?);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = shutdown_tx.send(true);
    });
    tokio::try_join!(
        daemon
            .clone()
            .serve_http(config.daemon.http_addr, shutdown_rx.clone()),
        daemon
            .clone()
            .serve_unix(config.daemon.unix_socket, shutdown_rx.clone()),
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

async fn unix_command(path: &PathBuf, command: DaemonCommand) -> Result<DaemonReply> {
    let stream = UnixStream::connect(path)
        .await
        .with_context(|| format!("connect daemon socket {}", path.display()))?;
    let (read, mut write) = stream.into_split();
    write
        .write_all(format!("{}\n", serde_json::to_string(&command)?).as_bytes())
        .await?;
    let mut lines = BufReader::new(read).lines();
    let line = lines
        .next_line()
        .await?
        .context("daemon closed without a reply")?;
    Ok(serde_json::from_str(&line)?)
}

fn print_reply(reply: DaemonReply) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(&reply)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_rejects_a_partial_executable_universe() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("akzio.toml");
        std::fs::write(
            &path,
            "[daemon]\nstore_root='store'\nhttp_addr='127.0.0.1:1'\nunix_socket='daemon.sock'\ntoken_env='TOKEN'\n[execution]\nassets=['TQQQ']\n",
        )
        .unwrap();
        assert!(load_config(&path).is_err());
    }
}
