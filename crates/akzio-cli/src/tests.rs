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
fn runtime_identity_components_cover_split_modules() {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for path in PROMPT_COMPONENTS
        .iter()
        .chain(CONTRACT_COMPONENTS)
        .chain(TOPOLOGY_COMPONENTS)
    {
        assert!(
            workspace_root.join(path).is_file(),
            "missing runtime identity component {path}"
        );
    }
    assert!(PROMPT_COMPONENTS.contains(&"crates/akzio-research/src/agent_v2/schemas.rs"));
    assert!(CONTRACT_COMPONENTS.contains(&"crates/akzio-research/src/agent_v2/catalogue.rs"));
    assert!(TOPOLOGY_COMPONENTS.contains(&"crates/akzio-runtime/src/runtime_v2/planner.rs"));
    assert!(TOPOLOGY_COMPONENTS.contains(&"crates/akzio-runtime/src/runtime_v2/workflow.rs"));
}

#[test]
fn config_rejects_a_partial_executable_universe() {
    let directory = tempfile::tempdir().unwrap();
    let path = write_config(&directory, "http_addr='127.0.0.1:1'", "['TQQQ']");

    assert!(load_config(&path).is_err());
}

#[test]
fn paper_session_command_accepts_broker_session_key() {
    assert!(Cli::try_parse_from(["akzio", "store", "paper-session", "2026-08-17",]).is_ok());
}

#[test]
fn config_rejects_zero_cost_auto_paper() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("akzio.toml");
    std::fs::write(
            &path,
            "[daemon]\nstore_root='store'\nauto_paper=true\nhttp_addr='127.0.0.1:1'\ntoken_env='TOKEN'\n[execution]\nassets=['TQQQ', 'QQQ', 'SOXX', 'SOXL']\n",
        )
        .unwrap();

    let error = load_config(&path).unwrap_err().to_string();
    assert!(error.contains("transaction_cost_ppm or slippage_ppm"));
}

#[test]
fn config_rejects_auto_paper_without_market_data_feed() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("akzio.toml");
    std::fs::write(
            &path,
            "[daemon]\nstore_root='store'\nauto_paper=true\nhttp_addr='127.0.0.1:1'\ntoken_env='TOKEN'\n[execution]\nassets=['TQQQ', 'QQQ', 'SOXX', 'SOXL']\ntransaction_cost_ppm=1\nslippage_ppm=1\n",
        )
        .unwrap();

    let error = load_config(&path).unwrap_err().to_string();
    assert!(error.contains("execution.market_data_feed"));
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
fn config_resolves_model_environment_placeholders() {
    let directory = tempfile::tempdir().unwrap();
    let path = write_config(
        &directory,
        "http_addr='127.0.0.1:1'",
        "['TQQQ', 'QQQ', 'SOXX', 'SOXL']",
    );
    let mut text = std::fs::read_to_string(&path).unwrap();
    text.push_str(
            "[model]\nbase_url='$AKZIO_TEST_MODEL_URL'\nmodel='fixture-model'\napi_key='$AKZIO_TEST_MODEL_KEY'\n",
        );
    std::fs::write(&path, text).unwrap();
    std::env::set_var("AKZIO_TEST_MODEL_URL", "http://fixture/v1");
    std::env::set_var("AKZIO_TEST_MODEL_KEY", "fixture-key");

    let model = load_config(&path).unwrap().model.unwrap();

    assert_eq!(model.base_url, "http://fixture/v1");
    assert_eq!(model.api_key, "fixture-key");
    std::env::remove_var("AKZIO_TEST_MODEL_URL");
    std::env::remove_var("AKZIO_TEST_MODEL_KEY");
}

#[test]
fn control_client_refuses_non_loopback_address() {
    assert!(
        ControlApiClient::new("0.0.0.0:1".parse().unwrap(), "fixture-token".to_owned()).is_err()
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
        let body = r#"{"status":"ok","frozen":false,"scheduler_owner":null,"scheduler_epoch":null,"metrics":{"run_counts":{},"task_counts":{},"attempt_counts":{},"event_count":0,"active_daemon_leases":0},"alerts":[]}"#;
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
