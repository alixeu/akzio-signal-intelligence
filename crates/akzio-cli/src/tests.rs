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

fn write_auto_paper_config(
    directory: &tempfile::TempDir,
    auto_paper: bool,
    execution: &str,
) -> PathBuf {
    let path = directory.path().join("akzio.toml");
    std::fs::write(
        &path,
        format!(
            "[daemon]\nstore_root='store'\nauto_paper={auto_paper}\nhttp_addr='127.0.0.1:1'\ntoken_env='TOKEN'\n[execution]\nassets=['TQQQ', 'QQQ', 'SOXX', 'SOXL']\n{execution}"
        ),
    )
    .unwrap();
    path
}

#[test]
fn runtime_identity_components_cover_split_modules() {
    let prompt = prompt_component_hash();
    let contract = contract_component_hash();
    let topology = topology_component_hash();
    assert_ne!(prompt, contract);
    assert_ne!(prompt, topology);
    assert_ne!(contract, topology);
    assert!(source_revision().unwrap().contains('+'));
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
fn config_accepts_worker_only_without_paper_requirements() {
    let directory = tempfile::tempdir().unwrap();
    let path = write_auto_paper_config(&directory, false, "");

    assert!(load_config(&path).is_ok());
}

#[test]
fn config_rejects_zero_cost_auto_paper() {
    let directory = tempfile::tempdir().unwrap();
    let path = write_auto_paper_config(&directory, true, "");

    let error = load_config(&path).unwrap_err().to_string();
    assert!(error.contains("transaction_cost_ppm or slippage_ppm"));
}

#[test]
fn config_rejects_auto_paper_without_market_data_feed() {
    let directory = tempfile::tempdir().unwrap();
    let path =
        write_auto_paper_config(&directory, true, "transaction_cost_ppm=1\nslippage_ppm=1\n");

    let error = load_config(&path).unwrap_err().to_string();
    assert!(error.contains("execution.market_data_feed"));
}

#[test]
fn config_accepts_complete_auto_paper_requirements() {
    let directory = tempfile::tempdir().unwrap();
    let path = write_auto_paper_config(
        &directory,
        true,
        "market_data_feed='iex'\ntransaction_cost_ppm=1\nslippage_ppm=1\n",
    );

    assert!(load_config(&path).is_ok());
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
            "[model]\nprovider='openai_responses'\nbase_url='http://fixture/v1'\nmodel='fixture-model'\napi_key='fixture-key'\nreasoning_effort='high'\ndebug=true\n",
        );
    std::fs::write(&path, text).unwrap();

    let model = load_config(&path).unwrap().model.unwrap();
    assert_eq!(model.base_url, "http://fixture/v1");
    assert_eq!(model.model, "fixture-model");
    assert_eq!(model.reasoning_effort, "high");
    assert!(model.debug);
}

#[test]
fn provider_config_rejects_unknown_and_ambiguous_legacy_endpoints() {
    let directory = tempfile::tempdir().unwrap();
    let path = write_config(
        &directory,
        "http_addr='127.0.0.1:1'",
        "['TQQQ', 'QQQ', 'SOXX', 'SOXL']",
    );
    let base = std::fs::read_to_string(&path).unwrap();

    std::fs::write(
        &path,
        format!(
            "{base}[model]\nprovider='anthropic_messages'\nbase_url='https://api.anthropic.com/v1'\nmodel='fixture-model'\napi_key='fixture-key'\n"
        ),
    )
    .unwrap();
    assert!(load_config(&path).is_err());

    std::fs::write(
        &path,
        format!(
            "{base}[model]\nbase_url='https://gateway.example.invalid/v1'\nmodel='fixture-model'\napi_key='fixture-key'\n"
        ),
    )
    .unwrap();
    let error = load_config(&path).unwrap_err();
    assert!(format!("{error:#}").contains("legacy model config"));

    std::fs::write(
        &path,
        format!(
            "{base}[model]\nbase_url='https://api.openai.com/v1'\nmodel='fixture-model'\napi_key='fixture-key'\n"
        ),
    )
    .unwrap();
    let legacy = load_config(&path).unwrap().model.unwrap();
    assert_eq!(legacy.provider_identity().as_str(), "openai_responses");
}

#[test]
fn observatory_configuration_and_credentials_live_in_private_toml() {
    let directory = tempfile::tempdir().unwrap();
    let template = write_config(
        &directory,
        "http_addr='127.0.0.1:7342'",
        "['TQQQ', 'QQQ', 'SOXX', 'SOXL']",
    );
    let mut text = std::fs::read_to_string(&template).unwrap();
    text.push_str(
        "[model]\nprovider='openai_responses'\nbase_url='$LLM_GATEWAY_BASE_URL'\nmodel='fixture-model'\napi_key='$LLM_GATEWAY_API_KEY'\nreasoning_effort='low'\nresponse_language='简体中文'\n",
    );
    std::fs::write(&template, text).unwrap();

    let home = directory.path().join(".akzio");
    let config_path = home.join("config.toml");
    let store_root = home.join("store");
    handle_observatory_config(
        &config_path,
        &ObservatoryConfigCommand::Init {
            template,
            store_root: store_root.clone(),
        },
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&config_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    let configuration = ObservatoryEditableConfiguration {
        llm_base_url: "https://llm.example/v1".to_owned(),
        llm_api_key: "fixture-llm-key".to_owned(),
        global_model: "gpt-fixture".to_owned(),
        global_reasoning_effort: "high".to_owned(),
        global_response_language: "简体中文".to_owned(),
        stage_models: BTreeMap::from([(
            "research.critic".to_owned(),
            akzio_model::OpenAIResponsesRouteConfig {
                model: "critic-fixture".to_owned(),
                reasoning_effort: "medium".to_owned(),
                response_language: None,
            },
        )]),
        alpaca_api_key: "fixture-alpaca-key".to_owned(),
        alpaca_api_secret: "fixture-alpaca-secret".to_owned(),
        fred_api_key: Some("fixture-fred-key".to_owned()),
        sec_user_agent: Some("Akzio test@example.com".to_owned()),
    };
    update_observatory_configuration(&config_path, configuration).unwrap();

    let saved = read_config_file(&config_path).unwrap();
    let model = saved.model.unwrap();
    assert_eq!(saved.daemon.store_root, store_root);
    assert_eq!(model.base_url, "https://llm.example/v1");
    assert_eq!(model.api_key, "fixture-llm-key");
    assert_eq!(model.routes["research.critic"].model, "critic-fixture");
    assert_eq!(
        saved.observatory.sec_user_agent.as_deref(),
        Some("Akzio test@example.com")
    );
    let rendered = std::fs::read_to_string(config_path).unwrap();
    assert!(rendered.contains("provider = \"openai_responses\""));
    assert_eq!(saved.credentials.alpaca_api_key, "fixture-alpaca-key");
    assert_eq!(saved.credentials.alpaca_api_secret, "fixture-alpaca-secret");
    assert!(rendered.contains("fixture-llm-key"));
}

#[test]
fn config_rejects_unknown_or_empty_model_routes() {
    let directory = tempfile::tempdir().unwrap();
    let path = write_config(
        &directory,
        "http_addr='127.0.0.1:1'",
        "['TQQQ', 'QQQ', 'SOXX', 'SOXL']",
    );
    let mut text = std::fs::read_to_string(&path).unwrap();
    text.push_str(
        "[model]\nprovider='openai_responses'\nbase_url='http://fixture/v1'\nmodel='fixture-model'\napi_key='fixture-key'\n\
         [model.routes.'research.unknown']\nmodel='route-model'\nreasoning_effort='low'\n",
    );
    std::fs::write(&path, text).unwrap();

    let error = load_config(&path).unwrap_err().to_string();
    assert!(error.contains("unsupported model route research.unknown"));

    let mut text = std::fs::read_to_string(&path).unwrap();
    text = text.replace("research.unknown", "research.critic");
    text = text.replace("model='route-model'", "model=''");
    std::fs::write(&path, text).unwrap();
    let error = load_config(&path).unwrap_err().to_string();
    assert!(error.contains("model route research.critic contains an empty value"));
}

#[test]
fn runtime_identity_binds_effective_model_routes() {
    let directory = tempfile::tempdir().unwrap();
    let path = write_config(
        &directory,
        "http_addr='127.0.0.1:1'",
        "['TQQQ', 'QQQ', 'SOXX', 'SOXL']",
    );
    let mut text = std::fs::read_to_string(&path).unwrap();
    text.push_str(
        "[model]\nprovider='openai_responses'\nbase_url='http://fixture/v1'\nmodel='fixture-model'\napi_key='fixture-key'\n",
    );
    std::fs::write(&path, text).unwrap();

    let mut config = load_config(&path).unwrap();
    config.execution.market_data_feed = Some(AlpacaMarketDataFeed::Iex);
    let baseline = runtime_identity_from_config(&config, &path).unwrap();
    config.model.as_mut().unwrap().routes.insert(
        "research.critic".to_owned(),
        akzio_model::OpenAIResponsesRouteConfig {
            model: "critic-model".to_owned(),
            reasoning_effort: "high".to_owned(),
            response_language: Some("简体中文".to_owned()),
        },
    );
    let routed = runtime_identity_from_config(&config, &path).unwrap();

    assert_ne!(baseline.config_hash, routed.config_hash);
}

#[test]
fn runtime_identity_redacts_rotated_credentials() {
    let directory = tempfile::tempdir().unwrap();
    let path = write_config(
        &directory,
        "http_addr='127.0.0.1:1'",
        "['TQQQ', 'QQQ', 'SOXX', 'SOXL']",
    );
    let mut text = std::fs::read_to_string(&path).unwrap();
    text.push_str(
        "[model]\nprovider='openai_responses'\nbase_url='http://fixture/v1'\nmodel='fixture-model'\napi_key='first-key'\n",
    );
    std::fs::write(&path, &text).unwrap();

    let mut first = load_config(&path).unwrap();
    first.execution.market_data_feed = Some(AlpacaMarketDataFeed::Iex);
    let first_identity = runtime_identity_from_config(&first, &path).unwrap();

    std::fs::write(&path, text.replace("first-key", "rotated-key")).unwrap();
    let mut rotated = load_config(&path).unwrap();
    rotated.execution.market_data_feed = Some(AlpacaMarketDataFeed::Iex);
    let rotated_identity = runtime_identity_from_config(&rotated, &path).unwrap();

    assert_eq!(first_identity.config_hash, rotated_identity.config_hash);
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
            "[model]\nprovider='openai_responses'\nbase_url='$AKZIO_TEST_MODEL_URL'\nmodel='fixture-model'\napi_key='$AKZIO_TEST_MODEL_KEY'\n",
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

#[test]
fn release_evidence_cli_supports_view_and_export() {
    let view = Cli::try_parse_from(["akzio", "store", "release-evidence", "run-fixture"]).unwrap();
    assert!(matches!(
        view.command,
        Command::Store {
            command: StoreCommand::ReleaseEvidence {
                ref run_id,
                target: None,
            }
        } if run_id == "run-fixture"
    ));

    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("bundle.json");
    let export = Cli::try_parse_from([
        "akzio",
        "store",
        "release-evidence",
        "run-fixture",
        "--target",
        target.to_str().unwrap(),
    ])
    .unwrap();
    assert!(matches!(
        export.command,
        Command::Store {
            command: StoreCommand::ReleaseEvidence {
                target: Some(_),
                ..
            }
        }
    ));
}

#[test]
fn release_evidence_export_is_deterministic_and_refuses_overwrite() {
    let bundle =
        akzio_domain::ReleaseEvidenceBundle::materialize(akzio_domain::ReleaseEvidenceBody {
            run_id: RunId::new(),
            purpose: RunPurpose::Debug,
            environment: akzio_domain::ReleaseEvidenceEnvironment::OfflineFixture,
            materialized_at: Utc::now(),
            runtime: None,
            workflow: None,
            contracts: akzio_domain::ReleaseContractEvidence::default(),
            provider_routes: Default::default(),
            source_snapshots: Default::default(),
            broker: None,
            session: None,
            daemon: None,
            execution: None,
            outcomes: Default::default(),
            learning: None,
            canary: None,
            human_approval: None,
            integrity: akzio_domain::ReleaseIntegrityEvidence {
                config_hash_matches: false,
                workflow_hash_matches: false,
                broker_account_matches: false,
                daemon_epoch_current: false,
            },
        })
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("release").join("bundle.json");
    export_release_evidence_bundle(&bundle, &target).unwrap();
    let decoded: akzio_domain::ReleaseEvidenceBundle =
        serde_json::from_slice(&std::fs::read(&target).unwrap()).unwrap();
    assert_eq!(decoded.bundle_hash, bundle.bundle_hash);
    assert!(export_release_evidence_bundle(&bundle, &target).is_err());
}
