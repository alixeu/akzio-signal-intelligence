pub use orchestrator_ingest::{jin10, technical};
pub use orchestrator_workflow::exec;
pub use orchestrator_workflow::report::report;

pub fn init_tracing() {
    let _ = orchestrator_core::load_project_env();
    init_tracing_with_debug(false);
}

pub fn init_tracing_with_debug(debug: bool) {
    let default_filter =
        "orchestrator_cli=debug,orchestrator_workflow=debug,orchestrator_llm=debug,orchestrator_llm::http=debug,info";
    let configured_filter = std::env::var("RUST_LOG").ok();
    let filter = if debug {
        // `--debug` must remain useful even when the shell exports a coarse
        // `RUST_LOG=warn`; append the debug targets instead of letting that
        // environment value hide the request/response diagnostics.
        configured_filter
            .filter(|value| !value.trim().is_empty())
            .map(|value| format!("{value},{default_filter}"))
            .unwrap_or_else(|| default_filter.to_owned())
    } else {
        configured_filter.unwrap_or_else(|| "info".to_owned())
    };
    let fallback_filter = if debug { default_filter } else { "info" };
    let filter = tracing_subscriber::EnvFilter::try_new(filter)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(fallback_filter));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}
