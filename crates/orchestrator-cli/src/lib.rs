pub mod cli_config;
pub mod sql_cli;

pub use orchestrator_ingest::{jin10, social, technical, wayinvideo, youtube};
pub use orchestrator_report::report;
pub use orchestrator_workflow::exec;

pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}
