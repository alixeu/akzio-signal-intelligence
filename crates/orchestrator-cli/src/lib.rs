pub mod cli_config;
pub mod eval;
pub mod memory_promote;
pub mod reflection_score;
pub mod sql_cli;
pub mod weekly_distill;

pub use orchestrator_ingest::{jin10, technical};
pub use orchestrator_workflow::exec;
pub use orchestrator_workflow::report::report;

pub fn init_tracing() {
    init_tracing_with_debug(false);
}

pub fn init_tracing_with_debug(debug: bool) {
    let default_filter = if debug {
        "orchestrator_cli=debug,orchestrator_workflow=debug,orchestrator_llm=debug,orchestrator_sql=debug,info"
    } else {
        "info"
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter)),
        )
        .try_init();
}
