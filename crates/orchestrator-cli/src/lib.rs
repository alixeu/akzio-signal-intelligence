pub mod cli_config;
pub mod exec;
pub mod jin10;
pub mod report;
pub mod social;
pub mod sql_cli;
pub mod technical;
pub mod wayinvideo;
pub mod youtube;

pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}
