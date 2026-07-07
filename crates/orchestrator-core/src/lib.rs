pub mod artifact;
pub mod config;
pub mod paths;
pub mod plugin_manifest;
pub mod prompt;
pub mod role_registry;
pub mod ticker;
pub mod token;

pub use artifact::{
    analyst_artifact_schema, extract_json_artifact, final_validation_schema, normalize_probability,
    portfolio_allocation_schema, research_artifact_schema, risk_constraints_schema, schema_for,
    trade_intent_schema, validate_research_artifact, AnalystTickerArtifact, FinalValidation,
    PortfolioAllocation, ResearchArtifact, RiskConstraints, TradeIntent, ValidationError,
};
pub use config::{
    config_bool, config_float, config_get, config_int, config_str, config_strings, deep_merge,
    expand_env_placeholders, load_config,
};
pub use paths::{default_project_root, project_path};
pub use prompt::replace_placeholders;
pub use role_registry::{AgentDefinition, AgentRegistry, DEFAULT_PHASE1_AGENTS};
pub use ticker::{display_ticker, parse_tickers, run_slug, slug_ticker};
