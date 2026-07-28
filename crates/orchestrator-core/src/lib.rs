pub mod artifact;
pub mod authority;
pub mod config;
pub mod jin10_csv;
pub mod paths;
pub mod plugin_manifest;
pub mod prompt;
pub mod prompt_plugins;
pub mod reflection;
pub mod technical_csv;
pub mod ticker;
pub mod token;

pub use artifact::{
    normalize_probability, research_rating_for_probability, validate_analyst_ticker_artifact,
    validate_asset_execution_constraint, validate_evidence_types, validate_final_validation,
    validate_research_decision, validate_risk_constraints, validate_trade_intent,
    AnalystTickerArtifact, AssetExecutionConstraint, BindingRiskControl, EvidenceItem,
    EvidenceType, FinalValidation, PortfolioAllocation, ResearchDecision, RiskConstraints,
    StopType, TradeIntent, ValidationError, CANONICAL_EVIDENCE_TYPES,
};
pub use authority::{
    AuthorityRegistration, AuthorityRegistry, AuthorityRegistryError, AuthorityRegistrySnapshot,
    RoleProfileKey, ToolManagedProfile, UnitPlanner, AUTHORITY_REGISTRY_SCHEMA_VERSION,
    BUILTIN_AUTHORITY_REGISTRY_VERSION,
};
pub use config::{
    config_bool, config_float, config_get, config_int, config_str, config_strings, deep_merge,
    expand_env_placeholders, load_config, load_project_env,
};
pub use jin10_csv::{
    default_jin10_csv_dir, jin10_csv_path, jin10_item_id, load_jin10_csv, load_jin10_csv_recent,
    load_jin10_csv_recent_from_dir, parse_jin10_csv, read_jin10_csv, render_jin10_csv, Jin10CsvRow,
    DEFAULT_JIN10_CSV_DIR,
};
pub use paths::{default_project_root, project_path};
pub use prompt::{render_template, replace_placeholders};
pub use prompt_plugins::{
    validate_plugins, ComponentPlugin, ComponentRegistry, KNOWN_RENDER_VARIABLES,
};
pub use reflection::{MarketRegime, Scope};
pub use technical_csv::{
    close_on_or_after, close_on_or_before, closes_for_correlation, default_technical_csv_dir,
    interval_file_label, latest_close, latest_indicator, latest_snapshot, parse_technical_csv,
    read_technical_csv, render_csv_file_blocks, render_technical_csv, storage_interval,
    technical_csv_filename, technical_csv_path, TechnicalCsvRow, DEFAULT_TECHNICAL_BARS,
    DEFAULT_TECHNICAL_CSV_DIR,
};
pub use ticker::{display_ticker, parse_tickers, run_slug, slug_ticker};
pub use token::{cost_usd, pricing_for_model, ModelPricing};
