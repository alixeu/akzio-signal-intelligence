pub mod artifact;
pub mod config;
pub mod paths;
pub mod prompt;
pub mod ticker;

pub use artifact::{
    extract_json_artifact, normalize_probability, validate_research_artifact, ArtifactEnvelope,
    ResearchArtifact, ValidationError,
};
pub use config::{
    config_bool, config_float, config_get, config_int, config_str, config_strings, deep_merge,
    load_config,
};
pub use paths::{default_project_root, project_path};
pub use prompt::replace_placeholders;
pub use ticker::{display_ticker, parse_tickers, run_slug, slug_ticker};
