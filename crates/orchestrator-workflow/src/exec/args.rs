use clap::{Args, ValueEnum};
use std::path::PathBuf;

#[derive(Debug, Clone, ValueEnum)]
pub enum Mode {
    Probability,
    Monitor,
}

impl Mode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Mode::Probability => "probability",
            Mode::Monitor => "monitor",
        }
    }
}

#[derive(Debug, Clone, Args)]
pub struct ExecArgs {
    #[arg(long)]
    pub date: Option<String>,
    #[arg(long, default_value = "zh")]
    pub lang: String,
    #[arg(long, value_enum, default_value_t = Mode::Probability)]
    pub mode: Mode,
    #[arg(long)]
    pub window_days: Option<i64>,
    /// Canonical FileStore root for all run metadata and artifacts.
    #[arg(long, value_name = "PATH")]
    pub store_root: Option<PathBuf>,
    #[arg(long)]
    pub config: Option<PathBuf>,
    #[arg(long)]
    pub model: Option<String>,
    #[arg(long)]
    pub reasoning_effort: Option<String>,
    #[arg(long)]
    pub max_debate_rounds: Option<i64>,
    #[arg(long)]
    pub max_topics_per_side: Option<i64>,
    #[arg(long, default_value_t = 0)]
    pub from_phase: i64,
    #[arg(long, default_value_t = 8)]
    pub to_phase: i64,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub tech_refresh_enabled: bool,
    #[arg(long, default_value_t = 24.0)]
    pub jin10_refresh_lookback_hours: f64,
    #[arg(long)]
    pub mock: bool,
    /// Write inspectable LLM and local reducer records below outputs/debug/.
    #[arg(long)]
    pub debug: bool,
}
