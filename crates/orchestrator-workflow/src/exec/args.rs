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
    #[arg(long)]
    pub phase1_agents: Option<String>,
    #[arg(long)]
    pub db_path: Option<PathBuf>,
    /// Optional debug dump directory for state.json / final_summary.md / end_context.
    /// Omitted by default; run state is persisted to SQLite only.
    #[arg(long)]
    pub run_dir: Option<PathBuf>,
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
    #[arg(long)]
    pub technical_weight: Option<f64>,
    #[arg(long)]
    pub news_weight: Option<f64>,
    #[arg(long)]
    pub youtube_weight: Option<f64>,
    #[arg(long)]
    pub reddit_weight: Option<f64>,
    #[arg(long)]
    pub x_weight: Option<f64>,
    #[arg(long, default_value_t = 1)]
    pub from_phase: i64,
    #[arg(long, default_value_t = 8)]
    pub to_phase: i64,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub tech_refresh_enabled: bool,
    #[arg(long, default_value = "1d,3h,20min")]
    pub tech_refresh_intervals: String,
    #[arg(long, default_value_t = 120)]
    pub tech_refresh_save_bars: i64,
    #[arg(long)]
    pub tech_refresh_script_path: Option<PathBuf>,
    #[arg(long, default_value_t = 900)]
    pub tech_refresh_timeout_sec: u64,
    #[arg(long)]
    pub tech_refresh_python_bin: Option<PathBuf>,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub jin10_refresh_enabled: bool,
    #[arg(long, default_value_t = 24.0)]
    pub jin10_refresh_lookback_hours: f64,
    #[arg(long)]
    pub jin10_refresh_script_path: Option<PathBuf>,
    #[arg(long, default_value_t = 120)]
    pub jin10_refresh_timeout_sec: u64,
    #[arg(long)]
    pub mock: bool,
    /// Write LLM/local reducer records to outputs/debug/phaseXX/{role}.jsonl.
    #[arg(long)]
    pub debug: bool,
}
