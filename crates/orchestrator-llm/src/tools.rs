use anyhow::{bail, Context, Result};
use rig_core::{
    completion::ToolDefinition,
    tool::{Tool, ToolError},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};
use tokio::process::Command;
use tracing::{debug, warn};

use crate::agent_loop::ToolRuntimeTurnContext;
use crate::web_search::{
    SearchQuery, SearchResult, WebSearchContextSize, WebSearchMode, WebSearchOptions,
};
pub use crate::web_search::{WebSearchConfig, WebSearchProvider};

pub const WEB_RUN_TOOL_NAME: &str = "web.run";
const WEB_RUN_MAX_SEARCH_QUERIES: usize = 4;
const WEB_RUN_MAX_QUERY_CHARS: usize = 512;
pub const READ_RUN_CONTEXT_TOOL_NAME: &str = "read_run_context";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExternalToolConfig {
    pub project_root: PathBuf,
    pub db_path: Option<PathBuf>,
    pub run_dir: Option<PathBuf>,
    #[serde(default)]
    pub run_id: Option<String>,
    pub tickers: Vec<String>,
}

impl ExternalToolConfig {
    fn command_spec(&self, bin_name: &str) -> CommandSpec {
        CommandSpec {
            program: "cargo".to_string(),
            args: vec![
                "run".to_string(),
                "-q".to_string(),
                "-p".to_string(),
                "orchestrator-cli".to_string(),
                "--bin".to_string(),
                bin_name.to_string(),
                "--".to_string(),
            ],
        }
    }
}

pub type SharedWebSearchProvider = Arc<dyn WebSearchProvider>;

#[derive(Clone)]
pub struct WebRunRuntime {
    config: WebSearchConfig,
    provider: Option<SharedWebSearchProvider>,
}

impl WebRunRuntime {
    pub fn new(config: WebSearchConfig) -> Self {
        Self {
            config,
            provider: None,
        }
    }

    pub fn with_provider(mut self, provider: SharedWebSearchProvider) -> Self {
        self.provider = Some(provider);
        self
    }

    pub fn config(&self) -> &WebSearchConfig {
        &self.config
    }

    pub async fn execute(&self, args: Value) -> Result<Value> {
        execute_web_run(args, &self.config, self.provider.as_deref()).await
    }
}

#[derive(Debug, Clone, Serialize)]
struct CommandSpec {
    program: String,
    args: Vec<String>,
}

impl CommandSpec {
    fn command(&self, cwd: &Path) -> Command {
        let mut cmd = Command::new(&self.program);
        cmd.current_dir(cwd).args(&self.args);
        cmd
    }

    fn push_arg(&mut self, value: impl Into<String>) {
        self.args.push(value.into());
    }

    fn push_path(&mut self, value: PathBuf) {
        self.args.push(value.to_string_lossy().to_string());
    }
}

#[derive(Debug, Clone)]
pub struct FetchJin10FlashTool {
    config: ExternalToolConfig,
}

impl FetchJin10FlashTool {
    pub fn new(config: ExternalToolConfig) -> Self {
        Self { config }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct FetchJin10FlashArgs {
    #[serde(default)]
    pub lookback_hours: Option<f64>,
    #[serde(default)]
    pub pages: Option<usize>,
    #[serde(default)]
    pub classify: Option<String>,
    #[serde(default)]
    pub output: Option<String>,
}

impl rig_core::tool::Tool for FetchJin10FlashTool {
    const NAME: &'static str = "fetch_jin10_flash";
    type Error = ToolError;
    type Args = FetchJin10FlashArgs;
    type Output = Value;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Fetch recent Jin10 flash news and return structured JSON.".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "lookback_hours": {"type": "number"},
                    "pages": {"type": "integer"},
                    "classify": {"type": "string"},
                    "output": {"type": "string"}
                }
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        run_json_command(
            fetch_jin10_flash_spec(&self.config, &args).command(&self.config.project_root),
            Self::NAME,
        )
        .await
        .map_err(tool_error)
    }
}

#[derive(Debug, Clone)]
pub struct FetchYoutubeTranscriptTool {
    config: ExternalToolConfig,
}

impl FetchYoutubeTranscriptTool {
    pub fn new(config: ExternalToolConfig) -> Self {
        Self { config }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct FetchYoutubeTranscriptArgs {
    #[serde(default)]
    pub all: bool,
    #[serde(default)]
    pub channel: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub max_videos: Option<usize>,
    #[serde(default)]
    pub output: Option<String>,
}

impl rig_core::tool::Tool for FetchYoutubeTranscriptTool {
    const NAME: &'static str = "fetch_youtube_transcript";
    type Error = ToolError;
    type Args = FetchYoutubeTranscriptArgs;
    type Output = Value;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Fetch YouTube channel/video candidates and transcript metadata as JSON."
                .to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "all": {"type": "boolean"},
                    "channel": {"type": "string"},
                    "url": {"type": "string"},
                    "max_videos": {"type": "integer"},
                    "output": {"type": "string"}
                }
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        run_json_command(
            fetch_youtube_transcript_spec(&self.config, &args).command(&self.config.project_root),
            Self::NAME,
        )
        .await
        .map_err(tool_error)
    }
}

#[derive(Debug, Clone)]
pub struct FetchWayinVideoTranscriptTool {
    config: ExternalToolConfig,
}

impl FetchWayinVideoTranscriptTool {
    pub fn new(config: ExternalToolConfig) -> Self {
        Self { config }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct FetchWayinVideoTranscriptArgs {
    pub url: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub published: Option<String>,
    #[serde(default)]
    pub task: Option<String>,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub output: Option<String>,
}

impl rig_core::tool::Tool for FetchWayinVideoTranscriptTool {
    const NAME: &'static str = "fetch_wayinvideo_transcript";
    type Error = ToolError;
    type Args = FetchWayinVideoTranscriptArgs;
    type Output = Value;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Fetch a WayinVideo transcript for a YouTube URL or existing task id."
                .to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "url": {"type": "string"},
                    "title": {"type": "string"},
                    "published": {"type": "string"},
                    "task": {"type": "string"},
                    "task_id": {"type": "string"},
                    "output": {"type": "string"}
                },
                "required": ["url"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        run_json_command(
            fetch_wayinvideo_transcript_spec(&self.config, &args)
                .command(&self.config.project_root),
            Self::NAME,
        )
        .await
        .map_err(tool_error)
    }
}

#[derive(Debug, Clone)]
pub struct ReadRunContextTool {
    config: ExternalToolConfig,
}

impl ReadRunContextTool {
    pub fn new(config: ExternalToolConfig) -> Self {
        Self { config }
    }
}

impl rig_core::tool::Tool for ReadRunContextTool {
    const NAME: &'static str = READ_RUN_CONTEXT_TOOL_NAME;
    type Error = ToolError;
    type Args = Value;
    type Output = Value;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description:
                "Read scoped current-run context from SQLite through structured named slices."
                    .to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "kind": {
                        "type": "string",
                        "enum": [
                            "context_packet",
                            "compose_context",
                            "analyst_reports",
                            "debate_history",
                            "research_inputs",
                            "topic_state",
                            "mediator_reviews",
                            "technical",
                            "technical_daily",
                            "technical_3h",
                            "technical_20min",
                            "role_summaries",
                            "turn_context",
                            "jin10"
                        ]
                    },
                    "run_id": {"type": "string"},
                    "ticker": {"type": "string"},
                    "tickers": {"type": "array", "items": {"type": "string"}},
                    "phase": {"type": "integer"},
                    "role": {"type": "string"},
                    "topic_id": {"type": "string"},
                    "turn_id": {"type": "string"},
                    "token_budget": {"type": "integer"}
                },
                "required": ["kind"],
                "additionalProperties": false
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        execute_read_run_context(args, &self.config, None).map_err(tool_error)
    }
}

#[derive(Debug, Clone)]
pub struct RunTechnicalIndicatorsTool {
    config: ExternalToolConfig,
}

impl RunTechnicalIndicatorsTool {
    pub fn new(config: ExternalToolConfig) -> Self {
        Self { config }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RunTechnicalIndicatorsArgs {
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub intervals: Vec<String>,
    #[serde(default)]
    pub start: Option<String>,
    #[serde(default)]
    pub end: Option<String>,
    #[serde(default)]
    pub days: Option<i64>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub db_path: Option<String>,
}

impl rig_core::tool::Tool for RunTechnicalIndicatorsTool {
    const NAME: &'static str = "run_technical_indicators";
    type Error = ToolError;
    type Args = RunTechnicalIndicatorsArgs;
    type Output = Value;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Download Twelve Data bars, compute local technical indicators, and import them into technical tables.".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "symbols": {"type": "array", "items": {"type": "string"}},
                    "intervals": {"type": "array", "items": {"type": "string"}},
                    "start": {"type": "string"},
                    "end": {"type": "string"},
                    "days": {"type": "integer"},
                    "model": {"type": "string"},
                    "db_path": {"type": "string"}
                }
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        run_json_command(
            run_technical_indicators_spec(&self.config, &args).command(&self.config.project_root),
            Self::NAME,
        )
        .await
        .map_err(tool_error)
    }
}

#[derive(Debug, Clone)]
pub struct FetchLast30DaysContextTool {
    config: ExternalToolConfig,
}

impl FetchLast30DaysContextTool {
    pub fn new(config: ExternalToolConfig) -> Self {
        Self { config }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct FetchLast30DaysContextArgs {
    #[serde(default)]
    pub ticker: Option<String>,
    #[serde(default)]
    pub tickers: Vec<String>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub days: Option<i64>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub depth: Option<String>,
}

impl rig_core::tool::Tool for FetchLast30DaysContextTool {
    const NAME: &'static str = "fetch_last30days_context";
    type Error = ToolError;
    type Args = FetchLast30DaysContextArgs;
    type Output = Value;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Fetch recent Reddit, X, or YouTube social context for ticker analysis."
                .to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "tickers": {"type": "array", "items": {"type": "string"}},
                    "source": {"type": "string", "enum": ["reddit", "x", "youtube"]},
                    "days": {"type": "integer"},
                    "limit": {"type": "integer"},
                    "depth": {"type": "string", "enum": ["quick", "default", "deep"]}
                },
                "required": ["source"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        let tickers = last30days_tickers(&args);
        run_last30days_context(
            &self.config,
            normalize_last30days_source(args.source.as_deref()),
            tickers,
            args.days,
            args.limit,
            args.depth.as_deref(),
            Self::NAME,
        )
        .await
        .map_err(tool_error)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebRunInput {
    #[serde(default, deserialize_with = "deserialize_web_run_search_query")]
    pub search_query: Vec<WebRunSearchQueryInput>,
    #[serde(default)]
    pub query: Option<String>,
    #[serde(default)]
    pub include_domains: Vec<String>,
    #[serde(default)]
    pub open: Vec<WebRunOpenInput>,
    #[serde(default)]
    pub find: Vec<WebRunFindInput>,
    #[serde(default)]
    pub response_length: WebRunResponseLength,
    #[serde(default)]
    pub num_results: Option<usize>,
    #[serde(default)]
    pub source: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebRunSearchQueryInput {
    pub q: String,
    #[serde(default)]
    pub recency: Option<u32>,
    #[serde(default)]
    pub domains: Vec<String>,
    #[serde(default, alias = "include_domains")]
    pub include_domains: Vec<String>,
    #[serde(default, alias = "numResults")]
    pub num_results: Option<usize>,
    #[serde(default, alias = "type")]
    pub search_type: Option<String>,
    #[serde(default)]
    pub livecrawl: Option<String>,
    #[serde(default, alias = "contextMaxCharacters")]
    pub context_max_characters: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebRunOpenInput {
    pub ref_id: String,
    #[serde(default)]
    pub lineno: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebRunFindInput {
    pub ref_id: String,
    pub pattern: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WebRunResponseLength {
    Short,
    #[default]
    Medium,
    Long,
}

#[derive(Clone)]
pub struct WebRunTool {
    config: WebSearchConfig,
    provider: Option<SharedWebSearchProvider>,
}

impl WebRunTool {
    pub fn new(config: WebSearchConfig) -> Self {
        Self {
            config,
            provider: None,
        }
    }

    pub fn with_provider(mut self, provider: SharedWebSearchProvider) -> Self {
        self.provider = Some(provider);
        self
    }
}

impl rig_core::tool::Tool for WebRunTool {
    const NAME: &'static str = WEB_RUN_TOOL_NAME;
    type Error = ToolError;
    type Args = WebRunInput;
    type Output = Value;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        web_run_tool_definition()
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        let args = serde_json::to_value(args).map_err(|error| {
            ToolError::ToolCallError(Box::new(std::io::Error::other(error.to_string())))
        })?;
        execute_web_run(args, &self.config, self.provider.as_deref())
            .await
            .map_err(tool_error)
    }
}

async fn run_last30days_context(
    config: &ExternalToolConfig,
    source: Option<&str>,
    tickers: Vec<String>,
    days: Option<i64>,
    limit: Option<usize>,
    depth: Option<&str>,
    tool_name: &str,
) -> Result<Value> {
    run_json_command(
        last30days_context_spec(config, source, tickers, days, limit, depth)
            .command(&config.project_root),
        tool_name,
    )
    .await
}

fn fetch_jin10_flash_spec(config: &ExternalToolConfig, args: &FetchJin10FlashArgs) -> CommandSpec {
    let mut spec = config.command_spec("fetch-jin10-flash");
    if let Some(value) = args.lookback_hours {
        spec.push_arg("--lookback-hours");
        spec.push_arg(value.to_string());
    }
    if let Some(value) = args.pages {
        spec.push_arg("--pages");
        spec.push_arg(value.to_string());
    }
    if let Some(value) = args.classify.as_deref().filter(|value| !value.is_empty()) {
        spec.push_arg("--classify");
        spec.push_arg(value);
    }
    if let Some(value) = args.output.as_deref().filter(|value| !value.is_empty()) {
        spec.push_arg("--output");
        spec.push_arg(value);
    }
    spec
}

fn fetch_youtube_transcript_spec(
    config: &ExternalToolConfig,
    args: &FetchYoutubeTranscriptArgs,
) -> CommandSpec {
    let mut spec = config.command_spec("fetch-youtube-transcript");
    if args.all {
        spec.push_arg("--all");
    }
    if let Some(value) = args.channel.as_deref().filter(|value| !value.is_empty()) {
        spec.push_arg("--channel");
        spec.push_arg(value);
    }
    if let Some(value) = args.url.as_deref().filter(|value| !value.is_empty()) {
        spec.push_arg("--url");
        spec.push_arg(value);
    }
    if let Some(value) = args.max_videos {
        spec.push_arg("--max-videos");
        spec.push_arg(value.to_string());
    }
    if let Some(value) = args.output.as_deref().filter(|value| !value.is_empty()) {
        spec.push_arg("--output");
        spec.push_arg(value);
    }
    spec
}

fn fetch_wayinvideo_transcript_spec(
    config: &ExternalToolConfig,
    args: &FetchWayinVideoTranscriptArgs,
) -> CommandSpec {
    let mut spec = config.command_spec("fetch-wayinvideo-transcript");
    spec.push_arg("--url");
    spec.push_arg(&args.url);
    if let Some(value) = args.title.as_deref().filter(|value| !value.is_empty()) {
        spec.push_arg("--title");
        spec.push_arg(value);
    }
    if let Some(value) = args.published.as_deref().filter(|value| !value.is_empty()) {
        spec.push_arg("--published");
        spec.push_arg(value);
    }
    if let Some(value) = args.task.as_deref().filter(|value| !value.is_empty()) {
        spec.push_arg("--task");
        spec.push_arg(value);
    }
    if let Some(value) = args.task_id.as_deref().filter(|value| !value.is_empty()) {
        spec.push_arg("--task-id");
        spec.push_arg(value);
    }
    if let Some(value) = args.output.as_deref().filter(|value| !value.is_empty()) {
        spec.push_arg("--output");
        spec.push_arg(value);
    }
    spec
}

fn run_technical_indicators_spec(
    config: &ExternalToolConfig,
    args: &RunTechnicalIndicatorsArgs,
) -> CommandSpec {
    let mut spec = config.command_spec("run-technical-indicators");
    let symbols = if args.symbols.is_empty() {
        config.tickers.clone()
    } else {
        args.symbols.clone()
    };
    if !symbols.is_empty() {
        spec.push_arg("--symbols");
        spec.push_arg(symbols.join(","));
    }
    let db_path = args
        .db_path
        .as_ref()
        .map(PathBuf::from)
        .or_else(|| config.db_path.clone());
    if let Some(path) = db_path {
        spec.push_arg("--db-path");
        spec.push_path(path);
    }
    if !args.intervals.is_empty() {
        spec.push_arg("--intervals");
        spec.push_arg(args.intervals.join(","));
    }
    if let Some(value) = args.start.as_deref().filter(|value| !value.is_empty()) {
        spec.push_arg("--start");
        spec.push_arg(value);
    }
    if let Some(value) = args.end.as_deref().filter(|value| !value.is_empty()) {
        spec.push_arg("--end");
        spec.push_arg(value);
    }
    if let Some(value) = args.days {
        spec.push_arg("--days");
        spec.push_arg(value.to_string());
    }
    if let Some(value) = args.model.as_deref().filter(|value| !value.is_empty()) {
        spec.push_arg("--model");
        spec.push_arg(value);
    }
    spec
}

fn last30days_context_spec(
    config: &ExternalToolConfig,
    source: Option<&str>,
    tickers: Vec<String>,
    days: Option<i64>,
    limit: Option<usize>,
    depth: Option<&str>,
) -> CommandSpec {
    let mut spec = config.command_spec("fetch-last30days-context");
    let tickers = if tickers.is_empty() {
        config.tickers.clone()
    } else {
        tickers
    };
    if !tickers.is_empty() {
        spec.push_arg("--tickers");
        spec.push_arg(tickers.join(","));
    }
    if let Some(value) = source.filter(|value| !value.is_empty()) {
        spec.push_arg("--source");
        spec.push_arg(value);
    }
    if let Some(value) = days {
        spec.push_arg("--days");
        spec.push_arg(value.to_string());
    }
    if let Some(value) = limit {
        spec.push_arg("--limit");
        spec.push_arg(value.to_string());
    }
    if let Some(value) = depth.filter(|value| !value.is_empty()) {
        spec.push_arg("--depth");
        spec.push_arg(value);
    }
    spec
}

async fn run_json_command(mut cmd: Command, tool_name: &str) -> Result<Value> {
    let started_at = Instant::now();
    debug!(tool = tool_name, "external json command starting");
    let output = cmd
        .output()
        .await
        .with_context(|| format!("{tool_name} command failed to start"))?;
    let elapsed_ms = started_at.elapsed().as_millis();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !output.status.success() {
        warn!(
            tool = tool_name,
            elapsed_ms,
            exit_code = output.status.code(),
            stderr_chars = stderr.len(),
            stdout_chars = stdout.len(),
            "external json command failed"
        );
        return Ok(json!({
            "status": "error",
            "tool": tool_name,
            "exit_code": output.status.code(),
            "message": stderr,
            "stdout": stdout
        }));
    }
    if stdout.is_empty() {
        debug!(
            tool = tool_name,
            elapsed_ms, "external json command completed without stdout"
        );
        return Ok(json!({"status": "success", "tool": tool_name}));
    }
    debug!(
        tool = tool_name,
        elapsed_ms,
        stdout_chars = stdout.len(),
        stderr_chars = stderr.len(),
        "external json command completed"
    );
    serde_json::from_str(&stdout).with_context(|| {
        format!(
            "{tool_name} returned non-JSON stdout: {}",
            stdout.chars().take(240).collect::<String>()
        )
    })
}

fn tool_error(error: anyhow::Error) -> ToolError {
    ToolError::ToolCallError(Box::new(std::io::Error::other(error.to_string())))
}

pub fn web_run_tool_definition() -> ToolDefinition {
    ToolDefinition {
        name: WEB_RUN_TOOL_NAME.to_string(),
        description: "Search the web and return source links for current information.".to_string(),
        parameters: web_run_tool_parameters(),
    }
}

pub fn web_run_tool_parameters() -> Value {
    json!({
        "type": "object",
        "properties": {
            "search_query": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "q": {"type": "string"},
                        "recency": {"type": "integer"},
                        "domains": {"type": "array", "items": {"type": "string"}},
                        "numResults": {"type": "integer", "minimum": 1, "maximum": 20},
                        "type": {"type": "string", "enum": ["auto", "fast", "deep"]},
                        "livecrawl": {"type": "string", "enum": ["fallback", "preferred"]},
                        "contextMaxCharacters": {"type": "integer", "minimum": 1, "maximum": 50000}
                    },
                    "required": ["q"]
                }
            },
            "open": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "ref_id": {"type": "string"},
                        "lineno": {"type": "integer"}
                    },
                    "required": ["ref_id"]
                }
            },
            "find": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "ref_id": {"type": "string"},
                        "pattern": {"type": "string"}
                    },
                    "required": ["ref_id", "pattern"]
                }
            },
            "response_length": {"type": "string", "enum": ["short", "medium", "long"]}
        }
    })
}

async fn execute_web_run(
    args: Value,
    config: &WebSearchConfig,
    provider: Option<&dyn WebSearchProvider>,
) -> Result<Value> {
    debug!(
        mode = ?config.mode,
        provider = ?config.provider,
        "web.run starting"
    );
    if config.mode == WebSearchMode::Disabled {
        debug!("web.run disabled");
        return Ok(safe_web_run_error("Web search is disabled."));
    }

    let args = match serde_json::from_value::<WebRunInput>(args) {
        Ok(args) => args,
        Err(_) => return Ok(safe_web_run_error("Invalid web.run arguments.")),
    };
    let args = normalize_web_run_input(args);
    if !args.open.is_empty() {
        return Ok(safe_web_run_error("web.run open is not implemented in v1."));
    }
    if !args.find.is_empty() {
        return Ok(safe_web_run_error("web.run find is not implemented in v1."));
    }
    if args.search_query.is_empty() {
        return Ok(safe_web_run_error("web.run requires search_query for v1."));
    }
    if args.search_query.len() > WEB_RUN_MAX_SEARCH_QUERIES {
        return Ok(safe_web_run_error(&format!(
            "web.run search_query supports at most {WEB_RUN_MAX_SEARCH_QUERIES} queries."
        )));
    }

    let Some(provider) = provider else {
        warn!("web.run provider missing");
        return Ok(safe_web_run_error("web.run provider is not configured."));
    };
    let response_length = args.response_length;
    let queries = match validated_web_search_queries(args) {
        Ok(queries) => queries,
        Err(error) => return Ok(safe_web_run_error(&error.to_string())),
    };
    if let Err(error) = validate_query_domain_policy(&queries, config) {
        return Ok(safe_web_run_error(&error.to_string()));
    }

    let search_queries = queries
        .iter()
        .map(|query| SearchQuery {
            q: query.query.clone(),
            recency: query.recency,
            domains: query.domains.clone(),
            num_results: query.num_results,
            search_type: query.search_type.clone(),
            livecrawl: query.livecrawl.clone(),
            context_max_characters: query.context_max_characters,
        })
        .collect::<Vec<_>>();
    debug!(
        query_count = search_queries.len(),
        response_length = ?response_length,
        "web.run provider search starting"
    );
    let options = WebSearchOptions {
        context_size: response_length_context_size(response_length),
        allowed_domains: config.allowed_domains.clone(),
        blocked_domains: config.blocked_domains.clone(),
        max_result_chars: config.max_result_chars,
    };
    let results = match provider.search(search_queries, options).await {
        Ok(results) => results,
        Err(error) => {
            warn!(error = %error, "web.run provider failed");
            return Ok(safe_web_run_error("web.run provider failed."));
        }
    };
    let results = filter_web_search_results(results, config);
    debug!(result_count = results.len(), "web.run completed");
    let text = truncate_chars(
        &format_web_search_results(&results),
        config.max_result_chars,
    );
    Ok(json!({
        "status": "success",
        "tool": WEB_RUN_TOOL_NAME,
        "content": text,
        "text": text,
        "results": results_to_json(&results),
    }))
}

fn safe_web_run_error(message: &str) -> Value {
    json!({
        "status": "error",
        "tool": WEB_RUN_TOOL_NAME,
        "content": message,
        "text": message,
    })
}

fn response_length_context_size(response_length: WebRunResponseLength) -> WebSearchContextSize {
    match response_length {
        WebRunResponseLength::Short => WebSearchContextSize::Low,
        WebRunResponseLength::Medium => WebSearchContextSize::Medium,
        WebRunResponseLength::Long => WebSearchContextSize::High,
    }
}

fn last30days_tickers(args: &FetchLast30DaysContextArgs) -> Vec<String> {
    if !args.tickers.is_empty() {
        return args.tickers.clone();
    }
    args.ticker
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| vec![value.to_string()])
        .unwrap_or_default()
}

fn normalize_last30days_source(source: Option<&str>) -> Option<&str> {
    source.map(|value| match value.trim() {
        "twitter" | "x_twitter" | "x-twitter" => "x",
        other => other,
    })
}

fn deserialize_web_run_search_query<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<WebRunSearchQueryInput>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::String(q) => Ok(vec![WebRunSearchQueryInput {
            q,
            recency: None,
            domains: Vec::new(),
            include_domains: Vec::new(),
            num_results: None,
            search_type: None,
            livecrawl: None,
            context_max_characters: None,
        }]),
        Value::Array(_) => serde_json::from_value(value).map_err(serde::de::Error::custom),
        Value::Null => Ok(Vec::new()),
        other => Err(serde::de::Error::custom(format!(
            "expected string or array for search_query, got {other}"
        ))),
    }
}

fn normalize_web_run_input(mut args: WebRunInput) -> WebRunInput {
    if args.search_query.is_empty() {
        if let Some(query) = args
            .query
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            args.search_query.push(WebRunSearchQueryInput {
                q: query.to_string(),
                recency: None,
                domains: Vec::new(),
                include_domains: Vec::new(),
                num_results: args.num_results,
                search_type: None,
                livecrawl: None,
                context_max_characters: None,
            });
        }
    }
    if let Some(num_results) = args.num_results {
        for query in &mut args.search_query {
            if query.num_results.is_none() {
                query.num_results = Some(num_results);
            }
        }
    }
    if !args.include_domains.is_empty() {
        for query in &mut args.search_query {
            query.domains.extend(args.include_domains.clone());
        }
    }
    args
}

fn validated_web_search_queries(args: WebRunInput) -> Result<Vec<SearchQueryRequest>> {
    args.search_query
        .into_iter()
        .enumerate()
        .map(|(index, query)| {
            let q = query.q.trim();
            if q.is_empty() {
                bail!("web.run search_query[{index}].q is required");
            }
            if q.chars().count() > WEB_RUN_MAX_QUERY_CHARS {
                bail!("web.run search_query[{index}].q exceeds {WEB_RUN_MAX_QUERY_CHARS} chars");
            }
            let domains = query
                .domains
                .into_iter()
                .chain(query.include_domains)
                .filter_map(|domain| normalize_domain(&domain))
                .collect::<Vec<_>>();
            if let Some(num_results) = query.num_results {
                if !(1..=20).contains(&num_results) {
                    bail!("web.run search_query[{index}].numResults must be between 1 and 20");
                }
            }
            if let Some(search_type) = query.search_type.as_deref() {
                if !matches!(search_type, "auto" | "fast" | "deep") {
                    bail!("web.run search_query[{index}].type must be auto, fast, or deep");
                }
            }
            if let Some(livecrawl) = query.livecrawl.as_deref() {
                if !matches!(livecrawl, "fallback" | "preferred") {
                    bail!("web.run search_query[{index}].livecrawl must be fallback or preferred");
                }
            }
            if let Some(context_max_characters) = query.context_max_characters {
                if !(1..=50_000).contains(&context_max_characters) {
                    bail!(
                        "web.run search_query[{index}].contextMaxCharacters must be between 1 and 50000"
                    );
                }
            }
            Ok(SearchQueryRequest {
                query: q.to_string(),
                recency: query.recency,
                domains,
                response_length: args.response_length,
                num_results: query.num_results,
                search_type: query.search_type,
                livecrawl: query.livecrawl,
                context_max_characters: query.context_max_characters,
            })
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SearchQueryRequest {
    query: String,
    recency: Option<u32>,
    domains: Vec<String>,
    response_length: WebRunResponseLength,
    num_results: Option<usize>,
    search_type: Option<String>,
    livecrawl: Option<String>,
    context_max_characters: Option<usize>,
}

fn validate_query_domain_policy(
    queries: &[SearchQueryRequest],
    config: &WebSearchConfig,
) -> Result<()> {
    for query in queries {
        for domain in &query.domains {
            if domain_matches_any(domain, &config.blocked_domains) {
                bail!("web.run query domain is blocked: {domain}");
            }
            if !config.allowed_domains.is_empty()
                && !domain_matches_any(domain, &config.allowed_domains)
            {
                bail!("web.run query domain is not allowed: {domain}");
            }
        }
    }
    Ok(())
}

fn filter_web_search_results(
    results: Vec<SearchResult>,
    config: &WebSearchConfig,
) -> Vec<SearchResult> {
    results
        .into_iter()
        .filter_map(|result| {
            let host = http_url_host(&result.url)?;
            if domain_matches_any(&host, &config.blocked_domains) {
                return None;
            }
            if !config.allowed_domains.is_empty()
                && !domain_matches_any(&host, &config.allowed_domains)
            {
                return None;
            }
            Some(SearchResult {
                ref_id: result.ref_id,
                title: clean_text_field(&result.title),
                url: sanitize_http_url(&result.url),
                published_at: result.published_at.map(|value| clean_text_field(&value)),
                snippet: clean_text_field(&result.snippet),
                source: result.source.map(|value| clean_text_field(&value)),
            })
        })
        .collect()
}

fn format_web_search_results(results: &[SearchResult]) -> String {
    let mut output = String::from("Search results:\n");
    if results.is_empty() {
        output.push_str("No results after filtering.\n");
    } else {
        for (index, result) in results.iter().enumerate() {
            if index > 0 {
                output.push('\n');
            }
            output.push_str(&format!("[ref_id: search{index}]\n"));
            output.push_str(&format!("Title: {}\n", result.title));
            output.push_str(&format!("URL: {}\n", result.url));
            output.push_str(&format!(
                "Published: {}\n",
                result.published_at.as_deref().unwrap_or("unknown")
            ));
            output.push_str(&format!("Snippet: {}\n", result.snippet));
        }
    }
    output.push_str("Use open with ref_id to inspect a result if needed.");
    output
}

fn results_to_json(results: &[SearchResult]) -> Value {
    Value::Array(
        results
            .iter()
            .enumerate()
            .map(|(index, result)| {
                json!({
                    "ref_id": if result.ref_id.is_empty() { format!("search{index}") } else { result.ref_id.clone() },
                    "title": result.title.clone(),
                    "url": result.url.clone(),
                    "published": result.published_at.clone(),
                    "snippet": result.snippet.clone(),
                })
            })
            .collect(),
    )
}

fn http_url_host(value: &str) -> Option<String> {
    let value = value.trim();
    let lower = value.to_ascii_lowercase();
    let rest = if lower.starts_with("https://") {
        &value[8..]
    } else if lower.starts_with("http://") {
        &value[7..]
    } else {
        return None;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let host_port = authority.rsplit('@').next().unwrap_or(authority);
    let host = if let Some(rest) = host_port.strip_prefix('[') {
        rest.split(']').next().unwrap_or_default()
    } else {
        host_port.split(':').next().unwrap_or_default()
    };
    normalize_domain(host)
}

fn normalize_domain(value: &str) -> Option<String> {
    let value = value
        .trim()
        .trim_start_matches("*.")
        .trim_end_matches('.')
        .to_ascii_lowercase();
    let value = value
        .strip_prefix("https://")
        .or_else(|| value.strip_prefix("http://"))
        .unwrap_or(&value)
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default()
        .split(':')
        .next()
        .unwrap_or_default()
        .trim()
        .trim_end_matches('.');
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn domain_matches_any(host: &str, domains: &[String]) -> bool {
    domains
        .iter()
        .filter_map(|domain| normalize_domain(domain))
        .any(|domain| domain_matches(host, &domain))
}

fn domain_matches(host: &str, domain: &str) -> bool {
    host == domain || host.ends_with(&format!(".{domain}"))
}

fn sanitize_http_url(value: &str) -> String {
    let value = value.trim();
    let end = value.find(['?', '#']).unwrap_or(value.len());
    value[..end].to_string()
}

fn clean_text_field(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string()
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    if max_chars == 0 {
        return String::new();
    }
    let suffix = "\n[truncated]";
    let suffix_len = suffix.chars().count();
    if max_chars <= suffix_len {
        return value.chars().take(max_chars).collect();
    }
    let mut output = value
        .chars()
        .take(max_chars - suffix_len)
        .collect::<String>();
    output.push_str(suffix);
    output
}

pub fn tool_names() -> &'static [&'static str] {
    &[
        READ_RUN_CONTEXT_TOOL_NAME,
        FetchJin10FlashTool::NAME,
        FetchYoutubeTranscriptTool::NAME,
        FetchWayinVideoTranscriptTool::NAME,
        RunTechnicalIndicatorsTool::NAME,
        FetchLast30DaysContextTool::NAME,
    ]
}

pub fn enabled_tool_names(web_run: Option<&WebSearchConfig>) -> Vec<&'static str> {
    let mut names = tool_names().to_vec();
    if web_run.is_some_and(|config| config.mode != WebSearchMode::Disabled) {
        names.push(WEB_RUN_TOOL_NAME);
    }
    names
}

pub async fn execute_named_tool(
    name: &str,
    args: Value,
    config: &ExternalToolConfig,
    turn_context: Option<&ToolRuntimeTurnContext>,
    web_run: Option<&WebRunRuntime>,
) -> Result<Value> {
    debug!(
        tool = name,
        has_turn_context = turn_context.is_some(),
        "named tool starting"
    );
    match name {
        READ_RUN_CONTEXT_TOOL_NAME => {
            let result = execute_read_run_context(args, config, turn_context);
            log_named_tool_result(name, &result);
            result
        }
        WEB_RUN_TOOL_NAME => {
            if let Some(web_run) = web_run {
                let result = web_run.execute(args).await;
                log_named_tool_result(name, &result);
                result
            } else {
                let result = Ok(safe_web_run_error("Web search is disabled."));
                log_named_tool_result(name, &result);
                result
            }
        }
        FetchJin10FlashTool::NAME => {
            let args = serde_json::from_value::<FetchJin10FlashArgs>(args)
                .context("invalid fetch_jin10_flash arguments")?;
            let result = FetchJin10FlashTool::new(config.clone())
                .call(args)
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()));
            log_named_tool_result(name, &result);
            result
        }
        FetchYoutubeTranscriptTool::NAME => {
            let args = serde_json::from_value::<FetchYoutubeTranscriptArgs>(args)
                .context("invalid fetch_youtube_transcript arguments")?;
            let result = FetchYoutubeTranscriptTool::new(config.clone())
                .call(args)
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()));
            log_named_tool_result(name, &result);
            result
        }
        FetchWayinVideoTranscriptTool::NAME => {
            let args = serde_json::from_value::<FetchWayinVideoTranscriptArgs>(args)
                .context("invalid fetch_wayinvideo_transcript arguments")?;
            let result = FetchWayinVideoTranscriptTool::new(config.clone())
                .call(args)
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()));
            log_named_tool_result(name, &result);
            result
        }
        RunTechnicalIndicatorsTool::NAME => {
            let args = serde_json::from_value::<RunTechnicalIndicatorsArgs>(args)
                .context("invalid run_technical_indicators arguments")?;
            let result = RunTechnicalIndicatorsTool::new(config.clone())
                .call(args)
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()));
            log_named_tool_result(name, &result);
            result
        }
        FetchLast30DaysContextTool::NAME => {
            let args = serde_json::from_value::<FetchLast30DaysContextArgs>(args)
                .context("invalid fetch_last30days_context arguments")?;
            let result = FetchLast30DaysContextTool::new(config.clone())
                .call(args)
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()));
            log_named_tool_result(name, &result);
            result
        }
        other => bail!("unknown tool name: {other}"),
    }
}

fn log_named_tool_result(name: &str, result: &Result<Value>) {
    match result {
        Ok(value) => {
            let status = value
                .get("status")
                .and_then(|value| value.as_str())
                .unwrap_or("ok");
            debug!(
                tool = name,
                status,
                output_chars = value.to_string().len(),
                "named tool completed"
            );
        }
        Err(error) => warn!(tool = name, error = %error, "named tool failed"),
    }
}

fn execute_read_run_context(
    args: Value,
    config: &ExternalToolConfig,
    turn_context: Option<&ToolRuntimeTurnContext>,
) -> Result<Value> {
    let mut request = serde_json::from_value::<orchestrator_sql::RunContextReadRequest>(args)
        .context("invalid read_run_context arguments")?;
    if request.run_id.is_none() {
        request.run_id = turn_context.map(|context| context.run_id.clone());
    }
    if request.role.is_none() {
        request.role = turn_context.map(|context| context.role.clone());
    }
    if request.kind.trim().is_empty() && request.role.as_deref() == Some("analyst.technical") {
        request.kind = "technical".to_string();
    }
    if request.tickers.is_empty() {
        request.tickers = config.tickers.clone();
    }
    let mut conn = tool_connection(config)?;
    orchestrator_sql::read_run_context(&mut conn, &request)
}

fn tool_connection(config: &ExternalToolConfig) -> Result<rusqlite::Connection> {
    orchestrator_sql::connect(runtime_db_path(config)?)
}

fn runtime_db_path(config: &ExternalToolConfig) -> Result<PathBuf> {
    config
        .db_path
        .clone()
        .context("runtime tool requires ExternalToolConfig.db_path")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::web_search::{MockWebPage, MockWebSearchProvider, WebSearchFuture, WebSearchMode};

    fn external_config() -> ExternalToolConfig {
        ExternalToolConfig {
            project_root: PathBuf::from("."),
            db_path: None,
            run_dir: None,
            run_id: None,
            tickers: Vec::new(),
        }
    }

    fn web_run_runtime<P>(config: WebSearchConfig, provider: P) -> WebRunRuntime
    where
        P: WebSearchProvider + 'static,
    {
        WebRunRuntime::new(config).with_provider(Arc::new(provider))
    }

    #[tokio::test]
    async fn read_run_context_uses_structured_sql_adapter() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("runtime.sqlite");
        let config = ExternalToolConfig {
            project_root: PathBuf::from("."),
            db_path: Some(db_path),
            run_dir: None,
            run_id: None,
            tickers: vec!["TQQQ".to_string()],
        };

        let output = execute_named_tool(
            READ_RUN_CONTEXT_TOOL_NAME,
            json!({"kind": "technical"}),
            &config,
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(output["query"], "get-technical-context");
        assert!(output["daily"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn run_technical_indicators_is_dispatchable() {
        let error = execute_named_tool(
            RunTechnicalIndicatorsTool::NAME,
            json!({"days": "bad"}),
            &external_config(),
            None,
            None,
        )
        .await
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("invalid run_technical_indicators arguments"));
    }

    #[tokio::test]
    async fn web_run_disabled_returns_safe_error() {
        let output = execute_named_tool(
            WEB_RUN_TOOL_NAME,
            json!({"search_query": [{"q": "TQQQ"}]}),
            &external_config(),
            None,
            Some(&WebRunRuntime::new(WebSearchConfig::default())),
        )
        .await
        .unwrap();

        assert_eq!(output["status"], "error");
        assert_eq!(output["content"], "Web search is disabled.");
    }

    #[tokio::test]
    async fn web_run_open_is_safe_not_implemented_for_v1() {
        let provider = MockWebSearchProvider::default();
        let config = WebSearchConfig {
            mode: WebSearchMode::Cached,
            ..WebSearchConfig::default()
        };

        let output = execute_named_tool(
            WEB_RUN_TOOL_NAME,
            json!({"open": [{"ref_id": "search0"}]}),
            &external_config(),
            None,
            Some(&web_run_runtime(config, provider)),
        )
        .await
        .unwrap();

        assert_eq!(output["status"], "error");
        assert_eq!(output["content"], "web.run open is not implemented in v1.");
    }

    #[tokio::test]
    async fn web_run_accepts_legacy_agent_search_shape() {
        let provider = MockWebSearchProvider::new(vec![MockWebPage {
            title: "TQQQ Reddit".to_string(),
            url: "https://www.reddit.com/r/TQQQ/comments/1".to_string(),
            content: "QQQ and VIX discussion for TQQQ.".to_string(),
        }]);
        let config = WebSearchConfig {
            mode: WebSearchMode::Cached,
            ..WebSearchConfig::default()
        };

        let output = execute_named_tool(
            WEB_RUN_TOOL_NAME,
            json!({
                "search_query": "TQQQ site:reddit.com QQQ VIX",
                "include_domains": ["reddit.com"],
                "num_results": 10,
                "source": "exa",
                "response_length": "medium"
            }),
            &external_config(),
            None,
            Some(&web_run_runtime(config, provider)),
        )
        .await
        .unwrap();

        assert_eq!(output["status"], "success");
        assert!(output["content"].as_str().unwrap().contains("TQQQ Reddit"));
    }

    #[tokio::test]
    async fn web_run_rejects_too_many_search_queries() {
        let config = WebSearchConfig {
            mode: WebSearchMode::Cached,
            ..WebSearchConfig::default()
        };

        let output = execute_named_tool(
            WEB_RUN_TOOL_NAME,
            json!({
                "search_query": [
                    {"q": "one"},
                    {"q": "two"},
                    {"q": "three"},
                    {"q": "four"},
                    {"q": "five"}
                ]
            }),
            &external_config(),
            None,
            Some(&WebRunRuntime::new(config)),
        )
        .await
        .unwrap();

        assert_eq!(output["status"], "error");
        assert!(output["content"]
            .as_str()
            .unwrap()
            .contains("at most 4 queries"));
    }

    #[tokio::test]
    async fn web_run_rejects_overlong_queries() {
        let provider = MockWebSearchProvider::default();
        let config = WebSearchConfig {
            mode: WebSearchMode::Cached,
            ..WebSearchConfig::default()
        };

        let output = execute_named_tool(
            WEB_RUN_TOOL_NAME,
            json!({"search_query": [{"q": "x".repeat(513)}]}),
            &external_config(),
            None,
            Some(&web_run_runtime(config, provider)),
        )
        .await
        .unwrap();

        assert_eq!(output["status"], "error");
        assert!(output["content"]
            .as_str()
            .unwrap()
            .contains("exceeds 512 chars"));
    }

    #[tokio::test]
    async fn web_run_formats_filters_and_truncates_search_results() {
        let provider = MockWebSearchProvider::new(vec![
            MockWebPage {
                title: "Allowed TQQQ".to_string(),
                url: "https://research.example.com/tqqq?token=secret#section".to_string(),
                content: "TQQQ volatility and liquidity signal with enough detail to truncate."
                    .to_string(),
            },
            MockWebPage {
                title: "Blocked TQQQ".to_string(),
                url: "https://blocked.example.com/tqqq".to_string(),
                content: "TQQQ blocked signal".to_string(),
            },
            MockWebPage {
                title: "Non HTTP TQQQ".to_string(),
                url: "ftp://research.example.com/tqqq".to_string(),
                content: "TQQQ non http signal".to_string(),
            },
        ]);
        let config = WebSearchConfig {
            mode: WebSearchMode::Cached,
            allowed_domains: vec!["example.com".to_string()],
            blocked_domains: vec!["blocked.example.com".to_string()],
            max_result_chars: 220,
            ..WebSearchConfig::default()
        };

        let output = execute_named_tool(
            WEB_RUN_TOOL_NAME,
            json!({"search_query": [{"q": "TQQQ"}], "response_length": "short"}),
            &external_config(),
            None,
            Some(&web_run_runtime(config.clone(), provider.clone())),
        )
        .await
        .unwrap();
        let text = output["text"].as_str().unwrap();

        assert!(text.starts_with("Search results:\n[ref_id: search0]"));
        assert!(text.contains("Title: Allowed TQQQ"));
        assert!(text.contains("URL: https://research.example.com/tqqq"));
        assert!(!text.contains("token=secret"));
        assert!(!text.contains("Blocked TQQQ"));
        assert!(!text.contains("Non HTTP TQQQ"));
        assert_eq!(output["results"].as_array().unwrap().len(), 1);
        assert_eq!(
            output["results"][0]["url"],
            "https://research.example.com/tqqq"
        );

        let truncated_config = WebSearchConfig {
            max_result_chars: 80,
            ..config
        };
        let truncated = execute_named_tool(
            WEB_RUN_TOOL_NAME,
            json!({"search_query": [{"q": "TQQQ"}]}),
            &external_config(),
            None,
            Some(&web_run_runtime(truncated_config, provider)),
        )
        .await
        .unwrap();
        assert!(truncated["text"].as_str().unwrap().ends_with("[truncated]"));
    }

    #[tokio::test]
    async fn web_run_rejects_query_domains_outside_policy() {
        let provider = MockWebSearchProvider::default();
        let config = WebSearchConfig {
            mode: WebSearchMode::Cached,
            allowed_domains: vec!["example.com".to_string()],
            ..WebSearchConfig::default()
        };

        let output = execute_named_tool(
            WEB_RUN_TOOL_NAME,
            json!({"search_query": [{"q": "TQQQ", "domains": ["not-example.com"]}]}),
            &external_config(),
            None,
            Some(&web_run_runtime(config, provider)),
        )
        .await
        .unwrap();

        assert_eq!(output["status"], "error");
        assert!(output["content"]
            .as_str()
            .unwrap()
            .contains("domain is not allowed"));
        assert!(!output.to_string().contains("TAVILY"));
    }

    #[derive(Debug)]
    struct FailingProvider;

    impl WebSearchProvider for FailingProvider {
        fn search<'a>(
            &'a self,
            _queries: Vec<SearchQuery>,
            _options: WebSearchOptions,
        ) -> WebSearchFuture<'a, Vec<SearchResult>> {
            Box::pin(async {
                bail!("provider rejected request with API key sk-secret-do-not-leak")
            })
        }
    }

    #[tokio::test]
    async fn web_run_sanitizes_provider_errors() {
        let config = WebSearchConfig {
            mode: WebSearchMode::Cached,
            ..WebSearchConfig::default()
        };

        let output = execute_named_tool(
            WEB_RUN_TOOL_NAME,
            json!({"search_query": [{"q": "TQQQ"}]}),
            &external_config(),
            None,
            Some(&web_run_runtime(config, FailingProvider)),
        )
        .await
        .unwrap();

        assert_eq!(output["status"], "error");
        assert_eq!(output["content"], "web.run provider failed.");
        assert!(!output.to_string().contains("sk-secret"));
    }
}
