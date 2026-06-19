use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use orchestrator_core::parse_tickers;
use orchestrator_sql::{
    connect, handle_read_command, write_agent_message_scoped, AgentMessageInput, RuntimeContext,
};
use serde_json::{json, Value};
use std::{env, fs, io::Read, path::PathBuf};

#[derive(Debug, Clone, Args)]
pub struct SqlArgs {
    #[command(subcommand)]
    pub command: SqlCommand,
}

#[derive(Debug, Clone, Subcommand)]
pub enum SqlCommand {
    GetAnalystReports(ReadArgs),
    GetDebateHistory(ReadArgs),
    GetOpponentLast(ReadArgs),
    GetTopics(ReadArgs),
    GetTopicFinalsAll(ReadArgs),
    GetRunInputs(ReadArgs),
    GetTechnicalContext(ReadArgs),
    GetJin10Context(ReadArgs),
    GetPreviousTopics(ReadArgs),
    GetMediatorReviews(ReadArgs),
    GetResearchInputs(ReadArgs),
    GetTopicBrief(ReadArgs),
    GetLiveThread(ReadArgs),
    GetUnreadEvents(ReadArgs),
    GetLatestCheckpoint(ReadArgs),
    GetTopicFinals(ReadArgs),
    PutAgentMessage(Box<PutAgentMessageArgs>),
}

#[derive(Debug, Clone, Args)]
pub struct ReadArgs {
    #[arg(long, default_value = "")]
    pub topic_id: String,
}

#[derive(Debug, Clone, Args)]
pub struct PutAgentMessageArgs {
    #[arg(long)]
    pub db_path: Option<PathBuf>,
    #[arg(long, default_value = "")]
    pub run_id: String,
    #[arg(long)]
    pub phase: Option<i64>,
    #[arg(long, default_value = "")]
    pub role: String,
    #[arg(long, default_value = "")]
    pub ticker: String,
    #[arg(long, default_value = "")]
    pub tickers: String,
    #[arg(long, default_value = "")]
    pub skill: String,
    #[arg(long, default_value = "artifact")]
    pub kind: String,
    #[arg(long, default_value = "")]
    pub topic_id: String,
    #[arg(long)]
    pub round: Option<i64>,
    #[arg(long, default_value = "")]
    pub message_group_id: String,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub valid: bool,
    #[arg(long, default_value = "")]
    pub last_md: String,
    #[arg(long)]
    pub last_md_file: Option<PathBuf>,
    #[arg(long)]
    pub json_file: Option<PathBuf>,
    #[arg(long, default_value = "")]
    pub json: String,
}

pub fn run(args: SqlArgs) -> Result<Value> {
    match args.command {
        SqlCommand::PutAgentMessage(args) => put_agent_message(*args),
        command => read_command(command),
    }
}

fn read_command(command: SqlCommand) -> Result<Value> {
    let (name, read_args) = read_command_name_args(command);
    let ctx = runtime_context()?;
    let conn = connect(&ctx.db_path)?;
    handle_read_command(
        &conn,
        name,
        &ctx,
        if read_args.topic_id.trim().is_empty() {
            None
        } else {
            Some(read_args.topic_id.as_str())
        },
    )
}

fn read_command_name_args(command: SqlCommand) -> (&'static str, ReadArgs) {
    match command {
        SqlCommand::GetAnalystReports(args) => ("get-analyst-reports", args),
        SqlCommand::GetDebateHistory(args) => ("get-debate-history", args),
        SqlCommand::GetOpponentLast(args) => ("get-opponent-last", args),
        SqlCommand::GetTopics(args) => ("get-topics", args),
        SqlCommand::GetTopicFinalsAll(args) => ("get-topic-finals-all", args),
        SqlCommand::GetRunInputs(args) => ("get-run-inputs", args),
        SqlCommand::GetTechnicalContext(args) => ("get-technical-context", args),
        SqlCommand::GetJin10Context(args) => ("get-jin10-context", args),
        SqlCommand::GetPreviousTopics(args) => ("get-previous-topics", args),
        SqlCommand::GetMediatorReviews(args) => ("get-mediator-reviews", args),
        SqlCommand::GetResearchInputs(args) => ("get-research-inputs", args),
        SqlCommand::GetTopicBrief(args) => ("get-topic-brief", args),
        SqlCommand::GetLiveThread(args) => ("get-live-thread", args),
        SqlCommand::GetUnreadEvents(args) => ("get-unread-events", args),
        SqlCommand::GetLatestCheckpoint(args) => ("get-latest-checkpoint", args),
        SqlCommand::GetTopicFinals(args) => ("get-topic-finals", args),
        SqlCommand::PutAgentMessage(_) => unreachable!(),
    }
}

fn runtime_context() -> Result<RuntimeContext> {
    let db_path = env_required("ORCH_DB_PATH")?;
    let run_id = env_required("ORCH_RUN_ID")?;
    let ticker = env::var("ORCH_TICKER").unwrap_or_default();
    let tickers = parse_tickers(env::var("ORCH_TICKERS").unwrap_or_else(|_| ticker.clone()));
    let phase = env::var("ORCH_PHASE")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let role = env::var("ORCH_ROLE").unwrap_or_default();
    Ok(RuntimeContext {
        db_path: PathBuf::from(db_path),
        run_id,
        ticker,
        tickers,
        phase,
        role,
    })
}

fn env_required(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("{name} is required"))?;
    if value.trim().is_empty() {
        anyhow::bail!("{name} is required");
    }
    Ok(value)
}

fn put_agent_message(args: PutAgentMessageArgs) -> Result<Value> {
    let db_path = args
        .db_path
        .or_else(|| env::var("ORCH_DB_PATH").ok().map(PathBuf::from))
        .context("--db-path or ORCH_DB_PATH is required")?;
    let mut conn = connect(&db_path)?;
    let run_id = if args.run_id.is_empty() {
        env_required("ORCH_RUN_ID")?
    } else {
        args.run_id
    };
    let phase = args
        .phase
        .or_else(|| {
            env::var("ORCH_PHASE")
                .ok()
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or(0);
    let role = if args.role.is_empty() {
        env::var("ORCH_ROLE").unwrap_or_default()
    } else {
        args.role
    };
    let ticker = if args.ticker.is_empty() {
        env::var("ORCH_TICKER").unwrap_or_default()
    } else {
        args.ticker
    };
    let tickers = parse_tickers(if args.tickers.is_empty() {
        env::var("ORCH_TICKERS").unwrap_or_else(|_| ticker.clone())
    } else {
        args.tickers
    });
    let content = read_content(args.json_file.as_ref(), &args.json)?;
    let last_md = if let Some(path) = args.last_md_file {
        fs::read_to_string(path)?
    } else {
        args.last_md
    };
    let written = write_agent_message_scoped(
        &mut conn,
        &AgentMessageInput {
            run_id,
            phase,
            role: role.clone(),
            ticker,
            tickers,
            skill: if args.skill.is_empty() {
                role
            } else {
                args.skill
            },
            kind: args.kind,
            topic_id: (!args.topic_id.is_empty()).then_some(args.topic_id),
            round: args.round,
            message_group_id: (!args.message_group_id.is_empty()).then_some(args.message_group_id),
            valid: args.valid,
            content,
            last_md,
        },
    )?;
    Ok(json!({"ok": true, "command": "put-agent-message", "written_rows": written}))
}

fn read_content(json_file: Option<&PathBuf>, inline_json: &str) -> Result<Value> {
    if let Some(path) = json_file {
        return Ok(serde_json::from_str(&fs::read_to_string(path)?)?);
    }
    if !inline_json.trim().is_empty() {
        return Ok(serde_json::from_str(inline_json)?);
    }
    let mut text = String::new();
    std::io::stdin().read_to_string(&mut text)?;
    Ok(serde_json::from_str(&text)?)
}
