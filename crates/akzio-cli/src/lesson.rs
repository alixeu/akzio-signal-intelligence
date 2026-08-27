use std::{
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
};

use super::{Config, ControlApiClient};
use akzio_domain::LessonLifecycle;
use anyhow::{bail, Context, Result};
use clap::Subcommand;
use serde::{Deserialize, Serialize};

#[derive(Debug, Subcommand)]
pub(crate) enum LessonCommand {
    Add {
        #[arg(long, default_value = "-")]
        file: PathBuf,
    },
    List {
        #[arg(long)]
        lifecycle: Option<String>,
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    Show {
        lesson_id: String,
    },
    Usage {
        lesson_id: String,
    },
    Approve {
        lesson_id: String,
        #[arg(long)]
        actor: String,
        #[arg(long)]
        reason: String,
    },
    Contest {
        lesson_id: String,
        #[arg(long)]
        actor: String,
        #[arg(long)]
        reason: String,
    },
    Retire {
        lesson_id: String,
        #[arg(long)]
        actor: String,
        #[arg(long)]
        reason: String,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LessonInput {
    lesson_id: Option<String>,
    title: String,
    statement: String,
    rationale: String,
    recommended_behavior: String,
    #[serde(default)]
    exclusions: Vec<String>,
    #[serde(default)]
    assets: Vec<String>,
    #[serde(default)]
    horizons: Vec<String>,
    #[serde(default)]
    regimes: Vec<String>,
    #[serde(default)]
    decision_stages: Vec<String>,
    #[serde(default)]
    supersedes: Vec<String>,
    #[serde(default)]
    conflicts_with: Vec<String>,
    #[serde(default = "default_confidence")]
    confidence_ppm: u32,
    authored_by: String,
}

fn default_confidence() -> u32 {
    500_000
}

pub(crate) async fn run(config: &Config, command: LessonCommand) -> Result<()> {
    let client = ControlApiClient::from_config(config)?;
    match command {
        LessonCommand::Add { file } => add(&client, &file).await,
        LessonCommand::List { lifecycle, limit } => {
            list(&client, lifecycle.as_deref(), limit).await
        }
        LessonCommand::Show { lesson_id } => show(&client, &lesson_id).await,
        LessonCommand::Usage { lesson_id } => usage(&client, &lesson_id).await,
        LessonCommand::Approve {
            lesson_id,
            actor,
            reason,
        } => {
            transition(
                &client,
                &lesson_id,
                LessonLifecycle::Active,
                &actor,
                &reason,
            )
            .await
        }
        LessonCommand::Contest {
            lesson_id,
            actor,
            reason,
        } => {
            transition(
                &client,
                &lesson_id,
                LessonLifecycle::Contested,
                &actor,
                &reason,
            )
            .await
        }
        LessonCommand::Retire {
            lesson_id,
            actor,
            reason,
        } => {
            transition(
                &client,
                &lesson_id,
                LessonLifecycle::Retired,
                &actor,
                &reason,
            )
            .await
        }
    }
}

async fn add(client: &ControlApiClient, file: &Path) -> Result<()> {
    let input: LessonInput = serde_json::from_str(&read_input(file)?)
        .with_context(|| format!("parse Lesson input {}", file.display()))?;
    if input.authored_by.trim().is_empty() {
        bail!("authored_by must not be empty");
    }
    let payload = serde_json::to_value(input)?;
    crate::print_json(&client.lesson_add(&payload).await?)
}

async fn list(client: &ControlApiClient, lifecycle: Option<&str>, limit: usize) -> Result<()> {
    if let Some(lifecycle) = lifecycle {
        parse_lifecycle(lifecycle)?;
    }
    crate::print_json(&client.lesson_list(lifecycle, limit).await?)
}

async fn show(client: &ControlApiClient, lesson_id: &str) -> Result<()> {
    crate::print_json(&client.lesson_show(lesson_id).await?)
}

async fn usage(client: &ControlApiClient, lesson_id: &str) -> Result<()> {
    crate::print_json(&client.lesson_usage(lesson_id).await?)
}

async fn transition(
    client: &ControlApiClient,
    lesson_id: &str,
    lifecycle: LessonLifecycle,
    actor: &str,
    reason: &str,
) -> Result<()> {
    crate::print_json(
        &client
            .lesson_transition(lesson_id, lifecycle, actor, reason)
            .await?,
    )
}

fn parse_lifecycle(value: &str) -> Result<LessonLifecycle> {
    serde_json::from_value(serde_json::Value::String(value.to_ascii_lowercase()))
        .with_context(|| format!("unsupported Lesson lifecycle {value}"))
}

fn read_input(path: &Path) -> Result<String> {
    if path.as_os_str() == "-" {
        let mut input = String::new();
        io::stdin().read_to_string(&mut input)?;
        Ok(input)
    } else {
        Ok(fs::read_to_string(path)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_parser_is_case_insensitive() {
        assert_eq!(parse_lifecycle("ACTIVE").unwrap(), LessonLifecycle::Active);
    }
}
