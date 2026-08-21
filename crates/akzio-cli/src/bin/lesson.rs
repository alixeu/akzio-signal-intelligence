use std::{
    collections::BTreeSet,
    fs,
    io::{self, Read},
    path::PathBuf,
};

use akzio_domain::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactProvenance, Asset, DecisionHorizon, Lesson,
    LessonId, LessonLifecycle, LessonOrigin, LessonScope, V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_store::{v2::StoredLesson, V2Store};
use anyhow::{bail, Context, Result};
use chrono::Utc;
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};

#[derive(Debug, Parser)]
#[command(name = "akzio-lesson", about = "Governed Akzio Lesson editor")]
struct Cli {
    #[arg(long, default_value = "outputs/akzio-v2-rebuild")]
    store_root: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
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
    #[serde(default = "default_confidence")]
    confidence_ppm: u32,
    authored_by: String,
}

#[derive(Debug, Serialize)]
struct LessonView<'a> {
    artifact: &'a Artifact,
    lesson: &'a Lesson,
    revision: u64,
}

fn default_confidence() -> u32 {
    500_000
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Add { file } => add(&cli.store_root, &file),
        Command::List { lifecycle, limit } => list(&cli.store_root, lifecycle.as_deref(), limit),
        Command::Show { lesson_id } => show(&cli.store_root, &lesson_id),
        Command::Approve {
            lesson_id,
            actor,
            reason,
        } => transition(
            &cli.store_root,
            &lesson_id,
            LessonLifecycle::Active,
            &actor,
            &reason,
        ),
        Command::Contest {
            lesson_id,
            actor,
            reason,
        } => transition(
            &cli.store_root,
            &lesson_id,
            LessonLifecycle::Contested,
            &actor,
            &reason,
        ),
        Command::Retire {
            lesson_id,
            actor,
            reason,
        } => transition(
            &cli.store_root,
            &lesson_id,
            LessonLifecycle::Retired,
            &actor,
            &reason,
        ),
    }
}

fn add(store_root: &PathBuf, file: &PathBuf) -> Result<()> {
    let input: LessonInput = serde_json::from_str(&read_input(file)?)
        .with_context(|| format!("parse Lesson input {}", file.display()))?;
    if input.authored_by.trim().is_empty() {
        bail!("authored_by must not be empty");
    }
    let now = Utc::now();
    let store = V2Store::open(store_root)?;
    let source = Artifact::new(
        ArtifactKind::SemanticDetail,
        store.put_json(&input)?,
        "operator.lesson.source",
        ArtifactLifecycle::Canonical,
        ArtifactProvenance {
            source_family: "akzio.operator".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: input.confidence_ppm,
            producer_contract_hash: None,
        },
        None,
        vec![],
        now,
    )?;
    let lesson = Lesson {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        lesson_id: input.lesson_id.map(LessonId).unwrap_or_default(),
        origin: LessonOrigin::Operator,
        lifecycle: LessonLifecycle::Draft,
        title: input.title,
        statement: input.statement,
        rationale: input.rationale,
        recommended_behavior: input.recommended_behavior,
        exclusions: input.exclusions,
        scope: LessonScope {
            assets: input
                .assets
                .iter()
                .map(|value| Asset::try_from(value.as_str()))
                .collect::<std::result::Result<BTreeSet<_>, _>>()?,
            horizons: input
                .horizons
                .iter()
                .map(|value| parse_horizon(value))
                .collect::<Result<BTreeSet<_>>>()?,
            regimes: input.regimes.into_iter().collect(),
            decision_stages: input.decision_stages.into_iter().collect(),
        },
        source_refs: vec![akzio_domain::ArtifactRef {
            artifact_id: source.artifact_id.clone(),
            kind: source.kind,
        }],
        supersedes: vec![],
        conflicts_with: vec![],
        confidence_ppm: input.confidence_ppm,
        authored_by: Some(input.authored_by),
        approved_by: None,
        created_at: now,
        updated_at: now,
    };
    let result = store.write_lesson(&lesson, &source, now)?;
    print_json(&view(&result.lesson))
}

fn list(store_root: &PathBuf, lifecycle: Option<&str>, limit: usize) -> Result<()> {
    let lifecycle = lifecycle.map(parse_lifecycle).transpose()?;
    let store = V2Store::open_existing(store_root)?;
    let lessons = store.lessons(lifecycle, limit)?;
    print_json(&lessons.iter().map(view).collect::<Vec<_>>())
}

fn show(store_root: &PathBuf, lesson_id: &str) -> Result<()> {
    let store = V2Store::open_existing(store_root)?;
    let lesson = store
        .lesson(&LessonId(lesson_id.to_owned()))?
        .with_context(|| format!("lesson {lesson_id} not found"))?;
    print_json(&view(&lesson))
}

fn transition(
    store_root: &PathBuf,
    lesson_id: &str,
    lifecycle: LessonLifecycle,
    actor: &str,
    reason: &str,
) -> Result<()> {
    let store = V2Store::open_existing(store_root)?;
    let lesson = store.transition_lesson(
        &LessonId(lesson_id.to_owned()),
        lifecycle,
        actor,
        reason,
        Utc::now(),
    )?;
    print_json(&view(&lesson))
}

fn parse_horizon(value: &str) -> Result<DecisionHorizon> {
    match value.trim().to_ascii_lowercase().as_str() {
        "t1" => Ok(DecisionHorizon::T1),
        "t3" => Ok(DecisionHorizon::T3),
        "t5" => Ok(DecisionHorizon::T5),
        other => bail!("unsupported horizon {other}; expected t1, t3 or t5"),
    }
}

fn parse_lifecycle(value: &str) -> Result<LessonLifecycle> {
    serde_json::from_value(serde_json::Value::String(value.to_ascii_lowercase()))
        .with_context(|| format!("unsupported Lesson lifecycle {value}"))
}

fn read_input(path: &PathBuf) -> Result<String> {
    if path.as_os_str() == "-" {
        let mut input = String::new();
        io::stdin().read_to_string(&mut input)?;
        Ok(input)
    } else {
        Ok(fs::read_to_string(path)?)
    }
}

fn print_json<T: Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn view(value: &StoredLesson) -> LessonView<'_> {
    LessonView {
        artifact: &value.artifact,
        lesson: &value.lesson,
        revision: value.revision,
    }
}
