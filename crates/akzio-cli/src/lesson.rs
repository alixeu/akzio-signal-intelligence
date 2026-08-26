use std::{
    collections::BTreeSet,
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
};

use akzio_domain::{
    Artifact, ArtifactId, ArtifactKind, ArtifactLifecycle, ArtifactProvenance, ArtifactRef, Asset,
    ContentHash, DecisionHorizon, Lesson, LessonId, LessonLifecycle, LessonOrigin, LessonScope,
    V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_store::{v2::StoredLesson, V2Store};
use anyhow::{bail, Context, Result};
use chrono::Utc;
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

#[derive(Debug, Serialize)]
struct LessonView<'a> {
    artifact: &'a Artifact,
    lesson: &'a Lesson,
    revision: u64,
}

fn default_confidence() -> u32 {
    500_000
}

pub(crate) fn run(store_root: &Path, command: LessonCommand) -> Result<()> {
    match command {
        LessonCommand::Add { file } => add(store_root, &file),
        LessonCommand::List { lifecycle, limit } => list(store_root, lifecycle.as_deref(), limit),
        LessonCommand::Show { lesson_id } => show(store_root, &lesson_id),
        LessonCommand::Usage { lesson_id } => usage(store_root, &lesson_id),
        LessonCommand::Approve {
            lesson_id,
            actor,
            reason,
        } => transition(
            store_root,
            &lesson_id,
            LessonLifecycle::Active,
            &actor,
            &reason,
        ),
        LessonCommand::Contest {
            lesson_id,
            actor,
            reason,
        } => transition(
            store_root,
            &lesson_id,
            LessonLifecycle::Contested,
            &actor,
            &reason,
        ),
        LessonCommand::Retire {
            lesson_id,
            actor,
            reason,
        } => transition(
            store_root,
            &lesson_id,
            LessonLifecycle::Retired,
            &actor,
            &reason,
        ),
    }
}

fn add(store_root: &Path, file: &Path) -> Result<()> {
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
        source_refs: vec![ArtifactRef {
            artifact_id: source.artifact_id.clone(),
            kind: source.kind,
        }],
        supersedes: parse_lesson_refs(&input.supersedes)?,
        conflicts_with: parse_lesson_refs(&input.conflicts_with)?,
        confidence_ppm: input.confidence_ppm,
        authored_by: Some(input.authored_by),
        approved_by: None,
        created_at: now,
        updated_at: now,
    };
    let result = store.write_lesson(&lesson, &source, now)?;
    crate::print_json(&view(&result.lesson))
}

fn list(store_root: &Path, lifecycle: Option<&str>, limit: usize) -> Result<()> {
    let lifecycle = lifecycle.map(parse_lifecycle).transpose()?;
    let store = V2Store::open_existing(store_root)?;
    let lessons = store.lessons(lifecycle, limit)?;
    crate::print_json(&lessons.iter().map(view).collect::<Vec<_>>())
}

fn show(store_root: &Path, lesson_id: &str) -> Result<()> {
    let store = V2Store::open_existing(store_root)?;
    let lesson = store
        .lesson(&LessonId(lesson_id.to_owned()))?
        .with_context(|| format!("lesson {lesson_id} not found"))?;
    crate::print_json(&view(&lesson))
}

fn usage(store_root: &Path, lesson_id: &str) -> Result<()> {
    let store = V2Store::open_existing(store_root)?;
    crate::print_json(&store.lesson_usage(&LessonId(lesson_id.to_owned()))?)
}

fn transition(
    store_root: &Path,
    lesson_id: &str,
    lifecycle: LessonLifecycle,
    actor: &str,
    reason: &str,
) -> Result<()> {
    let store = V2Store::open(store_root)?;
    let lesson = store.transition_lesson(
        &LessonId(lesson_id.to_owned()),
        lifecycle,
        actor,
        reason,
        Utc::now(),
    )?;
    crate::print_json(&view(&lesson))
}

fn parse_horizon(value: &str) -> Result<DecisionHorizon> {
    match value.trim().to_ascii_lowercase().as_str() {
        "t1" => Ok(DecisionHorizon::T1),
        "t3" => Ok(DecisionHorizon::T3),
        "t5" => Ok(DecisionHorizon::T5),
        other => bail!("unsupported horizon {other}; expected t1, t3 or t5"),
    }
}

fn parse_lesson_refs(values: &[String]) -> Result<Vec<ArtifactRef>> {
    values
        .iter()
        .map(|value| {
            Ok(ArtifactRef {
                artifact_id: ArtifactId(ContentHash::new(value.trim())?),
                kind: ArtifactKind::Lesson,
            })
        })
        .collect()
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

fn view(value: &StoredLesson) -> LessonView<'_> {
    LessonView {
        artifact: &value.artifact,
        lesson: &value.lesson,
        revision: value.revision,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use akzio_domain::{ArtifactProvenance, LessonScope};
    use tempfile::tempdir;

    #[test]
    fn transition_command_reopens_the_store_for_writes() {
        let root = tempdir().unwrap();
        let root_path = root.path().to_path_buf();
        let store = V2Store::open(&root_path).unwrap();
        let now = Utc::now();
        let source = Artifact::new(
            ArtifactKind::SemanticDetail,
            store
                .put_json(&serde_json::json!({"note": "operator source"}))
                .unwrap(),
            "operator.lesson.source",
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: "akzio.operator".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            None,
            vec![],
            now,
        )
        .unwrap();
        let lesson = Lesson {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            lesson_id: LessonId::new(),
            origin: LessonOrigin::Operator,
            lifecycle: LessonLifecycle::Draft,
            title: "Opening volatility".to_owned(),
            statement: "Require stronger evidence at the open.".to_owned(),
            rationale: "The first quote window is noisy.".to_owned(),
            recommended_behavior: "Wait for confirmation.".to_owned(),
            exclusions: vec![],
            scope: LessonScope::default(),
            source_refs: vec![ArtifactRef {
                artifact_id: source.artifact_id.clone(),
                kind: source.kind,
            }],
            supersedes: vec![],
            conflicts_with: vec![],
            confidence_ppm: 700_000,
            authored_by: Some("operator:test".to_owned()),
            approved_by: None,
            created_at: now,
            updated_at: now,
        };
        let lesson_id = lesson.lesson_id.0.clone();
        store.write_lesson(&lesson, &source, now).unwrap();

        transition(
            &root_path,
            &lesson_id,
            LessonLifecycle::Active,
            "operator:reviewer",
            "approved",
        )
        .unwrap();

        let stored = V2Store::open_existing(&root_path)
            .unwrap()
            .lesson(&LessonId(lesson_id))
            .unwrap()
            .unwrap();
        assert_eq!(stored.lesson.lifecycle, LessonLifecycle::Active);
    }
}
