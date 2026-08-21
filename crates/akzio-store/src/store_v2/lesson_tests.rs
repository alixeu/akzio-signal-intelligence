use super::*;
use std::collections::BTreeSet;

use akzio_domain::{ArtifactProvenance, Asset, ContentHash, DecisionHorizon, LessonScope};
use chrono::Utc;
use tempfile::tempdir;

fn source(store: &V2Store, now: DateTime<Utc>) -> Artifact {
    Artifact::new(
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
    .unwrap()
}

fn lesson(source: &Artifact, now: DateTime<Utc>) -> Lesson {
    Lesson {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        lesson_id: LessonId::new(),
        origin: LessonOrigin::Operator,
        lifecycle: LessonLifecycle::Draft,
        title: "Opening volatility".to_owned(),
        statement: "High opening volatility weakens the signal.".to_owned(),
        rationale: "The first quote window is noisy.".to_owned(),
        recommended_behavior: "Require stronger evidence before acting.".to_owned(),
        exclusions: vec![],
        scope: LessonScope {
            assets: BTreeSet::from([Asset::Tqqq]),
            horizons: BTreeSet::from([DecisionHorizon::T1]),
            ..LessonScope::default()
        },
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
    }
}

#[test]
fn lesson_write_is_idempotent_and_lifecycle_is_immutable_history() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let source = source(&store, now);
    let lesson = lesson(&source, now);

    let first = store.write_lesson(&lesson, &source, now).unwrap();
    assert!(first.newly_created);
    let duplicate = store.write_lesson(&lesson, &source, now).unwrap();
    assert!(!duplicate.newly_created);
    assert_eq!(duplicate.lesson.revision, 1);

    let active = store
        .transition_lesson(
            &lesson.lesson_id,
            LessonLifecycle::Active,
            "operator:reviewer",
            "approved for governed context",
            now + chrono::Duration::seconds(1),
        )
        .unwrap();
    assert_eq!(active.lesson.lifecycle, LessonLifecycle::Active);
    assert_eq!(active.revision, 2);
    assert_eq!(
        store
            .lessons(Some(LessonLifecycle::Active), 10)
            .unwrap()
            .len(),
        1
    );

    let retired = store
        .transition_lesson(
            &lesson.lesson_id,
            LessonLifecycle::Retired,
            "operator:reviewer",
            "superseded by a newer rule",
            now + chrono::Duration::seconds(2),
        )
        .unwrap();
    assert_eq!(retired.lesson.lifecycle, LessonLifecycle::Retired);
    assert_eq!(retired.revision, 3);
    assert_eq!(retired.lesson.supersedes.len(), 2);
    store.verify_integrity().unwrap();
}

#[test]
fn missing_source_closure_is_rejected() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let source = source(&store, now);
    let mut lesson = lesson(&source, now);
    lesson.source_refs[0].artifact_id = ArtifactId(ContentHash::of_bytes(b"missing"));
    assert!(matches!(
        store.write_lesson(&lesson, &source, now),
        Err(StoreError::InvalidLearningCommit("lesson.source_refs"))
    ));
}
