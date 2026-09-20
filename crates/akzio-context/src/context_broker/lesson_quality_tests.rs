use super::*;
use akzio_domain::{LessonGovernance, LessonId, LessonOrigin, LessonScope};

// 构造一个可被 Context 召回的最小 Active Lesson；各测试只改动与该断言相关的
// scope、文本或治理字段，不依赖外部 Store 状态。
fn lesson() -> Lesson {
    let now = Utc::now();
    Lesson {
        schema_version: DOMAIN_SCHEMA_VERSION,
        lesson_id: LessonId("quality".into()),
        origin: LessonOrigin::Operator,
        lifecycle: LessonLifecycle::Active,
        title: "quality".into(),
        statement: "Use bounded estimates".into(),
        rationale: "test only".into(),
        recommended_behavior: "Keep uncertainty".into(),
        exclusions: vec![],
        scope: LessonScope::default(),
        source_refs: vec![],
        supersedes: vec![],
        conflicts_with: vec![],
        confidence_ppm: 500000,
        authored_by: Some("test".into()),
        approved_by: Some("test".into()),
        created_at: now,
        updated_at: now,
        governance: Some(LessonGovernance::operator_reviewed(now)),
    }
}
#[test]
fn lesson_scope_filters_before_recency() {
    let mut specific = lesson();
    specific.scope.assets.insert(Asset::Qqq);
    assert!(!specific.scope.matches(
        &BTreeSet::from([Asset::Soxx]),
        &BTreeSet::new(),
        &BTreeSet::new(),
        &BTreeSet::new()
    ));
    assert!(specific.scope.matches(
        &BTreeSet::from([Asset::Qqq]),
        &BTreeSet::new(),
        &BTreeSet::new(),
        &BTreeSet::new()
    ));
}
#[test]
fn lesson_ranking_prefers_explicit_requested_scope() {
    let generic = lesson();
    let mut exact = generic.clone();
    exact.scope.assets.insert(Asset::Qqq);
    let scope = ContextQueryScope {
        assets: BTreeSet::from([Asset::Qqq]),
        ..Default::default()
    };
    assert!(lesson_relevance(&exact, &scope) > lesson_relevance(&generic, &scope));
}
#[test]
fn exact_lesson_duplicates_normalize_only_whitespace() {
    let a = lesson();
    let mut b = a.clone();
    b.statement = "Use  bounded\n estimates".into();
    assert_eq!(
        lesson_retrieval_fingerprint(&a).unwrap(),
        lesson_retrieval_fingerprint(&b).unwrap()
    );
    b.statement = "Never use bounded estimates".into();
    assert_ne!(
        lesson_retrieval_fingerprint(&a).unwrap(),
        lesson_retrieval_fingerprint(&b).unwrap()
    );
}
#[test]
fn lesson_scope_and_exclusions_prevent_duplicate_collapse() {
    let a = lesson();
    let mut b = a.clone();
    b.scope.assets.insert(Asset::Qqq);
    assert_ne!(
        lesson_retrieval_fingerprint(&a).unwrap(),
        lesson_retrieval_fingerprint(&b).unwrap()
    );
    b = a.clone();
    b.exclusions.push("high volatility".into());
    assert_ne!(
        lesson_retrieval_fingerprint(&a).unwrap(),
        lesson_retrieval_fingerprint(&b).unwrap()
    );
}
#[test]
fn expired_lesson_is_not_retrievable() {
    let mut a = lesson();
    a.governance.as_mut().unwrap().valid_until = Some(Utc::now() - Duration::seconds(1));
    assert!(!a.is_retrievable(Utc::now(), 0, &BTreeSet::new()));
}
#[test]
fn usage_exhaustion_requires_revalidation() {
    let a = lesson();
    assert!(!a.is_retrievable(Utc::now(), 100, &BTreeSet::new()));
    assert!(a.is_retrievable(Utc::now(), 99, &BTreeSet::new()));
}
#[test]
fn conflicting_lesson_component_is_kept_whole_or_excluded() {
    // 冲突关系按连通分量整体占用召回额度；分量放不下时必须整体排除，不能只展示一边。
    let ids = (0..6)
        .map(|i| ArtifactId(ContentHash::of_bytes(&[i])))
        .collect::<Vec<_>>();
    let pair = vec![
        (
            ids[0].clone(),
            vec![ArtifactRef {
                artifact_id: ids[1].clone(),
                kind: ArtifactKind::Lesson,
            }],
        ),
        (ids[1].clone(), vec![]),
    ];
    assert_eq!(select_lesson_groups(&pair, 4), ids[..2]);
    assert!(select_lesson_groups(&pair, 1).is_empty());
    let mut oversized = vec![(
        ids[0].clone(),
        ids[1..]
            .iter()
            .map(|id| ArtifactRef {
                artifact_id: id.clone(),
                kind: ArtifactKind::Lesson,
            })
            .collect(),
    )];
    oversized.extend(ids[1..].iter().map(|id| (id.clone(), vec![])));
    assert!(select_lesson_groups(&oversized, 4).is_empty());
}

#[test]
fn active_snapshot_includes_relevant_lesson_behind_fifty_newer_entries() {
    // 先写入同一隔离 Store 的完整 Active 快照，再对比旧的 50 条读取窗口，验证
    // 召回排序不会因为最近条数上限而把更早但匹配范围的 Lesson 永久隐藏。
    let store = Store::open(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.akzio/quality-tests")
            .join(akzio_domain::RunId::new().0),
    )
    .unwrap();
    let now = Utc::now();
    let source = Artifact::new(
        ArtifactKind::SemanticDetail,
        store
            .stage_json(&serde_json::json!({"synthetic":true}))
            .unwrap(),
        "test.operator",
        ArtifactLifecycle::Canonical,
        ArtifactProvenance {
            source_family: "akzio.operator".into(),
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
    for i in 0..135 {
        let mut a = lesson();
        a.lesson_id = LessonId(format!("quality-{i:03}"));
        a.created_at = now;
        a.updated_at = now + Duration::seconds(i);
        a.source_refs = vec![ArtifactRef {
            artifact_id: source.artifact_id.clone(),
            kind: source.kind,
        }];
        a.scope
            .assets
            .insert(if i == 0 { Asset::Qqq } else { Asset::Soxx });
        store.write_lesson(&a, &source, a.updated_at).unwrap();
    }
    assert!(store
        .lessons(Some(LessonLifecycle::Active), 50)
        .unwrap()
        .iter()
        .all(|s| !s.lesson.scope.assets.contains(&Asset::Qqq)));
    let snapshot = store.active_lessons_snapshot().unwrap();
    assert_eq!(snapshot.len(), 135);
    assert_eq!(
        snapshot
            .iter()
            .filter(|s| s.lesson.scope.assets.contains(&Asset::Qqq))
            .count(),
        1
    );
}
