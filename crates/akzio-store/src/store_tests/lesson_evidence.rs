// LessonEvidence is the descriptive half of the experience loop: it records
// which Lesson revision a decision cited and how that decision's sealed outcome
// turned out. It deliberately proves nothing causal, so these tests pin the
// ledger's immutability, its idempotency key, and its survival across Lesson
// revisions rather than any effect claim.

use akzio_domain::{
    Lesson, LessonAttribution, LessonEvidence, LessonLifecycle, LessonOrigin, LessonScope,
};

struct LessonEvidenceFixture {
    fixture: PolicyCommitFixture,
    lesson: Lesson,
    lesson_artifact: ArtifactRef,
    /// Several distinct DecisionContexts, so records can differ in the
    /// idempotency key instead of colliding.
    decision_contexts: Vec<ArtifactRef>,
    outcome: ArtifactRef,
}

impl LessonEvidenceFixture {
    fn new() -> Self {
        let fixture = PolicyCommitFixture::memory();
        // The permit is consumed by the policy-evaluation commit below, so every
        // task artifact this fixture needs has to be written first.
        let decision_contexts = ["primary", "second", "third"]
            .into_iter()
            .map(|label| Self::decision_context(&fixture, label))
            .collect::<Vec<_>>();

        // PolicyCommitFixture builds its learning artifacts but does not persist
        // them; the atomic outcome commit is what makes the sealed Outcome
        // durable and therefore referenceable by the ledger. `insert_pair` plus
        // `record_policy_evaluation` would also persist it but leaves a raw
        // shadow-pair row that Doctor rejects, and these tests assert Doctor is
        // clean.
        fixture
            .store
            .commit_outcomes(
                &fixture.permit,
                std::slice::from_ref(&fixture.outcome),
                fixture.now,
            )
            .unwrap();

        let source = artifact(
            &fixture.store,
            ArtifactKind::SemanticDetail,
            r#"{"note":"lesson evidence source"}"#,
            None,
        );
        // write_lesson requires a Canonical source it can insert itself.
        let source = Artifact::new(
            ArtifactKind::SemanticDetail,
            source.blob.clone(),
            "fixture.lesson_evidence",
            ArtifactLifecycle::Canonical,
            source.provenance.clone(),
            None,
            vec![],
            fixture.now,
        )
        .unwrap();
        let lesson = Lesson {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            lesson_id: LessonId::new(),
            origin: LessonOrigin::Operator,
            lifecycle: LessonLifecycle::Draft,
            title: "Opening volatility".to_owned(),
            statement: "High opening volatility weakens the signal.".to_owned(),
            rationale: "The first quote window is noisy.".to_owned(),
            recommended_behavior: "Do not act on the first quote window alone.".to_owned(),
            exclusions: vec![],
            scope: LessonScope::default(),
            source_refs: vec![artifact_ref(&source)],
            supersedes: vec![],
            conflicts_with: vec![],
            confidence_ppm: 500_000,
            authored_by: Some("operator:test".to_owned()),
            approved_by: None,
            created_at: fixture.now,
            updated_at: fixture.now,
        };
        let written = fixture
            .store
            .write_lesson(&lesson, &source, fixture.now)
            .unwrap();
        let lesson_artifact = artifact_ref(&written.lesson.artifact);
        let outcome = artifact_ref(&fixture.outcome);

        Self {
            fixture,
            lesson,
            lesson_artifact,
            decision_contexts,
            outcome,
        }
    }

    /// DecisionContext is not one of the atomically committed learning kinds, so
    /// the ordinary task-artifact path can persist it.
    fn decision_context(fixture: &PolicyCommitFixture, label: &str) -> ArtifactRef {
        let context = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::DecisionContext,
            &serde_json::json!({"fixture": label}),
            vec![],
            ArtifactLifecycle::RunScoped,
            fixture.now,
        );
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &context,
                LifecycleEventType::FixtureGenericWrite,
                fixture.now,
            )
            .unwrap();
        artifact_ref(&context)
    }

    fn evidence(&self, attribution: LessonAttribution, utility: [i64; 3]) -> LessonEvidence {
        self.evidence_for(0, attribution, utility)
    }

    fn evidence_for(
        &self,
        decision_index: usize,
        attribution: LessonAttribution,
        utility: [i64; 3],
    ) -> LessonEvidence {
        LessonEvidence {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            lesson_id: self.lesson.lesson_id.clone(),
            lesson_artifact: self.lesson_artifact.clone(),
            decision_context: self.decision_contexts[decision_index].clone(),
            outcome: self.outcome.clone(),
            attribution,
            utility_ppm_by_horizon: utility,
            calibration_ppm_by_horizon: [Some(960_000), Some(960_000), Some(750_000)],
            recorded_at: self.fixture.now,
        }
    }
}

/// The acceptance criterion from the brief: reprocessing the same
/// (lesson, decision, outcome) triple must not produce a duplicate record.
#[test]
fn lesson_evidence_is_idempotent_on_the_lesson_decision_outcome_triple() {
    let harness = LessonEvidenceFixture::new();
    let store = &harness.fixture.store;
    let now = harness.fixture.now;
    let record = harness.evidence(LessonAttribution::Applied, [10, 20, 30]);

    assert_eq!(
        store
            .record_lesson_evidence(std::slice::from_ref(&record), now)
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .record_lesson_evidence(&[record.clone(), record.clone()], now)
            .unwrap(),
        0,
        "the same triple must never insert twice, even within one batch"
    );
    assert_eq!(
        store.lesson_evidence(&harness.lesson.lesson_id).unwrap(),
        vec![record.clone()]
    );

    // Same key, different payload: the ledger is immutable, so this is corruption
    // rather than an update.
    let mut mutated = record;
    mutated.utility_ppm_by_horizon = [-1, -2, -3];
    assert!(matches!(
        store.record_lesson_evidence(&[mutated], now),
        Err(StoreError::Integrity(message)) if message.contains("immutable")
    ));

    store.verify_integrity().unwrap();
}

/// Every lifecycle transition writes a fresh Lesson artifact. Keying the ledger
/// on the stable `lesson_id` is what keeps earlier evidence reachable from the
/// new head; keying on `lesson_artifact_id` would orphan it.
#[test]
fn lesson_evidence_survives_a_lifecycle_transition() {
    let harness = LessonEvidenceFixture::new();
    let store = &harness.fixture.store;
    let now = harness.fixture.now;
    store
        .record_lesson_evidence(
            &[harness.evidence(LessonAttribution::Applied, [10, 20, 30])],
            now,
        )
        .unwrap();

    let transitioned = store
        .transition_lesson(
            &harness.lesson.lesson_id,
            LessonLifecycle::Active,
            "operator:reviewer",
            "approved for governed context",
            now,
        )
        .unwrap();
    assert_ne!(
        transitioned.artifact.artifact_id, harness.lesson_artifact.artifact_id,
        "a transition must produce a new Lesson artifact"
    );

    let records = store.lesson_evidence(&harness.lesson.lesson_id).unwrap();
    assert_eq!(
        records.len(),
        1,
        "prior evidence must remain reachable after the head moves"
    );
    assert_eq!(
        records[0].lesson_artifact.artifact_id, harness.lesson_artifact.artifact_id,
        "the record still names the revision that was actually injected"
    );
    store.verify_integrity().unwrap();
}

/// Answers "applied N times, of which M closed with positive utility" and marks
/// the answer observational so no caller can read it as a causal effect.
#[test]
fn lesson_evidence_summary_is_observational() {
    let harness = LessonEvidenceFixture::new();
    let store = &harness.fixture.store;
    let now = harness.fixture.now;

    let applied_positive = harness.evidence(LessonAttribution::Applied, [10, 20, 30]);
    let applied_negative =
        harness.evidence_for(1, LessonAttribution::Applied, [-10, -20, -30]);
    let rejected = harness.evidence_for(2, LessonAttribution::Rejected, [5, -1, -1]);

    assert_eq!(
        store
            .record_lesson_evidence(&[applied_positive, applied_negative, rejected], now)
            .unwrap(),
        3
    );

    let summary = store
        .lesson_evidence_summary(&harness.lesson.lesson_id)
        .unwrap();
    assert_eq!(summary.applied_count, 2);
    assert_eq!(summary.applied_with_positive_utility, 1);
    assert_eq!(summary.rejected_count, 1);
    // Not a counterfactual for the applied arm: the rejected arm ran a different
    // decision under different conditions.
    assert_eq!(summary.rejected_with_positive_utility, 1);
    assert_eq!(summary.mean_calibration_ppm, Some(890_000));
    assert!(
        summary.observational,
        "the summary must never be presented as an effect estimate"
    );
    store.verify_integrity().unwrap();
}

#[test]
fn lesson_evidence_requires_a_lesson_head_and_resolvable_references() {
    let harness = LessonEvidenceFixture::new();
    let store = &harness.fixture.store;
    let now = harness.fixture.now;

    let mut orphan = harness.evidence(LessonAttribution::Applied, [1, 1, 1]);
    orphan.lesson_id = LessonId("missing-lesson".to_owned());
    assert!(matches!(
        store.record_lesson_evidence(&[orphan], now),
        Err(StoreError::InvalidLearningCommit("lesson_evidence.lesson"))
    ));

    let mut wrong_kind = harness.evidence(LessonAttribution::Applied, [1, 1, 1]);
    wrong_kind.outcome = ArtifactRef {
        artifact_id: harness.decision_contexts[0].artifact_id.clone(),
        kind: ArtifactKind::Outcome,
    };
    assert!(matches!(
        store.record_lesson_evidence(&[wrong_kind], now),
        Err(StoreError::InvalidLearningCommit(
            "lesson_evidence.reference_kind"
        ))
    ));

    let mut future = harness.evidence(LessonAttribution::Applied, [1, 1, 1]);
    future.recorded_at = now + Duration::seconds(1);
    assert!(matches!(
        store.record_lesson_evidence(&[future], now),
        Err(StoreError::InvalidLearningCommit(
            "lesson_evidence.recorded_at"
        ))
    ));

    assert!(store
        .lesson_evidence(&harness.lesson.lesson_id)
        .unwrap()
        .is_empty());
    store.verify_integrity().unwrap();
}

/// Doctor must reject a hand-edited ledger row whose key columns no longer agree
/// with the payload it stores.
#[test]
fn store_doctor_rejects_tampered_lesson_evidence() {
    let harness = LessonEvidenceFixture::new();
    let store = &harness.fixture.store;
    let now = harness.fixture.now;
    store
        .record_lesson_evidence(&[harness.evidence(LessonAttribution::Applied, [1, 2, 3])], now)
        .unwrap();
    store.verify_integrity().unwrap();

    store
        .connection()
        .unwrap()
        .execute(
            "UPDATE rebuild_lesson_evidence SET attribution = 'rejected'",
            [],
        )
        .unwrap();

    assert!(matches!(
        store.verify_integrity(),
        Err(StoreError::Integrity(message))
            if message.contains("key columns disagree with its payload")
    ));
}
