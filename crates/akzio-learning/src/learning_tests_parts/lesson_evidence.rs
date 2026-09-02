// The akzio-learning half of the Lesson evidence ledger. `akzio-store` owns the
// persistence invariants; what is under test here is the derivation: which
// Lesson revisions a sealed decision took a position on, and what the sealed
// windows say. No test below asserts an effect, because a single arm of a single
// decision cannot support one.

use akzio_domain::{DecisionContext, DecisionId, LessonAttribution, LessonEvidence};

struct LessonEvidenceHarness {
    fixture: RuntimeFixture,
    applied_lesson: Lesson,
    applied_artifact: ArtifactRef,
    rejected_lesson: Lesson,
    rejected_artifact: ArtifactRef,
    decision_context_artifact: ArtifactRef,
    outcome_artifact: ArtifactRef,
    outcome: Outcome,
    recorded_at: DateTime<Utc>,
}

impl LessonEvidenceHarness {
    fn new() -> Self {
        let fixture = RuntimeFixture::new();
        let now = fixture_time();
        let (applied_lesson, applied_artifact) = lesson(&fixture.store, "opening-volatility", now);
        let (rejected_lesson, rejected_artifact) = lesson(&fixture.store, "gap-continuation", now);

        // The ledger stores artifact references the Store must resolve, so the
        // DecisionContext has to exist durably; a bare `ArtifactRef` would pass
        // derivation and then be rejected on write.
        let permit = fixture.claim_evaluation("lesson-evidence");
        let context_artifact = fixture_artifact(
            &fixture.store,
            Some(&permit),
            ArtifactKind::DecisionContext,
            ArtifactLifecycle::Canonical,
            &serde_json::json!({"context": "lesson-evidence"}),
            vec![],
            now,
        );
        fixture
            .store
            .write_task_artifact(
                &permit,
                &context_artifact,
                LifecycleEventType::PaperSeedArtifactCreated,
                now,
            )
            .unwrap();

        let decision_context_artifact = artifact_reference(&context_artifact);
        let outcome_artifact = fixture.parent_outcome.clone();
        // The same sealed payload the fixture already committed under
        // `parent_outcome`.
        let outcome = materialize_outcome(&fixture.materialization).unwrap();
        let recorded_at = fixture.pair_completed_at;

        Self {
            fixture,
            applied_lesson,
            applied_artifact,
            rejected_lesson,
            rejected_artifact,
            decision_context_artifact,
            outcome_artifact,
            outcome,
            recorded_at,
        }
    }

    fn decision_context(
        &self,
        applied: Vec<ArtifactRef>,
        rejected: Vec<ArtifactRef>,
    ) -> DecisionContext {
        DecisionContext {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            decision_id: DecisionId::new(),
            run_id: self.fixture.paper_run_id.clone(),
            claims: vec![reference(ArtifactKind::Claim, b"lesson-evidence-claim")],
            critiques: vec![],
            evidence: vec![],
            policy_influences: vec![],
            applied_learning_refs: applied,
            rejected_learning_refs: rejected,
            material_conflicts: vec![],
            hard_blockers: vec![],
            soft_warnings: vec![],
            decision_policy_hash: ContentHash::of_bytes(b"decision-policy"),
            target: self.fixture.materialization.target.clone(),
            created_at: fixture_time(),
        }
    }

    fn derive(&self, context: &DecisionContext) -> Vec<LessonEvidence> {
        self.derive_at(context, &self.outcome, self.recorded_at)
            .unwrap()
    }

    fn derive_at(
        &self,
        context: &DecisionContext,
        outcome: &Outcome,
        recorded_at: DateTime<Utc>,
    ) -> EvaluationRuntimeResult<Vec<LessonEvidence>> {
        self.fixture.runtime.lesson_evidence_from_decision(
            &self.decision_context_artifact,
            context,
            &self.outcome_artifact,
            outcome,
            recorded_at,
        )
    }
}

/// A Lesson that reaches a DecisionContext is an approved, Active one, so the
/// harness writes it in that state rather than as a Draft.
fn lesson(store: &V2Store, label: &str, now: DateTime<Utc>) -> (Lesson, ArtifactRef) {
    let source = fixture_artifact(
        store,
        None,
        ArtifactKind::SemanticDetail,
        ArtifactLifecycle::Canonical,
        &serde_json::json!({"lesson_source": label}),
        vec![],
        now,
    );
    let lesson = Lesson {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        lesson_id: LessonId::new(),
        origin: LessonOrigin::Operator,
        lifecycle: LessonLifecycle::Active,
        title: format!("{label} title"),
        statement: format!("{label} statement"),
        rationale: format!("{label} rationale"),
        recommended_behavior: format!("{label} recommended behavior"),
        exclusions: vec![],
        scope: LessonScope::default(),
        source_refs: vec![artifact_reference(&source)],
        supersedes: vec![],
        conflicts_with: vec![],
        confidence_ppm: 500_000,
        authored_by: Some("operator:test".to_owned()),
        approved_by: Some("operator:reviewer".to_owned()),
        created_at: now,
        updated_at: now,
    };
    let written = store.write_lesson(&lesson, &source, now).unwrap();
    (lesson, artifact_reference(&written.lesson.artifact))
}

/// Both arms are observable, not only the applied one: the decision gate already
/// forces every Lesson in the manifest into exactly one of them
/// (`MissingLearningAttribution`).
#[test]
fn lesson_evidence_records_both_the_applied_and_the_rejected_arm() {
    let harness = LessonEvidenceHarness::new();
    let context = harness.decision_context(
        vec![harness.applied_artifact.clone()],
        vec![harness.rejected_artifact.clone()],
    );

    let records = harness.derive(&context);
    assert_eq!(records.len(), 2);

    let applied = &records[0];
    assert_eq!(applied.attribution, LessonAttribution::Applied);
    assert_eq!(applied.lesson_id, harness.applied_lesson.lesson_id);
    // The stable id keeps evidence reachable after a lifecycle transition; the
    // artifact records which revision the model actually saw.
    assert_eq!(applied.lesson_artifact, harness.applied_artifact);
    assert_eq!(applied.decision_context, harness.decision_context_artifact);
    assert_eq!(applied.outcome, harness.outcome_artifact);
    assert_eq!(applied.recorded_at, harness.recorded_at);

    let rejected = &records[1];
    assert_eq!(rejected.attribution, LessonAttribution::Rejected);
    assert_eq!(rejected.lesson_id, harness.rejected_lesson.lesson_id);
    assert_eq!(rejected.lesson_artifact, harness.rejected_artifact);

    // Same decision, same outcome: the two arms differ only in attribution and
    // in which Lesson they name, which is exactly why neither is a
    // counterfactual for the other.
    assert_eq!(
        applied.utility_ppm_by_horizon,
        rejected.utility_ppm_by_horizon
    );
}

/// The record copies the sealed windows verbatim, in `OutcomeHorizon::ALL` order,
/// and never invents a value for a horizon that carried no scored forecast.
#[test]
fn lesson_evidence_copies_utility_and_calibration_from_the_sealed_windows() {
    let harness = LessonEvidenceHarness::new();
    let context = harness.decision_context(vec![harness.applied_artifact.clone()], vec![]);

    let record = harness.derive(&context).remove(0);
    assert_eq!(record.utility_ppm_by_horizon, [49_850, -50_150, -150]);
    assert_eq!(
        record.calibration_ppm_by_horizon,
        [Some(960_000), Some(960_000), Some(750_000)]
    );

    // An unmeasured horizon must stay `None` rather than collapse to 0, which on
    // the higher-is-better quality scale would read as a maximally wrong
    // forecast.
    let mut unmeasured = harness.outcome.clone();
    unmeasured.windows[2].calibration_ppm = None;
    let record = harness
        .derive_at(&context, &unmeasured, harness.recorded_at)
        .unwrap()
        .remove(0);
    assert_eq!(
        record.calibration_ppm_by_horizon,
        [Some(960_000), Some(960_000), None]
    );
}

/// Experience and CandidatePolicy are legal learning references but they are not
/// Lessons: they are outcome-backed artifacts with their own lineage, and this
/// ledger is keyed on `lesson_id`.
#[test]
fn lesson_evidence_skips_experience_and_candidate_policy_refs() {
    let harness = LessonEvidenceHarness::new();
    let context = harness.decision_context(
        vec![
            harness.applied_artifact.clone(),
            reference(ArtifactKind::Experience, b"experience"),
        ],
        vec![reference(ArtifactKind::CandidatePolicy, b"candidate-policy")],
    );

    let records = harness.derive(&context);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].lesson_id, harness.applied_lesson.lesson_id);
    assert_eq!(records[0].attribution, LessonAttribution::Applied);
}

/// Canonical learning is defined on sealed outcomes only. A partial outcome has
/// no `sealed_at` and is missing windows, so it cannot back a ledger record.
#[test]
fn lesson_evidence_requires_a_sealed_outcome() {
    let harness = LessonEvidenceHarness::new();
    let context = harness.decision_context(vec![harness.applied_artifact.clone()], vec![]);

    let mut partial_input = harness.fixture.materialization.clone();
    partial_input
        .observations
        .retain(|observation| observation.horizon == OutcomeHorizon::T1);
    let partial = materialize_partial_outcome(&partial_input).unwrap();
    assert!(partial.sealed_at.is_none());

    assert!(matches!(
        harness.derive_at(&context, &partial, harness.recorded_at),
        Err(EvaluationError::Domain(DomainError::InvalidBudget {
            field: "outcome.windows"
        }))
    ));
}

#[test]
fn lesson_evidence_rejects_mislabelled_reference_kinds() {
    let harness = LessonEvidenceHarness::new();
    let context = harness.decision_context(vec![harness.applied_artifact.clone()], vec![]);

    assert!(matches!(
        harness.fixture.runtime.lesson_evidence_from_decision(
            &harness.outcome_artifact,
            &context,
            &harness.outcome_artifact,
            &harness.outcome,
            harness.recorded_at,
        ),
        Err(EvaluationError::InvalidMaterialization(
            "lesson evidence reference kind"
        ))
    ));

    // A Lesson reference that does not resolve to a Lesson artifact must fail
    // rather than silently produce a record keyed on a guessed lesson_id.
    let mistyped = harness.decision_context(
        vec![ArtifactRef {
            artifact_id: harness.decision_context_artifact.artifact_id.clone(),
            kind: ArtifactKind::Lesson,
        }],
        vec![],
    );
    assert!(matches!(
        harness.derive_at(&mistyped, &harness.outcome, harness.recorded_at),
        Err(EvaluationError::InvalidMaterialization(
            "lesson evidence reference kind"
        ))
    ));
}

/// The cross-crate seam: what the runtime derives has to be exactly what the
/// ledger accepts, and reprocessing the same sealed decision on a later day must
/// stay a no-op even though it carries a later `recorded_at`.
#[test]
fn derived_lesson_evidence_is_accepted_and_deduplicated_by_the_ledger() {
    let harness = LessonEvidenceHarness::new();
    let store = &harness.fixture.store;
    let context = harness.decision_context(
        vec![harness.applied_artifact.clone()],
        vec![harness.rejected_artifact.clone()],
    );

    let records = harness.derive(&context);
    assert_eq!(
        store
            .record_lesson_evidence(&records, harness.recorded_at)
            .unwrap(),
        2
    );

    let later = harness.recorded_at + Duration::days(1);
    let replayed = harness
        .derive_at(&context, &harness.outcome, later)
        .unwrap();
    assert_ne!(replayed, records, "the replay carries a later recorded_at");
    assert_eq!(
        store.record_lesson_evidence(&replayed, later).unwrap(),
        0,
        "a reprocess of the same triple must not append a second record"
    );

    let summary = store
        .lesson_evidence_summary(&harness.applied_lesson.lesson_id)
        .unwrap();
    assert_eq!(summary.applied_count, 1);
    assert_eq!(summary.rejected_count, 0);
    assert!(summary.observational);
    store.verify_integrity().unwrap();
}




