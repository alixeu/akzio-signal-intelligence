use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

struct DeadlineModel {
    calls: AtomicUsize,
    pending_submit: bool,
    fixture: ModelClientAdapter,
}

impl DeadlineModel {
    fn new(pending_submit: bool) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            pending_submit,
            fixture: ModelClientAdapter::new(super::late_model_tests::outcome_accounting_fixture()),
        }
    }
}

impl AgentModel for DeadlineModel {
    fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        self.fixture.capability_snapshot()
    }

    fn turn<'a>(
        &'a self,
        request: AgentModelRequest,
    ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.pending_submit && request.phase == AgentTurnPhase::Submit {
                return std::future::pending().await;
            }
            let mut turn = self.fixture.turn(request).await?;
            // Explicit offline telemetry distinguishes the known Draft from
            // the pending Submit; no real provider usage is synthesized.
            let telemetry = turn.telemetry.as_mut().unwrap();
            telemetry.input_tokens = Some(10);
            telemetry.output_tokens = Some(5);
            telemetry.cached_input_tokens = Some(0);
            telemetry.reasoning_tokens = Some(0);
            Ok(turn)
        })
    }
}

#[tokio::test]
async fn deadline_pending_submit_records_unknown_usage_before_attempt_wall() {
    let claimed_at = Utc::now() - Duration::seconds(1);
    let (store, runtime, attempt) =
        super::late_model_tests::isolated_outcome_attempt(4, claimed_at);
    let model = DeadlineModel::new(true);
    let hard_deadline = claimed_at + Duration::seconds(4);
    let remaining = hard_deadline
        .signed_duration_since(Utc::now())
        .to_std()
        .unwrap();
    let result = tokio::time::timeout(
        remaining,
        runtime.run(
            &attempt.permit,
            &attempt.node,
            attempt.node.input_artifacts.clone(),
            &model,
            Utc::now(),
        ),
    )
    .await
    .expect("inner deadline must leave time to persist failure before outer wall");
    assert!(
        matches!(result, Err(ResearchError::WallTimeExceeded { .. })),
        "{result:?}"
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    let events = store
        .attempt_events(
            &attempt.run_id,
            &attempt.node.task_id,
            &attempt.permit.attempt_id,
        )
        .unwrap();
    let failed = events
        .iter()
        .filter(|event| event.event_type == "agent.turn_failed")
        .collect::<Vec<_>>();
    assert_eq!(failed.len(), 1);
    assert!(failed[0].created_at < hard_deadline);
    let artifact = store
        .artifact(failed[0].artifact_id.as_ref().unwrap())
        .unwrap();
    let payload: Value = serde_json::from_slice(&store.read_blob(&artifact.blob).unwrap()).unwrap();
    assert_eq!(payload["error_class"], "wall_time");
    assert_eq!(payload["request"]["phase"], "submit");
    let usage = store.run_model_usage(&attempt.run_id).unwrap();
    assert_eq!(usage.turns, 2);
    assert_eq!(
        usage.turns_missing_usage, 1,
        "timeout cannot invent provider usage"
    );
}

#[tokio::test]
async fn deadline_exhausted_attempt_never_dispatches_model() {
    let (store, runtime, attempt) =
        super::late_model_tests::isolated_outcome_attempt(2, Utc::now() - Duration::seconds(3));
    let model = DeadlineModel::new(false);
    assert!(runtime
        .run(
            &attempt.permit,
            &attempt.node,
            attempt.node.input_artifacts.clone(),
            &model,
            Utc::now()
        )
        .await
        .is_err());
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert!(!store
        .attempt_events(
            &attempt.run_id,
            &attempt.node.task_id,
            &attempt.permit.attempt_id
        )
        .unwrap()
        .iter()
        .any(|event| event.event_type == "agent.turn_started"));
}

#[tokio::test]
async fn deadline_fresh_attempt_still_accepts_immediate_draft_and_submit() {
    let (store, runtime, attempt) =
        super::late_model_tests::isolated_outcome_attempt(4, Utc::now());
    let model = DeadlineModel::new(false);
    let artifact = runtime
        .run(
            &attempt.permit,
            &attempt.node,
            attempt.node.input_artifacts.clone(),
            &model,
            Utc::now(),
        )
        .await
        .unwrap();
    assert_eq!(artifact.kind, ArtifactKind::RetrospectiveDraft);
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    let usage = store.run_model_usage(&attempt.run_id).unwrap();
    assert_eq!(usage.turns, 2);
    assert_eq!(usage.turns_missing_usage, 0);
}

#[tokio::test]
async fn deadline_exhausted_draft_window_never_dispatches_with_task_time_remaining() {
    let (store, runtime, attempt) =
        super::late_model_tests::isolated_outcome_attempt(4, Utc::now() - Duration::seconds(3));
    let model = DeadlineModel::new(false);
    assert!(runtime
        .run(
            &attempt.permit,
            &attempt.node,
            attempt.node.input_artifacts.clone(),
            &model,
            Utc::now()
        )
        .await
        .is_err());
    assert_eq!(
        model.calls.load(Ordering::SeqCst),
        0,
        "timeout(0) is not a pre-dispatch authorization check"
    );
    assert!(!store
        .attempt_events(
            &attempt.run_id,
            &attempt.node.task_id,
            &attempt.permit.attempt_id
        )
        .unwrap()
        .iter()
        .any(|event| event.event_type == "agent.turn_started"));
}

#[tokio::test]
async fn deadline_future_attempt_start_fails_closed_before_dispatch() {
    let (store, runtime, attempt) =
        super::late_model_tests::isolated_outcome_attempt(4, Utc::now() + Duration::seconds(10));
    let model = DeadlineModel::new(false);
    assert!(runtime
        .run(
            &attempt.permit,
            &attempt.node,
            attempt.node.input_artifacts.clone(),
            &model,
            Utc::now()
        )
        .await
        .is_err());
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
    assert!(!store
        .attempt_events(
            &attempt.run_id,
            &attempt.node.task_id,
            &attempt.permit.attempt_id
        )
        .unwrap()
        .iter()
        .any(|event| event.event_type == "agent.turn_started"));
}

#[tokio::test]
async fn deadline_alignment_does_not_backdate_context_materialization() {
    let claimed_at = Utc::now() - Duration::seconds(1);
    let (store, runtime, attempt) =
        super::late_model_tests::isolated_outcome_attempt(4, claimed_at);
    let context_now = Utc::now();
    let model = DeadlineModel::new(false);
    runtime
        .run(
            &attempt.permit,
            &attempt.node,
            attempt.node.input_artifacts.clone(),
            &model,
            context_now,
        )
        .await
        .unwrap();
    let events = store
        .attempt_events(
            &attempt.run_id,
            &attempt.node.task_id,
            &attempt.permit.attempt_id,
        )
        .unwrap();
    let created = events
        .iter()
        .find(|event| event.event_type == "context.manifest_created")
        .unwrap();
    let artifact = store
        .artifact(created.artifact_id.as_ref().unwrap())
        .unwrap();
    assert_eq!(artifact.created_at, context_now);
    assert!(artifact.created_at > claimed_at);
}
