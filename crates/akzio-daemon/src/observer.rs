//! Bounded read-only projection for a specific Run.

use super::*;
use akzio_domain::{
    Asset, DecisionDraft, DecisionHorizon, DecisionProposal, Evaluation, Experience, Outcome,
    OutcomeSchedule, PaperCommitment, ResearchAllocationPlan, ResearchCritique, WeightPpm,
    WorkflowProposalDraft,
};
use akzio_runtime::{DECISION_GATE_RECIPE_ID, EXECUTION_GATE_RECIPE_ID, RECONCILE_RECIPE_ID};
use serde::de::DeserializeOwned;
use serde_json::json;
use std::collections::BTreeSet;

#[path = "observer/operator_report.rs"]
mod operator_report;

#[cfg(test)]
#[path = "observer/partial_fill_tests.rs"]
mod partial_fill_tests;

const OBSERVER_EVENT_LIMIT: usize = 100;
const OBSERVER_TRAJECTORY_LIMIT: usize = 200;

#[derive(Debug, Serialize)]
pub(crate) struct ObserverRunDetail {
    pub inspection: akzio_store::RunInspection,
    pub research_audit: akzio_store::ResearchAudit,
    pub workflow: WorkflowSnapshot,
    pub events: Vec<EventView>,
    pub trajectory: Vec<TrajectoryEntry>,
    pub artifacts: Vec<ObserverArtifactView>,
    pub telemetry: ObserverRunTelemetry,
    pub operator_report: Value,
}

#[derive(Debug, Serialize)]
pub(crate) struct ObserverRunTelemetry {
    pub model_id: Option<String>,
    pub latency_millis: Option<u64>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub tool_calls: usize,
    pub turns: usize,
}

#[derive(Debug, Serialize)]
pub(crate) struct ObserverArtifactView {
    pub artifact_id: ArtifactId,
    pub kind: ArtifactKind,
    pub created_at: DateTime<Utc>,
    pub run_id: Option<RunId>,
    pub runtime_manifest: Option<ArtifactRef>,
    pub runtime_identity_hash: Option<ContentHash>,
    pub payload: Value,
}

impl Daemon {
    pub(crate) fn observer_run_detail(&self, run_id: &RunId) -> Result<ObserverRunDetail> {
        // 详情的有界审计投影与报表共享一个水位，Artifact payload 仍经过白名单类型解码。
        self.store.with_read_snapshot(|store| {
            let workflow = store.workflow_snapshot(run_id)?;
            let operator_report = operator_report::build(store, &workflow)?;
            #[cfg(test)]
            partial_fill_tests::after_operator_report();
            let events = store
                .recent_events(run_id, OBSERVER_EVENT_LIMIT)?
                .into_iter()
                .map(|event| EventView {
                    cursor: event.cursor,
                    event_type: event.event_type,
                    task_id: event.task_id.map(|task| task.0),
                    created_at: event.created_at.to_rfc3339(),
                })
                .collect();
            let trajectory = store.recent_trajectory(run_id, OBSERVER_TRAJECTORY_LIMIT)?;
            let artifacts = Self::observer_artifacts(store, &trajectory)?;
            let telemetry = observer_run_telemetry(&trajectory);

            Ok(ObserverRunDetail {
                inspection: Self::runtime_inspection(store, run_id)?,
                research_audit: store.research_audit(run_id)?,
                workflow,
                events,
                trajectory,
                artifacts,
                telemetry,
                operator_report,
            })
        })
    }

    fn observer_artifacts(
        store: &Store,
        trajectory: &[TrajectoryEntry],
    ) -> Result<Vec<ObserverArtifactView>> {
        // 只检查当前 Run 轨迹记录的 Artifact，正文由下方类型白名单限制。
        let mut seen = BTreeSet::new();
        let mut artifacts = Vec::new();
        for entry in trajectory {
            let Some(artifact_id) = entry
                .artifact_id
                .as_ref()
                .filter(|_| entry.artifact_kind.is_some())
            else {
                continue;
            };
            if !seen.insert(artifact_id.clone()) {
                continue;
            }
            let artifact = store.artifact(artifact_id)?;
            if let Some(view) = Self::observer_artifact_view(store, &artifact)? {
                artifacts.push(view);
            }
        }
        artifacts.sort_by_key(|artifact| artifact.created_at);
        Ok(artifacts)
    }

    fn observer_artifact_view(
        store: &Store,
        artifact: &Artifact,
    ) -> Result<Option<ObserverArtifactView>> {
        // 只有白名单业务 Artifact kind 才反序列化为 observer payload；未知 kind 保持 None，
        // runtime_manifest/approval 另从对应绑定读取，避免把非公开原文泄露给 UI。
        let payload = match artifact.kind {
            ArtifactKind::WorkflowProposalDraft => {
                Self::typed_observer_payload::<WorkflowProposalDraft>(store, artifact)?
            }
            ArtifactKind::Claim => Self::typed_observer_payload::<ResearchClaim>(store, artifact)?,
            ArtifactKind::Critique => {
                Self::typed_observer_payload::<ResearchCritique>(store, artifact)?
            }
            ArtifactKind::DecisionProposal => {
                Self::typed_observer_payload::<DecisionProposal>(store, artifact)?
            }
            ArtifactKind::DecisionContext => {
                Self::typed_observer_payload::<DecisionContext>(store, artifact)?
            }
            ArtifactKind::Decision => Self::typed_observer_payload::<Decision>(store, artifact)?,
            ArtifactKind::ExecutionContext => {
                Self::typed_observer_payload::<ExecutionContext>(store, artifact)?
            }
            ArtifactKind::ExecutionVerdict => {
                Self::typed_observer_payload::<ExecutionVerdict>(store, artifact)?
            }
            ArtifactKind::ExecutionPlan => {
                Self::typed_observer_payload::<ExecutionPlan>(store, artifact)?
            }
            ArtifactKind::OrderReceipt => {
                Self::typed_observer_payload::<OrderReceipt>(store, artifact)?
            }
            ArtifactKind::Reconciliation => {
                Self::typed_observer_payload::<Reconciliation>(store, artifact)?
            }
            ArtifactKind::OutcomeSchedule => {
                Self::typed_observer_payload::<OutcomeSchedule>(store, artifact)?
            }
            ArtifactKind::Outcome => Self::typed_observer_payload::<Outcome>(store, artifact)?,
            ArtifactKind::RetrospectiveDraft => {
                Self::typed_observer_payload::<RetrospectiveDraft>(store, artifact)?
            }
            ArtifactKind::Retrospective => {
                Self::typed_observer_payload::<Retrospective>(store, artifact)?
            }
            ArtifactKind::Experience => {
                Self::typed_observer_payload::<Experience>(store, artifact)?
            }
            ArtifactKind::Evaluation => {
                Self::typed_observer_payload::<Evaluation>(store, artifact)?
            }
            _ => return Ok(None),
        };
        let run_id = artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.clone());
        let (runtime_manifest, runtime_identity_hash) = if let Some(run_id) = &run_id {
            match store.paper_approval_for_run(run_id)? {
                Some((manifest, approval)) => (
                    Some(approval.runtime_manifest),
                    Some(manifest.runtime_identity_hash()?),
                ),
                None => (None, None),
            }
        } else {
            (None, None)
        };
        Ok(Some(ObserverArtifactView {
            artifact_id: artifact.artifact_id.clone(),
            kind: artifact.kind,
            created_at: artifact.created_at,
            run_id,
            runtime_manifest,
            runtime_identity_hash,
            payload,
        }))
    }

    fn typed_observer_payload<T>(store: &Store, artifact: &Artifact) -> Result<Value>
    where
        T: DeserializeOwned + Serialize,
    {
        // 泛型 T 同时约束反序列化和重新序列化，使 view 经过结构化 serde 解码而非
        // 直接透传 blob；此 helper 没调用 T 的领域 `validate()`，不能宣称业务有效。
        Ok(serde_json::to_value(serde_json::from_slice::<T>(
            &store.read_blob(&artifact.blob)?,
        )?)?)
    }
}

fn observer_run_telemetry(trajectory: &[TrajectoryEntry]) -> ObserverRunTelemetry {
    // 从轨迹中取最新 model/latency，只累计已有的 token 字段并用 checked_add 防溢出。
    // `filter_map` 会跳过缺失字段，全部缺失时 `try_fold` 返回 Some(0)：此值不是已核实的
    // 完整模型调用成本，UI 不应把它当完整账单。
    ObserverRunTelemetry {
        model_id: trajectory
            .iter()
            .rev()
            .find_map(|entry| entry.model.as_ref()?.model_id.clone()),
        latency_millis: trajectory
            .iter()
            .rev()
            .find_map(|entry| entry.latency_millis),
        input_tokens: trajectory
            .iter()
            .filter_map(|entry| entry.input_tokens)
            .try_fold(0_u64, u64::checked_add),
        output_tokens: trajectory
            .iter()
            .filter_map(|entry| entry.output_tokens)
            .try_fold(0_u64, u64::checked_add),
        tool_calls: trajectory
            .iter()
            .filter(|entry| entry.tool.is_some() && entry.event_type.contains("called"))
            .count(),
        turns: trajectory
            .iter()
            .filter(|entry| entry.turn.is_some() && entry.model.is_some())
            .count(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn fixture_daemon(model: ModelClient) -> Daemon {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/observer-report-tests")
            .join(RunId::new().0);
        Daemon::with_model(
            DaemonConfig {
                agent_budget: Default::default(),
                research_settings: Default::default(),
                outcome_processing: false,
                store_root: root,
                http_token: "observer-fixture-token".into(),
                worker_count: 1,
                auto_paper: false,
                market_data_feed: None,
                outcome_cost_model: akzio_domain::OutcomeCostModel {
                    transaction_cost_ppm: 0,
                    slippage_ppm: 0,
                },
                decision_policy: Default::default(),
                startup_decision_policy_hash: None,
                runtime_identity_hash: None,
                historical_evaluation_condition: None,
                model_knowledge_cutoff: None,
                run_log: None,
            },
            model,
        )
        .unwrap()
    }

    /// WHY THIS TEST EXISTS (Task #31): Observer must distinguish no evidence from neutral values and keep counterevidence and persisted provider usage in their proven scope.
    /// Do not weaken or delete this assertion.
    #[tokio::test]
    async fn operator_report_distinguishes_queued_data_from_a_committed_neutral_decision() {
        let akzio_model::ModelClient::FixtureByPurposePhase(templates) =
            crate::fixture_model_client()
        else {
            panic!("expected purpose-scoped research fixture responses");
        };
        let mut templates = (*templates).clone();
        for (recipe, templates) in &mut templates {
            if recipe != akzio_domain::RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID {
                templates[1]["usage"] = json!({
                    "input_tokens": 100,
                    "input_tokens_details": {"cached_tokens": 70},
                    "output_tokens": 40,
                    "output_tokens_details": {"reasoning_tokens": 30},
                });
            }
        }
        let mut daemon = fixture_daemon(crate::fixture_model_client());
        let run = daemon
            .launch_formal_run(
                RunPurpose::PositionPlan,
                Some(RunId::new()),
                None,
                "operator-report-fixture",
            )
            .await
            .unwrap()
            .run_id;
        let detail = |daemon: &Daemon| {
            serde_json::to_value(daemon.observer_run_detail(&run).unwrap()).unwrap()
        };
        let queued = detail(&daemon);
        let report = &queued["operator_report"];
        assert_eq!(report["version"], 1);
        assert_eq!(report["run_id"], run.0);
        assert_eq!(report["event_cursor"], queued["workflow"]["event_cursor"]);
        assert_eq!(report["forecasts"].as_array().unwrap().len(), 12);
        assert!(report["forecasts"].as_array().unwrap().iter().all(|slot| {
            slot["status"] == "unavailable" && slot["expected_return_ppm"].is_null()
        }));
        assert!(report["allocations"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| { row["formal_target_ppm"].is_null() && row["achieved_ppm"].is_null() }));
        assert_eq!(report["model_usage"]["coverage"], "not_observed");
        assert!(report["model_usage"]["input_tokens"].is_null());
        assert!(queued.get("research_audit").is_some());
        assert!(queued.get("telemetry").is_some());

        assert!(daemon.run_one("operator-report-fixture").await.unwrap());
        let evidence = daemon
            .store
            .run_artifacts_by_kind(&run, ArtifactKind::NormalizedEvidence)
            .unwrap();
        let find_bars = |prefix| {
            evidence
                .iter()
                .find(|artifact| {
                    let payload: Value =
                        serde_json::from_slice(&daemon.store.read_blob(&artifact.blob).unwrap())
                            .unwrap();
                    payload["resource"]
                        .as_str()
                        .is_some_and(|resource| resource.starts_with(prefix))
                })
                .unwrap()
        };
        let qqq_bars = find_bars("bars:QQQ:1d:");
        let unscoped_bars = find_bars("bars:SOXX:1d:");
        let qqq_ground = json!({
            "evidence": {"artifact_id": qqq_bars.artifact_id.0},
            "support": "This fixture reviews only QQQ.",
            "role": "descriptive",
            "assets": ["QQQ"],
            "domain": null,
        });
        let unscoped_ground = json!({
            "evidence": {"artifact_id": unscoped_bars.artifact_id.0},
            "support": "The legacy descriptive ground declares no asset scope.",
            "role": "descriptive",
            "assets": [],
            "domain": null,
        });
        let analyst = &mut templates
            .get_mut(akzio_domain::RESEARCH_ANALYST_RECIPE_ID)
            .unwrap()[1];
        let mut arguments: Value =
            serde_json::from_str(analyst["output"][0]["arguments"].as_str().unwrap()).unwrap();
        // Both grounds must reach the Critic: its bounded projection prioritizes
        // references from the parent Claim over optional background evidence.
        arguments["result"]["grounds"] = json!([qqq_ground, unscoped_ground]);
        arguments["result"]["evidence_gaps"] = json!([{
            "topic": "QQQ near-term evidence",
            "rationale": "The fixture has no qualified QQQ evidence at T1.",
            "impact": "blocks_directional_forecast",
            "assets": ["QQQ"],
            "horizons": ["t1"],
            "retriable": false,
            "supplemental_requests": []
        }]);
        analyst["output"][0]["arguments"] = json!(arguments.to_string());
        let critic = &mut templates
            .get_mut(akzio_domain::RESEARCH_CRITIC_RECIPE_ID)
            .unwrap()[1];
        let mut arguments: Value =
            serde_json::from_str(critic["output"][0]["arguments"].as_str().unwrap()).unwrap();
        arguments["result"]["grounds"] = json!([qqq_ground, unscoped_ground]);
        arguments["result"]["verification_status"] = json!("contradicted");
        arguments["result"]["conflicting_refs"] =
            json!([qqq_ground, unscoped_ground].map(|ground| json!({
                "evidence": ground["evidence"],
                "authority": "official",
                "temporal_validity": "valid_at_decision_cutoff",
            })));
        critic["output"][0]["arguments"] = json!(arguments.to_string());
        daemon.model =
            ModelClientAdapter::with_debug(ModelClient::fixture_by_purpose_phase(templates), false);

        assert!(daemon.run_one("operator-report-fixture").await.unwrap());
        let first_turn = detail(&daemon);
        for _ in 0..32 {
            if !daemon.run_one("operator-report-fixture").await.unwrap() {
                break;
            }
        }
        let completed = detail(&daemon);
        let report = &completed["operator_report"];
        let [decision]: [_; 1] = daemon
            .store
            .run_artifacts_by_kind(&run, ArtifactKind::Decision)
            .unwrap()
            .try_into()
            .unwrap();
        let persisted: Decision =
            serde_json::from_slice(&daemon.store.read_blob(&decision.blob).unwrap()).unwrap();
        assert_eq!(
            report["decision"]["artifact_id"],
            decision.artifact_id.0.as_str()
        );
        let forecasts = report["forecasts"].as_array().unwrap();
        assert_eq!(forecasts.len(), 12);
        for expected in persisted.forecasts {
            let found = forecasts
                .iter()
                .find(|slot| {
                    slot["asset"] == expected.asset.symbol()
                        && slot["horizon"] == serde_json::to_value(expected.horizon).unwrap()
                })
                .unwrap();
            assert_eq!(found["status"], "neutral");
            assert_eq!(found["expected_return_ppm"], expected.expected_return_ppm);
            assert_eq!(
                found["positive_return_probability_ppm"],
                expected.positive_return_probability_ppm
            );
            assert!(!found["evidence"].as_array().unwrap().is_empty());
            let coverage =
                if expected.asset == Asset::Qqq && expected.horizon == DecisionHorizon::T1 {
                    "blocked"
                } else {
                    "referenced"
                };
            assert_eq!(found["evidence_coverage"], coverage);
            let counterevidence = found["counterevidence"].as_array().unwrap();
            if expected.asset == Asset::Qqq {
                assert_eq!(counterevidence.len(), 1);
                let reference: ArtifactRef =
                    serde_json::from_value(counterevidence[0].clone()).unwrap();
                let critique: ResearchCritique = daemon.read_artifact_payload(&reference).unwrap();
                let claim: ResearchClaim = daemon.read_artifact_payload(&critique.target).unwrap();
                assert_eq!(claim.horizon, expected.horizon);
                assert!(critique
                    .conflicting_refs
                    .iter()
                    .any(|conflict| conflict.evidence.artifact_id == qqq_bars.artifact_id));
                assert!(critique
                    .conflicting_refs
                    .iter()
                    .any(|conflict| conflict.evidence.artifact_id == unscoped_bars.artifact_id));
                let horizon = found["horizon"].as_str().unwrap();
                assert_eq!(
                    report["counterevidence_without_asset_scope"][horizon],
                    found["counterevidence"]
                );
            } else {
                assert!(
                    counterevidence.is_empty(),
                    "{}: {found}",
                    expected.asset.symbol()
                );
            }
        }
        assert!(report["allocations"].as_array().unwrap().iter().all(|row| {
            row["research_validated_ppm"] == 0
                && row["formal_target_ppm"] == 0
                && row["execution_target_ppm"].is_null()
                && row["achieved_ppm"].is_null()
        }));
        assert_eq!(report["execution"]["status"], "not_applicable");
        let usage = &first_turn["operator_report"]["model_usage"];
        assert_eq!(usage["turns"], 1);
        assert_eq!(usage["coverage"], "complete");
        assert_eq!(usage["turns_with_usage"], 1);
        assert_eq!(usage["turns_missing_usage"], 0);
        assert_eq!(usage["input_tokens"], 100);
        assert_eq!(usage["output_tokens"], 40);
        assert_eq!(usage["source"], "store_agent_turn_journal");
        assert_eq!(usage["token_accounting_version"], 1);
        assert_eq!(usage["total_tokens"], 140);
        assert_eq!(
            first_turn["operator_report"]["event_cursor"],
            first_turn["workflow"]["event_cursor"]
        );

        let usage = &report["model_usage"];
        assert_eq!(usage["coverage"], "partial");
        assert!(usage["turns_with_usage"].as_u64().unwrap() > 0);
        assert_eq!(usage["turns_missing_usage"], 1);
        assert_eq!(
            usage["turns"].as_u64().unwrap(),
            usage["turns_with_usage"].as_u64().unwrap() + 1
        );
        for field in ["input_tokens", "output_tokens", "total_tokens"] {
            assert!(usage[field].is_null(), "{field}: {usage}");
        }
    }
}
