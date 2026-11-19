//! Bounded read-only projection for a specific Run.

use super::*;
use akzio_domain::{
    DecisionProposal, Evaluation, Experience, Outcome, ResearchCritique, WorkflowProposalDraft,
};
use serde::de::DeserializeOwned;
use std::collections::BTreeSet;

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
        // detail 按 bounded event/trajectory/artifact limit 组装一次 Run 的审计视图；所有
        // Artifact payload 仍经过白名单类型解码。
        let workflow = self.store.workflow_snapshot(run_id)?;
        let events = self
            .store
            .recent_events(run_id, OBSERVER_EVENT_LIMIT)?
            .into_iter()
            .map(|event| EventView {
                cursor: event.cursor,
                event_type: event.event_type,
                task_id: event.task_id.map(|task| task.0),
                created_at: event.created_at.to_rfc3339(),
            })
            .collect();
        let trajectory = self
            .store
            .recent_trajectory(run_id, OBSERVER_TRAJECTORY_LIMIT)?;
        let artifacts = self.observer_artifacts(&trajectory)?;
        let telemetry = observer_run_telemetry(&trajectory);

        Ok(ObserverRunDetail {
            inspection: self.runtime_inspection(run_id)?,
            research_audit: self.store.research_audit(run_id)?,
            workflow,
            events,
            trajectory,
            artifacts,
            telemetry,
        })
    }

    fn observer_artifacts(
        &self,
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
            let artifact = self.store.artifact(artifact_id)?;
            if let Some(view) = self.observer_artifact_view(&artifact)? {
                artifacts.push(view);
            }
        }
        artifacts.sort_by_key(|artifact| artifact.created_at);
        Ok(artifacts)
    }

    fn observer_artifact_view(&self, artifact: &Artifact) -> Result<Option<ObserverArtifactView>> {
        // 只有白名单业务 Artifact kind 才反序列化为 observer payload；未知 kind 保持 None，
        // runtime_manifest/approval 另从对应绑定读取，避免把非公开原文泄露给 UI。
        let payload = match artifact.kind {
            ArtifactKind::WorkflowProposalDraft => {
                self.typed_observer_payload::<WorkflowProposalDraft>(artifact)?
            }
            ArtifactKind::Claim => self.typed_observer_payload::<ResearchClaim>(artifact)?,
            ArtifactKind::Critique => self.typed_observer_payload::<ResearchCritique>(artifact)?,
            ArtifactKind::DecisionProposal => {
                self.typed_observer_payload::<DecisionProposal>(artifact)?
            }
            ArtifactKind::DecisionContext => {
                self.typed_observer_payload::<DecisionContext>(artifact)?
            }
            ArtifactKind::Decision => self.typed_observer_payload::<Decision>(artifact)?,
            ArtifactKind::ExecutionContext => {
                self.typed_observer_payload::<ExecutionContext>(artifact)?
            }
            ArtifactKind::ExecutionVerdict => {
                self.typed_observer_payload::<ExecutionVerdict>(artifact)?
            }
            ArtifactKind::ExecutionPlan => {
                self.typed_observer_payload::<ExecutionPlan>(artifact)?
            }
            ArtifactKind::OrderReceipt => self.typed_observer_payload::<OrderReceipt>(artifact)?,
            ArtifactKind::Reconciliation => {
                self.typed_observer_payload::<Reconciliation>(artifact)?
            }
            ArtifactKind::OutcomeSchedule => {
                self.typed_observer_payload::<OutcomeSchedule>(artifact)?
            }
            ArtifactKind::Outcome => self.typed_observer_payload::<Outcome>(artifact)?,
            ArtifactKind::RetrospectiveDraft => {
                self.typed_observer_payload::<RetrospectiveDraft>(artifact)?
            }
            ArtifactKind::Retrospective => {
                self.typed_observer_payload::<Retrospective>(artifact)?
            }
            ArtifactKind::Experience => self.typed_observer_payload::<Experience>(artifact)?,
            ArtifactKind::Evaluation => self.typed_observer_payload::<Evaluation>(artifact)?,
            _ => return Ok(None),
        };
        let run_id = artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.clone());
        let (runtime_manifest, runtime_identity_hash) = if let Some(run_id) = &run_id {
            match self.store.paper_approval_for_run(run_id)? {
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

    fn typed_observer_payload<T>(&self, artifact: &Artifact) -> Result<Value>
    where
        T: DeserializeOwned + Serialize,
    {
        // 泛型 T 同时约束反序列化和重新序列化，使 view 经过结构化 serde 解码而非
        // 直接透传 blob；此 helper 没调用 T 的领域 `validate()`，不能宣称业务有效。
        Ok(serde_json::to_value(serde_json::from_slice::<T>(
            &self.store.read_blob(&artifact.blob)?,
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
