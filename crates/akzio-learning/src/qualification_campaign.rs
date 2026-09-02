//! Fixed-order orchestration for an offline model-qualification campaign.
//!
//! The injected executor owns the mechanics of producing each immutable
//! evaluation artifact. This module only enforces stage order and evidence
//! closure before delegating deterministic report assembly to `qualification`.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Display,
};

use akzio_domain::{ArtifactKind, ArtifactRef, ContentHash, ModelQualificationKey};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    run_offline_model_qualification, OfflineModelQualificationInput,
    OfflineModelQualificationResult, QualificationRunError, QualificationScenarioObservation,
    QualificationStage,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QualificationCampaignPlan {
    pub key: ModelQualificationKey,
    pub frozen_context_manifest: ArtifactRef,
    pub champion_snapshot: ContentHash,
    pub challenger_snapshot: ContentHash,
    pub rollback_snapshot: ContentHash,
    pub observed_rollback_snapshot: ContentHash,
    pub required_scenarios: BTreeSet<String>,
    pub approved_by: String,
    pub qualified_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualificationStageRequest {
    pub stage: QualificationStage,
    pub key: ModelQualificationKey,
    pub frozen_context_manifest: ArtifactRef,
    pub champion_snapshot: ContentHash,
    pub challenger_snapshot: ContentHash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualificationStageResult {
    pub evaluation: ArtifactRef,
    pub scenarios: Vec<QualificationScenarioObservation>,
}

pub trait QualificationStageExecutor {
    type Error: Display;

    fn execute_stage(
        &mut self,
        request: &QualificationStageRequest,
    ) -> Result<QualificationStageResult, Self::Error>;
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum QualificationCampaignError {
    #[error("invalid qualification campaign plan: {0}")]
    InvalidPlan(&'static str),
    #[error("qualification rollback snapshot does not match the observed rollback state")]
    RollbackMismatch,
    #[error("qualification stage {0:?} did not return scenario observations")]
    MissingStage(QualificationStage),
    #[error("qualification stage {stage:?} execution failed: {message}")]
    StageExecution {
        stage: QualificationStage,
        message: String,
    },
    #[error("qualification stage {stage:?} returned non-evaluation evidence")]
    InvalidEvaluation { stage: QualificationStage },
    #[error("qualification stage {stage:?} reused evidence {evidence:?}")]
    DuplicateEvidence {
        stage: QualificationStage,
        evidence: ArtifactRef,
    },
    #[error(
        "qualification stage {stage:?} returned observation {scenario_id} for {observed_stage:?}"
    )]
    ScenarioStageMismatch {
        stage: QualificationStage,
        scenario_id: String,
        observed_stage: QualificationStage,
    },
    #[error("qualification scenario id duplicated: {0}")]
    DuplicateScenario(String),
    #[error("critical qualification regression in {stage:?}: {scenario_id}")]
    CriticalRegression {
        stage: QualificationStage,
        scenario_id: String,
    },
    #[error("qualification stage {stage:?} rejected challenger scenario {scenario_id}")]
    StageRejected {
        stage: QualificationStage,
        scenario_id: String,
    },
    #[error(transparent)]
    Assembly(#[from] QualificationRunError),
}

pub fn run_model_qualification_campaign<E: QualificationStageExecutor>(
    plan: &QualificationCampaignPlan,
    executor: &mut E,
) -> Result<OfflineModelQualificationResult, QualificationCampaignError> {
    validate_plan(plan)?;

    let mut stage_evaluations = BTreeMap::new();
    let mut evidence = BTreeSet::from([plan.frozen_context_manifest.clone()]);
    let mut scenario_ids = BTreeSet::new();
    let mut scenarios = Vec::new();

    for stage in QualificationStage::ORDERED {
        let request = QualificationStageRequest {
            stage,
            key: plan.key.clone(),
            frozen_context_manifest: plan.frozen_context_manifest.clone(),
            champion_snapshot: plan.champion_snapshot.clone(),
            challenger_snapshot: plan.challenger_snapshot.clone(),
        };
        let result = executor.execute_stage(&request).map_err(|error| {
            QualificationCampaignError::StageExecution {
                stage,
                message: error.to_string(),
            }
        })?;

        validate_stage_result(stage, &result, &mut evidence, &mut scenario_ids)?;
        stage_evaluations.insert(stage, result.evaluation);
        scenarios.extend(result.scenarios);
    }

    let input = OfflineModelQualificationInput {
        key: plan.key.clone(),
        frozen_context_manifest: plan.frozen_context_manifest.clone(),
        champion_snapshot: plan.champion_snapshot.clone(),
        challenger_snapshot: plan.challenger_snapshot.clone(),
        rollback_snapshot: plan.rollback_snapshot.clone(),
        observed_rollback_snapshot: plan.observed_rollback_snapshot.clone(),
        required_scenarios: plan.required_scenarios.clone(),
        scenarios,
        receipts: Vec::new(),
        stage_evaluations,
        approved_by: plan.approved_by.clone(),
        qualified_at: plan.qualified_at,
        expires_at: plan.expires_at,
    };
    run_offline_model_qualification(&input).map_err(Into::into)
}

fn validate_plan(plan: &QualificationCampaignPlan) -> Result<(), QualificationCampaignError> {
    plan.key
        .validate()
        .map_err(|_| QualificationCampaignError::InvalidPlan("qualification_key"))?;
    if plan.frozen_context_manifest.kind != ArtifactKind::ContextManifest {
        return Err(QualificationCampaignError::InvalidPlan(
            "frozen_context_manifest",
        ));
    }
    if plan.rollback_snapshot != plan.observed_rollback_snapshot {
        return Err(QualificationCampaignError::RollbackMismatch);
    }
    if plan.required_scenarios.is_empty()
        || plan
            .required_scenarios
            .iter()
            .any(|scenario| scenario.trim().is_empty())
    {
        return Err(QualificationCampaignError::InvalidPlan(
            "required_scenarios",
        ));
    }
    if plan.approved_by.trim().is_empty() || plan.expires_at <= plan.qualified_at {
        return Err(QualificationCampaignError::InvalidPlan("approval_window"));
    }
    Ok(())
}

fn validate_stage_result(
    stage: QualificationStage,
    result: &QualificationStageResult,
    evidence: &mut BTreeSet<ArtifactRef>,
    scenario_ids: &mut BTreeSet<String>,
) -> Result<(), QualificationCampaignError> {
    if result.evaluation.kind != ArtifactKind::Evaluation {
        return Err(QualificationCampaignError::InvalidEvaluation { stage });
    }
    if !evidence.insert(result.evaluation.clone()) {
        return Err(QualificationCampaignError::DuplicateEvidence {
            stage,
            evidence: result.evaluation.clone(),
        });
    }
    if result.scenarios.is_empty() {
        return Err(QualificationCampaignError::MissingStage(stage));
    }
    for scenario in &result.scenarios {
        if scenario.stage != stage {
            return Err(QualificationCampaignError::ScenarioStageMismatch {
                stage,
                scenario_id: scenario.scenario_id.clone(),
                observed_stage: scenario.stage,
            });
        }
        if !scenario_ids.insert(scenario.scenario_id.clone()) {
            return Err(QualificationCampaignError::DuplicateScenario(
                scenario.scenario_id.clone(),
            ));
        }
        if scenario.critical && scenario.champion_passed && !scenario.challenger_passed {
            return Err(QualificationCampaignError::CriticalRegression {
                stage,
                scenario_id: scenario.scenario_id.clone(),
            });
        }
        if !scenario.challenger_passed {
            return Err(QualificationCampaignError::StageRejected {
                stage,
                scenario_id: scenario.scenario_id.clone(),
            });
        }
    }
    Ok(())
}
