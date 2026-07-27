//! FileStore adapter for typed Phase 1 and Phase 3 domain tools.
//!
//! This is deliberately a narrow bridge: only profiles with a completed
//! FileStore builder are constructible here.  No SQL service, arbitrary file
//! operation, or legacy fallback is exposed to the LLM runtime.

use std::{collections::BTreeSet, path::Path, sync::Arc};

use anyhow::{bail, Context, Result};
use chrono::Utc;
use orchestrator_core::ToolManagedProfile;
use orchestrator_llm::tools::domain_tools::{
    AnalystAssessmentCommand, AnalystDataGapCommand, AnalystEvidenceCommand,
    AnalystInvalidationCommand, BindingRiskControlCommand, DomainToolRuntimeBinding,
    DomainToolScope, DomainToolService, PortfolioAssetDecisionCommand, ResearchDecisionCommand,
    ResearchHingeCommand, ResearchScenariosCommand, RiskAssessmentCommand, RiskConstraintsCommand,
    TradeBlockerCommand, TradeIntentCommand,
};
use orchestrator_store::{
    append_analyst_data_gap, append_analyst_evidence, append_research_hinge, content_hash,
    finalize_analyst_report, finalize_research_decision, set_analyst_assessment,
    set_analyst_invalidation, set_research_decision, set_research_scenarios,
    AnalystAssessmentInput, AnalystEvidenceInput, ArtifactScope, DomainFinalizeOutcome,
    DraftAppendOutcome, DraftProfile, FileStore, FileStoreOptions, FinalizeDraftOutcome,
    ResearchDecisionInput, ResearchScenarioInput, RunLocation,
};
use serde_json::{json, Value};

/// Create one strict runtime binding for a Phase 1 analyst or Phase 3
/// Research Manager unit.  Identity and source hash are constructed from the
/// workflow state before the model starts; neither is accepted from tools.
#[derive(Debug, Clone)]
pub(crate) struct FileStoreDomainRuntimePlan {
    pub role: String,
    pub phase: i64,
    pub profile: ToolManagedProfile,
    pub profile_version: u32,
    pub builder_version: u32,
    pub tickers: Vec<String>,
    pub visible_evidence_refs: BTreeSet<String>,
}

pub(crate) fn file_store_domain_runtime(
    store_root: &Path,
    state: &Value,
    plan: FileStoreDomainRuntimePlan,
) -> Result<DomainToolRuntimeBinding> {
    let draft_profile = match plan.profile {
        ToolManagedProfile::AnalystReport => DraftProfile::AnalystReport,
        ToolManagedProfile::ResearchDecision => DraftProfile::ResearchDecision,
        _ => bail!(
            "no FileStore domain adapter exists for {}",
            plan.profile.as_str()
        ),
    };
    let phase_u8 = u8::try_from(plan.phase).context("domain tool phase must fit in u8")?;
    let run_id = required_string(state, "run_id")?;
    let current_date = required_string(state, "current_date")?;
    if plan.tickers.is_empty() {
        bail!("FileStore domain runtime requires at least one ticker")
    }
    let source_payload_hash = content_hash(&json!({
        "run_id": run_id,
        "phase": phase_u8,
        "role": plan.role,
        "profile": plan.profile.as_str(),
        "tickers": plan.tickers,
        "source": source_payload_for(state, phase_u8, plan.profile),
    }))?;
    let unit_key = format!("phase{phase_u8}:{}:aggregate", plan.role);
    let scope = ArtifactScope {
        run_id: run_id.clone(),
        current_date: current_date.clone(),
        phase: phase_u8,
        role: plan.role,
        profile: draft_profile,
        profile_version: plan.profile_version,
        builder_version: plan.builder_version,
        unit_key,
        source_payload_hash,
        ticker: None,
        topic_id: None,
        side: None,
        stance: None,
        round: None,
        reflection_task: None,
    };
    let service = FileStoreDomainToolService {
        store: FileStore::open(store_root, FileStoreOptions::default())?,
        location: RunLocation::new(current_date, run_id)?,
        scope,
        expected_tickers: plan.tickers.clone(),
        created_at: Utc::now().to_rfc3339(),
    };
    DomainToolRuntimeBinding::new(
        DomainToolScope {
            profile: plan.profile,
            tickers: plan.tickers.into_iter().collect(),
            visible_evidence_refs: plan.visible_evidence_refs,
        },
        Arc::new(service),
    )
}

fn required_string(state: &Value, key: &str) -> Result<String> {
    state
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .with_context(|| format!("state.{key} is required for a FileStore domain runtime"))
}

fn source_payload_for(state: &Value, phase: u8, profile: ToolManagedProfile) -> Value {
    match (phase, profile) {
        (1, ToolManagedProfile::AnalystReport) => json!({
            "technical": state.get("technical"),
            "jin10": state.get("jin10"),
            "phase0": state.get("phase0"),
        }),
        (3, ToolManagedProfile::ResearchDecision) => json!({
            "phase1_index": state.get("phase1_index"),
            "debate_state_artifact": state.get("debate_state_artifact"),
            "phase2": state.get("phase2"),
        }),
        _ => Value::Null,
    }
}

#[derive(Debug)]
struct FileStoreDomainToolService {
    store: FileStore,
    location: RunLocation,
    scope: ArtifactScope,
    expected_tickers: Vec<String>,
    created_at: String,
}

impl FileStoreDomainToolService {
    fn require(&self, profile: DraftProfile) -> Result<()> {
        if self.scope.profile == profile {
            Ok(())
        } else {
            bail!(
                "{} domain tool is unavailable to {}",
                profile.as_str(),
                self.scope.profile.as_str()
            )
        }
    }

    fn append(outcome: DraftAppendOutcome) -> Value {
        match outcome {
            DraftAppendOutcome::Appended { draft, receipt } => json!({
                "status": "draft",
                "revision": draft.revision,
                "receipt": receipt.normalized_parameters_hash,
            }),
            DraftAppendOutcome::AlreadyApplied { draft, receipt } => json!({
                "status": "already_applied",
                "revision": draft.revision,
                "receipt": receipt.normalized_parameters_hash,
            }),
        }
    }

    fn finalized(&self, outcome: DomainFinalizeOutcome) -> Result<Value> {
        match outcome {
            DomainFinalizeOutcome::Analyst(outcome) => self.finalized_value(*outcome),
            DomainFinalizeOutcome::Research(outcome) => self.finalized_value(*outcome),
            _ => bail!("unexpected FileStore domain finalizer outcome"),
        }
    }

    fn finalized_value<T: serde::Serialize>(
        &self,
        outcome: FinalizeDraftOutcome<T>,
    ) -> Result<Value> {
        match outcome {
            FinalizeDraftOutcome::Completed { artifact, .. } => {
                serde_json::to_value(artifact).context("serialize finalized FileStore artifact")
            }
            FinalizeDraftOutcome::Recovered { artifact, .. } => self
                .store
                .read_json_value(&self.location.child_relative(&artifact.relative_path())?)
                .context("read recovered finalized FileStore artifact"),
        }
    }
}

impl DomainToolService for FileStoreDomainToolService {
    fn set_analyst_assessment(&self, command: AnalystAssessmentCommand) -> Result<Value> {
        self.require(DraftProfile::AnalystReport)?;
        Ok(Self::append(set_analyst_assessment(
            &self.store,
            &self.location,
            &self.scope,
            AnalystAssessmentInput {
                ticker: command.ticker,
                direction: command.direction,
                confidence: command.confidence,
                report: command.report,
                priced_in: command.priced_in,
                echo_chamber_risk: command.echo_chamber_risk,
                crowded_consensus_risk: command.crowded_consensus_risk,
            },
            &self.created_at,
        )?))
    }

    fn append_analyst_evidence(&self, command: AnalystEvidenceCommand) -> Result<Value> {
        self.require(DraftProfile::AnalystReport)?;
        Ok(Self::append(append_analyst_evidence(
            &self.store,
            &self.location,
            &self.scope,
            AnalystEvidenceInput {
                ticker: command.ticker,
                evidence: command.evidence,
                evidence_ref: command.evidence_ref,
            },
            &self.created_at,
        )?))
    }

    fn append_analyst_data_gap(&self, command: AnalystDataGapCommand) -> Result<Value> {
        self.require(DraftProfile::AnalystReport)?;
        Ok(Self::append(append_analyst_data_gap(
            &self.store,
            &self.location,
            &self.scope,
            command.ticker,
            command.data_gap,
            &self.created_at,
        )?))
    }

    fn set_analyst_invalidation(&self, command: AnalystInvalidationCommand) -> Result<Value> {
        self.require(DraftProfile::AnalystReport)?;
        Ok(Self::append(set_analyst_invalidation(
            &self.store,
            &self.location,
            &self.scope,
            command.ticker,
            command.validation_triggers,
            &self.created_at,
        )?))
    }

    fn finalize_analyst_report(&self) -> Result<Value> {
        self.require(DraftProfile::AnalystReport)?;
        self.finalized(finalize_analyst_report(
            &self.store,
            &self.location,
            &self.scope,
            &self.expected_tickers,
            &self.created_at,
        )?)
    }

    fn set_research_decision(&self, command: ResearchDecisionCommand) -> Result<Value> {
        self.require(DraftProfile::ResearchDecision)?;
        Ok(Self::append(set_research_decision(
            &self.store,
            &self.location,
            &self.scope,
            ResearchDecisionInput {
                ticker: command.ticker,
                rating: command.rating,
                long_probability: command.long_probability,
                short_probability: command.short_probability,
                confidence_basis: command.confidence_basis,
                hold_reason: command.hold_reason,
                plan: command.plan,
                probability_rationale: command.probability_rationale,
            },
            &self.created_at,
        )?))
    }

    fn set_research_scenarios(&self, command: ResearchScenariosCommand) -> Result<Value> {
        self.require(DraftProfile::ResearchDecision)?;
        Ok(Self::append(set_research_scenarios(
            &self.store,
            &self.location,
            &self.scope,
            ResearchScenarioInput {
                ticker: command.ticker,
                bull: command.bull,
                base: command.base,
                bear: command.bear,
            },
            &self.created_at,
        )?))
    }

    fn append_research_hinge(&self, command: ResearchHingeCommand) -> Result<Value> {
        self.require(DraftProfile::ResearchDecision)?;
        Ok(Self::append(append_research_hinge(
            &self.store,
            &self.location,
            &self.scope,
            command.ticker,
            command.hinge,
            command.evidence_ref,
            &self.created_at,
        )?))
    }

    fn finalize_research_decision(&self) -> Result<Value> {
        self.require(DraftProfile::ResearchDecision)?;
        self.finalized(finalize_research_decision(
            &self.store,
            &self.location,
            &self.scope,
            &self.expected_tickers,
            &self.created_at,
        )?)
    }

    fn set_trade_intent(&self, _: TradeIntentCommand) -> Result<Value> {
        bail!("trade domain runtime is not wired")
    }
    fn append_trade_blocker(&self, _: TradeBlockerCommand) -> Result<Value> {
        bail!("trade domain runtime is not wired")
    }
    fn finalize_trade_intent(&self) -> Result<Value> {
        bail!("trade domain runtime is not wired")
    }
    fn set_risk_assessment(&self, _: RiskAssessmentCommand) -> Result<Value> {
        bail!("risk domain runtime is not wired")
    }
    fn set_risk_constraints(&self, _: RiskConstraintsCommand) -> Result<Value> {
        bail!("risk domain runtime is not wired")
    }
    fn finalize_risk_review(&self) -> Result<Value> {
        bail!("risk domain runtime is not wired")
    }
    fn set_portfolio_asset_decision(&self, _: PortfolioAssetDecisionCommand) -> Result<Value> {
        bail!("portfolio domain runtime is not wired")
    }
    fn append_binding_risk_control(&self, _: BindingRiskControlCommand) -> Result<Value> {
        bail!("portfolio domain runtime is not wired")
    }
    fn finalize_portfolio_decision(&self) -> Result<Value> {
        bail!("portfolio domain runtime is not wired")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_llm::tools::domain_tools::{
        APPEND_ANALYST_EVIDENCE, FINALIZE_ANALYST_REPORT, SET_ANALYST_ASSESSMENT,
        SET_ANALYST_INVALIDATION,
    };

    fn state() -> Value {
        json!({
            "run_id": "run-domain-test",
            "current_date": "2026-07-27",
            "technical": {"source": "test"},
        })
    }

    #[test]
    fn analyst_binding_finalizes_and_recovers_a_canonical_file_store_artifact() {
        let temp = tempfile::tempdir().unwrap();
        let ticker = "QQQ".to_owned();
        let binding = file_store_domain_runtime(
            temp.path(),
            &state(),
            FileStoreDomainRuntimePlan {
                role: "analyst.technical".to_owned(),
                phase: 1,
                profile: ToolManagedProfile::AnalystReport,
                profile_version: 1,
                builder_version: 1,
                tickers: vec![ticker],
                visible_evidence_refs: ["evidence:test:QQQ".to_owned()].into_iter().collect(),
            },
        )
        .unwrap();
        binding
            .execute(
                SET_ANALYST_ASSESSMENT,
                json!({
                    "ticker":"QQQ", "direction":"neutral", "confidence":0.5,
                    "report":"test report", "priced_in":"unclear",
                    "echo_chamber_risk":"low", "crowded_consensus_risk":"low"
                }),
            )
            .unwrap();
        binding
            .execute(
                APPEND_ANALYST_EVIDENCE,
                json!({
                    "ticker":"QQQ", "evidence_ref":"evidence:test:QQQ",
                    "evidence": {
                        "claim":"test evidence", "evidence_type":"fact", "source":"test source",
                        "timestamp":"2026-07-27", "source_tier":"official", "first_source":"test source",
                        "is_derivative_repost":false, "evidence_age":"0-2d", "source_confidence":0.9
                    }
                }),
            )
            .unwrap();
        binding
            .execute(
                SET_ANALYST_INVALIDATION,
                json!({"ticker":"QQQ", "validation_triggers":["test invalidation"]}),
            )
            .unwrap();
        let finalized = binding.execute(FINALIZE_ANALYST_REPORT, json!({})).unwrap();
        assert_eq!(finalized["terminal"], true);
        assert_eq!(finalized["artifact"]["role"], "analyst.technical");
        assert_eq!(
            finalized["artifact"]["per_ticker"]["QQQ"]["report"],
            "test report"
        );
        let recovered = binding.execute(FINALIZE_ANALYST_REPORT, json!({})).unwrap();
        assert_eq!(recovered["artifact"], finalized["artifact"]);
        let artifacts = temp
            .path()
            .join("runs/2026-07-27")
            .read_dir()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path()
            .join("artifacts/phase1");
        assert!(artifacts.is_dir());
    }
}
