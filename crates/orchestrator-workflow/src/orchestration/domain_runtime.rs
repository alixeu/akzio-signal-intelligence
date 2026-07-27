//! FileStore adapter for typed Phase 1 and Phase 3 domain tools.
//!
//! This is deliberately a narrow bridge: only profiles with a completed
//! FileStore builder are constructible here.  No SQL service, arbitrary file
//! operation, or legacy fallback is exposed to the LLM runtime.

use std::{
    collections::BTreeSet,
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::{bail, Context, Result};
use chrono::Utc;
use orchestrator_core::ToolManagedProfile;
use orchestrator_llm::tools::domain_tools::{
    AnalystAssessmentCommand, AnalystDataGapCommand, AnalystEvidenceCommand,
    AnalystInvalidationCommand, BindingRiskControlCommand, ClaimStatusCommand, DebateClaimCommand,
    DebateResponseCommand, DebateSteerCommand, DomainToolRuntimeBinding, DomainToolScope,
    DomainToolService, EvidenceReadRecord, EvidenceVisibility, Phase2CommonGroundCommand,
    Phase2TopicCommand, PortfolioAssetDecisionCommand, ResearchDecisionCommand,
    ResearchHingeCommand, ResearchScenariosCommand, RiskAssessmentCommand, RiskConstraintsCommand,
    TextCommand, TopicSoftControlCommand, TradeBlockerCommand, TradeIntentCommand,
};
use orchestrator_store::{
    append_analyst_data_gap, append_analyst_evidence, append_research_hinge, append_session_event,
    content_hash, finalize_analyst_report, finalize_research_decision, read_session_events,
    read_session_manifest, set_analyst_assessment, set_analyst_invalidation, set_research_decision,
    set_research_scenarios, write_session_manifest, AnalystAssessmentInput, AnalystEvidenceInput,
    ArtifactScope, ClaimStatus, DomainFinalizeOutcome, DraftAppendOutcome, DraftProfile,
    EvidenceReadEvent, FileStore, FileStoreOptions, FinalizeDraftOutcome, ForkReference,
    Phase2DraftService, ResearchDecisionInput, ResearchScenarioInput, RunLocation,
    SessionEventInput, SessionEventType, SessionLocation, SessionManifest, VisibleEvidenceSet,
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
    pub topic_id: Option<String>,
    pub side: Option<String>,
    pub round: Option<u32>,
    pub visible_claims: BTreeSet<String>,
    pub fork: Option<ForkReference>,
}

pub(crate) fn file_store_domain_runtime(
    store_root: &Path,
    state: &Value,
    plan: FileStoreDomainRuntimePlan,
) -> Result<DomainToolRuntimeBinding> {
    let draft_profile = match plan.profile {
        ToolManagedProfile::AnalystReport => DraftProfile::AnalystReport,
        ToolManagedProfile::ResearcherWarmup => DraftProfile::ResearcherWarmup,
        ToolManagedProfile::TopicGeneration => DraftProfile::TopicGeneration,
        ToolManagedProfile::DebateSeed => DraftProfile::DebateSeed,
        ToolManagedProfile::DebateResponse => DraftProfile::DebateResponse,
        ToolManagedProfile::TopicControl => DraftProfile::TopicControl,
        ToolManagedProfile::ResearchDecision => DraftProfile::ResearchDecision,
        _ => bail!(
            "no FileStore domain adapter exists for {}",
            plan.profile.as_str()
        ),
    };
    let phase_u8 = u8::try_from(plan.phase).context("domain tool phase must fit in u8")?;
    let run_id = required_string(state, "run_id")?;
    let current_date = required_string(state, "current_date")?;
    if plan.tickers.is_empty()
        && !matches!(
            plan.profile,
            ToolManagedProfile::ResearcherWarmup
                | ToolManagedProfile::TopicGeneration
                | ToolManagedProfile::DebateSeed
                | ToolManagedProfile::DebateResponse
                | ToolManagedProfile::TopicControl
        )
    {
        bail!("FileStore domain runtime requires at least one ticker")
    }
    if plan.profile == ToolManagedProfile::AnalystReport && plan.tickers.len() != 1 {
        bail!("FileStore AnalystReport runtime requires exactly one Rust-planned ticker unit")
    }
    let source_payload_hash = content_hash(&json!({
        "run_id": run_id,
        "phase": phase_u8,
        "role": plan.role,
        "profile": plan.profile.as_str(),
        "tickers": plan.tickers,
        "topic_id": plan.topic_id,
        "side": plan.side,
        "round": plan.round,
        "source": source_payload_for(state, phase_u8, plan.profile),
    }))?;
    let unit_key = if plan.profile == ToolManagedProfile::AnalystReport {
        format!(
            "phase{phase_u8}:{}:ticker:{}",
            plan.role,
            plan.tickers
                .first()
                .expect("checked AnalystReport ticker unit")
        )
    } else if matches!(
        plan.profile,
        ToolManagedProfile::ResearcherWarmup | ToolManagedProfile::TopicGeneration
    ) {
        format!("phase{phase_u8}:{}:{}", plan.role, plan.profile.as_str())
    } else if let Some(topic_id) = plan.topic_id.as_deref() {
        format!(
            "phase{phase_u8}:{}:topic:{topic_id}:round:{}",
            plan.role,
            plan.round.unwrap_or(0)
        )
    } else {
        format!("phase{phase_u8}:{}:aggregate", plan.role)
    };
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
        ticker: (plan.profile == ToolManagedProfile::AnalystReport)
            .then(|| plan.tickers[0].clone()),
        topic_id: plan.topic_id.clone(),
        side: plan.side.clone(),
        stance: None,
        round: plan.round,
        reflection_task: None,
    };
    let store = FileStore::open(store_root, FileStoreOptions::default())?;
    let location = RunLocation::new(current_date, run_id)?;
    let evidence_visibility = Arc::new(FileStoreEvidenceVisibility::new(
        store.clone(),
        location.clone(),
        scope.role.clone(),
        phase_u8,
        plan.profile.as_str().to_owned(),
        plan.tickers.clone().into_iter().collect(),
        plan.fork.clone(),
    ));
    let service = FileStoreDomainToolService {
        store,
        location,
        scope,
        expected_tickers: plan.tickers.clone(),
        created_at: Utc::now().to_rfc3339(),
        visible_claims: plan.visible_claims.clone(),
        visible_evidence_refs: plan.visible_evidence_refs.clone(),
    };
    DomainToolRuntimeBinding::new_with_evidence_visibility(
        DomainToolScope {
            profile: plan.profile,
            tickers: plan.tickers.into_iter().collect(),
            visible_evidence_refs: plan.visible_evidence_refs,
        },
        Arc::new(service),
        evidence_visibility,
    )
}

/// Rust-owned degraded policy for a migrated Phase 1 unit.  It deliberately
/// follows the same typed Draft and terminal finalizer path as a successful
/// ToolManaged role; it is not a legacy JSON/SQLite fallback.  The synthetic
/// evidence is explicitly marked as a runtime failure record so downstream
/// reducers retain an honest zero-confidence, unobserved assessment.
pub(crate) fn finalize_degraded_analyst_report(
    store_root: &Path,
    state: &Value,
    mut plan: FileStoreDomainRuntimePlan,
    failure: &str,
) -> Result<Value> {
    if plan.profile != ToolManagedProfile::AnalystReport || plan.tickers.len() != 1 {
        bail!("degraded FileStore writer only supports one AnalystReport ticker unit")
    }
    let ticker = plan.tickers[0].clone();
    let evidence_ref = format!("runtime:degraded:{}:{ticker}", plan.role);
    plan.visible_evidence_refs.insert(evidence_ref.clone());
    let binding = file_store_domain_runtime(store_root, state, plan.clone())?;
    let current_date = required_string(state, "current_date")?;
    binding.execute(
        "set_analyst_assessment",
        json!({
            "ticker": ticker,
            "direction": "unobserved",
            "confidence": 0.0,
            "report": format!("{} did not produce usable evidence: {failure}", plan.role),
            "priced_in": "unclear",
            "echo_chamber_risk": "low",
            "crowded_consensus_risk": "low",
        }),
    )?;
    binding.execute(
        "append_analyst_evidence",
        json!({
            "ticker": ticker,
            "evidence_ref": evidence_ref,
            "evidence": {
                "claim": format!("{} failed before producing a usable assessment.", plan.role),
                "evidence_type": "inference",
                "source": "orchestrator runtime degraded policy",
                "timestamp": current_date,
                "source_tier": "unknown",
                "first_source": "orchestrator runtime degraded policy",
                "is_derivative_repost": false,
                "evidence_age": "unknown",
                "source_confidence": 0.0,
            }
        }),
    )?;
    binding.execute(
        "append_analyst_data_gap",
        json!({
            "ticker": ticker,
            "data_gap": format!("{} degraded: {failure}", plan.role),
        }),
    )?;
    binding.execute(
        "set_analyst_invalidation",
        json!({
            "ticker": ticker,
            "validation_triggers": ["Obtain a completed source-backed analyst assessment."],
        }),
    )?;
    binding
        .execute("finalize_analyst_report", json!({}))?
        .get("artifact")
        .cloned()
        .context("degraded FileStore analyst finalizer did not return an artifact")
}

/// Typed degraded policy for a migrated Phase 3 unit.  This deliberately
/// finalizes the same Draft that a successful Research Manager would use; it
/// must never fall through to the legacy JSON/SQLite degraded artifact.
pub(crate) fn finalize_degraded_research_decision(
    store_root: &Path,
    state: &Value,
    mut plan: FileStoreDomainRuntimePlan,
    failure: &str,
) -> Result<Value> {
    if plan.profile != ToolManagedProfile::ResearchDecision || plan.tickers.is_empty() {
        bail!("degraded FileStore writer requires a ResearchDecision ticker unit")
    }
    let evidence_ref = format!("runtime:degraded:{}", plan.role);
    plan.visible_evidence_refs.insert(evidence_ref.clone());
    let binding = file_store_domain_runtime(store_root, state, plan.clone())?;
    for ticker in &plan.tickers {
        binding.execute(
            "set_research_decision",
            json!({
                "ticker": ticker,
                "rating": "Hold",
                "long_probability": 0.5,
                "short_probability": 0.5,
                "confidence_basis": "runtime_degraded",
                "hold_reason": "runtime_degraded",
                "plan": format!("{} did not produce a usable research decision: {failure}", plan.role),
                "probability_rationale": "Runtime failure; probabilities are neutral placeholders and must not be treated as evidence.",
            }),
        )?;
        binding.execute(
            "set_research_scenarios",
            json!({
                "ticker": ticker,
                "bull": {"probability": 0.25, "drivers": ["No usable model output."], "triggers": ["Obtain a completed source-backed research decision."], "confirmation": "unavailable"},
                "base": {"probability": 0.50, "drivers": ["Runtime degraded."], "triggers": ["Obtain a completed source-backed research decision."], "confirmation": "unavailable"},
                "bear": {"probability": 0.25, "drivers": ["No usable model output."], "triggers": ["Obtain a completed source-backed research decision."], "confirmation": "unavailable"},
            }),
        )?;
        binding.execute(
            "append_research_hinge",
            json!({
                "ticker": ticker,
                "hinge": format!("{} degraded before a usable decision: {failure}", plan.role),
                "evidence_ref": evidence_ref,
            }),
        )?;
    }
    binding
        .execute("finalize_research_decision", json!({}))?
        .get("artifact")
        .cloned()
        .context("degraded FileStore research finalizer did not return an artifact")
}

/// FileStore-backed evidence visibility.  The model cannot call this type:
/// `ProjectToolRuntime` records a read only after the reader returns a
/// structured result, and domain writers query the same session immediately.
#[derive(Debug)]
struct FileStoreEvidenceVisibility {
    store: FileStore,
    run: RunLocation,
    role: String,
    phase: u8,
    profile: String,
    expected_tickers: BTreeSet<String>,
    fork: Option<ForkReference>,
    current_session_id: Mutex<Option<String>>,
}

impl FileStoreEvidenceVisibility {
    fn new(
        store: FileStore,
        run: RunLocation,
        role: String,
        phase: u8,
        profile: String,
        expected_tickers: BTreeSet<String>,
        fork: Option<ForkReference>,
    ) -> Self {
        Self {
            store,
            run,
            role,
            phase,
            profile,
            expected_tickers,
            fork,
            current_session_id: Mutex::new(None),
        }
    }

    fn location(&self, session_id: &str) -> Result<SessionLocation> {
        SessionLocation::new(self.run.clone(), session_id).map_err(anyhow::Error::from)
    }

    fn ensure_session(&self, location: &SessionLocation) -> Result<SessionManifest> {
        if self
            .store
            .root()
            .join(location.manifest_relative())
            .exists()
        {
            let manifest = read_session_manifest(&self.store, location)?;
            if manifest.role != self.role
                || manifest.phase != self.phase
                || manifest.profile != self.profile
            {
                bail!("FileStore session manifest differs from DomainTool scope")
            }
            return Ok(manifest);
        }
        let manifest = SessionManifest::new(
            location,
            self.role.clone(),
            self.phase,
            self.profile.clone(),
            self.fork.clone(),
            Utc::now().to_rfc3339(),
        )?;
        Ok(write_session_manifest(&self.store, location, manifest)?)
    }

    fn visible_for(&self, location: &SessionLocation) -> Result<VisibleEvidenceSet> {
        let manifest = self.ensure_session(location)?;
        let inherited = if let Some(fork) = manifest.fork.as_ref() {
            let parent = self.location(&fork.fork_from_session_id)?;
            let parent_events = read_session_events(&self.store, &parent)?;
            let cutoff = parent_events
                .iter()
                .filter(|event| event.turn_id == fork.fork_from_turn_id)
                .map(|event| event.created_at.as_str())
                .max()
                .with_context(|| {
                    format!(
                        "fork parent turn `{}` has no persisted session events",
                        fork.fork_from_turn_id
                    )
                })?
                .to_owned();
            VisibleEvidenceSet::from_events(
                parent_events
                    .into_iter()
                    .filter(|event| event.created_at.as_str() <= cutoff.as_str()),
            )?
        } else {
            VisibleEvidenceSet::default()
        };
        VisibleEvidenceSet::from_parent_and_events(
            inherited,
            read_session_events(&self.store, location)?,
        )
        .map_err(anyhow::Error::from)
    }
}

impl EvidenceVisibility for FileStoreEvidenceVisibility {
    fn set_turn_context(
        &self,
        context: &orchestrator_llm::agent_loop::ToolRuntimeTurnContext,
    ) -> Result<()> {
        if context.run_id != self.run.run_id
            || context.role != self.role
            || context.phase != Some(i64::from(self.phase))
        {
            bail!("turn context does not match FileStore DomainTool scope")
        }
        let location = self.location(&context.session_id)?;
        self.ensure_session(&location)?;
        *self
            .current_session_id
            .lock()
            .map_err(|_| anyhow::anyhow!("FileStore evidence visibility lock poisoned"))? =
            Some(context.session_id.clone());
        Ok(())
    }

    fn record_evidence_read(&self, read: EvidenceReadRecord) -> Result<()> {
        read.validate()?;
        if read.source_run_id != self.run.run_id {
            bail!("evidence_read cannot reference a different run")
        }
        if read.source_phase > self.phase {
            bail!("evidence_read cannot reference a future phase")
        }
        if let Some(ticker) = read.ticker.as_deref() {
            if !self.expected_tickers.contains(ticker) {
                bail!("evidence_read ticker is outside the Rust-owned scope")
            }
        }
        let active_session = self
            .current_session_id
            .lock()
            .map_err(|_| anyhow::anyhow!("FileStore evidence visibility lock poisoned"))?
            .clone()
            .context("evidence_read arrived before turn context")?;
        if active_session != read.session_id {
            bail!("evidence_read session differs from the active turn")
        }
        let location = self.location(&read.session_id)?;
        let manifest = self.ensure_session(&location)?;
        let payload = serde_json::to_value(EvidenceReadEvent {
            tool_name: read.tool_name,
            subject_kind: read.subject_kind,
            subject_id: read.subject_id,
            source_run_id: read.source_run_id,
            source_phase: read.source_phase,
            ticker: read.ticker,
            topic_id: read.topic_id,
            turn_id: read.turn_id.clone(),
            session_id: read.session_id,
        })?;
        append_session_event(
            &self.store,
            &location,
            &manifest,
            SessionEventInput {
                event_type: SessionEventType::EvidenceRead,
                turn_id: read.turn_id,
                payload,
                created_at: Utc::now().to_rfc3339(),
            },
        )?;
        Ok(())
    }

    fn contains(&self, reference: &str) -> Result<bool> {
        let session_id = self
            .current_session_id
            .lock()
            .map_err(|_| anyhow::anyhow!("FileStore evidence visibility lock poisoned"))?
            .clone()
            .context("domain evidence check arrived before turn context")?;
        Ok(self
            .visible_for(&self.location(&session_id)?)?
            .contains(reference))
    }
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
        (2, ToolManagedProfile::ResearcherWarmup) => json!({
            "phase1_index": state.get("phase1_index"),
            "phase_summaries": state.get("phase_summary_memory"),
        }),
        (2, ToolManagedProfile::TopicGeneration) => json!({
            "phase1_index": state.get("phase1_index"),
            "phase_summaries": state.get("phase_summary_memory"),
        }),
        (
            2,
            ToolManagedProfile::DebateSeed
            | ToolManagedProfile::DebateResponse
            | ToolManagedProfile::TopicControl,
        ) => json!({
            "topic": state.get("topic"),
            "phase2_warmup": state.get("phase2_warmup"),
            "topic_generation": state.get("topic_generation_artifact"),
            "topic_state": state.get("topic_debate_states"),
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
    visible_claims: BTreeSet<String>,
    visible_evidence_refs: BTreeSet<String>,
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

    fn phase2(&self) -> Result<Phase2DraftService> {
        Phase2DraftService::new(
            self.store.clone(),
            self.location.clone(),
            self.scope.clone(),
            self.created_at.clone(),
            self.visible_evidence_refs.iter().cloned(),
            self.visible_claims.iter().cloned(),
        )
        .map_err(Into::into)
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

    fn set_phase2_common_ground(&self, command: Phase2CommonGroundCommand) -> Result<Value> {
        self.require(DraftProfile::TopicGeneration)?;
        Ok(Self::append(
            self.phase2()?
                .set_phase2_common_ground(command.common_ground)?,
        ))
    }

    fn create_phase2_topic(&self, command: Phase2TopicCommand) -> Result<Value> {
        self.require(DraftProfile::TopicGeneration)?;
        let (topic_id, outcome) = self.phase2()?.create_phase2_topic(
            command.topic,
            command.decision_hinge,
            command.evidence_refs,
        )?;
        let mut value = Self::append(outcome);
        value["topic_id"] = json!(topic_id);
        Ok(value)
    }

    fn finalize_researcher_warmup(&self) -> Result<Value> {
        self.require(DraftProfile::ResearcherWarmup)?;
        Ok(Self::append(self.phase2()?.finalize_researcher_warmup()?))
    }

    fn finalize_topic_generation(&self) -> Result<Value> {
        self.require(DraftProfile::TopicGeneration)?;
        serde_json::to_value(self.phase2()?.finalize_topic_generation()?)
            .context("serialize phase2 topic generation artifact")
    }

    fn create_debate_claim(&self, command: DebateClaimCommand) -> Result<Value> {
        self.require(DraftProfile::DebateSeed)?;
        let (claim_id, outcome) = self.phase2()?.create_debate_claim(
            command.claim,
            command.confidence,
            command.evidence_refs,
        )?;
        let mut value = Self::append(outcome);
        value["claim_id"] = json!(claim_id);
        Ok(value)
    }

    fn finalize_debate_seed(&self) -> Result<Value> {
        self.require(DraftProfile::DebateSeed)?;
        serde_json::to_value(self.phase2()?.finalize_debate_seed()?)
            .context("serialize phase2 debate seed artifact")
    }

    fn respond_to_debate_claim(&self, command: DebateResponseCommand) -> Result<Value> {
        self.require(DraftProfile::DebateResponse)?;
        let (response_id, outcome) = self.phase2()?.respond_to_debate_claim(
            command.reply_to_claim_id,
            command.response,
            command.evidence_refs,
        )?;
        let mut value = Self::append(outcome);
        value["response_id"] = json!(response_id);
        Ok(value)
    }

    fn finalize_debate_response(&self) -> Result<Value> {
        self.require(DraftProfile::DebateResponse)?;
        serde_json::to_value(self.phase2()?.finalize_debate_response()?)
            .context("serialize phase2 debate response artifact")
    }

    fn set_claim_status(&self, command: ClaimStatusCommand) -> Result<Value> {
        self.require(DraftProfile::TopicControl)?;
        let status = match command.status.as_str() {
            "accepted" => ClaimStatus::Accepted,
            "rejected" => ClaimStatus::Rejected,
            "unresolved" => ClaimStatus::Unresolved,
            "blocked" => ClaimStatus::Blocked,
            _ => bail!("invalid Phase 2 claim status"),
        };
        Ok(Self::append(
            self.phase2()?.set_claim_status(command.claim_id, status)?,
        ))
    }

    fn add_agreed_fact(&self, command: TextCommand) -> Result<Value> {
        self.require(DraftProfile::TopicControl)?;
        Ok(Self::append(self.phase2()?.add_agreed_fact(command.value)?))
    }

    fn set_decision_hinge(&self, command: TextCommand) -> Result<Value> {
        self.require(DraftProfile::TopicControl)?;
        Ok(Self::append(
            self.phase2()?.set_decision_hinge(command.value)?,
        ))
    }

    fn route_debate_steer(&self, command: DebateSteerCommand) -> Result<Value> {
        self.require(DraftProfile::TopicControl)?;
        Ok(Self::append(
            self.phase2()?
                .route_debate_steer(command.target, command.instruction)?,
        ))
    }

    fn set_topic_soft_control(&self, command: TopicSoftControlCommand) -> Result<Value> {
        self.require(DraftProfile::TopicControl)?;
        Ok(Self::append(
            self.phase2()?
                .set_topic_soft_control(command.should_continue)?,
        ))
    }

    fn finalize_topic_control(&self) -> Result<Value> {
        self.require(DraftProfile::TopicControl)?;
        serde_json::to_value(self.phase2()?.finalize_topic_control()?)
            .context("serialize phase2 topic control artifact")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_llm::tools::domain_tools::{
        EvidenceReadRecord, ADD_AGREED_FACT, APPEND_ANALYST_EVIDENCE, CREATE_DEBATE_CLAIM,
        CREATE_PHASE2_TOPIC, FINALIZE_ANALYST_REPORT, FINALIZE_DEBATE_RESPONSE,
        FINALIZE_DEBATE_SEED, FINALIZE_RESEARCHER_WARMUP, FINALIZE_TOPIC_CONTROL,
        FINALIZE_TOPIC_GENERATION, RESPOND_TO_DEBATE_CLAIM, SET_ANALYST_ASSESSMENT,
        SET_ANALYST_INVALIDATION, SET_CLAIM_STATUS, SET_DECISION_HINGE, SET_PHASE2_COMMON_GROUND,
        SET_TOPIC_SOFT_CONTROL,
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
                topic_id: None,
                side: None,
                round: None,
                visible_claims: Default::default(),
                fork: None,
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

    #[test]
    fn file_store_visibility_persists_successful_reads_and_unlocks_same_turn_writes() {
        let temp = tempfile::tempdir().unwrap();
        let binding = file_store_domain_runtime(
            temp.path(),
            &state(),
            FileStoreDomainRuntimePlan {
                role: "analyst.technical".to_owned(),
                phase: 1,
                profile: ToolManagedProfile::AnalystReport,
                profile_version: 1,
                builder_version: 1,
                tickers: vec!["QQQ".to_owned()],
                visible_evidence_refs: Default::default(),
                topic_id: None,
                side: None,
                round: None,
                visible_claims: Default::default(),
                fork: None,
            },
        )
        .unwrap();
        binding
            .set_turn_context(&orchestrator_llm::agent_loop::ToolRuntimeTurnContext {
                run_id: "run-domain-test".to_owned(),
                session_id: "session-domain-test".to_owned(),
                turn_id: "turn-domain-test".to_owned(),
                role: "analyst.technical".to_owned(),
                phase: Some(1),
            })
            .unwrap();
        let command = || {
            json!({
                "ticker":"QQQ", "evidence_ref":"technical:QQQ:daily",
                "evidence": {
                    "claim":"support held", "evidence_type":"fact", "source":"technical",
                    "timestamp":"2026-07-27", "source_tier":"official", "first_source":"technical",
                    "is_derivative_repost":false, "evidence_age":"0-2d", "source_confidence":0.9
                }
            })
        };
        assert!(binding.execute(APPEND_ANALYST_EVIDENCE, command()).is_err());
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
            .record_evidence_read(EvidenceReadRecord {
                tool_name: "read_technical_snapshot".to_owned(),
                subject_kind: "technical_signal".to_owned(),
                subject_id: "technical:QQQ:daily".to_owned(),
                source_run_id: "run-domain-test".to_owned(),
                source_phase: 1,
                ticker: Some("QQQ".to_owned()),
                topic_id: None,
                turn_id: "turn-domain-test".to_owned(),
                session_id: "session-domain-test".to_owned(),
            })
            .unwrap();
        binding.execute(APPEND_ANALYST_EVIDENCE, command()).unwrap();

        let store = FileStore::open(temp.path(), FileStoreOptions::default()).unwrap();
        let session = SessionLocation::new(
            RunLocation::new("2026-07-27", "run-domain-test").unwrap(),
            "session-domain-test",
        )
        .unwrap();
        let events = read_session_events(&store, &session).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, SessionEventType::EvidenceRead);
    }

    fn phase2_plan(
        role: &str,
        profile: ToolManagedProfile,
        topic_id: Option<&str>,
        side: Option<&str>,
        round: Option<u32>,
        visible_claims: BTreeSet<String>,
    ) -> FileStoreDomainRuntimePlan {
        FileStoreDomainRuntimePlan {
            role: role.to_owned(),
            phase: 2,
            profile,
            profile_version: 1,
            builder_version: 1,
            tickers: vec![],
            visible_evidence_refs: ["evidence:phase1".to_owned()].into_iter().collect(),
            topic_id: topic_id.map(ToOwned::to_owned),
            side: side.map(ToOwned::to_owned),
            round,
            visible_claims,
            fork: None,
        }
    }

    #[test]
    fn phase2_bindings_finalize_typed_artifacts_without_legacy_writes() {
        let temp = tempfile::tempdir().unwrap();
        let mut state = state();
        state["phase1_index"] = json!({"evidence":"phase1"});
        let topic = file_store_domain_runtime(
            temp.path(),
            &state,
            phase2_plan(
                "mediator.topic",
                ToolManagedProfile::TopicGeneration,
                None,
                None,
                None,
                Default::default(),
            ),
        )
        .unwrap();
        topic
            .execute(
                SET_PHASE2_COMMON_GROUND,
                json!({"common_ground":"rates remain uncertain"}),
            )
            .unwrap();
        let topic_create = topic.execute(CREATE_PHASE2_TOPIC, json!({"topic":"duration risk","decision_hinge":"real yields","evidence_refs":["evidence:phase1"]})).unwrap();
        let topic_id = topic_create["topic_id"].as_str().unwrap().to_owned();
        let topic_final = topic.execute(FINALIZE_TOPIC_GENERATION, json!({})).unwrap();
        assert_eq!(topic_final["terminal"], true);

        let seed = file_store_domain_runtime(
            temp.path(),
            &state,
            phase2_plan(
                "researcher.bull.initial",
                ToolManagedProfile::DebateSeed,
                Some(&topic_id),
                Some("bull"),
                Some(1),
                Default::default(),
            ),
        )
        .unwrap();
        let claim = seed.execute(CREATE_DEBATE_CLAIM, json!({"claim":"duration pressure eases","confidence":0.65,"evidence_refs":["evidence:phase1"]})).unwrap();
        let claim_id = claim["claim_id"].as_str().unwrap().to_owned();
        assert_eq!(
            seed.execute(FINALIZE_DEBATE_SEED, json!({})).unwrap()["terminal"],
            true
        );

        let response = file_store_domain_runtime(
            temp.path(),
            &state,
            phase2_plan(
                "researcher.bear.interaction",
                ToolManagedProfile::DebateResponse,
                Some(&topic_id),
                Some("bear"),
                Some(2),
                [claim_id.clone()].into_iter().collect(),
            ),
        )
        .unwrap();
        response.execute(RESPOND_TO_DEBATE_CLAIM, json!({"reply_to_claim_id":claim_id,"response":"inflation can reaccelerate","evidence_refs":["evidence:phase1"]})).unwrap();
        assert_eq!(
            response
                .execute(FINALIZE_DEBATE_RESPONSE, json!({}))
                .unwrap()["terminal"],
            true
        );

        let controller = file_store_domain_runtime(
            temp.path(),
            &state,
            phase2_plan(
                "mediator.topic_controller",
                ToolManagedProfile::TopicControl,
                Some(&topic_id),
                None,
                Some(2),
                Default::default(),
            ),
        )
        .unwrap();
        controller
            .execute(SET_DECISION_HINGE, json!({"value":"real yields"}))
            .unwrap();
        controller
            .execute(
                ADD_AGREED_FACT,
                json!({"value":"policy sensitivity remains elevated"}),
            )
            .unwrap();
        controller
            .execute(
                SET_CLAIM_STATUS,
                json!({"claim_id":claim_id,"status":"unresolved"}),
            )
            .unwrap_err();
        controller
            .execute(SET_TOPIC_SOFT_CONTROL, json!({"should_continue":false}))
            .unwrap();
        assert_eq!(
            controller
                .execute(FINALIZE_TOPIC_CONTROL, json!({}))
                .unwrap()["terminal"],
            true
        );

        let warmup = file_store_domain_runtime(
            temp.path(),
            &state,
            phase2_plan(
                "mediator.topic",
                ToolManagedProfile::ResearcherWarmup,
                None,
                None,
                Some(0),
                Default::default(),
            ),
        )
        .unwrap();
        assert_eq!(
            warmup
                .execute(FINALIZE_RESEARCHER_WARMUP, json!({}))
                .unwrap()["terminal"],
            true
        );
    }
}
