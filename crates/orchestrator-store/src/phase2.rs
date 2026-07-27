//! Typed Phase 2 Draft -> Builder -> Finalize service.
//!
//! This is deliberately a domain service, not a JSON patch layer.  The
//! workflow provides the immutable [`ArtifactScope`] and evidence allowlist;
//! callers can only apply the named Topic/Debate/Controller commands below.

use std::{collections::BTreeSet, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::{
    apply_typed_draft_command, content_hash, create_or_recover_draft, finalize_draft_atomic,
    read_draft_for_scope, ArtifactDraftState, ArtifactScope, ContentHashDocument, DebateClaimDraft,
    DebateResponseDraftEntry, DraftAppendOutcome, DraftProfile, FileStore, FinalizableArtifact,
    FinalizeDraftOutcome, Phase2TopicDraft, Result, RunLocation, SafeSlug, StoreError, Versioned,
};

pub const PHASE2_ARTIFACT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimStatus {
    Accepted,
    Rejected,
    Unresolved,
    Blocked,
}

impl ClaimStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
            Self::Unresolved => "unresolved",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Phase2Artifact {
    pub schema_version: u32,
    pub artifact_id: String,
    pub run_id: String,
    pub phase: u8,
    pub role: String,
    pub profile: String,
    pub unit_key: String,
    pub source_payload_hash: String,
    pub ticker: Option<String>,
    pub topic_id: Option<String>,
    pub side: Option<String>,
    pub round: Option<u32>,
    pub payload: Phase2ArtifactPayload,
    pub evidence_refs: BTreeSet<String>,
    pub created_at: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Phase2ArtifactPayload {
    TopicGeneration {
        common_ground: String,
        topics: Vec<Phase2TopicDraft>,
    },
    DebateSeed {
        claims: Vec<DebateClaimDraft>,
    },
    DebateResponse {
        responses: Vec<DebateResponseDraftEntry>,
    },
    TopicControl {
        claim_statuses: Vec<ClaimStatusEntry>,
        agreed_facts: Vec<String>,
        decision_hinges: Vec<String>,
        routes: Vec<SteerRoute>,
        should_continue: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimStatusEntry {
    pub claim_id: String,
    pub status: ClaimStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SteerRoute {
    pub target: String,
    pub instruction: String,
}

impl ContentHashDocument for Phase2Artifact {
    fn content_hash(&self) -> &str {
        &self.content_hash
    }

    fn set_content_hash(&mut self, hash: String) {
        self.content_hash = hash;
    }
}

impl Versioned for Phase2Artifact {
    const SCHEMA_VERSION: u32 = PHASE2_ARTIFACT_SCHEMA_VERSION;
}

impl FinalizableArtifact for Phase2Artifact {
    fn artifact_id(&self) -> &str {
        &self.artifact_id
    }

    fn source_payload_hash(&self) -> &str {
        &self.source_payload_hash
    }
}

#[derive(Debug, Clone)]
pub struct Phase2DraftService {
    store: FileStore,
    location: RunLocation,
    scope: ArtifactScope,
    created_at: String,
    visible_evidence: BTreeSet<String>,
    visible_claims: BTreeSet<String>,
}

impl Phase2DraftService {
    pub fn new(
        store: FileStore,
        location: RunLocation,
        scope: ArtifactScope,
        created_at: impl Into<String>,
        visible_evidence: impl IntoIterator<Item = String>,
        visible_claims: impl IntoIterator<Item = String>,
    ) -> Result<Self> {
        if !matches!(
            scope.profile,
            DraftProfile::TopicGeneration
                | DraftProfile::ResearcherWarmup
                | DraftProfile::DebateSeed
                | DraftProfile::DebateResponse
                | DraftProfile::TopicControl
        ) {
            return Err(StoreError::InvalidDocument {
                kind: "phase2 draft service",
                message: "scope profile is not a Phase 2 tool-managed profile".to_owned(),
            });
        }
        let created_at = created_at.into();
        create_or_recover_draft(&store, &location, scope.clone(), created_at.clone())?;
        Ok(Self {
            store,
            location,
            scope,
            created_at,
            visible_evidence: visible_evidence.into_iter().collect(),
            visible_claims: visible_claims.into_iter().collect(),
        })
    }

    pub fn scope(&self) -> &ArtifactScope {
        &self.scope
    }

    pub fn set_phase2_common_ground(&self, common_ground: String) -> Result<DraftAppendOutcome> {
        require_profile(
            &self.scope,
            DraftProfile::TopicGeneration,
            "set_phase2_common_ground",
        )?;
        require_text("common_ground", &common_ground)?;
        let command = CommonGroundCommand {
            common_ground: common_ground.clone(),
        };
        self.apply(
            "set_phase2_common_ground",
            &command,
            "common-ground",
            move |state| {
                let ArtifactDraftState::TopicGeneration(draft) = state else {
                    return profile_state_error("topic_generation");
                };
                draft.common_ground = Some(common_ground);
                Ok(())
            },
        )
    }

    pub fn create_phase2_topic(
        &self,
        topic: String,
        decision_hinge: String,
        evidence_refs: Vec<String>,
    ) -> Result<(String, DraftAppendOutcome)> {
        require_profile(
            &self.scope,
            DraftProfile::TopicGeneration,
            "create_phase2_topic",
        )?;
        require_text("topic", &topic)?;
        require_text("decision_hinge", &decision_hinge)?;
        let evidence_refs = self.checked_evidence_refs(evidence_refs)?;
        let command = CreateTopicCommand {
            topic: topic.clone(),
            decision_hinge: decision_hinge.clone(),
            evidence_refs: evidence_refs.clone(),
        };
        let topic_id = stable_id("topic", &(&self.scope, &command))?;
        let result_id = topic_id.clone();
        let topic_id_for_state = topic_id.clone();
        let outcome = self.apply("create_phase2_topic", &command, result_id, move |state| {
            let ArtifactDraftState::TopicGeneration(draft) = state else {
                return profile_state_error("topic_generation");
            };
            draft.topics.insert(
                topic_id_for_state.clone(),
                Phase2TopicDraft {
                    topic_id: topic_id_for_state.clone(),
                    topic,
                    decision_hinge,
                    evidence_refs,
                },
            );
            Ok(())
        })?;
        Ok((topic_id, outcome))
    }

    /// Terminal warm-up acknowledgment.  It marks only the typed Draft: no
    /// claim or business Artifact is ever built for a warm-up unit.
    pub fn finalize_researcher_warmup(&self) -> Result<DraftAppendOutcome> {
        require_profile(
            &self.scope,
            DraftProfile::ResearcherWarmup,
            "finalize_researcher_warmup",
        )?;
        if self.visible_evidence.is_empty() {
            return Err(StoreError::InvalidDocument {
                kind: "researcher warmup",
                message: "warmup requires at least one completed evidence read".to_owned(),
            });
        }
        self.apply(
            "finalize_researcher_warmup",
            &WarmupCommand {},
            "warmup-terminal",
            |state| {
                let ArtifactDraftState::ResearcherWarmup(draft) = state else {
                    return profile_state_error("researcher_warmup");
                };
                draft.metadata.evidence_refs = self.visible_evidence.clone();
                draft.finalized = true;
                Ok(())
            },
        )
    }

    /// Terminal builder for the one Rust-planned Topic Generator unit.
    pub fn finalize_topic_generation(&self) -> Result<Phase2Artifact> {
        require_profile(
            &self.scope,
            DraftProfile::TopicGeneration,
            "finalize_topic_generation",
        )?;
        self.finalize()
    }

    pub fn create_debate_claim(
        &self,
        claim: String,
        confidence: f64,
        evidence_refs: Vec<String>,
    ) -> Result<(String, DraftAppendOutcome)> {
        require_profile(&self.scope, DraftProfile::DebateSeed, "create_debate_claim")?;
        require_text("claim", &claim)?;
        let confidence_bps = confidence_to_bps(confidence)?;
        let evidence_refs = self.checked_evidence_refs(evidence_refs)?;
        let command = CreateClaimCommand {
            claim: claim.clone(),
            confidence_bps,
            evidence_refs: evidence_refs.clone(),
        };
        let claim_id = stable_id("claim", &(&self.scope, &command))?;
        let result_id = claim_id.clone();
        let claim_id_for_state = claim_id.clone();
        let outcome = self.apply("create_debate_claim", &command, result_id, move |state| {
            let ArtifactDraftState::DebateSeed(draft) = state else {
                return profile_state_error("debate_seed");
            };
            draft.claims.insert(
                claim_id_for_state.clone(),
                DebateClaimDraft {
                    claim_id: claim_id_for_state.clone(),
                    claim,
                    confidence_bps,
                    evidence_refs,
                },
            );
            Ok(())
        })?;
        Ok((claim_id, outcome))
    }

    /// Terminal builder for one side/topic seed unit.
    pub fn finalize_debate_seed(&self) -> Result<Phase2Artifact> {
        require_profile(
            &self.scope,
            DraftProfile::DebateSeed,
            "finalize_debate_seed",
        )?;
        self.finalize()
    }

    pub fn respond_to_debate_claim(
        &self,
        reply_to_claim_id: String,
        response: String,
        evidence_refs: Vec<String>,
    ) -> Result<(String, DraftAppendOutcome)> {
        require_profile(
            &self.scope,
            DraftProfile::DebateResponse,
            "respond_to_debate_claim",
        )?;
        require_text("reply_to_claim_id", &reply_to_claim_id)?;
        require_text("response", &response)?;
        if !self.visible_claims.contains(&reply_to_claim_id) {
            return Err(StoreError::InvalidDocument {
                kind: "debate response",
                message: format!("claim {reply_to_claim_id:?} is not visible to this response"),
            });
        }
        let evidence_refs = self.checked_evidence_refs(evidence_refs)?;
        let command = DebateResponseCommand {
            reply_to_claim_id: reply_to_claim_id.clone(),
            response: response.clone(),
            evidence_refs: evidence_refs.clone(),
        };
        let response_id = stable_id("response", &(&self.scope, &command))?;
        let result_id = response_id.clone();
        let response_id_for_state = response_id.clone();
        let outcome = self.apply(
            "respond_to_debate_claim",
            &command,
            result_id,
            move |state| {
                let ArtifactDraftState::DebateResponse(draft) = state else {
                    return profile_state_error("debate_response");
                };
                draft.responses.insert(
                    response_id_for_state.clone(),
                    DebateResponseDraftEntry {
                        response_id: response_id_for_state.clone(),
                        reply_to_claim_id,
                        response,
                        evidence_refs,
                    },
                );
                Ok(())
            },
        )?;
        Ok((response_id, outcome))
    }

    /// Terminal builder for one side/topic interaction unit.
    pub fn finalize_debate_response(&self) -> Result<Phase2Artifact> {
        require_profile(
            &self.scope,
            DraftProfile::DebateResponse,
            "finalize_debate_response",
        )?;
        self.finalize()
    }

    pub fn set_claim_status(
        &self,
        claim_id: String,
        status: ClaimStatus,
    ) -> Result<DraftAppendOutcome> {
        require_profile(&self.scope, DraftProfile::TopicControl, "set_claim_status")?;
        if !self.visible_claims.contains(&claim_id) {
            return Err(StoreError::InvalidDocument {
                kind: "topic control",
                message: format!("claim {claim_id:?} is not visible to the controller"),
            });
        }
        let command = ClaimStatusCommand {
            claim_id: claim_id.clone(),
            status,
        };
        self.apply(
            "set_claim_status",
            &command,
            claim_id.clone(),
            move |state| {
                let ArtifactDraftState::TopicControl(draft) = state else {
                    return profile_state_error("topic_control");
                };
                draft
                    .claim_statuses
                    .insert(claim_id, status.as_str().to_owned());
                Ok(())
            },
        )
    }

    pub fn add_agreed_fact(&self, fact: String) -> Result<DraftAppendOutcome> {
        require_profile(&self.scope, DraftProfile::TopicControl, "add_agreed_fact")?;
        require_text("fact", &fact)?;
        self.apply(
            "add_agreed_fact",
            &TextCommand {
                value: fact.clone(),
            },
            fact.clone(),
            move |state| {
                let ArtifactDraftState::TopicControl(draft) = state else {
                    return profile_state_error("topic_control");
                };
                if !draft.agreed_facts.contains(&fact) {
                    draft.agreed_facts.push(fact);
                }
                Ok(())
            },
        )
    }

    pub fn set_decision_hinge(&self, hinge: String) -> Result<DraftAppendOutcome> {
        require_profile(
            &self.scope,
            DraftProfile::TopicControl,
            "set_decision_hinge",
        )?;
        require_text("hinge", &hinge)?;
        self.apply(
            "set_decision_hinge",
            &TextCommand {
                value: hinge.clone(),
            },
            hinge.clone(),
            move |state| {
                let ArtifactDraftState::TopicControl(draft) = state else {
                    return profile_state_error("topic_control");
                };
                if !draft.decision_hinges.contains(&hinge) {
                    draft.decision_hinges.push(hinge);
                }
                Ok(())
            },
        )
    }

    pub fn route_debate_steer(
        &self,
        target: String,
        instruction: String,
    ) -> Result<DraftAppendOutcome> {
        require_profile(
            &self.scope,
            DraftProfile::TopicControl,
            "route_debate_steer",
        )?;
        if !matches!(target.as_str(), "bull" | "bear") {
            return Err(StoreError::InvalidDocument {
                kind: "topic control",
                message: "steer target must be bull or bear".to_owned(),
            });
        }
        require_text("instruction", &instruction)?;
        let command = RouteCommand {
            target: target.clone(),
            instruction: instruction.clone(),
        };
        self.apply(
            "route_debate_steer",
            &command,
            target.clone(),
            move |state| {
                let ArtifactDraftState::TopicControl(draft) = state else {
                    return profile_state_error("topic_control");
                };
                draft.routes.insert(target, instruction);
                Ok(())
            },
        )
    }

    pub fn set_topic_soft_control(&self, should_continue: bool) -> Result<DraftAppendOutcome> {
        require_profile(
            &self.scope,
            DraftProfile::TopicControl,
            "set_topic_soft_control",
        )?;
        self.apply(
            "set_topic_soft_control",
            &SoftControlCommand { should_continue },
            format!("should-continue-{should_continue}"),
            move |state| {
                let ArtifactDraftState::TopicControl(draft) = state else {
                    return profile_state_error("topic_control");
                };
                draft.should_continue = Some(should_continue);
                Ok(())
            },
        )
    }

    /// Terminal builder for one controller round.  The builder owns the
    /// controller file name and refuses to finalize without a soft control and
    /// at least one decision hinge.
    pub fn finalize_topic_control(&self) -> Result<Phase2Artifact> {
        require_profile(
            &self.scope,
            DraftProfile::TopicControl,
            "finalize_topic_control",
        )?;
        self.finalize()
    }

    pub fn finalize(&self) -> Result<Phase2Artifact> {
        let draft = read_draft_for_scope(&self.store, &self.location, &self.scope)?;
        let (payload, evidence_refs) = build_payload(&draft.state)?;
        let artifact_relative = artifact_relative(&self.scope)?;
        let artifact = Phase2Artifact {
            schema_version: PHASE2_ARTIFACT_SCHEMA_VERSION,
            artifact_id: stable_id("artifact", &self.scope)?,
            run_id: self.scope.run_id.clone(),
            phase: self.scope.phase,
            role: self.scope.role.clone(),
            profile: self.scope.profile.as_str().to_owned(),
            unit_key: self.scope.unit_key.clone(),
            source_payload_hash: self.scope.source_payload_hash.clone(),
            ticker: self.scope.ticker.clone(),
            topic_id: self.scope.topic_id.clone(),
            side: self.scope.side.clone(),
            round: self.scope.round,
            payload,
            evidence_refs,
            created_at: self.created_at.clone(),
            content_hash: String::new(),
        };
        match finalize_draft_atomic(
            &self.store,
            &self.location,
            &self.scope,
            &artifact_relative,
            artifact,
            self.created_at.clone(),
        )? {
            FinalizeDraftOutcome::Completed { artifact, .. } => Ok(artifact),
            FinalizeDraftOutcome::Recovered { .. } => self.store.read_versioned_json(
                &self.location.child_relative(&artifact_relative)?,
                crate::FileSchemaKind::Artifact(self.scope.profile.as_str().to_owned()),
            ),
        }
    }

    fn checked_evidence_refs(&self, refs: Vec<String>) -> Result<BTreeSet<String>> {
        let refs = refs.into_iter().collect::<BTreeSet<_>>();
        if refs.is_empty() {
            return Err(StoreError::InvalidDocument {
                kind: "phase2 evidence",
                message: "at least one visible evidence reference is required".to_owned(),
            });
        }
        for reference in &refs {
            if !self.visible_evidence.contains(reference) {
                return Err(StoreError::InvalidDocument {
                    kind: "phase2 evidence",
                    message: format!("evidence ref {reference:?} is not visible to this unit"),
                });
            }
        }
        Ok(refs)
    }

    fn apply<T: Serialize>(
        &self,
        tool_name: &str,
        command: &T,
        result_id: impl Into<String>,
        mutate: impl FnOnce(&mut ArtifactDraftState) -> Result<()>,
    ) -> Result<DraftAppendOutcome> {
        apply_typed_draft_command(
            &self.store,
            &self.location,
            &self.scope,
            tool_name,
            command,
            result_id,
            self.created_at.clone(),
            mutate,
        )
    }
}

fn build_payload(state: &ArtifactDraftState) -> Result<(Phase2ArtifactPayload, BTreeSet<String>)> {
    match state {
        ArtifactDraftState::TopicGeneration(draft) => {
            let common_ground =
                draft
                    .common_ground
                    .clone()
                    .ok_or_else(|| StoreError::InvalidDocument {
                        kind: "topic generation finalize",
                        message: "common_ground is required".to_owned(),
                    })?;
            let topics = draft.topics.values().cloned().collect::<Vec<_>>();
            let evidence_refs = topics
                .iter()
                .flat_map(|topic| topic.evidence_refs.iter().cloned())
                .collect();
            Ok((
                Phase2ArtifactPayload::TopicGeneration {
                    common_ground,
                    topics,
                },
                evidence_refs,
            ))
        }
        ArtifactDraftState::DebateSeed(draft) => {
            if draft.claims.is_empty() {
                return Err(StoreError::InvalidDocument {
                    kind: "debate seed finalize",
                    message: "at least one claim is required".to_owned(),
                });
            }
            let claims = draft.claims.values().cloned().collect::<Vec<_>>();
            let evidence_refs = claims
                .iter()
                .flat_map(|claim| claim.evidence_refs.iter().cloned())
                .collect();
            Ok((Phase2ArtifactPayload::DebateSeed { claims }, evidence_refs))
        }
        ArtifactDraftState::DebateResponse(draft) => {
            if draft.responses.is_empty() {
                return Err(StoreError::InvalidDocument {
                    kind: "debate response finalize",
                    message: "at least one response is required".to_owned(),
                });
            }
            let responses = draft.responses.values().cloned().collect::<Vec<_>>();
            let evidence_refs = responses
                .iter()
                .flat_map(|response| response.evidence_refs.iter().cloned())
                .collect();
            Ok((
                Phase2ArtifactPayload::DebateResponse { responses },
                evidence_refs,
            ))
        }
        ArtifactDraftState::TopicControl(draft) => {
            let should_continue =
                draft
                    .should_continue
                    .ok_or_else(|| StoreError::InvalidDocument {
                        kind: "topic control finalize",
                        message: "soft control is required".to_owned(),
                    })?;
            if draft.decision_hinges.is_empty() {
                return Err(StoreError::InvalidDocument {
                    kind: "topic control finalize",
                    message: "at least one decision hinge is required".to_owned(),
                });
            }
            let claim_statuses = draft
                .claim_statuses
                .iter()
                .map(|(claim_id, status)| {
                    let status = match status.as_str() {
                        "accepted" => ClaimStatus::Accepted,
                        "rejected" => ClaimStatus::Rejected,
                        "unresolved" => ClaimStatus::Unresolved,
                        "blocked" => ClaimStatus::Blocked,
                        _ => {
                            return Err(StoreError::InvalidDocument {
                                kind: "topic control",
                                message: format!("invalid stored claim status {status:?}"),
                            })
                        }
                    };
                    Ok(ClaimStatusEntry {
                        claim_id: claim_id.clone(),
                        status,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let routes = draft
                .routes
                .iter()
                .map(|(target, instruction)| SteerRoute {
                    target: target.clone(),
                    instruction: instruction.clone(),
                })
                .collect();
            Ok((
                Phase2ArtifactPayload::TopicControl {
                    claim_statuses,
                    agreed_facts: draft.agreed_facts.clone(),
                    decision_hinges: draft.decision_hinges.clone(),
                    routes,
                    should_continue,
                },
                draft.metadata.evidence_refs.clone(),
            ))
        }
        ArtifactDraftState::ResearcherWarmup(_) => Err(StoreError::InvalidDocument {
            kind: "researcher warmup finalize",
            message: "warmup terminal has no business artifact".to_owned(),
        }),
        _ => profile_state_error("phase2 profile"),
    }
}

fn artifact_relative(scope: &ArtifactScope) -> Result<PathBuf> {
    let base = PathBuf::from("artifacts/phase2");
    match scope.profile {
        DraftProfile::TopicGeneration => Ok(base.join("topic-generation.json")),
        DraftProfile::DebateSeed => topic_artifact_path(
            scope,
            format!("{}-seed.json", required_scope(&scope.side, "side")?),
        ),
        DraftProfile::DebateResponse => topic_artifact_path(
            scope,
            format!(
                "{}-response-round-{}.json",
                required_scope(&scope.side, "side")?,
                scope.round.ok_or_else(|| StoreError::InvalidDocument {
                    kind: "phase2 artifact path",
                    message: "response round is required".to_owned()
                })?
            ),
        ),
        DraftProfile::TopicControl => topic_artifact_path(
            scope,
            format!(
                "controller-round-{}.json",
                scope.round.ok_or_else(|| StoreError::InvalidDocument {
                    kind: "phase2 artifact path",
                    message: "controller round is required".to_owned()
                })?
            ),
        ),
        _ => profile_state_error("phase2 artifact"),
    }
}

fn topic_artifact_path(scope: &ArtifactScope, file: String) -> Result<PathBuf> {
    let topic = SafeSlug::new("topic", required_scope(&scope.topic_id, "topic_id")?)?;
    Ok(PathBuf::from("artifacts/phase2/topics")
        .join(topic.as_str())
        .join(file))
}

fn require_profile(scope: &ArtifactScope, expected: DraftProfile, tool: &str) -> Result<()> {
    if scope.profile != expected {
        return Err(StoreError::InvalidDocument {
            kind: "phase2 tool",
            message: format!(
                "{tool} is not allowed for profile {}",
                scope.profile.as_str()
            ),
        });
    }
    Ok(())
}

fn require_text(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(StoreError::InvalidDocument {
            kind: "phase2 tool",
            message: format!("{field} must not be empty"),
        });
    }
    Ok(())
}

fn required_scope<'a>(value: &'a Option<String>, field: &str) -> Result<&'a str> {
    value
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| StoreError::InvalidDocument {
            kind: "phase2 artifact scope",
            message: format!("{field} is required"),
        })
}

fn confidence_to_bps(confidence: f64) -> Result<u16> {
    if !confidence.is_finite() || !(0.0..=1.0).contains(&confidence) {
        return Err(StoreError::InvalidDocument {
            kind: "debate claim",
            message: "confidence must be finite and in 0..=1".to_owned(),
        });
    }
    Ok((confidence * 10_000.0).round() as u16)
}

fn stable_id(prefix: &str, value: &impl Serialize) -> Result<String> {
    let value =
        serde_json::to_value(value).map_err(|source| StoreError::JsonSerialize { source })?;
    // `content_hash` intentionally accepts authoritative JSON objects only;
    // command tuples are wrapped so their canonical form is still stable.
    let envelope = serde_json::json!({"value": value});
    Ok(format!("{prefix}-{}", &content_hash(&envelope)?[..20]))
}

fn profile_state_error<T>(expected: &str) -> Result<T> {
    Err(StoreError::InvalidDocument {
        kind: "phase2 draft state",
        message: format!("expected {expected} typed state"),
    })
}

#[derive(Serialize)]
struct CommonGroundCommand {
    common_ground: String,
}
#[derive(Serialize)]
struct CreateTopicCommand {
    topic: String,
    decision_hinge: String,
    evidence_refs: BTreeSet<String>,
}
#[derive(Serialize)]
struct WarmupCommand {}
#[derive(Serialize)]
struct CreateClaimCommand {
    claim: String,
    confidence_bps: u16,
    evidence_refs: BTreeSet<String>,
}
#[derive(Serialize)]
struct DebateResponseCommand {
    reply_to_claim_id: String,
    response: String,
    evidence_refs: BTreeSet<String>,
}
#[derive(Serialize)]
struct ClaimStatusCommand {
    claim_id: String,
    status: ClaimStatus,
}
#[derive(Serialize)]
struct TextCommand {
    value: String,
}
#[derive(Serialize)]
struct RouteCommand {
    target: String,
    instruction: String,
}
#[derive(Serialize)]
struct SoftControlCommand {
    should_continue: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DraftProfile, FileStoreOptions};
    use tempfile::TempDir;

    fn service(
        profile: DraftProfile,
        topic_id: Option<&str>,
        side: Option<&str>,
        round: Option<u32>,
    ) -> (TempDir, Phase2DraftService) {
        let temp = TempDir::new().unwrap();
        let store = FileStore::open(temp.path(), FileStoreOptions::default()).unwrap();
        let location = RunLocation::new("2026-07-27", "run-1").unwrap();
        let scope = ArtifactScope {
            run_id: "run-1".to_owned(),
            current_date: "2026-07-27".to_owned(),
            phase: 2,
            role: "mediator.topic".to_owned(),
            profile,
            profile_version: 1,
            builder_version: 1,
            unit_key: format!("unit-{}", profile.as_str()),
            source_payload_hash: "source".to_owned(),
            ticker: Some("QQQ".to_owned()),
            topic_id: topic_id.map(ToOwned::to_owned),
            side: side.map(ToOwned::to_owned),
            stance: None,
            round,
            reflection_task: None,
        };
        let service = Phase2DraftService::new(
            store,
            location,
            scope,
            "2026-07-27T00:00:00Z",
            ["evidence:p1".to_owned()],
            ["claim-visible".to_owned()],
        )
        .unwrap();
        (temp, service)
    }

    #[test]
    fn topic_generation_builds_one_atomic_canonical_artifact() {
        let (_temp, service) = service(DraftProfile::TopicGeneration, None, None, None);
        service
            .set_phase2_common_ground("Rates remain uncertain".to_owned())
            .unwrap();
        let (topic_id, _) = service
            .create_phase2_topic(
                "Fed path".to_owned(),
                "CPI surprise".to_owned(),
                vec!["evidence:p1".to_owned()],
            )
            .unwrap();
        let artifact = service.finalize().unwrap();
        assert_eq!(
            artifact.artifact_id,
            stable_id("artifact", service.scope()).unwrap()
        );
        let Phase2ArtifactPayload::TopicGeneration { topics, .. } = artifact.payload else {
            panic!("topic generation payload")
        };
        assert_eq!(topics[0].topic_id, topic_id);
        assert!(service.finalize().is_ok(), "completed draft must recover");
    }

    #[test]
    fn response_rejects_invisible_claim_and_evidence() {
        let (_temp, service) = service(
            DraftProfile::DebateResponse,
            Some("rates"),
            Some("bull"),
            Some(1),
        );
        assert!(service
            .respond_to_debate_claim(
                "not-visible".to_owned(),
                "reply".to_owned(),
                vec!["evidence:p1".to_owned()]
            )
            .is_err());
        assert!(service
            .respond_to_debate_claim(
                "claim-visible".to_owned(),
                "reply".to_owned(),
                vec!["not-visible".to_owned()]
            )
            .is_err());
    }

    #[test]
    fn warmup_has_terminal_marker_but_no_business_artifact() {
        let (_temp, service) = service(DraftProfile::ResearcherWarmup, None, None, Some(0));
        service.finalize_researcher_warmup().unwrap();
        assert!(service.finalize().is_err());
    }
}
