#[derive(Debug, Error)]
pub enum ResearchError {
    #[error(transparent)]
    Context(#[from] ContextError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error("task has no Agent Contract hash")]
    MissingContractHash,
    #[error("Agent Contract {0} is not installed")]
    UnknownContract(akzio_domain::ContentHash),
    #[error("task contract hash and recipe contract hash do not match")]
    ContractMismatch,
    #[error("workflow node task does not match the write permit task")]
    TaskMismatch,
    #[error("workflow node policy diverges from its installed Agent Contract")]
    NodePolicyMismatch,
    #[error("Agent Contract {0} appears more than once in the catalogue")]
    DuplicateContract(akzio_domain::ContentHash),
    #[error("Agent Contract {contract_id:?} version {version} appears more than once")]
    DuplicateContractVersion {
        contract_id: akzio_domain::ContractId,
        version: u32,
    },
    #[error("active research contract purpose is not allowed: {0}")]
    UnexpectedActiveContractPurpose(String),
    #[error("active research contract purpose appears more than once: {0}")]
    DuplicateActiveContractPurpose(String),
    #[error("active research contract is missing: {0}")]
    MissingActiveContract(&'static str),
    #[error("active research contract {purpose} outputs {actual:?}, expected {expected:?}")]
    ActiveContractOutputMismatch {
        purpose: String,
        expected: ArtifactKind,
        actual: ArtifactKind,
    },
    #[error("active research contract {0} differs from the canonical definition")]
    NonCanonicalActiveContract(String),
    #[error("candidate contract {candidate} expands active contract {active} capability")]
    CandidateCapabilityExpansion {
        active: akzio_domain::ContentHash,
        candidate: akzio_domain::ContentHash,
    },
    #[error("model capability mismatch for {capability} ({provider_id}/{model_id})")]
    CapabilityMismatch {
        capability: &'static str,
        provider_id: String,
        model_id: String,
    },
    #[error("ReadGrant does not match the active task permit")]
    GrantPermitMismatch,
    #[error("Agent output did not satisfy Contract schema: {0}")]
    InvalidOutput(String),
    #[error("Agent model failed: {0}")]
    Model(String),
    #[error("provider {timeout_kind}: {message}")]
    ProviderTimeout {
        timeout_kind: &'static str,
        message: String,
        trace: Option<Box<ModelCallTrace>>,
    },
    #[error("Agent model rate limited: {0}")]
    RateLimited(String),
    #[error("provider response was incomplete: {reason}")]
    ProviderIncomplete {
        reason: String,
        usage: ModelUsage,
        trace: Option<Box<ModelCallTrace>>,
    },
    #[error("provider response omitted required input or output token totals")]
    ProviderUsageMissing {
        usage: ModelUsage,
        trace: Option<Box<ModelCallTrace>>,
    },
    #[error("Agent model {error_class} failed: {message}")]
    ModelDebug {
        error_class: &'static str,
        message: String,
        trace: ModelCallTrace,
    },
    #[error("tool {0} is not granted by the Agent Contract")]
    ToolNotGranted(String),
    #[error("invalid model ToolSpec: {0}")]
    InvalidToolSpec(String),
    #[error("tool {tool} is not granted for source family {source_family}")]
    ToolSourceNotGranted { tool: String, source_family: String },
    #[error("Agent exceeded its Contract tool-call budget")]
    ToolBudgetExceeded,
    #[error("Agent exceeded its derived provider-call budget")]
    ModelCallBudgetExceeded,
    #[error("Agent run input used {actual} tokens but Contract permits at most {maximum}")]
    InputBudgetExceeded { actual: u32, maximum: u32 },
    #[error("Agent run output used {actual} tokens but Contract permits at most {maximum}")]
    OutputBudgetExceeded { actual: u32, maximum: u32 },
    #[error("provider returned {actual} output tokens above the request cap of {maximum}")]
    ProviderOutputLimitExceeded { actual: u32, maximum: u32 },
    #[error("hard cost budget configured without an immutable pricing snapshot")]
    PricingUnavailable,
    #[error("pricing snapshot identity and version must be non-empty")]
    InvalidPricingSnapshot,
    #[error("priced model route identity must be non-empty")]
    InvalidPricingRoute,
    #[error("provider usage detail exceeds its reported token total")]
    InvalidProviderUsage,
    #[error("provider usage is unknown because a call did not close or its accounting history cannot be reconstructed")]
    ProviderUsageUnknown,
    #[error("provider call cost cannot be determined after missing usage")]
    CostUsageUnknown,
    #[error("Agent run cost reached {actual} micros but policy permits at most {maximum}")]
    CostBudgetExceeded { actual: u64, maximum: u64 },
    #[error("Agent run cost arithmetic overflowed")]
    CostOverflow,
    #[error("model pricing or cost policy changed within a durable Agent run")]
    BudgetPolicyMismatch,
    #[error("Agent exceeded its Contract wall-time budget of {maximum_secs} seconds")]
    WallTimeExceeded { maximum_secs: u32 },
    #[error("Agent completed without a final output")]
    MissingFinalOutput,
    #[error("Agent submission response is ambiguous")]
    AmbiguousSubmission,
    #[error("Agent model refused the task: {0}")]
    ModelRefused(String),
}

impl ResearchError {
    pub fn retry_cause(&self) -> Option<RetryCause> {
        match self {
            Self::InvalidOutput(_)
            | Self::MissingFinalOutput
            | Self::ModelDebug {
                error_class: "invalid_output",
                ..
            } => Some(RetryCause::InvalidOutput),
            _ => None,
        }
    }
}

pub type ResearchResult<T> = Result<T, ResearchError>;

struct CanonicalContractDefinition {
    purpose: &'static str,
    responsibility: &'static str,
    output_kind: ArtifactKind,
    output_schema: Value,
    permitted_kinds: BTreeSet<ArtifactKind>,
    min_context_artifacts: u16,
    budget: TaskBudget,
    termination: TerminationPolicy,
    on_failure: FailureDisposition,
}

fn canonical_active_contracts(store: &Store) -> ResearchResult<Vec<AgentContract>> {
    [
        CanonicalContractDefinition {
            purpose: RESEARCH_ANALYST_RECIPE_ID,
            responsibility: "Produce evidence-linked, bounded research claims for one shard of the approved workflow.",
            output_kind: ArtifactKind::Claim,
            output_schema: reviewed_research_schema(claim_output_schema()),
            permitted_kinds: BTreeSet::from([
                ArtifactKind::NormalizedEvidence,
                ArtifactKind::SemanticDetail,
            ]),
            min_context_artifacts: 1,
            budget: akzio_domain::budget::versioned_contract_budget(RESEARCH_ANALYST_RECIPE_ID).expect("registered agent role"),
            termination: TerminationPolicy {
                max_child_tasks: 2,
                max_depth: 2,
                require_evidence: true,
                stop_when_evidence_complete: true,
            },
            on_failure: FailureDisposition::FailTask,
        },
        CanonicalContractDefinition {
            purpose: RESEARCH_CRITIC_RECIPE_ID,
            responsibility: "Independently verify material claims against governed evidence and fail closed on unsupported, contradicted, or unreviewable claims without changing facts or execution authority.",
            output_kind: ArtifactKind::Critique,
            output_schema: reviewed_research_schema(critique_output_schema()),
            permitted_kinds: BTreeSet::from([
                ArtifactKind::Claim,
                ArtifactKind::NormalizedEvidence,
                ArtifactKind::SemanticDetail,
                ArtifactKind::DeliberationNote,
            ]),
            min_context_artifacts: 1,
            budget: akzio_domain::budget::versioned_contract_budget(RESEARCH_CRITIC_RECIPE_ID).expect("registered agent role"),
            termination: TerminationPolicy {
                max_child_tasks: 1,
                max_depth: 1,
                require_evidence: true,
                stop_when_evidence_complete: true,
            },
            on_failure: FailureDisposition::FailRun,
        },
        CanonicalContractDefinition {
            purpose: RESEARCH_SYNTHESIZER_RECIPE_ID,
            responsibility: "Synthesize approved claims and critiques into a DecisionProposal with typed blockers for Rust-owned gates.",
            output_kind: ArtifactKind::DecisionProposal,
            output_schema: reviewed_proposal_schema(),
 permitted_kinds: BTreeSet::from([
                ArtifactKind::Claim,
                ArtifactKind::Critique,
                ArtifactKind::Lesson,
                ArtifactKind::Experience,
 ArtifactKind::CandidatePolicy,
 ArtifactKind::NormalizedEvidence,
 ArtifactKind::SemanticDetail,
 ArtifactKind::DeliberationNote,
 ArtifactKind::DecisionProposal, ArtifactKind::ProposalReview,
 ]),
            min_context_artifacts: 1,
            budget: akzio_domain::budget::versioned_contract_budget(RESEARCH_SYNTHESIZER_RECIPE_ID).expect("registered agent role"),
            termination: TerminationPolicy::leaf(),
            on_failure: FailureDisposition::FailRun,
        },
        CanonicalContractDefinition {
            purpose: akzio_domain::RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID,
            responsibility: "Independently review the complete, exact final proposal; assess numerical basis and allocation without granting calibration or execution authority.",
            output_kind: ArtifactKind::ProposalReview,
            output_schema: proposal_review_schema(),
            permitted_kinds: BTreeSet::from([ArtifactKind::DecisionProposal, ArtifactKind::Claim, ArtifactKind::Critique, ArtifactKind::NormalizedEvidence, ArtifactKind::SemanticDetail]),
            min_context_artifacts: 1,
            budget: akzio_domain::budget::versioned_contract_budget("research.proposal_reviewer").expect("registered role"),
            termination: TerminationPolicy::leaf(),
            on_failure: FailureDisposition::FailRun,
        },
        CanonicalContractDefinition {
            purpose: LEARNING_OUTCOME_WORKER_RECIPE_ID,
            responsibility: "Produce a bounded retrospective draft from the governed Paper decision and outcome evidence chain.",
            output_kind: ArtifactKind::RetrospectiveDraft,
            output_schema: retrospective_draft_output_schema(),
            permitted_kinds: BTreeSet::from([
                ArtifactKind::Claim,
                ArtifactKind::Critique,
                ArtifactKind::Decision,
                ArtifactKind::DecisionContext,
                ArtifactKind::ExecutionContext,
                ArtifactKind::ExecutionVerdict,
                ArtifactKind::ExecutionCommitment,
                ArtifactKind::OrderReceipt,
                ArtifactKind::Reconciliation,
                ArtifactKind::OutcomeSchedule,
                ArtifactKind::Outcome,
                ArtifactKind::NormalizedEvidence,
                ArtifactKind::SemanticDetail,
                ArtifactKind::DeliberationNote,
                ArtifactKind::Retrospective,
            ]),
            min_context_artifacts: 1,
            budget: akzio_domain::budget::versioned_contract_budget(LEARNING_OUTCOME_WORKER_RECIPE_ID).expect("registered agent role"),
            termination: TerminationPolicy::leaf(),
            on_failure: FailureDisposition::FailTask,
        },
    ]
    .into_iter()
    .map(|definition| canonical_active_contract(store, definition))
    .collect()
}

fn canonical_active_contract(
    store: &Store,
    definition: CanonicalContractDefinition,
) -> ResearchResult<AgentContract> {
    let outcome = definition.purpose == LEARNING_OUTCOME_WORKER_RECIPE_ID;
    let role_prompt = prompts::role_prompt(definition.purpose)?;
    let prompt = PromptBundle {
        version: if outcome { 35 } else { ACTIVE_PROMPT_BUNDLE_VERSION },
        governance: store.stage_bytes(SHARED_GOVERNANCE_PROMPT.as_bytes(), "text/plain")?,
        role: store.stage_bytes(role_prompt.as_bytes(), "text/plain")?,
    };
    let schema = store.stage_json(&deliberation_output_schema(&definition.output_schema))?;
    let mut contract = AgentContract::new(
        ContractId(format!("akzio.{}", definition.purpose)),
        if outcome { 63 } else { ACTIVE_CONTRACT_VERSION },
        ContractPurpose::new(definition.purpose)?,
        definition.responsibility,
        prompt,
        ContextPolicy {
            permitted_kinds: definition.permitted_kinds,
            permitted_source_families: governed_context_sources(),
            min_artifacts: definition.min_context_artifacts,
            max_artifacts: 24,
            max_bytes: if definition.purpose == RESEARCH_SYNTHESIZER_RECIPE_ID {
                192 * 1024
            } else {
                128 * 1024
            },
            // Original CAS documents remain separately bounded and may be
            // read by range through the existing grant. The model budget is
            // enforced against compact projections above.
            max_source_bytes: Some(
                (if definition.purpose == RESEARCH_SYNTHESIZER_RECIPE_ID {
                    192_u64 * 1024
                } else {
                    128_u64 * 1024
                })
                .saturating_mul(4),
            ),
            // Outcome grants retain the original documents inside the 128 KiB
            // sandbox. Its compact model view and any detail reads still share
            // the separate resolved cumulative Attempt input budget.
            max_tokens: if definition.purpose == LEARNING_OUTCOME_WORKER_RECIPE_ID {
                32 * 1024
            } else {
                definition.budget.max_input_tokens
            },
            allow_raw_reread: false,
        },
        if outcome { evidence_read_grants() } else { vec![] },
        if outcome { evidence_read_tool_specs(store)? } else { vec![] },
        OutputContract {
            artifact_kind: definition.output_kind,
            schema,
        },
        definition.budget,
        active_retry_policy(),
        if outcome { definition.termination } else { TerminationPolicy { max_child_tasks:32, max_depth:32, ..definition.termination } },
        definition.on_failure,
    )?;
    contract.deliberation_policy = DeliberationPolicy::Required;
    contract.contract_hash = contract.expected_hash()?;
    contract.validate()?;
    Ok(contract)
}

fn governed_context_sources() -> BTreeSet<String> {
    GOVERNED_EVIDENCE_SOURCE_FAMILIES
        .into_iter()
        .chain([
            "akzio.ingest",
            "akzio.agent",
            "akzio.operator",
            "akzio.execution",
            "akzio-learning",
            "akzio.learning",
        ])
        .map(str::to_owned)
        .collect()
}

fn evidence_read_grants() -> Vec<ToolGrant> {
    vec![ToolGrant {
        kind: ToolKind::ReadEvidence,
        // Context selection and tool results share the same source authority.
        // ContextBroker additionally validates kind, producer, run and lifecycle.
        allowed_sources: governed_context_sources().into_iter().collect(),
    }]
}

fn active_retry_policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 2,
        initial_backoff_ms: 250,
        retry_transport: true,
        retry_rate_limited: true,
        retry_invalid_output: true,
    }
}

fn validate_proposal_at(proposal: &DecisionDraft, now: DateTime<Utc>) -> ResearchResult<()> {
    if let Some(plan) = &proposal.research_allocation {
        let errors = plan.allocations.iter().enumerate().filter_map(|(index, row)| {
            row.validate().err().map(|error| format!(
                "result.research_allocation.allocations[{index}] asset={} target_weight_ppm={}: {error}",
                row.asset.symbol(), row.target_weight_ppm.0))
        }).collect::<Vec<_>>();
        if !errors.is_empty() {
            return Err(ResearchError::InvalidOutput(format!(
                "{}; zero-weight rows require a nonempty abstention_reason; nonzero rows require abstention_reason=null and nonempty supporting_horizons/evidence_refs. Preserve valid rows and numeric conclusions.",
                errors.join("; "))));
        }
    }
    proposal
        .validate()
        .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;
    if proposal.forecasts.iter().any(|forecast| {
        forecast
            .thesis
            .as_ref()
            .is_some_and(|thesis| thesis.thesis_valid_until <= now)
    }) {
        return Err(ResearchError::InvalidOutput(format!(
            "Every thesis_valid_until must be strictly after submission time {now}. Evidence cutoff and context creation time are historical timestamps, not thesis expiry. Choose a justified future expiry with enough time for DecisionGate; preserve forecasts, evidence gaps and allocation conclusions."
        )));
    }
    Ok(())
}

fn validate_claim_submission(claim: &ResearchClaim) -> ResearchResult<()> {
    let mut evidence = BTreeSet::new();
    for ground in &claim.grounds {
        if !evidence.insert(&ground.evidence) {
            return Err(ResearchError::InvalidOutput(format!(
                "research.grounds has duplicate evidence {}. Cite each evidence artifact once; combine its supported asset scopes in that ground's assets array and preserve the evidence's actual domain, role and support. Do not invent a replacement source.",
                ground.evidence.artifact_id
            )));
        }
    }
    claim
        .validate()
        .map_err(|error| ResearchError::InvalidOutput(error.to_string()))
}

fn validate_critique_submission(critique: &ResearchCritique) -> ResearchResult<()> {
    if critique.verification_status == ClaimVerificationStatus::Supported {
        if critique.supporting_refs.is_empty() {
            return Err(ResearchError::InvalidOutput(
                "SUPPORTED requires at least one supporting_ref whose authority is not unrated and whose temporal_validity is valid_at_decision_cutoff; supporting_refs is empty"
                    .to_owned(),
            ));
        }
        if !critique.conflicting_refs.is_empty() {
            return Err(ResearchError::InvalidOutput(format!(
                "SUPPORTED requires conflicting_refs=[] but received evidence [{}]. Preserve genuine counterevidence; if it prevents a supported verdict, use CONTRADICTED or NOT_ENOUGH_INFORMATION rather than deleting it",
                critique
                    .conflicting_refs
                    .iter()
                    .map(|reference| reference.evidence.artifact_id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        let invalid_support = critique
            .supporting_refs
            .iter()
            .filter(|reference| !reference.is_current_authoritative())
            .map(|reference| {
                format!(
                    "{} (authority={:?}, temporal_validity={:?})",
                    reference.evidence.artifact_id,
                    reference.authority,
                    reference.temporal_validity
                )
            })
            .collect::<Vec<_>>();
        if !invalid_support.is_empty() {
            return Err(ResearchError::InvalidOutput(format!(
                "SUPPORTED has supporting_refs that are not current-authoritative: [{}]. Remove only these refs from supporting_refs, or change the verdict if they are necessary to support it; keep any such evidence in grounds when it remains relevant context",
                invalid_support.join(", ")
            )));
        }
    }
    critique
        .validate()
        .map_err(|error| ResearchError::InvalidOutput(error.to_string()))
}

fn validate_critique_claim_scope(critique: &ResearchCritique, claim: &ResearchClaim, contract_version: u32) -> ResearchResult<()> {
    let assets = claim.grounds.iter().flat_map(|g| g.assets.iter().copied()).collect::<BTreeSet<_>>();
    let gap_assets = assets.iter().copied().chain(claim.evidence_gaps.iter()
        .filter(|_| contract_version >= akzio_domain::REVIEWED_RESEARCH_CONTRACT_VERSION)
        .flat_map(|gap| gap.assets.iter().copied())).collect::<BTreeSet<_>>();
    if critique.evidence_gaps.iter().any(|gap| {
        (!gap.assets.is_empty() && !gap.assets.is_subset(&gap_assets))
            || gap.horizons.iter().any(|h| *h != claim.horizon)
            || gap.supplemental_requests.iter().any(|r| r.assets.iter().any(|a| !gap_assets.contains(a)))
    }) || critique.grounds.iter().any(|g| !g.assets.is_subset(&assets)) {
        return Err(ResearchError::InvalidOutput("Critique scope exceeds target Claim grounds asset x horizon; unrelated portfolio gaps must not change this verdict".into()));
    }
    if critique.verification_status == ClaimVerificationStatus::Supported
        && !critique.supporting_refs.iter().any(|verified| claim.grounds.iter().any(|g| g.evidence == verified.evidence)) {
        return Err(ResearchError::InvalidOutput("SUPPORTED must verify formal Claim.result.grounds; deliberation or new Critic evidence cannot supply missing grounds".into()));
    }
    Ok(())
}

fn research_output_source_refs(
    store: &Store,
    kind: ArtifactKind,
    output: &Value,
    manifest: &ContextManifest,
    now: DateTime<Utc>,
    contract_version: u32,
) -> ResearchResult<Vec<ArtifactRef>> {
    let refs = match kind {
        ArtifactKind::Claim => {
            let claim: ResearchClaim = serde_json::from_value(output.clone()).map_err(|error| {
                ResearchError::InvalidOutput(format!("invalid Claim payload: {error}"))
            })?;
            validate_claim_submission(&claim)?;
            if contract_version >= 63 { validate_supplemental_resources(&claim.evidence_gaps)?; }
            validate_claim_ground_scopes(store, &claim, manifest, contract_version)?;
            claim.source_refs()
        }
        ArtifactKind::Critique => {
            let critique: ResearchCritique =
                serde_json::from_value(output.clone()).map_err(|error| {
                    ResearchError::InvalidOutput(format!("invalid Critique payload: {error}"))
                })?;
            validate_critique_submission(&critique)?;
            if contract_version >= 63 {
                validate_supplemental_resources(&critique.evidence_gaps)?;
                for reference in &critique.supporting_refs {
                    let artifact = store.artifact(&reference.evidence.artifact_id)?;
                    let payload = serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
                    if !news_source_verified(&payload) {
                        return Err(ResearchError::InvalidOutput("unverified news cannot appear in supporting_refs; model authority is not source verification".into()));
                    }
                }
            }
            if contract_version >= akzio_domain::STRUCTURED_RESEARCH_CONTRACT_VERSION {
                let artifact = store.artifact(&critique.target.artifact_id)?;
                let claim: ResearchClaim = serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
                validate_critique_claim_scope(&critique, &claim, contract_version)?;
                // Additional counterevidence is allowed but has to be lawful evidence
                // in this Manifest; it cannot become a missing Analyst ground.
                let review_claim = ResearchClaim { grounds: critique.grounds.clone(), ..claim };
                validate_claim_ground_scopes(store, &review_claim, manifest, contract_version)?;
            }
            critique.source_refs()
        }
        ArtifactKind::ProposalReview => {
            let review: akzio_domain::ProposalReview = serde_json::from_value(output.clone())?;
            review.validate_for_contract(contract_version).map_err(|e| ResearchError::InvalidOutput(e.to_string()))?;
            let mut refs = vec![review.proposal];
            for assessment in review.assessments {
                refs.extend(assessment.evidence_refs);
                refs.extend(assessment.issues.into_iter().flat_map(|issue| issue.evidence_refs));
            }
            if refs.iter().any(|r| !manifest.payload.selections.iter().any(|s| &s.artifact == r)) {
                return Err(ResearchError::InvalidOutput("proposal review references outside manifest".into()));
            }
            refs.sort();
            refs.dedup();
            refs
        }
        ArtifactKind::Resolution => {
            validate_schema_value(output, &resolution_output_schema(), "$")
                .map_err(ResearchError::InvalidOutput)?;
            let resolution: ResearchResolution =
                serde_json::from_value(output.clone()).map_err(|error| {
                    ResearchError::InvalidOutput(format!("invalid Resolution payload: {error}"))
                })?;
            resolution
                .validate()
                .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;
            resolution.source_refs()
        }
        ArtifactKind::RetrospectiveDraft => {
            let draft: akzio_domain::RetrospectiveDraft = serde_json::from_value(output.clone())
                .map_err(|error| {
                    ResearchError::InvalidOutput(format!(
                        "invalid RetrospectiveDraft payload: {error}"
                    ))
                })?;
            draft
                .validate()
                .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;
            let mut refs = draft.source_refs.clone();
            refs.extend(
                draft
                    .lesson_proposals
                    .iter()
                    .flat_map(|p| p.evidence_refs.iter().cloned()),
            );
            refs.extend(
                draft
                    .findings
                    .iter()
                    .flat_map(|finding| finding.artifact_refs.iter().cloned()),
            );
            refs.sort();
            refs.dedup();
            refs
        }
        ArtifactKind::DecisionProposal => {
            let proposal: DecisionDraft =
                serde_json::from_value(output.clone()).map_err(|error| {
                    ResearchError::InvalidOutput(format!(
                        "invalid DecisionProposal payload: {error}"
                    ))
                })?;
            validate_proposal_at(&proposal, now)?;
            if contract_version >= akzio_domain::STRUCTURED_REVIEW_ISSUES_CONTRACT_VERSION {
                let reviews = manifest.payload.selections.iter().filter(|s| s.artifact.kind == ArtifactKind::ProposalReview).collect::<Vec<_>>();
                if reviews.len() > 1 { return Err(ResearchError::InvalidOutput("revision requires exactly one prior review".into())); }
                if let Some(selection) = reviews.first() {
                    let artifact = store.artifact(&selection.artifact.artifact_id)?;
                    let review: akzio_domain::ProposalReview = serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
                    let prior = store.artifact(&review.proposal.artifact_id)?;
                    if prior.blob.hash != review.proposal_hash || !manifest.payload.selections.iter().any(|s| s.artifact == review.proposal) {
                        return Err(ResearchError::InvalidOutput("revision prior proposal binding mismatch".into()));
                    }
                    let previous: DecisionDraft = serde_json::from_slice(&store.read_blob(&prior.blob)?)?;
                    akzio_domain::validate_proposal_revision(&previous,&proposal,&review)
                        .map_err(|e| ResearchError::InvalidOutput(e.to_string()))?;
                }
            }
            if contract_version >= akzio_domain::REVIEWED_RESEARCH_CONTRACT_VERSION {
                akzio_domain::validate_numeric_bases(&proposal.numeric_basis).map_err(|e| ResearchError::InvalidOutput(e.to_string()))?;
                let selected = manifest.payload.selections.iter().map(|s| &s.artifact).collect::<BTreeSet<_>>();
                if proposal.numeric_basis.iter().flat_map(|b| &b.inputs).any(|r| !selected.contains(r)) {
                    return Err(ResearchError::InvalidOutput("numeric_basis input outside selected manifest".into()));
                }
            }

            for reference in proposal.claims.iter().chain(proposal.critiques.iter()) {
                let artifact = store.artifact(&reference.artifact_id)?;
                if artifact.kind != reference.kind {
                    return Err(ResearchError::InvalidOutput(format!(
                        "DecisionProposal reference kind {:?} does not match stored artifact kind {:?}",
                        reference.kind, artifact.kind
                    )));
                }
            }

            let selected = manifest
                .payload
                .selections
                .iter()
                .map(|selection| selection.artifact.clone())
                .collect::<BTreeSet<_>>();
            let selected_claims = selected
                .iter()
                .filter(|reference| reference.kind == ArtifactKind::Claim)
                .cloned()
                .collect::<BTreeSet<_>>();
            let selected_critiques = selected
                .iter()
                .filter(|reference| reference.kind == ArtifactKind::Critique)
                .cloned()
                .collect::<BTreeSet<_>>();
            let submitted_claims = proposal.claims.iter().cloned().collect::<BTreeSet<_>>();
            let submitted_critiques = proposal.critiques.iter().cloned().collect::<BTreeSet<_>>();

            if submitted_claims.is_empty()
                && (!selected_claims.is_empty() || !proposal.evidence.is_empty())
            {
                return Err(ResearchError::InvalidOutput(
                    "DecisionProposal dropped all claims selected by ContextManifest".to_owned(),
                ));
            }
            if !selected_claims.is_subset(&submitted_claims) {
                return Err(ResearchError::InvalidOutput(
                    format!("DecisionProposal claims do not close over ContextManifest; missing claims: {:?}. Include blocked/neutral claims for provenance, not endorsement.", selected_claims.difference(&submitted_claims).map(|r| &r.artifact_id).collect::<Vec<_>>()),
                ));
            }
            if !selected_critiques.is_subset(&submitted_critiques) {
                return Err(ResearchError::InvalidOutput(
                    format!("DecisionProposal critiques do not close over ContextManifest; missing critiques: {:?}. Include unsupported critiques for provenance, not endorsement.", selected_critiques.difference(&submitted_critiques).map(|r| &r.artifact_id).collect::<Vec<_>>()),
                ));
            }

            let declared_evidence = proposal.evidence.iter().cloned().collect::<BTreeSet<_>>();
            let mut refs = proposal
                .claims
                .iter()
                .chain(proposal.critiques.iter())
                .chain(proposal.evidence.iter())
                .chain(proposal.numeric_basis.iter().flat_map(|basis| basis.inputs.iter()))
                .chain(
                    proposal
                        .research_allocation
                        .iter()
                        .flat_map(|plan| plan.allocations.iter())
                        .flat_map(|allocation| allocation.evidence_refs.iter()),
                )
                .cloned()
                .collect::<Vec<_>>();
            let mut claims = Vec::new();

            for reference in proposal.claims.iter().chain(proposal.critiques.iter()) {
                let artifact = store.artifact(&reference.artifact_id)?;
                let payload = store.read_blob(&artifact.blob)?;
                let source_refs = match reference.kind {
                    ArtifactKind::Claim => {
                        let claim: ResearchClaim = serde_json::from_slice(&payload)?;
                        claim
                            .validate()
                            .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;
                        validate_claim_ground_scopes(store, &claim, manifest, contract_version)?;
                        claims.push(claim.clone());
                        claim.source_refs()
                    }
                    ArtifactKind::Critique => {
                        let critique: ResearchCritique = serde_json::from_slice(&payload)?;
                        critique
                            .validate()
                            .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;
                        critique.source_refs()
                    }
                    _ => unreachable!("DecisionProposal references are schema-bounded"),
                };
                validate_decision_source_closure(
                    reference.kind,
                    &source_refs,
                    &submitted_claims,
                    &declared_evidence,
                    &selected,
                )?;
                refs.extend(source_refs);
            }
            let verified_claims = proposal
                .claims
                .iter()
                .cloned()
                .zip(claims.iter().cloned())
                .collect::<Vec<_>>();
            let critiques = proposal
                .critiques
                .iter()
                .map(|reference| {
                    let artifact = store.artifact(&reference.artifact_id)?;
                    Ok(serde_json::from_slice::<ResearchCritique>(
                        &store.read_blob(&artifact.blob)?,
                    )?)
                })
                .collect::<ResearchResult<Vec<_>>>()?;
            let validate_slots = if contract_version >= akzio_domain::STRUCTURED_RESEARCH_CONTRACT_VERSION {
                akzio_domain::validate_verified_forecast_slots
            } else { akzio_domain::validate_legacy_verified_forecast_slots };
            validate_slots(&proposal, &verified_claims, &critiques)
                .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;
            if contract_version < akzio_domain::STRUCTURED_RESEARCH_CONTRACT_VERSION {
                validate_decision_evidence_sufficiency(&proposal, &claims)
                    .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;
            } else {
                akzio_domain::validate_structured_allocation_eligibility(&proposal, &verified_claims, &critiques)
                    .map_err(|error| ResearchError::InvalidOutput(format!("allocation must cite its Rust-eligible Claim: {error}")))?;
            }
            validate_research_allocation_sufficiency(&proposal, &verified_claims, &critiques)?;
            refs.sort();
            refs.dedup();
            refs
        }
        _ => return Ok(vec![]),
    };
    let selected = manifest
        .payload
        .selections
        .iter()
        .map(|selection| selection.artifact.clone())
        .collect::<BTreeSet<_>>();
    if refs.iter().any(|reference| {
        !(selected.contains(reference)
            || reference.kind == ArtifactKind::NormalizedEvidence
            || reference.kind == ArtifactKind::SemanticDetail)
    }) {
        return Err(ResearchError::InvalidOutput(
            "research artifact cited an artifact outside ContextManifest".to_owned(),
        ));
    }
    Ok(refs)
}

fn validate_research_allocation_sufficiency(
    proposal: &DecisionDraft,
    claims: &[(ArtifactRef, ResearchClaim)],
    critiques: &[ResearchCritique],
) -> ResearchResult<()> {
    let Some(plan) = proposal.research_allocation.as_ref() else {
        return Ok(());
    };
    // A supported claim permits a recommendation; it never obliges the
    // synthesizer to take risk. Explicit cash remains a valid research result.
    for allocation in plan
        .allocations
        .iter()
        .filter(|row| row.target_weight_ppm.0 > 0)
    {
        let supported = allocation.supporting_horizons.iter().any(|horizon| {
            proposal.forecasts.iter().any(|forecast| {
                forecast.asset == allocation.asset
                    && forecast.horizon == *horizon
                    && forecast.expected_return_ppm > 0
            }) && claims.iter().any(|(claim_ref, claim)| {
                if claim.horizon != *horizon
                    || claim.stance != akzio_domain::ClaimStance::Bullish
                    || claim
                        .evidence_gaps
                        .iter()
                        .any(|gap| gap.blocks_slot(allocation.asset, *horizon, claim.horizon))
                {
                    return false;
                }
                let domains = claim
                    .grounds
                    .iter()
                    .filter(|ground| {
                        ground.role == EvidenceGroundRole::Directional
                            && ground.assets.contains(&allocation.asset)
                    })
                    .filter_map(|ground| ground.domain)
                    .collect::<BTreeSet<_>>();
                if ![ResearchShard::PriceMarketStructure, ResearchShard::Macro]
                    .into_iter()
                    .all(|domain| domains.contains(&domain))
                {
                    return false;
                }
                critiques.iter().enumerate().any(|(index, critique)| {
                    critique.target == *claim_ref
                        && critique.verification_status == ClaimVerificationStatus::Supported
                        && !critique.blocks_slot(allocation.asset, *horizon, claim.horizon)
                        && allocation.evidence_refs.iter().any(|reference| {
                            reference == claim_ref
                                || proposal.critiques.get(index) == Some(reference)
                                || claim.grounds.iter().any(|ground| {
                                    ground.role == EvidenceGroundRole::Directional
                                        && ground.assets.contains(&allocation.asset)
                                        && ground.evidence == *reference
                                })
                        })
                })
            })
        });
        if !supported {
            return Err(ResearchError::InvalidOutput(format!(
                "research_allocation target {} has no cited positive supporting_horizon; preserve a zero abstention or cite the matching Bullish price+macro Claim, a non-blocking SUPPORTED Critique, or its asset-scoped grounds",
                allocation.asset.symbol()
            )));
        }
    }
    Ok(())
}

fn validate_decision_source_closure(
    owner_kind: ArtifactKind,
    source_refs: &[ArtifactRef],
    submitted_claims: &BTreeSet<ArtifactRef>,
    declared_evidence: &BTreeSet<ArtifactRef>,
    selected: &BTreeSet<ArtifactRef>,
) -> ResearchResult<()> {
    for source in source_refs {
        match source.kind {
            ArtifactKind::Claim if owner_kind == ArtifactKind::Critique => {
                if !submitted_claims.contains(source) {
                    return Err(ResearchError::InvalidOutput(
                        "DecisionProposal claims do not close over Critique target".to_owned(),
                    ));
                }
            }
            ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail => {
                if !declared_evidence.contains(source)
                    && (owner_kind != ArtifactKind::Critique || selected.contains(source))
                {
                    return Err(ResearchError::InvalidOutput(format!(
                        "DecisionProposal evidence does not close over claim/critique grounds; missing evidence_ref artifact_id={} kind={:?}; add this exact selected reference to proposal.evidence",
                        source.artifact_id, source.kind
                    )));
                }
            }
            _ => {
                return Err(ResearchError::InvalidOutput(
                    "DecisionProposal claim/critique source kind is not permitted".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_claim_ground_scopes(
    store: &Store,
    claim: &ResearchClaim,
    manifest: &ContextManifest,
    contract_version: u32,
) -> ResearchResult<()> {
    let selected = manifest
        .payload
        .selections
        .iter()
        .map(|selection| {
            (
                selection.artifact.artifact_id.clone(),
                selection.artifact.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();

    for ground in &claim.grounds {
        let Some(selected_ref) = selected.get(&ground.evidence.artifact_id) else {
            return Err(ResearchError::InvalidOutput(
                "claim ground is outside ContextManifest".to_owned(),
            ));
        };
        if selected_ref != &ground.evidence {
            return Err(ResearchError::InvalidOutput(
                "claim ground kind does not match ContextManifest".to_owned(),
            ));
        }

        let artifact = store.artifact(&ground.evidence.artifact_id)?;
        let payload: Value = serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
        let scope = evidence_asset_scope(&payload)?;
        let domain = evidence_domain(&payload)?;
        if ground.domain != domain
            && (ground.role == EvidenceGroundRole::Directional
                || (ground.domain.is_some() && domain.is_some()))
        {
            return Err(ResearchError::InvalidOutput(format!(
                "ground domain {:?} does not match evidence resource {} (expected {:?}); choose the selected artifact whose resource matches the declared domain",
                ground.domain,
                payload
                    .get("resource")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown"),
                domain
            )));
        }

        if contract_version >= 63 && !news_source_verified(&payload)
            && (ground.role != EvidenceGroundRole::Descriptive
                || !ground.assets.is_empty() || ground.domain.is_some()) {
            return Err(ResearchError::InvalidOutput(format!(
                "news ground {} is not source verified; citations_complete and model_reviewed do not grant direction. Use role=descriptive, assets=[] and domain=null", ground.evidence.artifact_id)));
        }
        if ground.role == EvidenceGroundRole::Directional {
            if ground.evidence.kind != ArtifactKind::NormalizedEvidence
                || !evidence_has_complete_citations(&payload)
                || domain.is_none()
                || scope.is_none()
                || ground.assets.is_empty()
                || scope
                    .as_ref()
                    .is_some_and(|assets| !ground.assets.is_subset(assets))
            {
                // Name the rejected ground and the scope it left. A repair turn
                // that is only told the rule has to guess which ground to edit.
                return Err(ResearchError::InvalidOutput(format!(
                    "directional ground assets must stay within the scope of a citation-complete normalized evidence artifact: \
                     ground evidence {} (kind {:?}, resource {}) declares assets {} but its payload scope is {} \
                     (citations_complete={}, domain={:?}); keep only the assets in that scope or use role=descriptive with assets=[]",
                    ground.evidence.artifact_id.0,
                    ground.evidence.kind,
                    payload
                        .get("resource")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown"),
                    describe_assets(&ground.assets),
                    scope.as_ref().map_or_else(|| "unknown".to_owned(), describe_assets),
                    evidence_has_complete_citations(&payload),
                    domain,
                )));
            }
        } else if let Some(scope) = scope {
            if !ground.assets.is_subset(&scope) {
                return Err(ResearchError::InvalidOutput(format!(
                    "descriptive ground assets exceed evidence payload scope: ground evidence {} (resource {}) \
                     declares assets {} but its payload scope is {}; drop the assets outside that scope, \
                     or use assets=[] for a scope-free descriptive document",
                    ground.evidence.artifact_id.0,
                    payload
                        .get("resource")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown"),
                    describe_assets(&ground.assets),
                    describe_assets(&scope),
                )));
            }
        } else if !ground.assets.is_empty() {
            return Err(ResearchError::InvalidOutput(format!(
                "unknown evidence scope cannot declare assets: ground evidence {} (resource {}) \
                 declares assets {} but its payload has no asset scope; use assets=[] and domain=null",
                ground.evidence.artifact_id.0,
                payload
                    .get("resource")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown"),
                describe_assets(&ground.assets),
            )));
        }
    }
    Ok(())
}

fn evidence_has_complete_citations(payload: &Value) -> bool {
    payload
        .pointer("/quality/citations_complete")
        .and_then(Value::as_bool)
        == Some(true)
}

/// Render an asset set for a rejection message. `[]` is spelled out so an empty
/// declared set reads differently from an absent one.
fn describe_assets(assets: &BTreeSet<Asset>) -> String {
    if assets.is_empty() {
        return "[]".to_owned();
    }
    format!(
        "[{}]",
        assets
            .iter()
            .map(Asset::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn evidence_asset_scope(payload: &Value) -> ResearchResult<Option<BTreeSet<Asset>>> {
    let resource = payload.get("resource").and_then(Value::as_str);
    if let Some(resource) = resource {
        if let Some(symbol) = resource
            .strip_prefix("bars:")
            .and_then(|value| value.split(':').next())
        {
            let asset = Asset::try_from(symbol).map_err(|error| {
                ResearchError::InvalidOutput(format!("invalid bar asset scope: {error}"))
            })?;
            return Ok(Some(BTreeSet::from([asset])));
        }

        if let Some(symbol) = resource
            .strip_prefix("news:")
            .and_then(|value| value.split(':').next())
        {
            let asset = Asset::try_from(symbol).map_err(|error| {
                ResearchError::InvalidOutput(format!("invalid news asset scope: {error}"))
            })?;
            return Ok(Some(BTreeSet::from([asset])));
        }

        if resource.starts_with("series:") {
            let series = resource.split(':').nth(1).unwrap_or_default();
            if matches!(series, "DFF" | "DFII10" | "VIXCLS" | "DGS2" | "DGS10") {
                return Ok(Some(Asset::EXECUTABLE.into_iter().collect()));
            }
        }

        if resource == "paper.positions" {
            return scoped_symbols(payload.pointer("/value"));
        }
        if resource == "paper.quotes" {
            return scoped_object_keys(payload.pointer("/value/quotes"));
        }
        if resource == "paper.account"
            || resource == "paper.clock"
            || resource == "paper.open_orders"
            || resource.starts_with("paper.fills:")
        {
            return Ok(Some(BTreeSet::new()));
        }
        return Ok(None);
    }

    if payload.get("quotes").is_some() {
        return scoped_object_keys(payload.get("quotes"));
    }
    if payload.get("positions").is_some() {
        return scoped_object_keys(payload.get("positions"));
    }
    Ok(None)
}

fn evidence_domain(payload: &Value) -> ResearchResult<Option<ResearchShard>> {
    let Some(resource) = payload.get("resource").and_then(Value::as_str) else {
        return Ok(None);
    };
    if resource.starts_with("bars:") {
        return Ok(Some(ResearchShard::PriceMarketStructure));
    }
    if resource.starts_with("news:") {
        return Ok(Some(ResearchShard::NewsEvent));
    }
    if resource.starts_with("series:") {
        let series = resource.split(':').nth(1).unwrap_or_default();
        return Ok(
            matches!(series, "DFF" | "DFII10" | "VIXCLS" | "DGS2" | "DGS10")
                .then_some(ResearchShard::Macro),
        );
    }
    if resource.starts_with("research:leveraged_etf_terms:") {
        // The terms document is issuer-owned descriptive product material.
        // Preserve the legacy FundamentalsSemiconductor vocabulary for older
        // producer outputs, while the current prompt asks the model to keep
        // this ground descriptive with no asset/domain claim.
        return Ok(Some(ResearchShard::FundamentalsSemiconductor));
    }
    Ok(None)
}

fn scoped_symbols(value: Option<&Value>) -> ResearchResult<Option<BTreeSet<Asset>>> {
    let Some(Value::Array(items)) = value else {
        return Err(ResearchError::InvalidOutput(
            "asset-scoped evidence payload is not an array".to_owned(),
        ));
    };
    let mut assets = BTreeSet::new();
    for item in items {
        let Some(symbol) = item.get("symbol").and_then(Value::as_str) else {
            return Err(ResearchError::InvalidOutput(
                "asset-scoped evidence item has no symbol".to_owned(),
            ));
        };
        assets.insert(Asset::try_from(symbol).map_err(|error| {
            ResearchError::InvalidOutput(format!("invalid asset scope: {error}"))
        })?);
    }
    Ok(Some(assets))
}

fn scoped_object_keys(value: Option<&Value>) -> ResearchResult<Option<BTreeSet<Asset>>> {
    let Some(Value::Object(items)) = value else {
        return Err(ResearchError::InvalidOutput(
            "asset-scoped evidence payload is not an object".to_owned(),
        ));
    };
    let mut assets = BTreeSet::new();
    for symbol in items.keys() {
        assets.insert(Asset::try_from(symbol.as_str()).map_err(|error| {
            ResearchError::InvalidOutput(format!("invalid asset scope: {error}"))
        })?);
    }
    Ok(Some(assets))
}

#[cfg(test)]
mod allocation_authority_tests {
    use super::*;
    use akzio_domain::{
        ClaimStance, DecisionHorizon, ResearchAllocationPlan, ResearchAssetAllocation, WeightPpm,
    };

    fn fixture() -> (
        DecisionDraft,
        Vec<(ArtifactRef, ResearchClaim)>,
        Vec<ResearchCritique>,
    ) {
        let reference = |label: &str, kind| ArtifactRef {
            artifact_id: ArtifactId(akzio_domain::ContentHash::of_bytes(label.as_bytes())),
            kind,
        };
        let claim_ref = reference("claim", ArtifactKind::Claim);
        let price = reference("price", ArtifactKind::NormalizedEvidence);
        let macro_ref = reference("macro", ArtifactKind::NormalizedEvidence);
        let claim: ResearchClaim = serde_json::from_value(json!({
            "schema_version": DOMAIN_SCHEMA_VERSION, "topic": "QQQ T1", "statement": "Supported positive research",
            "horizon": "t1", "stance": "bullish", "materiality_ppm": 600000, "confidence_ppm": 800000,
            "grounds": [
                {"evidence": price, "support": "price", "role": "directional", "assets": ["QQQ"], "domain": "price_market_structure"},
                {"evidence": macro_ref, "support": "macro", "role": "directional", "assets": ["QQQ"], "domain": "macro"}
            ], "evidence_gaps": []
        })).unwrap();
        let critique: ResearchCritique = serde_json::from_value(json!({
            "schema_version": DOMAIN_SCHEMA_VERSION, "target": claim_ref, "topic": "QQQ T1 review",
            "severity": "low", "blocker": false, "rationale": "supported", "grounds": claim.grounds,
            "evidence_gaps": [], "verification_status": "supported", "supporting_refs": [{"evidence": price, "authority": "official", "temporal_validity": "valid_at_decision_cutoff"}], "conflicting_refs": []
        })).unwrap();
        let plan = ResearchAllocationPlan {
            cash_weight_ppm: WeightPpm(1_000_000),
            allocations: Asset::EXECUTABLE
                .into_iter()
                .map(|asset| ResearchAssetAllocation {
                    asset,
                    target_weight_ppm: WeightPpm::ZERO,
                    supporting_horizons: vec![],
                    evidence_refs: vec![],
                    rationale: "Explicit abstention despite available research".to_owned(),
                    abstention_reason: Some("No suitable allocation".to_owned()),
                })
                .collect(),
        };
        let draft = DecisionDraft {
                        numeric_basis: Vec::new(),
summary: "research only".to_owned(),
            confidence_ppm: 800000,
            forecasts: vec![akzio_domain::Forecast {
                asset: Asset::Qqq,
                horizon: DecisionHorizon::T1,
                positive_return_probability_ppm: 700000,
                expected_return_ppm: 10000,
                thesis: None,
            }],
            research_allocation: Some(plan),
            claims: vec![claim_ref.clone()],
            critiques: vec![],
            evidence: vec![price, macro_ref],
            material_conflicts: vec![],
            hard_blockers: vec![],
            soft_warnings: vec![],
            applied_learning_refs: vec![],
            rejected_learning_refs: vec![],
        };
        (draft, vec![(claim_ref, claim)], vec![critique])
    }

    #[test]
    fn proposal_rejection_identifies_every_invalid_allocation_row() {
        let (mut draft, _, _) = fixture();
        for row in &mut draft.research_allocation.as_mut().unwrap().allocations {
            row.abstention_reason = None;
        }
        let message = validate_proposal_at(&draft, Utc::now()).unwrap_err().to_string();
        for asset in Asset::EXECUTABLE {
            assert!(message.contains(&format!("asset={}", asset.symbol())));
        }
        assert!(message.contains("allocations[3]"));
        assert!(message.contains("nonempty abstention_reason"));
    }

    fn allocate_qqq(draft: &mut DecisionDraft) {
        let plan = draft.research_allocation.as_mut().unwrap();
        plan.cash_weight_ppm = WeightPpm(900_000);
        let row = plan
            .allocations
            .iter_mut()
            .find(|row| row.asset == Asset::Qqq)
            .unwrap();
        row.target_weight_ppm = WeightPpm(100_000);
        row.supporting_horizons = vec![DecisionHorizon::T1];
        row.evidence_refs = draft.claims.clone();
        row.abstention_reason = None;
    }

    #[test]
    fn supported_research_allows_explicit_cash() {
        let (draft, claims, critiques) = fixture();
        assert!(validate_research_allocation_sufficiency(&draft, &claims, &critiques).is_ok());
    }

    #[test]
    fn allocation_rejects_absent_or_bearish_support_and_accepts_positive_support() {
        let (mut draft, mut claims, critiques) = fixture();
        allocate_qqq(&mut draft);
        assert!(validate_research_allocation_sufficiency(&draft, &claims, &critiques).is_ok());
        let mut wrong_horizon = draft.clone();
        wrong_horizon
            .research_allocation
            .as_mut()
            .unwrap()
            .allocations
            .iter_mut()
            .find(|row| row.asset == Asset::Qqq)
            .unwrap()
            .supporting_horizons = vec![DecisionHorizon::T3];
        assert!(
            validate_research_allocation_sufficiency(&wrong_horizon, &claims, &critiques).is_err()
        );
        let mut neutral_forecast = draft.clone();
        neutral_forecast.forecasts[0].expected_return_ppm = 0;
        neutral_forecast.forecasts[0].positive_return_probability_ppm = 500_000;
        assert!(
            validate_research_allocation_sufficiency(&neutral_forecast, &claims, &critiques)
                .is_err()
        );
        let mut unrelated_reference = draft.clone();
        unrelated_reference
            .research_allocation
            .as_mut()
            .unwrap()
            .allocations
            .iter_mut()
            .find(|row| row.asset == Asset::Qqq)
            .unwrap()
            .evidence_refs = vec![ArtifactRef {
            artifact_id: ArtifactId(akzio_domain::ContentHash::of_bytes(b"unrelated")),
            kind: ArtifactKind::NormalizedEvidence,
        }];
        assert!(validate_research_allocation_sufficiency(
            &unrelated_reference,
            &claims,
            &critiques
        )
        .is_err());
        assert!(validate_research_allocation_sufficiency(&draft, &[], &[]).is_err());
        claims[0].1.stance = ClaimStance::Bearish;
        assert!(validate_research_allocation_sufficiency(&draft, &claims, &critiques).is_err());
    }
}

fn news_source_verified(payload: &Value) -> bool {
    !payload.get("resource").and_then(Value::as_str).is_some_and(|r| r.starts_with("news:"))
        || payload.pointer("/value/source_document/source_verified").and_then(Value::as_bool) == Some(true)
}

fn validate_supplemental_resources(gaps: &[akzio_domain::EvidenceGap]) -> ResearchResult<()> {
    for need in gaps.iter().flat_map(|gap| &gap.supplemental_needs) {
        let source: akzio_ingest::EvidenceSource = serde_json::from_value(json!(need.source_family))
            .map_err(|e| ResearchError::InvalidOutput(format!("supplemental source: {e}")))?;
        akzio_ingest::GovernedResource::parse(source, &need.resource)
            .map_err(|e| ResearchError::InvalidOutput(format!("invalid supplemental resource {}: {e}; use one governed asset per request", need.resource)))?;
    }
    Ok(())
}

#[cfg(test)]
mod review_submission_tests {
    use super::*;
    #[test]
    fn submission_and_wire_schema_agree_on_unverified_news() {
        let now = Utc::now();
        let (store, runtime, attempt) = super::late_model_tests::isolated_outcome_attempt(120, now);
        let payload = json!({"resource":"news:SOXX:2026-09-08:2026-09-22:market",
            "quality":{"citations_complete":true},
            "value":{"source_document":{"source_verified":false,"status":"model_reviewed"}}});
        let artifact = Artifact::new(
            ArtifactKind::NormalizedEvidence, store.stage_json(&payload).unwrap(),
            "evidence.normalize", ArtifactLifecycle::RunScoped,
            ArtifactProvenance { source_family:"news_web".into(), observed_at:Some(now),
                retrieved_at:now, source_uri:None, confidence_ppm:1_000_000,
                producer_contract_hash:None },
            Some(attempt.permit.artifact_origin()), vec![], now,
        ).unwrap();
        store.write_task_artifact(&attempt.permit, &artifact, LifecycleEventType::ArtifactCommitted, now).unwrap();
        let reference = ArtifactRef {artifact_id:artifact.artifact_id, kind:artifact.kind};
        let contract = &runtime.contract(attempt.node.contract_hash.as_ref().unwrap()).unwrap().contract;
        let manifest = ContextBroker::new(store.clone()).assemble(
            &attempt.permit, contract, &akzio_domain::ContextQueryScope::for_node(&attempt.node), attempt.node.input_artifacts.iter().cloned().chain([reference.clone()]), now, Duration::minutes(5),
        ).unwrap();
        assert!(manifest.payload.selections.iter().any(|s| s.artifact == reference));
        let mut claim = json!({"schema_version":DOMAIN_SCHEMA_VERSION,"topic":"SOXX news",
            "statement":"Market background, not an asset-specific event", "horizon":"t5",
            "stance":"neutral","materiality_ppm":100000,"confidence_ppm":100000,
            "grounds":[{"evidence":reference,"support":"Market background","role":"directional",
                "assets":["SOXX"],"domain":"news_event"}],"evidence_gaps":[]});
        let validate = |value: &Value, version| research_output_source_refs(
            &store, ArtifactKind::Claim, value, &manifest, now, version);
        assert!(validate(&claim, 61).is_ok(), "historical qualification is unchanged");
        assert!(matches!(validate(&claim, 63), Err(ResearchError::InvalidOutput(message))
            if message.contains("not source verified")));
        claim["grounds"][0]["role"] = json!("descriptive");
        // The wire schema already requires scope-free background. The business
        // validator must enforce the same rule for non-structured submissions.
        assert!(validate(&claim, 63).is_err());
        claim["grounds"][0]["assets"] = json!([]);
        claim["grounds"][0]["domain"] = Value::Null;
        assert!(validate(&claim, 63).is_ok());
        let mut schema = json!({"properties":{"result":{"properties":{"grounds":{"items":evidence_ground_schema()}}}}});
        schema["properties"]["result"]["properties"]["grounds"]["items"]["properties"]["evidence"]["properties"]["artifact_id"]["enum"] = json!([reference.artifact_id]);
        bind_ground_scope_schema(&store, &manifest, &mut schema, 63).unwrap();
        let ground_schema = &schema["properties"]["result"]["properties"]["grounds"]["items"];
        assert!(validate_schema_value(&claim["grounds"][0], ground_schema, "$").is_ok());
        claim["grounds"][0]["role"] = json!("directional");
        assert!(validate_schema_value(&claim["grounds"][0], ground_schema, "$").is_err());
        claim["grounds"][0]["role"] = json!("descriptive");
        claim["evidence_gaps"] = json!([{"topic":"news","rationale":"refresh background",
            "impact":"warning","retriable":true,"supplemental_needs":[{
                "schema_version":DOMAIN_SCHEMA_VERSION,"source_family":"news_web",
                "resource":"news:TQQQ,QQQ,SOXX,SOXL:2026-09-08:2026-09-22:market",
                "query":"news","assets":["SOXX"],"window_start":null,"window_end":null,
                "max_age_secs":3600,"max_results":8}]}]);
        assert!(validate(&claim, 61).is_ok());
        assert!(matches!(validate(&claim, 63), Err(ResearchError::InvalidOutput(message))
            if message.contains("invalid supplemental resource")));
        claim["evidence_gaps"][0]["supplemental_needs"][0]["resource"] =
            json!("news:SOXX:2026-09-08:2026-09-22:market");
        assert!(validate(&claim, 63).is_ok());
    }
    #[test]
    fn model_review_and_complete_citations_do_not_grant_news_direction() {
        let mut payload=json!({"resource":"news:SOXX:2026-09-08:2026-09-22:market",
            "quality":{"citations_complete":true},"value":{"source_document":{"source_verified":false,"status":"model_reviewed"}}});
        assert!(!news_source_verified(&payload));
        payload["value"]["source_document"]["source_verified"]=json!(true);
        assert!(news_source_verified(&payload));
        payload["value"]["source_document"]["source_verified"]=Value::Null;
        assert!(!news_source_verified(&payload));
        payload["resource"]=json!("series:DFF");
        assert!(news_source_verified(&payload));
    }
    #[test]
    fn supplemental_requests_use_the_adapter_resource_parser() {
        let mut gaps: Vec<akzio_domain::EvidenceGap>=serde_json::from_value(json!([{
            "topic":"news","rationale":"refresh","impact":"warning","retriable":true,
            "supplemental_needs":[{"schema_version":DOMAIN_SCHEMA_VERSION,"source_family":"news_web",
                "resource":"news:TQQQ,QQQ,SOXX,SOXL:2026-09-08:2026-09-22:market", "query":"news","assets":["QQQ"],
                "window_start":null,"window_end":null,"max_age_secs":3600,"max_results":8}]
        }])).unwrap();
        assert!(validate_supplemental_resources(&gaps).is_err());
        gaps[0].supplemental_needs[0].resource="news:QQQ:2026-09-08:2026-09-22:market".into();
        assert!(validate_supplemental_resources(&gaps).is_ok());
        gaps[0].supplemental_needs[0].source_family="fred".into();
        assert!(validate_supplemental_resources(&gaps).is_err());
    }
}
