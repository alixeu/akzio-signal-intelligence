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
    #[error("Agent model rate limited: {0}")]
    RateLimited(String),
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
            Self::InvalidOutput(_) | Self::MissingFinalOutput => Some(RetryCause::InvalidOutput),
            Self::ModelDebug {
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

fn canonical_active_contracts(store: &V2Store) -> ResearchResult<Vec<AgentContract>> {
    [
        CanonicalContractDefinition {
            purpose: PLANNER_RECIPE_ID,
            responsibility: "Lower a bounded research objective into a WorkflowProposalDraft using only installed research recipes and inline EvidenceNeed requests.",
            output_kind: ArtifactKind::WorkflowProposalDraft,
            output_schema: planner_draft_output_schema(),
            permitted_kinds: BTreeSet::from([
                ArtifactKind::NormalizedEvidence,
                ArtifactKind::SemanticDetail,
                ArtifactKind::Claim,
                ArtifactKind::Critique,
            ]),
            min_context_artifacts: 0,
            budget: TaskBudget {
                max_input_tokens: 12_000,
                max_output_tokens: 2_000,
                max_wall_time_secs: 120,
                max_tool_calls: 4,
            },
            termination: TerminationPolicy {
                max_child_tasks: PLANNER_MAX_DRAFT_TASKS,
                max_depth: 2,
                require_evidence: false,
                stop_when_evidence_complete: true,
            },
            on_failure: FailureDisposition::FailRun,
        },
        CanonicalContractDefinition {
            purpose: RESEARCH_ANALYST_RECIPE_ID,
            responsibility: "Produce evidence-linked, bounded research claims for one shard of the approved workflow.",
            output_kind: ArtifactKind::Claim,
            output_schema: claim_output_schema(),
            permitted_kinds: BTreeSet::from([
                ArtifactKind::NormalizedEvidence,
                ArtifactKind::SemanticDetail,
            ]),
            min_context_artifacts: 1,
            budget: TaskBudget {
            max_input_tokens: 48_000,
                max_output_tokens: 1_500,
                max_wall_time_secs: 120,
                max_tool_calls: 4,
            },
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
            responsibility: "Challenge material claims and surface evidence or risk gaps without changing facts or execution authority.",
            output_kind: ArtifactKind::Critique,
            output_schema: critique_output_schema(),
 permitted_kinds: BTreeSet::from([
 ArtifactKind::Claim,
 ArtifactKind::SemanticDetail,
 ArtifactKind::DeliberationNote,
 ]),
            min_context_artifacts: 1,
            budget: TaskBudget {
                max_input_tokens: 6_000,
                max_output_tokens: 1_500,
                max_wall_time_secs: 90,
                max_tool_calls: 2,
            },
            termination: TerminationPolicy {
                max_child_tasks: 1,
                max_depth: 1,
                require_evidence: true,
                stop_when_evidence_complete: true,
            },
            on_failure: FailureDisposition::SkipTask,
        },
        CanonicalContractDefinition {
            purpose: RESEARCH_SYNTHESIZER_RECIPE_ID,
            responsibility: "Synthesize approved claims and critiques into a DecisionProposal with typed blockers for Rust-owned gates.",
            output_kind: ArtifactKind::DecisionProposal,
            output_schema: decision_proposal_output_schema(),
 permitted_kinds: BTreeSet::from([
                ArtifactKind::Claim,
                ArtifactKind::Critique,
                ArtifactKind::Lesson,
                ArtifactKind::Experience,
 ArtifactKind::CandidatePolicy,
 ArtifactKind::NormalizedEvidence,
 ArtifactKind::SemanticDetail,
 ArtifactKind::DeliberationNote,
 ]),
            min_context_artifacts: 1,
            budget: TaskBudget {
            max_input_tokens: 48_000,
                max_output_tokens: 2_000,
                max_wall_time_secs: 120,
                max_tool_calls: 2,
            },
            termination: TerminationPolicy::leaf(),
            on_failure: FailureDisposition::FailRun,
        },
        CanonicalContractDefinition {
            purpose: LEARNING_OUTCOME_WORKER_RECIPE_ID,
            responsibility: "Produce a bounded retrospective draft from the governed Paper decision and outcome evidence chain.",
            output_kind: ArtifactKind::RetrospectiveDraft,
            output_schema: retrospective_draft_output_schema(),
            permitted_kinds: BTreeSet::from([
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
            budget: TaskBudget {
                max_input_tokens: 12_000,
                max_output_tokens: 2_500,
                max_wall_time_secs: 180,
                max_tool_calls: 2,
            },
            termination: TerminationPolicy::leaf(),
            on_failure: FailureDisposition::FailTask,
        },
    ]
    .into_iter()
    .map(|definition| canonical_active_contract(store, definition))
    .collect()
}

fn canonical_active_contract(
    store: &V2Store,
    definition: CanonicalContractDefinition,
) -> ResearchResult<AgentContract> {
    let base_prompt = two_phase_role_prompt(definition.purpose)?;
    let role_prompt = match definition.purpose {
        RESEARCH_SYNTHESIZER_RECIPE_ID => format!(
            "{}\n\nAlways return exactly 12 forecasts: one for each executable asset (TQQQ, QQQ, SOXX, SOXL) at each horizon (t1, t3, t5), even when the proposal is blocked; for blocked proposals use neutral zero forecasts and explain the blocker in hard_blockers and summary. In deliberation.basis_artifact_ids and result references, use only artifact IDs that appear as top-level selections in the current ContextManifest; do not copy nested evidence IDs unless they are also selected. Preserve each selected artifact's exact kind: use claim only for claim refs, critique only for critique refs, and normalized_evidence or semantic_detail only when that exact kind is selected. ContextManifest deliberation_note selections may appear in basis_artifact_ids but must not be relabeled as result claims, critiques, or evidence.",
            base_prompt
        ),
        RESEARCH_ANALYST_RECIPE_ID => format!(
            "{}\n\nKeep evidence_gaps to at most 2 items; combine overlapping limitations into concise, evidence-grounded gaps. Preserve the exact artifact kind shown in ContextManifest selections; do not relabel normalized_evidence as semantic_detail or vice versa. For every grounds.evidence reference, copy the exact 64-character artifact_id and exact kind from a top-level context item. Never use the ContextManifest ID, a resource name, or an alias as an evidence artifact_id. Include at least one ground when readable evidence is present. Supplemental needs max_results must be 1-32. For Alpaca bars shorthand, resource must be \"bars\", assets must be explicit, and window_start must be a concrete RFC3339 timestamp; Rust expands it into one canonical per-asset bars need.",
            base_prompt
        ),
        _ => base_prompt,
    };
    let role_prompt = format!(
        "{role_prompt}\n\nUse at most 3 alternatives and at most 3 uncertainties. Use at most 8 evidence-relevant IDs in deliberation.basis_artifact_ids. Provide one alternative_match_ppm value for each alternative. Provide one uncertainty_weight_ppm value for each uncertainty; those weights must sum exactly to 1000000 - confidence_ppm. Use empty score arrays when the corresponding text array is empty. These scores are model-assessed metadata, not observed market facts."
    );
    let role_prompt = match definition.purpose {
        RESEARCH_ANALYST_RECIPE_ID => format!(
            "{role_prompt}\n\nMark direction-blocking gaps with impact=blocks_directional_forecast. Every ground must declare role and assets. Use one directional ground per asset and never claim assets absent from the evidence payload."
        ),
        RESEARCH_SYNTHESIZER_RECIPE_ID => role_prompt.replace(
            "blocked proposals use neutral zero forecasts explain blocker in hard_blockers summary.",
            "blocking evidence gaps or incomplete asset/horizon coverage require MissingEvidence and neutral zero forecasts.",
        ),
        _ => role_prompt,
    };
    let role_prompt = format!(
        "{role_prompt}\n\nEvery evidence ground must declare role, assets, and domain. Blocking gaps may request at most 8 supplemental_needs; request only governed, asset-bound resources whose window ends no later than the current Paper session. Sentiment is not supported by this contract, and the current ETF Paper universe does not require SEC filings.",
    );
    let role_prompt = if definition.purpose == RESEARCH_ANALYST_RECIPE_ID {
        format!(
            "{role_prompt}\n\nFor directional grounds, use normalized evidence with one payload-scoped asset and set domain to bars=price_market_structure, series=macro, or news=news_event. For descriptive paper account, positions, open orders, fills, quotes, or clock evidence, set role=descriptive and domain=null; do not invent a shard."
        )
    } else {
        role_prompt
    };
    let role_prompt = if definition.purpose == RESEARCH_ANALYST_RECIPE_ID {
        format!(
                "{role_prompt}\n\nFor descriptive grounds over paper.* evidence, always set assets to an empty array."
            )
    } else {
        role_prompt
    };
    let role_prompt = if definition.purpose == RESEARCH_SYNTHESIZER_RECIPE_ID {
        format!(
                "{role_prompt}\n\nCopy every selected Claim reference unchanged into result.claims; if no Claim is selected, leave claims empty. Never put a normalized_evidence ID in claims or critiques."
            )
    } else {
        role_prompt
    };
    let prompt = PromptBundle {
        version: ACTIVE_PROMPT_BUNDLE_VERSION,
        governance: store.put_bytes(SHARED_GOVERNANCE_PROMPT.as_bytes(), "text/plain")?,
        role: store.put_bytes(role_prompt.as_bytes(), "text/plain")?,
    };
    let schema = store.put_json(&deliberation_output_schema(&definition.output_schema))?;
    let mut contract = AgentContract::new(
        ContractId(format!("akzio.v2.{}", definition.purpose)),
        ACTIVE_CONTRACT_VERSION,
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
            max_tokens: definition.budget.max_input_tokens,
            allow_raw_reread: false,
        },
        evidence_read_grants(),
        evidence_read_tool_specs(store)?,
        OutputContract {
            artifact_kind: definition.output_kind,
            schema,
        },
        definition.budget,
        active_retry_policy(),
        definition.termination,
        definition.on_failure,
    )?;
    contract.deliberation_policy = DeliberationPolicy::Required;
    contract.contract_hash = contract.expected_hash()?;
    contract.validate()?;
    Ok(contract)
}

fn two_phase_role_prompt(purpose: &str) -> ResearchResult<String> {
    let prompt = match purpose {
        PLANNER_RECIPE_ID => "You are Akzio's bounded research planner. In Draft, explain the bounded workflow, required evidence, dependencies, and uncertainty. In Submit, produce WorkflowProposalDraft through submit_result. You may name only research.analyst and research.synthesizer recipes and express evidence needs inline. Numeric bounds are strict: priority 0-100, max_age_secs 1-604800, max_results 1-32, at most 4 assets and 7 tasks. window_start and window_end must be null or RFC3339 timestamps. Do not construct ArtifactRef values, widen capabilities, submit a decision, or submit an order.",
        RESEARCH_ANALYST_RECIPE_ID => "You are Akzio's research analyst. In Draft, write an evidence-grounded memo covering the claim, support, counter-evidence, gaps, and uncertainty. In Submit, produce Claim through submit_result. Use only granted context artifacts. Do not call external systems, widen sources, change topology, submit decisions, or submit orders.",
        RESEARCH_CRITIC_RECIPE_ID => "You are Akzio's research critic. In Draft, write a concise critique memo covering counter-evidence, unsupported assumptions, gaps, and uncertainty. In Submit, produce Critique through submit_result. Challenge supplied claims using granted context. Do not invent evidence, widen sources or tools, alter workflow, produce a decision, or submit an order.",
        RESEARCH_SYNTHESIZER_RECIPE_ID => "You are Akzio's research synthesizer. In Draft, write a decision memo reconciling claims, critiques, blockers, alternatives, and uncertainty. In Submit, produce DecisionProposal through submit_result. Use only artifacts selected by ContextManifest. Do not change evidence, bypass DecisionGate, submit an order, or expand any capability.",
        LEARNING_OUTCOME_WORKER_RECIPE_ID => "You are Akzio's governed outcome reviewer. In Draft, write a bounded retrospective memo from granted decision, execution, outcomes, market evidence, deliberation notes, and prior retrospectives. In Submit, produce RetrospectiveDraft through submit_result. Never emit authoritative returns, calibration, slippage, risk recall, or policy decisions.",
        _ => return Err(ResearchError::UnexpectedActiveContractPurpose(purpose.to_owned())),
    };
    let prompt = if purpose == PLANNER_RECIPE_ID {
        prompt.replace(
            "priority 0-100",
            "research.analyst priority 1-90, research.synthesizer priority 1-100",
        )
    } else {
        prompt.to_owned()
    };
    Ok(prompt)
}

fn governed_context_sources() -> BTreeSet<String> {
    GOVERNED_EVIDENCE_SOURCE_FAMILIES
        .into_iter()
        .chain(["akzio.agent", "akzio.operator"])
        .map(str::to_owned)
        .collect()
}

fn evidence_read_grants() -> Vec<ToolGrant> {
    vec![ToolGrant {
        kind: ToolKind::ReadEvidence,
        allowed_sources: GOVERNED_EVIDENCE_SOURCE_FAMILIES
            .into_iter()
            .map(str::to_owned)
            .collect(),
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

fn research_output_source_refs(
    store: &V2Store,
    kind: ArtifactKind,
    output: &Value,
    manifest: &ContextManifest,
) -> ResearchResult<Vec<ArtifactRef>> {
    let refs = match kind {
        ArtifactKind::Claim => {
            let claim: ResearchClaim = serde_json::from_value(output.clone()).map_err(|error| {
                ResearchError::InvalidOutput(format!("invalid Claim payload: {error}"))
            })?;
            claim
                .validate()
                .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;
            validate_claim_ground_scopes(store, &claim, manifest)?;
            claim.source_refs()
        }
        ArtifactKind::Critique => {
            let critique: ResearchCritique =
                serde_json::from_value(output.clone()).map_err(|error| {
                    ResearchError::InvalidOutput(format!("invalid Critique payload: {error}"))
                })?;
            critique
                .validate()
                .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;
            critique.source_refs()
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
            proposal
                .validate()
                .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;

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
                    "DecisionProposal claims do not close over ContextManifest".to_owned(),
                ));
            }
            if !selected_critiques.is_subset(&submitted_critiques) {
                return Err(ResearchError::InvalidOutput(
                    "DecisionProposal critiques do not close over ContextManifest".to_owned(),
                ));
            }

            let declared_evidence = proposal.evidence.iter().cloned().collect::<BTreeSet<_>>();
            let mut refs = proposal
                .claims
                .iter()
                .chain(proposal.critiques.iter())
                .chain(proposal.evidence.iter())
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
                        validate_claim_ground_scopes(store, &claim, manifest)?;
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
                if source_refs
                    .iter()
                    .any(|source| !declared_evidence.contains(source))
                {
                    return Err(ResearchError::InvalidOutput(
                        "DecisionProposal evidence does not close over claim/critique grounds"
                            .to_owned(),
                    ));
                }
                refs.extend(source_refs);
            }
            validate_decision_evidence_sufficiency(&proposal, &claims)
                .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;
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
    if refs.iter().any(|reference| !selected.contains(reference)) {
        return Err(ResearchError::InvalidOutput(
            "research artifact cited an artifact outside ContextManifest".to_owned(),
        ));
    }
    Ok(refs)
}

fn validate_claim_ground_scopes(
    store: &V2Store,
    claim: &ResearchClaim,
    manifest: &ContextManifest,
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
            return Err(ResearchError::InvalidOutput(
                format!(
                    "ground domain {:?} does not match evidence resource {} (expected {:?}); choose the selected artifact whose resource matches the declared domain",
                    ground.domain,
                    payload
                        .get("resource")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown"),
                    domain
                ),
            ));
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
                return Err(ResearchError::InvalidOutput(
                    "directional ground must bind exactly one asset to a scoped, citation-complete normalized evidence artifact"
                        .to_owned(),
                ));
            }
        } else if let Some(scope) = scope {
            if !ground.assets.is_subset(&scope) {
                return Err(ResearchError::InvalidOutput(
                    "descriptive ground assets exceed evidence payload scope".to_owned(),
                ));
            }
        } else if !ground.assets.is_empty() {
            return Err(ResearchError::InvalidOutput(
                "unknown evidence scope cannot declare assets".to_owned(),
            ));
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
