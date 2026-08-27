pub(super) fn prepare_debug_draft(
    draft: &mut WorkflowProposalDraft,
    has_synthesizer_recipe: bool,
    fixture_mode: bool,
) -> RuntimeResult<()> {
    if draft.tasks.is_empty() {
        draft.tasks.insert(
            "debug_analyst".to_owned(),
            akzio_domain::WorkflowProposalDraftTask {
                recipe_id: TaskRecipeId::new(ANALYST_RECIPE_ID)?,
                objective: "Inspect the governed TQQQ debug fixture evidence.".to_owned(),
                depends_on: Vec::new(),
                priority: 80,
                evidence_needs: Vec::new(),
                research_intents: Vec::new(),
            },
        );
    }
    let first_analyst = draft
        .tasks
        .iter()
        .find(|(_, task)| task.recipe_id.as_str() == ANALYST_RECIPE_ID)
        .map(|(alias, _)| alias.clone());
    if let Some(first_analyst) = first_analyst.as_ref() {
        draft.tasks.retain(|alias, task| {
            task.recipe_id.as_str() != ANALYST_RECIPE_ID || alias == first_analyst
        });
    }
    let aliases = draft.tasks.keys().cloned().collect::<BTreeSet<_>>();
    for task in draft.tasks.values_mut() {
        task.depends_on
            .retain(|dependency| aliases.contains(dependency));
        if task.recipe_id.as_str() == ANALYST_RECIPE_ID && fixture_mode {
            task.evidence_needs.retain(|need| {
                need.source_family == DEBUG_FIXTURE_SOURCE
                    && need.resource == DEBUG_FIXTURE_RESOURCE
                    && need.max_age_secs > 0
            });
            task.research_intents.clear();
        } else if task.recipe_id.as_str() == SYNTHESIZER_RECIPE_ID {
            task.evidence_needs.clear();
            task.research_intents.clear();
        }
    }
    let analyst_aliases = draft
        .tasks
        .iter()
        .filter(|(_, task)| task.recipe_id.as_str() == ANALYST_RECIPE_ID)
        .map(|(alias, _)| alias.clone())
        .collect::<Vec<_>>();
    if analyst_aliases.is_empty() {
        return Ok(());
    }

    let default_needs = if fixture_mode {
        vec![EvidenceNeed {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            source_family: DEBUG_FIXTURE_SOURCE.to_owned(),
            resource: DEBUG_FIXTURE_RESOURCE.to_owned(),
            max_age_secs: DEBUG_FIXTURE_MAX_AGE_SECS,
        }]
    } else {
        let start = (Utc::now().date_naive() - Duration::days(28)).format("%Y-%m-%d");
        [
            "paper.account",
            "paper.positions",
            "paper.open_orders",
            "paper.clock",
            "paper.quotes",
        ]
        .into_iter()
        .map(|resource| EvidenceNeed {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            source_family: DEBUG_FIXTURE_SOURCE.to_owned(),
            resource: resource.to_owned(),
            max_age_secs: DEBUG_FIXTURE_MAX_AGE_SECS,
        })
        .chain(Asset::EXECUTABLE.into_iter().map(|asset| EvidenceNeed {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            source_family: DEBUG_FIXTURE_SOURCE.to_owned(),
            resource: format!("bars:{}:1d:{start}:32", asset.symbol()),
            max_age_secs: DEBUG_FIXTURE_MAX_AGE_SECS,
        }))
        .collect()
    };
    let mut injected_need = false;
    for task in draft
        .tasks
        .values_mut()
        .filter(|task| task.recipe_id.as_str() == ANALYST_RECIPE_ID)
    {
        let has_alpaca_need = task
            .evidence_needs
            .iter()
            .any(|need| need.source_family == DEBUG_FIXTURE_SOURCE)
            || task
                .research_intents
                .iter()
                .any(|intent| intent.source_family == DEBUG_FIXTURE_SOURCE);
        if !has_alpaca_need {
            task.evidence_needs.extend(default_needs.iter().cloned());
            injected_need = true;
        }
    }

    if !injected_need
        || !has_synthesizer_recipe
        || draft
            .tasks
            .values()
            .any(|task| task.recipe_id.as_str() == SYNTHESIZER_RECIPE_ID)
    {
        return Ok(());
    }

    let synthesizer_recipe = TaskRecipeId::new(SYNTHESIZER_RECIPE_ID)?;
    let alias = (0..)
        .map(|suffix| {
            if suffix == 0 {
                "debug_synthesizer".to_owned()
            } else {
                format!("debug_synthesizer_{suffix}")
            }
        })
        .find(|candidate| !draft.tasks.contains_key(candidate))
        .expect("unbounded alias search must find a free debug synthesizer alias");
    draft.tasks.insert(
        alias,
        akzio_domain::WorkflowProposalDraftTask {
            recipe_id: synthesizer_recipe,
            objective: "Synthesize the debug analyst claim into a blocked decision proposal."
                .to_owned(),
            depends_on: analyst_aliases,
            priority: 100,
            evidence_needs: Vec::new(),
            research_intents: Vec::new(),
        },
    );
    Ok(())
}

/// Returns whether the bounded Critic task should consume the supplied claims.
/// Rust owns this decision so a planner or model cannot add debate rounds.
pub fn should_run_structured_critique(claims: &[ResearchClaim]) -> bool {
    claims.iter().any(|claim| {
        claim.materiality_ppm >= STRUCTURED_CRITIQUE_MATERIALITY_PPM
            && (!claim.evidence_gaps.is_empty()
                || claim.confidence_ppm <= STRUCTURED_CRITIQUE_CONFIDENCE_PPM)
    }) || claims.iter().enumerate().any(|(index, claim)| {
        claims[index + 1..].iter().any(|other| {
            claim.topic == other.topic
                && claim.horizon == other.horizon
                && matches!(
                    (claim.stance, other.stance),
                    (ClaimStance::Bullish, ClaimStance::Bearish)
                        | (ClaimStance::Bearish, ClaimStance::Bullish)
                )
        })
    })
}
