#[test]
fn planner_draft_schema_is_closed_and_governed() {
    let schema = planner_draft_output_schema();
    let valid = serde_json::json!({
        "schema_version": V2_DOMAIN_SCHEMA_VERSION,
        "topology_id": "active",
        "tasks": {
            "analyst": {
                "recipe_id": "research.analyst",
                "objective": "analyse governed TQQQ evidence",
                "depends_on": [],
                "priority": 50,
                "evidence_needs": [{
                    "schema_version": V2_DOMAIN_SCHEMA_VERSION,
                    "source_family": "alpaca",
                    "resource": "bars:TQQQ:1d",
                    "max_age_secs": 86400
                }]
            }
        }
    });
    validate_schema_value(&valid, &schema, "$").unwrap();

    let mut invalid_version = valid.clone();
    invalid_version["schema_version"] = serde_json::json!(V2_DOMAIN_SCHEMA_VERSION + 1);
    assert!(validate_schema_value(&invalid_version, &schema, "$").is_err());

    let mut invalid_recipe = valid.clone();
    invalid_recipe["tasks"]["analyst"]["recipe_id"] = serde_json::json!("gate.paper");
    assert!(validate_schema_value(&invalid_recipe, &schema, "$").is_err());

    let mut invalid_source = valid.clone();
    invalid_source["tasks"]["analyst"]["evidence_needs"][0]["source_family"] =
        serde_json::json!("uninstalled-web");
    assert!(validate_schema_value(&invalid_source, &schema, "$").is_err());

    let mut invalid_priority = valid.clone();
    invalid_priority["tasks"]["analyst"]["priority"] = serde_json::json!(101);
    assert!(validate_schema_value(&invalid_priority, &schema, "$").is_err());

    let mut artifact_ref = valid.clone();
    artifact_ref["tasks"]["analyst"]["artifact_id"] = serde_json::json!("sha256:forged");
    assert!(validate_schema_value(&artifact_ref, &schema, "$").is_err());

    let mut tool_or_role = valid;
    tool_or_role["tasks"]["analyst"]["tool"] = serde_json::json!("fetch_web");
    assert!(validate_schema_value(&tool_or_role, &schema, "$").is_err());
}

#[test]
fn active_catalogue_installs_canonical_contracts_and_bounded_recipes() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let active = ActiveResearchCatalogue::install(&store, Utc::now()).unwrap();
    let expected = [
        (PLANNER_RECIPE_ID, ArtifactKind::WorkflowProposalDraft),
        ("research.analyst", ArtifactKind::Claim),
        ("research.critic", ArtifactKind::Critique),
        ("research.synthesizer", ArtifactKind::DecisionProposal),
        (
            LEARNING_OUTCOME_WORKER_RECIPE_ID,
            ArtifactKind::RetrospectiveDraft,
        ),
    ];

    assert_eq!(active.contracts.contracts().count(), expected.len());
    for (purpose, output_kind) in expected {
        let installed = active
            .contracts
            .contracts()
            .find(|installed| installed.contract.purpose.as_str() == purpose)
            .unwrap();
        assert_eq!(installed.contract.output.artifact_kind, output_kind);
        assert_eq!(
            installed.contract.context.min_artifacts,
            if purpose == PLANNER_RECIPE_ID { 0 } else { 1 }
        );
        assert_eq!(
            installed.contract.termination.require_evidence,
            purpose != PLANNER_RECIPE_ID
        );
        let recipe = active
            .recipes
            .recipe(&TaskRecipeId::new(purpose).unwrap())
            .unwrap();
        assert_eq!(
            recipe.contract_hash.as_ref(),
            Some(&installed.contract.contract_hash)
        );
        assert_eq!(recipe.budget, installed.contract.budget);
        assert_eq!(recipe.retry, installed.contract.retry);
        assert_eq!(recipe.on_failure, installed.contract.on_failure);
        assert_eq!(
            recipe.max_children,
            installed.contract.termination.max_child_tasks
        );
        assert_eq!(recipe.max_depth, installed.contract.termination.max_depth);
        assert_eq!(
            recipe.allowed_evidence_sources,
            recipe_evidence_sources(&installed.contract)
        );
    }

    for (recipe_id, task_class) in [
        (EVIDENCE_GATE_RECIPE_ID, RuntimeTaskClass::Evidence),
        (DECISION_GATE_RECIPE_ID, RuntimeTaskClass::DecisionGate),
        (EXECUTION_GATE_RECIPE_ID, RuntimeTaskClass::ExecutionGate),
        (PAPER_COMMIT_RECIPE_ID, RuntimeTaskClass::PaperCommit),
        (RECONCILE_RECIPE_ID, RuntimeTaskClass::Reconcile),
        (EVALUATE_RECIPE_ID, RuntimeTaskClass::Evaluate),
    ] {
        let recipe = active
            .recipes
            .recipe(&TaskRecipeId::new(recipe_id).unwrap())
            .unwrap();
        assert_eq!(recipe.task_class, task_class);
        assert_eq!(recipe.contract_hash, None);
        assert!(recipe.allowed_evidence_sources.is_empty());
        if task_class == RuntimeTaskClass::Evidence {
            assert_eq!(recipe.retry.max_attempts, 5);
            assert_eq!(recipe.retry.initial_backoff_ms, 1_000);
            assert!(recipe.retry.retry_transport);
            assert!(recipe.retry.retry_rate_limited);
            assert!(!recipe.retry.retry_invalid_output);
        } else if task_class == RuntimeTaskClass::ExecutionGate {
            assert_eq!(recipe.retry.max_attempts, 2);
            assert_eq!(recipe.retry.initial_backoff_ms, 1_000);
            assert!(recipe.retry.retry_transport);
            assert!(recipe.retry.retry_rate_limited);
            assert!(!recipe.retry.retry_invalid_output);
            assert_eq!(recipe.budget.max_wall_time_secs, 90);
        } else {
            assert_eq!(recipe.retry, RetryPolicy::none());
            assert_eq!(recipe.budget.max_wall_time_secs, 30);
        }
    }
    let worker_recipe = active
        .recipes
        .recipe(&TaskRecipeId::new(LEARNING_OUTCOME_WORKER_RECIPE_ID).unwrap())
        .unwrap();
    assert_eq!(worker_recipe.task_class, RuntimeTaskClass::Evaluate);
    assert!(worker_recipe.contract_hash.is_some());
    store.verify_integrity().unwrap();
}

#[test]
fn active_catalogue_restores_store_owned_heads_after_restart() {
    let root = tempdir().unwrap();
    let now = Utc::now();
    let store = V2Store::open(root.path()).unwrap();
    let first = ActiveResearchCatalogue::install(&store, now).unwrap();
    let expected = first
        .contracts
        .contracts()
        .map(|installed| {
            (
                installed.contract.purpose.as_str().to_owned(),
                installed.contract.contract_hash.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    drop(first);
    drop(store);

    let reopened = V2Store::open(root.path()).unwrap();
    let restored = ActiveResearchCatalogue::install(&reopened, now + Duration::seconds(1)).unwrap();
    let actual = restored
        .contracts
        .contracts()
        .map(|installed| {
            (
                installed.contract.purpose.as_str().to_owned(),
                installed.contract.contract_hash.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();

    assert_eq!(actual, expected);
    reopened.verify_integrity().unwrap();
}

#[test]
fn active_catalogue_upgrades_an_older_bounded_canonical_version() {
    let root = tempdir().unwrap();
    let now = Utc::now();
    let store = V2Store::open(root.path()).unwrap();
    let mut older = canonical_active_contracts(&store).unwrap();
    for contract in &mut older {
        contract.version = ACTIVE_CONTRACT_VERSION - 1;
        contract.prompt.version = ACTIVE_PROMPT_BUNDLE_VERSION - 1;
        contract.contract_hash = contract.expected_hash().unwrap();
        contract.validate().unwrap();
        store.install_active_contract(contract, now).unwrap();
    }

    let upgraded = ActiveResearchCatalogue::install(&store, now + Duration::seconds(1)).unwrap();
    for installed in upgraded.contracts.contracts() {
        assert_eq!(installed.contract.version, ACTIVE_CONTRACT_VERSION);
        assert_eq!(
            installed.contract.prompt.version,
            ACTIVE_PROMPT_BUNDLE_VERSION
        );
        assert_eq!(
            store
                .active_contract(&installed.contract.purpose)
                .unwrap()
                .unwrap()
                .contract
                .contract_hash,
            installed.contract.contract_hash
        );
    }
    store.verify_integrity().unwrap();
}

#[test]
fn candidate_install_is_durable_bounded_and_non_executable() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let active = ActiveResearchCatalogue::install(&store, now).unwrap();
    let baseline = active
        .contracts
        .contracts()
        .find(|installed| installed.contract.purpose.as_str() == PLANNER_RECIPE_ID)
        .unwrap()
        .contract
        .clone();
    let mut candidate = baseline.clone();
    candidate.version += 1;
    candidate.contract_hash = candidate.expected_hash().unwrap();
    candidate.validate().unwrap();

    let installed = active
        .install_candidate(&store, &baseline.contract_hash, &candidate, now)
        .unwrap();
    assert_eq!(installed.contract, candidate);
    assert_eq!(
        store
            .active_contract(&baseline.purpose)
            .unwrap()
            .unwrap()
            .contract
            .contract_hash,
        baseline.contract_hash
    );
    assert_eq!(
        store
            .contract_installation(&candidate.contract_hash)
            .unwrap()
            .unwrap()
            .baseline_contract_hash,
        Some(baseline.contract_hash.clone())
    );
    assert!(active.contracts.get(&candidate.contract_hash).is_err());

    let mut expanded = candidate;
    expanded.version += 1;
    expanded
        .context
        .permitted_source_families
        .insert("unapproved_source".to_owned());
    expanded.candidate_capability_ceiling = akzio_domain::CandidateCapabilityCeiling {
        context: expanded.context.clone(),
        tool_grants: expanded.tool_grants.clone(),
    };
    expanded.contract_hash = expanded.expected_hash().unwrap();
    expanded.validate().unwrap();
    assert!(matches!(
        active.install_candidate(&store, &baseline.contract_hash, &expanded, now),
        Err(ResearchError::CandidateCapabilityExpansion { .. })
    ));
    store.verify_integrity().unwrap();
}

#[test]
fn decision_proposal_schema_matches_typed_decision_draft() {
    let schema = decision_proposal_output_schema();
    let forecasts = akzio_domain::Asset::EXECUTABLE
        .into_iter()
        .flat_map(|asset| {
            ["t1", "t3", "t5"].into_iter().map(move |horizon| {
                json!({
                    "asset": asset.symbol(),
                    "horizon": horizon,
                    "positive_return_probability_ppm": 500000,
                    "expected_return_ppm": 0,
                })
            })
        })
        .collect::<Vec<_>>();
    let valid = json!({
        "summary": "blocked fixture decision",
        "confidence_ppm": 500000,
        "forecasts": forecasts,
        "claims": [],
        "critiques": [],
        "evidence": [],
        "material_conflicts": [],
        "hard_blockers": ["missing_evidence"],
        "soft_warnings": []
    });

    validate_schema_value(&valid, &schema, "$").unwrap();
    serde_json::from_value::<akzio_domain::DecisionDraft>(valid)
        .unwrap()
        .validate()
        .unwrap();
    for invalid in [
        json!({
            "summary": "invalid",
            "confidence_ppm": 500000,
            "blockers": ["anything"],
            "asset_views": {}
        }),
        json!({
            "summary": "extra field",
            "targets": {
                "weights": { "TQQQ": 0, "QQQ": 0, "SOXX": 0, "SOXL": 0 }
            },
            "confidence_ppm": 500000,
            "forecasts": [],
            "claims": [],
            "critiques": [],
            "evidence": [],
            "material_conflicts": [],
            "hard_blockers": ["missing_evidence"],
            "soft_warnings": [],
            "authority": "paper"
        }),
    ] {
        assert!(validate_schema_value(&invalid, &schema, "$").is_err());
    }
}

#[test]
fn schema_validator_accepts_nullable_union_types() {
    let schema = json!({"type": ["string", "null"]});

    validate_schema_value(&Value::Null, &schema, "$").unwrap();
    validate_schema_value(&json!("fixture"), &schema, "$").unwrap();
    assert!(validate_schema_value(&json!(42), &schema, "$").is_err());
}

#[test]
fn artifact_reference_schema_enforces_sha256_pattern() {
    let schema = artifact_ref_schema(&["claim"]);
    let valid = json!({
        "artifact_id": "a".repeat(64),
        "kind": "claim",
    });
    validate_schema_value(&valid, &schema, "$").unwrap();

    let invalid = json!({
        "artifact_id": "not-a-content-hash",
        "kind": "claim",
    });
    assert!(validate_schema_value(&invalid, &schema, "$").is_err());
}

#[test]
fn active_catalogue_rejects_planner_that_does_not_output_a_draft() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let mut contracts = canonical_active_contracts(&store).unwrap();
    let planner = contracts
        .iter_mut()
        .find(|contract| contract.purpose.as_str() == PLANNER_RECIPE_ID)
        .unwrap();
    planner.output.artifact_kind = ArtifactKind::WorkflowProposal;
    planner.contract_hash = planner.expected_hash().unwrap();
    planner.validate().unwrap();
    let catalogue = ContractCatalogue::install(&store, contracts, Utc::now()).unwrap();

    assert!(matches!(
        catalogue.active_recipe_catalogue(&store),
        Err(ResearchError::ActiveContractOutputMismatch {
            purpose,
            expected: ArtifactKind::WorkflowProposalDraft,
            actual: ArtifactKind::WorkflowProposal,
        }) if purpose == PLANNER_RECIPE_ID
    ));
}

#[test]
fn active_catalogue_rejects_candidate_or_unknown_contract_recipe() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let mut contracts = canonical_active_contracts(&store).unwrap();
    let mut candidate = contracts
        .iter()
        .find(|contract| contract.purpose.as_str() == "research.analyst")
        .unwrap()
        .clone();
    candidate.contract_id = ContractId("akzio.v2.research.candidate".to_owned());
    candidate.version = 2;
    candidate.purpose = ContractPurpose::new("research.candidate").unwrap();
    candidate.responsibility = "candidate data only".to_owned();
    candidate.contract_hash = candidate.expected_hash().unwrap();
    candidate.validate().unwrap();
    contracts.push(candidate);
    let catalogue = ContractCatalogue::install(&store, contracts, Utc::now()).unwrap();

    assert!(matches!(
        catalogue.active_recipe_catalogue(&store),
        Err(ResearchError::UnexpectedActiveContractPurpose(purpose))
            if purpose == "research.candidate"
    ));
}

#[test]
fn contract_catalogue_rejects_duplicate_hash_and_identity_version() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let contract = contract(&store);
    let catalogue = ContractCatalogue::install(&store, [contract.clone()], Utc::now()).unwrap();
    assert_eq!(
        catalogue.contract_hash_for(&contract.contract_id, contract.version),
        Some(&contract.contract_hash)
    );

    assert!(matches!(
        ContractCatalogue::install(&store, [contract.clone(), contract.clone()], Utc::now(),),
        Err(ResearchError::DuplicateContract(_))
    ));

    let mut changed = contract.clone();
    changed.responsibility = "different responsibility".to_owned();
    changed.contract_hash = changed.expected_hash().unwrap();
    changed.validate().unwrap();
    assert!(matches!(
        ContractCatalogue::install(&store, [contract, changed], Utc::now()),
        Err(ResearchError::DuplicateContractVersion { .. })
    ));
}

#[test]
fn contract_catalogue_rejects_candidate_capability_expansion() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let active = contract(&store);
    let catalogue = ContractCatalogue::install(&store, [active.clone()], Utc::now()).unwrap();

    let mut candidate = active.clone();
    candidate
        .context
        .permitted_source_families
        .insert("news".to_owned());
    candidate.tool_grants[0]
        .allowed_sources
        .push("news".to_owned());
    candidate.candidate_capability_ceiling = akzio_domain::CandidateCapabilityCeiling {
        context: candidate.context.clone(),
        tool_grants: candidate.tool_grants.clone(),
    };
    candidate.contract_hash = candidate.expected_hash().unwrap();
    candidate.validate().unwrap();

    assert!(matches!(
        catalogue.validate_candidate(&active.contract_hash, &candidate),
        Err(ResearchError::CandidateCapabilityExpansion { .. })
    ));

    let mut narrowed = active.clone();
    narrowed.budget.max_input_tokens /= 2;
    narrowed.contract_hash = narrowed.expected_hash().unwrap();
    narrowed.validate().unwrap();
    catalogue
        .validate_candidate(&active.contract_hash, &narrowed)
        .unwrap();
}
