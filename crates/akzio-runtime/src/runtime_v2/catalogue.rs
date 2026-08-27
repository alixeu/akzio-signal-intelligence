use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalRecipeSet {
    pub evidence_gate: TaskRecipeId,
    pub decision_gate: TaskRecipeId,
    pub execution_gate: TaskRecipeId,
    pub paper_commit: TaskRecipeId,
    pub reconcile: TaskRecipeId,
    pub evaluate: TaskRecipeId,
}

#[derive(Debug, Clone)]
pub struct RecipeCatalogue {
    pub(super) recipes: BTreeMap<TaskRecipeId, TaskRecipe>,
    pub(super) planner: TaskRecipeId,
    pub(super) terminals: TerminalRecipeSet,
    pub(super) max_nodes: usize,
}

impl RecipeCatalogue {
    pub fn new(
        recipes: impl IntoIterator<Item = TaskRecipe>,
        planner: TaskRecipeId,
        terminals: TerminalRecipeSet,
        max_nodes: usize,
    ) -> RuntimeResult<Self> {
        let recipes = recipes
            .into_iter()
            .map(|recipe| {
                recipe.validate()?;
                Ok((recipe.recipe_id.clone(), recipe))
            })
            .collect::<Result<BTreeMap<_, _>, DomainError>>()?;
        let catalogue = Self {
            recipes,
            planner,
            terminals,
            max_nodes,
        };
        if catalogue.max_nodes == 0 {
            return Err(RuntimeError::WorkflowNodeLimit);
        }
        catalogue.assert_planner(&catalogue.planner)?;
        catalogue.assert_terminal(
            &catalogue.terminals.evidence_gate,
            RuntimeTaskClass::Evidence,
        )?;
        catalogue.assert_terminal(
            &catalogue.terminals.decision_gate,
            RuntimeTaskClass::DecisionGate,
        )?;
        catalogue.assert_terminal(
            &catalogue.terminals.execution_gate,
            RuntimeTaskClass::ExecutionGate,
        )?;
        catalogue.assert_terminal(
            &catalogue.terminals.paper_commit,
            RuntimeTaskClass::PaperCommit,
        )?;
        catalogue.assert_terminal(&catalogue.terminals.reconcile, RuntimeTaskClass::Reconcile)?;
        catalogue.assert_terminal(&catalogue.terminals.evaluate, RuntimeTaskClass::Evaluate)?;
        Ok(catalogue)
    }

    pub(super) fn assert_planner(&self, recipe_id: &TaskRecipeId) -> RuntimeResult<()> {
        let recipe = self.recipe(recipe_id)?;
        if recipe.task_class != RuntimeTaskClass::Agent || recipe.contract_hash.is_none() {
            return Err(RuntimeError::PlannerPermitRequired);
        }
        Ok(())
    }

    pub fn recipe(&self, recipe_id: &TaskRecipeId) -> RuntimeResult<&TaskRecipe> {
        self.recipes
            .get(recipe_id)
            .ok_or_else(|| RuntimeError::MissingRecipe(recipe_id.clone()))
    }

    pub(super) fn assert_terminal(
        &self,
        recipe_id: &TaskRecipeId,
        expected: RuntimeTaskClass,
    ) -> RuntimeResult<()> {
        let recipe = self.recipe(recipe_id)?;
        if recipe.task_class != expected {
            return Err(RuntimeError::InvalidTerminalRecipe {
                recipe: recipe_id.clone(),
                actual: recipe.task_class,
                expected,
            });
        }
        Ok(())
    }

    pub(super) fn is_terminal(&self, recipe_id: &TaskRecipeId) -> bool {
        [
            &self.terminals.decision_gate,
            &self.terminals.execution_gate,
            &self.terminals.paper_commit,
            &self.terminals.reconcile,
            &self.terminals.evaluate,
        ]
        .into_iter()
        .any(|terminal| terminal == recipe_id)
    }

    pub(super) fn is_rust_gate(&self, recipe_id: &TaskRecipeId) -> bool {
        recipe_id == &self.terminals.evidence_gate || self.is_terminal(recipe_id)
    }
}

pub const EVIDENCE_GATE_RECIPE_ID: &str = "gate.evidence";
pub const DECISION_GATE_RECIPE_ID: &str = "gate.decision";
pub const EXECUTION_GATE_RECIPE_ID: &str = "gate.execution";
pub const PAPER_COMMIT_RECIPE_ID: &str = "gate.paper";
pub const RECONCILE_RECIPE_ID: &str = "gate.reconcile";
pub const EVALUATE_RECIPE_ID: &str = "gate.evaluate";

#[derive(Debug, Clone)]
pub struct ActiveContractRecipe {
    pub contract: akzio_domain::AgentContract,
    pub artifact: Artifact,
}

const ACTIVE_RECIPE_POLICIES: [(&str, ArtifactKind, u8); 4] = [
    (
        akzio_domain::RESEARCH_PLANNER_RECIPE_ID,
        ArtifactKind::WorkflowProposalDraft,
        100,
    ),
    (
        akzio_domain::RESEARCH_ANALYST_RECIPE_ID,
        ArtifactKind::Claim,
        90,
    ),
    (
        akzio_domain::RESEARCH_CRITIC_RECIPE_ID,
        ArtifactKind::Critique,
        80,
    ),
    (
        akzio_domain::RESEARCH_SYNTHESIZER_RECIPE_ID,
        ArtifactKind::DecisionProposal,
        100,
    ),
];

pub fn active_recipe_catalogue(
    store: &V2Store,
    contracts: impl IntoIterator<Item = ActiveContractRecipe>,
    planner: TaskRecipeId,
    max_nodes: usize,
) -> RuntimeResult<RecipeCatalogue> {
    let mut installed_purposes = BTreeSet::new();
    let mut recipes = Vec::with_capacity(ACTIVE_RECIPE_POLICIES.len() + 6);
    let mut outcome_worker_installed = false;

    for installed in contracts {
        let purpose = installed.contract.purpose.as_str();
        let Some((expected_output, priority_ceiling)) = ACTIVE_RECIPE_POLICIES
            .iter()
            .find(|(candidate, _, _)| *candidate == purpose)
            .map(|(_, output, priority)| (*output, *priority))
        else {
            if purpose != akzio_domain::LEARNING_OUTCOME_WORKER_RECIPE_ID {
                return Err(RuntimeError::UnexpectedActiveContractPurpose(
                    purpose.to_owned(),
                ));
            }
            if !installed_purposes.insert(purpose.to_owned()) {
                return Err(RuntimeError::DuplicateActiveContractPurpose(
                    purpose.to_owned(),
                ));
            }
            if installed.contract.output.artifact_kind != ArtifactKind::RetrospectiveDraft {
                return Err(RuntimeError::ActiveContractOutputMismatch {
                    purpose: purpose.to_owned(),
                    expected: ArtifactKind::RetrospectiveDraft,
                    actual: installed.contract.output.artifact_kind,
                });
            }
            let active = store
                .active_contract(&installed.contract.purpose)?
                .ok_or_else(|| RuntimeError::NonCanonicalActiveContract(purpose.to_owned()))?;
            if active.contract.contract_hash != installed.contract.contract_hash
                || active.artifact != installed.artifact
            {
                return Err(RuntimeError::NonCanonicalActiveContract(purpose.to_owned()));
            }
            outcome_worker_installed = true;
            recipes.push(TaskRecipe {
                recipe_id: TaskRecipeId::new(purpose)?,
                purpose: installed.contract.purpose.clone(),
                contract_hash: Some(installed.contract.contract_hash.clone()),
                task_class: RuntimeTaskClass::Evaluate,
                allowed_evidence_sources: recipe_evidence_sources(&installed.contract),
                max_children: 0,
                max_depth: 0,
                priority_ceiling: 100,
                budget: installed.contract.budget.clone(),
                retry: installed.contract.retry.clone(),
                on_failure: installed.contract.on_failure,
            });
            continue;
        };

        if !installed_purposes.insert(purpose.to_owned()) {
            return Err(RuntimeError::DuplicateActiveContractPurpose(
                purpose.to_owned(),
            ));
        }
        if installed.contract.output.artifact_kind != expected_output {
            return Err(RuntimeError::ActiveContractOutputMismatch {
                purpose: purpose.to_owned(),
                expected: expected_output,
                actual: installed.contract.output.artifact_kind,
            });
        }
        let active = store
            .active_contract(&installed.contract.purpose)?
            .ok_or_else(|| RuntimeError::NonCanonicalActiveContract(purpose.to_owned()))?;
        if active.contract.contract_hash != installed.contract.contract_hash
            || active.artifact != installed.artifact
        {
            return Err(RuntimeError::NonCanonicalActiveContract(purpose.to_owned()));
        }
        recipes.push(TaskRecipe {
            recipe_id: TaskRecipeId::new(purpose)?,
            purpose: installed.contract.purpose.clone(),
            contract_hash: Some(installed.contract.contract_hash.clone()),
            task_class: RuntimeTaskClass::Agent,
            allowed_evidence_sources: recipe_evidence_sources(&installed.contract),
            max_children: installed.contract.termination.max_child_tasks,
            max_depth: installed.contract.termination.max_depth,
            priority_ceiling,
            budget: installed.contract.budget.clone(),
            retry: installed.contract.retry.clone(),
            on_failure: installed.contract.on_failure,
        });
    }

    for (purpose, _, _) in ACTIVE_RECIPE_POLICIES {
        if !installed_purposes.contains(purpose) {
            return Err(RuntimeError::MissingActiveContract(purpose));
        }
    }
    if !outcome_worker_installed {
        return Err(RuntimeError::MissingActiveContract(
            akzio_domain::LEARNING_OUTCOME_WORKER_RECIPE_ID,
        ));
    }

    let (terminal_recipes, terminals) = rust_terminal_recipes()?;
    recipes.extend(terminal_recipes);
    RecipeCatalogue::new(recipes, planner, terminals, max_nodes)
}

fn recipe_evidence_sources(contract: &akzio_domain::AgentContract) -> BTreeSet<String> {
    contract
        .tool_grants
        .iter()
        .filter(|grant| grant.kind == akzio_domain::ToolKind::ReadEvidence)
        .flat_map(|grant| grant.allowed_sources.iter().cloned())
        .collect()
}

pub fn rust_terminal_recipes() -> RuntimeResult<(Vec<TaskRecipe>, TerminalRecipeSet)> {
    let evidence = rust_gate_recipe(EVIDENCE_GATE_RECIPE_ID, RuntimeTaskClass::Evidence)?;
    let decision = rust_gate_recipe(DECISION_GATE_RECIPE_ID, RuntimeTaskClass::DecisionGate)?;
    let execution = rust_gate_recipe(EXECUTION_GATE_RECIPE_ID, RuntimeTaskClass::ExecutionGate)?;
    let paper = rust_gate_recipe(PAPER_COMMIT_RECIPE_ID, RuntimeTaskClass::PaperCommit)?;
    let reconcile = rust_gate_recipe(RECONCILE_RECIPE_ID, RuntimeTaskClass::Reconcile)?;
    let evaluate = rust_gate_recipe(EVALUATE_RECIPE_ID, RuntimeTaskClass::Evaluate)?;
    let terminals = TerminalRecipeSet {
        evidence_gate: evidence.recipe_id.clone(),
        decision_gate: decision.recipe_id.clone(),
        execution_gate: execution.recipe_id.clone(),
        paper_commit: paper.recipe_id.clone(),
        reconcile: reconcile.recipe_id.clone(),
        evaluate: evaluate.recipe_id.clone(),
    };
    Ok((
        vec![evidence, decision, execution, paper, reconcile, evaluate],
        terminals,
    ))
}

fn rust_gate_recipe(recipe_id: &str, task_class: RuntimeTaskClass) -> RuntimeResult<TaskRecipe> {
    let retry = match task_class {
        RuntimeTaskClass::Evidence => RetryPolicy {
            max_attempts: 5,
            initial_backoff_ms: 1_000,
            retry_transport: true,
            retry_rate_limited: true,
            retry_invalid_output: false,
        },
        RuntimeTaskClass::ExecutionGate => RetryPolicy {
            max_attempts: 2,
            initial_backoff_ms: 1_000,
            retry_transport: true,
            retry_rate_limited: true,
            retry_invalid_output: false,
        },
        _ => RetryPolicy::none(),
    };
    let max_wall_time_secs = if task_class == RuntimeTaskClass::ExecutionGate {
        90
    } else {
        30
    };
    Ok(TaskRecipe {
        recipe_id: TaskRecipeId::new(recipe_id)?,
        purpose: ContractPurpose::new(recipe_id)?,
        contract_hash: None,
        task_class,
        allowed_evidence_sources: BTreeSet::new(),
        max_children: 0,
        max_depth: 0,
        priority_ceiling: 100,
        budget: TaskBudget {
            max_input_tokens: 1,
            max_output_tokens: 1,
            max_wall_time_secs,
            max_tool_calls: 0,
        },
        retry,
        on_failure: FailureDisposition::FailRun,
    })
}
