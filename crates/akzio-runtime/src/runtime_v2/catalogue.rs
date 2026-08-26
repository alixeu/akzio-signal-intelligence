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
