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
