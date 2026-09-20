//! Rust-owned relevance query. This does not grant access to any artifact.
use std::collections::BTreeSet;

use serde::Serialize;

use crate::{Asset, DecisionHorizon, LessonScope, WorkflowNode};

/// Empty query dimensions mean unknown, not unrestricted. An unrestricted
/// Lesson dimension still matches; a scoped Lesson needs an explicit overlap.
/// Cross-horizon research explicitly requests all three horizons.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ContextQueryScope {
    pub assets: BTreeSet<Asset>,
    pub horizons: BTreeSet<DecisionHorizon>,
    pub regimes: BTreeSet<String>,
    /// Frozen recipe IDs, not labels extracted from evidence prose.
    pub decision_stages: BTreeSet<String>,
}

impl ContextQueryScope {
    /// The caller must validate the node against its persisted execution scope.
    pub fn for_node(node: &WorkflowNode) -> Self {
        let recipe = node.recipe_id.as_str();
        let mut scope = Self::default();
        if !matches!(
            recipe,
            crate::RESEARCH_ANALYST_RECIPE_ID
                | crate::RESEARCH_CRITIC_RECIPE_ID
                | crate::RESEARCH_SYNTHESIZER_RECIPE_ID
                | crate::RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID
                | crate::LEARNING_OUTCOME_WORKER_RECIPE_ID
        ) {
            return scope;
        }
        // The current research contract covers the fixed four-ETF universe.
        scope.assets.extend(Asset::EXECUTABLE);
        scope.decision_stages.insert(recipe.to_owned());
        if matches!(
            recipe,
            crate::RESEARCH_SYNTHESIZER_RECIPE_ID | crate::RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID
        ) {
            scope.horizons.extend([
                DecisionHorizon::T1,
                DecisionHorizon::T3,
                DecisionHorizon::T5,
            ]);
        } else if let Some(horizon) = node.execution_spec().horizon {
            scope.horizons.insert(horizon);
        }
        scope
    }

    pub fn matches(&self, lesson: &LessonScope) -> bool {
        fn overlaps<T: Ord>(applicable: &BTreeSet<T>, requested: &BTreeSet<T>) -> bool {
            applicable.is_empty() || !applicable.is_disjoint(requested)
        }
        overlaps(&lesson.assets, &self.assets)
            && overlaps(&lesson.horizons, &self.horizons)
            && overlaps(&lesson.regimes, &self.regimes)
            && overlaps(&lesson.decision_stages, &self.decision_stages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FailureDisposition, NodeSpec, RetryPolicy, TaskBudget, TaskId, TaskRecipeId};

    fn node(recipe: &str) -> WorkflowNode {
        WorkflowNode {
            spec: Some(NodeSpec {
                key: "research.analyst.t1".into(),
                horizon: Some(DecisionHorizon::T1),
                research_round: Some(0),
                proposal_revision: None,
            }),
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new(recipe).unwrap(),
            contract_hash: None,
            objective: "[research_horizon=t5] stage:untrusted".into(),
            dependencies: vec![],
            input_artifacts: vec![],
            priority: 1,
            budget: TaskBudget {
                max_input_tokens: 1000,
                max_output_tokens: 100,
                max_wall_time_secs: 30,
                max_tool_calls: crate::budget::ToolCallLimit::Limited(0),
            },
            retry: RetryPolicy::none(),
            on_failure: FailureDisposition::FailTask,
            parent_task_id: None,
        }
    }

    #[test]
    fn typed_node_scope_wins_over_conflicting_objective() {
        let query = ContextQueryScope::for_node(&node(crate::RESEARCH_ANALYST_RECIPE_ID));
        assert_eq!(query.horizons, BTreeSet::from([DecisionHorizon::T1]));
        assert_eq!(query.assets, BTreeSet::from(Asset::EXECUTABLE));
        assert_eq!(
            query.decision_stages,
            BTreeSet::from([crate::RESEARCH_ANALYST_RECIPE_ID.to_owned()])
        );
    }

    #[test]
    fn unknown_horizon_does_not_mean_all_horizons() {
        let mut scoped_lesson = LessonScope::default();
        scoped_lesson.horizons.insert(DecisionHorizon::T5);
        assert!(!ContextQueryScope::default().matches(&scoped_lesson));
        assert!(ContextQueryScope::default().matches(&LessonScope::default()));
        let mut synthesis = node(crate::RESEARCH_SYNTHESIZER_RECIPE_ID);
        synthesis.spec = Some(NodeSpec {
            key: "synth".into(),
            horizon: None,
            research_round: None,
            proposal_revision: Some(0),
        });
        let query = ContextQueryScope::for_node(&synthesis);
        assert_eq!(
            query.horizons,
            BTreeSet::from([
                DecisionHorizon::T1,
                DecisionHorizon::T3,
                DecisionHorizon::T5
            ])
        );
        assert!(query.matches(&scoped_lesson));
    }

    #[test]
    fn historical_node_uses_existing_read_adapter_without_rewriting_it() {
        let mut historical = node(crate::RESEARCH_CRITIC_RECIPE_ID);
        historical.spec = None;
        let original = historical.clone();
        assert_eq!(
            ContextQueryScope::for_node(&historical).horizons,
            BTreeSet::from([DecisionHorizon::T5])
        );
        assert_eq!(historical, original);
        historical.objective = "No frozen horizon".into();
        assert!(ContextQueryScope::for_node(&historical).horizons.is_empty());
    }
}
