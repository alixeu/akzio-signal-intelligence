//! Rust-owned workflow and task authority for Akzio v2.
//!
//! Planner proposals are lowered through immutable recipes and mandatory
//! terminal gates.

mod runtime_v2;

pub mod v2 {
    pub use crate::runtime_v2::{
        rust_terminal_recipes, should_run_structured_critique, RecipeCatalogue, RetryCause,
        RuntimeError, RuntimeResult as Result, TaskCompletion, TaskRuntime, TerminalRecipeSet,
        WorkflowRuntime, DECISION_GATE_RECIPE_ID, EVALUATE_RECIPE_ID, EVIDENCE_GATE_RECIPE_ID,
        EXECUTION_GATE_RECIPE_ID, PAPER_COMMIT_RECIPE_ID, RECONCILE_RECIPE_ID,
        STRUCTURED_CRITIQUE_CONFIDENCE_PPM, STRUCTURED_CRITIQUE_MATERIALITY_PPM,
    };
}

pub use v2::*;
