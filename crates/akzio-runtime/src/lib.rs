//! Rust-owned workflow and task authority for Akzio v2.
//!
//! Planner proposals are lowered through immutable recipes and mandatory
//! terminal gates.

mod runtime_v2;

pub mod v2 {
    pub use crate::runtime_v2::{
        active_recipe_catalogue, rust_terminal_recipes, should_run_structured_critique,
        ActiveContractRecipe, RecipeCatalogue, RetryCause, RuntimeError, RuntimeResult as Result,
        StoreExecutor, TaskCompletion, TaskRuntime, TerminalRecipeSet, WorkflowRuntime,
        DECISION_GATE_RECIPE_ID, EVALUATE_RECIPE_ID, EVIDENCE_GATE_RECIPE_ID,
        EXECUTION_GATE_RECIPE_ID, PAPER_COMMIT_RECIPE_ID, RECONCILE_RECIPE_ID,
        STRUCTURED_CRITIQUE_CONFIDENCE_PPM, STRUCTURED_CRITIQUE_MATERIALITY_PPM,
    };
}

use akzio_domain::ContentHash;

pub fn topology_component_hash() -> ContentHash {
    let components: &[(&str, &[u8])] = &[
        (
            "crates/akzio-runtime/src/runtime_v2.rs",
            include_bytes!("runtime_v2.rs"),
        ),
        (
            "crates/akzio-runtime/src/runtime_v2/catalogue.rs",
            include_bytes!("runtime_v2/catalogue.rs"),
        ),
        (
            "crates/akzio-runtime/src/runtime_v2/planner.rs",
            include_bytes!("runtime_v2/planner.rs"),
        ),
        (
            "crates/akzio-runtime/src/runtime_v2/planner_parts/lowering.rs",
            include_bytes!("runtime_v2/planner_parts/lowering.rs"),
        ),
        (
            "crates/akzio-runtime/src/runtime_v2/planner_parts/validation.rs",
            include_bytes!("runtime_v2/planner_parts/validation.rs"),
        ),
        (
            "crates/akzio-runtime/src/runtime_v2/planner_parts/evidence.rs",
            include_bytes!("runtime_v2/planner_parts/evidence.rs"),
        ),
        (
            "crates/akzio-runtime/src/runtime_v2/planner_parts/helpers.rs",
            include_bytes!("runtime_v2/planner_parts/helpers.rs"),
        ),
        (
            "crates/akzio-runtime/src/runtime_v2/reducer.rs",
            include_bytes!("runtime_v2/reducer.rs"),
        ),
        (
            "crates/akzio-runtime/src/runtime_v2/replay.rs",
            include_bytes!("runtime_v2/replay.rs"),
        ),
        (
            "crates/akzio-runtime/src/runtime_v2/store_executor.rs",
            include_bytes!("runtime_v2/store_executor.rs"),
        ),
        (
            "crates/akzio-runtime/src/runtime_v2/task.rs",
            include_bytes!("runtime_v2/task.rs"),
        ),
        (
            "crates/akzio-runtime/src/runtime_v2/workflow.rs",
            include_bytes!("runtime_v2/workflow.rs"),
        ),
        ("crates/akzio-runtime/src/lib.rs", include_bytes!("lib.rs")),
    ];
    let mut bytes = Vec::new();
    for (path, component) in components {
        bytes.extend_from_slice(path.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(component);
        bytes.push(0);
    }
    ContentHash::of_bytes(&bytes)
}
