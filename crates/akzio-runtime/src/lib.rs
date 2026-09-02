//! Rust-owned workflow and task authority for Akzio.
//!
//! Planner proposals are lowered through immutable recipes and mandatory
//! terminal gates.

mod runtime;

pub use crate::runtime::{
    active_recipe_catalogue, rust_terminal_recipes, should_run_structured_critique,
    ActiveContractRecipe, RecipeCatalogue, RetryCause, RuntimeError, RuntimeResult as Result,
    StoreExecutor, StoreExecutorTelemetry, StoreMaintenanceKind, StoreMaintenanceOutcome,
    StoreMaintenanceState, TaskCompletion, TaskRuntime, TerminalRecipeSet, WorkflowRuntime,
    DECISION_GATE_RECIPE_ID, EVALUATE_RECIPE_ID, EVIDENCE_GATE_RECIPE_ID, EXECUTION_GATE_RECIPE_ID,
    PAPER_COMMIT_RECIPE_ID, RECONCILE_RECIPE_ID, STRUCTURED_CRITIQUE_CONFIDENCE_PPM,
    STRUCTURED_CRITIQUE_MATERIALITY_PPM,
};

use akzio_domain::ContentHash;

pub fn topology_component_hash() -> ContentHash {
    let components: &[(&str, &[u8])] = &[
        (
            "crates/akzio-runtime/src/runtime.rs",
            include_bytes!("runtime.rs"),
        ),
        (
            "crates/akzio-runtime/src/runtime/catalogue.rs",
            include_bytes!("runtime/catalogue.rs"),
        ),
        (
            "crates/akzio-runtime/src/runtime/planner.rs",
            include_bytes!("runtime/planner.rs"),
        ),
        (
            "crates/akzio-runtime/src/runtime/planner/lowering.rs",
            include_bytes!("runtime/planner/lowering.rs"),
        ),
        (
            "crates/akzio-runtime/src/runtime/planner/validation.rs",
            include_bytes!("runtime/planner/validation.rs"),
        ),
        (
            "crates/akzio-runtime/src/runtime/planner/evidence.rs",
            include_bytes!("runtime/planner/evidence.rs"),
        ),
        (
            "crates/akzio-runtime/src/runtime/planner/helpers.rs",
            include_bytes!("runtime/planner/helpers.rs"),
        ),
        (
            "crates/akzio-runtime/src/runtime/reducer.rs",
            include_bytes!("runtime/reducer.rs"),
        ),
        (
            "crates/akzio-runtime/src/runtime/replay.rs",
            include_bytes!("runtime/replay.rs"),
        ),
        (
            "crates/akzio-runtime/src/runtime/store_executor.rs",
            include_bytes!("runtime/store_executor.rs"),
        ),
        (
            "crates/akzio-runtime/src/runtime/task.rs",
            include_bytes!("runtime/task.rs"),
        ),
        (
            "crates/akzio-runtime/src/runtime/workflow.rs",
            include_bytes!("runtime/workflow.rs"),
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
