//! Outcome collection and evaluation dispatch.

use super::*;

#[path = "outcome_parts/canary.rs"]
mod canary;
#[path = "outcome_parts/collection.rs"]
mod collection;
#[path = "outcome_parts/helpers.rs"]
mod helpers;
#[path = "outcome_parts/materialization.rs"]
mod materialization;
#[path = "outcome_parts/shadow.rs"]
mod shadow;
#[path = "outcome_parts/worker.rs"]
mod worker;

use helpers::*;
