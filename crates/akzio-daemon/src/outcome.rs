//! Outcome collection and evaluation dispatch.

use super::*;

#[path = "outcome/canary.rs"]
mod canary;
#[path = "outcome/collection.rs"]
mod collection;
#[path = "outcome/helpers.rs"]
mod helpers;
#[path = "outcome/materialization.rs"]
mod materialization;
#[path = "outcome/narrative_repair.rs"]
mod narrative_repair;
#[path = "outcome/shadow.rs"]
mod shadow;
#[path = "outcome/worker.rs"]
mod worker;

use helpers::*;
