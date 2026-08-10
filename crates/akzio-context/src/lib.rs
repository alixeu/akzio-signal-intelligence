//! The only context route available to v2 agent tasks.
//!
//! The root surface is manifest-and-grant based. The hidden legacy module
//! remains solely for active callers that have not reached their owner phase;
//! new Agent, Evidence, Workflow, and Execution code must use the v2 broker.

mod broker_v2;

pub mod v2;

#[doc(hidden)]
pub mod legacy;

pub use v2::*;
