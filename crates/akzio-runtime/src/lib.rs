//! Rust-owned workflow and task authority for Akzio v2.
//!
//! Planner proposals are lowered through immutable recipes and mandatory
//! terminal gates. The hidden legacy module remains only until R8 removes its
//! fixed-lifecycle daemon callers.

mod runtime_v2;

pub mod v2;

#[doc(hidden)]
pub mod legacy;

pub use v2::*;
