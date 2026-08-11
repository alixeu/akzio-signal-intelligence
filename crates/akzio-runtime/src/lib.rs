//! Rust-owned workflow and task authority for Akzio v2.
//!
//! Planner proposals are lowered through immutable recipes and mandatory
//! terminal gates.

mod runtime_v2;

pub mod v2;

pub use v2::*;
