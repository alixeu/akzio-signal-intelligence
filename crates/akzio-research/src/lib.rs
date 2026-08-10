//! Contract-driven research plane for Akzio v2.
//!
//! The root API permits only installed immutable contracts, bound model turns,
//! schema-validated artifacts, and grant-checked tools. The hidden legacy
//! module exists solely until R5/R8 replace its fixed-role callers.

mod agent_v2;
mod tools;

pub mod v2;

#[doc(hidden)]
pub mod legacy;

pub use v2::*;
