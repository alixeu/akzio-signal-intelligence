//! The only context route available to v2 agent tasks.
//!
//! The root surface is manifest-and-grant based.

mod broker_v2;

pub mod v2;

pub use v2::*;
