//! The sole durable persistence authority for Akzio v2.
//!
//! The root surface exports only the source-incompatible v2 CAS, SQLite graph,
//! append-only events, permits, leases, slots, policy transitions, and Doctor.
mod store_v2;

pub mod v2;

pub use v2::*;
