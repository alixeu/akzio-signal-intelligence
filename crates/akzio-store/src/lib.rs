//! The sole durable persistence authority for Akzio v2.
//!
//! The root surface exports only the source-incompatible v2 CAS, SQLite graph,
//! append-only events, permits, leases, slots, policy transitions, and Doctor.
//! The hidden legacy module exists only until R3-R8 replace its current
//! callers; no new code may import it.

mod store_v2;

pub mod v2;

#[doc(hidden)]
pub mod legacy;

pub use v2::*;
