//! Context-manifest and read-grant domain vocabulary.
//!
//! These are data-only authority records.  Minting, expiry checks, and
//! document reads remain owned by akzio-context and akzio-store.

pub use crate::schema::{ContextManifestPayload, ContextSelection, ReadGrant};
