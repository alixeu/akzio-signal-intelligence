//! The only context route available to agent tasks.
//!
//! The root surface is manifest-and-grant based.

mod broker;

pub use crate::broker::{
    ContextBroker, ContextDocumentMetadata, ContextError, ContextManifest, ContextMaterialization,
    ContextMustReadDocument, ContextReadResult, ContextResult as Result,
};
