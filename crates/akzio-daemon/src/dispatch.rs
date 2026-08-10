//! Dispatch policy is implemented through the v2 `Daemon` task handler.
//!
//! The former document-oriented dispatcher was intentionally removed: it
//! could bypass `ContextManifest`/`ReadGrant` and the v2 task write permit.
//! `Daemon::execute_task` now routes only by the installed recipe class and
//! returns fail-closed completion for gates that have no R6/R7 implementation.
