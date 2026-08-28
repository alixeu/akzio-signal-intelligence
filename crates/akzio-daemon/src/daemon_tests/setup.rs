use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

use super::*;
use akzio_domain::{
    ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance, ArtifactRef, Asset,
    ContextManifestPayload, EvidenceNeed, MarketClockSnapshot, MoneyMicros, Outcome,
    OutcomeExecutionLineage, OutcomeSchedule, PaperApprovalScope, PaperLaunchApproval, Quote,
    RetrospectiveStatus, RuntimeManifest, TaskRecipeId, WorkflowProposal, WorkflowProposalDraft,
    WorkflowProposalDraftTask, WorkflowProposalTask, V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_execution::paper::{CommittedPaperBroker, PaperExecution, PaperOrderReceipt};
use akzio_ingest::{
    runtime::EvidenceAdapterError, AcquiredEvidence, AsyncEvidenceAdapter, EvidenceProvenance,
    EvidenceQuality, EvidenceRequest, NormalizedEvidencePayload, PaperDecodeError,
};
use akzio_research::v2::{
    fixture_claim_output, fixture_critique_output, fixture_model_client, AgentModel,
};
use akzio_store::v2::AlertSeverity;
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use chrono::{Datelike, Duration as ChronoDuration, Weekday};
use futures::{future::BoxFuture, StreamExt};
use tempfile::tempdir;
use tower::ServiceExt;

fn config(root: PathBuf) -> DaemonConfig {
    DaemonConfig {
        store_root: root,
        http_token: "fixture-token".to_owned(),
        observer_token: Some("fixture-observer-token".to_owned()),
        worker_count: 1,
        auto_paper: false,
        market_data_feed: Some(AlpacaMarketDataFeed::Iex),
        outcome_cost_model: OutcomeCostModel::default(),
        runtime_identity_hash: None,
    }
}
