//! Read-only, share-safe diagnostic bundle export.
//!
//! This module deliberately sits inside `akzio-store`.  It reads one SQLite
//! snapshot, follows only the selected run's artifact closure, and writes a
//! derived directory outside the Store.  It never repairs, migrates, claims,
//! leases, calls a model, or calls a network adapter.

use super::blob::read_blob_bytes;
use super::debug::{environment_identity, read_session};
use super::trajectory::stored_event_from_row;
use super::*;
use serde_json::json;
use std::fs::{self, OpenOptions};
use std::io::Write;

const EXPORTER_VERSION: &str = "debug-bundle-v2";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugBundleManifest {
    pub schema_version: u32,
    pub exporter_version: String,
    pub run_id: RunId,
    pub purpose: Option<RunPurpose>,
    pub store_identity: Option<String>,
    pub debug_session: Option<serde_json::Value>,
    pub raw_model_access: DebugBundleRawAccess,
    pub exported_at: DateTime<Utc>,
    pub snapshot_cursor: i64,
    pub counts: serde_json::Value,
    pub integrity: DebugBundleIntegrity,
    pub missing: Vec<serde_json::Value>,
    pub redactions: Vec<String>,
    pub file_hashes: BTreeMap<String, ContentHash>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugBundleRawAccess {
    pub requested: bool,
    pub allowed: bool,
    pub reason: String,
    pub compatibility: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugBundleIntegrity {
    pub complete: bool,
    pub partial: bool,
    pub status: String,
    pub captured_at_single_read_snapshot: bool,
    pub event_cursor_watermark: i64,
    pub unknown_after_crash_calls: u64,
    pub uncaptured_payloads: u64,
    pub corruption_or_missing_blobs: u64,
    pub dropped_records: u64,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone)]
struct BundleArtifact {
    artifact: Artifact,
    payload: Option<serde_json::Value>,
    omitted_reason: Option<String>,
}

#[derive(Debug, Clone)]
struct BundleEvent {
    event: StoredEvent,
    role: Option<String>,
    horizon: Option<String>,
    phase: Option<String>,
    turn: Option<u64>,
    call_id: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct BundleProblems {
    missing: Vec<serde_json::Value>,
    redactions: Vec<String>,
    unknown_after_crash_calls: u64,
    uncaptured_payloads: u64,
    corruption_or_missing_blobs: u64,
    dropped_records: u64,
}

impl Store {
    /// Export a human-readable and machine-readable bundle from one SQLite
    /// read transaction.  The target must not exist.  Existing debug
    /// isolation decides whether provider request/result bodies may be
    /// included; the caller cannot elevate that permission with a flag.
    pub fn export_debug_bundle(
        &self,
        run_id: &RunId,
        target: impl AsRef<Path>,
    ) -> StoreResult<DebugBundleManifest> {
        let target = target.as_ref().to_path_buf();
        validate_export_target(&target, self.root())?;

        let exported_at = Utc::now();
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;

        let (purpose, run_json) = read_run_identity(&transaction, run_id)?;
        let (debug_session, store_identity, raw_access) =
            raw_model_access(&transaction, run_id, purpose);
        let workflow_result = self
            .workflow_snapshot_with_connection(&transaction, run_id)
            .map(|snapshot| serde_json::to_value(snapshot).map_err(StoreError::from));
        let mut problems = BundleProblems::default();
        let workflow_json = match workflow_result {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                problems.missing.push(json!({
                    "kind": "workflow",
                    "reason": "serialization_failed",
                    "detail": error.to_string()
                }));
                serde_json::json!({"unavailable": true, "reason": "serialization_failed"})
            }
            Err(error) => {
                problems.missing.push(json!({
                    "kind": "workflow",
                    "reason": "not_readable",
                    "detail": error.to_string()
                }));
                raw_workflow_fallback(&transaction, run_id).unwrap_or_else(
                    |_| serde_json::json!({"unavailable": true, "reason": "not_readable"}),
                )
            }
        };

        let events = read_events_snapshot(&transaction, run_id)?;
        let snapshot_cursor = events.last().map(|event| event.cursor).unwrap_or_else(|| {
            transaction
                .query_row(
                    "SELECT COALESCE(MAX(event_id), 0) FROM rebuild_events",
                    [],
                    |row| row.get(0),
                )
                .unwrap_or_default()
        });
        let task_index = task_index(&transaction, run_id, &workflow_json)?;
        let attempts_json = read_tasks_attempts(&transaction, run_id, &task_index)?;

        let mut pending = BTreeSet::<ArtifactId>::new();
        pending.extend(events.iter().filter_map(|event| event.artifact_id.clone()));
        if let Some(graph_id) = run_json
            .get("graph_artifact_id")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| ContentHash::new(value.to_owned()).ok())
        {
            pending.insert(ArtifactId(graph_id));
        }
        if let Some(tasks) = task_index.get("tasks").and_then(|v| v.as_array()) {
            for task in tasks {
                if let Some(inputs) = task_field(task, "input_artifacts").and_then(|v| v.as_array())
                {
                    for input in inputs {
                        if let Some(id) = input
                            .get("artifact_id")
                            .and_then(serde_json::Value::as_str)
                            .and_then(|value| ContentHash::new(value.to_owned()).ok())
                        {
                            pending.insert(ArtifactId(id));
                        }
                    }
                }
            }
        }
        for artifact_id in attempt_output_ids(&transaction, run_id)? {
            pending.insert(artifact_id);
        }
        if let Some(session) = &debug_session {
            if let Some(identity) = session.get("identity") {
                for key in ["dataset", "parent_artifacts"] {
                    if let Some(values) = identity.get(key).and_then(|value| value.as_array()) {
                        for reference in values {
                            if let Some(id) = reference
                                .get("artifact_id")
                                .and_then(serde_json::Value::as_str)
                                .and_then(|value| ContentHash::new(value.to_owned()).ok())
                            {
                                pending.insert(ArtifactId(id));
                            }
                        }
                    }
                }
            }
        }

        let mut artifacts = Vec::new();
        let mut visited = BTreeSet::new();
        while let Some(artifact_id) = pending.pop_first() {
            if !visited.insert(artifact_id.clone()) {
                continue;
            }
            let artifact = match read_artifact(&transaction, &artifact_id) {
                Ok(artifact) => artifact,
                Err(error) => {
                    problems.missing.push(json!({
                        "artifact_id": artifact_id,
                        "reason": "artifact_row_unreadable",
                        "detail": error.to_string()
                    }));
                    problems.corruption_or_missing_blobs += 1;
                    continue;
                }
            };
            pending.extend(
                artifact
                    .source_refs
                    .iter()
                    .map(|reference| reference.artifact_id.clone()),
            );

            let raw_model = is_trajectory_redacted_kind(artifact.kind)
                || artifact.kind == ArtifactKind::RawEvidence;
            let cross_run_allowed = cross_run_payload_allowed(&artifact, run_id, &debug_session);
            let should_capture = cross_run_allowed && (!raw_model || raw_access.allowed);
            let (payload, omitted_reason) = if !should_capture {
                problems.uncaptured_payloads += 1;
                (
                    None,
                    Some(if !cross_run_allowed {
                        "cross_run_source_not_authorized".to_owned()
                    } else {
                        "provider_detail_not_authorized_for_this_run".to_owned()
                    }),
                )
            } else {
                match read_blob_bytes(&transaction, &artifact.blob.hash, artifact.blob.bytes) {
                    Ok(bytes) => match decode_bundle_payload(&artifact.blob.media_type, &bytes) {
                        Some(value) => (Some(value), None),
                        None => {
                            problems.missing.push(json!({
                                "artifact_id": artifact.artifact_id,
                                "reason": "unsupported_or_invalid_payload_encoding",
                                "source_bytes": artifact.blob.bytes
                            }));
                            problems.uncaptured_payloads += 1;
                            (
                                None,
                                Some("unsupported_or_invalid_payload_encoding".to_owned()),
                            )
                        }
                    },
                    Err(error) => {
                        problems.missing.push(json!({
                            "artifact_id": artifact.artifact_id,
                            "source_blob_hash": artifact.blob.hash,
                            "reason": "blob_missing_or_corrupt",
                            "detail": error.to_string()
                        }));
                        problems.corruption_or_missing_blobs += 1;
                        (None, Some("blob_missing_or_corrupt".to_owned()))
                    }
                }
            };
            artifacts.push(BundleArtifact {
                artifact,
                payload,
                omitted_reason,
            });
        }
        artifacts.sort_by(|left, right| left.artifact.artifact_id.cmp(&right.artifact.artifact_id));

        let bundle_events = events
            .into_iter()
            .map(|event| enrich_event(event, &task_index, &artifacts))
            .collect::<Vec<_>>();
        let call_records = build_llm_calls(
            &bundle_events,
            &artifacts,
            &task_index,
            raw_access.allowed,
            &mut problems,
        );
        let tool_records = build_tool_records(&bundle_events, &artifacts, &task_index);
        let rust_records = build_rust_decisions(&bundle_events, &artifacts, &task_index);
        let decision_matrix = build_decision_matrix(&artifacts, &task_index);
        let policies = build_policies_and_risk(&artifacts, &debug_session, &run_json);
        let acquisition_calls = observed_acquisition_calls(&artifacts);
        let mut failures = build_failures_and_missing(
            &bundle_events,
            &call_records,
            &tool_records,
            &problems,
            &raw_access,
        );
        failures["source_review_failures"] = source_review_failures(&artifacts);
        failures["scope"] = json!("model_failures covers research AgentTurn only; source_review_failures covers acquisition validation; missing raw access means unknown");
        let mut route_calls = call_records.clone();
        route_calls.extend(acquisition_calls.clone());
        let model_routes = build_model_routes(&route_calls);
        let evidence_status = build_evidence_status(&artifacts, &bundle_events);
        let context_coverage = build_context_coverage(&artifacts);
        let research_progress = super::research_review::research_progress(
            workflow_json["tasks"]
                .as_array()
                .map(Vec::as_slice)
                .unwrap_or_default(),
            &artifacts
                .iter()
                .filter_map(|a| a.payload.as_ref().map(|v| (&a.artifact, v)))
                .collect::<Vec<_>>(),
            &attempt_output_ids(&transaction, run_id)?
                .into_iter()
                .collect(),
            debug_session
                .as_ref()
                .and_then(|s| s.get("identity"))
                .is_some_and(|i| {
                    i["run_purpose"] == "position_plan"
                        && i["llm_mode"] != "fixture"
                        && i["decision_policy_artifact"].is_null()
                }),
        );
        let research_audit = super::research_review::build_research_audit(
            &workflow_json,
            research_progress,
            &artifacts
                .iter()
                .filter_map(|a| a.payload.as_ref().map(|v| (&a.artifact, v)))
                .collect::<Vec<_>>(),
        );
        let draft_submit_coverage = build_draft_submit_coverage(&call_records);
        let stage_acceptance = build_stage_acceptance(
            &artifacts,
            workflow_json["tasks"]
                .as_array()
                .map(Vec::as_slice)
                .unwrap_or_default(),
        );
        let timeline = bundle_events
            .iter()
            .map(|event| {
                json!({
                    "schema_version": DOMAIN_SCHEMA_VERSION,
                    "event_id": event.event.cursor,
                    "cursor": event.event.cursor,
                    "observed_at_utc": event.event.created_at,
                    "session_id": debug_session.as_ref().and_then(|s| s.pointer("/identity/debug_session_id")),
                    "experiment_id": run_json.get("run_id"),
                    "run_id": event.event.run_id,
                    "task_id": event.event.task_id,
                    "attempt_id": event.event.attempt_id,
                    "role": event.role,
                    "horizon": event.horizon,
                    "phase": event.phase,
                    "turn_id": event.turn,
                    "call_id": event.call_id,
                    "retry_index": event
                        .event
                        .artifact_id
                        .as_ref()
                        .and_then(|id| artifact_payload(&artifacts, id))
                        .and_then(|payload| payload.get("attempt"))
                        .cloned(),
                    "parent_call_id": parent_call_id_for_event(event, &call_records),
                    "event_type": event.event.event_type,
                    "artifact_refs": event.event.artifact_id,
                    "source_location": "persisted Store event; source location not recorded"
                })
            })
            .collect::<Vec<_>>();

        // Derived views may repeat provider payloads or nested evidence. Run
        // the same recursive share-safe redaction over every outward-facing
        // JSON view, not only over artifacts/.
        let safe_call_records = redact_records(&call_records, &mut problems.redactions);
        let safe_tool_records = redact_records(&tool_records, &mut problems.redactions);
        let safe_rust_records = redact_records(&rust_records, &mut problems.redactions);
        let safe_timeline = redact_records(&timeline, &mut problems.redactions);
        let safe_evidence_status = redact_value(&evidence_status, &mut problems.redactions);
        let safe_research_audit = redact_value(
            &serde_json::to_value(&research_audit)?,
            &mut problems.redactions,
        );
        let safe_context_coverage = redact_value(&context_coverage, &mut problems.redactions);
        let safe_draft_submit_coverage =
            redact_value(&draft_submit_coverage, &mut problems.redactions);
        let safe_stage_acceptance = redact_value(&stage_acceptance, &mut problems.redactions);
        let safe_decision_matrix = redact_value(&decision_matrix, &mut problems.redactions);
        let safe_policies = redact_value(&policies, &mut problems.redactions);
        let safe_failures = redact_value(&failures, &mut problems.redactions);
        let transcript = render_transcript(&safe_call_records, &raw_access);
        let decisions_markdown = render_rust_decisions(&safe_rust_records);
        let summary = render_summary(
            &run_json,
            &workflow_json,
            &safe_call_records,
            &safe_rust_records,
            &safe_decision_matrix,
            &safe_failures,
            snapshot_cursor,
        );

        drop(transaction);
        drop(connection);

        fs::create_dir_all(&target).map_err(|source| StoreError::Io {
            path: target.clone(),
            source,
        })?;
        secure_directory(&target)?;
        let artifacts_dir = target.join("artifacts");
        fs::create_dir(&artifacts_dir).map_err(|source| StoreError::Io {
            path: artifacts_dir.clone(),
            source,
        })?;
        secure_directory(&artifacts_dir)?;

        let mut file_hashes = BTreeMap::new();
        write_json_file(&target, "workflow.json", &workflow_json, &mut file_hashes)?;
        write_json_file(
            &target,
            "tasks_attempts.json",
            &attempts_json,
            &mut file_hashes,
        )?;
        write_json_file(
            &target,
            "model_routes.json",
            &model_routes,
            &mut file_hashes,
        )?;
        write_jsonl_file(&target, "timeline.jsonl", &safe_timeline, &mut file_hashes)?;
        write_jsonl_file(
            &target,
            "llm_calls.jsonl",
            &safe_call_records,
            &mut file_hashes,
        )?;
        write_text_file(&target, "llm_transcript.md", &transcript, &mut file_hashes)?;
        write_jsonl_file(&target, "tools.jsonl", &safe_tool_records, &mut file_hashes)?;
        write_jsonl_file(
            &target,
            "rust_decisions.jsonl",
            &safe_rust_records,
            &mut file_hashes,
        )?;
        write_text_file(
            &target,
            "rust_decisions.md",
            &decisions_markdown,
            &mut file_hashes,
        )?;
        write_json_file(
            &target,
            "evidence_status.json",
            &safe_evidence_status,
            &mut file_hashes,
        )?;
        write_json_file(
            &target,
            "research_review.json",
            &safe_research_audit,
            &mut file_hashes,
        )?;
        write_json_file(
            &target,
            "context_coverage.json",
            &safe_context_coverage,
            &mut file_hashes,
        )?;
        write_json_file(
            &target,
            "draft_submit_coverage.json",
            &safe_draft_submit_coverage,
            &mut file_hashes,
        )?;
        write_json_file(
            &target,
            "stage_acceptance.json",
            &safe_stage_acceptance,
            &mut file_hashes,
        )?;
        write_json_file(
            &target,
            "decision_matrix.json",
            &safe_decision_matrix,
            &mut file_hashes,
        )?;
        write_json_file(
            &target,
            "policies_and_risk.json",
            &safe_policies,
            &mut file_hashes,
        )?;
        write_json_file(
            &target,
            "failures_and_missing.json",
            &safe_failures,
            &mut file_hashes,
        )?;
        let readme = render_readme(&raw_access);
        write_text_file(&target, "README.md", &readme, &mut file_hashes)?;
        write_text_file(&target, "SUMMARY.md", &summary, &mut file_hashes)?;

        let mut artifact_index = Vec::new();
        for bundle_artifact in &artifacts {
            let artifact_id = bundle_artifact.artifact.artifact_id.0.as_str();
            let relative = format!("artifacts/{artifact_id}.json");
            let mut export_payload = bundle_artifact.payload.clone().map(|mut value| {
                redact_share_safe(&mut value, &mut problems.redactions);
                value
            });
            let (payload_value, status) = if let Some(value) = export_payload.take() {
                (value, "captured")
            } else {
                (
                    json!({
                        "omitted": true,
                        "reason": bundle_artifact
                            .omitted_reason
                            .as_deref()
                            .unwrap_or("not_available"),
                        "source_artifact_hash": bundle_artifact.artifact.blob.hash,
                        "source_bytes": bundle_artifact.artifact.blob.bytes
                    }),
                    "omitted",
                )
            };
            let bytes = serde_json::to_vec_pretty(&payload_value)?;
            write_new_file(&artifacts_dir.join(format!("{artifact_id}.json")), &bytes)?;
            let export_hash = ContentHash::of_bytes(&bytes);
            let source_hash = bundle_artifact.artifact.blob.hash.clone();
            file_hashes.insert(relative.clone(), export_hash.clone());
            artifact_index.push(json!({
                "artifact_id": bundle_artifact.artifact.artifact_id,
                "kind": bundle_artifact.artifact.kind,
                "producer": bundle_artifact.artifact.producer,
                "lifecycle": bundle_artifact.artifact.lifecycle,
                "source_artifact_hash": source_hash,
                "export_payload_hash": export_hash,
                "source_bytes": bundle_artifact.artifact.blob.bytes,
                "export_bytes": bytes.len(),
                "status": status,
                "payload_file": relative,
                "source_refs": bundle_artifact.artifact.source_refs,
                "origin": bundle_artifact.artifact.origin
            }));
        }
        write_json_file(
            &target,
            "artifact_index.json",
            &json!(artifact_index),
            &mut file_hashes,
        )?;

        let partial = problems.unknown_after_crash_calls > 0
            || problems.uncaptured_payloads > 0
            || problems.corruption_or_missing_blobs > 0
            || !problems.missing.is_empty();
        let integrity = DebugBundleIntegrity {
            complete: !partial,
            partial,
            status: if partial { "partial" } else { "complete" }.to_owned(),
            captured_at_single_read_snapshot: true,
            event_cursor_watermark: snapshot_cursor,
            unknown_after_crash_calls: problems.unknown_after_crash_calls,
            uncaptured_payloads: problems.uncaptured_payloads,
            corruption_or_missing_blobs: problems.corruption_or_missing_blobs,
            dropped_records: problems.dropped_records,
            notes: vec![
                "The bundle is a derived read-only projection; it is not a runtime state authority.".to_owned(),
                "Unmatched AgentTurnStarted is reported as unknown_after_crash/incomplete, never inferred as failed.".to_owned(),
            ],
        };
        let status_bytes: &[u8] = if partial { b"partial\n" } else { b"complete\n" };
        write_new_file(&target.join("EXPORT_STATUS"), status_bytes)?;
        file_hashes.insert(
            "EXPORT_STATUS".to_owned(),
            ContentHash::of_bytes(status_bytes),
        );
        let manifest = DebugBundleManifest {
            schema_version: DOMAIN_SCHEMA_VERSION,
            exporter_version: EXPORTER_VERSION.to_owned(),
            run_id: run_id.clone(),
            purpose,
            store_identity,
            debug_session,
            raw_model_access: raw_access,
            exported_at,
            snapshot_cursor,
            counts: json!({
                "events": timeline.len(),
                "artifacts": artifact_index.len(),
                "agent_calls": call_records.len(),
                "acquisition_model_calls": acquisition_calls.len(),
                "call_count_scope": "research AgentTurn and acquisition provider responses are separate",
                "tool_records": tool_records.len(),
                "rust_decisions": rust_records.len()
            }),
            integrity,
            missing: problems.missing,
            redactions: problems.redactions,
            file_hashes,
        };
        let final_manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
        write_new_file(&target.join("manifest.json"), &final_manifest_bytes)?;

        let mut checksums = String::new();
        let mut files = collect_files(&target)?;
        files.sort();
        for path in files {
            if path.file_name().and_then(|name| name.to_str()) == Some("checksums.sha256") {
                continue;
            }
            let bytes = fs::read(&path).map_err(|source| StoreError::Io {
                path: path.clone(),
                source,
            })?;
            let relative = path
                .strip_prefix(&target)
                .map_err(|_| StoreError::Integrity("bundle path escaped target".to_owned()))?
                .to_string_lossy()
                .replace(std::path::MAIN_SEPARATOR, "/");
            checksums.push_str(&format!(
                "{}  {}\n",
                ContentHash::of_bytes(&bytes),
                relative
            ));
        }
        write_new_file(&target.join("checksums.sha256"), checksums.as_bytes())?;
        secure_file(&target.join("manifest.json"))?;
        secure_file(&target.join("checksums.sha256"))?;
        Ok(manifest)
    }
}

fn validate_export_target(target: &Path, store_root: &Path) -> StoreResult<()> {
    if target.exists() {
        return Err(StoreError::BackupTargetExists(target.to_path_buf()));
    }
    let parent = target.parent().ok_or_else(|| StoreError::Io {
        path: target.to_path_buf(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidInput, "target has no parent"),
    })?;
    fs::create_dir_all(parent).map_err(|source| StoreError::Io {
        path: parent.to_path_buf(),
        source,
    })?;
    let canonical_parent = fs::canonicalize(parent).map_err(|source| StoreError::Io {
        path: parent.to_path_buf(),
        source,
    })?;
    let canonical_store = fs::canonicalize(store_root).map_err(|source| StoreError::Io {
        path: store_root.to_path_buf(),
        source,
    })?;
    if canonical_parent.starts_with(&canonical_store) {
        return Err(StoreError::BackupInsideStoreRoot(target.to_path_buf()));
    }
    Ok(())
}

fn read_run_identity(
    connection: &Connection,
    run_id: &RunId,
) -> StoreResult<(Option<RunPurpose>, serde_json::Value)> {
    let row = connection
        .query_row(
            "SELECT purpose, topology_id, graph_artifact_id, status, created_at, finished_at FROM rebuild_runs WHERE run_id=?1",
            params![run_id.0],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            },
        )
        .optional()?;
    let Some((purpose, topology_id, graph_artifact_id, status, created_at, finished_at)) = row
    else {
        return Err(StoreError::MissingRun(run_id.clone()));
    };
    let purpose_value = parse_enum::<RunPurpose>(&purpose).ok();
    Ok((
        purpose_value,
        json!({
            "run_id": run_id,
            "purpose": purpose,
            "topology_id": topology_id,
            "graph_artifact_id": graph_artifact_id,
            "status": status,
            "created_at": created_at,
            "finished_at": finished_at
        }),
    ))
}

fn raw_model_access(
    connection: &Connection,
    run_id: &RunId,
    purpose: Option<RunPurpose>,
) -> (
    Option<serde_json::Value>,
    Option<String>,
    DebugBundleRawAccess,
) {
    let session = read_session(connection, run_id)
        .ok()
        .flatten()
        .and_then(|value| serde_json::to_value(value).ok());
    let store_identity = environment_identity(connection).ok().flatten();
    if purpose == Some(RunPurpose::Debug) && session.is_none() {
        return (
            None,
            store_identity,
            DebugBundleRawAccess {
                requested: true,
                allowed: true,
                reason: "legacy_debug_purpose_compatibility".to_owned(),
                compatibility: "historical RunPurpose::Debug export remains readable".to_owned(),
            },
        );
    }
    let allowed = session.as_ref().is_some_and(|value| {
        let identity = value.get("identity");
        identity
            .and_then(|v| v.get("run_id"))
            .and_then(serde_json::Value::as_str)
            == Some(run_id.0.as_str())
            && identity
                .and_then(|v| v.get("learning_scope"))
                .and_then(serde_json::Value::as_str)
                == Some("isolated")
            && identity
                .and_then(|v| v.get("store_identity"))
                .and_then(serde_json::Value::as_str)
                == store_identity.as_deref()
            && identity
                .and_then(|v| v.get("run_purpose"))
                .and_then(serde_json::Value::as_str)
                == purpose.map(enum_name).as_deref()
    });
    (
        session,
        store_identity,
        DebugBundleRawAccess {
            requested: true,
            allowed,
            reason: if allowed {
                "isolated_debug_session_identity_matches_store_and_run".to_owned()
            } else {
                "raw_model_details_require_a_matching_isolated_debug_session".to_owned()
            },
            compatibility: "purpose_and_debug_isolation_are_checked_separately".to_owned(),
        },
    )
}

fn cross_run_payload_allowed(
    artifact: &Artifact,
    run_id: &RunId,
    debug_session: &Option<serde_json::Value>,
) -> bool {
    let Some(source_run) = artifact
        .origin
        .as_ref()
        .and_then(|origin| origin.run_id.as_ref())
    else {
        // Shared contract/workflow artifacts have no owning Run and are safe
        // to include as closure metadata/payload under the Store export gate.
        return true;
    };
    if source_run == run_id {
        return true;
    }
    let Some(identity) = debug_session
        .as_ref()
        .and_then(|value| value.get("identity"))
    else {
        return false;
    };
    ["dataset", "parent_artifacts"].iter().any(|key| {
        identity
            .get(*key)
            .and_then(|value| value.as_array())
            .is_some_and(|references| {
                references.iter().any(|reference| {
                    reference
                        .get("artifact_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(artifact.artifact_id.0.as_str())
                })
            })
    })
}

fn raw_workflow_fallback(
    connection: &Connection,
    run_id: &RunId,
) -> StoreResult<serde_json::Value> {
    let tasks = connection
        .prepare("SELECT task_id, recipe_id, objective, status, budget_json, input_artifacts_json, node_spec_json FROM rebuild_tasks WHERE run_id=?1 ORDER BY task_id")?
        .query_map(params![run_id.0], |row| {
            Ok(json!({
                "task_id": row.get::<_, String>(0)?,
                "recipe_id": row.get::<_, String>(1)?,
                "objective": row.get::<_, String>(2)?,
                "spec": row.get::<_, Option<String>>(6)?.and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok()),
                "status": row.get::<_, String>(3)?,
                "budget_json": serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(4)?).unwrap_or(serde_json::Value::Null),
                "input_artifacts": serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(5)?).unwrap_or(serde_json::Value::Null)
            }))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(json!({"run_id": run_id, "tasks": tasks, "unavailable": "typed_workflow_snapshot"}))
}

fn read_events_snapshot(connection: &Connection, run_id: &RunId) -> StoreResult<Vec<StoredEvent>> {
    connection
        .prepare("SELECT event_id, run_id, task_id, attempt_id, event_type, artifact_id, created_at FROM rebuild_events WHERE run_id=?1 ORDER BY event_id ASC")?
        .query_map(params![run_id.0], stored_event_from_row)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(StoreError::from)
}

fn task_index(
    connection: &Connection,
    run_id: &RunId,
    workflow: &serde_json::Value,
) -> StoreResult<serde_json::Value> {
    if let Some(tasks) = workflow.get("tasks").and_then(|value| value.as_array()) {
        return Ok(json!({"tasks": tasks}));
    }
    let tasks = connection
        .prepare("SELECT task_id, recipe_id, objective, status, budget_json, input_artifacts_json, node_spec_json FROM rebuild_tasks WHERE run_id=?1 ORDER BY task_id")?
        .query_map(params![run_id.0], |row| {
            let budget = serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(4)?).unwrap_or(serde_json::Value::Null);
            let inputs = serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(5)?).unwrap_or(serde_json::Value::Array(Vec::new()));
            Ok(json!({"task_id":row.get::<_,String>(0)?,"recipe_id":row.get::<_,String>(1)?,"objective":row.get::<_,String>(2)?,"spec":row.get::<_,Option<String>>(6)?.and_then(|s|serde_json::from_str::<serde_json::Value>(&s).ok()),"status":row.get::<_,String>(3)?,"budget":budget,"input_artifacts":inputs}))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(json!({"tasks": tasks}))
}

fn read_tasks_attempts(
    connection: &Connection,
    run_id: &RunId,
    index: &serde_json::Value,
) -> StoreResult<serde_json::Value> {
    let attempts = connection
        .prepare("SELECT attempt_id, task_id, run_id, lease_id, epoch, worker_id, status, started_at, finished_at FROM rebuild_attempts WHERE run_id=?1 ORDER BY task_id, epoch")?
        .query_map(params![run_id.0], |row| {
            Ok(json!({
                "attempt_id": row.get::<_, String>(0)?, "task_id": row.get::<_, String>(1)?, "run_id": row.get::<_, String>(2)?,
                "lease_id": row.get::<_, String>(3)?, "epoch": row.get::<_, u64>(4)?, "worker_id": row.get::<_, String>(5)?,
                "status": row.get::<_, String>(6)?, "started_at": row.get::<_, String>(7)?, "finished_at": row.get::<_, Option<String>>(8)?,
                "error": {"not_recorded": true, "reason": "rebuild_attempts has no error_json column"}
            }))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(
        json!({"tasks": index.get("tasks").cloned().unwrap_or_else(|| json!([])), "attempts": attempts}),
    )
}

fn attempt_output_ids(connection: &Connection, run_id: &RunId) -> StoreResult<Vec<ArtifactId>> {
    let ids = connection
        .prepare("SELECT o.artifact_id FROM rebuild_attempt_outputs o JOIN rebuild_attempts a ON a.attempt_id=o.attempt_id WHERE a.run_id=?1 ORDER BY o.event_id")?
        .query_map(params![run_id.0], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    ids.into_iter()
        .map(|id| {
            ContentHash::new(id)
                .map(ArtifactId)
                .map_err(StoreError::from)
        })
        .collect()
}

fn enrich_event(
    event: StoredEvent,
    task_index: &serde_json::Value,
    artifacts: &[BundleArtifact],
) -> BundleEvent {
    let task = event.task_id.as_ref().and_then(|id| {
        task_index
            .get("tasks")
            .and_then(|v| v.as_array())
            .and_then(|tasks| {
                tasks.iter().find(|task| {
                    task_field(task, "task_id").and_then(serde_json::Value::as_str)
                        == Some(id.0.as_str())
                })
            })
    });
    let role = task
        .and_then(|value| task_field(value, "recipe_id").and_then(serde_json::Value::as_str))
        .map(role_name);
    let horizon = task
        .and_then(|value| akzio_domain::projected_node_spec(value.get("node").unwrap_or(value)))
        .and_then(|spec| spec.horizon_name().map(str::to_owned));
    let payload = event
        .artifact_id
        .as_ref()
        .and_then(|id| artifact_payload(artifacts, id));
    BundleEvent {
        phase: payload
            .and_then(|value| {
                value
                    .pointer("/request/phase")
                    .or_else(|| value.pointer("/phase"))
            })
            .and_then(|v| v.as_str())
            .map(str::to_owned),
        turn: payload.and_then(|value| value.get("turn").and_then(serde_json::Value::as_u64)),
        call_id: payload
            .and_then(|value| value.get("call_id").and_then(serde_json::Value::as_str))
            .map(str::to_owned),
        event,
        role,
        horizon,
    }
}

fn task_field<'a>(task: &'a serde_json::Value, field: &str) -> Option<&'a serde_json::Value> {
    task.get(field)
        .or_else(|| task.pointer(&format!("/node/{field}")))
}

fn artifact_payload<'a>(
    artifacts: &'a [BundleArtifact],
    id: &ArtifactId,
) -> Option<&'a serde_json::Value> {
    artifacts
        .iter()
        .find(|artifact| &artifact.artifact.artifact_id == id)
        .and_then(|artifact| artifact.payload.as_ref())
}

fn build_llm_calls(
    events: &[BundleEvent],
    artifacts: &[BundleArtifact],
    task_index: &serde_json::Value,
    raw_allowed: bool,
    problems: &mut BundleProblems,
) -> Vec<serde_json::Value> {
    let mut terminal_events = BTreeMap::<ArtifactId, &BundleEvent>::new();
    let mut pairing = super::trajectory::AgentTurnPairing::default();
    for event in events {
        pairing.observe(&event.event);
        if matches!(
            event.event.event_type.as_str(),
            "agent.turn"
                | "agent.turn_completed"
                | "agent.turn_failed"
                | "agent.turn_retryable_failed"
        ) {
            if let Some(id) = &event.event.artifact_id {
                terminal_events.insert(id.clone(), event);
            }
        }
    }
    let mut calls = Vec::new();
    let mut ordered = terminal_events.into_iter().collect::<Vec<_>>();
    ordered.sort_by_key(|(_, event)| event.event.cursor);
    let mut previous_by_attempt = BTreeMap::<(Option<TaskId>, Option<AttemptId>), String>::new();
    for (artifact_id, event) in ordered {
        let Some(artifact) = artifacts
            .iter()
            .find(|candidate| candidate.artifact.artifact_id == artifact_id)
        else {
            continue;
        };
        let payload = artifact.payload.as_ref();
        let call_id = payload
            .and_then(|value| value.get("call_id").and_then(serde_json::Value::as_str))
            .map(str::to_owned)
            .unwrap_or_else(|| format!("agent-turn:{}", artifact_id.0));
        let key = (event.event.task_id.clone(), event.event.attempt_id.clone());
        let provider = payload.and_then(|value| {
            value
                .get("model_debug")
                .or_else(|| value.pointer("/response/model_debug"))
        });
        let raw_visible = raw_allowed && provider.is_some();
        let status = match event.event.event_type.as_str() {
            "agent.turn_failed" | "agent.turn_retryable_failed" => "failed",
            _ => "completed",
        };
        let request = if raw_allowed {
            payload
                .and_then(|value| value.get("request").or_else(|| value.get("domain_request")))
                .cloned()
                .unwrap_or_else(|| json!({"not_returned":true}))
        } else {
            json!({"not_authorized":true,"reason":"domain_request_not_exported_for_non_isolated_run"})
        };
        let response = if raw_allowed {
            payload.and_then(|value| value.get("response")).cloned()
        } else {
            Some(
                json!({"not_authorized":true,"reason":"model_response_not_exported_for_non_isolated_run"}),
            )
        };
        let telemetry = response.as_ref().and_then(|value| value.get("telemetry")).or_else(|| payload.and_then(|value| value.get("telemetry"))).cloned().unwrap_or_else(|| json!({"input_tokens":null,"output_tokens":null,"reasoning_tokens":null,"unknown_reason":"not_returned"}));
        let mut record = json!({
            "schema_version": DOMAIN_SCHEMA_VERSION,
            "event_cursor": event.event.cursor,
            "observed_at_utc": event.event.created_at,
            "run_id": event.event.run_id,
            "task_id": event.event.task_id,
            "attempt_id": event.event.attempt_id,
            "role": event.role,
            "horizon": event.horizon,
            "phase": payload.and_then(|value| value.pointer("/request/phase").or_else(|| value.pointer("/phase"))),
            "turn_id": payload.and_then(|value| value.get("turn")),
            "call_id": call_id,
            "retry_index": payload.and_then(|value| value.get("attempt")),
            "parent_call_id": previous_by_attempt.get(&key),
            "status": status,
            "terminal_event": event.event.event_type,
            "domain_request": request,
            "response": response,
            "telemetry": telemetry,
            "error_class": payload.and_then(|value| value.get("error_class")),
            "will_retry": payload.and_then(|value| value.get("will_retry")),
            "provider_visibility": if raw_visible { "captured_and_redacted" } else if raw_allowed { "not_returned_or_not_persisted" } else { "redacted_by_authorization" },
            "artifact_refs": [artifact_id],
            "source_location": "persisted AgentTurn artifact; source location not recorded"
        });
        if raw_visible {
            if let Some(provider) = provider {
                record["provider_request"] = provider
                    .get("request")
                    .cloned()
                    .unwrap_or_else(|| json!({"not_returned":true}));
                record["provider_result"] = provider
                    .get("result")
                    .cloned()
                    .unwrap_or_else(|| json!({"not_returned":true}));
            }
        } else {
            record["provider_request"] = json!({"not_returned":true});
            record["provider_result"] = json!({"not_returned":true});
            if !raw_allowed || provider.is_none() {
                problems.uncaptured_payloads += 1;
            }
        }
        previous_by_attempt.insert(
            key,
            record["call_id"].as_str().unwrap_or_default().to_owned(),
        );
        calls.push(record);
    }
    for unmatched in pairing.unmatched_starts() {
        let Some(start) = events
            .iter()
            .find(|event| event.event.cursor == unmatched.cursor)
        else {
            continue;
        };
        problems.unknown_after_crash_calls += 1;
        problems.uncaptured_payloads += 1;
        calls.push(json!({
            "schema_version": DOMAIN_SCHEMA_VERSION,
            "event_cursor": start.event.cursor,
            "observed_at_utc": start.event.created_at,
            "run_id": start.event.run_id,
            "task_id": start.event.task_id,
            "attempt_id": start.event.attempt_id,
            "role": start.role,
            "horizon": start.horizon,
            "turn_id": null,
            "call_id": null,
            "phase": null,
            "status": "unknown_after_crash",
            "lifecycle": "dispatch_started_without_terminal",
            "domain_request": {"not_returned":true,"reason":"no_preflight_artifact_before_process_boundary"},
            "provider_request": {"not_returned":true},
            "provider_result": {"not_returned":true},
            "telemetry": {"input_tokens":null,"output_tokens":null,"reasoning_tokens":null,"unknown_reason":"terminal_not_recorded"},
            "parent_call_id": null,
            "source_location": "AgentTurnStarted event without terminal artifact; do not infer failure"
        }));
    }
    calls.sort_by_key(|call| {
        call.get("event_cursor")
            .and_then(|v| v.as_i64())
            .unwrap_or(i64::MAX)
    });
    let _ = task_index;
    calls
}

fn build_tool_records(
    events: &[BundleEvent],
    artifacts: &[BundleArtifact],
    _task_index: &serde_json::Value,
) -> Vec<serde_json::Value> {
    events
        .iter()
        .filter(|event| matches!(event.event.event_type.as_str(), "tool.called" | "tool.completed" | "tool.failed"))
        .map(|event| {
            let payload = event.event.artifact_id.as_ref().and_then(|id| artifact_payload(artifacts, id));
            let call_id = payload.and_then(|value| value.get("call_id")).cloned().or_else(|| payload.and_then(|value| value.pointer("/call/call_id")).cloned());
            let name = payload.and_then(|value| value.get("name")).cloned().or_else(|| payload.and_then(|value| value.pointer("/call/name")).cloned());
            let bytes = payload.map(|value| serde_json::to_vec(value).map(|bytes| bytes.len()).unwrap_or_default());
            json!({
                "schema_version": DOMAIN_SCHEMA_VERSION,
                "event_id": event.event.cursor,
                "observed_at_utc": event.event.created_at,
                "run_id": event.event.run_id,
                "task_id": event.event.task_id,
                "attempt_id": event.event.attempt_id,
                "role": event.role,
                "horizon": event.horizon,
                "phase": event.phase,
                "turn_id": event.turn,
                "call_id": call_id,
                "name": name,
                "lifecycle": event.event.event_type,
                "model_requested": event.event.event_type == "tool.called",
                "rust_executed": matches!(event.event.event_type.as_str(), "tool.completed" | "tool.failed"),
                "permission_check": "Rust ContextGrant/tool source checks are authoritative; detailed check is not separately persisted",
                "result_bytes": bytes,
                "artifact_refs": event.event.artifact_id,
                "source_location": "persisted ToolCall/ToolResult artifact"
            })
        })
        .collect()
}

fn build_rust_decisions(
    events: &[BundleEvent],
    artifacts: &[BundleArtifact],
    _task_index: &serde_json::Value,
) -> Vec<serde_json::Value> {
    let decision_kinds = [
        "decision_context",
        "decision",
        "execution_context",
        "execution_verdict",
        "execution_plan",
        "execution_commitment",
        "outcome_schedule",
        "outcome",
        "retrospective",
        "evidence_need",
        "normalized_evidence",
        "context_manifest",
        "debug_record",
    ];
    events
        .iter()
        .filter_map(|event| {
            let id = event.event.artifact_id.as_ref()?;
            let artifact = artifacts.iter().find(|artifact| &artifact.artifact.artifact_id == id)?;
            let kind = serde_json::to_value(artifact.artifact.kind).ok()?.as_str()?.to_owned();
            if !decision_kinds.contains(&kind.as_str()) {
                return None;
            }
            let payload = artifact.payload.clone();
            let authoritative = payload.is_some();
            Some(json!({
                "schema_version": DOMAIN_SCHEMA_VERSION,
                "event_id": event.event.cursor,
                "observed_at_utc": event.event.created_at,
                "run_id": event.event.run_id,
                "task_id": event.event.task_id,
                "attempt_id": event.event.attempt_id,
                "role": event.role,
                "horizon": event.horizon,
                "artifact_id": id,
                "artifact_kind": kind,
                "trace_kind": if authoritative { "authoritative_persisted_payload" } else { "missing_payload" },
                "rule_id": payload.as_ref().and_then(|value| value.get("rule_id")),
                "rule_version": payload.as_ref().and_then(|value| value.get("rule_version")),
                "inputs": payload.as_ref().and_then(|value| value.get("inputs")).cloned().unwrap_or_else(|| json!({"not_returned":true})),
                "result": payload.as_ref().and_then(|value| value.get("result")).cloned().unwrap_or_else(|| json!({"persisted_payload":payload})),
                "first_zeroing_branch": payload.as_ref().and_then(|value| value.get("first_zeroing_branch")).cloned().unwrap_or_else(|| json!("not_recorded")),
                "short_circuited": payload.as_ref().and_then(|value| value.get("short_circuited")).cloned().unwrap_or(serde_json::Value::Null),
                "reconstruction": if authoritative { serde_json::Value::Null } else { json!({"method":"bundle_projection_v1","not_authoritative":true}) },
                "payload": payload,
                "artifact_refs": artifact.artifact.source_refs,
                "source_location": "persisted Rust artifact/event; source location not recorded"
            }))
        })
        .collect()
}

fn build_decision_matrix(
    artifacts: &[BundleArtifact],
    _task_index: &serde_json::Value,
) -> serde_json::Value {
    let mut forecasts = BTreeMap::<String, serde_json::Value>::new();
    let mut contexts = Vec::new();
    let mut claims = Vec::new();
    let mut critiques = Vec::new();
    let mut research_plans = Vec::new();
    for artifact in artifacts {
        let Some(payload) = &artifact.payload else {
            continue;
        };
        match artifact.artifact.kind {
            ArtifactKind::DecisionProposal | ArtifactKind::Decision => {
                if let Some(value) = payload
                    .get("research_allocation")
                    .or_else(|| payload.get("research_plan"))
                {
                    research_plans.push(json!({
                        "artifact_id": artifact.artifact.artifact_id,
                        "kind": artifact.artifact.kind,
                        "payload": value,
                    }));
                }
                if let Some(values) = payload.get("forecasts").and_then(|v| v.as_array()) {
                    for forecast in values {
                        let asset = forecast
                            .get("asset")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown");
                        let horizon = forecast
                            .get("horizon")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown");
                        forecasts.insert(format!("{asset}/{horizon}"), json!({"raw_forecast":forecast,"source_artifact":artifact.artifact.artifact_id}));
                    }
                }
            }
            ArtifactKind::DecisionContext => {
                contexts.push(json!({
                    "artifact_id": artifact.artifact.artifact_id,
                    "payload": payload
                }));
                if let Some(value) = payload.get("research_plan") {
                    research_plans.push(json!({
                        "artifact_id": artifact.artifact.artifact_id,
                        "kind": artifact.artifact.kind,
                        "payload": value,
                    }));
                }
            }
            ArtifactKind::Claim => {
                claims.push(json!({"artifact_id":artifact.artifact.artifact_id,"payload":payload}))
            }
            ArtifactKind::Critique => critiques
                .push(json!({"artifact_id":artifact.artifact.artifact_id,"payload":payload})),
            _ => {}
        }
    }
    let mut slots = Vec::new();
    for asset in ["TQQQ", "QQQ", "SOXX", "SOXL"] {
        for horizon in ["t1", "t3", "t5"] {
            let key = format!("{asset}/{horizon}");
            let forecast = forecasts
                .remove(&key)
                .unwrap_or_else(|| json!({"raw_forecast":null,"not_returned":true}));
            slots.push(json!({"asset":asset,"horizon":horizon,"forecast":forecast,"decision_context_refs":contexts.iter().map(|value| value.get("artifact_id")).collect::<Vec<_>>(),"claim_refs":claims.iter().map(|value| value.get("artifact_id")).collect::<Vec<_>>(),"critique_refs":critiques.iter().map(|value| value.get("artifact_id")).collect::<Vec<_>>()}));
        }
    }
    json!({"schema_version":DOMAIN_SCHEMA_VERSION,"units":{"probability":"ppm","expected_return":"ppm","weights":"ppm","confidence":"ppm"},"slots":slots,"contexts":contexts,"claims":claims,"critiques":critiques,"research_plans":research_plans,"unmatched_forecasts":forecasts})
}

fn build_policies_and_risk(
    artifacts: &[BundleArtifact],
    debug_session: &Option<serde_json::Value>,
    run: &serde_json::Value,
) -> serde_json::Value {
    let mut values = Vec::new();
    for artifact in artifacts {
        if matches!(
            artifact.artifact.kind,
            ArtifactKind::DecisionContext
                | ArtifactKind::ExecutionContext
                | ArtifactKind::ExecutionVerdict
                | ArtifactKind::ExecutionPlan
                | ArtifactKind::RuntimeManifest
                | ArtifactKind::DebugRecord
        ) {
            if let Some(payload) = &artifact.payload {
                values.push(json!({"artifact_id":artifact.artifact.artifact_id,"kind":artifact.artifact.kind,"payload":payload}));
            }
        }
    }
    json!({"schema_version":DOMAIN_SCHEMA_VERSION,"run":run,"debug_session":debug_session,"policy_and_risk_artifacts":values,"unknown_fields_are_null":true})
}

// Keep content structured so the same recursive redactor covers JSON and
// NDJSON provider envelopes. Text is an explicit derived representation, not
// an assertion that exported bytes have the original CAS hash.
fn decode_bundle_payload(media_type: &str, bytes: &[u8]) -> Option<serde_json::Value> {
    if let Ok(value) = serde_json::from_slice(bytes) {
        return Some(value);
    }
    let media_type = media_type.split(';').next()?.trim();
    let text = std::str::from_utf8(bytes).ok()?;
    if matches!(media_type, "application/x-ndjson" | "application/ndjson") {
        let records = text
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(serde_json::from_str::<serde_json::Value>)
            .collect::<Result<Vec<_>, _>>()
            .ok()?;
        return (!records.is_empty())
            .then(|| json!({"export_encoding":"ndjson", "records":records}));
    }
    (media_type.starts_with("text/")
        || matches!(media_type, "application/xml" | "application/xhtml+xml"))
    .then(|| json!({"export_encoding":"utf8", "media_type":media_type, "text":text}))
}

fn observed_acquisition_calls(artifacts: &[BundleArtifact]) -> Vec<serde_json::Value> {
    let mut seen = BTreeSet::new();
    let mut calls = Vec::new();
    for artifact in artifacts {
        if !matches!(
            artifact.artifact.kind,
            ArtifactKind::RawEvidence | ArtifactKind::NormalizedEvidence
        ) {
            continue;
        }
        let Some(payload) = &artifact.payload else {
            continue;
        };
        let records = if payload["export_encoding"] == "ndjson" {
            payload["records"]
                .as_array()
                .map(Vec::as_slice)
                .unwrap_or_default()
        } else {
            std::slice::from_ref(payload)
        };
        for record in records {
            for (response_path, request_path, role) in [
                (
                    "/provider_result",
                    "/provider_request",
                    "evidence.discovery",
                ),
                (
                    "/value/provider_result",
                    "/value/provider_request",
                    "evidence.discovery",
                ),
                (
                    "/audit/response",
                    "/audit/request",
                    "evidence.source_review",
                ),
            ] {
                let Some(response) = record.pointer(response_path) else {
                    continue;
                };
                let Some(id) = response["id"].as_str() else {
                    continue;
                };
                if !seen.insert(id.to_owned()) {
                    continue;
                }
                calls.push(json!({"role":role,"scope":"acquisition","source_artifact_id":artifact.artifact.artifact_id,
                    "response_id":id,"provider_request":record.pointer(request_path),
                    "telemetry":{"actual_model":response["model"],"input_tokens":response.pointer("/usage/input_tokens"),
                    "output_tokens":response.pointer("/usage/output_tokens"),"cached_input_tokens":response.pointer("/usage/input_tokens_details/cached_tokens")},
                    "status":response["status"]}));
            }
        }
    }
    calls
}

fn source_review_failures(artifacts: &[BundleArtifact]) -> serde_json::Value {
    let rows = artifacts.iter().filter(|a| a.artifact.kind == ArtifactKind::NormalizedEvidence)
        .filter_map(|artifact| {
            let review = artifact.payload.as_ref()?.pointer("/value/source_review")?;
            let has_error = review.get("error").is_some_and(|v| !v.is_null());
            let has_failures = review["validation_failures"].as_array().is_some_and(|v| !v.is_empty());
            (has_error || has_failures).then(|| json!({"artifact_id":artifact.artifact.artifact_id,
                "error":review["error"],"validation_failures":review["validation_failures"],"status":review["status"]}))
        }).collect::<Vec<_>>();
    json!(rows)
}

fn observed_web_calls(artifacts: &[BundleArtifact]) -> Vec<serde_json::Value> {
    let mut seen = BTreeSet::new();
    let mut calls = Vec::new();
    for artifact in artifacts {
        if !matches!(
            artifact.artifact.kind,
            ArtifactKind::RawEvidence | ArtifactKind::NormalizedEvidence
        ) {
            continue;
        }
        let Some(payload) = &artifact.payload else {
            continue;
        };
        let records = if payload["export_encoding"] == "ndjson" {
            payload["records"]
                .as_array()
                .map(Vec::as_slice)
                .unwrap_or_default()
        } else {
            std::slice::from_ref(payload)
        };
        for record in records {
            // Only pipeline-owned provider envelopes, never arbitrary nested
            // article text or model assertions, establish search execution.
            for response in [
                record.get("provider_result"),
                record.pointer("/value/provider_result"),
                record.pointer("/audit/response"),
            ]
            .into_iter()
            .flatten()
            {
                for item in response
                    .get("output")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    if item["type"] != "web_search_call" {
                        continue;
                    }
                    let identity = json!({"response_id":response["id"],"call":item});
                    let hash = ContentHash::of_bytes(identity.to_string().as_bytes());
                    if seen.insert(hash) {
                        calls.push(json!({"source_artifact_id":artifact.artifact.artifact_id,
                            "provider_response_id":response["id"],"call_id":item["id"],
                            "status":item["status"],"action":item["action"],
                            "has_action_sources":item.pointer("/action/sources").and_then(serde_json::Value::as_array).is_some_and(|sources| !sources.is_empty())}));
                    }
                }
            }
        }
    }
    calls
}

fn build_evidence_status(
    artifacts: &[BundleArtifact],
    _events: &[BundleEvent],
) -> serde_json::Value {
    let mut rows = Vec::new();
    for artifact in artifacts {
        if matches!(
            artifact.artifact.kind,
            ArtifactKind::EvidenceNeed
                | ArtifactKind::NormalizedEvidence
                | ArtifactKind::RawEvidence
                | ArtifactKind::SemanticDetail
        ) {
            rows.push(json!({"artifact_id":artifact.artifact.artifact_id,"kind":artifact.artifact.kind,"producer":artifact.artifact.producer,"payload":artifact.payload,"payload_status":if artifact.payload.is_some(){"captured"}else{"missing_or_omitted"},"source_refs":artifact.artifact.source_refs}));
        }
    }
    let calls = observed_web_calls(artifacts);
    let evidenced_calls = calls
        .iter()
        .filter(|call| call["has_action_sources"] == true)
        .count();
    let mut action_counts = BTreeMap::<String, usize>::new();
    for call in &calls {
        let action = call
            .pointer("/action/type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        *action_counts.entry(action.to_owned()).or_default() += 1;
    }
    let acquisition_calls = observed_acquisition_calls(artifacts);
    json!({"schema_version":DOMAIN_SCHEMA_VERSION,"records":rows,
        "web_search_calls": if calls.is_empty() { serde_json::Value::Null } else { json!(calls.len()) },
        "web_search_calls_scope":"all observed hosted web actions, including search/open/find; not source verification",
        "acquisition_model_call_count":acquisition_calls.len(),
        "acquisition_model_calls":acquisition_calls,
        "acquisition_scope":"deduplicated persisted discovery and source-review responses; separate from research llm_calls.jsonl",
        "source_review_failures":source_review_failures(artifacts),
        "web_search_audit":{"status":if calls.is_empty(){"not_observed_in_export"}else{"observed"},
            "observed_call_count":calls.len(),"action_counts":action_counts,
            "calls_with_source_metadata":evidenced_calls,"calls":calls,
            "scope":"persisted provider calls only; missing audit is not proof of no search; search is not source verification"},
        "hosted_web_search_evidence_is_only_claimed_when_provider_payload_contains_web_search_call_action_sources":true})
}

fn build_context_coverage(artifacts: &[BundleArtifact]) -> serde_json::Value {
    let manifests = artifacts
        .iter()
        .filter(|artifact| artifact.artifact.kind == ArtifactKind::ContextManifest)
        .filter_map(|artifact| {
            let payload = artifact.payload.as_ref()?;
            let selections = payload
                .get("selections")
                .cloned()
                .unwrap_or_else(|| json!([]));
            let selected_ids = selections.as_array().into_iter().flatten()
                .filter_map(|s| s.pointer("/artifact/artifact_id").and_then(serde_json::Value::as_str)).collect::<BTreeSet<_>>();
            let unselected = artifacts.iter().filter(|a| matches!(a.artifact.kind, ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail))
                .filter(|a| !selected_ids.contains(a.artifact.artifact_id.0.as_str()))
                .map(|a| json!({"artifact_id":a.artifact.artifact_id,"kind":a.artifact.kind,
                    "resource":a.payload.as_ref().and_then(|p| p.get("resource")),
                    "status":"exported_presence_only; availability_at_manifest_unknown"})).collect::<Vec<_>>();
            Some(json!({
                "manifest_artifact_id": artifact.artifact.artifact_id,
                "assembly_coverage": artifacts.iter().find(|a| a.artifact.producer == "context.coverage"
                    && a.payload.as_ref().is_some_and(|p| p["manifest"] == serde_json::json!(artifact.artifact.artifact_id))).and_then(|a| a.payload.as_ref()),
                "unselected_exported_evidence": unselected,
                "input_hash": payload.get("input_hash"),
                "source_bytes": payload.get("total_bytes"),
                "projected_bytes": payload.get("projected_bytes"),
                "estimated_tokens": payload.get("estimated_tokens"),
                "selection_count": selections.as_array().map_or(0, Vec::len),
                "selections": selections,
                "status": "selected_and_persisted"
            }))
        })
        .collect::<Vec<_>>();
    json!({
        "schema_version": DOMAIN_SCHEMA_VERSION,
        "records": manifests,
        "state_machine": [
            "requested", "collected", "normalized", "eligible_at_cutoff",
            "selected", "projected", "delivered_or_readable", "actually_read_if_tool_used",
            "cited", "validated"
        ],
        "unselected_collection_status_is_not_global_absence": true
    })
}

fn build_draft_submit_coverage(calls: &[serde_json::Value]) -> serde_json::Value {
    let records = calls
        .iter()
        .map(|call| {
            json!({
                "run_id": call.get("run_id"),
                "task_id": call.get("task_id"),
                "attempt_id": call.get("attempt_id"),
                "role": call.get("role"),
                "horizon": call.get("horizon"),
                "phase": call.get("phase"),
                "turn_id": call.get("turn_id"),
                "retry_index": call.get("retry_index"),
                "call_id": call.get("call_id"),
                "status": call.get("status"),
                "terminal_event": call.get("terminal_event"),
                "provider_visibility": call.get("provider_visibility"),
                "response_id": call.pointer("/telemetry/response_id"),
                "actual_model": call.pointer("/telemetry/actual_model"),
                "reported_usage": {
                    "input_tokens": call.pointer("/telemetry/input_tokens"),
                    "cached_input_tokens": call.pointer("/telemetry/cached_input_tokens"),
                    "output_tokens": call.pointer("/telemetry/output_tokens"),
                    "reasoning_tokens": call.pointer("/telemetry/reasoning_tokens")
                },
                "schema_rejection_or_retry": call.get("error_class").or_else(|| call.get("will_retry"))
            })
        })
        .collect::<Vec<_>>();
    json!({"schema_version": DOMAIN_SCHEMA_VERSION, "records": records})
}

fn build_stage_acceptance(
    artifacts: &[BundleArtifact],
    tasks: &[serde_json::Value],
) -> serde_json::Value {
    let records = artifacts
        .iter()
        .filter(|artifact| artifact.artifact.producer == "debug.stage_acceptance")
        .map(|artifact| {
            json!({
                "artifact_id": artifact.artifact.artifact_id,
                "payload": artifact.payload,
                "payload_status": if artifact.payload.is_some() { "captured" } else { "missing_or_omitted" }
            })
        })
        .collect::<Vec<_>>();
    let mut counts = BTreeMap::<String, usize>::new();
    let mut workflow_status_counts = BTreeMap::<String, usize>::new();
    let tasks = tasks.iter().map(|task| {
        *workflow_status_counts.entry(task["status"].as_str().unwrap_or("unknown").to_owned()).or_default() += 1;
        let task_id = &task["node"]["task_id"];
        let acceptance = records.iter().rev().find(|record| record["payload"]["task_id"] == *task_id);
        let status = if matches!(task["status"].as_str(),Some("queued" | "ready" | "pending" | "skipped" | "cancelled")) {
            "NOT_REACHED"
        } else {
            acceptance.and_then(|record| record["payload"]["test_result"].as_str()).unwrap_or("NOT_RUN")
        };
        *counts.entry(status.to_owned()).or_default() += 1;
        json!({"task_id":task_id,"role":task["node"]["recipe_id"],"workflow_status":task["status"],"acceptance_status":status})
    }).collect::<Vec<_>>();
    json!({"schema_version": DOMAIN_SCHEMA_VERSION, "records": records, "tasks":tasks,"counts":counts,"workflow_status_counts":workflow_status_counts,"not_run_is_not_pass": true})
}

fn build_failures_and_missing(
    events: &[BundleEvent],
    calls: &[serde_json::Value],
    tools: &[serde_json::Value],
    problems: &BundleProblems,
    access: &DebugBundleRawAccess,
) -> serde_json::Value {
    let failures = calls
        .iter()
        .filter(|call| call.get("status").and_then(|v| v.as_str()) == Some("failed"))
        .cloned()
        .collect::<Vec<_>>();
    let unknown = calls
        .iter()
        .filter(|call| call.get("status").and_then(|v| v.as_str()) == Some("unknown_after_crash"))
        .cloned()
        .collect::<Vec<_>>();
    let cancelled = events.iter().filter(|event| event.event.event_type.contains("cancel")).map(|event| json!({"event_id":event.event.cursor,"event_type":event.event.event_type,"task_id":event.event.task_id,"attempt_id":event.event.attempt_id})).collect::<Vec<_>>();
    json!({
        "schema_version":DOMAIN_SCHEMA_VERSION,
        "model_failures":failures,
        "unknown_after_crash":unknown,
        "cancel_events":cancelled,
        "tool_failures":tools.iter().filter(|tool| tool.get("lifecycle").and_then(|v| v.as_str())==Some("tool.failed")).cloned().collect::<Vec<_>>(),
        "missing_or_corrupt":problems.missing,
        "raw_access":access,
        "counts":{"event_count":events.len(),"agent_failure_count":failures.len(),"unknown_call_count":unknown.len(),"tool_count":tools.len(),"missing_count":problems.missing.len()},
        "not_returned_is_not_zero":true
    })
}

fn build_model_routes(calls: &[serde_json::Value]) -> serde_json::Value {
    let mut routes = BTreeMap::<String, serde_json::Value>::new();
    for call in calls {
        let role = call
            .get("role")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let entry = routes.entry(role.to_owned()).or_insert_with(|| json!({"role":role,"requested_models":[],"actual_models":[],"reasoning_efforts":[],"calls":0,"unknown_usage":0}));
        entry["calls"] = json!(entry["calls"]
            .as_u64()
            .unwrap_or_default()
            .saturating_add(1));
        for (field, list) in [
            ("requested_model", "requested_models"),
            ("actual_model", "actual_models"),
            ("reasoning_effort", "reasoning_efforts"),
        ] {
            if let Some(value) = call
                .pointer(&format!("/telemetry/{field}"))
                .filter(|v| v.is_string())
                .or_else(|| match field {
                    "reasoning_effort" => call.pointer("/provider_request/reasoning/effort"),
                    "requested_model" => call.pointer("/provider_request/model"),
                    _ => None,
                })
                .and_then(|v| v.as_str())
            {
                let values = entry[list].as_array_mut().expect("route list");
                if !values
                    .iter()
                    .any(|existing| existing.as_str() == Some(value))
                {
                    values.push(json!(value));
                }
            }
        }
        if call
            .pointer("/telemetry/input_tokens")
            .and_then(serde_json::Value::as_u64)
            .is_none()
            || call
                .pointer("/telemetry/output_tokens")
                .and_then(serde_json::Value::as_u64)
                .is_none()
        {
            entry["unknown_usage"] = json!(entry["unknown_usage"]
                .as_u64()
                .unwrap_or_default()
                .saturating_add(1));
        }
    }
    json!({"schema_version":DOMAIN_SCHEMA_VERSION,"routes":routes.values().collect::<Vec<_>>()})
}

fn render_transcript(calls: &[serde_json::Value], access: &DebugBundleRawAccess) -> String {
    let mut output = String::from("# LLM transcript\n\n");
    output.push_str("This document contains persisted provider-visible material only. It does not contain hidden chain-of-thought. Opaque encrypted continuation is redacted.\n\n");
    let mut sorted = calls.to_vec();
    sorted.sort_by_key(|value| {
        (
            value
                .get("role")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_owned(),
            value
                .get("horizon")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_owned(),
            value
                .get("attempt_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_owned(),
            value
                .get("event_cursor")
                .and_then(|v| v.as_i64())
                .unwrap_or(i64::MAX),
        )
    });
    for call in sorted {
        output.push_str(&format!(
            "## {} / {} / Attempt {} / {} / turn {}\n\n",
            call.get("role")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown role"),
            call.get("horizon")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown horizon"),
            call.get("attempt_id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown"),
            call.get("phase")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown phase"),
            call.get("turn_id")
                .and_then(|v| v.as_u64())
                .map(|v| v.to_string())
                .unwrap_or_else(|| "unknown".to_owned())
        ));
        output.push_str(&format!(
            "- status: `{}`; cursor: `{}`; call_id: `{}`\n",
            call.get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown"),
            call.get("event_cursor")
                .and_then(|v| v.as_i64())
                .map(|v| v.to_string())
                .unwrap_or_else(|| "unknown".to_owned()),
            call.get("call_id")
                .and_then(|v| v.as_str())
                .unwrap_or("not_returned")
        ));
        output.push_str(&format!(
            "- model: requested=`{}` actual=`{}` effort=`{}`\n",
            call.pointer("/telemetry/requested_model")
                .and_then(|v| v.as_str())
                .unwrap_or("not_returned"),
            call.pointer("/telemetry/actual_model")
                .and_then(|v| v.as_str())
                .unwrap_or("not_returned"),
            call.pointer("/telemetry/reasoning_effort")
                .and_then(|v| v.as_str())
                .unwrap_or("not_returned")
        ));
        for (label, key) in [
            ("domain request", "domain_request"),
            ("provider request", "provider_request"),
            ("response", "response"),
            ("provider result", "provider_result"),
        ] {
            let value = call
                .get(key)
                .cloned()
                .unwrap_or_else(|| json!({"not_returned":true}));
            output.push_str(&format!("\n### {label}\n\n"));
            output.push_str(&markdown_fence(
                &serde_json::to_string_pretty(&value)
                    .unwrap_or_else(|_| "{\"not_returned\":true}".to_owned()),
            ));
            output.push('\n');
        }
    }
    if !access.allowed {
        output.push_str("\n## Provider detail permission\n\nProvider request/result bodies are not included because this run lacks a matching isolated DebugSession. The omission is an authorization boundary, not a claim that the provider was not called.\n");
    }
    output
}

fn render_rust_decisions(records: &[serde_json::Value]) -> String {
    let mut output = String::from("# Rust decision trace\n\n");
    output.push_str("Records are ordered by persisted event cursor. Fields absent from the runtime payload are `not_recorded`; this exporter does not rerun rules or infer a first blocker.\n\n");
    for record in records {
        output.push_str(&format!(
            "## cursor {} · {} · {}\n\n",
            record
                .get("event_id")
                .and_then(|v| v.as_i64())
                .map(|v| v.to_string())
                .unwrap_or_else(|| "unknown".to_owned()),
            record
                .get("artifact_kind")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown"),
            record
                .get("artifact_id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
        ));
        output.push_str(&format!(
            "- trace: `{}`\n- rule_id: `{}`\n- first zeroing branch: `{}`\n- source: `{}`\n\n",
            record
                .get("trace_kind")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown"),
            record
                .get("rule_id")
                .and_then(|v| v.as_str())
                .unwrap_or("not_recorded"),
            record
                .get("first_zeroing_branch")
                .and_then(|v| v.as_str())
                .unwrap_or("not_recorded"),
            record
                .get("source_location")
                .and_then(|v| v.as_str())
                .unwrap_or("not_recorded")
        ));
        output.push_str(&markdown_fence(
            &serde_json::to_string_pretty(record).unwrap_or_else(|_| "{}".to_owned()),
        ));
        output.push('\n');
    }
    output
}

fn render_summary(
    run: &serde_json::Value,
    workflow: &serde_json::Value,
    calls: &[serde_json::Value],
    decisions: &[serde_json::Value],
    matrix: &serde_json::Value,
    failures: &serde_json::Value,
    cursor: i64,
) -> String {
    let status = workflow
        .pointer("/status")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| {
            run.get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
        });
    let zero_targets = matrix
        .get("slots")
        .and_then(|v| v.as_array())
        .map(|slots| {
            slots
                .iter()
                .filter(|slot| {
                    slot.pointer("/forecast/raw_forecast/expected_return_ppm")
                        .and_then(|v| v.as_i64())
                        == Some(0)
                })
                .count()
        })
        .unwrap_or(0);
    format!(
        "# SUMMARY\n\n- Run: `{}`; purpose: `{}`; persisted workflow status: `{}`.\n- Snapshot cursor: `{}`; this bundle is a read-only projection and does not choose a latest Run implicitly.\n- Agent calls recorded: `{}`; Rust decision records: `{}`; failure/unknown records: `{}`.\n- Decision matrix slots with an observed zero expected return: `{}/12`; this is a recorded forecast/decision value, not an inference that zero means safe or that a nonzero position was required.\n- Evidence and web-search status must be read from `evidence_status.json`; a citation without the persisted hosted `web_search_call.action.sources` shape is not upgraded to verified evidence.\n- Missing fields are explicitly `not_returned`, `not_recorded`, `not_authorized`, or `unknown_after_crash`; no model was called and no decision was rerun during export.\n\n## How to continue\n\nUse `timeline.jsonl` cursor order, then join `llm_calls.jsonl` by `call_id`/artifact refs, `tools.jsonl` by `call_id`, and `rust_decisions.jsonl` by event/artifact refs. `failures_and_missing.json` is the authoritative boundary list for this package.\n\n## Direct references\n\n- Run record: `{}`\n- First persisted call: `{}`\n- First Rust record: `{}`\n- Failure summary: `failures_and_missing.json`\n",
        run.get("run_id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown"),
        run.get("purpose")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown"),
        status,
        cursor,
        calls.len(),
        decisions.len(),
        failures
            .get("counts")
            .and_then(|v| v.get("agent_failure_count"))
            .and_then(|v| v.as_u64())
            .unwrap_or_default()
            .saturating_add(
                failures
                    .get("counts")
                    .and_then(|v| v.get("unknown_call_count"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or_default()
            ),
        zero_targets,
        run.get("run_id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown"),
        calls
            .first()
            .and_then(|v| v.get("call_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("not_returned"),
        decisions
            .first()
            .and_then(|v| v.get("artifact_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("not_returned")
    )
}

fn render_readme(access: &DebugBundleRawAccess) -> String {
    format!(
        "# Akzio Debug Bundle\n\nThis directory is a share-safe, read-only projection of one persisted Run. It was generated without calling a model, fetching evidence, placing an order, repairing Store data, or rerunning a decision.\n\n- Exporter: `{EXPORTER_VERSION}`\n- Provider request/result detail: `{}` (`{}`)\n- `source_artifact_hash` identifies the CAS object; `export_payload_hash` identifies the redacted exported payload. They are intentionally different when redaction occurred.\n- `not_returned`, `not_recorded`, `not_authorized`, and `unknown_after_crash` are evidence boundaries, not inferred values.\n- `checksums.sha256` covers every regular file except itself. No symlinks are permitted.\n\nJoin order: `timeline.jsonl` cursor → `llm_calls.jsonl` call/artifact refs → `tools.jsonl` call_id → `rust_decisions.jsonl` artifact/event refs. `SUMMARY.md` is the short human-readable orientation; the JSONL files are the machine-readable facts.\n",
        if access.allowed {
            "captured_and_redacted"
        } else {
            "not_returned"
        },
        access.reason
    )
}

fn parent_call_id_for_event(event: &BundleEvent, calls: &[serde_json::Value]) -> Option<String> {
    let current = event.call_id.as_deref()?;
    calls
        .iter()
        .filter(|call| {
            call.get("task_id") == Some(&json!(event.event.task_id))
                && call.get("attempt_id") == Some(&json!(event.event.attempt_id))
        })
        .filter_map(|call| call.get("call_id").and_then(|v| v.as_str()))
        .take_while(|id| *id != current)
        .last()
        .map(str::to_owned)
}

fn role_name(recipe: &str) -> String {
    recipe.to_owned()
}

fn redact_records(
    records: &[serde_json::Value],
    redactions: &mut Vec<String>,
) -> Vec<serde_json::Value> {
    records
        .iter()
        .map(|record| redact_value(record, redactions))
        .collect()
}

fn redact_value(value: &serde_json::Value, redactions: &mut Vec<String>) -> serde_json::Value {
    let mut value = value.clone();
    redact_share_safe(&mut value, redactions);
    value
}

fn redact_share_safe(value: &mut serde_json::Value, redactions: &mut Vec<String>) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, value) in map.iter_mut() {
                let lower = key.to_ascii_lowercase();
                if is_sensitive_key(&lower) {
                    *value = redaction_marker(
                        value,
                        if lower.contains("encrypted") {
                            "opaque_continuation"
                        } else {
                            "sensitive_field"
                        },
                    );
                    redactions.push(format!("field:{key}"));
                } else {
                    redact_share_safe(value, redactions);
                }
            }
        }
        serde_json::Value::Array(values) => values
            .iter_mut()
            .for_each(|value| redact_share_safe(value, redactions)),
        serde_json::Value::String(text) if looks_sensitive(text) => {
            let marker = redaction_marker(value, "credential_or_token_bearing_text");
            *value = marker;
            redactions.push("string:credential_or_token_bearing_text".to_owned());
        }
        _ => {}
    }
}

fn is_sensitive_key(key: &str) -> bool {
    [
        "api_key",
        "apikey",
        "authorization",
        "auth_header",
        "secret",
        "password",
        "credential",
        "access_token",
        "refresh_token",
        "daemon_token",
        "cookie",
        "headers",
        "encrypted_content",
    ]
    .iter()
    .any(|part| {
        key == *part || key.ends_with(&format!("_{part}")) || key.contains(&format!("{part}_"))
    })
}

fn looks_sensitive(value: &str) -> bool {
    [
        "Bearer ",
        "sk-",
        "PK",
        "api_key=",
        "api_secret=",
        "access_token=",
        "token=",
        ".daemon-token",
        "daemon-token",
    ]
    .iter()
    .any(|needle| value.contains(needle))
}

fn redaction_marker(value: &serde_json::Value, reason: &str) -> serde_json::Value {
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    json!({"redacted":true,"reason":reason,"original_type":match value { serde_json::Value::Null=>"null",serde_json::Value::Bool(_)=>"bool",serde_json::Value::Number(_)=>"number",serde_json::Value::String(_)=>"string",serde_json::Value::Array(_)=>"array",serde_json::Value::Object(_)=>"object" },"original_bytes":bytes.len(),"security_fingerprint":ContentHash::of_bytes(&bytes)})
}

fn markdown_fence(text: &str) -> String {
    let mut longest = 0usize;
    let mut current = 0usize;
    for byte in text.bytes() {
        if byte == b'`' {
            current += 1;
            longest = longest.max(current);
        } else {
            current = 0;
        }
    }
    let fence = "`".repeat(longest.saturating_add(1).max(3));
    format!("{fence}json\n{text}\n{fence}\n")
}

fn write_json_file(
    target: &Path,
    name: &str,
    value: &serde_json::Value,
    file_hashes: &mut BTreeMap<String, ContentHash>,
) -> StoreResult<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    write_new_file(&target.join(name), &bytes)?;
    file_hashes.insert(name.to_owned(), ContentHash::of_bytes(&bytes));
    Ok(())
}

fn write_jsonl_file(
    target: &Path,
    name: &str,
    values: &[serde_json::Value],
    file_hashes: &mut BTreeMap<String, ContentHash>,
) -> StoreResult<()> {
    let mut bytes = Vec::new();
    for value in values {
        bytes.extend_from_slice(&serde_json::to_vec(value)?);
        bytes.push(b'\n');
    }
    write_new_file(&target.join(name), &bytes)?;
    file_hashes.insert(name.to_owned(), ContentHash::of_bytes(&bytes));
    Ok(())
}

fn write_text_file(
    target: &Path,
    name: &str,
    text: &str,
    file_hashes: &mut BTreeMap<String, ContentHash>,
) -> StoreResult<()> {
    let bytes = text.as_bytes();
    write_new_file(&target.join(name), bytes)?;
    file_hashes.insert(name.to_owned(), ContentHash::of_bytes(bytes));
    Ok(())
}

fn write_new_file(path: &Path, bytes: &[u8]) -> StoreResult<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| StoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    file.write_all(bytes).map_err(|source| StoreError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    file.sync_all().map_err(|source| StoreError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    secure_file(path)
}

fn collect_files(root: &Path) -> StoreResult<Vec<PathBuf>> {
    fn walk(current: &Path, output: &mut Vec<PathBuf>) -> StoreResult<()> {
        for entry in fs::read_dir(current).map_err(|source| StoreError::Io {
            path: current.to_path_buf(),
            source,
        })? {
            let entry = entry.map_err(|source| StoreError::Io {
                path: current.to_path_buf(),
                source,
            })?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).map_err(|source| StoreError::Io {
                path: path.clone(),
                source,
            })?;
            if metadata.file_type().is_symlink() {
                return Err(StoreError::Integrity(format!(
                    "bundle contains symlink {}",
                    path.display()
                )));
            }
            if metadata.is_dir() {
                walk(&path, output)?;
            } else if metadata.is_file() {
                output.push(path);
            }
        }
        Ok(())
    }
    let mut output = Vec::new();
    walk(root, &mut output)?;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn succeeded_workflow_without_acceptance_is_not_test_pass() {
        let tasks = vec![
            json!({"node":{"task_id":"a","recipe_id":"research.analyst"},"status":"succeeded"}),
            json!({"node":{"task_id":"b","recipe_id":"research.critic"},"status":"queued"}),
            json!({"node":{"task_id":"c","recipe_id":"research.synthesizer"},"status":"skipped"}),
            json!({"node":{"task_id":"d","recipe_id":"research.proposal_reviewer"},"status":"failed"}),
        ];
        let summary = build_stage_acceptance(&[], &tasks);
        assert_eq!(summary["counts"], json!({"NOT_RUN":2,"NOT_REACHED":2}));
        assert_eq!(summary["workflow_status_counts"]["failed"], 1);
        assert!(summary["counts"].get("PASS").is_none());
    }

    #[test]
    fn acquisition_audit_separates_calls_metadata_and_review_errors() {
        let now = Utc::now();
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/review-audit-tests")
            .join(RunId::new().0);
        let store = Store::open(root).unwrap();
        let raw = json!({"export_encoding":"ndjson","records":[
            {"provider_request":{"model":"news","reasoning":{"effort":"low"}},
             "provider_result":{"id":"discovery","model":"news","usage":{"input_tokens":10,"output_tokens":2},
                "output":[{"id":"web1","type":"web_search_call","action":{"type":"search","sources":[]}}]}},
            {"audit":{"request":{"model":"review","reasoning":{"effort":"high"}},
                "response":{"id":"review","model":"review","usage":{"input_tokens":20,"output_tokens":3},
                "output":[{"id":"web2","type":"web_search_call","action":{"type":"open_page","sources":[{"url":"https://example.com"}]}}]}}}
        ]});
        let make = |kind, payload: serde_json::Value| BundleArtifact {
            artifact: Artifact::new(
                kind,
                store.stage_json(&payload).unwrap(),
                "evidence.normalize",
                ArtifactLifecycle::RunScoped,
                ArtifactProvenance {
                    source_family: "news_web".into(),
                    observed_at: Some(now),
                    retrieved_at: now,
                    source_uri: None,
                    confidence_ppm: 1_000_000,
                    producer_contract_hash: None,
                },
                None,
                vec![],
                now,
            )
            .unwrap(),
            payload: Some(payload),
            omitted_reason: None,
        };
        let artifacts = vec![
            make(ArtifactKind::RawEvidence, raw.clone()),
            make(ArtifactKind::RawEvidence, raw),
            make(
                ArtifactKind::NormalizedEvidence,
                json!({"value":{"source_review":{"status":"model_reviewed", "error":"facts_outside_window",
                "validation_failures":[{"reason":"facts_outside_window"}]}}}),
            ),
        ];
        let status = build_evidence_status(&artifacts, &[]);
        assert_eq!(status["web_search_calls"], 2);
        assert_eq!(
            status["web_search_audit"]["action_counts"],
            json!({"search":1,"open_page":1})
        );
        assert_eq!(status["web_search_audit"]["calls_with_source_metadata"], 1);
        assert_eq!(status["acquisition_model_call_count"], 2);
        assert_eq!(
            status["source_review_failures"].as_array().unwrap().len(),
            1
        );
        let routes = build_model_routes(&observed_acquisition_calls(&artifacts));
        assert_eq!(routes["routes"][0]["reasoning_efforts"], json!(["low"]));
        assert_eq!(routes["routes"][1]["reasoning_efforts"], json!(["high"]));
        assert_eq!(routes["routes"][0]["unknown_usage"], 0);
    }

    #[test]
    fn text_and_ndjson_export_preserve_content_without_accepting_partial_json() {
        let payload = decode_bundle_payload("text/html; charset=utf-8", b"<p>source</p>").unwrap();
        assert_eq!(payload["text"], "<p>source</p>");
        let records =
            decode_bundle_payload("application/x-ndjson", b"{\"a\":1}\n{\"b\":2}\n").unwrap();
        assert_eq!(records["records"].as_array().unwrap().len(), 2);
        assert!(decode_bundle_payload("application/x-ndjson", b"{\"a\":1}\n{broken}").is_none());
        assert!(decode_bundle_payload("application/json", b"{\"a\":1}\n{broken}").is_none());
        assert!(decode_bundle_payload("application/octet-stream", &[0xff, 0]).is_none());
        let status = build_evidence_status(&[], &[]);
        assert!(status["web_search_calls"].is_null());
        assert_eq!(
            status["web_search_audit"]["status"],
            "not_observed_in_export"
        );
    }

    #[test]
    fn share_safe_redaction_keeps_length_and_fingerprint_without_secret() {
        let mut value = json!({"Authorization":"Bearer sk-secret", "nested":{"encrypted_content":"opaque"}, "input_tokens": 12});
        let mut redactions = Vec::new();
        redact_share_safe(&mut value, &mut redactions);
        assert!(value.to_string().contains("original_bytes"));
        assert!(value.to_string().contains("security_fingerprint"));
        assert!(!value.to_string().contains("sk-secret"));
        assert_eq!(value["input_tokens"], 12);
    }

    #[test]
    fn markdown_fence_grows_past_embedded_backticks() {
        let text = "json ``` inside";
        let rendered = markdown_fence(text);
        assert!(rendered.starts_with("````json"));
        assert!(rendered.ends_with("````\n"));
    }
}
