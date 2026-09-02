#[test]
fn child_projection_from_proof_rejects_stale_parent_and_trace_outputs() {
    let (_root, store, parent_permit, parent_contract, parent, raw, now) = manifest_fixture();
    let child_contract = contract(&store);
    let mut child_permit = parent_permit.clone();
    child_permit.task_id = akzio_domain::TaskId::new();
    child_permit.attempt_id = akzio_domain::AttemptId::new();
    child_permit.lease_id = akzio_domain::LeaseId::new();
    child_permit.contract_hash = Some(child_contract.contract_hash.clone());
    let broker = ContextBroker::new(store.clone());

    let call = task_artifact(
        &store,
        &parent_permit,
        ArtifactKind::ToolCall,
        vec![],
        "trace-call",
    );
    store
        .write_task_artifact(&parent_permit, &call, LifecycleEventType::ToolCalled, now)
        .unwrap();
    let trace = task_artifact(
        &store,
        &parent_permit,
        ArtifactKind::ToolResult,
        vec![ArtifactRef {
            artifact_id: call.artifact_id,
            kind: ArtifactKind::ToolCall,
        }],
        "trace",
    );
    store
        .write_task_artifact(
            &parent_permit,
            &trace,
            LifecycleEventType::ToolCompleted,
            now,
        )
        .unwrap();
    store
        .commit_attempt(
            &parent_permit,
            &[parent.artifact.clone(), trace.clone()],
            akzio_domain::TaskStatus::Succeeded,
            now,
        )
        .unwrap();

    let current = store
        .current_succeeded_attempt(&parent_permit.run_id, &parent_permit.task_id)
        .unwrap();

    let mut stale = current;
    stale.epoch = stale.epoch.saturating_add(1);
    assert!(matches!(
        broker.assemble_child_from_proof(
            &stale,
            &parent_contract,
            &child_permit,
            &child_contract,
            now,
            Duration::minutes(5),
        ),
        Err(ContextError::InvalidManifestClosure)
    ));

    let trace_projection = ContextProjection {
        parent_manifest: ArtifactRef {
            artifact_id: parent.artifact.artifact_id.clone(),
            kind: ArtifactKind::ContextManifest,
        },
        allowed: vec![ArtifactRef {
            artifact_id: trace.artifact_id.clone(),
            kind: trace.kind,
        }],
        reason: "trace-output".to_owned(),
    };
    assert!(matches!(
        broker.assemble_child(
            &parent_permit,
            &parent_contract,
            &parent,
            &trace_projection,
            &child_permit,
            &child_contract,
            now,
            Duration::minutes(5),
        ),
        Err(ContextError::GrantDenied { .. })
    ));

    let forged = task_artifact(
        &store,
        &parent_permit,
        ArtifactKind::NormalizedEvidence,
        vec![ArtifactRef {
            artifact_id: raw.artifact_id,
            kind: ArtifactKind::RawEvidence,
        }],
        "foreign output",
    );
    let projection = ContextProjection {
        parent_manifest: ArtifactRef {
            artifact_id: parent.artifact.artifact_id.clone(),
            kind: ArtifactKind::ContextManifest,
        },
        allowed: vec![ArtifactRef {
            artifact_id: forged.artifact_id,
            kind: forged.kind,
        }],
        reason: "foreign-output".to_owned(),
    };
    assert!(matches!(
        broker.assemble_child(
            &parent_permit,
            &parent_contract,
            &parent,
            &projection,
            &child_permit,
            &child_contract,
            now,
            Duration::minutes(5),
        ),
        Err(ContextError::GrantDenied { .. })
    ));
}
