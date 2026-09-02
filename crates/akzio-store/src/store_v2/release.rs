use super::*;

const SCHEDULER_LEASE_NAME: &str = "akzio.local.scheduler";
const CLEAN_WORKTREE_HASH: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

impl V2Store {
    /// Materialize a deterministic release evidence projection exclusively from
    /// canonical Store state. Expectations are comparison gates only; they never
    /// supply evidence fields to the bundle.
    pub fn release_evidence_bundle(
        &self,
        run_id: &RunId,
        expectations: &ReleaseEvidenceExpectations,
    ) -> StoreResult<ReleaseEvidenceBundle> {
        let workflow = self.workflow_snapshot(run_id)?;
        let session = self.session_slot_for_run(run_id)?;
        let approval = self.paper_approval_for_run(run_id)?;
        let lease = self.daemon_lease(SCHEDULER_LEASE_NAME)?;
        let trajectory = self.trajectory(run_id)?;
        let connection = self.connection()?;
        let context_artifacts = run_artifacts(&connection, run_id, ArtifactKind::ContextManifest)?;
        let source_artifacts = [
            ArtifactKind::RawEvidence,
            ArtifactKind::NormalizedEvidence,
            ArtifactKind::SemanticDetail,
        ]
        .into_iter()
        .map(|kind| canonical_run_artifacts(&connection, run_id, kind))
        .collect::<StoreResult<Vec<_>>>()?
        .into_iter()
        .flatten();
        let plans = canonical_run_artifacts(&connection, run_id, ArtifactKind::ExecutionPlan)?;
        let commitments =
            canonical_run_artifacts(&connection, run_id, ArtifactKind::ExecutionCommitment)?;
        let reconciliations =
            canonical_run_artifacts(&connection, run_id, ArtifactKind::Reconciliation)?;
        let learning = release_learning_evidence(&connection, run_id)?;
        drop(connection);

        let runtime = approval.as_ref().map(|(manifest, _)| {
            let (repository_commit, dirty_worktree) = repository_state(&manifest.code_revision);
            ReleaseRuntimeEvidence {
                repository_commit,
                dirty_worktree,
                config_hash: manifest.config_hash.clone(),
                prompt_hash: manifest.prompt_hash.clone(),
                contract_hash: manifest.contract_hash.clone(),
                topology_hash: manifest.topology_hash.clone(),
            }
        });
        let workflow_hash = workflow.run.graph_artifact_id.0.clone();
        let workflow_evidence = Some(ReleaseWorkflowEvidence {
            graph: ArtifactRef {
                artifact_id: workflow.run.graph_artifact_id.clone(),
                kind: ArtifactKind::WorkflowGraph,
            },
            workflow_hash: workflow_hash.clone(),
            plan: workflow.revision.graph.clone(),
        });

        let mut contracts = ReleaseContractEvidence::default();
        let mut provider_routes = BTreeSet::new();
        for entry in trajectory {
            let Some(model) = entry.model else {
                continue;
            };
            if let Some(contract_hash) = model.contract_hash {
                contracts.contract_hashes.insert(contract_hash);
            }
            if let Some(tool_set_hash) = model.tool_set_hash {
                contracts.tool_set_hashes.insert(tool_set_hash);
            }
            if let (
                Some(provider_id),
                Some(model_id),
                Some(capability_snapshot_hash),
                Some(source),
            ) = (
                model.provider_id,
                model.model_id,
                model.capability_snapshot_hash,
                model.source,
            ) {
                provider_routes.insert(ReleaseProviderRouteEvidence {
                    provider_id,
                    model_id,
                    reasoning_effort: model.reasoning_effort,
                    capability_snapshot_hash,
                    supports_tool_calls: model.supports_tool_calls.unwrap_or(false),
                    supports_stateless_continuation: model
                        .supports_stateless_continuation
                        .unwrap_or(false),
                    native_web_tool: model.native_web_tool.unwrap_or(false),
                    streaming: model.streaming,
                    declared_context_limit: model.declared_context_limit,
                    declared_max_output_tokens: model.declared_max_output_tokens,
                    source,
                });
            }
        }
        for artifact in context_artifacts {
            contracts
                .context_manifest_hashes
                .insert(artifact.artifact_id.0);
        }
        if let Some(runtime) = &runtime {
            contracts
                .contract_hashes
                .insert(runtime.contract_hash.clone());
        }

        let source_snapshots = source_artifacts
            .into_iter()
            .map(|artifact| ReleaseSourceSnapshotEvidence {
                artifact: ArtifactRef {
                    artifact_id: artifact.artifact_id,
                    kind: artifact.kind,
                },
                blob_hash: artifact.blob.hash,
                source_family: artifact.provenance.source_family,
                observed_at: artifact.provenance.observed_at,
                retrieved_at: artifact.provenance.retrieved_at,
            })
            .collect::<BTreeSet<_>>();

        let plan = plans.last().cloned();
        let commitment = commitments.last().cloned();
        let reconciliation = reconciliations.last().cloned();

        let mut order_identities = Vec::new();
        let mut receipt_refs = Vec::new();
        let reconciliation_payload = reconciliation
            .as_ref()
            .map(|artifact| self.read_artifact_payload::<Reconciliation>(artifact))
            .transpose()?;
        if let Some(payload) = &reconciliation_payload {
            for receipt_ref in &payload.broker_receipts {
                let artifact = self.artifact(&receipt_ref.artifact_id)?;
                let receipt: OrderReceipt = self.read_artifact_payload(&artifact)?;
                receipt.validate()?;
                order_identities.push(ReleaseOrderIdentity {
                    client_order_id: receipt.client_order_id,
                    broker_order_id: receipt.broker_order_id,
                });
                receipt_refs.push(receipt_ref.clone());
            }
        }
        order_identities.sort();
        receipt_refs.sort();

        let runtime_account_fingerprint = approval
            .as_ref()
            .map(|(manifest, _)| ContentHash::of_bytes(manifest.broker_account_id.as_bytes()));
        let offline_fixture = approval.as_ref().is_some_and(|(manifest, _)| {
            manifest.provider_id.contains("fixture")
                || manifest.model_id.contains("fixture")
                || manifest.code_revision.contains("fixture")
        }) || provider_routes
            .iter()
            .any(|route| route.provider_id.contains("fixture") || route.source.contains("fixture"))
            || source_snapshots
                .iter()
                .any(|source| source.source_family.contains("fixture"));
        let broker = runtime_account_fingerprint
            .as_ref()
            .map(|account_fingerprint| ReleaseBrokerEvidence {
                account_fingerprint: account_fingerprint.clone(),
                trust: if !offline_fixture && !order_identities.is_empty() {
                    ReleaseBrokerEvidenceTrust::RealBroker
                } else {
                    ReleaseBrokerEvidenceTrust::OfflineFixture
                },
                orders: order_identities,
            });

        let execution = match (plan, commitment) {
            (Some(plan_artifact), Some(commitment_artifact)) => {
                let plan_payload: ExecutionPlan = self.read_artifact_payload(&plan_artifact)?;
                let commitment_payload: PaperCommitment =
                    self.read_artifact_payload(&commitment_artifact)?;
                plan_payload.validate()?;
                commitment_payload.validate()?;
                Some(ReleaseExecutionEvidence {
                    execution_plan: ArtifactRef {
                        artifact_id: plan_artifact.artifact_id,
                        kind: ArtifactKind::ExecutionPlan,
                    },
                    plan_hash: plan_payload.plan_hash,
                    commitment: ArtifactRef {
                        artifact_id: commitment_artifact.artifact_id,
                        kind: ArtifactKind::ExecutionCommitment,
                    },
                    commitment_id: commitment_payload.commitment_id.0,
                    reconciliation: reconciliation.as_ref().map(|artifact| ArtifactRef {
                        artifact_id: artifact.artifact_id.clone(),
                        kind: ArtifactKind::Reconciliation,
                    }),
                    reconciliation_receipts: receipt_refs,
                })
            }
            _ => None,
        };

        let mut outcomes = BTreeMap::new();
        let outcome_artifact = self.outcome_for_run(run_id)?;
        if let Some(artifact) = &outcome_artifact {
            let outcome: Outcome = self.read_artifact_payload(artifact)?;
            outcome.validate_sealed()?;
            let sealed_at = outcome
                .sealed_at
                .ok_or_else(|| StoreError::UnsealedOutcome(artifact.artifact_id.clone()))?;
            for window in outcome.windows {
                outcomes.insert(
                    window.horizon,
                    ReleaseOutcomeEvidence {
                        outcome: ArtifactRef {
                            artifact_id: artifact.artifact_id.clone(),
                            kind: ArtifactKind::Outcome,
                        },
                        sealed_at,
                        observed_on: window.observed_trading_day,
                    },
                );
            }
        }

        let canary = self
            .canary_session_for_run(run_id)?
            .map(|session| {
                self.canary_campaign(&session.reservation.campaign_id)
                    .map(|campaign| {
                        campaign.map(|campaign| ReleaseCanaryEvidence {
                            campaign_id: campaign.spec.campaign_id,
                            status: campaign.status,
                            revision: campaign.revision,
                        })
                    })
            })
            .transpose()?
            .flatten();

        let human_approval = approval
            .as_ref()
            .map(|(_, approval)| ReleaseHumanApprovalEvidence {
                status: ReleaseHumanApprovalStatus::Approved,
                operator_identity: approval.operator_identity.clone(),
                approved_at: Some(approval.approved_at),
                approval_hash: approval.approval_hash.clone(),
            });
        let session_evidence = session.as_ref().map(|slot| ReleaseSessionEvidence {
            session_key: slot.session_key.clone(),
            scheduler_epoch: slot.scheduler_epoch,
            reserved_at: slot.reserved_at,
            committed_at: slot.committed_at,
        });
        let daemon_evidence = lease.as_ref().map(|lease| ReleaseDaemonEvidence {
            lease_name: lease.lease_name.clone(),
            owner_id: lease.owner_id.clone(),
            epoch: lease.epoch,
            expires_at: lease.expires_at,
        });
        let environment = if broker
            .as_ref()
            .is_some_and(|broker| broker.trust == ReleaseBrokerEvidenceTrust::RealBroker)
        {
            ReleaseEvidenceEnvironment::Real
        } else {
            ReleaseEvidenceEnvironment::OfflineFixture
        };

        let config_hash_matches = expectations.config_hash.as_ref().is_none_or(|expected| {
            runtime
                .as_ref()
                .is_some_and(|runtime| &runtime.config_hash == expected)
        });
        let workflow_hash_matches = expectations
            .workflow_hash
            .as_ref()
            .is_none_or(|expected| expected == &workflow_hash);
        let broker_account_matches = expectations
            .broker_account_fingerprint
            .as_ref()
            .is_none_or(|expected| runtime_account_fingerprint.as_ref() == Some(expected));
        let daemon_epoch_current = match (&session, &lease) {
            (Some(slot), Some(lease)) => {
                slot.scheduler_epoch == lease.epoch
                    && expectations
                        .daemon_epoch
                        .is_none_or(|expected| expected == lease.epoch)
                    && expectations
                        .daemon_owner_id
                        .as_ref()
                        .is_none_or(|expected| expected == &lease.owner_id)
            }
            _ => false,
        };

        let materialized_at = release_materialized_at(
            workflow.run.created_at,
            &session,
            &lease,
            outcome_artifact.as_ref(),
        );
        ReleaseEvidenceBundle::materialize(ReleaseEvidenceBody {
            run_id: run_id.clone(),
            purpose: workflow.run.purpose,
            environment,
            materialized_at,
            runtime,
            workflow: workflow_evidence,
            contracts,
            provider_routes,
            source_snapshots,
            broker,
            session: session_evidence,
            daemon: daemon_evidence,
            execution,
            outcomes,
            learning,
            canary,
            human_approval,
            integrity: ReleaseIntegrityEvidence {
                config_hash_matches,
                workflow_hash_matches,
                broker_account_matches,
                daemon_epoch_current,
            },
        })
        .map_err(StoreError::from)
    }
}

fn run_artifacts(
    connection: &Connection,
    run_id: &RunId,
    kind: ArtifactKind,
) -> StoreResult<Vec<Artifact>> {
    Ok(read_kind_artifacts(connection, kind)?
        .into_iter()
        .filter(|artifact| {
            artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                == Some(run_id)
        })
        .collect())
}

fn canonical_run_artifacts(
    connection: &Connection,
    run_id: &RunId,
    kind: ArtifactKind,
) -> StoreResult<Vec<Artifact>> {
    Ok(run_artifacts(connection, run_id, kind)?
        .into_iter()
        .filter(|artifact| artifact.lifecycle == ArtifactLifecycle::Canonical)
        .collect())
}

fn repository_state(code_revision: &str) -> (String, bool) {
    match code_revision.split_once('+') {
        Some((commit, state_hash)) => (commit.to_owned(), state_hash != CLEAN_WORKTREE_HASH),
        None => (code_revision.to_owned(), true),
    }
}

fn release_learning_evidence(
    connection: &Connection,
    run_id: &RunId,
) -> StoreResult<Option<ReleaseLearningEvidence>> {
    let row = connection
        .query_row(
            r#"SELECT transition_id, from_state_json, to_state_json,
                      evaluation_artifact_id, completed_at
               FROM rebuild_policy_evaluations
               WHERE run_id = ?1 AND transition_id IS NOT NULL
               ORDER BY event_cursor DESC LIMIT 1"#,
            params![run_id.0],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )
        .optional()?;
    row.map(
        |(transition_id, from, to, evaluation_artifact_id, completed_at)| {
            Ok(ReleaseLearningEvidence {
                transition_id,
                from: serde_json::from_str(&from)?,
                to: serde_json::from_str(&to)?,
                evaluation: ArtifactRef {
                    artifact_id: ArtifactId(ContentHash::new(evaluation_artifact_id)?),
                    kind: ArtifactKind::Evaluation,
                },
                transitioned_at: parse_time(&completed_at)?,
            })
        },
    )
    .transpose()
}

fn release_materialized_at(
    created_at: DateTime<Utc>,
    session: &Option<SessionSlot>,
    lease: &Option<DaemonLease>,
    outcome: Option<&Artifact>,
) -> DateTime<Utc> {
    let mut latest = created_at;
    if let Some(session) = session {
        latest = latest.max(session.committed_at.unwrap_or(session.reserved_at));
    }
    if let Some(lease) = lease {
        latest = latest.max(lease.expires_at);
    }
    if let Some(outcome) = outcome {
        latest = latest.max(outcome.created_at);
    }
    latest
}
