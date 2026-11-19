use super::*;
use akzio_domain::{
    ClaimStance, DecisionId, DependencyClosure, EvidenceGround, EvidenceGroundRole, FactorExposure,
    FailureDisposition, Forecast, ForecastThesis, MandateAssessment, MarketClockSnapshot,
    ModelQualificationEvidence, ModelQualificationReport, NodeSpec, OrderIntent, OrderReceiptState,
    OrderSide, PaperApprovalScope, PaperCommitmentId, PaperReprice, PaperRepriceId,
    PreTradeSafetySnapshot, Quote, ReconciliationId, ReconciliationState, TargetPortfolio,
    TaskBudget, TaskRecipeId, WorkflowGraph, WorkflowNode, WorkflowProposal,
    WORKFLOW_DEFINITION_VERSION,
};
use akzio_runtime::PAPER_COMMIT_RECIPE_ID;
use akzio_store::{
    DecisionPolicyDescriptor, ExecutionCommit, PaperSubmissionAdmission, PaperSubmissionAuthority,
    PaperSubmissionBatch, SessionReservation, StoredRun, WorkflowCommit,
};
use std::cell::RefCell;
use std::sync::mpsc::{channel, RecvTimeoutError};

thread_local! {
    static AFTER_OPERATOR_REPORT: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
}

pub(super) fn after_operator_report() {
    if let Some(write) = AFTER_OPERATOR_REPORT.with(|hook| hook.borrow_mut().take()) {
        write();
    }
}

struct PaperReportFixture<'a> {
    store: &'a Store,
    run_id: RunId,
    policy_hash: ContentHash,
    session: String,
    now: DateTime<Utc>,
}

impl<'a> PaperReportFixture<'a> {
    fn prepare(store: &'a Store) -> (Self, akzio_store::DaemonLease) {
        let now = Utc::now();
        let fixture = Self {
            store,
            run_id: RunId::new(),
            policy_hash: ContentHash::of_bytes(b"observer fixture decision policy"),
            session: now.date_naive().to_string(),
            now,
        };
        fixture.activate_policy();
        let lease = store
            .acquire_daemon_lease(
                crate::scheduler::SCHEDULER_LEASE_NAME,
                "observer-fixture",
                now,
                now + Duration::minutes(2),
            )
            .unwrap()
            .unwrap();
        fixture.reserve(&lease);
        (fixture, lease)
    }

    fn stage<T: Serialize>(
        &self,
        permit: Option<&TaskWritePermit>,
        kind: ArtifactKind,
        producer: &str,
        value: &T,
        sources: Vec<ArtifactRef>,
    ) -> Artifact {
        let lifecycle = if matches!(
            kind,
            ArtifactKind::WorkflowGraph | ArtifactKind::WorkflowProposal | ArtifactKind::Claim
        ) {
            ArtifactLifecycle::RunScoped
        } else {
            ArtifactLifecycle::Canonical
        };
        let origin = match kind {
            ArtifactKind::DecisionPolicy
            | ArtifactKind::RuntimeManifest
            | ArtifactKind::PaperLaunchApproval => None,
            _ => Some(permit.map_or(
                ArtifactOrigin {
                    run_id: Some(self.run_id.clone()),
                    task_id: None,
                    attempt_id: None,
                    contract_hash: None,
                },
                TaskWritePermit::artifact_origin,
            )),
        };
        Artifact::new(
            kind,
            self.store.stage_json(value).unwrap(),
            producer,
            lifecycle,
            ArtifactProvenance {
                source_family: "fixture".into(),
                observed_at: None,
                retrieved_at: self.now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            origin,
            sources,
            self.now,
        )
        .unwrap()
    }

    fn activate_policy(&self) {
        let artifact = self.stage(
            None,
            ArtifactKind::DecisionPolicy,
            "fixture.decision_policy",
            &self.policy_hash,
            Vec::new(),
        );
        self.store
            .activate_decision_policy(
                &artifact,
                &DecisionPolicyDescriptor {
                    policy_hash: self.policy_hash.clone(),
                    envelope_hash: artifact.blob.hash.clone(),
                    provider_id: "fixture".into(),
                    model_id: "fixture".into(),
                    model_version_hash: ContentHash::of_bytes(b"observer fixture model"),
                    model_route: "research.synthesizer".into(),
                    contract_hash: ContentHash::of_bytes(b"observer fixture contract"),
                },
                self.now,
            )
            .unwrap();
    }

    // This fixture seeds accepted gate payloads through canonical Store APIs. The
    // session, fenced Commitment, journal transitions and Observer use production entries.
    fn reserve(&self, lease: &akzio_store::DaemonLease) {
        let mut nodes: Vec<WorkflowNode> = Vec::new();
        for recipe in [
            DECISION_GATE_RECIPE_ID,
            EXECUTION_GATE_RECIPE_ID,
            PAPER_COMMIT_RECIPE_ID,
            RECONCILE_RECIPE_ID,
        ] {
            nodes.push(WorkflowNode {
                spec: Some(NodeSpec::stage(recipe)),
                task_id: TaskId::new(),
                recipe_id: TaskRecipeId::new(recipe).unwrap(),
                contract_hash: None,
                objective: "Read back a disposable Paper execution.".into(),
                dependencies: nodes
                    .last()
                    .map(|node| node.task_id.clone())
                    .into_iter()
                    .collect(),
                input_artifacts: Vec::new(),
                priority: 1,
                budget: TaskBudget {
                    max_input_tokens: 1,
                    max_output_tokens: 1,
                    max_wall_time_secs: 30,
                    max_tool_calls: akzio_domain::budget::ToolCallLimit::Limited(0),
                },
                retry: akzio_domain::RetryPolicy::none(),
                on_failure: FailureDisposition::FailRun,
                parent_task_id: None,
            });
        }
        let topology_id = "observer-partial-execution-fixture".to_owned();
        let graph = WorkflowGraph {
            definition_version: Some(WORKFLOW_DEFINITION_VERSION),
            schema_version: DOMAIN_SCHEMA_VERSION,
            topology_id: topology_id.clone(),
            nodes: nodes.clone(),
            agent_budgets: Default::default(),
        };
        let graph = self.stage(
            None,
            ArtifactKind::WorkflowGraph,
            "runtime.workflow",
            &graph,
            Vec::new(),
        );
        let workflow = WorkflowCommit {
            run: StoredRun {
                run_id: self.run_id.clone(),
                purpose: RunPurpose::Paper,
                topology_id: topology_id.clone(),
                graph_artifact_id: graph.artifact_id.clone(),
                created_at: self.now,
            },
            graph,
            nodes,
        };
        let proposal = self.stage(
            None,
            ArtifactKind::WorkflowProposal,
            "runtime.paper_provisioning",
            &WorkflowProposal {
                schema_version: DOMAIN_SCHEMA_VERSION,
                topology_id,
                tasks: Default::default(),
                stop_reason: None,
            },
            Vec::new(),
        );
        let (manifest, approval) = self.approval();
        self.store
            .reserve_paper_session_with_approval(
                lease,
                &SessionReservation {
                    session_key: self.session.clone(),
                    workflow,
                    setup_artifacts: Vec::new(),
                    reserved_at: self.now,
                },
                &proposal,
                &manifest,
                &approval,
            )
            .unwrap();
    }

    fn approval(&self) -> (Artifact, Artifact) {
        let hash = ContentHash::of_bytes(b"observer fixture");
        let expiry = self.now + Duration::minutes(10);
        let components = [("default".to_owned(), hash.clone())].into();
        let bundle = akzio_domain::runtime_component_bundle_hash(&components).unwrap();
        let manifest: RuntimeManifest = serde_json::from_value(json!({
            "schema_version": DOMAIN_SCHEMA_VERSION, "code_revision": "fixture", "rust_toolchain": "1.96.0",
            "experiment_profile": "paper-engineering", "model_capability_hashes": components,
            "model_capability_bundle_hash": bundle, "governance_component_hashes": components,
            "governance_bundle_hash": bundle, "cargo_lock_hash": hash, "config_hash": hash,
            "provider_id": "fixture", "model_id": "fixture", "prompt_hash": hash, "contract_hash": hash,
            "topology_hash": hash, "decision_policy_hash": self.policy_hash, "execution_policy_hash": hash,
            "evaluation_policy_hash": hash, "market_data_feed": "iex", "broker_account_id": "offline-account",
            "maximum_notional": 10_000_000, "allowed_session_start": self.session,
            "allowed_session_end": self.session, "expires_at": expiry, "created_at": self.now,
        })).unwrap();
        let manifest_artifact = self.stage(
            None,
            ArtifactKind::RuntimeManifest,
            "runtime.manifest",
            &manifest,
            Vec::new(),
        );
        let manifest_ref = reference(&manifest_artifact);
        let evaluation = ArtifactRef {
            artifact_id: ArtifactId(hash.clone()),
            kind: ArtifactKind::Evaluation,
        };
        let mut qualification = ModelQualificationReport {
            key: manifest.runtime_identity().qualification_key().unwrap(),
            behavior_fingerprint: hash.clone(),
            evidence: ModelQualificationEvidence {
                frozen_context_manifest: ArtifactRef {
                    artifact_id: ArtifactId(hash.clone()),
                    kind: ArtifactKind::ContextManifest,
                },
                replay_evaluation: evaluation.clone(),
                adversarial_evaluation: evaluation.clone(),
                execution_simulation_evaluation: evaluation.clone(),
                shadow_evaluation: evaluation.clone(),
                canary_evaluation: evaluation,
            },
            replay_passed: true,
            adversarial_passed: true,
            execution_simulation_passed: true,
            shadow_passed: true,
            canary_passed: true,
            critical_regressions: 0,
            approved_by: "fixture".into(),
            qualified_at: self.now,
            expires_at: expiry,
            report_hash: hash.clone(),
        };
        qualification.report_hash = qualification.unsigned_hash();
        let mut approval = PaperLaunchApproval {
            schema_version: DOMAIN_SCHEMA_VERSION,
            operator_identity: "fixture".into(),
            runtime_manifest: manifest_ref.clone(),
            runtime_manifest_hash: manifest.manifest_hash().unwrap(),
            scope: PaperApprovalScope::Canary,
            reason: "Observer fixture".into(),
            behavior_bundle_hash: None,
            approved_at: self.now,
            expires_at: expiry,
            qualification,
            mandate: ExecutionGatePolicy::default().mandate,
            approval_hash: hash,
        };
        approval.approval_hash = approval.unsigned_hash().unwrap();
        (
            manifest_artifact,
            self.stage(
                None,
                ArtifactKind::PaperLaunchApproval,
                "operator.paper_approval",
                &approval,
                vec![manifest_ref],
            ),
        )
    }

    fn claim(&self, recipe: &str) -> TaskWritePermit {
        let task = self
            .store
            .claim_next_task_for_workload(
                "observer-fixture",
                Utc::now(),
                Duration::minutes(2),
                akzio_store::TaskWorkload::Session,
            )
            .unwrap()
            .unwrap();
        assert_eq!(task.node.recipe_id.as_str(), recipe);
        task.permit
    }

    fn decision(&self) -> ArtifactRef {
        let permit = self.claim(DECISION_GATE_RECIPE_ID);
        let evidence = self.stage(
            Some(&permit),
            ArtifactKind::SemanticDetail,
            "fixture.evidence",
            &json!({"statement":"No directional evidence was asserted."}),
            Vec::new(),
        );
        let claim = self.stage(
            Some(&permit),
            ArtifactKind::Claim,
            "agent.research.analyst",
            &ResearchClaim {
                schema_version: DOMAIN_SCHEMA_VERSION,
                topic: "Neutral fixture".into(),
                statement: "Cash is the fixture's target.".into(),
                horizon: DecisionHorizon::T1,
                stance: ClaimStance::Neutral,
                materiality_ppm: 100_000,
                confidence_ppm: 500_000,
                grounds: vec![EvidenceGround {
                    evidence: reference(&evidence),
                    support: "Observation only".into(),
                    role: EvidenceGroundRole::Descriptive,
                    assets: Default::default(),
                    domain: None,
                }],
                evidence_gaps: Vec::new(),
            },
            vec![reference(&evidence)],
        );
        let target = TargetPortfolio::zeroed();
        let context: DecisionContext = serde_json::from_value(json!({
            "schema_version": DOMAIN_SCHEMA_VERSION, "decision_id": DecisionId::new(), "run_id": self.run_id,
            "claims": [reference(&claim)], "critiques": [], "evidence": [reference(&evidence)], "policy_influences": [],
            "material_conflicts": [], "hard_blockers": [], "soft_warnings": [],
            "decision_policy_hash": self.policy_hash, "target": target, "created_at": self.now,
        })).unwrap();
        let context = self.stage(
            Some(&permit),
            ArtifactKind::DecisionContext,
            "decision.gate",
            &context,
            vec![reference(&claim), reference(&evidence)],
        );
        let forecasts = Asset::EXECUTABLE
            .into_iter()
            .flat_map(|asset| {
                DecisionHorizon::ALL.map(|horizon| Forecast {
                    asset,
                    horizon,
                    positive_return_probability_ppm: 500_000,
                    expected_return_ppm: 0,
                    thesis: Some(ForecastThesis {
                        thesis_valid_until: self.now
                            + Duration::days(i64::from(horizon.trading_days())),
                        expected_holding_period_days: horizon.trading_days(),
                        exit_condition: "End of fixture window".into(),
                        invalidation_conditions: vec!["Observation changes".into()],
                    }),
                })
            })
            .collect();
        let context_ref = reference(&context);
        let decision = self.stage(
            Some(&permit),
            ArtifactKind::Decision,
            "decision.gate",
            &Decision {
                schema_version: DOMAIN_SCHEMA_VERSION,
                decision_context: context_ref.clone(),
                summary: "Reduce the existing holdings to cash.".into(),
                targets: target,
                confidence_ppm: 500_000,
                forecasts,
                research_plan: None,
                created_at: self.now,
            },
            vec![context_ref.clone()],
        );
        self.store
            .commit_attempt(
                &permit,
                &[evidence, claim, context, decision],
                TaskStatus::Succeeded,
                Utc::now(),
            )
            .unwrap();
        context_ref
    }

    fn commit_plan(
        &self,
        decision_context: ArtifactRef,
        lease: &akzio_store::DaemonLease,
    ) -> (ArtifactRef, PaperCommitment) {
        let permit = self.claim(EXECUTION_GATE_RECIPE_ID);
        let assets = [Asset::Qqq, Asset::Soxx];
        let account = self.stage(
            Some(&permit),
            ArtifactKind::NormalizedEvidence,
            "execution.snapshot.account",
            &AccountSnapshot {
                schema_version: DOMAIN_SCHEMA_VERSION,
                broker_session: self.session.clone(),
                observed_at: self.now,
                equity: MoneyMicros(10_000_000),
                buying_power: MoneyMicros(8_000_000),
                day_turnover: MoneyMicros::ZERO,
                active: true,
                trading_blocked: false,
                positions: assets
                    .into_iter()
                    .map(|asset| {
                        (
                            asset,
                            akzio_domain::Position {
                                quantity_micros: 1_000_000,
                                market_value: MoneyMicros(1_000_000),
                            },
                        )
                    })
                    .collect(),
                external_positions: Default::default(),
                open_order_ids: Default::default(),
            },
            Vec::new(),
        );
        let quote = self.stage(
            Some(&permit),
            ArtifactKind::NormalizedEvidence,
            "execution.snapshot.quotes",
            &QuoteSnapshot {
                feed: Some("iex".into()),
                schema_version: DOMAIN_SCHEMA_VERSION,
                broker_session: self.session.clone(),
                observed_at: self.now,
                quotes: assets
                    .into_iter()
                    .map(|asset| {
                        (
                            asset,
                            Quote {
                                bid: MoneyMicros(1_000_000),
                                ask: MoneyMicros(1_000_100),
                                observed_at: self.now,
                            },
                        )
                    })
                    .collect(),
            },
            Vec::new(),
        );
        let clock = self.stage(
            Some(&permit),
            ArtifactKind::NormalizedEvidence,
            "execution.snapshot.clock",
            &MarketClockSnapshot {
                schema_version: DOMAIN_SCHEMA_VERSION,
                broker_session: self.session.clone(),
                observed_at: self.now,
                is_open: true,
                session: None,
            },
            Vec::new(),
        );
        let sources = vec![
            decision_context.clone(),
            reference(&account),
            reference(&quote),
            reference(&clock),
        ];
        let target = TargetPortfolio::zeroed();
        let mut plan = ExecutionPlan {
            schema_version: DOMAIN_SCHEMA_VERSION,
            decision_context: decision_context.clone(),
            account_snapshot: reference(&account),
            quote_snapshot: reference(&quote),
            market_clock_snapshot: reference(&clock),
            policy_hash: ExecutionPolicy::default().policy_hash().unwrap(),
            maximum_total_notional: MoneyMicros(10_000_000),
            factor_exposure: FactorExposure::from_target(&target).unwrap(),
            target,
            orders: assets
                .into_iter()
                .map(|asset| OrderIntent {
                    asset,
                    side: OrderSide::Sell,
                    notional: MoneyMicros(1_000_000),
                    limit_price: MoneyMicros(1_000_000),
                    extended_hours: false,
                })
                .collect(),
            gross_exposure_ppm: 0,
            net_exposure_ppm: 0,
            turnover_ppm: 200_000,
            broker_session: self.session.clone(),
            created_at: self.now,
            plan_hash: ContentHash::of_bytes(b"pending fixture plan hash"),
        };
        plan.refresh_hash().unwrap();
        let plan_artifact = self.stage(
            Some(&permit),
            ArtifactKind::ExecutionPlan,
            "execution.plan",
            &plan,
            sources.clone(),
        );
        let context = ExecutionContext {
            schema_version: DOMAIN_SCHEMA_VERSION,
            run_id: self.run_id.clone(),
            decision_context,
            account_snapshot: Some(reference(&account)),
            quote_snapshot: Some(reference(&quote)),
            market_clock_snapshot: Some(reference(&clock)),
            execution_plan: Some(reference(&plan_artifact)),
            factor_exposure: Some(plan.factor_exposure),
            turnover_ppm: Some(plan.turnover_ppm),
            plan_hash: Some(plan.plan_hash.clone()),
            broker_session: Some(self.session.clone()),
            mandate_assessment: Some(MandateAssessment {
                mandate_hash: ExecutionGatePolicy::default().mandate.identity_hash(),
                violations: Default::default(),
                distance_before_ppm: 0,
                distance_after_ppm: 0,
            }),
            pretrade_safety: Some(PreTradeSafetySnapshot {
                ordered_assets: assets.into(),
                information_classifications: Default::default(),
                recent_compliance_activity: Default::default(),
                dependency_closure: DependencyClosure {
                    captured_at: self.now,
                    dependencies: Default::default(),
                },
                capacity_scenario: None,
                missing_average_daily_dollar_volume: Default::default(),
                compliance_violations_by_asset: assets
                    .into_iter()
                    .map(|asset| (asset, Default::default()))
                    .collect(),
                missing_information_classifications: Default::default(),
                risk_increasing: false,
                dependency_permits_new_risk: false,
                dependency_permits_risk_reduction: true,
                permits_execution: true,
                permits_risk_increasing: false,
            }),
            final_process_quality: None,
            frozen: false,
            created_at: self.now,
        };
        let context = self.stage(
            Some(&permit),
            ArtifactKind::ExecutionContext,
            "execution.context",
            &context,
            [sources, vec![reference(&plan_artifact)]].concat(),
        );
        let context_ref = reference(&context);
        let verdict = self.stage(
            Some(&permit),
            ArtifactKind::ExecutionVerdict,
            "execution.gate",
            &ExecutionVerdict::Accepted {
                execution_context: context_ref.clone(),
            },
            vec![context_ref.clone()],
        );
        let verdict_ref = reference(&verdict);
        self.store
            .commit_attempt(
                &permit,
                &[account, quote, clock, plan_artifact, context, verdict],
                TaskStatus::Succeeded,
                Utc::now(),
            )
            .unwrap();
        let permit = self.claim(PAPER_COMMIT_RECIPE_ID);
        let payload = PaperCommitment {
            commitment_id: PaperCommitmentId::new(),
            execution_context: context_ref.clone(),
            plan_hash: plan.plan_hash,
            broker_session: self.session.clone(),
            created_at: self.now,
            client_order_ids: [
                (Asset::Qqq, "observer-qqq-r0".into()),
                (Asset::Soxx, "observer-soxx-r0".into()),
            ]
            .into(),
        };
        let commitment = self.stage(
            Some(&permit),
            ArtifactKind::ExecutionCommitment,
            "execution.paper_commitment",
            &payload,
            vec![verdict_ref, context_ref],
        );
        let commitment_ref = reference(&commitment);
        self.store
            .commit_execution(
                lease,
                &ExecutionCommit {
                    session_key: self.session.clone(),
                    permit,
                    commitment,
                    committed_at: Utc::now(),
                },
                Some(&self.policy_hash),
            )
            .unwrap();
        (commitment_ref, payload)
    }

    fn latest_receipt(&self, commitment: &ArtifactRef, client_order_id: &str) -> ArtifactRef {
        let Some(PaperSubmissionBatch::Journaled { orders, .. }) =
            self.store.paper_submission_batch(commitment).unwrap()
        else {
            panic!("expected exact committed Paper journal");
        };
        orders
            .into_iter()
            .find(|order| order.client_order_id == client_order_id)
            .and_then(|order| order.receipt)
            .unwrap()
    }

    fn settle(
        &self,
        authority: &PaperSubmissionAuthority<'_>,
        receipts: [Artifact; 2],
    ) -> ArtifactRef {
        let receipt_refs = receipts.each_ref().map(reference).to_vec();
        let target = TargetPortfolio::zeroed();
        let reconciliation = self.stage(
            Some(authority.permit),
            ArtifactKind::Reconciliation,
            "execution.reconciliation",
            &Reconciliation {
                reconciliation_id: ReconciliationId::new(),
                commitment: authority.commitment.clone(),
                state: ReconciliationState::Complete,
                broker_receipts: receipt_refs.clone(),
                unsubmitted_orders: Vec::new(),
                achieved_factor_exposure: Some(FactorExposure::from_target(&target).unwrap()),
                achieved_target: Some(target),
                reconciled_at: Utc::now(),
            },
            [vec![authority.commitment.clone()], receipt_refs].concat(),
        );
        let reconciliation_ref = reference(&reconciliation);
        let [qqq, soxx] = receipts;
        self.store
            .commit_fenced_attempt_with_effect(
                authority.lease,
                authority.permit,
                &[qqq, soxx, reconciliation],
                authority.commitment,
                true,
                Utc::now(),
            )
            .unwrap();
        reconciliation_ref
    }
}

fn reference(artifact: &Artifact) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: artifact.kind,
    }
}

fn qqq_partial_fill(commitment: &PaperCommitment) -> OrderReceipt {
    let observed_at = Utc::now();
    OrderReceipt {
        plan_hash: commitment.plan_hash.clone(),
        asset: Asset::Qqq,
        client_order_id: commitment.client_order_ids[&Asset::Qqq].clone(),
        broker_order_id: "offline-qqq".into(),
        state: OrderReceiptState::PartiallyFilled,
        requested_quantity_micros: 1_000_000,
        filled_quantity_micros: 400_000,
        remaining_quantity_micros: 600_000,
        average_fill_price: Some(MoneyMicros(1_000_000)),
        reason: None,
        broker_updated_at: observed_at,
        observed_at,
    }
}

/// WHY THIS TEST EXISTS (Task #31): A concurrent receipt append must not mix Observer's report watermark with newer Run detail fields.
/// Do not weaken or delete this assertion.
#[test]
fn observer_detail_uses_one_watermark_during_a_receipt_append() {
    let daemon = super::tests::fixture_daemon(crate::fixture_model_client());
    let (fixture, lease) = PaperReportFixture::prepare(&daemon.store);
    let (commitment, payload) = fixture.commit_plan(fixture.decision(), &lease);
    let permit = fixture.claim(RECONCILE_RECIPE_ID);
    let authority = PaperSubmissionAuthority {
        lease: &lease,
        permit: &permit,
        commitment: &commitment,
        startup_decision_policy_hash: Some(&fixture.policy_hash),
    };
    fixture
        .store
        .record_paper_effect_intent(&lease, &permit, &commitment, Utc::now())
        .unwrap();
    let before = daemon.observer_run_detail(&fixture.run_id).unwrap();
    let receipt = qqq_partial_fill(&payload);
    let writer = Store::open(fixture.store.root()).unwrap();
    let during = std::thread::scope(|scope| {
        let (start, report_built) = channel();
        let (completed, writer_finished) = channel();
        let observation = &receipt;
        let write = scope.spawn(move || {
            report_built.recv().unwrap();
            writer
                .record_paper_submission_receipt(&authority, observation)
                .unwrap();
            let _ = completed.send(());
        });
        AFTER_OPERATOR_REPORT.with(|hook| {
            hook.replace(Some(Box::new(move || {
                start.send(()).unwrap();
                // Rollback-journal SQLite can wait to commit until the snapshot closes.
                match writer_finished.recv_timeout(std::time::Duration::from_secs(1)) {
                    Ok(()) | Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => panic!("concurrent writer failed"),
                }
            })));
        });
        let during = daemon.observer_run_detail(&fixture.run_id);
        AFTER_OPERATOR_REPORT.with(|hook| drop(hook.take()));
        write.join().unwrap();
        during.unwrap()
    });
    let cursor = before.workflow.event_cursor;
    assert_eq!(during.workflow.event_cursor, cursor);
    assert_eq!(during.operator_report["event_cursor"], cursor);
    assert_eq!(
        during.inspection.workflow.event_cursor, cursor,
        "inspection and report must read the same watermark"
    );
    assert_eq!(
        serde_json::to_value(during).unwrap(),
        serde_json::to_value(before).unwrap()
    );

    let after = daemon.observer_run_detail(&fixture.run_id).unwrap();
    let new_cursor = after.workflow.event_cursor;
    assert!(new_cursor > cursor);
    assert_eq!(after.inspection.workflow.event_cursor, new_cursor);
    assert_eq!(after.operator_report["event_cursor"], new_cursor);
    assert_eq!(after.events.last().unwrap().cursor, new_cursor);
    let (reference, persisted) = fixture
        .store
        .legacy_paper_submission_receipts(&commitment)
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(persisted, receipt);
    assert!(after.trajectory.iter().any(|item| {
        item.cursor == new_cursor && item.artifact_id.as_ref() == Some(&reference.artifact_id)
    }));
    assert!(after
        .artifacts
        .iter()
        .any(|artifact| artifact.artifact_id == reference.artifact_id));
    assert_eq!(
        after.operator_report["execution"]["receipts"][0]["artifact"],
        json!(reference)
    );
}

/// WHY THIS TEST EXISTS (Task #31): Observer must show exact durable legacy observations without turning a historical effect into a per-order submission journal.
/// Do not weaken or delete this assertion.
#[test]
fn observer_reads_proven_legacy_receipts_without_inventing_order_states() {
    let daemon = super::tests::fixture_daemon(crate::fixture_model_client());
    let (fixture, lease) = PaperReportFixture::prepare(&daemon.store);
    let (commitment, payload) = fixture.commit_plan(fixture.decision(), &lease);
    let permit = fixture.claim(RECONCILE_RECIPE_ID);
    let authority = PaperSubmissionAuthority {
        lease: &lease,
        permit: &permit,
        commitment: &commitment,
        startup_decision_policy_hash: Some(&fixture.policy_hash),
    };
    fixture
        .store
        .record_paper_effect_intent(&lease, &permit, &commitment, Utc::now())
        .unwrap();
    let detail = || daemon.observer_run_detail(&fixture.run_id).unwrap();
    let before = detail();
    let execution = &before.operator_report["execution"];
    assert_eq!(execution["submission_journal"]["state"], "legacy_unknown");
    assert!(execution["observed_receipt_count"].is_null());
    assert!(execution["receipt_source"].is_null());

    let receipt = qqq_partial_fill(&payload);
    let pinned_report = std::thread::scope(|scope| {
        let (pinned, snapshot_opened) = std::sync::mpsc::channel();
        let writer = fixture.store;
        let observation = &receipt;
        let write = scope.spawn(move || {
            snapshot_opened.recv().unwrap();
            writer.record_paper_submission_receipt(&authority, observation)
        });
        let report = fixture.store.with_read_snapshot(|store| -> Result<_> {
            let workflow = store.workflow_snapshot(&fixture.run_id)?;
            pinned.send(()).unwrap();
            super::operator_report::build(store, &workflow)
        });
        drop(pinned);
        // Rollback-journal SQLite may hold the writer until the snapshot closes.
        write.join().unwrap().unwrap();
        report.unwrap()
    });
    assert_eq!(pinned_report["event_cursor"], before.workflow.event_cursor);
    assert!(pinned_report["execution"]["observed_receipt_count"].is_null());
    let observed = fixture
        .store
        .legacy_paper_submission_receipts(&commitment)
        .unwrap();
    let [(reference, persisted)] = observed.as_slice() else {
        panic!("expected one exact legacy observation");
    };
    assert_eq!(persisted, &receipt);
    let current = detail();
    let report = &current.operator_report;
    assert_eq!(report["event_cursor"], current.workflow.event_cursor);
    assert!(current.workflow.event_cursor > before.workflow.event_cursor);
    let execution = &report["execution"];
    assert_eq!(execution["receipt_source"], "legacy_submission_receipts");
    assert_eq!(execution["observed_receipt_count"], 1);
    assert_eq!(execution["submission_journal"]["state"], "legacy_unknown");
    assert_eq!(
        execution["submission_journal"]["commitment"],
        json!(commitment)
    );
    assert!(execution["submission_journal"]["orders"].is_null());
    assert!(execution["reconciliation"].is_null());
    assert!(execution["reconciliation_state"].is_null());
    let [seen] = execution["receipts"].as_array().unwrap().as_slice() else {
        panic!("expected one reported legacy observation");
    };
    assert_eq!(seen["artifact"], json!(reference));
    assert_eq!(seen["client_order_id"], receipt.client_order_id);
    assert_eq!(seen["state"], "partially_filled");
    assert_eq!(seen["filled_quantity_micros"], 400_000);
    assert_eq!(seen["remaining_quantity_micros"], 600_000);
    assert!(report["allocations"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["achieved_ppm"].is_null()));
}

/// WHY THIS TEST EXISTS (Task #31): Observer must show durable partial fills at the current watermark, keep replacement observations separate, and switch to the canonical reconciliation when it commits.
/// Do not weaken or delete this assertion.
#[test]
fn observer_reads_a_running_partial_fill_from_the_submission_journal() {
    let daemon = super::tests::fixture_daemon(crate::fixture_model_client());
    let (fixture, lease) = PaperReportFixture::prepare(&daemon.store);
    let (commitment, payload) = fixture.commit_plan(fixture.decision(), &lease);
    let permit = fixture.claim(RECONCILE_RECIPE_ID);
    let authority = PaperSubmissionAuthority {
        lease: &lease,
        permit: &permit,
        commitment: &commitment,
        startup_decision_policy_hash: Some(&fixture.policy_hash),
    };
    let authorization_expires_at = fixture.now + Duration::minutes(1);
    let detail = || daemon.observer_run_detail(&fixture.run_id).unwrap();
    let before = detail();
    fixture
        .store
        .begin_paper_submission_batch(&authority)
        .unwrap();
    let ready = detail();
    let client_id = &payload.client_order_ids[&Asset::Qqq];
    assert_eq!(
        fixture
            .store
            .admit_paper_submission(&authority, client_id, authorization_expires_at)
            .unwrap(),
        PaperSubmissionAdmission::Admitted
    );
    let receipt = qqq_partial_fill(&payload);
    fixture
        .store
        .record_paper_submission_receipt(&authority, &receipt)
        .unwrap();
    let snapshot = detail();
    let report = &snapshot.operator_report;
    assert_eq!(report["event_cursor"], snapshot.workflow.event_cursor);
    assert!(snapshot.workflow.event_cursor > before.workflow.event_cursor);
    let reconcile = snapshot
        .workflow
        .tasks
        .iter()
        .find(|task| task.node.recipe_id.as_str() == RECONCILE_RECIPE_ID)
        .unwrap();
    assert_eq!(reconcile.status, TaskStatus::Running);
    let execution = &report["execution"];
    assert_eq!(execution["submission_journal"]["state"], "journaled");
    assert_eq!(
        execution["submission_journal"]["commitment"],
        json!(commitment)
    );
    assert!(execution["reconciliation_state"].is_null());
    assert!(execution["reconciliation"].is_null());
    assert_eq!(execution["receipt_source"], "submission_journal");
    assert_eq!(execution["observed_receipt_count"], 1);
    let orders = execution["submission_journal"]["orders"]
        .as_array()
        .unwrap();
    let qqq = orders.iter().find(|row| row["asset"] == "QQQ").unwrap();
    let soxx = orders.iter().find(|row| row["asset"] == "SOXX").unwrap();
    assert_eq!(qqq["state"], "observed");
    assert_eq!(soxx["state"], "ready");
    assert!(soxx["receipt"].is_null());
    let seen = &execution["receipts"][0];
    assert_eq!(seen["artifact"], qqq["receipt"]);
    assert_eq!(seen["state"], "partially_filled");
    assert_eq!(seen["filled_quantity_micros"], 400_000);
    assert_eq!(seen["remaining_quantity_micros"], 600_000);
    let raw = fixture.latest_receipt(&commitment, client_id);
    assert_eq!(seen["artifact"], json!(raw));
    assert_eq!(
        daemon.read_artifact_payload::<OrderReceipt>(&raw).unwrap(),
        receipt
    );
    assert_eq!(
        before.operator_report["execution"]["submission_journal"]["state"],
        "not_started"
    );
    assert!(before.operator_report["execution"]["observed_receipt_count"].is_null());
    assert_eq!(
        ready.operator_report["execution"]["observed_receipt_count"],
        0
    );
    assert!(report["allocations"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["achieved_ppm"].is_null()));

    let reprice = PaperReprice {
        schema_version: DOMAIN_SCHEMA_VERSION,
        reprice_id: PaperRepriceId::new(),
        commitment: commitment.clone(),
        prior_receipt: raw.clone(),
        asset: receipt.asset,
        prior_client_order_id: client_id.clone(),
        replacement_client_order_id: "observer-qqq-r1".into(),
        prior_broker_order_id: receipt.broker_order_id.clone(),
        replacement_limit_price: MoneyMicros(1_100_000),
        created_at: Utc::now(),
    };
    let action = fixture.stage(
        Some(&permit),
        ArtifactKind::ExecutionReprice,
        "execution.reprice",
        &reprice,
        vec![commitment.clone(), raw.clone()],
    );
    fixture
        .store
        .commit_execution_reprice_intent(&lease, &permit, &action, Utc::now())
        .unwrap();
    let action_ref = reference(&action);
    fixture
        .store
        .begin_paper_reprice_submission(&authority, &action_ref)
        .unwrap()
        .unwrap();
    let replacement_id = &reprice.replacement_client_order_id;
    assert_eq!(
        fixture
            .store
            .admit_paper_submission(&authority, replacement_id, authorization_expires_at)
            .unwrap(),
        PaperSubmissionAdmission::Admitted
    );
    let observed_at = Utc::now();
    let mut replacement = OrderReceipt {
        client_order_id: replacement_id.clone(),
        broker_order_id: "offline-qqq-replacement".into(),
        requested_quantity_micros: 600_000,
        filled_quantity_micros: 200_000,
        remaining_quantity_micros: 400_000,
        broker_updated_at: observed_at,
        observed_at,
        ..receipt.clone()
    };
    fixture
        .store
        .record_paper_submission_receipt(&authority, &replacement)
        .unwrap();
    let after_reprice = detail();
    let execution = &after_reprice.operator_report["execution"];
    assert_eq!(execution["observed_receipt_count"], 2);
    let receipts = execution["receipts"].as_array().unwrap();
    let original_row = receipts
        .iter()
        .find(|row| row["client_order_id"] == *client_id)
        .unwrap();
    assert_eq!(original_row["artifact"], json!(raw));
    assert_eq!(original_row["filled_quantity_micros"], 400_000);
    assert!(original_row["reprice"].is_null());
    let replacement_row = receipts
        .iter()
        .find(|row| row["client_order_id"] == *replacement_id)
        .unwrap();
    let raw_replacement = fixture.latest_receipt(&commitment, replacement_id);
    assert_eq!(replacement_row["artifact"], json!(raw_replacement));
    assert_eq!(replacement_row["reprice"], json!(action_ref));
    assert_eq!(replacement_row["requested_quantity_micros"], 600_000);
    assert_eq!(replacement_row["filled_quantity_micros"], 200_000);

    let terminal_at = Utc::now().max(observed_at + Duration::milliseconds(1));
    replacement.state = OrderReceiptState::Filled;
    replacement.filled_quantity_micros = 600_000;
    replacement.remaining_quantity_micros = 0;
    replacement.broker_updated_at = terminal_at;
    replacement.observed_at = terminal_at;
    let soxx_id = &payload.client_order_ids[&Asset::Soxx];
    assert_eq!(
        fixture
            .store
            .admit_paper_submission(&authority, soxx_id, authorization_expires_at)
            .unwrap(),
        PaperSubmissionAdmission::Admitted
    );
    let soxx = OrderReceipt {
        asset: Asset::Soxx,
        client_order_id: soxx_id.clone(),
        broker_order_id: "offline-soxx".into(),
        requested_quantity_micros: 1_000_000,
        filled_quantity_micros: 1_000_000,
        ..replacement.clone()
    };
    let replaced_original = OrderReceipt {
        state: OrderReceiptState::Replaced,
        broker_updated_at: terminal_at,
        observed_at: terminal_at,
        ..receipt
    };
    for observation in [&replaced_original, &replacement, &soxx] {
        fixture
            .store
            .record_paper_submission_receipt(&authority, observation)
            .unwrap();
    }
    let latest_original = fixture.latest_receipt(&commitment, client_id);
    assert_ne!(latest_original, raw);
    let raw_replacement = fixture.latest_receipt(&commitment, replacement_id);
    let qqq = fixture.stage(
        Some(&permit),
        ArtifactKind::OrderReceipt,
        "execution.reconciliation",
        &akzio_domain::normalize_paper_replacement_receipt(
            &reprice,
            &replaced_original,
            &replacement,
        )
        .unwrap(),
        vec![
            commitment.clone(),
            action_ref,
            latest_original,
            raw_replacement,
        ],
    );
    let qqq_ref = reference(&qqq);
    let soxx = fixture.stage(
        Some(&permit),
        ArtifactKind::OrderReceipt,
        "execution.reconciliation",
        &soxx,
        vec![
            commitment.clone(),
            fixture.latest_receipt(&commitment, soxx_id),
        ],
    );
    let reconciliation = fixture.settle(&authority, [qqq, soxx]);
    let settled = detail();
    let execution = &settled.operator_report["execution"];
    assert_eq!(execution["reconciliation"], json!(reconciliation));
    assert_eq!(execution["reconciliation_state"], "complete");
    assert_eq!(execution["receipt_source"], "reconciliation");
    assert!(execution["submission_journal"].is_null());
    assert_eq!(execution["observed_receipt_count"], 2);
    let qqq = execution["receipts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["asset"] == "QQQ")
        .unwrap();
    assert_eq!(qqq["artifact"], json!(qqq_ref));
    assert_eq!(qqq["filled_quantity_micros"], 1_000_000);
    assert!(settled.operator_report["allocations"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["achieved_ppm"] == 0));
}
