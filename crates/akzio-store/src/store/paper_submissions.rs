//! Durable Paper order admission and observation under the exact Commitment.

use akzio_domain::{
    normalize_paper_replacement_receipt, reconciliation_state_for_paper_orders,
    PaperUnsubmittedReason, ReconciliationState, UnsubmittedPaperOrder,
};

use super::*;

const RAW_SUBMISSION_RECEIPT_PRODUCER: &str = "execution.submission_receipt";

/// The claimed Paper attempt and the exact immutable session Commitment.
#[derive(Debug, Clone, Copy)]
pub struct PaperSubmissionAuthority<'a> {
    pub lease: &'a DaemonLease,
    pub permit: &'a TaskWritePermit,
    pub commitment: &'a ArtifactRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaperSubmissionState {
    Ready,
    PossiblySubmitted,
    Observed,
    Terminal,
    ProvenUnsubmitted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaperSubmissionOrder {
    pub asset: Asset,
    pub client_order_id: String,
    pub transition_index: u32,
    pub state: PaperSubmissionState,
    pub reprice: Option<ArtifactRef>,
    pub receipt: Option<ArtifactRef>,
    pub reason: Option<PaperUnsubmittedReason>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaperSubmissionBatch {
    LegacyUnknown,
    Journaled {
        recovered: bool,
        observation_only: bool,
        orders: Vec<PaperSubmissionOrder>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaperSubmissionAdmission {
    Admitted,
    ObserveOnly,
}

#[derive(Debug, Clone, Copy)]
pub enum PaperSubmissionStop<'a> {
    SubmissionWindowExpired,
    BatchObservationOnly {
        client_order_id: &'a str,
        transition_index: u32,
    },
    RunCancelled,
}

impl Store {
    /// Publish the batch intent together with one Ready row for every original ID.
    /// An existing unjournaled effect remains observation-only.
    pub fn begin_paper_submission_batch(
        &self,
        authority: &PaperSubmissionAuthority<'_>,
    ) -> StoreResult<PaperSubmissionBatch> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = Utc::now();
        let commitment = authorize_attempt(&transaction, authority, now)?;
        let run_id = &authority.permit.run_id;
        let reference = authority.commitment;
        if let Some(batch) = read_batch(&transaction, reference, run_id, &commitment)? {
            if let PaperSubmissionBatch::Journaled {
                observation_only: false,
                orders,
                ..
            } = &batch
            {
                // An admitted order which still lacks broker evidence on dispatch
                // entry makes the batch observation-only. Close siblings first.
                if orders
                    .iter()
                    .any(|order| order.state == PaperSubmissionState::PossiblySubmitted)
                {
                    let orders = close_ready_orders(
                        &transaction,
                        authority,
                        orders,
                        PaperUnsubmittedReason::BatchObservationOnly,
                        now,
                    )?;
                    transaction.commit()?;
                    return Ok(PaperSubmissionBatch::Journaled {
                        recovered: true,
                        observation_only: true,
                        orders,
                    });
                }
            }
            transaction.commit()?;
            return Ok(batch);
        }
        if !assert_new_submission_allowed(&transaction, authority)? {
            return Err(StoreError::InvalidPaperEffect(
                reference.artifact_id.clone(),
            ));
        }
        let cursor = append_effect_intent(&transaction, authority, reference, now)?;
        transaction.execute(
            "INSERT INTO rebuild_paper_submission_batches
                (commitment_artifact_id, run_id, effect_intent_cursor) VALUES (?1, ?2, ?3)",
            params![reference.artifact_id.0.as_str(), run_id.0, cursor],
        )?;
        let mut orders = Vec::new();
        for (asset, id) in commitment.client_order_ids {
            let order = ready_order(asset, id, None);
            append_transition(&transaction, authority, &order, now)?;
            orders.push(order);
        }
        transaction.commit()?;
        Ok(PaperSubmissionBatch::Journaled {
            recovered: false,
            observation_only: false,
            orders,
        })
    }

    /// Enroll the one replacement ID represented by an already durable action.
    /// The effect intent and r1 Ready row commit together.
    pub fn begin_paper_reprice_submission(
        &self,
        authority: &PaperSubmissionAuthority<'_>,
        reprice: &ArtifactRef,
    ) -> StoreResult<Option<PaperSubmissionOrder>> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = Utc::now();
        let commitment = authorize_attempt(&transaction, authority, now)?;
        let run_id = &authority.permit.run_id;
        let Some(PaperSubmissionBatch::Journaled {
            observation_only: false,
            orders,
            ..
        }) = read_batch(&transaction, authority.commitment, run_id, &commitment)?
        else {
            return Ok(None);
        };
        let action = read_reprice(&transaction, authority.commitment, &commitment, reprice)?;
        if let Some(existing) = orders
            .iter()
            .find(|order| order.client_order_id == action.replacement_client_order_id)
        {
            if existing.reprice.as_ref() != Some(reprice) {
                return Err(StoreError::InvalidPaperEffect(reprice.artifact_id.clone()));
            }
            return Ok(Some(existing.clone()));
        }
        let original = find_order(&orders, &action.prior_client_order_id)?;
        if !original_can_be_repriced(&transaction, original)?
            || effect_intent_cursor(&transaction, run_id, reprice)?.is_some()
            || orders
                .iter()
                .any(|order| order.state == PaperSubmissionState::PossiblySubmitted)
            || !assert_new_submission_allowed(&transaction, authority)?
        {
            return Ok(None);
        }
        append_effect_intent(&transaction, authority, reprice, now)?;
        let order = ready_order(
            action.asset,
            action.replacement_client_order_id,
            Some(reprice.clone()),
        );
        append_transition(&transaction, authority, &order, now)?;
        transaction.commit()?;
        Ok(Some(order))
    }

    /// Commit the only transition that authorizes a new POST or PATCH for this ID.
    pub fn admit_paper_submission(
        &self,
        authority: &PaperSubmissionAuthority<'_>,
        client_order_id: &str,
        authorization_expires_at: DateTime<Utc>,
    ) -> StoreResult<PaperSubmissionAdmission> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = Utc::now();
        let commitment = authorize_attempt(&transaction, authority, now)?;
        let run_id = &authority.permit.run_id;
        let Some(PaperSubmissionBatch::Journaled {
            observation_only: false,
            orders,
            ..
        }) = read_batch(&transaction, authority.commitment, run_id, &commitment)?
        else {
            return Ok(PaperSubmissionAdmission::ObserveOnly);
        };
        let order = find_order(&orders, client_order_id)?;
        if order.state != PaperSubmissionState::Ready
            || orders
                .iter()
                .any(|order| order.state == PaperSubmissionState::PossiblySubmitted)
            || !assert_new_submission_allowed(&transaction, authority)?
        {
            return Ok(PaperSubmissionAdmission::ObserveOnly);
        }
        if order.reprice.is_some() {
            let original_id = commitment
                .client_order_ids
                .get(&order.asset)
                .ok_or_else(|| {
                    StoreError::InvalidPaperEffect(authority.commitment.artifact_id.clone())
                })?;
            if !original_can_be_repriced(&transaction, find_order(&orders, original_id)?)? {
                return Ok(PaperSubmissionAdmission::ObserveOnly);
            }
        }
        // The adapter's authorization can expire while waiting for SQLite.
        // A caller-supplied bound may narrow, but cannot extend, frozen authority.
        let deadline = self
            .frozen_submission_deadline(&transaction, authority, &commitment)?
            .map_or(authorization_expires_at, |frozen| {
                frozen.min(authorization_expires_at)
            });
        let admitted_at = Utc::now();
        if admitted_at > deadline {
            return Ok(PaperSubmissionAdmission::ObserveOnly);
        }
        assert_daemon_lease(&transaction, authority.lease, admitted_at)?;
        assert_permit(&transaction, authority.permit)?;
        let admitted = next_transition(order, PaperSubmissionState::PossiblySubmitted)?;
        append_transition(&transaction, authority, &admitted, admitted_at)?;
        transaction.commit()?;
        Ok(PaperSubmissionAdmission::Admitted)
    }

    /// Commit one actual broker receipt before advancing to another broker write.
    /// Observations are accepted while frozen, under the current attempt fence.
    pub fn record_paper_submission_receipt(
        &self,
        authority: &PaperSubmissionAuthority<'_>,
        receipt: &OrderReceipt,
    ) -> StoreResult<()> {
        receipt.validate()?;
        let blob = self.stage_json(receipt)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = Utc::now();
        let commitment = authorize_attempt(&transaction, authority, now)?;
        let run_id = &authority.permit.run_id;
        let batch = read_batch(&transaction, authority.commitment, run_id, &commitment)?;
        let original =
            commitment.client_order_ids.get(&receipt.asset) == Some(&receipt.client_order_id);
        if receipt.plan_hash != commitment.plan_hash {
            return Err(invalid_receipt(&receipt.client_order_id));
        }
        let orders = match batch {
            Some(PaperSubmissionBatch::Journaled { orders, .. }) => orders,
            Some(PaperSubmissionBatch::LegacyUnknown) => {
                let reprice = if original {
                    None
                } else {
                    Some(historical_reprice_for_observation(
                        &transaction,
                        authority,
                        &commitment,
                        receipt,
                    )?)
                };
                let latest =
                    read_legacy_receipts(&transaction, authority.commitment, run_id, &commitment)?;
                if let Some((_, previous)) = latest.get(&receipt.client_order_id) {
                    if !receipt_advances(previous, receipt)? {
                        return Ok(());
                    }
                }
                // Legacy observations append CAS and an event only. No admission
                // state is inferred from an earlier effect or its broker response.
                append_raw_submission_receipt(
                    &transaction,
                    authority,
                    receipt,
                    blob,
                    reprice,
                    now,
                )?;
                transaction.commit()?;
                return Ok(());
            }
            None => {
                return Err(StoreError::MissingPaperEffectIntent(
                    authority.commitment.artifact_id.clone(),
                ))
            }
        };
        let existing = orders
            .iter()
            .find(|order| order.client_order_id == receipt.client_order_id);
        let historical;
        let order = if let Some(order) = existing {
            order
        } else {
            let reprice =
                historical_reprice_for_observation(&transaction, authority, &commitment, receipt)?;
            historical = ready_order(
                receipt.asset,
                receipt.client_order_id.clone(),
                Some(reprice),
            );
            &historical
        };
        if receipt.asset != order.asset || order.state == PaperSubmissionState::ProvenUnsubmitted {
            return Err(invalid_receipt(&receipt.client_order_id));
        }
        if let Some(previous) = &order.receipt {
            let previous = load_receipt(&transaction, previous)?;
            if !receipt_advances(&previous, receipt)? {
                return Ok(());
            }
        }
        let state = if receipt.state.is_final_without_successor() {
            PaperSubmissionState::Terminal
        } else {
            PaperSubmissionState::Observed
        };
        let mut observed = if existing.is_some() {
            next_transition(order, state)?
        } else {
            // Existing r1 effects from the old path enroll solely on concrete
            // broker evidence. They never gain an admission transition.
            let mut observed = order.clone();
            observed.state = state;
            observed
        };
        observed.receipt = Some(append_raw_submission_receipt(
            &transaction,
            authority,
            receipt,
            blob,
            order.reprice.clone(),
            now,
        )?);
        append_transition(&transaction, authority, &observed, now)?;
        transaction.commit()?;
        Ok(())
    }

    /// Permanently close Ready IDs using an independently verifiable stop.
    pub fn retire_ready_paper_submissions(
        &self,
        authority: &PaperSubmissionAuthority<'_>,
        stop: PaperSubmissionStop<'_>,
    ) -> StoreResult<Vec<UnsubmittedPaperOrder>> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = Utc::now();
        let commitment = authorize_attempt(&transaction, authority, now)?;
        let run_id = &authority.permit.run_id;
        let Some(PaperSubmissionBatch::Journaled {
            observation_only,
            orders,
            ..
        }) = read_batch(&transaction, authority.commitment, run_id, &commitment)?
        else {
            return Ok(Vec::new());
        };
        let orders = if observation_only {
            orders
        } else {
            let reason = match stop {
                PaperSubmissionStop::SubmissionWindowExpired => {
                    if self
                        .frozen_submission_deadline(&transaction, authority, &commitment)?
                        .is_none_or(|deadline| now <= deadline)
                    {
                        return Ok(Vec::new());
                    }
                    PaperUnsubmittedReason::SubmissionWindowExpired
                }
                PaperSubmissionStop::BatchObservationOnly {
                    client_order_id,
                    transition_index,
                } => {
                    let unknown = find_order(&orders, client_order_id)?;
                    if unknown.transition_index != transition_index
                        || unknown.state != PaperSubmissionState::PossiblySubmitted
                    {
                        return Err(StoreError::Integrity(
                            "Paper observation-only stop lacks an ambiguous admission".into(),
                        ));
                    }
                    PaperUnsubmittedReason::BatchObservationOnly
                }
                PaperSubmissionStop::RunCancelled => {
                    if !run_cancelled(&transaction, run_id)? {
                        return Err(StoreError::Integrity(
                            "Paper cancellation stop lacks a Run cancellation".into(),
                        ));
                    }
                    PaperUnsubmittedReason::RunCancelled
                }
            };
            close_ready_orders(&transaction, authority, &orders, reason, now)?
        };
        let proofs = orders.iter().filter_map(unsubmitted_witness).collect();
        transaction.commit()?;
        Ok(proofs)
    }

    // The adapter may expire sooner on market data freshness; these immutable
    // bounds independently prove that the submission window cannot reopen.
    fn frozen_submission_deadline(
        &self,
        transaction: &Transaction<'_>,
        authority: &PaperSubmissionAuthority<'_>,
        commitment: &PaperCommitment,
    ) -> StoreResult<Option<DateTime<Utc>>> {
        let run_id = &authority.permit.run_id;
        let artifact = read_artifact(transaction, &authority.commitment.artifact_id)?;
        let plan = self.validate_execution_commitment_lineage(
            transaction,
            &artifact,
            commitment,
            run_id,
            &commitment.broker_session,
        )?;
        let decision_artifact = read_artifact(transaction, &plan.decision_context.artifact_id)?;
        let decision: akzio_domain::DecisionContext =
            serde_json::from_slice(&blob::read_blob_with(transaction, &decision_artifact.blob)?)?;
        decision.validate()?;
        if decision_artifact.kind != ArtifactKind::DecisionContext || decision.run_id != *run_id {
            return Err(StoreError::InvalidPaperEffect(
                authority.commitment.artifact_id.clone(),
            ));
        }
        let clock_artifact = read_artifact(transaction, &plan.market_clock_snapshot.artifact_id)?;
        let clock: akzio_domain::MarketClockSnapshot =
            serde_json::from_slice(&blob::read_blob_with(transaction, &clock_artifact.blob)?)?;
        clock.validate()?;
        if clock_artifact.kind != ArtifactKind::NormalizedEvidence
            || clock.broker_session != commitment.broker_session
        {
            return Err(StoreError::InvalidPaperEffect(
                authority.commitment.artifact_id.clone(),
            ));
        }
        let expiry = lease::paper_approval_for_run_with_connection(transaction, run_id)?.map(
            |(manifest, approval)| {
                manifest
                    .expires_at
                    .min(approval.expires_at)
                    .min(approval.qualification.expires_at)
            },
        );
        Ok([
            decision.validity.map(|validity| validity.valid_until),
            clock.session.and_then(|session| session.ends_at),
            expiry,
        ]
        .into_iter()
        .flatten()
        .min())
    }

    /// Latest exact broker receipts appended while observing a legacy effect.
    /// These CAS observations grant no submission authority and survive retries.
    pub fn legacy_paper_submission_receipts(
        &self,
        commitment_ref: &ArtifactRef,
    ) -> StoreResult<Vec<(ArtifactRef, OrderReceipt)>> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let (run_id, commitment) = load_committed_paper(&transaction, commitment_ref)?;
        if !matches!(
            read_batch(&transaction, commitment_ref, &run_id, &commitment)?,
            Some(PaperSubmissionBatch::LegacyUnknown)
        ) {
            return Ok(Vec::new());
        }
        Ok(
            read_legacy_receipts(&transaction, commitment_ref, &run_id, &commitment)?
                .into_values()
                .collect(),
        )
    }

    /// Read the bounded latest order states on one SQLite snapshot.
    pub fn paper_submission_batch(
        &self,
        commitment: &ArtifactRef,
    ) -> StoreResult<Option<PaperSubmissionBatch>> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        paper_submission_batch_with_connection(&transaction, commitment)
    }
}

pub(super) fn paper_submission_batch_with_connection(
    connection: &Connection,
    reference: &ArtifactRef,
) -> StoreResult<Option<PaperSubmissionBatch>> {
    let (run_id, commitment) = load_committed_paper(connection, reference)?;
    read_batch(connection, reference, &run_id, &commitment)
}

/// Validate terminal evidence and close nested effects before closing the batch.
pub(super) fn settle_journaled_reprice_effects(
    transaction: &Transaction<'_>,
    permit: &TaskWritePermit,
    commitment_ref: &ArtifactRef,
    outputs: &[Artifact],
    now: DateTime<Utc>,
) -> StoreResult<()> {
    if commitment_ref.kind != ArtifactKind::ExecutionCommitment {
        return Ok(());
    }
    let invalid = || StoreError::InvalidPaperEffect(commitment_ref.artifact_id.clone());
    let (run_id, commitment) = load_committed_paper(transaction, commitment_ref)?;
    let batch =
        read_batch(transaction, commitment_ref, &run_id, &commitment)?.ok_or_else(invalid)?;
    let mut reconciliations = outputs
        .iter()
        .filter(|artifact| artifact.kind == ArtifactKind::Reconciliation);
    let artifact = reconciliations.next().ok_or_else(invalid)?;
    if reconciliations.next().is_some() {
        return Err(invalid());
    }
    let reconciliation: Reconciliation =
        serde_json::from_slice(&blob::read_blob_with(transaction, &artifact.blob)?)?;
    reconciliation.validate()?;
    if !artifact.source_refs.contains(commitment_ref)
        || reconciliation.commitment != *commitment_ref
        || reconciliation.state != ReconciliationState::Complete
    {
        return Err(invalid());
    }
    let PaperSubmissionBatch::Journaled { orders, .. } = batch else {
        if !reconciliation.unsubmitted_orders.is_empty() {
            return Err(invalid());
        }
        return Ok(());
    };
    let reprices = recorded_reprices(transaction, commitment_ref, &commitment, &run_id)?;
    for proof in &reconciliation.unsubmitted_orders {
        let order = find_order(&orders, &proof.client_order_id)?;
        if order.state != PaperSubmissionState::ProvenUnsubmitted
            || unsubmitted_witness(order).as_ref() != Some(proof)
        {
            return Err(invalid());
        }
    }
    let receipts = reconciliation
        .broker_receipts
        .iter()
        .map(|reference| {
            if !artifact.source_refs.contains(reference) {
                return Err(invalid());
            }
            let receipt_artifact = outputs
                .iter()
                .find(|output| {
                    output.kind == ArtifactKind::OrderReceipt
                        && output.artifact_id == reference.artifact_id
                })
                .ok_or_else(invalid)?;
            verify_journal_receipt(transaction, &orders, &reprices, receipt_artifact)
        })
        .collect::<StoreResult<Vec<_>>>()?;
    if reconciliation_state_for_paper_orders(
        commitment_ref,
        &commitment,
        &receipts,
        &reprices,
        &reconciliation.unsubmitted_orders,
    )? != ReconciliationState::Complete
    {
        return Err(invalid());
    }
    for (reference, _) in reprices.values() {
        if !paper_effect_terminal_exists(transaction, &run_id, &reference.artifact_id)? {
            append_event(
                transaction,
                &run_id,
                Some(&permit.task_id),
                Some(&permit.attempt_id),
                LifecycleEventType::ExecutionEffectRecovered,
                Some(&reference.artifact_id),
                now,
            )?;
        }
    }
    Ok(())
}

fn recorded_reprices(
    connection: &Connection,
    commitment_ref: &ArtifactRef,
    commitment: &PaperCommitment,
    run_id: &RunId,
) -> StoreResult<BTreeMap<Asset, (ArtifactRef, PaperReprice)>> {
    let mut statement = connection.prepare(
        "SELECT reprice_artifact_id FROM rebuild_execution_reprices WHERE commitment_artifact_id = ?1",
    )?;
    let ids = statement.query_map(params![commitment_ref.artifact_id.0.as_str()], |row| {
        row.get::<_, String>(0)
    })?;
    let mut reprices = BTreeMap::new();
    for id in ids {
        let reference = ArtifactRef {
            artifact_id: ArtifactId(ContentHash::new(id?)?),
            kind: ArtifactKind::ExecutionReprice,
        };
        // A durable action is derived before admission. Only a durable effect
        // can require eventual replacement observation or a journal witness.
        if effect_intent_cursor(connection, run_id, &reference)?.is_none() {
            continue;
        }
        let reprice = read_reprice(connection, commitment_ref, commitment, &reference)?;
        reprices.insert(reprice.asset, (reference, reprice));
    }
    Ok(reprices)
}

fn append_raw_submission_receipt(
    transaction: &Transaction<'_>,
    authority: &PaperSubmissionAuthority<'_>,
    receipt: &OrderReceipt,
    blob: BlobRef,
    reprice: Option<ArtifactRef>,
    now: DateTime<Utc>,
) -> StoreResult<ArtifactRef> {
    let mut source_refs = vec![authority.commitment.clone()];
    source_refs.extend(reprice);
    let artifact = Artifact::new(
        ArtifactKind::OrderReceipt,
        blob,
        RAW_SUBMISSION_RECEIPT_PRODUCER,
        ArtifactLifecycle::Canonical,
        ArtifactProvenance {
            source_family: "akzio.execution".into(),
            observed_at: Some(receipt.observed_at),
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: authority.permit.contract_hash.clone(),
        },
        Some(authority.permit.artifact_origin()),
        source_refs,
        now,
    )?;
    insert_artifact(transaction, &artifact)?;
    append_event(
        transaction,
        &authority.permit.run_id,
        Some(&authority.permit.task_id),
        Some(&authority.permit.attempt_id),
        LifecycleEventType::ArtifactCommitted,
        Some(&artifact.artifact_id),
        now,
    )?;
    Ok(ArtifactRef {
        artifact_id: artifact.artifact_id,
        kind: ArtifactKind::OrderReceipt,
    })
}

fn read_legacy_receipts(
    connection: &Connection,
    commitment_ref: &ArtifactRef,
    run_id: &RunId,
    commitment: &PaperCommitment,
) -> StoreResult<BTreeMap<String, (ArtifactRef, OrderReceipt)>> {
    let invalid = || StoreError::InvalidPaperEffect(commitment_ref.artifact_id.clone());
    let intent = effect_intent_cursor(connection, run_id, commitment_ref)?.ok_or_else(invalid)?;
    let reprices = recorded_reprices(connection, commitment_ref, commitment, run_id)?;
    let mut statement = connection.prepare(
        "SELECT a.artifact_id
         FROM rebuild_events AS e
         JOIN rebuild_artifacts AS a ON a.artifact_id = e.artifact_id
         JOIN rebuild_artifact_refs AS r ON r.artifact_id = a.artifact_id
         WHERE e.run_id = ?1 AND e.event_id > ?2 AND e.event_type = ?3
           AND r.source_artifact_id = ?4 AND a.kind = ?5 AND a.producer = ?6
           AND json_extract(a.origin_json, '$.run_id') = e.run_id
           AND json_extract(a.origin_json, '$.task_id') = e.task_id
           AND json_extract(a.origin_json, '$.attempt_id') = e.attempt_id
         ORDER BY e.event_id DESC",
    )?;
    let ids = statement.query_map(
        params![
            run_id.0,
            intent,
            LifecycleEventType::ArtifactCommitted.as_str(),
            commitment_ref.artifact_id.0.as_str(),
            enum_name(ArtifactKind::OrderReceipt),
            RAW_SUBMISSION_RECEIPT_PRODUCER,
        ],
        |row| row.get::<_, String>(0),
    )?;
    let mut latest = BTreeMap::new();
    for id in ids {
        let artifact = read_artifact(connection, &ArtifactId(ContentHash::new(id?)?))?;
        artifact.validate()?;
        let receipt: OrderReceipt =
            serde_json::from_slice(&blob::read_blob_with(connection, &artifact.blob)?)?;
        receipt.validate()?;
        let mut sources = vec![commitment_ref.clone()];
        if commitment.client_order_ids.get(&receipt.asset) != Some(&receipt.client_order_id) {
            let (reference, reprice) = reprices.get(&receipt.asset).ok_or_else(invalid)?;
            if reprice.replacement_client_order_id != receipt.client_order_id {
                return Err(invalid());
            }
            sources.push(reference.clone());
        }
        if artifact.lifecycle != ArtifactLifecycle::Canonical
            || artifact.provenance.observed_at != Some(receipt.observed_at)
            || !has_exact_source_refs(&artifact, &sources)
            || receipt.plan_hash != commitment.plan_hash
        {
            return Err(invalid());
        }
        latest.entry(receipt.client_order_id.clone()).or_insert((
            ArtifactRef {
                artifact_id: artifact.artifact_id,
                kind: ArtifactKind::OrderReceipt,
            },
            receipt,
        ));
        if latest.len() == commitment.client_order_ids.len() + reprices.len() {
            break;
        }
    }
    Ok(latest)
}

fn verify_journal_receipt(
    connection: &Connection,
    orders: &[PaperSubmissionOrder],
    reprices: &BTreeMap<Asset, (ArtifactRef, PaperReprice)>,
    artifact: &Artifact,
) -> StoreResult<OrderReceipt> {
    let receipt: OrderReceipt =
        serde_json::from_slice(&blob::read_blob_with(connection, &artifact.blob)?)?;
    receipt.validate()?;
    let order = find_order(orders, &receipt.client_order_id)?;
    let invalid = || invalid_receipt(&receipt.client_order_id);
    let raw_ref = order.receipt.as_ref().ok_or_else(invalid)?;
    if order.state != PaperSubmissionState::Terminal || !artifact.source_refs.contains(raw_ref) {
        return Err(invalid());
    }
    let raw = load_receipt(connection, raw_ref)?;
    if !raw.state.is_final_without_successor() {
        return Err(invalid());
    }
    let mut expected = if let Some(reprice_ref) = &order.reprice {
        let (expected_ref, reprice) = reprices.get(&order.asset).ok_or_else(invalid)?;
        if reprice_ref != expected_ref || !artifact.source_refs.contains(reprice_ref) {
            return Err(invalid());
        }
        let original = find_order(orders, &reprice.prior_client_order_id)?;
        let original_ref = original.receipt.as_ref().ok_or_else(invalid)?;
        if !artifact.source_refs.contains(original_ref) {
            return Err(invalid());
        }
        let prior = load_receipt(connection, original_ref)?;
        // The replacement's terminal status cannot settle an original order
        // which the broker still reports open.
        if !prior.state.is_final_without_successor()
            && prior.state != akzio_domain::OrderReceiptState::Replaced
        {
            return Err(invalid_receipt(&original.client_order_id));
        }
        normalize_paper_replacement_receipt(reprice, &prior, &raw)?
    } else {
        raw
    };
    // Reconciliation carries its own observation timestamp; the immutable
    // raw artifact keeps actual adapter receipt time and broker update time.
    expected.observed_at = receipt.observed_at;
    if receipt != expected {
        return Err(invalid());
    }
    Ok(receipt)
}

fn authorize_attempt(
    transaction: &Transaction<'_>,
    authority: &PaperSubmissionAuthority<'_>,
    now: DateTime<Utc>,
) -> StoreResult<PaperCommitment> {
    assert_daemon_lease(transaction, authority.lease, now)?;
    assert_permit(transaction, authority.permit)?;
    let (run_id, commitment) = load_committed_paper(transaction, authority.commitment)?;
    if run_id != authority.permit.run_id {
        return Err(StoreError::InvalidPaperEffect(
            authority.commitment.artifact_id.clone(),
        ));
    }
    Ok(commitment)
}

fn load_committed_paper(
    connection: &Connection,
    reference: &ArtifactRef,
) -> StoreResult<(RunId, PaperCommitment)> {
    let invalid = || StoreError::InvalidPaperEffect(reference.artifact_id.clone());
    let artifact = read_artifact(connection, &reference.artifact_id)?;
    artifact.validate()?;
    if reference.kind != ArtifactKind::ExecutionCommitment
        || artifact.kind != reference.kind
        || artifact.lifecycle != ArtifactLifecycle::Canonical
    {
        return Err(invalid());
    }
    let run_id = artifact
        .origin
        .as_ref()
        .and_then(|origin| origin.run_id.clone())
        .ok_or_else(invalid)?;
    let commitment: PaperCommitment =
        serde_json::from_slice(&blob::read_blob_with(connection, &artifact.blob)?)?;
    commitment.validate()?;
    let exact_slot: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM rebuild_session_slots
             WHERE session_key = ?1 AND run_id = ?2 AND commitment_artifact_id = ?3)",
        params![
            commitment.broker_session,
            run_id.0,
            reference.artifact_id.0.as_str()
        ],
        |row| row.get(0),
    )?;
    if !exact_slot
        || !artifact.source_refs.contains(&commitment.execution_context)
        || run_purpose_from_connection(connection, &run_id)? != RunPurpose::Paper
    {
        return Err(invalid());
    }
    Ok((run_id, commitment))
}

fn assert_new_submission_allowed(
    transaction: &Transaction<'_>,
    authority: &PaperSubmissionAuthority<'_>,
) -> StoreResult<bool> {
    let run_id = &authority.permit.run_id;
    debug::assert_broker_write(transaction, run_id)?;
    if run_cancelled(transaction, run_id)?
        || paper_effect_terminal_exists(transaction, run_id, &authority.commitment.artifact_id)?
    {
        return Ok(false);
    }
    if let Some(state) = Store::read_execution_freeze_state_with_connection(transaction)?
        .filter(|state| state.frozen)
    {
        return Err(StoreError::ExecutionFrozen {
            reason: state.reason,
        });
    }
    Ok(true)
}

fn run_cancelled(connection: &Connection, run_id: &RunId) -> StoreResult<bool> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM rebuild_run_cancellations WHERE run_id = ?1)",
        params![run_id.0],
        |row| row.get(0),
    )?)
}

fn append_effect_intent(
    transaction: &Transaction<'_>,
    authority: &PaperSubmissionAuthority<'_>,
    effect: &ArtifactRef,
    now: DateTime<Utc>,
) -> StoreResult<i64> {
    append_event(
        transaction,
        &authority.permit.run_id,
        Some(&authority.permit.task_id),
        Some(&authority.permit.attempt_id),
        LifecycleEventType::ExecutionEffectIntent,
        Some(&effect.artifact_id),
        now,
    )
}

fn effect_intent_cursor(
    connection: &Connection,
    run_id: &RunId,
    effect: &ArtifactRef,
) -> StoreResult<Option<i64>> {
    let (count, cursor) = connection.query_row(
        "SELECT COUNT(*), MIN(event_id) FROM rebuild_events
           WHERE run_id = ?1 AND event_type = ?2 AND artifact_id = ?3",
        params![
            run_id.0,
            LifecycleEventType::ExecutionEffectIntent.as_str(),
            effect.artifact_id.0.as_str()
        ],
        |row| Ok((row.get::<_, u32>(0)?, row.get::<_, Option<i64>>(1)?)),
    )?;
    if count > 1 {
        return Err(StoreError::InvalidPaperEffect(effect.artifact_id.clone()));
    }
    Ok(cursor)
}

fn read_batch(
    connection: &Connection,
    reference: &ArtifactRef,
    run_id: &RunId,
    commitment: &PaperCommitment,
) -> StoreResult<Option<PaperSubmissionBatch>> {
    let cursor = effect_intent_cursor(connection, run_id, reference)?;
    if read_store_schema_version(connection)?.as_deref() == Some("18") {
        return Ok(cursor.map(|_| PaperSubmissionBatch::LegacyUnknown));
    }
    let header = connection
        .query_row(
            "SELECT run_id, effect_intent_cursor, stop_reason FROM rebuild_paper_submission_batches
         WHERE commitment_artifact_id = ?1",
            params![reference.artifact_id.0.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .optional()?;
    let Some((stored_run, stored_cursor, stop_reason)) = header else {
        return Ok(cursor.map(|_| PaperSubmissionBatch::LegacyUnknown));
    };
    if stored_run != run_id.0 || Some(stored_cursor) != cursor {
        return Err(StoreError::InvalidPaperEffect(
            reference.artifact_id.clone(),
        ));
    }
    let orders = latest_orders(connection, reference)?;
    let stopped: Option<PaperUnsubmittedReason> =
        stop_reason.as_deref().map(parse_enum).transpose()?;
    if orders.iter().any(|order| {
        (stopped.is_some() && order.state == PaperSubmissionState::Ready)
            || (order.state == PaperSubmissionState::ProvenUnsubmitted && order.reason != stopped)
    }) {
        return Err(StoreError::InvalidPaperEffect(
            reference.artifact_id.clone(),
        ));
    }
    for (asset, id) in &commitment.client_order_ids {
        let order = find_order(&orders, id)?;
        if order.asset != *asset || order.reprice.is_some() {
            return Err(StoreError::InvalidPaperEffect(
                reference.artifact_id.clone(),
            ));
        }
    }
    for order in &orders {
        if let Some(reprice) = &order.reprice {
            let action = read_reprice(connection, reference, commitment, reprice)?;
            if action.asset != order.asset
                || action.replacement_client_order_id != order.client_order_id
                || effect_intent_cursor(connection, run_id, reprice)?.is_none()
            {
                return Err(StoreError::InvalidPaperEffect(reprice.artifact_id.clone()));
            }
        } else if commitment.client_order_ids.get(&order.asset) != Some(&order.client_order_id) {
            return Err(StoreError::InvalidPaperEffect(
                reference.artifact_id.clone(),
            ));
        }
    }
    Ok(Some(PaperSubmissionBatch::Journaled {
        recovered: true,
        observation_only: stop_reason.is_some(),
        orders,
    }))
}

fn find_order<'a>(
    orders: &'a [PaperSubmissionOrder],
    client_order_id: &str,
) -> StoreResult<&'a PaperSubmissionOrder> {
    orders
        .iter()
        .find(|order| order.client_order_id == client_order_id)
        .ok_or_else(|| {
            StoreError::Integrity(format!(
                "Paper submission journal lacks client order {client_order_id}"
            ))
        })
}

fn read_reprice(
    connection: &Connection,
    commitment_ref: &ArtifactRef,
    commitment: &PaperCommitment,
    reprice: &ArtifactRef,
) -> StoreResult<PaperReprice> {
    let invalid = || StoreError::InvalidPaperEffect(reprice.artifact_id.clone());
    let artifact = read_artifact(connection, &reprice.artifact_id)?;
    artifact.validate()?;
    if reprice.kind != ArtifactKind::ExecutionReprice
        || artifact.kind != reprice.kind
        || artifact.lifecycle != ArtifactLifecycle::Canonical
        || !artifact.source_refs.contains(commitment_ref)
    {
        return Err(invalid());
    }
    let action: PaperReprice =
        serde_json::from_slice(&blob::read_blob_with(connection, &artifact.blob)?)?;
    action.validate()?;
    let indexed: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM rebuild_execution_reprices
             WHERE commitment_artifact_id = ?1 AND asset = ?2 AND reprice_artifact_id = ?3)",
        params![
            commitment_ref.artifact_id.0.as_str(),
            action.asset.symbol(),
            reprice.artifact_id.0.as_str()
        ],
        |row| row.get(0),
    )?;
    if !indexed
        || action.commitment != *commitment_ref
        || commitment.client_order_ids.get(&action.asset) != Some(&action.prior_client_order_id)
    {
        return Err(invalid());
    }
    Ok(action)
}

fn historical_reprice_for_observation(
    connection: &Connection,
    authority: &PaperSubmissionAuthority<'_>,
    commitment: &PaperCommitment,
    receipt: &OrderReceipt,
) -> StoreResult<ArtifactRef> {
    let artifact_id = connection
        .query_row(
            "SELECT reprice_artifact_id FROM rebuild_execution_reprices
         WHERE commitment_artifact_id = ?1 AND asset = ?2",
            params![
                authority.commitment.artifact_id.0.as_str(),
                receipt.asset.symbol()
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or_else(|| invalid_receipt(&receipt.client_order_id))?;
    let reference = ArtifactRef {
        artifact_id: ArtifactId(ContentHash::new(artifact_id)?),
        kind: ArtifactKind::ExecutionReprice,
    };
    let reprice = read_reprice(connection, authority.commitment, commitment, &reference)?;
    if reprice.replacement_client_order_id != receipt.client_order_id
        || effect_intent_cursor(connection, &authority.permit.run_id, &reference)?.is_none()
    {
        return Err(invalid_receipt(&receipt.client_order_id));
    }
    Ok(reference)
}

fn latest_orders(
    connection: &Connection,
    reference: &ArtifactRef,
) -> StoreResult<Vec<PaperSubmissionOrder>> {
    let mut statement = connection.prepare(
        "SELECT current.asset, current.client_order_id, current.transition_index, current.state,
                current.reprice_artifact_id, current.receipt_artifact_id, current.reason
         FROM rebuild_paper_submission_transitions AS current
         WHERE current.commitment_artifact_id = ?1 AND NOT EXISTS (
             SELECT 1 FROM rebuild_paper_submission_transitions AS newer
             WHERE newer.commitment_artifact_id = current.commitment_artifact_id
               AND newer.client_order_id = current.client_order_id
               AND newer.transition_index > current.transition_index
         ) ORDER BY current.asset, current.client_order_id",
    )?;
    let mut rows = statement.query(params![reference.artifact_id.0.as_str()])?;
    let mut orders = Vec::new();
    while let Some(row) = rows.next()? {
        let artifact_ref = |index: usize, kind| -> StoreResult<Option<ArtifactRef>> {
            row.get::<_, Option<String>>(index)?
                .map(|id| {
                    Ok(ArtifactRef {
                        artifact_id: ArtifactId(ContentHash::new(id)?),
                        kind,
                    })
                })
                .transpose()
        };
        orders.push(PaperSubmissionOrder {
            asset: Asset::try_from(row.get::<_, String>(0)?.as_str())?,
            client_order_id: row.get(1)?,
            transition_index: row.get(2)?,
            state: parse_enum(&row.get::<_, String>(3)?)?,
            reprice: artifact_ref(4, ArtifactKind::ExecutionReprice)?,
            receipt: artifact_ref(5, ArtifactKind::OrderReceipt)?,
            reason: row
                .get::<_, Option<String>>(6)?
                .map(|reason| parse_enum(&reason))
                .transpose()?,
        });
    }
    Ok(orders)
}

fn ready_order(
    asset: Asset,
    client_order_id: String,
    reprice: Option<ArtifactRef>,
) -> PaperSubmissionOrder {
    PaperSubmissionOrder {
        asset,
        client_order_id,
        transition_index: 0,
        state: PaperSubmissionState::Ready,
        reprice,
        receipt: None,
        reason: None,
    }
}

fn next_transition(
    order: &PaperSubmissionOrder,
    state: PaperSubmissionState,
) -> StoreResult<PaperSubmissionOrder> {
    let mut next = order.clone();
    next.transition_index = order.transition_index.checked_add(1).ok_or_else(|| {
        StoreError::Integrity("Paper submission journal transition index exhausted".into())
    })?;
    next.state = state;
    next.receipt = None;
    next.reason = None;
    Ok(next)
}

fn append_transition(
    transaction: &Transaction<'_>,
    authority: &PaperSubmissionAuthority<'_>,
    order: &PaperSubmissionOrder,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    transaction.execute(
        "INSERT INTO rebuild_paper_submission_transitions (
             commitment_artifact_id, client_order_id, transition_index, asset, reprice_artifact_id,
             state, daemon_lease_epoch, attempt_id, receipt_artifact_id, reason, recorded_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            authority.commitment.artifact_id.0.as_str(),
            order.client_order_id,
            order.transition_index,
            order.asset.symbol(),
            order
                .reprice
                .as_ref()
                .map(|reference| reference.artifact_id.0.as_str()),
            enum_name(order.state),
            authority.lease.epoch,
            authority.permit.attempt_id.0,
            order
                .receipt
                .as_ref()
                .map(|reference| reference.artifact_id.0.as_str()),
            order.reason.map(enum_name),
            now.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn close_ready_orders(
    transaction: &Transaction<'_>,
    authority: &PaperSubmissionAuthority<'_>,
    orders: &[PaperSubmissionOrder],
    reason: PaperUnsubmittedReason,
    now: DateTime<Utc>,
) -> StoreResult<Vec<PaperSubmissionOrder>> {
    transaction.execute(
        "UPDATE rebuild_paper_submission_batches SET stop_reason = ?1 WHERE commitment_artifact_id = ?2",
        params![enum_name(reason), authority.commitment.artifact_id.0.as_str()],
    )?;
    let mut stopped = orders.to_vec();
    for order in &mut stopped {
        if order.state == PaperSubmissionState::Ready {
            *order = next_transition(order, PaperSubmissionState::ProvenUnsubmitted)?;
            order.reason = Some(reason);
            append_transition(transaction, authority, order, now)?;
        }
    }
    Ok(stopped)
}

fn unsubmitted_witness(order: &PaperSubmissionOrder) -> Option<UnsubmittedPaperOrder> {
    let reason = order.reason?;
    Some(UnsubmittedPaperOrder {
        asset: order.asset,
        client_order_id: order.client_order_id.clone(),
        transition_index: order.transition_index,
        reason,
        reprice: order.reprice.clone(),
    })
}

fn invalid_receipt(client_order_id: &str) -> StoreError {
    StoreError::Integrity(format!("invalid Paper receipt for {client_order_id}"))
}

fn load_receipt(connection: &Connection, reference: &ArtifactRef) -> StoreResult<OrderReceipt> {
    let artifact = read_artifact(connection, &reference.artifact_id)?;
    artifact.validate()?;
    if artifact.kind != ArtifactKind::OrderReceipt
        || artifact.lifecycle != ArtifactLifecycle::Canonical
    {
        return Err(StoreError::InvalidPaperEffect(
            reference.artifact_id.clone(),
        ));
    }
    let receipt: OrderReceipt =
        serde_json::from_slice(&blob::read_blob_with(connection, &artifact.blob)?)?;
    receipt.validate()?;
    Ok(receipt)
}

fn original_can_be_repriced(
    connection: &Connection,
    original: &PaperSubmissionOrder,
) -> StoreResult<bool> {
    if original.state != PaperSubmissionState::Observed {
        return Ok(false);
    }
    let reference = original
        .receipt
        .as_ref()
        .ok_or_else(|| invalid_receipt(&original.client_order_id))?;
    let receipt = load_receipt(connection, reference)?;
    Ok(matches!(
        receipt.state,
        akzio_domain::OrderReceiptState::Accepted
            | akzio_domain::OrderReceiptState::PartiallyFilled
    ))
}

fn receipt_advances(previous: &OrderReceipt, receipt: &OrderReceipt) -> StoreResult<bool> {
    if previous.asset != receipt.asset
        || previous.plan_hash != receipt.plan_hash
        || previous.client_order_id != receipt.client_order_id
        || previous.broker_order_id != receipt.broker_order_id
        || previous.requested_quantity_micros != receipt.requested_quantity_micros
    {
        return Err(StoreError::PaperReceiptConflict(
            receipt.client_order_id.clone(),
        ));
    }
    if receipt.broker_updated_at < previous.broker_updated_at {
        return Ok(false);
    }
    let mut comparable = receipt.clone();
    comparable.observed_at = previous.observed_at;
    if previous.state.is_final_without_successor() {
        comparable.broker_updated_at = previous.broker_updated_at;
    }
    if &comparable == previous {
        return Ok(false);
    }
    if previous.state.is_final_without_successor()
        || receipt.broker_updated_at == previous.broker_updated_at
        || receipt.filled_quantity_micros < previous.filled_quantity_micros
    {
        return Err(StoreError::PaperReceiptConflict(
            receipt.client_order_id.clone(),
        ));
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::workflow::retired_debug_execution_tests::formal_fixture;
    use akzio_domain::{
        DecisionId, DependencyClosure, FactorExposure, HardBlocker, MandateAssessment,
        MarketClockSnapshot, MoneyMicros, OrderIntent, OrderReceiptState, PaperCommitmentId,
        PaperRepriceId, PreTradeSafetySnapshot, TargetPortfolio,
    };

    struct Fixture {
        store: Store,
        lease: DaemonLease,
        permit: TaskWritePermit,
        commitment: ArtifactRef,
        plan_hash: ContentHash,
    }

    impl Fixture {
        fn new() -> Self {
            Self::with_frozen_deadline(Utc::now() + Duration::minutes(5))
        }

        fn with_frozen_deadline(valid_until: DateTime<Utc>) -> Self {
            let (store, run_id, graph_id, _) = formal_fixture();
            let now = Utc::now();
            let lease = store
                .acquire_daemon_lease("scheduler", "journal", now, now + Duration::minutes(5))
                .unwrap()
                .unwrap();
            let permit = store
                .claim_next_task_for_workload(
                    "journal",
                    now,
                    Duration::minutes(2),
                    TaskWorkload::Session,
                )
                .unwrap()
                .unwrap()
                .permit;
            let session_key = now.date_naive().to_string();
            let hash = ContentHash::of_bytes(b"fixture policy");
            let target = TargetPortfolio::zeroed();
            let decision_at = valid_until - Duration::minutes(5);
            let mut artifacts = Vec::new();
            let mut stage = |kind, payload: serde_json::Value, sources| {
                let artifact = fixture_artifact(&store, &permit, kind, &payload, sources);
                let artifact_ref = reference(&artifact);
                artifacts.push(artifact);
                artifact_ref
            };
            let decision = stage(
                ArtifactKind::DecisionContext,
                serde_json::json!({
                    "schema_version": DOMAIN_SCHEMA_VERSION,
                    "decision_id": DecisionId::new(), "run_id": run_id,
                    "claims": [], "critiques": [], "evidence": [], "policy_influences": [],
                    "material_conflicts": [], "hard_blockers": [HardBlocker::MissingEvidence],
                    "soft_warnings": [], "decision_policy_hash": hash, "target": target,
                    "created_at": decision_at,
                    "validity": {
                        "evidence_cutoff": decision_at, "generated_at": decision_at,
                        "valid_until": valid_until, "maximum_execution_delay_ms": 300_000,
                        "market_state_hash": hash,
                    },
                }),
                vec![],
            );
            let account = stage(
                ArtifactKind::NormalizedEvidence,
                serde_json::json!("account"),
                vec![],
            );
            let quotes = stage(
                ArtifactKind::NormalizedEvidence,
                serde_json::json!("quotes"),
                vec![],
            );
            let clock = stage(
                ArtifactKind::NormalizedEvidence,
                serde_json::to_value(MarketClockSnapshot {
                    session: None,
                    schema_version: DOMAIN_SCHEMA_VERSION,
                    broker_session: session_key.clone(),
                    is_open: true,
                    observed_at: decision_at,
                })
                .unwrap(),
                vec![],
            );
            let mut plan = ExecutionPlan {
                schema_version: DOMAIN_SCHEMA_VERSION,
                decision_context: decision.clone(),
                account_snapshot: account.clone(),
                quote_snapshot: quotes.clone(),
                market_clock_snapshot: clock.clone(),
                policy_hash: hash.clone(),
                maximum_total_notional: MoneyMicros(50_000_000),
                factor_exposure: FactorExposure::from_target(&target).unwrap(),
                target,
                orders: [Asset::Qqq, Asset::Soxx]
                    .into_iter()
                    .map(|asset| OrderIntent {
                        extended_hours: false,
                        asset,
                        side: OrderSide::Sell,
                        notional: MoneyMicros(25_000_000),
                        limit_price: MoneyMicros(25_000_000),
                    })
                    .collect(),
                gross_exposure_ppm: 0,
                net_exposure_ppm: 0,
                turnover_ppm: 0,
                broker_session: session_key.clone(),
                created_at: decision_at,
                plan_hash: hash.clone(),
            };
            plan.refresh_hash().unwrap();
            let plan_hash = plan.plan_hash.clone();
            let mut sources = vec![
                decision.clone(),
                account.clone(),
                quotes.clone(),
                clock.clone(),
            ];
            let plan_ref = stage(
                ArtifactKind::ExecutionPlan,
                serde_json::to_value(&plan).unwrap(),
                sources.clone(),
            );
            sources.push(plan_ref.clone());
            let context_ref = stage(
                ArtifactKind::ExecutionContext,
                serde_json::to_value(ExecutionContext {
                    schema_version: DOMAIN_SCHEMA_VERSION,
                    run_id: run_id.clone(),
                    decision_context: decision,
                    account_snapshot: Some(account),
                    quote_snapshot: Some(quotes),
                    market_clock_snapshot: Some(clock),
                    execution_plan: Some(plan_ref),
                    factor_exposure: Some(plan.factor_exposure),
                    turnover_ppm: Some(0),
                    plan_hash: Some(plan_hash.clone()),
                    broker_session: Some(session_key.clone()),
                    mandate_assessment: Some(MandateAssessment {
                        mandate_hash: hash,
                        violations: Default::default(),
                        distance_before_ppm: 0,
                        distance_after_ppm: 0,
                    }),
                    pretrade_safety: Some(PreTradeSafetySnapshot {
                        ordered_assets: [Asset::Qqq, Asset::Soxx].into(),
                        information_classifications: Default::default(),
                        recent_compliance_activity: Default::default(),
                        dependency_closure: DependencyClosure {
                            captured_at: decision_at,
                            dependencies: Default::default(),
                        },
                        capacity_scenario: None,
                        missing_average_daily_dollar_volume: Default::default(),
                        compliance_violations_by_asset: [
                            (Asset::Qqq, Default::default()),
                            (Asset::Soxx, Default::default()),
                        ]
                        .into(),
                        missing_information_classifications: Default::default(),
                        risk_increasing: false,
                        dependency_permits_new_risk: false,
                        dependency_permits_risk_reduction: true,
                        permits_execution: true,
                        permits_risk_increasing: false,
                    }),
                    final_process_quality: None,
                    frozen: false,
                    created_at: decision_at,
                })
                .unwrap(),
                sources,
            );
            let verdict = stage(
                ArtifactKind::ExecutionVerdict,
                serde_json::to_value(ExecutionVerdict::Accepted {
                    execution_context: context_ref.clone(),
                })
                .unwrap(),
                vec![context_ref.clone()],
            );
            let commitment = fixture_artifact(
                &store,
                &permit,
                ArtifactKind::ExecutionCommitment,
                &PaperCommitment {
                    commitment_id: PaperCommitmentId::new(),
                    execution_context: context_ref.clone(),
                    plan_hash: plan_hash.clone(),
                    broker_session: session_key.clone(),
                    client_order_ids: [
                        (Asset::Qqq, "qqq-r0".into()),
                        (Asset::Soxx, "soxx-r0".into()),
                    ]
                    .into(),
                    created_at: now,
                },
                vec![context_ref, verdict],
            );
            // Upstream accepted slot and immutable identity are fixture inputs.
            // Journal operations below use real Store entry points and fences.
            {
                let mut connection = store.connection().unwrap();
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .unwrap();
                for artifact in &artifacts {
                    insert_artifact(&transaction, artifact).unwrap();
                }
                insert_artifact(&transaction, &commitment).unwrap();
                transaction.execute(
                    "INSERT INTO rebuild_session_slots (session_key, run_id, topology_id, graph_artifact_id,
                         run_created_at, scheduler_epoch, reserved_at, commitment_artifact_id, committed_at)
                     VALUES (?1, ?2, 'retired-debug-execution-test', ?3, ?4, ?5, ?4, ?6, ?4)",
                    params![session_key, run_id.0, graph_id.0.as_str(), now.to_rfc3339(), lease.epoch, commitment.artifact_id.0.as_str()],
                ).unwrap();
                transaction.commit().unwrap();
            }
            Self {
                store,
                lease,
                permit,
                commitment: reference(&commitment),
                plan_hash,
            }
        }

        fn authority(&self) -> PaperSubmissionAuthority<'_> {
            PaperSubmissionAuthority {
                lease: &self.lease,
                permit: &self.permit,
                commitment: &self.commitment,
            }
        }

        fn reprice(&self, prior_receipt: ArtifactRef, now: DateTime<Utc>) -> ArtifactRef {
            let action = fixture_artifact(
                &self.store,
                &self.permit,
                ArtifactKind::ExecutionReprice,
                &PaperReprice {
                    schema_version: DOMAIN_SCHEMA_VERSION,
                    reprice_id: PaperRepriceId::new(),
                    commitment: self.commitment.clone(),
                    prior_receipt: prior_receipt.clone(),
                    asset: Asset::Qqq,
                    prior_client_order_id: "qqq-r0".into(),
                    replacement_client_order_id: "qqq-r1".into(),
                    prior_broker_order_id: "broker-qqq".into(),
                    replacement_limit_price: MoneyMicros(25_000_000),
                    created_at: now,
                },
                vec![self.commitment.clone(), prior_receipt],
            );
            self.store
                .commit_execution_reprice_intent(&self.lease, &self.permit, &action, now)
                .unwrap();
            reference(&action)
        }

        fn receipt(
            &self,
            state: OrderReceiptState,
            filled: i64,
            observed_at: DateTime<Utc>,
        ) -> OrderReceipt {
            OrderReceipt {
                plan_hash: self.plan_hash.clone(),
                asset: Asset::Qqq,
                client_order_id: "qqq-r0".into(),
                broker_order_id: "broker-qqq".into(),
                state,
                requested_quantity_micros: 1_000_000,
                filled_quantity_micros: filled,
                remaining_quantity_micros: 1_000_000 - filled,
                average_fill_price: (filled != 0).then_some(MoneyMicros(25_000_000)),
                broker_updated_at: observed_at,
                reason: None,
                observed_at,
            }
        }
    }

    fn fixture_artifact<T: Serialize>(
        store: &Store,
        permit: &TaskWritePermit,
        kind: ArtifactKind,
        payload: &T,
        refs: Vec<ArtifactRef>,
    ) -> Artifact {
        let now = Utc::now();
        Artifact::new(
            kind,
            store.stage_json(payload).unwrap(),
            "test.paper_submission",
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: "akzio.test".into(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            Some(permit.artifact_origin()),
            refs,
            now,
        )
        .unwrap()
    }

    fn reference(artifact: &Artifact) -> ArtifactRef {
        ArtifactRef {
            artifact_id: artifact.artifact_id.clone(),
            kind: artifact.kind,
        }
    }

    fn journal_orders(batch: PaperSubmissionBatch) -> Vec<PaperSubmissionOrder> {
        let PaperSubmissionBatch::Journaled { orders, .. } = batch else {
            panic!("expected a journaled batch");
        };
        orders
    }

    /// WHY THIS TEST EXISTS (Task #30): Only time still valid inside the SQLite
    /// transaction can authorize a new send. Retirement requires a frozen bound.
    /// Do not weaken or delete these assertions.
    #[test]
    fn elapsed_adapter_deadline_blocks_admission_without_proving_non_submission() {
        let fixture = Fixture::new();
        let authority = fixture.authority();
        fixture
            .store
            .begin_paper_submission_batch(&authority)
            .unwrap();
        assert_eq!(
            fixture
                .store
                .admit_paper_submission(&authority, "qqq-r0", Utc::now() - Duration::seconds(1),)
                .unwrap(),
            PaperSubmissionAdmission::ObserveOnly,
        );
        assert!(fixture
            .store
            .retire_ready_paper_submissions(
                &authority,
                PaperSubmissionStop::SubmissionWindowExpired,
            )
            .unwrap()
            .is_empty());
        let orders = journal_orders(
            fixture
                .store
                .paper_submission_batch(&fixture.commitment)
                .unwrap()
                .unwrap(),
        );
        assert!(
            orders
                .iter()
                .all(|order| order.state == PaperSubmissionState::Ready
                    && order.transition_index == 0)
        );
    }

    /// WHY THIS TEST EXISTS (Task #30): The adapter cannot extend a frozen
    /// deadline, and expiry must preserve the existing actual receipt.
    /// Do not weaken or delete these assertions.
    #[test]
    fn elapsed_frozen_deadline_blocks_admission_and_proves_only_ready_sibling() {
        let fixture = Fixture::with_frozen_deadline(Utc::now() - Duration::seconds(1));
        let authority = fixture.authority();
        fixture
            .store
            .begin_paper_submission_batch(&authority)
            .unwrap();
        fixture
            .store
            .record_paper_submission_receipt(
                &authority,
                &fixture.receipt(OrderReceiptState::Filled, 1_000_000, Utc::now()),
            )
            .unwrap();
        assert_eq!(
            fixture
                .store
                .admit_paper_submission(&authority, "soxx-r0", fixture.lease.expires_at)
                .unwrap(),
            PaperSubmissionAdmission::ObserveOnly,
        );
        let proof = fixture
            .store
            .retire_ready_paper_submissions(
                &authority,
                PaperSubmissionStop::SubmissionWindowExpired,
            )
            .unwrap();
        assert_eq!(proof.len(), 1);
        assert_eq!(proof[0].client_order_id, "soxx-r0");
        assert_eq!(proof[0].transition_index, 1);
        assert_eq!(
            proof[0].reason,
            PaperUnsubmittedReason::SubmissionWindowExpired
        );
        let orders = journal_orders(
            fixture
                .store
                .paper_submission_batch(&fixture.commitment)
                .unwrap()
                .unwrap(),
        );
        let original = find_order(&orders, "qqq-r0").unwrap();
        assert_eq!(original.state, PaperSubmissionState::Terminal);
        assert!(original.receipt.is_some());
    }

    /// WHY THIS TEST EXISTS (Task #30): An admitted order without a response
    /// stays unknown after restart, and later observing it cannot permit a
    /// second submission or reopen the sibling's durable no-submission proof.
    /// Do not weaken or delete this assertion.
    #[test]
    fn admitted_unknown_survives_reopen_and_closes_ready_siblings() {
        let mut fixture = Fixture::new();
        let authority = fixture.authority();
        let ready = journal_orders(
            fixture
                .store
                .begin_paper_submission_batch(&authority)
                .unwrap(),
        );
        assert_eq!(ready.len(), 2);
        assert!(ready
            .iter()
            .all(|order| order.state == PaperSubmissionState::Ready));
        assert_eq!(
            fixture
                .store
                .admit_paper_submission(&authority, "qqq-r0", fixture.lease.expires_at)
                .unwrap(),
            PaperSubmissionAdmission::Admitted
        );
        assert_eq!(
            fixture
                .store
                .admit_paper_submission(&authority, "qqq-r0", fixture.lease.expires_at)
                .unwrap(),
            PaperSubmissionAdmission::ObserveOnly
        );
        let old = fixture.permit.clone();
        let now = Utc::now();
        let ready_at = now + Duration::seconds(1);
        fixture.store.defer_task(&old, ready_at, now).unwrap();
        fixture.store = Store::open(fixture.store.root()).unwrap();
        fixture.permit = fixture
            .store
            .claim_next_task_for_workload(
                "recovery",
                ready_at,
                Duration::minutes(2),
                TaskWorkload::Session,
            )
            .unwrap()
            .unwrap()
            .permit;
        let authority = fixture.authority();
        let orders = journal_orders(
            fixture
                .store
                .begin_paper_submission_batch(&authority)
                .unwrap(),
        );
        let qqq = find_order(&orders, "qqq-r0").unwrap();
        assert_eq!(qqq.state, PaperSubmissionState::PossiblySubmitted);
        assert!(qqq.receipt.is_none());
        let soxx = find_order(&orders, "soxx-r0").unwrap();
        assert_eq!(soxx.state, PaperSubmissionState::ProvenUnsubmitted);
        assert_eq!(
            soxx.reason,
            Some(PaperUnsubmittedReason::BatchObservationOnly)
        );
        let receipt = fixture.receipt(OrderReceiptState::Filled, 1_000_000, Utc::now());
        let stale = PaperSubmissionAuthority {
            permit: &old,
            ..authority
        };
        assert!(matches!(
            fixture
                .store
                .record_paper_submission_receipt(&stale, &receipt),
            Err(StoreError::StalePermit(_))
        ));
        fixture
            .store
            .record_paper_submission_receipt(&authority, &receipt)
            .unwrap();
        let proofs = fixture
            .store
            .retire_ready_paper_submissions(
                &authority,
                PaperSubmissionStop::BatchObservationOnly {
                    client_order_id: "qqq-r0",
                    transition_index: 1,
                },
            )
            .unwrap();
        assert_eq!(proofs, vec![unsubmitted_witness(soxx).unwrap()]);
        assert_eq!(
            fixture
                .store
                .admit_paper_submission(&authority, "soxx-r0", fixture.lease.expires_at)
                .unwrap(),
            PaperSubmissionAdmission::ObserveOnly
        );
    }

    /// WHY THIS TEST EXISTS (Task #30): Readers reopen the exact partial fill,
    /// and stale receipts or a new freeze never erase it or admit a sibling.
    /// Do not weaken or delete this assertion.
    #[test]
    fn partial_receipt_is_durable_before_the_next_admission() {
        let fixture = Fixture::new();
        let authority = fixture.authority();
        fixture
            .store
            .begin_paper_submission_batch(&authority)
            .unwrap();
        fixture
            .store
            .admit_paper_submission(&authority, "qqq-r0", fixture.lease.expires_at)
            .unwrap();
        let now = Utc::now();
        let receipt = fixture.receipt(OrderReceiptState::PartiallyFilled, 400_000, now);
        fixture
            .store
            .record_paper_submission_receipt(&authority, &receipt)
            .unwrap();
        fixture
            .store
            .record_paper_submission_receipt(&authority, &receipt)
            .unwrap();
        fixture
            .store
            .record_paper_submission_receipt(
                &authority,
                &fixture.receipt(OrderReceiptState::Accepted, 0, now - Duration::seconds(1)),
            )
            .unwrap();
        let observer = Store::open_existing(fixture.store.root()).unwrap();
        let orders = journal_orders(
            observer
                .paper_submission_batch(&fixture.commitment)
                .unwrap()
                .unwrap(),
        );
        let order = find_order(&orders, "qqq-r0").unwrap();
        assert_eq!(order.state, PaperSubmissionState::Observed);
        assert_eq!(order.transition_index, 2);
        let durable = observer
            .artifact(&order.receipt.as_ref().unwrap().artifact_id)
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<OrderReceipt>(&observer.read_blob(&durable.blob).unwrap())
                .unwrap(),
            receipt
        );

        let operator = Store::open(fixture.store.root()).unwrap();
        operator
            .write_freeze_state(true, "pause", Utc::now())
            .unwrap();
        assert!(matches!(
            fixture
                .store
                .admit_paper_submission(&authority, "soxx-r0", fixture.lease.expires_at),
            Err(StoreError::ExecutionFrozen { .. })
        ));
        let orders = journal_orders(
            observer
                .paper_submission_batch(&fixture.commitment)
                .unwrap()
                .unwrap(),
        );
        assert_eq!(
            find_order(&orders, "soxx-r0").unwrap().state,
            PaperSubmissionState::Ready
        );
    }

    /// WHY THIS TEST EXISTS (Task #30): A current receipt and a sibling's
    /// precise journal witness are both required to atomically settle an attempt.
    /// Do not weaken or delete this assertion.
    #[test]
    fn settlement_rejects_wrong_proof_and_receipt_before_closing_batch() {
        let fixture = Fixture::new();
        let authority = fixture.authority();
        let now = Utc::now();
        fixture
            .store
            .begin_paper_submission_batch(&authority)
            .unwrap();
        fixture
            .store
            .admit_paper_submission(&authority, "qqq-r0", fixture.lease.expires_at)
            .unwrap();
        fixture
            .store
            .begin_paper_submission_batch(&authority)
            .unwrap();
        let receipt = fixture.receipt(OrderReceiptState::Filled, 1_000_000, now);
        fixture
            .store
            .record_paper_submission_receipt(&authority, &receipt)
            .unwrap();
        let orders = journal_orders(
            fixture
                .store
                .paper_submission_batch(&fixture.commitment)
                .unwrap()
                .unwrap(),
        );
        let raw = find_order(&orders, "qqq-r0")
            .unwrap()
            .receipt
            .clone()
            .unwrap();
        let witness = unsubmitted_witness(find_order(&orders, "soxx-r0").unwrap()).unwrap();
        let output = |observed: &OrderReceipt, proof: UnsubmittedPaperOrder| {
            let normalized = fixture_artifact(
                &fixture.store,
                &fixture.permit,
                ArtifactKind::OrderReceipt,
                observed,
                vec![fixture.commitment.clone(), raw.clone()],
            );
            let normalized_ref = reference(&normalized);
            let settled = fixture_artifact(
                &fixture.store,
                &fixture.permit,
                ArtifactKind::Reconciliation,
                &Reconciliation {
                    reconciliation_id: akzio_domain::ReconciliationId::new(),
                    commitment: fixture.commitment.clone(),
                    state: ReconciliationState::Complete,
                    broker_receipts: vec![normalized_ref.clone()],
                    unsubmitted_orders: vec![proof],
                    achieved_target: None,
                    achieved_factor_exposure: None,
                    reconciled_at: now,
                },
                vec![fixture.commitment.clone(), normalized_ref],
            );
            [normalized, settled]
        };
        let settle = |artifacts: &[Artifact]| {
            fixture.store.commit_fenced_attempt_with_effect(
                &fixture.lease,
                &fixture.permit,
                artifacts,
                &fixture.commitment,
                true,
                Utc::now(),
            )
        };
        assert!(matches!(
            fixture.store.settle_paper_effect(
                &fixture.lease,
                &fixture.permit,
                &fixture.commitment,
                true,
                now,
            ),
            Err(StoreError::InvalidPaperEffect(_))
        ));
        let wrong_proof = UnsubmittedPaperOrder {
            transition_index: witness.transition_index + 1,
            ..witness.clone()
        };
        assert!(matches!(
            settle(&output(&receipt, wrong_proof)),
            Err(StoreError::InvalidPaperEffect(_))
        ));
        let wrong_receipt = OrderReceipt {
            average_fill_price: Some(MoneyMicros(1)),
            ..receipt.clone()
        };
        assert!(settle(&output(&wrong_receipt, witness.clone())).is_err());
        fixture.store.validate_task_permit(&fixture.permit).unwrap();
        let settled = output(&receipt, witness);
        settle(&settled).unwrap();
        assert!(fixture.store.validate_task_permit(&fixture.permit).is_err());
        assert_eq!(
            fixture.store.artifact(&settled[1].artifact_id).unwrap(),
            settled[1]
        );
    }

    /// WHY THIS TEST EXISTS (Task #30): Settlement must reproduce any merged
    /// r1 quantities and keep a live r0 open even when r1 is terminal. An
    /// action that never had an effect cannot block r0.
    /// Do not weaken or delete this assertion.
    #[test]
    fn settlement_counts_only_effective_reprices_and_normalizes_raw_fills() {
        for original_terminal in [
            None,
            Some(OrderReceiptState::Replaced),
            Some(OrderReceiptState::Canceled),
        ] {
            let replaced = original_terminal.is_some();
            let fixture = Fixture::new();
            let authority = fixture.authority();
            fixture
                .store
                .begin_paper_submission_batch(&authority)
                .unwrap();
            fixture
                .store
                .admit_paper_submission(&authority, "qqq-r0", fixture.lease.expires_at)
                .unwrap();
            let now = Utc::now();
            let prior = fixture.receipt(OrderReceiptState::PartiallyFilled, 400_000, now);
            fixture
                .store
                .record_paper_submission_receipt(&authority, &prior)
                .unwrap();
            let snapshot = || {
                journal_orders(
                    fixture
                        .store
                        .paper_submission_batch(&fixture.commitment)
                        .unwrap()
                        .unwrap(),
                )
            };
            let latest_raw = |id| {
                find_order(&snapshot(), id)
                    .unwrap()
                    .receipt
                    .clone()
                    .unwrap()
            };
            let reprice = fixture.reprice(latest_raw("qqq-r0"), now);
            let next_observation = now + Duration::milliseconds(1);
            let mut raw = fixture.receipt(OrderReceiptState::Filled, 1_000_000, next_observation);
            let mut normalized = raw.clone();
            if replaced {
                fixture
                    .store
                    .begin_paper_reprice_submission(&authority, &reprice)
                    .unwrap()
                    .unwrap();
                fixture
                    .store
                    .admit_paper_submission(&authority, "qqq-r1", fixture.lease.expires_at)
                    .unwrap();
                let newer_prior = fixture.receipt(
                    OrderReceiptState::PartiallyFilled,
                    500_000,
                    next_observation,
                );
                fixture
                    .store
                    .record_paper_submission_receipt(&authority, &newer_prior)
                    .unwrap();
                raw.client_order_id = "qqq-r1".into();
                raw.broker_order_id = "broker-qqq-r1".into();
                raw.requested_quantity_micros = 500_000;
                raw.filled_quantity_micros = 500_000;
                raw.average_fill_price = Some(MoneyMicros(30_000_000));
                normalized = OrderReceipt {
                    requested_quantity_micros: 1_000_000,
                    filled_quantity_micros: 1_000_000,
                    average_fill_price: Some(MoneyMicros(27_500_000)),
                    ..raw.clone()
                };
            }
            fixture
                .store
                .record_paper_submission_receipt(&authority, &raw)
                .unwrap();
            fixture
                .store
                .request_run_cancel(&fixture.permit.run_id, "cancel for test", Utc::now())
                .unwrap();
            let proofs = fixture
                .store
                .retire_ready_paper_submissions(&authority, PaperSubmissionStop::RunCancelled)
                .unwrap();
            assert_eq!(proofs.len(), 1);
            assert_eq!(proofs[0].client_order_id, "soxx-r0");
            let outputs = || {
                let mut sources =
                    vec![fixture.commitment.clone(), latest_raw(&raw.client_order_id)];
                if replaced {
                    sources.extend([reprice.clone(), latest_raw("qqq-r0")]);
                }
                let normalized = fixture_artifact(
                    &fixture.store,
                    &fixture.permit,
                    ArtifactKind::OrderReceipt,
                    &normalized,
                    sources,
                );
                let normalized_ref = reference(&normalized);
                let complete = fixture_artifact(
                    &fixture.store,
                    &fixture.permit,
                    ArtifactKind::Reconciliation,
                    &Reconciliation {
                        reconciliation_id: akzio_domain::ReconciliationId::new(),
                        commitment: fixture.commitment.clone(),
                        state: ReconciliationState::Complete,
                        broker_receipts: vec![normalized_ref.clone()],
                        unsubmitted_orders: proofs.clone(),
                        achieved_target: None,
                        achieved_factor_exposure: None,
                        reconciled_at: now,
                    },
                    vec![fixture.commitment.clone(), normalized_ref],
                );
                [normalized, complete]
            };
            let final_effects = || {
                fixture.store.connection().unwrap().query_row(
                "SELECT COUNT(*) FROM rebuild_events WHERE artifact_id = ?1 AND event_type IN (?2, ?3)",
                params![reprice.artifact_id.0.as_str(), LifecycleEventType::ExecutionEffectSettled.as_str(), LifecycleEventType::ExecutionEffectRecovered.as_str()],
                |row| row.get::<_, usize>(0),
            ).unwrap()
            };
            assert_eq!(final_effects(), 0);
            let settle = |outputs: &[Artifact]| {
                fixture.store.commit_fenced_attempt_with_effect(
                    &fixture.lease,
                    &fixture.permit,
                    outputs,
                    &fixture.commitment,
                    true,
                    Utc::now(),
                )
            };
            if let Some(original_terminal) = original_terminal {
                let live_original = outputs();
                let rejected = settle(&live_original);
                assert!(
                    matches!(&rejected, Err(StoreError::Integrity(message)) if message == "invalid Paper receipt for qqq-r0"),
                    "{rejected:?}",
                );
                assert_eq!(final_effects(), 0);
                fixture.store.validate_task_permit(&fixture.permit).unwrap();
                let closed_original = fixture.receipt(
                    original_terminal,
                    500_000,
                    next_observation + Duration::milliseconds(1),
                );
                fixture
                    .store
                    .record_paper_submission_receipt(&authority, &closed_original)
                    .unwrap();
                assert!(settle(&live_original).is_err());
            }
            let complete = outputs();
            settle(&complete).unwrap();
            assert!(settle(&complete).is_err());
            assert_eq!(final_effects(), usize::from(replaced));
        }
    }

    /// WHY THIS TEST EXISTS (Task #30): A legacy effect's raw receipt must be
    /// durable before a second broker request and remain exact across Attempts.
    /// Do not weaken or delete these assertions.
    #[test]
    fn legacy_raw_observations_survive_attempt_retry_without_admission() {
        let mut fixture = Fixture::new();
        let now = Utc::now();
        assert!(fixture
            .store
            .legacy_paper_submission_receipts(&fixture.commitment)
            .unwrap()
            .is_empty());
        fixture
            .store
            .record_paper_effect_intent(&fixture.lease, &fixture.permit, &fixture.commitment, now)
            .unwrap();
        let partial = fixture.receipt(OrderReceiptState::PartiallyFilled, 400_000, now);
        fixture
            .store
            .record_paper_submission_receipt(&fixture.authority(), &partial)
            .unwrap();
        let first = fixture
            .store
            .legacy_paper_submission_receipts(&fixture.commitment)
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].1, partial);
        let artifact = fixture.store.artifact(&first[0].0.artifact_id).unwrap();
        assert_eq!(artifact.origin, Some(fixture.permit.artifact_origin()));
        assert_eq!(artifact.source_refs, vec![fixture.commitment.clone()]);
        let reprice = fixture.reprice(first[0].0.clone(), now);

        let ready_at = now + Duration::seconds(1);
        fixture
            .store
            .defer_task(&fixture.permit, ready_at, now)
            .unwrap();
        fixture.store = Store::open(fixture.store.root()).unwrap();
        fixture.permit = fixture
            .store
            .claim_next_task_for_workload(
                "legacy-recovery",
                ready_at,
                Duration::minutes(2),
                TaskWorkload::Session,
            )
            .unwrap()
            .unwrap()
            .permit;
        let authority = fixture.authority();
        let duplicate = OrderReceipt {
            observed_at: ready_at,
            ..partial
        };
        fixture
            .store
            .record_paper_submission_receipt(&authority, &duplicate)
            .unwrap();
        let older = fixture.receipt(OrderReceiptState::Accepted, 0, now - Duration::seconds(1));
        fixture
            .store
            .record_paper_submission_receipt(&authority, &older)
            .unwrap();
        let regressed = fixture.receipt(OrderReceiptState::PartiallyFilled, 300_000, ready_at);
        assert!(matches!(
            fixture
                .store
                .record_paper_submission_receipt(&authority, &regressed),
            Err(StoreError::PaperReceiptConflict(_))
        ));
        assert_eq!(
            fixture
                .store
                .legacy_paper_submission_receipts(&fixture.commitment)
                .unwrap(),
            first
        );

        let closed = fixture.receipt(OrderReceiptState::Replaced, 500_000, ready_at);
        fixture
            .store
            .record_paper_submission_receipt(&authority, &closed)
            .unwrap();
        let latest = fixture
            .store
            .legacy_paper_submission_receipts(&fixture.commitment)
            .unwrap();
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].1, closed);
        assert_ne!(latest[0].0, first[0].0);
        assert_eq!(
            fixture
                .store
                .artifact(&latest[0].0.artifact_id)
                .unwrap()
                .origin,
            Some(fixture.permit.artifact_origin())
        );
        let successor = OrderReceipt {
            client_order_id: "qqq-r1".into(),
            broker_order_id: "broker-qqq-r1".into(),
            requested_quantity_micros: 500_000,
            remaining_quantity_micros: 500_000,
            ..fixture.receipt(OrderReceiptState::Accepted, 0, ready_at)
        };
        assert!(fixture
            .store
            .record_paper_submission_receipt(&authority, &successor)
            .is_err());
        fixture
            .store
            .record_paper_effect_intent(&fixture.lease, &fixture.permit, &reprice, ready_at)
            .unwrap();
        fixture
            .store
            .record_paper_submission_receipt(&authority, &successor)
            .unwrap();
        let observations = fixture
            .store
            .legacy_paper_submission_receipts(&fixture.commitment)
            .unwrap();
        assert_eq!(observations.len(), 2);
        assert_eq!(observations[0], latest[0]);
        assert_eq!(observations[1].1, successor);
        let successor_artifact = fixture
            .store
            .artifact(&observations[1].0.artifact_id)
            .unwrap();
        assert!(has_exact_source_refs(
            &successor_artifact,
            &[fixture.commitment.clone(), reprice]
        ));
        assert_eq!(
            fixture
                .store
                .paper_submission_batch(&fixture.commitment)
                .unwrap(),
            Some(PaperSubmissionBatch::LegacyUnknown)
        );
        assert_eq!(
            fixture
                .store
                .admit_paper_submission(&authority, "qqq-r0", fixture.lease.expires_at)
                .unwrap(),
            PaperSubmissionAdmission::ObserveOnly
        );
    }

    /// WHY THIS TEST EXISTS (Task #30): Neither old batch effects nor old r1
    /// effects gain Ready state by upgrading or resuming a new Core.
    /// Do not weaken or delete this assertion.
    #[test]
    fn existing_unjournaled_effects_never_gain_admission() {
        let legacy = Fixture::new();
        let authority = legacy.authority();
        legacy
            .store
            .record_paper_effect_intent(
                &legacy.lease,
                &legacy.permit,
                &legacy.commitment,
                Utc::now(),
            )
            .unwrap();
        assert_eq!(
            legacy
                .store
                .begin_paper_submission_batch(&authority)
                .unwrap(),
            PaperSubmissionBatch::LegacyUnknown
        );
        assert_eq!(
            legacy
                .store
                .admit_paper_submission(&authority, "qqq-r0", legacy.lease.expires_at)
                .unwrap(),
            PaperSubmissionAdmission::ObserveOnly
        );

        for historical in [true, false] {
            let fixture = Fixture::new();
            let authority = fixture.authority();
            fixture
                .store
                .begin_paper_submission_batch(&authority)
                .unwrap();
            fixture
                .store
                .admit_paper_submission(&authority, "qqq-r0", fixture.lease.expires_at)
                .unwrap();
            let now = Utc::now();
            let receipt = fixture.receipt(OrderReceiptState::Accepted, 0, now);
            fixture
                .store
                .record_paper_submission_receipt(&authority, &receipt)
                .unwrap();
            let orders = journal_orders(
                fixture
                    .store
                    .paper_submission_batch(&fixture.commitment)
                    .unwrap()
                    .unwrap(),
            );
            let prior_receipt = find_order(&orders, "qqq-r0")
                .unwrap()
                .receipt
                .clone()
                .unwrap();
            let reprice = fixture.reprice(prior_receipt, now);
            if historical {
                fixture
                    .store
                    .record_paper_effect_intent(&fixture.lease, &fixture.permit, &reprice, now)
                    .unwrap();
                assert!(fixture
                    .store
                    .begin_paper_reprice_submission(&authority, &reprice)
                    .unwrap()
                    .is_none());
            } else {
                let ready = fixture
                    .store
                    .begin_paper_reprice_submission(&authority, &reprice)
                    .unwrap()
                    .unwrap();
                assert_eq!(ready.state, PaperSubmissionState::Ready);
                assert_eq!(
                    fixture
                        .store
                        .admit_paper_submission(&authority, "qqq-r1", fixture.lease.expires_at)
                        .unwrap(),
                    PaperSubmissionAdmission::Admitted
                );
            }
            let receipt = OrderReceipt {
                client_order_id: "qqq-r1".into(),
                broker_order_id: "broker-qqq-r1".into(),
                ..receipt
            };
            fixture
                .store
                .record_paper_submission_receipt(&authority, &receipt)
                .unwrap();
            let orders = journal_orders(
                fixture
                    .store
                    .paper_submission_batch(&fixture.commitment)
                    .unwrap()
                    .unwrap(),
            );
            let successor = find_order(&orders, "qqq-r1").unwrap();
            assert_eq!(successor.state, PaperSubmissionState::Observed);
            assert_eq!(successor.reprice.as_ref(), Some(&reprice));
            assert_eq!(successor.transition_index, if historical { 0 } else { 2 });
            assert_eq!(
                fixture
                    .store
                    .admit_paper_submission(&authority, "qqq-r1", fixture.lease.expires_at)
                    .unwrap(),
                PaperSubmissionAdmission::ObserveOnly
            );
        }
    }
}
