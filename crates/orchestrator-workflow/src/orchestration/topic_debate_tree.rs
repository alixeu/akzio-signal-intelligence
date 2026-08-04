//! Rust-owned, topic-local event tree for Phase 2 debate delivery.
//!
//! The tree is the deep module behind Phase 2's small scheduling interface:
//! callers submit a participant result, apply a Controller action, or record
//! a failure.  It owns delivery ordering, lifecycle transitions, exactly-once
//! mailbox receipts, bounded retries, and closure invariants.  The in-memory
//! tree is checkpointed in the FileStore run state so a process restart can
//! reconstruct the same pending mailbox without exposing private role history.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DebateActor {
    Bull,
    Bear,
    Controller,
}

impl DebateActor {
    pub const PARTICIPANTS: [Self; 2] = [Self::Bull, Self::Bear];

    pub const fn role(self) -> &'static str {
        match self {
            Self::Bull => "researcher.bull",
            Self::Bear => "researcher.bear",
            Self::Controller => "mediator.topic_controller",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ParticipantStatus {
    Waiting,
    Runnable,
    Running,
    RetryScheduled,
    Failed,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DebateStatus {
    Open,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StreeNodeKind {
    Opening,
    Submission,
    Agreement,
    Route,
    Wait,
    Failure,
    Close,
    SafetyLimit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ParticipantState {
    pub status: ParticipantStatus,
    #[serde(default)]
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_node_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StreeNode {
    pub node_id: String,
    pub sequence: u64,
    pub round: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<DebateActor>,
    pub targets: Vec<DebateActor>,
    pub kind: StreeNodeKind,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StreeDelivery {
    pub delivery_id: String,
    pub node_id: String,
    pub target: DebateActor,
    pub delivered: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct DebateDispatch {
    pub actor: DebateActor,
    /// A Controller dispatch consumes its entire pending mailbox atomically.
    /// Bull and Bear receive one route/opening at a time.  Batching the
    /// Controller side prevents it from closing or routing based on only the
    /// first response in a collision wave.
    pub deliveries: Vec<StreeDelivery>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TopicDebateTree {
    pub schema_version: u32,
    pub topic_id: String,
    pub topic: Value,
    pub max_rounds: u32,
    pub round: u32,
    pub status: DebateStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closure: Option<Value>,
    #[serde(default)]
    pub nodes: Vec<StreeNode>,
    #[serde(default)]
    pub deliveries: Vec<StreeDelivery>,
    /// Rust-observed evidence IDs that participants may cite. This survives
    /// checkpoints so a resumed debate retains the same provenance boundary.
    #[serde(default)]
    pub evidence_registry: BTreeSet<String>,
    /// Event-level identity for every admissible evidence reference.  A new
    /// URL or evidence ID is not new information when it resolves to an event
    /// that was already visible in Phase 1 or an earlier debate turn.
    #[serde(default)]
    pub evidence_event_clusters: BTreeMap<String, String>,
    /// Runtime facts about the two research forks.  They are deliberately
    /// persisted with the tree so Phase 3 can distinguish a genuine
    /// cross-model corroboration from two role prompts applied to the same
    /// warm-up and model.
    #[serde(default)]
    pub independence_context: Value,
    pub participants: BTreeMap<DebateActor, ParticipantState>,
    #[serde(default)]
    next_sequence: u64,
}

impl TopicDebateTree {
    pub fn open(topic_id: impl Into<String>, topic: Value, max_rounds: u32) -> Result<Self> {
        let topic_id = topic_id.into();
        if topic_id.trim().is_empty() {
            bail!("TopicDebateTree requires a non-empty topic_id");
        }
        let mut participants = BTreeMap::new();
        for actor in [
            DebateActor::Bull,
            DebateActor::Bear,
            DebateActor::Controller,
        ] {
            participants.insert(
                actor,
                ParticipantState {
                    status: ParticipantStatus::Waiting,
                    attempts: 0,
                    last_node_id: None,
                },
            );
        }
        let mut tree = Self {
            schema_version: SCHEMA_VERSION,
            topic_id,
            topic,
            max_rounds,
            round: 0,
            status: DebateStatus::Open,
            closure: None,
            nodes: Vec::new(),
            deliveries: Vec::new(),
            evidence_registry: BTreeSet::new(),
            evidence_event_clusters: BTreeMap::new(),
            independence_context: json!({
                "shared_warmup": true,
                "bull_model": null,
                "bear_model": null,
                "model_independence": "unknown"
            }),
            participants,
            next_sequence: 1,
        };
        for actor in DebateActor::PARTICIPANTS {
            let node = tree.append_node(
                None,
                vec![actor],
                StreeNodeKind::Opening,
                json!({
                    "topic_id": tree.topic_id,
                    "topic": tree.topic.clone(),
                    "instruction": "Prepare your initial, evidence-bounded position and submit it to the Topic Controller."
                }),
            );
            tree.queue_delivery(&node, actor)?;
        }
        Ok(tree)
    }

    pub fn recover_inflight(&mut self) {
        if self.status == DebateStatus::Closed {
            return;
        }
        for participant in self.participants.values_mut() {
            if participant.status == ParticipantStatus::Running {
                participant.status = ParticipantStatus::RetryScheduled;
            }
        }
    }

    #[cfg(test)]
    fn register_evidence_refs<'a>(
        &mut self,
        references: impl IntoIterator<Item = &'a str>,
    ) -> Result<()> {
        for reference in references {
            self.register_evidence_ref_cluster(reference, reference)?;
        }
        Ok(())
    }

    /// Register one Rust-observed evidence item with its event-level identity.
    /// The caller owns the mapping; this tree only rejects malformed IDs and
    /// makes the mapping immutable once a reference has been admitted.
    pub fn register_evidence_ref_cluster(
        &mut self,
        reference: &str,
        event_cluster_id: &str,
    ) -> Result<()> {
        if !is_complete_evidence_ref(reference) {
            bail!("stree evidence registry requires a complete stable evidence ID")
        }
        let event_cluster_id = event_cluster_id.trim();
        if event_cluster_id.is_empty() {
            bail!("stree evidence registry requires a non-empty event cluster ID")
        }
        if let Some(existing) = self.evidence_event_clusters.get(reference) {
            if existing != event_cluster_id {
                bail!(
                    "stree evidence reference {reference} cannot change event cluster from {existing} to {event_cluster_id}"
                )
            }
        } else {
            self.evidence_event_clusters
                .insert(reference.to_owned(), event_cluster_id.to_owned());
        }
        self.evidence_registry.insert(reference.to_owned());
        Ok(())
    }

    pub fn set_independence_context(
        &mut self,
        bull_model: impl Into<String>,
        bear_model: impl Into<String>,
    ) {
        let bull_model = bull_model.into();
        let bear_model = bear_model.into();
        let model_independence = if bull_model.trim().is_empty() || bear_model.trim().is_empty() {
            "unknown"
        } else if bull_model == bear_model {
            "same_model"
        } else {
            "distinct_models"
        };
        self.independence_context = json!({
            "shared_warmup": true,
            "bull_model": bull_model,
            "bear_model": bear_model,
            "model_independence": model_independence,
        });
    }

    pub fn next_dispatch(&mut self) -> Option<DebateDispatch> {
        if self.status == DebateStatus::Closed {
            return None;
        }
        let actor = [DebateActor::Bull, DebateActor::Bear]
            .into_iter()
            .find(|actor| self.has_pending_opening_delivery(*actor))
            // A Controller route to both sides is one collision wave.  Both
            // participants must consume that wave before a new Controller
            // turn can react to only the first reply; otherwise Bull wins the
            // fixed ordering repeatedly and Bear starves.
            .or_else(|| {
                [DebateActor::Bull, DebateActor::Bear]
                    .into_iter()
                    .find(|actor| self.has_pending_route_delivery(*actor))
            })
            .or_else(|| {
                [DebateActor::Controller]
                    .into_iter()
                    .find(|actor| self.has_pending_delivery(*actor))
            })
            .or_else(|| {
                [
                    DebateActor::Controller,
                    DebateActor::Bull,
                    DebateActor::Bear,
                ]
                .into_iter()
                .find(|actor| self.status_of(*actor) == Some(ParticipantStatus::RetryScheduled))
            })
            .or_else(|| {
                [DebateActor::Bull, DebateActor::Bear]
                    .into_iter()
                    .find(|actor| self.has_pending_delivery(*actor))
            })?;
        let dispatch_all_pending = actor == DebateActor::Controller;
        let mut deliveries = Vec::new();
        for delivery in self
            .deliveries
            .iter_mut()
            .filter(|delivery| delivery.target == actor && !delivery.delivered)
        {
            delivery.delivered = true;
            deliveries.push(delivery.clone());
            if !dispatch_all_pending {
                break;
            }
        }
        self.participant_mut(actor)?.status = ParticipantStatus::Running;
        Some(DebateDispatch { actor, deliveries })
    }

    pub fn injected_user_message(&self, deliveries: &[StreeDelivery]) -> Result<String> {
        if deliveries.is_empty() {
            bail!("stree dispatch requires at least one delivery to inject");
        }
        let mut payloads = deliveries
            .iter()
            .map(|delivery| {
                let node = self
                    .nodes
                    .iter()
                    .find(|node| node.node_id == delivery.node_id)
                    .context("stree delivery references an unknown node")?;
                Ok((delivery, node))
            })
            .collect::<Result<Vec<_>>>()?;
        let controller_batch = deliveries
            .iter()
            .all(|delivery| delivery.target == DebateActor::Controller);
        let message = if controller_batch {
            // The Controller does not need the speaker label to route both
            // sides.  Redacting it and canonically sorting a compact payload
            // prevents the fixed Bull-first scheduler, duplicated report
            // prose, and presentation order from becoming a hidden vote.
            let mut normalized = payloads
                .drain(..)
                .map(|(delivery, node)| {
                    let payload = controller_visible_payload(node);
                    let sort_key = orchestrator_store::content_hash(&payload)?;
                    Ok(json!({
                        "delivery_id": delivery.delivery_id,
                        "node_id": node.node_id,
                        "sequence": node.sequence,
                        "round": node.round,
                        "kind": node.kind,
                        "payload": payload,
                        "presentation_key": sort_key,
                    }))
                })
                .collect::<Result<Vec<_>>>()?;
            normalized.sort_by(|left, right| {
                left.get("presentation_key")
                    .and_then(Value::as_str)
                    .cmp(&right.get("presentation_key").and_then(Value::as_str))
                    .then_with(|| {
                        left.get("node_id")
                            .and_then(Value::as_str)
                            .cmp(&right.get("node_id").and_then(Value::as_str))
                    })
            });
            for payload in &mut normalized {
                if let Some(object) = payload.as_object_mut() {
                    object.remove("presentation_key");
                }
            }
            json!({
                "topic_id": self.topic_id,
                "deliveries": normalized,
                "current_round": self.round,
                "max_rounds": self.max_rounds,
                "terminal_close_required": self.round >= self.max_rounds,
                "rust_continuation_gate": self.continuation_gate(),
                "presentation_policy": {
                    "role_labels_redacted": true,
                    "delivery_order": "canonical_content_hash_v1",
                    "duplicate_report_text_removed": true,
                },
                "trusted_protocol": "phase2_topic_debate_tree"
            })
        } else if payloads.len() == 1 {
            let (delivery, node) = payloads.into_iter().next().expect("one payload");
            let mut payload = json!({
                "delivery_id": delivery.delivery_id,
                "node_id": node.node_id,
                "sequence": node.sequence,
                "round": node.round,
                "from": node.from,
                "kind": node.kind,
                "payload": node.payload,
            });
            payload["trusted_protocol"] = json!("phase2_topic_debate_tree");
            payload
        } else {
            bail!("only Controller deliveries may be batched in an stree dispatch");
        };
        Ok(format!("stree: {}", serde_json::to_string(&message)?))
    }

    pub fn submit(&mut self, actor: DebateActor, mut payload: Value) -> Result<StreeNode> {
        self.ensure_open()?;
        if !matches!(actor, DebateActor::Bull | DebateActor::Bear) {
            bail!("only Bull or Bear may submit a debate position");
        }
        self.require_running(actor)?;
        let (stance, evidence_delta, evidence_links, reply_reference) = {
            let object = payload
                .as_object()
                .context("debate submission must be a JSON object")?;
            let stance = required_string(object, "stance", 32)?;
            if !matches!(
                stance.as_str(),
                "challenge"
                    | "partial_agree"
                    | "agree"
                    | "retract"
                    | "needs_evidence"
                    | "no_new_info"
            ) {
                bail!("unsupported debate stance {stance:?}");
            }
            let message = required_string(object, "message", 1_200)?;
            validate_stance_message_consistency(&stance, &message)?;
            self.validate_submission_evidence_refs(object)?;
            let evidence_links = self.canonical_submission_evidence_links(object)?;
            let evidence_delta = self.submission_evidence_delta(object)?;
            let reply_reference = object
                .get("reply_to_node_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            (stance, evidence_delta, evidence_links, reply_reference)
        };
        payload["evidence_delta"] = evidence_delta;
        payload["evidence_links"] = Value::Array(evidence_links);
        if let Some((delivery_id, route_node_id)) = self.active_route_receipt(actor) {
            // The delivery receipt is the authoritative parent for a routed
            // response. The model may use either a claim alias or a node ID,
            // but cannot redirect the reply away from the Controller's route.
            // Retain the supplied value for audit without letting its spelling
            // break the collision graph.
            if let Some(reply_reference) = reply_reference {
                payload["reply_to_reference"] = json!(reply_reference);
            }
            payload["reply_to_delivery_id"] = json!(delivery_id);
            payload["reply_to_node_id"] = json!(route_node_id);
        } else if let Some(reply_reference) = reply_reference {
            let reply_to_node_id =
                self.resolve_participant_reply_node_id(actor, &reply_reference)?;
            if reply_to_node_id != reply_reference {
                payload["reply_to_reference"] = json!(reply_reference);
                payload["reply_to_node_id"] = json!(reply_to_node_id);
            }
        }
        let kind = if matches!(stance.as_str(), "agree" | "partial_agree") {
            StreeNodeKind::Agreement
        } else {
            StreeNodeKind::Submission
        };
        let node = self.append_node(Some(actor), vec![DebateActor::Controller], kind, payload);
        self.participant_mut(actor)
            .context("participant state is missing")?
            .status = ParticipantStatus::Waiting;
        self.participant_mut(actor)
            .context("participant state is missing")?
            .last_node_id = Some(node.node_id.clone());
        self.queue_delivery(&node, DebateActor::Controller)?;
        Ok(node)
    }

    pub fn controller_route(&mut self, mut payload: Value) -> Result<Vec<StreeNode>> {
        self.ensure_open()?;
        self.require_running(DebateActor::Controller)?;
        self.require_controller_mailbox_drained()?;
        if self.initial_collision_complete() && !self.continuation_allowed() {
            bail!(
                "controller must close the debate after the completed collision: no newly observed evidence event was introduced"
            )
        }
        let (targets, reply_reference) = {
            let object = payload
                .as_object()
                .context("controller route must be a JSON object")?;
            required_string(object, "message", 1_200)?;
            (
                parse_targets(object.get("targets"))?,
                required_string(object, "reply_to_node_id", 128)?,
            )
        };
        let reply_to_node_id = self.resolve_controller_reply_node_id(&reply_reference)?;
        if reply_to_node_id != reply_reference {
            payload["reply_to_delivery_id"] = json!(reply_reference);
            payload["reply_to_node_id"] = json!(reply_to_node_id);
        }
        if self.round >= self.max_rounds {
            bail!("controller route exceeded max_debate_rounds; close the debate instead");
        }
        self.round = self.round.saturating_add(1);
        let mut nodes = Vec::new();
        let initial_nodes = (
            self.initial_submission_node(DebateActor::Bull)
                .map(|node| node.node_id.clone()),
            self.initial_submission_node(DebateActor::Bear)
                .map(|node| node.node_id.clone()),
        );
        if targets.len() != DebateActor::PARTICIPANTS.len() {
            bail!("controller route must target both Bull and Bear in the same collision wave");
        }
        for target in targets {
            let target_reply_to_node_id = match (&initial_nodes.0, &initial_nodes.1) {
                (Some(bull_initial), Some(bear_initial)) => match target {
                    DebateActor::Bull if reply_to_node_id == *bull_initial => bear_initial.clone(),
                    DebateActor::Bear if reply_to_node_id == *bear_initial => bull_initial.clone(),
                    DebateActor::Bull | DebateActor::Bear => reply_to_node_id.clone(),
                    DebateActor::Controller => reply_to_node_id.clone(),
                },
                _ => reply_to_node_id.clone(),
            };
            let mut target_payload = payload.clone();
            if target_reply_to_node_id != reply_to_node_id {
                target_payload["reply_to_node_id"] = json!(target_reply_to_node_id);
                if let Some(object) = target_payload.as_object_mut() {
                    object.remove("reply_to_delivery_id");
                }
            }
            let node = self.append_node(
                Some(DebateActor::Controller),
                vec![target],
                StreeNodeKind::Route,
                target_payload,
            );
            self.queue_delivery(&node, target)?;
            nodes.push(node);
        }
        self.participant_mut(DebateActor::Controller)
            .context("controller state is missing")?
            .status = ParticipantStatus::Waiting;
        Ok(nodes)
    }

    pub fn controller_wait(&mut self, payload: Value) -> Result<StreeNode> {
        self.ensure_open()?;
        self.require_running(DebateActor::Controller)?;
        let object = payload
            .as_object()
            .context("controller wait must be a JSON object")?;
        required_string(object, "message", 1_200)?;
        let node = self.append_node(
            Some(DebateActor::Controller),
            Vec::new(),
            StreeNodeKind::Wait,
            payload,
        );
        self.participant_mut(DebateActor::Controller)
            .context("controller state is missing")?
            .status = ParticipantStatus::Waiting;
        Ok(node)
    }

    pub fn controller_close(&mut self, payload: Value) -> Result<StreeNode> {
        self.controller_close_with_verified_evidence(payload, &BTreeSet::new())
    }

    /// Close a Controller turn with the exact evidence IDs Rust observed in
    /// that Controller session.  Consensus is stronger than a role label: the
    /// Controller must name the accepted current claims and attest to source
    /// IDs it actually expanded or fetched in this turn.
    pub fn controller_close_with_verified_evidence(
        &mut self,
        payload: Value,
        controller_verified_evidence_refs: &BTreeSet<String>,
    ) -> Result<StreeNode> {
        self.ensure_open()?;
        self.require_running(DebateActor::Controller)?;
        self.require_controller_mailbox_drained()?;
        let object = payload
            .as_object()
            .context("controller close must be a JSON object")?;
        let reason = required_string(object, "reason", 64)?;
        if !matches!(
            reason.as_str(),
            "consensus"
                | "unresolved_disagreement"
                | "evidence_exhausted"
                | "agent_failure"
                | "round_limit"
        ) {
            bail!("unsupported controller closure reason {reason:?}");
        }
        required_string(object, "message", 1_200)?;
        if reason != "agent_failure" && !self.initial_collision_complete() {
            bail!(
                "Controller cannot close before Bull and Bear directly respond to each other's initial positions"
            );
        }
        if reason == "consensus" && !self.has_full_agreement() {
            bail!("consensus close requires an explicit agreement from both Bull and Bear");
        }
        let consensus_claim_ids = if reason == "consensus" {
            self.latest_claim_ids()
        } else {
            Vec::new()
        };
        let accepted_evidence = if reason == "consensus" {
            self.validate_controller_accepted_evidence(
                object,
                &consensus_claim_ids,
                controller_verified_evidence_refs,
            )?
        } else {
            Vec::new()
        };
        let controller_message = object.get("message").cloned().unwrap_or(Value::Null);
        let node = self.append_node(
            Some(DebateActor::Controller),
            Vec::new(),
            StreeNodeKind::Close,
            payload.clone(),
        );
        self.status = DebateStatus::Closed;
        let claim_ledger = self.structured_claim_ledger();
        let unresolved_claim_ids = if reason == "consensus" {
            Vec::new()
        } else {
            self.latest_claim_ids()
        };
        self.closure = Some(json!({
            "reason": reason,
            "message": format!("Rust recorded {reason} at completed collision round {}.", self.round),
            "controller_message": controller_message,
            "node_id": node.node_id,
            "round": self.round,
            "controller_decided": true,
            "claim_ledger": claim_ledger,
            "consensus_claim_ids": consensus_claim_ids,
            "accepted_evidence": accepted_evidence,
            "controller_verified_evidence_refs": controller_verified_evidence_refs,
            "unresolved_claim_ids": unresolved_claim_ids,
            "information_gain": self.information_gain_summary(),
            "independence_assessment": self.independence_assessment(),
        }));
        for participant in self.participants.values_mut() {
            participant.status = ParticipantStatus::Closed;
        }
        Ok(node)
    }

    pub fn record_failure(
        &mut self,
        actor: DebateActor,
        error: impl Into<String>,
        max_retries: u32,
    ) -> Result<()> {
        self.ensure_open()?;
        let error = error.into();
        if error.trim().is_empty() {
            bail!("debate failure must include a message");
        }
        let (attempt, retry_scheduled) = {
            let participant = self
                .participant_mut(actor)
                .context("participant state is missing")?;
            participant.attempts = participant.attempts.saturating_add(1);
            let retry_scheduled = participant.attempts <= max_retries;
            participant.status = if retry_scheduled {
                ParticipantStatus::RetryScheduled
            } else {
                ParticipantStatus::Failed
            };
            (participant.attempts, retry_scheduled)
        };
        let node = self.append_node(
            Some(actor),
            if actor == DebateActor::Controller {
                Vec::new()
            } else {
                vec![DebateActor::Controller]
            },
            StreeNodeKind::Failure,
            json!({
                "actor": actor,
                "error": error,
                "attempt": attempt,
                "retry_scheduled": retry_scheduled,
            }),
        );
        if actor == DebateActor::Controller && !retry_scheduled {
            self.close_after_controller_failure(&node);
        } else if actor != DebateActor::Controller {
            self.queue_delivery(&node, DebateActor::Controller)?;
        }
        Ok(())
    }

    pub fn close_after_safety_limit(&mut self) -> Result<StreeNode> {
        self.ensure_open()?;
        let safety_node = self.append_node(
            None,
            vec![DebateActor::Controller],
            StreeNodeKind::SafetyLimit,
            json!({
                "reason": "round_limit",
                "message": "Rust safety cap reached; the debate is closed without inferring consensus."
            }),
        );
        let close_node = self.append_node(
            None,
            Vec::new(),
            StreeNodeKind::Close,
            json!({
                "reason": "round_limit",
                "message": "Rust safety cap closed the debate after the Controller did not reach a terminal decision.",
                "safety_node_id": safety_node.node_id,
            }),
        );
        self.status = DebateStatus::Closed;
        self.closure = Some(json!({
            "reason": "round_limit",
            "message": "Rust safety cap closed the debate after the Controller did not reach a terminal decision.",
            "node_id": close_node.node_id,
            "round": self.round,
            "controller_decided": false,
            "safety_enforced": true,
        }));
        for participant in self.participants.values_mut() {
            participant.status = ParticipantStatus::Closed;
        }
        Ok(close_node)
    }

    pub fn is_closed(&self) -> bool {
        self.status == DebateStatus::Closed
    }

    pub fn process_summary(&self) -> Value {
        let concessions = self
            .nodes
            .iter()
            .filter(|node| node.kind == StreeNodeKind::Agreement)
            .map(|node| {
                json!({
                    "node_id": node.node_id,
                    "from": node.from,
                    "stance": node.payload.get("stance").cloned().unwrap_or(Value::Null),
                    "message": node.payload.get("message").cloned().unwrap_or(Value::Null),
                    "reply_to_node_id": node.payload.get("reply_to_node_id").cloned().unwrap_or(Value::Null),
                })
            })
            .collect::<Vec<_>>();
        json!({
            "topic_id": self.topic_id,
            "status": self.status,
            "round": self.round,
            "closure": self.closure,
            "turn_count": self.nodes.len(),
            "concessions": concessions,
            "claim_ledger": self.structured_claim_ledger(),
            "evidence_registry": self.evidence_registry,
            "evidence_event_clusters": self.evidence_event_clusters,
            "continuation_gate": self.continuation_gate(),
            "independence_assessment": self.independence_assessment(),
            "nodes": self.nodes,
            "delivery_receipts": self.deliveries,
        })
    }

    fn ensure_open(&self) -> Result<()> {
        if self.status == DebateStatus::Closed {
            bail!("topic debate is already closed");
        }
        Ok(())
    }

    fn require_running(&self, actor: DebateActor) -> Result<()> {
        if self.status_of(actor) != Some(ParticipantStatus::Running) {
            bail!(
                "{} may act only while it owns the active debate turn",
                actor.role()
            );
        }
        Ok(())
    }

    fn status_of(&self, actor: DebateActor) -> Option<ParticipantStatus> {
        self.participants.get(&actor).map(|state| state.status)
    }

    fn participant_mut(&mut self, actor: DebateActor) -> Option<&mut ParticipantState> {
        self.participants.get_mut(&actor)
    }

    fn has_pending_delivery(&self, actor: DebateActor) -> bool {
        self.deliveries
            .iter()
            .any(|delivery| delivery.target == actor && !delivery.delivered)
    }

    fn require_controller_mailbox_drained(&self) -> Result<()> {
        if self.has_pending_delivery(DebateActor::Controller) {
            bail!(
                "Controller must receive every pending Bull/Bear submission before routing or closing"
            );
        }
        Ok(())
    }

    fn has_pending_opening_delivery(&self, actor: DebateActor) -> bool {
        self.deliveries.iter().any(|delivery| {
            delivery.target == actor
                && !delivery.delivered
                && self.nodes.iter().any(|node| {
                    node.node_id == delivery.node_id && node.kind == StreeNodeKind::Opening
                })
        })
    }

    fn has_pending_route_delivery(&self, actor: DebateActor) -> bool {
        self.deliveries.iter().any(|delivery| {
            delivery.target == actor
                && !delivery.delivered
                && self.nodes.iter().any(|node| {
                    node.node_id == delivery.node_id && node.kind == StreeNodeKind::Route
                })
        })
    }

    fn active_route_receipt(&self, actor: DebateActor) -> Option<(String, String)> {
        self.deliveries.iter().rev().find_map(|delivery| {
            if delivery.target != actor || !delivery.delivered {
                return None;
            }
            self.nodes
                .iter()
                .find(|node| node.node_id == delivery.node_id)
                .filter(|node| node.kind == StreeNodeKind::Route)
                .map(|node| (delivery.delivery_id.clone(), node.node_id.clone()))
        })
    }

    fn resolve_controller_reply_node_id(&self, reference: &str) -> Result<String> {
        if self.nodes.iter().any(|node| node.node_id == reference) {
            return Ok(reference.to_owned());
        }
        if let Some(delivery) = self.deliveries.iter().find(|delivery| {
            delivery.target == DebateActor::Controller
                && delivery.delivered
                && delivery.delivery_id == reference
        }) {
            return Ok(delivery.node_id.clone());
        }
        if let Some(node_id) = self.resolve_claim_reference(reference) {
            return Ok(node_id);
        }
        bail!("controller route reply_to_node_id is not in this topic tree")
    }

    fn resolve_participant_reply_node_id(
        &self,
        actor: DebateActor,
        reference: &str,
    ) -> Result<String> {
        if self.nodes.iter().any(|node| node.node_id == reference) {
            return Ok(reference.to_owned());
        }
        if let Some(delivery) = self.deliveries.iter().find(|delivery| {
            delivery.target == actor && delivery.delivered && delivery.delivery_id == reference
        }) {
            return Ok(delivery.node_id.clone());
        }

        if let Some(node_id) = self.resolve_claim_reference(reference) {
            return Ok(node_id);
        }

        let prefix = format!("{}:stree", self.topic_id);
        let sequence = reference
            .strip_prefix(&prefix)
            .and_then(|suffix| {
                suffix
                    .strip_prefix(':')
                    .or_else(|| suffix.strip_prefix('-'))
                    .or_else(|| suffix.strip_prefix('/'))
            })
            .filter(|suffix| !suffix.is_empty() && suffix.chars().all(|ch| ch.is_ascii_digit()));
        if let Some(sequence) = sequence {
            let canonical = format!("{prefix}:{sequence}");
            if self.nodes.iter().any(|node| node.node_id == canonical) {
                return Ok(canonical);
            }
        }
        bail!("participant reply_to_node_id is not in this topic tree")
    }

    fn resolve_claim_reference(&self, reference: &str) -> Option<String> {
        let topic_prefix = format!("{}:", self.topic_id);
        let reference = reference.strip_prefix(&topic_prefix).unwrap_or(reference);
        let (actor_label, sequence) = reference.split_once(':')?;
        let sequence = sequence
            .parse::<u64>()
            .ok()
            .filter(|sequence| *sequence > 0)?;
        let actor = match actor_label {
            "bull" | "researcher.bull" => DebateActor::Bull,
            "bear" | "researcher.bear" => DebateActor::Bear,
            _ => return None,
        };
        // Legacy prompt artifacts use `<topic_id>:<side>:<claim ordinal>`,
        // while the FileStore tree owns `<topic_id>:stree:<node sequence>`.
        // Prefer an exact participant node sequence for backwards-compatible
        // references such as `bear:4`; otherwise an initial seed claim alias
        // resolves to that side's initial submission, regardless of how many
        // claims it carried in a single message.
        self.nodes
            .iter()
            .find(|node| {
                node.sequence == sequence
                    && node.from == Some(actor)
                    && matches!(
                        node.kind,
                        StreeNodeKind::Submission | StreeNodeKind::Agreement
                    )
            })
            .map(|node| node.node_id.clone())
            .or_else(|| {
                self.initial_submission_node(actor)
                    .map(|node| node.node_id.clone())
            })
    }

    fn append_node(
        &mut self,
        from: Option<DebateActor>,
        targets: Vec<DebateActor>,
        kind: StreeNodeKind,
        payload: Value,
    ) -> StreeNode {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        let node = StreeNode {
            node_id: format!("{}:stree:{sequence}", self.topic_id),
            sequence,
            round: self.round,
            from,
            targets,
            kind,
            payload,
        };
        self.nodes.push(node.clone());
        node
    }

    fn queue_delivery(&mut self, node: &StreeNode, target: DebateActor) -> Result<()> {
        if self.is_closed() {
            bail!("cannot queue a stree delivery after close");
        }
        let delivery_id = format!("{}:to:{}", node.node_id, target.role());
        if self
            .deliveries
            .iter()
            .any(|delivery| delivery.delivery_id == delivery_id)
        {
            bail!("duplicate stree delivery {delivery_id}");
        }
        self.deliveries.push(StreeDelivery {
            delivery_id,
            node_id: node.node_id.clone(),
            target,
            delivered: false,
        });
        self.participant_mut(target)
            .context("participant state is missing")?
            .status = ParticipantStatus::Runnable;
        Ok(())
    }

    fn initial_collision_complete(&self) -> bool {
        let Some(bull_initial) = self.initial_submission_node(DebateActor::Bull) else {
            return false;
        };
        let Some(bear_initial) = self.initial_submission_node(DebateActor::Bear) else {
            return false;
        };
        let directly_replied = |actor, opposing_node_id: &str| {
            self.nodes.iter().any(|node| {
                matches!(
                    node.kind,
                    StreeNodeKind::Submission | StreeNodeKind::Agreement
                ) && node.from == Some(actor)
                    && node
                        .payload
                        .get("reply_to_node_id")
                        .and_then(Value::as_str)
                        .is_some_and(|reply| self.reply_reaches_node(reply, opposing_node_id))
            })
        };
        directly_replied(DebateActor::Bull, &bear_initial.node_id)
            && directly_replied(DebateActor::Bear, &bull_initial.node_id)
    }

    fn initial_submission_node(&self, actor: DebateActor) -> Option<&StreeNode> {
        self.nodes.iter().find(|node| {
            matches!(
                node.kind,
                StreeNodeKind::Submission | StreeNodeKind::Agreement
            ) && node.from == Some(actor)
                && node.round == 0
        })
    }

    fn reply_reaches_node(&self, reply: &str, target: &str) -> bool {
        let mut cursor = Some(reply.to_owned());
        for _ in 0..=self.nodes.len() {
            let Some(node_id) = cursor.take() else {
                return false;
            };
            if node_id == target {
                return true;
            }
            cursor = self
                .nodes
                .iter()
                .find(|node| node.node_id == node_id && node.kind == StreeNodeKind::Route)
                .and_then(|node| node.payload.get("reply_to_node_id"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
        }
        false
    }

    fn has_full_agreement(&self) -> bool {
        DebateActor::PARTICIPANTS.into_iter().all(|actor| {
            let opponent = if actor == DebateActor::Bull {
                DebateActor::Bear
            } else {
                DebateActor::Bull
            };
            let Some(opponent_initial) = self.initial_submission_node(opponent) else {
                return false;
            };
            self.nodes
                .iter()
                .rev()
                .find(|node| {
                    matches!(
                        node.kind,
                        StreeNodeKind::Submission | StreeNodeKind::Agreement
                    ) && node.from == Some(actor)
                })
                .is_some_and(|node| {
                    node.kind == StreeNodeKind::Agreement
                        && node.payload.get("stance").and_then(Value::as_str) == Some("agree")
                        && node
                            .payload
                            .get("reply_to_node_id")
                            .and_then(Value::as_str)
                            .is_some_and(|reply| {
                                self.reply_reaches_node(reply, &opponent_initial.node_id)
                            })
                })
        })
    }

    fn latest_claim_ids(&self) -> Vec<String> {
        DebateActor::PARTICIPANTS
            .into_iter()
            .filter_map(|actor| {
                self.nodes
                    .iter()
                    .rev()
                    .find(|node| {
                        matches!(
                            node.kind,
                            StreeNodeKind::Submission | StreeNodeKind::Agreement
                        ) && node.from == Some(actor)
                    })
                    .map(|node| node.node_id.clone())
            })
            .collect()
    }

    fn validate_submission_evidence_refs(
        &self,
        object: &serde_json::Map<String, Value>,
    ) -> Result<()> {
        let references = object
            .get("evidence_refs")
            .and_then(Value::as_array)
            .context("stree submission requires evidence_refs array")?;
        if references.len() > 3 {
            bail!("stree submission permits at most three evidence_refs")
        }
        let mut seen = BTreeSet::new();
        for reference in references {
            let reference = reference
                .as_str()
                .context("stree submission evidence_refs must contain strings")?;
            if !is_complete_evidence_ref(reference) {
                bail!("stree submission evidence_refs must contain complete stable evidence IDs")
            }
            if !seen.insert(reference) {
                bail!("stree submission contains duplicate evidence_refs")
            }
            if !self.evidence_registry.contains(reference) {
                bail!("stree submission references evidence not observed by Rust")
            }
        }
        Ok(())
    }

    /// Canonicalize the model-declared relationship between the turn's one
    /// compact claim (`message`) and each Rust-observed evidence ID.  This is
    /// an auditable claim-to-source edge, not an attempt to have Rust infer
    /// natural-language entailment.
    fn canonical_submission_evidence_links(
        &self,
        object: &serde_json::Map<String, Value>,
    ) -> Result<Vec<Value>> {
        let references = object
            .get("evidence_refs")
            .and_then(Value::as_array)
            .context("stree submission requires evidence_refs array")?
            .iter()
            .map(|reference| {
                reference
                    .as_str()
                    .map(str::trim)
                    .filter(|reference| !reference.is_empty())
                    .map(ToOwned::to_owned)
                    .context("stree submission evidence_refs must contain strings")
            })
            .collect::<Result<BTreeSet<_>>>()?;
        let links = object
            .get("evidence_links")
            .and_then(Value::as_array)
            .context("stree submission requires evidence_links array")?;
        if links.len() > 3 {
            bail!("stree submission permits at most three evidence_links")
        }
        let mut canonical = BTreeMap::new();
        for link in links {
            let link = link
                .as_object()
                .context("stree submission evidence_links must contain objects")?;
            let reference = required_string(link, "evidence_ref", 128)?;
            if !is_complete_evidence_ref(&reference) {
                bail!("stree submission evidence_links must use complete stable evidence IDs")
            }
            let relation = required_string(link, "relation", 32)?;
            if !matches!(relation.as_str(), "supports" | "refutes" | "qualifies") {
                bail!("stree submission evidence_links relation is invalid")
            }
            if canonical
                .insert(reference.clone(), Value::String(relation))
                .is_some()
            {
                bail!("stree submission evidence_links contains duplicate evidence_ref")
            }
        }
        if canonical.keys().cloned().collect::<BTreeSet<_>>() != references {
            bail!("stree submission evidence_links must cover each evidence_ref exactly once")
        }
        Ok(canonical
            .into_iter()
            .map(|(evidence_ref, relation)| {
                json!({
                    "evidence_ref": evidence_ref,
                    "relation": relation,
                    "authority": "model_declared_claim_evidence_relation_v1",
                })
            })
            .collect())
    }

    fn submission_evidence_delta(&self, object: &serde_json::Map<String, Value>) -> Result<Value> {
        let evidence_refs = object
            .get("evidence_refs")
            .and_then(Value::as_array)
            .context("stree submission requires evidence_refs array")?
            .iter()
            .filter_map(Value::as_str)
            .map(ToOwned::to_owned)
            .collect::<BTreeSet<_>>();
        let previous_refs = self.participant_evidence_refs();
        let previous_clusters = self.participant_evidence_clusters();
        let event_clusters = evidence_refs
            .iter()
            .filter_map(|reference| self.evidence_event_clusters.get(reference))
            .cloned()
            .collect::<BTreeSet<_>>();
        let novel_refs = evidence_refs
            .difference(&previous_refs)
            .cloned()
            .collect::<Vec<_>>();
        let novel_event_cluster_ids = event_clusters
            .difference(&previous_clusters)
            .cloned()
            .collect::<Vec<_>>();
        Ok(json!({
            "authority": "rust_phase2_event_lineage_v1",
            "evidence_refs": evidence_refs.into_iter().collect::<Vec<_>>(),
            "event_cluster_ids": event_clusters.into_iter().collect::<Vec<_>>(),
            "novel_evidence_refs": novel_refs,
            "novel_event_cluster_ids": novel_event_cluster_ids,
        }))
    }

    fn participant_evidence_refs(&self) -> BTreeSet<String> {
        self.nodes
            .iter()
            .filter(|node| {
                matches!(
                    node.kind,
                    StreeNodeKind::Submission | StreeNodeKind::Agreement
                )
            })
            .flat_map(|node| {
                node.payload
                    .get("evidence_refs")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .collect()
    }

    fn participant_evidence_clusters(&self) -> BTreeSet<String> {
        self.nodes
            .iter()
            .filter(|node| {
                matches!(
                    node.kind,
                    StreeNodeKind::Submission | StreeNodeKind::Agreement
                )
            })
            .flat_map(|node| {
                node.payload
                    .pointer("/evidence_delta/event_cluster_ids")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .collect()
    }

    fn continuation_allowed(&self) -> bool {
        !self.initial_collision_complete()
            || self
                .nodes
                .iter()
                .filter(|node| {
                    matches!(
                        node.kind,
                        StreeNodeKind::Submission | StreeNodeKind::Agreement
                    ) && node.round > 0
                })
                .any(|node| {
                    node.payload
                        .pointer("/evidence_delta/novel_event_cluster_ids")
                        .and_then(Value::as_array)
                        .is_some_and(|items| !items.is_empty())
                })
    }

    fn continuation_gate(&self) -> Value {
        let post_collision_novel_event_clusters = self
            .nodes
            .iter()
            .filter(|node| {
                matches!(
                    node.kind,
                    StreeNodeKind::Submission | StreeNodeKind::Agreement
                ) && node.round > 0
            })
            .flat_map(|node| {
                node.payload
                    .pointer("/evidence_delta/novel_event_cluster_ids")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .collect::<BTreeSet<_>>();
        let direct_collision_complete = self.initial_collision_complete();
        let continuation_allowed = self.continuation_allowed();
        json!({
            "authority": "rust_phase2_information_gain_gate_v1",
            "direct_collision_complete": direct_collision_complete,
            "continuation_allowed": continuation_allowed,
            "post_collision_novel_event_cluster_ids": post_collision_novel_event_clusters,
            "reason": if continuation_allowed {
                "initial_collision_pending_or_new_event_observed"
            } else {
                "close_required_no_new_event_after_direct_collision"
            },
        })
    }

    fn information_gain_summary(&self) -> Value {
        let participant_nodes = self
            .nodes
            .iter()
            .filter(|node| {
                matches!(
                    node.kind,
                    StreeNodeKind::Submission | StreeNodeKind::Agreement
                )
            })
            .collect::<Vec<_>>();
        let post_collision_novel_event_cluster_ids = participant_nodes
            .iter()
            .filter(|node| node.round > 0)
            .flat_map(|node| {
                node.payload
                    .pointer("/evidence_delta/novel_event_cluster_ids")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .collect::<BTreeSet<_>>();
        json!({
            "authority": "rust_phase2_information_gain_gate_v1",
            "participant_submission_count": participant_nodes.len(),
            "post_collision_novel_event_cluster_ids": post_collision_novel_event_cluster_ids,
            "continuation_gate": self.continuation_gate(),
        })
    }

    fn independence_assessment(&self) -> Value {
        let side_clusters = |actor| {
            self.nodes
                .iter()
                .filter(|node| {
                    matches!(
                        node.kind,
                        StreeNodeKind::Submission | StreeNodeKind::Agreement
                    ) && node.from == Some(actor)
                })
                .flat_map(|node| {
                    node.payload
                        .pointer("/evidence_delta/event_cluster_ids")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_str)
                        .map(ToOwned::to_owned)
                })
                .collect::<BTreeSet<_>>()
        };
        let bull_clusters = side_clusters(DebateActor::Bull);
        let bear_clusters = side_clusters(DebateActor::Bear);
        let shared_clusters = bull_clusters
            .intersection(&bear_clusters)
            .cloned()
            .collect::<BTreeSet<_>>();
        let bull_unique = bull_clusters
            .difference(&shared_clusters)
            .cloned()
            .collect::<BTreeSet<_>>();
        let bear_unique = bear_clusters
            .difference(&shared_clusters)
            .cloned()
            .collect::<BTreeSet<_>>();
        let model_independence = self
            .independence_context
            .get("model_independence")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let novel_event_observed = self
            .nodes
            .iter()
            .filter(|node| {
                matches!(
                    node.kind,
                    StreeNodeKind::Submission | StreeNodeKind::Agreement
                ) && node.round > 0
            })
            .any(|node| {
                node.payload
                    .pointer("/evidence_delta/novel_event_cluster_ids")
                    .and_then(Value::as_array)
                    .is_some_and(|items| !items.is_empty())
            });
        let adjustment_eligible = self.initial_collision_complete()
            && model_independence == "distinct_models"
            && novel_event_observed;
        json!({
            "authority": "rust_phase2_independence_gate_v1",
            "shared_warmup": self.independence_context.get("shared_warmup").cloned().unwrap_or(Value::Bool(true)),
            "bull_model": self.independence_context.get("bull_model").cloned().unwrap_or(Value::Null),
            "bear_model": self.independence_context.get("bear_model").cloned().unwrap_or(Value::Null),
            "model_independence": model_independence,
            "shared_event_cluster_ids": shared_clusters,
            "bull_unique_event_cluster_ids": bull_unique,
            "bear_unique_event_cluster_ids": bear_unique,
            "novel_event_observed_after_collision": novel_event_observed,
            "adjustment_eligible": adjustment_eligible,
            "reason": if adjustment_eligible {
                "distinct_models_and_new_event_after_direct_collision"
            } else if model_independence == "same_model" {
                "same_model_shared_warmup_is_correlated_not_an_independent_vote"
            } else if !novel_event_observed {
                "no_new_event_after_direct_collision"
            } else {
                "model_independence_not_proven"
            },
        })
    }

    fn validate_controller_accepted_evidence(
        &self,
        object: &serde_json::Map<String, Value>,
        consensus_claim_ids: &[String],
        controller_verified_evidence_refs: &BTreeSet<String>,
    ) -> Result<Vec<Value>> {
        let expected_claim_ids = consensus_claim_ids.iter().cloned().collect::<BTreeSet<_>>();
        let accepted_claims = object
            .get("accepted_claims")
            .and_then(Value::as_array)
            .context("consensus close requires accepted_claims")?;
        if accepted_claims.len() != expected_claim_ids.len() {
            bail!(
                "consensus close accepted_claims must cover each current Bull/Bear agreement exactly once"
            )
        }
        let mut canonical = BTreeMap::<String, BTreeSet<String>>::new();
        for accepted_claim in accepted_claims {
            let accepted_claim = accepted_claim
                .as_object()
                .context("consensus close accepted_claims must contain objects")?;
            let claim_id = required_string(accepted_claim, "claim_id", 256)?;
            if !expected_claim_ids.contains(&claim_id) {
                bail!("consensus close accepted_claims references a non-current claim")
            }
            let evidence_refs = accepted_claim
                .get("evidence_refs")
                .and_then(Value::as_array)
                .context("consensus close accepted_claims requires evidence_refs")?;
            if evidence_refs.is_empty() || evidence_refs.len() > 3 {
                bail!(
                    "consensus close accepted_claims evidence_refs must contain one to three stable evidence IDs"
                )
            }
            let declared_links = self.claim_declared_evidence_links(&claim_id)?;
            let mut accepted_refs = BTreeSet::new();
            for evidence_ref in evidence_refs {
                let evidence_ref = evidence_ref
                    .as_str()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .context(
                        "consensus close accepted_claims evidence_refs must contain strings",
                    )?;
                if !is_complete_evidence_ref(evidence_ref) {
                    bail!(
                        "consensus close accepted_claims evidence_refs must use complete stable evidence IDs"
                    )
                }
                if !declared_links.contains(evidence_ref) {
                    bail!(
                        "consensus close accepted evidence must have a participant-declared claim relation"
                    )
                }
                if !controller_verified_evidence_refs.contains(evidence_ref) {
                    bail!(
                        "consensus close accepted evidence must have been observed by Rust in the Controller turn"
                    )
                }
                if !accepted_refs.insert(evidence_ref.to_owned()) {
                    bail!("consensus close accepted_claims contains duplicate evidence_refs")
                }
            }
            if canonical.insert(claim_id, accepted_refs).is_some() {
                bail!("consensus close accepted_claims contains duplicate claim_id")
            }
        }
        if canonical.keys().cloned().collect::<BTreeSet<_>>() != expected_claim_ids {
            bail!(
                "consensus close accepted_claims must cover each current Bull/Bear agreement exactly once"
            )
        }
        Ok(canonical
            .into_iter()
            .map(|(claim_id, evidence_refs)| {
                json!({
                    "claim_id": claim_id,
                    "evidence_refs": evidence_refs,
                    "authority": "rust_controller_observed_consensus_evidence_v1",
                })
            })
            .collect())
    }

    fn claim_declared_evidence_links(&self, claim_id: &str) -> Result<BTreeSet<String>> {
        let node = self
            .nodes
            .iter()
            .find(|node| node.node_id == claim_id)
            .with_context(|| format!("consensus claim {claim_id} is absent from the topic tree"))?;
        if !matches!(
            node.kind,
            StreeNodeKind::Submission | StreeNodeKind::Agreement
        ) {
            bail!("consensus claim {claim_id} is not a participant submission")
        }
        let evidence_links = node
            .payload
            .get("evidence_links")
            .and_then(Value::as_array)
            .with_context(|| format!("consensus claim {claim_id} has no evidence_links"))?;
        let links = evidence_links
            .iter()
            .filter_map(|link| link.get("evidence_ref").and_then(Value::as_str))
            .map(ToOwned::to_owned)
            .collect::<BTreeSet<_>>();
        if links.is_empty() {
            bail!("consensus claim {claim_id} has no evidence-linked support")
        }
        Ok(links)
    }

    fn structured_claim_ledger(&self) -> Vec<Value> {
        self.nodes
            .iter()
            .filter(|node| {
                matches!(node.kind, StreeNodeKind::Submission | StreeNodeKind::Agreement)
            })
            .map(|node| {
                let stance = node
                    .payload
                    .get("stance")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                let status = match stance {
                    "agree" => "agreed",
                    "partial_agree" => "partially_agreed",
                    "retract" => "retracted",
                    "needs_evidence" => "unverifiable",
                    "no_new_info" => "duplicate",
                    _ => "contested",
                };
                json!({
                    "claim_id": node.node_id,
                    "from": node.from,
                    "round": node.round,
                    "stance": stance,
                    "status": status,
                    "reply_to_claim_id": node.payload.get("reply_to_node_id").cloned().unwrap_or(Value::Null),
                    "evidence_refs": node.payload.get("evidence_refs").cloned().unwrap_or_else(|| json!([])),
                    "evidence_links": node.payload.get("evidence_links").cloned().unwrap_or_else(|| json!([])),
                    "evidence_delta": node.payload.get("evidence_delta").cloned().unwrap_or(Value::Null),
                })
            })
            .collect()
    }

    fn close_after_controller_failure(&mut self, failure: &StreeNode) {
        self.status = DebateStatus::Closed;
        self.closure = Some(json!({
            "reason": "agent_failure",
            "message": "Controller failed after bounded retries; Rust closed without inferring agreement.",
            "node_id": failure.node_id,
            "round": self.round,
            "controller_decided": false,
        }));
        for participant in self.participants.values_mut() {
            participant.status = ParticipantStatus::Closed;
        }
    }
}

fn required_string(
    object: &serde_json::Map<String, Value>,
    field: &str,
    max_chars: usize,
) -> Result<String> {
    let value = object
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .with_context(|| format!("stree payload requires non-empty {field}"))?;
    if value.chars().count() > max_chars {
        bail!("stree payload field {field} exceeds {max_chars} characters");
    }
    Ok(value)
}

/// The Controller receives the evidence-bearing part of a participant turn,
/// not the long-form report that happened to precede it.  The report remains
/// in the immutable node for audit, while this compact packet removes a
/// controllable source of length, placement, and role-label bias.
fn controller_visible_payload(node: &StreeNode) -> Value {
    let message = node
        .payload
        .get("message")
        .and_then(Value::as_str)
        .map(|value| value.chars().take(600).collect::<String>())
        .unwrap_or_default();
    let evidence_refs = node
        .payload
        .get("evidence_refs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();
    let evidence_links = node
        .payload
        .get("evidence_links")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|link| {
            let evidence_ref = link.get("evidence_ref")?.as_str()?;
            let relation = link.get("relation")?.as_str()?;
            Some(json!({"evidence_ref": evidence_ref, "relation": relation}))
        })
        .collect::<Vec<_>>();
    json!({
        "stance": node.payload.get("stance").cloned().unwrap_or(Value::Null),
        "message": message,
        "reply_to_node_id": node.payload.get("reply_to_node_id").cloned().unwrap_or(Value::Null),
        "evidence_refs": evidence_refs,
        "evidence_links": evidence_links,
        "evidence_delta": node.payload.get("evidence_delta").cloned().unwrap_or(Value::Null),
        "failure": node.payload.get("error").cloned().unwrap_or(Value::Null),
        "packet_policy": "role_blinded_compact_claim_v1",
    })
}

fn is_complete_evidence_ref(reference: &str) -> bool {
    ["idx-", "technical-", "jin10-", "web-"]
        .into_iter()
        .find_map(|prefix| reference.strip_prefix(prefix))
        .is_some_and(|digest| {
            digest.len() == 64
                && digest
                    .chars()
                    .all(|character| character.is_ascii_hexdigit())
        })
}

fn validate_stance_message_consistency(stance: &str, message: &str) -> Result<()> {
    let normalized = message.to_lowercase();
    let explicit_agreement = ["我同意", "同意对方", "认可对方", "i agree", "we agree"]
        .iter()
        .any(|marker| normalized.contains(marker));
    let explicit_rejection = [
        "我不同意",
        "反对对方",
        "拒绝该",
        "i disagree",
        "we disagree",
    ]
    .iter()
    .any(|marker| normalized.contains(marker));
    // A challenge can concede a premise while disputing the conclusion.  Treat
    // an agreement/rejection marker as a disposition only when it is not
    // qualified by an explicit counterargument; otherwise a valid direct
    // collision is incorrectly recorded as a failed debate turn.
    let has_counterargument = [
        "但",
        "但是",
        "然而",
        "不过",
        "相反",
        "尽管",
        "不足以",
        "尚未",
        "并不",
        "不应",
        "but ",
        "however",
        " yet",
        "although",
        "unless",
        "not enough",
        "does not",
    ]
    .iter()
    .any(|marker| normalized.contains(marker));
    if stance == "challenge" && explicit_agreement && !has_counterargument {
        bail!("challenge stance contradicts an explicit agreement in message")
    }
    if stance == "agree" && explicit_rejection && !has_counterargument {
        bail!("agree stance contradicts an explicit rejection in message")
    }
    Ok(())
}

fn parse_targets(value: Option<&Value>) -> Result<Vec<DebateActor>> {
    let values = value
        .and_then(Value::as_array)
        .context("controller route requires targets array")?;
    let mut targets = Vec::new();
    for target in values {
        let target = target
            .as_str()
            .context("controller route target must be a string")?;
        let actor = match target {
            "bull" => DebateActor::Bull,
            "bear" => DebateActor::Bear,
            _ => bail!("controller route target must be bull or bear"),
        };
        if !targets.contains(&actor) {
            targets.push(actor);
        }
    }
    if targets.is_empty() {
        bail!("controller route requires at least one target");
    }
    Ok(targets)
}

#[cfg(test)]
mod tests {
    use super::*;

    const EVIDENCE_REF: &str =
        "technical-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const SAME_EVENT_DIFFERENT_REF: &str =
        "web-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn tree() -> TopicDebateTree {
        let mut tree = TopicDebateTree::open(
            "topic-a",
            json!({"topic": "rate path", "decision_hinge": "yield response"}),
            3,
        )
        .unwrap();
        tree.register_evidence_refs([EVIDENCE_REF]).unwrap();
        tree
    }

    fn submission(stance: &str, reply_to_node_id: Option<&str>) -> Value {
        submission_with_evidence(stance, reply_to_node_id, EVIDENCE_REF)
    }

    fn submission_with_evidence(
        stance: &str,
        reply_to_node_id: Option<&str>,
        evidence_ref: &str,
    ) -> Value {
        let mut value = json!({
            "stance": stance,
            "message": format!("{stance} with evidence boundary"),
            "evidence_refs": [evidence_ref],
            "evidence_links": [{"evidence_ref": evidence_ref, "relation": "supports"}]
        });
        if let Some(reply_to_node_id) = reply_to_node_id {
            value["reply_to_node_id"] = json!(reply_to_node_id);
        }
        value
    }

    #[test]
    fn both_initial_participants_run_before_controller_wakes() {
        let mut tree = tree();
        let first = tree.next_dispatch().unwrap();
        assert_eq!(first.actor, DebateActor::Bull);
        tree.submit(DebateActor::Bull, submission("challenge", None))
            .unwrap();

        let second = tree.next_dispatch().unwrap();
        assert_eq!(second.actor, DebateActor::Bear);
        tree.submit(DebateActor::Bear, submission("challenge", None))
            .unwrap();

        let controller = tree.next_dispatch().unwrap();
        assert_eq!(controller.actor, DebateActor::Controller);
        assert_eq!(controller.deliveries.len(), 2);
        let injection = tree.injected_user_message(&controller.deliveries).unwrap();
        let payload: Value = serde_json::from_str(injection.trim_start_matches("stree: ")).unwrap();
        assert_eq!(payload["deliveries"].as_array().unwrap().len(), 2);
        assert_eq!(payload["deliveries"][0]["node_id"], "topic-a:stree:3");
        assert_eq!(payload["deliveries"][1]["node_id"], "topic-a:stree:4");
    }

    #[test]
    fn controller_receives_the_entire_final_collision_wave_before_closing() {
        let mut tree = TopicDebateTree::open(
            "topic-a",
            json!({"topic": "rate path", "decision_hinge": "yield response"}),
            1,
        )
        .unwrap();
        tree.register_evidence_refs([EVIDENCE_REF]).unwrap();

        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Bull);
        let bull_opening = tree
            .submit(DebateActor::Bull, submission("challenge", None))
            .unwrap();
        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Bear);
        let bear_opening = tree
            .submit(DebateActor::Bear, submission("challenge", None))
            .unwrap();

        let opening_controller = tree.next_dispatch().unwrap();
        assert_eq!(opening_controller.actor, DebateActor::Controller);
        assert_eq!(opening_controller.deliveries.len(), 2);
        let routes = tree
            .controller_route(json!({
                "targets": ["bull", "bear"],
                "reply_to_node_id": bear_opening.node_id,
                "message": "both sides must address the opposing opening"
            }))
            .unwrap();

        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Bull);
        let bull_reply = tree
            .submit(
                DebateActor::Bull,
                submission("partial_agree", Some(&routes[0].node_id)),
            )
            .unwrap();
        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Bear);
        let bear_reply = tree
            .submit(
                DebateActor::Bear,
                submission("partial_agree", Some(&routes[1].node_id)),
            )
            .unwrap();

        let final_controller = tree.next_dispatch().unwrap();
        assert_eq!(final_controller.actor, DebateActor::Controller);
        assert_eq!(final_controller.deliveries.len(), 2);
        assert_eq!(
            final_controller
                .deliveries
                .iter()
                .map(|delivery| delivery.node_id.as_str())
                .collect::<Vec<_>>(),
            vec![bull_reply.node_id.as_str(), bear_reply.node_id.as_str()]
        );
        let injection = tree
            .injected_user_message(&final_controller.deliveries)
            .unwrap();
        let payload: Value = serde_json::from_str(injection.trim_start_matches("stree: ")).unwrap();
        assert_eq!(payload["terminal_close_required"], true);
        assert!(tree
            .deliveries
            .iter()
            .filter(|delivery| delivery.target == DebateActor::Controller)
            .all(|delivery| delivery.delivered));

        tree.controller_close(json!({
            "reason": "unresolved_disagreement",
            "message": "both final replies were reviewed before the terminal decision"
        }))
        .unwrap();
        assert_eq!(
            tree.closure.as_ref().unwrap()["unresolved_claim_ids"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(bull_opening.node_id, "topic-a:stree:3");
    }

    #[test]
    fn stance_cannot_contradict_an_explicit_message_disposition() {
        let mut tree = tree();
        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Bull);
        let mut payload = submission("challenge", None);
        payload["message"] = json!("我同意对方的核心结论");

        assert!(tree.submit(DebateActor::Bull, payload).is_err());
    }

    #[test]
    fn submission_rejects_evidence_that_rust_did_not_observe() {
        let mut tree = tree();
        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Bull);
        let mut payload = submission("challenge", None);
        payload["evidence_refs"] =
            json!(["technical-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]);
        payload["evidence_links"] = json!([{
            "evidence_ref": "technical-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "relation": "supports"
        }]);

        let error = tree.submit(DebateActor::Bull, payload).unwrap_err();
        assert!(error.to_string().contains("not observed by Rust"));
    }

    #[test]
    fn challenge_can_concede_a_premise_while_rejecting_the_conclusion() {
        let mut tree = tree();
        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Bull);
        let mut payload = submission("challenge", None);
        payload["message"] =
            json!("我同意转向必须由硬触发量化，但当前无会后供需硬确认；尚不足以否定下行主线。");

        assert!(tree.submit(DebateActor::Bull, payload).is_ok());
    }

    #[test]
    fn controller_routes_only_complete_collision_waves() {
        let mut tree = tree();
        tree.next_dispatch();
        tree.submit(DebateActor::Bull, submission("challenge", None))
            .unwrap();
        tree.next_dispatch();
        let bear = tree
            .submit(DebateActor::Bear, submission("challenge", None))
            .unwrap();
        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Controller);

        assert!(tree
            .controller_route(json!({
                "targets": ["bear"],
                "reply_to_node_id": bear.node_id,
                "message": "one-sided extra round"
            }))
            .is_err());
    }

    #[test]
    fn controller_cannot_close_before_direct_collision_but_can_close_unresolved_afterward() {
        let mut tree = tree();
        tree.next_dispatch();
        let bull = tree
            .submit(DebateActor::Bull, submission("challenge", None))
            .unwrap();
        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Bear);
        let bear = tree
            .submit(DebateActor::Bear, submission("challenge", None))
            .unwrap();
        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Controller);
        assert!(tree
            .controller_close(json!({"reason":"unresolved_disagreement", "message":"too early"}))
            .is_err());
        tree.controller_route(json!({
            "targets": ["bull", "bear"],
            "reply_to_node_id": bear.node_id,
            "message": "respond to the opposing initial position"
        }))
        .unwrap();

        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Bull);
        tree.submit(
            DebateActor::Bull,
            submission("partial_agree", Some(&bear.node_id)),
        )
        .unwrap();
        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Bear);
        tree.submit(
            DebateActor::Bear,
            submission("challenge", Some(&bull.node_id)),
        )
        .unwrap();
        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Controller);
        tree.controller_close(json!({
            "reason":"unresolved_disagreement",
            "message":"the remaining hinge is evidence-bound"
        }))
        .unwrap();
        assert!(tree.is_closed());
    }

    #[test]
    fn controller_accepts_its_delivered_receipt_and_canonicalizes_to_node_id() {
        let mut tree = tree();
        tree.next_dispatch();
        tree.submit(DebateActor::Bull, submission("challenge", None))
            .unwrap();
        tree.next_dispatch();
        let bear = tree
            .submit(DebateActor::Bear, submission("challenge", None))
            .unwrap();

        let controller = tree.next_dispatch().unwrap();
        assert_eq!(controller.actor, DebateActor::Controller);
        let receipt_id = controller
            .deliveries
            .iter()
            .find(|delivery| delivery.node_id == bear.node_id)
            .unwrap()
            .delivery_id
            .clone();
        tree.controller_route(json!({
            "targets": ["bull", "bear"],
            "reply_to_node_id": receipt_id,
            "message": "both sides must address the opposing opening"
        }))
        .unwrap();

        let route = tree
            .nodes
            .iter()
            .find(|node| node.kind == StreeNodeKind::Route)
            .unwrap();
        assert_eq!(route.payload["reply_to_node_id"], bear.node_id);
        assert_eq!(route.payload["reply_to_delivery_id"], receipt_id);
    }

    #[test]
    fn initial_collision_routes_each_side_to_the_opposing_seed_and_canonicalizes_replies() {
        let mut tree = tree();
        tree.next_dispatch();
        let bull = tree
            .submit(
                DebateActor::Bull,
                submission("challenge", Some("topic-a:stree:1")),
            )
            .unwrap();
        tree.next_dispatch();
        let bear = tree
            .submit(
                DebateActor::Bear,
                submission("challenge", Some("topic-a:stree:2")),
            )
            .unwrap();

        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Controller);
        let routes = tree
            .controller_route(json!({
                "targets": ["bull", "bear"],
                "reply_to_node_id": bear.node_id,
                "message": "respond directly to the opposing seed position"
            }))
            .unwrap();
        assert_eq!(routes.len(), 2);
        assert_eq!(routes[0].payload["reply_to_node_id"], bear.node_id);
        assert_eq!(routes[1].payload["reply_to_node_id"], bull.node_id);

        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Bull);
        let bull_route = routes[0].node_id.clone();
        let bull_reply = tree
            .submit(
                DebateActor::Bull,
                submission(
                    "partial_agree",
                    Some(&bull_route.replace(":stree:", ":stree-")),
                ),
            )
            .unwrap();
        assert_eq!(bull_reply.payload["reply_to_node_id"], routes[0].node_id);

        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Bear);
        let bear_route = routes[1].node_id.clone();
        tree.submit(
            DebateActor::Bear,
            submission("challenge", Some(&bear_route.replace(":stree:", ":stree/"))),
        )
        .unwrap();

        assert!(tree.initial_collision_complete());
        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Controller);
        tree.controller_close(json!({
            "reason": "unresolved_disagreement",
            "message": "both sides directly answered the opposing seed"
        }))
        .unwrap();
    }

    #[test]
    fn side_claim_aliases_resolve_to_the_initial_submission_node() {
        let mut tree = tree();
        tree.next_dispatch();
        let bull = tree
            .submit(DebateActor::Bull, submission("challenge", None))
            .unwrap();
        tree.next_dispatch();
        let bear = tree
            .submit(DebateActor::Bear, submission("challenge", None))
            .unwrap();

        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Controller);
        let routes = tree
            .controller_route(json!({
                "targets": ["bull", "bear"],
                "reply_to_node_id": "topic-a:bear:1",
                "message": "both sides must answer the opposing initial claim"
            }))
            .unwrap();

        assert_eq!(routes[0].payload["reply_to_node_id"], bear.node_id);
        assert_eq!(routes[1].payload["reply_to_node_id"], bull.node_id);
    }

    #[test]
    fn routed_participant_reply_binds_to_its_delivery_receipt() {
        let mut tree = tree();
        tree.next_dispatch();
        let bull = tree
            .submit(DebateActor::Bull, submission("challenge", None))
            .unwrap();
        tree.next_dispatch();
        let bear = tree
            .submit(DebateActor::Bear, submission("challenge", None))
            .unwrap();

        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Controller);
        let routes = tree
            .controller_route(json!({
                "targets": ["bull", "bear"],
                "reply_to_node_id": bear.node_id,
                "message": "respond directly to the opposing initial claim"
            }))
            .unwrap();

        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Bull);
        let bull_reply = tree
            .submit(
                DebateActor::Bull,
                submission("challenge", Some(&bull.node_id)),
            )
            .unwrap();
        assert_eq!(bull_reply.payload["reply_to_node_id"], routes[0].node_id);
        assert_eq!(bull_reply.payload["reply_to_reference"], bull.node_id);

        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Bear);
        let bear_reply = tree
            .submit(
                DebateActor::Bear,
                submission("challenge", Some(&bear.node_id)),
            )
            .unwrap();
        assert_eq!(bear_reply.payload["reply_to_node_id"], routes[1].node_id);
        assert_eq!(bear_reply.payload["reply_to_reference"], bear.node_id);
        assert!(tree.initial_collision_complete());
    }

    #[test]
    fn actor_claim_references_resolve_for_controller_and_participants() {
        let mut tree = tree();
        tree.next_dispatch();
        let bull = tree
            .submit(DebateActor::Bull, submission("challenge", None))
            .unwrap();
        tree.next_dispatch();
        let bear = tree
            .submit(DebateActor::Bear, submission("challenge", None))
            .unwrap();

        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Controller);
        let routes = tree
            .controller_route(json!({
                "targets": ["bull", "bear"],
                "reply_to_node_id": "topic-a:bear:4",
                "message": "Both sides must answer the opposing claim"
            }))
            .unwrap();
        let bull_route = &routes[0];
        let bear_route = &routes[1];
        assert_eq!(bull_route.payload["reply_to_node_id"], bear.node_id);
        assert_eq!(bear_route.payload["reply_to_node_id"], bull.node_id);

        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Bull);
        let bull_reply = tree
            .submit(
                DebateActor::Bull,
                submission("partial_agree", Some("bear:4")),
            )
            .unwrap();
        assert_eq!(bull_reply.payload["reply_to_node_id"], bull_route.node_id);
        assert_eq!(bull_reply.payload["reply_to_reference"], "bear:4");

        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Bear);
        let bear_reply = tree
            .submit(
                DebateActor::Bear,
                submission("challenge", Some("topic-a:bull:3")),
            )
            .unwrap();
        assert_eq!(bear_reply.payload["reply_to_node_id"], bear_route.node_id);
        assert_eq!(bear_reply.payload["reply_to_reference"], "topic-a:bull:3");

        assert!(tree.initial_collision_complete());
        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Controller);
        tree.controller_close(json!({
            "reason": "unresolved_disagreement",
            "message": "both sides answered the opposing claim references"
        }))
        .unwrap();
    }

    #[test]
    fn participant_failure_retries_once_then_notifies_controller() {
        let mut tree = tree();
        tree.next_dispatch();
        tree.record_failure(DebateActor::Bull, "gateway timeout", 1)
            .unwrap();
        assert_eq!(
            tree.participants[&DebateActor::Bull].status,
            ParticipantStatus::RetryScheduled
        );
        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Bear);
        tree.submit(DebateActor::Bear, submission("challenge", None))
            .unwrap();
        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Controller);
        tree.controller_wait(json!({"message": "Bull retry is scheduled"}))
            .unwrap();
        let retry = tree.next_dispatch().unwrap();
        assert_eq!(retry.actor, DebateActor::Bull);
        assert!(retry.deliveries.is_empty());
        tree.record_failure(DebateActor::Bull, "gateway timeout", 1)
            .unwrap();
        assert_eq!(
            tree.participants[&DebateActor::Bull].status,
            ParticipantStatus::Failed
        );
        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Controller);
    }

    #[test]
    fn safety_limit_closes_without_inventing_consensus() {
        let mut tree = tree();
        tree.close_after_safety_limit().unwrap();

        assert!(tree.is_closed());
        assert_eq!(tree.closure.as_ref().unwrap()["reason"], "round_limit");
        assert_eq!(tree.closure.as_ref().unwrap()["controller_decided"], false);
        assert_eq!(tree.closure.as_ref().unwrap()["safety_enforced"], true);
        assert_eq!(tree.nodes.last().unwrap().kind, StreeNodeKind::Close);
        assert!(tree.next_dispatch().is_none());
    }

    #[test]
    fn controller_packet_is_role_blinded_and_order_invariant() {
        let mut tree = tree();
        tree.next_dispatch();
        let mut bull = submission("challenge", None);
        bull["message"] = json!("Bull message with deliberately different wording");
        bull["report"] = json!("Bull report must never be delivered to the Controller");
        tree.submit(DebateActor::Bull, bull).unwrap();
        tree.next_dispatch();
        let mut bear = submission("challenge", None);
        bear["message"] = json!("Bear message with deliberately different wording");
        bear["report"] = json!("Bear report must never be delivered to the Controller");
        tree.submit(DebateActor::Bear, bear).unwrap();

        let dispatch = tree.next_dispatch().unwrap();
        assert_eq!(dispatch.actor, DebateActor::Controller);
        let canonical = tree.injected_user_message(&dispatch.deliveries).unwrap();
        let mut reversed = dispatch.deliveries.clone();
        reversed.reverse();
        assert_eq!(canonical, tree.injected_user_message(&reversed).unwrap());

        let packet: Value = serde_json::from_str(canonical.trim_start_matches("stree: ")).unwrap();
        assert_eq!(packet["presentation_policy"]["role_labels_redacted"], true);
        for delivery in packet["deliveries"].as_array().unwrap() {
            assert!(delivery.get("from").is_none());
            assert!(delivery["payload"].get("report").is_none());
        }
    }

    #[test]
    fn consensus_requires_controller_observed_claim_evidence() {
        let mut tree = tree();
        tree.set_independence_context("model-a", "model-b");
        tree.next_dispatch();
        let bull_opening = tree
            .submit(DebateActor::Bull, submission("challenge", None))
            .unwrap();
        tree.next_dispatch();
        let bear_opening = tree
            .submit(DebateActor::Bear, submission("challenge", None))
            .unwrap();
        tree.next_dispatch();
        let routes = tree
            .controller_route(json!({
                "targets": ["bull", "bear"],
                "reply_to_node_id": bear_opening.node_id,
                "message": "both sides must answer the opposing opening"
            }))
            .unwrap();

        tree.next_dispatch();
        let bull_agreement = tree
            .submit(
                DebateActor::Bull,
                submission("agree", Some(&routes[0].node_id)),
            )
            .unwrap();
        tree.next_dispatch();
        let bear_agreement = tree
            .submit(
                DebateActor::Bear,
                submission("agree", Some(&routes[1].node_id)),
            )
            .unwrap();
        assert!(tree.initial_collision_complete());
        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Controller);

        let close = || {
            json!({
                "reason": "consensus",
                "message": "Both current agreements were independently checked.",
                "accepted_claims": [
                    {"claim_id": bull_agreement.node_id, "evidence_refs": [EVIDENCE_REF]},
                    {"claim_id": bear_agreement.node_id, "evidence_refs": [EVIDENCE_REF]}
                ]
            })
        };
        let error = tree
            .controller_close_with_verified_evidence(close(), &BTreeSet::new())
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("observed by Rust in the Controller turn"));

        let mut verified = BTreeSet::new();
        verified.insert(EVIDENCE_REF.to_owned());
        tree.controller_close_with_verified_evidence(close(), &verified)
            .unwrap();
        assert_eq!(
            tree.closure.as_ref().unwrap()["accepted_evidence"][0]["authority"],
            "rust_controller_observed_consensus_evidence_v1"
        );
        assert_eq!(
            tree.closure.as_ref().unwrap()["consensus_claim_ids"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(bull_opening.node_id, "topic-a:stree:3");
    }

    #[test]
    fn same_event_under_a_new_id_cannot_buy_an_extra_debate_round() {
        let mut tree = TopicDebateTree::open(
            "topic-a",
            json!({"topic": "rate path", "decision_hinge": "yield response"}),
            3,
        )
        .unwrap();
        tree.register_evidence_ref_cluster(EVIDENCE_REF, "url:sameevent")
            .unwrap();
        tree.register_evidence_ref_cluster(SAME_EVENT_DIFFERENT_REF, "url:sameevent")
            .unwrap();
        tree.set_independence_context("model-a", "model-b");

        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Bull);
        let bull = tree
            .submit(
                DebateActor::Bull,
                submission_with_evidence("challenge", None, EVIDENCE_REF),
            )
            .unwrap();
        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Bear);
        let bear = tree
            .submit(
                DebateActor::Bear,
                submission_with_evidence("challenge", None, EVIDENCE_REF),
            )
            .unwrap();
        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Controller);
        let routes = tree
            .controller_route(json!({
                "targets": ["bull", "bear"],
                "reply_to_node_id": bear.node_id,
                "message": "direct collision first"
            }))
            .unwrap();
        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Bull);
        tree.submit(
            DebateActor::Bull,
            submission_with_evidence(
                "challenge",
                Some(&routes[0].node_id),
                SAME_EVENT_DIFFERENT_REF,
            ),
        )
        .unwrap();
        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Bear);
        tree.submit(
            DebateActor::Bear,
            submission_with_evidence(
                "challenge",
                Some(&routes[1].node_id),
                SAME_EVENT_DIFFERENT_REF,
            ),
        )
        .unwrap();

        let controller = tree.next_dispatch().unwrap();
        let packet: Value = serde_json::from_str(
            tree.injected_user_message(&controller.deliveries)
                .unwrap()
                .trim_start_matches("stree: "),
        )
        .unwrap();
        assert_eq!(
            packet["rust_continuation_gate"]["continuation_allowed"],
            false
        );
        let error = tree
            .controller_route(json!({
                "targets": ["bull", "bear"],
                "reply_to_node_id": bull.node_id,
                "message": "attempt an unsupported extra round"
            }))
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("no newly observed evidence event"));
        tree.controller_close(json!({
            "reason": "evidence_exhausted",
            "message": "same event was only repackaged under a new ID"
        }))
        .unwrap();
        assert_eq!(
            tree.closure.as_ref().unwrap()["information_gain"]["continuation_gate"]
                ["continuation_allowed"],
            false
        );
    }

    #[test]
    fn same_model_shared_warmup_is_not_an_independent_probability_vote() {
        let mut tree = tree();
        tree.set_independence_context("same-model", "same-model");
        tree.next_dispatch();
        let bull = tree
            .submit(DebateActor::Bull, submission("challenge", None))
            .unwrap();
        tree.next_dispatch();
        let bear = tree
            .submit(DebateActor::Bear, submission("challenge", None))
            .unwrap();
        tree.next_dispatch();
        let routes = tree
            .controller_route(json!({
                "targets": ["bull", "bear"],
                "reply_to_node_id": bear.node_id,
                "message": "direct collision"
            }))
            .unwrap();
        tree.next_dispatch();
        tree.submit(
            DebateActor::Bull,
            submission("partial_agree", Some(&routes[0].node_id)),
        )
        .unwrap();
        tree.next_dispatch();
        tree.submit(
            DebateActor::Bear,
            submission("partial_agree", Some(&routes[1].node_id)),
        )
        .unwrap();
        tree.next_dispatch();
        tree.controller_close(json!({
            "reason": "unresolved_disagreement",
            "message": "same model views remain correlated"
        }))
        .unwrap();
        assert_eq!(
            tree.closure.as_ref().unwrap()["independence_assessment"]["adjustment_eligible"],
            false
        );
        assert_eq!(
            tree.closure.as_ref().unwrap()["independence_assessment"]["reason"],
            "same_model_shared_warmup_is_correlated_not_an_independent_vote"
        );
        assert_eq!(bull.node_id, "topic-a:stree:3");
    }
}
