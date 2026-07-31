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
use std::collections::BTreeMap;

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
    pub delivery: Option<StreeDelivery>,
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
        let delivery = self
            .deliveries
            .iter_mut()
            .find(|delivery| delivery.target == actor && !delivery.delivered)
            .map(|delivery| {
                delivery.delivered = true;
                delivery.clone()
            });
        self.participant_mut(actor)?.status = ParticipantStatus::Running;
        Some(DebateDispatch { actor, delivery })
    }

    pub fn injected_user_message(&self, delivery: &StreeDelivery) -> Result<String> {
        let node = self
            .nodes
            .iter()
            .find(|node| node.node_id == delivery.node_id)
            .context("stree delivery references an unknown node")?;
        Ok(format!(
            "stree: {}",
            serde_json::to_string(&json!({
                "delivery_id": delivery.delivery_id,
                "node_id": node.node_id,
                "sequence": node.sequence,
                "round": node.round,
                "from": node.from,
                "kind": node.kind,
                "payload": node.payload,
                "trusted_protocol": "phase2_topic_debate_tree"
            }))?
        ))
    }

    pub fn submit(&mut self, actor: DebateActor, payload: Value) -> Result<StreeNode> {
        self.ensure_open()?;
        if !matches!(actor, DebateActor::Bull | DebateActor::Bear) {
            bail!("only Bull or Bear may submit a debate position");
        }
        self.require_running(actor)?;
        let object = payload
            .as_object()
            .context("debate submission must be a JSON object")?;
        let stance = required_string(object, "stance", 32)?;
        if !matches!(
            stance.as_str(),
            "challenge" | "partial_agree" | "agree" | "retract" | "needs_evidence" | "no_new_info"
        ) {
            bail!("unsupported debate stance {stance:?}");
        }
        required_string(object, "message", 1_200)?;
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
        for target in targets {
            let node = self.append_node(
                Some(DebateActor::Controller),
                vec![target],
                StreeNodeKind::Route,
                payload.clone(),
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
        self.ensure_open()?;
        self.require_running(DebateActor::Controller)?;
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
        let node = self.append_node(
            Some(DebateActor::Controller),
            Vec::new(),
            StreeNodeKind::Close,
            payload.clone(),
        );
        self.status = DebateStatus::Closed;
        self.closure = Some(json!({
            "reason": reason,
            "message": object.get("message").cloned().unwrap_or(Value::Null),
            "node_id": node.node_id,
            "round": self.round,
            "controller_decided": true,
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
        bail!("controller route reply_to_node_id is not in this topic tree")
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
        let initial = |actor| {
            self.nodes.iter().find(|node| {
                node.kind == StreeNodeKind::Submission
                    && node.from == Some(actor)
                    && node.payload.get("reply_to_node_id").is_none()
            })
        };
        let Some(bull_initial) = initial(DebateActor::Bull) else {
            return false;
        };
        let Some(bear_initial) = initial(DebateActor::Bear) else {
            return false;
        };
        let directly_replied = |actor, opposing_node_id: &str| {
            self.nodes.iter().any(|node| {
                matches!(
                    node.kind,
                    StreeNodeKind::Submission | StreeNodeKind::Agreement
                ) && node.from == Some(actor)
                    && node.payload.get("reply_to_node_id").and_then(Value::as_str)
                        == Some(opposing_node_id)
            })
        };
        directly_replied(DebateActor::Bull, &bear_initial.node_id)
            && directly_replied(DebateActor::Bear, &bull_initial.node_id)
    }

    fn has_full_agreement(&self) -> bool {
        DebateActor::PARTICIPANTS.into_iter().all(|actor| {
            self.nodes.iter().any(|node| {
                node.kind == StreeNodeKind::Agreement
                    && node.from == Some(actor)
                    && node.payload.get("stance").and_then(Value::as_str) == Some("agree")
            })
        })
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

    fn tree() -> TopicDebateTree {
        TopicDebateTree::open(
            "topic-a",
            json!({"topic": "rate path", "decision_hinge": "yield response"}),
            3,
        )
        .unwrap()
    }

    fn submission(stance: &str, reply_to_node_id: Option<&str>) -> Value {
        let mut value = json!({
            "stance": stance,
            "message": format!("{stance} with evidence boundary"),
            "evidence_refs": ["idx-a"]
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
        let delivery = controller.delivery.unwrap();
        assert!(tree
            .injected_user_message(&delivery)
            .unwrap()
            .starts_with("stree: "));
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
        tree.controller_wait(json!({"message": "need Bear's initial position"}))
            .unwrap();
        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Controller);
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

        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Controller);
        tree.controller_wait(json!({"message": "wait for Bear opening"}))
            .unwrap();
        let controller = tree.next_dispatch().unwrap();
        let receipt_id = controller.delivery.unwrap().delivery_id;
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
        assert_eq!(tree.next_dispatch().unwrap().actor, DebateActor::Controller);
        tree.controller_wait(json!({"message": "Bear initial position is available"}))
            .unwrap();
        let retry = tree.next_dispatch().unwrap();
        assert_eq!(retry.actor, DebateActor::Bull);
        assert!(retry.delivery.is_none());
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
}
