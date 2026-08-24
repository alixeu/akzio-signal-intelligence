import Foundation
import ObservatoryKit

func runObserverContractChecks() {
    Check.suite("Observer transport policy") {
        Check.expect(
            ObserverTransportPolicy.standardRequestTimeout > 5,
            "ordinary requests outlive the Rust five-second subrequest cap"
        )
        Check.expect(
            ObserverTransportPolicy.snapshotRequestTimeout
                > ObserverTransportPolicy.standardRequestTimeout,
            "snapshot aggregation receives extra timeout margin"
        )
    }

    Check.suite("Rust observer contract") {
        let json = Data(#"""
        {
          "schema_version": 2,
          "generated_at": "2026-08-20T10:20:30.123Z",
          "event_cursor": 42,
          "core": {
            "ready": true,
            "readiness_ppm": 0,
            "auto_paper": true,
            "health": {
              "status": "paper_scheduler_fail_closed",
              "frozen": false,
              "scheduler_owner": null,
              "scheduler_epoch": null,
              "metrics": {},
              "alerts": []
            },
            "approval": {
              "status": "missing",
              "operator_identity": null,
              "reason": null,
              "expires_at": null
            }
          },
          "current_run": null,
          "recent_runs": [{
            "run": {
              "run_id": "observer-fixture-run",
              "purpose": "debug",
              "topology_id": "fixture-debug",
              "graph_artifact_id": "sha256:fixture",
              "created_at": "2026-08-20T10:00:00Z"
            },
            "status": "running",
            "finished_at": null,
            "revision": {},
            "tasks": [],
            "event_cursor": 42,
            "cancel_requested": false
        }],
        "run_summaries": [],
        "portfolio": {
            "status": "unavailable",
            "observed_at": null,
            "reason": "not configured",
            "data": null
        },
        "outcome": {
            "status": "pending",
            "observed_at": null,
            "reason": "not ready",
            "data": null
        },
        "learning": {
            "status": "available",
            "observed_at": "2026-08-20T10:20:30.123Z",
            "reason": null,
            "data": {"artifacts": [], "policy_transitions": []}
          }
        }
        """#.utf8)

        do {
            let summary = try ObserverContractProbe.decode(json)
            Check.equal(summary.schemaVersion, 2, "schema version")
            Check.equal(summary.eventCursor, 42, "event cursor")
            Check.equal(summary.runCount, 1, "run count")
            Check.equal(summary.firstRunID, "observer-fixture-run", "run id")
            Check.equal(summary.readinessPpm, 0, "Rust readiness score")
            Check.equal(summary.outcomeStatus, "pending", "outcome availability")
        } catch {
            Check.expect(false, "observer JSON decodes: \(error)")
        }
    }

    Check.suite("Rust observer trace contract") {
        let json = Data(#"""
        {
          "trajectory": [{
            "cursor": 7,
            "task_id": "task-critic",
            "turn": 2,
            "event_type": "tool.completed",
            "artifact_id": "sha256:claim",
            "artifact_kind": "claim",
            "tool": {
              "call_id": "call-7",
              "name": "read_context",
              "lifecycle": "completed"
            },
            "output_refs": [{
              "artifact_id": "sha256:claim",
              "kind": "claim"
            }]
          }],
          "artifacts": [{
            "artifact_id": "sha256:claim",
            "kind": "claim",
            "created_at": "2026-08-20T10:21:00Z",
            "payload": {"claim": "bounded structured output", "confidence_ppm": 800000}
          }]
        }
        """#.utf8)

        do {
            let summary = try ObserverContractProbe.decodeTrace(json)
            Check.equal(summary.artifactID, "sha256:claim", "trajectory artifact id")
            Check.equal(summary.toolCallID, "call-7", "tool call id")
            Check.equal(summary.toolName, "read_context", "tool name")
            Check.equal(summary.toolLifecycle, "completed", "tool lifecycle")
            Check.equal(summary.outputReferenceIDs, ["sha256:claim"], "output references")
            Check.equal(summary.structuredArtifactKinds, ["claim"], "structured artifacts")
            Check.expect(
                summary.firstStructuredPayload?.contains("bounded structured output") == true,
                "structured payload is available for the node inspector"
            )
        } catch {
            Check.expect(false, "observer trace JSON decodes: \(error)")
        }
    }

    Check.suite("Observer reasoning stream contract") {
        let json = Data(#"""
        {
          "type": "reasoning-delta",
          "run_id": "run-7",
          "task_id": "task-critic",
          "attempt_id": "attempt-2",
          "purpose": "research.critic",
          "turn": 2,
          "delta": "bounded summary"
        }
        """#.utf8)
        do {
            let summary = try ObserverContractProbe.decodeReasoning(json)
            Check.equal(summary.type, "reasoning-delta", "reasoning event type")
            Check.equal(summary.runID, "run-7", "reasoning run id")
            Check.equal(summary.taskID, "task-critic", "reasoning task id")
            Check.equal(summary.purpose, "research.critic", "reasoning purpose")
            Check.equal(summary.turn, 2, "reasoning turn")
            Check.equal(summary.delta, "bounded summary", "reasoning delta")
        } catch {
            Check.expect(false, "reasoning stream JSON decodes: \(error)")
        }
    }
}
