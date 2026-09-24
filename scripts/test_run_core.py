"""Offline HTTP-boundary tests for the formal run launcher."""
import contextlib
import hashlib
import io
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import tempfile
import threading
import unittest
import zipfile

import run_core


class _Core(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def _reply(self, value, status=200):
        body = json.dumps(value).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self.server.requests.append(("GET", self.path, self.headers.get("x-akzio-token")))
        if self.path == "/ready":
            if self.server.ready_behavior == "unauthorized":
                self._reply({"error": "wrong token"}, 401)
                return
            if self.server.ready_behavior == "echo_token":
                self._reply({"error": self.headers.get("x-akzio-token")}, 400)
                return
            if self.server.ready_behavior == "redirect":
                self.send_response(307)
                self.send_header("Location", f"http://127.0.0.1:{self.server.server_port}/capture")
                self.end_headers()
                return
            self._reply({"status": "ok",
                         "store_scope": self.server.store_scope,
                         "formal_run_bundle_supported": self.server.bundle_supported,
                         "decision_capable": self.server.decision_capable,
                         "decision_policy_status": (
                             "ready_for_current_decision" if self.server.decision_capable
                             else "unconfigured_fail_closed")})
        elif self.path == "/runs/run-1/replay":
            self._reply(self.server.replay)
        elif self.path == "/v1/observer/runs/run-1":
            self._reply(self.server.detail)
        else:
            self._reply({"error": "not found"}, 404)

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.server.requests.append(("POST", self.path, body))
        if self.path == "/runs":
            if self.server.post_behavior == "ambiguous":
                self.connection.close()
                return
            if self.server.post_behavior == "invalid_json":
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"run_id":')
                return
            if self.server.post_behavior == "closed":
                self._reply({"error": "Paper 暂未启动：交易时段不可用；未创建订单"}, 400)
                return
            self._reply({"run_id": "run-1"})
        elif self.path == "/control/store/export-run-bundle":
            target = Path(body["target"])
            if self.server.export_symlink:
                target.symlink_to(self.server.secret_dir, target_is_directory=True)
                self._reply({"integrity": {"status": "unavailable"}, "missing": []})
                return
            target.mkdir()
            (target / "EXPORT_STATUS").write_text("partial\n")
            missing = [{"reason": "artifact_row_unreadable"}] if self.server.export_missing else []
            (target / "manifest.json").write_text(json.dumps({
                "integrity": {"status": "partial", "unknown_after_crash_calls": 0,
                              "corruption_or_missing_blobs": 0, "dropped_records": 0,
                              "uncaptured_payloads": 1}, "missing": missing}))
            lines = [
                f'{hashlib.sha256((target / name).read_bytes()).hexdigest()}  {name}\n'
                for name in ("EXPORT_STATUS", "manifest.json")
            ]
            if self.server.corrupt_bundle:
                lines[0] = f'{"0" * 64}  EXPORT_STATUS\n'
            (target / "checksums.sha256").write_text("".join(lines))
            self._reply({"run_id": "run-1", "integrity": {"status": "partial",
                         "unknown_after_crash_calls": 0, "corruption_or_missing_blobs": 0,
                         "dropped_records": 0, "uncaptured_payloads": 1}, "missing": missing})
        else:
            self._reply({"error": "not found"}, 404)


class FormalRunTests(unittest.TestCase):
    def setUp(self):
        test_parent = Path(__file__).resolve().parents[1] / "target"
        test_parent.mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=test_parent)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _Core)
        self.server.requests = []
        self.server.ready_behavior = "normal"
        self.server.post_behavior = "normal"
        self.server.decision_capable = False
        self.server.store_scope = "canonical"
        self.server.bundle_supported = True
        self.server.export_missing = False
        self.server.corrupt_bundle = False
        self.server.export_symlink = False
        self.server.secret_dir = self.root / "private"
        self.server.secret_dir.mkdir()
        (self.server.secret_dir / "secret.json").write_text('{"token":"must-not-leak"}')
        self.server.replay = {
            "run_id": "run-1", "purpose": "position_plan", "status": "completed",
            "lifecycle": {"execution_status": "completed", "outcome_scheduled": False,
                          "numeric_outcome_sealed": False},
        }
        self.server.detail = {
            "workflow": {"run": {"run_id": "run-1", "purpose": "position_plan"}},
            "artifacts": [{"artifact_id": "decision-1", "kind": "decision",
                           "payload": {"targets": {"weights": {
                               "TQQQ": 0, "QQQ": 0, "SOXX": 0, "SOXL": 0}},
                               "research_plan": {"status": "qualified_recommendation",
                                   "validated": {"cash_weight_ppm": 600000,
                                       "allocations": [
                                           {"asset": "QQQ", "target_weight_ppm": 400000},
                                           {"asset": "TQQQ", "target_weight_ppm": 0},
                                           {"asset": "SOXX", "target_weight_ppm": 0},
                                           {"asset": "SOXL", "target_weight_ppm": 0}]}}}}],
        }
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        store = self.root / "canonical-store"
        store.mkdir()
        (store / ".daemon-token").write_text("local-test-token")
        self.config = self.root / "config.toml"
        self.config.write_text(
            f'[daemon]\nstore_root = "{store}"\n'
            f'http_addr = "127.0.0.1:{self.server.server_port}"\n'
            'debug_control = false\nmanual_paper = true\n'
        )

    def test_first_position_plan_uses_formal_run_and_reports_cold_start(self):
        self.server.detail["research_audit"] = {
            "progress": {"review_status": "accepted",
                         "proposal": {"artifact_id": "proposal-1", "kind": "decision_proposal"}}
        }
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = run_core.run(self.config, artifact_parent=self.root / "reports")
        self.assertEqual(exit_code, 0)
        self.assertEqual(
            [request[1] for request in self.server.requests if request[0] == "POST"],
            ["/runs", "/control/store/export-run-bundle"],
        )
        self.assertEqual(self.server.requests[1][2], {"purpose": "position_plan"})
        self.assertIn("FINAL_STATUS=completed", stdout.getvalue())
        self.assertIn("EXPORT_STATUS=partial", stdout.getvalue())
        self.assertIn("RESEARCH_PROPOSAL_STATUS=accepted", stdout.getvalue())
        self.assertIn("DECISION_STATUS=observed", stdout.getvalue())
        self.assertIn("OUTCOME_SCHEDULED=False", stdout.getvalue())
        archive = next((self.root / "reports").glob("*.zip"))
        with zipfile.ZipFile(archive) as zipped:
            summary_path = next(name for name in zipped.namelist() if name.endswith("/summary.json"))
            summary = json.loads(zipped.read(summary_path))
            self.assertEqual(summary["decision"]["targets"]["weights"]["QQQ"], 0)
            self.assertEqual(summary["decision"]["research_plan"]["validated"]["cash_weight_ppm"], 600000)
            self.assertNotIn(b"local-test-token", zipped.read(summary_path))
            self.assertFalse(any("store/" in name for name in zipped.namelist()))

    def test_cold_paper_run_reports_no_order_without_claiming_a_fill(self):
        self.server.replay = {
            "run_id": "run-1", "purpose": "paper",
            "status": "completed_with_execution_rejection",
            "lifecycle": {"execution_status": "completed_with_execution_rejection",
                          "outcome_scheduled": True, "numeric_outcome_sealed": False,
                          "retrospective_status": {"t1": "pending", "t3": "pending", "t5": "pending"}},
        }
        self.server.detail["workflow"]["run"]["purpose"] = "paper"
        self.server.detail["artifacts"].extend([
            {"artifact_id": "verdict-1", "kind": "execution_verdict",
             "payload": {"verdict": "no_order",
                         "no_order": {"blockers": ["unqualified_runtime"]}}},
            {"artifact_id": "schedule-1", "kind": "outcome_schedule", "payload": {}},
        ])
        with contextlib.redirect_stdout(io.StringIO()):
            result = run_core.run(self.config, paper=True, artifact_parent=self.root / "reports")
        self.assertEqual(result, 0)
        self.assertEqual(self.server.requests[1][2], {"purpose": "paper"})
        with zipfile.ZipFile(next((self.root / "reports").glob("*.zip"))) as zipped:
            summary = json.loads(zipped.read(next(
                name for name in zipped.namelist() if name.endswith("/summary.json"))))
        self.assertEqual(summary["execution_verdict"]["status"], "no_order")
        self.assertEqual(summary["order_submission"], "no_order")
        self.assertEqual(summary["paper_fills"], "no_order")
        self.assertEqual(summary["outcome_schedule"]["status"], "observed")
        self.assertEqual(summary["lifecycle"]["numeric_outcome_sealed"], False)

    def test_missing_formal_core_does_not_create_a_run_or_report(self):
        self.config.write_text(self.config.read_text().replace(
            f"127.0.0.1:{self.server.server_port}", "127.0.0.1:1"))
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = run_core.run(self.config, artifact_parent=self.root / "reports")
        self.assertEqual(result, 1)
        self.assertIn("FINAL_STATUS=not_started", stdout.getvalue())
        self.assertIn("启动", stderr.getvalue())
        self.assertFalse((self.root / "reports").exists())
        self.assertFalse(any(request[0] == "POST" for request in self.server.requests))

    def test_bad_token_fails_before_launch(self):
        self.server.ready_behavior = "unauthorized"
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = run_core.run(self.config, artifact_parent=self.root / "reports")
        self.assertEqual(result, 1)
        self.assertIn("认证失败", stderr.getvalue())
        self.assertEqual([entry[1] for entry in self.server.requests], ["/ready"])

    def test_token_file_with_newline_is_rejected_before_http(self):
        (self.root / "canonical-store" / ".daemon-token").write_text("local-test-token\n")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            result = run_core.run(self.config, artifact_parent=self.root / "reports")
        self.assertEqual(result, 1)
        self.assertEqual(self.server.requests, [])

    def test_readiness_error_cannot_echo_a_store_token(self):
        self.server.ready_behavior = "echo_token"
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = run_core.run(self.config, artifact_parent=self.root / "reports")
        self.assertEqual(result, 1)
        self.assertNotIn("local-test-token", stdout.getvalue() + stderr.getvalue())

    def test_redirect_does_not_forward_the_store_token(self):
        self.server.ready_behavior = "redirect"
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = run_core.run(self.config, artifact_parent=self.root / "reports")
        self.assertEqual(result, 1)
        self.assertIn("重定向", stderr.getvalue())
        self.assertEqual([entry[1] for entry in self.server.requests], ["/ready"])

    def test_lost_post_response_never_retries_or_invents_a_run_id(self):
        self.server.post_behavior = "ambiguous"
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = run_core.run(self.config, artifact_parent=self.root / "reports")
        self.assertEqual(result, 1)
        self.assertIn("FINAL_STATUS=unknown_after_post", stdout.getvalue())
        self.assertNotIn("RUN_ID=", stdout.getvalue())
        self.assertEqual(
            [entry[1] for entry in self.server.requests if entry[0] == "POST"], ["/runs"])
        self.assertIn("勿直接重试", stderr.getvalue())

    def test_malformed_post_response_is_ambiguous_not_a_safe_rejection(self):
        self.server.post_behavior = "invalid_json"
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = run_core.run(self.config, artifact_parent=self.root / "reports")
        self.assertEqual(result, 1)
        self.assertIn("FINAL_STATUS=unknown_after_post", stdout.getvalue())
        self.assertEqual(
            [entry[1] for entry in self.server.requests if entry[0] == "POST"], ["/runs"])

    def test_closed_paper_session_is_rejected_without_fake_run(self):
        self.server.post_behavior = "closed"
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = run_core.run(self.config, paper=True, artifact_parent=self.root / "reports")
        self.assertEqual(result, 1)
        self.assertIn("FINAL_STATUS=not_started", stdout.getvalue())
        self.assertNotIn("RUN_ID=", stdout.getvalue())
        self.assertIn("交易时段不可用", stderr.getvalue())
        self.assertEqual(
            [entry[1] for entry in self.server.requests if entry[0] == "POST"], ["/runs"])

    def test_pending_paper_run_is_not_cancelled_or_reported_as_a_fill(self):
        self.server.replay = {
            "run_id": "run-1", "purpose": "paper", "status": "running",
            "lifecycle": {"execution_status": "running", "outcome_scheduled": False,
                          "numeric_outcome_sealed": False},
        }
        self.server.detail["workflow"]["run"]["purpose"] = "paper"
        self.server.detail["artifacts"].append({
            "artifact_id": "receipt-1", "kind": "order_receipt",
            "payload": {"state": "accepted", "filled_quantity_micros": 0},
        })
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            result = run_core.run(self.config, paper=True, observation_seconds=0,
                                  artifact_parent=self.root / "reports")
        self.assertEqual(result, 3)
        self.assertIn("FINAL_STATUS=pending", stdout.getvalue())
        self.assertEqual(
            [entry[1] for entry in self.server.requests if entry[0] == "POST"],
            ["/runs", "/control/store/export-run-bundle"],
        )
        with zipfile.ZipFile(next((self.root / "reports").glob("*.zip"))) as zipped:
            summary = json.loads(zipped.read(next(
                name for name in zipped.namelist() if name.endswith("/summary.json"))))
        self.assertEqual(summary["paper_fills"][0]["state"], "accepted")
        self.assertEqual(summary["paper_fills"][0]["filled_quantity_micros"], 0)
        self.assertEqual(summary["outcome_schedule"]["status"], "not_observed_in_bounded_projection")

    def test_paper_decision_stage_alone_is_not_a_completed_paper_run(self):
        self.server.replay = {
            "run_id": "run-1", "purpose": "paper", "status": "decision_completed",
            "lifecycle": {"execution_status": "decision_completed", "outcome_scheduled": False,
                          "numeric_outcome_sealed": False},
        }
        self.server.detail["workflow"]["run"]["purpose"] = "paper"
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            result = run_core.run(self.config, paper=True, observation_seconds=0,
                                  artifact_parent=self.root / "reports")
        self.assertEqual(result, 3)
        self.assertIn("FINAL_STATUS=pending", stdout.getvalue())

    def test_position_plan_cannot_claim_an_execution_rejection_as_success(self):
        self.server.replay["status"] = "completed_with_execution_rejection"
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(io.StringIO()):
            result = run_core.run(self.config, artifact_parent=self.root / "reports")
        self.assertEqual(result, 1)
        self.assertIn("FINAL_STATUS=failed_to_observe", stdout.getvalue())

    def test_ready_policy_is_not_activated_or_bypassed_by_the_client(self):
        self.server.decision_capable = True
        self.server.post_behavior = "closed"  # Core still requires an approval/session.
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = run_core.run(self.config, paper=True, artifact_parent=self.root / "reports")
        self.assertEqual(result, 1)
        self.assertIn("FINAL_STATUS=not_started", stdout.getvalue())
        self.assertEqual(
            [entry[1] for entry in self.server.requests if entry[0] == "POST"], ["/runs"])
        self.assertEqual(self.server.requests[1][2], {"purpose": "paper"})

    def test_debug_configuration_cannot_be_used_for_a_formal_run(self):
        self.config.write_text(self.config.read_text().replace(
            "debug_control = false", "debug_control = true"))
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            result = run_core.run(self.config, artifact_parent=self.root / "reports")
        self.assertEqual(result, 1)
        self.assertEqual(self.server.requests, [])

    def test_old_or_isolated_core_is_rejected_before_a_paid_run(self):
        for scope, bundle_supported in (("canonical", False), ("isolated_debug", True)):
            self.server.store_scope = scope
            self.server.bundle_supported = bundle_supported
            self.server.requests.clear()
            stdout, stderr = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                result = run_core.run(self.config, artifact_parent=self.root / "reports")
            self.assertEqual(result, 1)
            self.assertIn("FINAL_STATUS=not_started", stdout.getvalue())
            self.assertEqual([entry[1] for entry in self.server.requests], ["/ready"])

    def test_unexpected_export_omissions_do_not_mask_a_completed_run(self):
        self.server.export_missing = True
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            result = run_core.run(self.config, artifact_parent=self.root / "reports")
        self.assertEqual(result, 2)
        self.assertIn("FINAL_STATUS=completed", stdout.getvalue())
        self.assertIn("EXPORT_STATUS=partial", stdout.getvalue())

    def test_completed_plan_without_decision_projection_reports_incomplete_observation(self):
        self.server.detail["artifacts"] = []
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            result = run_core.run(self.config, artifact_parent=self.root / "reports")
        self.assertEqual(result, 2)
        self.assertIn("FINAL_STATUS=completed", stdout.getvalue())
        self.assertIn("DECISION_TARGETS=null", stdout.getvalue())

    def test_corrupted_bundle_is_not_presented_as_a_verified_zip(self):
        self.server.corrupt_bundle = True
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = run_core.run(self.config, artifact_parent=self.root / "reports")
        self.assertEqual(result, 2)
        self.assertIn("FINAL_STATUS=completed", stdout.getvalue())
        self.assertNotIn("ZIP=", stdout.getvalue())
        self.assertIn("ARCHIVE_ERROR", stderr.getvalue())

    def test_symbolic_bundle_does_not_archive_unrelated_files(self):
        self.server.export_symlink = True
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = run_core.run(self.config, artifact_parent=self.root / "reports")
        self.assertEqual(result, 2)
        self.assertNotIn("ZIP=", stdout.getvalue())
        self.assertIn("符号链接", stderr.getvalue())

    def test_research_only_flag_is_not_a_hidden_formal_mode(self):
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                run_core.parse_args(["--research-only"])
        self.assertEqual(error.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
