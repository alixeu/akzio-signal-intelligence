#!/usr/bin/env python3
"""Regression tests for the local loop CLI and Antigravity hook."""

from __future__ import annotations

import io
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest
from contextlib import ExitStack, redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import hook_guard  # noqa: E402
import issue_hub  # noqa: E402


class HookGuardTests(unittest.TestCase):
    def test_interactive_denies_new_root_and_crate_markdown(self) -> None:
        for rel in ("UNSOLICITED_SUMMARY.md", "crates/akzio-domain/NOTES.md"):
            payload = {
                "hookEventName": "PreToolUse",
                "cwd": str(hook_guard.REPO_ROOT),
                "lastUserInput": "fix the bug",
                "toolCall": {
                    "name": "write_file",
                    "args": {"Path": str(hook_guard.REPO_ROOT / rel), "Content": "# hi"},
                },
            }
            res = hook_guard.handle_pre_tool_use(payload)
            self.assertEqual(res["decision"], "deny", f"expected deny for {rel}: {res}")

    def test_interactive_force_asks_new_docs_markdown_and_allows_existing_edits(self) -> None:
        new_doc = {
            "hookEventName": "PreToolUse",
            "cwd": str(hook_guard.REPO_ROOT),
            "lastUserInput": "document this",
            "toolCall": {
                "name": "write_file",
                "args": {"Path": str(hook_guard.REPO_ROOT / "docs/nonexistent-guide.md"), "Content": "# hi"},
            },
        }
        self.assertEqual(hook_guard.handle_pre_tool_use(new_doc)["decision"], "force_ask")

        existing_doc = {
            "hookEventName": "PreToolUse",
            "cwd": str(hook_guard.REPO_ROOT),
            "lastUserInput": "update AGENTS.md",
            "toolCall": {
                "name": "edit_file",
                "args": {"Path": str(hook_guard.REPO_ROOT / "AGENTS.md")},
            },
        }
        self.assertEqual(hook_guard.handle_pre_tool_use(existing_doc)["decision"], "ask")

    def test_interactive_does_not_block_artifact_directory_markdown(self) -> None:
        artifact_dir = "/tmp/agy-artifacts/conv-123"
        payload = {
            "hookEventName": "PreToolUse",
            "cwd": str(hook_guard.REPO_ROOT),
            "artifactDirectoryPath": artifact_dir,
            "lastUserInput": "review",
            "toolCall": {
                "name": "write_file",
                "args": {
                    "Path": f"{artifact_dir}/plan.md",
                    "ArtifactMetadata": {"Summary": "plan", "UserFacing": True, "RequestFeedback": False},
                },
            },
        }
        self.assertEqual(hook_guard.handle_pre_tool_use(payload)["decision"], "ask")

    def test_interactive_blocks_shell_redirection_to_new_root_markdown(self) -> None:
        payload = {
            "hookEventName": "PreToolUse",
            "cwd": str(hook_guard.REPO_ROOT),
            "lastUserInput": "summarize",
            "toolCall": {
                "name": "run_command",
                "args": {"CommandLine": "echo '# summary' > UNSOLICITED_SUMMARY.md"},
            },
        }
        self.assertEqual(hook_guard.handle_pre_tool_use(payload)["decision"], "deny")

        benign_read = {
            "hookEventName": "PreToolUse",
            "cwd": str(hook_guard.REPO_ROOT),
            "lastUserInput": "check readme",
            "toolCall": {
                "name": "run_command",
                "args": {"CommandLine": "cat README.md | head -n 5"},
            },
        }
        self.assertEqual(hook_guard.handle_pre_tool_use(benign_read)["decision"], "ask")


class LocalLoopTests(unittest.TestCase):
    SOURCE = "crates/akzio-domain/src/lib.rs"
    INTEGRATION_REF = "refs/heads/loop/nightly"
    INITIAL_SOURCE = "pub fn answer() -> u8 { 41 }\n"
    FIXED_SOURCE = "pub fn answer() -> u8 { 42 }\n"

    def setUp(self) -> None:
        stack = ExitStack()
        self.addCleanup(stack.close)
        directory = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="akzio-loop-test-"))).resolve()
        self.repo = directory / "repo"
        self.repo.mkdir()
        self.providers = directory / "providers"
        self.providers.mkdir()
        self.gh_called = self.providers / "gh-called"
        self.output = self.repo / "outputs/loop"
        self.notes = self.output / "tmp"
        self.worktrees = self.output / "worktrees"
        state = self.output / "state"
        stack.enter_context(patch.multiple(
            issue_hub, REPO_ROOT=self.repo, OUTPUT_ROOT=self.output, STATE_DIR=state,
            WORKTREES_DIR=self.worktrees, TMP_DIR=self.notes, REPORTS_DIR=self.output / "reports",
            DB_PATH=state / "loop.sqlite3",
        ))
        # Git must use the disposable repository even when invoked from a Git hook.
        env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
        env.update(
            PATH=os.pathsep.join((str(self.providers), env.get("PATH", os.defpath))),
            GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1",
        )
        stack.enter_context(patch.dict(os.environ, env, clear=True))
        self.install_provider("cargo", "#!/bin/sh\nexit 0\n")
        self.install_provider("gh", f"#!/bin/sh\ntouch {shlex.quote(str(self.gh_called))}\nexit 1\n")

        self.git("init", "--quiet", "--initial-branch=main", "--template=")
        self.git("config", "user.name", "Akzio Loop Tests")
        self.git("config", "user.email", "loop-tests@example.invalid")
        (self.repo / ".gitignore").write_text("outputs/\n", encoding="utf-8")
        source = self.repo / self.SOURCE
        source.parent.mkdir(parents=True)
        source.write_text(self.INITIAL_SOURCE, encoding="utf-8")
        scripts = self.repo / "scripts"
        scripts.mkdir()
        (scripts / "run_core.sh").write_text("#!/bin/bash\n", encoding="utf-8")
        self.git("add", "--all")
        self.git("commit", "-qm", "Fixture baseline")
        self.base = self.git("rev-parse", "HEAD")

    def install_provider(self, name, script):
        executable = self.providers / name
        executable.write_text(script, encoding="utf-8")
        executable.chmod(0o700)

    def git(self, *args, cwd=None):
        result = subprocess.run(
            ["git", *args], cwd=cwd or self.repo, capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(result.returncode, 0, f"git {args!r}: {result.stderr}")
        return result.stdout.strip()

    def run_cli(self, *args, code=0):
        stdout, stderr = io.StringIO(), io.StringIO()
        argv = ["issue_hub.py", *map(str, args)]
        with patch.object(sys, "argv", argv), redirect_stdout(stdout), redirect_stderr(stderr):
            actual = issue_hub.main()
        output = stdout.getvalue() or stderr.getvalue()
        self.assertEqual(actual, code, f"{args!r}: {output}")
        return output

    def hub(self, *args, code=0):
        return json.loads(self.run_cli(*args, code=code))

    def task_cmd(self, command, task, wave, *args, code=0):
        return self.hub(command, "--task", task, "--wave", wave, *args, code=code)

    def note(self, name, content):
        self.notes.mkdir(parents=True, exist_ok=True)
        path = self.notes / name
        path.write_text(content, encoding="utf-8")
        return path.relative_to(self.repo).as_posix()

    def propose(self, fingerprint="fixture-answer", *flags):
        return self.hub(
            "propose", "--title", "Correct the fixture answer", "--area", "domain",
            "--fingerprint", fingerprint, "--candidate-files", self.SOURCE,
            "--body-file", self.note("proposal.md", "The answer must be 42.\n"), *flags,
        )

    def design(self, task, wave):
        return self.task_cmd(
            "post-design", task, wave, "--allowed-files", self.SOURCE,
            "--design-file", self.note("design.md", "Fix the answer in the existing Rust file.\n"),
        )

    def claim(self):
        task = self.propose()["task"]
        wave = self.hub("start-wave")["wave"]
        self.assertEqual(self.design(task, wave)["status"], "ready")
        claimed = self.task_cmd("claim", task, wave)
        self.assertEqual(claimed["status"], "in_progress")
        self.assertEqual(claimed["attempt"], 1)
        return task, wave, Path(claimed["worktree_path"])

    def commit(self, task, wave, worktree, *, attempt=1):
        (worktree / self.SOURCE).write_text(self.FIXED_SOURCE, encoding="utf-8")
        result = self.task_cmd(
            "worker-commit", task, wave, "--attempt", attempt, "--message", "Correct the fixture answer",
        )
        self.assertEqual(result["changed_files"], [self.SOURCE])
        self.assertEqual(result["candidate_head"], self.git("rev-parse", "HEAD", cwd=worktree))
        return result["candidate_head"]

    def verify(self, task, wave, *, attempt=1, code=0):
        return self.task_cmd("verify-worktree", task, wave, "--attempt", attempt, code=code)

    def review(self, task, wave, head, *, verdict="PASS", code=0):
        return self.task_cmd(
            "review", task, wave, "--head", head, "--verdict", verdict,
            "--summary", "Fixture review evidence", code=code,
        )

    def reviewed_candidate(self):
        task, wave, worktree = self.claim()
        head = self.commit(task, wave, worktree)
        self.assertEqual(self.verify(task, wave)["head_sha"], head)
        self.assertEqual(self.review(task, wave, head)["status"], "reviewed")
        return task, wave, worktree, head

    def hook_decision(self, header, tool, **args):
        with patch.multiple(
            hook_guard, REPO_ROOT=self.repo, TMP_ROOT=self.notes, WORKTREES_ROOT=self.worktrees,
            LESSONS_PATH=self.repo / ".agents/loop/agent-lessons.md",
        ):
            return hook_guard.handle_pre_tool_use({
                "hookEventName": "PreToolUse",
                "cwd": str(self.repo),
                "lastUserInput": f"{header}\nFixture task",
                "toolCall": {"name": tool, "args": args},
            })["decision"]

    def test_hook_worker_writes_only_current_task_allowed_files(self):
        task, wave, worktree = self.claim()
        header = f"[AKZIO-LOOP-WORKER] {wave} {task} 1"
        source = worktree / self.SOURCE
        self.assertEqual(self.hook_decision(header, "edit_file", Path=str(source)), "allow")
        for path in (
            self.repo / self.SOURCE,
            worktree / "UNSOLICITED_SUMMARY.md",
            self.worktrees / f"task-{task + 1}" / self.SOURCE,
        ):
            with self.subTest(path=path):
                self.assertEqual(self.hook_decision(header, "write_file", Path=str(path)), "deny")
        stale = f"[AKZIO-LOOP-WORKER] {wave} {task} 2"
        self.assertEqual(self.hook_decision(stale, "edit_file", Path=str(source)), "deny")

    def test_hook_verifier_can_review_only_current_verified_sha(self):
        task, wave, worktree = self.claim()
        head = self.commit(task, wave, worktree)
        self.verify(task, wave)
        header = f"[AKZIO-LOOP-VERIFIER] {wave} {task} {head}"
        cli = ("python3", ".agents/loop/bin/issue_hub.py")
        scope = ("--task", str(task), "--wave", wave)
        diff = shlex.join((*cli, "show-diff", *scope))
        review = (*cli, "review", *scope, "--verdict", "PASS", "--summary", "Fixture review")
        correct_review = shlex.join((*review, "--head", head))
        wrong_review = shlex.join((*review, "--head", self.base))
        stale = f"[AKZIO-LOOP-VERIFIER] {wave} {task} {self.base}"
        self.assertEqual(self.hook_decision(header, "run_command", CommandLine=diff), "allow")
        self.assertEqual(self.hook_decision(header, "run_command", CommandLine=correct_review), "allow")
        self.assertEqual(self.hook_decision(header, "run_command", CommandLine=wrong_review), "deny")
        self.assertEqual(self.hook_decision(stale, "run_command", CommandLine=diff), "deny")
        self.assertEqual(self.hook_decision(header, "edit_file", Path=str(worktree / self.SOURCE)), "deny")

    def test_propose_deduplicates_offline_and_reports_dirty_main(self):
        self.assertEqual(self.hub("init")["recommended_action"], "scout_needed")
        self.assertEqual(self.propose("fixture-answer", "--dry-run")["status"], "dry_run_valid")
        self.assertEqual(self.hub("poll")["counts"], {})
        created = self.propose()
        self.assertEqual(created["status"], "created")
        task = created["task"]
        self.assertEqual(self.propose(), {"status": "skipped_duplicate", "task": task})
        self.assertFalse(self.gh_called.exists())

        (self.repo / self.SOURCE).write_text(self.FIXED_SOURCE, encoding="utf-8")
        (self.repo / "local-notes.txt").touch()
        poll = self.hub("poll")
        self.assertEqual(poll["recommended_action"], "design_inbox")
        self.assertEqual(poll["target_task"]["id"], task)
        self.assertEqual(poll["counts"], {"inbox": 1})
        self.assertEqual(poll["dirty_main_files"], {"modified": [self.SOURCE], "untracked": ["local-notes.txt"]})
        self.assertEqual(self.hub("daily-report")["counts"], {"task_proposed": 1})

    def verify_include_format(self, baseline, *, code=0, tab_spaces=None, candidate=None):
        fragment = "crates/akzio-domain/src/answer.rs"
        include = 'include!("answer.rs");\n'
        if tab_spaces is not None:
            (self.repo / fragment).with_name("rustfmt.toml").write_text(
                f"tab_spaces = {tab_spaces}\n", encoding="utf-8",
            )
        (self.providers / "cargo").unlink()
        (self.repo / "Cargo.toml").write_text(
            f'[package]\nname = "include-format-fixture"\nversion = "0.1.0"\nedition = "2021"\n'
            f'[lib]\npath = "{self.SOURCE}"\n', encoding="utf-8",
        )
        (self.repo / "Cargo.lock").write_text(
            'version = 4\n\n[[package]]\nname = "include-format-fixture"\nversion = "0.1.0"\n',
            encoding="utf-8",
        )
        if baseline is not None:
            (self.repo / self.SOURCE).write_text(include, encoding="utf-8")
            (self.repo / fragment).write_text(baseline, encoding="utf-8")
        self.git("add", "--all")
        self.git("commit", "-qm", "Prepare fixture for the include formatting check")
        task = self.propose()["task"]
        wave = self.hub("start-wave")["wave"]
        self.task_cmd(
            "post-design", task, wave, "--allowed-files", f"{self.SOURCE},{fragment}",
            "--design-file", self.note("include-design.md", "Check formatting of the included Rust source.\n"),
        )
        worktree = Path(self.task_cmd("claim", task, wave)["worktree_path"])
        (worktree / self.SOURCE).write_text(include, encoding="utf-8")
        (worktree / fragment).write_text(self.FIXED_SOURCE if candidate is None else candidate, encoding="utf-8")
        head = self.task_cmd(
            "worker-commit", task, wave, "--attempt", 1, "--message", "Change included answer",
        )["candidate_head"]
        result = self.verify(task, wave, code=code)
        self.assertEqual(result["head_sha"], head)
        self.assertEqual(self.git("rev-parse", "HEAD", cwd=worktree), head)
        if code:
            self.assertEqual(result["failures"], [f"{fragment} failed standalone rustfmt."])
        return result

    def test_include_format_accepts_changed_preexisting_unformatted_fragment(self):
        result = self.verify_include_format(self.INITIAL_SOURCE)
        self.assertEqual(result["status"], "verified")
        for gate in ("bash_syntax", "git_diff_check", "cargo_fmt", "cargo_check", "cargo_clippy", "cargo_test"):
            self.assertEqual(result["checks"].get(gate), 0, gate)

    def test_include_format_rejects_a_regression_from_a_formatted_baseline(self):
        result = self.verify_include_format("pub fn answer() -> u8 {\n  41\n}\n", code=2, tab_spaces=2)
        self.assertEqual(result["verdict"], "FAIL")

    def test_include_format_rejects_a_new_unformatted_fragment(self):
        result = self.verify_include_format(None, code=2)
        self.assertEqual(result["verdict"], "FAIL")

    def test_include_format_rejects_formatter_error_on_preexisting_fragment(self):
        result = self.verify_include_format(self.INITIAL_SOURCE, code=2, candidate="pub fn answer(\n")
        self.assertEqual(result["verdict"], "FAIL")

    def test_claim_resumes_candidate_under_new_wave(self):
        task, wave, worktree = self.claim()
        self.assertEqual(self.hub("start-wave")["status"], "busy")
        second_task = self.propose("another-fixture")["task"]
        self.design(second_task, wave)
        refusal = self.task_cmd("claim", second_task, wave, code=1)
        self.assertIn(f"Resume task #{task}", refusal["error"])
        head = self.commit(task, wave, worktree)

        self.assertEqual(self.hub("finish-wave", "--wave", "0" * 32)["status"], "stale")
        self.assertEqual(self.hub("finish-wave", "--wave", wave)["status"], "finished")
        next_wave = self.hub("start-wave")["wave"]
        self.assertNotEqual(next_wave, wave)
        self.assertIn("wave", self.verify(task, wave, code=1)["error"])
        self.assertIn("not owned", self.verify(task, next_wave, code=1)["error"])
        poll = self.hub("poll")
        self.assertEqual(poll["recommended_action"], "resume_task")
        self.assertEqual(poll["target_task"]["id"], task)

        resumed = self.task_cmd("claim", task, next_wave)
        self.assertEqual(resumed["attempt"], 1)
        self.assertEqual(resumed["worktree_path"], str(worktree))
        self.assertEqual(resumed["candidate_head"], head)
        self.assertEqual(self.verify(task, next_wave)["head_sha"], head)

    def test_claim_recovers_worker_commit_interrupted_before_registration(self):
        task, wave, worktree = self.claim()
        (worktree / self.SOURCE).write_text(self.FIXED_SOURCE, encoding="utf-8")
        with patch.object(issue_hub, "event", side_effect=RuntimeError("Interrupted candidate registration")):
            interrupted = self.task_cmd(
                "worker-commit", task, wave, "--attempt", 1, "--message", "Correct the fixture answer", code=1,
            )
        self.assertIn("Interrupted candidate registration", interrupted["error"])
        head = self.git("rev-parse", "HEAD", cwd=worktree)
        self.assertNotEqual(head, self.base)
        self.assertEqual(self.git("status", "--porcelain", cwd=worktree), "")
        self.assertIsNone(self.hub("show", "--task", task)["candidate_head"])

        self.hub("finish-wave", "--wave", wave)
        next_wave = self.hub("start-wave")["wave"]
        self.assertIn("wave", self.task_cmd("claim", task, wave, code=1)["error"])
        self.assertIn("not owned", self.verify(task, next_wave, code=1)["error"])
        resumed = self.task_cmd("claim", task, next_wave)
        self.assertEqual((resumed["status"], resumed["attempt"], resumed["candidate_head"]), ("in_progress", 1, head))
        self.assertIsNone(resumed["verified_head"])
        self.assertEqual(self.git("rev-parse", "HEAD", cwd=worktree), head)
        self.assertEqual(self.task_cmd("claim", task, next_wave)["candidate_head"], head)
        self.assertEqual(self.hub("daily-report")["counts"]["candidate_recovered"], 1)
        self.assertIn("attempt", self.verify(task, next_wave, attempt=2, code=1)["error"])
        self.assertEqual(self.verify(task, next_wave)["head_sha"], head)
        self.assertEqual(self.git("rev-parse", self.INTEGRATION_REF), self.base)

    def test_claim_does_not_register_a_candidate_after_the_wave_expires(self):
        task, wave, worktree = self.claim()
        (worktree / self.SOURCE).write_text(self.FIXED_SOURCE, encoding="utf-8")
        self.git("commit", "-qam", "Correct the fixture answer", cwd=worktree)
        now = issue_hub.utc_timestamp()
        with patch.object(issue_hub, "utc_timestamp", side_effect=[now, now + issue_hub.WAVE_SECONDS]):
            refused = self.task_cmd("claim", task, wave, code=1)
        self.assertIn("expired", refused["error"])
        self.assertIsNone(self.hub("show", "--task", task)["candidate_head"])
        self.assertNotIn("candidate_recovered", self.hub("daily-report")["counts"])

    def test_claim_leaves_unadvanced_and_dirty_heads_unregistered(self):
        task, wave, worktree = self.claim()
        self.assertIsNone(self.task_cmd("claim", task, wave)["candidate_head"])
        source = worktree / self.SOURCE
        source.write_text(self.FIXED_SOURCE, encoding="utf-8")
        self.git("commit", "-qam", "Correct the fixture answer", cwd=worktree)

        source.write_text(self.INITIAL_SOURCE, encoding="utf-8")
        with self.subTest(state="modified file"):
            self.assertIsNone(self.task_cmd("claim", task, wave)["candidate_head"])
        self.git("add", self.SOURCE, cwd=worktree)
        source.write_text(self.FIXED_SOURCE, encoding="utf-8")
        with self.subTest(state="different staged content"):
            self.assertIsNone(self.task_cmd("claim", task, wave)["candidate_head"])
        self.git("restore", "--staged", self.SOURCE, cwd=worktree)
        (worktree / "untracked.txt").touch()
        with self.subTest(state="untracked file"):
            self.assertIsNone(self.task_cmd("claim", task, wave)["candidate_head"])
        self.assertNotIn("candidate_recovered", self.hub("daily-report")["counts"])

    def test_claim_leaves_commits_with_outside_files_unregistered(self):
        task, wave, worktree = self.claim()
        (worktree / self.SOURCE).write_text(self.FIXED_SOURCE, encoding="utf-8")
        (worktree / "unexpected.txt").touch()
        self.git("add", self.SOURCE, "unexpected.txt", cwd=worktree)
        self.git("commit", "-qm", "Out-of-design fixture change", cwd=worktree)
        self.assertIsNone(self.task_cmd("claim", task, wave)["candidate_head"])
        self.assertNotIn("candidate_recovered", self.hub("daily-report")["counts"])

    def test_claim_checks_outside_paths_ignored_by_diff_config(self):
        task, wave, worktree = self.claim()
        self.git("config", "diff.ignoreSubmodules", "all")
        (worktree / self.SOURCE).write_text(self.FIXED_SOURCE, encoding="utf-8")
        self.git("add", self.SOURCE, cwd=worktree)
        self.git("update-index", "--add", "--cacheinfo", f"160000,{self.base},unexpected-link", cwd=worktree)
        self.git("commit", "-qm", "Out-of-design fixture gitlink", cwd=worktree)
        self.assertIsNone(self.task_cmd("claim", task, wave)["candidate_head"])

    def test_claim_refuses_recovery_after_diverging_from_attempt_base(self):
        task, wave, worktree = self.claim()
        head = self.commit(task, wave, worktree)
        self.verify(task, wave)
        self.assertEqual(self.review(task, wave, head, verdict="REJECT")["status"], "in_progress")
        self.git("reset", "--hard", self.base, cwd=worktree)
        (worktree / self.SOURCE).write_text("pub fn answer() -> u8 { 43 }\n", encoding="utf-8")
        self.git("commit", "-qam", "Fixture sibling of the attempt base", cwd=worktree)
        resumed = self.task_cmd("claim", task, wave)
        self.assertEqual(resumed["attempt_base"], head)
        self.assertIsNone(resumed["candidate_head"])

    def test_claim_requires_the_managed_branch_and_worktree_for_recovery(self):
        task, wave, worktree = self.claim()
        (worktree / self.SOURCE).write_text(self.FIXED_SOURCE, encoding="utf-8")
        self.git("commit", "-qam", "Correct the fixture answer", cwd=worktree)
        task_branch = self.git("symbolic-ref", "--short", "HEAD", cwd=worktree)
        self.git("switch", "-c", "other-fixture-branch", cwd=worktree)
        self.assertIn("Unexpected branch", self.task_cmd("claim", task, wave, code=1)["error"])
        self.git("switch", task_branch, cwd=worktree)
        moved = self.repo.parent / "moved-worktree"
        worktree.rename(moved)
        self.git("clone", "--quiet", "--branch", task_branch, str(self.repo), str(worktree))
        self.assertIn("registered worktree", self.task_cmd("claim", task, wave, code=1)["error"])
        self.assertIsNone(self.hub("show", "--task", task)["candidate_head"])

    def test_worker_commit_refuses_changes_outside_frozen_design(self):
        task, wave, worktree = self.claim()
        outside_design = worktree / "unexpected.txt"
        outside_design.write_text("Outside the design.\n", encoding="utf-8")
        refused = self.task_cmd(
            "worker-commit", task, wave, "--attempt", 1, "--message", "Unexpected change", code=1,
        )
        self.assertIn("unexpected.txt", refused["error"])
        self.assertEqual(self.git("rev-parse", "HEAD", cwd=worktree), self.base)
        self.assertEqual(self.git("diff", "--cached", "--name-only", cwd=worktree), "")
        self.assertIsNone(self.hub("show", "--task", task)["candidate_head"])
        outside_design.unlink()
        test_run = self.task_cmd(
            "worker-test", task, wave, "--attempt", 1, "--crate", "akzio-domain", "--filter", "answer",
        )
        self.assertEqual(test_run["exit_code"], 0)
        self.commit(task, wave, worktree)

    def test_landing_requires_independent_review_of_the_verified_sha(self):
        task, wave, worktree = self.claim()
        head = self.commit(task, wave, worktree)
        review_refused = self.review(task, wave, head, code=1)
        self.assertIn("required stage", review_refused["error"])
        verified = self.verify(task, wave)
        self.assertEqual((verified["status"], verified["head_sha"]), ("verified", head))
        for gate in ("bash_syntax", "git_diff_check", "cargo_fmt", "cargo_check", "cargo_clippy", "cargo_test"):
            self.assertEqual(verified["checks"].get(gate), 0, gate)
        source = worktree / self.SOURCE
        source.write_text("pub fn answer() -> u8 { 99 }\n", encoding="utf-8")
        diff = self.run_cli("show-diff", "--task", task, "--wave", wave)
        self.assertIn(f"+{self.FIXED_SOURCE}", diff)
        self.assertNotIn("{ 99 }", diff)
        source.write_text(self.FIXED_SOURCE, encoding="utf-8")
        self.assertIn("required stage", self.task_cmd("worktree-land", task, wave, code=1)["error"])
        self.assertEqual(self.git("rev-parse", self.INTEGRATION_REF), self.base)
        self.assertIn("SHA differs", self.review(task, wave, self.base, code=1)["error"])
        self.assertEqual(self.review(task, wave, head)["status"], "reviewed")
        self.assertIn("required stage", self.review(task, wave, head, code=1)["error"])

        landed = self.task_cmd("worktree-land", task, wave)
        self.assertEqual((landed["status"], landed["verified_head"]), ("done", head))
        self.assertEqual(self.git("rev-parse", self.INTEGRATION_REF), head)
        self.assertEqual(self.git("rev-parse", "HEAD"), self.base)
        self.assertEqual((self.repo / self.SOURCE).read_text(encoding="utf-8"), self.INITIAL_SOURCE)
        self.assertEqual(self.task_cmd("worktree-land", task, wave)["status"], "done")
        self.assertEqual(self.hub("daily-report")["counts"]["task_landed"], 1)

    def test_landing_preserves_a_divergent_integration_ref(self):
        task, wave, worktree, _ = self.reviewed_candidate()
        tree = self.git("rev-parse", f"{self.base}^{{tree}}")
        advanced = self.git("commit-tree", tree, "-p", self.base, "-m", "Competing integration advance")
        self.git("update-ref", self.INTEGRATION_REF, advanced, self.base)

        blocked = self.task_cmd("worktree-land", task, wave)
        self.assertEqual((blocked["status"], blocked["blocked_reason"]), ("blocked", "integration_advanced"))
        self.assertEqual(self.git("rev-parse", self.INTEGRATION_REF), advanced)
        self.assertTrue(worktree.is_dir())

    def test_landing_resumes_after_ref_moves_before_completion(self):
        task, wave, _, head = self.reviewed_candidate()
        checkout = self.repo.parent / "integration-checkout"
        self.git("worktree", "add", str(checkout), "loop/nightly")
        refused = self.task_cmd("worktree-land", task, wave, code=1)
        self.assertIn("checked out", refused["error"])
        self.assertEqual(self.hub("show", "--task", task)["status"], "landing")
        self.git("worktree", "remove", str(checkout))
        # Reproduce the ref update that survives an interruption before SQLite records completion.
        self.git("update-ref", self.INTEGRATION_REF, head, self.base)
        self.hub("finish-wave", "--wave", wave)
        next_wave = self.hub("start-wave")["wave"]
        self.assertEqual(self.task_cmd("claim", task, next_wave)["status"], "landing")
        self.assertEqual(self.task_cmd("worktree-land", task, next_wave)["status"], "done")
        self.assertEqual(self.git("rev-parse", self.INTEGRATION_REF), head)
        counts = self.hub("daily-report")["counts"]
        self.assertEqual((counts["landing_intent"], counts["task_landed"]), (1, 1))

    def origin(self):
        remote = self.repo.parent / "origin.git"
        self.git("init", "--bare", "--quiet", "--initial-branch=main", "--template=", str(remote))
        self.git("remote", "add", "origin", str(remote))
        self.git("push", "--quiet", "origin", "main")
        return remote

    def test_delivery_publishes_reviewed_head_and_retires_managed_refs(self):
        remote = self.origin()
        task, wave, worktree, head = self.reviewed_candidate()
        branch = self.git("symbolic-ref", "HEAD", cwd=worktree)
        self.task_cmd("worktree-land", task, wave)
        self.hub("finish-wave", "--wave", wave)
        self.assertEqual(self.git("rev-parse", "HEAD"), self.base)
        self.assertEqual(self.git("rev-parse", "main", cwd=remote), self.base)

        delivered = self.hub("deliver-main")
        self.assertEqual((delivered["status"], delivered["head_sha"]), ("published", head))
        self.assertEqual(self.git("rev-parse", "HEAD"), head)
        self.assertEqual(self.git("rev-parse", "main", cwd=remote), head)
        self.assertEqual(self.git("for-each-ref", "--format=%(refname)", branch, self.INTEGRATION_REF), "")
        self.assertFalse(worktree.exists())
        self.assertEqual(self.hub("deliver-main")["status"], "up_to_date")
        self.assertEqual(self.hub("daily-report")["counts"]["task_published"], 1)

    def test_delivery_retries_a_failed_push_after_local_fast_forward(self):
        remote = self.origin()
        task, wave, _, head = self.reviewed_candidate()
        self.task_cmd("worktree-land", task, wave)
        self.hub("finish-wave", "--wave", wave)
        pre_receive = remote / "hooks" / "pre-receive"
        pre_receive.parent.mkdir()
        pre_receive.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
        pre_receive.chmod(0o700)
        self.assertIn("rejected", self.hub("deliver-main", code=1)["error"])
        self.assertEqual(self.git("rev-parse", "HEAD"), head)
        self.assertEqual(self.git("rev-parse", "main", cwd=remote), self.base)
        self.assertEqual(self.git("rev-parse", self.INTEGRATION_REF), head)
        self.assertNotIn("task_published", self.hub("daily-report")["counts"])
        pre_receive.unlink()
        self.assertEqual(self.hub("deliver-main")["status"], "published")
        self.assertEqual(self.git("rev-parse", "main", cwd=remote), head)

    def test_delivery_preserves_dirty_main_and_refuses_unpublished_base_commits(self):
        remote = self.origin()
        task, wave, _, head = self.reviewed_candidate()
        self.task_cmd("worktree-land", task, wave)
        self.hub("finish-wave", "--wave", wave)
        local_file = self.repo / "operator.txt"
        local_file.write_text("operator's work\n", encoding="utf-8")
        self.assertIn("clean", self.hub("deliver-main", code=1)["error"])
        self.assertEqual(local_file.read_text(encoding="utf-8"), "operator's work\n")
        self.assertEqual(self.git("rev-parse", "main", cwd=remote), self.base)
        local_file.unlink()
        self.assertEqual(self.hub("deliver-main")["head_sha"], head)

        self.git("commit", "--allow-empty", "-qm", "Unpublished operator commit")
        next_task = self.propose("second-fixture")["task"]
        next_wave = self.hub("start-wave")["wave"]
        self.design(next_task, next_wave)
        second_tree = Path(self.task_cmd("claim", next_task, next_wave)["worktree_path"])
        (second_tree / self.SOURCE).write_text(self.INITIAL_SOURCE, encoding="utf-8")
        next_head = self.task_cmd(
            "worker-commit", next_task, next_wave, "--attempt", 1, "--message", "Update fixture answer",
        )["candidate_head"]
        self.verify(next_task, next_wave)
        self.review(next_task, next_wave, next_head)
        self.task_cmd("worktree-land", next_task, next_wave)
        self.hub("finish-wave", "--wave", next_wave)
        self.assertIn("approved", self.hub("deliver-main", code=1)["error"])
        self.assertEqual(self.git("rev-parse", "main", cwd=remote), head)

    def test_fresh_database_does_not_adopt_existing_task_checkout(self):
        old_branch = "loop/task-1"
        old_tree = self.worktrees / "task-1"
        old_tree.parent.mkdir(parents=True)
        self.git("worktree", "add", "-b", old_branch, str(old_tree), self.base)
        (old_tree / self.SOURCE).write_text(self.FIXED_SOURCE, encoding="utf-8")
        self.git("commit", "-qam", "Unrelated prior task", cwd=old_tree)
        old_head = self.git("rev-parse", "HEAD", cwd=old_tree)
        task, _, worktree = self.claim()
        self.assertEqual(task, 1)
        self.assertNotEqual(worktree, old_tree)
        self.assertEqual(self.git("rev-parse", "HEAD", cwd=worktree), self.base)
        self.assertEqual(self.git("rev-parse", "HEAD", cwd=old_tree), old_head)

    def test_two_attempt_breaker_counts_missing_candidate_and_rejected_review(self):
        task, wave, worktree = self.claim()
        failed = self.verify(task, wave, code=2)
        self.assertEqual((failed["status"], failed["verdict"]), ("in_progress", "FAIL"))
        self.assertIn("candidate", " ".join(failed["failures"]).lower())
        self.assertIn("attempt", self.verify(task, wave, code=1)["error"].lower())
        subsequent = self.hub("show", "--task", task)
        self.assertEqual(subsequent["attempt"], 2)
        self.assertEqual(subsequent["last_failure"]["kind"], "gate_failed")
        self.assertEqual(subsequent["last_failure"]["summary"], failed["failures"])

        head = self.commit(task, wave, worktree, attempt=2)
        self.assertEqual(self.verify(task, wave, attempt=2)["head_sha"], head)
        self.assertEqual(self.review(task, wave, head, verdict="REJECT")["status"], "blocked")
        blocked = self.hub("show", "--task", task)
        self.assertEqual((blocked["attempt"], blocked["blocked_reason"]), (2, "circuit_breaker_2_rounds"))
        self.assertEqual(blocked["last_failure"]["head_sha"], head)
        self.assertIsNone(blocked["verified_head"])
        self.assertTrue(worktree.is_dir())
        self.assertEqual(self.hub("poll")["recommended_action"], "scout_needed")
        counts = self.hub("daily-report")["counts"]
        self.assertEqual((counts["gate_failed"], counts["review_rejected"], counts["task_blocked"]), (1, 1, 1))

    def install_audit_provider(self):
        mode_path = self.providers / "gh-mode"
        comments_path = self.providers / "gh-comments.json"
        provider = textwrap.dedent("""\
            import json
            import sys
            from pathlib import Path

            root = Path(sys.argv[1])
            mode = (root / "gh-mode").read_text(encoding="utf-8").strip()
            store = root / "gh-comments.json"
            comments = json.loads(store.read_text(encoding="utf-8")) if store.exists() else []
            endpoint = "repos/fixture/loop/issues/42/comments"
            match sys.argv[2:]:
                case ["api", "user"]:
                    print('{"id": 17}')
                case ["api", "--paginate", "--slurp", query] if query == endpoint + "?per_page=100":
                    print(json.dumps([comments]))
                case ["api", "--method", "POST", post, "--input", "-"] if post == endpoint:
                    if mode == "offline":
                        sys.exit("fixture unavailable before delivery")
                    comment = {"id": len(comments) + 1, "user": {"id": 17}, "body": json.load(sys.stdin)["body"]}
                    comments.append(comment)
                    store.write_text(json.dumps(comments), encoding="utf-8")
                    if mode == "lost_reply":
                        sys.exit("fixture lost response after delivery")
                    print(json.dumps(comment))
                case _:
                    sys.exit(f"Unexpected GitHub CLI arguments: {sys.argv[2:]!r}")
        """)
        launcher = shlex.join([sys.executable, "-c", provider, str(self.providers)])
        self.install_provider("gh", f"#!/bin/sh\nexec {launcher} \"$@\"\n")
        return mode_path, comments_path

    def test_audit_retains_events_and_recovers_ambiguous_delivery(self):
        mode_path, comments_path = self.install_audit_provider()
        audit = ("publish-log", "--repo", "fixture/loop", "--issue", 42)
        task = self.propose()["task"]
        mode_path.write_text("offline", encoding="utf-8")
        self.assertIn("before delivery", self.hub(*audit, code=1)["error"])
        self.assertFalse(comments_path.exists())
        self.assertEqual(self.hub("show", "--task", task)["status"], "inbox")
        self.assertEqual(self.hub("daily-report")["counts"], {"task_proposed": 1})

        mode_path.write_text("lost_reply", encoding="utf-8")
        self.assertIn("after delivery", self.hub(*audit, code=1)["error"])
        delivered = json.loads(comments_path.read_text(encoding="utf-8"))
        self.assertEqual(len(delivered), 1)
        self.assertIn("`task_proposed`", delivered[0]["body"])
        self.assertIn("`daily_report`", delivered[0]["body"])

        mode_path.write_text("online", encoding="utf-8")
        self.assertEqual(self.hub(*audit), {"status": "published", "events": 2, "new_events": 0})
        self.assertEqual(self.hub(*audit), {"status": "up_to_date", "events": 0})
        self.assertEqual(json.loads(comments_path.read_text(encoding="utf-8")), delivered)


    def test_wave_delivers_to_main_and_drains_the_configured_audit(self):
        source = Path(__file__).resolve().parent
        destination = self.repo / ".agents/loop/bin"
        destination.mkdir(parents=True)
        for name in ("issue_hub.py", "loop_daemon.sh"):
            shutil.copy2(source / name, destination / name)
        self.git("add", ".agents")
        self.git("commit", "-qm", "Install fixture launcher")
        remote = self.origin()
        self.git("config", "--local", "akzioLoop.auditRepo", "fixture/loop")
        self.git("config", "--local", "akzioLoop.auditIssue", "42")
        mode_path, comments_path = self.install_audit_provider()
        mode_path.write_text("online", encoding="utf-8")
        start = {
            "event": "init", "conversation_id": "fixture",
            "init": {"cwd": str(self.repo), "agent": "loop-leader"},
        }
        end = {"event": "result", "result": {"conversation_id": "fixture", "status": "SUCCESS"}}
        self.install_provider("agy", f"#!/bin/sh\ncat <<'EVENTS'\n{json.dumps(start)}\n{json.dumps(end)}\nEVENTS\n")
        task, wave, _, head = self.reviewed_candidate()
        self.task_cmd("worktree-land", task, wave)
        self.hub("finish-wave", "--wave", wave)
        for index in range(25):
            self.propose(f"backlog-{index}")

        launcher = destination / "loop_daemon.sh"
        for command in ("wave", "deliver"):
            result = subprocess.run(
                ["bash", str(launcher), command], cwd=self.repo, capture_output=True, text=True, timeout=30,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(self.git("rev-parse", "main", cwd=remote), head)
            comments = json.loads(comments_path.read_text(encoding="utf-8"))
            self.assertEqual(len(comments), 2)
            self.assertIn("`task_published`", comments[-1]["body"])
            self.assertIn(head, comments[-1]["body"])


if __name__ == "__main__":
    unittest.main()
