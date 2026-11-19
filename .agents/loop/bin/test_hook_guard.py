#!/usr/bin/env python3
"""Unit tests for `.agents/loop/bin/hook_guard.py`."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import hook_guard  # noqa: E402


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

    def test_loop_subagent_isolation_and_markdown_block(self) -> None:
        wt_md = {
            "hookEventName": "PreToolUse",
            "cwd": str(hook_guard.REPO_ROOT),
            "lastUserInput": f"{hook_guard.SUBAGENT_TOKEN}\nImplement issue",
            "toolCall": {
                "name": "write_file",
                "args": {
                    "Path": str(hook_guard.REPO_ROOT / "outputs/loop/worktrees/issue-1/docs/NEW_DOC.md"),
                },
            },
        }
        self.assertEqual(hook_guard.handle_pre_tool_use(wt_md)["decision"], "deny")

        wt_rs = {
            "hookEventName": "PreToolUse",
            "cwd": str(hook_guard.REPO_ROOT),
            "lastUserInput": f"{hook_guard.SUBAGENT_TOKEN}\nImplement issue",
            "toolCall": {
                "name": "edit_file",
                "args": {
                    "Path": str(hook_guard.REPO_ROOT / "outputs/loop/worktrees/issue-1/crates/akzio-domain/src/core.rs"),
                },
            },
        }
        self.assertEqual(hook_guard.handle_pre_tool_use(wt_rs)["decision"], "allow")

        main_tree_edit = {
            "hookEventName": "PreToolUse",
            "cwd": str(hook_guard.REPO_ROOT),
            "lastUserInput": f"{hook_guard.SUBAGENT_TOKEN}\nImplement issue",
            "toolCall": {
                "name": "edit_file",
                "args": {
                    "Path": str(hook_guard.REPO_ROOT / "crates/akzio-domain/src/core.rs"),
                },
            },
        }
        self.assertEqual(hook_guard.handle_pre_tool_use(main_tree_edit)["decision"], "deny")


if __name__ == "__main__":
    unittest.main()
