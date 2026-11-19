#!/usr/bin/env python3
"""
Antigravity Lifecycle Hook Guard (`PreToolUse` & `Stop`) for Akzio 24h Autonomous Loop.

- In `PreToolUse` (for `run_command`):
  1. Distinguishes Loop Leader (`[AKZIO-AUTONOMOUS-LOOP]`), Loop Subagents (`[AKZIO-LOOP-SUBAGENT]`),
     and normal interactive conversations (`in_loop == False`).
  2. When `in_loop` is False (normal interactive user sessions), returns `{"decision": "ask"}`
     without imposing loop-only denials (preserving user-approved `run_core.sh paper` or `~/.akzio` checks
     and Antigravity's normal Always-Allow / Turbo permissions).
  3. When `in_loop` is True:
     - Hard-denies (`"decision": "deny"`) touching `~/.akzio` paths (excluding commit/issue text),
       force-pushing, pushing directly to `main`/`master`, launching `scripts/run_core.sh paper`,
       or running destructive/mutating git commands outside `outputs/loop/worktrees/`.
     - Auto-allows (`"decision": "allow"`) verified safe loop commands (after parsing shell chains
       and validating every segment) so unattended overnight waves and subagents never hang on prompts.
     - Returns `{"decision": "ask"}` for anything outside the allowlist.

- In `Stop`:
  1. Only activates for the Loop Leader (`[AKZIO-AUTONOMOUS-LOOP]` in `lastUserInput` AND
     `[AKZIO-LOOP-SUBAGENT]` NOT in `lastUserInput`), so subagents are never hijacked to run `poll`.
  2. Checks `outputs/loop/state/queue_cache.json` (kept synchronized by `issue_hub.py`);
     if `terminationReason == "NO_TOOL_CALL"`, `not continuationActive`, `executionNum < 2`,
     and actionable items remain (`ready > 0` or `inbox > 0`), continues for at most 1 more step.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[3]
SHARED_CARGO_TARGET_DIR = str(REPO_ROOT / "target")
QUEUE_CACHE_PATH = REPO_ROOT / "outputs" / "loop" / "state" / "queue_cache.json"

LEADER_TOKEN = "[AKZIO-AUTONOMOUS-LOOP]"
SUBAGENT_TOKEN = "[AKZIO-LOOP-SUBAGENT]"

AKZIO_PATH_RE = re.compile(r"(?:~|\$HOME|\$\{HOME\}|/Users/[^/\s'\";&|]+)/\.akzio(?:/|\b)")
QUOTED_MSG_RE = re.compile(r"""(?:^|\s)(?:-m|--title|--message)\s+(?:"[^"]*"|'[^']*')""")


def strip_commit_or_title_strings(cmd_line: str) -> str:
    """Remove `-m "..."` / `--title "..."` string literals before scanning for `~/.akzio` path usage."""
    return QUOTED_MSG_RE.sub(" ", cmd_line)


def split_shell_segments(cmd_line: str) -> Optional[List[str]]:
    """
    Split a shell command line on `&&`, `||`, `;`, `|` outside single/double quotes.
    Returns None if unclosed quotes or unsupported command substitutions (`$(`, `` ` ``) exist.
    """
    segments: List[str] = []
    buf: List[str] = []
    in_single = False
    in_double = False
    escaped = False
    i = 0
    n = len(cmd_line)

    while i < n:
        ch = cmd_line[i]
        if escaped:
            buf.append(ch)
            escaped = False
            i += 1
            continue

        if ch == "\\" and not in_single:
            buf.append(ch)
            escaped = True
            i += 1
            continue

        if ch == "'" and not in_double:
            in_single = not in_single
            buf.append(ch)
            i += 1
            continue

        if ch == '"' and not in_single:
            in_double = not in_double
            buf.append(ch)
            i += 1
            continue

        if not in_single:
            if ch == "`" or (ch == "$" and i + 1 < n and cmd_line[i + 1] == "("):
                return None
            if not in_double and ch in ("<", ">") and i + 1 < n and cmd_line[i + 1] == "(":
                return None

        if not in_single and not in_double:
            two = cmd_line[i : i + 2]
            if two in ("&&", "||"):
                seg = "".join(buf).strip()
                if seg:
                    segments.append(seg)
                buf = []
                i += 2
                continue
            if ch in (";", "|", "\n"):
                seg = "".join(buf).strip()
                if seg:
                    segments.append(seg)
                buf = []
                i += 1
                continue

        buf.append(ch)
        i += 1

    if in_single or in_double or escaped:
        return None

    tail = "".join(buf).strip()
    if tail:
        segments.append(tail)
    return segments


def has_unsafe_redirection(seg: str) -> bool:
    """Reject output redirection except benign `2>&1`, `>/dev/null`, `2>/dev/null`, or `> outputs/loop/...`."""
    in_single = False
    in_double = False
    escaped = False
    i = 0
    n = len(seg)
    while i < n:
        ch = seg[i]
        if escaped:
            escaped = False
            i += 1
            continue
        if ch == "\\" and not in_single:
            escaped = True
            i += 1
            continue
        if ch == "'" and not in_double:
            in_single = not in_single
            i += 1
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            i += 1
            continue
        if not in_single and not in_double and ch == ">":
            rest = seg[max(0, i - 1) :].lstrip()
            if rest.startswith("2>&1") or rest.startswith(">&1"):
                i += 1
                continue
            after = seg[i + 1 :].lstrip()
            if after.startswith(">"):
                after = after[1:].lstrip()
            target = after.split()[0] if after.split() else ""
            if target == "/dev/null" or target.startswith("outputs/loop/tmp/") or target.startswith(
                f"{REPO_ROOT}/outputs/loop/tmp/"
            ):
                i += 1
                continue
            return True
        i += 1
    return False


def is_in_worktree(path_hint: str) -> bool:
    return "outputs/loop/worktrees/" in path_hint


def is_safe_segment(seg: str, active_cwd: str) -> Tuple[bool, str]:
    """
    Validate a single shell segment and return `(allowed, updated_cwd)`.
    """
    if has_unsafe_redirection(seg):
        return False, active_cwd

    # Strip trailing benign redirections like `2>&1`, `>/dev/null`, `2>/dev/null`
    cleaned = re.sub(r"\s+(?:[12]?>>?\s*/dev/null|2>&1)\s*$", "", seg).strip()

    # Handle `cd <dir>`
    m_cd = re.match(r"^cd\s+(.+)$", cleaned)
    if m_cd:
        target = m_cd.group(1).strip().strip("'\"")
        if target.startswith("/"):
            next_cwd = target
        else:
            next_cwd = str((Path(active_cwd or REPO_ROOT) / target).resolve())
        return True, next_cwd

    # issue_hub.py (always runs against repo root or explicit path)
    if re.match(r"^python3\s+(?:\./)?(?:\S*/)?\.agents/loop/bin/issue_hub\.py\b", cleaned):
        return True, active_cwd

    # Cargo / rustfmt gates (allow sharing main repo target dir to avoid per-worktree bloat)
    target_dir_re = re.escape(SHARED_CARGO_TARGET_DIR)
    if re.match(rf"^export\s+CARGO_TARGET_DIR=[\"']?{target_dir_re}[\"']?$", cleaned):
        return True, active_cwd
    if re.match(
        rf"^(?:CARGO_TARGET_DIR=[\"']?{target_dir_re}[\"']?\s+)?cargo\s+(?:fmt|check|clippy|test|metadata|version)\b",
        cleaned,
    ):
        return True, active_cwd
    if re.match(r"^rustfmt\s+(?:--edition\s+\S+\s+)?(?:--check\s+)?\S+", cleaned):
        return True, active_cwd

    # Shell syntax check in CI
    if re.match(r"^bash\s+-n\s+scripts/run_core\.sh\b", cleaned):
        return True, active_cwd

    # GitHub CLI issue/label transitions
    if re.match(r"^gh\s+(?:issue|label)\s+(?:list|view|comment|edit|create|close)\b", cleaned):
        return True, active_cwd

    # Read-only git inspection
    if re.match(r"^git\s+(?:status|diff|show|log|rev-parse|merge-base)\b", cleaned):
        return True, active_cwd
    if re.match(r"^git\s+branch\s*(?:--show-current|-a|-r|--list)?$", cleaned):
        return True, active_cwd

    # Write git commands strictly restricted to isolated issue worktrees
    if re.match(r"^git\s+(?:add|commit)\b", cleaned):
        if is_in_worktree(active_cwd) or re.search(r"-C\s+\S*outputs/loop/worktrees/", cleaned):
            return True, active_cwd
        return False, active_cwd

    # Read-only inspection tools (forbid `find ... -exec/-delete` and `sed -i`)
    if re.match(r"^(?:ls|cat|head|tail|wc|rg|grep|pwd|which|sort|uniq|echo|printf)\b", cleaned):
        return True, active_cwd
    if re.match(r"^find\b", cleaned) and not re.search(r"\s-(?:exec|execdir|delete|ok)\b", cleaned):
        return True, active_cwd
    if re.match(r"^sed\s+-n\b", cleaned):
        return True, active_cwd

    # Scoped loop temp directory ops
    if re.match(r"^mkdir\s+-p\s+(?:\S*/)?outputs/loop/(?:tmp|state|reports|logs)(?:/\S*)?$", cleaned):
        return True, active_cwd

    return False, active_cwd


def is_safe_loop_command(cmd_line: str, cwd: str) -> bool:
    segments = split_shell_segments(cmd_line)
    if not segments:
        return False
    active_cwd = cwd or str(REPO_ROOT)
    for seg in segments:
        ok, active_cwd = is_safe_segment(seg, active_cwd)
        if not ok:
            return False
    return True


def _resolve_target_path(raw_path: str, cwd: str) -> Optional[Path]:
    if not raw_path:
        return None
    expanded = Path(raw_path).expanduser()
    if expanded.is_absolute():
        return expanded.resolve(strict=False)
    base = Path(cwd).expanduser() if cwd else REPO_ROOT
    return (base / expanded).resolve(strict=False)


def _repo_relative_path(target: Path) -> Optional[str]:
    repo_resolved = REPO_ROOT.resolve(strict=False)
    try:
        return str(target.relative_to(repo_resolved)).replace("\\", "/")
    except ValueError:
        return None


def _strip_worktree_prefix(rel_path: str) -> str:
    m = re.match(r"^outputs/loop/worktrees/issue-\d+/(.+)$", rel_path)
    return m.group(1) if m else rel_path


def _is_loop_ephemeral_markdown(rel_path: str) -> bool:
    return rel_path.startswith(("outputs/loop/tmp/", "outputs/loop/reports/"))


def _is_crate_prompt_or_fixture_md(logical_rel: str) -> bool:
    return bool(
        re.match(r"^crates/[^/]+/src/(?:.+/)?(?:prompts|fixtures)/.+\.(?:md|markdown)$", logical_rel)
    )


def _check_markdown_policy(
    target: Path,
    rel_path: str,
    is_create_tool: bool,
    in_loop: bool,
) -> Optional[Dict[str, Any]]:
    """
    Enforce anti-doc-slop policy: agents must not arbitrarily write new `.md` files
    in the workspace (especially at repository root or under `crates/`).
    """
    if not rel_path.lower().endswith((".md", ".markdown")):
        return None
    if _is_loop_ephemeral_markdown(rel_path):
        return {"decision": "allow"} if in_loop else {"decision": "ask"}

    logical_rel = _strip_worktree_prefix(rel_path)
    is_new_file = is_create_tool and not target.exists()
    if is_new_file:
        if in_loop:
            return {
                "decision": "deny",
                "reason": (
                    f"Creating new Markdown file '{logical_rel}' is forbidden in autonomous loop mode "
                    f"(only outputs/loop/tmp/ and outputs/loop/reports/ are allowed)."
                ),
            }
        if "/" not in logical_rel or (
            logical_rel.startswith("crates/") and not _is_crate_prompt_or_fixture_md(logical_rel)
        ):
            return {
                "decision": "deny",
                "reason": (
                    f"Creating unsolicited Markdown file '{logical_rel}' in repository root or crates/ is "
                    f"forbidden (anti-doc-slop guardrail). Keep rules in code/lints/tests or edit existing docs."
                ),
            }
        return {
            "decision": "force_ask",
            "reason": (
                f"Creating a new Markdown document '{logical_rel}' requires explicit user confirmation "
                f"(anti-doc-slop guardrail)."
            ),
        }

    return None


def _find_shell_markdown_write(cmd_line: str, cwd: str) -> Optional[str]:
    """
    Detect obvious shell creation/redirection of `.md` files in repo root or `crates/`
    during interactive sessions (e.g. `> SUMMARY.md`, `touch crates/foo/README.md`, `tee NOTES.md`).
    """
    segments = split_shell_segments(cmd_line)
    if not segments:
        return None
    active_cwd = cwd or str(REPO_ROOT)
    for seg in segments:
        m_cd = re.match(r"^cd\s+(.+)$", seg.strip())
        if m_cd:
            target_dir = m_cd.group(1).strip().strip("'\"")
            p = Path(target_dir)
            active_cwd = str(p if p.is_absolute() else (Path(active_cwd) / p).resolve(strict=False))
            continue

        candidates: List[str] = []
        for m in re.finditer(r"(?:^|\s)\d?>>?\s*([^\s;&|'\"]+|'[^']*'|\"[^\"]*\")", seg):
            candidates.append(m.group(1).strip("'\""))
        m_touch = re.match(r"^(?:touch|tee(?:\s+-a)?)\s+(.+)$", seg.strip())
        if m_touch:
            for tok in m_touch.group(1).split():
                if not tok.startswith("-"):
                    candidates.append(tok.strip("'\""))

        for cand in candidates:
            if not cand.lower().endswith((".md", ".markdown")):
                continue
            resolved = _resolve_target_path(cand, active_cwd)
            if not resolved:
                continue
            rel = _repo_relative_path(resolved)
            if not rel or _is_loop_ephemeral_markdown(rel):
                continue
            logical_rel = _strip_worktree_prefix(rel)
            if ("/" not in logical_rel or logical_rel.startswith("crates/")) and not resolved.exists():
                return logical_rel
    return None


def handle_file_write_tool(
    payload: Dict[str, Any],
    tool_name: str,
    args: Dict[str, Any],
    last_input: str,
    in_loop: bool,
) -> Dict[str, Any]:
    raw_path = str(args.get("Path") or "").strip()
    if in_loop and AKZIO_PATH_RE.search(raw_path):
        return {
            "decision": "deny",
            "reason": "Touching ~/.akzio is strictly forbidden in autonomous loop mode (AGENTS.md §1).",
        }

    cwd = str(payload.get("cwd") or "")
    target = _resolve_target_path(raw_path, cwd)
    if target is None:
        return {"decision": "ask"}

    # Do not block Antigravity artifact directory writes outside the repository.
    artifact_dir_raw = str(payload.get("artifactDirectoryPath") or "").strip()
    if artifact_dir_raw:
        artifact_root = Path(artifact_dir_raw).expanduser().resolve(strict=False)
        try:
            target.relative_to(artifact_root)
            return {"decision": "ask"}
        except ValueError:
            pass

    rel = _repo_relative_path(target)
    if rel is None:
        return {"decision": "ask"}

    if in_loop:
        is_leader = (LEADER_TOKEN in last_input) and (SUBAGENT_TOKEN not in last_input)
        if is_leader and not rel.startswith("outputs/loop/"):
            return {
                "decision": "deny",
                "reason": (
                    f"Loop Leader must not directly edit '{rel}'. "
                    f"Delegate edits to rust-worker inside outputs/loop/worktrees/issue-<N>."
                ),
            }
        if not is_leader and not rel.startswith(("outputs/loop/worktrees/", "outputs/loop/tmp/")):
            return {
                "decision": "deny",
                "reason": (
                    f"Loop subagent write to '{rel}' is outside outputs/loop/worktrees/ and outputs/loop/tmp/."
                ),
            }

    md_decision = _check_markdown_policy(
        target=target,
        rel_path=rel,
        is_create_tool=(tool_name == "write_file"),
        in_loop=in_loop,
    )
    if md_decision is not None:
        return md_decision

    if in_loop:
        return {"decision": "allow"}
    return {"decision": "ask"}


def handle_pre_tool_use(payload: Dict[str, Any]) -> Dict[str, Any]:
    tool_call = payload.get("toolCall") or {}
    tool_name = str(tool_call.get("name") or "")
    args = tool_call.get("args") or {}
    last_input = str(payload.get("lastUserInput") or "")
    in_loop = (LEADER_TOKEN in last_input) or (SUBAGENT_TOKEN in last_input)

    if tool_name in ("write_file", "edit_file"):
        return handle_file_write_tool(payload, tool_name, args, last_input, in_loop)

    if tool_name != "run_command":
        return {"decision": "ask"}

    cmd_line = str(args.get("CommandLine") or "")
    cwd = str(payload.get("cwd") or "")

    # Interactive sessions: preserve user permissions, except block unsolicited shell .md creation in root/crates.
    if not in_loop:
        blocked_md = _find_shell_markdown_write(cmd_line, cwd)
        if blocked_md:
            return {
                "decision": "deny",
                "reason": (
                    f"Creating unsolicited Markdown file '{blocked_md}' via shell is forbidden "
                    f"(anti-doc-slop guardrail)."
                ),
            }
        return {"decision": "ask"}

    # Hard denials in autonomous loop mode
    stripped_for_akzio = strip_commit_or_title_strings(cmd_line)
    if AKZIO_PATH_RE.search(stripped_for_akzio):
        return {
            "decision": "deny",
            "reason": "Touching ~/.akzio is strictly forbidden in autonomous loop mode (AGENTS.md §1/§5).",
        }

    if re.search(r"\bgit\s+push\b.*--force", cmd_line):
        return {"decision": "deny", "reason": "Force-pushing is forbidden in autonomous loop."}
    if re.search(r"\bgit\s+push\b.*\b(?:main|master)\b", cmd_line):
        return {"decision": "deny", "reason": "Direct push to main/master is forbidden."}
    if re.search(r"\bscripts/run_core\.sh\s+paper\b", cmd_line):
        return {"decision": "deny", "reason": "Unattended loop must not launch real Alpaca Paper runs."}

    # Prevent git write/destructive commands from touching the developer's main working directory
    if re.search(r"\bgit\s+(?:reset\s+--hard|clean\s+-[fd]|checkout\s+--|stash\b)", cmd_line):
        if not is_in_worktree(cmd_line) and not is_in_worktree(cwd):
            return {
                "decision": "deny",
                "reason": "Destructive git reset/clean/checkout/stash is blocked in the main workspace. Operate inside outputs/loop/worktrees/issue-<N>.",
            }
    if re.search(r"\bgit\s+(?:add|commit)\b", cmd_line):
        if not is_in_worktree(cmd_line) and not is_in_worktree(cwd):
            return {
                "decision": "deny",
                "reason": "git add/commit is blocked in the main workspace. Operate inside outputs/loop/worktrees/issue-<N>.",
            }

    if is_safe_loop_command(cmd_line, cwd):
        return {"decision": "allow"}

    return {"decision": "ask"}


def handle_stop(payload: Dict[str, Any]) -> Dict[str, Any]:
    last_input = str(payload.get("lastUserInput") or "")
    # Never hijack subagents (`[AKZIO-LOOP-SUBAGENT]`) to run Leader polling
    if LEADER_TOKEN not in last_input or SUBAGENT_TOKEN in last_input:
        return {"decision": "stop"}

    term_reason = str(payload.get("terminationReason") or "")
    continuation_active = bool(payload.get("continuationActive"))
    exec_num = int(payload.get("executionNum") or 0)

    if term_reason != "NO_TOOL_CALL" or continuation_active or exec_num >= 2:
        return {"decision": "stop"}

    if QUEUE_CACHE_PATH.exists():
        try:
            cache = json.loads(QUEUE_CACHE_PATH.read_text(encoding="utf-8"))
            if cache.get("stale"):
                return {"decision": "stop"}
            counts = cache.get("counts") or {}
            ready_cnt = int(counts.get("ready") or 0)
            inbox_cnt = int(counts.get("inbox") or 0)
            if ready_cnt > 0 or inbox_cnt > 0:
                return {
                    "decision": "continue",
                    "reason": (
                        f"Queue still has actionable items (ready={ready_cnt}, inbox={inbox_cnt}) "
                        f"and wave step count is within budget (executionNum={exec_num} < 2). "
                        f"Run `python3 .agents/loop/bin/issue_hub.py poll` to process one more item before context reset."
                    ),
                }
        except Exception:
            pass

    return {"decision": "stop"}


def main() -> int:
    raw = sys.stdin.read().strip()
    if not raw:
        print("{}")
        return 0
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        print("{}")
        return 0

    event_name = payload.get("hookEventName")
    if event_name == "PreToolUse":
        out = handle_pre_tool_use(payload)
    elif event_name == "Stop":
        out = handle_stop(payload)
    else:
        out = {}

    print(json.dumps(out, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
