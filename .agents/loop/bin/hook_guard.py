#!/usr/bin/env python3
"""Enforce the Akzio loop policy on Antigravity PreToolUse payloads."""

from __future__ import annotations

import argparse
import io
import json
import re
import shlex
import sqlite3
import sys
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[3]
HUB_RELATIVE = ".agents/loop/bin/issue_hub.py"
TMP_ROOT = REPO_ROOT / "outputs/loop/tmp"
WORKTREES_ROOT = REPO_ROOT / "outputs/loop/worktrees"
LESSONS_PATH = REPO_ROOT / ".agents/loop/agent-lessons.md"

WAVE_PATTERN = r"(?P<wave>[0-9a-f]{32})"
TASK_PATTERN = r"(?P<task>[0-9]+)"
HEAD_PATTERN = r"(?P<version>[0-9a-f]{40}(?:[0-9a-f]{24})?)"
ROLE_HEADERS = {
    "leader": re.compile(rf"\[AKZIO-LOOP-LEADER\] {WAVE_PATTERN}"),
    "scout": re.compile(rf"\[AKZIO-LOOP-SCOUT\] {WAVE_PATTERN}"),
    "worker": re.compile(rf"\[AKZIO-LOOP-WORKER\] {WAVE_PATTERN} {TASK_PATTERN} (?P<version>[0-9]+)"),
    "verifier": re.compile(rf"\[AKZIO-LOOP-VERIFIER\] {WAVE_PATTERN} {TASK_PATTERN} {HEAD_PATTERN}"),
}
ROLE_COMMANDS = {
    "leader": {"poll", "show", "post-design", "claim", "verify-worktree", "review-missing", "worktree-land", "block"},
    "scout": {"poll", "propose"},
    "worker": {"show", "worker-test", "worker-commit"},
    "verifier": {"show", "show-diff", "review"},
}
DELEGATE_ROLES = {"issue-scout": "scout", "rust-worker": "worker", "rust-verifier": "verifier"}
READ_TOOLS = {"read_file", "view_file"}
CREATE_TOOLS = {"write_file", "write_to_file"}
WRITE_TOOLS = CREATE_TOOLS | {"edit_file"}
FILE_PATH_ALIASES = {"view_file": "AbsolutePath", "write_to_file": "TargetFile"}
CLI_TEMP_FILES = {"propose": "body_file", "post-design": "design_file", "block": "report_file"}

# Argument quoting is allowed. Control operators, expansions, and escapes
# stay outside the grammar so the shell and shlex see the same argv.
QUOTED_PART = r'''(?:'[^'\x00-\x1f\x7f]*'|"[^"$`\\!\x00-\x1f\x7f]*")'''
BARE_CHAR = r"[A-Za-z0-9_./:@%+,\-]"
LITERAL_WORD = rf"(?:{BARE_CHAR}|{QUOTED_PART})+"
# zsh can expand an unquoted leading '=' even after an empty quoted fragment.
# Limit bare '=' to the option separator in an explicit --name=value argument.
LITERAL_ARG = rf"(?:--[a-z][a-z0-9-]*=)?{LITERAL_WORD}"
LITERAL_COMMAND = re.compile(rf"[ \t]*{LITERAL_ARG}(?:[ \t]+{LITERAL_ARG})*[ \t]*")


@dataclass(frozen=True)
class LoopHeader:
    role: str
    wave: str
    task_id: Optional[int]
    version: str


@dataclass(frozen=True)
class LoopScope:
    worktree: Optional[Path] = None
    allowed_files: frozenset[str] = frozenset()


def _deny(reason: str) -> Dict[str, str]:
    return {"decision": "deny", "reason": reason}


def _parse_loop_header(prompt: Any) -> Optional[LoopHeader]:
    if not isinstance(prompt, str):
        return None
    line = prompt.partition("\n")[0].removesuffix("\r")
    for role, pattern in ROLE_HEADERS.items():
        match = pattern.fullmatch(line)
        if match is None:
            continue
        fields = match.groupdict()
        task_id = int(fields["task"]) if "task" in fields else None
        version = fields.get("version", "")
        if role == "worker":
            version = str(int(version))
        return LoopHeader(role, fields["wave"], task_id, version)
    if re.match(r"[\s\ufeff]*\[AKZIO-(?:LOOP|AUTONOMOUS-LOOP)", line, re.IGNORECASE):
        raise ValueError("Malformed or legacy Akzio loop role header.")
    return None


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
    m = re.match(r"^outputs/loop/worktrees/(?:issue|task)-\d+/(.+)$", rel_path)
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
            return _deny(f"Creating new Markdown file '{logical_rel}' is forbidden for loop workers.")
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


def _file_tool_path(tool_name: str, args: Dict[str, Any]) -> Any:
    path = args.get("Path")
    alias = FILE_PATH_ALIASES.get(tool_name)
    if alias is not None and alias in args:
        if path is not None and path != args[alias]:
            raise ValueError("Conflicting file tool paths.")
        return args[alias]
    return path


def _handle_interactive(payload: Dict[str, Any], tool_name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    cwd = str(payload.get("cwd") or "")
    if tool_name == "run_command":
        blocked_md = _find_shell_markdown_write(str(args.get("CommandLine") or ""), cwd)
        if blocked_md:
            return _deny(
                f"Creating unsolicited Markdown file '{blocked_md}' via shell is forbidden "
                "(anti-doc-slop guardrail)."
            )
    elif tool_name in WRITE_TOOLS:
        try:
            raw_path = _file_tool_path(tool_name, args)
        except ValueError:
            return {"decision": "ask"}
        target = _resolve_target_path(str(raw_path or "").strip(), cwd)
        if target is not None:
            artifact_dir = str(payload.get("artifactDirectoryPath") or "").strip()
            if artifact_dir and target.is_relative_to(Path(artifact_dir).expanduser().resolve()):
                return {"decision": "ask"}
            rel = _repo_relative_path(target)
            if rel is not None:
                decision = _check_markdown_policy(target, rel, tool_name in CREATE_TOOLS, in_loop=False)
                if decision is not None:
                    return decision
    return {"decision": "ask"}


def _canonical_path(raw_path: Any, cwd: Path) -> Path:
    if not isinstance(raw_path, str) or not raw_path or "\x00" in raw_path:
        raise ValueError("Missing or invalid path.")
    if raw_path.startswith(("~", "$")) or "://" in raw_path:
        raise ValueError("Loop tools require literal filesystem paths.")
    path = cwd / raw_path
    if path != path.resolve():
        raise ValueError("Loop paths must be canonical and cannot traverse symlinks.")
    return path


def _relative_file(path: Path, root: Path) -> Optional[Path]:
    if path == root or not path.is_relative_to(root):
        return None
    return path.relative_to(root)


def _protected_path(relative: Path) -> bool:
    parts = [part.casefold() for part in relative.parts]
    if any(part in {".git", ".akzio", ".gemini", ".env"} or part.startswith(".env.") for part in parts):
        return True
    if parts[0] == "config" and len(parts) > 1:
        filename = parts[-1]
        return filename.endswith((".local.yaml", ".local.toml")) or (
            filename.startswith("local") and filename.endswith(".yaml")
        )
    return False


def _is_source(relative: Optional[Path]) -> bool:
    return (
        relative is not None
        and relative.parts[0].casefold() not in {"outputs", "report", "target", "tmp"}
        and not _protected_path(relative)
    )


def _is_loop_temp(path: Path) -> bool:
    relative = _relative_file(path, TMP_ROOT)
    return relative is not None and not _protected_path(relative)


def _read_scope(header: LoopHeader) -> LoopScope:
    from issue_hub import loop_scope

    scope = loop_scope(header.role, header.wave, header.task_id, header.version)
    if not isinstance(scope, dict):
        raise ValueError("Invalid loop scope.")
    if header.role in {"leader", "scout"}:
        return LoopScope()
    raw_worktree = scope.get("worktree")
    if not isinstance(raw_worktree, str) or not Path(raw_worktree).is_absolute():
        raise ValueError("Loop scope is missing an absolute worktree.")
    tree = _canonical_path(raw_worktree, REPO_ROOT)
    if _relative_file(tree, WORKTREES_ROOT) is None:
        raise ValueError("Loop worktree lies outside the managed root.")
    files = scope.get("allowed_files")
    if not isinstance(files, list) or not all(isinstance(file, str) for file in files):
        raise ValueError("Loop scope is missing its allowed files.")
    return LoopScope(tree, frozenset(files))


def _handle_loop_file(
    header: LoopHeader,
    scope: LoopScope,
    tool_name: str,
    args: Dict[str, Any],
    cwd: Path,
) -> Dict[str, Any]:
    path = _canonical_path(_file_tool_path(tool_name, args), cwd)
    is_read = tool_name in READ_TOOLS
    if scope.worktree is None:
        if _is_loop_temp(path) or (is_read and _is_source(_relative_file(path, REPO_ROOT))):
            return {"decision": "allow"}
        return _deny("This loop role may read repository source and use managed loop temporary files.")

    relative = _relative_file(path, scope.worktree)
    if is_read:
        if path == LESSONS_PATH or _is_source(relative):
            return {"decision": "allow"}
        return _deny("This loop role may read only its assigned worktree and the shared lesson document.")
    if header.role != "worker":
        return _deny("The loop verifier has read-only file access.")
    if relative is None or not _is_source(relative) or relative.as_posix() not in scope.allowed_files:
        return _deny("Worker file writes require a canonical path on the assigned worktree whitelist.")
    return _check_markdown_policy(path, relative.as_posix(), tool_name in CREATE_TOOLS, in_loop=True) or {
        "decision": "allow"
    }


def _parse_cli(arguments: List[str]) -> Optional[argparse.Namespace]:
    from issue_hub import build_parser

    # argparse emits usage and help text before SystemExit. The Hook's stdout
    # must contain a single JSON decision, even for rejected CLI grammar.
    with redirect_stderr(io.StringIO()), redirect_stdout(io.StringIO()):
        try:
            return build_parser().parse_args(arguments)
        except SystemExit:
            return None


def _matches_cli_option(arguments: List[str], parsed: argparse.Namespace, name: str, expected: str) -> bool:
    values = []
    for index, arg in enumerate(arguments):
        flag, separator, value = arg.partition("=")
        if flag == f"--{name}":
            if not separator and index + 1 < len(arguments):
                value = arguments[index + 1]
            values.append(value)
    return values == [expected] and str(getattr(parsed, name, None)) == expected


def _handle_loop_command(header: LoopHeader, cmd_line: Any, cwd: Path) -> Dict[str, Any]:
    if not isinstance(cmd_line, str) or not LITERAL_COMMAND.fullmatch(cmd_line):
        return _deny("Loop shell accepts a single literal issue_hub.py invocation.")
    argv = shlex.split(cmd_line)
    script = REPO_ROOT / HUB_RELATIVE
    if (
        len(argv) < 3
        or argv[0] != "python3"
        or argv[1] not in {HUB_RELATIVE, f"./{HUB_RELATIVE}", str(script)}
        or _canonical_path(argv[1], cwd) != script
    ):
        return _deny("Loop shell accepts only python3 with the canonical issue_hub.py script.")
    command = argv[2]
    if command not in ROLE_COMMANDS[header.role]:
        return _deny("This issue_hub.py command is unavailable to the current loop role.")
    parsed = _parse_cli(argv[2:])
    if parsed is None:
        return _deny("Invalid issue_hub.py command arguments.")

    required_options = {}
    if command not in {"poll", "show", "propose"} or getattr(parsed, "wave", None) is not None:
        required_options["wave"] = header.wave
    if header.task_id is not None:
        required_options["task"] = str(header.task_id)
    if command in {"worker-test", "worker-commit"}:
        required_options["attempt"] = header.version
    if command == "review":
        required_options["head"] = header.version
    for name, expected in required_options.items():
        if not _matches_cli_option(argv[3:], parsed, name, expected):
            return _deny(f"Loop command --{name} must match the active role header.")

    note_option = CLI_TEMP_FILES.get(command)
    if note_option is not None and not _is_loop_temp(_canonical_path(getattr(parsed, note_option, None), REPO_ROOT)):
        return _deny("Loop notes must be canonical files in outputs/loop/tmp.")
    return {"decision": "allow"}


def _validate_child(header: LoopHeader, prompt: Any, role: Optional[str] = None) -> bool:
    child = _parse_loop_header(prompt)
    if child is None or child.wave != header.wave or child.role == "leader" or (role is not None and child.role != role):
        return False
    _read_scope(child)
    return True


def _handle_loop_subagent(header: LoopHeader, tool_name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    if header.role != "leader":
        return _deny("Only the loop leader may delegate or manage loop subagents.")
    if tool_name == "invoke_subagent":
        type_name = args.get("TypeName")
        role = DELEGATE_ROLES.get(type_name) if isinstance(type_name, str) else None
        if not role or args.get("Workspace") not in (None, "inherit"):
            return _deny("The leader may invoke known loop subagent types in the current workspace.")
        if not _validate_child(header, args.get("Prompt"), role):
            return _deny("Subagent Prompt must start with a verified same-wave header for the requested role.")
    elif tool_name == "send_message":
        recipient = args.get("Recipient")
        if not isinstance(recipient, str) or not recipient.strip():
            return _deny("Loop subagent messages require a recipient.")
        if not _validate_child(header, args.get("Message")):
            return _deny("Loop subagent messages must start with a verified same-wave child header.")
    elif args.get("Action") not in ("list", "kill", "kill_all"):
        return _deny("Unknown subagent management action.")
    return {"decision": "allow"}


def handle_pre_tool_use(payload: Dict[str, Any]) -> Dict[str, Any]:
    tool_call = payload.get("toolCall")
    if not isinstance(tool_call, dict):
        tool_call = {}
    tool_name = str(tool_call.get("name") or "")
    args = tool_call.get("args")
    if not isinstance(args, dict):
        args = {}

    try:
        header = _parse_loop_header(payload.get("lastUserInput"))
    except ValueError:
        return _deny("Malformed or legacy Akzio loop role header.")
    if header is None:
        return _handle_interactive(payload, tool_name, args)

    try:
        scope = _read_scope(header)
    except (ImportError, OSError, OverflowError, RuntimeError, ValueError, sqlite3.Error):
        return _deny("The Akzio loop role is outside its active wave or task scope.")
    try:
        cwd = Path(payload.get("cwd") or REPO_ROOT).resolve()
        if tool_name in READ_TOOLS | WRITE_TOOLS:
            return _handle_loop_file(header, scope, tool_name, args, cwd)
        if tool_name == "run_command":
            return _handle_loop_command(header, args.get("CommandLine"), cwd)
        if tool_name in {"invoke_subagent", "send_message", "manage_subagents"}:
            return _handle_loop_subagent(header, tool_name, args)
    except (ImportError, OSError, OverflowError, RuntimeError, TypeError, ValueError, sqlite3.Error):
        return _deny("Loop tool scope or tool arguments could not be verified.")
    return _deny("This tool is unavailable in the Akzio loop.")


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError:
        payload = None
    decision = {}
    if isinstance(payload, dict) and payload.get("hookEventName") == "PreToolUse":
        decision = handle_pre_tool_use(payload)
    print(json.dumps(decision, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
