#!/usr/bin/env python3
"""Akzio development loop with local verification and operator delivery."""

from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid
from collections import Counter
from contextlib import contextmanager
from pathlib import Path, PurePosixPath

REPO_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_ROOT = REPO_ROOT / "outputs" / "loop"
STATE_DIR = OUTPUT_ROOT / "state"
WORKTREES_DIR = OUTPUT_ROOT / "worktrees"
TMP_DIR = OUTPUT_ROOT / "tmp"
REPORTS_DIR = OUTPUT_ROOT / "reports"
DB_PATH = STATE_DIR / "loop.sqlite3"
INTEGRATION_REF = "refs/heads/loop/nightly"
MAIN_REF = "refs/heads/main"
REMOTE_MAIN_REF = "refs/remotes/origin/main"
WAVE_SECONDS = 2 * 60 * 60
MAX_ATTEMPTS = 2
RESUMABLE = ("landing", "reviewed", "verified", "in_progress")
PRIORITIES = ("P1", "P2", "P3")
WAITING = ("awaiting_input", "awaiting_pr", "awaiting_merge")
VISIBLE_STATUS = "CASE WHEN status = 'blocked' AND blocked_reason IN ('awaiting_input', 'awaiting_pr', 'awaiting_merge') THEN blocked_reason ELSE status END"
DECISION_TTL = 7 * 24 * 3600
DECISION_REASONS = ("needs_product_decision", "contract_boundary")
SHA = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
BANNED_ATTRIBUTES = re.compile(r"#\s*!?\s*\[\s*(?:allow\s*\(|ignore\b)")
TEST_ATTRIBUTE = re.compile(r"#\s*\[\s*(?:tokio::)?test\b")

SCHEMA = """
CREATE TABLE IF NOT EXISTS loop_info (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    identity TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS wave (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    token TEXT NOT NULL,
    expires_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY,
    fingerprint TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    priority TEXT NOT NULL CHECK (priority IN ('P1', 'P2', 'P3')),
    area TEXT NOT NULL,
    candidate_files TEXT NOT NULL,
    body TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'inbox' CHECK (status IN (
        'inbox', 'ready', 'in_progress', 'verified', 'reviewed', 'landing', 'done', 'blocked'
    )),
    design_body TEXT,
    allowed_files TEXT,
    wave TEXT,
    base_sha TEXT,
    attempt INTEGER NOT NULL DEFAULT 0,
    attempt_base TEXT,
    candidate_head TEXT,
    verified_head TEXT,
    checks TEXT,
    review_summary TEXT,
    last_failure TEXT,
    blocked_reason TEXT,
    checkout TEXT
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY,
    created_at INTEGER NOT NULL,
    task_id INTEGER REFERENCES tasks(id),
    kind TEXT NOT NULL,
    details TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS deliveries (
    destination TEXT NOT NULL,
    event_id INTEGER NOT NULL REFERENCES events(id),
    comment_id INTEGER NOT NULL,
    PRIMARY KEY (destination, event_id)
);
CREATE TABLE IF NOT EXISTS github_tasks (
    task_id INTEGER PRIMARY KEY REFERENCES tasks(id),
    repo TEXT NOT NULL,
    issue INTEGER NOT NULL,
    start_comment INTEGER NOT NULL,
    snapshot_sha TEXT NOT NULL,
    base_sha TEXT NOT NULL,
    pr INTEGER,
    merge_sha TEXT,
    acknowledged INTEGER NOT NULL DEFAULT 0,
    UNIQUE(repo, issue)
);
CREATE TABLE IF NOT EXISTS decision_requests (
    id TEXT PRIMARY KEY,
    task_id INTEGER NOT NULL REFERENCES tasks(id),
    version INTEGER NOT NULL,
    requested_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    reason TEXT NOT NULL,
    visibility TEXT NOT NULL CHECK (visibility IN ('local', 'github')),
    request TEXT NOT NULL,
    repo TEXT,
    issue INTEGER,
    opened_at INTEGER,
    public_body_sha TEXT,
    approver INTEGER,
    invalidated INTEGER NOT NULL DEFAULT 0,
    notified INTEGER NOT NULL DEFAULT 0,
    closed INTEGER NOT NULL DEFAULT 0,
    choice TEXT,
    decided_at INTEGER,
    actor INTEGER,
    comment INTEGER,
    UNIQUE(task_id, version),
    UNIQUE(repo, issue),
    UNIQUE(repo, comment)
);
CREATE TABLE IF NOT EXISTS github_sync (
    repo TEXT PRIMARY KEY,
    enabled_at INTEGER NOT NULL,
    synced_at INTEGER NOT NULL
);
"""


def utc_timestamp() -> int:
    return int(time.time())


def iso_time(timestamp: int) -> str:
    return dt.datetime.fromtimestamp(timestamp, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def emit(value) -> int:
    print(json.dumps(value, ensure_ascii=False, indent=2))
    return 0


def run_cmd(cmd, cwd=None, *, check=True, timeout=120, env=None, input_text=None):
    proc = subprocess.run(
        cmd, cwd=cwd or REPO_ROOT, text=True, capture_output=True, input=input_text,
        timeout=timeout, env=env,
    )
    if check and proc.returncode:
        detail = (proc.stderr or proc.stdout).strip()[-1200:]
        raise RuntimeError(f"{cmd[0]} exited {proc.returncode}: {detail}")
    return proc


def git(*args, cwd=None, check=True):
    return run_cmd(["git", "--literal-pathspecs", *args], cwd, check=check)


def ensure_dirs() -> None:
    for path in (STATE_DIR, WORKTREES_DIR, TMP_DIR, REPORTS_DIR):
        if path.resolve() != path:
            raise RuntimeError(f"Loop output directory must not traverse a symlink: {path}")
        path.mkdir(mode=0o700, parents=True, exist_ok=True)


@contextmanager
def open_db(*, readonly=False):
    if not readonly:
        ensure_dirs()
    if DB_PATH.resolve() != DB_PATH:
        raise RuntimeError("The loop database path must not traverse a symlink.")
    uri = f"{DB_PATH.as_uri()}?mode=ro" if readonly else str(DB_PATH)
    db = sqlite3.connect(uri, uri=readonly, isolation_level=None, timeout=15)
    db.row_factory = sqlite3.Row
    try:
        db.execute("PRAGMA foreign_keys = ON")
        if not readonly:
            db.execute("PRAGMA synchronous = FULL")
            db.executescript(SCHEMA)
            with transaction(db):
                if "checkout" not in {column["name"] for column in db.execute("PRAGMA table_info(tasks)")}:
                    db.execute("ALTER TABLE tasks ADD COLUMN checkout TEXT")
            db.execute(
                "INSERT OR IGNORE INTO loop_info VALUES (1, ?, ?)",
                (uuid.uuid4().hex, utc_timestamp()),
            )
        yield db
    finally:
        db.close()


@contextmanager
def transaction(db):
    db.execute("BEGIN IMMEDIATE")
    try:
        yield
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise


@contextmanager
def process_lock(name):
    ensure_dirs()
    path = STATE_DIR / f"{name}.lock"
    if path.resolve() != path:
        raise RuntimeError(f"Invalid loop lock path: {path}")
    with path.open("a", encoding="utf-8") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        yield


def event(db, kind, task_id=None, **details):
    db.execute(
        "INSERT INTO events (created_at, task_id, kind, details) VALUES (?, ?, ?, ?)",
        (utc_timestamp(), task_id, kind, json.dumps(details, ensure_ascii=False)),
    )


def task_row(db, task_id):
    row = db.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
    if row is None:
        raise RuntimeError(f"Unknown task #{task_id}.")
    return row


def checkout_name(task):
    name = task["checkout"] if "checkout" in task.keys() else None
    return name or f"task-{task['id']}"


def worktree_path(task) -> Path:
    path = WORKTREES_DIR / checkout_name(task)
    if path.resolve() != path:
        raise RuntimeError(f"Invalid task worktree path: {path}")
    return path


def task_view(db, row):
    result = dict(row)
    del result["wave"]
    result.pop("checkout", None)
    for field in ("candidate_files", "allowed_files", "checks", "last_failure"):
        if result[field] is not None:
            result[field] = json.loads(result[field])
    if row["base_sha"]:
        result["worktree_path"] = str(worktree_path(row))
    if row["status"] == "blocked" and row["blocked_reason"] in WAITING:
        result["status"] = row["blocked_reason"]
    # Read-only status must also work before the first writable upgrade of an old queue.
    tables = {item[0] for item in db.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('github_tasks', 'decision_requests')"
    )}
    if "github_tasks" in tables:
        origin = db.execute("SELECT * FROM github_tasks WHERE task_id = ?", (row["id"],)).fetchone()
        if origin:
            result["github"] = dict(origin)
    if "decision_requests" in tables:
        decision = db.execute(
            "SELECT * FROM decision_requests WHERE task_id = ? ORDER BY version DESC LIMIT 1", (row["id"],),
        ).fetchone()
        if decision:
            result["decision"] = {**dict(decision), "request": json.loads(decision["request"])}
    return result


def require_wave(db, token, *, renew=False):
    row = db.execute("SELECT token, expires_at FROM wave WHERE singleton = 1").fetchone()
    now = utc_timestamp()
    if not row or row["token"] != token or row["expires_at"] <= now:
        raise RuntimeError("This loop wave is absent, finished, or expired.")
    if renew:
        db.execute("UPDATE wave SET expires_at = ? WHERE singleton = 1", (now + WAVE_SECONDS,))


def owned_task(db, task_id, wave, statuses):
    require_wave(db, wave, renew=True)
    task = task_row(db, task_id)
    if task["wave"] != wave or task["status"] not in statuses:
        raise RuntimeError(f"Task #{task_id} is not owned in the required stage by this wave.")
    return task


def loop_scope(role: str, wave: str, task_id: int | None, version: str) -> dict:
    """Read-only scope for Antigravity tooling. OS sandbox policy is configured by the runtime."""
    with open_db(readonly=True) as db:
        require_wave(db, wave)
        if role in ("leader", "scout"):
            return {}
        if role not in ("worker", "verifier"):
            raise RuntimeError("Unknown loop role.")
        task = task_row(db, task_id)
        expected_stage = "in_progress" if role == "worker" else "verified"
        expected_version = str(task["attempt"]) if role == "worker" else task["verified_head"]
        if task["wave"] != wave or task["status"] != expected_stage or version != expected_version:
            raise RuntimeError("This role no longer owns the task stage or version.")
        return {
            "worktree": str(worktree_path(task)),
            "allowed_files": json.loads(task["allowed_files"]),
        }


def next_task(db):
    return db.execute(
        """SELECT * FROM tasks
           WHERE status IN ('landing', 'reviewed', 'verified', 'in_progress', 'ready', 'inbox')
           ORDER BY CASE status WHEN 'landing' THEN 0 WHEN 'reviewed' THEN 1
               WHEN 'verified' THEN 2 WHEN 'in_progress' THEN 3 WHEN 'ready' THEN 4 ELSE 5 END,
               priority, id LIMIT 1"""
    ).fetchone()


def cmd_start_wave(_args):
    with process_lock("control"), open_db() as db, transaction(db):
        now = utc_timestamp()
        current = db.execute("SELECT expires_at FROM wave WHERE singleton = 1").fetchone()
        if current and current["expires_at"] > now:
            return emit({"status": "busy", "expires_at": iso_time(current["expires_at"])})
        token = uuid.uuid4().hex
        task = next_task(db)
        checkpoint = db.execute("SELECT coalesce(max(id), 0) FROM events").fetchone()[0]
        db.execute("INSERT OR REPLACE INTO wave VALUES (1, ?, ?)", (token, now + WAVE_SECONDS))
    return emit({"status": "started", "wave": token, "expires_at": iso_time(now + WAVE_SECONDS),
                 "expected_task": task["id"] if task else None, "event_checkpoint": checkpoint})


def cmd_finish_wave(args):
    if (args.expect_task is None) != (args.after_event is None):
        raise ValueError("Provide both the expected task and the event checkpoint.")
    if args.expect_task is not None and (args.expect_task < 1 or args.after_event < 0):
        raise ValueError("The expected task must be positive and the event checkpoint nonnegative.")
    with process_lock("control"), open_db() as db, transaction(db):
        closed = db.execute("DELETE FROM wave WHERE token = ?", (args.wave,)).rowcount
        if not closed:
            return emit({"status": "stale"})
        if args.expect_task is not None:
            progress = db.execute(
                """SELECT 1 FROM events WHERE task_id = ? AND id > ? AND kind IN (
                   'task_designed', 'task_claimed', 'candidate_committed', 'candidate_recovered',
                   'gate_failed', 'gate_passed', 'review_passed', 'review_rejected', 'review_missing',
                   'landing_intent', 'task_landed', 'task_pr_queued', 'task_blocked', 'decision_requested'
                ) LIMIT 1""", (args.expect_task, args.after_event),
            ).fetchone()
            if not progress:
                emit({"status": "no_progress", "task": args.expect_task})
                return 1
    return emit({"status": "finished"})


def get_dirty_main_files():
    status = git("status", "--porcelain=v1", "-z", "--untracked-files=all").stdout.split("\0")
    files = {"modified": [], "untracked": []}
    index = 0
    while index < len(status) and status[index]:
        row = status[index]
        paths = [row[3:]]
        index += 1
        if any(code in row[:2] for code in "RC") and index < len(status):
            paths.append(status[index])
            index += 1
        bucket = "untracked" if row.startswith("??") else "modified"
        files[bucket].extend(path for path in paths if not path.startswith("outputs/"))
    return files


def cmd_poll(_args):
    counts = {}
    target = None
    target_view = None
    if DB_PATH.exists():
        with open_db(readonly=True) as db:
            counts = dict(db.execute(f"SELECT {VISIBLE_STATUS} AS stage, count(*) FROM tasks GROUP BY stage"))
            target = next_task(db)
            target_view = task_view(db, target) if target else None
    action = "scout_needed"
    if target:
        action = {"ready": "execute_ready", "inbox": "design_inbox"}.get(target["status"], "resume_task")
    return emit({
        "recommended_action": action,
        "target_task": target_view,
        "counts": counts,
        "dirty_main_files": get_dirty_main_files(),
    })


def cmd_show(args):
    with open_db(readonly=True) as db:
        return emit(task_view(db, task_row(db, args.task)))


def cmd_list(args):
    if not DB_PATH.exists():
        return emit({"tasks": []})
    with open_db(readonly=True) as db:
        rows = db.execute(
            f"""SELECT id, title, priority, {VISIBLE_STATUS} AS status, attempt, verified_head, blocked_reason
                FROM tasks WHERE (? IS NULL OR {VISIBLE_STATUS} = ?) ORDER BY id""",
            (args.status, args.status),
        ).fetchall()
    return emit({"tasks": [dict(row) for row in rows]})


def read_note(raw_path):
    path = REPO_ROOT / raw_path
    resolved = path.resolve()
    if not resolved.is_relative_to(TMP_DIR) or not resolved.is_file() or resolved != path.absolute():
        raise ValueError("Notes must be regular files under outputs/loop/tmp/.")
    text = resolved.read_text(encoding="utf-8").strip()
    if not text:
        raise ValueError("The note is empty.")
    return text


def validate_relative_path(name):
    path = PurePosixPath(name)
    if (
        not name or path.is_absolute() or path.as_posix() != name or name == "."
        or any(part in ("..", ".git", ".akzio", ".agents", "outputs", "target") for part in path.parts)
        or any(part == ".env" or part.startswith(".env.") for part in path.parts)
        or "\\" in name or any(ord(char) < 32 for char in name)
    ):
        raise ValueError(f"Unsafe or reserved repository path: {name!r}")
    target = REPO_ROOT / name
    if target.resolve() != target or target.is_dir():
        raise ValueError(f"Expected a regular repository file path: {name!r}")
    return target


def file_list(csv, *, existing):
    files = list(dict.fromkeys(item.strip() for item in csv.split(",") if item.strip()))
    if not files:
        raise ValueError("At least one repository file is required.")
    for name in files:
        target = validate_relative_path(name)
        if not target.is_file() and (existing or not target.parent.is_dir()):
            raise ValueError(f"File or parent directory missing: {name}")
    return files


def cmd_propose(args):
    fingerprint = args.fingerprint.strip()
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{3,79}", fingerprint):
        raise ValueError("Fingerprint must be 4–80 lowercase alphanumeric, dash, or underscore characters.")
    files = file_list(args.candidate_files, existing=True)
    title, area = args.title.strip(), args.area.strip()
    if not title or not area:
        raise ValueError("A title and area are required.")
    body = read_note(args.body_file)
    if args.dry_run:
        return emit({"status": "dry_run_valid", "fingerprint": fingerprint, "candidate_files": files})
    with open_db() as db, transaction(db):
        previous = db.execute("SELECT id FROM tasks WHERE fingerprint = ?", (fingerprint,)).fetchone()
        if previous:
            return emit({"status": "skipped_duplicate", "task": previous["id"]})
        task_id = db.execute(
            "INSERT INTO tasks (fingerprint, title, priority, area, candidate_files, body) VALUES (?, ?, ?, ?, ?, ?)",
            (fingerprint, title, args.priority, area, json.dumps(files), body),
        ).lastrowid
        event(db, "task_proposed", task_id, priority=args.priority)
    return emit({"status": "created", "task": task_id})


def cmd_post_design(args):
    allowed = file_list(args.allowed_files, existing=False)
    design = read_note(args.design_file)
    with process_lock("control"), open_db() as db, transaction(db):
        require_wave(db, args.wave, renew=True)
        task = task_row(db, args.task)
        if task["status"] != "inbox":
            raise RuntimeError("Only an inbox task can receive a design.")
        if db.execute("SELECT 1 FROM github_tasks WHERE task_id = ?", (args.task,)).fetchone():
            if any(PurePosixPath(name).parts[0] == ".github" for name in allowed):
                raise ValueError("GitHub tasks cannot change repository workflow or collaboration configuration.")
        db.execute(
            "UPDATE tasks SET status = 'ready', design_body = ?, allowed_files = ? WHERE id = ?",
            (design, json.dumps(allowed), args.task),
        )
        event(db, "task_designed", args.task)
    return emit({"status": "ready", "task": args.task})


def ref_commit(ref):
    result = git("rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}", check=False)
    return result.stdout.strip() if result.returncode == 0 else None


def branch_checked_out(ref):
    branches = git("worktree", "list", "--porcelain", "-z").stdout.split("\0")
    return f"branch {ref}" in branches


def integration_base():
    main = ref_commit(MAIN_REF)
    if not main:
        raise RuntimeError("The local main branch is required to claim a task.")
    current = ref_commit(INTEGRATION_REF)
    if current and is_ancestor(main, current):
        return current
    if current and not is_ancestor(current, main):
        raise RuntimeError("The local main and loop/nightly branches have diverged.")
    if branch_checked_out(INTEGRATION_REF):
        raise RuntimeError("The integration branch is checked out in a worktree.")
    git("update-ref", INTEGRATION_REF, main, current or "0" * len(main))
    return main


def task_branch(task):
    return f"loop/{checkout_name(task)}"


def check_worktree(task):
    path = worktree_path(task)
    if not path.is_dir() or Path(git("rev-parse", "--show-toplevel", cwd=path).stdout.strip()) != path:
        raise RuntimeError(f"The registered worktree is missing or invalid: {path}")
    git_dir_args = ("rev-parse", "--path-format=absolute", "--git-common-dir")
    if git(*git_dir_args, cwd=path).stdout != git(*git_dir_args).stdout:
        raise RuntimeError(f"The registered worktree is missing or invalid: {path}")
    if git("symbolic-ref", "--quiet", "HEAD", cwd=path).stdout.strip() != f"refs/heads/{task_branch(task)}":
        raise RuntimeError(f"Unexpected branch in {path}.")
    return path


def create_or_resume_worktree(task):
    path = worktree_path(task)
    branch = task_branch(task)
    if not path.exists():
        if ref_commit(f"refs/heads/{branch}"):
            git("worktree", "add", str(path), branch)
        else:
            git("worktree", "add", "-b", branch, str(path), task["base_sha"])
    return check_worktree(task)


def cmd_claim(args):
    with process_lock("control"), open_db() as db:
        with transaction(db):
            require_wave(db, args.wave, renew=True)
            task = task_row(db, args.task)
            if task["status"] == "ready":
                active = db.execute(
                    "SELECT id FROM tasks WHERE status IN ('in_progress', 'verified', 'reviewed', 'landing')"
                ).fetchone()
                if active:
                    raise RuntimeError(f"Resume task #{active['id']} before claiming a new task.")
                github_task = db.execute("SELECT base_sha FROM github_tasks WHERE task_id = ?", (args.task,)).fetchone()
                base = github_task["base_sha"] if github_task else integration_base()
                if github_task and ref_commit(base) != base:
                    raise RuntimeError("The frozen base of this GitHub task is unavailable.")
                identity = db.execute("SELECT identity FROM loop_info").fetchone()[0]
                suffix = uuid.uuid4().hex if task["checkout"] else identity
                checkout = f"task-{args.task}-{suffix}"
                db.execute(
                    """UPDATE tasks SET status = 'in_progress', wave = ?, base_sha = ?,
                       attempt = 1, attempt_base = ?, checkout = ? WHERE id = ?""",
                    (args.wave, base, base, checkout, args.task),
                )
                event(db, "task_claimed", args.task, base_sha=base, attempt=1)
            elif task["status"] in RESUMABLE:
                if task["wave"] != args.wave:
                    db.execute("UPDATE tasks SET wave = ? WHERE id = ?", (args.wave, args.task))
                    event(db, "task_resumed", args.task, attempt=task["attempt"])
            else:
                raise RuntimeError(f"Task #{args.task} cannot be claimed in stage {task['status']}.")
            task = task_row(db, args.task)
        if task["status"] == "in_progress":
            path = create_or_resume_worktree(task)
            if task["candidate_head"] is None:
                task = recover_candidate(db, task, path)
        return emit(task_view(db, task))


def cmd_block(args):
    note = read_note(args.report_file)
    with process_lock("control"), open_db() as db, transaction(db):
        require_wave(db, args.wave, renew=True)
        task = task_row(db, args.task)
        if task["status"] not in ("inbox", "ready", "in_progress", "verified"):
            raise RuntimeError(f"Cannot block a task in stage {task['status']}.")
        if task["status"] in ("in_progress", "verified") and task["wave"] != args.wave:
            raise RuntimeError("Claim the task before blocking it.")
        db.execute("UPDATE tasks SET status = 'blocked', blocked_reason = ? WHERE id = ?", (args.reason_code, args.task))
        event(db, "task_blocked", args.task, reason=args.reason_code, note=note)
    return emit({"status": "blocked", "task": args.task, "reason": args.reason_code})


def decision_request(raw):
    def field(value, limit):
        if not isinstance(value, str) or not value.strip() or len(value) > limit:
            raise ValueError("Decision text is missing or exceeds its length limit.")
        return value.replace("\r\n", "\n").strip()

    request = json.loads(raw)
    if not isinstance(request, dict) or set(request) != {"question", "options", "recommendation"}:
        raise ValueError("A decision needs question, options and recommendation fields.")
    choices = request["options"]
    if not isinstance(choices, dict) or not 2 <= len(choices) <= 5 or not set(choices) <= set("ABCDE"):
        raise ValueError("Use two to five decision options labelled A through E.")
    options = {}
    for key, option in sorted(choices.items()):
        if not isinstance(option, dict) or set(option) != {"label", "impact", "resume"} or type(option["resume"]) is not bool:
            raise ValueError("Each decision option needs label, impact and a boolean resume field.")
        options[key] = {"label": field(option["label"], 160), "impact": field(option["impact"], 800),
                        "resume": option["resume"]}
    recommendation = request["recommendation"]
    if not isinstance(recommendation, str) or recommendation not in options:
        raise ValueError("The recommendation must be one of the declared options.")
    return {"question": field(request["question"], 2000), "options": options, "recommendation": recommendation}


def cmd_request_decision(args):
    request = decision_request(read_note(args.report_file))
    with process_lock("control"), open_db() as db, transaction(db):
        require_wave(db, args.wave, renew=True)
        task = task_row(db, args.task)
        now = utc_timestamp()
        if task["status"] == "blocked" and task["blocked_reason"] == "awaiting_input":
            prior = db.execute(
                "SELECT id, visibility, reason, request, expires_at FROM decision_requests WHERE task_id = ? ORDER BY version DESC LIMIT 1",
                (args.task,),
            ).fetchone()
            if prior and prior["expires_at"] > now:
                matching = (prior["visibility"], prior["reason"], json.loads(prior["request"]))
                if matching == (args.visibility, args.reason_code, request):
                    return emit({"status": "awaiting_input", "task": args.task, "decision_id": prior["id"]})
                raise RuntimeError("Resolve the pending decision before opening another one.")
        elif task["status"] in ("in_progress", "verified"):
            owned_task(db, args.task, args.wave, ("in_progress", "verified"))
        elif not (task["status"] in ("inbox", "ready") or
                  (task["status"] == "blocked" and task["blocked_reason"] in (*DECISION_REASONS, "decision_expired", "decision_changed"))):
            raise RuntimeError(f"Cannot request input in stage {task['status']}.")
        version = db.execute(
            "SELECT coalesce(max(version), 0) + 1 FROM decision_requests WHERE task_id = ?", (args.task,),
        ).fetchone()[0]
        decision_id = f"AZ-{args.task}-R{version}-{uuid.uuid4().hex[:12]}"
        db.execute(
            """INSERT INTO decision_requests
               (id, task_id, version, requested_at, expires_at, reason, visibility, request)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (decision_id, args.task, version, now, now + DECISION_TTL, args.reason_code, args.visibility,
             json.dumps(request, ensure_ascii=False)),
        )
        db.execute("UPDATE tasks SET status = 'blocked', blocked_reason = 'awaiting_input', wave = NULL WHERE id = ?", (args.task,))
        event(db, "decision_requested", args.task, decision_id=decision_id, reason=args.reason_code,
              previous_stage=task["status"], candidate_head=task["candidate_head"], checkout=task["checkout"],
              visibility=args.visibility)
    return emit({"status": "awaiting_input", "task": args.task, "decision_id": decision_id})


def reset_for_design(db, task_id):
    db.execute(
        """UPDATE tasks SET status = 'inbox', blocked_reason = NULL, wave = NULL, design_body = NULL,
           allowed_files = NULL, base_sha = NULL, attempt = 0, attempt_base = NULL,
           candidate_head = NULL, verified_head = NULL, checks = NULL, review_summary = NULL,
           last_failure = NULL WHERE id = ?""", (task_id,),
    )


def apply_decision(db, decision_id, choice, *, actor=None, comment=None, received_at=None):
    decision = db.execute("SELECT * FROM decision_requests WHERE id = ?", (decision_id,)).fetchone()
    if not decision:
        raise ValueError("Unknown decision request.")
    if db.execute("SELECT 1 FROM decision_requests WHERE task_id = ? AND version > ?",
                  (decision["task_id"], decision["version"])).fetchone():
        raise ValueError("This decision request has been superseded.")
    if decision["invalidated"]:
        raise ValueError("This decision request is no longer valid. Request a new version.")
    if decision["choice"] is not None:
        if decision["choice"] == choice and (decision["actor"], decision["comment"]) == (actor, comment):
            return "already_resolved"
        raise ValueError("This decision has already been recorded.")
    now = utc_timestamp()
    decided_at = now if received_at is None else received_at
    if not decision["requested_at"] <= decided_at <= now or decided_at >= decision["expires_at"]:
        raise ValueError("This decision was submitted outside the request's validity period.")
    options = json.loads(decision["request"])["options"]
    if choice not in options:
        raise ValueError("The selected option is not in the request.")
    task = task_row(db, decision["task_id"])
    if (task["status"], task["blocked_reason"]) != ("blocked", "awaiting_input"):
        raise ValueError("The original task is no longer awaiting this decision.")
    db.execute("UPDATE decision_requests SET choice = ?, decided_at = ?, actor = ?, comment = ? WHERE id = ?",
               (choice, decided_at, actor, comment, decision_id))
    stage = "inbox" if options[choice]["resume"] else "blocked"
    if stage == "inbox":
        reset_for_design(db, task["id"])
    else:
        db.execute("UPDATE tasks SET blocked_reason = 'decision_declined' WHERE id = ?", (task["id"],))
    event(db, "decision_recorded", task["id"], decision_id=decision_id, choice=choice, next_stage=stage,
          actor=actor, comment=comment, visibility=decision["visibility"])
    return stage


def cmd_resolve_decision(args):
    with process_lock("control"), open_db() as db, transaction(db):
        stage = apply_decision(db, args.id, args.choice)
    return emit({"status": stage, "decision_id": args.id})


def nul_files(output):
    return [path for path in output.split("\0") if path]


def pending_files(path):
    tracked = git("diff", "--name-only", "--no-renames", "-z", "HEAD", "--", cwd=path).stdout
    untracked = git("ls-files", "--others", "--exclude-standard", "-z", cwd=path).stdout
    return sorted(set(nul_files(tracked) + nul_files(untracked)))


def validate_changes(task, names, path):
    allowed = set(json.loads(task["allowed_files"]))
    extra = sorted(set(names) - allowed)
    if extra:
        raise RuntimeError(f"Changes outside the frozen design: {extra}")
    for name in names:
        if (path / name).resolve() != path / name or (path / name).is_dir():
            raise RuntimeError(f"Task changes must be regular files: {name}")


def cargo_env():
    keep = {
        "PATH", "HOME", "USER", "LOGNAME", "TMPDIR", "TMP", "TEMP", "CARGO_HOME", "RUSTUP_HOME",
        "SSL_CERT_FILE", "SSL_CERT_DIR", "SSL_CERT_CRLFILE", "CARGO_HTTP_PROXY", "ALL_PROXY",
        "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "all_proxy", "http_proxy", "https_proxy", "no_proxy",
    }
    env = {key: val for key, val in os.environ.items() if key in keep or key == "LANG" or key.startswith("LC_")}
    env["CARGO_TARGET_DIR"] = str(OUTPUT_ROOT / "target")
    return env


def worker_task(db, args):
    task = owned_task(db, args.task, args.wave, ("in_progress",))
    if task["attempt"] != args.attempt:
        raise RuntimeError("This Worker attempt is no longer current.")
    return task


def cmd_worker_test(args):
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_-]*", args.crate):
        raise ValueError("Invalid Cargo package name.")
    if args.filter and not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_:.-]*", args.filter):
        raise ValueError("Invalid Rust test filter.")
    with process_lock("control"), open_db() as db:
        with transaction(db):
            task = worker_task(db, args)
        path = check_worktree(task)
        validate_changes(task, pending_files(path), path)
        cmd = ["cargo", "test", "-p", args.crate, "--locked"]
        if args.filter:
            cmd.append(args.filter)
        proc = run_cmd(cmd, path, check=False, timeout=900, env=cargo_env())
        emit({"exit_code": proc.returncode, "stdout": proc.stdout[-4000:], "stderr": proc.stderr[-4000:]})
        return proc.returncode


def is_ancestor(base, head):
    result = git("merge-base", "--is-ancestor", base, head, check=False)
    if result.returncode not in (0, 1):
        raise RuntimeError(f"Could not compare Git commits: {result.stderr.strip()}")
    return result.returncode == 0


def cmd_worker_commit(args):
    message = args.message.strip()
    if not message:
        raise ValueError("A commit message is required.")
    with process_lock("control"), open_db() as db:
        with transaction(db):
            task = worker_task(db, args)
        path = check_worktree(task)
        changes = pending_files(path)
        validate_changes(task, changes, path)
        if changes:
            git("add", "-A", "--", *changes, cwd=path)
            git("commit", "-m", message, cwd=path)
        head = git("rev-parse", "HEAD", cwd=path).stdout.strip()
        if head == task["attempt_base"] or not is_ancestor(task["attempt_base"], head) or pending_files(path):
            raise RuntimeError("This attempt needs a new commit and a clean worktree.")
        committed = changed_files(task["base_sha"], head)
        validate_changes(task, committed, path)
        with transaction(db):
            require_wave(db, args.wave, renew=True)
            if task["candidate_head"] != head:
                db.execute("UPDATE tasks SET candidate_head = ? WHERE id = ?", (head, args.task))
                event(db, "candidate_committed", args.task, head_sha=head, attempt=args.attempt)
    return emit({"status": "committed", "task": args.task, "candidate_head": head, "changed_files": committed})


def changed_files(base, head):
    return nul_files(git("diff", "--name-only", "--no-renames", "--ignore-submodules=none", "-z", base, head, "--").stdout)


def recover_candidate(db, task, path):
    head = git("rev-parse", "HEAD", cwd=path).stdout.strip()
    if head == task["attempt_base"]:
        return task
    branch = f"refs/heads/{task_branch(task)}"
    worktrees = git("worktree", "list", "--porcelain", "-z").stdout.split("\0\0")
    expected = [f"worktree {path}", f"HEAD {head}", f"branch {branch}"]
    if not any(entry.split("\0")[:3] == expected for entry in worktrees):
        raise RuntimeError(f"The registered worktree is missing or invalid: {path}")

    # Status checks the index and worktree separately, including untracked files.
    if git("status", "--porcelain=v1", "-z", "--untracked-files=all", "--ignore-submodules=none", cwd=path).stdout:
        return task
    if not is_ancestor(task["attempt_base"], head):
        return task
    files = changed_files(task["base_sha"], head)
    if not files:
        return task
    try:
        validate_changes(task, files, path)
    except RuntimeError:
        return task

    with transaction(db):
        require_wave(db, task["wave"])
        updated = db.execute(
            """UPDATE tasks SET candidate_head = ? WHERE id = ? AND status = 'in_progress'
               AND wave = ? AND attempt = ? AND candidate_head IS NULL""",
            (head, task["id"], task["wave"], task["attempt"]),
        ).rowcount
        if updated != 1:
            raise RuntimeError("This task attempt is no longer current.")
        event(db, "candidate_recovered", task["id"], head_sha=head, attempt=task["attempt"])
        return task_row(db, task["id"])


def rust_diff_failures(base, head, path, files, task_id):
    failures = []
    marker = f"/// WHY THIS TEST EXISTS (Task #{task_id}):"
    for file in files:
        if not file.endswith(".rs") or not (path / file).is_file():
            continue
        diff = git("diff", "--no-color", "--no-ext-diff", "--no-textconv", "-U0", base, head, "--", file).stdout
        added = []
        new_line = 0
        lines = (path / file).read_text(encoding="utf-8").splitlines()
        for line in diff.splitlines():
            hunk = re.match(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@", line)
            if hunk:
                new_line = int(hunk.group(1))
            elif new_line and line.startswith("+"):
                content = line[1:]
                added.append(content)
                if TEST_ATTRIBUTE.search(content):
                    before = new_line - 2
                    while before >= 0 and lines[before].lstrip().startswith(("//", "#[")):
                        before -= 1
                    after = new_line
                    while after < len(lines) and lines[after].lstrip().startswith(("//", "#[")):
                        after += 1
                    if not any(row.lstrip().startswith(marker) for row in lines[before + 1:after]):
                        failures.append(f"{file}:{new_line} needs {marker}")
                new_line += 1
        if BANNED_ATTRIBUTES.search("\n".join(added)):
            failures.append(f"{file} adds a forbidden allow or ignore attribute.")
    return failures


def rustfmt_needs_formatting(tree, file):
    """Distinguish a named formatting diff from a formatter failure."""
    path = str(tree / file)
    check = run_cmd(["rustfmt", "--edition", "2021", "--check", "--files-with-diff", path], tree, check=False)
    if check.returncode == 0:
        return False
    if check.returncode == 1 and path in check.stdout.splitlines():
        return True
    return None


def include_format_failures(base, path, files):
    included = set()
    for source in (path / "crates").rglob("*.rs"):
        if source.is_symlink():
            continue
        for name in re.findall(r'include!\s*\(\s*"([^"]+)"\s*\)', source.read_text(encoding="utf-8")):
            target = (source.parent / name).resolve()
            if target.is_relative_to(path):
                included.add(target.relative_to(path).as_posix())
    failures = []
    unformatted = []
    for file in files:
        if file not in included or not (path / file).is_file():
            continue
        needs_formatting = rustfmt_needs_formatting(path, file)
        if needs_formatting is None:
            failures.append(f"{file} failed standalone rustfmt.")
        elif needs_formatting:
            unformatted.append(file)
    if not unformatted:
        return failures

    # stdin check can exit successfully even when rustfmt would change the input.
    # Use the named file and formatter configuration from the frozen baseline.
    with detached_snapshot(base) as baseline:
        for file in unformatted:
            old = baseline / file
            if old.resolve() != old or not old.is_file() or not rustfmt_needs_formatting(baseline, file):
                failures.append(f"{file} failed standalone rustfmt.")
    return failures


@contextmanager
def detached_snapshot(head):
    with tempfile.TemporaryDirectory(prefix="verify-", dir=WORKTREES_DIR) as directory:
        path = Path(directory) / "tree"
        git("worktree", "add", "--detach", str(path), head)
        try:
            yield path
        finally:
            git("worktree", "remove", "--force", str(path))


def check_candidate(task, path, head):
    base = task["base_sha"]
    if head == base or not is_ancestor(base, head):
        return ["The candidate must advance the frozen base."], {}
    files = changed_files(base, head)
    checks = {"changed_files": files}
    try:
        validate_changes(task, files, path)
    except RuntimeError as exc:
        return [str(exc)], checks
    if not files:
        return ["The candidate contains no file changes."], checks
    failures = rust_diff_failures(base, head, path, files, task["id"])
    for file in files:
        entry = git("ls-tree", head, "--", file).stdout
        if entry.startswith(("120000 ", "160000 ")):
            failures.append(f"{file} introduces a symlink or submodule.")
    if failures:
        return failures, checks
    failures += include_format_failures(base, path, files)
    for name, command in (
        ("bash_syntax", ["bash", "-n", "scripts/run_core.sh"]),
        ("git_diff_check", ["git", "diff", "--check", base, head, "--"]),
        ("cargo_fmt", ["cargo", "fmt", "--all", "--", "--check"]),
        ("cargo_check", ["cargo", "check", "--workspace", "--locked"]),
        ("cargo_clippy", ["cargo", "clippy", "--workspace", "--all-targets", "--all-features", "--locked", "--", "-D", "warnings"]),
        ("cargo_test", ["cargo", "test", "--workspace", "--locked"]),
    ):
        if failures:
            break
        proc = run_cmd(command, path, check=False, timeout=900, env=cargo_env())
        checks[name] = proc.returncode
        if proc.returncode:
            failures.append(f"{name} exited {proc.returncode}: {(proc.stderr or proc.stdout)[-2400:]}")
    return failures, checks


def record_failure(db, task, kind, head, *, checks=None, summary=None):
    details = {"head_sha": head, "attempt": task["attempt"], "checks": checks, "summary": summary}
    event(db, kind, task["id"], **details)
    blocked = task["attempt"] >= MAX_ATTEMPTS
    db.execute(
        """UPDATE tasks SET status = ?, attempt = ?, attempt_base = ?, candidate_head = NULL,
           verified_head = NULL, checks = NULL, review_summary = NULL, last_failure = ?, blocked_reason = ?
           WHERE id = ?""",
        ("blocked" if blocked else "in_progress", task["attempt"] + (not blocked), head,
         json.dumps({"kind": kind, **details}, ensure_ascii=False),
         "circuit_breaker_2_rounds" if blocked else None, task["id"]),
    )
    if blocked:
        event(db, "task_blocked", task["id"], reason="circuit_breaker_2_rounds", attempt=task["attempt"])
    return "blocked" if blocked else "in_progress"


def cmd_verify(args):
    with process_lock("control"), open_db() as db:
        with transaction(db):
            task = worker_task(db, args)
        path = check_worktree(task)
        head = task["candidate_head"]
        if not head or git("rev-parse", "HEAD", cwd=path).stdout.strip() != head or pending_files(path):
            failures = ["The Worker did not leave a recorded candidate commit and a clean worktree."]
            checks = {}
            head = head or task["attempt_base"]
        else:
            with detached_snapshot(head) as snapshot:
                try:
                    failures, checks = check_candidate(task, snapshot, head)
                except subprocess.TimeoutExpired as exc:
                    failures = [f"The {exc.cmd[0]} verification command timed out after {exc.timeout:g}s."]
                    checks = {}
        with transaction(db):
            require_wave(db, args.wave, renew=True)
            if failures:
                status = record_failure(db, task, "gate_failed", head, checks=checks, summary=failures)
            else:
                status = "verified"
                db.execute(
                    "UPDATE tasks SET status = 'verified', verified_head = ?, checks = ? WHERE id = ?",
                    (head, json.dumps(checks), args.task),
                )
                event(db, "gate_passed", args.task, head_sha=head, attempt=task["attempt"])
    emit({"status": status, "verdict": "FAIL" if failures else "PASS", "head_sha": head, "checks": checks, "failures": failures})
    return 2 if failures else 0


def cmd_show_diff(args):
    with open_db(readonly=True) as db:
        require_wave(db, args.wave)
        task = task_row(db, args.task)
        if task["wave"] != args.wave or task["status"] != "verified":
            raise RuntimeError("Only the current verified candidate can be reviewed.")
    print(git("diff", "--no-ext-diff", "--no-textconv", task["base_sha"], task["verified_head"], "--").stdout, end="")
    return 0


def cmd_review(args):
    if not SHA.fullmatch(args.head) or not args.summary.strip():
        raise ValueError("A full reviewed commit SHA and a nonempty review are required.")
    with process_lock("control"), open_db() as db, transaction(db):
        task = owned_task(db, args.task, args.wave, ("verified",))
        if task["verified_head"] != args.head:
            raise RuntimeError("The independently reviewed SHA differs from the mechanically verified SHA.")
        if args.verdict == "REJECT":
            status = record_failure(db, task, "review_rejected", args.head, summary=args.summary)
        else:
            status = "reviewed"
            db.execute("UPDATE tasks SET status = 'reviewed', review_summary = ? WHERE id = ?", (args.summary, args.task))
            event(db, "review_passed", args.task, head_sha=args.head, attempt=task["attempt"])
    return emit({"status": status, "task": args.task, "head_sha": args.head})


def cmd_review_missing(args):
    with process_lock("control"), open_db() as db, transaction(db):
        task = owned_task(db, args.task, args.wave, ("verified",))
        if task["verified_head"] != args.head:
            raise RuntimeError("This attempt is no longer awaiting that review.")
        status = record_failure(db, task, "review_missing", args.head,
                                summary="Verifier finished without recording a verdict.")
    return emit({"status": status, "task": args.task, "head_sha": args.head})


def cmd_land(args):
    with process_lock("control"), open_db() as db:
        with transaction(db):
            require_wave(db, args.wave, renew=True)
            task = task_row(db, args.task)
            if task["status"] == "done" or (task["status"] == "blocked" and task["blocked_reason"] in ("awaiting_pr", "awaiting_merge")):
                return emit(task_view(db, task))
            owned_task(db, args.task, args.wave, ("reviewed", "landing"))
            head = task["verified_head"]
            base = task["base_sha"]
            if not head or not task["review_summary"] or not task["checks"]:
                raise RuntimeError("Landing requires both gate and review evidence for the candidate.")
            if db.execute("SELECT 1 FROM github_tasks WHERE task_id = ?", (args.task,)).fetchone():
                if task["status"] != "reviewed" or task["candidate_head"] != head or not is_ancestor(base, head):
                    raise RuntimeError("A GitHub task needs the reviewed candidate based on its frozen origin/main.")
                if any(code != 0 for name, code in json.loads(task["checks"]).items() if name != "changed_files"):
                    raise RuntimeError("A GitHub task must pass its recorded offline gates.")
                db.execute("UPDATE tasks SET status = 'blocked', blocked_reason = 'awaiting_pr', wave = NULL WHERE id = ?", (args.task,))
                event(db, "task_pr_queued", args.task, base_sha=base, head_sha=head)
                return emit(task_view(db, task_row(db, args.task)))
            if task["status"] == "reviewed":
                db.execute("UPDATE tasks SET status = 'landing' WHERE id = ?", (args.task,))
                event(db, "landing_intent", args.task, base_sha=base, head_sha=head)
        current = ref_commit(INTEGRATION_REF)
        if current == base:
            if not is_ancestor(base, head):
                raise RuntimeError("The candidate cannot fast-forward the frozen base.")
            if branch_checked_out(INTEGRATION_REF):
                raise RuntimeError("The integration branch is checked out in a worktree.")
            moved = git("update-ref", INTEGRATION_REF, head, base, check=False)
            current = head if moved.returncode == 0 else ref_commit(INTEGRATION_REF)
            if current == base:
                raise RuntimeError(f"Could not update the integration ref: {moved.stderr.strip()}")
        landed = current is not None and is_ancestor(head, current)
        with transaction(db):
            if landed:
                db.execute("UPDATE tasks SET status = 'done' WHERE id = ?", (args.task,))
                event(db, "task_landed", args.task, base_sha=base, head_sha=head)
            else:
                db.execute("UPDATE tasks SET status = 'blocked', blocked_reason = 'integration_advanced' WHERE id = ?", (args.task,))
                event(db, "task_blocked", args.task, reason="integration_advanced", base_sha=base, head_sha=head)
        if landed:
            cleanup_task_checkout(task)
        return emit(task_view(db, task_row(db, args.task)))


def cleanup_task_checkout(task):
    ref = f"refs/heads/{task_branch(task)}"
    head = task["verified_head"]
    if not head or ref_commit(ref) != head:
        return
    path = worktree_path(task)
    if path.is_dir():
        try:
            check_worktree(task)
            if git("rev-parse", "HEAD", cwd=path).stdout.strip() != head:
                raise RuntimeError("The task branch changed after landing.")
            if git("status", "--porcelain=v1", "--untracked-files=all", "--ignored",
                   "--ignore-submodules=none", cwd=path).stdout:
                raise RuntimeError("The worktree contains local files or changes.")
            git("worktree", "remove", str(path))
        except RuntimeError as exc:
            print(f"Retained task #{task['id']} worktree {path}: {exc}", file=sys.stderr)
            return
    if not branch_checked_out(ref):
        git("update-ref", "-d", ref, head)


def fetch_remote_main():
    git("fetch", "--no-tags", "--no-write-fetch-head", "origin", f"{MAIN_REF}:{REMOTE_MAIN_REF}")
    return git("rev-parse", "--verify", REMOTE_MAIN_REF).stdout.strip()


def pending_main_tasks(db):
    return db.execute(
        """SELECT t.* FROM tasks t JOIN events e ON e.task_id = t.id AND e.kind = 'task_landed'
           AND json_extract(e.details, '$.head_sha') = t.verified_head
           WHERE t.status = 'done' AND NOT EXISTS (SELECT 1 FROM github_tasks g WHERE g.task_id = t.id)
           AND NOT EXISTS (SELECT 1 FROM events p WHERE p.task_id = t.id AND p.kind = 'task_published'
                           AND json_extract(p.details, '$.head_sha') = t.verified_head)
           ORDER BY e.id"""
    ).fetchall()


def requeue_stale_main(db, remote):
    """Preserve and reschedule an unpublished integration chain after remote main advances."""
    integration = ref_commit(INTEGRATION_REF)
    main = ref_commit(MAIN_REF)
    if (not integration or not main or not is_ancestor(main, remote)
            or is_ancestor(integration, remote) or is_ancestor(remote, integration)
            or branch_checked_out(INTEGRATION_REF)):
        return 0
    if db.execute("SELECT 1 FROM tasks WHERE status IN ('in_progress', 'verified', 'reviewed', 'landing')").fetchone():
        return 0
    if db.execute(
        "SELECT 1 FROM events WHERE kind = 'direct_batch_superseded' AND json_extract(details, '$.head_sha') = ?",
        (integration,),
    ).fetchone():
        git("update-ref", "-d", INTEGRATION_REF, integration)
        return 0
    stale = [task for task in pending_main_tasks(db) if not is_ancestor(task["verified_head"], remote)]
    tip = git("merge-base", integration, remote).stdout.strip()
    for task in stale:
        if task["base_sha"] != tip or not is_ancestor(tip, task["verified_head"]):
            return 0
        tip = task["verified_head"]
    if not stale or tip != integration:
        return 0
    identity = db.execute("SELECT identity FROM loop_info").fetchone()[0]
    archive = f"refs/archive/akzio-loop/superseded/{identity}/{integration}"
    git("update-ref", archive, integration)
    with transaction(db):
        for task in stale:
            reset_for_design(db, task["id"])
            event(db, "task_requeued_after_main_advanced", task["id"], head_sha=task["verified_head"], base_sha=remote)
        event(db, "direct_batch_superseded", head_sha=integration, base_sha=remote)
    git("update-ref", "-d", INTEGRATION_REF, integration)
    return len(stale)


def cmd_deliver_main(_args):
    if not DB_PATH.exists():
        return emit({"status": "up_to_date"})
    with process_lock("control"), open_db() as db:
        if db.execute("SELECT 1 FROM wave WHERE expires_at > ?", (utc_timestamp(),)).fetchone():
            return emit({"status": "busy"})
        pending = pending_main_tasks(db)
        integration = ref_commit(INTEGRATION_REF)
        if not pending and not integration:
            return emit({"status": "up_to_date"})

        main = git("rev-parse", "--verify", MAIN_REF).stdout.strip()
        remote = fetch_remote_main()
        target = remote
        for task in pending:
            head = task["verified_head"]
            if is_ancestor(head, remote):
                continue
            if task["base_sha"] != target or not is_ancestor(target, head):
                raise RuntimeError("origin/main cannot reach the candidate through approved Loop tasks.")
            target = head

        if pending:
            head = pending[-1]["verified_head"]
            needs_merge = not is_ancestor(head, main)
            if needs_merge:
                if not is_ancestor(main, head):
                    raise RuntimeError("Local main diverged from the approved Loop commits.")
                checkout = git("symbolic-ref", "--quiet", "HEAD", check=False).stdout.strip()
                if checkout != MAIN_REF or git("status", "--porcelain=v1", "--untracked-files=all").stdout:
                    raise RuntimeError("Delivery requires main checked out in a clean primary worktree.")
            if target != remote:
                # Git rejects a concurrent origin update before it can strand the primary checkout.
                git("push", "--no-follow-tags", "--recurse-submodules=no", "origin", f"{target}:{MAIN_REF}")
                remote = fetch_remote_main()
            if needs_merge:
                if not is_ancestor(head, remote):
                    raise RuntimeError("Origin did not retain the approved commit.")
                git("merge", "--ff-only", "--no-autostash", "--no-overwrite-ignore", head)
                main = git("rev-parse", "--verify", MAIN_REF).stdout.strip()
            for task in pending:
                head = task["verified_head"]
                if not is_ancestor(head, main) or not is_ancestor(head, remote):
                    raise RuntimeError("An approved commit is missing from local main or origin/main.")
                cleanup_task_checkout(task)
            with transaction(db):
                for task in pending:
                    event(db, "task_published", task["id"], head_sha=task["verified_head"])

        active = db.execute(
            "SELECT 1 FROM tasks WHERE status IN ('in_progress', 'verified', 'reviewed', 'landing')"
        ).fetchone()
        if (integration and not active and is_ancestor(integration, main)
                and is_ancestor(integration, remote) and not branch_checked_out(INTEGRATION_REF)):
            git("update-ref", "-d", INTEGRATION_REF, integration)
    if pending:
        return emit({"status": "published", "head_sha": pending[-1]["verified_head"], "tasks": len(pending)})
    return emit({"status": "up_to_date"})


def cmd_daily_report(args):
    with process_lock("report"), open_db() as db, transaction(db):
        now = utc_timestamp()
        if args.if_due:
            since = db.execute("SELECT max(created_at) FROM events WHERE kind = 'daily_report'").fetchone()[0]
            if since is None:
                since = db.execute("SELECT created_at FROM loop_info").fetchone()[0]
            if now - since < 24 * 3600:
                return emit({"status": "not_due"})
        rows = db.execute("SELECT * FROM events WHERE created_at >= ? ORDER BY id", (now - 24 * 3600,)).fetchall()
        counts = Counter(row["kind"] for row in rows)
        date = iso_time(now)[:10]
        lines = [f"# Akzio Loop 日报 {date}", "", f"截至 `{iso_time(now)}` 的过去 24 小时。", ""]
        lines.extend(f"- `{kind}` × {count}" for kind, count in sorted(counts.items()))
        for row in rows:
            if row["kind"] == "task_landed":
                details = json.loads(row["details"])
                lines.append(f"- Task #{row['task_id']} → `{details['head_sha']}` (`offline-verified`)")
            if row["kind"] == "task_published":
                details = json.loads(row["details"])
                lines.append(f"- Task #{row['task_id']} → `origin/main` · `{details['head_sha']}`")
            if row["kind"] == "task_blocked":
                details = json.loads(row["details"])
                lines.append(f"- Task #{row['task_id']} · `{details['reason']}`")
        path = REPORTS_DIR / f"daily-{date}.md"
        with tempfile.NamedTemporaryFile(mode="w", dir=REPORTS_DIR, encoding="utf-8", delete=False) as report:
            report.write("\n".join(lines) + "\n")
        os.replace(report.name, path)
        event(db, "daily_report", landed=counts["task_landed"], blocked=counts["task_blocked"])
    return emit({"status": "generated", "path": str(path), "counts": dict(counts)})


def audit_marker(identity, event_id):
    return f"<!-- akzio-loop-event:{identity}:{event_id} -->"


def audit_line(row):
    data = json.loads(row["details"])
    fields = ("priority", "attempt", "base_sha", "head_sha", "reason", "landed", "blocked",
              "decision_id", "choice", "next_stage", "issue", "pr", "merge_sha", "ci_run", "ci_attempt")
    details = " · ".join(f"{key}=`{data[key]}`" for key in fields if key in data)
    task = f" · Task #{row['task_id']}" if row["task_id"] else ""
    return f"- `{iso_time(row['created_at'])}` · `{row['kind']}`{task}" + (f" · {details}" if details else "")


def cmd_publish_log(args):
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", args.repo) or args.issue <= 0:
        raise ValueError("The audit destination requires OWNER/REPO and a positive Issue number.")
    destination = f"{args.repo.lower()}#{args.issue}"
    endpoint = f"repos/{args.repo}/issues/{args.issue}/comments"
    with process_lock("publish"), open_db() as db:
        identity = db.execute("SELECT identity FROM loop_info").fetchone()[0]
        rows = db.execute(
            """SELECT e.* FROM events e
            WHERE coalesce(json_extract(e.details, '$.visibility'), 'github') <> 'local' AND NOT EXISTS (
                SELECT 1 FROM deliveries d WHERE d.destination = ? AND d.event_id = e.id
            ) ORDER BY e.id""", (destination,),
        ).fetchall()
        if not rows:
            return emit({"status": "up_to_date", "events": 0})
        actor = run_cmd(["gh", "api", "user"], timeout=15)
        actor_id = json.loads(actor.stdout)["id"]
        comments = run_cmd(["gh", "api", "--paginate", "--slurp", f"{endpoint}?per_page=100"], timeout=30)
        observed = {}
        for page in json.loads(comments.stdout):
            for comment in page:
                if (comment.get("user") or {}).get("id") == actor_id:
                    for marker in re.findall(r"<!-- akzio-loop-event:[0-9a-f]{32}:\d+ -->", comment.get("body") or ""):
                        observed[marker] = comment["id"]
        new_events = 0
        for offset in range(0, len(rows), 25):
            receipts = {}
            pending = []
            for row in rows[offset:offset + 25]:
                comment_id = observed.get(audit_marker(identity, row["id"]))
                if comment_id is None:
                    pending.append(row)
                else:
                    receipts[row["id"]] = comment_id
            if pending:
                body = "## Akzio Loop 审计日志\n\n" + "\n".join(
                    f"{audit_line(row)}\n{audit_marker(identity, row['id'])}" for row in pending
                )
                posted = run_cmd(["gh", "api", "--method", "POST", endpoint, "--input", "-"], timeout=30,
                                 input_text=json.dumps({"body": body}, ensure_ascii=False))
                comment_id = json.loads(posted.stdout)["id"]
                receipts.update((row["id"], comment_id) for row in pending)
                new_events += len(pending)
            with transaction(db):
                db.executemany(
                    "INSERT OR IGNORE INTO deliveries (destination, event_id, comment_id) VALUES (?, ?, ?)",
                    [(destination, event_id, comment_id) for event_id, comment_id in receipts.items()],
                )
    return emit({"status": "published", "events": len(rows), "new_events": new_events})


def cmd_init(args):
    with open_db():
        pass
    return cmd_poll(args)


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    sub = parser.add_subparsers(dest="cmd", required=True)

    def command(name, func, *, task=False, wave=False):
        child = sub.add_parser(name, allow_abbrev=False)
        child.set_defaults(func=func)
        if task:
            child.add_argument("--task", type=int, required=True)
        if wave:
            child.add_argument("--wave", required=True)
        return child

    command("init", cmd_init)
    command("start-wave", cmd_start_wave)
    finish = command("finish-wave", cmd_finish_wave, wave=True)
    finish.add_argument("--expect-task", type=int)
    finish.add_argument("--after-event", type=int)
    command("poll", cmd_poll)
    command("show", cmd_show, task=True)
    command("list", cmd_list).add_argument("--status", choices=("inbox", "ready", *RESUMABLE, *WAITING, "done", "blocked"))
    propose = command("propose", cmd_propose)
    for option in ("title", "area", "fingerprint", "candidate-files", "body-file"):
        propose.add_argument(f"--{option}", required=True)
    propose.add_argument("--priority", choices=PRIORITIES, default="P2")
    propose.add_argument("--dry-run", action="store_true")
    design = command("post-design", cmd_post_design, task=True, wave=True)
    design.add_argument("--allowed-files", required=True)
    design.add_argument("--design-file", required=True)
    command("claim", cmd_claim, task=True, wave=True)
    test = command("worker-test", cmd_worker_test, task=True, wave=True)
    test.add_argument("--attempt", type=int, required=True)
    test.add_argument("--crate", required=True)
    test.add_argument("--filter")
    commit = command("worker-commit", cmd_worker_commit, task=True, wave=True)
    commit.add_argument("--attempt", type=int, required=True)
    commit.add_argument("--message", required=True)
    command("verify-worktree", cmd_verify, task=True, wave=True).add_argument("--attempt", type=int, required=True)
    command("show-diff", cmd_show_diff, task=True, wave=True)
    review = command("review", cmd_review, task=True, wave=True)
    review.add_argument("--head", required=True)
    review.add_argument("--verdict", choices=("PASS", "REJECT"), required=True)
    review.add_argument("--summary", required=True)
    command("review-missing", cmd_review_missing, task=True, wave=True).add_argument("--head", required=True)
    command("worktree-land", cmd_land, task=True, wave=True)
    command("deliver-main", cmd_deliver_main)
    block = command("block", cmd_block, task=True, wave=True)
    block.add_argument("--reason-code", choices=DECISION_REASONS, required=True)
    block.add_argument("--report-file", required=True)
    decision = command("request-decision", cmd_request_decision, task=True, wave=True)
    decision.add_argument("--reason-code", choices=DECISION_REASONS, required=True)
    decision.add_argument("--visibility", choices=("local", "github"), default="local")
    decision.add_argument("--report-file", required=True)
    resolve = command("resolve-decision", cmd_resolve_decision)
    resolve.add_argument("--id", required=True)
    resolve.add_argument("--choice", choices=tuple("ABCDE"), required=True)
    command("daily-report", cmd_daily_report).add_argument("--if-due", action="store_true")
    audit = command("publish-log", cmd_publish_log)
    audit.add_argument("--repo", required=True)
    audit.add_argument("--issue", type=int, required=True)
    return parser


def main():
    args = build_parser().parse_args()
    try:
        return args.func(args)
    except (RuntimeError, ValueError, KeyError, OSError, sqlite3.Error, subprocess.TimeoutExpired) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
