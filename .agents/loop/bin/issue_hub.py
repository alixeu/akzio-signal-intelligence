#!/usr/bin/env python3
"""
Akzio 24h Autonomous Agent Loop — GitHub Issues Hub & Backpressure Engine.

Responsibilities:
1. GitHub Issues state-machine routing (`loop:inbox` -> `loop:ready` -> `loop:in-progress` -> `loop:done` / `loop:blocked`).
2. Issue proposing (`propose`) with fingerprint deduplication and file-existence anti-hallucination checks.
3. Stage 1 Design contract publishing (`post-design`) with machine-readable `allowed_files` metadata.
4. Isolated Git Worktree lifecycle (`worktree-create`, `verify-worktree`, `worktree-land`, `worktree-discard`)
   that NEVER touches uncommitted files in the developer's main working directory.
5. Deterministic Agent Backpressure (`verify-worktree`): whitelist check, anti-reward-hacking diff scan,
   `include!` fragment `rustfmt` check, and Cargo workspace gates (`fmt`, `check`, `clippy -D warnings`, `test`).
6. Stage 3 Completion (`complete`), 2-Round Circuit Breaker (`block`), and 24h Daily Digest (`daily-report`).
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[3]
SHARED_CARGO_TARGET_DIR = REPO_ROOT / "target"
OUTPUT_ROOT = REPO_ROOT / "outputs" / "loop"
STATE_DIR = OUTPUT_ROOT / "state"
WORKTREES_DIR = OUTPUT_ROOT / "worktrees"
REPORTS_DIR = OUTPUT_ROOT / "reports"
TMP_DIR = OUTPUT_ROOT / "tmp"
LEDGER_PATH = STATE_DIR / "ledger.jsonl"
QUEUE_CACHE_PATH = STATE_DIR / "queue_cache.json"
LESSONS_PATH = REPO_ROOT / ".agents" / "loop" / "agent-lessons.md"

INTEGRATION_BRANCH = "loop/nightly"
LEASE_TIMEOUT_MINUTES = 90
DAILY_REPORT_INTERVAL_HOURS = 24
DAILY_REPORT_ISSUE_TITLE = "📊 [Akzio Loop] 24h 无人值守研发日报汇总"

LOOP_LABELS = [
    ("loop:inbox", "0E8A16", "Proposed issue awaiting Leader Stage-1 [LOOP-DESIGN] contract"),
    ("loop:ready", "1D76DB", "Stage-1 [LOOP-DESIGN] approved; ready for isolated Flash Worker"),
    ("loop:in-progress", "FBCA04", "Currently claimed by an active Leader/Worker wave"),
    ("loop:done", "5319E7", "Verified by Cargo gates + Fresh-Context Verifier and landed on loop/nightly"),
    ("loop:blocked", "D93F0B", "Circuit breaker tripped or awaiting human product/security decision"),
    ("loop:daily-report", "0052CC", "Pinned tracking issue for 24h autonomous loop daily reports"),
    ("P1", "B60205", "Priority 1: Execution safety / reconciliation / critical invariant"),
    ("P2", "D93F0B", "Priority 2: Engineering risk / test gap / contract alignment"),
    ("P3", "0E8A16", "Priority 3: Low-risk hardening / documentation / cleanup"),
]

FORBIDDEN_PATH_PATTERNS = [
    r"(^|/)\.akzio(/|$)",
    r"^~",
    r"(^|/)\.env($|\.)",
]

BANNED_DIFF_PATTERNS = [
    (r"#\s*\[\s*allow\s*\(", "Adding #[allow(...)] is forbidden (Reward-Hacking rule)"),
    (r"#\s*!\s*\[\s*allow\s*\(", "Adding #![allow(...)] is forbidden (Reward-Hacking rule)"),
    (r"#\s*\[\s*ignore\b", "Adding #[ignore] to skip tests is forbidden (Reward-Hacking rule)"),
]


def ensure_dirs() -> None:
    for d in (STATE_DIR, WORKTREES_DIR, REPORTS_DIR, TMP_DIR):
        d.mkdir(parents=True, exist_ok=True)


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_now() -> str:
    return utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")


def append_ledger(event_type: str, payload: Dict[str, Any]) -> None:
    ensure_dirs()
    record = {
        "timestamp": iso_now(),
        "event": event_type,
        **payload,
    }
    with LEDGER_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def read_ledger() -> List[Dict[str, Any]]:
    if not LEDGER_PATH.exists():
        return []
    records: List[Dict[str, Any]] = []
    for line in LEDGER_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records


def notify_human(subject: str, body: str, metadata: Optional[Dict[str, Any]] = None) -> None:
    """
    Reserved hook for notifying the human operator when `loop:blocked` or `daily-report` fires.
    Currently writes to `outputs/loop/state/notifications.jsonl` and optionally posts to
    `AKZIO_LOOP_WEBHOOK_URL` if configured later by the user.
    """
    ensure_dirs()
    notif_file = STATE_DIR / "notifications.jsonl"
    entry = {
        "timestamp": iso_now(),
        "subject": subject,
        "body": body,
        "metadata": metadata or {},
    }
    with notif_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def run_cmd(
    cmd: List[str],
    cwd: Optional[Path] = None,
    check: bool = True,
    timeout: int = 600,
    env: Optional[Dict[str, str]] = None,
) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        cmd,
        cwd=str(cwd or REPO_ROOT),
        text=True,
        capture_output=True,
        timeout=timeout,
        env=env,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"Command failed (exit {proc.returncode}): {' '.join(cmd)}\n"
            f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
    return proc


def gh_json(args: List[str]) -> Any:
    proc = run_cmd(["gh", *args], check=True)
    out = proc.stdout.strip()
    return json.loads(out) if out else None


def get_dirty_main_files() -> Dict[str, List[str]]:
    """Inspect uncommitted/untracked files in the main working tree without modifying anything."""
    proc = run_cmd(["git", "status", "--porcelain"], cwd=REPO_ROOT, check=False)
    modified: List[str] = []
    untracked: List[str] = []
    for raw in proc.stdout.splitlines():
        if len(raw) < 4:
            continue
        code = raw[:2]
        path = raw[3:].strip()
        if path.startswith("outputs/"):
            continue
        if code == "??":
            untracked.append(path)
        else:
            modified.append(path)
    return {"modified": modified, "untracked": untracked}


def sync_queue_cache(
    delta_ready: int = 0,
    delta_inbox: int = 0,
    daily_report_due: Optional[bool] = None,
) -> None:
    """Keep `queue_cache.json` synchronized after transitions so `Stop` hook never reads stale counts."""
    if not QUEUE_CACHE_PATH.exists():
        return
    try:
        cache = json.loads(QUEUE_CACHE_PATH.read_text(encoding="utf-8"))
        counts = cache.setdefault("counts", {})
        counts["ready"] = max(0, int(counts.get("ready") or 0) + delta_ready)
        counts["inbox"] = max(0, int(counts.get("inbox") or 0) + delta_inbox)
        if daily_report_due is not None:
            cache["daily_report_due"] = daily_report_due
        cache["stale"] = False
        QUEUE_CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def set_issue_labels(issue_num: int, add_labels: List[str], remove_labels: List[str]) -> None:
    """Safely update GitHub issue labels so missing remove-labels never abort adding the target label."""
    issue_info = gh_json(["issue", "view", str(issue_num), "--json", "labels"]) or {}
    current = {lbl.get("name", "") for lbl in issue_info.get("labels", [])}
    to_remove = [lbl for lbl in remove_labels if lbl in current]
    to_add = [lbl for lbl in add_labels if lbl not in current]
    if not to_remove and not to_add:
        return
    cmd = ["gh", "issue", "edit", str(issue_num)]
    if to_remove:
        cmd.extend(["--remove-label", ",".join(to_remove)])
    if to_add:
        cmd.extend(["--add-label", ",".join(to_add)])
    run_cmd(cmd, check=True)


def parse_design_meta(comments: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    pattern = re.compile(r"<!--\s*loop-design-meta:\s*(\{.*?\})\s*-->", re.DOTALL)
    for comment in reversed(comments or []):
        body = comment.get("body") or ""
        m = pattern.search(body)
        if m:
            try:
                return json.loads(m.group(1))
            except json.JSONDecodeError:
                continue
    return None


def priority_rank(labels: List[str]) -> int:
    if "P1" in labels:
        return 1
    if "P2" in labels:
        return 2
    if "P3" in labels:
        return 3
    return 4


def is_safe_repo_path(rel_path: str) -> bool:
    rel_path = rel_path.strip()
    if not rel_path or rel_path.startswith("/"):
        return False
    for pat in FORBIDDEN_PATH_PATTERNS:
        if re.search(pat, rel_path):
            return False
    resolved = (REPO_ROOT / rel_path).resolve()
    try:
        resolved.relative_to(REPO_ROOT.resolve())
    except ValueError:
        return False
    return True


def cmd_init_labels(args: argparse.Namespace) -> int:
    created = []
    for name, color, desc in LOOP_LABELS:
        if args.dry_run:
            created.append({"name": name, "color": color, "description": desc, "dry_run": True})
            continue
        run_cmd(["gh", "label", "create", name, "--color", color, "--description", desc, "--force"])
        created.append({"name": name, "color": color})
    print(json.dumps({"status": "ok", "labels": created}, ensure_ascii=False, indent=2))
    return 0


def cmd_propose(args: argparse.Namespace) -> int:
    ensure_dirs()
    fingerprint = args.fingerprint.strip()
    if not re.match(r"^[a-z0-9][a-z0-9\-_]{3,80}$", fingerprint):
        raise ValueError("Invalid --fingerprint: use lowercase alphanumeric/kebab-case (4..80 chars).")

    candidate_files = [f.strip() for f in args.candidate_files.split(",") if f.strip()]
    if not candidate_files:
        raise ValueError("--candidate-files must specify at least one existing repository file.")

    for fpath in candidate_files:
        if not is_safe_repo_path(fpath):
            raise ValueError(f"Unsafe or forbidden path in --candidate-files: {fpath}")
        if not (REPO_ROOT / fpath).exists():
            raise ValueError(
                f"Anti-Hallucination Guard: candidate file '{fpath}' does not exist in {REPO_ROOT}."
            )

    body_text = Path(args.body_file).read_text(encoding="utf-8").strip()
    marker = f"<!-- loop-fingerprint: {fingerprint} -->"

    if not args.dry_run:
        # 1. Exact local ledger deduplication (guards against GitHub search index lag)
        for rec in read_ledger():
            if rec.get("event") == "issue_proposed" and rec.get("fingerprint") == fingerprint:
                print(
                    json.dumps(
                        {
                            "status": "skipped_duplicate",
                            "source": "local_ledger",
                            "fingerprint": fingerprint,
                            "existing_issue": {
                                "number": rec.get("issue"),
                                "title": rec.get("title"),
                                "url": rec.get("url"),
                            },
                        },
                        ensure_ascii=False,
                        indent=2,
                    )
                )
                return 0

        # 2. Exact GitHub issue body marker check across recent open+closed issues
        recent_issues = (
            gh_json(
                [
                    "issue",
                    "list",
                    "--state",
                    "all",
                    "--limit",
                    "200",
                    "--json",
                    "number,title,state,url,body",
                ]
            )
            or []
        )
        exact_matches = [
            {k: it.get(k) for k in ("number", "title", "state", "url")}
            for it in recent_issues
            if marker in (it.get("body") or "")
        ]
        if not exact_matches:
            search_hits = (
                gh_json(
                    [
                        "issue",
                        "list",
                        "--state",
                        "all",
                        "--search",
                        f'"{fingerprint}" in:body',
                        "--json",
                        "number,title,state,url,body",
                    ]
                )
                or []
            )
            exact_matches = [
                {k: it.get(k) for k in ("number", "title", "state", "url")}
                for it in search_hits
                if marker in (it.get("body") or "")
            ]
        if exact_matches:
            print(
                json.dumps(
                    {
                        "status": "skipped_duplicate",
                        "source": "github",
                        "fingerprint": fingerprint,
                        "existing_issue": exact_matches[0],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0

    meta_header = (
        f"{marker}\n"
        f"> **Loop Metadata**: `priority={args.priority}` · `area={args.area}` · "
        f"`candidate_files={', '.join(candidate_files)}`\n\n"
    )
    full_body = meta_header + body_text

    if args.dry_run:
        print(
            json.dumps(
                {
                    "status": "dry_run_valid",
                    "title": args.title,
                    "priority": args.priority,
                    "area": args.area,
                    "fingerprint": fingerprint,
                    "candidate_files": candidate_files,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    labels = ["loop:inbox", args.priority]
    proc = run_cmd(
        [
            "gh",
            "issue",
            "create",
            "--title",
            args.title,
            "--body",
            full_body,
            "--label",
            ",".join(labels),
        ]
    )
    issue_url = proc.stdout.strip().splitlines()[-1]
    issue_num_match = re.search(r"/issues/(\d+)$", issue_url)
    issue_num = int(issue_num_match.group(1)) if issue_num_match else None

    append_ledger(
        "issue_proposed",
        {
            "issue": issue_num,
            "title": args.title,
            "priority": args.priority,
            "area": args.area,
            "fingerprint": fingerprint,
            "candidate_files": candidate_files,
            "url": issue_url,
        },
    )
    sync_queue_cache(delta_inbox=1)
    print(
        json.dumps(
            {
                "status": "created",
                "issue": issue_num,
                "url": issue_url,
                "fingerprint": fingerprint,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def check_daily_report_due(records: List[Dict[str, Any]]) -> bool:
    for rec in reversed(records):
        if rec.get("event") == "daily_report_generated":
            ts = rec.get("timestamp")
            if ts:
                try:
                    last_dt = dt.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(
                        tzinfo=dt.timezone.utc
                    )
                    return (utc_now() - last_dt).total_seconds() >= DAILY_REPORT_INTERVAL_HOURS * 3600
                except ValueError:
                    pass
    for rec in records:
        ts = rec.get("timestamp")
        if ts:
            try:
                first_dt = dt.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=dt.timezone.utc
                )
                return (utc_now() - first_dt).total_seconds() >= DAILY_REPORT_INTERVAL_HOURS * 3600
            except ValueError:
                continue
    return False


def cmd_poll(args: argparse.Namespace) -> int:
    ensure_dirs()
    records = read_ledger()

    if args.offline:
        issues = []
    else:
        issues = (
            gh_json(
                [
                    "issue",
                    "list",
                    "--state",
                    "open",
                    "--limit",
                    "50",
                    "--json",
                    "number,title,labels,body,updatedAt,comments,url",
                ]
            )
            or []
        )

    ready_list: List[Dict[str, Any]] = []
    inbox_list: List[Dict[str, Any]] = []
    in_progress_list: List[Dict[str, Any]] = []
    blocked_list: List[Dict[str, Any]] = []

    for item in issues:
        label_names = [lbl.get("name", "") for lbl in item.get("labels", [])]
        if "loop:daily-report" in label_names or item.get("title") == DAILY_REPORT_ISSUE_TITLE:
            continue

        design_meta = parse_design_meta(item.get("comments", []))
        summary = {
            "number": item["number"],
            "title": item["title"],
            "labels": label_names,
            "priority_rank": priority_rank(label_names),
            "updated_at": item.get("updatedAt"),
            "url": item.get("url"),
            "design_meta": design_meta,
        }

        if "loop:blocked" in label_names:
            blocked_list.append(summary)
        elif "loop:in-progress" in label_names:
            # Check if stale (> LEASE_TIMEOUT_MINUTES)
            updated_str = item.get("updatedAt")
            is_stale = False
            if updated_str:
                try:
                    upd_dt = dt.datetime.strptime(updated_str, "%Y-%m-%dT%H:%M:%SZ").replace(
                        tzinfo=dt.timezone.utc
                    )
                    is_stale = (utc_now() - upd_dt).total_seconds() > LEASE_TIMEOUT_MINUTES * 60
                except ValueError:
                    pass
            summary["stale_lease"] = is_stale
            if is_stale and design_meta:
                ready_list.append(summary)
            else:
                in_progress_list.append(summary)
        elif "loop:ready" in label_names and design_meta:
            ready_list.append(summary)
        else:
            inbox_list.append(summary)

    ready_list.sort(key=lambda x: (x["priority_rank"], x["number"]))
    inbox_list.sort(key=lambda x: (x["priority_rank"], x["number"]))

    if ready_list:
        recommended_action = "execute_ready"
        target_issue = ready_list[0]
    elif inbox_list:
        recommended_action = "design_inbox"
        target_issue = inbox_list[0]
    else:
        recommended_action = "scout_needed"
        target_issue = None

    dirty_main = get_dirty_main_files()
    result = {
        "timestamp": iso_now(),
        "recommended_action": recommended_action,
        "target_issue": target_issue,
        "counts": {
            "ready": len(ready_list),
            "inbox": len(inbox_list),
            "in_progress": len(in_progress_list),
            "blocked": len(blocked_list),
        },
        "ready_queue": ready_list,
        "inbox_queue": inbox_list,
        "blocked_queue": blocked_list,
        "dirty_main_files": dirty_main,
        "daily_report_due": check_daily_report_due(records),
    }
    QUEUE_CACHE_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def cmd_post_design(args: argparse.Namespace) -> int:
    ensure_dirs()
    allowed_files = [f.strip() for f in args.allowed_files.split(",") if f.strip()]
    if args.verdict == "ready":
        if not allowed_files:
            raise ValueError("--allowed-files is required when --verdict=ready")
        for fpath in allowed_files:
            if not is_safe_repo_path(fpath):
                raise ValueError(f"Forbidden or unsafe path in --allowed-files: {fpath}")
            target = REPO_ROOT / fpath
            if not target.exists() and not target.parent.exists():
                raise ValueError(f"Path '{fpath}' (and parent dir) does not exist in repository.")

    design_body = Path(args.design_file).read_text(encoding="utf-8").strip()
    meta = {
        "issue": args.issue,
        "verdict": args.verdict,
        "allowed_files": allowed_files,
        "designed_at": iso_now(),
    }
    comment_body = (
        f"## 📐 `[LOOP-DESIGN]` Pre-Implementation Contract\n\n"
        f"<!-- loop-design-meta: {json.dumps(meta, ensure_ascii=False)} -->\n\n"
        f"- **Verdict**: `{args.verdict.upper()}`\n"
        f"- **Allowed Files Whitelist (`Seams`)**: {', '.join(f'`{p}`' for p in allowed_files) if allowed_files else 'N/A'}\n\n"
        f"---\n\n"
        f"{design_body}\n"
    )

    if not args.dry_run:
        run_cmd(["gh", "issue", "comment", str(args.issue), "--body", comment_body])
        new_label = "loop:ready" if args.verdict == "ready" else "loop:blocked"
        set_issue_labels(
            args.issue,
            add_labels=[new_label],
            remove_labels=["loop:inbox", "loop:in-progress"],
        )

    append_ledger(
        "issue_designed",
        {
            "issue": args.issue,
            "verdict": args.verdict,
            "allowed_files": allowed_files,
        },
    )
    if not args.dry_run:
        sync_queue_cache(delta_inbox=-1, delta_ready=(1 if args.verdict == "ready" else 0))
    print(json.dumps({"status": "ok", **meta}, ensure_ascii=False, indent=2))
    return 0


def cmd_claim(args: argparse.Namespace) -> int:
    ensure_dirs()
    if not args.dry_run:
        set_issue_labels(
            args.issue,
            add_labels=["loop:in-progress"],
            remove_labels=["loop:ready", "loop:inbox"],
        )
        sync_queue_cache(delta_ready=-1)
    append_ledger("issue_claimed", {"issue": args.issue})
    print(json.dumps({"status": "claimed", "issue": args.issue, "timestamp": iso_now()}, indent=2))
    return 0


def ensure_integration_branch() -> str:
    proc = run_cmd(["git", "rev-parse", "--verify", INTEGRATION_BRANCH], check=False)
    if proc.returncode != 0:
        head_sha = run_cmd(["git", "rev-parse", "HEAD"]).stdout.strip()
        run_cmd(["git", "branch", INTEGRATION_BRANCH, head_sha])
    return run_cmd(["git", "rev-parse", INTEGRATION_BRANCH]).stdout.strip()


def cmd_worktree_create(args: argparse.Namespace) -> int:
    ensure_dirs()
    base_sha = ensure_integration_branch()
    wt_path = WORKTREES_DIR / f"issue-{args.issue}"
    branch_name = f"loop/issue-{args.issue}"

    if wt_path.exists():
        run_cmd(["git", "worktree", "remove", "--force", str(wt_path)], check=False)
        if wt_path.exists():
            shutil.rmtree(wt_path)

    run_cmd(["git", "branch", "-D", branch_name], check=False)
    run_cmd(["git", "worktree", "add", "-b", branch_name, str(wt_path), INTEGRATION_BRANCH])

    append_ledger(
        "worktree_created",
        {
            "issue": args.issue,
            "worktree": str(wt_path),
            "branch": branch_name,
            "base_branch": INTEGRATION_BRANCH,
            "base_sha": base_sha,
        },
    )
    print(
        json.dumps(
            {
                "status": "ok",
                "issue": args.issue,
                "worktree_path": str(wt_path),
                "branch": branch_name,
                "base_branch": INTEGRATION_BRANCH,
                "base_sha": base_sha,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def load_allowed_files_for_issue(issue_num: int, explicit_csv: Optional[str]) -> List[str]:
    if explicit_csv:
        return [f.strip() for f in explicit_csv.split(",") if f.strip()]
    # Fallback 1: check ledger
    for rec in reversed(read_ledger()):
        if rec.get("event") == "issue_designed" and rec.get("issue") == issue_num:
            files = rec.get("allowed_files") or []
            if files:
                return files
    # Fallback 2: query GitHub issue comments
    issue_data = gh_json(["issue", "view", str(issue_num), "--json", "comments"])
    if issue_data:
        meta = parse_design_meta(issue_data.get("comments", []))
        if meta and meta.get("allowed_files"):
            return meta["allowed_files"]
    raise RuntimeError(
        f"Could not find [LOOP-DESIGN] allowed_files for issue #{issue_num}. Run post-design first."
    )


def cmd_verify_worktree(args: argparse.Namespace) -> int:
    ensure_dirs()
    wt_path = Path(args.worktree) if args.worktree else (WORKTREES_DIR / f"issue-{args.issue}")
    if not wt_path.exists():
        raise RuntimeError(f"Worktree does not exist: {wt_path}")

    allowed_files = load_allowed_files_for_issue(args.issue, args.allowed_files)
    base_ref = args.base_ref or INTEGRATION_BRANCH

    base_sha = run_cmd(["git", "rev-parse", base_ref], cwd=wt_path).stdout.strip()
    head_sha = run_cmd(["git", "rev-parse", "HEAD"], cwd=wt_path).stdout.strip()

    failures: List[str] = []
    checks: Dict[str, Any] = {}

    # 1. Check that Worker actually created a commit and working tree is clean
    status_out = run_cmd(["git", "status", "--porcelain"], cwd=wt_path).stdout.strip()
    if status_out:
        failures.append(f"Worktree has uncommitted changes:\n{status_out}")
    if head_sha == base_sha:
        failures.append(f"No new commit on branch (HEAD {head_sha} == {base_ref}).")

    # 2. Check both source and destination paths of moved files against the whitelist.
    changed_paths = run_cmd(
        ["git", "diff", "--name-only", "--no-renames", "-z", f"{base_ref}...HEAD"],
        cwd=wt_path,
    ).stdout
    changed_files = [path for path in changed_paths.split("\0") if path]
    checks["changed_files"] = changed_files
    checks["allowed_files"] = allowed_files

    unauthorized = [f for f in changed_files if f not in allowed_files]
    if unauthorized:
        failures.append(
            f"Modified files outside [LOOP-DESIGN] whitelist: {unauthorized} (allowed: {allowed_files})"
        )

    # 3. Anti-Reward-Hacking diff scan (scoped strictly to `.rs` files)
    diff_text = run_cmd(["git", "diff", f"{base_ref}...HEAD"], cwd=wt_path).stdout
    current_file = ""
    new_line_no = 0
    added_rs_lines: List[str] = []
    added_test_locations: List[Tuple[str, int]] = []

    for raw_line in diff_text.splitlines():
        if raw_line.startswith("diff --git "):
            current_file = ""
            new_line_no = 0
            continue
        if raw_line.startswith("+++ b/"):
            current_file = raw_line[6:].strip()
            continue
        if raw_line.startswith("+++ /dev/null"):
            current_file = ""
            continue
        m_hunk = re.match(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@", raw_line)
        if m_hunk:
            new_line_no = int(m_hunk.group(1))
            continue
        if not current_file.endswith(".rs"):
            continue
        if raw_line.startswith("+") and not raw_line.startswith("+++"):
            content = raw_line[1:]
            added_rs_lines.append(content)
            if re.search(r"#\s*\[\s*(?:tokio::)?test\b", content):
                added_test_locations.append((current_file, new_line_no))
            new_line_no += 1
        elif not raw_line.startswith("-"):
            new_line_no += 1

    added_rs_blob = "\n".join(added_rs_lines)
    for pattern, reason in BANNED_DIFF_PATTERNS:
        if re.search(pattern, added_rs_blob):
            failures.append(reason)

    # Check EVERY added #[test] / #[tokio::test] in .rs files has a WHY THIS TEST EXISTS comment attached to it
    for test_file, test_line in added_test_locations:
        file_abs = wt_path / test_file
        if not file_abs.is_file():
            continue
        file_lines = file_abs.read_text(encoding="utf-8", errors="replace").splitlines()
        idx = test_line - 1
        attached: List[str] = []
        # Walk upwards across contiguous comment / attribute lines (stop at blank line or code like `}` / `;`)
        up = idx - 1
        while up >= 0:
            s = file_lines[up].strip()
            if s.startswith("//") or s.startswith("#["):
                attached.append(file_lines[up])
                up -= 1
            else:
                break
        # Also walk downwards until the `fn ` declaration in case doc comment sits between #[test] and fn
        down = idx + 1
        while down < len(file_lines):
            s = file_lines[down].strip()
            if s.startswith("//") or s.startswith("#["):
                attached.append(file_lines[down])
                down += 1
            else:
                break
        if "WHY THIS TEST EXISTS" not in "\n".join(attached):
            failures.append(
                f"New #[test] at {test_file}:{test_line} lacks mandatory '/// WHY THIS TEST EXISTS (Issue #<N>): ...' docstring."
            )

    # 4. Check ALL `include!` fragment formatting across all crates (Lesson 002)
    #    without breaking on pre-existing unformatted legacy files
    discovered_includes = set()
    crates_dir = wt_path / "crates"
    if crates_dir.is_dir():
        for rs_file in crates_dir.rglob("*.rs"):
            rs_text = rs_file.read_text(encoding="utf-8", errors="replace")
            for m in re.finditer(r'include!\s*\(\s*"([^"]+)"\s*\)', rs_text):
                inc_target = (rs_file.parent / m.group(1)).resolve(strict=False)
                try:
                    discovered_includes.add(str(inc_target.relative_to(wt_path.resolve(strict=False))).replace("\\", "/"))
                except ValueError:
                    continue

    include_rs_files = [
        f
        for f in changed_files
        if f.endswith(".rs") and (f in discovered_includes or f.startswith("crates/akzio-store/src/store/"))
    ]
    for inc_file in include_rs_files:
        base_blob_proc = run_cmd(
            ["git", "show", f"{base_ref}:{inc_file}"], cwd=wt_path, check=False
        )
        base_formatted = False
        if base_blob_proc.returncode == 0:
            base_fmt_proc = subprocess.run(
                ["rustfmt", "--edition", "2021", "--check"],
                input=base_blob_proc.stdout,
                text=True,
                capture_output=True,
                cwd=str(wt_path),
            )
            base_formatted = base_fmt_proc.returncode == 0

        if not base_formatted:
            checks[f"rustfmt_include:{inc_file}"] = "preexisting_unformatted_base_skipped"
            continue

        proc = run_cmd(
            ["rustfmt", "--edition", "2021", "--check", inc_file],
            cwd=wt_path,
            check=False,
        )
        checks[f"rustfmt_include:{inc_file}"] = proc.returncode
        if proc.returncode != 0:
            failures.append(f"include! fragment '{inc_file}' failed rustfmt --edition 2021 --check.")

    # 5. Deterministic Cargo & Shell gates
    gate_commands: List[Tuple[str, List[str]]] = [
        ("bash_syntax", ["bash", "-n", "scripts/run_core.sh"]),
        ("git_diff_check", ["git", "diff", "--check", f"{base_ref}...HEAD"]),
        ("cargo_fmt", ["cargo", "fmt", "--all", "--", "--check"]),
        ("cargo_check", ["cargo", "check", "--workspace", "--locked"]),
        (
            "cargo_clippy",
            [
                "cargo",
                "clippy",
                "--workspace",
                "--all-targets",
                "--all-features",
                "--locked",
                "--",
                "-D",
                "warnings",
            ],
        ),
        ("cargo_test", ["cargo", "test", "--workspace", "--locked"]),
    ]

    cargo_env = {
        **os.environ,
        "CARGO_TARGET_DIR": os.environ.get("CARGO_TARGET_DIR") or str(SHARED_CARGO_TARGET_DIR),
    }
    for gate_name, gate_cmd in gate_commands:
        cmd_env = cargo_env if gate_cmd[0] == "cargo" else None
        proc = run_cmd(gate_cmd, cwd=wt_path, check=False, timeout=900, env=cmd_env)
        checks[gate_name] = {
            "exit_code": proc.returncode,
            "command": " ".join(gate_cmd),
        }
        if proc.returncode != 0:
            tail = (proc.stderr or proc.stdout)[-1200:]
            failures.append(f"Gate '{gate_name}' failed (exit {proc.returncode}):\n{tail}")

    verdict = "PASS" if not failures else "FAIL"
    cert = {
        "issue": args.issue,
        "verdict": verdict,
        "head_sha": head_sha,
        "base_sha": base_sha,
        "worktree": str(wt_path),
        "timestamp": iso_now(),
        "checks": checks,
        "failures": failures,
    }
    cert_path = STATE_DIR / f"cert-issue-{args.issue}.json"
    cert_path.write_text(json.dumps(cert, ensure_ascii=False, indent=2), encoding="utf-8")

    append_ledger(
        "worktree_verified",
        {
            "issue": args.issue,
            "verdict": verdict,
            "head_sha": head_sha,
            "changed_files": changed_files,
            "failure_count": len(failures),
        },
    )
    print(json.dumps(cert, ensure_ascii=False, indent=2))
    return 0 if verdict == "PASS" else 2


def load_verified_certificate(issue: int) -> Dict[str, Any]:
    path = STATE_DIR / f"cert-issue-{issue}.json"
    if not path.is_file():
        raise RuntimeError(f"Verification certificate missing ({path}). Run verify-worktree first.")
    cert = json.loads(path.read_text(encoding="utf-8"))
    checks = cert.get("checks", {})
    required_checks = ("bash_syntax", "git_diff_check", "cargo_fmt", "cargo_check", "cargo_clippy", "cargo_test")
    if (
        cert.get("issue") != issue
        or cert.get("verdict") != "PASS"
        or not cert.get("head_sha")
        or any(checks.get(name, {}).get("exit_code") != 0 for name in required_checks)
    ):
        raise RuntimeError(f"Run verify-worktree with all gates before accepting issue #{issue}.")
    return cert


def cmd_worktree_land(args: argparse.Namespace) -> int:
    ensure_dirs()
    wt_path = WORKTREES_DIR / f"issue-{args.issue}"
    if not wt_path.exists():
        raise RuntimeError(f"Worktree not found: {wt_path}")

    cert = load_verified_certificate(args.issue)
    head_sha = run_cmd(["git", "rev-parse", "HEAD"], cwd=wt_path).stdout.strip()
    if cert["head_sha"] != head_sha:
        raise RuntimeError(
            f"Refusing to land unverified commit {head_sha} (cert_sha={cert['head_sha']})."
        )

    # Verify fast-forward safety so we never overwrite newer commits on INTEGRATION_BRANCH
    is_ff = run_cmd(
        ["git", "merge-base", "--is-ancestor", INTEGRATION_BRANCH, head_sha], check=False
    )
    if is_ff.returncode != 0:
        raise RuntimeError(
            f"Refusing non-fast-forward land: '{INTEGRATION_BRANCH}' has advanced beyond base of {head_sha}. "
            f"Rebase onto '{INTEGRATION_BRANCH}' and re-run verify-worktree."
        )

    # Update INTEGRATION_BRANCH ref to head_sha without touching main working tree
    run_cmd(["git", "branch", "-f", INTEGRATION_BRANCH, head_sha])
    run_cmd(["git", "worktree", "remove", "--force", str(wt_path)], check=False)

    append_ledger(
        "worktree_landed",
        {
            "issue": args.issue,
            "commit": head_sha,
            "target_branch": INTEGRATION_BRANCH,
        },
    )
    print(
        json.dumps(
            {
                "status": "landed",
                "issue": args.issue,
                "commit": head_sha,
                "target_branch": INTEGRATION_BRANCH,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def cmd_worktree_discard(args: argparse.Namespace) -> int:
    ensure_dirs()
    wt_path = WORKTREES_DIR / f"issue-{args.issue}"
    branch_name = f"loop/issue-{args.issue}"
    if wt_path.exists():
        run_cmd(["git", "worktree", "remove", "--force", str(wt_path)], check=False)
        if wt_path.exists():
            shutil.rmtree(wt_path)
    run_cmd(["git", "branch", "-D", branch_name], check=False)
    append_ledger("worktree_discarded", {"issue": args.issue, "branch": branch_name})
    print(json.dumps({"status": "discarded", "issue": args.issue, "branch": branch_name}, indent=2))
    return 0


def cmd_complete(args: argparse.Namespace) -> int:
    ensure_dirs()
    cert = load_verified_certificate(args.issue)
    commit_sha = cert["head_sha"]
    if args.commit and args.commit != commit_sha:
        raise RuntimeError(f"Completion commit {args.commit} differs from verified commit {commit_sha}.")
    summary_md = Path(args.summary_file).read_text(encoding="utf-8").strip()

    report_comment = (
        f"## ✅ `[LOOP-REPORT]` Autonomous Implementation & Verification Complete\n\n"
        f"- **Issue**: `#{args.issue}`\n"
        f"- **Landed Branch**: `{INTEGRATION_BRANCH}`\n"
        f"- **Commit SHA**: `{commit_sha}`\n"
        f"- **Changed Files**: {', '.join(f'`{f}`' for f in cert.get('checks', {}).get('changed_files', []))}\n"
        f"- **Verification Level**: `{args.verification_level}`\n"
        f"- **Timestamp**: `{iso_now()}`\n\n"
        f"---\n\n"
        f"{summary_md}\n"
    )

    if not args.dry_run:
        issue_info = gh_json(["issue", "view", str(args.issue), "--json", "labels"]) or {}
        prev_labels = {lbl.get("name", "") for lbl in issue_info.get("labels", [])}
        run_cmd(["gh", "issue", "comment", str(args.issue), "--body", report_comment])
        set_issue_labels(
            args.issue,
            add_labels=["loop:done"],
            remove_labels=["loop:in-progress", "loop:ready", "loop:inbox"],
        )
        if args.close:
            run_cmd(["gh", "issue", "close", str(args.issue)])
        sync_queue_cache(
            delta_ready=(-1 if "loop:ready" in prev_labels else 0),
            delta_inbox=(-1 if "loop:inbox" in prev_labels else 0),
        )

    append_ledger(
        "issue_completed",
        {
            "issue": args.issue,
            "commit": commit_sha,
            "branch": INTEGRATION_BRANCH,
            "verification_level": args.verification_level,
            "closed": bool(args.close),
        },
    )
    print(
        json.dumps(
            {
                "status": "completed",
                "issue": args.issue,
                "commit": commit_sha,
                "branch": INTEGRATION_BRANCH,
                "closed": bool(args.close),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def cmd_block(args: argparse.Namespace) -> int:
    ensure_dirs()
    reason_text = Path(args.report_file).read_text(encoding="utf-8").strip()
    comment_body = (
        f"## 🛑 `[LOOP-BLOCKED]` Non-Blocking Circuit Breaker / Human Decision Needed\n\n"
        f"- **Issue**: `#{args.issue}`\n"
        f"- **Reason Code**: `{args.reason_code}`\n"
        f"- **Timestamp**: `{iso_now()}`\n"
        f"- **Loop Action**: Skipped automatically to keep the 24h queue moving (`0-human-blocking`).\n\n"
        f"---\n\n"
        f"{reason_text}\n"
    )

    if not args.dry_run:
        issue_info = gh_json(["issue", "view", str(args.issue), "--json", "labels"]) or {}
        prev_labels = {lbl.get("name", "") for lbl in issue_info.get("labels", [])}
        run_cmd(["gh", "issue", "comment", str(args.issue), "--body", comment_body])
        set_issue_labels(
            args.issue,
            add_labels=["loop:blocked"],
            remove_labels=["loop:in-progress", "loop:ready", "loop:inbox"],
        )
        sync_queue_cache(
            delta_ready=(-1 if "loop:ready" in prev_labels else 0),
            delta_inbox=(-1 if "loop:inbox" in prev_labels else 0),
        )

    append_ledger(
        "issue_blocked",
        {
            "issue": args.issue,
            "reason_code": args.reason_code,
        },
    )
    notify_human(
        subject=f"[Akzio Loop Blocked] Issue #{args.issue} ({args.reason_code})",
        body=reason_text,
        metadata={"issue": args.issue, "reason_code": args.reason_code},
    )
    print(
        json.dumps(
            {"status": "blocked", "issue": args.issue, "reason_code": args.reason_code},
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def ensure_daily_report_issue() -> Optional[int]:
    issues = (
        gh_json(
            [
                "issue",
                "list",
                "--state",
                "open",
                "--label",
                "loop:daily-report",
                "--json",
                "number,title",
            ]
        )
        or []
    )
    if issues:
        return int(issues[0]["number"])

    proc = run_cmd(
        [
            "gh",
            "issue",
            "create",
            "--title",
            DAILY_REPORT_ISSUE_TITLE,
            "--label",
            "loop:daily-report",
            "--body",
            (
                "# 📊 Akzio 24h Autonomous Agent Loop — Daily Reports\n\n"
                "本 Issue 由 `.agents/loop/bin/issue_hub.py daily-report` 自动维护，"
                "每日汇总 24h 无人值守研发 Loop 的设计、验证、提交、熔断与待人工决策事项。\n"
            ),
        ]
    )
    url = proc.stdout.strip().splitlines()[-1]
    m = re.search(r"/issues/(\d+)$", url)
    return int(m.group(1)) if m else None


def cmd_daily_report(args: argparse.Namespace) -> int:
    ensure_dirs()
    cutoff = utc_now() - dt.timedelta(hours=args.hours)
    records = read_ledger()

    window_records = []
    for rec in records:
        ts = rec.get("timestamp")
        if not ts:
            continue
        try:
            rec_dt = dt.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=dt.timezone.utc
            )
            if rec_dt >= cutoff:
                window_records.append(rec)
        except ValueError:
            continue

    completed = [r for r in window_records if r.get("event") == "issue_completed"]
    designed = [r for r in window_records if r.get("event") == "issue_designed"]
    proposed = [r for r in window_records if r.get("event") == "issue_proposed"]
    blocked = [r for r in window_records if r.get("event") == "issue_blocked"]
    verified = [r for r in window_records if r.get("event") == "worktree_verified"]
    failed_verifications = [r for r in verified if r.get("verdict") == "FAIL"]

    date_str = utc_now().strftime("%Y-%m-%d")
    report_lines = [
        f"# 📊 Akzio 24h Agent Loop 日报 ({date_str})",
        "",
        f"- **统计窗口**: 过去 `{args.hours}` 小时（截至 `{iso_now()}`）",
        f"- **集成分支**: `{INTEGRATION_BRANCH}`",
        "",
        "## 1. 核心吞吐与质量概览",
        "",
        "| 指标 | 数量 | 说明 |",
        "|---|---|---|",
        f"| 🔍 新发现并提单 (`issue_proposed`) | {len(proposed)} | 由 `issue-scout` 或人工通过指纹去重提单 |",
        f"| 📐 完成架构设计 (`issue_designed`) | {len(designed)} | Leader 发布 `[LOOP-DESIGN]` 契约并锁定白名单 |",
        f"| 🛡️ 反压门禁执行 (`worktree_verified`) | {len(verified)} | 拦截不合规/未过测提交 `{len(failed_verifications)}` 次 |",
        f"| ✅ 完成并合入 (`issue_completed`) | {len(completed)} | 通过 Cargo 全门禁 + 双轴盲审并合入 `{INTEGRATION_BRANCH}` |",
        f"| 🛑 触发熔断/待决策 (`issue_blocked`) | {len(blocked)} | 0 人工阻塞自动跳过，等待后续拍板 |",
        "",
        "## 2. 本周期已完成 Issues (`loop:done`)",
        "",
    ]

    if completed:
        for item in completed:
            report_lines.append(
                f"- **Issue #{item.get('issue')}** — Commit `{item.get('commit')}` "
                f"(`{item.get('verification_level')}`)"
            )
    else:
        report_lines.append("- 本周期暂无新合入的 Issue。")

    report_lines.extend(["", "## 3. 阻塞与待人类拍板项 (`loop:blocked`)", ""])
    if blocked:
        for item in blocked:
            report_lines.append(
                f"- **Issue #{item.get('issue')}** — 原因: `{item.get('reason_code')}` "
                f"（详见对应 Issue 下方的 `[LOOP-BLOCKED]` 评论）"
            )
    else:
        report_lines.append("- 本周期无新增阻塞项。")

    report_lines.extend(["", "## 4. 新提单与下一周期预告 (`loop:inbox` / `loop:ready`)", ""])
    if proposed:
        for item in proposed:
            report_lines.append(
                f"- **Issue #{item.get('issue')}** (`{item.get('priority')}` / `{item.get('area')}`): "
                f"{item.get('title')}"
            )
    else:
        report_lines.append("- 本周期无新提单。")

    report_md = "\n".join(report_lines) + "\n"
    report_path = Path(args.output) if args.output else (REPORTS_DIR / f"daily-{date_str}.md")
    report_path.write_text(report_md, encoding="utf-8")

    published_issue = None
    if args.publish_github and not args.dry_run:
        published_issue = ensure_daily_report_issue()
        if published_issue:
            run_cmd(["gh", "issue", "comment", str(published_issue), "--body", report_md])

    append_ledger(
        "daily_report_generated",
        {
            "report_path": str(report_path),
            "published_issue": published_issue,
            "completed_count": len(completed),
            "blocked_count": len(blocked),
        },
    )
    if not args.dry_run:
        sync_queue_cache(daily_report_due=False)
    notify_human(
        subject=f"[Akzio Loop Daily Report] {date_str}",
        body=report_md,
        metadata={"report_path": str(report_path), "published_issue": published_issue},
    )
    print(
        json.dumps(
            {
                "status": "ok",
                "report_path": str(report_path),
                "published_issue": published_issue,
                "summary": {
                    "proposed": len(proposed),
                    "designed": len(designed),
                    "completed": len(completed),
                    "blocked": len(blocked),
                },
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Akzio 24h Autonomous Loop — GitHub Issues Hub & Backpressure CLI"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init-labels", help="Create/sync loop:* and priority labels on GitHub")
    p_init.add_argument("--dry-run", action="store_true")
    p_init.set_defaults(func=cmd_init_labels)

    p_poll = sub.add_parser("poll", help="Fetch GitHub Issues queue and compute next Leader action")
    p_poll.add_argument("--offline", action="store_true", help="Do not call gh; inspect local ledger only")
    p_poll.set_defaults(func=cmd_poll)

    p_prop = sub.add_parser("propose", help="Propose a deduplicated, grounded issue into loop:inbox")
    p_prop.add_argument("--title", required=True)
    p_prop.add_argument("--priority", choices=["P1", "P2", "P3"], default="P2")
    p_prop.add_argument("--area", required=True)
    p_prop.add_argument("--fingerprint", required=True)
    p_prop.add_argument("--candidate-files", required=True, help="Comma-separated existing repo files")
    p_prop.add_argument("--body-file", required=True)
    p_prop.add_argument("--dry-run", action="store_true")
    p_prop.set_defaults(func=cmd_propose)

    p_design = sub.add_parser("post-design", help="Publish Stage-1 [LOOP-DESIGN] contract onto an issue")
    p_design.add_argument("--issue", type=int, required=True)
    p_design.add_argument("--allowed-files", default="", help="Comma-separated repo-relative paths")
    p_design.add_argument("--design-file", required=True)
    p_design.add_argument("--verdict", choices=["ready", "blocked"], default="ready")
    p_design.add_argument("--dry-run", action="store_true")
    p_design.set_defaults(func=cmd_post_design)

    p_claim = sub.add_parser("claim", help="Mark a loop:ready issue as loop:in-progress")
    p_claim.add_argument("--issue", type=int, required=True)
    p_claim.add_argument("--dry-run", action="store_true")
    p_claim.set_defaults(func=cmd_claim)

    p_wt_create = sub.add_parser(
        "worktree-create", help="Create isolated Git worktree branched from loop/nightly"
    )
    p_wt_create.add_argument("--issue", type=int, required=True)
    p_wt_create.set_defaults(func=cmd_worktree_create)

    p_verify = sub.add_parser(
        "verify-worktree", help="Run deterministic Agent Backpressure checks on an issue worktree"
    )
    p_verify.add_argument("--issue", type=int, required=True)
    p_verify.add_argument("--worktree", default="")
    p_verify.add_argument("--base-ref", default="")
    p_verify.add_argument("--allowed-files", default="")
    p_verify.set_defaults(func=cmd_verify_worktree)

    p_land = sub.add_parser(
        "worktree-land", help="Land a verified issue worktree commit onto loop/nightly"
    )
    p_land.add_argument("--issue", type=int, required=True)
    p_land.set_defaults(func=cmd_worktree_land)

    p_discard = sub.add_parser(
        "worktree-discard", help="Discard an unmerged issue worktree (2-round circuit breaker)"
    )
    p_discard.add_argument("--issue", type=int, required=True)
    p_discard.set_defaults(func=cmd_worktree_discard)

    p_comp = sub.add_parser(
        "complete", help="Post [LOOP-REPORT] to GitHub Issue and transition to loop:done"
    )
    p_comp.add_argument("--issue", type=int, required=True)
    p_comp.add_argument("--commit", default="")
    p_comp.add_argument("--summary-file", required=True)
    p_comp.add_argument("--verification-level", default="offline-verified")
    p_comp.add_argument("--close", action="store_true")
    p_comp.add_argument("--dry-run", action="store_true")
    p_comp.set_defaults(func=cmd_complete)

    p_block = sub.add_parser(
        "block", help="Post [LOOP-BLOCKED] to GitHub Issue and transition to loop:blocked"
    )
    p_block.add_argument("--issue", type=int, required=True)
    p_block.add_argument(
        "--reason-code",
        choices=["circuit_breaker_2_rounds", "needs_product_decision", "contract_boundary"],
        required=True,
    )
    p_block.add_argument("--report-file", required=True)
    p_block.add_argument("--dry-run", action="store_true")
    p_block.set_defaults(func=cmd_block)

    p_daily = sub.add_parser("daily-report", help="Generate and optionally publish 24h daily report")
    p_daily.add_argument("--hours", type=int, default=24)
    p_daily.add_argument("--output", default="")
    p_daily.add_argument("--publish-github", action="store_true")
    p_daily.add_argument("--dry-run", action="store_true")
    p_daily.set_defaults(func=cmd_daily_report)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return args.func(args)
    except Exception as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
