#!/usr/bin/env python3
"""Operator-side sync between the local Loop queue and one GitHub repository."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
import sqlite3
import subprocess
import sys
import textwrap

import issue_hub as hub
from loop_github import GitHub, issue_number, positive_id
from loop_pull_requests import sync_pull_requests


START = re.compile(r"/agent start ([0-9a-f]{64})")
CHOOSE = re.compile(r"(?:CHOOSE|/agent choose) (AZ-[1-9][0-9]*-R[1-9][0-9]*-[0-9a-f]{12}) ([A-E])")
SECTIONS = ("objective", "constraints", "acceptance criteria")
ORIGIN = re.compile(r"(?:https://github\.com/|ssh://git@github\.com/|git@github\.com:)([^/]+/[^/]+)", re.I)
SYNC_OVERLAP = 5 * 60
SYNC_ERRORS = (RuntimeError, ValueError, KeyError, OSError, sqlite3.Error, subprocess.TimeoutExpired)
ISSUE_EVENTS = {
    "github_task_imported": "任务已入队",
    "task_designed": "已冻结设计与验收范围",
    "task_claimed": "已领取任务并建立隔离 worktree",
    "gate_passed": "机械门禁通过",
    "review_passed": "独立审查通过",
    "task_blocked": "任务已阻塞",
    "task_pr_created": "已创建 PR，等待人工合并",
    "task_pr_reopened": "已重新打开经验证的 PR，等待人工合并",
}


def stamp(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z", value):
        raise ValueError("Expected a GitHub UTC timestamp.")
    return int(dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())


def user_id(payload):
    return (payload.get("user") or {}).get("id")


def bind_origin(repo):
    for extra in ((), ("--push",)):
        urls = hub.git("remote", "get-url", "--all", *extra, "origin").stdout.splitlines()
        match = ORIGIN.fullmatch(urls[0]) if len(urls) == 1 else None
        if not match or match[1].lower().removesuffix(".git") != repo:
            raise RuntimeError("Both origin fetch and push must point to the configured GitHub repository.")


def issue_snapshot(issue, number, *, open_only=True):
    if issue.get("number") != number or "pull_request" in issue or (open_only and issue.get("state") != "open"):
        raise ValueError("Only an open Issue can be imported.")
    title, body = issue.get("title"), issue.get("body")
    if not isinstance(title, str) or not title.strip() or len(title) > 200:
        raise ValueError("The Issue needs a title of at most 200 characters.")
    if not isinstance(body, str) or len(body) > 12000:
        raise ValueError("The Issue needs a task body of at most 12,000 characters.")
    sections = {}
    for match in re.finditer(r"^## (Objective|Constraints|Acceptance criteria)[ \t]*$\n(.*?)(?=^## |\Z)",
                             body.replace("\r\n", "\n"), re.I | re.M | re.S):
        heading = match[1].lower()
        if heading in sections or not match[2].strip():
            raise ValueError("Each task heading must appear once with content.")
        sections[heading] = match[2].strip()
    if set(sections) != set(SECTIONS):
        raise ValueError("The Issue needs Objective, Constraints, and Acceptance criteria headings.")
    spec = {"title": title.strip(), **{key.replace(" ", "_"): sections[key] for key in SECTIONS}}
    digest = hashlib.sha256(json.dumps(spec, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    return spec, digest


def import_start(db, github, number, issue, operator, comment, digest):
    spec, actual = issue_snapshot(issue, number)
    if digest != actual:
        raise ValueError("The authorization does not match the Issue snapshot. Prepare it again.")
    fingerprint = "github-" + hashlib.sha256(f"{github.repo}#{number}".encode()).hexdigest()
    body = "\n\n".join(f"## {name}\n{spec[name.lower().replace(' ', '_')]}" for name in
                       ("Objective", "Constraints", "Acceptance criteria"))
    base = hub.ref_commit(hub.REMOTE_MAIN_REF)
    if not base:
        raise RuntimeError("Fetch origin/main before authorizing an Issue task.")
    with hub.transaction(db):
        task = db.execute(
            "INSERT INTO tasks (fingerprint, title, priority, area, candidate_files, body) VALUES (?, ?, 'P2', 'github', '[]', ?)",
            (fingerprint, spec["title"], body),
        ).lastrowid
        db.execute(
            "INSERT INTO github_tasks (task_id, repo, issue, start_comment, snapshot_sha, base_sha) VALUES (?, ?, ?, ?, ?, ?)",
            (task, github.repo, number, positive_id(comment["id"]), actual, base),
        )
        hub.event(db, "github_task_imported", task, issue=number, snapshot_sha=actual, actor=operator,
                  comment=comment["id"])
    return True


def import_choice(db, github, decision, operator, comment, choice):
    created = stamp(comment["created_at"])
    if not decision["opened_at"] <= created <= hub.utc_timestamp():
        raise ValueError("The reply is outside the published decision's lifetime.")
    with hub.transaction(db):
        stage = hub.apply_decision(db, decision["id"], choice, actor=operator,
                                   comment=positive_id(comment["id"]), received_at=created)
    return stage != "already_resolved"


def is_operator_comment(comment, operator):
    return user_id(comment) == operator and comment.get("created_at") == comment.get("updated_at")


def import_starts(db, github, operator, audit_issue):
    now = hub.utc_timestamp()
    cursor = db.execute("SELECT * FROM github_sync WHERE repo = ?", (github.repo,)).fetchone()
    start = max(cursor["enabled_at"], cursor["synced_at"] - SYNC_OVERLAP)
    candidates = []
    for comment in github.repository_comments(hub.iso_time(start - 1)):
        if not is_operator_comment(comment, operator):
            continue
        match = START.fullmatch((comment.get("body") or "").strip())
        if match:
            candidates.append((stamp(comment["created_at"]), positive_id(comment["id"]), match[1], comment))
    imported = 0
    ignored = []
    for created, cid, digest, comment in sorted(candidates, key=lambda row: row[:2]):
        if created < start or created > now:
            continue
        try:
            number = issue_number(github.repo, comment.get("issue_url"))
            if number == audit_issue or db.execute(
                "SELECT 1 FROM github_tasks WHERE repo = ? AND issue = ?", (github.repo, number),
            ).fetchone():
                continue
            if db.execute("SELECT 1 FROM decision_requests WHERE repo = ? AND issue = ?", (github.repo, number)).fetchone():
                continue
            imported += import_start(db, github, number, github.issue(number), operator, comment, digest)
        except ValueError as exc:
            ignored.append({"comment": cid, "reason": str(exc)})
    with hub.transaction(db):
        db.execute("UPDATE github_sync SET synced_at = max(synced_at, ?) WHERE repo = ?", (now, github.repo))
    return {"imported": imported, "ignored": ignored}


def import_replies(db, github, operator):
    pending = db.execute(
        """SELECT * FROM decision_requests d WHERE d.repo = ? AND d.issue IS NOT NULL AND d.choice IS NULL
           AND d.invalidated = 0 AND NOT EXISTS (SELECT 1 FROM decision_requests newer WHERE newer.task_id = d.task_id AND newer.version > d.version)
           ORDER BY d.requested_at, d.id""", (github.repo,),
    ).fetchall()
    resolved = 0
    ignored = []
    for decision in pending:
        if decision["approver"] != operator:
            invalidate_decision(db, decision, "decision_changed")
            continue
        issue = github.issue(decision["issue"])
        if (issue.get("number") != decision["issue"] or issue.get("state") != "open" or "pull_request" in issue
                or body_sha(issue.get("body")) != decision["public_body_sha"]):
            invalidate_decision(db, decision, "decision_changed")
            ignored.append({"decision": decision["id"], "reason": "The decision Issue is closed or its published text changed."})
            continue
        candidates = []
        for comment in github.issue_comments(decision["issue"]):
            if not is_operator_comment(comment, operator):
                continue
            first = (comment.get("body") or "").strip().partition("\n")[0].rstrip("\r")
            match = CHOOSE.fullmatch(first)
            if match and match[1] == decision["id"]:
                candidates.append((stamp(comment["created_at"]), positive_id(comment["id"]), match[2], comment))
        for _, cid, choice, comment in sorted(candidates, key=lambda row: row[:2]):
            try:
                resolved += import_choice(db, github, decision, operator, comment, choice)
                break
            except ValueError as exc:
                ignored.append({"comment": cid, "reason": str(exc)})
        else:
            if decision["expires_at"] <= hub.utc_timestamp():
                invalidate_decision(db, decision, "decision_expired")
    return {"resolved": resolved, "ignored": ignored}


def invalidate_decision(db, decision, reason):
    with hub.transaction(db):
        db.execute("UPDATE decision_requests SET invalidated = 1 WHERE id = ?", (decision["id"],))
        db.execute("UPDATE tasks SET blocked_reason = ? WHERE id = ? AND blocked_reason = 'awaiting_input'",
                   (reason, decision["task_id"]))
        hub.event(db, "task_blocked", decision["task_id"], reason=reason, decision_id=decision["id"],
                  visibility=decision["visibility"])


def body_sha(body):
    return hashlib.sha256(body.encode()).hexdigest() if isinstance(body, str) else None


def marker_lookup(rows, actor, marker):
    matches = [row for row in rows if user_id(row) == actor and marker in (row.get("body") or "")]
    if len(matches) > 1:
        raise RuntimeError("GitHub returned duplicate Loop markers. Inspect the remote records before retrying.")
    return matches[0] if matches else None


def post_once(github, number, actor, marker, body):
    existing = marker_lookup(github.issue_comments(number), actor, marker)
    if existing:
        return positive_id(existing["id"])
    posted = github.create_comment(number, f"{body}\n\n{marker}")
    if user_id(posted) != actor or marker not in (posted.get("body") or ""):
        raise RuntimeError("GitHub did not confirm the expected comment author and marker.")
    return positive_id(posted["id"])


def publish_decisions(db, github, actor, operator):
    pending = db.execute(
        """SELECT d.*, g.issue AS source_issue FROM decision_requests d
           JOIN tasks t ON t.id = d.task_id LEFT JOIN github_tasks g ON g.task_id = t.id
           WHERE d.visibility = 'github' AND d.issue IS NULL AND d.choice IS NULL
           AND d.invalidated = 0 AND d.expires_at > ? AND t.blocked_reason = 'awaiting_input'
           AND (g.repo IS NULL OR g.repo = ?)
           ORDER BY d.requested_at, d.id""",
        (hub.utc_timestamp(), github.repo),
    ).fetchall()
    if not pending:
        return 0
    login = github.user_login(operator)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,38}", login):
        raise RuntimeError("GitHub returned an invalid approver login.")
    observed = [issue for issue in github.issues_by_creator(actor["login"]) if "pull_request" not in issue]
    for decision in pending:
        marker = f"<!-- akzio-loop-decision:{decision['id']} -->"
        request = json.loads(decision["request"])
        lines = [request["question"], ""]
        lines += [f"{letter}. {option['label']}\n   {option['impact']}" for letter, option in request["options"].items()]
        source = f" · 原 Issue #{decision['source_issue']}" if decision["source_issue"] else ""
        body = (
            f"@{login}，Loop 任务 {decision['task_id']} 等待裁决{source}。\n\n"
            + textwrap.indent("\n".join(lines), "    ")
            + f"\n\n建议选项 `{request['recommendation']}`。请回复此邮件或评论，在第一行写 "
              f"`CHOOSE {decision['id']} {request['recommendation']}`（末尾字母改成你的选项）。\n\n"
              f"有效期至 `{hub.iso_time(decision['expires_at'])}`。\n\n{marker}"
        )
        issue = marker_lookup(observed, actor["id"], marker)
        if not issue:
            issue = github.create_issue(f"Akzio Loop 决策 · {decision['id']}", body)
        if user_id(issue) != actor["id"] or issue.get("body") != body or issue.get("state") != "open":
            raise RuntimeError("GitHub did not confirm the expected decision Issue and frozen text.")
        issue_id = positive_id(issue["number"])
        with hub.transaction(db):
            db.execute(
                "UPDATE decision_requests SET repo = ?, issue = ?, opened_at = ?, public_body_sha = ?, approver = ? WHERE id = ?",
                (github.repo, issue_id, stamp(issue["created_at"]), body_sha(body), operator, decision["id"]),
            )
            hub.event(db, "decision_issue_opened", decision["task_id"], decision_id=decision["id"], issue=issue_id)
    return len(pending)


def finish_decisions(db, github, actor):
    pending = db.execute(
        "SELECT * FROM decision_requests WHERE repo = ? AND issue IS NOT NULL AND choice IS NOT NULL AND closed = 0",
        (github.repo,),
    ).fetchall()
    for decision in pending:
        number = decision["issue"]
        if not decision["notified"]:
            status = "原任务已重新入队" if json.loads(decision["request"])["options"][decision["choice"]]["resume"] else "原任务保持阻塞"
            post_once(github, number, actor, f"<!-- akzio-loop-decision-result:{decision['id']} -->",
                      f"已记录选项 `{decision['choice']}`。{status}。")
            with hub.transaction(db):
                db.execute("UPDATE decision_requests SET notified = 1 WHERE id = ?", (decision["id"],))
        issue = github.issue(number)
        if issue.get("state") != "closed":
            issue = github.close_issue(number)
        if issue.get("number") != number or issue.get("state") != "closed":
            raise RuntimeError(f"GitHub did not confirm closing decision Issue #{number}.")
        with hub.transaction(db):
            db.execute("UPDATE decision_requests SET closed = 1 WHERE id = ?", (decision["id"],))
    return len(pending)


def publish_issue_updates(db, github, actor):
    identity = db.execute("SELECT identity FROM loop_info").fetchone()[0]
    states = tuple(ISSUE_EVENTS)
    rows = db.execute(
        f"""SELECT g.issue, e.* FROM github_tasks g JOIN events e ON e.task_id = g.task_id
            WHERE g.repo = ? AND e.kind IN ({','.join('?' for _ in states)}) AND NOT EXISTS (
              SELECT 1 FROM deliveries d WHERE d.destination = g.repo || '#' || g.issue AND d.event_id = e.id
            ) ORDER BY e.id""", (github.repo, *states),
    ).fetchall()
    for row in rows:
        details = json.loads(row["details"])
        message = ISSUE_EVENTS[row["kind"]]
        if row["kind"] in ("task_pr_created", "task_pr_reopened"):
            message += f" [PR #{int(details['pr'])}](https://github.com/{github.repo}/pull/{int(details['pr'])})"
        elif row["kind"] == "task_blocked":
            message += f"（`{details['reason']}`）"
        number = row["issue"]
        marker = hub.audit_marker(identity, row["id"])
        cid = post_once(github, number, actor, marker, f"Akzio Loop 任务 {row['task_id']} · {message}。")
        with hub.transaction(db):
            db.execute("INSERT INTO deliveries VALUES (?, ?, ?)", (f"{github.repo}#{number}", row["id"], cid))
    return len(rows)


def advance_local_main(remote):
    local = hub.ref_commit(hub.MAIN_REF)
    if (local and local != remote and hub.is_ancestor(local, remote)
            and hub.git("symbolic-ref", "--quiet", "HEAD", check=False).stdout.strip() == hub.MAIN_REF
            and not hub.git("status", "--porcelain=v1", "--untracked-files=all").stdout):
        hub.git("merge", "--ff-only", "--no-autostash", "--no-overwrite-ignore", remote)


def verify_binding(db, github):
    if db.execute("SELECT 1 FROM github_sync WHERE repo <> ?", (github.repo,)).fetchone():
        raise RuntimeError("This Loop queue is bound to a different GitHub repository.")


def snapshot_matches(issue, number, digest):
    try:
        return issue_snapshot(issue, number, open_only=False)[1] == digest
    except ValueError:
        return False


def synchronize(db, github, operator, audit_issue):
    verify_binding(db, github)
    actor = github.actor()
    positive_id(actor["id"])
    now = hub.utc_timestamp()
    with hub.transaction(db):
        db.execute("INSERT OR IGNORE INTO github_sync VALUES (?, ?, ?)", (github.repo, now, now))
    result = {"status": "synced", "errors": []}
    stages = (
        ("decisions_opened", lambda: publish_decisions(db, github, actor, operator)),
        ("inbound", lambda: import_starts(db, github, operator, audit_issue)),
        ("replies", lambda: import_replies(db, github, operator)),
        ("decisions_closed", lambda: finish_decisions(db, github, actor["id"])),
        ("pull_requests", lambda: sync_pull_requests(db, github, snapshot_matches)),
        ("updates", lambda: publish_issue_updates(db, github, actor["id"])),
    )
    for name, operation in stages:
        try:
            result[name] = operation()
            if name == "pull_requests":
                result["errors"].extend(result[name].get("errors", []))
        except SYNC_ERRORS as exc:
            result["errors"].append(f"{name}: {exc}")
    if result["errors"]:
        result["status"] = "pending"
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare", allow_abbrev=False)
    prepare.add_argument("--repo", required=True)
    prepare.add_argument("--issue", required=True, type=int)
    sync = commands.add_parser("sync", allow_abbrev=False)
    sync.add_argument("--repo", required=True)
    sync.add_argument("--operator", required=True, type=int)
    sync.add_argument("--audit-issue", required=True, type=int)
    args = parser.parse_args()
    try:
        github = GitHub(args.repo, hub.run_cmd)
        bind_origin(github.repo)
        if args.command == "prepare":
            spec, digest = issue_snapshot(github.issue(args.issue), args.issue)
            return hub.emit({"task": spec, "command": f"/agent start {digest}"})
        operator, audit_issue = positive_id(args.operator), positive_id(args.audit_issue)
        with hub.process_lock("control"), hub.open_db() as db:
            if db.execute("SELECT 1 FROM wave WHERE expires_at > ?", (hub.utc_timestamp(),)).fetchone():
                return hub.emit({"status": "busy"})
            verify_binding(db, github)
            remote = hub.fetch_remote_main()
            requeued = hub.requeue_stale_main(db, remote)
            advance_local_main(remote)
            result = synchronize(db, github, operator, audit_issue)
            result["local_tasks_requeued"] = requeued
        hub.emit(result)
        return int(bool(result["errors"]))
    except SYNC_ERRORS as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
