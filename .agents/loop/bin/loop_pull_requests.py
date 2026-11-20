"""Deliver verified GitHub Issues through pull requests and observe their merges."""

from __future__ import annotations

import json
import re
import subprocess

import issue_hub as hub
from loop_github import positive_id


def _verified_head(db, task):
    head = task["verified_head"]
    if (not head or not hub.SHA.fullmatch(head) or head != task["candidate_head"]
            or not task["checks"] or not task["review_summary"]):
        raise RuntimeError("PR delivery requires the same candidate SHA for verification and review.")
    evidence = set()
    for row in db.execute(
        "SELECT kind, details FROM events WHERE task_id = ? AND kind IN ('gate_passed', 'review_passed')",
        (task["id"],),
    ):
        details = json.loads(row["details"])
        if details.get("head_sha") == head and details.get("attempt") == task["attempt"]:
            evidence.add(row["kind"])
    if evidence != {"gate_passed", "review_passed"}:
        raise RuntimeError("PR delivery is missing durable gate or review evidence for the current SHA and attempt.")
    return head


def _pull_branch(task):
    checkout = hub.checkout_name(task)
    if not re.fullmatch(rf"task-{task['id']}(?:-[0-9a-zA-Z]+)?", checkout):
        raise RuntimeError("The frozen checkout name cannot identify a PR branch.")
    return f"loop/pr/{checkout}"


def _has_marker(body, marker):
    return isinstance(body, str) and marker in body.splitlines()


def _validate_pull(pull, repo, branch, head, marker, *, number=None):
    actual_number = positive_id(pull.get("number"))
    if number is not None and actual_number != number:
        raise RuntimeError("GitHub returned an unexpected pull request number.")
    base = pull.get("base") or {}
    source = pull.get("head") or {}
    base_repo = (base.get("repo") or {}).get("full_name")
    head_repo = (source.get("repo") or {}).get("full_name")
    if (not isinstance(base_repo, str) or base_repo.lower() != repo.lower()
            or not isinstance(head_repo, str) or head_repo.lower() != repo.lower()
            or base.get("ref") != "main" or source.get("ref") != branch
            or source.get("sha") != head or not _has_marker(pull.get("body"), marker)):
        raise RuntimeError(f"PR #{actual_number} does not match its repository, branch, verified SHA, or task marker.")
    return actual_number


def _remote_branch(ref):
    rows = hub.git("ls-remote", "--refs", "origin", ref).stdout.splitlines()
    if not rows:
        return None
    if len(rows) == 1:
        sha, _, name = rows[0].partition("\t")
        if name == ref and hub.SHA.fullmatch(sha):
            return sha
    raise RuntimeError(f"Unexpected origin response for {ref}.")


def _check_candidate_history(db, task, base, head):
    commits = hub.git("rev-list", "--parents", f"{base}..{head}").stdout.splitlines()
    earlier = set()
    for commit in commits:
        parts = commit.split()
        if len(parts) != 2:
            raise RuntimeError("The candidate contains a merge outside the frozen base.")
        if parts[0] != head:
            earlier.add(parts[0])
    if not earlier:
        return
    recorded = {
        json.loads(row["details"]).get("head_sha")
        for row in db.execute(
            "SELECT details FROM events WHERE task_id = ? AND kind IN ('candidate_committed', 'candidate_recovered')",
            (task["id"],),
        )
    }
    if not earlier <= recorded:
        raise RuntimeError("The candidate contains commits beyond the task's recorded history.")


def _publish_branch(db, task, branch, head):
    ref = f"refs/heads/{branch}"
    current = _remote_branch(ref)
    if current is not None and current != head:
        raise RuntimeError(f"Origin branch {branch} points to a different commit.")
    remote_main = hub.fetch_remote_main()
    base = task["base_sha"]
    if (not base or not hub.SHA.fullmatch(base) or base == head or hub.ref_commit(head) != head
            or not hub.is_ancestor(base, remote_main) or not hub.is_ancestor(base, head)):
        raise RuntimeError("The verified commit must advance a frozen base reachable from origin/main.")
    if hub.is_ancestor(head, remote_main):
        raise RuntimeError("Origin/main already contains the verified commit without a matching PR.")
    _check_candidate_history(db, task, base, head)
    if current is None:
        # The empty lease requires the remote ref to be absent even if it appeared after ls-remote.
        hub.git("push", "--no-follow-tags", "--recurse-submodules=no",
                f"--force-with-lease={ref}:", "origin", f"{head}:{ref}")
    if _remote_branch(ref) != head:
        raise RuntimeError(f"Origin branch {branch} could not be verified at the approved SHA.")


def _read_pull(github, number, branch, head, marker):
    pull = github.pull(number)
    _validate_pull(pull, github.repo, branch, head, marker, number=number)
    if pull.get("state") == "open" and pull.get("merged") is False:
        if _remote_branch(f"refs/heads/{branch}") != head:
            raise RuntimeError(f"Open PR #{number} no longer has its verified origin branch.")
    elif pull.get("state") != "closed" or type(pull.get("merged")) is not bool:
        raise RuntimeError(f"PR #{number} has an unexpected state.")
    return pull


def _ensure_pull(db, github, task, branch, head, marker):
    existing = github.pulls_by_head(branch)
    if len(existing) > 1:
        raise RuntimeError(f"Multiple pull requests already use branch {branch}.")
    if existing:
        proposed = existing[0]
    else:
        _publish_branch(db, task, branch, head)
        body = (f"Source Issue #{task['issue']}\n\n"
                f"Verified commit `{head}` · `offline-verified`\n\n"
                "Shell syntax, diff check, Cargo formatting, compilation, Clippy, tests, and independent review passed.\n\n"
                f"{marker}")
        proposed = github.create_pull(task["title"], body, branch)
    number = _validate_pull(proposed, github.repo, branch, head, marker)
    return _read_pull(github, number, branch, head, marker)


def _confirmed_merge(pull, task):
    sha = pull.get("merge_commit_sha")
    if not isinstance(sha, str) or not hub.SHA.fullmatch(sha):
        raise RuntimeError(f"Merged PR #{pull['number']} is missing its merge commit SHA.")
    if task["merge_sha"] is not None and task["merge_sha"] != sha:
        raise RuntimeError(f"PR #{pull['number']} reports a different merge commit from the recorded receipt.")
    if not hub.is_ancestor(sha, hub.fetch_remote_main()):
        raise RuntimeError(f"Merge commit {sha} for PR #{pull['number']} is missing from freshly fetched origin/main.")
    return sha


def _source_issue(issue, number):
    if (issue.get("number") != number or "pull_request" in issue
            or issue.get("state") not in {"open", "closed"}):
        raise RuntimeError(f"GitHub did not return source Issue #{number} with a known state.")
    return issue


def _acknowledge_merge(github, task, identity, number, merge, snapshot_matches):
    issue_number = positive_id(task["issue"])
    issue = _source_issue(github.issue(issue_number), issue_number)
    actor_id = positive_id(github.actor().get("id"))
    marker = f"<!-- akzio-loop-pr-merged:{identity}:{task['id']}:{number}:{merge} -->"
    matches_snapshot = snapshot_matches(issue, issue_number, task["snapshot_sha"])

    def matches(comment):
        return ((comment.get("user") or {}).get("id") == actor_id
                and _has_marker(comment.get("body"), marker))

    if not any(matches(comment) for comment in github.issue_comments(issue_number)):
        result = f"PR #{number} was merged into `main` at `{merge}`."
        if not matches_snapshot:
            result += " The current Issue differs from the approved snapshot and remains for the maintainers to assess."
        body = f"{result}\n\n{marker}"
        comment = github.create_comment(issue_number, body)
        if not matches(comment):
            raise RuntimeError(f"GitHub did not confirm the acknowledgment for Issue #{issue_number}.")
        positive_id(comment.get("id"))
    if issue["state"] == "open" and matches_snapshot:
        # Re-read just before the close to avoid acting on an earlier Issue snapshot.
        latest = _source_issue(github.issue(issue_number), issue_number)
        if latest["state"] == "closed" or not snapshot_matches(latest, issue_number, task["snapshot_sha"]):
            return
        closed = _source_issue(github.close_issue(issue_number), issue_number)
        if closed["state"] != "closed":
            raise RuntimeError(f"GitHub did not confirm that Issue #{issue_number} was closed.")


def _retire_branch(branch, head):
    ref = f"refs/heads/{branch}"
    current = _remote_branch(ref)
    if current is None:
        return
    if current != head:
        raise RuntimeError(f"Origin branch {branch} changed after merge and was retained.")
    # Compare and delete the dedicated PR ref. A concurrent ref update invalidates this lease.
    result = hub.git("push", "--no-follow-tags", "--recurse-submodules=no",
                     f"--force-with-lease={ref}:{head}", "origin", f":{ref}", check=False)
    remaining = _remote_branch(ref)
    if remaining is None:
        return
    if remaining != head:
        raise RuntimeError(f"Origin branch {branch} changed during retirement and was retained.")
    detail = (result.stderr or result.stdout).strip()[-1200:]
    raise RuntimeError(f"Origin branch {branch} still exists after retirement: {detail}")


def _sync_task(db, github, task, identity, counts, snapshot_matches):
    task_id = task["id"]
    if task["repo"].lower() != github.repo.lower():
        raise RuntimeError("The task was registered against a different GitHub repository.")
    head = _verified_head(db, task)
    branch = _pull_branch(task)
    marker = f"<!-- akzio-loop-pr:{identity}:{task_id}:{head} -->"
    if task["blocked_reason"] == "awaiting_pr":
        if task["pr"] is not None or task["merge_sha"] is not None or task["acknowledged"]:
            raise RuntimeError("The task awaiting a PR already carries a delivery receipt.")
        pull = _ensure_pull(db, github, task, branch, head, marker)
        number = pull["number"]
        with hub.transaction(db):
            db.execute("UPDATE github_tasks SET pr = ? WHERE task_id = ?", (number, task_id))
            db.execute("UPDATE tasks SET blocked_reason = 'awaiting_merge' WHERE id = ?", (task_id,))
            hub.event(db, "task_pr_created", task_id, repo=github.repo, issue=task["issue"],
                      pr=number, branch=branch, head_sha=head)
        counts["created"] += 1
    else:
        number = positive_id(task["pr"])
        pull = _read_pull(github, number, branch, head, marker)
    if pull["merged"] is not True:
        if task["status"] == "done":
            raise RuntimeError(f"Previously merged PR #{number} is no longer reported as merged.")
        stage = "pr_closed" if pull["state"] == "closed" else "awaiting_merge"
        previous = "awaiting_merge" if task["blocked_reason"] == "awaiting_pr" else task["blocked_reason"]
        if previous != stage:
            with hub.transaction(db):
                db.execute("UPDATE tasks SET blocked_reason = ? WHERE id = ?", (stage, task_id))
                hub.event(db, "task_blocked" if stage == "pr_closed" else "task_pr_reopened", task_id,
                          reason=stage, issue=task["issue"], pr=number)
        return

    merge = _confirmed_merge(pull, task)
    if task["status"] != "done":
        with hub.transaction(db):
            db.execute("UPDATE github_tasks SET merge_sha = ? WHERE task_id = ?", (merge, task_id))
            db.execute("UPDATE tasks SET status = 'done', blocked_reason = NULL WHERE id = ?", (task_id,))
            hub.event(db, "task_pr_merged", task_id, repo=github.repo, issue=task["issue"],
                      pr=number, head_sha=head, merge_sha=merge)
        counts["merged"] += 1
    if not task["acknowledged"]:
        if not hub.is_ancestor(head, merge):
            hub.git("update-ref", f"refs/archive/akzio-loop/pr/{identity}/{head}", head)
        hub.cleanup_task_checkout(task)
        _acknowledge_merge(github, task, identity, number, merge, snapshot_matches)
        with hub.transaction(db):
            db.execute("UPDATE github_tasks SET acknowledged = 1 WHERE task_id = ?", (task_id,))
        counts["acknowledged"] += 1
    _retire_branch(branch, head)
    with hub.transaction(db):
        hub.event(db, "pr_branch_retired", task_id, repo=github.repo, pr=number,
                  branch=branch, head_sha=head)
    counts["retired"] += 1


def sync_pull_requests(db, github, snapshot_matches):
    """Reconcile deliveries under the bridge and hub control locks with no active wave.

    The bridge must bind origin's fetch and push URLs to the configured GitHub repository.
    Each merged task stays eligible until its Issue is acknowledged and the dedicated
    branch is either absent or deleted with a lease against the verified head.
    """
    tasks = db.execute(
        """SELECT t.*, g.repo, g.issue, g.pr, g.merge_sha, g.acknowledged, g.snapshot_sha
           FROM tasks t JOIN github_tasks g ON g.task_id = t.id
           WHERE (t.status = 'blocked' AND t.blocked_reason IN ('awaiting_pr', 'awaiting_merge', 'pr_closed'))
              OR (t.status = 'done' AND g.pr IS NOT NULL AND g.merge_sha IS NOT NULL AND
                  NOT EXISTS (SELECT 1 FROM events e WHERE e.task_id = t.id AND e.kind = 'pr_branch_retired'))
           ORDER BY t.id""",
    ).fetchall()
    counts = {"created": 0, "merged": 0, "acknowledged": 0, "retired": 0, "errors": []}
    if not tasks:
        return counts
    identity = db.execute("SELECT identity FROM loop_info WHERE singleton = 1").fetchone()["identity"]
    for task in tasks:
        try:
            _sync_task(db, github, task, identity, counts, snapshot_matches)
        except (RuntimeError, ValueError, OSError, subprocess.TimeoutExpired) as exc:
            counts["errors"].append({"task": task["id"], "issue": task["issue"], "error": str(exc)})
    return counts
