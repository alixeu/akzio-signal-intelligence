"""GitHub REST transport for the Loop Issue bridge."""

from __future__ import annotations

import json
import re
from urllib.parse import urlencode


REPOSITORY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*/[A-Za-z0-9_.-]+")
ISSUE_URL = re.compile(r"https://api\.github\.com/repos/([^/]+/[^/]+)/issues/([1-9][0-9]*)")


def normalize_repo(repo: str) -> str:
    if not isinstance(repo, str) or not REPOSITORY.fullmatch(repo) or repo.split("/")[1] in {".", ".."}:
        raise ValueError("GitHub repository must be OWNER/REPO.")
    return repo.lower()


def issue_number(repo: str, issue_url: str) -> int:
    repo = normalize_repo(repo)
    match = ISSUE_URL.fullmatch(issue_url) if isinstance(issue_url, str) else None
    if match and REPOSITORY.fullmatch(match[1]) and match[1].lower() == repo:
        return int(match[2])
    raise ValueError("GitHub issue_url must reference an Issue in the configured repository.")


def positive_id(value: int) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError("GitHub IDs must be positive integers.")
    return value


def _local_branch(branch: str) -> str:
    if not isinstance(branch, str) or not branch.strip() or ":" in branch:
        raise ValueError("GitHub pull request requires a branch in the configured repository.")
    return branch


class GitHub:
    def __init__(self, repo: str, run):
        self.repo = normalize_repo(repo)
        self.endpoint = f"repos/{self.repo}"
        self.run = run

    def _request(self, *args, timeout, **kwargs):
        # Propagate rate limits and ambiguous writes to the caller for reconciliation.
        response = self.run(["gh", "api", "--hostname", "github.com", *args], timeout=timeout, **kwargs)
        try:
            return json.loads(response.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError("GitHub returned invalid JSON.") from exc

    def _object(self, endpoint, *, method="GET", payload=None) -> dict:
        if method == "GET":
            value = self._request(endpoint, timeout=15)
        else:
            value = self._request(
                "--method", method, endpoint, "--input", "-", timeout=30,
                input_text=json.dumps(payload, ensure_ascii=False),
            )
        if not isinstance(value, dict):
            raise RuntimeError("GitHub returned a response other than a JSON object.")
        return value

    def _pages(self, endpoint, **query) -> list[dict]:
        endpoint = f"{endpoint}?{urlencode(dict(query, per_page=100))}"
        pages = self._request("--paginate", "--slurp", endpoint, timeout=30)
        if not isinstance(pages, list) or any(not isinstance(page, list) for page in pages):
            raise RuntimeError("GitHub returned malformed JSON pages.")
        rows = [row for page in pages for row in page]
        if any(not isinstance(row, dict) for row in rows):
            raise RuntimeError("GitHub returned a page containing a non-object entry.")
        return rows

    def actor(self) -> dict:
        return self._object("user")

    def user_login(self, user_id: int) -> str:
        user = self._object(f"user/{positive_id(user_id)}")
        login = user.get("login")
        if not isinstance(login, str) or not login.strip():
            raise RuntimeError("GitHub user response is missing a login.")
        return login

    def repository(self) -> dict:
        return self._object(self.endpoint)

    def issue(self, number: int) -> dict:
        return self._object(f"{self.endpoint}/issues/{positive_id(number)}")

    def repository_comments(self, since: str) -> list[dict]:
        return self._pages(f"{self.endpoint}/issues/comments", sort="updated", direction="asc", since=since)

    def issue_comments(self, number: int) -> list[dict]:
        return self._pages(f"{self.endpoint}/issues/{positive_id(number)}/comments")

    def issues_by_creator(self, login: str) -> list[dict]:
        return self._pages(f"{self.endpoint}/issues", state="all", creator=login)

    def create_issue(self, title: str, body: str) -> dict:
        return self._object(f"{self.endpoint}/issues", method="POST", payload={"title": title, "body": body})

    def create_comment(self, number: int, body: str) -> dict:
        return self._object(
            f"{self.endpoint}/issues/{positive_id(number)}/comments", method="POST", payload={"body": body},
        )

    def pulls_by_head(self, branch: str) -> list[dict]:
        owner = self.repo.split("/", 1)[0]
        return self._pages(f"{self.endpoint}/pulls", state="all", head=f"{owner}:{_local_branch(branch)}", base="main")

    def create_pull(self, title: str, body: str, branch: str) -> dict:
        return self._object(
            f"{self.endpoint}/pulls", method="POST",
            payload={"title": title, "body": body, "head": _local_branch(branch), "base": "main"},
        )

    def pull(self, number: int) -> dict:
        return self._object(f"{self.endpoint}/pulls/{positive_id(number)}")

    def close_issue(self, number: int, *, completed: bool = True) -> dict:
        return self._object(
            f"{self.endpoint}/issues/{positive_id(number)}", method="PATCH",
            payload={"state": "closed", "state_reason": "completed" if completed else "not_planned"},
        )
