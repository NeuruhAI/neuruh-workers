"""Real GitHub PR/CI adapters for the PROMOTION EXECUTOR, via the `gh` CLI.

Reads with `gh pr view` / `gh api`; the ONLY writes are `gh pr ready` and
`gh pr merge --merge --match-head-commit <sha>` — and the latter is reachable
only through ``promote_merge`` with a Governance authorization reference.
``merge`` (ungoverned) still raises AUTO_MERGE. No force, no squash, no
rebase, no tags, no deploys, no comments.
"""
from __future__ import annotations

import json
import os
import subprocess
from typing import Any, Callable, Sequence

from .adapters import CISnapshot, PullRequest, summarize_ci
from .errors import GitCustodianRefused
from .redact import redact_text

Runner = Callable[[Sequence[str]], subprocess.CompletedProcess]

_PR_FIELDS = "number,state,isDraft,mergedAt,headRefName,baseRefName,headRefOid,mergeCommit,url,title,body"


def _default_runner(argv: Sequence[str]) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["GH_PROMPT_DISABLED"] = "1"
    env["GIT_TERMINAL_PROMPT"] = "0"
    return subprocess.run(list(argv), capture_output=True, text=True, timeout=120,
                          env=env, check=False)


def _mission_from_body(body: str) -> str:
    for line in (body or "").splitlines():
        stripped = line.strip()
        for prefix in ("MISSION:", "MISSION="):
            if stripped.startswith(prefix):
                return stripped[len(prefix):].strip()
    return ""


class GhPRAdapter:
    backend_name = "gh.pr"

    def __init__(self, runner: Runner | None = None):
        self._run = runner or _default_runner
        self.command_log: list[list[str]] = []
        self.promote_calls: list[dict[str, Any]] = []
        self.ready_calls: list[dict[str, Any]] = []
        self.merge_calls: list[dict[str, Any]] = []

    def _gh(self, args: Sequence[str]) -> subprocess.CompletedProcess:
        argv = ["gh", *args]
        self.command_log.append(list(argv))
        return self._run(argv)

    def _require(self, args: Sequence[str], *, reason: str) -> str:
        result = self._gh(args)
        if result.returncode != 0:
            raise GitCustodianRefused(
                f"gh {' '.join(args[:2])} failed",
                {"stderr": redact_text(result.stderr or result.stdout)}, reason=reason)
        return result.stdout

    def get(self, repo: str, number: int) -> PullRequest | None:
        result = self._gh(["pr", "view", str(number), "-R", repo, "--json", _PR_FIELDS])
        if result.returncode != 0:
            if "Could not resolve" in (result.stderr or "") or "not found" in (result.stderr or "").lower():
                return None
            raise GitCustodianRefused(
                "gh pr view failed", {"stderr": redact_text(result.stderr)}, reason="PR_READ_FAILED")
        try:
            data = json.loads(result.stdout or "{}")
        except ValueError as exc:
            raise GitCustodianRefused("gh pr view returned non-JSON", reason="PR_READ_FAILED") from exc
        merged = bool(data.get("mergedAt"))
        merge_commit = data.get("mergeCommit") or {}
        return PullRequest(
            number=int(data.get("number") or number),
            repo=repo,
            head_branch=str(data.get("headRefName") or ""),
            base_branch=str(data.get("baseRefName") or ""),
            title=str(data.get("title") or ""),
            body=str(data.get("body") or ""),
            state="MERGED" if merged else str(data.get("state") or "").upper(),
            draft=bool(data.get("isDraft")),
            merged=merged,
            mission_id=_mission_from_body(str(data.get("body") or "")),
            comments=[],
            html_url=str(data.get("url") or ""),
            head_sha=str(data.get("headRefOid") or "").lower(),
            merge_commit_sha=str(merge_commit.get("oid") or "").lower() if isinstance(merge_commit, dict) else "",
        )

    def active_prs(self, repo: str, branch: str, mission_id: str) -> list[PullRequest]:
        out = self._require(["pr", "list", "-R", repo, "--head", branch, "--state", "open",
                             "--json", "number"], reason="PR_READ_FAILED")
        try:
            numbers = [int(item["number"]) for item in json.loads(out or "[]")]
        except (ValueError, KeyError, TypeError) as exc:
            raise GitCustodianRefused("gh pr list returned non-JSON", reason="PR_READ_FAILED") from exc
        found = [self.get(repo, n) for n in numbers]
        return [pr for pr in found if pr is not None and pr.mission_id == mission_id]

    def mark_ready(self, pr: PullRequest) -> PullRequest:
        self.ready_calls.append({"number": pr.number})
        self._require(["pr", "ready", str(pr.number), "-R", pr.repo], reason="PR_READY_FAILED")
        refreshed = self.get(pr.repo, pr.number)
        return refreshed or pr

    def promote_merge(self, pr: PullRequest, *, match_head_sha: str,
                      method: str = "merge", authorization_ref: str = "") -> dict[str, Any]:
        self.promote_calls.append({
            "number": pr.number, "match_head_sha": match_head_sha,
            "method": method, "authorization_ref": authorization_ref,
        })
        if not authorization_ref.startswith("governance:"):
            raise GitCustodianRefused(
                "a merge without a Governance authorization is AUTO_MERGE",
                {"number": pr.number}, reason="AUTO_MERGE")
        if method != "merge":
            raise GitCustodianRefused(
                "only a true merge commit is a promotion", {"method": method},
                reason="REWRITE_HISTORY")
        if pr.merged:
            raise GitCustodianRefused("PR is already merged", {"number": pr.number},
                                      reason="PR_ALREADY_MERGED")
        result = self._gh(["pr", "merge", str(pr.number), "-R", pr.repo, "--merge",
                           "--match-head-commit", match_head_sha])
        if result.returncode != 0:
            err = redact_text(result.stderr or result.stdout)
            reason = "MATCH_HEAD_MISMATCH" if "head" in err.lower() and "match" in err.lower() else "MERGE_FAILED"
            raise GitCustodianRefused("gh pr merge refused", {"stderr": err}, reason=reason)
        after = self.get(pr.repo, pr.number)
        if after is None or not after.merged or not after.merge_commit_sha:
            raise GitCustodianRefused(
                "merge reported success but the PR does not read back as merged",
                {"number": pr.number}, reason="MERGE_NOT_CONFIRMED")
        return {"merge_sha": after.merge_commit_sha, "method": method}

    # -- everything else stays structurally forbidden ---------------------
    def create(self, *args: Any, **kwargs: Any) -> PullRequest:
        raise GitCustodianRefused(
            "PR_CREATE is not part of the promotion surface; Git Custodian V1 owns it",
            reason="PR_CREATE_FORBIDDEN")

    def update(self, pr: PullRequest, **fields: Any) -> PullRequest:
        raise GitCustodianRefused("PR_UPDATE is not part of the promotion surface",
                                  reason="PR_UPDATE_FORBIDDEN")

    def comment(self, pr: PullRequest, body: str) -> None:
        raise GitCustodianRefused("comments are not authorized", {"number": pr.number},
                                  reason="COMMENT_FORBIDDEN")

    def merge(self, pr: PullRequest) -> None:
        self.merge_calls.append({"number": pr.number})
        raise GitCustodianRefused("AUTO_MERGE is forbidden", {"number": pr.number},
                                  reason="AUTO_MERGE")

    def create_tag(self, *args: Any, **kwargs: Any) -> None:
        raise GitCustodianRefused("tag writes are structurally forbidden", reason="TAG_WRITE")

    def deploy(self, *args: Any, **kwargs: Any) -> None:
        raise GitCustodianRefused("production deployment is structurally forbidden",
                                  reason="PRODUCTION_DEPLOY")


class GhCIAdapter:
    backend_name = "gh.ci"

    def __init__(self, runner: Runner | None = None):
        self._run = runner or _default_runner
        self.command_log: list[list[str]] = []

    def _api(self, path: str) -> Any:
        argv = ["gh", "api", path]
        self.command_log.append(list(argv))
        result = self._run(argv)
        if result.returncode != 0:
            raise GitCustodianRefused(
                "gh api failed", {"path": path, "stderr": redact_text(result.stderr)},
                reason="CI_READ_FAILED")
        try:
            return json.loads(result.stdout or "{}")
        except ValueError as exc:
            raise GitCustodianRefused("gh api returned non-JSON", reason="CI_READ_FAILED") from exc

    def snapshot(self, repo: str, sha: str) -> CISnapshot:
        runs = self._api(f"repos/{repo}/commits/{sha}/check-runs")
        status = self._api(f"repos/{repo}/commits/{sha}/status")
        checks = [dict(item) for item in (runs.get("check_runs") or []) if isinstance(item, dict)]
        statuses = [dict(item) for item in (status.get("statuses") or []) if isinstance(item, dict)]
        return CISnapshot(status=summarize_ci(checks, statuses), checks=checks, statuses=statuses)


__all__ = ["GhCIAdapter", "GhPRAdapter"]
