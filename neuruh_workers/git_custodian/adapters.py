"""Pluggable PR and CI adapters. V1 ships mocks; no live GitHub writes."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from .constants import CI_STATES, PR_BODY_SECTIONS
from .errors import GitCustodianRefused


@dataclass
class PullRequest:
    number: int
    repo: str
    head_branch: str
    base_branch: str
    title: str
    body: str
    state: str
    draft: bool
    merged: bool
    mission_id: str
    comments: list[dict[str, Any]] = field(default_factory=list)
    html_url: str = ""
    head_sha: str = ""
    merge_commit_sha: str = ""


def render_pr_body(candidate: Mapping[str, Any], court: Mapping[str, Any],
                   *, base_head: str) -> str:
    meta = candidate["pr_body_metadata"]
    values = {
        "MISSION": candidate["mission_id"],
        "STAGE": candidate["task_id"],
        "SOURCE_HEAD": candidate["expected_local_head"],
        "BASE_HEAD": base_head,
        "COURT_VERDICT": court.get("independent_court", ""),
        "TEST_SUMMARY": str(meta.get("TEST_SUMMARY", "")).strip(),
        "RECEIPT_REFERENCE": str(
            meta.get("RECEIPT_REFERENCE") or candidate["court_receipt_ref"]
        ).strip(),
        "KNOWN_RESIDUAL_RISKS": str(meta.get("KNOWN_RESIDUAL_RISKS", "")).strip(),
    }
    missing = [k for k in PR_BODY_SECTIONS if not values.get(k)]
    if missing:
        raise GitCustodianRefused(
            "PR body is missing required sections",
            {"missing": missing}, reason="MALFORMED_INPUT")
    return "\n".join(f"{key}: {values[key]}" for key in PR_BODY_SECTIONS)


class MockPRAdapter:
    backend_name = "mock.pr"

    def __init__(self, prs: Sequence[PullRequest] | None = None):
        self.prs: list[PullRequest] = list(prs or [])
        self.create_calls: list[dict[str, Any]] = []
        self.update_calls: list[dict[str, Any]] = []
        self.comment_calls: list[dict[str, Any]] = []
        self.merge_calls: list[dict[str, Any]] = []
        self.promote_calls: list[dict[str, Any]] = []
        self.ready_calls: list[dict[str, Any]] = []
        self.creates = 0
        # PROMOTION court hook: performs the actual merge (e.g. in a scratch
        # clone against a bare remote) and returns the merge commit sha. None
        # => a synthetic sha, for courts that only need the contract.
        self.merge_fn = None

    def active_prs(self, repo: str, branch: str, mission_id: str) -> list[PullRequest]:
        return [
            pr for pr in self.prs
            if pr.repo == repo and pr.head_branch == branch
            and pr.mission_id == mission_id
            and pr.state == "OPEN" and not pr.merged
        ]

    def create(self, *, repo: str, title: str, body: str, head: str, base: str,
               mission_id: str, draft: bool = True,
               head_sha: str = "") -> PullRequest:
        self.creates += 1
        self.create_calls.append({
            "repo": repo, "title": title, "head": head, "base": base,
            "mission_id": mission_id, "draft": draft,
        })
        pr = PullRequest(
            number=len(self.prs) + 1,
            repo=repo, head_branch=head, base_branch=base,
            title=title, body=body, state="OPEN", draft=draft, merged=False,
            mission_id=mission_id, comments=[],
            html_url=f"mock://{repo}/pull/{len(self.prs) + 1}",
            head_sha=head_sha,
        )
        self.prs.append(pr)
        return pr

    def update(self, pr: PullRequest, **fields: Any) -> PullRequest:
        if "draft" in fields and fields["draft"] != pr.draft:
            raise GitCustodianRefused(
                "refusing to change PR draft state",
                {"number": pr.number}, reason="DRAFT_PRESERVE")
        self.update_calls.append({"number": pr.number, "fields": dict(fields)})
        for key, value in fields.items():
            if key == "draft":
                continue
            setattr(pr, key, value)
        return pr

    def comment(self, pr: PullRequest, body: str) -> None:
        self.comment_calls.append({"number": pr.number, "body": body})
        raise GitCustodianRefused(
            "comments are not authorized in git-custodian v1",
            {"number": pr.number}, reason="COMMENT_FORBIDDEN")

    def merge(self, pr: PullRequest) -> None:
        self.merge_calls.append({"number": pr.number})
        raise GitCustodianRefused(
            "AUTO_MERGE is forbidden",
            {"number": pr.number}, reason="AUTO_MERGE")

    # -- PROMOTION EXECUTOR surface (governed; not AUTO_MERGE) -------------
    def get(self, repo: str, number: int) -> PullRequest | None:
        for pr in self.prs:
            if pr.repo == repo and pr.number == number:
                return pr
        return None

    def mark_ready(self, pr: PullRequest) -> PullRequest:
        self.ready_calls.append({"number": pr.number})
        pr.draft = False
        return pr

    def promote_merge(self, pr: PullRequest, *, match_head_sha: str,
                      method: str = "merge", authorization_ref: str = "") -> dict[str, Any]:
        """Merge exactly ``match_head_sha`` of this PR, or refuse.

        ``authorization_ref`` must name a Governance authorization; without one
        this is an AUTO_MERGE and refuses the same way ``merge`` does.
        """
        self.promote_calls.append({
            "number": pr.number, "match_head_sha": match_head_sha,
            "method": method, "authorization_ref": authorization_ref,
        })
        if not authorization_ref.startswith("governance:"):
            raise GitCustodianRefused(
                "a merge without a Governance authorization is AUTO_MERGE",
                {"number": pr.number}, reason="AUTO_MERGE")
        if pr.merged:
            raise GitCustodianRefused(
                "PR is already merged", {"number": pr.number},
                reason="PR_ALREADY_MERGED")
        if pr.head_sha != match_head_sha:
            raise GitCustodianRefused(
                "PR head moved since authorization; refusing to merge a "
                "different commit", {"number": pr.number,
                                     "head": pr.head_sha,
                                     "expected": match_head_sha},
                reason="MATCH_HEAD_MISMATCH")
        if method != "merge":
            raise GitCustodianRefused(
                "only a true merge commit is a promotion; squash/rebase "
                "rewrite the courted history", {"method": method},
                reason="REWRITE_HISTORY")
        if self.merge_fn is not None:
            merge_sha = self.merge_fn(pr)
        else:
            import hashlib
            merge_sha = hashlib.sha1(
                f"merge:{pr.repo}:{pr.number}:{match_head_sha}".encode()).hexdigest()
        pr.merged = True
        pr.state = "MERGED"
        pr.merge_commit_sha = merge_sha
        return {"merge_sha": merge_sha, "method": method}

    def create_tag(self, *args: Any, **kwargs: Any) -> None:
        raise GitCustodianRefused(
            "tag writes are structurally forbidden", reason="TAG_WRITE")

    def deploy(self, *args: Any, **kwargs: Any) -> None:
        raise GitCustodianRefused(
            "production deployment is structurally forbidden",
            reason="PRODUCTION_DEPLOY")


@dataclass
class CISnapshot:
    status: str
    checks: list[dict[str, Any]]
    statuses: list[dict[str, Any]]


def summarize_ci(check_runs: Sequence[Mapping[str, Any]],
                 statuses: Sequence[Mapping[str, Any]]) -> str:
    if not check_runs and not statuses:
        return "NOT_CONFIGURED_OR_NO_RUNS"

    def conclusion_of(item: Mapping[str, Any]) -> str:
        raw = str(item.get("conclusion") or item.get("state")
                  or item.get("status") or "").lower()
        if raw in ("failure", "failed", "error"):
            return "FAIL"
        if raw in ("success", "pass", "passed"):
            return "PASS"
        if raw in ("cancelled", "canceled"):
            return "CANCELLED"
        if raw in ("in_progress", "running"):
            return "RUNNING"
        if raw in ("queued", "pending", "waiting", "requested", "waiting_for_review"):
            return "PENDING"
        if raw in ("completed",):
            return "PASS" if str(item.get("conclusion", "")).lower() in (
                "success", "pass", "passed") else "UNKNOWN"
        return "UNKNOWN"

    ranks = {
        "FAIL": 0, "CANCELLED": 1, "RUNNING": 2, "PENDING": 3,
        "UNKNOWN": 4, "PASS": 5,
    }
    outcomes = [conclusion_of(item) for item in list(check_runs) + list(statuses)]
    worst = min(outcomes, key=lambda s: ranks.get(s, 4))
    if worst not in CI_STATES:
        return "UNKNOWN"
    return worst


class MockCIAdapter:
    backend_name = "mock.ci"

    def __init__(self, check_runs: Sequence[Mapping[str, Any]] | None = None,
                 statuses: Sequence[Mapping[str, Any]] | None = None):
        self.check_runs = list(check_runs or [])
        self.statuses = list(statuses or [])

    def snapshot(self, repo: str, sha: str) -> CISnapshot:
        checks = [dict(item) for item in self.check_runs]
        statuses = [dict(item) for item in self.statuses]
        for item in checks:
            item.setdefault("commit_sha", sha)
        for item in statuses:
            item.setdefault("commit_sha", sha)
        return CISnapshot(
            status=summarize_ci(checks, statuses),
            checks=checks, statuses=statuses,
        )
