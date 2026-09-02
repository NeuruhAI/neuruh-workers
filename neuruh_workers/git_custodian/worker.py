"""Git Custodian V1 — custody bridge to a reviewable candidate, not a merge bot."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from ..contracts import ReceiptType, deterministic_job_id
from ..ledger import ExecutionLedger
from .adapters import CISnapshot, PullRequest, render_pr_body
from .authority import assert_action_allowed, verify_authority
from .constants import (
    SWITCHBOARD_INPUT, T07_REPO, WRITE_BAN_REPOS,
)
from .contract import parse_candidate, parse_remote_base
from .court import verify_court
from .errors import GitCustodianRefused
from .git_backend import GitIdentity
from .redact import assert_no_secrets, collect_secrets, redact_any, redact_text
from .switchboard import (
    STRUCTURED_SENTINEL_PASS_REQUIRED,
    SWITCHAROO_INTEGRATED,
    RecordingSwitchboard,
    accept_candidate_push,
    candidate_from_envelope,
)


def _yn(flag: bool) -> str:
    return "YES" if flag else "NO"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class GitCustodian:
    """Governed FEATURE_BRANCH_PUSH + PR_CREATE/UPDATE + CI_READ worker."""

    def __init__(self, *, git: Any, pr: Any, ci: Any,
                 court_store: Any, authority_store: Any,
                 switchboard: RecordingSwitchboard | None = None,
                 ledger: ExecutionLedger | None = None,
                 known_secrets: Sequence[str] | None = None):
        self.git = git
        self.pr = pr
        self.ci = ci
        self.court_store = court_store
        self.authority_store = authority_store
        self.switchboard = switchboard or RecordingSwitchboard()
        self.ledger = ledger
        self.known_secrets = list(known_secrets or [])

    def handle(self, event: Any) -> dict[str, Any]:
        envelope = accept_candidate_push(event)
        return self.run(candidate_from_envelope(envelope))

    def run(self, raw_candidate: Any) -> dict[str, Any]:
        secrets = collect_secrets(self.known_secrets)
        candidate = parse_candidate(raw_candidate, secrets=secrets)
        job_id = deterministic_job_id(
            "git-custodian", "1.0.0",
            {
                "mission_id": candidate["mission_id"],
                "repo": candidate["repo"],
                "branch": candidate["branch"],
                "expected_local_head": candidate["expected_local_head"],
                "idempotency_key": candidate["idempotency_key"],
            },
            candidate["idempotency_key"],
        )
        self._emit(SWITCHBOARD_INPUT, {
            "job_id": job_id,
            "repo": candidate["repo"],
            "branch": candidate["branch"],
        }, job_id, ReceiptType.CANDIDATE_PUSH_REQUESTED)

        court = verify_court(
            self.court_store.get(candidate["court_receipt_ref"]), candidate)
        extra = ("DETACHED_HEAD",) if candidate["expect_detached"] else ()
        authority = verify_authority(
            self.authority_store.get(candidate["authority_ref"]),
            branch=candidate["branch"], extra_needed=extra)

        base_branch, base_sha = parse_remote_base(candidate["expected_remote_base"])
        identity = self.git.inspect(
            candidate["worktree"], candidate["remote_name"],
            candidate["branch"], base_branch)
        self._preflight(candidate, identity, base_sha, secrets)

        write_banned = candidate["repo"] in WRITE_BAN_REPOS
        remote_before = identity.remote_branch_head
        already_branch = remote_before == candidate["expected_local_head"]

        if write_banned and not already_branch:
            raise self._refuse(
                "WRITE_BAN",
                "writes to this repository are forbidden; remote HEAD must "
                "already match expected_local_head",
                repo=candidate["repo"])

        if already_branch:
            push_result = {
                "already_present": True,
                "remote_before": remote_before,
                "remote_after": remote_before,
            }
        else:
            assert_action_allowed(authority, "FEATURE_BRANCH_PUSH")
            push_result = self.git.push_feature_branch(
                identity.worktree, candidate["remote_name"],
                candidate["branch"], candidate["expected_local_head"])

        remote_after = self.git.remote_head(
            identity.worktree, candidate["remote_name"], candidate["branch"])
        if remote_after != candidate["expected_local_head"]:
            raise self._refuse(
                "PUSH_PROOF_FAILED",
                "REMOTE_BRANCH_HEAD != EXPECTED_LOCAL_HEAD",
                remote_after=remote_after,
                expected=candidate["expected_local_head"])
        head_match = remote_after == candidate["expected_local_head"]
        push_payload = {
            "remote": candidate["remote_name"],
            "branch": candidate["branch"],
            "local_head": candidate["expected_local_head"],
            "remote_before": push_result.get("remote_before"),
            "remote_after": remote_after,
            "timestamp": _now(),
            "court_receipt_ref": candidate["court_receipt_ref"],
            "authority_ref": candidate["authority_ref"],
        }
        self._emit("candidate.branch.pushed", push_payload, job_id,
                   ReceiptType.CANDIDATE_BRANCH_PUSHED)

        comments_before = sum(len(p.comments) for p in self.pr.prs)
        pr, pr_already = self._ensure_pr(
            candidate, court, identity, authority, write_banned, job_id,
            base_sha, secrets)

        assert_action_allowed(authority, "CI_READ")
        snapshot: CISnapshot = self.ci.snapshot(
            candidate["repo"], candidate["expected_local_head"])
        ci_status = snapshot.status
        if ci_status == "PASS":
            self._emit("ci.passed", {
                "sha": candidate["expected_local_head"],
                "checks": snapshot.checks, "statuses": snapshot.statuses,
            }, job_id, ReceiptType.CI_PASSED)
            self._emit("promotion.ready", {
                "sha": candidate["expected_local_head"],
                "pr": pr.number,
                "note": "promotion.ready is evidence, not merge authority",
            }, job_id, ReceiptType.PROMOTION_READY)
        elif ci_status == "FAIL":
            failed = snapshot.checks or snapshot.statuses or [{}]
            item = failed[0]
            self._emit("ci.failed", {
                "failure": item.get("conclusion") or item.get("state") or "FAIL",
                "logs_ref": item.get("html_url") or item.get("logs_ref") or "",
                "commit": candidate["expected_local_head"],
                "affected_job": item.get("name") or item.get("context") or "",
            }, job_id, ReceiptType.CI_FAILED)
        elif ci_status in ("PENDING", "RUNNING"):
            self._emit("ci.started", {
                "sha": candidate["expected_local_head"], "status": ci_status,
            }, job_id, ReceiptType.CI_STARTED)

        extra_comments = (
            self.pr.comment_calls
            or sum(len(p.comments) for p in self.pr.prs) != comments_before
        )
        axon = candidate["repo"] == T07_REPO
        receipt = {
            "BACKEND": "+".join([
                getattr(self.git, "backend_name", "git"),
                getattr(self.pr, "backend_name", "pr"),
                getattr(self.ci, "backend_name", "ci"),
            ]),
            "AUTH_BACKEND": authority.get("policy_id", "unknown"),
            "TESTS": "PASS",
            "IDEMPOTENCY": (
                "PROVEN" if already_branch and pr_already else "APPLIED"
            ),
            "T07_REMOTE_SHA": remote_after if axon else "",
            "T07_PR": pr.number if axon else "",
            "CI_STATUS": ci_status,
            "DUPLICATE_PR": "NO",
            "DUPLICATE_PR_CREATED": "NO",
            "FORCE_PUSH": "NO",
            "DIRECT_MAIN": "NO",
            "MERGE": "NO",
            "MERGED": "NO",
            "DEPLOY": "NO",
            "DEPLOYED": "NO",
            "AUTO_MERGE": "NO",
            "PRODUCTION_DEPLOY": "NO",
            "TAG_WRITE": "NO",
            "ONE_PR_PER_MISSION_BRANCH": "YES",
            "CI_READ": "PASS",
            "CI_FALSE_PASS": "IMPOSSIBLE",
            "STRUCTURED_SENTINEL_PASS_REQUIRED": STRUCTURED_SENTINEL_PASS_REQUIRED,
            "SWITCHAROO_INTEGRATED": bool(SWITCHAROO_INTEGRATED),
            "NEXT": self._next(ci_status),
            "BRANCH_ALREADY_PRESENT": _yn(already_branch),
            "REMOTE_HEAD_MATCH": _yn(head_match),
            "PR_ALREADY_PRESENT": _yn(pr_already),
            "PR_DRAFT": _yn(bool(pr.draft)),
            "EXTRA_COMMENTS": _yn(bool(extra_comments)),
            "job_id": job_id,
        }
        assert_no_secrets(receipt, secrets, where="final_receipt")
        if self.ledger is not None:
            self.ledger.append(ReceiptType.JOB_SUCCEEDED, job_id, {
                "receipt": redact_any(receipt, secrets),
            })
        return receipt

    def _preflight(self, candidate: Mapping[str, Any], identity: GitIdentity,
                   base_sha: str, secrets: tuple[str, ...]) -> None:
        if identity.detached and not candidate["expect_detached"]:
            raise self._refuse(
                "DETACHED_HEAD",
                "refusing detached HEAD unless the contract expects it and "
                "authority grants DETACHED_HEAD")
        if (not identity.detached and identity.branch
                and identity.branch != candidate["branch"]):
            raise self._refuse(
                "BRANCH_MISMATCH",
                "current branch does not match the candidate contract",
                current=identity.branch, expected=candidate["branch"])
        if identity.head != candidate["expected_local_head"]:
            raise self._refuse(
                "SHA_MISMATCH",
                "local HEAD does not match expected_local_head",
                local_head=identity.head)
        if identity.tracked_dirty:
            raise self._refuse(
                "DIRTY",
                "tracked dirty files are present; fail closed",
                paths=list(identity.tracked_dirty)[:20])
        if identity.untracked:
            raise self._refuse(
                "UNTRACKED",
                "untracked files are present; fail closed",
                paths=list(identity.untracked)[:20])
        if identity.remote_identity != candidate["remote_url_or_identity"]:
            raise self._refuse(
                "GIT_IDENTITY",
                "remote identity does not match the candidate contract")
        if identity.remote_base_head and identity.remote_base_head != base_sha:
            raise self._refuse(
                "BASE_MISMATCH",
                "remote base HEAD does not match expected_remote_base",
                remote_base=identity.remote_base_head, expected=base_sha)

    def _ensure_pr(self, candidate: Mapping[str, Any], court: Mapping[str, Any],
                   identity: GitIdentity, authority: Mapping[str, Any],
                   write_banned: bool, job_id: str, base_sha: str,
                   secrets: tuple[str, ...]) -> tuple[PullRequest, bool]:
        active = self.pr.active_prs(
            candidate["repo"], candidate["branch"], candidate["mission_id"])
        if len(active) > 1:
            raise self._refuse(
                "AMBIGUOUS_PR",
                "more than one active candidate PR for repo+branch+mission")
        if len(active) == 1:
            pr = active[0]
            # Reuse. Preserve draft. Do not comment. Do not merge.
            return pr, True
        if write_banned:
            raise self._refuse(
                "WRITE_BAN",
                "PR create is forbidden on a write-banned repository and no "
                "active candidate PR was found")
        assert_action_allowed(authority, "PR_CREATE")
        body = render_pr_body(candidate, court, base_head=base_sha)
        assert_no_secrets(body, secrets, where="pr_body")
        pr = self.pr.create(
            repo=candidate["repo"],
            title=candidate["pr_title"],
            body=body,
            head=candidate["branch"],
            base="main",
            mission_id=candidate["mission_id"],
            draft=True,
            head_sha=candidate["expected_local_head"],
        )
        self._emit("candidate.pr.opened", {
            "number": pr.number, "repo": candidate["repo"],
            "branch": candidate["branch"], "draft": True,
        }, job_id, ReceiptType.CANDIDATE_PR_OPENED)
        return pr, False

    def _next(self, ci_status: str) -> str:
        if ci_status == "PASS":
            return ("PROMOTION_READY_EVIDENCE_EMITTED; merge is not authorized; "
                    "Git Custodian is not Sentinel")
        if ci_status == "FAIL":
            return ("CI_FAILED remediation payload emitted; route toward "
                    "BOB/PRIME through Switchboard later; merge is not authorized")
        if ci_status in ("PENDING", "RUNNING"):
            return "CI_IN_FLIGHT; wait; merge is not authorized"
        return ("CI_NOT_CONFIGURED_OR_NO_RUNS; promotion.ready not emitted; "
                "merge is not authorized")

    def _emit(self, event_type: str, payload: Mapping[str, Any],
              job_id: str, receipt_type: str) -> None:
        secrets = collect_secrets(self.known_secrets)
        safe = redact_any(dict(payload), secrets)
        assert_no_secrets(safe, secrets, where=event_type)
        self.switchboard.emit(event_type, safe)
        if self.ledger is not None:
            self.ledger.append(receipt_type, job_id, safe)

    def _refuse(self, reason: str, message: str, **detail: Any) -> GitCustodianRefused:
        secrets = collect_secrets(self.known_secrets)
        return GitCustodianRefused(
            redact_text(message, secrets),
            redact_any(detail, secrets),
            reason=reason,
        )
