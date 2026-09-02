"""PROMOTION EXECUTOR — the mechanical seam between a courted candidate and canon.

Git Custodian V1 stops at ``promotion.ready`` ("evidence, not merge authority")
and forbids AUTO_MERGE.  That left every passed candidate parked as a draft
PR: there was no executor for an *authorized* promotion.  This is that
executor, and it is mechanical on purpose — it decides nothing, it verifies
four things and then performs one exact git act:

    Sentinel PASS          sealed independent_sentinel_court, bound to the head
  + Governance ALLOW       a FRESH /v2/authorize for THIS merge (nonce-bound,
                           precondition-bound, spent once; never cached)
  + Honey Pot custody      promotion.ready for this repo/branch/head/PR; PR open,
                           head unchanged, base unchanged since the court
  + head unchanged         re-checked immediately before the merge AND enforced
                           by the merge itself (match-head-commit)
  -> authorized merge -> verify the resulting SHA -> deploy child event if required

Honey Pot remains custody (this lives inside it and performs the git act).
Sentinel remains court (its sealed record is verified here, never produced).
Bob remains orchestration (this is invoked; it never decides when).
Governance remains authority (asked for every promotion, believed, never bypassed).

AUTO_MERGE stays forbidden.  PROMOTION_MERGE is a different action: it does
not exist without the four gates above and is granted only by a separate
founder policy (``founder.promotion.v1``).  A merge is not idempotent, so a
verification failure after the merge is reported as MERGED_UNVERIFIED and is
never retried by this module.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Protocol, Sequence

from ..contracts import ReceiptType, content_sha, deterministic_job_id
from ..ledger import ExecutionLedger
from .adapters import CISnapshot, PullRequest
from .authority import assert_action_allowed, is_protected_branch
from .constants import (
    AUTHORITY_SCHEMA, FORBIDDEN_ACTIONS, OPTIONAL_PROMOTION_FIELDS,
    PROMOTION_ALLOWED_ACTIONS, PROMOTION_BASE_BRANCHES, PROMOTION_INPUT,
    PROMOTION_SCHEMA, REQUIRED_PROMOTION_FIELDS, WRITE_BAN_REPOS,
)
from .contract import require_hex40
from .errors import GitCustodianRefused
from .redact import assert_no_secrets, collect_secrets, redact_any, redact_text
from .switchboard import (
    RecordingSwitchboard, accept_promotion_request, promotion_from_envelope,
)

GOVERNANCE_AUTHORIZE_URL = "http://127.0.0.1:8848/v2/authorize"
FACTS_SCHEMA = "gov.action.facts.v1"
AUTHORIZATION_SCHEMA = "gov.authorization.v1"
NOT_CLAIMED = "NOT_CLAIMED_BY_GOVERNANCE"
COMPONENT = "git_custodian"
DEFAULT_LANE = "honey-pot-promotion"
MERGE_ACTION_CLASS = "merge"
SENTINEL_RECORD_KIND = "independent_sentinel_court"
CUSTODY_RECORD_KIND = "honey_pot_custody"
RISK_TIERS = ("R0", "R1", "R2", "R3", "R4", "R5")
PROMOTION_MERGE_METHOD = "merge"

# The founder policy that grants a promotion. It allows exactly the four
# promotion actions and must still name every v1-forbidden action as
# forbidden: AUTO_MERGE remains forbidden under it.
FOUNDER_PROMOTION_V1: dict[str, Any] = {
    "schema_version": AUTHORITY_SCHEMA,
    "policy_id": "founder.promotion.v1",
    "allowed": PROMOTION_ALLOWED_ACTIONS,
    "forbidden": FORBIDDEN_ACTIONS,
}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _yn(flag: bool) -> str:
    return "YES" if flag else "NO"


# --------------------------------------------------------------------------- #
# Contract


def parse_promotion(raw: Any, *, secrets: tuple[str, ...] = ()) -> dict[str, Any]:
    """Parse and fail-closed validate nrh.git.promotion.v1."""
    if not isinstance(raw, Mapping):
        raise GitCustodianRefused("promotion must be an object", reason="MALFORMED_INPUT")
    missing = [f for f in REQUIRED_PROMOTION_FIELDS if f not in raw]
    if missing:
        raise GitCustodianRefused(
            "promotion is missing required fields", {"missing": missing},
            reason="MALFORMED_INPUT")
    extra = [k for k in raw
             if k not in REQUIRED_PROMOTION_FIELDS and k not in OPTIONAL_PROMOTION_FIELDS]
    if extra:
        raise GitCustodianRefused(
            "promotion has unknown fields", {"unknown": extra}, reason="MALFORMED_INPUT")
    if raw["schema_version"] != PROMOTION_SCHEMA:
        raise GitCustodianRefused(
            "unsupported promotion schema_version",
            {"schema_version": redact_text(str(raw["schema_version"]), secrets)},
            reason="MALFORMED_INPUT")

    def nonempty_str(field: str) -> str:
        value = raw[field]
        if not isinstance(value, str) or not value.strip():
            raise GitCustodianRefused(
                f"{field} must be a non-empty string", {"field": field},
                reason="MALFORMED_INPUT")
        return value.strip()

    pr_number = raw["pr_number"]
    if isinstance(pr_number, bool) or not isinstance(pr_number, int) or pr_number <= 0:
        raise GitCustodianRefused(
            "pr_number must be a positive integer", {"field": "pr_number"},
            reason="MALFORMED_INPUT")
    deploy_required = raw.get("deploy_required", False)
    if deploy_required not in (True, False):
        raise GitCustodianRefused(
            "deploy_required must be a boolean", {"field": "deploy_required"},
            reason="MALFORMED_INPUT")
    for opt in ("founder_approval", "grant"):
        if raw.get(opt) is not None and not isinstance(raw[opt], Mapping):
            raise GitCustodianRefused(
                f"{opt} must be an object when provided", {"field": opt},
                reason="MALFORMED_INPUT")
    deploy_target = raw.get("deploy_target")
    if deploy_target is not None and (not isinstance(deploy_target, str) or not deploy_target.strip()):
        raise GitCustodianRefused(
            "deploy_target must be a non-empty string when provided",
            {"field": "deploy_target"}, reason="MALFORMED_INPUT")
    lane = raw.get("lane") or DEFAULT_LANE
    if not isinstance(lane, str) or not lane.strip() or "*" in lane:
        raise GitCustodianRefused("lane must be an exact string", {"field": "lane"},
                                  reason="MALFORMED_INPUT")

    parsed = {
        "schema_version": PROMOTION_SCHEMA,
        "mission_id": nonempty_str("mission_id"),
        "task_id": nonempty_str("task_id"),
        "repo": nonempty_str("repo"),
        "worktree": nonempty_str("worktree"),
        "branch": nonempty_str("branch"),
        "base_branch": nonempty_str("base_branch"),
        "expected_head": require_hex40(raw["expected_head"], "expected_head"),
        "expected_base_head": require_hex40(raw["expected_base_head"], "expected_base_head"),
        "remote_name": nonempty_str("remote_name"),
        "pr_number": int(pr_number),
        "sentinel_receipt_ref": nonempty_str("sentinel_receipt_ref"),
        "sentinel_receipt_hash": nonempty_str("sentinel_receipt_hash"),
        "custody_receipt_ref": nonempty_str("custody_receipt_ref"),
        "custody_receipt_hash": nonempty_str("custody_receipt_hash"),
        "authority_ref": nonempty_str("authority_ref"),
        "idempotency_key": nonempty_str("idempotency_key"),
        "founder_approval": dict(raw["founder_approval"]) if raw.get("founder_approval") else None,
        "grant": dict(raw["grant"]) if raw.get("grant") else None,
        "deploy_required": bool(deploy_required),
        "deploy_target": deploy_target.strip() if deploy_target else None,
        "lane": lane.strip(),
    }
    if parsed["remote_name"].startswith("http://") or "://" in parsed["remote_name"]:
        raise GitCustodianRefused(
            "remote_name must be a local remote name, not a URL",
            {"field": "remote_name"}, reason="MALFORMED_INPUT")
    if parsed["base_branch"] not in PROMOTION_BASE_BRANCHES:
        raise GitCustodianRefused(
            "promotion base must be a canonical branch",
            {"base_branch": parsed["base_branch"], "allowed": sorted(PROMOTION_BASE_BRANCHES)},
            reason="BASE_NOT_CANONICAL")
    if is_protected_branch(parsed["branch"]) or parsed["branch"] == parsed["base_branch"]:
        raise GitCustodianRefused(
            "candidate branch must be a feature branch, never a protected one",
            {"branch": parsed["branch"]}, reason="PROTECTED_BRANCH")
    if parsed["repo"] in WRITE_BAN_REPOS:
        raise GitCustodianRefused(
            "writes to this repository are forbidden; nothing is promoted into it",
            {"repo": parsed["repo"]}, reason="WRITE_BAN")
    if parsed["expected_head"] == parsed["expected_base_head"]:
        raise GitCustodianRefused(
            "candidate head equals base head; nothing to promote",
            reason="NOTHING_TO_PROMOTE")
    return parsed


def verify_promotion_authority(policy: Any, *, candidate_branch: str,
                               base_branch: str) -> dict[str, Any]:
    """founder.promotion.v1 (or an equivalent shape) — never a founder candidate policy."""
    if policy is None:
        raise GitCustodianRefused("authority receipt is missing", reason="AUTHORITY_MISSING")
    if not isinstance(policy, dict):
        raise GitCustodianRefused(
            "authority must be a structured policy object", reason="AUTHORITY_INVALID")
    allowed = tuple(policy.get("allowed") or ())
    forbidden = tuple(policy.get("forbidden") or ())
    if not allowed:
        raise GitCustodianRefused("authority lists no allowed actions", reason="AUTHORITY_INVALID")
    missing_forbid = [a for a in FORBIDDEN_ACTIONS if a not in forbidden]
    if missing_forbid:
        raise GitCustodianRefused(
            "authority is incomplete; forbidden actions must be explicit",
            {"missing_forbidden": missing_forbid}, reason="AUTHORITY_INCOMPLETE")
    for action in FORBIDDEN_ACTIONS:
        if action in allowed:
            raise GitCustodianRefused(
                "authority tries to allow a forbidden action (AUTO_MERGE is not a promotion)",
                {"action": action}, reason="AUTHORITY_INVALID")
    for needed in PROMOTION_ALLOWED_ACTIONS:
        if needed not in allowed:
            raise GitCustodianRefused(
                "authority does not grant a required promotion action",
                {"missing": needed}, reason="AUTHORITY_MISSING")
    if is_protected_branch(candidate_branch):
        raise GitCustodianRefused(
            "candidate branch is protected", {"branch": candidate_branch},
            reason="PROTECTED_BRANCH")
    if base_branch not in PROMOTION_BASE_BRANCHES:
        raise GitCustodianRefused(
            "promotion base is not canonical", {"base_branch": base_branch},
            reason="BASE_NOT_CANONICAL")
    return {
        "schema_version": policy.get("schema_version", AUTHORITY_SCHEMA),
        "policy_id": policy.get("policy_id", "unknown"),
        "allowed": allowed,
        "forbidden": forbidden,
    }


def verify_sentinel_record(record: Any, promotion: Mapping[str, Any]) -> dict[str, Any]:
    """A sealed independent Sentinel PASS bound to THIS head. A bare string is not a court."""
    if record is None:
        raise GitCustodianRefused(
            "Sentinel record is missing", {"ref": promotion["sentinel_receipt_ref"]},
            reason="SENTINEL_MISSING")
    if not isinstance(record, dict):
        raise GitCustodianRefused(
            "Sentinel evidence must be a sealed record; a bare PASS string is not a court",
            {"type": type(record).__name__}, reason="SENTINEL_INVALID")
    required = ("kind", "source_role", "independent", "verdict", "candidate_sha", "court_id")
    missing = [k for k in required if k not in record]
    if missing:
        raise GitCustodianRefused(
            "Sentinel record is incomplete", {"missing": missing}, reason="SENTINEL_INVALID")
    if (record["kind"] != SENTINEL_RECORD_KIND or record["source_role"] != "SENTINEL"
            or record["independent"] is not True):
        raise GitCustodianRefused(
            "Sentinel record is not an independent Sentinel court",
            {"kind": record.get("kind"), "source_role": record.get("source_role")},
            reason="SENTINEL_NOT_INDEPENDENT")
    verdict = record["verdict"]
    exceptions = list(record.get("exceptions") or [])
    repairs = list(record.get("required_repairs") or [])
    passed = verdict == "PASS" or (verdict == "PASS_WITH_EXCEPTIONS" and not exceptions)
    if not passed or repairs:
        # Never invert FAIL, never promote over open exceptions or repairs.
        raise GitCustodianRefused(
            "Sentinel verdict is not a clean PASS",
            {"verdict": verdict, "exceptions": len(exceptions), "required_repairs": len(repairs)},
            reason="SENTINEL_NOT_PASS")
    if str(record["candidate_sha"]).lower() != promotion["expected_head"]:
        raise GitCustodianRefused(
            "Sentinel courted a different head than the one being promoted",
            {"courted": record["candidate_sha"], "expected": promotion["expected_head"]},
            reason="SENTINEL_HEAD_MISMATCH")
    mission = record.get("mission_id")
    if mission and mission != promotion["mission_id"]:
        raise GitCustodianRefused(
            "Sentinel record belongs to a different mission",
            {"court": mission, "promotion": promotion["mission_id"]},
            reason="SENTINEL_MISSION_MISMATCH")
    if content_sha(record) != promotion["sentinel_receipt_hash"]:
        raise GitCustodianRefused(
            "sentinel_receipt_hash does not match the sealed record",
            reason="SENTINEL_HASH_MISMATCH")
    return dict(record)


def verify_custody_record(record: Any, promotion: Mapping[str, Any]) -> dict[str, Any]:
    """The Honey Pot's own promotion.ready custody for this exact repo/branch/head/PR."""
    if record is None:
        raise GitCustodianRefused(
            "Honey Pot custody record is missing",
            {"ref": promotion["custody_receipt_ref"]}, reason="CUSTODY_MISSING")
    if not isinstance(record, dict):
        raise GitCustodianRefused(
            "custody evidence must be a structured record", reason="CUSTODY_INVALID")
    if record.get("kind") != CUSTODY_RECORD_KIND:
        raise GitCustodianRefused(
            "custody record is not Honey Pot custody", {"kind": record.get("kind")},
            reason="CUSTODY_INVALID")
    if record.get("promotion_ready") is not True:
        raise GitCustodianRefused(
            "Honey Pot never emitted promotion.ready for this candidate",
            reason="CUSTODY_NOT_READY")
    checks = (("repo", promotion["repo"]), ("branch", promotion["branch"]),
              ("head", promotion["expected_head"]), ("pr", promotion["pr_number"]))
    for key, expected in checks:
        if record.get(key) != expected:
            raise GitCustodianRefused(
                f"custody record {key} does not match the promotion",
                {"custody": record.get(key), "promotion": expected},
                reason="CUSTODY_MISMATCH")
    if content_sha(record) != promotion["custody_receipt_hash"]:
        raise GitCustodianRefused(
            "custody_receipt_hash does not match the custody record",
            reason="CUSTODY_HASH_MISMATCH")
    return dict(record)


def custody_record_for(*, repo: str, branch: str, head: str, pr: int,
                       ci_status: str = "PASS", job_id: str = "") -> dict[str, Any]:
    """Shape the Honey Pot's promotion.ready custody takes (helper for producers)."""
    return {
        "kind": CUSTODY_RECORD_KIND,
        "repo": repo, "branch": branch, "head": head, "pr": pr,
        "promotion_ready": True, "ci_status": ci_status, "job_id": job_id,
    }


# --------------------------------------------------------------------------- #
# Governance


def precondition_hash(promotion: Mapping[str, Any]) -> str:
    """Binds the authorization to the exact heads it was asked about."""
    return content_sha({
        "repo": promotion["repo"],
        "base_branch": promotion["base_branch"],
        "expected_base_head": promotion["expected_base_head"],
        "branch": promotion["branch"],
        "expected_head": promotion["expected_head"],
        "pr_number": promotion["pr_number"],
    })


def promotion_nonce(promotion: Mapping[str, Any], issued_at: str) -> str:
    return "pn_" + content_sha({"k": promotion["idempotency_key"], "t": issued_at})[:29]


def promotion_facts(promotion: Mapping[str, Any], *, source_sha: str, nonce: str,
                    issued_at: str) -> dict[str, Any]:
    """The exact gov.action.facts.v1 for THIS merge. Governance classifies it (R3+)."""
    if not source_sha or len(source_sha) < 7:
        raise GitCustodianRefused(
            "promotion facts require the executor's attested source SHA",
            reason="SOURCE_SHA_UNATTESTED")
    repo = promotion["repo"]
    pr = promotion["pr_number"]
    return {
        "schema_version": FACTS_SCHEMA,
        "action_id": "promote-" + content_sha({
            "repo": repo, "pr": pr, "head": promotion["expected_head"]})[:24],
        "lane": promotion["lane"],
        "component": COMPONENT,
        "source_sha": source_sha,
        "action_class": MERGE_ACTION_CLASS,
        "summary": (f"promote {repo}#{pr} {promotion['branch']}@{promotion['expected_head'][:12]} "
                    f"into {promotion['base_branch']}"),
        "target": {"type": "pull_request", "id": f"{repo}#{pr}"},
        "repository": repo,
        "branch": promotion["base_branch"],
        "externality": "none",
        "reversibility": "rollback_known",
        "rollback_plan": f"git revert -m 1 <merge_sha> on {promotion['base_branch']}; reopen PR #{pr}",
        "blast_radius": "bounded",
        "production_effect": "none",
        "spend_usd": 0,
        "precondition_hash": precondition_hash(promotion),
        "nonce": nonce,
        "issued_at": issued_at,
    }


def verify_governance_allow(envelope: Any, facts: Mapping[str, Any], *,
                            secrets: Sequence[str] = ()) -> dict[str, Any]:
    """Accept only an ALLOW that is provably about THESE facts. Anything else refuses.

    Governance's reason text is external input; it is redacted before it can
    reach a refusal detail, a Switchboard event or the ledger.
    """
    def _r(value: Any) -> Any:
        return redact_any(value, tuple(secrets))
    if not isinstance(envelope, Mapping):
        raise GitCustodianRefused(
            "Governance answered with something other than an authorization envelope",
            reason="GOVERNANCE_INVALID")
    if envelope.get("schema_version") != AUTHORIZATION_SCHEMA:
        raise GitCustodianRefused(
            "unknown authorization schema", {"schema_version": envelope.get("schema_version")},
            reason="GOVERNANCE_INVALID")
    if envelope.get("execution") != NOT_CLAIMED:
        raise GitCustodianRefused(
            "Governance envelope claims execution; Governance decides, it never executes",
            {"execution": envelope.get("execution")}, reason="GOVERNANCE_CLAIMS_EXECUTION")
    if envelope.get("facts_nonce") != facts["nonce"]:
        # A stale, replayed or forged envelope is not an answer to this ask.
        raise GitCustodianRefused(
            "Governance envelope is not bound to this request's nonce",
            {"envelope_nonce": envelope.get("facts_nonce")}, reason="GOVERNANCE_ENVELOPE_UNBOUND")
    decision = envelope.get("decision")
    reason_code = redact_text(str(envelope.get("reason_code") or "UNSPECIFIED"), tuple(secrets))
    authorization_id = envelope.get("authorization_id")
    if not isinstance(authorization_id, str) or not authorization_id:
        raise GitCustodianRefused(
            "Governance envelope carries no authorization id", reason="GOVERNANCE_INVALID")
    if decision == "founder_required":
        prompt = envelope.get("founder_prompt")
        raise GitCustodianRefused(
            "Governance requires the founder for this promotion",
            {"reason_code": reason_code, "risk_tier": envelope.get("risk_tier"),
             "authorization_id": authorization_id,
             "founder_prompt": _r(dict(prompt)) if isinstance(prompt, Mapping) else None},
            reason="GOVERNANCE_FOUNDER_REQUIRED")
    if decision == "deny":
        raise GitCustodianRefused(
            "Governance denied this promotion",
            {"reason_code": reason_code, "risk_tier": envelope.get("risk_tier"),
             "authorization_id": authorization_id,
             "reasons": _r(list(envelope.get("reasons") or [])[:5])},
            reason="GOVERNANCE_DENY")
    if decision != "allow":
        raise GitCustodianRefused(
            "Governance decision vocabulary is unknown", {"decision": decision},
            reason="GOVERNANCE_INVALID")
    for key in ("action_class", "repository", "branch"):
        if envelope.get(key) != facts.get(key):
            raise GitCustodianRefused(
                "Governance allowed a different action than the one asked",
                {"field": key, "envelope": envelope.get(key), "facts": facts.get(key)},
                reason="GOVERNANCE_ENVELOPE_UNBOUND")
    if dict(envelope.get("target") or {}) != dict(facts["target"]):
        raise GitCustodianRefused(
            "Governance allowed a different target than the one asked",
            reason="GOVERNANCE_ENVELOPE_UNBOUND")
    if envelope.get("risk_tier") not in RISK_TIERS:
        raise GitCustodianRefused(
            "Governance allow carries no risk tier", reason="GOVERNANCE_INVALID")
    receipt = envelope.get("decision_receipt_hash")
    if not isinstance(receipt, str) or not receipt:
        raise GitCustodianRefused(
            "Governance allow carries no decision receipt; an unreceipted allow is not authority",
            reason="GOVERNANCE_INVALID")
    return dict(envelope)


class GovernanceAuthorizePort(Protocol):
    def authorize(self, facts: Mapping[str, Any], *, grant: Mapping[str, Any] | None = None,
                  founder_approval: Mapping[str, Any] | None = None) -> Mapping[str, Any]: ...


class LoopbackGovernanceAuthorize:
    """POST loopback /v2/authorize. Submits; never signs; never caches an allow."""

    def __init__(self, url: str = GOVERNANCE_AUTHORIZE_URL, *, timeout_s: float = 5.0,
                 opener: Callable[..., Any] | None = None):
        if not (url.startswith("http://127.0.0.1:") or url.startswith("http://localhost:")):
            raise GitCustodianRefused(
                "Governance must be dialled on loopback", {"url": url},
                reason="GOVERNANCE_NOT_LOOPBACK")
        self.url = url
        self.timeout_s = timeout_s
        self.opener = opener

    def authorize(self, facts: Mapping[str, Any], *, grant: Mapping[str, Any] | None = None,
                  founder_approval: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        from ..organ_consumers import http_post_json
        body: dict[str, Any] = {"facts": dict(facts)}
        if grant is not None:
            body["grant"] = dict(grant)
        if founder_approval is not None:
            body["founder_approval"] = dict(founder_approval)
        result = http_post_json(self.url, body, timeout_s=self.timeout_s, opener=self.opener)
        if not result.get("ok"):
            raise GitCustodianRefused(
                "Governance is unreachable or refused the request transport",
                {"status": result.get("status"), "error": result.get("error")},
                reason="GOVERNANCE_UNREACHABLE")
        return result.get("data") or {}


# --------------------------------------------------------------------------- #
# Executor


class PromotionExecutor:
    """Sentinel PASS + Governance ALLOW + Honey Pot custody + unchanged head -> merge -> verify."""

    def __init__(self, *, git: Any, pr: Any, ci: Any,
                 court_store: Any, custody_store: Any, authority_store: Any,
                 governance: GovernanceAuthorizePort,
                 switchboard: RecordingSwitchboard | None = None,
                 ledger: ExecutionLedger | None = None,
                 known_secrets: Sequence[str] | None = None,
                 source_sha: str = "",
                 clock: Callable[[], str] | None = None):
        self.git = git
        self.pr = pr
        self.ci = ci
        self.court_store = court_store
        self.custody_store = custody_store
        self.authority_store = authority_store
        self.governance = governance
        self.switchboard = switchboard or RecordingSwitchboard()
        self.ledger = ledger
        self.known_secrets = list(known_secrets or [])
        self.source_sha = (source_sha or "").lower()
        self.clock = clock or _now

    # -- entry points ----------------------------------------------------- #

    def handle(self, event: Any) -> dict[str, Any]:
        envelope = accept_promotion_request(event)
        return self.run(promotion_from_envelope(envelope))

    def run(self, raw_promotion: Any) -> dict[str, Any]:
        secrets = collect_secrets(self.known_secrets)
        promotion = parse_promotion(raw_promotion, secrets=secrets)
        job_id = deterministic_job_id(
            "promotion-executor", "1.0.0",
            {
                "mission_id": promotion["mission_id"],
                "repo": promotion["repo"],
                "pr_number": promotion["pr_number"],
                "expected_head": promotion["expected_head"],
                "idempotency_key": promotion["idempotency_key"],
            },
            promotion["idempotency_key"],
        )
        self._emit(PROMOTION_INPUT, {
            "job_id": job_id, "repo": promotion["repo"], "pr": promotion["pr_number"],
            "head": promotion["expected_head"], "base": promotion["base_branch"],
        }, job_id, ReceiptType.PROMOTION_REQUESTED)
        try:
            return self._run_gated(promotion, job_id, secrets)
        except GitCustodianRefused as exc:
            self._emit("promotion.refused", {
                "job_id": job_id, "reason": exc.reason,
                "detail": redact_any(dict(exc.detail), secrets),
            }, job_id, ReceiptType.PROMOTION_REFUSED)
            raise

    # -- the four gates ---------------------------------------------------- #

    def _run_gated(self, promotion: Mapping[str, Any], job_id: str,
                   secrets: tuple[str, ...]) -> dict[str, Any]:
        repo, branch, base = promotion["repo"], promotion["branch"], promotion["base_branch"]
        head, base_head = promotion["expected_head"], promotion["expected_base_head"]
        worktree, remote = promotion["worktree"], promotion["remote_name"]

        # Gate 0: the founder policy that even permits a promotion.
        authority = verify_promotion_authority(
            self.authority_store.get(promotion["authority_ref"]),
            candidate_branch=branch, base_branch=base)

        # Gate 1: Sentinel PASS, sealed, bound to this head.
        sentinel = verify_sentinel_record(
            self.court_store.get(promotion["sentinel_receipt_ref"]), promotion)

        # Gate 2: Honey Pot custody — promotion.ready for exactly this candidate,
        # and the PR it opened still says so.
        custody = verify_custody_record(
            self.custody_store.get(promotion["custody_receipt_ref"]), promotion)
        assert_action_allowed(authority, "PR_READ")
        pr = self.pr.get(repo, promotion["pr_number"])
        if pr is None:
            raise self._refuse("PR_MISSING", "the custody PR does not exist",
                               repo=repo, pr=promotion["pr_number"])
        if pr.merged:
            return self._already_promoted(promotion, pr, job_id, authority)
        if pr.state != "OPEN":
            raise self._refuse("PR_NOT_OPEN", "the custody PR is not open",
                               state=pr.state)
        if pr.head_branch != branch or pr.base_branch != base:
            raise self._refuse("PR_MISMATCH", "PR branches do not match the promotion",
                               pr_head=pr.head_branch, pr_base=pr.base_branch)
        if pr.mission_id and pr.mission_id != promotion["mission_id"]:
            raise self._refuse("PR_MISMATCH", "PR belongs to a different mission",
                               pr_mission=pr.mission_id)
        if pr.head_sha and pr.head_sha != head:
            raise self._refuse("HEAD_DRIFT", "PR head is not the courted head",
                               pr_head=pr.head_sha, expected=head)

        # Gate 3: the heads are what the court and the custody saw.
        self.git.fetch(worktree, remote)
        remote_branch = self.git.remote_head(worktree, remote, branch)
        if remote_branch != head:
            raise self._refuse("HEAD_DRIFT", "candidate branch moved since the court",
                               remote=remote_branch, expected=head)
        remote_base = self.git.remote_head(worktree, remote, base)
        if remote_base != base_head:
            raise self._refuse("BASE_DRIFT", "base branch moved since the court",
                               remote=remote_base, expected=base_head)
        assert_action_allowed(authority, "CI_READ")
        snapshot: CISnapshot = self.ci.snapshot(repo, head)
        if snapshot.status != "PASS":
            raise self._refuse("CI_NOT_PASS", "CI is not PASS for the courted head",
                               ci_status=snapshot.status)

        # Gate 4: Governance ALLOW for THIS merge, fresh, nonce- and precondition-bound.
        issued_at = self.clock()
        nonce = promotion_nonce(promotion, issued_at)
        facts = promotion_facts(promotion, source_sha=self.source_sha, nonce=nonce,
                                issued_at=issued_at)
        try:
            answer = self.governance.authorize(
                facts, grant=promotion.get("grant"),
                founder_approval=promotion.get("founder_approval"))
        except GitCustodianRefused:
            raise
        except Exception as exc:  # noqa: BLE001 — an unreachable authority refuses
            raise self._refuse("GOVERNANCE_UNREACHABLE", "Governance could not be asked",
                               cause=type(exc).__name__)
        envelope = verify_governance_allow(answer, facts, secrets=secrets)
        authorization_id = str(envelope["authorization_id"])
        self._emit("promotion.authorized", {
            "job_id": job_id, "authorization_id": authorization_id,
            "risk_tier": envelope.get("risk_tier"), "reason_code": envelope.get("reason_code"),
            "decision_receipt_hash": envelope.get("decision_receipt_hash"),
            "facts_nonce": nonce, "precondition_hash": facts["precondition_hash"],
            "approver_ids": list(envelope.get("approver_ids") or []),
        }, job_id, ReceiptType.PROMOTION_AUTHORIZED)

        # Head re-checked at the last instant, then enforced by the merge itself.
        self.git.fetch(worktree, remote)
        if self.git.remote_head(worktree, remote, branch) != head:
            raise self._refuse("HEAD_DRIFT", "candidate branch moved after authorization",
                               expected=head)
        if pr.draft:
            assert_action_allowed(authority, "PR_READY")
            pr = self.pr.mark_ready(pr)
        assert_action_allowed(authority, "PROMOTION_MERGE")
        merged = self.pr.promote_merge(
            pr, match_head_sha=head, method=PROMOTION_MERGE_METHOD,
            authorization_ref=f"governance:{authorization_id}")
        merge_sha = str(merged.get("merge_sha") or "").lower()
        self._emit("promotion.executed", {
            "job_id": job_id, "merge_sha": merge_sha, "pr": pr.number,
            "authorization_id": authorization_id, "method": PROMOTION_MERGE_METHOD,
        }, job_id, ReceiptType.PROMOTION_EXECUTED)

        # Verify the resulting SHA. A merge is not idempotent: a failure here is
        # MERGED_UNVERIFIED, reported, never retried.
        self.git.fetch(worktree, remote)
        base_after = self.git.remote_head(worktree, remote, base)
        pr_after = self.pr.get(repo, pr.number)
        verified = bool(
            merge_sha and base_after
            and (base_after == merge_sha or self.git.is_ancestor(worktree, merge_sha, base_after))
            and self.git.is_ancestor(worktree, head, base_after)
            and self.git.is_ancestor(worktree, base_head, base_after)
            and pr_after is not None and pr_after.merged
        )
        if not verified:
            raise self._refuse(
                "PROMOTION_VERIFY_FAILED",
                "MERGED_UNVERIFIED: the base branch does not provably contain the promotion",
                merge_sha=merge_sha, base_after=base_after,
                pr_merged=bool(pr_after and pr_after.merged))
        self._emit("promotion.verified", {
            "job_id": job_id, "merge_sha": merge_sha, "base_before": base_head,
            "base_after": base_after, "contains_head": True, "pr": pr.number,
        }, job_id, ReceiptType.PROMOTION_VERIFIED)

        deploy_child = None
        if promotion["deploy_required"]:
            deploy_child = {
                "job_id": job_id, "repo": repo, "sha": merge_sha,
                "target": promotion.get("deploy_target") or "",
                "mission_id": promotion["mission_id"],
                "note": "child request; no deploy is performed by the promotion executor",
            }
            self._emit("deploy.requested", deploy_child, job_id, ReceiptType.DEPLOY_REQUESTED)

        receipt = self._receipt(
            promotion, authority, sentinel, envelope, job_id,
            merge_sha=merge_sha, base_after=base_after, pr=pr,
            idempotency="APPLIED", deploy_child=deploy_child, drift="NO")
        assert_no_secrets(receipt, secrets, where="final_receipt")
        if self.ledger is not None:
            self.ledger.append(ReceiptType.JOB_SUCCEEDED, job_id, {
                "receipt": redact_any(receipt, secrets)})
        return receipt

    def _already_promoted(self, promotion: Mapping[str, Any], pr: PullRequest,
                          job_id: str, authority: Mapping[str, Any]) -> dict[str, Any]:
        """Replay: the PR is merged. Prove the base contains it; ask nothing, merge nothing."""
        worktree, remote = promotion["worktree"], promotion["remote_name"]
        self.git.fetch(worktree, remote)
        base_after = self.git.remote_head(worktree, remote, promotion["base_branch"])
        merge_sha = (pr.merge_commit_sha or "").lower()
        contained = bool(
            base_after
            and self.git.is_ancestor(worktree, promotion["expected_head"], base_after)
            and (not merge_sha or base_after == merge_sha
                 or self.git.is_ancestor(worktree, merge_sha, base_after)))
        if not contained:
            raise self._refuse(
                "PR_ALREADY_MERGED_UNVERIFIED",
                "the PR reports merged but the base does not provably contain the head",
                base_after=base_after, merge_sha=merge_sha)
        self._emit("promotion.verified", {
            "job_id": job_id, "merge_sha": merge_sha, "base_after": base_after,
            "contains_head": True, "pr": pr.number, "replay": True,
        }, job_id, ReceiptType.PROMOTION_VERIFIED)
        return self._receipt(
            promotion, authority, None, None, job_id,
            merge_sha=merge_sha, base_after=base_after, pr=pr,
            idempotency="PROVEN", deploy_child=None, drift="NO")

    # -- receipts ---------------------------------------------------------- #

    def _receipt(self, promotion, authority, sentinel, envelope, job_id, *,
                 merge_sha, base_after, pr, idempotency, deploy_child, drift) -> dict[str, Any]:
        return {
            "BACKEND": "+".join([
                getattr(self.git, "backend_name", "git"),
                getattr(self.pr, "backend_name", "pr"),
                getattr(self.ci, "backend_name", "ci"),
            ]),
            "AUTH_BACKEND": authority.get("policy_id", "unknown"),
            "SENTINEL_COURT": sentinel.get("court_id") if sentinel else "REPLAY",
            "GOVERNANCE_ALLOW": envelope.get("authorization_id") if envelope else "REPLAY",
            "GOVERNANCE_RISK_TIER": envelope.get("risk_tier") if envelope else "REPLAY",
            "GOVERNANCE_REASON": envelope.get("reason_code") if envelope else "REPLAY",
            "HONEY_POT_CUSTODY": "VALID",
            "HEAD_DRIFT": drift,
            "MERGE": "YES",
            "MERGED": "YES",
            "MERGE_SHA": merge_sha,
            "BASE_BEFORE": promotion["expected_base_head"],
            "BASE_AFTER": base_after,
            "PR": pr.number,
            "AUTO_MERGE": "NO",
            "FORCE_PUSH": "NO",
            "DIRECT_MAIN": "NO",
            "REWRITE_HISTORY": "NO",
            "TAG_WRITE": "NO",
            "DEPLOY": "NO",
            "DEPLOYED": "NO",
            "PRODUCTION_DEPLOY": "NO",
            "DEPLOY_CHILD_REQUESTED": _yn(deploy_child is not None),
            "IDEMPOTENCY": idempotency,
            "SOURCE_SHA": self.source_sha,
            "NEXT": ("deploy.requested child emitted; deploy is its own governed act"
                     if deploy_child else "PROMOTED; no deploy required"),
            "job_id": job_id,
        }

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
            redact_text(message, secrets), redact_any(detail, secrets), reason=reason)


__all__ = [
    "COMPONENT", "DEFAULT_LANE", "FOUNDER_PROMOTION_V1", "GovernanceAuthorizePort",
    "LoopbackGovernanceAuthorize", "PromotionExecutor", "custody_record_for",
    "parse_promotion", "precondition_hash", "promotion_facts", "promotion_nonce",
    "verify_custody_record", "verify_governance_allow", "verify_promotion_authority",
    "verify_sentinel_record",
]
