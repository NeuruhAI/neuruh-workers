"""PROMOTION EXECUTOR court — the mechanical seam that consumes promotion.ready.

Sentinel PASS + Governance ALLOW + Honey Pot custody + unchanged head -> one
authorized merge -> verified SHA -> deploy child if required.  Every gate is
courted alone (refuses; asks Governance nothing; merges nothing), the golden
path is courted with mocks and with real git against a bare remote, and the
Switchboard hop that Bob would drive is courted end to end.
"""
from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import tempfile
import unittest

from neuruh_workers.contracts import ReceiptType, content_sha
from neuruh_workers.events import EventType
from neuruh_workers.git_custodian import (
    FOUNDER_CANDIDATE_V1,
    FOUNDER_PROMOTION_V1,
    GitCustodianRefused,
    GitIdentity,
    MemoryAuthorityStore,
    MemoryCourtStore,
    MockCIAdapter,
    MockPRAdapter,
    PromotionExecutor,
    PullRequest,
    RealGitBackend,
    RecordedGitBackend,
    RecordingSwitchboard,
    build_promotion_request_event,
    custody_record_for,
)
from neuruh_workers.git_custodian.gh_backend import GhPRAdapter
from neuruh_workers.git_custodian.promotion import (
    NOT_CLAIMED, precondition_hash, promotion_facts,
)
from neuruh_workers.switchboard import Switchboard
from tests.git_custodian_fixtures import git as gitcmd, make_seeded_pair, sha as gitsha
from tests.helpers import fixed_clock, make_stack

REPO = "NeuruhAI/fixture"
MISSION = "NEURUH_PROMOTION_SYNTHETIC_COURT_20260902"
TASK = "promote-fixture"
BRANCH = "feat/promotion-court"
HEAD = "1" * 40
BASE = "2" * 40
MERGE = "3" * 40
OTHER = "4" * 40
WORKERS_SHA = "95fadbe0118e56fbedc778f5209a47ba1f9655ac"
SENTINEL_REF = "sentinel://court/synthetic-1"
CUSTODY_REF = "custody://honey-pot/synthetic-1"
AUTH_REF = "authority://founder.promotion.v1"
PLANTED = "TESTCRED_promotion_notreal_9f8e7d"


def sentinel_record(head=HEAD, verdict="PASS", **over):
    record = {
        "kind": "independent_sentinel_court", "source_role": "SENTINEL", "independent": True,
        "court_id": "court-synthetic-0001", "receipt_ref": "receipt://prime/synthetic",
        "evidence_refs": ["file:///tmp/evidence.json"], "observed_sha": WORKERS_SHA,
        "candidate_sha": head, "canonical_sha": BASE, "findings": [], "exceptions": [],
        "required_repairs": [], "verdict": verdict, "sealed_at": "2026-09-02T00:00:00Z",
        "ref": "/tmp/sentinel.json", "mission_id": MISSION, "job_id": "job-court-1",
        "workers_sentinel_model_execution": "BLOCKED",
    }
    record.update(over)
    return record


def custody(head=HEAD, pr=7, **over):
    record = custody_record_for(repo=REPO, branch=BRANCH, head=head, pr=pr, job_id="job-hp-1")
    record.update(over)
    return record


def promotion(sentinel=None, custody_rec=None, **over):
    sentinel = sentinel_record() if sentinel is None else sentinel
    custody_rec = custody() if custody_rec is None else custody_rec
    body = {
        "schema_version": "nrh.git.promotion.v1",
        "mission_id": MISSION, "task_id": TASK, "repo": REPO,
        "worktree": "/recorded/fixture", "branch": BRANCH, "base_branch": "main",
        "expected_head": HEAD, "expected_base_head": BASE, "remote_name": "origin",
        "pr_number": 7,
        "sentinel_receipt_ref": SENTINEL_REF, "sentinel_receipt_hash": content_sha(sentinel),
        "custody_receipt_ref": CUSTODY_REF, "custody_receipt_hash": content_sha(custody_rec),
        "authority_ref": AUTH_REF, "idempotency_key": "promotion-synthetic-court-1",
    }
    body.update(over)
    return body


class RecordedGovernance:
    """Echoes the facts back as an envelope, exactly as authorize.mjs finalize() does."""

    def __init__(self, decision="allow", *, reason_code=None, forge_nonce=False,
                 raise_exc=None, mutate=None, side_effect=None):
        self.decision = decision
        self.reason_code = reason_code
        self.forge_nonce = forge_nonce
        self.raise_exc = raise_exc
        self.mutate = mutate
        self.side_effect = side_effect
        self.calls: list[dict] = []
        self.seq = 0

    def authorize(self, facts, *, grant=None, founder_approval=None):
        self.calls.append({"facts": dict(facts), "grant": grant, "founder_approval": founder_approval})
        if self.side_effect is not None:
            self.side_effect()
        if self.raise_exc is not None:
            raise self.raise_exc
        self.seq += 1
        auth_id = f"auth_promo_{self.seq:04d}"
        default_code = {"allow": "GRANT_QUALIFIED_ADOPTION", "founder_required": "FOUNDER_REQUIRED_NO_GRANT",
                        "deny": "POLICY_BLOCKED"}[self.decision]
        env = {
            "schema_version": "gov.authorization.v1", "authorization_id": auth_id,
            "action_id": facts["action_id"], "lane": facts["lane"], "component": facts["component"],
            "action_class": facts["action_class"], "target": dict(facts["target"]),
            "repository": facts.get("repository"), "branch": facts.get("branch"),
            "service_label": None, "summary": facts["summary"], "risk_tier": "R3",
            "risk_factors": ["action class merge is baseline R3.", "mutation of protected branch main (>= R3)"],
            "risk_model_version": "rm_court", "decision": self.decision,
            "reason_code": self.reason_code or default_code, "reasons": [self.reason_code or default_code],
            "grant_id": None, "grant_expires_at": None,
            "approver_ids": ["founder"] if self.decision == "allow" else [],
            "session_scope_used": False, "scope_key": "s" * 64,
            "founder_prompt": ({"prompt_id": "prompt_court", "already_prompted": False,
                                "action": "merge: " + facts["summary"], "risk": "R3",
                                "why_human_required": "No valid FounderGrant covers this bounded action.",
                                "approve_once": True, "approve_scope_session": True}
                               if self.decision == "founder_required" else None),
            "canonical_facts_hash": "c" * 64,
            "facts_nonce": "forged_nonce_0000000000" if self.forge_nonce else facts["nonce"],
            "issued_at": facts["issued_at"], "expires_at": "2099-01-01T00:00:00.000Z",
            "evidence_refs": [], "execution": NOT_CLAIMED,
            "decision_hash": hashlib.sha256(auth_id.encode()).hexdigest(),
            "decision_receipt_hash": hashlib.sha256(("r" + auth_id).encode()).hexdigest(),
            "receipt_seq": self.seq,
        }
        if self.mutate is not None:
            self.mutate(env)
        return env


def _stack(testcase, *, governance=None, pr_head=HEAD, draft=True, merged=False, ci_pass=True,
           sentinel=None, custody_rec=None, authority=None, remote_heads=None,
           ancestry=None, known_secrets=None, promotion_body=None):
    git = RecordedGitBackend(GitIdentity(
        worktree="/recorded/fixture", git_dir="/recorded/fixture/.git", head=HEAD, branch=BRANCH,
        detached=False, tracked_dirty=(), untracked=(), remote_identity="github.com/NeuruhAI/fixture",
        remote_branch_head=HEAD, remote_base_head=BASE, ahead=0, behind=0, commits_to_push=0))
    git.remote_heads = {BRANCH: HEAD, "main": BASE}
    git.remote_heads.update(remote_heads or {})
    git.ancestry = set(ancestry if ancestry is not None else {(HEAD, MERGE), (BASE, MERGE)})
    pr = MockPRAdapter([PullRequest(
        number=7, repo=REPO, head_branch=BRANCH, base_branch="main", title="[candidate] court",
        body=f"MISSION: {MISSION}", state="MERGED" if merged else "OPEN", draft=draft, merged=merged,
        mission_id=MISSION, html_url=f"mock://{REPO}/pull/7", head_sha=pr_head,
        merge_commit_sha=MERGE if merged else "")])

    def _merge(pull):
        git.remote_heads["main"] = MERGE
        return MERGE

    pr.merge_fn = _merge
    ci = MockCIAdapter(check_runs=[{"name": "court", "conclusion": "success" if ci_pass else "failure"}])
    sentinel = sentinel_record() if sentinel is None else sentinel
    custody_rec = custody() if custody_rec is None else custody_rec
    gov = governance or RecordedGovernance()
    stack = make_stack(testcase)
    executor = PromotionExecutor(
        git=git, pr=pr, ci=ci,
        court_store=MemoryCourtStore({SENTINEL_REF: sentinel}),
        custody_store=MemoryCourtStore({CUSTODY_REF: custody_rec}),
        authority_store=MemoryAuthorityStore({AUTH_REF: authority if authority is not None else FOUNDER_PROMOTION_V1}),
        governance=gov, switchboard=RecordingSwitchboard(), ledger=stack.ledger,
        known_secrets=known_secrets or [PLANTED], source_sha=WORKERS_SHA, clock=fixed_clock(),
    )
    body = promotion_body or promotion(sentinel=sentinel, custody_rec=custody_rec)
    return executor, git, pr, gov, stack, body


def _events(executor):
    return [e["type"] for e in executor.switchboard.events]


def _refuses(testcase, executor, body, reason):
    with testcase.assertRaises(GitCustodianRefused) as ctx:
        executor.run(body)
    testcase.assertEqual(ctx.exception.reason, reason, ctx.exception)
    testcase.assertEqual(_events(executor)[-1], "promotion.refused")
    return ctx.exception


class GoldenPromotionTests(unittest.TestCase):
    def test_all_four_gates_produce_exactly_one_verified_merge(self):
        executor, git, pr, gov, stack, body = _stack(self)
        receipt = executor.run(body)
        self.assertEqual(receipt["MERGE"], "YES")
        self.assertEqual(receipt["MERGED"], "YES")
        self.assertEqual(receipt["MERGE_SHA"], MERGE)
        self.assertEqual(receipt["BASE_BEFORE"], BASE)
        self.assertEqual(receipt["BASE_AFTER"], MERGE)
        self.assertEqual(receipt["HEAD_DRIFT"], "NO")
        self.assertEqual(receipt["AUTO_MERGE"], "NO")
        self.assertEqual(receipt["FORCE_PUSH"], "NO")
        self.assertEqual(receipt["DIRECT_MAIN"], "NO")
        self.assertEqual(receipt["REWRITE_HISTORY"], "NO")
        self.assertEqual(receipt["DEPLOY"], "NO")
        self.assertEqual(receipt["DEPLOYED"], "NO")
        self.assertEqual(receipt["DEPLOY_CHILD_REQUESTED"], "NO")
        self.assertEqual(receipt["GOVERNANCE_ALLOW"], "auth_promo_0001")
        self.assertEqual(receipt["GOVERNANCE_RISK_TIER"], "R3")
        self.assertEqual(receipt["SENTINEL_COURT"], "court-synthetic-0001")
        self.assertEqual(receipt["HONEY_POT_CUSTODY"], "VALID")
        self.assertEqual(receipt["AUTH_BACKEND"], "founder.promotion.v1")
        self.assertEqual(receipt["IDEMPOTENCY"], "APPLIED")
        self.assertEqual(receipt["SOURCE_SHA"], WORKERS_SHA)
        # Receipts in order, on the Switchboard and the ledger.
        self.assertEqual(_events(executor), [
            "promotion.requested", "promotion.authorized", "promotion.executed", "promotion.verified"])
        types = [r.receipt_type for r in stack.ledger.receipts(receipt["job_id"])]
        for expected in (ReceiptType.PROMOTION_REQUESTED, ReceiptType.PROMOTION_AUTHORIZED,
                         ReceiptType.PROMOTION_EXECUTED, ReceiptType.PROMOTION_VERIFIED,
                         ReceiptType.JOB_SUCCEEDED):
            self.assertIn(expected.value, types)
        # Governance was asked exactly once, about exactly this merge.
        self.assertEqual(len(gov.calls), 1)
        facts = gov.calls[0]["facts"]
        self.assertEqual(facts["action_class"], "merge")
        self.assertEqual(facts["component"], "git_custodian")
        self.assertEqual(facts["repository"], REPO)
        self.assertEqual(facts["branch"], "main")
        self.assertEqual(facts["target"], {"type": "pull_request", "id": f"{REPO}#7"})
        self.assertEqual(facts["source_sha"], WORKERS_SHA)
        self.assertEqual(facts["precondition_hash"], precondition_hash(executor_parsed(body)))
        self.assertEqual(facts["spend_usd"], 0)
        self.assertEqual(facts["externality"], "none")
        self.assertRegex(facts["nonce"], r"^[A-Za-z0-9_-]{16,128}$")
        self.assertIsNone(gov.calls[0]["grant"])
        self.assertIsNone(gov.calls[0]["founder_approval"])
        # The merge was the governed one, head-matched, never the AUTO_MERGE path.
        self.assertEqual(pr.merge_calls, [])
        self.assertEqual(len(pr.promote_calls), 1)
        self.assertEqual(pr.promote_calls[0]["match_head_sha"], HEAD)
        self.assertTrue(pr.promote_calls[0]["authorization_ref"].startswith("governance:auth_promo_0001"))
        self.assertEqual(pr.promote_calls[0]["method"], "merge")
        self.assertEqual(pr.ready_calls, [{"number": 7}], "a draft is made ready under PR_READY")
        self.assertTrue(pr.prs[0].merged)
        self.assertGreaterEqual(git.fetch_calls, 3, "fetched before gate 3, before merge, and for verify")

    def test_deploy_child_is_requested_never_performed(self):
        executor, git, pr, gov, stack, body = _stack(self)
        body = dict(body, deploy_required=True, deploy_target="vercel:fixture-preview")
        receipt = executor.run(body)
        self.assertEqual(receipt["DEPLOY_CHILD_REQUESTED"], "YES")
        self.assertEqual(receipt["DEPLOY"], "NO")
        self.assertEqual(receipt["DEPLOYED"], "NO")
        self.assertEqual(_events(executor)[-1], "deploy.requested")
        child = executor.switchboard.events[-1]["payload"]
        self.assertEqual(child["sha"], MERGE)
        self.assertEqual(child["target"], "vercel:fixture-preview")
        types = [r.receipt_type for r in stack.ledger.receipts(receipt["job_id"])]
        self.assertIn(ReceiptType.DEPLOY_REQUESTED.value, types)

    def test_replay_after_merge_asks_nothing_and_merges_nothing(self):
        executor, git, pr, gov, stack, body = _stack(self)
        first = executor.run(body)
        second = executor.run(body)
        self.assertEqual(first["IDEMPOTENCY"], "APPLIED")
        self.assertEqual(second["IDEMPOTENCY"], "PROVEN")
        self.assertEqual(second["MERGE_SHA"], MERGE)
        self.assertEqual(second["job_id"], first["job_id"])
        self.assertEqual(len(gov.calls), 1)
        self.assertEqual(len(pr.promote_calls), 1)

    def test_no_secret_ever_reaches_a_receipt(self):
        gov = RecordedGovernance(decision="deny", reason_code=f"POLICY_BLOCKED {PLANTED}")
        executor, git, pr, gov, stack, body = _stack(self, governance=gov)
        exc = _refuses(self, executor, body, "GOVERNANCE_DENY")
        self.assertNotIn(PLANTED, repr(exc.detail))
        self.assertNotIn(PLANTED, repr(executor.switchboard.events))
        with open(stack.ledger_path, encoding="utf-8") as fh:
            self.assertNotIn(PLANTED, fh.read())


def executor_parsed(body):
    from neuruh_workers.git_custodian.promotion import parse_promotion
    return parse_promotion(body)


class HeadDriftTests(unittest.TestCase):
    def test_remote_branch_moved_refuses_before_governance(self):
        executor, git, pr, gov, stack, body = _stack(self, remote_heads={BRANCH: OTHER})
        _refuses(self, executor, body, "HEAD_DRIFT")
        self.assertEqual(gov.calls, [])
        self.assertEqual(pr.promote_calls, [])

    def test_pr_head_moved_refuses_before_governance(self):
        executor, git, pr, gov, stack, body = _stack(self, pr_head=OTHER)
        _refuses(self, executor, body, "HEAD_DRIFT")
        self.assertEqual(gov.calls, [])

    def test_base_moved_since_court_refuses_before_governance(self):
        executor, git, pr, gov, stack, body = _stack(self, remote_heads={"main": OTHER})
        _refuses(self, executor, body, "BASE_DRIFT")
        self.assertEqual(gov.calls, [])
        self.assertEqual(pr.promote_calls, [])

    def test_head_moves_between_authorization_and_merge_refuses_at_the_last_check(self):
        holder = {}
        gov = RecordedGovernance(side_effect=lambda: holder["git"].remote_heads.__setitem__(BRANCH, OTHER))
        executor, git, pr, gov, stack, body = _stack(self, governance=gov)
        holder["git"] = git
        _refuses(self, executor, body, "HEAD_DRIFT")
        self.assertEqual(len(gov.calls), 1, "authorization was obtained, then the head moved")
        self.assertEqual(pr.promote_calls, [], "no merge was attempted")
        self.assertIn("promotion.authorized", _events(executor))

    def test_pr_head_moves_after_authorization_is_refused_by_match_head(self):
        holder = {}

        def _move():
            holder["pr"].prs[0].head_sha = OTHER

        gov = RecordedGovernance(side_effect=_move)
        executor, git, pr, gov, stack, body = _stack(self, governance=gov)
        holder["pr"] = pr
        _refuses(self, executor, body, "MATCH_HEAD_MISMATCH")
        self.assertEqual(len(pr.promote_calls), 1)
        self.assertFalse(pr.prs[0].merged)


class SentinelGateTests(unittest.TestCase):
    def _refuse_with(self, sentinel, reason, *, hash_of=None):
        executor, git, pr, gov, stack, body = _stack(
            self, sentinel=sentinel,
            promotion_body=promotion(sentinel=hash_of if hash_of is not None else sentinel))
        _refuses(self, executor, body, reason)
        self.assertEqual(gov.calls, [])
        self.assertEqual(pr.promote_calls, [])

    def test_fail_is_never_inverted(self):
        self._refuse_with(sentinel_record(verdict="FAIL"), "SENTINEL_NOT_PASS")

    def test_pass_with_open_exceptions_or_repairs_is_not_pass(self):
        self._refuse_with(sentinel_record(verdict="PASS_WITH_EXCEPTIONS",
                                          exceptions=[{"id": "x"}]), "SENTINEL_NOT_PASS")
        self._refuse_with(sentinel_record(required_repairs=["fix it"]), "SENTINEL_NOT_PASS")

    def test_pass_with_exceptions_but_none_open_promotes(self):
        record = sentinel_record(verdict="PASS_WITH_EXCEPTIONS")
        executor, git, pr, gov, stack, body = _stack(self, sentinel=record,
                                                     promotion_body=promotion(sentinel=record))
        self.assertEqual(executor.run(body)["MERGED"], "YES")

    def test_bare_pass_string_is_not_a_court(self):
        executor, git, pr, gov, stack, body = _stack(self, sentinel="PASS")
        _refuses(self, executor, body, "SENTINEL_INVALID")
        self.assertEqual(gov.calls, [])

    def test_courted_head_must_be_the_promoted_head(self):
        self._refuse_with(sentinel_record(head=OTHER), "SENTINEL_HEAD_MISMATCH")

    def test_sealed_record_hash_must_match(self):
        self._refuse_with(sentinel_record(findings=[{"tampered": True}]), "SENTINEL_HASH_MISMATCH",
                          hash_of=sentinel_record())

    def test_record_must_be_independent_sentinel(self):
        self._refuse_with(sentinel_record(independent=False), "SENTINEL_NOT_INDEPENDENT")
        self._refuse_with(sentinel_record(source_role="PRIME"), "SENTINEL_NOT_INDEPENDENT")
        self._refuse_with(sentinel_record(kind="ci_job_named_court"), "SENTINEL_NOT_INDEPENDENT")

    def test_other_mission_record_is_refused(self):
        self._refuse_with(sentinel_record(mission_id="SOME_OTHER_MISSION"), "SENTINEL_MISSION_MISMATCH")

    def test_missing_record_refuses(self):
        executor, git, pr, gov, stack, body = _stack(self, sentinel=None)
        executor.court_store = MemoryCourtStore({})
        _refuses(self, executor, body, "SENTINEL_MISSING")


class CustodyGateTests(unittest.TestCase):
    def test_promotion_ready_is_required(self):
        rec = custody(promotion_ready=False)
        executor, git, pr, gov, stack, body = _stack(self, custody_rec=rec, promotion_body=promotion(custody_rec=rec))
        _refuses(self, executor, body, "CUSTODY_NOT_READY")
        self.assertEqual(gov.calls, [])

    def test_custody_must_name_this_exact_candidate(self):
        rec = custody(head=OTHER)
        executor, git, pr, gov, stack, body = _stack(self, custody_rec=rec, promotion_body=promotion(custody_rec=rec))
        _refuses(self, executor, body, "CUSTODY_MISMATCH")

    def test_custody_hash_must_match(self):
        executor, git, pr, gov, stack, body = _stack(self, custody_rec=custody(ci_status="PENDING"),
                                                     promotion_body=promotion(custody_rec=custody()))
        _refuses(self, executor, body, "CUSTODY_HASH_MISMATCH")

    def test_missing_custody_refuses(self):
        executor, git, pr, gov, stack, body = _stack(self)
        executor.custody_store = MemoryCourtStore({})
        _refuses(self, executor, body, "CUSTODY_MISSING")

    def test_pr_must_exist_and_be_open(self):
        executor, git, pr, gov, stack, body = _stack(self)
        pr.prs.clear()
        _refuses(self, executor, body, "PR_MISSING")
        executor2, git2, pr2, gov2, stack2, body2 = _stack(self)
        pr2.prs[0].state = "CLOSED"
        _refuses(self, executor2, body2, "PR_NOT_OPEN")
        self.assertEqual(gov.calls + gov2.calls, [])

    def test_ci_must_pass(self):
        executor, git, pr, gov, stack, body = _stack(self, ci_pass=False)
        _refuses(self, executor, body, "CI_NOT_PASS")
        self.assertEqual(gov.calls, [])


class GovernanceGateTests(unittest.TestCase):
    def _refuse_with(self, governance, reason):
        executor, git, pr, gov, stack, body = _stack(self, governance=governance)
        exc = _refuses(self, executor, body, reason)
        self.assertEqual(pr.promote_calls, [], "nothing merges without a bound ALLOW")
        self.assertFalse(pr.prs[0].merged)
        return exc

    def test_founder_required_surfaces_the_exact_prompt(self):
        exc = self._refuse_with(RecordedGovernance("founder_required"), "GOVERNANCE_FOUNDER_REQUIRED")
        self.assertEqual(exc.detail["reason_code"], "FOUNDER_REQUIRED_NO_GRANT")
        self.assertEqual(exc.detail["founder_prompt"]["risk"], "R3")
        self.assertTrue(exc.detail["founder_prompt"]["why_human_required"])

    def test_deny_refuses(self):
        exc = self._refuse_with(RecordedGovernance("deny"), "GOVERNANCE_DENY")
        self.assertEqual(exc.detail["reason_code"], "POLICY_BLOCKED")

    def test_unreachable_refuses(self):
        self._refuse_with(RecordedGovernance(raise_exc=ConnectionRefusedError("down")), "GOVERNANCE_UNREACHABLE")

    def test_envelope_not_bound_to_this_nonce_is_a_forgery(self):
        self._refuse_with(RecordedGovernance(forge_nonce=True), "GOVERNANCE_ENVELOPE_UNBOUND")

    def test_envelope_about_a_different_action_is_refused(self):
        for field, value in (("branch", "develop"), ("repository", "NeuruhAI/other"),
                             ("action_class", "git_push"), ("target", {"type": "pull_request", "id": f"{REPO}#8"})):
            with self.subTest(field=field):
                self._refuse_with(RecordedGovernance(mutate=lambda e, f=field, v=value: e.__setitem__(f, v)),
                                  "GOVERNANCE_ENVELOPE_UNBOUND")

    def test_envelope_claiming_execution_or_lacking_receipt_is_refused(self):
        self._refuse_with(RecordedGovernance(mutate=lambda e: e.__setitem__("execution", "EXECUTED")),
                          "GOVERNANCE_CLAIMS_EXECUTION")
        self._refuse_with(RecordedGovernance(mutate=lambda e: e.__setitem__("decision_receipt_hash", "")),
                          "GOVERNANCE_INVALID")
        self._refuse_with(RecordedGovernance(mutate=lambda e: e.__setitem__("schema_version", "gov.authorization.v0")),
                          "GOVERNANCE_INVALID")

    def test_grant_and_founder_approval_are_forwarded_verbatim_never_minted(self):
        executor, git, pr, gov, stack, body = _stack(self)
        grant = {"schema_version": "gov.grant.v1", "grant_id": "g1", "signature": "f" * 64}
        approval = {"schema_version": "gov.founder.approval.v1", "mode": "scope_once", "signature": "a" * 64}
        executor.run(dict(body, grant=grant, founder_approval=approval))
        self.assertEqual(gov.calls[0]["grant"], grant)
        self.assertEqual(gov.calls[0]["founder_approval"], approval)


class AuthorityGateTests(unittest.TestCase):
    def test_founder_candidate_policy_cannot_promote(self):
        executor, git, pr, gov, stack, body = _stack(self, authority=FOUNDER_CANDIDATE_V1)
        _refuses(self, executor, body, "AUTHORITY_MISSING")
        self.assertEqual(gov.calls, [])

    def test_policy_that_allows_auto_merge_is_invalid(self):
        policy = dict(FOUNDER_PROMOTION_V1, allowed=FOUNDER_PROMOTION_V1["allowed"] + ("AUTO_MERGE",))
        executor, git, pr, gov, stack, body = _stack(self, authority=policy)
        _refuses(self, executor, body, "AUTHORITY_INVALID")

    def test_policy_missing_forbidden_list_is_incomplete(self):
        policy = dict(FOUNDER_PROMOTION_V1, forbidden=("FORCE_PUSH",))
        executor, git, pr, gov, stack, body = _stack(self, authority=policy)
        _refuses(self, executor, body, "AUTHORITY_INCOMPLETE")

    def test_missing_authority_refuses(self):
        executor, git, pr, gov, stack, body = _stack(self)
        executor.authority_store = MemoryAuthorityStore({})
        _refuses(self, executor, body, "AUTHORITY_MISSING")


class ContractTests(unittest.TestCase):
    def _refuse(self, reason, **over):
        executor, git, pr, gov, stack, body = _stack(self)
        with self.assertRaises(GitCustodianRefused) as ctx:
            executor.run(dict(body, **over))
        self.assertEqual(ctx.exception.reason, reason)
        self.assertEqual(gov.calls, [])

    def test_base_must_be_canonical(self):
        self._refuse("BASE_NOT_CANONICAL", base_branch="develop")

    def test_candidate_cannot_be_a_protected_branch(self):
        self._refuse("PROTECTED_BRANCH", branch="main")
        self._refuse("PROTECTED_BRANCH", branch="release/1.0")

    def test_write_banned_repo_is_never_promoted_into(self):
        self._refuse("WRITE_BAN", repo="NeuruhAI/neuruh-axon")

    def test_unknown_or_missing_fields_fail_closed(self):
        self._refuse("MALFORMED_INPUT", caller_approved=True)
        executor, git, pr, gov, stack, body = _stack(self)
        body.pop("sentinel_receipt_hash")
        with self.assertRaises(GitCustodianRefused) as ctx:
            executor.run(body)
        self.assertEqual(ctx.exception.reason, "MALFORMED_INPUT")

    def test_head_equal_to_base_is_nothing(self):
        self._refuse("NOTHING_TO_PROMOTE", expected_head=BASE)

    def test_homemade_event_dict_is_refused(self):
        executor, git, pr, gov, stack, body = _stack(self)
        with self.assertRaises(GitCustodianRefused) as ctx:
            executor.handle({"type": "promotion.requested", "promotion": body})
        self.assertEqual(ctx.exception.reason, "SWITCHBOARD_UNKNOWN")
        with self.assertRaises(GitCustodianRefused) as ctx2:
            from neuruh_workers.git_custodian import build_candidate_push_event
            executor.handle(build_candidate_push_event({
                "mission_id": MISSION, "task_id": TASK, "expected_local_head": HEAD, "repo": REPO,
                "worktree": "/x", "branch": BRANCH, "idempotency_key": "k"}))
        self.assertEqual(ctx2.exception.reason, "SWITCHBOARD_UNKNOWN")


class VerifyTests(unittest.TestCase):
    def test_merge_that_cannot_be_verified_is_reported_merged_unverified_and_never_retried(self):
        executor, git, pr, gov, stack, body = _stack(self, ancestry={(BASE, MERGE)})  # HEAD not in main
        exc = _refuses(self, executor, body, "PROMOTION_VERIFY_FAILED")
        self.assertIn("MERGED_UNVERIFIED", str(exc))
        self.assertEqual(len(pr.promote_calls), 1)
        self.assertIn("promotion.executed", _events(executor))
        # Retry: the PR is merged; the base still does not contain the head; no second merge.
        with self.assertRaises(GitCustodianRefused) as ctx:
            executor.run(body)
        self.assertEqual(ctx.exception.reason, "PR_ALREADY_MERGED_UNVERIFIED")
        self.assertEqual(len(pr.promote_calls), 1)
        self.assertEqual(len(gov.calls), 1)


class RealGitBareRemoteCourt(unittest.TestCase):
    """The verify seam against real git: a bare origin, a real merge commit, real ancestry."""

    def _pair(self):
        tmp = tempfile.mkdtemp(prefix="neuruh-promotion-court-")
        self.addCleanup(lambda: shutil.rmtree(tmp, ignore_errors=True))
        pair = make_seeded_pair(tmp, feature_branch=BRANCH)
        gitcmd(pair["work"], "push", "-u", "origin", BRANCH)
        return tmp, pair

    def _real_stack(self, pair, tmp, governance=None):
        head, base = pair["feature_sha"], pair["base_sha"]
        sentinel = sentinel_record(head=head, canonical_sha=base)
        custody_rec = custody(head=head)
        pr = MockPRAdapter([PullRequest(
            number=7, repo=REPO, head_branch=BRANCH, base_branch="main", title="court", body=f"MISSION: {MISSION}",
            state="OPEN", draft=True, merged=False, mission_id=MISSION, head_sha=head)])

        def _merge(pull):
            clone = os.path.join(tmp, "merger")
            gitcmd(tmp, "clone", "-q", pair["remote"], clone)
            gitcmd(clone, "config", "user.name", "Promotion Court")
            gitcmd(clone, "config", "user.email", "promotion-court@neuruh.local")
            gitcmd(clone, "checkout", "-q", "main")
            gitcmd(clone, "merge", "--no-ff", "-m", f"promote {BRANCH}", head)
            merge_sha = gitsha(clone)
            gitcmd(clone, "push", "-q", "origin", "main")
            return merge_sha

        pr.merge_fn = _merge
        stack = make_stack(self)
        body = promotion(sentinel=sentinel, custody_rec=custody_rec, worktree=pair["work"],
                         expected_head=head, expected_base_head=base)
        executor = PromotionExecutor(
            git=RealGitBackend(), pr=pr, ci=MockCIAdapter(check_runs=[{"name": "court", "conclusion": "success"}]),
            court_store=MemoryCourtStore({SENTINEL_REF: sentinel}),
            custody_store=MemoryCourtStore({CUSTODY_REF: custody_rec}),
            authority_store=MemoryAuthorityStore({AUTH_REF: FOUNDER_PROMOTION_V1}),
            governance=governance or RecordedGovernance(), switchboard=RecordingSwitchboard(),
            ledger=stack.ledger, source_sha=WORKERS_SHA, clock=fixed_clock())
        return executor, pr, body, head, base

    def test_real_merge_is_verified_against_the_bare_remote_and_replays_proven(self):
        tmp, pair = self._pair()
        executor, pr, body, head, base = self._real_stack(pair, tmp)
        receipt = executor.run(body)
        self.assertEqual(receipt["MERGED"], "YES")
        gitcmd(pair["work"], "fetch", "-q", "origin")
        main_now = gitsha(pair["work"], "origin/main")
        self.assertEqual(receipt["BASE_AFTER"], main_now)
        self.assertEqual(receipt["MERGE_SHA"], main_now)
        self.assertEqual(gitcmd(pair["work"], "merge-base", "--is-ancestor", head, main_now, check=False).returncode, 0)
        self.assertEqual(gitcmd(pair["work"], "merge-base", "--is-ancestor", base, main_now, check=False).returncode, 0)
        self.assertEqual(receipt["BASE_BEFORE"], base)
        again = executor.run(body)
        self.assertEqual(again["IDEMPOTENCY"], "PROVEN")
        self.assertEqual(len(pr.promote_calls), 1)

    def test_real_head_drift_after_the_court_refuses_before_governance(self):
        tmp, pair = self._pair()
        gov = RecordedGovernance()
        executor, pr, body, head, base = self._real_stack(pair, tmp, governance=gov)
        # Someone pushes one more commit to the courted branch after the court sealed.
        with open(os.path.join(pair["work"], "late.txt"), "w", encoding="utf-8") as fh:
            fh.write("late\n")
        gitcmd(pair["work"], "add", "late.txt")
        gitcmd(pair["work"], "commit", "-q", "-m", "late commit after court")
        gitcmd(pair["work"], "push", "-q", "origin", BRANCH)
        with self.assertRaises(GitCustodianRefused) as ctx:
            executor.run(body)
        self.assertEqual(ctx.exception.reason, "HEAD_DRIFT")
        self.assertEqual(gov.calls, [])
        self.assertEqual(pr.promote_calls, [])
        gitcmd(pair["work"], "fetch", "-q", "origin")
        self.assertEqual(gitsha(pair["work"], "origin/main"), base, "main untouched")


class SwitchboardHopTests(unittest.TestCase):
    def _board(self, executor=None):
        tmp = tempfile.mkdtemp(prefix="neuruh-promotion-board-")
        self.addCleanup(lambda: shutil.rmtree(tmp, ignore_errors=True))
        receipts = os.path.join(tmp, "receipts")
        os.makedirs(receipts, exist_ok=True)
        return Switchboard(os.path.join(tmp, "state"), receipts_dir=receipts,
                           known_missions=frozenset({MISSION}), clock=fixed_clock(),
                           promotion_executor=executor)

    def test_promotion_requested_routes_to_the_executor_and_acks(self):
        executor, git, pr, gov, stack, body = _stack(self)
        board = self._board(executor)
        event = build_promotion_request_event(body)
        self.assertEqual(event["event_type"], EventType.PROMOTION_REQUESTED.value)
        board.ingest_raw(event)
        report = board.tick()
        self.assertIn(event["event_id"], report.acked)
        self.assertTrue(pr.prs[0].merged)
        self.assertEqual(len(gov.calls), 1)
        self.assertEqual(_events(executor)[-1], "promotion.verified")
        # A second tick delivers nothing new and merges nothing more.
        report2 = board.tick()
        self.assertEqual(report2.acked, [])
        self.assertEqual(len(pr.promote_calls), 1)

    def test_without_an_executor_the_switchboard_never_merges(self):
        executor, git, pr, gov, stack, body = _stack(self)
        board = self._board(None)
        board.ingest_raw(build_promotion_request_event(body))
        report = board.tick()
        self.assertEqual(report.acked, [])
        self.assertFalse(pr.prs[0].merged)
        self.assertEqual(gov.calls, [])

    def test_unknown_mission_is_deadlettered_not_promoted(self):
        executor, git, pr, gov, stack, body = _stack(self)
        board = self._board(executor)
        board.ingest_raw(build_promotion_request_event(dict(body, mission_id="UNKNOWN_MISSION")))
        report = board.tick()
        self.assertEqual(len(report.deadlettered), 1)
        self.assertFalse(pr.prs[0].merged)


class GhAdapterTests(unittest.TestCase):
    """The real adapter's argv contract, with a recorded gh."""

    def _pr_json(self, *, merged=False, head=HEAD, draft=True):
        import json
        return json.dumps({"number": 7, "state": "MERGED" if merged else "OPEN", "isDraft": draft,
                           "mergedAt": "2026-09-02T00:00:00Z" if merged else None,
                           "headRefName": BRANCH, "baseRefName": "main", "headRefOid": head,
                           "mergeCommit": {"oid": MERGE} if merged else None,
                           "url": f"https://github.com/{REPO}/pull/7", "title": "t", "body": f"MISSION: {MISSION}"})

    def test_promote_merge_uses_match_head_commit_and_reads_back_the_merge(self):
        state = {"merged": False}
        log = []

        def runner(argv):
            log.append(list(argv))
            if argv[1:3] == ["pr", "view"]:
                return subprocess.CompletedProcess(argv, 0, self._pr_json(merged=state["merged"]), "")
            if argv[1:3] == ["pr", "merge"]:
                state["merged"] = True
                return subprocess.CompletedProcess(argv, 0, "", "")
            if argv[1:3] == ["pr", "ready"]:
                return subprocess.CompletedProcess(argv, 0, "", "")
            return subprocess.CompletedProcess(argv, 1, "", "unexpected")

        gh = GhPRAdapter(runner)
        pr = gh.get(REPO, 7)
        self.assertEqual(pr.head_sha, HEAD)
        self.assertEqual(pr.mission_id, MISSION)
        self.assertFalse(pr.merged)
        out = gh.promote_merge(pr, match_head_sha=HEAD, authorization_ref="governance:auth_x")
        self.assertEqual(out["merge_sha"], MERGE)
        merge_argv = [a for a in log if a[1:3] == ["pr", "merge"]][0]
        self.assertIn("--merge", merge_argv)
        self.assertIn("--match-head-commit", merge_argv)
        self.assertIn(HEAD, merge_argv)
        self.assertNotIn("--squash", merge_argv)
        self.assertNotIn("--rebase", merge_argv)
        self.assertNotIn("--admin", merge_argv)

    def test_ungoverned_merge_paths_stay_forbidden(self):
        gh = GhPRAdapter(lambda argv: subprocess.CompletedProcess(argv, 0, self._pr_json(), ""))
        pr = gh.get(REPO, 7)
        with self.assertRaises(GitCustodianRefused) as ctx:
            gh.promote_merge(pr, match_head_sha=HEAD, authorization_ref="")
        self.assertEqual(ctx.exception.reason, "AUTO_MERGE")
        with self.assertRaises(GitCustodianRefused) as ctx2:
            gh.merge(pr)
        self.assertEqual(ctx2.exception.reason, "AUTO_MERGE")
        with self.assertRaises(GitCustodianRefused) as ctx3:
            gh.promote_merge(pr, match_head_sha=HEAD, authorization_ref="governance:x", method="squash")
        self.assertEqual(ctx3.exception.reason, "REWRITE_HISTORY")
        for forbidden in (gh.create_tag, gh.deploy):
            with self.assertRaises(GitCustodianRefused):
                forbidden()


if __name__ == "__main__":
    unittest.main()
