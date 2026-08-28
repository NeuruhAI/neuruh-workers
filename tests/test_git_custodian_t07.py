"""FIRST COURT — golden T07 candidate idempotency.

Input is the current T07 candidate against a recorded/mock GitHub + local
identity matching the golden fixture. The candidate is ALREADY pushed and
PR 4 already exists. V1 must prove it will not mutate axon, force-push,
merge, deploy, duplicate the PR, or invent CI_PASS.
"""
from __future__ import annotations

import json
import os
import unittest

from neuruh_workers import ExecutionLedger, ExternalActionDenied, WorkerRegistry
from neuruh_workers.git_custodian import (
    FOUNDER_CANDIDATE_V1,
    GitCustodian,
    GitIdentity,
    MemoryAuthorityStore,
    MemoryCourtStore,
    MockCIAdapter,
    MockPRAdapter,
    PullRequest,
    RecordedGitBackend,
    RecordingSwitchboard,
)
from neuruh_workers.registry import EXTERNAL_AUTHORIZATION_SENTINEL
from tests.git_custodian_fixtures import (
    T07_AUTHORITY_REF,
    T07_BASE_SHA,
    T07_BRANCH,
    T07_COURT,
    T07_COURT_REF,
    T07_EXISTING_COMMENTS,
    T07_HEAD,
    T07_MISSION,
    T07_PR,
    T07_PR_TITLE,
    T07_REPO,
    T07_STAGE,
    PLANTED_CREDENTIAL,
    t07_candidate,
)
from tests.helpers import make_stack


def _t07_stack(testcase, *, comments=None, court=None, authority=None,
               extra_secret=None):
    git = RecordedGitBackend(GitIdentity(
        worktree="/recorded/neuruh-axon",
        git_dir="/recorded/neuruh-axon/.git",
        head=T07_HEAD,
        branch=T07_BRANCH,
        detached=False,
        tracked_dirty=(),
        untracked=(),
        remote_identity="github.com/NeuruhAI/neuruh-axon",
        remote_branch_head=T07_HEAD,
        remote_base_head=T07_BASE_SHA,
        ahead=0,
        behind=0,
        commits_to_push=0,
    ))
    pr = MockPRAdapter([
        PullRequest(
            number=T07_PR,
            repo=T07_REPO,
            head_branch=T07_BRANCH,
            base_branch="main",
            title=T07_PR_TITLE,
            body=f"MISSION={T07_MISSION}\nSTAGE={T07_STAGE}",
            state="OPEN",
            draft=True,
            merged=False,
            mission_id=T07_MISSION,
            comments=list(comments if comments is not None else [
                {"id": 5447679688, "body": "SENTINEL T07 CLOSURE"},
            ]),
            html_url=f"https://github.com/{T07_REPO}/pull/{T07_PR}",
            head_sha=T07_HEAD,
        ),
    ])
    ci = MockCIAdapter(check_runs=[], statuses=[])
    court_store = MemoryCourtStore({T07_COURT_REF: court or T07_COURT})
    auth_store = MemoryAuthorityStore({
        T07_AUTHORITY_REF: authority or FOUNDER_CANDIDATE_V1,
    })
    switchboard = RecordingSwitchboard()
    stack = make_stack(testcase)
    secrets = [PLANTED_CREDENTIAL]
    if extra_secret:
        secrets.append(extra_secret)
    worker = GitCustodian(
        git=git, pr=pr, ci=ci,
        court_store=court_store, authority_store=auth_store,
        switchboard=switchboard, ledger=stack.ledger,
        known_secrets=secrets,
    )
    return worker, git, pr, ci, switchboard, stack


class T07GoldenIdempotencyTests(unittest.TestCase):
    """FIRST TEST — golden T07 candidate (idempotency)."""

    def _run_twice(self):
        worker, git, pr, ci, switchboard, stack = _t07_stack(self)
        event = {"type": "candidate.push.requested",
                 "candidate": t07_candidate()}
        first = worker.handle(event)
        second = worker.handle(event)
        return first, second, git, pr, switchboard, stack

    def test_t07_idempotent_already_present_does_not_mutate(self):
        first, second, git, pr, switchboard, stack = self._run_twice()
        for receipt in (first, second):
            self.assertEqual(receipt["BRANCH_ALREADY_PRESENT"], "YES")
            self.assertEqual(receipt["REMOTE_HEAD_MATCH"], "YES")
            self.assertEqual(receipt["PR_ALREADY_PRESENT"], "YES")
            self.assertEqual(receipt["DUPLICATE_PR"], "NO")
            self.assertEqual(receipt["DUPLICATE_PR_CREATED"], "NO")
            self.assertEqual(receipt["FORCE_PUSH"], "NO")
            self.assertEqual(receipt["MERGE"], "NO")
            self.assertEqual(receipt["MERGED"], "NO")
            self.assertEqual(receipt["DEPLOY"], "NO")
            self.assertEqual(receipt["DEPLOYED"], "NO")
            self.assertEqual(receipt["DIRECT_MAIN"], "NO")
            self.assertEqual(receipt["CI_STATUS"], "NOT_CONFIGURED_OR_NO_RUNS")
            self.assertEqual(receipt["T07_REMOTE_SHA"], T07_HEAD)
            self.assertEqual(receipt["T07_PR"], T07_PR)
            self.assertEqual(receipt["PR_DRAFT"], "YES")
            self.assertEqual(receipt["EXTRA_COMMENTS"], "NO")
            self.assertIs(receipt["SWITCHAROO_INTEGRATED"], False)
            self.assertNotEqual(receipt["NEXT"], "")
            self.assertIn("BACKEND", receipt)
            self.assertIn("AUTH_BACKEND", receipt)
            self.assertIn("TESTS", receipt)
            self.assertIn("IDEMPOTENCY", receipt)
            self.assertEqual(receipt["MERGED"], "NO")
            self.assertEqual(receipt["DEPLOYED"], "NO")

        self.assertEqual(git.push_calls, [])
        self.assertEqual(pr.create_calls, [])
        self.assertEqual(pr.merge_calls, [])
        self.assertEqual(pr.comment_calls, [])
        self.assertEqual(len(pr.active_prs(T07_REPO, T07_BRANCH, T07_MISSION)), 1)
        self.assertEqual(pr.prs[0].draft, True)
        self.assertEqual(len(pr.prs[0].comments), T07_EXISTING_COMMENTS)
        self.assertEqual(pr.creates, 0)
        self.assertFalse(git.force_used)

        opened = [e for e in switchboard.events
                  if e["type"] == "candidate.pr.opened"]
        self.assertEqual(opened, [])
        self.assertFalse(any(e["type"] == "ci.passed" for e in switchboard.events))
        self.assertFalse(any(e["type"] == "promotion.ready"
                             for e in switchboard.events))

    def test_t07_does_not_invent_ci_pass_from_empty_provider(self):
        worker, *_rest = _t07_stack(self)
        receipt = worker.handle({
            "type": "candidate.push.requested",
            "candidate": t07_candidate(),
        })
        self.assertEqual(receipt["CI_STATUS"], "NOT_CONFIGURED_OR_NO_RUNS")
        self.assertNotEqual(receipt["CI_STATUS"], "PASS")
        self.assertNotEqual(receipt["CI_STATUS"], "PENDING")

    def test_t07_bare_pass_string_is_not_a_court(self):
        worker, git, pr, ci, switchboard, stack = _t07_stack(self)
        worker.court_store = MemoryCourtStore({T07_COURT_REF: "PASS"})
        with self.assertRaises(Exception) as caught:
            worker.handle({
                "type": "candidate.push.requested",
                "candidate": t07_candidate(),
            })
        self.assertIn("COURT", str(caught.exception).upper())
        self.assertEqual(git.push_calls, [])
        self.assertEqual(pr.creates, 0)

    def test_planted_credential_never_enters_receipt_or_ledger(self):
        worker, git, pr, ci, switchboard, stack = _t07_stack(self)
        candidate = t07_candidate(
            remote_url_or_identity=(
                f"https://user:{PLANTED_CREDENTIAL}@github.com/NeuruhAI/neuruh-axon"
            ),
        )
        # Identity still matches after redaction; the planted value must not
        # leak even if the caller handed it to us.
        receipt = worker.handle({
            "type": "candidate.push.requested",
            "candidate": candidate,
        })
        blob = json.dumps(receipt, sort_keys=True)
        self.assertNotIn(PLANTED_CREDENTIAL, blob)
        with open(stack.ledger_path, encoding="utf-8") as fh:
            ledger_text = fh.read()
        self.assertNotIn(PLANTED_CREDENTIAL, ledger_text)
        for event in switchboard.events:
            self.assertNotIn(PLANTED_CREDENTIAL, json.dumps(event))

    def test_default_registry_lockout_is_not_weakened(self):
        from neuruh_workers.registry import Capability, WorkerDeclaration
        registry = WorkerRegistry()
        with self.assertRaises(ExternalActionDenied):
            registry.register_worker(WorkerDeclaration(
                worker_id="mailer", description="d",
                capabilities=(Capability(
                    name="comms.send_email", description="sends email",
                    read_only=False, external=True,
                    execute=lambda ctx, params: {"sent": True}),),
            ))
        # Even the operator sentinel still cannot execute externals via the
        # v1 runner — that lockout lives in runner.py, proven elsewhere.
        authorized = WorkerRegistry(
            external_authorization=EXTERNAL_AUTHORIZATION_SENTINEL)
        self.assertTrue(authorized.external_authorized)
        census_path = os.path.join(
            os.path.dirname(__file__), os.pardir, "neuruh_workers", "runner.py")
        with open(census_path, encoding="utf-8") as fh:
            runner_src = fh.read()
        self.assertIn("EXTERNAL_ACTION_DENIED", runner_src)
        self.assertIn("cap.external", runner_src)


if __name__ == "__main__":
    unittest.main()
