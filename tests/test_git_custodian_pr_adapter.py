"""THIRD COURT — mock PR adapter.

Same mission/repo/branch yields exactly one PR: create on the first run,
reuse on the second. Draft is preserved. Comments are not spammed.
"""
from __future__ import annotations

import unittest

from neuruh_workers.contracts import content_sha
from neuruh_workers.git_custodian import (
    FOUNDER_CANDIDATE_V1,
    GitCustodian,
    GitIdentity,
    MemoryAuthorityStore,
    MemoryCourtStore,
    MockCIAdapter,
    MockPRAdapter,
    RecordedGitBackend,
    RecordingSwitchboard,
)
from tests.git_custodian_fixtures import T07_AUTHORITY_REF
from tests.helpers import make_stack

REPO = "local/pr-fixture"
BRANCH = "feat/one-pr-only"
MISSION = "PR_ADAPTER_COURT"
STAGE = "ONE_PR"
HEAD = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
BASE = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
COURT_REF = "court://pr-adapter/one"


def _court():
    return {
        "schema_version": "nrh.court.receipt.v1",
        "independent_court": "PASS",
        "mission_id": MISSION,
        "stage": STAGE,
        "source_head": HEAD,
        "candidate": {
            "repo": REPO,
            "branch": BRANCH,
            "head": HEAD,
            "base": "main",
            "base_head": BASE,
        },
    }


def _candidate(court):
    return {
        "schema_version": "nrh.git.candidate.v1",
        "mission_id": MISSION,
        "task_id": STAGE,
        "repo": REPO,
        "worktree": "/recorded/pr-fixture",
        "branch": BRANCH,
        "expected_local_head": HEAD,
        "remote_name": "origin",
        "remote_url_or_identity": "local/pr-fixture",
        "expected_remote_base": f"main@{BASE}",
        "court_receipt_ref": COURT_REF,
        "court_receipt_hash": content_sha(court),
        "authority_ref": T07_AUTHORITY_REF,
        "commit_scope": "mock adapter court",
        "pr_title": "[DO NOT MERGE] one PR only",
        "pr_body_metadata": {
            "TEST_SUMMARY": "mock PR adapter",
            "KNOWN_RESIDUAL_RISKS": "none",
            "RECEIPT_REFERENCE": COURT_REF,
        },
        "idempotency_key": "pr-adapter-one-v1",
    }


class MockPRAdapterCourtTests(unittest.TestCase):
    """THIRD TEST — mock PR adapter, one PR per repo+branch+mission."""

    def setUp(self):
        court = _court()
        git = RecordedGitBackend(GitIdentity(
            worktree="/recorded/pr-fixture",
            git_dir="/recorded/pr-fixture/.git",
            head=HEAD,
            branch=BRANCH,
            detached=False,
            tracked_dirty=(),
            untracked=(),
            remote_identity="local/pr-fixture",
            remote_branch_head=None,  # not yet pushed
            remote_base_head=BASE,
            ahead=1,
            behind=0,
            commits_to_push=1,
        ))
        # After the worker "pushes", the recorded backend reports the new head.
        git.auto_advance_remote = True
        self.pr = MockPRAdapter()
        self.switchboard = RecordingSwitchboard()
        stack = make_stack(self)
        self.worker = GitCustodian(
            git=git, pr=self.pr, ci=MockCIAdapter(),
            court_store=MemoryCourtStore({COURT_REF: court}),
            authority_store=MemoryAuthorityStore(
                {T07_AUTHORITY_REF: FOUNDER_CANDIDATE_V1}),
            switchboard=self.switchboard, ledger=stack.ledger,
        )
        self.event = {"type": "candidate.push.requested",
                      "candidate": _candidate(court)}

    def test_create_then_reuse_exactly_one_pr(self):
        first = self.worker.handle(self.event)
        self.assertEqual(first["PR_ALREADY_PRESENT"], "NO")
        self.assertEqual(first["DUPLICATE_PR"], "NO")
        self.assertEqual(first["DUPLICATE_PR_CREATED"], "NO")
        self.assertEqual(self.pr.creates, 1)
        self.assertEqual(len(self.pr.prs), 1)
        created = self.pr.prs[0]
        self.assertTrue(created.draft)
        self.assertEqual(created.state, "OPEN")
        self.assertIn("MISSION", created.body)
        self.assertIn("STAGE", created.body)
        self.assertIn("SOURCE_HEAD", created.body)
        self.assertIn("BASE_HEAD", created.body)
        self.assertIn("COURT_VERDICT", created.body)
        self.assertIn("TEST_SUMMARY", created.body)
        self.assertIn("RECEIPT_REFERENCE", created.body)
        self.assertIn("KNOWN_RESIDUAL_RISKS", created.body)
        opened = [e for e in self.switchboard.events
                  if e["type"] == "candidate.pr.opened"]
        self.assertEqual(len(opened), 1)

        second = self.worker.handle(self.event)
        self.assertEqual(second["PR_ALREADY_PRESENT"], "YES")
        self.assertEqual(second["DUPLICATE_PR"], "NO")
        self.assertEqual(second["DUPLICATE_PR_CREATED"], "NO")
        self.assertEqual(self.pr.creates, 1)
        self.assertEqual(len(self.pr.prs), 1)
        self.assertEqual(self.pr.prs[0].number, created.number)
        self.assertTrue(self.pr.prs[0].draft)
        self.assertEqual(self.pr.comment_calls, [])
        self.assertEqual(self.pr.merge_calls, [])
        opened_after = [e for e in self.switchboard.events
                        if e["type"] == "candidate.pr.opened"]
        self.assertEqual(len(opened_after), 1)

    def test_pr_body_does_not_embed_secrets(self):
        from tests.git_custodian_fixtures import PLANTED_CREDENTIAL
        self.worker.known_secrets = [PLANTED_CREDENTIAL]
        self.event["candidate"]["pr_body_metadata"]["TEST_SUMMARY"] = (
            f"leaked {PLANTED_CREDENTIAL}"
        )
        with self.assertRaises(Exception):
            self.worker.handle(self.event)
        self.assertEqual(self.pr.creates, 0)


if __name__ == "__main__":
    unittest.main()
