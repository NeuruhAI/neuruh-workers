"""SECOND COURT — temporary local bare remote.

A new feature candidate is pushed to a throwaway bare remote, the exact
remote SHA is verified, and a rerun produces no duplicate state. Also
proves the fail-closed matrix: main push, wrong SHA, detached HEAD,
dirty state, remote divergence, force push, malformed input, court,
authority, and credential leak.
"""
from __future__ import annotations

import inspect
import json
import os
import tempfile
import unittest

from neuruh_workers.git_custodian import (
    FOUNDER_CANDIDATE_V1,
    GitCustodian,
    GitCustodianRefused,
    MemoryAuthorityStore,
    MemoryCourtStore,
    MockCIAdapter,
    MockPRAdapter,
    RealGitBackend,
    RecordingSwitchboard,
)
from neuruh_workers.contracts import content_sha
from tests.git_custodian_fixtures import (
    PLANTED_CREDENTIAL,
    T07_AUTHORITY_REF,
    git,
    local_candidate,
    local_court,
    make_seeded_pair,
    sha,
    write_file,
)
from tests.helpers import make_stack


def _worker(testcase, pair, court, *, pr=None, extra_secrets=None,
            authority=None):
    stack = make_stack(testcase)
    pr = pr or MockPRAdapter()
    switchboard = RecordingSwitchboard()
    worker = GitCustodian(
        git=RealGitBackend(),
        pr=pr,
        ci=MockCIAdapter(),
        court_store=MemoryCourtStore({"court://local/bare-remote": court}),
        authority_store=MemoryAuthorityStore({
            T07_AUTHORITY_REF: authority or FOUNDER_CANDIDATE_V1,
        }),
        switchboard=switchboard,
        ledger=stack.ledger,
        known_secrets=list(extra_secrets or []) + [PLANTED_CREDENTIAL],
    )
    return worker, pr, switchboard, stack


class BareRemoteCourtTests(unittest.TestCase):
    """SECOND TEST — temporary local bare remote."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="neuruh-gitcust-")
        self.addCleanup(_rmtree, self.tmpdir)
        self.pair = make_seeded_pair(self.tmpdir)
        self.court = local_court(self.pair)

    def _event(self, **overrides):
        return {
            "type": "candidate.push.requested",
            "candidate": local_candidate(self.pair, court=self.court, **overrides),
        }

    def test_push_verifies_exact_remote_sha_and_rerun_is_idempotent(self):
        worker, pr, switchboard, stack = _worker(self, self.pair, self.court)
        first = worker.handle(self._event())
        remote_sha = sha(self.pair["remote"],
                         f"refs/heads/{self.pair['feature_branch']}")
        self.assertEqual(remote_sha, self.pair["feature_sha"])
        self.assertEqual(first["REMOTE_HEAD_MATCH"], "YES")
        self.assertEqual(first["T07_REMOTE_SHA"], "")  # not the axon fixture
        self.assertEqual(first["FORCE_PUSH"], "NO")
        self.assertEqual(first["DIRECT_MAIN"], "NO")
        self.assertEqual(first["MERGED"], "NO")
        self.assertEqual(first["DEPLOYED"], "NO")
        self.assertIs(first["SWITCHAROO_INTEGRATED"], False)
        pushed = [e for e in switchboard.events
                  if e["type"] == "candidate.branch.pushed"]
        self.assertEqual(len(pushed), 1)
        self.assertEqual(pushed[0]["payload"]["remote_after"],
                         self.pair["feature_sha"])

        second = worker.handle(self._event())
        self.assertEqual(second["BRANCH_ALREADY_PRESENT"], "YES")
        self.assertEqual(second["REMOTE_HEAD_MATCH"], "YES")
        self.assertEqual(second["DUPLICATE_PR"], "NO")
        self.assertEqual(pr.creates, 1)  # created once, reused on rerun
        self.assertEqual(len(pr.prs), 1)
        again = sha(self.pair["remote"],
                    f"refs/heads/{self.pair['feature_branch']}")
        self.assertEqual(again, self.pair["feature_sha"])
        self.assertFalse(any("--force" in " ".join(cmd) or "-f" in cmd
                             for cmd in worker.git.command_log))

    def test_main_push_rejected(self):
        worker, pr, *_ = _worker(self, self.pair, self.court)
        git(self.pair["work"], "checkout", "main")
        event = self._event(
            branch="main",
            expected_local_head=self.pair["base_sha"],
            worktree=self.pair["work"],
        )
        # Court evidence still names the feature candidate; even a matching
        # court would not authorize main. Use a main-shaped court so we fail
        # on the branch policy, not on court mismatch.
        main_court = local_court(self.pair)
        main_court["source_head"] = self.pair["base_sha"]
        main_court["candidate"] = {
            **main_court["candidate"],
            "branch": "main",
            "head": self.pair["base_sha"],
        }
        worker.court_store = MemoryCourtStore({
            "court://local/bare-remote": main_court,
        })
        event["candidate"]["court_receipt_hash"] = content_sha(main_court)
        event["candidate"]["expected_local_head"] = self.pair["base_sha"]
        event["candidate"]["branch"] = "main"
        with self.assertRaises(GitCustodianRefused) as caught:
            worker.handle(event)
        self.assertIn("PROTECTED_BRANCH", caught.exception.code_detail())
        self.assertEqual(pr.creates, 0)
        # main on the bare remote must still be the seed, not rewritten
        self.assertEqual(sha(self.pair["remote"], "refs/heads/main"),
                         self.pair["base_sha"])

    def test_wrong_expected_sha_rejected(self):
        worker, pr, *_ = _worker(self, self.pair, self.court)
        wrong = "0" * 40
        lying_court = dict(self.court)
        lying_court["source_head"] = wrong
        lying_court["candidate"] = {**lying_court["candidate"], "head": wrong}
        worker.court_store = MemoryCourtStore(
            {"court://local/bare-remote": lying_court})
        event = self._event(
            expected_local_head=wrong,
            court_receipt_hash=content_sha(lying_court),
        )
        with self.assertRaises(GitCustodianRefused) as caught:
            worker.handle(event)
        self.assertIn("SHA_MISMATCH", caught.exception.code_detail())
        self.assertEqual(pr.creates, 0)
        ls = git(self.pair["remote"], "show-ref", "--heads",
                 self.pair["feature_branch"], check=False)
        self.assertNotEqual(ls.returncode, 0)

    def test_detached_head_rejected_unless_approved(self):
        worker, pr, *_ = _worker(self, self.pair, self.court)
        git(self.pair["work"], "checkout", "--detach", "HEAD")
        with self.assertRaises(GitCustodianRefused) as caught:
            worker.handle(self._event())
        self.assertIn("DETACHED_HEAD", caught.exception.code_detail())
        self.assertEqual(pr.creates, 0)

        approved = dict(FOUNDER_CANDIDATE_V1)
        approved["allowed"] = tuple(approved["allowed"]) + ("DETACHED_HEAD",)
        worker2, pr2, *_ = _worker(
            self, self.pair, self.court, authority=approved)
        event = self._event(expect_detached=True)
        receipt = worker2.handle(event)
        self.assertEqual(receipt["REMOTE_HEAD_MATCH"], "YES")
        self.assertEqual(pr2.creates, 1)

    def test_dirty_and_untracked_fail_closed(self):
        worker, pr, *_ = _worker(self, self.pair, self.court)
        write_file(self.pair["work"], "candidate.txt", "dirty\n")
        with self.assertRaises(GitCustodianRefused) as caught:
            worker.handle(self._event())
        self.assertIn("DIRTY", caught.exception.code_detail())
        git(self.pair["work"], "checkout", "--", "candidate.txt")
        write_file(self.pair["work"], "oops-untracked.txt", "nope\n")
        with self.assertRaises(GitCustodianRefused) as caught:
            worker.handle(self._event())
        self.assertIn("UNTRACKED", caught.exception.code_detail())
        self.assertEqual(pr.creates, 0)

    def test_remote_divergence_fails_closed_no_force(self):
        worker, pr, *_ = _worker(self, self.pair, self.court)
        worker.handle(self._event())
        # Diverge the remote via a second clone.
        other = os.path.join(self.tmpdir, "other")
        git(self.tmpdir, "clone", self.pair["remote"], other)
        git(other, "checkout", "-B", self.pair["feature_branch"],
            f"origin/{self.pair['feature_branch']}")
        write_file(other, "diverged.txt", "remote moved\n")
        git(other, "add", "diverged.txt")
        git(other, "commit", "-m", "remote divergence")
        git(other, "push", "origin", self.pair["feature_branch"])
        diverged_sha = sha(other)

        worker2, pr2, *_ = _worker(self, self.pair, self.court)
        with self.assertRaises(GitCustodianRefused) as caught:
            worker2.handle(self._event())
        self.assertIn("STOP_REMOTE_DIVERGENCE", caught.exception.code_detail())
        self.assertFalse(any(
            "--force" in " ".join(c) or "--force-with-lease" in " ".join(c)
            for c in worker2.git.command_log))
        self.assertEqual(
            sha(self.pair["remote"], f"refs/heads/{self.pair['feature_branch']}"),
            diverged_sha)

    def test_force_push_is_structurally_impossible(self):
        backend = RealGitBackend()
        sig = inspect.signature(backend.push_feature_branch)
        self.assertNotIn("force", sig.parameters)
        self.assertNotIn("force_with_lease", sig.parameters)
        with self.assertRaises(TypeError):
            backend.push_feature_branch(  # type: ignore[call-arg]
                self.pair["work"], "origin", self.pair["feature_branch"],
                self.pair["feature_sha"], force=True)

    def test_malformed_input_rejected(self):
        worker, pr, *_ = _worker(self, self.pair, self.court)
        with self.assertRaises(GitCustodianRefused):
            worker.handle({"type": "candidate.push.requested", "candidate": {}})
        with self.assertRaises(GitCustodianRefused):
            worker.handle({"type": "not.a.real.event",
                           "candidate": local_candidate(self.pair, court=self.court)})
        bad = local_candidate(self.pair, court=self.court)
        bad["schema_version"] = "nrh.git.candidate.v0"
        with self.assertRaises(GitCustodianRefused):
            worker.handle({"type": "candidate.push.requested", "candidate": bad})
        self.assertEqual(pr.creates, 0)

    def test_missing_and_failed_court_rejected(self):
        worker, pr, *_ = _worker(self, self.pair, self.court)
        worker.court_store = MemoryCourtStore({})
        with self.assertRaises(GitCustodianRefused) as caught:
            worker.handle(self._event())
        self.assertIn("COURT", caught.exception.code_detail())

        failed = dict(self.court)
        failed["independent_court"] = "FAIL"
        worker.court_store = MemoryCourtStore(
            {"court://local/bare-remote": failed})
        event = self._event()
        event["candidate"]["court_receipt_hash"] = content_sha(failed)
        with self.assertRaises(GitCustodianRefused) as caught:
            worker.handle(event)
        self.assertIn("COURT", caught.exception.code_detail())

        mismatched = dict(self.court)
        mismatched["mission_id"] = "SOME_OTHER_MISSION"
        worker.court_store = MemoryCourtStore(
            {"court://local/bare-remote": mismatched})
        event = self._event()
        event["candidate"]["court_receipt_hash"] = content_sha(mismatched)
        with self.assertRaises(GitCustodianRefused) as caught:
            worker.handle(event)
        self.assertIn("COURT", caught.exception.code_detail())
        self.assertEqual(pr.creates, 0)

    def test_missing_authority_rejected(self):
        worker, pr, *_ = _worker(self, self.pair, self.court)
        worker.authority_store = MemoryAuthorityStore({})
        with self.assertRaises(GitCustodianRefused) as caught:
            worker.handle(self._event())
        self.assertIn("AUTHORITY", caught.exception.code_detail())
        self.assertEqual(pr.creates, 0)

    def test_credential_values_never_logged(self):
        worker, pr, switchboard, stack = _worker(self, self.pair, self.court)
        event = self._event()
        event["candidate"]["remote_url_or_identity"] = (
            f"https://user:{PLANTED_CREDENTIAL}@example.invalid/org/repo.git"
        )
        # Identity is the local path; a credentialed URL that does not match
        # the real remote must fail closed — and still not log the secret.
        with self.assertRaises(GitCustodianRefused) as caught:
            worker.handle(event)
        self.assertNotIn(PLANTED_CREDENTIAL, str(caught.exception))
        self.assertNotIn(PLANTED_CREDENTIAL, json.dumps(caught.exception.detail,
                                                        default=str))
        with open(stack.ledger_path, encoding="utf-8") as fh:
            self.assertNotIn(PLANTED_CREDENTIAL, fh.read())
        for cmd in worker.git.command_log:
            self.assertNotIn(PLANTED_CREDENTIAL, " ".join(cmd))


def _rmtree(path):
    import shutil
    shutil.rmtree(path, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
