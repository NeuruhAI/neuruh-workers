"""Switchboard bind: Git Custodian consumes nrh.event.v1 candidate.push.requested."""
from __future__ import annotations

import os
import unittest

from neuruh_workers import Switchboard
from neuruh_workers.events import EventType, SCHEMA_VERSION, validate_envelope
from neuruh_workers.git_custodian import (
    FOUNDER_CANDIDATE_V1,
    GitCustodian,
    MemoryAuthorityStore,
    MemoryCourtStore,
    MockCIAdapter,
    MockPRAdapter,
    RealGitBackend,
    RecordingSwitchboard,
    SWITCHAROO_INTEGRATED,
)
from tests.git_custodian_fixtures import (
    T07_AUTHORITY_REF,
    git,
    local_candidate,
    local_court,
    make_seeded_pair,
    sha,
)
from tests.helpers import make_stack
from tests.test_switchboard_first_seam import _independent_evidence, _write_receipt

MISSION = "LOCAL_CUSTODIAN_SWITCHAROO_BIND"


def _receipt_body(pair, *, mission=MISSION, stage="BARE_REMOTE_PUSH"):
    return "\n".join([
        f"MISSION={mission}",
        f"STAGE={stage}",
        f"TASK_ID={stage}",
        "REPO=local/custodian-fixture",
        f"WORKTREE={pair['work']}",
        f"BRANCH={pair['feature_branch']}",
        f"HEAD={pair['feature_sha']}",
        "REMOTE=origin",
        "SOURCE_ROLE=PRIME",
        "SOURCE_COMPONENT=prime-builder",
        "SEALED=YES",
        f"AUTHORITY_REF={T07_AUTHORITY_REF}",
        "BIND=127.0.0.1:9",
        "CLAIMANT=PRIME",
        "EXPECTED_GIT_OUTCOME_PR=",
    ]) + "\n"


class SwitchboardGitCustodianBindTests(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.tmpdir = tempfile.mkdtemp(prefix="neuruh-bind-")
        self.addCleanup(_rmtree, self.tmpdir)
        self.pair = make_seeded_pair(self.tmpdir)
        self.court = local_court(self.pair, mission_id=MISSION)
        self.candidate = local_candidate(
            self.pair, court=self.court, mission_id=MISSION)
        stack = make_stack(self)
        self.pr = MockPRAdapter()
        self.switchboard_log = RecordingSwitchboard()
        self.worker = GitCustodian(
            git=RealGitBackend(),
            pr=self.pr,
            ci=MockCIAdapter(),
            court_store=MemoryCourtStore(
                {"court://local/bare-remote": self.court}),
            authority_store=MemoryAuthorityStore(
                {T07_AUTHORITY_REF: FOUNDER_CANDIDATE_V1}),
            switchboard=self.switchboard_log,
            ledger=stack.ledger,
        )
        state = os.path.join(self.tmpdir, "switchboard")
        receipts = os.path.join(self.tmpdir, "receipts")
        os.makedirs(receipts, exist_ok=True)
        self.board = Switchboard(
            state,
            receipts_dir=receipts,
            known_missions=frozenset({MISSION}),
            git_custodian=self.worker,
        )

    def test_switchboard_emits_envelope_git_custodian_consumes_and_pushes(self):
        _write_receipt(self.board.receipts_dir, "BIND_RECEIPT.txt",
                       _receipt_body(self.pair))
        self.board.tick()
        self.assertEqual(len(self.board.events(EventType.CANDIDATE_PUSH_REQUESTED)), 0)

        sealed = self.board.events(EventType.RECEIPT_SEALED)[-1]
        court = self.board.events(EventType.COURT_REQUESTED)[-1]
        self.board.record_court_outcome(
            passed=True,
            court_job_id=court["payload_ref_or_payload"]["court_job_id"],
            mission_id=sealed["mission_id"],
            plan_id=sealed["plan_id"],
            task_id=sealed["task_id"],
            correlation_id=sealed["correlation_id"],
            causation_id=court["event_id"],
            source_head=sealed["source_head"],
            authority_ref=sealed["authority_ref"],
            source_repo=sealed["source_repo"],
            receipt_ref=sealed["payload_ref_or_payload"]["receipt_ref"],
            evidence=_independent_evidence(),
            extra_payload={"honey_pot_expected": {
                "repo": self.candidate["repo"],
                "branch": self.candidate["branch"],
                "head": self.candidate["expected_local_head"],
                "candidate": self.candidate,
            }},
        )
        self.board.tick()  # court.passed → emit candidate.push.requested
        pushes = self.board.events(EventType.CANDIDATE_PUSH_REQUESTED)
        self.assertEqual(len(pushes), 1)
        envelope = validate_envelope(pushes[0])
        self.assertEqual(envelope["schema_version"], SCHEMA_VERSION)
        self.assertEqual(envelope["event_type"],
                         EventType.CANDIDATE_PUSH_REQUESTED.value)
        self.assertIn("candidate", envelope["payload_ref_or_payload"])

        self.board.tick()  # git-custodian consumer
        remote_sha = sha(self.pair["remote"],
                         f"refs/heads/{self.pair['feature_branch']}")
        self.assertEqual(remote_sha, self.pair["feature_sha"])
        self.assertEqual(self.pr.creates, 1)
        self.assertTrue(self.pr.prs[0].draft)
        self.assertEqual(len(self.pr.prs), 1)

        self.board.tick()  # idempotent rerun
        self.assertEqual(self.pr.creates, 1)
        self.assertEqual(len(self.pr.prs), 1)
        self.assertEqual(
            sha(self.pair["remote"], f"refs/heads/{self.pair['feature_branch']}"),
            self.pair["feature_sha"])
        self.assertFalse(any(
            "--force" in " ".join(c) or "--force-with-lease" in " ".join(c)
            for c in self.worker.git.command_log))
        self.assertTrue(SWITCHAROO_INTEGRATED)

    def test_court_fail_does_not_dispatch_git_custodian(self):
        _write_receipt(self.board.receipts_dir, "BIND_RECEIPT.txt",
                       _receipt_body(self.pair))
        self.board.tick()
        sealed = self.board.events(EventType.RECEIPT_SEALED)[-1]
        court = self.board.events(EventType.COURT_REQUESTED)[-1]
        self.board.record_court_outcome(
            passed=False,
            court_job_id=court["payload_ref_or_payload"]["court_job_id"],
            mission_id=sealed["mission_id"],
            plan_id=sealed["plan_id"],
            task_id=sealed["task_id"],
            correlation_id=sealed["correlation_id"],
            causation_id=court["event_id"],
            source_head=sealed["source_head"],
            authority_ref=sealed["authority_ref"],
            source_repo=sealed["source_repo"],
            receipt_ref=sealed["payload_ref_or_payload"]["receipt_ref"],
            evidence=_independent_evidence(),
        )
        self.board.tick()
        self.board.tick()
        self.assertEqual(len(self.board.events(EventType.CANDIDATE_PUSH_REQUESTED)), 0)
        self.assertEqual(self.pr.creates, 0)
        ls = git(self.pair["remote"], "show-ref", "--heads",
                 self.pair["feature_branch"], check=False)
        self.assertNotEqual(ls.returncode, 0)

    def test_bare_pass_string_still_refused_on_bound_path(self):
        self.worker.court_store = MemoryCourtStore(
            {"court://local/bare-remote": "PASS"})
        _write_receipt(self.board.receipts_dir, "BIND_RECEIPT.txt",
                       _receipt_body(self.pair))
        self.board.tick()
        sealed = self.board.events(EventType.RECEIPT_SEALED)[-1]
        court = self.board.events(EventType.COURT_REQUESTED)[-1]
        self.board.record_court_outcome(
            passed=True,
            court_job_id=court["payload_ref_or_payload"]["court_job_id"],
            mission_id=sealed["mission_id"],
            plan_id=sealed["plan_id"],
            task_id=sealed["task_id"],
            correlation_id=sealed["correlation_id"],
            causation_id=court["event_id"],
            source_head=sealed["source_head"],
            authority_ref=sealed["authority_ref"],
            source_repo=sealed["source_repo"],
            receipt_ref=sealed["payload_ref_or_payload"]["receipt_ref"],
            evidence=_independent_evidence(),
            extra_payload={"honey_pot_expected": {
                "candidate": self.candidate,
            }},
        )
        self.board.tick()  # emit candidate.push.requested
        self.board.tick()  # consume attempt 1 (refuses bare PASS)
        self.board.tick()
        self.board.tick()
        self.assertEqual(self.pr.creates, 0)
        ls = git(self.pair["remote"], "show-ref", "--heads",
                 self.pair["feature_branch"], check=False)
        self.assertNotEqual(ls.returncode, 0)
        dead = self.board.deadletters()
        self.assertTrue(dead)


def _rmtree(path):
    import shutil
    shutil.rmtree(path, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
