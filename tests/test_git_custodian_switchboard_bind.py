"""Switchboard bind: Git Custodian consumes nrh.event.v1 candidate.push.requested."""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import unittest

from neuruh_workers import Switchboard
from neuruh_workers.contracts import content_sha
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
from tests.canonical_fixture import OfflineCanonical
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


def _receipt_body(
    pair, candidate, *, mission=MISSION, stage="BARE_REMOTE_PUSH",
):
    candidate_json = json.dumps(
        candidate, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
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
        "CODE_CUSTODY_REQUIRED=YES",
        f"CANDIDATE_JSON={candidate_json}",
        f"CANDIDATE_SHA256={content_sha(candidate)}",
    ]) + "\n"


class _SealedRecordStore:
    """The production FileRecordStore contract: an absolute ref is a local JSON file."""

    def get(self, ref):
        if not isinstance(ref, str) or not os.path.isabs(ref) or not os.path.isfile(ref):
            return None
        with open(ref, "r", encoding="utf-8") as fh:
            return json.load(fh)


class SwitchboardGitCustodianBindTests(unittest.TestCase):
    def setUp(self):
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
            # Production binds FileRecordStore over the board's record dirs: the
            # sealed Sentinel record the board ingested is the court custody binds.
            court_store=_SealedRecordStore(),
            authority_store=MemoryAuthorityStore(
                {T07_AUTHORITY_REF: FOUNDER_CANDIDATE_V1}),
            switchboard=self.switchboard_log,
            ledger=stack.ledger,
        )
        state = os.path.join(self.tmpdir, "switchboard")
        receipts = os.path.join(self.tmpdir, "receipts")
        os.makedirs(receipts, exist_ok=True)
        # The seeded pair's origin IS a bare repository on disk: the board reads
        # main off it, and the fixture observer refuses any other worktree.
        self.canonical = OfflineCanonical(self.pair["work"], self.pair["remote"])
        self.board = Switchboard(
            state,
            receipts_dir=receipts,
            known_missions=frozenset({MISSION}),
            git_custodian=self.worker,
            canonical_observer=self.canonical.observe,
        )

    def test_switchboard_emits_envelope_git_custodian_consumes_and_pushes(self):
        _write_receipt(self.board.receipts_dir, "BIND_RECEIPT.txt",
                       _receipt_body(self.pair, self.candidate))
        self.board.tick()
        self.assertEqual(len(self.board.events(EventType.CANDIDATE_PUSH_REQUESTED)), 0)

        sealed = self.board.events(EventType.RECEIPT_SEALED)[-1]
        court = self.board.events(EventType.COURT_REQUESTED)[-1]
        request = court["payload_ref_or_payload"]
        # The board read main off the bare remote: the base is the seeded main,
        # not the feature commit under judgement, and the record it accepts says
        # so (candidate_descends_from_canonical_base, derived from this request).
        self.assertEqual(request["canonical_sha"], self.pair["base_sha"])
        self.assertNotEqual(request["canonical_sha"], self.pair["feature_sha"])
        self.assertEqual(request["canonical_repo"], self.pair["remote"])
        self.assertNotIn("canonical_base", request["binding_gaps"])
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
            evidence=_independent_evidence(
                self.board, sealed, court, passed=True),
        )
        self.board.tick()  # court.passed → emit candidate.push.requested
        pushes = self.board.events(EventType.CANDIDATE_PUSH_REQUESTED)
        self.assertEqual(len(pushes), 1)
        envelope = validate_envelope(pushes[0])
        self.assertEqual(envelope["schema_version"], SCHEMA_VERSION)
        self.assertEqual(envelope["event_type"],
                         EventType.CANDIDATE_PUSH_REQUESTED.value)
        self.assertIn("candidate", envelope["payload_ref_or_payload"])
        # The producer named its candidate before any court existed; the board
        # bound it to the sealed Sentinel record that actually judged it.
        bound = envelope["payload_ref_or_payload"]["candidate"]
        passed = self.board.events(EventType.COURT_PASSED)[-1]["payload_ref_or_payload"]["evidence"]
        self.assertEqual(bound["court_receipt_ref"], passed["ref"])
        self.assertEqual(bound["court_receipt_hash"], passed["record_sha"])
        with open(passed["ref"], "r", encoding="utf-8") as fh:
            sealed_record = json.load(fh)
        self.assertEqual(sealed_record["kind"], "independent_sentinel_court")
        self.assertEqual(content_sha(sealed_record), bound["court_receipt_hash"])

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
                       _receipt_body(self.pair, self.candidate))
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
            evidence=_independent_evidence(
                self.board, sealed, court, passed=False),
        )
        self.board.tick()
        self.board.tick()
        self.assertEqual(len(self.board.events(EventType.CANDIDATE_PUSH_REQUESTED)), 0)
        self.assertEqual(self.pr.creates, 0)
        ls = git(self.pair["remote"], "show-ref", "--heads",
                 self.pair["feature_branch"], check=False)
        self.assertNotEqual(ls.returncode, 0)

    def test_bare_pass_string_still_refused_on_bound_path(self):
        # A bare PASS string where the sealed record should be is still refused.
        class _BarePass:
            def get(self, ref):
                return "PASS"
        self.worker.court_store = _BarePass()
        _write_receipt(self.board.receipts_dir, "BIND_RECEIPT.txt",
                       _receipt_body(self.pair, self.candidate))
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
            evidence=_independent_evidence(
                self.board, sealed, court, passed=True),
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
    shutil.rmtree(path, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
