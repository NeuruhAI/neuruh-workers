"""SWITCHAROO first seam: sealed PRIME receipt → one Sentinel court job →
on court.passed, one Honey Pot / Git Custodian artifact.  No clipboard.
"""
from __future__ import annotations

import inspect
import os
import shutil
import tempfile
import unittest

from neuruh_workers import (
    OrganRegistrationSeam, OrganSeamDeclaration, ReceiptType, SeamNotAuthorized,
    Switchboard,
)
from neuruh_workers.events import AckState, DeadletterReason, EventType, SCHEMA_VERSION
from neuruh_workers.prime_receipts import seal_receipt_text
from neuruh_workers.switchboard import T07_HEAD, T07_MISSION, T07_STAGE, _CrashSimulated
from neuruh_workers.switchboard_jobs import (
    GIT_PUSH, HONEY_POT_WORKFLOW, REMEDIATION_WORKFLOW, SENTINEL_MODEL_EXECUTION,
    SENTINEL_WORKFLOW, materialize_candidate_push, materialize_court_artifact,
)
from tests.helpers import fixed_clock, make_stack

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
T07_FIXTURE = os.path.join(REPO_ROOT, "tests", "fixtures", "t07", "T07_RECEIPT.txt")
SYNTHETIC_MISSION = "NEURUH_SWITCHAROO_SYNTHETIC_SEAM_V1"
SYNTHETIC_HEAD = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"


def _board(testcase, *, missions=None, crash_after_lease=False, fail_handler=None,
           receipts=None):
    tmp = tempfile.mkdtemp(prefix="neuruh-switchboard-")
    testcase.addCleanup(lambda: shutil.rmtree(tmp, ignore_errors=True))
    receipts_dir = receipts or os.path.join(tmp, "receipts")
    os.makedirs(receipts_dir, exist_ok=True)
    return Switchboard(
        os.path.join(tmp, "state"),
        receipts_dir=receipts_dir,
        known_missions=frozenset(missions or {T07_MISSION, SYNTHETIC_MISSION}),
        clock=fixed_clock(),
        crash_after_lease=crash_after_lease,
        fail_handler=fail_handler,
    )


def _write_receipt(directory, name, body: str) -> str:
    path = os.path.join(directory, name)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(seal_receipt_text(body))
    return path


def _synthetic_body(**overrides) -> str:
    fields = {
        "MISSION": SYNTHETIC_MISSION,
        "STAGE": "SYNTHETIC_FIRST_SEAM",
        "PLAN_ID": "SYNTHETIC_FIRST_SEAM",
        "TASK_ID": "synthetic-first-seam",
        "REPO": "NeuruhAI/neuruh-workers",
        "WORKTREE": "/tmp/synthetic-worktree",
        "BRANCH": "feature/synthetic-first-seam",
        "HEAD": SYNTHETIC_HEAD,
        "REMOTE": "origin",
        "SOURCE_ROLE": "PRIME",
        "SOURCE_COMPONENT": "prime-builder",
        "SEALED": "YES",
        "AUTHORITY_REF": "agc:r0:synthetic-first-seam",
        "BIND": "127.0.0.1:9",
        "CLAIMANT": "PRIME",
        "EXPECTED_GIT_OUTCOME_PR": "",
    }
    fields.update(overrides)
    return "\n".join(f"{k}={v}" for k, v in fields.items()) + "\n"


def _ingest_pass(board: Switchboard, *, passed: bool):
    sealed = board.events(EventType.RECEIPT_SEALED)[-1]
    court = board.events(EventType.COURT_REQUESTED)[-1]
    return board.record_court_outcome(
        passed=passed,
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
        verdict_note="independent Sentinel close (fixture); Switchboard did not PASS",
    )


class T07FirstSeamTests(unittest.TestCase):
    def setUp(self):
        self.board = _board(self)
        shutil.copy(T07_FIXTURE, os.path.join(self.board.receipts_dir, "T07_RECEIPT.txt"))

    def test_t07_observed_once_validated_once_one_court_job(self):
        report = self.board.tick()
        self.assertEqual(report.observed, ["T07_RECEIPT.txt"])
        created = self.board.events(EventType.RECEIPT_CREATED)
        sealed = self.board.events(EventType.RECEIPT_SEALED)
        court = self.board.events(EventType.COURT_REQUESTED)
        self.assertEqual(len(created), 1)
        self.assertEqual(len(sealed), 1)
        self.assertEqual(len(court), 1)
        jobs = self.board.jobs_for(SENTINEL_WORKFLOW)
        self.assertEqual(len(jobs), 1)
        state = self.board.queue.load(jobs[0])
        self.assertEqual(state.status.value, "SUCCEEDED")
        artifact = state.step_results["materialize"]
        self.assertEqual(artifact["sentinel_model_execution"], SENTINEL_MODEL_EXECUTION)
        payload = court[0]["payload_ref_or_payload"]
        for key in ("court_job_id", "mission", "stage", "claimer", "receipt_ref",
                    "source_head", "required_invariants", "authority_ref"):
            self.assertIn(key, payload)
        self.assertEqual(payload["mission"], T07_MISSION)
        self.assertEqual(payload["stage"], T07_STAGE)
        self.assertEqual(payload["source_head"], T07_HEAD)
        self.assertEqual(payload["court_job_id"], jobs[0])
        self.board.tick()
        self.assertEqual(len(self.board.events(EventType.COURT_REQUESTED)), 1)
        self.assertEqual(len(self.board.jobs_for(SENTINEL_WORKFLOW)), 1)

    def test_sentinel_pass_materialises_exactly_one_honey_pot_no_git_push(self):
        self.board.tick()
        _ingest_pass(self.board, passed=True)
        report = self.board.tick()
        pushes = self.board.events(EventType.CANDIDATE_PUSH_REQUESTED)
        self.assertEqual(len(pushes), 1)
        jobs = self.board.jobs_for(HONEY_POT_WORKFLOW)
        self.assertEqual(len(jobs), 1)
        self.assertIn(jobs[0], report.jobs_created)
        artifact = self.board.queue.load(jobs[0]).step_results["materialize"]
        self.assertEqual(artifact["git_push"], GIT_PUSH)
        self.assertEqual(artifact["expected_pr"], "4")
        self.assertEqual(artifact["head"], T07_HEAD)
        self.assertEqual(artifact["repo"], "NeuruhAI/neuruh-axon")
        for key in ("mission_id", "task_id", "repo", "worktree", "branch", "head",
                    "remote", "court_receipt", "authority_ref", "idempotency_key"):
            self.assertIn(key, artifact)
        self.assertNotIn("git push", inspect.getsource(materialize_candidate_push))
        self.assertNotIn("git push", inspect.getsource(materialize_court_artifact))
        self.assertEqual(len(self.board.jobs_for(REMEDIATION_WORKFLOW)), 0)
        self.board.tick()
        self.assertEqual(len(self.board.events(EventType.CANDIDATE_PUSH_REQUESTED)), 1)

    def test_chain_preserves_correlation_and_causation(self):
        self.board.tick()
        _ingest_pass(self.board, passed=True)
        self.board.tick()
        created = self.board.events(EventType.RECEIPT_CREATED)[0]
        sealed = self.board.events(EventType.RECEIPT_SEALED)[0]
        court = self.board.events(EventType.COURT_REQUESTED)[0]
        passed = self.board.events(EventType.COURT_PASSED)[0]
        push = self.board.events(EventType.CANDIDATE_PUSH_REQUESTED)[0]
        corr = created["correlation_id"]
        for event in (sealed, court, passed, push):
            self.assertEqual(event["correlation_id"], corr)
        self.assertEqual(sealed["causation_id"], created["event_id"])
        self.assertEqual(court["causation_id"], sealed["event_id"])
        self.assertEqual(passed["causation_id"], court["event_id"])
        self.assertEqual(push["causation_id"], passed["event_id"])
        for event in (created, sealed, court, passed, push):
            self.assertEqual(event["schema_version"], SCHEMA_VERSION)
            self.assertEqual(event["ack_state"], AckState.PENDING.value)


class SyntheticAndIdempotencyTests(unittest.TestCase):
    def test_synthetic_fresh_fixture_same_path(self):
        board = _board(self)
        _write_receipt(board.receipts_dir, "SYNTHETIC_RECEIPT.txt", _synthetic_body())
        board.tick()
        self.assertEqual(len(board.events(EventType.COURT_REQUESTED)), 1)
        self.assertEqual(len(board.jobs_for(SENTINEL_WORKFLOW)), 1)
        _ingest_pass(board, passed=True)
        board.tick()
        self.assertEqual(len(board.events(EventType.CANDIDATE_PUSH_REQUESTED)), 1)
        artifact = board.queue.load(board.jobs_for(HONEY_POT_WORKFLOW)[0]).step_results["materialize"]
        self.assertEqual(artifact["git_push"], GIT_PUSH)
        self.assertEqual(artifact["head"], SYNTHETIC_HEAD)

    def test_replay_of_same_events_produces_no_duplicate_jobs(self):
        board = _board(self)
        _write_receipt(board.receipts_dir, "SYNTHETIC_RECEIPT.txt", _synthetic_body())
        board.tick()
        _ingest_pass(board, passed=True)
        board.tick()
        # Replay: drop the same receipt "again" (identical bytes) and tick twice more.
        board.tick()
        fresh = board.reopen()
        fresh.tick()
        self.assertEqual(len(fresh.events(EventType.RECEIPT_SEALED)), 1)
        self.assertEqual(len(fresh.events(EventType.COURT_REQUESTED)), 1)
        self.assertEqual(len(fresh.events(EventType.CANDIDATE_PUSH_REQUESTED)), 1)
        self.assertEqual(len(fresh.jobs_for(SENTINEL_WORKFLOW)), 1)
        self.assertEqual(len(fresh.jobs_for(HONEY_POT_WORKFLOW)), 1)
        types = [r.receipt_type for r in fresh.job_ledger.receipts(
            fresh.jobs_for(SENTINEL_WORKFLOW)[0])]
        self.assertEqual(types.count(ReceiptType.JOB_ENQUEUED), 1)

    def test_kill_restart_unacked_event_recovers_without_duplicate(self):
        board = _board(self, crash_after_lease=True)
        _write_receipt(board.receipts_dir, "SYNTHETIC_RECEIPT.txt", _synthetic_body())
        with self.assertRaises(_CrashSimulated):
            board.tick()
        self.assertEqual(len(board.jobs_for(SENTINEL_WORKFLOW)), 0)
        leased = board._consumer_state("switchboard.court-on-sealed.v1")["lease"]
        self.assertEqual(leased["state"], AckState.LEASED.value)
        fresh = board.reopen()
        report = fresh.tick()
        self.assertTrue(report.recovered)
        self.assertEqual(len(fresh.jobs_for(SENTINEL_WORKFLOW)), 1)
        fresh.tick()
        self.assertEqual(len(fresh.jobs_for(SENTINEL_WORKFLOW)), 1)
        self.assertEqual(len(fresh.events(EventType.COURT_REQUESTED)), 1)


class FailClosedTests(unittest.TestCase):
    def test_tampered_hash_deadlettered_no_court_job(self):
        board = _board(self)
        path = _write_receipt(board.receipts_dir, "bad.txt", _synthetic_body())
        with open(path) as fh:
            text = fh.read()
        with open(path, "w") as fh:
            fh.write(text.replace(
                f"PAYLOAD_HASH={text.split('PAYLOAD_HASH=')[-1].strip()}",
                "PAYLOAD_HASH=" + "0" * 64))
        board.tick()
        self.assertEqual(len(board.jobs_for(SENTINEL_WORKFLOW)), 0)
        self.assertEqual(len(board.events(EventType.COURT_REQUESTED)), 0)
        dls = board.deadletters()
        self.assertTrue(dls)
        self.assertEqual(dls[0]["payload_ref_or_payload"]["reason"],
                         DeadletterReason.BROKEN_HASH.value)
        self.assertTrue(dls[0]["payload_ref_or_payload"]["original_retained"])

    def test_invalid_schema_deadletter_original_retained(self):
        board = _board(self)
        dead = board.ingest_raw({"not": "an envelope", "mission_id": SYNTHETIC_MISSION})
        self.assertEqual(dead["event_type"], EventType.DEADLETTER_CREATED.value)
        payload = dead["payload_ref_or_payload"]
        self.assertEqual(payload["reason"], DeadletterReason.INVALID_SCHEMA.value)
        self.assertTrue(payload["original_retained"])
        self.assertEqual(payload["original_event"]["not"], "an envelope")
        self.assertEqual(len(board.jobs_for(SENTINEL_WORKFLOW)), 0)

    def test_missing_and_invalid_authority_fail_closed(self):
        board = _board(self)
        _write_receipt(board.receipts_dir, "noauth.txt",
                       _synthetic_body(AUTHORITY_REF="none"))
        board.tick()
        self.assertEqual(len(board.jobs_for(SENTINEL_WORKFLOW)), 0)
        reasons = [d["payload_ref_or_payload"]["reason"] for d in board.deadletters()]
        self.assertIn(DeadletterReason.AUTHORITY_INVALID.value, reasons)

        board2 = _board(self)
        _write_receipt(board2.receipts_dir, "missing.txt",
                       _synthetic_body(AUTHORITY_REF=""))
        board2.tick()
        self.assertEqual(len(board2.jobs_for(SENTINEL_WORKFLOW)), 0)

    def test_court_fail_zero_push_jobs_plus_remediation(self):
        board = _board(self)
        _write_receipt(board.receipts_dir, "SYNTHETIC_RECEIPT.txt", _synthetic_body())
        board.tick()
        _ingest_pass(board, passed=False)
        board.tick()
        self.assertEqual(len(board.events(EventType.CANDIDATE_PUSH_REQUESTED)), 0)
        self.assertEqual(len(board.jobs_for(HONEY_POT_WORKFLOW)), 0)
        self.assertEqual(len(board.jobs_for(REMEDIATION_WORKFLOW)), 1)
        self.assertEqual(len(board.events(EventType.TASK_READY)), 1)
        artifact = board.queue.load(
            board.jobs_for(REMEDIATION_WORKFLOW)[0]).step_results["materialize"]
        self.assertEqual(artifact["kind"], "remediation")
        self.assertEqual(artifact["git_push"], GIT_PUSH)

    def test_max_retries_deadletter(self):
        def boom(consumer_id):
            raise RuntimeError("induced consumer failure")

        board = _board(self, fail_handler=boom)
        _write_receipt(board.receipts_dir, "SYNTHETIC_RECEIPT.txt", _synthetic_body())
        for _ in range(4):
            board.tick()
        reasons = [d["payload_ref_or_payload"]["reason"] for d in board.deadletters()]
        self.assertIn(DeadletterReason.MAX_RETRIES.value, reasons)
        self.assertEqual(len(board.jobs_for(SENTINEL_WORKFLOW)), 0)

    def test_unknown_mission_deadletter(self):
        board = _board(self, missions={T07_MISSION})
        _write_receipt(board.receipts_dir, "x.txt", _synthetic_body())
        board.tick()
        reasons = [d["payload_ref_or_payload"]["reason"] for d in board.deadletters()]
        self.assertIn(DeadletterReason.UNKNOWN_MISSION.value, reasons)
        self.assertEqual(len(board.jobs_for(SENTINEL_WORKFLOW)), 0)

    def test_unknown_source_head_deadletter(self):
        board = _board(self)
        _write_receipt(board.receipts_dir, "x.txt", _synthetic_body(HEAD="not-a-sha"))
        board.tick()
        reasons = [d["payload_ref_or_payload"]["reason"] for d in board.deadletters()]
        self.assertIn(DeadletterReason.UNKNOWN_SOURCE_HEAD.value, reasons)

    def test_secrets_absent_from_logs_and_event_ledger(self):
        planted = "secret_" + "a1B2c3D4e5" * 3
        board = _board(self)
        _write_receipt(board.receipts_dir, "leaky.txt",
                       _synthetic_body() + f"TOKEN={planted}\n")
        board.tick()
        with open(board.event_ledger._path) as fh:
            ledger = fh.read()
        self.assertNotIn(planted, ledger)
        self.assertIn("[REDACTED]", ledger)
        self.assertEqual(len(board.jobs_for(SENTINEL_WORKFLOW)), 0)

    def test_activate_still_refuses_and_seams_unchanged(self):
        stack = make_stack(self)
        seam = OrganRegistrationSeam(stack.ledger)
        seam.declare(OrganSeamDeclaration(
            organ="axon", endpoint="http://127.0.0.1:8300",
            source_sha="f3a94d1a68ac6b07525e962692901411bd43e6bf"))
        with self.assertRaises(SeamNotAuthorized):
            seam.activate("axon")
        refused = [r for r in stack.ledger.receipts("seam-registry")
                   if r.receipt_type == ReceiptType.SEAM_ACTIVATION_REFUSED]
        self.assertEqual(len(refused), 1)
        source = inspect.getsource(OrganRegistrationSeam.activate)
        self.assertIn("always refuses", source)

    def test_no_public_bind_loopback_only(self):
        with self.assertRaises(Exception) as caught:
            Switchboard("/tmp/nope", receipts_dir="/tmp/nope-r", bind_host="0.0.0.0")
        self.assertIn("loopback", str(caught.exception).lower())
        board = _board(self)
        with self.assertRaises(Exception) as caught:
            board.listen("8.8.8.8", 80)
        self.assertIn("public bind refused", str(caught.exception))
        with self.assertRaises(Exception) as caught:
            board.listen("127.0.0.1", 9)
        self.assertIn("no network listener", str(caught.exception))
        self.assertEqual(board.bind_host, "127.0.0.1")

    def test_t07_fixture_is_documented_reconstruction(self):
        with open(T07_FIXTURE, encoding="utf-8") as fh:
            text = fh.read()
        self.assertIn("RECONSTRUCTION", text)
        self.assertIn(T07_MISSION, text)
        self.assertIn(T07_STAGE, text)
        self.assertIn(T07_HEAD, text)
        self.assertIn("EXPECTED_GIT_OUTCOME_PR=4", text)
        self.assertIn("~/.neuruh-receipts/grok-sovereign-v4/T07_RECEIPT.txt", text)


if __name__ == "__main__":
    unittest.main()
