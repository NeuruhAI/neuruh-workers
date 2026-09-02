"""SWITCHAROO first seam: sealed PRIME receipt → one Sentinel court job →
on court.passed, one Honey Pot / Git Custodian artifact.  No clipboard.
"""
from __future__ import annotations

import inspect
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

from neuruh_workers import (
    HUMAN_CLIPBOARD_REQUIRED,
    OrganRegistrationSeam, OrganSeamDeclaration, ReceiptType, SeamNotAuthorized,
    Switchboard,
)
from neuruh_workers.events import AckState, DeadletterReason, EventType, SCHEMA_VERSION
from neuruh_workers.ledger import ExecutionLedger
from neuruh_workers.prime_receipts import seal_receipt_text
from neuruh_workers.queue import JobQueue
from neuruh_workers.switchboard import (
    INDEPENDENT_COURT_KIND, T07_AXON_BRANCH, T07_AXON_PR, T07_AXON_PR_URL,
    T07_HEAD, T07_MISSION, T07_STAGE, SwitchboardError, _CrashSimulated,
)
from neuruh_workers.switchboard_jobs import (
    GIT_PUSH, HONEY_POT_WORKFLOW, REMEDIATION_WORKFLOW, SENTINEL_MODEL_EXECUTION,
    SENTINEL_WORKFLOW, materialize_candidate_push, materialize_court_artifact,
)
from tests.helpers import fixed_clock, make_stack

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
T07_FIXTURE = os.path.join(REPO_ROOT, "tests", "fixtures", "t07", "T07_RECEIPT.txt")
T07_COURT_FIXTURE = os.path.join(
    REPO_ROOT, "tests", "fixtures", "t07", "T07_OBSERVER_COURT.json")
T07_WORKTREE = (
    "/Users/jeramiehicks/neuruh-worktrees/grok-sovereign-mcp-v4-20260827/neuruh-axon")
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


def _independent_evidence(ref: str = "test://sentinel-independent-close") -> dict:
    return {
        "kind": INDEPENDENT_COURT_KIND,
        "source_role": "SENTINEL",
        "independent": True,
        "ref": ref,
    }


def _ingest_pass(board: Switchboard, *, passed: bool, extra_payload=None):
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
        evidence=_independent_evidence(),
        verdict_note="independent Sentinel close; Switchboard did not PASS",
        extra_payload=extra_payload,
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
        # Clipboard defect: receipt claims sealed but transport was human/file.
        defect = sealed[0]["payload_ref_or_payload"]["clipboard_defect"]
        self.assertEqual(defect["event_target"], "receipt.sealed")
        self.assertEqual(defect["event_implementation_status"], "NOT_PROVEN")
        self.assertIn("receipt_file", defect["transport"])
        self.assertEqual(defect["sentinel_closure_in_receipt"], "PENDING")
        # SENTINEL_CLOSURE=PENDING and PRIME_VERDICT=PASS are not a court PASS.
        self.assertEqual(len(self.board.events(EventType.COURT_PASSED)), 0)
        self.assertEqual(len(self.board.events(EventType.CANDIDATE_PUSH_REQUESTED)), 0)
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
        self.assertEqual(payload["claimer"], "NEURUH_PRIME")
        self.assertEqual(payload["court_job_id"], jobs[0])
        self.board.tick()
        self.assertEqual(len(self.board.events(EventType.COURT_REQUESTED)), 1)
        self.assertEqual(len(self.board.jobs_for(SENTINEL_WORKFLOW)), 1)

    def test_independent_court_pass_materialises_exactly_one_honey_pot_no_git_push(self):
        """Second seam: court.passed from independent evidence, never from T07 reconstruction."""
        self.board.tick()
        with self.assertRaises(SwitchboardError) as refused:
            self.board.ingest_court_fixture(T07_COURT_FIXTURE)
        self.assertIn("reconstruction", str(refused.exception).lower())
        self.assertEqual(len(self.board.events(EventType.COURT_PASSED)), 0)
        _ingest_pass(self.board, passed=True, extra_payload={
            "summary": {"pass_count": 1, "fail_count": 0},
            "honey_pot_expected": {
                "repo": "NeuruhAI/neuruh-axon",
                "branch": T07_AXON_BRANCH,
                "head": T07_HEAD,
                "pr_number": T07_AXON_PR,
                "pr_url": T07_AXON_PR_URL,
                "pr_draft": "YES",
            },
        })
        report = self.board.tick()
        passed = self.board.events(EventType.COURT_PASSED)
        self.assertEqual(len(passed), 1)
        evidence = passed[0]["payload_ref_or_payload"]["evidence"]
        self.assertEqual(evidence["kind"], INDEPENDENT_COURT_KIND)
        self.assertEqual(evidence["source_role"], "SENTINEL")
        self.assertTrue(evidence["independent"])
        self.assertEqual(passed[0]["source_role"], "SENTINEL")
        pushes = self.board.events(EventType.CANDIDATE_PUSH_REQUESTED)
        self.assertEqual(len(pushes), 1)
        jobs = self.board.jobs_for(HONEY_POT_WORKFLOW)
        self.assertEqual(len(jobs), 1)
        self.assertIn(jobs[0], report.jobs_created)
        artifact = self.board.queue.load(jobs[0]).step_results["materialize"]
        self.assertEqual(artifact["git_push"], GIT_PUSH)
        self.assertEqual(artifact["expected_pr"], T07_AXON_PR)
        self.assertEqual(artifact["expected_pr_url"], T07_AXON_PR_URL)
        self.assertEqual(artifact["expected_pr_draft"], "YES")
        self.assertEqual(artifact["head"], T07_HEAD)
        self.assertEqual(artifact["repo"], "NeuruhAI/neuruh-axon")
        self.assertEqual(artifact["branch"], T07_AXON_BRANCH)
        self.assertEqual(artifact["worktree"], T07_WORKTREE)
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
        _ingest_pass(self.board, passed=True, extra_payload={
            "honey_pot_expected": {
                "repo": "NeuruhAI/neuruh-axon",
                "branch": T07_AXON_BRANCH,
                "head": T07_HEAD,
                "pr_number": T07_AXON_PR,
                "pr_url": T07_AXON_PR_URL,
                "pr_draft": "YES",
            },
        })
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

    def test_t07_live_fixture_and_secret_pointer_is_path_only(self):
        with open(T07_FIXTURE, encoding="utf-8") as fh:
            text = fh.read()
        self.assertIn("MISSION_ID=NEURUH_GROK_SOVEREIGN_BINDING_V4", text)
        self.assertIn("STAGE_ID=T07_LOCAL_GROK_OBSERVER_BIND", text)
        self.assertIn(T07_HEAD, text)
        self.assertIn("EVENT_TARGET=receipt.sealed", text)
        self.assertIn("EVENT_IMPLEMENTATION_STATUS=NOT_PROVEN", text)
        self.assertIn("SENTINEL_CLOSURE=PENDING", text)
        self.assertIn("NUMBER=4", text)
        self.assertIn(T07_AXON_PR_URL, text)
        self.assertIn(T07_AXON_BRANCH, text)
        self.assertNotIn("RECONSTRUCTION=YES", text)
        with open(T07_COURT_FIXTURE, encoding="utf-8") as fh:
            court = fh.read()
        self.assertIn("secret_pointer", court)
        self.assertIn("T07_OBSERVER_COURT.secret", court)
        self.assertNotIn("secret_", court.replace("secret_pointer", ""))
        board = _board(self)
        shutil.copy(T07_FIXTURE, os.path.join(board.receipts_dir, "T07_RECEIPT.txt"))
        shutil.copy(T07_COURT_FIXTURE, os.path.join(board.receipts_dir,
                                                    "T07_OBSERVER_COURT.json"))
        observed = board.tick().observed
        self.assertEqual(observed, ["T07_RECEIPT.txt"])
        self.assertEqual(board.tick().observed, [])
        self.assertFalse(os.path.exists(
            "/Users/jeramiehicks/.neuruh-receipts/grok-sovereign-v4/"
            "T07_OBSERVER_COURT.secret"))
        with self.assertRaises(SwitchboardError):
            board.ingest_court_fixture(T07_COURT_FIXTURE)
        self.assertEqual(len(board.events(EventType.COURT_PASSED)), 0)
        with open(board.event_ledger._path) as fh:
            ledger = fh.read()
        self.assertNotIn("BEGIN ", ledger)
        with open(T07_FIXTURE, encoding="utf-8") as receipt_fh:
            self.assertIn("NOT_PROVEN", receipt_fh.read())


class RequiredProofTests(unittest.TestCase):
    """Exact proofs required by NEURUH-JGI-CONVERGENCE-002 Phase 1 / PR #3 court."""

    def test_1_receipt_observation_is_idempotent(self):
        board = _board(self)
        shutil.copy(T07_FIXTURE, os.path.join(board.receipts_dir, "T07_RECEIPT.txt"))
        first = board.tick()
        second = board.tick()
        third = board.reopen().tick()
        self.assertEqual(first.observed, ["T07_RECEIPT.txt"])
        self.assertEqual(second.observed, [])
        self.assertEqual(third.observed, [])
        self.assertEqual(len(board.reopen().events(EventType.RECEIPT_SEALED)), 1)

    def test_2_receipt_sealed_requests_sentinel_court_not_pass(self):
        board = _board(self)
        shutil.copy(T07_FIXTURE, os.path.join(board.receipts_dir, "T07_RECEIPT.txt"))
        board.tick()
        self.assertEqual(len(board.events(EventType.RECEIPT_SEALED)), 1)
        self.assertEqual(len(board.events(EventType.COURT_REQUESTED)), 1)
        self.assertEqual(len(board.jobs_for(SENTINEL_WORKFLOW)), 1)
        self.assertEqual(len(board.events(EventType.COURT_PASSED)), 0)
        artifact = board.queue.load(
            board.jobs_for(SENTINEL_WORKFLOW)[0]).step_results["materialize"]
        self.assertEqual(artifact["sentinel_model_execution"], SENTINEL_MODEL_EXECUTION)
        self.assertEqual(SENTINEL_MODEL_EXECUTION, "BLOCKED")

    def test_3_no_fake_sentinel_pass(self):
        board = _board(self)
        shutil.copy(T07_FIXTURE, os.path.join(board.receipts_dir, "T07_RECEIPT.txt"))
        board.tick()
        sealed = board.events(EventType.RECEIPT_SEALED)[0]
        self.assertEqual(
            sealed["payload_ref_or_payload"]["sentinel_closure_in_receipt"], "PENDING")
        self.assertEqual(
            sealed["payload_ref_or_payload"]["clipboard_defect"]["sentinel_closure_in_receipt"],
            "PENDING")
        with self.assertRaises(SwitchboardError) as caught:
            board.ingest_court_fixture(T07_COURT_FIXTURE)
        self.assertIn("fake sentinel pass", str(caught.exception).lower())
        court = board.events(EventType.COURT_REQUESTED)[-1]
        for kind in ("reconstruction", "injected", "prime_verdict",
                     "sentinel_closure_in_receipt"):
            with self.assertRaises(SwitchboardError):
                board.record_court_outcome(
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
                    evidence={
                        "kind": kind,
                        "source_role": "SENTINEL",
                        "independent": True,
                        "ref": "test://fake",
                    },
                )
        self.assertEqual(len(board.events(EventType.COURT_PASSED)), 0)

    def test_4_court_passed_only_from_independent_court_evidence(self):
        board = _board(self)
        _write_receipt(board.receipts_dir, "SYNTHETIC_RECEIPT.txt", _synthetic_body())
        board.tick()
        sealed = board.events(EventType.RECEIPT_SEALED)[-1]
        court = board.events(EventType.COURT_REQUESTED)[-1]
        with self.assertRaises(SwitchboardError):
            board.record_court_outcome(
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
                evidence={
                    "kind": INDEPENDENT_COURT_KIND,
                    "source_role": "PRIME",
                    "independent": True,
                    "ref": "test://self-court",
                },
            )
        empty = _board(self)
        _write_receipt(empty.receipts_dir, "SYNTHETIC_RECEIPT.txt", _synthetic_body())
        empty.tick()
        sealed = empty.events(EventType.RECEIPT_SEALED)[-1]
        with self.assertRaises(SwitchboardError) as caught:
            empty.record_court_outcome(
                passed=True,
                court_job_id="job-does-not-exist",
                mission_id=sealed["mission_id"],
                plan_id=sealed["plan_id"],
                task_id=sealed["task_id"],
                correlation_id=sealed["correlation_id"],
                causation_id=sealed["event_id"],
                source_head=sealed["source_head"],
                authority_ref=sealed["authority_ref"],
                source_repo=sealed["source_repo"],
                receipt_ref=sealed["payload_ref_or_payload"]["receipt_ref"],
                evidence=_independent_evidence(),
            )
        self.assertIn("court.requested", str(caught.exception))
        _ingest_pass(board, passed=True)
        self.assertEqual(len(board.events(EventType.COURT_PASSED)), 1)
        self.assertEqual(
            board.events(EventType.COURT_PASSED)[0]["payload_ref_or_payload"]
            ["evidence"]["kind"],
            INDEPENDENT_COURT_KIND)

    def test_5_candidate_push_requested_only_after_court_pass(self):
        board = _board(self)
        _write_receipt(board.receipts_dir, "SYNTHETIC_RECEIPT.txt", _synthetic_body())
        board.tick()
        self.assertEqual(len(board.events(EventType.CANDIDATE_PUSH_REQUESTED)), 0)
        _ingest_pass(board, passed=False)
        board.tick()
        self.assertEqual(len(board.events(EventType.CANDIDATE_PUSH_REQUESTED)), 0)
        self.assertEqual(len(board.jobs_for(HONEY_POT_WORKFLOW)), 0)
        self.assertEqual(len(board.jobs_for(REMEDIATION_WORKFLOW)), 1)
        board2 = _board(self)
        _write_receipt(board2.receipts_dir, "SYNTHETIC_RECEIPT.txt", _synthetic_body())
        board2.tick()
        _ingest_pass(board2, passed=True)
        board2.tick()
        self.assertEqual(len(board2.events(EventType.CANDIDATE_PUSH_REQUESTED)), 1)

    def test_6_restart_recovery(self):
        board = _board(self, crash_after_lease=True)
        _write_receipt(board.receipts_dir, "SYNTHETIC_RECEIPT.txt", _synthetic_body())
        with self.assertRaises(_CrashSimulated):
            board.tick()
        fresh = board.reopen()
        report = fresh.tick()
        self.assertTrue(report.recovered)
        self.assertEqual(len(fresh.jobs_for(SENTINEL_WORKFLOW)), 1)
        self.assertEqual(len(fresh.events(EventType.COURT_REQUESTED)), 1)

    def test_7_deadletter_behavior(self):
        board = _board(self)
        path = _write_receipt(board.receipts_dir, "bad.txt", _synthetic_body())
        with open(path) as fh:
            text = fh.read()
        with open(path, "w") as fh:
            fh.write(text.replace(
                f"PAYLOAD_HASH={text.split('PAYLOAD_HASH=')[-1].strip()}",
                "PAYLOAD_HASH=" + "0" * 64))
        board.tick()
        reasons = {d["payload_ref_or_payload"]["reason"] for d in board.deadletters()}
        self.assertIn(DeadletterReason.BROKEN_HASH.value, reasons)
        self.assertTrue(all(
            d["payload_ref_or_payload"]["original_retained"]
            for d in board.deadletters()))
        self.assertEqual(len(board.jobs_for(SENTINEL_WORKFLOW)), 0)

    def test_8_no_duplicate_consequential_jobs(self):
        board = _board(self)
        _write_receipt(board.receipts_dir, "SYNTHETIC_RECEIPT.txt", _synthetic_body())
        board.tick()
        _ingest_pass(board, passed=True)
        board.tick()
        board.tick()
        fresh = board.reopen()
        fresh.tick()
        self.assertEqual(len(fresh.jobs_for(SENTINEL_WORKFLOW)), 1)
        self.assertEqual(len(fresh.jobs_for(HONEY_POT_WORKFLOW)), 1)
        types = [r.receipt_type for r in fresh.job_ledger.receipts(
            fresh.jobs_for(SENTINEL_WORKFLOW)[0])]
        self.assertEqual(types.count(ReceiptType.JOB_ENQUEUED), 1)

    def test_9_no_ether_or_second_event_bus(self):
        import neuruh_workers.events as events_mod
        import neuruh_workers.switchboard as switchboard_mod
        board = _board(self)
        self.assertIsInstance(board.event_ledger, ExecutionLedger)
        self.assertIsInstance(board.job_ledger, ExecutionLedger)
        self.assertIsInstance(board.queue, JobQueue)
        self.assertIs(board.queue.ledger if hasattr(board.queue, "ledger") else board.job_ledger,
                      board.job_ledger)
        for module in (events_mod, switchboard_mod):
            src = inspect.getsource(module).lower()
            self.assertNotIn("ether-layer", src)
            self.assertNotIn("import ether", src)
            self.assertNotIn("from ether", src)
        self.assertFalse(any("ether" in name for name in sys.modules))
        self.assertEqual(board.event_ledger._path, os.path.join(board.root, "events.jsonl"))
        self.assertEqual(board.job_ledger._path, os.path.join(board.root, "jobs.jsonl"))

    def test_10_no_axon_mutation(self):
        tracked = subprocess.check_output(
            ["git", "ls-files"], cwd=REPO_ROOT, text=True).splitlines()
        self.assertFalse(any(path.startswith("neuruh-axon/") or path.endswith("axon.py")
                             for path in tracked))
        src = inspect.getsource(materialize_candidate_push)
        self.assertNotIn("git push", src)
        self.assertEqual(GIT_PUSH, "BLOCKED")
        board = _board(self)
        _write_receipt(board.receipts_dir, "SYNTHETIC_RECEIPT.txt", _synthetic_body())
        board.tick()
        _ingest_pass(board, passed=True)
        board.tick()
        artifact = board.queue.load(
            board.jobs_for(HONEY_POT_WORKFLOW)[0]).step_results["materialize"]
        self.assertEqual(artifact["git_push"], "BLOCKED")
        self.assertIn("no git mutation", artifact["note"])

    def test_11_human_clipboard_required_is_no(self):
        self.assertEqual(HUMAN_CLIPBOARD_REQUIRED, "NO")
        board = _board(self)
        shutil.copy(T07_FIXTURE, os.path.join(board.receipts_dir, "T07_RECEIPT.txt"))
        report = board.tick()
        self.assertEqual(report.observed, ["T07_RECEIPT.txt"])
        self.assertEqual(len(board.events(EventType.COURT_REQUESTED)), 1)
        self.assertEqual(len(board.jobs_for(SENTINEL_WORKFLOW)), 1)

    def test_independent_fixture_file_is_accepted(self):
        board = _board(self)
        _write_receipt(board.receipts_dir, "SYNTHETIC_RECEIPT.txt", _synthetic_body())
        board.tick()
        sealed = board.events(EventType.RECEIPT_SEALED)[-1]
        path = os.path.join(board.receipts_dir, "independent_court.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump({
                "kind": INDEPENDENT_COURT_KIND,
                "source_role": "SENTINEL",
                "independent": True,
                "verdict": "PASS",
                "mission_id": sealed["mission_id"],
                "stage_id": sealed["plan_id"],
                "source_head": sealed["source_head"],
                "repo": sealed["source_repo"],
            }, fh)
        board.ingest_court_fixture(path)
        self.assertEqual(len(board.events(EventType.COURT_PASSED)), 1)
        self.assertEqual(
            board.events(EventType.COURT_PASSED)[0]["payload_ref_or_payload"]
            ["evidence"]["kind"],
            INDEPENDENT_COURT_KIND)


if __name__ == "__main__":
    unittest.main()
