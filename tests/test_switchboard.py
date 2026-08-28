"""Switchboard V1: a ledger watcher wrap, not a new bus.

These tests first document the historical gap (a sealed Prime receipt does
not by itself enqueue a Sentinel job) and then prove the wrap: ticking
Switchboard turns that receipt into a sentinel-verify job on the existing
queue, without a human clipboard, without activate(), and without git push.
"""
from __future__ import annotations

import inspect
import json
import os
import shutil
import tempfile
import unittest

import neuruh_workers.switchboard as switchboard_mod
from neuruh_workers import (
    EVENT_IMPLEMENTATION_STATUS,
    ExecutionLedger,
    JobStatus,
    LedgerCorrupt,
    OrganRegistrationSeam,
    OrganSeamDeclaration,
    ReceiptType,
    SeamNotAuthorized,
    StepContext,
    Switchboard,
    SwitchboardCursorCorrupt,
    SwitchboardPathRefused,
    SwitchboardRefused,
    WorkflowRunner,
)
from neuruh_workers.switchboard import (
    CONTRACT_CANDIDATE_PUSH_READY,
    CONTRACT_RECEIPT_SEALED,
    GIT_CUSTODY_WORKFLOW,
    SENTINEL_WORKFLOW,
    SWITCHBOARD_JOB_ID,
    _execute_git_custody,
)
from tests.helpers import make_stack

CONTENT_SHA = "a" * 64


def _make_switchboard(stack, **kwargs) -> Switchboard:
    allowed_root = os.path.dirname(stack.ledger_path)
    return Switchboard(
        stack.ledger,
        ledger_path=stack.ledger_path,
        allowed_root=allowed_root,
        **kwargs,
    )


def _append_prime_sealed(ledger, receipt_id, *, content_sha=CONTENT_SHA,
                         sealed=True, extra=None, job_id=None):
    payload = {
        "sealed": sealed,
        "receipt_id": receipt_id,
        "content_sha": content_sha,
        "contract": CONTRACT_RECEIPT_SEALED,
        "issuer": "prime",
    }
    if extra:
        payload.update(extra)
    return ledger.append(
        ReceiptType.PRIME_RECEIPT_SEALED,
        job_id or f"prime:{receipt_id}",
        payload,
    )


def _enqueued_for(ledger, workflow) -> list:
    return [
        r for r in ledger.receipts()
        if r.receipt_type == ReceiptType.JOB_ENQUEUED
        and r.payload.get("workflow") == workflow
    ]


def _switchboard_receipts(ledger, receipt_type=None) -> list:
    out = []
    for r in ledger.receipts(SWITCHBOARD_JOB_ID):
        if receipt_type is None or r.receipt_type == receipt_type:
            out.append(r)
    return out


class SwitchboardGapTests(unittest.TestCase):
    """The expected broken reason before the wrap: no auto Sentinel job."""

    def test_sealed_prime_receipt_does_not_enqueue_sentinel_by_itself(self):
        stack = make_stack(self)
        _append_prime_sealed(stack.ledger, "rec-gap")
        self.assertEqual(_enqueued_for(stack.ledger, SENTINEL_WORKFLOW), [])
        self.assertEqual(_enqueued_for(stack.ledger, GIT_CUSTODY_WORKFLOW), [])


class SwitchboardPositiveTests(unittest.TestCase):
    def test_prime_sealed_receipt_enqueues_sentinel_job_without_human(self):
        stack = make_stack(self)
        _append_prime_sealed(stack.ledger, "rec-1")
        self.assertEqual(_enqueued_for(stack.ledger, SENTINEL_WORKFLOW), [])

        report = _make_switchboard(stack).tick()
        jobs = _enqueued_for(stack.ledger, SENTINEL_WORKFLOW)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].payload["params"]["receipt_id"], "rec-1")
        self.assertEqual(jobs[0].payload["params"]["content_sha"], CONTENT_SHA)
        self.assertEqual(jobs[0].payload["params"]["contract"], CONTRACT_RECEIPT_SEALED)
        self.assertEqual(jobs[0].payload["requested_by"], "switchboard-v1")
        self.assertEqual(report.sentinel_jobs_created, [jobs[0].job_id])
        self.assertEqual(report.sentinel_jobs_existing, [])

        created = _switchboard_receipts(stack.ledger, ReceiptType.SWITCHBOARD_JOB_CREATED)
        self.assertEqual(len(created), 1)
        self.assertEqual(created[0].payload["action"], "enqueue_sentinel_verify")
        self.assertEqual(created[0].payload["target_job_id"], jobs[0].job_id)
        self.assertEqual(created[0].payload["receipt_id"], "rec-1")

    def test_sentinel_pass_enqueues_git_custody_payload_job(self):
        stack = make_stack(self)
        _append_prime_sealed(stack.ledger, "rec-pass")
        sb = _make_switchboard(stack)
        sb.tick()
        sentinel = _enqueued_for(stack.ledger, SENTINEL_WORKFLOW)[0]

        stack.ledger.append(ReceiptType.SENTINEL_VERIFICATION_PASS, "sentinel-pass:rec-pass", {
            "result": "PASS",
            "receipt_id": "rec-pass",
            "content_sha": CONTENT_SHA,
            "sentinel_job_id": sentinel.job_id,
        })
        report = sb.tick()
        custody = _enqueued_for(stack.ledger, GIT_CUSTODY_WORKFLOW)
        self.assertEqual(len(custody), 1)
        params = custody[0].payload["params"]
        self.assertEqual(params["contract"], CONTRACT_CANDIDATE_PUSH_READY)
        self.assertIs(params["git_push"], False)
        self.assertEqual(params["custody"], "payload_only")
        self.assertEqual(params["receipt_id"], "rec-pass")
        self.assertEqual(params["sentinel_job_id"], sentinel.job_id)
        self.assertEqual(report.git_custody_jobs_created, [custody[0].job_id])

    def test_job_succeeded_on_sentinel_verify_is_equivalent_pass(self):
        stack = make_stack(self)
        _append_prime_sealed(stack.ledger, "rec-run")
        sb = _make_switchboard(stack)
        sb.tick()
        sentinel_id = _enqueued_for(stack.ledger, SENTINEL_WORKFLOW)[0].job_id
        runner = WorkflowRunner(sb.queue, sb.registry, sb.catalog)
        state = runner.execute(sentinel_id)
        self.assertEqual(state.status, JobStatus.SUCCEEDED)
        self.assertEqual(state.step_results["verify_sealed_receipt"]["result"], "PASS")

        report = sb.tick()
        custody = _enqueued_for(stack.ledger, GIT_CUSTODY_WORKFLOW)
        self.assertEqual(len(custody), 1)
        self.assertEqual(report.git_custody_jobs_created, [custody[0].job_id])
        custody_state = runner.execute(custody[0].job_id)
        self.assertEqual(custody_state.status, JobStatus.SUCCEEDED)
        self.assertIs(custody_state.step_results["record_custody_payload"]["git_push"], False)

    def test_distinct_receipts_enqueue_distinct_sentinel_jobs(self):
        stack = make_stack(self)
        _append_prime_sealed(stack.ledger, "rec-a")
        _append_prime_sealed(stack.ledger, "rec-b")
        _make_switchboard(stack).tick()
        jobs = _enqueued_for(stack.ledger, SENTINEL_WORKFLOW)
        self.assertEqual(len(jobs), 2)
        self.assertEqual(
            {j.payload["params"]["receipt_id"] for j in jobs}, {"rec-a", "rec-b"})


class SwitchboardIdempotencyTests(unittest.TestCase):
    def test_duplicate_sealed_receipt_does_not_double_enqueue(self):
        stack = make_stack(self)
        _append_prime_sealed(stack.ledger, "rec-dup")
        _append_prime_sealed(stack.ledger, "rec-dup")  # same content, new seq
        sb = _make_switchboard(stack)
        report = sb.tick()
        jobs = _enqueued_for(stack.ledger, SENTINEL_WORKFLOW)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(len(report.sentinel_jobs_created), 1)
        self.assertEqual(len(report.sentinel_jobs_existing), 1)
        self.assertEqual(report.sentinel_jobs_created[0], report.sentinel_jobs_existing[0])
        created = _switchboard_receipts(stack.ledger, ReceiptType.SWITCHBOARD_JOB_CREATED)
        existed = _switchboard_receipts(
            stack.ledger, ReceiptType.SWITCHBOARD_JOB_ALREADY_EXISTED)
        self.assertEqual(len(created), 1)
        self.assertEqual(len(existed), 1)

    def test_retick_does_not_double_enqueue(self):
        stack = make_stack(self)
        _append_prime_sealed(stack.ledger, "rec-retick")
        sb = _make_switchboard(stack)
        sb.tick()
        report = sb.tick()
        self.assertEqual(report.scanned, 0)
        self.assertEqual(len(_enqueued_for(stack.ledger, SENTINEL_WORKFLOW)), 1)
        self.assertEqual(
            len(_switchboard_receipts(stack.ledger, ReceiptType.SWITCHBOARD_JOB_CREATED)), 1)


class SwitchboardNegativeTests(unittest.TestCase):
    def test_unsigned_unsealed_record_is_ignored(self):
        stack = make_stack(self)
        stack.ledger.append("job.enqueued", "job-noise", {
            "workflow": "not-a-prime-seal", "params": {},
        })
        stack.ledger.append(ReceiptType.PRIME_RECEIPT_SEALED, "prime:unsigned", {
            "sealed": False,
            "receipt_id": "rec-unsigned",
            "content_sha": CONTENT_SHA,
            "contract": CONTRACT_RECEIPT_SEALED,
        })
        stack.ledger.append(ReceiptType.PRIME_RECEIPT_SEALED, "prime:no-flag", {
            "receipt_id": "rec-no-flag",
            "content_sha": CONTENT_SHA,
        })
        report = _make_switchboard(stack).tick()
        self.assertEqual(_enqueued_for(stack.ledger, SENTINEL_WORKFLOW), [])
        self.assertGreaterEqual(report.ignored, 3)
        self.assertEqual(_switchboard_receipts(stack.ledger), [])

    def test_sentinel_fail_does_not_enqueue_git_custody(self):
        stack = make_stack(self)
        _append_prime_sealed(stack.ledger, "rec-fail")
        sb = _make_switchboard(stack)
        sb.tick()
        stack.ledger.append(ReceiptType.SENTINEL_VERIFICATION_PASS, "sentinel-pass:rec-fail", {
            "result": "FAIL",
            "receipt_id": "rec-fail",
            "content_sha": CONTENT_SHA,
        })
        sb.tick()
        self.assertEqual(_enqueued_for(stack.ledger, GIT_CUSTODY_WORKFLOW), [])


class SwitchboardReplayTests(unittest.TestCase):
    def test_replay_from_cursor_file_does_not_double_enqueue(self):
        stack = make_stack(self)
        _append_prime_sealed(stack.ledger, "rec-replay")
        first = _make_switchboard(stack)
        first.tick()
        self.assertGreater(first.cursor_seq, 0)
        self.assertTrue(os.path.exists(first.cursor_path))

        reopened = ExecutionLedger(stack.ledger_path)
        second = Switchboard(
            reopened,
            ledger_path=stack.ledger_path,
            allowed_root=os.path.dirname(stack.ledger_path),
            cursor_path=first.cursor_path,
        )
        report = second.tick()
        self.assertEqual(report.scanned, 0)
        self.assertEqual(len(_enqueued_for(stack.ledger, SENTINEL_WORKFLOW)), 1)

    def test_lost_cursor_still_idempotent_via_switchboard_receipts(self):
        stack = make_stack(self)
        _append_prime_sealed(stack.ledger, "rec-lost-cursor")
        first = _make_switchboard(stack)
        first.tick()
        os.remove(first.cursor_path)

        reopened = ExecutionLedger(stack.ledger_path)
        second = Switchboard(
            reopened,
            ledger_path=stack.ledger_path,
            allowed_root=os.path.dirname(stack.ledger_path),
            cursor_path=first.cursor_path,
        )
        report = second.tick()
        self.assertEqual(len(_enqueued_for(stack.ledger, SENTINEL_WORKFLOW)), 1)
        # Source seq already routed: no extra switchboard receipts, no new jobs.
        self.assertEqual(report.sentinel_jobs_created, [])
        self.assertEqual(report.sentinel_jobs_existing, [])
        self.assertEqual(
            len(_switchboard_receipts(stack.ledger, ReceiptType.SWITCHBOARD_JOB_CREATED)), 1)


class SwitchboardMalformedTests(unittest.TestCase):
    def test_malformed_prime_payloads_are_ignored(self):
        stack = make_stack(self)
        stack.ledger.append(ReceiptType.PRIME_RECEIPT_SEALED, "prime:bad-sha", {
            "sealed": True, "receipt_id": "rec-bad-sha", "content_sha": "not-a-sha",
        })
        stack.ledger.append(ReceiptType.PRIME_RECEIPT_SEALED, "prime:missing-id", {
            "sealed": True, "content_sha": CONTENT_SHA,
        })
        stack.ledger.append(ReceiptType.PRIME_RECEIPT_SEALED, "prime:string-flag", {
            "sealed": "true", "receipt_id": "rec-str", "content_sha": CONTENT_SHA,
        })
        stack.ledger.append(ReceiptType.PRIME_RECEIPT_SEALED, "prime:wrong-contract", {
            "sealed": True, "receipt_id": "rec-ether", "content_sha": CONTENT_SHA,
            "contract": "ether.not-canonical",
        })
        report = _make_switchboard(stack).tick()
        self.assertEqual(_enqueued_for(stack.ledger, SENTINEL_WORKFLOW), [])
        self.assertGreaterEqual(report.ignored, 4)

    def test_corrupt_ledger_fail_closes_on_tick(self):
        stack = make_stack(self)
        _append_prime_sealed(stack.ledger, "rec-ok")
        sb = _make_switchboard(stack)
        with open(stack.ledger_path, "a", encoding="utf-8") as fh:
            fh.write("{not json}\n")
        with self.assertRaises(LedgerCorrupt):
            sb.tick()
        self.assertEqual(_enqueued_for(stack.ledger, SENTINEL_WORKFLOW), [])

    def test_malformed_cursor_is_refused(self):
        stack = make_stack(self)
        cursor = stack.ledger_path + ".switchboard-cursor.json"
        with open(cursor, "w", encoding="utf-8") as fh:
            fh.write("{not json}\n")
        with self.assertRaises(SwitchboardCursorCorrupt):
            _make_switchboard(stack, cursor_path=cursor)

    def test_cursor_for_other_ledger_is_refused(self):
        stack = make_stack(self)
        cursor = stack.ledger_path + ".switchboard-cursor.json"
        with open(cursor, "w", encoding="utf-8") as fh:
            json.dump({"version": 1, "last_seq": 0,
                       "ledger_path": "/tmp/other-ledger.jsonl"}, fh)
        with self.assertRaises(SwitchboardCursorCorrupt):
            _make_switchboard(stack, cursor_path=cursor)


class SwitchboardPathRefusalTests(unittest.TestCase):
    def test_ledger_outside_allowed_root_is_refused(self):
        stack = make_stack(self)
        other = tempfile.mkdtemp(prefix="neuruh-switchboard-other-")
        self.addCleanup(_rmtree, other)
        with self.assertRaises(SwitchboardPathRefused):
            Switchboard(
                stack.ledger,
                ledger_path=stack.ledger_path,
                allowed_root=other,
            )

    def test_non_loopback_mode_is_refused(self):
        stack = make_stack(self)
        with self.assertRaises(SwitchboardRefused) as caught:
            _make_switchboard(stack, loopback=False)
        self.assertIn("SWITCHBOARD_REFUSED", str(caught.exception))
        self.assertIn("loopback-only", str(caught.exception))

    def test_missing_allowed_root_is_refused(self):
        stack = make_stack(self)
        with self.assertRaises(SwitchboardPathRefused):
            Switchboard(
                stack.ledger,
                ledger_path=stack.ledger_path,
                allowed_root="",
            )

    def test_symlink_escape_is_refused(self):
        stack = make_stack(self)
        sandbox = tempfile.mkdtemp(prefix="neuruh-switchboard-sandbox-")
        self.addCleanup(_rmtree, sandbox)
        escape = os.path.join(os.path.dirname(stack.ledger_path), "outside.jsonl")
        with open(escape, "w", encoding="utf-8") as fh:
            fh.write("")
        link = os.path.join(sandbox, "ledger.jsonl")
        os.symlink(escape, link)
        with self.assertRaises(SwitchboardPathRefused):
            Switchboard(
                stack.ledger,
                ledger_path=link,
                allowed_root=sandbox,
            )


class SwitchboardIsolationTests(unittest.TestCase):
    def test_does_not_call_or_fill_organ_activate(self):
        source = inspect.getsource(switchboard_mod)
        self.assertNotIn("OrganRegistrationSeam", source)
        self.assertNotIn(".activate(", source)
        self.assertNotIn("import ether", source)
        self.assertNotIn("from ether", source)

        stack = make_stack(self)
        _append_prime_sealed(stack.ledger, "rec-seam")
        _make_switchboard(stack).tick()
        seam = OrganRegistrationSeam(stack.ledger)
        seam.declare(OrganSeamDeclaration(
            organ="axon", endpoint="http://127.0.0.1:8300",
            source_sha="f3a94d1a68ac6b07525e962692901411bd43e6bf"))
        with self.assertRaises(SeamNotAuthorized):
            seam.activate("axon")
        refused = [
            r for r in stack.ledger.receipts("seam-registry")
            if r.receipt_type == ReceiptType.SEAM_ACTIVATION_REFUSED
        ]
        self.assertEqual(len(refused), 1)

    def test_git_custody_execute_refuses_push_and_does_not_invoke_git(self):
        ctx = StepContext(
            job_id="job-x", workflow=GIT_CUSTODY_WORKFLOW, step_id="record",
            attempt=1, dry_run=False, params={}, results={})
        result = _execute_git_custody(ctx, {
            "receipt_id": "rec-1", "git_push": False, "sentinel_job_id": "job-s",
        })
        self.assertIs(result["git_push"], False)
        self.assertEqual(result["status"], "PAYLOAD_RECORDED")
        with self.assertRaises(ValueError):
            _execute_git_custody(ctx, {"receipt_id": "rec-1", "git_push": True})

        source = inspect.getsource(switchboard_mod)
        self.assertNotIn("subprocess", source)
        self.assertNotIn("git push", source)
        self.assertNotIn("git commit", source)

    def test_event_implementation_status_is_honest(self):
        self.assertEqual(
            EVENT_IMPLEMENTATION_STATUS["receipt.sealed"]["status"], "TARGET_CONTRACT")
        self.assertEqual(
            EVENT_IMPLEMENTATION_STATUS["candidate.push.ready"]["status"],
            "TARGET_CONTRACT")
        self.assertEqual(
            EVENT_IMPLEMENTATION_STATUS["prime.receipt_sealed"]["status"], "PROVEN")
        self.assertEqual(
            EVENT_IMPLEMENTATION_STATUS["sentinel.verification_pass"]["status"],
            "PROVEN")
        self.assertEqual(
            EVENT_IMPLEMENTATION_STATUS["git-custody"]["status"], "PROVEN")

    def test_sample_registry_is_not_mutated_by_switchboard(self):
        stack = make_stack(self)
        before = stack.registry.declarations()
        _append_prime_sealed(stack.ledger, "rec-census")
        _make_switchboard(stack).tick()
        self.assertEqual(stack.registry.declarations(), before)
        caps = before["local-readonly-worker-v1"]["capabilities"]
        self.assertGreaterEqual(len(caps), 19)


def _rmtree(path):
    shutil.rmtree(path, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
