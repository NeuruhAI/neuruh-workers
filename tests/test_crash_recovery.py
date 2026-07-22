"""Crash recovery: a process that dies mid-run leaves an honest ledger, and a
fresh runner completes the job without re-executing finished steps."""
import unittest

from neuruh_workers import JobStatus, ReceiptType
from neuruh_workers.contracts import content_sha
from tests.helpers import make_stack

PINS = {"pins": [{"pin_id": "p1", "lat": 33.1, "lon": -97.2},
                 {"pin_id": "p2", "lat": -12.4, "lon": 130.8}]}


class CrashSimulated(BaseException):
    """BaseException so the runner's Exception handling does NOT catch it —
    exactly like a process kill, the run just stops."""


class CrashRecoveryTests(unittest.TestCase):
    def _crashing_stack(self, crash_before_step):
        state = {"armed": True}

        def hook(job_id, step_id):
            if step_id == crash_before_step and state["armed"]:
                state["armed"] = False
                raise CrashSimulated(f"process died before {step_id}")

        return make_stack(self, step_hook=hook)

    def test_crash_between_steps_then_recover_without_reexecution(self):
        stack = self._crashing_stack("derive_geo_context")
        wf = stack.catalog.get("field-pin-enrichment")
        job = stack.queue.enqueue(wf, PINS, requested_by="op")
        with self.assertRaises(CrashSimulated):
            stack.runner.execute(job)

        # The ledger tells the truth: job RUNNING, first step done, no outcome
        # for the rest.
        state = stack.queue.load(job)
        self.assertEqual(state.status, JobStatus.RUNNING)
        self.assertIn("validate_pins", state.step_results)
        self.assertNotIn("derive_geo_context", state.step_results)
        first_result_sha = content_sha(state.step_results["validate_pins"])

        # A fresh "process" resumes from the ledger.
        fresh = stack.reopen()
        recovered = fresh.runner.execute(job)
        self.assertEqual(recovered.status, JobStatus.SUCCEEDED)
        self.assertEqual(recovered.step_results["enrich_pins"]["count"], 2)
        # The completed step's result is byte-identical (served, not re-run).
        self.assertEqual(
            content_sha(recovered.step_results["validate_pins"]), first_result_sha)
        types = [r.receipt_type for r in fresh.ledger.receipts(job)]
        self.assertIn(ReceiptType.STEP_REPLAY_SKIPPED, types)
        # validate_pins must have exactly ONE started/succeeded pair.
        started_validate = [
            r for r in fresh.ledger.receipts(job)
            if r.receipt_type == ReceiptType.STEP_STARTED
            and r.payload["step_id"] == "validate_pins"]
        self.assertEqual(len(started_validate), 1)

    def test_crash_mid_step_recovers_and_does_not_burn_retry_budget(self):
        stack = make_stack(self)
        wf = stack.catalog.get("field-pin-enrichment")
        job = stack.queue.enqueue(wf, PINS, requested_by="op")
        # Simulate a process that died AFTER starting a step but before any
        # outcome receipt: run step 1 for real, then forge the crash point by
        # appending only the STEP_STARTED receipt for step 2.
        stack.ledger.append(ReceiptType.JOB_STARTED, job, {})
        stack.ledger.append(ReceiptType.STEP_STARTED, job, {
            "step_id": "validate_pins", "attempt": 1,
            "capability": "pins.validate", "dry_run": False})
        state = stack.queue.load(job)
        self.assertEqual(state.open_steps, {"validate_pins": 1})

        fresh = stack.reopen()
        recovered = fresh.runner.execute(job)
        self.assertEqual(recovered.status, JobStatus.SUCCEEDED)
        receipts = fresh.ledger.receipts(job)
        types = [r.receipt_type for r in receipts]
        self.assertIn(ReceiptType.JOB_RECOVERED, types)
        # The interrupted attempt is renumbered, and the interrupted start is
        # not counted as a failure (no STEP_FAILED anywhere).
        self.assertNotIn(ReceiptType.STEP_FAILED, types)
        second_start = [r for r in receipts
                        if r.receipt_type == ReceiptType.STEP_STARTED
                        and r.payload["step_id"] == "validate_pins"][-1]
        self.assertEqual(second_start.payload["attempt"], 2)

    def test_recovery_receipt_records_open_steps(self):
        stack = make_stack(self)
        wf = stack.catalog.get("cockpit-health-snapshot")
        job = stack.queue.enqueue(
            wf, {"components": [{"name": "iar", "status": "healthy"}]},
            requested_by="op")
        stack.ledger.append(ReceiptType.JOB_STARTED, job, {})
        stack.ledger.append(ReceiptType.STEP_STARTED, job, {
            "step_id": "collect_component_statuses", "attempt": 1,
            "capability": "cockpit.collect", "dry_run": False})
        fresh = stack.reopen()
        fresh.runner.execute(job)
        recovered = [r for r in fresh.ledger.receipts(job)
                     if r.receipt_type == ReceiptType.JOB_RECOVERED]
        self.assertEqual(len(recovered), 1)
        self.assertEqual(recovered[0].payload["open_steps"],
                         {"collect_component_statuses": 1})


if __name__ == "__main__":
    unittest.main()
