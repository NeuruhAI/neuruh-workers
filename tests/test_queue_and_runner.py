"""Unit tests: queue states, deterministic IDs, runner execution semantics."""
import unittest

from neuruh_workers import (
    Capability, JobStateInvalid, JobStatus, ReceiptType, RetryPolicy,
    StepDefinition, StepContext, WorkerDeclaration, WorkerRegistry,
    WorkflowCatalog, WorkflowDefinition, WorkflowRunner, JobQueue,
    deterministic_job_id,
)
from tests.helpers import make_stack

PINS = {"pins": [{"pin_id": "p1", "lat": 33.1, "lon": -97.2, "label": "gate"}]}


class QueueTests(unittest.TestCase):
    def test_deterministic_job_ids(self):
        a = deterministic_job_id("wf", "1", {"x": 1}, None)
        b = deterministic_job_id("wf", "1", {"x": 1}, None)
        c = deterministic_job_id("wf", "1", {"x": 2}, None)
        d = deterministic_job_id("wf", "1", {"x": 1}, "other-day")
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)
        self.assertNotEqual(a, d)

    def test_enqueue_is_idempotent(self):
        stack = make_stack(self)
        wf = stack.catalog.get("field-pin-enrichment")
        j1 = stack.queue.enqueue(wf, PINS, requested_by="op")
        j2 = stack.queue.enqueue(wf, PINS, requested_by="op")
        self.assertEqual(j1, j2)
        types = [r.receipt_type for r in stack.ledger.receipts(j1)]
        self.assertEqual(types.count(ReceiptType.JOB_ENQUEUED), 1)
        self.assertEqual(types.count(ReceiptType.JOB_ENQUEUE_DEDUPLICATED), 1)

    def test_unknown_job_and_unknown_capability_fail_closed(self):
        stack = make_stack(self)
        with self.assertRaises(JobStateInvalid):
            stack.queue.load("job-nope")
        bad = WorkflowDefinition(
            name="bad-flow", version="1", description="d",
            steps=(StepDefinition(step_id="s", capability="not.registered"),))
        with self.assertRaises(Exception) as caught:
            stack.queue.enqueue(bad, {}, requested_by="op")
        self.assertIn("CAPABILITY_UNKNOWN", str(caught.exception))

    def test_pause_resume_cancel_states(self):
        stack = make_stack(self)
        wf = stack.catalog.get("field-pin-enrichment")
        job = stack.queue.enqueue(wf, PINS, requested_by="op")
        self.assertEqual(stack.queue.load(job).status, JobStatus.QUEUED)
        self.assertEqual(stack.queue.pause(job).status, JobStatus.PAUSED)
        with self.assertRaises(JobStateInvalid):
            stack.runner.execute(job)  # paused jobs must be resumed first
        self.assertEqual(stack.queue.resume(job).status, JobStatus.RUNNING)
        state = stack.runner.execute(job)
        self.assertEqual(state.status, JobStatus.SUCCEEDED)
        with self.assertRaises(JobStateInvalid):
            stack.queue.cancel(job, reason="too late")  # terminal is terminal

    def test_cancel_receipts_reason(self):
        stack = make_stack(self)
        wf = stack.catalog.get("field-pin-enrichment")
        job = stack.queue.enqueue(wf, PINS, requested_by="op")
        state = stack.queue.cancel(job, reason="operator abort")
        self.assertEqual(state.status, JobStatus.CANCELLED)
        self.assertEqual(state.failure_reason, "operator abort")


def _registry_with(fn, *, name="test.cap", fails_before_success=0,
                   read_only=True):
    calls = {"n": 0}

    def execute(ctx: StepContext, params):
        calls["n"] += 1
        if calls["n"] <= fails_before_success:
            raise RuntimeError(f"induced failure {calls['n']}")
        return fn(ctx, params)

    registry = WorkerRegistry()
    registry.register_worker(WorkerDeclaration(
        worker_id="test-worker", description="test",
        capabilities=(Capability(name=name, description="t", read_only=read_only,
                                 external=False, execute=execute),)))
    return registry, calls


def _single_step_catalog(*, retry=None, timeout_s=10.0, name="test-flow"):
    catalog = WorkflowCatalog()
    catalog.register(WorkflowDefinition(
        name=name, version="1", description="d",
        steps=(StepDefinition(step_id="only", capability="test.cap",
                              retry=retry or RetryPolicy(max_attempts=3,
                                                         base_delay_s=1.0),
                              timeout_s=timeout_s),)))
    return catalog


class RunnerTests(unittest.TestCase):
    def test_retry_backoff_then_success(self):
        registry, calls = _registry_with(lambda c, p: {"ok": True},
                                         fails_before_success=2)
        catalog = _single_step_catalog()
        stack = make_stack(self, registry=registry, catalog=catalog)
        job = stack.queue.enqueue(catalog.get("test-flow"), {}, requested_by="op")
        state = stack.runner.execute(job)
        self.assertEqual(state.status, JobStatus.SUCCEEDED)
        self.assertEqual(calls["n"], 3)
        self.assertEqual(stack.sleeps, [1.0, 2.0])  # deterministic backoff
        types = [r.receipt_type for r in stack.ledger.receipts(job)]
        self.assertEqual(types.count(ReceiptType.STEP_FAILED), 2)
        self.assertEqual(types.count(ReceiptType.STEP_RETRY_SCHEDULED), 2)
        self.assertEqual(types.count(ReceiptType.STEP_SUCCEEDED), 1)

    def test_retry_exhaustion_fails_job_with_clear_state(self):
        registry, calls = _registry_with(lambda c, p: {"ok": True},
                                         fails_before_success=99)
        catalog = _single_step_catalog(retry=RetryPolicy(max_attempts=2,
                                                         base_delay_s=0.5))
        stack = make_stack(self, registry=registry, catalog=catalog)
        job = stack.queue.enqueue(catalog.get("test-flow"), {}, requested_by="op")
        state = stack.runner.execute(job)
        self.assertEqual(state.status, JobStatus.FAILED)
        self.assertEqual(state.failure_reason, "retry_exhausted")
        self.assertEqual(calls["n"], 2)
        with self.assertRaises(JobStateInvalid):
            stack.runner.execute(job)  # failed jobs cannot be re-executed

    def test_timeout_is_enforced_and_receipted(self):
        import time as _time

        def slow(ctx, params):
            _time.sleep(2.0)
            return {"late": True}

        registry, _ = _registry_with(slow)
        catalog = _single_step_catalog(
            retry=RetryPolicy(max_attempts=1), timeout_s=0.1)
        stack = make_stack(self, registry=registry, catalog=catalog)
        job = stack.queue.enqueue(catalog.get("test-flow"), {}, requested_by="op")
        state = stack.runner.execute(job)
        self.assertEqual(state.status, JobStatus.FAILED)
        types = [r.receipt_type for r in stack.ledger.receipts(job)]
        self.assertIn(ReceiptType.STEP_TIMEOUT, types)

    def test_every_transition_has_a_receipt(self):
        stack = make_stack(self)
        wf = stack.catalog.get("cockpit-health-snapshot")
        job = stack.queue.enqueue(
            wf, {"components": [{"name": "qdrant", "status": "healthy"}]},
            requested_by="op")
        stack.runner.execute(job)
        types = [r.receipt_type for r in stack.ledger.receipts(job)]
        self.assertEqual(types[0], ReceiptType.JOB_ENQUEUED)
        self.assertEqual(types[1], ReceiptType.JOB_STARTED)
        self.assertEqual(types[-1], ReceiptType.JOB_SUCCEEDED)
        self.assertEqual(types.count(ReceiptType.STEP_STARTED), 3)
        self.assertEqual(types.count(ReceiptType.STEP_SUCCEEDED), 3)

    def test_workflow_drift_refused_no_silent_fallback(self):
        stack = make_stack(self)
        wf = stack.catalog.get("field-pin-enrichment")
        job = stack.queue.enqueue(wf, PINS, requested_by="op")
        drifted = WorkflowCatalog()
        drifted.register(WorkflowDefinition(
            name="field-pin-enrichment", version="1.0.1",  # not what was sealed
            description="drifted",
            steps=(StepDefinition(step_id="validate_pins",
                                  capability="pins.validate"),)))
        runner = WorkflowRunner(stack.queue, stack.registry, drifted)
        with self.assertRaises(JobStateInvalid):
            runner.execute(job)

    def test_mid_run_pause_honored_at_step_boundary_then_resumes(self):
        pause_at = {"step": "derive_geo_context", "done": False}

        def hook(job_id, step_id):
            if step_id == pause_at["step"] and not pause_at["done"]:
                pause_at["done"] = True
                stack.queue.pause(job_id)

        stack = make_stack(self, step_hook=hook)
        wf = stack.catalog.get("field-pin-enrichment")
        job = stack.queue.enqueue(wf, PINS, requested_by="op")
        state = stack.runner.execute(job)
        self.assertEqual(state.status, JobStatus.PAUSED)
        self.assertIn("validate_pins", state.step_results)
        self.assertNotIn("derive_geo_context", state.step_results)
        stack.queue.resume(job)
        state = stack.runner.execute(job)
        self.assertEqual(state.status, JobStatus.SUCCEEDED)
        self.assertEqual(state.step_results["enrich_pins"]["count"], 1)


if __name__ == "__main__":
    unittest.main()
