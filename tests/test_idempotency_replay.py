"""Replay and idempotency: identical work is identical, completed work is
never re-executed, and canonical receipt chains are deterministic across runs."""
import unittest

from neuruh_workers import (
    Capability, JobStateInvalid, JobStatus, ReceiptType, StepDefinition,
    WorkerDeclaration, WorkerRegistry, WorkflowCatalog, WorkflowDefinition,
)
from tests.helpers import make_stack

PARAMS = {"current_worlds": ["defensenet", "aegis"],
          "proposed_worlds": ["defensenet", "meshgrid"]}


class IdempotencyTests(unittest.TestCase):
    def test_completed_job_cannot_rerun_and_results_persist(self):
        stack = make_stack(self)
        wf = stack.catalog.get("world-emitter-refresh-preview")
        job = stack.queue.enqueue(wf, PARAMS, requested_by="op")
        state = stack.runner.execute(job)
        self.assertEqual(state.status, JobStatus.SUCCEEDED)
        with self.assertRaises(JobStateInvalid):
            stack.runner.execute(job)
        reloaded = stack.queue.load(job)
        self.assertEqual(
            reloaded.step_results["compute_refresh_plan"]["plan"],
            {"add": ["meshgrid"], "remove": ["aegis"], "keep": ["defensenet"]})

    def test_side_effect_counting_step_executes_exactly_once_across_reruns(self):
        executions = {"n": 0}

        def counting(ctx, params):
            executions["n"] += 1
            return {"execution": executions["n"]}

        registry = WorkerRegistry()
        registry.register_worker(WorkerDeclaration(
            worker_id="w", description="d",
            capabilities=(
                Capability(name="count.once", description="", read_only=True,
                           external=False, execute=counting),
                Capability(name="fail.once", description="", read_only=True,
                           external=False,
                           execute=self._fail_first_factory()),
            )))
        catalog = WorkflowCatalog()
        catalog.register(WorkflowDefinition(
            name="two-step", version="1", description="d",
            steps=(StepDefinition(step_id="first", capability="count.once"),
                   StepDefinition(step_id="second", capability="fail.once"))))
        stack = make_stack(self, registry=registry, catalog=catalog)
        job = stack.queue.enqueue(catalog.get("two-step"), {}, requested_by="op")
        state = stack.runner.execute(job)  # first succeeds; second fails (1 attempt)
        self.assertEqual(state.status, JobStatus.FAILED)
        self.assertEqual(executions["n"], 1)
        # Enqueue the identical job again: deduplicated, same terminal state,
        # the completed step is NOT re-executed.
        job2 = stack.queue.enqueue(catalog.get("two-step"), {}, requested_by="op")
        self.assertEqual(job, job2)
        self.assertEqual(executions["n"], 1)

    def _fail_first_factory(self):
        seen = {"n": 0}

        def fail_once(ctx, params):
            seen["n"] += 1
            raise RuntimeError("always fails")

        return fail_once

    def test_canonical_chain_identical_across_independent_runs(self):
        chains, results = [], []
        for _ in range(2):
            stack = make_stack(self)
            wf = stack.catalog.get("property-research")
            job = stack.queue.enqueue(
                wf, {"address": "428 Bluff Rd, Decatur, TX", "county": "Wise"},
                requested_by="op")
            state = stack.runner.execute(job)
            self.assertEqual(state.status, JobStatus.SUCCEEDED)
            chains.append(stack.ledger.canonical_chain(job))
            results.append(state.step_results)
        self.assertEqual(results[0], results[1])
        self.assertEqual(chains[0], chains[1])
        self.assertGreater(len(chains[0]), 5)

    def test_replay_after_reopen_reconstructs_identical_state(self):
        stack = make_stack(self)
        wf = stack.catalog.get("parcel-intelligence-assembly")
        job = stack.queue.enqueue(
            wf, {"parcel": {"apn": "R-1441", "county": "wise"},
                 "signals": [{"apn": "R-1441", "kind": "tax_delinquent"},
                             {"apn": "R-9999", "kind": "noise"}]},
            requested_by="op")
        stack.runner.execute(job)
        before = stack.queue.load(job)
        fresh = stack.reopen()  # "new process" over the same ledger file
        after = fresh.queue.load(job)
        self.assertEqual(before.status, after.status)
        self.assertEqual(before.step_results, after.step_results)
        self.assertEqual(
            after.step_results["assemble_parcel_report"]["parcel_report"]["flags"],
            ["UNMATCHED_SIGNALS_PRESENT"])

    def test_dry_run_produces_plans_not_effects_and_is_receipted(self):
        executions = {"n": 0}

        def effectful(ctx, params):
            executions["n"] += 1
            return {"did": "something"}

        registry = WorkerRegistry()
        registry.register_worker(WorkerDeclaration(
            worker_id="w", description="d",
            capabilities=(Capability(
                name="effect.cap", description="", read_only=True,
                external=False, execute=effectful),)))
        catalog = WorkflowCatalog()
        catalog.register(WorkflowDefinition(
            name="effect-flow", version="1", description="d",
            steps=(StepDefinition(step_id="only", capability="effect.cap"),)))
        stack = make_stack(self, registry=registry, catalog=catalog)
        job = stack.queue.enqueue(catalog.get("effect-flow"), {},
                                  requested_by="op", dry_run=True)
        state = stack.runner.execute(job)
        self.assertEqual(state.status, JobStatus.SUCCEEDED)
        self.assertEqual(executions["n"], 0)  # execute() never called
        self.assertTrue(state.step_results["only"]["dry_run"])
        enqueued = [r for r in stack.ledger.receipts(job)
                    if r.receipt_type == ReceiptType.JOB_ENQUEUED][0]
        self.assertTrue(enqueued.payload["dry_run"])


if __name__ == "__main__":
    unittest.main()
