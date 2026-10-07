"""Shared test harness: a full stack on a temp ledger with a fixed clock."""
from __future__ import annotations

import itertools
import os
import tempfile

from neuruh_workers import (
    ExecutionLedger, JobQueue, WorkflowRunner,
)
from neuruh_workers.workflows import build_catalog, build_registry


def fixed_clock():
    counter = itertools.count()
    return lambda: f"2026-07-22T00:00:{next(counter) % 60:02d}.000Z"


class Stack:
    def __init__(self, tmpdir: str, *, step_hook=None, sleep_fn=None,
                 registry=None, catalog=None):
        self.ledger_path = os.path.join(tmpdir, "ledger.jsonl")
        self.ledger = ExecutionLedger(self.ledger_path, clock=fixed_clock())
        self.registry = registry or build_registry()
        self.catalog = catalog or build_catalog()
        self.queue = JobQueue(self.ledger, self.registry)
        self.sleeps: list[float] = []
        self.runner = WorkflowRunner(
            self.queue, self.registry, self.catalog,
            sleep_fn=sleep_fn or self.sleeps.append,
            step_hook=step_hook,
        )

    def reopen(self, *, step_hook=None):
        """A 'new process' over the same ledger file."""
        fresh = Stack.__new__(Stack)
        fresh.ledger_path = self.ledger_path
        fresh.ledger = ExecutionLedger(self.ledger_path, clock=fixed_clock())
        fresh.registry = build_registry()
        fresh.catalog = build_catalog()
        fresh.queue = JobQueue(fresh.ledger, fresh.registry)
        fresh.sleeps = []
        fresh.runner = WorkflowRunner(
            fresh.queue, fresh.registry, fresh.catalog,
            sleep_fn=fresh.sleeps.append, step_hook=step_hook)
        return fresh


def make_stack(testcase, **kwargs) -> Stack:
    tmpdir = tempfile.mkdtemp(prefix="neuruh-workers-test-")
    testcase.addCleanup(_rmtree, tmpdir)
    return Stack(tmpdir, **kwargs)


def _rmtree(path):
    import shutil
    shutil.rmtree(path, ignore_errors=True)


BRIEF_PARAMS = {
    "date": "2026-07-22",
    "opportunities": [
        {"id": "opp-2", "title": "Storm roof lead", "vertical": "roofops", "score": 7.5},
        {"id": "opp-1", "title": "Tax lien parcel", "vertical": "landos", "score": 9.0},
        {"id": "opp-1", "title": "duplicate", "vertical": "landos", "score": 1.0},
    ],
}


def run_brief_to_gate(stack: Stack, *, dry_run: bool = False) -> str:
    job_id = stack.queue.enqueue(
        stack.catalog.get("daily-opportunity-brief"), BRIEF_PARAMS,
        requested_by="operator", dry_run=dry_run)
    stack.runner.execute(job_id)
    return job_id


def pending_approval(stack: Stack, job_id: str) -> dict:
    return stack.queue.load(job_id).pending_approval
