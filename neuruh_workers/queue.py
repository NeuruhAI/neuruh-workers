"""Job queue with deterministic identity and event-sourced state.

There is no job table.  A job IS its receipts: ``JobState`` is always a pure
replay of the ledger, so a crashed process, a paused job and a fresh load are
all the same code path.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from .contracts import (
    JobStateInvalid,
    JobStatus,
    ReceiptType,
    TERMINAL_STATUSES,
    content_sha,
    deterministic_job_id,
)
from .definitions import WorkflowDefinition
from .ledger import ExecutionLedger
from .registry import WorkerRegistry


@dataclass
class JobState:
    job_id: str
    workflow: str = ""
    version: str = ""
    params: Mapping[str, Any] = field(default_factory=dict)
    spec: Mapping[str, Any] = field(default_factory=dict)
    spec_sha: str = ""
    requested_by: str = ""
    dry_run: bool = False
    status: JobStatus = JobStatus.QUEUED
    step_results: dict[str, Any] = field(default_factory=dict)
    attempts_started: dict[str, int] = field(default_factory=dict)
    failures: dict[str, int] = field(default_factory=dict)
    open_steps: dict[str, int] = field(default_factory=dict)  # started, no outcome
    approvals_granted: set[str] = field(default_factory=set)
    pending_approval: dict[str, Any] | None = None
    used_nonces: set[str] = field(default_factory=set)
    pause_requested: bool = False
    replay_notices: list[str] = field(default_factory=list)
    failure_reason: str = ""
    # Mission continuation: child_role -> dispatch record / ingested result.
    children: dict[str, dict[str, Any]] = field(default_factory=dict)
    child_results: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES


def replay_job(receipts) -> JobState:
    """Fold receipts into state.  Unknown receipt types fail closed."""
    if not receipts:
        raise JobStateInvalid("job has no receipts")
    state = JobState(job_id=receipts[0].job_id)
    for r in receipts:
        rt = r.receipt_type
        p = r.payload
        if rt == ReceiptType.JOB_ENQUEUED:
            state.workflow = p["workflow"]
            state.version = p["version"]
            state.params = p["params"]
            state.spec = p["spec"]
            state.spec_sha = p["spec_sha"]
            state.requested_by = p["requested_by"]
            state.dry_run = p["dry_run"]
            state.status = JobStatus.QUEUED
        elif rt == ReceiptType.JOB_ENQUEUE_DEDUPLICATED:
            pass
        elif rt in (ReceiptType.JOB_STARTED, ReceiptType.JOB_RECOVERED,
                    ReceiptType.JOB_RESUMED):
            state.status = JobStatus.RUNNING
            if rt == ReceiptType.JOB_RESUMED:
                state.pause_requested = False
        elif rt == ReceiptType.JOB_PAUSE_REQUESTED:
            state.pause_requested = True
        elif rt == ReceiptType.JOB_PAUSED:
            state.status = JobStatus.PAUSED
            state.pause_requested = False
        elif rt == ReceiptType.STEP_STARTED:
            step_id = p["step_id"]
            state.attempts_started[step_id] = p["attempt"]
            state.open_steps[step_id] = p["attempt"]
        elif rt == ReceiptType.STEP_SUCCEEDED:
            step_id = p["step_id"]
            state.step_results[step_id] = p["result"]
            state.open_steps.pop(step_id, None)
        elif rt in (ReceiptType.STEP_FAILED, ReceiptType.STEP_TIMEOUT):
            step_id = p["step_id"]
            state.failures[step_id] = state.failures.get(step_id, 0) + 1
            state.open_steps.pop(step_id, None)
        elif rt == ReceiptType.STEP_RETRY_SCHEDULED:
            pass
        elif rt == ReceiptType.STEP_REPLAY_SKIPPED:
            state.replay_notices.append(p["step_id"])
        elif rt == ReceiptType.APPROVAL_REQUESTED:
            state.pending_approval = dict(p)
            state.status = JobStatus.BLOCKED_ON_APPROVAL
        elif rt == ReceiptType.APPROVAL_GRANTED:
            state.approvals_granted.add(p["step_id"])
            state.used_nonces.add(p["nonce"])
            state.pending_approval = None
            state.status = JobStatus.RUNNING
        elif rt == ReceiptType.APPROVAL_DENIED:
            state.used_nonces.add(p["nonce"])
            state.pending_approval = None
        elif rt == ReceiptType.APPROVAL_REJECTED_ATTEMPT:
            pass
        elif rt == ReceiptType.EXTERNAL_ACTION_DENIED:
            pass
        elif rt in (ReceiptType.SEAM_DECLARED,
                    ReceiptType.SEAM_ACTIVATION_REFUSED):
            pass
        elif rt in (ReceiptType.CANDIDATE_PUSH_REQUESTED,
                    ReceiptType.CANDIDATE_BRANCH_PUSHED,
                    ReceiptType.CANDIDATE_PR_OPENED,
                    ReceiptType.CI_STARTED,
                    ReceiptType.CI_PASSED,
                    ReceiptType.CI_FAILED,
                    ReceiptType.PROMOTION_READY):
            pass
        elif rt == ReceiptType.CHILD_DISPATCHED:
            state.children[p["child_role"]] = dict(p)
        elif rt == ReceiptType.CHILD_RESULT_INGESTED:
            state.child_results[p["child_role"]] = dict(p)
        elif rt == ReceiptType.JOB_SUCCEEDED:
            state.status = JobStatus.SUCCEEDED
        elif rt == ReceiptType.JOB_FAILED:
            state.status = JobStatus.FAILED
            state.failure_reason = p.get("reason", "")
        elif rt == ReceiptType.JOB_CANCELLED:
            state.status = JobStatus.CANCELLED
            state.failure_reason = p.get("reason", "")
        else:
            raise JobStateInvalid(
                f"unknown receipt type {rt!r} in ledger", {"receipt_type": rt})
    return state


class JobQueue:
    def __init__(self, ledger: ExecutionLedger, registry: WorkerRegistry):
        self._ledger = ledger
        self._registry = registry

    @property
    def ledger(self) -> ExecutionLedger:
        return self._ledger

    def enqueue(self, definition: WorkflowDefinition, params: Mapping[str, Any],
                *, requested_by: str, dry_run: bool = False,
                dedupe_key: str | None = None) -> str:
        if not requested_by:
            raise JobStateInvalid("requested_by is required")
        self._registry.validate_binding(definition)
        job_id = deterministic_job_id(
            definition.name, definition.version, params, dedupe_key,
            dry_run=bool(dry_run))
        existing = self._ledger.receipts(job_id)
        if any(r.receipt_type == ReceiptType.JOB_ENQUEUED for r in existing):
            self._ledger.append(ReceiptType.JOB_ENQUEUE_DEDUPLICATED, job_id, {
                "requested_by": requested_by,
                "note": "identical job already enqueued; enqueue is idempotent",
            })
            return job_id
        spec = definition.spec()
        self._ledger.append(ReceiptType.JOB_ENQUEUED, job_id, {
            "workflow": definition.name,
            "version": definition.version,
            "params": dict(params),
            "params_sha": content_sha(dict(params)),
            "spec": spec,
            "spec_sha": content_sha(spec),
            "requested_by": requested_by,
            "dry_run": bool(dry_run),
            "dedupe_key": dedupe_key,
        })
        return job_id

    def load(self, job_id: str) -> JobState:
        receipts = self._ledger.receipts(job_id)
        if not receipts:
            raise JobStateInvalid(f"unknown job {job_id!r}", {"job_id": job_id})
        return replay_job(receipts)

    def pause(self, job_id: str) -> JobState:
        state = self.load(job_id)
        if state.terminal:
            raise JobStateInvalid(
                f"cannot pause {state.status.value} job", {"job_id": job_id})
        if state.status == JobStatus.PAUSED:
            return state
        if state.status == JobStatus.QUEUED:
            self._ledger.append(ReceiptType.JOB_PAUSED, job_id,
                                {"paused_from": state.status.value})
        else:
            # A running/blocked job is paused cooperatively at the next
            # step boundary; the runner appends JOB_PAUSED when it honors it.
            self._ledger.append(ReceiptType.JOB_PAUSE_REQUESTED, job_id, {})
        return self.load(job_id)

    def resume(self, job_id: str) -> JobState:
        state = self.load(job_id)
        if state.status != JobStatus.PAUSED:
            raise JobStateInvalid(
                f"cannot resume a {state.status.value} job", {"job_id": job_id})
        self._ledger.append(ReceiptType.JOB_RESUMED, job_id, {})
        return self.load(job_id)

    def cancel(self, job_id: str, *, reason: str) -> JobState:
        state = self.load(job_id)
        if state.terminal:
            raise JobStateInvalid(
                f"cannot cancel a {state.status.value} job", {"job_id": job_id})
        self._ledger.append(ReceiptType.JOB_CANCELLED, job_id, {"reason": reason})
        return self.load(job_id)

    def job_ids(self) -> list[str]:
        seen: list[str] = []
        for r in self._ledger.receipts():
            if r.job_id and r.job_id not in seen:
                seen.append(r.job_id)
        return seen
