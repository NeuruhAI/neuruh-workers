"""Workflow runner: executes the RECORDED spec step by step.

Properties enforced here:
- the runner executes the spec sealed into the enqueue receipt; if the
  catalog's current definition has drifted, execution refuses (no silent drift);
- completed steps are never re-executed (idempotent replay from the ledger);
- retry budget counts RECEIPTED failures only — an interrupted attempt is
  evidence of a crash, not of a step failure, so recovery can re-run it;
- timeouts are enforced per step and receipted;
- approval gates block in every mode, including dry-run (no bypass path);
- external capabilities are re-checked at execution time (defense in depth
  behind the registry gate) and denied with a receipt.
"""
from __future__ import annotations

import threading
from typing import Any, Callable, Mapping

from .contracts import (
    ApprovalInvalid,
    CapabilityViolation,
    ExternalActionDenied,
    JobStateInvalid,
    JobStatus,
    ReceiptType,
    StepTimeout,
    content_sha,
    sha256_hex,
)
from .definitions import RetryPolicy, StepDefinition, WorkflowCatalog
from .ledger import ExecutionLedger
from .queue import JobQueue, JobState
from .registry import StepContext, WorkerRegistry

SleepFn = Callable[[float], None]
StepHook = Callable[[str, str], None]  # (job_id, step_id) before each step


def _step_from_spec(spec_step: Mapping[str, Any]) -> StepDefinition:
    retry = spec_step["retry"]
    return StepDefinition(
        step_id=spec_step["step_id"],
        capability=spec_step["capability"],
        params=spec_step["params"],
        retry=RetryPolicy(
            max_attempts=retry["max_attempts"],
            base_delay_s=retry["base_delay_s"],
            multiplier=retry["multiplier"],
            max_delay_s=retry["max_delay_s"],
        ),
        timeout_s=spec_step["timeout_s"],
        requires_approval=spec_step["requires_approval"],
        approvers=tuple(spec_step["approvers"]),
    )


def _run_with_timeout(fn: Callable[[], Mapping[str, Any]], timeout_s: float):
    box: dict[str, Any] = {}

    def target():
        try:
            box["result"] = fn()
        except BaseException as exc:  # noqa: BLE001 — transported to caller
            box["error"] = exc

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout_s)
    if thread.is_alive():
        raise StepTimeout(f"step exceeded {timeout_s}s", {"timeout_s": timeout_s})
    if "error" in box:
        raise box["error"]
    return box["result"]


class WorkflowRunner:
    def __init__(self, queue: JobQueue, registry: WorkerRegistry,
                 catalog: WorkflowCatalog, *,
                 sleep_fn: SleepFn | None = None,
                 step_hook: StepHook | None = None):
        self._queue = queue
        self._ledger: ExecutionLedger = queue.ledger
        self._registry = registry
        self._catalog = catalog
        self._sleep = sleep_fn or (lambda _s: None)
        self._step_hook = step_hook

    # ------------------------------------------------------------------ #

    def execute(self, job_id: str) -> JobState:
        state = self._queue.load(job_id)
        if state.terminal:
            raise JobStateInvalid(
                f"job is already {state.status.value}", {"job_id": job_id})
        if state.status == JobStatus.PAUSED:
            raise JobStateInvalid(
                "job is paused; resume it first", {"job_id": job_id})
        if state.status == JobStatus.BLOCKED_ON_APPROVAL:
            raise JobStateInvalid(
                "job is blocked on approval", {"job_id": job_id})

        self._verify_spec_integrity(state)

        recovering = bool(state.open_steps)
        if state.status == JobStatus.QUEUED:
            self._ledger.append(ReceiptType.JOB_STARTED, job_id, {})
        elif recovering:
            self._ledger.append(ReceiptType.JOB_RECOVERED, job_id, {
                "open_steps": dict(state.open_steps),
                "note": "prior execution ended without a step outcome; "
                        "idempotent re-run from the ledger",
            })

        replay_noted = recovering or state.status == JobStatus.RUNNING
        for spec_step in state.spec["steps"]:
            step = _step_from_spec(spec_step)
            state = self._queue.load(job_id)

            if step.step_id in state.step_results:
                if replay_noted:
                    self._ledger.append(ReceiptType.STEP_REPLAY_SKIPPED, job_id, {
                        "step_id": step.step_id,
                        "note": "already succeeded; result served from ledger",
                    })
                continue

            if state.pause_requested:
                self._ledger.append(ReceiptType.JOB_PAUSED, job_id,
                                    {"paused_before_step": step.step_id})
                return self._queue.load(job_id)

            if self._step_hook is not None:
                self._step_hook(job_id, step.step_id)
                state = self._queue.load(job_id)
                if state.pause_requested:
                    self._ledger.append(ReceiptType.JOB_PAUSED, job_id,
                                        {"paused_before_step": step.step_id})
                    return self._queue.load(job_id)

            if step.requires_approval and step.step_id not in state.approvals_granted:
                self._request_approval(state, step)
                return self._queue.load(job_id)

            outcome = self._run_step(state, step)
            if outcome is not None:  # a terminal failure was receipted
                return outcome

        self._ledger.append(ReceiptType.JOB_SUCCEEDED, job_id, {
            "results_sha": content_sha(self._queue.load(job_id).step_results),
            "dry_run": state.dry_run,
        })
        return self._queue.load(job_id)

    # ------------------------------------------------------------------ #

    def _verify_spec_integrity(self, state: JobState) -> None:
        if content_sha(state.spec) != state.spec_sha:
            raise JobStateInvalid(
                "recorded spec does not match its sealed hash",
                {"job_id": state.job_id})
        current = self._catalog.get(state.workflow)
        if content_sha(current.spec()) != state.spec_sha:
            raise JobStateInvalid(
                f"workflow {state.workflow!r} has drifted from the spec this "
                "job was enqueued with; refusing to run a different workflow "
                "under an old job id", {"job_id": state.job_id})

    def _approval_binding_sha(self, state: JobState, step: StepDefinition) -> str:
        return content_sha({
            "job_params": dict(state.params),
            "step_params": dict(step.params),
        })

    def _request_approval(self, state: JobState, step: StepDefinition) -> None:
        params_sha = self._approval_binding_sha(state, step)
        challenge = sha256_hex(
            f"{state.job_id}|{step.step_id}|{params_sha}|{self._ledger.verify()['tail_sha']}")
        self._ledger.append(ReceiptType.APPROVAL_REQUESTED, state.job_id, {
            "step_id": step.step_id,
            "params_sha": params_sha,
            "challenge": challenge,
            "approvers": list(step.approvers),
            "requested_by": state.requested_by,
            "dry_run": state.dry_run,
        })

    def _run_step(self, state: JobState, step: StepDefinition) -> JobState | None:
        """Run one step to success or terminal failure.  Returns a terminal
        JobState if the job failed here, else None."""
        job_id = state.job_id
        cap = self._registry.resolve(step.capability)

        # Defense in depth: these were checked at enqueue; re-check at the
        # moment of execution so a mutated registry cannot smuggle an action.
        if cap.external:
            self._ledger.append(ReceiptType.EXTERNAL_ACTION_DENIED, job_id, {
                "step_id": step.step_id, "capability": cap.name})
            self._ledger.append(ReceiptType.JOB_FAILED, job_id, {
                "reason": "external_action_denied", "step_id": step.step_id})
            raise ExternalActionDenied(
                f"capability {cap.name!r} is external; execution is not authorized",
                {"job_id": job_id, "step_id": step.step_id})
        if state.spec["read_only"] and not cap.read_only:
            self._ledger.append(ReceiptType.JOB_FAILED, job_id, {
                "reason": "capability_violation", "step_id": step.step_id})
            raise CapabilityViolation(
                f"read-only job bound non-read-only capability {cap.name!r}",
                {"job_id": job_id, "step_id": step.step_id})

        while True:
            state = self._queue.load(job_id)
            attempt = state.attempts_started.get(step.step_id, 0) + 1
            self._ledger.append(ReceiptType.STEP_STARTED, job_id, {
                "step_id": step.step_id, "attempt": attempt,
                "capability": cap.name, "dry_run": state.dry_run,
            })
            ctx = StepContext(
                job_id=job_id, workflow=state.workflow, step_id=step.step_id,
                attempt=attempt, dry_run=state.dry_run,
                params={**dict(state.params), **dict(step.params)},
                results=dict(state.step_results),
            )
            try:
                if state.dry_run:
                    plan_fn = cap.plan or (lambda c, p: {
                        "plan": f"would execute {cap.name}", "params": dict(p)})
                    result = _run_with_timeout(
                        lambda: plan_fn(ctx, ctx.params), step.timeout_s)
                    result = {**dict(result), "dry_run": True}
                else:
                    result = _run_with_timeout(
                        lambda: cap.execute(ctx, ctx.params), step.timeout_s)
                    result = dict(result)
            except Exception as exc:  # noqa: BLE001 — receipted, never swallowed
                receipt_type = (ReceiptType.STEP_TIMEOUT
                                if isinstance(exc, StepTimeout)
                                else ReceiptType.STEP_FAILED)
                self._ledger.append(receipt_type, job_id, {
                    "step_id": step.step_id, "attempt": attempt,
                    "error_code": getattr(exc, "code", "EXCEPTION"),
                    "error": str(exc),
                })
                failures = self._queue.load(job_id).failures.get(step.step_id, 0)
                if failures >= step.retry.max_attempts:
                    self._ledger.append(ReceiptType.JOB_FAILED, job_id, {
                        "reason": "retry_exhausted", "step_id": step.step_id,
                        "failures": failures,
                    })
                    return self._queue.load(job_id)
                delay = step.retry.delay_for_attempt(failures)
                self._ledger.append(ReceiptType.STEP_RETRY_SCHEDULED, job_id, {
                    "step_id": step.step_id, "after_failure": failures,
                    "delay_s": delay,
                })
                self._sleep(delay)
                continue

            self._ledger.append(ReceiptType.STEP_SUCCEEDED, job_id, {
                "step_id": step.step_id, "attempt": attempt,
                "result": result, "result_sha": content_sha(result),
            })
            return None

    # ------------------------------------------------------------------ #
    # Human approval gates — content-bound, nonce-protected, receipted.

    def _load_blocked(self, job_id: str, step_id: str,
                      approver: str, nonce: str) -> tuple[JobState, dict]:
        state = self._queue.load(job_id)
        pending = state.pending_approval

        def reject(reason: str):
            self._ledger.append(ReceiptType.APPROVAL_REJECTED_ATTEMPT, job_id, {
                "step_id": step_id, "approver": approver, "reason": reason})
            raise ApprovalInvalid(reason, {"job_id": job_id, "step_id": step_id})

        if state.status != JobStatus.BLOCKED_ON_APPROVAL or pending is None:
            reject("job is not blocked on approval")
        if pending["step_id"] != step_id:
            reject("approval targets the wrong step")
        if approver not in pending["approvers"]:
            reject("approver is not on this gate's approver list")
        if approver == state.requested_by:
            reject("self-approval is forbidden")
        if not nonce or nonce in state.used_nonces:
            reject("approval nonce is missing or already used")
        return state, pending

    def grant_approval(self, job_id: str, step_id: str, *, approver: str,
                       nonce: str, challenge: str, params_sha: str) -> JobState:
        state, pending = self._load_blocked(job_id, step_id, approver, nonce)

        def reject(reason: str):
            self._ledger.append(ReceiptType.APPROVAL_REJECTED_ATTEMPT, job_id, {
                "step_id": step_id, "approver": approver, "reason": reason})
            raise ApprovalInvalid(reason, {"job_id": job_id, "step_id": step_id})

        if challenge != pending["challenge"]:
            reject("approval challenge does not match the requested gate")
        if params_sha != pending["params_sha"]:
            reject("approval is not bound to the exact job/step parameters")
        self._ledger.append(ReceiptType.APPROVAL_GRANTED, job_id, {
            "step_id": step_id, "approver": approver, "nonce": nonce,
            "challenge": challenge, "params_sha": params_sha,
        })
        return self._queue.load(job_id)

    def deny_approval(self, job_id: str, step_id: str, *, approver: str,
                      nonce: str, reason: str) -> JobState:
        self._load_blocked(job_id, step_id, approver, nonce)
        self._ledger.append(ReceiptType.APPROVAL_DENIED, job_id, {
            "step_id": step_id, "approver": approver, "nonce": nonce,
            "reason": reason,
        })
        self._ledger.append(ReceiptType.JOB_FAILED, job_id, {
            "reason": "approval_denied", "step_id": step_id,
        })
        return self._queue.load(job_id)
